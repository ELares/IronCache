// SPDX-License-Identifier: MIT OR Apache-2.0
//! Server wiring: config -> runtime -> per-shard accept -> per-connection
//! read/dispatch/write loop (CLI_BINARY.md "zero-config boot", RUNTIME.md).
//!
//! Each shard runs on its own OS thread with its own current-thread tokio runtime
//! (ADR-0002, shared-nothing). Per-shard state (the client-id counter and the
//! observability counters) is core-local: it lives in `Rc<RefCell<..>>` owned by
//! the shard's tasks, never shared across cores, so there is no cross-core
//! synchronization. The connection loop decodes RESP, dispatches Tier-0 commands,
//! and writes the encoded reply.

use crate::coordinator;
use ironcache_config::{Config, RuntimeConfig};
use ironcache_env::{Clock, Env, Rng, SystemEnv};
use ironcache_eviction::{Policy, map_policy_name};
use ironcache_observe::{CounterSnapshot, MemoryInfo, ServerInfo, ShardCounters};
use ironcache_runtime::bootstrap::{ShardConfig, ShardId, ShardSet};
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{
    ConnState, CounterDeltas, DecodeOutcome, EXPIRE_CYCLE_INTERVAL, Limits, MAX_RECLAIM_PER_CYCLE,
    ProtoVersion, Request, ScanCursor, TimingWheel, UnixMillis, decode, dispatch_with_cmd,
    drain_due_keys, route,
};
use ironcache_storage::CountingAccounting;
use ironcache_store::{ShardStore, process_memory};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// The name of the global allocator selected at build time, for INFO
/// `mem_allocator`. This MUST track the `#[global_allocator]` cfg in `main.rs`
/// (jemalloc on every target except MSVC, where it falls back to the system
/// allocator), so INFO never claims jemalloc on a build that linked the system
/// allocator.
#[cfg(not(target_env = "msvc"))]
pub const GLOBAL_ALLOCATOR_NAME: &str = "jemalloc";
#[cfg(target_env = "msvc")]
pub const GLOBAL_ALLOCATOR_NAME: &str = "libc";

/// An RAII guard for one admitted slot in the process-global `maxclients` connection gate (L1,
/// PROD-SAFETY #3). It is created ONLY on a successful `try_admit` and RELEASES the slot on
/// [`Drop`] -- on the normal close path, on an early `return`, AND on a PANIC unwinding through the
/// serve loop. The release used to be a plain statement at the end of `serve_connection`, which a
/// panic after `try_admit` would skip, permanently leaking the slot (a progressive, false "max
/// number of clients reached" as leaked slots accumulate). Mirrors the `SaveGuard` pattern in the
/// persistence code. The REJECT path and the TLS-handshake-fail path return BEFORE `try_admit`, so
/// they never construct this guard (a rejected/handshake-failed connection was never admitted and
/// must not release).
struct ConnGateGuard {
    gate: Arc<ironcache_observe::ConnectionGate>,
}

impl Drop for ConnGateGuard {
    fn drop(&mut self) {
        self.gate.release();
    }
}

/// RAII deregistration of a connection from the node-level [`ironcache_observe::ClientRegistry`]
/// (PROD-7). A connection REGISTERS itself on accept; this guard DEREGISTERS it on EVERY exit path
/// (normal close, early return, panic), so a closed connection never lingers in CLIENT LIST and a
/// CLIENT KILL targeting a now-dead id is a clean no-op. Mirrors [`ConnGateGuard`].
struct ClientRegistryGuard {
    registry: Arc<ironcache_observe::ClientRegistry>,
    id: u64,
}

impl Drop for ClientRegistryGuard {
    fn drop(&mut self) {
        self.registry.deregister(self.id);
    }
}

/// Format a 40-lowercase-hex cluster node id from the determinism seam's RNG
/// (CLUSTER_CONTRACT.md #70). It draws THREE `u64`s (192 bits) and renders 160 of them as
/// 40 lowercase hex chars: the full first two words plus the LOW 32 bits of the third
/// (`c as u32` keeps the low half; the high 32 bits are dropped), matching the Redis 40-hex
/// node-id width.
///
/// This helper is PURE: it takes `&mut impl Rng` and does NO time/OS-entropy access of
/// its own, so the no-rand invariant lint stays green (the only entropy comes through the
/// `ironcache-env` seam the caller owns, ADR-0003). The caller draws from the binary's
/// `SystemEnv` ONCE at boot so the id is stable for the process lifetime and identical
/// across shards.
fn node_id_hex(rng: &mut impl Rng) -> String {
    let a = rng.next_u64();
    let b = rng.next_u64();
    let c = rng.next_u64();
    // 64 + 64 + 32 = 160 bits = 40 hex nibbles. `{:016x}` and `{:08x}` zero-pad to the
    // full nibble width so the id is always exactly 40 chars.
    format!("{a:016x}{b:016x}{:08x}", c as u32)
}

/// Draw a PER-BOOT replication HISTORY token: 20 random bytes (a [`ReplId`]) from the determinism
/// seam, NEW on every process boot. This is the RESUME identity, distinct from the STABLE
/// `cluster_node_id` (the cluster-mode replid was the announce id, UNCHANGED across restarts), so a
/// primary restart yields a fresh token and a reconnecting replica's remembered (old) token
/// mismatches -> a full re-sync (the fence against silent divergence: a restarted primary resets its
/// offset space to 0 yet kept the stable cluster id). Like [`node_id_hex`] it is PURE: the only
/// entropy comes through the `ironcache-env` RNG the caller owns (ADR-0003, no `rand` outside
/// `ironcache-env`), drawn ONCE at boot. 160 bits = 20 bytes = the full [`ReplId`] width.
fn repl_history_token(rng: &mut impl Rng) -> ironcache_server::ReplId {
    let a = rng.next_u64().to_be_bytes();
    let b = rng.next_u64().to_be_bytes();
    let c = rng.next_u64().to_be_bytes();
    let mut raw = [0u8; 20];
    raw[0..8].copy_from_slice(&a);
    raw[8..16].copy_from_slice(&b);
    // 64 + 64 + 32 = 160 bits; take the low 4 bytes of the third draw for the final 32 bits.
    raw[16..20].copy_from_slice(&c[4..8]);
    ironcache_server::ReplId::from_bytes(raw)
}

/// The concrete per-shard store the binary wires: the `ShardStore` over the
/// configured eviction [`Policy`] and the logical-byte accounting hook. The generic
/// dispatch runs against this through the `Store` + `Admit` waist traits.
///
/// `pub(crate)` so the [`crate::coordinator`] drain loop names the same concrete store
/// type the per-shard thread-locals hold (it runs remote keyed work against it).
pub(crate) type ShardStoreImpl = ShardStore<Policy, CountingAccounting>;

/// Per-shard, core-local mutable state. Single-threaded access on the shard's
/// thread (no `Send`/`Sync` needed, no locks; shared-nothing ADR-0002).
///
/// `pub(crate)` so the [`crate::coordinator`] drain loop can fold a remote command's
/// counter deltas into the OWNING shard's counters (the data lives there).
pub(crate) struct ShardState {
    pub(crate) next_client_id: u64,
    pub(crate) counters: ShardCounters,
    /// Per-shard command + error execution stats for INFO COMMANDSTATS / ERRORSTATS (#413).
    /// Home-shard-local (INFO reads the serving shard's table, the same scope the other INFO
    /// counters use); the serve loop records each executed command here, `CONFIG RESETSTAT`
    /// clears it.
    pub(crate) command_stats: ironcache_observe::CommandStats,
    /// The last runtime-config GENERATION this shard observed (PR-4b). Dispatch compares
    /// the shared `RuntimeConfig::generation()` against this once per command (a relaxed
    /// atomic load + integer compare, NO lock when unchanged) and, on a change, rebuilds
    /// this shard's eviction policy from the new `maxmemory-policy` name. Core-local
    /// (per shard, shared-nothing ADR-0002): each shard catches up to a `CONFIG SET
    /// maxmemory-policy` on its next command.
    pub(crate) last_policy_generation: u64,
}

/// Boot the server: derive the shard config from `config`, start the shard set,
/// and return the [`ShardSet`] handle for shutdown. Errors if the listener cannot
/// bind (e.g. port in use).
///
/// `too_many_lines` is allowed: this is the single BOOT wiring point (the leaked policy/node-id
/// statics, the runtime overlay, the cluster slot-map build, the OPT-IN raft control-plane
/// bootstrap, the shared ServerContext template, and the coordinator inbox + shard spawn). Each
/// is a documented step the boot must run in one place; splitting it would scatter the wiring
/// across helpers that all need the same boot-resolved locals. The same precedent as
/// `route_and_dispatch` / `serve_connection`.
pub fn run_server(config: &Config) -> anyhow::Result<ShardSet> {
    run_server_inner(config).map(|(set, _raft)| set)
}

/// Like [`run_server`] but ALSO returns the raft-mode [`RaftHandle`] (HA-prod-membership), so a
/// test can directly observe the live Raft CONFIGURATION (voter / learner sets via
/// `RaftHandle::config`) without a new wire surface. `None` outside raft-governance mode. Boots
/// exactly as `run_server` (which is `run_server_inner` discarding the handle); production never
/// needs the handle out here (it lives in `ServerContext`).
pub fn run_server_inner(
    config: &Config,
) -> anyhow::Result<(ShardSet, Option<ironcache_server::RaftHandle>)> {
    // No metrics endpoint on this boot path (tests / `run_server`), so no readiness state to thread.
    let handles = run_server_observed(config, None, None)?;
    Ok((handles.set, handles.raft))
}

/// The full set of boot handles the binary needs to wire the out-of-band observability endpoint
/// (OBSERVABILITY.md, #152) on top of a running server, returned by [`run_server_observed`].
///
/// Beyond the [`ShardSet`] (for shutdown/join) it carries the live `raft` handle (the
/// `ironcache_raft_*` gauges + the `/readyz` leader gate), the `persist` state (the last-save +
/// dirty gauges; `None` when persistence is off), and the `runtime` config cell (so the
/// `maxmemory` gauge reflects a `CONFIG SET`). The metrics registry is the SAME one the caller
/// passed in (the shards adopted its cells), so it is not echoed back.
pub struct BootHandles {
    /// The running shard set, for graceful shutdown + join.
    pub set: ShardSet,
    /// The raft control-plane handle, `Some` only in raft-governance mode.
    pub raft: Option<ironcache_server::RaftHandle>,
    /// The node-level persistence state, `Some` only when a `data_dir` is configured.
    pub persist: Option<Arc<crate::persist::PersistState>>,
    /// The process-wide runtime-config overlay (for the live `maxmemory` gauge).
    pub runtime: Arc<ironcache_config::RuntimeConfig>,
    /// The structured-topology read state (#365): node identity + the (optional) cluster slot map,
    /// for the `/topology` admin endpoint. Coherent in standalone mode (no cluster map).
    pub topology: crate::topology::TopologyHandle,
}

/// Boot the server like [`run_server`], but thread an optional [`MetricsRegistry`] through every
/// shard's [`ServerContext`] (so each shard ADOPTS its pre-allocated counter cell and the metrics
/// HTTP task can read the cells across threads), and return the [`BootHandles`] the binary uses to
/// stand up the `/metrics` + `/livez` + `/readyz` endpoint.
///
/// `metrics_registry` is `Some` ONLY when `--metrics-addr` is set; passing `None` (every test and
/// the no-flag default) makes the shards use a standalone counter cell and is byte-identical to
/// the pre-observability boot.
///
/// `ready` is the `/readyz` readiness state (`Some` only when the metrics endpoint is enabled). It
/// is threaded into the per-shard drain closure so each shard, AFTER its `load_shard_on_boot`
/// completes, signals one unit of the readiness countdown -- so `/readyz` reports 200 only once
/// EVERY shard has actually finished loading its snapshot, never while a restore is still in flight
/// (the previous wiring flipped a single flag right after this function returned, before any shard
/// had loaded). `None` (every test boot without metrics, the no-flag default) signals nothing and
/// is byte-identical to before.
#[allow(clippy::too_many_lines)]
pub fn run_server_observed(
    config: &Config,
    metrics_registry: Option<ironcache_observe::MetricsRegistry>,
    ready: Option<Arc<crate::metrics_http::ReadyState>>,
) -> anyhow::Result<BootHandles> {
    let bind: SocketAddr = SocketAddr::new(config.bind, config.port);
    let shard_cfg = ShardConfig {
        shards: config.shards,
        bind,
        // SHARD-OWNER ENDPOINTS (#517): in shard-owners mode, bind one listener per shard (port + i)
        // so a cluster-aware client routes each key to its owner's port and skips the internal hop.
        shard_owner_ports: config.cluster_mode == ironcache_config::ClusterMode::ShardOwners,
    };

    // The BOOT eviction policy NAME is leaked to a 'static str so INFO/ServerInfo can
    // hold it cheaply for the process lifetime as the STATIC boot fact. The CURRENT
    // effective policy (which a `CONFIG SET maxmemory-policy` changes) lives in the
    // RuntimeConfig cell; INFO reads it from there (PR-4b). One small leak at boot.
    let policy_name: &'static str = Box::leak(config.maxmemory_policy.clone().into_boxed_str());

    // The cluster node id (CLUSTER_CONTRACT.md #70), leaked to a 'static str at boot exactly
    // like `policy_name`. It is resolved ONCE at the run_server level (NOT per shard) so it is
    // IDENTICAL across every shard, and shared by value via the cloned ServerContext.
    // `CLUSTER MYID` / `CLUSTER NODES` report it. There are two sources, reconciled here:
    //
    //   * CLUSTER-MAP MODE (slice 2: `cluster_enabled` + a topology + an announce id): the id
    //     IS the configured announce id, so it is STABLE across boots and matches the node's
    //     entry in the static topology. `Config::validate` already proved the announce id is
    //     40-hex AND present in the topology, so this is the node's permanent identity and the
    //     MOVED target / `CLUSTER NODES` self line / `map.me().id` all agree on it.
    //   * SLICE-1 / NON-CLUSTER / NO-MAP: keep the deterministic random id drawn from the
    //     binary's SystemEnv RNG (the sanctioned determinism seam, ADR-0003: no `rand` outside
    //     ironcache-env). A real Redis assigns a 40-hex id whether or not cluster mode is on,
    //     and so does IronCache; this path and its determinism test are unchanged.
    //
    // One small leak at boot in either case.
    let mut boot_env = SystemEnv::new();
    let cluster_node_id: &'static str =
        match (&config.cluster_topology, &config.cluster_announce_id) {
            (Some(_), Some(id)) if config.cluster_enabled => Box::leak(id.clone().into_boxed_str()),
            _ => Box::leak(node_id_hex(boot_env.rng()).into_boxed_str()),
        };

    // The process-wide runtime-config overlay (PR-4b, the highest-precedence layer):
    // ONE Arc shared (cloned) into every shard's context, exactly like the shutdown
    // AtomicBool precedent. A `CONFIG SET` mutates it; the per-command reads are cheap
    // atomic loads (maxmemory/generation) with the string params behind a lock taken
    // only on CONFIG SET. Seeded from the boot-resolved config.
    let runtime = RuntimeConfig::from_config(config);
    // A clone of the runtime-config handle returned in `BootHandles` so the `/metrics` maxmemory
    // gauge reads the LIVE effective ceiling (a `CONFIG SET maxmemory` is reflected). `runtime`
    // itself moves into `ctx_template` below.
    let ctx_template_runtime = Arc::clone(&runtime);

    // The process-wide ACL user registry (#106): ONE Arc shared (cloned) into every shard's
    // context, exactly like `runtime`. Seeded from the boot-resolved requirepass digest (the
    // legacy single-`default`-user posture); if an `aclfile` is configured its `user ...` lines
    // are LOADED on top so ACL users survive a restart. With no requirepass and no aclfile the
    // registry is the single all-permissive `default` user, so `is_acl_active()` is false and the
    // per-command enforcement path is byte-identical (no ACL cost).
    let acl = ironcache_server::AclState::from_requirepass(config.requirepass.as_deref());
    if let Some(path) = config.aclfile.as_ref() {
        match std::fs::read_to_string(path) {
            Ok(text) => match acl.load_users(&text) {
                Ok(n) => {
                    tracing::info!(users = n, path = %path.display(), "loaded ACL users from aclfile");
                }
                Err((lineno, e)) => {
                    // A malformed aclfile is an operator error; fail boot loudly rather than run
                    // with a surprising (or empty) ACL. The error NEVER includes a plaintext
                    // password (the file holds only #digests / the redacted rule).
                    panic!("aclfile {} line {lineno}: {}", path.display(), e.reason);
                }
            },
            Err(e) => {
                panic!("failed to read aclfile {}: {e}", path.display());
            }
        }
    }

    // The cluster slot-ownership map (CLUSTER_CONTRACT.md #70), built ONCE here at boot and
    // threaded (Arc) into every shard's context. Slice 3: a cluster-ENABLED node ALWAYS gets a
    // `Some(map)`, in one of two shapes; a cluster-DISABLED standalone node gets `None` (and keeps
    // the slice-1 single-node-owns-all CLUSTER projection bodies).
    //
    //   * STATIC TOPOLOGY (cluster_enabled + a configured topology): build the validated multi-node
    //     map from config. The build cannot fail here: `Config::validate` already ran
    //     `SlotMap::build` on the same (topology, announce-id) input and the process would have
    //     exited non-zero on any error, so `.expect("validated")` documents that invariant.
    //   * NO TOPOLOGY (cluster_enabled, no topology): an EMPTY single-node map owning ZERO slots
    //     (`SlotMap::empty_self`). The operator forms the cluster at runtime via `CLUSTER MEET /
    //     ADDSLOTS / SETSLOT / ...` (slice 3). NOTE: a cluster-enabled no-topology node previously
    //     (slice 1/2) reported single-node-owns-all (16384 slots, state:ok); it now owns ZERO slots
    //     until ADDSLOTS and reports cluster_state:fail, matching a fresh real-Redis node.
    // In RAFT mode the slot map is built fresh as `empty_self` and governed by the control plane
    // (handled in the raft block below), so the STATIC topology build (which requires a complete
    // map) is skipped: a raft-mode topology legitimately carries empty slot ranges. A raft-mode
    // node therefore takes the `empty_self` arm here exactly like a no-topology node, and the
    // shared governed map replaces it below.
    let build_static_topology = config.cluster_mode == ironcache_config::ClusterMode::Static;
    let build_shard_owners = config.cluster_mode == ironcache_config::ClusterMode::ShardOwners;
    let mut cluster: Option<Arc<ironcache_cluster::SlotMap>> = if config.cluster_enabled
        && build_shard_owners
    {
        // SHARD-OWNERS (#517 PR4): build the N-SHARD PROJECTION -- N synthetic cluster nodes, node i
        // at `(advertised_host, base_port + i)` owning the CONTIGUOUS slot range `shard_slot_range(i,
        // n)`, the SAME partition the internal router uses (`owner_shard = slot_to_shard(key_slot)`).
        // A cluster-aware client reads this via CLUSTER SLOTS and dials each key's owner PORT, so the
        // connection homes on the owning shard (PR3's per-shard listeners) and the internal cross-
        // shard hop is ELIMINATED; a mis-routed key gets a MOVED to the owner's port (see
        // `moved_if_unowned`). Node 0 carries `cluster_node_id` (the announce id) so it is `self` in
        // the map and `CLUSTER MYID` is stable; nodes 1.. get deterministic synthetic ids.
        let n = config.shards.max(1);
        let base = config.port;
        let host = shard_owner_announce_host(config.bind);
        let nodes: Vec<(ironcache_cluster::NodeEntry, Vec<[u16; 2]>)> = (0..n)
            .map(|i| {
                let [start, end_excl] = ironcache_server::route::shard_slot_range(i, n);
                let offset = u16::try_from(i).expect("shard index <= MAX_SHARDS fits u16");
                let port = base.checked_add(offset).unwrap_or_else(|| {
                    panic!("shard-owner base port {base} + shard {i} overflows u16")
                });
                let id: Box<str> = if i == 0 {
                    cluster_node_id.into()
                } else {
                    ironcache_server::cmd_cluster::synth_node_id(&host, port).into_boxed_str()
                };
                (
                    ironcache_cluster::NodeEntry {
                        id,
                        host: host.clone().into_boxed_str(),
                        port,
                    },
                    // `shard_slot_range` is half-open [start, end_excl); `SlotMap::build` wants an
                    // INCLUSIVE [start, end], so the top slot is `end_excl - 1`.
                    vec![[start, end_excl - 1]],
                )
            })
            .collect();
        Some(Arc::new(
            ironcache_cluster::SlotMap::build(nodes, cluster_node_id)
                .expect("the N-shard-owner projection partitions [0,16384) with self present"),
        ))
    } else if config.cluster_enabled {
        match config.cluster_topology.as_ref() {
            Some(topo) if build_static_topology => {
                let nodes: Vec<(ironcache_cluster::NodeEntry, Vec<[u16; 2]>)> = topo
                    .nodes
                    .iter()
                    .map(|n| {
                        (
                            ironcache_cluster::NodeEntry {
                                id: n.id.clone().into_boxed_str(),
                                host: n.host.clone().into_boxed_str(),
                                port: n.port,
                            },
                            n.slots.clone(),
                        )
                    })
                    .collect();
                Some(Arc::new(
                    ironcache_cluster::SlotMap::build(nodes, cluster_node_id).expect("validated"),
                ))
            }
            // No topology, OR raft-mode (the static build is skipped): an EMPTY single-node map.
            // The raft block below REPLACES this with the shared, control-plane-governed map.
            Some(_) | None => Some(Arc::new(ironcache_cluster::SlotMap::empty_self(
                cluster_node_id,
                &config.bind.to_string(),
                config.port,
            ))),
        }
    } else {
        None
    };

    // RAFT-GOVERNANCE MODE (HA-4c), strictly opt-in (`cluster_mode == Raft`, only meaningful
    // with `cluster_enabled` + a topology). The DEFAULT (`Static`) path skips this ENTIRELY, so
    // `raft` stays `None` and `cluster` keeps the slice-2/3 shape (byte-unchanged).
    //
    // In raft-mode the slot map is GOVERNED by the merged Raft control plane: build ONE shared
    // `Arc<SlotMap>` seeded `empty_self` (a fresh cluster-enabled node owning ZERO slots, exactly
    // like the no-topology slice-3 boot), install it as BOTH `ctx.cluster` (routing + the CLUSTER
    // projection read committed state with NO change to those readers) AND the config state
    // machine's map, then spawn the per-node control-plane task. A CLUSTER mutator then PROPOSES a
    // ConfigCmd through the log; on commit every node applies the same change into its shared map.
    let raft: Option<ironcache_server::RaftHandle> = if config.cluster_enabled
        && config.cluster_mode == ironcache_config::ClusterMode::Raft
        && config.cluster_topology.is_some()
    {
        // The shared map the control plane WRITES (via apply) and the shards READ. Seeded
        // empty_self for THIS node; the topology only supplies the voter set + peer bus addrs
        // (slot ownership is established at runtime through committed proposals, not the static
        // ranges). The `cluster_node_id` is the validated announce id (== this node's id).
        let shared = Arc::new(ironcache_cluster::SlotMap::empty_self(
            cluster_node_id,
            &config.bind.to_string(),
            config.port,
        ));
        // A JOINING node (HA-prod-membership) boots as a non-voter that learns its membership from
        // the replicated log (after an operator CLUSTER MEET); the default boot makes every topology
        // node an initial voter (byte-unchanged). The F1 node-id-scheme guard can REFUSE boot here
        // (an in-place upgrade onto persisted state written under the old NodeId scheme); propagate
        // it as a fatal boot error so the operator gets the actionable message instead of a silent
        // split brain.
        let boot = if config.cluster_raft_joining {
            crate::raft_boot::spawn_control_plane_joining(config, cluster_node_id, shared)?
        } else {
            crate::raft_boot::spawn_control_plane(config, cluster_node_id, shared)?
        };
        // Install the SHARED map as ctx.cluster (replacing the static/empty map built above) so
        // every shard's routing + projection reads the SAME map the ConfigSm converges.
        cluster = Some(boot.cluster);
        Some(boot.raft)
    } else {
        None
    };

    // The NODE-LEVEL replication status cell (HA-7e): one per node, shared by `Arc` onto every
    // shard's context. `Some` ONLY in raft-governance mode (the same gate as `raft`), so the
    // default static path carries `None` and INFO/CLUSTER SHARDS render the byte-compatible
    // standalone posture. The repl tasks (installed only in raft-mode by `replica_attach`)
    // publish role / offsets / link state here; the serve layer reads a snapshot for INFO /
    // CLUSTER SHARDS, and HA-8's gate reads `is_in_sync`. It is cold node-level state (atomics,
    // no hot-path lock), never touched per stored key.
    let repl_status: Option<Arc<ironcache_server::ReplNodeStatus>> = if raft.is_some() {
        Some(Arc::new(ironcache_server::ReplNodeStatus::new()))
    } else {
        None
    };

    // The SOURCE-SIDE in-sync-replica COUNT (ADR-0026, the WRITE-SIDE `min-replicas-to-write`
    // guardrail): one per node, shared by `Arc` onto every shard's context AND into the primary's
    // per-replica serve tasks (which maintain it with lock-free per-connection deltas). `Some` ONLY
    // in raft-governance mode (the same gate as `repl_status`), so the default static path carries
    // `None` and the write path never even has a cell to read. It is a single `AtomicUsize` (no
    // hot-path lock), node-level cold state, never touched per stored key. The WRITE path reads it
    // ONLY when `min_replicas_to_write > 0`, so the default-disabled guardrail is byte-unchanged.
    let in_sync_replicas: Option<Arc<ironcache_server::InSyncReplicas>> = if raft.is_some() {
        Some(Arc::new(ironcache_server::InSyncReplicas::new()))
    } else {
        None
    };

    // The PER-BOOT replication HISTORY token (the resume identity): drawn ONCE here from the SAME
    // boot RNG seam `cluster_node_id` used (ADR-0003), so it is a NEW value on every restart while
    // the stable cluster id is unchanged. `Some` ONLY in raft-governance mode (the only mode that
    // serves the live incremental resume, the same gate as `repl_status`/`in_sync_replicas`); the
    // default static path carries `None` (it never serves the resume, so a first-connect replica
    // always full-syncs and the path is byte-unchanged). Carried in the `ServerContext` to the repl
    // tasks: the primary advertises it in `FullSync`, the replica remembers + re-advertises it, and
    // a resume happens ONLY on an exact match (a restart -> a NEW token -> a full re-sync).
    let repl_history_id: Option<ironcache_server::ReplId> = if raft.is_some() {
        Some(repl_history_token(boot_env.rng()))
    } else {
        None
    };

    // PERSISTENCE node-level state (#58): `Some` ONLY when a `data_dir` is configured (the single
    // enable switch). Created BEFORE the server context so the context can carry the SHARED
    // persistence-stats cell (last-save + dirty) the INFO `# Persistence` section + the `/metrics`
    // gauges read -- all three see the same live atomics this state writes. With `None` (the
    // default) the context's `persist_stats` is `None`, the serve router never intercepts
    // SAVE/BGSAVE/LASTSAVE, no dirty counter is bumped, no periodic timer is spawned, and
    // load-on-boot is a no-op -- so the default boot + hot path are byte-unchanged.
    let persist = crate::persist::PersistState::from_config(config);
    // The shared persistence-stats cell handed into the context (and below into the metrics
    // handles); `None` when persistence is off so INFO renders the honest persistence-disabled
    // section and the gauges report 0.
    let persist_stats = persist.as_ref().map(|p| p.stats());

    // The process-GLOBAL allocator-memory gauge (PROD-SAFETY #1/#2): ONE per node, shared by `Arc`
    // onto every shard's context AND into each shard's periodic expiry tick (which reads the
    // jemalloc mallctl OFF the command hot path and publishes here). The maxmemory admission gate
    // reads it so the over-limit DECISION is driven off REAL process memory against the FULL
    // `maxmemory` (protecting the HOST from OOM), with the per-shard logical counter as the
    // byte-unchanged fallback when no allocator figure is available. Created unconditionally (it is
    // two cheap atomics); when `maxmemory == 0` (the default) the gate is never consulted, so this
    // adds nothing to the default hot path.
    let process_memory = Arc::new(ironcache_observe::ProcessMemoryGauge::new());

    // The process-GLOBAL live-connection gate (PROD-SAFETY #3, the `maxclients` connection-
    // exhaustion DoS fix): ONE per node, shared by `Arc` onto every shard's accept path so the
    // per-connection serve loop can reject a connection over the `maxclients` ceiling (read from the
    // runtime overlay) and release the slot on close. Created unconditionally (a single atomic); the
    // default ceiling is 10000 (Redis parity), so an unconfigured node is protected.
    let conn_gate = Arc::new(ironcache_observe::ConnectionGate::new());

    // The node-level OPERABILITY state (PROD-7): the SLOWLOG ring, the LATENCY monitor, and the
    // live-connection registry CLIENT KILL/PAUSE act through. ONE of each per node, shared by `Arc`
    // onto every shard's context. The SLOWLOG ring is seeded from the boot config's
    // `slowlog-log-slower-than` / `slowlog-max-len` so a node started with a configured threshold
    // keeps it; a `CONFIG SET slowlog-*` later mirrors into it (cmd_config). When the threshold is
    // -1 (disabled) the per-command hook short-circuits on one relaxed atomic load, so the default
    // hot path is byte-unchanged.
    let slowlog = Arc::new(ironcache_observe::SlowLog::with_config(
        config.slowlog_log_slower_than,
        config.slowlog_max_len,
    ));
    let latency = Arc::new(ironcache_observe::LatencyMonitor::new());
    let clients = Arc::new(ironcache_observe::ClientRegistry::new());
    let hotkeys = Arc::new(ironcache_observe::Hotkeys::new());

    // Static, cheaply-cloned server context shared by value onto each shard. The
    // mutable cross-shard state is ONLY the runtime cell (an Arc); the rest is
    // immutable, so cloning per shard does not violate shared-nothing.
    let ctx_template = ServerContext {
        runtime,
        acl,
        boot: config.clone(),
        databases: config.databases,
        shards: config.shards,
        info: ServerInfo {
            tcp_port: config.port,
            shards: config.shards,
            pid: std::process::id(),
            // started_at is filled in per shard at boot via the shard's clock so
            // uptime is measured from when the shard's Env started.
            started_at: ironcache_env::Monotonic::ZERO,
            maxmemory: config.maxmemory,
            maxmemory_policy: policy_name,
            mem_allocator: GLOBAL_ALLOCATOR_NAME,
            // The boot-generated 40-hex node id (identical across shards) and the boot
            // cluster-mode flag (CLUSTER_CONTRACT.md #70). Slice 1 is cluster-disabled, so
            // `cluster_enabled` is `false` in practice, but it is sourced from config.
            cluster_node_id,
            cluster_enabled: config.cluster_enabled,
        },
        // The boot-resolved static slot map (None unless cluster mode + a topology). Shared
        // by Arc-clone into every shard's context (immutable after boot in static mode; the
        // shared map written by the control plane in raft-mode; ADR-0002).
        cluster: cluster.clone(),
        // The Raft control-plane handle (HA-4c): `Some` only in raft-governance mode, else
        // `None` (the default static path). Cloned by value onto every shard's context; the
        // clone is the cheap `Send` inbox/status handle, not the `!Send` engine.
        raft: raft.clone(),
        // The node-level replication status cell (HA-7e), `Some` only in raft-mode. Cloned by
        // Arc onto every shard's context so any shard serving INFO / CLUSTER SHARDS reads the
        // same cell the repl tasks publish to.
        repl_status: repl_status.clone(),
        // The source-side in-sync-replica count (ADR-0026 write-side guardrail), `Some` only in
        // raft-mode. Cloned by Arc onto every shard's context (the write path reads it) AND handed
        // to the per-replica serve tasks (which maintain it); the same cell, lock-free.
        in_sync_replicas: in_sync_replicas.clone(),
        // The PER-BOOT replication history token (the resume identity), `Some` only in raft-mode (the
        // only mode that serves the live incremental resume). Generated ONCE at boot through the
        // determinism seam (drawn from `boot_env`'s RNG, ADR-0003), so it is a NEW value on every
        // restart while `cluster_node_id` (the stable identity) is unchanged. The primary advertises
        // it in `FullSync`; a reconnecting replica re-advertises the token it last synced under, and
        // the primary resumes ONLY on an exact match -> a restart forces a full re-sync (no silent
        // divergence). `None` on the default static path (it never serves the live resume).
        repl_history_id,
        // The per-shard metrics registry (OBSERVABILITY.md, #152), `Some` only when the `/metrics`
        // endpoint is enabled. Moved into the template (a cheap `Arc<Vec<_>>`), then cloned per
        // shard via `ctx_template.clone()` so each shard adopts its cell by index at boot; `None`
        // on the default path (byte-identical). The caller keeps its own registry handle.
        metrics_registry,
        // The shared persistence-stats cell (last-save + dirty), `Some` only when a data_dir is
        // configured. Cloned by Arc onto every shard's context so any shard serving INFO reads the
        // same live atomics the persistence path writes (durability footgun fix #5). `None` on the
        // default persistence-off path -> INFO renders the honest persistence-disabled section.
        persist_stats: persist_stats.clone(),
        // The process-global allocator-memory gauge (PROD-SAFETY #1/#2): the SAME `Arc` cloned onto
        // every shard's context, so each shard's admission gate reads the figure the shards'
        // periodic expiry ticks publish into it.
        process_memory: process_memory.clone(),
        // The process-global live-connection gate (PROD-SAFETY #3): the SAME `Arc` cloned onto every
        // shard's context, so every shard's accept path enforces the one node-level `maxclients` cap.
        conn_gate: conn_gate.clone(),
        // The node-level operability state (PROD-7): the SAME `Arc`s cloned onto every shard's
        // context, so the SLOWLOG command reads/resets the same ring the per-command timing hook
        // populates, the LATENCY command reads the same monitor, and CLIENT KILL/PAUSE/LIST operate
        // over the one node-wide connection registry every connection registers itself in.
        slowlog: slowlog.clone(),
        latency: latency.clone(),
        clients: clients.clone(),
        hotkeys: hotkeys.clone(),
    };
    let default_proto = if config.default_resp3 {
        ProtoVersion::Resp3
    } else {
        ProtoVersion::Resp2
    };

    // The cross-shard coordinator substrate (COORDINATOR.md #107): one bounded inbound
    // queue PER shard. `inbox` (the shared senders) is captured into the per-connection
    // serve closure so any home core can route a single-key command to the shard that
    // OWNS the key; `rxs` (the matching receivers, one per shard, in shard-index order)
    // are handed to `run_shards`, which moves each into its shard's drain loop. With
    // shards == 1 every key is home-owned, so the queues carry no traffic and the path is
    // byte-identical to before this layer (verified by the coordinator_stage1 parity test).
    let total = config.shards.max(1);
    let (inbox, rxs) = coordinator::build_inboxes(total);

    // Clone the (immutable-after-boot) context for the drain closure BEFORE the serve
    // closure moves `ctx_template` in. Each shard's drain loop gets this clone so it has
    // the admission budget / policy generation / databases it needs to run remote keyed
    // work; the per-connection serve closure clones the original per connection.
    let drain_ctx = ctx_template.clone();

    // A clone of the persistence handle (created above, before the context) returned in
    // `BootHandles` so the `/metrics` last-save + dirty gauges read the live persistence atomics.
    // `None` when persistence is off (no data_dir), in which case those gauges report 0. `persist`
    // itself is cloned into the serve/drain closures below.
    let persist_for_handles = persist.clone();

    // Clones for the OPTIONAL io_uring serve closure (PROD-10 / #28), captured BEFORE the tokio
    // `serve` closure moves `ctx_template` in. These feed the io_uring per-connection serve loop
    // when (and only when) the io_uring backend is selected at the bootstrap-selection branch
    // below; on the default / non-Linux / no-feature build they are an extra cheap clone bound to
    // `_` there (no behavior change to the tokio path).
    let ctx_template_for_uring = ctx_template.clone();
    let inbox_for_uring = inbox.clone();
    let persist_for_uring = persist.clone();

    // EMBEDDED TRANSPORT TLS for the CLIENT listener (#105, docs/design/TLS.md). Build the rustls
    // acceptor ONCE at boot when `tls = on`, from the configured cert/key PEM, and clone the cheap
    // (Arc-inside) handle into every connection's serve closure. When `tls = off` (the DEFAULT)
    // this is `None` and the serve path returns a PLAIN TcpStream exactly as before -- no rustls,
    // no per-byte cost, byte-unchanged. A build failure here (unreadable / unparseable cert or key)
    // is a hard boot error: a TLS-only listener with no usable material would reject every client,
    // so we refuse to start rather than silently serve nothing. `Config::validate` already proved
    // the paths are present + readable, so this is the PEM-parse + rustls-acceptance step.
    let tls_acceptor: Option<tokio_rustls::TlsAcceptor> = if config.tls
        == ironcache_config::TlsMode::On
    {
        // validate() guaranteed both paths are Some + readable; expressing it as a clear error here
        // keeps the boot failure precise if a future path reaches this without validation.
        let cert = config.tls_cert_path.as_ref().ok_or_else(|| {
            anyhow::anyhow!("tls = on requires tls_cert_path (should have been caught by validate)")
        })?;
        let key = config.tls_key_path.as_ref().ok_or_else(|| {
            anyhow::anyhow!("tls = on requires tls_key_path (should have been caught by validate)")
        })?;
        let acceptor =
            ironcache_runtime::build_acceptor(&cert.to_string_lossy(), &key.to_string_lossy())
                .map_err(|e| anyhow::anyhow!("building the TLS listener: {e}"))?;
        tracing::info!(
            cert = %cert.display(),
            key = %key.display(),
            "ironcache: TLS enabled (rustls, server-auth) on the client listener"
        );
        Some(acceptor)
    } else {
        None
    };

    let serve = {
        let inbox = inbox.clone();
        let persist = persist.clone();
        // `run_shards` hands the shard's `TokioRuntime` backend; the per-connection serve loop
        // drives data I/O through the `ClientStream` (plain or TLS), and the shard's background
        // timer task constructs its own zero-sized backend, so this connection path no longer
        // needs the handle directly (the underscore keeps the run_shards closure shape).
        move |_rt: TokioRuntime, stream: tokio::net::TcpStream, shard: ShardId| {
            let ctx = ctx_template.clone();
            let inbox = inbox.clone();
            // Clone the cheap acceptor handle (Arc inside) per connection; `None` when tls = off,
            // in which case the serve loop takes the plaintext fast path.
            let tls_acceptor = tls_acceptor.clone();
            // The node-level persistence state (#58), `Some` only when a data_dir is configured.
            let persist = persist.clone();
            async move {
                serve_connection(
                    stream,
                    shard,
                    ctx,
                    default_proto,
                    inbox,
                    tls_acceptor,
                    persist,
                )
                .await;
            }
        }
    };

    // The per-shard drain closure: turn a shard's (index, receiver) into its drain-loop future.
    // run_shards spawns it on each shard's LocalSet alongside the accept loop, BEFORE accepting
    // (a shard can own keys without ever accepting a connection). The shard INDEX is passed so the
    // drain loop can LOAD this shard's snapshot file at boot (#58) and shard 0 can host the periodic
    // save timer.
    let drain = {
        let inbox = inbox.clone();
        // The readiness countdown (OBSERVABILITY.md, #152): MOVED into the drain closure (then cloned
        // per shard via the closure's own `Clone`) so each shard signals one unit AFTER its
        // load-on-boot completes. `None` when the metrics endpoint is off (byte-identical: the drain
        // loop's signal is a no-op). `run_server_observed` does not use `ready` after this point.
        move |index: usize,
              rx: tokio::sync::mpsc::Receiver<coordinator::ShardWork>,
              shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>| {
            let ctx = drain_ctx.clone();
            let inbox = inbox.clone();
            let persist = persist.clone();
            let ready = ready.clone();
            // The shutdown flag (the SAME one the signal handler flips) lets shard 0's drain loop
            // drive the SAVE-ON-EXIT (#139) when a SIGTERM/SIGINT-triggered stop begins. `ready`
            // is the per-shard readiness countdown (#152): this shard decrements it once its
            // load-on-boot has finished, so `/readyz` flips to 200 only after EVERY shard loaded.
            coordinator::run_drain_loop(index, rx, ctx, inbox, persist, shutdown, ready)
        }
    };

    // Capture the runtime-config handle (for the live `maxmemory` metrics gauge) BEFORE
    // `run_shards` consumes the serve/drain closures. `runtime` moved into `ctx_template`, so
    // read the clone off the template's Arc; `persist` is cloned (it was cloned into the
    // closures above, and `from_config` returns `None` when persistence is off).
    let runtime_handle = ctx_template_runtime.clone();
    let persist_handle = persist_for_handles;

    // RUNTIME BACKEND SELECTION (PROD-10 / #28). The DEFAULT (`runtime = tokio`, and the only
    // option in the default no-feature build / off Linux) drives the per-shard bootstrap on the
    // portable tokio backend, byte-unchanged. `runtime = io_uring` is honored ONLY when ALL of:
    //   * the binary was built `--features io_uring`,
    //   * the target is Linux, and
    //   * TLS is OFF (rustls does not compose with io_uring in v1; see below),
    // are true; in every other case we LOG a one-line fallback and use the tokio backend, so
    // selecting io_uring can never fail to start a node. The io_uring path uses a dedicated,
    // seam-driven plaintext serve loop ([`serve_connection_uring`]) that REUSES the same
    // route+dispatch engine through the `Runtime` owned-buffer recv/send. The selection is inline
    // (not a helper fn) so the concrete `serve` / `drain` closures keep their inferred opaque
    // types and need no nameable trait alias.
    let want_io_uring = config.runtime == ironcache_config::RuntimeBackend::IoUring;

    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    let set = if want_io_uring && config.tls != ironcache_config::TlsMode::On {
        tracing::info!(
            "runtime = io_uring: using the Linux io_uring datapath (plaintext); the \
             registered-buffer / multishot fast path and a perf benchmark are deferred to a \
             Linux soak (no throughput claim made)"
        );
        let uring_serve = {
            let ctx_template = ctx_template_for_uring;
            let inbox = inbox_for_uring;
            let persist = persist_for_uring;
            move |rt: ironcache_runtime::IoUringRuntime,
                  stream: ironcache_runtime::UringTcpStream,
                  shard: ShardId| {
                let ctx = ctx_template.clone();
                let inbox = inbox.clone();
                let persist = persist.clone();
                async move {
                    serve_connection_uring(rt, stream, shard, ctx, default_proto, inbox, persist)
                        .await;
                }
            }
        };
        ironcache_runtime::run_shards_uring(&shard_cfg, uring_serve, rxs, drain)?
    } else {
        if want_io_uring {
            // io_uring + TLS do not compose in v1 (rustls drives tokio AsyncRead/AsyncWrite, not
            // io_uring submissions). Refusing would break a TLS deployment that asked for io_uring;
            // FALL BACK to tokio (which serves TLS) and log it, never breaking TLS.
            tracing::warn!(
                "runtime = io_uring requested with TLS on; the io_uring datapath does not support \
                 TLS in v1 -- falling back to the tokio backend for this node"
            );
        }
        ironcache_runtime::bootstrap::run_shards(&shard_cfg, serve, rxs, drain)?
    };

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    let set = {
        if want_io_uring {
            // Requested but this build/target cannot provide it: fall back to tokio with a clear
            // one-line log (never a boot failure). The `_for_uring` captures are unused on this
            // build; bind them to `_` so the no-feature build has no dead-code warning.
            tracing::warn!(
                "runtime = io_uring requested, but this build is not a Linux build with the \
                 `io_uring` feature; falling back to the tokio backend"
            );
        }
        let _ = (
            &ctx_template_for_uring,
            &inbox_for_uring,
            &persist_for_uring,
        );
        ironcache_runtime::bootstrap::run_shards(&shard_cfg, serve, rxs, drain)?
    };

    // The structured-topology read state (#365), captured from the finalized cluster map + node
    // identity (raft_mode read before `raft` is moved into the handles below).
    let topology = crate::topology::TopologyHandle {
        node_id: cluster_node_id,
        cluster_enabled: config.cluster_enabled,
        raft_mode: raft.is_some(),
        tcp_port: config.port,
        shards: config.shards,
        cluster: cluster.clone(),
        repl_status: repl_status.clone(),
    };

    Ok(BootHandles {
        set,
        raft,
        persist: persist_handle,
        runtime: runtime_handle,
        topology,
    })
}

thread_local! {
    // The shard's core-local state. Created lazily on first use on each shard
    // thread; never shared across threads.
    static SHARD: RefCell<Option<Rc<RefCell<ShardState>>>> = const { RefCell::new(None) };
    // This shard's PRE-ALLOCATED metrics counter cell (OBSERVABILITY.md, #152), adopted at shard
    // boot from the process-wide `MetricsRegistry` by shard index. Set ONCE per shard thread by
    // [`adopt_metrics_cell`] (called from the drain-loop boot AND the first connection, both of
    // which know this shard's index) BEFORE [`shard_state`] first builds the `ShardState`, so the
    // shard's `ShardCounters` mutate the SAME cell the out-of-band metrics task reads across
    // threads. `None` on the DEFAULT path (no `--metrics-addr`): `shard_state` then builds a
    // standalone counter cell, byte-identical to before this feature.
    static METRICS_CELL: RefCell<Option<std::sync::Arc<ironcache_observe::ShardCountersCell>>> =
        const { RefCell::new(None) };
    // The process-GLOBAL allocator-memory gauge (PROD-SAFETY #1/#2): the SHARED node-level
    // `Arc<ProcessMemoryGauge>` from the server context, ADOPTED per shard at boot so this shard's
    // periodic expiry tick can PUBLISH the latest jemalloc figure into it OFF the command hot path.
    // The admission gate reads the same gauge (via the context) to drive the over-limit trigger off
    // REAL process memory. `None` until adopted (the first connection / drain-loop boot adopts it);
    // when unadopted the tick simply does not publish (and the gate's fallback to the per-shard
    // logical counter keeps the default path byte-unchanged).
    static PROCESS_MEMORY_GAUGE: RefCell<Option<std::sync::Arc<ironcache_observe::ProcessMemoryGauge>>> =
        const { RefCell::new(None) };
    // The shard's per-shard store: the per-DB hashbrown kvobj map (ADR-0005) wired
    // with the configured eviction policy. Held as Rc<RefCell<..>> exactly like ENV,
    // so it is core-local and unsynchronized; created lazily per shard thread. The
    // concrete ShardStore implements the Store + Admit waist traits the generic
    // dispatch runs against.
    static STORE: RefCell<Option<Rc<RefCell<ShardStoreImpl>>>> = const { RefCell::new(None) };
    // The shard's per-shard TTL timing wheel (#51), held as Rc<RefCell<..>> exactly
    // like STORE/ENV so it is core-local and unsynchronized (ADR-0002/0005). The
    // active drain pops due keys from it before each command; TTL-setting commands
    // register deadlines into it. Created lazily per shard thread.
    static WHEEL: RefCell<Option<Rc<RefCell<TimingWheel>>>> = const { RefCell::new(None) };
    // One SystemEnv per shard thread (the sanctioned real-time boundary). It is
    // wrapped in a RefCell so the determinism seam's RNG half is REACHABLE: the
    // shard is single-threaded (current-thread runtime, !Send tasks), so clock
    // reads go through `.borrow()` and `Env::rng` through `.borrow_mut()` with no
    // cross-core synchronization. A bare `Rc<SystemEnv>` would make `.rng()`
    // (which needs `&mut self`) structurally uncallable; PR-2/PR-3 need RNG on the
    // hot path (S3-FIFO sampling, TTL jitter).
    static ENV: RefCell<Option<Rc<RefCell<SystemEnv>>>> = const { RefCell::new(None) };
    static STARTED_AT: RefCell<Option<ironcache_env::Monotonic>> = const { RefCell::new(None) };
    // Whether THIS shard thread has already spawned its background active-expiry timer
    // task (PR-3c). Spawned exactly ONCE per shard, lazily on the first connection (the
    // shard's tokio LocalSet must exist, which it does once a connection is being
    // served), so an idle shard that has had at least one connection still reclaims
    // expired memory with no further commands. A plain Cell suffices (single-threaded
    // per shard; shared-nothing ADR-0002).
    static EXPIRE_TASK_SPAWNED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    // Whether THIS shard is a PASSIVE REPLICA (HA-7d, CARRY-FORWARD 2). A replica's store is
    // a faithful mirror of the slot OWNER's: it must apply key removals ONLY from the
    // replication stream (via `ReplicaApplier`), NEVER from its OWN active-expiry reaper or
    // capacity eviction -- else it would independently drop keys the primary still holds and
    // DIVERGE from the primary. Set `true` by [`crate::replica_attach`] when this shard
    // becomes a committed replica (the atomic store swap point), and checked at the TOP of
    // [`expire_cycle_tick`] (the background reaper) which returns 0 immediately when passive.
    // A plain `Cell` suffices (single-threaded per shard; shared-nothing ADR-0002). DEFAULTS
    // `false`, so the non-replica path is byte-unchanged: the reaper runs exactly as before
    // and this Cell is only ever read (one bool load) on a path that already borrows the
    // shard state. The eviction/admission removal path is unreachable on a replica for a
    // separate reason (documented on `set_replica_passive`): a replica never serves the
    // owner's WRITE path, so no admission/evict runs there; removals arrive only via the
    // stream. This guard closes the one remaining self-removal source (the timer reaper).
    static REPLICA_PASSIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    // The shard's PER-SHARD Pub/Sub subscription table (SERVER_PUSH.md #20, PR 91a): channel
    // -> {conn id -> push sender}. Core-local (per shard, shared-nothing ADR-0002) with NO
    // lock; held as Rc<RefCell<..>> exactly like STORE/WHEEL/ENV so a connection task, the
    // coordinator drain loop's `__ICPUBLISH` delivery, and the disconnect cleanup all reach
    // the SAME table on this shard. Created lazily per shard thread. The only cross-core
    // handle it stores is the `Send` mpsc push sender of each subscriber (a PUBLISH fans out
    // to every shard, so each shard renders to its own connections from its own table).
    static PUBSUB: RefCell<Option<Rc<RefCell<crate::pubsub::ShardPubSub>>>> =
        const { RefCell::new(None) };
    // The shard's PER-SHARD CLIENT TRACKING invalidation table (#409): key -> {conn id -> push
    // handle}, the keys tracking clients READ on this (owner) shard. Core-local (per shard,
    // shared-nothing ADR-0002), NO lock, held as Rc<RefCell<..>> exactly like PUBSUB: a tracking
    // client's read registers here, a write to the key on the SAME owner shard invalidates here,
    // and the stored `Send` push sender delivers the invalidation cross-core to the client's conn.
    // Created lazily per shard thread; empty until a tracking client reads (zero non-tracking cost).
    static TRACKING: RefCell<Option<Rc<RefCell<crate::pubsub::ShardTracking>>>> =
        const { RefCell::new(None) };
    // The shard's PER-SHARD BLOCKING-WAITER registry (PROD-9): `(db, key)` -> a FIFO queue of
    // parked connections. Core-local (per shard, shared-nothing ADR-0002) with NO lock; held as
    // Rc<RefCell<..>> exactly like PUBSUB/STORE/WHEEL so a connection that PARKS, a pusher that
    // WAKES a waiter, and the RAII deregister all reach the SAME table on this shard. Created
    // lazily per shard thread. The only cross-core handle it stores is a `Send` `Arc<Notify>` per
    // waiter (the connection lives on this shard; the notify is shared so a wake from this shard's
    // pusher resumes it spin-free).
    static BLOCKING: RefCell<Option<Rc<RefCell<crate::blocking::ShardBlocking>>>> =
        const { RefCell::new(None) };
}

/// The shard's per-shard blocking-waiter registry handle (PROD-9), lazily created on first use
/// on this shard thread (mirrors [`shard_pubsub`]). A connection PARKS a [`crate::blocking::Waiter`]
/// here when a blocking pop finds every key empty; a push on this shard WAKES the longest-waiting
/// waiter. `pub(crate)` so the serve loop's wake path reaches the SAME table connections park into.
pub(crate) fn shard_blocking() -> Rc<RefCell<crate::blocking::ShardBlocking>> {
    BLOCKING.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(
                crate::blocking::ShardBlocking::default(),
            )));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

/// Mark THIS shard as a PASSIVE REPLICA (HA-7d, CARRY-FORWARD 2), or clear the mark.
///
/// `pub(crate)` so [`crate::replica_attach`] sets it `true` at the ATOMIC STORE SWAP point
/// (when this shard adopts the owner's full-sync) and clears it back to `false` if the shard
/// ever stops being a replica (a future role change / teardown). Once set, the background
/// active-expiry reaper ([`expire_cycle_tick`]) is INERT on this shard (returns 0 without
/// touching the store), so the replica never independently reaps a key the owner still holds.
///
/// ## Why this is the only self-removal source to gate
///
/// A passive replica must remove keys ONLY from the replication stream (via
/// `ironcache_repl::ReplicaApplier`), so its keyspace stays byte-identical to the owner's.
/// There are exactly THREE places the store removes a key on its OWN initiative:
/// 1. the BACKGROUND active-expiry timer ([`spawn_expire_task`] -> [`expire_cycle_tick`]) --
///    gated HERE (the reaper returns 0 when passive);
/// 2. the OPPORTUNISTIC per-command active-expiry drain + lazy-expiry probe -- reached only on
///    the COMMAND path, which a replica connection never drives for its replicated slots (a
///    READONLY read returns the value; a write returns `-MOVED` to the owner before any store
///    borrow), and the cross-shard drain loop only runs work the coordinator routes to an
///    OWNED slot, never a replicated one;
/// 3. capacity EVICTION on the ADMISSION path -- reached only on the owner's WRITE path, which
///    a replica never serves (a write to a replicated slot is `-MOVED` to the owner).
///
/// So gating the timer reaper here, plus the structural fact that a replica never serves the
/// write/admission path, makes the replica store removal-passive end to end. The applier's
/// own `delete` (from a `StreamDel`) is the SANCTIONED removal and is unaffected.
pub(crate) fn set_replica_passive(passive: bool) {
    REPLICA_PASSIVE.with(|c| c.set(passive));
}

/// Whether THIS shard is currently a passive replica (HA-7d, CARRY-FORWARD 2). Defaults
/// `false` (the non-replica path), a single `Cell` bool load. `pub(crate)` for
/// [`crate::replica_attach`] to read its own attach state and for the reaper guard.
#[must_use]
pub(crate) fn is_replica_passive() -> bool {
    REPLICA_PASSIVE.with(std::cell::Cell::get)
}

/// The shard's per-shard Pub/Sub subscription table handle (SERVER_PUSH.md #20, PR 91a),
/// lazily created on first use on this shard thread (mirrors [`shard_store`] / [`shard_state`]).
/// `pub(crate)` so the [`crate::coordinator`] `__ICPUBLISH` delivery reaches the SAME table the
/// connection tasks register into.
pub(crate) fn shard_pubsub() -> Rc<RefCell<crate::pubsub::ShardPubSub>> {
    PUBSUB.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(crate::pubsub::ShardPubSub::default())));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

/// This shard's CLIENT TRACKING invalidation table (#409), lazily created on first use on this
/// shard thread (mirrors [`shard_pubsub`]). `pub(crate)` so the read-registration hook, the
/// write-invalidation hook, and the disconnect cleanup all reach the SAME per-shard table.
pub(crate) fn shard_tracking() -> Rc<RefCell<crate::pubsub::ShardTracking>> {
    TRACKING.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(
                crate::pubsub::ShardTracking::default(),
            )));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

/// Spawn the per-shard BACKGROUND active-expiry timer task ONCE on this shard's
/// executor (EXPIRATION.md idle-shard memory boundedness, PR-3c). Idempotent per shard:
/// guarded by [`EXPIRE_TASK_SPAWNED`] so repeated connections do not spawn duplicates.
///
/// The task loops: `rt.timer(EXPIRE_CYCLE_INTERVAL).await` (the Runtime timer SEAM, NOT
/// `tokio::time` directly, ADR-0003), then reads `now` from the shard's Env clock (NOT
/// std time) and drains a BOUNDED batch from the wheel via the SAME [`drain_due_keys`]
/// helper the opportunistic per-command path uses. The reclaimed count folds into the
/// shard's `expired_keys` counter so idle reclamation shows up in INFO alongside the
/// command-path drain.
///
/// ## Borrow discipline (critical, ADR-0002/0005)
///
/// Each tick borrows the per-shard ENV / STORE / WHEEL / STATE RefCells ONLY briefly
/// and DROPS every borrow BEFORE the next `.await`. A RefCell borrow held across an
/// await would double-borrow-panic when a concurrently-scheduled command handler runs
/// on the same single thread between the timer firing and resuming. The tick body is a
/// single non-async block (`expire_cycle_tick`) that takes and releases all borrows and
/// returns a plain `u64`, so no `Ref`/`RefMut` is alive when the loop awaits the timer.
/// Bring up THIS shard's background tasks at shard boot: lazily init the per-shard
/// store/wheel/env/state handles and spawn the active-expiry timer task ONCE.
///
/// Called from the coordinator's per-shard drain-loop setup at SHARD BOOT (not on the
/// first connection), because a shard can now OWN keys (and so need active expiry) even
/// if it never accepts a connection (COORDINATOR.md #107 partitions the keyspace across
/// shards). It is idempotent (the spawn is guarded by [`EXPIRE_TASK_SPAWNED`]) and runs
/// on the shard's LocalSet (the drain loop is spawned there), which is exactly what
/// `spawn_on_shard` needs. `databases`/`policy_name` are the boot facts the store
/// lazy-init needs (the same values `serve_connection` passes).
///
/// The [`TokioRuntime`] backend is zero-sized (it carries no state; the shard's tasks
/// live on the LocalSet), so it is constructed here rather than threaded in.
pub(crate) fn ensure_shard_started(
    databases: u32,
    policy_name: &str,
    reserved_bits: u32,
    runtime: Arc<ironcache_config::RuntimeConfig>,
) {
    let env = shard_env();
    let store_rc = shard_store(databases, policy_name, reserved_bits);
    let wheel_rc = shard_wheel();
    let state_rc = shard_state();
    spawn_expire_task(
        TokioRuntime::new(),
        env,
        store_rc,
        wheel_rc,
        state_rc,
        runtime,
    );
}

fn spawn_expire_task(
    rt: TokioRuntime,
    env: Rc<RefCell<SystemEnv>>,
    store_rc: Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: Rc<RefCell<TimingWheel>>,
    state_rc: Rc<RefCell<ShardState>>,
    runtime: Arc<ironcache_config::RuntimeConfig>,
) {
    if EXPIRE_TASK_SPAWNED.with(std::cell::Cell::get) {
        return;
    }
    EXPIRE_TASK_SPAWNED.with(|c| c.set(true));
    rt.spawn_on_shard(async move {
        loop {
            // Await the cycle interval through the Runtime timer seam (NOT tokio::time
            // directly). No RefCell borrow is held across this await.
            rt.timer(EXPIRE_CYCLE_INTERVAL).await;
            // One tick: take + release all borrows inside this call, returning a u64.
            // Nothing borrowed survives to the next await iteration. The runtime `Arc` is
            // read for the DEBUG SET-ACTIVE-EXPIRE gate (#411); the clone is one shard-local
            // owned handle, never re-cloned per tick.
            expire_cycle_tick(&env, &store_rc, &wheel_rc, &state_rc, &runtime);
        }
    });
}

/// Run ONE background active-expiry cycle: read `now` from the Env clock, drain a
/// bounded batch from the wheel (reusing [`drain_due_keys`]), and fold the reclaimed
/// count into the shard's `expired_keys` counter. Returns the number of keys reaped
/// (for the wiring smoke test).
///
/// This is a SYNCHRONOUS function: it acquires every RefCell borrow and releases it
/// before returning, so the async caller never holds a borrow across an `.await` (the
/// borrow-discipline contract above). The clock read (`env.borrow()`) and the
/// store/wheel mutation (separate RefCells) do not alias.
fn expire_cycle_tick(
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
    runtime: &ironcache_config::RuntimeConfig,
) -> u64 {
    // CARRY-FORWARD 2 (PASSIVE replica, HA-7d): a replica shard must NOT run its own active
    // expiry -- it would independently reap keys the slot OWNER still holds and DIVERGE. When
    // this shard is a passive replica, the reaper is INERT: return 0 BEFORE taking any store /
    // wheel borrow (a single `Cell` bool load). Removals on a replica arrive ONLY from the
    // replication stream (`ReplicaApplier`). DEFAULTS `false`, so the non-replica path is
    // byte-unchanged (one bool test, then the identical reap below). See `set_replica_passive`.
    if is_replica_passive() {
        return 0;
    }
    // DEBUG SET-ACTIVE-EXPIRE (#411): when the node disabled the active-expiry cycle, this
    // background reaper is INERT too (return 0 before any borrow), so only LAZY reap-on-access
    // removes a key -- the conformance contract. One relaxed load, default-true so the common
    // path is byte-unchanged. The flag lives in the per-node runtime `Arc`, so a toggle on any
    // connection reaches every shard's tick.
    if !runtime.active_expire_enabled() {
        return 0;
    }
    // The WORK (which keys are due) is decided by the Env clock (ADR-0003), so a DST
    // replay reaps the identical keys; only the FIRING schedule is wall-clock.
    let now = UnixMillis(env.borrow().now_unix_millis());
    let reaped = {
        let mut store = store_rc.borrow_mut();
        let mut wheel = wheel_rc.borrow_mut();
        // The `&mut *` derefs THROUGH the RefMut to the concrete ShardStore/TimingWheel
        // the generic `drain_due_keys` bound needs (a bare `&mut wheel` would be
        // `&mut RefMut<..>`, which does not satisfy `S: Store + ActiveExpiry`). The
        // deref is load-bearing, so the auto-deref lint is silenced here.
        #[allow(clippy::explicit_auto_deref)]
        drain_due_keys(&mut *wheel, &mut *store, now, MAX_RECLAIM_PER_CYCLE)
        // store + wheel borrows DROP here, before the state borrow below and before
        // the caller's next await.
    };
    if reaped > 0 {
        let deltas = CounterDeltas {
            expired: reaped,
            ..CounterDeltas::default()
        };
        state_rc.borrow_mut().counters.apply(deltas);
    }
    // Publish THIS shard's live key count into its metrics cell (OBSERVABILITY.md, #152), a GAUGE
    // store OFF the command hot path (this is the periodic reaper, not a command). A no-op when
    // `/metrics` is disabled (no adopted cell); when enabled the `/metrics` keyspace gauge is
    // refreshed every expiry cycle (eventually-consistent, zero per-command cost). The brief
    // `store.len()` read (a sum over the per-DB lengths) and one relaxed atomic store do not touch
    // the command path.
    publish_keyspace_keys(store_rc.borrow().len() as u64);
    // Refresh the PROCESS-GLOBAL allocator-memory gauge (PROD-SAFETY #1/#2) OFF the command hot
    // path: read the jemalloc `(allocated, resident)` pair once and publish it so the maxmemory
    // admission gate decides over-limit off REAL process memory (the figure that bounds RSS), not
    // the logical counter that undercounts ~2x. A no-op when the gauge is unadopted. This runs on
    // EVERY shard's tick (each shard publishes the same node-global figure), which is harmless: the
    // last writer wins and the value is a fuzzy, eventually-consistent snapshot by design.
    refresh_process_memory_gauge();
    reaped
}

pub(crate) fn shard_state() -> Rc<RefCell<ShardState>> {
    SHARD.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            // Build the shard's counters over its ADOPTED registry cell when the metrics endpoint
            // is enabled (so the shard's mutations land in the cell the metrics task reads across
            // threads), else over a fresh standalone cell (the default, byte-identical path).
            let counters = METRICS_CELL.with(|c| {
                c.borrow().as_ref().map_or_else(ShardCounters::new, |cell| {
                    ShardCounters::with_cell(std::sync::Arc::clone(cell))
                })
            });
            *b = Some(Rc::new(RefCell::new(ShardState {
                next_client_id: 1,
                counters,
                command_stats: ironcache_observe::CommandStats::default(),
                // Start at 0 (the RuntimeConfig generation also starts at 0): the first
                // CONFIG SET maxmemory-policy bumps it, and this shard notices on its
                // next command.
                last_policy_generation: 0,
            })));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

/// Adopt THIS shard's pre-allocated metrics cell from the registry by index (OBSERVABILITY.md,
/// #152), so the shard's [`ShardCounters`] mutate the SAME cell the out-of-band `/metrics` task
/// reads across threads. Idempotent and a no-op when metrics are disabled (`registry` is `None`)
/// or already adopted; MUST run BEFORE [`shard_state`] first builds the `ShardState` (the
/// drain-loop boot and the first connection both call it with this shard's index).
pub(crate) fn adopt_metrics_cell(
    registry: Option<&ironcache_observe::MetricsRegistry>,
    shard_index: usize,
) {
    let Some(registry) = registry else { return };
    METRICS_CELL.with(|c| {
        let mut b = c.borrow_mut();
        if b.is_none() {
            *b = Some(registry.shard_cell(shard_index));
        }
    });
}

/// Publish THIS shard's live key count into its adopted metrics cell (OBSERVABILITY.md, #152), a
/// GAUGE store off the command hot path. Called from the periodic active-expiry tick, so the
/// `/metrics` keyspace gauge is eventually-consistent (bounded by the expiry cycle) at zero
/// per-command cost. A no-op when metrics are disabled (no adopted cell).
fn publish_keyspace_keys(keys: u64) {
    METRICS_CELL.with(|c| {
        if let Some(cell) = c.borrow().as_ref() {
            cell.set_keyspace_keys(keys);
        }
    });
}

/// Adopt THIS shard's reference to the SHARED process-global allocator-memory gauge (PROD-SAFETY
/// #1/#2), so the shard's periodic expiry tick can publish the latest jemalloc figure into the
/// SAME gauge the admission gate reads via the context. Idempotent (a no-op once adopted); MUST run
/// before the first expiry tick. Both the drain-loop boot and the first connection call it with the
/// node-level gauge from the context.
pub(crate) fn adopt_process_memory_gauge(
    gauge: &std::sync::Arc<ironcache_observe::ProcessMemoryGauge>,
) {
    PROCESS_MEMORY_GAUGE.with(|c| {
        let mut b = c.borrow_mut();
        if b.is_none() {
            *b = Some(std::sync::Arc::clone(gauge));
        }
    });
}

/// Publish the latest PROCESS-GLOBAL allocator figure into the adopted gauge (PROD-SAFETY #1/#2),
/// OFF the command hot path (called from the periodic active-expiry tick). Reads the jemalloc
/// `(allocated, resident)` pair via the store's mallctl ONCE per cycle and stores it, so the
/// maxmemory admission gate sees a live (eventually-consistent, bounded by the expiry cycle)
/// process-memory figure without ever advancing the jemalloc epoch per command. A no-op when the
/// gauge is unadopted (the default path before the first connection, or a build with no allocator
/// to query, where `process_memory()` reports 0 and the gate falls back to the logical counter).
fn refresh_process_memory_gauge() {
    PROCESS_MEMORY_GAUGE.with(|c| {
        if let Some(gauge) = c.borrow().as_ref() {
            let (used_memory, used_memory_rss) = process_memory();
            gauge.publish(used_memory, used_memory_rss);
        }
    });
}

/// The number of LOW `scan_hash` bits the cross-shard composite SCAN cursor must reserve
/// for the shard index, given the total shard count (COORDINATOR.md #107, FIX 1). `0` for
/// a single (or degenerate zero) shard server -- SCAN is then byte-identical to the
/// pre-coordinator behavior (the inner cursor passes through verbatim) -- and
/// [`ScanCursor::SHARD_BITS`] when more than one shard is configured, so `scan_step`
/// returns BAND-ALIGNED next cursors the composite cursor round-trips losslessly.
pub(crate) fn scan_reserved_bits(total_shards: usize) -> u32 {
    if total_shards > 1 {
        ScanCursor::SHARD_BITS
    } else {
        0
    }
}

/// Build a FRESH [`ShardStoreImpl`] with this shard's configured eviction policy, accounting,
/// and scan-band width, WITHOUT caching it in the thread-local (unlike [`shard_store`], which
/// builds-once-and-caches the LIVE store).
///
/// This is the constructor the HA-7d replica attach hands to `receive_full_sync` as its
/// `make_store` argument. The temp store a full-sync loads into must be the SAME concrete type
/// the live serve path uses, so an ATOMIC SWAP of it into the live `STORE` handle is
/// type-identical and behaves the same (same Policy from the configured name, same
/// `CountingAccounting`, same scan-band bits). It shares the build logic with [`shard_store`]
/// so the two never drift. `pub(crate)` for [`crate::replica_attach`].
pub(crate) fn fresh_shard_store(
    databases: u32,
    policy_name: &str,
    reserved_bits: u32,
) -> ShardStoreImpl {
    // Build the shard's eviction policy from the configured name, seeding the Random variant
    // from THIS shard's Env RNG (ADR-0003: no std rand; the seed comes through the determinism
    // seam). The name was validated at config time, so map_policy_name cannot return None here;
    // fall back to the cache default defensively if a future un-validated path slips in.
    let seed = shard_env().borrow_mut().rng().next_u64();
    let policy = map_policy_name(policy_name, seed).unwrap_or_else(Policy::cache_default);
    // The reserved-band width makes `scan_step` return band-aligned next cursors for the
    // cross-shard composite cursor (0 on a single-shard server, so SCAN stays byte-identical to
    // before the coordinator layer; FIX 1).
    ShardStore::with_hooks(databases, policy, CountingAccounting::new())
        .with_scan_band_bits(reserved_bits)
}

pub(crate) fn shard_store(
    databases: u32,
    policy_name: &str,
    reserved_bits: u32,
) -> Rc<RefCell<ShardStoreImpl>> {
    STORE.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(fresh_shard_store(
                databases,
                policy_name,
                reserved_bits,
            ))));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

pub(crate) fn shard_wheel() -> Rc<RefCell<TimingWheel>> {
    WHEEL.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(TimingWheel::new())));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

pub(crate) fn shard_env() -> Rc<RefCell<SystemEnv>> {
    ENV.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            let env = SystemEnv::new();
            // Record the shard's boot instant for uptime.
            STARTED_AT.with(|s| {
                use ironcache_env::Clock;
                *s.borrow_mut() = Some(env.now());
            });
            *b = Some(Rc::new(RefCell::new(env)));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

fn shard_started_at() -> ironcache_env::Monotonic {
    STARTED_AT.with(|s| s.borrow().unwrap_or(ironcache_env::Monotonic::ZERO))
}

/// One DEFERRED cross-shard hop (#8 overlap): a remote single-key command whose `ShardWork` was
/// enqueued but whose reply is not yet awaited, so the next command's hop can be issued while this
/// owner is still working. The serve loop parks a RUN of these and drains them together (in order)
/// at the next barrier / end of batch, encoding each reply into `out` and running its per-command
/// hooks then -- because those hooks read the reply bytes (`record_command_stats`/`record_hotkeys`),
/// they cannot run until the reply is materialized. Every field is the per-command state the hooks
/// need, captured at defer time.
///
/// KNOWN BEST-EFFORT EDGE (client tracking): the drain-time hooks read the deferred command's own
/// snapshotted `was_tracking`/`was_bcast`, but the CLIENT-TRACKING / CLIENT-CACHING hooks
/// (`apply_client_tracking`, `consume_caching_flag`) also read LIVE `conn` tracking state. If a
/// tracking-CONTROL command (`CLIENT CACHING`/`TRACKING`) is pipelined BETWEEN deferred remote hops
/// in the same batch, a hop's tracking hook can observe that control command's mutation (it runs as
/// a barrier before the drain). This only affects CLIENT-side tracking of REMOTE keys, which is
/// ALREADY documented single-shard-only / best-effort (see `apply_client_tracking`); it never
/// affects the data reply bytes or their wire order. Snapshotting the full tracking sub-state into
/// this struct is a clean follow-up; deferred here to keep the overlap focused on the FIFO property.
struct DeferredHop {
    /// The reply receiver (`None` = the owner was gone at send; `finish_hop` encodes shard-unavailable).
    rx: Option<coordinator::HopReceiver>,
    /// The request, for the hooks (cheap clone: `Request` is `Vec<Bytes>`, refcounted).
    request: Request,
    /// The monotonic start stamp for this command's elapsed-time (slowlog + commandstats).
    cmd_start: ironcache_env::Monotonic,
    /// Tracking-state snapshot taken BEFORE dispatch (so the tracking hook sees an ON->OFF flip).
    was_tracking: bool,
    was_bcast: bool,
    /// The slowlog threshold snapshot (negative = disabled) for this command.
    slow_threshold: i64,
    /// The connection's negotiated proto, to encode the reply on the home core.
    proto: ProtoVersion,
}

/// Drain a run of [`DeferredHop`]s (in FIFO order) into `out`: for each, await + encode its reply,
/// then run its per-command hooks (commandstats, hotkeys, client-tracking, caching, slowlog) exactly
/// as the inline path does -- so a deferred remote command is observably identical to a
/// non-deferred one, only its reply is assembled later (still in order). Called at every barrier and
/// at end of batch, so `out` stays strictly append-in-command-order (FIFO on the wire).
#[allow(clippy::too_many_arguments)]
async fn drain_deferred_hops(
    pending: &mut Vec<DeferredHop>,
    out: &mut Vec<u8>,
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    state_rc: &Rc<RefCell<ShardState>>,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
) {
    for d in pending.drain(..) {
        let out_before = out.len();
        coordinator::finish_hop(d.rx, out, d.proto).await;
        let cmd_elapsed_us = u64::try_from(
            env.borrow()
                .now()
                .saturating_duration_since(d.cmd_start)
                .as_micros(),
        )
        .unwrap_or(u64::MAX);
        record_command_stats(state_rc, &d.request, out_before, out, cmd_elapsed_us);
        if ctx.hotkeys.is_active() {
            let reply_bytes =
                u64::try_from(out.len().saturating_sub(out_before)).unwrap_or(u64::MAX);
            record_hotkeys(ctx, env, &d.request, cmd_elapsed_us, reply_bytes);
        }
        apply_client_tracking(
            conn,
            push_tx,
            shed_flag,
            &d.request,
            d.was_tracking,
            d.was_bcast,
        );
        consume_caching_flag(conn, &d.request);
        if d.slow_threshold >= 0 {
            record_slow_command(ctx, env, conn, &d.request, d.cmd_start, d.slow_threshold);
        }
    }
}

// `too_many_lines` is allowed: this is the per-connection WIRING + read/dispatch/write loop --
// the shard-handle lazy-inits, the per-connection push channel + shed signal (FIX D), the
// pipelined decode/route/flush loop, the subscribe-mode idle wait, and the close-path cleanup
// (subscription deregistration + WATCH deregistration + counter close). Each is a documented step
// the connection lifecycle must run in one place; splitting it would scatter the loop's control
// flow across helpers that all need the same locals.
#[allow(clippy::too_many_lines)]
async fn serve_connection(
    tcp: tokio::net::TcpStream,
    home: ShardId,
    mut ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    persist: Option<Arc<crate::persist::PersistState>>,
) {
    // TLS HANDSHAKE (or plaintext passthrough), #105. When TLS is enabled the accepted TCP
    // connection is upgraded RIGHT HERE, before any RESP byte is read: a rustls handshake runs
    // and yields a `ClientStream::Tls` the serve loop reads/writes transparently. A plaintext
    // client to a TLS port FAILS this handshake -> the connection is dropped (rejected, not hung).
    // TCP KEEPALIVE (Area C, Redis `tcp-keepalive`): apply SO_KEEPALIVE with the LIVE keepalive
    // idle interval at ACCEPT, on the raw accepted TcpStream BEFORE the TLS wrap (the option lives
    // on the kernel socket, beneath any TLS layering). Read from the runtime overlay so a
    // `CONFIG SET tcp-keepalive` applies to NEWLY-accepted connections (an established connection
    // keeps the option it was accepted with, matching Redis). `0` disables keepalive. A single cold
    // accept-path call; the helper ignores socket errors (a connection still functions without the
    // probe), and lives in the runtime crate so the socket2 borrow (no `unsafe` here) stays out of
    // this `#![forbid(unsafe_code)]` crate.
    ironcache_runtime::set_keepalive(&tcp, ctx.runtime.tcp_keepalive_secs());
    // When TLS is OFF (the default) we wrap the same TcpStream in `ClientStream::Plain`, a thin
    // passthrough to the identical TcpStream read/write code -- the plaintext hot path is
    // byte-unchanged. The client stream's own recv/send carry the data bytes from here on.
    let mut stream = match tls_acceptor {
        Some(acceptor) => match ironcache_runtime::accept_tls(&acceptor, tcp).await {
            Ok(s) => s,
            Err(_e) => {
                // A failed handshake (a non-TLS client, an unsupported version, a truncated
                // ClientHello): close the connection. The bytes that arrived were never RESP and
                // never reached the engine, so there is nothing to flush -- just return.
                return;
            }
        },
        None => ironcache_runtime::ClientStream::plain(tcp),
    };
    let env = shard_env();
    // Adopt THIS shard's metrics cell (OBSERVABILITY.md, #152) BEFORE `shard_state()` first builds
    // the `ShardState`, so its `ShardCounters` wrap the registry cell the metrics task reads. The
    // drain-loop boot usually adopts first; this is the idempotent fallback for a connection that
    // races ahead of the drain loop's first poll. A no-op when `/metrics` is disabled.
    adopt_metrics_cell(ctx.metrics_registry.as_ref(), home.index);
    // Adopt THIS shard's reference to the shared process-global allocator-memory gauge (PROD-SAFETY
    // #1/#2) so this shard's expiry tick publishes the live jemalloc figure the admission gate reads.
    adopt_process_memory_gauge(&ctx.process_memory);
    let state_rc = shard_state();
    // The reserved-band width is derived from the configured TOTAL shard count so SCAN's
    // composite cursor is band-aligned when shards > 1 (FIX 1); 0 keeps single-shard SCAN
    // byte-identical.
    let reserved_bits = scan_reserved_bits(ctx.shards);
    let store_rc = shard_store(ctx.databases, ctx.info.maxmemory_policy, reserved_bits);
    // Seed this shard store's collection-encoding thresholds (#40) from the runtime overlay (which
    // was itself seeded from the boot config). The store is built lazily with the COMPILED defaults;
    // this idempotent per-connection field write installs the BOOT-configured thresholds so a node
    // started with a non-default `*-max-listpack-*` (via TOML/env) honors it from the first command
    // (a later `CONFIG SET` then refreshes via the generation-change check in `maybe_hot_swap_policy`).
    store_rc
        .borrow_mut()
        .set_encoding_thresholds(ctx.runtime.encoding_thresholds());
    let wheel_rc = shard_wheel();
    // Ensure this shard's background active-expiry timer is up (PR-3c, idempotent). The
    // canonical spawn point is now SHARD BOOT (the coordinator drain loop calls
    // `ensure_shard_started` before its recv loop, COORDINATOR.md #107: a key-owning shard
    // must reclaim even with no connection). This call is the same idempotent helper, so a
    // connection arriving before the drain loop's first poll still gets the timer started;
    // the EXPIRE_TASK_SPAWNED guard makes the duplicate call a no-op.
    ensure_shard_started(
        ctx.databases,
        ctx.info.maxmemory_policy,
        reserved_bits,
        ctx.runtime.clone(),
    );
    // Correct the context's started_at to this shard's boot instant.
    ctx.info.started_at = shard_started_at();

    // MAXCLIENTS gate (PROD-SAFETY #3, the connection-exhaustion DoS fix). Atomically admit this
    // connection against the process-GLOBAL live-connection count vs the `maxclients` ceiling (read
    // from the runtime overlay, so a `CONFIG SET maxclients` takes effect for new connections). When
    // the node is already AT the cap, reject: write the byte-exact Redis `-ERR max number of clients
    // reached` reply, then close, WITHOUT counting this connection (it was never admitted) and
    // WITHOUT entering the serve loop. `maxclients == 0` disables the cap (the pre-fix behavior).
    // This is a COLD accept-path check (once per connection, never per command). On admit, the
    // returned `ConnGateGuard` (L1) frees the slot on Drop -- on the normal close path, an early
    // return, OR a panic unwinding through the serve loop -- so a panic can never leak the slot.
    if !ctx.conn_gate.try_admit(ctx.runtime.maxclients()) {
        let mut reject = Vec::with_capacity(64);
        encode_into(
            &mut reject,
            &ironcache_server::Value::Error(ironcache_server::ErrorReply::err(
                "max number of clients reached",
            )),
            default_proto,
        );
        let _ = stream.send(reject).await;
        return;
    }
    // Admitted: hold the slot in an RAII guard so it is released on EVERY exit path (the normal
    // close below, any early return, or a panic). The guard lives for the rest of the function.
    let _conn_gate_guard = ConnGateGuard {
        gate: Arc::clone(&ctx.conn_gate),
    };

    let addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let laddr = stream
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();

    let client_id = {
        let mut s = state_rc.borrow_mut();
        let id = s.next_client_id;
        s.next_client_id += 1;
        s.counters.on_connection_open();
        id
    };

    // Register this connection in the node-level client registry (PROD-7) so CLIENT LIST sees it
    // and CLIENT KILL / CLIENT PAUSE can act on it. `client_handle` is THIS connection's record; the
    // serve loop checks its `is_killed()` flag after each command batch (a cold relaxed load) and
    // closes if set. The RAII guard deregisters on every exit path. A cold accept-path registration,
    // never touched per command (except the post-batch kill/pause check, which is also cold).
    let client_handle = ctx
        .clients
        .register(client_id, addr.clone(), laddr.clone(), 0);
    let _client_registry_guard = ClientRegistryGuard {
        registry: Arc::clone(&ctx.clients),
        id: client_id,
    };

    let mut conn = ConnState::new(client_id, default_proto, ctx.requires_auth(), addr, laddr);

    // The per-connection PUSH channel (SERVER_PUSH.md #20, PR 91a). `push_tx` is the `Send`
    // handle registered into the home shard's subscription table on SUBSCRIBE (so a PUBLISH on
    // any core can hand this connection a message); `push_rx` is owned by THIS serve loop and
    // drained in the subscribe-mode idle-wait below. Bounded for back-pressure: a slow consumer
    // past the bound is shed by the publisher. Created for EVERY connection (cheap), but only
    // WIRED into the select! once the connection enters subscribe mode, so the non-subscriber
    // hot path never touches it.
    let (mut push_tx, mut push_rx) =
        tokio::sync::mpsc::channel::<crate::pubsub::ServerPush>(crate::pubsub::PUSH_CHANNEL_BOUND);
    // The per-connection SHED/kill signal (SERVER_PUSH.md #20, FIX D). When the publisher sheds
    // this connection (its bounded push channel overflowed past the bound), it both drops the
    // table sender AND trips this signal; the subscriber idle-wait observes it and CLOSES the
    // connection. This is necessary because the serve loop holds its OWN `push_tx` clone, so
    // `push_rx.recv()` would NEVER return None on a shed alone -- the signal is the disconnect
    // trigger. Registered into the table alongside `push_tx` on each (P)SUBSCRIBE. Shared
    // cross-core (the publisher runs on any shard), so an `Arc<ShedSignal>` (an atomic latch +
    // a `Notify` for a spin-free wake).
    let mut shed_flag = std::sync::Arc::new(crate::pubsub::ShedSignal::default());

    // The decoder hardening Limits (#138): the `proto-max-bulk-len` ceiling is RUNTIME-SETTABLE, so
    // build the Limits from the live overlay at connection setup (a single cold relaxed load) rather
    // than the compiled default. A `CONFIG SET proto-max-bulk-len` then applies to NEWLY-accepted
    // connections; an established connection keeps the ceiling it was built with (re-reading per
    // decode would add a hot-path load for a knob that effectively never changes mid-connection).
    let limits = Limits {
        max_bulk_len: i64::try_from(ctx.runtime.proto_max_bulk_len()).unwrap_or(i64::MAX),
        ..Limits::default()
    };
    let mut read_buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);
    // CROSS-SHARD HOP OVERLAP (#8): the run of DEFERRED remote hops awaiting assembly. Parked as they
    // are decoded and drained (in order) at the next barrier / end of batch. Always empty between
    // batches (drained before every flush), so it carries no state across the outer loop.
    let mut pending: Vec<DeferredHop> = Vec::new();

    // The IDLE TIMEOUT (PROD-SAFETY #4): a connection that sits idle (no command) longer than
    // `timeout` seconds is CLOSED, so idle connections cannot accumulate. `0` (the Redis default)
    // DISABLES it -- the non-subscriber idle wait then stays a plain `recv` with no timer, the
    // byte-unchanged hot path. The timeout is RUNTIME-SETTABLE (Redis `timeout` is a MODIFIABLE
    // config): the serve loop RE-READS `ctx.runtime.timeout_secs()` at the top of each connection-
    // loop iteration (just below, before the idle wait), so a `CONFIG SET timeout` takes effect LIVE
    // for an already-connected client on its next idle wait -- a non-zero<->0 change switches between
    // the timer-select arm and the plain-recv arm. One relaxed atomic load per (cold, post-batch)
    // iteration. It is measured via the Runtime timer SEAM (NOT wall-clock) and the deadline RE-ARMS
    // on each loop iteration -- i.e. after each command batch is served -- which is the per-command
    // deadline reset (an active connection is never closed). A zero-sized `TokioRuntime` backend
    // supplies the timer (the shard's tasks live on the LocalSet; this carries no state). The
    // OUTPUT-BUFFER cap (PROD-SAFETY #5) is likewise read from the runtime overlay each flush (a
    // `CONFIG SET` takes effect).
    let timer_rt = TokioRuntime::new();

    'conn: loop {
        // Drain every complete request currently buffered (pipelining), building
        // one combined output buffer, then flush once. A running cursor (`consumed_total`) advances
        // per command and the read buffer is drained ONCE after the batch, instead of per command:
        // draining per command memmoves all remaining pipelined bytes to the front each time, which is
        // O(P^2) over a depth-P pipeline. `decode` parses a `&[u8]`, so we hand it `read_buf[cursor..]`.
        out.clear();
        // OUTPUT-BUFFER hard cap (PROD-SAFETY #5 / #529): read the live cap ONCE per batch (one cold
        // relaxed load, off the per-command hot path) and enforce it in TWO places -- after each
        // command's reply is appended INSIDE the decode loop below (#529: bound resident output so a
        // single pipelined batch of large-reply commands cannot OOM the host before the post-batch
        // check), and again after the batch is assembled (the original PROD-SAFETY #5 pre-flush
        // check). Reading once per batch matches the documented "takes effect for subsequent batches"
        // semantics (a `CONFIG SET output-buffer-limit` applies from the next batch), so both checks
        // use one consistent value. `0` disables the cap (the pre-fix unbounded behavior).
        let obl = ctx.runtime.output_buffer_limit();
        let mut consumed_total = 0usize;
        loop {
            match decode(&read_buf[consumed_total..], &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    // SLOWLOG TIMING HOOK (PROD-7). When the SLOWLOG is ENABLED (threshold >= 0) read
                    // the monotonic clock ONCE before dispatch; when DISABLED (-1, the default) this
                    // is a single relaxed atomic load + branch and NOTHING else runs (no clock read,
                    // no ring touch), so the hot path is byte-unchanged. The threshold is read from
                    // the runtime overlay (the canonical CONFIG source); `ctx.slowlog` mirrors it for
                    // the hot-path copy, but reading the overlay directly here keeps a single source.
                    let slow_threshold = ctx.slowlog.log_slower_than_micros();
                    // COMMANDSTATS timing (#413): capture the monotonic start ALWAYS (one read),
                    // SHARED with the SLOWLOG hook below so the slowlog-enabled path adds no start
                    // read. The end is read once after dispatch to record this command's usec.
                    let cmd_start = env.borrow().now();
                    // The offset where THIS command's reply will be appended, so the commandstats
                    // hook can tell an error reply (leading `-`) from a success without re-parsing.
                    // `mut`: the cross-shard-hop overlap (#8) may re-base this when a run of pending
                    // hop replies is spliced in ahead of a barrier command's output (see below).
                    let mut out_before = out.len();
                    // CLIENT TRACKING (#409): snapshot whether this connection was tracking / in
                    // BCAST mode BEFORE dispatch, so the hook can detect the ON->OFF / RESET and the
                    // BCAST enter/leave transitions (purge or register prefixes accordingly).
                    let was_tracking = conn.tracking_on;
                    let was_bcast = conn.tracking_bcast;
                    // Route + dispatch one decoded request (COORDINATOR.md #107, Stage 1),
                    // appending its encoded reply to `out`; returns whether to close (QUIT).
                    // Factored out of the serve loop so the connection loop stays small.
                    // `block_request` (PROD-9) is set by the blocking-command interception when the
                    // command must PARK; it stays `None` for every non-blocking command (the hot
                    // path), so this is a single stack `Option` write per command.
                    let mut block_request: Option<BlockPark> = None;
                    // CROSS-SHARD HOP OVERLAP (#8): route_and_dispatch sets this (the tokio loop opts
                    // IN below) when the command is a single-key REMOTE hop it ENQUEUED but did not
                    // await -- we park it in `pending` and drain the run at the next barrier / end of
                    // batch, instead of blocking here on the cross-thread round-trip (which serialized
                    // N pipelined hops into 2N wakeups and made 2 shards slower than 1).
                    let mut deferred_hop = coordinator::HopOutcome::NotHop;
                    // CLIENT PAUSE (#388, write-aware): hold this command HERE while a pause that
                    // applies to it is active -- an ALL pause holds every command, a WRITE pause
                    // holds only writes (reads + admin like SAVE proceed). This is the single point
                    // both kinds are honored, so it is correct under pipelining and holds the very
                    // next command after a pause begins. Default (no pause): a single relaxed atomic
                    // load returns immediately, so the hot path is byte-unchanged (no clock read, no
                    // classify).
                    pause_stall(&ctx, &conn, &request, &env, &timer_rt, &client_handle).await;
                    let close = route_and_dispatch(
                        &ctx,
                        &mut conn,
                        home,
                        &inbox,
                        &mut push_tx,
                        &mut push_rx,
                        &mut shed_flag,
                        &env,
                        &store_rc,
                        &wheel_rc,
                        &state_rc,
                        persist.as_ref(),
                        &request,
                        &mut out,
                        &mut block_request,
                        // #8: opt IN to hop deferral -- this is the default tokio serve loop.
                        true,
                        &mut deferred_hop,
                    )
                    .await;
                    // #8 OVERLAP: a DEFERRED remote hop -- park it and keep decoding, so the next
                    // command's hop is issued while this owner works. `out` is UNTOUCHED (the reply is
                    // encoded later, in order), so FIFO on the wire is preserved. A hop is never a
                    // blocking command and never closes the connection, so we skip the hooks + park +
                    // close handling and go straight to the next command; its hooks run at drain time.
                    if let coordinator::HopOutcome::Deferred(rx) = deferred_hop {
                        pending.push(DeferredHop {
                            rx,
                            request,
                            cmd_start,
                            was_tracking,
                            was_bcast,
                            slow_threshold,
                            proto: conn.proto,
                        });
                        consumed_total += consumed;
                        continue;
                    }
                    // BARRIER (any synchronous / control / home / fan-out command): if a run of hops is
                    // pending, splice their replies into `out` BEFORE this command's already-encoded
                    // output (FIFO), running each hop's deferred hooks in order. `split_off` lifts this
                    // command's bytes aside; after draining the run we re-append them and re-base
                    // `out_before` so this command's own hooks read the right reply slice.
                    if !pending.is_empty() {
                        let barrier_bytes = out.split_off(out_before);
                        drain_deferred_hops(
                            &mut pending,
                            &mut out,
                            &ctx,
                            &mut conn,
                            &env,
                            &state_rc,
                            &push_tx,
                            &shed_flag,
                        )
                        .await;
                        out_before = out.len();
                        out.extend_from_slice(&barrier_bytes);
                    }
                    // SLOWLOG record (PROD-7): only reached when the SLOWLOG was enabled at the start
                    // of this command. Measure elapsed micros through the SAME Env clock seam
                    // (ADR-0003), and if it met the threshold, push the command (args + this
                    // connection's addr/name) into the node-level ring and feed the LATENCY
                    // `command` event. This whole block is skipped entirely when SLOWLOG is disabled.
                    // COMMANDSTATS / ERRORSTATS (#413): record this command's elapsed micros +
                    // outcome on the serving shard (ALWAYS, the call/usec/failed tally INFO reads).
                    // The end clock read is shared with the slowlog hook (same `cmd_start`).
                    let cmd_elapsed_us = u64::try_from(
                        env.borrow()
                            .now()
                            .saturating_duration_since(cmd_start)
                            .as_micros(),
                    )
                    .unwrap_or(u64::MAX);
                    record_command_stats(&state_rc, &request, out_before, &out, cmd_elapsed_us);
                    // HOTKEYS (#428): attribute this command's CPU micros + net bytes to its keys
                    // when a tracking session is active. The gate is ONE relaxed atomic load, so the
                    // default (no session) path -- and the perf-gate -- never reach the recorder.
                    if ctx.hotkeys.is_active() {
                        let reply_bytes =
                            u64::try_from(out.len().saturating_sub(out_before)).unwrap_or(u64::MAX);
                        record_hotkeys(&ctx, &env, &request, cmd_elapsed_us, reply_bytes);
                    }
                    // CLIENT TRACKING (#409): register this command's read keys for a tracking
                    // connection, or invalidate a write's keys for every tracking client. The
                    // common no-tracking path is one cheap gate inside (see `apply_client_tracking`).
                    apply_client_tracking(
                        &conn,
                        &push_tx,
                        &shed_flag,
                        &request,
                        was_tracking,
                        was_bcast,
                    );
                    // OPTIN/OPTOUT (#409 stage 3): consume the one-shot CLIENT CACHING flag now that
                    // this command's track decision has used it (cleared except on CLIENT CACHING).
                    consume_caching_flag(&mut conn, &request);
                    if slow_threshold >= 0 {
                        record_slow_command(&ctx, &env, &conn, &request, cmd_start, slow_threshold);
                    }
                    consumed_total += consumed;
                    // -- BLOCKING PARK (PROD-9). The blocking-command interception in
                    // `route_and_dispatch` set `block_request`: the command's non-blocking attempt
                    // found every key empty (or WAIT's quorum not yet met), so PARK this connection
                    // here, where the serve loop owns the stream (to observe a peer close), the
                    // runtime timer (the timeout), and the read buffer (to keep pipelined bytes). The
                    // park loop FLUSHES any pending pipelined replies in `out` FIRST (FIFO), then
                    // registers a per-shard FIFO waiter and `select!`s on (the wake / the timeout /
                    // a peer close), re-attempting the pop on a wake. It returns the close flag.
                    // `block_request == None` on the hot path (every non-blocking command), so this
                    // is a single `is_some` check then skipped.
                    if let Some(park) = block_request {
                        // The running cursor deferred the read-buffer drain; apply it now so
                        // `run_block_park` (which owns `read_buf` during the park + re-decodes bytes
                        // that arrive) sees the buffer starting at the next UNprocessed byte. The
                        // blocking command's own bytes are included in `consumed_total`.
                        read_buf.drain(..consumed_total);
                        consumed_total = 0;
                        // Flush the pipelined replies that preceded the blocking command, so a
                        // blocked client still receives the earlier commands' replies before it
                        // parks (FIFO, never a blocking command holding up prior replies).
                        if !out.is_empty() {
                            match stream.send(std::mem::take(&mut out)).await {
                                Ok(returned) => out = returned,
                                Err(_) => break 'conn,
                            }
                        }
                        let park_close = run_block_park(
                            &mut stream,
                            &timer_rt,
                            &ctx,
                            &conn,
                            &client_handle,
                            &env,
                            &store_rc,
                            &inbox,
                            home,
                            &mut read_buf,
                            &mut out,
                            park,
                        )
                        .await;
                        if park_close {
                            break 'conn;
                        }
                        // The park completed (a pop succeeded, or the timeout / quorum reply was
                        // sent). `out` was flushed inside the park loop, so continue the decode loop
                        // to process any bytes that arrived while parked (re-decode `read_buf`).
                        continue;
                    }
                    if close {
                        // Flush the QUIT reply then close. send returns the owned
                        // buffer (owned-buffer model); we are closing, so the
                        // returned buffer is dropped rather than reclaimed. Sent over the
                        // CLIENT stream (plain or TLS); the plain arm is byte-identical to the
                        // prior `rt.send` (it calls the same TcpStream write_all), #105.
                        let _ = stream.send(std::mem::take(&mut out)).await;
                        break 'conn;
                    }
                    // OUTPUT-BUFFER intra-batch cap (#529): this command's reply is now appended to
                    // `out`. If the accumulated reply buffer has grown past the cap MID-BATCH, stop
                    // decoding more commands and fall through to close -- the post-batch drain + the
                    // pre-flush check below then close the connection. This bounds resident output so
                    // a single pipelined batch of large-reply commands cannot drive unbounded server
                    // memory before the (post-batch) check runs. At the default cap (1 GiB) / `0`
                    // (disabled) this is a single compare against a local that is never true on the
                    // hot path, so the branch is predicted not-taken and the batch decodes as before.
                    if obl > 0 && out.len() as u64 > obl {
                        break;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Error(e) => {
                    // Protocol error: write it and close the connection (hardening). #8: FIRST drain
                    // any pending cross-shard hops so the replies for the VALID commands that preceded
                    // the malformed frame still go out (in order) before the error + close -- otherwise
                    // the overlap would silently drop them (the inline path flushed them).
                    if !pending.is_empty() {
                        drain_deferred_hops(
                            &mut pending,
                            &mut out,
                            &ctx,
                            &mut conn,
                            &env,
                            &state_rc,
                            &push_tx,
                            &shed_flag,
                        )
                        .await;
                    }
                    encode_into(&mut out, &ironcache_server::Value::Error(e), conn.proto);
                    let _ = stream.send(std::mem::take(&mut out)).await;
                    break 'conn;
                }
            }
        }

        // CROSS-SHARD HOP OVERLAP (#8): the batch ended (decode Incomplete) with a run of remote hops
        // still pending (no trailing barrier drained them). Assemble their replies into `out` NOW, in
        // order, BEFORE the flush -- so every reply for this batch is on the wire in command order.
        if !pending.is_empty() {
            drain_deferred_hops(
                &mut pending,
                &mut out,
                &ctx,
                &mut conn,
                &env,
                &state_rc,
                &push_tx,
                &shed_flag,
            )
            .await;
        }

        // Drain the whole processed batch ONCE (the running-cursor deferral): a single memmove removes
        // every consumed command, leaving only a partial trailing frame at the front of the buffer.
        if consumed_total > 0 {
            read_buf.drain(..consumed_total);
        }

        // OUTPUT-BUFFER hard cap (PROD-SAFETY #5): before flushing, if the pending reply buffer has
        // grown past the configured `output_buffer_limit`, CLOSE the connection rather than let a
        // slow consumer / a huge reply / a pipelined flood drive unbounded server memory. `0`
        // disables the cap (the pre-fix unbounded behavior); the default is a high ceiling so a
        // legitimate large reply / deep pipeline is never affected. Uses the `obl` read once at the
        // top of this batch (the intra-batch check #529 shares it), so a `CONFIG SET
        // output-buffer-limit` takes effect for subsequent batches. We drop the oversized buffer
        // unsent and close (matching Redis closing a client over the limit).
        if obl > 0 && out.len() as u64 > obl {
            break;
        }

        if !out.is_empty() {
            // Owned-buffer send: hand `out` over and take the returned buffer back. Over the
            // client stream (plain or TLS); the plain arm is the same TcpStream write the prior
            // `rt.send` did (#105).
            match stream.send(std::mem::take(&mut out)).await {
                Ok(returned) => out = returned,
                Err(_) => break,
            }
        }

        // CLIENT KILL (PROD-7): if a peer's `CLIENT KILL` flagged THIS connection, close it now
        // (after its in-flight batch's reply was flushed above). A single relaxed atomic load on a
        // cold post-batch path; the default (never killed) is byte-unchanged.
        if client_handle.is_killed() {
            break;
        }

        // CLIENT PAUSE (PROD-7, #388): the pause stall is now PER-COMMAND, done in the decode loop
        // above (`pause_stall`) right before each command is dispatched, so a WRITE pause holds only
        // writes (reads + admin like SAVE proceed) and an ALL pause holds every command -- and the
        // VERY NEXT command after a pause begins is held (a post-batch stall here would only hold the
        // command AFTER the current batch was already served). The default (no pause) cost is a single
        // relaxed atomic load per command at that gate; there is no post-batch pause work.

        // The LIVE idle timeout (PROD-SAFETY #4): re-read the runtime overlay (one relaxed atomic
        // load) so a `CONFIG SET timeout` takes effect for this already-connected client on THIS
        // idle wait. `0` (the default) is `None` -> the plain-recv arm below (byte-unchanged hot
        // path); a non-zero value is `Some(Duration)` -> the timer-select arm. Because this is
        // recomputed each iteration, a runtime non-zero<->0 change correctly switches arms.
        let idle_timeout: Option<core::time::Duration> = {
            let secs = ctx.runtime.timeout_secs();
            if secs == 0 {
                None
            } else {
                Some(core::time::Duration::from_secs(secs))
            }
        };

        // IDLE WAIT. The NON-subscriber path (the common, hot path) is BYTE-IDENTICAL to before
        // pub/sub when no idle timeout is configured: just await `rt.recv`, no select! overhead.
        // Only a connection in SUBSCRIBE mode pays for the select! that ALSO drains the push channel
        // (`subscriber_idle_wait`). FIFO ordering holds because `out` was already flushed above
        // before we reach this idle wait, so a push is rendered and sent only AFTER the in-flight
        // command batch's reply went out -- a push never precedes a command reply on the connection
        // (SERVER_PUSH.md FIFO). A CLIENT TRACKING connection (#409) drains the push channel the
        // SAME way (its `invalidate` pushes ride the same channel), without entering subscribe MODE
        // (the RESP2 command gate stays `is_subscriber()`-only, so a tracking client runs any command).
        if conn.is_subscriber() || conn.tracking_on {
            if subscriber_idle_wait(
                &mut stream,
                &mut push_rx,
                &shed_flag,
                &mut read_buf,
                &mut out,
                conn.proto,
            )
            .await
            {
                break;
            }
        } else if let Some(timeout) = idle_timeout {
            // IDLE-TIMEOUT path (PROD-SAFETY #4): race the read against the Runtime timer seam. The
            // deadline is fresh on each iteration (re-armed after the command batch above), so an
            // active connection never trips it; only a connection idle for `timeout` seconds with no
            // new bytes does, and is then closed. The read is into a FRESH buffer and the new bytes
            // are APPENDED to `read_buf` (NOT moved into the recv): had the timer won, a `read_buf`
            // moved into the cancelled recv future would be dropped along with any partial frame it
            // held; reading into a temporary keeps `read_buf`'s partial bytes safe across
            // cancellation (the same pattern the subscriber idle-wait uses).
            tokio::select! {
                res = stream.recv(Vec::new()) => {
                    let Ok(res) = res else { break; };
                    if res.n == 0 {
                        break; // peer closed
                    }
                    read_buf.extend_from_slice(&res.buf[..res.n]);
                }
                () = timer_rt.timer(timeout) => {
                    // Idle past the timeout with no new command bytes: close the connection
                    // (PROD-SAFETY #4). `read_buf` (with any buffered partial frame) is simply
                    // dropped on the close path below.
                    break;
                }
            }
        } else {
            // Need more bytes: read over the client stream (plain or TLS). The plain arm is the
            // same TcpStream read the prior `rt.recv` did, so the plaintext hot path is unchanged
            // (no idle timeout configured -> no timer, byte-identical to before PROD-SAFETY #4).
            let Ok(res) = stream.recv(std::mem::take(&mut read_buf)).await else {
                break;
            };
            read_buf = res.buf;
            if res.n == 0 {
                break; // peer closed
            }
        }

        // QUERY-BUFFER hard cap (#528): every idle-wait arm above (subscriber drain / idle-timeout
        // race / plain recv) APPENDS the newly-arrived bytes into `read_buf`, then this loop
        // re-decodes. If the accumulated inbound query buffer has grown past the configured
        // `query_buffer_limit`, CLOSE the connection rather than let a client that announces a large
        // multibulk (`*<huge>\r\n`) and then DRIBBLES the elements force unbounded pre-auth inbound
        // buffering (a memory-amplification DoS: the frame never completes, so decode keeps returning
        // Incomplete and `read_buf` grows without bound). A single cold relaxed load on this
        // post-batch / idle path (never per command); `0` disables the cap (Redis parity, the pre-fix
        // unbounded behavior). The oversized buffer is dropped and the connection closed.
        let qbl = ctx.runtime.query_buffer_limit();
        if qbl > 0 && read_buf.len() as u64 > qbl {
            break;
        }
    }

    // Connection close: deregister this connection's PUB/SUB subscriptions from THIS shard's
    // subscription table (SERVER_PUSH.md #20, PR 91a). The connection's subscriptions are
    // home-shard-local, so this runs on the home shard, driven off `conn.sub_channels` /
    // `sub_patterns` (O(subs)). Like the WATCH cleanup below, this is the only exit that bypasses
    // the per-command deregistration, so a QUIT / error close / peer close all prune the table
    // here. Then DROP `push_tx`: with both the registered table senders and this owned handle
    // gone, the channel is fully closed (a no-op for a non-subscriber). A no-op when not
    // subscribed. The borrow of the subscription table is taken + released inside the helper.
    deregister_all_subscriptions(&conn);
    drop(push_tx);

    // Connection close: deregister this connection's WATCHes from the shard store
    // (TRANSACTIONS.md, PR-10b). `ConnState` holds the watch SNAPSHOTS but not the store
    // handle (the store carries the per-key watcher counts), so the deregistration is
    // done explicitly here in the serve loop before `conn` drops. This is the only exit
    // that bypasses the dispatch arms (which deregister on EXEC/DISCARD/UNWATCH/RESET), so
    // it prevents a watch from lingering in the store after a client disconnects mid-WATCH
    // (a QUIT, an error close, or the peer closing the socket all land here). A no-op when
    // the connection has no active watch set. Borrow the store separately from the state
    // counter borrow below (distinct RefCells, no alias).
    if !conn.watch.is_empty() {
        use ironcache_storage::Watch;
        store_rc.borrow_mut().unwatch(&conn.watch);
        conn.clear_watch();
    }
    state_rc.borrow_mut().counters.on_connection_close();
    // This connection's slot in the process-GLOBAL connection gate (PROD-SAFETY #3) is released by
    // the `_conn_gate_guard` RAII guard on Drop (L1) as this function returns, keeping the live count
    // accurate so the `maxclients` cap is enforced against a true figure over the node's lifetime.
    // The guard also covers any earlier return / panic, which the prior bare `release()` here did
    // not (a panic would have leaked the slot permanently).
}

/// The per-connection serve loop for the OPTIONAL io_uring datapath (PROD-10 / #28,
/// docs/design/IOURING_DATAPATH.md). Compiled ONLY on a Linux build with the `io_uring` feature;
/// reached ONLY when `runtime = io_uring` is selected at boot AND TLS is off (the selection branch
/// in [`run_server_observed`] falls back to the tokio [`serve_connection`] otherwise).
///
/// It drives the SAME engine the tokio path does: it REUSES [`route_and_dispatch`] (the route +
/// dispatch core is stream-agnostic -- it works on a decoded `Request` and an output `Vec<u8>`),
/// the same per-shard setup helpers ([`shard_env`] / [`shard_state`] / [`shard_store`] /
/// [`shard_wheel`] / [`ensure_shard_started`]), the same `maxclients` admission gate + RAII
/// guards, the same client-registry registration, and the same RESP `decode`. The ONLY difference
/// is the transport: bytes flow through the io_uring [`ironcache_runtime::Runtime`] owned-buffer
/// `recv`/`send` (one ring per shard) instead of a tokio `TcpStream` / `ClientStream`. The pure
/// command engine (the determinism seam, the store, the coordinator) is UNTOUCHED.
///
/// ## v1 scope and the io_uring-path behavior (honest, documented)
///
/// This v1 serves the CORE request/reply RESP datapath (pipelined decode -> dispatch -> single
/// coalesced flush), which is the bulk of cache traffic and the path the io_uring throughput lever
/// targets. The serve-loop features that the tokio path drives via `tokio::select!` over the stream
/// behave as follows on the io_uring loop (each is SAFE -- a defined reply, no hang, no silent drop,
/// close-on-shed):
///   * PUB/SUB (SERVER_PUSH.md) -- FULLY WIRED. A subscriber's idle wait runs
///     [`subscriber_idle_wait_uring`], which `select!`s the per-connection push channel alongside
///     the io_uring `recv`, so a PUSH-while-idle is delivered PROMPTLY (not silently dropped, not
///     deferred to the next command). Queued pushes coalesce into one write; the SHED kill-signal
///     is observed in the select! AND re-checked at the post-batch top of the loop, so a flooded
///     subscriber is CLOSED rather than accumulating forever.
///   * BLOCKING (PROD-9, BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP/WAIT) --
///     IMMEDIATE-REPLY (NOT a true park). The non-blocking attempt runs in `route_and_dispatch`; if
///     it would PARK (every key empty / WAIT quorum not yet met), the io_uring loop writes the
///     command's IMMEDIATE non-blocking reply instead of leaving `out` empty: the BLPOP-family pops
///     reply the nil-array (the zero-timeout result), WAIT replies the CURRENT in-sync replica
///     count. So a blocking command on this path RETURNS AT ONCE and never hangs. LIMITATION: true
///     block-until-data (parking past the first attempt) is NOT yet supported on the io_uring path
///     (it needs a `select!` over the io_uring `recv` future whose cancel-on-drop semantics are
///     unvalidated here); that is a documented Linux follow-up. The tokio path's `run_block_park`
///     remains the full implementation.
///   * The IDLE-TIMEOUT (PROD-SAFETY #4) timer race is deferred; the `maxclients` cap + peer-close
///     detection still bound connections.
///
/// CLIENT PAUSE (PROD-7) IS enforced: a pause window stalls this connection's command processing
/// (the same conservative-superset stall the tokio loop applies, re-checking via the Runtime timer
/// seam so UNPAUSE / CLIENT KILL take effect promptly).
///
/// The registered-buffer / multishot fast path (IOURING_DATAPATH.md) and a perf benchmark are
/// deferred to a Linux soak; no throughput claim is made here.
#[cfg(all(target_os = "linux", feature = "io_uring"))]
#[allow(clippy::too_many_lines)]
async fn serve_connection_uring(
    rt: ironcache_runtime::IoUringRuntime,
    mut stream: ironcache_runtime::UringTcpStream,
    home: ShardId,
    mut ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
    persist: Option<Arc<crate::persist::PersistState>>,
) {
    use ironcache_runtime::Runtime;

    let env = shard_env();
    adopt_metrics_cell(ctx.metrics_registry.as_ref(), home.index);
    adopt_process_memory_gauge(&ctx.process_memory);
    let state_rc = shard_state();
    let reserved_bits = scan_reserved_bits(ctx.shards);
    let store_rc = shard_store(ctx.databases, ctx.info.maxmemory_policy, reserved_bits);
    // Seed the encoding thresholds (#40) from the runtime overlay, same as the tokio serve path.
    store_rc
        .borrow_mut()
        .set_encoding_thresholds(ctx.runtime.encoding_thresholds());
    let wheel_rc = shard_wheel();
    ensure_shard_started(
        ctx.databases,
        ctx.info.maxmemory_policy,
        reserved_bits,
        ctx.runtime.clone(),
    );
    ctx.info.started_at = shard_started_at();

    // MAXCLIENTS gate (PROD-SAFETY #3), identical to the tokio path: admit against the global
    // live-connection count vs the `maxclients` ceiling, reject (byte-exact reply) + close when at
    // the cap, hold the slot in a Drop-release guard otherwise.
    if !ctx.conn_gate.try_admit(ctx.runtime.maxclients()) {
        let mut reject = Vec::with_capacity(64);
        encode_into(
            &mut reject,
            &ironcache_server::Value::Error(ironcache_server::ErrorReply::err(
                "max number of clients reached",
            )),
            default_proto,
        );
        let _ = rt.send(&mut stream, reject).await;
        return;
    }
    let _conn_gate_guard = ConnGateGuard {
        gate: Arc::clone(&ctx.conn_gate),
    };

    // Peer/local addresses for CLIENT INFO. tokio-uring's TcpStream has no peer_addr/local_addr,
    // so derive them from the borrowed fd (without taking fd ownership). The borrowed-fd dance
    // needs `unsafe`, which THIS crate FORBIDS (`#![forbid(unsafe_code)]`), so it lives in the
    // runtime crate's `peer_local_addrs` helper (mirroring the io_uring backend's nodelay helper).
    // A failure leaves the field empty (cosmetic only).
    let (addr, laddr) = ironcache_runtime::peer_local_addrs(&stream);

    // TCP KEEPALIVE (Area C), the io_uring analog of the tokio accept path: apply SO_KEEPALIVE with
    // the live `tcp-keepalive` idle interval at accept (the borrowed-fd helper lives in the runtime
    // crate so the `unsafe` fd dance stays out of this crate). `0` disables it; a `CONFIG SET`
    // applies to newly-accepted connections.
    ironcache_runtime::set_keepalive_uring(&stream, ctx.runtime.tcp_keepalive_secs());

    let client_id = {
        let mut s = state_rc.borrow_mut();
        let id = s.next_client_id;
        s.next_client_id += 1;
        s.counters.on_connection_open();
        id
    };
    let client_handle = ctx
        .clients
        .register(client_id, addr.clone(), laddr.clone(), 0);
    let _client_registry_guard = ClientRegistryGuard {
        registry: Arc::clone(&ctx.clients),
        id: client_id,
    };

    let mut conn = ConnState::new(client_id, default_proto, ctx.requires_auth(), addr, laddr);

    // The per-connection PUSH channel + SHED signal are still created (route_and_dispatch needs
    // the `push_tx`/`push_rx`/`shed_flag` handles for SUBSCRIBE bookkeeping), but the push-while-
    // idle drain is deferred on this path (see the fn doc): pushes are delivered on the next
    // command round-trip rather than mid-idle.
    let (mut push_tx, mut push_rx) =
        tokio::sync::mpsc::channel::<crate::pubsub::ServerPush>(crate::pubsub::PUSH_CHANNEL_BOUND);
    let mut shed_flag = std::sync::Arc::new(crate::pubsub::ShedSignal::default());

    // Build the decoder Limits from the live `proto-max-bulk-len` overlay (Area B), same as the
    // tokio serve path: a single cold load at connection setup; the value applies to new connections.
    let limits = Limits {
        max_bulk_len: i64::try_from(ctx.runtime.proto_max_bulk_len()).unwrap_or(i64::MAX),
        ..Limits::default()
    };
    let mut read_buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);
    // The Runtime timer seam for the CLIENT PAUSE stall (FIX3). Under `tokio_uring::start` the
    // canonical tokio time driver is enabled (the io_uring backend's `timer` is `tokio::time::sleep`),
    // so the SAME zero-sized `TokioRuntime` timer the tokio serve loop uses drives the pause poll
    // here too -- no io_uring timeout op needed for the timer abstraction.
    let timer_rt = TokioRuntime::new();

    'conn: loop {
        out.clear();
        // OUTPUT-BUFFER hard cap (PROD-SAFETY #5 / #529), identical to the tokio path: read the live
        // cap ONCE per batch and enforce it both INSIDE the decode loop after each command's reply is
        // appended (#529: bound resident output so one pipelined batch of large-reply commands cannot
        // OOM the host) and again pre-flush after the batch is assembled. `0` disables the cap.
        let obl = ctx.runtime.output_buffer_limit();
        // Running cursor: advance per command, drain the read buffer ONCE after the batch instead of
        // per command (per-command drain memmoves the remaining pipelined bytes each time -> O(P^2)).
        let mut consumed_total = 0usize;
        loop {
            match decode(&read_buf[consumed_total..], &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    let slow_threshold = ctx.slowlog.log_slower_than_micros();
                    // COMMANDSTATS timing (#413): capture the monotonic start ALWAYS (one read),
                    // SHARED with the SLOWLOG hook below so the slowlog-enabled path adds no start
                    // read. The end is read once after dispatch to record this command's usec.
                    let cmd_start = env.borrow().now();
                    // The offset where THIS command's reply will be appended, so the commandstats
                    // hook can tell an error reply (leading `-`) from a success without re-parsing.
                    let out_before = out.len();
                    // CLIENT TRACKING (#409): snapshot whether this connection was tracking / in
                    // BCAST mode BEFORE dispatch, so the hook can detect the ON->OFF / RESET and the
                    // BCAST enter/leave transitions (purge or register prefixes accordingly).
                    let was_tracking = conn.tracking_on;
                    let was_bcast = conn.tracking_bcast;
                    // PROD-9 blocking-park (FIX1): a blocking command (BLPOP/.../WAIT) whose
                    // non-blocking attempt found no data sets `block_request` and leaves `out`
                    // EMPTY, expecting the OWNING serve loop to PARK (the tokio loop runs
                    // `run_block_park`). The io_uring loop does NOT support a true park yet, so we
                    // write the IMMEDIATE non-blocking reply below instead of leaving `out` empty
                    // (which would HANG the client and desync later replies). See the fn doc: true
                    // blocking is a documented io_uring-path limitation; the reply is the same one
                    // the command would have produced on a zero timeout.
                    let mut block_request: Option<BlockPark> = None;
                    // #8: the io_uring loop does NOT opt into hop deferral yet (a following PR mirrors
                    // the overlap here); it passes `defer_hops = false` so route_and_dispatch awaits
                    // each hop inline exactly as before, and this stays `NotHop` (byte-identical path).
                    let mut deferred_hop = coordinator::HopOutcome::NotHop;
                    // CLIENT PAUSE (#388, write-aware), identical to the tokio path: hold this
                    // command while a pause that applies to it is active -- an ALL pause holds
                    // everything, a WRITE pause holds only writes (reads + admin like SAVE proceed).
                    // Default (no pause): one relaxed atomic load returns immediately -- the
                    // byte-unchanged hot path.
                    pause_stall(&ctx, &conn, &request, &env, &timer_rt, &client_handle).await;
                    let close = route_and_dispatch(
                        &ctx,
                        &mut conn,
                        home,
                        &inbox,
                        &mut push_tx,
                        &mut push_rx,
                        &mut shed_flag,
                        &env,
                        &store_rc,
                        &wheel_rc,
                        &state_rc,
                        persist.as_ref(),
                        &request,
                        &mut out,
                        &mut block_request,
                        // #8: io_uring loop does NOT defer hops (inline await, byte-identical).
                        false,
                        &mut deferred_hop,
                    )
                    .await;
                    let _ = &deferred_hop; // never set (defer_hops = false); kept for the shared signature.
                    // COMMANDSTATS / ERRORSTATS (#413): record this command's elapsed micros +
                    // outcome on the serving shard (ALWAYS, the call/usec/failed tally INFO reads).
                    // The end clock read is shared with the slowlog hook (same `cmd_start`).
                    let cmd_elapsed_us = u64::try_from(
                        env.borrow()
                            .now()
                            .saturating_duration_since(cmd_start)
                            .as_micros(),
                    )
                    .unwrap_or(u64::MAX);
                    record_command_stats(&state_rc, &request, out_before, &out, cmd_elapsed_us);
                    // HOTKEYS (#428): attribute this command's CPU micros + net bytes to its keys
                    // when a tracking session is active. The gate is ONE relaxed atomic load, so the
                    // default (no session) path -- and the perf-gate -- never reach the recorder.
                    if ctx.hotkeys.is_active() {
                        let reply_bytes =
                            u64::try_from(out.len().saturating_sub(out_before)).unwrap_or(u64::MAX);
                        record_hotkeys(&ctx, &env, &request, cmd_elapsed_us, reply_bytes);
                    }
                    // CLIENT TRACKING (#409): register this command's read keys for a tracking
                    // connection, or invalidate a write's keys for every tracking client. The
                    // common no-tracking path is one cheap gate inside (see `apply_client_tracking`).
                    apply_client_tracking(
                        &conn,
                        &push_tx,
                        &shed_flag,
                        &request,
                        was_tracking,
                        was_bcast,
                    );
                    // OPTIN/OPTOUT (#409 stage 3): consume the one-shot CLIENT CACHING flag now that
                    // this command's track decision has used it (cleared except on CLIENT CACHING).
                    consume_caching_flag(&mut conn, &request);
                    if slow_threshold >= 0 {
                        record_slow_command(&ctx, &env, &conn, &request, cmd_start, slow_threshold);
                    }
                    consumed_total += consumed;
                    // FIX1: the blocking command parked (`out` is empty). Write its IMMEDIATE
                    // non-blocking reply so it returns at once rather than hanging: the BLPOP-family
                    // pops get the nil-array timeout reply; WAIT gets the CURRENT in-sync replica
                    // count. The command counter was already bumped inside `handle_blocking_live` /
                    // `handle_wait_live` when the park was set up, so we ONLY encode here (no double
                    // count). `block_request == None` for every non-blocking command (the hot path),
                    // so this is a single `is_some` check then skipped.
                    if let Some(park) = block_request {
                        let reply = match park.spec.op {
                            ironcache_server::BlockOp::Wait { .. } => {
                                ironcache_server::Value::Integer(
                                    ironcache_server::in_sync_replica_count(&ctx),
                                )
                            }
                            _ => block_timeout_value(),
                        };
                        encode_into(&mut out, &reply, conn.proto);
                    }
                    if close {
                        let _ = rt.send(&mut stream, std::mem::take(&mut out)).await;
                        break 'conn;
                    }
                    // OUTPUT-BUFFER intra-batch cap (#529), identical to the tokio path: this
                    // command's reply is now in `out`; if the accumulated reply buffer has grown past
                    // the cap MID-BATCH, stop decoding and fall through to the post-batch drain +
                    // pre-flush check, which close the connection. Bounds resident output so a single
                    // pipelined batch of large-reply commands cannot OOM the host. At the default cap
                    // / `0` (disabled) this is a single never-taken compare on the hot path.
                    if obl > 0 && out.len() as u64 > obl {
                        break;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Error(e) => {
                    encode_into(&mut out, &ironcache_server::Value::Error(e), conn.proto);
                    let _ = rt.send(&mut stream, std::mem::take(&mut out)).await;
                    break 'conn;
                }
            }
        }

        // Drain the whole processed batch ONCE (the running-cursor deferral): one memmove removes all
        // consumed commands, leaving any partial trailing frame at the front. Done before the flush +
        // the subscriber-idle / recv paths below (which own `read_buf` and append to it).
        if consumed_total > 0 {
            read_buf.drain(..consumed_total);
        }

        // OUTPUT-BUFFER hard cap (PROD-SAFETY #5), identical to the tokio path: the pre-flush check,
        // using the `obl` read once at the top of this batch (shared with the intra-batch check).
        if obl > 0 && out.len() as u64 > obl {
            break;
        }

        if !out.is_empty() {
            match rt.send(&mut stream, std::mem::take(&mut out)).await {
                Ok(returned) => out = returned,
                Err(_) => break,
            }
        }

        // CLIENT KILL (PROD-7): close if a peer flagged this connection (cold relaxed load). FIX2:
        // also close if the publisher SHED this connection (its bounded push channel overflowed):
        // a flooded subscriber must be torn down rather than accumulate forever. For a
        // non-subscriber `shed_flag` is never tripped (no table sender registered), so this is a
        // single relaxed atomic load on the cold post-batch path.
        if client_handle.is_killed() || shed_flag.is_tripped() {
            break;
        }

        // CLIENT PAUSE (PROD-7, FIX3, #388): the pause stall is now PER-COMMAND, done in the decode
        // loop above (`pause_stall`) right before each command is dispatched, identical to the tokio
        // serve loop -- a WRITE pause holds only writes (reads + admin like SAVE proceed), an ALL
        // pause holds every command, and the very next command after a pause begins is held. There is
        // no post-batch pause work; the default (no pause) cost is the single relaxed atomic load per
        // command at that gate.

        // IDLE WAIT. The NON-subscriber path is the plain io_uring `recv` (byte-unchanged from
        // before this fix). A connection in SUBSCRIBE mode instead takes the `select!`-based
        // `subscriber_idle_wait_uring` (FIX2), which delivers a PUSH-while-idle promptly (it selects
        // on the push channel alongside the recv), coalesces queued pushes into one write, observes
        // the SHED kill-signal, and detects a peer close -- so pub/sub messages are no longer
        // silently dropped on the io_uring path. FIFO holds because `out` was flushed above before
        // this idle wait, so a push is rendered + sent only AFTER the in-flight command batch's
        // reply went out.
        if conn.is_subscriber() || conn.tracking_on {
            if subscriber_idle_wait_uring(
                &rt,
                &mut stream,
                &mut push_rx,
                &shed_flag,
                &mut read_buf,
                &mut out,
                conn.proto,
            )
            .await
            {
                break;
            }
        } else {
            // Read the next command batch. `recv_batch` uses this shard's REGISTERED fixed-buffer
            // datapath when the kernel selected it (the startup probe, #495/#496), else the owned
            // recv seam -- both APPEND into `read_buf`, preserving any partial-frame carryover, so
            // this is behavior-preserving for the pipelining model. A clean EOF (`n == 0`) or any
            // error closes the connection.
            let Ok(n) = ironcache_runtime::recv_batch(&rt, &mut stream, &mut read_buf).await else {
                break;
            };
            if n == 0 {
                break; // peer closed
            }
        }

        // QUERY-BUFFER hard cap (#528), identical to the tokio path: every idle-wait arm above (the
        // subscriber drain / the plain recv_batch) APPENDS the newly-arrived bytes into `read_buf`,
        // then this loop re-decodes. If the accumulated inbound query buffer exceeds the configured
        // `query_buffer_limit`, CLOSE the connection rather than let a slow-dribble multibulk force
        // unbounded pre-auth inbound buffering. A single cold relaxed load on this post-batch / idle
        // path; `0` disables the cap. The oversized buffer is dropped and the connection closed.
        let qbl = ctx.runtime.query_buffer_limit();
        if qbl > 0 && read_buf.len() as u64 > qbl {
            break;
        }
    }

    // Connection close: same shard-local cleanup the tokio path performs (subscription table,
    // dropped push_tx, watch deregistration, connection-close counter). The conn-gate + registry
    // guards release on Drop as this function returns.
    deregister_all_subscriptions(&conn);
    drop(push_tx);
    if !conn.watch.is_empty() {
        use ironcache_storage::Watch;
        store_rc.borrow_mut().unwatch(&conn.watch);
        conn.clear_watch();
    }
    state_rc.borrow_mut().counters.on_connection_close();
}

/// The SUBSCRIBE-mode idle wait (SERVER_PUSH.md #20, PR 91a): `select!` between draining the
/// per-connection push channel, observing the SHED kill-signal, and reading the next command.
/// Returns `true` when the connection should CLOSE (the push consumer was SHED for back-pressure,
/// a peer close, or an I/O error), `false` to keep looping. Split out of [`serve_connection`] so
/// the hot non-subscriber path stays a plain `rt.recv` and this select! lives in one place.
///
/// The select! is FAIR (no `biased;`, FIX E): a `biased` push-first poll could STARVE the read
/// arm for a fast-flooded subscriber (the push arm would always be ready first), so the connection
/// could never re-arm its command read; random fairness re-arms the read promptly. All three arms
/// are still polled each iteration, so a command is never starved and a ready push is still
/// delivered. A delivered push is COALESCED with any further already-queued pushes (`try_recv`,
/// non-blocking) into ONE write. The read branch reads into a FRESH buffer and APPENDS to
/// `read_buf`: had another branch won, a `read_buf` moved into the CANCELLED recv future would be
/// lost with any partial frame it held; reading into a temporary keeps `read_buf`'s partial bytes
/// safe across cancellation. No RefCell borrow is held across the `.await`s (render is pure; the
/// subscription table is untouched).
///
/// The SHED arm (FIX D) `await`s `shed.wait()`: when the publisher overflows this connection's
/// bound it trips the signal AND drops the table sender, but the serve loop holds its OWN
/// `push_tx` clone, so `push_rx.recv()` would NOT return `None`. The signal is the disconnect
/// trigger; `wait()` parks on the signal's `Notify` (SPIN-FREE, no busy poll), so a healthy
/// subscriber pays nothing for this arm.
async fn subscriber_idle_wait(
    stream: &mut ironcache_runtime::ClientStream,
    push_rx: &mut tokio::sync::mpsc::Receiver<crate::pubsub::ServerPush>,
    shed: &std::sync::Arc<crate::pubsub::ShedSignal>,
    read_buf: &mut Vec<u8>,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) -> bool {
    // Fast pre-check: if the publisher already shed this connection between iterations, close
    // now without entering the select! (the table sender is gone; nothing more will arrive).
    if shed.is_tripped() {
        return true;
    }
    tokio::select! {
        maybe_push = push_rx.recv() => {
            let Some(push) = maybe_push else {
                // Both the table sender(s) AND the serve loop's own push_tx are gone: the
                // channel is fully closed. Treat as a disconnect and close.
                return true;
            };
            // `out` was flushed before the idle wait, so it is empty here; rendering into it
            // preserves the flush-before-idle FIFO ordering.
            out.clear();
            encode_into(out, &push.render(proto), proto);
            while let Ok(next) = push_rx.try_recv() {
                encode_into(out, &next.render(proto), proto);
            }
            stream.send(std::mem::take(out)).await.map_or(true, |returned| {
                *out = returned;
                false
            })
        }
        () = shed.wait() => {
            // The publisher SHED this slow consumer (its push channel overflowed past the
            // bound): close the connection (its subscriptions are cleaned up on the close path).
            true
        }
        res = stream.recv(Vec::new()) => {
            let Ok(res) = res else { return true; };
            if res.n == 0 {
                return true; // peer closed
            }
            read_buf.extend_from_slice(&res.buf);
            false
        }
    }
}

/// The io_uring-path SUBSCRIBE-mode idle wait (FIX2, the io_uring analog of [`subscriber_idle_wait`]).
/// `select!`s between draining this connection's per-connection push channel, observing the SHED
/// kill-signal, and reading the next command over the io_uring owned-buffer `recv` seam. Returns
/// `true` when the connection should CLOSE (the push consumer was SHED for back-pressure, a peer
/// close, or an I/O error), `false` to keep looping.
///
/// This is a SEPARATE function from the tokio [`subscriber_idle_wait`] because the io_uring transport
/// is a distinct type ([`ironcache_runtime::UringTcpStream`] driven through the [`Runtime`] seam's
/// owned-buffer `recv`/`send`), not the tokio `ClientStream`. The fairness/coalescing/FIFO STRUCTURE
/// mirrors [`subscriber_idle_wait`], but one io_uring-path semantic differs and is a documented
/// limitation (a soak-time follow-up): tokio's `recv` is READINESS-based, so cancelling it (a push
/// or shed arm winning the `select!`) consumes no socket bytes; io_uring's `recv` is COMPLETION-based,
/// so a recv SQE that already consumed inbound client bytes into its buffer before the push/shed arm
/// wins is dropped (the buffer is kept alive by tokio-uring's Ignored lifecycle until the kernel CQE,
/// so this is memory-safe, but those consumed bytes are NOT re-read). In practice this can only lose
/// inbound bytes for a subscriber that is ACTIVELY PIPELINING commands while simultaneously receiving
/// a push (an uncommon pattern; an idle subscriber's cancelled recv consumed nothing). A true fix
/// (an AsyncCancel + drain, or not racing recv against pushes mid-command) is deferred to the Linux
/// soak where io_uring can be runtime-validated. The rest applies verbatim:
///   * NO `biased;`, so a fast-flooded subscriber never starves its command-read arm.
///   * a delivered push coalesces any further already-queued pushes (`try_recv`) into ONE write.
///   * the read arm reads into a FRESH `Vec::new()` and APPENDS to `read_buf`, so a partial frame
///     already in `read_buf` survives the recv future being cancelled when another arm wins. The
///     io_uring `recv` future keeps its OWN throwaway buffer alive until the kernel completes/cancels
///     the op (tokio-uring's cancel-on-drop contract) -- `read_buf` is never moved into it, so no
///     buffered bytes are at risk.
///
/// The SHED arm parks on the signal's `Notify` (spin-free); the fast pre-check closes promptly when
/// the publisher already shed this connection between iterations.
#[cfg(all(target_os = "linux", feature = "io_uring"))]
async fn subscriber_idle_wait_uring(
    rt: &ironcache_runtime::IoUringRuntime,
    stream: &mut ironcache_runtime::UringTcpStream,
    push_rx: &mut tokio::sync::mpsc::Receiver<crate::pubsub::ServerPush>,
    shed: &std::sync::Arc<crate::pubsub::ShedSignal>,
    read_buf: &mut Vec<u8>,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) -> bool {
    use ironcache_runtime::Runtime;
    // Fast pre-check: if the publisher already shed this connection between iterations, close now
    // without entering the select! (the table sender is gone; nothing more will arrive). This is
    // ALSO the close-on-shed for a pure-idle flooded subscriber (FIX2 non-negotiable).
    if shed.is_tripped() {
        return true;
    }
    tokio::select! {
        maybe_push = push_rx.recv() => {
            let Some(push) = maybe_push else {
                // Both the table sender(s) AND the serve loop's own push_tx are gone: the
                // channel is fully closed. Treat as a disconnect and close.
                return true;
            };
            // `out` was flushed before the idle wait, so it is empty here; rendering into it
            // preserves the flush-before-idle FIFO ordering.
            out.clear();
            encode_into(out, &push.render(proto), proto);
            while let Ok(next) = push_rx.try_recv() {
                encode_into(out, &next.render(proto), proto);
            }
            rt.send(stream, std::mem::take(out)).await.map_or(true, |returned| {
                *out = returned;
                false
            })
        }
        () = shed.wait() => {
            // The publisher SHED this slow consumer (its push channel overflowed past the bound):
            // close the connection (its subscriptions are cleaned up on the close path).
            true
        }
        res = rt.recv(stream, Vec::new()) => {
            let Ok(res) = res else { return true; };
            if res.n == 0 {
                return true; // peer closed
            }
            read_buf.extend_from_slice(&res.buf[..res.n]);
            false
        }
    }
}

/// Whether the SUBSCRIBE-MODE gate BLOCKS `cmd_upper` for this connection (SERVER_PUSH.md #20,
/// PR 91a), writing the byte-exact Redis error into `out` and bumping `commands_processed` when
/// it does. A RESP2 subscriber may run ONLY the pub/sub control set + PING/QUIT/RESET; RESP3 has
/// NO restriction. Returns `false` (does nothing) when the connection is not a RESP2 subscriber
/// or the command is allowed.
///
/// This gate is enforced in TWO places by necessity: `dispatch` checks it on the HOME path, but a
/// subscriber's KEYED command on a REMOTE-owned key takes the `dispatch_via` hop (and the multikey
/// / spanning / whole-keyspace fan-outs), which calls `dispatch_remote_keyed` DIRECTLY and BYPASSES
/// the dispatch gate -- so a RESP2 subscriber's remote GET would wrongly execute. Checking it HERE,
/// BEFORE the routing decision (exactly like the in-MULTI guards), gates EVERY route uniformly. The
/// pub/sub commands + the subscriber PING are already handled by `try_handle_pubsub`; QUIT/RESET are
/// AlwaysHome (allowed). With `shards == 1` the dispatch gate alone suffices, but this check is
/// byte-identical (same error), so single-shard parity holds.
fn subscriber_gate_blocks(
    conn: &ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    out: &mut Vec<u8>,
) -> bool {
    if !(conn.is_subscriber()
        && conn.proto == ProtoVersion::Resp2
        && !matches!(
            cmd_upper,
            b"SUBSCRIBE"
                | b"UNSUBSCRIBE"
                | b"PSUBSCRIBE"
                | b"PUNSUBSCRIBE"
                // Sharded Pub/Sub (#410): a RESP2 subscriber may also run the SHARD (un)subscribe
                // control commands (Redis allows SSUBSCRIBE/SUNSUBSCRIBE in subscribe mode). SPUBLISH
                // is NOT allowed (it is a publish, like PUBLISH, which the gate blocks).
                | b"SSUBSCRIBE"
                | b"SUNSUBSCRIBE"
                | b"PING"
                | b"QUIT"
                | b"RESET"
        ))
    {
        return false;
    }
    state_rc.borrow_mut().counters.on_command();
    let name = String::from_utf8_lossy(cmd_upper).to_ascii_lowercase();
    encode_into(
        out,
        &ironcache_server::Value::error(ironcache_protocol::ErrorReply::subscribe_mode(&name)),
        conn.proto,
    );
    true
}

/// The cluster slot-ownership decision for one command (CLUSTER_CONTRACT.md #70, slice 2):
/// returns `Some(error)` when the command must be REDIRECTED (`-MOVED`) or REJECTED
/// (`-CROSSSLOT`) because its key(s) are not served by THIS node, else `None` (proceed to the
/// normal local / internal-shard routing). It is a PURE function of `(map, route, cmd, request)`
/// and is the SINGLE source of the cluster redirect rule, reused by both the live command path
/// ([`route_and_dispatch`]) and the MULTI queue-time hook ([`route_in_multi`]).
///
/// The rules (matching Redis `getNodeByQuery` in src/cluster.c):
/// - KEYLESS / ADMIN commands carry no slot, so [`route::CommandClass::AlwaysHome`] and
///   [`route::CommandClass::WholeKeyspace`] are NEVER redirected (`-> None`). Only `KeyedSingle`
///   / `KeyedMulti` reach the slot logic.
/// - The CLIENT-VISIBLE slot is [`ironcache_protocol::key_slot`] (CRC16/XMODEM + the hash-tag
///   rule), NOT `route::hash64` (the internal FNV-1a shard hash): they answer different
///   questions (which NODE owns the wire slot vs which of MY shards owns the key).
/// - For a multi-key command, ALL keys must hash to ONE slot; if they SPAN slots the reply is
///   `-CROSSSLOT` and this is checked BEFORE ownership (a cross-slot command is CROSSSLOT even
///   when none of its slots is local, matching Redis), so cluster mode never scatters a
///   cross-slot multi-key command.
/// - A single resolved slot NOT owned by this node yields `-MOVED <slot> <owner host:port>`;
///   an owned (or co-located + owned) slot yields `None` and falls through unchanged.
///
/// A malformed / short request that yields no key ([`route::KeySpec::None`]) returns `None`, so
/// the home handler emits the proper wrong-arity error rather than a redirect.
/// Whether THIS node's replica link is currently IN SYNC within the configured
/// `replica_max_lag` (HA-8 replica-read staleness bound): the HA-7e `is_in_sync` signal off
/// `ctx.repl_status` (link up AND lag <= max_lag). Returns `false` when there is no repl-status
/// cell (the default static / non-raft path), which combined with the `readonly` gate keeps that
/// path's routing byte-unchanged (it never reaches the replica-serve leg anyway). Cold: reads a
/// handful of atomics off the node-level status cell, never per stored key.
fn replica_read_in_sync(ctx: &ServerContext) -> bool {
    ctx.repl_status
        .as_ref()
        .is_some_and(|s| s.is_in_sync(ctx.boot.replica_max_lag))
}

/// The HA-6 migration context the redirect needs that the static/WATCH paths do NOT: whether the
/// connection set the one-shot `ASKING` flag, and a resolver telling whether a given CLIENT-VISIBLE
/// key is PRESENT (and live) on the shard that OWNS it. The resolver is the only store-touching part
/// of the redirect; the serve path supplies it, so [`cluster_redirect`] / [`redirect_for_keys`]
/// stay pure functions over `(map, keys, ...)` plus this borrowed context. `None` (the static path,
/// raft-without-migration, and the WATCH guard) makes the redirect byte-identical to pre-HA-6: the
/// migration arms are reached ONLY when a slot is actually tagged MIGRATING/IMPORTING.
///
/// MULTI-SHARD EXACTNESS (COORDINATOR.md #107): the resolver is now EXACT on a multi-shard node. A
/// migrating-slot key lives on the shard it FNV-hashes to ([`route::owner_shard`]), which on a
/// multi-shard node may be a SIBLING of the accept shard. The serve path pre-resolves any such
/// non-home key on its owner shard via the coordinator presence hop ([`coordinator::presence_via`])
/// and feeds the EXACT owner-shard answer here, so the source ASKs only for genuinely absent keys
/// (no more spurious `-ASK` for a present sibling-shard key). With `shards == 1` every key is home,
/// so the resolver is a pure local `contains_live` read -- byte-identical to before this fix.
struct MigrationCtx<'a> {
    /// Whether the connection's one-shot `ASKING` flag is set for THIS command (consumed by the
    /// caller after dispatch). Gates serving an IMPORTING slot locally.
    asking: bool,
    /// Resolve whether a CLIENT-VISIBLE key is present-and-live on the shard that OWNS it (the home
    /// shard for a home key; a sibling shard's pre-resolved answer for a non-home key on a
    /// multi-shard node). Used to decide ASK (all keys gone) vs serve (all present) vs TRYAGAIN
    /// (mixed) on a MIGRATING slot.
    key_present: &'a dyn Fn(&[u8]) -> bool,
}

/// The host a shard-owner node ADVERTISES in its `CLUSTER SLOTS` projection (and thus in every
/// MOVED it emits). Clients must be able to DIAL it, so an unspecified bind (`0.0.0.0` / `::`) --
/// which is not a connectable address -- falls back to loopback (shard-owners is a single-box mode;
/// its clients/benches dial localhost). A concrete bind IP is advertised as-is. There is no
/// `cluster-announce-ip` knob today; adding one is a follow-up for the cross-host case.
fn shard_owner_announce_host(bind: std::net::IpAddr) -> String {
    if bind.is_unspecified() {
        match bind {
            std::net::IpAddr::V6(_) => "::1",
            std::net::IpAddr::V4(_) => "127.0.0.1",
        }
        .to_owned()
    } else {
        bind.to_string()
    }
}

/// The home `ShardId` to hand the cluster redirect, but ONLY in shard-owners mode (`Some(home)` ->
/// the per-shard ownership predicate in [`moved_if_unowned`]; `None` -> the default single-self-node
/// redirect used by Static/Raft, byte-unchanged). Centralizes the mode check so every redirect call
/// site agrees on when per-shard ownership applies.
fn shard_owner_home(ctx: &ServerContext, home: ShardId) -> Option<ShardId> {
    (ctx.cluster_mode() == ironcache_config::ClusterMode::ShardOwners).then_some(home)
}

// The cluster redirect predicate takes many orthogonal inputs (the map, the command's class + key
// spec, two read-gate flags, the migration context, and the shard-owner home). Bundling them would
// obscure more than it clarifies, so allow the extra parameter.
#[allow(clippy::too_many_arguments)]
fn cluster_redirect(
    map: &ironcache_cluster::SlotMap,
    route: route::CommandClass,
    cmd_upper: &[u8],
    request: &Request,
    readonly: bool,
    replica_in_sync: bool,
    migration: Option<&MigrationCtx<'_>>,
    home_owner: Option<ShardId>,
) -> Option<ironcache_protocol::ErrorReply> {
    // (a) keyless / admin exemption: only KEYED data commands carry slots.
    let spec = match route {
        route::CommandClass::KeyedSingle => match route::single_key(request) {
            Some(k) => route::KeySpec::One(k),
            None => return None, // malformed/short: home handler emits the arity error.
        },
        route::CommandClass::KeyedMulti => route::command_keys(cmd_upper, request),
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => return None,
    };

    // HA-7d replica-read gate (REPLICA_READ.md #147): a READ on a READONLY connection MAY be
    // served locally by a replica of the slot. A WRITE never is (it returns MOVED to the owner),
    // and a non-READONLY connection never is (the default strong-read behavior). The command's
    // write-ness comes from the #89 registry (`is_write`); an unknown command is treated as a
    // write (conservative), so a replica never serves an unrecognized command locally.
    //
    // HA-8 REPLICA-READ STALENESS BOUND (REPLICA_READ.md, finishing the 7d TODO): a replica may
    // serve the READONLY read ONLY while WITHIN the lag bound (link up AND lag <= max_lag, the
    // HA-7e `is_in_sync` signal, threaded in as `replica_in_sync`). Past the bound (or link down)
    // it is NOT in sync, so `replica_serves` is false and the slot it replicates-but-does-not-own
    // returns MOVED to the OWNER -- a stale replica never serves a stale read. In the default
    // static path there is no replication, so the caller passes `replica_in_sync = false` AND
    // `readonly` is the only other gate, keeping that path byte-unchanged (a static node owns its
    // slots, so it never reaches the replica leg regardless).
    let replica_serves =
        readonly && replica_in_sync && !ironcache_server::command_spec::is_write(cmd_upper);

    // (b) reduce the key(s) to a slot via the CLIENT-VISIBLE key_slot (CRC16 + hash-tag) and
    // apply the ONE shared redirect rule (CROSSSLOT-before-MOVED). The `route::KeySpec` is just
    // a borrowed view over the request bytes, so collapse it to an iterator of key slices and
    // hand it to `redirect_for_keys` (the SINGLE predicate WATCH also uses).
    match spec {
        // No routable key (malformed / short): fall through, the handler errors properly.
        route::KeySpec::None => None,
        route::KeySpec::One(k) => redirect_for_keys(
            map,
            std::iter::once(k),
            replica_serves,
            migration,
            home_owner,
        ),
        route::KeySpec::Many(keys) => redirect_for_keys(
            map,
            keys.iter().copied(),
            replica_serves,
            migration,
            home_owner,
        ),
    }
}

/// The SINGLE cluster redirect predicate over a sequence of CLIENT-VISIBLE keys: the one
/// place the CROSSSLOT-before-MOVED rule lives, shared by [`cluster_redirect`] (data commands)
/// and the WATCH cluster guard in [`route_and_dispatch`] (WATCH is `AlwaysHome` for
/// connection-state reasons but carries a key spec in Redis, so it must redirect like a keyed
/// command). Returns `Some(error)` when the keys must be REJECTED (`-CROSSSLOT`, they span
/// slots) or REDIRECTED (`-MOVED`, their single slot is foreign), else `None` (proceed local).
///
/// The rule, matching Redis `getNodeByQuery` (src/cluster.c):
/// - reduce each key to its slot via [`ironcache_protocol::key_slot`] (CRC16/XMODEM + hash-tag);
/// - if any key's slot differs from the first key's slot -> `-CROSSSLOT` (checked BEFORE
///   ownership: a cross-slot request is CROSSSLOT even when none of its slots is local);
/// - else the request resolves to ONE slot -> `-MOVED <slot> <owner host:port>` if this node
///   does not own it, else `None`.
///
/// An EMPTY key sequence yields `None` (no routable key: the home handler errors properly); it
/// cannot occur for a well-formed command but is handled defensively rather than indexing.
///
/// HA-6: when `migration` is `Some`, the single resolved slot's MIGRATING / IMPORTING state is
/// consulted AFTER CROSSSLOT but BEFORE the plain MOVED, producing `-ASK` / serve-locally /
/// `-TRYAGAIN` per [`migration_decision`]. When `None` (static / WATCH path) the migration arm is
/// skipped entirely and the result is byte-identical to pre-HA-6.
fn redirect_for_keys<'a, I>(
    map: &ironcache_cluster::SlotMap,
    keys: I,
    replica_serves: bool,
    migration: Option<&MigrationCtx<'_>>,
    home_owner: Option<ShardId>,
) -> Option<ironcache_protocol::ErrorReply>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    // Collect the keys into a slice so the migration arm can iterate them twice (presence per key);
    // for the common single-key command this is a one-element Vec and the CROSSSLOT loop is trivial.
    let key_vec: Vec<&[u8]> = keys.into_iter().collect();
    let first = *key_vec.first()?;
    let first_slot = ironcache_protocol::key_slot(first);
    // CROSSSLOT (keys span slots) takes precedence over MOVED/ASK, regardless of ownership: a
    // cross-slot request is rejected, never scattered.
    for &k in &key_vec[1..] {
        if ironcache_protocol::key_slot(k) != first_slot {
            return Some(ironcache_protocol::ErrorReply::crossslot());
        }
    }
    // HA-6: if the slot is mid-migration AND the caller supplied a migration context, the per-key
    // cutover decision (ASK / serve / TRYAGAIN / IMPORTING-ASKING) replaces the plain MOVED. The
    // function returns None to FALL THROUGH to the static decision below when the slot is not
    // migrating, so the default path is unchanged.
    if let Some(mig) = migration {
        if let Some(decision) = migration_decision(map, first_slot, &key_vec, mig) {
            return decision.into_reply();
        }
    }
    // All keys co-locate on one non-migrating slot: MOVED if this node neither owns nor (read-only)
    // replicates it. `replica_serves` carries the HA-7d READONLY-read gate (see `moved_if_unowned`).
    moved_if_unowned(map, first_slot, replica_serves, home_owner)
}

/// THE WRITE-SIDE replication guardrail decision (ADR-0026, Redis `min-replicas-to-write`). Returns
/// `Some(-NOREPLICAS)` when a WRITE to a slot THIS node owns must be REJECTED because fewer than
/// `min_replicas_to_write` replicas are currently in sync, else `None` (the write proceeds).
///
/// The CALLER has already established `ctx.boot.min_replicas_to_write > 0` (the byte-unchanged
/// short-circuit) and that the redirect returned `None` (so a keyed slot here is OWNED, not foreign
/// / read-replica-served). This function applies the remaining gates and is otherwise a PURE
/// decision over the context + the parsed request (it reads only the count atomic + the slot map +
/// the registry `is_write` bit; no store, no time, no rand):
///
/// 1. ONLY WRITES: a read command is never blocked (`is_write` from the #89 registry). An unknown
///    command is conservatively a write, but the redirect already passed it, so it is keyless/admin
///    (gate 3 then exempts it).
/// 2. ONLY in raft-mode with the count cell present (`ctx.in_sync_replicas` is `Some` iff raft-mode,
///    the same gate the cell is created under). `None` -> no guardrail (defensive; the caller's
///    `> 0` gate plus a static node having no cell already excludes this).
/// 3. ONLY OWNED KEYED slots: a keyless / admin / whole-keyspace command carries no slot, so it is
///    EXEMPT (Redis gates `min-replicas-to-write` on the per-command `is-write` + a key; a keyless
///    admin write like FLUSHALL is not slot-replicated through this path). A keyed command's slot is
///    resolved via the CLIENT-VISIBLE `key_slot`; the redirect guarantees this node OWNS it.
/// 4. THE QUORUM: reject when the in-sync replica count (`InSyncReplicas::count`, ONE relaxed load)
///    is BELOW `min_replicas_to_write`.
fn write_guardrail(
    ctx: &ServerContext,
    route: route::CommandClass,
    cmd_upper: &[u8],
    request: &Request,
) -> Option<ironcache_protocol::ErrorReply> {
    // (1) ONLY WRITES. A read is never blocked.
    if !ironcache_server::command_spec::is_write(cmd_upper) {
        return None;
    }
    // (2) The count cell exists ONLY in raft-mode (the same gate it is created under). Without it
    // there is no replication to gate on, so the guardrail does not apply.
    let in_sync = ctx.in_sync_replicas.as_deref()?;

    // (3) ONLY a KEYED slot this node OWNS. A keyless / admin / whole-keyspace command carries no
    // routable slot, so it is exempt (mirrors `cluster_redirect`'s keyless exemption). For a keyed
    // command we resolve its CLIENT-VISIBLE slot; the redirect already ensured this node owns it
    // (a foreign slot returned MOVED above and never reaches here), so an owned keyed write is the
    // only case that proceeds to the quorum check.
    let has_owned_keyed_slot = match route {
        route::CommandClass::KeyedSingle => route::single_key(request).is_some(),
        route::CommandClass::KeyedMulti => {
            // A multi-key write co-locates on one owned slot (CROSSSLOT was rejected by the redirect
            // above), so the presence of any key means an owned keyed slot is being written.
            !matches!(
                route::command_keys(cmd_upper, request),
                route::KeySpec::None
            )
        }
        // No slot: keyless / admin / whole-keyspace writes are exempt (not slot-replicated here).
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => false,
    };
    if !has_owned_keyed_slot {
        return None;
    }

    // (4) THE QUORUM: reject when too few replicas are currently in sync. ONE relaxed atomic load,
    // delegated to the pure decision so the gate is unit-testable without a ServerContext.
    write_guardrail_decision(ctx.boot.min_replicas_to_write, in_sync.count())
}

/// THE PURE write-side quorum decision (ADR-0026), split out of [`write_guardrail`] so the
/// reject/allow rule is unit-testable over plain values (no `ServerContext`, no atomics, no I/O).
/// Returns `Some(-NOREPLICAS)` when the live `in_sync_count` is BELOW the required
/// `min_replicas_to_write`, else `None` (the write proceeds). The CALLER has already applied the
/// is-write / owned-keyed-slot / raft-mode gates; this is only the final count compare.
///
/// `min_required == 0` would never reach here (the hot-path caller short-circuits on `> 0` before
/// touching the count), but it is handled correctly anyway: `count >= 0` always holds, so it
/// returns `None` (allow), which is the byte-unchanged default.
#[must_use]
fn write_guardrail_decision(
    min_required: u32,
    in_sync_count: usize,
) -> Option<ironcache_protocol::ErrorReply> {
    if (in_sync_count as u64) < u64::from(min_required) {
        Some(ironcache_protocol::ErrorReply::no_replicas())
    } else {
        None
    }
}

/// The outcome of the HA-6 migration redirect decision for one slot's keys. Distinct from a bare
/// `Option<ErrorReply>` so the "serve locally" outcome (None reply) is explicit and cannot be
/// confused with "not migrating, fall through to the static decision".
enum MigrationDecision {
    /// Serve the command locally (the keys are present here on a MIGRATING slot, or this is an
    /// IMPORTING slot with ASKING set). The redirect returns `None`.
    Serve,
    /// `-ASK <slot> <dest:port>`: every key has already migrated to the destination.
    Ask(ironcache_protocol::ErrorReply),
    /// `-TRYAGAIN ...`: a multi-key command on a MIGRATING slot whose keys are split.
    TryAgain,
}

impl MigrationDecision {
    /// The redirect reply for this decision: `None` to serve locally, `Some(error)` to redirect.
    fn into_reply(self) -> Option<ironcache_protocol::ErrorReply> {
        match self {
            MigrationDecision::Serve => None,
            MigrationDecision::Ask(reply) => Some(reply),
            MigrationDecision::TryAgain => Some(ironcache_protocol::ErrorReply::tryagain()),
        }
    }
}

/// THE HA-6 per-slot migration redirect decision (the heart of online slot migration). Returns
/// `Some(decision)` when `slot` is mid-migration in a way that overrides the plain MOVED/serve, or
/// `None` when the slot is NOT migrating (the caller falls through to the static MOVED/owns/replica
/// decision, so the default path is byte-unchanged).
///
/// The decision table (real Redis Cluster semantics, adapted to the Raft-committed map):
///
/// - Slot is MIGRATING toward `dest` AND THIS node still OWNS it (the SOURCE side):
///   * EVERY key is present locally -> Serve (the key has not migrated yet; serve it here).
///   * EVERY key is absent locally -> `-ASK <slot> <dest>` (migrated already / never existed; the
///     destination is where it lives now -- a ONE-TIME hint, NOT MOVED, ownership unchanged).
///   * MIXED (some present, some absent; only possible for a multi-key command) -> `-TRYAGAIN`
///     (cannot serve atomically on either side; the client retries as the migration converges).
/// - Slot is IMPORTING from `src` AND THIS node does NOT yet own it (the DESTINATION side):
///   * the connection set `ASKING` -> Serve (the migrated key has arrived; this is the second leg
///     of the ASK redirect).
///   * `ASKING` NOT set -> `None` (fall through to the static MOVED-to-owner: a client that lands
///     here without ASKING is talking to the wrong node for a slot it does not own yet).
/// - Any other combination (not migrating, or MIGRATING but this node does not own it, or IMPORTING
///   but already owns it) -> `None` (fall through to the static decision).
///
/// SAFETY: this never grants ownership; it only decides WHERE a request is served DURING the
/// migration window. Ownership transfers solely through the committed FLIP, after which the slot is
/// no longer MIGRATING/IMPORTING (the FLIP clears it) and this returns `None` -> the source serves
/// MOVED and the destination owns. So there is never a state where two nodes both serve a key as
/// owner: the source serves only present keys (handing absent ones to dest via ASK), and the dest
/// serves only under ASKING (or after it owns).
/// The NON-HOME keys whose presence the HA-6 migration ASK decision must resolve on a SIBLING
/// shard (COORDINATOR.md #107, the multi-shard exactness fix), each paired with its OWNER shard
/// index, or `None` when no cross-shard presence resolution is needed (so the caller uses the
/// byte-identical LOCAL `contains_live` resolver). The fast `None` short-circuit keeps the
/// `shards == 1` / default / hot path untouched.
///
/// Returns `None` (use the local resolver) UNLESS ALL of:
/// - there is more than one shard (`home.total > 1`); with one shard every key is home-owned, so
///   the local read is already exact -- this is the FIRST gate, so the single-shard path never
///   even looks at the slot or the keys (byte-identical to pre-fix);
/// - the command is KEYED (only `KeyedSingle` / `KeyedMulti` carry a slot the migration arm reads);
/// - the command's keys resolve to ONE slot (a CROSSSLOT multi-key command is rejected before the
///   migration arm, so presence is never consulted for it) and THIS node is MIGRATING that slot
///   (`MigrationState::Migrating` AND `owns(slot)` -- the ONLY arm of `migration_decision` that
///   calls `key_present`; IMPORTING / non-migrating slots never consult presence);
/// - at least one key is NOT home-owned (a key on a SIBLING shard, where the accept-shard read
///   could be wrong). When EVERY key is home-owned, the local read is exact and we return `None`.
///
/// When it returns `Some`, the vec holds EVERY non-home key of the migrating slot (deduplicated)
/// paired with its FNV `owner_shard` -- the SAME hash the coordinator routes a single-key op with,
/// so the presence hop lands on the shard that actually stores the key. Home-owned keys are
/// deliberately OMITTED (the caller resolves them locally), so a co-located subset still uses the
/// zero-hop local read. The migrating slot's keys all share ONE client-visible slot (CROSSSLOT is
/// enforced upstream) but may map to DIFFERENT internal FNV shards, so the multi-key case can yield
/// several owners -- one presence hop each (mirroring the coordinator's per-owner multi-key gather).
fn xshard_presence_keys(
    map: &ironcache_cluster::SlotMap,
    route: route::CommandClass,
    cmd_upper: &[u8],
    request: &Request,
    home: ShardId,
) -> Option<Vec<(Vec<u8>, usize)>> {
    use ironcache_cluster::MigrationState;
    // FIRST gate: a single-shard node never needs a cross-shard hop (every key is home-owned).
    // This short-circuits BEFORE touching the slot map or extracting keys, so the shards == 1
    // path is byte-identical to before this fix.
    if home.total <= 1 {
        return None;
    }
    // Only KEYED commands carry a slot the migration arm consults; reduce to the key spec exactly
    // as `cluster_redirect` does (so "which bytes are keys" cannot drift from the redirect).
    let spec = match route {
        route::CommandClass::KeyedSingle => match route::single_key(request) {
            Some(k) => route::KeySpec::One(k),
            None => return None,
        },
        route::CommandClass::KeyedMulti => route::command_keys(cmd_upper, request),
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => return None,
    };
    let keys: Vec<&[u8]> = match spec {
        route::KeySpec::None => return None,
        route::KeySpec::One(k) => vec![k],
        route::KeySpec::Many(ks) => ks,
    };
    let first = *keys.first()?;
    let slot = ironcache_protocol::key_slot(first);
    // A multi-key command spanning client-visible slots is rejected (-CROSSSLOT) BEFORE the
    // migration arm, so presence is never consulted for it; only resolve when every key shares the
    // first key's slot (the single-slot case the migration arm actually reaches).
    if keys[1..]
        .iter()
        .any(|k| ironcache_protocol::key_slot(k) != slot)
    {
        return None;
    }
    // Presence is consulted ONLY on the migration SOURCE arm (MIGRATING + this node owns the slot);
    // IMPORTING / non-migrating slots never call `key_present`, so no hop is needed for them.
    if !(map.migration_state(slot) == MigrationState::Migrating && map.owns(slot)) {
        return None;
    }
    // Collect the NON-home keys (deduplicated) with their owner shard. A home-owned key is resolved
    // locally by the caller (zero hop), so omit it. If EVERY key is home-owned, return None so the
    // caller uses the pure local resolver (no cross-shard work at all).
    let mut remote: Vec<(Vec<u8>, usize)> = Vec::new();
    for &k in &keys {
        let owner = route::owner_shard(k, home.total);
        if owner != home.index && !remote.iter().any(|(existing, _)| existing.as_slice() == k) {
            remote.push((k.to_vec(), owner));
        }
    }
    if remote.is_empty() {
        None
    } else {
        Some(remote)
    }
}

fn migration_decision(
    map: &ironcache_cluster::SlotMap,
    slot: u16,
    keys: &[&[u8]],
    mig: &MigrationCtx<'_>,
) -> Option<MigrationDecision> {
    use ironcache_cluster::MigrationState;
    match map.migration_state(slot) {
        MigrationState::Migrating if map.owns(slot) => {
            // SOURCE side: decide by local key presence.
            let mut any_present = false;
            let mut any_absent = false;
            for &k in keys {
                if (mig.key_present)(k) {
                    any_present = true;
                } else {
                    any_absent = true;
                }
            }
            if any_present && any_absent {
                // Multi-key split across the cutover: cannot serve atomically -> TRYAGAIN.
                Some(MigrationDecision::TryAgain)
            } else if any_absent {
                // All keys gone (migrated / never existed): ASK to the destination. The dest
                // endpoint must resolve; if it somehow does not (peer forgotten mid-migration),
                // `map()` yields None and we fall through to the static decision rather than dial a
                // nonexistent node.
                map.migration_peer_endpoint(slot).map(|(host, port)| {
                    MigrationDecision::Ask(ironcache_protocol::ErrorReply::ask(
                        slot,
                        &format!("{host}:{port}"),
                    ))
                })
            } else {
                // All keys present: serve locally (not migrated yet).
                Some(MigrationDecision::Serve)
            }
        }
        MigrationState::Importing if !map.owns(slot) => {
            // DESTINATION side: serve locally ONLY under ASKING; otherwise fall through to MOVED.
            if mig.asking {
                Some(MigrationDecision::Serve)
            } else {
                None
            }
        }
        // Not migrating, or a migration tag that does not match this node's ownership (e.g. a stale
        // MIGRATING tag on a node that no longer owns the slot): fall through to the static rule.
        _ => None,
    }
}

/// `Some(-MOVED <slot> <owner host:port>)` when THIS node does not own `slot` (and does not serve
/// it as a read-only replica), else `None`.
///
/// `replica_serves` is the HA-7d replica-read gate: `true` when the request is a READ on a
/// READONLY connection (computed by the caller from `conn.readonly && !is_write`). When set, a
/// slot this node does NOT own but IS a committed replica of ([`SlotMap::is_replica_of_self`]) is
/// served LOCALLY (returns `None`), the replica-read leg of REPLICA_READ.md #147. A write, a
/// non-READONLY read, or a slot this node neither owns nor replicates still returns `-MOVED` to
/// the OWNER. `replica_serves` is `false` for every non-replica/non-readonly path, so the default
/// (owner-only) routing is byte-unchanged; the cold `is_replica_of_self` check runs ONLY when a
/// slot is already known foreign AND the connection opted into replica reads.
///
/// The redirect target is the OWNER node's advertised `host:port` (what the client should dial),
/// never the bind address. `moved_target` resolves the owner's advertised endpoint under the
/// node lock (the COLD redirect path); the `?` on its `None` (an unassigned slot) is defensive
/// (an empty-self / mid-formation node may not yet own the slot, so we simply do not redirect
/// rather than dial a nonexistent owner).
fn moved_if_unowned(
    map: &ironcache_cluster::SlotMap,
    slot: u16,
    replica_serves: bool,
    home_owner: Option<ShardId>,
) -> Option<ironcache_protocol::ErrorReply> {
    // SHARD-OWNERS (#517 PR4): the projection map has N nodes (one per shard), but every shard shares
    // ONE `ctx.cluster`, so `map.owns(slot)` (which asks the SINGLE self-node) cannot tell shard i
    // from shard j. Instead this shard owns `slot` iff the CONTIGUOUS partition maps it here --
    // `slot_to_shard(slot, N) == home.index` -- the SAME predicate the internal hop uses, so when a
    // client dialed the right owner port (homed here) it serves locally with neither MOVED nor hop.
    // A foreign slot is MOVED to its owner's advertised `host:base+owner` (resolved from the N-node
    // map). Replica reads do not apply in shard-owners mode (no replication), so that leg is skipped.
    if let Some(home) = home_owner {
        if route::slot_to_shard(slot, home.total) == home.index {
            return None;
        }
        let (host, port) = map.moved_target(slot)?;
        return Some(ironcache_protocol::ErrorReply::moved(
            slot,
            &format!("{host}:{port}"),
        ));
    }
    if map.owns(slot) {
        return None;
    }
    // HA-7d replica read: a READONLY read for a slot this node replicates is served locally.
    if replica_serves && map.is_replica_of_self(slot) {
        return None;
    }
    let (host, port) = map.moved_target(slot)?;
    Some(ironcache_protocol::ErrorReply::moved(
        slot,
        &format!("{host}:{port}"),
    ))
}

/// ROUTE + DISPATCH one decoded request (COORDINATOR.md #107, Stage 1), appending its
/// encoded reply to `out` and returning whether the connection should close (QUIT). Split
/// out of the serve loop so the connection loop stays small; the routing decision is:
///
/// - KEYED (single/multi) command whose key(s) ALL resolve to ONE shard -> that shard:
///   the LOCAL fast path (sync `handle_request`) when it is home, else a single remote HOP
///   ([`coordinator::dispatch_via`]). A key-SPANNING multi-key command stays HOME (the
///   documented Stage 2 fan-out gap).
/// - WHOLE-KEYSPACE (KEYS/SCAN/DBSIZE/FLUSHALL/FLUSHDB/RANDOMKEY) -> SCATTER-GATHER across
///   ALL shards so it covers the WHOLE keyspace (not just the home shard's ~1/N): SCAN is a
///   single-shard-per-call COMPOSITE-cursor walk ([`crate::whole_keyspace::scan_cross_shard`]),
///   the rest broadcast + merge ([`crate::whole_keyspace::fan_out_and_merge`]).
/// - AlwaysHome (control/conn/txn, SWAPDB, unknown) -> HOME (sync `handle_request`).
///
/// With shards == 1 every key is home-owned and the fan-out degenerates to the single local
/// call, so the whole path is byte-identical (no channel) to before this layer.
///
/// The per-connection `commands_processed` is bumped here for the remote / fan-out paths
/// (matching the bump `handle_request` does on the home path), so every command is counted
/// exactly once regardless of route.
///
/// The router enforces a STRICT ORDER for the pub/sub-related gates (the root-cause fix for the
/// adversarial-review findings): the internal-verb gate (FIX F), then the in-MULTI pub/sub REJECT
/// (FIX C), then the RESP2 subscribe-mode gate (FIX B, MOVED to run BEFORE pub/sub interception so
/// a RESP2 subscriber's PUBLISH/PUBSUB is rejected), then RESET interception (FIX A, deregisters
/// subscriptions + swaps the push channel), then `try_handle_pubsub`. The previous order ran the
/// pub/sub interception BEFORE both the in-MULTI gate and the subscribe-mode gate, so pub/sub
/// commands BYPASSED both; the order below closes that hole.
///
/// `too_many_lines` is allowed: this is the connection's central ROUTING HUB (the internal-verb
/// gate, the in-MULTI pub/sub reject, the subscribe-mode gate, RESET interception, the pub/sub
/// interception, the in-MULTI/WATCH guards, then the keyed / multikey / spanning / whole-keyspace
/// / home branches), each a documented decision the router must make in one place; splitting it
/// further would scatter the routing contract. The same precedent as `dispatch_inner` /
/// `command_spec::spec_of`.
/// HA-6: consume the one-shot `ASKING` flag for THIS command and return whether it was set.
///
/// `ASKING` itself just SETS the flag (handled in the router) and must NOT consume the flag it is
/// about to set, so for the `ASKING` command this returns `false` WITHOUT touching `conn.asking`.
/// For EVERY other command it reads the flag, CLEARS it, and returns its prior value. Calling this
/// EXACTLY ONCE at the top of `route_and_dispatch` -- before any early return (pubsub / in_multi /
/// WATCH) -- is what guarantees a set flag can never LEAK into a later command (the adversarial-
/// review Finding 1 hole). It is a single bool read+write; a non-cluster / non-migrating connection
/// never sets `asking`, so the value is always `false` and the static path is unaffected.
fn consume_one_shot_asking(cmd_upper: &[u8], conn: &mut ConnState) -> bool {
    if cmd_upper == b"ASKING" {
        false
    } else {
        let a = conn.asking;
        conn.asking = false;
        a
    }
}

/// Whether `request` is `CLIENT UNPAUSE` (case-insensitive), the pause-RECOVERY command that
/// [`pause_stall`] must never hold. Cheap and short-circuiting: it checks the `CLIENT` token first
/// (so a non-CLIENT command bails after one compare) and only then the `UNPAUSE` subcommand. Reached
/// ONLY when a pause is armed, never on the default hot path.
fn request_is_client_unpause(request: &Request) -> bool {
    request.args.len() == 2
        && request.args[0].eq_ignore_ascii_case(b"CLIENT")
        && request.args[1].eq_ignore_ascii_case(b"UNPAUSE")
}

/// The PER-COMMAND `CLIENT PAUSE` gate (#388, write-aware). Called in the serve loop's decode loop
/// for EACH decoded command, right BEFORE it is dispatched, so the command is HELD while a pause
/// that applies to it is active and released the instant the window clears or `CLIENT UNPAUSE` runs.
/// This is the SINGLE point where both pause kinds are honored, which is why it is correct under
/// PIPELINING (in a batch of mixed reads + writes under a WRITE pause, each read passes here and
/// each write holds here) and why it holds the VERY NEXT command after a pause begins (the old
/// post-batch stall let the first command after a pause slip through, since it stalled only AFTER
/// replying to the current batch):
///
/// * an ALL pause holds EVERY command (reads included), matching the prior ALL behavior;
/// * a WRITE-only pause holds ONLY writes -- reads + admin (PING/INFO/SAVE/...) flow straight
///   through -- making `CLIENT PAUSE WRITE` genuinely write-only (Redis semantics). This is the fix
///   for the ironcache-upgrade write-freeze, where the upgrade issues `CLIENT PAUSE WRITE` then
///   `SAVE`: the old superset stall held the SAVE too, deadlocking the upgrade's own snapshot.
///
/// HOT PATH (no pause): a SINGLE relaxed atomic load via [`ClientRegistry::is_pause_armed`] returns
/// `false`, so this returns immediately -- NO clock read, NO command uppercasing, NO classification,
/// NO further work. The default (never-paused) connection therefore pays only that one load per
/// command, and the rest of this function is never entered (the byte-identical hot path).
///
/// When a pause IS armed it reads the kind once. For a WRITE-only pause it classifies the command
/// via [`request_is_write_for_pause`] (which also covers `EXEC` of a write-containing transaction
/// and does NOT hold a command merely being queued inside a `MULTI`); a non-write proceeds at once.
/// A held command stalls in a short poll loop (a ~50ms quantum + an Env-monotonic deadline + the
/// Runtime timer seam) until the relevant remaining-ms reaches `0` (window expiry or `CLIENT
/// UNPAUSE`) or the connection is `CLIENT KILL`ed. It does NOT itself close the connection: the
/// caller re-checks `client_handle.is_killed()` after dispatch.
async fn pause_stall<T: Runtime>(
    ctx: &ServerContext,
    conn: &ConnState,
    request: &Request,
    env: &Rc<RefCell<SystemEnv>>,
    timer_rt: &T,
    client_handle: &ironcache_observe::ClientHandle,
) {
    // The single cheap guard: nothing recorded -> return without touching the clock, UPPERCASING the
    // command token, or classifying it. This keeps the default (no pause) hot path to one relaxed
    // atomic load per command.
    if !ctx.clients.is_pause_armed() {
        return;
    }
    // RECOVERY EXEMPTION: `CLIENT UNPAUSE` is NEVER held by a pause -- it is the command that LIFTS
    // the pause, so an ALL pause (which otherwise holds every command) would be UNRECOVERABLE from
    // the very connection that set it if its own UNPAUSE were stalled. This is the un-wedge the
    // ironcache-upgrade safe-abort relies on. (Under a WRITE pause, CLIENT is already a non-write and
    // passes the classifier below; the exemption is only load-bearing for an ALL pause.) Cheap: only
    // reached when a pause is armed, and short-circuits on the `CLIENT` token before the subcommand
    // compare.
    if request_is_client_unpause(request) {
        return;
    }
    // A pause is armed. A WRITE-only pause holds ONLY writes; an ALL pause holds everything. For a
    // WRITE pause, classify the command (uppercasing is only paid on this cold, paused path) and let
    // a non-write through. For an ALL pause every command is held.
    if ctx.clients.pause_is_writes_only() {
        let cmd_upper = ascii_upper(request.command());
        if !ironcache_server::request_is_write_for_pause(&cmd_upper, conn.in_multi, &conn.queued) {
            return;
        }
    }
    // Hold this command until the applicable pause window clears (a write is blocked by BOTH kinds;
    // a non-write reaches here only under an ALL pause), an UNPAUSE clears it, or the connection is
    // killed. `pause_write_remaining_ms` is the raw window for either kind, so it is the correct
    // remaining for both the write-under-any-pause case and the any-command-under-ALL case.
    loop {
        let now_mono_ms = env.borrow().now().as_millis();
        let remaining = ctx.clients.pause_write_remaining_ms(now_mono_ms);
        if remaining == 0 {
            break;
        }
        if client_handle.is_killed() {
            break;
        }
        let wait = remaining.min(50);
        timer_rt
            .timer(core::time::Duration::from_millis(wait))
            .await;
    }
}

/// A request to PARK a connection on a blocking command (PROD-9). When [`route_and_dispatch`]
/// finds a blocking pop's keys all empty (and the connection is NOT in MULTI), it sets the
/// serve loop's `block_request` out-param to this instead of replying, so the OWNING serve loop
/// (which holds the stream + the timer + the read buffer) runs the park loop: it registers a
/// waiter, `select!`s on (the wake / the timeout / a peer close), and on a wake re-attempts the
/// pop. The spec + db are everything the re-attempt needs; the home shard owns the keys.
pub(crate) struct BlockPark {
    /// The parsed blocking command (timeout + keys + op): the re-attempt reuses it.
    spec: ironcache_server::BlockSpec,
    /// The connection's selected DB at park time (the re-attempt + the waiter key are db-scoped).
    db: u32,
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn route_and_dispatch(
    ctx: &ServerContext,
    conn: &mut ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    push_tx: &mut tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    push_rx: &mut tokio::sync::mpsc::Receiver<crate::pubsub::ServerPush>,
    shed_flag: &mut std::sync::Arc<crate::pubsub::ShedSignal>,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
    persist: Option<&Arc<crate::persist::PersistState>>,
    request: &Request,
    out: &mut Vec<u8>,
    block_request: &mut Option<BlockPark>,
    // CROSS-SHARD HOP OVERLAP (#8): when `defer_hops` is true (the tokio serve loop opts in), a
    // single-target remote hop ENQUEUES its ShardWork and returns the reply receiver via
    // `deferred_hop` INSTEAD of awaiting it inline -- so a pipeline of hops runs concurrently (the
    // owner drains the run FIFO) rather than N serialized round-trips. The caller parks the receiver
    // and drains it in order. When `defer_hops` is false (io_uring loop, or any non-pipelined caller)
    // the hop is awaited inline exactly as before and `deferred_hop` stays `None` -- byte-identical.
    defer_hops: bool,
    deferred_hop: &mut coordinator::HopOutcome,
) -> bool {
    let cmd_upper = ascii_upper(request.command());
    let route = route::classify(&cmd_upper);

    // -- THE HOISTED NOAUTH CHOKEPOINT (production security fix). This is the SINGLE earliest
    // point after the command name is known but BEFORE any interception, cross-shard fan-out,
    // CLUSTER-mutator proposal, persistence/shutdown handling, MULTI queueing, or local dispatch.
    // EVERY client command reaches dispatch THROUGH this router, so gating here closes -- in one
    // place -- the whole class of auth-bypass holes that existed when the gate lived DOWNSTREAM in
    // `dispatch_inner` (which the router's early-returning forks never reach):
    //   * a GET/SET on a FOREIGN-shard key (the `coordinator::dispatch_via` remote hop below),
    //   * the whole-keyspace fan-outs (KEYS/SCAN/DBSIZE/FLUSHDB/FLUSHALL/RANDOMKEY),
    //   * the multi-key + spanning-combine scatter-gather fan-outs,
    //   * the CLUSTER topology mutators (MEET/FORGET/ADDSLOTS/SETSLOT/DELSLOTS/REPLICATE/
    //     SET-CONFIG-EPOCH, whether handled synchronously by `cmd_cluster` or proposed via Raft),
    //   * SAVE/BGSAVE/LASTSAVE + SHUTDOWN (previously point-fixed inline; now gated here too),
    //   * a command issued INSIDE a MULTI (the `route_in_multi` queue path is downstream of here,
    //     so a queued command from an unauth client is rejected, never staged -- Redis parity).
    // The pre-auth allow-list is the EXACT shared `command_allowed_pre_auth` predicate the
    // downstream `dispatch_with_cmd` gate uses (AUTH/HELLO/QUIT/RESET), so the two can never
    // diverge and AUTH / HELLO AUTH still work pre-auth unchanged.
    //
    // DEFAULT (no requirepass) is BYTE-UNCHANGED + adds no cost: `ctx.requires_auth()` reads the
    // runtime requirepass overlay (the same load the connection's `authenticated` init + the
    // dispatch gate already do) and short-circuits the `&&` immediately, so an authed or
    // no-auth-configured connection pays at most this single bool check before falling through to
    // the identical routing below. The reply is the IDENTICAL `-NOAUTH` the dispatch gate emits.
    if ctx.requires_auth()
        && !conn.authenticated
        && !ironcache_server::command_allowed_pre_auth(&cmd_upper)
    {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::noauth()),
            conn.proto,
        );
        return false;
    }

    // -- LIVE-REVOCATION RE-RESOLVE (#106, F1). Run ONCE per command right BEFORE the ACL
    // enforcement chokepoint, so a mid-session `ACL SETUSER` / `ACL DELUSER` / `ACL LOAD` reaches
    // this already-AUTHed connection on its VERY NEXT command (was fail-open until reconnect,
    // diverging from Redis which revokes live). HOT PATH: one relaxed atomic load + integer
    // compare of the registry generation against the connection's cached generation; on the no-ACL
    // path (and whenever no `ACL` admin verb has run since this connection cached its user) the
    // generations match and this returns immediately -- byte-unchanged. ONLY when the generation
    // MOVED (rare) does it take the registry lock to re-resolve the connection's user by name. A
    // `false` return means the connection's user was DELUSER'd: it is now deauthenticated, so we
    // reply NOAUTH and CLOSE it (Redis kills a deleted user's clients).
    if !ironcache_server::acl_resolve_if_stale(ctx, conn) {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::noauth()),
            conn.proto,
        );
        return true;
    }

    // -- THE HOISTED ACL ENFORCEMENT CHOKEPOINT (#106). Immediately AFTER the NOAUTH gate and
    // BEFORE any interception / cross-shard fan-out / CLUSTER-mutator / persistence / MULTI
    // queueing / local dispatch, so per-command + per-key + per-channel authorization covers
    // EVERY command path in ONE place (the same reason the NOAUTH gate is hoisted here). The
    // connection's authenticated ACL identity (`conn.acl_user`, `None` == the implicit all-
    // permissive default) was cached at AUTH time, so this check is LOCK-FREE: it reads the
    // cached `Arc<User>`, never the ACL registry.
    //
    // DEFAULT (no ACL config) is BYTE-UNCHANGED + adds at most ~two bool tests: `acl_user` is
    // `None` for every connection on the no-ACL path, so `acl_enforce` returns `None` after a
    // single match, and `ctx.acl.is_acl_active()` is one relaxed atomic load that is `false`.
    // Only an ACL-governed connection (a narrowed `Some(user)`) pays for the command/key/
    // channel checks. A DENY short-circuits with the `-NOPERM` reply, exactly like the NOAUTH
    // gate above, and never reaches routing / dispatch.
    if let Some(deny) = ironcache_server::acl_enforce(
        ctx.acl.is_acl_active(),
        conn.acl_user.as_deref(),
        &cmd_upper,
        request,
    ) {
        state_rc.borrow_mut().counters.on_command();
        encode_into(out, &ironcache_server::Value::error(deny), conn.proto);
        return false;
    }

    // HA-6: consume the one-shot ASKING EXACTLY ONCE PER COMMAND, BEFORE any early return
    // (pubsub interception / in_multi / WATCH cluster-redirect / WATCH cross-shard / the internal-
    // verb gate), so a set flag can NEVER leak into a later command. Previously the flag was
    // captured + cleared only inside the `ctx.cluster` block below, but the early returns above it
    // left `conn.asking` still true: `ASKING` then `SUBSCRIBE ch` (pubsub early return) then `GET
    // <key in an IMPORTING slot>` would see asking == true and serve LOCALLY on a node that does
    // NOT own the slot (a following `SET` there writes an orphaned key -> divergence / lost write
    // on a migration abort). `ASKING` itself (the command, handled below) must NOT clear the flag
    // it is about to set, so it is excluded here. Capturing for every OTHER command -- including
    // the early-returning ones -- means the flag is consumed once and the leak is closed; the
    // captured `asking` local is read by the migration redirect in the cluster block. A non-cluster
    // / non-migrating connection never sets `asking`, so this is a single bool read+write on the
    // cold path and the default static path is byte-unchanged.
    let asking = consume_one_shot_asking(&cmd_upper, conn);

    // HA-6 ASKING-IN-MULTI: carry the PRE-MULTI one-shot ASKING into the transaction it opens. The
    // one-shot is consumed PER COMMAND above (so it cannot LEAK past a command), which would
    // otherwise drop a flag set by `ASKING` BEFORE `MULTI` before the transaction's commands are
    // QUEUED. Redis keeps the single `CLIENT_ASKING` flag live across the MULTI queueing phase (its
    // cluster redirect runs at QUEUE time), so `ASKING; MULTI; <cmd on an IMPORTING slot>; EXEC`
    // queues + serves on the importing destination. We mirror that by recording the consumed
    // `asking` into the transaction-scoped `conn.txn_asking` for the MULTI that OPENS a transaction
    // (a nested `MULTI` is `in_multi` already and routes through `route_in_multi`, so it never
    // reaches here). The queue-time redirect in `route_in_multi` consults `txn_asking`; `clear_txn`
    // / `reset` clear it on EXEC / DISCARD / RESET, so it can NEVER leak past the transaction. On a
    // non-cluster / non-migrating connection `asking` is always false, so this is a single cold
    // bool write and the default path is byte-unchanged.
    if cmd_upper == b"MULTI" && !conn.in_multi {
        conn.txn_asking = asking;
    }

    // -- INTERNAL-VERB CLIENT GATE (COORDINATOR.md #107, Stage 2b). `__ICSTORESET` /
    // `__ICSTOREZSET` / `__ICSTOREHLL` are the coordinator's INTERNAL cross-shard *STORE
    // dest-write verbs (set / zset / PFMERGE-HLL): each lives in the command registry + has a
    // real dispatch arm (so it routes / admits like any keyed write and the registry-vs-dispatch
    // cross-check stays exact) but must be UNREACHABLE from clients -- only the coordinator
    // issues them (via `dispatch_one_value` / `run_local_keyed`, which call
    // `dispatch_remote_keyed` DIRECTLY and never pass through this router). A CLIENT socket only
    // ever reaches dispatch THROUGH this router, so rejecting them here -- before any routing or
    // queueing -- makes a client `__ICSTORE*` (in or out of MULTI) get the standard
    // unknown-command error while the coordinator's internal path is untouched.
    if cmd_upper == ironcache_server::ICSTORESET
        || cmd_upper == ironcache_server::ICSTOREZSET
        || cmd_upper == ironcache_server::ICSTOREHLL
        // `__ICPUBLISH` is the INTERNAL cross-shard PUBLISH fan-out verb (SERVER_PUSH.md #20, PR
        // 91a): in the registry so the cross-check stays exact, but client-unreachable -- only
        // the coordinator issues it (via the inbox). Reject a CLIENT `__ICPUBLISH` here with the
        // same unknown-command reply as the *STORE verbs.
        || cmd_upper == ironcache_server::ICPUBLISH
        // `__ICSPUBLISH` is the INTERNAL cross-shard SHARDED-PUBLISH fan-out verb (#410): the same
        // gate as `__ICPUBLISH` -- registry-present (cross-check exact) but client-unreachable; only
        // the coordinator issues it.
        || cmd_upper == ironcache_server::ICSPUBLISH
        // `__ICPUBSUB` is the INTERNAL cross-shard PUBSUB-introspection gather verb (SERVER_PUSH.md
        // #20, PR 91b): the same gate -- registry-present (cross-check exact) but client-
        // unreachable; only the coordinator issues it (via the inbox per shard).
        || cmd_upper == ironcache_server::ICPUBSUB
        // `__ICEXISTS` is the INTERNAL cross-shard KEY-PRESENCE query (HA-6 multi-shard migration,
        // COORDINATOR.md #107): the same gate -- client-unreachable; only the coordinator issues it
        // (via `coordinator::presence_via` to the key's owner shard). It is NOT in the `spec_of`
        // registry (it is dispatched directly, never classified), so a client sending it would
        // already fall to the unknown-command home arm; rejecting it HERE keeps the contract
        // explicit and uniform with the other internal verbs.
        || cmd_upper == ironcache_server::ICEXISTS
        // `__ICSAVE` is the INTERNAL cross-shard SAVE fan-out verb (#58 persistence): the same gate
        // -- client-unreachable; only the home core issues it (via `do_save_all`'s `fan_out_split`
        // to each shard's drain loop, which dumps that shard's partition). Like `__ICEXISTS` it is
        // NOT in the `spec_of` registry (dispatched directly by the coordinator), so a client
        // sending it would already get unknown-command; rejecting it HERE keeps the contract uniform.
        || cmd_upper == crate::persist::ICSAVE
        // `__ICCOUNTKEYSINSLOT` / `__ICGETKEYSINSLOT` are the INTERNAL #371 slot-scan whole-keyspace
        // verbs the serve loop rewrites a cluster-mode `CLUSTER COUNTKEYSINSLOT`/`GETKEYSINSLOT` into;
        // a client must never reach them directly. Like `__ICEXISTS`/`__ICSAVE` they are not in
        // `spec_of` (a client sending one already gets unknown-command via the home arm), but gating
        // them here keeps the contract explicit and uniform.
        || cmd_upper == ironcache_server::ICCOUNTKEYSINSLOT
        || cmd_upper == ironcache_server::ICGETKEYSINSLOT
    {
        // FIX F: when a client issues an internal verb INSIDE a MULTI, dirty the transaction in
        // addition to replying the unknown-command error, so EXEC returns -EXECABORT exactly as
        // a genuine unknown command would (the queue gate dirties an unknown command at queue
        // time; this router intercepts the internal verb BEFORE that gate, so it must dirty here).
        reject_internal_verb(conn, state_rc, request, out);
        if conn.in_multi {
            conn.dirty_exec = true;
        }
        return false;
    }

    // -- GRACEFUL SHUTDOWN INTERCEPTION (#139, SHUTDOWN.md): SHUTDOWN [NOSAVE|SAVE]. The process
    // exit + the save-on-exit live HERE in the serve layer (it owns the runtime, the per-shard
    // stores, the data_dir, and the env Clock for the save timestamp); the generic dispatch sees
    // only the storage waist and cannot exit the process, so it MUST be intercepted before it. This
    // runs REGARDLESS of whether persistence is configured (NOSAVE / a bare SHUTDOWN with no save
    // policy exits without saving even when `persist` is `None`), so it is OUTSIDE the persistence
    // `Some` block below. Gated `!conn.in_multi` exactly like SAVE: a SHUTDOWN inside a MULTI falls
    // through to the dispatch fallback at EXEC (a documented minor divergence). On a successful stop
    // this NEVER returns (the process exits 0); on a refused save (a SAVE/policy save that fails) it
    // replies an error and does NOT exit, so the connection keeps serving. A non-SHUTDOWN command
    // never enters this block, so the hot path is byte-unchanged.
    if !conn.in_multi && cmd_upper == b"SHUTDOWN" {
        handle_shutdown_command(persist, ctx, conn, home, inbox, request, out).await;
        return false;
    }

    // -- ACL COMMAND INTERCEPTION (#106). The `ACL` admin family (WHOAMI/LIST/USERS/GETUSER/
    // SETUSER/DELUSER/CAT/GENPASS/SAVE/LOAD) is handled HERE in the serve layer (like CONFIG /
    // persistence) because it mutates the shared `ctx.acl` registry and SAVE/LOAD do aclfile
    // I/O the server crate (no std::fs by policy on the data path) does not own. It is gated
    // `!conn.in_multi` exactly like SAVE/SHUTDOWN: an ACL inside a MULTI falls through to the
    // generic dispatch (which has no ACL arm -> the standard unknown-command path), a tolerable
    // minor divergence. The per-command ACL ENFORCEMENT above already ran, so a user without
    // `+acl` cannot reach this; `default` (and any `+acl` user) can. A non-ACL command never
    // enters this block, so the hot path is byte-unchanged.
    if !conn.in_multi && cmd_upper == b"ACL" {
        handle_acl_command(ctx, conn, env, request, out);
        return false;
    }

    // -- PERSISTENCE INTERCEPTION (#58): SAVE / BGSAVE / LASTSAVE. When persistence is ENABLED (a
    // data_dir is configured -> `persist.is_some()`) and the command is NOT inside a MULTI (a SAVE
    // in MULTI is rare; it falls through to the persistence-disabled dispatch fallback inside EXEC,
    // a documented minor divergence), this router runs the REAL cross-shard save / reports the real
    // LASTSAVE -- the generic dispatch sees only the storage waist, not the concrete stores to dump.
    // With persistence OFF (`None`) this whole block is skipped and the commands fall through to the
    // dispatch persistence-disabled fallback, so the default posture is byte-unchanged.
    if let Some(persist) = persist {
        if !conn.in_multi && matches!(cmd_upper.as_slice(), b"SAVE" | b"BGSAVE" | b"LASTSAVE") {
            handle_persist_command(persist, ctx, conn, home, inbox, &cmd_upper, request, out).await;
            return false;
        }
        // DIRTY-WRITE COUNTER (#58 save policy): bump the node-level dirty counter for a write
        // command so the periodic save policy can decide whether enough changed since the last save.
        // This is a SINGLE RELAXED ATOMIC increment, gated on persistence being ENABLED (so the
        // default persistence-off path never touches it) AND on the command being a write
        // (`is_write`, the registry flag; a read / admin command never bumps it). It is in the SERVE
        // layer, NOT the store hot path, so the store primitives are byte-unchanged. It is
        // intentionally approximate (a write that later errors still bumped it), exactly like Redis's
        // `server.dirty` heuristic that drives its own `save` points.
        if ironcache_server::is_write(&cmd_upper) {
            persist.note_write();
        }
    }

    // -- IN-MULTI PUB/SUB REJECT (SERVER_PUSH.md #20, FIX C). The pub/sub commands are handled in
    // THIS serve layer (`try_handle_pubsub`), NOT in `dispatch_inner`, so EXEC -- which replays
    // the queued batch through `dispatch_inner` -- cannot run them. Rather than execute them
    // EAGERLY inside MULTI (silently wrong + out of transaction order, the bug the interception
    // order caused) or queue-then-fail-at-EXEC, REJECT them loudly at queue time and dirty the
    // transaction (so EXEC returns -EXECABORT and applies nothing): the same "correct, or
    // explicitly aborted, never silently wrong" contract as the cross-shard in-MULTI guards.
    //
    // DOCUMENTED DIVERGENCE from current Redis: Redis QUEUES the pub/sub commands inside MULTI
    // and runs them at EXEC (they do NOT carry CMD_NO_MULTI; verified against redis/redis
    // src/commands/*.json). Serve-layer EXEC replay of pub/sub is the tracked follow-up that
    // removes this divergence; until then we reject (never silently mis-execute). The reject runs
    // BEFORE `try_handle_pubsub` so the command is neither executed nor queued.
    if conn.in_multi && is_serve_pubsub_command(&cmd_upper) {
        state_rc.borrow_mut().counters.on_command();
        conn.dirty_exec = true;
        let name = String::from_utf8_lossy(&cmd_upper).into_owned();
        encode_into(
            out,
            &ironcache_server::Value::error(
                ironcache_protocol::ErrorReply::not_allowed_in_transactions(&name),
            ),
            conn.proto,
        );
        return false;
    }

    // -- SUBSCRIBE-MODE GATE (SERVER_PUSH.md #20, FIX B). MOVED to run BEFORE the pub/sub
    // interception: a RESP2 subscriber may run ONLY the (P)SUBSCRIBE / (P)UNSUBSCRIBE control set
    // + PING/QUIT/RESET; PUBLISH and PUBSUB are NOT allowed. The previous order intercepted
    // PUBLISH/PUBSUB before this gate, so a RESP2 subscriber wrongly executed them. The gate's
    // allowlist still passes the subscribe-family + PING/QUIT/RESET through to interception (so
    // SUBSCRIBE while subscribed, the subscribed PING array, etc. still work); only PUBLISH/PUBSUB
    // (and any other non-pub/sub command) get the subscribe-mode error. RESP3 has NO restriction.
    // The check + reply live in `subscriber_gate_blocks` (kept out of this router so it stays
    // small); it returns true (and has written the error) when the command is blocked. See that
    // helper for WHY the gate is ALSO in `dispatch` (a remote keyed hop bypasses the dispatch gate).
    if subscriber_gate_blocks(conn, state_rc, &cmd_upper, out) {
        return false;
    }

    // -- RESET INTERCEPTION (SERVER_PUSH.md #20, FIX A). RESET goes through the home dispatch path
    // (`dispatch_inner`'s RESET arm), which clears `conn.sub_channels` / `sub_patterns` but CANNOT
    // reach the per-shard subscription table (the push senders live in this serve layer). Without
    // this interception a post-RESET connection would still appear subscribed in the shard table:
    // a PUBLISH would still count + deliver to it (a GHOST), and PUBSUB CHANNELS would still list
    // it. So when RESET arrives on a subscriber, we FIRST deregister all its subscriptions from
    // the table (driven off the PRE-reset conn sub sets), THEN replace the per-connection push
    // channel (drop the old sender/receiver + shed flag, install a fresh trio) so a post-RESET
    // SUBSCRIBE re-registers cleanly with a live channel, and only THEN let dispatch run RESET
    // (which clears the conn sub sets + the rest of the reset). A RESET on a non-subscriber skips
    // straight to dispatch (the deregister is a no-op), so the non-subscriber path is unchanged.
    if cmd_upper == b"RESET" && conn.is_subscriber() {
        deregister_all_subscriptions(conn);
        // Swap in a fresh push channel + shed flag: the old `push_tx`/`push_rx`/`shed_flag` are
        // dropped, so any in-flight ghost sender the publisher still holds is closed, and a fresh
        // SUBSCRIBE after RESET registers the NEW sender. The serve loop owns these by &mut, so
        // the swap is visible to the idle wait on the next iteration.
        let (new_tx, new_rx) = tokio::sync::mpsc::channel::<crate::pubsub::ServerPush>(
            crate::pubsub::PUSH_CHANNEL_BOUND,
        );
        *push_tx = new_tx;
        *push_rx = new_rx;
        *shed_flag = std::sync::Arc::new(crate::pubsub::ShedSignal::default());
        // Fall through to dispatch so the RESET arm clears the conn sub sets + the rest of reset
        // and replies "+RESET".
    }

    // -- PUB/SUB SERVE-LAYER INTERCEPTION (SERVER_PUSH.md #20, PR 91a). SUBSCRIBE / UNSUBSCRIBE /
    // PUBLISH (and PING-while-subscribed under RESP2) are handled HERE because registration needs
    // the per-connection push sender + the per-shard subscription table that live in this serve
    // layer (the server crate has no tokio dep). By the time we reach here the in-MULTI reject and
    // the RESP2 subscribe-mode gate have already run (FIX C / FIX B), so a pub/sub command that
    // arrives here is NOT in MULTI and (if a RESP2 subscriber) is in the allowed control set. When
    // `try_handle_pubsub` handled the command it returns `Some(close)`; every other command
    // (`None`) falls through to the normal routing + dispatch. Split out so this router stays small.
    if let Some(close) = try_handle_pubsub(
        conn, home, inbox, push_tx, shed_flag, state_rc, &cmd_upper, request, out,
    )
    .await
    {
        return close;
    }

    // -- BLOCKING-COMMAND SERVE-LAYER INTERCEPTION (PROD-9). BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/
    // BZPOPMIN/BZPOPMAX/BZMPOP/WAIT are handled HERE (not in `dispatch_inner`) on the LIVE path
    // because PARKING needs the per-connection waker + the runtime timer seam + the connection's
    // stream (to observe a peer close while parked), which the serve loop owns. It fires ONLY when
    // NOT in a MULTI: inside a transaction a blocking command must NOT block (Redis: it QUEUES and
    // runs NON-BLOCKING at EXEC, returning nil at once if empty), so an in-MULTI blocking command
    // FALLS THROUGH to `route_in_multi` below -> the dispatch queue gate stages it (+QUEUED), and
    // EXEC replays it through its NON-BLOCKING dispatch arm. On the live path:
    //
    //   * a parse error is replied immediately (no park);
    //   * a non-blocking ATTEMPT that finds data replies it immediately (the fast path: NO park);
    //   * an attempt that finds every key empty sets `block_request` and returns -- the OWNING
    //     serve loop then runs the park loop (register a FIFO waiter, `select!` on wake/timeout/
    //     close, re-attempt on wake). WAIT sets a `block_request` too (it parks on the replica-ack
    //     quorum), with NO keys (it touches no keyspace).
    //
    // A non-blocking command never enters this block (a single `is_blocking_command` predicate),
    // so the hot path is byte-unchanged.
    if !conn.in_multi && ironcache_server::is_blocking_command(&cmd_upper) {
        let close = handle_blocking_live(
            ctx,
            conn,
            env,
            store_rc,
            state_rc,
            &cmd_upper,
            request,
            out,
            block_request,
        );
        // A blocking pop that found data on the FAST path recorded keyspace event(s) (the same
        // lpop/rpop/zpopmin emit as the non-blocking pop); drain + publish them now, AFTER the
        // reply is encoded (per-connection FIFO), through the EXISTING Pub/Sub fan-out -- exactly
        // like the normal home dispatch path. On the PARK path the store was not mutated (no pop),
        // so the drain is a no-op; the re-attempt in the serve loop's park loop publishes its own
        // events on a successful wake. The drain short-circuits on an empty buffer, so this is a
        // single thread-local `is_empty` check when notifications are off.
        publish_pending_keyspace_events(inbox, home.index).await;
        return close;
    }

    // -- TRANSACTION CORRECTNESS UNDER PARTITIONING (COORDINATOR.md #107, the critical fix).
    //
    // The coordinator routes each command to its key's OWNER shard. But a command issued
    // INSIDE a `MULTI` must be QUEUED (reply `+QUEUED`), not executed: routing it remotely
    // (the dispatch_via / multikey / whole-keyspace branches below) would EXECUTE it eagerly
    // and out of transaction order. The queue gate lives in `dispatch` (the server crate) on
    // the HOME path only, so the remote/fan-out branches bypass it entirely. We close that
    // hole here, BEFORE the routing decision.
    //
    // The KEY INVARIANT we establish: a transaction reaches real (home-only) EXEC ONLY when
    // ALL its watched keys AND all its queued commands' keys are HOME-OWNED, so home
    // execution is always correct. Otherwise we reject it LOUDLY (a transaction is correct,
    // or explicitly aborted -- never silently wrong). True cross-shard transactions (txid +
    // ordered apply) are Stage 3, out of scope here.
    //
    // With `shards == 1` every key is home-owned, so the guards below NEVER fire and the
    // `in_multi -> home path` branch is exactly the pre-coordinator behavior (home dispatch
    // was always the path): byte-identical, and every existing transaction test stays green.

    // (1) QUEUE GATE + (2) CROSS-SHARD-IN-MULTI / WHOLE-KEYSPACE GUARDS. Inside a transaction
    // a command must be QUEUED (or a control verb handled), NEVER routed/executed remotely, and
    // a transaction may reach real (home-only) EXEC ONLY when all its keys are home-owned. That
    // transaction-correctness logic lives in `route_in_multi` (kept out of this router so it
    // stays small); it returns the close flag when it handled the in-MULTI case.
    if conn.in_multi {
        return route_in_multi(
            ctx, conn, home, env, store_rc, wheel_rc, state_rc, &cmd_upper, route, request, out,
        );
    }

    // (3a) CLUSTER WATCH SLOT GUARD (CLUSTER_CONTRACT.md #70, slice 2). WATCH is classified
    // `AlwaysHome` (it is a connection-state verb that bypasses MULTI queueing), so the data
    // `cluster_redirect` below EXEMPTS it; but in Redis WATCH carries a key spec and goes
    // through `getNodeByQuery`, so a `WATCH <foreign-slot key>` must reply `-MOVED` (and two
    // keys spanning slots `-CROSSSLOT`), NOT snapshot locally and reply +OK (a bogus optimistic
    // lock + a parity hole). We therefore run the SAME shared `redirect_for_keys` predicate the
    // keyed-data path uses, over WATCH's keys (args[1..], read DIRECTLY because `command_keys`
    // does not extract an AlwaysHome command's keys), and only when a cluster map is configured
    // (`ctx.cluster` Some). On a redirect we short-circuit exactly like the data redirect below:
    // bump the command counter, encode the error, do NOT run WATCH, do NOT close. A WATCH whose
    // keys are all home-slot (or a malformed/arity-wrong WATCH that yields no key) returns None
    // and falls through to the cross-shard WATCH guard, then the home dispatch, unchanged. This
    // runs BEFORE the internal cross-shard WATCH guard so cluster MOVED/CROSSSLOT (the
    // client-visible, retryable redirect) takes precedence over the internal-shard error.
    if cmd_upper == b"WATCH" && request.args.len() >= 2 {
        if let Some(map) = ctx.cluster.as_deref() {
            // WATCH snapshots for a transaction (a CAS that gates a WRITE), so it must NEVER be
            // served by a replica's stale state: pass `replica_serves = false` so a WATCH of a
            // foreign slot always MOVEDs to the owner, even on a READONLY replica connection.
            // WATCH never participates in the HA-6 migration ASK/IMPORTING handshake (it is a CAS
            // gate, not a data read/write that the client retries with ASKING), so pass `None`:
            // the migration arm is skipped and WATCH redirects exactly as before HA-6.
            if let Some(reply) = redirect_for_keys(
                map,
                request.args[1..].iter().map(AsRef::as_ref),
                false,
                None,
                shard_owner_home(ctx, home),
            ) {
                state_rc.borrow_mut().counters.on_command();
                encode_into(out, &ironcache_server::Value::error(reply), conn.proto);
                return false;
            }
        }
    }

    // (3) CROSS-SHARD WATCH GUARD (only when NOT in_multi; WATCH inside MULTI already errors
    // via dispatch's watch_inside_multi path). A `WATCH` of a key owned by a remote shard
    // would snapshot the WRONG (home) store, making the dirty-CAS meaningless. `route::classify`
    // treats WATCH as AlwaysHome and `command_keys` does not extract its keys, so we read
    // WATCH's keys (args[1..]) DIRECTLY here. If any is not home-owned, reply the cross-shard
    // WATCH error and do NOT run WATCH (no snapshot, no conn.watch mutation); the connection is
    // left un-watched so a following MULTI/EXEC works. A WATCH of only home-owned keys (or a
    // malformed/arity-wrong WATCH) falls through to the home dispatch -> cmd_watch unchanged.
    if cmd_upper == b"WATCH"
        && request.args.len() >= 2
        && request.args[1..]
            .iter()
            .any(|k| route::owner_shard(k.as_ref(), home.total) != home.index)
    {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::watch_cross_shard()),
            conn.proto,
        );
        return false;
    }

    // CLUSTER SLOT OWNERSHIP (CLUSTER_CONTRACT.md #70, slice 2). BEFORE any internal shard
    // routing (the multikey / spanning / single-target fan-out below): in cluster-map mode a
    // KEYED data command whose key(s) are not served by THIS node is REDIRECTED (`-MOVED`) or
    // REJECTED (`-CROSSSLOT`). `ctx.cluster` is `Some` ONLY when cluster mode is enabled AND a
    // topology is configured, so a standalone (or topology-less) node skips this entirely and
    // is byte-identical to slice 1 (Redis parity: a non-cluster node never sends MOVED). Keyless
    // / admin / whole-keyspace commands are exempt (`cluster_redirect` returns None for them).
    // The in-MULTI path does NOT reach here (it returned to `route_in_multi` above); queued
    // commands are checked at QUEUE time there, reusing this SAME predicate.
    // HA-6 ASKING: the one-shot per-connection flag. `ASKING` itself just sets the flag and replies
    // +OK (it does NOT consume it -- the NEXT command does). Handled HERE (the router) so the flag
    // is in scope for the migration redirect below; `ASKING` is `AlwaysHome`, so it otherwise falls
    // through to the home dispatch, but intercepting it here keeps the one-shot lifetime tight.
    if cmd_upper == b"ASKING" {
        state_rc.borrow_mut().counters.on_command();
        if request.args.len() == 1 {
            conn.asking = true;
            encode_into(out, &ironcache_server::Value::ok(), conn.proto);
        } else {
            encode_into(
                out,
                &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                    "asking",
                )),
                conn.proto,
            );
        }
        return false;
    }

    if let Some(map) = ctx.cluster.as_deref() {
        let in_sync = replica_read_in_sync(ctx);
        // HA-6: use the one-shot ASKING captured + cleared at the TOP of this function (before any
        // early return), so a flag set by an earlier `ASKING` can never leak past a pubsub / in_multi
        // / WATCH early return into this decision. The migration redirect is consulted only in raft
        // cluster mode (a committed MIGRATING/IMPORTING tag); a non-migrating slot makes the resolver
        // irrelevant and the decision byte-identical to pre-HA-6.
        let now = UnixMillis(env.borrow().now_unix_millis());
        let db = conn.db;

        // HA-6 MULTI-SHARD EXACT PRESENCE (COORDINATOR.md #107): the migration source's ASK
        // decision classifies each key present/absent against the shard that OWNS it (the FNV
        // `owner_shard`). On a SINGLE-shard node every key is home, so the local `contains_live`
        // is already exact -- and `xshard_presence_keys` returns `None`, so this whole block is a
        // single cheap predicate and the resolver below is BYTE-IDENTICAL to pre-fix (the hot path
        // is untouched). On a MULTI-shard node a migrating-slot key may live on a SIBLING shard;
        // there the accept-shard `contains_live` could report a present key ABSENT and emit a
        // (safe but unnecessary) extra `-ASK`. So when the command's keys are on a slot this node
        // is MIGRATING (the only case `migration_decision` consults presence) AND some key is NOT
        // home-owned, we PRE-RESOLVE that key's presence on its owner shard via the coordinator
        // (`presence_via`, the cross-shard `contains_live`), making the decision EXACT. This is a
        // COLD path (a slot actually MIGRATING + a keyed command landing on this owner) and the
        // hop is the same deadlock-free single-key mechanism Stage 1 routing uses (see
        // `presence_via`); the borrow of `store_rc` for a home key is taken + dropped INSIDE the
        // closure, never across the awaits done here.
        let xshard_presence: Vec<(Vec<u8>, bool)> =
            match xshard_presence_keys(map, route, &cmd_upper, request, home) {
                None => Vec::new(),
                Some(remote_keys) => {
                    let mut resolved = Vec::with_capacity(remote_keys.len());
                    for (key, owner) in remote_keys {
                        // Each remote key is resolved on its OWNER shard (a cross-shard hop). No
                        // `RefCell` borrow is held across this await (the only borrows in this fn
                        // are the brief `env.borrow()` above, already dropped, and the per-call
                        // closure borrow below).
                        let present = coordinator::presence_via(inbox, owner, &key, db).await;
                        resolved.push((key, present));
                    }
                    resolved
                }
            };

        // The key-presence resolver. For a HOME-owned key (always true when shards == 1) it reads
        // THIS shard's store via the pure `contains_live` -- byte-identical to before. For a key
        // PRE-RESOLVED on a sibling shard above (multi-shard migration only) it returns the EXACT
        // owner-shard answer. A key that is neither (cannot occur: `xshard_presence_keys` returns
        // EVERY non-home key of the migrating slot) falls back to the local read, the safe default.
        let key_present = |k: &[u8]| {
            if let Some(&(_, present)) = xshard_presence.iter().find(|(key, _)| key.as_slice() == k)
            {
                present
            } else {
                store_rc.borrow().contains_live(db, k, now)
            }
        };
        let mig = MigrationCtx {
            asking,
            key_present: &key_present,
        };
        if let Some(reply) = cluster_redirect(
            map,
            route,
            &cmd_upper,
            request,
            conn.readonly,
            in_sync,
            Some(&mig),
            shard_owner_home(ctx, home),
        ) {
            state_rc.borrow_mut().counters.on_command();
            encode_into(out, &ironcache_server::Value::error(reply), conn.proto);
            // Short-circuit WITHOUT closing the connection (same as the WATCH guard above):
            // the client keeps the connection and retries at the redirect target.
            return false;
        }
    }

    // WRITE-SIDE REPLICATION GUARDRAIL (ADR-0026, Redis `min-replicas-to-write`). After the
    // redirect above returned `None` (so this node OWNS the keyed slot, or the command is
    // keyless/admin), an owned WRITE is REJECTED with `-NOREPLICAS Not enough good replicas to
    // write.` when too few replicas are currently in sync -- so an ACKNOWLEDGED write is known to
    // be on at least `min_replicas_to_write` replicas, bounding the failover loss window.
    //
    // BYTE-UNCHANGED at the default: the FIRST gate is `min_replicas_to_write > 0`. With the
    // guardrail at its default-disabled 0 this whole block short-circuits BEFORE touching the
    // count atomic, the map, or `is_write` -- the write hot path is byte-identical to before. The
    // check applies ONLY to WRITES (`is_write`), ONLY to slots this node OWNS (the redirect already
    // sent foreign / read-replica-served slots away), and ONLY in raft-mode (the count cell is
    // `Some` only there). Reads are never blocked.
    if ctx.boot.min_replicas_to_write > 0 {
        if let Some(reply) = write_guardrail(ctx, route, &cmd_upper, request) {
            state_rc.borrow_mut().counters.on_command();
            encode_into(out, &ironcache_server::Value::error(reply), conn.proto);
            // Short-circuit WITHOUT closing the connection: the client may retry once enough
            // replicas are back in sync (the same non-closing contract as the redirect above).
            return false;
        }
    }

    // RAFT-MODE CLUSTER MUTATOR -> PROPOSAL (HA-4c). A `CLUSTER ADDSLOTS / ADDSLOTSRANGE /
    // SETSLOT / MEET / FORGET / SET-CONFIG-EPOCH` is normally handled SYNCHRONOUSLY by
    // `cmd_cluster` (the slice-3 direct local mutation). In raft-governance mode the slot map is
    // owned by the committed log, so the mutator becomes a PROPOSAL: build a `ConfigCmd`, await
    // its commit through the leader, and reply `+OK` (committed) or `-CLUSTERDOWN` (this node is
    // not the leader). This branch fires ONLY when `cluster_mode == Raft` AND a handle is present
    // (`ctx.raft.is_some()`), so the DEFAULT static path never reaches it and is byte-unchanged.
    //
    // It is intercepted HERE (the async router), NOT in `cmd_cluster` (which is sync and cannot
    // await a commit): `CLUSTER` is `AlwaysHome`, so none of the keyed routing below applies to
    // it, and the await parks on the proposal's one-shot ack (the single control-plane task
    // fulfills it) WITHOUT blocking the shard executor. The introspection subcommands (SLOTS /
    // SHARDS / NODES / INFO / MYID / ...) are NOT mutators, so they fall through to the unchanged
    // home dispatch, which reads the committed `ctx.cluster` map.
    if cmd_upper == b"CLUSTER"
        && ctx.cluster_mode() == ironcache_config::ClusterMode::Raft
        && ctx.raft.is_some()
    {
        if let Some(close) = try_raft_cluster_mutator(ctx, conn, state_rc, request, out).await {
            return close;
        }
        // Not a mutator (an introspection subcommand or a malformed CLUSTER): fall through to the
        // unchanged home dispatch.
    }

    // A SHARD-SPANNING KeyedMulti command (its keys land on >1 shard, so `owner_shard_set`
    // is None) that is one of the SIX fan-out-supported commands routes to the multi-key
    // SCATTER-GATHER (COORDINATOR.md #107, Stage 2a). Co-located KeyedMulti (Some(shard))
    // routes via Stage 1 below; any OTHER spanning multi-key command stays on the home sync
    // fall-through (the documented Stage 2b/2c gap), unchanged. We compute this BEFORE the
    // single-target `target` so the two are mutually exclusive (a spanning command has no
    // single owner, so `target` would be None anyway).
    let multikey_fan_out =
        matches!(route, route::CommandClass::KeyedMulti) && is_fan_out_multikey(&cmd_upper) && {
            let spec = route::command_keys(&cmd_upper, request);
            // None from owner_shard_set means EITHER a malformed/short request (keep home,
            // the handler emits the proper error) OR a genuine spanning command. We only
            // fan out when the spec actually has MULTIPLE keys spanning shards; a malformed
            // command (KeySpec::None) must stay home. `command_keys` returns None/One for
            // the degenerate cases, so require Many AND a None owner set (truly spanning).
            matches!(spec, route::KeySpec::Many(_))
                && route::owner_shard_set(&spec, home.total).is_none()
        };

    // A SHARD-SPANNING gather-combine command -- set algebra (SINTER/SUNION/SDIFF/SINTERCARD
    // + the three *STORE), zset algebra (ZUNION/ZINTER/ZDIFF/ZINTERCARD + the three *STORE +
    // ZRANGESTORE), BITOP, or HyperLogLog (PFCOUNT/PFMERGE) -- routes to the GATHER + (shared)
    // COMBINE + STORE path (COORDINATOR.md #107, Stage 2b-1 + 2b-2 + 2b-3). The gate is the
    // SAME shape as `multikey_fan_out`: KeyedMulti, one of the supported tokens, and the keys
    // genuinely SPAN shards (`Many` AND a `None` owner set). Co-located invocations route via
    // Stage 1 below; a malformed/short request stays home (the handler emits the proper
    // error). The two predicates are mutually exclusive (their command sets are disjoint). The
    // remaining spanning multi-key commands (RENAME/COPY/MOVE/SMOVE/LMOVE/RPOPLPUSH moves)
    // stay on the home fall-through (deferred).
    let spanning_set_fan_out = matches!(route, route::CommandClass::KeyedMulti)
        && is_fan_out_spanning_combine(&cmd_upper)
        && {
            let spec = route::command_keys(&cmd_upper, request);
            matches!(spec, route::KeySpec::Many(_))
                && route::owner_shard_set(&spec, home.total).is_none()
        };

    // A SHARD-SPANNING element-MOVE command -- SMOVE (set member), LMOVE / RPOPLPUSH (list
    // element) -- whose two keys span shards routes to the ATOMIC cross-shard apply
    // (COORDINATOR.md #107, the PROD-9 cross-shard atomicity slice): the spanning_move module
    // gathers + validates the source (read-only), then COMMITS the dst write + the src
    // mutation in a deadlock-free deterministic order, ending the prior SILENT home-subset
    // partial-apply. Co-located invocations route via Stage 1 below (the single-shard
    // handler); a malformed/short request stays home (the handler emits the proper error). The
    // gate shape mirrors `multikey_fan_out` / `spanning_set_fan_out` (Many AND a None owner
    // set = truly spanning); the command sets are disjoint, so the branches are exclusive.
    let spanning_move_fan_out = matches!(route, route::CommandClass::KeyedMulti)
        && is_fan_out_spanning_move(&cmd_upper)
        && {
            let spec = route::command_keys(&cmd_upper, request);
            matches!(spec, route::KeySpec::Many(_))
                && route::owner_shard_set(&spec, home.total).is_none()
        };

    // A SHARD-SPANNING all-or-nothing MSETNX (COORDINATOR.md #107): EXISTS-scan every key on
    // its owner FIRST, then (iff none exist) fan a per-owner MSET out -- replacing the prior
    // home-subset existence check + home-subset write (which set ONLY the home keys and
    // MISREPORTED its 1/0). Co-located MSETNX routes via Stage 1; a malformed request stays
    // home. MSETNX is NOT in `is_fan_out_multikey` (the Stage 2a fan-out deliberately deferred
    // it), so this is its dedicated spanning gate.
    let spanning_msetnx = cmd_upper == b"MSETNX" && {
        let spec = route::command_keys(&cmd_upper, request);
        matches!(spec, route::KeySpec::Many(_))
            && route::owner_shard_set(&spec, home.total).is_none()
    };

    // A SHARD-SPANNING multi-key command this slice cannot apply atomically
    // (RENAME/RENAMENX/COPY/LMPOP/ZMPOP/SORT...STORE) is REJECTED LOUDLY (a clear error naming
    // the hash-tag remedy) rather than falling through to the home shard and SILENTLY
    // operating on only the home subset (COORDINATOR.md #107). The gate is the same
    // truly-spanning shape; co-located invocations (incl. a SORT without STORE -- one key)
    // route via Stage 1 / the home path, unchanged.
    let spanning_move_reject = matches!(route, route::CommandClass::KeyedMulti)
        && is_spanning_move_reject(&cmd_upper)
        && {
            let spec = route::command_keys(&cmd_upper, request);
            matches!(spec, route::KeySpec::Many(_))
                && route::owner_shard_set(&spec, home.total).is_none()
        };

    // CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT in cluster mode (#371): a slot's keys span EVERY
    // shard (the client CRC16 slot vs the FNV owner-shard are independent), so an honest count /
    // key list must aggregate cross-shard. This fires ONLY for a fully-valid slot-scan AND only
    // when cluster mode is on; a malformed one (or standalone) falls to the home `CLUSTER` path,
    // which returns the exact error (or `-ERR cluster support disabled`). The `args[1]` peek runs
    // only for the CLUSTER command, never on the GET/SET hot path.
    let cluster_slot_scan: Option<ironcache_server::SlotScan> = (cmd_upper == b"CLUSTER"
        && ctx.info.cluster_enabled)
        .then(|| ironcache_server::parse_slot_scan(request))
        .flatten();

    // The routing TARGET shard, if a KEYED command routes to exactly one NON-home shard
    // (else `None` -> the home path). The single-key case keeps the zero-alloc fast path
    // (one hash + compare); only the genuinely multi-key commands pay the `command_keys`
    // walk. WholeKeyspace is NOT a single-target hop (it fans out in its own branch).
    let target = match route {
        route::CommandClass::KeyedSingle => route::single_key(request).and_then(|key| {
            let owner = route::owner_shard(key, home.total);
            (owner != home.index).then_some(owner)
        }),
        route::CommandClass::KeyedMulti => {
            let spec = route::command_keys(&cmd_upper, request);
            route::owner_shard_set(&spec, home.total).filter(|&owner| owner != home.index)
        }
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => None,
    };

    let close = if matches!(route, route::CommandClass::WholeKeyspace) {
        // WHOLE-KEYSPACE SCATTER-GATHER: cover EVERY shard's partition. SCAN walks one shard
        // per call (composite cursor); the rest broadcast + merge on the home core. The home
        // shard's partial runs LOCALLY + synchronously (no self-channel hop). These were
        // never on the single-key hot path, so awaiting here is fine.
        state_rc.borrow_mut().counters.on_command();
        if cmd_upper == b"SCAN" {
            crate::whole_keyspace::scan_cross_shard(
                inbox, ctx, request, conn.db, home.index, out, conn.proto,
            )
            .await;
        } else {
            // RANDOMKEY draws its shard-pick from the home Env RNG seam ONCE (ADR-0003);
            // the other whole-keyspace merges (DBSIZE / KEYS / FLUSHDB / FLUSHALL) ignore
            // it. Gate the draw to RANDOMKEY (FIX 3): drawing unconditionally (for a bare
            // arity-1 DBSIZE / FLUSHALL / FLUSHDB) would PERTURB the per-shard SplitMix64
            // stream that RANDOMKEY / SPOP / *-random eviction read from, breaking ADR-0003
            // replay AND the shards == 1 byte-identical parity (the home path draws 0 for
            // these). Non-RANDOMKEY -> 0, no draw.
            let pick = if cmd_upper == b"RANDOMKEY" {
                crate::whole_keyspace::randomkey_pick(request)
            } else {
                0
            };
            crate::whole_keyspace::fan_out_and_merge(
                inbox, ctx, &cmd_upper, request, conn.db, home.index, pick, out, conn.proto,
            )
            .await;
        }
        false
    } else if multikey_fan_out {
        // SHARD-SPANNING multi-key SCATTER-GATHER (COORDINATOR.md #107, Stage 2a): one of
        // the six (MGET/MSET/DEL/EXISTS/UNLINK/TOUCH) whose keys span shards. The multikey
        // module groups the keys by owner, runs a per-shard sub-request (the home shard's
        // subset LOCALLY + sync, the rest via their drain loops), and reassembles the reply.
        // Bump commands_processed here (matching the home / remote / whole-keyspace paths);
        // the owning shards fold their own data counters.
        state_rc.borrow_mut().counters.on_command();
        crate::multikey::fan_out_multikey(
            inbox, ctx, &cmd_upper, request, conn.db, home, out, conn.proto,
        )
        .await;
        false
    } else if spanning_set_fan_out {
        // SHARD-SPANNING gather-combine (COORDINATOR.md #107, Stage 2b-1/2b-2/2b-3): set /
        // zset algebra, BITOP, or HyperLogLog (PFCOUNT/PFMERGE) whose keys span shards. The
        // spanning_combine module gathers each source from its owner (the home subset LOCALLY
        // + sync, the rest via their drain loops), combines with the PURE combiner shared with
        // the single-shard handler, and for the write forms writes the result to the dest
        // owner. Bump commands_processed here (matching the home / remote / whole-keyspace /
        // multikey paths); the owning shards fold their own data counters. The per-command
        // dispatch is split out so this router stays small.
        state_rc.borrow_mut().counters.on_command();
        dispatch_spanning_combine(ctx, conn, home, inbox, &cmd_upper, request, out).await;
        false
    } else if spanning_move_fan_out {
        // SHARD-SPANNING element MOVE (COORDINATOR.md #107, the PROD-9 cross-shard atomicity
        // slice): SMOVE / LMOVE / RPOPLPUSH whose two keys span shards. The spanning_move
        // module gathers + validates the source (read-only on its owner), then COMMITS the dst
        // write + the src mutation across the owner shards in a deadlock-free deterministic
        // order -- ENDING the prior SILENT home-subset partial-apply. Bump commands_processed
        // here (matching the home / remote / whole-keyspace / multikey / spanning-combine
        // paths); the owning shards fold their own data counters.
        state_rc.borrow_mut().counters.on_command();
        crate::spanning_move::fan_out_spanning_move(
            inbox, ctx, &cmd_upper, request, conn.db, home, out, conn.proto,
        )
        .await;
        false
    } else if spanning_msetnx {
        // SHARD-SPANNING all-or-nothing MSETNX (COORDINATOR.md #107): EXISTS-scan every key on
        // its owner FIRST, then (iff none exist) fan a per-owner MSET out. Replaces the prior
        // home-subset existence check + home-subset write (a SILENT partial that set only the
        // home keys and misreported 1/0). Bump commands_processed here; the owning shards fold
        // their own data counters.
        state_rc.borrow_mut().counters.on_command();
        crate::spanning_move::fan_out_spanning_msetnx(
            inbox, ctx, request, conn.db, home, out, conn.proto,
        )
        .await;
        false
    } else if spanning_move_reject {
        // SHARD-SPANNING RENAME/RENAMENX/COPY/LMPOP/ZMPOP/SORT...STORE: REJECT LOUDLY (a clear
        // error naming the hash-tag remedy) rather than fall through to the home shard and
        // SILENTLY operate on only the home subset (the cardinal safety bug). These need a
        // value-object cross-shard transfer / multi-key pop the engine does not expose yet;
        // the reject is the "correct, or explicitly aborted, never silently wrong" contract.
        reject_spanning_move(conn, state_rc, &cmd_upper, out);
        false
    } else if let Some(scan) = cluster_slot_scan {
        // CLUSTER COUNTKEYSINSLOT/GETKEYSINSLOT CROSS-SHARD FAN-OUT (#371): rewrite the validated
        // slot-scan into its internal whole-keyspace verb and broadcast + merge across EVERY shard,
        // exactly like DBSIZE (sum) / KEYS (concat). The home shard's partial runs locally + sync;
        // the rest via their drain loops. Attribute commands_processed like the other fan-out paths
        // (the per-shard slot-scan partials fold no data counters). `pick = 0`: only RANDOMKEY draws
        // from the Env RNG seam, so this never perturbs the per-shard SplitMix64 stream (ADR-0003).
        state_rc.borrow_mut().counters.on_command();
        let (verb, internal): (&'static [u8], Request) = match scan {
            ironcache_server::SlotScan::Count { slot } => (
                ironcache_server::ICCOUNTKEYSINSLOT,
                Request {
                    args: vec![
                        bytes::Bytes::from_static(ironcache_server::ICCOUNTKEYSINSLOT),
                        bytes::Bytes::from(slot.to_string()),
                    ],
                },
            ),
            ironcache_server::SlotScan::Get { slot, count } => (
                ironcache_server::ICGETKEYSINSLOT,
                Request {
                    args: vec![
                        bytes::Bytes::from_static(ironcache_server::ICGETKEYSINSLOT),
                        bytes::Bytes::from(slot.to_string()),
                        bytes::Bytes::from(count.to_string()),
                    ],
                },
            ),
        };
        crate::whole_keyspace::fan_out_and_merge(
            inbox, ctx, verb, &internal, conn.db, home.index, 0, out, conn.proto,
        )
        .await;
        false
    } else if let Some(target) = target {
        // REMOTE keyed hop: enqueue to the owning shard, encode its reply here. The owning shard
        // folded the data counters; here we only attribute commands_processed.
        // KEYSPACE NOTIFICATIONS (PROD-8): the MUTATION runs on the OWNER shard, so it records its
        // keyspace events into the OWNER shard's pending buffer; that shard's drain loop drains +
        // publishes them (see `run_remote`). The home path here records nothing for a remote write.
        state_rc.borrow_mut().counters.on_command();
        if defer_hops {
            // #8 OVERLAP: enqueue the hop and hand the receiver back WITHOUT awaiting, so the caller
            // can issue the next command's hop before this owner replies. `out` is left UNTOUCHED
            // (the reply is encoded later, in order, by the caller's drain), so `deferred_hop` being
            // Some is the caller's signal to defer this command's hooks + reply. A None inside means
            // the owner was already gone; the caller's `finish_hop` encodes shard-unavailable in order.
            *deferred_hop = coordinator::HopOutcome::Deferred(
                coordinator::dispatch_via_send(inbox, target, request, conn.db).await,
            );
            // No home post-processing for a deferred remote hop: the wake/keyspace-publish below run
            // on the OWNER shard (via run_remote), and the home probes are no-ops for a remote key.
            // Return early so we do NOT run the shared post-dispatch (wake/publish) against `out`.
            return false;
        }
        coordinator::dispatch_via(inbox, target, request, conn.db, out, conn.proto).await;
        false
    } else {
        // HOME path: the SYNC fast path (zero await/channel). Covers the home-owned keyed
        // commands, AlwaysHome, and the key-SPANNING multi-key commands (Stage 2 gap).
        // Pass the ALREADY-uppercased command (FIX 5): we computed `cmd_upper` above for
        // routing, so the home dispatch reuses it instead of re-uppercasing + re-allocating.
        handle_request(
            ctx, conn, env, store_rc, wheel_rc, state_rc, request, &cmd_upper, out,
        )
    };

    // BLOCKING WAKE (PROD-9): a HOME-shard WRITE that may have ADDED an element to a list/zset
    // (a push / move-dest / zadd / store-into) WAKES the longest-waiting parked waiter on that
    // destination key, so a BLPOP/BZPOPMIN/... blocked on the key re-attempts its pop and gets the
    // pushed element (Redis "serve the longest-waiting blocked client first"). It runs on the
    // HOME (key-owner) shard, the same shard a co-located blocked client parked on, so the wake +
    // the park share the one per-shard registry with no cross-shard coordination -- the common
    // co-located/single-key case is fully covered. A REMOTE write (a cross-shard push to a sibling
    // shard) wakes a waiter parked on THAT shard via its own drain loop (`run_remote`); a blocking
    // command whose keys SPAN shards is documented as not awaited cross-shard this pass. The wake
    // is a single registry probe gated on the command being an element-adding write
    // (`wake_keys_for_write` returns empty for every read / non-adding command), so the hot path is
    // a single match + an empty-Vec check. An over-broad wake is SAFE: the woken waiter re-checks
    // and re-parks if the key is still empty.
    wake_blocking_waiters_home(conn.db, &cmd_upper, request);

    // KEYSPACE NOTIFICATIONS (PROD-8): any HOME-shard mutation in the branches above (the home
    // keyed path, the home SUBSET of a multikey / spanning fan-out, the active TTL drain) recorded
    // its keyspace event(s) into THIS shard's pending buffer DURING dispatch. Drain + PUBLISH them
    // now, AFTER the reply is encoded (per-connection FIFO, SERVER_PUSH.md), through the existing
    // Pub/Sub fan-out. The drain short-circuits on an EMPTY buffer (the common case: a read, or
    // notifications disabled), so on the default deployment this is a single thread-local
    // `is_empty` check and the path is byte-identical. Events recorded on a REMOTE owner shard
    // (a cross-shard write) are drained + published by THAT shard's drain loop (`run_remote`).
    publish_pending_keyspace_events(inbox, home.index).await;
    close
}

/// WAKE any blocking waiter parked on a key this HOME-shard WRITE may have made ready (PROD-9).
/// `wake_keys_for_write` returns the destination key(s) of an element-adding command (push / move
/// dest / zadd / store-into), or an EMPTY vec for every other command -- so on the hot path (reads,
/// non-adding writes) this is one match + an `is_empty` check and the registry is never touched.
/// For each ready key it wakes the FRONT (longest-waiting) waiter (Redis fairness); the woken
/// connection re-attempts its pop. The registry handle is taken + dropped here (cold path).
fn wake_blocking_waiters_home(db: u32, cmd_upper: &[u8], request: &Request) {
    let keys = ironcache_server::wake_keys_for_write(cmd_upper, request);
    if keys.is_empty() {
        return;
    }
    let registry = shard_blocking();
    let mut reg = registry.borrow_mut();
    for key in keys {
        reg.wake_one(db, &key);
    }
}

/// WAKE blocking waiters parked on THIS shard for a CROSS-SHARD write that ran here (PROD-9), called
/// from the coordinator drain loop's `run_remote` path. It uppercases the command itself (the
/// coordinator carries the raw request) and delegates to the same wake logic as the home path, so a
/// push that lands on this shard from a writer homed elsewhere still wakes a co-located blocked
/// client. `pub(crate)` so `crate::coordinator` reaches it on the owner shard thread.
pub(crate) fn wake_blocking_waiters_for_shard(db: u32, request: &Request) {
    let cmd_upper = ascii_upper(request.command());
    wake_blocking_waiters_home(db, &cmd_upper, request);
}

/// DRAIN this shard's pending keyspace events (PROD-8) and PUBLISH each through the EXISTING
/// Pub/Sub fan-out ([`coordinator::fan_out_publish`]), so subscribers of `__keyspace@db__:<key>` /
/// `__keyevent@db__:<event>` (and PSUBSCRIBE patterns + cross-shard subscribers) receive them
/// exactly like a client PUBLISH. Called AFTER the command's reply is encoded (per-connection FIFO,
/// SERVER_PUSH.md "a push arrives after that command's reply").
///
/// FAST PATH: when no event was recorded (a read, or `notify-keyspace-events` disabled -- the
/// common case) the drain returns an empty Vec and this returns immediately, so it costs a single
/// thread-local `is_empty` check and no fan-out. Only when an event was actually recorded does it
/// build the channel name(s) + fan out. Each recorded event publishes the `K` keyspace message
/// (channel `__keyspace@db__:<key>`, payload = the event name) and/or the `E` keyevent message
/// (channel `__keyevent@db__:<event>`, payload = the key), per the channel selectors resolved at
/// record time. The receiver COUNT each PUBLISH returns is ignored (a notification's value is the
/// delivery, not a reply).
async fn publish_pending_keyspace_events(inbox: &coordinator::Inbox, home: usize) {
    let events = ironcache_config::notify::drain();
    if events.is_empty() {
        return;
    }
    for ev in events {
        if ev.keyspace {
            let channel = ev.keyspace_channel();
            coordinator::fan_out_publish(inbox, &channel, ev.event.as_bytes(), ev.db, home).await;
        }
        if ev.keyevent {
            let channel = ev.keyevent_channel();
            coordinator::fan_out_publish(inbox, &channel, &ev.key, ev.db, home).await;
        }
    }
}

/// Handle SAVE / BGSAVE / LASTSAVE when persistence is ENABLED (#58). Bumps `commands_processed`
/// (matching every other route), then:
///
/// - `SAVE`: BLOCKS until every shard has dumped its partition AND the manifest is committed (Redis
///   parity), then replies `+OK` (or an `-ERR` on a shard / manifest failure). The fan-out is the
///   forkless, borrow-releasing per-shard dump, so it never double-memories the keyspace.
/// - `BGSAVE`: SPAWNS the SAME save off the request path on the home shard's executor and replies
///   `+Background saving started` IMMEDIATELY, so the ISSUING connection is not blocked. NOTE (M4):
///   each dumping shard STILL holds its store borrow across its full dump + fsync (it does not yield
///   between chunks), so BGSAVE BLOCKS EACH SHARD for ITS OWN dump duration; the win over SAVE is
///   only that the issuing client is freed immediately.
/// - `LASTSAVE`: replies `:<unix_secs>` of the last committed save (`:0` until the first save).
///
/// Concurrent saves are serialized by [`crate::persist::PersistState::try_begin_save`]: a SAVE /
/// BGSAVE / periodic tick that finds a save already in progress is a no-op success (BGSAVE) or
/// proceeds once the latch is free. The save TIMESTAMP is read from the home shard's Env Clock seam
/// (the determinism boundary, ADR-0003).
#[allow(clippy::too_many_arguments)]
async fn handle_persist_command(
    persist: &Arc<crate::persist::PersistState>,
    ctx: &ServerContext,
    conn: &ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    cmd_upper: &[u8],
    request: &Request,
    out: &mut Vec<u8>,
) {
    use ironcache_runtime::Runtime;
    use ironcache_server::Value;
    shard_state().borrow_mut().counters.on_command();

    // -- AUTH (H2). The old inline NOAUTH gate that lived here was REMOVED when the gate was hoisted
    // to the single router chokepoint at the top of `route_and_dispatch`: an unauthenticated client
    // (requirepass set) is now short-circuited with `-NOAUTH` THERE, before SAVE/BGSAVE/LASTSAVE is
    // ever intercepted, so this handler is unreachable unauth and the inline gate was dead code. The
    // chokepoint covers this path AND every other (cross-shard, fan-out, CLUSTER mutator, SHUTDOWN),
    // which the point-fix here never could. See the hoisted gate for the full rationale.

    match cmd_upper {
        b"LASTSAVE" => {
            if request.args.len() != 1 {
                encode_into(
                    out,
                    &Value::error(ironcache_protocol::ErrorReply::wrong_arity("lastsave")),
                    conn.proto,
                );
                return;
            }
            #[allow(clippy::cast_possible_wrap)]
            let secs = persist.last_save() as i64;
            encode_into(out, &Value::Integer(secs), conn.proto);
        }
        b"SAVE" => {
            if request.args.len() != 1 {
                encode_into(
                    out,
                    &Value::error(ironcache_protocol::ErrorReply::wrong_arity("save")),
                    conn.proto,
                );
                return;
            }
            // The save timestamp from the home shard's Env clock (ADR-0003), in unix seconds.
            let now_secs = shard_env().borrow().now_unix_millis() / 1_000;
            // Serialize against a concurrent save. If one is already running, wait for the latch by
            // proceeding once free is overkill here; a SAVE that races a BGSAVE simply runs after the
            // latch frees on the next attempt. Acquire-or-bail: if busy, report the in-progress save
            // as a success (its data is being written), matching the "save is happening" intent.
            // The RAII guard releases the latch on completion AND on a panic unwinding the save (H3).
            let Some(_guard) = persist.try_begin_save() else {
                encode_into(out, &Value::ok(), conn.proto);
                return;
            };
            let result =
                crate::persist::do_save_all(persist, inbox, ctx, home, conn.db, now_secs).await;
            match result {
                Ok(()) => encode_into(out, &Value::ok(), conn.proto),
                Err(msg) => encode_into(
                    out,
                    &Value::error(ironcache_protocol::ErrorReply::err(msg)),
                    conn.proto,
                ),
            }
        }
        // BGSAVE [SCHEDULE]: kick the save off the request path and reply immediately.
        _ => {
            // The save timestamp captured NOW (on the request path) so the background save records
            // a faithful start time; the dump runs after, but LASTSAVE reporting the request time is
            // Redis-faithful enough (Redis stamps lastsave at fork time).
            let now_secs = shard_env().borrow().now_unix_millis() / 1_000;
            if let Some(guard) = persist.try_begin_save() {
                // Spawn the save on THIS (home) shard's executor; it owns the borrow-free fan-out.
                // The RAII guard is MOVED into the task so the latch releases when the task finishes
                // OR when it is CANCELLED at shutdown (the bare release_save() before could be
                // skipped on cancel, wedging the latch forever -- H3).
                let persist = Arc::clone(persist);
                let inbox = inbox.clone();
                let ctx = ctx.clone();
                let db = conn.db;
                let rt = ironcache_runtime::TokioRuntime::new();
                rt.spawn_on_shard(async move {
                    let _guard = guard; // dropped on task completion or cancellation -> releases.
                    let _ = crate::persist::do_save_all(&persist, &inbox, &ctx, home, db, now_secs)
                        .await;
                });
            }
            // Whether we won the latch or a save was already running, the Redis-faithful reply is
            // the same acknowledgement (a save is in progress).
            encode_into(
                out,
                &Value::SimpleString("Background saving started".to_owned()),
                conn.proto,
            );
        }
    }
}

/// Handle the `ACL` admin command family (#106) in the serve layer. Resolves the connection's
/// WHOAMI (the cached ACL user's name, or `default` for the implicit all-permissive default),
/// runs [`ironcache_server::dispatch_acl`] against the shared registry with the determinism-seam
/// RNG (for GENPASS), then performs any aclfile SAVE/LOAD I/O the handler asks for (the server
/// crate cannot touch `std::fs` on the data path, so the file I/O lives here, next to boot LOAD).
///
/// SAVE writes [`AclState::serialize_aclfile`] to the configured `aclfile`; LOAD reads it and
/// calls [`AclState::load_users`]. With NO `aclfile` configured both reply the Redis-faithful
/// `-ERR This Redis instance is not configured to use an ACL file...`. Passwords are persisted
/// only as `#<sha256-hex>` digests; an I/O or parse error is surfaced (never a plaintext secret).
fn handle_acl_command(
    ctx: &ServerContext,
    conn: &ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    use ironcache_server::{AclSideEffect, Value};
    shard_state().borrow_mut().counters.on_command();

    // WHOAMI: the cached ACL identity's name, or `default` (the implicit all-permissive default
    // / legacy-requirepass posture caches `None`). Resolved here, not on the data path.
    let whoami: &str = conn
        .acl_user
        .as_deref()
        .map_or(ironcache_server::DEFAULT_USER, |u| u.name.as_str());

    // Run the pure ACL handler with the determinism-seam RNG (GENPASS draws from it, ADR-0003).
    let (reply, effect) = {
        let mut env_ref = env.borrow_mut();
        ironcache_server::dispatch_acl(&ctx.acl, whoami, env_ref.rng(), request)
    };

    let reply = match effect {
        AclSideEffect::None => reply,
        AclSideEffect::Save(text) => match ctx.boot.aclfile.as_ref() {
            None => Value::error(ironcache_protocol::ErrorReply::err(
                "This Redis instance is not configured to use an ACL file. \
                 You may want to specify users via the ACL SETUSER command and then issue a \
                 CONFIG REWRITE (assuming you have a Redis configuration file set) in order to \
                 store users in the Redis configuration.",
            )),
            Some(path) => match std::fs::write(path, text.as_bytes()) {
                Ok(()) => reply,
                Err(e) => Value::error(ironcache_protocol::ErrorReply::err(format!(
                    "ACL SAVE failed writing the aclfile: {e}"
                ))),
            },
        },
        AclSideEffect::Load => match ctx.boot.aclfile.as_ref() {
            None => Value::error(ironcache_protocol::ErrorReply::err(
                "This Redis instance is not configured to use an ACL file. \
                 You may want to specify users via the ACL SETUSER command and then issue a \
                 CONFIG REWRITE (assuming you have a Redis configuration file set) in order to \
                 store users in the Redis configuration.",
            )),
            Some(path) => match std::fs::read_to_string(path) {
                Ok(text) => match ctx.acl.load_users(&text) {
                    Ok(_) => reply,
                    // The error never includes a plaintext password (the file holds only
                    // #digests / the redacted rule), so it is safe to surface verbatim.
                    Err((lineno, e)) => Value::error(ironcache_protocol::ErrorReply::err(format!(
                        "ACL LOAD failed at aclfile line {lineno}: {}",
                        e.reason
                    ))),
                },
                Err(e) => Value::error(ironcache_protocol::ErrorReply::err(format!(
                    "ACL LOAD failed reading the aclfile: {e}"
                ))),
            },
        },
    };
    encode_into(out, &reply, conn.proto);
}

/// Handle the `SHUTDOWN [NOSAVE|SAVE]` graceful-shutdown command (#139, SHUTDOWN.md). This is the
/// LIVE path for every non-MULTI SHUTDOWN (the serve router intercepts it before generic dispatch,
/// which cannot exit the process). The sequence (Redis-faithful):
///
/// 1. AUTH is enforced UPSTREAM by the hoisted NOAUTH chokepoint at the top of `route_and_dispatch`
///    (an UNAUTHENTICATED client with `requirepass` set is short-circuited with `-NOAUTH` before
///    SHUTDOWN is ever intercepted), so a public port still cannot be killed by an anonymous
///    SHUTDOWN -- the gate moved upstream + now covers every command, so the old inline gate here
///    was removed as dead code.
/// 2. PARSE the modifier ([`ironcache_server::parse_shutdown`], shared with the dispatch fallback so
///    the grammar cannot diverge): a bad/extra modifier replies `-ERR syntax error` and does NOT
///    exit.
/// 3. RESOLVE the save decision [redis-shutdown-save-nosave-default]:
///      * `SHUTDOWN SAVE`   -> save-on-exit ALWAYS. If persistence is NOT configured (no `data_dir`)
///        there is nowhere to save, so it replies `-ERR ... no data_dir configured` and does NOT
///        exit (Redis errors when it cannot honor a forced SAVE -- we surface the same fail rather
///        than exit-0 over unwritten data).
///      * `SHUTDOWN NOSAVE` -> exit 0 IMMEDIATELY without saving (even with a save policy).
///      * bare `SHUTDOWN`   -> save IFF a save policy is configured (persistence on +
///        `has_save_policy`), else exit without saving.
/// 4. If saving was resolved, perform the SYNCHRONOUS cross-shard save reusing the SAME atomic
///    persistence path SAVE uses ([`crate::persist::do_save_all`] -- forkless per-shard dump +
///    manifest committed LAST via a tmp->rename, so there is never a half-written file). A save
///    FAILURE replies `-ERR ...` and does NOT exit (fail-closed: an orchestrator must not record a
///    clean stop that lost data).
/// 5. On a resolved clean stop the process exits with code 0 (the orchestrator contract): SHUTDOWN
///    does NOT reply on success (Redis: the process is gone). The committed manifest is durable
///    BEFORE the exit, and the atomic rename means killed background tasks leave no torn file.
///
/// On any refused / failed save this returns normally (a reply is in `out`); on a clean stop it
/// NEVER returns (`std::process::exit`).
#[allow(clippy::too_many_arguments)]
async fn handle_shutdown_command(
    persist: Option<&Arc<crate::persist::PersistState>>,
    ctx: &ServerContext,
    conn: &ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    request: &Request,
    out: &mut Vec<u8>,
) {
    use ironcache_server::{ShutdownMode, Value};
    shard_state().borrow_mut().counters.on_command();

    // 1. AUTH. The old inline NOAUTH gate here was REMOVED when the gate was hoisted to the single
    // router chokepoint at the top of `route_and_dispatch`: an unauthenticated client (requirepass
    // set) is short-circuited with `-NOAUTH` THERE, before SHUTDOWN is ever intercepted, so a public
    // port still cannot be killed by an anonymous SHUTDOWN -- the protection moved upstream and now
    // covers every path uniformly, not just this one. See the hoisted gate for the full rationale.

    // 2. PARSE the modifier (shared grammar with the dispatch fallback). A bad modifier is a syntax
    // error and does NOT shut down.
    let mode = match ironcache_server::parse_shutdown(request) {
        Ok(mode) => mode,
        Err(e) => {
            encode_into(out, &Value::error(e), conn.proto);
            return;
        }
    };

    // 3. RESOLVE whether this stop saves. SAVE forces it (and errors if it cannot); NOSAVE never
    // saves; the bare form saves iff a save policy is configured.
    let want_save = match mode {
        ShutdownMode::NoSave => false,
        ShutdownMode::Save => {
            if persist.is_none() {
                // A forced SAVE with no data_dir cannot be honored: error, do NOT exit (Redis errors
                // when it cannot save rather than silently exiting over unwritten data).
                encode_into(
                    out,
                    &Value::error(ironcache_protocol::ErrorReply::err(
                        "Errors trying to SHUTDOWN. Check logs. (no data_dir configured for SAVE)",
                    )),
                    conn.proto,
                );
                return;
            }
            true
        }
        // Bare SHUTDOWN: save iff persistence is on AND a save policy (a periodic cadence) exists.
        // The policy is the LIVE runtime one (`CONFIG SET save` may have changed it since boot).
        ShutdownMode::Default => persist.is_some() && ctx.runtime.has_save_policy(),
    };

    // 4. If saving was resolved, perform the SYNCHRONOUS atomic save reusing the SAVE path. A save
    // FAILURE is fail-closed: reply the error and do NOT exit, so the connection keeps serving and
    // the orchestrator does not see a clean stop over unwritten data.
    if want_save {
        // `want_save` is only ever true with persistence configured (the Save-with-no-data_dir case
        // returned above, and Default gates on `persist.is_some()`), so this expect documents that
        // invariant rather than guarding a reachable None.
        let persist = persist.expect("want_save implies persistence is configured");
        // The save timestamp from the home shard's Env clock (ADR-0003), in unix seconds.
        let now_secs = shard_env().borrow().now_unix_millis() / 1_000;
        // H1 (data loss): the OLD code did `try_begin_save() else { /* covered */ }` and FELL THROUGH
        // to exit(0) when the latch was busy. But a concurrent BGSAVE / periodic save may be mid-
        // `do_save_all` with some `.icss` files written and the manifest (the atomic COMMIT point)
        // NOT yet run, so exiting over it KILLS that save before it commits -- the committed manifest
        // still points at the PRIOR snapshot and every write since is LOST despite this explicit
        // SAVE-on-exit. The fix: BOUNDED-WAIT for the busy latch to free (the in-flight save commits
        // + drops its guard; on a single-threaded executor the timer await yields to it), THEN run a
        // FRESH save (the operator demanded a CURRENT save), THEN exit. No borrow is held across the
        // wait (it only touches the `saving` atomic + the timer seam), so it cannot deadlock.
        let Some(_guard) =
            crate::persist::wait_to_begin_save(persist, crate::persist::SHUTDOWN_SAVE_WAIT).await
        else {
            // The wait TIMED OUT: a genuinely wedged save never freed the latch (the LOW case). Do
            // NOT hang forever -- proceed to a BEST-EFFORT exit. The in-flight save MAY still commit
            // its prior-or-partial state; we cannot do better without unbounded waiting.
            tracing::warn!(
                ?mode,
                "ironcache: SHUTDOWN: a prior save did not finish within SHUTDOWN_SAVE_WAIT; \
                 exiting best-effort (the in-flight save may still commit)"
            );
            std::process::exit(0);
        };
        // We hold the freed latch: run a FRESH save, BOUNDED so a wedged sibling drain loop (alive
        // but stuck) cannot hang the exit (L1). A failure (or fan-out timeout) is fail-closed: reply
        // the error and do NOT exit, so the orchestrator does not record a clean stop over unwritten
        // data and the connection keeps serving.
        match crate::persist::do_save_all_bounded(
            persist,
            inbox,
            ctx,
            home,
            conn.db,
            now_secs,
            crate::persist::SHUTDOWN_SAVE_WAIT,
        )
        .await
        {
            Ok(()) => {} // committed; fall through to the clean exit.
            Err(msg) => {
                encode_into(
                    out,
                    &Value::error(ironcache_protocol::ErrorReply::err(format!(
                        "Errors trying to SHUTDOWN. Check logs. ({msg})"
                    ))),
                    conn.proto,
                );
                return;
            }
        }
    }

    // 5. CLEAN STOP. The resolved save (if any) is committed + durable; exit 0 (the orchestrator
    // contract). SHUTDOWN does NOT reply on success (Redis: the process is gone). `process::exit`
    // is faithful to Redis's own SHUTDOWN handler (it exits from the command path after the save);
    // the committed manifest's atomic rename means the killed background tasks leave no torn file.
    tracing::info!(?mode, "ironcache: SHUTDOWN -> exit 0");
    std::process::exit(0);
}

/// Handle a raft-mode `CLUSTER` MUTATOR by proposing the matching [`ConfigCmd`](ironcache_raft::ConfigCmd)
/// through the control plane (HA-4c). Returns `Some(close)` (always `Some(false)`) when the
/// subcommand WAS a mutator (a reply has been written to `out`), or `None` for a non-mutator
/// subcommand (the caller falls through to the unchanged home dispatch, which reads the committed
/// `ctx.cluster` map for the introspection projections).
///
/// The caller has already established `cluster_mode == Raft` and `ctx.raft.is_some()`. The
/// mutator -> ConfigCmd mapping (CONTROL_PLANE.md / the HA-3e `ConfigCmd` taxonomy):
///   * `ADDSLOTS` / `ADDSLOTSRANGE`  -> `AssignSlots { node: self_id, slots }`
///   * `SETSLOT <slot> NODE <id>`    -> `SetSlotOwner { slot, node: id }`
///   * `MEET <ip> <port> [bus]`      -> `AddNode { id, host, port }`
///   * `FORGET <id>`                 -> `RemoveNode { id }`
///   * `SET-CONFIG-EPOCH <epoch>`    -> `SetConfigEpoch(epoch)`
///   * `DELSLOTS` / `DELSLOTSRANGE`  -> `UnassignSlots { slots }` (the parsed / range-expanded list)
///   * `FLUSHSLOTS`                  -> `UnassignSlots { slots }` (every slot THIS node owns in the
///     committed map; Redis FLUSHSLOTS clears the node's own slots)
///
/// On commit -> `+OK`; when this node is NOT the leader -> `-CLUSTERDOWN ...` (the client retries
/// against the leader). The slot/argument validation mirrors the Redis error shapes the static
/// `cmd_cluster` mutators use for the common cases. `commands_processed` is bumped exactly once
/// (matching every other route), regardless of outcome.
async fn try_raft_cluster_mutator(
    ctx: &ServerContext,
    conn: &ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out: &mut Vec<u8>,
) -> Option<bool> {
    use ironcache_protocol::ErrorReply;
    use ironcache_server::Value;

    // A bare `CLUSTER` (no subcommand) is not a mutator: let the home dispatch emit the arity
    // error (byte-identical to the static path).
    if request.args.len() < 2 {
        return None;
    }
    let sub = ascii_upper(&request.args[1]);

    // Build the ConfigCmd SEQUENCE for a recognized mutator, or return the appropriate immediate
    // error / None (non-mutator). `Err(reply)` is a validation error to send WITHOUT proposing;
    // `Ok(cmds)` is the ordered batch to propose+commit; `None` falls through to home dispatch.
    //
    // ADDSLOTS / ADDSLOTSRANGE prepend a self-`AddNode` (build_self_assign): they assign slots to
    // THIS node, but a FOLLOWER's committed map does not yet know the leader's id (each node boots
    // `empty_self` knowing only itself; MEET is leader -> peer). Committing `AddNode{self}` FIRST
    // teaches every node the leader's id+endpoint, so the following `AssignSlots{self}` applies
    // (and MOVED resolves) on every node. AddNode is idempotent on the leader's own table.
    let built: Option<Result<Vec<ironcache_raft::ConfigCmd>, ErrorReply>> = match sub.as_slice() {
        b"ADDSLOTS" => Some(build_self_assign(ctx, request, parse_addslots_slots)),
        b"ADDSLOTSRANGE" => Some(build_self_assign(ctx, request, parse_addslotsrange_slots)),
        b"SETSLOT" => Some(build_setslot(ctx, request).map(|c| vec![c])),
        // MEET LEARNS the peer's REAL announce id over the cluster bus (item-7 id-reconciliation),
        // which is real I/O (a bounded `CLUSTER MYID` fetch through the Runtime seam), so it is the
        // one builder that is `async`. On a reachable peer the committed `AddNode { id: real_id }`
        // COINCIDES with the peer's self-added announce entry (meet is idempotent on a duplicate
        // id), so no synth/announce duplicate inflates `cluster_known_nodes`; on an unreachable peer
        // it falls back to the synth id so the cluster still forms. See `build_meet`.
        b"MEET" => Some(build_meet(request).await.map(|c| vec![c])),
        b"FORGET" => Some(build_forget(request).map(|c| vec![c])),
        b"SET-CONFIG-EPOCH" => Some(build_set_config_epoch(request).map(|c| vec![c])),
        // HA-7d: `CLUSTER REPLICATE <node-id> <slot> [slot ...]` assigns `<node-id>` as a REPLICA
        // of the listed slots (drives `AssignReplica`). The named node must already be known (a
        // prior MEET / AddNode); the committed log order guarantees that, and the replica node
        // then attaches to each slot OWNER's primary (full-sync + tail) and serves READONLY reads.
        b"REPLICATE" => Some(build_replicate(request).map(|c| vec![c])),
        // HA-8 / #371: `CLUSTER FAILOVER` promotes THIS in-sync replica to owner of the slots it
        // replicates via a committed `PromoteReplica` (the operator entry point to the same path the
        // automatic failover uses). The in-sync gate (the data-safety crux) lives in `build_failover`.
        b"FAILOVER" => Some(build_failover(ctx, request)),
        // DELSLOTS / DELSLOTSRANGE UN-assign the parsed / range-expanded slots (the inverse of
        // ADDSLOTS / ADDSLOTSRANGE; the SAME slot-parse helpers). FLUSHSLOTS UN-assigns every slot
        // THIS node owns in the committed map. Each commits an `UnassignSlots` ConfigCmd, so the
        // slots become owned by nobody on every node (cluster_slots_assigned drops by that many).
        b"DELSLOTS" => Some(build_unassign(request, parse_addslots_slots)),
        b"DELSLOTSRANGE" => Some(build_unassign(request, parse_addslotsrange_slots)),
        b"FLUSHSLOTS" => Some(build_flushslots(ctx, request)),
        // #371: `CLUSTER REBALANCE APPLY` ARMS the planned slot migrations (a committed
        // MIGRATING + IMPORTING per move, driving HA-6's auto-copy); the DRYRUN / default form is a
        // read-only plan handled by the home dispatch (falls through the `_ => None` arm below).
        b"REBALANCE" if request.args.len() >= 3 && ascii_upper(&request.args[2]) == b"APPLY" => {
            Some(build_rebalance_apply(ctx))
        }
        // Any other subcommand (the introspection set, BUMPEPOCH, HELP, unknown, ...) is NOT a
        // mutator: fall through to the unchanged home dispatch.
        _ => None,
    };

    let cmds = match built? {
        Ok(cmds) => cmds,
        Err(reply) => {
            // A validation error (bad slot / arity / node id): reply now, do not propose.
            state_rc.borrow_mut().counters.on_command();
            encode_into(out, &Value::error(reply), conn.proto);
            return Some(false);
        }
    };

    // HA-prod-membership: a raft-mode MEET / FORGET ALSO drives the Raft VOTER / LEARNER set, not
    // just the node TABLE. Capture the membership intent from the built ConfigCmd batch BEFORE it is
    // consumed by the propose loop: MEET's `AddNode { id, host, port }` -> stage the node in as a
    // non-voting LEARNER (it catches up, then the leader's auto-promote driver promotes it to a
    // voter); FORGET's `RemoveNode { id }` -> drop it from the voter / learner set. The node-table
    // change and the membership change are SEPARATE committed entries (the table commits first
    // below; the membership change is proposed after), each correct on its own.
    let membership_intent = match sub.as_slice() {
        b"MEET" => cmds.iter().find_map(|c| match c {
            ironcache_raft::ConfigCmd::AddNode { id, host, port } => Some(MembershipIntent::Add {
                id: id.clone(),
                host: host.clone(),
                client_port: *port,
            }),
            _ => None,
        }),
        b"FORGET" => cmds.iter().find_map(|c| match c {
            ironcache_raft::ConfigCmd::RemoveNode { id } => {
                Some(MembershipIntent::Remove { id: id.clone() })
            }
            _ => None,
        }),
        _ => None,
    };

    // Count the command once (matching the home / remote / fan-out paths), then propose each
    // ConfigCmd in order and await its commit. The whole mutator replies `+OK` only if EVERY
    // entry commits; the FIRST NotLeader short-circuits to the `-CLUSTERDOWN` redirect (a
    // follower never commits anything, so a partial batch cannot land).
    state_rc.borrow_mut().counters.on_command();
    let handle = ctx
        .raft
        .as_ref()
        .expect("caller checked ctx.raft.is_some() before dispatching a raft mutator");
    for cmd in cmds {
        if matches!(
            handle.propose(cmd).await,
            ironcache_server::ProposeOutcome::NotLeader
        ) {
            // No leader reachable (no leader recognized, or a forward to the leader timed out;
            // with HA-9 forwarding a follower normally COMMITS transparently, so this is the
            // genuine no-leader / timeout case). PROD-9: resolve the leader's ADVERTISED CLIENT
            // endpoint (the SAME host:port `CLUSTER SHARDS` reports, dial-able by an operator) from
            // the raft `leader_id` via the committed slot map, so the redirect NAMES where to reissue
            // -- not the cluster-bus port (which is not a client target). Distinct messages for the
            // resolvable-client, unresolvable-but-known-id (degrade), and no-leader-elected cases.
            let msg = match ironcache_server::resolve_leader_hint(ctx) {
                // SelfIsLeader is unreachable here (a self-leader commits rather than redirecting),
                // but fold it into the no-leader retry text rather than panicking on an impossible
                // state: if we somehow got NotLeader while believing we are the leader, a retry is
                // the safe answer.
                ironcache_server::LeaderHint::SelfIsLeader
                | ironcache_server::LeaderHint::NoLeader => {
                    "NOTLEADER no leader is currently elected; retry the CLUSTER write once a leader is elected"
                        .to_owned()
                }
                ironcache_server::LeaderHint::Client(addr) => format!(
                    "NOTLEADER the current raft leader is {addr}; reissue the CLUSTER write there"
                ),
                ironcache_server::LeaderHint::NodeId(id) => format!(
                    "NOTLEADER this node is not the raft leader; the leader is raft node {id} (its client address is not yet known here); retry the CLUSTER write against the leader"
                ),
            };
            encode_into(out, &Value::error(ErrorReply::clusterdown(msg)), conn.proto);
            return Some(false);
        }
    }

    // HA-prod-membership: the node-table change committed; now drive the Raft config. A failure here
    // (not leader, in flight, refused) does NOT fail the whole CLUSTER command -- the table change is
    // already committed and the membership change is idempotent / retryable -- so it is surfaced as a
    // NOTE appended to the reply rather than a hard error, keeping the byte-compatible `+OK` for the
    // success path while telling the operator when the membership step needs a retry.
    if let Some(intent) = membership_intent {
        // F2: the EXISTING node table's announce ids (from the committed `ctx.cluster` map), so
        // `apply_membership_intent` can REJECT a MEET whose derived NodeId collides with an existing
        // node that has a DIFFERENT announce id (two physical nodes -> one Raft identity) rather than
        // silently swallowing it. The list is the source of announce-id -> derived-NodeId truth that
        // the raft config's `BTreeSet<NodeId>` alone cannot recover.
        let known_announce_ids: Vec<String> = ctx
            .cluster
            .as_deref()
            .map(|m| m.nodes().into_iter().map(|n| n.id.to_string()).collect())
            .unwrap_or_default();
        if let Some(note) = apply_membership_intent(handle, intent, &known_announce_ids).await {
            // A non-empty note means the membership step did not (yet) take effect; reply with a
            // -CLUSTERDOWN-style error carrying the reason so the operator retries the membership.
            encode_into(
                out,
                &Value::error(ErrorReply::clusterdown(note)),
                conn.proto,
            );
            return Some(false);
        }
    }

    encode_into(out, &Value::ok(), conn.proto);
    // The connection stays open in every case (mirrors the static CLUSTER path).
    Some(false)
}

/// The Raft-membership side of a raft-mode `CLUSTER MEET` / `FORGET` (HA-prod-membership), captured
/// from the built [`ConfigCmd`](ironcache_raft::ConfigCmd) batch so the voter / learner set is
/// driven ALONGSIDE the node table.
enum MembershipIntent {
    /// MEET: stage the node in as a non-voting LEARNER (it catches up, then auto-promotes to voter).
    /// Carries the 40-hex id (to derive the `NodeId`) plus the advertised host + client port (to
    /// derive the cluster-bus `SocketAddr` the leader replicates to).
    Add {
        id: String,
        host: String,
        client_port: u16,
    },
    /// FORGET: drop the node from the voter / learner set.
    Remove { id: String },
}

/// Apply the [`MembershipIntent`] of a committed raft-mode MEET / FORGET to the Raft config
/// (HA-prod-membership). Returns `None` on success (the membership change committed, or the FORGET
/// found nothing to remove), or `Some(note)` with an operator-facing reason when the membership step
/// did not take effect (not leader, a change already in flight, a quorum-safety refusal, or -- F2 --
/// a derived-NodeId collision with an existing different-announce-id node) so the operator can retry
/// or fix the id. The node-table commit has already happened; this only governs consensus
/// membership, which is idempotent and safely retryable.
///
/// `known_announce_ids` is the committed node table's announce ids (F2): a MEET whose derived NodeId
/// collides with an EXISTING node that has a DIFFERENT announce id is REJECTED (rather than silently
/// swallowed as an idempotent no-op), because two physical nodes mapping to one Raft identity is
/// catastrophic.
///
/// MEET stages the node as a LEARNER ([`MembershipChange::AddLearner`]): a non-voting member that
/// receives the log and catches up but is counted in NO quorum, so adding it can never stall
/// consensus. The leader's auto-promote driver later promotes it to a voter once it has caught up.
/// The new node's cluster-bus endpoint (`host` + `bus_port(client_port)`, reconstructed from the
/// MEET args) is passed as a [`PeerEndpoint`] so the leader can replicate to a runtime-joined node
/// that is NOT in the static topology peer map. The endpoint holds the HOST + PORT (a DNS hostname
/// OR an IP literal), resolved fresh on each dial -- so a hostname-addressed joiner (a k8s
/// StatefulSet pod) is reachable and a restarted pod's new IP is picked up, instead of the old code
/// which dropped a DNS-named joiner's address (`None`) because it only accepted an IP literal.
async fn apply_membership_intent(
    handle: &ironcache_server::RaftHandle,
    intent: MembershipIntent,
    known_announce_ids: &[String],
) -> Option<String> {
    use ironcache_raft::MembershipChange;
    match intent {
        MembershipIntent::Add {
            id,
            host,
            client_port,
        } => {
            let node = crate::raft_boot::node_id_from_announce(&id);
            // F2 COLLISION REJECT: the engine keys nodes by the derived NodeId (the announce id's top
            // 64 bits). If an EXISTING node with a DIFFERENT announce id derives the SAME NodeId, this
            // MEET would map two physical nodes to ONE Raft identity (catastrophic). The previous
            // guard below would SILENTLY swallow that (the colliding NodeId is already a voter/learner,
            // so `cfg.*.contains(&node)` is true and it returns `None` == success). Detect it
            // explicitly against the committed node table's announce ids and REJECT with a clear error
            // so the operator fixes the id, rather than a confusing silent no-op (or a shadowed node).
            if let Some(other) = known_announce_ids.iter().find(|known| {
                known.as_str() != id && crate::raft_boot::node_id_from_announce(known) == node
            }) {
                return Some(format!(
                    "MEET rejected: node id '{id}' derives the same raft NodeId as the existing node \
                     '{other}' (the engine keys nodes by the top 64 bits / first 16 hex digits of the \
                     announce id, which collide); use an id that differs within its first 16 hex digits"
                ));
            }
            // CRITICAL SAFETY: do NOT AddLearner a node that is ALREADY a voter (or this node
            // itself). The boot topology's voters MEET each other during formation, and a MEET is
            // idempotent on the node table; but `AddLearner` of an existing voter would DEMOTE it
            // out of the voter set (apply_membership_delta moves it voters -> learners), shrinking
            // quorum. So skip the learner-add when the named node is already a voter or is self --
            // the node table still records the MEET, the raft config is left correct. A node already
            // a LEARNER is also skipped (idempotent; it is already staged and catching up). (The F2
            // reject above already excluded a DIFFERENT-announce-id collision, so reaching this guard
            // with `contains(&node)` true means the SAME announce id is re-MEET'd -- a true no-op.)
            let cfg = handle.config();
            if node == handle.node_id()
                || cfg.voters.contains(&node)
                || cfg.learners.contains(&node)
            {
                return None;
            }
            // The new node's cluster-bus endpoint: host + (client_port + BUS_PORT_OFFSET). Held as a
            // PeerEndpoint (host + port, a DNS name OR an IP literal) so the leader can dial +
            // replicate to this runtime-joined node, re-resolving the host per dial -- a
            // hostname-addressed joiner is reachable (the old IP-only parse dropped it).
            let bus = crate::raft_boot::bus_port(client_port);
            let addr = Some(ironcache_clusterbus::PeerEndpoint::new(host.clone(), bus));
            match handle
                .propose_membership(MembershipChange::AddLearner(node), addr)
                .await
            {
                ironcache_server::MembershipOutcome::Committed(_) => None,
                ironcache_server::MembershipOutcome::NotLeader => {
                    Some("MEET committed the node table but this node is not the raft leader; retry to add it to the raft config".to_owned())
                }
                ironcache_server::MembershipOutcome::InFlight => {
                    Some("MEET committed the node table but a raft membership change is in flight; retry to add it to the raft config".to_owned())
                }
                ironcache_server::MembershipOutcome::Refused(why) => Some(why),
            }
        }
        MembershipIntent::Remove { id } => {
            let node = crate::raft_boot::node_id_from_announce(&id);
            let cfg = handle.config();
            // Nothing to do if the node is neither a voter nor a learner (a FORGET of an unknown id,
            // or one only ever in the table): the table removal already handled it.
            if !cfg.voters.contains(&node) && !cfg.learners.contains(&node) {
                return None;
            }
            let change = if cfg.voters.contains(&node) {
                MembershipChange::RemoveVoter(node)
            } else {
                MembershipChange::RemoveLearner(node)
            };
            match handle.propose_membership(change, None).await {
                ironcache_server::MembershipOutcome::Committed(_) => None,
                ironcache_server::MembershipOutcome::NotLeader => {
                    Some("FORGET committed the node table but this node is not the raft leader; retry to remove it from the raft config".to_owned())
                }
                ironcache_server::MembershipOutcome::InFlight => {
                    Some("FORGET committed the node table but a raft membership change is in flight; retry to remove it from the raft config".to_owned())
                }
                ironcache_server::MembershipOutcome::Refused(why) => Some(why),
            }
        }
    }
}

/// Build the `AssignSlots { node: self, slots }` ConfigCmd for raft-mode `CLUSTER ADDSLOTS`
/// (HA-4c). Build the SELF-ASSIGN batch for `CLUSTER ADDSLOTS` / `ADDSLOTSRANGE`: a self-`AddNode`
/// (so every node learns this node's id+endpoint before the assignment references it; idempotent
/// on self) FOLLOWED by `AssignSlots { node: self, slots }`. `parse_slots` extracts the slot list
/// from the request (the per-verb arity + slot validation, mirroring the static `cmd_cluster`
/// Redis error shapes).
fn build_self_assign(
    ctx: &ServerContext,
    request: &Request,
    parse_slots: impl Fn(&Request) -> Result<Vec<u16>, ironcache_protocol::ErrorReply>,
) -> Result<Vec<ironcache_raft::ConfigCmd>, ironcache_protocol::ErrorReply> {
    let slots = parse_slots(request)?;
    let (id, host, port) = self_node_endpoint(ctx);
    Ok(vec![
        ironcache_raft::ConfigCmd::AddNode {
            id: id.clone(),
            host,
            port,
        },
        ironcache_raft::ConfigCmd::AssignSlots { node: id, slots },
    ])
}

/// Build the committed `ConfigCmd` for a manual `CLUSTER FAILOVER` (#371): promote THIS node from
/// replica to OWNER of the slots it replicates, proposed + committed through the leader (the same
/// raft path every other `ConfigCmd` mutator uses).
///
/// DATA-SAFETY (the crux): refuse unless this node is an IN-SYNC replica, reusing the EXACT gate the
/// AUTOMATIC promotion and the replica-read path use ([`replica_read_in_sync`]: `is_in_sync` within
/// `replica_max_lag`, ADR-0026). So a manual failover can NEVER promote a node the automatic path
/// would not, and a stale replica is never promoted (which would lose committed writes). The
/// committed `PromoteReplica` then atomically transfers ownership + bumps the config epoch (the
/// split-brain fence: at most one owner per slot per epoch), and the OLD owner steps down on apply.
/// There is a small check-to-commit window (the replica could fall behind before the entry commits),
/// identical to the automatic path's; the epoch fence still guarantees no two committed owners.
///
/// `FORCE` / `TAKEOVER` (which in Redis bypass the in-sync and committed-consensus safety) are
/// REFUSED: the only supported form is the safe, gated, committed failover.
fn build_failover(
    ctx: &ServerContext,
    request: &Request,
) -> Result<Vec<ironcache_raft::ConfigCmd>, ironcache_protocol::ErrorReply> {
    // FORCE / TAKEOVER would bypass the safety gates; not supported (do not bypass, per #371).
    if request.args.len() > 2 {
        return Err(ironcache_protocol::ErrorReply::err(
            "CLUSTER FAILOVER FORCE/TAKEOVER is not supported (it would bypass the in-sync + \
             committed-consensus safety gates); use a bare CLUSTER FAILOVER",
        ));
    }
    // THE DATA-SAFETY GATE: only an in-sync replica may take over (the SAME gate the automatic
    // promotion uses). A non-replica / link-down / lagging node is refused here, never promoted.
    if !replica_read_in_sync(ctx) {
        return Err(ironcache_protocol::ErrorReply::err(
            "CLUSTER FAILOVER refused: this node is not an in-sync replica (not a replica, link \
             down, or lagging past replica_max_lag); promoting it would risk losing committed writes",
        ));
    }
    let Some(map) = ctx.cluster.as_ref() else {
        return Err(ironcache_protocol::ErrorReply::err(
            "CLUSTER FAILOVER requires cluster mode with a slot map",
        ));
    };
    // The slots this node currently replicates are exactly the slots it would take ownership of.
    let slots: Vec<u16> = (0..ironcache_cluster::CLUSTER_SLOTS)
        .filter(|&s| map.is_replica_of_self(s))
        .collect();
    if slots.is_empty() {
        return Err(ironcache_protocol::ErrorReply::err(
            "CLUSTER FAILOVER refused: this node replicates no slots to take over",
        ));
    }
    let (id, _host, _port) = self_node_endpoint(ctx);
    Ok(vec![ironcache_raft::ConfigCmd::PromoteReplica {
        slots,
        new_primary: id,
    }])
}

/// Build the `UnassignSlots { slots }` ConfigCmd for raft-mode `CLUSTER DELSLOTS` / `DELSLOTSRANGE`
/// (the inverse of [`build_self_assign`]). `parse_slots` is the SAME per-verb slot parser ADDSLOTS /
/// ADDSLOTSRANGE use (`parse_addslots_slots` / `parse_addslotsrange_slots`), so the arity + slot +
/// range validation (and the Redis error shapes) match the add path exactly. UN-assign needs no
/// `AddNode` prefix (it references no node) and clears the slots on EVERY node (the committed map is
/// shared), so a single `UnassignSlots` entry is the whole proposal.
fn build_unassign(
    request: &Request,
    parse_slots: impl Fn(&Request) -> Result<Vec<u16>, ironcache_protocol::ErrorReply>,
) -> Result<Vec<ironcache_raft::ConfigCmd>, ironcache_protocol::ErrorReply> {
    let slots = parse_slots(request)?;
    Ok(vec![ironcache_raft::ConfigCmd::UnassignSlots { slots }])
}

/// Build the `UnassignSlots { slots }` ConfigCmd for raft-mode `CLUSTER FLUSHSLOTS` (Redis clears
/// the node's OWN slots). Arity is exactly 2 (the Redis FLUSHSLOTS form; a wrong argc is the
/// addReplySubcommandSyntaxError class, mirroring the static path). The slot set is every slot THIS
/// node currently owns in the committed map (read via `owns()`), so the proposal UN-assigns exactly
/// the running node's slots; an empty set (the node owns nothing) is a valid, degenerate batch.
///
/// DOCUMENTED DIVERGENCE (same as the static `cluster_flushslots`): Redis errors `DB must be empty
/// to perform CLUSTER FLUSHSLOTS.` when the keyspace is non-empty; IronCache has no per-slot key
/// count index yet, so it cannot test DB-emptiness and proposes unconditionally.
fn build_flushslots(
    ctx: &ServerContext,
    request: &Request,
) -> Result<Vec<ironcache_raft::ConfigCmd>, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::{CLUSTER_SLOTS, ErrorReply};
    if request.args.len() != 2 {
        // Wrong argc: the addReplySubcommandSyntaxError class (Redis parity), not wrong-arity.
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    // The committed map is always installed as ctx.cluster in raft-mode (the caller established
    // cluster_mode == Raft); collect the slots this node owns. If, defensively, no map is present,
    // there are no owned slots to clear, so the batch is empty (a harmless no-op proposal).
    let slots: Vec<u16> = match ctx.cluster.as_deref() {
        Some(map) => (0..CLUSTER_SLOTS).filter(|&s| map.owns(s)).collect(),
        None => Vec::new(),
    };
    Ok(vec![ironcache_raft::ConfigCmd::UnassignSlots { slots }])
}

/// Parse the slot list of `CLUSTER ADDSLOTS <slot>...` (arity Min(3); each slot strictly
/// validated, mirroring the static path's Redis error shapes).
fn parse_addslots_slots(request: &Request) -> Result<Vec<u16>, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() < 3 {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    let mut slots = Vec::with_capacity(request.args.len() - 2);
    for a in &request.args[2..] {
        slots.push(parse_slot_strict(a)?);
    }
    Ok(slots)
}

/// Parse + expand the `<start> <end>` pairs of `CLUSTER ADDSLOTSRANGE` (even, non-empty arg count;
/// each slot strictly validated, `start <= end`).
fn parse_addslotsrange_slots(
    request: &Request,
) -> Result<Vec<u16>, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    let pairs = &request.args[2..];
    if pairs.is_empty() {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    if pairs.len() % 2 != 0 {
        return Err(ErrorReply::wrong_arity("cluster|addslotsrange"));
    }
    let mut slots = Vec::new();
    for pair in pairs.chunks_exact(2) {
        let start = parse_slot_strict(&pair[0])?;
        let end = parse_slot_strict(&pair[1])?;
        if start > end {
            return Err(ErrorReply::err(format!(
                "start slot number {start} is greater than end slot number {end}"
            )));
        }
        slots.extend(start..=end);
    }
    Ok(slots)
}

/// Build the SETSLOT ConfigCmd for raft-mode `CLUSTER SETSLOT` (HA-4c + HA-6). Four forms:
/// - `<slot> NODE <id>`      -> [`ConfigCmd::SetSlotOwner`]   (the committed FLIP, HA-4c).
/// - `<slot> MIGRATING <id>` -> [`ConfigCmd::SetSlotMigrating`] (source-side handshake, HA-6).
/// - `<slot> IMPORTING <id>` -> [`ConfigCmd::SetSlotImporting`] (destination-side handshake, HA-6).
/// - `<slot> STABLE`         -> [`ConfigCmd::SetSlotStable`]    (clear/abort, HA-6).
///
/// NODE/MIGRATING/IMPORTING take a node id (argc == 5); STABLE takes none (argc == 4). Any other
/// action or a known action at the wrong argc is the single Redis SETSLOT error.
///
/// HA-6 (Finding 2): the `IMPORTING <src>` proposal carries an explicit `dest` so apply tags
/// IMPORTING on EXACTLY the destination node (via `SlotMap::is_self`), never on a bystander
/// non-owner. The wire command stays `SETSLOT <slot> IMPORTING <src>` (the operator names only the
/// source). In IronCache's raft model every CLUSTER mutator is proposed by the LEADER (a follower
/// replies `-CLUSTERDOWN`), so "the node running the command" is the leader, NOT the importer --
/// using the local node id would tag the leader, which is wrong. The slot is already MIGRATING
/// toward a known DEST (the MIGRATING step of the handshake committed first), so the builder reads
/// the recorded migration peer (`migration_peer_id`) as the dest. If the slot is not yet migrating
/// on the leader (a malformed handshake with no prior MIGRATING, or a single node issuing IMPORTING
/// against itself), it falls back to the local node id -- the conservative choice that tags the
/// running node, matching the standalone Redis-style case.
fn build_setslot(
    ctx: &ServerContext,
    request: &Request,
) -> Result<ironcache_raft::ConfigCmd, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    let setslot_err = || {
        ErrorReply::err("Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP")
    };
    // The shortest form (STABLE) is 4 args; a node-id form is 5.
    if request.args.len() < 4 {
        return Err(setslot_err());
    }
    let slot = parse_slot_strict(&request.args[2])?;
    let action = ascii_upper(&request.args[3]);
    let node = |request: &Request| String::from_utf8_lossy(&request.args[4]).into_owned();
    match action.as_slice() {
        b"NODE" if request.args.len() == 5 => Ok(ironcache_raft::ConfigCmd::SetSlotOwner {
            slot,
            node: node(request),
        }),
        b"MIGRATING" if request.args.len() == 5 => {
            Ok(ironcache_raft::ConfigCmd::SetSlotMigrating {
                slot,
                dest: node(request),
            })
        }
        b"IMPORTING" if request.args.len() == 5 => {
            // The dest is the node this slot is MIGRATING toward (the recorded migration peer the
            // prior committed MIGRATING step set), so apply tags IMPORTING on EXACTLY that node --
            // never on the leader (which proposes every mutator) or a bystander non-owner. Fall back
            // to the local node id when the slot is not yet migrating on the leader (a handshake with
            // no prior MIGRATING, or a node issuing IMPORTING against itself).
            let dest = ctx
                .cluster
                .as_deref()
                .and_then(|m| m.migration_peer_id(slot))
                .unwrap_or_else(|| self_node_endpoint(ctx).0);
            Ok(ironcache_raft::ConfigCmd::SetSlotImporting {
                slot,
                src: node(request),
                dest,
            })
        }
        b"STABLE" if request.args.len() == 4 => {
            Ok(ironcache_raft::ConfigCmd::SetSlotStable { slot })
        }
        // Unknown action, or a known action at the wrong argc.
        _ => Err(setslot_err()),
    }
}

/// The MAX slot moves one `CLUSTER REBALANCE APPLY` ARMS per call. The command proposes + awaits each
/// `ConfigCmd` synchronously, so this bounds the command's latency; a large rebalance is armed over
/// several calls (re-running arms the next batch of not-yet-migrating moves). `* 2` because each move
/// is a MIGRATING + an IMPORTING proposal.
const MAX_REBALANCE_APPLY_MOVES: usize = 128;

/// Build the committed `ConfigCmd` batch for `CLUSTER REBALANCE APPLY` (#371, REBALANCE_APPLY.md).
///
/// For each planned move ([`SlotMap::rebalance_moves`]) whose slot is NOT already migrating (up to the
/// per-call cap), it tags the SOURCE `MIGRATING <dest>` and the DESTINATION `IMPORTING <src>` -- which
/// ARMS HA-6's `run_import_control` to auto-copy the slot's keys + tail to the destination. It does
/// NOT propose the ownership FLIP (`SETSLOT NODE`): the operator finalizes each slot with
/// `CLUSTER SETSLOT <slot> NODE <dest>` once `CLUSTER COUNTKEYSINSLOT` shows the destination caught up
/// (a background auto-flip controller is a tracked follow-up). Leaving the flip out is the SAFE choice
/// -- APPLY never races a last-moment source write against the flip.
///
/// Idempotent + resumable: every `SetSlot*` apply is idempotent, and re-running APPLY skips slots
/// already migrating and arms the NEXT batch, so a big rebalance is driven over repeated calls (the
/// operator flips caught-up slots in between, which lets `rebalance_moves` recompute). An empty batch
/// (already balanced, or every move already in flight) commits nothing and replies `+OK`.
fn build_rebalance_apply(
    ctx: &ServerContext,
) -> Result<Vec<ironcache_raft::ConfigCmd>, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    let map = ctx
        .cluster
        .as_deref()
        .ok_or_else(|| ErrorReply::err("This instance has cluster support disabled"))?;
    Ok(rebalance_apply_cmds(map, MAX_REBALANCE_APPLY_MOVES))
}

/// The PURE core of [`build_rebalance_apply`] (#371): the `MIGRATING` + `IMPORTING` `ConfigCmd`s for
/// up to `max_moves` of `map`'s planned moves whose slot is not already migrating. Pure over the slot
/// map, so the batch is unit-tested without a raft quorum. Deterministic (it walks
/// [`SlotMap::rebalance_moves`]'s deterministic order).
fn rebalance_apply_cmds(
    map: &ironcache_cluster::SlotMap,
    max_moves: usize,
) -> Vec<ironcache_raft::ConfigCmd> {
    let mut cmds = Vec::new();
    for mv in map.rebalance_moves() {
        if cmds.len() >= max_moves * 2 {
            break;
        }
        // Skip slots already migrating (armed by a prior APPLY): re-running arms the NEXT batch.
        if map.migration_state(mv.slot) != ironcache_cluster::MigrationState::None {
            continue;
        }
        cmds.push(ironcache_raft::ConfigCmd::SetSlotMigrating {
            slot: mv.slot,
            dest: mv.dst_node_id.clone(),
        });
        cmds.push(ironcache_raft::ConfigCmd::SetSlotImporting {
            slot: mv.slot,
            src: mv.src_node_id,
            dest: mv.dst_node_id,
        });
    }
    cmds
}

/// The bound on the MEET id-learning fetch: how long a raft-mode `CLUSTER MEET` will wait for the
/// peer's `CLUSTER MYID` before falling back to the synth id. Generous enough for a one-round-trip
/// loopback / LAN fetch, short enough that a MEET to a not-yet-up peer does not hang the serve
/// path (it falls back and the cluster still forms). Read through the Runtime timer seam.
const MEET_ID_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// Build the `AddNode { id, host, port }` ConfigCmd for raft-mode `CLUSTER MEET <ip> <port> [bus]`
/// (HA-4c + item-7 id-reconciliation).
///
/// HISTORY / THE BUG THIS FIXES: the original raft-mode MEET SYNTHESIZED the peer's id from
/// `host:port` (`synth_meet_node_id`) because there is no gossip to learn the real id. But every
/// node ALSO self-adds under its REAL announce id (`empty_self`) and is declared under that id, so
/// a MEET'd peer ended up in the committed node table under BOTH a synth id AND its announce id ->
/// `cluster_known_nodes` / `CLUSTER NODES` were INFLATED with a duplicate per MEET'd peer (routing
/// stayed correct -- it matches by ENDPOINT -- but the operator-visible node count was wrong).
///
/// THE FIX: on a raft-mode MEET we LEARN the peer's REAL announce id by dialing the peer's RESP
/// CLIENT port (`host:port`, the same endpoint a client / a MOVED redirect uses -- NOT the
/// `+10000` cluster-bus port, which speaks only RAFTMSG) and reading `CLUSTER MYID` over the
/// cluster-bus `peer_node_id` helper. The fetch is BOUNDED by [`MEET_ID_FETCH_TIMEOUT`] through
/// the Runtime timer seam so it can never hang the serve path. We then propose
/// `AddNode { id: real_id, host, port }`; because the peer self-added that SAME announce id and the
/// committed `meet` apply is idempotent on a duplicate id, the table holds ONE entry per node and
/// `cluster_known_nodes` equals the real node count (no inflation).
///
/// FALLBACK (peer unreachable): if the fetch fails or times out (the peer is not yet up, refuses,
/// or returns a non-id reply), we FALL BACK to the deterministic `synth_meet_node_id` so a MEET to
/// a transiently-down peer STILL makes progress and the cluster forms (the synth entry later
/// reconciles when the peer comes up and is re-MEET'd, or via the cluster crate's defensive
/// `SlotMap::dedup_nodes_by_endpoint`). This is the documented fallback the slice-3 static MEET
/// also uses.
///
/// SCOPING (no SWIM): this is a LIGHTWEIGHT id-reconciliation, deliberately NOT a SWIM/Lifeguard
/// failure detector. Raft already provides the cluster's liveness + failover signal (heartbeats,
/// elections, committed `PromoteReplica`), so a separate gossip failure detector would be
/// redundant. The only gap raft-mode MEET had was learning a peer's stable IDENTITY at join time,
/// which one bounded `CLUSTER MYID` fetch closes; ongoing liveness stays Raft's job.
///
/// The `request` is the validated `CLUSTER MEET` frame; the runtime is a fresh zero-sized
/// [`TokioRuntime`] (the dial is one short-lived outbound connection over the seam, like the
/// expire task's runtime; no shard state is touched and no hot-path lock is taken).
async fn build_meet(
    request: &Request,
) -> Result<ironcache_raft::ConfigCmd, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() != 4 && request.args.len() != 5 {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    let host = String::from_utf8_lossy(&request.args[2]).into_owned();
    let port_arg = String::from_utf8_lossy(&request.args[3]).into_owned();
    let Some(port) = ironcache_server::cmd_util::parse_i64(&request.args[3]) else {
        return Err(ErrorReply::err(format!(
            "Invalid base port specified: {port_arg}"
        )));
    };
    if !(1..=65535).contains(&port) {
        return Err(ErrorReply::err(format!(
            "Invalid node address specified: {host}:{port_arg}"
        )));
    }
    let port = port as u16;
    // Learn the peer's REAL announce id over the bus (bounded); fall back to the synth id when the
    // peer is unreachable so the cluster still forms.
    let id = learn_or_synth_meet_id(&host, port).await;
    Ok(ironcache_raft::ConfigCmd::AddNode { id, host, port })
}

/// Resolve the node id to commit for a raft-mode `CLUSTER MEET <host> <port>`: the peer's REAL
/// announce id when it can be fetched within [`MEET_ID_FETCH_TIMEOUT`], else the deterministic
/// `synth_meet_node_id` fallback (item-7). Dials the peer's RESP CLIENT port (`host:port`) and
/// reads `CLUSTER MYID` via [`ironcache_clusterbus::peer_node_id`], BOUNDED by the Runtime timer
/// seam (`select!` of the fetch vs the timer) so a not-yet-up peer never hangs the serve path.
///
/// The fetched id is accepted ONLY when it is a syntactically valid node id (40 lowercase hex);
/// any other reply (an empty / malformed id, an error, a wrong reply kind) is treated as a failed
/// fetch and falls back to the synth id, so a peer that is up but not yet cluster-identity-ready
/// can never poison the committed table with a junk id.
async fn learn_or_synth_meet_id(host: &str, port: u16) -> String {
    let synth = || synth_meet_node_id(host, port);
    let rt = TokioRuntime::new();
    // The advertised CLIENT endpoint (what a MOVED redirect / a client dials). RESOLVE it accepting
    // a DNS hostname OR an IP literal (k8s): a hostname-addressed peer can now be dialed to learn its
    // real id, where the old IP-only parse fell straight back to the synth id for any DNS name. A
    // host that does not resolve (a peer not yet up) still falls back to the synth id so the cluster
    // forms; the id is reconciled later via the auto-promote / status path.
    //
    // H1: `resolve` is now ASYNC (getaddrinfo on tokio's blocking pool, bounded by RESOLVE_TIMEOUT
    // via the Runtime timer seam), so a wedged resolver can never freeze THIS serve task; it is
    // awaited with the same `rt` that bounds the id fetch below.
    let Ok(addr) = ironcache_clusterbus::PeerEndpoint::new(host, port)
        .resolve(&rt)
        .await
    else {
        return synth();
    };
    // Bound the fetch: whichever of the fetch or the timer completes first wins. The timer is the
    // sanctioned time seam (no `std::time` / `tokio::time` directly), matching the adapter's
    // FORWARD_TIMEOUT shape.
    let learned = tokio::select! {
        r = ironcache_clusterbus::peer_node_id(&rt, addr) => r.ok(),
        () = rt.timer(MEET_ID_FETCH_TIMEOUT) => None,
    };
    match learned {
        Some(id) if is_valid_node_id(&id) => id,
        // Unreachable / timed out / a non-id reply: fall back to the synth id so the cluster forms.
        _ => synth(),
    }
}

/// Whether `id` is a syntactically valid IronCache node id: exactly 40 lowercase-hex characters
/// (the shape `CLUSTER MYID` / the announce id / `synth_meet_node_id` all produce). Used to gate a
/// fetched MEET id so a peer that answers with a malformed / empty id falls back to the synth id
/// rather than committing junk into the node table.
fn is_valid_node_id(id: &str) -> bool {
    id.len() == 40
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Build the `RemoveNode { id }` ConfigCmd for raft-mode `CLUSTER FORGET <id>` (HA-4c).
fn build_forget(
    request: &Request,
) -> Result<ironcache_raft::ConfigCmd, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() != 3 {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    Ok(ironcache_raft::ConfigCmd::RemoveNode {
        id: String::from_utf8_lossy(&request.args[2]).into_owned(),
    })
}

/// Build the `SetConfigEpoch(epoch)` ConfigCmd for raft-mode `CLUSTER SET-CONFIG-EPOCH <epoch>`
/// (HA-4c). A negative epoch is the Redis invalid-epoch error.
fn build_set_config_epoch(
    request: &Request,
) -> Result<ironcache_raft::ConfigCmd, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() != 3 {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    let Some(epoch) = ironcache_server::cmd_util::parse_i64(&request.args[2]) else {
        return Err(ErrorReply::not_an_integer());
    };
    if epoch < 0 {
        return Err(ErrorReply::err(format!(
            "Invalid config epoch specified: {epoch}"
        )));
    }
    Ok(ironcache_raft::ConfigCmd::SetConfigEpoch(epoch as u64))
}

/// Build the `AssignReplica { node, slots }` ConfigCmd for raft-mode `CLUSTER REPLICATE <node-id>
/// <slot> [slot ...]` (HA-7d). The first arg after the subcommand is the node id that should
/// REPLICATE the listed slots; the rest are strictly-validated slots. Arity is Min(4) (the verb,
/// REPLICATE, the node id, and at least one slot). The committed entry records `node` as the
/// replica of each slot in the shared map; the named node then attaches to each slot owner's
/// primary and serves READONLY reads.
fn build_replicate(
    request: &Request,
) -> Result<ironcache_raft::ConfigCmd, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() < 4 {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    let node = String::from_utf8_lossy(&request.args[2]).into_owned();
    let mut slots = Vec::with_capacity(request.args.len() - 3);
    for a in &request.args[3..] {
        slots.push(parse_slot_strict(a)?);
    }
    Ok(ironcache_raft::ConfigCmd::AssignReplica { node, slots })
}

/// THIS node's committed identity `(id, host, port)` for a self-`AddNode` / `AssignSlots`,
/// read from the shared map's self entry (the advertised endpoint a MOVED redirect points at).
/// Falls back to the boot node id + bind/port if the map is somehow absent (unreachable in
/// raft-mode, which always installs the shared map as `ctx.cluster`).
fn self_node_endpoint(ctx: &ServerContext) -> (String, String, u16) {
    match ctx.cluster.as_deref() {
        Some(m) => {
            let me = m.me();
            (me.id.to_string(), me.host.to_string(), me.port)
        }
        None => (
            ctx.info.cluster_node_id.to_owned(),
            ctx.boot.bind.to_string(),
            ctx.info.tcp_port,
        ),
    }
}

/// Parse + bounds-check a slot the way Redis's `getSlotOrReply` does for the mutator paths: a
/// non-integer OR an out-of-range value is the single `Invalid or out of range slot` error.
fn parse_slot_strict(arg: &[u8]) -> Result<u16, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::{CLUSTER_SLOTS, ErrorReply};
    match ironcache_server::cmd_util::parse_i64(arg) {
        Some(n) if (0..i64::from(CLUSTER_SLOTS)).contains(&n) => Ok(n as u16),
        _ => Err(ErrorReply::err("Invalid or out of range slot")),
    }
}

/// Synthesize a deterministic 40-lowercase-hex placeholder node id from a MEET endpoint (FNV-1a
/// over `host:port`, hex-padded to 40), so the MEET'd peer is addressable before gossip learns
/// its real id. The SAME derivation the static slice-3 `cmd_cluster` MEET uses, so a node MEET'd
/// in either mode gets the identical id.
fn synth_meet_node_id(host: &str, port: u16) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let endpoint = format!("{host}:{port}");
    for b in endpoint.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let hex16 = format!("{h:016x}");
    let mut id = String::with_capacity(40);
    while id.len() < 40 {
        id.push_str(&hex16);
    }
    id.truncate(40);
    id
}

/// Dispatch ONE shard-spanning gather-combine command to its per-command fan-out
/// (COORDINATOR.md #107, Stage 2b), split out of [`route_and_dispatch`] so the router stays
/// small. The caller has already established the command is a supported gather-combine token
/// whose keys SPAN shards (the `spanning_set_fan_out` gate) and bumped `commands_processed`.
///
/// BITOP / PFCOUNT / PFMERGE (Stage 2b-3) each have their OWN parse + combine, so each gets a
/// dedicated fan-out; the eight zset tokens (Stage 2b-2) share `fan_out_zset`; the seven set
/// tokens (Stage 2b-1) share `fan_out_set`. The fan-out gathers each source from its owner
/// (the home subset LOCALLY + sync, the rest via their drain loops), combines with the PURE
/// combiner shared with the single-shard handler, and for the write forms writes the result
/// to the dest owner, encoding the reply into `out`.
async fn dispatch_spanning_combine(
    ctx: &ServerContext,
    conn: &ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    cmd_upper: &[u8],
    request: &Request,
    out: &mut Vec<u8>,
) {
    match cmd_upper {
        b"BITOP" => {
            crate::spanning_combine::fan_out_bitop(
                inbox, ctx, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        b"PFCOUNT" => {
            crate::spanning_combine::fan_out_pfcount(
                inbox, ctx, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        b"PFMERGE" => {
            crate::spanning_combine::fan_out_pfmerge(
                inbox, ctx, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        _ if is_fan_out_spanning_zset(cmd_upper) => {
            crate::spanning_combine::fan_out_zset(
                inbox, ctx, cmd_upper, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        _ => {
            crate::spanning_combine::fan_out_set(
                inbox, ctx, cmd_upper, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
    }
}

/// The in-MULTI transaction-correctness guards (COORDINATOR.md #107, the critical fix), split
/// out of [`route_and_dispatch`] so the router stays small. Returns the connection-close flag
/// (always `false` here; in-MULTI commands never close).
///
/// A command issued inside a transaction must be QUEUED (reply `+QUEUED`), not executed:
/// routing it remotely (the dispatch_via / multikey / whole-keyspace branches) would EXECUTE it
/// eagerly and out of transaction order, since the queue gate lives in `dispatch` on the HOME
/// path only. So EVERY in-MULTI command goes to the HOME path EXCEPT the two reject-loudly
/// cases below. The KEY INVARIANT: a transaction reaches real (home-only) EXEC ONLY when ALL its
/// watched keys AND all queued commands' keys are HOME-OWNED, so home execution is always
/// correct; otherwise it is rejected LOUDLY (correct, or explicitly aborted -- never silently
/// wrong). True cross-shard transactions (txid + ordered apply) are Stage 3. With `shards == 1`
/// every key is home-owned, so the guards never fire and this is the pre-coordinator behavior.
#[allow(clippy::too_many_arguments)]
fn route_in_multi(
    ctx: &ServerContext,
    conn: &mut ConnState,
    home: ShardId,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    route: route::CommandClass,
    request: &Request,
    out: &mut Vec<u8>,
) -> bool {
    let keyed = matches!(
        route,
        route::CommandClass::KeyedSingle | route::CommandClass::KeyedMulti
    );
    // CLUSTER QUEUE-TIME REDIRECT (CLUSTER_CONTRACT.md #70, slice 2). A queued data command
    // whose key(s) are not served by THIS node must honor cluster routing too, or a
    // `MULTI; SET foreign-key v; EXEC` would silently execute a non-owned write. Redis replies
    // the MOVED / CROSSSLOT error at QUEUE time AND dirties the transaction (so `EXEC` returns
    // `-EXECABORT` and applies nothing). We run the SAME `cluster_redirect` predicate as the
    // live path (one source of truth, no second key extractor), and on a redirect reply the
    // error for the queued command and mark the transaction dirty. This is checked BEFORE the
    // intra-node `all_keys_home_owned` gate: cluster ownership (which NODE) is the outer
    // question, internal-shard ownership (which of MY shards) the inner one.
    if let Some(map) = ctx.cluster.as_deref() {
        let in_sync = replica_read_in_sync(ctx);
        // HA-6: the in-MULTI QUEUE-TIME redirect honors ASKING exactly like the non-MULTI live path,
        // by building the SAME `MigrationCtx { asking, key_present }` and passing `Some(&mig)` to the
        // shared `cluster_redirect`. The `asking` is the TRANSACTION-SCOPED `conn.txn_asking` (the
        // PRE-MULTI one-shot the router carried into this transaction), NOT the per-command one-shot
        // (consumed at the top of `route_and_dispatch` and gone by the time commands queue). This
        // mirrors Redis, whose cluster redirect runs at QUEUE time with `CLIENT_ASKING` still live:
        // `ASKING; MULTI; <cmd on an IMPORTING slot>; EXEC` is QUEUED + served on the importing
        // destination, while WITHOUT ASKING the same queued command MOVEDs/dirties (the migration arm
        // is inert unless the slot is actually MIGRATING/IMPORTING, so a non-migrating slot is
        // byte-identical to before -- the static MOVED/CROSSSLOT decision). The key-presence resolver
        // reads THIS connection's accept-shard store at the current time, consulted ONLY when a slot
        // is mid-migration (the cold path).
        //
        // MULTI-SHARD note: unlike the non-MULTI live path (which pre-resolves a sibling-shard key's
        // presence via the coordinator for an EXACT ASK), the QUEUE-TIME path uses the LOCAL read and
        // does NOT hop. It does not need to: this redirect runs BEFORE the `all_keys_home_owned` gate
        // below, which REJECTS (and dirties the transaction with the cross-shard error) any queued
        // keyed command with a key on a SIBLING shard -- such a command can never EXEC correctly
        // home-only (cross-shard transactions are Stage 3), so it is aborted regardless of presence.
        // The only key the local read can mis-classify (a present sibling-shard key) is one that is
        // about to be rejected anyway; for a HOME-owned key the local read is already exact. So the
        // queue-time path is correctly conservative without a hop (and `route_in_multi` stays sync).
        let now = UnixMillis(env.borrow().now_unix_millis());
        let db = conn.db;
        let key_present = |k: &[u8]| store_rc.borrow().contains_live(db, k, now);
        let mig = MigrationCtx {
            asking: conn.txn_asking,
            key_present: &key_present,
        };
        if let Some(reply) = cluster_redirect(
            map,
            route,
            cmd_upper,
            request,
            conn.readonly,
            in_sync,
            Some(&mig),
            shard_owner_home(ctx, home),
        ) {
            state_rc.borrow_mut().counters.on_command();
            conn.dirty_exec = true;
            encode_into(out, &ironcache_server::Value::error(reply), conn.proto);
            return false;
        }
    }
    // A KEYED DATA command whose keys are not ALL home-owned is rejected at queue time (Redis's
    // queue-time-error behavior): reply the cross-shard error NOW and dirty the transaction, so
    // EXEC returns -EXECABORT and applies nothing. Bump commands_processed like the other paths.
    if keyed && !all_keys_home_owned(cmd_upper, request, home) {
        state_rc.borrow_mut().counters.on_command();
        conn.dirty_exec = true;
        encode_into(
            out,
            &ironcache_server::Value::error(
                ironcache_protocol::ErrorReply::txn_cross_shard_command(),
            ),
            conn.proto,
        );
        return false;
    }
    // A WHOLE-KEYSPACE command (KEYS/SCAN/DBSIZE/FLUSHALL/FLUSHDB/RANDOMKEY) cannot run correctly
    // home-only at EXEC when the keyspace is partitioned: EXEC replays synchronously on the HOME
    // store, so it would cover only the home shard's ~1/N (a `MULTI; FLUSHALL; EXEC` would
    // partially flush -- silent data RETENTION). There is no single owner to hop to and EXEC
    // cannot fan out (it is synchronous), so reject at queue time (dirty -> -EXECABORT), the same
    // "correct or explicitly aborted, never silently wrong" contract as the cross-shard keyed
    // case. Gate on `home.total > 1`: with one shard the home shard IS the whole keyspace, so
    // they run correctly home-only and must keep working (shards == 1 byte-identical parity).
    if matches!(route, route::CommandClass::WholeKeyspace) && home.total > 1 {
        state_rc.borrow_mut().counters.on_command();
        conn.dirty_exec = true;
        encode_into(
            out,
            &ironcache_server::Value::error(
                ironcache_protocol::ErrorReply::txn_whole_keyspace_unsupported(),
            ),
            conn.proto,
        );
        return false;
    }
    // All-home keyed command OR a control verb: HOME path. `dispatch`'s queue gate queues the
    // keyed command (`+QUEUED`) and runs EXEC/DISCARD/etc. specially. This is the ONLY routing
    // branch taken while in_multi (no remote hop, no fan-out), so a transaction that reaches real
    // EXEC has ALL queued keys home-owned -> home-only EXEC is correct.
    handle_request(
        ctx, conn, env, store_rc, wheel_rc, state_rc, request, cmd_upper, out,
    )
}

/// The LIVE (non-MULTI) blocking-command handler (PROD-9): the FIRST attempt + the park
/// decision. WAIT is handled inline (it touches no keys); the pop family parses + attempts the
/// non-blocking op. Returns the connection-close flag (always `false` here -- a blocking command
/// never closes the connection) and, when the command must PARK, sets `*block_request` to the
/// [`BlockPark`] the serve loop's park loop consumes.
///
/// On the FAST path (data present, or a parse / WRONGTYPE error) it replies immediately and
/// leaves `block_request` `None`. On the PARK path it leaves `out` EMPTY and sets
/// `block_request`. The `commands_processed` counter is bumped exactly once (on the immediate
/// reply OR when the park is set up), matching every other reply path.
#[allow(clippy::too_many_arguments)]
fn handle_blocking_live(
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    request: &Request,
    out: &mut Vec<u8>,
    block_request: &mut Option<BlockPark>,
) -> bool {
    // WAIT numreplicas timeout (PROD-9): block until at least `numreplicas` replicas have acked,
    // or `timeout` ms elapse; reply the integer count of in-sync replicas. It touches NO keyspace,
    // so it has no pop attempt / waiter key. If the quorum is ALREADY met (or numreplicas == 0),
    // reply the current count immediately; else PARK on the replica-ack count (the serve loop polls
    // the count under the timer seam).
    if cmd_upper == b"WAIT" {
        return handle_wait_live(ctx, conn, state_rc, request, out, block_request);
    }

    // Parse + ATTEMPT the blocking pop. A parse error replies immediately (no park).
    let spec = match ironcache_server::parse_block(cmd_upper, request) {
        Ok(s) => s,
        Err(e) => {
            state_rc.borrow_mut().counters.on_command();
            encode_into(out, &ironcache_server::Value::error(e), conn.proto);
            return false;
        }
    };
    state_rc.borrow_mut().counters.on_command();
    let now = UnixMillis(env.borrow().now_unix_millis());
    let attempt = {
        let mut store = store_rc.borrow_mut();
        ironcache_server::try_block_op(&mut *store, conn.db, now, &spec)
    };
    // Data present (or a WRONGTYPE error): reply immediately. The store mutation recorded any
    // keyspace event(s); the caller (`route_and_dispatch`) drains + publishes them right after this
    // returns (via `publish_pending_keyspace_events`), so a blocking pop fires the same lpop/rpop/
    // zpopmin notification as the non-blocking pop. Every key empty/absent (`None`): PARK -- leave
    // `out` empty and set `block_request`; the serve loop runs the park loop (re-attempt on a wake,
    // or the nil-array on timeout).
    if let Some(reply) = attempt {
        encode_into(out, &reply, conn.proto);
    } else {
        *block_request = Some(BlockPark { spec, db: conn.db });
    }
    false
}

/// WAIT's LIVE handler (PROD-9): parse `numreplicas` + `timeout`, and either reply the current
/// in-sync replica count immediately (the quorum is already met, or numreplicas == 0) or PARK on
/// it. Parking is represented by a `BlockPark` with NO keys and the WAIT op carried via the spec's
/// `keys`/`op` being unused; the serve loop's WAIT park loop polls the count under the timer seam.
fn handle_wait_live(
    ctx: &ServerContext,
    conn: &mut ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out: &mut Vec<u8>,
    block_request: &mut Option<BlockPark>,
) -> bool {
    state_rc.borrow_mut().counters.on_command();
    if request.args.len() != 3 {
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity("wait")),
            conn.proto,
        );
        return false;
    }
    let (Some(numreplicas), Some(timeout_ms)) = (
        parse_wait_int(&request.args[1]),
        parse_wait_int(&request.args[2]),
    ) else {
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::not_an_integer()),
            conn.proto,
        );
        return false;
    };
    if timeout_ms < 0 {
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::err(
                "timeout is negative",
            )),
            conn.proto,
        );
        return false;
    }
    let current = ironcache_server::in_sync_replica_count(ctx);
    // The quorum is already met (or 0 requested): reply the count now, no park.
    if numreplicas <= current {
        encode_into(out, &ironcache_server::Value::Integer(current), conn.proto);
        return false;
    }
    // PARK: carry the WAIT parameters in a BlockPark. The op + keys are a WAIT marker (no keys);
    // the serve loop polls the in-sync count vs `numreplicas` under the timer.
    *block_request = Some(BlockPark {
        spec: ironcache_server::BlockSpec {
            timeout_ms: if timeout_ms == 0 {
                None
            } else {
                Some(timeout_ms as u64)
            },
            keys: Vec::new(),
            op: ironcache_server::BlockOp::Wait {
                numreplicas: numreplicas.max(0) as u64,
            },
        },
        db: conn.db,
    });
    false
}

/// Parse a WAIT integer arg (numreplicas / timeout) the strict Redis way.
fn parse_wait_int(arg: &[u8]) -> Option<i64> {
    core::str::from_utf8(arg).ok()?.parse::<i64>().ok()
}

/// The poll quantum for a WAIT park (PROD-9): how often to re-check the in-sync replica count + a
/// kill while parked, so an UNPAUSE / a newly-attached replica / a CLIENT KILL is observed
/// promptly. WAIT polls because its quorum is published by the repl tasks, not via a waiter
/// registry.
const WAIT_POLL_QUANTUM: core::time::Duration = core::time::Duration::from_millis(50);

/// The kill-poll quantum for a POP park (PROD-9 FIX2): the UPPER BOUND on a pop park's timer arm,
/// so a forever-parked (no timeout) blocked client on an idle key still reaches its loop-top
/// `is_killed()` check within ~50ms of a `CLIENT KILL` and is torn down promptly. The pop park is
/// otherwise spin-free (it parks on the waiter `Notify` for the wake); this bounded re-check exists
/// ONLY so a kill of an idle-key forever-park is not deferred until the next push / pipelined bytes
/// / peer close. `ClientHandle` is deliberately runtime-agnostic (it depends only on
/// `ironcache-env`, no tokio), so it carries no wake handle of its own; a bounded poll -- mirroring
/// WAIT's existing quantum -- is the runtime-coupling-free way to make a kill prompt. A push still
/// wakes the park immediately via the `Notify` (no added latency on the data path).
const KILL_POLL_QUANTUM: core::time::Duration = core::time::Duration::from_millis(50);

/// Run the BLOCKING PARK loop (PROD-9): park this connection until a wake (a push to a waited key
/// makes it ready), the timeout elapses, the connection is closed/killed, or (for WAIT) the
/// replica-ack quorum is met. Returns the connection-CLOSE flag (`true` to tear the connection
/// down: a peer close or an I/O error while parked).
///
/// ## Mechanism (the core of PROD-9)
///
/// POP PARK (BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP):
/// 1. Register a per-shard FIFO [`crate::blocking::Waiter`] on EVERY key (the RAII
///    [`crate::blocking::WaiterGuard`] deregisters on EVERY exit -- success, timeout, close, kill,
///    or a panic -- so a parked connection never leaks a registry entry and a push never wakes a
///    gone connection).
/// 2. `select!` on (the waiter's `Notify` wake / the runtime timer to the deadline / a stream read,
///    which detects a PEER CLOSE while parked). NO busy-wait: the wake arm parks on the `Notify`.
/// 3. On a WAKE re-attempt the pop. Success -> encode + flush the reply, drop the guard, return.
///    Still empty (another waiter raced it, or a spurious wake) -> loop and re-park on the SAME
///    `Notify` (the guard is held the whole time, so the waiter keeps its FIFO position).
/// 4. On TIMEOUT -> encode + flush the nil-array reply, drop the guard, return.
///
/// WAIT PARK: no waiter (it touches no keys); POLL the in-sync replica count vs `numreplicas` under
/// a short timer quantum until the quorum is met or the timeout elapses, then reply the count.
///
/// A KILL (CLIENT KILL flagged this connection) or a peer close ends the park early.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_block_park(
    stream: &mut ironcache_runtime::ClientStream,
    timer_rt: &TokioRuntime,
    ctx: &ServerContext,
    conn: &ConnState,
    client_handle: &std::sync::Arc<ironcache_observe::ClientHandle>,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    inbox: &coordinator::Inbox,
    home: ShardId,
    read_buf: &mut Vec<u8>,
    out: &mut Vec<u8>,
    park: BlockPark,
) -> bool {
    // The absolute DEADLINE (a monotonic instant), or None for "block forever". Computed ONCE from
    // the Env clock seam (ADR-0003, NOT wall clock) so a re-park after a spurious wake counts the
    // already-elapsed time toward the same deadline (the timer re-arms with the REMAINING duration).
    let start = env.borrow().now();
    let deadline: Option<ironcache_env::Monotonic> = park
        .spec
        .timeout_ms
        .map(|ms| start.saturating_add(core::time::Duration::from_millis(ms)));

    // WAIT parks on the replica-ack quorum, not a key waiter.
    if let ironcache_server::BlockOp::Wait { numreplicas } = park.spec.op {
        return wait_park(
            stream,
            timer_rt,
            ctx,
            conn,
            client_handle,
            env,
            out,
            numreplicas,
            deadline,
        )
        .await;
    }

    // POP PARK. Register a FIFO waiter on every key BEFORE the first attempt; the guard deregisters
    // on EVERY exit (RAII). Registering first is what makes the loop below an ATTEMPT-THEN-PARK
    // (register-then-recheck) rather than a park-then-attempt: see the re-attempt at the top of the
    // loop and the lost-wakeup note there.
    let registry = shard_blocking();
    let (_guard, wake) =
        crate::blocking::WaiterGuard::park(&registry, park.db, park.spec.wait_keys(), conn.id);

    // Whether to PROBE the store this iteration. Set on the FIRST iteration (the register-then-
    // recheck) and after a WAKE / pipelined BYTES (a push may have made a key ready). NOT set after a
    // bare kill-poll timer tick: a periodic tick must only re-check `is_killed()`, NEVER re-probe the
    // store -- otherwise a NON-front waiter could grab a pushed element off its own poll tick,
    // breaking FIFO fairness. The pop is driven by the WAKE path (FIFO: a push wakes only the FRONT
    // waiter); the poll exists solely for prompt kill detection on a forever-park.
    let mut attempt_pop = true;

    loop {
        // A kill observed between iterations ends the park (the reply is abandoned; the connection
        // is torn down). Cold relaxed load. With the bounded KILL_POLL_QUANTUM arm in the select!
        // below, a forever-park (no timeout) on an idle key still reaches this check within ~50ms
        // of a `CLIENT KILL`, so a killed blocked client is torn down promptly (PROD-9 FIX2) rather
        // than only on the next push / pipelined bytes / peer close.
        if client_handle.is_killed() {
            return true;
        }

        // RE-ATTEMPT THE POP (register-then-recheck, PROD-9 FIX1), but ONLY when `attempt_pop` is set
        // (the first iteration, or after a wake / pipelined bytes -- never on a bare kill-poll tick).
        // The waiter is ALREADY registered (above, on the first iteration; the same guard is held on
        // every later iteration), so the FIRST-iteration probe closes the LOST-WAKEUP window: when
        // this blocking command is pipelined behind a reply-producing command, the serve loop FLUSHES
        // that earlier reply (an `await` that can yield) BEFORE calling this function. A concurrent
        // push during that pre-registration flush would have called `wake_one` and found NO waiter
        // (ours not yet registered) -> woken nobody. Because the waiter is registered before this
        // recheck, that push's element is observed HERE on the first iteration instead of being lost
        // until timeout. The recheck also covers the cross-shard wake path (a sibling-shard push that
        // ran here through `run_remote`) and any number of awaits that preceded registration. Cost:
        // one extra store probe per park (a COLD path, never the hot per-command path). On a WAKE this
        // is the same re-attempt the prior park-then-attempt loop did, so the keep-FIFO-position
        // behavior is unchanged (the guard is held the whole time).
        if attempt_pop {
            let now = UnixMillis(env.borrow().now_unix_millis());
            let attempt = {
                let mut store = store_rc.borrow_mut();
                ironcache_server::try_block_op(&mut *store, park.db, now, &park.spec)
            };
            if let Some(reply) = attempt {
                // A successful blocked pop fires the SAME lpop/rpop/zpopmin keyspace event as a
                // non-blocking pop; publish it AFTER the reply is flushed (per-connection FIFO).
                let closed = flush_block_reply(stream, out, conn.proto, reply).await;
                publish_pending_keyspace_events(inbox, home.index).await;
                return closed;
            }
        }

        // Compute the REMAINING time to the deadline; if already past, reply the nil-array (timeout).
        // This is evaluated EVERY iteration (including after a kill-poll tick) so a real timeout still
        // fires even though the poll tick itself does not re-probe the store.
        let remaining: Option<core::time::Duration> = match deadline {
            None => None,
            Some(dl) => {
                let now = env.borrow().now();
                if now >= dl {
                    // Timed out: reply the nil-array and finish.
                    return flush_block_reply(stream, out, conn.proto, block_timeout_value()).await;
                }
                Some(dl.saturating_duration_since(now))
            }
        };

        // PARK: select on the wake, the timer, and a stream read (peer-close detection). The read is
        // into a FRESH buffer and APPENDED to `read_buf` so a partial frame already in `read_buf`
        // survives a cancelled read (the same pattern the idle wait uses). NO RefCell borrow is held
        // across the await.
        //
        // The timer duration is the remaining time CAPPED at KILL_POLL_QUANTUM (for a finite
        // deadline) or exactly KILL_POLL_QUANTUM (for a forever-park), so a killed forever-parked
        // client notices within ~50ms (mirrors the WAIT poll's bounded quantum). The timer arm fires
        // either at the real deadline (-> the next iteration's remaining-time check replies the
        // nil-array) OR at the bounded poll quantum before the deadline (-> loop, re-check
        // `is_killed()` ONLY, no store probe). The two are distinguished by re-reading the clock at
        // the top of the loop.
        let timer_dur = match remaining {
            Some(dur) => dur.min(KILL_POLL_QUANTUM),
            None => KILL_POLL_QUANTUM,
        };
        let woken = tokio::select! {
            () = wake.notified() => WakeOutcome::Wake,
            () = timer_rt.timer(timer_dur) => WakeOutcome::Timer,
            res = stream.recv(Vec::new()) => match res {
                Ok(r) if r.n == 0 => return true, // peer closed while parked
                Ok(r) => { read_buf.extend_from_slice(&r.buf[..r.n]); WakeOutcome::Bytes }
                Err(_) => return true,
            },
        };

        // Decide whether the NEXT iteration probes the store. A WAKE (a push woke THIS front waiter)
        // or pipelined BYTES drive a re-attempt; a bare kill-poll TIMER tick does NOT (it only loops
        // to re-check `is_killed()` + the deadline, preserving FIFO -- only the woken front waiter
        // races for the element).
        attempt_pop = matches!(woken, WakeOutcome::Wake | WakeOutcome::Bytes);
    }
}

/// The outcome of a single park `select!` (PROD-9): which arm fired.
enum WakeOutcome {
    /// The waiter `Notify` fired (a push to a waited key): re-attempt the pop.
    Wake,
    /// The park timer elapsed: either the real deadline (the next loop iteration replies the
    /// nil-array once it confirms the deadline is past) OR a bounded kill-poll tick (the next
    /// iteration re-checks `is_killed()` ONLY -- it does NOT re-probe the store, so a poll tick
    /// never lets a non-front waiter steal a pushed element, preserving FIFO fairness). The two are
    /// distinguished by re-reading the clock at the top of the loop.
    Timer,
    /// New bytes arrived while parked (a pipelined command): re-attempt (harmless) and keep the
    /// bytes in `read_buf` for the decode loop to process after the park ends.
    Bytes,
}

/// The WAIT park (PROD-9): poll the in-sync replica count vs `numreplicas` under a short timer
/// quantum until the quorum is met or the deadline elapses, then reply the CURRENT count. A peer
/// close or a kill ends it early. WAIT touches no keys, so there is no waiter registry entry; the
/// quorum is published by the repl tasks (a relaxed atomic load), so a poll is the right model.
#[allow(clippy::too_many_arguments)]
async fn wait_park(
    stream: &mut ironcache_runtime::ClientStream,
    timer_rt: &TokioRuntime,
    ctx: &ServerContext,
    conn: &ConnState,
    client_handle: &std::sync::Arc<ironcache_observe::ClientHandle>,
    env: &Rc<RefCell<SystemEnv>>,
    out: &mut Vec<u8>,
    numreplicas: u64,
    deadline: Option<ironcache_env::Monotonic>,
) -> bool {
    loop {
        if client_handle.is_killed() {
            return true;
        }
        let current = ironcache_server::in_sync_replica_count(ctx);
        // Quorum met: reply the count.
        if current >= 0 && (current as u64) >= numreplicas {
            return flush_block_reply(
                stream,
                out,
                conn.proto,
                ironcache_server::Value::Integer(current),
            )
            .await;
        }
        // Remaining time to the deadline; if past, reply the current count (Redis: WAIT returns the
        // count it achieved on timeout, typically below `numreplicas`).
        let wait = match deadline {
            None => WAIT_POLL_QUANTUM,
            Some(dl) => {
                let now = env.borrow().now();
                if now >= dl {
                    return flush_block_reply(
                        stream,
                        out,
                        conn.proto,
                        ironcache_server::Value::Integer(current),
                    )
                    .await;
                }
                dl.saturating_duration_since(now).min(WAIT_POLL_QUANTUM)
            }
        };
        // Race a short poll quantum against a peer close (so a disconnect ends the wait promptly).
        tokio::select! {
            () = timer_rt.timer(wait) => {}
            res = stream.recv(Vec::new()) => {
                match res {
                    Ok(r) if r.n == 0 => return true, // peer closed
                    // Bytes while parked in WAIT: Redis would not process a new command until WAIT
                    // returns; we drop them (a rare edge -- a client pipelining behind WAIT). The
                    // poll loop continues. (Buffering them safely is a documented follow-up.)
                    Ok(_) => {}
                    Err(_) => return true,
                }
            }
        }
    }
}

/// Encode `reply` into a FRESH `out` and flush it over the stream, returning the connection-CLOSE
/// flag (`true` on an I/O error). `out` is cleared first (any pipelined replies were already
/// flushed before the park), so this writes exactly the blocking command's reply.
async fn flush_block_reply(
    stream: &mut ironcache_runtime::ClientStream,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
    reply: ironcache_server::Value,
) -> bool {
    out.clear();
    encode_into(out, &reply, proto);
    match stream.send(std::mem::take(out)).await {
        Ok(returned) => {
            *out = returned;
            false
        }
        Err(_) => true,
    }
}

/// The nil-array a blocking pop replies on timeout (Redis NULL ARRAY: RESP2 `*-1`, RESP3 `_`).
fn block_timeout_value() -> ironcache_server::Value {
    ironcache_server::block_timeout_reply()
}

/// Dispatch one request and append its encoded reply to `out`. Returns whether
/// the connection should close after flushing (QUIT).
///
/// `env` is the shard's owned-mutable env handle; `store_rc` is the shard's store.
/// The absolute `now` deadline basis is computed ONCE here from the Env wall clock
/// (ADR-0003: the store reads no clock) and passed into dispatch wrapped in
/// [`UnixMillis`]; the data commands convert relative EX/PX against it. Clock reads
/// go through `env.borrow()`; the store is mutated through `store_rc.borrow_mut()`.
#[allow(clippy::too_many_arguments)]
fn handle_request(
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    cmd_upper: &[u8],
    out: &mut Vec<u8>,
) -> bool {
    state_rc.borrow_mut().counters.on_command();
    let snapshot_fn = || state_rc.borrow().counters.snapshot();
    let rollup: &dyn Fn() -> CounterSnapshot = &snapshot_fn;
    // COMMANDSTATS / ERRORSTATS render (#413): render the serving shard's per-command + per-error
    // tables into the INFO section bodies. Invoked ONLY when INFO asks for those sections (the
    // closure is not called otherwise), and it borrows `state_rc` immutably like `rollup` does
    // (sequentially, never aliasing dispatch's later mutable borrow).
    let cmdstats_fn = || {
        let st = state_rc.borrow();
        let (mut cs, mut es) = (String::new(), String::new());
        st.command_stats.render_commandstats(&mut cs);
        st.command_stats.render_errorstats(&mut es);
        (cs, es)
    };
    let cmdstats: ironcache_server::CmdStatsFn<'_> = &cmdstats_fn;
    // Compute `now` once per command from the shard's wall clock, then run dispatch
    // against the per-shard store. `env` and `store` are SEPARATE RefCells, so the
    // env clock read at the dispatch call site can overlap the held store
    // borrow_mut with no conflict: overlapping borrows of distinct RefCells never
    // alias the same cell.
    let now = UnixMillis(env.borrow().now_unix_millis());
    // The process-global allocator figures for INFO (ADR-0006). One call advances
    // the jemalloc epoch (a mallctl) ONCE and reads allocated + resident from the
    // SAME snapshot, so the two INFO figures are mutually consistent. Read it ONLY
    // for INFO (once, on the shard serving the command) and keep it off every other
    // command's hot path. A process-global figure must NOT be summed across shards;
    // one read on the serving shard is the honest total.
    let mem = if request.command().eq_ignore_ascii_case(b"INFO") {
        let (used_memory, used_memory_rss) = process_memory();
        MemoryInfo {
            used_memory,
            used_memory_rss,
        }
    } else {
        MemoryInfo::default()
    };
    let mut deltas = CounterDeltas::default();
    // The shard's last-seen runtime-config generation (PR-4b), copied OUT of state_rc
    // into a plain local so dispatch can take `&mut` it WITHOUT borrowing state_rc
    // (the rollup closure already captured state_rc immutably for INFO; a held mutable
    // borrow of the same cell would conflict). Dispatch updates the local on a
    // generation-change policy swap; we write it back after dispatch returns.
    let mut shard_generation = state_rc.borrow().last_policy_generation;
    // The lazy-backstop expiry count this command produced (a separate signal from the
    // dispatch deltas): the store accumulates it inside the four primitives, and we
    // drain it after dispatch returns and fold it into `expired_keys` alongside the
    // active-drain count, so both expiry paths feed the INFO counter.
    let lazy_expired;
    let reply = {
        let mut store = store_rc.borrow_mut();
        let mut wheel = wheel_rc.borrow_mut();
        // dispatch now takes `env: &mut E` (clock + RNG, ADR-0003): RANDOMKEY draws a
        // random index through the RNG half, so the env handle must be MUTABLE. `env`
        // is a SEPARATE RefCell from store/wheel, so `env.borrow_mut()` here does not
        // alias the held store/wheel borrows. `now` was already read above from a
        // distinct, now-dropped `env.borrow()`.
        let mut env_ref = env.borrow_mut();
        // Use the cross-shard serve loop's already-computed uppercased command (FIX 5):
        // `dispatch_with_cmd` skips the second `ascii_upper` allocation on this hot path.
        let r = dispatch_with_cmd(
            ctx,
            conn,
            &mut *env_ref,
            &mut *store,
            &mut wheel,
            now,
            &mut shard_generation,
            rollup,
            cmdstats,
            mem,
            &mut deltas,
            request,
            cmd_upper,
        );
        drop(env_ref);
        lazy_expired = store.take_lazy_expired();
        r
        // The store/wheel borrows end here, BEFORE the counter apply below borrows
        // `state_rc` mutably (the rollup closure captured `state_rc` too, so the two
        // borrows must not overlap; they do not, the dispatch call has returned).
    };
    // Fold this command's dynamic counters into the shard's totals for INFO and write
    // back the (possibly advanced) policy generation. Each is a cheap no-op on the
    // common hot path (no deltas, no generation change).
    {
        deltas.expired += lazy_expired;
        let reset_stats = deltas.reset_stats;
        let mut st = state_rc.borrow_mut();
        if deltas != CounterDeltas::default() {
            st.counters.apply(deltas);
        }
        // CONFIG RESETSTAT (#413): the same signal that zeroes the counter cell also clears the
        // per-command + per-error stats tables (Redis `resetServerStats` resets both).
        if reset_stats {
            st.command_stats.reset();
        }
        st.last_policy_generation = shard_generation;
    }
    encode_into(out, &reply, conn.proto);
    conn.should_close
}

/// Record a command into the SLOWLOG ring + the LATENCY `command` event IF it met the threshold
/// (PROD-7). Called ONLY when the SLOWLOG was enabled at the start of the command (the hot-path hook
/// short-circuits otherwise), so the elapsed-time read + the threshold compare are the only cost on
/// a fast command, and the ring/monitor locks are touched ONLY for a genuinely slow command (rare).
///
/// `start` is the monotonic instant captured before dispatch; the elapsed micros are measured here
/// through the SAME Env clock seam (ADR-0003). The unix TIMESTAMP for the entry is read from the Env
/// wall clock. The args + this connection's addr/name are copied into the entry (capped by the ring
/// builder). The LATENCY `command` event samples the same elapsed time in milliseconds, gated on a
/// fixed floor so the monitor only records meaningful spikes.
/// Record one command into the serving shard's INFO COMMANDSTATS / ERRORSTATS tables (#413),
/// driven off the already-encoded reply so there is no second dispatch. `out_before` is the
/// offset where THIS command's reply began in `out`; an error reply starts with `-`, and its
/// CODE is the first token after the `-` (up to a space or CR). Only REGISTRY commands are
/// tracked (an unknown command has no canonical name and was rejected); the name key is the
/// registry `&'static`, so a record allocates nothing. `elapsed_us` is this command's measured
/// micros (shared with the slowlog timing read). Off the per-key hot path; one map update.
fn record_command_stats(
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out_before: usize,
    out: &[u8],
    elapsed_us: u64,
) {
    let cmd_upper = ascii_upper(request.command());
    let Some(spec) = ironcache_server::spec_of(&cmd_upper) else {
        return;
    };
    // The reply for THIS command begins at `out_before`; a leading `-` is an error reply (the
    // command ran but failed). A push/array/status/integer/bulk lead byte is a success.
    let failed = out.get(out_before) == Some(&b'-');
    let mut st = state_rc.borrow_mut();
    st.command_stats.record(spec.name, elapsed_us, failed);
    if failed {
        // The error CODE: the first whitespace/CR-delimited token after the `-`.
        let code_start = out_before + 1;
        let rest = &out[code_start..];
        let code_len = rest
            .iter()
            .position(|&b| b == b' ' || b == b'\r')
            .unwrap_or(rest.len());
        st.command_stats.record_error(&rest[..code_len]);
    }
}

/// CLIENT TRACKING read-register / write-invalidate hook (#409), run after each command. A READ by
/// a tracking connection registers its read keys in THIS shard's tracking table; ANY write (by any
/// connection) invalidates its keys for every tracking client (NOLOOP skips the writer's own
/// connection); FLUSHALL/FLUSHDB invalidate everything.
///
/// PERF: the common no-tracking path is gated to a single bool + one thread-local borrow + `is_none`
/// (the table is never created until a tracking client reads), so a server with no tracking clients
/// pays one cheap check per command and allocates nothing. Only when tracking is active does it
/// uppercase the command + consult the key spec.
///
/// SCOPE (this stage): SINGLE-SHARD-correct. A tracking client's read and the matching write both
/// run on the key's owner shard, which IS this shard when `shards == 1` (the default + the
/// differential bar). The cross-shard case (a read routed to a remote owner shard) is a documented
/// follow-up; a stale foreign-shard entry self-heals when its key next changes (the push to a gone
/// connection fails and is shed).
/// Whether a DEFAULT-mode tracking read should register its keys, given the OPTIN/OPTOUT mode and
/// the one-shot `CLIENT CACHING` flag (#409 stage 3). Default mode (neither OPTIN nor OPTOUT) tracks
/// every read; OPTIN tracks only after `CACHING YES`; OPTOUT tracks unless `CACHING NO`.
fn tracking_should_register_read(conn: &ConnState) -> bool {
    if conn.tracking_optin {
        conn.caching_next == Some(true)
    } else if conn.tracking_optout {
        conn.caching_next != Some(false)
    } else {
        true
    }
}

/// Consume the one-shot `CLIENT CACHING` flag (#409 stage 3): it is cleared after the command that
/// FOLLOWS `CLIENT CACHING` (i.e. every command except `CLIENT CACHING` itself, which sets it), so
/// the OPTIN/OPTOUT decision applies to exactly one command. A single `is_some` check when no flag
/// is pending (the common case), so the non-OPTIN hot path is unaffected.
fn consume_caching_flag(conn: &mut ConnState, request: &Request) {
    if conn.caching_next.is_none() {
        return;
    }
    let is_caching_cmd = request.command().eq_ignore_ascii_case(b"CLIENT")
        && request
            .args
            .get(1)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"CACHING"));
    if !is_caching_cmd {
        conn.caching_next = None;
    }
}

fn apply_client_tracking(
    conn: &ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    request: &Request,
    was_tracking: bool,
    was_bcast: bool,
) {
    // CLIENT TRACKING OFF / RESET transition: the connection WAS tracking and now is not. Purge it
    // from this shard's table (both the per-key and the BCAST prefix sets) so a later write does
    // not push to a connection that opted out.
    if was_tracking && !conn.tracking_on {
        purge_conn_tracking(conn.id);
        return;
    }

    // BCAST mode transitions (#409 stage 2). Entering BCAST registers the connection's prefixes
    // (the EMPTY prefix when none were given = track all keys); leaving BCAST (a re-issue of
    // TRACKING ON without BCAST) purges the stale prefix entries. Both happen on the CLIENT TRACKING
    // command itself (it registers no read and writes no key), so we fall through after.
    if was_bcast && !conn.tracking_bcast {
        purge_conn_tracking(conn.id);
    }
    if conn.tracking_bcast && !was_bcast {
        // A REDIRECT client (stage 4) whose target is not (yet) subscribed registers nothing; for
        // BCAST that means the target must SUBSCRIBE `__redis__:invalidate` BEFORE enabling tracking
        // (BCAST registers once here, not per read, so it does not self-heal on a later read).
        if let Some(entry) = make_track_entry(conn, push_tx, shed_flag) {
            let tbl = shard_tracking();
            let mut t = tbl.borrow_mut();
            if conn.tracking_prefixes.is_empty() {
                t.track_prefix(bytes::Bytes::new(), conn.id, entry);
            } else {
                for p in &conn.tracking_prefixes {
                    t.track_prefix(p.clone(), conn.id, entry.clone());
                }
            }
        }
    }

    // The cheap gate: skip entirely unless THIS connection is tracking (it may need to register a
    // read) OR the per-shard table already holds trackers (a write may need to invalidate).
    let table_has_trackers = TRACKING.with(|cell| {
        cell.borrow()
            .as_ref()
            .is_some_and(|t| !t.borrow().is_empty())
    });
    if !conn.tracking_on && !table_has_trackers {
        return;
    }

    let cmd_upper = ascii_upper(request.command());
    if ironcache_server::is_write(&cmd_upper) {
        if !table_has_trackers {
            return;
        }
        let tbl = shard_tracking();
        let mut t = tbl.borrow_mut();
        // NOLOOP: a tracking writer does not get the echo for its OWN change.
        let skip = conn.tracking_noloop.then_some(conn.id);
        if cmd_upper == b"FLUSHALL" || cmd_upper == b"FLUSHDB" {
            t.invalidate_all(skip);
            return;
        }
        // Conservative: invalidate every key the write NAMED (a no-op write over-invalidates, which
        // is safe -- the client just re-reads; it never MISSES a real change).
        match ironcache_server::command_keys(&cmd_upper, request) {
            ironcache_server::KeySpec::One(k) => {
                t.invalidate(k, skip);
            }
            ironcache_server::KeySpec::Many(ks) => {
                for k in ks {
                    t.invalidate(k, skip);
                }
            }
            ironcache_server::KeySpec::None => {}
        }
    } else if conn.tracking_on && !conn.tracking_bcast && tracking_should_register_read(conn) {
        // A READ by a DEFAULT-mode tracking connection (and, in OPTIN/OPTOUT, one the one-shot
        // CLIENT CACHING gate admits): register every key it read so a later change pushes an
        // invalidation. (A BCAST connection tracks PREFIXES, not reads, so it skips this.) A
        // non-keyed read (PING/INFO/...) registers nothing.
        let keys: Vec<bytes::Bytes> = match ironcache_server::command_keys(&cmd_upper, request) {
            ironcache_server::KeySpec::One(k) => vec![bytes::Bytes::copy_from_slice(k)],
            ironcache_server::KeySpec::Many(ks) => ks
                .iter()
                .map(|k| bytes::Bytes::copy_from_slice(k))
                .collect(),
            ironcache_server::KeySpec::None => Vec::new(),
        };
        if keys.is_empty() {
            return;
        }
        // A REDIRECT client (stage 4) registers the TARGET's handle; if the target is not currently
        // subscribed to `__redis__:invalidate`, skip (it self-heals when the client next reads).
        let Some(entry) = make_track_entry(conn, push_tx, shed_flag) else {
            return;
        };
        let tbl = shard_tracking();
        let mut t = tbl.borrow_mut();
        for k in keys {
            t.track(k, conn.id, entry.clone());
        }
    }
}

/// Build the tracking-table entry for THIS connection's registration (#409). A REDIRECT client
/// (stage 4, `tracking_redirect != 0`) registers the redirect TARGET's push handle with
/// `redirect = true` (the target must be SUBSCRIBEd to `__redis__:invalidate`); a non-redirect
/// client registers its OWN push handle. Returns `None` for a redirect client whose target is not
/// currently subscribed there, so the caller skips registration (it self-heals on a later read).
fn make_track_entry(
    conn: &ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
) -> Option<crate::pubsub::TrackEntry> {
    if conn.tracking_redirect != 0 {
        let sub = resolve_redirect_target(conn.tracking_redirect)?;
        Some(crate::pubsub::TrackEntry {
            sub,
            redirect: true,
        })
    } else {
        Some(crate::pubsub::TrackEntry {
            sub: crate::pubsub::Subscriber {
                sender: push_tx.clone(),
                shed: std::sync::Arc::clone(shed_flag),
            },
            redirect: false,
        })
    }
}

/// Resolve the push handle of a CLIENT TRACKING REDIRECT target (#409 stage 4): the target must have
/// SUBSCRIBEd `__redis__:invalidate`, so its [`crate::pubsub::Subscriber`] lives in THIS shard's
/// Pub/Sub channel table. Returns `None` when the target is not (currently) subscribed there. SCOPE:
/// single-shard-correct, exactly like the rest of tracking (the redirect target's SUBSCRIBE and the
/// key's owner shard coincide when `shards == 1`).
fn resolve_redirect_target(target_id: u64) -> Option<crate::pubsub::Subscriber> {
    let tbl = shard_pubsub();
    let t = tbl.borrow();
    t.channels
        .get(crate::pubsub::REDIRECT_INVALIDATE_CHANNEL)
        .and_then(|subs| subs.get(&target_id))
        .cloned()
}

/// HOTKEYS recording hook (#428): attribute one command's resource use to its keys while a tracking
/// session is active. The CALLER gates this on `ctx.hotkeys.is_active()` (one relaxed atomic), so the
/// default (no session) path never reaches here. Piggybacks the already-measured `cmd_elapsed_us`
/// (the CPU metric) and the reply byte delta; the request payload bytes are summed here. A command
/// with no routable key (HOTKEYS itself, PING, ...) attributes nothing to any key, only to the
/// session totals.
fn record_hotkeys(
    ctx: &ServerContext,
    env: &Rc<RefCell<SystemEnv>>,
    request: &Request,
    cmd_elapsed_us: u64,
    reply_bytes: u64,
) {
    let cmd_upper = ascii_upper(request.command());
    let req_bytes: u64 = request.args.iter().map(|a| a.len() as u64).sum();
    let net_bytes = req_bytes.saturating_add(reply_bytes);
    let now_ms = env.borrow().now_unix_millis();
    let keys: Vec<&[u8]> = match ironcache_server::command_keys(&cmd_upper, request) {
        ironcache_server::KeySpec::One(k) => vec![k],
        ironcache_server::KeySpec::Many(ks) => ks,
        ironcache_server::KeySpec::None => Vec::new(),
    };
    ctx.hotkeys.record(&keys, cmd_elapsed_us, net_bytes, now_ms);
}

fn record_slow_command(
    ctx: &ServerContext,
    env: &Rc<RefCell<SystemEnv>>,
    conn: &ConnState,
    request: &Request,
    start: ironcache_env::Monotonic,
    threshold_micros: i64,
) {
    // Read the clock ONCE (monotonic now + wall unix) under a single short borrow.
    let (elapsed, unix_secs) = {
        let e = env.borrow();
        let elapsed = e.now().saturating_duration_since(start);
        let unix_secs = e.now_unix_millis() / 1000;
        (elapsed, unix_secs)
    };
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    // Threshold compare: `threshold_micros` is >= 0 here (the caller gated on enabled). `0` logs
    // everything; a positive value logs only commands at/above it.
    if micros >= u64::try_from(threshold_micros).unwrap_or(0) {
        let raw_args: Vec<Vec<u8>> = request.args.iter().map(|a| a.to_vec()).collect();
        // SECRET-LEAK FIX: SLOWLOG entries are readable by any @admin via `SLOWLOG GET`, and with
        // `slowlog-log-slower-than 0` EVERY command (including auth) is logged. Redact the secret
        // args of auth/password-setting commands BEFORE they enter the ring, so a password never
        // sits in the slow log in cleartext (Redis applies the same per-command sensitive-arg rule).
        let cmd_upper = ascii_upper(request.command());
        let raw_args = redact_args_for_slowlog(&cmd_upper, raw_args);
        ctx.slowlog.record(
            unix_secs,
            micros,
            &raw_args,
            conn.addr.clone(),
            conn.name.clone(),
        );
    }
    // LATENCY `command` event (PROD-7): sample this command's elapsed time in MILLISECONDS, gated on
    // a fixed floor so only meaningful spikes are recorded (a sub-millisecond command is never a
    // latency event). This is the always-tracked event; subsystem events are a follow-up.
    if micros >= ironcache_observe::LATENCY_COMMAND_FLOOR_MICROS {
        ctx.latency.record("command", unix_secs, micros / 1000);
    }
}

/// The placeholder a redacted SLOWLOG argument is replaced with (Redis convention).
const SLOWLOG_REDACTED: &[u8] = b"(redacted)";

/// Redact the secret arguments of `args` (the verbatim request, `args[0]` = command) for a
/// SLOWLOG entry, based on the UPPERCASED command `cmd_upper`. Returns the (possibly rewritten)
/// argument vector; non-sensitive commands are returned UNCHANGED.
///
/// This runs ONLY inside [`record_slow_command`], i.e. only for a command already deemed slow,
/// so it is off the hot path. It mirrors the Redis per-command sensitive-arg rule:
/// - `AUTH`: every arg after the verb is the credential (`AUTH pass` or `AUTH user pass`); redact
///   all of them. (Redis only redacts the password; redacting every post-verb arg is a strict
///   superset that can never leak the password and never reveals a username either.)
/// - `HELLO`: redact the two args following an `AUTH` token (the username and password).
/// - `CONFIG SET`: when the parameter (arg2, case-insensitive) is `requirepass` or `masterauth`,
///   redact its value (arg3).
/// - `ACL SETUSER`: redact every password/hash rule token (`>`/`<`/`#`/`!`) via the shared
///   [`ironcache_server::acl::redacted_rule`] so the redaction matches the ACL error reply.
fn redact_args_for_slowlog(cmd_upper: &[u8], mut args: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    match cmd_upper {
        b"AUTH" => {
            // Redact every arg after the verb (the credential(s)).
            for a in args.iter_mut().skip(1) {
                *a = SLOWLOG_REDACTED.to_vec();
            }
        }
        b"HELLO" => {
            // Redact the (user, pass) pair following an AUTH token, wherever it sits.
            let mut i = 1;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"AUTH") {
                    for a in args.iter_mut().skip(i + 1).take(2) {
                        *a = SLOWLOG_REDACTED.to_vec();
                    }
                    break;
                }
                i += 1;
            }
        }
        b"CONFIG" => {
            // CONFIG SET <param> <value> [<param> <value> ...]: redact the value of every
            // requirepass/masterauth pair (case-insensitive param match).
            if args.len() >= 2 && args[1].eq_ignore_ascii_case(b"SET") {
                let mut i = 2;
                while i + 1 < args.len() {
                    if args[i].eq_ignore_ascii_case(b"requirepass")
                        || args[i].eq_ignore_ascii_case(b"masterauth")
                    {
                        args[i + 1] = SLOWLOG_REDACTED.to_vec();
                    }
                    i += 2;
                }
            }
        }
        b"ACL" => {
            // ACL SETUSER <name> <rule>...: redact each password/hash rule token, reusing the
            // canonical ACL redactor so SLOWLOG and the ACL error reply never drift.
            if args.len() >= 3 && args[1].eq_ignore_ascii_case(b"SETUSER") {
                for a in args.iter_mut().skip(3) {
                    let token = String::from_utf8_lossy(a);
                    let redacted = ironcache_server::acl::redacted_rule(&token);
                    if redacted != token {
                        *a = redacted.into_bytes();
                    }
                }
            }
        }
        _ => {}
    }
    args
}

/// Encode `value` and append the bytes to `out`. `Vec<u8>` is a `bytes::BufMut` sink, so
/// `encode` writes the reply STRAIGHT into `out` -- no per-reply `BytesMut` allocation and no
/// intermediate copy (the encoder is generic over the sink; PROTOCOL.md's zero-copy note).
fn encode_into(out: &mut Vec<u8>, value: &ironcache_server::Value, proto: ProtoVersion) {
    ironcache_protocol::encode(out, value, proto);
}

/// Whether EVERY routing key of a KEYED data command (`KeyedSingle`/`KeyedMulti`) is owned
/// by the HOME shard (COORDINATOR.md #107, the in-MULTI cross-shard guard). Used inside a
/// transaction to decide whether a queued command is safe to run home-only at EXEC: only a
/// command whose keys are ALL home-owned may queue (and later EXEC correctly home-only); any
/// key on a remote shard means home-only EXEC would silently lose the write, so the caller
/// rejects + dirties the transaction instead.
///
/// It reuses the SAME key-extraction the router uses ([`route::single_key`] for the single-key
/// fast path, [`route::command_keys`] for multi-key), so "which bytes are keys" cannot drift
/// from the routing decision. A command with NO extractable key (a malformed / short request,
/// `KeySpec::None`) has no remote key, so it is treated as home-owned: it queues and the home
/// handler emits the proper runtime error as the EXEC array element (matching Redis, where a
/// queued command's argument error surfaces at run time, not queue time).
fn all_keys_home_owned(cmd_upper: &[u8], request: &Request, home: ShardId) -> bool {
    let is_home = |key: &[u8]| route::owner_shard(key, home.total) == home.index;
    match route::classify(cmd_upper) {
        route::CommandClass::KeyedSingle => route::single_key(request).is_none_or(is_home),
        route::CommandClass::KeyedMulti => match route::command_keys(cmd_upper, request) {
            route::KeySpec::None => true,
            route::KeySpec::One(k) => is_home(k),
            route::KeySpec::Many(keys) => keys.iter().all(|k| is_home(k)),
        },
        // Only keyed commands reach this helper (the caller gates on `keyed`); a control /
        // whole-keyspace command has no owned key, so treat it as home (it never routes
        // remotely from inside MULTI anyway).
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => true,
    }
}

/// Whether `cmd_upper` is one of the SERVE-LAYER pub/sub commands intercepted by
/// [`try_handle_pubsub`] (SERVER_PUSH.md #20): SUBSCRIBE / UNSUBSCRIBE / PSUBSCRIBE /
/// PUNSUBSCRIBE / PUBLISH / PUBSUB. These are handled in the serve layer (not `dispatch_inner`),
/// so EXEC cannot replay them; the in-MULTI reject (FIX C) uses this to decide which commands to
/// reject + dirty inside a transaction. PING is NOT in this set (a subscribed PING is handled by
/// `try_handle_pubsub` but PING is a normal command that DOES reach `dispatch_inner`, so it
/// queues + replays at EXEC like any other command).
fn is_serve_pubsub_command(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"SUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"PUBLISH"
            | b"PUBSUB"
            // Sharded Pub/Sub (#410): SSUBSCRIBE / SUNSUBSCRIBE / SPUBLISH are serve-routed too.
            | b"SSUBSCRIBE"
            | b"SUNSUBSCRIBE"
            | b"SPUBLISH"
    )
}

/// Whether `cmd_upper` is one of the SIX multi-key DATA commands the coordinator fans out
/// across shards when its keys SPAN shards (COORDINATOR.md #107, Stage 2a): MGET, MSET,
/// DEL, EXISTS, UNLINK, TOUCH. Every OTHER spanning multi-key command (SINTER*/SUNION*/
/// SDIFF*/ZUNION*/ZINTER*/ZDIFF*/BITOP/PFCOUNT/PFMERGE spanning, RENAME/RENAMENX/COPY/MOVE/
/// SMOVE/LMOVE/RPOPLPUSH) is DEFERRED (Stage 2b/2c) and stays on the home sync fall-through;
/// MSETNX is DEFERRED to Stage 3 (it needs cross-shard atomicity), so it is NOT here. This
/// list is the single gate the serve loop and [`crate::multikey::fan_out_multikey`]'s match
/// agree on.
fn is_fan_out_multikey(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"MGET" | b"MSET" | b"DEL" | b"EXISTS" | b"UNLINK" | b"TOUCH"
    )
}

/// Reply the standard unknown-command error for a CLIENT-issued INTERNAL verb (the
/// coordinator's `__ICSTORESET`, COORDINATOR.md #107 Stage 2b). The verb is in the command
/// registry so the coordinator's internal path can dispatch it, but a client must never reach
/// it: this renders the SAME `-ERR unknown command ...` reply the dispatch `_ =>` arm renders
/// for a genuinely unknown token (name + leading args, single-quoted), and bumps
/// commands_processed like every other reply path so the rejection still counts.
fn reject_internal_verb(
    conn: &ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    state_rc.borrow_mut().counters.on_command();
    let name = String::from_utf8_lossy(request.command()).into_owned();
    let rest: Vec<&[u8]> = request.args[1..].iter().map(bytes::Bytes::as_ref).collect();
    encode_into(
        out,
        &ironcache_server::Value::error(ironcache_protocol::ErrorReply::unknown_command(
            &name, &rest,
        )),
        conn.proto,
    );
}

// -- Pub/Sub serve-layer handlers (SERVER_PUSH.md #20, PR 91a). These live in the SERVE layer
// (not `dispatch_inner`) because registration needs the per-connection push SENDER (`push_tx`,
// a tokio handle the server crate has no dependency for) and the per-shard subscription table
// (`shard_pubsub()`, a serve thread-local). SUBSCRIBE/UNSUBSCRIBE are HOME-LOCAL (the
// connection's subscriptions live on its home shard); PUBLISH fans out via the coordinator.

/// Intercept and handle the SERVE-LAYER pub/sub commands (SERVER_PUSH.md #20, PR 91a/91b),
/// returning `Some(close)` when `cmd_upper` is one of them (always `false`: a pub/sub command
/// never closes the connection) and `None` when it is NOT a pub/sub command (the caller falls
/// through to the normal routing + dispatch). Split out of [`route_and_dispatch`] so the router
/// stays small.
///
/// `commands_processed` is bumped here for every handled command (matching every other reply
/// path's single count). SUBSCRIBE / PSUBSCRIBE / PUBLISH validate arity inline (the registry
/// arity, mirroring the dispatch arity path); UNSUBSCRIBE / PUNSUBSCRIBE accept zero args
/// (unsubscribe-all); PUBSUB validates its subcommand inline. PING is intercepted ONLY when the
/// connection is a RESP2 subscriber (the `["pong", ...]` array shape); a non-subscriber / RESP3
/// PING returns `None` so the normal `cmd_ping` arm handles it unchanged.
// `too_many_lines` allowed: this is the pub/sub command DISPATCH (one arm per SUBSCRIBE /
// UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE / PUBLISH / PUBSUB / the #410 sharded trio / subscribed
// PING), each a thin arity-check + handler call. Splitting it would scatter the single
// pub/sub interception point that mirrors the serve router's one entry.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn try_handle_pubsub(
    conn: &mut ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    request: &Request,
    out: &mut Vec<u8>,
) -> Option<bool> {
    match cmd_upper {
        b"SUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            // Arity (>= 2) is the registry's; a bare SUBSCRIBE with no channel is a wrong-arity
            // error, mirroring the dispatch arity path for the other serve-routed commands.
            if request.args.len() >= 2 {
                handle_subscribe(conn, push_tx, shed_flag, request, out);
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "subscribe",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        b"UNSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            handle_unsubscribe(conn, request, out);
            Some(false)
        }
        b"PSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            // Arity (>= 2) is the registry's; a bare PSUBSCRIBE with no pattern is a
            // wrong-arity error, mirroring SUBSCRIBE's inline arity path.
            if request.args.len() >= 2 {
                handle_psubscribe(conn, push_tx, shed_flag, request, out);
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "psubscribe",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        b"PUNSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            handle_punsubscribe(conn, request, out);
            Some(false)
        }
        b"PUBSUB" => {
            state_rc.borrow_mut().counters.on_command();
            // PUBSUB <subcommand> [args]: a cross-shard introspection GATHER (CHANNELS /
            // NUMSUB / NUMPAT). Like PUBLISH it lives in the serve layer (it reads the
            // per-shard subscription tables) and fans out via the coordinator's inbox.
            handle_pubsub(conn, inbox, home, request, out).await;
            Some(false)
        }
        b"PUBLISH" => {
            state_rc.borrow_mut().counters.on_command();
            if request.args.len() == 3 {
                handle_publish(conn, inbox, home, request, out).await;
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "publish",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        // -- Sharded Pub/Sub (#410): the SSUBSCRIBE / SUNSUBSCRIBE / SPUBLISH analogs of
        // SUBSCRIBE / UNSUBSCRIBE / PUBLISH, over the separate SHARD-channel namespace. --
        b"SSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            if request.args.len() >= 2 {
                handle_ssubscribe(conn, push_tx, shed_flag, request, out);
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "ssubscribe",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        b"SUNSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            handle_sunsubscribe(conn, request, out);
            Some(false)
        }
        b"SPUBLISH" => {
            state_rc.borrow_mut().counters.on_command();
            if request.args.len() == 3 {
                handle_spublish(conn, inbox, home, request, out).await;
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "spublish",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        b"PING" if conn.is_subscriber() && conn.proto == ProtoVersion::Resp2 => {
            // PING while subscribed (RESP2): the `["pong", <arg>]` array shape, NOT `+PONG`. Bump
            // commands_processed like the dispatch path would, then encode the array. PING arity
            // is Min(1); a >2-arg PING is a wrong-arity error (Redis), matching `cmd_ping`.
            state_rc.borrow_mut().counters.on_command();
            let reply = if request.args.len() <= 2 {
                ping_subscribed_reply(request)
            } else {
                ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity("ping"))
            };
            encode_into(out, &reply, conn.proto);
            Some(false)
        }
        _ => None,
    }
}

/// Append a `["subscribe", channel, count]` confirmation (one per channel; SERVER_PUSH.md). It
/// is rendered through [`ironcache_protocol::Value::Push`], so the encoder writes RESP3 `>` /
/// RESP2 `*` from the connection proto (ADR-0019), matching Redis's subscribe-confirmation shape.
fn push_confirm(kind: &str, channel: &[u8], count: i64) -> ironcache_server::Value {
    ironcache_server::Value::Push(vec![
        ironcache_server::Value::bulk_str(kind),
        ironcache_server::Value::bulk(bytes::Bytes::copy_from_slice(channel)),
        ironcache_server::Value::Integer(count),
    ])
}

/// The running subscription count for a connection (`channels + patterns`), the integer in
/// each subscribe/unsubscribe confirmation (Redis reports the TOTAL of both, post-mutation).
fn running_count(conn: &ConnState) -> i64 {
    i64::try_from(conn.sub_channels.len() + conn.sub_patterns.len()).unwrap_or(i64::MAX)
}

/// `SUBSCRIBE channel [channel ...]` (SERVER_PUSH.md #20, PR 91a). For EACH channel: insert it
/// into `conn.sub_channels` and register `(channel, conn.id, push_tx.clone())` into THIS shard's
/// subscription table, then append a `["subscribe", channel, running_count]` confirmation. The
/// running count is `sub_channels.len() + sub_patterns.len()` AFTER the insert; a re-subscribe to
/// an already-subscribed channel does NOT bump the count (the `HashSet`/table inserts are
/// idempotent), matching Redis. One confirmation message per channel argument, in order.
fn handle_subscribe(
    conn: &mut ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let pubsub = shard_pubsub();
    for channel in &request.args[1..] {
        conn.sub_channels.insert(channel.clone());
        pubsub.borrow_mut().subscribe(
            channel.clone(),
            conn.id,
            crate::pubsub::Subscriber {
                sender: push_tx.clone(),
                shed: std::sync::Arc::clone(shed_flag),
            },
        );
        let count = running_count(conn);
        encode_into(
            out,
            &push_confirm("subscribe", channel.as_ref(), count),
            conn.proto,
        );
    }
}

/// `UNSUBSCRIBE [channel ...]` (SERVER_PUSH.md #20, PR 91a). With channel args, unsubscribe each
/// named channel; with NO args, unsubscribe ALL currently-subscribed channels. Reply one
/// `["unsubscribe", channel, running_count]` per AFFECTED channel; the no-args-and-none-subscribed
/// edge replies a single `["unsubscribe", nil, 0]` (matching Redis). Deregister each from THIS
/// shard's subscription table (the connection's subscriptions are home-shard-local).
fn handle_unsubscribe(conn: &mut ConnState, request: &Request, out: &mut Vec<u8>) {
    let pubsub = shard_pubsub();
    // The channels to drop: the named args, or ALL currently-subscribed when none are named.
    let targets: Vec<bytes::Bytes> = if request.args.len() > 1 {
        request.args[1..].to_vec()
    } else {
        conn.sub_channels.iter().cloned().collect()
    };

    if targets.is_empty() {
        // No args AND nothing subscribed: Redis replies a single nil-channel confirmation.
        encode_into(
            out,
            &ironcache_server::Value::Push(vec![
                ironcache_server::Value::bulk_str("unsubscribe"),
                ironcache_server::Value::Null,
                ironcache_server::Value::Integer(0),
            ]),
            conn.proto,
        );
        return;
    }

    for channel in targets {
        conn.sub_channels.remove(&channel);
        pubsub.borrow_mut().unsubscribe(channel.as_ref(), conn.id);
        let count = running_count(conn);
        encode_into(
            out,
            &push_confirm("unsubscribe", channel.as_ref(), count),
            conn.proto,
        );
    }
}

/// The running SHARD-channel subscription count (#410), the integer in each
/// ssubscribe/sunsubscribe confirmation. Redis reports the SHARD-channel count ONLY here (NOT
/// the channels+patterns total `running_count` uses), so a client tracks its sharded
/// subscriptions independently of its regular ones.
fn running_shard_count(conn: &ConnState) -> i64 {
    i64::try_from(conn.sub_shard_channels.len()).unwrap_or(i64::MAX)
}

/// `SSUBSCRIBE shardchannel [shardchannel ...]` (#410, the sharded analog of SUBSCRIBE). For EACH
/// channel: insert it into `conn.sub_shard_channels` and register the subscriber into THIS shard's
/// `shard_channels` table, then append a `["ssubscribe", channel, running_shard_count]`
/// confirmation. Idempotent (a re-subscribe does not bump the count), matching Redis. The
/// SHARD-channel namespace is separate from SUBSCRIBE's, so an SPUBLISH (not a PUBLISH) delivers
/// here.
fn handle_ssubscribe(
    conn: &mut ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let pubsub = shard_pubsub();
    for channel in &request.args[1..] {
        conn.sub_shard_channels.insert(channel.clone());
        pubsub.borrow_mut().subscribe_shard(
            channel.clone(),
            conn.id,
            crate::pubsub::Subscriber {
                sender: push_tx.clone(),
                shed: std::sync::Arc::clone(shed_flag),
            },
        );
        let count = running_shard_count(conn);
        encode_into(
            out,
            &push_confirm("ssubscribe", channel.as_ref(), count),
            conn.proto,
        );
    }
}

/// `SUNSUBSCRIBE [shardchannel ...]` (#410, the sharded analog of UNSUBSCRIBE). With args,
/// unsubscribe each named shard channel; with NO args, unsubscribe ALL currently-held shard
/// channels. Reply one `["sunsubscribe", channel, running_shard_count]` per affected channel; the
/// no-args-and-none-subscribed edge replies a single `["sunsubscribe", nil, 0]` (matching Redis).
fn handle_sunsubscribe(conn: &mut ConnState, request: &Request, out: &mut Vec<u8>) {
    let pubsub = shard_pubsub();
    let targets: Vec<bytes::Bytes> = if request.args.len() > 1 {
        request.args[1..].to_vec()
    } else {
        conn.sub_shard_channels.iter().cloned().collect()
    };

    if targets.is_empty() {
        encode_into(
            out,
            &ironcache_server::Value::Push(vec![
                ironcache_server::Value::bulk_str("sunsubscribe"),
                ironcache_server::Value::Null,
                ironcache_server::Value::Integer(0),
            ]),
            conn.proto,
        );
        return;
    }

    for channel in targets {
        conn.sub_shard_channels.remove(&channel);
        pubsub
            .borrow_mut()
            .unsubscribe_shard(channel.as_ref(), conn.id);
        let count = running_shard_count(conn);
        encode_into(
            out,
            &push_confirm("sunsubscribe", channel.as_ref(), count),
            conn.proto,
        );
    }
}

/// `SPUBLISH shardchannel message` (#410, the sharded analog of PUBLISH). Fan the message out to
/// every shard's LOCAL `shard_channels` table via the coordinator (node-local; an SPUBLISH never
/// reaches a SUBSCRIBE subscriber), replying the integer total receiver count.
async fn handle_spublish(
    conn: &ConnState,
    inbox: &coordinator::Inbox,
    home: ShardId,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let channel = request.args[1].clone();
    let payload = request.args[2].clone();
    let total = coordinator::fan_out_spublish(
        inbox,
        channel.as_ref(),
        payload.as_ref(),
        conn.db,
        home.index,
    )
    .await;
    encode_into(out, &ironcache_server::Value::Integer(total), conn.proto);
}

/// `PSUBSCRIBE pattern [pattern ...]` (SERVER_PUSH.md #20, PR 91b). For EACH pattern: insert it
/// into `conn.sub_patterns` and register `(pattern, conn.id, push_tx.clone())` into THIS shard's
/// subscription `patterns` table, then append a `["psubscribe", pattern, running_count]`
/// confirmation. The running count is `sub_channels.len() + sub_patterns.len()` AFTER the insert
/// (the TOTAL of channels + patterns, exactly as SUBSCRIBE); a re-subscribe to an already-held
/// pattern does NOT bump the count (the `HashSet` / table inserts are idempotent), matching
/// Redis. One confirmation message per pattern argument, in order.
fn handle_psubscribe(
    conn: &mut ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let pubsub = shard_pubsub();
    for pattern in &request.args[1..] {
        conn.sub_patterns.insert(pattern.clone());
        pubsub.borrow_mut().subscribe_pattern(
            pattern.clone(),
            conn.id,
            crate::pubsub::Subscriber {
                sender: push_tx.clone(),
                shed: std::sync::Arc::clone(shed_flag),
            },
        );
        let count = running_count(conn);
        encode_into(
            out,
            &push_confirm("psubscribe", pattern.as_ref(), count),
            conn.proto,
        );
    }
}

/// `PUNSUBSCRIBE [pattern ...]` (SERVER_PUSH.md #20, PR 91b). With pattern args, unsubscribe each
/// named pattern; with NO args, unsubscribe ALL currently-subscribed patterns. Reply one
/// `["punsubscribe", pattern, running_count]` per AFFECTED pattern; the no-args-and-none-subscribed
/// edge replies a single `["punsubscribe", nil, 0]` (matching Redis). Deregister each from THIS
/// shard's subscription `patterns` table (the connection's subscriptions are home-shard-local).
fn handle_punsubscribe(conn: &mut ConnState, request: &Request, out: &mut Vec<u8>) {
    let pubsub = shard_pubsub();
    // The patterns to drop: the named args, or ALL currently-subscribed when none are named.
    let targets: Vec<bytes::Bytes> = if request.args.len() > 1 {
        request.args[1..].to_vec()
    } else {
        conn.sub_patterns.iter().cloned().collect()
    };

    if targets.is_empty() {
        // No args AND nothing subscribed: Redis replies a single nil-pattern confirmation.
        encode_into(
            out,
            &ironcache_server::Value::Push(vec![
                ironcache_server::Value::bulk_str("punsubscribe"),
                ironcache_server::Value::Null,
                ironcache_server::Value::Integer(0),
            ]),
            conn.proto,
        );
        return;
    }

    for pattern in targets {
        conn.sub_patterns.remove(&pattern);
        pubsub
            .borrow_mut()
            .unsubscribe_pattern(pattern.as_ref(), conn.id);
        let count = running_count(conn);
        encode_into(
            out,
            &push_confirm("punsubscribe", pattern.as_ref(), count),
            conn.proto,
        );
    }
}

/// `PUBSUB CHANNELS [pattern] | NUMSUB [ch ...] | NUMPAT` (SERVER_PUSH.md #20, PR 91b) -- the
/// cross-shard introspection GATHER. Subscription state is PER-SHARD (a channel may have
/// subscribers on several shards), so each subcommand fans the SAME internal `__ICPUBSUB <sub>
/// [args]` request out to EVERY shard via [`coordinator::fan_out_pubsub`] (the home shard runs
/// it locally, peers via their drain loops) and MERGES the per-shard partials per subcommand:
/// CHANNELS unions+dedups the channel names, NUMSUB sums the per-channel counts, NUMPAT unions
/// the pattern names and counts the DISTINCT total. `commands_processed` was already bumped by
/// the caller.
///
/// Per-subcommand ARITY is validated here, byte-exact to Redis `pubsubCommand` (verified against
/// redis/redis src/pubsub.c): CHANNELS accepts `argc == 2 || argc == 3` (at MOST one pattern arg,
/// FIX H), NUMPAT accepts EXACTLY `argc == 2` (NO args, FIX H), NUMSUB accepts `argc >= 2` (any
/// number of channels). A bare `PUBSUB` (no subcommand) is a WRONG-ARITY error (FIX G; the
/// registry arity is min-2, and Redis returns wrong-arity for a missing subcommand). Every other
/// invalid case -- an unknown subcommand, OR a known subcommand with the wrong arg count -- is the
/// Redis `addReplySubcommandSyntaxError` (our [`ErrorReply::unknown_subcommand`], byte-identical:
/// `ERR unknown subcommand or wrong number of arguments for '<sub>'. Try PUBSUB HELP.`).
async fn handle_pubsub(
    conn: &ConnState,
    inbox: &coordinator::Inbox,
    home: ShardId,
    request: &Request,
    out: &mut Vec<u8>,
) {
    // FIX G: a bare `PUBSUB` (no subcommand) is WRONG-ARITY (not unknown-subcommand). The registry
    // arity is min-2; Redis rejects a missing subcommand with the wrong-arity error.
    let Some(sub_raw) = request.args.get(1) else {
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity("pubsub")),
            conn.proto,
        );
        return;
    };
    let sub_upper = ascii_upper(sub_raw.as_ref());
    let argc = request.args.len();
    // Each known subcommand carries its own arg-count rule (FIX H), byte-exact to Redis
    // pubsubCommand. A present-but-unrecognized subcommand OR a recognized subcommand with a bad
    // arg count both fall to the same subcommand-syntax error (Redis's addReplySubcommandSyntaxError).
    let valid = match sub_upper.as_slice() {
        // CHANNELS [pattern] / the sharded SHARDCHANNELS [pattern] (#410): at most one pattern
        // -> argc 2 or 3.
        b"CHANNELS" | b"SHARDCHANNELS" => argc == 2 || argc == 3,
        // NUMSUB [channel ...] / the sharded SHARDNUMSUB [channel ...] (#410): any number of
        // channels -> argc >= 2 (no upper bound).
        b"NUMSUB" | b"SHARDNUMSUB" => argc >= 2,
        // NUMPAT: takes NO args -> argc exactly 2.
        b"NUMPAT" => argc == 2,
        _ => false,
    };
    if !valid {
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::unknown_subcommand(
                "pubsub",
                &String::from_utf8_lossy(sub_raw.as_ref()),
            )),
            conn.proto,
        );
        return;
    }
    let merged = coordinator::fan_out_pubsub(inbox, request, home.index).await;
    encode_into(out, &merged, conn.proto);
}

/// `PUBLISH channel payload` (SERVER_PUSH.md #20, PR 91a) -> the total number of receivers across
/// ALL shards. Classic Pub/Sub channels are not slotted, so delivery FANS OUT to every shard's
/// local subscriber table via [`coordinator::fan_out_publish`] (the home shard delivers locally,
/// peers via their drain loops), summing the per-shard counts. Encodes a [`Value::Integer`].
async fn handle_publish(
    conn: &ConnState,
    inbox: &coordinator::Inbox,
    home: ShardId,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let channel = request.args[1].clone();
    let payload = request.args[2].clone();
    let total = coordinator::fan_out_publish(
        inbox,
        channel.as_ref(),
        payload.as_ref(),
        conn.db,
        home.index,
    )
    .await;
    encode_into(out, &ironcache_server::Value::Integer(total), conn.proto);
}

/// The PING reply for a connection in SUBSCRIBE mode under RESP2 (SERVER_PUSH.md #20, PR 91a).
/// Redis replies a 2-element ARRAY `["pong", ""]` (or `["pong", <arg>]`) rather than the usual
/// `+PONG` simple string while subscribed, so a client multiplexing pushes and replies can tell
/// the PONG apart from a pushed message. RESP3 and non-subscriber PING are unchanged (handled by
/// the normal `cmd_ping` dispatch arm). The reply is a plain `Array` (NOT a push frame): Redis
/// sends it as a normal multi-bulk reply.
fn ping_subscribed_reply(request: &Request) -> ironcache_server::Value {
    let second = request
        .args
        .get(1)
        .map_or_else(|| bytes::Bytes::from_static(b""), bytes::Bytes::clone);
    ironcache_server::Value::Array(Some(vec![
        ironcache_server::Value::bulk_str("pong"),
        ironcache_server::Value::bulk(second),
    ]))
}

/// Deregister EVERY subscription a connection holds from THIS shard's subscription table
/// (SERVER_PUSH.md #20, PR 91a), driven off `conn.sub_channels` / `conn.sub_patterns` /
/// `conn.sub_shard_channels` (O(subs)). Called on connection close (and could be reused on RESET):
/// the connection's subscriptions are home-shard-local, so this runs on the connection's home
/// shard. A no-op when not subscribed.
fn deregister_all_subscriptions(conn: &ConnState) {
    // CLIENT TRACKING (#409): purge this connection from the per-shard tracking table on close, so a
    // later write never pushes an invalidation to a gone connection. A no-op (no alloc) when no
    // tracking client ever used this shard; runs regardless of the pub/sub state below.
    purge_conn_tracking(conn.id);

    if conn.sub_channels.is_empty()
        && conn.sub_patterns.is_empty()
        && conn.sub_shard_channels.is_empty()
    {
        return;
    }
    let pubsub = shard_pubsub();
    let mut table = pubsub.borrow_mut();
    for channel in &conn.sub_channels {
        table.unsubscribe(channel.as_ref(), conn.id);
    }
    for pattern in &conn.sub_patterns {
        // PSUBSCRIBE pattern subscriptions (PR 91b): deregister each from this shard's
        // `patterns` table so a QUIT / error close / peer close leaves no pattern leak.
        table.unsubscribe_pattern(pattern.as_ref(), conn.id);
    }
    for channel in &conn.sub_shard_channels {
        // SSUBSCRIBE shard subscriptions (#410): deregister each from this shard's
        // `shard_channels` table so a close leaves no shard-channel leak.
        table.unsubscribe_shard(channel.as_ref(), conn.id);
    }
}

/// Purge a connection from this shard's CLIENT TRACKING table (#409): `CLIENT TRACKING OFF` /
/// RESET / disconnect. Accesses the table only if it EXISTS (no tracking client ever -> no-op, no
/// allocation), so the common no-tracking close path is one thread-local borrow + `is_none`.
fn purge_conn_tracking(conn_id: u64) {
    TRACKING.with(|cell| {
        if let Some(t) = cell.borrow().as_ref() {
            t.borrow_mut().forget_conn(conn_id);
        }
    });
}

/// Whether `cmd_upper` is one of the set-algebra OR sorted-set-algebra commands the
/// coordinator GATHERS + (shared) COMBINES + STOREs across shards when its keys SPAN shards
/// (COORDINATOR.md #107, Stage 2b-1 + 2b-2). This is the single gate the serve loop uses to
/// route to the spanning-combine path; [`is_fan_out_spanning_zset`] then splits the zset
/// subset (dispatched to [`crate::spanning_combine::fan_out_zset`]) from the set subset
/// (dispatched to [`crate::spanning_combine::fan_out_set`]).
///
/// Set forms (Stage 2b-1): SINTER, SUNION, SDIFF, SINTERCARD (read) + SINTERSTORE,
/// SUNIONSTORE, SDIFFSTORE (store). Zset forms (Stage 2b-2): ZUNION, ZINTER, ZDIFF,
/// ZINTERCARD (read) + ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE (store) + ZRANGESTORE (a 2-key
/// copy-range). BITOP + HyperLogLog forms (Stage 2b-3): BITOP (write), PFCOUNT (read),
/// PFMERGE (write). Every OTHER spanning multi-key command (RENAME/COPY/MOVE/SMOVE/LMOVE/
/// RPOPLPUSH) stays on the home sync fall-through (deferred). The command set is DISJOINT
/// from [`is_fan_out_multikey`]'s, so the fan-out branches are mutually exclusive.
fn is_fan_out_spanning_combine(cmd_upper: &[u8]) -> bool {
    is_fan_out_spanning_zset(cmd_upper)
        || matches!(
            cmd_upper,
            b"SINTER"
                | b"SUNION"
                | b"SDIFF"
                | b"SINTERCARD"
                | b"SINTERSTORE"
                | b"SUNIONSTORE"
                | b"SDIFFSTORE"
                | b"BITOP"
                | b"PFCOUNT"
                | b"PFMERGE"
        )
}

/// Whether `cmd_upper` is one of the EIGHT sorted-set-algebra commands the coordinator gathers,
/// (shared) combines, and stores across shards (COORDINATOR.md #107, Stage 2b-2). The read
/// forms are ZUNION, ZINTER, ZDIFF, ZINTERCARD; the store forms are ZUNIONSTORE, ZINTERSTORE,
/// ZDIFFSTORE, and ZRANGESTORE (a 2-key copy-range). This splits the zset subset of
/// [`is_fan_out_spanning_combine`] so the serve loop dispatches it to
/// [`crate::spanning_combine::fan_out_zset`] (the set subset goes to `fan_out_set`).
fn is_fan_out_spanning_zset(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"ZUNION"
            | b"ZINTER"
            | b"ZDIFF"
            | b"ZINTERCARD"
            | b"ZUNIONSTORE"
            | b"ZINTERSTORE"
            | b"ZDIFFSTORE"
            | b"ZRANGESTORE"
    )
}

/// Whether `cmd_upper` is one of the THREE element-move commands the coordinator applies
/// ATOMICALLY across the two owner shards when its keys span shards (COORDINATOR.md #107, the
/// PROD-9 cross-shard atomicity slice): SMOVE (set member move), LMOVE / RPOPLPUSH (list
/// element move). The serve loop dispatches a spanning invocation of these to
/// [`crate::spanning_move::fan_out_spanning_move`] (the gather-validate-then-commit), ending
/// the prior SILENT home-subset partial-apply. A co-located invocation routes via Stage 1
/// (the single-shard handler), unchanged. The set is DISJOINT from [`is_fan_out_multikey`] /
/// [`is_fan_out_spanning_combine`] / [`is_spanning_move_reject`], so the branches are mutually
/// exclusive.
fn is_fan_out_spanning_move(cmd_upper: &[u8]) -> bool {
    matches!(cmd_upper, b"SMOVE" | b"LMOVE" | b"RPOPLPUSH")
}

/// Whether `cmd_upper` is a spanning multi-key command this slice REJECTS LOUDLY (rather than
/// silently home-subset partial-apply) when its keys span internal shards (COORDINATOR.md
/// #107). These need more than a two-hop element move: RENAME / RENAMENX / COPY transfer an
/// ARBITRARY-typed value object intact (no cross-shard serialize/restore primitive exists
/// yet -- `Keyspace::move_object` is same-shard only by design); LMPOP / ZMPOP are
/// first-non-empty multi-key pops; SORT ... STORE writes a sorted projection. A spanning
/// invocation is rejected with a clear, descriptive error naming the co-location (hash-tag)
/// remedy (see [`reject_spanning_move`]), the SAME "correct, or explicitly aborted, never
/// silently wrong" contract the cross-shard MULTI/EXEC + WATCH guards follow. NOTE: SORT is
/// only rejected when it carries a STORE dest on a DIFFERENT owner than the source (the gate
/// caller checks `owner_shard_set == None`, which a SORT without STORE -- one key -- never
/// triggers). The set is DISJOINT from the fan-out predicates.
///
/// MSETEX (#412) joins this set: it is an ATOMIC all-or-nothing multi-key set (the NX/XX gate
/// must see every key before any write, and a shared TTL applies to all), so a naive per-shard
/// fan-out (like MSET) would break the gate's atomicity. A spanning MSETEX is therefore
/// rejected loudly rather than partial-applied; the cross-shard atomic MSETEX (the gather-then
/// -conditional-fan-out that `spanning_msetnx` does for MSETNX) is a deferred follow-up. On a
/// single-node deployment (every key home-owned) MSETEX never spans, so this never fires there.
fn is_spanning_move_reject(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"RENAME" | b"RENAMENX" | b"COPY" | b"LMPOP" | b"ZMPOP" | b"SORT" | b"MSETEX"
    )
}

/// REJECT a SHARD-SPANNING invocation of a multi-key command this slice cannot apply
/// atomically (RENAME/RENAMENX/COPY/LMPOP/ZMPOP/SORT...STORE), encoding a clear LOUD error
/// rather than letting it fall through to the home shard and SILENTLY operate on only the
/// home subset (the cardinal safety bug). Bumps `commands_processed` like every reply path.
/// The error names the co-location (hash-tag) remedy so a client can make the command
/// single-shard. This is a plain `ERR` (not `-CROSSSLOT`): IronCache presents as a SINGLE
/// NODE, matching the existing cross-shard MULTI/EXEC + WATCH guards' deliberate choice
/// ([`ironcache_protocol::ErrorReply::txn_cross_shard_command`] et al). With `shards == 1`
/// every key is home-owned, so this never fires (byte-identical single-shard parity).
fn reject_spanning_move(
    conn: &ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    out: &mut Vec<u8>,
) {
    state_rc.borrow_mut().counters.on_command();
    let name = String::from_utf8_lossy(cmd_upper).into_owned();
    encode_into(
        out,
        &ironcache_server::Value::error(ironcache_protocol::ErrorReply::err(format!(
            "{name} across internal shards is not supported yet; \
             use a hash tag so the keys co-locate on one shard \
             (e.g. {{tag}}key1 {{tag}}key2)"
        ))),
        conn.proto,
    );
}

/// ASCII-uppercase the command token for routing classification (RESP command tokens are
/// ASCII; mirrors the dispatcher's own case-insensitive token handling). The classified
/// token is used ONLY to pick a route; dispatch re-uppercases its own copy. `pub(crate)`
/// so the [`crate::coordinator`] drain loop classifies the same way (keyed vs whole-keyspace).
pub(crate) fn ascii_upper(b: &[u8]) -> Vec<u8> {
    b.iter().map(u8::to_ascii_uppercase).collect()
}

/// Wait for a shutdown signal (SIGINT/SIGTERM) and then stop the shard set.
///
/// Signal handling lives in the binary only (CLI_BINARY.md): the library crates
/// never touch raw signals, preserving the determinism boundary. We use a small
/// blocking wait on a self-pipe-free `libc::sigwait`-style loop via tokio's signal
/// support on the main thread.
pub fn install_shutdown(set: &ShardSet) -> Arc<std::sync::atomic::AtomicBool> {
    set.shutdown_flag()
}

/// Block the calling (main) thread until a termination signal (SIGINT/SIGTERM) arrives, then flip
/// `flag` so the shard accept loops + the save-on-exit watch begin the GRACEFUL stop (#139,
/// SHUTDOWN.md): the FIRST signal initiates the graceful shutdown (drain + save-on-exit, driven by
/// the shard executors), and a SECOND signal arriving while that stop is in progress ESCALATES to an
/// IMMEDIATE `exit(0)` so an operator can always force the issue (a stuck drain or a slow exit save
/// can never trap the process). The signal handler itself does NOT terminate from inside the handler
/// (Redis-faithful: a stop signal becomes a controlled shutdown, not an abrupt in-handler exit
/// [redis-sigterm-sigint-graceful-shutdown]); it only records the request via `flag`, and the second
/// signal's force-exit is the deliberate IronCache escalation.
///
/// Uses tokio's signal handling on a small dedicated current-thread runtime (signal handling lives
/// in the binary only, CLI_BINARY.md, so the determinism boundary holds).
pub fn wait_for_signal(flag: &Arc<std::sync::atomic::AtomicBool>) {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    rt.block_on(async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let Ok(mut sigint) = signal(SignalKind::interrupt()) else {
                return;
            };
            let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
                return;
            };
            // FIRST signal: initiate the graceful stop (the caller drives the drain + join next).
            tokio::select! {
                _ = sigint.recv() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    });
    // Record the stop request so the shards drain + the save-on-exit watch fires.
    flag.store(true, Ordering::SeqCst);

    // SECOND-SIGNAL FORCE (#139, SHUTDOWN.md): arm a DEDICATED long-lived watcher thread for the
    // ESCALATION. It must outlive this function (the graceful join the caller runs next can take up
    // to the drain grace window), so it owns its OWN current-thread runtime on its OWN OS thread
    // rather than a task on the short-lived runtime above (which is dropped when this fn returns). A
    // second SIGINT/SIGTERM arriving while the graceful stop is in progress forces an immediate
    // `exit(0)` so an operator can always force the issue. On unix only (the signal surface); a
    // build-failure to install the watcher is non-fatal (the graceful path still completes).
    #[cfg(unix)]
    {
        let _ = std::thread::Builder::new()
            .name("ironcache-force-stop".to_string())
            .spawn(|| {
                let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                rt.block_on(async {
                    use tokio::signal::unix::{SignalKind, signal};
                    let (Ok(mut sigint), Ok(mut sigterm)) = (
                        signal(SignalKind::interrupt()),
                        signal(SignalKind::terminate()),
                    ) else {
                        return;
                    };
                    tokio::select! {
                        _ = sigint.recv() => {}
                        _ = sigterm.recv() => {}
                    }
                    tracing::warn!("ironcache: second stop signal -> forcing immediate exit");
                    std::process::exit(0);
                });
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_env::{Clock, Env, Rng};
    use ironcache_storage::{ExpireWrite, NewValue, Store};

    /// The per-shard handles the timer-task tests drive (the same Rc<RefCell<..>> set
    /// `spawn_expire_task` / `expire_cycle_tick` consume).
    type TimerFixtures = (
        Rc<RefCell<SystemEnv>>,
        Rc<RefCell<ShardStoreImpl>>,
        Rc<RefCell<TimingWheel>>,
        Rc<RefCell<ShardState>>,
    );

    /// Build a fresh per-shard store + wheel + env + state for the timer-task tests, but
    /// independent of the shard thread-locals so a test can plant entries directly.
    fn timer_fixtures() -> TimerFixtures {
        let env = Rc::new(RefCell::new(SystemEnv::new()));
        let store = Rc::new(RefCell::new(ShardStore::with_hooks(
            16,
            Policy::cache_default(),
            CountingAccounting::new(),
        )));
        let wheel = Rc::new(RefCell::new(TimingWheel::new()));
        let state = Rc::new(RefCell::new(ShardState {
            next_client_id: 1,
            counters: ShardCounters::new(),
            command_stats: ironcache_observe::CommandStats::default(),
            last_policy_generation: 0,
        }));
        (env, store, wheel, state)
    }

    /// Plant a key with a deadline already in the PAST relative to the real wall clock
    /// (deadline 1ms after the Unix epoch), and register it in the wheel, so the next
    /// active-expiry cycle finds it due regardless of the precise SystemEnv `now`.
    fn plant_expired(
        store: &Rc<RefCell<ShardStoreImpl>>,
        wheel: &Rc<RefCell<TimingWheel>>,
        key: &[u8],
    ) {
        let deadline = UnixMillis(1);
        // now=0 so the upsert itself does not lazily reap it before the cycle runs.
        store.borrow_mut().upsert(
            0,
            key,
            NewValue::Bytes(b"v"),
            ExpireWrite::Set(deadline),
            UnixMillis(0),
        );
        wheel.borrow_mut().register(0, key, deadline);
    }

    #[test]
    fn expire_cycle_tick_reaps_expired_and_bumps_counter() {
        // The background cycle FUNCTION (driven directly, deterministically): a key whose
        // deadline is in the past is reaped and folded into the shard's expired_keys
        // counter, with NO command issued (the idle-shard boundedness guarantee).
        let (env, store, wheel, state) = timer_fixtures();
        // Establish the wheel origin in the past so the elapsed-to-now walk retires the
        // entry (the first advance only sets the base).
        wheel.borrow_mut().advance(UnixMillis(0), 0);
        plant_expired(&store, &wheel, b"k1");
        plant_expired(&store, &wheel, b"k2");
        assert_eq!(store.borrow().len(), 2);

        let rt_cfg =
            ironcache_config::RuntimeConfig::from_config(&ironcache_config::Config::default());
        let reaped = expire_cycle_tick(&env, &store, &wheel, &state, &rt_cfg);
        assert_eq!(
            reaped, 2,
            "the cycle reaped both expired keys with no command"
        );
        assert_eq!(store.borrow().len(), 0, "resident memory bounded when idle");
        assert_eq!(
            state.borrow().counters.snapshot().expired_keys,
            2,
            "the cycle folds reclamation into the shard expired_keys counter"
        );
    }

    #[test]
    fn expire_cycle_tick_is_inert_when_active_expire_disabled() {
        // DEBUG SET-ACTIVE-EXPIRE 0 (#411): with the runtime active-expire flag off, the
        // background reaper does NOTHING (the expired keys stay resident for inspection); the
        // SAME fixture reaps them once the flag is re-enabled.
        let (env, store, wheel, state) = timer_fixtures();
        wheel.borrow_mut().advance(UnixMillis(0), 0);
        plant_expired(&store, &wheel, b"k1");
        plant_expired(&store, &wheel, b"k2");
        let rt_cfg =
            ironcache_config::RuntimeConfig::from_config(&ironcache_config::Config::default());
        rt_cfg.set_active_expire(false);
        assert_eq!(
            expire_cycle_tick(&env, &store, &wheel, &state, &rt_cfg),
            0,
            "active-expire disabled -> the cycle reaps nothing"
        );
        assert_eq!(
            store.borrow().len(),
            2,
            "expired keys stay resident when disabled"
        );
        // Re-enable -> the same cycle now reaps them.
        rt_cfg.set_active_expire(true);
        assert_eq!(expire_cycle_tick(&env, &store, &wheel, &state, &rt_cfg), 2);
        assert_eq!(store.borrow().len(), 0);
    }

    #[test]
    fn expire_cycle_tick_is_a_noop_when_nothing_due() {
        // A cycle with nothing due reaps nothing and leaves the counter untouched (the
        // common idle case: an empty wheel fast-forwards in O(1)).
        let (env, store, wheel, state) = timer_fixtures();
        wheel.borrow_mut().advance(UnixMillis(0), 0);
        let rt_cfg =
            ironcache_config::RuntimeConfig::from_config(&ironcache_config::Config::default());
        let reaped = expire_cycle_tick(&env, &store, &wheel, &state, &rt_cfg);
        assert_eq!(reaped, 0);
        assert_eq!(state.borrow().counters.snapshot().expired_keys, 0);
    }

    #[test]
    fn spawn_expire_task_drains_an_idle_shard_via_the_timer_seam() {
        // Wiring smoke for the SPAWNED async task: run it on a current-thread LocalSet
        // (as a shard does), plant an expired key, and assert the timer task reclaims it
        // with NO command ever issued. This exercises spawn_on_shard + Runtime::timer +
        // the borrow discipline (a held RefCell borrow across the await would panic here
        // because the test thread reborrows the same cells between ticks).
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let (env, store, wheel, state) = timer_fixtures();
            wheel.borrow_mut().advance(UnixMillis(0), 0);
            plant_expired(&store, &wheel, b"idle");
            assert_eq!(store.borrow().len(), 1);

            let runtime = TokioRuntime::new();
            let rt_cfg =
                ironcache_config::RuntimeConfig::from_config(&ironcache_config::Config::default());
            // EXPIRE_TASK_SPAWNED is thread-local; this test thread spawns exactly once.
            spawn_expire_task(
                runtime,
                Rc::clone(&env),
                Rc::clone(&store),
                Rc::clone(&wheel),
                Rc::clone(&state),
                rt_cfg,
            );

            // Drive the LocalSet: the timer task awaits EXPIRE_CYCLE_INTERVAL (100ms) then
            // drains. Yield-sleep past a BOUNDED number of cycles (no wall-clock deadline,
            // so this stays off std::time per the determinism lint). While we sleep we ALSO
            // reborrow the shared cells (as a command handler would), proving the task does
            // not hold a borrow across its await.
            for _ in 0..40 {
                tokio::time::sleep(EXPIRE_CYCLE_INTERVAL).await;
                // Reborrow the cells between the task's awaits: would panic if the task
                // held a borrow across .await.
                if store.borrow().is_empty() {
                    break;
                }
            }
            assert!(
                store.borrow().is_empty(),
                "the background timer task reclaimed the idle shard's expired key"
            );
            assert!(
                state.borrow().counters.snapshot().expired_keys >= 1,
                "idle reclamation folded into expired_keys"
            );
        });
    }

    #[test]
    fn shard_env_rng_is_reachable_as_wired() {
        // Regression for the determinism seam: the shard hands out an
        // owned-mutable env handle (Rc<RefCell<SystemEnv>>), so BOTH halves of the
        // seam are reachable. A bare Rc<SystemEnv> would make `.rng()` (which needs
        // `&mut self`) structurally uncallable. Prove the RNG path works through
        // the borrow, as the per-connection code is wired.
        let env = shard_env();
        // Clock half: reachable via shared borrow.
        let _ = env.borrow().now();
        // RNG half: reachable via mutable borrow. Two draws differ (the stream
        // advances), confirming we hold a live, mutable RNG and not a no-op.
        let mut handle = env.borrow_mut();
        let a = handle.rng().next_u64();
        let b = handle.rng().next_u64();
        assert_ne!(a, b, "RNG stream did not advance through the env handle");
    }

    #[test]
    fn dbsize_flush_do_not_advance_rng_only_randomkey_does() {
        // FIX 3 (deterministic regression guard): the whole-keyspace fan-out's RNG-draw
        // decision -- the EXACT gate `route_and_dispatch` uses -- must draw the home Env
        // RNG ONLY for RANDOMKEY. Drawing for DBSIZE / FLUSHALL / FLUSHDB (all arity-1)
        // would advance the per-shard SplitMix64 stream that RANDOMKEY / SPOP / *-random
        // eviction read from, breaking ADR-0003 replay AND the shards == 1 byte-identical
        // parity (the home path draws 0 for these). We snapshot the thread-local RNG by
        // CLONING it before and after each gate evaluation: if a non-RANDOMKEY command did
        // not draw, the two clones are at the SAME state, so their next draw matches.
        use ironcache_server::Request;

        // The gate, lifted verbatim from `route_and_dispatch` (kept in sync by review): a
        // non-RANDOMKEY whole-keyspace command must yield 0 WITHOUT touching the RNG.
        fn gate_pick(cmd_upper: &[u8], request: &Request) -> u64 {
            if cmd_upper == b"RANDOMKEY" {
                crate::whole_keyspace::randomkey_pick(request)
            } else {
                0
            }
        }

        fn req(parts: &[&[u8]]) -> Request {
            Request {
                args: parts
                    .iter()
                    .map(|p| bytes::Bytes::copy_from_slice(p))
                    .collect(),
            }
        }

        let env = shard_env();

        // Snapshot = a CLONE of the live RNG state (cloning does NOT advance the real
        // stream). Two snapshots taken with NO draw between them are at the same state, so
        // their next draw matches; a draw in between makes the post-snapshot's next draw
        // differ (the stream advanced).
        let snapshot = |env: &Rc<RefCell<SystemEnv>>| -> ironcache_env::SplitMix64 {
            env.borrow_mut().rng().clone()
        };

        // Non-RANDOMKEY arity-1 whole-keyspace commands must NOT draw: the stream stays put.
        for cmd in [b"DBSIZE".as_slice(), b"FLUSHALL", b"FLUSHDB"] {
            let mut before = snapshot(&env);
            let pick = gate_pick(cmd, &req(&[cmd]));
            assert_eq!(
                pick,
                0,
                "{} must yield pick 0",
                String::from_utf8_lossy(cmd)
            );
            let mut after = snapshot(&env);
            assert_eq!(
                before.next_u64(),
                after.next_u64(),
                "{} must NOT advance the RNG stream (FIX 3)",
                String::from_utf8_lossy(cmd)
            );
        }

        // RANDOMKEY (arity 1) MUST draw: the live stream advances, so a snapshot before vs
        // after the gate is at a DIFFERENT state (their next draws differ).
        let mut before = snapshot(&env);
        let _ = gate_pick(b"RANDOMKEY", &req(&[b"RANDOMKEY"]));
        let mut after = snapshot(&env);
        assert_ne!(
            before.next_u64(),
            after.next_u64(),
            "RANDOMKEY MUST advance the RNG stream (the draw the gate exists to gate)"
        );
    }

    /// The cluster node id is DRAWN ONLY FROM THE ENV SEAM (ADR-0003), so the same seed
    /// yields the same 40-hex id every time: `node_id_hex` is pure over `&mut impl Rng`.
    /// This pins the determinism contract (CLUSTER_CONTRACT.md #70) without touching the OS.
    #[test]
    fn node_id_hex_is_deterministic_for_a_seed() {
        const SEED: u64 = 0xC0FF_EE12_3456_789A;
        let mut a = ironcache_env::TestEnv::new(SEED);
        let mut b = ironcache_env::TestEnv::new(SEED);
        let id_a = node_id_hex(a.rng());
        let id_b = node_id_hex(b.rng());
        // Same seed -> identical id (the determinism invariant).
        assert_eq!(id_a, id_b, "same seed must yield the same node id");
        // Shape: exactly 40 lowercase-hex chars, matching the Redis node-id width.
        assert_eq!(id_a.len(), 40, "node id must be 40 hex chars: {id_a:?}");
        assert!(
            id_a.bytes()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "node id must be lowercase hex: {id_a:?}"
        );
        // A DIFFERENT seed yields a different id (the draw actually uses the stream).
        let mut c = ironcache_env::TestEnv::new(SEED ^ 0x1);
        assert_ne!(
            id_a,
            node_id_hex(c.rng()),
            "a different seed should yield a different id"
        );
    }

    // ----- cluster_redirect / moved_if_unowned (CLUSTER_CONTRACT.md #70, slice 2) -----
    //
    // `cluster_redirect` is PURE over (map, route, cmd, request), so it is tested directly
    // without a socket. The fixture is a TWO-node map: node A (self) owns the LOW half
    // [0, 8191], node B owns the HIGH half [8192, 16383]. A key whose `key_slot` is in the
    // high half is therefore foreign (-> MOVED), one in the low half is owned (-> None).

    const RID_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const RID_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    /// A two-node SlotMap with `self` = node A (low half), node B advertised on
    /// `10.0.0.2:7002` (the MOVED target).
    fn redirect_map() -> ironcache_cluster::SlotMap {
        ironcache_cluster::SlotMap::build(
            vec![
                (
                    ironcache_cluster::NodeEntry {
                        id: RID_A.into(),
                        host: "10.0.0.1".into(),
                        port: 7001,
                    },
                    vec![[0, 8191]],
                ),
                (
                    ironcache_cluster::NodeEntry {
                        id: RID_B.into(),
                        host: "10.0.0.2".into(),
                        port: 7002,
                    },
                    vec![[8192, 16383]],
                ),
            ],
            RID_A,
        )
        .expect("a two-way split is valid")
    }

    #[test]
    fn rebalance_apply_cmds_arms_migrating_and_importing_pairs_capped() {
        // Node A owns everything, node B is empty: the plan moves ~half of A's slots to B.
        let map = ironcache_cluster::SlotMap::build(
            vec![
                (
                    ironcache_cluster::NodeEntry {
                        id: RID_A.into(),
                        host: "10.0.0.1".into(),
                        port: 7001,
                    },
                    vec![[0, 16383]],
                ),
                (
                    ironcache_cluster::NodeEntry {
                        id: RID_B.into(),
                        host: "10.0.0.2".into(),
                        port: 7002,
                    },
                    vec![],
                ),
            ],
            RID_A,
        )
        .unwrap();

        // Cap of 3 moves -> 6 cmds, each a MIGRATING(dest=B) then IMPORTING(src=A, dest=B) pair.
        let cmds = super::rebalance_apply_cmds(&map, 3);
        assert_eq!(cmds.len(), 6, "3 moves, capped, is 6 config cmds");
        for pair in cmds.chunks(2) {
            match (&pair[0], &pair[1]) {
                (
                    ironcache_raft::ConfigCmd::SetSlotMigrating { slot: ms, dest },
                    ironcache_raft::ConfigCmd::SetSlotImporting {
                        slot: is,
                        src,
                        dest: idest,
                    },
                ) => {
                    assert_eq!(ms, is, "the MIGRATING + IMPORTING are for the same slot");
                    assert_eq!(dest, RID_B, "MIGRATING toward B");
                    assert_eq!(src, RID_A, "IMPORTING from A");
                    assert_eq!(idest, RID_B, "IMPORTING onto B");
                }
                other => panic!("expected a MIGRATING+IMPORTING pair, got {other:?}"),
            }
        }

        // Re-running skips a slot already MIGRATING (idempotent progress): arm one, re-plan.
        let armed = match &cmds[0] {
            ironcache_raft::ConfigCmd::SetSlotMigrating { slot, .. } => *slot,
            _ => unreachable!("cmds[0] is a MIGRATING"),
        };
        map.set_migrating(armed, RID_B).unwrap();
        let after = super::rebalance_apply_cmds(&map, 3);
        assert!(
            after.iter().all(|c| !matches!(
                c,
                ironcache_raft::ConfigCmd::SetSlotMigrating { slot, .. } if *slot == armed
            )),
            "an already-migrating slot is not re-armed"
        );
    }

    #[test]
    fn rebalance_apply_cmds_of_a_balanced_map_is_empty() {
        // A balanced two-way split (8192 / 8192) proposes no moves, so no cmds are armed.
        assert!(super::rebalance_apply_cmds(&redirect_map(), 128).is_empty());
    }

    fn rreq(parts: &[&[u8]]) -> Request {
        Request {
            args: parts
                .iter()
                .map(|p| bytes::Bytes::copy_from_slice(p))
                .collect(),
        }
    }

    /// `CLIENT UNPAUSE` (case-insensitive, exactly 2 args) is the pause-recovery command the pause
    /// stall must never hold; everything else (incl. other CLIENT subcommands and a malformed UNPAUSE
    /// with extra args) is NOT exempt and is gated normally.
    #[test]
    fn client_unpause_is_recognized_for_the_pause_exemption() {
        assert!(request_is_client_unpause(&rreq(&[b"CLIENT", b"UNPAUSE"])));
        assert!(request_is_client_unpause(&rreq(&[b"client", b"unpause"])));
        assert!(request_is_client_unpause(&rreq(&[b"Client", b"UnPause"])));
        // NOT an exempt UNPAUSE: other subcommands, PAUSE itself, a bare CLIENT, or trailing args.
        assert!(!request_is_client_unpause(&rreq(&[
            b"CLIENT", b"PAUSE", b"100"
        ])));
        assert!(!request_is_client_unpause(&rreq(&[
            b"CLIENT", b"KILL", b"ID", b"1"
        ])));
        assert!(!request_is_client_unpause(&rreq(&[b"CLIENT"])));
        assert!(!request_is_client_unpause(&rreq(&[b"GET", b"UNPAUSE"])));
        assert!(!request_is_client_unpause(&rreq(&[
            b"CLIENT", b"UNPAUSE", b"extra"
        ])));
    }

    /// Find a short key whose `key_slot` is in `[lo, hi]` (the slot space is dense).
    fn key_in_slot_range(lo: u16, hi: u16) -> String {
        for i in 0..100_000u32 {
            let k = format!("k{i}");
            let s = ironcache_protocol::key_slot(k.as_bytes());
            if s >= lo && s <= hi {
                return k;
            }
        }
        panic!("no key in [{lo}, {hi}]");
    }

    #[test]
    fn redirect_owned_single_key_proceeds() {
        let map = redirect_map();
        let key = key_in_slot_range(0, 8191); // owned by self (node A)
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        assert_eq!(
            cluster_redirect(&map, route, b"GET", &req, false, false, None, None),
            None,
            "an owned single-key command proceeds (no redirect)"
        );
    }

    // ----- WRITE-SIDE replication guardrail (ADR-0026, min-replicas-to-write) -----

    /// The PURE quorum decision: reject `-NOREPLICAS` only when the in-sync count is BELOW the
    /// required minimum; otherwise allow. This is the count-compare heart of `write_guardrail`.
    #[test]
    fn write_guardrail_decision_rejects_below_quorum() {
        // min_replicas_to_write = 0 (disabled) ALWAYS allows, regardless of the count -- this is
        // the byte-unchanged default (the hot-path caller never even reaches here at 0).
        assert_eq!(write_guardrail_decision(0, 0), None);
        assert_eq!(write_guardrail_decision(0, 5), None);

        // min = 1: 0 in-sync replicas -> NOREPLICAS; 1 (or more) in sync -> allow.
        let reply = write_guardrail_decision(1, 0).expect("0 in-sync < 1 required -> reject");
        assert_eq!(
            reply.line(),
            "-NOREPLICAS Not enough good replicas to write."
        );
        assert_eq!(write_guardrail_decision(1, 1), None);
        assert_eq!(write_guardrail_decision(1, 2), None);

        // min = 2: 1 in sync is still below quorum (reject); 2 meets it (allow).
        assert!(write_guardrail_decision(2, 1).is_some());
        assert_eq!(write_guardrail_decision(2, 2), None);
    }

    /// The FULL `write_guardrail`: a WRITE to an OWNED slot below quorum is `-NOREPLICAS`; the
    /// same write WITH the quorum met is allowed; a READ is NEVER blocked even below quorum; a
    /// keyless/admin command is exempt. Drives the real function with a constructed context.
    #[test]
    fn write_guardrail_blocks_owned_writes_only() {
        let key = key_in_slot_range(0, 8191); // owned by self (node A) in the redirect map.

        // A context with the guardrail enabled (min_replicas_to_write = 1) and ZERO in-sync
        // replicas: an owned WRITE must be rejected.
        let ctx_no_replica = guardrail_ctx(1, 0);
        let set_req = rreq(&[b"SET", key.as_bytes(), b"v"]);
        let reply = write_guardrail(&ctx_no_replica, route::classify(b"SET"), b"SET", &set_req)
            .expect("an owned write with 0 in-sync replicas is rejected");
        assert_eq!(
            reply.line(),
            "-NOREPLICAS Not enough good replicas to write."
        );

        // A READ is never blocked, even with 0 in-sync replicas.
        let get_req = rreq(&[b"GET", key.as_bytes()]);
        assert_eq!(
            write_guardrail(&ctx_no_replica, route::classify(b"GET"), b"GET", &get_req),
            None,
            "a read is never blocked by the write-side guardrail"
        );

        // A keyless / admin write (PING is AlwaysHome) carries no slot -> exempt.
        let ping_req = rreq(&[b"PING"]);
        assert_eq!(
            write_guardrail(
                &ctx_no_replica,
                route::classify(b"PING"),
                b"PING",
                &ping_req
            ),
            None,
            "a keyless command is exempt (no replicated slot)"
        );

        // With ONE in-sync replica, the SAME owned write is allowed (quorum met).
        let ctx_one_replica = guardrail_ctx(1, 1);
        assert_eq!(
            write_guardrail(&ctx_one_replica, route::classify(b"SET"), b"SET", &set_req),
            None,
            "an owned write with the quorum met proceeds"
        );
    }

    /// Build a minimal raft-mode `ServerContext` for the guardrail tests: the write-side knobs set
    /// to `min_required`, the in-sync count cell seeded to `count`, and a cluster map where self
    /// owns the low half (so a low-half key is OWNED). Only the fields the guardrail reads matter.
    fn guardrail_ctx(min_required: u32, count: usize) -> ServerContext {
        use std::sync::Arc;
        let in_sync = Arc::new(ironcache_server::InSyncReplicas::new());
        for _ in 0..count {
            in_sync.set_replica_in_sync(false, true);
        }
        let boot = ironcache_config::Config {
            cluster_enabled: true,
            cluster_mode: ironcache_config::ClusterMode::Raft,
            min_replicas_to_write: min_required,
            min_replicas_max_lag: 10,
            ..ironcache_config::Config::default()
        };
        ServerContext {
            runtime: ironcache_config::RuntimeConfig::from_config(&boot),
            acl: ironcache_server::AclState::from_requirepass(boot.requirepass.as_deref()),
            databases: boot.databases,
            shards: 1,
            info: ServerInfo {
                tcp_port: 7001,
                shards: 1,
                pid: 1,
                started_at: ironcache_env::Monotonic::ZERO,
                maxmemory: 0,
                maxmemory_policy: "allkeys-lru",
                mem_allocator: "test",
                cluster_node_id: RID_A,
                cluster_enabled: true,
            },
            cluster: Some(std::sync::Arc::new(redirect_map())),
            raft: None,
            repl_status: Some(Arc::new(ironcache_server::ReplNodeStatus::new())),
            in_sync_replicas: Some(in_sync),
            repl_history_id: None,
            metrics_registry: None,
            persist_stats: None,
            process_memory: Arc::new(ironcache_observe::ProcessMemoryGauge::new()),
            conn_gate: Arc::new(ironcache_observe::ConnectionGate::new()),
            slowlog: Arc::new(ironcache_observe::SlowLog::new()),
            latency: Arc::new(ironcache_observe::LatencyMonitor::new()),
            clients: Arc::new(ironcache_observe::ClientRegistry::new()),
            hotkeys: Arc::new(ironcache_observe::Hotkeys::new()),
            boot,
        }
    }

    #[test]
    fn redirect_foreign_single_key_is_moved() {
        let map = redirect_map();
        let key = key_in_slot_range(8192, 16383); // owned by node B
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        let reply = cluster_redirect(&map, route, b"GET", &req, false, false, None, None)
            .expect("foreign key -> MOVED");
        // The MOVED carries the CLIENT-VISIBLE slot and node B's ADVERTISED host:port.
        assert_eq!(reply.line(), format!("-MOVED {slot} 10.0.0.2:7002"));
    }

    #[test]
    fn redirect_cross_slot_multi_key_is_crossslot_regardless_of_ownership() {
        let map = redirect_map();
        // Two keys in DIFFERENT slots: CROSSSLOT, even though neither/both ownership matters.
        let lo = key_in_slot_range(0, 8191);
        let hi = key_in_slot_range(8192, 16383);
        let req = rreq(&[b"MGET", lo.as_bytes(), hi.as_bytes()]);
        let route = route::classify(b"MGET");
        let reply = cluster_redirect(&map, route, b"MGET", &req, false, false, None, None)
            .expect("cross-slot -> CROSSSLOT");
        assert_eq!(
            reply.line(),
            "-CROSSSLOT Keys in request don't hash to the same slot"
        );
        // Cross-slot precedence holds even when BOTH keys are in the FOREIGN half (so the
        // command would otherwise be MOVED): CROSSSLOT still wins.
        let h1 = key_in_slot_range(8192, 16383);
        // A second high-half key in a DIFFERENT slot than h1.
        let h2 = (0..100_000u32)
            .map(|i| format!("h{i}"))
            .find(|k| {
                let s = ironcache_protocol::key_slot(k.as_bytes());
                s >= 8192 && s != ironcache_protocol::key_slot(h1.as_bytes())
            })
            .expect("a second distinct high-half slot");
        let req2 = rreq(&[b"MGET", h1.as_bytes(), h2.as_bytes()]);
        let reply2 = cluster_redirect(&map, route, b"MGET", &req2, false, false, None, None)
            .expect("still CROSSSLOT");
        assert_eq!(
            reply2.line(),
            "-CROSSSLOT Keys in request don't hash to the same slot",
            "CROSSSLOT takes precedence over MOVED even when all keys are foreign"
        );
    }

    // ----- raft-mode UNASSIGN builders (DELSLOTS / DELSLOTSRANGE / FLUSHSLOTS, slice ha-unassign) --
    //
    // build_unassign / build_flushslots are PURE over (request[, ctx]), so they are tested directly
    // without a socket: each must produce a single `UnassignSlots { slots }` ConfigCmd carrying the
    // right slot set, and the Redis-shaped error on a bad argument.

    /// Pull the slots out of a one-element `[UnassignSlots]` batch (the shape both DELSLOTS builders
    /// and FLUSHSLOTS return); panics if the batch is not exactly that, which is itself the assertion.
    fn unassign_slots(batch: Vec<ironcache_raft::ConfigCmd>) -> Vec<u16> {
        assert_eq!(batch.len(), 1, "an UNASSIGN is exactly one ConfigCmd");
        match batch.into_iter().next().unwrap() {
            ironcache_raft::ConfigCmd::UnassignSlots { slots } => slots,
            other => panic!("expected UnassignSlots, got {other:?}"),
        }
    }

    #[test]
    fn build_unassign_delslots_parses_the_slot_list() {
        // DELSLOTS <slot ...> -> UnassignSlots { the parsed slots } (the inverse of ADDSLOTS, the
        // SAME parser). The boundary slot 16383 is accepted.
        let req = rreq(&[b"CLUSTER", b"DELSLOTS", b"0", b"100", b"16383"]);
        let slots = unassign_slots(build_unassign(&req, parse_addslots_slots).expect("valid"));
        assert_eq!(slots, vec![0, 100, 16_383]);
    }

    #[test]
    fn build_unassign_delslotsrange_expands_the_ranges() {
        // DELSLOTSRANGE <start end ...> -> UnassignSlots { the inclusive-range expansion } (the
        // inverse of ADDSLOTSRANGE, the SAME parser). Two pairs expand + concatenate in order.
        let req = rreq(&[b"CLUSTER", b"DELSLOTSRANGE", b"0", b"2", b"10", b"11"]);
        let slots = unassign_slots(build_unassign(&req, parse_addslotsrange_slots).expect("valid"));
        assert_eq!(slots, vec![0, 1, 2, 10, 11]);
    }

    #[test]
    fn build_unassign_delslots_bad_slot_is_the_redis_error() {
        // A non-integer / out-of-range slot is the single Redis `Invalid or out of range slot`
        // error (mirroring ADDSLOTS), produced WITHOUT building a proposal.
        let req = rreq(&[b"CLUSTER", b"DELSLOTS", b"xyz"]);
        let err = build_unassign(&req, parse_addslots_slots).expect_err("bad slot");
        assert_eq!(err.line(), "-ERR Invalid or out of range slot");
    }

    #[test]
    fn build_unassign_delslotsrange_start_gt_end_is_the_redis_error() {
        // start > end is the Redis range error (mirroring ADDSLOTSRANGE).
        let req = rreq(&[b"CLUSTER", b"DELSLOTSRANGE", b"50", b"10"]);
        let err = build_unassign(&req, parse_addslotsrange_slots).expect_err("start > end");
        assert_eq!(
            err.line(),
            "-ERR start slot number 50 is greater than end slot number 10"
        );
    }

    #[test]
    fn build_flushslots_unassigns_exactly_the_self_owned_slots() {
        // FLUSHSLOTS -> UnassignSlots { every slot THIS node owns in the committed map }. The
        // fixture map has self (RID_A) owning the LOW half [0, 8191], so the batch is exactly those
        // 8192 slots (and NOT the high half node B owns).
        let ctx = guardrail_ctx(0, 0); // cluster == redirect_map(): self owns [0, 8191].
        let req = rreq(&[b"CLUSTER", b"FLUSHSLOTS"]);
        let slots = unassign_slots(build_flushslots(&ctx, &req).expect("valid arity"));
        assert_eq!(slots.len(), 8192, "self owns the low half (8192 slots)");
        assert_eq!(*slots.first().unwrap(), 0);
        assert_eq!(*slots.last().unwrap(), 8191);
        assert!(
            slots.iter().all(|&s| s <= 8191),
            "FLUSHSLOTS must clear ONLY the self-owned half, never node B's slots"
        );
    }

    #[test]
    fn build_flushslots_wrong_argc_is_the_subcommand_syntax_error() {
        // FLUSHSLOTS takes exactly 2 args (CLUSTER FLUSHSLOTS). An extra arg is the
        // addReplySubcommandSyntaxError class (Redis parity), produced without proposing.
        let ctx = guardrail_ctx(0, 0);
        let req = rreq(&[b"CLUSTER", b"FLUSHSLOTS", b"extra"]);
        let err = build_flushslots(&ctx, &req).expect_err("wrong argc");
        assert!(
            err.line()
                .starts_with("-ERR unknown subcommand or wrong number of arguments"),
            "unexpected error line: {:?}",
            err.line()
        );
    }

    #[test]
    fn redirect_colocated_multi_key_owned_proceeds() {
        let map = redirect_map();
        // Hash-tagged keys co-locate on ONE slot; pick a tag whose slot is owned by self.
        let tag = (0..100_000u32)
            .map(|i| format!("t{i}"))
            .find(|t| {
                let s = ironcache_protocol::key_slot(format!("{{{t}}}a").as_bytes());
                s <= 8191
            })
            .expect("a tag whose slot is owned by self");
        let k1 = format!("{{{tag}}}a");
        let k2 = format!("{{{tag}}}b");
        assert_eq!(
            ironcache_protocol::key_slot(k1.as_bytes()),
            ironcache_protocol::key_slot(k2.as_bytes()),
            "hash-tagged keys co-locate"
        );
        let req = rreq(&[b"MGET", k1.as_bytes(), k2.as_bytes()]);
        let route = route::classify(b"MGET");
        assert_eq!(
            cluster_redirect(&map, route, b"MGET", &req, false, false, None, None),
            None,
            "co-located + owned multi-key proceeds"
        );
    }

    #[test]
    fn redirect_exempts_keyless_admin_and_whole_keyspace() {
        let map = redirect_map();
        // AlwaysHome (PING / CLUSTER / MULTI) and WholeKeyspace (KEYS / SCAN) never redirect,
        // even though a foreign-slot key would otherwise be MOVED.
        for cmd in [b"PING".as_slice(), b"CLUSTER", b"MULTI", b"KEYS", b"SCAN"] {
            let req = rreq(&[cmd, b"*"]);
            let route = route::classify(cmd);
            assert_eq!(
                cluster_redirect(&map, route, cmd, &req, false, false, None, None),
                None,
                "{} must be exempt from cluster redirect",
                String::from_utf8_lossy(cmd)
            );
        }
    }

    #[test]
    fn redirect_malformed_keyed_command_falls_through() {
        let map = redirect_map();
        // A GET with NO key (arity-wrong) yields no slot: fall through so the handler emits
        // the proper wrong-arity error rather than a redirect.
        let req = rreq(&[b"GET"]);
        let route = route::classify(b"GET");
        assert_eq!(
            cluster_redirect(&map, route, b"GET", &req, false, false, None, None),
            None
        );
    }

    // ----- redirect_for_keys (the SHARED predicate WATCH uses directly over its key args) -----
    //
    // `cluster_redirect` reduces a `KeySpec` to this same iterator-based predicate, and the
    // WATCH cluster guard calls it directly with `args[1..]`. These pin the predicate over a
    // raw key sequence (the exact WATCH call shape) so WATCH and the data path provably share
    // ONE rule.

    #[test]
    fn redirect_for_keys_owned_single_proceeds() {
        let map = redirect_map();
        let key = key_in_slot_range(0, 8191); // owned by self
        assert_eq!(
            redirect_for_keys(&map, std::iter::once(key.as_bytes()), false, None, None),
            None,
            "a single owned key proceeds (this is the WATCH-of-owned-key +OK case)"
        );
    }

    #[test]
    fn redirect_for_keys_foreign_single_is_moved() {
        let map = redirect_map();
        let key = key_in_slot_range(8192, 16383); // owned by node B
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        let reply = redirect_for_keys(&map, std::iter::once(key.as_bytes()), false, None, None)
            .expect("foreign key -> MOVED (the WATCH-of-foreign-key case)");
        assert_eq!(reply.line(), format!("-MOVED {slot} 10.0.0.2:7002"));
    }

    #[test]
    fn redirect_for_keys_cross_slot_is_crossslot() {
        let map = redirect_map();
        let lo = key_in_slot_range(0, 8191);
        let hi = key_in_slot_range(8192, 16383);
        let keys = [lo.as_bytes(), hi.as_bytes()];
        let reply = redirect_for_keys(&map, keys.iter().copied(), false, None, None)
            .expect("two keys spanning slots -> CROSSSLOT (the WATCH-of-two-spanning-keys case)");
        assert_eq!(
            reply.line(),
            "-CROSSSLOT Keys in request don't hash to the same slot"
        );
    }

    #[test]
    fn redirect_for_keys_empty_is_none() {
        let map = redirect_map();
        let empty: std::iter::Empty<&[u8]> = std::iter::empty();
        assert_eq!(
            redirect_for_keys(&map, empty, false, None, None),
            None,
            "no key -> None (defensive; a well-formed WATCH always has >=1 key)"
        );
    }

    // ----- HA-7d replica-read routing (REPLICA_READ.md #147) -----
    //
    // `self` = node A owns the low half [0,8191]; node B owns [8192,16383]. We make A a REPLICA
    // of one of B's slots and assert: a READONLY read for that slot is served locally (None),
    // a WRITE is MOVED to B even under READONLY, and a non-READONLY read is MOVED to B.

    /// `redirect_map()` plus: `self` (node A) is made a replica of a single B-owned slot. Returns
    /// `(map, foreign_replicated_key)` where the key hashes to that slot.
    fn redirect_map_with_self_replica() -> (ironcache_cluster::SlotMap, String) {
        let map = redirect_map();
        let key = key_in_slot_range(8192, 16383); // B-owned
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        // A (== self, RID_A) replicates this B-owned slot.
        map.set_slot_replica(slot, RID_A)
            .expect("RID_A is a known node");
        (map, key)
    }

    #[test]
    fn cluster_failover_refuses_force_and_takeover() {
        // FORCE / TAKEOVER bypass the in-sync + committed-consensus safety gates: refuse them (#371).
        let ctx = guardrail_ctx(0, 1);
        assert!(
            build_failover(&ctx, &rreq(&[b"CLUSTER", b"FAILOVER", b"FORCE"])).is_err(),
            "FAILOVER FORCE must be refused"
        );
        assert!(
            build_failover(&ctx, &rreq(&[b"CLUSTER", b"FAILOVER", b"TAKEOVER"])).is_err(),
            "FAILOVER TAKEOVER must be refused"
        );
    }

    #[test]
    fn cluster_failover_refuses_a_node_that_is_not_an_in_sync_replica() {
        // THE DATA-SAFETY GATE: guardrail_ctx's fresh ReplNodeStatus is NOT a replica (role !=
        // Replica), so replica_read_in_sync is false and the failover is refused. A non-in-sync
        // node must never be promotable (promoting it would lose committed writes).
        let ctx = guardrail_ctx(0, 1);
        assert!(
            build_failover(&ctx, &rreq(&[b"CLUSTER", b"FAILOVER"])).is_err(),
            "a non-in-sync node must not be promotable"
        );
    }

    #[test]
    fn cluster_failover_of_an_in_sync_replica_proposes_promote_replica_for_its_slots() {
        // The positive path: an IN-SYNC replica of some slots may take them over. `set_replica_attached`
        // sets role=Replica + link up + node_offset == master_offset (lag 0, so is_in_sync is true),
        // and `redirect_map_with_self_replica` makes RID_A (self) the replica of a slot.
        let mut ctx = guardrail_ctx(0, 1);
        ctx.cluster = Some(std::sync::Arc::new(redirect_map_with_self_replica().0));
        ctx.repl_status.as_ref().unwrap().set_replica_attached(
            "127.0.0.1",
            7000,
            ironcache_repl::ReplOffset(0),
        );
        let cmds = build_failover(&ctx, &rreq(&[b"CLUSTER", b"FAILOVER"]))
            .expect("an in-sync replica may fail over");
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            ironcache_raft::ConfigCmd::PromoteReplica { slots, new_primary } => {
                assert!(!slots.is_empty(), "promotes the replicated slots");
                assert_eq!(new_primary, RID_A, "names self as the new primary");
            }
            other => panic!("expected a PromoteReplica proposal, got {other:?}"),
        }
    }

    #[test]
    fn replica_serves_readonly_read_locally() {
        let (map, key) = redirect_map_with_self_replica();
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        // READONLY (replica_serves = true via readonly=true & GET is a read): served locally.
        assert_eq!(
            cluster_redirect(&map, route, b"GET", &req, true, true, None, None),
            None,
            "a READONLY GET for a replicated slot is served locally (no MOVED)"
        );
    }

    #[test]
    fn replica_read_past_the_lag_bound_moves_to_owner() {
        // HA-8 staleness bound (REPLICA_READ.md, finishing the 7d TODO): a READONLY read for a
        // slot this node replicates is served LOCALLY only while IN SYNC. When the replica is NOT
        // in sync (link down OR lag > max_lag, surfaced as replica_in_sync = false), the SAME read
        // returns MOVED to the OWNER -- a stale replica never serves a stale read. (Contrast
        // `replica_serves_readonly_read_locally`, which passes in_sync = true and is served local.)
        let (map, key) = redirect_map_with_self_replica();
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        // READONLY but NOT in sync: the replica is too stale to serve -> MOVED to the owner (B).
        let reply = cluster_redirect(&map, route, b"GET", &req, true, false, None, None)
            .expect("a READONLY read past the lag bound MOVEDs to the owner");
        assert_eq!(reply.line(), format!("-MOVED {slot} 10.0.0.2:7002"));
    }

    #[test]
    fn replica_moves_write_even_under_readonly() {
        let (map, key) = redirect_map_with_self_replica();
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        let req = rreq(&[b"SET", key.as_bytes(), b"v"]);
        let route = route::classify(b"SET");
        // SET is a write: MOVED to the OWNER (B) even on a READONLY connection.
        let reply = cluster_redirect(&map, route, b"SET", &req, true, true, None, None)
            .expect("a write on a replica is MOVED to the owner even under READONLY");
        assert_eq!(reply.line(), format!("-MOVED {slot} 10.0.0.2:7002"));
    }

    #[test]
    fn replica_moves_non_readonly_read() {
        let (map, key) = redirect_map_with_self_replica();
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        // A non-READONLY (default) connection gets MOVED to the owner for the strong read.
        let reply = cluster_redirect(&map, route, b"GET", &req, false, false, None, None)
            .expect("a non-READONLY read of a replicated-but-not-owned slot is MOVED to the owner");
        assert_eq!(reply.line(), format!("-MOVED {slot} 10.0.0.2:7002"));
    }

    #[test]
    fn replica_read_does_not_engage_for_a_slot_this_node_does_not_replicate() {
        // A READONLY read for a B-owned slot this node does NOT replicate is still MOVED.
        let map = redirect_map(); // no replica assignment
        let key = key_in_slot_range(8192, 16383);
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        let reply = cluster_redirect(&map, route, b"GET", &req, true, true, None, None)
            .expect("READONLY does not serve a slot this node neither owns nor replicates");
        assert_eq!(reply.line(), format!("-MOVED {slot} 10.0.0.2:7002"));
    }

    // ----- HA-6 online slot migration: the ASK / ASKING / MOVED / TRYAGAIN decision table -----
    //
    // `self` = node A owns the low half [0,8191]; node B owns [8192,16383] advertised on
    // 10.0.0.2:7002. These pin the migration redirect over every case in `migration_decision`,
    // using an in-test `key_present` closure (the serve path supplies the real store resolver).

    /// SOURCE side, MIGRATING slot, the key is ABSENT locally (migrated already) -> -ASK to dest.
    #[test]
    fn migrating_source_absent_key_is_ask_to_dest() {
        let map = redirect_map();
        let key = key_in_slot_range(0, 8191); // A-owned (self is the source)
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        map.set_migrating(slot, RID_B).expect("B is known");
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        // The key is NOT present locally (migrated away / never existed).
        let key_present = |_k: &[u8]| false;
        let ctx = MigrationCtx {
            asking: false,
            key_present: &key_present,
        };
        let reply = cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx), None)
            .expect("absent key on a migrating slot -> ASK");
        // ASK carries the client-visible slot and the DEST's advertised host:port (B = 10.0.0.2:7002).
        assert_eq!(reply.line(), format!("-ASK {slot} 10.0.0.2:7002"));
    }

    /// SOURCE side, MIGRATING slot, the key IS present locally (not migrated yet) -> serve (None).
    #[test]
    fn migrating_source_present_key_is_served() {
        let map = redirect_map();
        let key = key_in_slot_range(0, 8191);
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        map.set_migrating(slot, RID_B).expect("B is known");
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        let key_present = |_k: &[u8]| true; // present locally
        let ctx = MigrationCtx {
            asking: false,
            key_present: &key_present,
        };
        assert_eq!(
            cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx), None),
            None,
            "a present key on a migrating slot is served locally"
        );
    }

    /// SOURCE side, MIGRATING slot, MULTI-KEY split (one present, one absent) -> -TRYAGAIN.
    #[test]
    fn migrating_source_mixed_multikey_is_tryagain() {
        let map = redirect_map();
        // Two co-located (hash-tagged) keys on an A-owned slot.
        let tag = (0..100_000u32)
            .map(|i| format!("t{i}"))
            .find(|t| ironcache_protocol::key_slot(format!("{{{t}}}a").as_bytes()) <= 8191)
            .expect("a self-owned tag");
        let k1 = format!("{{{tag}}}a");
        let k2 = format!("{{{tag}}}b");
        let slot = ironcache_protocol::key_slot(k1.as_bytes());
        map.set_migrating(slot, RID_B).expect("B is known");
        let req = rreq(&[b"MGET", k1.as_bytes(), k2.as_bytes()]);
        let route = route::classify(b"MGET");
        // k1 present, k2 absent -> split across the cutover.
        let k1_bytes = k1.clone();
        let key_present = move |k: &[u8]| k == k1_bytes.as_bytes();
        let ctx = MigrationCtx {
            asking: false,
            key_present: &key_present,
        };
        let reply = cluster_redirect(&map, route, b"MGET", &req, false, false, Some(&ctx), None)
            .expect("a split multi-key on a migrating slot -> TRYAGAIN");
        assert_eq!(
            reply.line(),
            "-TRYAGAIN Multiple keys request during rehashing of slot"
        );
    }

    /// DESTINATION side, IMPORTING slot, NO ASKING -> MOVED to the real owner (not served here).
    #[test]
    fn importing_dest_without_asking_is_moved_to_owner() {
        let map = redirect_map();
        // A B-owned slot that self (A) is IMPORTING (does not own yet).
        let key = key_in_slot_range(8192, 16383);
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        map.set_importing(slot, RID_B).expect("B is known");
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        let key_present = |_k: &[u8]| false;
        let ctx = MigrationCtx {
            asking: false, // NO ASKING
            key_present: &key_present,
        };
        let reply = cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx), None)
            .expect("an importing slot without ASKING -> MOVED to the owner");
        // MOVED to the OWNER (B = 10.0.0.2:7002), NOT served locally.
        assert_eq!(reply.line(), format!("-MOVED {slot} 10.0.0.2:7002"));
    }

    /// DESTINATION side, IMPORTING slot, ASKING set -> serve locally (None). The ASK second leg.
    #[test]
    fn importing_dest_with_asking_is_served() {
        let map = redirect_map();
        let key = key_in_slot_range(8192, 16383);
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        map.set_importing(slot, RID_B).expect("B is known");
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        let key_present = |_k: &[u8]| false;
        let ctx = MigrationCtx {
            asking: true, // ASKING set
            key_present: &key_present,
        };
        assert_eq!(
            cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx), None),
            None,
            "an importing slot WITH ASKING is served locally (the ASK second leg)"
        );
    }

    /// POST-FLIP: once ownership has flipped to B and the migration is cleared (the FLIP clears it
    /// in lockstep), the old owner (self) serves plain MOVED, never ASK.
    #[test]
    fn post_flip_source_serves_moved_not_ask() {
        let map = redirect_map();
        let key = key_in_slot_range(0, 8191); // was A-owned
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        // Migrate, then FLIP ownership to B (set_slot_node clears the migration in lockstep).
        map.set_migrating(slot, RID_B).expect("B is known");
        map.set_slot_node(slot, RID_B).expect("B is known");
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        // Even an ABSENT key now yields MOVED (not ASK): the slot is no longer migrating.
        let key_present = |_k: &[u8]| false;
        let ctx = MigrationCtx {
            asking: false,
            key_present: &key_present,
        };
        let reply = cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx), None)
            .expect("post-FLIP the old owner serves MOVED");
        assert_eq!(
            reply.line(),
            format!("-MOVED {slot} 10.0.0.2:7002"),
            "after the FLIP the source serves MOVED to the new owner, never ASK"
        );
    }

    // ----- HA-6 MULTI-SHARD presence exactness: the `xshard_presence_keys` routing predicate -----
    //
    // It decides WHEN the migration ASK decision needs a CROSS-SHARD presence hop (vs the
    // byte-identical local read). `home(index, total)` builds the accept shard's identity.

    fn home(index: usize, total: usize) -> ShardId {
        ShardId { index, total }
    }

    /// SINGLE-SHARD short-circuit: `home.total == 1` always returns None (every key is home-owned),
    /// so the resolver stays the pure local `contains_live` -- byte-identical to pre-fix. This is the
    /// FIRST gate, checked before the slot map or the keys are even looked at.
    #[test]
    fn xshard_presence_single_shard_is_always_none() {
        let map = redirect_map();
        let key = key_in_slot_range(0, 8191); // self-owned
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        map.set_migrating(slot, RID_B).expect("B is known"); // even a MIGRATING slot
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        assert_eq!(
            xshard_presence_keys(&map, route, b"GET", &req, home(0, 1)),
            None,
            "a single-shard node never needs a cross-shard presence hop"
        );
    }

    /// NON-MIGRATING slot: even on a multi-shard node, a slot that is NOT MIGRATING (or not owned)
    /// never consults presence, so no hop is needed -> None (local resolver, byte-unchanged).
    #[test]
    fn xshard_presence_non_migrating_slot_is_none() {
        let map = redirect_map();
        let key = key_in_slot_range(0, 8191); // self-owned, NOT migrating
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        // 8 shards: the key's FNV owner is almost surely not the accept shard we pass, but the slot
        // is not migrating, so it does not matter.
        assert_eq!(
            xshard_presence_keys(&map, route, b"GET", &req, home(0, 8)),
            None,
            "a non-migrating slot never consults presence, so no hop"
        );
    }

    /// MIGRATING slot, key on a SIBLING shard: returns Some([(key, owner)]) so the caller hops to the
    /// FNV owner shard for an EXACT presence read. This is the multi-shard case the fix targets.
    #[test]
    fn xshard_presence_migrating_sibling_key_returns_owner_hop() {
        let map = redirect_map();
        let key = key_in_slot_range(0, 8191); // self-owned slot
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        map.set_migrating(slot, RID_B).expect("B is known");
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        let total = 8;
        let owner = route::owner_shard(key.as_bytes(), total);
        // Accept on a DIFFERENT shard than the FNV owner (a sibling): the local read would be wrong.
        let accept = (owner + 1) % total;
        let got = xshard_presence_keys(&map, route, b"GET", &req, home(accept, total))
            .expect("a migrating-slot key on a sibling shard needs a presence hop");
        assert_eq!(
            got,
            vec![(key.as_bytes().to_vec(), owner)],
            "the hop targets the key's FNV owner shard"
        );
    }

    /// MIGRATING slot, key HOME-owned: returns None (the local read is exact, zero hop), even on a
    /// multi-shard node -- the cross-shard branch is taken ONLY for a non-home key.
    #[test]
    fn xshard_presence_migrating_home_key_is_none() {
        let map = redirect_map();
        let key = key_in_slot_range(0, 8191);
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        map.set_migrating(slot, RID_B).expect("B is known");
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        let total = 8;
        let owner = route::owner_shard(key.as_bytes(), total);
        // Accept ON the FNV owner shard: the key is home, so the local read is exact -> no hop.
        assert_eq!(
            xshard_presence_keys(&map, route, b"GET", &req, home(owner, total)),
            None,
            "a home-owned key uses the exact local read, no cross-shard hop"
        );
    }

    /// THE STATIC-PATH IDENTITY: with NO migration state on the map, the migration-aware redirect
    /// (Some(ctx)) is BYTE-IDENTICAL to the static redirect (None) for every owned/foreign case --
    /// the migration arms never fire when no slot is tagged, so the default path is unchanged.
    #[test]
    fn no_migration_state_is_byte_identical_to_static_redirect() {
        let map = redirect_map();
        let owned = key_in_slot_range(0, 8191);
        let foreign = key_in_slot_range(8192, 16383);
        // A present resolver + ASKING set that WOULD change a migrating/importing decision IF a slot
        // were tagged; with no tag, neither matters and both calls must agree.
        let key_present = |_k: &[u8]| true;
        let ctx_ask = MigrationCtx {
            asking: true,
            key_present: &key_present,
        };
        for (cmd, key) in [(b"GET".as_slice(), &owned), (b"GET".as_slice(), &foreign)] {
            let req = rreq(&[cmd, key.as_bytes()]);
            let route = route::classify(cmd);
            let with_mig =
                cluster_redirect(&map, route, cmd, &req, false, false, Some(&ctx_ask), None);
            let without_mig = cluster_redirect(&map, route, cmd, &req, false, false, None, None);
            assert_eq!(
                with_mig, without_mig,
                "no migration state -> Some(ctx) must equal None for key {key}"
            );
        }
    }

    // ----- HA-6 Finding 1: the one-shot ASKING is consumed EXACTLY ONCE PER COMMAND, before any
    // early return -- so a flag set by `ASKING` can never LEAK past a pubsub / in_multi / WATCH
    // early return into a later command (which would serve a key on a non-owner -> divergence). -----

    /// A fresh connection for the consume-asking unit tests.
    fn test_conn() -> ConnState {
        ConnState::new(
            1,
            ProtoVersion::Resp2,
            false,
            "10.0.0.9:5000".to_string(),
            "10.0.0.1:7001".to_string(),
        )
    }

    /// `ASKING` itself sets the flag (in the router) and must NOT consume the flag it is about to
    /// set: `consume_one_shot_asking` returns false for `ASKING` and leaves `conn.asking` untouched.
    #[test]
    fn consume_asking_does_not_clear_on_the_asking_command_itself() {
        let mut conn = test_conn();
        conn.asking = true; // a prior ASKING already set it; this command IS `ASKING`
        let was = consume_one_shot_asking(b"ASKING", &mut conn);
        assert!(!was, "ASKING does not report itself as a captured one-shot");
        assert!(
            conn.asking,
            "ASKING must NOT clear the flag it is about to (re)set"
        );
    }

    /// THE LEAK-CLOSED INVARIANT: after `ASKING`, the VERY NEXT command -- including an early-
    /// returning one (a pubsub command like SUBSCRIBE, or a no-op) -- CONSUMES the flag. So a third
    /// command can never see a stale `asking == true`. This is exactly the sequence the Finding 1
    /// hole allowed: `ASKING; SUBSCRIBE ch; GET <importing-slot key>` previously left the flag set
    /// for the GET because SUBSCRIBE returned early before the (old) consume site.
    #[test]
    fn consume_asking_clears_on_an_early_returning_command_no_leak() {
        let mut conn = test_conn();
        // ASKING set the flag (its own handler does conn.asking = true).
        conn.asking = true;
        // The NEXT command is SUBSCRIBE -- a pubsub command that EARLY-RETURNS in route_and_dispatch.
        // consume_one_shot_asking runs at the TOP, BEFORE that early return, so it consumes here.
        let captured = consume_one_shot_asking(b"SUBSCRIBE", &mut conn);
        assert!(captured, "the command right after ASKING captures asking");
        assert!(
            !conn.asking,
            "the one-shot is cleared even though SUBSCRIBE early-returns -> NO leak to the next cmd"
        );
        // The THIRD command (e.g. GET on an importing slot) now sees asking == false: it would be
        // MOVED to the owner, never wrongly served locally on this non-owner node.
        let next = consume_one_shot_asking(b"GET", &mut conn);
        assert!(
            !next,
            "a command two hops after ASKING must NOT see a leaked asking"
        );
    }

    /// A non-ASKING command with NO prior ASKING captures false and leaves the flag clear (the
    /// overwhelmingly common path: a single bool read+write, no behavioral change).
    #[test]
    fn consume_asking_is_false_without_a_prior_asking() {
        let mut conn = test_conn();
        assert!(!conn.asking);
        let captured = consume_one_shot_asking(b"GET", &mut conn);
        assert!(!captured);
        assert!(!conn.asking, "still clear");
    }

    /// RESET still clears a pending ASKING (conn.rs reset() parity), so the consume helper and RESET
    /// agree: neither lets a stale one-shot survive.
    #[test]
    fn reset_clears_a_pending_asking() {
        let mut conn = test_conn();
        conn.asking = true;
        conn.reset(false);
        assert!(!conn.asking, "RESET clears the one-shot ASKING");
    }

    // ----- HA-6 ASKING-IN-MULTI: the PRE-MULTI one-shot ASKING is carried into the transaction it
    // opens (`conn.txn_asking`) so the in-MULTI QUEUE-TIME cluster redirect honors it, and it is
    // cleared on EXEC / DISCARD / RESET so it can NEVER leak past the transaction. These pin the
    // connection-state side of the fix (the router records `txn_asking` for the opening MULTI; the
    // queue-time redirect in `route_in_multi` consults it). -----

    /// The PRE-MULTI ASKING carried into a transaction is CLEARED when the transaction ends
    /// (`clear_txn`, called by EXEC / DISCARD), so an `ASKING; MULTI; ...; EXEC` cannot leave a stale
    /// `txn_asking` for a command issued AFTER the transaction. This is the leak-fix invariant
    /// extended across the transaction boundary.
    #[test]
    fn txn_asking_is_cleared_when_the_transaction_ends() {
        let mut conn = test_conn();
        // The router records the pre-MULTI ASKING into txn_asking for the MULTI that opens the txn.
        conn.txn_asking = true;
        conn.enter_multi();
        assert!(
            conn.txn_asking,
            "enter_multi must NOT clobber the router-recorded pre-MULTI ASKING"
        );
        // EXEC / DISCARD clear the transaction (and with it txn_asking): no leak past the txn.
        conn.clear_txn();
        assert!(
            !conn.txn_asking,
            "clear_txn (EXEC/DISCARD) clears the transaction-scoped ASKING -> no leak past the txn"
        );
    }

    /// RESET inside a MULTI aborts the transaction AND clears the transaction-scoped ASKING, so a
    /// RESET cannot carry a pre-MULTI ASKING forward (the same no-leak contract as the one-shot).
    #[test]
    fn reset_clears_the_transaction_scoped_asking() {
        let mut conn = test_conn();
        conn.txn_asking = true;
        conn.enter_multi();
        conn.reset(false);
        assert!(
            !conn.txn_asking,
            "RESET clears the transaction-scoped ASKING (no carry past the aborted txn)"
        );
        assert!(!conn.in_multi, "RESET also aborts the transaction");
    }

    /// THE IN-MULTI QUEUE-TIME DECISION the wiring fix enables: the SAME `cluster_redirect` predicate
    /// `route_in_multi` now calls, over an IMPORTING slot, built with the transaction-scoped ASKING.
    /// WITH asking -> served (None, the queued command runs on the importing destination at EXEC);
    /// WITHOUT asking -> MOVED to the owner (the queued command would dirty the transaction). This is
    /// exactly the non-MULTI importing behavior, now reachable from inside a transaction.
    #[test]
    fn in_multi_importing_slot_honors_transaction_scoped_asking() {
        let map = redirect_map();
        let key = key_in_slot_range(8192, 16383); // a B-owned slot self (A) is IMPORTING
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        map.set_importing(slot, RID_B).expect("B is known");
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        let key_present = |_k: &[u8]| false;

        // txn_asking == true (the client did `ASKING; MULTI; GET k; ...`): the queued command is
        // SERVED on the importing destination (None -> proceed -> queue, run at EXEC).
        let mig_asking = MigrationCtx {
            asking: true,
            key_present: &key_present,
        };
        assert_eq!(
            cluster_redirect(
                &map,
                route,
                b"GET",
                &req,
                false,
                false,
                Some(&mig_asking),
                None
            ),
            None,
            "an in-MULTI command on an IMPORTING slot WITH the transaction-scoped ASKING is served"
        );

        // txn_asking == false (a plain `MULTI; GET k; ...`, no preceding ASKING): MOVED to the owner,
        // which the in-MULTI path turns into a dirtied transaction -> EXECABORT, exactly as today.
        let mig_no_asking = MigrationCtx {
            asking: false,
            key_present: &key_present,
        };
        let reply = cluster_redirect(
            &map,
            route,
            b"GET",
            &req,
            false,
            false,
            Some(&mig_no_asking),
            None,
        )
        .expect("an in-MULTI importing command WITHOUT ASKING is MOVED (dirties the txn)");
        assert_eq!(reply.line(), format!("-MOVED {slot} 10.0.0.2:7002"));
    }

    /// THE DEFAULT-PATH IDENTITY for the in-MULTI redirect: on a NON-migrating slot, the migration
    /// context the in-MULTI path now passes (`Some(ctx)`, with the transaction-scoped ASKING) is
    /// BYTE-IDENTICAL to the pre-fix `None`, for both an owned (proceed) and a foreign (MOVED) key --
    /// so a transaction over non-migrating slots queues / redirects EXACTLY as before HA-6.
    #[test]
    fn in_multi_non_migrating_slot_is_byte_identical_to_pre_fix() {
        let map = redirect_map();
        let owned = key_in_slot_range(0, 8191);
        let foreign = key_in_slot_range(8192, 16383);
        // An ASKING + present resolver that WOULD matter on a migrating/importing slot; with no tag
        // neither is consulted, so the migration-aware call must equal the old `None` call.
        let key_present = |_k: &[u8]| true;
        let ctx_ask = MigrationCtx {
            asking: true,
            key_present: &key_present,
        };
        for key in [&owned, &foreign] {
            let req = rreq(&[b"GET", key.as_bytes()]);
            let route = route::classify(b"GET");
            let with_mig = cluster_redirect(
                &map,
                route,
                b"GET",
                &req,
                false,
                false,
                Some(&ctx_ask),
                None,
            );
            let pre_fix = cluster_redirect(&map, route, b"GET", &req, false, false, None, None);
            assert_eq!(
                with_mig, pre_fix,
                "no migration state -> the in-MULTI Some(ctx) must equal the pre-fix None for {key}"
            );
        }
    }

    /// `is_valid_node_id` accepts EXACTLY a 40-lowercase-hex string (the announce-id / MYID /
    /// synth-id shape) and rejects everything else, so a peer that answers MEET's `CLUSTER MYID`
    /// fetch with a malformed / empty / wrong-length / uppercase id falls back to the synth id
    /// rather than committing junk into the node table.
    #[test]
    fn is_valid_node_id_accepts_only_40_lowercase_hex() {
        // The canonical shapes that MUST be accepted.
        assert!(is_valid_node_id(&"a".repeat(40)));
        assert!(is_valid_node_id("0123456789abcdef0123456789abcdef01234567"));
        assert!(is_valid_node_id(&synth_meet_node_id("127.0.0.1", 7000)));
        // Rejections: wrong length, uppercase hex, non-hex, empty.
        assert!(!is_valid_node_id(&"a".repeat(39)), "too short");
        assert!(!is_valid_node_id(&"a".repeat(41)), "too long");
        assert!(
            !is_valid_node_id(&"A".repeat(40)),
            "uppercase is not accepted"
        );
        assert!(!is_valid_node_id(&"g".repeat(40)), "non-hex letter");
        assert!(!is_valid_node_id(""), "empty");
    }

    /// MEET-with-UNREACHABLE-peer FALLBACK (item-7, the no-hang guarantee): `learn_or_synth_meet_id`
    /// dialing a CLOSED port must NOT hang -- it returns the deterministic synth id (the documented
    /// fallback) well within the test budget. We grab a free port, DROP its listener so the connect
    /// is refused, and assert the helper returns the synth id quickly. This proves a MEET to a
    /// not-yet-up peer still makes progress (commits the synth fallback) instead of blocking the
    /// serve path. The bound itself is `MEET_ID_FETCH_TIMEOUT` (read through the Runtime timer seam).
    #[test]
    fn meet_id_learn_falls_back_to_synth_on_unreachable_peer() {
        // A port that nothing is listening on (bind then immediately drop the listener).
        let dead_port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let expect_synth = synth_meet_node_id("127.0.0.1", dead_port);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let id = local.block_on(&rt, async move {
            // A wall-clock CEILING far above MEET_ID_FETCH_TIMEOUT: if the helper ever HUNG this
            // outer timeout would trip and the unwrap below would panic (a loud failure), so a PASS
            // proves the fetch is bounded. A connection-refused normally returns immediately; the
            // ceiling only guards a true hang.
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                learn_or_synth_meet_id("127.0.0.1", dead_port),
            )
            .await
            .expect("learn_or_synth_meet_id must not hang on an unreachable peer")
        });
        assert_eq!(
            id, expect_synth,
            "an unreachable-peer MEET must FALL BACK to the deterministic synth id (no hang)"
        );
    }
}
