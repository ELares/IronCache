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
    let mut cluster: Option<Arc<ironcache_cluster::SlotMap>> = if config.cluster_enabled {
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

    // Static, cheaply-cloned server context shared by value onto each shard. The
    // mutable cross-shard state is ONLY the runtime cell (an Arc); the rest is
    // immutable, so cloning per shard does not violate shared-nothing.
    let ctx_template = ServerContext {
        runtime,
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

    let set = ironcache_runtime::bootstrap::run_shards(&shard_cfg, serve, rxs, drain)?;
    Ok(BootHandles {
        set,
        raft,
        persist: persist_handle,
        runtime: runtime_handle,
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
pub(crate) fn ensure_shard_started(databases: u32, policy_name: &str, reserved_bits: u32) {
    let env = shard_env();
    let store_rc = shard_store(databases, policy_name, reserved_bits);
    let wheel_rc = shard_wheel();
    let state_rc = shard_state();
    spawn_expire_task(TokioRuntime::new(), env, store_rc, wheel_rc, state_rc);
}

fn spawn_expire_task(
    rt: TokioRuntime,
    env: Rc<RefCell<SystemEnv>>,
    store_rc: Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: Rc<RefCell<TimingWheel>>,
    state_rc: Rc<RefCell<ShardState>>,
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
            // Nothing borrowed survives to the next await iteration.
            expire_cycle_tick(&env, &store_rc, &wheel_rc, &state_rc);
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
    let wheel_rc = shard_wheel();
    // Ensure this shard's background active-expiry timer is up (PR-3c, idempotent). The
    // canonical spawn point is now SHARD BOOT (the coordinator drain loop calls
    // `ensure_shard_started` before its recv loop, COORDINATOR.md #107: a key-owning shard
    // must reclaim even with no connection). This call is the same idempotent helper, so a
    // connection arriving before the drain loop's first poll still gets the timer started;
    // the EXPIRE_TASK_SPAWNED guard makes the duplicate call a no-op.
    ensure_shard_started(ctx.databases, ctx.info.maxmemory_policy, reserved_bits);
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

    let limits = Limits::default();
    let mut read_buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);

    // The IDLE TIMEOUT (PROD-SAFETY #4): a connection that sits idle (no command) longer than
    // `timeout_secs` is CLOSED, so idle connections cannot accumulate. `0` (the Redis default)
    // DISABLES it -- the non-subscriber idle wait then stays a plain `recv` with no timer, the
    // byte-unchanged hot path. The timeout is boot-config (Redis `timeout` is settable, but
    // IronCache reads it once per connection here; a runtime change is a documented follow-up). It
    // is measured via the Runtime timer SEAM (NOT wall-clock) and the deadline RE-ARMS on each loop
    // iteration -- i.e. after each command batch is served -- which is the per-command deadline
    // reset (an active connection is never closed). A zero-sized `TokioRuntime` backend supplies the
    // timer (the shard's tasks live on the LocalSet; this carries no state). The OUTPUT-BUFFER cap
    // (PROD-SAFETY #5) is read from the runtime overlay each flush (a `CONFIG SET` takes effect).
    let idle_timeout: Option<core::time::Duration> = {
        let secs = ctx.boot.timeout_secs;
        if secs == 0 {
            None
        } else {
            Some(core::time::Duration::from_secs(secs))
        }
    };
    let timer_rt = TokioRuntime::new();

    'conn: loop {
        // Drain every complete request currently buffered (pipelining), building
        // one combined output buffer, then flush once.
        out.clear();
        loop {
            match decode(&read_buf, &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    // Route + dispatch one decoded request (COORDINATOR.md #107, Stage 1),
                    // appending its encoded reply to `out`; returns whether to close (QUIT).
                    // Factored out of the serve loop so the connection loop stays small.
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
                    )
                    .await;
                    read_buf.drain(..consumed);
                    if close {
                        // Flush the QUIT reply then close. send returns the owned
                        // buffer (owned-buffer model); we are closing, so the
                        // returned buffer is dropped rather than reclaimed. Sent over the
                        // CLIENT stream (plain or TLS); the plain arm is byte-identical to the
                        // prior `rt.send` (it calls the same TcpStream write_all), #105.
                        let _ = stream.send(std::mem::take(&mut out)).await;
                        break 'conn;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Error(e) => {
                    // Protocol error: write it and close the connection (hardening).
                    encode_into(&mut out, &ironcache_server::Value::Error(e), conn.proto);
                    let _ = stream.send(std::mem::take(&mut out)).await;
                    break 'conn;
                }
            }
        }

        // OUTPUT-BUFFER hard cap (PROD-SAFETY #5): before flushing, if the pending reply buffer has
        // grown past the configured `output_buffer_limit`, CLOSE the connection rather than let a
        // slow consumer / a huge reply / a pipelined flood drive unbounded server memory. `0`
        // disables the cap (the pre-fix unbounded behavior); the default is a high ceiling so a
        // legitimate large reply / deep pipeline is never affected. Read from the runtime overlay so
        // a `CONFIG SET output-buffer-limit` takes effect for subsequent batches. We drop the
        // oversized buffer unsent and close (matching Redis closing a client over the limit).
        let obl = ctx.runtime.output_buffer_limit();
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

        // IDLE WAIT. The NON-subscriber path (the common, hot path) is BYTE-IDENTICAL to before
        // pub/sub when no idle timeout is configured: just await `rt.recv`, no select! overhead.
        // Only a connection in SUBSCRIBE mode pays for the select! that ALSO drains the push channel
        // (`subscriber_idle_wait`). FIFO ordering holds because `out` was already flushed above
        // before we reach this idle wait, so a push is rendered and sent only AFTER the in-flight
        // command batch's reply went out -- a push never precedes a command reply on the connection
        // (SERVER_PUSH.md FIFO).
        if conn.is_subscriber() {
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

fn cluster_redirect(
    map: &ironcache_cluster::SlotMap,
    route: route::CommandClass,
    cmd_upper: &[u8],
    request: &Request,
    readonly: bool,
    replica_in_sync: bool,
    migration: Option<&MigrationCtx<'_>>,
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
        route::KeySpec::One(k) => {
            redirect_for_keys(map, std::iter::once(k), replica_serves, migration)
        }
        route::KeySpec::Many(keys) => {
            redirect_for_keys(map, keys.iter().copied(), replica_serves, migration)
        }
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
    moved_if_unowned(map, first_slot, replica_serves)
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
) -> Option<ironcache_protocol::ErrorReply> {
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

    if matches!(route, route::CommandClass::WholeKeyspace) {
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
    } else if let Some(target) = target {
        // REMOTE keyed hop: enqueue to the owning shard, await its reply, encode here. The
        // owning shard folded the data counters; here we only attribute commands_processed.
        state_rc.borrow_mut().counters.on_command();
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
        // DELSLOTS / DELSLOTSRANGE UN-assign the parsed / range-expanded slots (the inverse of
        // ADDSLOTS / ADDSLOTSRANGE; the SAME slot-parse helpers). FLUSHSLOTS UN-assigns every slot
        // THIS node owns in the committed map. Each commits an `UnassignSlots` ConfigCmd, so the
        // slots become owned by nobody on every node (cluster_slots_assigned drops by that many).
        b"DELSLOTS" => Some(build_unassign(request, parse_addslots_slots)),
        b"DELSLOTSRANGE" => Some(build_unassign(request, parse_addslotsrange_slots)),
        b"FLUSHSLOTS" => Some(build_flushslots(ctx, request)),
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
            // genuine no-leader / timeout case). Include the leader hint when known. NOTE the hint
            // is the leader's CLUSTER-BUS endpoint (host:port+10000), not its client port, so the
            // message labels it as such rather than presenting it as a dial-able client target;
            // resolving the leader's client endpoint here is a tracked follow-up.
            let msg = match handle.leader_hint() {
                Some(addr) => {
                    format!(
                        "the raft leader's cluster-bus endpoint is {addr}; retry the CLUSTER write against the leader"
                    )
                }
                None => {
                    "this node is not the raft leader; retry the CLUSTER write against the leader"
                        .to_owned()
                }
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
        let mut st = state_rc.borrow_mut();
        if deltas != CounterDeltas::default() {
            st.counters.apply(deltas);
        }
        st.last_policy_generation = shard_generation;
    }
    encode_into(out, &reply, conn.proto);
    conn.should_close
}

/// Encode `value` and append the bytes to `out`. PR-1 encodes into a fresh
/// `BytesMut` per reply and appends; pooling is a later optimization behind this
/// same call site (PROTOCOL.md notes zero-copy/pooling sit behind the interface).
fn encode_into(out: &mut Vec<u8>, value: &ironcache_server::Value, proto: ProtoVersion) {
    let mut bm = bytes::BytesMut::with_capacity(64);
    ironcache_protocol::encode(&mut bm, value, proto);
    out.extend_from_slice(&bm);
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
        b"SUBSCRIBE" | b"UNSUBSCRIBE" | b"PSUBSCRIBE" | b"PUNSUBSCRIBE" | b"PUBLISH" | b"PUBSUB"
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
#[allow(clippy::too_many_arguments)]
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
        // CHANNELS [pattern]: at most one pattern -> argc 2 or 3.
        b"CHANNELS" => argc == 2 || argc == 3,
        // NUMSUB [channel ...]: any number of channels -> argc >= 2 (no upper bound).
        b"NUMSUB" => argc >= 2,
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
/// (SERVER_PUSH.md #20, PR 91a), driven off `conn.sub_channels` / `conn.sub_patterns` (O(subs)).
/// Called on connection close (and could be reused on RESET): the connection's subscriptions are
/// home-shard-local, so this runs on the connection's home shard. A no-op when not subscribed.
fn deregister_all_subscriptions(conn: &ConnState) {
    if conn.sub_channels.is_empty() && conn.sub_patterns.is_empty() {
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

        let reaped = expire_cycle_tick(&env, &store, &wheel, &state);
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
    fn expire_cycle_tick_is_a_noop_when_nothing_due() {
        // A cycle with nothing due reaps nothing and leaves the counter untouched (the
        // common idle case: an empty wheel fast-forwards in O(1)).
        let (env, store, wheel, state) = timer_fixtures();
        wheel.borrow_mut().advance(UnixMillis(0), 0);
        let reaped = expire_cycle_tick(&env, &store, &wheel, &state);
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
            // EXPIRE_TASK_SPAWNED is thread-local; this test thread spawns exactly once.
            spawn_expire_task(
                runtime,
                Rc::clone(&env),
                Rc::clone(&store),
                Rc::clone(&wheel),
                Rc::clone(&state),
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

    fn rreq(parts: &[&[u8]]) -> Request {
        Request {
            args: parts
                .iter()
                .map(|p| bytes::Bytes::copy_from_slice(p))
                .collect(),
        }
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
            cluster_redirect(&map, route, b"GET", &req, false, false, None),
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
            metrics_registry: None,
            persist_stats: None,
            process_memory: Arc::new(ironcache_observe::ProcessMemoryGauge::new()),
            conn_gate: Arc::new(ironcache_observe::ConnectionGate::new()),
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
        let reply = cluster_redirect(&map, route, b"GET", &req, false, false, None)
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
        let reply = cluster_redirect(&map, route, b"MGET", &req, false, false, None)
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
        let reply2 = cluster_redirect(&map, route, b"MGET", &req2, false, false, None)
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
            cluster_redirect(&map, route, b"MGET", &req, false, false, None),
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
                cluster_redirect(&map, route, cmd, &req, false, false, None),
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
            cluster_redirect(&map, route, b"GET", &req, false, false, None),
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
            redirect_for_keys(&map, std::iter::once(key.as_bytes()), false, None),
            None,
            "a single owned key proceeds (this is the WATCH-of-owned-key +OK case)"
        );
    }

    #[test]
    fn redirect_for_keys_foreign_single_is_moved() {
        let map = redirect_map();
        let key = key_in_slot_range(8192, 16383); // owned by node B
        let slot = ironcache_protocol::key_slot(key.as_bytes());
        let reply = redirect_for_keys(&map, std::iter::once(key.as_bytes()), false, None)
            .expect("foreign key -> MOVED (the WATCH-of-foreign-key case)");
        assert_eq!(reply.line(), format!("-MOVED {slot} 10.0.0.2:7002"));
    }

    #[test]
    fn redirect_for_keys_cross_slot_is_crossslot() {
        let map = redirect_map();
        let lo = key_in_slot_range(0, 8191);
        let hi = key_in_slot_range(8192, 16383);
        let keys = [lo.as_bytes(), hi.as_bytes()];
        let reply = redirect_for_keys(&map, keys.iter().copied(), false, None)
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
            redirect_for_keys(&map, empty, false, None),
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
    fn replica_serves_readonly_read_locally() {
        let (map, key) = redirect_map_with_self_replica();
        let req = rreq(&[b"GET", key.as_bytes()]);
        let route = route::classify(b"GET");
        // READONLY (replica_serves = true via readonly=true & GET is a read): served locally.
        assert_eq!(
            cluster_redirect(&map, route, b"GET", &req, true, true, None),
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
        let reply = cluster_redirect(&map, route, b"GET", &req, true, false, None)
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
        let reply = cluster_redirect(&map, route, b"SET", &req, true, true, None)
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
        let reply = cluster_redirect(&map, route, b"GET", &req, false, false, None)
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
        let reply = cluster_redirect(&map, route, b"GET", &req, true, true, None)
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
        let reply = cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx))
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
            cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx)),
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
        let reply = cluster_redirect(&map, route, b"MGET", &req, false, false, Some(&ctx))
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
        let reply = cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx))
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
            cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx)),
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
        let reply = cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx))
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
            let with_mig = cluster_redirect(&map, route, cmd, &req, false, false, Some(&ctx_ask));
            let without_mig = cluster_redirect(&map, route, cmd, &req, false, false, None);
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
            cluster_redirect(&map, route, b"GET", &req, false, false, Some(&mig_asking)),
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
            let with_mig =
                cluster_redirect(&map, route, b"GET", &req, false, false, Some(&ctx_ask));
            let pre_fix = cluster_redirect(&map, route, b"GET", &req, false, false, None);
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
