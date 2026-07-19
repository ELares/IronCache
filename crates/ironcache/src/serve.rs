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
// #510: `Bytes` wraps the per-batch read buffer for zero-copy argument slicing; `Buf`
// brings `Bytes::advance` into scope for the per-frame cursor advance in the serve loops.
use bytes::{Buf, Bytes};
use ironcache_config::{Config, RuntimeConfig};
use ironcache_env::{Clock, Env, Rng, SystemEnv};
use ironcache_eviction::Policy;
use ironcache_observe::{CounterSnapshot, MemoryInfo, ServerInfo, ShardCounters};
use ironcache_runtime::bootstrap::{ShardConfig, ShardId, ShardSet};
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{
    ConnState, CounterDeltas, DecodeOutcome, Limits, ProtoVersion, Request, TimingWheel, UnixMillis,
    decode_shared, dispatch_with_cmd, route,
};
use ironcache_storage::CountingAccounting;
use ironcache_store::{ShardStore, process_memory};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;

// Cohesive sub-modules split out of this file (#625). Each is a behavior-preserving relocation of a
// self-contained group of items; the `use` re-exports below keep every call site in `serve` +
// `serve_tests` (which does `use super::*`) + `main.rs` (`serve::wait_for_signal` /
// `serve::SignalOutcome`) resolving exactly as before.
#[path = "serve_hop.rs"]
mod serve_hop;
#[path = "serve_signal.rs"]
mod serve_signal;
#[path = "serve_util.rs"]
mod serve_util;
#[path = "serve_shard_state.rs"]
mod serve_shard_state;

use serve_hop::{DeferredHop, drain_deferred_hops};
// #625: the per-shard core-local state accessors + lifecycle flags. `pub(crate)` re-export so the
// spine still in `serve` (serve loops, routing), every sibling submodule, and external callers
// (`crate::coordinator`, `crate::replica_attach`, `crate::upgrade`, INFO) resolve them unchanged.
pub(crate) use serve_shard_state::{
    STORE_SLOTS_PER_DB, TRACKING, adopt_metrics_cell, adopt_process_memory_gauge, ensure_shard_ring,
    ensure_shard_started, fresh_shard_store, install_receiver_flip_barrier, is_serving,
    is_shard_loading, quiesce_shard, report_receiver_shard_committed, scan_reserved_bits,
    set_replica_passive, set_serving, shard_blocking, shard_env, shard_pubsub, shard_started_at,
    shard_state, shard_store, shard_tracking, shard_wheel, stash_shard_ring, unquiesce_shard,
};
// These shard-state items are referenced ONLY by the shard-state unit tests in `serve_tests.rs`
// (via `use super::*`), so their re-exports/imports are `#[cfg(test)]` (mirrors the serve_signal
// test-only re-exports below): the loading/ring setters + the expiry-tick internals + the reap
// interval the expiry test drives.
#[cfg(test)]
pub(crate) use serve_shard_state::{
    expire_cycle_tick, set_shard_loading, shard_ring, spawn_expire_task,
};
#[cfg(test)]
use ironcache_server::EXPIRE_CYCLE_INTERVAL;
// #625: `encode_into` (reply-encoder shim, ~28 call sites) + `ascii_upper` (routing-token
// uppercaser, also used by `crate::coordinator` as `crate::serve::ascii_upper`). `pub(crate)` so the
// coordinator path + every sibling serve submodule resolve them unchanged.
pub(crate) use serve_util::{ascii_upper, encode_into};
// `main.rs` calls `serve::wait_for_signal` and matches `serve::SignalOutcome`; `serve_tests.rs`
// (via `use super::*`) references `resolve_signal` / `apply_signal_flag`. Re-export the whole module
// so all of those paths resolve unchanged.
pub use serve_signal::{SignalOutcome, wait_for_signal};
// `apply_signal_flag` and `resolve_signal` are referenced ONLY by the (deterministic) signal-seam
// unit tests in `serve_tests.rs` (which `use super::*`), so their re-exports are `#[cfg(test)]`;
// `resolve_signal` is additionally `#[cfg(unix)]` (the SIGUSR1 arm) like its test.
#[cfg(test)]
pub(crate) use serve_signal::apply_signal_flag;
#[cfg(all(test, unix))]
pub(crate) use serve_signal::resolve_signal;

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
    /// The cross-shard coordinator inbox senders (#556): the `/metrics` endpoint SAMPLES each
    /// shard's inbox length (via [`coordinator::inbox_depths`]) for the `ironcache_shard_inbox_depth`
    /// back-pressure gauge. Always present (an inbox is built for every boot); the metrics task only
    /// READS its depth, never sends. A cheap `Arc<[Sender]>` clone.
    pub inbox: coordinator::Inbox,
    /// The #638 slice-3 per-shard CUTOVER CONTROL senders (one per shard, in shard order): the
    /// in-server streamed live-cutover host delivers a
    /// [`CutoverStart`](crate::upgrade::cutover_coord::CutoverStart) on `[i]` to trigger shard `i`'s
    /// per-shard cutover task (its drain loop's 3rd select arm), SEPARATE from the data inbox so the
    /// trigger never queues behind cross-shard traffic. Held here so the binary's SIGUSR1 host drive
    /// can reach every shard; empty of traffic on any non-cutover boot.
    pub cutover_control:
        Vec<tokio::sync::mpsc::Sender<crate::upgrade::cutover_coord::CutoverStart>>,
    /// The hot TLS cert-reload handle (#563), `Some` only when `tls = on`. The binary installs a
    /// SIGHUP handler ([`spawn_tls_reload_on_sighup`]) over it so the configured cert/key can be
    /// re-read and atomically swapped WITHOUT a restart. `None` (TLS off / every plaintext boot)
    /// arms no handler.
    pub tls_reload: Option<TlsReloadHandle>,
}

/// The hot TLS certificate-reload handle threaded out of the boot wiring (#563): the shared
/// [`ironcache_runtime::ReloadableAcceptor`] (the ArcSwap-published client-listener config) plus the
/// SAME configured cert/key paths, so the binary can re-read those paths on SIGHUP and atomically
/// publish the fresh cert. Cloning is cheap (an `Arc` bump on the acceptor + two `String` clones).
///
/// Only the CLIENT listener is covered here (the cert that expires + rotates in practice); the
/// intra-cluster bus/repl TLS reload is a documented follow-up (docs/TLS.md "Certificate rotation").
#[derive(Clone)]
pub struct TlsReloadHandle {
    /// The shared, swappable client-listener acceptor every shard's serve closure reads from.
    pub acceptor: ironcache_runtime::ReloadableAcceptor,
    /// The configured cert-chain PEM path re-read on each reload.
    pub cert_path: String,
    /// The configured private-key PEM path re-read on each reload.
    pub key_path: String,
}

/// Boot the server like [`run_server`], but thread an optional [`MetricsRegistry`] through every
/// shard's [`ServerContext`] (so each shard ADOPTS its pre-allocated counter cell and the metrics
/// HTTP task can read the cells across threads), and return the [`BootHandles`] the binary uses to
/// stand up the `/metrics` + `/livez` + `/readyz` endpoint.
///
/// `metrics_registry` is passed as `Some` ONLY by the caller that ALSO stands up the `/metrics`
/// HTTP endpoint (it needs the SAME handle for the scrape task). When it is `None` (every test and
/// the no-flag default) this fn now BUILDS one anyway (#531): INFO's `# Stats`/`# Clients` rollup
/// must be NODE-WIDE (summed across every shard's counter cell), which requires every shard to
/// adopt a REGISTERED cell, so the registry can no longer be gated on `--metrics-addr`. ONLY the
/// HTTP endpoint stays optional; the registry is always present. It is a pre-allocated
/// `Arc<Vec<ShardCountersCell>>` (one cheap cell per shard) and adopting a cell is EXACTLY what
/// already happened with metrics ON, so the metrics-disabled hot path stays allocation/perf-neutral
/// (a shard's `ShardCounters` wrap the registered cell instead of a standalone one; the per-command
/// increments are the same relaxed atomics either way).
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
    // #531: the metrics registry is now ALWAYS present. The caller supplies `Some` only when it
    // also runs the `/metrics` endpoint (reusing the handle); otherwise build one here so every
    // shard adopts a REGISTERED counter cell and INFO's node-wide rollup (`aggregate()`) can sum
    // them. Sized to the shard count, exactly like the endpoint-enabled path. Cheap: one
    // pre-allocated cell per shard, no per-command cost (see the doc comment).
    let metrics_registry =
        metrics_registry.unwrap_or_else(|| ironcache_observe::MetricsRegistry::new(config.shards));
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

    // The per-DB store slot partition count (#570): publish the boot config value so EVERY
    // shard's store is built with it. Set here (before any shard spawns) and read once per
    // per-shard store construction in `fresh_shard_store`; the store rounds it UP to a power
    // of two. A process-global boot fact like `policy_name`, off the command hot path.
    STORE_SLOTS_PER_DB.store(config.slots_per_db as usize, Ordering::Relaxed);

    // #391 PR-5 RECEIVER SERVE GATE: a streamed-handoff RECEIVER boot must NOT serve any client
    // command until the cross-shard cutover has COMMITTED (all shards). Flip the process-global serve
    // gate to `false` HERE -- ONCE, at boot, before any shard spawns or the acceptor starts -- so the
    // NEW rejects every client command with `-LOADING` (the top-of-`route_and_dispatch` gate) until
    // the PR-4 `Committed` flip (`upgrade::commit::begin_serving_on_commit`). The condition is EXACTLY
    // PR-2's receiver gate (`handoff_role == receiver` AND a `handoff_socket`); a normal (sender /
    // no-socket) boot leaves the gate `true`, so the default datapath is BYTE-UNCHANGED and
    // `is_serving()` is never taken. The orchestrator that actually spawns the sibling in this role
    // (and keeps its acceptor closed until the flip) is PR-6; until then this branch is dormant on
    // every real deployment.
    if config.handoff_role == ironcache_config::HandoffRole::Receiver
        && config.handoff_socket.is_some()
    {
        set_serving(false);
        // #638 slice-4 RECEIVER FLIP BARRIER: install the cross-shard flip barrier sized to the shard
        // count, ONCE, before any shard spawns. Each shard, on its OWN cutover commit (adopt), reports
        // to it (`report_receiver_shard_committed`); the LAST shard to commit performs the single
        // all-or-nothing `set_serving(true)` flip -- never the FIRST shard while a sibling shard is
        // still not committed. A normal (non-receiver) boot never installs it, so the gate is absent.
        install_receiver_flip_barrier(config.shards.max(1));
    }

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

    // The process RUN ID (INFO `run_id`, #527): a fresh 40-lowercase-hex value drawn ONCE here from
    // the SAME boot RNG seam as `cluster_node_id` (ADR-0003: the only entropy is the binary's
    // `SystemEnv`, no `rand` outside ironcache-env), then leaked to `'static` and shared by value
    // into every shard's context -- so it is IDENTICAL across shards for one process. It reuses
    // `node_id_hex` because the SHAPE is the same (40 hex from 3 seam draws, Redis `getRandomHexChars`
    // parity), but it is a DISTINCT draw from `cluster_node_id`: unlike the node id (a stable identity,
    // in cluster mode the configured announce id), the run id is ALWAYS random-per-boot and identifies
    // THIS incarnation, so it CHANGES on every restart (clients + `redis_exporter` read it to detect a
    // restart). One small leak at boot, like the node id / policy name.
    let run_id: &'static str = Box::leak(node_id_hex(boot_env.rng()).into_boxed_str());

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
            // The boot-generated per-boot 40-hex run id (#527), identical across shards, NEW every
            // process boot (drawn above from the same env seam as `cluster_node_id`).
            run_id,
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
        // The per-shard metrics registry (OBSERVABILITY.md, #152). NOW ALWAYS `Some` (#531): built
        // above whether or not the `/metrics` endpoint is enabled, so every shard adopts its
        // REGISTERED counter cell and INFO's node-wide rollup (`aggregate()`) sums the whole node.
        // Moved into the template (a cheap `Arc<Vec<_>>`), then cloned per shard via
        // `ctx_template.clone()` so each shard adopts its cell by index at boot. The endpoint-
        // enabled caller keeps its own clone of the SAME registry handle for the scrape task.
        metrics_registry: Some(metrics_registry),
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
    let (inbox, data_rxs) = coordinator::build_inboxes(total);
    // The #638 slice-3 per-shard CUTOVER CONTROL channels: a DEDICATED mpsc PER shard (separate from
    // the data inbox, so a SIGUSR1-driven cutover trigger never queues behind cross-shard traffic).
    // The senders go out on `BootHandles` for the in-server cutover host to deliver on; each receiver
    // rides into its shard's drain loop, paired with the shard's data receiver.
    let (cutover_control, cutover_rxs) = coordinator::build_cutover_control(total);
    // Pair each shard's (data receiver, cutover-control receiver) by index; `run_shards` moves the
    // pair to shard `i`'s drain loop, which splits it into the two select arms.
    let rxs: Vec<(
        tokio::sync::mpsc::Receiver<coordinator::ShardWork>,
        tokio::sync::mpsc::Receiver<crate::upgrade::cutover_coord::CutoverStart>,
    )> = data_rxs.into_iter().zip(cutover_rxs).collect();
    // A clone of the inbox senders returned in `BootHandles` so the `/metrics` endpoint can SAMPLE
    // each shard's inbox occupancy for the `ironcache_shard_inbox_depth` gauge (#556). Taken here,
    // before `inbox` is cloned into the serve / drain / io_uring closures below, so it is always
    // available regardless of those moves. A cheap `Arc<[Sender]>` bump.
    let inbox_for_handles = inbox.clone();

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
    // A SECOND clone set for the RAW io_uring serve closure (#682 P2): io_uring and io_uring_raw are
    // separate cfg branches that would each `move` the captures, so the raw branch needs its own set
    // (only one backend runs; the unused set is bound to `_` in the tokio fallback below).
    let ctx_template_for_raw_uring = ctx_template.clone();
    let inbox_for_raw_uring = inbox.clone();
    let persist_for_raw_uring = persist.clone();

    // EMBEDDED TRANSPORT TLS for the CLIENT listener (#105, docs/design/TLS.md). Build the rustls
    // acceptor ONCE at boot when `tls = on`, from the configured cert/key PEM, and clone the cheap
    // (Arc-inside) handle into every connection's serve closure. When `tls = off` (the DEFAULT)
    // this is `None` and the serve path returns a PLAIN TcpStream exactly as before -- no rustls,
    // no per-byte cost, byte-unchanged. A build failure here (unreadable / unparseable cert or key)
    // is a hard boot error: a TLS-only listener with no usable material would reject every client,
    // so we refuse to start rather than silently serve nothing. `Config::validate` already proved
    // the paths are present + readable, so this is the PEM-parse + rustls-acceptance step.
    // The acceptor is held behind an ArcSwap (a `ReloadableAcceptor`, #563) so a SIGHUP-triggered
    // hot cert reload can atomically PUBLISH a freshly parsed `ServerConfig` that SUBSEQUENT
    // handshakes pick up, while in-flight connections keep theirs (rustls config is per-handshake) --
    // rotating a soon-to-expire cert needs no restart. The boot build runs the SAME PEM-parse +
    // rustls-acceptance validation as before; a failure here is still a hard boot error. `tls_reload`
    // carries the SAME shared swap cell plus the configured cert/key paths out to the binary, which
    // installs the SIGHUP handler that re-reads those paths and swaps.
    let (tls_acceptor, tls_reload): (
        Option<ironcache_runtime::ReloadableAcceptor>,
        Option<TlsReloadHandle>,
    ) = if config.tls == ironcache_config::TlsMode::On {
        // validate() guaranteed both paths are Some + readable; expressing it as a clear error here
        // keeps the boot failure precise if a future path reaches this without validation.
        let cert = config.tls_cert_path.as_ref().ok_or_else(|| {
            anyhow::anyhow!("tls = on requires tls_cert_path (should have been caught by validate)")
        })?;
        let key = config.tls_key_path.as_ref().ok_or_else(|| {
            anyhow::anyhow!("tls = on requires tls_key_path (should have been caught by validate)")
        })?;
        let cert_path = cert.to_string_lossy().into_owned();
        let key_path = key.to_string_lossy().into_owned();
        let acceptor = ironcache_runtime::ReloadableAcceptor::from_paths(&cert_path, &key_path)
            .map_err(|e| anyhow::anyhow!("building the TLS listener: {e}"))?;
        tracing::info!(
            cert = %cert.display(),
            key = %key.display(),
            "ironcache: TLS enabled (rustls, server-auth) on the client listener; SIGHUP reloads the cert"
        );
        // The reload handle shares the SAME ArcSwap (a cheap `Arc` clone), so a swap it applies is
        // seen by every shard's serve closure.
        let reload = TlsReloadHandle {
            acceptor: acceptor.clone(),
            cert_path,
            key_path,
        };
        (Some(acceptor), Some(reload))
    } else {
        (None, None)
    };

    let serve = {
        let inbox = inbox.clone();
        let persist = persist.clone();
        // `run_shards` hands the shard's `TokioRuntime` backend; the per-connection serve loop
        // drives data I/O through the `ClientStream` (plain or TLS), and the shard's background
        // timer task constructs its own zero-sized backend, so this connection path no longer
        // needs the handle directly (the underscore keeps the run_shards closure shape).
        move |_rt: TokioRuntime,
              stream: tokio::net::TcpStream,
              shard: ShardId,
              shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>| {
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
                    // The shared shutdown flag (#543): the subscribe-mode idle wait races it so a
                    // PARKED subscriber closes promptly on a graceful stop.
                    shutdown,
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
              rxs: (
            tokio::sync::mpsc::Receiver<coordinator::ShardWork>,
            tokio::sync::mpsc::Receiver<crate::upgrade::cutover_coord::CutoverStart>,
        ),
              shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>| {
            // Split the per-shard pair into the data receiver + the #638 slice-3 cutover-control
            // receiver (the drain loop's 2nd and 3rd select arms).
            let (rx, cutover_rx) = rxs;
            let ctx = drain_ctx.clone();
            let inbox = inbox.clone();
            let persist = persist.clone();
            let ready = ready.clone();
            // The shutdown flag (the SAME one the signal handler flips) lets shard 0's drain loop
            // drive the SAVE-ON-EXIT (#139) when a SIGTERM/SIGINT-triggered stop begins. `ready`
            // is the per-shard readiness countdown (#152): this shard decrements it once its
            // load-on-boot has finished, so `/readyz` flips to 200 only after EVERY shard loaded.
            coordinator::run_drain_loop(index, rx, cutover_rx, ctx, inbox, persist, shutdown, ready)
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
    let want_raw_uring = config.runtime == ironcache_config::RuntimeBackend::IoUringRaw;

    // PROBE THE RUNNING KERNEL before committing to EITHER io_uring backend (tokio-uring or the raw
    // backend, #682). The per-shard ring setup would otherwise die a shard thread on a kernel that
    // lacks io_uring / has it disabled; probing here and falling back to tokio CLEANLY means
    // `runtime = io_uring[_raw]` never fails to start a node. Both backends share the same probe +
    // the same TLS-off precondition (neither serves TLS in v1). Each `*_ok` is const-`false` unless
    // its feature is compiled in, so the matching serve branch below is dead (not compiled) on a
    // build without that feature.
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    let io_uring_ok = want_io_uring
        && config.tls != ironcache_config::TlsMode::On
        && match ironcache_runtime::uring_probe::probe_uring_caps() {
            Ok(_) => true,
            Err(e) => {
                tracing::warn!(
                    "runtime = io_uring requested, but this kernel cannot provide io_uring ({e}); \
                     falling back to the tokio backend for this node"
                );
                false
            }
        };
    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    let io_uring_ok = false;

    #[cfg(all(target_os = "linux", feature = "io_uring_raw"))]
    let raw_uring_ok = want_raw_uring
        && config.tls != ironcache_config::TlsMode::On
        && match ironcache_runtime::uring_probe::probe_uring_caps() {
            Ok(_) => true,
            Err(e) => {
                tracing::warn!(
                    "runtime = io_uring_raw requested, but this kernel cannot provide io_uring \
                     ({e}); falling back to the tokio backend for this node"
                );
                false
            }
        };
    #[cfg(not(all(target_os = "linux", feature = "io_uring_raw")))]
    let raw_uring_ok = false;

    // Assign the shard set exactly once. At most one of `io_uring_ok` / `raw_uring_ok` is true
    // (`config.runtime` is a single value). Each io_uring body is cfg-gated so a build WITHOUT that
    // feature compiles the (unreachable, provably-not-taken) fallback arm in its place.
    let set = if io_uring_ok {
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            tracing::info!(
                "runtime = io_uring: using the Linux io_uring datapath (plaintext); the \
                 registered-buffer / multishot fast path and a perf benchmark are deferred to a \
                 Linux soak (no throughput claim made)"
            );
            let uring_serve =
                {
                    let ctx_template = ctx_template_for_uring;
                    let inbox = inbox_for_uring;
                    let persist = persist_for_uring;
                    move |rt: ironcache_runtime::IoUringRuntime,
                      stream: ironcache_runtime::UringTcpStream,
                      shard: ShardId,
                      shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>| {
                    let ctx = ctx_template.clone();
                    let inbox = inbox.clone();
                    let persist = persist.clone();
                    async move {
                        serve_connection_uring(
                            rt, stream, shard, ctx, default_proto, inbox, persist, shutdown,
                        )
                        .await;
                    }
                }
                };
            ironcache_runtime::run_shards_uring(&shard_cfg, uring_serve, rxs, drain)?
        }
        #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
        {
            unreachable!("io_uring_ok is false unless the io_uring feature is compiled in")
        }
    } else if raw_uring_ok {
        #[cfg(all(target_os = "linux", feature = "io_uring_raw"))]
        {
            tracing::info!(
                "runtime = io_uring_raw: using the Linux RAW io_uring datapath (plaintext, #682); \
                 it cross-builds on static-musl and reaches multishot (deferred to a Linux soak; no \
                 throughput claim made)"
            );
            let raw_serve =
                {
                    let ctx_template = ctx_template_for_raw_uring;
                    let inbox = inbox_for_raw_uring;
                    let persist = persist_for_raw_uring;
                    move |rt: ironcache_runtime::RawIoUringRuntime,
                      stream: ironcache_runtime::RawUringTcpStream,
                      shard: ShardId,
                      shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>| {
                    let ctx = ctx_template.clone();
                    let inbox = inbox.clone();
                    let persist = persist.clone();
                    async move {
                        serve_connection_raw_uring(
                            rt, stream, shard, ctx, default_proto, inbox, persist, shutdown,
                        )
                        .await;
                    }
                }
                };
            ironcache_runtime::run_shards_raw_uring(&shard_cfg, raw_serve, rxs, drain)?
        }
        #[cfg(not(all(target_os = "linux", feature = "io_uring_raw")))]
        {
            unreachable!("raw_uring_ok is false unless the io_uring_raw feature is compiled in")
        }
    } else {
        // Tokio fallback: the default + non-Linux + no-feature + TLS-on + kernel-incapable path.
        if (want_io_uring || want_raw_uring) && config.tls == ironcache_config::TlsMode::On {
            tracing::warn!(
                "runtime = io_uring[_raw] requested with TLS on; the io_uring datapath does not \
                 support TLS in v1 -- falling back to the tokio backend for this node"
            );
        } else if want_io_uring || want_raw_uring {
            // Requested but this build/target/kernel cannot provide it (a compiled-in probe already
            // logged the specific kernel reason above). Never a boot failure.
            tracing::warn!(
                "runtime = io_uring[_raw] requested, but this build/target/kernel cannot provide \
                 it; falling back to the tokio backend for this node"
            );
        }
        // Bind the OPTIONAL io_uring serve captures to `_` so an arm/build that did not consume them
        // has no dead-code warning (they are cheap clones prepared unconditionally above).
        let _ = (
            &ctx_template_for_uring,
            &inbox_for_uring,
            &persist_for_uring,
            &ctx_template_for_raw_uring,
            &inbox_for_raw_uring,
            &persist_for_raw_uring,
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
        inbox: inbox_for_handles,
        cutover_control,
        tls_reload,
    })
}

// `too_many_lines` is allowed: this is the per-connection WIRING + read/dispatch/write loop --
// the shard-handle lazy-inits, the per-connection push channel + shed signal (FIX D), the
// pipelined decode/route/flush loop, the subscribe-mode idle wait, and the close-path cleanup
// (subscription deregistration + WATCH deregistration + counter close). Each is a documented step
// the connection lifecycle must run in one place; splitting it would scatter the loop's control
// flow across helpers that all need the same locals.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn serve_connection(
    tcp: tokio::net::TcpStream,
    home: ShardId,
    mut ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
    tls_acceptor: Option<ironcache_runtime::ReloadableAcceptor>,
    persist: Option<Arc<crate::persist::PersistState>>,
    // The per-shard graceful-shutdown flag (#543), the SAME `Arc<AtomicBool>` the acceptor and the
    // drain loop watch. The subscribe-mode idle wait races a SHORT poll of it so a connection PARKED
    // in `subscriber_idle_wait` (a pub/sub subscriber blocked on its push channel, or a CLIENT
    // TRACKING client) observes a graceful stop within one poll interval and CLOSES -- instead of
    // sitting on the `select!` forever and blocking the shard-thread join until the DRAIN_GRACE
    // backstop expires. The non-subscriber hot path never reads it (it closes when the acceptor drops
    // the connection channel on shutdown, exactly as before).
    shutdown: Arc<std::sync::atomic::AtomicBool>,
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
        Some(reloadable) => {
            // Read the CURRENTLY published acceptor right before the handshake (#563): a hot cert
            // reload that swapped in a new ServerConfig is picked up by THIS new connection, while
            // any in-flight connection keeps the config it already handshook with. A cheap lock-free
            // ArcSwap load.
            let acceptor = reloadable.acceptor();
            match ironcache_runtime::accept_tls(&acceptor, tcp).await {
                Ok(s) => s,
                Err(_e) => {
                    // A failed handshake (a non-TLS client, an unsupported version, a truncated
                    // ClientHello): close the connection. The bytes that arrived were never RESP and
                    // never reached the engine, so there is nothing to flush -- just return.
                    return;
                }
            }
        }
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
        // Drain every complete request currently buffered (pipelining), building one combined output
        // buffer, then flush once. #510: the batch's bytes are wrapped as a shared `Bytes` and each
        // decoded frame ADVANCES that view (`decode_shared` slices args zero-copy from it), so there
        // is no per-command `read_buf` drain (which would memmove all remaining pipelined bytes to the
        // front each time -- O(P^2) over a depth-P pipeline); the unconsumed tail is restored to
        // `read_buf` once after the batch.
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
        // #510 ZERO-COPY PARSE: wrap the accumulated read buffer as a shared `Bytes` for the
        // duration of this batch (O(1) move, no copy) so each decoded argument is a refcounted
        // SLICE of it rather than a fresh per-arg heap allocation + memcpy -- the deep-pipeline
        // win. `read_buf` is emptied here; the unconsumed trailing frame is moved back into it
        // after the batch. We `advance` `batch` past each consumed frame in place of a running
        // `consumed_total` cursor (the old per-batch `drain` is likewise replaced below).
        let mut batch = Bytes::from(std::mem::take(&mut read_buf));
        loop {
            match decode_shared(&batch, &limits) {
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
                    if let coordinator::HopOutcome::Deferred(target) = deferred_hop {
                        pending.push(DeferredHop {
                            target,
                            db: conn.db,
                            request,
                            cmd_start,
                            was_tracking,
                            was_bcast,
                            slow_threshold,
                            proto: conn.proto,
                        });
                        batch.advance(consumed);
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
                            &inbox,
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
                    batch.advance(consumed);
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
                        // #510: `batch` was already advanced past the blocking command (above), so it
                        // now holds exactly the UNprocessed tail. Move that tail into `read_buf` so
                        // `run_block_park` -- which owns `read_buf` during the park and re-decodes
                        // bytes that arrive -- starts at the next unprocessed byte. `read_buf` was
                        // emptied when `batch` was formed, so this restores it. (`batch` itself is
                        // re-formed from `read_buf` after the park returns, below.)
                        read_buf.extend_from_slice(&batch);
                        // Flush the pipelined replies that preceded the blocking command, so a
                        // blocked client still receives the earlier commands' replies before it
                        // parks (FIFO, never a blocking command holding up prior replies).
                        if !out.is_empty() {
                            let sent = out.len();
                            match stream.send(std::mem::take(&mut out)).await {
                                Ok(returned) => {
                                    out = returned;
                                    // #527: net output for the pre-park pipelined flush.
                                    state_rc.borrow().counters.on_net_output(sent as u64);
                                }
                                Err(_) => break 'conn,
                            }
                        }
                        // #661: count this connection in the node-wide `blocked_clients` gauge for
                        // the ENTIRE park. The guard clones the shard counters cell (no borrow held
                        // across the await) and decrements on Drop, so a wake, a timeout, or a peer
                        // close all clear the count -- INFO `blocked_clients` reflects the live
                        // parked set leak-free, on BOTH the pop-park and WAIT-park paths inside.
                        let _blocked = state_rc.borrow().counters.block_guard();
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
                        // sent). `out` was flushed inside the park loop. `run_block_park` owned
                        // `read_buf` and left the still-unprocessed bytes in it, so re-form `batch`
                        // from it (#510) and continue the decode loop to process whatever arrived.
                        batch = Bytes::from(std::mem::take(&mut read_buf));
                        continue;
                    }
                    if close {
                        // Flush the QUIT reply then close. send returns the owned
                        // buffer (owned-buffer model); we are closing, so the
                        // returned buffer is dropped rather than reclaimed. Sent over the
                        // CLIENT stream (plain or TLS); the plain arm is byte-identical to the
                        // prior `rt.send` (it calls the same TcpStream write_all), #105.
                        let sent = out.len();
                        if stream.send(std::mem::take(&mut out)).await.is_ok() {
                            // #527: net output for the QUIT reply.
                            state_rc.borrow().counters.on_net_output(sent as u64);
                        }
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
                            &inbox,
                        )
                        .await;
                    }
                    encode_into(&mut out, &ironcache_server::Value::Error(e), conn.proto);
                    let sent = out.len();
                    if stream.send(std::mem::take(&mut out)).await.is_ok() {
                        // #527: net output for the protocol-error reply before close.
                        state_rc.borrow().counters.on_net_output(sent as u64);
                    }
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
                &inbox,
            )
            .await;
        }

        // #510: `batch` now holds exactly the unconsumed trailing bytes (a partial frame, or empty).
        // Move them back into `read_buf` for the next recv to append to. `read_buf` was emptied when
        // `batch` was formed at the top of this batch (or by the block-park refresh above), so this
        // restores it to just the carry-over -- the same buffer state the old post-batch drain left.
        read_buf.extend_from_slice(&batch);

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
            let sent = out.len();
            match stream.send(std::mem::take(&mut out)).await {
                Ok(returned) => {
                    out = returned;
                    // #527: count the reply bytes written to the client socket (net output). ONE
                    // relaxed atomic add through this shard's counters, summed node-wide for INFO.
                    state_rc.borrow().counters.on_net_output(sent as u64);
                }
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
                // #543: race a short poll of the shard shutdown flag so a parked subscriber closes
                // promptly on a graceful stop. `timer_rt` is the same Runtime timer seam the idle-
                // timeout arm uses (ADR-0003: no wall-clock on the decision path).
                &shutdown,
                &timer_rt,
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
                    // #527: count the command bytes read off the client socket (net input).
                    state_rc.borrow().counters.on_net_input(res.n as u64);
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
            // #527: count the command bytes read off the client socket (net input).
            state_rc.borrow().counters.on_net_input(res.n as u64);
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
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
/// The io_uring serve loop, GENERIC over the [`ironcache_runtime::Runtime`] backend so BOTH the
/// tokio-uring (`IoUringRuntime`) and the raw (`RawIoUringRuntime`) backends share ONE implementation
/// (#682 P2). The backend-specific per-connection setup -- peer/local address + `SO_KEEPALIVE`, which
/// need a borrowed-fd `unsafe` dance done in the runtime crate -- is performed by the thin wrappers
/// `serve_connection_uring` / `serve_connection_raw_uring` and handed in as `addr`/`laddr`. Both
/// backends fix `Buf = Vec<u8>`, so the loop's owned buffers are concrete `Vec<u8>`.
async fn serve_connection_generic<R>(
    rt: R,
    mut stream: R::Stream,
    home: ShardId,
    mut ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
    persist: Option<Arc<crate::persist::PersistState>>,
    addr: String,
    laddr: String,
    // The per-shard graceful-shutdown flag (#543), mirrored from the tokio serve path: the
    // subscribe-mode idle wait races a short poll of it so a PARKED io_uring subscriber closes
    // promptly on a graceful stop instead of blocking the shard-thread join until the drain grace.
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) where
    R: ironcache_runtime::BatchedRecvSend,
{
    // No `use` of the traits is needed: `rt`'s methods (`recv_batch`/`send_batch`, and the `Runtime`
    // supertrait's `send`/`timer`) resolve through the generic `R: BatchedRecvSend` bound directly.

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

    // `addr`/`laddr` (peer/local for CLIENT INFO) and the SO_KEEPALIVE application are done by the
    // backend-specific wrapper before this generic loop is entered -- they need a borrowed-fd
    // `unsafe` dance on the concrete stream type, which lives in the runtime crate.

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
    // ZERO-COPY GET splice list (#515 P3b, INERT until P4): the ordered `ZcInsert`s (a pinned stored
    // value + its offset in `out`) to interleave into `out` at flush, so a large present value is
    // written store->socket with no copy into `out`. EMPTY in P3b -- nothing pushes yet -- so every
    // flush is `send_zc(out, [])`, exactly a `send_batch(out)` (byte-identical). Always drained (taken)
    // at every flush, so it carries no state across the outer loop. P4's `get_home_by_ref` pushes here.
    let mut zc_inserts: Vec<ironcache_runtime::ZcInsert> = Vec::new();
    // The zero-copy PINS (#515 P4): the frozen-slot handles (store `ZcPin`s, type-erased) that keep
    // each `zc_inserts` region alive until the send's CQE -- the io_uring send OWNS them, so even a
    // cancelled send reads valid memory. Drained (taken) with `zc_inserts` at every flush; EMPTY in
    // P4b (nothing pins yet), so the flush is byte-identical. P4c's `get_home_by_ref` pushes here.
    let mut zc_pins: Vec<Box<dyn core::any::Any>> = Vec::new();
    // #515 P4c: INSTALL this shard thread's zero-copy GET sink (once per thread -- idempotent), so
    // `get_home_by_ref` splices a large String hit's value straight from the store instead of copying
    // it into `out`. Installed ONLY here (the io_uring serve loop); the tokio loop never installs one,
    // so `get_home_by_ref` there always copies. Left installed for the thread's life -- every command
    // drains it back to empty (see `drain_zc_sink` / `ZC_SINK`), so sharing it across the connections
    // multiplexed on this shard thread is sound.
    ZC_SINK.with(|c| {
        let mut g = c.borrow_mut();
        if g.is_none() {
            *g = Some(ZcSink::default());
        }
    });
    // CROSS-SHARD HOP OVERLAP (#8 / #514): the run of DEFERRED remote hops awaiting assembly, mirrored
    // from the tokio serve loop. Parked as they are decoded and drained (in order) at the next barrier /
    // error / end of batch, so a run of pipelined remote-key ops overlaps the owner's work instead of
    // serializing on each cross-thread round-trip. Always empty between batches (drained before every
    // flush), so it carries no state across the outer loop.
    let mut pending: Vec<DeferredHop> = Vec::new();
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
        // #510 ZERO-COPY PARSE (mirrors the tokio loop): wrap the accumulated read buffer as a
        // shared `Bytes` for this batch (O(1) move) so each decoded argument is a refcounted SLICE
        // of it, not a per-arg heap allocation + memcpy. `read_buf` is emptied here and the
        // unconsumed trailing frame is moved back into it after the batch; we `advance` `batch` per
        // consumed frame in place of a running cursor (and replace the old post-batch `drain`).
        let mut batch = Bytes::from(std::mem::take(&mut read_buf));
        loop {
            match decode_shared(&batch, &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    let slow_threshold = ctx.slowlog.log_slower_than_micros();
                    // COMMANDSTATS timing (#413): capture the monotonic start ALWAYS (one read),
                    // SHARED with the SLOWLOG hook below so the slowlog-enabled path adds no start
                    // read. The end is read once after dispatch to record this command's usec.
                    let cmd_start = env.borrow().now();
                    // The offset where THIS command's reply will be appended, so the commandstats
                    // hook can tell an error reply (leading `-`) from a success without re-parsing.
                    // `mut`: the cross-shard-hop overlap (#8 / #514) re-bases this when a run of pending
                    // hop replies is spliced in ahead of a barrier command's output (see below).
                    let mut out_before = out.len();
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
                    // #8 / #514: the io_uring loop opts IN to hop deferral, mirroring the tokio serve
                    // loop -- route_and_dispatch ENQUEUES a single-key remote hop and sets this to
                    // `Deferred(rx)` without awaiting, so a run of pipelined hops overlaps the owner's
                    // work; we park it in `pending` and drain the run in order at the next barrier.
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
                        // #8 / #514: io_uring loop opts IN to hop deferral (mirrors the tokio loop).
                        true,
                        &mut deferred_hop,
                    )
                    .await;
                    // #8 / #514 OVERLAP: a DEFERRED remote hop -- park it and keep decoding, so the next
                    // command's hop is issued while this owner works. `out` is UNTOUCHED (the reply is
                    // encoded later, in order), so FIFO on the wire is preserved. A hop is never a
                    // blocking command and never closes the connection, so we skip the hooks + FIX1 +
                    // close handling and go straight to the next command; its hooks run at drain time.
                    if let coordinator::HopOutcome::Deferred(target) = deferred_hop {
                        pending.push(DeferredHop {
                            target,
                            db: conn.db,
                            request,
                            cmd_start,
                            was_tracking,
                            was_bcast,
                            slow_threshold,
                            proto: conn.proto,
                        });
                        batch.advance(consumed);
                        continue;
                    }
                    // #515 P4c: DRAIN this command's zero-copy splices (a large home-GET hit may have
                    // pinned its value + pushed a splice) into the flush lists. This runs with NO
                    // `.await` since `route_and_dispatch` returned, so the sink holds exactly THIS
                    // command's splices and no other multiplexed connection can have raced it. Capture
                    // the pre-drain length so the barrier below can re-base only this command's offsets.
                    let zc_added_from = zc_inserts.len();
                    drain_zc_sink(&mut zc_inserts, &mut zc_pins);
                    // BARRIER (any synchronous / control / home / fan-out / blocking command): if a run of
                    // hops is pending, splice their replies into `out` BEFORE this command's already-
                    // encoded output (FIFO), running each hop's deferred hooks in order. `split_off` lifts
                    // this command's bytes aside; after draining the run we re-append them and re-base
                    // `out_before` so this command's own hooks read the right reply slice. A blocking
                    // command left `out` empty here (FIX1 writes its immediate reply below, AFTER this
                    // drain), so the drained hop replies still precede it on the wire.
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
                            &inbox,
                        )
                        .await;
                        // #515 P4c: the hop replies were spliced BEFORE this command's bytes, shifting
                        // this command's region forward by exactly the bytes inserted ahead of it. Its
                        // zero-copy splice offsets (`ZcInsert::at`, absolute into `out`) must move by the
                        // same delta; offsets from EARLIER commands sit before `out_before` and are
                        // untouched by the `split_off`, so only `[zc_added_from..]` is re-based.
                        let zc_shift = out.len() - out_before;
                        out_before = out.len();
                        out.extend_from_slice(&barrier_bytes);
                        for zi in &mut zc_inserts[zc_added_from..] {
                            zi.at += zc_shift;
                        }
                    }
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
                    batch.advance(consumed);
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
                        // #515: the flush SPLICES the pinned zero-copy value regions (`zc_inserts`)
                        // into the wire stream straight from the store, so the bytes actually SENT are
                        // `out` PLUS those pinned regions -- count both for the net-output stat. Empty
                        // `zc_inserts` (no large GET in this batch) makes the flush byte-identical to a
                        // plain `send_batch(out)`. The pins (`zc_pins`) are OWNED by `send_zc` until its
                        // CQE, so the pinned store bytes stay valid for the kernel read even if the send
                        // is cancelled (the #576-COW keeps the frozen value immutable meanwhile).
                        let sent = out.len() + zc_inserts.iter().map(|i| i.len).sum::<usize>();
                        let flushed_ok = rt
                            .send_zc(
                                &mut stream,
                                std::mem::take(&mut out),
                                std::mem::take(&mut zc_inserts),
                                std::mem::take(&mut zc_pins),
                            )
                            .await
                            .is_ok();
                        if flushed_ok {
                            // #527: net output for the io_uring QUIT reply.
                            state_rc.borrow().counters.on_net_output(sent as u64);
                        }
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
                    // Protocol error: write it and close the connection (hardening). #8 / #514: FIRST
                    // drain any pending cross-shard hops so the replies for the VALID commands that
                    // preceded the malformed frame still go out (in order) before the error + close --
                    // otherwise the overlap would silently drop them (the inline path flushed them).
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
                            &inbox,
                        )
                        .await;
                    }
                    encode_into(&mut out, &ironcache_server::Value::Error(e), conn.proto);
                    // #515: bytes SENT = `out` plus the spliced pinned regions (see the QUIT flush
                    // above). `zc_pins` is owned by `send_zc` until its CQE.
                    let sent = out.len() + zc_inserts.iter().map(|i| i.len).sum::<usize>();
                    let flushed_ok = rt
                        .send_zc(
                            &mut stream,
                            std::mem::take(&mut out),
                            std::mem::take(&mut zc_inserts),
                            std::mem::take(&mut zc_pins),
                        )
                        .await
                        .is_ok();
                    if flushed_ok {
                        // #527: net output for the io_uring protocol-error reply before close.
                        state_rc.borrow().counters.on_net_output(sent as u64);
                    }
                    break 'conn;
                }
            }
        }

        // CROSS-SHARD HOP OVERLAP (#8 / #514): the batch ended (decode Incomplete) with a run of remote
        // hops still pending (no trailing barrier drained them). Assemble their replies into `out` NOW,
        // in order, BEFORE the flush -- so every reply for this batch is on the wire in command order.
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
                &inbox,
            )
            .await;
        }

        // #510: `batch` now holds exactly the unconsumed trailing bytes (a partial frame, or empty);
        // move them back into `read_buf` for the flush + subscriber-idle / recv paths below (which
        // own `read_buf` and append to it). `read_buf` was emptied when `batch` was formed, so this
        // restores just the carry-over -- the same buffer state the old post-batch drain left.
        read_buf.extend_from_slice(&batch);

        // OUTPUT-BUFFER hard cap (PROD-SAFETY #5), identical to the tokio path: the pre-flush check,
        // using the `obl` read once at the top of this batch (shared with the intra-batch check).
        if obl > 0 && out.len() as u64 > obl {
            break;
        }

        if !out.is_empty() {
            // OneShotFixed WRITE tier (#284): stage the reply through this shard's REGISTERED
            // fixed buffer when one is free and the reply fits, else the owned send. Byte-identical
            // output; hands the buffer back for reuse exactly like the owned `rt.send` did.
            // #515: bytes SENT = `out` plus the spliced pinned zero-copy value regions (`zc_inserts`),
            // which are NOT copied into `out` -- count both for the net-output stat. Empty `zc_inserts`
            // (no large GET this batch) makes the flush byte-identical to a plain `send_batch(out)`. A
            // large GET always writes its `$len\r\n..\r\n` framing into `out`, so `out` is non-empty
            // whenever `zc_inserts` is (this guard never skips a pending splice). The pins (`zc_pins`)
            // are OWNED by `send_zc` until its CQE.
            let sent = out.len() + zc_inserts.iter().map(|i| i.len).sum::<usize>();
            let flush_result = rt
                .send_zc(
                    &mut stream,
                    std::mem::take(&mut out),
                    std::mem::take(&mut zc_inserts),
                    std::mem::take(&mut zc_pins),
                )
                .await;
            match flush_result {
                Ok(returned) => {
                    out = returned;
                    // #527: net output for the io_uring batch flush (one relaxed atomic add).
                    state_rc.borrow().counters.on_net_output(sent as u64);
                }
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
            if subscriber_idle_wait_generic(
                &rt,
                &mut stream,
                &mut push_rx,
                &shed_flag,
                &mut read_buf,
                &mut out,
                conn.proto,
                // #543: race a short poll of the shard shutdown flag so a parked subscriber closes
                // promptly on a graceful stop. `rt` supplies the timer seam (its `timer` is the
                // canonical tokio time driver under `tokio_uring::start`), ADR-0003-clean.
                &shutdown,
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
            let Ok(n) = rt.recv_batch(&mut stream, &mut read_buf).await else {
                break;
            };
            if n == 0 {
                break; // peer closed
            }
            // #527: count the command bytes read off the client socket (net input), io_uring path.
            state_rc.borrow().counters.on_net_input(n as u64);
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

/// Thin TOKIO-URING wrapper: does the concrete-`UringTcpStream` per-connection setup (peer/local
/// address for CLIENT INFO + SO_KEEPALIVE, both via the runtime crate's borrowed-fd helpers so the
/// `unsafe` stays out of this crate), then runs the shared [`serve_connection_generic`] loop.
#[cfg(all(target_os = "linux", feature = "io_uring"))]
#[allow(clippy::too_many_arguments)]
async fn serve_connection_uring(
    rt: ironcache_runtime::IoUringRuntime,
    stream: ironcache_runtime::UringTcpStream,
    home: ShardId,
    ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
    persist: Option<Arc<crate::persist::PersistState>>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    let (addr, laddr) = ironcache_runtime::peer_local_addrs(&stream);
    ironcache_runtime::set_keepalive_uring(&stream, ctx.runtime.tcp_keepalive_secs());
    serve_connection_generic(
        rt,
        stream,
        home,
        ctx,
        default_proto,
        inbox,
        persist,
        addr,
        laddr,
        shutdown,
    )
    .await;
}

/// Thin RAW io_uring wrapper (#682 P2): the same per-connection setup as `serve_connection_uring`
/// but on the raw backend's concrete `RawUringTcpStream`, then the SAME shared generic serve loop.
#[cfg(all(target_os = "linux", feature = "io_uring_raw"))]
#[allow(clippy::too_many_arguments)]
async fn serve_connection_raw_uring(
    rt: ironcache_runtime::RawIoUringRuntime,
    stream: ironcache_runtime::RawUringTcpStream,
    home: ShardId,
    ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
    persist: Option<Arc<crate::persist::PersistState>>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    let (addr, laddr) = ironcache_runtime::peer_local_addrs_raw(&stream);
    ironcache_runtime::set_keepalive_raw(&stream, ctx.runtime.tcp_keepalive_secs());
    serve_connection_generic(
        rt,
        stream,
        home,
        ctx,
        default_proto,
        inbox,
        persist,
        addr,
        laddr,
        shutdown,
    )
    .await;
}

/// How often a subscribe-mode idle wait re-checks the shared shutdown flag (#543). A subscriber
/// parked on the `select!` of (push recv, shed, socket recv) has no arm a graceful stop wakes, so
/// it races this SHORT timer (the Runtime timer SEAM, ADR-0003 -- NOT wall-clock) to observe the
/// flag within one interval and close. Chosen well UNDER the shutdown-latency budget (the shard
/// join must return in ~2s with active subscribers) yet coarse enough that a fully idle subscriber
/// wakes only a few times a second -- a negligible cost the steady-state push hot path never pays
/// (a ready push always wins its own arm before this timer elapses).
const SUBSCRIBER_SHUTDOWN_POLL: core::time::Duration = core::time::Duration::from_millis(250);

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
///
/// The SHUTDOWN arm (#543) races a SHORT periodic poll of the shared per-shard shutdown flag
/// (`timer_rt.timer(SUBSCRIBER_SHUTDOWN_POLL)`, the SAME Runtime timer seam the idle-timeout arm
/// uses -- ADR-0003, no wall-clock on the decision path). A non-subscriber closes on shutdown
/// because the acceptor drops its connection channel, but a subscriber PARKED on this `select!` of
/// (push recv, shed, socket recv) has NO arm that a graceful stop wakes, so without this it would
/// sit here until the DRAIN_GRACE backstop expires and block the shard-thread join. The poll fires
/// ONLY on a fully idle subscriber (a ready push always wins its own arm first, so the steady-state
/// hot path is unaffected); on each wake it does one relaxed atomic load -- `true` -> close, `false`
/// -> re-arm the idle wait (the same "return false, re-loop with an unchanged `read_buf`" path a
/// delivered push already takes), so a false wake costs one cheap loop turn, never a busy spin.
#[allow(clippy::too_many_arguments)]
async fn subscriber_idle_wait(
    stream: &mut ironcache_runtime::ClientStream,
    push_rx: &mut tokio::sync::mpsc::Receiver<crate::pubsub::ServerPush>,
    shed: &std::sync::Arc<crate::pubsub::ShedSignal>,
    read_buf: &mut Vec<u8>,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    timer_rt: &TokioRuntime,
) -> bool {
    // Fast pre-check: if the publisher already shed this connection between iterations, close
    // now without entering the select! (the table sender is gone; nothing more will arrive).
    if shed.is_tripped() {
        return true;
    }
    // Fast pre-check: if a graceful stop already began between iterations, close now (#543) without
    // entering the select! -- symmetrical to the shed pre-check above.
    if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
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
            let sent = out.len();
            stream.send(std::mem::take(out)).await.map_or(true, |returned| {
                *out = returned;
                // #527: net output for a coalesced pub/sub push write (cold path -- reach the
                // serving shard's counters through the thread-local, no `state_rc` in scope here).
                shard_state().borrow().counters.on_net_output(sent as u64);
                false
            })
        }
        () = shed.wait() => {
            // The publisher SHED this slow consumer (its push channel overflowed past the
            // bound): close the connection (its subscriptions are cleaned up on the close path).
            true
        }
        () = timer_rt.timer(SUBSCRIBER_SHUTDOWN_POLL) => {
            // #543: a short poll of the shared shutdown flag. On a graceful stop, close (`true`);
            // otherwise (`false`) re-arm the idle wait -- the outer serve loop re-decodes the
            // unchanged `read_buf` (Incomplete), flushes the empty `out`, and re-enters here.
            shutdown.load(std::sync::atomic::Ordering::Relaxed)
        }
        res = stream.recv(Vec::new()) => {
            let Ok(res) = res else { return true; };
            if res.n == 0 {
                return true; // peer closed
            }
            read_buf.extend_from_slice(&res.buf);
            // #527: net input for a command read while in the subscriber idle wait.
            shard_state().borrow().counters.on_net_input(res.n as u64);
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
///
/// The SHUTDOWN arm (#543) mirrors the tokio [`subscriber_idle_wait`]: it races a SHORT poll of the
/// shared per-shard shutdown flag ([`SUBSCRIBER_SHUTDOWN_POLL`] via `rt.timer`, whose seam is the
/// canonical tokio time driver under `tokio_uring::start`), so a subscriber parked on this `select!`
/// observes a graceful stop within one interval and closes instead of blocking the shard-thread join
/// until the drain grace. It fires only on a fully idle subscriber; a false wake re-arms the wait.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
#[allow(clippy::too_many_arguments)]
async fn subscriber_idle_wait_generic<R>(
    rt: &R,
    stream: &mut R::Stream,
    push_rx: &mut tokio::sync::mpsc::Receiver<crate::pubsub::ServerPush>,
    shed: &std::sync::Arc<crate::pubsub::ShedSignal>,
    read_buf: &mut Vec<u8>,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> bool
where
    R: ironcache_runtime::BatchedRecvSend,
{
    // The socket-read arm uses `recv_batch` (NOT a fresh single-shot `rt.recv`): on the raw backend a
    // MULTISHOT recv may already be armed on this fd from the main loop, and opening a competing
    // single-shot `Read` would split the byte stream between two ops (#513 review). `recv_batch`
    // drains the SAME multishot slot; a `select!`-cancel of it is drop-safe (the multishot slot is
    // persistent, and the owned fallback appends into a fresh buffer). `rt`'s methods resolve through
    // the `R: BatchedRecvSend` bound directly.
    // Fast pre-check: if the publisher already shed this connection between iterations, close now
    // without entering the select! (the table sender is gone; nothing more will arrive). This is
    // ALSO the close-on-shed for a pure-idle flooded subscriber (FIX2 non-negotiable).
    if shed.is_tripped() {
        return true;
    }
    // Fast pre-check: if a graceful stop already began between iterations, close now (#543).
    if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
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
            let sent = out.len();
            rt.send(stream, std::mem::take(out)).await.map_or(true, |returned| {
                *out = returned;
                // #527: net output for a coalesced pub/sub push write (io_uring cold path).
                shard_state().borrow().counters.on_net_output(sent as u64);
                false
            })
        }
        () = shed.wait() => {
            // The publisher SHED this slow consumer (its push channel overflowed past the bound):
            // close the connection (its subscriptions are cleaned up on the close path).
            true
        }
        () = rt.timer(SUBSCRIBER_SHUTDOWN_POLL) => {
            // #543: a short poll of the shared shutdown flag. On a graceful stop, close (`true`);
            // otherwise (`false`) re-arm the idle wait (the outer serve loop re-decodes the
            // unchanged `read_buf`, flushes the empty `out`, and re-enters here).
            shutdown.load(std::sync::atomic::Ordering::Relaxed)
        }
        n = rt.recv_batch(stream, read_buf) => {
            let Ok(n) = n else { return true; };
            if n == 0 {
                return true; // peer closed
            }
            // recv_batch already APPENDED the newly-arrived bytes into `read_buf` (multishot slot or
            // owned fallback); count them. #527: net input for a subscriber-idle-wait command read.
            shard_state().borrow().counters.on_net_input(n as u64);
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
    // -- THE GLOBAL SERVE GATE (#391 PR-5 streamed live-cutover). Until THIS process has COMMITTED the
    // cross-shard cutover, reject EVERY client command with `-LOADING` -- BEFORE the command name is
    // even classified -- so a client never reads a half-loaded or not-yet-committed store. `is_serving`
    // is a single process-global relaxed load that is `true` on every normal (non-handoff) boot, so the
    // default datapath pays one predictable-not-taken branch and is BYTE-UNCHANGED; it is `false` ONLY
    // on a streamed-handoff RECEIVER boot, and flips to `true` EXACTLY ONCE, atomically for all shards
    // (one global bool, no per-shard stagger), on the PR-4 `Committed` transition
    // (`upgrade::commit::begin_serving_on_commit`). That flip happens only AFTER the OLD released write
    // authority (permanent quiesce, PR-4), so no write is ever double-acked across the cutover. This
    // reuses PR-3's retryable `-LOADING` (`ErrorReply::loading`); the connection stays OPEN (returns
    // `false`) so the client retries against whichever process ends up authoritative. In production the
    // orchestrator (PR-6) keeps the NEW's acceptor closed until the flip, so this gate is
    // defense-in-depth that never fires on the normal path.
    if !is_serving() {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::loading()),
            conn.proto,
        );
        return false;
    }

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

    // -- THE `-LOADING` WRITE-QUIESCE GATE (#391 streamed live-cutover, Decision 2 Option C).
    // While THIS shard is quiescing for the final delta cut, reject every client MUTATOR with
    // `-LOADING` HERE -- BEFORE routing, MULTI queueing, cross-shard hop, or local dispatch, and so
    // BEFORE the store's write funnel assigns the write a ring offset. That is what makes "a client
    // write is acked only if its offset <= E" structural: the write never reaches the ring, so it
    // can never land above the latched cut offset. Reads (and admin like PING/INFO) flow straight
    // through. The write classifier is the SAME [`ironcache_server::request_is_write_for_pause`] the
    // CLIENT PAUSE gate uses, so the MULTI/EXEC convention matches exactly: a command merely being
    // QUEUED inside a MULTI is not a write here (it is held at its EXEC), and an EXEC whose staged
    // batch contains any write IS a write (so the whole transaction is rejected, never partially
    // applied above E). DEFAULT (not quiescing) is BYTE-UNCHANGED and near-free: `is_shard_loading`
    // is a single core-local `Cell<bool>` load that short-circuits the `&&`, so the classifier never
    // runs and this is one predictable-not-taken branch. This is DELIBERATELY not CLIENT PAUSE: a
    // paused write applies AFTER the window (at an offset > E, lost by the cut), whereas this REJECTS
    // it so the client retries against whichever process ends up authoritative.
    if is_shard_loading()
        && ironcache_server::request_is_write_for_pause(&cmd_upper, conn.in_multi, &conn.queued)
    {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::loading()),
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
        // -- client-unreachable; only the home core issues it (via `do_save_all`'s `fan_out_save`
        // to each shard's drain loop, which dumps that shard's partition, yielding between chunks).
        // Like `__ICEXISTS` it is
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

    // -- MONITOR HONESTY INTERCEPTION (#527). MONITOR (stream every executed command to the
    // subscribed client) is NOT implemented: a correct implementation needs a fan-out from the
    // command choke point to a set of monitor connections, which this build does not have. Rather
    // than let it fall through to the generic `unknown command` reply (which would suggest it is
    // merely unrecognized) OR silently mis-behave, reply a CLEAR, honest `-ERR MONITOR is not
    // supported`. It is intentionally NOT registered in the command spec, so `COMMAND` does not
    // advertise it (we do not claim a capability we lack -- the same honesty that removed the
    // MONITOR mention from the README secret-hygiene note). Gated `!conn.in_multi` like the other
    // serve-layer rejects: inside a MULTI it is an unregistered token, so the queue gate rejects it
    // with the standard unknown-command error and dirties the transaction. A non-MONITOR command
    // never enters this block (one byte-compare), so the hot path is byte-unchanged.
    if !conn.in_multi && cmd_upper == b"MONITOR" {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::err(
                "MONITOR is not supported",
            )),
            conn.proto,
        );
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
        publish_pending_keyspace_events(inbox, home.index);
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
        // WHOLE-KEYSPACE dispatch. In Static/Raft the keyspace is ONE logical whole, so a
        // whole-keyspace command SCATTER-GATHERS across EVERY shard's partition (SCAN walks one
        // shard per call via the composite cursor; the rest broadcast + merge on the home core).
        //
        // In shard-owners mode (#526) the node advertises its N internal shards as N cluster
        // nodes (one per port) and each shard's store holds EXACTLY its slot range (#520). So a
        // whole-keyspace command issued to shard i's port must answer for shard i ONLY -- the
        // per-node Redis Cluster view -- NOT the global fan-out (which would make a per-node
        // aggregator over-count DBSIZE by N and return N copies from SCAN). Serve HOME-ONLY: the
        // connecting shard's local partial IS the whole per-node answer. Both paths run the SAME
        // per-shard partial; they differ only in whether it is fanned out or served alone.
        //
        // These were never on the single-key hot path, so awaiting here is fine.
        state_rc.borrow_mut().counters.on_command();
        let home_only = ctx.cluster_mode() == ironcache_config::ClusterMode::ShardOwners;
        if cmd_upper == b"SCAN" {
            // SCAN pins to the home shard when `home_only` (start there, finish when it is
            // exhausted rather than advancing to a sibling); else it walks all shards.
            crate::whole_keyspace::scan_cross_shard(
                inbox, ctx, request, conn.db, home.index, out, conn.proto, home_only,
            )
            .await;
        } else if home_only {
            // HOME-ONLY (no fan-out, no cross-shard RNG shard-pick): DBSIZE / KEYS / RANDOMKEY /
            // FLUSHDB / FLUSHALL served from the connecting shard's local partition alone.
            // RANDOMKEY draws from THIS shard's own Env RNG seam inside the partial (ADR-0003);
            // FLUSHDB / FLUSHALL clear only this shard's slice (each cluster node flushes its
            // own slots).
            crate::whole_keyspace::run_home_only(ctx, request, conn.db, out, conn.proto);
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
        // COORDINATOR HOP OBSERVABILITY (#556, the #517 zero-hop measurement harness): THIS shard is
        // about to DISPATCH a single-target cross-shard keyed hop to `target` -- the hop it PAYS.
        // Count it here (covering BOTH the deferred #8-overlap enqueue and the fused `dispatch_via`),
        // ONE relaxed atomic on this already-taken remote branch. hop-rate = hops_sent / (hops_sent +
        // local_served); in shard-owners mode a client dialing owner ports never reaches this branch,
        // so `hops_sent` trends to ~0 -- the #517 property, now MEASURABLE instead of merely claimed.
        state_rc.borrow().counters.on_hop_sent();
        if defer_hops {
            // #8 OVERLAP + #674 COALESCING: RECORD the owning shard; do NOT send yet. The serve loop
            // parks this as a `DeferredHop` and `drain_deferred_hops` groups the whole run's hops per
            // shard, sending ONE coalesced `ShardWork::Batch` per shard with >= 2 hops (a `Single` for
            // a lone hop) and demuxing the replies in wire order. `out` is left UNTOUCHED (the reply is
            // encoded later, in order, at drain). Deferring the send to drain is what enables the
            // coalescing; the decode-overlap it trades away is negligible (decode is not the cost).
            *deferred_hop = coordinator::HopOutcome::Deferred(target);
            // No home post-processing for a deferred remote hop: the wake/keyspace-publish run on the
            // OWNER shard (via run_remote), and the home probes are no-ops for a remote key. Return
            // early so we do NOT run the shared post-dispatch (wake/publish) against `out`.
            return false;
        }
        coordinator::dispatch_via(inbox, target, request, conn.db, out, conn.proto).await;
        false
    } else {
        // HOME path: the SYNC fast path (zero await/channel). Covers the home-owned keyed
        // commands, AlwaysHome, and the key-SPANNING multi-key commands (Stage 2 gap).
        // COORDINATOR HOP OBSERVABILITY (#556, the #517 zero-hop measurement harness): a KEYED
        // request whose owner IS the home shard is served here with NO hop -- the ZERO-hop path, the
        // complement of `hops_sent` (so hop-rate = hops_sent / (hops_sent + local_served)). Count it
        // ONLY for the keyed classes (AlwaysHome control/conn commands are not keyed requests and
        // must not dilute the ratio); a co-located KeyedMulti reaches here too (the shard-spanning
        // forms took their fan-out branches above). ONE relaxed atomic on this existing home branch.
        if matches!(
            route,
            route::CommandClass::KeyedSingle | route::CommandClass::KeyedMulti
        ) {
            state_rc.borrow().counters.on_local_served();
        }
        // Pass the ALREADY-uppercased command (FIX 5): we computed `cmd_upper` above for
        // routing, so the home dispatch reuses it instead of re-uppercasing + re-allocating.
        //
        // #531: an INFO whose reply includes the `# Keyspace` section reports the NODE-WIDE per-db
        // key counts on a multi-shard node -- consistent with DBSIZE. Gather them FIRST via the SAME
        // whole-keyspace scatter-gather DBSIZE uses, then hand the summed lines to the sync INFO
        // render. This runs ONLY for INFO (a cold, rare command) on a >1-shard node; a single-shard
        // node (the serving shard IS the whole keyspace) passes `None`, so its local `db_len` render
        // stays byte-identical. Both serve loops (tokio + io_uring) route through here, so the fix
        // covers both. AlwaysHome, so INFO reaches this home branch; the await here is off the data
        // hot path.
        let node_keyspace: Option<Vec<ironcache_observe::KeyspaceDbLine>> =
            if home.total > 1 && cmd_upper == b"INFO" && info_reply_includes_keyspace(request) {
                Some(
                    crate::whole_keyspace::gather_node_keyspace(
                        inbox,
                        ctx,
                        ctx.databases,
                        conn.db,
                        home.index,
                    )
                    .await,
                )
            } else {
                None
            };
        handle_request(
            ctx,
            conn,
            env,
            store_rc,
            wheel_rc,
            state_rc,
            request,
            &cmd_upper,
            node_keyspace.as_deref(),
            out,
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
    publish_pending_keyspace_events(inbox, home.index);
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
fn publish_pending_keyspace_events(inbox: &coordinator::Inbox, home: usize) {
    let events = ironcache_config::notify::drain();
    if events.is_empty() {
        return;
    }
    for ev in events {
        // FIRE-AND-FORGET (#543): the delivery COUNT is ignored for a notification, so this enqueues
        // the fan-out and returns rather than awaiting every shard's reply. This keeps notifications
        // off the command's synchronous cross-shard path (the drain-loop analog in
        // `coordinator::publish_pending_keyspace_events` MUST be fire-and-forget to avoid a two-shard
        // drain-loop deadlock; the home path matches it for a consistent, deadlock-free model). FIFO
        // to any one subscriber is preserved (per source->target inbox ordering); a self-subscribed
        // connection still receives the push AFTER its command reply because the push rides the
        // separate per-connection channel drained only after this batch's reply is flushed.
        if ev.keyspace {
            let channel = ev.keyspace_channel();
            coordinator::fan_out_publish_notify(inbox, &channel, ev.event.as_bytes(), ev.db, home);
        }
        if ev.keyevent {
            let channel = ev.keyevent_channel();
            coordinator::fan_out_publish_notify(inbox, &channel, &ev.key, ev.db, home);
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
///   `+Background saving started` IMMEDIATELY, so the ISSUING connection is not blocked. The per-shard
///   dump now YIELDS between snapshot chunks (#571), re-acquiring the store borrow per chunk, so a
///   dumping shard services queued writes DURING its dump instead of being blocked for the whole
///   keyspace dump -- a bounded, predictable save tail. (The snapshot is then an approximate
///   warm-start point rather than a strict per-shard point-in-time; see `crate::persist`.)
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
            // `SAVE` -> the normal DURABLE data_dir save (Redis parity). `SAVE HANDOFF` -> the #390
            // upgrade-handoff save, staged on tmpfs when the RAM-headroom guard admits it (else the
            // durable data_dir); it is issued by `ironcache upgrade` to shrink the reload window and
            // is client-reachable only over the auth-gated loopback the upgrade CLI uses. Any other
            // argument shape is a syntax error.
            let handoff = match request.args.len() {
                1 => false,
                2 if request.args[1].eq_ignore_ascii_case(b"HANDOFF") => true,
                _ => {
                    encode_into(
                        out,
                        &Value::error(ironcache_protocol::ErrorReply::wrong_arity("save")),
                        conn.proto,
                    );
                    return;
                }
            };
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
            let result = if handoff {
                crate::persist::do_handoff_save_all(persist, inbox, ctx, home, conn.db, now_secs)
                    .await
            } else {
                crate::persist::do_save_all(persist, inbox, ctx, home, conn.db, now_secs).await
            };
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
    //
    // #531: `None` node-keyspace here -- the MULTI queue path cannot fan out (EXEC replays
    // synchronously), so an INFO queued in a transaction falls back to the serving shard's local
    // `db_len` keyspace (a documented edge, consistent with the rest of the serving-shard-scoped
    // EXEC-replay data). A bare non-transaction INFO takes the async home branch above with the
    // node-wide gather.
    handle_request(
        ctx, conn, env, store_rc, wheel_rc, state_rc, request, cmd_upper, None, out,
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
                publish_pending_keyspace_events(inbox, home.index);
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
                Ok(r) => {
                    read_buf.extend_from_slice(&r.buf[..r.n]);
                    // #527: net input for pipelined bytes read while parked on a blocking command.
                    shard_state().borrow().counters.on_net_input(r.n as u64);
                    WakeOutcome::Bytes
                }
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
                    // poll loop continues. (Buffering them safely is a documented follow-up.) They
                    // WERE read off the socket, so #527 still counts them as net input.
                    Ok(r) => shard_state().borrow().counters.on_net_input(r.n as u64),
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
    let sent = out.len();
    match stream.send(std::mem::take(out)).await {
        Ok(returned) => {
            *out = returned;
            // #527: net output for a blocking command's reply (BLPOP/WAIT/... timeout or result).
            shard_state().borrow().counters.on_net_output(sent as u64);
            false
        }
        Err(_) => true,
    }
}

/// The nil-array a blocking pop replies on timeout (Redis NULL ARRAY: RESP2 `*-1`, RESP3 `_`).
fn block_timeout_value() -> ironcache_server::Value {
    ironcache_server::block_timeout_reply()
}

/// Whether an `INFO [section]` reply will INCLUDE the `# Keyspace` section (#531), so the router
/// only pays the cross-shard keyspace gather when the client will actually see it. This mirrors
/// `ironcache_observe::build_info`'s section `want` gate EXACTLY: the keyspace section renders for a
/// bare `INFO` (no section) or a section of `default` / `all` / `everything` / `keyspace` (case-
/// insensitive). `INFO server` / `INFO stats` / etc. do NOT include it, so they skip the fan-out.
fn info_reply_includes_keyspace(request: &Request) -> bool {
    match request.args.get(1) {
        None => true,
        Some(section) => {
            let s = String::from_utf8_lossy(section).to_ascii_lowercase();
            s == "default" || s == "all" || s == "everything" || s == "keyspace"
        }
    }
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
    // #531: the NODE-WIDE INFO `# Keyspace` lines (per-db counts summed across every shard), or
    // `None` to fall back to THIS shard's local `db_len`. The router gathers it (via a whole-
    // keyspace fan-out) ONLY for an INFO whose reply includes the keyspace section on a >1-shard
    // node; every other command and the single-shard node pass `None` (byte-identical). Borrowed
    // from the router's stack for the duration of the synchronous dispatch.
    node_keyspace: Option<&[ironcache_observe::KeyspaceDbLine]>,
    out: &mut Vec<u8>,
) -> bool {
    // #511 GET-BY-REFERENCE HOME FAST PATH (Dragonfly GET-gap, root cause #2). A plain 2-arg `GET`
    // served on its OWN (home) shard is answered by encoding the RESP bulk string DIRECTLY from the
    // stored value bytes into `out`, DROPPING the per-GET `Bytes::copy_from_slice` + the
    // `Value::BulkString` allocation that `cmd_get` builds (the value is written store->`out` in ONE
    // copy, ZERO heap alloc). Everything else -- a wrong-arity `GET`, every other command, and the
    // cross-shard HOP path (whose reply must be an OWNED, `Send` `Value` crossing the coordinator
    // channel, so it KEEPS `cmd_get`) -- falls through to the UNCHANGED `dispatch_with_cmd` below.
    // This is a home-path-only diversion of the reply ENCODING; the router already ran the auth /
    // subscribe-mode gates (a GET reaching here is authenticated and is not a blocked subscriber
    // command), so bypassing dispatch's redundant backstop gates is safe. The `!conn.in_multi`
    // guard is REQUIRED: `route_in_multi` also funnels through `handle_request` and relies on
    // dispatch's QUEUE GATE to stage a GET inside a transaction as `+QUEUED` (NOT execute it), so an
    // in-MULTI GET must fall through to `dispatch_with_cmd` below; only the LIVE (non-queued) home
    // GET takes the by-ref fast path. A queued GET is replayed by `EXEC` through `dispatch`
    // (`cmd_get`), so its reply bytes are unchanged.
    if cmd_upper == b"GET" && request.args.len() == 2 && !conn.in_multi {
        return get_home_by_ref(ctx, conn, env, store_rc, state_rc, &request.args[1], out);
    }
    state_rc.borrow_mut().counters.on_command();
    // INFO ROLLUP (#531): the `# Stats`/`# Clients` counters are the NODE-WIDE sum, not this
    // serving shard's ~1/N view. The metrics registry is always present now (built at boot even
    // with `/metrics` off), and every shard's `ShardCounters` mutate their registered cell, so
    // `aggregate()` folds EVERY shard into one snapshot -- invariant to which shard homed this
    // connection, and consistent with DBSIZE / `/metrics`. The serving-shard snapshot is the
    // defensive fallback for a registry-absent `ServerContext` (unit tests that build one bare); in
    // the binary the registry is always `Some`, so the node-wide arm is always taken. The closure
    // is invoked ONLY by INFO (inside dispatch); the aggregate arm borrows nothing of `state_rc`,
    // and the fallback arm's `state_rc.borrow()` runs sequentially with (never aliasing) dispatch's
    // later mutable borrow, exactly as before.
    let snapshot_fn = || {
        ctx.metrics_registry.as_ref().map_or_else(
            || state_rc.borrow().counters.snapshot(),
            ironcache_observe::MetricsRegistry::aggregate,
        )
    };
    let rollup: &dyn Fn() -> CounterSnapshot = &snapshot_fn;
    // #531: the node-wide INFO keyspace source. `Some` slice -> yield the fanned-out per-db lines;
    // `None` -> `cmd_info` falls back to this shard's local `db_len` (single-shard / non-INFO).
    let keyspace_fn = || node_keyspace.map(<[_]>::to_vec);
    let keyspace: ironcache_server::KeyspaceFn<'_> = &keyspace_fn;
    // COMMANDSTATS / ERRORSTATS render (#413): render the serving shard's per-command + per-error
    // tables into the INFO section bodies. Invoked ONLY when INFO asks for those sections (the
    // closure is not called otherwise), and it borrows `state_rc` immutably like `rollup` does
    // (sequentially, never aliasing dispatch's later mutable borrow).
    let cmdstats_fn = || {
        let (mut cs, mut es) = (String::new(), String::new());
        // COMMANDSTATS node-wide (#527): sum EVERY shard's per-command atomic table via the registry
        // (the SAME cross-shard rollup #545 uses for `# Stats`) and render node-wide `cmdstat_<cmd>`
        // lines -- invariant to which shard homed this connection. The registry is always present in
        // the binary; a bare unit-test context without one renders an empty body (it records no
        // per-command stats either), byte-identical to the pre-#527 empty-closure fallback.
        if let Some(reg) = ctx.metrics_registry.as_ref() {
            ironcache_server::render_commandstats_agg(&mut cs, &reg.aggregate_command_stats());
        }
        // ERRORSTATS stays serving-shard-scoped (#527 follow-up): render THIS shard's local error
        // table (single-shard nodes see the whole node; multi-shard error aggregation is the
        // remaining smaller follow-up).
        state_rc.borrow().command_stats.render_errorstats(&mut es);
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
            keyspace,
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
    let reset_stats = deltas.reset_stats;
    {
        deltas.expired += lazy_expired;
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
    // CONFIG RESETSTAT NODE-WIDE (#531): `apply` above zeroed only THIS serving shard's cell, but
    // INFO now reports the node-wide rollup (every shard's cell summed via `aggregate()`), so a
    // reset must fan across EVERY shard's cell or a sibling shard's stale totals would survive in
    // the rollup. The registry is always present in the binary; the reset is a handful of relaxed
    // atomic stores per cell (RESETSTAT is a rare admin command, never on the data hot path).
    if reset_stats {
        if let Some(registry) = ctx.metrics_registry.as_ref() {
            registry.reset_stats();
        }
    }
    encode_into(out, &reply, conn.proto);
    conn.should_close
}

/// The per-shard-thread ZERO-COPY GET sink (#515 P4c). See [`ZC_SINK`] / [`push_zc_bulk`].
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
#[derive(Default)]
struct ZcSink {
    /// The ordered value SPLICES (offset in `out` + pinned `(ptr, len)`) for this batch's flush.
    inserts: Vec<ironcache_runtime::ZcInsert>,
    /// The frozen-slot handles (type-erased [`ironcache_store::ZcPin`]s) backing those splices; the
    /// io_uring `send_zc` takes ownership of these until its CQE so the bytes outlive the write.
    pins: Vec<Box<dyn core::any::Any>>,
}

#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
thread_local! {
    /// ZERO-COPY GET sink for THIS shard thread (#515 P4c). The io_uring serve loop
    /// ([`serve_connection_generic`]) installs one (`Some`) on its shard thread; [`get_home_by_ref`]
    /// pushes a large String hit's frozen-value pin + splice offset here INSTEAD of copying the bytes
    /// into `out`, and the loop DRAINS it (via [`drain_zc_sink`]) into the flush's `zc_inserts`/
    /// `zc_pins` immediately after every `route_and_dispatch` returns.
    ///
    /// SOUNDNESS of the shared thread-local across the connections multiplexed on this shard thread:
    /// the ONLY pusher ([`get_home_by_ref`]) is fully SYNCHRONOUS, and `route_and_dispatch`'s
    /// home-GET path has NO `.await` between that push and the loop's drain (the post-dispatch
    /// blocking-wake + keyspace-publish are both sync, and a GET wakes/publishes nothing). So the sink
    /// is always drained back to empty before the loop's next yield -- no other connection can ever
    /// observe another's pins at an await boundary. The TOKIO serve loop never installs a sink, so
    /// `get_home_by_ref` there finds `None` and copies via `encode_bulk_ref` (byte-identical to #511).
    static ZC_SINK: core::cell::RefCell<Option<ZcSink>> = const { core::cell::RefCell::new(None) };
}

/// Is a zero-copy GET sink installed on THIS thread (i.e. are we on an io_uring serve loop)? A single
/// thread-local borrow + `is_some`. On a non-io_uring build there is no sink type, so this is a
/// compile-time `false` and every GET takes the copy path.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
fn zc_sink_active() -> bool {
    ZC_SINK.with(|c| c.borrow().is_some())
}
#[cfg(not(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
)))]
const fn zc_sink_active() -> bool {
    false
}

/// PIN a present large String value for a zero-copy send and frame it into `out` (#515 P4c). Called
/// ONLY after [`zc_sink_active`] returned true and the by-ref classify saw a String hit at/above the
/// live `zero-copy-get-threshold`, so the sink IS installed and the key IS live (same synchronous `borrow_mut`
/// scope, no await, no other code interleaves). Frames `$<len>\r\n` then the SPLICE POINT then `\r\n`
/// -- the value bytes are NOT copied into `out`; the send interleaves them from the pin at that
/// offset. Returns `true` on success. Returns `false` (leaving `out` UNTOUCHED) only in the
/// unreachable-in-practice case that the re-probe misses or the sink vanished, so the caller can fall
/// back to a copy and never desync the reply.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
fn push_zc_bulk(
    store: &ShardStoreImpl,
    db: u32,
    key: &[u8],
    now: UnixMillis,
    out: &mut Vec<u8>,
) -> bool {
    // Re-probe under the SAME `borrow_mut` to obtain the slot-`Arc`-backed pin. `pin_value_frozen`
    // holds a clone of the value's slot `Arc`, so any later (or concurrent) write to that key COWs
    // the live slot and this frozen clone keeps the ORIGINAL bytes valid + immutable until dropped
    // (the #576 mechanism) -- no fence, no copy. `None` is unreachable here (we just read the key as a
    // live String in this same scope), so a `None` cleanly declines to the caller's copy fallback.
    let Some(pin) = store.pin_value_frozen(db, key, now) else {
        return false;
    };
    ZC_SINK.with(|c| {
        let mut g = c.borrow_mut();
        let Some(sink) = g.as_mut() else {
            // Unreachable (the caller checked `zc_sink_active`, and nothing uninstalls the sink);
            // decline WITHOUT having written the header, so `out` is pristine for the copy fallback.
            return false;
        };
        // Header, then the splice offset (`at` = where the value logically goes: after `out[..at]`,
        // before the trailing CRLF), then the trailing CRLF. `send_zc` splices the pinned bytes at
        // `at`, reproducing exactly `encode_bulk_ref`'s `$<len>\r\n<bytes>\r\n` on the wire.
        ironcache_protocol::encode_bulk_len_prefix(out, pin.len());
        let at = out.len();
        sink.inserts.push(ironcache_runtime::ZcInsert {
            at,
            ptr: pin.as_ptr(),
            len: pin.len(),
        });
        sink.pins.push(Box::new(pin));
        out.extend_from_slice(b"\r\n");
        true
    })
}
#[cfg(not(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
)))]
fn push_zc_bulk(_: &ShardStoreImpl, _: u32, _: &[u8], _: UnixMillis, _: &mut Vec<u8>) -> bool {
    // No io_uring send_zc on this target; `zc_sink_active()` is a const `false`, so this is never
    // reached. Present only so `get_home_by_ref` compiles identically across targets.
    false
}

/// DRAIN this shard thread's zero-copy sink into the flush's insert/pin lists (#515 P4c). Called by
/// the io_uring serve loop immediately after each `route_and_dispatch` returns -- a window with NO
/// `.await`, so the sink holds exactly THIS command's splices (a home GET may have pushed one) and no
/// other multiplexed connection can have raced it (see [`ZC_SINK`]). Moves the elements out (leaving
/// the sink empty for the next command); a no-op fast path when the command pinned nothing.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
fn drain_zc_sink(
    inserts: &mut Vec<ironcache_runtime::ZcInsert>,
    pins: &mut Vec<Box<dyn core::any::Any>>,
) {
    ZC_SINK.with(|c| {
        if let Some(sink) = c.borrow_mut().as_mut() {
            if !sink.inserts.is_empty() {
                inserts.append(&mut sink.inserts);
                pins.append(&mut sink.pins);
            }
        }
    });
}

/// #511 GET-BY-REFERENCE HOME FAST PATH. Answer a plain 2-arg `GET` served on its home shard by
/// framing the RESP bulk string DIRECTLY from the stored value bytes into `out`, dropping the
/// `Bytes::copy_from_slice` + `Value::BulkString` allocation `cmd_get` pays (root cause #2 of the
/// Dragonfly GET gap). The value bytes are written store->`out` in a SINGLE copy with ZERO heap
/// allocation; the cross-shard HOP path is untouched (it still returns an owned `Value` via
/// `cmd_get`, which must be `Send` to cross the coordinator channel -- a borrow cannot hop).
///
/// BORROW SAFETY. `store.read` returns a `ValueRef` that BORROWS the shard store (the value bytes
/// are a `&[u8]` into the stored buffer, the #519 single-probe read). That borrow is CONSUMED here,
/// INSIDE the `store` borrow scope, before it is released: `encode_bulk_ref` copies the bytes into
/// the SEPARATE `out` buffer immediately, so the value ref can NEVER outlive the store borrow. A
/// `GET` does not mutate the value; the only write `read` performs is the in-object S3-FIFO freq
/// bump, which happens BEFORE the bytes are handed back (single `find_mut`), so the byte slice we
/// encode cannot alias a concurrent mutation or a freed entry.
///
/// PARITY WITH `dispatch`. The command counter, the `keyspace_hits`/`keyspace_misses` fold, the
/// per-command notify-flag snapshot (so a lazy-TTL `expired` event fired by `store.read` reads the
/// CURRENT flags), and the lazy-expiry `expired_keys` drain are all reproduced so INFO + keyspace
/// notifications stay identical. The WRONGTYPE and NULL replies reuse the EXACT `Value`s `cmd_get`
/// returns (byte-identical). The router's post-dispatch wake + keyspace-event publish still run on
/// the returned `close` flag, exactly as for the general path. The active timing-wheel drain and
/// the rare maxmemory-policy hot-swap check are deliberately NOT reproduced here: neither affects a
/// GET's reply bytes (they reap OTHER keys / swap the eviction policy), both are still driven by
/// every non-GET command and the background reap timer, and skipping them on a read matches Redis's
/// access-does-not-active-expire behavior. Kept as one self-contained fn so the change is trivially
/// reversible (delete the top-of-`handle_request` branch + this fn) and cannot drift the hot path.
fn get_home_by_ref(
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    state_rc: &Rc<RefCell<ShardState>>,
    key: &[u8],
    out: &mut Vec<u8>,
) -> bool {
    // The `Store` waist trait, in scope so `read` (the by-ref accessor) resolves on the concrete
    // `ShardStoreImpl`. Local like the other inner-scope waist-trait uses in this module.
    use ironcache_storage::{DataType, Store};

    state_rc.borrow_mut().counters.on_command();
    let now = UnixMillis(env.borrow().now_unix_millis());
    // Snapshot the live `notify-keyspace-events` flags into this shard's per-command emit gate,
    // EXACTLY as `dispatch_with_cmd` does, so a keyspace `expired` event fired by the lazy TTL
    // backstop inside `store.read` below reads the CURRENT flags (not the previous command's). One
    // relaxed atomic load + a thread-local `Cell` write; a no-op when notifications are disabled.
    ironcache_config::notify::set_command_flags(ctx.runtime.notify_flags());

    let mut deltas = CounterDeltas::default();
    let lazy_expired;
    {
        let mut store = store_rc.borrow_mut();
        // Classify the read; for the COPY path frame the reply inline from the borrowed value bytes.
        // For the #515 ZERO-COPY path we only DECIDE here (`true`): the value is pinned AFTER `v`'s
        // borrow of `store` ends (below), because `pin_value_frozen` also borrows `store` (`&self`)
        // and must not overlap the `read` borrow.
        let defer_zc = match store.read(conn.db, key, now) {
            Some(v) if v.data_type() == DataType::String => {
                deltas.keyspace_hits += 1;
                let bytes = v.as_bytes();
                // #515 ZERO-COPY GET: a value at/above the live `zero-copy-get-threshold` on the
                // io_uring serve loop is SPLICED into the socket write straight from the store -- its
                // bytes are NEVER copied into `out`. A smaller value, a `0` threshold (zero-copy
                // disabled), or any value on the tokio loop (`zc_sink_active()` is a const `false` off
                // io_uring), takes the by-ref COPY fast path (#511): frame `$<len>\r\n<bytes>\r\n`
                // straight from the stored bytes into `out` -- no `Bytes::copy_from_slice`, no
                // `Value::BulkString`. `out` is a distinct buffer from `store`, so `v.as_bytes()` (a
                // borrow into the store) and `&mut out` do not alias; the borrow ends at the arm
                // boundary, inside the store scope. The threshold is one relaxed atomic load per home
                // GET (config-tunable, hot-reloadable via `CONFIG SET zero-copy-get-threshold`).
                let zc_threshold = ctx.runtime.zero_copy_get_threshold();
                if zc_threshold != 0 && bytes.len() as u64 >= zc_threshold && zc_sink_active() {
                    true
                } else {
                    ironcache_protocol::encode_bulk_ref(out, bytes);
                    false
                }
            }
            Some(_) => {
                // A non-string key: byte-identical to `cmd_get`'s WRONGTYPE reply. Rare, off the hot
                // path. Neither a hit nor a miss (mirrors `keyspace_counted`).
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_type()),
                    conn.proto,
                );
                false
            }
            None => {
                // Missing OR lazily-expired: the null reply (`$-1` RESP2 / `_` RESP3), a keyspace
                // MISS. Byte-identical to `cmd_get`'s `Value::Null`.
                encode_into(out, &ironcache_server::Value::Null, conn.proto);
                deltas.keyspace_misses += 1;
                false
            }
        };
        // `v` (and its borrow of `store`) is now dropped, so `store` is free to pin. Frame the large
        // value's reply via the zero-copy splice. `push_zc_bulk` re-probes under this SAME synchronous
        // `borrow_mut` (no await, nothing else runs), so it is guaranteed to find the still-live key;
        // its `false` return is the unreachable-in-practice defensive fallback (re-read + copy) so a
        // missed pin can never desync the reply.
        if defer_zc && !push_zc_bulk(&store, conn.db, key, now, out) {
            match store.read(conn.db, key, now) {
                Some(v) if v.data_type() == DataType::String => {
                    ironcache_protocol::encode_bulk_ref(out, v.as_bytes());
                }
                _ => encode_into(out, &ironcache_server::Value::Null, conn.proto),
            }
        }
        // Drain the lazy-backstop expiry count `store.read` may have produced (a GET of an expired
        // key reaps it), inside the store scope, exactly as `handle_request` does after dispatch.
        lazy_expired = store.take_lazy_expired();
    }

    // Fold this command's keyspace hit/miss + lazy-expiry count into the shard's counters for INFO.
    // A cheap no-op on the common hit path with notifications/TTLs quiescent.
    deltas.expired += lazy_expired;
    if deltas != CounterDeltas::default() {
        state_rc.borrow_mut().counters.apply(deltas);
    }
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
pub(crate) fn record_command_stats(
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out_before: usize,
    out: &[u8],
    elapsed_us: u64,
) {
    // LATENCY HISTOGRAM (#546): record this command's elapsed micros into the shard's per-shard
    // histogram FIRST, before the `spec_of` gate below early-returns for an unknown command. The
    // histogram is the operator-facing tail-latency view (p99/p99.9 graphable from `/metrics`), so
    // it must count EVERY command that reached the timing site -- known or not -- to stay consistent
    // with the commands-processed total. This is the single per-command choke point every serve path
    // funnels through (the tokio loop, the io_uring loop, and the deferred-hop drain all call it with
    // the SAME reused SLOWLOG/COMMANDSTATS elapsed), so recording here covers them all with no new
    // clock read: a branchless find-bucket + one relaxed atomic increment (see `observe_latency`).
    state_rc.borrow().counters.observe_latency(elapsed_us);
    let cmd_upper = ascii_upper(request.command());
    let Some(spec) = ironcache_server::spec_of(&cmd_upper) else {
        return;
    };
    // The reply for THIS command begins at `out_before`; a leading `-` is an error reply (the
    // command ran but failed). A push/array/status/integer/bulk lead byte is a success.
    let failed = out.get(out_before) == Some(&b'-');
    // COMMANDSTATS node-wide (#527): record this command's calls/usec/failed into THIS shard's
    // per-command atomic slot in the metrics registry (indexed by the command's STABLE ordinal), so
    // INFO COMMANDSTATS sums it across shards via `aggregate_command_stats` -- the per-command analog
    // of the #545 `# Stats` rollup. ONE relaxed atomic add, no lock, no allocation (the slot is
    // pre-allocated). Only registry commands reach here (the `spec_of` gate above), so the ordinal is
    // always present; a defensive miss is a no-op.
    if let Some(index) = ironcache_server::command_stat_index(spec.name) {
        state_rc
            .borrow()
            .counters
            .on_command_stat(index, elapsed_us, failed);
    }
    if failed {
        // The error CODE: the first whitespace/CR-delimited token after the `-`. ERRORSTATS stays
        // serving-shard-scoped (#527 follow-up): record it into THIS shard's local error table for
        // the `errorstat_*` section (cross-shard error aggregation is the remaining smaller follow-up).
        let code_start = out_before + 1;
        let rest = &out[code_start..];
        let code_len = rest
            .iter()
            .position(|&b| b == b' ' || b == b'\r')
            .unwrap_or(rest.len());
        state_rc
            .borrow_mut()
            .command_stats
            .record_error(&rest[..code_len]);
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
pub(crate) fn consume_caching_flag(conn: &mut ConnState, request: &Request) {
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

pub(crate) fn apply_client_tracking(
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
pub(crate) fn record_hotkeys(
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

pub(crate) fn record_slow_command(
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

/// Wait for a shutdown signal (SIGINT/SIGTERM) and then stop the shard set.
///
/// Signal handling lives in the binary only (CLI_BINARY.md): the library crates
/// never touch raw signals, preserving the determinism boundary. We use a small
/// blocking wait on a self-pipe-free `libc::sigwait`-style loop via tokio's signal
/// support on the main thread.
pub fn install_shutdown(set: &ShardSet) -> Arc<std::sync::atomic::AtomicBool> {
    set.shutdown_flag()
}

/// Re-read the configured cert/key PEM and, if VALID, atomically swap it into the live client
/// listener (#563 hot TLS cert reload). The SIGHUP handler calls this; a test can call it DIRECTLY
/// (the signal handler is a thin wrapper over it) to exercise the reload hermetically.
///
/// This is the FAIL-SAFE point: on a bad/missing/mismatched replacement the error is returned and
/// the PREVIOUS good config stays live (the listener is never torn down). Only the CLIENT listener
/// is reloaded; the intra-cluster bus/repl TLS reload is a documented follow-up.
///
/// # Errors
///
/// Returns [`ironcache_runtime::TlsConfigError`] if the new material cannot be read/parsed or rustls
/// rejects it; in that case NO swap happens and existing TLS keeps working.
pub fn reload_client_tls(
    handle: &TlsReloadHandle,
) -> Result<(), ironcache_runtime::TlsConfigError> {
    handle
        .acceptor
        .reload_from_paths(&handle.cert_path, &handle.key_path)
}

/// Install the SIGHUP hot cert-reload handler (#563) over the client-listener [`TlsReloadHandle`].
///
/// On each SIGHUP the handler re-reads the SAME configured cert/key paths and, via
/// [`reload_client_tls`], atomically swaps the new material in so SUBSEQUENT handshakes present the
/// fresh cert -- with no restart and no dropped existing connections (rustls config is per-handshake,
/// so in-flight connections keep theirs). A bad replacement is LOGGED and REJECTED, keeping the
/// previous good cert live. SIGHUP is the conventional cert/config-reload signal (nginx/haproxy),
/// and the handler LOOPS so repeated rotations each take effect.
///
/// Runs on a DEDICATED thread with its own current-thread runtime (mirroring the force-stop watcher)
/// so it handles SIGHUP CONCURRENTLY while the main thread blocks in [`wait_for_signal`] awaiting
/// SIGINT/SIGTERM. Signal handling lives in the binary only (CLI_BINARY.md), and the reload is a
/// boot/ops-path file read + atomic pointer swap, outside the determinism boundary (ADR-0003).
/// A no-op off unix (SIGHUP is a unix signal).
pub fn spawn_tls_reload_on_sighup(handle: TlsReloadHandle) {
    #[cfg(unix)]
    {
        let _ = std::thread::Builder::new()
            .name("ironcache-tls-reload".to_string())
            .spawn(move || {
                let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                rt.block_on(async move {
                    use tokio::signal::unix::{SignalKind, signal};
                    let Ok(mut sighup) = signal(SignalKind::hangup()) else {
                        tracing::warn!(
                            "ironcache: could not install the SIGHUP TLS-reload handler; \
                             cert rotation still requires a restart"
                        );
                        return;
                    };
                    tracing::info!(
                        cert = %handle.cert_path,
                        key = %handle.key_path,
                        "ironcache: SIGHUP TLS cert reload armed (send SIGHUP after replacing the cert/key)"
                    );
                    // Loop so repeated SIGHUPs each rotate the cert. `recv` returns `None` only when
                    // the signal stream is torn down (process exit); stop watching then.
                    while sighup.recv().await.is_some() {
                        match reload_client_tls(&handle) {
                            Ok(()) => tracing::info!(
                                cert = %handle.cert_path,
                                key = %handle.key_path,
                                "ironcache: TLS certificate reloaded on SIGHUP; new handshakes present the new cert (existing connections undisturbed)"
                            ),
                            Err(e) => tracing::error!(
                                error = %e,
                                cert = %handle.cert_path,
                                key = %handle.key_path,
                                "ironcache: TLS cert reload FAILED on SIGHUP; keeping the previous certificate"
                            ),
                        }
                    }
                });
            });
    }
    #[cfg(not(unix))]
    {
        let _ = handle;
    }
}

#[cfg(test)]
#[path = "serve_tests.rs"]
mod tests;
