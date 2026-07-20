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
use ironcache_env::{Env, Rng, SystemEnv};
use ironcache_eviction::Policy;
use ironcache_observe::{ServerInfo, ShardCounters};
use ironcache_runtime::TokioRuntime;
use ironcache_runtime::bootstrap::{ShardConfig, ShardId, ShardSet};
use ironcache_server::ProtoVersion;
use ironcache_server::dispatch::ServerContext;
use ironcache_storage::CountingAccounting;
use ironcache_store::ShardStore;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
// `Rc` / `RefCell` / `TimingWheel` are used only by the `serve_tests.rs` unit tests (via
// `use super::*`) now that the serve loops + router moved out of this file (#625), so their imports
// are `#[cfg(test)]`.
#[cfg(test)]
use ironcache_server::{ConnState, Request, TimingWheel, UnixMillis, route};
#[cfg(test)]
use std::cell::RefCell;
#[cfg(test)]
use std::rc::Rc;

// Cohesive sub-modules split out of this file (#625). Each is a behavior-preserving relocation of a
// self-contained group of items; the `use` re-exports below keep every call site in `serve` +
// `serve_tests` (which does `use super::*`) + `main.rs` (`serve::wait_for_signal` /
// `serve::SignalOutcome`) resolving exactly as before.
#[path = "serve_admin_cmd.rs"]
mod serve_admin_cmd;
#[path = "serve_classify.rs"]
mod serve_classify;
#[path = "serve_cluster_cmd.rs"]
mod serve_cluster_cmd;
#[path = "serve_conn_loop.rs"]
mod serve_conn_loop;
#[path = "serve_home_dispatch.rs"]
mod serve_home_dispatch;
#[path = "serve_hooks.rs"]
mod serve_hooks;
#[path = "serve_hop.rs"]
mod serve_hop;
#[path = "serve_pubsub_cmd.rs"]
mod serve_pubsub_cmd;
#[path = "serve_routing.rs"]
mod serve_routing;
#[path = "serve_shard_state.rs"]
mod serve_shard_state;
#[path = "serve_signal.rs"]
mod serve_signal;
#[path = "serve_txn_block.rs"]
mod serve_txn_block;
#[path = "serve_util.rs"]
mod serve_util;

use serve_hop::{DeferredHop, drain_deferred_hops};
// #625: the per-connection serve loops. `serve_connection` is spawned by `run_server_observed`
// (below); `subscriber_gate_blocks` is used by the router. The io_uring loop wrappers are cfg-gated.
#[cfg(all(target_os = "linux", feature = "io_uring_raw"))]
pub(crate) use serve_conn_loop::serve_connection_raw_uring;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub(crate) use serve_conn_loop::serve_connection_uring;
pub(crate) use serve_conn_loop::{serve_connection, subscriber_gate_blocks};
// #625: the serve-layer router. `pub(crate)` re-export so the serve loops (route_and_dispatch +
// pause_stall), the coordinator drain loop (wake_blocking_waiters_for_shard), and sibling modules
// (replica_read_in_sync / MigrationCtx used by serve_txn_block) resolve them unchanged.
pub(crate) use serve_routing::{
    MigrationCtx, cluster_redirect, pause_stall, publish_pending_keyspace_events,
    replica_read_in_sync, route_and_dispatch, shard_owner_announce_host, shard_owner_home,
    wake_blocking_waiters_for_shard,
};
// These router helpers are referenced ONLY by the routing unit tests in `serve_tests.rs` (via
// `use super::*`), so their re-exports are `#[cfg(test)]`.
#[cfg(test)]
pub(crate) use serve_routing::{
    consume_one_shot_asking, redirect_for_keys, request_is_client_unpause, write_guardrail,
    write_guardrail_decision, xshard_presence_keys,
};
// #625: MULTI queue-time routing + blocking/WAIT park + spanning-combine dispatch. `pub(crate)`
// re-export of the entrypoints so route_and_dispatch + the serve loops resolve them unchanged.
pub(crate) use serve_txn_block::{
    BlockPark, dispatch_spanning_combine, handle_blocking_live, route_in_multi, run_block_park,
};
// `block_timeout_value` is used ONLY by the io_uring serve loop's FIX1 immediate-reply path (the
// tokio loop truly parks), so its re-export is cfg-gated to the io_uring datapath.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
pub(crate) use serve_txn_block::block_timeout_value;
// #625: the home-shard command dispatch (handle_request + get_home_by_ref + the #515 zero-copy sink).
// `handle_request` + the INFO-keyspace gate re-export to the router; the zero-copy sink type/
// thread-local/drain are used ONLY by the io_uring serve loop, so those re-exports are cfg-gated.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
pub(crate) use serve_home_dispatch::{ZC_SINK, ZcSink, drain_zc_sink};
pub(crate) use serve_home_dispatch::{handle_request, info_reply_includes_keyspace};
// #625: the admin (persist/ACL/shutdown) + raft-cluster mutator command handlers. `pub(crate)`
// re-export of the route_and_dispatch entrypoints so the router resolves them unchanged.
pub(crate) use serve_admin_cmd::{
    handle_acl_command, handle_persist_command, handle_shutdown_command,
};
pub(crate) use serve_cluster_cmd::try_raft_cluster_mutator;
// These cluster builders/parsers are referenced ONLY by the cluster unit tests in `serve_tests.rs`
// (via `use super::*`), so their re-exports are `#[cfg(test)]` (mirrors the serve_shard_state /
// serve_signal test-only re-exports).
#[cfg(test)]
pub(crate) use serve_cluster_cmd::{
    build_failover, build_flushslots, build_unassign, is_valid_node_id, learn_or_synth_meet_id,
    parse_addslots_slots, parse_addslotsrange_slots, rebalance_apply_cmds, synth_meet_node_id,
};
// #625: the serve-layer Pub/Sub command handlers. `pub(crate)` re-export of the two entrypoints so
// route_and_dispatch (interception) + the connection close path resolve them unchanged.
pub(crate) use serve_pubsub_cmd::{deregister_all_subscriptions, try_handle_pubsub};
// #625: the per-command observability + client-tracking hooks (COMMANDSTATS/LATENCY, CLIENT
// TRACKING, HOTKEYS, SLOWLOG) + the close-path tracking purge. `pub(crate)` re-export so the serve
// loops resolve them unchanged.
pub(crate) use serve_hooks::{
    apply_client_tracking, consume_caching_flag, purge_conn_tracking, record_command_stats,
    record_hotkeys, record_slow_command,
};
// #625: the routing/dispatch classification predicates + internal-verb/spanning-move reject encoders.
// `pub(crate)` re-export so the spine still in `serve` (route_and_dispatch, handle_request) resolves
// them unchanged.
pub(crate) use serve_classify::{
    all_keys_home_owned, is_fan_out_multikey, is_fan_out_spanning_combine,
    is_fan_out_spanning_move, is_fan_out_spanning_zset, is_serve_pubsub_command,
    is_spanning_move_reject, reject_internal_verb, reject_spanning_move,
};
// #625: the per-shard core-local state accessors + lifecycle flags. `pub(crate)` re-export so the
// spine still in `serve` (serve loops, routing), every sibling submodule, and external callers
// (`crate::coordinator`, `crate::replica_attach`, `crate::upgrade`, INFO) resolve them unchanged.
pub(crate) use serve_shard_state::{
    STORE_SLOTS_PER_DB, TRACKING, adopt_metrics_cell, adopt_process_memory_gauge,
    ensure_shard_ring, ensure_shard_started, fresh_shard_store, install_receiver_flip_barrier,
    is_serving, is_shard_loading, quiesce_shard, report_receiver_shard_committed,
    scan_reserved_bits, set_replica_passive, set_serving, shard_blocking, shard_env, shard_pubsub,
    shard_started_at, shard_state, shard_store, shard_tracking, shard_wheel, stash_shard_ring,
    unquiesce_shard,
};
// These shard-state items are referenced ONLY by the shard-state unit tests in `serve_tests.rs`
// (via `use super::*`), so their re-exports/imports are `#[cfg(test)]` (mirrors the serve_signal
// test-only re-exports below): the loading/ring setters + the expiry-tick internals + the reap
// interval the expiry test drives.
#[cfg(test)]
use ironcache_server::EXPIRE_CYCLE_INTERVAL;
#[cfg(test)]
pub(crate) use serve_shard_state::{
    expire_cycle_tick, set_shard_loading, shard_ring, spawn_expire_task,
};
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
