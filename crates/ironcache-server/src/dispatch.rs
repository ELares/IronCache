// SPDX-License-Identifier: MIT OR Apache-2.0
//! Tier-0 command dispatch (COMMANDS.md, PROTOCOL.md "Tier 0 connection
//! commands"). Maps a parsed [`Request`] to a [`Value`] reply, mutating the
//! per-connection [`ConnState`] where a command does (HELLO, SELECT, RESET,
//! CLIENT SETNAME, AUTH, QUIT).
//!
//! Dispatch is case-insensitive on the command token. Unknown commands return the
//! verbatim `ERR unknown command '...'` from the catalog. PR-1 implements only the
//! handshake/connection tier; data commands (GET/SET/...) arrive with the store
//! in PR-2.

use crate::admission::is_denyoom;
use crate::conn::ConnState;
use crate::{
    cmd_bitmap, cmd_cluster, cmd_config, cmd_dump, cmd_expire, cmd_hash, cmd_hll, cmd_introspect,
    cmd_keyspace, cmd_list, cmd_set, cmd_sort, cmd_string, cmd_txn, cmd_zset, command_spec,
};
use ironcache_config::{ClusterMode, Config, RuntimeConfig};
use ironcache_env::{Clock, Env, Rng};
use ironcache_expiry::TimingWheel;
use ironcache_observe::{CounterDeltas, CounterSnapshot, KeyspaceDbLine, MemoryInfo, ServerInfo};
use ironcache_protocol::{ErrorReply, ProtoVersion, Request, Value};
use ironcache_storage::{ActiveExpiry, Admit, Keyspace, PolicySwap, Store, UnixMillis, Watch};
use std::sync::Arc;

// #625: the SLOWLOG/HOTKEYS/MEMORY/LATENCY/INFO introspection command handlers, relocated verbatim.
// `dispatch_inner` (still here) calls these, so they are re-imported into this module's scope.
#[path = "dispatch_introspect.rs"]
mod dispatch_introspect;
use dispatch_introspect::{cmd_hotkeys, cmd_info, cmd_latency, cmd_memory, cmd_slowlog};

/// The bounded number of expired keys the active timing-wheel drain reclaims per
/// command (EXPIRATION.md "bounded reclamation"). A small cap keeps the drain off the
/// command path's critical section: a flood of co-expiring keys is reclaimed across
/// several commands rather than stalling one. The lazy backstop still prevents
/// OBSERVING an expired key, so this bound only governs how fast resident memory for
/// expired keys returns, never correctness.
///
/// This is the cap for the OPPORTUNISTIC (per-command) drain. The PR-3c background
/// timer task for IDLE shards calls the SAME [`drain_due_keys`] helper with its own
/// per-cycle cap ([`crate::MAX_RECLAIM_PER_CYCLE`]); both paths share the one bounded
/// drain so there is no duplicate reclamation logic (EXPIRATION.md idle-shard memory
/// boundedness).
pub const MAX_RECLAIM_PER_CALL: usize = 20;

/// The bounded number of expired keys the PR-3c per-shard BACKGROUND timer task
/// reclaims per cycle (EXPIRATION.md idle-shard memory boundedness). The timer task is
/// what keeps an IDLE shard's resident memory bounded: an opportunistic
/// (per-command) drain only fires when a command arrives, so a shard with no traffic
/// would otherwise accumulate expired-but-not-reclaimed values until the next command.
/// The per-cycle cap is larger than [`MAX_RECLAIM_PER_CALL`] (the background task is
/// off the command critical section, so it may reclaim more aggressively per cycle),
/// but still bounded so one cycle never monopolizes the shard's single thread. It is a
/// #8-tunable internal default, not a wire-exposed knob.
pub const MAX_RECLAIM_PER_CYCLE: usize = 100;

/// The interval between background active-expiry cycles on each shard (the Redis `hz`
/// analog, EXPIRATION.md). The PR-3c timer task awaits `Runtime::timer(EXPIRE_CYCLE_INTERVAL)`
/// then drains a bounded batch, so an idle shard reclaims expired memory roughly every
/// interval even with no traffic. 100ms matches the timing-wheel bottom-level
/// resolution ([`ironcache_expiry::TICK_MS`]), so the active drain keeps pace with the
/// finest deadline bucket. A #8-tunable internal default, not a wire knob; the timer
/// FIRING schedule is wall-clock and does NOT affect observable behavior (the lazy
/// backstop guarantees no expired key is ever observed regardless of when cleanup runs).
pub const EXPIRE_CYCLE_INTERVAL: core::time::Duration = core::time::Duration::from_millis(100);

/// Drain a BOUNDED batch of due keys from the timing `wheel` at `now` and reap the
/// ones whose stored deadline has actually passed (EXPIRATION.md active reclamation).
/// Returns the number of keys ACTUALLY reaped (the `expired_keys` contribution).
///
/// This is the SINGLE bounded-drain helper SHARED by both active-reclamation paths
/// (EXPIRATION.md "runs on the owning core"):
/// - the OPPORTUNISTIC per-command drain in [`dispatch`] (cap [`MAX_RECLAIM_PER_CALL`]),
/// - the PR-3c per-shard BACKGROUND timer task for idle shards (its own per-cycle cap),
///
/// so the advance-and-reap logic lives in one place. The wheel may offer a STALE entry
/// (a re-TTL'd / PERSISTed / overwritten key); [`ActiveExpiry::reap_if_expired`]
/// re-checks the store's real `expire_at`, so only a genuinely-expired key is reaped
/// and counted. `max` caps the work so neither path stalls. The lazy backstop in the
/// store remains the correctness guarantee; this is purely the memory optimization.
///
/// Determinism (ADR-0003): the WORK (which keys are due) is decided entirely by the
/// `now` the caller reads from the Env clock; the helper itself reads no clock. So a
/// background timer firing on wall-clock time does not change observable behavior, but
/// the keys it reaps for a given `now` are byte-identical on a seeded replay.
pub fn drain_due_keys<S: Store + ActiveExpiry>(
    wheel: &mut TimingWheel,
    store: &mut S,
    now: UnixMillis,
    max: usize,
) -> u64 {
    let mut reaped = 0u64;
    for (db, key) in wheel.advance(now, max) {
        if store.reap_if_expired(db, &key, now) {
            reaped += 1;
            // KEYSPACE NOTIFICATION (PROD-8): an ACTIVE TTL reap fires the `expired` event (class
            // `x`, Redis `NOTIFY_EXPIRED`). `notify::record` short-circuits on the disabled default before touching the
            // key, so this is zero-cost when notifications are off. The key is the wheel entry's,
            // exactly the reaped key.
            ironcache_config::notify::record(
                ironcache_config::EventClass::Expired,
                "expired",
                &key,
                db,
            );
        }
    }
    reaped
}

/// Immutable, server-wide context a handler may read. It is cloned cheaply onto
/// each shard; the dynamic per-rollup counters are passed in separately.
///
/// ## The runtime-config cell (PR-4b, the one new cross-shard shared state)
///
/// `runtime` is the process-wide [`RuntimeConfig`] overlay, shared as an `Arc` cloned
/// into every shard's context at boot (exactly like the shutdown `AtomicBool`
/// precedent in the bootstrap). It is the HIGHEST-precedence config layer (CONFIG.md):
/// a `CONFIG SET` mutates it, and the per-command reads here are cheap atomic loads
/// (`maxmemory`/`generation`), with the string-valued params (policy name /
/// requirepass) behind a lock that lives in `ironcache-config` and is taken only on
/// `CONFIG SET`/generation-change, never per command. `boot` is the lower-layer fold
/// (CLI > env > TOML > defaults), read by `CONFIG GET` for the restart-required params.
#[derive(Debug, Clone)]
pub struct ServerContext {
    /// The process-wide runtime-config overlay (the highest-precedence layer). Shared
    /// across shards as an `Arc`; the per-command hot-path reads are atomic loads.
    pub runtime: Arc<RuntimeConfig>,
    /// The process-wide ACL user registry (#106), shared across shards as an `Arc` exactly
    /// like [`Self::runtime`]. Holds the named users + their per-command/key/channel
    /// permissions; the serve layer reads it for `AUTH` (resolve + cache the connection's
    /// `Arc<User>` once) and for the rare `ACL` admin verbs. The per-command ENFORCEMENT
    /// reads the connection's cached `Arc<User>`, NOT this registry, so the data hot path
    /// never locks. `AclState::is_acl_active()` is a single relaxed-atomic gate the
    /// enforcement layer checks first, so the no-ACL default deployment is byte-identical.
    pub acl: Arc<crate::acl::AclState>,
    /// The boot-resolved config (CLI > env > TOML > defaults), the lower-layer fold.
    /// `CONFIG GET` reads it for the restart-required params (bind/port/databases/...).
    pub boot: Config,
    /// Number of logical databases (`SELECT` range is `[0, databases)`).
    pub databases: u32,
    /// The shard count, for computing the per-shard admission budget
    /// (`current maxmemory / shards`). The maxmemory ceiling is split evenly across
    /// shards (shared-nothing, ADR-0002), recomputed from the CURRENT runtime
    /// `maxmemory` on each ceiling check so a `CONFIG SET maxmemory` reaches all shards.
    pub shards: usize,
    /// Static server facts for INFO/HELLO.
    pub info: ServerInfo,
    /// The static cluster slot-ownership map (CLUSTER_CONTRACT.md #70, slice 2). `Some` iff
    /// cluster mode is enabled AND a topology was configured; `None` for a standalone node OR
    /// a cluster-enabled node with no topology (which stays single-node-owns-all, slice-1).
    ///
    /// Shared by `Arc` across every shard task (the map is immutable after boot in STATIC mode;
    /// in RAFT mode the same `Arc` is written by the single control-plane task via the config
    /// state machine and read concurrently by the shards). The `Arc` is a shared-ownership
    /// pointer, NOT a lock, so the shared-nothing invariant (ADR-0002) holds; it is the blessed
    /// path the runtime already uses for the cross-shard shutdown flag. The CLUSTER projection
    /// reads it to render the real multi-node SLOTS/SHARDS/NODES/INFO; the serve-layer router
    /// reads it to decide MOVED/CROSSSLOT redirection.
    pub cluster: Option<Arc<ironcache_cluster::SlotMap>>,
    /// The Raft control-plane handle (HA-4c), present ONLY in raft-governance mode
    /// (`cluster_mode == Raft`); `None` for the DEFAULT static path (and every standalone /
    /// slice-2/3 node), so that path is byte-unchanged and pays zero new cost.
    ///
    /// When `Some`, a CLUSTER MUTATOR (ADDSLOTS / SETSLOT / MEET / FORGET / SET-CONFIG-EPOCH)
    /// PROPOSES a `ConfigCmd` through the log via this handle instead of mutating the local map
    /// directly; on commit every node's config state machine applies the same change into its
    /// shared `cluster` map. The handle is the clonable `Send` inbox/status handle, NOT the
    /// `!Send` engine (which lives on its own control-plane thread).
    pub raft: Option<ironcache_raft_net::RaftHandle>,
    /// The NODE-LEVEL replication status cell (HA-7e), present ONLY in raft-governance mode
    /// (`raft.is_some()`); `None` for the DEFAULT static path (and every standalone / slice-2/3
    /// node), so INFO reports the byte-compatible standalone `role:master`/`connected_slaves:0`
    /// posture and CLUSTER SHARDS the unchanged single-master-at-offset-0-online projection.
    ///
    /// When `Some`, the repl tasks (the primary per-replica serve task + the replica control/tail
    /// task, each a SINGLE WRITER for its half) publish the current role / offsets / link state
    /// here on the replication cadence (per attach / per shipped batch / per applied op), and the
    /// serve layer reads a `ReplStatusSnapshot` to render the INFO `# Replication` section +
    /// CLUSTER SHARDS health/offset. It is a small bag of ATOMICS (no hot-path lock) shared as an
    /// `Arc`; it is NODE-LEVEL cold state, NEVER touched per stored key, so the data hot path and
    /// `bytes_per_key` are unaffected. HA-8's promotion gate consumes `ReplNodeStatus::is_in_sync`
    /// off the same cell.
    pub repl_status: Option<Arc<ironcache_repl::ReplNodeStatus>>,
    /// The SOURCE-SIDE in-sync-replica COUNT (ADR-0026, the WRITE-SIDE `min-replicas-to-write`
    /// guardrail), present ONLY in raft-governance mode (the same gate as `repl_status`); `None`
    /// for the DEFAULT static path. The primary's per-replica serve tasks maintain it with
    /// lock-free per-connection deltas (in sync <-> out of sync, on attach / disconnect, behind the
    /// `min_replicas_max_lag` lag gate); the WRITE path reads it with ONE relaxed atomic load, and
    /// ONLY when `min_replicas_to_write > 0`, so the default-disabled guardrail leaves the write
    /// hot path byte-unchanged. It is NODE-LEVEL cold state (one `AtomicUsize`), never touched per
    /// stored key, so `bytes_per_key` is unaffected.
    pub in_sync_replicas: Option<Arc<ironcache_repl::InSyncReplicas>>,
    /// The PER-BOOT replication HISTORY token (the resume identity, distinct from the STABLE
    /// [`crate::ServerInfo::cluster_node_id`]). Generated ONCE at boot through the determinism seam
    /// (ADR-0003: drawn from the binary's `SystemEnv` RNG in `serve::run_server`), so it is a NEW
    /// value on every process restart while the cluster identity stays the same. The primary
    /// advertises it in the `FullSync` frame; a reconnecting replica REMEMBERS the exact token it
    /// last synced under and re-advertises it, and the primary RESUMES the incremental tail ONLY on
    /// an EXACT token match (else a full re-sync). This is the fence against silent divergence: a
    /// restarted primary resets its offset space to 0 yet kept the stable cluster id, so without a
    /// per-boot token a replica would resume against a DIFFERENT history and silently keep stale
    /// data. `None` outside raft-governance mode (the default static path never serves the live
    /// resume) and in tests that do not exercise the resume gate, where a first-connect replica
    /// always full-syncs. NODE-LEVEL cold state, never touched per stored key.
    pub repl_history_id: Option<ironcache_repl::ReplId>,
    /// The process-wide per-shard metrics registry (OBSERVABILITY.md, #152), present ONLY when
    /// the out-of-band `/metrics` endpoint is enabled (`--metrics-addr` set); `None` on the
    /// DEFAULT path. When `Some`, each shard ADOPTS its pre-allocated cell at boot (so its
    /// counter mutations land in the same cell the metrics HTTP task reads across threads), and
    /// the periodic active-expiry tick publishes the shard's live key count into it. It is a
    /// lock-free aggregation point (one `Arc<Vec<Arc<cell>>>`, no `Mutex`); reading it (the
    /// `/metrics` scrape) never touches the command hot path, and on the default path the field
    /// is `None` so the shard's counters use a standalone cell and boot is byte-identical.
    pub metrics_registry: Option<ironcache_observe::MetricsRegistry>,
    /// The LIVE node-level persistence stats (last-save time + dirty counter), present ONLY when
    /// durable persistence is enabled (a `data_dir` is configured); `None` on the persistence-OFF
    /// default path. Shared by `Arc` with the binary's persistence state (which writes it) and the
    /// `/metrics` gauges (which read it), so the INFO `# Persistence` section reports the SAME live
    /// `rdb_last_save_time` / `rdb_changes_since_last_save` atomics (durability footgun fix #5). The
    /// save POLICY itself is read from [`Self::runtime`] (the runtime overlay a `CONFIG SET save`
    /// mutates); this cell carries only the last-save + dirty signal. Lives in `ironcache-observe`
    /// so this crate holds it without depending on the binary above it. `None` renders the honest
    /// persistence-disabled section (last-save 0, empty policy).
    pub persist_stats: Option<Arc<ironcache_observe::PersistRuntime>>,
    /// The process-GLOBAL allocator-memory gauge (PROD-SAFETY #1/#2): the latest jemalloc
    /// `used_memory` / `used_memory_rss` figures, published OFF the hot path by the binary's
    /// periodic expiry tick and read by the maxmemory admission gate so the over-limit DECISION
    /// is driven off REAL process memory against the FULL `maxmemory` (protecting the HOST from
    /// OOM, not a logical fiction that undercounts ~2x), rather than only the per-shard logical
    /// counter against the even-split per-shard budget. Shared by `Arc` onto every shard's
    /// context; one relaxed load on the eviction path, never per command. When the figure is `0`
    /// (no allocator to query / nothing published yet) the gate falls back to the per-shard
    /// logical counter, so the default and test paths are byte-unchanged.
    pub process_memory: Arc<ironcache_observe::ProcessMemoryGauge>,
    /// The process-GLOBAL live-connection gate (PROD-SAFETY #3, the `maxclients` connection-
    /// exhaustion DoS fix): ONE per node, shared by `Arc` onto every shard's accept path. The
    /// per-connection serve loop calls `try_admit(maxclients)` at the TOP of the connection (a cold
    /// accept-path check, never per command) and rejects a connection over the cap with `-ERR max
    /// number of clients reached`, releasing the slot on close. The cap is read from the runtime
    /// overlay (`maxclients`), so `0` disables it (unlimited, the pre-fix behavior) and a
    /// `CONFIG SET maxclients` takes effect for subsequent connections.
    pub conn_gate: Arc<ironcache_observe::ConnectionGate>,
    /// The node-level SLOWLOG ring (PROD-7 operability): the slowest recent commands plus the live
    /// `slowlog-log-slower-than` / `slowlog-max-len` knobs. ONE per node, shared by `Arc` onto every
    /// shard's context. The per-command timing HOOK lives in the serve layer (it needs the client
    /// addr/name + the Env clock); the `SLOWLOG` command handler here reads/resets this ring. When
    /// disabled (`slowlog-log-slower-than` = -1) the hook short-circuits on one relaxed atomic load,
    /// so the fast path costs a single compare and never touches the ring (see
    /// [`ironcache_observe::SlowLog`]).
    pub slowlog: Arc<ironcache_observe::SlowLog>,
    /// The node-level LATENCY monitor (PROD-7): the worst spike + bounded history per named event.
    /// ONE per node, shared by `Arc`. Updated on the same slow-path the SLOWLOG hook runs on (only
    /// when a sample exceeds a floor), read only by the `LATENCY` admin command, so it is off the
    /// per-command hot path.
    pub latency: Arc<ironcache_observe::LatencyMonitor>,
    /// The node-level live-connection REGISTRY (PROD-7): the directory `CLIENT LIST` / `CLIENT KILL`
    /// / `CLIENT PAUSE` operate over. ONE per node, shared by `Arc`. A connection registers itself
    /// on accept and deregisters on close (cold paths, not per command); the registry is consulted
    /// only by the rare CLIENT admin verbs and the serve loop's post-batch kill/pause check.
    pub clients: Arc<ironcache_observe::ClientRegistry>,
    /// The node-level HOTKEYS tracking container (#428): the faithful Redis 8.6 hot-key tracker the
    /// `HOTKEYS START`/`STOP`/`GET`/`RESET` verbs drive. ONE per node, shared by `Arc`. The
    /// per-command recording HOOK lives in the serve layer (it needs the command's elapsed micros +
    /// keys); when no session is active the hook short-circuits on ONE relaxed atomic
    /// ([`ironcache_observe::Hotkeys::is_active`]), so the default deployment and the per-PR perf-gate
    /// (which run with tracking off) are byte-unchanged and never touch the lock or the sketch.
    pub hotkeys: Arc<ironcache_observe::Hotkeys>,
}

impl ServerContext {
    /// Whether a password is currently configured (auth required). Reads the runtime
    /// overlay so a `CONFIG SET requirepass` takes effect for new commands. Takes the
    /// overlay's lock (off the per-command hot path: the auth check is rare relative to
    /// data commands, and the lock is uncontended in the single-threaded-per-shard model).
    #[must_use]
    pub fn requires_auth(&self) -> bool {
        self.runtime.requires_auth()
    }

    /// The current effective `maxmemory` ceiling in bytes (a cheap atomic load from the
    /// runtime overlay). `0` means unlimited.
    #[must_use]
    pub fn maxmemory(&self) -> u64 {
        self.runtime.maxmemory()
    }

    /// The CURRENT per-shard byte budget: `current maxmemory / shards` (the even split,
    /// ADR-0002), recomputed from the runtime overlay so a `CONFIG SET maxmemory`
    /// reaches every shard's admission gate. `0` when `maxmemory == 0` (unlimited).
    #[must_use]
    pub fn per_shard_budget(&self) -> u64 {
        let max = self.maxmemory();
        if max == 0 {
            0
        } else {
            (max / self.shards.max(1) as u64).max(1)
        }
    }

    /// Whether the memory ceiling is enabled (a non-zero current `maxmemory`). When
    /// `false`, admission is a no-op and every write is served.
    #[must_use]
    pub fn ceiling_enabled(&self) -> bool {
        self.maxmemory() > 0
    }

    /// Whether the node is OVER its `maxmemory` ceiling, driving the admission decision off the
    /// REAL allocator figure rather than only the per-shard LOGICAL counter (PROD-SAFETY #1/#2).
    ///
    /// The caller passes `shard_logical_used` (this shard's `store.used_memory()`). The decision is
    /// the OR of two tests, so the ceiling protects the HOST:
    ///
    /// 1. PROCESS-GLOBAL allocator trigger (the host-OOM fix): if a live allocator `used_memory`
    ///    figure has been published, compare it against the FULL `maxmemory`. This is the real
    ///    process memory (the same figure INFO reports), which the logical counter undercounts by
    ///    ~2x (slab slack, table overhead) -- so this is what actually bounds RSS. It is also
    ///    PROCESS-GLOBAL (vs the even per-shard split), so a HOT shard sheds when the NODE is over
    ///    the limit even if that shard's individual even-split budget is not exceeded
    ///    (PROD-SAFETY #2 global trigger). Strict `>` matches Redis `getMaxmemoryState`
    ///    (under-limit at `used <= maxmemory`).
    /// 2. PER-SHARD logical fallback: the prior behavior (this shard's logical bytes vs its
    ///    even-split per-shard budget). Retained so that when NO allocator figure is available
    ///    (the system-allocator / MSVC build, or before the first publish, or in unit tests with a
    ///    zeroed gauge) the gate behaves EXACTLY as before -- the default/test path is
    ///    byte-unchanged -- and so a per-shard logical overshoot still triggers eviction even
    ///    between allocator-figure refreshes.
    ///
    /// Either single relaxed atomic load is off the per-command path (only a `denyoom` write while
    /// the ceiling is enabled reaches here), so this adds no steady-state per-command cost.
    #[must_use]
    pub fn over_maxmemory(&self, shard_logical_used: u64) -> bool {
        let max = self.maxmemory();
        if max == 0 {
            return false;
        }
        // (1) the global allocator figure vs the FULL ceiling (the host-protecting trigger).
        let allocator_used = self.process_memory.used_memory();
        if allocator_used > 0 && allocator_used > max {
            return true;
        }
        // (2) the per-shard logical fallback vs the even-split budget (the byte-unchanged path).
        shard_logical_used > self.per_shard_budget()
    }

    /// Whether this shard's LOGICAL bytes still exceed its even-split per-shard budget -- the
    /// cache-mode POST-eviction -OOM decision (M1). Unlike [`Self::over_maxmemory`], this reads ONLY
    /// the per-shard logical counter the shard just acted on; it does NOT consult the process-global
    /// allocator gauge.
    ///
    /// ## Why the cache-mode decision must NOT use the global gauge (M1)
    ///
    /// The global allocator gauge is refreshed only on the ~100ms expiry tick (it advances the
    /// jemalloc epoch). Eviction frees LOGICAL bytes immediately, but the gauge -- and the
    /// allocator's resident pages, which it may hold after a free -- do NOT drop within the command.
    /// If the cache-mode post-eviction re-check used [`Self::over_maxmemory`] (which ORs the
    /// stale-high global gauge), a write that eviction LOGICALLY satisfied would still be spuriously
    /// `-OOM`'d for up to ~100ms near the ceiling, diverging from Redis (an evicting policy clears
    /// OOM WITHIN the command). So the cache-mode per-command -OOM decision is driven here, off the
    /// fresh per-shard logical figure: a successful eviction (logical now <= budget) ALLOWS the
    /// write. The global gauge still TRIGGERS eviction via [`Self::over_maxmemory`] (so the node
    /// keeps evicting to work RSS down over ticks); it just must not -OOM a logically-satisfied
    /// write. NOEVICTION keeps [`Self::over_maxmemory`]'s global gauge as the HARD CEILING (no
    /// eviction can clear it, so a denyoom write -OOMs when global RSS > maxmemory -- correct).
    ///
    /// Strict `>` matches the trigger's getMaxmemoryState semantics (`used <= budget` is under
    /// limit). `0` budget (maxmemory disabled) is never over.
    #[must_use]
    pub fn over_per_shard_budget(&self, shard_logical_used: u64) -> bool {
        let budget = self.per_shard_budget();
        budget > 0 && shard_logical_used > budget
    }

    /// HOW the cluster's slot map is governed (HA-4c): the boot `cluster_mode`. The DEFAULT is
    /// [`ClusterMode::Static`] (the byte-unchanged pre-HA-4c path). Only [`ClusterMode::Raft`]
    /// routes a CLUSTER mutator through the control plane (and only then is [`Self::raft`] set).
    /// Boot-only (like `cluster_enabled`), so it reads the boot config, not the runtime overlay.
    #[must_use]
    pub fn cluster_mode(&self) -> ClusterMode {
        self.boot.cluster_mode
    }
}

/// A source of the rolled-up counters for INFO's `# Stats` / `# Clients` sections. The serve loop
/// supplies the NODE-WIDE rollup (#531): it sums EVERY shard's counter cell through the always-on
/// metrics registry ([`ironcache_observe::MetricsRegistry::aggregate`]), so INFO reports the whole
/// node's totals invariant to which shard homed the connection -- consistent with `DBSIZE` and
/// `/metrics`. (The registry-absent unit-test path falls back to the serving shard's snapshot.)
pub type RollupFn<'a> = &'a dyn Fn() -> CounterSnapshot;

/// A source of the NODE-WIDE `# Keyspace` per-db lines for INFO (#531). The serve loop supplies the
/// cross-shard sum (gathered by the SAME whole-keyspace scatter-gather `DBSIZE` uses) as
/// `Some(lines)`, so INFO's `dbN:keys=...` counts equal `DBSIZE` on a multi-shard node. `None`
/// tells [`cmd_info`] to fall back to the SERVING shard's local `db_len` (the single-shard node,
/// where the serving shard IS the whole keyspace, and any path -- EXEC replay / a unit-test
/// dispatch -- that cannot fan out); that fallback is byte-identical to the pre-#531 behavior.
/// Invoked ONLY when INFO renders the keyspace section (zero cost on every other command).
pub type KeyspaceFn<'a> = &'a dyn Fn() -> Option<Vec<KeyspaceDbLine>>;

/// Yields the INFO `COMMANDSTATS` + `ERRORSTATS` section BODIES (#413) as
/// `(commandstats_body, errorstats_body)`, rendered by the serve layer. The `COMMANDSTATS` body is
/// now NODE-WIDE (#527): the serve loop records each command's calls/usec/failed into its shard's
/// per-command atomic slot in the metrics registry, and this closure sums EVERY shard's table via
/// [`ironcache_observe::MetricsRegistry::aggregate_command_stats`] -- the per-command analog of the
/// node-wide `# Stats` rollup ([`RollupFn`], #545), invariant to which shard homed the connection.
/// The `ERRORSTATS` body remains the SERVING shard's local `errorstat_*` table (home-shard-local,
/// the same scope the pre-#527 per-command table used); cross-shard error aggregation is the
/// remaining smaller follow-up (the acceptance surface is per-command `calls`, not error codes).
/// `INFO` invokes it ONLY for the `commandstats` / `errorstats` / `everything` sections, so it costs
/// nothing on the common INFO path; a caller with no metrics registry + no per-shard error table
/// (bare unit-test contexts) passes a closure yielding two empty strings.
pub type CmdStatsFn<'a> = &'a dyn Fn() -> (String, String);

/// Dispatch one request to its handler, returning the reply [`Value`].
///
/// `env` is the per-shard determinism seam (ADR-0003): its CLOCK half provides INFO
/// uptime (no direct time) and its RNG half is the source RANDOMKEY draws a random
/// index from (the CALLER draws through the seam; the store reads no RNG, KEYSPACE.md).
/// It is `&mut E` because the RNG needs `&mut self`; dispatch is the single owner of
/// the env borrow for the command, so the clock read and the RNG draw do not alias.
/// `store` is the per-shard storage waist (#34) the data commands run against; `now`
/// is the
/// absolute wall-clock deadline basis for this command, computed once per command
/// by the caller from the Env clock (ADR-0003: the store reads no clock). `state`
/// is the mutable per-connection state. `rollup` yields the counters for INFO;
/// `mem` is the process-global allocator snapshot (ADR-0006) the caller read ONCE at
/// the binary edge for INFO `used_memory`/`used_memory_rss` (the server crate cannot
/// read the concrete store's mallctl by the layering contract, so the figure is
/// supplied in).
///
/// Tier-0 (connection) commands ignore `store`/`now`; the data commands use them.
/// The function is generic over `S: Store + Admit + ActiveExpiry + Keyspace` for
/// monomorphization, and over `E: Env` (clock + RNG). The [`Admit`] bound lets the
/// dispatcher enforce the maxmemory ceiling (evict-to-fit / `-OOM`); the [`Keyspace`]
/// bound adds the iteration + bulk-management surface (SCAN/KEYS/DBSIZE/RANDOMKEY/
/// RENAME/COPY/MOVE/SWAPDB/FLUSH) without naming the concrete store.
///
/// `deltas` is an out-parameter dispatch accumulates this command's dynamic counter
/// changes into (eviction count, active-expiry reclamation count, keyspace hits/misses);
/// it starts zeroed and the serve loop folds it into the shard's [`ShardCounters`]
/// AFTER dispatch returns. It is a `&mut` out-parameter rather than a counter handle so
/// dispatch does not alias the `rollup` closure's borrow of the same per-shard counters.
///
/// `wheel` is the per-shard timing wheel (#51): dispatch drains a BOUNDED batch of due
/// keys from it BEFORE the command body (the active reclamation), and the TTL-setting
/// commands register their new deadline into it.
///
/// The arguments are each a distinct, orthogonal seam (ctx/state/clock/store/wheel/now/
/// rollup/mem/deltas/req) the dispatcher fans out to handlers; bundling them into a
/// struct would just move the same fields behind one name and obscure the per-command
/// borrows (the lifetime-parameterized `rollup` closure in particular). The over-7-args
/// lint is allowed here with that justification.
/// The per-command `maxmemory-policy` hot-swap check (CONFIG.md, PR-4b). The hot-path
/// cost is ONE Acquire atomic load (`generation`) + an integer compare against this
/// shard's last-seen value; when nothing changed (the common case) it returns
/// immediately with NO lock. The Acquire load pairs with the writer's Release bump
/// (`RuntimeConfig::set_policy_name`), so observing a new generation here happens-after
/// the new policy name was written: the subsequent `policy_name()` read is guaranteed
/// to see it, with the ordering carried by the atomic itself (not just the strings Mutex).
///
/// On an actual change (a `CONFIG SET maxmemory-policy` happened on SOME shard), this
/// shard rebuilds its OWN eviction policy from the new name and catches up. Only here
/// (rare) do we take the overlay lock to read the new name and draw an RNG seed through
/// the Env seam (ADR-0003: the `*-random` policy is seeded deterministically, never std
/// rand). The swap reaches all shards eventually-consistently: each shard notices on its
/// next command (at most one command of lag), so a connection on a DIFFERENT shard sees
/// the new policy on its next command too. `set_policy_by_name` validated the name at
/// `CONFIG SET` time; a `false` return (defensive) leaves the existing policy in place.
fn maybe_hot_swap_policy<E: Env, S: PolicySwap>(
    ctx: &ServerContext,
    env: &mut E,
    store: &mut S,
    shard_generation: &mut u64,
    now: UnixMillis,
) {
    let current_generation = ctx.runtime.generation();
    if current_generation != *shard_generation {
        let new_name = ctx.runtime.policy_name();
        let seed = env.rng().next_u64();
        // `now` lets the swap skip re-seeding lazily-expired entries into the new policy
        // (IC-1: the new policy is re-seeded from the live keyspace so eviction works
        // immediately after the swap).
        let _ = store.set_policy_by_name(&new_name, seed, now);
        // The generation also rises on a `CONFIG SET *-max-listpack-*` (#40): refresh this shard's
        // cached encoding-threshold snapshot on the SAME generation-change check, so a threshold
        // change reaches the encoding-transition decision for FUTURE inserts (existing keys keep
        // their encoding). A plain field write off the hot path; the common no-change path skips it
        // (the generation did not move).
        store.apply_encoding_thresholds(ctx.runtime.encoding_thresholds());
        *shard_generation = current_generation;
    }
}

/// The LIVE `proto-max-bulk-len` ceiling as a `usize`, read from the runtime overlay (Area B). The
/// growth-capable string/bitmap handlers (APPEND/SETRANGE/SETBIT/BITFIELD) read it so a
/// `CONFIG SET proto-max-bulk-len` takes effect for subsequent commands. A single relaxed atomic
/// load on the (already write-side) command path; `0` (rejected at set time) is unreachable, and a
/// value past `usize::MAX` on a 32-bit target saturates (the decoder Limits already bound the
/// inbound size). The default 512 MB keeps the prior compiled-constant behavior byte-identical.
#[inline]
#[must_use]
fn max_bulk_len(ctx: &ServerContext) -> usize {
    usize::try_from(ctx.runtime.proto_max_bulk_len()).unwrap_or(usize::MAX)
}

/// The PRE-AUTH ALLOW-LIST (Redis: `HELLO`, `AUTH`, `QUIT`, `RESET`). With `requirepass`
/// configured, a connection that has NOT yet authenticated may run ONLY these commands;
/// every other command (data, admin, whole-keyspace, CLUSTER mutators, persistence,
/// SHUTDOWN, the cross-shard fan-outs) is `-NOAUTH`. This is the SINGLE SOURCE OF TRUTH
/// for that allow-list: both the downstream `dispatch_with_cmd` gate AND the hoisted
/// serve-layer router chokepoint (`crate::serve::route_and_dispatch`) call it, so the two
/// gates can NEVER diverge on which commands are allowed before auth. `cmd` MUST be the
/// uppercased command token (the only form the callers hold).
///
/// Keep this list IDENTICAL to Redis (`ACLCheckAllPerm` allow-set: HELLO/AUTH/RESET +
/// QUIT, which is connection teardown): do NOT add or remove a command here without a
/// deliberate parity change -- it is the security boundary.
#[inline]
#[must_use]
pub fn command_allowed_pre_auth(cmd: &[u8]) -> bool {
    matches!(cmd, b"AUTH" | b"HELLO" | b"QUIT" | b"RESET")
}

/// LIVE-REVOCATION RE-RESOLVE (F1): bring a connection's cached ACL identity up to date with the
/// registry when a mutation has happened SINCE the identity was cached, run ONCE per command at
/// the router chokepoint right BEFORE [`acl_enforce`]. This is what makes a mid-session `ACL
/// SETUSER app -@all` / `ACL DELUSER app` (or an `ACL SETUSER default ...` narrowing the implicit
/// default) take effect on the offending connection's VERY NEXT command, instead of being fail-open
/// until that client re-AUTHs or disconnects (the F1 finding; Redis revokes live).
///
/// ## Hot path: one relaxed load + integer compare
///
/// The COMMON case is no mutation since the connection cached its user: the live registry
/// `generation` equals `conn.acl_user_gen`, so this returns `true` after ONE relaxed atomic load
/// and an integer compare -- no lock, no allocation, byte-identical on the no-ACL path (where the
/// generation never moves at all). Only when the generation MOVED (rare: an `ACL` admin verb ran)
/// does it take the registry lock to re-resolve the connection's user by `acl_user_name`:
/// - user still present -> refresh `conn.acl_user` (`None` when all-permissive, so a back-to-
///   permissive default re-collapses to the byte-identical fast path; `Some` when narrowed, so a
///   fresh restriction is picked up) and update `conn.acl_user_gen`; returns `true`.
/// - user DELETED (`ACL DELUSER`) -> DEAUTHENTICATE the connection (clear `authenticated` + drop
///   the cached user back to the implicit default and reset the name) so its next command hits the
///   NOAUTH gate, and return `false` so the caller CLOSES it -- mirroring Redis, which kills a
///   deleted user's clients. Closing (vs silently reverting to the all-permissive default) is the
///   safe choice: a no-requirepass deployment would otherwise leave the connection running as the
///   permissive default after its narrowed user was deleted.
///
/// Returns `true` when the connection remains a valid identity (possibly refreshed), `false` when
/// it was deauthenticated and the caller should close it.
#[must_use]
pub fn acl_resolve_if_stale(ctx: &ServerContext, conn: &mut ConnState) -> bool {
    // HOT PATH: one relaxed load + compare. Unchanged generation -> nothing to do.
    if ctx.acl.generation() == conn.acl_user_gen {
        return true;
    }
    // COLD PATH (a mutation happened): re-resolve the connection's user by name under the lock.
    match ctx.acl.resolve_if_stale(&conn.acl_user_name) {
        crate::acl::AclResolution::Refresh { user, generation } => {
            conn.acl_user = user;
            conn.acl_user_gen = generation;
            true
        }
        crate::acl::AclResolution::Deauth => {
            // The user was DELUSER'd: this connection is no longer authenticated AS anyone. Drop
            // it back to the unauthenticated implicit-default baseline so a NEXT command (if the
            // caller did not close) hits NOAUTH, and signal the caller to close (Redis parity).
            conn.authenticated = !ctx.requires_auth();
            conn.acl_user = None;
            // Reuse the existing allocation rather than reassign (clippy::assigning_clones).
            conn.acl_user_name.clear();
            conn.acl_user_name.push_str(crate::acl::DEFAULT_USER);
            conn.acl_user_gen = ctx.acl.generation();
            false
        }
    }
}

/// Does this `SORT` / `SORT_RO` request use a BY/GET option that DEREFERENCES external keys?
///
/// `SORT key ... [BY pat] ... [GET pat ...]` reads keys built by substituting the source
/// element into `pat`. Those keys are NOT part of the command key-spec, so the ACL per-key
/// check cannot see them. The dereferencing forms are:
/// - `BY pat` where `pat` contains a `*` (a `BY` pattern with NO `*` is `nosort` -- it skips
///   sorting and does NOT read any external key, so it is EXEMPT, matching `cmd_sort`);
/// - any `GET pat` that is not exactly `#` (`GET #` projects the element ITSELF, not an
///   external key, so it is EXEMPT).
///
/// This mirrors the option scan in [`crate::cmd_sort`] (BY/GET each consume the next arg) so
/// the two never diverge. It runs ONLY for SORT/SORT_RO under an active, non-allkeys ACL --
/// off the hot path for every other command and for allkeys / ACL-off connections.
#[must_use]
fn sort_derefs_external_keys(req: &Request) -> bool {
    // The option tail begins after the command token and the source key (args[0], args[1]).
    let Some(opts) = req.args.get(2..) else {
        return false;
    };
    let mut i = 0;
    while i < opts.len() {
        let tok = &opts[i];
        if tok.eq_ignore_ascii_case(b"BY") {
            // BY consumes the next arg as its pattern. A `*` in the pattern means it
            // dereferences an external key per element; no `*` is the exempt `nosort` form.
            if let Some(pat) = opts.get(i + 1) {
                if pat.contains(&b'*') {
                    return true;
                }
            }
            i += 2;
        } else if tok.eq_ignore_ascii_case(b"GET") {
            // GET consumes the next arg as its pattern. `GET #` is the element itself (exempt);
            // ANY other GET pattern reads an external key.
            if let Some(pat) = opts.get(i + 1) {
                if pat.as_ref() != b"#" {
                    return true;
                }
            }
            i += 2;
        } else if tok.eq_ignore_ascii_case(b"LIMIT") {
            // LIMIT consumes two args (offset count); skip them so a numeric arg is never
            // mistaken for an option token. (ASC/DESC/ALPHA/STORE consume only themselves;
            // STORE's destination IS in the key-spec, so it is checked by the normal path.)
            i += 3;
        } else {
            i += 1;
        }
    }
    false
}

/// THE PER-COMMAND ACL ENFORCEMENT CHECK (#106). Given the connection's cached ACL identity
/// (`acl_user`, `None` == the implicit all-permissive default), the command token, and the
/// parsed request, decide whether the user may run it. Returns `None` when ALLOWED and
/// `Some(ErrorReply)` (a `-NOPERM`) when DENIED. This is the value the ACL engine adds: it is
/// wired at the router chokepoint right after the existing NOAUTH gate, so it covers EVERY
/// command path (home, cross-shard, whole-keyspace, pubsub, MULTI-queue, CLUSTER mutators).
///
/// ## Hot-path discipline (cheap)
///
/// The COMMON case is the no-ACL deployment: `acl_active` is `false` (one relaxed atomic
/// load) and/or the connection is the implicit default (`acl_user == None`), so this returns
/// `None` after a single bool test -- byte-identical, O(1), no per-command allocation. Only
/// an ACL-governed connection (a narrowed `Some(user)`) pays for the checks:
/// 1. the COMMAND test: the user's compiled command-rule replay (`can_run_command`, O(rules)).
/// 2. the KEY test: ONLY for a key-bearing command, a glob match over its FEW key args
///    (extracted via the #89 command-spec key spec) against the user's key patterns.
/// 3. the CHANNEL test: ONLY for a pub/sub command, over its channel args.
///
/// The pre-auth allow-list commands (AUTH/HELLO/QUIT/RESET) are NEVER denied here (they ran
/// before the user was even resolved); they are short-circuited by `acl_user == None` for the
/// default and explicitly exempted for a narrowed user so a locked-down user can still AUTH /
/// switch users / RESET (Redis: these are always permitted).
#[must_use]
pub fn acl_enforce(
    acl_active: bool,
    acl_user: Option<&crate::acl::User>,
    cmd_upper: &[u8],
    req: &Request,
) -> Option<ErrorReply> {
    // FAST GATE: the no-ACL deployment (no narrowed user anywhere) skips everything. A
    // connection with no cached narrowed user is the implicit all-permissive default and is
    // never denied (the `?` returns `None` == ALLOWED); if ACL is globally inactive there is
    // nothing to enforce either way.
    let user = acl_user?;
    if !acl_active {
        return None;
    }

    // The connection-control / handshake commands are ALWAYS allowed (Redis: a user can
    // always AUTH/HELLO/QUIT/RESET regardless of command perms, so it can re-authenticate or
    // disconnect). This mirrors the pre-auth allow-list.
    if command_allowed_pre_auth(cmd_upper) {
        return None;
    }

    // (a) COMMAND permission. For a CONTAINER command (CLUSTER today) that carries a SUBCOMMAND,
    // the PER-SUBCOMMAND grant decides (so CLUSTER SLOTS, tagged @slow only, is allowed for a
    // `-@dangerous` user while CLUSTER ADDSLOTS, tagged @admin+@dangerous, is NOPERM). The
    // subcommand is uppercased with the SAME `ascii_upper` cmd_cluster.rs uses to dispatch, so ACL
    // and dispatch agree on case. Every other command keeps the whole-command check unchanged --
    // this is the only behavioral change, and it fires only for a narrowed (non-None) user (the
    // acl_user==None default short-circuited above).
    if crate::command_spec::subcommands_of(cmd_upper).is_some() && req.args.len() >= 2 {
        let sub = ascii_upper(&req.args[1]);
        if !user.can_run_command_sub(cmd_upper, Some(&sub)) {
            // NOPERM names the `cmd|sub` pair (lowercased, pipe), Redis 7 parity.
            let cmd_lc = String::from_utf8_lossy(cmd_upper).to_ascii_lowercase();
            let sub_lc = String::from_utf8_lossy(&sub).to_ascii_lowercase();
            return Some(ErrorReply::noperm_command(
                &user.name,
                &format!("{cmd_lc}|{sub_lc}"),
            ));
        }
    } else if !user.can_run_command(cmd_upper) {
        let cmd_lc = String::from_utf8_lossy(cmd_upper).to_ascii_lowercase();
        return Some(ErrorReply::noperm_command(&user.name, &cmd_lc));
    }

    // (c) CHANNEL permission for pub/sub commands (the channel args are the message targets).
    // SUBSCRIBE/UNSUBSCRIBE/PUBLISH take channel name(s) at args[1..]; PUBLISH's args[1] is the
    // channel (args[2] is the message, not a channel, but it is harmless to also pattern-check
    // a non-channel here -- Redis checks only the channel, so restrict to the right arg).
    match cmd_upper {
        b"SUBSCRIBE" | b"UNSUBSCRIBE" | b"PSUBSCRIBE" | b"PUNSUBSCRIBE" => {
            if !user.channels.is_allchannels() {
                for ch in req.args.iter().skip(1) {
                    if !user.can_access_channel(ch) {
                        return Some(ErrorReply::noperm_channel());
                    }
                }
            }
            return None;
        }
        b"PUBLISH" => {
            if !user.channels.is_allchannels() {
                if let Some(ch) = req.args.get(1) {
                    if !user.can_access_channel(ch) {
                        return Some(ErrorReply::noperm_channel());
                    }
                }
            }
            return None;
        }
        _ => {}
    }

    // (b) KEY permission for key-bearing commands. The all-keys fast path skips the whole
    // extraction. Otherwise extract the command's keys via the #89 command-spec key spec and
    // require EVERY touched key to be allowed by the user's key patterns.
    //
    // ONLY genuine KEYED commands (`KeyedSingle`/`KeyedMulti`) are key-checked. A
    // `WholeKeyspace` command (KEYS/SCAN/FLUSHALL/FLUSHDB/DBSIZE/RANDOMKEY) owns no specific
    // key -- its `key_spec` is the `Arg1` fallback that would return the GLOB PATTERN (KEYS
    // <pattern>) as if it were a key -- so it is gated by COMMAND perms (it is @keyspace /
    // @dangerous), NOT key perms, exactly like Redis. `AlwaysHome` commands have no key.
    if !user.keys.is_allkeys() {
        // SORT / SORT_RO BY/GET external-key dereference gate (redis#10106 / redis 7.0).
        // A `BY pattern` containing `*` or a non-`#` `GET pattern` DEREFERENCES external
        // keys (`weight_*`, `data_*->field`) at runtime; those pattern-keys are NOT in the
        // command key-spec, so the per-key check below never sees them. Redis closes this
        // by denying such a SORT unless the user has FULL key-read permission (allkeys).
        // We are already inside `!is_allkeys()`, so any dereferencing form is denied here.
        // The non-dereferencing forms are EXEMPT: a `nosort` BY (no `*`, no deref) and
        // `GET #` (the element itself, not an external key). When ACL is off / the user is
        // allkeys, this block never runs -> default/allkeys/ACL-off byte-identical.
        if matches!(cmd_upper, b"SORT" | b"SORT_RO") && sort_derefs_external_keys(req) {
            return Some(ErrorReply::noperm_key());
        }
        if let Some(spec) = crate::command_spec::spec_of(cmd_upper) {
            if !matches!(
                spec.class,
                crate::command_spec::CommandClass::KeyedSingle
                    | crate::command_spec::CommandClass::KeyedMulti
            ) {
                return None;
            }
            match crate::command_spec::extract_keys(spec.key_spec, req) {
                crate::route::KeySpec::None => {}
                crate::route::KeySpec::One(k) => {
                    if !user.can_access_key(k) {
                        return Some(ErrorReply::noperm_key());
                    }
                }
                crate::route::KeySpec::Many(keys) => {
                    for k in keys {
                        if !user.can_access_key(k) {
                            return Some(ErrorReply::noperm_key());
                        }
                    }
                }
            }
        }
    }

    None
}

/// Dispatch one CLIENT command: the top-of-command work that must run ONCE per
/// command from the wire, then either QUEUE it (inside a transaction) or run it.
///
/// ## Why this is split from [`dispatch_inner`] (PR-10a re-entrancy)
///
/// `EXEC` must re-run each queued command WITHOUT re-borrowing the store/wheel/env
/// RefCells (the serve loop already holds them borrowed across the whole `dispatch`
/// call). So the command body lives in [`dispatch_inner`], which takes the
/// already-borrowed `&mut store` / `&mut wheel` / `&mut env`: `EXEC`'s loop calls
/// `dispatch_inner` per queued command, reusing the SAME refs, with NO re-borrow of
/// the thread-locals and NO double-borrow panic.
///
/// This outer `dispatch` does the work that must happen exactly ONCE per command
/// arriving from the client and NOT per queued command at EXEC time:
/// 1. reset `deltas` (a fresh per-command accumulator);
/// 2. the `maxmemory-policy` hot-swap generation check (CONFIG.md, PR-4b);
/// 3. the active-expiry wheel drain (EXPIRATION.md #51);
/// 4. the auth gate (NOAUTH before authenticating);
/// 5. THE QUEUE GATE (PR-10a): when inside `MULTI`, validate + stage the command and
///    reply `+QUEUED`, OR reply a queue-time error now and dirty the transaction.
///
/// The per-command `maxmemory` admission gate is INSIDE `dispatch_inner` instead,
/// because Redis evaluates `denyoom` per command at EXEC time (a queued over-budget
/// write becomes an `-OOM` element in the array, no rollback).
#[allow(clippy::too_many_arguments)]
pub fn dispatch<E: Env, S: Store + Admit + ActiveExpiry + Keyspace + PolicySwap + Watch>(
    ctx: &ServerContext,
    state: &mut ConnState,
    env: &mut E,
    store: &mut S,
    wheel: &mut TimingWheel,
    now: UnixMillis,
    shard_generation: &mut u64,
    rollup: RollupFn<'_>,
    cmdstats: CmdStatsFn<'_>,
    keyspace: KeyspaceFn<'_>,
    mem: MemoryInfo,
    deltas: &mut CounterDeltas,
    req: &Request,
) -> Value {
    // Uppercase the command ONCE here, then delegate. This is the entry every test /
    // EXEC re-dispatch path uses; the cross-shard serve loop instead calls
    // [`dispatch_with_cmd`] DIRECTLY with the command it already uppercased for routing,
    // so the home hot path uppercases exactly once (FIX 5).
    let cmd = ascii_upper(req.command());
    dispatch_with_cmd(
        ctx,
        state,
        env,
        store,
        wheel,
        now,
        shard_generation,
        rollup,
        cmdstats,
        keyspace,
        mem,
        deltas,
        req,
        &cmd,
    )
}

/// [`dispatch`] with the uppercased command token supplied by the caller (FIX 5). The
/// cross-shard serve loop computes `cmd_upper` once for routing
/// ([`crate::route::classify`]) and passes the SAME slice here, so the hottest path (a
/// home-owned single-key command) does NOT re-uppercase + re-allocate the command per
/// command. The body is byte-for-byte identical to the prior `dispatch`; only the source
/// of `cmd` changed (param instead of a local `ascii_upper`). `cmd` MUST equal
/// `ascii_upper(req.command())` (the contract the two callers uphold).
#[allow(clippy::too_many_arguments)]
pub fn dispatch_with_cmd<
    E: Env,
    S: Store + Admit + ActiveExpiry + Keyspace + PolicySwap + Watch,
>(
    ctx: &ServerContext,
    state: &mut ConnState,
    env: &mut E,
    store: &mut S,
    wheel: &mut TimingWheel,
    now: UnixMillis,
    shard_generation: &mut u64,
    rollup: RollupFn<'_>,
    cmdstats: CmdStatsFn<'_>,
    keyspace: KeyspaceFn<'_>,
    mem: MemoryInfo,
    deltas: &mut CounterDeltas,
    req: &Request,
    cmd: &[u8],
) -> Value {
    *deltas = CounterDeltas::default();

    // KEYSPACE NOTIFICATIONS (PROD-8): snapshot the live `notify-keyspace-events` flags into THIS
    // shard's emit gate ONCE per command (a single relaxed atomic load + a thread-local `Cell`
    // write), so every `notify::record` the handlers + the expiry/eviction paths make during this
    // command read the SAME flags without re-loading the atomic per event. On the default
    // deployment the flags are `0` (DISABLED), so `record` short-circuits and the write hot path is
    // byte-identical (no channel built, nothing published). The serve/coordinator loop DRAINS the
    // recorded events and PUBLISHes them through the existing Pub/Sub fan-out after the reply.
    ironcache_config::notify::set_command_flags(ctx.runtime.notify_flags());

    // maxmemory-policy HOT-SWAP reach (CONFIG.md, PR-4b): a single relaxed atomic load
    // + compare; the rebuild (rare) is factored into a helper to keep this fn small.
    maybe_hot_swap_policy(ctx, env, store, shard_generation, now);

    // Active TTL reclamation (EXPIRATION.md #51), BEFORE the command body: drain a
    // BOUNDED batch of due keys from the timing wheel and reap the ones whose stored
    // deadline has actually passed (the wheel may offer a stale entry; the store
    // re-checks). This bounds resident memory for expired keys under traffic; the lazy
    // backstop in the store still prevents OBSERVING an expired key, so this is purely
    // a memory optimization. MAX_RECLAIM_PER_CALL caps the work per command so the
    // drain never stalls the command path. The SAME [`drain_due_keys`] helper backs the
    // PR-3c background timer task for idle shards (no duplicate drain logic). It runs
    // ONCE per client command (here), not per queued command at EXEC time.
    //
    // GATED on the runtime active-expire flag (`DEBUG SET-ACTIVE-EXPIRE`, #411): when disabled
    // the active reaper is inert (one relaxed load, default-true so the common path is
    // unchanged) and only LAZY reap-on-access removes a key -- the conformance contract.
    if ctx.runtime.active_expire_enabled() {
        deltas.expired += drain_due_keys(wheel, store, now, MAX_RECLAIM_PER_CALL);
    }

    // Auth gate (DEFENSE IN DEPTH): before authenticating, only the pre-auth allow-list
    // (Redis: HELLO, AUTH, QUIT, RESET) is allowed; everything else is NOAUTH. The PRIMARY
    // gate now lives HOISTED at the top of the serve router (`route_and_dispatch`), the
    // single chokepoint EVERY client command passes through BEFORE any interception /
    // cross-shard fan-out / CLUSTER-mutator path -- which is why a foreign-shard key, a
    // whole-keyspace fan-out, or a CLUSTER mutator can no longer reach execution unauth.
    // This local gate is kept as a redundant backstop for any direct `dispatch` caller
    // (tests, EXEC re-dispatch) that does NOT come through the router; it shares the EXACT
    // `command_allowed_pre_auth` predicate, so it can never diverge from the hoisted gate
    // and never double-replies NOAUTH (the router already short-circuited the unauth case
    // before this runs). Runs once per client command; queued commands at EXEC time are
    // already past auth (you cannot MULTI before authenticating).
    if ctx.requires_auth() && !state.authenticated && !command_allowed_pre_auth(cmd) {
        return Value::error(ErrorReply::noauth());
    }

    // THE SUBSCRIBE-MODE GATE (SERVER_PUSH.md #20, PR 91a). A RESP2 connection in SUBSCRIBE
    // mode may run ONLY the pub/sub control set + PING/QUIT/RESET; anything else is rejected
    // with the byte-exact Redis "allowed in this context" error. RESP3 has NO restriction (a
    // RESP3 subscriber may run any command, per HELLO 3 semantics), so this gate is RESP2-only.
    //
    // The pub/sub commands themselves (SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PUBLISH)
    // are intercepted in the SERVE layer (`route_and_dispatch`) BEFORE dispatch -- registration
    // needs the per-connection push sender + the per-shard table, which live there -- so this
    // gate never sees them; it only sees the non-pubsub commands a subscriber tries to run.
    // PING/QUIT/RESET DO reach dispatch and must stay allowed (Redis allows them in this mode),
    // so they are in the allow set here. PSUBSCRIBE/PUNSUBSCRIBE are listed for forward
    // compatibility (PR 91b) so the allow set is the full Redis set even before they are wired.
    if state.is_subscriber()
        && state.proto == ProtoVersion::Resp2
        && !matches!(
            cmd,
            b"SUBSCRIBE"
                | b"UNSUBSCRIBE"
                | b"PSUBSCRIBE"
                | b"PUNSUBSCRIBE"
                | b"PING"
                | b"QUIT"
                | b"RESET"
        )
    {
        let name = String::from_utf8_lossy(cmd).to_ascii_lowercase();
        return Value::error(ErrorReply::subscribe_mode(&name));
    }

    // THE QUEUE GATE (TRANSACTIONS.md "queue then apply", PR-10a). While inside a
    // transaction, every command EXCEPT the control commands MULTI/EXEC/DISCARD (and
    // RESET/QUIT, which act on the connection itself) is QUEUED rather than executed:
    //   - validate it against the command table (known command + table arity). On a
    //     queue-time error (unknown command / wrong arity), reply the error NOW and
    //     mark the transaction dirty, so EXEC returns -EXECABORT and applies nothing.
    //   - otherwise stage a CLONE of the request and reply +QUEUED.
    // The control commands fall through to their arms in `dispatch_inner`.
    //
    // WATCH (PR-10b) is SPECIAL: WATCH inside MULTI is rejected with `-ERR WATCH inside
    // MULTI is not allowed` and must NOT dirty the transaction (the txn stays open +
    // clean, so a following EXEC still runs). So WATCH is in the queue-gate exclusion set
    // (it does not queue) and its `dispatch_inner` arm returns the error when `in_multi`.
    // UNWATCH, by contrast, is a NORMAL command inside MULTI: it QUEUES like any other (it
    // is NOT in the exclusion set) and runs at EXEC (a no-op there, since the dirty-CAS
    // already ran + cleared the watches at EXEC entry).
    //
    // The exclusion set is the `control` flag of the #89 single-source-of-truth command
    // registry ([`command_spec::spec_of`]): the 6 control verbs (MULTI/EXEC/DISCARD/RESET/
    // QUIT/WATCH) carry `control: true` and NOTHING else does (asserted by
    // `command_spec::tests::control_set_is_exactly_the_six_queue_gate_verbs`), so this reads
    // the registry instead of an inline `matches!`. An unknown command (not in the registry)
    // is NOT control, so it correctly falls through to `queue_validate` (which rejects it
    // with the unknown-command error and dirties the txn), exactly as before.
    if state.in_multi && !crate::command_spec::spec_of(cmd).is_some_and(|s| s.control) {
        return match cmd_txn::queue_validate(cmd, &req.args) {
            Ok(()) => {
                state.queued.push(req.clone());
                Value::simple("QUEUED")
            }
            Err(e) => {
                state.dirty_exec = true;
                Value::error(e)
            }
        };
    }

    let reply = dispatch_inner(
        ctx, state, env, store, wheel, now, rollup, cmdstats, keyspace, mem, deltas, req, cmd,
    );

    // KEYSPACE NOTIFICATIONS (PROD-8): map this just-executed command + its reply to the Redis
    // keyspace/keyevent event(s) and RECORD them into the per-shard pending buffer (the serve loop
    // drains + publishes them after the reply). The mapping helper's first action per recorded
    // event is the disabled-flags short-circuit (the flags snapshot we set at the top of this fn),
    // so on the default deployment this is a single `Cell` read for the few commands in the table
    // and nothing for the rest -- byte-identical. DEL/UNLINK record their own per-deleted-key
    // events inside the handler (the reply is a count, not which keys), so they are NOT in the
    // table here; the active TTL drain above already recorded `expired` for reaped keys.
    crate::notify::notify_for_command(cmd, req, &reply, state.db);

    reply
}

/// The command body: the per-command `maxmemory` admission gate followed by the big
/// command-routing match. Split out from [`dispatch`] so `EXEC` can re-run each queued
/// command here against the ALREADY-BORROWED store/wheel/env (PR-10a re-entrancy, see
/// `dispatch`'s doc). The caller passes the uppercased command token `cmd` so this
/// fn does not re-uppercase per queued command.
///
/// The dispatcher is one large command-routing match (the command table); its arms grow
/// as commands land. The big-match shape is the intended structure, so the line-count
/// lint is allowed here alongside the arg-count one.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn dispatch_inner<E: Env, S: Store + Admit + ActiveExpiry + Keyspace + PolicySwap + Watch>(
    ctx: &ServerContext,
    state: &mut ConnState,
    env: &mut E,
    store: &mut S,
    wheel: &mut TimingWheel,
    now: UnixMillis,
    rollup: RollupFn<'_>,
    cmdstats: CmdStatsFn<'_>,
    keyspace: KeyspaceFn<'_>,
    mem: MemoryInfo,
    deltas: &mut CounterDeltas,
    req: &Request,
    cmd: &[u8],
) -> Value {
    let db = state.db;
    // The arms below are HAND-SYNCED with the queue-time arity table in
    // [`cmd_txn::arity_of`] (every dispatch arm has a table entry, and vice versa). The
    // sync is guarded by the bidirectional + count cross-check
    // `cmd_txn::tests::table_covers_every_dispatch_arm`; if you add or remove a command
    // arm here, update that table (and its hand-listed `dispatch_arms`) too. A true
    // single-source-of-truth table that removes the hand-sync is the tracked follow-up
    // (#89).
    match cmd {
        b"PING" => cmd_ping(req),
        b"ECHO" => cmd_echo(req),
        b"LOLWUT" => cmd_lolwut(req),
        b"HELLO" => cmd_hello(ctx, state, req),
        b"AUTH" => cmd_auth(ctx, state, req),
        b"SELECT" => cmd_select(ctx, state, req),
        b"QUIT" => {
            state.should_close = true;
            Value::ok()
        }
        b"RESET" => {
            // RESET clears any WATCH set too (TRANSACTIONS.md, PR-10b): deregister the
            // watches from the store FIRST (the store holds the per-key watcher counts),
            // then clear the connection-side list, then run the rest of the reset (which
            // also aborts an open MULTI via clear_txn).
            store.unwatch(&state.watch);
            state.clear_watch();
            state.reset(ctx.requires_auth());
            Value::SimpleString("RESET".to_owned())
        }
        // READONLY / READWRITE (REPLICA_READ.md #147, HA-7d): set / clear the per-connection
        // read-only bit. On a REPLICA the bit lets a keyed READ for a replicated slot be served
        // locally; on any node it always replies +OK (Redis: these are accepted unconditionally,
        // the bit only changes routing). Each takes exactly the command token (arity 1).
        b"READONLY" => {
            if req.args.len() != 1 {
                return Value::error(ErrorReply::wrong_arity("readonly"));
            }
            state.readonly = true;
            Value::ok()
        }
        b"READWRITE" => {
            if req.args.len() != 1 {
                return Value::error(ErrorReply::wrong_arity("readwrite"));
            }
            state.readonly = false;
            Value::ok()
        }
        // ASKING (HA-6 online slot migration): set the one-shot ASKING flag and reply +OK. In the
        // SERVE path this is intercepted by the router (which consumes the flag for the next
        // command); this home arm is the fallback (a non-cluster node, or any path that reaches
        // dispatch directly) so the command is never an unknown-command error and the flag is set
        // consistently. Arity 1, like READONLY/READWRITE.
        b"ASKING" => {
            if req.args.len() != 1 {
                return Value::error(ErrorReply::wrong_arity("asking"));
            }
            state.asking = true;
            Value::ok()
        }
        // -- Transaction control: MULTI/EXEC/DISCARD (PR-10a) + WATCH/UNWATCH (PR-10b),
        // TRANSACTIONS.md. These reach `dispatch_inner` only as direct client commands
        // (the queue gate in `dispatch` excludes MULTI/EXEC/DISCARD/RESET/QUIT/WATCH from
        // queueing); UNWATCH is a normal queued command inside MULTI and reaches here
        // either directly (outside MULTI) or replayed by EXEC. --
        b"MULTI" => {
            // MULTI takes exactly the command token. A wrong-arity MULTI issued INSIDE a
            // transaction DIRTIES it (Redis runs commandCheckArity BEFORE the MULTI queue
            // block, so an arity failure on ANY verb, control verbs included, calls
            // flagTransaction -> CLIENT_DIRTY_EXEC). The txn stays OPEN + dirty, so a later
            // clean EXEC returns EXECABORT. When NOT in_multi this is just a plain
            // wrong-arity error.
            if req.args.len() != 1 {
                if state.in_multi {
                    state.dirty_exec = true;
                }
                return Value::error(ErrorReply::wrong_arity("multi"));
            }
            if state.in_multi {
                return Value::error(ErrorReply::multi_nested());
            }
            state.enter_multi();
            Value::ok()
        }
        b"DISCARD" => {
            // A wrong-arity DISCARD issued INSIDE a transaction DIRTIES it (arity is
            // checked before the queue block; see the MULTI arm), leaving the txn open so a
            // later clean EXEC returns EXECABORT. NOT exiting MULTI on this path matches
            // Redis (the dirty bit is set, the queue is untouched).
            if req.args.len() != 1 {
                if state.in_multi {
                    state.dirty_exec = true;
                }
                return Value::error(ErrorReply::wrong_arity("discard"));
            }
            if !state.in_multi {
                return Value::error(ErrorReply::discard_without_multi());
            }
            // Drop the queue + leave transaction state, applying nothing. DISCARD also
            // clears the WATCH set (TRANSACTIONS.md, PR-10b): deregister from the store,
            // then clear the connection-side list, then drop the queue (clear_txn).
            store.unwatch(&state.watch);
            state.clear_watch();
            state.clear_txn();
            Value::ok()
        }
        b"WATCH" => cmd_watch(state, store, now, req),
        b"UNWATCH" => cmd_unwatch(state, store, req),
        b"EXEC" => exec_transaction(
            ctx, state, env, store, wheel, now, rollup, cmdstats, keyspace, mem, deltas, req,
        ),
        b"CLIENT" => cmd_client(ctx, state, env, req),
        b"COMMAND" => cmd_command(req),
        // SLOWLOG GET/LEN/RESET/HELP (PROD-7 operability). Reads/resets the node-level ring in
        // `ctx.slowlog`; the per-command timing HOOK that POPULATES the ring lives in the serve
        // layer (it needs the client addr/name + the Env clock). AlwaysHome, no key.
        b"SLOWLOG" => cmd_slowlog(ctx, req),
        // HOTKEYS START/STOP/GET/RESET (#428): the faithful Redis 8.6 hot-key tracker. Drives the
        // node-level container in `ctx.hotkeys`; the per-command recording HOOK that POPULATES it
        // lives in the serve layer (it needs the command's elapsed micros + keys). `now` supplies the
        // Env-clock unix-ms for the session timestamps. AlwaysHome, no key.
        b"HOTKEYS" => cmd_hotkeys(ctx, now, req),
        // MEMORY USAGE/DOCTOR/STATS/HELP (PROD-7). USAGE estimates one key's bytes via the store;
        // STATS reuses the observe gauges; DOCTOR is a human string. AlwaysHome; only USAGE keys.
        b"MEMORY" => cmd_memory(ctx, store, db, now, mem, req),
        // LATENCY RESET/HISTORY/LATEST/DOCTOR/HELP (PROD-7). Reads/resets the node-level monitor in
        // `ctx.latency`; the per-command SAMPLE that feeds it lives in the serve layer. AlwaysHome.
        b"LATENCY" => cmd_latency(ctx, req),
        // DEBUG conformance subset (#411): OBJECT/JMAP/SLEEP/SET-ACTIVE-EXPIRE/STRINGMATCH-LEN/
        // QUICKLIST-PACKED-THRESHOLD. AlwaysHome admin container; OBJECT reads its key on this
        // shard; SET-ACTIVE-EXPIRE toggles the node's active-expiry flag via `ctx.runtime`.
        b"DEBUG" => cmd_introspect::cmd_debug(&ctx.runtime, store, db, now, req),
        // INFO reads only the CLOCK half of the env seam (uptime); pass `env` as the
        // `&C: Clock` it needs. `store` is the SERVING shard's, used ONLY as the `# Keyspace`
        // fallback (`Keyspace::db_len`) when `keyspace` yields `None`; on a multi-shard node the
        // serve loop hands `keyspace` the NODE-WIDE per-db counts (#531) so INFO matches DBSIZE.
        b"INFO" => cmd_info(ctx, env, store, rollup, cmdstats, keyspace, mem, req),
        // CONFIG GET/SET/RESETSTAT/REWRITE/HELP (PR-4b). RESETSTAT signals the counter reset via
        // `deltas.reset_stats`; the serve loop honors it NODE-WIDE (#531) -- it zeroes the serving
        // shard's cell (ShardCounters::apply) AND fans the reset across every shard's cell.
        b"CONFIG" => cmd_config::cmd_config(ctx, deltas, req),
        // CLUSTER (cluster-disabled-but-introspectable, CLUSTER_CONTRACT.md #70, slice 1):
        // the read-only CLUSTER surface (KEYSLOT/MYID/INFO/SLOTS/SHARDS/NODES/...) plus the
        // cluster-disabled reject for mutating subcommands. AlwaysHome, never key-routed; it
        // reads only ctx.info (node id, listen addr, cluster_enabled). No store/wheel/state.
        b"CLUSTER" => cmd_cluster::cmd_cluster(ctx, req),
        // PERSISTENCE (#58): SAVE / BGSAVE / LASTSAVE. The REAL cross-shard save (dump every
        // shard's partition + commit the manifest) + the LASTSAVE timestamp live in the binary's
        // serve layer, which holds the concrete per-shard stores, the data_dir, and the env Clock
        // (the generic dispatch here sees only the storage WAIST, not a concrete store to dump).
        // The serve router INTERCEPTS these BEFORE the generic dispatch (exactly like the raft
        // CLUSTER mutator + the WholeKeyspace fan-out), so these arms are the PERSISTENCE-DISABLED
        // fallback (no data_dir / shards==1 generic path / any path that reaches dispatch
        // directly): a Redis-faithful success reply that is a no-op (nothing to dump through the
        // waist), so the command is never an unknown-command error. `LASTSAVE` with no committed
        // save reports `:0`.
        b"SAVE" => cmd_persist_save_fallback(req),
        b"BGSAVE" => cmd_persist_bgsave_fallback(req),
        b"LASTSAVE" => cmd_persist_lastsave_fallback(req),
        // GRACEFUL SHUTDOWN (#139, SHUTDOWN.md): the save-on-exit + the process exit-0 live in the
        // binary's serve layer (which holds the per-shard stores, the data_dir, and the env Clock),
        // and the serve router INTERCEPTS SHUTDOWN before this generic dispatch -- so this arm is
        // ONLY the never-intercepted fallback (a SHUTDOWN reaching dispatch directly, e.g. inside an
        // EXEC replay). It validates the NOSAVE/SAVE modifier grammar and returns a syntax error for
        // a bad modifier; it never exits the process here (the process exit is the serve layer's job,
        // which owns the runtime + the drain). A documented minor divergence from Redis, which would
        // exit; the serve-layer interception is the live path for every non-MULTI SHUTDOWN.
        b"SHUTDOWN" => cmd_shutdown_fallback(req),
        // BLOCKING list/zset pops (PROD-9): BLPOP / BRPOP / BLMOVE / BRPOPLPUSH / BLMPOP /
        // BZPOPMIN / BZPOPMAX / BZMPOP. The LIVE blocking path (the park-until-pushed-or-timeout)
        // is handled in the BINARY's serve layer (it needs the per-connection waker + the runtime
        // timer seam), which intercepts these BEFORE this generic dispatch -- exactly like the
        // persistence / SHUTDOWN / pub/sub interceptions. This arm is the NON-BLOCKING fallback
        // reached ONLY via an EXEC replay (a blocking command QUEUED inside a MULTI runs NON-
        // blocking at EXEC, Redis parity: it returns nil at once if every key is empty) or any path
        // that reaches dispatch directly. It ATTEMPTS the pop and replies the result / a parse or
        // WRONGTYPE error / the nil-array (never parks). The pop reuses the keyed store mutation
        // path, so keyspace notifications fire identically. `AlwaysHome`, so it is here (not in
        // `dispatch_keyed_data`).
        b"BLPOP" | b"BRPOP" | b"BLMOVE" | b"BRPOPLPUSH" | b"BLMPOP" | b"BZPOPMIN" | b"BZPOPMAX"
        | b"BZMPOP" => crate::cmd_block::cmd_block_nonblocking(store, db, now, cmd, req),
        // WAIT numreplicas timeout (PROD-9): the LIVE path (block until the replica ack quorum or
        // the timeout) is handled in the serve layer (it reads the runtime in-sync replica count +
        // the timer seam). This arm is the EXEC-replay / direct-dispatch fallback: it validates
        // arity + the integer args and replies the CURRENT in-sync replica count IMMEDIATELY (no
        // block), which on a single node / no replicas is `:0`. The serve-layer interception is the
        // live path for every non-MULTI WAIT.
        b"WAIT" => cmd_wait_fallback(ctx, req),
        // Every OTHER command is a KEYED-DATA command (or an unknown token): it touches
        // only store/wheel/db/now (+ env for the RNG-drawing members), NO ConnState. The
        // bodies live in [`dispatch_keyed_data`], the SINGLE keyed-arm definition that
        // BOTH this home path and the cross-shard [`dispatch_remote_keyed`] path call, so
        // the two cannot diverge (COORDINATOR.md #107). The maxmemory admission gate runs
        // INSIDE that helper (it is per-command, owned by the shard holding the key).
        _ => dispatch_keyed_data(ctx, env, store, wheel, db, now, deltas, req, cmd),
    }
}

/// The KEYED-DATA command bodies + the per-command `maxmemory` admission gate: the
/// SINGLE definition shared by the home path ([`dispatch_inner`]'s default arm) and the
/// cross-shard remote path ([`dispatch_remote_keyed`]), so a keyed command's behavior is
/// byte-identical whether it runs on its home shard or after a cross-thread hop
/// (COORDINATOR.md #107). FACTORED (not copy-pasted) precisely so the two paths cannot
/// drift.
///
/// It takes NO [`ConnState`]: every arm here keys on `args[1]` (or is an unknown token)
/// and touches only `store`/`wheel`/`db`/`now`/`deltas`, plus `env` for the RNG-drawing
/// members (RANDOMKEY is whole-keyspace and stays in `dispatch_inner`'s control set, but
/// SPOP/SRANDMEMBER/HRANDFIELD/ZRANDMEMBER draw a per-command seed through the Env RNG
/// seam, ADR-0003). `db` is supplied by the caller (`state.db` on the home path, the
/// `ShardWork.db` on the remote path).
///
/// The big-match shape is the intended structure (the command table), so the
/// line-count and arg-count lints are allowed here as on the parent.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn dispatch_keyed_data<E: Env, S: Store + Admit + ActiveExpiry + Keyspace>(
    ctx: &ServerContext,
    env: &mut E,
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    deltas: &mut CounterDeltas,
    req: &Request,
    cmd: &[u8],
) -> Value {
    // maxmemory admission (ADMISSION.md #128, ADR-0007). For a `denyoom` write, before
    // the command body: if the ceiling is enabled and this shard is STRICTLY OVER its
    // budget, either evict-to-fit (cache mode) or reply `-OOM` (datastore/noeviction).
    // The comparison is strict `>` to match Redis's getMaxmemoryState (evict.c):
    // memory is "under limit" when `used <= maxmemory`, so a write at EXACTLY
    // used==budget is served, and only used>budget triggers eviction/-OOM (the -OOM
    // string itself reads "used memory > 'maxmemory'"). Non-denyoom commands (reads,
    // DEL, Tier-0) are ALWAYS served, even over budget, so a client can read and free
    // under pressure.
    //
    // It runs PER QUEUED COMMAND in the EXEC loop (this helper is the dispatch_inner
    // default arm, which EXEC re-enters per queued command), matching Redis (denyoom is
    // evaluated per command at EXEC). A queued denyoom write that is over budget becomes
    // an `-OOM` error ELEMENT in the EXEC array; the batch continues (no rollback). On the
    // cross-shard remote path the gate runs against the OWNING shard's budget (the shard
    // holding the key owns its share of the maxmemory ceiling).
    if ctx.ceiling_enabled() && is_denyoom(cmd) {
        // OVER-LIMIT TRIGGER off the REAL allocator figure, not only the logical counter
        // (PROD-SAFETY #1/#2). `over_maxmemory` ORs (a) the PROCESS-GLOBAL allocator `used_memory`
        // vs the FULL `maxmemory` -- the figure that actually bounds RSS (the logical counter
        // undercounts ~2x via slab slack / table overhead, so it could let the host OOM), and
        // PROCESS-GLOBAL so a HOT shard sheds when the NODE is over even if its even-split budget
        // is not -- with (b) the prior per-shard logical-vs-budget test as the byte-unchanged
        // fallback when no allocator figure is available (system-allocator / MSVC / tests). Both
        // are cheap relaxed loads, only on a `denyoom` write while the ceiling is on.
        if ctx.over_maxmemory(store.used_memory()) {
            if store.policy_evicts() {
                // CACHE / EVICTING MODE (M1). The global gauge TRIGGERED eviction above (so the node
                // keeps shedding to work RSS down over ticks even when a hot shard's own even-split
                // budget is not exceeded). The eviction TARGET is this shard's even-split per-shard
                // budget (eviction-locality: a shard can only evict its OWN keys, COORDINATOR.md
                // #107). The per-command -OOM DECISION, however, is driven off the FRESH per-shard
                // LOGICAL figure (`over_per_shard_budget`), NOT the global gauge: the gauge is
                // ~100ms-stale (refreshed on the expiry tick) and the allocator may hold freed pages,
                // so re-checking the global gauge here would spuriously -OOM a write that eviction
                // LOGICALLY satisfied -- diverging from Redis, where an evicting policy clears OOM
                // within the command. So a successful eviction (logical now <= budget) ALLOWS the
                // write; only when eviction could NOT free enough (the write outruns it, or only
                // ineligible keys remain) does the shard remain logically over budget and -OOM. The
                // freed count is reported for INFO.
                deltas.evicted += store.evict_to_fit(ctx.per_shard_budget(), now);
                if ctx.over_per_shard_budget(store.used_memory()) {
                    return Value::error(ErrorReply::oom());
                }
            } else {
                // STRICT DATASTORE / NOEVICTION MODE. No eviction can clear the pressure, so the
                // global RSS gauge inside `over_maxmemory` stands as the HARD CEILING: a denyoom
                // write -OOMs whenever the node is over (global RSS > maxmemory, or the per-shard
                // logical fallback over budget). -OOM is the over-capacity behavior.
                return Value::error(ErrorReply::oom());
            }
        }
    }

    match cmd {
        // -- Data commands (PR-2a) over the storage waist. The two pure reads (GET,
        // STRLEN) feed the keyspace hit/miss counters (PR-3b): a found live key is a
        // hit, an absent/expired key a miss. --
        b"GET" => keyspace_counted(deltas, cmd_string::cmd_get(store, db, now, req)),
        // MGET / MSET are multi-key string commands (KeyedMulti). A co-located or
        // single-key invocation runs here directly; a SHARD-SPANNING invocation is
        // fanned out by the coordinator (crate::multikey) which runs per-shard sub-MGET/
        // sub-MSET requests through this same arm on each owning shard (COORDINATOR.md
        // #107, Stage 2a). MGET is read-only; MSET is a denyoom write (admission above).
        b"MGET" => cmd_string::cmd_mget(store, db, now, req),
        b"MSET" => cmd_string::cmd_mset(store, wheel, db, now, req),
        // MSETEX: atomic multi-key set with a shared expiration (Redis 8.4, #412). Takes the
        // wheel like MSET/SET (it registers each key's deadline when an expire is set).
        b"MSETEX" => cmd_string::cmd_msetex(store, wheel, db, now, req),
        b"SET" => cmd_string::cmd_set(store, wheel, db, now, req),
        b"SETNX" => cmd_string::cmd_setnx(store, db, now, req),
        b"GETSET" => cmd_string::cmd_getset(store, db, now, req),
        // STRLEN is intentionally NOT keyspace-counted: its absent reply Integer(0) is
        // indistinguishable from STRLEN of an empty string, so a reply-shape signal
        // would misclassify; the lookup-side hit/miss is left as a later refinement.
        b"STRLEN" => cmd_string::cmd_strlen(store, db, now, req),
        // -- Numeric RMW + APPEND (PR-2b) over the storage waist. --
        b"INCR" => cmd_string::cmd_incr(store, db, now, req),
        b"DECR" => cmd_string::cmd_decr(store, db, now, req),
        b"INCRBY" => cmd_string::cmd_incrby(store, db, now, req),
        b"DECRBY" => cmd_string::cmd_decrby(store, db, now, req),
        b"INCRBYFLOAT" => cmd_string::cmd_incrbyfloat(store, db, now, req),
        b"APPEND" => cmd_string::cmd_append(store, db, now, req, max_bulk_len(ctx)),
        // GETRANGE / SUBSTR (signed-range substring), SETRANGE (zero-pad-extend overwrite),
        // GETDEL (GET-then-DEL). GETRANGE/SUBSTR are NOT keyspace-counted: their absent reply
        // is the EMPTY bulk (indistinguishable from a real empty value), so a hit/miss signal
        // would misclassify -- the same reason STRLEN is uncounted. GETDEL's reply IS an
        // unambiguous found(bulk)/not-found(Null) signal, so it is counted (it is a real
        // lookup, like GET).
        b"GETRANGE" => cmd_string::cmd_getrange(store, db, now, req),
        b"SUBSTR" => cmd_string::cmd_substr(store, db, now, req),
        b"SETRANGE" => cmd_string::cmd_setrange(store, db, now, req, max_bulk_len(ctx)),
        b"GETDEL" => keyspace_counted(deltas, cmd_string::cmd_getdel(store, db, now, req)),
        // MSETNX is a multi-key all-or-nothing set; like MSET it runs here on co-located /
        // single-key invocations (a spanning MSETNX is kept home by the serve loop so the
        // atomic all-or-nothing holds; cross-shard atomic MSETNX is the Stage-3 follow-up).
        b"MSETNX" => cmd_string::cmd_msetnx(store, db, now, req),
        // DELIFEQ: compare-and-delete a string (Valkey 9.0, #412). NOT keyspace-counted (its
        // 0/1 reply conflates a missing key with a value mismatch, so it is no hit/miss signal).
        b"DELIFEQ" => cmd_string::cmd_delifeq(store, db, now, req),
        b"DEL" => cmd_keyspace::cmd_del(store, db, now, req),
        b"EXISTS" => cmd_keyspace::cmd_exists(store, db, now, req),
        b"TYPE" => cmd_keyspace::cmd_type(store, db, now, req),
        // -- DUMP / RESTORE (#129): the Redis-compatible serialization blob (string type). --
        b"DUMP" => cmd_dump::cmd_dump(store, db, now, req),
        b"RESTORE" => cmd_dump::cmd_restore(store, db, now, req),
        // -- TTL / EXPIRE family (PR-3b) over the frozen waist. TTL-setting commands
        // also register their new deadline in the per-shard timing wheel. --
        b"EXPIRE" => cmd_expire::cmd_expire(store, wheel, db, now, req),
        b"PEXPIRE" => cmd_expire::cmd_pexpire(store, wheel, db, now, req),
        b"EXPIREAT" => cmd_expire::cmd_expireat(store, wheel, db, now, req),
        b"PEXPIREAT" => cmd_expire::cmd_pexpireat(store, wheel, db, now, req),
        // TTL / PTTL / EXPIRETIME / PEXPIRETIME are TTL-family INTROSPECTION and use
        // LOOKUP_NOTOUCH in Redis: they do NOT update keyspace_hits/keyspace_misses
        // (src/expire.c ttlGenericCommand / expiretimeGenericCommand). Only GET/GETEX
        // count (the #8 fix).
        b"TTL" => cmd_expire::cmd_ttl(store, db, now, req),
        b"PTTL" => cmd_expire::cmd_pttl(store, db, now, req),
        b"EXPIRETIME" => cmd_expire::cmd_expiretime(store, db, now, req),
        b"PEXPIRETIME" => cmd_expire::cmd_pexpiretime(store, db, now, req),
        b"PERSIST" => cmd_expire::cmd_persist(store, db, now, req),
        b"GETEX" => keyspace_counted(deltas, cmd_expire::cmd_getex(store, wheel, db, now, req)),
        b"SETEX" => cmd_expire::cmd_setex(store, wheel, db, now, req),
        b"PSETEX" => cmd_expire::cmd_psetex(store, wheel, db, now, req),
        // -- Generic keyspace commands (PR-4a) over the additive Keyspace seam. These
        // are SINGLE-SHARD-PER-CONNECTION (the store IS this connection's whole
        // keyspace; no cross-shard routing exists yet, so SCAN/KEYS/DBSIZE/RANDOMKEY/
        // FLUSHDB cover the connection's entire keyspace). True cross-shard fan-out is
        // deferred to the coordinator (KEYSPACE.md); the cursor's reserved slot bits
        // are shaped for it. --
        b"KEYS" => cmd_keyspace::cmd_keys(store, db, now, req),
        b"SCAN" => cmd_keyspace::cmd_scan(store, db, now, req),
        b"DBSIZE" => cmd_keyspace::cmd_dbsize(store, db, req),
        b"RANDOMKEY" => {
            // RANDOMKEY's randomness enters through the Env RNG seam (ADR-0003,
            // KEYSPACE.md): the CALLER draws the index here and passes it in; the store
            // reads no RNG. Draw ONLY for RANDOMKEY so the per-command RNG stream is not
            // perturbed by other commands.
            let pick = if req.args.len() == 1 {
                env.rng().next_u64()
            } else {
                0
            };
            cmd_keyspace::cmd_randomkey(store, db, pick, now, req)
        }
        b"RENAME" => cmd_keyspace::cmd_rename(store, db, now, req),
        b"RENAMENX" => cmd_keyspace::cmd_renamenx(store, db, now, req),
        b"COPY" => cmd_keyspace::cmd_copy(store, ctx, db, now, req),
        b"MOVE" => cmd_keyspace::cmd_move(store, ctx, db, now, req),
        b"SWAPDB" => cmd_keyspace::cmd_swapdb(store, ctx, req),
        b"TOUCH" => cmd_keyspace::cmd_touch(store, db, now, req),
        // UNLINK is DEL today: there is no async background free yet (#51), so it
        // removes synchronously and counts the same keys. Documented in the handler.
        b"UNLINK" => cmd_keyspace::cmd_unlink(store, db, now, req),
        b"FLUSHDB" => cmd_keyspace::cmd_flushdb(store, db, req),
        b"FLUSHALL" => cmd_keyspace::cmd_flushall(store, req),
        // -- List commands (PR-5) over the in-place-mutation RMW extension. Mutating
        // commands route through `rmw_mut` (OccupiedMut/Mutated) or Insert (create) /
        // Delete (emptied); reads through `rmw_mut` with Keep. WRONGTYPE on a non-list.
        // Blocking variants (BLPOP/...) are DEFERRED. --
        b"LPUSH" => cmd_list::cmd_lpush(store, db, now, req),
        b"RPUSH" => cmd_list::cmd_rpush(store, db, now, req),
        b"LPUSHX" => cmd_list::cmd_lpushx(store, db, now, req),
        b"RPUSHX" => cmd_list::cmd_rpushx(store, db, now, req),
        b"LPOP" => cmd_list::cmd_lpop(store, db, now, req),
        b"RPOP" => cmd_list::cmd_rpop(store, db, now, req),
        b"LLEN" => cmd_list::cmd_llen(store, db, now, req),
        b"LRANGE" => cmd_list::cmd_lrange(store, db, now, req),
        b"LINDEX" => cmd_list::cmd_lindex(store, db, now, req),
        b"LSET" => cmd_list::cmd_lset(store, db, now, req),
        b"LINSERT" => cmd_list::cmd_linsert(store, db, now, req),
        b"LREM" => cmd_list::cmd_lrem(store, db, now, req),
        b"LTRIM" => cmd_list::cmd_ltrim(store, db, now, req),
        b"LMOVE" => cmd_list::cmd_lmove(store, db, now, req),
        b"RPOPLPUSH" => cmd_list::cmd_rpoplpush(store, db, now, req),
        b"LPOS" => cmd_list::cmd_lpos(store, db, now, req),
        // LMPOP: pop from the FIRST non-empty list among the named keys (the non-blocking
        // multi-key pop). Multi-key, but runs here on co-located / single-key invocations (a
        // spanning LMPOP is kept home by the serve loop so the "first non-empty wins" order
        // holds across the named keys on the one store).
        b"LMPOP" => cmd_list::cmd_lmpop(store, db, now, req),
        // -- Hash commands (PR-6) over the in-place-mutation RMW extension. Mutating
        // commands route through `rmw_mut` (OccupiedMut/Mutated) or Insert (create) /
        // Delete (emptied); reads through `rmw_mut` with Keep. WRONGTYPE on a non-hash.
        // HRANDFIELD's randomness enters through the Env RNG seam (the caller draws the
        // seed here, like RANDOMKEY). HSCAN reuses the SCAN hash-ordered cursor over the
        // hash's own field table. --
        b"HSET" => cmd_hash::cmd_hset(store, db, now, req),
        b"HMSET" => cmd_hash::cmd_hmset(store, db, now, req),
        b"HSETNX" => cmd_hash::cmd_hsetnx(store, db, now, req),
        b"HGET" => cmd_hash::cmd_hget(store, db, now, req),
        b"HMGET" => cmd_hash::cmd_hmget(store, db, now, req),
        b"HDEL" => cmd_hash::cmd_hdel(store, db, now, req),
        b"HGETALL" => cmd_hash::cmd_hgetall(store, db, now, req),
        b"HKEYS" => cmd_hash::cmd_hkeys(store, db, now, req),
        b"HVALS" => cmd_hash::cmd_hvals(store, db, now, req),
        b"HLEN" => cmd_hash::cmd_hlen(store, db, now, req),
        b"HEXISTS" => cmd_hash::cmd_hexists(store, db, now, req),
        b"HSTRLEN" => cmd_hash::cmd_hstrlen(store, db, now, req),
        b"HINCRBY" => cmd_hash::cmd_hincrby(store, db, now, req),
        b"HINCRBYFLOAT" => cmd_hash::cmd_hincrbyfloat(store, db, now, req),
        b"HEXPIRE" => cmd_hash::cmd_hexpire(store, db, now, req),
        b"HPEXPIRE" => cmd_hash::cmd_hpexpire(store, db, now, req),
        b"HEXPIREAT" => cmd_hash::cmd_hexpireat(store, db, now, req),
        b"HPEXPIREAT" => cmd_hash::cmd_hpexpireat(store, db, now, req),
        b"HTTL" => cmd_hash::cmd_httl(store, db, now, req),
        b"HPTTL" => cmd_hash::cmd_hpttl(store, db, now, req),
        b"HEXPIRETIME" => cmd_hash::cmd_hexpiretime(store, db, now, req),
        b"HPEXPIRETIME" => cmd_hash::cmd_hpexpiretime(store, db, now, req),
        b"HPERSIST" => cmd_hash::cmd_hpersist(store, db, now, req),
        b"HGETDEL" => cmd_hash::cmd_hgetdel(store, db, now, req),
        b"HGETEX" => cmd_hash::cmd_hgetex(store, db, now, req),
        b"HSETEX" => cmd_hash::cmd_hsetex(store, db, now, req),
        b"HRANDFIELD" => {
            // HRANDFIELD's randomness enters through the Env RNG seam (ADR-0003,
            // KEYSPACE.md): the CALLER draws the seed here and passes it in; the store +
            // handler read no RNG. Draw ONLY for HRANDFIELD so the per-command RNG stream
            // is not perturbed by other commands (mirrors the RANDOMKEY draw block).
            let seed = env.rng().next_u64();
            cmd_hash::cmd_hrandfield(store, db, seed, now, req)
        }
        b"HSCAN" => cmd_hash::cmd_hscan(store, db, now, req),
        // -- Set commands (PR-7) over the in-place-mutation RMW extension. Mutating
        // commands route through `rmw_mut` (OccupiedMut/Mutated) or Insert (create) /
        // Delete (emptied); reads through `rmw_mut` with Keep. WRONGTYPE on a non-set; a
        // MISSING source key is treated as an EMPTY set for the read/algebra commands.
        // SPOP/SRANDMEMBER randomness enters through the Env RNG seam (the caller draws the
        // seed here, like HRANDFIELD/RANDOMKEY). SSCAN reuses the SCAN hash-ordered cursor
        // over the set's own member table. The multi-key reads (SINTER/...) and the *STORE
        // writes operate on this connection's accept shard (single-shard-per-connection,
        // like the keyspace commands); no cross-shard fan-out. --
        b"SADD" => cmd_set::cmd_sadd(store, db, now, req),
        b"SREM" => cmd_set::cmd_srem(store, db, now, req),
        b"SMEMBERS" => cmd_set::cmd_smembers(store, db, now, req),
        b"SISMEMBER" => cmd_set::cmd_sismember(store, db, now, req),
        b"SMISMEMBER" => cmd_set::cmd_smismember(store, db, now, req),
        b"SCARD" => cmd_set::cmd_scard(store, db, now, req),
        b"SPOP" => {
            // SPOP's randomness enters through the Env RNG seam (ADR-0003): the CALLER
            // draws the seed here and passes it in; the store + handler read no RNG. Draw
            // ONLY for SPOP so the per-command RNG stream is not perturbed by other
            // commands (mirrors the HRANDFIELD/RANDOMKEY draw blocks).
            let seed = env.rng().next_u64();
            cmd_set::cmd_spop(store, db, seed, now, req)
        }
        b"SRANDMEMBER" => {
            let seed = env.rng().next_u64();
            cmd_set::cmd_srandmember(store, db, seed, now, req)
        }
        b"SMOVE" => cmd_set::cmd_smove(store, db, now, req),
        b"SINTER" => cmd_set::cmd_sinter(store, db, now, req),
        b"SUNION" => cmd_set::cmd_sunion(store, db, now, req),
        b"SDIFF" => cmd_set::cmd_sdiff(store, db, now, req),
        b"SINTERCARD" => cmd_set::cmd_sintercard(store, db, now, req),
        b"SINTERSTORE" => cmd_set::cmd_sinterstore(store, db, now, req),
        b"SUNIONSTORE" => cmd_set::cmd_sunionstore(store, db, now, req),
        b"SDIFFSTORE" => cmd_set::cmd_sdiffstore(store, db, now, req),
        b"SSCAN" => cmd_set::cmd_sscan(store, db, now, req),
        // -- Sorted-set (zset) commands (PR-8, COMMANDS.md zset semantics). --
        b"ZADD" => cmd_zset::cmd_zadd(store, db, now, req),
        b"ZINCRBY" => cmd_zset::cmd_zincrby(store, db, now, req),
        b"ZREM" => cmd_zset::cmd_zrem(store, db, now, req),
        b"ZSCORE" => cmd_zset::cmd_zscore(store, db, now, req),
        b"ZMSCORE" => cmd_zset::cmd_zmscore(store, db, now, req),
        b"ZCARD" => cmd_zset::cmd_zcard(store, db, now, req),
        b"ZRANK" => cmd_zset::cmd_zrank(store, db, now, req),
        b"ZREVRANK" => cmd_zset::cmd_zrevrank(store, db, now, req),
        b"ZCOUNT" => cmd_zset::cmd_zcount(store, db, now, req),
        b"ZLEXCOUNT" => cmd_zset::cmd_zlexcount(store, db, now, req),
        b"ZRANGE" => cmd_zset::cmd_zrange(store, db, now, req),
        b"ZREVRANGE" => cmd_zset::cmd_zrevrange(store, db, now, req),
        b"ZRANGEBYSCORE" => cmd_zset::cmd_zrangebyscore(store, db, now, req),
        b"ZREVRANGEBYSCORE" => cmd_zset::cmd_zrevrangebyscore(store, db, now, req),
        b"ZRANGEBYLEX" => cmd_zset::cmd_zrangebylex(store, db, now, req),
        b"ZREVRANGEBYLEX" => cmd_zset::cmd_zrevrangebylex(store, db, now, req),
        b"ZREMRANGEBYRANK" => cmd_zset::cmd_zremrangebyrank(store, db, now, req),
        b"ZREMRANGEBYSCORE" => cmd_zset::cmd_zremrangebyscore(store, db, now, req),
        b"ZREMRANGEBYLEX" => cmd_zset::cmd_zremrangebylex(store, db, now, req),
        b"ZPOPMIN" => cmd_zset::cmd_zpopmin(store, db, now, req),
        b"ZPOPMAX" => cmd_zset::cmd_zpopmax(store, db, now, req),
        b"ZRANDMEMBER" => {
            // ZRANDMEMBER's randomness enters through the Env RNG seam (ADR-0003): the
            // CALLER draws the seed here and passes it in; the store + handler read no
            // RNG. Draw ONLY for ZRANDMEMBER so the per-command RNG stream is not perturbed
            // (mirrors the SRANDMEMBER/HRANDFIELD/RANDOMKEY draw blocks).
            let seed = env.rng().next_u64();
            cmd_zset::cmd_zrandmember(store, db, seed, now, req)
        }
        b"ZSCAN" => cmd_zset::cmd_zscan(store, db, now, req),
        b"ZRANGESTORE" => cmd_zset::cmd_zrangestore(store, db, now, req),
        b"ZUNION" => cmd_zset::cmd_zunion(store, db, now, req),
        b"ZINTER" => cmd_zset::cmd_zinter(store, db, now, req),
        b"ZDIFF" => cmd_zset::cmd_zdiff(store, db, now, req),
        b"ZUNIONSTORE" => cmd_zset::cmd_zunionstore(store, db, now, req),
        b"ZINTERSTORE" => cmd_zset::cmd_zinterstore(store, db, now, req),
        b"ZDIFFSTORE" => cmd_zset::cmd_zdiffstore(store, db, now, req),
        b"ZINTERCARD" => cmd_zset::cmd_zintercard(store, db, now, req),
        // ZMPOP: pop min/max from the FIRST non-empty zset among the named keys (the
        // non-blocking multi-key pop). Multi-key, runs here on co-located / single-key
        // invocations (a spanning ZMPOP is kept home by the serve loop).
        b"ZMPOP" => cmd_zset::cmd_zmpop(store, db, now, req),
        // -- Bitmap commands (PR-9, BITMAPS.md) over the STRING type. A bitmap is the
        // string value addressed at bit granularity (TYPE=string, OBJECT ENCODING a
        // string encoding); these need no new type. Mutations (SETBIT, BITOP-dest,
        // BITFIELD with SET/INCRBY) route through the string `rmw` rebuild-Replace path;
        // reads (GETBIT/BITCOUNT/BITPOS/BITFIELD-all-GET/BITFIELD_RO) through `read`.
        // WRONGTYPE on a non-string. BITOP is multi-key (reads sources + writes dest on
        // this connection's accept shard, single-shard-per-connection like the other
        // multi-key commands; an empty result deletes dest). --
        b"SETBIT" => cmd_bitmap::cmd_setbit(store, db, now, req, max_bulk_len(ctx)),
        b"GETBIT" => cmd_bitmap::cmd_getbit(store, db, now, req, max_bulk_len(ctx)),
        b"BITCOUNT" => cmd_bitmap::cmd_bitcount(store, db, now, req),
        b"BITPOS" => cmd_bitmap::cmd_bitpos(store, db, now, req),
        b"BITOP" => cmd_bitmap::cmd_bitop(store, db, now, req),
        b"BITFIELD" => cmd_bitmap::cmd_bitfield(store, db, now, req, max_bulk_len(ctx)),
        b"BITFIELD_RO" => cmd_bitmap::cmd_bitfield_ro(store, db, now, req, max_bulk_len(ctx)),
        // -- HyperLogLog commands (PR-11, COMMANDS.md HLL) over the STRING type. An HLL
        // is the dense (12304-byte) string object addressed opaquely (TYPE=string); these
        // need no new type. PFADD writes through the string `rmw` path (Insert on a new
        // key, Replace only when a register actually changed, Keep otherwise so a no-op
        // PFADD does not dirty a watched key); PFCOUNT is read-only (always recomputes the
        // cardinality, never writes back a cache); PFMERGE reads all sources + writes the
        // union to dest. WRONGTYPE on a non-string, or the HLL-invalid error on a string
        // that is not a valid dense HLL. The multi-key PFCOUNT/PFMERGE operate on this
        // connection's accept shard (single-shard-per-connection, like BITOP). --
        b"PFADD" => cmd_hll::cmd_pfadd(store, db, now, req),
        b"PFCOUNT" => cmd_hll::cmd_pfcount(store, db, now, req),
        b"PFMERGE" => cmd_hll::cmd_pfmerge(store, db, now, req),
        // -- PFDEBUG GETREG/ENCODING/TODENSE (#242 part 3): HLL introspection, key at args[2]
        // (routed like OBJECT via ObjectArg2). --
        b"PFDEBUG" => cmd_hll::cmd_pfdebug(store, db, now, req),
        // -- Generic SORT / SORT_RO (cmd_sort). SORT sorts the elements of a list/set/zset
        // (numeric or ALPHA) with LIMIT/ASC/DESC/BY/GET/STORE; SORT_RO is the read-only form
        // (no STORE). The BY/GET/STORE keys are dereferenced on this connection's accept shard
        // (single-shard-per-connection, like the other multi-key commands). --
        b"SORT" => cmd_sort::cmd_sort(store, db, now, req),
        b"SORT_RO" => cmd_sort::cmd_sort_ro(store, db, now, req),
        // -- Introspection: OBJECT ENCODING/REFCOUNT/IDLETIME/FREQ/HELP (PR-4a, #40). --
        b"OBJECT" => cmd_introspect::cmd_object(store, db, now, req),
        // -- INTERNAL cross-shard *STORE dest-write verb (COORDINATOR.md #107, Stage 2b).
        // `__ICSTORESET dest m...` writes a spanning set-*STORE result to the dest owner with
        // the EXACT blind-overwrite-clearing-TTL semantics the single-shard *STORE uses. This
        // arm is reached ONLY via the coordinator's internal dispatch (`dispatch_remote_keyed`
        // / `run_local_keyed`); a CLIENT sending `__ICSTORESET` is rejected before routing (the
        // serve-loop router + queue-time validator gate it), so it gets unknown-command and
        // never reaches here. --
        b"__ICSTORESET" => cmd_set::cmd_icstoreset(store, db, now, req),
        // -- INTERNAL cross-shard zset *STORE / ZRANGESTORE dest-write verb (COORDINATOR.md
        // #107, Stage 2b-2). `__ICSTOREZSET dest m1 s1 ...` writes a spanning zset *STORE /
        // ZRANGESTORE result to the dest owner with the EXACT blind-overwrite-clearing-TTL
        // semantics the single-shard *STORE / ZRANGESTORE uses. Reached ONLY via the
        // coordinator's internal dispatch (`dispatch_remote_keyed` / `run_local_keyed`); a
        // CLIENT sending `__ICSTOREZSET` is rejected before routing (the serve-loop router +
        // queue-time validator gate it), so it gets unknown-command and never reaches here. --
        b"__ICSTOREZSET" => cmd_zset::cmd_icstorezset(store, db, now, req),
        // -- INTERNAL cross-shard PFMERGE dest-write verb (COORDINATOR.md #107, Stage 2b-3).
        // `__ICSTOREHLL dest <dense-hll-bytes>` writes a spanning-PFMERGE merged HLL to the
        // dest owner with the EXACT TTL-PRESERVING semantics the single-shard PFMERGE uses
        // (so an existing dest TTL survives). Reached ONLY via the coordinator's internal
        // dispatch (`dispatch_remote_keyed` / `run_local_keyed`); a CLIENT sending
        // `__ICSTOREHLL` is rejected before routing (the serve-loop router gates it), so it
        // gets unknown-command and never reaches here. --
        b"__ICSTOREHLL" => cmd_hll::cmd_icstorehll(store, db, now, req),
        _ => {
            let name = String::from_utf8_lossy(req.command()).into_owned();
            let rest: Vec<&[u8]> = req.args[1..].iter().map(bytes::Bytes::as_ref).collect();
            Value::error(ErrorReply::unknown_command(&name, &rest))
        }
    }
}

/// Run ONE keyed data command (a [`route::CommandClass::KeyedSingle`](crate::route) OR a
/// [`route::CommandClass::KeyedMulti`](crate::route) whose keys all co-locate on one shard)
/// on the shard that OWNS its key(s), after a cross-thread hop (COORDINATOR.md #107, Stage
/// 1). This is the REMOTE counterpart of the home [`dispatch`] fast path: the coordinator's
/// per-shard drain loop calls it on the OWNING shard with that shard's OWN store/wheel/env,
/// so a `SET k v` issued on a connection homed on shard 0 lands in shard `owner_shard(k)`'s
/// partition and a later `GET k` (or `DEL k`) on any connection finds it.
///
/// It runs the SAME keyed-arm bodies the home path does (via the shared
/// [`dispatch_keyed_data`], so the two cannot diverge), preceded by the two per-command
/// shard-owned steps the home `dispatch` also runs and the owning shard still owns:
///   1. the `maxmemory-policy` hot-swap generation check (CONFIG.md, PR-4b) against THIS
///      shard's `shard_generation` (so a `CONFIG SET maxmemory-policy` reaches the owning
///      shard on its next remote command too);
///   2. the active-expiry wheel drain (EXPIRATION.md #51) on THIS shard's wheel/store.
///
/// The maxmemory admission gate runs INSIDE `dispatch_keyed_data` against THIS shard's
/// budget (the owning shard owns its share of the ceiling).
///
/// It has NO [`ConnState`]: it is only ever called for a keyed data command (the serve loop
/// classifies + extracts keys before hopping), which by construction touches no connection
/// state. `now` is read by the CALLER from the OWNING shard's Env clock (the determinism
/// seam, ADR-0003), NOT supplied by the home shard, so a seeded replay reaps/expires
/// identically on the owning core. `deltas` accumulates this command's counter changes; the
/// caller folds them into the OWNING shard's counters (where the data lives) and ships a
/// copy back so the home core does not double-count.
///
/// If somehow handed a non-keyed command (a classification bug; never happens given the
/// serve loop's `route::classify` + `command_keys` gate), it returns an internal error
/// rather than silently running a control command without its `ConnState`.
#[allow(clippy::too_many_arguments)]
pub fn dispatch_remote_keyed<E: Env, S: Store + Admit + ActiveExpiry + Keyspace + PolicySwap>(
    ctx: &ServerContext,
    env: &mut E,
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    shard_generation: &mut u64,
    deltas: &mut CounterDeltas,
    req: &Request,
) -> Value {
    *deltas = CounterDeltas::default();
    let cmd = ascii_upper(req.command());

    // Defense in depth: only KEYED data commands are ever routed here (COORDINATOR.md #107,
    // Stage 1). KeyedSingle (args[1] key) AND KeyedMulti (DEL/EXISTS/RENAME/SINTER/BITOP/
    // PFCOUNT/.../OBJECT) are BOTH routable: the serve loop routes a keyed command whose
    // keys ALL resolve to one shard, and every such handler is ConnState-free (it runs via
    // the shared `dispatch_keyed_data` arms below), so it executes correctly after the hop.
    // A control/conn/txn (AlwaysHome) or whole-keyspace command reaching this path is a
    // classification bug; refuse it rather than run it without the ConnState / fan-out it
    // needs. (A key-SPANNING multi-key command is kept HOME by the serve loop and never
    // reaches here -- the Stage 2 fan-out gap.)
    if !matches!(
        crate::route::classify(&cmd),
        crate::route::CommandClass::KeyedSingle | crate::route::CommandClass::KeyedMulti
    ) {
        return Value::error(ErrorReply::err(
            "command routed cross-shard is not key-routable",
        ));
    }

    // KEYSPACE NOTIFICATIONS (PROD-8): snapshot the live flags into THIS (owning) shard's emit
    // gate, exactly like the home `dispatch_with_cmd`, so a CROSS-SHARD keyed write records its
    // keyspace events on the OWNER shard (where the mutation runs). The owner shard's drain loop
    // drains + publishes them. Disabled-default short-circuit keeps this byte-identical when off.
    ironcache_config::notify::set_command_flags(ctx.runtime.notify_flags());

    // (1) maxmemory-policy HOT-SWAP reach on the owning shard (CONFIG.md, PR-4b): one
    // relaxed atomic load + compare; the rebuild (rare) is the shared helper.
    maybe_hot_swap_policy(ctx, env, store, shard_generation, now);

    // (2) Active TTL reclamation on the owning shard (EXPIRATION.md #51), BEFORE the
    // command body, exactly like the home `dispatch`: drain a BOUNDED batch of due keys
    // from THIS shard's wheel and reap the genuinely-expired ones (the SAME
    // `drain_due_keys` helper). Bounds resident memory for expired keys under traffic.
    // `drain_due_keys` records an `expired` keyspace notification per reaped key (gated off
    // by default). GATED on the runtime active-expire flag (`DEBUG SET-ACTIVE-EXPIRE`, #411)
    // exactly like the home `dispatch`, so a node-wide toggle reaches the owner shard too.
    if ctx.runtime.active_expire_enabled() {
        deltas.expired += drain_due_keys(wheel, store, now, MAX_RECLAIM_PER_CALL);
    }

    // (3) The keyed command body + the per-command admission gate, via the SINGLE shared
    // keyed-arm definition (so home and remote cannot diverge).
    let reply = dispatch_keyed_data(ctx, env, store, wheel, db, now, deltas, req, &cmd);

    // KEYSPACE NOTIFICATIONS (PROD-8): map the just-executed cross-shard keyed command + its reply
    // to the keyspace event(s) and RECORD them on THIS owner shard (the home `dispatch_with_cmd`
    // does the same after `dispatch_inner`). DEL/UNLINK record their own per-key events inside the
    // handler. The owner shard's drain loop drains + publishes the recorded events.
    crate::notify::notify_for_command(&cmd, req, &reply, db);

    reply
}

/// Run ONE shard's PARTIAL of a [`route::CommandClass::WholeKeyspace`](crate::route)
/// command against THIS shard's partition, for the cross-shard scatter-gather fan-out
/// (COORDINATOR.md #107, the whole-keyspace pass). The coordinator's home core sends the
/// SAME request to every other shard (and runs it locally on the home shard); each shard
/// returns its slice's result, which the home core MERGES:
///   - DBSIZE: this shard's key count (the home core sums the per-shard integers).
///   - KEYS pattern: this shard's matching keys (the home core concatenates the arrays).
///   - SCAN cursor ...: this shard's scan_step batch over the per-shard INNER cursor the
///     home core rewrote into `args[1]` before sending (the home core decodes/encodes the
///     COMPOSITE cursor; each shard runs the plain single-shard SCAN against its partition).
///   - FLUSHDB / FLUSHALL: clear this shard's selected db / all dbs, returning `+OK`.
///   - RANDOMKEY: a random live key from this shard's selected db, or nil if it has none.
///
/// Unlike a KEYED hop the whole-keyspace partials touch NO single owned key: they run the
/// SAME `cmd_keyspace::*` handlers the home `dispatch_keyed_data` arms call (so the per
/// -shard behavior is byte-identical to the single-shard path), needing only `db` (+ the
/// Env RNG seam for RANDOMKEY's index, ADR-0003) and NO [`ConnState`]. They do NOT run the
/// active-expiry drain or the maxmemory admission gate (a read-only count/iterate or a
/// flush is not a `denyoom` write; FLUSH frees memory), so this path is lean.
///
/// `db` is the issuing connection's selected DB (the `ShardWork.db`). It returns an
/// internal error if handed a non-whole-keyspace command (a coordinator classification
/// bug; the serve loop only fans out WholeKeyspace commands here).
pub fn dispatch_remote_whole_keyspace<E: Env, S: Store + Keyspace>(
    env: &mut E,
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    let cmd = ascii_upper(req.command());

    // Defense in depth: only WholeKeyspace commands are fanned out here. Anything else is
    // a coordinator classification bug; refuse it rather than run it on a wrong path. The three
    // internal verbs are allow-listed: they are NOT in `spec_of` (so `classify` returns
    // `AlwaysHome`), but the serve loop only ever emits them in exactly the whole-keyspace
    // broadcast shape -- the two #371 slot-scans (rewritten from a cluster-mode CLUSTER slot-scan)
    // and the #531 `__ICINFOKEYSPACE` node-wide INFO keyspace gather.
    let is_internal_whole_keyspace = cmd == crate::command_spec::ICCOUNTKEYSINSLOT
        || cmd == crate::command_spec::ICGETKEYSINSLOT
        || cmd == crate::command_spec::ICINFOKEYSPACE;
    if !is_internal_whole_keyspace
        && !matches!(
            crate::route::classify(&cmd),
            crate::route::CommandClass::WholeKeyspace
        )
    {
        return Value::error(ErrorReply::err(
            "command fanned out cross-shard is not whole-keyspace",
        ));
    }

    match cmd.as_slice() {
        // Per-shard partials over the Keyspace seam (no ConnState, no admission/expiry).
        b"DBSIZE" => cmd_keyspace::cmd_dbsize(store, db, req),
        b"KEYS" => cmd_keyspace::cmd_keys(store, db, now, req),
        b"SCAN" => cmd_keyspace::cmd_scan(store, db, now, req),
        b"FLUSHDB" => cmd_keyspace::cmd_flushdb(store, db, req),
        b"FLUSHALL" => cmd_keyspace::cmd_flushall(store, req),
        b"RANDOMKEY" => {
            // RANDOMKEY's index enters through the Env RNG seam (ADR-0003) on THIS shard,
            // mirroring the home `dispatch_keyed_data` arm: the caller draws it here; the
            // store reads no RNG. Each shard returns its own random key (or nil); the home
            // core then picks ONE among the non-nil shard replies (also via the Env seam).
            let pick = if req.args.len() == 1 {
                env.rng().next_u64()
            } else {
                0
            };
            cmd_keyspace::cmd_randomkey(store, db, pick, now, req)
        }
        // #371: the per-shard partials of CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT, which the serve
        // loop rewrites into these internal whole-keyspace verbs. `args[1]` is the (pre-validated)
        // slot; for GET `args[2]` is the count. The slot/count parses are defensive (the serve loop
        // only routes a validated command here), falling back to an empty answer, never a panic.
        b"__ICCOUNTKEYSINSLOT" => match whole_keyspace_slot_arg(req) {
            Some(slot) => {
                Value::Integer(cmd_keyspace::count_keys_in_slot(store, db, slot, now) as i64)
            }
            None => Value::Integer(0),
        },
        b"__ICGETKEYSINSLOT" => match whole_keyspace_slot_arg(req) {
            Some(slot) => {
                let limit = whole_keyspace_count_arg(req);
                let keys = cmd_keyspace::keys_in_slot(store, db, slot, limit, now);
                Value::Array(Some(
                    keys.into_iter()
                        .map(|k| Value::bulk(k.into_vec()))
                        .collect(),
                ))
            }
            None => Value::Array(Some(Vec::new())),
        },
        // #531: THIS shard's per-db key counts for the node-wide INFO `# Keyspace`. `args[1]` is the
        // `databases` count (from the issuing node's config); reply an Array of that many Integers,
        // `[db_len(0), db_len(1), ...]`, which the home core SUMS element-wise (the SAME db_len each
        // shard's DBSIZE partial reports, so the merged per-db totals equal DBSIZE). A missing /
        // malformed count is defensive-zero (an empty array), never a panic; the serve loop always
        // supplies the config's `databases`.
        b"__ICINFOKEYSPACE" => {
            let databases = whole_keyspace_databases_arg(req);
            Value::Array(Some(
                (0..databases)
                    .map(|i| Value::Integer(store.db_len(i) as i64))
                    .collect(),
            ))
        }
        // The classify gate above already excludes everything else.
        _ => Value::error(ErrorReply::err(
            "command fanned out cross-shard is not whole-keyspace",
        )),
    }
}

/// Parse the slot argument (`args[1]`) of an `__ICCOUNTKEYSINSLOT`/`__ICGETKEYSINSLOT` internal
/// request into an in-range slot, or `None` if absent / not an integer / out of `[0, 16384)`.
fn whole_keyspace_slot_arg(req: &Request) -> Option<u16> {
    let raw = req.args.get(1)?;
    let n: u16 = std::str::from_utf8(raw).ok()?.parse().ok()?;
    (n < ironcache_protocol::CLUSTER_SLOTS).then_some(n)
}

/// Parse the count argument (`args[2]`) of an `__ICGETKEYSINSLOT` internal request into a
/// non-negative limit, or `0` if absent / malformed (an empty result, never a panic).
fn whole_keyspace_count_arg(req: &Request) -> usize {
    req.args
        .get(2)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&n| n >= 0)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0)
}

/// Parse the `databases` argument (`args[1]`) of an `__ICINFOKEYSPACE` internal request (#531): the
/// number of logical DBs the issuing node is configured with, so this shard reports `db_len(0)`..
/// `db_len(databases-1)`. Returns `0` (an empty per-db array) if absent / malformed -- the serve
/// loop always supplies the config's `databases`, so this is purely defensive, never a panic.
fn whole_keyspace_databases_arg(req: &Request) -> u32 {
    req.args
        .get(1)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0)
}

/// `WATCH key [key ...]` (TRANSACTIONS.md per-key dirty-CAS, PR-10b). Marks each key
/// watched on the connection's accept shard: per key, [`Watch::watch_snapshot`] records
/// the key's current version + present/absent status into a [`WatchEntry`] pushed onto
/// `state.watch`. Replies `+OK`.
///
/// A WELL-FORMED WATCH inside MULTI is rejected with `-ERR WATCH inside MULTI is not
/// allowed` WITHOUT dirtying the transaction (the txn stays open + clean, so a following
/// EXEC still runs): the queue gate excludes WATCH from queueing, and this arm returns the
/// error when `in_multi`. WATCH is arity -2 (the command token + at least one key); the
/// queue-gate arity table holds the same value, but WATCH outside MULTI is validated here
/// too (the gate only runs inside MULTI), so check it explicitly. The arity check runs
/// BEFORE the in-MULTI rejection (matching the MULTI/DISCARD/EXEC arms and Redis's
/// pre-queue commandCheckArity -> flagTransaction), so a MALFORMED WATCH (no keys) inside
/// MULTI DIRTIES the transaction (`dirty_exec`), and a later clean EXEC returns EXECABORT.
///
/// SINGLE-SHARD-PER-CONNECTION (PR-10b scope): every watched key is registered on this
/// connection's accept shard `store`; a watched key on a different shard is out of scope
/// (cross-shard EXEC, COORDINATOR.md #29).
fn cmd_watch<S: Store + Watch>(
    state: &mut ConnState,
    store: &mut S,
    now: UnixMillis,
    req: &Request,
) -> Value {
    // Arity -2: command token + >= 1 key. Checked FIRST (matching the MULTI/DISCARD/EXEC
    // arms): Redis runs commandCheckArity -> flagTransaction at the pre-queue arity check,
    // so a malformed WATCH issued INSIDE a transaction DIRTIES it (a later clean EXEC then
    // returns EXECABORT). Only AFTER arity passes does the WATCH-inside-MULTI rejection
    // apply (a well-formed WATCH inside MULTI is the legal-but-disallowed case that leaves
    // the txn OPEN + CLEAN).
    if req.args.len() < 2 {
        if state.in_multi {
            state.dirty_exec = true;
        }
        return Value::error(ErrorReply::wrong_arity("watch"));
    }
    // WATCH inside MULTI: error, txn left OPEN + CLEAN (no dirty_exec).
    if state.in_multi {
        return Value::error(ErrorReply::watch_inside_multi());
    }
    let db = state.db;
    for key in &req.args[1..] {
        let entry = store.watch_snapshot(db, key, now);
        state.watch.push(entry);
    }
    Value::ok()
}

/// `UNWATCH` (TRANSACTIONS.md, PR-10b). Flushes the connection's watch set: deregister
/// every snapshot from the store ([`Watch::unwatch`]), then clear the connection-side
/// list. Always replies `+OK` (UNWATCH never errors; an empty watch set is a clean
/// no-op). Arity is exactly 1.
///
/// UNWATCH is a NORMAL command (NOT control-flow): inside MULTI it QUEUES like any other
/// and is REPLAYED at EXEC, where it runs as a no-op-ish (the dirty-CAS already cleared
/// the watches at EXEC entry, so `state.watch` is already empty). Outside MULTI it runs
/// here directly. Either way it just clears whatever watch set is present.
fn cmd_unwatch<S: Watch>(state: &mut ConnState, store: &mut S, req: &Request) -> Value {
    if req.args.len() != 1 {
        return Value::error(ErrorReply::wrong_arity("unwatch"));
    }
    store.unwatch(&state.watch);
    state.clear_watch();
    Value::ok()
}

/// Run `EXEC` (TRANSACTIONS.md "queue then apply", PR-10a). Decides the three Redis
/// outcomes and, on the apply path, REPLAYS each queued command against the SAME
/// already-borrowed store/wheel/env by calling [`dispatch_inner`] per command (the
/// re-entrancy the dispatch split exists for):
/// - NOT in a transaction -> `-ERR EXEC without MULTI`;
/// - in a transaction but DIRTIED (a queue-time error) -> `-EXECABORT ...`, drop the
///   queue, exit MULTI, apply nothing;
/// - otherwise -> run every queued command in order, collect each reply into an array
///   element, and return the array (an empty MULTI;EXEC is an empty array `*0`). There
///   is NO rollback: a per-command runtime error (WRONGTYPE, not-an-integer, `-OOM`
///   over budget) is an Error element and the batch continues.
///
/// Determinism (ADR-0003): the whole batch reuses the SINGLE `now` the serve loop read
/// once for this EXEC command, so a replay reaps/expires identically; no per-queued-
/// command clock read. Exiting the transaction clears the queue in all three branches.
#[allow(clippy::too_many_arguments)]
fn exec_transaction<E: Env, S: Store + Admit + ActiveExpiry + Keyspace + PolicySwap + Watch>(
    ctx: &ServerContext,
    state: &mut ConnState,
    env: &mut E,
    store: &mut S,
    wheel: &mut TimingWheel,
    now: UnixMillis,
    rollup: RollupFn<'_>,
    cmdstats: CmdStatsFn<'_>,
    keyspace: KeyspaceFn<'_>,
    mem: MemoryInfo,
    deltas: &mut CounterDeltas,
    req: &Request,
) -> Value {
    if req.args.len() != 1 {
        // A wrong-arity EXEC issued INSIDE a transaction DIRTIES it (Redis runs
        // commandCheckArity before the MULTI queue block, so an arity failure on the EXEC
        // verb itself calls flagTransaction -> CLIENT_DIRTY_EXEC). The txn stays OPEN +
        // dirty, so a later clean EXEC returns EXECABORT. When NOT in_multi this is just a
        // plain wrong-arity error (and a clean EXEC then returns EXEC-without-MULTI).
        if state.in_multi {
            state.dirty_exec = true;
        }
        return Value::error(ErrorReply::wrong_arity("exec"));
    }
    if !state.in_multi {
        return Value::error(ErrorReply::exec_without_multi());
    }
    if state.dirty_exec {
        // A queue-time error dirtied the batch: refuse the whole thing and apply
        // nothing (clear_txn drops the queue + exits MULTI). EXEC clears the WATCH set on
        // EVERY exit path (TRANSACTIONS.md, PR-10b), the EXECABORT path included:
        // deregister the watches from the store, then clear the connection-side list.
        store.unwatch(&state.watch);
        state.clear_watch();
        state.clear_txn();
        return Value::error(ErrorReply::exec_abort());
    }
    // WATCH dirty-CAS check (TRANSACTIONS.md per-key dirty-CAS, PR-10b), BEFORE running
    // the batch. If ANY watched key was modified between WATCH and now (its version moved,
    // or its present/absent status changed), the optimistic lock failed: EXEC ABORTS,
    // returning a NULL ARRAY (`Value::Array(None)`, which the encoder renders as RESP2
    // `*-1` / RESP3 `_`) and applying NOTHING. NOTE this is the null ARRAY, not the null
    // bulk (`Value::Null` -> RESP2 `$-1`): Redis's abort reply is `addReply(c,
    // shared.nullarray[...])` (src/multi.c execCommand). The watches are deregistered +
    // cleared (EXEC always clears watches) and the transaction exits. The check is
    // O(watched keys), each a version compare + a present/absent probe.
    if state.watch.iter().any(|e| store.watch_is_dirty(e, now)) {
        store.unwatch(&state.watch);
        state.clear_watch();
        state.clear_txn();
        return Value::Array(None);
    }
    // The CAS passed: the watches have served their purpose. Deregister + clear them
    // BEFORE running the batch (EXEC clears the watch set on the run path too), so the
    // batch's own writes do not re-trigger a watch and a queued UNWATCH at EXEC is a
    // clean no-op against an already-empty set.
    store.unwatch(&state.watch);
    state.clear_watch();
    // Take the queue OUT of `state` so we can pass `&mut state` to `dispatch_inner` per
    // command without aliasing `state.queued`. Exit the transaction NOW (before running)
    // so a queued RESET/MULTI/etc. sees a clean post-EXEC connection state, matching
    // Redis (EXEC ends the transaction, then runs the batch).
    let queued = std::mem::take(&mut state.queued);
    state.clear_txn();
    // F1 (live revocation, EXEC sub-case): bring the connection's cached ACL identity up to date
    // BEFORE replaying the batch, then RE-CHECK ACL per queued command below. This closes the race
    // where an EXTERNAL `ACL SETUSER` / `DELUSER` revokes a permission BETWEEN `MULTI` and `EXEC`:
    // the commands were queued under the OLD permissions, but Redis re-checks ACL at EXEC time
    // (per queued command), so a now-forbidden command must be denied at replay. `deauthed` is
    // `true` when the connection's user was DELUSER'd mid-transaction: every queued command is then
    // denied (the identity is gone). The HOT PATH (no mutation since MULTI opened) is the same one
    // relaxed load + compare as the router, so a transaction that ran with no concurrent ACL change
    // pays nothing here.
    let deauthed = !acl_resolve_if_stale(ctx, state);
    let mut replies = Vec::with_capacity(queued.len());
    for q in &queued {
        // Re-derive the uppercased token per queued command (cheap; the request was
        // validated at queue time). Reuse the SAME borrowed store/wheel/env + `now`:
        // no re-borrow of the thread-locals, no double-borrow. Counter deltas
        // ACCUMULATE across the batch (eviction / keyspace hits-misses use `+=`).
        let qcmd = ascii_upper(q.command());
        // F1: per-queued-command ACL re-check at EXEC. When the user was DELUSER'd mid-txn, deny
        // every command (NOPERM on the now-nonexistent identity); otherwise run the same
        // `acl_enforce` the router runs for a live command, so a permission revoked between MULTI
        // and EXEC denies the affected command (the rest of the batch still runs, Redis parity).
        let reply = if deauthed {
            let cmd_lc = String::from_utf8_lossy(&qcmd).to_ascii_lowercase();
            Value::error(ErrorReply::noperm_command(&state.acl_user_name, &cmd_lc))
        } else if let Some(deny) =
            acl_enforce(ctx.acl.is_acl_active(), state.acl_user.as_deref(), &qcmd, q)
        {
            Value::error(deny)
        } else {
            dispatch_inner(
                ctx, state, env, store, wheel, now, rollup, cmdstats, keyspace, mem, deltas, q,
                &qcmd,
            )
        };
        replies.push(reply);
    }
    Value::Array(Some(replies))
}

/// `PING` -> `+PONG`; `PING msg` -> bulk `msg`.
fn cmd_ping(req: &Request) -> Value {
    match req.args.len() {
        1 => Value::simple("PONG"),
        2 => Value::BulkString(Some(req.args[1].clone())),
        _ => Value::error(ErrorReply::wrong_arity("ping")),
    }
}

/// `ECHO msg` -> bulk `msg`.
fn cmd_echo(req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("echo"));
    }
    Value::BulkString(Some(req.args[1].clone()))
}

/// `LOLWUT [VERSION version]` -> a bulk string naming the server and its version. Redis
/// renders generative ASCII art selected by the optional VERSION argument; IronCache returns
/// a small, stable banner so clients and health probes that call LOLWUT get a non-error bulk
/// reply (the observable contract here is a bulk string, never an error, for any probe form).
/// Redis is lenient about the arguments: it draws art for any argument shape and errors ONLY
/// when the VERSION option is given a non-integer value (it parses argv[2] as a long). This
/// matches that leniency, so the only error path is `LOLWUT VERSION <non-integer>`. The art
/// bytes themselves are server-specific and never asserted by clients.
fn cmd_lolwut(req: &Request) -> Value {
    if req.args.len() >= 3
        && req.args[1].eq_ignore_ascii_case(b"VERSION")
        && crate::cmd_util::parse_i64(&req.args[2]).is_none()
    {
        return Value::error(ErrorReply::not_an_integer());
    }
    let banner = format!("IronCache ver. {}\n", ironcache_observe::SERVER_VERSION);
    Value::bulk(banner)
}

/// `SAVE` PERSISTENCE-DISABLED fallback (#58): reached only when the serve layer did NOT
/// intercept the command (no data_dir configured / a path that reaches dispatch directly), so
/// there is nothing to dump through the storage waist. Redis replies `+OK` to a successful SAVE;
/// with persistence off a SAVE is a no-op success (there is no on-disk target). The cross-shard
/// dump + manifest commit is the binary serve layer's job (it holds the concrete stores).
fn cmd_persist_save_fallback(req: &Request) -> Value {
    if req.args.len() != 1 {
        return Value::error(ErrorReply::wrong_arity("save"));
    }
    Value::ok()
}

/// `BGSAVE [SCHEDULE]` PERSISTENCE-DISABLED fallback (#58): the serve-layer non-intercept path.
/// Redis replies `+Background saving started`; with persistence off there is no background save
/// to start, but the reply is the Redis-faithful acknowledgement (a no-op success). Accepts the
/// bare form and an optional trailing arg (Redis BGSAVE SCHEDULE), which is ignored here.
fn cmd_persist_bgsave_fallback(req: &Request) -> Value {
    if req.args.is_empty() {
        return Value::error(ErrorReply::wrong_arity("bgsave"));
    }
    Value::SimpleString("Background saving started".to_owned())
}

/// `LASTSAVE` PERSISTENCE-DISABLED fallback (#58): the serve-layer non-intercept path. Redis
/// returns the unix time of the last successful save as an integer; with no committed save (or
/// persistence off) that is `0`. The real value (the committed manifest's `save_unix_secs`) is
/// reported by the serve layer when persistence is configured.
fn cmd_persist_lastsave_fallback(req: &Request) -> Value {
    if req.args.len() != 1 {
        return Value::error(ErrorReply::wrong_arity("lastsave"));
    }
    Value::Integer(0)
}

/// The resolved save decision a `SHUTDOWN [NOSAVE|SAVE]` carries (#139, SHUTDOWN.md). The serve
/// layer resolves the modifier ONCE via [`parse_shutdown`], then drives the stop sequence: SAVE
/// forces a save-on-exit even with no save policy, NOSAVE suppresses it even with one, and the bare
/// form (`Default`) saves IFF a save policy is configured [redis-shutdown-save-nosave-default].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownMode {
    /// Bare `SHUTDOWN`: save iff a save policy is configured, else exit without saving.
    Default,
    /// `SHUTDOWN SAVE`: force a save-on-exit even when no save policy is configured.
    Save,
    /// `SHUTDOWN NOSAVE`: suppress the save-on-exit even when a save policy is configured.
    NoSave,
}

/// Parse a `SHUTDOWN [NOSAVE|SAVE]` request into its resolved [`ShutdownMode`], or an `-ERR syntax
/// error` for a bad/extra modifier (#139). `SAVE` and `NOSAVE` are the ONLY two modifiers v1 honors
/// (SHUTDOWN.md "the only two modifiers"); the Redis ABORT / FORCE / NOW grammar is deferred (#150).
/// The token is matched case-insensitively (RESP command args are byte slices; Redis matches the
/// SHUTDOWN modifier with a case-insensitive compare). Shared by the serve-layer interception and
/// the [`cmd_shutdown_fallback`] dispatch arm so the two cannot disagree on the grammar.
///
/// # Errors
///
/// Returns an `-ERR syntax error` when there is more than one modifier or the single modifier is
/// neither `SAVE` nor `NOSAVE`.
pub fn parse_shutdown(req: &Request) -> Result<ShutdownMode, ErrorReply> {
    match req.args.get(1..) {
        // Bare `SHUTDOWN`: the default save-iff-policy-configured decision.
        Some([]) | None => Ok(ShutdownMode::Default),
        Some([modifier]) => {
            if modifier.eq_ignore_ascii_case(b"SAVE") {
                Ok(ShutdownMode::Save)
            } else if modifier.eq_ignore_ascii_case(b"NOSAVE") {
                Ok(ShutdownMode::NoSave)
            } else {
                Err(ErrorReply::syntax_error())
            }
        }
        // More than one modifier (e.g. `SHUTDOWN SAVE NOSAVE`) is a syntax error.
        Some(_) => Err(ErrorReply::syntax_error()),
    }
}

/// `SHUTDOWN [NOSAVE|SAVE]` NEVER-INTERCEPTED fallback (#139, SHUTDOWN.md): reached ONLY when the
/// serve layer did NOT intercept the command (a SHUTDOWN reaching dispatch directly, e.g. an EXEC
/// replay inside a transaction). The actual stop sequence -- drain, save-on-exit, process exit-0 --
/// lives in the binary's serve layer, which owns the runtime + the per-shard stores; this generic
/// dispatch path has neither, so it does NOT exit the process here. It still VALIDATES the modifier
/// grammar (so a bad modifier replies `-ERR syntax error` consistently) and otherwise returns `+OK`
/// without acting. A documented minor divergence from Redis (which would exit); the serve-layer
/// interception is the live path for every non-MULTI SHUTDOWN.
fn cmd_shutdown_fallback(req: &Request) -> Value {
    match parse_shutdown(req) {
        Ok(_) => Value::ok(),
        Err(e) => Value::error(e),
    }
}

/// The CURRENT number of in-sync replicas (PROD-9 WAIT): the runtime quorum count the WAIT
/// command reports + the serve layer blocks on. `ctx.in_sync_replicas` is `Some` only in
/// raft-governance mode; on a single node / standalone (the default), it is `None`, so the
/// count is `0` (no replica has acknowledged anything), exactly the value `WAIT N timeout`
/// returns once it times out with no replicas.
#[must_use]
pub fn in_sync_replica_count(ctx: &ServerContext) -> i64 {
    ctx.in_sync_replicas
        .as_ref()
        .map_or(0, |c| i64::try_from(c.count()).unwrap_or(i64::MAX))
}

/// `WAIT numreplicas timeout` NON-BLOCKING fallback (PROD-9): the EXEC-replay / direct-dispatch
/// path. The LIVE blocking WAIT lives in the serve layer (it can park on the timer seam until the
/// quorum is met); this arm validates the two integer args and replies the CURRENT in-sync replica
/// count immediately. Inside an EXEC, Redis's WAIT does not block, so reporting the current count
/// is the faithful non-blocking behavior.
fn cmd_wait_fallback(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("wait"));
    }
    // numreplicas + timeout must both be integers (Redis parses them as longs); a bad value is
    // the not-an-integer error. The values themselves do not change the non-blocking reply (it is
    // the current count), but they must validate.
    if parse_int_arg(&req.args[1]).is_none() || parse_int_arg(&req.args[2]).is_none() {
        return Value::error(ErrorReply::not_an_integer());
    }
    Value::Integer(in_sync_replica_count(ctx))
}

/// `HELLO [proto] [AUTH user pass] [SETNAME name]` (CONNECTION_LIFECYCLE.md).
///
/// With no version it reports the server map and keeps the current proto;
/// `HELLO 2`/`HELLO 3` switch; any other version is `-NOPROTO`. AUTH and SETNAME
/// options are applied in order before the reply is built.
fn cmd_hello(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
    let mut idx = 1;
    let mut new_proto = state.proto;

    // Optional protocol version (must be the first arg if present and numeric).
    if idx < req.args.len() {
        // The version token is only consumed if it parses as a number; otherwise
        // it must be an option keyword (AUTH/SETNAME).
        if let Some(ver) = parse_int_arg(&req.args[idx]) {
            new_proto = match ver {
                2 => ProtoVersion::Resp2,
                3 => ProtoVersion::Resp3,
                _ => return Value::error(ErrorReply::noproto()),
            };
            idx += 1;
        } else if !is_hello_option(&req.args[idx]) {
            // A non-numeric, non-option first token is an unsupported version.
            return Value::error(ErrorReply::noproto());
        }
    }

    // Parse the option tail: AUTH <user> <pass> and SETNAME <name>, in any order.
    let mut pending_auth: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut pending_name: Option<String> = None;
    while idx < req.args.len() {
        let opt = ascii_upper(&req.args[idx]);
        match opt.as_slice() {
            b"AUTH" => {
                if idx + 2 >= req.args.len() {
                    return Value::error(ErrorReply::wrong_arity("hello"));
                }
                pending_auth = Some((req.args[idx + 1].to_vec(), req.args[idx + 2].to_vec()));
                idx += 3;
            }
            b"SETNAME" => {
                if idx + 1 >= req.args.len() {
                    return Value::error(ErrorReply::wrong_arity("hello"));
                }
                pending_name = Some(String::from_utf8_lossy(&req.args[idx + 1]).into_owned());
                idx += 2;
            }
            _ => {
                return Value::error(ErrorReply::hello_syntax_error(&String::from_utf8_lossy(
                    &req.args[idx],
                )));
            }
        }
    }

    // Apply AUTH if provided; a failed AUTH aborts HELLO without switching proto.
    if let Some((user, pass)) = pending_auth {
        match check_auth(ctx, &user, &pass) {
            AuthResult::Ok(u) => apply_auth_success(ctx, state, u),
            AuthResult::NoPasswordSet => {
                return Value::error(ErrorReply::auth_no_password_set());
            }
            AuthResult::WrongPass => return Value::error(ErrorReply::wrongpass()),
        }
    }

    // If auth is required and still not satisfied, HELLO is refused with NOAUTH.
    if ctx.requires_auth() && !state.authenticated {
        return Value::error(ErrorReply::noauth());
    }

    // Commit proto and name only after all checks pass.
    state.proto = new_proto;
    if let Some(name) = pending_name {
        state.name = name;
    }

    hello_map(ctx, state)
}

/// Build the HELLO reply map (server, version, proto, id, mode, role, modules).
fn hello_map(ctx: &ServerContext, state: &ConnState) -> Value {
    let pairs = vec![
        (Value::bulk_str("server"), Value::bulk_str("ironcache")),
        (
            Value::bulk_str("version"),
            Value::bulk_str(ironcache_observe::SERVER_VERSION),
        ),
        (
            Value::bulk_str("proto"),
            Value::Integer(state.proto.as_i64()),
        ),
        (Value::bulk_str("id"), Value::Integer(state.id as i64)),
        (Value::bulk_str("mode"), Value::bulk_str("standalone")),
        (Value::bulk_str("role"), Value::bulk_str("master")),
        (Value::bulk_str("modules"), Value::Array(Some(vec![]))),
    ];
    let _ = ctx;
    Value::Map(pairs)
}

fn is_hello_option(arg: &[u8]) -> bool {
    let u = ascii_upper(arg);
    matches!(u.as_slice(), b"AUTH" | b"SETNAME")
}

/// `AUTH [user] pass` (PROTOCOL.md Tier-0, ERRORS.md auth strings).
///
/// `AUTH <pass>` authenticates as `default` (the legacy single-password path); `AUTH <user>
/// <pass>` authenticates as that ACL user (#106). On success the resolved `Arc<User>` is
/// CACHED on the connection so the per-command authorization check reads it lock-free.
fn cmd_auth(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
    let (user, pass): (&[u8], &[u8]) = match req.args.len() {
        2 => (b"default", &req.args[1]),
        3 => (&req.args[1], &req.args[2]),
        _ => return Value::error(ErrorReply::wrong_arity("auth")),
    };
    match check_auth(ctx, user, pass) {
        AuthResult::Ok(u) => {
            apply_auth_success(ctx, state, u);
            Value::ok()
        }
        AuthResult::NoPasswordSet => Value::error(ErrorReply::auth_no_password_set()),
        AuthResult::WrongPass => Value::error(ErrorReply::wrongpass()),
    }
}

/// Commit a successful authentication onto the connection: mark authenticated and CACHE the
/// resolved ACL user (#106). When the resolved user is the all-permissive default, we cache
/// `None` (the implicit-default fast path) so the per-command enforcement gate skips it and
/// the no-ACL deployment stays byte-identical; a NARROWED user is cached as `Some(Arc<User>)`.
///
/// F1 (live revocation): also record the user's NAME and the registry GENERATION the user was
/// resolved against. The per-command path re-resolves by `acl_user_name` when the generation
/// moves, so a mid-session `ACL SETUSER`/`DELUSER` reaches this connection. The generation is
/// read from the SAME registry the user came from; a concurrent mutation between resolve and
/// this read only makes the cached generation conservatively stale (the next command re-checks).
fn apply_auth_success(ctx: &ServerContext, state: &mut ConnState, user: Arc<crate::acl::User>) {
    state.authenticated = true;
    // Reuse the existing allocation rather than reassign (clippy::assigning_clones).
    state.acl_user_name.clone_from(&user.name);
    state.acl_user_gen = ctx.acl.generation();
    state.acl_user = if user.is_all_permissive() {
        None
    } else {
        Some(user)
    };
}

/// The outcome of an authentication attempt: the resolved ACL user on success.
enum AuthResult {
    /// Authenticated; carries the resolved `Arc<User>` to cache on the connection.
    Ok(Arc<crate::acl::User>),
    /// `AUTH` was issued but no password / ACL user is configured for the target.
    NoPasswordSet,
    /// Wrong username/password pair, or the user is disabled.
    WrongPass,
}

/// Check credentials against the ACL registry (#106). `AUTH <pass>` targets `default`; `AUTH
/// <user> <pass>` targets that user. The registry resolves the user, verifies the password
/// in CONSTANT TIME against the stored SHA-256 digests (or accepts any for a `nopass` user),
/// and gates on the user being enabled. The plaintext guess lives only as `pass` during
/// hashing and is never stored or logged.
///
/// Backward compatibility: with NO requirepass and NO ACL config, the registry holds the
/// `default` `nopass` user, so a bare `AUTH <anything>` for `default` would succeed -- but
/// Redis instead replies `ERR Client sent AUTH, but no password is set` in that posture. We
/// preserve that by reporting [`AuthResult::NoPasswordSet`] when targeting `default` and no
/// requirepass is configured AND no narrower ACL is active. Once an ACL is active (a real
/// `default` password, or any other user), normal resolution applies.
fn check_auth(ctx: &ServerContext, user: &[u8], pass: &[u8]) -> AuthResult {
    let name = if user.is_empty() {
        crate::acl::DEFAULT_USER.to_owned()
    } else {
        String::from_utf8_lossy(user).into_owned()
    };

    let targets_default = name.eq_ignore_ascii_case(crate::acl::DEFAULT_USER);

    // Redis parity: `AUTH <pass>` against the bare default with no password set is an ERR,
    // not a silent success. This is true exactly when targeting `default`, no requirepass is
    // configured, and the ACL registry is otherwise inactive (only the all-permissive
    // default exists). Any active ACL (a default password, or another user) skips this.
    if targets_default && ctx.runtime.requirepass().is_none() && !ctx.acl.is_acl_active() {
        return AuthResult::NoPasswordSet;
    }

    // LEGACY requirepass compatibility for the `default` user (see [`check_default_requirepass`]).
    // `Some(result)` = the requirepass path DECIDED the auth (matched -> Ok, or a nopass default
    // mismatch -> WrongPass); `None` = fall through to the normal ACL verify below.
    if targets_default {
        if let Some(result) = check_default_requirepass(ctx, pass) {
            return result;
        }
    }

    match ctx.acl.authenticate(&name, pass) {
        Some(u) => AuthResult::Ok(u),
        None => AuthResult::WrongPass,
    }
}

/// The LEGACY `requirepass` path for the `default` user (#106 back-compat). `CONFIG SET
/// requirepass` (and boot requirepass) live in the runtime overlay, NOT the ACL registry, so for
/// `default` we ALSO accept the CURRENT runtime requirepass digest -- a `CONFIG SET requirepass`
/// takes effect LIVE for `AUTH <pass>` alongside any ACL `>pass` digests the registry holds. The
/// compare is constant-time over the fixed-width hex digests.
///
/// SECURITY: when a runtime requirepass IS configured it is AUTHORITATIVE. A `CONFIG SET
/// requirepass` does not touch the registry, so the boot-default is still `nopass` (it would
/// accept ANY password). We must NOT let that implicit `nopass` bypass the live requirepass: so a
/// mismatch against the requirepass digest, when the default carries NO explicit ACL password, is
/// `-WRONGPASS` here (not a fall-through to the nopass ACL verify).
///
/// Returns `Some(AuthResult)` when this path DECIDES the auth (digest match -> `Ok` with the live
/// default user; or a nopass-default mismatch -> `WrongPass`); `None` when there is no requirepass,
/// or the default carries explicit ACL passwords (let the caller's ACL verify run those).
fn check_default_requirepass(ctx: &ServerContext, pass: &[u8]) -> Option<AuthResult> {
    let configured_hash = ctx.runtime.requirepass()?;
    let guess_hash = ironcache_config::sha256_hex(pass);
    if constant_time_eq(guess_hash.as_bytes(), configured_hash.as_bytes()) {
        // Resolve the live `default` user to cache (its perms apply); fall back to the all-
        // permissive default if somehow absent (it cannot be deleted).
        let u = ctx
            .acl
            .get_user(crate::acl::DEFAULT_USER)
            .unwrap_or_else(|| Arc::new(crate::acl::User::default_nopass()));
        return Some(AuthResult::Ok(u));
    }
    // Mismatch: only an EXPLICIT ACL password on the default may still authenticate; the implicit
    // boot `nopass` must NOT (it would defeat the requirepass).
    let default_is_nopass = ctx
        .acl
        .get_user(crate::acl::DEFAULT_USER)
        .is_none_or(|u| u.nopass);
    if default_is_nopass {
        Some(AuthResult::WrongPass)
    } else {
        None
    }
}

/// Compare two byte slices in CONSTANT TIME with respect to their CONTENTS: the running
/// time depends only on the slice LENGTHS, never on WHERE the first differing byte is,
/// so an attacker cannot learn a correct password byte-by-byte from response timing
/// (the timing-leak finding). No new dependency: a hand-rolled fold.
///
/// Mechanism: if the lengths differ, return false immediately (length is not secret in
/// this model). Otherwise fold every byte pair into an XOR accumulator and check it is
/// zero at the END, examining ALL bytes regardless of an early mismatch. The accumulator
/// is read through [`std::hint::black_box`] before the final compare so the optimizer
/// cannot prove the loop short-circuitable and re-introduce a data-dependent early exit.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }
    // Defeat any optimization that would let the compiler reintroduce an early-out:
    // force the accumulator to be materialized before the zero test.
    std::hint::black_box(acc) == 0
}

/// `SELECT index` (PROTOCOL.md Tier-0). Validates the range `[0, databases)`.
fn cmd_select(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("select"));
    }
    let Some(idx) = parse_int_arg(&req.args[1]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    if idx < 0 || idx >= i64::from(ctx.databases) {
        return Value::error(ErrorReply::select_out_of_range());
    }
    state.db = idx as u32;
    // Mirror the selected DB into the registry record so CLIENT LIST / CLIENT INFO for a PEER
    // connection reports the live db (PROD-7). A no-op for a direct-dispatch caller not in the
    // registry (tests).
    if let Some(h) = ctx.clients.by_id(state.id) {
        h.db.store(u64::from(state.db), core::sync::atomic::Ordering::Relaxed);
    }
    Value::ok()
}

/// `CLIENT <subcommand>` (handshake-critical subset, PROTOCOL.md).
fn cmd_client<E: Clock>(
    ctx: &ServerContext,
    state: &mut ConnState,
    env: &E,
    req: &Request,
) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("client"));
    }
    // ===================== CO-EDIT CONTRACT with the PER-SUBCOMMAND ACL =====================
    // These match arms are the AUTHORITATIVE list of CLIENT subcommands and the privileged-vs-plain
    // split. Per-subcommand ACL (`+client|info`) mirrors them in `command_spec::CLIENT_SUBCOMMANDS`
    // (the @admin/@dangerous flags) and pins them in
    // `command_spec::tests::client_subcommand_table_matches_dispatch_arms`. If you ADD, REMOVE, or
    // RECLASSIFY an arm, you MUST update BOTH in the same change. SECURITY: LIST/KILL/PAUSE/UNPAUSE/
    // NO-EVICT are @admin+@dangerous (denied by -@dangerous); ID/GETNAME/SETNAME/SETINFO/INFO/
    // NO-TOUCH are @slow @connection (NOT dangerous). A privileged arm mistagged as a plain read in
    // CLIENT_SUBCOMMANDS would let a -@dangerous user run it -- an escalation the pin test cannot
    // catch (it cannot read these arms at runtime).
    // =======================================================================================
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        b"ID" => Value::Integer(state.id as i64),
        b"GETNAME" => Value::bulk_str(&state.name),
        b"SETNAME" => {
            if req.args.len() != 3 {
                return Value::error(ErrorReply::wrong_arity("client|setname"));
            }
            // The name may not contain spaces or newlines (Redis rule).
            if req.args[2]
                .iter()
                .any(|&b| b == b' ' || b == b'\n' || b == b'\r')
            {
                return Value::error(ErrorReply::client_name_invalid_chars());
            }
            state.name = String::from_utf8_lossy(&req.args[2]).into_owned();
            // Mirror the new name into the registry record so CLIENT LIST / CLIENT INFO for THIS
            // connection (and a peer's CLIENT KILL filtering) sees the live name. The registry is
            // node-level; if this connection is not registered (a direct dispatch caller / a test)
            // this is a harmless no-op.
            if let Some(h) = ctx.clients.by_id(state.id) {
                if let Ok(mut g) = h.name.lock() {
                    state.name.clone_into(&mut g);
                }
            }
            Value::ok()
        }
        b"SETINFO" => {
            // CLIENT SETINFO lib-name/lib-ver: accept and ack (clients send it on
            // connect). Arity is `CLIENT SETINFO <attr> <value>`.
            if req.args.len() != 4 {
                return Value::error(ErrorReply::wrong_arity("client|setinfo"));
            }
            Value::ok()
        }
        b"INFO" => Value::bulk_str(&client_info_line(state)),
        // CLIENT LIST [ID id ...] (PROD-7): one text line per live connection from the node-level
        // registry. The optional `ID <id> [id...]` filter selects specific connections.
        b"LIST" => cmd_client_list(ctx, state, req),
        // CLIENT KILL <ID id|ADDR addr|...> (PROD-7): flag a matching connection for close via the
        // registry; the target's serve loop observes the flag and closes after its current batch.
        b"KILL" => cmd_client_kill(ctx, state, req),
        // CLIENT PAUSE ms [WRITE|ALL] (PROD-7): pause command processing node-wide for `ms` ms; the
        // serve loop honors the pause window after each batch. UNPAUSE clears it.
        b"PAUSE" => cmd_client_pause(ctx, env, req),
        b"UNPAUSE" => {
            if req.args.len() != 2 {
                return Value::error(ErrorReply::wrong_arity("client|unpause"));
            }
            ctx.clients.unpause();
            Value::ok()
        }
        // CLIENT NO-EVICT on|off (PROD-7): accept + ack. IronCache does not evict client connection
        // buffers to free memory (the output-buffer cap closes an over-budget connection instead),
        // so the flag is a no-op acked for client compatibility. Arity `CLIENT NO-EVICT <on|off>`.
        b"NO-EVICT" => {
            if req.args.len() != 3 {
                return Value::error(ErrorReply::wrong_arity("client|no-evict"));
            }
            match ascii_upper(&req.args[2]).as_slice() {
                b"ON" | b"OFF" => Value::ok(),
                _ => Value::error(ErrorReply::syntax_error()),
            }
        }
        b"NO-TOUCH" => Value::ok(),
        // CLIENT TRACKING ON|OFF [NOLOOP] (#409): toggle server-assisted client-side caching for
        // this connection. Sets the per-connection flags; the serve layer's read/write hooks do the
        // registration + invalidation, and the OFF/RESET/disconnect transition purges the table.
        b"TRACKING" => cmd_client_tracking(ctx, state, req),
        // CLIENT TRACKINGINFO (#409): the current tracking state (flags / redirect / prefixes).
        b"TRACKINGINFO" => cmd_client_trackinginfo(state, req),
        // CLIENT CACHING YES|NO (#409 stage 3): the one-shot OPTIN/OPTOUT caching gate.
        b"CACHING" => cmd_client_caching(state, req),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "CLIENT",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// The parsed option tail of `CLIENT TRACKING` (everything after `ON`/`OFF`), produced by
/// [`parse_tracking_options`]. Kept separate so the command handler stays short.
// lint-allow: the four flags are independent Redis TRACKING toggles (NOLOOP/BCAST/OPTIN/OPTOUT),
// each a distinct protocol option mirrored 1:1 onto `ConnState`; a state machine would not model
// them more faithfully (OPTIN/OPTOUT exclusivity is validated separately, not encoded in the type).
#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
struct TrackingOpts {
    noloop: bool,
    bcast: bool,
    optin: bool,
    optout: bool,
    /// The `REDIRECT` target id (stage 4); `0` means no redirection.
    redirect: u64,
    /// The `BCAST` `PREFIX` list (stage 2).
    prefixes: Vec<bytes::Bytes>,
}

/// Parse the option tail of `CLIENT TRACKING` (`req.args[3..]`): NOLOOP/BCAST/OPTIN/OPTOUT/PREFIX/
/// REDIRECT (#409). Returns the parsed [`TrackingOpts`] or, on a malformed option, the error `Value`
/// to return to the client.
fn parse_tracking_options(req: &Request) -> Result<TrackingOpts, Value> {
    let mut o = TrackingOpts::default();
    let mut i = 3;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"NOLOOP" => {
                o.noloop = true;
                i += 1;
            }
            b"BCAST" => {
                o.bcast = true;
                i += 1;
            }
            b"OPTIN" => {
                o.optin = true;
                i += 1;
            }
            b"OPTOUT" => {
                o.optout = true;
                i += 1;
            }
            b"PREFIX" => {
                if i + 1 >= req.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                o.prefixes.push(req.args[i + 1].clone());
                i += 2;
            }
            // Stage 4: REDIRECT <id> routes invalidations to another connection (id 0 = no redirect).
            b"REDIRECT" => {
                if i + 1 >= req.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                o.redirect = match core::str::from_utf8(&req.args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(id) => id,
                    None => return Err(Value::error(ErrorReply::err("Invalid client ID"))),
                };
                i += 2;
            }
            _ => return Err(Value::error(ErrorReply::syntax_error())),
        }
    }
    Ok(o)
}

/// `CLIENT TRACKING <ON|OFF> [REDIRECT id] [PREFIX p [PREFIX p ...]] [BCAST] [OPTIN] [OPTOUT]
/// [NOLOOP]` (#409): enable/disable server-assisted client-side caching for this connection. `ON`
/// requires RESP3 mode OR a `REDIRECT` target (stage 4): a RESP2 client has no push type, so its
/// invalidations are routed to a SECOND connection (the redirect target, which `SUBSCRIBE`d
/// `__redis__:invalidate`) as a Pub/Sub `message`. `REDIRECT 0` means no redirection. The redirect
/// target must be a live connection (looked up in the client registry), matching Redis.
fn cmd_client_tracking(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("client|tracking"));
    }
    let on = match ascii_upper(&req.args[2]).as_slice() {
        b"ON" => true,
        b"OFF" => false,
        _ => return Value::error(ErrorReply::syntax_error()),
    };
    let TrackingOpts {
        noloop,
        bcast,
        optin,
        optout,
        redirect,
        prefixes,
    } = match parse_tracking_options(req) {
        Ok(o) => o,
        Err(e) => return e,
    };
    // OPTIN and OPTOUT are mutually exclusive, and neither combines with BCAST (Redis).
    if optin && optout {
        return Value::error(ErrorReply::err(
            "You can't specify both OPTIN mode and OPTOUT mode",
        ));
    }
    if (optin || optout) && bcast {
        return Value::error(ErrorReply::err(
            "OPTIN and OPTOUT are not compatible with BCAST",
        ));
    }
    // PREFIX requires BCAST (Redis), and the prefixes must not overlap (one being a prefix of
    // another would double-deliver an invalidation).
    if !prefixes.is_empty() && !bcast {
        return Value::error(ErrorReply::err(
            "PREFIX option requires BCAST mode to be enabled",
        ));
    }
    for a in 0..prefixes.len() {
        for b in 0..prefixes.len() {
            if a != b && prefixes[a].starts_with(prefixes[b].as_ref()) {
                return Value::error(ErrorReply::err(format!(
                    "Prefix '{}' overlaps with an existing prefix '{}'. Prefixes for a single \
                     client must not overlap.",
                    String::from_utf8_lossy(&prefixes[a]),
                    String::from_utf8_lossy(&prefixes[b])
                )));
            }
        }
    }
    // A non-zero REDIRECT target must be a LIVE connection (Redis looks it up in the client table).
    // Checked only when enabling: `OFF` ignores any REDIRECT, and `REDIRECT 0` means no redirect.
    if on && redirect != 0 && ctx.clients.by_id(redirect).is_none() {
        return Value::error(ErrorReply::err(
            "The client ID you want redirect to does not exist",
        ));
    }
    if on {
        // RESP3 is required UNLESS a redirect target is given: a RESP2 client cannot receive bare
        // `invalidate` pushes, but it CAN route them to a redirect target's SUBSCRIBE.
        if state.proto != ProtoVersion::Resp3 && redirect == 0 {
            return Value::error(ErrorReply::err(
                "Client tracking can be enabled only in RESP3 mode or when a redirection client is \
                 specified via the 'REDIRECT' option",
            ));
        }
        state.tracking_on = true;
        state.tracking_noloop = noloop;
        state.tracking_bcast = bcast;
        state.tracking_prefixes = prefixes;
        state.tracking_optin = optin;
        state.tracking_optout = optout;
        state.tracking_redirect = redirect;
        // A fresh CLIENT TRACKING ON drops any dangling one-shot CACHING flag.
        state.caching_next = None;
    } else {
        state.tracking_on = false;
        state.tracking_noloop = false;
        state.tracking_bcast = false;
        state.tracking_prefixes.clear();
        state.tracking_optin = false;
        state.tracking_optout = false;
        state.tracking_redirect = 0;
        state.caching_next = None;
    }
    Value::ok()
}

/// `CLIENT CACHING YES|NO` (#409 stage 3): set the ONE-SHOT caching flag that the NEXT command's
/// track decision consumes. Valid ONLY when the connection is tracking in OPTIN or OPTOUT mode
/// (Redis errors otherwise). In OPTIN, `YES` opts the next read's keys IN; in OPTOUT, `NO` opts
/// them OUT.
fn cmd_client_caching(state: &mut ConnState, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("client|caching"));
    }
    let yes = match ascii_upper(&req.args[2]).as_slice() {
        b"YES" => true,
        b"NO" => false,
        _ => return Value::error(ErrorReply::syntax_error()),
    };
    if !(state.tracking_optin || state.tracking_optout) {
        return Value::error(ErrorReply::err(
            "CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or \
             OPTOUT mode enabled",
        ));
    }
    state.caching_next = Some(yes);
    Value::ok()
}

/// `CLIENT TRACKINGINFO` (#409): a map of this connection's tracking state. `flags` is `[off]` or
/// `[on]` (plus `bcast`/`optin`/`optout`/`noloop`/`caching-yes`/`caching-no` as set); `redirect` is
/// `-1` when tracking is off, `0` when on with no redirect, or the REDIRECT target id (stage 4);
/// `prefixes` lists the BCAST prefixes. Rendered as a [`Value::Map`] (RESP3 `%`, degrading to a flat
/// array under RESP2).
fn cmd_client_trackinginfo(state: &ConnState, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("client|trackinginfo"));
    }
    let flags = if state.tracking_on {
        let mut f = vec![Value::bulk_str("on")];
        if state.tracking_bcast {
            f.push(Value::bulk_str("bcast"));
        }
        if state.tracking_optin {
            f.push(Value::bulk_str("optin"));
        }
        if state.tracking_optout {
            f.push(Value::bulk_str("optout"));
        }
        if state.tracking_noloop {
            f.push(Value::bulk_str("noloop"));
        }
        // The pending one-shot CLIENT CACHING decision, if set (Redis exposes caching-yes/-no).
        match state.caching_next {
            Some(true) => f.push(Value::bulk_str("caching-yes")),
            Some(false) => f.push(Value::bulk_str("caching-no")),
            None => {}
        }
        f
    } else {
        vec![Value::bulk_str("off")]
    };
    // `redirect`: -1 when tracking is off, the REDIRECT target id when on with a redirect (stage 4),
    // 0 when on with no redirect.
    let redirect = if state.tracking_on {
        i64::try_from(state.tracking_redirect).unwrap_or(i64::MAX)
    } else {
        -1
    };
    // In BCAST mode the prefixes are reported (an empty list means the EMPTY prefix = all keys).
    let prefixes: Vec<Value> = state
        .tracking_prefixes
        .iter()
        .map(|p| Value::bulk(p.clone()))
        .collect();
    Value::Map(vec![
        (Value::bulk_str("flags"), Value::Array(Some(flags))),
        (Value::bulk_str("redirect"), Value::Integer(redirect)),
        (Value::bulk_str("prefixes"), Value::Array(Some(prefixes))),
    ])
}

/// `CLIENT LIST [ID id [id ...]]` (PROD-7): a bulk string of one `id=.. addr=.. ...` line per live
/// connection (Redis CLIENT LIST shape, a subset of fields), newline-separated. The optional `ID`
/// filter restricts the output to the named connection ids. The line for THIS connection reflects
/// its live name/db from `state` (the registry copy is updated on SETNAME/SELECT but `state` is the
/// freshest); other connections render from their registry records.
fn cmd_client_list(ctx: &ServerContext, state: &ConnState, req: &Request) -> Value {
    // Parse an optional `ID <id> [id...]` filter.
    let mut filter: Option<Vec<u64>> = None;
    if req.args.len() >= 3 {
        if !ascii_upper(&req.args[2]).eq_ignore_ascii_case(b"ID") {
            return Value::error(ErrorReply::syntax_error());
        }
        if req.args.len() == 3 {
            return Value::error(ErrorReply::syntax_error());
        }
        let mut ids = Vec::new();
        for a in &req.args[3..] {
            match core::str::from_utf8(a)
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
            {
                Some(id) => ids.push(id),
                None => return Value::error(ErrorReply::err("Invalid client ID")),
            }
        }
        filter = Some(ids);
    }
    let mut body = String::new();
    for h in ctx.clients.snapshot() {
        if let Some(ids) = &filter {
            if !ids.contains(&h.id) {
                continue;
            }
        }
        // For THIS connection, prefer the live `state` name/db/resp (freshest); others render from
        // the registry record.
        if h.id == state.id {
            body.push_str(&client_info_line(state));
        } else {
            body.push_str(&registry_info_line(&h));
        }
        body.push('\n');
    }
    Value::bulk(body.into_bytes())
}

/// `CLIENT KILL ...` (PROD-7). Supports the OLD form `CLIENT KILL addr:port` (returns +OK or an
/// error if no match) and the NEW filter form `CLIENT KILL <ID id|ADDR addr|LADDR addr> [...]`
/// (returns the integer count of connections killed). A connection cannot reach KILL unless it is
/// authorized (the ACL/admin gate ran upstream); the actual close happens in the target's serve
/// loop, which observes the registry kill flag after its current batch.
fn cmd_client_kill(ctx: &ServerContext, state: &ConnState, req: &Request) -> Value {
    // OLD form: exactly one argument that is an addr (`CLIENT KILL 1.2.3.4:5`).
    if req.args.len() == 3 {
        let addr = String::from_utf8_lossy(&req.args[2]).into_owned();
        return if ctx.clients.kill_addr(&addr) {
            Value::ok()
        } else {
            Value::error(ErrorReply::err("No such client"))
        };
    }
    // NEW filter form: `CLIENT KILL <filter value> [<filter value> ...]`, an EVEN tail of
    // filter/value pairs. We support ID, ADDR, and LADDR (the common operator filters); a SKIPME
    // option is accepted for compatibility (default yes -> never kill the caller).
    let rest = &req.args[2..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return Value::error(ErrorReply::syntax_error());
    }
    let mut want_id: Option<u64> = None;
    let mut want_peer_addr: Option<String> = None;
    let mut want_local_addr: Option<String> = None;
    let mut skipme = true;
    for pair in rest.chunks_exact(2) {
        let opt = ascii_upper(&pair[0]);
        let val = String::from_utf8_lossy(&pair[1]).into_owned();
        match opt.as_slice() {
            b"ID" => match val.parse::<u64>() {
                Ok(id) => want_id = Some(id),
                Err(_) => {
                    return Value::error(ErrorReply::err("client-id should be greater than 0"));
                }
            },
            b"ADDR" => want_peer_addr = Some(val),
            b"LADDR" => want_local_addr = Some(val),
            b"SKIPME" => match val.to_ascii_lowercase().as_str() {
                "yes" => skipme = true,
                "no" => skipme = false,
                _ => return Value::error(ErrorReply::syntax_error()),
            },
            // TYPE / USER / MAXAGE: accepted-but-ignored filters for compatibility (a single-tier
            // connection model has no client TYPE distinction). They never match-narrow here.
            b"TYPE" | b"USER" | b"MAXAGE" => {}
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    }
    let mut killed = 0i64;
    for h in ctx.clients.snapshot() {
        if skipme && h.id == state.id {
            continue;
        }
        if let Some(id) = want_id {
            if h.id != id {
                continue;
            }
        }
        if let Some(addr) = &want_peer_addr {
            if &h.addr != addr {
                continue;
            }
        }
        if let Some(laddr) = &want_local_addr {
            if &h.laddr != laddr {
                continue;
            }
        }
        h.kill();
        killed += 1;
    }
    Value::Integer(killed)
}

/// `CLIENT PAUSE <ms> [WRITE|ALL]` (PROD-7): pause command processing node-wide for `ms`
/// milliseconds. `ALL` (the default) pauses all commands; `WRITE` pauses only writes. The serve
/// loop reads the pause window (a monotonic deadline) after each batch and stalls while it is
/// active. The deadline basis is the Env monotonic clock (ADR-0003), passed in by the caller.
fn cmd_client_pause<E: Clock>(ctx: &ServerContext, env: &E, req: &Request) -> Value {
    if req.args.len() != 3 && req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("client|pause"));
    }
    let Some(ms) = core::str::from_utf8(&req.args[2])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return Value::error(ErrorReply::err("timeout is not an integer or out of range"));
    };
    let writes_only = if req.args.len() == 4 {
        match ascii_upper(&req.args[3]).as_slice() {
            b"WRITE" => true,
            b"ALL" => false,
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    } else {
        false
    };
    // The monotonic-millis basis the serve loop also reads: the Env clock's `now()` as millis.
    let now_mono_ms = env.now().as_millis();
    ctx.clients.pause(now_mono_ms, ms, writes_only);
    Value::ok()
}

/// A single-line CLIENT INFO description for THIS connection (subset of Redis fields).
fn client_info_line(state: &ConnState) -> String {
    format!(
        "id={} addr={} laddr={} name={} db={} resp={}",
        state.id,
        state.addr,
        state.laddr,
        state.name,
        state.db,
        state.proto.as_i64()
    )
}

/// A CLIENT LIST line for a PEER connection rendered from its registry record (this connection's
/// `ConnState` is not reachable cross-connection, so the registry holds the load-bearing fields:
/// id / addr / laddr / name / db).
fn registry_info_line(h: &ironcache_observe::ClientHandle) -> String {
    format!(
        "id={} addr={} laddr={} name={} db={}",
        h.id,
        h.addr,
        h.laddr,
        h.name(),
        h.db.load(core::sync::atomic::Ordering::Relaxed),
    )
}

/// `COMMAND [COUNT|INFO|DOCS|LIST|GETKEYS|...]` command introspection (PROTOCOL.md, #158).
///
/// CLUSTER-AWARE CLIENTS need a REAL command table here: a `RedisCluster` (redis-py), go-redis, or
/// ioredis calls bare `COMMAND` at connect to learn each command's key positions so it can compute
/// the slot of a command's keys and route to the owning node. The prior PR-1 stub returned an EMPTY
/// table, which made redis-py raise `"<CMD> command doesn't exist in Redis commands"` and refuse to
/// route ANY keyed op against a cluster. We now project the real table from the single-source
/// [`command_spec`] registry. The SINGLE-NODE path is functionally unaffected (a non-cluster client
/// does not consult the command table to route), so this is purely additive correctness.
fn cmd_command(req: &Request) -> Value {
    if req.args.len() == 1 {
        // Bare COMMAND: the full command table, one flat entry per client-visible command.
        let entries = command_spec::CLIENT_COMMAND_NAMES
            .iter()
            .filter_map(|name| command_spec::spec_of(name).map(command_table_entry))
            .collect();
        return Value::Array(Some(entries));
    }
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        // COUNT: the number of client-visible commands (the real count, not 0).
        b"COUNT" => Value::Integer(command_spec::CLIENT_COMMAND_NAMES.len() as i64),
        // LIST: a flat array of every command name (lowercased, as Redis renders names).
        b"LIST" => Value::Array(Some(
            command_spec::CLIENT_COMMAND_NAMES
                .iter()
                .map(|n| Value::bulk(n.to_ascii_lowercase()))
                .collect(),
        )),
        // INFO [name ...]: one table entry per requested command (NULL array element for an
        // unknown name, matching Redis). Bare `COMMAND INFO` (no names) returns the full table.
        b"INFO" => {
            if req.args.len() == 2 {
                let entries = command_spec::CLIENT_COMMAND_NAMES
                    .iter()
                    .filter_map(|name| command_spec::spec_of(name).map(command_table_entry))
                    .collect();
                return Value::Array(Some(entries));
            }
            let entries = req.args[2..]
                .iter()
                .map(|name| {
                    let upper = name.to_ascii_uppercase();
                    command_spec::spec_of(&upper).map_or(Value::Array(None), command_table_entry)
                })
                .collect();
            Value::Array(Some(entries))
        }
        // GETKEYS <command> [args ...]: extract the routable keys of the supplied command line via
        // the registry's key-spec (the SAME extraction the router uses). This is what a cluster
        // client falls back to for a `movablekeys` command. Errors match Redis's classes.
        b"GETKEYS" => cmd_command_getkeys(req),
        // DOCS: an empty map is well-formed and accepted by clients at startup. (A full DOCS body
        // -- summaries/since/group -- is not needed for routing; clients tolerate an empty map.)
        b"DOCS" => Value::Map(vec![]),
        // Any other subcommand: an empty, well-formed array (COMMAND is probed at client startup
        // with assorted subcommands; an empty array is more tolerant than an error).
        _ => Value::Array(Some(vec![])),
    }
}

/// One `COMMAND` table entry for a [`command_spec::CommandSpec`], as the Redis flat array
/// `[name, arity, [flags], first_key, last_key, step, [acl-cats], [tips], [key-specs], [subcmds]]`
/// (#158). A cluster client reads `name`/`arity`/`flags`/`first_key`/`last_key`/`step` to route;
/// the trailing three (acl-cats/tips/key-specs/subcommands) are emitted EMPTY (well-formed and
/// tolerated -- redis-py reads them only when present).
///
/// `arity` follows the Redis encoding: a POSITIVE n for `Exact(n)`, a NEGATIVE -n for `Min(n)`.
/// `flags` carry the routing-relevant set: `write`/`readonly`, `denyoom`, and `movablekeys` for a
/// command whose keys are option/numkeys-dependent (so the client falls back to `COMMAND GETKEYS`).
fn command_table_entry(spec: &command_spec::CommandSpec) -> Value {
    let arity = match spec.arity {
        // Redis arity encoding: positive n = exactly n total args; negative -n = at least n.
        command_spec::Arity::Exact(n) => i64::try_from(n).unwrap_or(i64::MAX),
        command_spec::Arity::Min(n) => -i64::try_from(n).unwrap_or(i64::MAX),
    };
    let (first_key, last_key, step, movable) = command_spec::command_key_positions(spec);
    let mut flags: Vec<Value> = Vec::new();
    flags.push(Value::simple(if spec.is_write {
        "write"
    } else {
        "readonly"
    }));
    if spec.denyoom {
        flags.push(Value::simple("denyoom"));
    }
    if movable {
        flags.push(Value::simple("movablekeys"));
    }
    Value::Array(Some(vec![
        Value::bulk(spec.name.to_ascii_lowercase()),
        Value::Integer(arity),
        Value::Array(Some(flags)),
        Value::Integer(first_key),
        Value::Integer(last_key),
        Value::Integer(step),
        // acl-categories, tips, key-specs, subcommands: empty (well-formed; not needed for routing).
        Value::Array(Some(vec![])),
        Value::Array(Some(vec![])),
        Value::Array(Some(vec![])),
        Value::Array(Some(vec![])),
    ]))
}

/// `COMMAND GETKEYS <command> [arg ...]` -> the routable keys of the supplied command line (#158).
/// Reuses the registry's [`command_spec::extract_keys`] (the SAME key extraction the cluster router
/// uses), so a cluster client's movable-key fallback agrees byte-for-byte with how the server would
/// route. Redis error parity: a missing inner command line is `wrong_arity`; an unknown inner
/// command is `Invalid command specified`; a known command with no key args is the
/// `command_no_key_args` message.
fn cmd_command_getkeys(req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("command|getkeys"));
    }
    // Build the inner request (the command + its args) the extraction operates on: args[2..].
    let inner = Request {
        args: req.args[2..].to_vec(),
    };
    let upper = ascii_upper(&inner.args[0]);
    let Some(spec) = command_spec::spec_of(&upper) else {
        return Value::error(ErrorReply::err("Invalid command specified"));
    };
    match command_spec::extract_keys(spec.key_spec, &inner) {
        crate::route::KeySpec::None => Value::error(ErrorReply::command_no_key_args()),
        crate::route::KeySpec::One(k) => Value::Array(Some(vec![Value::bulk(k.to_vec())])),
        crate::route::KeySpec::Many(keys) => Value::Array(Some(
            keys.into_iter().map(|k| Value::bulk(k.to_vec())).collect(),
        )),
    }
}

// -- helpers --

/// ASCII-uppercase a byte slice for case-insensitive command matching (the command token is
/// ASCII per RESP). Delegates to the canonical [`crate::cmd_util::ascii_upper`], whose
/// stack-backed [`UpperToken`](crate::cmd_util::UpperToken) uppercases the per-command token
/// with NO heap allocation on this dispatch hot path.
fn ascii_upper(b: &[u8]) -> crate::cmd_util::UpperToken {
    crate::cmd_util::ascii_upper(b)
}

/// Parse a base-10 i64 from an argument, returning `None` on any non-digit.
fn parse_int_arg(arg: &[u8]) -> Option<i64> {
    let s = core::str::from_utf8(arg).ok()?;
    s.parse::<i64>().ok()
}

/// Fold a read command's hit/miss into the keyspace counters (PR-3b, INFO
/// `keyspace_hits`/`keyspace_misses`), then return the reply unchanged.
///
/// A MISS is the "key not found" reply shape: a `Null` bulk (GET/GETEX absent). An
/// `Error` reply (e.g. WRONGTYPE) is NEITHER a hit nor a miss (it is not a successful
/// lookup result). Everything else is a HIT (the key was found live). This is applied
/// only to GET / GETEX, whose reply shape is an UNAMBIGUOUS found/not-found signal and
/// which Redis counts (a real keyspace LOOKUP). The TTL-family introspection commands
/// (TTL/PTTL/EXPIRETIME/PEXPIRETIME) use LOOKUP_NOTOUCH and are NOT counted (the #8
/// fix); STRLEN's reply collides with a real value (0) so it is also not counted.
fn keyspace_counted(deltas: &mut CounterDeltas, reply: Value) -> Value {
    match &reply {
        Value::Error(_) => {}
        // A `Null` bulk (GET/GETEX absent) is a miss; anything else (a found value) is
        // a hit.
        Value::Null => deltas.keyspace_misses += 1,
        _ => deltas.keyspace_hits += 1,
    }
    reply
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
