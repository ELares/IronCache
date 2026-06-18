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
    cmd_bitmap, cmd_cluster, cmd_config, cmd_expire, cmd_hash, cmd_hll, cmd_introspect,
    cmd_keyspace, cmd_list, cmd_set, cmd_string, cmd_txn, cmd_zset,
};
use ironcache_config::{ClusterMode, Config, RuntimeConfig};
use ironcache_env::{Clock, Env, Rng};
use ironcache_expiry::TimingWheel;
use ironcache_observe::{
    CounterDeltas, CounterSnapshot, EffectiveMemoryConfig, MemoryInfo, ReplicaLine,
    ReplicationInfo, ServerInfo, build_info,
};
use ironcache_protocol::{ErrorReply, ProtoVersion, Request, Value};
use ironcache_storage::{ActiveExpiry, Admit, Keyspace, PolicySwap, Store, UnixMillis, Watch};
use std::sync::Arc;

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

    /// HOW the cluster's slot map is governed (HA-4c): the boot `cluster_mode`. The DEFAULT is
    /// [`ClusterMode::Static`] (the byte-unchanged pre-HA-4c path). Only [`ClusterMode::Raft`]
    /// routes a CLUSTER mutator through the control plane (and only then is [`Self::raft`] set).
    /// Boot-only (like `cluster_enabled`), so it reads the boot config, not the runtime overlay.
    #[must_use]
    pub fn cluster_mode(&self) -> ClusterMode {
        self.boot.cluster_mode
    }
}

/// A source of the rolled-up counters for INFO. The serve loop supplies the
/// current per-shard snapshot (PR-1 reports the local shard's view; the
/// cross-shard rollup wires in with the coordinator later).
pub type RollupFn<'a> = &'a dyn Fn() -> CounterSnapshot;

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
        *shard_generation = current_generation;
    }
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
    mem: MemoryInfo,
    deltas: &mut CounterDeltas,
    req: &Request,
    cmd: &[u8],
) -> Value {
    *deltas = CounterDeltas::default();

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
    deltas.expired += drain_due_keys(wheel, store, now, MAX_RECLAIM_PER_CALL);

    // Auth gate: before authenticating, only a small set of commands is allowed
    // (Redis: HELLO, AUTH, QUIT, RESET). Everything else (including the data
    // commands) is NOAUTH. Runs once per client command; queued commands at EXEC time
    // are already past auth (you cannot MULTI before authenticating).
    if ctx.requires_auth()
        && !state.authenticated
        && !matches!(cmd, b"AUTH" | b"HELLO" | b"QUIT" | b"RESET")
    {
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

    dispatch_inner(
        ctx, state, env, store, wheel, now, rollup, mem, deltas, req, cmd,
    )
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
        b"EXEC" => exec_transaction(ctx, state, env, store, wheel, now, rollup, mem, deltas, req),
        b"CLIENT" => cmd_client(state, req),
        b"COMMAND" => cmd_command(req),
        // INFO reads only the CLOCK half of the env seam (uptime); pass `env` as the
        // `&C: Clock` it needs.
        b"INFO" => cmd_info(ctx, env, rollup, mem, req),
        // CONFIG GET/SET/RESETSTAT/REWRITE/HELP (PR-4b). RESETSTAT signals the serving
        // shard's counter reset via `deltas.reset_stats` (the serve loop honors it in
        // ShardCounters::apply); the serving-shard scope is documented in cmd_config.
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
        // The per-shard budget is recomputed from the CURRENT runtime maxmemory (a
        // cheap atomic load divided by the shard count), so a `CONFIG SET maxmemory`
        // tightens/loosens every shard's gate on its next denyoom write (PR-4b).
        let budget = ctx.per_shard_budget();
        if store.used_memory() > budget {
            if store.policy_evicts() {
                // Cache mode: try to free space, then re-check. If eviction cannot get
                // us down to budget (write outruns eviction, or only ineligible keys
                // remain), reject -OOM. The freed count is reported for INFO.
                deltas.evicted += store.evict_to_fit(budget, now);
                if store.used_memory() > budget {
                    return Value::error(ErrorReply::oom());
                }
            } else {
                // Strict datastore / noeviction: -OOM is the over-capacity behavior.
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
        b"APPEND" => cmd_string::cmd_append(store, db, now, req),
        b"DEL" => cmd_keyspace::cmd_del(store, db, now, req),
        b"EXISTS" => cmd_keyspace::cmd_exists(store, db, now, req),
        b"TYPE" => cmd_keyspace::cmd_type(store, db, now, req),
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
        // -- Bitmap commands (PR-9, BITMAPS.md) over the STRING type. A bitmap is the
        // string value addressed at bit granularity (TYPE=string, OBJECT ENCODING a
        // string encoding); these need no new type. Mutations (SETBIT, BITOP-dest,
        // BITFIELD with SET/INCRBY) route through the string `rmw` rebuild-Replace path;
        // reads (GETBIT/BITCOUNT/BITPOS/BITFIELD-all-GET/BITFIELD_RO) through `read`.
        // WRONGTYPE on a non-string. BITOP is multi-key (reads sources + writes dest on
        // this connection's accept shard, single-shard-per-connection like the other
        // multi-key commands; an empty result deletes dest). --
        b"SETBIT" => cmd_bitmap::cmd_setbit(store, db, now, req),
        b"GETBIT" => cmd_bitmap::cmd_getbit(store, db, now, req),
        b"BITCOUNT" => cmd_bitmap::cmd_bitcount(store, db, now, req),
        b"BITPOS" => cmd_bitmap::cmd_bitpos(store, db, now, req),
        b"BITOP" => cmd_bitmap::cmd_bitop(store, db, now, req),
        b"BITFIELD" => cmd_bitmap::cmd_bitfield(store, db, now, req),
        b"BITFIELD_RO" => cmd_bitmap::cmd_bitfield_ro(store, db, now, req),
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

    // (1) maxmemory-policy HOT-SWAP reach on the owning shard (CONFIG.md, PR-4b): one
    // relaxed atomic load + compare; the rebuild (rare) is the shared helper.
    maybe_hot_swap_policy(ctx, env, store, shard_generation, now);

    // (2) Active TTL reclamation on the owning shard (EXPIRATION.md #51), BEFORE the
    // command body, exactly like the home `dispatch`: drain a BOUNDED batch of due keys
    // from THIS shard's wheel and reap the genuinely-expired ones (the SAME
    // `drain_due_keys` helper). Bounds resident memory for expired keys under traffic.
    deltas.expired += drain_due_keys(wheel, store, now, MAX_RECLAIM_PER_CALL);

    // (3) The keyed command body + the per-command admission gate, via the SINGLE shared
    // keyed-arm definition (so home and remote cannot diverge).
    dispatch_keyed_data(ctx, env, store, wheel, db, now, deltas, req, &cmd)
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
    // a coordinator classification bug; refuse it rather than run it on a wrong path.
    if !matches!(
        crate::route::classify(&cmd),
        crate::route::CommandClass::WholeKeyspace
    ) {
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
        // The classify gate above already excludes everything else.
        _ => Value::error(ErrorReply::err(
            "command fanned out cross-shard is not whole-keyspace",
        )),
    }
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
    let mut replies = Vec::with_capacity(queued.len());
    for q in &queued {
        // Re-derive the uppercased token per queued command (cheap; the request was
        // validated at queue time). Reuse the SAME borrowed store/wheel/env + `now`:
        // no re-borrow of the thread-locals, no double-borrow. Counter deltas
        // ACCUMULATE across the batch (eviction / keyspace hits-misses use `+=`).
        let qcmd = ascii_upper(q.command());
        let reply = dispatch_inner(
            ctx, state, env, store, wheel, now, rollup, mem, deltas, q, &qcmd,
        );
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
            AuthResult::Ok => state.authenticated = true,
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
fn cmd_auth(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
    let (user, pass): (&[u8], &[u8]) = match req.args.len() {
        2 => (b"default", &req.args[1]),
        3 => (&req.args[1], &req.args[2]),
        _ => return Value::error(ErrorReply::wrong_arity("auth")),
    };
    match check_auth(ctx, user, pass) {
        AuthResult::Ok => {
            state.authenticated = true;
            Value::ok()
        }
        AuthResult::NoPasswordSet => Value::error(ErrorReply::auth_no_password_set()),
        AuthResult::WrongPass => Value::error(ErrorReply::wrongpass()),
    }
}

enum AuthResult {
    Ok,
    NoPasswordSet,
    WrongPass,
}

/// Check credentials against the CURRENT configured password (read from the runtime
/// overlay, so a `CONFIG SET requirepass` takes effect immediately, PR-4b). The single
/// `requirepass`/default-user model (full ACL is later). The username must be `default`
/// (or empty) when a password is set.
///
/// The password is stored as a SHA-256 digest AT REST (#65): the runtime overlay holds
/// only the SHA-256 HEX of the configured password, never the plaintext. Verification
/// HASHES the provided guess with [`ironcache_config::sha256_hex`] and compares the two
/// 64-char hex digests in CONSTANT TIME (see [`constant_time_eq`]), so the reply latency
/// does not leak how many leading bytes of a guess matched the secret (the timing-leak
/// finding). The plaintext guess lives only as the `pass` argument during hashing and is
/// never stored or logged. The threat model (#142) accepts plain SHA-256 (not a KDF) for
/// Redis behavioral equivalence (ADR-0009) and accepts the compare side-channel this
/// milestone.
fn check_auth(ctx: &ServerContext, user: &[u8], pass: &[u8]) -> AuthResult {
    match ctx.runtime.requirepass() {
        None => AuthResult::NoPasswordSet,
        Some(configured_hash) => {
            let user_ok = user.is_empty() || user.eq_ignore_ascii_case(b"default");
            // Hash the GUESS and compare digest-to-digest: the stored credential is the
            // SHA-256 hex, so we never compare (or hold) the plaintext at rest. The
            // compare is constant-time over the two fixed-width (64-char) hex digests.
            let guess_hash = ironcache_config::sha256_hex(pass);
            if user_ok && constant_time_eq(guess_hash.as_bytes(), configured_hash.as_bytes()) {
                AuthResult::Ok
            } else {
                AuthResult::WrongPass
            }
        }
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
    Value::ok()
}

/// `CLIENT <subcommand>` (handshake-critical subset, PROTOCOL.md).
fn cmd_client(state: &mut ConnState, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("client"));
    }
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
        b"NO-EVICT" | b"NO-TOUCH" => Value::ok(),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "CLIENT",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// A single-line CLIENT INFO description (subset of Redis fields).
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

/// `COMMAND [DOCS|COUNT|...]` (startup stubs, PROTOCOL.md).
fn cmd_command(req: &Request) -> Value {
    if req.args.len() == 1 {
        // Bare COMMAND returns the (empty in PR-1) command table as an array.
        return Value::Array(Some(vec![]));
    }
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        // COUNT: number of supported commands. PR-1 reports 0 (table not yet
        // generated); clients that call COUNT tolerate any integer.
        b"COUNT" => Value::Integer(0),
        // DOCS: an empty map is well-formed and accepted by clients at startup.
        b"DOCS" => Value::Map(vec![]),
        b"GETKEYS" => Value::error(ErrorReply::command_no_key_args()),
        // INFO and any other subcommand: an empty, well-formed array. DELIBERATE
        // divergence from the sibling stubs (CLIENT/CONFIG return an
        // unknown_subcommand error for an unknown sub): COMMAND is probed at client
        // startup with assorted subcommands, and an empty array is more tolerant
        // than an error. Do not "fix" this to unknown_subcommand without checking
        // client startup probes (PR-1 has no command table yet).
        _ => Value::Array(Some(vec![])),
    }
}

/// `INFO [section]` -> delegates to ironcache-observe. `mem` is the process-global
/// allocator snapshot (ADR-0006) the caller read once at the binary edge (the
/// server crate has no access to the concrete store's mallctl readers, by the
/// layering contract; the binary supplies the figure).
fn cmd_info<C: Clock>(
    ctx: &ServerContext,
    clock: &C,
    rollup: RollupFn<'_>,
    mem: MemoryInfo,
    req: &Request,
) -> Value {
    let section = if req.args.len() >= 2 {
        Some(String::from_utf8_lossy(&req.args[1]).into_owned())
    } else {
        None
    };
    // PR-4b: report the CURRENT effective maxmemory + policy (read from the runtime
    // overlay), so a `CONFIG SET maxmemory`/`maxmemory-policy` is reflected in INFO.
    // The policy name is cloned once here (off the per-command hot path: INFO is rare).
    let policy = ctx.runtime.policy_name();
    let effective = EffectiveMemoryConfig {
        maxmemory: ctx.runtime.maxmemory(),
        maxmemory_policy: &policy,
    };
    // The `# Replication` section facts (HA-7e): translate the node-level repl status snapshot to
    // the observe POD. `None` (the default static path, no status cell) -> the byte-compatible
    // standalone master-at-offset-0 posture.
    let replication = replication_info(ctx);
    let body = build_info(
        clock,
        &ctx.info,
        rollup(),
        mem,
        effective,
        &replication,
        section.as_deref(),
    );
    Value::bulk(body.into_bytes())
}

/// Build the INFO `# Replication` facts (HA-7e) from `ctx`'s node-level replication status. When
/// no status cell is present (the DEFAULT static path / standalone), returns
/// [`ReplicationInfo::standalone`] -- a master with no slaves at offset 0, byte-compatible with a
/// standalone Redis. In raft-mode it reads a [`ReplStatusSnapshot`] and maps it to Redis's field
/// shape: a master reports its head + a `slaveN:` line (with the per-replica lag) per connected
/// replica; a replica reports its master endpoint, link status, and applied offset.
fn replication_info(ctx: &ServerContext) -> ReplicationInfo {
    let Some(status) = ctx.repl_status.as_ref() else {
        return ReplicationInfo::standalone();
    };
    let snap = status.snapshot();
    match snap.role {
        ironcache_repl::ReplRole::Master => {
            // One `slaveN:` line per connected replica (0 or 1 in the single-shard HA-7d wiring).
            // The lag is the master's view (`head - replica_acked`), known while connected.
            let mut slaves = Vec::new();
            if snap.connected_slaves > 0 {
                let lag = snap
                    .slave_lag()
                    .and_then(ironcache_repl::ReplicaLag::lag)
                    .unwrap_or(0);
                // The master does not learn the replica's advertised client endpoint over the
                // current handshake (HA-7d sends node id + ack only), so report a placeholder
                // endpoint; the offset + lag (the load-bearing fields HA-8 + operators read) are
                // real. A follow-up can carry the replica's advertised endpoint in the handshake.
                slaves.push(ReplicaLine {
                    ip: String::new(),
                    port: 0,
                    offset: snap.slave_offset.0,
                    lag,
                });
            }
            ReplicationInfo {
                is_master: true,
                master_repl_offset: snap.node_offset.0,
                slaves,
                master_endpoint: None,
                master_link_up: false,
                slave_repl_offset: 0,
            }
        }
        ironcache_repl::ReplRole::Replica => ReplicationInfo {
            is_master: false,
            // master_repl_offset on a replica = the master's head as last observed on the link.
            master_repl_offset: snap.master_offset.0,
            slaves: Vec::new(),
            master_endpoint: snap.master_endpoint.clone(),
            master_link_up: snap.master_link.is_up(),
            // slave_repl_offset = this replica's own applied offset.
            slave_repl_offset: snap.node_offset.0,
        },
    }
}

// -- helpers --

/// ASCII-uppercase a byte slice into an owned `Vec<u8>` for case-insensitive
/// command matching (the command token is ASCII per RESP).
fn ascii_upper(b: &[u8]) -> Vec<u8> {
    b.iter().map(u8::to_ascii_uppercase).collect()
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
mod tests {
    use super::*;
    use bytes::Bytes;
    use ironcache_env::{Monotonic, TestEnv};
    use ironcache_eviction::{Policy, map_policy_name};
    use ironcache_storage::CountingAccounting;
    use ironcache_store::ShardStore;

    /// The store type the dispatch tests drive: the concrete per-shard store wired
    /// with a real eviction policy (so it satisfies the `Admit` bound dispatch now
    /// requires). Defaults to the cache-mode S3-FIFO policy.
    type TestStore = ShardStore<Policy, CountingAccounting>;

    /// A test store with `databases` DBs and the given policy.
    fn store_with(databases: u32, policy: Policy) -> TestStore {
        ShardStore::with_hooks(databases, policy, CountingAccounting::new())
    }

    /// The default test store (cache-mode S3-FIFO, ceiling off).
    fn test_store(databases: u32) -> TestStore {
        store_with(databases, Policy::cache_default())
    }

    fn ctx(pass: Option<&str>) -> ServerContext {
        ctx_full(pass, 0, "allkeys-lru")
    }

    /// A test context with an explicit requirepass, maxmemory ceiling, and policy name
    /// seeded into the runtime overlay (so the generation-gated swap + ceiling tests
    /// can drive the shared cell directly).
    fn ctx_full(pass: Option<&str>, maxmemory: u64, policy: &str) -> ServerContext {
        let boot = ironcache_config::Config {
            maxmemory,
            maxmemory_policy: policy.to_owned(),
            // `Config::requirepass` holds the SHA-256 HEX at rest (#65), so the test
            // harness hashes the test PLAINTEXT just as a real boot would (resolve()
            // hashes it). AUTH with the plaintext then verifies by hashing the guess and
            // matching this digest.
            requirepass: pass.map(|p| ironcache_config::sha256_hex(p.as_bytes())),
            databases: 16,
            shards: 1,
            ..ironcache_config::Config::default()
        };
        let runtime = RuntimeConfig::from_config(&boot);
        ServerContext {
            runtime,
            databases: 16,
            shards: 1,
            info: ServerInfo {
                tcp_port: 6379,
                shards: 1,
                pid: 1,
                started_at: Monotonic::ZERO,
                maxmemory,
                maxmemory_policy: "allkeys-lru",
                mem_allocator: "jemalloc",
                cluster_node_id: "0000000000000000000000000000000000000000",
                cluster_enabled: false,
            },
            cluster: None,
            raft: None,
            repl_status: None,
            in_sync_replicas: None,
            boot,
        }
    }

    fn state(ctx: &ServerContext) -> ConnState {
        ConnState::new(
            7,
            ProtoVersion::Resp2,
            ctx.requires_auth(),
            "127.0.0.1:1".to_owned(),
            "127.0.0.1:6379".to_owned(),
        )
    }

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    fn run(ctx: &ServerContext, st: &mut ConnState, parts: &[&[u8]]) -> Value {
        let mut env = TestEnv::new(1);
        let mut store = test_store(ctx.databases);
        let mut wheel = TimingWheel::new();
        let zero = || CounterSnapshot::default();
        let mut deltas = CounterDeltas::default();
        let mut shard_gen = ctx.runtime.generation();
        dispatch(
            ctx,
            st,
            &mut env,
            &mut store,
            &mut wheel,
            UnixMillis(0),
            &mut shard_gen,
            &zero,
            MemoryInfo::default(),
            &mut deltas,
            &req(parts),
        )
    }

    /// Like [`run`] but threads a caller-owned store and `now`, for the data-command
    /// tests that need state to persist across calls (SET then GET) and a clock to
    /// advance (EX/lazy expiry).
    fn run_on(
        ctx: &ServerContext,
        st: &mut ConnState,
        store: &mut TestStore,
        now: UnixMillis,
        parts: &[&[u8]],
    ) -> Value {
        let mut wheel = TimingWheel::new();
        run_on_wheel(ctx, st, store, &mut wheel, now, parts)
    }

    /// Like [`run_on`] but threads a caller-owned [`TimingWheel`] (and surfaces the
    /// counter deltas), for the EXPIRE / active-drain tests that need the wheel to
    /// persist across calls (register on one command, drain on a later one).
    fn run_on_wheel(
        ctx: &ServerContext,
        st: &mut ConnState,
        store: &mut TestStore,
        wheel: &mut TimingWheel,
        now: UnixMillis,
        parts: &[&[u8]],
    ) -> Value {
        let (reply, _deltas) = run_on_wheel_deltas(ctx, st, store, wheel, now, parts);
        reply
    }

    /// Like [`run_on_wheel`] but also returns the [`CounterDeltas`] dispatch produced
    /// (the active-drain expiry count and keyspace hit/miss), for the counter tests.
    fn run_on_wheel_deltas(
        ctx: &ServerContext,
        st: &mut ConnState,
        store: &mut TestStore,
        wheel: &mut TimingWheel,
        now: UnixMillis,
        parts: &[&[u8]],
    ) -> (Value, CounterDeltas) {
        let mut env = TestEnv::new(1);
        let zero = || CounterSnapshot::default();
        let mut deltas = CounterDeltas::default();
        let mut shard_gen = ctx.runtime.generation();
        let reply = dispatch(
            ctx,
            st,
            &mut env,
            store,
            wheel,
            now,
            &mut shard_gen,
            &zero,
            MemoryInfo::default(),
            &mut deltas,
            &req(parts),
        );
        (reply, deltas)
    }

    #[test]
    fn ping_variants() {
        let c = ctx(None);
        let mut s = state(&c);
        assert_eq!(run(&c, &mut s, &[b"PING"]), Value::simple("PONG"));
        assert_eq!(
            run(&c, &mut s, &[b"ping", b"hi"]),
            Value::BulkString(Some(Bytes::from_static(b"hi")))
        );
        assert_eq!(run(&c, &mut s, &[b"PinG"]), Value::simple("PONG")); // case-insensitive
    }

    #[test]
    fn unknown_command_is_byte_exact() {
        let c = ctx(None);
        let mut s = state(&c);
        let v = run(&c, &mut s, &[b"FROBNICATE", b"a", b"b"]);
        match v {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-ERR unknown command 'FROBNICATE', with args beginning with: 'a' 'b' "
            ),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn hello_no_version_keeps_proto_and_returns_map() {
        let c = ctx(None);
        let mut s = state(&c);
        let v = run(&c, &mut s, &[b"HELLO"]);
        assert!(matches!(v, Value::Map(_)));
        assert_eq!(s.proto, ProtoVersion::Resp2);
    }

    #[test]
    fn hello_3_upgrades_proto() {
        let c = ctx(None);
        let mut s = state(&c);
        let v = run(&c, &mut s, &[b"HELLO", b"3"]);
        assert!(matches!(v, Value::Map(_)));
        assert_eq!(s.proto, ProtoVersion::Resp3);
    }

    #[test]
    fn hello_bad_version_is_noproto_and_does_not_switch() {
        let c = ctx(None);
        let mut s = state(&c);
        let v = run(&c, &mut s, &[b"HELLO", b"4"]);
        match v {
            Value::Error(e) => assert_eq!(e.line(), "-NOPROTO unsupported protocol version"),
            other => panic!("expected NOPROTO, got {other:?}"),
        }
        assert_eq!(s.proto, ProtoVersion::Resp2);
    }

    #[test]
    fn hello_with_setname() {
        let c = ctx(None);
        let mut s = state(&c);
        let _ = run(&c, &mut s, &[b"HELLO", b"3", b"SETNAME", b"app1"]);
        assert_eq!(s.name, "app1");
        assert_eq!(s.proto, ProtoVersion::Resp3);
    }

    #[test]
    fn hello_auth_success_and_failure() {
        let c = ctx(Some("s3cr3t"));
        let mut s = state(&c);
        // Wrong pass -> wrongpass, proto unchanged, not authenticated.
        let v = run(&c, &mut s, &[b"HELLO", b"3", b"AUTH", b"default", b"nope"]);
        assert!(matches!(v, Value::Error(_)));
        assert!(!s.authenticated);
        // Correct pass -> map, authenticated, proto upgraded.
        let v = run(
            &c,
            &mut s,
            &[b"HELLO", b"3", b"AUTH", b"default", b"s3cr3t"],
        );
        assert!(matches!(v, Value::Map(_)));
        assert!(s.authenticated);
        assert_eq!(s.proto, ProtoVersion::Resp3);
    }

    #[test]
    fn auth_no_password_configured() {
        let c = ctx(None);
        let mut s = state(&c);
        let v = run(&c, &mut s, &[b"AUTH", b"whatever"]);
        match v {
            Value::Error(e) => assert!(e.line().starts_with(
                "-ERR AUTH <password> called without any password configured for the default user"
            )),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn noauth_gate_blocks_until_authenticated() {
        let c = ctx(Some("pw"));
        let mut s = state(&c);
        // PING before auth is refused.
        let v = run(&c, &mut s, &[b"PING"]);
        match v {
            Value::Error(e) => assert_eq!(e.line(), "-NOAUTH Authentication required."),
            other => panic!("expected NOAUTH, got {other:?}"),
        }
        // AUTH then PING works.
        assert_eq!(run(&c, &mut s, &[b"AUTH", b"pw"]), Value::ok());
        assert_eq!(run(&c, &mut s, &[b"PING"]), Value::simple("PONG"));
    }

    #[test]
    fn auth_correct_password_succeeds_wrong_password_is_wrongpass() {
        // The constant-time compare must still be CORRECT: the exact password
        // authenticates, and any mismatch (wrong content, or a prefix/suffix of the
        // secret) is WRONGPASS. We cannot test timing here, only that the constant-time
        // path returns the right answer.
        let c = ctx(Some("s3cr3t"));
        // Correct password authenticates.
        let mut ok = state(&c);
        assert_eq!(run(&c, &mut ok, &[b"AUTH", b"s3cr3t"]), Value::ok());
        // A same-length wrong password is WRONGPASS.
        let mut bad = state(&c);
        match run(&c, &mut bad, &[b"AUTH", b"s3cr3T"]) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-WRONGPASS invalid username-password pair or user is disabled."
            ),
            other => panic!("expected WRONGPASS, got {other:?}"),
        }
        // A shorter password sharing the secret's prefix is WRONGPASS (length differs).
        let mut shortp = state(&c);
        match run(&c, &mut shortp, &[b"AUTH", b"s3cr3"]) {
            Value::Error(e) => assert!(e.line().starts_with("-WRONGPASS")),
            other => panic!("expected WRONGPASS, got {other:?}"),
        }
        // A longer password with the secret as a prefix is WRONGPASS.
        let mut longp = state(&c);
        match run(&c, &mut longp, &[b"AUTH", b"s3cr3t!"]) {
            Value::Error(e) => assert!(e.line().starts_with("-WRONGPASS")),
            other => panic!("expected WRONGPASS, got {other:?}"),
        }
    }

    #[test]
    fn requirepass_stored_as_hash_not_plaintext() {
        // SECURITY (#65): the runtime overlay the auth path reads holds ONLY the SHA-256
        // hex digest of the password, never the plaintext.
        let c = ctx(Some("s3cr3t"));
        let stored = c.runtime.requirepass().expect("requirepass should be set");
        assert_eq!(stored, ironcache_config::sha256_hex(b"s3cr3t"));
        assert_eq!(stored.len(), 64);
        assert_ne!(stored, "s3cr3t");
        // The boot config likewise holds the digest, not the plaintext.
        assert_eq!(
            c.boot.requirepass.as_deref(),
            Some(ironcache_config::sha256_hex(b"s3cr3t").as_str())
        );
        assert_ne!(c.boot.requirepass.as_deref(), Some("s3cr3t"));
    }

    #[test]
    fn config_set_requirepass_then_auth_with_plaintext_succeeds() {
        // SECURITY (#65): hash-on-set (CONFIG SET) and hash-on-verify (AUTH) converge.
        // A CONFIG SET requirepass <plaintext> stores the digest; AUTH with that SAME
        // plaintext then authenticates (the guess is hashed and matches the stored
        // digest), while a wrong plaintext is WRONGPASS.
        let c = ctx(None);
        let mut admin = state(&c);
        // No password yet: AUTH reports no-password-configured.
        match run(&c, &mut admin, &[b"AUTH", b"newpass"]) {
            Value::Error(e) => assert!(e.line().starts_with(
                "-ERR AUTH <password> called without any password configured for the default user"
            )),
            other => panic!("expected auth_no_password_set, got {other:?}"),
        }
        // CONFIG SET requirepass with a plaintext password.
        assert_eq!(
            run(
                &c,
                &mut admin,
                &[b"CONFIG", b"SET", b"requirepass", b"newpass"]
            ),
            Value::ok()
        );
        // The overlay now holds the DIGEST, not the plaintext.
        assert_eq!(
            c.runtime.requirepass().as_deref(),
            Some(ironcache_config::sha256_hex(b"newpass").as_str())
        );
        // A fresh connection (built once a password is configured) starts unauthenticated.
        let mut fresh = state(&c);
        assert!(!fresh.authenticated);
        assert_eq!(run(&c, &mut fresh, &[b"AUTH", b"newpass"]), Value::ok());
        assert!(fresh.authenticated);
        // A wrong plaintext is a digest mismatch -> WRONGPASS.
        let mut wrong = state(&c);
        match run(&c, &mut wrong, &[b"AUTH", b"nope"]) {
            Value::Error(e) => assert!(e.line().starts_with("-WRONGPASS")),
            other => panic!("expected WRONGPASS, got {other:?}"),
        }
    }

    #[test]
    fn constant_time_eq_matches_naive_equality() {
        // The hand-rolled constant-time compare agrees with naive equality on a spread
        // of length/content cases (correctness of the timing-safe path).
        let cases: &[(&[u8], &[u8])] = &[
            (b"", b""),
            (b"a", b"a"),
            (b"a", b"b"),
            (b"", b"x"),
            (b"abc", b"ab"),
            (b"abc", b"abc"),
            (b"abc", b"abd"),
            (b"secret", b"secret"),
            (b"secret", b"Secret"),
        ];
        for &(a, b) in cases {
            assert_eq!(
                constant_time_eq(a, b),
                a == b,
                "constant_time_eq disagreed for {a:?} vs {b:?}"
            );
        }
    }

    #[test]
    fn select_range_validation() {
        let c = ctx(None);
        let mut s = state(&c);
        assert_eq!(run(&c, &mut s, &[b"SELECT", b"3"]), Value::ok());
        assert_eq!(s.db, 3);
        match run(&c, &mut s, &[b"SELECT", b"16"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR DB index is out of range"),
            other => panic!("expected range error, got {other:?}"),
        }
        match run(&c, &mut s, &[b"SELECT", b"-1"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR DB index is out of range"),
            other => panic!("expected range error, got {other:?}"),
        }
        match run(&c, &mut s, &[b"SELECT", b"abc"]) {
            Value::Error(e) => assert!(e.line().contains("not an integer")),
            other => panic!("expected int error, got {other:?}"),
        }
    }

    #[test]
    fn quit_sets_close_and_replies_ok() {
        let c = ctx(None);
        let mut s = state(&c);
        assert_eq!(run(&c, &mut s, &[b"QUIT"]), Value::ok());
        assert!(s.should_close);
    }

    #[test]
    fn reset_clears_state() {
        let c = ctx(None);
        let mut s = state(&c);
        let _ = run(&c, &mut s, &[b"HELLO", b"3", b"SETNAME", b"x"]);
        let _ = run(&c, &mut s, &[b"SELECT", b"5"]);
        let v = run(&c, &mut s, &[b"RESET"]);
        assert_eq!(v, Value::SimpleString("RESET".to_owned()));
        assert_eq!(s.proto, ProtoVersion::Resp2);
        assert_eq!(s.db, 0);
        assert_eq!(s.name, "");
    }

    #[test]
    fn client_subcommands() {
        let c = ctx(None);
        let mut s = state(&c);
        assert_eq!(run(&c, &mut s, &[b"CLIENT", b"ID"]), Value::Integer(7));
        assert_eq!(
            run(&c, &mut s, &[b"CLIENT", b"SETNAME", b"app"]),
            Value::ok()
        );
        assert_eq!(s.name, "app");
        assert_eq!(
            run(&c, &mut s, &[b"CLIENT", b"GETNAME"]),
            Value::bulk_str("app")
        );
        // Name with space rejected.
        assert!(matches!(
            run(&c, &mut s, &[b"CLIENT", b"SETNAME", b"a b"]),
            Value::Error(_)
        ));
        // INFO is a bulk string mentioning the id.
        match run(&c, &mut s, &[b"CLIENT", b"INFO"]) {
            Value::BulkString(Some(b)) => {
                assert!(String::from_utf8_lossy(&b).contains("id=7"));
            }
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    #[test]
    fn command_stubs_well_formed() {
        let c = ctx(None);
        let mut s = state(&c);
        assert!(matches!(
            run(&c, &mut s, &[b"COMMAND"]),
            Value::Array(Some(_))
        ));
        assert_eq!(run(&c, &mut s, &[b"COMMAND", b"COUNT"]), Value::Integer(0));
        assert!(matches!(
            run(&c, &mut s, &[b"COMMAND", b"DOCS"]),
            Value::Map(_)
        ));
    }

    #[test]
    fn info_delegates_and_includes_port() {
        let c = ctx(None);
        let mut s = state(&c);
        match run(&c, &mut s, &[b"INFO"]) {
            Value::BulkString(Some(b)) => {
                assert!(String::from_utf8_lossy(&b).contains("tcp_port:6379"));
            }
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    /// The INFO body as a `String` (the test reads the bulk reply text).
    fn info_text(c: &ServerContext, s: &mut ConnState, section: &[&[u8]]) -> String {
        let mut args: Vec<&[u8]> = vec![b"INFO"];
        args.extend_from_slice(section);
        match run(c, s, &args) {
            Value::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
            other => panic!("expected bulk INFO, got {other:?}"),
        }
    }

    /// HA-7e: with NO repl status cell (the default static path), INFO `# Replication` reports the
    /// byte-compatible standalone posture: role:master, connected_slaves:0, master_repl_offset:0.
    #[test]
    fn info_replication_default_is_standalone_master() {
        let c = ctx(None); // ctx has repl_status: None
        let mut s = state(&c);
        let body = info_text(&c, &mut s, &[b"replication"]);
        assert!(body.contains("# Replication\r\n"), "{body}");
        assert!(body.contains("role:master\r\n"), "{body}");
        assert!(body.contains("connected_slaves:0\r\n"), "{body}");
        assert!(body.contains("master_repl_offset:0\r\n"), "{body}");
        assert!(!body.contains("slave0:"), "{body}");
    }

    /// HA-7e: a master with a connected replica reports connected_slaves:1 + a slave0: line with
    /// the slave's offset + lag.
    #[test]
    fn info_replication_master_with_connected_slave() {
        let mut c = ctx(None);
        let status = std::sync::Arc::new(ironcache_repl::ReplNodeStatus::new());
        status.set_master_head(ironcache_repl::ReplOffset(200));
        status.set_replica_connected(ironcache_repl::ReplOffset(190)); // lag 10
        c.repl_status = Some(status);
        let mut s = state(&c);
        let body = info_text(&c, &mut s, &[b"replication"]);
        assert!(body.contains("role:master\r\n"), "{body}");
        assert!(body.contains("connected_slaves:1\r\n"), "{body}");
        // The slaveN line carries the offset + lag (the endpoint is a placeholder in the MVP
        // handshake; the offset/lag are the load-bearing fields).
        assert!(
            body.contains("state=online,offset=190,lag=10\r\n"),
            "{body}"
        );
        assert!(body.contains("master_repl_offset:200\r\n"), "{body}");
    }

    /// HA-7e: a replica reports role:replica, its master endpoint, master_link_status, the offsets,
    /// and slave_read_only:1.
    #[test]
    fn info_replication_replica_view() {
        let mut c = ctx(None);
        let status = std::sync::Arc::new(ironcache_repl::ReplNodeStatus::new());
        status.set_replica_attached("10.0.0.9", 6400, ironcache_repl::ReplOffset(50));
        status.set_observed_master_head(ironcache_repl::ReplOffset(60));
        status.set_replica_applied(ironcache_repl::ReplOffset(58));
        c.repl_status = Some(status);
        let mut s = state(&c);
        let body = info_text(&c, &mut s, &[b"replication"]);
        assert!(body.contains("role:replica\r\n"), "{body}");
        assert!(body.contains("master_host:10.0.0.9\r\n"), "{body}");
        assert!(body.contains("master_port:6400\r\n"), "{body}");
        assert!(body.contains("master_link_status:up\r\n"), "{body}");
        assert!(body.contains("slave_read_only:1\r\n"), "{body}");
        assert!(body.contains("slave_repl_offset:58\r\n"), "{body}");
        assert!(body.contains("master_repl_offset:60\r\n"), "{body}");
    }

    // -- Data commands (PR-2a) through dispatch over a real ShardStore. --

    fn bulk(b: &[u8]) -> Value {
        Value::BulkString(Some(Bytes::copy_from_slice(b)))
    }

    #[test]
    fn set_then_get_round_trips() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"foo", b"bar"]),
            Value::ok()
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"foo"]),
            bulk(b"bar")
        );
        // Missing key -> null.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"nope"]),
            Value::Null
        );
    }

    #[test]
    fn set_nx_only_when_absent() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v1", b"NX"]),
            Value::ok()
        );
        // Second NX on a present key -> nil, value unchanged.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v2", b"NX"]),
            Value::Null
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"v1"));
    }

    #[test]
    fn set_xx_only_when_present() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // XX on absent key -> nil, nothing written.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v", b"XX"]),
            Value::Null
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
        // Create, then XX overwrite works.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v1"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v2", b"XX"]),
            Value::ok()
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"v2"));
    }

    #[test]
    fn set_get_returns_old_value() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"old"]);
        // SET k new XX GET -> returns old, writes new.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", b"k", b"new", b"XX", b"GET"]
            ),
            bulk(b"old")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]),
            bulk(b"new")
        );
        // SET GET on an absent key returns null and writes the new value.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"fresh", b"v", b"GET"]),
            Value::Null
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"fresh"]),
            bulk(b"v")
        );
    }

    #[test]
    fn set_keepttl_preserves_deadline() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        // Set with a 100-second TTL at t=0 (deadline 100000ms).
        run_on(
            &c,
            &mut s,
            &mut st,
            UnixMillis(0),
            &[b"SET", b"k", b"a", b"EX", b"100"],
        );
        // KEEPTTL overwrite at t=1000: value changes, deadline preserved.
        run_on(
            &c,
            &mut s,
            &mut st,
            UnixMillis(1_000),
            &[b"SET", b"k", b"b", b"KEEPTTL"],
        );
        // Alive AT the original deadline (Valkey boundary is `now > deadline`).
        assert_eq!(
            run_on(&c, &mut s, &mut st, UnixMillis(100_000), &[b"GET", b"k"]),
            bulk(b"b")
        );
        // Expired one ms past the original deadline (KEEPTTL kept it, did not extend).
        assert_eq!(
            run_on(&c, &mut s, &mut st, UnixMillis(100_001), &[b"GET", b"k"]),
            Value::Null
        );
    }

    #[test]
    fn set_ex_stores_deadline_and_lazy_expires() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        // EX 10 at t=0 -> deadline 10000ms.
        run_on(
            &c,
            &mut s,
            &mut st,
            UnixMillis(0),
            &[b"SET", b"k", b"v", b"EX", b"10"],
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, UnixMillis(9_999), &[b"GET", b"k"]),
            bulk(b"v")
        );
        // Alive AT the deadline (Valkey boundary is `now > deadline`).
        assert_eq!(
            run_on(&c, &mut s, &mut st, UnixMillis(10_000), &[b"GET", b"k"]),
            bulk(b"v")
        );
        // Expired one ms past the deadline.
        assert_eq!(
            run_on(&c, &mut s, &mut st, UnixMillis(10_001), &[b"GET", b"k"]),
            Value::Null
        );
    }

    #[test]
    fn set_conflicting_options_is_syntax_error() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        for opts in [
            vec![b"SET".as_slice(), b"k", b"v", b"NX", b"XX"],
            vec![b"SET", b"k", b"v", b"EX", b"1", b"PX", b"1"],
            vec![b"SET", b"k", b"v", b"EX", b"1", b"KEEPTTL"],
            vec![b"SET", b"k", b"v", b"BOGUS"],
        ] {
            match run_on(&c, &mut s, &mut st, t, &opts) {
                Value::Error(e) => assert_eq!(e.line(), "-ERR syntax error", "{opts:?}"),
                other => panic!("expected syntax error for {opts:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn set_non_positive_or_overflowing_expire_is_invalid_expire_time() {
        // Redis emits `-ERR invalid expire time in 'set' command` (a class DISTINCT
        // from syntax error) for an EX/PX/EXAT/PXAT value <= 0 or one that overflows
        // the millisecond computation. Nothing is written.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        for opts in [
            vec![b"SET".as_slice(), b"k", b"v", b"EX", b"0"],
            vec![b"SET", b"k", b"v", b"EX", b"-1"],
            vec![b"SET", b"k", b"v", b"PX", b"0"],
            vec![b"SET", b"k", b"v", b"EXAT", b"0"],
            vec![b"SET", b"k", b"v", b"PXAT", b"0"],
            // EX * 1000 overflows i64 -> invalid expire (an integer, but out of ms range).
            vec![b"SET", b"k", b"v", b"EX", b"9223372036854775807"],
        ] {
            match run_on(&c, &mut s, &mut st, t, &opts) {
                Value::Error(e) => assert_eq!(
                    e.line(),
                    "-ERR invalid expire time in 'set' command",
                    "{opts:?}"
                ),
                other => panic!("expected invalid expire time for {opts:?}, got {other:?}"),
            }
        }
        // No key was ever written.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
    }

    #[test]
    fn set_non_integer_expire_is_not_an_integer_error() {
        // A NON-integer expire argument is the shared not-an-integer error, thrown
        // BEFORE the <= 0 check (a distinct class from invalid expire time). A
        // leading '+' is also rejected (Redis string2ll rejects '+').
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        for opts in [
            vec![b"SET".as_slice(), b"k", b"v", b"EX", b"abc"],
            vec![b"SET", b"k", b"v", b"PX", b"1.5"],
            vec![b"SET", b"k", b"v", b"EX", b"+5"],
        ] {
            match run_on(&c, &mut s, &mut st, t, &opts) {
                Value::Error(e) => assert_eq!(
                    e.line(),
                    "-ERR value is not an integer or out of range",
                    "{opts:?}"
                ),
                other => panic!("expected not-an-integer for {opts:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn setnx_and_getset() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SETNX", b"k", b"v1"]),
            Value::Integer(1)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SETNX", b"k", b"v2"]),
            Value::Integer(0)
        );
        // GETSET returns old and writes new.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GETSET", b"k", b"v3"]),
            bulk(b"v1")
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"v3"));
        // GETSET on absent key returns null.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GETSET", b"new", b"x"]),
            Value::Null
        );
    }

    #[test]
    fn del_and_exists_variadic_counts() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
        // EXISTS counts repeats (Redis semantics).
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"EXISTS", b"a", b"a", b"b", b"missing"]
            ),
            Value::Integer(3)
        );
        // DEL removes live keys, returns count removed.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DEL", b"a", b"b", b"missing"]),
            Value::Integer(2)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"a", b"b"]),
            Value::Integer(0)
        );
    }

    #[test]
    fn type_and_strlen() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"k"]),
            Value::simple("none")
        );
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"hello"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"k"]),
            Value::simple("string")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"k"]),
            Value::Integer(5)
        );
        // STRLEN of an int value is the decimal length.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"-12345"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"n"]),
            Value::Integer(6)
        );
        // STRLEN of an absent key is 0.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"gone"]),
            Value::Integer(0)
        );
    }

    #[test]
    fn wrongtype_on_get_against_non_string() {
        use ironcache_storage::{DataType, Encoding};
        use ironcache_store::kvobj::{Header, KvObj, ValueRepr};

        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);

        // Plant a non-String value directly (PR-2a commands only ever produce
        // Strings, so this is the only way to reach the WRONGTYPE branch before
        // collections land). A List-typed kvobj under key "lst".
        let mut obj = KvObj::from_bytes(b"lst", b"x", None);
        obj.header = Header {
            data_type: DataType::List,
            encoding: Encoding::ListPack,
            eviction_rank: 0,
            ttl_present: false,
            snapshot_version: 0,
        };
        obj.value = ValueRepr::Inline(Box::from(&b"x"[..]));
        st.insert_object(0, obj);

        // GET / STRLEN / GETSET against the non-string -> WRONGTYPE.
        match run_on(&c, &mut s, &mut st, t, &[b"GET", b"lst"]) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-WRONGTYPE Operation against a key holding the wrong kind of value"
            ),
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
        assert!(matches!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"lst"]),
            Value::Error(_)
        ));
        assert!(matches!(
            run_on(&c, &mut s, &mut st, t, &[b"GETSET", b"lst", b"v"]),
            Value::Error(_)
        ));
        // TYPE never returns WRONGTYPE; it reports the type name.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"lst"]),
            Value::simple("list")
        );
    }

    #[test]
    fn mget_returns_null_for_missing_and_non_string_never_wrongtype() {
        use ironcache_storage::{DataType, Encoding};
        use ironcache_store::kvobj::{Header, KvObj, ValueRepr};

        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);

        // A real string, a missing key, and a non-string (list) value.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"str", b"hi"]),
            Value::ok()
        );
        let mut obj = KvObj::from_bytes(b"lst", b"x", None);
        obj.header = Header {
            data_type: DataType::List,
            encoding: Encoding::ListPack,
            eviction_rank: 0,
            ttl_present: false,
            snapshot_version: 0,
        };
        obj.value = ValueRepr::Inline(Box::from(&b"x"[..]));
        st.insert_object(0, obj);

        // MGET str missing lst -> [bulk("hi"), Null, Null]. The non-string yields Null,
        // NOT a WRONGTYPE error (MGET never errors on a wrong-type element, matching Redis).
        let reply = run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"MGET", b"str", b"missing", b"lst"],
        );
        assert_eq!(
            reply,
            Value::Array(Some(vec![
                Value::BulkString(Some(bytes::Bytes::from_static(b"hi"))),
                Value::Null,
                Value::Null,
            ])),
            "MGET: present string -> bulk, missing -> Null, non-string -> Null (no WRONGTYPE)"
        );

        // MGET arity: bare MGET (no key) is the wrong-arity error.
        match run_on(&c, &mut s, &mut st, t, &[b"MGET"]) {
            Value::Error(e) => {
                assert_eq!(
                    e.line(),
                    "-ERR wrong number of arguments for 'mget' command"
                );
            }
            other => panic!("bare MGET must be wrong-arity, got {other:?}"),
        }
    }

    #[test]
    fn mset_sets_pairs_clears_ttl_and_rejects_odd_args() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);

        // A pre-existing key WITH a TTL, to prove MSET clears it (default SET semantics).
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", b"k1", b"old", b"EX", b"100"]
            ),
            Value::ok()
        );
        assert!(
            matches!(run_on(&c, &mut s, &mut st, t, &[b"TTL", b"k1"]), Value::Integer(n) if n > 0),
            "k1 has a TTL before MSET"
        );

        // MSET k1 v1 k2 v2 -> +OK; overwrites k1 (clearing its TTL) and creates k2.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"MSET", b"k1", b"v1", b"k2", b"v2"]
            ),
            Value::ok()
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"k1"]),
            Value::BulkString(Some(bytes::Bytes::from_static(b"v1")))
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"k2"]),
            Value::BulkString(Some(bytes::Bytes::from_static(b"v2")))
        );
        // TTL cleared by MSET (-1 = no expire).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"TTL", b"k1"]),
            Value::Integer(-1),
            "MSET must CLEAR the existing TTL (default SET semantics)"
        );

        // Odd arg count (argc-1 odd) -> wrong-arity error.
        match run_on(&c, &mut s, &mut st, t, &[b"MSET", b"a", b"1", b"b"]) {
            Value::Error(e) => {
                assert_eq!(
                    e.line(),
                    "-ERR wrong number of arguments for 'mset' command"
                );
            }
            other => panic!("odd-arg MSET must be wrong-arity, got {other:?}"),
        }
        // Bare MSET (no pair) -> wrong-arity too.
        assert!(matches!(
            run_on(&c, &mut s, &mut st, t, &[b"MSET"]),
            Value::Error(_)
        ));
    }

    #[test]
    fn arity_errors_on_data_commands() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        for cmd in [
            vec![b"GET".as_slice()],
            vec![b"SET", b"k"],
            vec![b"DEL"],
            vec![b"EXISTS"],
            vec![b"TYPE"],
            vec![b"STRLEN"],
            vec![b"SETNX", b"k"],
            vec![b"GETSET", b"k"],
            // PR-2b numeric/append arity.
            vec![b"INCR"],
            vec![b"DECR", b"a", b"b"],
            vec![b"INCRBY", b"k"],
            vec![b"DECRBY", b"k"],
            vec![b"INCRBYFLOAT", b"k"],
            vec![b"APPEND", b"k"],
        ] {
            assert!(
                matches!(run_on(&c, &mut s, &mut st, t, &cmd), Value::Error(_)),
                "expected arity error for {cmd:?}"
            );
        }
    }

    // -- Numeric RMW + APPEND (PR-2b). --

    /// The store-level encoding of `key` in db 0 (for int-encoding assertions). The
    /// command layer only ever sees bytes; the test reaches the store directly to
    /// confirm the result is stored int-encoded, which is the ENCODINGS.md contract.
    fn encoding_of(st: &mut TestStore, key: &[u8]) -> Option<ironcache_storage::Encoding> {
        st.read(0, key, UnixMillis(0)).map(|v| v.encoding())
    }

    fn err_line(v: Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn incr_decr_from_absent_and_existing() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Absent key starts at 0: INCR -> 1.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCR", b"n"]),
            Value::Integer(1)
        );
        // The result is int-encoded.
        assert_eq!(
            encoding_of(&mut st, b"n"),
            Some(ironcache_storage::Encoding::Int)
        );
        // STRLEN reflects the decimal length of the result.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"n"]),
            Value::Integer(1)
        );
        // INCRBY and DECR/DECRBY.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"5"]),
            Value::Integer(6)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DECR", b"n"]),
            Value::Integer(5)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DECRBY", b"n", b"10"]),
            Value::Integer(-5)
        );
        // After several ops the decimal length is 2 ("-5").
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"n"]),
            Value::Integer(2)
        );
        // A negative increment via INCRBY works.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"-5"]),
            Value::Integer(-10)
        );
    }

    #[test]
    fn incr_on_existing_string_set_value() {
        // SET n 10 (stored int-encoded), then INCR/INCRBY/DECR through dispatch.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"10"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCR", b"n"]),
            Value::Integer(11)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"5"]),
            Value::Integer(16)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DECR", b"n"]),
            Value::Integer(15)
        );
    }

    #[test]
    fn incr_non_integer_value_and_arg_are_not_an_integer() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Non-integer EXISTING value (an embstr) -> not-an-integer.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"s", b"hello"]);
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"s"])),
            "-ERR value is not an integer or out of range"
        );
        // A leading-zero / non-canonical existing string is also rejected (string2ll).
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"z", b"007"]);
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"z"])),
            "-ERR value is not an integer or out of range"
        );
        // Non-integer INCREMENT argument -> not-an-integer.
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"1.5"])),
            "-ERR value is not an integer or out of range"
        );
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"abc"])),
            "-ERR value is not an integer or out of range"
        );
    }

    #[test]
    fn incr_overflow_and_decr_underflow_and_decrby_min_edge() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // INCR of i64::MAX overflows.
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", b"max", b"9223372036854775807"],
        );
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"max"])),
            "-ERR increment or decrement would overflow"
        );
        // DECR of i64::MIN underflows.
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", b"min", b"-9223372036854775808"],
        );
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"DECR", b"min"])),
            "-ERR increment or decrement would overflow"
        );
        // DECRBY key i64::MIN: the decrement cannot be negated -> overflow error.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"x", b"0"]);
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"DECRBY", b"x", b"-9223372036854775808"]
            )),
            "-ERR increment or decrement would overflow"
        );
        // The value was not modified by any of the failed ops.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"x"]), bulk(b"0"));
    }

    #[test]
    fn incr_wrongtype_against_non_string() {
        use ironcache_storage::{DataType, Encoding};
        use ironcache_store::kvobj::{Header, KvObj, ValueRepr};
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let mut obj = KvObj::from_bytes(b"lst", b"x", None);
        obj.header = Header {
            data_type: DataType::List,
            encoding: Encoding::ListPack,
            eviction_rank: 0,
            ttl_present: false,
            snapshot_version: 0,
        };
        obj.value = ValueRepr::Inline(Box::from(&b"x"[..]));
        st.insert_object(0, obj);
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"lst"])),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"INCRBYFLOAT", b"lst", b"1"]
            )),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"lst", b"x"])),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    #[test]
    fn incrbyfloat_wrongtype_beats_invalid_increment() {
        // Redis `incrbyfloatCommand` checks the TYPE before parsing the increment
        // argument, so `INCRBYFLOAT <list-key> abc` is WRONGTYPE, NOT
        // "value is not a valid float" (the malformed increment is irrelevant once
        // the key is the wrong type). Plant a non-string via the store seam as the
        // other WRONGTYPE tests do.
        use ironcache_storage::{DataType, Encoding};
        use ironcache_store::kvobj::{Header, KvObj, ValueRepr};
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let mut obj = KvObj::from_bytes(b"lst", b"x", None);
        obj.header = Header {
            data_type: DataType::List,
            encoding: Encoding::ListPack,
            eviction_rank: 0,
            ttl_present: false,
            snapshot_version: 0,
        };
        obj.value = ValueRepr::Inline(Box::from(&b"x"[..]));
        st.insert_object(0, obj);
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"INCRBYFLOAT", b"lst", b"abc"]
            )),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    #[test]
    fn incrbyfloat_absent_format_and_storage() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Absent -> 0 + 10.5 = "10.5" (bulk string).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBYFLOAT", b"f", b"10.5"]),
            bulk(b"10.5")
        );
        // Stored as a STRING (its decimal); GET returns the same bytes.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"f"]),
            bulk(b"10.5")
        );
        // Add 0.1 -> "10.6" (shortest round-trip, no trailing zeros).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBYFLOAT", b"f", b"0.1"]),
            bulk(b"10.6")
        );
    }

    #[test]
    fn incrbyfloat_integer_valued_result_round_trips_to_incr() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // 5.0e3 -> "5000" (integer-valued result, no dot), stored as a string that
        // is int-encoded (since "5000" is a canonical integer), so a later INCR
        // works (matching Redis INCRBYFLOAT -> INCR round-trip for integer results).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBYFLOAT", b"v", b"5.0e3"]),
            bulk(b"5000")
        );
        assert_eq!(
            encoding_of(&mut st, b"v"),
            Some(ironcache_storage::Encoding::Int)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCR", b"v"]),
            Value::Integer(5001)
        );
    }

    #[test]
    fn incrbyfloat_invalid_float_and_nan_inf() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Non-float existing value -> not-a-valid-float.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"s", b"hello"]);
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"INCRBYFLOAT", b"s", b"1.0"]
            )),
            "-ERR value is not a valid float"
        );
        // Non-float increment argument -> not-a-valid-float.
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"INCRBYFLOAT", b"f", b"abc"]
            )),
            "-ERR value is not a valid float"
        );
        // An infinite increment produces an infinite result -> NaN/Inf error.
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"INCRBYFLOAT", b"f", b"inf"]
            )),
            "-ERR increment would produce NaN or Infinity"
        );
        // None of the failed ops created the key.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"f"]), Value::Null);
    }

    #[test]
    fn append_absent_existing_and_binary_safe() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Absent: APPEND creates and returns len(value).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"s", b"abc"]),
            Value::Integer(3)
        );
        // Existing string: appends, returns new len.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"s", b"de"]),
            Value::Integer(5)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"s"]),
            bulk(b"abcde")
        );
        // DIVERGENCE (documented in cmd_append): the frozen waist classifies the
        // rebuilt value by LENGTH, so a SHORT append result is embstr where Redis
        // (which never re-embstrs an appended SDS) would report raw. A result over
        // the embstr threshold is raw, which is the promotion the brief pins; assert
        // that explicitly below.
        assert_eq!(
            encoding_of(&mut st, b"s"),
            Some(ironcache_storage::Encoding::EmbStr)
        );
        // Appending past the embstr threshold promotes the result to raw.
        let big = vec![b'q'; 60];
        run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"s", &big]);
        assert_eq!(
            encoding_of(&mut st, b"s"),
            Some(ironcache_storage::Encoding::Raw)
        );
        // Binary-safe append (NUL bytes preserved).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"b", b"\x00\x01"]),
            Value::Integer(2)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"b", b"\x02"]),
            Value::Integer(3)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"b"]),
            bulk(b"\x00\x01\x02")
        );
    }

    #[test]
    fn append_promotes_int_off_the_int_encoding() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // SET n 10 is int-encoded; APPEND promotes the concatenation OFF int (to a
        // string encoding). The exact string encoding is length-based in the frozen
        // waist (embstr here for the short "10x"; raw past the threshold).
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"10"]);
        assert_eq!(
            encoding_of(&mut st, b"n"),
            Some(ironcache_storage::Encoding::Int)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"n", b"x"]),
            Value::Integer(3)
        );
        // "10x" is not an integer -> a string encoding (no longer int), and GET sees
        // the decimal+suffix.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"n"]),
            bulk(b"10x")
        );
        assert_ne!(
            encoding_of(&mut st, b"n"),
            Some(ironcache_storage::Encoding::Int),
            "APPEND must promote off the int encoding"
        );
    }

    // -- maxmemory admission (PR-3a, ADMISSION.md #128, ADR-0007). --

    /// Run a command against a caller-owned store with the ceiling ON, returning the
    /// reply and the number of keys the admission gate evicted.
    fn run_admit(
        ctx: &ServerContext,
        st: &mut ConnState,
        store: &mut TestStore,
        now: UnixMillis,
        parts: &[&[u8]],
    ) -> (Value, u64) {
        let mut env = TestEnv::new(1);
        let mut wheel = TimingWheel::new();
        let zero = || CounterSnapshot::default();
        let mut deltas = CounterDeltas::default();
        let mut shard_gen = ctx.runtime.generation();
        let reply = dispatch(
            ctx,
            st,
            &mut env,
            store,
            &mut wheel,
            now,
            &mut shard_gen,
            &zero,
            MemoryInfo::default(),
            &mut deltas,
            &req(parts),
        );
        (reply, deltas.evicted)
    }

    /// A context with the ceiling enabled at `per_shard_budget` bytes (single-shard
    /// tests, so maxmemory == per_shard_budget). The ceiling is seeded into the runtime
    /// overlay (the highest-precedence layer), where the admission gate reads it.
    fn ctx_with_budget(per_shard_budget: u64) -> ServerContext {
        ctx_full(None, per_shard_budget, "allkeys-lru")
    }

    fn err_of(v: Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn noeviction_over_budget_rejects_denyoom_write_with_byte_exact_oom() {
        // Strict datastore mode: a denyoom write at/over the budget gets the exact
        // -OOM string, and nothing is written.
        let c = ctx_with_budget(50);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::NoEviction);
        let t = UnixMillis(0);
        let big = vec![b'v'; 100];
        // The first SET: used_memory starts at 0 (< 50), so the gate lets it through;
        // the store is now over budget.
        let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert_eq!(r, Value::ok());
        assert_eq!(ev, 0);
        assert!(st.used_memory() >= 50);
        // A SECOND denyoom write is rejected -OOM (byte-exact), nothing evicted.
        let (r2, ev2) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k2", &big]);
        assert_eq!(
            err_of(r2),
            "-OOM command not allowed when used memory > 'maxmemory'."
        );
        assert_eq!(ev2, 0, "noeviction evicts nothing");
        // k2 was not written.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"k2"]),
            Value::Null
        );
    }

    #[test]
    fn denyoom_write_at_exactly_used_equals_budget_is_served() {
        // Strict-over semantics (Redis getMaxmemoryState: under-limit at
        // `used <= maxmemory`). With used == budget EXACTLY, a denyoom write is served
        // (the gate's `used > budget` is false), NOT OOM'd, even under `noeviction`.
        let mut probe = store_with(16, Policy::NoEviction);
        let t = UnixMillis(0);
        let big = vec![b'v'; 100];
        // Plant one key with no ceiling, then read the resulting footprint and set the
        // budget to EXACTLY that, so used == budget on the next gated write.
        probe.upsert(
            0,
            b"k",
            ironcache_storage::NewValue::Bytes(&big),
            ironcache_storage::ExpireWrite::Clear,
            t,
        );
        let exact = probe.used_memory();
        assert!(exact > 0);

        let c = ctx_with_budget(exact);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::NoEviction);
        // Replay the same plant against the gated store so used == budget exactly.
        let (r0, ev0) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert_eq!(r0, Value::ok());
        assert_eq!(ev0, 0);
        assert_eq!(
            st.used_memory(),
            exact,
            "used must equal the budget exactly"
        );

        // A denyoom write that does NOT grow memory (overwrite same key, same size) at
        // used == budget is SERVED: the gate is strict `>`, so used==budget passes.
        let (r1, ev1) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert_eq!(r1, Value::ok(), "at used==budget the write must be served");
        assert_eq!(ev1, 0);

        // Now push STRICTLY over the budget (a second, larger key with no ceiling
        // would not be gated; instead grow via the gated path: the first overwrite was
        // served and left used==budget, so a NEW key now tips strictly over and the
        // NEXT denyoom write is OOM'd under noeviction).
        let (r2, _ev2) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k2", &big]);
        // The k2 write happened at used==budget (served), pushing used strictly over.
        assert_eq!(r2, Value::ok());
        assert!(st.used_memory() > exact, "used is now strictly over budget");
        // The FOLLOWING denyoom write is rejected -OOM (strictly over, noeviction).
        let (r3, ev3) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k3", &big]);
        assert_eq!(
            err_of(r3),
            "-OOM command not allowed when used memory > 'maxmemory'."
        );
        assert_eq!(ev3, 0);
    }

    #[test]
    fn cache_mode_at_exactly_budget_serves_without_evicting() {
        // Cache mode mirror of the strict-over boundary: at used == budget the gate is
        // not entered, so evict_to_fit does NOT run and nothing is evicted.
        let mut probe = store_with(16, Policy::cache_default());
        let t = UnixMillis(0);
        let big = vec![b'v'; 100];
        probe.upsert(
            0,
            b"k",
            ironcache_storage::NewValue::Bytes(&big),
            ironcache_storage::ExpireWrite::Clear,
            t,
        );
        let exact = probe.used_memory();

        let c = ctx_with_budget(exact);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::cache_default());
        run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert_eq!(st.used_memory(), exact);
        // Overwrite at used==budget: served, and the eviction gate did not fire.
        let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert_eq!(r, Value::ok());
        assert_eq!(ev, 0, "at used==budget cache mode must not evict");
    }

    #[test]
    fn reads_and_del_are_served_over_budget() {
        // Non-denyoom commands are ALWAYS served even over budget (a client must be
        // able to read and free under memory pressure).
        let c = ctx_with_budget(50);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::NoEviction);
        let t = UnixMillis(0);
        let big = vec![b'v'; 100];
        run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert!(st.used_memory() >= 50);
        // GET still works over budget.
        let (got_get, _) = run_admit(&c, &mut s, &mut st, t, &[b"GET", b"k"]);
        assert_eq!(
            got_get,
            Value::BulkString(Some(Bytes::copy_from_slice(&big)))
        );
        // DEL (memory-releasing) still works over budget and frees space.
        let (got_del, _) = run_admit(&c, &mut s, &mut st, t, &[b"DEL", b"k"]);
        assert_eq!(got_del, Value::Integer(1));
        assert!(st.used_memory() < 50, "DEL freed space");
    }

    #[test]
    fn cache_mode_over_budget_evicts_then_the_write_succeeds() {
        // Cache mode: a denyoom write at the budget triggers evict-to-fit; once there
        // is room the write proceeds and the evicted count is reported.
        let c = ctx_with_budget(300);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::cache_default());
        let t = UnixMillis(0);
        let val = vec![b'v'; 100];
        // Write several keys to get over the 300-byte budget.
        for i in 0u32..5 {
            run_admit(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", format!("k{i}").as_bytes(), &val],
            );
        }
        assert!(
            st.used_memory() >= 300,
            "should be over budget after the fills"
        );
        // The next denyoom write evicts to fit, then succeeds.
        let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"new", &val]);
        assert_eq!(r, Value::ok(), "the write should succeed after eviction");
        assert!(ev > 0, "cache mode should have evicted at least one key");
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"new"]),
            Value::BulkString(Some(Bytes::copy_from_slice(&val)))
        );
    }

    #[test]
    fn wtinylfu_eviction_preserves_a_hot_key_under_the_ceiling() {
        // End-to-end W-TinyLFU through the real evict_to_fit flow, demonstrating the
        // ACTUAL #57 mechanism (the candidate-admission door): a hot resident survives
        // under memory pressure NOT because it was GET'd (on_access is now a no-op under
        // #57, so GETs build no frequency), but because each cold SET candidate LOSES the
        // admission door and self-evicts (stored-then-evicted), sparing the hot key.
        // Frequency is built on the DECISION PATH only; here the hot key is warmed via
        // REPEATED SETs (each on_insert is a decision-path bump), not GETs.
        let c = ctx_with_budget(400);
        let mut s = state(&c);
        let mut st = store_with(
            c.databases,
            map_policy_name("allkeys-lfu", 1).expect("allkeys-lfu maps"),
        );
        // Sanity: it is genuinely the W-TinyLFU engine, not a stand-in.
        assert_eq!(st.policy_name(), "allkeys-lfu");
        let t = UnixMillis(0);
        let val = vec![b'v'; 100];

        // Warm the hot key via REPEATED SETs: each SET is a decision-path bump
        // (on_insert min-increments the candidate), so the sketch records a high
        // frequency for "hot". (A GET loop would be INERT here under #57.) These early
        // SETs are under the budget, so no eviction yet.
        for _ in 0..20 {
            run_admit(&c, &mut s, &mut st, t, &[b"SET", b"hot", &val]);
        }
        // Now stream many cold keys, each written once. Each cold SET becomes the pending
        // admission candidate; when the write pushes the shard over budget, evict_to_fit
        // runs the door: the cold candidate (estimate ~1) does NOT strictly beat the hot
        // incumbent, so the COLD candidate self-evicts. The hot key is never the victim.
        let mut total_evicted = 0u64;
        for i in 0u32..15 {
            let (_r, ev) = run_admit(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", format!("cold{i}").as_bytes(), &val],
            );
            total_evicted += ev;
        }
        // The hot key must still be present: it survived because the cold SET candidates
        // lost the admission door, NOT because it was read. This is the #57 door
        // mechanism (the SELECTABLE W-TinyLFU variant's scan resistance).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"hot"]),
            Value::BulkString(Some(Bytes::copy_from_slice(&val))),
            "the hot resident survives: cold SET candidates lose the door and self-evict"
        );
        // Eviction actually happened (the budget is small, so the cold flood forced the
        // door to fire). Every victim was a COLD candidate, never the hot incumbent.
        assert!(
            total_evicted > 0,
            "the cold-candidate flood must have driven W-TinyLFU door evictions"
        );
        // The keyspace stayed small (bounded by the budget): far fewer than the 16 keys
        // written, since rejected cold candidates were continually self-evicted.
        assert!(
            st.len() < 8,
            "W-TinyLFU kept the resident set bounded under the ceiling ({} keys)",
            st.len()
        );
    }

    #[test]
    fn ceiling_off_serves_every_write() {
        // maxmemory == 0 (unlimited): the gate is off; writes always succeed.
        let c = ctx(None);
        assert!(!c.ceiling_enabled());
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let big = vec![b'v'; 10_000];
        for i in 0u32..5 {
            let (r, ev) = run_admit(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", format!("k{i}").as_bytes(), &big],
            );
            assert_eq!(r, Value::ok());
            assert_eq!(ev, 0);
        }
    }

    // -- TTL / EXPIRE family (PR-3b). --

    fn int(v: Value) -> i64 {
        match v {
            Value::Integer(n) => n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    #[test]
    fn expire_sets_ttl_and_ttl_pttl_reflect_it_then_lazy_expires() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        // SET then EXPIRE 10 at t=0 -> deadline 10000ms.
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"SET", b"k", b"v"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"EXPIRE", b"k", b"10"]
            )),
            1
        );
        // TTL ~10s, PTTL ~10000ms.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"TTL", b"k"]
            )),
            10
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"PTTL", b"k"]
            )),
            10_000
        );
        // Alive AT the deadline (Valkey boundary now > deadline).
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(10_000),
                &[b"GET", b"k"]
            ),
            bulk(b"v")
        );
        // Expired one ms past the deadline.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(10_001),
                &[b"GET", b"k"]
            ),
            Value::Null
        );
    }

    #[test]
    fn pexpire_expireat_pexpireat_set_ttl() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"a", b"v"]);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"b", b"v"]);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"d", b"v"]);
        // PEXPIRE a 5000 -> 5000ms.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRE", b"a", b"5000"]
            )),
            1
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PTTL", b"a"]
            )),
            5_000
        );
        // EXPIREAT b 100 (absolute seconds) -> 100000ms.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIREAT", b"b", b"100"]
            )),
            1
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRETIME", b"b"]
            )),
            100_000
        );
        // PEXPIREAT d 250000 (absolute ms).
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIREAT", b"d", b"250000"]
            )),
            1
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRETIME", b"d"]
            )),
            250_000
        );
    }

    #[test]
    fn expire_on_missing_key_replies_zero() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"nope", b"10"]
            )),
            0
        );
    }

    #[test]
    fn expire_past_deadline_deletes_the_key_and_replies_one() {
        // A resolved deadline strictly in the PAST deletes the key and replies 1.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        // now = 100000ms.
        let t = UnixMillis(100_000);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        // EXPIREAT in the past (unix second 1 -> 1000ms, well before now): reply 1,
        // key deleted.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIREAT", b"k", b"1"]
            )),
            1
        );
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]),
            Value::Null
        );
    }

    #[test]
    fn expire_nx_xx_gt_lt_accept_and_reject() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);

        // NX on a key with NO TTL: applies (reply 1). Sets deadline 10000.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"10", b"NX"]
            )),
            1
        );
        // NX again now that a TTL exists: rejected (reply 0).
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"20", b"NX"]
            )),
            0
        );
        // XX with a TTL present: applies (reply 1). Set to 20000.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"20", b"XX"]
            )),
            1
        );
        // GT with a GREATER new expiry (30 > 20): applies.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"30", b"GT"]
            )),
            1
        );
        // GT with a LESSER new expiry (5 < 30): rejected.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"5", b"GT"]
            )),
            0
        );
        // LT with a LESSER new expiry (5 < 30): applies.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"5", b"LT"]
            )),
            1
        );
        // LT with a GREATER new expiry (100 > 5): rejected.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"100", b"LT"]
            )),
            0
        );
    }

    #[test]
    fn expire_gt_lt_treat_no_ttl_as_infinite() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        // A key with NO TTL is treated as +infinity.
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"g", b"v"]);
        // GT against a no-TTL key NEVER applies (nothing is greater than infinity).
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"g", b"10", b"GT"]
            )),
            0
        );
        // Still no TTL.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"g"]
            )),
            -1
        );
        // LT against a no-TTL key ALWAYS applies (anything is less than infinity).
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"l", b"v"]);
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"l", b"10", b"LT"]
            )),
            1
        );
    }

    #[test]
    fn expire_conflicting_options_are_specific_errors() {
        // The three EXPIRE-option conflicts / the unknown token each map to their
        // SPECIFIC Redis message (src/expire.c parseExtendedExpireArgumentsOrReply),
        // NOT the generic syntax error (the #7 fix).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        let cases: &[(&[&[u8]], &str)] = &[
            (
                &[b"EXPIRE", b"k", b"10", b"NX", b"XX"],
                "-ERR NX and XX, GT or LT options at the same time are not compatible",
            ),
            (
                &[b"EXPIRE", b"k", b"10", b"NX", b"GT"],
                "-ERR NX and XX, GT or LT options at the same time are not compatible",
            ),
            (
                &[b"EXPIRE", b"k", b"10", b"NX", b"LT"],
                "-ERR NX and XX, GT or LT options at the same time are not compatible",
            ),
            (
                &[b"EXPIRE", b"k", b"10", b"GT", b"LT"],
                "-ERR GT and LT options at the same time are not compatible",
            ),
            // The unknown-option token is echoed verbatim.
            (
                &[b"EXPIRE", b"k", b"10", b"BOGUS"],
                "-ERR Unsupported option BOGUS",
            ),
        ];
        for (opts, want) in cases {
            match run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, opts) {
                Value::Error(e) => assert_eq!(&e.line(), want, "{opts:?}"),
                other => panic!("expected {want} for {opts:?}, got {other:?}"),
            }
        }
        // GT+XX and LT+XX are LEGAL (no error). With a TTL present XX is satisfied.
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"10"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"20", b"GT", b"XX"]
            )),
            1
        );
    }

    #[test]
    fn expire_lt_xx_independent_gates_drop_xx_on_no_ttl() {
        // The #1 fix: EXPIRE evaluates the existence gate (NX/XX) and the ordering gate
        // (GT/LT) INDEPENDENTLY, and BOTH must pass. `LT XX` on a key with NO current
        // TTL: XX fails (no TTL), so the timeout is NOT set and the reply is 0 even
        // though LT alone (no-TTL = +infinity) would have applied. The old collapsed
        // enum dropped the XX gate and (wrongly) set the TTL.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        // LT XX on a no-TTL key -> reply 0, nothing set.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"10", b"LT", b"XX"]
            )),
            0,
            "LT XX must fail the XX gate on a key with no TTL"
        );
        // TTL is still -1 (no TTL was set).
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            -1,
            "no TTL was set"
        );
        // Now give it a TTL, then LT XX with a SMALLER deadline applies (both gates
        // pass: XX has a TTL, LT is strictly less).
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"100"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"50", b"LT", b"XX"]
            )),
            1,
            "LT XX applies when a TTL exists and the new deadline is smaller"
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            50
        );
    }

    #[test]
    fn ttl_pttl_minus_two_minus_one_conventions() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        // Missing key -> -2.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"missing"]
            )),
            -2
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PTTL", b"missing"]
            )),
            -2
        );
        // Present, no TTL -> -1.
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            -1
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PTTL", b"k"]
            )),
            -1
        );
        // EXPIRETIME/PEXPIRETIME conventions too.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRETIME", b"missing"]
            )),
            -2
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRETIME", b"k"]
            )),
            -1
        );
    }

    #[test]
    fn expiretime_pexpiretime_are_absolute() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        // PEXPIREAT to an absolute ms; EXPIRETIME is that / 1000, PEXPIRETIME is it.
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIREAT", b"k", b"123456"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRETIME", b"k"]
            )),
            123_456
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRETIME", b"k"]
            )),
            123 // (123456 + 500) / 1000 = 123 (ms component < 500 rounds down)
        );
    }

    #[test]
    fn expiretime_rounds_to_nearest_second() {
        // EXPIRETIME rounds the absolute ms deadline to the NEAREST second
        // (`(ms + 500) / 1000`, Redis ttlGenericCommand output_abs), so an ms component
        // >= 500 rounds UP. PEXPIRETIME stays exact ms (the #5 fix).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        // 123556ms: ms component 556 >= 500 -> EXPIRETIME rounds up to 124.
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIREAT", b"k", b"123556"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRETIME", b"k"]
            )),
            124,
            "(123556 + 500) / 1000 = 124"
        );
        // PEXPIRETIME is the exact ms, unrounded.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRETIME", b"k"]
            )),
            123_556
        );
    }

    #[test]
    fn expire_deadline_equal_to_now_deletes_immediately() {
        // The #6 command-time boundary: a resolved deadline EQUAL to now is treated as
        // already past (Redis checkAlreadyExpired, `when <= now`), so PEXPIREAT k <now>
        // replies 1 and the key is gone the same tick. This is DISTINCT from the store's
        // lazy-read backstop (`now > deadline`, alive at now==deadline), which governs a
        // SET deadline reached later.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let now = UnixMillis(100_000);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, now, &[b"SET", b"k", b"v"]);
        // PEXPIREAT to exactly `now` -> reply 1, key deleted immediately.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                now,
                &[b"PEXPIREAT", b"k", b"100000"]
            )),
            1,
            "deadline == now deletes and replies 1 (checkAlreadyExpired <= now)"
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                now,
                &[b"EXISTS", b"k"]
            )),
            0,
            "key is gone same-tick"
        );
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, now, &[b"GET", b"k"]),
            Value::Null
        );
    }

    #[test]
    fn getex_exat_in_the_past_returns_value_then_deletes() {
        // The #6 boundary for GETEX: an ABSOLUTE EXAT/PXAT deadline at or before now
        // returns the value AND deletes the key (Redis checkAlreadyExpired). A past
        // RELATIVE EX/PX is still the invalid-expire error, not this path.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let now = UnixMillis(100_000);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, now, &[b"SET", b"k", b"v"]);
        // PXAT exactly at now (100000ms): value returned, key deleted.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                now,
                &[b"GETEX", b"k", b"PXAT", b"100000"]
            ),
            bulk(b"v"),
            "GETEX returns the value even when the absolute deadline is past"
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                now,
                &[b"EXISTS", b"k"]
            )),
            0,
            "the key is deleted after the read (past absolute deadline)"
        );
    }

    #[test]
    fn persist_removes_ttl_and_stops_expiring() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"SET", b"k", b"v", b"EX", b"10"],
        );
        // PERSIST removes the TTL -> reply 1.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PERSIST", b"k"]
            )),
            1
        );
        // TTL now -1 (no TTL).
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            -1
        );
        // PERSIST again (no TTL) -> reply 0.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PERSIST", b"k"]
            )),
            0
        );
        // PERSIST on a missing key -> 0.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PERSIST", b"gone"]
            )),
            0
        );
        // The key no longer expires at the old deadline.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(20_000),
                &[b"GET", b"k"]
            ),
            bulk(b"v")
        );
    }

    #[test]
    fn getex_matrix() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"SET", b"k", b"v", b"EX", b"100"],
        );
        // Bare GETEX returns the value and does NOT change the TTL.
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"k"]),
            bulk(b"v")
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            100
        );
        // GETEX EX 5 sets a new TTL.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"GETEX", b"k", b"EX", b"5"]
            ),
            bulk(b"v")
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            5
        );
        // GETEX PERSIST clears the TTL.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"GETEX", b"k", b"PERSIST"]
            ),
            bulk(b"v")
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            -1
        );
        // GETEX on an absent key -> nil.
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"absent"]),
            Value::Null
        );
        // GETEX PXAT (absolute ms).
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"GETEX", b"k", b"PXAT", b"50000"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRETIME", b"k"]
            )),
            50_000
        );
    }

    #[test]
    fn getex_wrongtype_and_invalid_expire() {
        use ironcache_storage::{DataType, Encoding};
        use ironcache_store::kvobj::{Header, KvObj, ValueRepr};
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        // GETEX against a non-string -> WRONGTYPE.
        let mut obj = KvObj::from_bytes(b"lst", b"x", None);
        obj.header = Header {
            data_type: DataType::List,
            encoding: Encoding::ListPack,
            eviction_rank: 0,
            ttl_present: false,
            snapshot_version: 0,
        };
        obj.value = ValueRepr::Inline(Box::from(&b"x"[..]));
        st.insert_object(0, obj);
        match run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"lst"]) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-WRONGTYPE Operation against a key holding the wrong kind of value"
            ),
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
        // GETEX with an invalid (<= 0) expire -> invalid expire time in 'getex'.
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        match run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"GETEX", b"k", b"EX", b"0"],
        ) {
            Value::Error(e) => {
                assert_eq!(e.line(), "-ERR invalid expire time in 'getex' command");
            }
            other => panic!("expected invalid expire time, got {other:?}"),
        }
        // GETEX with conflicting options -> syntax error.
        match run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"GETEX", b"k", b"EX", b"5", b"PERSIST"],
        ) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR syntax error"),
            other => panic!("expected syntax error, got {other:?}"),
        }
    }

    #[test]
    fn setex_psetex_set_value_and_ttl() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        // SETEX k 10 v -> +OK, value set, TTL 10s.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"SETEX", b"k", b"10", b"v"]
            ),
            Value::ok()
        );
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]),
            bulk(b"v")
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            10
        );
        // PSETEX p 5000 v -> TTL 5000ms.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PSETEX", b"p", b"5000", b"v"]
            ),
            Value::ok()
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PTTL", b"p"]
            )),
            5_000
        );
    }

    #[test]
    fn setex_psetex_non_positive_is_invalid_expire_and_writes_nothing() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        match run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"SETEX", b"k", b"0", b"v"],
        ) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR invalid expire time in 'setex' command"),
            other => panic!("expected invalid expire time, got {other:?}"),
        }
        match run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PSETEX", b"k", b"-1", b"v"],
        ) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR invalid expire time in 'psetex' command"),
            other => panic!("expected invalid expire time, got {other:?}"),
        }
        // Nothing was written.
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]),
            Value::Null
        );
    }

    #[test]
    fn expire_family_arity_errors() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        for cmd in [
            vec![b"EXPIRE".as_slice(), b"k"],
            vec![b"PEXPIRE", b"k"],
            vec![b"TTL"],
            vec![b"PTTL", b"a", b"b"],
            vec![b"PERSIST"],
            vec![b"EXPIRETIME"],
            vec![b"GETEX"],
            vec![b"SETEX", b"k", b"10"],
            vec![b"PSETEX", b"k", b"10"],
        ] {
            assert!(
                matches!(
                    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &cmd),
                    Value::Error(_)
                ),
                "expected arity error for {cmd:?}"
            );
        }
    }

    // -- Active drain + counters (PR-3b). --

    #[test]
    fn active_drain_reclaims_expired_keys_and_bumps_expired_counter() {
        // Set short TTLs, advance now via the dispatch `now`, then issue a command:
        // the active drain pops the due keys from the wheel and reaps them, bumping the
        // expired delta.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        // Establish the wheel origin at t=0 (the first advance only sets the base).
        let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
        // Three keys each with a 1s TTL (deadline 1000ms), registered in the wheel.
        for k in [b"a".as_slice(), b"b", b"c"] {
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"SET", k, b"v", b"EX", b"1"],
            );
        }
        assert_eq!(st.len(), 3);
        // Advance well past the deadline and issue a command: the active drain reaps
        // all three before the command body. The drain count is in the expired delta.
        let (_r, deltas) = run_on_wheel_deltas(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(5_000),
            &[b"PING"],
        );
        assert_eq!(
            deltas.expired, 3,
            "active drain reaped the three expired keys"
        );
        // The store no longer holds them (the drain deleted them, not just the lazy
        // backstop on a read).
        assert_eq!(
            st.len(),
            0,
            "expired keys are resident-evicted by the drain"
        );
    }

    #[test]
    fn active_drain_skips_re_ttld_key_via_store_recheck() {
        // A stale wheel entry (a key whose TTL was extended) must NOT be reaped early:
        // the store re-checks the real expire_at, so the drain skips it.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
        // SET with a 1s TTL (deadline 1000), then EXTEND to 100s (deadline 100000).
        // The wheel still holds the OLD 1000ms registration (a stale entry).
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"SET", b"k", b"v", b"EX", b"1"],
        );
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"EXPIRE", b"k", b"100"],
        );
        // Advance past the OLD deadline (2000ms) but not the new one: the drain offers
        // the stale entry, but the store re-check finds the key NOT expired and skips.
        let (_r, deltas) = run_on_wheel_deltas(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(2_000),
            &[b"PING"],
        );
        assert_eq!(
            deltas.expired, 0,
            "stale wheel entry must not reap a re-TTL'd key"
        );
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(2_000),
                &[b"GET", b"k"]
            ),
            bulk(b"v"),
            "the re-TTL'd key is still alive"
        );
    }

    #[test]
    fn drain_due_keys_helper_reaps_bounded_batch_deterministically() {
        // The SHARED bounded-drain helper (PR-3c) both the opportunistic per-command
        // path and the background timer task call. Drive it directly: register keys with
        // deadlines, advance the TestEnv-equivalent `now` past them, and assert it reaps
        // exactly the due keys, bumps the count, and respects the `max` bound.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        // Establish the wheel origin at t=0.
        let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
        // 5 keys each with a 1s TTL (deadline 1000ms), registered in the wheel via SET EX.
        for k in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"SET", k, b"v", b"EX", b"1"],
            );
        }
        assert_eq!(st.len(), 5);
        // Drain with a small bound (max=2): the helper reaps at most 2 per call.
        let now = UnixMillis(5_000);
        let first = drain_due_keys(&mut wheel, &mut st, now, 2);
        assert!(first <= 2, "the helper respects the max bound");
        // Keep draining until nothing more is due; total reaped is exactly the 5 keys.
        let mut total = first;
        loop {
            let n = drain_due_keys(&mut wheel, &mut st, now, 2);
            if n == 0 {
                break;
            }
            assert!(n <= 2, "every call respects the max bound");
            total += n;
        }
        assert_eq!(total, 5, "the helper reaps exactly the due keys");
        assert_eq!(st.len(), 0, "all expired keys are resident-evicted");

        // Determinism (ADR-0003): a fresh replay against the same registrations + the
        // same `now` reaps the identical count (the helper reads time only via `now`).
        let mut st2 = test_store(c.databases);
        let mut wheel2 = TimingWheel::new();
        let mut s2 = state(&c);
        let _ = run_on_wheel_deltas(
            &c,
            &mut s2,
            &mut st2,
            &mut wheel2,
            UnixMillis(0),
            &[b"PING"],
        );
        for k in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            run_on_wheel(
                &c,
                &mut s2,
                &mut st2,
                &mut wheel2,
                UnixMillis(0),
                &[b"SET", k, b"v", b"EX", b"1"],
            );
        }
        let replay = drain_due_keys(&mut wheel2, &mut st2, now, 100);
        assert_eq!(
            replay, 5,
            "same now + same registrations => same reclamation"
        );
    }

    #[test]
    fn drain_due_keys_helper_skips_stale_re_ttld_entry() {
        // The helper reaps ONLY genuinely-expired keys: a re-TTL'd key whose stale wheel
        // entry is offered is re-checked by the store and skipped (no false reap).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"SET", b"k", b"v", b"EX", b"1"],
        );
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"EXPIRE", b"k", b"100"],
        );
        // Past the OLD deadline (2000ms) but not the new one: the stale entry is offered,
        // the store re-check finds it live, the helper reaps nothing.
        let reaped = drain_due_keys(&mut wheel, &mut st, UnixMillis(2_000), 100);
        assert_eq!(reaped, 0, "stale wheel entry must not reap a re-TTL'd key");
        assert_eq!(st.len(), 1, "the re-TTL'd key survives");
    }

    #[test]
    fn keyspace_hits_and_misses_are_counted_for_reads() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        // GET hit.
        let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]);
        assert_eq!((d.keyspace_hits, d.keyspace_misses), (1, 0));
        // GET miss.
        let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"absent"]);
        assert_eq!((d.keyspace_hits, d.keyspace_misses), (0, 1));
        // GETEX is also counted (a real keyspace lookup): a hit on a present key.
        let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"k"]);
        assert_eq!((d.keyspace_hits, d.keyspace_misses), (1, 0));
        let (_r, d) =
            run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"absent"]);
        assert_eq!((d.keyspace_hits, d.keyspace_misses), (0, 1));
    }

    #[test]
    fn ttl_family_does_not_count_keyspace_hits_or_misses() {
        // TTL-family introspection (TTL/PTTL/EXPIRETIME/PEXPIRETIME) uses LOOKUP_NOTOUCH
        // and must NOT bump keyspace_hits/keyspace_misses (the #8 fix), unlike GET/GETEX.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        for cmd in [
            vec![b"TTL".as_slice(), b"k"],
            vec![b"TTL", b"absent"],
            vec![b"PTTL", b"k"],
            vec![b"PTTL", b"absent"],
            vec![b"EXPIRETIME", b"k"],
            vec![b"EXPIRETIME", b"absent"],
            vec![b"PEXPIRETIME", b"k"],
            vec![b"PEXPIRETIME", b"absent"],
        ] {
            let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &cmd);
            assert_eq!(
                (d.keyspace_hits, d.keyspace_misses),
                (0, 0),
                "{cmd:?} must not count keyspace hits/misses (LOOKUP_NOTOUCH)"
            );
        }
    }

    #[test]
    fn determinism_replay_drives_identical_expiry_sets() {
        // The same command + now sequence replays the identical expiry outcome (the
        // determinism contract, ADR-0003: the wheel + store read time only via `now`).
        let run = || -> (usize, u64) {
            let c = ctx(None);
            let mut s = state(&c);
            let mut st = test_store(c.databases);
            let mut wheel = TimingWheel::new();
            let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
            for i in 0..10u32 {
                run_on_wheel(
                    &c,
                    &mut s,
                    &mut st,
                    &mut wheel,
                    UnixMillis(0),
                    &[b"SET", format!("k{i}").as_bytes(), b"v", b"PX", b"500"],
                );
            }
            let mut total_expired = 0u64;
            for step in [200u64, 600, 1_000, 5_000] {
                let (_r, d) = run_on_wheel_deltas(
                    &c,
                    &mut s,
                    &mut st,
                    &mut wheel,
                    UnixMillis(step),
                    &[b"PING"],
                );
                total_expired += d.expired;
            }
            (st.len(), total_expired)
        };
        let a = run();
        let b = run();
        assert_eq!(a, b, "identical now sequence => identical expiry outcome");
        // All ten keys expired (deadline 500ms, drained by step 600+).
        assert_eq!(a.0, 0);
        assert_eq!(a.1, 10);
    }

    // -- Generic keyspace + introspection commands (PR-4a) through dispatch. --

    /// A test store wired with an LFU policy (for OBJECT FREQ/IDLETIME gating tests).
    fn lfu_store(databases: u32) -> TestStore {
        let policy = map_policy_name("allkeys-lfu", 1).expect("allkeys-lfu maps");
        ShardStore::with_hooks(databases, policy, CountingAccounting::new())
    }

    /// Extract a Bulk string's bytes (panics on any other reply shape).
    fn bulk_bytes(v: &Value) -> Vec<u8> {
        match v {
            Value::BulkString(Some(b)) => b.to_vec(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    #[test]
    fn keys_matches_glob_and_equals_full_scan() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        for k in [b"user:1".as_slice(), b"user:2", b"post:1", b"misc"] {
            run_on(&c, &mut s, &mut st, t, &[b"SET", k, b"v"]);
        }
        // KEYS user:* -> the two user keys (order-independent compare).
        let v = run_on(&c, &mut s, &mut st, t, &[b"KEYS", b"user:*"]);
        let mut got: Vec<Vec<u8>> = match v {
            Value::Array(Some(items)) => items.iter().map(bulk_bytes).collect(),
            other => panic!("expected array, got {other:?}"),
        };
        got.sort();
        assert_eq!(got, vec![b"user:1".to_vec(), b"user:2".to_vec()]);
        // KEYS * -> all four.
        let all = run_on(&c, &mut s, &mut st, t, &[b"KEYS", b"*"]);
        match all {
            Value::Array(Some(items)) => assert_eq!(items.len(), 4),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn scan_to_completion_collects_all_keys() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        for i in 0..40 {
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", format!("k{i}").as_bytes(), b"v"],
            );
        }
        // Loop SCAN with a small COUNT to completion, collecting every key.
        let mut collected: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut cursor = b"0".to_vec();
        loop {
            let v = run_on(&c, &mut s, &mut st, t, &[b"SCAN", &cursor, b"COUNT", b"3"]);
            let items = match v {
                Value::Array(Some(items)) => items,
                other => panic!("SCAN reply must be a 2-array, got {other:?}"),
            };
            assert_eq!(items.len(), 2, "[next_cursor, [keys]]");
            let next = bulk_bytes(&items[0]);
            if let Value::Array(Some(keys)) = &items[1] {
                for k in keys {
                    collected.insert(bulk_bytes(k));
                }
            } else {
                panic!("SCAN keys element must be an array");
            }
            if next == b"0" {
                break;
            }
            cursor = next;
        }
        assert_eq!(
            collected.len(),
            40,
            "SCAN to completion collected every key"
        );
    }

    #[test]
    fn scan_match_and_type_filters() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        for i in 0..10 {
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", format!("s{i}").as_bytes(), b"v"],
            );
        }
        // SCAN 0 MATCH s1* -> just s1 (s1 only; s10..s19 do not exist).
        let v = run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SCAN", b"0", b"MATCH", b"s1", b"COUNT", b"100"],
        );
        if let Value::Array(Some(items)) = v {
            if let Value::Array(Some(keys)) = &items[1] {
                assert_eq!(keys.len(), 1);
                assert_eq!(bulk_bytes(&keys[0]), b"s1");
            }
        }
        // SCAN 0 TYPE list -> nothing (all are strings).
        let v = run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SCAN", b"0", b"TYPE", b"list", b"COUNT", b"100"],
        );
        if let Value::Array(Some(items)) = v {
            if let Value::Array(Some(keys)) = &items[1] {
                assert!(keys.is_empty(), "no list-typed keys");
            }
        }
    }

    #[test]
    fn scan_invalid_cursor_errors() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        match run_on(
            &c,
            &mut s,
            &mut st,
            UnixMillis(0),
            &[b"SCAN", b"notanumber"],
        ) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR invalid cursor"),
            other => panic!("expected invalid cursor, got {other:?}"),
        }
    }

    #[test]
    fn dbsize_counts_keys() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DBSIZE"]),
            Value::Integer(0)
        );
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DBSIZE"]),
            Value::Integer(2)
        );
    }

    // The short test-fixture names (c/s/st/t plus the a/b reply bindings) are the
    // established convention across this test module; the lint trips only because this
    // case names a couple of reply temporaries too.
    #[allow(clippy::many_single_char_names)]
    #[test]
    fn randomkey_member_nil_and_deterministic_under_seeded_env() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Empty DB -> nil.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"RANDOMKEY"]), Value::Null);
        for i in 0..10 {
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", format!("k{i}").as_bytes(), b"v"],
            );
        }
        // The reply is a live member.
        let v = run_on(&c, &mut s, &mut st, t, &[b"RANDOMKEY"]);
        let key = bulk_bytes(&v);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXISTS", &key]),
            Value::Integer(1),
            "RANDOMKEY returned a live member"
        );
        // Deterministic under the seeded TestEnv: `run_on` builds a fresh TestEnv(seed=1)
        // each call, so the first RNG draw (the pick) is identical, yielding the same key.
        let a = run_on(&c, &mut s, &mut st, t, &[b"RANDOMKEY"]);
        let b = run_on(&c, &mut s, &mut st, t, &[b"RANDOMKEY"]);
        assert_eq!(a, b, "RANDOMKEY deterministic under a seeded env");
    }

    #[test]
    fn set_spop_srandmember_sscan_are_deterministic_through_the_env_seam() {
        // ADR-0003: SPOP/SRANDMEMBER draw their seed from the Env RNG via dispatch (the
        // caller-draws seam); SSCAN reads no RNG. `run_on` builds a fresh TestEnv(seed=1)
        // each call, so the first RNG draw (the SPOP/SRANDMEMBER seed) is identical across
        // calls, yielding the same selection. This pins that the randomness enters through
        // the seam (the store/handler read no RNG) and is deterministic for a fixed seed.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SADD", b"k", b"a", b"b", b"c", b"d", b"e"],
        );
        // SRANDMEMBER (no removal): two calls with the same fresh-seed env match.
        let rand_a = run_on(&c, &mut s, &mut st, t, &[b"SRANDMEMBER", b"k", b"3"]);
        let rand_b = run_on(&c, &mut s, &mut st, t, &[b"SRANDMEMBER", b"k", b"3"]);
        assert_eq!(
            rand_a, rand_b,
            "SRANDMEMBER deterministic under the seeded env"
        );

        // SSCAN reads no RNG: identical across calls (cursor 0, small set -> all at once).
        let scan_a = run_on(&c, &mut s, &mut st, t, &[b"SSCAN", b"k", b"0"]);
        let scan_b = run_on(&c, &mut s, &mut st, t, &[b"SSCAN", b"k", b"0"]);
        assert_eq!(scan_a, scan_b, "SSCAN deterministic (reads no RNG)");

        // SPOP on two FRESH identical stores with the same seeded env pops the SAME member.
        let mut st1 = test_store(c.databases);
        let mut st2 = test_store(c.databases);
        for store in [&mut st1, &mut st2] {
            run_on(
                &c,
                &mut s,
                store,
                t,
                &[b"SADD", b"k", b"a", b"b", b"c", b"d"],
            );
        }
        let p1 = run_on(&c, &mut s, &mut st1, t, &[b"SPOP", b"k"]);
        let p2 = run_on(&c, &mut s, &mut st2, t, &[b"SPOP", b"k"]);
        assert_eq!(p1, p2, "SPOP deterministic under the seeded env");
    }

    #[test]
    fn rename_preserves_value_and_renamenx_copy_semantics() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"src", b"hello"]);
        // RENAME -> +OK, src gone, dst holds the value.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RENAME", b"src", b"dst"]),
            Value::ok()
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"dst"]),
            bulk(b"hello")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"src"]),
            Value::Null
        );
        // RENAME of a missing key -> no such key.
        match run_on(&c, &mut s, &mut st, t, &[b"RENAME", b"gone", b"x"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR no such key"),
            other => panic!("expected no such key, got {other:?}"),
        }
        // RENAMENX: dst exists -> 0; dst free -> 1.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RENAMENX", b"a", b"b"]),
            Value::Integer(0)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RENAMENX", b"a", b"c"]),
            Value::Integer(1)
        );
        // COPY with REPLACE overwrites; without REPLACE onto an existing dst -> 0.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"from", b"X"]);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"to", b"Y"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"COPY", b"from", b"to"]),
            Value::Integer(0),
            "COPY declines without REPLACE when dst exists"
        );
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"COPY", b"from", b"to", b"REPLACE"]
            ),
            Value::Integer(1)
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"to"]), bulk(b"X"));
    }

    #[test]
    fn move_across_dbs_and_noop_when_dest_occupied() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // The connection is on db 0 (default).
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v"]);
        // MOVE k 1 -> 1; gone from db 0.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"MOVE", b"k", b"1"]),
            Value::Integer(1)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"k"]),
            Value::Integer(0)
        );
        // A fresh k in db 0; MOVE to db 1 where k already exists -> 0 (no-op).
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v2"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"MOVE", b"k", b"1"]),
            Value::Integer(0)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"k"]),
            Value::Integer(1)
        );
        // MOVE to the same db is an error.
        assert!(matches!(
            run_on(&c, &mut s, &mut st, t, &[b"MOVE", b"k", b"0"]),
            Value::Error(_)
        ));
    }

    #[test]
    fn swapdb_swaps_contents() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Put a in db 0, b in db 1 (via MOVE), then SWAPDB 0 1.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"in0"]);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"in0too"]);
        run_on(&c, &mut s, &mut st, t, &[b"MOVE", b"b", b"1"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SWAPDB", b"0", b"1"]),
            Value::ok()
        );
        // After swap, db 0 holds what was db 1 (b), and a is now in db 1.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"a"]),
            Value::Integer(0)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"b"]),
            Value::Integer(1)
        );
    }

    #[test]
    fn touch_and_unlink_counts() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
        // TOUCH counts live keys (repeats counted, like EXISTS).
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"TOUCH", b"a", b"a", b"b", b"missing"]
            ),
            Value::Integer(3)
        );
        // UNLINK removes live keys, returns the count (== DEL today).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"UNLINK", b"a", b"b", b"missing"]),
            Value::Integer(2)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"a", b"b"]),
            Value::Integer(0)
        );
    }

    #[test]
    fn flushdb_and_flushall_empty_scope() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
        run_on(&c, &mut s, &mut st, t, &[b"MOVE", b"b", b"1"]);
        // FLUSHDB (with the SYNC option accepted) empties only db 0.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"FLUSHDB", b"SYNC"]),
            Value::ok()
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DBSIZE"]),
            Value::Integer(0)
        );
        // FLUSHALL ASYNC empties everything.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"FLUSHALL", b"ASYNC"]),
            Value::ok()
        );
        // An unknown flush option is a syntax error.
        assert!(matches!(
            run_on(&c, &mut s, &mut st, t, &[b"FLUSHDB", b"BOGUS"]),
            Value::Error(_)
        ));
    }

    #[test]
    fn object_encoding_int_embstr_raw() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // int
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"12345"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"n"]),
            bulk(b"int")
        );
        // embstr (short string)
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"e", b"hello"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"e"]),
            bulk(b"embstr")
        );
        // raw (long string > 44 bytes)
        let big = vec![b'z'; 100];
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"r", &big]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"r"]),
            bulk(b"raw")
        );
        // Missing key -> null (Redis replies the null bulk, not an error).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"nope"]),
            Value::Null
        );
    }

    #[test]
    fn object_encoding_append_stays_short_is_a_known_divergence() {
        // KNOWN DIVERGENCE (ADR-0009, recorded for the conformance suite): an APPEND
        // whose result stays SHORT reports `embstr`/`int` here where REDIS reports
        // `raw` (Redis converts any APPENDed string to raw unconditionally). IronCache's
        // APPEND rebuilds-and-reclassifies through the rmw waist, so a short result
        // reclassifies. The fix needs the deferred in-place-mutation waist extension; it
        // is NOT fixed here. This test asserts the CURRENT (divergent) behavior so the
        // conformance suite tracks it.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // APPEND to a fresh key with a short value -> Redis would report `raw`; we report
        // `embstr` (the documented divergence).
        run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"a", b"abc"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"a"]),
            bulk(b"embstr"),
            "KNOWN DIVERGENCE: APPEND-stays-short reports embstr here, raw in Redis"
        );
        // An APPEND producing a pure-integer string reports `int` here (Redis: raw).
        run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"b", b"42"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"b"]),
            bulk(b"int"),
            "KNOWN DIVERGENCE: APPEND of digits reports int here, raw in Redis"
        );
    }

    #[test]
    fn object_refcount_shared_int_vs_one() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // A shared small int (0..=9999) reports OBJ_SHARED_REFCOUNT = 2147483647.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"small", b"100"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"REFCOUNT", b"small"]),
            Value::Integer(2_147_483_647)
        );
        // A large int (>= 10000) is not shared -> 1.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"big", b"100000"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"REFCOUNT", b"big"]),
            Value::Integer(1)
        );
        // A non-int string -> 1.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"str", b"hello"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"REFCOUNT", b"str"]),
            Value::Integer(1)
        );
    }

    #[test]
    fn object_idletime_zero_under_non_lfu_and_errors_under_lfu() {
        let c = ctx(None);
        let mut s = state(&c);
        // Non-LFU (default cache policy): IDLETIME is 0.
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"IDLETIME", b"k"]),
            Value::Integer(0)
        );
        // LFU policy: IDLETIME errors (idle time not tracked under LFU).
        let mut lfu = lfu_store(c.databases);
        run_on(&c, &mut s, &mut lfu, t, &[b"SET", b"k", b"v"]);
        match run_on(&c, &mut s, &mut lfu, t, &[b"OBJECT", b"IDLETIME", b"k"]) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-ERR An LFU maxmemory policy is selected, idle time not tracked. \
                 Please note that when switching between policies at runtime LRU and \
                 LFU data will take some time to adjust."
            ),
            other => panic!("expected LFU idletime error, got {other:?}"),
        }
    }

    #[test]
    fn object_freq_under_lfu_and_errors_under_non_lfu() {
        let c = ctx(None);
        let mut s = state(&c);
        let t = UnixMillis(0);
        // Non-LFU: FREQ errors (requires an LFU policy).
        let mut st = test_store(c.databases);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v"]);
        match run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"FREQ", b"k"]) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-ERR An LFU maxmemory policy is not selected, access frequency not \
                 tracked. Please note that when switching between policies at runtime \
                 LRU and LFU data will take some time to adjust."
            ),
            other => panic!("expected LFU freq error, got {other:?}"),
        }
        // LFU: FREQ returns an integer estimate (>= 0).
        let mut lfu = lfu_store(c.databases);
        run_on(&c, &mut s, &mut lfu, t, &[b"SET", b"k", b"v"]);
        // Access it a few times so the sketch estimate is non-trivial.
        for _ in 0..5 {
            run_on(&c, &mut s, &mut lfu, t, &[b"GET", b"k"]);
        }
        match run_on(&c, &mut s, &mut lfu, t, &[b"OBJECT", b"FREQ", b"k"]) {
            Value::Integer(n) => assert!((0..=15).contains(&n), "FREQ estimate in 0..=15, got {n}"),
            other => panic!("expected integer freq, got {other:?}"),
        }
        // FREQ of a missing key (under LFU) -> null.
        assert_eq!(
            run_on(&c, &mut s, &mut lfu, t, &[b"OBJECT", b"FREQ", b"absent"]),
            Value::Null
        );
    }

    #[test]
    fn object_help_and_unknown_subcommand() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // HELP -> a non-empty array of bulk strings.
        match run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"HELP"]) {
            Value::Array(Some(items)) => assert!(!items.is_empty()),
            other => panic!("expected help array, got {other:?}"),
        }
        // An unknown subcommand errors.
        assert!(matches!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"BOGUS", b"k"]),
            Value::Error(_)
        ));
    }

    #[test]
    fn keyspace_command_arity_errors() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        for cmd in [
            vec![b"KEYS".as_slice()],
            vec![b"SCAN"],
            vec![b"RENAME", b"a"],
            vec![b"RENAMENX", b"a"],
            vec![b"MOVE", b"a"],
            vec![b"SWAPDB", b"0"],
            vec![b"TOUCH"],
            vec![b"UNLINK"],
            vec![b"OBJECT"],
        ] {
            assert!(
                matches!(run_on(&c, &mut s, &mut st, t, &cmd), Value::Error(_)),
                "expected arity error for {cmd:?}"
            );
        }
    }

    // -- CONFIG maxmemory-policy hot-swap through dispatch (PR-4b). --

    /// Drive ONE command through dispatch against a caller-owned store + per-shard
    /// generation, with a seeded [`TestEnv`] (so the swap seed is deterministic), and
    /// return the reply.
    fn run_swap(
        ctx: &ServerContext,
        st: &mut ConnState,
        store: &mut TestStore,
        shard_gen: &mut u64,
        seed: u64,
        parts: &[&[u8]],
    ) -> Value {
        let mut env = TestEnv::new(seed);
        let mut wheel = TimingWheel::new();
        let zero = || CounterSnapshot::default();
        let mut deltas = CounterDeltas::default();
        dispatch(
            ctx,
            st,
            &mut env,
            store,
            &mut wheel,
            UnixMillis(0),
            shard_gen,
            &zero,
            MemoryInfo::default(),
            &mut deltas,
            &req(parts),
        )
    }

    #[test]
    fn dispatch_hot_swaps_policy_on_generation_change() {
        // A CONFIG SET maxmemory-policy bumps the shared generation; the NEXT command on
        // a shard whose last-seen generation is behind rebuilds that shard's policy from
        // the new name (the per-command atomic load + compare at the top of dispatch).
        let c = ctx_full(None, 0, "allkeys-lru");
        let mut s = state(&c);
        let mut st = store_with(c.databases, map_policy_name("allkeys-lru", 1).unwrap());
        let mut shard_gen = c.runtime.generation();

        // A no-op command does not swap (generation unchanged).
        let _ = run_swap(&c, &mut s, &mut st, &mut shard_gen, 1, &[b"PING"]);
        assert_eq!(st.policy_name(), "allkeys-lru");

        // CONFIG SET maxmemory-policy allkeys-lfu (bumps the shared generation).
        let _ = run_swap(
            &c,
            &mut s,
            &mut st,
            &mut shard_gen,
            1,
            &[b"CONFIG", b"SET", b"maxmemory-policy", b"allkeys-lfu"],
        );
        // The swap happens at the TOP of the NEXT dispatch (the CONFIG SET command that
        // bumped the generation observed the OLD generation at its own top). Issue
        // another command: now the store has swapped.
        let _ = run_swap(&c, &mut s, &mut st, &mut shard_gen, 1, &[b"PING"]);
        assert_eq!(
            st.policy_name(),
            "allkeys-lfu",
            "store swapped to the new policy"
        );
        assert_eq!(
            shard_gen,
            c.runtime.generation(),
            "shard caught up to the gen"
        );
    }

    #[test]
    fn dispatch_swap_seed_is_deterministic() {
        // Two identical seeded runs that swap to a *-random policy through dispatch
        // produce the same victim ordering (ADR-0003: the swap seeds the RNG through the
        // Env seam, so a fixed seed is reproducible; the shared atomic reads add no
        // nondeterminism for a fixed command sequence).
        fn build_and_swap(seed: u64) -> TestStore {
            let c = ctx_full(None, 0, "allkeys-lru");
            let mut s = state(&c);
            let mut st = store_with(c.databases, map_policy_name("allkeys-lru", 1).unwrap());
            let mut shard_gen = c.runtime.generation();
            // Plant keys.
            for i in 0..8u8 {
                let key = [b'k', i];
                let _ = run_swap(
                    &c,
                    &mut s,
                    &mut st,
                    &mut shard_gen,
                    seed,
                    &[b"SET", &key, b"v"],
                );
            }
            // Swap to allkeys-random; the swap draws its seed from the seeded TestEnv on
            // the FIRST command after the generation bump.
            let _ = run_swap(
                &c,
                &mut s,
                &mut st,
                &mut shard_gen,
                seed,
                &[b"CONFIG", b"SET", b"maxmemory-policy", b"allkeys-random"],
            );
            // The next command triggers the swap (and re-tracks the keys via reads).
            for i in 0..8u8 {
                let key = [b'k', i];
                let _ = run_swap(&c, &mut s, &mut st, &mut shard_gen, seed, &[b"GET", &key]);
            }
            assert_eq!(st.policy_name(), "allkeys-random");
            st
        }
        // The swap-seed determinism is anchored by the FIRST command after the gen bump:
        // both runs draw the SAME seed value from a TestEnv seeded the same way, because
        // that command's env is `TestEnv::new(seed)` and the RNG draw for the swap is the
        // first draw on that fresh env. So both stores swap to a Random policy seeded
        // identically; their used_memory and policy name match deterministically.
        let a = build_and_swap(99);
        let b = build_and_swap(99);
        assert_eq!(a.policy_name(), b.policy_name());
        assert_eq!(a.used_memory(), b.used_memory());
        assert_eq!(a.len(), b.len());
    }

    // -- List commands (PR-5) through dispatch over a real ShardStore. --

    /// An integer reply value (named `iv` to avoid colliding with the existing `int`
    /// helper, which EXTRACTS an i64 from a Value).
    fn iv(n: i64) -> Value {
        Value::Integer(n)
    }

    /// A bulk-string array reply from byte slices.
    fn arr(items: &[&[u8]]) -> Value {
        Value::Array(Some(items.iter().map(|b| bulk(b)).collect()))
    }

    #[test]
    fn lpush_rpush_order_and_return_len() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // RPUSH appends: k = [a, b, c]; returns the running length.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a"]),
            iv(1)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"b", b"c"]),
            iv(3)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
            arr(&[b"a", b"b", b"c"])
        );
        // LPUSH prepends each in turn: LPUSH k x y -> y then x at the head, so the
        // list becomes [y, x, a, b, c].
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LPUSH", b"k", b"x", b"y"]),
            iv(5)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
            arr(&[b"y", b"x", b"a", b"b", b"c"])
        );
        // TYPE is list; OBJECT ENCODING is listpack while small.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"k"]),
            Value::simple("list")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"k"]),
            bulk(b"listpack")
        );
    }

    #[test]
    fn pushx_only_when_exists() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // LPUSHX/RPUSHX on a missing key -> 0, no create.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LPUSHX", b"k", b"a"]),
            iv(0)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RPUSHX", b"k", b"a"]),
            iv(0)
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"LLEN", b"k"]), iv(0));
        // Create with RPUSH, then PUSHX appends.
        run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RPUSHX", b"k", b"b"]),
            iv(2)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LPUSHX", b"k", b"z"]),
            iv(3)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
            arr(&[b"z", b"a", b"b"])
        );
    }

    #[test]
    fn lpop_rpop_single_count_and_nil() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"RPUSH", b"k", b"a", b"b", b"c", b"d"],
        );
        // Single LPOP -> bulk "a".
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"LPOP", b"k"]), bulk(b"a"));
        // RPOP -> bulk "d".
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"RPOP", b"k"]), bulk(b"d"));
        // LPOP with count -> array.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LPOP", b"k", b"2"]),
            arr(&[b"b", b"c"])
        );
        // The list is now empty -> key deleted; LPOP -> nil (no count), nil array (count).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LPOP", b"k"]),
            Value::Null
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LPOP", b"k", b"3"]),
            Value::Array(None)
        );
        // A negative count is the must-be-positive error.
        run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"x"]);
        match run_on(&c, &mut s, &mut st, t, &[b"LPOP", b"k", b"-1"]) {
            Value::Error(e) => {
                assert_eq!(e.line(), "-ERR value is out of range, must be positive");
            }
            other => panic!("expected must-be-positive error, got {other:?}"),
        }
    }

    #[test]
    fn lrange_inclusive_and_negative_indices() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"RPUSH", b"k", b"a", b"b", b"c", b"d", b"e"],
        );
        // Inclusive range.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"1", b"3"]),
            arr(&[b"b", b"c", b"d"])
        );
        // Negative indices from the tail.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"-2", b"-1"]),
            arr(&[b"d", b"e"])
        );
        // Out-of-range / inverted -> empty array.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"5", b"10"]),
            Value::Array(Some(vec![]))
        );
        // Absent key -> empty array.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"nope", b"0", b"-1"]),
            Value::Array(Some(vec![]))
        );
    }

    #[test]
    fn lindex_nil_out_of_range() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a", b"b", b"c"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LINDEX", b"k", b"0"]),
            bulk(b"a")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LINDEX", b"k", b"-1"]),
            bulk(b"c")
        );
        // Out of range -> nil.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LINDEX", b"k", b"5"]),
            Value::Null
        );
    }

    #[test]
    fn lset_no_such_key_index_out_of_range_and_success() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // LSET on a missing key -> -ERR no such key.
        match run_on(&c, &mut s, &mut st, t, &[b"LSET", b"k", b"0", b"v"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR no such key"),
            other => panic!("expected no such key, got {other:?}"),
        }
        run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a", b"b", b"c"]);
        // Out-of-range index -> -ERR index out of range.
        match run_on(&c, &mut s, &mut st, t, &[b"LSET", b"k", b"9", b"v"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR index out of range"),
            other => panic!("expected index out of range, got {other:?}"),
        }
        // Success.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LSET", b"k", b"1", b"B"]),
            Value::ok()
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
            arr(&[b"a", b"B", b"c"])
        );
    }

    #[test]
    fn linsert_before_after_pivot_not_found_and_key_absent() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Absent key -> 0.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LINSERT", b"k", b"BEFORE", b"x", b"y"]
            ),
            iv(0)
        );
        run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a", b"c"]);
        // BEFORE c -> insert b: [a, b, c]; returns new len 3.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LINSERT", b"k", b"BEFORE", b"c", b"b"]
            ),
            iv(3)
        );
        // AFTER a -> insert A: [a, A, b, c]; returns 4.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LINSERT", b"k", b"AFTER", b"a", b"A"]
            ),
            iv(4)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
            arr(&[b"a", b"A", b"b", b"c"])
        );
        // Pivot not found -> -1, no change.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LINSERT", b"k", b"BEFORE", b"zzz", b"q"]
            ),
            iv(-1)
        );
    }

    #[test]
    fn lrem_positive_negative_zero() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let seed = |st: &mut TestStore, s: &mut ConnState| {
            run_on(&c, s, st, t, &[b"DEL", b"k"]);
            run_on(
                &c,
                s,
                st,
                t,
                &[b"RPUSH", b"k", b"a", b"b", b"a", b"c", b"a"],
            );
        };
        // count > 0: remove first 2 'a' head->tail: [b, c, a].
        seed(&mut st, &mut s);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LREM", b"k", b"2", b"a"]),
            iv(2)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
            arr(&[b"b", b"c", b"a"])
        );
        // count < 0: remove first 1 'a' tail->head: [a, b, a, c].
        seed(&mut st, &mut s);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LREM", b"k", b"-1", b"a"]),
            iv(1)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
            arr(&[b"a", b"b", b"a", b"c"])
        );
        // count == 0: remove ALL 'a': [b, c].
        seed(&mut st, &mut s);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LREM", b"k", b"0", b"a"]),
            iv(3)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
            arr(&[b"b", b"c"])
        );
    }

    #[test]
    fn ltrim_inclusive_and_empty_deletes_key() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"RPUSH", b"k", b"a", b"b", b"c", b"d", b"e"],
        );
        // Keep [1, 3] -> [b, c, d].
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LTRIM", b"k", b"1", b"3"]),
            Value::ok()
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
            arr(&[b"b", b"c", b"d"])
        );
        // An out-of-range trim empties the list -> key deleted.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LTRIM", b"k", b"5", b"10"]),
            Value::ok()
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"k"]),
            Value::simple("none")
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"k"]), iv(0));
    }

    #[test]
    fn lmove_and_rpoplpush_including_src_eq_dst_rotate() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"RPUSH", b"src", b"a", b"b", b"c"],
        );
        // LMOVE src dst LEFT RIGHT: pop 'a' from src head, push to dst tail.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LMOVE", b"src", b"dst", b"LEFT", b"RIGHT"]
            ),
            bulk(b"a")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"src", b"0", b"-1"]),
            arr(&[b"b", b"c"])
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"dst", b"0", b"-1"]),
            arr(&[b"a"])
        );
        // RPOPLPUSH src dst: pop 'c' from src tail, push to dst head.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RPOPLPUSH", b"src", b"dst"]),
            bulk(b"c")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"dst", b"0", b"-1"]),
            arr(&[b"c", b"a"])
        );
        // src == dst rotate: RPOPLPUSH dst dst moves the tail to the head.
        // dst = [c, a] -> rotate -> [a, c].
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RPOPLPUSH", b"dst", b"dst"]),
            bulk(b"a")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"dst", b"0", b"-1"]),
            arr(&[b"a", b"c"])
        );
        // LMOVE from an absent src -> nil.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LMOVE", b"nope", b"dst", b"LEFT", b"LEFT"]
            ),
            Value::Null
        );
    }

    #[test]
    fn lpos_rank_count_maxlen() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // [a, b, c, a, b, c, a]
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"RPUSH", b"k", b"a", b"b", b"c", b"a", b"b", b"c", b"a"],
        );
        // First 'a' -> index 0.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LPOS", b"k", b"a"]),
            iv(0)
        );
        // RANK 2 -> the SECOND 'a' -> index 3.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LPOS", b"k", b"a", b"RANK", b"2"]
            ),
            iv(3)
        );
        // RANK -1 -> the last 'a' -> index 6.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LPOS", b"k", b"a", b"RANK", b"-1"]
            ),
            iv(6)
        );
        // COUNT 0 -> all 'a' positions [0, 3, 6].
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LPOS", b"k", b"a", b"COUNT", b"0"]
            ),
            Value::Array(Some(vec![iv(0), iv(3), iv(6)]))
        );
        // MAXLEN 2 with COUNT 0 -> only the first 2 elements are scanned -> [0].
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LPOS", b"k", b"a", b"COUNT", b"0", b"MAXLEN", b"2"]
            ),
            Value::Array(Some(vec![iv(0)]))
        );
        // No match -> nil (no COUNT), empty array (with COUNT).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"LPOS", b"k", b"zzz"]),
            Value::Null
        );
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"LPOS", b"k", b"zzz", b"COUNT", b"0"]
            ),
            Value::Array(Some(vec![]))
        );
    }

    #[test]
    fn wrongtype_on_a_string_key() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"str", b"hello"]);
        for cmd in [
            vec![b"LPUSH".as_slice(), b"str", b"x"],
            vec![b"RPUSH", b"str", b"x"],
            vec![b"LPOP", b"str"],
            vec![b"LLEN", b"str"],
            vec![b"LRANGE", b"str", b"0", b"-1"],
            vec![b"LINDEX", b"str", b"0"],
            vec![b"LSET", b"str", b"0", b"v"],
            vec![b"LINSERT", b"str", b"BEFORE", b"a", b"b"],
            vec![b"LREM", b"str", b"0", b"a"],
            vec![b"LTRIM", b"str", b"0", b"-1"],
            vec![b"LPOS", b"str", b"a"],
        ] {
            match run_on(&c, &mut s, &mut st, t, &cmd) {
                Value::Error(e) => assert_eq!(
                    e.line(),
                    "-WRONGTYPE Operation against a key holding the wrong kind of value",
                    "{cmd:?}"
                ),
                other => panic!("expected WRONGTYPE for {cmd:?}, got {other:?}"),
            }
        }
        // The string value is untouched.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"str"]),
            bulk(b"hello")
        );
    }

    #[test]
    fn object_encoding_listpack_then_quicklist() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"k"]),
            bulk(b"listpack")
        );
        // Push a value over the 8 KB byte budget -> quicklist.
        let big = vec![b'q'; 9000];
        run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", &big]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"k"]),
            bulk(b"quicklist")
        );
    }

    #[test]
    fn list_command_arity_errors() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        for bad in [
            vec![b"LPUSH".as_slice(), b"k"],         // needs >= 1 element
            vec![b"LPOP", b"k", b"1", b"extra"],     // at most key + count
            vec![b"LRANGE", b"k", b"0"],             // needs start AND stop
            vec![b"LSET", b"k", b"0"],               // needs index AND element
            vec![b"LINSERT", b"k", b"BEFORE", b"p"], // needs pivot AND element
            vec![b"LLEN"],                           // needs a key
        ] {
            match run_on(&c, &mut s, &mut st, t, &bad) {
                Value::Error(e) => assert!(
                    e.line().contains("wrong number of arguments"),
                    "{bad:?} -> {}",
                    e.line()
                ),
                other => panic!("expected arity error for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn hash_commands_through_dispatch() {
        // Drive the HASH commands through the full dispatcher (so the HRANDFIELD RNG draw
        // off the Env seam, the denyoom gate, and the command-table wiring are exercised).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // HSET two new fields -> :2.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"HSET", b"h", b"a", b"1", b"b", b"2"]
            ),
            Value::Integer(2)
        );
        // HGET -> bulk.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"HGET", b"h", b"a"]),
            bulk(b"1")
        );
        // HRANDFIELD with no count -> one of the fields (the RNG seed comes off the Env
        // seam inside dispatch; we just assert it is a member).
        match run_on(&c, &mut s, &mut st, t, &[b"HRANDFIELD", b"h"]) {
            Value::BulkString(Some(f)) => {
                assert!(f.as_ref() == b"a" || f.as_ref() == b"b", "got {f:?}");
            }
            other => panic!("HRANDFIELD -> {other:?}"),
        }
        // HGETALL -> a map value (the encoder degrades it per proto).
        match run_on(&c, &mut s, &mut st, t, &[b"HGETALL", b"h"]) {
            Value::Map(pairs) => assert_eq!(pairs.len(), 2),
            other => panic!("HGETALL -> {other:?}"),
        }
        // HDEL both fields -> :2, then the key is gone (empty-deletes-key).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"HDEL", b"h", b"a", b"b"]),
            Value::Integer(2)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"h"]),
            Value::Integer(0)
        );
    }

    // -- Transactions: MULTI/EXEC/DISCARD queueing (TRANSACTIONS.md, PR-10a). These use
    // the persistent-store `run_on` helper so the per-connection MULTI state (in_multi /
    // queued / dirty_exec on `s`) and the store both persist across calls. --

    #[test]
    fn multi_opens_a_transaction_and_queues_commands() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // MULTI -> +OK and the connection is in a transaction.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        assert!(s.in_multi);
        // Each subsequent command is QUEUED (a SimpleString "QUEUED"), NOT executed.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]),
            Value::simple("QUEUED")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]),
            Value::simple("QUEUED")
        );
        // The queue grew; nothing applied yet (k still absent in the store).
        assert_eq!(s.queued.len(), 2);
        // Even a read like GET is QUEUED inside MULTI (it does not execute now), so it
        // replies +QUEUED rather than the value, and the queue grows to 3.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]),
            Value::simple("QUEUED")
        );
        assert_eq!(s.queued.len(), 3);
    }

    #[test]
    fn exec_runs_queued_commands_in_order_returning_an_array() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]);
        // EXEC -> Array([+OK, :2]) in order.
        match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
            Value::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Value::ok());
                assert_eq!(items[1], Value::Integer(2));
            }
            other => panic!("EXEC -> {other:?}"),
        }
        // The transaction is over and the batch applied: k == 2.
        assert!(!s.in_multi);
        assert!(s.queued.is_empty());
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"2"));
    }

    #[test]
    fn empty_multi_exec_is_an_empty_array() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
            Value::Array(Some(vec![]))
        );
        assert!(!s.in_multi);
    }

    #[test]
    fn discard_drops_the_queue_and_exits_multi() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]),
            Value::simple("QUEUED")
        );
        // DISCARD -> +OK, queue dropped, not in MULTI, nothing applied.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"DISCARD"]), Value::ok());
        assert!(!s.in_multi);
        assert!(s.queued.is_empty());
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
    }

    #[test]
    fn exec_and_discard_without_multi_are_errors() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
            "-ERR EXEC without MULTI"
        );
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"DISCARD"])),
            "-ERR DISCARD without MULTI"
        );
    }

    #[test]
    fn nested_multi_is_an_error_and_leaves_the_queue_intact() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        assert_eq!(s.queued.len(), 1);
        // A nested MULTI errors and does NOT touch the queue or the transaction state.
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"MULTI"])),
            "-ERR MULTI calls can not be nested"
        );
        assert!(s.in_multi);
        assert_eq!(
            s.queued.len(),
            1,
            "the queue is intact after a nested MULTI"
        );
    }

    #[test]
    fn queue_time_arity_error_dirties_and_exec_aborts() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        // A valid queued write first.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]),
            Value::simple("QUEUED")
        );
        // GET with no key: a queue-time ARITY error reported NOW, and the txn dirtied.
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"GET"])),
            "-ERR wrong number of arguments for 'get' command"
        );
        assert!(s.dirty_exec);
        // EXEC -> EXECABORT, nothing applied (k absent), transaction cleared.
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
            "-EXECABORT Transaction discarded because of previous errors."
        );
        assert!(!s.in_multi);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
    }

    #[test]
    fn queue_time_unknown_command_dirties_and_exec_aborts() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        // An unknown command inside MULTI: the unknown-command error NOW + dirty.
        match run_on(&c, &mut s, &mut st, t, &[b"FROBNICATE", b"a"]) {
            Value::Error(e) => assert!(
                e.line().starts_with("-ERR unknown command 'FROBNICATE'"),
                "{}",
                e.line()
            ),
            other => panic!("expected unknown-command error, got {other:?}"),
        }
        assert!(s.dirty_exec);
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
            "-EXECABORT Transaction discarded because of previous errors."
        );
        assert!(!s.in_multi);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
    }

    #[test]
    fn wrong_arity_exec_inside_multi_dirties_and_next_exec_aborts() {
        // commandCheckArity runs BEFORE the MULTI queue block in Redis, so a wrong-arity
        // control verb (here EXEC) issued inside a transaction DIRTIES it: the bad EXEC
        // replies its arity error, the txn stays OPEN + dirty, and a SUBSEQUENT clean EXEC
        // returns EXECABORT. (MULTI; EXEC x; EXEC -> +OK, arity error, EXECABORT.)
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        // EXEC with an extra arg: wrong arity reported NOW, txn dirtied but still open.
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC", b"x"])),
            "-ERR wrong number of arguments for 'exec' command"
        );
        assert!(s.in_multi, "the wrong-arity EXEC does NOT exit the txn");
        assert!(s.dirty_exec, "the wrong-arity EXEC dirties the txn");
        // A subsequent clean EXEC aborts because the txn is dirty.
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
            "-EXECABORT Transaction discarded because of previous errors."
        );
        assert!(!s.in_multi);
    }

    #[test]
    fn wrong_arity_multi_inside_multi_dirties_and_next_exec_aborts() {
        // Same as the EXEC case but with a wrong-arity MULTI: it dirties the open txn (a
        // bad-arity control verb is rejected before the nested-MULTI check), so the later
        // clean EXEC aborts. (MULTI; MULTI x; EXEC -> +OK, arity error, EXECABORT.)
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"MULTI", b"x"])),
            "-ERR wrong number of arguments for 'multi' command"
        );
        assert!(s.in_multi, "the wrong-arity MULTI does NOT exit the txn");
        assert!(s.dirty_exec, "the wrong-arity MULTI dirties the txn");
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
            "-EXECABORT Transaction discarded because of previous errors."
        );
        assert!(!s.in_multi);
    }

    #[test]
    fn wrong_arity_discard_inside_multi_dirties_and_next_exec_aborts() {
        // Same with a wrong-arity DISCARD: it dirties the open txn (the arity failure is
        // before the queue block) and does NOT discard it; the later clean EXEC aborts.
        // (MULTI; DISCARD x; EXEC -> +OK, arity error, EXECABORT.)
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"DISCARD", b"x"])),
            "-ERR wrong number of arguments for 'discard' command"
        );
        assert!(s.in_multi, "the wrong-arity DISCARD does NOT exit the txn");
        assert!(s.dirty_exec, "the wrong-arity DISCARD dirties the txn");
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
            "-EXECABORT Transaction discarded because of previous errors."
        );
        assert!(!s.in_multi);
    }

    #[test]
    fn wrong_arity_control_verb_outside_multi_is_a_plain_error() {
        // When NOT in a transaction, a wrong-arity control verb is just its arity error
        // (nothing to dirty): EXEC x -> arity error; a later clean EXEC is EXEC-without-
        // MULTI (NOT EXECABORT), confirming nothing was left dirty.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC", b"x"])),
            "-ERR wrong number of arguments for 'exec' command"
        );
        assert!(!s.in_multi);
        assert!(!s.dirty_exec);
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"DISCARD", b"x"])),
            "-ERR wrong number of arguments for 'discard' command"
        );
        assert!(!s.dirty_exec);
        // A clean EXEC now: EXEC-without-MULTI, not EXECABORT.
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
            "-ERR EXEC without MULTI"
        );
    }

    #[test]
    fn exec_does_not_roll_back_on_a_runtime_error() {
        // No rollback (TRANSACTIONS.md): a per-command runtime error at EXEC time becomes
        // an Error ELEMENT in the array; the batch continues and later writes apply.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // A string value that INCR cannot parse.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"sv", b"hello"]),
            Value::ok()
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"sv"]); // will fail at run time
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"s2", b"ok"]); // must still apply
        match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
            Value::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                match &items[0] {
                    Value::Error(e) => {
                        assert_eq!(e.line(), "-ERR value is not an integer or out of range");
                    }
                    other => panic!("element 0 should be the INCR error, got {other:?}"),
                }
                assert_eq!(items[1], Value::ok());
            }
            other => panic!("EXEC -> {other:?}"),
        }
        // No rollback: s2 was set despite the earlier error element.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"s2"]),
            bulk(b"ok")
        );
    }

    #[test]
    fn reset_mid_multi_clears_the_transaction() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        // RESET inside MULTI clears the transaction (it is in the queue-gate exclusion
        // set, so it runs immediately and resets the connection).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RESET"]),
            Value::SimpleString("RESET".to_owned())
        );
        assert!(!s.in_multi);
        assert!(s.queued.is_empty());
        // A subsequent EXEC is now "without MULTI".
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
            "-ERR EXEC without MULTI"
        );
    }

    #[test]
    fn per_command_admission_runs_inside_exec() {
        // The maxmemory denyoom gate is evaluated PER QUEUED COMMAND at EXEC time (it
        // lives in dispatch_inner). With a tiny budget + noeviction, a queued write that
        // tips strictly over budget becomes an -OOM error ELEMENT in the array; the batch
        // does not roll back the writes that already applied.
        let c = ctx_with_budget(50);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::NoEviction);
        let t = UnixMillis(0);
        let big = vec![b'v'; 100];
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        // First queued SET: at EXEC time used starts at 0 (< 50), so it is served and
        // pushes the store over budget.
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k1", &big]);
        // Second queued SET: at EXEC time used is now strictly over budget -> -OOM.
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k2", &big]);
        match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
            Value::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Value::ok(), "first write served");
                match &items[1] {
                    Value::Error(e) => assert_eq!(
                        e.line(),
                        "-OOM command not allowed when used memory > 'maxmemory'."
                    ),
                    other => panic!("element 1 should be -OOM, got {other:?}"),
                }
            }
            other => panic!("EXEC -> {other:?}"),
        }
        // No rollback: k1 is present, k2 was rejected.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k1"]), bulk(&big));
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"k2"]),
            Value::Null
        );
    }

    #[test]
    fn control_commands_are_not_queued_inside_multi() {
        // MULTI/EXEC/DISCARD/RESET/QUIT are NOT staged: they act on the connection even
        // while in a transaction. Here QUIT inside MULTI closes (and is not queued).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        assert_eq!(s.queued.len(), 1);
        // QUIT runs immediately (sets should_close), not queued.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"QUIT"]), Value::ok());
        assert!(s.should_close);
        assert_eq!(s.queued.len(), 1, "QUIT was not queued");
    }

    // -- WATCH/UNWATCH optimistic-lock dirty-CAS (TRANSACTIONS.md, PR-10b). These drive
    // dispatch over a PERSISTENT store via run_on; the cross-connection tests drive two
    // ConnStates against the SAME store (the per-key version slots are shared on the one
    // accept shard, single-shard-per-connection). --

    #[test]
    fn cas_abort_same_connection_modifies_then_exec_is_null() {
        // WATCH k; SET k v (same connection, before MULTI); MULTI; INCR k; EXEC -> Null;
        // nothing applied (the optimistic lock saw k change between WATCH and EXEC).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
            Value::ok()
        );
        // Modify the watched key (a plain SET runs now, it is not in MULTI).
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"2"]);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]),
            Value::simple("QUEUED")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
            Value::Array(None),
            "a dirtied watch makes EXEC return the null array"
        );
        assert!(!s.in_multi);
        assert!(s.watch.is_empty(), "EXEC cleared the watch set");
        // The INCR did NOT apply: k is still "2" (from the modification), not 3.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"2"));
    }

    #[test]
    fn cas_pass_unmodified_then_exec_runs() {
        // WATCH k; (no modification); MULTI; INCR k; EXEC -> runs; k incremented.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
            Value::ok()
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]);
        match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
            Value::Array(Some(items)) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], Value::Integer(2));
            }
            other => panic!("EXEC -> {other:?}"),
        }
        assert!(s.watch.is_empty(), "EXEC cleared the watch set");
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"2"));
    }

    #[test]
    fn cas_abort_cross_connection() {
        // conn1 WATCH k; conn2 SET k v (on the SAME store); conn1 MULTI; INCR k; EXEC ->
        // Null. Two connections, one shared accept shard (single-shard-per-connection).
        let c = ctx(None);
        let mut s1 = state(&c);
        let mut s2 = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let _ = run_on(&c, &mut s1, &mut st, t, &[b"SET", b"k", b"1"]);
        assert_eq!(
            run_on(&c, &mut s1, &mut st, t, &[b"WATCH", b"k"]),
            Value::ok()
        );
        // conn2 modifies the watched key.
        let _ = run_on(&c, &mut s2, &mut st, t, &[b"SET", b"k", b"99"]);
        assert_eq!(run_on(&c, &mut s1, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s1, &mut st, t, &[b"INCR", b"k"]);
        assert_eq!(
            run_on(&c, &mut s1, &mut st, t, &[b"EXEC"]),
            Value::Array(None),
            "another connection's write on the same shard aborts the watcher's EXEC"
        );
        assert_eq!(
            run_on(&c, &mut s1, &mut st, t, &[b"GET", b"k"]),
            bulk(b"99")
        );
    }

    #[test]
    fn unwatch_cancels_the_watch() {
        // WATCH k; UNWATCH; modify k; MULTI; INCR k; EXEC -> runs (the watch was canceled
        // so the later modification does not abort).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
            Value::ok()
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"UNWATCH"]), Value::ok());
        assert!(s.watch.is_empty(), "UNWATCH cleared the watch set");
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"5"]);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]);
        match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
            Value::Array(Some(items)) => assert_eq!(items[0], Value::Integer(6)),
            other => panic!("EXEC -> {other:?}"),
        }
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"6"));
    }

    #[test]
    fn watch_inside_multi_errors_without_dirtying() {
        // MULTI; WATCH k -> the error, txn stays OPEN + CLEAN; a following SET queues; EXEC
        // runs (NOT EXECABORT: WATCH-inside-MULTI does not dirty the batch).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"])),
            "-ERR WATCH inside MULTI is not allowed"
        );
        // The txn is intact: still in MULTI, NOT dirty, watch set empty (WATCH did not run).
        assert!(s.in_multi);
        assert!(!s.dirty_exec, "WATCH inside MULTI does not dirty the batch");
        assert!(s.watch.is_empty());
        // A following command still queues, and EXEC runs cleanly.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"7"]),
            Value::simple("QUEUED")
        );
        match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
            Value::Array(Some(items)) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], Value::ok());
            }
            other => panic!("EXEC after WATCH-inside-MULTI -> {other:?}"),
        }
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"7"));
    }

    #[test]
    fn unwatch_inside_multi_queues_and_runs_at_exec() {
        // UNWATCH inside MULTI is a NORMAL command: it QUEUES (+QUEUED) and runs at EXEC
        // (as a +OK element). It is NOT control-flow (unlike WATCH).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"UNWATCH"]),
            Value::simple("QUEUED"),
            "UNWATCH queues inside MULTI"
        );
        assert_eq!(s.queued.len(), 1);
        match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
            Value::Array(Some(items)) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], Value::ok(), "the queued UNWATCH ran as +OK");
            }
            other => panic!("EXEC -> {other:?}"),
        }
    }

    #[test]
    fn no_op_write_dirties_the_watch_through_dispatch() {
        // SADD s a; WATCH s; SADD s a (already a member -> no value change); MULTI; INCR x;
        // EXEC -> Null (the no-op write still bumped the version through dispatch).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SADD", b"s", b"a"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"s"]),
            Value::ok()
        );
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SADD", b"s", b"a"]); // no-op
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"x"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
            Value::Array(None)
        );
    }

    #[test]
    fn watched_key_expiry_dirties_through_dispatch() {
        // SET k v EX (a short TTL via PEXPIRE); WATCH k; advance `now` past the deadline so
        // the lazy reap fires inside the EXEC CAS check; MULTI; INCR k; EXEC -> Null.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t0 = UnixMillis(0);
        let _ = run_on_wheel(&c, &mut s, &mut st, &mut wheel, t0, &[b"SET", b"k", b"1"]);
        // Set a deadline at t=10 (PEXPIRE 10 against now=0).
        let _ = run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t0,
            &[b"PEXPIRE", b"k", b"10"],
        );
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t0, &[b"WATCH", b"k"]),
            Value::ok()
        );
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t0, &[b"MULTI"]),
            Value::ok()
        );
        let _ = run_on_wheel(&c, &mut s, &mut st, &mut wheel, t0, &[b"INCR", b"k"]);
        // EXEC at t=100 (past the deadline): the watched key has expired -> Null.
        let t_late = UnixMillis(100);
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t_late, &[b"EXEC"]),
            Value::Array(None),
            "an expiry of the watched key aborts EXEC"
        );
    }

    #[test]
    fn already_absent_watch_stays_clean_through_dispatch() {
        // WATCH missing; (stays missing); MULTI; SET other v; EXEC -> runs.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"missing"]),
            Value::ok()
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"other", b"v"]);
        match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
            Value::Array(Some(items)) => assert_eq!(items[0], Value::ok()),
            other => panic!("EXEC -> {other:?}"),
        }
    }

    #[test]
    fn watched_absent_then_created_aborts_through_dispatch() {
        // WATCH missing; SET missing v; MULTI; INCR x; EXEC -> Null.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"missing"]),
            Value::ok()
        );
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"missing", b"v"]);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"x"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
            Value::Array(None)
        );
    }

    #[test]
    fn flushdb_dirties_a_watch_through_dispatch() {
        // SET k v; WATCH k; FLUSHDB; MULTI; SET k 2; EXEC -> Null.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
            Value::ok()
        );
        let _ = run_on(&c, &mut s, &mut st, t, &[b"FLUSHDB"]);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"2"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
            Value::Array(None)
        );
    }

    #[test]
    fn discard_clears_the_watch_set() {
        // WATCH k; MULTI; DISCARD -> the watch set is cleared (a later modification +
        // MULTI/EXEC runs, the watch was dropped by DISCARD).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
            Value::ok()
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"DISCARD"]), Value::ok());
        assert!(s.watch.is_empty(), "DISCARD cleared the watch set");
        // The watch is gone: a modification then MULTI/EXEC runs.
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"9"]);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]);
        match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
            Value::Array(Some(items)) => assert_eq!(items[0], Value::Integer(10)),
            other => panic!("EXEC -> {other:?}"),
        }
    }

    #[test]
    fn reset_clears_the_watch_set() {
        // WATCH k; RESET -> the watch set is cleared (and the store deregistered, so a
        // later modification + MULTI/EXEC runs).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
            Value::ok()
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"RESET"]),
            Value::SimpleString("RESET".to_owned())
        );
        assert!(s.watch.is_empty(), "RESET cleared the watch set");
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"4"]);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]);
        match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
            Value::Array(Some(items)) => assert_eq!(items[0], Value::Integer(5)),
            other => panic!("EXEC -> {other:?}"),
        }
    }

    #[test]
    fn watch_arity_and_multi_key() {
        // WATCH with no key -> arity error; WATCH of several keys, any one dirtied aborts.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            err_of(run_on(&c, &mut s, &mut st, t, &[b"WATCH"])),
            "-ERR wrong number of arguments for 'watch' command"
        );
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"1"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"a", b"b"]),
            Value::ok()
        );
        assert_eq!(s.watch.len(), 2, "both keys snapshotted");
        // Modify the SECOND watched key only -> EXEC still aborts.
        let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
        let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"a"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
            Value::Array(None)
        );
    }
}
