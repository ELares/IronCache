// SPDX-License-Identifier: MIT OR Apache-2.0
//! Per-shard core-local state split out of `serve.rs` (#625): the `thread_local!` shard-state block
//! (store / replication ring / timing wheel / env / pub-sub + tracking + blocking registries /
//! metrics cells / lifecycle flags) and every `shard_*` accessor + the passive/loading/serving/
//! quiesce flags + the lazy background-expiry task + the metrics-cell adoption helpers. Shared-nothing
//! (ADR-0002): each item is core-local and never crosses threads. Behavior-preserving relocation:
//! the item bodies are byte-identical to their former in-`serve.rs` definitions.

use super::{ShardState, ShardStoreImpl};
use ironcache_env::{Clock, Env, Rng, SystemEnv};
use ironcache_eviction::{Policy, map_policy_name};
use ironcache_observe::ShardCounters;
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{
    CounterDeltas, EXPIRE_CYCLE_INTERVAL, MAX_RECLAIM_PER_CYCLE, ScanCursor, TimingWheel,
    UnixMillis, drain_due_keys,
};
use ironcache_storage::CountingAccounting;
use ironcache_store::{ShardStore, process_memory};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;

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
    // The shard's per-shard replication tail RING (#391/#638), the delta buffer the store's write
    // observer feeds. Held as Rc<RefCell<..>> exactly like STORE, symmetric and core-local (per
    // shard, shared-nothing ADR-0002); the SAME Rc is shared by the store's boxed observer and any
    // reader (the raft repl listener, or the streamed live-cutover sender). `None` on the DEFAULT
    // serving path: a non-raft, non-cutover shard installs NO observer, so this stays empty and the
    // hot path is byte-unchanged. It becomes `Some` when a ring is installed -- by
    // [`crate::replica_attach`] in raft mode (which stashes here) or by [`ensure_shard_ring`] at
    // cutover start -- and [`shard_ring`] reads it back so a second install can REUSE the existing
    // ring instead of clobbering the live observer.
    static RING: RefCell<Option<Rc<RefCell<ironcache_repl::ReplRing>>>> =
        const { RefCell::new(None) };
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
    // Whether THIS shard is mid-QUIESCE for a streamed live-cutover final delta cut (#391,
    // Decision 2 Option C). While `true`, the dispatch write gate rejects every client MUTATOR with
    // `-LOADING` BEFORE the write is assigned a ring offset / appended to the ring, so the final cut
    // ships a consistent tail: no mutation is acked at an offset above the latched cut offset E.
    // Reads are still served during the quiesce. Core-local (per shard, shared-nothing ADR-0002), a
    // plain `Cell<bool>` exactly like REPLICA_PASSIVE above -- deliberately NOT a shared atomic: a
    // cross-thread atomic would RACE the E-latch (the on-thread quiesce sets this flag and reads
    // `ring.head()` in ONE uninterrupted critical section, which a shared flag cannot guarantee). Set
    // by [`quiesce_shard`] (with the E-latch, one on-thread step), cleared by [`unquiesce_shard`]
    // (the abort/resume path). DEFAULTS `false`: the non-cutover hot path pays a single
    // predictable-not-taken bool load per command and is byte-unchanged.
    static LOADING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
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
    pub(crate) static TRACKING: RefCell<Option<Rc<RefCell<crate::pubsub::ShardTracking>>>> =
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

/// Set THIS shard's core-local `-LOADING` write-quiesce flag (#391, Decision 2 Option C).
///
/// `pub(crate)` so the quiesce/unquiesce entries below (and, in a later PR, the on-shard-thread
/// `ShardWork` control message that drives the live cutover) flip it. A plain `Cell` write on the
/// shard's own thread; NOT a shared atomic (that would race the E-latch, see [`quiesce_shard`]).
///
/// `dead_code`-allowed: this + [`quiesce_shard`] + [`unquiesce_shard`] are the quiesce ENTRY the
/// PR-4 cutover coordinator wires (there is no live caller yet; the dispatch gate uses the separate
/// [`is_shard_loading`] read, which IS live). Covered by the PR-3 quiesce unit test.
#[allow(dead_code)]
pub(crate) fn set_shard_loading(loading: bool) {
    LOADING.with(|c| c.set(loading));
}

/// Whether THIS shard is currently quiescing writes for a streamed-cutover final delta cut (#391).
///
/// This is the HOT-PATH read the dispatch write gate ([`route_and_dispatch`]) and the cross-shard
/// drain gate ([`crate::coordinator::run_remote`]) consult: a single core-local `Cell<bool>` load
/// (shared-nothing ADR-0002), `false` on every non-cutover command, so the default path pays one
/// predictable-not-taken branch and never reaches the write classifier. `pub(crate)` so the
/// coordinator drain loop reads the OWNING shard's flag on its own thread.
#[must_use]
pub(crate) fn is_shard_loading() -> bool {
    LOADING.with(std::cell::Cell::get)
}

/// QUIESCE THIS shard for a streamed live-cutover final delta cut and return the cut offset `E`
/// (#391, Decision 2 Option C). The clean entry the PR-4 cutover coordinator calls, ON this shard's
/// thread (via the existing inbox), to stop acking writes and latch the tail.
///
/// The two statements below are ONE uninterrupted on-shard-thread critical section: there is no
/// `.await` and no cross-thread hop between setting the core-local `loading` flag and latching
/// `E = ring.head()`, so nothing else runs in between on this single-threaded, shared-nothing shard
/// (ADR-0002). The instant `loading` is set, the dispatch write gate rejects every client MUTATOR
/// with `-LOADING` BEFORE it is assigned a ring offset (the offset is assigned only inside the
/// store's write funnel, downstream of the gate), so no write can be appended above the `E` captured
/// here. That structurally guarantees "a client write is acked only if its offset <= E" and closes
/// the E-latch TOCTOU window (W1) WITHOUT a lock or a cross-thread atomic. Reads keep flowing.
///
/// `ring` is the shard's always-on replication observer ring (the same ring the write path appends
/// to and the cutover sender drains); it is threaded in explicitly, exactly as
/// [`crate::upgrade::stream::send_cutover`] takes it, rather than read from a thread-local -- so the
/// quiesce and the delta ship agree on one ring. Idempotent: re-quiescing simply re-latches the head.
///
/// `dead_code`-allowed: the PR-4 cutover coordinator is the (single) live caller (it invokes this on
/// the shard thread via the inbox, then ships `ring[F+1..=E]` with the returned `E`). Exercised now
/// by the PR-3 quiesce unit test.
#[allow(dead_code)]
pub(crate) fn quiesce_shard(
    ring: &Rc<RefCell<ironcache_repl::ReplRing>>,
) -> ironcache_repl::ReplOffset {
    set_shard_loading(true);
    ring.borrow().head()
}

/// UNQUIESCE THIS shard: clear the `-LOADING` flag so it resumes acking writes (#391). The
/// ABORT/RESUME path -- a failed or aborted cutover leaves every shard fully serving again. Runs on
/// the shard's own thread (like [`quiesce_shard`]); idempotent (a no-op when not quiescing).
///
/// `dead_code`-allowed: the PR-4 cutover coordinator calls this on the abort edge. Exercised now by
/// the PR-3 quiesce unit test.
#[allow(dead_code)]
pub(crate) fn unquiesce_shard() {
    set_shard_loading(false);
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

pub(crate) fn spawn_expire_task(
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
pub(crate) fn expire_cycle_tick(
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
    // #391 PR-4 (W5, internal-mutator suspension): while THIS shard is QUIESCING for a streamed
    // live-cutover final delta cut, the background active-expiry reaper is INERT (return 0 before any
    // store/wheel borrow). A reap during the outage would route a removal through the write funnel
    // and append a StreamDel at a ring offset ABOVE the latched cut `E`; the bounded delta ship stops
    // at `E`, so that removal would be MISSED by the receiver and the OLD would diverge from the
    // E-consistent cut the receiver committed. Suspending it here -- together with the store's
    // passive lazy-expiry the cutover driver sets alongside the quiesce (so a read during the outage
    // reports a due key as absent WITHOUT physically removing it) -- means NO internal mutation is
    // acked above `E`, so `bulk UNION delta(F, E]` stays EXACTLY the acked keyspace as of `E`. One
    // core-local bool load, default `false`: the non-cutover path is byte-unchanged.
    if is_shard_loading() {
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

/// The per-DB store slot count (#570, `store-slots-per-db`) resolved from the boot config,
/// defaulting to [`ironcache_store::DEFAULT_SLOTS_PER_DB`]. Set ONCE in `run_server_inner`
/// before any shard spawns, then read by every per-shard store construction below. A
/// process-global boot fact (the store-geometry sibling of the leaked `policy_name`), read
/// once per store build OFF the hot path; the store rounds it up to a power of two. Relaxed
/// ordering suffices: the boot store happens-before the shard spawn that reads it, and the
/// value never changes at runtime (it is structurally restart-required, like `databases`).
pub(crate) static STORE_SLOTS_PER_DB: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(ironcache_store::DEFAULT_SLOTS_PER_DB);

/// The process-GLOBAL client SERVE gate (#391 PR-5): whether THIS process may serve client commands
/// yet. `true` on every normal (non-handoff) boot, so the default datapath is byte-unchanged and the
/// hot-path read ([`is_serving`], at the top of [`route_and_dispatch`]) is a single relaxed load that
/// is NEVER taken. Set to `false` ONCE at boot (in [`run_server_observed`]) when this process is the
/// streamed-handoff RECEIVER (`handoff_role == receiver` + a socket): the NEW must NOT serve any
/// client command until the cross-shard cutover has COMMITTED across ALL shards, so a client never
/// reads a half-loaded or not-yet-committed store. Flipped back to `true` EXACTLY ONCE, on the
/// receiver-authoritative `Committed` transition (PR-4 [`crate::upgrade::commit::begin_serving_on_commit`]).
///
/// This is a SINGLE process-global (deliberately NOT a per-shard thread-local like [`LOADING`]): one
/// bool for the whole node makes the client-visible flip ALL-OR-NOTHING with no per-shard stagger --
/// the cross-shard barrier already decided all-or-nothing before this flips, so every shard reads the
/// one flag and starts serving in the same instant. Relaxed ordering suffices: the boot store
/// happens-before every shard spawn, and the commit flip is a single monotonic `false -> true` edge
/// gating a RETRYABLE `-LOADING`, not a data dependency (the durable store was promoted BEFORE the
/// flip, so a stale read racing the flip at worst returns one more retryable `-LOADING`).
static SERVING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// Whether THIS process may serve client commands yet (#391 PR-5): the HOT-PATH read the global serve
/// gate at the top of [`route_and_dispatch`] consults. A single relaxed [`SERVING`] load, `true` on
/// every non-handoff boot, so the default datapath pays one predictable-not-taken branch per command
/// and is byte-unchanged. `pub(crate)` for the dispatch gate.
#[must_use]
pub(crate) fn is_serving() -> bool {
    SERVING.load(Ordering::Relaxed)
}

/// Flip the process-GLOBAL serve gate (#391 PR-5). Two callers, both OFF the hot path:
/// [`run_server_observed`] sets it `false` at boot in the RECEIVER role (the NEW must not serve until
/// commit), and the receiver-authoritative COMMIT path
/// ([`crate::upgrade::commit::begin_serving_on_commit`]) sets it `true` EXACTLY ONCE on the PR-4
/// `Committed` transition -- the all-or-nothing client-visible flip. A single relaxed store; the gate
/// read is [`is_serving`].
pub(crate) fn set_serving(serving: bool) {
    SERVING.store(serving, Ordering::Relaxed);
}

/// The #638 slice-4 RECEIVER-side cross-shard FLIP barrier (process-global). Installed ONCE at boot,
/// ONLY in the streamed-handoff RECEIVER role, with N = the shard count. Each shard, on its OWN
/// successful cutover commit (adopt), reports to it; the barrier flips [`SERVING`] to `true` EXACTLY
/// ONCE, on the Nth (all shards) report -- so the multi-shard sibling begins serving ALL-OR-NOTHING,
/// never on the FIRST shard's commit while a sibling shard is still not committed. `None` (absent) on
/// every normal (non-receiver) boot, so the default datapath never touches it.
static RECEIVER_FLIP_BARRIER: std::sync::OnceLock<
    std::sync::Arc<crate::upgrade::commit::ReceiverFlipBarrier>,
> = std::sync::OnceLock::new();

/// Install the process-global receiver FLIP barrier for a streamed-handoff RECEIVER boot (#638 slice
/// 4), sized to `total` shards. Called ONCE at boot (in [`run_server_observed`]) right where the
/// receiver serve gate is set `false`, BEFORE any shard spawns. Idempotent via [`std::sync::OnceLock`]
/// (a second call is ignored); a non-receiver boot never calls it, so the barrier stays absent.
pub(crate) fn install_receiver_flip_barrier(total: usize) {
    let _ = RECEIVER_FLIP_BARRIER.set(std::sync::Arc::new(
        crate::upgrade::commit::ReceiverFlipBarrier::new(total),
    ));
}

/// Report THIS shard's own cutover commit to the process-global receiver FLIP barrier (#638 slice 4):
/// the LAST shard to commit performs the single all-or-nothing [`set_serving`]`(true)` flip. A no-op
/// when no barrier is installed (every non-receiver boot), so the default path is byte-unchanged.
pub(crate) fn report_receiver_shard_committed() {
    if let Some(barrier) = RECEIVER_FLIP_BARRIER.get() {
        barrier.report_committed();
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
        // The per-DB slot partition (#570): the configured (or default) slot count, published
        // by `run_server_inner` at boot. Bounds a table resize to ~one slot's entries.
        .with_slots_per_db(STORE_SLOTS_PER_DB.load(Ordering::Relaxed))
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

/// This shard's replication tail ring, if one is installed (#391/#638). Symmetric with
/// [`shard_store`] but a pure READ: it returns the ring [`crate::replica_attach`] (raft mode) or
/// [`ensure_shard_ring`] (cutover start) stashed into the [`RING`] thread-local, and `None` on the
/// DEFAULT serving path where no write observer is installed (so a caller can tell "no ring yet"
/// from "ring present" without touching the store). Cheap: one thread-local borrow + an `Rc` clone.
///
/// `dead_code`-allowed: the PR-4 cutover task is the (single) live caller (via [`ensure_shard_ring`]);
/// exercised now by the PR-1 unit tests.
#[allow(dead_code)]
#[must_use]
pub(crate) fn shard_ring() -> Option<Rc<RefCell<ironcache_repl::ReplRing>>> {
    RING.with(|cell| cell.borrow().clone())
}

/// Record `ring` as THIS shard's installed replication tail ring in the [`RING`] thread-local.
///
/// Called on the shard thread right after a write observer feeding `ring` is set on the store, so
/// [`shard_ring`] / [`ensure_shard_ring`] can later find and REUSE it instead of installing a second
/// observer (which would drop the live observer box and break replication). Two installers stash
/// here: [`crate::replica_attach`] in raft mode, and [`ensure_shard_ring`] for the streamed cutover.
pub(crate) fn stash_shard_ring(ring: Rc<RefCell<ironcache_repl::ReplRing>>) {
    RING.with(|cell| *cell.borrow_mut() = Some(ring));
}

/// Idempotently ensure THIS shard has an always-on replication tail ring, returning it (#638 PR-1).
///
/// The streamed live-cutover's `freeze_cut` needs the shard's write observer active BEFORE it
/// latches the cut, so every post-freeze write lands in the ring at an offset above the cut. On a
/// non-raft node there is NO ring on the default serving path; this installs one ON DEMAND, at
/// cutover start only, so the steady-state serving path pays nothing (no observer, byte-unchanged).
///
/// Idempotent + raft-safe:
/// - If a ring is ALREADY stashed ([`shard_ring`] is `Some`) -- a prior `ensure_shard_ring`, or the
///   raft/replica attach ([`crate::replica_attach`], which stashes at install) -- REUSE it and return
///   the SAME `Rc`. Never install a second observer over a live one.
/// - Otherwise build a fresh DISK-BACKED ring (the [`crate::replica_attach::build_disk_backlog`]
///   config, so a long Phase-1 bulk under sustained write load cannot overflow the delta window and
///   force an abort), set it as the store's write observer, stash it, and return it. The
///   [`ShardStore::write_observer_active`] guard makes the `set_write_observer` a no-op-if-already-active
///   belt-and-braces against clobbering, even though the `shard_ring` reuse above already covers the
///   production installers.
///
/// Runs on the shard thread (the thread-local store + ring are `!Send`); call it as the FIRST
/// synchronous action of a shard's cutover task, before any await.
///
/// `dead_code`-allowed: the PR-4 cutover task is the (single) live caller; exercised now by the PR-1
/// unit tests (idempotency, raft-reuse, and post-install ring advance).
#[allow(dead_code)]
pub(crate) fn ensure_shard_ring(
    ctx: &ServerContext,
    shard_index: usize,
) -> Rc<RefCell<ironcache_repl::ReplRing>> {
    // Reuse an already-installed ring (raft/replica attach stashes here; so does a prior ensure).
    if let Some(existing) = shard_ring() {
        return existing;
    }
    let store = shard_store(
        ctx.databases,
        ctx.info.maxmemory_policy,
        scan_reserved_bits(ctx.shards),
    );
    // A DISK-backed ring so the (F, E] delta window cannot overflow under sustained write load during
    // a long bulk phase (the same config the raft primary observer uses); `None` disk (no data_dir /
    // the size knob at 0) degrades to the in-memory-only tail, exactly like the raft path.
    let disk = crate::replica_attach::build_disk_backlog(ctx, shard_index);
    let ring = ironcache_repl::ReplRing::with_disk(
        crate::replica_attach::TAIL_RING_CAP,
        ironcache_repl::ReplOffset::ZERO,
        disk,
    );
    {
        let mut s = store.borrow_mut();
        // Defense in depth: only flip the observer on when the store has none. In production an
        // active observer always has a stashed ring (handled by the reuse above), so this is reached
        // with no observer; the guard makes a double-install impossible regardless.
        if !s.write_observer_active() {
            s.set_write_observer(ironcache_repl::ReplObserver::boxed(Rc::clone(&ring)));
        }
    }
    stash_shard_ring(Rc::clone(&ring));
    ring
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

pub(crate) fn shard_started_at() -> ironcache_env::Monotonic {
    STARTED_AT.with(|s| s.borrow().unwrap_or(ironcache_env::Monotonic::ZERO))
}
