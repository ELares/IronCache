// SPDX-License-Identifier: MIT OR Apache-2.0

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
fn quiesce_latches_e_gates_writes_and_unquiesce_resumes() {
    // #391 PR-3 HERO: the per-shard `-LOADING` write quiesce + E-latch. This is the load-bearing
    // safety unit: it asserts "a client write is acked only if its offset <= E" and that an
    // in-flight-tail write after the E-latch is rejected + never appended, while reads keep
    // flowing and unquiesce resumes writes.
    use ironcache_repl::{ReplObserver, ReplOffset, ReplRing};

    // Belt-and-braces: this test drives the shard-thread-local LOADING flag; ensure no prior
    // state leaks in (each libtest test runs on its own thread, so this is defensive).
    unquiesce_shard();
    assert!(!is_shard_loading(), "a fresh shard is not quiescing");

    // An always-on observer ring on a fresh store: EVERY applied write is assigned a monotonic
    // ring offset (the keystone the cut relies on). Two client writes give the ring a tail.
    let ring = ReplRing::new(4096, ReplOffset::ZERO);
    let mut store = ShardStore::with_hooks(16, Policy::cache_default(), CountingAccounting::new());
    store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
    let now = UnixMillis(1_000);
    store.upsert(0, b"k1", NewValue::Bytes(b"v1"), ExpireWrite::Clear, now);
    store.upsert(0, b"k2", NewValue::Bytes(b"v2"), ExpireWrite::Clear, now);
    let head_before = ring.borrow().head();
    assert_ne!(
        head_before,
        ReplOffset::ZERO,
        "the two writes advanced the ring before the quiesce"
    );

    // QUIESCE: on this (shard) thread, set loading + latch E in ONE critical section.
    let e = quiesce_shard(&ring);
    assert!(
        is_shard_loading(),
        "the shard is quiescing after quiesce_shard"
    );
    assert_eq!(e, head_before, "E is the ring head at the quiesce instant");

    // A client MUTATOR attempted now is REJECTED by the gate BEFORE offset assignment. The
    // production gate is `is_shard_loading() && request_is_write_for_pause(..)`; assert that
    // exact decision. Because a rejected write returns before the store is ever touched, we do
    // NOT apply it -- and the ring never advances, so no acked write lands above E.
    assert!(
        is_shard_loading() && ironcache_server::request_is_write_for_pause(b"SET", false, &[]),
        "a client write is quiesced (the gate would reject it with -LOADING)"
    );
    assert_eq!(
        ring.borrow().head(),
        e,
        "the rejected write is never appended: the offset stays at E"
    );

    // A READ is still served during the quiesce: the gate never classifies a read as a write,
    // so it flows through, and serving it does not advance the ring.
    assert!(
        !ironcache_server::request_is_write_for_pause(b"GET", false, &[]),
        "a read is not a write, so the quiesce gate lets it through"
    );
    assert!(
        store.contains_live(0, b"k1", now),
        "a read is served during the quiesce"
    );
    assert_eq!(ring.borrow().head(), e, "a read does not advance the ring");

    // Structural "acked implies offset <= E": nothing new was assigned an offset while
    // quiescing, so the ring head is still exactly E.
    assert_eq!(ring.borrow().head(), e, "no mutation was acked above E");

    // UNQUIESCE (the abort/resume path): writes resume; the next write acks and advances PAST E.
    unquiesce_shard();
    assert!(
        !is_shard_loading(),
        "the shard resumes acking writes after unquiesce"
    );
    assert!(
        !(is_shard_loading() && ironcache_server::request_is_write_for_pause(b"SET", false, &[])),
        "a write is no longer quiesced after unquiesce"
    );
    store.upsert(0, b"k3", NewValue::Bytes(b"v3"), ExpireWrite::Clear, now);
    assert!(
        ring.borrow().head() > e,
        "a resumed write acks and advances the offset past E"
    );
}

#[test]
fn quiesce_gate_is_a_core_local_bool_and_defaults_off() {
    // HOT PATH / DEFAULT: the quiesce guard is a single core-local `Cell<bool>` (NOT a shared
    // atomic), defaulting `false`, so a non-cutover shard pays one predictable-not-taken bool
    // load per command and the `&&` short-circuits BEFORE the write classifier ever runs -- the
    // dispatch path is byte-unchanged. This documents + pins that default-off property.
    unquiesce_shard();
    assert!(!is_shard_loading(), "not quiescing by default");
    // The guard toggles on this shard's own thread only (shared-nothing ADR-0002).
    set_shard_loading(true);
    assert!(is_shard_loading(), "the core-local flag flips on");
    set_shard_loading(false);
    assert!(!is_shard_loading(), "the core-local flag flips back off");
}

#[test]
fn ensure_shard_ring_installs_once_and_reuses() {
    // #638 PR-1: the on-demand always-on ring installer is IDEMPOTENT (installs once; the second
    // call reuses the SAME ring), and a write after install advances the ring head (the observer
    // is live). A fresh non-raft shard starts with NO ring, so the default serving path is
    // byte-unchanged until a cutover calls this.
    let ctx = guardrail_ctx(0, 0);

    assert!(
        shard_ring().is_none(),
        "a fresh non-raft shard has no ring (no observer on the default serving path)"
    );

    // First install: a ring appears and the store's write observer is now active.
    let r1 = ensure_shard_ring(&ctx, 0);
    assert!(shard_ring().is_some(), "ensure_shard_ring installs a ring");
    let store = shard_store(
        ctx.databases,
        ctx.info.maxmemory_policy,
        scan_reserved_bits(ctx.shards),
    );
    assert!(
        store.borrow().write_observer_active(),
        "the store's write observer is active after ensure_shard_ring"
    );

    // Second call REUSES the same ring (installs once): the SAME Rc, no second observer.
    let r2 = ensure_shard_ring(&ctx, 0);
    assert!(
        Rc::ptr_eq(&r1, &r2),
        "ensure_shard_ring is idempotent: the second call reuses the first ring"
    );

    // A write after install advances the ring head: the observer feeds the reused ring.
    let head_before = r1.borrow().head();
    store.borrow_mut().upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Clear,
        UnixMillis(1_000),
    );
    assert!(
        r1.borrow().head() > head_before,
        "a write after install advances ring.head() (the ring is live)"
    );
}

#[test]
fn ensure_shard_ring_reuses_preinstalled_raft_ring_without_clobber() {
    // #638 PR-1 (risk 5): in raft mode a ring is ALREADY installed by replica_attach (which now
    // stashes into RING). ensure_shard_ring must REUSE it, NOT install a second observer -- a
    // double-install would drop the replica's observer box and break replication.
    use ironcache_repl::{ReplObserver, ReplOffset, ReplRing};
    let ctx = guardrail_ctx(0, 0);

    // Simulate the raft/replica attach: install an observer ring on the store AND stash it into
    // RING, exactly what replica_attach::spawn_on_shard now does at the observer install.
    let store = shard_store(
        ctx.databases,
        ctx.info.maxmemory_policy,
        scan_reserved_bits(ctx.shards),
    );
    let preinstalled = ReplRing::new(4096, ReplOffset::ZERO);
    store
        .borrow_mut()
        .set_write_observer(ReplObserver::boxed(Rc::clone(&preinstalled)));
    stash_shard_ring(Rc::clone(&preinstalled));

    // ensure_shard_ring returns the SAME Rc: reuse, not clobber.
    let got = ensure_shard_ring(&ctx, 0);
    assert!(
        Rc::ptr_eq(&got, &preinstalled),
        "the pre-installed (raft) ring is reused, not clobbered"
    );

    // The reused ring still tracks writes: the ORIGINAL observer is intact (never replaced).
    let head_before = preinstalled.borrow().head();
    store.borrow_mut().upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Clear,
        UnixMillis(1_000),
    );
    assert!(
        preinstalled.borrow().head() > head_before,
        "the reused ring still observes writes: replication was not clobbered"
    );
}

/// #638 SIGNAL SEAM (arm selection, deterministic): [`resolve_signal`] maps SIGINT and SIGTERM to
/// `Shutdown` and SIGUSR1 to `Cutover`, whichever fires. Driven with plain futures (a `ready` for
/// the arm under test, `pending` for the others) so it is hermetic -- no real signals reach the
/// test process.
#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn resolve_signal_maps_each_arm() {
    use std::future::{pending, ready};
    // SIGINT -> Shutdown.
    assert_eq!(
        resolve_signal(ready(()), pending::<()>(), pending::<()>()).await,
        SignalOutcome::Shutdown,
        "SIGINT initiates the graceful shutdown"
    );
    // SIGTERM -> Shutdown.
    assert_eq!(
        resolve_signal(pending::<()>(), ready(()), pending::<()>()).await,
        SignalOutcome::Shutdown,
        "SIGTERM initiates the graceful shutdown"
    );
    // SIGUSR1 -> Cutover (the #638 streamed live-cutover trigger).
    assert_eq!(
        resolve_signal(pending::<()>(), pending::<()>(), ready(())).await,
        SignalOutcome::Cutover,
        "SIGUSR1 initiates a streamed live cutover"
    );
}

/// #638 SIGNAL SEAM (flag side effect): a `Shutdown` records the stop request on the flag (the
/// unchanged #139 behavior); a `Cutover` leaves it UNTOUCHED (the cutover runs BEFORE any
/// shutdown, so `DRAIN_GRACE` never bounds it). This pins that a SIGUSR1 does NOT begin a
/// shutdown, distinguishing it from SIGINT/SIGTERM.
#[test]
fn apply_signal_flag_sets_only_on_shutdown() {
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    apply_signal_flag(SignalOutcome::Cutover, &flag);
    assert!(
        !flag.load(Ordering::SeqCst),
        "a Cutover (SIGUSR1) does NOT set the shutdown flag"
    );

    apply_signal_flag(SignalOutcome::Shutdown, &flag);
    assert!(
        flag.load(Ordering::SeqCst),
        "a Shutdown (SIGINT/SIGTERM) sets the shutdown flag (unchanged #139 path)"
    );
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

    let rt_cfg = ironcache_config::RuntimeConfig::from_config(&ironcache_config::Config::default());
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
    let rt_cfg = ironcache_config::RuntimeConfig::from_config(&ironcache_config::Config::default());
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
    let rt_cfg = ironcache_config::RuntimeConfig::from_config(&ironcache_config::Config::default());
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

/// #527: the process RUN ID (INFO `run_id`) is a DISTINCT per-boot draw from the SAME env seam
/// as `cluster_node_id` (ADR-0003), mirroring how the real boot draws it (a second `node_id_hex`
/// off `boot_env` right after the node id). Two properties matter: within ONE boot the run id
/// differs from the node id (an independent draw off the stream, not the same value), and a
/// DIFFERENT boot seed yields a DIFFERENT run id -- the mechanism that makes `run_id` change on
/// every restart while staying entirely on the sanctioned seam (no raw entropy). The 40-hex
/// shape + non-zero-ness are covered by `node_id_hex_is_deterministic_for_a_seed` (same helper).
#[test]
fn run_id_is_a_distinct_per_boot_draw_from_the_same_seam() {
    // One boot: node id is the first draw, run id the second -> they differ.
    let mut boot = ironcache_env::TestEnv::new(0x5EED_1234_5678_9ABC);
    let node_id = node_id_hex(boot.rng());
    let run_id = node_id_hex(boot.rng());
    assert_eq!(run_id.len(), 40, "run id must be 40 hex chars: {run_id:?}");
    assert_ne!(
        node_id, run_id,
        "the run id must be a distinct draw from the node id, not a copy"
    );
    assert_ne!(
        run_id, "0000000000000000000000000000000000000000",
        "the run id must not be the old zero placeholder"
    );
    // A DIFFERENT boot (a different seed, as SystemEnv is wall-clock-seeded per boot) yields a
    // DIFFERENT run id -> the run id changes on restart.
    let mut other = ironcache_env::TestEnv::new(0x5EED_1234_5678_9ABD);
    let _ = node_id_hex(other.rng()); // consume the node-id draw so we compare the RUN-id draw
    let run_id_other = node_id_hex(other.rng());
    assert_ne!(
        run_id, run_id_other,
        "a different boot must yield a different run id"
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
            run_id: RID_A,
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

/// #549: a FORCED save failure flips INFO `rdb_last_bgsave_status` to `err`. We point the data
/// dir under a regular FILE so `create_dir_all` fails, drive the REAL `do_save_all`, and assert
/// the shared persistence-stats cell (the SAME atomics INFO reads) flips to not-ok. The default
/// (pre-save) status is `ok`, matching Redis. The `err` rendering itself is covered by the observe
/// `# Persistence` render test.
#[test]
fn a_failed_save_flips_rdb_last_bgsave_status_to_err() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    // A path that cannot be created: its parent is a regular file, so `create_dir_all` fails.
    let blocker = std::env::temp_dir().join(format!("ic-savefail-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&blocker);
    let _ = std::fs::remove_file(&blocker);
    std::fs::write(&blocker, b"x").expect("write the blocking file");
    let broken_dir = blocker.join("nested");
    let persist = Arc::new(crate::persist::PersistState {
        dir: broken_dir.clone(),
        handoff_base: None,
        boot_load_dir: broken_dir,
        handoff_cleanup_dir: None,
        shards_pending_load: std::sync::atomic::AtomicUsize::new(1),
        stats: Arc::new(ironcache_observe::PersistRuntime::new()),
        save_id: AtomicU64::new(0),
        saving: AtomicBool::new(false),
        needs_base: AtomicBool::new(true),
    });
    // Before any save the status is ok (Redis parity).
    assert!(persist.stats.last_bgsave_ok(), "default status is ok");
    // `ctx` is unused on the create-dir failure arm (it returns before the shard fan-out); an
    // empty inbox drives `n_shards == 0`.
    let ctx = guardrail_ctx(0, 0);
    let inbox: crate::coordinator::Inbox = Arc::from(Vec::new());
    let home = ShardId { index: 0, total: 0 };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let res = rt.block_on(crate::persist::do_save_all(
        &persist,
        &inbox,
        &ctx,
        home,
        0,
        1_700_000_000,
    ));
    assert!(res.is_err(), "the broken data dir fails the save: {res:?}");
    assert!(
        !persist.stats.last_bgsave_ok(),
        "a failed save flips rdb_last_bgsave_status to err"
    );
    let _ = std::fs::remove_file(&blocker);
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
        let with_mig = cluster_redirect(&map, route, cmd, &req, false, false, Some(&ctx_ask), None);
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
