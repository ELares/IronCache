// SPDX-License-Identifier: MIT OR Apache-2.0
//! The PR-10b WATCH optimistic-lock mechanism, exercised at the STORE level
//! (TRANSACTIONS.md "WATCH optimistic locking via per-key dirty-CAS"). These drive the
//! concrete [`ShardStore`] through the additive `Watch` side-trait
//! (watch_snapshot/watch_is_dirty/unwatch) + the write-funnel notify, asserting:
//!
//! - a watched key that is OVERWRITTEN / DELETED / created-while-absent reads dirty;
//! - a NO-OP in-place write (SADD of an existing member) still bumps the version ->
//!   dirty (any write touches the version, matching Redis);
//! - an EXPIRY (lazy reap) of a watched-live key reads dirty (present->absent);
//! - the already-absent-at-WATCH rule: a watched-absent key that STAYS absent is clean,
//!   but one that becomes present is dirty (the Redis 6.0.9+ `wk->expired` rule);
//! - FLUSHDB / SWAPDB dirty every watched key in the affected db(s), INCLUDING
//!   watched-but-absent keys;
//! - the HOT-PATH fast gate: a connection that never WATCHes leaves `watched_count == 0`
//!   and the `version_clock` does not advance across writes (the funnel notify does no
//!   per-key work / no hash probe on the non-watching path);
//! - unwatch deregistration drops the slot when the last watcher leaves;
//! - DETERMINISM: identical dirty/clean decisions across an identical seeded replay.
//!
//! Determinism (ADR-0003): the watch mechanism is a u64 VERSION COUNTER, never a clock
//! or RNG; `now` is passed in, and the version decisions are a pure function of the write
//! sequence, so a replay is byte-identical.

use ironcache_storage::{
    ExpireWrite, Keyspace, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store,
    UnixMillis, Watch,
};
use ironcache_store::ShardStore;

const NOW: UnixMillis = UnixMillis(1_000);

fn store() -> ShardStore {
    ShardStore::new(16)
}

/// SADD one member through the store's in-place-mutation arm (create-on-missing via
/// Insert, else Mutated), mirroring the command-layer handler so the no-op-write notify
/// path is exercised through the real `Mutated` route.
fn sadd(store: &mut ShardStore, key: &[u8], member: &[u8]) -> bool {
    let m = member.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::set(vec![m])),
            expire: ExpireWrite::Clear,
            reply: true,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let th = o.thresholds();
            let set = o.as_set_mut().expect("set");
            let was_new = set.add(&m, &th);
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: was_new,
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    })
}

/// EXPIRE-style TTL write through the read-only `rmw` Keep arm: set a new deadline on a
/// live key WITHOUT touching the value (mirrors the cmd_expire handler's
/// `RmwAction::Keep` + `ExpireWrite::Set`). Returns whether the key was live.
fn expire(store: &mut ShardStore, key: &[u8], deadline: UnixMillis) -> bool {
    store.rmw(0, key, NOW, move |entry| match entry {
        RmwEntry::Occupied(_) => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Set(deadline),
            reply: true,
        },
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
        RmwEntry::OccupiedMut(_) => unreachable!(),
    })
}

/// PERSIST-style TTL write through the read-only `rmw` Keep arm: clear the deadline on a
/// live key WITHOUT touching the value (mirrors cmd_persist's `RmwAction::Keep` +
/// `ExpireWrite::Clear`). Returns whether the key was live.
fn persist(store: &mut ShardStore, key: &[u8]) -> bool {
    store.rmw(0, key, NOW, |entry| match entry {
        RmwEntry::Occupied(_) => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Clear,
            reply: true,
        },
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
        RmwEntry::OccupiedMut(_) => unreachable!(),
    })
}

/// Bare GETEX (no TTL option) through the read-only `rmw` Keep arm: a pure read that
/// leaves the deadline UNCHANGED (mirrors the GetexTtl::Unchanged path,
/// `RmwAction::Keep` + `ExpireWrite::Unchanged`). This must NOT dirty a watched key.
fn getex_noop(store: &mut ShardStore, key: &[u8]) -> bool {
    store.rmw(0, key, NOW, |entry| match entry {
        RmwEntry::Occupied(_) => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: true,
        },
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
        RmwEntry::OccupiedMut(_) => unreachable!(),
    })
}

#[test]
fn expire_of_a_watched_key_is_dirty() {
    // SET k v (no TTL); WATCH k; EXPIRE k 100 (sets a deadline) -> dirty. This is the
    // TTL-only-command-dirties-a-watched-key fix: the `rmw` Keep arm bumps the version on
    // a real TTL change, so a following EXEC would abort with the null array.
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let e = s.watch_snapshot(0, b"k", NOW);
    assert!(e.present_at_watch);
    assert!(!s.watch_is_dirty(&e, NOW), "no write yet -> clean");
    assert!(expire(&mut s, b"k", UnixMillis(100_000)), "key was live");
    assert!(
        s.watch_is_dirty(&e, NOW),
        "EXPIRE (a TTL change) dirties the watched key"
    );
}

#[test]
fn persist_of_a_watched_key_with_ttl_is_dirty() {
    // SET k v with a TTL; WATCH k; PERSIST k (clears the deadline) -> dirty.
    let mut s = store();
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(UnixMillis(100_000)),
        NOW,
    );
    let e = s.watch_snapshot(0, b"k", NOW);
    assert!(e.present_at_watch);
    assert!(!s.watch_is_dirty(&e, NOW), "no write yet -> clean");
    assert!(persist(&mut s, b"k"), "key was live");
    assert!(
        s.watch_is_dirty(&e, NOW),
        "PERSIST (clearing a deadline) dirties the watched key"
    );
}

#[test]
fn bare_getex_no_ttl_option_does_not_dirty() {
    // SET k v; WATCH k; bare GETEX k (no TTL option -> ExpireWrite::Unchanged) -> CLEAN.
    // The fix scopes the notify to the real-change branch, so a no-op TTL write keeps the
    // key clean (matches Redis: a bare GETEX does not signalModifiedKey).
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let e = s.watch_snapshot(0, b"k", NOW);
    assert!(getex_noop(&mut s, b"k"), "key was live");
    assert!(
        !s.watch_is_dirty(&e, NOW),
        "a bare GETEX (no TTL option) is a pure read and does NOT dirty the watched key"
    );
}

#[test]
fn no_op_expire_same_deadline_does_not_dirty() {
    // A watched key with a TTL; EXPIRE to the SAME deadline -> no change -> CLEAN. The
    // real-change scoping means an EXPIRE that does not move the deadline stays clean.
    let mut s = store();
    let deadline = UnixMillis(100_000);
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(deadline),
        NOW,
    );
    let e = s.watch_snapshot(0, b"k", NOW);
    assert!(expire(&mut s, b"k", deadline), "key was live");
    assert!(
        !s.watch_is_dirty(&e, NOW),
        "an EXPIRE that does not move the deadline is a no-op and does NOT dirty"
    );
}

#[test]
fn lazily_expired_watched_key_dirties_via_version_bump() {
    // A watched LIVE key that lazily expires between WATCH and EXEC reads dirty, and the
    // dirty signal is carried by the VERSION bump (not only the present/absent flip): the
    // expire_if_due removal now funnels through remove_object, which fires touch_watch.
    // Prove it via the version: after the reap the slot version has advanced past the
    // snapshot, so even if the present/absent comparison agreed, the version alone catches
    // it. We assert the version moved by reading the live slot version through a SECOND
    // watcher planted at the same key after the reap is irrelevant; instead we check the
    // store's version_clock advanced across the reap and that the entry reads dirty.
    let mut s = store();
    let deadline = UnixMillis(2_000);
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(deadline),
        NOW,
    );
    let e = s.watch_snapshot(0, b"k", NOW);
    assert!(e.present_at_watch, "live at watch time");
    let clock_before = s.version_clock();
    // Past the deadline: the dirty check runs the lazy backstop. The removal funnels
    // through remove_object -> touch_watch, so the version clock advances (the version-bump
    // dirty signal), independent of the present/absent fallback.
    assert!(
        s.watch_is_dirty(&e, UnixMillis(2_001)),
        "a lazily-expired watched key is dirty"
    );
    assert!(
        s.version_clock() > clock_before,
        "the lazy expiry funneled through remove_object and bumped the version clock (not only present/absent)"
    );
}

#[test]
fn overwrite_of_a_watched_key_is_dirty() {
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let e = s.watch_snapshot(0, b"k", NOW);
    assert!(e.present_at_watch);
    assert!(!s.watch_is_dirty(&e, NOW), "no write yet -> clean");
    // Overwrite the watched key (even with a different value): dirties it.
    s.upsert(0, b"k", NewValue::Bytes(b"v2"), ExpireWrite::Clear, NOW);
    assert!(s.watch_is_dirty(&e, NOW), "an overwrite bumps the version");
}

#[test]
fn delete_of_a_watched_key_is_dirty() {
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let e = s.watch_snapshot(0, b"k", NOW);
    s.delete(0, b"k", NOW);
    assert!(
        s.watch_is_dirty(&e, NOW),
        "a delete dirties (version bumped + present->absent)"
    );
}

#[test]
fn unrelated_write_does_not_dirty() {
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let e = s.watch_snapshot(0, b"k", NOW);
    // A write to a DIFFERENT key, and the SAME key in a DIFFERENT db, leave it clean.
    s.upsert(0, b"other", NewValue::Bytes(b"x"), ExpireWrite::Clear, NOW);
    s.upsert(1, b"k", NewValue::Bytes(b"x"), ExpireWrite::Clear, NOW);
    assert!(!s.watch_is_dirty(&e, NOW), "unrelated writes do not dirty");
}

#[test]
fn no_op_in_place_write_dirties() {
    // SADD s a; WATCH s; SADD s a (already a member -> no value change); -> dirty.
    let mut s = store();
    assert!(sadd(&mut s, b"s", b"a"), "first SADD is new");
    let e = s.watch_snapshot(0, b"s", NOW);
    assert!(e.present_at_watch);
    assert!(
        !sadd(&mut s, b"s", b"a"),
        "re-SADD of an existing member is a no-op"
    );
    assert!(
        s.watch_is_dirty(&e, NOW),
        "a no-op write (SADD of an existing member) still bumps the version (Redis: any write touches the key)"
    );
}

#[test]
fn expiry_of_a_watched_live_key_dirties() {
    // SET k v with a deadline; WATCH k (present); advance `now` past the deadline so the
    // lazy reap removes it; -> dirty (present->absent + version bumped by the reap).
    let mut s = store();
    let deadline = UnixMillis(2_000);
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(deadline),
        NOW,
    );
    let e = s.watch_snapshot(0, b"k", NOW);
    assert!(e.present_at_watch, "live at watch time");
    // Before the deadline: still clean.
    assert!(!s.watch_is_dirty(&e, UnixMillis(1_999)));
    // Past the deadline: the dirty check runs the lazy backstop, reaping it -> dirty.
    assert!(
        s.watch_is_dirty(&e, UnixMillis(2_001)),
        "an expiry of a watched-live key is a modification"
    );
}

#[test]
fn already_absent_at_watch_stays_clean() {
    // WATCH missing; (stays missing) -> clean (the wk->expired already-absent rule).
    let mut s = store();
    let e = s.watch_snapshot(0, b"missing", NOW);
    assert!(!e.present_at_watch, "absent at watch time");
    assert!(
        !s.watch_is_dirty(&e, NOW),
        "an already-absent key that stays absent is NOT a modification"
    );
    // A write to an unrelated key still leaves it clean.
    s.upsert(0, b"other", NewValue::Bytes(b"x"), ExpireWrite::Clear, NOW);
    assert!(!s.watch_is_dirty(&e, NOW));
}

#[test]
fn watched_absent_then_created_dirties() {
    // WATCH missing; SET missing v -> dirty (absent->present, even though same version
    // slot: the present/absent transition alone is a modification).
    let mut s = store();
    let e = s.watch_snapshot(0, b"missing", NOW);
    assert!(!e.present_at_watch);
    s.upsert(
        0,
        b"missing",
        NewValue::Bytes(b"v"),
        ExpireWrite::Clear,
        NOW,
    );
    assert!(
        s.watch_is_dirty(&e, NOW),
        "a watched-absent key that becomes present is a modification"
    );
}

#[test]
fn flushdb_dirties_all_watches_including_absent() {
    // SET k v; WATCH k (present) + WATCH gone (absent); FLUSHDB -> BOTH dirty (the
    // resident key by remove_object's notify, the absent key by the bulk
    // touch_all_watches_in_db).
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let ek = s.watch_snapshot(0, b"k", NOW);
    let eg = s.watch_snapshot(0, b"gone", NOW);
    assert!(ek.present_at_watch);
    assert!(!eg.present_at_watch);
    s.flush_db(0);
    assert!(
        s.watch_is_dirty(&ek, NOW),
        "FLUSHDB dirties the resident watched key"
    );
    // `gone` is still absent (present/absent unchanged), but its VERSION was bumped by
    // the bulk flush signal, so it reads dirty.
    assert!(
        s.watch_is_dirty(&eg, NOW),
        "FLUSHDB dirties even a watched-but-absent key (touchAllWatchedKeysOnDb)"
    );
}

#[test]
fn flushdb_does_not_dirty_a_watch_in_another_db() {
    let mut s = store();
    s.upsert(1, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let e = s.watch_snapshot(1, b"k", NOW);
    s.flush_db(0); // a different db
    assert!(
        !s.watch_is_dirty(&e, NOW),
        "FLUSHDB of db 0 leaves a db-1 watch clean"
    );
}

#[test]
fn swapdb_dirties_watches_in_both_dbs() {
    // A key watched in db 0 and a key watched in db 1; SWAPDB 0 1 dirties both.
    let mut s = store();
    s.upsert(0, b"a", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    s.upsert(1, b"b", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let ea = s.watch_snapshot(0, b"a", NOW);
    let eb = s.watch_snapshot(1, b"b", NOW);
    s.swap_db(0, 1);
    assert!(s.watch_is_dirty(&ea, NOW), "SWAPDB dirties a watch in db a");
    assert!(s.watch_is_dirty(&eb, NOW), "SWAPDB dirties a watch in db b");
}

#[test]
fn unwatch_drops_the_slot_when_the_last_watcher_leaves() {
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let e = s.watch_snapshot(0, b"k", NOW);
    assert_eq!(s.watched_count(), 1);
    s.unwatch(std::slice::from_ref(&e));
    assert_eq!(
        s.watched_count(),
        0,
        "the last unwatch drops the slot + flag"
    );
}

#[test]
fn two_watchers_share_a_slot_and_each_must_unwatch() {
    // Two connections WATCH the same key: the slot has watchers=2; one UNWATCH leaves it
    // watched (count 1), the second drops it.
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let e1 = s.watch_snapshot(0, b"k", NOW);
    let e2 = s.watch_snapshot(0, b"k", NOW);
    assert_eq!(s.watched_count(), 2);
    s.unwatch(std::slice::from_ref(&e1));
    assert_eq!(s.watched_count(), 1, "one unwatch leaves the other watcher");
    // The remaining watcher still sees a write as dirty.
    s.upsert(0, b"k", NewValue::Bytes(b"v2"), ExpireWrite::Clear, NOW);
    assert!(s.watch_is_dirty(&e2, NOW));
    s.unwatch(std::slice::from_ref(&e2));
    assert_eq!(s.watched_count(), 0);
}

#[test]
fn hot_path_no_watch_means_funnel_does_no_work() {
    // A connection that never WATCHes -> watched_count stays 0 across a stream of writes,
    // and the version_clock does NOT advance (the funnel notify returns on the
    // watched_count==0 fast path before any hash probe / counter bump). This is the
    // structural proof the non-watching hot path pays at most one branch.
    let mut s = store();
    assert_eq!(s.watched_count(), 0);
    assert_eq!(s.version_clock(), 0);
    for i in 0..50u64 {
        let key = format!("k{i}");
        s.upsert(
            0,
            key.as_bytes(),
            NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
    }
    // SADD (an in-place Mutated path), DEL, and overwrites too -> still no version work.
    sadd(&mut s, b"set", b"m");
    sadd(&mut s, b"set", b"m"); // no-op in-place edit
    s.delete(0, b"k0", NOW);
    s.upsert(0, b"k1", NewValue::Bytes(b"v2"), ExpireWrite::Clear, NOW);
    assert_eq!(s.watched_count(), 0, "nothing watched");
    assert_eq!(
        s.version_clock(),
        0,
        "the funnel notify never advanced the version clock with no watches active"
    );
}

#[test]
fn hot_path_only_watched_keys_bump_the_clock() {
    // Once SOMETHING is watched, the clock advances ONLY for writes to a watched key, not
    // for writes to unwatched keys (the per-key hash probe gates the bump).
    let mut s = store();
    s.upsert(0, b"w", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let _e = s.watch_snapshot(0, b"w", NOW);
    let before = s.version_clock();
    // Writes to UNWATCHED keys: no bump.
    s.upsert(0, b"u1", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    s.upsert(0, b"u2", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    assert_eq!(
        s.version_clock(),
        before,
        "unwatched-key writes do not bump"
    );
    // A write to the WATCHED key: bumps.
    s.upsert(0, b"w", NewValue::Bytes(b"v2"), ExpireWrite::Clear, NOW);
    assert!(
        s.version_clock() > before,
        "the watched-key write bumps the clock"
    );
}

#[test]
fn dirty_decisions_are_deterministic_across_replay() {
    // Two independent stores driven through the IDENTICAL write/watch sequence produce
    // the IDENTICAL dirty/clean decision at each step (ADR-0003: the version counter is a
    // pure function of the sequence, no clock/rand). A seeded in-test sequence.
    fn replay() -> Vec<bool> {
        let mut s = store();
        let mut decisions = Vec::new();
        // Seeded splitmix64 to pick keys/ops deterministically (no std::time / no rand).
        let mut seed: u64 = 0x1234_5678_9abc_def0;
        let mut next = || {
            seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        // Plant + watch three keys, then a sequence of writes, recording the dirty
        // decision for each watched key after each step.
        for k in [b"a".as_slice(), b"b", b"c"] {
            s.upsert(0, k, NewValue::Bytes(b"v0"), ExpireWrite::Clear, NOW);
        }
        let watches: Vec<_> = [b"a".as_slice(), b"b", b"c", b"absent"]
            .iter()
            .map(|k| s.watch_snapshot(0, k, NOW))
            .collect();
        let keys: [&[u8]; 4] = [b"a", b"b", b"c", b"absent"];
        for _ in 0..40 {
            let key = keys[(next() % 4) as usize];
            if next() % 2 == 0 {
                s.upsert(0, key, NewValue::Bytes(b"vN"), ExpireWrite::Clear, NOW);
            } else {
                s.delete(0, key, NOW);
            }
            for w in &watches {
                decisions.push(s.watch_is_dirty(w, NOW));
            }
        }
        decisions
    }
    assert_eq!(
        replay(),
        replay(),
        "dirty/clean decisions replay identically"
    );
}
