// SPDX-License-Identifier: MIT OR Apache-2.0
//! INDEX-BACKEND growth tests (#285 Stage 3): force the per-slot index through heavy
//! growth and assert reads / SCAN / flush stay correct.
//!
//! WHY A SEPARATE FILE from keyspace.rs's resize tests: those are calibrated to hashbrown
//! capacities at the DEFAULT slot count (256 tables per DB), where a few thousand keys
//! spread to ~tens per table -- far below the dash backend's per-segment capacity, so
//! under the (default) dash backend they force ZERO segment splits and dash growth would go
//! silently untested. These tests pin `with_slots_per_db(1)` so EVERY key lands in ONE
//! index table: a few thousand keys then force many dash segment splits + directory
//! doublings (and, under the default backend, several hashbrown resizes -- the tests are
//! backend-neutral and run under both, exercising whichever growth machinery is compiled
//! in). The mid-SCAN test is the split analog of keyspace.rs's
//! `scan_spans_a_forced_hashbrown_resize_and_returns_every_key`: the SCAN cursor is
//! ordered by the growth-invariant `scan_hash`, so records physically MOVING between
//! segments (a split) must never skip a key that was present throughout.

use ironcache_storage::{ExpireWrite, Keyspace, NewValue, ScanCursor, Store, UnixMillis};
use ironcache_store::ShardStore;
use std::collections::HashSet;

const NOW: UnixMillis = UnixMillis(1_000);

/// A store with ONE index table per DB, so every insert grows the SAME table.
fn single_slot_store() -> ShardStore {
    ShardStore::new(16).with_slots_per_db(1)
}

fn set(s: &mut ShardStore, key: &[u8]) {
    s.upsert(0, key, NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
}

#[test]
fn single_slot_heavy_growth_keeps_every_key_readable() {
    // 5000 keys into one table: under the dashtable backend that is dozens of segment
    // splits + multiple directory doublings; under hashbrown, several power-of-two
    // resizes. Every key must stay readable, DBSIZE exact, and interleaved deletes must
    // not disturb survivors (splits move records between segments; a stale position
    // would surface here).
    let mut s = single_slot_store();
    let n = 5000;
    for i in 0..n {
        set(&mut s, format!("g:{i}").as_bytes());
    }
    assert_eq!(s.db_len(0), n, "DBSIZE after growth");
    for i in 0..n {
        assert!(
            s.read(0, format!("g:{i}").as_bytes(), NOW).is_some(),
            "key g:{i} unreadable after growth"
        );
    }
    // Delete every third key, then re-verify both populations.
    for i in (0..n).step_by(3) {
        assert!(
            s.delete(0, format!("g:{i}").as_bytes(), NOW),
            "delete g:{i} missed"
        );
    }
    for i in 0..n {
        let present = s.read(0, format!("g:{i}").as_bytes(), NOW).is_some();
        assert_eq!(
            present,
            i % 3 != 0,
            "key g:{i} wrong after interleaved deletes"
        );
    }
}

#[test]
fn scan_spans_forced_single_slot_growth_and_returns_every_initial_key() {
    // The split-mid-SCAN analog of keyspace.rs's forced-resize test: start a scan over a
    // small set, then insert enough keys mid-flight to force heavy index growth in the
    // ONE table the scan is walking. Every key present throughout must be returned at
    // least once: the cursor orders by the growth-invariant scan_hash, so physical record
    // movement (dash splits / hashbrown rehash) must not skip.
    let mut s = single_slot_store();
    let initial: Vec<Vec<u8>> = (0..16).map(|i| format!("s:{i}").into_bytes()).collect();
    for k in &initial {
        set(&mut s, k);
    }
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut cursor = ScanCursor::START;
    let mut grown = false;
    loop {
        let (next, batch) = s.scan_step(0, cursor, 4, NOW, |_, _| true);
        for k in batch {
            seen.insert(k.to_vec());
        }
        if !grown {
            // 2900 inserts into the one table mid-scan: many splits + doublings.
            for i in 100..3000 {
                set(&mut s, format!("s:{i}").as_bytes());
            }
            grown = true;
        }
        if next.is_start() {
            break;
        }
        cursor = next;
    }
    for k in &initial {
        assert!(
            seen.contains(k),
            "key {k:?} present throughout was skipped across index growth"
        );
    }
}

#[test]
fn flush_after_heavy_growth_empties_and_the_table_stays_reusable() {
    // FLUSHDB is collect-every-key-then-remove (no clear()), so it walks a many-segment /
    // many-bucket table and drives each key through the full remove funnel. After the
    // flush the DB must be empty and immediately reusable (fresh inserts land correctly
    // in the retained, grown index).
    let mut s = single_slot_store();
    for i in 0..3000 {
        set(&mut s, format!("f:{i}").as_bytes());
    }
    assert_eq!(s.db_len(0), 3000);
    let flushed = s.flush_db(0);
    assert_eq!(flushed, 3000, "flush must remove every key");
    assert_eq!(s.db_len(0), 0);
    for i in 0..64 {
        set(&mut s, format!("f2:{i}").as_bytes());
    }
    assert_eq!(s.db_len(0), 64);
    for i in 0..64 {
        assert!(
            s.read(0, format!("f2:{i}").as_bytes(), NOW).is_some(),
            "post-flush key f2:{i} unreadable"
        );
    }
}
