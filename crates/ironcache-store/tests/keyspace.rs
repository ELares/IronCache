// SPDX-License-Identifier: MIT OR Apache-2.0
//! Integration tests for the additive [`Keyspace`] seam on the per-shard store
//! (KEYSPACE.md): the hash-ordered SCAN cursor (including resize tolerance), KEYS via
//! scan-to-completion, DBSIZE, RANDOMKEY, RENAME/RENAMENX/COPY/MOVE (value-object
//! preservation), SWAPDB, FLUSHDB/FLUSHALL, and SCAN determinism.
//!
//! The forced equal-hash COLLISION case (two distinct keys sharing a 64-bit hash) is
//! white-box and lives in the store crate's `scan_core_tests` (it needs hand-crafted
//! hashes, which a black-box test cannot construct without inverting `scan_hash`).

use ironcache_storage::{
    DataType, Encoding, ExpireWrite, Keyspace, MoveMode, MoveOutcome, NewValue, ScanCursor, Store,
    UnixMillis,
};
use ironcache_store::ShardStore;
use ironcache_store::kvobj::{Header, KvObj, ValueRepr};
use std::collections::HashSet;

const NOW: UnixMillis = UnixMillis(1_000);

fn store() -> ShardStore {
    ShardStore::new(16)
}

/// Set `key` to a string value in db 0 (no TTL).
fn set(s: &mut ShardStore, key: &[u8]) {
    s.upsert(0, key, NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
}

/// Drive SCAN to completion (cursor 0 -> ... -> 0) with the given COUNT and no filter,
/// returning every key returned (with multiplicity, so a test can assert "at least
/// once"). Asserts the cursor terminates.
fn scan_all(s: &mut ShardStore, db: u32, count: usize, now: UnixMillis) -> Vec<Box<[u8]>> {
    let mut out = Vec::new();
    let mut cursor = ScanCursor::START;
    // A generous bound so a cursor bug fails rather than hangs.
    let mut guard = 0;
    loop {
        let (next, batch) = s.scan_step(db, cursor, count, now, |_k, _t| true);
        out.extend(batch);
        if next.is_start() {
            break;
        }
        cursor = next;
        guard += 1;
        assert!(guard < 100_000, "SCAN did not terminate");
    }
    out
}

#[test]
fn scan_empty_db_completes_immediately() {
    let mut s = store();
    let (next, batch) = s.scan_step(0, ScanCursor::START, 10, NOW, |_, _| true);
    assert!(batch.is_empty());
    assert!(next.is_start(), "empty db -> cursor 0 on the first call");
}

#[test]
fn scan_full_iteration_returns_every_key_once_small_count() {
    let mut s = store();
    let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("key:{i}").into_bytes()).collect();
    for k in &keys {
        set(&mut s, k);
    }
    // COUNT 1 (the smallest) still completes and returns every key exactly once.
    let got = scan_all(&mut s, 0, 1, NOW);
    let got_set: HashSet<Vec<u8>> = got.iter().map(|k| k.to_vec()).collect();
    assert_eq!(got.len(), keys.len(), "no key returned more than once");
    for k in &keys {
        assert!(got_set.contains(k), "missing {k:?}");
    }
}

#[test]
fn scan_under_concurrent_insert_returns_every_stable_key_at_least_once() {
    // The SCAN guarantee: every key present for the ENTIRE scan is returned at least
    // once. We interleave inserts between batches; the originally-present keys must all
    // appear (a mid-scan insert MAY or may not appear, which is within contract).
    let mut s = store();
    let stable: Vec<Vec<u8>> = (0..150).map(|i| format!("s:{i}").into_bytes()).collect();
    for k in &stable {
        set(&mut s, k);
    }
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut cursor = ScanCursor::START;
    let mut extra = 0u32;
    loop {
        let (next, batch) = s.scan_step(0, cursor, 10, NOW, |_, _| true);
        for k in batch {
            seen.insert(k.to_vec());
        }
        // Insert a fresh key mid-scan (added keys may or may not appear; the stable set
        // must still be fully covered).
        set(&mut s, format!("new:{extra}").as_bytes());
        extra += 1;
        if next.is_start() {
            break;
        }
        cursor = next;
    }
    for k in &stable {
        assert!(seen.contains(k), "stable key {k:?} was skipped by SCAN");
    }
}

#[test]
fn scan_under_delete_still_covers_surviving_keys() {
    // Deleting keys mid-scan must not skip a surviving key. We delete a disjoint set of
    // keys between batches; every NON-deleted key must still be returned.
    let mut s = store();
    let all: Vec<Vec<u8>> = (0..120).map(|i| format!("d:{i}").into_bytes()).collect();
    for k in &all {
        set(&mut s, k);
    }
    // Keys we will delete mid-scan (the second half).
    let to_delete: Vec<Vec<u8>> = (60..120).map(|i| format!("d:{i}").into_bytes()).collect();
    let survivors: Vec<Vec<u8>> = (0..60).map(|i| format!("d:{i}").into_bytes()).collect();

    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut cursor = ScanCursor::START;
    let mut del_iter = to_delete.iter();
    loop {
        let (next, batch) = s.scan_step(0, cursor, 8, NOW, |_, _| true);
        for k in batch {
            seen.insert(k.to_vec());
        }
        // Delete a couple of doomed keys each batch.
        for _ in 0..2 {
            if let Some(k) = del_iter.next() {
                s.delete(0, k, NOW);
            }
        }
        if next.is_start() {
            break;
        }
        cursor = next;
    }
    for k in &survivors {
        assert!(seen.contains(k), "surviving key {k:?} skipped under delete");
    }
}

#[test]
fn scan_spans_a_forced_hashbrown_resize_and_returns_every_key() {
    // THE headline rehash-tolerance test: force the hashbrown map to GROW mid-scan
    // (insert enough keys to cross a power-of-two boundary), and assert every key
    // present THROUGHOUT the scan is returned at least once. Because the SCAN cursor is
    // ordered by the resize-invariant `scan_hash`, the order is identical before and
    // after the resize, so no key is skipped.
    let mut s = store();
    // Start with a modest set so the map has a small capacity.
    let initial: Vec<Vec<u8>> = (0..16).map(|i| format!("r:{i}").into_bytes()).collect();
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
            // Insert a LOT of keys in one shot to force at least one all-at-once resize
            // (power-of-two growth) WHILE the scan is in flight.
            for i in 100..900 {
                set(&mut s, format!("r:{i}").as_bytes());
            }
            grown = true;
        }
        if next.is_start() {
            break;
        }
        cursor = next;
    }
    // Every INITIAL key (present throughout the whole scan) must have been returned at
    // least once, despite the resize that happened mid-scan.
    for k in &initial {
        assert!(
            seen.contains(k),
            "key {k:?} present throughout was skipped across the resize"
        );
    }
}

#[test]
fn scan_order_is_identical_across_two_runs_and_a_resize() {
    // Determinism (ADR-0003): the SCAN order is identical on two independent runs of the
    // same key set, and identical whether or not the map has been grown (resize does not
    // reorder, because scan_hash is resize-invariant).
    let build = |grow: bool| -> Vec<Vec<u8>> {
        let mut s = store();
        for i in 0..50 {
            set(&mut s, format!("o:{i}").as_bytes());
        }
        if grow {
            // Grow then shrink back to the same logical set: insert + delete extras, so
            // the table capacity grew but the live set is identical.
            for i in 1000..2000 {
                set(&mut s, format!("x:{i}").as_bytes());
            }
            for i in 1000..2000 {
                s.delete(0, format!("x:{i}").as_bytes(), NOW);
            }
        }
        scan_all(&mut s, 0, 7, NOW)
            .iter()
            .map(|k| k.to_vec())
            .collect()
    };
    let a = build(false);
    let b = build(false);
    let c = build(true);
    assert_eq!(a, b, "SCAN order identical across two runs");
    assert_eq!(a, c, "SCAN order identical across a forced resize");
}

#[test]
fn scan_match_filter_applies_before_clone() {
    let mut s = store();
    for i in 0..30 {
        set(&mut s, format!("user:{i}").as_bytes());
        set(&mut s, format!("post:{i}").as_bytes());
    }
    // Only the `user:*` keys (the keep filter is the MATCH glob, applied here directly).
    let mut out = Vec::new();
    let mut cursor = ScanCursor::START;
    loop {
        let (next, batch) = s.scan_step(0, cursor, 5, NOW, |k, _t| k.starts_with(b"user:"));
        out.extend(batch);
        if next.is_start() {
            break;
        }
        cursor = next;
    }
    assert_eq!(out.len(), 30, "exactly the 30 user:* keys");
    assert!(out.iter().all(|k| k.starts_with(b"user:")));
}

#[test]
fn scan_type_filter_selects_by_data_type() {
    // Plant a non-string (List) value via insert_object, plus several strings, and TYPE
    // -filter to the List type.
    let mut s = store();
    for i in 0..10 {
        set(&mut s, format!("str:{i}").as_bytes());
    }
    let mut lst = KvObj::from_bytes(b"mylist", b"x", None);
    lst.header = Header {
        data_type: DataType::List,
        encoding: Encoding::ListPack,
        eviction_rank: 0,
        ttl_present: false,
        snapshot_version: 0,
    };
    lst.value = ValueRepr::Inline(Box::from(&b"x"[..]));
    s.insert_object(0, lst);

    let mut out = Vec::new();
    let mut cursor = ScanCursor::START;
    loop {
        let (next, batch) = s.scan_step(0, cursor, 4, NOW, |_k, t| t == DataType::List);
        out.extend(batch);
        if next.is_start() {
            break;
        }
        cursor = next;
    }
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].as_ref(), b"mylist");
}

#[test]
fn scan_skips_lazily_expired_keys() {
    // A key whose deadline has passed is NOT returned by SCAN (the lazy backstop / active
    // drain reclaim it). Plant a live key and an expired key.
    let mut s = store();
    s.upsert(
        0,
        b"live",
        NewValue::Bytes(b"v"),
        ExpireWrite::Clear,
        UnixMillis(0),
    );
    s.upsert(
        0,
        b"dead",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(UnixMillis(10)),
        UnixMillis(0),
    );
    // At now=100 the "dead" key is expired.
    let got = scan_all(&mut s, 0, 10, UnixMillis(100));
    let set: HashSet<Vec<u8>> = got.iter().map(|k| k.to_vec()).collect();
    assert!(set.contains(b"live".as_slice()));
    assert!(
        !set.contains(b"dead".as_slice()),
        "expired key not returned by SCAN"
    );
}

#[test]
fn db_len_is_raw_and_does_not_active_expire() {
    // DBSIZE returns the RAW dict size (Redis does not active-expire on DBSIZE): an
    // expired-but-not-reaped key still counts.
    let mut s = store();
    s.upsert(
        0,
        b"a",
        NewValue::Bytes(b"v"),
        ExpireWrite::Clear,
        UnixMillis(0),
    );
    s.upsert(
        0,
        b"b",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(UnixMillis(10)),
        UnixMillis(0),
    );
    // Even at now well past b's deadline, db_len counts both (no active expiry).
    assert_eq!(s.db_len(0), 2);
}

#[test]
fn random_key_returns_a_member_and_nil_on_empty() {
    let mut s = store();
    assert!(s.random_key(0, 12345, NOW).is_none(), "empty db -> None");
    for i in 0..20 {
        set(&mut s, format!("m:{i}").as_bytes());
    }
    // Several picks all return a live member.
    for pick in [0u64, 1, 7, 999, u64::MAX] {
        let k = s.random_key(0, pick, NOW).expect("a member");
        assert!(s.contains(0, &k, NOW), "returned key {k:?} is live");
    }
}

#[test]
fn random_key_is_deterministic_for_a_given_pick() {
    // The same `pick` against the same key set yields the same key (ADR-0003: the store
    // reads no RNG; randomness is the caller's pick, so a seeded replay is identical).
    let mut s = store();
    for i in 0..20 {
        set(&mut s, format!("m:{i}").as_bytes());
    }
    let a = s.random_key(0, 42, NOW);
    let b = s.random_key(0, 42, NOW);
    assert_eq!(a, b, "same pick -> same key");
}

#[test]
fn random_key_skips_an_expired_pick() {
    // If the picked position lands on an expired key, RANDOMKEY probes onward and
    // returns a live key (never None while live keys remain).
    let mut s = store();
    // One live key, several expired.
    s.upsert(
        0,
        b"live",
        NewValue::Bytes(b"v"),
        ExpireWrite::Clear,
        UnixMillis(0),
    );
    for i in 0..10 {
        s.upsert(
            0,
            format!("dead:{i}").as_bytes(),
            NewValue::Bytes(b"v"),
            ExpireWrite::Set(UnixMillis(10)),
            UnixMillis(0),
        );
    }
    // At now=100 only "live" is live; every pick must return it.
    for pick in 0..20u64 {
        assert_eq!(
            s.random_key(0, pick, UnixMillis(100)).as_deref(),
            Some(&b"live"[..]),
            "pick {pick} must skip expired and return the live key"
        );
    }
}

#[test]
fn rename_preserves_value_encoding_and_ttl() {
    let mut s = store();
    // An int-encoded value with a TTL.
    s.upsert(
        0,
        b"src",
        NewValue::Bytes(b"12345"),
        ExpireWrite::Set(UnixMillis(50_000)),
        UnixMillis(0),
    );
    assert_eq!(
        s.read(0, b"src", UnixMillis(0)).unwrap().encoding(),
        Encoding::Int
    );

    let out = s.move_object(0, b"src", 0, b"dst", MoveMode::Rename, true, UnixMillis(0));
    assert_eq!(out, MoveOutcome::Moved);
    // Source gone, destination holds the value object INTACT (encoding + TTL preserved).
    assert!(s.read(0, b"src", UnixMillis(0)).is_none());
    let v = s.read(0, b"dst", UnixMillis(0)).expect("dst present");
    assert_eq!(v.as_bytes(), b"12345");
    assert_eq!(
        v.encoding(),
        Encoding::Int,
        "encoding preserved across rename"
    );
    assert_eq!(v.expire_at(), Some(UnixMillis(50_000)), "TTL preserved");
    // The TTL still expires at the same deadline.
    assert!(
        s.read(0, b"dst", UnixMillis(50_000)).is_some(),
        "alive at deadline"
    );
    assert!(
        s.read(0, b"dst", UnixMillis(50_001)).is_none(),
        "expired past deadline"
    );
}

#[test]
fn rename_missing_source_is_no_source() {
    let mut s = store();
    let out = s.move_object(0, b"nope", 0, b"dst", MoveMode::Rename, true, NOW);
    assert_eq!(out, MoveOutcome::NoSource);
}

#[test]
fn renamenx_declines_when_destination_exists() {
    let mut s = store();
    set(&mut s, b"src");
    set(&mut s, b"dst");
    // replace=false (RENAMENX): destination occupied -> DestExists, nothing moved.
    let out = s.move_object(0, b"src", 0, b"dst", MoveMode::Rename, false, NOW);
    assert_eq!(out, MoveOutcome::DestExists);
    assert!(s.contains(0, b"src", NOW), "src untouched on decline");
}

#[test]
fn copy_leaves_source_and_replace_controls_overwrite() {
    let mut s = store();
    s.upsert(
        0,
        b"src",
        NewValue::Bytes(b"hello"),
        ExpireWrite::Clear,
        NOW,
    );
    // COPY without an existing dest -> Copied, source remains.
    let out = s.move_object(0, b"src", 0, b"dst", MoveMode::Copy, false, NOW);
    assert_eq!(out, MoveOutcome::Copied);
    assert!(s.contains(0, b"src", NOW), "COPY leaves the source");
    assert_eq!(s.read(0, b"dst", NOW).unwrap().as_bytes(), b"hello");

    // COPY onto an existing dest WITHOUT replace -> DestExists.
    s.upsert(0, b"dst", NewValue::Bytes(b"old"), ExpireWrite::Clear, NOW);
    let out = s.move_object(0, b"src", 0, b"dst", MoveMode::Copy, false, NOW);
    assert_eq!(out, MoveOutcome::DestExists);
    assert_eq!(
        s.read(0, b"dst", NOW).unwrap().as_bytes(),
        b"old",
        "not overwritten"
    );

    // COPY onto an existing dest WITH replace -> Copied, overwritten.
    let out = s.move_object(0, b"src", 0, b"dst", MoveMode::Copy, true, NOW);
    assert_eq!(out, MoveOutcome::Copied);
    assert_eq!(s.read(0, b"dst", NOW).unwrap().as_bytes(), b"hello");
}

#[test]
fn move_relocates_across_dbs_and_noops_when_dest_occupied() {
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    // MOVE k to db 1 (replace=false): relocated, gone from db 0.
    let out = s.move_object(0, b"k", 1, b"k", MoveMode::Rename, false, NOW);
    assert_eq!(out, MoveOutcome::Moved);
    assert!(s.read(0, b"k", NOW).is_none(), "moved out of db 0");
    assert_eq!(
        s.read(1, b"k", NOW).unwrap().as_bytes(),
        b"v",
        "present in db 1"
    );

    // MOVE again from db 0 (now empty) -> NoSource.
    let out = s.move_object(0, b"k", 1, b"k", MoveMode::Rename, false, NOW);
    assert_eq!(out, MoveOutcome::NoSource);

    // A fresh key in db 0; MOVE to db 1 where the key already exists -> DestExists (no-op).
    s.upsert(0, b"k", NewValue::Bytes(b"v2"), ExpireWrite::Clear, NOW);
    let out = s.move_object(0, b"k", 1, b"k", MoveMode::Rename, false, NOW);
    assert_eq!(out, MoveOutcome::DestExists);
    assert!(s.contains(0, b"k", NOW), "source kept when dest occupied");
}

#[test]
fn swap_db_exchanges_contents_in_o1() {
    let mut s = store();
    s.upsert(0, b"a", NewValue::Bytes(b"in0"), ExpireWrite::Clear, NOW);
    s.upsert(1, b"b", NewValue::Bytes(b"in1"), ExpireWrite::Clear, NOW);
    s.swap_db(0, 1);
    // db 0 now holds what was in db 1, and vice versa.
    assert!(s.read(0, b"a", NOW).is_none());
    assert_eq!(s.read(0, b"b", NOW).unwrap().as_bytes(), b"in1");
    assert_eq!(s.read(1, b"a", NOW).unwrap().as_bytes(), b"in0");
    assert!(s.read(1, b"b", NOW).is_none());
}

#[test]
fn flush_db_and_flush_all_empty_the_right_scope_and_account() {
    let mut s = store();
    for i in 0..30 {
        set(&mut s, format!("k0:{i}").as_bytes());
    }
    for i in 0..10 {
        s.upsert(
            1,
            format!("k1:{i}").as_bytes(),
            NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
    }
    let before = s.used_memory();
    assert!(before > 0);

    // FLUSHDB empties only db 0; db 1 untouched.
    let removed0 = s.flush_db(0);
    assert_eq!(removed0, 30);
    assert_eq!(s.db_len(0), 0);
    assert_eq!(s.db_len(1), 10, "FLUSHDB scoped to one db");

    // FLUSHALL empties everything and drops the accounting to zero.
    let removed_all = s.flush_all();
    assert_eq!(removed_all, 10);
    assert!(s.is_empty());
    assert_eq!(
        s.used_memory(),
        0,
        "accounting drops to zero after FLUSHALL"
    );
}

#[test]
fn keys_via_scan_to_completion_equals_the_live_set() {
    // KEYS == the SCAN-completed set (the command layer loops scan_step; here we mimic
    // it directly to assert the store-level equivalence).
    let mut s = store();
    let keys: Vec<Vec<u8>> = (0..77).map(|i| format!("k:{i}").into_bytes()).collect();
    for k in &keys {
        set(&mut s, k);
    }
    let scanned: HashSet<Vec<u8>> = scan_all(&mut s, 0, 10, NOW)
        .iter()
        .map(|k| k.to_vec())
        .collect();
    let expected: HashSet<Vec<u8>> = keys.iter().cloned().collect();
    assert_eq!(scanned, expected);
}
