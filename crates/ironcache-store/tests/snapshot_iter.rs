// SPDX-License-Identifier: MIT OR Apache-2.0
//! The HA-5b SNAPSHOT iterator, exercised at the STORE level. These drive the concrete
//! [`ShardStore`] through the store-public `snapshot_chunk` resumable pull API, asserting:
//!
//! - `snapshot_dump_emits_all_current_keys`: a drained snapshot yields EVERY live key with
//!   its CURRENT value (across mixed dbs, types, and TTLs); replaying the dump into a fresh
//!   store reconstructs the original keyspace.
//! - `snapshot_plus_stream_converges`: a snapshot of a store, plus the post-begin writes the
//!   HA-5a observer would record (create / overwrite / delete), applied snapshot-then-stream
//!   onto a fresh replica, CONVERGES to the live keyspace -- last-write-wins via the stream,
//!   with NO per-key version and NO point-in-time filter. THIS is the correctness proof.
//! - `resumable_chunks_cover_the_keyspace`: a max=1 / max=2 chunked drain WITH concurrent
//!   inserts between chunks still emits every present-throughout key at least once
//!   (at-least-once across a resize).
//! - `constant_memory`: a large shard drained in small chunks never returns a chunk larger
//!   than the budget (the iteration is a bounded pull, not a full-keyspace Vec).
//! - `expired_entries_are_not_emitted`: a lazily-expired key is skipped (no tombstone-as-live).
//! - `empty_shard_snapshot_is_empty`.
//!
//! DETERMINISM (ADR-0003): the snapshot is a pure SCAN dump; `now` is the caller's clock (the
//! store reads none), so the emitted set is a pure function of the live keyspace.

use ironcache_storage::{
    ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};
use ironcache_store::{KvObj, ShardStore, SnapshotCursor};

const NOW: UnixMillis = UnixMillis(1_000);
const DBS: u32 = 4;

/// A post-begin write the HA-5a observer would record (the convergence stream): a put
/// (create/overwrite, on_put post-image), a delete (on_remove), or an in-place set add
/// (on_put post-edit value).
enum Op {
    Put(u32, Vec<u8>, Vec<u8>),
    Del(u32, Vec<u8>),
    SetAdd(u32, Vec<u8>, Vec<u8>),
}

fn store() -> ShardStore {
    ShardStore::new(DBS)
}

/// Blind-set a string key.
fn put(s: &mut ShardStore, db: u32, key: &[u8], val: &[u8]) {
    s.upsert(db, key, NewValue::Bytes(val), ExpireWrite::Clear, NOW);
}

/// Blind-set a string key with a TTL deadline.
fn put_ttl(s: &mut ShardStore, db: u32, key: &[u8], val: &[u8], deadline: u64) {
    s.upsert(
        db,
        key,
        NewValue::Bytes(val),
        ExpireWrite::Set(UnixMillis(deadline)),
        NOW,
    );
}

/// Create a SET key (create-on-missing path).
fn put_set(s: &mut ShardStore, db: u32, key: &[u8], members: &[&[u8]]) {
    let members: Vec<Vec<u8>> = members.iter().map(|m| m.to_vec()).collect();
    s.rmw_mut(db, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::set(members)),
            expire: ExpireWrite::Keep,
            reply: (),
        },
        RmwEntry::Occupied(_) | RmwEntry::OccupiedMut(_) => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Keep,
            reply: (),
        },
    });
}

/// Add a member to an existing SET in place (the in-place Mutated edit path).
fn sadd(s: &mut ShardStore, db: u32, key: &[u8], member: &[u8]) {
    let m = member.to_vec();
    s.rmw_mut(db, key, NOW, move |entry| match entry {
        RmwEntry::OccupiedMut(mut h) => {
            let th = h.thresholds();
            let set = h.as_set_mut().expect("a set");
            set.add(&m, &th);
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Keep,
                reply: (),
            }
        }
        _ => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Keep,
            reply: (),
        },
    });
}

/// Drain the WHOLE snapshot via chunks of `max`, returning every emitted `(db, key, KvObj)`
/// in order. Asserts NO chunk exceeds the budget (constant-memory) and that the drain
/// TERMINATES (the cursor's db_index never regresses).
fn drain_snapshot(s: &ShardStore, max: usize, now: UnixMillis) -> Vec<(u32, Vec<u8>, KvObj)> {
    let databases = DBS as usize;
    let mut cursor = SnapshotCursor::START;
    let mut out = Vec::new();
    let mut guard = 0;
    while !cursor.is_done(databases) {
        let (chunk, next) = s.snapshot_chunk(cursor, max, now);
        assert!(
            chunk.len() <= max.max(1),
            "a chunk ({}) must never exceed the budget ({})",
            chunk.len(),
            max.max(1)
        );
        for (db, key, kv) in chunk {
            out.push((db, key.to_vec(), kv));
        }
        cursor = next;
        guard += 1;
        assert!(guard < 100_000, "snapshot drain did not terminate");
    }
    out
}

/// Insert every emitted `KvObj` into a fresh REPLICA store via `insert_object` (the replay
/// path), reconstructing the snapshot's keyspace. Returns the replica.
fn replay(emitted: &[(u32, Vec<u8>, KvObj)]) -> ShardStore {
    let mut replica = store();
    for (db, _key, kv) in emitted {
        replica.insert_object(*db, kv.clone());
    }
    replica
}

/// The element COUNT of the collection stored at `(db, key)` in `s`, read via a snapshot
/// dump (the snapshot's `KvObj` carries `collection_len`; `ValueRef` does not expose it).
fn coll_len(s: &ShardStore, db: u32, key: &[u8]) -> Option<usize> {
    drain_snapshot(s, 64, NOW)
        .into_iter()
        .find(|(d, k, _)| *d == db && k.as_slice() == key)
        .and_then(|(_, _, kv)| kv.collection_len())
}

#[test]
fn empty_shard_snapshot_is_empty() {
    let s = store();
    let emitted = drain_snapshot(&s, 8, NOW);
    assert!(
        emitted.is_empty(),
        "an empty shard yields no snapshot entries"
    );
}

#[test]
fn snapshot_dump_emits_all_current_keys() {
    let mut s = store();
    // Mixed dbs, types, TTLs.
    put(&mut s, 0, b"s-a", b"alpha");
    put(&mut s, 0, b"s-b", b"123"); // int-encoded
    put_ttl(&mut s, 1, b"s-ttl", b"withttl", 5_000);
    put_set(&mut s, 2, b"set-x", &[b"m1", b"m2", b"m3"]);
    put(&mut s, 3, b"s-d", b"delta");

    let emitted = drain_snapshot(&s, 8, NOW);

    // EVERY live key is emitted exactly once.
    let mut keys: Vec<(u32, Vec<u8>)> = emitted.iter().map(|(d, k, _)| (*d, k.clone())).collect();
    keys.sort();
    let mut want: Vec<(u32, Vec<u8>)> = vec![
        (0, b"s-a".to_vec()),
        (0, b"s-b".to_vec()),
        (1, b"s-ttl".to_vec()),
        (2, b"set-x".to_vec()),
        (3, b"s-d".to_vec()),
    ];
    want.sort();
    assert_eq!(keys, want, "the snapshot yields exactly the live keys");

    // CURRENT values: replay into a fresh replica and read them back == original.
    let mut replica = replay(&emitted);
    assert_eq!(replica.read(0, b"s-a", NOW).unwrap().as_bytes(), b"alpha");
    assert_eq!(replica.read(0, b"s-b", NOW).unwrap().as_bytes(), b"123");
    assert_eq!(
        replica.read(1, b"s-ttl", NOW).unwrap().as_bytes(),
        b"withttl"
    );
    assert_eq!(replica.read(3, b"s-d", NOW).unwrap().as_bytes(), b"delta");
    // The int encoding round-trips (the replica re-classifies the digits as int).
    assert_eq!(
        replica.read(0, b"s-b", NOW).unwrap().encoding(),
        s.read(0, b"s-b", NOW).unwrap().encoding(),
        "the int-encoded value round-trips its encoding"
    );
    // The TTL deadline survived the snapshot reconstruction.
    assert_eq!(
        replica.read(1, b"s-ttl", NOW).unwrap().expire_at(),
        Some(UnixMillis(5_000))
    );
    // The set reconstructs with its three members.
    let set_kv = emitted
        .iter()
        .find(|(d, k, _)| *d == 2 && k == b"set-x")
        .map(|(_, _, kv)| kv)
        .expect("the set entry is emitted");
    assert_eq!(
        set_kv.collection_len(),
        Some(3),
        "the set has three members"
    );
    assert_eq!(
        coll_len(&replica, 2, b"set-x"),
        Some(3),
        "the replayed set has three members"
    );
}

#[test]
fn snapshot_plus_stream_converges() {
    // The CONVERGENCE PROOF: snapshot (unfiltered current values) + the post-begin writes
    // the HA-5a observer records, applied snapshot-THEN-stream onto a fresh replica, equals
    // the live keyspace. NO per-key version, NO point-in-time filter: correctness is
    // last-write-wins via the idempotent stream.
    let mut s = store();
    put(&mut s, 0, b"keep", b"v-keep");
    put(&mut s, 0, b"overwrite", b"v-old");
    put(&mut s, 0, b"delete-me", b"v-del");
    put_set(&mut s, 1, b"set", &[b"a", b"b"]);

    // Snapshot-begin: take the dump (a replica would also capture the stream offset here).
    let emitted = drain_snapshot(&s, 8, NOW);

    // Post-begin writes on the LIVE store; the HA-5a observer would carry each (a
    // create/overwrite is an on_put with the post-image, a delete is an on_remove, an
    // in-place edit is an on_put with the post-edit value). We RECORD them as a simple
    // ordered op list, then replay them onto the replica AFTER the snapshot.
    let mut stream: Vec<Op> = Vec::new();

    // CREATE after begin.
    put(&mut s, 0, b"new-key", b"v-new");
    stream.push(Op::Put(0, b"new-key".to_vec(), b"v-new".to_vec()));
    // OVERWRITE after begin (note: the snapshot may carry the OLD value; the stream's
    // later write wins).
    put(&mut s, 0, b"overwrite", b"v-new-val");
    stream.push(Op::Put(0, b"overwrite".to_vec(), b"v-new-val".to_vec()));
    // DELETE after begin.
    s.delete(0, b"delete-me", NOW);
    stream.push(Op::Del(0, b"delete-me".to_vec()));
    // IN-PLACE collection edit after begin.
    sadd(&mut s, 1, b"set", b"c");
    stream.push(Op::SetAdd(1, b"set".to_vec(), b"c".to_vec()));

    // Replica = snapshot dump, THEN the stream (last-write-wins).
    let mut replica = replay(&emitted);
    for op in &stream {
        match op {
            Op::Put(db, key, val) => {
                replica.upsert(*db, key, NewValue::Bytes(val), ExpireWrite::Clear, NOW);
            }
            Op::Del(db, key) => {
                replica.delete(*db, key, NOW);
            }
            Op::SetAdd(db, key, member) => sadd(&mut replica, *db, key, member),
        }
    }

    // CONVERGENCE: the replica matches the LIVE store key-for-key.
    assert_eq!(replica.read(0, b"keep", NOW).unwrap().as_bytes(), b"v-keep");
    assert_eq!(
        replica.read(0, b"new-key", NOW).unwrap().as_bytes(),
        b"v-new"
    );
    assert_eq!(
        replica.read(0, b"overwrite", NOW).unwrap().as_bytes(),
        b"v-new-val",
        "the stream's later overwrite wins over the snapshot's stale value"
    );
    assert!(
        replica.read(0, b"delete-me", NOW).is_none(),
        "the deleted key is absent after convergence (stream on_remove wins)"
    );
    assert_eq!(
        coll_len(&replica, 1, b"set"),
        Some(3),
        "the in-place set edit converges via the stream (a,b,c)"
    );

    // And it matches the LIVE store for those same keys.
    for (db, key) in [
        (0u32, b"keep".as_slice()),
        (0, b"new-key"),
        (0, b"overwrite"),
        (1, b"set"),
    ] {
        let live = s.read(db, key, NOW);
        let rep = replica.read(db, key, NOW);
        assert_eq!(
            live.as_ref().map(|v| v.as_bytes().to_vec()),
            rep.as_ref().map(|v| v.as_bytes().to_vec()),
            "live and replica agree on db {db} key {key:?}"
        );
    }
    assert!(
        s.read(0, b"delete-me", NOW).is_none() && replica.read(0, b"delete-me", NOW).is_none(),
        "both stores agree the deleted key is gone"
    );
}

#[test]
fn redundant_snapshot_value_is_idempotent_under_stream() {
    // A key the snapshot emits AND the stream re-carries (it was written after begin) must
    // converge: applying the snapshot value then the identical stream value is idempotent.
    let mut s = store();
    put(&mut s, 0, b"k", b"v1");
    let emitted = drain_snapshot(&s, 8, NOW);
    // Stream re-carries the same key (e.g. a touch that did not change the value).
    put(&mut s, 0, b"k", b"v1");

    let mut replica = replay(&emitted);
    replica.upsert(0, b"k", NewValue::Bytes(b"v1"), ExpireWrite::Clear, NOW);
    assert_eq!(
        replica.read(0, b"k", NOW).unwrap().as_bytes(),
        b"v1",
        "a redundant snapshot+stream write is idempotent"
    );
}

#[test]
fn resumable_chunks_cover_the_keyspace() {
    let mut s = store();
    // 50 keys spread across the dbs.
    let mut expected: Vec<(u32, Vec<u8>)> = Vec::new();
    for i in 0..50u32 {
        let db = i % DBS;
        let key = format!("k{i:03}").into_bytes();
        put(&mut s, db, &key, format!("v{i}").as_bytes());
        expected.push((db, key));
    }
    expected.sort();

    // Drain via SMALL chunks (max=2) with a CONCURRENT insert between chunks. The snapshot is
    // at-least-once across a resize: every present-throughout key is covered at least once.
    // (Concurrent inserts MAY or may not be visited; the guaranteed property is that the
    // present-throughout keys are never MISSED.)
    let databases = DBS as usize;
    let mut cursor = SnapshotCursor::START;
    let mut seen: Vec<(u32, Vec<u8>)> = Vec::new();
    let mut concurrent = 0u32;
    let mut guard = 0;
    while !cursor.is_done(databases) {
        let (chunk, next) = s.snapshot_chunk(cursor, 2, NOW);
        assert!(chunk.len() <= 2, "max=2 chunk is bounded by 2");
        for (db, key, _kv) in chunk {
            // Only record the present-throughout keys (the `k###` set); a concurrent key
            // being visited is allowed but not asserted.
            if key.starts_with(b"k") && !key.starts_with(b"concurrent") {
                seen.push((db, key.to_vec()));
            }
        }
        cursor = next;
        // A concurrent insert between chunks. Force a table grow by inserting to exercise
        // resize-stability of the scan_hash order.
        let ckey = format!("concurrent{concurrent:03}").into_bytes();
        put(&mut s, concurrent % DBS, &ckey, b"post-begin");
        concurrent += 1;
        guard += 1;
        assert!(guard < 100_000, "drain terminates");
    }

    // Every present-throughout key is emitted AT LEAST once (dedup, since at-least-once
    // allows duplicates across a resize).
    seen.sort();
    seen.dedup();
    assert_eq!(
        seen, expected,
        "every present-throughout key is covered at least once (none missed)"
    );
}

#[test]
fn resumable_chunks_max_one() {
    // max=1 is the tightest pull: one key per chunk, must still cover everything.
    let mut s = store();
    let mut expected: Vec<(u32, Vec<u8>)> = Vec::new();
    for i in 0..12u32 {
        let db = i % DBS;
        let key = format!("k{i:02}").into_bytes();
        put(&mut s, db, &key, b"v");
        expected.push((db, key));
    }
    expected.sort();

    let emitted = drain_snapshot(&s, 1, NOW);
    let mut got: Vec<(u32, Vec<u8>)> = emitted.iter().map(|(d, k, _)| (*d, k.clone())).collect();
    got.sort();
    got.dedup();
    assert_eq!(got, expected, "max=1 covers the whole keyspace");
}

#[test]
fn constant_memory() {
    // A large shard drained in small chunks: the per-chunk Vec is always <= budget, proving
    // the iteration is a bounded PULL, never a full-keyspace materialization.
    let mut s = store();
    for i in 0..1_000u32 {
        put(
            &mut s,
            i % DBS,
            format!("key{i:04}").into_bytes().as_slice(),
            b"x",
        );
    }

    let databases = DBS as usize;
    let max = 16;
    let mut cursor = SnapshotCursor::START;
    let mut total = 0usize;
    let mut max_chunk = 0usize;
    let mut guard = 0;
    while !cursor.is_done(databases) {
        let (chunk, next) = s.snapshot_chunk(cursor, max, NOW);
        assert!(
            chunk.len() <= max,
            "no chunk exceeds the budget: {} <= {}",
            chunk.len(),
            max
        );
        max_chunk = max_chunk.max(chunk.len());
        total += chunk.len();
        cursor = next;
        guard += 1;
        assert!(guard < 100_000, "drain terminates");
    }
    assert_eq!(
        total, 1_000,
        "every key is emitted across the chunked drain"
    );
    assert!(
        max_chunk <= max,
        "the largest chunk stayed within the bounded budget"
    );
}

#[test]
fn expired_entries_are_not_emitted() {
    // A lazily-expired key must NOT ship as a live snapshot entry (no tombstone-as-live).
    let mut s = store();
    put(&mut s, 0, b"live", b"v");
    put_ttl(&mut s, 0, b"dead", b"v", 500); // deadline 500 < drain `now` 1000 -> expired
    put_ttl(&mut s, 0, b"future", b"v", 5_000); // deadline 5000 > now -> still live

    let emitted = drain_snapshot(&s, 8, NOW);
    let keys: Vec<Vec<u8>> = emitted.iter().map(|(_, k, _)| k.clone()).collect();
    assert!(keys.iter().any(|k| k == b"live"), "the live key is emitted");
    assert!(
        keys.iter().any(|k| k == b"future"),
        "a not-yet-expired key is emitted"
    );
    assert!(
        !keys.iter().any(|k| k == b"dead"),
        "an expired key is skipped, not shipped as live"
    );
}
