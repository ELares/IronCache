// SPDX-License-Identifier: MIT OR Apache-2.0
//! Integration tests for the per-shard store against the storage-waist contract:
//! the four primitives, encoding classification through the store, hashbrown
//! grow/rehash, and the lazy expiry-on-read backstop (STORAGE_API.md, HASHTABLE.md,
//! EXPIRATION.md).

use ironcache_storage::{
    DataType, Encoding, ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store,
    UnixMillis,
};
use ironcache_store::ShardStore;

const NOW: UnixMillis = UnixMillis(1_000);

fn store() -> ShardStore {
    ShardStore::new(16)
}

#[test]
fn read_absent_is_none() {
    let mut s = store();
    assert!(s.read(0, b"missing", NOW).is_none());
}

#[test]
fn upsert_then_read_round_trips() {
    let mut s = store();
    let existed = s.upsert(0, b"k", NewValue::Bytes(b"hello"), ExpireWrite::Clear, NOW);
    assert!(!existed, "first upsert: no prior live key");
    let v = s.read(0, b"k", NOW).expect("present after upsert");
    assert_eq!(v.as_bytes(), b"hello");
    assert_eq!(v.data_type(), DataType::String);
    assert_eq!(v.encoding(), Encoding::EmbStr);
    // A second upsert reports the prior live key existed.
    assert!(s.upsert(0, b"k", NewValue::Bytes(b"world"), ExpireWrite::Clear, NOW));
    assert_eq!(s.read(0, b"k", NOW).unwrap().as_bytes(), b"world");
}

#[test]
fn upsert_numeric_string_is_int_encoded() {
    let mut s = store();
    s.upsert(0, b"n", NewValue::Bytes(b"12345"), ExpireWrite::Clear, NOW);
    let v = s.read(0, b"n", NOW).unwrap();
    assert_eq!(v.encoding(), Encoding::Int);
    assert_eq!(v.as_bytes(), b"12345"); // command layer always sees bytes
}

#[test]
fn upsert_int_value_variant_stores_int() {
    let mut s = store();
    s.upsert(0, b"n", NewValue::Int(-42), ExpireWrite::Clear, NOW);
    let v = s.read(0, b"n", NOW).unwrap();
    assert_eq!(v.encoding(), Encoding::Int);
    assert_eq!(v.as_bytes(), b"-42");
}

#[test]
fn delete_returns_existed() {
    let mut s = store();
    assert!(!s.delete(0, b"k", NOW), "deleting absent key");
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    assert!(s.delete(0, b"k", NOW), "deleting present key");
    assert!(s.read(0, b"k", NOW).is_none());
    assert!(!s.delete(0, b"k", NOW), "second delete: gone");
}

#[test]
fn rmw_vacant_inserts() {
    let mut s = store();
    let reply: i64 = s.rmw(0, b"k", NOW, |e| match e {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::bytes(&b"new"[..])),
            expire: ExpireWrite::Clear,
            reply: 1,
        },
        RmwEntry::Occupied(_) => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: 0,
        },
        // The read-only `rmw` never yields the in-place-mutation arm (PR-5).
        RmwEntry::OccupiedMut(_) => unreachable!("rmw never yields OccupiedMut"),
    });
    assert_eq!(reply, 1);
    assert_eq!(s.read(0, b"k", NOW).unwrap().as_bytes(), b"new");
}

#[test]
fn rmw_occupied_mutates_and_replies() {
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"old"), ExpireWrite::Clear, NOW);
    // Observe the old value (atomically) and replace it, replying with the old.
    let old: Vec<u8> = s.rmw(0, b"k", NOW, |e| match e {
        RmwEntry::Occupied(o) => RmwStep {
            action: RmwAction::Replace(NewValueOwned::bytes(&b"new"[..])),
            expire: ExpireWrite::Clear,
            reply: o.as_bytes().to_vec(),
        },
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: Vec::new(),
        },
        RmwEntry::OccupiedMut(_) => unreachable!("rmw never yields OccupiedMut"),
    });
    assert_eq!(old, b"old");
    assert_eq!(s.read(0, b"k", NOW).unwrap().as_bytes(), b"new");
}

#[test]
fn rmw_observe_and_write_are_atomic() {
    // The closure observes the pre-write value AND decides the write in one call;
    // there is no window where another op could interleave (single-owner core).
    let mut s = store();
    s.upsert(0, b"c", NewValue::Int(10), ExpireWrite::Clear, NOW);
    // Increment-style: read the int, write +1, reply the new value.
    let new: i64 = s.rmw(0, b"c", NOW, |e| {
        let cur = match e {
            RmwEntry::Occupied(o) => std::str::from_utf8(o.as_bytes())
                .unwrap()
                .parse::<i64>()
                .unwrap(),
            RmwEntry::Vacant => 0,
            RmwEntry::OccupiedMut(_) => unreachable!("rmw never yields OccupiedMut"),
        };
        let next = cur + 1;
        RmwStep {
            action: RmwAction::Replace(NewValueOwned::Int(next)),
            expire: ExpireWrite::Unchanged,
            reply: next,
        }
    });
    assert_eq!(new, 11);
    let v = s.read(0, b"c", NOW).unwrap();
    assert_eq!(v.as_bytes(), b"11");
    assert_eq!(v.encoding(), Encoding::Int);
}

#[test]
fn rmw_delete_removes() {
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let removed: bool = s.rmw(0, b"k", NOW, |e| RmwStep {
        action: RmwAction::Delete,
        expire: ExpireWrite::Unchanged,
        reply: matches!(e, RmwEntry::Occupied(_)),
    });
    assert!(removed);
    assert!(s.read(0, b"k", NOW).is_none());
}

#[test]
fn contains_and_type_of() {
    let mut s = store();
    assert!(!s.contains(0, b"k", NOW));
    assert_eq!(s.type_of(0, b"k", NOW), None);
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    assert!(s.contains(0, b"k", NOW));
    assert_eq!(s.type_of(0, b"k", NOW), Some(DataType::String));
}

#[test]
fn per_db_keyspace_is_isolated() {
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"db0"), ExpireWrite::Clear, NOW);
    s.upsert(1, b"k", NewValue::Bytes(b"db1"), ExpireWrite::Clear, NOW);
    assert_eq!(s.read(0, b"k", NOW).unwrap().as_bytes(), b"db0");
    assert_eq!(s.read(1, b"k", NOW).unwrap().as_bytes(), b"db1");
    assert!(s.read(2, b"k", NOW).is_none());
}

#[test]
fn strlen_of_int_is_decimal_length() {
    let mut s = store();
    s.upsert(0, b"n", NewValue::Bytes(b"-12345"), ExpireWrite::Clear, NOW);
    let v = s.read(0, b"n", NOW).unwrap();
    assert_eq!(v.len(), 6); // "-12345" is six bytes
    assert_eq!(v.encoding(), Encoding::Int);
}

// -- hashbrown grow / rehash: insert past initial capacity, force a resize, and
// confirm every key is still readable (HASHTABLE.md acceptance: a resize touches
// the table but loses no key). --

#[test]
fn grow_and_rehash_keeps_all_keys_readable() {
    const N: usize = 5_000;
    let mut s = store();
    for i in 0..N {
        let key = format!("key:{i}");
        let val = format!("val:{i}");
        s.upsert(
            0,
            key.as_bytes(),
            NewValue::Bytes(val.as_bytes()),
            ExpireWrite::Clear,
            NOW,
        );
    }
    assert_eq!(s.len(), N);
    for i in 0..N {
        let key = format!("key:{i}");
        let val = format!("val:{i}");
        let v = s
            .read(0, key.as_bytes(), NOW)
            .unwrap_or_else(|| panic!("missing {key}"));
        assert_eq!(v.as_bytes(), val.as_bytes());
    }
}

// -- lazy expiry-on-read backstop (EXPIRATION.md): set a deadline, advance `now`
// past it, confirm read -> None and the key is gone (removed, not just hidden). --

#[test]
fn lazy_expiry_removes_on_read_after_deadline() {
    let mut s = store();
    // Deadline at t=1000; written at "now" earlier.
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(UnixMillis(1_000)),
        UnixMillis(500),
    );
    // Before the deadline: live.
    assert!(s.read(0, b"k", UnixMillis(999)).is_some());
    assert_eq!(s.len(), 1);
    // AT the deadline: still live (Valkey boundary is `now > deadline`).
    assert!(s.read(0, b"k", UnixMillis(1_000)).is_some());
    assert_eq!(s.len(), 1);
    // One past the deadline: absent and removed.
    assert!(s.read(0, b"k", UnixMillis(1_001)).is_none());
    assert_eq!(s.len(), 0, "expired key must be removed, not just hidden");
    // A subsequent read with a smaller `now` still sees nothing (it is gone).
    assert!(s.read(0, b"k", UnixMillis(1)).is_none());
}

#[test]
fn lazy_expiry_applies_to_contains_type_delete() {
    let mut s = store();
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(UnixMillis(100)),
        UnixMillis(0),
    );
    // contains, type_of, and delete all honor the backstop.
    assert!(!s.contains(0, b"k", UnixMillis(200)));
    assert_eq!(s.len(), 0);
    s.upsert(
        0,
        b"k2",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(UnixMillis(100)),
        UnixMillis(0),
    );
    assert_eq!(s.type_of(0, b"k2", UnixMillis(200)), None);
    s.upsert(
        0,
        b"k3",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(UnixMillis(100)),
        UnixMillis(0),
    );
    assert!(
        !s.delete(0, b"k3", UnixMillis(200)),
        "expired key DEL reports not-existed"
    );
    assert!(s.is_empty());
}

#[test]
fn keepttl_preserves_deadline_clear_removes_it() {
    let mut s = store();
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"a"),
        ExpireWrite::Set(UnixMillis(5_000)),
        NOW,
    );
    assert_eq!(
        s.read(0, b"k", NOW).unwrap().expire_at(),
        Some(UnixMillis(5_000))
    );
    // KEEPTTL: overwrite value, keep the deadline.
    s.upsert(0, b"k", NewValue::Bytes(b"b"), ExpireWrite::Keep, NOW);
    let v = s.read(0, b"k", NOW).unwrap();
    assert_eq!(v.as_bytes(), b"b");
    assert_eq!(v.expire_at(), Some(UnixMillis(5_000)));
    // Default SET (Clear): overwrite value, drop the deadline.
    s.upsert(0, b"k", NewValue::Bytes(b"c"), ExpireWrite::Clear, NOW);
    assert_eq!(s.read(0, b"k", NOW).unwrap().expire_at(), None);
}

#[test]
fn used_memory_tracks_inserts_and_deletes() {
    let mut s = store();
    assert_eq!(s.used_memory(), 0);
    s.upsert(
        0,
        b"key",
        NewValue::Bytes(b"value"),
        ExpireWrite::Clear,
        NOW,
    ); // 3 + 5
    assert_eq!(s.used_memory(), 8);
    s.upsert(0, b"k2", NewValue::Bytes(b"xy"), ExpireWrite::Clear, NOW); // 2 + 2
    assert_eq!(s.used_memory(), 12);
    // Overwrite swaps the value weight.
    s.upsert(0, b"key", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW); // 3 + 1
    assert_eq!(s.used_memory(), 4 + 4); // (3+1) + (2+2)
    s.delete(0, b"key", NOW);
    s.delete(0, b"k2", NOW);
    assert_eq!(s.used_memory(), 0);
}
