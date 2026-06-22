// SPDX-License-Identifier: MIT OR Apache-2.0
//! The PR-6 HASH value over the in-place-mutation RMW mechanism, exercised at the
//! store level (STORAGE_API.md "the RMW in-place-mutation contract", COLLECTIONS.md).
//! These drive the concrete [`ShardStore`] through the additive `rmw_mut` /
//! `OccupiedMut` / `as_hash_mut` / `RmwAction::Mutated` surface and assert:
//!
//! - a Mutated edit grows/shrinks `used_memory` by EXACTLY the field+value delta,
//!   across a random HSET/HDEL/HINCRBY sequence INCLUDING the listpack->hashtable
//!   transition (a property test against a from-scratch recompute of a shadow model);
//! - the encoding flips to `hashtable` at BOTH thresholds (the HASH entry count 512 --
//!   NOT the 128 zset/set cap -- and per-element byte 64) and is a one-way ratchet;
//! - an emptied hash DELETES the key (no empty hash observable);
//! - WRONGTYPE on a string key (the typed view returns None, the handler returns
//!   Keep, no edit / no accounting change).
//!
//! Determinism (ADR-0003): the "random" sequence is a SEEDED in-test splitmix64 (no
//! std::time / no rand crate), so the run is byte-identical on replay.

use ironcache_config::{DEFAULT_HASH_MAX_LISTPACK_ENTRIES, DEFAULT_HASH_MAX_LISTPACK_VALUE};
use ironcache_storage::{
    ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};
use ironcache_store::ShardStore;
use std::collections::BTreeMap;

const NOW: UnixMillis = UnixMillis(0);

/// A from-scratch recompute of the per-shard accounting weight for a SINGLE hash key:
/// `key.len() + sum(field byte len + value byte len)`, matching
/// `KvObj::accounted_bytes()` for a hash. An EMPTY model means the key is DELETED
/// (empty-collection-deletes-key), so the weight is 0 (the key bytes are not resident).
fn expected_bytes(key: &[u8], model: &BTreeMap<Vec<u8>, Vec<u8>>) -> u64 {
    if model.is_empty() {
        return 0;
    }
    let elems: usize = model.iter().map(|(f, v)| f.len() + v.len()).sum();
    (key.len() + elems) as u64
}

/// HSET one field through the store's in-place-mutation arm (create-on-missing via
/// Insert, else Mutated). Mirrors the command-layer handler so the test exercises the
/// real measure-delta path. Returns whether the field was NEW.
fn hset(store: &mut ShardStore, key: &[u8], field: &[u8], value: &[u8]) -> bool {
    let f = field.to_vec();
    let v = value.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::hash(vec![(f, v)])),
            expire: ExpireWrite::Clear,
            reply: true,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let th = o.thresholds();
            let hash = o.as_hash_mut().expect("hash");
            let was_new = hash.set(&f, &v, &th);
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: was_new,
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    })
}

/// HDEL one field (deletes the key if it drains the hash). Returns whether it existed.
fn hdel(store: &mut ShardStore, key: &[u8], field: &[u8]) -> bool {
    let f = field.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let hash = o.as_hash_mut().expect("hash");
            let removed = hash.del(&f);
            let action = if hash.is_empty() {
                RmwAction::Delete
            } else {
                RmwAction::Mutated
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: removed,
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    })
}

/// The current encoding name of a key (or "none" if absent), via the read path.
fn encoding_of(store: &mut ShardStore, key: &[u8]) -> String {
    match store.read(0, key, NOW) {
        Some(v) => v.encoding().encoding_name().to_owned(),
        None => "none".to_owned(),
    }
}

#[test]
fn mutated_tracks_used_memory_exactly_across_a_random_sequence() {
    // A seeded splitmix64 (deterministic; no std::time / no rand crate, ADR-0003) drives
    // a mix of HSET (new + overwrite) and HDEL on a SINGLE key, and we assert
    // used_memory equals a from-scratch recompute of the shadow model after every op,
    // INCLUDING across the listpack->hashtable transition (the per-element 64-byte cap is
    // crossed by the variable-length values below; the entry-count transition is pinned
    // separately and exactly in the dedicated threshold test). The store measures the
    // delta itself (it does not trust the handler), so this pins the
    // measure-before/after-delta + re-account mechanism for hashes.
    let key = b"H";
    let mut store = ShardStore::new(1);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let mut state: u64 = 0x1234_5678_9abc_def0;
    let mut next = || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };

    for _ in 0..4000 {
        let op = next() % 3;
        match op {
            // HSET a field (field id 0..40 so collisions / overwrites happen). The value
            // is variable-length so totals cross the per-element byte cap. The field id
            // space (40) stays under the 512 HASH entry cap, so the byte-cap transition is
            // what this property exercises; the entry-count transition is pinned in the
            // dedicated threshold test.
            0 | 1 => {
                let fid = next() % 40;
                let field = format!("f{fid}").into_bytes();
                let vlen = (next() % 80) as usize; // up to 80 bytes -> crosses the 64 cap
                let value = vec![b'a' + (next() % 26) as u8; vlen];
                let was_new = hset(&mut store, key, &field, &value);
                let prev = model.insert(field, value);
                assert_eq!(was_new, prev.is_none(), "HSET new-vs-overwrite mismatch");
            }
            _ => {
                // HDEL a field (sometimes present, sometimes absent).
                let fid = next() % 40;
                let field = format!("f{fid}").into_bytes();
                let removed = hdel(&mut store, key, &field);
                let had = model.remove(&field).is_some();
                assert_eq!(removed, had, "HDEL removed-vs-present mismatch");
            }
        }

        // The accounting invariant after EVERY op: used_memory == from-scratch recompute.
        let want = expected_bytes(key, &model);
        assert_eq!(
            store.used_memory(),
            want,
            "used_memory drift after op {op} (model len {})",
            model.len()
        );

        // The key is present iff the model is non-empty (empty-deletes-key).
        assert_eq!(
            store.read(0, key, NOW).is_some(),
            !model.is_empty(),
            "key presence tracks non-empty model"
        );
    }
}

#[test]
fn encoding_flips_to_hashtable_at_the_entry_count_threshold() {
    // Crossing the HASH entry cap (512, NOT the 128 zset/set cap; with small field/value
    // bytes, under the byte cap) flips to hashtable. This isolates the ENTRY-COUNT
    // threshold and pins the Redis-correct 512/513 boundary: exactly 512 entries stay
    // listpack, 513 flips to hashtable.
    assert_eq!(
        DEFAULT_HASH_MAX_LISTPACK_ENTRIES, 512,
        "the HASH entry cap is 512 (Redis 7.4 config.c / t_hash.c), not the 128 zset/set cap"
    );
    let key = b"e";
    let mut store = ShardStore::new(1);

    // Add exactly the cap many small fields (512): still listpack at the cap.
    for i in 0..DEFAULT_HASH_MAX_LISTPACK_ENTRIES {
        let f = format!("f{i}").into_bytes();
        hset(&mut store, key, &f, b"v");
    }
    assert_eq!(
        encoding_of(&mut store, key),
        "listpack",
        "at the 512-entry cap, still listpack"
    );

    // One more distinct field (the 513th) crosses the cap -> hashtable.
    hset(&mut store, key, b"over_the_cap_field", b"v");
    assert_eq!(
        encoding_of(&mut store, key),
        "hashtable",
        "over the 512-entry cap (the 513th field) -> hashtable"
    );

    // One-way ratchet: deleting back below the cap stays hashtable (Redis parity).
    for i in 0..DEFAULT_HASH_MAX_LISTPACK_ENTRIES {
        let f = format!("f{i}").into_bytes();
        hdel(&mut store, key, &f);
    }
    assert_eq!(
        encoding_of(&mut store, key),
        "hashtable",
        "the listpack->hashtable transition is a one-way ratchet (Redis parity)"
    );
}

#[test]
fn encoding_flips_to_hashtable_at_the_value_byte_threshold() {
    // A FEW entries but ONE value over the 64-byte per-element cap flips to hashtable.
    // This isolates the PER-ELEMENT BYTE threshold (independent of the entry count).
    let key = b"b";
    let mut store = ShardStore::new(1);

    // A small field with a short value: listpack.
    hset(&mut store, key, b"f1", b"short");
    assert_eq!(encoding_of(&mut store, key), "listpack");

    // A value over the 64-byte cap -> hashtable, even with only 2 entries.
    let big = vec![b'q'; DEFAULT_HASH_MAX_LISTPACK_VALUE + 1];
    hset(&mut store, key, b"f2", &big);
    assert_eq!(
        encoding_of(&mut store, key),
        "hashtable",
        "a value over the 64-byte cap -> hashtable"
    );

    // A long FIELD name (over the cap) also triggers the transition.
    let key2 = b"b2";
    let big_field = vec![b'k'; DEFAULT_HASH_MAX_LISTPACK_VALUE + 1];
    hset(&mut store, key2, &big_field, b"v");
    assert_eq!(
        encoding_of(&mut store, key2),
        "hashtable",
        "a field name over the 64-byte cap -> hashtable"
    );
}

#[test]
fn emptied_hash_deletes_the_key_no_empty_hash_observable() {
    let key = b"d";
    let mut store = ShardStore::new(1);
    hset(&mut store, key, b"only", b"v");
    assert!(store.read(0, key, NOW).is_some());
    // Delete the last field: the key must be GONE (empty-collection-deletes-key).
    let removed = hdel(&mut store, key, b"only");
    assert!(removed);
    assert!(
        store.read(0, key, NOW).is_none(),
        "an emptied hash deletes the key"
    );
    assert_eq!(store.used_memory(), 0, "accounting returns to zero");
}

#[test]
fn wrongtype_on_a_string_key_makes_no_edit() {
    let key = b"s";
    let mut store = ShardStore::new(1);
    store.upsert(0, key, NewValue::Bytes(b"hello"), ExpireWrite::Clear, NOW);
    let before = store.used_memory();

    // A hash edit on a string key: the typed view returns None, so the handler returns
    // Keep with a WRONGTYPE-shaped reply and NO accounting change.
    let saw_non_hash = store.rmw_mut(0, key, NOW, |entry| match entry {
        RmwEntry::OccupiedMut(mut o) => {
            let is_hash = o.as_hash_mut().is_some();
            RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: !is_hash,
            }
        }
        _ => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
    });
    assert!(saw_non_hash, "as_hash_mut returns None on a string key");
    assert_eq!(
        store.used_memory(),
        before,
        "WRONGTYPE makes no accounting change"
    );
    // The string value is untouched.
    assert_eq!(store.read(0, key, NOW).unwrap().as_bytes(), b"hello");
}

#[test]
fn seeded_hash_workload_replays_identically() {
    // Determinism (ADR-0003): the SAME seeded workload run twice on two independent
    // stores produces byte-identical results (used_memory, encoding, sorted contents).
    // Hashes use no RNG in the store, so this pins that a hash workload is fully
    // deterministic on replay, INCLUDING across the listpack->hashtable transition (the
    // hashtable iteration order is sorted by the fixed-seed field hash, ADR-0003).
    // (used_memory, encoding name, the deterministic (field, value) pairs).
    type RunResult = (u64, String, Vec<(Vec<u8>, Vec<u8>)>);
    fn run(seed: u64) -> RunResult {
        let key = b"w";
        let mut store = ShardStore::new(1);
        let mut state = seed;
        let mut next = || {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        for _ in 0..2000 {
            // Mix HSET (driving the hash to hashtable via the per-element byte cap) and
            // HDEL. The vlen below can exceed the 64-byte value cap, which is what flips
            // the encoding here (the 200 field-id space stays under the 512 entry cap).
            match next() % 3 {
                0 | 1 => {
                    let fid = next() % 200;
                    let f = format!("f{fid}").into_bytes();
                    let vlen = (next() % 70) as usize; // up to 69 -> crosses the 64 value cap
                    let v = vec![b'a' + (next() % 26) as u8; vlen];
                    hset(&mut store, key, &f, &v);
                }
                _ => {
                    let fid = next() % 200;
                    let f = format!("f{fid}").into_bytes();
                    hdel(&mut store, key, &f);
                }
            }
        }
        let enc = encoding_of(&mut store, key);
        // Read the contents through the typed view (the deterministic pairs() order).
        let contents = store.rmw_mut(0, key, NOW, |entry| match entry {
            RmwEntry::OccupiedMut(mut o) => {
                let pairs = o.as_hash_mut().map(|h| h.pairs()).unwrap_or_default();
                RmwStep {
                    action: RmwAction::Keep,
                    expire: ExpireWrite::Unchanged,
                    reply: pairs,
                }
            }
            _ => RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: Vec::new(),
            },
        });
        (store.used_memory(), enc, contents)
    }

    let a = run(0xDEAD_BEEF);
    let b = run(0xDEAD_BEEF);
    assert_eq!(a.0, b.0, "used_memory replays identically");
    assert_eq!(a.1, b.1, "encoding replays identically");
    assert_eq!(
        a.2, b.2,
        "hash contents (in deterministic pairs() order) replay identically"
    );
}

/// Area A (#40): `set_encoding_thresholds` on the STORE makes a NEW hash promote to `hashtable`
/// past a LOWERED `hash-max-listpack-entries`, exercised end-to-end through `rmw_mut` (the same
/// path the command layer + the create-on-missing Insert use). An EXISTING hash built before the
/// lowering keeps its encoding (a CONFIG SET is future-only; resident data is never re-encoded).
#[test]
fn store_runtime_threshold_promotes_new_hash_and_leaves_existing_untouched() {
    use ironcache_storage::EncodingThresholds;
    let mut store = ShardStore::new(1);

    // An EXISTING hash built under the DEFAULT (512-entry) cap with 5 small fields: listpack.
    for i in 0..5 {
        hset(&mut store, b"existing", format!("f{i}").as_bytes(), b"v");
    }
    assert_eq!(encoding_of(&mut store, b"existing"), "listpack");

    // Lower the live cap to 4 entries (a `CONFIG SET hash-max-listpack-entries 4` analog).
    store.set_encoding_thresholds(EncodingThresholds {
        hash_max_listpack_entries: 4,
        ..EncodingThresholds::defaults()
    });

    // The EXISTING hash is NOT re-encoded just because the cap dropped (no edit re-evaluates it).
    assert_eq!(
        encoding_of(&mut store, b"existing"),
        "listpack",
        "an existing hash keeps its encoding after a CONFIG SET (future-only)"
    );

    // A NEW hash with 5 fields under the lowered cap promotes to hashtable (5 > 4).
    for i in 0..5 {
        hset(&mut store, b"fresh", format!("f{i}").as_bytes(), b"v");
    }
    assert_eq!(
        encoding_of(&mut store, b"fresh"),
        "hashtable",
        "a new hash past the lowered cap is hashtable"
    );
}
