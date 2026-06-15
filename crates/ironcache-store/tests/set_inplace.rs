// SPDX-License-Identifier: MIT OR Apache-2.0
//! The PR-7 SET value over the in-place-mutation RMW mechanism, exercised at the store
//! level (STORAGE_API.md "the RMW in-place-mutation contract", COLLECTIONS.md). These
//! drive the concrete [`ShardStore`] through the additive `rmw_mut` / `OccupiedMut` /
//! `as_set_mut` / `RmwAction::Mutated` surface and assert:
//!
//! - a Mutated edit grows/shrinks `used_memory` by EXACTLY the member-byte delta, across
//!   a random SADD/SREM/SPOP sequence INCLUDING the intset->listpack and ->hashtable
//!   transitions (a property test against a from-scratch recompute of a shadow model);
//! - the encoding ladder fires at each Redis threshold (all-integer stays intset; a
//!   non-integer member -> listpack; an integer set over set-max-intset-entries (512) ->
//!   straight to hashtable; a listpack over set-max-listpack-entries (128) or over the
//!   64-byte per-member cap -> hashtable) and is a one-way ratchet;
//! - an emptied set DELETES the key (no empty set observable);
//! - WRONGTYPE on a string key (the typed view returns None, the handler returns Keep,
//!   no edit / no accounting change);
//! - SPOP determinism: a seeded index-draw replays byte-identically AND survives a resize
//!   (the intset->hashtable transition mid-sequence).
//!
//! Determinism (ADR-0003): the "random" sequence and the SPOP index draws are a SEEDED
//! in-test splitmix64 (no std::time / no rand crate), so the run is byte-identical on
//! replay. The store reads no RNG; the caller draws the indices.

use ironcache_config::{
    DEFAULT_SET_MAX_INTSET_ENTRIES, DEFAULT_SET_MAX_LISTPACK_ENTRIES,
    DEFAULT_SET_MAX_LISTPACK_VALUE,
};
use ironcache_storage::{
    ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};
use ironcache_store::ShardStore;
use std::collections::BTreeSet;

const NOW: UnixMillis = UnixMillis(0);

/// A from-scratch recompute of the per-shard accounting weight for a SINGLE set key:
/// `key.len() + sum(member byte len)`, matching `KvObj::accounted_bytes()` for a set
/// (for canonical-integer members the stored byte form equals the member bytes, so the
/// model can sum the member bytes directly). An EMPTY model means the key is DELETED
/// (empty-collection-deletes-key), so the weight is 0.
fn expected_bytes(key: &[u8], model: &BTreeSet<Vec<u8>>) -> u64 {
    if model.is_empty() {
        return 0;
    }
    let elems: usize = model.iter().map(Vec::len).sum();
    (key.len() + elems) as u64
}

/// SADD one member through the store's in-place-mutation arm (create-on-missing via
/// Insert, else Mutated). Mirrors the command-layer handler so the test exercises the
/// real measure-delta path. Returns whether the member was NEW.
fn sadd(store: &mut ShardStore, key: &[u8], member: &[u8]) -> bool {
    let m = member.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::set(vec![m])),
            expire: ExpireWrite::Clear,
            reply: true,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let set = o.as_set_mut().expect("set");
            let was_new = set.add(&m);
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: was_new,
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    })
}

/// SREM one member (deletes the key if it drains the set). Returns whether it existed.
fn srem(store: &mut ShardStore, key: &[u8], member: &[u8]) -> bool {
    let m = member.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let set = o.as_set_mut().expect("set");
            let removed = set.remove(&m);
            let action = if set.is_empty() {
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

/// SPOP one member: the CALLER draws an index (Env-rng seam analog) and the closure pops
/// the member at that index of the deterministic `members()` order, deleting the key if it
/// drains the set. Returns the popped member, or None if absent. Mirrors the SPOP handler.
fn spop_at(store: &mut ShardStore, key: &[u8], pick: u64) -> Option<Vec<u8>> {
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: None,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let set = o.as_set_mut().expect("set");
            let members = set.members();
            if members.is_empty() {
                return RmwStep {
                    action: RmwAction::Keep,
                    expire: ExpireWrite::Unchanged,
                    reply: None,
                };
            }
            let idx = (pick % members.len() as u64) as usize;
            let chosen = members[idx].clone();
            set.remove(&chosen);
            let action = if set.is_empty() {
                RmwAction::Delete
            } else {
                RmwAction::Mutated
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: Some(chosen),
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

/// A seeded splitmix64 step (deterministic; no std::time / no rand crate, ADR-0003).
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[test]
fn mutated_tracks_used_memory_exactly_across_a_random_sequence() {
    // A seeded splitmix64 drives a mix of SADD (new + dup) / SREM / SPOP on a SINGLE key,
    // and we assert used_memory equals a from-scratch recompute of the shadow model after
    // every op, INCLUDING across the intset->listpack and ->hashtable transitions. The
    // store measures the delta itself (it does not trust the handler), so this pins the
    // measure-before/after-delta + re-account mechanism for sets.
    let key = b"S";
    let mut store = ShardStore::new(1);
    let mut model: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut state: u64 = 0x1234_5678_9abc_def0;

    for _ in 0..5000 {
        let op = splitmix(&mut state) % 4;
        match op {
            // SADD an integer member (drives intset growth) OR sometimes a non-integer
            // member (forces the listpack/hashtable forms). The id space (300) crosses the
            // 128 listpack-entries cap so the byte/entry transitions both fire.
            0 | 1 => {
                let member = if splitmix(&mut state) % 5 == 0 {
                    // Occasionally a non-integer / variable-length member (crosses the
                    // 64-byte per-member cap sometimes, and forces listpack/hashtable).
                    let vlen = (splitmix(&mut state) % 80) as usize;
                    let mut m = vec![b's'; vlen];
                    m.push(b'x'); // ensure non-integer
                    m
                } else {
                    let id = splitmix(&mut state) % 300;
                    id.to_string().into_bytes()
                };
                let was_new = sadd(&mut store, key, &member);
                let inserted = model.insert(member);
                assert_eq!(was_new, inserted, "SADD new-vs-dup mismatch");
            }
            2 => {
                // SREM a member (sometimes present, sometimes absent).
                let id = splitmix(&mut state) % 300;
                let member = id.to_string().into_bytes();
                let removed = srem(&mut store, key, &member);
                let had = model.remove(&member);
                assert_eq!(removed, had, "SREM removed-vs-present mismatch");
            }
            _ => {
                // SPOP a random member (caller draws the index).
                let pick = splitmix(&mut state);
                let popped = spop_at(&mut store, key, pick);
                match popped {
                    Some(m) => assert!(model.remove(&m), "SPOP popped a non-member"),
                    None => assert!(model.is_empty(), "SPOP nil only on an empty set"),
                }
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
fn encoding_ladder_intset_listpack_hashtable() {
    // All-integer + small -> intset. A non-integer member -> listpack. An integer set
    // over set-max-intset-entries (512) -> straight to hashtable (512 > 128).
    assert_eq!(DEFAULT_SET_MAX_INTSET_ENTRIES, 512);
    assert_eq!(DEFAULT_SET_MAX_LISTPACK_ENTRIES, 128);
    assert_eq!(DEFAULT_SET_MAX_LISTPACK_VALUE, 64);

    // intset.
    let key = b"i";
    let mut store = ShardStore::new(1);
    sadd(&mut store, key, b"1");
    sadd(&mut store, key, b"2");
    assert_eq!(encoding_of(&mut store, key), "intset");

    // A non-integer member -> listpack (still small).
    sadd(&mut store, key, b"hello");
    assert_eq!(encoding_of(&mut store, key), "listpack");

    // An integer set over 512 entries -> hashtable (straight, no listpack).
    let key2 = b"big_int";
    let mut store2 = ShardStore::new(1);
    for n in 0..=DEFAULT_SET_MAX_INTSET_ENTRIES {
        sadd(&mut store2, key2, n.to_string().as_bytes());
    }
    assert_eq!(
        encoding_of(&mut store2, key2),
        "hashtable",
        "an integer set past 512 entries goes straight to hashtable (512 > 128)"
    );
    // Exactly 512 integer entries stays intset.
    let key3 = b"at_cap";
    let mut store3 = ShardStore::new(1);
    for n in 0..DEFAULT_SET_MAX_INTSET_ENTRIES {
        sadd(&mut store3, key3, n.to_string().as_bytes());
    }
    assert_eq!(encoding_of(&mut store3, key3), "intset");
}

#[test]
fn encoding_listpack_thresholds_and_one_way_ratchet() {
    // A listpack set over 128 entries -> hashtable; over the 64-byte per-member cap ->
    // hashtable; both transitions are a one-way ratchet (no demote on shrink).

    // Entry-count transition.
    let key = b"lp_entries";
    let mut store = ShardStore::new(1);
    sadd(&mut store, key, b"x"); // non-integer -> listpack
    assert_eq!(encoding_of(&mut store, key), "listpack");
    for i in 0..DEFAULT_SET_MAX_LISTPACK_ENTRIES {
        sadd(&mut store, key, format!("m{i}").as_bytes());
    }
    assert_eq!(
        encoding_of(&mut store, key),
        "hashtable",
        "over the 128 listpack-entries cap -> hashtable"
    );
    // One-way: shrink back below the cap stays hashtable.
    for i in 0..DEFAULT_SET_MAX_LISTPACK_ENTRIES {
        srem(&mut store, key, format!("m{i}").as_bytes());
    }
    assert_eq!(
        encoding_of(&mut store, key),
        "hashtable",
        "one-way ratchet: a hashtable set never demotes"
    );

    // Per-member byte transition.
    let key2 = b"lp_bytes";
    let mut store2 = ShardStore::new(1);
    sadd(&mut store2, key2, b"y"); // listpack
    let big = vec![b'q'; DEFAULT_SET_MAX_LISTPACK_VALUE + 1];
    sadd(&mut store2, key2, &big);
    assert_eq!(
        encoding_of(&mut store2, key2),
        "hashtable",
        "a member over the 64-byte cap -> hashtable"
    );
}

#[test]
fn emptied_set_deletes_the_key_no_empty_set_observable() {
    let key = b"d";
    let mut store = ShardStore::new(1);
    sadd(&mut store, key, b"only");
    assert!(store.read(0, key, NOW).is_some());
    // Remove the last member: the key must be GONE (empty-collection-deletes-key).
    assert!(srem(&mut store, key, b"only"));
    assert!(
        store.read(0, key, NOW).is_none(),
        "an emptied set deletes the key"
    );
    assert_eq!(store.used_memory(), 0, "accounting returns to zero");

    // Same via SPOP draining the last member.
    sadd(&mut store, key, b"solo");
    let popped = spop_at(&mut store, key, 0);
    assert_eq!(popped, Some(b"solo".to_vec()));
    assert!(
        store.read(0, key, NOW).is_none(),
        "SPOP-to-empty deletes the key"
    );
    assert_eq!(store.used_memory(), 0);
}

#[test]
fn wrongtype_on_a_string_key_makes_no_edit() {
    let key = b"s";
    let mut store = ShardStore::new(1);
    store.upsert(0, key, NewValue::Bytes(b"hello"), ExpireWrite::Clear, NOW);
    let before = store.used_memory();

    // A set edit on a string key: the typed view returns None, so the handler returns Keep
    // with a WRONGTYPE-shaped reply and NO accounting change.
    let saw_non_set = store.rmw_mut(0, key, NOW, |entry| match entry {
        RmwEntry::OccupiedMut(mut o) => {
            let is_set = o.as_set_mut().is_some();
            RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: !is_set,
            }
        }
        _ => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
    });
    assert!(saw_non_set, "as_set_mut returns None on a string key");
    assert_eq!(
        store.used_memory(),
        before,
        "WRONGTYPE makes no accounting change"
    );
    assert_eq!(store.read(0, key, NOW).unwrap().as_bytes(), b"hello");
}

#[test]
fn seeded_spop_workload_replays_identically_across_a_resize() {
    // Determinism (ADR-0003): the SAME seeded SADD/SPOP workload run twice on two
    // independent stores produces byte-identical results, INCLUDING across the
    // intset->hashtable resize (the hashtable members() order is sorted by the fixed-seed
    // member hash). The store reads no RNG; the caller draws every SPOP index from the
    // seeded stream, so the popped sequence is fully deterministic on replay.
    type RunResult = (u64, String, Vec<Vec<u8>>, Vec<Vec<u8>>);
    fn run(seed: u64) -> RunResult {
        let key = b"w";
        let mut store = ShardStore::new(1);
        let mut state = seed;
        let mut popped_seq: Vec<Vec<u8>> = Vec::new();
        // Grow the set well past 512 integer members so it crosses to hashtable (the
        // resize), then SPOP a chunk back out, indices drawn from the same seeded stream.
        for n in 0..700u64 {
            sadd(&mut store, key, n.to_string().as_bytes());
        }
        for _ in 0..300 {
            let pick = splitmix(&mut state);
            if let Some(m) = spop_at(&mut store, key, pick) {
                popped_seq.push(m);
            }
        }
        let enc = encoding_of(&mut store, key);
        // The deterministic surviving members.
        let survivors = store.rmw_mut(0, key, NOW, |entry| match entry {
            RmwEntry::OccupiedMut(mut o) => {
                let members = o.as_set_mut().map(|s| s.members()).unwrap_or_default();
                RmwStep {
                    action: RmwAction::Keep,
                    expire: ExpireWrite::Unchanged,
                    reply: members,
                }
            }
            _ => RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: Vec::new(),
            },
        });
        (store.used_memory(), enc, popped_seq, survivors)
    }

    let a = run(0xC0FF_EE12_3456_789A);
    let b = run(0xC0FF_EE12_3456_789A);
    assert_eq!(a.0, b.0, "used_memory replays identically");
    assert_eq!(a.1, b.1, "encoding replays identically");
    assert_eq!(
        a.2, b.2,
        "the SPOP'd sequence replays identically (across the resize)"
    );
    assert_eq!(a.3, b.3, "surviving members replay identically");
    assert_eq!(
        a.1, "hashtable",
        "the workload crossed into the hashtable form"
    );
}
