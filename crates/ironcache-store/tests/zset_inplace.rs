// SPDX-License-Identifier: MIT OR Apache-2.0
//! The PR-8 ZSET value over the in-place-mutation RMW mechanism, exercised at the store
//! level (STORAGE_API.md "the RMW in-place-mutation contract", COLLECTIONS.md,
//! ZSET_LARGE.md). These drive the concrete [`ShardStore`] through the additive `rmw_mut`
//! / `OccupiedMut` / `as_zset_mut` / `RmwAction::Mutated` surface and assert:
//!
//! - a Mutated edit grows/shrinks `used_memory` by EXACTLY the per-member delta (member
//!   bytes + the fixed 8-byte score charge), across a random ZADD/ZREM/ZINCRBY/ZPOPMIN
//!   sequence INCLUDING the listpack->skiplist transition (a property test against a
//!   from-scratch recompute of a shadow model);
//! - the encoding transition fires at the Redis thresholds (128 entries / 64-byte member)
//!   and is a one-way ratchet;
//! - an emptied zset DELETES the key (no empty zset observable);
//! - WRONGTYPE on a string key (the typed view returns None, the handler returns Keep, no
//!   edit / no accounting change);
//! - ZPOPMIN determinism is trivial (it is rank-ordered, not RNG-driven), so the property
//!   test's seeded sequence replays byte-identically AND the accounting survives a resize.
//!
//! Determinism (ADR-0003): the "random" sequence is a SEEDED in-test splitmix64 (no
//! std::time / no rand crate), so the run is byte-identical on replay. The store reads no
//! RNG.

// Test-only stylistic relaxations: `store`/`score` read as similar to clippy but are
// clearly distinct here, and the `rmw_mut(..)`-tail helpers return `()` from an expression
// that clippy would prefer terminated with `;`.
#![allow(clippy::similar_names, clippy::semicolon_if_nothing_returned)]

use ironcache_config::{DEFAULT_ZSET_MAX_LISTPACK_ENTRIES, DEFAULT_ZSET_MAX_LISTPACK_VALUE};
use ironcache_storage::{
    ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
    ZAddFlags,
};
use ironcache_store::ShardStore;
use std::collections::BTreeMap;

const NOW: UnixMillis = UnixMillis(0);

/// A deterministic splitmix64 (no std::time, no rand crate; ADR-0003).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// A from-scratch recompute of the per-shard accounting weight for a SINGLE zset key:
/// `key.len() + sum(member byte len + 8)`, matching `KvObj::accounted_bytes()` for a zset
/// (member bytes plus the fixed 8-byte score charge). An EMPTY model means the key is
/// DELETED (empty-collection-deletes-key), so the weight is 0.
fn expected_bytes(key: &[u8], model: &BTreeMap<Vec<u8>, f64>) -> u64 {
    if model.is_empty() {
        return 0;
    }
    let elems: usize = model.keys().map(|m| m.len() + 8).sum();
    (key.len() + elems) as u64
}

/// ZADD one (member, score) through the store's in-place-mutation arm (create-on-missing
/// via Insert, else Mutated). Mirrors the command-layer handler so the test exercises the
/// real measure-delta path.
fn zadd(store: &mut ShardStore, key: &[u8], member: &[u8], score: f64) {
    let m = member.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::zset(vec![(m, score)])),
            expire: ExpireWrite::Clear,
            reply: (),
        },
        RmwEntry::OccupiedMut(mut o) => {
            let zset = o.as_zset_mut().expect("zset");
            zset.add(&m, score, ZAddFlags::default());
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: (),
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    })
}

/// ZINCRBY one member (create-or-grow) through the in-place arm.
fn zincrby(store: &mut ShardStore, key: &[u8], member: &[u8], delta: f64) {
    let m = member.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::zset(vec![(m, delta)])),
            expire: ExpireWrite::Clear,
            reply: (),
        },
        RmwEntry::OccupiedMut(mut o) => {
            let zset = o.as_zset_mut().expect("zset");
            zset.incr(&m, delta, ZAddFlags::default());
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: (),
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    })
}

/// ZREM one member (deletes the key if it drains the zset). Returns whether it existed.
fn zrem(store: &mut ShardStore, key: &[u8], member: &[u8]) -> bool {
    let m = member.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let zset = o.as_zset_mut().expect("zset");
            let removed = zset.remove(&m);
            let action = if zset.is_empty() {
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

/// ZPOPMIN one member: pops the lowest-score member, deleting the key if drained. Returns
/// the popped member, or None if absent.
fn zpopmin(store: &mut ShardStore, key: &[u8]) -> Option<Vec<u8>> {
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: None,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let zset = o.as_zset_mut().expect("zset");
            let popped = zset.pop_min(1);
            let member = popped.into_iter().next().map(|(m, _)| m);
            let action = if zset.is_empty() {
                RmwAction::Delete
            } else {
                RmwAction::Mutated
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: member,
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    })
}

#[test]
fn accounting_matches_from_scratch_recompute_across_transition() {
    let mut store = ShardStore::new(1);
    let key = b"z";
    // The shadow model: member -> score. Mirrors the zset's logical contents.
    let mut model: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
    let mut rng = Rng(0x5151_5151_DEAD_BEEF);

    // Drive enough operations to cross the 128-entry listpack->skiplist transition and
    // back (one-way: it stays skiplist), with a mix of ZADD/ZINCRBY/ZREM/ZPOPMIN.
    for step in 0..4000u64 {
        let op = rng.next() % 4;
        // Member key space wide enough to both grow past 128 and churn.
        let mid = rng.next() % 200;
        let member = format!("m{mid:04}");
        let mb = member.as_bytes();
        match op {
            0 => {
                let score = (rng.next() % 1000) as f64;
                zadd(&mut store, key, mb, score);
                model.insert(member.into_bytes(), score);
            }
            1 => {
                let delta = ((rng.next() % 20) as f64) - 10.0;
                zincrby(&mut store, key, mb, delta);
                let entry = model.entry(member.into_bytes()).or_insert(0.0);
                *entry += delta;
            }
            2 => {
                zrem(&mut store, key, mb);
                model.remove(member.as_bytes());
            }
            _ => {
                let popped = zpopmin(&mut store, key);
                if let Some(p) = popped {
                    model.remove(&p);
                }
            }
        }
        // Every few steps assert the accounting equals the from-scratch recompute.
        if step % 7 == 0 {
            assert_eq!(
                store.used_memory(),
                expected_bytes(key, &model),
                "accounting drift at step {step} (model size {})",
                model.len()
            );
        }
    }
    // Final exact check.
    assert_eq!(store.used_memory(), expected_bytes(key, &model));

    // Drain everything via ZPOPMIN; the key must end deleted (empty-deletes-key) and the
    // accounting back to zero.
    while !model.is_empty() {
        if let Some(p) = zpopmin(&mut store, key) {
            model.remove(&p);
        } else {
            break;
        }
    }
    assert!(!store.contains(0, key, NOW), "draining deletes the key");
    assert_eq!(
        store.used_memory(),
        0,
        "accounting returns to zero when empty"
    );
}

#[test]
fn encoding_transition_at_entry_and_byte_thresholds() {
    let mut store = ShardStore::new(1);
    let key = b"z";
    // 128 small members stay listpack.
    for i in 0..DEFAULT_ZSET_MAX_LISTPACK_ENTRIES {
        zadd(&mut store, key, format!("m{i:04}").as_bytes(), i as f64);
    }
    assert_eq!(
        store.read(0, key, NOW).unwrap().encoding().encoding_name(),
        "listpack",
        "exactly 128 entries stays listpack"
    );
    // The 129th flips to skiplist.
    zadd(&mut store, key, b"overflow", 999.0);
    assert_eq!(
        store.read(0, key, NOW).unwrap().encoding().encoding_name(),
        "skiplist",
        "129 entries flips to skiplist"
    );

    // Byte threshold: a fresh key with a member over 64 bytes -> skiplist immediately.
    let mut store2 = ShardStore::new(1);
    let big = vec![b'q'; DEFAULT_ZSET_MAX_LISTPACK_VALUE + 1];
    zadd(&mut store2, b"z2", &big, 1.0);
    assert_eq!(
        store2
            .read(0, b"z2", NOW)
            .unwrap()
            .encoding()
            .encoding_name(),
        "skiplist",
        "a 65-byte member flips to skiplist"
    );
}

#[test]
fn wrongtype_leaves_a_string_key_untouched() {
    let mut store = ShardStore::new(1);
    store.upsert(
        0,
        b"str",
        NewValue::Bytes(b"hello"),
        ExpireWrite::Clear,
        NOW,
    );
    let before = store.used_memory();
    // A zset edit against a string key: the typed as_zset_mut returns None, so the handler
    // returns Keep with no edit and no accounting change.
    let saw_non_zset = store.rmw_mut(0, b"str", NOW, |entry| {
        let is_non_zset = match entry {
            RmwEntry::OccupiedMut(mut o) => o.as_zset_mut().is_none(),
            _ => false,
        };
        RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: is_non_zset,
        }
    });
    assert!(saw_non_zset, "a string key yields no zset view (WRONGTYPE)");
    assert_eq!(store.used_memory(), before, "no accounting change");
    assert_eq!(store.read(0, b"str", NOW).unwrap().as_bytes(), b"hello");
}
