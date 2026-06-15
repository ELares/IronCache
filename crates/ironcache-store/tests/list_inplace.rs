// SPDX-License-Identifier: MIT OR Apache-2.0
//! The PR-5 in-place-mutation RMW mechanism, exercised at the store level
//! (STORAGE_API.md "the RMW in-place-mutation contract", COLLECTIONS.md). These
//! drive the concrete [`ShardStore`] through the additive `rmw_mut` / `OccupiedMut` /
//! `RmwAction::Mutated` surface and assert:
//!
//! - a Mutated edit grows/shrinks `used_memory` by EXACTLY the element delta, across
//!   a random LPUSH/RPUSH/LPOP/LREM/LSET sequence INCLUDING the listpack->quicklist
//!   transition (a property test against a from-scratch recompute);
//! - the encoding flips to `quicklist` at the threshold and back to `listpack`;
//! - an emptied list DELETES the key (no empty list observable);
//! - WRONGTYPE on a string key (the typed view returns None, the handler returns
//!   Keep, no edit / no accounting change).
//!
//! Determinism (ADR-0003): the "random" sequence is a SEEDED in-test LCG (no
//! std::time / no rand crate), so the run is byte-identical on replay.

use ironcache_config::DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES;
use ironcache_storage::{
    ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};
use ironcache_store::ShardStore;

const NOW: UnixMillis = UnixMillis(0);

/// A deterministic from-scratch recompute of the per-shard accounting weight for a
/// SINGLE list key: `key.len() + sum(element byte lengths)`, matching
/// `KvObj::accounted_bytes()` for a list. An EMPTY model means the key is DELETED
/// (empty-collection-deletes-key), so the weight is 0 (the key bytes are not resident).
fn expected_bytes(key: &[u8], model: &[Vec<u8>]) -> u64 {
    if model.is_empty() {
        return 0;
    }
    let elems: usize = model.iter().map(Vec::len).sum();
    (key.len() + elems) as u64
}

/// LPUSH one element through the store's in-place-mutation arm (create-on-missing via
/// Insert, else Mutated). Mirrors the command-layer handler so the test exercises the
/// real measure-delta path.
fn lpush(store: &mut ShardStore, key: &[u8], elem: &[u8]) {
    let e = elem.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::list(vec![e])),
            expire: ExpireWrite::Clear,
            reply: (),
        },
        RmwEntry::OccupiedMut(mut o) => {
            let list = o.as_list_mut().expect("list");
            list.push_front(&e);
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: (),
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    });
}

/// RPUSH one element through the in-place arm.
fn rpush(store: &mut ShardStore, key: &[u8], elem: &[u8]) {
    let e = elem.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::list(vec![e])),
            expire: ExpireWrite::Clear,
            reply: (),
        },
        RmwEntry::OccupiedMut(mut o) => {
            let list = o.as_list_mut().expect("list");
            list.push_back(&e);
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: (),
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    });
}

/// LPOP one element (deletes the key if it drains the list).
fn lpop(store: &mut ShardStore, key: &[u8]) -> Option<Vec<u8>> {
    store.rmw_mut(0, key, NOW, |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: None,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let list = o.as_list_mut().expect("list");
            let popped = list.pop_front();
            let action = if list.is_empty() {
                RmwAction::Delete
            } else {
                RmwAction::Mutated
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: popped,
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    })
}

/// LREM count element (count>0 head->tail; deletes the key if it empties the list).
fn lrem(store: &mut ShardStore, key: &[u8], count: i64, elem: &[u8]) -> usize {
    let e = elem.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: 0,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let list = o.as_list_mut().expect("list");
            let n = list.remove_matching(count, &e);
            let action = if list.is_empty() {
                RmwAction::Delete
            } else {
                RmwAction::Mutated
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: n,
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    })
}

/// LSET index element (no-op if out of range).
fn lset(store: &mut ShardStore, key: &[u8], index: i64, elem: &[u8]) -> bool {
    let e = elem.to_vec();
    store.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let list = o.as_list_mut().expect("list");
            let ok = list.set(index, &e);
            RmwStep {
                action: if ok {
                    RmwAction::Mutated
                } else {
                    RmwAction::Keep
                },
                expire: ExpireWrite::Unchanged,
                reply: ok,
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
    // A seeded LCG (deterministic; no std::time / no rand crate, ADR-0003) drives a
    // mix of LPUSH/RPUSH/LPOP/LREM/LSET on a SINGLE key, and we assert used_memory
    // equals a from-scratch recompute of the shadow model after every op, INCLUDING
    // across the listpack->quicklist transition (the elements grow past the 8 KB
    // budget). The store measures the delta itself (it does not trust the handler),
    // so this pins the measure-before/after-delta + re-account mechanism.
    let key = b"L";
    let mut store = ShardStore::new(1);
    let mut model: Vec<Vec<u8>> = Vec::new();

    let mut state: u64 = 0x1234_5678_9abc_def0;
    let mut next = || {
        // splitmix64-ish step, fully deterministic.
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };

    for _ in 0..4000 {
        let op = next() % 5;
        match op {
            0 => {
                // LPUSH a variable-length element (so total bytes cross 8 KB).
                let len = (next() % 200) as usize;
                let elem = vec![b'a' + (next() % 26) as u8; len];
                lpush(&mut store, key, &elem);
                model.insert(0, elem);
            }
            1 => {
                let len = (next() % 200) as usize;
                let elem = vec![b'a' + (next() % 26) as u8; len];
                rpush(&mut store, key, &elem);
                model.push(elem);
            }
            2 => {
                let got = lpop(&mut store, key);
                let want = if model.is_empty() {
                    None
                } else {
                    Some(model.remove(0))
                };
                assert_eq!(got, want, "LPOP element mismatch");
            }
            3 => {
                // LREM count>0 of a present-ish element.
                if !model.is_empty() {
                    let pick = (next() as usize) % model.len();
                    let target = model[pick].clone();
                    let count = 1 + (next() % 3) as i64;
                    let removed = lrem(&mut store, key, count, &target);
                    // Recompute the shadow model: remove up to `count` head->tail matches.
                    let mut left = count;
                    let mut shadow_removed = 0usize;
                    model.retain(|e| {
                        if left > 0 && *e == target {
                            left -= 1;
                            shadow_removed += 1;
                            false
                        } else {
                            true
                        }
                    });
                    assert_eq!(removed, shadow_removed, "LREM removed count mismatch");
                }
            }
            _ => {
                // LSET a random in-range index.
                if !model.is_empty() {
                    let idx = (next() as usize % model.len()) as i64;
                    let len = (next() % 100) as usize;
                    let elem = vec![b'A' + (next() % 26) as u8; len];
                    let ok = lset(&mut store, key, idx, &elem);
                    assert!(ok, "LSET in-range should succeed");
                    model[idx as usize] = elem;
                }
            }
        }

        // The accounting invariant after EVERY op: used_memory == from-scratch recompute.
        // An emptied list deletes the key, so its weight is 0 then.
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
fn encoding_flips_to_quicklist_at_the_threshold_and_back() {
    let key = b"e";
    let mut store = ShardStore::new(1);

    // A small list is listpack.
    rpush(&mut store, key, b"x");
    assert_eq!(encoding_of(&mut store, key), "listpack");

    // Push past the BYTE budget: one element over 8 KB forces quicklist.
    let big = vec![b'q'; DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES + 1];
    rpush(&mut store, key, &big);
    assert_eq!(
        encoding_of(&mut store, key),
        "quicklist",
        "over the byte budget -> quicklist"
    );

    // Pop the big element back off: the list is small again -> listpack.
    lpop(&mut store, key); // pops "x" (head); the big element is now the only one
    // After popping "x", the big element remains, so still quicklist.
    assert_eq!(encoding_of(&mut store, key), "quicklist");
    lpop(&mut store, key); // pops the big element; only ... wait, list is now empty -> deleted
    assert_eq!(encoding_of(&mut store, key), "none");

    // No element-count cap for lists (Redis -2 negative fill: count unlimited). MANY
    // small elements that stay UNDER the byte budget remain `listpack`, well past any
    // collection entry cap (512 hash / 128 zset-set): 200 single-byte elements = 200
    // bytes, far under 8 KB.
    let key2 = b"e2";
    for _ in 0..200 {
        rpush(&mut store, key2, b"z");
    }
    assert_eq!(
        encoding_of(&mut store, key2),
        "listpack",
        "many small elements stay listpack: byte-driven transition only, no entry cap"
    );

    // Crossing the 8 KB byte budget with many small elements flips to quicklist.
    let key3 = b"e3";
    let chunk = vec![b'y'; 100];
    let pushes = (DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES / chunk.len()) + 2;
    for _ in 0..pushes {
        rpush(&mut store, key3, &chunk);
    }
    assert_eq!(
        encoding_of(&mut store, key3),
        "quicklist",
        "crossing the 8 KB byte budget flips to quicklist"
    );
}

#[test]
fn emptied_list_deletes_the_key_no_empty_list_observable() {
    let key = b"d";
    let mut store = ShardStore::new(1);
    rpush(&mut store, key, b"only");
    assert!(store.read(0, key, NOW).is_some());
    // Pop the last element: the key must be GONE (empty-collection-deletes-key).
    let popped = lpop(&mut store, key);
    assert_eq!(popped, Some(b"only".to_vec()));
    assert!(
        store.read(0, key, NOW).is_none(),
        "an emptied list deletes the key"
    );
    assert_eq!(store.used_memory(), 0, "accounting returns to zero");

    // Same via LREM draining the list.
    rpush(&mut store, key, b"a");
    rpush(&mut store, key, b"a");
    let removed = lrem(&mut store, key, 0, b"a"); // count 0 = remove all
    assert_eq!(removed, 2);
    assert!(
        store.read(0, key, NOW).is_none(),
        "LREM-to-empty deletes key"
    );
    assert_eq!(store.used_memory(), 0);
}

#[test]
fn wrongtype_on_a_string_key_makes_no_edit() {
    let key = b"s";
    let mut store = ShardStore::new(1);
    store.upsert(0, key, NewValue::Bytes(b"hello"), ExpireWrite::Clear, NOW);
    let before = store.used_memory();

    // A list edit on a string key: the typed view returns None, so the handler
    // returns Keep with a WRONGTYPE-shaped reply and NO accounting change.
    let saw_non_list = store.rmw_mut(0, key, NOW, |entry| match entry {
        RmwEntry::OccupiedMut(mut o) => {
            let is_list = o.as_list_mut().is_some();
            RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: !is_list,
            }
        }
        _ => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
    });
    assert!(saw_non_list, "as_list_mut returns None on a string key");
    assert_eq!(
        store.used_memory(),
        before,
        "WRONGTYPE makes no accounting change"
    );
    // The string value is untouched.
    assert_eq!(store.read(0, key, NOW).unwrap().as_bytes(), b"hello");
}

#[test]
fn seeded_list_workload_replays_identically() {
    // Determinism (ADR-0003): the SAME seeded workload run twice on two independent
    // stores produces byte-identical results (used_memory, encoding, contents). Lists
    // use no RNG, so the only nondeterminism that could leak is the store's own state;
    // this pins that a list workload is fully deterministic on replay.
    fn run(seed: u64) -> (u64, String, Vec<Vec<u8>>) {
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
        for _ in 0..1500 {
            match next() % 3 {
                0 => {
                    let len = (next() % 300) as usize;
                    lpush(&mut store, key, &vec![b'a' + (next() % 26) as u8; len]);
                }
                1 => {
                    let len = (next() % 300) as usize;
                    rpush(&mut store, key, &vec![b'a' + (next() % 26) as u8; len]);
                }
                _ => {
                    lpop(&mut store, key);
                }
            }
        }
        let enc = encoding_of(&mut store, key);
        let contents = match store.read(0, key, NOW) {
            // The contents are read via a fresh range over the typed view.
            Some(_) => {
                let mut out = Vec::new();
                // Drain a copy by repeated LINDEX-style reads through pos/range is
                // awkward here; instead drain via LPOP on a CLONE is not possible, so
                // read length and each index through a dedicated read closure.
                let n = store.rmw_mut(0, key, NOW, |entry| match entry {
                    RmwEntry::OccupiedMut(mut o) => {
                        let list = o.as_list_mut().expect("list");
                        RmwStep {
                            action: RmwAction::Keep,
                            expire: ExpireWrite::Unchanged,
                            reply: list.len(),
                        }
                    }
                    _ => RmwStep {
                        action: RmwAction::Keep,
                        expire: ExpireWrite::Unchanged,
                        reply: 0,
                    },
                });
                for i in 0..n {
                    let e = store.rmw_mut(0, key, NOW, move |entry| match entry {
                        RmwEntry::OccupiedMut(mut o) => {
                            let list = o.as_list_mut().expect("list");
                            RmwStep {
                                action: RmwAction::Keep,
                                expire: ExpireWrite::Unchanged,
                                reply: list.get(i as i64).unwrap_or_default(),
                            }
                        }
                        _ => RmwStep {
                            action: RmwAction::Keep,
                            expire: ExpireWrite::Unchanged,
                            reply: Vec::new(),
                        },
                    });
                    out.push(e);
                }
                out
            }
            None => Vec::new(),
        };
        (store.used_memory(), enc, contents)
    }

    let a = run(0xDEAD_BEEF);
    let b = run(0xDEAD_BEEF);
    assert_eq!(a.0, b.0, "used_memory replays identically");
    assert_eq!(a.1, b.1, "encoding replays identically");
    assert_eq!(a.2, b.2, "list contents replay identically");
}

#[test]
fn quicklist_to_listpack_transition_is_a_pure_function_of_the_repr() {
    // Build a list that is quicklist (over the BYTE budget), then shrink it back below
    // the budget and confirm it reports listpack again -- the NAME is a pure function of
    // the active repr (#40), not a sticky flag. The transition is byte-driven only (no
    // element-count cap for lists).
    let key = b"t";
    let mut store = ShardStore::new(1);
    let chunk = vec![b'y'; 100];
    // Enough 100-byte chunks to cross 8 KB (with a margin).
    let budget_chunks = DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES / chunk.len();
    let pushes = budget_chunks + 5;
    for _ in 0..pushes {
        rpush(&mut store, key, &chunk);
    }
    assert_eq!(encoding_of(&mut store, key), "quicklist");

    // Pop chunks until well under the byte budget (~half the budget worth remaining):
    // pop (pushes - budget_chunks/2) chunks. The remaining bytes are then below 8 KB.
    let pops = pushes - budget_chunks / 2;
    for _ in 0..pops {
        lpop(&mut store, key);
    }
    assert_eq!(
        encoding_of(&mut store, key),
        "listpack",
        "shrinking below the byte budget reports listpack again (pure function of repr)"
    );
}
