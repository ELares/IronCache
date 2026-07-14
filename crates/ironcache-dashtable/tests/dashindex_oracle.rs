// SPDX-License-Identifier: MIT OR Apache-2.0
//! ORACLE parity for [`ironcache_dashtable::index::DashIndex`] (#285 Stage 3, PR-1):
//! `hashbrown::HashTable` -- the EXACT type + API the store uses today -- is driven with the
//! IDENTICAL op stream, identical explicit hashes, and identical key-in-object records, and
//! the two tables must agree on every lookup, every upsert outcome, every removal, `len`,
//! and the full record multiset. This is the wiring-safety net: the store swap (PR-2) is a
//! type alias over exactly this API surface, so op-for-op agreement here IS the evidence
//! the swap preserves behavior.
//!
//! ## Determinism (ADR-0003)
//!
//! The op stream is drawn from a SEEDED `ironcache_env::SplitMix64` (no `rand`, no OS
//! entropy, no time), so a failure reproduces byte-identically from the printed seed.

use hashbrown::HashTable;
use ironcache_dashtable::index::{DashIndex, Entry as DashEntry};
use ironcache_env::{Rng, SplitMix64};

/// The key-in-object record, shaped like the store's `Entry` usage: the table stores only
/// the object; the key lives inside it and `eq` compares against it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Rec {
    key: u64,
    val: u64,
}

/// The explicit hash both tables receive for a key: SplitMix64's finalizer (fixed, well
/// mixed). Both tables get the SAME hash for the same key, mirroring how the store feeds
/// its one table hash to whichever index backend is compiled in.
fn hash_of(key: u64) -> u64 {
    let mut z = key.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Upsert into the DASH index through the same entry-match shape the store's `put_object`
/// uses. Returns the replaced value (`Some` = overwrite), mirroring the oracle helper.
fn dash_upsert(t: &mut DashIndex<Rec>, key: u64, val: u64) -> Option<u64> {
    match t.entry(hash_of(key), |r| r.key == key, |r| hash_of(r.key)) {
        DashEntry::Occupied(mut e) => Some(std::mem::replace(e.get_mut(), Rec { key, val }).val),
        DashEntry::Vacant(e) => {
            e.insert(Rec { key, val });
            None
        }
    }
}

/// Upsert into the HASHBROWN oracle through its `entry` API (the store's real call shape).
fn oracle_upsert(t: &mut HashTable<Rec>, key: u64, val: u64) -> Option<u64> {
    match t.entry(hash_of(key), |r| r.key == key, |r| hash_of(r.key)) {
        hashbrown::hash_table::Entry::Occupied(mut e) => {
            Some(std::mem::replace(e.get_mut(), Rec { key, val }).val)
        }
        hashbrown::hash_table::Entry::Vacant(e) => {
            e.insert(Rec { key, val });
            None
        }
    }
}

fn dash_remove(t: &mut DashIndex<Rec>, key: u64) -> Option<u64> {
    match t.find_entry(hash_of(key), |r| r.key == key) {
        Ok(occ) => Some(occ.remove().0.val),
        Err(_) => None,
    }
}

fn oracle_remove(t: &mut HashTable<Rec>, key: u64) -> Option<u64> {
    match t.find_entry(hash_of(key), |r| r.key == key) {
        Ok(occ) => Some(occ.remove().0.val),
        Err(_) => None,
    }
}

/// Assert the two tables hold the identical record MULTISET (order-insensitively: both
/// backends leave iteration order unspecified; the store sorts everything it exposes).
fn assert_same_records(dash: &DashIndex<Rec>, oracle: &HashTable<Rec>, ctx: &str) {
    let mut a: Vec<(u64, u64)> = dash.iter().map(|r| (r.key, r.val)).collect();
    let mut b: Vec<(u64, u64)> = oracle.iter().map(|r| (r.key, r.val)).collect();
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, b, "record multiset diverged {ctx}");
    assert_eq!(dash.len(), oracle.len(), "len diverged {ctx}");
}

/// One full randomized run: `ops` operations over a `key_space`-sized key domain, with the
/// given op mix, comparing outcomes op-for-op and the multiset at checkpoints. The small
/// key space forces heavy overwrite/remove-reinsert churn; the op count forces many
/// segment splits and directory doublings in the dash index.
fn run_oracle(seed: u64, ops: usize, key_space: u64) {
    let mut rng = SplitMix64::new(seed);
    let mut dash: DashIndex<Rec> = DashIndex::new();
    let mut oracle: HashTable<Rec> = HashTable::new();

    for op_idx in 0..ops {
        let r = rng.next_u64();
        let key = (r >> 8) % key_space;
        match r % 10 {
            // 60% upsert (inserts early, overwrites as the space fills).
            0..=5 => {
                let val = rng.next_u64();
                let a = dash_upsert(&mut dash, key, val);
                let b = oracle_upsert(&mut oracle, key, val);
                assert_eq!(a, b, "upsert outcome diverged at op {op_idx} (seed {seed})");
            }
            // 20% remove.
            6 | 7 => {
                let a = dash_remove(&mut dash, key);
                let b = oracle_remove(&mut oracle, key);
                assert_eq!(a, b, "remove outcome diverged at op {op_idx} (seed {seed})");
            }
            // 10% find.
            8 => {
                let a = dash.find(hash_of(key), |r| r.key == key).map(|r| r.val);
                let b = oracle.find(hash_of(key), |r| r.key == key).map(|r| r.val);
                assert_eq!(a, b, "find diverged at op {op_idx} (seed {seed})");
            }
            // 10% find_mut + in-place edit (exercises the mutable path on both).
            _ => {
                let delta = rng.next_u64();
                let a = dash.find_mut(hash_of(key), |r| r.key == key).map(|r| {
                    r.val = r.val.wrapping_add(delta);
                    r.val
                });
                let b = oracle.find_mut(hash_of(key), |r| r.key == key).map(|r| {
                    r.val = r.val.wrapping_add(delta);
                    r.val
                });
                assert_eq!(a, b, "find_mut diverged at op {op_idx} (seed {seed})");
            }
        }
        if op_idx % 1024 == 0 {
            assert_same_records(&dash, &oracle, &format!("at op {op_idx} (seed {seed})"));
        }
    }
    assert_same_records(&dash, &oracle, &format!("at end (seed {seed})"));
}

#[test]
fn dash_index_matches_hashbrown_over_deterministic_op_streams() {
    // Several seeds x a key space small enough to churn every path: vacant insert,
    // occupied overwrite, remove + re-insert, find/find_mut hits and misses, splits,
    // directory doublings. ~16k ops per seed keeps the suite fast (<1s).
    for seed in [0x5DEE_CE66_D1CE_5EED_u64, 1, 42, 0xFFFF_FFFF_FFFF_FFFF] {
        run_oracle(seed, 16 * 1024, 512);
    }
}

#[test]
fn dash_index_matches_hashbrown_under_growth_heavy_load() {
    // A larger key space so the run is insert-dominated: the dash index ends with many
    // segments + a deep directory, and the multiset must still match exactly.
    run_oracle(0xA5A5_A5A5_5A5A_5A5A, 32 * 1024, 1 << 20);
}

#[test]
fn clone_divergence_matches_oracle_clone_divergence() {
    // The #576 COW shape: clone both tables mid-stream, then apply DIFFERENT tails to the
    // original and the clone; each pair must stay in lockstep independently.
    let seed = 0xC0C0_C0C0_C0C0_C0C0_u64;
    let mut rng = SplitMix64::new(seed);
    let mut dash: DashIndex<Rec> = DashIndex::new();
    let mut oracle: HashTable<Rec> = HashTable::new();
    for _ in 0..4096 {
        let key = rng.next_u64() % 256;
        let val = rng.next_u64();
        dash_upsert(&mut dash, key, val);
        oracle_upsert(&mut oracle, key, val);
    }
    let mut dash2 = dash.clone();
    let mut oracle2 = oracle.clone();
    // Original gets inserts; clone gets removes. No cross-talk allowed.
    for k in 0..128u64 {
        let a = dash_upsert(&mut dash, 10_000 + k, k);
        let b = oracle_upsert(&mut oracle, 10_000 + k, k);
        assert_eq!(a, b);
        let a = dash_remove(&mut dash2, k);
        let b = oracle_remove(&mut oracle2, k);
        assert_eq!(a, b);
    }
    assert_same_records(&dash, &oracle, "original after divergence");
    assert_same_records(&dash2, &oracle2, "clone after divergence");
}
