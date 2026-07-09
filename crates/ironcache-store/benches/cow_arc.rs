// SPDX-License-Identifier: MIT OR Apache-2.0
//! COW/MVCC de-risking micro-bench (#576 PR-1c): quantify the HOT-PATH cost of
//! wrapping each per-slot store table in an `Arc` so the concurrent-snapshot save can
//! freeze + read a slot off-thread (O(1) clone) instead of stalling the shard.
//!
//! This bench does NOT touch the real store. It reconstructs the post-#570 per-slot
//! layout (`Vec<HashTable<Entry>>`, [`ironcache_store::ShardStore`] `dbs[db]`) and an
//! Arc-wrapped variant (`Vec<Arc<HashTable<Entry>>>`) side by side, populates BOTH with
//! the same representative keyspace (1M keys across the default 256 slots, ~4000 entries
//! per slot), and measures the SAME GET/SET probe through each so the DELTA is purely the
//! Arc indirection + the saving-flag check + (during a save) the COW slot clone.
//!
//! ## What each path models (mirrors `ShardStore` in crates/ironcache-store/src/lib.rs)
//!
//! - GET (the `Store::read` hot path): hash the key, route to its slot table
//!   (`slot_index`), `find_mut` + `bump_freq` (the S3-FIFO freq bump lives ON the entry, so
//!   a GET is a WRITE to the entry). Through an `Arc` the in-place bump needs `Arc::get_mut`
//!   (a uniqueness / refcount check) when the slot is not frozen. We also measure a
//!   read-ONLY probe (no bump) to isolate the PURE Arc-deref indirection floor the design
//!   note calls out ("deref Arc -> HashTable").
//! - SET no-save (the `Store::upsert` -> `put_object` hot path): build the new `Entry`,
//!   check the per-shard `AtomicBool` "saving" flag (false -> fast path, no COW), get the
//!   slot table mutably (`Arc::get_mut`, unique) and overwrite. Measures the flag check +
//!   `get_mut` cost the COW SET fast path adds.
//! - SET during-save: with the "saving" flag set and a background reader holding a frozen
//!   clone of the target slot (refcount 2), a write must COW: `Arc::make_mut` deep-clones
//!   that one slot's ~4000 `Entry`s so the frozen reader keeps its stable view. This is the
//!   per-episode during-save write cost (the first write to a frozen slot pays it; later
//!   writes to the now-unique slot are fast until the next save re-freezes it).
//!
//! ## Timing seam (ADR-0003 determinism, invariant 2)
//!
//! criterion drives the mean/median A/B (its own timing harness, no clock text in this
//! crate). The supplementary p50/p99/p99.9 tail table times each op through the sanctioned
//! [`ironcache_env`] monotonic seam (never a raw `Instant`), exactly as the load generator
//! does, so `scripts/ci/check-rust-invariants.sh` stays green. This bench is off the engine
//! path; it reads no shard state and installs no clock into the store.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)] // The percentile report is one linear, readable pass.

use criterion::{Criterion, criterion_group, criterion_main};
use hashbrown::hash_table::Entry as TableEntry;
use hashbrown::{DefaultHashBuilder, HashTable};
use ironcache_env::{Clock, SystemEnv};
use ironcache_store::{DEFAULT_SLOTS_PER_DB, Entry, scan_hash};
use std::hash::BuildHasher;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The representative keyspace: 1M keys (a power of two so the rotating probe index
/// masks instead of dividing). Spread across [`DEFAULT_SLOTS_PER_DB`] (256) slots this is
/// ~4096 entries per slot, matching the #570 sizing the store doc cites.
const N: usize = 1 << 20;

/// The per-DB slot count under test (the store default, #570).
const SLOTS: usize = DEFAULT_SLOTS_PER_DB;

/// The slot for `key`, replicating the store-private `slot_index`: the fixed-seed
/// [`scan_hash`] masked to the (power-of-two) slot count. Kept byte-identical to the store
/// so the bench routes keys to slots exactly as `ShardStore` does.
#[inline]
fn slot_of(key: &[u8]) -> usize {
    (scan_hash(key) as usize) & (SLOTS - 1)
}

/// A deterministic key for index `i` (ASCII, no RNG/clock): `"key:<i>"`.
fn key_for(i: usize) -> Vec<u8> {
    format!("key:{i}").into_bytes()
}

/// A deterministic 16-byte embstr value for `key` (<= the 44-byte embstr threshold, so the
/// entry classifies as a single boxed blob, the common small-value shape). The `key:` stamp
/// keeps distinct keys carrying distinct bytes and clear of the canonical-integer rule.
fn value_for(key: &[u8]) -> Vec<u8> {
    let mut v = key.to_vec();
    v.resize(16, b'x');
    v
}

/// Build the `N` keys once (shared by every variant so all measure the same keyspace).
fn build_keys() -> Vec<Vec<u8>> {
    (0..N).map(key_for).collect()
}

/// Populate a fresh per-slot `Vec<HashTable<Entry>>` (the BASELINE layout: `ShardStore.dbs[db]`)
/// with every key, routed by [`slot_of`] and hashed with the store's table hasher.
fn build_slots(keys: &[Vec<u8>], hasher: &DefaultHashBuilder) -> Vec<HashTable<Entry>> {
    let mut slots: Vec<HashTable<Entry>> = (0..SLOTS).map(|_| HashTable::new()).collect();
    for key in keys {
        let e = Entry::str_from_bytes(key, &value_for(key), None);
        let h = hasher.hash_one(key.as_slice());
        // Keys are unique, so an unconditional unique-insert is correct and avoids a probe.
        slots[slot_of(key)].insert_unique(h, e, |x| hasher.hash_one(x.key()));
    }
    slots
}

/// The Arc-wrapped variant: the same populated per-slot tables, each behind its own `Arc`
/// (the COW layout `Vec<Arc<HashTable<Entry>>>`). Built independently (not a deep clone of
/// the baseline) so both hold live, distinct data.
fn build_arc_slots(keys: &[Vec<u8>], hasher: &DefaultHashBuilder) -> Vec<Arc<HashTable<Entry>>> {
    build_slots(keys, hasher)
        .into_iter()
        .map(Arc::new)
        .collect()
}

/// Overwrite `key`'s entry in `table` (the SET inner op, shared by every SET variant): build
/// a fresh `Entry` (as `upsert` does) and replace in place. Keys pre-exist, so this is the
/// Occupied overwrite arm of the store's write funnel.
#[inline]
fn set_into(table: &mut HashTable<Entry>, key: &[u8], hasher: &DefaultHashBuilder) {
    let e = Entry::str_from_bytes(key, &value_for(key), None);
    let h = hasher.hash_one(key);
    match table.entry(h, |x| x.key() == key, |x| hasher.hash_one(x.key())) {
        TableEntry::Occupied(mut o) => *o.get_mut() = e,
        TableEntry::Vacant(v) => {
            v.insert(e);
        }
    }
}

// ---------------------------------------------------------------------------
// Supplementary p50/p99/p99.9 tail table (deterministic-clock, hdr-free).
// ---------------------------------------------------------------------------

/// One measured op's tail: p50/p99/p99.9 in nanoseconds plus throughput (ops/sec).
struct Stats {
    p50: u64,
    p99: u64,
    p999: u64,
    ops_per_sec: f64,
}

/// The percentile at `num/den` of a sorted sample slice (nearest-rank).
fn pct(sorted: &[u64], num: usize, den: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() * num) / den).min(sorted.len() - 1);
    sorted[idx]
}

/// Warm `op` then time `iters` single invocations through the monotonic env seam, returning
/// the per-op tail. Each sample is one `now()`-delta around `op`, so it includes a fixed
/// clock-read overhead that CANCELS in any baseline-vs-arc delta (both pay it); the caller
/// subtracts the calibrated overhead for the corrected absolute figures.
fn measure<F: FnMut()>(env: &SystemEnv, warmup: usize, iters: usize, mut op: F) -> Stats {
    for _ in 0..warmup {
        op();
    }
    let mut samples: Vec<u64> = Vec::with_capacity(iters);
    let wall_start = env.now();
    for _ in 0..iters {
        let t0 = env.now();
        op();
        let t1 = env.now();
        samples
            .push(u64::try_from(t1.saturating_duration_since(t0).as_nanos()).unwrap_or(u64::MAX));
    }
    let wall = env.now().saturating_duration_since(wall_start);
    samples.sort_unstable();
    let ops_per_sec = if wall.as_secs_f64() > 0.0 {
        iters as f64 / wall.as_secs_f64()
    } else {
        0.0
    };
    Stats {
        p50: pct(&samples, 50, 100),
        p99: pct(&samples, 99, 100),
        p999: pct(&samples, 999, 1000),
        ops_per_sec,
    }
}

/// Print the p50/p99/p99.9 tail table and the derived regression verdict. Runs before the
/// criterion means so its output sits at the top of `cargo bench` output.
fn report_percentiles(
    env: &SystemEnv,
    baseline: &mut [HashTable<Entry>],
    arc: &mut [Arc<HashTable<Entry>>],
    hasher: &DefaultHashBuilder,
    keys: &[Vec<u8>],
) {
    let warm = 200_000;
    let iters = 1 << 20; // ~1M timed samples per op.
    let mask = N - 1;
    let saving = AtomicBool::new(false);

    // Calibrate the fixed per-sample clock-read overhead (an empty timed op).
    let cal = measure(env, 10_000, iters, || {
        black_box(0u64);
    });
    let overhead = cal.p50;

    // GET baseline: find_mut + bump_freq (the real Store::read entry op).
    let mut i = 0usize;
    let get_base = measure(env, warm, iters, || {
        let key = &keys[i & mask];
        i = i.wrapping_add(1);
        let h = hasher.hash_one(key.as_slice());
        let hit = baseline[slot_of(key)]
            .find_mut(h, |e| e.key() == key)
            .map(|e| {
                e.bump_freq();
                e.key().len()
            });
        black_box(hit);
    });

    // GET arc: Arc::get_mut (uniqueness/refcount check, slot not frozen) + find_mut + bump.
    let mut i = 0usize;
    let get_arc = measure(env, warm, iters, || {
        let key = &keys[i & mask];
        i = i.wrapping_add(1);
        let h = hasher.hash_one(key.as_slice());
        let slot = slot_of(key);
        let table = Arc::get_mut(&mut arc[slot]).expect("slot arc is unique when no save");
        let hit = table.find_mut(h, |e| e.key() == key).map(|e| {
            e.bump_freq();
            e.key().len()
        });
        black_box(hit);
    });

    // GET arc pure-deref floor: shared deref through the Arc + read-only find (no bump), the
    // minimum the Arc indirection can cost if the freq bump moved off the &mut path.
    let mut i = 0usize;
    let get_arc_ro = measure(env, warm, iters, || {
        let key = &keys[i & mask];
        i = i.wrapping_add(1);
        let h = hasher.hash_one(key.as_slice());
        let table: &HashTable<Entry> = &arc[slot_of(key)];
        let hit = table.find(h, |e| e.key() == key).map(|e| e.key().len());
        black_box(hit);
    });

    // SET baseline: build entry + overwrite in place.
    let mut i = 0usize;
    let set_base = measure(env, warm, iters, || {
        let key = &keys[i & mask];
        i = i.wrapping_add(1);
        set_into(&mut baseline[slot_of(key)], key, hasher);
    });

    // SET arc no-save: saving-flag check (false) + Arc::get_mut + overwrite.
    let mut i = 0usize;
    let set_arc = measure(env, warm, iters, || {
        let key = &keys[i & mask];
        i = i.wrapping_add(1);
        let slot = slot_of(key);
        if saving.load(Ordering::Relaxed) {
            // Never taken here; models the branch the fast path pays.
            black_box(&saving);
        }
        let table = Arc::get_mut(&mut arc[slot]).expect("slot arc is unique when no save");
        set_into(table, key, hasher);
    });

    // SET during-save: a background reader holds a frozen clone of the target slot, so the
    // write must Arc::make_mut (deep-clone the ~4000-entry slot) before overwriting. Each
    // iteration re-freezes to measure the per-episode COW clone cost (not the amortized
    // post-clone fast path). Fewer iters: each op copies a whole slot.
    let cow_iters = 4_000;
    let mut i = 0usize;
    let target_slot = 0usize;
    let set_cow = measure(env, 50, cow_iters, || {
        let key = &keys[i & mask];
        i = i.wrapping_add(1);
        let frozen = arc[target_slot].clone(); // save holds a stable view -> refcount 2.
        let table = Arc::make_mut(&mut arc[target_slot]); // count 2 -> deep-clone this slot.
        set_into(table, key, hasher);
        drop(frozen);
    });

    let adj = |v: u64| v.saturating_sub(overhead);

    println!("\n==================== COW Arc micro-bench: p50/p99/p99.9 tail ====================");
    println!(
        "keyspace: {N} keys / {SLOTS} slots (~{} entries/slot), value 16B embstr",
        N / SLOTS
    );
    println!("clock-read overhead (subtracted below): p50 = {overhead} ns/sample\n");
    println!(
        "{:<26} {:>10} {:>10} {:>12} {:>14}",
        "op (overhead-corrected)", "p50 ns", "p99 ns", "p99.9 ns", "Mops/s"
    );
    let row = |name: &str, s: &Stats| {
        println!(
            "{:<26} {:>10} {:>10} {:>12} {:>14.2}",
            name,
            adj(s.p50),
            adj(s.p99),
            adj(s.p999),
            s.ops_per_sec / 1e6
        );
    };
    row("GET baseline (bump)", &get_base);
    row("GET arc (get_mut+bump)", &get_arc);
    row("GET arc (deref, read-only)", &get_arc_ro);
    row("SET baseline", &set_base);
    row("SET arc (no-save)", &set_arc);
    row("SET arc (during-save COW)", &set_cow);
    // The GET/SET FAST-PATH Arc overhead is a ~1-4 ns per-op effect, BELOW this harness's
    // per-op monotonic-clock resolution (a single now()-delta cannot resolve a 3 ns delta on
    // a 30 ns op). The AUTHORITATIVE ns-scale A/B is the criterion section below (warmed,
    // outlier-controlled, tight CIs). This table is for the ABSOLUTE tail magnitude and,
    // especially, the during-save COW cost, which it resolves cleanly (hundreds of us):
    println!(
        "\nSET during-save p99 = {} ns (~{} us) = one per-slot COW deep-clone of ~{} entries.",
        adj(set_cow.p99),
        adj(set_cow.p99) / 1000,
        N / SLOTS
    );
    println!("NOTE: the GET/SET fast-path Arc deltas are ~1-4 ns/op, below this per-op clock's");
    println!(
        "      resolution; read the ns-scale regression + GATE from the criterion means below."
    );
    println!("================================================================================\n");
}

// ---------------------------------------------------------------------------
// criterion mean/median A/B (the rigorous headline regression).
// ---------------------------------------------------------------------------

/// The full bench: build the fixtures ONCE, print the tail table, then run the criterion
/// mean/median A/B for every variant.
fn cow_arc_benches(c: &mut Criterion) {
    let env = SystemEnv::new();
    let hasher = DefaultHashBuilder::default();
    let keys = build_keys();
    let mut baseline = build_slots(&keys, &hasher);
    let mut arc = build_arc_slots(&keys, &hasher);
    let mask = N - 1;
    let saving = AtomicBool::new(false);

    // Supplementary p50/p99/p99.9 tail (deterministic-clock seam).
    report_percentiles(&env, &mut baseline, &mut arc, &hasher, &keys);

    // --- GET ---
    let mut i = 0usize;
    c.bench_function("cow/get_baseline_bump", |b| {
        b.iter(|| {
            let key = &keys[i & mask];
            i = i.wrapping_add(1);
            let h = hasher.hash_one(key.as_slice());
            let hit = baseline[slot_of(key)]
                .find_mut(h, |e| e.key() == key)
                .map(|e| {
                    e.bump_freq();
                    e.key().len()
                });
            black_box(hit);
        });
    });

    let mut i = 0usize;
    c.bench_function("cow/get_arc_getmut_bump", |b| {
        b.iter(|| {
            let key = &keys[i & mask];
            i = i.wrapping_add(1);
            let h = hasher.hash_one(key.as_slice());
            let slot = slot_of(key);
            let table = Arc::get_mut(&mut arc[slot]).expect("unique slot");
            let hit = table.find_mut(h, |e| e.key() == key).map(|e| {
                e.bump_freq();
                e.key().len()
            });
            black_box(hit);
        });
    });

    let mut i = 0usize;
    c.bench_function("cow/get_baseline_readonly", |b| {
        b.iter(|| {
            let key = &keys[i & mask];
            i = i.wrapping_add(1);
            let h = hasher.hash_one(key.as_slice());
            let table: &HashTable<Entry> = &baseline[slot_of(key)];
            let hit = table.find(h, |e| e.key() == key).map(|e| e.key().len());
            black_box(hit);
        });
    });

    let mut i = 0usize;
    c.bench_function("cow/get_arc_deref_readonly", |b| {
        b.iter(|| {
            let key = &keys[i & mask];
            i = i.wrapping_add(1);
            let h = hasher.hash_one(key.as_slice());
            let table: &HashTable<Entry> = &arc[slot_of(key)];
            let hit = table.find(h, |e| e.key() == key).map(|e| e.key().len());
            black_box(hit);
        });
    });

    // --- SET ---
    let mut i = 0usize;
    c.bench_function("cow/set_baseline", |b| {
        b.iter(|| {
            let key = &keys[i & mask];
            i = i.wrapping_add(1);
            set_into(&mut baseline[slot_of(key)], key, &hasher);
        });
    });

    let mut i = 0usize;
    c.bench_function("cow/set_arc_nosave", |b| {
        b.iter(|| {
            let key = &keys[i & mask];
            i = i.wrapping_add(1);
            let slot = slot_of(key);
            if saving.load(Ordering::Relaxed) {
                black_box(&saving);
            }
            let table = Arc::get_mut(&mut arc[slot]).expect("unique slot");
            set_into(table, key, &hasher);
        });
    });

    let mut i = 0usize;
    let target_slot = 0usize;
    c.bench_function("cow/set_arc_during_save_cow", |b| {
        b.iter(|| {
            let key = &keys[i & mask];
            i = i.wrapping_add(1);
            let frozen = arc[target_slot].clone();
            let table = Arc::make_mut(&mut arc[target_slot]);
            set_into(table, key, &hasher);
            drop(frozen);
        });
    });
}

criterion_group!(benches, cow_arc_benches);
criterion_main!(benches);
