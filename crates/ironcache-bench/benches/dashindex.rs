// SPDX-License-Identifier: MIT OR Apache-2.0
//! DashIndex-vs-hashbrown index micro-bench (#285 Stage 3, the DASHTABLE.md gate:
//! "the standalone-crate microbench gates the throughput risk BEFORE any store wiring").
//!
//! Head-to-head over the exact API shape the store drives: key-in-object records,
//! caller-supplied 64-bit hash, `find` probes (hit + miss), `entry`-funnel fill, and full
//! iteration. `hashbrown::HashTable`'s SIMD group probe is the bar; the SAFE parallel-array
//! DashIndex is EXPECTED to trail it (DASHTABLE.md names this the headline risk) -- this
//! harness exists to QUANTIFY the gap and then to gate the dense-layout follow-up, which
//! must close it before any default flip.
//!
//! Determinism: fixed keys `0..n`, hashes from the fixed SplitMix64 finalizer; no RNG, no
//! clock (ADR-0003).

#![forbid(unsafe_code)]

use criterion::{Criterion, criterion_group, criterion_main};
use hashbrown::HashTable;
use ironcache_dashtable::index::{DashIndex, Entry as DashEntry};
use std::hint::black_box;

/// The key-in-object record, shaped like the store's `Entry` usage (8-byte-aligned
/// payload; the key lives inside the object).
#[derive(Clone)]
struct Rec {
    key: u64,
    val: u64,
}

/// The explicit hash both tables receive: SplitMix64's finalizer (fixed, well mixed) --
/// the same stand-in the oracle parity suite uses.
fn hash_of(key: u64) -> u64 {
    let mut z = key.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The steady-state table size the probe benches run at. 100k records per table is the
/// per-slot-table ballpark at large residency (the store partitions each DB across 256
/// slot tables), large enough that probes miss cache like production and growth has gone
/// through many splits / doublings.
const N: u64 = 100_000;

fn dash_filled(n: u64) -> DashIndex<Rec> {
    let mut t: DashIndex<Rec> = DashIndex::new();
    for key in 0..n {
        match t.entry(hash_of(key), |r| r.key == key, |r| hash_of(r.key)) {
            DashEntry::Occupied(_) => unreachable!("keys are distinct"),
            DashEntry::Vacant(e) => {
                e.insert(Rec { key, val: key });
            }
        }
    }
    t
}

fn oracle_filled(n: u64) -> HashTable<Rec> {
    let mut t: HashTable<Rec> = HashTable::new();
    for key in 0..n {
        match t.entry(hash_of(key), |r| r.key == key, |r| hash_of(r.key)) {
            hashbrown::hash_table::Entry::Occupied(_) => unreachable!("keys are distinct"),
            hashbrown::hash_table::Entry::Vacant(e) => {
                e.insert(Rec { key, val: key });
            }
        }
    }
    t
}

/// Probe HIT: find a present key, striding through the keyspace so consecutive probes do
/// not share cache lines (the store's zipf reads are hot-set-heavy, but the bench measures
/// the probe machinery, not the cache).
fn bench_probe_hit(c: &mut Criterion) {
    let dash = dash_filled(N);
    let oracle = oracle_filled(N);
    let mut g = c.benchmark_group("dashindex/probe_hit");
    let mut k = 0u64;
    g.bench_function("dash", |b| {
        b.iter(|| {
            k = (k + 7919) % N; // a prime stride over the keyspace
            let got = dash.find(hash_of(k), |r| r.key == k).map(|r| r.val);
            black_box(got);
        });
    });
    let mut k = 0u64;
    g.bench_function("hashbrown", |b| {
        b.iter(|| {
            k = (k + 7919) % N;
            let got = oracle.find(hash_of(k), |r| r.key == k).map(|r| r.val);
            black_box(got);
        });
    });
    g.finish();
}

/// Probe MISS: find an absent key (hashes route + fingerprint-scan, `eq` mostly never
/// runs). The miss path is what every SET of a new key pays before inserting.
fn bench_probe_miss(c: &mut Criterion) {
    let dash = dash_filled(N);
    let oracle = oracle_filled(N);
    let mut g = c.benchmark_group("dashindex/probe_miss");
    let mut k = 0u64;
    g.bench_function("dash", |b| {
        b.iter(|| {
            k = (k + 7919) % N;
            let absent = N + k; // never inserted
            let got = dash.find(hash_of(absent), |r| r.key == absent).is_some();
            black_box(got);
        });
    });
    let mut k = 0u64;
    g.bench_function("hashbrown", |b| {
        b.iter(|| {
            k = (k + 7919) % N;
            let absent = N + k;
            let got = oracle.find(hash_of(absent), |r| r.key == absent).is_some();
            black_box(got);
        });
    });
    g.finish();
}

/// Fill through the entry funnel: N vacant inserts into a fresh table, growth included
/// (dash splits + directory doublings vs hashbrown doublings + rehashes). Per-iteration
/// cost is the AMORTIZED insert. A smaller N than the probe benches keeps criterion's
/// per-iteration table build affordable.
fn bench_fill(c: &mut Criterion) {
    const FILL_N: u64 = 10_000;
    let mut g = c.benchmark_group("dashindex/fill_10k");
    g.sample_size(20);
    g.bench_function("dash", |b| {
        b.iter(|| black_box(dash_filled(FILL_N).len()));
    });
    g.bench_function("hashbrown", |b| {
        b.iter(|| black_box(oracle_filled(FILL_N).len()));
    });
    g.finish();
}

/// Full iteration (the store's refill_evict_pool / snapshot / flush walks): sum a field
/// across every record.
fn bench_iter(c: &mut Criterion) {
    let dash = dash_filled(N);
    let oracle = oracle_filled(N);
    let mut g = c.benchmark_group("dashindex/iter_100k");
    g.sample_size(30);
    g.bench_function("dash", |b| {
        b.iter(|| black_box(dash.iter().map(|r| r.val).sum::<u64>()));
    });
    g.bench_function("hashbrown", |b| {
        b.iter(|| black_box(oracle.iter().map(|r| r.val).sum::<u64>()));
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_probe_hit,
    bench_probe_miss,
    bench_fill,
    bench_iter
);
criterion_main!(benches);
