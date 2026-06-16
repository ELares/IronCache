// SPDX-License-Identifier: MIT OR Apache-2.0
//! Store primitive micro-benches (BENCHMARK.md #8): `upsert` (the blind-set write
//! path) and `read` (the hot-key read path) against a per-shard `ShardStore`.
//!
//! Determinism: keys/values come from the deterministic helpers in the crate lib
//! (no RNG, no clock); `now` is a fixed constant. The read bench reads a key that
//! is always present (a hot key), so it measures the live-hit path, not a miss.
//!
//! NOTE: eviction (`evict_to_fit`) and TTL (`TimingWheel`) micro-benches are
//! DEFERRED from PR-A1 (the brief marks them optional). They are not required for
//! the A1 green bar and are tracked for a later PR in the performance track so the
//! eviction-budget and timing-wheel-advance hot paths get coverage too.

#![forbid(unsafe_code)]

use criterion::{Criterion, criterion_group, criterion_main};
use ironcache_bench::{DB, EncodingClass, FIXED_NOW, build_store, key_for, value_for};
use ironcache_storage::{ExpireWrite, NewValue, Store};
use ironcache_store::ShardStore;
use std::hint::black_box;

/// Bench a single `upsert` (blind SET) of one embstr key into a fresh store. Each
/// iteration writes the SAME key into the SAME (one-key) store: this isolates the
/// classify + write-funnel cost (the overwrite path) without growing the table,
/// which would otherwise fold occasional rehash cost into the per-op number.
fn bench_upsert(c: &mut Criterion) {
    let key = key_for(0);
    let value = value_for(EncodingClass::EmbStr, 0, 16);
    let mut store = ShardStore::new(1);
    c.bench_function("store/upsert_embstr", |b| {
        b.iter(|| {
            let existed = store.upsert(
                black_box(DB),
                black_box(&key),
                black_box(NewValue::Bytes(&value)),
                black_box(ExpireWrite::Clear),
                black_box(FIXED_NOW),
            );
            black_box(existed);
        });
    });
}

/// Bench a `read` of a hot key from a pre-populated store. The store holds 10k
/// embstr keys; the bench reads one that is always present, so it measures the
/// live-hit lookup (hash + probe + borrow materialization), not a miss or the
/// lazy-expiry backstop.
fn bench_read_hit(c: &mut Criterion) {
    let n = 10_000;
    let mut store = build_store(EncodingClass::EmbStr, n, 16);
    // A key that is present and roughly mid-table.
    let key = key_for(n / 2);
    c.bench_function("store/read_hit_embstr", |b| {
        b.iter(|| {
            let got = store.read(black_box(DB), black_box(&key), black_box(FIXED_NOW));
            // The value is present; touch its bytes so the borrow is not elided.
            black_box(got.map(|v| v.as_bytes().len()));
        });
    });
}

criterion_group!(benches, bench_upsert, bench_read_hit);
criterion_main!(benches);
