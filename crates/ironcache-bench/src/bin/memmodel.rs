// SPDX-License-Identifier: MIT OR Apache-2.0
//! Allocator-true memory model (BENCHMARK.md #8): the honest bytes-per-key cost of
//! the IronCache store, measured against jemalloc's live `stats.allocated` figure
//! rather than a logical-byte estimate.
//!
//! ## Method (resize-free decomposition)
//!
//! Per-key memory has two distinct parts that must NOT be conflated:
//!
//!   1. the per-entry DATA cost: the key `Box<[u8]>`, the value allocation (for
//!      `raw`), and the entry stored in the table slot, and
//!   2. the hash table's SLOT cost: the control bytes plus empty-slot slack of the
//!      bucket array, whose size is a function of the table's CAPACITY (a power of
//!      two), not of the stored object.
//!
//! A naive "fill N, divide allocated by N" folds (2) into (1) at whatever load
//! factor N happens to land on. Because the table doubles its capacity at a load
//! threshold, that per-key figure SAWTOOTHS with N (the same object measured at a
//! point just past a doubling looks far heavier than one just before it). An
//! earlier version of this tool divided a two-wave allocation delta by N and
//! reported a single number; that number swung by more than half for the IDENTICAL
//! object purely with the choice of N, because [`ShardStore`] grew its table with
//! no pre-sizing, so the doubling landed asymmetrically between the waves.
//!
//! The fix is to make the fill RESIZE-FREE and report the two parts separately. For
//! each case we sample `stats.allocated` at three points against one store:
//!
//!   alloc_empty    = an empty store, before any reservation
//!   (reserve the table to the final key count: one deterministic bucket array)
//!   alloc_reserved = after the reservation (the bucket array is now allocated)
//!   (fill exactly N keys; no resize happens, because we reserved for N)
//!   alloc_filled   = after the fill
//!
//!   object_bytes_per_key = (alloc_filled    - alloc_reserved) / N
//!   table_bytes_per_key  = (alloc_reserved  - alloc_empty)    / N
//!   total_bytes_per_key  = (alloc_filled    - alloc_empty)    / N
//!
//! `object_bytes_per_key` is the property of the stored object: it is RESIZE-FREE
//! (the slots already exist) and therefore stable across N, and it is the figure to
//! compare against a competitor's per-item overhead. `table_bytes_per_key` is the
//! amortized bucket-array cost at this fill's load factor; it is reported alongside,
//! not hidden inside the per-object number. The three raw `alloc_*` samples are
//! emitted too so the decomposition is fully auditable.
//!
//! ## Competitor reference (tracked elsewhere)
//!
//! The competitor reference columns -- Redis 8.x `kvobj`, Valkey's 8-byte embedded
//! key, Dragonfly's dashtable -- are NOT measured here. They live in the A3
//! competitor matrix (a committed, separately-sourced table), so this binary
//! reports only IronCache's own allocator-true numbers. Pinning the published
//! total to a fixed load factor is an A3 concern; here `object_bytes_per_key` is
//! the load-factor-independent figure.
//!
//! ## Determinism (ADR-0003)
//!
//! No RNG, no clock: keys/values are derived from an index, `now` is fixed.

#![forbid(unsafe_code)]

use ironcache_bench::{DB, EncodingClass, fill_store, new_store};

// jemalloc as this binary's global allocator so the mallctl `stats.allocated`
// figures reflect real allocations (the default test/binary harness does not set
// one, so without this the stats would not move). Same cfg-gate as the main
// binary: non-MSVC, where jemalloc is the allocator the release targets ship.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// One measured case: an encoding class at a value size, with the three allocation
/// samples and the per-key decomposition derived from them.
struct CaseResult {
    encoding_class: &'static str,
    value_size: usize,
    n: usize,
    /// The per-entry data cost (resize-free, stable across `n`): key box + value
    /// allocation + the entry in its slot.
    object_bytes_per_key: f64,
    /// The amortized bucket-array cost at this fill's load factor.
    table_bytes_per_key: f64,
    /// The sum of the two: total resident bytes per stored key at this fill.
    total_bytes_per_key: f64,
    alloc_empty: u64,
    alloc_reserved: u64,
    alloc_filled: u64,
}

impl CaseResult {
    /// Hand-written JSON for this case (no serde dependency). Per-key figures are
    /// fixed to two decimals; the raw `alloc_*` samples are emitted whole.
    fn to_json(&self) -> String {
        format!(
            concat!(
                "{{\"encoding_class\":\"{}\",\"value_size\":{},\"n\":{},",
                "\"object_bytes_per_key\":{:.2},\"table_bytes_per_key\":{:.2},",
                "\"total_bytes_per_key\":{:.2},",
                "\"alloc_empty\":{},\"alloc_reserved\":{},\"alloc_filled\":{}}}"
            ),
            self.encoding_class,
            self.value_size,
            self.n,
            self.object_bytes_per_key,
            self.table_bytes_per_key,
            self.total_bytes_per_key,
            self.alloc_empty,
            self.alloc_reserved,
            self.alloc_filled,
        )
    }
}

/// The process-wide jemalloc `stats.allocated` total in bytes, freshly sampled.
/// Delegates to the store crate's reader (which advances the epoch first). On MSVC
/// (no jemalloc) it reports 0; the binary still runs and emits parse-clean JSON.
fn allocated_bytes() -> u64 {
    #[cfg(not(target_env = "msvc"))]
    {
        ironcache_store::process_allocated_bytes()
    }
    #[cfg(target_env = "msvc")]
    {
        0
    }
}

/// Per-key bytes from an allocation delta: `delta / n`. Saturating on the (rare,
/// sampling-noise) case where a later sample reads lower than an earlier one, so the
/// figure never goes negative or panics. Factored out so it is unit-testable without
/// an allocator.
fn per_key(earlier: u64, later: u64, n: usize) -> f64 {
    if n == 0 {
        return 0.0;
    }
    let delta = later.saturating_sub(earlier);
    delta as f64 / n as f64
}

/// Measure one case with the resize-free decomposition: build an empty store,
/// reserve the table to the final key count (so the fill triggers no resize), then
/// fill `n` keys, sampling `stats.allocated` at each boundary.
fn measure_case(class: EncodingClass, value_size: usize, n: usize) -> CaseResult {
    let mut store = new_store();
    let alloc_empty = allocated_bytes();

    // Reserve to the final count: the bucket array is allocated ONCE here, so the
    // fill that follows does not resize and the per-entry delta is pure data cost.
    store.reserve(DB, n);
    let alloc_reserved = allocated_bytes();

    fill_store(&mut store, class, 0, n, value_size);
    let alloc_filled = allocated_bytes();

    // Keep the store live until after the final sample so its allocations count.
    std::hint::black_box(&store);

    CaseResult {
        encoding_class: class.name(),
        value_size,
        n,
        object_bytes_per_key: per_key(alloc_reserved, alloc_filled, n),
        table_bytes_per_key: per_key(alloc_empty, alloc_reserved, n),
        total_bytes_per_key: per_key(alloc_empty, alloc_filled, n),
        alloc_empty,
        alloc_reserved,
        alloc_filled,
    }
}

fn main() {
    // The case matrix: each string encoding class at a representative size.
    //   int    -> no value allocation (value_size is nominal here).
    //   embstr -> a 16-byte inline string (<= the 44-byte threshold).
    //   raw    -> a 256-byte out-of-line string (> the threshold).
    const N: usize = 100_000;
    let cases = [
        (EncodingClass::Int, 0usize),
        (EncodingClass::EmbStr, 16usize),
        (EncodingClass::Raw, 256usize),
    ];

    let mut results = Vec::with_capacity(cases.len());
    for (class, value_size) in cases {
        results.push(measure_case(class, value_size, N));
    }

    // Emit a single JSON array of case objects to stdout.
    let body = results
        .iter()
        .map(CaseResult::to_json)
        .collect::<Vec<_>>()
        .join(",");
    println!("[{body}]");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_key_divides_delta() {
        // (1800 - 1000) / 100 = 8.0
        assert!((per_key(1000, 1800, 100) - 8.0).abs() < 1e-9);
    }

    #[test]
    fn per_key_is_zero_for_zero_n() {
        assert!(per_key(1000, 2000, 0).abs() < 1e-9);
    }

    #[test]
    fn per_key_saturates_on_negative_delta() {
        // A later sample lower than the earlier one (sampling noise) clamps to 0,
        // never negative, never a panic.
        assert!(per_key(2000, 1000, 100).abs() < 1e-9);
    }

    #[test]
    fn json_is_well_formed_and_has_all_fields() {
        let r = CaseResult {
            encoding_class: "embstr",
            value_size: 16,
            n: 100,
            object_bytes_per_key: 72.5,
            table_bytes_per_key: 9.25,
            total_bytes_per_key: 81.75,
            alloc_empty: 1_000,
            alloc_reserved: 1_925,
            alloc_filled: 9_175,
        };
        let j = r.to_json();
        assert!(j.starts_with('{') && j.ends_with('}'));
        assert!(j.contains("\"encoding_class\":\"embstr\""));
        assert!(j.contains("\"value_size\":16"));
        assert!(j.contains("\"object_bytes_per_key\":72.50"));
        assert!(j.contains("\"table_bytes_per_key\":9.25"));
        assert!(j.contains("\"total_bytes_per_key\":81.75"));
        assert!(j.contains("\"alloc_empty\":1000"));
        assert!(j.contains("\"alloc_reserved\":1925"));
        assert!(j.contains("\"alloc_filled\":9175"));
    }

    // NOTE: the two allocation-measuring assertions below live in ONE test on
    // purpose. `measure_case` reads the PROCESS-WIDE jemalloc `stats.allocated`, so
    // two separate multi-megabyte allocation tests running on different harness
    // threads would contaminate each other's deltas. Keeping the measurements in a
    // single test thread makes the three samples per case sequential and
    // uncontended; the other tests in this module allocate only a tiny JSON string,
    // which is negligible (sub-kilobyte) against the multi-megabyte fills here.
    #[test]
    fn decomposition_is_consistent_and_object_cost_is_n_stable() {
        // A real measurement run (jemalloc is this test binary's allocator). The
        // object and table costs must be positive for a raw value, and total must
        // equal their sum (the three deltas share endpoints).
        let small = measure_case(EncodingClass::Raw, 128, 30_000);
        assert_eq!(small.encoding_class, "raw");
        assert!(
            small.object_bytes_per_key > 0.0,
            "raw object cost must be positive"
        );
        assert!(
            small.table_bytes_per_key > 0.0,
            "reserved table must cost bytes"
        );
        let sum = small.object_bytes_per_key + small.table_bytes_per_key;
        assert!(
            (small.total_bytes_per_key - sum).abs() < 1e-6,
            "total {} must equal object+table {sum}",
            small.total_bytes_per_key
        );

        // The regression guard for the resize sawtooth: because the fill is
        // resize-free (we reserve to N first), the per-ENTRY data cost must be
        // essentially independent of N. The previous marginal-over-N method swung
        // by more than 50% for the IDENTICAL object between these two key counts;
        // the decomposition holds them within a few percent. Tolerance is generous
        // (12%) only to absorb jemalloc size-class edges at the key-length boundary.
        let large = measure_case(EncodingClass::Raw, 128, 60_000);
        assert!(large.object_bytes_per_key > 0.0);
        let rel = (small.object_bytes_per_key - large.object_bytes_per_key).abs()
            / small.object_bytes_per_key;
        assert!(
            rel < 0.12,
            "object cost must be N-stable: {} vs {} (rel {rel:.3})",
            small.object_bytes_per_key,
            large.object_bytes_per_key
        );
    }
}
