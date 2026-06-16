// SPDX-License-Identifier: MIT OR Apache-2.0
//! Shared helpers for the IronCache performance harness (BENCHMARK.md #8).
//!
//! This crate holds the criterion micro-benches (`benches/`) and the
//! allocator-true memory model (`src/bin/memmodel.rs`). The library half is just
//! the small, deterministic fixtures both share: deterministic key/value
//! generation for each string encoding class, and a populated [`ShardStore`]
//! builder.
//!
//! ## Determinism (ADR-0003, invariant 2)
//!
//! Everything here is DETERMINISTIC: no RNG, no clock. Keys and values are derived
//! from an index with [`format!`], and the store is fed a fixed `now`. This is a
//! hard CI constraint (the invariant lint forbids any time/rand use outside the
//! `ironcache-env` seam), and it also makes the benches and the memory model
//! reproducible run to run.
//!
//! ## The three string encoding classes (ENCODINGS.md)
//!
//! The store classifies a string value into one of three encodings, which have
//! very different memory profiles. The memory model and the store benches sweep
//! all three so bytes-per-key is reported per class:
//!
//! - [`EncodingClass::Int`]: a canonical i64 in decimal (e.g. `"12345"`). Stored
//!   inline in the per-key object's value enum with NO value heap allocation (the
//!   `int` variant is the only string-family variant that does not box its bytes).
//!   `OBJECT ENCODING` -> `int`.
//! - [`EncodingClass::EmbStr`]: a short string at or below the embstr threshold
//!   (44 bytes, [`ironcache_store::encoding::EMBSTR_THRESHOLD`]), stored in a single
//!   boxed value allocation (memory Round 2 shrank the per-key slot by boxing the
//!   embstr bytes rather than carrying a fixed inline buffer). `OBJECT ENCODING` ->
//!   `embstr`.
//! - [`EncodingClass::Raw`]: a longer string stored out-of-line (a separate heap
//!   allocation). `OBJECT ENCODING` -> `raw`.

//! ## The load generator (PR-A2, BENCHMARK.md #8, PERF_REGRESSION_GATE.md #159)
//!
//! The other half of this crate is a self-contained macro load generator that
//! drives a running IronCache (or any RESP server) over TCP with a SEEDED zipf
//! workload, in two passes that are never conflated:
//!
//! - [`closed_loop`]: N concurrent connections each loop request->reply as fast as
//!   possible for a duration; reports peak throughput (total ops / wall seconds).
//! - [`open_loop`]: a wrk2-style constant-rate pass that issues requests on a FIXED
//!   schedule (partitioned round-robin across the connections, each its own task with
//!   its own seeded RNG and local histogram) and measures each request's latency from
//!   its INTENDED send time, so the reported tail is free of coordinated omission. The
//!   per-request wait uses a coarse-sleep-then-busy-spin (`open_loop::wait_until`) for
//!   microsecond dispatch precision instead of tokio's ~1ms timer floor, and the
//!   result flags a generator-limited (`saturated`) run.
//!
//! The shared pieces are the [`workload`] generator (deterministic given a seed),
//! the minimal async RESP [`client`], and the [`report`] JSON serializer. The thin
//! CLI wiring lives in `src/bin/loadgen.rs`. Every time stamp goes through
//! `ironcache_env` and every workload draw goes through a seeded
//! `ironcache_env::SplitMix64`, so the .rs files here contain no direct
//! `Instant`/`SystemTime`/`rand` use (invariant 2, ADR-0003).

#![forbid(unsafe_code)]

pub mod client;
pub mod closed_loop;
pub mod open_loop;
pub mod report;
pub mod workload;

/// An in-test RESP stub server, shared by the closed-loop and open-loop unit tests
/// so neither requires a real IronCache. Compiled only under `cfg(test)`.
#[cfg(test)]
pub mod testutil;

use ironcache_storage::{ExpireWrite, NewValue, Store, UnixMillis};
use ironcache_store::ShardStore;

/// A fixed logical timestamp for every write in the harness. The store reads no
/// clock (ADR-0003), so any constant works; using a stable non-zero value keeps
/// the (TTL-free) writes well clear of the epoch boundary. Writes here pass
/// [`ExpireWrite::Clear`], so the deadline is never consulted regardless.
pub const FIXED_NOW: UnixMillis = UnixMillis(1_000_000_000_000);

/// The single logical database the harness exercises.
pub const DB: u32 = 0;

/// The three string encoding classes the store classifies a value into
/// (ENCODINGS.md). The harness sweeps all three because their memory profiles
/// differ sharply (int: no value alloc; embstr: inline; raw: out-of-line).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingClass {
    /// A canonical integer value (no value allocation). `OBJECT ENCODING` -> int.
    Int,
    /// A short inline string (<= 44 bytes). `OBJECT ENCODING` -> embstr.
    EmbStr,
    /// A longer out-of-line string. `OBJECT ENCODING` -> raw.
    Raw,
}

impl EncodingClass {
    /// The stable lowercase name used in the memory-model JSON and bench ids.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            EncodingClass::Int => "int",
            EncodingClass::EmbStr => "embstr",
            EncodingClass::Raw => "raw",
        }
    }
}

/// A deterministic key for index `i`: `"key:<i>"`. Distinct per index, ASCII, no
/// RNG. The colon-decimal shape is well clear of the canonical-integer rule, so a
/// key is never itself misclassified as an int.
#[must_use]
pub fn key_for(i: usize) -> Vec<u8> {
    format!("key:{i}").into_bytes()
}

/// A deterministic value for index `i` in the given encoding class.
///
/// - [`EncodingClass::Int`]: the decimal of `i` (a canonical i64), so the store
///   stores the int encoding with no value allocation. `value_size` is ignored.
/// - [`EncodingClass::EmbStr`] / [`EncodingClass::Raw`]: an ASCII string of length
///   `value_size`, filled with a fixed glyph and stamped with `i` at the front so
///   distinct keys carry distinct bytes. The caller picks `value_size` (<= 44 for
///   embstr, > 44 for raw); the value is NOT a canonical integer, so it classifies
///   as a string, not an int.
#[must_use]
pub fn value_for(class: EncodingClass, i: usize, value_size: usize) -> Vec<u8> {
    match class {
        EncodingClass::Int => i.to_string().into_bytes(),
        EncodingClass::EmbStr | EncodingClass::Raw => {
            // Stamp the index at the front so values differ per key, then pad with a
            // fixed glyph to the requested length. A leading 'v' guarantees the bytes
            // are not a canonical integer (so they classify as a string, never int).
            let mut v = format!("v{i}").into_bytes();
            if v.len() < value_size {
                v.resize(value_size, b'x');
            } else {
                v.truncate(value_size);
            }
            v
        }
    }
}

/// A fresh, EMPTY [`ShardStore`] (one DB, default hooks). The memory model uses
/// this so it can `reserve` to the final key count before filling (a resize-free
/// fill), which is what lets it separate per-entry data cost from table slack.
#[must_use]
pub fn new_store() -> ShardStore {
    ShardStore::new(1)
}

/// Build a fresh [`ShardStore`] (one DB, default hooks) populated with `n` keys of
/// the given encoding class and value size, all written without a TTL.
///
/// Deterministic: key `i` is [`key_for`] and value `i` is [`value_for`]. Used by the
/// store read bench (a hot, pre-populated store).
#[must_use]
pub fn build_store(class: EncodingClass, n: usize, value_size: usize) -> ShardStore {
    let mut store = new_store();
    fill_store(&mut store, class, 0, n, value_size);
    store
}

/// Insert keys `[start, start + count)` of the given class/size into `store`,
/// without a TTL. Factored out so the memory model can `reserve` and then fill a
/// known range, and so the read bench can pre-populate a hot store.
pub fn fill_store(
    store: &mut ShardStore,
    class: EncodingClass,
    start: usize,
    count: usize,
    value_size: usize,
) {
    for i in start..start + count {
        let key = key_for(i);
        let value = value_for(class, i, value_size);
        store.upsert(
            DB,
            &key,
            NewValue::Bytes(&value),
            ExpireWrite::Clear,
            FIXED_NOW,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::Store;
    use ironcache_store::encoding::EMBSTR_THRESHOLD;

    #[test]
    fn keys_are_distinct_and_stable() {
        assert_eq!(key_for(0), b"key:0");
        assert_eq!(key_for(42), b"key:42");
        // Stable across calls (no RNG/clock).
        assert_eq!(key_for(7), key_for(7));
        assert_ne!(key_for(1), key_for(2));
    }

    #[test]
    fn value_classes_have_the_expected_shape() {
        // Int: canonical decimal, value_size ignored.
        assert_eq!(value_for(EncodingClass::Int, 12345, 0), b"12345");
        // EmbStr: exactly value_size bytes, at or below the threshold.
        let emb = value_for(EncodingClass::EmbStr, 3, 16);
        assert_eq!(emb.len(), 16);
        assert!(emb.len() <= EMBSTR_THRESHOLD);
        assert!(emb.starts_with(b"v3"));
        // Raw: exactly value_size bytes, above the threshold.
        let raw = value_for(EncodingClass::Raw, 3, 128);
        assert_eq!(raw.len(), 128);
        assert!(raw.len() > EMBSTR_THRESHOLD);
    }

    #[test]
    fn class_names_are_stable() {
        assert_eq!(EncodingClass::Int.name(), "int");
        assert_eq!(EncodingClass::EmbStr.name(), "embstr");
        assert_eq!(EncodingClass::Raw.name(), "raw");
    }

    #[test]
    fn build_store_populates_and_reads_back() {
        let n = 64;
        let mut store = build_store(EncodingClass::EmbStr, n, 16);
        // Every key inserted is present and live.
        for i in 0..n {
            let key = key_for(i);
            let got = store.read(DB, &key, FIXED_NOW);
            assert!(got.is_some(), "key {i} should be present");
            assert_eq!(
                got.unwrap().as_bytes(),
                value_for(EncodingClass::EmbStr, i, 16).as_slice()
            );
        }
        // A key that was never inserted is absent.
        assert!(store.read(DB, &key_for(n + 1), FIXED_NOW).is_none());
    }

    #[test]
    fn classes_report_the_expected_store_encoding() {
        // Each class must actually land in the store encoding it claims, or the
        // per-class memory numbers would be mislabeled.
        let mut ints = build_store(EncodingClass::Int, 8, 0);
        assert_eq!(
            ints.read(DB, &key_for(5), FIXED_NOW).unwrap().encoding(),
            ironcache_storage::Encoding::Int
        );
        assert_eq!(
            ints.read(DB, &key_for(5), FIXED_NOW).unwrap().as_bytes(),
            b"5"
        );

        let mut embs = build_store(EncodingClass::EmbStr, 8, 16);
        assert_eq!(
            embs.read(DB, &key_for(5), FIXED_NOW).unwrap().encoding(),
            ironcache_storage::Encoding::EmbStr
        );

        let mut raws = build_store(EncodingClass::Raw, 8, 128);
        assert_eq!(
            raws.read(DB, &key_for(5), FIXED_NOW).unwrap().encoding(),
            ironcache_storage::Encoding::Raw
        );
    }
}
