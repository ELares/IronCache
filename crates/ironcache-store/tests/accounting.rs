// SPDX-License-Identifier: MIT OR Apache-2.0
//! Accounting smoke test for the PROCESS-GLOBAL jemalloc figures (ADR-0006,
//! OBSERVABILITY.md): `process_allocated_bytes`/`process_resident_bytes`.
//!
//! This test installs jemalloc as ITS OWN `#[global_allocator]` so the mallctl
//! `stats.allocated`/`stats.resident` figures reflect real allocations (the library
//! crate and the default test harness do not register a global allocator, so
//! without this the stats would not move). It asserts the figure is > 0 after boot
//! and STRICTLY grows after storing a large value. It does NOT assert an exact
//! number: `stats.allocated` reflects ALL process allocations (not just the store),
//! so only ">0" and "grew" are stable (the brief's accounting-smoke contract).
//!
//! On MSVC jemalloc is unavailable and `process_*_bytes` returns 0 (no global to
//! query), so the test is gated to the non-MSVC targets where the figure is live.

#![cfg(not(target_env = "msvc"))]

use ironcache_storage::{ExpireWrite, NewValue, Store, UnixMillis};
use ironcache_store::{ShardStore, process_allocated_bytes, process_resident_bytes};

// jemalloc as this test binary's global allocator so the mallctl stats are live.
// NOT under miri: miri cannot execute jemalloc's foreign allocator/`mallctl` C
// functions, and a `#[global_allocator]` is exercised at binary STARTUP (before any
// test), so leaving jemalloc registered would abort the whole binary under miri. Gating
// it out lets the miri run LOAD this binary; the one test inside is `ignore`d under miri
// (it still needs the live mallctl stats), and miri uses its own allocator for the
// binary's startup. Outside miri, jemalloc is the global allocator exactly as before.
#[cfg(not(miri))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Ignored under miri ONLY: this test reads jemalloc's `mallctl` FFI (epoch::advance /
// stats::allocated), which miri cannot execute, and it needs the jemalloc
// `#[global_allocator]` (gated out under miri above) for the stats to be live. This is a
// non-UB incompatibility (a shell-out to the jemalloc allocator), NOT an Entry-safety
// concern — the Entry alloc/dealloc/drop/access paths are covered by the kvobj unit
// tests + the other store tests, which DO run under miri. Outside miri it runs normally.
#[cfg_attr(
    miri,
    ignore = "needs the jemalloc global allocator + mallctl FFI, which miri cannot execute"
)]
#[test]
fn process_allocated_bytes_is_positive_and_grows() {
    // After boot the process has allocated SOMETHING, so the figure is > 0.
    let before = process_allocated_bytes();
    assert!(
        before > 0,
        "process_allocated_bytes should be > 0 after boot"
    );

    // Store a large value so the live allocated total strictly grows. Keep the
    // store alive (and the value out of the int fast path: a 1 MiB raw string is a
    // real heap allocation the store holds) across the second read.
    let mut store = ShardStore::new(1);
    let big = vec![b'q'; 1024 * 1024];
    store.upsert(
        0,
        b"big",
        NewValue::Bytes(&big),
        ExpireWrite::Clear,
        UnixMillis(0),
    );
    // Drop the local `big` so only the store's copy keeps the megabyte live, then
    // re-measure. The store still holds ~1 MiB, so allocated stays above `before`.
    drop(big);

    let after = process_allocated_bytes();
    assert!(
        after > before,
        "process_allocated_bytes should grow after storing a large value \
         (before={before}, after={after})"
    );

    // Keep the store live to here so the megabyte is still allocated at the read.
    assert!(
        store.used_memory() >= 1024 * 1024,
        "per-shard counter holds the value"
    );

    // RSS is also readable and positive (it can exceed the logical total under
    // fragmentation; we only assert it is live, not an exact figure).
    let rss = process_resident_bytes();
    assert!(rss > 0, "process_resident_bytes should be > 0");
}
