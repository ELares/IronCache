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
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
