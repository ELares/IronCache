// SPDX-License-Identifier: MIT OR Apache-2.0
//! Persist-read pacer (#676, reusing the `save-backpressure-percent` knob #577): bounds the
//! BASE snapshot's read+encode duty cycle so it leaves DRAM-bandwidth headroom for the serving
//! datapath.
//!
//! MEASURED root cause (c7g): the base save's full-keyspace read+encode on the persist thread
//! saturates shared memory bandwidth; on an all-cores-busy thread-per-core server that STARVES
//! serving (the datapath's effective clock halves, ~27% backend-idle) -- the during-save p99.9.
//! It is not copy-on-write, not slot-size, not queueing (all ablated out). Pacing sleeps
//! proportionally between fixed-size encode chunks so the read consumes only ~`pct`% of the
//! wall-time, spreading the base save over a longer window at a low duty cycle (the operator keeps
//! the save CADENCE above the stretched duration).
//!
//! The `Instant` clock read lives HERE, in `ironcache-runtime` (the I/O + TIMER seam the
//! determinism-invariant lint exempts), so `ironcache-persist` stays clock-free
//! (`#![forbid(unsafe_code)]` + linted): the persist encode loop calls a plain `FnMut()` callback
//! and THIS type owns the timing + the sleep. ADR-0003 is untouched -- pacing produces NO
//! observable command output (exactly like `DEBUG SLEEP` and the persist-core pin); it only delays
//! a background durability op.

use std::time::{Duration, Instant};

/// The maximum single paced sleep. Bounds how long the persist thread lingers per chunk so a live
/// `CONFIG SET save-backpressure-percent` (read per chunk) and process shutdown stay responsive.
const MAX_PACE_SLEEP: Duration = Duration::from_millis(100);

/// Pure pacing math (NO clock, NO sleep -- unit-testable): the sleep to insert after a chunk that
/// took `elapsed` to encode, so the read uses ~`pct`% of the wall-time. `pct >= 100` yields zero
/// (no throttle); `pct` is clamped to `>= 1` (the registry already rejects 0; this is defense in
/// depth against a divide-by-zero). Result capped at `cap`.
#[must_use]
fn compute_sleep(elapsed: Duration, pct: u64, cap: Duration) -> Duration {
    if pct >= 100 {
        return Duration::ZERO;
    }
    let pct = pct.max(1) as u32;
    // sleep = elapsed * (100 - pct) / pct  (duty cycle = pct/100).
    let sleep = elapsed
        .saturating_mul(100 - pct)
        .checked_div(pct)
        .unwrap_or(Duration::ZERO);
    sleep.min(cap)
}

/// A between-chunks pacer for the persist read. Build once per BASE save; call [`Self::pace`] after
/// each fixed-size encode chunk with the LIVE `pct` (so a `CONFIG SET` mid-save applies).
#[derive(Debug)]
pub struct ChunkPacer {
    /// The instant the current (un-paced) chunk started encoding.
    chunk_start: Instant,
}

impl ChunkPacer {
    /// Start pacing (records the first chunk's start).
    #[must_use]
    pub fn new() -> Self {
        Self {
            chunk_start: Instant::now(),
        }
    }

    /// Pace after one encode chunk. If `pct >= 100` this is a TRUE no-op: NO clock read, NO sleep,
    /// so a default deployment (`pct == 100`) is zero-overhead and the sealed file is byte-identical.
    /// Otherwise sleep `chunk_time * (100 - pct) / pct` (capped at [`MAX_PACE_SLEEP`]) so the read
    /// holds a ~`pct`% duty cycle, then reset the chunk clock so the NEXT chunk measures only its
    /// own encode time.
    pub fn pace(&mut self, pct: u64) {
        if pct >= 100 {
            return; // fast path: no Instant read, no sleep.
        }
        let sleep = compute_sleep(self.chunk_start.elapsed(), pct, MAX_PACE_SLEEP);
        if !sleep.is_zero() {
            std::thread::sleep(sleep);
        }
        self.chunk_start = Instant::now();
    }
}

impl Default for ChunkPacer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pct_100_is_a_true_noop() {
        // No throttle: zero sleep regardless of how long the chunk took.
        assert_eq!(
            compute_sleep(Duration::from_millis(50), 100, MAX_PACE_SLEEP),
            Duration::ZERO
        );
        assert_eq!(
            compute_sleep(Duration::from_secs(10), 100, MAX_PACE_SLEEP),
            Duration::ZERO
        );
    }

    #[test]
    fn pct_50_sleeps_one_to_one() {
        // 50% duty cycle: sleep == encode time.
        assert_eq!(
            compute_sleep(Duration::from_millis(10), 50, Duration::from_secs(1)),
            Duration::from_millis(10)
        );
    }

    #[test]
    fn pct_25_sleeps_three_to_one() {
        // 25% duty cycle: sleep == 3x encode time.
        assert_eq!(
            compute_sleep(Duration::from_millis(10), 25, Duration::from_secs(1)),
            Duration::from_millis(30)
        );
    }

    #[test]
    fn cap_bounds_the_sleep() {
        // pct=1 would be 99x the encode time; the cap bounds it so shutdown/CONFIG SET stay live.
        let cap = Duration::from_millis(100);
        assert_eq!(compute_sleep(Duration::from_millis(10), 1, cap), cap);
    }

    #[test]
    fn pct_zero_is_clamped_not_a_divide_by_zero() {
        // The registry rejects 0, but be defensive: pct=0 clamps to 1 (no panic, finite result).
        let cap = Duration::from_millis(100);
        assert_eq!(compute_sleep(Duration::from_millis(10), 0, cap), cap);
    }

    #[test]
    fn zero_elapsed_yields_zero_sleep() {
        assert_eq!(
            compute_sleep(Duration::ZERO, 10, Duration::from_secs(1)),
            Duration::ZERO
        );
    }
}
