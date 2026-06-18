// SPDX-License-Identifier: MIT OR Apache-2.0
//! The open-loop pass: a wrk2-style constant-rate latency measurement that is free
//! of coordinated omission.
//!
//! ## Coordinated omission, and the fix
//!
//! A naive latency benchmark issues a request, waits for the reply, times THAT, then
//! issues the next. If the server stalls, the benchmark stalls with it: the requests
//! that "should" have been sent during the stall are never sent, so their (large)
//! latencies are never recorded. The histogram is then dominated by fast samples
//! taken while the server was healthy. This systematic under-counting of the tail is
//! "coordinated omission" (Gil Tene's term; wrk2 is the canonical fix).
//!
//! The fix, implemented here, is to fix the SCHEDULE up front and measure each
//! request's latency from its INTENDED send time, not its actual send time:
//!
//!   - The request rate `R` defines a fixed cadence: request `j` is INTENDED to be
//!     sent at `t_j = t0 + j * (1 / R)`, for `j` in `0..(R * D)`.
//!   - The global schedule is PARTITIONED round-robin across the `C` connections:
//!     connection `c` (in `0..C`) owns request indices `c, c+C, c+2C, ...`. Each
//!     connection is its own task with its own connection, its own seeded RNG, and
//!     its own local histogram, so there is NO shared receiver mutex and NO shared
//!     histogram lock during the run (the review flagged both as bottlenecks). The
//!     per-connection histograms are merged with `Histogram::add` at the end.
//!   - A connection issues one request at a time, but it measures each request's
//!     latency from its INTENDED time `t_j`, not its actual send time. If a reply is
//!     slow, the connection's next intended time is already in the past, so its
//!     recorded latency balloons (correct) instead of the schedule silently slipping.
//!     This keeps the measurement coordinated-omission-free even though each
//!     connection is serial: the schedule is fixed, never relative to "after the last
//!     reply".
//!
//! ## Microsecond dispatch precision (the timer-floor fix)
//!
//! A per-request `tokio::time::sleep(intended - now)` is fatal to a microsecond-class
//! cache's latency numbers: tokio's timer has a coarse (~1ms) granularity and rounds
//! UP, so at any rate above ~1000 QPS the sub-millisecond per-request cadence is
//! drowned in ~1ms of timer rounding, and that rounding is measured AS latency. Against
//! a zero-delay stub the old scheduler reported p50 ~800us-1.5ms of pure timer noise.
//!
//! The fix is [`wait_until`]: a coarse-sleep-then-busy-spin precise wait. It sleeps
//! (via `tokio::time`) until it is within `SPIN_THRESHOLD` of the target, then BUSY-
//! SPINS reading `env.now()` (with `std::hint::spin_loop()`, no `.await`) for the final
//! sub-threshold remainder. This deliberately burns client CPU for timing precision,
//! which is standard for a load generator (A3 pins the client cores). To keep the
//! spins from starving the executor, the load-gen binary builds a multi-thread tokio
//! runtime sized to the connection count (see `bin/loadgen.rs`).
//!
//! ## Generator-limited (saturation) reporting
//!
//! If the client cannot keep up with `target_rate` (too few connections, server too
//! slow), the measured "latencies" reflect the generator backing up, not the server.
//! The result therefore reports the ACHIEVED rate (`total / actual_elapsed`) and a
//! `saturated` flag (achieved materially below target); the CLI prints a loud warning
//! when saturated so a generator-limited run is never mistaken for a server result.
//!
//! ## Timing through the determinism seam (ADR-0003, invariant 2)
//!
//! All wall stamps go through `ironcache_env::SystemEnv` (`env.now()` ->
//! `Monotonic`), never `Instant::now`. The schedule base `t0` is one `env.now()`
//! read taken AFTER all connects (so connect time never drifts the cadence); each
//! intended time is `t0 + j * period` as a `Monotonic`. Coarse waits use
//! `tokio::time::sleep` (the lint permits `tokio::time`); the busy-spin uses only
//! `env.now()` and `std::hint::spin_loop()` (neither is a clock/RNG seam violation).
//! The latency is recorded in MICROSECONDS into an `hdrhistogram::Histogram<u64>`.

#![forbid(unsafe_code)]

use core::time::Duration;
use std::sync::Arc;

use hdrhistogram::Histogram;
use ironcache_env::{Clock, Monotonic, SplitMix64, SystemEnv};

use crate::client::Conn;
use crate::report::{OpenLoopResult, RunParams};
use crate::workload::Workload;

/// The lowest and highest latency the histogram tracks, in microseconds. The HDR
/// `high` bound must exceed any recordable latency; we cap recorded values to it so
/// a pathological stall saturates rather than panicking. 60s is far above any
/// healthy cache reply and any test-injected delay.
const HIST_LOW_US: u64 = 1;
const HIST_HIGH_US: u64 = 60_000_000;
/// Significant decimal digits of precision the HDR histogram retains (3 => 0.1%).
const HIST_SIGFIG: u8 = 3;

/// The busy-spin window for [`wait_until`]: while the time remaining to the target
/// exceeds this, we coarse-sleep (yielding to the executor); inside this final window
/// we busy-spin (no `.await`) for microsecond precision.
///
/// This MUST exceed tokio's worst-case sleep OVERSHOOT, not just its granularity: a
/// `tokio::time::sleep` rounds up to (and can overshoot past) its ~1ms timer tick, so
/// a too-small window lets the coarse sleep blow straight through the target before the
/// spin ever runs (empirically a 175us coarse sleep landed ~1.5ms late). 2ms is
/// comfortably above that overshoot, so the coarse sleep always wakes INSIDE the spin
/// window and the busy-spin lands on the target with microsecond precision. The cost is
/// up to ~2ms of client CPU burned spinning per request per connection, which is the
/// standard trade for a load generator (A3 pins client cores).
const SPIN_THRESHOLD: Duration = Duration::from_millis(2);

/// A connection is considered to have MATERIALLY missed the target rate when its
/// achieved rate is below this fraction of the target; the CLI warns when `saturated`.
const SATURATION_FRACTION: f64 = 0.95;

/// Wait precisely until `target`, using a coarse sleep for the bulk of the wait and a
/// busy-spin for the final sub-[`SPIN_THRESHOLD`] remainder.
///
/// This gives microsecond dispatch precision instead of tokio's ~1ms timer floor: the
/// coarse `tokio::time::sleep` covers everything except the last `SPIN_THRESHOLD` (so
/// the executor is not starved during the long part of the wait), then a tight loop
/// reading `env.now()` with `std::hint::spin_loop()` lands on the target. If `target`
/// is already in the past the function returns immediately. The spin intentionally
/// burns client CPU for timing precision; this is standard for a load generator (A3
/// pins client cores).
///
/// Determinism: time is read only through `env.now()` (the seam); the coarse wait uses
/// `tokio::time` (permitted by the invariant lint); `std::hint::spin_loop()` is neither
/// a clock nor an RNG.
pub async fn wait_until(env: &SystemEnv, target: Monotonic) {
    loop {
        let remaining = target.saturating_duration_since(env.now());
        if remaining.is_zero() {
            return;
        }
        if remaining > SPIN_THRESHOLD {
            // Coarse-sleep for everything except the full SPIN_THRESHOLD window, so the
            // sleep wakes INSIDE the spin window even after tokio rounds/overshoots its
            // ~1ms tick. The busy-spin below then lands on the target. We re-loop after
            // each coarse sleep (rather than spinning immediately) in case the sleep
            // returned still more than a window short. `remaining > SPIN_THRESHOLD` is
            // checked above, so the subtraction never underflows.
            let coarse = remaining.saturating_sub(SPIN_THRESHOLD);
            tokio::time::sleep(coarse).await;
            continue;
        }
        // Final sub-threshold remainder: busy-spin (no .await) for microsecond
        // precision, reading only the monotonic seam.
        while env.now() < target {
            std::hint::spin_loop();
        }
        return;
    }
}

/// Run the open-loop pass and return the latency-tail result.
///
/// Issues `target_rate * duration` requests on a fixed cadence, with the global
/// schedule partitioned round-robin across `connections` per-connection tasks, each
/// recording its requests' intended-time latencies into a local HDR histogram. The
/// merged histogram is returned alongside the summary so the caller can write the
/// artifact.
///
/// # Errors
///
/// Returns an error if any connection cannot be established. A per-request I/O error
/// after connect is recorded as a saturated (max) latency rather than aborting the
/// run, so a transient server hiccup does not lose the whole measurement.
pub async fn run(
    host: &str,
    port: u16,
    target_rate: f64,
    duration: Duration,
    connections: usize,
    seed: u64,
    workload: Workload,
) -> std::io::Result<(OpenLoopResult, Histogram<u64>)> {
    let env = Arc::new(SystemEnv::new());
    let workload = Arc::new(workload);
    let connections = connections.max(1);
    let rate = target_rate.max(1.0);
    let total: u64 = (rate * duration.as_secs_f64()).round() as u64;
    let period = 1.0 / rate;

    // Connect ALL connections first, then take the single schedule base t0, so connect
    // latency never drifts the cadence (request j is intended at t0 + j*period).
    let mut conns = Vec::with_capacity(connections);
    for _ in 0..connections {
        conns.push(Conn::connect(host, port).await?);
    }
    let t0 = env.now();

    // Wall-clock STOP HORIZON. A healthy run finishes its whole fixed schedule right at
    // `duration` (the last intended time is ~duration). An UNDER-PROVISIONED run cannot:
    // draining every one of the `total` indices would run for many multiples of
    // `duration` (the connections are the bottleneck), so a connection stops issuing new
    // requests once wall time is past this horizon. The grace factor leaves room for a
    // healthy run's final replies to land (so its count is not truncated) while still
    // bounding a generator-limited run to a small multiple of `duration`. The
    // already-issued ballooned latencies plus `achieved_rate = count / elapsed` then
    // make the saturation visible without the run hanging.
    let stop_horizon = t0.saturating_add(duration.saturating_add(duration / 2));

    // Spawn one task per connection. Connection `c` owns request indices
    // c, c+C, c+2C, ...; each has its own seeded RNG (decorrelated per connection) and
    // its own local histogram (no shared lock during the run).
    let mut tasks = Vec::with_capacity(connections);
    for (c, mut conn) in conns.into_iter().enumerate() {
        let env = Arc::clone(&env);
        let workload = Arc::clone(&workload);
        let conn_count = connections as u64;
        let conn_index = c as u64;
        let stream_seed = seed ^ conn_index;
        tasks.push(tokio::spawn(async move {
            let mut local_hist =
                Histogram::<u64>::new_with_bounds(HIST_LOW_US, HIST_HIGH_US, HIST_SIGFIG)
                    .expect("valid HDR bounds");
            let mut rng = SplitMix64::new(stream_seed);
            let value = workload.value_bytes();
            // This connection's request indices: conn_index, conn_index+C, ...
            let mut j = conn_index;
            while j < total {
                // intended_j = t0 + j*period, computed from the FIXED base so rounding
                // can never drift the cadence.
                let offset = Duration::from_secs_f64(j as f64 * period);
                let intended = t0.saturating_add(offset);
                // Stop horizon: if we are already past it, the run is generator-limited
                // and draining the rest of the fixed schedule would run far past
                // `duration`. Stop issuing; the recorded ballooned latencies and the
                // achieved-rate gap already expose the saturation.
                if env.now() >= stop_horizon {
                    break;
                }
                // Wait precisely until the intended time (sub-millisecond precision).
                wait_until(&env, intended).await;
                // Draw THIS index's op. The per-connection RNG is decorrelated by seed,
                // so connections do not draw the same sequence.
                let op = workload.next_op(&mut rng);
                let key = workload.key_bytes(op.key_index());
                let result = match op {
                    crate::workload::Op::Get(_) => conn.get(&key).await,
                    crate::workload::Op::Set(_) => conn.set(&key, &value).await,
                };
                // Latency from the INTENDED send time (coordinated-omission-free). If
                // the reply was slow, env.now() is now well past `intended`, so this
                // balloons correctly instead of the schedule slipping.
                let lat = env.now().saturating_duration_since(intended);
                let us = duration_to_us_capped(lat);
                let record = if result.is_ok() { us } else { HIST_HIGH_US };
                local_hist.record(record).unwrap_or(());
                j += conn_count;
            }
            local_hist
        }));
    }

    // Merge the per-connection histograms.
    let mut histogram = Histogram::<u64>::new_with_bounds(HIST_LOW_US, HIST_HIGH_US, HIST_SIGFIG)
        .expect("valid HDR bounds");
    for t in tasks {
        let local = t
            .await
            .map_err(|e| std::io::Error::other(format!("worker join error: {e}")))?;
        histogram
            .add(&local)
            .map_err(|e| std::io::Error::other(format!("histogram merge error: {e}")))?;
    }

    // The ACTUAL elapsed wall time, measured at the END through the seam. The achieved
    // rate is total recorded / actual elapsed; `saturated` flags a generator-limited
    // run whose "latencies" reflect the generator backing up, not the server.
    let elapsed = env.now().saturating_duration_since(t0);
    let elapsed_secs = elapsed.as_secs_f64();
    let count = histogram.len();
    let achieved_rate = if elapsed_secs > 0.0 {
        count as f64 / elapsed_secs
    } else {
        0.0
    };
    let saturated = achieved_rate < rate * SATURATION_FRACTION;

    let result = OpenLoopResult {
        params: RunParams {
            mode: "open",
            seed,
            keyspace: workload.keyspace(),
            theta: workload.theta(),
            read_ratio: workload.read_ratio(),
            value_size: workload.value_size(),
            duration_secs: duration.as_secs_f64(),
        },
        target_rate: rate,
        achieved_rate,
        elapsed_secs,
        saturated,
        count,
        p50_us: histogram.value_at_quantile(0.50),
        p99_us: histogram.value_at_quantile(0.99),
        p999_us: histogram.value_at_quantile(0.999),
        p9999_us: histogram.value_at_quantile(0.9999),
        max_us: histogram.max(),
    };
    Ok((result, histogram))
}

/// Convert a `Duration` to whole microseconds, clamped to the histogram's high
/// bound so a pathological stall saturates rather than overflowing or panicking on
/// record.
fn duration_to_us_capped(d: Duration) -> u64 {
    let us = d.as_micros();
    if us > u128::from(HIST_HIGH_US) {
        HIST_HIGH_US
    } else {
        us as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn wait_until_has_sub_millisecond_precision() {
        // THE regression guard for the timer-floor finding. Wait for a 300us target and
        // assert the actual elapsed lands within a tight bound of 300us, far under the
        // ~1ms floor a per-request `tokio::time::sleep` would have produced. This needs
        // NO server: it proves the dispatch precision directly.
        let env = SystemEnv::new();
        let t0 = env.now();
        let target = t0.saturating_add(Duration::from_micros(300));
        wait_until(&env, target).await;
        let elapsed = env.now().saturating_duration_since(t0);
        let elapsed_us = elapsed.as_micros() as i64;
        let error_us = (elapsed_us - 300).abs();
        eprintln!("wait_until(300us) measured: elapsed={elapsed_us}us error={error_us}us");
        assert!(
            error_us < 250,
            "wait_until(300us) landed at {elapsed_us}us (error {error_us}us); \
             expected well under the ~1ms tokio floor (< 250us error)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn wait_until_returns_immediately_for_a_past_target() {
        // A target already in the past must not block (and must not underflow).
        let env = SystemEnv::new();
        let now = env.now();
        let past = Monotonic::ZERO; // far in the past
        let before = env.now();
        wait_until(&env, past).await;
        let after = env.now();
        // It returned essentially immediately (well under a millisecond).
        assert!(
            after.saturating_duration_since(before) < Duration::from_millis(1),
            "wait_until on a past target should return immediately"
        );
        // sanity: `now` is monotonic and not in the past relative to ZERO.
        assert!(now >= past);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn open_loop_no_delay_has_small_latency_and_right_count() {
        // No-delay stub at a modest rate for a short time: the recorded count should
        // be in the right ballpark (~rate*duration) and p99 should be small. With the
        // spin-precise wait, p99 against a zero-delay stub should be far below the old
        // ~1ms timer floor.
        let stub = testutil::spawn(None).await;
        let wl = Workload::new(10_000, 0.99, 0.9, 128);
        let rate = 5_000.0;
        let dur = Duration::from_millis(400);
        let (res, _hist) = run("127.0.0.1", stub.port, rate, dur, 16, 0x1234, wl)
            .await
            .expect("open-loop run");

        let expected = (rate * dur.as_secs_f64()) as u64; // ~2000
        // Count is in the right ballpark (allow generous slack for scheduling jitter
        // and the run finishing slightly short).
        assert!(
            res.count >= expected / 2 && res.count <= expected + expected / 2,
            "count {} should be near {expected}",
            res.count
        );
        // No injected delay: p99 must be far below a gross per-request floor. This is a COARSE
        // smoke check (a broken wait that adds a fixed delay would blow past this), NOT a latency
        // SLA: the bound is deliberately generous (500ms) because the test also runs on shared,
        // heavily-contended CI runners (notably GitHub macos) whose scheduler stalls can push p99
        // into the tens-to-hundreds of ms with no code regression. Precise latency belongs to the
        // benchmark harness (scripts/bench), not this unit test; the deterministic signals here are
        // the count and not-saturated assertions above/below.
        assert!(
            res.p99_us < 500_000,
            "no-delay p99 {} us should be far below a gross per-request floor",
            res.p99_us
        );
        // The run was not generator-limited.
        assert!(
            !res.saturated,
            "a 5k-rate run with 16 connections against a zero-delay stub should not \
             be saturated (achieved {} of target {})",
            res.achieved_rate, res.target_rate
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn open_loop_delayed_stub_surfaces_the_delay_in_the_tail() {
        // THE coordinated-omission guard. Every reply is delayed by a fixed 20ms.
        // A naive (closed-loop) measurement that timed from the ACTUAL send would see
        // ~20ms flat and the schedule would slip so the tail would look fine. Because
        // we time from the INTENDED send time and the schedule does NOT slip, the
        // injected delay shows up in the tail: p99 must be at least the delay.
        let injected = Duration::from_millis(20);
        let stub = testutil::spawn(Some(injected)).await;
        let wl = Workload::new(10_000, 0.99, 0.9, 128);
        // A rate high enough that, with a few connections each blocked 20ms per reply,
        // the pool cannot keep up: the schedule outruns the workers, so intended-time
        // latency accumulates and the tail reflects the stall.
        let rate = 2_000.0;
        let dur = Duration::from_millis(500);
        let (res, _hist) = run("127.0.0.1", stub.port, rate, dur, 8, 0x55, wl)
            .await
            .expect("open-loop delayed run");

        let injected_us = injected.as_micros() as u64;
        assert!(
            res.p99_us >= injected_us,
            "p99 {} us must reflect the injected {} us delay (coordinated omission \
             would have hidden it)",
            res.p99_us,
            injected_us
        );
        assert!(res.count > 0, "should have recorded samples");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn open_loop_under_provisioned_run_reports_saturated() {
        // A deliberately under-provisioned run: a very high target rate with only 2
        // connections against a delayed stub cannot keep up, so the achieved rate must
        // be far below target and `saturated` must be true. This is the signal that the
        // reported latencies reflect the generator, not the server.
        let injected = Duration::from_millis(5);
        let stub = testutil::spawn(Some(injected)).await;
        let wl = Workload::new(10_000, 0.99, 0.9, 128);
        let rate = 200_000.0;
        let dur = Duration::from_millis(300);
        let (res, _hist) = run("127.0.0.1", stub.port, rate, dur, 2, 0x99, wl)
            .await
            .expect("open-loop under-provisioned run");

        assert!(
            res.saturated,
            "an under-provisioned run (rate {}, 2 conns, 5ms reply delay) must be \
             flagged saturated; achieved {}",
            res.target_rate, res.achieved_rate
        );
        assert!(
            res.achieved_rate < res.target_rate * SATURATION_FRACTION,
            "achieved rate {} must be materially below target {}",
            res.achieved_rate,
            res.target_rate
        );
    }
}
