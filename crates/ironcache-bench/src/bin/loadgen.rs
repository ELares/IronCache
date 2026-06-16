// SPDX-License-Identifier: MIT OR Apache-2.0
//! `loadgen`: the IronCache macro load generator (BENCHMARK.md #8,
//! PERF_REGRESSION_GATE.md #159).
//!
//! A thin clap CLI over the `ironcache-bench` library. It drives a running
//! IronCache (or any RESP server) over TCP with a seeded zipf workload in one of two
//! passes, which are NEVER conflated:
//!
//! - `--mode closed`: N concurrent connections loop request->reply as fast as
//!   possible for a duration; reports peak throughput (QPS).
//! - `--mode open`: a wrk2-style constant-rate pass that measures each request's
//!   latency from its INTENDED send time (free of coordinated omission) and reports
//!   p50/p99/p999/p9999 in microseconds.
//!
//! All logic lives in the library (`workload`/`client`/`closed_loop`/`open_loop`/
//! `report`); this binary only parses flags and prints the result JSON. The seed is
//! echoed into the output so a run is reproducible.
//!
//! ## Determinism (ADR-0003, invariant 2)
//!
//! No time/rand here either: timing goes through `ironcache_env` inside the library
//! and the workload draws from a seeded `SplitMix64`. The async runtime is a
//! multi-thread tokio (the load generator is a CLIENT whose job is to saturate
//! connections; it is not the shared-nothing engine).
//!
//! ## Runtime sizing (open-loop spin precision)
//!
//! The open-loop scheduler busy-spins (no `.await`) for the final sub-millisecond of
//! each request's wait to get microsecond dispatch precision (see
//! [`ironcache_bench::open_loop::wait_until`]). A spinning task occupies a worker
//! thread, so the runtime is sized to `connections + 1` worker threads (capped at a
//! sane maximum) so a spinning connection never starves the others. This is correct
//! for the CLIENT (it is not the shared-nothing engine, which is current-thread).

#![forbid(unsafe_code)]

use core::time::Duration;
use std::fs::File;
use std::io::{BufWriter, Write};

use clap::{Parser, ValueEnum};
use ironcache_bench::report::write_histogram_percentiles;
use ironcache_bench::workload::Workload;
use ironcache_bench::{closed_loop, open_loop};

/// The two measurement passes. They are exclusive: a single invocation runs exactly
/// one (peak throughput XOR the latency tail), because conflating them is the
/// methodological error this tool exists to avoid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// N connections at full tilt; reports peak QPS.
    Closed,
    /// Constant-rate, coordinated-omission-free; reports the latency tail.
    Open,
}

/// A default seed constant. Fixed so that, absent `--seed`, the workload is still
/// byte-reproducible run to run (it is a well-known splitmix64-friendly constant).
const DEFAULT_SEED: u64 = 0x5DEE_CE66_D1CE_5EED;

/// The upper bound on tokio worker threads for the client runtime. The runtime is
/// sized to `connections + 1` (so a spinning open-loop connection never starves the
/// others) but capped here so a pathological `--connections` does not request an
/// absurd thread count.
const MAX_WORKER_THREADS: usize = 256;

/// A clap `value_parser` for the `f64` flags (`--rate`/`--theta`/`--read-ratio`/
/// `--duration-secs`) that REJECTS non-finite values. Without this, clap would happily
/// parse `nan`/`inf` and they would serialize into invalid JSON like
/// `"read_ratio":NaN`. The parser accepts any string `f64::from_str` accepts, then
/// errors if the result is not finite.
fn finite_f64(s: &str) -> Result<f64, String> {
    let v: f64 = s
        .parse()
        .map_err(|_| format!("`{s}` is not a valid number"))?;
    if v.is_finite() {
        Ok(v)
    } else {
        Err(format!("`{s}` must be a finite number (got {v})"))
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "loadgen",
    about = "IronCache macro load generator: closed-loop peak QPS or open-loop latency tails."
)]
struct Args {
    /// Which pass to run.
    #[arg(long, value_enum)]
    mode: Mode,

    /// Target host.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Target port.
    #[arg(long, default_value_t = 6379)]
    port: u16,

    /// Workload RNG seed (a fixed seed makes the workload byte-reproducible).
    #[arg(long, default_value_t = DEFAULT_SEED)]
    seed: u64,

    /// Number of distinct keys.
    #[arg(long, default_value_t = 1_000_000)]
    keyspace: u64,

    /// Zipf exponent (skew). 0.99 is the YCSB default; larger is more skewed.
    #[arg(long, default_value_t = 0.99, value_parser = finite_f64)]
    theta: f64,

    /// Read fraction of the op-mix (0.9 => 90% GET / 10% SET).
    #[arg(long, default_value_t = 0.9, value_parser = finite_f64)]
    read_ratio: f64,

    /// SET value size in bytes (clamped to the locked 64..=1024 range).
    #[arg(long, default_value_t = 128)]
    value_size: usize,

    /// Run duration in seconds.
    #[arg(long, default_value_t = 10.0, value_parser = finite_f64)]
    duration_secs: f64,

    /// Concurrent connections. In `closed` mode this is the load fan-out; in `open`
    /// mode it is the size of the dispatch pool that services the fixed schedule.
    #[arg(long, default_value_t = 50)]
    connections: usize,

    /// Target request rate in ops/sec (open mode only).
    #[arg(long, default_value_t = 50_000.0, value_parser = finite_f64)]
    rate: f64,

    /// Where to write the result JSON: a file path, or `-`/omitted for stdout.
    #[arg(long, default_value = "-")]
    out: String,

    /// Where to write the HdrHistogram percentile artifact (open mode only). Omitted
    /// means no artifact is written.
    #[arg(long)]
    hist: Option<String>,
}

/// Synchronous entry point: build a multi-thread tokio runtime sized to the connection
/// count (so an open-loop connection that busy-spins for dispatch precision never
/// starves the others) and drive the async run on it. We build the runtime manually
/// rather than with `#[tokio::main]` because the worker-thread count depends on a
/// parsed flag (`--connections`).
fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Size the runtime to connections + 1, capped, so every spinning connection has a
    // worker thread plus one for the scheduler/IO. Floor at 2 so even `--connections 1`
    // has headroom.
    let worker_threads = (args.connections + 1).clamp(2, MAX_WORKER_THREADS);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?;

    runtime.block_on(run(args))
}

/// The async body: dispatch to the requested pass, write the artifact, print the JSON,
/// and (for a generator-limited open-loop run) warn loudly that the latencies reflect
/// the generator, not the server.
async fn run(args: Args) -> anyhow::Result<()> {
    let duration = Duration::from_secs_f64(args.duration_secs.max(0.0));
    let workload = Workload::new(args.keyspace, args.theta, args.read_ratio, args.value_size);

    let json = match args.mode {
        Mode::Closed => {
            let res = closed_loop::run(
                &args.host,
                args.port,
                args.connections,
                duration,
                args.seed,
                workload,
            )
            .await?;
            res.to_json()
        }
        Mode::Open => {
            let (res, hist) = open_loop::run(
                &args.host,
                args.port,
                args.rate,
                duration,
                args.connections,
                args.seed,
                workload,
            )
            .await?;
            if let Some(path) = args.hist.as_deref() {
                let f = File::create(path)?;
                let mut w = BufWriter::new(f);
                write_histogram_percentiles(&mut w, &hist)?;
                w.flush()?;
            }
            // A generator-limited run reports latencies that reflect the GENERATOR
            // backing up, not the server. Warn loudly so the JSON is not misread.
            if res.saturated {
                eprintln!(
                    "WARNING: generator-limited run. Achieved {:.0} ops/sec of a target \
                     {:.0} ops/sec ({:.1}%) over {:.3}s with {} connection(s). The latency \
                     tail reflects the GENERATOR (too few connections / server too slow), \
                     NOT the server. Increase --connections or lower --rate.",
                    res.achieved_rate,
                    res.target_rate,
                    100.0 * res.achieved_rate / res.target_rate,
                    res.elapsed_secs,
                    args.connections,
                );
            }
            res.to_json()
        }
    };

    if args.out == "-" {
        println!("{json}");
    } else {
        let mut f = File::create(&args.out)?;
        writeln!(f, "{json}")?;
    }
    Ok(())
}
