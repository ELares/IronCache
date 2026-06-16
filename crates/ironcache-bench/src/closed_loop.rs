// SPDX-License-Identifier: MIT OR Apache-2.0
//! The closed-loop pass: peak throughput.
//!
//! `C` concurrent connections each loop request->reply as fast as possible for a
//! duration `D`, counting completed ops. There is no rate limit and no pacing: this
//! pass measures the saturation point of the server under this client, so the
//! reported number is `total_ops / wall_seconds` (peak QPS). It deliberately says
//! NOTHING about latency tails (a closed loop hides tail latency by construction,
//! since a slow reply simply slows that one connection's next request); the
//! open-loop pass is the one that measures latency without coordinated omission.
//!
//! ## Determinism (ADR-0003, invariant 2)
//!
//! - The deadline is measured with `ironcache_env::SystemEnv::now()` (the sanctioned
//!   monotonic seam), never `Instant::now`.
//! - Each connection draws its workload from a SEEDED `ironcache_env::SplitMix64`
//!   derived from `seed + conn_index`, so the per-connection op stream is
//!   reproducible (the interleaving across connections is not, but the QPS metric
//!   does not depend on interleaving).

#![forbid(unsafe_code)]

use core::time::Duration;
use std::sync::Arc;

use ironcache_env::{Clock, SplitMix64, SystemEnv};

use crate::client::Conn;
use crate::report::{ClosedLoopResult, RunParams};
use crate::workload::{Op, Workload};

/// Run the closed-loop pass and return the throughput result.
///
/// Opens `connections` connections to `host:port`, runs each in its own task
/// looping ops until `duration` elapses (deadline read from `env.now()`), sums the
/// completed ops, and computes QPS over the actual elapsed wall time.
///
/// # Errors
///
/// Returns an error if a connection cannot be established or a request/reply fails.
pub async fn run(
    host: &str,
    port: u16,
    connections: usize,
    duration: Duration,
    seed: u64,
    workload: Workload,
) -> std::io::Result<ClosedLoopResult> {
    let env = Arc::new(SystemEnv::new());
    let connections = connections.max(1);
    let workload = Arc::new(workload);
    let host = host.to_string();

    let start = env.now();
    let deadline = start.saturating_add(duration);

    let mut handles = Vec::with_capacity(connections);
    for conn_index in 0..connections {
        let env = Arc::clone(&env);
        let workload = Arc::clone(&workload);
        let host = host.clone();
        // Per-connection seeded stream: distinct, reproducible.
        let stream_seed = seed.wrapping_add(conn_index as u64);
        handles.push(tokio::spawn(async move {
            connection_loop(&host, port, deadline, stream_seed, &workload, &env).await
        }));
    }

    let mut total_ops: u64 = 0;
    for h in handles {
        // A connection task returns its op count or an I/O error; propagate the first error.
        total_ops += h
            .await
            .map_err(|e| std::io::Error::other(format!("join error: {e}")))??;
    }

    let elapsed = env.now().saturating_duration_since(start);
    let elapsed_secs = elapsed.as_secs_f64();
    let qps = if elapsed_secs > 0.0 {
        total_ops as f64 / elapsed_secs
    } else {
        0.0
    };

    Ok(ClosedLoopResult {
        params: RunParams {
            mode: "closed",
            seed,
            keyspace: workload.keyspace(),
            theta: workload.theta(),
            read_ratio: workload.read_ratio(),
            value_size: workload.value_size(),
            duration_secs: duration.as_secs_f64(),
        },
        connections,
        total_ops,
        elapsed_secs,
        qps,
    })
}

/// One connection's hot loop: issue ops back-to-back until the deadline, returning
/// the number of completed ops. The deadline is checked against `env.now()` after
/// each op (cheap monotonic read), so the loop stops promptly when `D` elapses.
async fn connection_loop(
    host: &str,
    port: u16,
    deadline: ironcache_env::Monotonic,
    stream_seed: u64,
    workload: &Workload,
    env: &SystemEnv,
) -> std::io::Result<u64> {
    let mut conn = Conn::connect(host, port).await?;
    let mut rng = SplitMix64::new(stream_seed);
    let value = workload.value_bytes();
    let mut ops: u64 = 0;

    loop {
        if env.now() >= deadline {
            break;
        }
        match workload.next_op(&mut rng) {
            Op::Get(idx) => {
                let key = workload.key_bytes(idx);
                conn.get(&key).await?;
            }
            Op::Set(idx) => {
                let key = workload.key_bytes(idx);
                conn.set(&key, &value).await?;
            }
        }
        ops += 1;
    }
    Ok(ops)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil;

    #[tokio::test]
    async fn closed_loop_against_stub_makes_progress() {
        // A short closed-loop run against the no-delay stub: a few connections for
        // 200ms must complete some ops and report a positive QPS.
        let stub = testutil::spawn(None).await;
        let wl = Workload::new(1000, 0.99, 0.9, 128);
        let res = run(
            "127.0.0.1",
            stub.port,
            4,
            Duration::from_millis(200),
            0x00AB_CDEF,
            wl,
        )
        .await
        .expect("closed-loop run");

        assert!(res.total_ops > 0, "should complete some ops");
        assert!(res.qps > 0.0, "QPS should be positive");
        assert_eq!(res.connections, 4);
        assert_eq!(res.params.mode, "closed");
        // The stub's reply counter should be at least the ops we counted (the client
        // counts a completed reply per op).
        assert!(
            stub.replies.load(std::sync::atomic::Ordering::Relaxed) >= res.total_ops,
            "stub replied to every counted op"
        );
    }
}
