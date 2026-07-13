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
/// Opens `connections` connections spread evenly across `endpoints`, runs each in its
/// own task looping ops until `duration` elapses (deadline read from `env.now()`), sums
/// the completed ops, and computes QPS over the actual elapsed wall time.
///
/// ## Cluster-aware / zero-hop mode (multiple endpoints)
///
/// With ONE endpoint this is the classic single-target load. With `N > 1` endpoints the
/// generator is CLUSTER-AWARE: connection `i` is bound to `endpoints[i % N]` and draws its
/// keys from a DISJOINT partition `p = i % N` of the keyspace (`workload.keyspace()` keys
/// per partition, offset by `p * keyspace`), so every key deterministically lands on the
/// endpoint that owns it. Against `N` separately-addressable single-shard servers this is
/// ZERO-HOP -- it measures the ceiling a perfectly-routed client reaches, isolating the
/// cost of the cross-shard hop that a single-endpoint client pays on a multi-shard node.
/// The reported `keyspace` is the aggregate across partitions (`per-partition * N`).
///
/// # Errors
///
/// Returns an error if a connection cannot be established or a request/reply fails.
pub async fn run(
    endpoints: &[(String, u16)],
    connections: usize,
    duration: Duration,
    seed: u64,
    workload: Workload,
    pipeline: usize,
) -> std::io::Result<ClosedLoopResult> {
    let env = Arc::new(SystemEnv::new());
    let connections = connections.max(1);
    // Pipeline depth: 1 is the classic one-op-per-round-trip loop (byte-identical to the
    // pre-feature hot path). N>1 sends N commands in ONE write and reads N replies, which
    // amortizes the per-op syscall/round-trip so the throughput pass can measure batching.
    let pipeline = pipeline.max(1);
    // The endpoints to spread connections across (fall back to loopback:6379 if somehow
    // empty). `n_endpoints` partitions the keyspace: connection `i` owns partition `i % n`.
    let endpoints: Vec<(String, u16)> = if endpoints.is_empty() {
        vec![("127.0.0.1".to_string(), 6379)]
    } else {
        endpoints.to_vec()
    };
    let n_endpoints = endpoints.len();
    // Each partition draws from `[0, per_partition)` and is offset into a disjoint range,
    // so the aggregate distinct keyspace is `per_partition * n_endpoints`.
    let per_partition = workload.keyspace();
    let workload = Arc::new(workload);

    let start = env.now();
    let deadline = start.saturating_add(duration);

    let mut handles = Vec::with_capacity(connections);
    for conn_index in 0..connections {
        let env = Arc::clone(&env);
        let workload = Arc::clone(&workload);
        // Bind this connection to its partition's endpoint; draw keys from that partition's
        // disjoint index range so every request is local to the owning endpoint (zero-hop).
        let partition = conn_index % n_endpoints;
        let (host, port) = endpoints[partition].clone();
        let base = partition as u64 * per_partition;
        // Per-connection seeded stream: distinct, reproducible.
        let stream_seed = seed.wrapping_add(conn_index as u64);
        handles.push(tokio::spawn(async move {
            connection_loop(
                &host,
                port,
                base,
                deadline,
                stream_seed,
                &workload,
                &env,
                pipeline,
            )
            .await
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
            // The AGGREGATE distinct keyspace across all partitions (single endpoint => the
            // per-partition value itself, so this is unchanged for the classic single-target run).
            keyspace: per_partition.saturating_mul(n_endpoints as u64),
            theta: workload.theta(),
            read_ratio: workload.read_ratio(),
            value_size: workload.value_size(),
            duration_secs: duration.as_secs_f64(),
        },
        connections,
        pipeline,
        total_ops,
        elapsed_secs,
        qps,
    })
}

/// One connection's hot loop: issue ops back-to-back until the deadline, returning
/// the number of completed ops. The deadline is checked against `env.now()` after
/// each op (cheap monotonic read), so the loop stops promptly when `D` elapses.
///
/// At `pipeline == 1` this is the exact one-op-per-round-trip loop (byte-identical to
/// the pre-feature hot path). At `pipeline == N` each iteration draws N ops, sends the
/// N commands in ONE write, and reads N replies (RESP pipelining), counting `ops += N`;
/// the deadline is checked between batches (a partial final batch is fine).
#[allow(clippy::too_many_arguments)]
async fn connection_loop(
    host: &str,
    port: u16,
    base: u64,
    deadline: ironcache_env::Monotonic,
    stream_seed: u64,
    workload: &Workload,
    env: &SystemEnv,
    pipeline: usize,
) -> std::io::Result<u64> {
    let mut conn = Conn::connect(host, port).await?;
    let mut rng = SplitMix64::new(stream_seed);
    let value = workload.value_bytes();
    let mut ops: u64 = 0;
    // `base` offsets this connection's per-partition key index into its DISJOINT global range
    // (zero for a single-endpoint run, so `base + idx == idx` and the key bytes are unchanged).

    if pipeline <= 1 {
        // Depth 1: the original hot path, unchanged.
        loop {
            if env.now() >= deadline {
                break;
            }
            match workload.next_op(&mut rng) {
                Op::Get(idx) => {
                    let key = workload.key_bytes(base + idx);
                    conn.get(&key).await?;
                }
                Op::Set(idx) => {
                    let key = workload.key_bytes(base + idx);
                    conn.set(&key, &value).await?;
                }
            }
            ops += 1;
        }
        return Ok(ops);
    }

    // Depth N: draw N ops, build N commands, pipeline them in one write, read N replies.
    // The per-iteration `keys` buffer owns the key bytes so the borrowed `&[&[u8]]` batch
    // slices stay valid for the single `pipeline` call; both are reused each iteration to
    // avoid reallocating in the hot loop.
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(pipeline);
    let mut is_set: Vec<bool> = Vec::with_capacity(pipeline);
    loop {
        if env.now() >= deadline {
            break;
        }
        keys.clear();
        is_set.clear();
        for _ in 0..pipeline {
            match workload.next_op(&mut rng) {
                Op::Get(idx) => {
                    keys.push(workload.key_bytes(base + idx));
                    is_set.push(false);
                }
                Op::Set(idx) => {
                    keys.push(workload.key_bytes(base + idx));
                    is_set.push(true);
                }
            }
        }
        // Borrow the owned key bytes into the RESP arg slices for this batch.
        let batch: Vec<[&[u8]; 3]> = keys
            .iter()
            .zip(is_set.iter())
            .map(|(key, &set)| {
                if set {
                    [b"SET".as_slice(), key.as_slice(), value.as_slice()]
                } else {
                    // GET is a 2-arg command; pad the unused slot with an empty slice and
                    // pass only the first two args below.
                    [b"GET".as_slice(), key.as_slice(), b"".as_slice()]
                }
            })
            .collect();
        let cmds: Vec<&[&[u8]]> = batch
            .iter()
            .zip(is_set.iter())
            .map(|(args, &set)| if set { &args[..3] } else { &args[..2] })
            .collect();
        let replies = conn.pipeline(&cmds).await?;
        ops += replies.len() as u64;
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
            &[("127.0.0.1".to_string(), stub.port)],
            4,
            Duration::from_millis(200),
            0x00AB_CDEF,
            wl,
            1,
        )
        .await
        .expect("closed-loop run");

        assert!(res.total_ops > 0, "should complete some ops");
        assert!(res.qps > 0.0, "QPS should be positive");
        assert_eq!(res.connections, 4);
        assert_eq!(res.pipeline, 1);
        assert_eq!(res.params.mode, "closed");
        // The stub's reply counter should be at least the ops we counted (the client
        // counts a completed reply per op).
        assert!(
            stub.replies.load(std::sync::atomic::Ordering::Relaxed) >= res.total_ops,
            "stub replied to every counted op"
        );
    }

    #[tokio::test]
    async fn closed_loop_pipeline_depth_counts_every_op_and_records_depth() {
        // A depth-8 run against the stub must still count one op per completed reply and
        // echo the pipeline depth. The stub replies one frame per request even when a
        // batch arrives coalesced in a single read, so ops must equal the stub's replies.
        let stub = testutil::spawn(None).await;
        let wl = Workload::new(1000, 0.99, 0.9, 128);
        let res = run(
            &[("127.0.0.1".to_string(), stub.port)],
            4,
            Duration::from_millis(200),
            0x00AB_CDEF,
            wl,
            8,
        )
        .await
        .expect("closed-loop pipelined run");

        assert!(res.total_ops > 0, "should complete some ops");
        assert!(res.qps > 0.0, "QPS should be positive");
        assert_eq!(res.pipeline, 8, "the depth is recorded in the result");
        // Depth-N counts N per batch, one per reply: it never exceeds the stub's replies.
        assert!(
            stub.replies.load(std::sync::atomic::Ordering::Relaxed) >= res.total_ops,
            "stub replied to every counted op even under pipelining"
        );
    }

    #[tokio::test]
    async fn cluster_aware_mode_spreads_connections_across_endpoints() {
        // Zero-hop mode: with TWO endpoints and an even connection count, both endpoints must
        // receive load (connections are bound round-robin), and the reported keyspace is the
        // aggregate across the two partitions (per-partition * 2).
        use std::sync::atomic::Ordering;
        let stub_a = testutil::spawn(None).await;
        let stub_b = testutil::spawn(None).await;
        let per_partition = 500;
        let wl = Workload::new(per_partition, 0.99, 0.9, 128);
        let res = run(
            &[
                ("127.0.0.1".to_string(), stub_a.port),
                ("127.0.0.1".to_string(), stub_b.port),
            ],
            4, // 4 connections -> 2 per endpoint
            Duration::from_millis(200),
            0x00AB_CDEF,
            wl,
            1,
        )
        .await
        .expect("cluster-aware closed-loop run");

        assert!(res.total_ops > 0, "should complete some ops");
        // Both endpoints served load (each got 2 of the 4 connections, so > 0 replies).
        assert!(
            stub_a.replies.load(Ordering::Relaxed) > 0,
            "endpoint A received no ops"
        );
        assert!(
            stub_b.replies.load(Ordering::Relaxed) > 0,
            "endpoint B received no ops"
        );
        // The reported keyspace is the aggregate: per-partition * n_endpoints.
        assert_eq!(res.params.keyspace, per_partition * 2);
    }
}
