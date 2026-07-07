// SPDX-License-Identifier: MIT OR Apache-2.0
//! Out-of-band metrics / health endpoint acceptance tests (OBSERVABILITY.md, #152).
//!
//! These boot the REAL multi-shard `run_server` WITH the metrics endpoint enabled (the same
//! wiring `cmd_server` runs) on ephemeral ports and drive both the RESP listener (to move the
//! counters) and the HTTP `/metrics` + `/livez` + `/readyz` endpoints over real sockets, so they
//! exercise the whole path: shard counter mutation -> cross-thread registry aggregation ->
//! Prometheus render -> HTTP response. They also assert the DEFAULT (no metrics) boot starts NO
//! HTTP listener.

use ironcache::test_support::{run_server_for_test, run_server_with_metrics_for_test};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Grab a free TCP port (small TOCTOU window before the server re-binds; fine on loopback).
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Connect to the RESP port with a few short retries (shards bind asynchronously).
async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = s.set_nodelay(true);
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never came up on RESP port {port}");
}

/// Send `PING` and read `+PONG`.
async fn ping(client: &mut TcpStream) {
    client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut buf = [0u8; 7];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"+PONG\r\n");
}

/// Perform one HTTP/1.1 `GET path` against the metrics endpoint and return `(status_code, body)`.
/// Retries the connect briefly (the metrics thread binds asynchronously too).
async fn http_get(metrics_port: u16, path: &str) -> (u16, String) {
    let mut stream = None;
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", metrics_port)).await {
            stream = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut stream = stream.expect("metrics endpoint never came up");
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    // Parse the status code from the first line: "HTTP/1.1 <code> <reason>".
    let code: u16 = text
        .lines()
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    // Split headers from body at the blank line.
    let body = text
        .split_once("\r\n\r\n")
        .map_or(String::new(), |(_, b)| b.to_owned());
    (code, body)
}

/// `/metrics` returns 200 + valid Prometheus text, and the `commands_processed` counter reflects
/// the commands actually run over the RESP listener (the cross-shard aggregation is live).
#[tokio::test]
async fn metrics_endpoint_serves_prometheus_and_reflects_commands() {
    // The number of commands we drive over the RESP listener; the aggregated counter must be >= N.
    const N: usize = 11;
    let resp_port = free_port();
    let metrics_port = free_port();
    let set = run_server_with_metrics_for_test(resp_port, 4, metrics_port);

    // Drive a known number of commands across (possibly several) shards.
    let mut c = connect_retry(resp_port).await;
    for _ in 0..N {
        ping(&mut c).await;
    }
    drop(c);
    // The counter publish is in the per-command fold (synchronous), so a scrape now sees them.
    let (code, body) = http_get(metrics_port, "/metrics").await;
    assert_eq!(code, 200, "body: {body}");
    // Valid Prometheus exposition: HELP/TYPE headers + the expected metric names.
    assert!(
        body.contains("# TYPE ironcache_commands_processed_total counter"),
        "{body}"
    );
    assert!(
        body.contains("# TYPE ironcache_connected_clients gauge"),
        "{body}"
    );
    assert!(body.contains("ironcache_uptime_seconds"), "{body}");
    assert!(body.contains("ironcache_used_memory_bytes"), "{body}");
    // The aggregated processed counter is AT LEAST N (the PINGs); other handshake/internal
    // commands may add a few, so assert a lower bound the test fully controls.
    let line = body
        .lines()
        .find(|l| l.starts_with("ironcache_commands_processed_total "))
        .expect("processed counter present");
    let value: u64 = line.rsplit(' ').next().unwrap().parse().unwrap();
    assert!(value >= N as u64, "processed={value} < {N}; body: {body}");

    // #546 tail-latency histogram: the command-latency histogram series appears in the SAME scrape,
    // p99-graphable. Assert its shape (HELP/TYPE), that its `_count` reflects the driven commands,
    // and that the cumulative `+Inf` bucket equals `_count` (the load-bearing histogram invariant).
    assert!(
        body.contains("# TYPE ironcache_command_duration_seconds histogram"),
        "{body}"
    );
    let hcount_line = body
        .lines()
        .find(|l| l.starts_with("ironcache_command_duration_seconds_count "))
        .expect("histogram _count present");
    let hcount: u64 = hcount_line.rsplit(' ').next().unwrap().parse().unwrap();
    assert!(
        hcount >= N as u64,
        "hist _count={hcount} < {N}; body: {body}"
    );
    let inf_line = body
        .lines()
        .find(|l| l.starts_with("ironcache_command_duration_seconds_bucket{le=\"+Inf\"}"))
        .expect("histogram +Inf bucket present");
    let inf: u64 = inf_line.rsplit(' ').next().unwrap().parse().unwrap();
    assert_eq!(inf, hcount, "+Inf bucket must equal _count; body: {body}");

    set.shutdown_and_join().unwrap();
}

/// `/livez` is 200 once the node is up; `/readyz` becomes 200 once load-on-boot is done for EVERY
/// shard (standalone, no raft gate); an unknown path is 404.
///
/// Readiness is now SIGNAL-DRIVEN (#152): each shard decrements the readiness countdown after its
/// own load-on-boot completes, so `/readyz` flips to 200 only after BOTH shards have signalled
/// rather than being set synchronously at boot. With persistence off each shard signals essentially
/// immediately, so we poll `/readyz` briefly for the 200 (the same way a k8s readiness probe does).
#[tokio::test]
async fn livez_readyz_and_unknown_path() {
    let resp_port = free_port();
    let metrics_port = free_port();
    let set = run_server_with_metrics_for_test(resp_port, 2, metrics_port);

    let (live_code, _) = http_get(metrics_port, "/livez").await;
    assert_eq!(live_code, 200);

    // Poll for readiness: the per-shard signal drains the countdown to 0 shortly after boot.
    let mut ready_code = 0;
    let mut ready_body = String::new();
    for _ in 0..100 {
        let (code, body) = http_get(metrics_port, "/readyz").await;
        ready_code = code;
        ready_body = body;
        if ready_code == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        ready_code, 200,
        "readyz never became ready; body: {ready_body}"
    );

    let (nf_code, _) = http_get(metrics_port, "/does-not-exist").await;
    assert_eq!(nf_code, 404);

    set.shutdown_and_join().unwrap();
}

/// A malformed / oversized request is bounded: the responder rejects an oversized request line
/// with 413 and never hangs (the connection completes). This drives a request whose header
/// exceeds the responder's cap.
#[tokio::test]
async fn oversized_request_is_bounded_not_hung() {
    let resp_port = free_port();
    let metrics_port = free_port();
    let set = run_server_with_metrics_for_test(resp_port, 1, metrics_port);

    let mut stream = None;
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", metrics_port)).await {
            stream = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut stream = stream.expect("metrics endpoint never came up");
    // A request line with NO newline, larger than MAX_REQUEST_BYTES (8 KiB): the responder must
    // break out with a 413 rather than buffering unboundedly or hanging.
    let mut big = b"GET /".to_vec();
    big.extend(std::iter::repeat_n(b'a', 16 * 1024));
    stream.write_all(&big).await.unwrap();
    // Read the response with a timeout so a hang fails the test rather than blocking forever.
    let mut raw = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut raw)).await;
    assert!(
        read.is_ok(),
        "metrics responder hung on an oversized request"
    );
    let text = String::from_utf8_lossy(&raw);
    assert!(
        text.starts_with("HTTP/1.1 413"),
        "expected 413, got: {text}"
    );

    set.shutdown_and_join().unwrap();
}

/// DEFAULT (no `--metrics-addr`): the plain `run_server` boots NO HTTP listener. We prove it by
/// booting a normal server and confirming a TCP connect to a fresh ephemeral metrics port is
/// REFUSED (nothing is listening there), while the RESP port serves normally.
#[tokio::test]
async fn default_boot_starts_no_metrics_listener() {
    let resp_port = free_port();
    let would_be_metrics_port = free_port();
    let set = run_server_for_test(resp_port, 2);

    // The RESP listener is up.
    let mut c = connect_retry(resp_port).await;
    ping(&mut c).await;
    drop(c);

    // Nothing is listening on the metrics port: a connect is refused (give it a moment to be sure
    // no async bind sneaks in; the default path never spawns the metrics thread).
    tokio::time::sleep(Duration::from_millis(100)).await;
    let refused = TcpStream::connect(("127.0.0.1", would_be_metrics_port)).await;
    assert!(
        refused.is_err(),
        "default boot must not bind a metrics listener"
    );

    set.shutdown_and_join().unwrap();
}
