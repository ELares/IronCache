// SPDX-License-Identifier: MIT OR Apache-2.0
//! Integration test (issue #353): boot the real accept loop and exercise the
//! console's HTTP surface over a TCP socket end to end.

use std::net::SocketAddr;
use std::sync::Arc;

use ironcache_console::http::{ConsoleHttpState, accept_loop};
use ironcache_console::metrics::ConsoleMetrics;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Open a fresh connection, send a `GET path`, and read the full response (the
/// server closes the connection after one request, so `read_to_end` terminates).
async fn http_get(addr: SocketAddr, path: &str) -> String {
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
    c.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    c.read_to_end(&mut raw).await.unwrap();
    String::from_utf8_lossy(&raw).into_owned()
}

#[tokio::test]
async fn serves_livez_readyz_metrics_and_404_over_tcp() {
    let state = ConsoleHttpState::new(Arc::new(ConsoleMetrics::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    state.set_live(true);
    state.set_ready(true);
    let serving = state.clone();
    tokio::spawn(async move {
        accept_loop(listener, serving).await;
    });

    let livez = http_get(addr, "/livez").await;
    assert!(livez.starts_with("HTTP/1.1 200 OK"), "{livez}");

    let readyz = http_get(addr, "/readyz").await;
    assert!(readyz.starts_with("HTTP/1.1 200 OK"), "{readyz}");

    let metrics = http_get(addr, "/metrics").await;
    assert!(metrics.starts_with("HTTP/1.1 200 OK"), "{metrics}");
    assert!(
        metrics.contains("ironcache_console_build_info"),
        "{metrics}"
    );

    let missing = http_get(addr, "/nope").await;
    assert!(missing.starts_with("HTTP/1.1 404 Not Found"), "{missing}");
}

#[tokio::test]
async fn livez_503_before_live_flag_is_set() {
    let state = ConsoleHttpState::new(Arc::new(ConsoleMetrics::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = state.clone();
    tokio::spawn(async move {
        accept_loop(listener, serving).await;
    });
    // live flag not set: liveness reports starting.
    let livez = http_get(addr, "/livez").await;
    assert!(livez.starts_with("HTTP/1.1 503"), "{livez}");
}
