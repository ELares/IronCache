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

/// Split a raw HTTP response into the header block and the body.
fn split_body(resp: &str) -> (&str, &str) {
    resp.split_once("\r\n\r\n").expect("response has a body")
}

/// The REST API over a real TCP socket: `/api/cluster` and `/api/nodes` are
/// `503` JSON before any poll, then return valid JSON of the right shape once a
/// topology is published into the shared holder. Exercises the SAME bounded
/// responder the probes use.
#[tokio::test]
async fn api_cluster_and_nodes_over_tcp() {
    use ironcache_console::info::NodeInfo;
    use ironcache_console::snapshot::{NodeSnapshot, Topology, TopologyMode};

    let state = ConsoleHttpState::new(Arc::new(ConsoleMetrics::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    state.set_live(true);
    let serving = state.clone();
    tokio::spawn(async move {
        accept_loop(listener, serving).await;
    });

    // Before any poll: 503 JSON with an error field.
    let before = http_get(addr, "/api/cluster").await;
    assert!(before.starts_with("HTTP/1.1 503"), "{before}");
    assert!(before.contains("application/json"), "{before}");
    let (_h, body) = split_body(&before);
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert!(v["error"].is_string(), "{body}");

    // Publish a single-node topology into the shared holder.
    let info = NodeInfo {
        redis_version: Some("7.2.4".to_owned()),
        connected_clients: Some(2),
        used_memory: Some(4096),
        total_keys: Some(11),
        ..Default::default()
    };
    let topo = Topology {
        mode: TopologyMode::Standalone,
        nodes: vec![NodeSnapshot {
            addr: "10.0.0.1:6379".to_owned(),
            reachable: true,
            error: None,
            info: Some(info),
            slowlog: Vec::new(),
            slowlog_error: None,
            clients: Vec::new(),
            clients_error: None,
            fetched_unixtime: 100,
        }],
        fetched_unixtime: 100,
    };
    *state.topology().write().await = Some(topo);

    // /api/cluster: 200 JSON overview.
    let cluster = http_get(addr, "/api/cluster").await;
    assert!(cluster.starts_with("HTTP/1.1 200 OK"), "{cluster}");
    let (_h, body) = split_body(&cluster);
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["mode"], "standalone");
    assert_eq!(v["nodes_total"], 1);
    assert_eq!(v["nodes_reachable"], 1);
    assert_eq!(v["totals"]["keys"], 11);

    // /api/nodes: 200 JSON array of summaries.
    let nodes = http_get(addr, "/api/nodes").await;
    assert!(nodes.starts_with("HTTP/1.1 200 OK"), "{nodes}");
    let (_h, body) = split_body(&nodes);
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert!(v.is_array(), "{body}");
    assert_eq!(v[0]["addr"], "10.0.0.1:6379");
    assert_eq!(v[0]["keys"], 11);
    assert_eq!(v[0]["version"], "7.2.4");

    // /api/nodes/{addr}: 200 for the known node, 404 for an unknown one.
    let one = http_get(addr, "/api/nodes/10.0.0.1:6379").await;
    assert!(one.starts_with("HTTP/1.1 200 OK"), "{one}");
    let (_h, body) = split_body(&one);
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["addr"], "10.0.0.1:6379");

    let missing = http_get(addr, "/api/nodes/9.9.9.9:1").await;
    assert!(missing.starts_with("HTTP/1.1 404 Not Found"), "{missing}");

    // /api/openapi.json: 200 valid JSON.
    let openapi = http_get(addr, "/api/openapi.json").await;
    assert!(openapi.starts_with("HTTP/1.1 200 OK"), "{openapi}");
    let (_h, body) = split_body(&openapi);
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["openapi"], "3.0.3");
}

/// A stub history source for the integration test: returns a canned series for any
/// allowed metric, so the `/api/timeseries` 200 path can be exercised end to end
/// over a real socket without a Prometheus.
struct StubHistory;

impl ironcache_console::history::HistorySource for StubHistory {
    fn query_range<'a>(
        &'a self,
        _metric: &'a str,
        _start_unix: u64,
        _end_unix: u64,
        _step_secs: u64,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        Vec<ironcache_console::history::TimeSeries>,
                        ironcache_console::history::HistoryError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut labels = std::collections::BTreeMap::new();
            labels.insert(
                "__name__".to_owned(),
                "ironcache_used_memory_bytes".to_owned(),
            );
            Ok(vec![ironcache_console::history::TimeSeries {
                labels,
                points: vec![(1000, 1.5), (1015, 2.5)],
            }])
        })
    }
}

/// `/api/timeseries` over a real TCP socket: 503 when no source is configured;
/// then 200 with the series shape, and 400 on a disallowed (injection) metric,
/// when a stub source is wired. Exercises the SAME bounded responder.
#[tokio::test]
async fn api_timeseries_over_tcp() {
    // 1. No history source configured -> 503 JSON.
    let state = ConsoleHttpState::new(Arc::new(ConsoleMetrics::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    state.set_live(true);
    let serving = state.clone();
    tokio::spawn(async move {
        accept_loop(listener, serving).await;
    });
    let unconfigured = http_get(addr, "/api/timeseries?metric=ironcache_used_memory_bytes").await;
    assert!(unconfigured.starts_with("HTTP/1.1 503"), "{unconfigured}");
    let (_h, body) = split_body(&unconfigured);
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert!(
        v["error"].as_str().unwrap().contains("no history source"),
        "{body}"
    );

    // 2. A wired stub source -> 200 with the series, and 400 on injection.
    let wired = ConsoleHttpState::new(Arc::new(ConsoleMetrics::new()))
        .with_history(Some(Arc::new(StubHistory)));
    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();
    wired.set_live(true);
    let serving2 = wired.clone();
    tokio::spawn(async move {
        accept_loop(listener2, serving2).await;
    });

    let ok = http_get(
        addr2,
        "/api/timeseries?metric=ironcache_used_memory_bytes&range=600&step=30",
    )
    .await;
    assert!(ok.starts_with("HTTP/1.1 200 OK"), "{ok}");
    assert!(ok.contains("application/json"), "{ok}");
    let (_h, body) = split_body(&ok);
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["metric"], "ironcache_used_memory_bytes");
    assert_eq!(v["step_secs"], 30);
    assert_eq!(v["series"][0]["points"][0][0], 1000);
    assert_eq!(v["series"][0]["points"][0][1], 1.5);

    // SSRF / injection guard: a PromQL-laced metric is rejected with 400 and never
    // reaches the source.
    let bad = http_get(addr2, "/api/timeseries?metric=rate(up%5B5m%5D)").await;
    assert!(bad.starts_with("HTTP/1.1 400"), "{bad}");
}

/// The dashboard SPA (#359) over a real TCP socket: `GET /` returns the HTML
/// shell with the strict security headers and references the SEPARATE app.css /
/// app.js, and `GET /app.js` returns the JavaScript. The UI needs no topology,
/// so it serves even before the first poll. Exercises the SAME bounded responder
/// the probes use.
#[tokio::test]
async fn serves_dashboard_assets_over_tcp() {
    let state = ConsoleHttpState::new(Arc::new(ConsoleMetrics::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    state.set_live(true);
    let serving = state.clone();
    tokio::spawn(async move {
        accept_loop(listener, serving).await;
    });

    // GET / -> the HTML shell with the security headers and the asset links.
    let index = http_get(addr, "/").await;
    assert!(index.starts_with("HTTP/1.1 200 OK"), "{index}");
    assert!(
        index.contains("Content-Type: text/html; charset=utf-8"),
        "{index}"
    );
    assert!(
        index.contains("Content-Security-Policy: default-src 'self'"),
        "missing CSP: {index}"
    );
    assert!(index.contains("X-Content-Type-Options: nosniff"), "{index}");
    assert!(index.contains("X-Frame-Options: DENY"), "{index}");
    assert!(index.contains("Referrer-Policy: no-referrer"), "{index}");
    let (_h, body) = split_body(&index);
    assert!(body.contains("IronCache Console"), "{body}");
    // The HTML references the separate CSS/JS (so the CSP needs no inline).
    assert!(body.contains("/app.css"), "{body}");
    assert!(body.contains("/app.js"), "{body}");

    // GET /app.js -> the dashboard JavaScript.
    let js = http_get(addr, "/app.js").await;
    assert!(js.starts_with("HTTP/1.1 200 OK"), "{js}");
    assert!(
        js.contains("Content-Type: application/javascript; charset=utf-8"),
        "{js}"
    );
    let (_h, jsbody) = split_body(&js);
    // It fetches the /api/* surface and avoids the innerHTML XSS sink.
    assert!(jsbody.contains("/api/cluster"), "{jsbody}");
    assert!(
        !jsbody.contains(".innerHTML"),
        "app.js must avoid innerHTML"
    );

    // GET /app.css -> the dashboard stylesheet.
    let css = http_get(addr, "/app.css").await;
    assert!(css.starts_with("HTTP/1.1 200 OK"), "{css}");
    assert!(
        css.contains("Content-Type: text/css; charset=utf-8"),
        "{css}"
    );
}
