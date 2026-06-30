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

/// Send an arbitrary METHOD + body with an optional Bearer token and read the full
/// response. Used by the management (#361) write tests.
async fn http_send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> String {
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth = token.map_or_else(String::new, |t| format!("Authorization: Bearer {t}\r\n"));
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    c.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    c.read_to_end(&mut raw).await.unwrap();
    String::from_utf8_lossy(&raw).into_owned()
}

/// Spawn a stub RESP node that answers a scripted reply per command it reads, then
/// idles. Returns its `host:port`. Used to drive the management dispatch end to end
/// without a real IronCache.
async fn spawn_stub_node(replies: Vec<&'static [u8]>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut sock, _peer) = listener.accept().await.unwrap();
        let mut chunk = [0u8; 4096];
        for reply in replies {
            let _ = sock.read(&mut chunk).await;
            let _ = sock.write_all(reply).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    });
    addr
}

/// Build a console HTTP state whose management connections target `node_addr`, with
/// an explicit auth policy.
fn state_with_node(auth: ironcache_console::auth::AuthPolicy, node_addr: &str) -> ConsoleHttpState {
    let access = ironcache_console::node::NodeAccess {
        addr: node_addr.to_owned(),
        tls: None,
        auth: None,
        connect_timeout: std::time::Duration::from_secs(2),
        op_timeout: std::time::Duration::from_secs(2),
    };
    let s = ConsoleHttpState::with_topology_and_auth(
        Arc::new(ConsoleMetrics::new()),
        ironcache_console::poll::new_topology_holder(),
        auth,
    )
    .with_node_access(Some(Arc::new(access)));
    s.set_live(true);
    s
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
        cluster: None,
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

/// SECURITY (#369): the `/api/*` JSON responses carry `X-Content-Type-Options:
/// nosniff` and `Cache-Control: no-store` over a real socket, while the probe and
/// metrics responses do NOT carry `Cache-Control` (the headers are scoped to the
/// API surface only). Exercises the SAME bounded responder end to end.
#[tokio::test]
async fn api_responses_carry_security_headers_over_tcp() {
    let state = ConsoleHttpState::new(Arc::new(ConsoleMetrics::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    state.set_live(true);
    let serving = state.clone();
    tokio::spawn(async move {
        accept_loop(listener, serving).await;
    });

    // An /api/* response (health, no poll needed) carries nosniff + no-store.
    let health = http_get(addr, "/api/health").await;
    assert!(health.starts_with("HTTP/1.1 200 OK"), "{health}");
    assert!(
        health.contains("Content-Type: application/json"),
        "{health}"
    );
    assert!(
        health.contains("X-Content-Type-Options: nosniff"),
        "{health}"
    );
    assert!(health.contains("Cache-Control: no-store"), "{health}");

    // The probes/metrics do NOT carry Cache-Control (scoped to /api/* only).
    for path in ["/livez", "/readyz", "/metrics"] {
        let resp = http_get(addr, path).await;
        assert!(
            !resp.contains("Cache-Control"),
            "{path} must not carry Cache-Control: {resp}"
        );
    }
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

    // GET /app.css -> the dashboard stylesheet, which imports the self-hosted
    // fonts (no CDN) for the strict CSP.
    let css = http_get(addr, "/app.css").await;
    assert!(css.starts_with("HTTP/1.1 200 OK"), "{css}");
    assert!(
        css.contains("Content-Type: text/css; charset=utf-8"),
        "{css}"
    );
    let (_h, cssbody) = split_body(&css);
    assert!(
        cssbody.contains("@import url('/assets/fonts.css')"),
        "app.css must import the self-hosted fonts"
    );
}

/// The self-hosted fonts (#359 re-skin) over a real TCP socket: `/assets/fonts.css`
/// serves as `text/css` with the @font-face declarations, and each woff2 serves
/// as `font/woff2` raw bytes with the strict UI security headers. Exercises the
/// SAME bounded responder.
#[tokio::test]
async fn serves_self_hosted_fonts_over_tcp() {
    let state = ConsoleHttpState::new(Arc::new(ConsoleMetrics::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    state.set_live(true);
    let serving = state.clone();
    tokio::spawn(async move {
        accept_loop(listener, serving).await;
    });

    // The @font-face stylesheet.
    let fonts_css = http_get(addr, "/assets/fonts.css").await;
    assert!(fonts_css.starts_with("HTTP/1.1 200 OK"), "{fonts_css}");
    assert!(
        fonts_css.contains("Content-Type: text/css; charset=utf-8"),
        "{fonts_css}"
    );
    assert!(
        fonts_css.contains("Content-Security-Policy: default-src 'self'"),
        "{fonts_css}"
    );
    let (_h, body) = split_body(&fonts_css);
    assert!(body.contains("@font-face"), "{body}");

    // Each woff2 serves as font/woff2 with the security headers and a 200.
    for path in [
        "/assets/fonts/hanken-grotesk.woff2",
        "/assets/fonts/jetbrains-mono.woff2",
    ] {
        let resp = http_get(addr, path).await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{path}: {resp}");
        assert!(resp.contains("Content-Type: font/woff2"), "{path}: {resp}");
        assert!(
            resp.contains("X-Content-Type-Options: nosniff"),
            "{path}: {resp}"
        );
    }
}

/// The node-level MANAGEMENT layer (#361) end to end over a real TCP socket: the
/// admin-gated write surface reaches a stub RESP node, the tier gate blocks a
/// no-token mutation BEFORE the node, the SCAN browser returns the right shape, and
/// a DELETE requires admin. Exercises the SAME bounded responder + the body read
/// path.
#[tokio::test]
async fn management_surface_over_tcp() {
    use ironcache_console::auth::AuthPolicy;

    // POST /api/config (CONFIG SET) with the admin token -> {ok}. The stub answers
    // +OK to the one CONFIG SET command.
    let node = spawn_stub_node(vec![b"+OK\r\n"]).await;
    let state = state_with_node(AuthPolicy::resolve(None, Some("admin-tok"), true), &node);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = state.clone();
    tokio::spawn(async move {
        accept_loop(listener, serving).await;
    });
    let ok = http_send(
        addr,
        "POST",
        "/api/config",
        Some("admin-tok"),
        "{\"param\":\"maxmemory\",\"value\":\"128mb\"}",
    )
    .await;
    assert!(ok.starts_with("HTTP/1.1 200 OK"), "{ok}");
    assert!(ok.contains("X-Content-Type-Options: nosniff"), "{ok}");
    let (_h, body) = split_body(&ok);
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["ok"], true);

    // A mutation with NO token is blocked at the gate (401) and never reaches the
    // node (a fresh state pointed at a dead addr so a 502 would prove a gate leak).
    let dead = state_with_node(
        AuthPolicy::resolve(Some("read-tok"), Some("admin-tok"), true),
        "127.0.0.1:1",
    );
    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();
    let serving2 = dead.clone();
    tokio::spawn(async move {
        accept_loop(listener2, serving2).await;
    });
    let unauth = http_send(addr2, "POST", "/api/config", None, "{}").await;
    assert!(
        unauth.starts_with("HTTP/1.1 401"),
        "no token must be 401: {unauth}"
    );
    // A read token on a write is 403 (insufficient tier).
    let forbidden = http_send(addr2, "POST", "/api/config", Some("read-tok"), "{}").await;
    assert!(
        forbidden.starts_with("HTTP/1.1 403"),
        "read token on a write must be 403: {forbidden}"
    );
    // A DELETE on a key with the read token is 403 too.
    let del_denied = http_send(addr2, "DELETE", "/api/keys/foo", Some("read-tok"), "").await;
    assert!(del_denied.starts_with("HTTP/1.1 403"), "{del_denied}");

    // GET /api/keys (SCAN) returns the {cursor, keys} shape; the stub answers SCAN
    // then TYPE + TTL for the one key.
    let scan_node = spawn_stub_node(vec![
        b"*2\r\n$1\r\n0\r\n*1\r\n$6\r\nuser:1\r\n",
        b"+string\r\n",
        b":-1\r\n",
    ])
    .await;
    let scan_state = state_with_node(
        AuthPolicy::resolve(Some("read-tok"), None, true),
        &scan_node,
    );
    let listener3 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr3 = listener3.local_addr().unwrap();
    let serving3 = scan_state.clone();
    tokio::spawn(async move {
        accept_loop(listener3, serving3).await;
    });
    let scan = http_send(addr3, "GET", "/api/keys?pattern=*", Some("read-tok"), "").await;
    assert!(scan.starts_with("HTTP/1.1 200 OK"), "{scan}");
    let (_h, body) = split_body(&scan);
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["cursor"], "0");
    assert_eq!(v["keys"][0]["key"], "user:1");
    assert_eq!(v["keys"][0]["type"], "string");
}
