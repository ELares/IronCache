// SPDX-License-Identifier: MIT OR Apache-2.0
//! The console's bounded, hand-rolled tokio HTTP/1.1 responder (issue #353).
//!
//! In PR-1 it serves three fixed routes:
//!   * `GET /metrics` -> the console's OWN Prometheus self-metrics,
//!   * `GET /livez`   -> `200` once the process is up (a liveness probe), and
//!   * `GET /readyz`  -> `200` when the console is ready to serve (a readiness probe).
//!
//! Later PRs hang the `/api/*` surface (#358) and the SPA (#359) off this same
//! server. It is hand-rolled (no hyper/axum) for the same reason the engine's
//! metrics endpoint is: a tiny fixed-route surface keeps the static musl build
//! pure-Rust and adds no new dependency. It bounds each request (a whole-request
//! deadline, a small header cap, a connection-concurrency cap) and is NOT a
//! general HTTP server: anything malformed/oversized gets a fixed error + close.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::metrics::ConsoleMetrics;
use crate::poll::{TopologyHolder, new_topology_holder};
use crate::snapshot::TopologyMode;

/// Max request bytes before a `413` (probes send only a request line + a few
/// headers, never a body); bounds the per-connection buffer.
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// The WHOLE-REQUEST deadline: the entire request-read phase must complete in
/// this window, so a slow-drip (slowloris) client cannot hold the socket.
const REQUEST_DEADLINE: Duration = Duration::from_secs(10);

/// Max connections served concurrently; the accept loop drops the excess rather
/// than queueing unbounded tasks.
const MAX_CONCURRENT_CONNS: usize = 128;

/// The shared state the HTTP handler reads at request time. Cheap, lock-free
/// reads; cloned (`Arc` inside) into each connection task.
#[derive(Clone)]
pub struct ConsoleHttpState {
    metrics: Arc<ConsoleMetrics>,
    /// Liveness: set `true` at the end of boot; never flips back.
    live: Arc<AtomicBool>,
    /// Readiness: set `true` on the FIRST successful node poll (#355). The poll
    /// loop owns this flip, so `/readyz` is 503 until the console has real data.
    ready: Arc<AtomicBool>,
    /// The latest polled topology, shared with the poll loop (#355/#366). The
    /// REST API (#358) reads it; this PR also exposes it at `/debug/topology`.
    topology: TopologyHolder,
    /// Whether the unauthenticated `/debug/topology` recon route is served.
    /// Default FALSE: it exposes node addresses / version / key counts with no
    /// auth, so it stays off until it moves behind the privileged/auth tier.
    // SECURITY: `/debug/topology` is unauthenticated recon (node addresses,
    // version, key counts). It MUST move behind the privileged/auth tier
    // (#360/#369) before the console is exposed; until then it is gated OFF by
    // default and only served when this flag is explicitly enabled.
    enable_debug_routes: bool,
}

impl ConsoleHttpState {
    #[must_use]
    pub fn new(metrics: Arc<ConsoleMetrics>) -> Self {
        Self::with_topology(metrics, new_topology_holder())
    }

    /// Construct with an EXISTING topology holder, so the poll loop and the HTTP
    /// surface share one cell (the loop writes, the handler reads). The debug
    /// route is OFF (the safe default); use [`Self::with_options`] to enable it.
    #[must_use]
    pub fn with_topology(metrics: Arc<ConsoleMetrics>, topology: TopologyHolder) -> Self {
        Self::with_options(metrics, topology, false)
    }

    /// Construct with an existing topology holder and the explicit
    /// `enable_debug_routes` gate. The gate controls whether the unauthenticated
    /// `/debug/topology` recon route is served (default OFF in the other ctors).
    #[must_use]
    pub fn with_options(
        metrics: Arc<ConsoleMetrics>,
        topology: TopologyHolder,
        enable_debug_routes: bool,
    ) -> Self {
        ConsoleHttpState {
            metrics,
            live: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(false)),
            topology,
            enable_debug_routes,
        }
    }

    /// The shared topology holder (so `lib.rs` can hand the same cell to the poll
    /// loop it handed the HTTP state).
    #[must_use]
    pub fn topology(&self) -> TopologyHolder {
        self.topology.clone()
    }

    /// Flip liveness (called once at end of boot).
    pub fn set_live(&self, v: bool) {
        self.live.store(v, Ordering::SeqCst);
    }

    /// Flip readiness.
    pub fn set_ready(&self, v: bool) {
        self.ready.store(v, Ordering::SeqCst);
    }

    /// Render the response bytes for a parsed `(method, path)`. Reads the live /
    /// ready state and the latest topology and returns the bytes; the connection
    /// handler writes them. Async because `/debug/topology` reads the shared
    /// topology behind an async `RwLock`. Exposed for tests.
    pub async fn respond_async(&self, method: &str, path: &str) -> Vec<u8> {
        let head = method == "HEAD";
        let bare = path.split('?').next().unwrap_or(path);
        // SECURITY: only serve the unauthenticated `/debug/topology` recon route
        // when explicitly enabled; otherwise fall through to the normal 404 so its
        // existence is not even disclosed. It must move behind the privileged/auth
        // tier (#360/#369) before the console is exposed.
        if self.enable_debug_routes && (method == "GET" || head) && bare == "/debug/topology" {
            let body = self.render_topology_json().await;
            return http_response(
                200,
                "OK",
                "application/json; charset=utf-8",
                body.as_bytes(),
                head,
            );
        }
        self.respond(method, path)
    }

    /// Render the response bytes for the FIXED routes (`/metrics`, `/livez`,
    /// `/readyz`, and the 404/405 fallbacks). Pure: reads only the atomic flags.
    /// Exposed for tests; `/debug/topology` goes through [`Self::respond_async`].
    #[must_use]
    pub fn respond(&self, method: &str, path: &str) -> Vec<u8> {
        let head = method == "HEAD";
        if method != "GET" && !head {
            return http_response(
                405,
                "Method Not Allowed",
                "text/plain; charset=utf-8",
                b"",
                head,
            );
        }
        let path = path.split('?').next().unwrap_or(path);
        match path {
            "/metrics" => http_response(
                200,
                "OK",
                "text/plain; version=0.0.4; charset=utf-8",
                self.metrics.render().as_bytes(),
                head,
            ),
            "/livez" => {
                if self.live.load(Ordering::SeqCst) {
                    http_response(200, "OK", "text/plain; charset=utf-8", b"OK\n", head)
                } else {
                    http_response(
                        503,
                        "Service Unavailable",
                        "text/plain; charset=utf-8",
                        b"starting\n",
                        head,
                    )
                }
            }
            "/readyz" => {
                if self.ready.load(Ordering::SeqCst) {
                    http_response(200, "OK", "text/plain; charset=utf-8", b"OK\n", head)
                } else {
                    http_response(
                        503,
                        "Service Unavailable",
                        "text/plain; charset=utf-8",
                        b"not ready\n",
                        head,
                    )
                }
            }
            _ => http_response(
                404,
                "Not Found",
                "text/plain; charset=utf-8",
                b"not found\n",
                head,
            ),
        }
    }

    /// Render the latest topology as a small JSON object for `/debug/topology`.
    /// `{"polled":false}` before the first poll; otherwise the mode, fetch time,
    /// and a per-node summary. Hand-rolled (no serde_json dep) to keep the HTTP
    /// surface dependency-light; the full REST API with proper serialization is
    /// #358. NEVER includes a secret (it only carries INFO-derived numbers).
    async fn render_topology_json(&self) -> String {
        let guard = self.topology.read().await;
        let Some(topo) = guard.as_ref() else {
            return "{\"polled\":false}".to_owned();
        };
        let mut out = String::with_capacity(256);
        out.push_str("{\"polled\":true,\"mode\":\"");
        out.push_str(topology_mode_str(topo.mode));
        out.push_str("\",\"fetched_unixtime\":");
        out.push_str(&topo.fetched_unixtime.to_string());
        out.push_str(",\"nodes\":[");
        for (i, node) in topo.nodes.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str("{\"addr\":\"");
            out.push_str(&json_escape(&node.addr));
            out.push_str("\",\"reachable\":");
            out.push_str(if node.reachable { "true" } else { "false" });
            if let Some(err) = &node.error {
                out.push_str(",\"error\":\"");
                out.push_str(&json_escape(err));
                out.push('"');
            }
            if let Some(info) = &node.info {
                if let Some(v) = &info.redis_version {
                    out.push_str(",\"redis_version\":\"");
                    out.push_str(&json_escape(v));
                    out.push('"');
                }
                if let Some(keys) = info.total_keys {
                    out.push_str(",\"total_keys\":");
                    out.push_str(&keys.to_string());
                }
            }
            out.push('}');
        }
        out.push_str("]}");
        out
    }
}

/// The lowercase string form of a topology mode for JSON.
fn topology_mode_str(mode: TopologyMode) -> &'static str {
    match mode {
        TopologyMode::Standalone => "standalone",
        TopologyMode::Clustered => "clustered",
    }
}

/// Escape a string for embedding in a JSON double-quoted value (the minimal set:
/// backslash, quote, and control chars). Node addresses and error strings are the
/// only inputs, so this small escaper suffices for the debug route.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

/// Build a complete HTTP/1.1 response (status line, content headers,
/// `Connection: close`, body). One request per connection. When `head` is true
/// the `Content-Length` reflects what a GET would return but NO body bytes are
/// written (RFC 9110: a HEAD response must not carry a message body).
fn http_response(code: u16, reason: &str, content_type: &str, body: &[u8], head: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 128);
    let header = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    out.extend_from_slice(header.as_bytes());
    if !head {
        out.extend_from_slice(body);
    }
    out
}

/// Parse the request LINE (`METHOD SP PATH SP HTTP/x.y`). Returns `Some` once a
/// line terminator is present, `None` if incomplete (read more). A line with too
/// few tokens yields an empty method (answered `405`) or an empty path (answered
/// `404`); it never panics.
fn parse_request_line(buf: &[u8]) -> Option<(String, String)> {
    let line_end = buf.iter().position(|&b| b == b'\n')?;
    let line = &buf[..line_end];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let text = String::from_utf8_lossy(line);
    let mut parts = text.split(' ');
    let method = parts.next().unwrap_or("").to_owned();
    let path = parts.next().unwrap_or("").to_owned();
    Some((method, path))
}

/// Serve ONE connection with the production whole-request deadline.
async fn serve_conn(stream: tokio::net::TcpStream, state: ConsoleHttpState) {
    serve_conn_with_deadline(stream, state, REQUEST_DEADLINE).await;
}

/// [`serve_conn`] with an explicit deadline so a test can drive the slowloris
/// drop path on a short deadline. The whole read phase is under ONE timeout.
async fn serve_conn_with_deadline(
    mut stream: tokio::net::TcpStream,
    state: ConsoleHttpState,
    deadline: Duration,
) {
    let read_phase = tokio::time::timeout(deadline, async {
        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        let mut chunk = [0u8; 1024];
        loop {
            let read = match stream.read(&mut chunk).await {
                Ok(n) if n > 0 => n,
                Ok(_) | Err(_) => return None,
            };
            buf.extend_from_slice(&chunk[..read]);
            if buf.len() > MAX_REQUEST_BYTES {
                return Some(http_response(
                    413,
                    "Payload Too Large",
                    "text/plain; charset=utf-8",
                    b"request too large\n",
                    false,
                ));
            }
            if let Some((method, path)) = parse_request_line(&buf) {
                return Some(state.respond_async(&method, &path).await);
            }
        }
    })
    .await;
    let Ok(Some(response)) = read_phase else {
        return;
    };
    let _ = stream.write_all(&response).await;
    let _ = stream.flush().await;
}

/// The accept loop: accept connections and spawn a bounded [`serve_conn`] per
/// connection. Returns only on an unrecoverable listener error (a transient
/// accept error backs off and continues). At most [`MAX_CONCURRENT_CONNS`] are
/// served at once; the excess is dropped immediately.
pub async fn accept_loop(listener: tokio::net::TcpListener, state: ConsoleHttpState) {
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNS));
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    serve_conn(stream, state).await;
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "console http: accept error; backing off");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> ConsoleHttpState {
        ConsoleHttpState::new(Arc::new(ConsoleMetrics::new()))
    }

    /// A state with the debug route ENABLED (for the `/debug/topology` tests).
    fn debug_state() -> ConsoleHttpState {
        ConsoleHttpState::with_options(Arc::new(ConsoleMetrics::new()), new_topology_holder(), true)
    }

    #[test]
    fn metrics_route_returns_console_prometheus_text() {
        let state = test_state();
        let resp = String::from_utf8(state.respond("GET", "/metrics")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"), "{resp}");
        assert!(
            resp.contains("Content-Type: text/plain; version=0.0.4"),
            "{resp}"
        );
        assert!(resp.contains("ironcache_console_build_info"), "{resp}");
        assert!(resp.contains("ironcache_console_uptime_seconds"), "{resp}");
    }

    #[test]
    fn livez_flips_with_the_live_flag() {
        let state = test_state();
        let before = String::from_utf8(state.respond("GET", "/livez")).unwrap();
        assert!(before.starts_with("HTTP/1.1 503"), "{before}");
        state.set_live(true);
        let after = String::from_utf8(state.respond("GET", "/livez")).unwrap();
        assert!(after.starts_with("HTTP/1.1 200 OK"), "{after}");
    }

    #[test]
    fn head_request_has_content_length_but_no_body() {
        let state = test_state();
        let text = String::from_utf8(state.respond("HEAD", "/metrics")).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK"), "{text}");
        let (header, body) = text.split_once("\r\n\r\n").unwrap();
        // Content-Length reflects what a GET would return (non-zero)...
        let cl: usize = header
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            cl > 0,
            "HEAD Content-Length should match the GET body length"
        );
        // ...but no body bytes follow (RFC 9110).
        assert!(body.is_empty(), "HEAD must not return a body, got {body:?}");
        // GET on the same route DOES return the body, of that exact length.
        let get = String::from_utf8(state.respond("GET", "/metrics")).unwrap();
        let (_gh, gbody) = get.split_once("\r\n\r\n").unwrap();
        assert_eq!(gbody.len(), cl);
    }

    #[test]
    fn readyz_flips_with_the_ready_flag() {
        let state = test_state();
        let before = String::from_utf8(state.respond("GET", "/readyz")).unwrap();
        assert!(before.starts_with("HTTP/1.1 503"), "{before}");
        state.set_ready(true);
        let after = String::from_utf8(state.respond("GET", "/readyz")).unwrap();
        assert!(after.starts_with("HTTP/1.1 200 OK"), "{after}");
    }

    #[test]
    fn unknown_path_is_404() {
        let resp = String::from_utf8(test_state().respond("GET", "/nope")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 404 Not Found"), "{resp}");
    }

    #[tokio::test]
    async fn debug_topology_is_404_when_disabled() {
        // Default-OFF: the recon route is not served and not even disclosed (404,
        // identical to any unknown path).
        let state = test_state();
        let resp = String::from_utf8(state.respond_async("GET", "/debug/topology").await).unwrap();
        assert!(resp.starts_with("HTTP/1.1 404 Not Found"), "{resp}");
    }

    #[tokio::test]
    async fn debug_topology_reports_unpolled_then_polled() {
        use crate::snapshot::{NodeSnapshot, Topology, TopologyMode};
        let state = debug_state();
        // Before any poll: polled=false.
        let before =
            String::from_utf8(state.respond_async("GET", "/debug/topology").await).unwrap();
        assert!(before.starts_with("HTTP/1.1 200 OK"), "{before}");
        assert!(before.contains("application/json"), "{before}");
        assert!(before.contains("{\"polled\":false}"), "{before}");

        // Publish a topology into the shared holder, then it appears in the JSON.
        let topo = Topology {
            mode: TopologyMode::Standalone,
            nodes: vec![NodeSnapshot {
                addr: "10.0.0.1:6379".to_owned(),
                reachable: true,
                error: None,
                info: None,
                fetched_unixtime: 42,
            }],
            fetched_unixtime: 42,
        };
        *state.topology().write().await = Some(topo);
        let after = String::from_utf8(state.respond_async("GET", "/debug/topology").await).unwrap();
        assert!(after.contains("\"polled\":true"), "{after}");
        assert!(after.contains("\"mode\":\"standalone\""), "{after}");
        assert!(after.contains("10.0.0.1:6379"), "{after}");
        assert!(after.contains("\"reachable\":true"), "{after}");
    }

    #[test]
    fn json_escape_handles_quotes_and_controls() {
        assert_eq!(json_escape("a\"b\\c"), "a\\\"b\\\\c");
        assert_eq!(json_escape("line\nbreak"), "line\\nbreak");
    }

    #[test]
    fn non_get_is_405() {
        let resp = String::from_utf8(test_state().respond("POST", "/metrics")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 405"), "{resp}");
    }

    #[test]
    fn query_string_is_stripped() {
        let state = test_state();
        state.set_live(true);
        let resp = String::from_utf8(state.respond("GET", "/livez?foo=bar")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
    }

    #[test]
    fn request_line_parse_incomplete_then_complete() {
        assert!(parse_request_line(b"GET /metrics HTTP/1.1").is_none());
        let (m, p) = parse_request_line(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        assert_eq!(m, "GET");
        assert_eq!(p, "/metrics");
        let (m, p) = parse_request_line(b"GET /livez HTTP/1.1\n").unwrap();
        assert_eq!(m, "GET");
        assert_eq!(p, "/livez");
    }

    /// A slow-drip client that sends a partial request line then stalls is
    /// dropped by the whole-request deadline, not held to the size cap.
    #[tokio::test]
    async fn slow_drip_request_is_dropped_within_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
            c.write_all(b"GET /metr").await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(c);
        });
        let (stream, _peer) = listener.accept().await.unwrap();
        let served = tokio::time::timeout(
            Duration::from_secs(5),
            serve_conn_with_deadline(stream, test_state(), Duration::from_millis(200)),
        )
        .await;
        assert!(
            served.is_ok(),
            "stalled connection must be dropped at the deadline"
        );
        client.abort();
    }

    /// A complete request within the deadline gets the normal response.
    #[tokio::test]
    async fn complete_request_within_deadline_is_served() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
            c.write_all(b"GET /livez HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut raw = Vec::new();
            c.read_to_end(&mut raw).await.unwrap();
            String::from_utf8_lossy(&raw).into_owned()
        });
        let (stream, _peer) = listener.accept().await.unwrap();
        let state = test_state();
        state.set_live(true);
        serve_conn_with_deadline(stream, state, Duration::from_secs(5)).await;
        let body = client.await.unwrap();
        assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
    }
}
