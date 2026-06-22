// SPDX-License-Identifier: MIT OR Apache-2.0
//! The console's bounded, hand-rolled tokio HTTP/1.1 responder (issue #353).
//!
//! It serves the fixed probe/metrics routes:
//!   * `GET /metrics` -> the console's OWN Prometheus self-metrics,
//!   * `GET /livez`   -> `200` once the process is up (a liveness probe), and
//!   * `GET /readyz`  -> `200` when the console is ready to serve (a readiness probe),
//!
//! plus the JSON REST API at `/api/*` (#358, handled in [`crate::api`]). The SPA
//! (#359) hangs off this same server later. It is hand-rolled (no hyper/axum) for
//! the same reason the engine's metrics endpoint is: a tiny route surface keeps
//! the static musl build pure-Rust and adds no new HTTP-server dependency. It
//! bounds each request (a whole-request deadline, a small header cap, a
//! connection-concurrency cap) and is NOT a general HTTP server: anything
//! malformed/oversized gets a fixed error + close. The `/api/*` routes go through
//! that SAME bounded responder, so the deadline/size-cap/permit still apply.
//!
//! SECURITY: the `/api/*` surface exposes node internals (node addresses, slowlog
//! argv = key names, client IPs). It is UNAUTHENTICATED today and relies on the
//! loopback default bind; it MUST move behind the auth/RBAC tier (#360) and the
//! VPN-locked exposure (#369) before the console is exposed. See [`crate::api`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::api::{self, ApiContext};
use crate::history::HistorySource;
use crate::metrics::ConsoleMetrics;
use crate::poll::{TopologyHolder, new_topology_holder};

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
    /// REST API (#358) reads it to render the `/api/*` responses.
    topology: TopologyHolder,
    /// The configured history source (#356), shared into each request. `None` when
    /// no `prometheus_url` is configured, in which case `/api/timeseries` is 503.
    /// SECURITY: this carries the SERVER-configured Prometheus base URL; the
    /// request never supplies it (the SSRF boundary).
    history: Option<Arc<dyn HistorySource>>,
}

impl ConsoleHttpState {
    #[must_use]
    pub fn new(metrics: Arc<ConsoleMetrics>) -> Self {
        Self::with_topology(metrics, new_topology_holder())
    }

    /// Construct with an EXISTING topology holder, so the poll loop and the HTTP
    /// surface share one cell (the loop writes, the handler reads through the REST
    /// API). No history source (the unconfigured case).
    #[must_use]
    pub fn with_topology(metrics: Arc<ConsoleMetrics>, topology: TopologyHolder) -> Self {
        ConsoleHttpState {
            metrics,
            live: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(false)),
            topology,
            history: None,
        }
    }

    /// Attach a history source (the Prometheus adapter, #356), consuming and
    /// returning `self` (builder style) so `lib.rs` can wire it after constructing
    /// the state with the shared topology holder.
    #[must_use]
    pub fn with_history(mut self, history: Option<Arc<dyn HistorySource>>) -> Self {
        self.history = history;
        self
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
    /// handler writes them. Async because the `/api/*` routes read the shared
    /// topology behind an async `RwLock`. Exposed for tests.
    ///
    /// The `/api/*` namespace (#358) is dispatched to [`crate::api`] here; all
    /// other paths fall through to the fixed-route [`Self::respond`]. The API goes
    /// through this SAME bounded responder, so the whole-request deadline, the
    /// size cap, and the concurrency permit still apply.
    pub async fn respond_async(&self, method: &str, path: &str) -> Vec<u8> {
        let head = method == "HEAD";
        let bare = path.split('?').next().unwrap_or(path);
        if api::is_api_path(bare) {
            // SECURITY: the `/api/*` surface is unauthenticated recon today (node
            // addresses, slowlog argv = key names, client IPs). It MUST move behind
            // the auth/RBAC tier (#360) and VPN-locked exposure (#369) before the
            // console is exposed; until then it relies on the loopback default bind.
            if method != "GET" && !head {
                return http_response(
                    405,
                    "Method Not Allowed",
                    "text/plain; charset=utf-8",
                    b"",
                    head,
                );
            }
            let ctx = ApiContext {
                version: crate::cli::BUILD_VERSION,
                live: self.live.load(Ordering::SeqCst),
                ready: self.ready.load(Ordering::SeqCst),
                uptime_seconds: self.metrics.uptime_seconds(),
                // "now" via the same env clock seam the metrics use (#356), never
                // SystemTime::now directly.
                now_unix: self.metrics.now_unix_seconds(),
            };
            // The history route does I/O (a Prometheus query) and does NOT need the
            // topology, so handle it WITHOUT holding the topology read lock: holding
            // it across a slow upstream query would block the poll loop's write for
            // up to the request deadline. Every OTHER route is pure over the
            // topology, so the guard is held only for those and is dropped promptly.
            let resp = if bare == "/api/timeseries" {
                let query = path.split_once('?').map_or("", |(_, q)| q);
                api::handle_timeseries(query, self.history.as_deref(), &ctx).await
            } else {
                let guard = self.topology.read().await;
                api::handle(bare, guard.as_ref(), &ctx)
            };
            return http_response(
                resp.status,
                status_reason(resp.status),
                api::CONTENT_TYPE,
                resp.body.as_bytes(),
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
            // The dashboard SPA (#359): static assets embedded with `include_str!`
            // and served off this same responder. They need no topology, so they
            // live here in the sync `respond`. Each carries the strict UI security
            // headers (CSP, nosniff, frame-deny, no-referrer): CSS and JS are
            // SEPARATE files so the CSP `default-src 'self'` needs no
            // 'unsafe-inline'.
            //
            // SECURITY: the dashboard reads the unauthenticated `/api/*` recon
            // surface (node addresses, slowlog argv = key names, client IPs). The
            // UI is UNAUTHENTICATED today and relies on the loopback default bind;
            // it MUST move behind the auth/RBAC tier (#360) and VPN-locked
            // exposure (#369) before the console is exposed. See [`crate::api`].
            "/" => ui_response(
                "text/html; charset=utf-8",
                crate::assets::INDEX_HTML.as_bytes(),
                head,
            ),
            "/app.css" => ui_response(
                "text/css; charset=utf-8",
                crate::assets::APP_CSS.as_bytes(),
                head,
            ),
            "/app.js" => ui_response(
                "application/javascript; charset=utf-8",
                crate::assets::APP_JS.as_bytes(),
                head,
            ),
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
}

/// The HTTP reason phrase for the status codes the console emits. The default
/// (`200 OK`) covers the success case and any unexpected code defensively.
fn status_reason(code: u16) -> &'static str {
    match code {
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

/// The extra response headers carried by the UI assets (the dashboard SPA,
/// #359). A strict CSP that allows ONLY same-origin resources (so the separate
/// `app.css` / `app.js` load while inline script/style and any external/CDN
/// fetch are blocked), plus `X-Content-Type-Options: nosniff`, `X-Frame-Options:
/// DENY`, and `Referrer-Policy: no-referrer`. Each line is `Name: value\r\n`;
/// the builder inserts the block before the blank-line terminator.
const UI_SECURITY_HEADERS: &str = concat!(
    "Content-Security-Policy: default-src 'self'; base-uri 'none'; ",
    "frame-ancestors 'none'; object-src 'none'\r\n",
    "X-Content-Type-Options: nosniff\r\n",
    "X-Frame-Options: DENY\r\n",
    "Referrer-Policy: no-referrer\r\n",
);

/// Build a complete HTTP/1.1 response (status line, content headers,
/// `Connection: close`, body). One request per connection. When `head` is true
/// the `Content-Length` reflects what a GET would return but NO body bytes are
/// written (RFC 9110: a HEAD response must not carry a message body).
fn http_response(code: u16, reason: &str, content_type: &str, body: &[u8], head: bool) -> Vec<u8> {
    http_response_with_headers(code, reason, content_type, body, head, "")
}

/// A `200 OK` for a static UI asset, carrying the strict [`UI_SECURITY_HEADERS`]
/// in addition to the normal content headers. A HEAD still returns the headers
/// with the correct `Content-Length` but no body.
fn ui_response(content_type: &str, body: &[u8], head: bool) -> Vec<u8> {
    http_response_with_headers(200, "OK", content_type, body, head, UI_SECURITY_HEADERS)
}

/// [`http_response`] with an OPTIONAL block of `extra_headers` (each a complete
/// `Name: value\r\n` line, or empty for none) inserted before the blank-line
/// terminator. The status line, `Content-Type`, `Content-Length`, and
/// `Connection: close` are always emitted (so the existing probe/metrics/api
/// responses are byte-for-byte unchanged when `extra_headers` is empty), and a
/// HEAD still writes no body.
fn http_response_with_headers(
    code: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
    head: bool,
    extra_headers: &str,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 256);
    let header = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n",
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

    #[test]
    fn root_serves_html_with_the_security_headers() {
        let resp = String::from_utf8(test_state().respond("GET", "/")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(
            resp.contains("Content-Type: text/html; charset=utf-8"),
            "{resp}"
        );
        // The strict UI security headers are present.
        assert!(
            resp.contains("Content-Security-Policy: default-src 'self'"),
            "missing CSP: {resp}"
        );
        assert!(resp.contains("X-Content-Type-Options: nosniff"), "{resp}");
        assert!(resp.contains("X-Frame-Options: DENY"), "{resp}");
        assert!(resp.contains("Referrer-Policy: no-referrer"), "{resp}");
        // The existing Connection: close is still emitted.
        assert!(resp.contains("Connection: close"), "{resp}");
        // A known marker from the dashboard shell is in the body, and it links
        // the SEPARATE css/js (so the CSP needs no 'unsafe-inline').
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        assert!(body.contains("IronCache Console"), "{body}");
        assert!(body.contains("/app.css"), "{body}");
        assert!(body.contains("/app.js"), "{body}");
    }

    #[test]
    fn app_js_is_served_as_javascript() {
        let resp = String::from_utf8(test_state().respond("GET", "/app.js")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(
            resp.contains("Content-Type: application/javascript; charset=utf-8"),
            "{resp}"
        );
        assert!(
            resp.contains("Content-Security-Policy: default-src 'self'"),
            "{resp}"
        );
    }

    #[test]
    fn app_css_is_served_as_css() {
        let resp = String::from_utf8(test_state().respond("GET", "/app.css")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(
            resp.contains("Content-Type: text/css; charset=utf-8"),
            "{resp}"
        );
        assert!(
            resp.contains("Content-Security-Policy: default-src 'self'"),
            "{resp}"
        );
    }

    #[test]
    fn head_on_root_has_the_headers_but_no_body() {
        let resp = String::from_utf8(test_state().respond("HEAD", "/")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        // The headers (incl. CSP and a non-zero Content-Length) are present.
        assert!(
            resp.contains("Content-Security-Policy: default-src 'self'"),
            "{resp}"
        );
        let (header, body) = resp.split_once("\r\n\r\n").unwrap();
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
        // The GET on `/` returns a body of exactly that length.
        let get = String::from_utf8(test_state().respond("GET", "/")).unwrap();
        let (_gh, gbody) = get.split_once("\r\n\r\n").unwrap();
        assert_eq!(gbody.len(), cl);
    }

    #[test]
    fn metrics_response_is_unchanged_by_the_header_extension() {
        // The probe/metrics responses must NOT carry the UI security headers (the
        // header-block extension is opt-in via `ui_response`), and the existing
        // headers are intact.
        let resp = String::from_utf8(test_state().respond("GET", "/metrics")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"), "{resp}");
        assert!(!resp.contains("Content-Security-Policy"), "{resp}");
        assert!(!resp.contains("X-Frame-Options"), "{resp}");
        assert!(resp.contains("Connection: close"), "{resp}");
    }

    /// `/api/health` is served through the bounded responder, returns JSON, and
    /// does not require a polled topology.
    #[tokio::test]
    async fn api_health_is_json_without_a_poll() {
        let state = test_state();
        state.set_live(true);
        let resp = String::from_utf8(state.respond_async("GET", "/api/health").await).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(resp.contains("Content-Type: application/json"), "{resp}");
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["live"], true);
        assert_eq!(v["ready"], false);
    }

    /// A data route is `503` JSON before the first poll, then `200` after a
    /// topology is published into the shared holder.
    #[tokio::test]
    async fn api_cluster_is_503_before_poll_then_200_after() {
        use crate::snapshot::{NodeSnapshot, Topology, TopologyMode};
        let state = test_state();
        let before = String::from_utf8(state.respond_async("GET", "/api/cluster").await).unwrap();
        assert!(before.starts_with("HTTP/1.1 503"), "{before}");
        assert!(before.contains("application/json"), "{before}");
        let (_h, body) = before.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert!(v["error"].is_string(), "{body}");

        let topo = Topology {
            mode: TopologyMode::Standalone,
            nodes: vec![NodeSnapshot {
                addr: "10.0.0.1:6379".to_owned(),
                reachable: true,
                error: None,
                info: None,
                slowlog: Vec::new(),
                slowlog_error: None,
                clients: Vec::new(),
                clients_error: None,
                fetched_unixtime: 42,
            }],
            fetched_unixtime: 42,
        };
        *state.topology().write().await = Some(topo);
        let after = String::from_utf8(state.respond_async("GET", "/api/cluster").await).unwrap();
        assert!(after.starts_with("HTTP/1.1 200 OK"), "{after}");
        let (_h, body) = after.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["mode"], "standalone");
        assert_eq!(v["nodes_total"], 1);
    }

    /// An unknown `/api/*` endpoint is `404` JSON, and a non-GET to `/api/*` is
    /// `405`.
    #[tokio::test]
    async fn api_unknown_is_404_and_post_is_405() {
        let state = test_state();
        // A topology so we are past the 503-before-poll gate.
        *state.topology().write().await = Some(crate::snapshot::Topology {
            mode: crate::snapshot::TopologyMode::Standalone,
            nodes: Vec::new(),
            fetched_unixtime: 1,
        });
        let nf = String::from_utf8(state.respond_async("GET", "/api/bogus").await).unwrap();
        assert!(nf.starts_with("HTTP/1.1 404 Not Found"), "{nf}");
        assert!(nf.contains("application/json"), "{nf}");
        let post = String::from_utf8(state.respond_async("POST", "/api/cluster").await).unwrap();
        assert!(post.starts_with("HTTP/1.1 405"), "{post}");
    }

    /// `/api/openapi.json` is served and parses as JSON.
    #[tokio::test]
    async fn api_openapi_is_valid_json() {
        let state = test_state();
        let resp =
            String::from_utf8(state.respond_async("GET", "/api/openapi.json").await).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["openapi"], "3.0.3");
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
