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
    /// Readiness: set `true` when the console can serve. PR-1 sets it at boot;
    /// #355 will gate it on the first successful node poll.
    ready: Arc<AtomicBool>,
}

impl ConsoleHttpState {
    #[must_use]
    pub fn new(metrics: Arc<ConsoleMetrics>) -> Self {
        ConsoleHttpState {
            metrics,
            live: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Flip liveness (called once at end of boot).
    pub fn set_live(&self, v: bool) {
        self.live.store(v, Ordering::SeqCst);
    }

    /// Flip readiness.
    pub fn set_ready(&self, v: bool) {
        self.ready.store(v, Ordering::SeqCst);
    }

    /// Render the response bytes for a parsed `(method, path)`. Pure: reads the
    /// live state and returns the bytes; the connection handler writes them.
    /// Exposed for tests.
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
                return Some(state.respond(&method, &path));
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
