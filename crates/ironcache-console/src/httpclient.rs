// SPDX-License-Identifier: MIT OR Apache-2.0
//! A minimal, bounded, hand-rolled HTTP/1.1 GET client (issue #356).
//!
//! The console queries Prometheus's HTTP API for history, but there was no HTTP
//! CLIENT in the crate: the [`crate::node`] client is RESP, and the metrics
//! endpoint is a hand-rolled HTTP SERVER ([`crate::http`]). This is the matching
//! CLIENT, built in the SAME hand-rolled style (a tokio [`TcpStream`], no
//! hyper/reqwest) so the static musl/aarch64 build stays pure-Rust and the
//! supply-chain (cargo-deny) posture is unchanged: it adds NO dependency.
//!
//! It is HARD bounded, the same discipline as the node client:
//!
//! * the TCP connect is wrapped in a `connect_timeout`, and
//! * the response read is wrapped in a `read_timeout`.
//!
//! The read bound is load-bearing: a down or never-replying Prometheus must
//! surface a [`HttpError::Timeout`] PROMPTLY, never hang the request task (the
//! same hazard a missing read timeout once caused in the node client). A hard
//! response-SIZE cap ([`MAX_RESPONSE_BYTES`]) means a hostile or huge response
//! cannot drive an unbounded allocation / OOM the console.
//!
//! ## HTTP only (v1)
//!
//! Only `http://` is supported. HTTPS-to-Prometheus is DEFERRED: the production
//! Prometheus is in-VPC (reached over the private network), and the runtime
//! crate's TLS client ([`ironcache_runtime::tls`]) presents a FIXED cluster SNI
//! (`ironcache-cluster`), which is unsuitable for dialing an arbitrary host. A
//! per-host SNI TLS client for outbound console links is future work (the same
//! deferral the node-to-console link carries, #369). An `https://` URL is
//! rejected here rather than silently downgraded.
//!
//! ## SSRF defense-in-depth (#369)
//!
//! The Prometheus URL is server-config-only (the request never supplies it, and
//! the metric name is allowlisted to a bare `ironcache_*` name in
//! [`crate::history`]), so SSRF is already structurally bounded. This client adds
//! two belt-and-suspenders defenses so a compromised/misconfigured upstream still
//! cannot be used as a pivot:
//!
//! * NO redirect following. A 3xx response is surfaced as
//!   [`HttpError::RedirectNotFollowed`]; the client never auto-connects to a
//!   `Location`-supplied host (that would be an SSRF pivot).
//! * Link-local / cloud-metadata block. After the host is RESOLVED, every
//!   candidate address is screened ([`is_blocked_ip`]): `169.254.0.0/16` (which
//!   includes the `169.254.169.254` instance-metadata IP) and `fe80::/10` are
//!   rejected with [`HttpError::BlockedAddress`] BEFORE a socket opens. The check
//!   is on the PARSED [`IpAddr`], not the host string, so the decimal/octal/hex
//!   IPv4 forms and the IPv4-mapped-IPv6 form cannot bypass it. Normal private
//!   ranges (RFC 1918, the in-VPC Prometheus) are deliberately NOT blocked.
//!
//! ## Determinism (ADR-0003)
//!
//! No clock and no RNG: this is pure I/O plus the runtime timer bound (the only
//! `tokio::time` use, the sanctioned timer seam the determinism lint allows).

use std::net::IpAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

/// A hard cap on the total response bytes (status line + headers + body) buffered
/// before the client gives up. A Prometheus `query_range` reply for the bounded
/// windows the console requests is far under this; the cap exists so a hostile or
/// misbehaving endpoint cannot drive an unbounded allocation. The chunked decoder
/// and the Content-Length path both enforce it.
pub const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// A cap on a SINGLE chunk size declared by a `chunked` response, so a hostile
/// chunk-size line cannot request an enormous allocation in one step. The overall
/// [`MAX_RESPONSE_BYTES`] still bounds the accumulated total; this additionally
/// bounds any one declared chunk before a byte of it is read.
const MAX_CHUNK_BYTES: u64 = MAX_RESPONSE_BYTES as u64;

/// A typed error from an HTTP GET. Distinct variants so the caller (the history
/// adapter) can label a failure precisely: a connect/timeout failure reads
/// differently from a malformed response or a non-2xx status.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// The URL could not be parsed (bad scheme, missing host, unsupported
    /// scheme), BEFORE any connection is attempted.
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
    /// The TCP connect to the host failed.
    #[error("connecting to {host}:{port}: {source}")]
    Connect {
        /// The host dialed.
        host: String,
        /// The port dialed.
        port: u16,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// An operation (connect or read) exceeded its timeout bound.
    #[error("HTTP operation timed out after {0:?}")]
    Timeout(Duration),
    /// A general I/O error writing the request or reading the response.
    #[error("HTTP I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The response was not valid HTTP/1.x the client could parse (bad status
    /// line, bad header, malformed chunked framing).
    #[error("malformed HTTP response: {0}")]
    Protocol(String),
    /// The response (head + body) exceeded [`MAX_RESPONSE_BYTES`].
    #[error("HTTP response exceeded {0} bytes")]
    TooLarge(usize),
    /// The server answered with a 3xx redirect. SSRF DEFENSE-IN-DEPTH: the client
    /// does NOT follow redirects (a `Location` to an attacker-chosen host would be
    /// an SSRF pivot from the server-config-only Prometheus URL). The status is
    /// surfaced as this typed error instead of auto-connecting onward.
    #[error("HTTP redirect ({status}) not followed (SSRF defense): Location {location}")]
    RedirectNotFollowed {
        /// The 3xx status code returned.
        status: u16,
        /// The `Location` header value, or `<none>` when the redirect carried no
        /// `Location`. Surfaced for diagnostics only; never dialed.
        location: String,
    },
    /// The resolved address is a link-local / cloud-metadata address
    /// (`169.254.0.0/16`, which includes the `169.254.169.254` metadata IP, or
    /// `fe80::/10`). SSRF DEFENSE-IN-DEPTH: dialing such an address could read
    /// instance credentials, so it is rejected after resolution, BEFORE connecting.
    /// Normal private ranges (RFC 1918, e.g. the in-VPC Prometheus) are NOT blocked.
    #[error("refusing to connect to link-local/metadata address {0} (SSRF defense)")]
    BlockedAddress(std::net::IpAddr),
}

/// A parsed HTTP response: the status code and the body bytes. Headers are parsed
/// internally (to decide the body framing) but not surfaced; the caller needs
/// only the status and the body (a JSON document, for the Prometheus adapter).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// The HTTP status code (e.g. 200, 400, 503).
    pub status: u16,
    /// The decoded response body bytes (Content-Length or de-chunked).
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// The body as a lossy UTF-8 string (convenience for JSON callers).
    #[must_use]
    pub fn body_string(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// A parsed `http://host[:port]/path?query` URL. Only the parts the client needs.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedUrl {
    host: String,
    port: u16,
    /// The path plus query (the HTTP request target), always starting with `/`.
    target: String,
}

/// Parse an `http://` URL into [`ParsedUrl`]. Rejects a non-`http` scheme (HTTPS
/// is deferred, see the module docs) and a missing host. An IPv6 literal host in
/// brackets (`http://[::1]:9090/`) is handled. A missing path defaults to `/`.
///
/// # Errors
///
/// Returns [`HttpError::InvalidUrl`] on an unsupported scheme, a missing host, or
/// an unparseable port.
fn parse_url(url: &str) -> Result<ParsedUrl, HttpError> {
    // Scheme: HTTP only. HTTPS is deferred (fixed-SNI runtime TLS, module docs);
    // reject it rather than silently downgrade to plaintext.
    let Some(rest) = url.strip_prefix("http://") else {
        if url.starts_with("https://") {
            return Err(HttpError::InvalidUrl(
                "https is not supported (HTTP only for v1; the in-VPC Prometheus is reached over \
                 plaintext, see the module docs)"
                    .to_owned(),
            ));
        }
        return Err(HttpError::InvalidUrl(format!(
            "expected an http:// URL, got '{url}'"
        )));
    };

    // Split the authority (host[:port]) from the path+query at the first '/'.
    let (authority, target) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_owned()),
        None => (rest, "/".to_owned()),
    };
    if authority.is_empty() {
        return Err(HttpError::InvalidUrl("missing host".to_owned()));
    }

    // Host + optional port. Handle a bracketed IPv6 literal first so its inner
    // colons are not mistaken for the port separator.
    let (host, port) = if let Some(after_bracket) = authority.strip_prefix('[') {
        let close = after_bracket.find(']').ok_or_else(|| {
            HttpError::InvalidUrl(format!("unterminated IPv6 literal in '{authority}'"))
        })?;
        let host = &after_bracket[..close];
        let remainder = &after_bracket[close + 1..];
        let port = parse_optional_port(remainder.strip_prefix(':'), remainder)?;
        (host.to_owned(), port)
    } else if let Some((h, p)) = authority.rsplit_once(':') {
        let port = p
            .parse::<u16>()
            .map_err(|e| HttpError::InvalidUrl(format!("invalid port '{p}': {e}")))?;
        (h.to_owned(), port)
    } else {
        (authority.to_owned(), 80)
    };
    if host.is_empty() {
        return Err(HttpError::InvalidUrl("missing host".to_owned()));
    }

    Ok(ParsedUrl { host, port, target })
}

/// Validate a SERVER-CONFIGURED outbound base URL at BOOT (#369): the same
/// parse the client applies per request (`http://` scheme only, a non-empty
/// host, a well-formed port), surfaced so `config::validate` can fail fast on a
/// bad scheme/host instead of erroring on the first query. No I/O and no
/// resolution here; the per-dial screens (redirect refusal, link-local block)
/// still apply at request time.
///
/// # Errors
///
/// Returns [`HttpError::InvalidUrl`] exactly when a request-time [`get`] of this
/// URL would.
pub fn validate_url(url: &str) -> Result<(), HttpError> {
    parse_url(url).map(|_| ())
}

/// Parse the port that may follow an IPv6 `]`: `Some(":9090")`-stripped digits, or
/// the default `80` when no `:port` follows. `remainder` is included only for the
/// error message when a non-`:` trailer is present.
fn parse_optional_port(after_colon: Option<&str>, remainder: &str) -> Result<u16, HttpError> {
    match after_colon {
        Some(p) => p
            .parse::<u16>()
            .map_err(|e| HttpError::InvalidUrl(format!("invalid port '{p}': {e}"))),
        None if remainder.is_empty() => Ok(80),
        None => Err(HttpError::InvalidUrl(format!(
            "unexpected text after IPv6 literal: '{remainder}'"
        ))),
    }
}

/// Issue one bounded HTTP/1.1 `GET url` and return the [`HttpResponse`].
///
/// The whole TCP connect is bounded by `connect_timeout`; the whole
/// request-write + response-read phase is bounded by `read_timeout`. A
/// never-replying server therefore returns [`HttpError::Timeout`] promptly, never
/// hanging. The response is capped at [`MAX_RESPONSE_BYTES`]; both the
/// Content-Length and the `Transfer-Encoding: chunked` body framings are handled.
/// The connection is single-use (`Connection: close`).
///
/// # Errors
///
/// Returns [`HttpError::InvalidUrl`] on a bad URL, [`HttpError::BlockedAddress`]
/// when the host resolves to a link-local/metadata address (SSRF defense),
/// [`HttpError::Connect`] / [`HttpError::Timeout`] on a failed or slow dial,
/// [`HttpError::RedirectNotFollowed`] on a 3xx (the client never follows
/// redirects, SSRF defense), [`HttpError::Io`] / [`HttpError::Protocol`] on a
/// transport / parse fault, or [`HttpError::TooLarge`] when the response exceeds
/// the cap.
pub async fn get(
    url: &str,
    connect_timeout: Duration,
    read_timeout: Duration,
) -> Result<HttpResponse, HttpError> {
    let parsed = parse_url(url)?;
    let stream = tokio::time::timeout(connect_timeout, dial(&parsed))
        .await
        .map_err(|_| HttpError::Timeout(connect_timeout))??;
    tokio::time::timeout(read_timeout, exchange(stream, &parsed))
        .await
        .map_err(|_| HttpError::Timeout(read_timeout))?
}

/// Connect to the parsed host/port and set `TCP_NODELAY`. Not itself bounded (the
/// caller wraps it in `connect_timeout`).
///
/// SSRF DEFENSE-IN-DEPTH: the host is RESOLVED first, then every candidate
/// [`IpAddr`] is screened against the link-local / cloud-metadata block
/// ([`is_blocked_ip`]) BEFORE a socket is opened, and the connect targets the
/// vetted [`std::net::SocketAddr`] (not the original host string). Screening the
/// PARSED ip, not the textual host, defeats the decimal / octal / hex IPv4 forms
/// and the IPv4-mapped-IPv6 form, all of which `to_socket_addrs` normalizes to the
/// same `IpAddr` we inspect. A host that resolves to no usable (non-blocked)
/// address yields [`HttpError::BlockedAddress`] / [`HttpError::Connect`] without a
/// dial. Normal private ranges (RFC 1918) are deliberately NOT blocked.
async fn dial(url: &ParsedUrl) -> Result<TcpStream, HttpError> {
    // Resolve every candidate address for host:port. `lookup_host` parses a bare
    // IP literal directly and resolves a DNS name (no socket opened yet).
    let addrs = tokio::net::lookup_host((url.host.as_str(), url.port))
        .await
        .map_err(|source| HttpError::Connect {
            host: url.host.clone(),
            port: url.port,
            source,
        })?;

    // Screen, then try to connect to the first vetted address. ANY blocked
    // candidate aborts the whole dial (fail closed): a name that resolves to a mix
    // of public and metadata addresses must not be reachable via the public one.
    let mut last_err: Option<HttpError> = None;
    let mut any_candidate = false;
    for addr in addrs {
        any_candidate = true;
        let ip = addr.ip();
        if is_blocked_ip(ip) {
            return Err(HttpError::BlockedAddress(ip));
        }
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                stream.set_nodelay(true)?;
                return Ok(stream);
            }
            Err(source) => {
                last_err = Some(HttpError::Connect {
                    host: url.host.clone(),
                    port: url.port,
                    source,
                });
            }
        }
    }

    Err(last_err.unwrap_or_else(|| HttpError::Connect {
        host: url.host.clone(),
        port: url.port,
        source: std::io::Error::new(
            std::io::ErrorKind::NotFound,
            if any_candidate {
                "no address could be connected"
            } else {
                "host resolved to no addresses"
            },
        ),
    }))
}

/// Whether `ip` is a link-local / cloud-metadata address the client refuses to
/// dial (SSRF defense): IPv4 `169.254.0.0/16` (which includes the cloud metadata
/// IP `169.254.169.254`) or IPv6 `fe80::/10` link-local. An IPv4-mapped IPv6
/// address (`::ffff:a.b.c.d`) is unwrapped to its IPv4 form first, so the mapped
/// form of a link-local v4 address is caught too. Normal private ranges (RFC 1918:
/// `10/8`, `172.16/12`, `192.168/16`) are intentionally NOT blocked: the in-VPC
/// Prometheus is RFC 1918, and only the link-local/metadata range is the SSRF risk
/// this guard targets.
fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => {
            // Unwrap an IPv4-mapped/compatible address to its v4 form and re-check,
            // so `::ffff:169.254.169.254` is caught as a link-local v4 address.
            if let Some(mapped) = v6.to_ipv4() {
                return mapped.is_link_local();
            }
            // fe80::/10 link-local: the top 10 bits are 1111111010.
            let seg0 = v6.segments()[0];
            (seg0 & 0xffc0) == 0xfe80
        }
    }
}

/// Write the GET request and read + frame one response. The caller bounds this in
/// `read_timeout` (so the write and the read together cannot hang).
async fn exchange(mut stream: TcpStream, url: &ParsedUrl) -> Result<HttpResponse, HttpError> {
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: ironcache-console\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
        url.target,
        host_header(url),
    );
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;
    read_response(&mut stream).await
}

/// The `Host` header value: `host` for a default port, `host:port` otherwise. An
/// IPv6 literal host is re-bracketed.
fn host_header(url: &ParsedUrl) -> String {
    let host = if url.host.contains(':') {
        format!("[{}]", url.host)
    } else {
        url.host.clone()
    };
    if url.port == 80 {
        host
    } else {
        format!("{host}:{}", url.port)
    }
}

/// Read the full response from `stream`: the head (status line + headers) up to
/// the blank line, then the body framed by Content-Length or chunked encoding.
/// Enforces [`MAX_RESPONSE_BYTES`] across the whole read.
async fn read_response<S>(stream: &mut S) -> Result<HttpResponse, HttpError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    // Read until the end of the header block (CRLF CRLF) is in the buffer.
    let header_end = loop {
        if let Some(pos) = find_header_end(&buf) {
            break pos;
        }
        if buf.len() > MAX_RESPONSE_BYTES {
            return Err(HttpError::TooLarge(MAX_RESPONSE_BYTES));
        }
        let n = read_some(stream, &mut buf).await?;
        if n == 0 {
            return Err(HttpError::Protocol(
                "connection closed before the response headers completed".to_owned(),
            ));
        }
    };

    // header_end is the index just past the blank line; everything after is body.
    let head = &buf[..header_end];
    let (status, framing, location) = parse_head(head)?;
    let body_start = header_end;

    // SSRF DEFENSE-IN-DEPTH: do NOT follow a 3xx redirect. Auto-connecting to a
    // `Location`-supplied host would be an SSRF pivot from the server-config-only
    // Prometheus URL. Surface the status as a typed error and stop here (the body
    // is not even drained); the caller never dials the redirect target.
    if (300..400).contains(&status) {
        return Err(HttpError::RedirectNotFollowed {
            status,
            location: location.unwrap_or_else(|| "<none>".to_owned()),
        });
    }

    let body = match framing {
        BodyFraming::ContentLength(len) => {
            read_content_length(stream, &mut buf, body_start, len).await?
        }
        BodyFraming::Chunked => decode_chunked(stream, &mut buf, body_start).await?,
        BodyFraming::CloseDelimited => read_until_close(stream, &mut buf, body_start).await?,
    };

    Ok(HttpResponse { status, body })
}

/// How the response body is framed, decided from the headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyFraming {
    /// `Content-Length: n` bytes follow.
    ContentLength(usize),
    /// `Transfer-Encoding: chunked`.
    Chunked,
    /// Neither header: the body runs until the peer closes (HTTP/1.0 style). We
    /// sent `Connection: close`, so a server may answer this way.
    CloseDelimited,
}

/// Find the index just past the header-terminating blank line (`\r\n\r\n`), or
/// `None` if the head is not complete yet. A bare `\n\n` terminator is also
/// accepted defensively (tolerant of a non-canonical server).
fn find_header_end(buf: &[u8]) -> Option<usize> {
    // Canonical CRLFCRLF.
    if let Some(i) = find_subslice(buf, b"\r\n\r\n") {
        return Some(i + 4);
    }
    // Tolerant LF LF (some minimal servers / test stubs).
    if let Some(i) = find_subslice(buf, b"\n\n") {
        return Some(i + 2);
    }
    None
}

/// Find the first index of `needle` in `haystack`, or `None`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse the status line and the headers (the byte slice up to and including the
/// blank line). Returns the status code, the body framing, and the `Location`
/// header value when present (surfaced so a 3xx can report where it would have
/// redirected, WITHOUT following it). Header names are matched case-insensitively.
/// A `chunked` Transfer-Encoding takes precedence over a Content-Length (per
/// RFC 9112).
fn parse_head(head: &[u8]) -> Result<(u16, BodyFraming, Option<String>), HttpError> {
    let text = String::from_utf8_lossy(head);
    let mut lines = text.split("\r\n").flat_map(|l| l.split('\n'));
    let status_line = lines
        .next()
        .ok_or_else(|| HttpError::Protocol("empty response".to_owned()))?;
    let status = parse_status_line(status_line)?;

    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    let mut location: Option<String> = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            // A header line without a colon is malformed; be tolerant and skip it
            // (a continuation/obs-fold is rare and not expected from Prometheus).
            continue;
        };
        let name = name.trim();
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let n = value.parse::<usize>().map_err(|e| {
                HttpError::Protocol(format!("invalid Content-Length '{value}': {e}"))
            })?;
            if n > MAX_RESPONSE_BYTES {
                return Err(HttpError::TooLarge(MAX_RESPONSE_BYTES));
            }
            content_length = Some(n);
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            // Any transfer-coding list ending in `chunked` means chunked framing.
            if value
                .split(',')
                .map(str::trim)
                .any(|t| t.eq_ignore_ascii_case("chunked"))
            {
                chunked = true;
            }
        } else if name.eq_ignore_ascii_case("location") && location.is_none() {
            // Captured for the 3xx diagnostic only; the client never dials it.
            location = Some(value.to_owned());
        }
    }

    // chunked wins over Content-Length (RFC 9112 6.1).
    let framing = if chunked {
        BodyFraming::Chunked
    } else if let Some(n) = content_length {
        BodyFraming::ContentLength(n)
    } else {
        BodyFraming::CloseDelimited
    };
    Ok((status, framing, location))
}

/// Parse the `HTTP/1.x <code> <reason>` status line into the numeric code.
fn parse_status_line(line: &str) -> Result<u16, HttpError> {
    let mut parts = line.split(' ');
    let version = parts.next().unwrap_or("");
    if !version.starts_with("HTTP/") {
        return Err(HttpError::Protocol(format!(
            "not an HTTP status line: '{line}'"
        )));
    }
    let code = parts
        .next()
        .ok_or_else(|| HttpError::Protocol(format!("missing status code in '{line}'")))?;
    code.parse::<u16>()
        .map_err(|e| HttpError::Protocol(format!("invalid status code '{code}': {e}")))
}

/// Read exactly `len` body bytes (some may already be buffered after the head).
/// `buf[body_start..]` is the already-read body prefix.
async fn read_content_length<S>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    body_start: usize,
    len: usize,
) -> Result<Vec<u8>, HttpError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    // The total bytes we will hold (head already counted in buf). Guard the cap.
    if body_start.saturating_add(len) > MAX_RESPONSE_BYTES {
        return Err(HttpError::TooLarge(MAX_RESPONSE_BYTES));
    }
    while buf.len() - body_start < len {
        let n = read_some(stream, buf).await?;
        if n == 0 {
            return Err(HttpError::Protocol(format!(
                "connection closed with {} of {} body bytes read",
                buf.len() - body_start,
                len
            )));
        }
    }
    Ok(buf[body_start..body_start + len].to_vec())
}

/// Read body bytes until the peer closes the connection (no length, no chunked).
/// Bounded by [`MAX_RESPONSE_BYTES`].
async fn read_until_close<S>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    body_start: usize,
) -> Result<Vec<u8>, HttpError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    loop {
        if buf.len() > MAX_RESPONSE_BYTES {
            return Err(HttpError::TooLarge(MAX_RESPONSE_BYTES));
        }
        let n = read_some(stream, buf).await?;
        if n == 0 {
            break;
        }
    }
    Ok(buf[body_start..].to_vec())
}

/// Decode a `Transfer-Encoding: chunked` body into the assembled bytes. The
/// already-buffered tail (`buf[body_start..]`) is the start of the chunk stream;
/// more is read from `stream` as needed. Each chunk is `<hex-size>CRLF<data>CRLF`,
/// terminated by a zero-size chunk. A trailing chunk-extension (`;ext`) on the
/// size line is ignored. Bounds: a single chunk size is capped at
/// [`MAX_CHUNK_BYTES`] and the accumulated total at [`MAX_RESPONSE_BYTES`].
async fn decode_chunked<S>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    body_start: usize,
) -> Result<Vec<u8>, HttpError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut out: Vec<u8> = Vec::new();
    // `cursor` walks the raw chunk stream within `buf`, starting at the body.
    let mut cursor = body_start;
    loop {
        // 1. Read the chunk-size line (up to CRLF), filling from the socket.
        let (size, line_end) = read_chunk_size_line(stream, buf, cursor).await?;
        cursor = line_end;
        if size == 0 {
            // The last chunk. Consume the trailing CRLF (and any trailers) up to
            // the final blank line, but we do not need them; just stop.
            return Ok(out);
        }
        if size > MAX_CHUNK_BYTES {
            return Err(HttpError::Protocol(format!(
                "chunk size {size} exceeds the {MAX_CHUNK_BYTES}-byte per-chunk cap"
            )));
        }
        let size = size as usize;
        if out.len().saturating_add(size) > MAX_RESPONSE_BYTES {
            return Err(HttpError::TooLarge(MAX_RESPONSE_BYTES));
        }
        // 2. Ensure `size` data bytes plus the trailing CRLF are buffered.
        let need_end = cursor.saturating_add(size).saturating_add(2);
        while buf.len() < need_end {
            if buf.len() > MAX_RESPONSE_BYTES {
                return Err(HttpError::TooLarge(MAX_RESPONSE_BYTES));
            }
            let n = read_some(stream, buf).await?;
            if n == 0 {
                return Err(HttpError::Protocol(
                    "connection closed mid-chunk".to_owned(),
                ));
            }
        }
        out.extend_from_slice(&buf[cursor..cursor + size]);
        cursor += size;
        // 3. The chunk data is followed by a CRLF; consume it.
        if &buf[cursor..cursor + 2] != b"\r\n" {
            return Err(HttpError::Protocol(
                "chunk data not terminated by CRLF".to_owned(),
            ));
        }
        cursor += 2;
    }
}

/// Read and parse one chunk-size line (`<hex>[;ext]CRLF`) starting at `cursor` in
/// `buf`, filling from `stream` until the CRLF is present. Returns the parsed
/// size and the index just past the CRLF. A bare LF terminator is tolerated.
async fn read_chunk_size_line<S>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    cursor: usize,
) -> Result<(u64, usize), HttpError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    loop {
        // Search for a line terminator from the cursor onward.
        if let Some(rel) = buf[cursor..].iter().position(|&b| b == b'\n') {
            let line_end = cursor + rel + 1;
            // The size token is everything up to the LF, minus a trailing CR, and
            // before any `;` chunk-extension.
            let mut line = &buf[cursor..cursor + rel];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            let token = match line.iter().position(|&b| b == b';') {
                Some(i) => &line[..i],
                None => line,
            };
            let size = parse_hex_size(token)?;
            return Ok((size, line_end));
        }
        // Not yet a full line. Bound the unparsed size-line length so a hostile
        // server cannot stream an endless size line (no LF) past the cap.
        if buf.len() - cursor > 64 * 1024 {
            return Err(HttpError::Protocol(
                "chunk size line exceeded 64KiB without a terminator".to_owned(),
            ));
        }
        if buf.len() > MAX_RESPONSE_BYTES {
            return Err(HttpError::TooLarge(MAX_RESPONSE_BYTES));
        }
        let n = read_some(stream, buf).await?;
        if n == 0 {
            return Err(HttpError::Protocol(
                "connection closed before a chunk size line completed".to_owned(),
            ));
        }
    }
}

/// Parse a hex chunk-size token (e.g. `1a`) into a `u64`. An empty or non-hex
/// token is a protocol error.
fn parse_hex_size(token: &[u8]) -> Result<u64, HttpError> {
    let s = std::str::from_utf8(token)
        .map_err(|_| HttpError::Protocol("non-UTF8 chunk size".to_owned()))?
        .trim();
    if s.is_empty() {
        return Err(HttpError::Protocol("empty chunk size".to_owned()));
    }
    u64::from_str_radix(s, 16)
        .map_err(|e| HttpError::Protocol(format!("invalid chunk size '{s}': {e}")))
}

/// Read some bytes from `stream` into the grown tail of `buf` (no large stack
/// chunk), returning the count (0 = peer closed). Mirrors the node client's
/// read-into-tail helper.
async fn read_some<S>(stream: &mut S, buf: &mut Vec<u8>) -> Result<usize, HttpError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let start = buf.len();
    let want = 16 * 1024;
    buf.resize(start + want, 0);
    let n = stream.read(&mut buf[start..]).await?;
    buf.truncate(start + n);
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_url_plain_host_default_port() {
        let u = parse_url("http://prom/api/v1/query_range?query=x").unwrap();
        assert_eq!(u.host, "prom");
        assert_eq!(u.port, 80);
        assert_eq!(u.target, "/api/v1/query_range?query=x");
    }

    #[test]
    fn parse_url_host_with_port_and_no_path() {
        let u = parse_url("http://10.0.0.5:9090").unwrap();
        assert_eq!(u.host, "10.0.0.5");
        assert_eq!(u.port, 9090);
        // A missing path defaults to "/".
        assert_eq!(u.target, "/");
    }

    #[test]
    fn parse_url_ipv6_literal_with_port() {
        let u = parse_url("http://[::1]:9090/api").unwrap();
        assert_eq!(u.host, "::1");
        assert_eq!(u.port, 9090);
        assert_eq!(u.target, "/api");
    }

    #[test]
    fn parse_url_ipv6_literal_default_port() {
        let u = parse_url("http://[2001:db8::1]/x").unwrap();
        assert_eq!(u.host, "2001:db8::1");
        assert_eq!(u.port, 80);
    }

    #[test]
    fn parse_url_rejects_https_and_other_schemes() {
        let e = parse_url("https://prom:9090/api").unwrap_err();
        assert!(matches!(e, HttpError::InvalidUrl(_)));
        assert!(e.to_string().contains("https is not supported"), "{e}");
        assert!(matches!(
            parse_url("ftp://x/").unwrap_err(),
            HttpError::InvalidUrl(_)
        ));
    }

    #[test]
    fn parse_url_rejects_missing_host_and_bad_port() {
        assert!(matches!(
            parse_url("http:///path").unwrap_err(),
            HttpError::InvalidUrl(_)
        ));
        assert!(matches!(
            parse_url("http://host:notaport/").unwrap_err(),
            HttpError::InvalidUrl(_)
        ));
    }

    #[test]
    fn host_header_omits_default_port_and_brackets_ipv6() {
        let u = ParsedUrl {
            host: "prom".to_owned(),
            port: 80,
            target: "/".to_owned(),
        };
        assert_eq!(host_header(&u), "prom");
        let u = ParsedUrl {
            host: "prom".to_owned(),
            port: 9090,
            target: "/".to_owned(),
        };
        assert_eq!(host_header(&u), "prom:9090");
        let u = ParsedUrl {
            host: "::1".to_owned(),
            port: 9090,
            target: "/".to_owned(),
        };
        assert_eq!(host_header(&u), "[::1]:9090");
    }

    #[test]
    fn find_header_end_handles_crlf_and_lf() {
        assert_eq!(find_header_end(b"HTTP/1.1 200 OK\r\n\r\nbody"), Some(19));
        assert_eq!(find_header_end(b"HTTP/1.1 200 OK\n\nbody"), Some(17));
        assert_eq!(find_header_end(b"HTTP/1.1 200 OK\r\nNo end yet"), None);
    }

    #[test]
    fn parse_head_reads_status_and_content_length() {
        let head =
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 42\r\n\r\n";
        let (status, framing, location) = parse_head(head).unwrap();
        assert_eq!(status, 200);
        assert_eq!(framing, BodyFraming::ContentLength(42));
        assert_eq!(location, None);
    }

    #[test]
    fn parse_head_detects_chunked_case_insensitively_over_length() {
        let head = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nTransfer-Encoding: CHUNKED\r\n\r\n";
        let (status, framing, _location) = parse_head(head).unwrap();
        assert_eq!(status, 200);
        // chunked takes precedence over Content-Length.
        assert_eq!(framing, BodyFraming::Chunked);
    }

    #[test]
    fn parse_head_defaults_to_close_delimited() {
        let head = b"HTTP/1.0 200 OK\r\n\r\n";
        let (_status, framing, _location) = parse_head(head).unwrap();
        assert_eq!(framing, BodyFraming::CloseDelimited);
    }

    #[test]
    fn parse_head_rejects_oversize_content_length() {
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            MAX_RESPONSE_BYTES + 1
        );
        assert!(matches!(
            parse_head(head.as_bytes()).unwrap_err(),
            HttpError::TooLarge(_)
        ));
    }

    #[test]
    fn parse_head_rejects_bad_status_line() {
        assert!(matches!(
            parse_head(b"NOT HTTP\r\n\r\n").unwrap_err(),
            HttpError::Protocol(_)
        ));
    }

    #[test]
    fn parse_hex_size_parses_and_rejects() {
        assert_eq!(parse_hex_size(b"1a").unwrap(), 26);
        assert_eq!(parse_hex_size(b"0").unwrap(), 0);
        assert!(parse_hex_size(b"").is_err());
        assert!(parse_hex_size(b"zz").is_err());
    }

    /// A small in-memory reader so the body-framing decoders can be unit-tested
    /// without a socket: it yields the bytes in fixed-size slices to exercise the
    /// "read more" loops (the body arriving across several reads).
    struct ChunkedReader {
        data: Vec<u8>,
        pos: usize,
        step: usize,
    }

    impl ChunkedReader {
        fn new(data: &[u8], step: usize) -> Self {
            ChunkedReader {
                data: data.to_vec(),
                pos: 0,
                step: step.max(1),
            }
        }
    }

    impl tokio::io::AsyncRead for ChunkedReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            dst: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let remaining = self.data.len().saturating_sub(self.pos);
            let n = remaining.min(self.step).min(dst.remaining());
            if n > 0 {
                let start = self.pos;
                dst.put_slice(&self.data[start..start + n]);
                self.pos += n;
            }
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn read_response_content_length_body() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\nhello world";
        // Tiny step so the body arrives across many reads, exercising the loop.
        let mut reader = ChunkedReader::new(raw, 3);
        let resp = read_response(&mut reader).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"hello world");
        assert_eq!(resp.body_string(), "hello world");
    }

    #[tokio::test]
    async fn read_response_chunked_body() {
        // Two data chunks ("Wiki" + "pedia") then the terminating zero chunk.
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                    4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
        let mut reader = ChunkedReader::new(raw, 2);
        let resp = read_response(&mut reader).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"Wikipedia");
    }

    #[tokio::test]
    async fn read_response_chunked_with_extension_is_ignored() {
        // A chunk-size line carrying a chunk-extension (`;name=value`).
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                    4;foo=bar\r\nWiki\r\n0\r\n\r\n";
        let mut reader = ChunkedReader::new(raw, 5);
        let resp = read_response(&mut reader).await.unwrap();
        assert_eq!(resp.body, b"Wiki");
    }

    #[tokio::test]
    async fn read_response_close_delimited_body() {
        // No Content-Length, no chunked: body runs to EOF.
        let raw = b"HTTP/1.0 200 OK\r\n\r\nthe whole body";
        let mut reader = ChunkedReader::new(raw, 4);
        let resp = read_response(&mut reader).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"the whole body");
    }

    #[tokio::test]
    async fn read_response_truncated_content_length_is_protocol_error() {
        // Declares 20 bytes but only 5 follow then EOF.
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\nhello";
        let mut reader = ChunkedReader::new(raw, 8);
        let err = read_response(&mut reader).await.unwrap_err();
        assert!(matches!(err, HttpError::Protocol(_)), "{err}");
    }

    #[tokio::test]
    async fn read_response_oversize_chunk_is_rejected() {
        // A single chunk that declares more than the per-chunk cap.
        let raw = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
            MAX_CHUNK_BYTES + 1
        );
        let mut reader = ChunkedReader::new(raw.as_bytes(), 16);
        let err = read_response(&mut reader).await.unwrap_err();
        assert!(matches!(err, HttpError::Protocol(_)), "{err}");
    }

    /// END-TO-END over a real loopback socket: a stub HTTP server replies with a
    /// Content-Length body; the client connects, GETs, and reads it back.
    #[tokio::test]
    async fn get_against_stub_server_content_length() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut sink = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut sink)
                .await
                .unwrap();
            let body = "{\"status\":\"success\"}";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });
        let url = format!("http://{addr}/api/v1/query_range?query=x");
        let resp = get(&url, Duration::from_secs(2), Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body_string(), "{\"status\":\"success\"}");
        server.abort();
    }

    /// END-TO-END chunked over a real socket.
    #[tokio::test]
    async fn get_against_stub_server_chunked() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut sink = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut sink)
                .await
                .unwrap();
            let resp = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
                        7\r\n{\"a\":1}\r\n0\r\n\r\n";
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });
        let url = format!("http://{addr}/api");
        let resp = get(&url, Duration::from_secs(2), Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body_string(), "{\"a\":1}");
        server.abort();
    }

    /// A non-2xx status is returned (the body is the error JSON); the client does
    /// NOT treat a 4xx/5xx as a transport error (the history layer interprets it).
    #[tokio::test]
    async fn get_returns_non_2xx_status_and_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut sink = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut sink)
                .await
                .unwrap();
            let body = "{\"status\":\"error\",\"error\":\"bad\"}";
            let resp = format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });
        let url = format!("http://{addr}/api");
        let resp = get(&url, Duration::from_secs(2), Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(resp.status, 400);
        assert!(resp.body_string().contains("error"));
        server.abort();
    }

    /// THE HANG GUARD: a server that accepts the connection but NEVER replies must
    /// surface a Timeout PROMPTLY (via the read_timeout), not hang. A tight outer
    /// guard (the sanctioned runtime timer) proves promptness without a real clock.
    #[tokio::test]
    async fn get_times_out_when_server_never_replies() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            // Drain the request so the client's write completes, then stall.
            let mut sink = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut sink).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(sock);
        });
        let url = format!("http://{addr}/api");
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            get(&url, Duration::from_secs(2), Duration::from_millis(200)),
        )
        .await;
        assert!(
            result.is_ok(),
            "get must return via its own read timeout, well within the 1s guard (not hang)"
        );
        let inner = result.unwrap();
        assert!(
            matches!(inner, Err(HttpError::Timeout(_))),
            "a never-replying server must yield HttpError::Timeout, got {inner:?}"
        );
        server.abort();
    }

    /// A refused connection (nothing listening) is a Connect (or Timeout) error,
    /// promptly.
    #[tokio::test]
    async fn get_refused_connection_is_connect_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let url = format!("http://{addr}/api");
        let result = get(&url, Duration::from_secs(2), Duration::from_secs(2)).await;
        assert!(
            matches!(
                result,
                Err(HttpError::Connect { .. } | HttpError::Timeout(_))
            ),
            "a refused dial must be a Connect (or Timeout) error, got {result:?}"
        );
    }

    /// THE SIZE CAP over a real socket: a Content-Length-framed body larger than
    /// the cap is rejected with TooLarge (declared length exceeds the cap, caught
    /// at header parse before reading the body).
    #[tokio::test]
    async fn get_rejects_oversize_response_via_content_length() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut sink = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut sink)
                .await
                .unwrap();
            // Declare a body far over the cap (we never send it; the client must
            // reject at header-parse time).
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_RESPONSE_BYTES + 1
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.flush().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let url = format!("http://{addr}/api");
        let result = get(&url, Duration::from_secs(2), Duration::from_secs(2)).await;
        assert!(
            matches!(result, Err(HttpError::TooLarge(_))),
            "an oversize response must be rejected with TooLarge, got {result:?}"
        );
        server.abort();
    }

    /// THE SIZE CAP for a close-delimited (unbounded) body that streams past the
    /// cap: the accumulating reader must stop with TooLarge rather than OOM. Uses
    /// the in-memory reader with a body just over the cap.
    #[tokio::test]
    async fn read_response_close_delimited_over_cap_is_too_large() {
        let mut raw = b"HTTP/1.0 200 OK\r\n\r\n".to_vec();
        raw.resize(raw.len() + MAX_RESPONSE_BYTES + 1024, b'x');
        let mut reader = ChunkedReader::new(&raw, 64 * 1024);
        let err = read_response(&mut reader).await.unwrap_err();
        assert!(matches!(err, HttpError::TooLarge(_)), "{err}");
    }

    // ---- SSRF defense-in-depth (#369) ----

    /// SSRF: `parse_head` reports a 3xx status and surfaces the `Location` value
    /// (so the caller can refuse to follow it) without trying to follow it itself.
    #[test]
    fn parse_head_reports_redirect_status_and_location() {
        let head =
            b"HTTP/1.1 302 Found\r\nLocation: http://evil.example/\r\nContent-Length: 0\r\n\r\n";
        let (status, _framing, location) = parse_head(head).unwrap();
        assert_eq!(status, 302);
        assert_eq!(location.as_deref(), Some("http://evil.example/"));
    }

    /// SSRF: a 3xx response is turned into a typed `RedirectNotFollowed` error by
    /// `read_response`; the body is NOT consumed and no onward connection happens.
    #[tokio::test]
    async fn read_response_does_not_follow_a_redirect() {
        let raw = b"HTTP/1.1 302 Found\r\nLocation: http://169.254.169.254/latest/meta-data/\r\n\
                    Content-Length: 0\r\n\r\n";
        let mut reader = ChunkedReader::new(raw, 7);
        let err = read_response(&mut reader).await.unwrap_err();
        match err {
            HttpError::RedirectNotFollowed { status, location } => {
                assert_eq!(status, 302);
                assert_eq!(location, "http://169.254.169.254/latest/meta-data/");
            }
            other => panic!("expected RedirectNotFollowed, got {other:?}"),
        }
    }

    /// SSRF END-TO-END: a stub server that answers 302 (an SSRF pivot attempt) must
    /// NOT cause the client to connect onward. `get` returns RedirectNotFollowed.
    #[tokio::test]
    async fn get_does_not_follow_redirect_over_tcp() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut sink = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut sink)
                .await
                .unwrap();
            // A redirect to the cloud-metadata IP: the classic SSRF pivot. The
            // client must surface it, never chase it.
            let resp = "HTTP/1.1 302 Found\r\nLocation: http://169.254.169.254/latest/meta-data/iam/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });
        let url = format!("http://{addr}/api/v1/query_range?query=x");
        let result = get(&url, Duration::from_secs(2), Duration::from_secs(2)).await;
        assert!(
            matches!(
                result,
                Err(HttpError::RedirectNotFollowed { status: 302, .. })
            ),
            "a 302 must not be followed; expected RedirectNotFollowed, got {result:?}"
        );
        server.abort();
    }

    /// SSRF: the link-local / metadata block screens the PARSED ip. The cloud
    /// metadata IP and the wider 169.254.0.0/16 are blocked; fe80::/10 is blocked;
    /// the IPv4-mapped form of a link-local v4 address is blocked; normal private
    /// ranges and public addresses are NOT blocked.
    #[test]
    fn is_blocked_ip_blocks_link_local_and_metadata_only() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        // Blocked: the metadata IP and the rest of 169.254.0.0/16.
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254))));
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 0, 1))));
        // Blocked: fe80::/10 link-local v6 (boundary segments).
        assert!(is_blocked_ip(IpAddr::V6(
            "fe80::1".parse::<Ipv6Addr>().unwrap()
        )));
        assert!(is_blocked_ip(IpAddr::V6(
            "febf::1".parse::<Ipv6Addr>().unwrap()
        )));
        // Blocked: IPv4-mapped form of the metadata IP (::ffff:169.254.169.254).
        assert!(is_blocked_ip(IpAddr::V6(
            "::ffff:169.254.169.254".parse::<Ipv6Addr>().unwrap()
        )));
        // NOT blocked: RFC 1918 private ranges (the in-VPC Prometheus).
        assert!(!is_blocked_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))));
        assert!(!is_blocked_ip(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
        assert!(!is_blocked_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));
        // NOT blocked: loopback and a public address.
        assert!(!is_blocked_ip(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(!is_blocked_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        // NOT blocked: an fec0::/10 site-local-ish or a public v6 (just outside fe80::/10).
        assert!(!is_blocked_ip(IpAddr::V6(
            "fe00::1".parse::<Ipv6Addr>().unwrap()
        )));
        assert!(!is_blocked_ip(IpAddr::V6(
            "2606:4700::1".parse::<Ipv6Addr>().unwrap()
        )));
    }

    /// SSRF END-TO-END: a request to the cloud-metadata IP literal is rejected with
    /// BlockedAddress BEFORE any socket is opened (the screen runs on the resolved
    /// ip in `dial`, so no connection is attempted to 169.254.169.254).
    #[tokio::test]
    async fn get_blocks_metadata_ip_literal() {
        let result = get(
            "http://169.254.169.254/latest/meta-data/",
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .await;
        assert!(
            matches!(result, Err(HttpError::BlockedAddress(_))),
            "the metadata IP must be blocked, got {result:?}"
        );
    }

    /// SSRF: the IPv4-mapped-IPv6 form of the metadata IP is ALSO blocked at dial
    /// time (the screen unwraps `::ffff:a.b.c.d` to the v4 address before the
    /// check), so the mapped form cannot bypass the block.
    #[tokio::test]
    async fn get_blocks_ipv4_mapped_metadata_literal() {
        let result = get(
            "http://[::ffff:169.254.169.254]/latest/meta-data/",
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .await;
        assert!(
            matches!(result, Err(HttpError::BlockedAddress(_))),
            "the IPv4-mapped metadata IP must be blocked, got {result:?}"
        );
    }

    /// A loopback target is NOT blocked: `dial` screens only link-local/metadata,
    /// so a normal (here, refused) loopback dial proceeds to the connect attempt.
    #[tokio::test]
    async fn get_does_not_block_loopback() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let url = format!("http://{addr}/api");
        let result = get(&url, Duration::from_secs(2), Duration::from_secs(2)).await;
        // It reaches the connect attempt (refused), proving the screen did not
        // wrongly block a non-link-local address.
        assert!(
            matches!(
                result,
                Err(HttpError::Connect { .. } | HttpError::Timeout(_))
            ),
            "loopback must not be blocked; expected a Connect/Timeout, got {result:?}"
        );
    }
}
