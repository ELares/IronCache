// SPDX-License-Identifier: MIT OR Apache-2.0
//! Inter-node transport (HA-1, the first slice of the full-HA clustering build).
//!
//! The shipped sharded cluster (slices 1-3) routes clients across nodes but the
//! server itself has no way to talk to a peer: the [`ironcache_runtime::Runtime`]
//! seam was inbound-only (accept/recv/send). The control plane (CONTROL_PLANE.md
//! #73), replication (REPLICATION.md #77), and online migration (MIGRATION.md #75)
//! all need a node to act as a client to its peers. This crate is that substrate:
//! an outbound RESP connection ([`PeerConn`]) built ENTIRELY on the
//! [`ironcache_runtime::Runtime`] seam (via the new [`Runtime::connect`]), so it
//! runs over the production tokio backend today and over a deterministic-simulation
//! `Runtime` (TESTING.md) for replayable multi-node tests later.
//!
//! Scope is deliberately minimal: a connection, a one-command request/reply, and a
//! small RESP reply decoder covering the reply kinds the control plane probes use
//! (simple string, error, integer, bulk string). It is NOT a full client codec; the
//! richer cluster-bus framing and timeouts arrive with the consumers that need them.
//!
//! [`Runtime::connect`]: ironcache_runtime::Runtime::connect

use ironcache_runtime::Runtime;
use std::net::{SocketAddr, ToSocketAddrs};

/// A peer's ADVERTISED dial address held as `host` + `port`, NOT a pre-resolved
/// [`SocketAddr`] (k8s StatefulSet support).
///
/// A Kubernetes StatefulSet addresses its pods by STABLE per-pod DNS names (e.g.
/// `ironcache-0.ironcache.default.svc.cluster.local`), and a restarted pod keeps that
/// hostname but gets a NEW pod IP. Storing a resolved `SocketAddr` would freeze the
/// FIRST IP and dial a dead address forever after a restart; storing the host + port and
/// calling [`resolve`](PeerEndpoint::resolve) FRESH on every connect attempt picks up the
/// new IP on reconnect. An IP-literal host resolves to itself, so an IP-addressed cluster
/// is byte-identical (the resolver returns the same `SocketAddr` `parse` would have).
///
/// The host string is whatever the topology advertised: a DNS name OR an IP literal. The
/// resolution is REAL I/O (a name lookup), so it lives here in the dial/bus adapter, never
/// in the pure deterministic engine (ADR-0027): the engine only ever sees an already-dialed
/// connection, never a hostname.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEndpoint {
    /// The advertised host: a DNS hostname OR an IP literal (resolved fresh per dial).
    pub host: String,
    /// The advertised TCP port.
    pub port: u16,
}

impl PeerEndpoint {
    /// Build an endpoint from an advertised `host` (DNS name or IP literal) and `port`.
    #[must_use]
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Resolve this endpoint to a [`SocketAddr`], accepting BOTH a DNS hostname AND an IP
    /// literal, and return the FIRST resolved address.
    ///
    /// This is the single replacement for the old `format!("{host}:{port}").parse::<SocketAddr>()`,
    /// which ONLY accepted an IP literal and SILENTLY dropped a DNS hostname. `(host, port)` fed to
    /// [`ToSocketAddrs`] resolves a hostname through the OS resolver AND resolves an IP literal
    /// trivially to itself (so an IP-addressed cluster is byte-identical). The FIRST address is taken
    /// for a stable, deterministic choice (the resolver yields a consistent order for a given host);
    /// a dual-stack host therefore dials its first advertised family, and the next dial re-resolves.
    ///
    /// # Errors
    ///
    /// Returns a [`ResolveError`] (naming the unresolvable `host:port`) when the OS resolver yields
    /// NO address for the host, or the lookup itself fails. The caller MUST surface this loudly
    /// (a boot error or a logged dial failure) and NEVER silently drop the peer: a dropped voter
    /// breaks quorum with no diagnostic.
    pub fn resolve(&self) -> Result<SocketAddr, ResolveError> {
        // `(&str, u16)` implements ToSocketAddrs: an IP literal resolves to itself (byte-identical
        // to the old parse), a DNS name goes through the OS resolver. Real I/O, so it belongs in
        // this adapter, not the engine.
        match (self.host.as_str(), self.port).to_socket_addrs() {
            Ok(mut addrs) => addrs.next().ok_or_else(|| ResolveError {
                host: self.host.clone(),
                port: self.port,
                cause: "the resolver returned no addresses".to_owned(),
            }),
            Err(e) => Err(ResolveError {
                host: self.host.clone(),
                port: self.port,
                cause: e.to_string(),
            }),
        }
    }
}

/// A peer-address RESOLUTION failure: the advertised `host:port` could not be turned into a
/// dial-able [`SocketAddr`] (k8s StatefulSet support).
///
/// Carries the exact `host` + `port` and the underlying cause so the boot path / the dial path can
/// surface a CLEAR, actionable diagnostic instead of the old SILENT `continue` that quietly omitted
/// a voter (which breaks quorum with no error). Never swallow this: log it loudly or hard-fail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveError {
    /// The host that failed to resolve (a DNS name or a malformed literal).
    pub host: String,
    /// The port that was being resolved alongside the host.
    pub port: u16,
    /// The underlying resolver cause (the OS error text, or "no addresses").
    pub cause: String,
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to resolve peer address {}:{} ({})",
            self.host, self.port, self.cause
        )
    }
}

impl std::error::Error for ResolveError {}

/// A RESP reply the bus understands. Enough for control-plane probes; the full
/// RESP3 surface is the client codec's job, not the bus's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// `+OK\r\n` style simple string (the text after `+`).
    Simple(String),
    /// `-ERR ...\r\n` style error (the text after `-`).
    Error(String),
    /// `:123\r\n` style integer.
    Integer(i64),
    /// `$len\r\n<bytes>\r\n` bulk string; `None` is the null bulk `$-1\r\n`.
    Bulk(Option<Vec<u8>>),
}

/// A bus failure, generic over the runtime's own I/O error type.
#[derive(Debug)]
pub enum BusError<E> {
    /// Underlying runtime I/O error (connect/recv/send).
    Io(E),
    /// The peer closed the connection before a full reply arrived.
    Eof,
    /// The bytes on the wire were not a reply kind the bus decodes.
    Protocol(&'static str),
    /// The peer answered with a RESP error reply (`-...`).
    Remote(String),
    /// The peer's advertised `host:port` could not be RESOLVED to a dial-able address (k8s
    /// StatefulSet support): a DNS name with no record, or a not-yet-up pod. Surfaced loudly by
    /// the dial path (logged + reconnect) instead of the old silent peer drop.
    Resolve(ResolveError),
}

/// An outbound RESP connection to a peer node, built on the [`Runtime`] seam.
///
/// Generic over the runtime so it runs over the production tokio backend and over
/// a simulated runtime for deterministic multi-node tests. The buffer bound
/// (`From`/`Into<Vec<u8>>`) is satisfied trivially by the tokio backend's `Vec<u8>`
/// buffer and lets a future simulated buffer participate too.
pub struct PeerConn<R: Runtime> {
    stream: R::Stream,
    /// Bytes received but not yet consumed by a parsed reply.
    pending: Vec<u8>,
}

impl<R> PeerConn<R>
where
    R: Runtime,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
{
    /// Open a connection to `addr` over the runtime seam.
    pub async fn connect(rt: &R, addr: SocketAddr) -> Result<Self, BusError<R::Error>> {
        let stream = rt.connect(addr).await.map_err(BusError::Io)?;
        Ok(Self {
            stream,
            pending: Vec::new(),
        })
    }

    /// Open a connection to a peer by its advertised [`PeerEndpoint`] (host + port), RESOLVING the
    /// host FRESH on this call before dialing (k8s StatefulSet support).
    ///
    /// This is the reconnect-safe entry point: because the endpoint holds the HOSTNAME (not a
    /// pre-resolved [`SocketAddr`]), every call re-runs DNS, so a restarted pod that kept its
    /// hostname but got a NEW IP is dialed at its new address. A resolution failure is returned as a
    /// [`BusError::Resolve`] (carrying the host:port), NEVER silently dropped. An IP-literal host
    /// resolves to itself, so an IP-addressed peer dials byte-identically to [`PeerConn::connect`].
    pub async fn connect_endpoint(
        rt: &R,
        endpoint: &PeerEndpoint,
    ) -> Result<Self, BusError<R::Error>> {
        let addr = endpoint.resolve().map_err(BusError::Resolve)?;
        Self::connect(rt, addr).await
    }

    /// Send one command (an array of bulk-string args) and read exactly one reply.
    ///
    /// The request is RESP-encoded and written through the seam's owned-buffer
    /// `send`; the reply is read by appending `recv` chunks until one full reply
    /// decodes from the pending buffer.
    pub async fn request(&mut self, rt: &R, args: &[&[u8]]) -> Result<Reply, BusError<R::Error>> {
        let encoded: R::Buf = encode_command(args).into();
        let _ = rt
            .send(&mut self.stream, encoded)
            .await
            .map_err(BusError::Io)?;
        loop {
            if let Some((reply, consumed)) =
                parse_reply(&self.pending).map_err(BusError::Protocol)?
            {
                self.pending.drain(..consumed);
                return Ok(reply);
            }
            // Need more bytes: hand the pending buffer to recv (it appends) and take
            // it back grown. For the tokio backend the From/Into is identity (no copy).
            let taken: R::Buf = core::mem::take(&mut self.pending).into();
            let res = rt
                .recv(&mut self.stream, taken)
                .await
                .map_err(BusError::Io)?;
            if res.n == 0 {
                return Err(BusError::Eof);
            }
            self.pending = res.buf.into();
        }
    }
}

/// Connect to a peer and return its cluster node id (via `CLUSTER MYID`).
///
/// The first real consumer of the inter-node transport: it proves a node can reach
/// a peer's RESP port end to end. The control plane (HA-3+) builds its handshake on
/// this same path.
pub async fn peer_node_id<R>(rt: &R, addr: SocketAddr) -> Result<String, BusError<R::Error>>
where
    R: Runtime,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
{
    let mut conn = PeerConn::connect(rt, addr).await?;
    match conn.request(rt, &[b"CLUSTER", b"MYID"]).await? {
        Reply::Bulk(Some(bytes)) => {
            String::from_utf8(bytes).map_err(|_| BusError::Protocol("node id is not utf-8"))
        }
        Reply::Simple(s) => Ok(s),
        Reply::Error(e) => Err(BusError::Remote(e)),
        _ => Err(BusError::Protocol("unexpected CLUSTER MYID reply kind")),
    }
}

/// Connect to a peer and `PING` it, returning `true` on `+PONG`.
pub async fn peer_ping<R>(rt: &R, addr: SocketAddr) -> Result<bool, BusError<R::Error>>
where
    R: Runtime,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
{
    let mut conn = PeerConn::connect(rt, addr).await?;
    match conn.request(rt, &[b"PING"]).await? {
        Reply::Simple(s) => Ok(s.eq_ignore_ascii_case("PONG")),
        Reply::Error(e) => Err(BusError::Remote(e)),
        _ => Err(BusError::Protocol("unexpected PING reply kind")),
    }
}

/// RESP-encode a command as an array of bulk strings (`*N\r\n$len\r\narg\r\n...`).
fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Try to decode one reply from `buf`.
///
/// Returns `Ok(Some((reply, consumed)))` when a full reply is present (with the
/// number of bytes it occupied), `Ok(None)` when more bytes are needed, and `Err`
/// for a malformed or unsupported reply.
fn parse_reply(buf: &[u8]) -> Result<Option<(Reply, usize)>, &'static str> {
    let Some(&kind) = buf.first() else {
        return Ok(None);
    };
    let Some(line_end) = find_crlf(buf) else {
        return Ok(None);
    };
    let line = &buf[1..line_end];
    let after = line_end + 2;
    match kind {
        b'+' => Ok(Some((
            Reply::Simple(String::from_utf8_lossy(line).into_owned()),
            after,
        ))),
        b'-' => Ok(Some((
            Reply::Error(String::from_utf8_lossy(line).into_owned()),
            after,
        ))),
        b':' => {
            let n = parse_i64(line).ok_or("malformed integer reply")?;
            Ok(Some((Reply::Integer(n), after)))
        }
        b'$' => {
            let len = parse_i64(line).ok_or("malformed bulk length")?;
            if len < 0 {
                return Ok(Some((Reply::Bulk(None), after)));
            }
            let len = usize::try_from(len).map_err(|_| "bulk length out of range")?;
            let need = after + len + 2;
            if buf.len() < need {
                return Ok(None);
            }
            if &buf[after + len..need] != b"\r\n" {
                return Err("bulk body not CRLF-terminated");
            }
            Ok(Some((
                Reply::Bulk(Some(buf[after..after + len].to_vec())),
                need,
            )))
        }
        _ => Err("unsupported reply kind"),
    }
}

/// Index of the first `\r\n` in `buf`, if present.
fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Parse an ASCII signed integer line, rejecting anything non-numeric.
fn parse_i64(line: &[u8]) -> Option<i64> {
    core::str::from_utf8(line).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_is_resp_array_of_bulk_strings() {
        assert_eq!(
            encode_command(&[b"CLUSTER", b"MYID"]),
            b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n"
        );
        assert_eq!(encode_command(&[b"PING"]), b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn parse_simple_string() {
        assert_eq!(
            parse_reply(b"+PONG\r\n").unwrap(),
            Some((Reply::Simple("PONG".to_owned()), 7))
        );
    }

    #[test]
    fn parse_error() {
        assert_eq!(
            parse_reply(b"-ERR nope\r\n").unwrap(),
            Some((Reply::Error("ERR nope".to_owned()), 11))
        );
    }

    #[test]
    fn parse_integer() {
        assert_eq!(
            parse_reply(b":42\r\n").unwrap(),
            Some((Reply::Integer(42), 5))
        );
    }

    #[test]
    fn parse_bulk_string_and_null() {
        let id = b"$40\r\n1111111111111111111111111111111111111111\r\n";
        let (reply, consumed) = parse_reply(id).unwrap().unwrap();
        assert_eq!(consumed, id.len());
        assert_eq!(
            reply,
            Reply::Bulk(Some(b"1111111111111111111111111111111111111111".to_vec()))
        );
        assert_eq!(
            parse_reply(b"$-1\r\n").unwrap(),
            Some((Reply::Bulk(None), 5))
        );
    }

    #[test]
    fn partial_replies_need_more_bytes() {
        assert_eq!(parse_reply(b"").unwrap(), None);
        assert_eq!(parse_reply(b"+PO").unwrap(), None);
        // Bulk header present but body not yet fully arrived.
        assert_eq!(parse_reply(b"$4\r\nPO").unwrap(), None);
    }

    #[test]
    fn unsupported_kind_is_rejected() {
        assert!(parse_reply(b"*2\r\n").is_err());
    }

    /// An IP-LITERAL endpoint resolves to itself, byte-identical to the old
    /// `format!("{host}:{port}").parse::<SocketAddr>()` (so an IP-addressed cluster is unchanged).
    #[test]
    fn resolve_accepts_ip_literal() {
        let ep = PeerEndpoint::new("127.0.0.1", 7001);
        let addr = ep.resolve().expect("an IP literal must resolve to itself");
        assert_eq!(addr, "127.0.0.1:7001".parse::<SocketAddr>().unwrap());
        assert_eq!(addr.port(), 7001);
        assert!(addr.ip().is_loopback());
    }

    /// A resolvable DNS HOSTNAME (`localhost`) resolves to a loopback `SocketAddr` (127.0.0.1 or
    /// ::1). This is the case the old IP-only `parse` SILENTLY dropped, so a StatefulSet (whose pods
    /// are addressed by per-pod DNS names) could never form a cluster.
    #[test]
    fn resolve_accepts_dns_hostname() {
        let ep = PeerEndpoint::new("localhost", 7001);
        let addr = ep
            .resolve()
            .expect("localhost must resolve to a loopback address");
        assert_eq!(addr.port(), 7001);
        assert!(
            addr.ip().is_loopback(),
            "localhost should resolve to a loopback ip, got {addr}"
        );
    }

    /// An UNRESOLVABLE host yields a CLEAR `Err` (not a silent `None` / dropped peer). The error
    /// names the offending host:port so the boot / dial path can surface an actionable diagnostic.
    #[test]
    fn resolve_unresolvable_host_is_a_clear_error() {
        // `.invalid` is reserved by RFC 6761 to never resolve, so this is hermetic (no real lookup
        // can succeed) and deterministic across environments.
        let ep = PeerEndpoint::new("nonexistent.invalid", 7001);
        let err = ep
            .resolve()
            .expect_err("an unresolvable host must be a clear error, not a silent drop");
        let msg = err.to_string();
        assert!(msg.contains("nonexistent.invalid"), "got {msg:?}");
        assert!(msg.contains("7001"), "got {msg:?}");
        assert!(msg.contains("failed to resolve"), "got {msg:?}");
    }
}
