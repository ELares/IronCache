// SPDX-License-Identifier: MIT OR Apache-2.0
//! An async RESP client the console uses to talk to ONE IronCache node (#355).
//!
//! The console is read-only against a node: it connects, optionally `AUTH`s as a
//! least-privilege ACL user, and issues admin commands (`PING`, `INFO`). This
//! client is deliberately small (it reuses the [`crate::resp`] parser and the
//! `ironcache-bench`-style request framing, reimplemented here) and HARD bounded:
//!
//! * the connect (TCP, and the TLS handshake when enabled) is wrapped in a
//!   `connect_timeout`, and
//! * EVERY operation (write the command + read one complete reply) is wrapped in
//!   an `op_timeout`.
//!
//! The read bound is load-bearing: a node that accepts a command but never
//! replies must surface a [`NodeError::Timeout`] PROMPTLY, never hang the poll
//! loop (a missing read timeout previously caused a production hang). The only
//! `tokio::time` use here is that bound, which the determinism invariant lint
//! explicitly allows (it is the runtime timer seam, not a clock read).
//!
//! ## TLS
//!
//! When `node_tls` is set the connection is wrapped with the runtime crate's
//! cluster TLS client ([`ironcache_runtime::tls::build_cluster_client_config`] +
//! [`ironcache_runtime::tls::connect_tls`]). That dialer presents a FIXED SNI
//! (`ironcache-cluster`). By DEFAULT it VERIFIES the peer against the configured
//! CA ([`NodeTls::ca_path`]); verification is only skipped when the operator
//! EXPLICITLY sets [`NodeTls::insecure_skip_verify`] (config
//! `node_tls_insecure_skip_verify`), in which case the link is encrypted but
//! UNAUTHENTICATED (an active MITM could impersonate a node and capture the AUTH
//! credential) and a loud warning is emitted at dial time. With TLS on and no CA
//! and no explicit opt-out the dial is REFUSED (enforced both in config
//! validation and by the runtime builder), so accept-any is NEVER installed
//! silently. The dialer does NOT yet support an arbitrary per-host SNI or mTLS;
//! full SNI / mTLS for the console-to-node link is deferred to #369, and the
//! PLAINTEXT path is the fully-supported v1 path.
//!
//! ## Determinism (ADR-0003)
//!
//! No clock and no RNG: the client is pure I/O plus the runtime timer bound. The
//! caller stamps freshness through `ironcache-env`.

use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use zeroize::Zeroizing;

use ironcache_runtime::tls::{SecureStream, build_cluster_client_config, connect_tls};

use crate::resp::{self, ParseError, RespValue};

/// Max bytes buffered while reading ONE reply, before the client gives up with a
/// protocol error. The console's admin replies (`+OK`, an `INFO` dump) are small;
/// this caps a misbehaving / malicious peer's reply at a few MiB so it cannot
/// drive an unbounded allocation. INFO is well under this even on a large node.
const MAX_REPLY_BYTES: usize = 8 * 1024 * 1024;

/// A typed error talking to a node. Distinct variants so the poller can label a
/// snapshot precisely (and so a connect/timeout reads differently from a real
/// auth rejection). The password is NEVER included in any variant.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The TCP connect (or TLS handshake) failed.
    #[error("connecting to node {addr}: {source}")]
    Connect {
        /// The node address dialed.
        addr: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// An operation (connect, write, or read) exceeded its timeout bound.
    #[error("node operation timed out after {0:?}")]
    Timeout(Duration),
    /// `AUTH` was rejected by the node (wrong user/password, or auth required and
    /// none configured). The node's error text is included; it never echoes the
    /// password.
    #[error("node AUTH failed: {0}")]
    Auth(String),
    /// The configured TLS material could not be loaded / built.
    #[error("node TLS configuration: {0}")]
    Tls(String),
    /// A general I/O error reading from / writing to the node.
    #[error("node I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The node sent a reply the client could not parse as RESP2.
    #[error("node protocol error: {0}")]
    Protocol(String),
    /// A command returned a RESP error reply (the node's error text).
    #[error("node returned an error: {0}")]
    Command(String),
}

impl From<ParseError> for NodeError {
    fn from(e: ParseError) -> Self {
        NodeError::Protocol(e.to_string())
    }
}

/// How the console authenticates to a node. Built from the resolved config at
/// connect time; the password is read from its file here, not held in config.
///
/// The password is held in a [`Zeroizing`] buffer so it is scrubbed from memory
/// on drop (project convention #145), and the manual [`std::fmt::Debug`] impl
/// REDACTS it: the secret is never logged, formatted, or placed in an error.
#[derive(Clone)]
pub struct NodeAuth {
    /// The ACL user (`AUTH <user> <pass>`), or `None` for the default user
    /// (`AUTH <pass>`).
    pub user: Option<String>,
    /// The password bytes (read from `node_password_file`), zeroized on drop.
    pub password: Zeroizing<Vec<u8>>,
}

impl std::fmt::Debug for NodeAuth {
    /// Never print the password: a `Debug` that derived it could leak the secret
    /// into a log line or an error chain. The redaction is deliberate.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeAuth")
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// TLS settings for the node dial. `None` means plaintext (the default).
#[derive(Debug, Clone)]
pub struct NodeTls {
    /// CA bundle (PEM) path to verify the node cert against, if any. With a CA the
    /// peer is authenticated; supply a CA for a verified link.
    pub ca_path: Option<String>,
    /// EXPLICITLY accept any node certificate without verifying it. Default false:
    /// the dial verifies against [`Self::ca_path`], and with no CA and this false
    /// the dial is REFUSED. Only when this is true is an accept-any verifier
    /// installed (encrypted but unauthenticated, MITM-exposed), with a loud
    /// warning. Never inferred from `ca_path.is_none()`.
    pub insecure_skip_verify: bool,
}

/// The transport under the client: plaintext TCP or a TLS-wrapped stream.
#[derive(Debug)]
enum Transport {
    Plain(TcpStream),
    Tls(Box<SecureStream>),
}

impl Transport {
    /// Write all of `bytes` to the transport.
    async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Transport::Plain(s) => s.write_all(bytes).await,
            // SecureStream::send takes an owned buffer and returns it; we discard.
            Transport::Tls(s) => s.send(bytes.to_vec()).await.map(|_| ()),
        }
    }

    /// Read some bytes into `buf` (appended), returning the count (0 = peer
    /// closed). The read goes straight into the grown tail of `buf` (no large
    /// stack chunk), keeping the surrounding future small.
    async fn read_some(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        match self {
            Transport::Plain(s) => {
                let start = buf.len();
                let want = 16 * 1024;
                buf.resize(start + want, 0);
                let n = s.read(&mut buf[start..]).await?;
                buf.truncate(start + n);
                Ok(n)
            }
            Transport::Tls(s) => {
                // SecureStream::recv appends into the owned buffer and reports n.
                let taken = std::mem::take(buf);
                let res = s.recv(taken).await?;
                *buf = res.buf;
                Ok(res.n)
            }
        }
    }
}

/// The connection parameters for opening an ON-DEMAND [`NodeClient`] to the seed
/// node (issue #361). The poll loop opens a fresh connection per tick and drops
/// it; the management layer needs the same ability to open a SHORT-LIVED
/// connection per request (run the command, then drop), so this captures the
/// addr, the resolved TLS, the resolved AUTH (with the zeroized password), and the
/// connect/op timeout bounds in one cloneable unit shared into the HTTP state.
///
/// SECURITY: the password is held in [`NodeAuth`]'s zeroized buffer and is never
/// logged (the manual `Debug` redacts it); a clone keeps that redaction.
#[derive(Debug, Clone)]
pub struct NodeAccess {
    /// The seed node address (`host:port`) to connect to.
    pub addr: String,
    /// The resolved TLS settings, or `None` for plaintext.
    pub tls: Option<NodeTls>,
    /// The resolved AUTH (user + zeroized password), or `None` for no auth.
    pub auth: Option<NodeAuth>,
    /// The TCP/TLS connect timeout.
    pub connect_timeout: Duration,
    /// The per-operation (write + read one reply) timeout.
    pub op_timeout: Duration,
}

impl NodeAccess {
    /// Open a fresh, AUTH'd [`NodeClient`] to the seed node with the captured
    /// bounds. The caller runs one (or a few bounded) commands then drops it; a
    /// short-lived connection per management request keeps the model simple and
    /// avoids sharing a single mutable client across concurrent requests.
    ///
    /// # Errors
    ///
    /// Propagates [`NodeError`] from [`NodeClient::connect`] (connect/timeout/auth).
    pub async fn connect(&self) -> Result<NodeClient, NodeError> {
        NodeClient::connect(
            &self.addr,
            self.tls.as_ref(),
            self.auth.as_ref(),
            self.connect_timeout,
            self.op_timeout,
        )
        .await
    }
}

/// An async RESP connection to one node, with per-operation timeout bounds.
#[derive(Debug)]
pub struct NodeClient {
    transport: Transport,
    /// Bytes read from the socket but not yet consumed by a completed reply.
    buf: Vec<u8>,
    /// The per-operation bound applied to every write+read.
    op_timeout: Duration,
    /// The node address, for error messages.
    addr: String,
}

impl NodeClient {
    /// Connect to `addr` (`host:port`), set `TCP_NODELAY`, optionally wrap TLS,
    /// and (when `auth` is given) `AUTH`. The whole connect (TCP + TLS handshake)
    /// is bounded by `connect_timeout`; once connected, every operation is bounded
    /// by `op_timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::Connect`] on a failed dial, [`NodeError::Timeout`] if
    /// the connect exceeds `connect_timeout`, [`NodeError::Tls`] on bad TLS
    /// material, or [`NodeError::Auth`] if `AUTH` is rejected.
    pub async fn connect(
        addr: &str,
        tls: Option<&NodeTls>,
        auth: Option<&NodeAuth>,
        connect_timeout: Duration,
        op_timeout: Duration,
    ) -> Result<Self, NodeError> {
        let transport = tokio::time::timeout(connect_timeout, dial(addr, tls))
            .await
            .map_err(|_| NodeError::Timeout(connect_timeout))??;
        let mut client = NodeClient {
            transport,
            buf: Vec::with_capacity(4096),
            op_timeout,
            addr: addr.to_owned(),
        };
        if let Some(auth) = auth {
            client.authenticate(auth).await?;
        }
        Ok(client)
    }

    /// Send `AUTH` and require `+OK`. A RESP error reply is mapped to
    /// [`NodeError::Auth`]. The password is sent but never logged or returned.
    async fn authenticate(&mut self, auth: &NodeAuth) -> Result<(), NodeError> {
        let reply = if let Some(user) = &auth.user {
            self.command(&[b"AUTH", user.as_bytes(), &auth.password])
                .await
        } else {
            self.command(&[b"AUTH", &auth.password]).await
        };
        match reply {
            Ok(RespValue::Simple(_)) => Ok(()),
            Ok(RespValue::Error(e)) => {
                Err(NodeError::Auth(String::from_utf8_lossy(&e).into_owned()))
            }
            Ok(other) => Err(NodeError::Auth(format!("unexpected AUTH reply: {other:?}"))),
            // A command-level error (the node returned `-...`) during AUTH is an
            // auth failure too; a transport error stays a transport error.
            Err(NodeError::Command(e)) => Err(NodeError::Auth(e)),
            Err(e) => Err(e),
        }
    }

    /// Issue one command (args are the RESP bulk-string arguments) and read one
    /// complete reply. The ENTIRE write+read is bounded by `op_timeout`: a node
    /// that accepts the write but never replies returns [`NodeError::Timeout`]
    /// promptly rather than hanging. A RESP error reply is surfaced as
    /// [`NodeError::Command`].
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::Timeout`] on the op bound, [`NodeError::Io`] /
    /// [`NodeError::Protocol`] on transport / parse faults, or
    /// [`NodeError::Command`] on a `-ERR` reply.
    pub async fn command(&mut self, args: &[&[u8]]) -> Result<RespValue, NodeError> {
        let op_timeout = self.op_timeout;
        tokio::time::timeout(op_timeout, self.command_inner(args))
            .await
            .map_err(|_| NodeError::Timeout(op_timeout))?
    }

    /// The unbounded body of [`Self::command`]; the caller wraps it in the op
    /// timeout. Encodes the request, writes it, then reads one reply.
    async fn command_inner(&mut self, args: &[&[u8]]) -> Result<RespValue, NodeError> {
        let request = encode_command(args);
        self.transport.write_all(&request).await?;
        let reply = self.read_reply().await?;
        if let RespValue::Error(e) = &reply {
            return Err(NodeError::Command(String::from_utf8_lossy(e).into_owned()));
        }
        Ok(reply)
    }

    /// Read exactly one complete RESP reply, filling the buffer from the transport
    /// as needed. Consumed bytes are dropped from the front so a long-lived
    /// connection's buffer does not grow unbounded.
    async fn read_reply(&mut self) -> Result<RespValue, NodeError> {
        loop {
            if let Some((value, consumed)) = resp::parse_reply(&self.buf)? {
                self.buf.drain(..consumed);
                return Ok(value);
            }
            if self.buf.len() > MAX_REPLY_BYTES {
                return Err(NodeError::Protocol(format!(
                    "reply exceeded {MAX_REPLY_BYTES} bytes without completing"
                )));
            }
            let n = self.transport.read_some(&mut self.buf).await?;
            if n == 0 {
                return Err(NodeError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("node {} closed the connection mid-reply", self.addr),
                )));
            }
        }
    }

    /// `PING` and require the reply body to be `PONG` (case-insensitive; a
    /// simple-string `+PONG` or a bulk `PONG`). `OK` is also accepted (some
    /// auth/select paths answer `+OK`); any OTHER body is rejected rather than
    /// silently treated as success, so a wrong / proxied endpoint is caught.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::Protocol`] on a non-text reply or a body that is
    /// neither `PONG` nor `OK`; otherwise propagates [`NodeError`] from
    /// [`Self::command`].
    pub async fn ping(&mut self) -> Result<(), NodeError> {
        let reply = self.command(&[b"PING"]).await?;
        match reply.as_text_bytes() {
            Some(body)
                if body.eq_ignore_ascii_case(b"PONG") || body.eq_ignore_ascii_case(b"OK") =>
            {
                Ok(())
            }
            _ => Err(NodeError::Protocol(format!(
                "unexpected PING reply: {reply:?}"
            ))),
        }
    }

    /// `INFO` and return the bulk-string body as a `String` (lossy on non-UTF-8).
    ///
    /// # Errors
    ///
    /// Propagates [`NodeError`]; returns [`NodeError::Protocol`] if INFO did not
    /// reply with a bulk/simple string.
    pub async fn info(&mut self) -> Result<String, NodeError> {
        let reply = self.command(&[b"INFO"]).await?;
        match reply.as_text_bytes() {
            Some(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
            None => Err(NodeError::Protocol(format!(
                "unexpected INFO reply: {reply:?}"
            ))),
        }
    }

    /// `SLOWLOG GET <count>` and parse the reply array into [`SlowlogEntry`]
    /// values. The reply is a RESP array of 6-field entries; a non-array reply is
    /// a protocol error. Individual malformed entries are skipped (parsed
    /// defensively) rather than failing the whole call, so one odd entry does not
    /// blind the dashboard to the rest.
    ///
    /// # Errors
    ///
    /// Propagates [`NodeError`]; returns [`NodeError::Protocol`] if SLOWLOG did not
    /// reply with an array (and [`NodeError::Command`] on a `-ERR` / ACL denial).
    pub async fn slowlog(&mut self, count: u64) -> Result<Vec<SlowlogEntry>, NodeError> {
        let count = count.to_string();
        let reply = self
            .command(&[b"SLOWLOG", b"GET", count.as_bytes()])
            .await?;
        match reply {
            RespValue::Array(items) => Ok(parse_slowlog(&items)),
            other => Err(NodeError::Protocol(format!(
                "unexpected SLOWLOG reply: {other:?}"
            ))),
        }
    }

    /// `CLIENT LIST` and parse the bulk-string reply (one client per line) into
    /// [`ClientInfo`] values. A non-text reply is a protocol error; an empty body
    /// yields an empty list.
    ///
    /// # Errors
    ///
    /// Propagates [`NodeError`]; returns [`NodeError::Protocol`] if CLIENT LIST did
    /// not reply with a bulk/simple string (and [`NodeError::Command`] on a `-ERR`
    /// / ACL denial).
    pub async fn client_list(&mut self) -> Result<Vec<ClientInfo>, NodeError> {
        let reply = self.command(&[b"CLIENT", b"LIST"]).await?;
        match reply.as_text_bytes() {
            Some(bytes) => Ok(parse_client_list(&String::from_utf8_lossy(bytes))),
            None => Err(NodeError::Protocol(format!(
                "unexpected CLIENT LIST reply: {reply:?}"
            ))),
        }
    }
}

/// One entry from `SLOWLOG GET`: a command the node logged as slow. The canonical
/// Redis entry is a 6-element array `[id, timestamp, micros, [argv...],
/// client_addr, client_name]`; older servers omit the trailing client fields,
/// which are left empty here. The `argv` is decoded lossily (a key/value may be
/// non-UTF-8) and is the recon-sensitive part (it can contain key names).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SlowlogEntry {
    /// The unique, monotonically increasing entry id.
    pub id: i64,
    /// Unix time (seconds) the slow command ran.
    pub timestamp: i64,
    /// The command's execution time in microseconds.
    pub micros: i64,
    /// The command and its arguments, each decoded lossily from the bulk reply.
    pub argv: Vec<String>,
    /// The client `addr:port` (RESP-array form, server 4.0+); empty if absent.
    pub client_addr: String,
    /// The client connection name (`CLIENT SETNAME`); empty if absent / unset.
    pub client_name: String,
}

/// One client from `CLIENT LIST`: the dashboard-relevant fields plus the full raw
/// `field=value` map for anything not modeled. Every typed field is optional so a
/// server that omits one (version skew) leaves it `None` rather than defaulting.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ClientInfo {
    /// `id` (the unique client connection id).
    pub id: Option<u64>,
    /// `addr` (the client `peer:port`).
    pub addr: Option<String>,
    /// `name` (the `CLIENT SETNAME` value; empty string in the reply if unset).
    pub name: Option<String>,
    /// `age` (seconds the connection has been open).
    pub age: Option<u64>,
    /// `idle` (seconds since the last command on this connection).
    pub idle: Option<u64>,
    /// `db` (the selected database index).
    pub db: Option<u64>,
    /// `cmd` (the last command, e.g. `get` or `client|list`).
    pub cmd: Option<String>,
    /// The full `field=value` map (every parsed field), for fields not modeled.
    pub raw: std::collections::HashMap<String, String>,
}

/// Parse a `SLOWLOG GET` reply array (already extracted from the RESP frame) into
/// [`SlowlogEntry`] values. Each element should itself be a 6-field array; an
/// element with too few fields or the wrong shape is SKIPPED (defensive), never a
/// panic. The argv sub-array is decoded element by element (lossy UTF-8).
fn parse_slowlog(items: &[RespValue]) -> Vec<SlowlogEntry> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let RespValue::Array(fields) = item else {
            continue;
        };
        // Need at least id, timestamp, micros, argv. The two client fields are
        // optional (older servers omit them).
        if fields.len() < 4 {
            continue;
        }
        let (Some(id), Some(timestamp), Some(micros)) = (
            resp_int(&fields[0]),
            resp_int(&fields[1]),
            resp_int(&fields[2]),
        ) else {
            continue;
        };
        let argv = match &fields[3] {
            RespValue::Array(args) => args.iter().filter_map(resp_text).collect(),
            _ => Vec::new(),
        };
        let client_addr = fields.get(4).and_then(resp_text).unwrap_or_default();
        let client_name = fields.get(5).and_then(resp_text).unwrap_or_default();
        out.push(SlowlogEntry {
            id,
            timestamp,
            micros,
            argv,
            client_addr,
            client_name,
        });
    }
    out
}

/// Parse a `CLIENT LIST` bulk body (one client per line, space-separated
/// `field=value` pairs) into [`ClientInfo`] values. Tolerant: a blank line is
/// skipped, a token without `=` is ignored, every `field=value` is recorded in
/// `raw`, and the typed fields are read out of that map (a missing or
/// unparseable numeric field becomes `None`).
fn parse_client_list(body: &str) -> Vec<ClientInfo> {
    let mut out = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut raw: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for token in line.split(' ') {
            if let Some((k, v)) = token.split_once('=') {
                if !k.is_empty() {
                    raw.insert(k.to_owned(), v.to_owned());
                }
            }
        }
        if raw.is_empty() {
            continue;
        }
        let u = |k: &str| raw.get(k).and_then(|v| v.parse::<u64>().ok());
        let s = |k: &str| raw.get(k).map(ToOwned::to_owned);
        out.push(ClientInfo {
            id: u("id"),
            addr: s("addr"),
            name: s("name"),
            age: u("age"),
            idle: u("idle"),
            db: u("db"),
            cmd: s("cmd"),
            raw,
        });
    }
    out
}

/// Borrow an integer from a [`RespValue::Integer`], or a [`RespValue`] text body
/// that parses as an i64 (some servers return SLOWLOG numeric fields as bulk).
fn resp_int(v: &RespValue) -> Option<i64> {
    match v {
        RespValue::Integer(n) => Some(*n),
        other => other
            .as_text_bytes()
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.trim().parse().ok()),
    }
}

/// Decode a [`RespValue`] text body (simple or bulk) to a lossy `String`, or
/// `None` for a non-text value.
fn resp_text(v: &RespValue) -> Option<String> {
    v.as_text_bytes()
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

/// Read the node password from `path`, trimming a single trailing newline (the
/// common `echo secret > file` shape) but otherwise taking the file bytes
/// verbatim. The bytes are returned in a [`Zeroizing`] buffer (scrubbed on drop)
/// and are never logged.
///
/// # Errors
///
/// Returns the I/O error if the file cannot be read.
pub fn read_password_file(path: &Path) -> std::io::Result<Zeroizing<Vec<u8>>> {
    let mut bytes = Zeroizing::new(std::fs::read(path)?);
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    Ok(bytes)
}

/// Dial `addr`, set `TCP_NODELAY`, and wrap TLS if configured. Not itself bounded
/// (the caller wraps it in the connect timeout).
async fn dial(addr: &str, tls: Option<&NodeTls>) -> Result<Transport, NodeError> {
    let tcp = TcpStream::connect(addr)
        .await
        .map_err(|source| NodeError::Connect {
            addr: addr.to_owned(),
            source,
        })?;
    tcp.set_nodelay(true)?;
    match tls {
        None => Ok(Transport::Plain(tcp)),
        Some(tls) => {
            if tls.insecure_skip_verify {
                tracing::warn!(
                    addr,
                    "node TLS peer verification DISABLED (node_tls_insecure_skip_verify): the link \
                     to this node is encrypted but the certificate is NOT verified, so an active \
                     MITM could impersonate the node and capture the AUTH credential. Supply \
                     node_tls_ca instead."
                );
            }
            // Pass the EXPLICIT opt-out flag, never `ca_path.is_none()`: with no CA
            // and the flag false the runtime builder REFUSES to build an
            // accept-any dialer, so verification is required by default.
            let connector =
                build_cluster_client_config(tls.ca_path.as_deref(), tls.insecure_skip_verify)
                    .map_err(|e| NodeError::Tls(e.to_string()))?;
            // connect_tls applies its OWN handshake timeout internally and is
            // additionally inside the caller's connect-timeout bound.
            let secure =
                connect_tls(&connector, tcp)
                    .await
                    .map_err(|source| NodeError::Connect {
                        addr: addr.to_owned(),
                        source,
                    })?;
            Ok(Transport::Tls(Box::new(secure)))
        }
    }
}

/// Encode `args` as a RESP array of bulk strings:
/// `["GET","k"]` -> `*2\r\n$3\r\nGET\r\n$1\r\nk\r\n`.
fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + args.iter().map(|a| a.len() + 16).sum::<usize>());
    out.extend_from_slice(b"*");
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for a in args {
        out.extend_from_slice(b"$");
        out.extend_from_slice(a.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

#[cfg(test)]
mod tests {
    // `super::*` already brings `AsyncReadExt`/`AsyncWriteExt` (imported `as _`).
    use super::*;

    #[test]
    fn encodes_a_command_as_resp_bulk_array() {
        let bytes = encode_command(&[b"AUTH", b"user", b"pw"]);
        assert_eq!(bytes, b"*3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$2\r\npw\r\n");
    }

    #[test]
    fn parse_slowlog_parses_realistic_entries() {
        // A realistic SLOWLOG GET reply: two 6-field entries. Entry 0 is a
        // `GET foo` that took 15000us; entry 1 is a `SET bar baz`. The argv
        // bulks carry KEY NAMES (the recon-sensitive part). Server 4.0+ adds the
        // client addr + name trailing fields.
        let items = vec![
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::Integer(1_700_000_000),
                RespValue::Integer(15_000),
                RespValue::Array(vec![
                    RespValue::Bulk(Some(b"GET".to_vec())),
                    RespValue::Bulk(Some(b"foo".to_vec())),
                ]),
                RespValue::Bulk(Some(b"10.0.0.7:54321".to_vec())),
                RespValue::Bulk(Some(b"worker-1".to_vec())),
            ]),
            RespValue::Array(vec![
                RespValue::Integer(0),
                RespValue::Integer(1_699_999_900),
                RespValue::Integer(8_200),
                RespValue::Array(vec![
                    RespValue::Bulk(Some(b"SET".to_vec())),
                    RespValue::Bulk(Some(b"bar".to_vec())),
                    RespValue::Bulk(Some(b"baz".to_vec())),
                ]),
                RespValue::Bulk(Some(b"10.0.0.8:40000".to_vec())),
                RespValue::Bulk(Some(b"".to_vec())),
            ]),
        ];
        let entries = parse_slowlog(&items);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, 1);
        assert_eq!(entries[0].timestamp, 1_700_000_000);
        assert_eq!(entries[0].micros, 15_000);
        assert_eq!(entries[0].argv, vec!["GET", "foo"]);
        assert_eq!(entries[0].client_addr, "10.0.0.7:54321");
        assert_eq!(entries[0].client_name, "worker-1");
        assert_eq!(entries[1].argv, vec!["SET", "bar", "baz"]);
        assert_eq!(entries[1].client_name, "");
    }

    #[test]
    fn parse_slowlog_tolerates_old_servers_and_bad_entries() {
        let items = vec![
            // Old server: only the first 4 fields, no client addr/name.
            RespValue::Array(vec![
                RespValue::Integer(5),
                RespValue::Integer(123),
                RespValue::Integer(99),
                RespValue::Array(vec![RespValue::Bulk(Some(b"PING".to_vec()))]),
            ]),
            // Too few fields: skipped, not a panic.
            RespValue::Array(vec![RespValue::Integer(6)]),
            // Wrong shape (not an array): skipped.
            RespValue::Integer(7),
        ];
        let entries = parse_slowlog(&items);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, 5);
        assert_eq!(entries[0].argv, vec!["PING"]);
        assert_eq!(entries[0].client_addr, "");
        assert_eq!(entries[0].client_name, "");
    }

    #[test]
    fn parse_client_list_parses_realistic_lines() {
        // A realistic two-client CLIENT LIST body. The first line is the querying
        // client itself (cmd=client|list); the second is a worker. The fields are
        // space-separated `field=value`; `tot-net-in` is an UNMODELED field that
        // must survive in `raw`.
        let body = "id=7 addr=127.0.0.1:6379 name=console age=42 idle=0 db=0 cmd=client|list tot-net-in=120\n\
                    id=8 addr=10.0.0.9:5050 name= age=3600 idle=12 db=2 cmd=get\n";
        let clients = parse_client_list(body);
        assert_eq!(clients.len(), 2);
        assert_eq!(clients[0].id, Some(7));
        assert_eq!(clients[0].addr.as_deref(), Some("127.0.0.1:6379"));
        assert_eq!(clients[0].name.as_deref(), Some("console"));
        assert_eq!(clients[0].age, Some(42));
        assert_eq!(clients[0].idle, Some(0));
        assert_eq!(clients[0].db, Some(0));
        assert_eq!(clients[0].cmd.as_deref(), Some("client|list"));
        // The unmodeled field survives in raw.
        assert_eq!(
            clients[0].raw.get("tot-net-in").map(String::as_str),
            Some("120")
        );
        // The worker: an empty name parses to Some("") (the field WAS present).
        assert_eq!(clients[1].id, Some(8));
        assert_eq!(clients[1].name.as_deref(), Some(""));
        assert_eq!(clients[1].db, Some(2));
        assert_eq!(clients[1].cmd.as_deref(), Some("get"));
    }

    #[test]
    fn parse_client_list_tolerates_blanks_and_missing_fields() {
        // A blank line, a line with a tokenless field, and a line missing the
        // numeric fields (so they parse to None, not a default).
        let body = "\n   \nid=1 addr=1.2.3.4:9 cmd=ping bogus noeq\n";
        let clients = parse_client_list(body);
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].id, Some(1));
        assert_eq!(clients[0].addr.as_deref(), Some("1.2.3.4:9"));
        assert_eq!(clients[0].cmd.as_deref(), Some("ping"));
        // Unset numeric fields are None, not 0.
        assert_eq!(clients[0].age, None);
        assert_eq!(clients[0].idle, None);
        assert_eq!(clients[0].db, None);
        // A non-key=value token is ignored, not recorded.
        assert!(!clients[0].raw.contains_key("bogus"));
    }

    #[test]
    fn read_password_file_trims_one_trailing_newline() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("ironcache-console-pw-{}.txt", std::process::id()));
        std::fs::write(&path, b"s3cr3t\n").unwrap();
        assert_eq!(read_password_file(&path).unwrap().as_slice(), b"s3cr3t");
        std::fs::write(&path, b"s3cr3t\r\n").unwrap();
        assert_eq!(read_password_file(&path).unwrap().as_slice(), b"s3cr3t");
        std::fs::write(&path, b"s3cr3t").unwrap();
        assert_eq!(read_password_file(&path).unwrap().as_slice(), b"s3cr3t");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn node_auth_debug_redacts_the_password() {
        // The manual Debug must never print the password bytes (project #145).
        let auth = NodeAuth {
            user: Some("monitor".to_owned()),
            password: Zeroizing::new(b"hunter2".to_vec()),
        };
        let text = format!("{auth:?}");
        assert!(text.contains("<redacted>"), "{text}");
        assert!(
            !text.contains("hunter2"),
            "Debug must not leak the password: {text}"
        );
    }

    /// A stub server that accepts the connection but NEVER replies: the client's
    /// op timeout must fire promptly with [`NodeError::Timeout`], not hang. This
    /// is the regression guard for the production read-timeout hang.
    #[tokio::test]
    async fn op_timeout_fires_when_server_never_replies() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // The server accepts and then holds the socket open, reading nothing back
        // and writing nothing, forever (for the test's lifetime).
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            // Drain the client's command write so its write_all completes, then
            // stall: never send a reply.
            let mut sink = [0u8; 256];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut sink).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(sock);
        });

        let mut client = NodeClient::connect(
            &addr.to_string(),
            None,
            None,
            Duration::from_secs(5),
            Duration::from_millis(200),
        )
        .await
        .unwrap();

        // The op timeout (200ms) must fire PROMPTLY: wrap the call in a TIGHT
        // outer guard (1s, the runtime timer seam, which the determinism lint
        // allows). If the read had hung, the outer guard would elapse first and
        // `result` would be `Err`; instead the 200ms op timeout returns inside it,
        // so `result` is `Ok(Err(NodeError::Timeout))`. This proves both no-hang
        // and promptness without reading a real clock.
        let result = tokio::time::timeout(Duration::from_secs(1), client.ping()).await;
        assert!(
            result.is_ok(),
            "ping must return via its own op timeout, well within the 1s guard (not hang)"
        );
        let inner = result.unwrap();
        assert!(
            matches!(inner, Err(NodeError::Timeout(_))),
            "a never-replying server must yield NodeError::Timeout, got {inner:?}"
        );
        server.abort();
    }

    /// A stub server that speaks RESP: it replies `+PONG` to PING and a canned
    /// INFO bulk, proving the happy path (command framing + reply parsing).
    #[tokio::test]
    async fn talks_to_a_stub_resp_server() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let info_body = "redis_version:9.9.9\r\nconnected_clients:1\r\n";
            let mut chunk = [0u8; 1024];
            // Read the PING request, reply +PONG.
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(b"+PONG\r\n").await.unwrap();
            // Read the INFO request, reply the bulk INFO body.
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            let bulk = format!("${}\r\n{info_body}\r\n", info_body.len());
            sock.write_all(bulk.as_bytes()).await.unwrap();
            // Keep the connection open briefly so the client reads cleanly.
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let mut client = NodeClient::connect(
            &addr.to_string(),
            None,
            None,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        client.ping().await.unwrap();
        let info = client.info().await.unwrap();
        assert!(info.contains("redis_version:9.9.9"), "{info}");
        server.abort();
    }

    /// A stub server that answers PING with an arbitrary (non-PONG) simple string:
    /// `ping` must REJECT it rather than treat any reply as success (Fix 6).
    #[tokio::test]
    async fn ping_rejects_a_non_pong_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut chunk = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            // A wrong/proxied endpoint that answers with something other than PONG.
            sock.write_all(b"+WELCOME\r\n").await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let mut client = NodeClient::connect(
            &addr.to_string(),
            None,
            None,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let result = client.ping().await;
        assert!(
            matches!(result, Err(NodeError::Protocol(_))),
            "a non-PONG PING reply must be rejected, got {result:?}"
        );
        server.abort();
    }

    /// PING answered with a lowercase bulk `pong` is accepted (case-insensitive).
    #[tokio::test]
    async fn ping_accepts_case_insensitive_pong() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut chunk = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(b"$4\r\npong\r\n").await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let mut client = NodeClient::connect(
            &addr.to_string(),
            None,
            None,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        client.ping().await.unwrap();
        server.abort();
    }

    /// A stub server that rejects AUTH with a RESP error: the client must map it
    /// to [`NodeError::Auth`], and the error text must not be the password.
    #[tokio::test]
    async fn auth_rejection_maps_to_auth_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut chunk = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(b"-WRONGPASS invalid username-password pair\r\n")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let auth = NodeAuth {
            user: Some("monitor".to_owned()),
            password: Zeroizing::new(b"hunter2".to_vec()),
        };
        let result = NodeClient::connect(
            &addr.to_string(),
            None,
            Some(&auth),
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await;
        match result {
            Err(NodeError::Auth(msg)) => {
                assert!(msg.contains("WRONGPASS"), "{msg}");
                assert!(
                    !msg.contains("hunter2"),
                    "auth error must not leak the password: {msg}"
                );
            }
            other => panic!("expected NodeError::Auth, got {other:?}"),
        }
        server.abort();
    }

    /// A refused connection (nothing listening) yields a Connect error promptly.
    #[tokio::test]
    async fn refused_connection_is_a_connect_error() {
        // Bind then drop to obtain a port that is (very likely) now free.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let result = NodeClient::connect(
            &addr.to_string(),
            None,
            None,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await;
        assert!(
            matches!(
                result,
                Err(NodeError::Connect { .. } | NodeError::Timeout(_))
            ),
            "a refused dial must be a Connect (or Timeout) error, got {result:?}"
        );
    }
}
