// SPDX-License-Identifier: MIT OR Apache-2.0
//! Intra-cluster transport security for the node-to-node links (PROD-3): TLS encryption +
//! shared-secret peer authentication, wrapping the [`crate::PeerConn`] dial and the
//! `ironcache-raft-net` `RAFTMSG` listener (and reused by the replication transport).
//!
//! ## Why this exists
//!
//! A production-readiness audit found the Raft cluster-bus (the `RAFTMSG` control-plane messages)
//! AND the replication links were PLAINTEXT, UNAUTHENTICATED, and bound to the operator interface
//! (often `0.0.0.0`). An attacker on the network could FORGE consensus messages (fake AppendEntries
//! / RequestVote to hijack the cluster) or SIPHON the full keyspace off the replication stream.
//! This module adds, OPT-IN and default-off byte-unchanged:
//!
//! * TLS (rustls, the `ring` provider, reusing `ironcache-runtime`'s client-TLS infra) on the dial
//!   and the listener, so the link is encrypted + integrity-protected.
//! * A shared CLUSTER SECRET handshake (a constant-time compare right after the TLS handshake) that
//!   AUTHENTICATES the peer: a party lacking the secret is dropped even if it can reach the port and
//!   complete a TLS handshake. This is the v1 peer-auth; mTLS with a cluster CA (client-cert
//!   verification) is the documented stronger follow-up.
//!
//! ## Determinism boundary (ADR-0003)
//!
//! Security is REAL transport I/O on the dial / accept seam, never the pure engine. The
//! deterministic-simulation runtimes (`ironcache-sim`) drive the pure consensus engine directly via
//! `SimNode`, NOT this transport, so they never construct a [`ClusterSecurity`] and always take the
//! plaintext [`BusStream::Runtime`] path; the DST determinism + raft-safety gates are untouched. The
//! secure path is concrete to the tokio `TcpStream` (the production `TokioRuntime::Stream`), which is
//! the only runtime that ever drives the bus / repl transports.

use ironcache_runtime::{RecvResult, Runtime};

#[cfg(feature = "tls")]
use ironcache_runtime::{SecretError, SecureStream};
#[cfg(feature = "tls")]
use std::sync::Arc;
#[cfg(feature = "tls")]
use tokio::net::TcpStream;
#[cfg(feature = "tls")]
use tokio_rustls::{TlsAcceptor, TlsConnector};
#[cfg(feature = "tls")]
use zeroize::Zeroizing;

/// The configured intra-cluster transport security a node holds (PROD-3), shared by the bus dial,
/// the bus listener, and the replication transport. Cheap to clone (the rustls configs and the
/// secret are behind `Arc`s), so the boot wiring builds it ONCE and clones it onto every dial /
/// accept. `None` everywhere it appears means the pre-PROD-3 plaintext path (byte-unchanged).
///
/// At least ONE of TLS or a secret is configured when a `ClusterSecurity` exists (a fully-empty one
/// would be equivalent to plaintext): the boot wiring constructs it only when `cluster_tls = on`
/// and/or a `cluster_secret` is set.
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct ClusterSecurity {
    /// The rustls CLIENT connector for the DIAL side (wraps a dialed `TcpStream`). `None` when TLS
    /// is off (a plaintext-but-authenticated cluster: secret only).
    connector: Option<TlsConnector>,
    /// The rustls SERVER acceptor for the LISTENER side (wraps an accepted `TcpStream`). `None` when
    /// TLS is off. Built from the same cluster cert/key as the connector verifies against.
    acceptor: Option<TlsAcceptor>,
    /// The shared cluster secret, sent + verified (constant-time) right after the TLS handshake on
    /// BOTH sides. `None` is permitted only when TLS is on with mTLS-style CA verification in a
    /// future follow-up; in v1 the secret is always present when a `ClusterSecurity` exists.
    ///
    /// ZEROIZE-ON-DROP (#145, the in-memory secret-lifetime defense-in-depth): the plaintext secret
    /// is held for the PROCESS LIFETIME (the handshake compares it against the peer's, so unlike a
    /// password it cannot be reduced to a hash at rest). It is wrapped in a [`Zeroizing<Vec<u8>>`] so
    /// the backing bytes are scrubbed (a volatile write the optimizer may not elide) when the LAST
    /// `Arc` to the [`ClusterSecurity`] drops, so a core dump / memory disclosure of a torn-down node
    /// does not trivially yield the plaintext. `Zeroizing` derefs to `[u8]`, so the handshake read
    /// site is byte-unchanged; the wrap touches only construction + drop, never the hot data path.
    secret: Option<Arc<Zeroizing<Vec<u8>>>>,
}

#[cfg(feature = "tls")]
impl ClusterSecurity {
    /// Assemble the security handle from the (already-built) optional rustls connector + acceptor
    /// and the optional shared secret. The boot wiring builds the connector/acceptor from the
    /// configured cluster cert/key/CA and passes the `cluster_secret` bytes.
    #[must_use]
    pub fn new(
        connector: Option<TlsConnector>,
        acceptor: Option<TlsAcceptor>,
        secret: Option<Vec<u8>>,
    ) -> Self {
        Self {
            connector,
            acceptor,
            // Take ownership of the plaintext secret into a `Zeroizing` buffer so it is scrubbed on
            // the final drop (#145). The caller's source `Vec` is MOVED in here (no extra copy).
            secret: secret.map(|s| Arc::new(Zeroizing::new(s))),
        }
    }

    /// Whether TLS is configured (vs a plaintext-but-authenticated secret-only cluster).
    #[must_use]
    pub fn has_tls(&self) -> bool {
        self.connector.is_some() || self.acceptor.is_some()
    }

    /// Secure a freshly DIALED `TcpStream` (the bus / repl dial side): perform the rustls CLIENT
    /// handshake if a connector is configured, then run the shared-secret peer-authentication
    /// handshake. Returns a [`SecureStream`] the caller reads/writes transparently.
    ///
    /// # Errors
    ///
    /// Returns a [`std::io::Error`] on a TLS handshake failure or a secret-handshake failure
    /// (mismatch / malformed / I/O), so the caller drops + re-dials the connection.
    pub async fn dial(&self, tcp: TcpStream) -> std::io::Result<SecureStream> {
        let mut stream = match &self.connector {
            Some(connector) => ironcache_runtime::connect_tls(connector, tcp).await?,
            None => SecureStream::plain(tcp),
        };
        self.run_secret_handshake(&mut stream).await?;
        Ok(stream)
    }

    /// Secure a freshly ACCEPTED `TcpStream` (the bus / repl listener side): perform the rustls
    /// SERVER handshake if an acceptor is configured, then run the shared-secret peer-authentication
    /// handshake. A plaintext dialer to a TLS port fails the handshake here and is dropped (not
    /// hung); a peer without the correct secret is dropped after the secret check.
    ///
    /// # Errors
    ///
    /// Returns a [`std::io::Error`] on a TLS or secret handshake failure, so the caller drops the
    /// connection.
    pub async fn accept(&self, tcp: TcpStream) -> std::io::Result<SecureStream> {
        let mut stream = match &self.acceptor {
            Some(acceptor) => ironcache_runtime::accept_cluster_tls(acceptor, tcp).await?,
            None => SecureStream::plain(tcp),
        };
        self.run_secret_handshake(&mut stream).await?;
        Ok(stream)
    }

    /// Run the shared-secret peer-authentication handshake on `stream` if a secret is configured
    /// (send OUR secret, read + constant-time-verify the PEER's). A [`SecretError`] is mapped to a
    /// [`std::io::Error`] so the dial / accept paths surface one error type. When no secret is
    /// configured the handshake is skipped (TLS-only; not the v1 default but supported).
    ///
    /// BOUNDED by `HANDSHAKE_TIMEOUT` (SECURITY, PROD-3 slow-loris fix): the secret exchange runs
    /// AFTER the (already bounded) TLS handshake but had NO timeout of its own, so a peer that
    /// completed TLS then stalled sending its secret would pin this serve / dial task FOREVER. The
    /// bound lives in [`ironcache_runtime::authenticate_peer_bounded`] (the runtime crate owns the
    /// tokio time seam + the timeout const); it drops a stalled exchange (an
    /// `io::ErrorKind::TimedOut`), freeing the task.
    async fn run_secret_handshake(&self, stream: &mut SecureStream) -> std::io::Result<()> {
        if let Some(secret) = &self.secret {
            // `Zeroizing<Vec<u8>>` derefs to `[u8]`; pass the bytes by slice so the handshake read is
            // byte-identical to the pre-zeroize `Arc<Vec<u8>>` path (no copy, no behavior change).
            ironcache_runtime::authenticate_peer_bounded(stream, secret.as_slice())
                .await
                .map_err(secret_error_to_io)?;
        }
        Ok(())
    }
}

/// Map a [`SecretError`] onto a [`std::io::Error`] so the dial / accept return type is uniform.
/// A mismatch / malformed frame is `PermissionDenied` (an untrusted peer); an I/O error keeps its
/// kind.
#[cfg(feature = "tls")]
fn secret_error_to_io(e: SecretError) -> std::io::Error {
    match e {
        SecretError::Io(io) => io,
        SecretError::Malformed | SecretError::Mismatch => {
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, e)
        }
    }
}

/// A node-to-node bus stream: either the PLAINTEXT runtime-seam stream (`R::Stream`, the default
/// byte-unchanged path, fully generic over the runtime) or a SECURE (TLS / secret) [`SecureStream`]
/// (the production tokio path when cluster security is configured).
///
/// The bus [`crate::PeerConn`] (dial) and the `ironcache-raft-net` listener (accept) read/write
/// THROUGH this enum so one request / serve loop drives both transports. The `Runtime` arm calls
/// the runtime seam's owned-buffer `send` / `recv` exactly as before this layer (byte-identical);
/// the `Secure` arm reads/writes decrypted application bytes through rustls. The `Secure` variant is
/// only constructed for the tokio backend (whose `Stream` is `TcpStream`).
pub enum BusStream<R: Runtime> {
    /// The plaintext runtime-seam stream (the default; byte-identical to the pre-PROD-3 path).
    Runtime(R::Stream),
    /// A TLS / secret-authenticated secure stream (PROD-3, opt-in). Behind the `tls` feature.
    #[cfg(feature = "tls")]
    Secure(SecureStream),
}

impl<R> BusStream<R>
where
    R: Runtime,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
{
    /// Write all of `buf` to the stream. The `Runtime` arm routes through the runtime seam's
    /// owned-buffer `send` (byte-identical hot path); the `Secure` arm writes through rustls. The
    /// owned buffer is consumed (the bus does not reuse it).
    ///
    /// # Errors
    ///
    /// Propagates the underlying write error.
    pub async fn send(&mut self, rt: &R, buf: Vec<u8>) -> Result<(), WriteError<R::Error>> {
        match self {
            BusStream::Runtime(s) => {
                let _ = rt.send(s, buf.into()).await.map_err(WriteError::Runtime)?;
                Ok(())
            }
            #[cfg(feature = "tls")]
            BusStream::Secure(s) => {
                let _ = s.send(buf).await.map_err(WriteError::Secure)?;
                Ok(())
            }
        }
    }

    /// Read a chunk into `buf` (appending), returning the grown buffer + the byte count (0 = peer
    /// closed). The `Runtime` arm routes through the runtime seam's owned-buffer `recv`; the
    /// `Secure` arm reads decrypted bytes through rustls.
    ///
    /// # Errors
    ///
    /// Propagates the underlying read error.
    pub async fn recv(
        &mut self,
        rt: &R,
        buf: Vec<u8>,
    ) -> Result<RecvResult<Vec<u8>>, WriteError<R::Error>> {
        match self {
            BusStream::Runtime(s) => {
                let res = rt.recv(s, buf.into()).await.map_err(WriteError::Runtime)?;
                Ok(RecvResult {
                    buf: res.buf.into(),
                    n: res.n,
                })
            }
            #[cfg(feature = "tls")]
            BusStream::Secure(s) => s.recv(buf).await.map_err(WriteError::Secure),
        }
    }
}

/// A bus-stream I/O error, distinguishing the runtime-seam error (the plaintext arm) from a secure
/// (TLS / rustls) I/O error (the secure arm), so the caller can render either uniformly.
#[derive(Debug)]
pub enum WriteError<E> {
    /// An error from the plaintext runtime-seam `send` / `recv`.
    Runtime(E),
    /// An I/O error from the secure (TLS) `send` / `recv`.
    #[cfg(feature = "tls")]
    Secure(std::io::Error),
}
