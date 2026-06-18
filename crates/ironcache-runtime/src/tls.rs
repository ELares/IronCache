// SPDX-License-Identifier: MIT OR Apache-2.0
//! Embedded transport TLS for the CLIENT listener (#105, docs/design/TLS.md).
//!
//! This is the pure-Rust TLS layer that wraps an accepted client connection: rustls
//! (via tokio-rustls) terminates TLS 1.2/1.3 IN-PROCESS, so the single static binary
//! stays the whole deployment with no OpenSSL, no C TLS library, and no sidecar
//! (ADR-0017, CLI_BINARY.md #81). The crypto provider is `ring` (pinned at the
//! workspace level, NOT the aws-lc-rs default that needs cmake), keeping the
//! musl/aarch64 cross-build reproducible.
//!
//! ## Where this plugs in (and what stays byte-unchanged)
//!
//! The [`crate::Runtime`] trait's `Stream` associated type is UNCHANGED (`TcpStream`):
//! the cluster-bus / replication node-to-node links (REPLICATION.md, HA-7) keep
//! talking raw `Runtime::Stream`, so this client-TLS layer does NOT touch them (their
//! TLS is a documented follow-up). Instead, the CLIENT serve loop reads/writes a
//! [`ClientStream`] -- an enum of either the plain [`tokio::net::TcpStream`] (the
//! DEFAULT, `tls = off`) or a [`tokio_rustls::server::TlsStream`]. The plain variant's
//! [`ClientStream::recv`] / [`ClientStream::send`] are a THIN PASSTHROUGH to the exact
//! same `TcpStream` read/write code [`crate::TokioRuntime`] runs, so the plaintext hot
//! path is byte-identical to before this layer; the TLS variant pays the rustls
//! record framing only when TLS is configured.
//!
//! ## Determinism boundary (ADR-0003)
//!
//! TLS is real transport I/O on the runtime/accept seam, NOT a clock/RNG-on-the-engine
//! concern. rustls' own handshake RNG (ring -> getrandom) is TRANSPORT entropy that
//! never reaches the DST-verified command core, so it does not cross the determinism
//! boundary. The grep invariant-lint deliberately excludes `ironcache-runtime` (the
//! I/O seam) for exactly this reason.

use crate::{IoBuf, RecvResult};
use rustls_pki_types::pem::PemObject;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor as RustlsAcceptor;
use tokio_rustls::TlsConnector as RustlsConnector;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::server::TlsStream;

/// Errors building the TLS server configuration from the configured cert/key PEM.
///
/// Distinct from a per-connection handshake failure (which is a plain
/// [`std::io::Error`] surfaced by [`accept_tls`]): these are BOOT-time configuration
/// faults -- a missing/unreadable file, a PEM with no cert or no key, or a key rustls
/// rejects -- so the binary can fail boot with a clear message rather than starting a
/// listener that rejects every handshake.
#[derive(Debug)]
pub enum TlsConfigError {
    /// The cert or key file could not be read.
    Io {
        /// The path that failed to read.
        path: String,
        /// The underlying I/O error rendered.
        source: io::Error,
    },
    /// The PEM parsed but contained no usable item of the expected kind.
    Pem {
        /// The path whose PEM was empty/malformed.
        path: String,
        /// What was missing (e.g. "no certificates", "no private key").
        reason: String,
    },
    /// rustls rejected the assembled cert chain + key (bad key, mismatch, etc.).
    Rustls(rustls::Error),
}

impl std::fmt::Display for TlsConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsConfigError::Io { path, source } => {
                write!(f, "reading TLS material from {path}: {source}")
            }
            TlsConfigError::Pem { path, reason } => {
                write!(f, "TLS PEM at {path}: {reason}")
            }
            TlsConfigError::Rustls(e) => write!(f, "rustls rejected the TLS config: {e}"),
        }
    }
}

impl std::error::Error for TlsConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TlsConfigError::Io { source, .. } => Some(source),
            TlsConfigError::Rustls(e) => Some(e),
            TlsConfigError::Pem { .. } => None,
        }
    }
}

/// Build a [`tokio_rustls::TlsAcceptor`] from the cert-chain and private-key PEM files
/// at `cert_path` / `key_path` (#105, docs/design/TLS.md "Cert/key config").
///
/// The result is server-auth ONLY (no client-certificate verification; mTLS is a
/// documented follow-up, docs/design/TLS.md "Optional mutual TLS"). rustls' default
/// [`ServerConfig`] excludes SSLv3/TLS1.0/TLS1.1 by construction, so the negotiated
/// floor is TLS 1.2 with TLS 1.3 preferred and the cipher suites are rustls' vetted
/// defaults (TLS.md version/cipher floor). The acceptor is cheap to clone (an `Arc`
/// inside), so the boot wiring builds it ONCE and clones it onto every shard.
///
/// # Errors
///
/// Returns [`TlsConfigError`] if a file cannot be read, the cert PEM has no
/// certificate, the key PEM has no private key, or rustls rejects the pair.
pub fn build_acceptor(cert_path: &str, key_path: &str) -> Result<RustlsAcceptor, TlsConfigError> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(TlsConfigError::Rustls)?;
    Ok(RustlsAcceptor::from(Arc::new(config)))
}

/// Read and parse the PEM cert CHAIN at `path` into rustls' owned DER certificates, via
/// `rustls-pki-types`' own `PemObject` parser (the maintained successor to the archived
/// rustls-pemfile, RUSTSEC-2025-0134). A leaf-first chain with any intermediates is supported
/// (every `CERTIFICATE` block is taken in file order). Errors if the file is unreadable, the PEM
/// is malformed, or it holds no certificate.
fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let pem = std::fs::read(path).map_err(|e| TlsConfigError::Io {
        path: path.to_owned(),
        source: e,
    })?;
    // `pem_slice_iter` yields every CERTIFICATE block in file order; a malformed block surfaces as
    // a parse error (mapped to a Pem error) rather than being silently skipped.
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<_, _>>()
        .map_err(|e| TlsConfigError::Pem {
            path: path.to_owned(),
            reason: format!("malformed certificate PEM: {e}"),
        })?;
    if certs.is_empty() {
        return Err(TlsConfigError::Pem {
            path: path.to_owned(),
            reason: "no certificates found (expected at least one PEM CERTIFICATE block)"
                .to_owned(),
        });
    }
    Ok(certs)
}

/// Read and parse the PEM PRIVATE KEY at `path`, accepting PKCS#8, RSA (PKCS#1), and SEC1 (EC)
/// encodings (`PrivateKeyDer::from_pem_slice` takes the FIRST key block of any of those forms).
/// Errors if the file is unreadable, the PEM is malformed, or it holds no private key.
fn load_key(path: &str) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    let pem = std::fs::read(path).map_err(|e| TlsConfigError::Io {
        path: path.to_owned(),
        source: e,
    })?;
    PrivateKeyDer::from_pem_slice(&pem).map_err(|e| TlsConfigError::Pem {
        path: path.to_owned(),
        reason: format!("no usable private key (expected a PKCS#8, RSA, or SEC1 PEM block): {e}"),
    })
}

/// The maximum time a client may take to complete the TLS handshake before the connection
/// is dropped. Bounds the slow-loris vector: half-open handshakes cannot accumulate and
/// exhaust the per-shard serve tasks. Ten seconds matches the common industry default
/// (e.g. the `tls-listener` crate) and is generous for a legitimate client.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Perform the TLS server handshake on a freshly accepted [`TcpStream`], returning a
/// [`ClientStream::Tls`] the serve loop reads/writes transparently.
///
/// This is called RIGHT AFTER `accept` and BEFORE the RESP loop: a client that connects
/// in plaintext to a TLS port fails the handshake here (returning `Err`), so it is
/// REJECTED (the connection is dropped) rather than hung. The `TlsStream` is boxed
/// because it is much larger than the plain `TcpStream` variant, keeping the enum small
/// for the common plaintext case.
///
/// # Errors
///
/// Returns the [`std::io::Error`] from a failed handshake (a non-TLS client, an
/// untrusted client cert if mTLS were on, an unsupported protocol version, etc.), or a
/// [`std::io::ErrorKind::TimedOut`] error if the handshake does not complete within
/// [`HANDSHAKE_TIMEOUT`].
pub async fn accept_tls(acceptor: &RustlsAcceptor, tcp: TcpStream) -> io::Result<ClientStream> {
    // Bound the handshake: a client that completes the TCP connect but then dribbles or
    // never sends a valid ClientHello would otherwise pin this per-shard serve task
    // forever (tokio-rustls imposes no handshake timeout of its own), which is a classic
    // slow-loris DoS that is especially cheap against the thread-per-core runtime. After
    // the bound the connection is dropped, freeing the slot.
    let tls = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out"))??;
    Ok(ClientStream::Tls(Box::new(tls)))
}

/// The stream the CLIENT serve loop reads/writes: either a plain TCP connection (the
/// DEFAULT, `tls = off`) or a rustls-terminated TLS connection (`tls = on`).
///
/// The serve loop is written against THIS type (its [`Self::recv`] / [`Self::send`])
/// instead of the raw [`crate::Runtime`] methods, so a single loop serves both
/// transports. The plaintext variant is a thin passthrough to the SAME `TcpStream`
/// read/write code the [`crate::TokioRuntime`] backend uses (the owned-buffer model,
/// RUNTIME_ABSTRACTION.md), so `tls = off` is byte-identical to before this layer.
#[derive(Debug)]
pub enum ClientStream {
    /// A plaintext client connection (the default). Byte-identical to the pre-TLS path.
    Plain(TcpStream),
    /// A rustls-terminated TLS client connection. Boxed (a `TlsStream` is large) to keep
    /// the enum small for the common plaintext case.
    Tls(Box<TlsStream<TcpStream>>),
}

impl ClientStream {
    /// Wrap a plain accepted TCP stream WITHOUT TLS (the `tls = off` default path).
    #[must_use]
    pub fn plain(tcp: TcpStream) -> Self {
        ClientStream::Plain(tcp)
    }

    /// The peer (client) address, for `CLIENT INFO`. Mirrors `TcpStream::peer_addr` on
    /// both variants (the TLS variant reads it off the wrapped TCP socket).
    ///
    /// # Errors
    ///
    /// Propagates the underlying socket error.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            ClientStream::Plain(s) => s.peer_addr(),
            ClientStream::Tls(s) => s.get_ref().0.peer_addr(),
        }
    }

    /// The local (server) address, for `CLIENT INFO`. Mirrors `TcpStream::local_addr`.
    ///
    /// # Errors
    ///
    /// Propagates the underlying socket error.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            ClientStream::Plain(s) => s.local_addr(),
            ClientStream::Tls(s) => s.get_ref().0.local_addr(),
        }
    }

    /// Read into the owned `buf` (owned-buffer model, RUNTIME_ABSTRACTION.md), appending
    /// to its existing contents, and return the buffer plus the byte count (0 = peer
    /// closed). The plaintext arm is the SAME code as [`crate::TokioRuntime::recv`]
    /// (byte-identical hot path); the TLS arm reads decrypted application bytes out of
    /// the rustls record layer.
    ///
    /// # Errors
    ///
    /// Propagates the underlying read error.
    pub async fn recv(&mut self, mut buf: Vec<u8>) -> io::Result<RecvResult<Vec<u8>>> {
        let start = IoBuf::len(&buf);
        let want = 16 * 1024;
        buf.resize(start + want, 0);
        let n = match self {
            ClientStream::Plain(s) => s.read(&mut buf[start..]).await?,
            ClientStream::Tls(s) => s.read(&mut buf[start..]).await?,
        };
        buf.truncate(start + n);
        Ok(RecvResult { buf, n })
    }

    /// Write all of `buf`, then RETURN the buffer so the caller (or a pool) can reclaim
    /// it (owned-buffer model, symmetric with [`Self::recv`]). The plaintext arm is the
    /// SAME code as [`crate::TokioRuntime::send`]; the TLS arm encrypts into rustls
    /// records.
    ///
    /// # Errors
    ///
    /// Propagates the underlying write error.
    pub async fn send(&mut self, buf: Vec<u8>) -> io::Result<Vec<u8>> {
        match self {
            ClientStream::Plain(s) => s.write_all(buf.as_ref()).await?,
            ClientStream::Tls(s) => s.write_all(buf.as_ref()).await?,
        }
        Ok(buf)
    }
}

// ===========================================================================
// Intra-cluster transport security (PROD-3): TLS + shared-secret auth + frame bound
// for the node-to-node links (the Raft cluster-bus RAFTMSG control plane and the
// replication stream), which were plaintext, unauthenticated, and bound to the
// operator interface (often 0.0.0.0). Reuses the building blocks above (the `ring`
// provider pinning, the bounded handshake-timeout pattern, the PEM loaders) and adds
// the CLIENT side rustls handshake the dial path needs, so a single static binary
// (no C TLS library) secures both transports.
//
// Threat model addressed: an attacker on the network can FORGE consensus messages
// (fake AppendEntries / RequestVote to hijack the cluster) or SIPHON the entire
// keyspace off the replication stream. TLS encrypts + integrity-protects the link;
// the shared cluster secret (a constant-time compare right after the handshake)
// authenticates the PEER, so a party lacking the secret is dropped even if it can
// reach the port and complete a TLS handshake. mTLS with a cluster CA (client-cert
// verification) is a documented stronger follow-up; the shared secret is the v1.
// ===========================================================================

/// Compare two byte slices in CONSTANT TIME (no data-dependent early-out), for the
/// shared-secret peer-authentication handshake on the intra-cluster links. A naive
/// `==` leaks a timing side-channel (it returns on the first differing byte), letting a
/// network attacker recover the secret prefix-by-prefix; this folds EVERY byte pair
/// into an XOR accumulator and only tests it at the end, examining all bytes regardless
/// of an early mismatch. A length difference short-circuits (the secret length is not
/// itself secret, and the loop needs equal lengths); the accumulator is read through
/// [`std::hint::black_box`] so the optimizer cannot prove the loop short-circuitable and
/// re-introduce a data-dependent exit. Hand-rolled (no `subtle` dep) to keep the I/O
/// seam dependency-light, mirroring `ironcache_server`'s `check_auth` compare.
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }
    std::hint::black_box(acc) == 0
}

/// The SERVER name the cluster TLS CLIENT presents in its handshake SNI / verifies the
/// peer cert against. Intra-cluster certs are operated as a single self-signed / cluster
/// cert (not per-host public-CA certs), so a fixed logical name is used on both sides;
/// peer IDENTITY is established by the shared secret (v1) or a cluster CA (mTLS follow-up),
/// not by hostname matching against a public PKI. A stable constant keeps the client config
/// buildable once and cloned onto every dial.
pub const CLUSTER_TLS_SERVER_NAME: &str = "ironcache-cluster";

/// Build the rustls CLIENT configuration the intra-cluster DIAL path uses to wrap a freshly
/// connected `TcpStream` (the bus `send_to_peer` dial and the replica/importer dial).
///
/// When `ca_path` is `Some`, the peer (server) certificate is verified against the cluster CA
/// root loaded from that PEM: this is the standard rustls webpki verification, so a server
/// presenting a cert NOT signed by the cluster CA fails the handshake. When `ca_path` is `None`,
/// the client uses a verifier that ACCEPTS the peer cert (an intra-cluster self-signed cert with
/// no shared CA): the link is still ENCRYPTED, and peer AUTHENTICATION is then provided by the
/// shared-secret handshake performed right after the TLS handshake. The accept-any verifier is
/// deliberately scoped to the cluster transport (never the public client listener) and is the
/// pragmatic v1 for a self-signed cluster cert; configuring `ca_path` (a cluster CA) is the
/// stronger posture and is preferred when available.
///
/// # Errors
///
/// Returns [`TlsConfigError`] if the CA file cannot be read, its PEM holds no certificate, or
/// rustls rejects the assembled root store.
pub fn build_cluster_client_config(
    ca_path: Option<&str>,
) -> Result<RustlsConnector, TlsConfigError> {
    let config = match ca_path {
        Some(ca) => {
            let roots = load_root_store(ca)?;
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
        None => ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert))
            .with_no_client_auth(),
    };
    Ok(RustlsConnector::from(Arc::new(config)))
}

/// Load a CA cert PEM at `path` into a rustls [`RootCertStore`] (every `CERTIFICATE` block is
/// added as a trust anchor), for verifying the intra-cluster peer cert against a cluster CA.
fn load_root_store(path: &str) -> Result<RootCertStore, TlsConfigError> {
    let certs = load_certs(path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(TlsConfigError::Rustls)?;
    }
    Ok(roots)
}

/// A rustls server-certificate verifier that ACCEPTS any presented certificate, used ONLY for the
/// intra-cluster dial when no cluster CA is configured ([`build_cluster_client_config`] with
/// `ca_path = None`). The link is still encrypted; peer authentication is delegated to the
/// shared-secret handshake. This is NEVER used by the public client listener (which has no client
/// side here) and never by a CA-configured cluster; it is the self-signed-cluster-cert v1 path.
#[derive(Debug)]
struct AcceptAnyServerCert;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        // Advertise the ring provider's full default scheme set so a tls12 or tls13 server cert is
        // accepted regardless of its signature algorithm (we accept the cert anyway).
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Perform the rustls CLIENT handshake on a freshly DIALED [`TcpStream`] (the intra-cluster dial:
/// the bus `send_to_peer` and the replica/importer dial), returning a [`SecureStream::ClientTls`]
/// the caller reads/writes transparently. Bounded by [`HANDSHAKE_TIMEOUT`] (the same slow-loris
/// bound the accept path uses) through tokio's timer: a server that completes the TCP connect but
/// then stalls the TLS handshake cannot pin the dialing task forever.
///
/// # Errors
///
/// Returns the [`std::io::Error`] from a failed handshake (an untrusted peer cert when a cluster
/// CA is configured, a plaintext / non-TLS server, an unsupported version), or a
/// [`std::io::ErrorKind::TimedOut`] if the handshake does not complete within [`HANDSHAKE_TIMEOUT`].
pub async fn connect_tls(connector: &RustlsConnector, tcp: TcpStream) -> io::Result<SecureStream> {
    let server_name = ServerName::try_from(CLUSTER_TLS_SERVER_NAME)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
        .to_owned();
    let tls = tokio::time::timeout(HANDSHAKE_TIMEOUT, connector.connect(server_name, tcp))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS client handshake timed out"))??;
    Ok(SecureStream::ClientTls(Box::new(tls)))
}

/// Perform the rustls SERVER handshake on a freshly ACCEPTED [`TcpStream`] for the intra-cluster
/// LISTENER (the bus `RAFTMSG` listener and the replication source listener), returning a
/// [`SecureStream::ServerTls`]. Reuses [`accept_tls`]'s bounded-handshake machinery via the same
/// [`HANDSHAKE_TIMEOUT`]; a plaintext dialer to a TLS cluster port fails the handshake here and is
/// dropped (not hung).
///
/// # Errors
///
/// Returns the handshake [`std::io::Error`] or a [`std::io::ErrorKind::TimedOut`] on the bound.
pub async fn accept_cluster_tls(
    acceptor: &RustlsAcceptor,
    tcp: TcpStream,
) -> io::Result<SecureStream> {
    let tls = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS server handshake timed out"))??;
    Ok(SecureStream::ServerTls(Box::new(tls)))
}

/// A node-to-node intra-cluster stream: either a plain TCP connection (the DEFAULT, cluster
/// transport security OFF -- byte-identical to before this layer) or a rustls-terminated TLS
/// connection (the CLIENT side from [`connect_tls`] on the dial, the SERVER side from
/// [`accept_cluster_tls`] on the listener).
///
/// The bus + repl transports read/write THROUGH this type (its [`Self::recv`] / [`Self::send`])
/// so a single code path serves both plaintext and TLS. The plaintext variant is a thin
/// passthrough to the same `TcpStream` read/write the tokio backend uses (owned-buffer model), so
/// the default-off path is byte-identical. The TLS variants are boxed (a `TlsStream` is large) to
/// keep the enum small for the common plaintext case.
#[derive(Debug)]
pub enum SecureStream {
    /// A plaintext node-to-node connection (the default, byte-identical to the pre-PROD-3 path).
    Plain(TcpStream),
    /// A rustls-terminated TLS connection from the CLIENT (dial) side.
    ClientTls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    /// A rustls-terminated TLS connection from the SERVER (listener) side.
    ServerTls(Box<TlsStream<TcpStream>>),
}

impl SecureStream {
    /// Wrap a plain stream WITHOUT TLS (the cluster-transport-security-OFF default path).
    #[must_use]
    pub fn plain(tcp: TcpStream) -> Self {
        SecureStream::Plain(tcp)
    }

    /// Read into the owned `buf` (owned-buffer model), appending to its existing contents, and
    /// return the buffer plus the byte count (0 = peer closed). The plaintext arm is the SAME read
    /// the tokio backend runs (byte-identical); the TLS arms read decrypted application bytes out
    /// of the rustls record layer.
    ///
    /// # Errors
    ///
    /// Propagates the underlying read error.
    pub async fn recv(&mut self, mut buf: Vec<u8>) -> io::Result<RecvResult<Vec<u8>>> {
        let start = IoBuf::len(&buf);
        let want = 16 * 1024;
        buf.resize(start + want, 0);
        let n = match self {
            SecureStream::Plain(s) => s.read(&mut buf[start..]).await?,
            SecureStream::ClientTls(s) => s.read(&mut buf[start..]).await?,
            SecureStream::ServerTls(s) => s.read(&mut buf[start..]).await?,
        };
        buf.truncate(start + n);
        Ok(RecvResult { buf, n })
    }

    /// Write all of `buf`, then RETURN the buffer (owned-buffer model). The plaintext arm is the
    /// SAME write the tokio backend runs; the TLS arms encrypt into rustls records.
    ///
    /// # Errors
    ///
    /// Propagates the underlying write error.
    pub async fn send(&mut self, buf: Vec<u8>) -> io::Result<Vec<u8>> {
        match self {
            SecureStream::Plain(s) => s.write_all(buf.as_ref()).await?,
            SecureStream::ClientTls(s) => s.write_all(buf.as_ref()).await?,
            SecureStream::ServerTls(s) => s.write_all(buf.as_ref()).await?,
        }
        Ok(buf)
    }

    /// Read EXACTLY `n` bytes into a fresh buffer (used by the fixed-length shared-secret handshake
    /// read). Errors with [`std::io::ErrorKind::UnexpectedEof`] if the peer closes early.
    ///
    /// # Errors
    ///
    /// Propagates the underlying read error, or `UnexpectedEof` on a short read.
    async fn read_exact_n(&mut self, n: usize) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; n];
        match self {
            SecureStream::Plain(s) => s.read_exact(&mut buf).await?,
            SecureStream::ClientTls(s) => s.read_exact(&mut buf).await?,
            SecureStream::ServerTls(s) => s.read_exact(&mut buf).await?,
        };
        Ok(buf)
    }
}

/// The wire framing of the shared-secret AUTH handshake performed RIGHT AFTER the TLS handshake
/// (and on a plaintext link when only a secret is configured), BEFORE any RAFTMSG / repl byte is
/// exchanged: a fixed 4-byte big-endian length prefix followed by the secret bytes. Bounded by
/// [`MAX_SECRET_LEN`] so a peer cannot claim a huge secret length to drive an allocation; the
/// secret is compared in CONSTANT TIME ([`constant_time_eq`]). A length-prefixed frame (not a
/// delimiter) keeps the secret opaque binary and lets either side detect a truncated handshake.
const SECRET_LEN_PREFIX: usize = 4;

/// Upper bound on the secret length accepted off the wire in [`read_peer_secret`], so the 4-byte
/// length prefix cannot drive a large pre-handshake allocation. A cluster secret is a short
/// shared token; 4 KiB is far above any sane secret yet a trivial allocation if forged.
pub const MAX_SECRET_LEN: usize = 4096;

/// Errors from the shared-secret peer-authentication handshake on an intra-cluster link.
#[derive(Debug)]
pub enum SecretError {
    /// An I/O error sending or receiving the secret frame.
    Io(io::Error),
    /// The peer's secret frame was malformed (a length over [`MAX_SECRET_LEN`], or a short read).
    Malformed,
    /// The peer presented a secret that did NOT match (constant-time compared): it is NOT a
    /// trusted cluster member and the connection MUST be dropped.
    Mismatch,
}

impl std::fmt::Display for SecretError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecretError::Io(e) => write!(f, "cluster secret handshake I/O error: {e}"),
            SecretError::Malformed => write!(f, "cluster secret handshake frame malformed"),
            SecretError::Mismatch => write!(f, "cluster peer presented an incorrect secret"),
        }
    }
}

impl std::error::Error for SecretError {}

/// Send THIS node's cluster `secret` over `stream` as a length-prefixed frame (the first bytes
/// after the TLS handshake, before any RAFTMSG / repl data). Both the dialer and the acceptor send
/// their secret and verify the peer's, so authentication is MUTUAL.
///
/// # Errors
///
/// Returns [`SecretError::Io`] on a write failure.
pub async fn send_secret(stream: &mut SecureStream, secret: &[u8]) -> Result<(), SecretError> {
    let len = u32::try_from(secret.len()).map_err(|_| SecretError::Malformed)?;
    let mut frame = Vec::with_capacity(SECRET_LEN_PREFIX + secret.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(secret);
    stream.send(frame).await.map_err(SecretError::Io)?;
    Ok(())
}

/// Read the PEER's secret frame off `stream` and verify it against the expected `secret` in
/// CONSTANT TIME. A length over [`MAX_SECRET_LEN`] or a short read is [`SecretError::Malformed`];
/// a non-matching secret is [`SecretError::Mismatch`] (the caller MUST drop the connection). On
/// success the peer is an authenticated cluster member.
///
/// # Errors
///
/// Returns [`SecretError::Io`] on a read failure, [`SecretError::Malformed`] on a bad frame, or
/// [`SecretError::Mismatch`] on a wrong secret.
pub async fn read_peer_secret(stream: &mut SecureStream, secret: &[u8]) -> Result<(), SecretError> {
    let prefix = stream
        .read_exact_n(SECRET_LEN_PREFIX)
        .await
        .map_err(SecretError::Io)?;
    let len = u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize;
    if len > MAX_SECRET_LEN {
        return Err(SecretError::Malformed);
    }
    let presented = stream.read_exact_n(len).await.map_err(SecretError::Io)?;
    if constant_time_eq(&presented, secret) {
        Ok(())
    } else {
        Err(SecretError::Mismatch)
    }
}

/// Perform the full peer-authentication handshake on a freshly secured `stream` (after any TLS
/// handshake): send OUR secret, then read + verify the PEER's, both bounded. Used identically by
/// the bus dial / accept and the repl dial / accept so the order is symmetric (both sides send
/// then read). A wrong / absent secret drops the connection: an attacker who reached the port but
/// lacks the secret cannot join the bus, forge RAFTMSG, or pull the repl stream.
///
/// # Errors
///
/// Propagates [`SecretError`] (I/O, malformed, or mismatch).
pub async fn authenticate_peer(
    stream: &mut SecureStream,
    secret: &[u8],
) -> Result<(), SecretError> {
    send_secret(stream, secret).await?;
    read_peer_secret(stream, secret).await
}

#[cfg(test)]
mod tests {
    use super::*;

    // A throwaway self-signed test cert + key generated ONCE with openssl and pasted here, so the
    // unit test is fully deterministic (no rcgen, no wall-clock/entropy in the test) and offline.
    // CN=ironcache-test, valid 100 years. It is used ONLY to exercise build_acceptor's PEM-load
    // and rustls-acceptance path; the wire-level round-trip lives in the ironcache crate's
    // tests/tls.rs against the real server.
    const TEST_CERT: &str = include_str!("../tests/tls/cert.pem");
    const TEST_KEY: &str = include_str!("../tests/tls/key.pem");

    /// Write `contents` to a uniquely-named temp file and return the path. Uses the process id +
    /// a per-call counter for the name (NO rand: deterministic-enough for a test fixture, and the
    /// determinism lint excludes this crate anyway). The file lives under the OS temp dir.
    fn temp_pem(tag: &str, contents: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ironcache-tls-test-{tag}-{}-{n}.pem",
            std::process::id()
        ));
        std::fs::write(&path, contents).expect("write temp pem");
        path
    }

    #[test]
    fn build_acceptor_loads_valid_cert_and_key() {
        let cert = temp_pem("cert", TEST_CERT);
        let key = temp_pem("key", TEST_KEY);
        // `TlsAcceptor` is not `Debug`; map the Ok arm to () so a failure prints the error only.
        let built = build_acceptor(&cert.to_string_lossy(), &key.to_string_lossy()).map(|_| ());
        assert!(built.is_ok(), "valid cert+key should build: {built:?}");
        let _ = std::fs::remove_file(&cert);
        let _ = std::fs::remove_file(&key);
    }

    #[test]
    fn build_acceptor_missing_file_is_io_error() {
        // `TlsAcceptor` is not `Debug`, so map the Ok arm to () before asserting on the error.
        let key = temp_pem("key", TEST_KEY);
        let err = build_acceptor("/nonexistent/cert.pem", &key.to_string_lossy())
            .map(|_| ())
            .expect_err("a missing cert file must error");
        assert!(matches!(err, TlsConfigError::Io { .. }), "got {err:?}");
        let _ = std::fs::remove_file(&key);
    }

    #[test]
    fn build_acceptor_empty_cert_pem_is_pem_error() {
        // A PEM with a key but NO certificate block -> the "no certificates" PEM error.
        let cert = temp_pem("emptycert", "not a pem at all\n");
        let key = temp_pem("key", TEST_KEY);
        let err = build_acceptor(&cert.to_string_lossy(), &key.to_string_lossy())
            .map(|_| ())
            .expect_err("a cert PEM with no CERTIFICATE block must error");
        assert!(matches!(err, TlsConfigError::Pem { .. }), "got {err:?}");
        let _ = std::fs::remove_file(&cert);
        let _ = std::fs::remove_file(&key);
    }

    #[test]
    fn build_acceptor_no_key_is_pem_error() {
        // A key PEM with no private-key block -> the "no private key" PEM error.
        let cert = temp_pem("cert", TEST_CERT);
        let key = temp_pem("nokey", "garbage\n");
        let err = build_acceptor(&cert.to_string_lossy(), &key.to_string_lossy())
            .map(|_| ())
            .expect_err("a key PEM with no private key must error");
        assert!(matches!(err, TlsConfigError::Pem { .. }), "got {err:?}");
        let _ = std::fs::remove_file(&cert);
        let _ = std::fs::remove_file(&key);
    }

    #[test]
    fn plain_client_stream_round_trips_like_a_tcp_stream() {
        // The Plain variant must behave exactly like a bare TcpStream (the byte-unchanged default
        // path): connect, send PING, read it back on the server side, reply, read the reply.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::task::spawn_local(async move {
                let (tcp, _peer) = listener.accept().await.unwrap();
                let mut s = ClientStream::plain(tcp);
                // peer/local addr work on the plain variant.
                assert!(s.peer_addr().is_ok());
                assert!(s.local_addr().is_ok());
                let res = s.recv(Vec::with_capacity(16)).await.unwrap();
                assert_eq!(&res.buf[..res.n], b"PING\r\n");
                let returned = s.send(b"+PONG\r\n".to_vec()).await.unwrap();
                assert_eq!(returned, b"+PONG\r\n");
            });
            let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
            client.write_all(b"PING\r\n").await.unwrap();
            let mut reply = [0u8; 7];
            client.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"+PONG\r\n");
            server.await.unwrap();
        });
    }

    // --- Intra-cluster transport security (PROD-3) ---

    #[test]
    fn constant_time_eq_matches_naive_equality() {
        // The constant-time secret compare must agree with `==` on the boolean RESULT (only the
        // TIMING differs). Exercise equal, differing-byte, and differing-length pairs.
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secre"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn build_cluster_client_config_with_and_without_ca() {
        // No CA -> the accept-any verifier path (encrypted, secret-authenticated); a connector
        // builds. With a CA pointing at the test cert -> the webpki-verified path; also builds.
        let no_ca = build_cluster_client_config(None).map(|_| ());
        assert!(no_ca.is_ok(), "no-CA connector should build: {no_ca:?}");
        let ca = temp_pem("ca", TEST_CERT);
        let with_ca = build_cluster_client_config(Some(&ca.to_string_lossy())).map(|_| ());
        assert!(with_ca.is_ok(), "CA connector should build: {with_ca:?}");
        let _ = std::fs::remove_file(&ca);
    }

    #[test]
    fn secret_handshake_round_trip_and_mismatch_over_plaintext() {
        // The shared-secret handshake is symmetric (both sides send then verify). Over a loopback
        // PLAIN SecureStream (no TLS, so the test is hermetic + fast), a MATCHING secret authenticates
        // both peers; a MISMATCH yields SecretError::Mismatch and the connection is rejected.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            // Case 1: matching secret on both ends -> both authenticate_peer return Ok.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::task::spawn_local(async move {
                let (tcp, _peer) = listener.accept().await.unwrap();
                let mut s = SecureStream::plain(tcp);
                authenticate_peer(&mut s, b"cluster-secret").await
            });
            let client_tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut client = SecureStream::plain(client_tcp);
            let client_res = authenticate_peer(&mut client, b"cluster-secret").await;
            assert!(client_res.is_ok(), "client auth: {client_res:?}");
            assert!(server.await.unwrap().is_ok(), "server auth should succeed");

            // Case 2: WRONG secret on the client -> the server's read_peer_secret rejects it.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::task::spawn_local(async move {
                let (tcp, _peer) = listener.accept().await.unwrap();
                let mut s = SecureStream::plain(tcp);
                // The server presents the RIGHT secret and verifies the peer's (which is wrong).
                authenticate_peer(&mut s, b"cluster-secret").await
            });
            let client_tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut client = SecureStream::plain(client_tcp);
            // The client presents the WRONG secret; it still verifies the server's (which matches
            // the client's expectation? no -- the client expects "wrong-secret", server sends
            // "cluster-secret", so the client ALSO sees a mismatch). Either way at least one side
            // rejects; assert the SERVER (the listener admitting a peer) rejects the wrong secret.
            let _ = authenticate_peer(&mut client, b"wrong-secret").await;
            let server_res = server.await.unwrap();
            assert!(
                matches!(server_res, Err(SecretError::Mismatch)),
                "the server must REJECT a peer presenting the wrong secret, got {server_res:?}"
            );
        });
    }

    #[test]
    fn read_peer_secret_rejects_oversized_length_prefix() {
        // A forged length prefix over MAX_SECRET_LEN is rejected as Malformed BEFORE allocating the
        // claimed body, bounding a pre-handshake allocation DoS.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            use tokio::io::AsyncWriteExt;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::task::spawn_local(async move {
                let (tcp, _peer) = listener.accept().await.unwrap();
                let mut s = SecureStream::plain(tcp);
                read_peer_secret(&mut s, b"cluster-secret").await
            });
            let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
            // A 4-byte big-endian length far over MAX_SECRET_LEN (0xFFFFFFFF), then nothing.
            client.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
            let res = server.await.unwrap();
            assert!(
                matches!(res, Err(SecretError::Malformed)),
                "an over-cap secret length must be rejected as Malformed, got {res:?}"
            );
        });
    }

    #[test]
    fn max_cluster_frame_len_is_the_documented_cap() {
        // The frame bound is 512 MiB (Redis proto-max-bulk-len), enforced by the bus + repl parsers.
        assert_eq!(crate::MAX_CLUSTER_FRAME_LEN, 512 * 1024 * 1024);
    }
}
