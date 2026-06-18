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
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, ServerConfig};
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
}
