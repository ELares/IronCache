// SPDX-License-Identifier: MIT OR Apache-2.0
//! Embedded transport-TLS acceptance tests (#105, docs/design/TLS.md).
//!
//! These boot the REAL multi-shard `run_server` with `tls = on` on an ephemeral port (the actual
//! SO_REUSEPORT thread-per-core topology + the rustls accept-path handshake) and drive it over a
//! real tokio-rustls CLIENT, so they exercise the whole client-TLS path end to end: the boot-time
//! acceptor build from the checked-in cert/key, the per-connection rustls handshake before the
//! RESP loop, and RESP commands flowing over the encrypted `ClientStream::Tls`. A companion test
//! proves a PLAINTEXT client to the TLS port is REJECTED (the handshake fails, the connection is
//! dropped, not hung), and the existing plaintext integration suites (pubsub/cluster/...) prove
//! the default `tls = off` path stays byte-unchanged.
//!
//! The cert/key are CHECKED IN under `tests/tls/` (a long-lived self-signed cert for
//! localhost/127.0.0.1), so the test is fully deterministic and offline: no cert generation, no
//! wall-clock/entropy in the test.

use ironcache::test_support::run_tls_server_for_test;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

// jemalloc as this test binary's global allocator, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// The directory holding the checked-in test cert/key PEM, resolved from the crate manifest dir
/// so it is independent of the test's CWD.
fn tls_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls")
}

fn cert_path() -> PathBuf {
    tls_dir().join("cert.pem")
}

fn key_path() -> PathBuf {
    tls_dir().join("key.pem")
}

/// Grab a free TCP port by binding an ephemeral listener and dropping it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Build a tokio-rustls CLIENT connector that TRUSTS the checked-in self-signed test cert (it is
/// its own CA). Server-auth only (no client cert), matching the server's posture.
fn test_connector() -> TlsConnector {
    let pem = std::fs::read(cert_path()).expect("read test cert");
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<_, _>>()
        .expect("parse test cert");
    let mut roots = RootCertStore::empty();
    for c in certs {
        roots.add(c).expect("add test cert to root store");
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

/// Connect a TLS client to `port` with a few short retries (the shards bind asynchronously after
/// `run_server`), performing the rustls handshake against the "localhost" SAN in the test cert.
async fn connect_tls_retry(port: u16) -> TlsStream<TcpStream> {
    let connector = test_connector();
    let server_name = ServerName::try_from("localhost").expect("valid server name");
    let mut last_err: Option<String> = None;
    for _ in 0..50 {
        if let Ok(tcp) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = tcp.set_nodelay(true);
            match connector.connect(server_name.clone(), tcp).await {
                Ok(tls) => return tls,
                // The listener may be up but a shard not yet ready; retry briefly.
                Err(e) => last_err = Some(e.to_string()),
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("TLS server never came up on port {port}; last handshake error: {last_err:?}");
}

/// Read until at least `min` bytes are buffered, returning the buffer.
async fn read_some<S>(stream: &mut S, min: usize) -> Vec<u8>
where
    S: AsyncReadExt + Unpin,
{
    let mut buf = Vec::new();
    while buf.len() < min {
        let mut chunk = [0u8; 1024];
        let n = stream.read(&mut chunk).await.expect("read");
        assert!(n > 0, "connection closed mid-reply (have {buf:?})");
        buf.extend_from_slice(&chunk[..n]);
    }
    buf
}

#[test]
fn tls_round_trip_ping_and_set_get() {
    // Boot the real server with TLS on, then drive PING + SET/GET over a rustls client.
    let port = free_port();
    let set = run_tls_server_for_test(port, 2, cert_path(), key_path());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut tls = connect_tls_retry(port).await;

        // PING -> +PONG over TLS.
        tls.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let pong = read_some(&mut tls, b"+PONG\r\n".len()).await;
        assert_eq!(&pong[..b"+PONG\r\n".len()], b"+PONG\r\n", "PING over TLS");

        // SET k v -> +OK over TLS.
        tls.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nval\r\n")
            .await
            .unwrap();
        let ok = read_some(&mut tls, b"+OK\r\n".len()).await;
        assert_eq!(&ok[..b"+OK\r\n".len()], b"+OK\r\n", "SET over TLS");

        // GET k -> $3\r\nval\r\n over TLS.
        tls.write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        let got = read_some(&mut tls, b"$3\r\nval\r\n".len()).await;
        assert_eq!(
            &got[..b"$3\r\nval\r\n".len()],
            b"$3\r\nval\r\n",
            "GET over TLS"
        );
    });

    set.shutdown_and_join().expect("clean shutdown");
}

#[test]
fn plaintext_client_to_tls_port_is_rejected() {
    // A client that speaks PLAINTEXT RESP to a TLS-only port must FAIL the handshake: the server
    // sees a non-TLS ClientHello, rejects it, and closes -- so the plaintext client gets a closed
    // connection (read returns 0 / an error), NOT a +PONG and NOT an indefinite hang.
    let port = free_port();
    let set = run_tls_server_for_test(port, 1, cert_path(), key_path());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // Give the shard a moment to bind.
        let mut tcp = None;
        for _ in 0..50 {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
                tcp = Some(s);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut tcp = tcp.expect("TLS port never accepted a TCP connection");
        // Send a plaintext PING. rustls reads this as a malformed TLS record and aborts the
        // handshake, dropping the connection.
        let _ = tcp.write_all(b"*1\r\n$4\r\nPING\r\n").await;

        // The read must NOT return a RESP +PONG. It should observe a clean close (n == 0) or a
        // reset/error, all WITHIN a bounded time (never an indefinite hang).
        let mut chunk = [0u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(5), tcp.read(&mut chunk))
            .await
            .expect("plaintext client to TLS port HUNG (no close within 5s)");
        // A clean close (Ok(0)) or a reset/error are both the expected "rejected" outcome. The
        // ONLY failure is the server actually serving RESP back: any non-empty read that begins
        // with a RESP +PONG reply means a plaintext client was wrongly served on a TLS port.
        if let Ok(n) = read {
            assert!(
                !chunk[..n].starts_with(b"+PONG"),
                "a plaintext client must never get a RESP reply from a TLS port"
            );
        }
    });

    set.shutdown_and_join().expect("clean shutdown");
}

#[test]
fn stalled_tls_handshake_is_dropped_within_the_timeout() {
    // SLOW-LORIS GUARD (#323 review C1): a client that opens TCP to the TLS port but then STALLS
    // mid-handshake (sends the start of a TLS record and nothing more) must be DROPPED after the
    // bounded `HANDSHAKE_TIMEOUT`, not pin the per-shard serve task forever. Before the timeout was
    // added, the read below would hang for the entire connection lifetime (the DoS). This test
    // would FAIL (time out) on the unbounded code and PASS now.
    let port = free_port();
    let set = run_tls_server_for_test(port, 1, cert_path(), key_path());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut tcp = None;
        for _ in 0..50 {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
                tcp = Some(s);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut tcp = tcp.expect("TLS port never accepted a TCP connection");
        // One byte: the TLS handshake content-type (0x16) starting a record that never completes,
        // so the rustls handshake blocks waiting for the rest of the ClientHello.
        let _ = tcp.write_all(&[0x16u8]).await;

        // The server must close the stalled connection within the handshake timeout (plus a
        // scheduling margin), never hang. The bound is read from the public const so it tracks the
        // server's actual timeout.
        let bound = ironcache_runtime::HANDSHAKE_TIMEOUT + Duration::from_secs(5);
        let mut chunk = [0u8; 16];
        let read = tokio::time::timeout(bound, tcp.read(&mut chunk))
            .await
            .expect("stalled TLS handshake was NOT dropped within the timeout (slow-loris hang)");
        // A clean close (Ok(0)) or a reset/error is the expected drop; the server must never serve
        // RESP back on a stalled handshake.
        if let Ok(n) = read {
            assert!(
                n == 0 || !chunk[..n].starts_with(b"+"),
                "a stalled TLS handshake must be dropped, never served"
            );
        }
    });

    set.shutdown_and_join().expect("clean shutdown");
}
