// SPDX-License-Identifier: MIT OR Apache-2.0
//! Hot TLS certificate reload acceptance tests (#563, docs/TLS.md "Certificate rotation").
//!
//! These boot the REAL multi-shard `run_server_observed` with `tls = on` and then rotate the
//! certificate WITHOUT a restart, exactly as an operator would: replace the configured cert/key
//! files on disk and trigger a reload. The SIGHUP handler is a thin wrapper over
//! `ironcache::serve::reload_client_tls`, so the test calls that function DIRECTLY (a hermetic,
//! signal-free reload) and asserts that:
//!
//!   * a NEW connection after a valid reload presents the NEW cert (a client trusting only the OLD
//!     cert now fails the handshake; a client trusting the NEW cert succeeds), and the reload does
//!     NOT drop the server (it keeps serving), and
//!   * a reload with a BAD/missing cert is REJECTED -- the previous good cert stays live (a client
//!     trusting it still completes PING/PONG) and the server never crashes.
//!
//! Two self-signed cert/key pairs are CHECKED IN under `tests/tls/` (`cert.pem`/`key.pem` and
//! `cert2.pem`/`key2.pem`, both for localhost/127.0.0.1), so the test is fully deterministic and
//! offline. The server boots against a COPY of pair #1 in a temp dir so the reload can overwrite
//! that copy in place with pair #2 (the real replace-file-then-reload flow) without touching the
//! checked-in fixtures.

use ironcache::test_support::run_tls_server_with_reload_for_test;
use std::path::{Path, PathBuf};
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

/// The directory holding the checked-in test cert/key PEM, resolved from the crate manifest dir.
fn tls_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls")
}

/// Grab a free TCP port by binding an ephemeral listener and dropping it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// A unique temp directory for this test's mutable cert/key copies (process id + a per-call
/// counter, no rand: deterministic enough for a fixture path).
fn unique_tmp_dir(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "ironcache-tls-reload-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Build a tokio-rustls CLIENT connector that trusts EXACTLY the self-signed cert at `cert_file`
/// (it is its own CA). Server-auth only, matching the server's posture. A server presenting a
/// DIFFERENT self-signed cert fails this client's handshake (unknown issuer), which is how the test
/// distinguishes the old cert from the new one.
fn connector_trusting(cert_file: &Path) -> TlsConnector {
    let pem = std::fs::read(cert_file).expect("read trusted cert");
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<_, _>>()
        .expect("parse trusted cert");
    let mut roots = RootCertStore::empty();
    for c in certs {
        roots.add(c).expect("add trusted cert to root store");
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

/// Attempt ONE TLS handshake to `port` with `connector` (a single TCP connect + rustls handshake,
/// no retry). `Ok` = the server's cert was trusted and the handshake completed; `Err` = the TCP
/// connect or the handshake failed (e.g. the server presented an untrusted cert).
async fn try_connect_once(
    port: u16,
    connector: &TlsConnector,
) -> Result<TlsStream<TcpStream>, String> {
    let server_name = ServerName::try_from("localhost").expect("valid server name");
    let tcp = TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|e| format!("tcp connect: {e}"))?;
    let _ = tcp.set_nodelay(true);
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| format!("handshake: {e}"))
}

/// Connect (with brief retries while the shards bind) using a connector that trusts `cert_file`,
/// then drive PING and assert +PONG over the encrypted stream. Proves the listener is alive AND
/// presenting a cert this connector trusts.
async fn assert_ping_pong_trusting(port: u16, cert_file: &Path) {
    let connector = connector_trusting(cert_file);
    let mut last_err = None;
    for _ in 0..50 {
        match try_connect_once(port, &connector).await {
            Ok(mut tls) => {
                tls.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
                let mut buf = Vec::new();
                while buf.len() < b"+PONG\r\n".len() {
                    let mut chunk = [0u8; 64];
                    let n = tls.read(&mut chunk).await.expect("read reply");
                    assert!(n > 0, "connection closed mid-reply");
                    buf.extend_from_slice(&chunk[..n]);
                }
                assert_eq!(&buf[..b"+PONG\r\n".len()], b"+PONG\r\n", "PING over TLS");
                return;
            }
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not complete a trusted TLS handshake on port {port}: {last_err:?}");
}

#[test]
fn sighup_reload_swaps_cert_and_bad_reload_keeps_old() {
    // Boot the real TLS server against a COPY of cert/key pair #1, so the reload can overwrite that
    // copy in place (the replace-file-then-SIGHUP flow) without disturbing the checked-in fixtures.
    let dir = unique_tmp_dir("swap");
    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");
    std::fs::copy(tls_dir().join("cert.pem"), &cert).expect("copy cert1");
    std::fs::copy(tls_dir().join("key.pem"), &key).expect("copy key1");

    let cert1 = tls_dir().join("cert.pem");
    let cert2_src = tls_dir().join("cert2.pem");
    let key2_src = tls_dir().join("key2.pem");

    let port = free_port();
    let (set, reload) = run_tls_server_with_reload_for_test(port, 2, cert.clone(), key.clone());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // 1) Before any reload, a client trusting cert #1 completes PING/PONG (the boot cert).
        assert_ping_pong_trusting(port, &cert1).await;

        // 2) Replace the on-disk cert/key with pair #2, then reload (what the SIGHUP handler does).
        std::fs::copy(&cert2_src, &cert).expect("overwrite with cert2");
        std::fs::copy(&key2_src, &key).expect("overwrite with key2");
        ironcache::serve::reload_client_tls(&reload).expect("a valid reload succeeds");

        // 3) A NEW connection now presents cert #2: a client trusting ONLY cert #1 FAILS the
        //    handshake (untrusted issuer), proving new handshakes use the swapped cert.
        let old_only = connector_trusting(&cert1);
        let res = try_connect_once(port, &old_only).await;
        assert!(
            res.is_err(),
            "after a reload to cert #2, a client trusting only cert #1 must fail the handshake"
        );

        // 4) A client trusting cert #2 SUCCEEDS and gets +PONG -- proves the new cert is live AND
        //    the reload did NOT drop the server (it keeps serving on the same port).
        assert_ping_pong_trusting(port, &cert2_src).await;

        // 5) FAIL-SAFE: a reload with a MALFORMED cert file is REJECTED and the previous good cert
        //    (#2) stays live. Overwrite the cert with junk, reload -> Err, then a cert #2 client
        //    still completes PING/PONG and the server never crashed.
        std::fs::write(&cert, b"not a valid pem\n").expect("write junk cert");
        let bad = ironcache::serve::reload_client_tls(&reload);
        assert!(bad.is_err(), "a malformed cert reload must be rejected");
        assert_ping_pong_trusting(port, &cert2_src).await;

        // 6) FAIL-SAFE: a reload with a MISSING cert file is also rejected, old cert still serves.
        std::fs::remove_file(&cert).expect("remove cert to simulate a missing file");
        let missing = ironcache::serve::reload_client_tls(&reload);
        assert!(missing.is_err(), "a missing cert reload must be rejected");
        assert_ping_pong_trusting(port, &cert2_src).await;
    });

    set.shutdown_and_join().expect("clean shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
