// SPDX-License-Identifier: MIT OR Apache-2.0
//! `CLIENT PAUSE WRITE` write-only regression tests (#388).
//!
//! A live dev test exposed a real engine bug: `CLIENT PAUSE <ms> WRITE` is supposed to pause only
//! WRITE commands (Redis keeps reads + admin flowing), but the serve loop stalled on the
//! WRITE-flag-AGNOSTIC post-batch pause check, so a WRITE pause conservatively stalled the ENTIRE
//! serve loop, including reads, PING, INFO and SAVE. That broke `ironcache upgrade`'s lossless
//! write-freeze: the upgrade issues `CLIENT PAUSE WRITE` then `SAVE`, but the SAVE was stalled by
//! the very pause it set, so the SAVE timed out and the upgrade safe-aborted.
//!
//! These tests boot the REAL server over real sockets and assert the FIXED semantics:
//!
//! - Under `CLIENT PAUSE <bigms> WRITE`, a GET / PING / INFO / SAVE returns PROMPTLY (not stalled)
//!   while a SET is HELD until `CLIENT UNPAUSE` -- exactly the SAVE-must-pass case that deadlocked
//!   the upgrade. This is the test that would have caught the bug.
//! - The regression guard: under `CLIENT PAUSE <bigms> ALL`, even a GET is HELD (the ALL pause still
//!   stalls everything), and `CLIENT UNPAUSE` releases it.

use ironcache::test_support::run_server_for_test;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Grab a free TCP port by binding an ephemeral listener and dropping it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Connect with a few short retries (the shards bind asynchronously after `run_server`).
async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = s.set_nodelay(true);
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never came up on port {port}");
}

/// Encode a RESP2 command array from string args.
fn encode_args(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

/// Send one command and read ONE socket read of reply, returning the raw bytes.
async fn cmd(client: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut buf = [0u8; 8192];
    let n = client.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

/// Send a command and assert a reply ARRIVES within `within` (the command was NOT stalled),
/// returning the raw reply bytes. Fails the test if no reply arrives in time.
async fn cmd_prompt(client: &mut TcpStream, args: &[&str], within: Duration) -> Vec<u8> {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut buf = [0u8; 8192];
    let n = tokio::time::timeout(within, client.read(&mut buf))
        .await
        .unwrap_or_else(|_| panic!("{args:?} did not return promptly (it was stalled)"))
        .unwrap();
    buf[..n].to_vec()
}

fn rt() -> (tokio::runtime::Runtime, tokio::task::LocalSet) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    (rt, local)
}

/// THE BUG-CATCHING TEST (#388). Under a WRITE-only `CLIENT PAUSE`, a GET / PING / INFO / SAVE must
/// return PROMPTLY (reads + admin flow through), while a SET is HELD until `CLIENT UNPAUSE`. This is
/// the exact `CLIENT PAUSE WRITE` then `SAVE` sequence the lossless upgrade runs: before the fix the
/// SAVE deadlocked behind the pause it had just set; after the fix the SAVE returns at once and only
/// the writes are frozen.
#[test]
fn write_pause_allows_reads_and_save_but_holds_writes() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);

        // The PAUSER connection issues the node-wide CLIENT PAUSE. A SECOND connection (the worker)
        // runs the reads/admin/writes, so the test is not just serializing on one socket.
        let mut pauser = connect_retry(port).await;
        let mut worker = connect_retry(port).await;

        // Seed a key so the GET below returns a value (a real read, not a miss path).
        assert_eq!(cmd(&mut worker, &["SET", "k", "v"]).await, b"+OK\r\n");

        // CLIENT PAUSE 60000 WRITE: a long WRITE-only pause. +OK and the window is now active.
        assert_eq!(
            cmd(&mut pauser, &["CLIENT", "PAUSE", "60000", "WRITE"]).await,
            b"+OK\r\n",
            "CLIENT PAUSE WRITE is accepted"
        );

        // READS + ADMIN must flow through PROMPTLY under a WRITE pause (this is the fix). A generous
        // 2s bound: if any of these is stalled by the pause, it will not return inside 2s (the window
        // is 60s) and the test fails with a clear message.
        let within = Duration::from_secs(2);
        assert_eq!(
            cmd_prompt(&mut worker, &["GET", "k"], within).await,
            b"$1\r\nv\r\n",
            "GET must NOT be stalled by a WRITE pause"
        );
        assert_eq!(
            cmd_prompt(&mut worker, &["PING"], within).await,
            b"+PONG\r\n",
            "PING must NOT be stalled by a WRITE pause"
        );
        // INFO returns a bulk string; just assert SOMETHING came back promptly (not stalled).
        let info = cmd_prompt(&mut worker, &["INFO"], within).await;
        assert!(
            info.starts_with(b"$"),
            "INFO must NOT be stalled by a WRITE pause (got {:?})",
            String::from_utf8_lossy(&info)
        );
        // SAVE is the load-bearing case for the upgrade write-freeze: it MUST pass while WRITE-paused.
        assert_eq!(
            cmd_prompt(&mut worker, &["SAVE"], within).await,
            b"+OK\r\n",
            "SAVE must NOT be stalled by a WRITE pause (this is the upgrade write-freeze fix)"
        );

        // A WRITE (SET) must be HELD: it does NOT return while the WRITE pause is active. We send it
        // and assert NO reply arrives within a short window (the command is stalling in the serve
        // loop's per-command write-pause gate).
        worker
            .write_all(&encode_args(&["SET", "k", "v2"]))
            .await
            .unwrap();
        let mut buf = [0u8; 256];
        let held = tokio::time::timeout(Duration::from_millis(700), worker.read(&mut buf)).await;
        assert!(
            held.is_err(),
            "a SET must be HELD (not reply) while a WRITE pause is active"
        );

        // CLIENT UNPAUSE (from the pauser connection) releases the held write: the SET now completes.
        assert_eq!(
            cmd(&mut pauser, &["CLIENT", "UNPAUSE"]).await,
            b"+OK\r\n",
            "CLIENT UNPAUSE is accepted"
        );
        // The previously-held SET's reply now arrives (read the same `worker` socket).
        let n = tokio::time::timeout(Duration::from_secs(2), worker.read(&mut buf))
            .await
            .expect("the held SET must complete promptly after UNPAUSE")
            .unwrap();
        assert_eq!(
            &buf[..n],
            b"+OK\r\n",
            "the held SET completes once the WRITE pause is lifted"
        );
        // The write actually applied.
        assert_eq!(cmd(&mut worker, &["GET", "k"]).await, b"$2\r\nv2\r\n");

        drop(pauser);
        drop(worker);
        server.shutdown_and_join().unwrap();
    });
}

/// REGRESSION GUARD: an ALL pause (`CLIENT PAUSE <ms>` / `... ALL`) still HOLDS EVERYTHING, including
/// reads. Even a GET does not return until the window is lifted by `CLIENT UNPAUSE`. This guards the
/// unchanged ALL behavior so the write-only fix did not weaken an ALL pause into a write-only one.
#[test]
fn all_pause_holds_even_reads() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);

        let mut pauser = connect_retry(port).await;
        let mut worker = connect_retry(port).await;
        assert_eq!(cmd(&mut worker, &["SET", "k", "v"]).await, b"+OK\r\n");

        // CLIENT PAUSE 60000 ALL: a long ALL pause (ALL is also the default with no kind arg).
        assert_eq!(
            cmd(&mut pauser, &["CLIENT", "PAUSE", "60000", "ALL"]).await,
            b"+OK\r\n"
        );

        // Even a GET is HELD under an ALL pause: send it and assert no reply within a short window.
        worker.write_all(&encode_args(&["GET", "k"])).await.unwrap();
        let mut buf = [0u8; 256];
        let held = tokio::time::timeout(Duration::from_millis(700), worker.read(&mut buf)).await;
        assert!(
            held.is_err(),
            "a GET must be HELD while an ALL pause is active (the ALL pause stalls reads too)"
        );

        // UNPAUSE releases the held GET.
        assert_eq!(cmd(&mut pauser, &["CLIENT", "UNPAUSE"]).await, b"+OK\r\n");
        let n = tokio::time::timeout(Duration::from_secs(2), worker.read(&mut buf))
            .await
            .expect("the held GET must complete promptly after UNPAUSE")
            .unwrap();
        assert_eq!(
            &buf[..n],
            b"$1\r\nv\r\n",
            "the held GET completes once the ALL pause is lifted"
        );

        drop(pauser);
        drop(worker);
        server.shutdown_and_join().unwrap();
    });
}

/// A WRITE pause that EXPIRES on its own (no UNPAUSE) releases the held write. We use a SHORT window
/// so the test does not wait long: a SET issued during the window is held, then completes once the
/// window self-expires -- the natural end of the upgrade write-freeze when the new process boots.
#[test]
fn write_pause_self_expiry_releases_the_write() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);

        let mut pauser = connect_retry(port).await;
        let mut worker = connect_retry(port).await;

        // A 700ms WRITE pause: short enough that the test does not wait long, long enough that the
        // SET below is observably held first.
        assert_eq!(
            cmd(&mut pauser, &["CLIENT", "PAUSE", "700", "WRITE"]).await,
            b"+OK\r\n"
        );

        // The SET is held initially (no reply within 200ms, well inside the 700ms window).
        worker
            .write_all(&encode_args(&["SET", "k", "v"]))
            .await
            .unwrap();
        let mut buf = [0u8; 256];
        let held = tokio::time::timeout(Duration::from_millis(200), worker.read(&mut buf)).await;
        assert!(
            held.is_err(),
            "the SET is held while the WRITE pause is open"
        );

        // Within ~2s (comfortably past the 700ms window + the ~50ms poll quantum) the window expires
        // on its own and the held SET completes -- no UNPAUSE needed.
        let n = tokio::time::timeout(Duration::from_secs(2), worker.read(&mut buf))
            .await
            .expect("the held SET must complete once the WRITE window self-expires")
            .unwrap();
        assert_eq!(
            &buf[..n],
            b"+OK\r\n",
            "the held SET completes when the WRITE pause window elapses"
        );

        drop(pauser);
        drop(worker);
        server.shutdown_and_join().unwrap();
    });
}
