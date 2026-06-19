// SPDX-License-Identifier: MIT OR Apache-2.0
//! Connection / output safety-limit regression tests (PROD-SAFETY #3/#4/#5).
//!
//! A production-readiness audit found three connection-side DoS gaps the cache could not protect
//! itself from:
//!
//! - #3 NO `maxclients` cap: the accept loop NEVER rejected, so an attacker could exhaust
//!   connections (file descriptors / memory) without bound.
//! - #4 the `timeout` idle-timeout was PARSED but UNENFORCED: an idle connection was never closed,
//!   so idle connections accumulated.
//! - #5 NO output-buffer-limit: a slow consumer / huge reply / pipelined flood could grow a
//!   connection's pending output unbounded (server-memory DoS).
//!
//! These tests boot the REAL server over real sockets and assert each limit is now enforced: the
//! Nth+1 connection over the cap gets `-ERR max number of clients reached` and is closed; a
//! connection idle past the (short, test-configured) timeout is closed while an ACTIVE one is not;
//! and a connection whose accumulated reply exceeds the (small, test-configured) output-buffer cap
//! is closed without unbounded growth. The defaults (no limits beyond the high ceilings) leave the
//! behavior unchanged, covered by the byte-for-byte default-path test at the end.

use ironcache::test_support::run_server_with_limits_for_test;
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

/// Send one command, read ONE socket read of reply, return the raw bytes.
async fn cmd(client: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut buf = [0u8; 4096];
    let n = client.read(&mut buf).await.unwrap();
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

/// PROD-SAFETY #3: with `maxclients = 2`, the first two connections are admitted and serve PING,
/// but the THIRD (the Nth+1 over the cap) gets `-ERR max number of clients reached` and is closed.
/// After one of the admitted connections closes, a new connection is admitted again (the released
/// slot frees capacity). Single shard so the global cap is exercised deterministically.
#[test]
fn maxclients_rejects_connections_over_the_cap() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        // maxclients = 2; no idle timeout; a high output cap (these two off / generous here).
        let server = run_server_with_limits_for_test(port, 1, 2, 0, 0);

        // Two admitted connections: each serves PING (proving they are live, not capped).
        let mut c1 = connect_retry(port).await;
        assert_eq!(cmd(&mut c1, &["PING"]).await, b"+PONG\r\n", "1st admitted");
        let mut c2 = connect_retry(port).await;
        assert_eq!(cmd(&mut c2, &["PING"]).await, b"+PONG\r\n", "2nd admitted");

        // The THIRD connection is over the cap: it receives the rejection error, then EOF (closed).
        let mut c3 = connect_retry(port).await;
        let mut buf = [0u8; 256];
        let n = c3.read(&mut buf).await.unwrap();
        assert_eq!(
            &buf[..n],
            b"-ERR max number of clients reached\r\n",
            "the 3rd connection over maxclients must be rejected with the byte-exact error"
        );
        // The server closed it: the next read returns 0 (EOF).
        let n2 = c3.read(&mut buf).await.unwrap();
        assert_eq!(n2, 0, "the rejected connection is closed by the server");

        // Free a slot: drop c2 and wait for the server to observe the close (release the gate).
        drop(c2);
        // Retry a fresh connection until it is admitted (it must succeed once the slot is freed).
        let mut admitted = None;
        for _ in 0..100 {
            let mut c = connect_retry(port).await;
            // Probe with PING: an admitted connection answers +PONG; a still-capped one is rejected.
            c.write_all(&encode_args(&["PING"])).await.unwrap();
            let mut b = [0u8; 256];
            let n = c.read(&mut b).await.unwrap();
            if &b[..n] == b"+PONG\r\n" {
                admitted = Some(c);
                break;
            }
            drop(c);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            admitted.is_some(),
            "a new connection must be admitted after a slot is freed (the gate released it)"
        );

        drop(c1);
        drop(admitted);
        server.shutdown_and_join().unwrap();
    });
}

/// PROD-SAFETY #4: with a SHORT idle `timeout`, a connection that sits idle past the timeout is
/// CLOSED by the server (a clean EOF), while a connection that stays ACTIVE (keeps issuing commands
/// within the window) is NOT closed. The idle timeout uses the Runtime timer seam with a per-command
/// deadline reset, so the active connection's deadline keeps re-arming.
#[test]
fn idle_timeout_closes_idle_connection_but_not_an_active_one() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        // A 1-second idle timeout (short for the test); maxclients/output off.
        let server = run_server_with_limits_for_test(port, 1, 0, 1, 0);

        // (a) IDLE connection: connect, PING once, then sit idle past the timeout -> server closes.
        let mut idle = connect_retry(port).await;
        assert_eq!(cmd(&mut idle, &["PING"]).await, b"+PONG\r\n");
        // Wait comfortably past the 1s idle timeout, then read: the server should have closed it.
        let closed = tokio::time::timeout(Duration::from_secs(4), async {
            let mut buf = [0u8; 64];
            idle.read(&mut buf).await
        })
        .await
        .expect("the idle connection should be closed well within the outer timeout");
        assert_eq!(
            closed.unwrap(),
            0,
            "an idle connection past timeout_secs must be closed (EOF) by the server"
        );

        // (b) ACTIVE connection: keep issuing PINGs faster than the timeout for longer than the
        // timeout window. It must stay open the whole time (the deadline re-arms each command).
        let mut active = connect_retry(port).await;
        for _ in 0..6 {
            assert_eq!(
                cmd(&mut active, &["PING"]).await,
                b"+PONG\r\n",
                "an active connection (command within the idle window) must NOT be closed"
            );
            tokio::time::sleep(Duration::from_millis(400)).await;
        }

        drop(active);
        server.shutdown_and_join().unwrap();
    });
}

/// PROD-SAFETY #5: with a SMALL output-buffer cap, a single reply (or a pipelined batch) whose
/// rendered output exceeds the cap causes the server to CLOSE the connection rather than buffer it
/// unbounded. We set a tiny cap, store a value LARGER than the cap, then GET it: the reply would
/// exceed the cap, so the connection is closed (EOF) without the server growing its output without
/// bound. A reply UNDER the cap round-trips normally (the cap does not break legitimate replies).
#[test]
fn output_buffer_limit_closes_a_connection_over_the_cap() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        // A small 4 KiB output cap; maxclients/timeout off.
        let cap: u64 = 4096;
        let server = run_server_with_limits_for_test(port, 1, 0, 0, cap);

        let mut c = connect_retry(port).await;

        // A reply UNDER the cap round-trips fine: SET a small value, GET it back.
        assert_eq!(cmd(&mut c, &["SET", "small", "hello"]).await, b"+OK\r\n");
        assert_eq!(
            cmd(&mut c, &["GET", "small"]).await,
            b"$5\r\nhello\r\n",
            "a reply under the output cap is unaffected"
        );

        // Store a value LARGER than the cap, then GET it: the reply (bulk header + ~64 KiB body)
        // exceeds the 4 KiB cap, so the server closes the connection instead of buffering it.
        let big = "x".repeat(64 * 1024);
        assert_eq!(cmd(&mut c, &["SET", "big", &big]).await, b"+OK\r\n");
        c.write_all(&encode_args(&["GET", "big"])).await.unwrap();
        // Read until EOF (or the outer timeout); the server must NOT deliver the full oversized
        // reply -- it closes the connection. We bound the total bytes we are willing to read to a
        // little over the cap; receiving far more than the cap would mean the limit was not enforced.
        let result = tokio::time::timeout(Duration::from_secs(4), async {
            let mut total = 0usize;
            let mut buf = [0u8; 8192];
            loop {
                let n = c.read(&mut buf).await.unwrap();
                if n == 0 {
                    break; // EOF: the server closed the connection (the limit fired).
                }
                total += n;
                if total > (cap as usize) * 4 {
                    // Far more than the cap arrived -> the limit was NOT enforced.
                    return Err(total);
                }
            }
            Ok(total)
        })
        .await
        .expect("the over-cap connection must be closed well within the outer timeout");
        let delivered = result
            .expect("the server must close the connection, not deliver the full oversized reply");
        assert!(
            delivered <= (cap as usize) * 4,
            "the server must not deliver an unbounded oversized reply (delivered {delivered} bytes \
             for a {cap}-byte cap); the connection should be closed"
        );

        server.shutdown_and_join().unwrap();
    });
}

/// DEFAULTS UNCHANGED: with NO connection limits beyond the high defaults (maxclients disabled here
/// to prove the unlimited path, timeout off, output cap off), many connections all serve normally
/// and stay open -- the default posture is byte-for-byte the pre-fix behavior. This guards against a
/// regression where the new gates would reject / close a legitimate default-config connection.
#[test]
fn defaults_do_not_reject_or_close_legitimate_connections() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        // All three limits DISABLED (0): the pre-fix unlimited behavior.
        let server = run_server_with_limits_for_test(port, 2, 0, 0, 0);

        // Open many connections; every one serves PING + a SET/GET round-trip (none rejected).
        let mut conns = Vec::new();
        for i in 0..16 {
            let mut c = connect_retry(port).await;
            assert_eq!(cmd(&mut c, &["PING"]).await, b"+PONG\r\n", "conn {i} PING");
            let key = format!("k{i}");
            assert_eq!(cmd(&mut c, &["SET", &key, "v"]).await, b"+OK\r\n");
            assert_eq!(cmd(&mut c, &["GET", &key]).await, b"$1\r\nv\r\n");
            conns.push(c);
        }
        // After a brief pause (longer than any default would matter), they are STILL open: a PING
        // on the first one still answers (no idle-timeout close on the default path).
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            cmd(&mut conns[0], &["PING"]).await,
            b"+PONG\r\n",
            "default-config connections stay open (no idle timeout)"
        );

        drop(conns);
        server.shutdown_and_join().unwrap();
    });
}
