// SPDX-License-Identifier: MIT OR Apache-2.0
//! Connection / output safety-limit regression tests (PROD-SAFETY #3/#4/#5 + #528/#529).
//!
//! A production-readiness audit found five connection-side DoS gaps the cache could not protect
//! itself from:
//!
//! - #3 NO `maxclients` cap: the accept loop NEVER rejected, so an attacker could exhaust
//!   connections (file descriptors / memory) without bound.
//! - #4 the `timeout` idle-timeout was PARSED but UNENFORCED: an idle connection was never closed,
//!   so idle connections accumulated.
//! - #5 NO output-buffer-limit: a slow consumer / huge reply / pipelined flood could grow a
//!   connection's pending output unbounded (server-memory DoS).
//! - #528 NO query-buffer-limit: a client that announces a large multibulk (`*<huge>\r\n`) and then
//!   DRIBBLES the elements forced unbounded PRE-AUTH inbound buffering (the frame never completes,
//!   so the read buffer grew without bound).
//! - #529 the output-buffer-limit fired only POST-batch: a single pipelined batch of large-reply
//!   commands could accumulate unbounded reply bytes and OOM the host before the check ran.
//!
//! These tests boot the REAL server over real sockets and assert each limit is now enforced: the
//! Nth+1 connection over the cap gets `-ERR max number of clients reached` and is closed; a
//! connection idle past the (short, test-configured) timeout is closed while an ACTIVE one is not;
//! a connection whose accumulated reply exceeds the (small, test-configured) output-buffer cap is
//! closed without unbounded growth (both at the single-reply boundary AND mid-pipelined-batch, #529);
//! and a connection that dribbles a never-completing multibulk past the (small, test-configured)
//! query-buffer cap is closed (#528). Both new caps are exercised via boot config AND a live
//! `CONFIG SET`. The defaults (no limits beyond the high ceilings) leave the behavior unchanged,
//! covered by the byte-for-byte default-path test at the end.

use ironcache::test_support::{
    run_server_with_limits_for_test, run_server_with_query_buffer_limit_for_test,
};
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

/// Dribble a never-completing multibulk into `client` and return the total inbound bytes the client
/// managed to WRITE before the server closed the connection (a write error / a read EOF). The client
/// first announces a large multibulk header (`*1048576\r\n`, exactly the decoder's `max_multibulk`
/// cap so it is ACCEPTED, not a protocol error), then trickles single-char bulk elements that never
/// complete the array -- the server must keep buffering every partial byte, so the query-buffer cap
/// is what stops it. We keep writing in modest chunks and probe for the close after each, bounding
/// the total we are willing to write to `max_bytes`; exceeding that means the cap was NOT enforced.
async fn dribble_until_closed(client: &mut TcpStream, max_bytes: usize) -> Result<usize, usize> {
    // Announce a huge (but at-cap, so accepted) multibulk that will never be completed.
    if client.write_all(b"*1048576\r\n").await.is_err() {
        return Ok(10);
    }
    // ~3 KiB of dribble per chunk (512 x the 6-byte "$1\r\nx\r\n" element).
    let mut chunk = Vec::new();
    for _ in 0..512 {
        chunk.extend_from_slice(b"$1\r\nx\r\n");
    }
    let mut written = 10usize;
    loop {
        if client.write_all(&chunk).await.is_err() {
            return Ok(written); // the server closed the socket (write failed): the cap fired.
        }
        written += chunk.len();
        // Probe for a close without blocking: a short read. EOF (0) or a read error = closed; a
        // read timeout (`Err`) or a stray byte (`Ok(Ok(n>0))`, not expected for a dribble) means the
        // connection is still open, so keep dribbling.
        let mut buf = [0u8; 64];
        if let Ok(Ok(0) | Err(_)) =
            tokio::time::timeout(Duration::from_millis(50), client.read(&mut buf)).await
        {
            return Ok(written); // EOF / read error: the server closed the connection.
        }
        if written > max_bytes {
            return Err(written); // far past the cap and still open -> the limit was NOT enforced.
        }
    }
}

/// #528: with a SMALL query-buffer cap, a connection that announces a large multibulk and then
/// DRIBBLES the elements (never completing the frame) is CLOSED once its accumulated inbound buffer
/// crosses the cap, rather than being allowed to buffer unbounded pre-auth memory. The connection is
/// dropped well before an unbounded amount of dribble is accepted.
#[test]
fn query_buffer_limit_closes_a_slow_dribble_multibulk() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        // A small 16 KiB query-buffer cap; single shard.
        let cap: u64 = 16 * 1024;
        let server = run_server_with_query_buffer_limit_for_test(port, 1, cap);

        let mut c = connect_retry(port).await;
        // Sanity: a normal command still works under the cap (the cap does not break legitimate use).
        assert_eq!(cmd(&mut c, &["PING"]).await, b"+PONG\r\n");

        // Dribble a never-completing multibulk: the server must close the connection at the cap. We
        // bound the bytes we will write to 8x the cap; the server should close far sooner.
        let bound = (cap as usize) * 8;
        let written = dribble_until_closed(&mut c, bound)
            .await
            .expect("the dribbling connection must be closed at the query-buffer cap");
        assert!(
            written <= bound,
            "the server must close a dribbled never-completing multibulk near the query cap \
             (wrote {written} bytes for a {cap}-byte cap); the connection should be closed"
        );

        server.shutdown_and_join().unwrap();
    });
}

/// #528 (CONFIG SET): the query-buffer cap is LIVE-settable. Boot with the cap OFF (the high
/// default), then `CONFIG SET query-buffer-limit` to a small value on the SAME connection; the very
/// next dribble is bounded and the connection is closed. Proves the cap is runtime-settable over a
/// real socket (not only boot config).
#[test]
fn query_buffer_limit_is_config_settable_and_then_closes_a_dribble() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        // Boot with the query cap DISABLED (0): only a live CONFIG SET arms it.
        let server = run_server_with_query_buffer_limit_for_test(port, 1, 0);

        let mut c = connect_retry(port).await;
        // Arm a small 16 KiB cap live; the setter accepts a human size and replies +OK.
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "query-buffer-limit", "16kb"]).await,
            b"+OK\r\n",
            "CONFIG SET query-buffer-limit must apply and ack"
        );
        // CONFIG GET reflects the live value (reported in bytes).
        let getreply = cmd(&mut c, &["CONFIG", "GET", "query-buffer-limit"]).await;
        assert!(
            getreply
                .windows(b"query-buffer-limit".len())
                .any(|w| w == b"query-buffer-limit")
                && getreply.windows(b"16384".len()).any(|w| w == b"16384"),
            "CONFIG GET must report the live query-buffer-limit in bytes, got {getreply:?}"
        );

        // Now the dribble is bounded and the connection is closed at the live cap.
        let cap = 16 * 1024usize;
        let bound = cap * 8;
        let written = dribble_until_closed(&mut c, bound)
            .await
            .expect("the dribbling connection must be closed at the live query-buffer cap");
        assert!(
            written <= bound,
            "a CONFIG SET query-buffer-limit must bound a subsequent dribble (wrote {written} bytes)"
        );

        server.shutdown_and_join().unwrap();
    });
}

/// #529: with a SMALL output cap, a SINGLE pipelined batch of many large-reply commands is cut off
/// MID-BATCH at the cap: the server stops accumulating replies and closes the connection rather than
/// building the whole (potentially host-OOMing) batch output first. From the wire this reads as the
/// connection being closed with far fewer than the full batch's reply bytes delivered. The prior
/// code only checked the cap POST-batch, so it would have assembled the entire batch in memory first.
#[test]
fn output_buffer_limit_cuts_off_a_pipelined_batch_mid_batch() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        // A small 4 KiB output cap; maxclients/timeout off.
        let cap: u64 = 4096;
        let server = run_server_with_limits_for_test(port, 1, 0, 0, cap);

        let mut c = connect_retry(port).await;

        // Store one moderate value (~512 bytes, comfortably UNDER the cap so no single reply trips
        // it): the batch must cross the cap only by ACCUMULATION across commands.
        let val = "y".repeat(512);
        assert_eq!(cmd(&mut c, &["SET", "k", &val]).await, b"+OK\r\n");

        // Pipeline 200 GETs of it in ONE write: ~200 * ~520 bytes ~= 104 KiB of reply, which crosses
        // the 4 KiB cap after ~8 commands. The intra-batch check (#529) must cut the batch off there
        // and close the connection, so the client sees EOF with far less than the full 104 KiB.
        let mut batch = Vec::new();
        for _ in 0..200 {
            batch.extend_from_slice(&encode_args(&["GET", "k"]));
        }
        c.write_all(&batch).await.unwrap();

        // Read until EOF (or the outer timeout), bounding how much we will accept. Receiving the full
        // oversized batch would mean the mid-batch cut did not fire.
        let result = tokio::time::timeout(Duration::from_secs(4), async {
            let mut total = 0usize;
            let mut buf = [0u8; 8192];
            loop {
                let n = c.read(&mut buf).await.unwrap();
                if n == 0 {
                    break; // EOF: the server closed the connection (the cap fired mid-batch).
                }
                total += n;
                if total > (cap as usize) * 4 {
                    return Err(total); // far more than the cap arrived -> not cut off mid-batch.
                }
            }
            Ok(total)
        })
        .await
        .expect("the over-cap pipelined batch must close the connection within the outer timeout");
        let delivered = result
            .expect("the server must cut the batch off mid-flight, not deliver the full batch reply");
        assert!(
            delivered <= (cap as usize) * 4,
            "a pipelined batch over the output cap must be cut off mid-batch (delivered {delivered} \
             bytes for a {cap}-byte cap); the connection should be closed"
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
