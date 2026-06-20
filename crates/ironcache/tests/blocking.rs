// SPDX-License-Identifier: MIT OR Apache-2.0
//! BLOCKING command acceptance tests (PROD-9): BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/
//! BZPOPMAX/BZMPOP/WAIT over REAL sockets against the REAL `run_server` (the actual SO_REUSEPORT
//! thread-per-core topology + cross-shard coordinator).
//!
//! These exercise the whole PROD-9 path end to end: the serve-layer blocking interception, the
//! non-blocking FAST path (data present), the PARK on an empty key, the per-shard FIFO waiter
//! registry woken on a push (FIFO fairness), the runtime-timer-seam timeout (nil-array reply), the
//! in-MULTI no-block behavior, the RAII deregister on a closed parked connection, and WAIT on a
//! single node.
//!
//! They run with `shards == 1` so a parking connection and the pushing connection share the one
//! shard's store + waiter registry (the co-located/single-key case the deliverable fully covers).

use ironcache::test_support::run_server_for_test;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

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

fn boot(shards: usize) -> (ironcache_runtime::bootstrap::ShardSet, u16) {
    let port = free_port();
    let set = run_server_for_test(port, shards);
    (set, port)
}

// ---------------------------------------------------------------------------
// A minimal RESP2 reader (the blocking replies are all RESP2 arrays/bulks/integers/nil).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Resp {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
    Null,
}

async fn read_line(client: &mut TcpStream, buf: &mut Vec<u8>) -> Vec<u8> {
    loop {
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            let line = buf[..pos].to_vec();
            buf.drain(..pos + 2);
            return line;
        }
        let mut chunk = [0u8; 1024];
        let n = client.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed mid-reply");
        buf.extend_from_slice(&chunk[..n]);
    }
}

async fn read_bulk_body(client: &mut TcpStream, buf: &mut Vec<u8>, n: usize) -> Vec<u8> {
    while buf.len() < n + 2 {
        let mut chunk = [0u8; 1024];
        let got = client.read(&mut chunk).await.unwrap();
        assert!(got > 0, "connection closed mid-bulk");
        buf.extend_from_slice(&chunk[..got]);
    }
    let body = buf[..n].to_vec();
    buf.drain(..n + 2);
    body
}

async fn read_reply(client: &mut TcpStream, buf: &mut Vec<u8>) -> Resp {
    let line = read_line(client, buf).await;
    let (tag, rest) = line.split_first().unwrap();
    match tag {
        b'+' => Resp::Simple(rest.to_vec()),
        b'-' => Resp::Error(rest.to_vec()),
        b':' => Resp::Integer(std::str::from_utf8(rest).unwrap().parse().unwrap()),
        b'_' => Resp::Null,
        b'$' => {
            let len: i64 = std::str::from_utf8(rest).unwrap().parse().unwrap();
            if len < 0 {
                Resp::Bulk(None)
            } else {
                Resp::Bulk(Some(read_bulk_body(client, buf, len as usize).await))
            }
        }
        b'*' => {
            let len: i64 = std::str::from_utf8(rest).unwrap().parse().unwrap();
            if len < 0 {
                return Resp::Array(None);
            }
            let mut items = Vec::with_capacity(len as usize);
            for _ in 0..len {
                items.push(Box::pin(read_reply(client, buf)).await);
            }
            Resp::Array(Some(items))
        }
        other => panic!("unexpected RESP tag {:?}", *other as char),
    }
}

async fn send_cmd(client: &mut TcpStream, parts: &[&[u8]]) {
    let mut frame = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        frame.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        frame.extend_from_slice(p);
        frame.extend_from_slice(b"\r\n");
    }
    client.write_all(&frame).await.unwrap();
}

async fn send_and_read(client: &mut TcpStream, buf: &mut Vec<u8>, parts: &[&[u8]]) -> Resp {
    send_cmd(client, parts).await;
    read_reply(client, buf).await
}

/// Read with a timeout, returning None on timeout (used to assert "still parked, no reply yet").
async fn read_with_timeout(client: &mut TcpStream, buf: &mut Vec<u8>, ms: u64) -> Option<Resp> {
    tokio::time::timeout(Duration::from_millis(ms), read_reply(client, buf))
        .await
        .ok()
}

fn bulk(s: &[u8]) -> Resp {
    Resp::Bulk(Some(s.to_vec()))
}

/// A current-thread tokio runtime (the test client runs here; the server runs on its OWN
/// thread-per-core OS threads spawned by `run_server_for_test`, so a single-threaded client
/// runtime + a `LocalSet` is sufficient -- the same harness pattern the pub/sub tests use).
fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Run an async test body on a current-thread runtime + LocalSet (the pub/sub harness pattern).
fn run<F: std::future::Future<Output = ()>>(body: impl FnOnce() -> F) {
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, body());
}

// ===========================================================================
// FAST PATH: a blocking pop on a NON-empty key returns immediately (no park).
// ===========================================================================

#[test]
fn blpop_on_a_non_empty_list_returns_immediately() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();
        assert_eq!(
            send_and_read(&mut c, &mut buf, &[b"RPUSH", b"k", b"a", b"b"]).await,
            Resp::Integer(2)
        );
        // BLPOP with a non-empty list: immediate [key, element].
        let r = send_and_read(&mut c, &mut buf, &[b"BLPOP", b"k", b"0"]).await;
        assert_eq!(r, Resp::Array(Some(vec![bulk(b"k"), bulk(b"a")])));
    });
}

// ===========================================================================
// PARK + WAKE: a blocking pop on an EMPTY key PARKS, then returns when ANOTHER
// connection pushes (two connections, over real sockets).
// ===========================================================================

#[test]
fn blpop_parks_then_wakes_on_a_push_from_another_connection() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut blocker = connect_retry(port).await;
        let mut bbuf = Vec::new();
        let mut pusher = connect_retry(port).await;
        let mut pbuf = Vec::new();

        // The blocker issues BLPOP on an empty key: it PARKS (no reply within a short window).
        send_cmd(&mut blocker, &[b"BLPOP", b"q", b"0"]).await;
        assert!(
            read_with_timeout(&mut blocker, &mut bbuf, 200)
                .await
                .is_none(),
            "BLPOP on an empty key must PARK (no immediate reply)"
        );

        // The pusher LPUSHes: the parked BLPOP wakes and returns [key, element].
        assert_eq!(
            send_and_read(&mut pusher, &mut pbuf, &[b"RPUSH", b"q", b"hello"]).await,
            Resp::Integer(1)
        );
        let r = read_with_timeout(&mut blocker, &mut bbuf, 2000)
            .await
            .expect("the parked BLPOP must wake after the push");
        assert_eq!(r, Resp::Array(Some(vec![bulk(b"q"), bulk(b"hello")])));
    });
}

// ===========================================================================
// TIMEOUT: a blocking pop on an empty key returns the NIL-ARRAY after ~the timeout.
// ===========================================================================

#[test]
fn blpop_timeout_returns_nil_array() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();
        // BLPOP with a 0.3s timeout on an empty key. First prove it PARKS: a read with a SHORT
        // (100ms) deadline finds no reply yet (the command is blocking, not immediate).
        send_cmd(&mut c, &[b"BLPOP", b"none", b"0.3"]).await;
        assert!(
            read_with_timeout(&mut c, &mut buf, 100).await.is_none(),
            "the reply must come only after ~the timeout, not immediately"
        );
        // Then the nil-array arrives once the timeout elapses (a generous read deadline).
        let r = read_with_timeout(&mut c, &mut buf, 2000)
            .await
            .expect("the nil-array must arrive after the timeout");
        assert_eq!(r, Resp::Array(None), "timeout -> the nil array");
    });
}

// ===========================================================================
// FIFO FAIRNESS: two blockers on one key, one push -> the FIRST blocker wins.
// ===========================================================================

#[test]
fn fifo_fairness_first_blocker_wins_the_single_push() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut b1 = connect_retry(port).await;
        let mut b1buf = Vec::new();
        let mut b2 = connect_retry(port).await;
        let mut b2buf = Vec::new();
        let mut pusher = connect_retry(port).await;
        let mut pbuf = Vec::new();

        // b1 parks first.
        send_cmd(&mut b1, &[b"BLPOP", b"fair", b"0"]).await;
        assert!(read_with_timeout(&mut b1, &mut b1buf, 100).await.is_none());
        // b2 parks second (a short gap so the arrival order is deterministic).
        tokio::time::sleep(Duration::from_millis(50)).await;
        send_cmd(&mut b2, &[b"BLPOP", b"fair", b"0"]).await;
        assert!(read_with_timeout(&mut b2, &mut b2buf, 100).await.is_none());

        // ONE push: the LONGEST-waiting blocker (b1) wins; b2 stays parked.
        assert_eq!(
            send_and_read(&mut pusher, &mut pbuf, &[b"RPUSH", b"fair", b"x"]).await,
            Resp::Integer(1)
        );
        let r1 = read_with_timeout(&mut b1, &mut b1buf, 2000)
            .await
            .expect("b1 (longest-waiting) must win the single push");
        assert_eq!(r1, Resp::Array(Some(vec![bulk(b"fair"), bulk(b"x")])));
        // b2 is still parked (only one element was pushed).
        assert!(
            read_with_timeout(&mut b2, &mut b2buf, 200).await.is_none(),
            "b2 must stay parked: only one element was pushed and b1 took it"
        );
    });
}

// ===========================================================================
// BLMOVE blocking: parks on an empty src, wakes on a push, moves the element.
// ===========================================================================

#[test]
fn blmove_parks_then_moves_on_a_push() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut blocker = connect_retry(port).await;
        let mut bbuf = Vec::new();
        let mut pusher = connect_retry(port).await;
        let mut pbuf = Vec::new();

        send_cmd(
            &mut blocker,
            &[b"BLMOVE", b"src", b"dst", b"LEFT", b"RIGHT", b"0"],
        )
        .await;
        assert!(
            read_with_timeout(&mut blocker, &mut bbuf, 200)
                .await
                .is_none()
        );

        assert_eq!(
            send_and_read(&mut pusher, &mut pbuf, &[b"RPUSH", b"src", b"v"]).await,
            Resp::Integer(1)
        );
        let r = read_with_timeout(&mut blocker, &mut bbuf, 2000)
            .await
            .expect("BLMOVE must wake and move the element");
        assert_eq!(r, bulk(b"v"));
        // dst now holds the moved element.
        assert_eq!(
            send_and_read(&mut pusher, &mut pbuf, &[b"LRANGE", b"dst", b"0", b"-1"]).await,
            Resp::Array(Some(vec![bulk(b"v")]))
        );
    });
}

// ===========================================================================
// BZPOPMIN blocking + timeout.
// ===========================================================================

#[test]
fn bzpopmin_parks_then_wakes_on_a_zadd() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut blocker = connect_retry(port).await;
        let mut bbuf = Vec::new();
        let mut pusher = connect_retry(port).await;
        let mut pbuf = Vec::new();

        send_cmd(&mut blocker, &[b"BZPOPMIN", b"z", b"0"]).await;
        assert!(
            read_with_timeout(&mut blocker, &mut bbuf, 200)
                .await
                .is_none()
        );

        assert_eq!(
            send_and_read(&mut pusher, &mut pbuf, &[b"ZADD", b"z", b"5", b"m"]).await,
            Resp::Integer(1)
        );
        let r = read_with_timeout(&mut blocker, &mut bbuf, 2000)
            .await
            .expect("BZPOPMIN must wake on the ZADD");
        // [key, member, score]; the score is a RESP2 bulk "5".
        assert_eq!(
            r,
            Resp::Array(Some(vec![bulk(b"z"), bulk(b"m"), bulk(b"5")]))
        );
    });
}

#[test]
fn bzpopmin_timeout_returns_nil_array() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();
        let r = send_and_read(&mut c, &mut buf, &[b"BZPOPMIN", b"empty", b"0.2"]).await;
        assert_eq!(r, Resp::Array(None));
    });
}

// ===========================================================================
// WAIT 0 0 returns immediately with the current replica count (0 on a single node).
// ===========================================================================

#[test]
fn wait_zero_returns_immediately() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();
        send_cmd(&mut c, &[b"WAIT", b"0", b"0"]).await;
        // WAIT 0 must NOT block: the reply arrives within a short window (a block would time out).
        let r = read_with_timeout(&mut c, &mut buf, 500)
            .await
            .expect("WAIT 0 0 must return immediately, not block");
        assert_eq!(r, Resp::Integer(0), "WAIT 0 0 -> 0 replicas");
    });
}

#[test]
fn wait_for_replicas_times_out_to_the_current_count() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();
        // No replicas exist, so WAIT 1 BLOCKS until the timeout. First prove it parks (a short read
        // finds no reply), then the count (0) arrives after the 300ms timeout.
        send_cmd(&mut c, &[b"WAIT", b"1", b"300"]).await;
        assert!(
            read_with_timeout(&mut c, &mut buf, 100).await.is_none(),
            "WAIT N>0 with no replicas must block until the timeout"
        );
        let r = read_with_timeout(&mut c, &mut buf, 2000)
            .await
            .expect("WAIT must return the count after the timeout");
        assert_eq!(r, Resp::Integer(0));
    });
}

// ===========================================================================
// IN-MULTI: a blocking command inside MULTI/EXEC does NOT block (returns nil at once).
// ===========================================================================

#[test]
fn blpop_inside_multi_does_not_block() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();
        assert_eq!(
            send_and_read(&mut c, &mut buf, &[b"MULTI"]).await,
            Resp::Simple(b"OK".to_vec())
        );
        // The blocking command is QUEUED (+QUEUED), not run live.
        assert_eq!(
            send_and_read(&mut c, &mut buf, &[b"BLPOP", b"none", b"0"]).await,
            Resp::Simple(b"QUEUED".to_vec())
        );
        // EXEC runs the queued BLPOP NON-BLOCKING: the key is empty -> nil array, IMMEDIATELY
        // (a `0` "block forever" timeout must NOT hang inside EXEC). A short read deadline proves
        // it did not block: if EXEC had parked, this read would time out and the expect() panics.
        send_cmd(&mut c, &[b"EXEC"]).await;
        let r = read_with_timeout(&mut c, &mut buf, 500)
            .await
            .expect("BLPOP inside EXEC must not block");
        // EXEC returns an array with one element: the BLPOP's nil-array result.
        assert_eq!(r, Resp::Array(Some(vec![Resp::Array(None)])));
    });
}

// ===========================================================================
// RAII deregister: a parked connection that CLOSES leaves no leak; a later push
// does not panic / hang and a fresh blocker still works.
// ===========================================================================

#[test]
fn a_closed_parked_connection_deregisters_cleanly() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut pusher = connect_retry(port).await;
        let mut pbuf = Vec::new();

        // A blocker parks on `k`, then DROPS its socket while parked (a hard close).
        {
            let mut blocker = connect_retry(port).await;
            let mut bbuf = Vec::new();
            send_cmd(&mut blocker, &[b"BLPOP", b"k", b"0"]).await;
            assert!(
                read_with_timeout(&mut blocker, &mut bbuf, 200)
                    .await
                    .is_none()
            );
            // Drop the blocker socket: the serve loop observes the peer close while parked and the
            // RAII guard deregisters the waiter (no leak, no waking a dead connection).
            drop(blocker);
        }
        // Give the server a moment to observe the close + deregister.
        tokio::time::sleep(Duration::from_millis(150)).await;

        // A push to the same key must NOT panic / hang (the dead waiter is gone). The element stays
        // in the list (no one is waiting), so a fresh LRANGE sees it.
        assert_eq!(
            send_and_read(&mut pusher, &mut pbuf, &[b"RPUSH", b"k", b"v1"]).await,
            Resp::Integer(1)
        );
        assert_eq!(
            send_and_read(&mut pusher, &mut pbuf, &[b"LRANGE", b"k", b"0", b"-1"]).await,
            Resp::Array(Some(vec![bulk(b"v1")])),
            "the pushed element stays (the closed waiter was deregistered, not delivered to)"
        );

        // A FRESH blocker on a NEW key still parks + wakes correctly (the registry is healthy).
        let mut blocker2 = connect_retry(port).await;
        let mut b2buf = Vec::new();
        send_cmd(&mut blocker2, &[b"BLPOP", b"k2", b"0"]).await;
        assert!(
            read_with_timeout(&mut blocker2, &mut b2buf, 200)
                .await
                .is_none()
        );
        assert_eq!(
            send_and_read(&mut pusher, &mut pbuf, &[b"RPUSH", b"k2", b"again"]).await,
            Resp::Integer(1)
        );
        let r = read_with_timeout(&mut blocker2, &mut b2buf, 2000)
            .await
            .expect("a fresh blocker must still wake after a prior connection closed mid-park");
        assert_eq!(r, Resp::Array(Some(vec![bulk(b"k2"), bulk(b"again")])));
    });
}

// ===========================================================================
// Arity / parse errors come back immediately (no park).
// ===========================================================================

#[test]
fn blocking_parse_errors_are_immediate() {
    run(|| async {
        let (_set, port) = boot(1);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();
        // Missing timeout (arity).
        match send_and_read(&mut c, &mut buf, &[b"BLPOP", b"k"]).await {
            Resp::Error(_) => {}
            other => panic!("BLPOP with no timeout must be an arity error, got {other:?}"),
        }
        // Negative timeout.
        match send_and_read(&mut c, &mut buf, &[b"BLPOP", b"k", b"-1"]).await {
            Resp::Error(e) => assert!(
                String::from_utf8_lossy(&e).contains("negative"),
                "negative timeout error, got {:?}",
                String::from_utf8_lossy(&e)
            ),
            other => panic!("negative timeout must error, got {other:?}"),
        }
    });
}
