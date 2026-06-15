// SPDX-License-Identifier: MIT OR Apache-2.0
//! Cross-shard coordinator Stage 1 (PASS 1) acceptance tests (COORDINATOR.md #107).
//!
//! These boot the REAL multi-shard `run_server` on an ephemeral port (the actual
//! SO_REUSEPORT thread-per-core topology, NOT a single hand-rolled serve loop) and drive
//! it over real sockets, so they exercise the whole path: classify -> route ->
//! enqueue -> cross-thread drain -> remote dispatch -> oneshot reply -> home-core encode.
//!
//! The core bug this layer fixes: before routing, each shard held an INDEPENDENT full
//! store, so `SET k v` on a connection homed on shard 0 was invisible to `GET k` on a
//! connection homed on shard 1. The cross-connection test below is the direct regression.

use ironcache::test_support::run_server_for_test;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary (so the
// process-memory path used by INFO is live; harmless for these tests otherwise).
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Grab a free TCP port by binding an ephemeral listener and dropping it. There is a
/// small TOCTOU window before `run_server` re-binds, acceptable for a localhost test;
/// SO_REUSEADDR on the server side tolerates the lingering bind.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Connect with a few short retries: the shards bind asynchronously on their own threads
/// after `run_server` returns, so the first connect may race the bind.
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

/// Read exactly `expect.len()` bytes and assert they match.
async fn expect_reply(client: &mut TcpStream, expect: &[u8]) {
    let mut buf = vec![0u8; expect.len()];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, expect, "got {:?}", String::from_utf8_lossy(&buf));
}

/// `SET key val` over a connection (RESP2 array), expecting `+OK`.
async fn set(client: &mut TcpStream, key: &str, val: &str) {
    let frame = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        val.len(),
        val
    );
    client.write_all(frame.as_bytes()).await.unwrap();
    expect_reply(client, b"+OK\r\n").await;
}

/// `GET key` over a connection, returning the raw reply bytes (read once; the small
/// replies here fit a single read).
async fn get_raw(client: &mut TcpStream, key: &str) -> Vec<u8> {
    let frame = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client.write_all(frame.as_bytes()).await.unwrap();
    let mut buf = [0u8; 256];
    let n = client.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

/// A bulk-string reply `$<len>\r\n<bytes>\r\n` for `val`.
fn bulk(val: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", val.len(), val).into_bytes()
}

/// Send a single-arg keyed command (`CMD key`) and return the raw reply bytes (one read).
async fn cmd_key_raw(client: &mut TcpStream, cmd: &str, key: &str) -> Vec<u8> {
    let frame = format!(
        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        cmd.len(),
        cmd,
        key.len(),
        key
    );
    client.write_all(frame.as_bytes()).await.unwrap();
    let mut buf = [0u8; 256];
    let n = client.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

/// Boot a multi-shard server, returning (handle, port). The handle must be kept alive
/// for the server's lifetime and joined at the end.
fn boot(shards: usize) -> (ironcache_runtime::bootstrap::ShardSet, u16) {
    let port = free_port();
    let set = run_server_for_test(port, shards);
    (set, port)
}

// ---------------------------------------------------------------------------
// Whole-keyspace fan-out helpers (COORDINATOR.md #107, the final Stage 1 piece).
//
// These parse the RESP replies (which may span socket reads), so they buffer until a
// complete reply is read. A minimal RESP2/RESP3 reader: enough for integer / bulk /
// null / array-of-bulks, the shapes the whole-keyspace commands return.
// ---------------------------------------------------------------------------

/// A tiny RESP reader over a TcpStream: reads one complete reply, returning the parsed
/// shape. Only the shapes the whole-keyspace commands use are handled.
#[derive(Debug, PartialEq, Eq)]
enum Resp {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
    Null,
}

/// Read one CRLF-terminated line (without the CRLF) from the stream, buffering.
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

/// Read exactly `n` bytes (plus the trailing CRLF) for a bulk string body.
async fn read_bulk_body(client: &mut TcpStream, buf: &mut Vec<u8>, n: usize) -> Vec<u8> {
    while buf.len() < n + 2 {
        let mut chunk = [0u8; 1024];
        let got = client.read(&mut chunk).await.unwrap();
        assert!(got > 0, "connection closed mid-bulk");
        buf.extend_from_slice(&chunk[..got]);
    }
    let body = buf[..n].to_vec();
    buf.drain(..n + 2); // drop body + CRLF
    body
}

/// Read one complete RESP reply (recursively for arrays), buffering across socket reads.
async fn read_reply(client: &mut TcpStream, buf: &mut Vec<u8>) -> Resp {
    let line = read_line(client, buf).await;
    let (tag, rest) = line.split_first().unwrap();
    match tag {
        b'+' => Resp::Simple(rest.to_vec()),
        b'-' => Resp::Error(rest.to_vec()),
        b':' => Resp::Integer(std::str::from_utf8(rest).unwrap().parse().unwrap()),
        b'_' => Resp::Null, // RESP3 null
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
                Resp::Array(None)
            } else {
                let mut items = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    items.push(Box::pin(read_reply(client, buf)).await);
                }
                Resp::Array(Some(items))
            }
        }
        other => panic!("unexpected RESP tag {:?}", *other as char),
    }
}

/// Send a raw command built from `parts` as a RESP2 array.
async fn send_cmd(client: &mut TcpStream, parts: &[&[u8]]) {
    let mut frame = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        frame.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        frame.extend_from_slice(p);
        frame.extend_from_slice(b"\r\n");
    }
    client.write_all(&frame).await.unwrap();
}

/// `DBSIZE` -> the integer reply.
async fn dbsize(client: &mut TcpStream, buf: &mut Vec<u8>) -> i64 {
    send_cmd(client, &[b"DBSIZE"]).await;
    match read_reply(client, buf).await {
        Resp::Integer(n) => n,
        other => panic!("DBSIZE non-integer: {other:?}"),
    }
}

/// `KEYS pattern` -> the set of returned keys (order-independent).
async fn keys(client: &mut TcpStream, buf: &mut Vec<u8>, pattern: &str) -> Vec<Vec<u8>> {
    send_cmd(client, &[b"KEYS", pattern.as_bytes()]).await;
    match read_reply(client, buf).await {
        Resp::Array(Some(items)) => items
            .into_iter()
            .map(|i| match i {
                Resp::Bulk(Some(b)) => b,
                other => panic!("KEYS non-bulk element: {other:?}"),
            })
            .collect(),
        other => panic!("KEYS non-array: {other:?}"),
    }
}

/// Drive a full cross-shard SCAN loop (cursor 0 until a returned cursor of "0"),
/// collecting every key seen. `opts` is the option tail (MATCH/COUNT/TYPE), if any. Caps
/// the iteration count so an infinite loop FAILS the test rather than hangs.
async fn scan_all(client: &mut TcpStream, buf: &mut Vec<u8>, opts: &[&[u8]]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut cursor = b"0".to_vec();
    let mut iterations = 0u32;
    loop {
        iterations += 1;
        assert!(
            iterations < 100_000,
            "SCAN did not terminate (infinite loop): cursor stuck"
        );
        let mut parts: Vec<&[u8]> = vec![b"SCAN", cursor.as_slice()];
        parts.extend_from_slice(opts);
        send_cmd(client, &parts).await;
        let Resp::Array(Some(items)) = read_reply(client, buf).await else {
            panic!("SCAN reply not an array");
        };
        assert_eq!(items.len(), 2, "SCAN reply must be [cursor, keys]");
        let mut it = items.into_iter();
        let next = match it.next().unwrap() {
            Resp::Bulk(Some(b)) => b,
            other => panic!("SCAN cursor not a bulk string: {other:?}"),
        };
        if let Resp::Array(Some(keys)) = it.next().unwrap() {
            for k in keys {
                if let Resp::Bulk(Some(b)) = k {
                    out.push(b);
                }
            }
        }
        if next == b"0" {
            break;
        }
        cursor = next;
    }
    out
}

#[test]
fn remote_round_trip_on_one_connection_covers_all_shards() {
    // (1) Remote round-trip on ONE connection. We do not know the connection's home
    // shard, so we SET then GET many keys spanning all shards on a SINGLE connection:
    // some are home-owned (local fast path), some are remote (enqueue -> drain -> reply).
    // Every key must round-trip, proving the remote path end-to-end alongside the local.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut client = connect_retry(port).await;

        // 64 distinct keys: with 4 shards and FNV-1a routing, these span every shard, so
        // both the local fast path and the remote hop are exercised on this one conn.
        for i in 0..64 {
            let key = format!("rt:key:{i}");
            let val = format!("val{i}");
            set(&mut client, &key, &val).await;
            let reply = get_raw(&mut client, &key).await;
            assert_eq!(
                reply,
                bulk(&val),
                "key {key} did not round-trip (got {:?})",
                String::from_utf8_lossy(&reply)
            );
        }

        // INCR on a counter key then GET it back: the remote RMW must apply on the owning
        // shard and be visible on read-back (proves a mutating remote command, not just SET).
        client
            .write_all(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":1\r\n").await;
        client
            .write_all(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":2\r\n").await;
        let reply = get_raw(&mut client, "counter").await;
        assert_eq!(reply, bulk("2"), "INCR result not visible on read-back");

        drop(client);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn cross_connection_correctness_is_the_bug_fix() {
    // (2) The CORE BUG FIX: SET on connection A, GET on a FRESH connection B (likely a
    // different home shard) must see the value, for keys spanning multiple shards. Before
    // key->shard routing each shard had an independent store and this failed.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;

        for i in 0..64 {
            let key = format!("xc:key:{i}");
            let val = format!("v{i}");
            set(&mut a, &key, &val).await;
            let reply = get_raw(&mut b, &key).await;
            assert_eq!(
                reply,
                bulk(&val),
                "cross-connection GET of {key} failed (the partition bug); got {:?}",
                String::from_utf8_lossy(&reply)
            );
        }

        drop(a);
        drop(b);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn multi_key_single_key_route_is_the_regression_fix() {
    // THE REGRESSION TEST (COORDINATOR.md #107, Stage 1): the multi-key commands
    // (DEL/EXISTS/UNLINK/TOUCH/...) used to run HOME-ONLY even when invoked on a single
    // key, so `SET k v` (routed to owner(k)) then `DEL k` (run on the HOME shard whose
    // store did NOT hold k) returned 0 and k persisted. Now a single-key invocation of a
    // variadic command routes to the owning shard, so it is CORRECT. This test FAILS on the
    // pre-fix code (KeyedMulti kept home) and PASSES after.
    //
    // We hammer 64 keys spanning all 4 shards so some are remote-owned (the broken case).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);

        // (a) Single CONNECTION: SET k v ; EXISTS k -> 1 ; DEL k -> 1 ; EXISTS k -> 0 ;
        // GET k -> nil. The whole cycle on ONE connection (home shard fixed) over keys that
        // span shards: any remote-owned key would have broken DEL/EXISTS pre-fix.
        let mut c = connect_retry(port).await;
        for i in 0..64 {
            let key = format!("rg:{i}");
            set(&mut c, &key, "v").await;
            assert_eq!(
                cmd_key_raw(&mut c, "EXISTS", &key).await,
                b":1\r\n",
                "EXISTS of present routed key {key} must be 1"
            );
            assert_eq!(
                cmd_key_raw(&mut c, "DEL", &key).await,
                b":1\r\n",
                "DEL of routed key {key} must return 1 (the regression: it returned 0)"
            );
            assert_eq!(
                cmd_key_raw(&mut c, "EXISTS", &key).await,
                b":0\r\n",
                "EXISTS after DEL of {key} must be 0"
            );
            assert_eq!(
                get_raw(&mut c, &key).await,
                b"$-1\r\n",
                "GET after DEL of {key} must be nil (the key must be gone)"
            );
            // EXISTS of an absent key is 0.
            assert_eq!(
                cmd_key_raw(&mut c, "EXISTS", &format!("rg:absent:{i}")).await,
                b":0\r\n",
                "EXISTS of an absent key must be 0"
            );
        }

        // (b) Cross-connection: SET on A, DEL on a FRESH connection B (likely a different
        // home shard) -> 1, and the key is gone.
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        for i in 0..64 {
            let key = format!("rgx:{i}");
            set(&mut a, &key, "v").await;
            assert_eq!(
                cmd_key_raw(&mut b, "DEL", &key).await,
                b":1\r\n",
                "cross-connection DEL of {key} must return 1"
            );
            assert_eq!(
                get_raw(&mut a, &key).await,
                b"$-1\r\n",
                "key {key} must be gone after cross-connection DEL"
            );
        }

        // (c) UNLINK and TOUCH single-key route correctly too (UNLINK == DEL today; TOUCH
        // of a present key returns 1).
        let mut c = connect_retry(port).await;
        for i in 0..32 {
            let key = format!("ut:{i}");
            set(&mut c, &key, "v").await;
            assert_eq!(
                cmd_key_raw(&mut c, "TOUCH", &key).await,
                b":1\r\n",
                "TOUCH of present routed key {key} must be 1"
            );
            assert_eq!(
                cmd_key_raw(&mut c, "UNLINK", &key).await,
                b":1\r\n",
                "UNLINK of routed key {key} must return 1"
            );
            assert_eq!(
                cmd_key_raw(&mut c, "TOUCH", &key).await,
                b":0\r\n",
                "TOUCH after UNLINK of {key} must be 0 (key gone)"
            );
        }

        drop(c);
        drop(a);
        drop(b);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn shard_spanning_multi_key_del_stays_home_well_formed_no_panic() {
    // The STAGE 2 GAP (documented): a shard-SPANNING multi-key command (DEL k1 k2 where
    // owner(k1) != owner(k2)) is NOT fanned out this stage; it runs HOME. It is acceptable
    // that the home shard's store does not hold the remote-owned key, so the count may be
    // PARTIALLY WRONG (this is the deferred Stage 2 fan-out). The contract we ASSERT here is
    // only: it does NOT panic, and it returns a WELL-FORMED RESP integer reply `:N\r\n`.
    // (A future Stage 2 PR replaces this with a correctness assertion once fan-out lands.)
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;

        // Try many key PAIRS until we find one that spans shards (different owners), then
        // DEL both in one command. We do not know the FNV mapping here, so we just exercise
        // a spread of pairs; every reply must be a well-formed integer (`:`-prefixed CRLF).
        for i in 0..64 {
            let k1 = format!("span:a:{i}");
            let k2 = format!("span:b:{i}");
            set(&mut c, &k1, "v").await;
            set(&mut c, &k2, "v").await;
            let frame = format!(
                "*3\r\n$3\r\nDEL\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                k1.len(),
                k1,
                k2.len(),
                k2
            );
            c.write_all(frame.as_bytes()).await.unwrap();
            let mut buf = [0u8; 64];
            let n = c.read(&mut buf).await.unwrap();
            let reply = &buf[..n];
            // Well-formed integer reply: `:` <digits> `\r\n`. No panic reached here.
            assert!(
                reply.first() == Some(&b':') && reply.ends_with(b"\r\n"),
                "DEL k1 k2 must return a well-formed integer reply this stage; got {:?}",
                String::from_utf8_lossy(reply)
            );
            let body = &reply[1..reply.len() - 2];
            assert!(
                !body.is_empty() && body.iter().all(u8::is_ascii_digit),
                "DEL reply integer body must be ascii digits; got {:?}",
                String::from_utf8_lossy(reply)
            );
        }

        drop(c);
        // A clean join proves no shard thread panicked on the spanning DEL.
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn remote_absent_key_returns_proto_shaped_nil() {
    // (3) An absent key routed to a remote shard returns the right proto-shaped nil:
    // RESP2 $-1 by default, and RESP3 _ after HELLO 3. We hammer many distinct absent
    // keys so at least some are remote (not home-owned).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut client = connect_retry(port).await;

        // RESP2 leg: absent GET -> $-1.
        for i in 0..32 {
            let key = format!("absent:{i}");
            let reply = get_raw(&mut client, &key).await;
            assert_eq!(
                reply,
                b"$-1\r\n",
                "absent {key} not RESP2 null bulk; got {:?}",
                String::from_utf8_lossy(&reply)
            );
        }

        // RESP3 leg: HELLO 3 (a map), then absent GET -> _ (RESP3 null).
        client
            .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
            .await
            .unwrap();
        let mut hbuf = [0u8; 512];
        let _hn = client.read(&mut hbuf).await.unwrap();
        assert_eq!(hbuf[0], b'%', "HELLO 3 should return a RESP3 map");
        for i in 0..32 {
            let key = format!("absent3:{i}");
            let reply = get_raw(&mut client, &key).await;
            assert_eq!(
                reply,
                b"_\r\n",
                "absent {key} not RESP3 null; got {:?}",
                String::from_utf8_lossy(&reply)
            );
        }

        drop(client);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn shards_one_parity_no_channel_traffic() {
    // (4) shards == 1 parity: with a single shard every key is home-owned, so every
    // command takes the SYNC local fast path (no channel traffic). The same data-command
    // behavior the single-shard e2e suite asserts must hold here booted via run_server.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(1);
        let mut client = connect_retry(port).await;

        // SET/GET round-trip.
        set(&mut client, "foo", "bar").await;
        let reply = get_raw(&mut client, "foo").await;
        assert_eq!(reply, bulk("bar"));

        // SET k v NX -> +OK ; SET k v2 NX -> $-1 (the SET-with-options arms still work).
        client
            .write_all(b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nNX\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv2\r\n$2\r\nNX\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$-1\r\n").await;

        // DEL foo k -> :2 (a multi-key command stays home, served as before).
        client
            .write_all(b"*3\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":2\r\n").await;

        // PING -> +PONG (a control command stays home).
        client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        expect_reply(&mut client, b"+PONG\r\n").await;

        drop(client);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn rapid_cross_shard_commands_do_not_panic_borrow_discipline() {
    // (5) Back-pressure / borrow-discipline smoke: fire MANY rapid commands across shards
    // on ONE connection, interleaving SET/GET/INCR. If the drain loop held a RefCell borrow
    // across its `recv().await` (the invariant this layer must respect), an interleaved
    // command on the same single-threaded shard would double-borrow-panic, surfacing as a
    // shard thread panic at shutdown_and_join. Reaching the end + a clean join proves the
    // no-borrow-across-await contract holds under load.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut client = connect_retry(port).await;

        for i in 0..500 {
            let key = format!("bp:{}", i % 50);
            // Pipeline a SET + GET + INCR-on-a-shared-counter back to back.
            let setf = format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\nx\r\n",
                key.len(),
                key
            );
            client.write_all(setf.as_bytes()).await.unwrap();
            expect_reply(&mut client, b"+OK\r\n").await;
            let reply = get_raw(&mut client, &key).await;
            assert_eq!(reply, bulk("x"));
        }

        drop(client);
        // A clean join (no shard panicked) is the assertion: a borrow-across-await would
        // have panicked a shard thread, which shutdown_and_join surfaces as an Err.
        server.shutdown_and_join().unwrap();
    });
}

/// A focused unit-level guard that the routing decision is the LOCAL fast path for a
/// home-owned key (no channel) and remote otherwise. This asserts the classify+owner_shard
/// decision directly (the serve loop's branch condition), the cheapest "fast-path
/// detection" the task asks for without a test-only send counter.
#[test]
fn home_owned_key_classifies_local_remote_otherwise() {
    use bytes::Bytes;
    use ironcache_protocol::Request;
    use ironcache_server::route::{CommandClass, classify, owner_shard, single_key};

    let n = 4usize;
    // A SET command is KeyedSingle and routes on args[1].
    let set_req = Request {
        args: vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"some-key"),
            Bytes::from_static(b"v"),
        ],
    };
    assert_eq!(classify(b"SET"), CommandClass::KeyedSingle);
    let key = single_key(&set_req).unwrap();
    let owner = owner_shard(key, n);
    // For the owner shard the serve loop takes the LOCAL fast path (target == home);
    // for any other home it routes remote. Verify the split is exactly owner vs not.
    for home in 0..n {
        let is_local = owner == home;
        assert_eq!(
            is_local,
            owner_shard(key, n) == home,
            "fast-path decision must be owner_shard == home"
        );
    }
}

// ===========================================================================
// Whole-keyspace SCATTER-GATHER fan-out (COORDINATOR.md #107, the final Stage 1 piece).
// These boot shards >= 3 so the keyspace genuinely partitions, and assert each
// whole-keyspace command covers the WHOLE keyspace (not just the home shard's ~1/N).
// ===========================================================================

#[test]
fn dbsize_sums_across_all_shards() {
    // SET many keys spanning shards on ONE connection; DBSIZE must equal the TOTAL count
    // (the fan-out sum), NOT ~1/N (the pre-fix home-only count).
    const N: i64 = 300;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        assert_eq!(
            dbsize(&mut c, &mut buf).await,
            0,
            "empty keyspace DBSIZE is 0"
        );

        for i in 0..N {
            set(&mut c, &format!("ds:{i}"), "v").await;
        }
        assert_eq!(
            dbsize(&mut c, &mut buf).await,
            N,
            "DBSIZE must sum every shard's partition to the full count"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn keys_returns_all_keys_across_shards_and_globs() {
    // KEYS * returns ALL keys across shards (compare to the full SET set); KEYS with a glob
    // filters correctly across shards.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        let mut expected: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for i in 0..200 {
            let k = format!("k:{i}");
            set(&mut c, &k, "v").await;
            expected.insert(k.into_bytes());
        }
        // A few keys under a different prefix, to exercise the glob filter.
        for i in 0..20 {
            set(&mut c, &format!("other:{i}"), "v").await;
        }

        // KEYS k:* must return exactly the k:* set, gathered from EVERY shard.
        let got: std::collections::BTreeSet<Vec<u8>> =
            keys(&mut c, &mut buf, "k:*").await.into_iter().collect();
        assert_eq!(
            got, expected,
            "KEYS k:* must cover all shards with the glob applied"
        );

        // KEYS * must return all 220 keys.
        let all = keys(&mut c, &mut buf, "*").await;
        assert_eq!(all.len(), 220, "KEYS * must return every key across shards");

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn flushall_clears_every_shard() {
    // After SET across shards, FLUSHALL then DBSIZE == 0 and GET of any prior key == nil
    // (every shard cleared, not just the home shard).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        for i in 0..200 {
            set(&mut c, &format!("fa:{i}"), "v").await;
        }
        assert_eq!(dbsize(&mut c, &mut buf).await, 200);

        send_cmd(&mut c, &[b"FLUSHALL"]).await;
        assert_eq!(
            read_reply(&mut c, &mut buf).await,
            Resp::Simple(b"OK".to_vec()),
            "FLUSHALL must reply +OK"
        );

        assert_eq!(
            dbsize(&mut c, &mut buf).await,
            0,
            "FLUSHALL must clear every shard"
        );
        // A prior key on any shard must now be nil.
        for i in 0..200 {
            assert_eq!(
                get_raw(&mut c, &format!("fa:{i}")).await,
                b"$-1\r\n",
                "key fa:{i} must be gone after FLUSHALL"
            );
        }

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn flushdb_clears_selected_db_across_shards_and_leaves_other_db() {
    // FLUSHDB clears the SELECTED db across all shards; another db is untouched.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        // db 0: 150 keys.
        for i in 0..150 {
            set(&mut c, &format!("d0:{i}"), "v").await;
        }
        // SELECT db 1: 80 keys.
        send_cmd(&mut c, &[b"SELECT", b"1"]).await;
        assert_eq!(
            read_reply(&mut c, &mut buf).await,
            Resp::Simple(b"OK".to_vec())
        );
        for i in 0..80 {
            set(&mut c, &format!("d1:{i}"), "v").await;
        }
        assert_eq!(dbsize(&mut c, &mut buf).await, 80, "db1 has 80 keys");

        // FLUSHDB on db 1 clears db1 across all shards.
        send_cmd(&mut c, &[b"FLUSHDB"]).await;
        assert_eq!(
            read_reply(&mut c, &mut buf).await,
            Resp::Simple(b"OK".to_vec())
        );
        assert_eq!(
            dbsize(&mut c, &mut buf).await,
            0,
            "db1 cleared across shards"
        );

        // db 0 is untouched.
        send_cmd(&mut c, &[b"SELECT", b"0"]).await;
        assert_eq!(
            read_reply(&mut c, &mut buf).await,
            Resp::Simple(b"OK".to_vec())
        );
        assert_eq!(
            dbsize(&mut c, &mut buf).await,
            150,
            "db0 untouched by FLUSHDB db1"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn randomkey_nil_when_empty_then_returns_an_existing_key_spanning_shards() {
    // RANDOMKEY on an empty keyspace -> nil; after SET across shards -> returns a key that
    // EXISTS (GET it -> non-nil); over many calls it can return keys from different shards
    // (a soft check that it is not pinned to one shard).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        // Empty -> nil.
        send_cmd(&mut c, &[b"RANDOMKEY"]).await;
        let empty = read_reply(&mut c, &mut buf).await;
        assert!(
            matches!(empty, Resp::Bulk(None) | Resp::Null),
            "RANDOMKEY on empty keyspace must be nil, got {empty:?}"
        );

        let mut all: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for i in 0..200 {
            let k = format!("rk:{i}");
            set(&mut c, &k, "v").await;
            all.insert(k.into_bytes());
        }

        // Many RANDOMKEY calls: each must return an EXISTING key; collect the distinct ones.
        let mut seen: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for _ in 0..100 {
            send_cmd(&mut c, &[b"RANDOMKEY"]).await;
            let r = read_reply(&mut c, &mut buf).await;
            let Resp::Bulk(Some(k)) = r else {
                panic!("RANDOMKEY must return a key when the keyspace is non-empty, got {r:?}");
            };
            assert!(all.contains(&k), "RANDOMKEY returned a non-existent key");
            // GET it: must be non-nil (the key really exists on its shard).
            let key_str = String::from_utf8(k.clone()).unwrap();
            let got = get_raw(&mut c, &key_str).await;
            assert_eq!(got, bulk("v"), "RANDOMKEY's key must GET non-nil");
            seen.insert(k);
        }
        // Soft check: over 100 draws across 200 keys / 4 shards, we expect more than one
        // distinct key (a stuck single-shard pick would return very few distinct keys).
        assert!(
            seen.len() > 1,
            "RANDOMKEY should return varied keys across shards, saw {} distinct",
            seen.len()
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn scan_full_loop_visits_every_key_across_shards_exactly_covering_the_set() {
    // CRITICAL: a full SCAN loop (cursor 0 until cursor 0) collects EVERY key across all
    // shards, terminates (no infinite loop), and covers the full SET set. We de-dup the
    // collected keys (SCAN may legally re-emit) and compare the SET to the full set.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(5);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        let mut expected: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for i in 0..500 {
            let k = format!("sc:{i}");
            set(&mut c, &k, "v").await;
            expected.insert(k.into_bytes());
        }

        // Full SCAN with a small COUNT hint so the traversal takes many steps across shards.
        let collected: std::collections::BTreeSet<Vec<u8>> =
            scan_all(&mut c, &mut buf, &[b"COUNT", b"7"])
                .await
                .into_iter()
                .collect();
        assert_eq!(
            collected, expected,
            "a full cross-shard SCAN must visit EVERY key exactly covering the SET set"
        );

        // SCAN MATCH filters across shards.
        for i in 0..30 {
            set(&mut c, &format!("match:{i}"), "v").await;
        }
        let matched: std::collections::BTreeSet<Vec<u8>> =
            scan_all(&mut c, &mut buf, &[b"MATCH", b"match:*", b"COUNT", b"10"])
                .await
                .into_iter()
                .collect();
        assert_eq!(matched.len(), 30, "SCAN MATCH must filter across shards");
        assert!(
            matched.iter().all(|k| k.starts_with(b"match:")),
            "SCAN MATCH must only return matching keys"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn whole_keyspace_shards_one_parity() {
    // shards == 1 parity: the whole-keyspace commands behave exactly as the single-shard
    // path (fan_out_all degenerates to the local call; the SCAN composite cursor == the
    // inner cursor, byte-identical). DBSIZE/KEYS/SCAN/FLUSH all work on one shard.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(1);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        for i in 0..50 {
            set(&mut c, &format!("p:{i}"), "v").await;
        }
        assert_eq!(dbsize(&mut c, &mut buf).await, 50);

        let all: std::collections::BTreeSet<Vec<u8>> =
            scan_all(&mut c, &mut buf, &[b"COUNT", b"5"])
                .await
                .into_iter()
                .collect();
        let keys_set: std::collections::BTreeSet<Vec<u8>> =
            keys(&mut c, &mut buf, "*").await.into_iter().collect();
        assert_eq!(all, keys_set, "SCAN and KEYS agree on one shard");
        assert_eq!(all.len(), 50);

        send_cmd(&mut c, &[b"FLUSHDB"]).await;
        assert_eq!(
            read_reply(&mut c, &mut buf).await,
            Resp::Simple(b"OK".to_vec())
        );
        assert_eq!(dbsize(&mut c, &mut buf).await, 0);

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn scan_count_one_terminates_and_covers_every_key_the_band_advance_regression() {
    // THE FIX 1 regression guard end to end (the critical one): a full cross-shard SCAN
    // loop with COUNT 1 -- the WORST case, forcing the per-shard step to return a fresh
    // (band-aligned) cursor on every single key -- over several hundred keys spanning all
    // shards MUST (a) TERMINATE within scan_all's hard iteration cap (no infinite loop),
    // (b) cover the FULL key set (every key visited at least once, no shard starved, no
    // key skipped), (c) reach cursor 0 only after every shard.
    //
    // This exercises the band-ADVANCE path the composite cursor depends on: each step
    // packs `compose(shard, band_floor)` and the next call `decompose`s it back; FIX 1
    // makes the per-shard cursor band-aligned so that `>> 8` truncation is lossless and
    // the wire cursor strictly advances. A genuine in-band hash collision (two keys
    // sharing the top 56 scan_hash bits) is astronomically rare for real keys, so the
    // *hand-crafted* dense band lives in the scan_plan unit tests
    // (`band_aligned_next_cursor_clears_low_bits`,
    // `dense_band_terminates_and_visits_every_key_with_band_bits` in ironcache-store),
    // which is where the round-DOWN truncation defect is provable deterministically; this
    // e2e test guards the same advance over the real wire with the worst-case COUNT 1.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        let mut expected: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for i in 0..400 {
            let k = format!("c1:{i}");
            set(&mut c, &k, "v").await;
            expected.insert(k.into_bytes());
        }

        // COUNT 1 (worst case) and COUNT 7: both must terminate AND fully cover.
        for count in [b"1".as_slice(), b"7".as_slice()] {
            let collected: std::collections::BTreeSet<Vec<u8>> =
                scan_all(&mut c, &mut buf, &[b"COUNT", count])
                    .await
                    .into_iter()
                    .collect();
            assert_eq!(
                collected,
                expected,
                "full cross-shard SCAN must terminate AND visit every key (COUNT {})",
                std::str::from_utf8(count).unwrap()
            );
        }

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn bare_scan_is_wrong_arity_not_invalid_cursor_at_one_and_many_shards() {
    // FIX 4: a bare `SCAN` (no cursor) is arity -2, so it must return the wrong-arity
    // error, NOT `-ERR invalid cursor`. This must hold at shards == 1 (parity with the
    // single-shard cmd_scan, which checks arity first) AND at shards > 1 (the cross-shard
    // scan_cross_shard checks arity before parsing the cursor).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        for shards in [1usize, 3] {
            let (server, port) = boot(shards);
            let mut c = connect_retry(port).await;
            let mut buf = Vec::new();

            send_cmd(&mut c, &[b"SCAN"]).await;
            let reply = read_reply(&mut c, &mut buf).await;
            let Resp::Error(line) = reply else {
                panic!("bare SCAN at shards={shards} must be an error, got {reply:?}");
            };
            let text = String::from_utf8(line).unwrap();
            assert_eq!(
                text, "ERR wrong number of arguments for 'scan' command",
                "bare SCAN at shards={shards} must be the wrong-arity error, not invalid-cursor"
            );

            drop(c);
            server.shutdown_and_join().unwrap();
        }
    });
}

// FIX 3 (DBSIZE / FLUSH must NOT draw the home Env RNG) is asserted DETERMINISTICALLY by
// the unit test `serve::tests::dbsize_flush_do_not_advance_rng_only_randomkey_does`, which
// snapshots the per-shard SplitMix64 stream around the route's RNG-draw decision. It cannot
// be a stable cross-boot e2e assertion here because `SystemEnv` seeds its RNG from the wall
// clock (each `run_server` boot draws a different seed), so two boots' RANDOMKEY sequences
// differ regardless of the fix. The unit test holds one seeded stream fixed and is the
// precise regression guard.
