// SPDX-License-Identifier: MIT OR Apache-2.0
//! Pub/Sub acceptance tests (SERVER_PUSH.md #20, PR 91a exact channels + PR 91b glob patterns).
//!
//! These boot the REAL multi-shard `run_server` on an ephemeral port (the actual
//! SO_REUSEPORT thread-per-core topology + cross-shard coordinator) and drive it over real
//! sockets, so they exercise the whole path: the serve-layer pub/sub interception (SUBSCRIBE,
//! PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB), the per-shard subscription tables,
//! the cross-shard `__ICPUBLISH` fan-out plus the `__ICPUBSUB` introspection gather, the
//! per-connection push channel plus the `select!` idle wait, the RESP3 `>` and RESP2 `*` framing,
//! the subscribe-mode gate, PING-while-subscribed, back-pressure shedding, and disconnect
//! cleanup. With shards=4 a publisher and a subscriber land on (likely) different cores, so the
//! cross-shard fan-out and the per-shard-gather introspection are genuinely exercised.

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

/// Boot a multi-shard server, returning (handle, port).
fn boot(shards: usize) -> (ironcache_runtime::bootstrap::ShardSet, u16) {
    let port = free_port();
    let set = run_server_for_test(port, shards);
    (set, port)
}

// ---------------------------------------------------------------------------
// A minimal RESP2/RESP3 reader. It records the AGGREGATE FRAME TYPE (`*` array vs `>`
// push) so a test can assert the RESP3 push vs RESP2 array distinction for a delivery.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Resp {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    /// An aggregate (`*` array or `>` push). `is_push` records the wire frame byte so the
    /// RESP3-`>`-vs-RESP2-`*` distinction is observable; both decode to the same elements.
    Agg {
        is_push: bool,
        items: Vec<Resp>,
    },
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

async fn read_agg(client: &mut TcpStream, buf: &mut Vec<u8>, rest: &[u8], is_push: bool) -> Resp {
    let len: i64 = std::str::from_utf8(rest).unwrap().parse().unwrap();
    if len < 0 {
        return Resp::Null;
    }
    let mut items = Vec::with_capacity(len as usize);
    for _ in 0..len {
        items.push(Box::pin(read_reply(client, buf)).await);
    }
    Resp::Agg { is_push, items }
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
        b'*' => read_agg(client, buf, rest, false).await,
        b'>' => read_agg(client, buf, rest, true).await,
        // HELLO 3 replies a `%` map; we never read it as a structured reply (we drain it
        // raw), so any other tag in these tests is unexpected.
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

async fn send_and_read(client: &mut TcpStream, buf: &mut Vec<u8>, parts: &[&[u8]]) -> Resp {
    send_cmd(client, parts).await;
    read_reply(client, buf).await
}

/// Read with a timeout, returning None on timeout (used to assert "no message arrives").
async fn read_with_timeout(client: &mut TcpStream, buf: &mut Vec<u8>, ms: u64) -> Option<Resp> {
    tokio::time::timeout(Duration::from_millis(ms), read_reply(client, buf))
        .await
        .ok()
}

/// Switch a connection to RESP3 (`HELLO 3`) and drain its `%` map reply raw (we do not need
/// to parse the map; we just consume bytes up to the point the server stops sending). We read
/// once and discard whatever arrived (the HELLO map fits one read here).
async fn hello3(client: &mut TcpStream) {
    send_cmd(client, &[b"HELLO", b"3"]).await;
    // Drain the HELLO reply: it is a `%` map. Read a chunk and discard; the map is small and
    // arrives in one segment for our config. We then assert nothing else is pending.
    let mut chunk = [0u8; 4096];
    let n = tokio::time::timeout(Duration::from_millis(500), client.read(&mut chunk))
        .await
        .expect("HELLO 3 reply timed out")
        .unwrap();
    assert!(n > 0, "HELLO 3 returned no bytes");
}

/// Assert a delivered message frame is `["message", channel, payload]` and return whether it
/// was a RESP3 push (`>`) vs a RESP2 array (`*`).
fn assert_message(reply: &Resp, channel: &[u8], payload: &[u8]) -> bool {
    let Resp::Agg { is_push, items } = reply else {
        panic!("delivery must be an aggregate, got {reply:?}");
    };
    assert_eq!(items.len(), 3, "message has 3 elements");
    assert_eq!(items[0], Resp::Bulk(Some(b"message".to_vec())));
    assert_eq!(items[1], Resp::Bulk(Some(channel.to_vec())));
    assert_eq!(items[2], Resp::Bulk(Some(payload.to_vec())));
    *is_push
}

/// Assert a delivered pattern-message frame is `["pmessage", pattern, channel, payload]` and
/// return whether it was a RESP3 push (`>`) vs a RESP2 array (`*`).
fn assert_pmessage(reply: &Resp, pattern: &[u8], channel: &[u8], payload: &[u8]) -> bool {
    let Resp::Agg { is_push, items } = reply else {
        panic!("pattern delivery must be an aggregate, got {reply:?}");
    };
    assert_eq!(items.len(), 4, "pmessage has 4 elements");
    assert_eq!(items[0], Resp::Bulk(Some(b"pmessage".to_vec())));
    assert_eq!(items[1], Resp::Bulk(Some(pattern.to_vec())));
    assert_eq!(items[2], Resp::Bulk(Some(channel.to_vec())));
    assert_eq!(items[3], Resp::Bulk(Some(payload.to_vec())));
    *is_push
}

/// Extract the elements of an aggregate (array/push), panicking on any other reply shape.
fn agg_items(reply: &Resp) -> &[Resp] {
    let Resp::Agg { items, .. } = reply else {
        panic!("expected an aggregate reply, got {reply:?}");
    };
    items
}

/// Collect the bulk-string channel names from a PUBSUB CHANNELS reply (an array of bulks),
/// sorted so order-independent unions are comparable.
fn sorted_channel_names(reply: &Resp) -> Vec<Vec<u8>> {
    let mut names: Vec<Vec<u8>> = agg_items(reply)
        .iter()
        .map(|item| match item {
            Resp::Bulk(Some(b)) => b.clone(),
            other => panic!("PUBSUB CHANNELS element must be a bulk string, got {other:?}"),
        })
        .collect();
    names.sort();
    names
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn cross_shard_delivery_and_receiver_count() {
    // Conn A SUBSCRIBE ch; conn B PUBLISH ch payload across shards=4 (A and B are likely on
    // different cores). A receives the ["message", ch, payload] frame; B's PUBLISH returns 1.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        let mut abuf = Vec::new();
        let mut bbuf = Vec::new();

        // A subscribes; the subscribe confirmation is ["subscribe", ch, 1].
        let sub = send_and_read(&mut a, &mut abuf, &[b"SUBSCRIBE", b"ch"]).await;
        let Resp::Agg { items, .. } = &sub else {
            panic!("SUBSCRIBE confirm must be an aggregate, got {sub:?}");
        };
        assert_eq!(items[0], Resp::Bulk(Some(b"subscribe".to_vec())));
        assert_eq!(items[1], Resp::Bulk(Some(b"ch".to_vec())));
        assert_eq!(items[2], Resp::Integer(1));

        // Give the subscription a beat to register on A's home shard before B publishes.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // B publishes; the count is 1 (one subscriber across all shards).
        let pubr = send_and_read(&mut b, &mut bbuf, &[b"PUBLISH", b"ch", b"hello"]).await;
        assert_eq!(pubr, Resp::Integer(1), "PUBLISH must report 1 receiver");

        // A receives the message frame.
        let msg = read_with_timeout(&mut a, &mut abuf, 1000)
            .await
            .expect("A must receive the published message");
        assert_message(&msg, b"ch", b"hello");

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn publish_counts_all_subscribers_and_zero_for_none() {
    // N subscribers across shards -> PUBLISH returns N; PUBLISH to a channel with no
    // subscribers returns 0.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        // Three subscribers on the same channel (they spread across shards via SO_REUSEPORT).
        let mut subs = Vec::new();
        for _ in 0..3 {
            let mut s = connect_retry(port).await;
            let mut sb = Vec::new();
            let _ = send_and_read(&mut s, &mut sb, &[b"SUBSCRIBE", b"news"]).await;
            subs.push((s, sb));
        }
        tokio::time::sleep(Duration::from_millis(80)).await;

        let mut pubc = connect_retry(port).await;
        let mut pb = Vec::new();
        let n = send_and_read(&mut pubc, &mut pb, &[b"PUBLISH", b"news", b"x"]).await;
        assert_eq!(n, Resp::Integer(3), "PUBLISH must count all 3 subscribers");

        // A channel nobody subscribed to: 0 receivers.
        let z = send_and_read(&mut pubc, &mut pb, &[b"PUBLISH", b"silent", b"x"]).await;
        assert_eq!(z, Resp::Integer(0), "PUBLISH to no subscribers returns 0");

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn subscribe_mode_gate_resp2_blocks_other_commands_resp3_allows() {
    // RESP2 subscriber running GET -> the subscribe-mode error. RESP3 subscriber (HELLO 3 then
    // SUBSCRIBE) running GET -> works (no restriction).
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);

        // RESP2 path: subscribe, then GET is rejected with the exact Redis error.
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();
        let _ = send_and_read(&mut c, &mut cb, &[b"SUBSCRIBE", b"ch"]).await;
        let err = send_and_read(&mut c, &mut cb, &[b"GET", b"k"]).await;
        let Resp::Error(line) = err else {
            panic!("RESP2 subscriber GET must error, got {err:?}");
        };
        assert_eq!(
            line,
            b"ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
        );

        // RESP3 path: HELLO 3, subscribe, then GET works (subscribe mode does not restrict RESP3).
        let mut d = connect_retry(port).await;
        let mut db = Vec::new();
        hello3(&mut d).await;
        let _ = send_and_read(&mut d, &mut db, &[b"SUBSCRIBE", b"ch"]).await;
        // SET then GET on the RESP3 subscriber: both must succeed.
        let setr = send_and_read(&mut d, &mut db, &[b"SET", b"k", b"v"]).await;
        assert_eq!(setr, Resp::Simple(b"OK".to_vec()), "RESP3 subscriber SET works");
        let getr = send_and_read(&mut d, &mut db, &[b"GET", b"k"]).await;
        assert_eq!(getr, Resp::Bulk(Some(b"v".to_vec())), "RESP3 subscriber GET works");

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn subscribe_reply_shape_and_unsubscribe_all() {
    // SUBSCRIBE c1 c2 c3 -> one ["subscribe", ci, running_count] per channel, running count
    // incrementing. UNSUBSCRIBE with NO args unsubscribes ALL (count walks down to 0).
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();

        // One SUBSCRIBE with three channels: three confirmations, counts 1,2,3.
        send_cmd(&mut c, &[b"SUBSCRIBE", b"c1", b"c2", b"c3"]).await;
        for (i, ch) in [&b"c1"[..], b"c2", b"c3"].iter().enumerate() {
            let r = read_reply(&mut c, &mut cb).await;
            let Resp::Agg { items, .. } = &r else {
                panic!("subscribe confirm must be an aggregate, got {r:?}");
            };
            assert_eq!(items[0], Resp::Bulk(Some(b"subscribe".to_vec())));
            assert_eq!(items[1], Resp::Bulk(Some(ch.to_vec())));
            assert_eq!(items[2], Resp::Integer(i as i64 + 1));
        }

        // Idempotent re-subscribe to c1 does NOT bump the count (stays 3).
        let again = send_and_read(&mut c, &mut cb, &[b"SUBSCRIBE", b"c1"]).await;
        let Resp::Agg { items, .. } = &again else {
            panic!("re-subscribe confirm must be an aggregate, got {again:?}");
        };
        assert_eq!(
            items[2],
            Resp::Integer(3),
            "re-subscribe does not bump count"
        );

        // UNSUBSCRIBE with no args: one confirmation per channel, count counting DOWN to 0.
        send_cmd(&mut c, &[b"UNSUBSCRIBE"]).await;
        let mut last_count = i64::MAX;
        for _ in 0..3 {
            let r = read_reply(&mut c, &mut cb).await;
            let Resp::Agg { items, .. } = &r else {
                panic!("unsubscribe confirm must be an aggregate, got {r:?}");
            };
            assert_eq!(items[0], Resp::Bulk(Some(b"unsubscribe".to_vec())));
            let Resp::Integer(count) = items[2] else {
                panic!("unsubscribe count must be an integer");
            };
            last_count = count;
        }
        assert_eq!(last_count, 0, "UNSUBSCRIBE-all ends at running count 0");

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn resp3_push_frame_vs_resp2_array_frame() {
    // The SAME delivered message renders as a RESP3 push (`>` first byte) on a RESP3 subscriber
    // and a RESP2 array (`*`) on a RESP2 subscriber.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);

        // RESP2 subscriber.
        let mut r2 = connect_retry(port).await;
        let mut r2b = Vec::new();
        let _ = send_and_read(&mut r2, &mut r2b, &[b"SUBSCRIBE", b"ch"]).await;

        // RESP3 subscriber.
        let mut r3 = connect_retry(port).await;
        let mut r3b = Vec::new();
        hello3(&mut r3).await;
        let _ = send_and_read(&mut r3, &mut r3b, &[b"SUBSCRIBE", b"ch"]).await;

        tokio::time::sleep(Duration::from_millis(80)).await;

        let mut p = connect_retry(port).await;
        let mut pb = Vec::new();
        let n = send_and_read(&mut p, &mut pb, &[b"PUBLISH", b"ch", b"v"]).await;
        assert_eq!(n, Resp::Integer(2), "two subscribers");

        // RESP2 subscriber: a `*` array frame (is_push == false).
        let m2 = read_with_timeout(&mut r2, &mut r2b, 1000)
            .await
            .expect("RESP2 subscriber must receive");
        assert!(
            !assert_message(&m2, b"ch", b"v"),
            "RESP2 delivery is a `*` array"
        );

        // RESP3 subscriber: a `>` push frame (is_push == true).
        let m3 = read_with_timeout(&mut r3, &mut r3b, 1000)
            .await
            .expect("RESP3 subscriber must receive");
        assert!(
            assert_message(&m3, b"ch", b"v"),
            "RESP3 delivery is a `>` push"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn disconnect_cleanup_leaves_no_leak() {
    // A subscriber drops its socket; a later PUBLISH returns 0 (the subscription was pruned).
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);

        // We need the publisher to land on the SAME shard the subscriber used so the
        // disconnect cleanup is observable as a 0-count publish on that channel regardless of
        // shard. Since channels are not slotted, a PUBLISH fans out to ALL shards, so a single
        // publisher observes the global count: after the subscriber disconnects, that is 0.
        {
            let mut s = connect_retry(port).await;
            let mut sb = Vec::new();
            let _ = send_and_read(&mut s, &mut sb, &[b"SUBSCRIBE", b"gone"]).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            // Drop the socket (s goes out of scope) -> the serve loop's disconnect cleanup
            // deregisters the subscription from its home shard's table.
        }
        // Give the server a moment to observe the close + run cleanup.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let mut p = connect_retry(port).await;
        let mut pb = Vec::new();
        let n = send_and_read(&mut p, &mut pb, &[b"PUBLISH", b"gone", b"x"]).await;
        assert_eq!(
            n,
            Resp::Integer(0),
            "a disconnected subscriber leaves no leak"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn ping_while_subscribed_resp2_is_pong_array() {
    // PING while subscribed (RESP2) -> the ["pong", ""] (or ["pong", arg]) array, not +PONG.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();
        let _ = send_and_read(&mut c, &mut cb, &[b"SUBSCRIBE", b"ch"]).await;

        // Bare PING -> ["pong", ""].
        let p = send_and_read(&mut c, &mut cb, &[b"PING"]).await;
        let Resp::Agg { is_push, items } = &p else {
            panic!("subscribed PING must be an array, got {p:?}");
        };
        assert!(!is_push, "the pong reply is a `*` array, not a push frame");
        assert_eq!(items[0], Resp::Bulk(Some(b"pong".to_vec())));
        assert_eq!(items[1], Resp::Bulk(Some(b"".to_vec())));

        // PING with an argument -> ["pong", arg].
        let p2 = send_and_read(&mut c, &mut cb, &[b"PING", b"hi"]).await;
        let Resp::Agg { items, .. } = &p2 else {
            panic!("subscribed PING arg must be an array, got {p2:?}");
        };
        assert_eq!(items[0], Resp::Bulk(Some(b"pong".to_vec())));
        assert_eq!(items[1], Resp::Bulk(Some(b"hi".to_vec())));

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn back_pressure_sheds_slow_consumer_and_publisher_stays_responsive() {
    // A subscriber that NEVER reads is flooded past the push-channel bound; it is shed
    // (disconnected), and the publisher never blocks (it stays responsive throughout).
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);

        // The slow consumer subscribes, reads ONLY its subscribe confirmation, then never
        // reads again.
        let mut slow = connect_retry(port).await;
        let mut slowb = Vec::new();
        let _ = send_and_read(&mut slow, &mut slowb, &[b"SUBSCRIBE", b"flood"]).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The publisher floods well past PUSH_CHANNEL_BOUND (1024). The serve loop drains the
        // push channel into the consumer's KERNEL socket buffer; since the consumer never reads,
        // that socket buffer fills, the serve loop's `rt.send` then blocks, the channel stops
        // draining and fills to the bound, and the next delivery's `try_send` SHEDS the
        // consumer. A LARGE payload (8 KiB) fills both buffers quickly + deterministically.
        // Every PUBLISH must return promptly (never block on the slow consumer).
        let big = vec![b'x'; 8 * 1024];
        let mut p = connect_retry(port).await;
        let mut pb = Vec::new();
        let mut saw_zero_after_full = false;
        for _ in 0..6000u32 {
            // Each PUBLISH is awaited with a generous timeout; a hang here (blocked on the
            // slow consumer) would fail the test by timing out.
            let n = tokio::time::timeout(
                Duration::from_secs(2),
                send_and_read(&mut p, &mut pb, &[b"PUBLISH", b"flood", &big]),
            )
            .await
            .expect("PUBLISH must never block on a slow consumer");
            if n == Resp::Integer(0) {
                saw_zero_after_full = true;
                break;
            }
        }
        assert!(
            saw_zero_after_full,
            "the slow consumer must be shed (count drops to 0) once its push channel overflows"
        );

        // The publisher is still fully responsive after the shed.
        let still = send_and_read(&mut p, &mut pb, &[b"PUBLISH", b"flood", b"x"]).await;
        assert_eq!(
            still,
            Resp::Integer(0),
            "publisher responsive; consumer gone"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn single_shard_subscribe_publish_parity() {
    // shards == 1: SUBSCRIBE/PUBLISH work (the home delivery runs locally, no self-channel hop).
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(1);
        let mut a = connect_retry(port).await;
        let mut ab = Vec::new();
        let _ = send_and_read(&mut a, &mut ab, &[b"SUBSCRIBE", b"one"]).await;
        tokio::time::sleep(Duration::from_millis(40)).await;

        let mut b = connect_retry(port).await;
        let mut bb = Vec::new();
        let n = send_and_read(&mut b, &mut bb, &[b"PUBLISH", b"one", b"v"]).await;
        assert_eq!(
            n,
            Resp::Integer(1),
            "single-shard PUBLISH counts the subscriber"
        );

        let msg = read_with_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("single-shard subscriber must receive");
        assert_message(&msg, b"one", b"v");

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn non_subscriber_hot_path_get_set_unchanged() {
    // A normal (non-subscriber) connection's GET/SET still works -- the select! idle wait is
    // bypassed when not subscribed, so the common hot path is unchanged.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();
        let setr = send_and_read(&mut c, &mut cb, &[b"SET", b"k", b"v"]).await;
        assert_eq!(setr, Resp::Simple(b"OK".to_vec()));
        let getr = send_and_read(&mut c, &mut cb, &[b"GET", b"k"]).await;
        assert_eq!(getr, Resp::Bulk(Some(b"v".to_vec())));
        // A PING on a non-subscriber is the normal +PONG simple string (NOT the pong array).
        let pong = send_and_read(&mut c, &mut cb, &[b"PING"]).await;
        assert_eq!(pong, Resp::Simple(b"PONG".to_vec()));

        server.shutdown_and_join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// PR 91b: PSUBSCRIBE / PUNSUBSCRIBE (glob pattern subscriptions) + PUBSUB
// CHANNELS / NUMSUB / NUMPAT introspection.
// ---------------------------------------------------------------------------

#[test]
fn pattern_delivery_cross_shard() {
    // Conn A PSUBSCRIBE news.* ; conn B PUBLISH news.tech x (shards=4, likely different cores).
    // A receives ["pmessage", news.*, news.tech, x]; B's PUBLISH reports 1 receiver.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        let mut abuf = Vec::new();
        let mut bbuf = Vec::new();

        // PSUBSCRIBE confirmation is ["psubscribe", news.*, 1].
        let sub = send_and_read(&mut a, &mut abuf, &[b"PSUBSCRIBE", b"news.*"]).await;
        let items = agg_items(&sub);
        assert_eq!(items[0], Resp::Bulk(Some(b"psubscribe".to_vec())));
        assert_eq!(items[1], Resp::Bulk(Some(b"news.*".to_vec())));
        assert_eq!(items[2], Resp::Integer(1));

        tokio::time::sleep(Duration::from_millis(60)).await;

        let pubr = send_and_read(&mut b, &mut bbuf, &[b"PUBLISH", b"news.tech", b"x"]).await;
        assert_eq!(
            pubr,
            Resp::Integer(1),
            "PUBLISH to a channel matched by one pattern reports 1"
        );

        let msg = read_with_timeout(&mut a, &mut abuf, 1000)
            .await
            .expect("pattern subscriber must receive the pmessage");
        assert_pmessage(&msg, b"news.*", b"news.tech", b"x");

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn exact_and_pattern_double_delivery_and_double_count() {
    // A connection subscribed to BOTH exact "news.tech" AND pattern "news.*" receives BOTH a
    // "message" AND a "pmessage" for ONE PUBLISH, and the PUBLISH count includes BOTH (= 2).
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        let mut a = connect_retry(port).await;
        let mut abuf = Vec::new();

        // Both subscriptions on the SAME connection (so they share a home shard + push channel).
        let _ = send_and_read(&mut a, &mut abuf, &[b"SUBSCRIBE", b"news.tech"]).await;
        let _ = send_and_read(&mut a, &mut abuf, &[b"PSUBSCRIBE", b"news.*"]).await;
        tokio::time::sleep(Duration::from_millis(60)).await;

        let mut b = connect_retry(port).await;
        let mut bbuf = Vec::new();
        let pubr = send_and_read(&mut b, &mut bbuf, &[b"PUBLISH", b"news.tech", b"v"]).await;
        assert_eq!(
            pubr,
            Resp::Integer(2),
            "exact + pattern on one conn counts as 2 receivers"
        );

        // The connection receives TWO frames: one "message" and one "pmessage" (order between
        // them is not guaranteed across the exact-vs-pattern fan-out, so accept either order).
        let f1 = read_with_timeout(&mut a, &mut abuf, 1000)
            .await
            .expect("first frame must arrive");
        let f2 = read_with_timeout(&mut a, &mut abuf, 1000)
            .await
            .expect("second frame must arrive");
        let kind = |r: &Resp| match agg_items(r)[0].clone() {
            Resp::Bulk(Some(b)) => b,
            other => panic!("frame tag must be a bulk string, got {other:?}"),
        };
        let mut kinds = [kind(&f1), kind(&f2)];
        kinds.sort();
        assert_eq!(
            kinds,
            [b"message".to_vec(), b"pmessage".to_vec()],
            "one message + one pmessage delivered for a single PUBLISH"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn punsubscribe_no_args_unsubscribes_all_patterns() {
    // PSUBSCRIBE two patterns, then PUNSUBSCRIBE with NO args: one confirm per pattern, the
    // running count counts DOWN to 0; a later PUBLISH to a matched channel reports 0.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();

        send_cmd(&mut c, &[b"PSUBSCRIBE", b"a.*", b"b.*"]).await;
        for (i, pat) in [&b"a.*"[..], b"b.*"].iter().enumerate() {
            let r = read_reply(&mut c, &mut cb).await;
            let items = agg_items(&r);
            assert_eq!(items[0], Resp::Bulk(Some(b"psubscribe".to_vec())));
            assert_eq!(items[1], Resp::Bulk(Some(pat.to_vec())));
            assert_eq!(items[2], Resp::Integer(i as i64 + 1));
        }

        // PUNSUBSCRIBE with no args: one confirm per pattern, count walking down to 0.
        send_cmd(&mut c, &[b"PUNSUBSCRIBE"]).await;
        let mut last_count = i64::MAX;
        for _ in 0..2 {
            let r = read_reply(&mut c, &mut cb).await;
            let items = agg_items(&r);
            assert_eq!(items[0], Resp::Bulk(Some(b"punsubscribe".to_vec())));
            let Resp::Integer(count) = items[2] else {
                panic!("punsubscribe count must be an integer");
            };
            last_count = count;
        }
        assert_eq!(last_count, 0, "PUNSUBSCRIBE-all ends at running count 0");

        // The connection left subscribe mode: a PUBLISH to a previously-matched channel is 0.
        let mut p = connect_retry(port).await;
        let mut pb = Vec::new();
        let n = send_and_read(&mut p, &mut pb, &[b"PUBLISH", b"a.x", b"v"]).await;
        assert_eq!(
            n,
            Resp::Integer(0),
            "no pattern subscribers after PUNSUBSCRIBE"
        );

        server.shutdown_and_join().unwrap();
    });
}

/// Boot, spread several subscribers across shards, and exercise PUBSUB CHANNELS / NUMSUB /
/// NUMPAT. `shards` is parametrized so the same assertions run at shards=4 (cross-shard gather)
/// and shards=1 (single-shard parity).
fn run_pubsub_introspection(shards: usize) {
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(shards);

        // Several subscribers on a mix of channels + a pattern, spread across shards via
        // SO_REUSEPORT. Keep the sockets alive for the lifetime of the introspection.
        let mut held = Vec::new();
        // news.tech: two subscribers; news.sports: one; pattern news.* : two (distinct conns).
        for spec in [
            &[b"SUBSCRIBE".as_slice(), b"news.tech"][..],
            &[b"SUBSCRIBE", b"news.tech"],
            &[b"SUBSCRIBE", b"news.sports"],
            &[b"PSUBSCRIBE", b"news.*"],
            &[b"PSUBSCRIBE", b"news.*"],
        ] {
            let mut s = connect_retry(port).await;
            let mut sb = Vec::new();
            let _ = send_and_read(&mut s, &mut sb, spec).await;
            held.push((s, sb));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut q = connect_retry(port).await;
        let mut qb = Vec::new();

        // PUBSUB CHANNELS: union across shards, deduped -> {news.tech, news.sports}. Patterns
        // are NOT channels, so news.* must NOT appear.
        let chans = send_and_read(&mut q, &mut qb, &[b"PUBSUB", b"CHANNELS"]).await;
        assert_eq!(
            sorted_channel_names(&chans),
            vec![b"news.sports".to_vec(), b"news.tech".to_vec()],
            "PUBSUB CHANNELS unions + dedups channel names across shards"
        );

        // PUBSUB CHANNELS news.spo* : glob filter -> only news.sports.
        let filtered =
            send_and_read(&mut q, &mut qb, &[b"PUBSUB", b"CHANNELS", b"news.spo*"]).await;
        assert_eq!(
            sorted_channel_names(&filtered),
            vec![b"news.sports".to_vec()],
            "PUBSUB CHANNELS applies the glob filter"
        );

        // PUBSUB NUMSUB news.tech news.sports absent : flat [ch, n, ...] in requested order;
        // counts summed across shards; a channel with no subs -> 0.
        let numsub = send_and_read(
            &mut q,
            &mut qb,
            &[
                b"PUBSUB",
                b"NUMSUB",
                b"news.tech",
                b"news.sports",
                b"absent",
            ],
        )
        .await;
        let items = agg_items(&numsub);
        assert_eq!(
            items.len(),
            6,
            "NUMSUB returns a flat [ch, n] pair per channel"
        );
        assert_eq!(items[0], Resp::Bulk(Some(b"news.tech".to_vec())));
        assert_eq!(items[1], Resp::Integer(2), "news.tech has 2 subscribers");
        assert_eq!(items[2], Resp::Bulk(Some(b"news.sports".to_vec())));
        assert_eq!(items[3], Resp::Integer(1), "news.sports has 1 subscriber");
        assert_eq!(items[4], Resp::Bulk(Some(b"absent".to_vec())));
        assert_eq!(items[5], Resp::Integer(0), "an unsubscribed channel is 0");

        // PUBSUB NUMPAT : distinct patterns globally. Two conns subscribed to the SAME pattern
        // news.* -> ONE distinct pattern, not two.
        let numpat = send_and_read(&mut q, &mut qb, &[b"PUBSUB", b"NUMPAT"]).await;
        assert_eq!(
            numpat,
            Resp::Integer(1),
            "PUBSUB NUMPAT counts DISTINCT patterns globally (the same pattern on 2 conns is 1)"
        );

        server.shutdown_and_join().unwrap();
        drop(held);
    });
}

#[test]
fn pubsub_introspection_cross_shard() {
    run_pubsub_introspection(4);
}

#[test]
fn pubsub_introspection_single_shard_parity() {
    run_pubsub_introspection(1);
}

#[test]
fn pubsub_unknown_subcommand_errors() {
    // PUBSUB with an unknown subcommand -> the Redis unknown-subcommand error.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();
        let err = send_and_read(&mut c, &mut cb, &[b"PUBSUB", b"BOGUS"]).await;
        let Resp::Error(line) = err else {
            panic!("PUBSUB BOGUS must be an error, got {err:?}");
        };
        assert_eq!(
            line,
            b"ERR unknown subcommand or wrong number of arguments for 'BOGUS'. Try PUBSUB HELP."
        );
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn pattern_delivery_resp3_push_frame() {
    // A pattern delivery renders as a RESP3 push (`>` first byte) on a RESP3 subscriber.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        let mut r3 = connect_retry(port).await;
        let mut r3b = Vec::new();
        hello3(&mut r3).await;
        let _ = send_and_read(&mut r3, &mut r3b, &[b"PSUBSCRIBE", b"news.*"]).await;
        tokio::time::sleep(Duration::from_millis(60)).await;

        let mut p = connect_retry(port).await;
        let mut pb = Vec::new();
        let n = send_and_read(&mut p, &mut pb, &[b"PUBLISH", b"news.tech", b"v"]).await;
        assert_eq!(n, Resp::Integer(1));

        let m3 = read_with_timeout(&mut r3, &mut r3b, 1000)
            .await
            .expect("RESP3 pattern subscriber must receive");
        assert!(
            assert_pmessage(&m3, b"news.*", b"news.tech", b"v"),
            "RESP3 pattern delivery is a `>` push frame"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn single_shard_pattern_delivery_parity() {
    // shards == 1: PSUBSCRIBE/PUBLISH pattern delivery works (home delivery runs locally).
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(1);
        let mut a = connect_retry(port).await;
        let mut ab = Vec::new();
        let _ = send_and_read(&mut a, &mut ab, &[b"PSUBSCRIBE", b"ev.*"]).await;
        tokio::time::sleep(Duration::from_millis(40)).await;

        let mut b = connect_retry(port).await;
        let mut bb = Vec::new();
        let n = send_and_read(&mut b, &mut bb, &[b"PUBLISH", b"ev.login", b"v"]).await;
        assert_eq!(
            n,
            Resp::Integer(1),
            "single-shard pattern PUBLISH counts the subscriber"
        );

        let msg = read_with_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("single-shard pattern subscriber must receive");
        assert_pmessage(&msg, b"ev.*", b"ev.login", b"v");

        server.shutdown_and_join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// Adversarial-review regression tests (SERVER_PUSH.md #20, FIX A-H). Each pins the
// behavior a finding was about so it cannot silently regress.
// ---------------------------------------------------------------------------

/// Pull the `-...` error text (without the trailing CRLF) out of an error reply, panicking on
/// any other shape.
fn err_text(reply: &Resp) -> Vec<u8> {
    match reply {
        Resp::Error(line) => line.clone(),
        other => panic!("expected an error reply, got {other:?}"),
    }
}

#[test]
fn fix_a_reset_clears_subscriptions_from_the_shard_table() {
    // FIX A: after SUBSCRIBE then RESET, the connection is NO LONGER in the shard subscription
    // table -- a PUBLISH to the channel returns 0 (no ghost delivery) and PUBSUB CHANNELS is
    // empty. Before the fix, RESET cleared the conn-side sets but left the table entry, so the
    // connection stayed a ghost subscriber.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        let mut a = connect_retry(port).await;
        let mut ab = Vec::new();

        // Subscribe, then RESET. RESET replies the simple string "+RESET".
        let _ = send_and_read(&mut a, &mut ab, &[b"SUBSCRIBE", b"ch"]).await;
        let reset = send_and_read(&mut a, &mut ab, &[b"RESET"]).await;
        assert_eq!(
            reset,
            Resp::Simple(b"RESET".to_vec()),
            "RESET replies +RESET"
        );
        // Give the home shard a beat (the deregister ran synchronously in route_and_dispatch,
        // but allow scheduling slack before the cross-shard PUBSUB gather).
        tokio::time::sleep(Duration::from_millis(50)).await;

        // A PUBLISH to the channel finds NO subscriber (the table entry was deregistered).
        let mut p = connect_retry(port).await;
        let mut pb = Vec::new();
        let n = send_and_read(&mut p, &mut pb, &[b"PUBLISH", b"ch", b"v"]).await;
        assert_eq!(
            n,
            Resp::Integer(0),
            "PUBLISH after RESET must find no subscriber (no ghost)"
        );

        // PUBSUB CHANNELS is empty (the channel has no subscribers anywhere).
        let chans = send_and_read(&mut p, &mut pb, &[b"PUBSUB", b"CHANNELS"]).await;
        assert!(
            sorted_channel_names(&chans).is_empty(),
            "PUBSUB CHANNELS must be empty after the only subscriber RESET"
        );

        // The post-RESET connection is no longer in subscribe mode: a plain GET works (it left
        // the RESP2 subscribe-mode restriction), and a fresh SUBSCRIBE re-registers cleanly.
        let getr = send_and_read(&mut a, &mut ab, &[b"GET", b"missing"]).await;
        assert_eq!(
            getr,
            Resp::Bulk(None),
            "post-RESET GET works (left subscribe mode)"
        );
        let resub = send_and_read(&mut a, &mut ab, &[b"SUBSCRIBE", b"ch"]).await;
        let items = agg_items(&resub);
        assert_eq!(items[0], Resp::Bulk(Some(b"subscribe".to_vec())));
        assert_eq!(
            items[2],
            Resp::Integer(1),
            "fresh SUBSCRIBE re-registers at count 1"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The re-subscribe registered the NEW push channel: a PUBLISH now counts + delivers.
        let n2 = send_and_read(&mut p, &mut pb, &[b"PUBLISH", b"ch", b"again"]).await;
        assert_eq!(n2, Resp::Integer(1), "re-subscribe after RESET delivers");
        let msg = read_with_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("re-subscribed connection must receive on its fresh push channel");
        assert_message(&msg, b"ch", b"again");

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn fix_b_resp2_subscriber_publish_and_pubsub_are_subscribe_mode_errors() {
    // FIX B: the RESP2 subscribe-mode gate must cover PUBLISH and PUBSUB (they are NOT in the
    // allowed set). Before the fix, try_handle_pubsub intercepted them BEFORE the gate, so a
    // RESP2 subscriber executed them. A RESP3 subscriber has NO restriction (PUBLISH/PUBSUB
    // work), and a NON-subscriber PUBLISH always works.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);

        // RESP2 subscriber: PUBLISH and PUBSUB are both rejected with the subscribe-mode error.
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();
        let _ = send_and_read(&mut c, &mut cb, &[b"SUBSCRIBE", b"ch"]).await;
        let pub_err = send_and_read(&mut c, &mut cb, &[b"PUBLISH", b"ch", b"v"]).await;
        assert_eq!(
            err_text(&pub_err),
            b"ERR Can't execute 'publish': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
        );
        let pubsub_err = send_and_read(&mut c, &mut cb, &[b"PUBSUB", b"CHANNELS"]).await;
        assert_eq!(
            err_text(&pubsub_err),
            b"ERR Can't execute 'pubsub': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
        );
        // SUBSCRIBE-family + PING still pass the gate (the allowlist reaches interception).
        let again = send_and_read(&mut c, &mut cb, &[b"SUBSCRIBE", b"ch2"]).await;
        assert_eq!(agg_items(&again)[0], Resp::Bulk(Some(b"subscribe".to_vec())));

        // RESP3 subscriber: PUBLISH and PUBSUB both work (no subscribe-mode restriction).
        let mut d = connect_retry(port).await;
        let mut db = Vec::new();
        hello3(&mut d).await;
        let _ = send_and_read(&mut d, &mut db, &[b"SUBSCRIBE", b"chd"]).await;
        let pubr = send_and_read(&mut d, &mut db, &[b"PUBLISH", b"nobody", b"v"]).await;
        assert_eq!(pubr, Resp::Integer(0), "RESP3 subscriber PUBLISH works");
        let chans = send_and_read(&mut d, &mut db, &[b"PUBSUB", b"CHANNELS"]).await;
        // chd has a subscriber (this connection): CHANNELS lists it (alongside any channels the
        // still-connected RESP2 subscriber holds -- the gather is global, so assert containment
        // rather than exclusive equality).
        assert!(
            sorted_channel_names(&chans).contains(&b"chd".to_vec()),
            "RESP3 subscriber PUBSUB CHANNELS works (lists chd)"
        );

        // Non-subscriber PUBLISH always works.
        let mut e = connect_retry(port).await;
        let mut eb = Vec::new();
        let n = send_and_read(&mut e, &mut eb, &[b"PUBLISH", b"none", b"v"]).await;
        assert_eq!(n, Resp::Integer(0), "non-subscriber PUBLISH works");

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn fix_c_pubsub_commands_in_multi_are_rejected_and_execabort() {
    // FIX C: a serve-layer pub/sub command inside MULTI must NOT execute eagerly. It is rejected
    // (the "is not allowed in transactions" error) and dirties the transaction, so EXEC returns
    // -EXECABORT. This is a documented divergence from Redis (which queues + runs at EXEC); see
    // the constructor doc. Covers SUBSCRIBE and PUBLISH.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();

        let multi = send_and_read(&mut c, &mut cb, &[b"MULTI"]).await;
        assert_eq!(multi, Resp::Simple(b"OK".to_vec()));

        // SUBSCRIBE inside MULTI: rejected, NOT +QUEUED.
        let sub = send_and_read(&mut c, &mut cb, &[b"SUBSCRIBE", b"ch"]).await;
        assert_eq!(
            err_text(&sub),
            b"ERR SUBSCRIBE is not allowed in transactions",
            "SUBSCRIBE in MULTI is rejected, not queued"
        );
        // PUBLISH inside MULTI: rejected too.
        let pubr = send_and_read(&mut c, &mut cb, &[b"PUBLISH", b"ch", b"v"]).await;
        assert_eq!(
            err_text(&pubr),
            b"ERR PUBLISH is not allowed in transactions",
            "PUBLISH in MULTI is rejected, not queued"
        );

        // The transaction was dirtied: EXEC -> -EXECABORT, applying nothing.
        let exec = send_and_read(&mut c, &mut cb, &[b"EXEC"]).await;
        assert_eq!(
            err_text(&exec),
            b"EXECABORT Transaction discarded because of previous errors.",
            "a rejected pub/sub command in MULTI dirties the txn -> EXECABORT"
        );

        // The connection is NOT in subscribe mode (the SUBSCRIBE never executed): a GET works.
        let getr = send_and_read(&mut c, &mut cb, &[b"GET", b"missing"]).await;
        assert_eq!(
            getr,
            Resp::Bulk(None),
            "the rejected SUBSCRIBE did not subscribe"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn fix_d_flooded_non_reading_subscriber_is_disconnected() {
    // FIX D: a subscriber that never reads is flooded past the push-channel bound; it is shed
    // AND its socket is actively CLOSED (read returns 0 = EOF), and the publisher stays
    // responsive. The active disconnect (not just table removal) is the fix: the serve loop
    // holds its own push_tx clone, so the shed signal -- not push_rx closing -- drives the close.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);

        // The slow consumer subscribes, reads only its confirmation, then never reads again.
        let mut slow = connect_retry(port).await;
        let mut slowb = Vec::new();
        let _ = send_and_read(&mut slow, &mut slowb, &[b"SUBSCRIBE", b"flood"]).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Flood well past the bound with a large payload, like the existing back-pressure test,
        // until the publisher observes the shed (count 0). Every PUBLISH must return promptly.
        let big = vec![b'x'; 8 * 1024];
        let mut p = connect_retry(port).await;
        let mut pb = Vec::new();
        let mut shed = false;
        for _ in 0..6000u32 {
            let n = tokio::time::timeout(
                Duration::from_secs(2),
                send_and_read(&mut p, &mut pb, &[b"PUBLISH", b"flood", &big]),
            )
            .await
            .expect("PUBLISH must never block on a slow consumer");
            if n == Resp::Integer(0) {
                shed = true;
                break;
            }
        }
        assert!(
            shed,
            "the slow consumer must be shed once its push channel overflows"
        );

        // FIX D: the shed connection's SOCKET is actively closed. A read on it returns 0 (EOF)
        // promptly (the serve loop observed the shed signal and broke its loop). Drain any
        // buffered pushes first, then assert EOF within a bounded wait. The read buffer is a
        // heap Vec (not a 16 KiB stack array) so the awaited future stays small.
        let eof = tokio::time::timeout(Duration::from_secs(3), async {
            let mut chunk = vec![0u8; 16 * 1024];
            loop {
                // Ok(0) is a clean EOF (the server closed the socket); Err is a reset/error.
                // Both mean the connection is gone -> return true. Ok(n>0) is buffered push
                // bytes: keep draining.
                match slow.read(&mut chunk).await {
                    Ok(0) | Err(_) => return true,
                    Ok(_) => {}
                }
            }
        })
        .await
        .expect("the shed subscriber's socket must close (EOF) within the timeout");
        assert!(
            eof,
            "the flooded non-reading subscriber is disconnected (EOF)"
        );

        // The publisher is still fully responsive after the shed + disconnect.
        let still = send_and_read(&mut p, &mut pb, &[b"PUBLISH", b"flood", b"x"]).await;
        assert_eq!(
            still,
            Resp::Integer(0),
            "publisher responsive; consumer gone"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn fix_f_client_internal_verb_in_multi_dirties_and_execaborts() {
    // FIX F: a client-issued INTERNAL verb (__ICPUBLISH) inside MULTI must reject with the
    // unknown-command error AND dirty the transaction, so EXEC returns -EXECABORT (exactly as a
    // genuine unknown command in MULTI does). Before the fix the reject path did not set
    // dirty_exec, so the internal verb did not abort the batch.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();

        assert_eq!(
            send_and_read(&mut c, &mut cb, &[b"MULTI"]).await,
            Resp::Simple(b"OK".to_vec())
        );

        // A client __ICPUBLISH in MULTI: rejected as unknown-command (the internal verb is
        // client-unreachable), NOT +QUEUED.
        let icpub = send_and_read(&mut c, &mut cb, &[b"__ICPUBLISH", b"ch", b"v"]).await;
        let text = err_text(&icpub);
        assert!(
            text.starts_with(b"ERR unknown command '__ICPUBLISH'"),
            "client __ICPUBLISH is the unknown-command error; got {:?}",
            String::from_utf8_lossy(&text)
        );

        // The txn was dirtied: EXEC -> -EXECABORT.
        let exec = send_and_read(&mut c, &mut cb, &[b"EXEC"]).await;
        assert_eq!(
            err_text(&exec),
            b"EXECABORT Transaction discarded because of previous errors.",
            "a client internal verb in MULTI dirties the txn -> EXECABORT"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn fix_g_bare_pubsub_is_wrong_arity() {
    // FIX G: a bare `PUBSUB` (no subcommand) returns the WRONG-ARITY error, not the
    // unknown-subcommand error (Redis returns wrong-arity for a missing subcommand).
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();
        let err = send_and_read(&mut c, &mut cb, &[b"PUBSUB"]).await;
        assert_eq!(
            err_text(&err),
            b"ERR wrong number of arguments for 'pubsub' command",
            "bare PUBSUB is wrong-arity, not unknown-subcommand"
        );
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn fix_h_pubsub_channels_and_numpat_reject_extra_args() {
    // FIX H: PUBSUB CHANNELS takes at most ONE pattern arg; PUBSUB NUMPAT takes NO args. Extra
    // args -> the Redis subcommand-syntax error (addReplySubcommandSyntaxError = our
    // unknown-subcommand text). NUMSUB takes any number of channels (no upper bound), so it
    // stays valid with several channels.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();

        // CHANNELS with TWO pattern args (one too many) -> the subcommand-syntax error.
        let chans_err =
            send_and_read(&mut c, &mut cb, &[b"PUBSUB", b"CHANNELS", b"a*", b"b*"]).await;
        assert_eq!(
            err_text(&chans_err),
            b"ERR unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.",
            "PUBSUB CHANNELS with >1 pattern is the syntax/arity error"
        );

        // NUMPAT with ANY arg -> the subcommand-syntax error (it takes no args).
        let numpat_err = send_and_read(&mut c, &mut cb, &[b"PUBSUB", b"NUMPAT", b"x"]).await;
        assert_eq!(
            err_text(&numpat_err),
            b"ERR unknown subcommand or wrong number of arguments for 'NUMPAT'. Try PUBSUB HELP.",
            "PUBSUB NUMPAT with any arg is the syntax/arity error"
        );

        // CHANNELS with exactly one pattern is VALID (an empty array here, nobody subscribed).
        let chans_ok = send_and_read(&mut c, &mut cb, &[b"PUBSUB", b"CHANNELS", b"a*"]).await;
        assert!(
            sorted_channel_names(&chans_ok).is_empty(),
            "PUBSUB CHANNELS with one pattern is valid"
        );
        // NUMSUB with several channels stays valid (flat [ch, 0, ch, 0] pairs).
        let numsub_ok =
            send_and_read(&mut c, &mut cb, &[b"PUBSUB", b"NUMSUB", b"x", b"y", b"z"]).await;
        assert_eq!(
            agg_items(&numsub_ok).len(),
            6,
            "PUBSUB NUMSUB accepts any number of channels"
        );

        server.shutdown_and_join().unwrap();
    });
}
