// SPDX-License-Identifier: MIT OR Apache-2.0
//! Keyspace-notification acceptance tests (PROD-8, SERVER_PUSH.md "keyspace notifications off by
//! default", the pinned claim [keyspace-notifications-off-by-default]).
//!
//! These boot the REAL multi-shard `run_server` on an ephemeral port and drive it over real
//! sockets, so they exercise the WHOLE path: `CONFIG SET notify-keyspace-events` -> the runtime
//! overlay -> the per-command flag snapshot -> the per-shard pending buffer the command handlers /
//! expiry / eviction record into -> the serve/coordinator drain + the EXISTING Pub/Sub fan-out ->
//! the `__keyspace@<db>__:<key>` / `__keyevent@<db>__:<event>` channels delivered to SUBSCRIBE /
//! PSUBSCRIBE subscribers. With shards=4 the mutating connection and the subscriber land on
//! (likely) different cores, so the cross-shard fan-out of the notification is genuinely exercised.
//!
//! The default-OFF byte-identical posture is covered by the existing pub/sub + data tests (which
//! never see a notification because `notify-keyspace-events` is empty); here a `default_off_*` test
//! pins it explicitly: a mutation with notifications disabled delivers NOTHING to a `__keyspace@`
//! PSUBSCRIBE.

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

fn boot(shards: usize) -> (ironcache_runtime::bootstrap::ShardSet, u16) {
    let port = free_port();
    let set = run_server_for_test(port, shards);
    (set, port)
}

// -- A minimal RESP2/RESP3 reader (mirrors tests/pubsub.rs). --

#[derive(Debug, Clone, PartialEq, Eq)]
enum Resp {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Agg { is_push: bool, items: Vec<Resp> },
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

async fn read_with_timeout(client: &mut TcpStream, buf: &mut Vec<u8>, ms: u64) -> Option<Resp> {
    tokio::time::timeout(Duration::from_millis(ms), read_reply(client, buf))
        .await
        .ok()
}

/// Assert a `["message", channel, payload]` delivery (a SUBSCRIBE delivery).
fn assert_message(reply: &Resp, channel: &[u8], payload: &[u8]) {
    let Resp::Agg { items, .. } = reply else {
        panic!("delivery must be an aggregate, got {reply:?}");
    };
    assert_eq!(items.len(), 3, "message has 3 elements: {items:?}");
    assert_eq!(items[0], Resp::Bulk(Some(b"message".to_vec())));
    assert_eq!(items[1], Resp::Bulk(Some(channel.to_vec())));
    assert_eq!(items[2], Resp::Bulk(Some(payload.to_vec())));
}

/// Assert a `["pmessage", pattern, channel, payload]` delivery (a PSUBSCRIBE delivery).
fn assert_pmessage(reply: &Resp, pattern: &[u8], channel: &[u8], payload: &[u8]) {
    let Resp::Agg { items, .. } = reply else {
        panic!("pattern delivery must be an aggregate, got {reply:?}");
    };
    assert_eq!(items.len(), 4, "pmessage has 4 elements: {items:?}");
    assert_eq!(items[0], Resp::Bulk(Some(b"pmessage".to_vec())));
    assert_eq!(items[1], Resp::Bulk(Some(pattern.to_vec())));
    assert_eq!(items[2], Resp::Bulk(Some(channel.to_vec())));
    assert_eq!(items[3], Resp::Bulk(Some(payload.to_vec())));
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Issue `CONFIG SET notify-keyspace-events <flags>` on `client` and assert +OK.
async fn set_notify(client: &mut TcpStream, buf: &mut Vec<u8>, flags: &[u8]) {
    let r = send_and_read(
        client,
        buf,
        &[b"CONFIG", b"SET", b"notify-keyspace-events", flags],
    )
    .await;
    assert_eq!(
        r,
        Resp::Simple(b"OK".to_vec()),
        "CONFIG SET notify-keyspace-events {} must be +OK",
        String::from_utf8_lossy(flags)
    );
}

#[test]
fn config_set_get_round_trips_canonical_flag_string() {
    // CONFIG SET notify-keyspace-events KEA; CONFIG GET reports the canonical re-emit `AKE`
    // (A-alias collapse + channels last), the Redis canonical form. An invalid flag is rejected.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(2);
        let mut c = connect_retry(port).await;
        let mut cb = Vec::new();

        // Default: empty (disabled).
        let g = send_and_read(&mut c, &mut cb, &[b"CONFIG", b"GET", b"notify-keyspace-events"]).await;
        // CONFIG GET is a Map rendered as a flat array in RESP2: [name, value].
        let Resp::Agg { items, .. } = &g else {
            panic!("CONFIG GET must be an aggregate, got {g:?}");
        };
        assert_eq!(items[0], Resp::Bulk(Some(b"notify-keyspace-events".to_vec())));
        assert_eq!(items[1], Resp::Bulk(Some(b"".to_vec())), "default is empty/disabled");

        // SET KEA, GET reports the canonical `AKE`.
        set_notify(&mut c, &mut cb, b"KEA").await;
        let g = send_and_read(&mut c, &mut cb, &[b"CONFIG", b"GET", b"notify-keyspace-events"]).await;
        let Resp::Agg { items, .. } = &g else {
            panic!("CONFIG GET must be an aggregate, got {g:?}");
        };
        assert_eq!(items[1], Resp::Bulk(Some(b"AKE".to_vec())), "KEA renders canonically as AKE");

        // An invalid flag char is rejected (a CONFIG SET failed error), not silently accepted.
        let bad = send_and_read(
            &mut c,
            &mut cb,
            &[b"CONFIG", b"SET", b"notify-keyspace-events", b"KEQ"],
        )
        .await;
        assert!(
            matches!(&bad, Resp::Error(e) if String::from_utf8_lossy(e).contains("CONFIG SET failed")),
            "an invalid flag must be a CONFIG SET failed error, got {bad:?}"
        );

        // SET "" disables again, GET reports empty.
        set_notify(&mut c, &mut cb, b"").await;
        let g = send_and_read(&mut c, &mut cb, &[b"CONFIG", b"GET", b"notify-keyspace-events"]).await;
        let Resp::Agg { items, .. } = &g else {
            panic!("CONFIG GET must be an aggregate, got {g:?}");
        };
        assert_eq!(items[1], Resp::Bulk(Some(b"".to_vec())), "SET \"\" disables");

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn keyevent_and_keyspace_for_set_del_expire_lpush() {
    // With KEA enabled: SUBSCRIBE __keyevent@0__:set then SET k v -> ["message", ch, "k"];
    // PSUBSCRIBE __keyspace@0__:* then SET k v -> a pmessage payload "set"; DEL -> "del";
    // EXPIRE -> "expire"; LPUSH -> "lpush".
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        // The mutating connection enables notifications (runtime overlay is process-wide).
        let mut m = connect_retry(port).await;
        let mut mb = Vec::new();
        set_notify(&mut m, &mut mb, b"KEA").await;

        // A keyevent subscriber to `set`, and a keyspace pattern subscriber to every key.
        let mut ev = connect_retry(port).await;
        let mut evb = Vec::new();
        let _ = send_and_read(&mut ev, &mut evb, &[b"SUBSCRIBE", b"__keyevent@0__:set"]).await;

        let mut ks = connect_retry(port).await;
        let mut ksb = Vec::new();
        let _ = send_and_read(&mut ks, &mut ksb, &[b"PSUBSCRIBE", b"__keyspace@0__:*"]).await;

        // Let both subscriptions register on their home shards before the mutations fan out.
        tokio::time::sleep(Duration::from_millis(80)).await;

        // SET k v -> the keyevent subscriber gets the KEY as payload; the keyspace pattern
        // subscriber gets the EVENT NAME `set` as payload.
        let r = send_and_read(&mut m, &mut mb, &[b"SET", b"k", b"v"]).await;
        assert_eq!(r, Resp::Simple(b"OK".to_vec()));

        let ev_msg = read_with_timeout(&mut ev, &mut evb, 1000)
            .await
            .expect("keyevent subscriber must receive the set event");
        assert_message(&ev_msg, b"__keyevent@0__:set", b"k");

        let ks_msg = read_with_timeout(&mut ks, &mut ksb, 1000)
            .await
            .expect("keyspace subscriber must receive the set event");
        assert_pmessage(&ks_msg, b"__keyspace@0__:*", b"__keyspace@0__:k", b"set");

        // DEL k -> keyspace pattern subscriber gets `del`.
        let r = send_and_read(&mut m, &mut mb, &[b"DEL", b"k"]).await;
        assert_eq!(r, Resp::Integer(1));
        let ks_msg = read_with_timeout(&mut ks, &mut ksb, 1000)
            .await
            .expect("keyspace subscriber must receive the del event");
        assert_pmessage(&ks_msg, b"__keyspace@0__:*", b"__keyspace@0__:k", b"del");

        // SET e v then EXPIRE e 100 -> keyspace gets `set` then `expire`.
        let _ = send_and_read(&mut m, &mut mb, &[b"SET", b"e", b"v"]).await;
        let _ = read_with_timeout(&mut ks, &mut ksb, 1000).await; // drain the `set`
        let r = send_and_read(&mut m, &mut mb, &[b"EXPIRE", b"e", b"100"]).await;
        assert_eq!(r, Resp::Integer(1));
        let ks_msg = read_with_timeout(&mut ks, &mut ksb, 1000)
            .await
            .expect("keyspace subscriber must receive the expire event");
        assert_pmessage(&ks_msg, b"__keyspace@0__:*", b"__keyspace@0__:e", b"expire");

        // LPUSH mylist a -> keyspace gets `lpush`.
        let r = send_and_read(&mut m, &mut mb, &[b"LPUSH", b"mylist", b"a"]).await;
        assert_eq!(r, Resp::Integer(1));
        let ks_msg = read_with_timeout(&mut ks, &mut ksb, 1000)
            .await
            .expect("keyspace subscriber must receive the lpush event");
        assert_pmessage(
            &ks_msg,
            b"__keyspace@0__:*",
            b"__keyspace@0__:mylist",
            b"lpush",
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn ttl_expiry_fires_expired_event() {
    // A short PX TTL, then a lazy access after it passes, fires the `expired` event (class x).
    // We PSUBSCRIBE the keyspace pattern and observe the `set` then `expired` payloads.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        let mut m = connect_retry(port).await;
        let mut mb = Vec::new();
        set_notify(&mut m, &mut mb, b"KEA").await;

        let mut ks = connect_retry(port).await;
        let mut ksb = Vec::new();
        let _ = send_and_read(&mut ks, &mut ksb, &[b"PSUBSCRIBE", b"__keyspace@0__:*"]).await;
        tokio::time::sleep(Duration::from_millis(80)).await;

        // SET with a 30ms TTL.
        let _ = send_and_read(&mut m, &mut mb, &[b"SET", b"t", b"v", b"PX", b"30"]).await;
        // Drain the `set` event.
        let set_ev = read_with_timeout(&mut ks, &mut ksb, 1000)
            .await
            .expect("keyspace subscriber must receive the set event");
        assert_pmessage(&set_ev, b"__keyspace@0__:*", b"__keyspace@0__:t", b"set");

        // SET ... PX also fires the secondary `expire` event (the key was given a TTL), matching
        // Redis which fires both `set` and `expire` for a SET with an expiration option.
        let set_expire = read_with_timeout(&mut ks, &mut ksb, 1000)
            .await
            .expect("SET PX must also fire the secondary expire event");
        assert_pmessage(
            &set_expire,
            b"__keyspace@0__:*",
            b"__keyspace@0__:t",
            b"expire",
        );

        // Let the TTL pass, then trigger a lazy access (a GET on the now-expired key reaps it).
        tokio::time::sleep(Duration::from_millis(60)).await;
        let g = send_and_read(&mut m, &mut mb, &[b"GET", b"t"]).await;
        assert_eq!(g, Resp::Bulk(None), "the key has expired (GET returns nil)");

        // The lazy reap fired the `expired` event.
        let exp = read_with_timeout(&mut ks, &mut ksb, 1000)
            .await
            .expect("keyspace subscriber must receive the expired event");
        assert_pmessage(&exp, b"__keyspace@0__:*", b"__keyspace@0__:t", b"expired");

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn class_filtering_keyspace_string_only() {
    // With only `K$` (keyspace channel + STRING class): a SET fires a keyspace event but a LIST
    // op (LPUSH) fires NOTHING (the list class `l` is not selected). is_enabled() requires K + a
    // class, so K$ is active for strings only.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        let mut m = connect_retry(port).await;
        let mut mb = Vec::new();
        set_notify(&mut m, &mut mb, b"K$").await;

        let mut ks = connect_retry(port).await;
        let mut ksb = Vec::new();
        let _ = send_and_read(&mut ks, &mut ksb, &[b"PSUBSCRIBE", b"__keyspace@0__:*"]).await;
        tokio::time::sleep(Duration::from_millis(80)).await;

        // A string SET fires a keyspace event.
        let _ = send_and_read(&mut m, &mut mb, &[b"SET", b"s", b"v"]).await;
        let ev = read_with_timeout(&mut ks, &mut ksb, 1000)
            .await
            .expect("a string SET must fire a keyspace event under K$");
        assert_pmessage(&ev, b"__keyspace@0__:*", b"__keyspace@0__:s", b"set");

        // A LIST LPUSH fires NOTHING (the list class is not selected). No frame within the window.
        let _ = send_and_read(&mut m, &mut mb, &[b"LPUSH", b"l", b"a"]).await;
        let none = read_with_timeout(&mut ks, &mut ksb, 300).await;
        assert!(
            none.is_none(),
            "a list op must NOT fire a keyspace event under K$ (string class only), got {none:?}"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn class_filtering_keyevent_list_only() {
    // With `Elg` (keyevent channel + list + generic, NO keyspace `K`): a LIST op fires a KEYEVENT
    // (the `E` channel) but NO keyspace (`K`) channel message. We subscribe the keyevent channel
    // for `lpush` and a keyspace pattern, and assert only the keyevent arrives.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        let mut m = connect_retry(port).await;
        let mut mb = Vec::new();
        set_notify(&mut m, &mut mb, b"Elg").await;

        let mut ev = connect_retry(port).await;
        let mut evb = Vec::new();
        let _ = send_and_read(&mut ev, &mut evb, &[b"SUBSCRIBE", b"__keyevent@0__:lpush"]).await;

        let mut ks = connect_retry(port).await;
        let mut ksb = Vec::new();
        let _ = send_and_read(&mut ks, &mut ksb, &[b"PSUBSCRIBE", b"__keyspace@0__:*"]).await;
        tokio::time::sleep(Duration::from_millis(80)).await;

        // LPUSH fires the keyevent (list class selected).
        let _ = send_and_read(&mut m, &mut mb, &[b"LPUSH", b"l", b"a"]).await;
        let ev_msg = read_with_timeout(&mut ev, &mut evb, 1000)
            .await
            .expect("a list op must fire a keyevent under Elg");
        assert_message(&ev_msg, b"__keyevent@0__:lpush", b"l");

        // The keyspace pattern subscriber gets NOTHING (no `K` channel selected under Elg).
        let none = read_with_timeout(&mut ks, &mut ksb, 300).await;
        assert!(
            none.is_none(),
            "no keyspace (K) message must arrive under Elg (E channel only), got {none:?}"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn default_off_fires_nothing() {
    // The DEFAULT (notify-keyspace-events empty): a SET / DEL / LPUSH delivers NOTHING to a
    // __keyspace@ PSUBSCRIBE, pinning the byte-identical-when-disabled posture.
    let runtime = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let (server, port) = boot(4);
        // NOTE: no CONFIG SET notify-keyspace-events -> it stays empty (disabled).
        let mut m = connect_retry(port).await;
        let mut mb = Vec::new();

        let mut ks = connect_retry(port).await;
        let mut ksb = Vec::new();
        let _ = send_and_read(&mut ks, &mut ksb, &[b"PSUBSCRIBE", b"__keyspace@0__:*"]).await;
        tokio::time::sleep(Duration::from_millis(80)).await;

        // A flurry of mutations: NONE fires a notification (disabled default).
        let _ = send_and_read(&mut m, &mut mb, &[b"SET", b"k", b"v"]).await;
        let _ = send_and_read(&mut m, &mut mb, &[b"LPUSH", b"l", b"a"]).await;
        let _ = send_and_read(&mut m, &mut mb, &[b"DEL", b"k"]).await;
        let none = read_with_timeout(&mut ks, &mut ksb, 400).await;
        assert!(
            none.is_none(),
            "with notifications DISABLED (default), no keyspace event may arrive, got {none:?}"
        );

        server.shutdown_and_join().unwrap();
    });
}
