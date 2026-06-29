// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end test for HOTKEYS (#428): a tracking session is started, real keyed commands are run
//! against a hammered "hot" key, and `HOTKEYS GET` shows that key dominating `by-net-bytes` with the
//! session totals accumulated. This exercises the SERVE-LAYER recording hook (the unit/dispatch tests
//! seed the sketch directly; only a live server drives the per-command attribution).

use ironcache::test_support::run_server_for_test;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

#[derive(Debug, Clone, PartialEq, Eq)]
enum Resp {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Resp>),
    Null,
}

async fn read_line(c: &mut TcpStream, buf: &mut Vec<u8>) -> Vec<u8> {
    loop {
        if let Some(p) = buf.windows(2).position(|w| w == b"\r\n") {
            let line = buf[..p].to_vec();
            buf.drain(..p + 2);
            return line;
        }
        let mut chunk = [0u8; 1024];
        let n = c.read(&mut chunk).await.unwrap();
        assert!(n > 0, "closed mid-reply");
        buf.extend_from_slice(&chunk[..n]);
    }
}

async fn read_bulk(c: &mut TcpStream, buf: &mut Vec<u8>, n: usize) -> Vec<u8> {
    while buf.len() < n + 2 {
        let mut chunk = [0u8; 1024];
        let g = c.read(&mut chunk).await.unwrap();
        assert!(g > 0, "closed mid-bulk");
        buf.extend_from_slice(&chunk[..g]);
    }
    let body = buf[..n].to_vec();
    buf.drain(..n + 2);
    body
}

async fn read_reply(c: &mut TcpStream, buf: &mut Vec<u8>) -> Resp {
    let line = read_line(c, buf).await;
    let (tag, rest) = line.split_first().unwrap();
    match tag {
        b'+' => Resp::Simple(rest.to_vec()),
        b'-' => Resp::Error(rest.to_vec()),
        b':' => Resp::Integer(std::str::from_utf8(rest).unwrap().parse().unwrap()),
        b'_' => Resp::Null,
        b'$' | b'=' => {
            let len: i64 = std::str::from_utf8(rest).unwrap().parse().unwrap();
            if len < 0 {
                Resp::Bulk(None)
            } else {
                Resp::Bulk(Some(read_bulk(c, buf, len as usize).await))
            }
        }
        b'*' | b'>' | b'~' | b'%' => {
            let mut n: i64 = std::str::from_utf8(rest).unwrap().parse().unwrap();
            if n < 0 {
                return Resp::Array(Vec::new());
            }
            if *tag == b'%' {
                n *= 2; // a map is 2N elements on the wire
            }
            let mut items = Vec::with_capacity(n as usize);
            for _ in 0..n {
                items.push(Box::pin(read_reply(c, buf)).await);
            }
            Resp::Array(items)
        }
        other => panic!("unexpected RESP tag {:?}", *other as char),
    }
}

async fn cmd(c: &mut TcpStream, buf: &mut Vec<u8>, parts: &[&[u8]]) -> Resp {
    let mut f = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        f.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        f.extend_from_slice(p);
        f.extend_from_slice(b"\r\n");
    }
    c.write_all(&f).await.unwrap();
    read_reply(c, buf).await
}

/// Find a field value in a flattened `[k, v, k, v, ...]` HOTKEYS GET reply (RESP2 degrades the map to
/// a flat array).
fn field<'a>(reply: &'a Resp, name: &[u8]) -> &'a Resp {
    let Resp::Array(items) = reply else {
        panic!("HOTKEYS GET must be an array/map, got {reply:?}");
    };
    for pair in items.chunks(2) {
        if pair[0] == Resp::Bulk(Some(name.to_vec())) {
            return &pair[1];
        }
    }
    panic!("missing field {:?}", std::str::from_utf8(name));
}

#[test]
fn hotkeys_end_to_end_tracks_a_hot_key() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut a = connect_retry(port).await;
        let mut ab = Vec::new();

        // Start tracking both metrics (RESP2 client; HOTKEYS works on any protocol).
        assert_eq!(
            cmd(
                &mut a,
                &mut ab,
                &[b"HOTKEYS", b"START", b"METRICS", b"2", b"CPU", b"NET"]
            )
            .await,
            Resp::Simple(b"OK".to_vec())
        );

        // Hammer one hot key; touch a cold key once.
        for _ in 0..30 {
            cmd(&mut a, &mut ab, &[b"SET", b"hotkey", b"value123"]).await;
            cmd(&mut a, &mut ab, &[b"GET", b"hotkey"]).await;
        }
        cmd(&mut a, &mut ab, &[b"SET", b"coldkey", b"x"]).await;

        let g = cmd(&mut a, &mut ab, &[b"HOTKEYS", b"GET"]).await;
        // Session is active and the byte total is non-zero (net bytes are deterministic).
        assert_eq!(*field(&g, b"tracking-active"), Resp::Integer(1));
        match field(&g, b"net-bytes-all-commands-all-slots") {
            Resp::Integer(n) => assert!(*n > 0, "net bytes should accumulate, got {n}"),
            other => panic!("net-bytes must be an integer, got {other:?}"),
        }
        // `hotkey` dominates by-net-bytes (it ran 60x vs coldkey's 1x).
        let by_net = field(&g, b"by-net-bytes");
        let Resp::Array(items) = by_net else {
            panic!("by-net-bytes must be an array, got {by_net:?}");
        };
        assert!(!items.is_empty(), "by-net-bytes must list the hot key");
        assert_eq!(
            items[0],
            Resp::Bulk(Some(b"hotkey".to_vec())),
            "hotkey must rank first by net bytes"
        );
        match &items[1] {
            Resp::Integer(n) => assert!(*n > 0, "hotkey net bytes must be positive, got {n}"),
            other => panic!("hotkey weight must be an integer, got {other:?}"),
        }

        // STOP preserves the data; a later GET still shows it, now inactive.
        assert_eq!(
            cmd(&mut a, &mut ab, &[b"HOTKEYS", b"STOP"]).await,
            Resp::Simple(b"OK".to_vec())
        );
        let g2 = cmd(&mut a, &mut ab, &[b"HOTKEYS", b"GET"]).await;
        assert_eq!(*field(&g2, b"tracking-active"), Resp::Integer(0));

        // RESET releases the data; GET is null afterwards (RESP2 renders null as a nil bulk `$-1`).
        assert_eq!(
            cmd(&mut a, &mut ab, &[b"HOTKEYS", b"RESET"]).await,
            Resp::Simple(b"OK".to_vec())
        );
        let after = cmd(&mut a, &mut ab, &[b"HOTKEYS", b"GET"]).await;
        assert!(
            matches!(after, Resp::Null | Resp::Bulk(None)),
            "GET after RESET must be null, got {after:?}"
        );
    });
}
