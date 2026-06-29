// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end tests for CLIENT TRACKING / server-assisted client-side caching (#409, stage 1):
//! a RESP3 tracking client reads a key, a second connection changes it, and the tracking client
//! receives the `["invalidate", [key]]` push. Plus NOLOOP, the RESP2 gate, TRACKINGINFO, OFF, and
//! the FLUSH (invalidate-everything) form.

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
    Agg { is_push: bool, items: Vec<Resp> },
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
        b'#' => Resp::Integer(i64::from(rest == b"t")),
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
                return Resp::Agg {
                    is_push: *tag == b'>',
                    items: Vec::new(),
                };
            }
            if *tag == b'%' {
                n *= 2; // a map has 2N elements
            }
            let mut items = Vec::with_capacity(n as usize);
            for _ in 0..n {
                items.push(Box::pin(read_reply(c, buf)).await);
            }
            Resp::Agg {
                is_push: *tag == b'>',
                items,
            }
        }
        other => panic!("unexpected RESP tag {:?}", *other as char),
    }
}

async fn send(c: &mut TcpStream, parts: &[&[u8]]) {
    let mut f = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        f.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        f.extend_from_slice(p);
        f.extend_from_slice(b"\r\n");
    }
    c.write_all(&f).await.unwrap();
}

async fn cmd(c: &mut TcpStream, buf: &mut Vec<u8>, parts: &[&[u8]]) -> Resp {
    send(c, parts).await;
    read_reply(c, buf).await
}

async fn read_timeout(c: &mut TcpStream, buf: &mut Vec<u8>, ms: u64) -> Option<Resp> {
    tokio::time::timeout(Duration::from_millis(ms), read_reply(c, buf))
        .await
        .ok()
}

async fn hello3(c: &mut TcpStream, buf: &mut Vec<u8>) {
    let r = cmd(c, buf, &[b"HELLO", b"3"]).await;
    assert!(
        matches!(r, Resp::Agg { .. }),
        "HELLO 3 must succeed, got {r:?}"
    );
}

fn rt() -> (tokio::runtime::Runtime, tokio::task::LocalSet) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    (rt, tokio::task::LocalSet::new())
}

/// Whether a CLIENT TRACKINGINFO reply (a flattened map `[k, v, ...]`) has `want` among its
/// `flags` array (e.g. `on`/`off`/`noloop`).
fn trackinginfo_flag(reply: &Resp, want: &[u8]) -> bool {
    let Resp::Agg { items, .. } = reply else {
        panic!("TRACKINGINFO must be a map, got {reply:?}");
    };
    let flags = items
        .chunks(2)
        .find(|c| c[0] == Resp::Bulk(Some(b"flags".to_vec())))
        .map(|c| &c[1])
        .expect("TRACKINGINFO must have a flags field");
    let Resp::Agg { items: fs, .. } = flags else {
        panic!("flags must be an array, got {flags:?}");
    };
    fs.contains(&Resp::Bulk(Some(want.to_vec())))
}

/// Assert an invalidation push `["invalidate", [keys...]]` (or `["invalidate", nil]` for flush).
fn assert_invalidate(reply: &Resp, expect_keys: Option<&[&[u8]]>) {
    let Resp::Agg { is_push, items } = reply else {
        panic!("invalidation must be an aggregate, got {reply:?}");
    };
    assert!(is_push, "invalidation must be a RESP3 push (>), got array");
    assert_eq!(items.len(), 2, "invalidate push has 2 elements");
    assert_eq!(items[0], Resp::Bulk(Some(b"invalidate".to_vec())));
    match expect_keys {
        None => {
            let is_flush = match &items[1] {
                Resp::Null => true,
                Resp::Agg { items: ks, .. } => ks.is_empty(),
                _ => false,
            };
            assert!(
                is_flush,
                "flush invalidate must carry nil/empty, got {:?}",
                items[1]
            );
        }
        Some(keys) => {
            let Resp::Agg { items: ks, .. } = &items[1] else {
                panic!("invalidate keys must be an array, got {:?}", items[1]);
            };
            let got: Vec<Vec<u8>> = ks
                .iter()
                .map(|k| match k {
                    Resp::Bulk(Some(b)) => b.clone(),
                    other => panic!("key must be a bulk, got {other:?}"),
                })
                .collect();
            for k in keys {
                assert!(
                    got.iter().any(|g| g.as_slice() == *k),
                    "expected key {k:?} in {got:?}"
                );
            }
        }
    }
}

#[test]
fn tracking_read_then_foreign_write_pushes_invalidate() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        let (mut ab, mut bb) = (Vec::new(), Vec::new());

        hello3(&mut a, &mut ab).await;
        assert_eq!(
            cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKING", b"ON"]).await,
            Resp::Simple(b"OK".to_vec())
        );
        // A reads k (registers it for tracking).
        cmd(&mut a, &mut ab, &[b"GET", b"k"]).await;
        tokio::time::sleep(Duration::from_millis(40)).await;

        // B changes k.
        cmd(&mut b, &mut bb, &[b"SET", b"k", b"v"]).await;

        // A receives the invalidation push for k.
        let inv = read_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("A must receive an invalidate push");
        assert_invalidate(&inv, Some(&[b"k"]));
    });
}

#[test]
fn tracking_noloop_suppresses_own_write_but_not_foreign() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        let (mut ab, mut bb) = (Vec::new(), Vec::new());

        hello3(&mut a, &mut ab).await;
        cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKING", b"ON", b"NOLOOP"]).await;
        cmd(&mut a, &mut ab, &[b"GET", b"k"]).await; // register k
        tokio::time::sleep(Duration::from_millis(40)).await;

        // A's OWN write of k: NOLOOP means A gets NO invalidation echo.
        cmd(&mut a, &mut ab, &[b"SET", b"k", b"v1"]).await;
        assert!(
            read_timeout(&mut a, &mut ab, 300).await.is_none(),
            "NOLOOP must suppress the invalidation for A's own write"
        );

        // A re-reads to re-track (the invalidation is one-shot, and A's own write removed it).
        cmd(&mut a, &mut ab, &[b"GET", b"k"]).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        // A FOREIGN write (B) still invalidates A.
        cmd(&mut b, &mut bb, &[b"SET", b"k", b"v2"]).await;
        let inv = read_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("foreign write must invalidate");
        assert_invalidate(&inv, Some(&[b"k"]));
    });
}

#[test]
fn tracking_resp2_gate_and_trackinginfo_and_off() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);

        // RESP2 connection: CLIENT TRACKING ON is rejected (RESP3 or REDIRECT required).
        let mut two = connect_retry(port).await;
        let mut tb = Vec::new();
        let denied = cmd(&mut two, &mut tb, &[b"CLIENT", b"TRACKING", b"ON"]).await;
        assert!(
            matches!(&denied, Resp::Error(e) if String::from_utf8_lossy(e).contains("RESP3")),
            "RESP2 CLIENT TRACKING ON must error, got {denied:?}"
        );

        // RESP3 connection: ON, TRACKINGINFO shows on, OFF, TRACKINGINFO shows off.
        let mut a = connect_retry(port).await;
        let mut ab = Vec::new();
        hello3(&mut a, &mut ab).await;
        assert_eq!(
            cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKING", b"ON"]).await,
            Resp::Simple(b"OK".to_vec())
        );
        assert!(trackinginfo_flag(
            &cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKINGINFO"]).await,
            b"on"
        ));

        assert_eq!(
            cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKING", b"OFF"]).await,
            Resp::Simple(b"OK".to_vec())
        );
        assert!(trackinginfo_flag(
            &cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKINGINFO"]).await,
            b"off"
        ));

        // The one still-unsupported option (REDIRECT, stage 4) is rejected loudly, not silently
        // mis-moded. (BCAST is stage 2; OPTIN/OPTOUT are stage 3, each with their own tests.)
        let redirect = cmd(
            &mut a,
            &mut ab,
            &[b"CLIENT", b"TRACKING", b"ON", b"REDIRECT", b"5"],
        )
        .await;
        assert!(
            matches!(&redirect, Resp::Error(_)),
            "REDIRECT must be a loud not-yet-supported error"
        );
    });
}

#[test]
fn tracking_flushall_invalidates_everything() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        let (mut ab, mut bb) = (Vec::new(), Vec::new());

        hello3(&mut a, &mut ab).await;
        cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKING", b"ON"]).await;
        cmd(&mut a, &mut ab, &[b"GET", b"k1"]).await;
        cmd(&mut a, &mut ab, &[b"GET", b"k2"]).await;
        tokio::time::sleep(Duration::from_millis(40)).await;

        cmd(&mut b, &mut bb, &[b"FLUSHALL"]).await;
        let inv = read_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("FLUSHALL must invalidate everything");
        assert_invalidate(&inv, None);
    });
}

// ---------------------------------------------------------------------------
// BCAST mode (#409 stage 2): prefix-based broadcast tracking. A BCAST client does NOT
// register reads; its PREFIX subscriptions invalidate on every matching changed key (sticky).
// ---------------------------------------------------------------------------

#[test]
fn tracking_bcast_prefix_only_invalidates_matching_keys() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        let (mut ab, mut bb) = (Vec::new(), Vec::new());

        hello3(&mut a, &mut ab).await;
        // BCAST with a prefix: A tracks "user:*" WITHOUT reading anything.
        assert_eq!(
            cmd(
                &mut a,
                &mut ab,
                &[b"CLIENT", b"TRACKING", b"ON", b"BCAST", b"PREFIX", b"user:"]
            )
            .await,
            Resp::Simple(b"OK".to_vec())
        );
        tokio::time::sleep(Duration::from_millis(40)).await;

        // A matching key change -> A is invalidated with the changed key.
        cmd(&mut b, &mut bb, &[b"SET", b"user:1", b"v"]).await;
        let inv = read_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("matching prefix must invalidate");
        assert_invalidate(&inv, Some(&[b"user:1"]));

        // BCAST is STICKY: a SECOND matching key still invalidates (no re-subscribe needed).
        cmd(&mut b, &mut bb, &[b"SET", b"user:2", b"v"]).await;
        let inv2 = read_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("BCAST is sticky");
        assert_invalidate(&inv2, Some(&[b"user:2"]));

        // A NON-matching key change -> A receives NOTHING.
        cmd(&mut b, &mut bb, &[b"SET", b"other:1", b"v"]).await;
        assert!(
            read_timeout(&mut a, &mut ab, 300).await.is_none(),
            "a key outside the prefix must NOT invalidate a BCAST client"
        );
    });
}

#[test]
fn tracking_bcast_empty_prefix_tracks_all_keys() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        let (mut ab, mut bb) = (Vec::new(), Vec::new());

        hello3(&mut a, &mut ab).await;
        // BCAST with NO prefix = the empty prefix = track ALL keys.
        cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKING", b"ON", b"BCAST"]).await;
        // TRACKINGINFO reports the bcast flag.
        assert!(trackinginfo_flag(
            &cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKINGINFO"]).await,
            b"bcast"
        ));
        tokio::time::sleep(Duration::from_millis(40)).await;

        cmd(&mut b, &mut bb, &[b"SET", b"anything", b"v"]).await;
        let inv = read_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("empty-prefix BCAST tracks all");
        assert_invalidate(&inv, Some(&[b"anything"]));
    });
}

#[test]
fn tracking_bcast_prefix_validation() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut a = connect_retry(port).await;
        let mut ab = Vec::new();
        hello3(&mut a, &mut ab).await;

        // PREFIX without BCAST is rejected.
        let no_bcast = cmd(
            &mut a,
            &mut ab,
            &[b"CLIENT", b"TRACKING", b"ON", b"PREFIX", b"x"],
        )
        .await;
        assert!(
            matches!(&no_bcast, Resp::Error(e) if String::from_utf8_lossy(e).contains("BCAST")),
            "PREFIX without BCAST must error, got {no_bcast:?}"
        );

        // Overlapping prefixes are rejected (foo is a prefix of foobar).
        let overlap = cmd(
            &mut a,
            &mut ab,
            &[
                b"CLIENT",
                b"TRACKING",
                b"ON",
                b"BCAST",
                b"PREFIX",
                b"foo",
                b"PREFIX",
                b"foobar",
            ],
        )
        .await;
        assert!(
            matches!(&overlap, Resp::Error(e) if String::from_utf8_lossy(e).contains("overlaps")),
            "overlapping prefixes must error, got {overlap:?}"
        );
    });
}

// ---------------------------------------------------------------------------
// OPTIN / OPTOUT modes (#409 stage 3): the one-shot CLIENT CACHING gate per read.
// ---------------------------------------------------------------------------

#[test]
fn tracking_optin_caches_only_after_caching_yes() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        let (mut ab, mut bb) = (Vec::new(), Vec::new());

        hello3(&mut a, &mut ab).await;
        cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKING", b"ON", b"OPTIN"]).await;

        // A read WITHOUT a preceding CACHING YES is NOT tracked.
        cmd(&mut a, &mut ab, &[b"GET", b"k1"]).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        cmd(&mut b, &mut bb, &[b"SET", b"k1", b"v"]).await;
        assert!(
            read_timeout(&mut a, &mut ab, 300).await.is_none(),
            "OPTIN: an un-opted read must NOT be tracked"
        );

        // CACHING YES opts the NEXT read in.
        cmd(&mut a, &mut ab, &[b"CLIENT", b"CACHING", b"YES"]).await;
        cmd(&mut a, &mut ab, &[b"GET", b"k2"]).await;
        // A FOLLOWING read (no CACHING) is NOT tracked (the flag is one-shot).
        cmd(&mut a, &mut ab, &[b"GET", b"k3"]).await;
        tokio::time::sleep(Duration::from_millis(40)).await;

        cmd(&mut b, &mut bb, &[b"SET", b"k3", b"v"]).await;
        assert!(
            read_timeout(&mut a, &mut ab, 300).await.is_none(),
            "OPTIN: k3 (no CACHING YES) must NOT be tracked"
        );
        cmd(&mut b, &mut bb, &[b"SET", b"k2", b"v"]).await;
        let inv = read_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("k2 was opted in");
        assert_invalidate(&inv, Some(&[b"k2"]));
    });
}

#[test]
fn tracking_optout_caches_unless_caching_no() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        let (mut ab, mut bb) = (Vec::new(), Vec::new());

        hello3(&mut a, &mut ab).await;
        cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKING", b"ON", b"OPTOUT"]).await;

        // CACHING NO opts the NEXT read OUT.
        cmd(&mut a, &mut ab, &[b"CLIENT", b"CACHING", b"NO"]).await;
        cmd(&mut a, &mut ab, &[b"GET", b"k1"]).await;
        // A following read (no CACHING NO) IS tracked (OPTOUT default).
        cmd(&mut a, &mut ab, &[b"GET", b"k2"]).await;
        tokio::time::sleep(Duration::from_millis(40)).await;

        cmd(&mut b, &mut bb, &[b"SET", b"k1", b"v"]).await;
        assert!(
            read_timeout(&mut a, &mut ab, 300).await.is_none(),
            "OPTOUT: k1 (CACHING NO) must NOT be tracked"
        );
        cmd(&mut b, &mut bb, &[b"SET", b"k2", b"v"]).await;
        let inv = read_timeout(&mut a, &mut ab, 1000)
            .await
            .expect("k2 tracked by OPTOUT default");
        assert_invalidate(&inv, Some(&[b"k2"]));
    });
}

#[test]
fn tracking_caching_and_mode_validation() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut a = connect_retry(port).await;
        let mut ab = Vec::new();
        hello3(&mut a, &mut ab).await;

        // CLIENT CACHING without OPTIN/OPTOUT mode is an error.
        let no_mode = cmd(&mut a, &mut ab, &[b"CLIENT", b"CACHING", b"YES"]).await;
        assert!(
            matches!(&no_mode, Resp::Error(_)),
            "CACHING needs OPTIN/OPTOUT, got {no_mode:?}"
        );

        // OPTIN + OPTOUT together is an error.
        let both = cmd(
            &mut a,
            &mut ab,
            &[b"CLIENT", b"TRACKING", b"ON", b"OPTIN", b"OPTOUT"],
        )
        .await;
        assert!(matches!(&both, Resp::Error(_)), "OPTIN+OPTOUT must error");

        // OPTIN + BCAST is an error.
        let optin_bcast = cmd(
            &mut a,
            &mut ab,
            &[b"CLIENT", b"TRACKING", b"ON", b"OPTIN", b"BCAST"],
        )
        .await;
        assert!(
            matches!(&optin_bcast, Resp::Error(_)),
            "OPTIN+BCAST must error"
        );

        // TRACKINGINFO reports the optin flag.
        cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKING", b"ON", b"OPTIN"]).await;
        assert!(trackinginfo_flag(
            &cmd(&mut a, &mut ab, &[b"CLIENT", b"TRACKINGINFO"]).await,
            b"optin"
        ));
    });
}
