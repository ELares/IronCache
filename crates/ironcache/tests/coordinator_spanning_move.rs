// SPDX-License-Identifier: MIT OR Apache-2.0
//! Cross-shard coordinator SPANNING-MOVE acceptance tests (COORDINATOR.md #107, the PROD-9
//! cross-shard atomicity slice).
//!
//! These boot the REAL multi-shard `run_server` on an ephemeral port (the actual
//! SO_REUSEPORT thread-per-core topology) and drive it over real sockets, with keys CHOSEN
//! to hash to DIFFERENT internal shards (via `ironcache_server::owner_shard`) so the spanning
//! path is genuinely exercised, not just the co-located fast path.
//!
//! The headline guards (the bug PROD-9 closes is a SILENT home-subset partial-apply):
//! - a spanning SMOVE moves the member ATOMICALLY (removed from src, present in dst; not a
//!   home-subset no-op) and replies :1;
//! - a spanning LMOVE / RPOPLPUSH moves the element ATOMICALLY (popped from src, pushed to
//!   dst) and replies the element; a missing src is nil with no dst write; a non-list dst is
//!   WRONGTYPE with src unchanged (the element restored);
//! - a spanning MSETNX is ALL-OR-NOTHING (sets ALL keys across shards and replies :1 when none
//!   exist; sets NONE and replies :0 when ANY exists), NOT a home subset;
//! - a spanning RENAME / COPY / LMPOP is FAIL-LOUD (a clear error), never a silent partial;
//! - co-located (same-shard) invocations are byte-identical to the single-shard handler.

use ironcache::test_support::run_server_for_test;
use ironcache_server::owner_shard;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SHARDS: usize = 4;

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
// A minimal RESP2 reader (enough for the shapes these commands return).
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
        b'*' | b'~' => {
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

async fn send_cmd(client: &mut TcpStream, parts: &[&[u8]]) {
    let mut frame = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        frame.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        frame.extend_from_slice(p);
        frame.extend_from_slice(b"\r\n");
    }
    client.write_all(&frame).await.unwrap();
}

async fn roundtrip(client: &mut TcpStream, buf: &mut Vec<u8>, parts: &[&[u8]]) -> Resp {
    send_cmd(client, parts).await;
    read_reply(client, buf).await
}

// ---------------------------------------------------------------------------
// Key picking: find key names that land on DIFFERENT shards (force the spanning path)
// or the SAME shard (the co-located path), under the SAME hash the router uses.
// ---------------------------------------------------------------------------

/// A key `<prefix>:<n>` whose owner under `SHARDS` is `shard`. Panics if none is found in a
/// generous search (the FNV hash spreads, so a few hundred candidates always cover 4 shards).
fn key_on_shard(prefix: &str, shard: usize) -> String {
    for n in 0..100_000 {
        let k = format!("{prefix}:{n}");
        if owner_shard(k.as_bytes(), SHARDS) == shard {
            return k;
        }
    }
    panic!("no key for shard {shard} found");
}

/// Two keys that hash to DIFFERENT shards (the spanning path).
fn spanning_pair() -> (String, String) {
    let a = key_on_shard("src", 0);
    let b = key_on_shard("dst", 1);
    assert_ne!(
        owner_shard(a.as_bytes(), SHARDS),
        owner_shard(b.as_bytes(), SHARDS),
        "the chosen pair must span shards"
    );
    (a, b)
}

/// Two keys that hash to the SAME shard (the co-located path), for parity.
fn colocated_pair() -> (String, String) {
    let a = key_on_shard("c", 2);
    let b = key_on_shard("c2", 2);
    assert_eq!(
        owner_shard(a.as_bytes(), SHARDS),
        owner_shard(b.as_bytes(), SHARDS)
    );
    (a, b)
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// SMOVE: atomic spanning member move.
// ---------------------------------------------------------------------------

#[test]
fn spanning_smove_is_atomic_member_move() {
    let r = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&r, async {
        let (server, port) = boot(SHARDS);
        let mut c = connect_retry(port).await;
        let mut b = Vec::new();

        let (src, dst) = spanning_pair();
        // Load src as a set holding "m" (+ a sibling so src is not deleted by the move).
        roundtrip(&mut c, &mut b, &[b"SADD", src.as_bytes(), b"m", b"keep"]).await;

        // Spanning SMOVE src dst m -> :1, member moved (NOT a home-subset no-op).
        let reply = roundtrip(
            &mut c,
            &mut b,
            &[b"SMOVE", src.as_bytes(), dst.as_bytes(), b"m"],
        )
        .await;
        assert_eq!(reply, Resp::Integer(1), "spanning SMOVE must reply :1");

        // src no longer holds m; dst holds m.
        let in_src = roundtrip(&mut c, &mut b, &[b"SISMEMBER", src.as_bytes(), b"m"]).await;
        assert_eq!(in_src, Resp::Integer(0), "m removed from src");
        let in_dst = roundtrip(&mut c, &mut b, &[b"SISMEMBER", dst.as_bytes(), b"m"]).await;
        assert_eq!(in_dst, Resp::Integer(1), "m present in dst");
        // src still holds its sibling (the move did not nuke src).
        let keep = roundtrip(&mut c, &mut b, &[b"SISMEMBER", src.as_bytes(), b"keep"]).await;
        assert_eq!(keep, Resp::Integer(1), "src sibling untouched");

        // A member NOT in src -> :0 with no write.
        let absent = roundtrip(
            &mut c,
            &mut b,
            &[b"SMOVE", src.as_bytes(), dst.as_bytes(), b"nope"],
        )
        .await;
        assert_eq!(absent, Resp::Integer(0), "absent member -> :0");

        server.shutdown_and_join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// LMOVE / RPOPLPUSH: atomic spanning element move.
// ---------------------------------------------------------------------------

#[test]
fn spanning_lmove_and_rpoplpush_are_atomic_element_moves() {
    let r = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&r, async {
        let (server, port) = boot(SHARDS);
        let mut c = connect_retry(port).await;
        let mut b = Vec::new();

        let (src, dst) = spanning_pair();
        // src = [a, b, c] (LPUSH reverses, so RPUSH to keep order).
        roundtrip(
            &mut c,
            &mut b,
            &[b"RPUSH", src.as_bytes(), b"a", b"b", b"c"],
        )
        .await;

        // RPOPLPUSH src dst -> "c" (pop right of src, push left of dst).
        let moved = roundtrip(
            &mut c,
            &mut b,
            &[b"RPOPLPUSH", src.as_bytes(), dst.as_bytes()],
        )
        .await;
        assert_eq!(
            moved,
            Resp::Bulk(Some(b"c".to_vec())),
            "RPOPLPUSH moves the right element"
        );
        // src is now [a, b]; dst is [c].
        let lsrc = roundtrip(&mut c, &mut b, &[b"LRANGE", src.as_bytes(), b"0", b"-1"]).await;
        assert_eq!(
            lsrc,
            Resp::Array(Some(vec![
                Resp::Bulk(Some(b"a".to_vec())),
                Resp::Bulk(Some(b"b".to_vec()))
            ])),
            "src lost only the moved element"
        );
        let ldst = roundtrip(&mut c, &mut b, &[b"LRANGE", dst.as_bytes(), b"0", b"-1"]).await;
        assert_eq!(
            ldst,
            Resp::Array(Some(vec![Resp::Bulk(Some(b"c".to_vec()))])),
            "dst got the element"
        );

        // LMOVE src dst LEFT RIGHT -> "a" (pop left of src, push right of dst).
        let moved = roundtrip(
            &mut c,
            &mut b,
            &[b"LMOVE", src.as_bytes(), dst.as_bytes(), b"LEFT", b"RIGHT"],
        )
        .await;
        assert_eq!(
            moved,
            Resp::Bulk(Some(b"a".to_vec())),
            "LMOVE moves the left element"
        );
        let ldst = roundtrip(&mut c, &mut b, &[b"LRANGE", dst.as_bytes(), b"0", b"-1"]).await;
        assert_eq!(
            ldst,
            Resp::Array(Some(vec![
                Resp::Bulk(Some(b"c".to_vec())),
                Resp::Bulk(Some(b"a".to_vec()))
            ])),
            "dst appended the element on the right"
        );

        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn spanning_lmove_missing_src_is_nil_and_wrongtype_dst_restores() {
    let r = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&r, async {
        let (server, port) = boot(SHARDS);
        let mut c = connect_retry(port).await;
        let mut b = Vec::new();

        let (src, dst) = spanning_pair();

        // (1) Missing src -> nil, NO dst write (source-first). Pre-set dst to a string so a
        // bogus dst write would be observable; it must stay untouched.
        roundtrip(&mut c, &mut b, &[b"SET", dst.as_bytes(), b"notalist"]).await;
        let nil = roundtrip(
            &mut c,
            &mut b,
            &[b"RPOPLPUSH", src.as_bytes(), dst.as_bytes()],
        )
        .await;
        assert_eq!(nil, Resp::Bulk(None), "missing src -> nil");
        let dstv = roundtrip(&mut c, &mut b, &[b"GET", dst.as_bytes()]).await;
        assert_eq!(
            dstv,
            Resp::Bulk(Some(b"notalist".to_vec())),
            "dst untouched on missing src"
        );

        // (2) Present src, non-list dst -> WRONGTYPE, src restored (no element lost).
        roundtrip(&mut c, &mut b, &[b"RPUSH", src.as_bytes(), b"x", b"y"]).await;
        let wt = roundtrip(
            &mut c,
            &mut b,
            &[b"RPOPLPUSH", src.as_bytes(), dst.as_bytes()],
        )
        .await;
        assert!(
            matches!(wt, Resp::Error(ref e) if e.starts_with(b"WRONGTYPE")),
            "non-list dst -> WRONGTYPE, got {wt:?}"
        );
        let lsrc = roundtrip(&mut c, &mut b, &[b"LRANGE", src.as_bytes(), b"0", b"-1"]).await;
        assert_eq!(
            lsrc,
            Resp::Array(Some(vec![
                Resp::Bulk(Some(b"x".to_vec())),
                Resp::Bulk(Some(b"y".to_vec()))
            ])),
            "src restored intact after dst-WRONGTYPE (no element lost)"
        );

        server.shutdown_and_join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// MSETNX: all-or-nothing across shards.
// ---------------------------------------------------------------------------

#[test]
fn spanning_msetnx_is_all_or_nothing() {
    let r = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&r, async {
        let (server, port) = boot(SHARDS);
        let mut c = connect_retry(port).await;
        let mut b = Vec::new();

        // Three keys, each on a distinct shard, to force a true multi-shard MSETNX.
        let k0 = key_on_shard("m", 0);
        let k1 = key_on_shard("m", 1);
        let k2 = key_on_shard("m", 2);

        // None exist -> :1 and ALL set (not a home subset).
        let ok = roundtrip(
            &mut c,
            &mut b,
            &[
                b"MSETNX",
                k0.as_bytes(),
                b"v0",
                k1.as_bytes(),
                b"v1",
                k2.as_bytes(),
                b"v2",
            ],
        )
        .await;
        assert_eq!(ok, Resp::Integer(1), "MSETNX with no conflict -> :1");
        for (k, v) in [(&k0, b"v0"), (&k1, b"v1"), (&k2, b"v2")] {
            let got = roundtrip(&mut c, &mut b, &[b"GET", k.as_bytes()]).await;
            assert_eq!(
                got,
                Resp::Bulk(Some(v.to_vec())),
                "{k} must be set across shards"
            );
        }

        // Now one key exists -> :0 and NOTHING new written (all-or-nothing).
        let k3 = key_on_shard("m", 3);
        let zero = roundtrip(
            &mut c,
            &mut b,
            &[
                b"MSETNX",
                k3.as_bytes(),
                b"new",
                k1.as_bytes(),
                b"overwrite",
            ],
        )
        .await;
        assert_eq!(zero, Resp::Integer(0), "MSETNX with a conflict -> :0");
        // k3 must NOT have been created (the whole command aborted before any write).
        let k3v = roundtrip(&mut c, &mut b, &[b"GET", k3.as_bytes()]).await;
        assert_eq!(k3v, Resp::Bulk(None), "k3 not written when MSETNX aborts");
        // k1 must keep its old value (not overwritten).
        let k1v = roundtrip(&mut c, &mut b, &[b"GET", k1.as_bytes()]).await;
        assert_eq!(
            k1v,
            Resp::Bulk(Some(b"v1".to_vec())),
            "k1 unchanged when MSETNX aborts"
        );

        server.shutdown_and_join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// FAIL-LOUD: spanning RENAME/COPY/LMPOP rejected, never a silent partial.
// ---------------------------------------------------------------------------

#[test]
fn spanning_rename_copy_lmpop_are_fail_loud_not_silent() {
    let r = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&r, async {
        let (server, port) = boot(SHARDS);
        let mut c = connect_retry(port).await;
        let mut b = Vec::new();

        let (src, dst) = spanning_pair();
        roundtrip(&mut c, &mut b, &[b"SET", src.as_bytes(), b"val"]).await;

        // Spanning RENAME -> a LOUD error, NOT -ERR no such key (the old silent home-subset
        // behavior) and NOT a silent partial.
        let ren = roundtrip(&mut c, &mut b, &[b"RENAME", src.as_bytes(), dst.as_bytes()]).await;
        assert!(
            matches!(ren, Resp::Error(_)),
            "spanning RENAME must be a loud error, got {ren:?}"
        );
        // src is untouched (the reject wrote nothing), dst was not created.
        let srcv = roundtrip(&mut c, &mut b, &[b"GET", src.as_bytes()]).await;
        assert_eq!(
            srcv,
            Resp::Bulk(Some(b"val".to_vec())),
            "src untouched by a rejected RENAME"
        );
        let dstv = roundtrip(&mut c, &mut b, &[b"EXISTS", dst.as_bytes()]).await;
        assert_eq!(
            dstv,
            Resp::Integer(0),
            "dst not created by a rejected RENAME"
        );

        // Spanning COPY -> a LOUD error.
        let copy = roundtrip(&mut c, &mut b, &[b"COPY", src.as_bytes(), dst.as_bytes()]).await;
        assert!(
            matches!(copy, Resp::Error(_)),
            "spanning COPY must be a loud error, got {copy:?}"
        );

        // Spanning LMPOP -> a LOUD error.
        let lmpop = roundtrip(
            &mut c,
            &mut b,
            &[b"LMPOP", b"2", src.as_bytes(), dst.as_bytes(), b"LEFT"],
        )
        .await;
        assert!(
            matches!(lmpop, Resp::Error(_)),
            "spanning LMPOP must be a loud error, got {lmpop:?}"
        );

        server.shutdown_and_join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// Co-located parity: same-shard SMOVE / LMOVE / MSETNX behave like single-shard.
// ---------------------------------------------------------------------------

#[test]
fn colocated_moves_are_unchanged() {
    let r = rt();
    let local = tokio::task::LocalSet::new();
    local.block_on(&r, async {
        let (server, port) = boot(SHARDS);
        let mut c = connect_retry(port).await;
        let mut b = Vec::new();

        let (a, b2) = colocated_pair();
        // Co-located SMOVE.
        roundtrip(&mut c, &mut b, &[b"SADD", a.as_bytes(), b"m"]).await;
        let sm = roundtrip(
            &mut c,
            &mut b,
            &[b"SMOVE", a.as_bytes(), b2.as_bytes(), b"m"],
        )
        .await;
        assert_eq!(sm, Resp::Integer(1), "co-located SMOVE :1");
        let in_dst = roundtrip(&mut c, &mut b, &[b"SISMEMBER", b2.as_bytes(), b"m"]).await;
        assert_eq!(in_dst, Resp::Integer(1));

        // Co-located RENAME works (it is NOT spanning, so no reject).
        roundtrip(&mut c, &mut b, &[b"SET", a.as_bytes(), b"v"]).await;
        let ren = roundtrip(&mut c, &mut b, &[b"RENAME", a.as_bytes(), b2.as_bytes()]).await;
        assert_eq!(ren, Resp::Simple(b"OK".to_vec()), "co-located RENAME +OK");
        let got = roundtrip(&mut c, &mut b, &[b"GET", b2.as_bytes()]).await;
        assert_eq!(
            got,
            Resp::Bulk(Some(b"v".to_vec())),
            "co-located RENAME moved the value"
        );

        // Co-located MSETNX.
        let mk0 = key_on_shard("co", 2);
        let mk1 = key_on_shard("co2", 2);
        assert_eq!(
            owner_shard(mk0.as_bytes(), SHARDS),
            owner_shard(mk1.as_bytes(), SHARDS)
        );
        let ok = roundtrip(
            &mut c,
            &mut b,
            &[b"MSETNX", mk0.as_bytes(), b"x", mk1.as_bytes(), b"y"],
        )
        .await;
        assert_eq!(ok, Resp::Integer(1), "co-located MSETNX :1");

        server.shutdown_and_join().unwrap();
    });
}
