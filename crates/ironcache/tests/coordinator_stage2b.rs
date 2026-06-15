// SPDX-License-Identifier: MIT OR Apache-2.0
//! Cross-shard coordinator Stage 2b-1 (spanning SET algebra + STORE) acceptance tests
//! (COORDINATOR.md #107).
//!
//! These boot the REAL multi-shard `run_server` on an ephemeral port (the actual
//! SO_REUSEPORT thread-per-core topology) and drive it over real sockets, so they exercise
//! the whole spanning path: classify -> command_keys -> owner_shard_set==None ->
//! is_fan_out_spanning_combine -> spanning_combine::fan_out_set (gather each source via
//! SMEMBERS on its owner -> shared set_combine -> for *STORE write the result to the dest
//! owner via the internal __ICSTORESET / DEL) -> home-core encode.
//!
//! The headline guards: cross-shard == single-shard PARITY (byte-identical replies on N
//! shards vs 1), the *STORE dest write + TTL clear, the empty-result dest-delete, the
//! WRONGTYPE-source ABORT leaving dest untouched, SINTERCARD LIMIT, and that the internal
//! `__ICSTORESET` verb is NOT client-reachable.

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
// A minimal RESP2/RESP3 reader (enough for the shapes these commands return).
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
        // RESP2 set degrades to an array; under RESP2 the server replies '*' for a set.
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

/// Send `parts` and read one complete reply.
async fn roundtrip(client: &mut TcpStream, buf: &mut Vec<u8>, parts: &[&[u8]]) -> Resp {
    send_cmd(client, parts).await;
    read_reply(client, buf).await
}

/// `SADD key m...` -> the integer reply (added count).
async fn sadd(client: &mut TcpStream, buf: &mut Vec<u8>, key: &str, members: &[&str]) {
    let mut parts: Vec<&[u8]> = vec![b"SADD", key.as_bytes()];
    for m in members {
        parts.push(m.as_bytes());
    }
    let r = roundtrip(client, buf, &parts).await;
    assert!(
        matches!(r, Resp::Integer(_)),
        "SADD {key} must reply integer, got {r:?}"
    );
}

/// The SORTED bulk members of an array/set reply (the result ordering is the server's
/// BTreeSet order, but we sort to compare order-independent set equality regardless).
fn sorted_members(r: &Resp) -> Vec<Vec<u8>> {
    let items = match r {
        Resp::Array(Some(items)) => items,
        other => panic!("expected an array/set reply, got {other:?}"),
    };
    let mut out: Vec<Vec<u8>> = items
        .iter()
        .map(|i| match i {
            Resp::Bulk(Some(b)) => b.clone(),
            other => panic!("non-bulk in set reply: {other:?}"),
        })
        .collect();
    out.sort();
    out
}

/// `SMEMBERS key` sorted.
async fn smembers_sorted(client: &mut TcpStream, buf: &mut Vec<u8>, key: &str) -> Vec<Vec<u8>> {
    let r = roundtrip(client, buf, &[b"SMEMBERS", key.as_bytes()]).await;
    sorted_members(&r)
}

// ---------------------------------------------------------------------------
// Shared workload: load three overlapping sets across keys that SPAN shards (with N
// shards) and exactly the same logical data on a single shard, then assert the algebra
// replies match byte-for-byte.
// ---------------------------------------------------------------------------

/// Load three sets s:a, s:b, s:c with overlapping members (chosen so SINTER/SUNION/SDIFF
/// each have a non-trivial, non-empty result). With N>1 shards these keys land on
/// different shards (the spanning path); with 1 shard they co-locate (the single-shard
/// path) -- the parity tests run BOTH and compare.
async fn load_three_sets(client: &mut TcpStream, buf: &mut Vec<u8>) {
    // a = {1,2,3,4,5}, b = {3,4,5,6,7}, c = {4,5,8}.
    // SINTER(a,b,c) = {4,5}; SUNION = {1..8}; SDIFF(a,b,c) = {1,2}.
    sadd(client, buf, "s:a", &["1", "2", "3", "4", "5"]).await;
    sadd(client, buf, "s:b", &["3", "4", "5", "6", "7"]).await;
    sadd(client, buf, "s:c", &["4", "5", "8"]).await;
}

/// Run SINTER/SUNION/SDIFF/SINTERCARD over (s:a, s:b, s:c) and return the four replies, for
/// a parity comparison between a multi-shard and a single-shard server.
async fn run_algebra(client: &mut TcpStream, buf: &mut Vec<u8>) -> (Resp, Resp, Resp, Resp) {
    let sinter = roundtrip(client, buf, &[b"SINTER", b"s:a", b"s:b", b"s:c"]).await;
    let sunion = roundtrip(client, buf, &[b"SUNION", b"s:a", b"s:b", b"s:c"]).await;
    let sdiff = roundtrip(client, buf, &[b"SDIFF", b"s:a", b"s:b", b"s:c"]).await;
    let sintercard = roundtrip(client, buf, &[b"SINTERCARD", b"3", b"s:a", b"s:b", b"s:c"]).await;
    (sinter, sunion, sdiff, sintercard)
}

#[test]
fn spanning_set_algebra_matches_single_shard_byte_for_byte() {
    // PARITY: the same data + the same commands on a multi-shard server (keys span shards ->
    // the gather/combine path) vs a single-shard server (the in-store algebra) must produce
    // IDENTICAL replies. We sort the set replies (the wire order is unspecified to clients,
    // and we want to assert SET equality), and compare the cardinality directly.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        // Multi-shard.
        let (server_n, port_n) = boot(5);
        let mut cn = connect_retry(port_n).await;
        let mut bn = Vec::new();
        load_three_sets(&mut cn, &mut bn).await;
        let (inter_n, union_n, diff_n, card_n) = run_algebra(&mut cn, &mut bn).await;

        // Single-shard.
        let (server_1, port_1) = boot(1);
        let mut c1 = connect_retry(port_1).await;
        let mut b1 = Vec::new();
        load_three_sets(&mut c1, &mut b1).await;
        let (inter_1, union_1, diff_1, card_1) = run_algebra(&mut c1, &mut b1).await;

        // The set replies match (sorted set-equality) and equal the expected algebra.
        assert_eq!(
            sorted_members(&inter_n),
            sorted_members(&inter_1),
            "SINTER parity"
        );
        assert_eq!(sorted_members(&inter_n), vec![b"4".to_vec(), b"5".to_vec()]);
        assert_eq!(
            sorted_members(&union_n),
            sorted_members(&union_1),
            "SUNION parity"
        );
        assert_eq!(
            sorted_members(&union_n),
            (1..=8)
                .map(|i| i.to_string().into_bytes())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            sorted_members(&diff_n),
            sorted_members(&diff_1),
            "SDIFF parity"
        );
        assert_eq!(sorted_members(&diff_n), vec![b"1".to_vec(), b"2".to_vec()]);
        // SINTERCARD = |SINTER| = 2.
        assert_eq!(card_n, card_1, "SINTERCARD parity");
        assert_eq!(card_n, Resp::Integer(2));

        drop(cn);
        drop(c1);
        server_n.shutdown_and_join().unwrap();
        server_1.shutdown_and_join().unwrap();
    });
}

#[test]
fn spanning_store_writes_dest_matches_single_shard_and_clears_ttl() {
    // *STORE: spanning SINTERSTORE/SUNIONSTORE/SDIFFSTORE write the result to dest. The dest
    // read-back (SMEMBERS) must equal the single-shard dest, the reply is the cardinality,
    // and a pre-existing TTL on dest is CLEARED by the store (blind overwrite).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        for &shards in &[5usize, 1usize] {
            let (server, port) = boot(shards);
            let mut c = connect_retry(port).await;
            let mut buf = Vec::new();
            load_three_sets(&mut c, &mut buf).await;

            // Pre-create dest keys as STRINGS with a TTL, to prove the *STORE blindly
            // overwrites the type AND clears the TTL (matching the single-shard store).
            for dest in ["d:inter", "d:union", "d:diff"] {
                let r = roundtrip(
                    &mut c,
                    &mut buf,
                    &[b"SET", dest.as_bytes(), b"old", b"EX", b"100"],
                )
                .await;
                assert_eq!(r, Resp::Simple(b"OK".to_vec()));
            }

            // SINTERSTORE d:inter s:a s:b s:c -> 2 ({4,5}).
            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"SINTERSTORE", b"d:inter", b"s:a", b"s:b", b"s:c"],
            )
            .await;
            assert_eq!(r, Resp::Integer(2), "SINTERSTORE card (shards={shards})");
            assert_eq!(
                smembers_sorted(&mut c, &mut buf, "d:inter").await,
                vec![b"4".to_vec(), b"5".to_vec()],
                "SINTERSTORE dest content (shards={shards})"
            );

            // SUNIONSTORE d:union s:a s:b s:c -> 8.
            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"SUNIONSTORE", b"d:union", b"s:a", b"s:b", b"s:c"],
            )
            .await;
            assert_eq!(r, Resp::Integer(8), "SUNIONSTORE card (shards={shards})");
            assert_eq!(
                smembers_sorted(&mut c, &mut buf, "d:union").await,
                (1..=8)
                    .map(|i| i.to_string().into_bytes())
                    .collect::<Vec<_>>(),
                "SUNIONSTORE dest content (shards={shards})"
            );

            // SDIFFSTORE d:diff s:a s:b s:c -> 2 ({1,2}).
            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"SDIFFSTORE", b"d:diff", b"s:a", b"s:b", b"s:c"],
            )
            .await;
            assert_eq!(r, Resp::Integer(2), "SDIFFSTORE card (shards={shards})");
            assert_eq!(
                smembers_sorted(&mut c, &mut buf, "d:diff").await,
                vec![b"1".to_vec(), b"2".to_vec()],
                "SDIFFSTORE dest content (shards={shards})"
            );

            // TTL must be CLEARED on every overwritten dest (TTL -1 = no expiry, since the
            // key exists as a set now).
            for dest in ["d:inter", "d:union", "d:diff"] {
                let r = roundtrip(&mut c, &mut buf, &[b"TTL", dest.as_bytes()]).await;
                assert_eq!(
                    r,
                    Resp::Integer(-1),
                    "*STORE must clear dest TTL (dest={dest}, shards={shards})"
                );
                // And the type is now a set, not the old string.
                let t = roundtrip(&mut c, &mut buf, &[b"TYPE", dest.as_bytes()]).await;
                assert_eq!(
                    t,
                    Resp::Simple(b"set".to_vec()),
                    "dest is a set now (dest={dest})"
                );
            }

            drop(c);
            server.shutdown_and_join().unwrap();
        }
    });
}

#[test]
fn spanning_store_empty_result_deletes_dest_and_replies_zero() {
    // Empty-result dest-delete: a spanning SINTERSTORE whose intersection is EMPTY must
    // DELETE dest (EXISTS dest == 0) and reply 0 (Redis deletes dest on an empty result).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        for &shards in &[5usize, 1usize] {
            let (server, port) = boot(shards);
            let mut c = connect_retry(port).await;
            let mut buf = Vec::new();

            // Two DISJOINT sets spanning shards: intersection is empty.
            sadd(&mut c, &mut buf, "e:a", &["1", "2", "3"]).await;
            sadd(&mut c, &mut buf, "e:b", &["4", "5", "6"]).await;
            // Pre-create dest with content, to prove it is DELETED (not left stale).
            sadd(&mut c, &mut buf, "e:dest", &["stale"]).await;
            assert_eq!(
                roundtrip(&mut c, &mut buf, &[b"EXISTS", b"e:dest"]).await,
                Resp::Integer(1),
                "dest pre-exists (shards={shards})"
            );

            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"SINTERSTORE", b"e:dest", b"e:a", b"e:b"],
            )
            .await;
            assert_eq!(
                r,
                Resp::Integer(0),
                "empty SINTERSTORE replies 0 (shards={shards})"
            );
            assert_eq!(
                roundtrip(&mut c, &mut buf, &[b"EXISTS", b"e:dest"]).await,
                Resp::Integer(0),
                "empty SINTERSTORE deletes dest (shards={shards})"
            );

            drop(c);
            server.shutdown_and_join().unwrap();
        }
    });
}

#[test]
fn spanning_store_wrongtype_source_aborts_before_any_write() {
    // WRONGTYPE abort: one source is a STRING -> the whole *STORE returns WRONGTYPE and dest
    // is UNCHANGED (neither written nor deleted), matching single-node Redis (the type check
    // precedes the dest write).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        for &shards in &[5usize, 1usize] {
            let (server, port) = boot(shards);
            let mut c = connect_retry(port).await;
            let mut buf = Vec::new();

            sadd(&mut c, &mut buf, "w:a", &["1", "2", "3"]).await;
            // w:b is a STRING (a non-set source).
            assert_eq!(
                roundtrip(&mut c, &mut buf, &[b"SET", b"w:b", b"notaset"]).await,
                Resp::Simple(b"OK".to_vec())
            );
            // Pre-create dest with a known set, to prove it is UNTOUCHED on the abort.
            sadd(&mut c, &mut buf, "w:dest", &["keep"]).await;

            // SUNIONSTORE w:dest w:a w:b -> WRONGTYPE (w:b is a string).
            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"SUNIONSTORE", b"w:dest", b"w:a", b"w:b"],
            )
            .await;
            let Resp::Error(line) = r else {
                panic!("WRONGTYPE source must error, got {r:?} (shards={shards})");
            };
            assert!(
                String::from_utf8_lossy(&line).starts_with("WRONGTYPE"),
                "must be WRONGTYPE, got {:?} (shards={shards})",
                String::from_utf8_lossy(&line)
            );
            // dest UNCHANGED: still the original single member, not written/deleted.
            assert_eq!(
                smembers_sorted(&mut c, &mut buf, "w:dest").await,
                vec![b"keep".to_vec()],
                "dest must be untouched on WRONGTYPE abort (shards={shards})"
            );

            // Also for the READ form: SINTER with a wrong-type source is WRONGTYPE.
            let r = roundtrip(&mut c, &mut buf, &[b"SINTER", b"w:a", b"w:b"]).await;
            assert!(
                matches!(&r, Resp::Error(l) if String::from_utf8_lossy(l).starts_with("WRONGTYPE")),
                "SINTER wrong-type source must be WRONGTYPE, got {r:?}"
            );

            drop(c);
            server.shutdown_and_join().unwrap();
        }
    });
}

#[test]
fn spanning_sintercard_limit_caps_and_zero_is_unlimited() {
    // SINTERCARD LIMIT: LIMIT 0 = unlimited (the full intersection cardinality); LIMIT n caps
    // at n. Cross-shard and single-shard must agree.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        for &shards in &[5usize, 1usize] {
            let (server, port) = boot(shards);
            let mut c = connect_retry(port).await;
            let mut buf = Vec::new();

            // a,b share {2,3,4,5} (4 common members), spanning shards.
            sadd(&mut c, &mut buf, "lc:a", &["1", "2", "3", "4", "5"]).await;
            sadd(&mut c, &mut buf, "lc:b", &["2", "3", "4", "5", "6"]).await;

            // LIMIT 0 -> the full intersection cardinality (4).
            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"SINTERCARD", b"2", b"lc:a", b"lc:b", b"LIMIT", b"0"],
            )
            .await;
            assert_eq!(r, Resp::Integer(4), "LIMIT 0 = unlimited (shards={shards})");

            // No LIMIT -> also the full cardinality (4).
            let r = roundtrip(&mut c, &mut buf, &[b"SINTERCARD", b"2", b"lc:a", b"lc:b"]).await;
            assert_eq!(
                r,
                Resp::Integer(4),
                "no LIMIT = unlimited (shards={shards})"
            );

            // LIMIT 2 -> capped at 2.
            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"SINTERCARD", b"2", b"lc:a", b"lc:b", b"LIMIT", b"2"],
            )
            .await;
            assert_eq!(r, Resp::Integer(2), "LIMIT 2 caps (shards={shards})");

            // LIMIT 10 (> card) -> the full cardinality (4), not 10.
            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"SINTERCARD", b"2", b"lc:a", b"lc:b", b"LIMIT", b"10"],
            )
            .await;
            assert_eq!(r, Resp::Integer(4), "LIMIT > card = card (shards={shards})");

            drop(c);
            server.shutdown_and_join().unwrap();
        }
    });
}

#[test]
fn internal_icstoreset_verb_is_not_client_reachable() {
    // The internal __ICSTORESET verb (the coordinator's dest-write) MUST be unreachable from
    // a client: a client sending it gets the standard unknown-command error, NOT a successful
    // store. Tested on BOTH a multi-shard and a single-shard server (the client gate is at the
    // serve-loop router, which both topologies share).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        for &shards in &[5usize, 1usize] {
            let (server, port) = boot(shards);
            let mut c = connect_retry(port).await;
            let mut buf = Vec::new();

            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"__ICSTORESET", b"k", b"a", b"b"],
            )
            .await;
            let Resp::Error(line) = r else {
                panic!("__ICSTORESET must be rejected, got {r:?} (shards={shards})");
            };
            assert!(
                String::from_utf8_lossy(&line).starts_with("ERR unknown command"),
                "client __ICSTORESET must be unknown-command, got {:?} (shards={shards})",
                String::from_utf8_lossy(&line)
            );
            // And it did NOT create the key (the write never happened).
            assert_eq!(
                roundtrip(&mut c, &mut buf, &[b"EXISTS", b"k"]).await,
                Resp::Integer(0),
                "rejected __ICSTORESET must not have written the key (shards={shards})"
            );

            // Lowercase form too (commands are case-insensitive): also unknown-command.
            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"__icstoreset", b"k2", b"x"],
            )
            .await;
            assert!(
                matches!(&r, Resp::Error(l) if String::from_utf8_lossy(l).starts_with("ERR unknown command")),
                "lowercase __icstoreset must be unknown-command too, got {r:?}"
            );

            drop(c);
            server.shutdown_and_join().unwrap();
        }
    });
}
