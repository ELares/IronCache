// SPDX-License-Identifier: MIT OR Apache-2.0
//! Cross-shard coordinator Stage 2a (multi-key DATA fan-out) acceptance tests
//! (COORDINATOR.md #107).
//!
//! These boot the REAL multi-shard `run_server` on an ephemeral port (the actual
//! SO_REUSEPORT thread-per-core topology) and drive it over real sockets, so they
//! exercise the whole spanning path: classify -> command_keys -> owner_shard_set==None ->
//! group-by-owner -> fan_out_split -> per-shard sub-request (home LOCAL + remote drain) ->
//! reassemble -> home-core encode.
//!
//! Stage 2a fans out exactly SIX commands when their keys SPAN shards: MGET, MSET, DEL,
//! EXISTS, UNLINK, TOUCH. The order-preservation regression guard for MGET (the reassembly
//! restores ORIGINAL argument order, not shard order) is the headline test here.

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
// A minimal RESP2/RESP3 reader (enough for the shapes the six commands return).
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
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

/// `SET key val` -> +OK.
async fn set(client: &mut TcpStream, buf: &mut Vec<u8>, key: &str, val: &str) {
    let r = roundtrip(client, buf, &[b"SET", key.as_bytes(), val.as_bytes()]).await;
    assert_eq!(r, Resp::Simple(b"OK".to_vec()), "SET {key} must be +OK");
}

/// `GET key` -> the bulk body (or None).
async fn get(client: &mut TcpStream, buf: &mut Vec<u8>, key: &str) -> Option<Vec<u8>> {
    match roundtrip(client, buf, &[b"GET", key.as_bytes()]).await {
        Resp::Bulk(b) => b,
        Resp::Null => None,
        other => panic!("GET {key} unexpected: {other:?}"),
    }
}

#[test]
fn mget_across_shards_preserves_requested_order_with_misses_and_wrong_type() {
    // THE order-preservation regression guard: SET several keys spanning shards, then MGET
    // them in a MIXED order; the returned array must match the values IN THE REQUESTED
    // ORDER (not shard order). Include MISSING keys (-> Null) and a NON-STRING key (-> Null,
    // no WRONGTYPE error).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        // Populate 12 keys that span shards (k0..k11), each value tagged by index.
        for i in 0..12 {
            set(&mut c, &mut buf, &format!("mg:{i}"), &format!("val{i}")).await;
        }
        // A non-string key (a list) to prove MGET returns Null (not WRONGTYPE) for it.
        let r = roundtrip(&mut c, &mut buf, &[b"RPUSH", b"mg:list", b"a"]).await;
        assert!(matches!(r, Resp::Integer(1)), "RPUSH setup must succeed");

        // MGET in a DELIBERATELY MIXED, NON-SORTED order, with a missing key and the list
        // key interleaved. The expected reply is the value for each requested key IN ORDER.
        let request_order: Vec<String> = vec![
            "mg:7".into(),       // val7
            "mg:0".into(),       // val0
            "mg:missing1".into(),// Null (absent)
            "mg:11".into(),      // val11
            "mg:list".into(),    // Null (non-string -> NOT WRONGTYPE)
            "mg:3".into(),       // val3
            "mg:5".into(),       // val5
            "mg:missing2".into(),// Null (absent)
            "mg:1".into(),       // val1
        ];
        let mut parts: Vec<&[u8]> = vec![b"MGET"];
        for k in &request_order {
            parts.push(k.as_bytes());
        }
        let reply = roundtrip(&mut c, &mut buf, &parts).await;
        let Resp::Array(Some(items)) = reply else {
            panic!("MGET must return an array, got {reply:?}");
        };
        assert_eq!(items.len(), request_order.len(), "one element per requested key");

        let expected: Vec<Resp> = vec![
            Resp::Bulk(Some(b"val7".to_vec())),
            Resp::Bulk(Some(b"val0".to_vec())),
            Resp::Bulk(None), // missing1 (RESP2 null bulk)
            Resp::Bulk(Some(b"val11".to_vec())),
            Resp::Bulk(None), // the list key -> Null, NOT an error
            Resp::Bulk(Some(b"val3".to_vec())),
            Resp::Bulk(Some(b"val5".to_vec())),
            Resp::Bulk(None), // missing2
            Resp::Bulk(Some(b"val1".to_vec())),
        ];
        assert_eq!(
            items, expected,
            "MGET must restore the REQUESTED argument order (not shard order), with misses and a non-string key as Null"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn mset_across_shards_sets_every_key_and_returns_ok() {
    // MSET k1 v1 k2 v2 ... spanning shards in ONE call -> +OK; then GET each (routes to its
    // owner) returns the right value. Odd arg count -> wrong-arity error.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        // 10 pairs spanning shards in one MSET.
        let keys: Vec<String> = (0..10).map(|i| format!("ms:{i}")).collect();
        let vals: Vec<String> = (0..10).map(|i| format!("v{i}")).collect();
        let mut parts: Vec<&[u8]> = vec![b"MSET"];
        for i in 0..10 {
            parts.push(keys[i].as_bytes());
            parts.push(vals[i].as_bytes());
        }
        let r = roundtrip(&mut c, &mut buf, &parts).await;
        assert_eq!(r, Resp::Simple(b"OK".to_vec()), "MSET spanning must be +OK");

        // Each key must read back its value (GET routes to the owning shard).
        for i in 0..10 {
            let got = get(&mut c, &mut buf, &keys[i]).await;
            assert_eq!(
                got,
                Some(vals[i].clone().into_bytes()),
                "MSET-set key {} must read back its value",
                keys[i]
            );
        }

        // Odd arg count (a dangling key with no value) -> the wrong-arity error.
        let odd = roundtrip(&mut c, &mut buf, &[b"MSET", b"x", b"1", b"y"]).await;
        let Resp::Error(line) = odd else {
            panic!("odd-arg MSET must be an error, got {odd:?}");
        };
        assert_eq!(
            String::from_utf8(line).unwrap(),
            "ERR wrong number of arguments for 'mset' command",
            "odd MSET must be the mset wrong-arity error"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn del_exists_unlink_touch_across_shards_sum_correctly() {
    // Populate keys spanning shards; DEL/EXISTS/UNLINK/TOUCH over a spanning mix return the
    // correct total count, and DEL/UNLINK actually remove the keys.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(5);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        // 8 present keys spanning shards.
        let present: Vec<String> = (0..8).map(|i| format!("dx:{i}")).collect();
        for k in &present {
            set(&mut c, &mut buf, k, "v").await;
        }

        // EXISTS over a MIX of present + absent spanning keys: the count is the number of
        // PRESENT keys (EXISTS counts repeats, but here all distinct).
        let exists_args: Vec<&[u8]> = {
            let mut v: Vec<&[u8]> = vec![b"EXISTS"];
            v.push(present[0].as_bytes());
            v.push(b"dx:absent:a");
            v.push(present[3].as_bytes());
            v.push(present[7].as_bytes());
            v.push(b"dx:absent:b");
            v.push(present[5].as_bytes());
            v
        };
        let r = roundtrip(&mut c, &mut buf, &exists_args).await;
        assert_eq!(
            r,
            Resp::Integer(4),
            "EXISTS over a spanning present/absent mix must sum the present ones"
        );

        // TOUCH the same mix: same count (TOUCH counts live keys).
        let mut touch_args: Vec<&[u8]> = vec![b"TOUCH"];
        touch_args.extend_from_slice(&exists_args[1..]);
        let r = roundtrip(&mut c, &mut buf, &touch_args).await;
        assert_eq!(r, Resp::Integer(4), "TOUCH spanning must sum live keys");

        // EXISTS counting REPEATS across shards: EXISTS k k (present) -> 2.
        let r = roundtrip(
            &mut c,
            &mut buf,
            &[b"EXISTS", present[0].as_bytes(), present[0].as_bytes()],
        )
        .await;
        assert_eq!(r, Resp::Integer(2), "EXISTS counts repeats");

        // DEL a spanning subset (the first 3) -> 3, and they are gone.
        let r = roundtrip(
            &mut c,
            &mut buf,
            &[
                b"DEL",
                present[0].as_bytes(),
                present[1].as_bytes(),
                present[2].as_bytes(),
            ],
        )
        .await;
        assert_eq!(
            r,
            Resp::Integer(3),
            "DEL spanning must return the total removed"
        );
        for k in &present[0..3] {
            assert_eq!(get(&mut c, &mut buf, k).await, None, "{k} gone after DEL");
        }

        // UNLINK a spanning subset (next 3) -> 3, and they are gone. Includes one already
        // -absent key so the count is the live ones only.
        let r = roundtrip(
            &mut c,
            &mut buf,
            &[
                b"UNLINK",
                present[3].as_bytes(),
                present[4].as_bytes(),
                b"dx:absent:c",
                present[5].as_bytes(),
            ],
        )
        .await;
        assert_eq!(r, Resp::Integer(3), "UNLINK spanning sums live keys only");
        for k in &present[3..6] {
            assert_eq!(
                get(&mut c, &mut buf, k).await,
                None,
                "{k} gone after UNLINK"
            );
        }

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn co_located_multikey_still_works_via_stage1() {
    // Co-located: MGET / DEL with all keys on ONE shard still route via Stage 1 (the single
    // owner). We force co-location by using the SAME key repeated (trivially co-located) and
    // by a single-key invocation, both of which owner_shard_set collapses to one shard.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        set(&mut c, &mut buf, "co:1", "a").await;

        // MGET of the SAME key twice: co-located (same owner) -> [a, a], routed via Stage 1.
        let r = roundtrip(&mut c, &mut buf, &[b"MGET", b"co:1", b"co:1"]).await;
        assert_eq!(
            r,
            Resp::Array(Some(vec![
                Resp::Bulk(Some(b"a".to_vec())),
                Resp::Bulk(Some(b"a".to_vec())),
            ])),
            "co-located MGET (same key) must work via Stage 1"
        );

        // DEL of the same key twice: only one live key, removed once -> 1 (the co-located
        // single-owner path; the second occurrence finds it already gone).
        let r = roundtrip(&mut c, &mut buf, &[b"DEL", b"co:1", b"co:1"]).await;
        assert_eq!(
            r,
            Resp::Integer(1),
            "co-located DEL of one live key returns 1"
        );
        assert_eq!(get(&mut c, &mut buf, "co:1").await, None);

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn single_key_mget_mset_route_via_stage1() {
    // Single-key MGET / MSET (MGET k ; MSET k v) route via Stage 1 (co-located: one key,
    // one owner) and work.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        // MSET with a single pair.
        let r = roundtrip(&mut c, &mut buf, &[b"MSET", b"sk:1", b"hello"]).await;
        assert_eq!(r, Resp::Simple(b"OK".to_vec()), "single-pair MSET -> +OK");
        assert_eq!(
            get(&mut c, &mut buf, "sk:1").await,
            Some(b"hello".to_vec()),
            "single-key MSET set the value"
        );

        // MGET with a single key.
        let r = roundtrip(&mut c, &mut buf, &[b"MGET", b"sk:1"]).await;
        assert_eq!(
            r,
            Resp::Array(Some(vec![Resp::Bulk(Some(b"hello".to_vec()))])),
            "single-key MGET returns a one-element array"
        );
        // Single-key MGET of an absent key -> [Null].
        let r = roundtrip(&mut c, &mut buf, &[b"MGET", b"sk:absent"]).await;
        assert_eq!(r, Resp::Array(Some(vec![Resp::Bulk(None)])));

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn shards_one_parity_for_the_six_commands() {
    // shards == 1 parity: every key is home-owned -> co-located -> the six commands take the
    // Stage 1 / home path (never the fan-out) and behave identically.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(1);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        // MSET several pairs.
        let r = roundtrip(
            &mut c,
            &mut buf,
            &[b"MSET", b"o:1", b"a", b"o:2", b"b", b"o:3", b"c"],
        )
        .await;
        assert_eq!(r, Resp::Simple(b"OK".to_vec()));

        // MGET in mixed order with a miss.
        let r = roundtrip(&mut c, &mut buf, &[b"MGET", b"o:3", b"o:miss", b"o:1"]).await;
        assert_eq!(
            r,
            Resp::Array(Some(vec![
                Resp::Bulk(Some(b"c".to_vec())),
                Resp::Bulk(None),
                Resp::Bulk(Some(b"a".to_vec())),
            ])),
            "shards==1 MGET preserves order with a miss"
        );

        // EXISTS / TOUCH / DEL / UNLINK.
        assert_eq!(
            roundtrip(&mut c, &mut buf, &[b"EXISTS", b"o:1", b"o:2", b"o:miss"]).await,
            Resp::Integer(2)
        );
        assert_eq!(
            roundtrip(&mut c, &mut buf, &[b"TOUCH", b"o:1", b"o:3"]).await,
            Resp::Integer(2)
        );
        assert_eq!(
            roundtrip(&mut c, &mut buf, &[b"DEL", b"o:1", b"o:2"]).await,
            Resp::Integer(2)
        );
        assert_eq!(
            roundtrip(&mut c, &mut buf, &[b"UNLINK", b"o:3", b"o:miss"]).await,
            Resp::Integer(1)
        );

        // Odd MSET is still a wrong-arity error at shards==1.
        let odd = roundtrip(&mut c, &mut buf, &[b"MSET", b"a", b"1", b"b"]).await;
        assert!(
            matches!(odd, Resp::Error(_)),
            "odd MSET is a wrong-arity error"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn other_spanning_multikey_still_stays_home_unchanged() {
    // The Stage 2b/2c gap is UNCHANGED: a spanning multi-key command that is NOT one of the
    // six (e.g. PFCOUNT over keys spanning shards) stays on the home sync fall-through. We
    // only assert it does NOT panic and returns a WELL-FORMED reply (the home shard's
    // partial may be partially wrong, exactly as the Stage 1 spanning-DEL test documented
    // before Stage 2a covered DEL). This guards that we did not accidentally route a
    // non-six spanning command through the new fan-out.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut buf = Vec::new();

        // Build several HLLs spanning shards, then PFCOUNT over them (a spanning multi-key
        // read that STAYS HOME this stage). Just assert a well-formed integer, no panic.
        for i in 0..16 {
            let r = roundtrip(
                &mut c,
                &mut buf,
                &[b"PFADD", format!("hll:{i}").as_bytes(), b"x", b"y"],
            )
            .await;
            assert!(matches!(r, Resp::Integer(_)), "PFADD setup ok");
        }
        let pfcount_keys: Vec<String> = (0..16).map(|i| format!("hll:{i}")).collect();
        let mut parts: Vec<&[u8]> = vec![b"PFCOUNT"];
        for k in &pfcount_keys {
            parts.push(k.as_bytes());
        }
        let r = roundtrip(&mut c, &mut buf, &parts).await;
        assert!(
            matches!(r, Resp::Integer(_)),
            "spanning PFCOUNT must stay home and return a well-formed integer (Stage 2b gap), got {r:?}"
        );

        drop(c);
        // A clean join proves no shard thread panicked.
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn cross_connection_mset_then_mget_spanning() {
    // MSET on connection A spanning shards; MGET on a FRESH connection B (likely a different
    // home shard) sees all the values in order. Proves the fan-out writes land on the owning
    // shards (not the issuing connection's home) and any connection reads them back.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut a = connect_retry(port).await;
        let mut b = connect_retry(port).await;
        let mut abuf = Vec::new();
        let mut bbuf = Vec::new();

        let keys: Vec<String> = (0..9).map(|i| format!("xc2:{i}")).collect();
        let vals: Vec<String> = (0..9).map(|i| format!("w{i}")).collect();
        let mut mset: Vec<&[u8]> = vec![b"MSET"];
        for i in 0..9 {
            mset.push(keys[i].as_bytes());
            mset.push(vals[i].as_bytes());
        }
        assert_eq!(
            roundtrip(&mut a, &mut abuf, &mset).await,
            Resp::Simple(b"OK".to_vec())
        );

        // MGET on B in reverse order: must see every value in the REQUESTED (reverse) order.
        let mut mget: Vec<&[u8]> = vec![b"MGET"];
        for i in (0..9).rev() {
            mget.push(keys[i].as_bytes());
        }
        let Resp::Array(Some(items)) = roundtrip(&mut b, &mut bbuf, &mget).await else {
            panic!("MGET must be an array");
        };
        let expected: Vec<Resp> = (0..9)
            .rev()
            .map(|i| Resp::Bulk(Some(vals[i].clone().into_bytes())))
            .collect();
        assert_eq!(
            items, expected,
            "cross-connection MGET (reverse order) must see all MSET values in requested order"
        );

        drop(a);
        drop(b);
        server.shutdown_and_join().unwrap();
    });
}
