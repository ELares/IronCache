// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-6 MULTI-SHARD online-slot-migration presence exactness (COORDINATOR.md #107).
//!
//! The migration SOURCE's `-ASK` decision must classify each migrating-slot key present/absent
//! against the shard that OWNS it (the internal FNV `owner_shard`), which on a MULTI-shard node may
//! be a SIBLING of the connection's accept shard (the kernel SO_REUSEPORT picks the accept shard at
//! random). Before the cross-shard presence hop, the source read ONLY its accept-shard store, so a
//! key that is PRESENT on a sibling shard could be mis-reported ABSENT and answered with a spurious
//! `-ASK` whenever the connection homed to a non-owner shard.
//!
//! This boots ONE REAL multi-shard `run_server` in static CLUSTER mode owning the whole slot space
//! (a second topology node is the migration DEST so `MIGRATING <dest>` resolves), writes a key,
//! drives `CLUSTER SETSLOT <slot> MIGRATING <dest>`, then GETs the key over MANY fresh connections
//! (so accept shards spread across all shards). With the fix EVERY connection SERVES the present key
//! (never `-ASK`), because presence is resolved on the key's FNV owner shard. A genuinely ABSENT key
//! in the migrating slot is `-ASK`'d to the dest (the redirect itself still works). The "many
//! connections, never `-ASK` a present key" assertion is exactly what FAILS on the old
//! accept-shard-only behavior (some connection homes to a sibling shard and mis-ASKs).

use ironcache::test_support::run_cluster_node_for_test_shards;
use ironcache_config::{ClusterNode, ClusterTopology};
use ironcache_protocol::key_slot;
use ironcache_server::route::owner_shard;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SRC_ID: &str = "1111111111111111111111111111111111111111";
const DST_ID: &str = "2222222222222222222222222222222222222222";

/// The number of serve shards on the source node. > 1 so a key's FNV owner shard can be a SIBLING
/// of a connection's (random) accept shard -- the multi-shard topology the fix targets.
const SHARDS: usize = 4;

/// Grab a free TCP port (small TOCTOU window before `run_server` re-binds; fine on loopback).
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Run `body` on a fresh current-thread tokio runtime + `LocalSet` (the same harness the cluster /
/// coordinator integration tests use): the server lives on its own threads, the client driver here
/// is single-threaded.
fn with_runtime<F>(body: F)
where
    F: std::future::Future<Output = ()>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, body);
}

/// Connect with a few short retries: the shards bind asynchronously after `run_server` returns.
async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = s.set_nodelay(true);
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("cluster node never came up on port {port}");
}

/// Read once and return the raw reply bytes (the small replies here fit a single read).
async fn read_raw(client: &mut TcpStream) -> Vec<u8> {
    let mut buf = [0u8; 4096];
    let n = client.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

/// `SET key val` (RESP2 array); return the raw reply.
async fn set_raw(client: &mut TcpStream, key: &str, val: &str) -> Vec<u8> {
    let frame = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        val.len(),
        val
    );
    client.write_all(frame.as_bytes()).await.unwrap();
    read_raw(client).await
}

/// `GET key` (RESP2 array); return the raw reply.
async fn get_raw(client: &mut TcpStream, key: &str) -> Vec<u8> {
    let frame = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client.write_all(frame.as_bytes()).await.unwrap();
    read_raw(client).await
}

/// Send a raw command frame built from `args` and return the raw reply.
async fn cmd_raw(client: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut frame = format!("*{}\r\n", args.len());
    for a in args {
        write!(frame, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    client.write_all(frame.as_bytes()).await.unwrap();
    read_raw(client).await
}

/// Brute-force a short key whose FNV `owner_shard` is NOT shard 0 (so on a 4-shard node it lives on
/// a sibling of at least three of the four possible accept shards). The CRC16 slot is incidental
/// (the source owns every slot), but is returned so the test can drive `SETSLOT MIGRATING` on it.
fn key_owned_by_nonzero_shard() -> (String, u16, usize) {
    for i in 0..1_000_000u32 {
        let k = format!("k{i}");
        let owner = owner_shard(k.as_bytes(), SHARDS);
        if owner != 0 {
            let slot = key_slot(k.as_bytes());
            return (k, slot, owner);
        }
    }
    panic!("no key found whose FNV owner shard is non-zero");
}

/// The single-node static topology: SRC (this node) owns the WHOLE slot space; DST is a second node
/// (the migration destination) that owns nothing but must EXIST so `MIGRATING <DST>` resolves to a
/// real advertised endpoint for the `-ASK` redirect target. Both advertise 127.0.0.1.
fn topology(src_port: u16, dst_port: u16) -> ClusterTopology {
    ClusterTopology {
        nodes: vec![
            ClusterNode {
                id: SRC_ID.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: src_port,
                slots: vec![[0, 16383]],
            },
            ClusterNode {
                id: DST_ID.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: dst_port,
                slots: vec![],
            },
        ],
    }
}

#[test]
fn multishard_migrating_slot_serves_a_sibling_shard_key_never_spurious_ask() {
    with_runtime(async {
        run_multishard_presence_body().await;
    });
}

async fn run_multishard_presence_body() {
    let src_port = free_port();
    let dst_port = free_port();
    let src =
        run_cluster_node_for_test_shards(src_port, SHARDS, topology(src_port, dst_port), SRC_ID);

    // The key whose FNV owner shard is NOT shard 0 (so it can live on a sibling of a random accept
    // shard), plus its CRC16 slot (which the source owns and we tag MIGRATING).
    let (key, slot, owner) = key_owned_by_nonzero_shard();
    assert_ne!(owner, 0, "the test key must NOT be owned by shard 0");

    // (1) Write the key. The SET is routed to the key's FNV owner shard by the coordinator
    // regardless of which shard this connection accepted on, so the value lands on `owner`.
    {
        let mut c = connect_retry(src_port).await;
        let r = set_raw(&mut c, &key, "v").await;
        assert_eq!(
            &r, b"+OK\r\n",
            "SET should succeed (the node owns every slot)"
        );
    }

    // (2) Tag the key's slot MIGRATING toward DST. Now the source's `-ASK` decision depends on
    // whether the key is present on the shard that OWNS it (a SIBLING of most accept shards).
    {
        let mut c = connect_retry(src_port).await;
        let r = cmd_raw(
            &mut c,
            &["CLUSTER", "SETSLOT", &slot.to_string(), "MIGRATING", DST_ID],
        )
        .await;
        assert_eq!(
            &r,
            b"+OK\r\n",
            "SETSLOT MIGRATING should succeed on a static cluster node, got {:?}",
            String::from_utf8_lossy(&r)
        );
    }

    // (3) GET the PRESENT key over MANY fresh connections, so the random SO_REUSEPORT accept shard
    // spreads across all shards. With the cross-shard presence fix EVERY connection SERVES the value
    // (the source resolves presence on the FNV owner shard); the OLD accept-shard-only read would
    // `-ASK` on every connection that homed to a shard other than `owner` (a present key mis-reported
    // absent). We make 40 attempts: on a 4-shard node the probability ALL 40 happen to home on the
    // single owner shard (and so never trip the old bug) is ~(1/4)^... negligible; in practice the
    // accept shard varies connection to connection.
    let bulk_v = b"$1\r\nv\r\n".to_vec();
    let mut served = 0usize;
    for _ in 0..40 {
        let mut c = connect_retry(src_port).await;
        let r = get_raw(&mut c, &key).await;
        assert!(
            !r.starts_with(b"-ASK"),
            "a PRESENT migrating-slot key must be SERVED, never -ASK'd (multi-shard exactness); got {:?}",
            String::from_utf8_lossy(&r)
        );
        assert_eq!(
            r,
            bulk_v,
            "the present key must return its value on every connection; got {:?}",
            String::from_utf8_lossy(&r)
        );
        served += 1;
    }
    assert_eq!(served, 40, "every GET of the present key must be served");

    // (4) A GENUINELY ABSENT key in a MIGRATING slot is still `-ASK`'d to the dest (the redirect
    // itself works; only the presence classification became exact). Find an absent key on a
    // sibling-owned slot, tag its slot MIGRATING, and confirm `-ASK <slot> 127.0.0.1:<dst_port>`.
    let (absent_key, absent_slot, absent_owner) = {
        // A different key (never written) whose FNV owner is non-zero and whose slot differs.
        let mut chosen = None;
        for i in 1_000_000..2_000_000u32 {
            let k = format!("k{i}");
            let o = owner_shard(k.as_bytes(), SHARDS);
            let s = key_slot(k.as_bytes());
            if o != 0 && s != slot {
                chosen = Some((k, s, o));
                break;
            }
        }
        chosen.expect("an absent sibling-owned key on a fresh slot")
    };
    assert_ne!(absent_owner, 0, "the absent key must be sibling-owned too");
    {
        let mut c = connect_retry(src_port).await;
        let r = cmd_raw(
            &mut c,
            &[
                "CLUSTER",
                "SETSLOT",
                &absent_slot.to_string(),
                "MIGRATING",
                DST_ID,
            ],
        )
        .await;
        assert_eq!(
            &r, b"+OK\r\n",
            "SETSLOT MIGRATING (absent-key slot) should succeed"
        );
    }
    let expect_ask = format!("-ASK {absent_slot} 127.0.0.1:{dst_port}\r\n");
    let mut asked = false;
    // Try several connections so an accept shard that happens to differ from the owner still
    // exercises the cross-shard ABSENT path (which must agree with the local-absent answer: -ASK).
    for _ in 0..40 {
        let mut c = connect_retry(src_port).await;
        let r = get_raw(&mut c, &absent_key).await;
        assert!(
            r.starts_with(b"-ASK"),
            "an ABSENT key on a MIGRATING slot must -ASK to the dest; got {:?}",
            String::from_utf8_lossy(&r)
        );
        assert_eq!(
            String::from_utf8_lossy(&r),
            expect_ask,
            "the -ASK must carry the slot and the dest's advertised endpoint"
        );
        asked = true;
    }
    assert!(asked, "the absent key must -ASK");

    src.shutdown_and_join().unwrap();
}
