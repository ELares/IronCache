// SPDX-License-Identifier: MIT OR Apache-2.0
//! Cluster slice-2 acceptance tests (CLUSTER_CONTRACT.md #70): a static two-node topology
//! split across the slot space, booted as TWO real `run_server` nodes on the loopback at
//! different ports sharing ONE topology. They drive the nodes over real sockets so the whole
//! cluster path is exercised end to end: classify -> `cluster_redirect` -> MOVED / CROSSSLOT
//! encode, plus the multi-node `CLUSTER SLOTS` / `CLUSTER INFO` projection.
//!
//! How MOVED is exercised locally without two real hosts: both nodes declare `host =
//! "127.0.0.1"` with their own port (7001 / 7002) and own half of the 16384-slot space
//! ([0, 8191] / [8192, 16383]). Each node boots with its OWN matching announce id, so the
//! same shared map names a different `self` per node. Against node 7001:
//! - an owned-slot key is served (`+OK` / value);
//! - a foreign-slot key returns the EXACT `-MOVED <slot> 127.0.0.1:7002`;
//! - a cross-slot `MGET` returns the EXACT `-CROSSSLOT ...` wire string;
//! - `CLUSTER SLOTS` reflects both ranges and `CLUSTER INFO` the two-node counts.
//!
//! A short key whose `key_slot` lands in a target slot range is found by brute force (the
//! `key_in_range` helper), so the test never hard-codes a key whose CRC16 might drift.

use ironcache::test_support::run_cluster_node_for_test;
use ironcache_config::{ClusterNode, ClusterTopology};
use ironcache_protocol::key_slot;
use ironcache_runtime::bootstrap::ShardSet;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary (so the
// process-memory path used by INFO is live; harmless for these tests otherwise).
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const ID1: &str = "1111111111111111111111111111111111111111";
const ID2: &str = "2222222222222222222222222222222222222222";

/// The shared two-node topology: ID1 (port 7001) owns [0, 8191], ID2 (port 7002) owns
/// [8192, 16383], both advertised on 127.0.0.1.
fn two_node_topology(port1: u16, port2: u16) -> ClusterTopology {
    ClusterTopology {
        nodes: vec![
            ClusterNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: port1,
                slots: vec![[0, 8191]],
            },
            ClusterNode {
                id: ID2.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: port2,
                slots: vec![[8192, 16383]],
            },
        ],
    }
}

/// Grab a free TCP port by binding an ephemeral listener and dropping it (small TOCTOU
/// window before `run_server` re-binds; acceptable for a localhost test).
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Brute-force a short key whose `key_slot` falls in `[lo, hi]` (deterministic, fast: the
/// slot space is dense, so a handful of integer keys covers any half). Panics if none is
/// found within a generous bound (would only happen on a logic error).
fn key_in_range(lo: u16, hi: u16) -> String {
    for i in 0..100_000u32 {
        let k = format!("k{i}");
        let s = key_slot(k.as_bytes());
        if s >= lo && s <= hi {
            return k;
        }
    }
    panic!("no key found whose slot is in [{lo}, {hi}]");
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

/// `MGET k1 k2` (RESP2 array); return the raw reply.
async fn mget_raw(client: &mut TcpStream, k1: &str, k2: &str) -> Vec<u8> {
    let frame = format!(
        "*3\r\n$4\r\nMGET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        k1.len(),
        k1,
        k2.len(),
        k2
    );
    client.write_all(frame.as_bytes()).await.unwrap();
    read_raw(client).await
}

/// `CLUSTER <sub>` (RESP2 array); read enough to capture the whole reply.
async fn cluster_sub(client: &mut TcpStream, sub: &str) -> Vec<u8> {
    let frame = format!("*2\r\n$7\r\nCLUSTER\r\n${}\r\n{}\r\n", sub.len(), sub);
    client.write_all(frame.as_bytes()).await.unwrap();
    // CLUSTER SLOTS / INFO can be larger than one TCP segment; read until quiet.
    let mut acc = Vec::new();
    loop {
        let mut buf = [0u8; 4096];
        let n = tokio::time::timeout(Duration::from_millis(200), client.read(&mut buf))
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(0);
        if n == 0 {
            break;
        }
        acc.extend_from_slice(&buf[..n]);
        if n < buf.len() {
            break;
        }
    }
    acc
}

/// Boot both nodes (single shard each), returning their handles + the chosen ports. The two
/// nodes run on their OWN OS threads (the SO_REUSEPORT thread-per-core topology inside
/// `run_server`), so the test driver only needs a current-thread client runtime.
fn boot_two_nodes() -> (ShardSet, ShardSet, u16, u16) {
    let port1 = free_port();
    let port2 = free_port();
    let topo = two_node_topology(port1, port2);
    let n1 = run_cluster_node_for_test(port1, topo.clone(), ID1);
    let n2 = run_cluster_node_for_test(port2, topo, ID2);
    (n1, n2, port1, port2)
}

/// Run `body` on a fresh current-thread tokio runtime + `LocalSet` (the same harness the
/// coordinator integration tests use): the server lives on its own threads, the client driver
/// here is single-threaded.
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

#[test]
fn owned_slot_key_is_served_locally() {
    with_runtime(async {
        let (n1, n2, port1, _port2) = boot_two_nodes();
        let mut c = connect_retry(port1).await;

        // A key whose slot is in node 1's half [0, 8191] is served locally.
        let key = key_in_range(0, 8191);
        assert!(key_slot(key.as_bytes()) <= 8191);
        let set = set_raw(&mut c, &key, "v").await;
        assert_eq!(set, b"+OK\r\n", "owned-slot SET should be served: {set:?}");
        let got = get_raw(&mut c, &key).await;
        assert_eq!(
            got, b"$1\r\nv\r\n",
            "owned-slot GET should return the value: {got:?}"
        );

        drop(c);
        n1.shutdown_and_join().unwrap();
        n2.shutdown_and_join().unwrap();
    });
}

#[test]
fn foreign_slot_key_is_moved_to_the_owner() {
    with_runtime(async {
        let (n1, n2, port1, port2) = boot_two_nodes();
        let mut c = connect_retry(port1).await;

        // A key whose slot is in node 2's half [8192, 16383] must be MOVED to 127.0.0.1:port2.
        let key = key_in_range(8192, 16383);
        let slot = key_slot(key.as_bytes());
        assert!(slot >= 8192);
        let reply = set_raw(&mut c, &key, "v").await;
        let expect = format!("-MOVED {slot} 127.0.0.1:{port2}\r\n");
        assert_eq!(
            reply,
            expect.as_bytes(),
            "foreign-slot SET must be MOVED to the owner, got {:?}",
            String::from_utf8_lossy(&reply)
        );

        drop(c);
        n1.shutdown_and_join().unwrap();
        n2.shutdown_and_join().unwrap();
    });
}

#[test]
fn cross_slot_mget_is_rejected() {
    with_runtime(async {
        let (n1, n2, port1, _port2) = boot_two_nodes();
        let mut c = connect_retry(port1).await;

        // Two keys in DIFFERENT slots (one per half) -> CROSSSLOT, regardless of ownership.
        let k_lo = key_in_range(0, 8191);
        let k_hi = key_in_range(8192, 16383);
        assert_ne!(key_slot(k_lo.as_bytes()), key_slot(k_hi.as_bytes()));
        let reply = mget_raw(&mut c, &k_lo, &k_hi).await;
        assert_eq!(
            reply,
            b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
            "cross-slot MGET must be CROSSSLOT, got {:?}",
            String::from_utf8_lossy(&reply)
        );

        drop(c);
        n1.shutdown_and_join().unwrap();
        n2.shutdown_and_join().unwrap();
    });
}

#[test]
fn cluster_slots_and_info_reflect_the_two_node_map() {
    with_runtime(async {
        let (n1, n2, port1, port2) = boot_two_nodes();
        let mut c = connect_retry(port1).await;

        // CLUSTER SLOTS: two ranges [0, 8191, 127.0.0.1:port1] and [8192, 16383, 127.0.0.1:port2].
        let slots = cluster_sub(&mut c, "SLOTS").await;
        let slots_txt = String::from_utf8_lossy(&slots);
        // The reply is a RESP array of [start, end, [host, port, id]] entries. Assert the slot
        // boundaries, the advertised ports, and both ids appear.
        assert!(
            slots_txt.contains(":0\r\n"),
            "range starts at 0: {slots_txt:?}"
        );
        assert!(
            slots_txt.contains(":8191\r\n"),
            "first range ends at 8191: {slots_txt:?}"
        );
        assert!(
            slots_txt.contains(":8192\r\n"),
            "second range starts at 8192: {slots_txt:?}"
        );
        assert!(
            slots_txt.contains(":16383\r\n"),
            "second range ends at 16383: {slots_txt:?}"
        );
        assert!(
            slots_txt.contains(&format!(":{port1}\r\n")),
            "advertises port1: {slots_txt:?}"
        );
        assert!(
            slots_txt.contains(&format!(":{port2}\r\n")),
            "advertises port2: {slots_txt:?}"
        );
        assert!(slots_txt.contains(ID1), "names ID1: {slots_txt:?}");
        assert!(slots_txt.contains(ID2), "names ID2: {slots_txt:?}");

        // CLUSTER INFO: two known nodes, size two, all 16384 slots assigned.
        let info = cluster_sub(&mut c, "INFO").await;
        let info_txt = String::from_utf8_lossy(&info);
        assert!(
            info_txt.contains("cluster_known_nodes:2"),
            "two known nodes: {info_txt:?}"
        );
        assert!(
            info_txt.contains("cluster_size:2"),
            "cluster size two: {info_txt:?}"
        );
        assert!(
            info_txt.contains("cluster_slots_assigned:16384"),
            "all slots assigned: {info_txt:?}"
        );
        assert!(
            info_txt.contains("cluster_state:ok"),
            "state ok: {info_txt:?}"
        );

        drop(c);
        n1.shutdown_and_join().unwrap();
        n2.shutdown_and_join().unwrap();
    });
}
