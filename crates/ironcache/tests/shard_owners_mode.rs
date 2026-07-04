// SPDX-License-Identifier: MIT OR Apache-2.0
//! #517 shard-owners acceptance.
//!
//! PR2: a node booted with `cluster_mode = shard-owners` must boot into a SERVING cluster (all 16384
//! slots assigned, `cluster_state:ok`, keys served immediately -- no `CLUSTER ADDSLOTS`). Guards the
//! "cluster-enabled but zero slots -> CLUSTERDOWN" trap: enabling the perf mode must NOT turn a
//! working cache into a non-serving node.
//!
//! PR3: the mode binds ONE listener PER shard at `base + i`, each homing its connections on shard
//! `i` (asserted via per-port liveness).
//!
//! PR4 (the hop-elimination milestone): the N shards are projected as N cluster nodes, node `i` at
//! `base + i` owning the contiguous slot range `slot_to_shard` assigns it. A cluster-aware client
//! that dials a key's owner port is served LOCALLY (no MOVED; internally owner == home so no hop); a
//! mis-routed key gets `-MOVED <slot> host:owner_port`, so clients converge to zero hops.

use ironcache::test_support::run_shard_owners_node_for_test;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            s.set_nodelay(true).unwrap();
            return s;
        }
        tokio::task::yield_now().await;
    }
    panic!("could not connect to the shard-owners test node on {port}");
}

fn cmd(args: &[&str]) -> String {
    use std::fmt::Write as _;
    let mut f = format!("*{}\r\n", args.len());
    for a in args {
        write!(f, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    f
}

/// Read one reply's bytes (a single read; the small replies here fit one packet).
async fn send_read(c: &mut TcpStream, frame: &str) -> Vec<u8> {
    c.write_all(frame.as_bytes()).await.unwrap();
    let mut buf = [0u8; 4096];
    let n = c.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

/// The contiguous partition the router + projection share: slot -> owning shard.
fn owner_shard(key: &str, n: usize) -> usize {
    let slot = ironcache_protocol::key_slot(key.as_bytes()) as usize;
    (slot * n) / 16384
}

#[tokio::test(flavor = "current_thread")]
async fn shard_owners_mode_owns_all_slots_and_serves_keys() {
    const N: usize = 4;
    let (_node, base) = run_shard_owners_node_for_test(N);
    let mut c = connect_retry(base).await;

    // CLUSTER INFO must report a HEALTHY cluster with ALL 16384 slots assigned (across the N shard
    // owners), NOT the zero-slot `cluster_state:fail` a bare cluster-enabled node shows before
    // ADDSLOTS. This is the boot-serving guard against the CLUSTERDOWN trap.
    let info = send_read(&mut c, &cmd(&["CLUSTER", "INFO"])).await;
    let info_s = String::from_utf8_lossy(&info);
    assert!(
        info_s.contains("cluster_state:ok"),
        "shard-owners node must be cluster_state:ok, got:\n{info_s}"
    );
    assert!(
        info_s.contains("cluster_slots_assigned:16384"),
        "shard-owners node must have all 16384 slots assigned, got:\n{info_s}"
    );

    // And it SERVES keys immediately (no ADDSLOTS): a key OWNED by shard 0 (the `base` port) round
    // trips locally. (Shard 0 only owns its slot range; a foreign key would MOVED -- covered by the
    // dedicated MOVED test -- so pick a key `base` actually owns.)
    let key = (0..100_000)
        .map(|i| format!("k{i}"))
        .find(|k| owner_shard(k, N) == 0)
        .expect("some key in the first 100k hashes to shard 0");
    let set = send_read(&mut c, &cmd(&["SET", &key, "hello"])).await;
    assert_eq!(
        &set, b"+OK\r\n",
        "SET of a shard-0 key must serve locally on base"
    );
    let get = send_read(&mut c, &cmd(&["GET", &key])).await;
    assert_eq!(&get, b"$5\r\nhello\r\n", "GET must return the value");
}

#[tokio::test(flavor = "current_thread")]
async fn shard_owners_binds_one_serving_listener_per_shard() {
    // PR3: 4 shards -> the node binds 4 listeners (base .. base + 3). The helper reserves a
    // CONTIGUOUS free block and retries on a bind race, so this is robust under parallel test load.
    const SHARDS: u16 = 4;
    let (_node, base) = run_shard_owners_node_for_test(SHARDS as usize);

    // EVERY per-shard port must be independently BOUND and responsive (a connection to `base + i`
    // homes on shard `i`). Liveness via PING per port -- a missing listener fails to connect, a
    // broken one fails PING. (Each port only SERVES the keys it OWNS and MOVEDs the rest, so a blind
    // SET is not a valid liveness probe; the owns/MOVED semantics are covered below.)
    for i in 0..SHARDS {
        let port = base + i;
        let mut c = connect_retry(port).await;
        let pong = send_read(&mut c, &cmd(&["PING"])).await;
        assert_eq!(
            &pong, b"+PONG\r\n",
            "per-shard port {port} (shard {i}) must be bound and responsive"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn owner_port_serves_locally_and_a_misrouted_key_gets_moved() {
    // PR4 (the milestone): a cluster-aware client that dials a key's OWNER port is served LOCALLY
    // (no MOVED, and internally no cross-shard hop -- owner_shard == home shard). A client that dials
    // the WRONG port gets a -MOVED to the owner's port, so it converges to zero hops.
    const N: usize = 4;
    let (_node, base) = run_shard_owners_node_for_test(N);

    for key in [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
    ] {
        let owner = owner_shard(key, N);
        let owner_port = base + owner as u16;

        // OWNER port -> served locally, no MOVED.
        let mut c = connect_retry(owner_port).await;
        let set = send_read(&mut c, &cmd(&["SET", key, "v"])).await;
        assert_eq!(
            &set, b"+OK\r\n",
            "the owner port {owner_port} must serve {key} locally (no MOVED)"
        );
        let get = send_read(&mut c, &cmd(&["GET", key])).await;
        assert_eq!(&get, b"$1\r\nv\r\n", "owner port must return {key}'s value");

        // A NON-owner port -> MOVED to the owner's port.
        let wrong_port = base + ((owner as u16 + 1) % N as u16);
        assert_ne!(wrong_port, owner_port, "test bug: wrong port equals owner");
        let mut c2 = connect_retry(wrong_port).await;
        let r = send_read(&mut c2, &cmd(&["GET", key])).await;
        let rs = String::from_utf8_lossy(&r);
        assert!(
            rs.starts_with("-MOVED"),
            "non-owner port {wrong_port} must MOVED {key}, got: {rs}"
        );
        assert!(
            rs.trim_end().ends_with(&format!(":{owner_port}")),
            "the MOVED for {key} must point to the owner port {owner_port}, got: {rs}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cluster_info_reflects_the_n_shard_projection() {
    // The projection makes the node a healthy N-node cluster (was 1 node owning all slots pre-PR4).
    let (_node, base) = run_shard_owners_node_for_test(4);
    let mut c = connect_retry(base).await;
    let info =
        String::from_utf8_lossy(&send_read(&mut c, &cmd(&["CLUSTER", "INFO"])).await).into_owned();
    for want in [
        "cluster_state:ok",
        "cluster_slots_assigned:16384",
        "cluster_known_nodes:4",
        "cluster_size:4",
    ] {
        assert!(
            info.contains(want),
            "CLUSTER INFO must contain `{want}`, got:\n{info}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn n_equals_one_serves_every_key_locally_without_moved() {
    // N=1: a single owner of all 16384 slots -> every key is local, never MOVED (byte-identical to
    // the pre-projection single-node build).
    let (_node, base) = run_shard_owners_node_for_test(1);
    let mut c = connect_retry(base).await;
    for key in ["a", "b", "c", "zzz", "{tag}.x"] {
        let r = send_read(&mut c, &cmd(&["SET", key, "1"])).await;
        assert_eq!(
            &r,
            b"+OK\r\n",
            "N=1 must serve {key} locally with no MOVED, got: {}",
            String::from_utf8_lossy(&r)
        );
    }
}
