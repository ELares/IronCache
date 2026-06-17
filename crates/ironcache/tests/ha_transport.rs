// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-1 acceptance test: inter-node transport over the Runtime seam.
//!
//! Boots TWO real cluster nodes on the loopback (the same two-node static topology
//! shape as the slice-2 test) and drives a NODE-TO-NODE connection from the test
//! using `ironcache_clusterbus` over a real `TokioRuntime`: it reaches each peer's
//! RESP port through the new `Runtime::connect` outbound seam and reads back the
//! peer's `CLUSTER MYID`, asserting it equals that node's announce id, plus a
//! `PING`/`+PONG` round-trip. This exercises the whole outbound path end to end
//! (connect -> send -> recv -> RESP decode) the cluster control plane will build on.

use ironcache::test_support::run_cluster_node_for_test;
use ironcache_clusterbus::{peer_node_id, peer_ping};
use ironcache_config::{ClusterNode, ClusterTopology};
use ironcache_runtime::TokioRuntime;
use ironcache_runtime::bootstrap::ShardSet;
use std::net::SocketAddr;
use std::time::Duration;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const ID1: &str = "1111111111111111111111111111111111111111";
const ID2: &str = "2222222222222222222222222222222222222222";

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

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn boot_two_nodes() -> (ShardSet, ShardSet, u16, u16) {
    let port1 = free_port();
    let port2 = free_port();
    let topo = two_node_topology(port1, port2);
    let n1 = run_cluster_node_for_test(port1, topo.clone(), ID1);
    let n2 = run_cluster_node_for_test(port2, topo, ID2);
    (n1, n2, port1, port2)
}

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

/// Fetch a peer's node id with a few short retries: the shards bind asynchronously
/// on their own threads after `run_cluster_node_for_test` returns, so the first
/// connect can race the bind.
async fn node_id_retry(rt: &TokioRuntime, addr: SocketAddr) -> String {
    for _ in 0..50 {
        match peer_node_id(rt, addr).await {
            Ok(id) => return id,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("peer never answered CLUSTER MYID at {addr}");
}

#[test]
fn node_reaches_peer_over_the_runtime_connect_seam() {
    with_runtime(async {
        let (n1, n2, port1, port2) = boot_two_nodes();
        let rt = TokioRuntime::new();
        let addr1: SocketAddr = format!("127.0.0.1:{port1}").parse().unwrap();
        let addr2: SocketAddr = format!("127.0.0.1:{port2}").parse().unwrap();

        // Each peer reports its own announce id over the outbound bus connection.
        let id1 = node_id_retry(&rt, addr1).await;
        assert_eq!(id1, ID1, "node 1 CLUSTER MYID over the bus");
        let id2 = node_id_retry(&rt, addr2).await;
        assert_eq!(id2, ID2, "node 2 CLUSTER MYID over the bus");

        // And a PING round-trips to +PONG over the same path.
        assert!(peer_ping(&rt, addr1).await.unwrap(), "node 1 PING -> PONG");
        assert!(peer_ping(&rt, addr2).await.unwrap(), "node 2 PING -> PONG");

        n1.shutdown_and_join().unwrap();
        n2.shutdown_and_join().unwrap();
    });
}
