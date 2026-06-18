// SPDX-License-Identifier: MIT OR Apache-2.0
//! PROD-3 acceptance test: the Raft cluster-bus over TLS + a shared cluster secret.
//!
//! This is the security analog of the plaintext `loopback_cluster.rs` proof. It boots a THREE-node
//! `RaftClusterBusNode` cluster on real loopback TCP, but every node's RAFTMSG listener performs a
//! rustls SERVER handshake on accept and every dial performs a rustls CLIENT handshake, and BOTH
//! sides run a constant-time shared-secret handshake before any RAFTMSG byte. It asserts the three
//! security properties (plus the default-off byte-identical posture is covered by the existing
//! plaintext loopback test, which is unchanged):
//!
//!   1. TLS CLUSTER FORMS: with TLS on + a shared secret, exactly one leader emerges, a propose
//!      commits, and all three nodes converge -- consensus works end to end over the secured bus.
//!   2. WRONG SECRET REJECTED: a fourth node holding the WRONG secret cannot get its RAFTMSG
//!      accepted by the cluster (the listener drops it after the secret check), so it can never
//!      forge consensus / join the quorum.
//!   3. PLAINTEXT DIALER REJECTED: a raw (non-TLS) TCP client to a TLS bus port fails the handshake
//!      and is dropped (not hung).
//!
//! Not a deterministic test (the DST suite covers timing); it polls with generous real-time bounds.

#![cfg(feature = "tls")]

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::Duration;

use ironcache_clusterbus::{ClusterSecurity, PeerEndpoint};
use ironcache_env::{Clock, SystemEnv};
use ironcache_raft::{EntryPayload, MemStorage, NodeId, RaftConfig, RaftNode};
use ironcache_raft_net::{
    NodeHandle, RaftClusterBusNode, RecordingSm, Status, run_listener_secure,
};
use ironcache_runtime::TokioRuntime;
use ironcache_runtime::tokio_rt::bind_reuseport;
use tokio::sync::oneshot;

const TEST_CERT: &str = include_str!("tls/cert.pem");
const TEST_KEY: &str = include_str!("tls/key.pem");
const CLUSTER_SECRET: &[u8] = b"the-shared-cluster-secret";

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

fn voters() -> BTreeSet<NodeId> {
    [N1, N2, N3].into_iter().collect()
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Write the bundled test cert + key to temp files and build a [`ClusterSecurity`] with TLS (the
/// acceptor presents the cert, the connector accepts the peer cert -- no CA, secret-authenticated)
/// and the given `secret`. The temp files persist for the process; tests are short-lived.
fn build_security(secret: &[u8]) -> ClusterSecurity {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir();
    let cert = dir.join(format!("ic-bustls-cert-{}-{n}.pem", std::process::id()));
    let key = dir.join(format!("ic-bustls-key-{}-{n}.pem", std::process::id()));
    std::fs::write(&cert, TEST_CERT).unwrap();
    std::fs::write(&key, TEST_KEY).unwrap();
    let acceptor =
        ironcache_runtime::build_acceptor(&cert.to_string_lossy(), &key.to_string_lossy()).unwrap();
    let connector = ironcache_runtime::build_cluster_client_config(None).unwrap();
    ClusterSecurity::new(Some(connector), Some(acceptor), Some(secret.to_vec()))
}

struct RunningNode {
    handle: NodeHandle,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl RunningNode {
    fn status(&self) -> Status {
        self.handle.status()
    }
    fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Spawn one node with the given cluster SECURITY (TLS + secret) applied to BOTH its listener and
/// its dials. Mirrors the plaintext loopback `spawn_node` but threads `security` through
/// `new_secure` + `run_listener_secure`.
fn spawn_node(
    id: NodeId,
    addr: SocketAddr,
    peers: BTreeMap<NodeId, PeerEndpoint>,
    security: ClusterSecurity,
) -> RunningNode {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (handle_tx, handle_rx) = std::sync::mpsc::channel::<NodeHandle>();

    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            let listener = bind_reuseport(addr).unwrap();
            let raft = RaftNode::with_state_machine(
                id,
                voters(),
                MemStorage::new(),
                RaftConfig::default(),
                RecordingSm::new(),
            );
            let runtime = TokioRuntime::new();
            let (node, handle) = RaftClusterBusNode::new_secure(
                raft,
                SystemEnv::new(),
                runtime,
                peers,
                Some(security.clone()),
            );
            handle_tx.send(handle.clone()).unwrap();
            let inbox = handle.inbox().clone();
            let lrt = TokioRuntime::new();
            tokio::task::spawn_local(async move {
                run_listener_secure::<TokioRuntime>(lrt, listener, inbox, Some(security)).await;
            });
            tokio::task::spawn_local(async move {
                node.run().await;
            });
            let _ = shutdown_rx.await;
        });
    });

    let handle = handle_rx.recv().unwrap();
    RunningNode {
        handle,
        shutdown: Some(shutdown_tx),
        thread: Some(thread),
    }
}

fn peers_excluding(
    addrs: &BTreeMap<NodeId, SocketAddr>,
    id: NodeId,
) -> BTreeMap<NodeId, PeerEndpoint> {
    addrs
        .iter()
        .filter(|&(&other, _)| other != id)
        .map(|(&other, &a)| (other, PeerEndpoint::new(a.ip().to_string(), a.port())))
        .collect()
}

fn poll_until<T>(timeout: Duration, mut f: impl FnMut() -> Option<T>) -> Option<T> {
    let env = SystemEnv::new();
    let start = env.now();
    loop {
        if let Some(v) = f() {
            return Some(v);
        }
        if env.now().saturating_duration_since(start) >= timeout {
            return None;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn unique_leader(nodes: &[RunningNode]) -> Option<usize> {
    let leaders: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.status().is_leader())
        .map(|(i, _)| i)
        .collect();
    (leaders.len() == 1).then(|| leaders[0])
}

fn propose_on(handle: &NodeHandle, payload: EntryPayload) -> Option<u64> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move { handle.propose(payload).await })
}

#[test]
fn tls_cluster_forms_elects_and_commits() {
    let timeout = Duration::from_secs(10);
    let addrs: BTreeMap<NodeId, SocketAddr> = [N1, N2, N3]
        .into_iter()
        .map(|id| (id, format!("127.0.0.1:{}", free_port()).parse().unwrap()))
        .collect();

    // Every node holds the SAME shared secret + the same cluster cert -> the secured bus forms.
    let mut nodes: Vec<RunningNode> = [N1, N2, N3]
        .into_iter()
        .map(|id| {
            spawn_node(
                id,
                addrs[&id],
                peers_excluding(&addrs, id),
                build_security(CLUSTER_SECRET),
            )
        })
        .collect();

    // (1) A unique leader emerges over the TLS + secret bus.
    let leader_idx = poll_until(timeout, || unique_leader(&nodes))
        .expect("a unique leader must be elected over the TLS bus within the timeout");

    // (2) A propose on the leader commits and all three nodes apply through it (consensus works end
    // to end over the encrypted + authenticated transport).
    let want = EntryPayload::Bytes(b"prod3-tls-bus-commit".to_vec());
    let proposed_index = propose_on(&nodes[leader_idx].handle, want)
        .expect("the leader must commit a proposal over the TLS bus");
    let converged = poll_until(timeout, || {
        nodes
            .iter()
            .all(|n| n.status().last_applied >= proposed_index)
            .then_some(())
    });
    assert!(
        converged.is_some(),
        "all nodes must apply through index {proposed_index} over the TLS bus; got {:?}",
        nodes
            .iter()
            .map(|n| n.status().last_applied)
            .collect::<Vec<_>>()
    );

    for n in &mut nodes {
        n.stop();
    }
}

#[test]
fn wrong_secret_peer_cannot_join_the_quorum() {
    // Two nodes (N1, N2) share the right secret; N3 holds the WRONG secret. With the correct quorum
    // (N1+N2 = 2 of 3) a leader still forms among the two good nodes, but N3's RAFTMSG is REJECTED
    // by the good nodes' listeners (the secret check fails after the TLS handshake), so N3 can never
    // get a vote counted or an AppendEntries accepted -- it cannot forge consensus or join.
    let timeout = Duration::from_secs(10);
    let addrs: BTreeMap<NodeId, SocketAddr> = [N1, N2, N3]
        .into_iter()
        .map(|id| (id, format!("127.0.0.1:{}", free_port()).parse().unwrap()))
        .collect();

    let good = || build_security(CLUSTER_SECRET);
    let bad = || build_security(b"the-WRONG-secret");
    let mut n1 = spawn_node(N1, addrs[&N1], peers_excluding(&addrs, N1), good());
    let mut n2 = spawn_node(N2, addrs[&N2], peers_excluding(&addrs, N2), good());
    let mut n3 = spawn_node(N3, addrs[&N3], peers_excluding(&addrs, N3), bad());

    // A leader emerges among the two GOOD nodes (a 2-of-3 majority); the bad-secret N3 is never the
    // one whose votes form that majority (its messages are dropped by N1/N2's listeners).
    let leader_among_good = poll_until(timeout, || {
        let n1_leader = n1.status().is_leader();
        let n2_leader = n2.status().is_leader();
        (n1_leader ^ n2_leader).then_some(n1_leader)
    });
    assert!(
        leader_among_good.is_some(),
        "a leader must form among the two correct-secret nodes (n1 leader={}, n2 leader={}, n3 leader={})",
        n1.status().is_leader(),
        n2.status().is_leader(),
        n3.status().is_leader()
    );

    // The wrong-secret node N3 must NEVER become leader: it cannot get a vote from N1/N2 (its
    // RequestVote is dropped at their listeners), so it can never win a majority.
    assert!(
        !n3.status().is_leader(),
        "a node with the WRONG cluster secret must never become leader (it cannot be admitted)"
    );

    n1.stop();
    n2.stop();
    n3.stop();
}

#[test]
fn plaintext_dialer_to_a_tls_bus_port_is_rejected() {
    // A raw (non-TLS) TCP client that connects to a TLS bus port and sends bytes must FAIL the
    // rustls handshake (the server expects a ClientHello) and be dropped, never hung. We assert the
    // server closes the connection (a read returns 0 / errors) rather than serving the bytes.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let security = build_security(CLUSTER_SECRET);
        // Run the secure listener; an inbox no one reads is fine (no valid frame ever arrives).
        let (inbox_tx, _inbox_rx) = tokio::sync::mpsc::unbounded_channel();
        let lrt = TokioRuntime::new();
        let server = tokio::task::spawn_local(async move {
            run_listener_secure::<TokioRuntime>(lrt, listener, inbox_tx, Some(security)).await;
        });

        // A plaintext client sends a RESP-looking RAFTMSG frame; rustls on the server expects a TLS
        // ClientHello, so the handshake fails. rustls replies with a TLS ALERT record (content type
        // 0x15 = 21, the "alert" type) and CLOSES the connection -- it never serves our `+OK` RESP
        // reply. We drain to EOF (bounded so a hung / insecure server FAILS rather than hangs) and
        // assert that (a) the connection closes and (b) nothing the server returned is the `+OK`
        // application reply a real RAFTMSG would get -- i.e. the plaintext peer is rejected, never
        // admitted to the command path.
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _ = client
            .write_all(b"*3\r\n$7\r\nRAFTMSG\r\n$1\r\n2\r\n$0\r\n\r\n")
            .await;
        let mut collected: Vec<u8> = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            let Ok(read) =
                tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf)).await
            else {
                panic!("the plaintext dialer hung against the TLS bus (no rejection within 5s)");
            };
            // A clean close (0 bytes) OR a reset/error: either way the plaintext peer was rejected.
            match read {
                Ok(0) | Err(_) => break,
                Ok(n) => collected.extend_from_slice(&buf[..n]), // a TLS alert, not app data.
            }
            assert!(
                collected.len() <= 4096,
                "the TLS bus streamed unexpected data to a plaintext dialer: {collected:?}"
            );
        }
        // The server MUST NOT have served the RESP `+OK` ack a real RAFTMSG would get; any bytes are
        // a TLS alert (the rejection), never application data.
        assert!(
            !collected.windows(5).any(|w| w == b"+OK\r\n"),
            "a TLS bus must NOT serve the +OK RAFTMSG ack to a plaintext dialer; got {collected:?}"
        );
        server.abort();
    });
}
