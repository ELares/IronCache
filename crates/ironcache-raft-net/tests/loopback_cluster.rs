// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-4a acceptance test: the pure Raft engine forms a cluster and commits over
//! REAL TCP, driven by the production [`RaftClusterBusNode`] adapter.
//!
//! This is the loopback proof that the DST-verified engine works on the real
//! network path. It boots THREE `RaftClusterBusNode`s, each on its OWN OS thread
//! with its own current-thread tokio runtime + `LocalSet` (the shared-nothing,
//! `!Send`, thread-per-core shape the production runtime uses, ADR-0002; the same
//! multi-node-on-loopback pattern as `ironcache/tests/cluster_slice2.rs`). Each node
//! binds a real `RAFTMSG` listener on a free loopback port and is wired to the other
//! two as peers. The test thread holds only the `Send` handles (an mpsc inbox sender
//! and a watch status receiver per node), so it observes and proposes without ever
//! touching the `!Send` engine.
//!
//! It asserts the three things 4a must prove:
//!   1. ELECTION: exactly one leader emerges within a generous real-time bound.
//!   2. COMMIT + CONVERGENCE: a `propose` on the leader commits and ALL three nodes'
//!      `RecordingSm`s apply the SAME committed sequence including that entry.
//!   3. RESTART / CATCH-UP: a follower's task is torn down and restarted with FRESH
//!      `MemStorage`; it re-joins and catches up the committed log via the leader's
//!      AppendEntries.
//!
//! This is NOT a deterministic test (that is the DST suite's job): it runs on the
//! real clock, so it polls with generous timeouts and retries rather than asserting
//! exact timing.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::Duration;

use ironcache_env::{Clock, SystemEnv};
use ironcache_raft::{EntryPayload, LogEntry, MemStorage, NodeId, RaftConfig, RaftNode};
use ironcache_raft_net::{NodeHandle, RaftClusterBusNode, RecordingSm, Status, run_listener};
use ironcache_runtime::TokioRuntime;
use ironcache_runtime::tokio_rt::bind_reuseport;
use tokio::sync::{mpsc, oneshot};

// The three node ids and the voter set they share.
const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

fn voters() -> BTreeSet<NodeId> {
    [N1, N2, N3].into_iter().collect()
}

/// A free loopback port (bind to :0, read the assigned port, drop the listener). The
/// node re-binds it under `SO_REUSEPORT` immediately after; the brief gap is fine on
/// loopback for a test.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Everything the test thread keeps for one running node.
struct RunningNode {
    handle: NodeHandle,
    /// Drains the node's applied-entry stream (mirrored from its `RecordingSm`).
    applied_rx: mpsc::UnboundedReceiver<LogEntry>,
    /// Fire to tear the node's thread down (ends its run loop + listener).
    shutdown: Option<oneshot::Sender<()>>,
    /// The node's OS thread (joined on teardown).
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

/// Spawn one node on its own thread: bind its listener, build the engine with FRESH
/// `MemStorage` and a sink-backed `RecordingSm`, and run the listener + control loop
/// on a `LocalSet` until the shutdown signal fires. Returns the `Send` handle, the
/// applied-entry receiver, and the shutdown/thread handles.
fn spawn_node(id: NodeId, addr: SocketAddr, peers: BTreeMap<NodeId, SocketAddr>) -> RunningNode {
    let (applied_tx, applied_rx) = mpsc::unbounded_channel::<LogEntry>();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (handle_tx, handle_rx) = std::sync::mpsc::channel::<NodeHandle>();

    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            // Bind the listener inside the runtime (registers with the reactor).
            let listener = bind_reuseport(addr).unwrap();

            // Build the pure engine: fresh MemStorage, default timing, a RecordingSm
            // that mirrors applied entries to the test's collector.
            let raft = RaftNode::with_state_machine(
                id,
                voters(),
                MemStorage::new(),
                RaftConfig::default(),
                RecordingSm::with_sink(applied_tx),
            );
            let runtime = TokioRuntime::new();
            let (node, handle) = RaftClusterBusNode::new(raft, SystemEnv::new(), runtime, peers);

            // Hand the Send handle back to the test thread.
            handle_tx.send(handle.clone()).unwrap();

            // The listener feeds Event::Inbound into the run loop's inbox.
            let inbox = handle.inbox().clone();
            let lrt = TokioRuntime::new();
            tokio::task::spawn_local(async move {
                run_listener::<TokioRuntime>(lrt, listener, inbox).await;
            });
            // The control-plane run loop owns the engine.
            tokio::task::spawn_local(async move {
                node.run().await;
            });

            // Run until the test signals shutdown; dropping the LocalSet then aborts
            // the spawned listener + run-loop tasks and the thread returns.
            let _ = shutdown_rx.await;
        });
    });

    let handle = handle_rx.recv().unwrap();
    RunningNode {
        handle,
        applied_rx,
        shutdown: Some(shutdown_tx),
        thread: Some(thread),
    }
}

/// The full three-node peer wiring: every node maps the OTHER two ids to addresses.
fn peer_maps(
    addrs: &BTreeMap<NodeId, SocketAddr>,
) -> BTreeMap<NodeId, BTreeMap<NodeId, SocketAddr>> {
    let mut out = BTreeMap::new();
    for &id in addrs.keys() {
        out.insert(id, peers_excluding(addrs, id));
    }
    out
}

/// The peer map for `id`: every OTHER node's id mapped to its address.
fn peers_excluding(
    addrs: &BTreeMap<NodeId, SocketAddr>,
    id: NodeId,
) -> BTreeMap<NodeId, SocketAddr> {
    addrs
        .iter()
        .filter(|&(&other, _)| other != id)
        .map(|(&other, &a)| (other, a))
        .collect()
}

/// Poll `f` until it returns `Some`, up to `timeout`, sleeping briefly between tries.
/// Returns `None` on timeout. The test thread runs no async runtime, so this is a
/// plain blocking poll on real time (the production path is what is under test, not
/// deterministic timing). The deadline is measured through the `ironcache-env`
/// monotonic clock (ADR-0003 / invariant 2: even tests read real time only through
/// the env seam, never `std::time::Instant`).
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

/// Index of the single current leader among `nodes`, or `None` if there is not
/// exactly one node reporting `Leader`.
fn unique_leader(nodes: &[RunningNode]) -> Option<usize> {
    let leaders: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.status().is_leader())
        .map(|(i, _)| i)
        .collect();
    if leaders.len() == 1 {
        Some(leaders[0])
    } else {
        None
    }
}

/// Drain whatever applied entries are currently buffered in a node's stream
/// (non-blocking).
fn drain_applied(node: &mut RunningNode) -> Vec<LogEntry> {
    let mut out = Vec::new();
    while let Ok(entry) = node.applied_rx.try_recv() {
        out.push(entry);
    }
    out
}

/// Assert all three nodes converged to the SAME committed prefix and that the entry
/// at `proposed_index` is the expected opaque payload.
fn assert_converged(
    applied: &[Vec<LogEntry>; 3],
    proposed_index: u64,
    expect_payload: &EntryPayload,
) {
    let common = applied.iter().map(Vec::len).min().unwrap();
    assert!(
        common >= proposed_index as usize,
        "every node should have applied at least {proposed_index} entries; got {:?}",
        applied.iter().map(Vec::len).collect::<Vec<_>>()
    );
    assert_eq!(
        &applied[0][..common],
        &applied[1][..common],
        "nodes 1 and 2 applied divergent committed sequences"
    );
    assert_eq!(
        &applied[1][..common],
        &applied[2][..common],
        "nodes 2 and 3 applied divergent committed sequences"
    );
    let at = &applied[0][proposed_index as usize - 1];
    assert_eq!(at.index, proposed_index);
    assert_eq!(
        &at.payload, expect_payload,
        "the committed entry at the proposed index must be our payload"
    );
}

/// Block on a single async `propose` on the leader's handle, returning the assigned
/// index. The test thread has no ambient runtime, so it spins a throwaway one.
fn propose_on(handle: &NodeHandle, payload: EntryPayload) -> Option<u64> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move { handle.propose(payload).await })
}

#[test]
fn cluster_elects_commits_and_a_restarted_follower_catches_up() {
    // Generous bounds: election base+jitter is 150-300ms, so a few seconds is ample
    // for a leader to emerge and commits to propagate even on a loaded CI machine.
    let timeout = Duration::from_secs(10);

    // Pick three free ports and build the address + peer maps up front so every node
    // knows where its peers listen before any of them start.
    let addrs: BTreeMap<NodeId, SocketAddr> = [N1, N2, N3]
        .into_iter()
        .map(|id| (id, format!("127.0.0.1:{}", free_port()).parse().unwrap()))
        .collect();
    let peers = peer_maps(&addrs);

    // Boot all three nodes (held in a Vec so indices line up with leader/follower).
    let ids = [N1, N2, N3];
    let mut nodes: Vec<RunningNode> = ids
        .iter()
        .map(|&id| spawn_node(id, addrs[&id], peers[&id].clone()))
        .collect();

    // ---- (1) ELECTION: exactly one leader emerges. ----
    let leader_idx = poll_until(timeout, || unique_leader(&nodes))
        .expect("a unique leader must be elected within the timeout");

    // ---- (2) COMMIT + CONVERGENCE: propose on the leader; all nodes converge. ----
    let want = EntryPayload::Bytes(b"ha-4a-loopback-commit".to_vec());
    let proposed_index = propose_on(&nodes[leader_idx].handle, want.clone())
        .expect("the leader must accept the proposal and return its index");
    assert!(proposed_index >= 1, "a proposed index is 1-based");

    // Wait until every node has applied through the proposed index.
    let converged = poll_until(timeout, || {
        nodes
            .iter()
            .all(|n| n.status().last_applied >= proposed_index)
            .then_some(())
    });
    assert!(
        converged.is_some(),
        "all nodes must apply through index {proposed_index}; got {:?}",
        nodes
            .iter()
            .map(|n| n.status().last_applied)
            .collect::<Vec<_>>()
    );

    // Drain every node's applied stream; assert identical committed prefix + payload.
    let applied: [Vec<LogEntry>; 3] = [
        drain_applied(&mut nodes[0]),
        drain_applied(&mut nodes[1]),
        drain_applied(&mut nodes[2]),
    ];
    let survivor_log = applied[0].clone();
    assert_converged(&applied, proposed_index, &want);

    // ---- (3) RESTART / CATCH-UP: tear a follower down and restart it FRESH. ----
    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();
    let restart_id = ids[follower_idx];
    nodes[follower_idx].stop();
    nodes[follower_idx] = spawn_node(
        restart_id,
        addrs[&restart_id],
        peers_excluding(&addrs, restart_id),
    );

    // The fresh follower (empty MemStorage) catches up via the leader's AppendEntries.
    let caught_up = poll_until(timeout, || {
        (nodes[follower_idx].status().last_applied >= proposed_index).then_some(())
    });
    assert!(
        caught_up.is_some(),
        "the restarted follower must catch up to index {proposed_index}; got {}",
        nodes[follower_idx].status().last_applied
    );

    // What it applied on catch-up matches the survivors' committed prefix.
    let recovered = drain_applied(&mut nodes[follower_idx]);
    let n = (proposed_index as usize).min(recovered.len());
    assert_eq!(
        &recovered[..n],
        &survivor_log[..n],
        "the restarted follower applied a sequence divergent from the survivors"
    );

    // Clean shutdown (Drop also handles this, but be explicit).
    for mut node in nodes {
        node.stop();
    }
}
