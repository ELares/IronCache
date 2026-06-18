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

use ironcache_clusterbus::PeerEndpoint;
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
fn spawn_node(id: NodeId, addr: SocketAddr, peers: BTreeMap<NodeId, PeerEndpoint>) -> RunningNode {
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

/// The full three-node peer wiring: every node maps the OTHER two ids to [`PeerEndpoint`]s
/// (host + port). The loopback `SocketAddr`s carry an IP-literal host, so an endpoint built from
/// one resolves byte-identically to the address it came from (the re-resolve path is exercised by
/// the dedicated resolver unit tests; here it dials the same loopback ip every time).
fn peer_maps(
    addrs: &BTreeMap<NodeId, SocketAddr>,
) -> BTreeMap<NodeId, BTreeMap<NodeId, PeerEndpoint>> {
    let mut out = BTreeMap::new();
    for &id in addrs.keys() {
        out.insert(id, peers_excluding(addrs, id));
    }
    out
}

/// The peer map for `id`: every OTHER node's id mapped to its [`PeerEndpoint`] (the loopback
/// `SocketAddr`'s ip + port, held as host + port so the dial path re-resolves per connect).
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

/// HA-prod-commit-ack over real TCP: a `propose()` resolves `Committed` only AFTER a MAJORITY
/// commit, and a majority does NOT require ALL voters. We elect a 3-voter leader, STOP one
/// follower (so only leader + 1 follower remain = 2 of 3 = a majority), then propose on the
/// leader. With the ack now resolving on the COMMIT-ADVANCE (not at append), the propose must
/// still return its index -- the surviving follower's ack carries the entry onto a majority and
/// commits it -- and the two live nodes converge. (Before this change the ack resolved at append
/// regardless of replication; this proves it now waits for a real majority, yet does not need the
/// dead third node.)
#[test]
fn propose_commits_on_majority_with_one_follower_down() {
    let timeout = Duration::from_secs(10);

    let addrs: BTreeMap<NodeId, SocketAddr> = [N1, N2, N3]
        .into_iter()
        .map(|id| (id, format!("127.0.0.1:{}", free_port()).parse().unwrap()))
        .collect();
    let peers = peer_maps(&addrs);

    let ids = [N1, N2, N3];
    let mut nodes: Vec<RunningNode> = ids
        .iter()
        .map(|&id| spawn_node(id, addrs[&id], peers[&id].clone()))
        .collect();

    let leader_idx = poll_until(timeout, || unique_leader(&nodes))
        .expect("a unique leader must be elected within the timeout");
    let follower_a = (0..3).find(|&i| i != leader_idx).unwrap();
    let follower_b = (0..3)
        .find(|&i| i != leader_idx && i != follower_a)
        .unwrap();

    // STOP one follower: leader + the OTHER follower is 2 of 3 = still a majority.
    nodes[follower_a].stop();

    // Propose on the leader. The ack resolves on the COMMIT-ADVANCE: the surviving follower acks,
    // the entry reaches a 2/3 majority, commit advances, and the parked ack fires Committed(index).
    // This MUST return (a quorum is live) with a 1-based index.
    let want = EntryPayload::Bytes(b"commit-on-majority".to_vec());
    let proposed_index = propose_on(&nodes[leader_idx].handle, want.clone()).expect(
        "a quorum is live, so the propose must COMMIT on the majority and return its index",
    );
    assert!(proposed_index >= 1, "a proposed index is 1-based");

    // The two LIVE nodes apply through the committed index (the dead one cannot, and is not needed).
    let converged = poll_until(timeout, || {
        (nodes[leader_idx].status().last_applied >= proposed_index
            && nodes[follower_b].status().last_applied >= proposed_index)
            .then_some(())
    });
    assert!(
        converged.is_some(),
        "the leader and the surviving follower must apply through index {proposed_index}; got \
         leader={}, follower={}",
        nodes[leader_idx].status().last_applied,
        nodes[follower_b].status().last_applied
    );

    // The committed entry at the proposed index is OUR payload on both live nodes.
    let leader_applied = drain_applied(&mut nodes[leader_idx]);
    let follower_applied = drain_applied(&mut nodes[follower_b]);
    let li = proposed_index as usize - 1;
    assert_eq!(leader_applied[li].index, proposed_index);
    assert_eq!(
        &leader_applied[li].payload, &want,
        "the committed entry must be our payload on the leader"
    );
    assert_eq!(
        &follower_applied[li].payload, &want,
        "the committed entry must be our payload on the surviving follower"
    );

    for mut node in nodes {
        node.stop();
    }
}

/// HA-9 LEADER-FORWARDING over real TCP: a `propose()` issued to a FOLLOWER is transparently
/// forwarded to the leader, commits, and applies on EVERY node (so a follower no longer has to
/// be the leader for a proposal to land). This is the headline property forwarding unlocks.
#[test]
fn follower_propose_forwards_to_leader_commits_and_converges() {
    let timeout = Duration::from_secs(10);

    let addrs: BTreeMap<NodeId, SocketAddr> = [N1, N2, N3]
        .into_iter()
        .map(|id| (id, format!("127.0.0.1:{}", free_port()).parse().unwrap()))
        .collect();
    let peers = peer_maps(&addrs);

    let ids = [N1, N2, N3];
    let mut nodes: Vec<RunningNode> = ids
        .iter()
        .map(|&id| spawn_node(id, addrs[&id], peers[&id].clone()))
        .collect();

    // Elect a leader, then pick a FOLLOWER. Also confirm the follower has LEARNED the leader
    // (its status `leader_id` resolves to the elected leader) so the forward has a target; this
    // is the surfaced engine `leader_id` the forwarding path routes on.
    let leader_idx = poll_until(timeout, || unique_leader(&nodes))
        .expect("a unique leader must be elected within the timeout");
    let leader_id = nodes[leader_idx].handle.id();
    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();
    let learned = poll_until(timeout, || {
        (nodes[follower_idx].status().leader_id == Some(leader_id)).then_some(())
    });
    assert!(
        learned.is_some(),
        "the follower must learn (recognize) the leader before forwarding; leader_id = {:?}",
        nodes[follower_idx].status().leader_id
    );

    // Propose ON THE FOLLOWER. With forwarding, this returns Some(index): the follower forwarded
    // the entry to the leader, which appended + replicated it.
    let want = EntryPayload::Bytes(b"ha-9-follower-forward".to_vec());
    let proposed_index = propose_on(&nodes[follower_idx].handle, want.clone())
        .expect("a follower propose must FORWARD to the leader and return the committed index");
    assert!(proposed_index >= 1, "a proposed index is 1-based");

    // Every node applies through the forwarded entry's index.
    let converged = poll_until(timeout, || {
        nodes
            .iter()
            .all(|n| n.status().last_applied >= proposed_index)
            .then_some(())
    });
    assert!(
        converged.is_some(),
        "all nodes must apply the forwarded entry through index {proposed_index}; got {:?}",
        nodes
            .iter()
            .map(|n| n.status().last_applied)
            .collect::<Vec<_>>()
    );

    // The committed entry at the forwarded index is OUR payload on every node.
    let applied: [Vec<LogEntry>; 3] = [
        drain_applied(&mut nodes[0]),
        drain_applied(&mut nodes[1]),
        drain_applied(&mut nodes[2]),
    ];
    assert_converged(&applied, proposed_index, &want);

    for mut node in nodes {
        node.stop();
    }
}

/// HA-9: a `propose()` on a node that recognizes NO leader returns `None` AT ONCE (no hang). A
/// single node booted into a THREE-voter set can never reach a majority, so it never elects a
/// leader and never learns one: its `leader_id` stays `None`. A propose there must resolve
/// `None` promptly (the caller retries), not block waiting for a leader that will never appear.
#[test]
fn propose_with_no_known_leader_returns_none_without_hanging() {
    let timeout = Duration::from_secs(10);

    // Three configured voters, but we boot only ONE of them, with the other two as (unreachable)
    // peers. With 1 of 3 alive there is no quorum: this node oscillates Follower/Candidate and
    // never recognizes a leader.
    let addrs: BTreeMap<NodeId, SocketAddr> = [N1, N2, N3]
        .into_iter()
        .map(|id| (id, format!("127.0.0.1:{}", free_port()).parse().unwrap()))
        .collect();
    let mut lone = spawn_node(N1, addrs[&N1], peers_excluding(&addrs, N1));

    // It must NOT be (and stay not) a leader, and its leader_id stays None.
    assert!(
        !lone.status().is_leader(),
        "a lone node in a 3-voter set must not win leadership"
    );

    // A propose must come back None well within the timeout (bounded, no hang). We measure the
    // call returns at all (a hang would never return); the value is None (no leader to forward to).
    let got = poll_until(timeout, || {
        Some(propose_on(&lone.handle, EntryPayload::Noop))
    });
    assert_eq!(
        got,
        Some(None),
        "a propose with no recognized leader must resolve None promptly, not hang"
    );

    lone.stop();
}

/// HA-9: the forward await is BOUNDED. After a leader is elected and a follower has learned it,
/// we KILL a quorum (the leader + the other follower) so (a) no new leader can be elected and
/// (b) the recognized leader is unreachable. A `propose()` on the surviving follower must still
/// RETURN (resolving `None`) within a bounded time -- via the forward timeout if the forward was
/// sent to the now-dead leader, or immediately once the survivor's own election clears its
/// `leader_id`. The load-bearing assertion is "it returns, it does not hang".
#[test]
fn forward_to_partitioned_leader_returns_none_within_timeout_no_hang() {
    let timeout = Duration::from_secs(10);

    let addrs: BTreeMap<NodeId, SocketAddr> = [N1, N2, N3]
        .into_iter()
        .map(|id| (id, format!("127.0.0.1:{}", free_port()).parse().unwrap()))
        .collect();
    let peers = peer_maps(&addrs);

    let ids = [N1, N2, N3];
    let mut nodes: Vec<RunningNode> = ids
        .iter()
        .map(|&id| spawn_node(id, addrs[&id], peers[&id].clone()))
        .collect();

    let leader_idx = poll_until(timeout, || unique_leader(&nodes))
        .expect("a unique leader must be elected within the timeout");
    let leader_id = nodes[leader_idx].handle.id();
    let survivor_idx = (0..3).find(|&i| i != leader_idx).unwrap();
    let other_follower_idx = (0..3)
        .find(|&i| i != leader_idx && i != survivor_idx)
        .unwrap();

    // The survivor must have learned the leader (so it would attempt a forward).
    let learned = poll_until(timeout, || {
        (nodes[survivor_idx].status().leader_id == Some(leader_id)).then_some(())
    });
    assert!(
        learned.is_some(),
        "the survivor must recognize the leader before we partition it"
    );

    // KILL the leader and the other follower: 2 of 3 dead -> no quorum -> the recognized leader is
    // gone and no replacement can be elected. The survivor's forward target is now unreachable.
    nodes[leader_idx].stop();
    nodes[other_follower_idx].stop();

    // A propose on the survivor MUST return (not hang): either the forward to the dead leader times
    // out (FORWARD_TIMEOUT) and resolves None, or the survivor has since become a Candidate (its
    // leader_id cleared) and resolves None immediately. Both are None; both are bounded.
    let got = propose_on(&nodes[survivor_idx].handle, EntryPayload::Noop);
    assert_eq!(
        got, None,
        "a forward to a partitioned-away leader must resolve None (bounded), never hang"
    );

    nodes[survivor_idx].stop();
}
