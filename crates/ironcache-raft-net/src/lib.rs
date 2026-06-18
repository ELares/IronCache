// SPDX-License-Identifier: MIT OR Apache-2.0
//! Production adapter (HA-4a): drive the pure [`ironcache_raft`] engine over real
//! TCP.
//!
//! The Raft engine ([`ironcache_raft::RaftNode`]) is a PURE step function: it owns
//! no clock, no RNG, no transport, and performs no I/O (ADR-0027). It is verified
//! deterministically in the `ironcache-sim` DST harness. This crate is the FIRST
//! sub-slice of the production cutover (HA-4a): it proves that same compiled engine
//! forms a cluster and commits over a REAL network, by wrapping it in a per-node
//! control-plane task that supplies the three things the engine asks of its caller
//! and nothing more:
//!
//! - TIME, read through the [`ironcache_env::SystemEnv`] monotonic clock and passed
//!   into each engine step as `now`. The engine never reads a clock.
//! - RANDOMNESS, the election-timeout jitter, drawn through the same `SystemEnv`'s
//!   RNG and passed in as `&mut dyn RaftRng`. The engine never reaches an RNG.
//! - TRANSPORT and TIMERS, both through the [`ironcache_runtime::Runtime`] seam: a
//!   listener built on `Runtime::accept` feeds inbound messages in, outbound
//!   [`ironcache_clusterbus::PeerConn`]s carry messages out, and every timer the
//!   engine arms is realized as a `Runtime::timer` future.
//!
//! ## What stays pure
//!
//! The engine step ([`RaftNode::on_message`] / [`on_timer`] / [`propose`]) is SYNC
//! and does no I/O: the adapter reads `now` from the env, runs the step into a fresh
//! [`Effects`] set, lets the env borrow end, and only THEN performs the I/O the
//! effects describe (arm timers, send messages). So the engine remains exactly the
//! DST-verified pure function; this crate is the only thing that touches a real
//! clock, socket, or timer, and it touches them only through the sanctioned seams.
//!
//! [`on_timer`]: ironcache_raft::RaftNode::on_timer
//! [`propose`]: ironcache_raft::RaftNode::propose
//! [`Effects`]: ironcache_raft::Effects
//!
//! ## Scope (4a)
//!
//! - HA-4b adds a durable, fsync-backed [`RaftStorage`](ironcache_raft::RaftStorage),
//!   [`FileStorage`] (in [`storage`]): an append-only, CRC-framed record log that is
//!   `fsync`'d before every mutating method returns and REPLAYED on restart, so a
//!   crashed node recovers its `currentTerm` / `votedFor` / `log` and cannot
//!   double-vote in a term it already voted in. The loopback proof still boots fresh
//!   [`MemStorage`](ironcache_raft::MemStorage); `FileStorage` recovery is unit-tested
//!   directly in [`storage`].
//! - NO `serve.rs` / `cmd_cluster` / dispatch changes: this is a new crate plus
//!   tests, purely additive, so it cannot perturb the live cluster.
//! - The [`RecordingSm`] test state machine records the applied entry sequence so
//!   the loopback test can prove all nodes converge to the same committed log; the
//!   real `SlotMap`-projecting config state machine is wired in a later slice when
//!   `serve` consumes this adapter.

#![forbid(unsafe_code)]

use core::time::Duration;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use ironcache_clusterbus::{PeerConn, Reply};
use ironcache_env::{Clock, Env, SystemEnv};
use ironcache_raft::{
    Effects, EntryPayload, LogEntry, NodeId, RaftMsg, RaftNode, RaftRng, RaftStorage, Role,
    StateMachine, TimerOp,
};
use ironcache_runtime::Runtime;
use tokio::sync::{mpsc, oneshot, watch};

/// How long a follower waits for a [`RaftMsg::ForwardProposeResult`] before giving up and
/// resolving the proposal [`NotLeader`](raft_handle::ProposeOutcome::NotLeader) (HA-9
/// leader-forwarding). The await MUST be bounded: a lost forward, a leader change mid-flight, or a
/// partitioned leader would otherwise hang the caller forever. The bound is generous relative to
/// the election timeout (base+jitter 150-300ms) so a healthy leader almost always answers well
/// inside it; on expiry the caller (a CLUSTER mutator, or the replica promotion task) simply
/// retries, by which point it has re-learned the current leader. Measured through the
/// [`Runtime::timer`] seam, never wall-clock (ADR-0003).
const FORWARD_TIMEOUT: Duration = Duration::from_secs(2);

pub mod codec;
pub use codec::{decode_raft_msg, encode_raft_msg};

pub mod storage;
pub use storage::FileStorage;

pub mod config_sm;
pub use config_sm::ConfigSm;

pub mod raft_handle;
pub use raft_handle::{ProposeOutcome, RaftHandle};

/// The cluster-bus command verb that carries an encoded [`RaftMsg`].
///
/// The outbound request is the RESP array `["RAFTMSG", <self_node_id_decimal>,
/// <encoded-bytes>]`: the verb, the SENDER's node id as a decimal string (so the
/// receiver knows which peer the message is `from` without it being in the engine's
/// wire `RaftMsg`, mirroring how real Raft learns a reply's sender from the
/// transport), and the [`codec`]-encoded message bytes as the third bulk argument.
///
/// [`RaftMsg`]: ironcache_raft::RaftMsg
pub const RAFTMSG: &[u8] = b"RAFTMSG";

/// An event the per-node control-plane task processes, one at a time, off its inbox.
///
/// The run loop ([`RaftClusterBusNode::run`]) is a single task that owns the engine,
/// so every input that can change Raft state is funneled through this one mpsc queue
/// and applied serially. That serialization is what lets the engine stay a plain,
/// non-`Sync` value with no internal locking (ADR-0002): there is exactly one writer.
#[derive(Debug)]
pub enum Event {
    /// A decoded [`RaftMsg`] arrived from peer `from` over the listener.
    ///
    /// [`RaftMsg`]: ironcache_raft::RaftMsg
    Inbound {
        /// The sending peer's id (from the `RAFTMSG` command's second argument).
        from: NodeId,
        /// The decoded message.
        msg: ironcache_raft::RaftMsg,
    },
    /// An armed timer fired. `generation` is the arm-epoch this fire belongs to; the
    /// run loop ignores a fire whose generation is stale (a timer that was re-armed
    /// or cancelled after this fire was scheduled), so a superseded election timeout
    /// cannot spuriously start an election.
    Timer {
        /// The timer token ([`ELECTION_TIMEOUT`](ironcache_raft::ELECTION_TIMEOUT) /
        /// [`HEARTBEAT`](ironcache_raft::HEARTBEAT)).
        token: u64,
        /// The arm-epoch this fire was scheduled under.
        generation: u64,
    },
    /// A LOCAL client proposal: append `payload` to the log. The optional `ack` reports back
    /// the assigned log index (`Some(index)`) or `None` if the proposal could not land, so a
    /// caller can learn where (and whether) its entry landed.
    ///
    /// HA-9 LEADER-FORWARDING changed the non-leader case: the run loop no longer immediately
    /// answers `None` on a follower. If this node IS the leader it proposes locally as before; if
    /// it is a FOLLOWER that recognizes a leader, it FORWARDS the proposal to that leader over the
    /// cluster bus and `ack` is fulfilled when the leader replies (or `None` on a bounded timeout);
    /// only when NO leader is known does it answer `None` at once. So `ack = Some(index)` may now be
    /// a commit that happened on the leader after a forward; the caller's contract is unchanged.
    Propose {
        /// The opaque payload to append (the engine never interprets it).
        payload: EntryPayload,
        /// An optional one-shot to receive the proposed index (`None` = not leader / no leader /
        /// forward timed out).
        ack: Option<oneshot::Sender<Option<u64>>>,
    },
    /// A pending forward (HA-9) has exceeded [`FORWARD_TIMEOUT`]: if `corr` is still pending, resolve
    /// it `None` (the caller retries). Posted by the per-forward timeout task the run loop spawns so
    /// a lost `ForwardProposeResult` (a partitioned / changed leader) cannot hang the caller. A
    /// `corr` already completed by its result is simply absent and the fire is a no-op.
    ForwardTimeout {
        /// The correlation id of the forward whose deadline elapsed.
        corr: u64,
    },
}

/// A point-in-time snapshot of the engine state, published to a [`watch`] channel
/// after every step so readers (tests, future observability) can poll role / term /
/// commit progress WITHOUT racing the single-writer run loop.
///
/// Reading the live engine from another task would need shared access to a non-`Sync`
/// value; instead the run loop is the sole reader/writer and publishes this cheap
/// `Copy` snapshot, which is the lock-free way to expose state from a shared-nothing
/// control task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// The node's current role.
    pub role: Role,
    /// The node's persisted current term.
    pub current_term: u64,
    /// The highest log index known to be committed.
    pub commit_index: u64,
    /// The highest log index applied to the state machine.
    pub last_applied: u64,
    /// How many entries the state machine has applied (the apply witness).
    pub applied_count: u64,
    /// The leader this node currently recognizes for its term, if any (HA-9
    /// leader-forwarding). Mirrors the engine's passive [`RaftNode::leader_id`] record:
    /// `Some(self)` on a leader, the recognized peer on a follower, `None` on a
    /// candidate / just-stepped-down node. A follower forwards a proposal to this peer;
    /// [`NodeHandle::leader_hint`] resolves it to a `host:port` via the static peer map.
    pub leader_id: Option<NodeId>,
}

impl Status {
    /// Whether the node currently believes it is leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
}

/// A handle to a running [`RaftClusterBusNode`], held by whoever spawned it.
///
/// It carries the inbox sender (so the listener and local proposers can feed events
/// in) and the status-watch receiver (so readers can observe the node). It is
/// `Clone` so the listener task and the test can each hold one. Dropping every clone
/// closes the inbox, which ends the run loop.
#[derive(Clone)]
pub struct NodeHandle {
    id: NodeId,
    inbox: mpsc::UnboundedSender<Event>,
    status: watch::Receiver<Status>,
    /// Every voter id (including self) to the `SocketAddr` of its `RAFTMSG` cluster-bus listener
    /// (HA-9). Shared so [`NodeHandle::leader_hint`] can resolve the watched `leader_id` to a
    /// `host:port` for a redirect reply, without reaching into the run loop. `Arc` because the
    /// handle is `Clone` (the listener task and the serve path each hold one) and the map is
    /// immutable after boot.
    addrs: Arc<BTreeMap<NodeId, SocketAddr>>,
}

impl NodeHandle {
    /// This node's id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The inbox sender, for feeding [`Event`]s (the listener forwards decoded
    /// inbound messages here; a local client sends [`Event::Propose`]).
    #[must_use]
    pub fn inbox(&self) -> &mpsc::UnboundedSender<Event> {
        &self.inbox
    }

    /// The latest published [`Status`] snapshot (a cheap `Copy` read of the watch
    /// channel's current value; never blocks).
    #[must_use]
    pub fn status(&self) -> Status {
        *self.status.borrow()
    }

    /// The current leader's cluster-bus `host:port`, resolved from the watched
    /// `leader_id` via the static peer map, or `None` when no leader is recognized
    /// (HA-9). Used by the serve path's redirect reply when a forward could not land.
    /// The address is the leader's RAFTMSG (cluster-bus) endpoint, which is the only
    /// per-node address this adapter holds; it is informational in the redirect.
    #[must_use]
    pub fn leader_hint(&self) -> Option<String> {
        let leader = self.status().leader_id?;
        self.addrs.get(&leader).map(SocketAddr::to_string)
    }

    /// Submit a local proposal and await the assigned log index, or `None` if it could not land.
    ///
    /// HA-9 LEADER-FORWARDING: on the LEADER this proposes locally as before; on a FOLLOWER that
    /// recognizes a leader the run loop FORWARDS the proposal to that leader and this await is
    /// fulfilled by the leader's reply (so `Some(index)` may be a commit that happened on the
    /// leader after a forward), bounded by [`FORWARD_TIMEOUT`]; with NO known leader it returns
    /// `None` at once. Returns `None` too if the run loop has stopped (the inbox is closed). The
    /// await does NOT block the shard executor: it parks on the proposal's one-shot ack channel.
    pub async fn propose(&self, payload: EntryPayload) -> Option<u64> {
        let (tx, rx) = oneshot::channel();
        if self
            .inbox
            .send(Event::Propose {
                payload,
                ack: Some(tx),
            })
            .is_err()
        {
            return None;
        }
        rx.await.ok().flatten()
    }
}

/// A LOCAL proposal parked awaiting TRUE COMMIT (HA-prod-commit-ack).
///
/// When this leader appends a local [`Event::Propose`] the run loop does NOT ack at
/// append time; it parks this, keyed by the assigned log index, and resolves it on the
/// commit-advance. `term` is the term of the appended entry: if `term_at(index)` ever
/// reads a DIFFERENT term the entry was overwritten by a new leader before committing,
/// so the ack fails [`None`] (NotLeader). `ack` is the originating [`NodeHandle::propose`]
/// one-shot: `Some(index)` on commit, `None` on overwrite / step-down / shutdown.
struct PendingCommit {
    /// The term of the appended entry, to detect a new leader overwriting the index.
    term: u64,
    /// The originating local one-shot: `Some(index)` committed, `None` not-leader.
    ack: oneshot::Sender<Option<u64>>,
}

/// A FORWARDED proposal (HA-9) parked awaiting TRUE COMMIT (HA-prod-commit-ack).
///
/// The leader-side analog of [`PendingCommit`] for a [`RaftMsg::ForwardPropose`]: rather
/// than answering the origin with the index at append time, the leader parks this keyed by
/// the assigned index and ships a [`RaftMsg::ForwardProposeResult`] only when the entry
/// commits (`outcome = Some(index)`) or is overwritten / this node steps down
/// (`outcome = None`), so a follower's forwarded `+OK` also means COMMITTED.
struct PendingForwardResult {
    /// The term of the appended entry, to detect a new leader overwriting the index.
    term: u64,
    /// The origin peer to send the [`RaftMsg::ForwardProposeResult`] back to.
    origin: NodeId,
    /// The forward's correlation id, echoed in the result so the origin matches it.
    corr: u64,
}

/// The per-node production adapter: owns the pure [`RaftNode`] engine plus the
/// real-world seams it is driven through, and runs the single control-plane task
/// that feeds it events and performs the I/O its [`Effects`] describe.
///
/// Generic over the [`RaftStorage`] `S` and the [`StateMachine`] `M` exactly as the
/// engine is, so the same adapter drives the test [`RecordingSm`] today and the real
/// config state machine in a later slice. Generic over the [`Runtime`] `R` so it
/// runs on the production tokio backend and, in principle, any future backend behind
/// the same seam.
pub struct RaftClusterBusNode<R, S, M>
where
    R: Runtime,
    S: RaftStorage,
    M: StateMachine,
{
    /// The pure consensus engine. The adapter is the ONLY caller; every mutation
    /// happens on the single run-loop task, so the engine needs no internal locking.
    raft: RaftNode<S, M>,
    /// The determinism seam: the run loop reads `now` from this clock and draws
    /// election jitter from this RNG, passing both into each engine step. This is the
    /// adapter's sanctioned source of real time and entropy (ADR-0003); the engine
    /// owns neither.
    env: SystemEnv,
    /// The runtime seam: all socket I/O (outbound connect/send/recv) and every timer
    /// go through this, never raw tokio.
    rt: R,
    /// The static peer-address map: every OTHER voter's id to the `SocketAddr` of its
    /// `RAFTMSG` listener. Used to (lazily) open an outbound connection per peer.
    peers: BTreeMap<NodeId, SocketAddr>,
    /// Pending follower-side forwards (HA-9): the correlation id of each in-flight
    /// [`RaftMsg::ForwardPropose`] to the one-shot that the originating
    /// [`NodeHandle::propose`] await parks on. Fulfilled (and removed) when the matching
    /// [`RaftMsg::ForwardProposeResult`] arrives or the forward's [`FORWARD_TIMEOUT`]
    /// elapses. Owned solely by the single run-loop task (no lock needed).
    pending_forwards: BTreeMap<u64, oneshot::Sender<Option<u64>>>,
    /// LOCAL proposals appended on THIS leader but not yet COMMITTED (HA-prod-commit-ack):
    /// the assigned log index to the [`PendingCommit`] (the parked ack + the entry's term).
    /// Instead of fulfilling the [`Event::Propose`] ack at APPEND time, the run loop parks
    /// it here and resolves it on the COMMIT-ADVANCE: when a step's
    /// [`Effects::committed_through`](ironcache_raft::Effects::committed_through) reaches the
    /// index it is answered `Some(index)` (committed), and if this node loses leadership or
    /// the still-uncommitted entry is overwritten by a new leader it is answered `None`
    /// (NotLeader, the idempotent caller retries). Owned solely by the run loop (no lock).
    pending_commits: BTreeMap<u64, PendingCommit>,
    /// FORWARDED proposals (HA-9) this leader accepted on a follower's behalf but has not
    /// yet COMMITTED (HA-prod-commit-ack): the assigned log index to the
    /// [`PendingForwardResult`] (the origin + correlation id to answer, plus the entry's
    /// term). Like [`pending_commits`](RaftClusterBusNode::pending_commits) but the resolved
    /// outcome is shipped back as a [`RaftMsg::ForwardProposeResult`] rather than a local
    /// one-shot, so a follower's forwarded `+OK` also means COMMITTED. Owned by the run loop.
    pending_forward_results: BTreeMap<u64, PendingForwardResult>,
    /// The next correlation id to assign a forward (HA-9). A monotonic run-loop counter,
    /// NEVER random (ADR-0003): it only needs to be unique among this node's in-flight
    /// forwards, and uniqueness from a counter is deterministic.
    next_corr: u64,
    /// Lazily-opened outbound connections, one per peer. A connection is opened on
    /// first send to a peer and dropped on any I/O error (the next send reconnects);
    /// a dropped `RaftMsg` is harmless because Raft re-sends on the next heartbeat.
    conns: BTreeMap<NodeId, PeerConn<R>>,
    /// Per-token timer arm-epoch. Incremented on every (re)arm or cancel of a token;
    /// a fired [`Event::Timer`] whose `generation` is older than the current epoch for
    /// its token is a superseded timer and is dropped, so re-arming "resets" a timer
    /// without a stale fire ever reaching the engine.
    timer_gen: BTreeMap<u64, u64>,
    /// The inbox sender, cloned into each spawned timer task so a fired timer can post
    /// itself back as an [`Event::Timer`].
    inbox_tx: mpsc::UnboundedSender<Event>,
    /// The inbox RECEIVER, taken by [`RaftClusterBusNode::run`] when the loop starts.
    /// It lives here (not on the handle) because the run loop is the single consumer
    /// of the inbox; an `Option` so `run` can `take` it and consume it by value.
    inbox_rx: Option<mpsc::UnboundedReceiver<Event>>,
    /// The status-snapshot publisher; the run loop sends a fresh [`Status`] after
    /// every step.
    status_tx: watch::Sender<Status>,
}

impl<R, S, M> RaftClusterBusNode<R, S, M>
where
    R: Runtime + Clone + 'static,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
    S: RaftStorage,
    M: StateMachine,
{
    /// Assemble a node: the pure `raft` engine, the `env` / `rt` seams, and the
    /// `peers` address map (every other voter's listener address). Returns the node
    /// (to be driven by [`RaftClusterBusNode::run`]) and a [`NodeHandle`] the caller
    /// keeps to feed events and read status.
    ///
    /// The handle's inbox is the ONLY way state reaches the engine; the listener and
    /// any local proposer push [`Event`]s through it.
    #[must_use]
    pub fn new(
        raft: RaftNode<S, M>,
        env: SystemEnv,
        rt: R,
        peers: BTreeMap<NodeId, SocketAddr>,
    ) -> (Self, NodeHandle) {
        let id = raft.id();
        let (inbox_tx, inbox_rx) = mpsc::unbounded_channel();

        let initial = Status {
            role: raft.role(),
            current_term: raft.current_term(),
            commit_index: raft.commit_index(),
            last_applied: raft.last_applied(),
            applied_count: raft.applied_count(),
            leader_id: raft.leader_id(),
        };
        let (status_tx, status_rx) = watch::channel(initial);

        // The peer-address map for leader_hint resolution (HA-9): the OTHER voters' bus endpoints.
        // SELF is deliberately absent (its bus address is not in `peers`): the serve path only
        // calls leader_hint on a NON-leader redirect, so the resolved leader is always a PEER; a
        // self-leader never redirects. Immutable after boot, shared by Arc with every handle clone.
        let addrs = Arc::new(peers.clone());

        let handle = NodeHandle {
            id,
            inbox: inbox_tx.clone(),
            status: status_rx,
            addrs,
        };
        let node = RaftClusterBusNode {
            raft,
            env,
            rt,
            peers,
            pending_forwards: BTreeMap::new(),
            pending_commits: BTreeMap::new(),
            pending_forward_results: BTreeMap::new(),
            next_corr: 0,
            conns: BTreeMap::new(),
            timer_gen: BTreeMap::new(),
            inbox_tx,
            inbox_rx: Some(inbox_rx),
            status_tx,
        };
        (node, handle)
    }

    /// Run the control-plane loop until the inbox closes (every [`NodeHandle`]
    /// dropped).
    ///
    /// This is the single task that owns the engine. It first calls
    /// [`RaftNode::start`] to arm the initial election timer (draining the resulting
    /// effects), then loops: pull the next [`Event`] off the inbox, run the matching
    /// SYNC engine step into a fresh [`Effects`] set, then DRAIN those effects (arm
    /// timers, send messages) and publish a fresh [`Status`]. The engine borrow of
    /// the env ends before any I/O, so the engine never observes a clock mid-step and
    /// stays the DST-verified pure function.
    ///
    /// Spawn this with [`Runtime::spawn_on_shard`] (or a `LocalSet`'s `spawn_local`):
    /// it is `!Send` because the engine and `PeerConn`s are shard-local, matching the
    /// shared-nothing model (ADR-0002).
    pub async fn run(mut self) {
        let mut inbox = self
            .inbox_rx
            .take()
            .expect("run() called once: the inbox receiver is present exactly once");

        // Arm the initial election timer (RaftNode::start). Read `now` from the env,
        // run the (sync) start, drop the env borrow, then drain.
        let mut effects = Effects::new();
        {
            let now = self.env.now();
            let rng: &mut dyn RaftRng = self.env.rng();
            self.raft.start(now, rng, &mut effects);
        }
        let committed_through = effects.committed_through;
        self.drain_effects(effects).await;
        self.resolve_pending_commits(committed_through).await;
        self.publish_status();

        while let Some(event) = inbox.recv().await {
            let mut effects = Effects::new();
            match event {
                Event::Inbound { from, msg } => {
                    // HA-9: intercept the transport-level forwarding messages BEFORE the engine
                    // (the pure engine treats them as inert no-ops; the forwarding logic lives
                    // here in the adapter). Everything else is a real RPC for the engine.
                    match msg {
                        RaftMsg::ForwardPropose { corr, payload } => {
                            self.on_forward_propose(from, corr, payload, &mut effects);
                        }
                        RaftMsg::ForwardProposeResult { corr, outcome } => {
                            self.on_forward_result(corr, outcome);
                        }
                        other => {
                            let now = self.env.now();
                            let rng: &mut dyn RaftRng = self.env.rng();
                            self.raft.on_message(now, rng, from, other, &mut effects);
                        }
                    }
                }
                Event::Timer { token, generation } => {
                    // Drop a superseded fire: if this token has been re-armed or
                    // cancelled since `generation` was scheduled, the current epoch is
                    // higher and this fire is stale. Equality means it is the live arm.
                    if self.timer_gen.get(&token).copied().unwrap_or(0) != generation {
                        continue;
                    }
                    let now = self.env.now();
                    let rng: &mut dyn RaftRng = self.env.rng();
                    self.raft.on_timer(now, rng, token, &mut effects);
                }
                Event::Propose { payload, ack } => {
                    self.on_local_propose(payload, ack, &mut effects).await;
                }
                Event::ForwardTimeout { corr } => {
                    // HA-9: a forward exceeded FORWARD_TIMEOUT. If still pending, resolve it
                    // `None` so the caller stops waiting and retries; an already-answered corr is
                    // absent and this is a no-op. No engine step, so no effects.
                    if let Some(ack) = self.pending_forwards.remove(&corr) {
                        let _ = ack.send(None);
                    }
                }
            }
            // HA-prod-commit-ack: capture the commit high-water this step reached BEFORE
            // the effects are drained (drain consumes them by value), then resolve any
            // parked propose acks the advance (or a leadership change) settles.
            let committed_through = effects.committed_through;
            self.drain_effects(effects).await;
            self.resolve_pending_commits(committed_through).await;
            self.publish_status();
        }

        // The inbox closed (every NodeHandle dropped): the node is going away. Drop every
        // still-parked propose ack so its caller does not hang -- the dropped one-shot
        // resolves the `NodeHandle::propose` await to `None` (NotLeader), and the dropped
        // forward-result map simply stops answering forwards (their origins time out and
        // retry). No leak: both maps are emptied as `self` is dropped.
        self.pending_commits.clear();
        self.pending_forward_results.clear();
    }

    /// Handle a LOCAL [`Event::Propose`] (HA-9 leader-forwarding). Three cases:
    ///
    /// - THIS node IS the leader: propose locally exactly as before, ack the assigned index.
    /// - A FOLLOWER that recognizes a leader: FORWARD the proposal to that leader over the cluster
    ///   bus (a fresh correlation id, the ack parked in `pending_forwards`, a bounded timeout armed)
    ///   and DO NOT ack now; the ack is fulfilled by the leader's `ForwardProposeResult` or the
    ///   timeout. No engine step runs (the follower's engine would just reject it), so `effects` is
    ///   untouched here; the bus send happens immediately below.
    /// - NO leader known (a candidate, or just after a step-down): ack `None` at once. The caller
    ///   retries, and by then a leader is usually known.
    async fn on_local_propose(
        &mut self,
        payload: EntryPayload,
        ack: Option<oneshot::Sender<Option<u64>>>,
        effects: &mut Effects,
    ) {
        if self.raft.is_leader() {
            let now = self.env.now();
            let rng: &mut dyn RaftRng = self.env.rng();
            let index = self.raft.propose(payload, now, rng, effects);
            // HA-prod-commit-ack: do NOT ack at append time. Park the ack keyed by the
            // assigned index; the post-drain resolve pass fulfils it on the commit-advance
            // (Some(index)) or fails it (None) if this node loses leadership / the entry is
            // overwritten. A single-voter cluster commits within THIS step, so the very next
            // resolve pass (committed_through >= index) answers it promptly, unchanged.
            match (index, ack) {
                (Some(index), Some(ack)) => {
                    // Record the appended entry's term so an overwrite (a new leader putting
                    // a different term at this index) is detectable before it commits.
                    let term = self.raft.current_term();
                    self.pending_commits
                        .insert(index, PendingCommit { term, ack });
                }
                // propose() returned None (not leader after all -- a race the is_leader()
                // guard makes unlikely, but handle it): nothing landed, so NotLeader now.
                (None, Some(ack)) => {
                    let _ = ack.send(None);
                }
                // No ack channel: a fire-and-forget propose, nothing to resolve.
                (_, None) => {}
            }
            return;
        }

        // Not the leader: forward to the recognized leader if there is one. A leader that is THIS
        // node was handled above; leader_id() == Some(self) cannot occur on a non-leader.
        let leader = self.raft.leader_id();
        match (leader, ack) {
            (Some(leader), Some(ack)) if self.peers.contains_key(&leader) => {
                let corr = self.next_corr;
                self.next_corr = self.next_corr.wrapping_add(1);
                self.pending_forwards.insert(corr, ack);
                // Arm the bounded timeout so a lost result / changed leader cannot hang the caller.
                let tx = self.inbox_tx.clone();
                let rt = self.rt.clone();
                self.rt.spawn_on_shard(async move {
                    rt.timer(FORWARD_TIMEOUT).await;
                    let _ = tx.send(Event::ForwardTimeout { corr });
                });
                // Send the forward over the cluster bus (best-effort, like any send; on drop the
                // timeout resolves NotLeader and the caller retries).
                let msg = RaftMsg::ForwardPropose { corr, payload };
                self.send_to_peer(leader, &msg).await;
            }
            // A known leader that is not a configured peer (defensive), no leader at all, or no ack
            // channel: there is nothing to forward to / nobody to answer. Resolve NotLeader now.
            (_, ack) => {
                if let Some(ack) = ack {
                    let _ = ack.send(None);
                }
            }
        }
    }

    /// Handle an inbound [`RaftMsg::ForwardPropose`] (HA-9): a peer `from` asked us to propose
    /// `payload` on its behalf. If we are the leader, propose locally (the same engine machinery a
    /// local `Propose` uses) and PARK the result keyed by the assigned index (HA-prod-commit-ack):
    /// the [`RaftMsg::ForwardProposeResult`] is shipped only when the entry COMMITS (`Some(index)`)
    /// or can no longer commit here (`None`), so the follower's forwarded `+OK` also means COMMITTED.
    /// If we are NOT the leader (or the propose did not land), reply `None` AT ONCE WITHOUT chaining
    /// the forward onward (the ONE-HOP rule: the origin retries and by then knows the new leader, so
    /// a second hop would only add latency and risk a loop). The local propose's `effects` (the
    /// replication AppendEntries it triggers) are drained by the caller.
    fn on_forward_propose(
        &mut self,
        from: NodeId,
        corr: u64,
        payload: EntryPayload,
        effects: &mut Effects,
    ) {
        if self.raft.is_leader() {
            let now = self.env.now();
            let rng: &mut dyn RaftRng = self.env.rng();
            // propose() appends + replicates and returns the assigned index (Some) on a leader.
            if let Some(index) = self.raft.propose(payload, now, rng, effects) {
                // HA-prod-commit-ack: do NOT answer the forward at append time. Park the
                // result keyed by the assigned index; the resolve pass ships a
                // ForwardProposeResult { Some(index) } on commit, or { None } if this node
                // steps down / the entry is overwritten, so the follower's forwarded +OK
                // also means COMMITTED. (A single-voter leader commits within this step, so
                // the very next resolve pass answers it at once.)
                let term = self.raft.current_term();
                self.pending_forward_results.insert(
                    index,
                    PendingForwardResult {
                        term,
                        origin: from,
                        corr,
                    },
                );
                return;
            }
            // propose() returned None despite being leader (e.g. a one-change-in-flight
            // membership refusal): nothing landed, answer NotLeader now.
        }
        // Not the leader (one-hop only: we do not re-forward; the origin retries), or the
        // propose did not land. Answer None immediately, queued as an engine SEND so it ships
        // through the same encode + PeerConn path as any other effect.
        effects.sends.push((
            from,
            RaftMsg::ForwardProposeResult {
                corr,
                outcome: None,
            },
        ));
    }

    /// Handle an inbound [`RaftMsg::ForwardProposeResult`] (HA-9): the leader answered a forward we
    /// sent. Complete and remove the matching pending one-shot so the originating
    /// [`NodeHandle::propose`] await resolves with the outcome. A `corr` we no longer hold (already
    /// timed out, or a duplicate result) is simply ignored.
    fn on_forward_result(&mut self, corr: u64, outcome: Option<u64>) {
        if let Some(ack) = self.pending_forwards.remove(&corr) {
            let _ = ack.send(outcome);
        }
    }

    /// Resolve parked propose acks after a step (HA-prod-commit-ack). Called once per
    /// event, AFTER the step's effects are drained, with the commit high-water this step
    /// reached (`committed_through`, `None` if commit did not advance). Two passes:
    ///
    /// 1. COMMITTED: every parked entry whose index is `<= committed_through` is now on a
    ///    majority. Fulfil its local ack with `Some(index)` (Committed) or ship its
    ///    forwarded `ForwardProposeResult { Some(index) }`, and remove it.
    /// 2. FAILED: a parked entry that did NOT commit but can no longer commit HERE is
    ///    resolved `None` (NotLeader, the idempotent caller retries). That is true when
    ///    this node is no longer the leader (a step-down: it can no longer drive the entry
    ///    to commit), or the entry was OVERWRITTEN before committing (a different term now
    ///    occupies the index, or the index was truncated below the log's end) -- exactly the
    ///    overwrite the commit-on-append behaviour used to hide.
    ///
    /// Monotone + leak-free: an entry is removed the first time either pass settles it, so
    /// each ack fires exactly once and the maps never retain a settled entry.
    async fn resolve_pending_commits(&mut self, committed_through: Option<u64>) {
        // Pass 1: COMMIT everything at-or-below the new high-water.
        if let Some(hi) = committed_through {
            // Local proposals: split off the committed prefix [..=hi] and ack each.
            let mut committed = self.pending_commits.split_off(&(hi + 1));
            core::mem::swap(&mut committed, &mut self.pending_commits);
            for (index, pending) in committed {
                let _ = pending.ack.send(Some(index));
            }
            // Forwarded proposals: same, but ship a ForwardProposeResult to the origin.
            let mut committed_fwd = self.pending_forward_results.split_off(&(hi + 1));
            core::mem::swap(&mut committed_fwd, &mut self.pending_forward_results);
            for (index, pending) in committed_fwd {
                let msg = RaftMsg::ForwardProposeResult {
                    corr: pending.corr,
                    outcome: Some(index),
                };
                self.send_to_peer(pending.origin, &msg).await;
            }
        }

        // Pass 2: FAIL anything that can no longer commit here (step-down or overwrite).
        let stepped_down = !self.raft.is_leader();
        // Collect the indices to fail first (cannot mutate the maps while iterating, and
        // the forward replies need an await outside the borrow).
        let fail_local: Vec<u64> = self
            .pending_commits
            .iter()
            .filter(|&(&index, p)| stepped_down || self.entry_overwritten(index, p.term))
            .map(|(&index, _)| index)
            .collect();
        for index in fail_local {
            if let Some(pending) = self.pending_commits.remove(&index) {
                let _ = pending.ack.send(None);
            }
        }
        let fail_fwd: Vec<u64> = self
            .pending_forward_results
            .iter()
            .filter(|&(&index, p)| stepped_down || self.entry_overwritten(index, p.term))
            .map(|(&index, _)| index)
            .collect();
        for index in fail_fwd {
            if let Some(pending) = self.pending_forward_results.remove(&index) {
                let msg = RaftMsg::ForwardProposeResult {
                    corr: pending.corr,
                    outcome: None,
                };
                self.send_to_peer(pending.origin, &msg).await;
            }
        }
    }

    /// Whether the parked entry at `index` (appended in term `parked_term`) has been
    /// OVERWRITTEN before committing (HA-prod-commit-ack). True when the log no longer
    /// holds `parked_term` at `index`: a NEW leader put a different term there (a
    /// conflict truncation + re-append), or the index was truncated away entirely (the
    /// engine's `term_at` returns 0 past the log end / for a compacted index). A
    /// still-present entry reads back its own term, so this is false on the common path.
    /// Only meaningful for an index strictly above `commit_index` (a committed entry is
    /// resolved by pass 1 and removed, so it is never re-examined here).
    fn entry_overwritten(&self, index: u64, parked_term: u64) -> bool {
        self.raft.storage().term_at(index) != parked_term
    }

    /// Drain one step's [`Effects`]: arm/cancel timers first, then send messages
    /// (the engine's documented drain order). Timer ops update the per-token epoch
    /// and spawn a [`Runtime::timer`] future per `Set`; sends ship the encoded
    /// message over the peer's (lazily-opened) [`PeerConn`].
    async fn drain_effects(&mut self, effects: Effects) {
        for op in effects.timer_ops {
            match op {
                TimerOp::Set { token, after } => {
                    // Bump the token's epoch so any in-flight earlier fire is now
                    // stale, then spawn a timer that posts THIS epoch back when it
                    // elapses. The engine re-checks role/term on fire, but the epoch
                    // tag also prevents a re-armed election timeout from firing twice.
                    let generation = self.bump_timer_gen(token);
                    let tx = self.inbox_tx.clone();
                    let rt = self.rt.clone();
                    self.rt.spawn_on_shard(async move {
                        rt.timer(after).await;
                        // A closed inbox (the node stopped) makes this a no-op.
                        let _ = tx.send(Event::Timer { token, generation });
                    });
                }
                TimerOp::Cancel { token } => {
                    // Bump the epoch so any pending fire for this token is dropped on
                    // arrival; there is no live arm to schedule.
                    let _ = self.bump_timer_gen(token);
                }
            }
        }
        for (to, msg) in effects.sends {
            self.send_to_peer(to, &msg).await;
        }
    }

    /// Increment and return the current arm-epoch for `token`. A fired timer carrying
    /// an epoch below the current one is superseded and dropped by the run loop.
    fn bump_timer_gen(&mut self, token: u64) -> u64 {
        let generation = self.timer_gen.entry(token).or_insert(0);
        *generation += 1;
        *generation
    }

    /// Ship one encoded [`RaftMsg`] to peer `to` over its [`PeerConn`], opening the
    /// connection lazily and dropping it on any error so the next send reconnects.
    ///
    /// A send is best-effort: an unknown peer, a connect failure, or an I/O error is
    /// logged-by-drop, not retried here. That is correct for Raft, which re-sends
    /// state via the next heartbeat / election; a dropped control message never breaks
    /// safety, only (briefly) liveness.
    async fn send_to_peer(&mut self, to: NodeId, msg: &ironcache_raft::RaftMsg) {
        let Some(&addr) = self.peers.get(&to) else {
            // Not a configured peer (e.g. a stray id); nothing to do.
            return;
        };

        // Open the connection lazily if we do not hold one for this peer.
        if !self.conns.contains_key(&to) {
            match PeerConn::connect(&self.rt, addr).await {
                Ok(conn) => {
                    self.conns.insert(to, conn);
                }
                Err(_) => return, // reconnect on the next send
            }
        }

        let encoded = encode_raft_msg(msg);
        // The RAFTMSG command: verb, OUR id as decimal, the encoded message bytes.
        let self_id = self.raft.id().0.to_string();
        let args: [&[u8]; 3] = [RAFTMSG, self_id.as_bytes(), &encoded];

        let conn = self
            .conns
            .get_mut(&to)
            .expect("connection just inserted/present");
        match conn.request(&self.rt, &args).await {
            Ok(Reply::Simple(_)) => {} // +OK ack; the reply body is not needed.
            // Any other reply kind, a remote error, or an I/O failure: drop the
            // connection so the next send to this peer reconnects fresh.
            _ => {
                self.conns.remove(&to);
            }
        }
    }

    /// Publish a fresh [`Status`] snapshot from the live engine to the watch channel.
    fn publish_status(&self) {
        let status = Status {
            role: self.raft.role(),
            current_term: self.raft.current_term(),
            commit_index: self.raft.commit_index(),
            last_applied: self.raft.last_applied(),
            applied_count: self.raft.applied_count(),
            leader_id: self.raft.leader_id(),
        };
        // A send error means every reader dropped; the node can still run, so ignore.
        let _ = self.status_tx.send(status);
    }
}

// ---------------------------------------------------------------------------
// The RAFTMSG listener.
// ---------------------------------------------------------------------------

/// Bind `addr` and serve the inbound `RAFTMSG` stream into `inbox` until the
/// listener errors or the process ends.
///
/// One dedicated listener per node. It accepts connections (each peer keeps one open
/// for its outbound sends) and, per connection, recv-loops: it parses RESP command
/// frames, and for each `["RAFTMSG", <from-id>, <encoded-bytes>]` it [`decode`]s the
/// message, forwards an [`Event::Inbound`] to the run loop's inbox, and replies
/// `+OK`. A malformed frame or a decode failure closes that connection (the peer
/// reconnects); the listener keeps accepting.
///
/// [`decode`]: decode_raft_msg
pub async fn run_listener<R>(rt: R, listener: R::Listener, inbox: mpsc::UnboundedSender<Event>)
where
    R: Runtime + Clone + 'static,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
{
    loop {
        let Ok((stream, _peer)) = rt.accept(&listener).await else {
            // The listener socket failed; stop serving (the node is going away).
            return;
        };
        // Each connection is served on its own shard-local task so a slow or stuck
        // peer cannot block accepting the others.
        let rt2 = rt.clone();
        let inbox2 = inbox.clone();
        rt.spawn_on_shard(async move {
            serve_conn::<R>(&rt2, stream, &inbox2).await;
        });
    }
}

/// Serve one accepted connection: decode `RAFTMSG` commands, feed [`Event::Inbound`]
/// to the inbox, reply `+OK`. Returns when the peer closes or sends a malformed /
/// undecodable frame.
async fn serve_conn<R>(rt: &R, mut stream: R::Stream, inbox: &mpsc::UnboundedSender<Event>)
where
    R: Runtime,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
{
    let mut pending: Vec<u8> = Vec::new();
    loop {
        // Try to parse a complete command out of what we already have.
        match parse_raftmsg_command(&pending) {
            Ok(Some((from, msg, consumed))) => {
                pending.drain(..consumed);
                // Forward to the run loop; a closed inbox means the node stopped.
                if inbox.send(Event::Inbound { from, msg }).is_err() {
                    return;
                }
                // Acknowledge so the sender's `request` (which reads exactly one
                // reply) completes and it can pipeline the next message.
                let ok = b"+OK\r\n".to_vec();
                if rt.send(&mut stream, ok.into()).await.is_err() {
                    return;
                }
                // Loop to parse any further buffered commands before reading again.
                continue;
            }
            Ok(None) => {}     // need more bytes
            Err(()) => return, // malformed frame: drop the connection
        }

        // Need more bytes: read another chunk, appending to `pending`.
        let taken: R::Buf = core::mem::take(&mut pending).into();
        match rt.recv(&mut stream, taken).await {
            Ok(res) => {
                if res.n == 0 {
                    return; // peer closed
                }
                pending = res.buf.into();
            }
            Err(_) => return,
        }
    }
}

/// A parsed `RAFTMSG` command: the sending peer id, the decoded message, and the
/// number of bytes the command occupied in the buffer.
type ParsedRaftMsg = (NodeId, ironcache_raft::RaftMsg, usize);

/// Try to parse one `["RAFTMSG", <from-id>, <encoded-bytes>]` command from `buf`.
///
/// Returns `Ok(Some((from, msg, consumed)))` when a full, well-formed RAFTMSG command
/// is present (with the byte length it occupied), `Ok(None)` when more bytes are
/// needed, and `Err(())` for a framing error or a command that is not a decodable
/// RAFTMSG (a non-array, a wrong verb / arity, a non-numeric sender id, or bytes that
/// do not [`decode`] to a `RaftMsg`).
///
/// [`decode`]: decode_raft_msg
fn parse_raftmsg_command(buf: &[u8]) -> Result<Option<ParsedRaftMsg>, ()> {
    let Some((args, consumed)) = parse_command_array(buf)? else {
        return Ok(None);
    };
    // Exactly three args: the verb, the sender id, the encoded message.
    if args.len() != 3 || !args[0].eq_ignore_ascii_case(RAFTMSG) {
        return Err(());
    }
    let from_id: u64 = core::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(())?;
    let msg = decode_raft_msg(&args[2]).ok_or(())?;
    Ok(Some((NodeId(from_id), msg, consumed)))
}

/// A parsed RESP command: its bulk-string args plus the number of bytes it occupied.
type ParsedCommand = (Vec<Vec<u8>>, usize);

/// Parse one RESP array-of-bulk-strings command (`*N\r\n$len\r\narg\r\n...`) from
/// `buf`, the request shape [`ironcache_clusterbus`] encodes.
///
/// Returns the decoded args plus the bytes consumed, or `Ok(None)` if the command is
/// not yet fully buffered, or `Err(())` on a malformed frame. This is the inbound
/// counterpart to the bus's outbound `encode_command`; the bus crate only ships a
/// REPLY decoder, so the request decoder lives here with its consumer.
fn parse_command_array(buf: &[u8]) -> Result<Option<ParsedCommand>, ()> {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] != b'*' {
        return Err(());
    }
    let mut pos = 0usize;
    let Some((count, next)) = read_int_line(buf, pos)? else {
        return Ok(None);
    };
    pos = next;
    let count = usize::try_from(count).map_err(|_| ())?;
    let mut args = Vec::with_capacity(count.min(16));
    for _ in 0..count {
        // Each arg is a bulk string: `$len\r\n<bytes>\r\n`.
        match buf.get(pos) {
            Some(b'$') => {}
            Some(_) => return Err(()),
            None => return Ok(None),
        }
        let Some((len, next)) = read_int_line(buf, pos)? else {
            return Ok(None);
        };
        let len = usize::try_from(len).map_err(|_| ())?;
        let body_start = next;
        let body_end = body_start.checked_add(len).ok_or(())?;
        let crlf_end = body_end.checked_add(2).ok_or(())?;
        if buf.len() < crlf_end {
            return Ok(None);
        }
        if &buf[body_end..crlf_end] != b"\r\n" {
            return Err(());
        }
        args.push(buf[body_start..body_end].to_vec());
        pos = crlf_end;
    }
    Ok(Some((args, pos)))
}

/// Read a `<prefix-char><int>\r\n` header line starting at `start` (the prefix char
/// is already validated by the caller), returning the parsed integer and the index
/// just past the `\r\n`, or `Ok(None)` if the line is not yet complete.
fn read_int_line(buf: &[u8], start: usize) -> Result<Option<(i64, usize)>, ()> {
    // The prefix char is at `start`; the number runs to the next CRLF.
    let rest = &buf[start + 1..];
    let Some(rel) = rest.windows(2).position(|w| w == b"\r\n") else {
        return Ok(None);
    };
    let line = &rest[..rel];
    let n: i64 = core::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(())?;
    // Absolute index just past the CRLF: start + 1 (prefix) + rel + 2 (CRLF).
    Ok(Some((n, start + 1 + rel + 2)))
}

// ---------------------------------------------------------------------------
// A recording state machine, for the loopback convergence proof.
// ---------------------------------------------------------------------------

/// A [`StateMachine`] that RECORDS every applied entry, so the loopback integration
/// test can assert all nodes converged to the SAME committed sequence.
///
/// 4a proves the transport plus consensus over real TCP; the convergence witness is
/// "every node applied the identical `(index, payload)` list in order". This machine
/// keeps that list internally AND mirrors each applied entry down an optional
/// [`mpsc`] channel, so an OUTSIDE observer (the test) can watch a node's apply
/// stream WITHOUT reaching into the run loop that owns the engine. (The internal
/// `Vec` is unreachable once the engine is moved into the run loop; the channel is
/// how applied entries escape the single-writer task.) The real
/// `SlotMap`-projecting config state machine belongs to a later slice (when `serve`
/// consumes the adapter); here we only need a deterministic record of what committed.
#[derive(Debug, Default)]
pub struct RecordingSm {
    /// Every applied entry, in apply (ascending index) order (the in-task record).
    applied: Vec<LogEntry>,
    /// An optional mirror sink: each applied entry is also sent here so an external
    /// observer can collect the converged sequence. `None` means record-only.
    sink: Option<mpsc::UnboundedSender<LogEntry>>,
}

impl RecordingSm {
    /// A fresh recorder that keeps an internal record only (no external mirror).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A recorder that ALSO mirrors every applied entry down `sink`, so an external
    /// observer (a test) can collect the applied sequence as the run loop applies it.
    #[must_use]
    pub fn with_sink(sink: mpsc::UnboundedSender<LogEntry>) -> Self {
        RecordingSm {
            applied: Vec::new(),
            sink: Some(sink),
        }
    }

    /// The applied entries, in order (for in-process inspection).
    #[must_use]
    pub fn applied(&self) -> &[LogEntry] {
        &self.applied
    }
}

impl StateMachine for RecordingSm {
    fn apply(&mut self, entry: &LogEntry) {
        self.applied.push(entry.clone());
        if let Some(sink) = &self.sink {
            // Best-effort mirror: a closed sink (observer gone) just drops it.
            let _ = sink.send(entry.clone());
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        // HA-3c: the loopback recorder's whole applied state is the COUNT of entries it has
        // applied (the convergence witness is the apply sequence, which the test collects via
        // the sink). Serialize that count little-endian so a node restored from a snapshot
        // resumes a consistent apply WATERMARK; the per-entry record is a test artifact that
        // does not survive compaction (compaction is opt-in via the config and this test SM is
        // only used by the loopback proof, which does not exercise it).
        (self.applied.len() as u64).to_le_bytes().to_vec()
    }

    fn restore(&mut self, _data: &[u8]) {
        // HA-3c: restore by clearing the recorded sequence (the snapshot subsumes it). The
        // count is implicit in the cleared-then-replayed tail; the recorder does not need to
        // reconstruct the pre-snapshot entries (it never serialized them), so this is a clear.
        self.applied.clear();
    }
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use ironcache_raft::{ConfigCmd, MembershipChange, RaftMsg};

    /// A single round-trip assertion: encode then decode must reproduce the input
    /// byte-for-byte (PartialEq over the whole `RaftMsg`).
    fn assert_round_trips(msg: &RaftMsg) {
        let bytes = encode_raft_msg(msg);
        let decoded = decode_raft_msg(&bytes)
            .unwrap_or_else(|| panic!("decode failed for {msg:?} (encoded {bytes:?})"));
        assert_eq!(&decoded, msg, "round-trip mismatch for {msg:?}");
    }

    /// THE codec gate: every `RaftMsg` variant survives encode -> decode unchanged,
    /// including an `AppendEntries` carrying several `LogEntry` of each `EntryPayload`
    /// kind (Noop, Bytes, and every `ConfigCmd` shape inside a Config payload). The
    /// wire codec is the one place that can silently corrupt consensus, so this
    /// exercises the full surface, not just the scalar messages.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn codec_round_trips_every_raftmsg_variant() {
        // RequestVote.
        assert_round_trips(&RaftMsg::RequestVote {
            term: 7,
            candidate: NodeId(42),
            last_log_index: 99,
            last_log_term: 6,
        });
        // RequestVote at the zero-edges (empty-log sentinel, node 0).
        assert_round_trips(&RaftMsg::RequestVote {
            term: 0,
            candidate: NodeId(0),
            last_log_index: 0,
            last_log_term: 0,
        });

        // RequestVoteResp, both polarities.
        assert_round_trips(&RaftMsg::RequestVoteResp {
            term: 3,
            vote_granted: true,
        });
        assert_round_trips(&RaftMsg::RequestVoteResp {
            term: u64::MAX,
            vote_granted: false,
        });

        // AppendEntriesResp, both polarities + a large match_index.
        assert_round_trips(&RaftMsg::AppendEntriesResp {
            term: 11,
            success: true,
            match_index: 12_345,
        });
        assert_round_trips(&RaftMsg::AppendEntriesResp {
            term: 11,
            success: false,
            match_index: u64::MAX,
        });

        // Propose with each payload kind.
        assert_round_trips(&RaftMsg::Propose {
            payload: EntryPayload::Noop,
        });
        assert_round_trips(&RaftMsg::Propose {
            payload: EntryPayload::Bytes(vec![]),
        });
        assert_round_trips(&RaftMsg::Propose {
            payload: EntryPayload::Bytes(vec![0, 1, 2, 255, 254, 13, 10]),
        });

        // AppendEntries: empty (a heartbeat).
        assert_round_trips(&RaftMsg::AppendEntries {
            term: 5,
            leader: NodeId(2),
            prev_log_index: 4,
            prev_log_term: 5,
            entries: vec![],
            leader_commit: 4,
        });

        // HA-9 ForwardPropose: a follower hands the leader a proposal. Cover each
        // payload kind + the zero/large corr edges.
        assert_round_trips(&RaftMsg::ForwardPropose {
            corr: 0,
            payload: EntryPayload::Noop,
        });
        assert_round_trips(&RaftMsg::ForwardPropose {
            corr: u64::MAX,
            payload: EntryPayload::Bytes(vec![0, 1, 2, 255, 13, 10]),
        });
        assert_round_trips(&RaftMsg::ForwardPropose {
            corr: 7,
            payload: EntryPayload::Config(ConfigCmd::PromoteReplica {
                slots: vec![0, 100, 16_383],
                new_primary: "abababababababababababababababababababab".to_owned(),
            }),
        });

        // HA-9 ForwardProposeResult: BOTH outcomes (Some(index) accepted, None
        // not-leader) + the zero/large edges must round-trip byte-for-byte.
        assert_round_trips(&RaftMsg::ForwardProposeResult {
            corr: 7,
            outcome: Some(42),
        });
        assert_round_trips(&RaftMsg::ForwardProposeResult {
            corr: u64::MAX,
            outcome: Some(0),
        });
        assert_round_trips(&RaftMsg::ForwardProposeResult {
            corr: 0,
            outcome: None,
        });

        // AppendEntries: a vector with a LogEntry of EVERY payload kind, including
        // every ConfigCmd shape inside a Config payload. This is the field most
        // likely to be mis-framed, so cover the whole payload taxonomy here.
        assert_round_trips(&RaftMsg::AppendEntries {
            term: 8,
            leader: NodeId(3),
            prev_log_index: 4,
            prev_log_term: 5,
            entries: every_payload_kind_entries(),
            leader_commit: 11,
        });

        // HA-3c InstallSnapshot: empty data, a typical config-snapshot blob (arbitrary
        // bytes incl. zero / CRLF / 0xFF), and the zero/large index/term edges, all
        // round-trip byte-for-byte (the snapshot data is the field most likely to be
        // mis-framed, so cover the length-prefixed blob edges).
        assert_round_trips(&RaftMsg::InstallSnapshot {
            term: 0,
            leader_id: NodeId(0),
            last_included_index: 0,
            last_included_term: 0,
            data: vec![],
            // HA-3d: empty config baseline (a static / pre-membership cluster).
            voters: BTreeSet::new(),
            learners: BTreeSet::new(),
        });
        assert_round_trips(&RaftMsg::InstallSnapshot {
            term: 12,
            leader_id: NodeId(4),
            last_included_index: 9_001,
            last_included_term: 11,
            data: vec![0, 1, 2, 255, 254, 13, 10, 0, 42],
            // HA-3d: a populated config baseline (voters + a learner) round-trips too.
            voters: [NodeId(1), NodeId(4), NodeId(7)].into_iter().collect(),
            learners: [NodeId(9)].into_iter().collect(),
        });
        assert_round_trips(&RaftMsg::InstallSnapshot {
            term: u64::MAX,
            leader_id: NodeId(u64::MAX),
            last_included_index: u64::MAX,
            last_included_term: u64::MAX,
            data: vec![7; 128],
            voters: [NodeId(0), NodeId(u64::MAX)].into_iter().collect(),
            learners: BTreeSet::new(),
        });

        // HA-3c InstallSnapshotResp: the term PLUS the echoed last_included_index (Figure
        // 13), at the zero and large edges of both fields.
        assert_round_trips(&RaftMsg::InstallSnapshotResp {
            term: 0,
            last_included_index: 0,
        });
        assert_round_trips(&RaftMsg::InstallSnapshotResp {
            term: u64::MAX,
            last_included_index: u64::MAX,
        });
        assert_round_trips(&RaftMsg::InstallSnapshotResp {
            term: 7,
            last_included_index: 9_001,
        });
    }

    /// A log-entry vector exercising every [`EntryPayload`] shape, including every
    /// [`ConfigCmd`] variant inside a `Config` payload. Factored out of the round-trip
    /// test so each stays under the line cap and the payload taxonomy is named once.
    // A flat builder enumerating one entry per payload/ConfigCmd shape; it is intentionally long
    // (it names the whole taxonomy in one place) and grows by one block per new variant.
    #[allow(clippy::too_many_lines)]
    fn every_payload_kind_entries() -> Vec<LogEntry> {
        vec![
            LogEntry {
                term: 5,
                index: 5,
                payload: EntryPayload::Noop,
            },
            LogEntry {
                term: 5,
                index: 6,
                payload: EntryPayload::Bytes(b"opaque-client-command".to_vec()),
            },
            LogEntry {
                term: 5,
                index: 7,
                payload: EntryPayload::Config(ConfigCmd::AddNode {
                    id: "1111111111111111111111111111111111111111".to_owned(),
                    host: "10.0.0.7".to_owned(),
                    port: 6379,
                }),
            },
            LogEntry {
                term: 6,
                index: 8,
                payload: EntryPayload::Config(ConfigCmd::RemoveNode {
                    id: "2222222222222222222222222222222222222222".to_owned(),
                }),
            },
            LogEntry {
                term: 6,
                index: 9,
                payload: EntryPayload::Config(ConfigCmd::SetSlotOwner {
                    slot: 16_383,
                    node: "3333333333333333333333333333333333333333".to_owned(),
                }),
            },
            LogEntry {
                term: 7,
                index: 10,
                payload: EntryPayload::Config(ConfigCmd::AssignSlots {
                    node: "4444444444444444444444444444444444444444".to_owned(),
                    slots: vec![0, 1, 2, 100, 8191, 8192, 16_383],
                }),
            },
            LogEntry {
                term: 7,
                index: 11,
                payload: EntryPayload::Config(ConfigCmd::AssignSlots {
                    // An empty slot list is a valid (if degenerate) batch.
                    node: "5555555555555555555555555555555555555555".to_owned(),
                    slots: vec![],
                }),
            },
            LogEntry {
                term: 8,
                index: 12,
                payload: EntryPayload::Config(ConfigCmd::SetConfigEpoch(u64::MAX)),
            },
            LogEntry {
                term: 8,
                index: 13,
                payload: EntryPayload::Config(ConfigCmd::AssignReplica {
                    node: "6666666666666666666666666666666666666666".to_owned(),
                    slots: vec![0, 1, 2, 100, 8191, 8192, 16_383],
                }),
            },
            LogEntry {
                term: 8,
                index: 14,
                payload: EntryPayload::Config(ConfigCmd::AssignReplica {
                    // An empty replica slot list is a valid (if degenerate) batch.
                    node: "7777777777777777777777777777777777777777".to_owned(),
                    slots: vec![],
                }),
            },
            LogEntry {
                term: 9,
                index: 15,
                payload: EntryPayload::Config(ConfigCmd::PromoteReplica {
                    // HA-8 failover: slots-then-node wire shape must round-trip byte-for-byte.
                    slots: vec![0, 1, 2, 100, 8191, 8192, 16_383],
                    new_primary: "8888888888888888888888888888888888888888".to_owned(),
                }),
            },
            LogEntry {
                term: 9,
                index: 16,
                payload: EntryPayload::Config(ConfigCmd::PromoteReplica {
                    // An empty promotion slot list is a valid (if degenerate) batch.
                    slots: vec![],
                    new_primary: "9999999999999999999999999999999999999999".to_owned(),
                }),
            },
            LogEntry {
                term: 10,
                index: 17,
                payload: EntryPayload::Config(ConfigCmd::SetSlotMigrating {
                    // HA-6: slot-then-node wire shape must round-trip byte-for-byte (incl. the
                    // boundary slot 16383).
                    slot: 16_383,
                    dest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                }),
            },
            LogEntry {
                term: 10,
                index: 18,
                payload: EntryPayload::Config(ConfigCmd::SetSlotImporting {
                    // HA-6: slot-then-src-then-dest wire shape must round-trip byte-for-byte; the
                    // distinct src/dest ids prove both string fields decode in order.
                    slot: 0,
                    src: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                    dest: "cccccccccccccccccccccccccccccccccccccccc".to_owned(),
                }),
            },
            LogEntry {
                term: 10,
                index: 19,
                payload: EntryPayload::Config(ConfigCmd::SetSlotStable { slot: 8192 }),
            },
            LogEntry {
                term: 10,
                index: 20,
                payload: EntryPayload::Config(ConfigCmd::UnassignSlots {
                    // The inverse of AssignSlots: a length-prefixed slot list with NO node string
                    // must round-trip byte-for-byte (incl. the boundary slot 16383).
                    slots: vec![0, 1, 2, 100, 8191, 8192, 16_383],
                }),
            },
            LogEntry {
                term: 10,
                index: 21,
                payload: EntryPayload::Config(ConfigCmd::UnassignSlots {
                    // An empty UN-assign slot list is a valid (if degenerate) batch.
                    slots: vec![],
                }),
            },
            // HA-3d: a LogEntry of every MembershipChange shape inside a ConfigChange
            // payload, exercising the new wire discriminant + the one-NodeId tail.
            LogEntry {
                term: 11,
                index: 22,
                payload: EntryPayload::ConfigChange(MembershipChange::AddVoter(NodeId(5))),
            },
            LogEntry {
                term: 11,
                index: 23,
                payload: EntryPayload::ConfigChange(MembershipChange::RemoveVoter(NodeId(3))),
            },
            LogEntry {
                term: 12,
                index: 24,
                payload: EntryPayload::ConfigChange(MembershipChange::AddLearner(NodeId(8))),
            },
            LogEntry {
                term: 12,
                index: 25,
                payload: EntryPayload::ConfigChange(MembershipChange::PromoteLearner(NodeId(8))),
            },
        ]
    }

    /// Decode rejects malformed input rather than panicking or fabricating a message:
    /// an unknown discriminant, a truncated frame, and trailing garbage after a
    /// complete message all yield `None`.
    #[test]
    fn decode_rejects_malformed_input() {
        // Empty buffer.
        assert!(decode_raft_msg(&[]).is_none());
        // Unknown message discriminant.
        assert!(decode_raft_msg(&[0xFF]).is_none());
        // A RequestVoteResp truncated mid-term (needs 8 term bytes + 1 flag).
        assert!(decode_raft_msg(&[2, 1, 2, 3]).is_none());
        // A valid message with one extra trailing byte must be rejected.
        let mut bytes = encode_raft_msg(&RaftMsg::RequestVoteResp {
            term: 1,
            vote_granted: true,
        });
        bytes.push(0);
        assert!(decode_raft_msg(&bytes).is_none());
    }

    /// The inbound RESP command parser round-trips the bus's outbound encoding: an
    /// encoded RAFTMSG command parses back to the same (from, msg), with the exact
    /// byte length consumed and the empty / partial buffers handled as "need more".
    #[test]
    fn raftmsg_command_parses_what_the_bus_encodes() {
        let msg = RaftMsg::AppendEntries {
            term: 9,
            leader: NodeId(2),
            prev_log_index: 3,
            prev_log_term: 9,
            entries: vec![LogEntry {
                term: 9,
                index: 4,
                payload: EntryPayload::Bytes(b"x".to_vec()),
            }],
            leader_commit: 3,
        };
        let encoded = encode_raft_msg(&msg);
        // Build the RESP array exactly as the bus's encode_command would for
        // ["RAFTMSG", "2", <encoded>].
        let from = b"2";
        let mut frame = Vec::new();
        frame.extend_from_slice(b"*3\r\n");
        frame.extend_from_slice(format!("${}\r\n", RAFTMSG.len()).as_bytes());
        frame.extend_from_slice(RAFTMSG);
        frame.extend_from_slice(b"\r\n");
        frame.extend_from_slice(format!("${}\r\n", from.len()).as_bytes());
        frame.extend_from_slice(from);
        frame.extend_from_slice(b"\r\n");
        frame.extend_from_slice(format!("${}\r\n", encoded.len()).as_bytes());
        frame.extend_from_slice(&encoded);
        frame.extend_from_slice(b"\r\n");

        // A partial frame needs more bytes.
        assert_eq!(parse_raftmsg_command(&frame[..frame.len() - 3]), Ok(None));

        let (got_from, got_msg, consumed) = parse_raftmsg_command(&frame)
            .expect("well-formed frame")
            .expect("complete frame");
        assert_eq!(got_from, NodeId(2));
        assert_eq!(got_msg, msg);
        assert_eq!(consumed, frame.len());
    }

    // -- HA-prod-commit-ack: the adapter pending_commits resolution -----------
    //
    // These drive the adapter's PARK-then-RESOLVE logic directly (no run loop / real
    // election needed), proving the propose ack resolves on TRUE COMMIT, fails NotLeader
    // on overwrite / step-down, and never leaks. They exercise the engine through the same
    // entry points the run loop uses (on_local_propose / resolve_pending_commits), with a
    // throwaway tokio runtime to drive the (rarely-awaiting) resolve.

    use ironcache_raft::{ELECTION_TIMEOUT, MemStorage, RaftConfig, RaftNode, Role};
    use ironcache_runtime::TokioRuntime;

    /// Build an adapter node over a 1-voter cluster (so it can self-elect deterministically
    /// without peers), with a no-peer address map (no real I/O needed for the local-ack
    /// path). Returns the node and its handle.
    fn lone_voter_node() -> (
        RaftClusterBusNode<TokioRuntime, MemStorage, RecordingSm>,
        NodeHandle,
    ) {
        let voters: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
        let raft = RaftNode::with_state_machine(
            NodeId(1),
            voters,
            MemStorage::new(),
            RaftConfig::default(),
            RecordingSm::new(),
        );
        RaftClusterBusNode::new(raft, SystemEnv::new(), TokioRuntime::new(), BTreeMap::new())
    }

    /// Drive the engine to LEADER (a lone voter self-elects on its election timeout).
    fn drive_to_leader(node: &mut RaftClusterBusNode<TokioRuntime, MemStorage, RecordingSm>) {
        let mut effects = Effects::new();
        let now = node.env.now();
        let rng: &mut dyn RaftRng = node.env.rng();
        node.raft.on_timer(now, rng, ELECTION_TIMEOUT, &mut effects);
        assert!(node.raft.is_leader(), "the lone voter must self-elect");
    }

    /// A propose on a 1-voter leader commits within the step, so the ack resolves
    /// `Some(index)` (Committed) on the very next resolve pass -- the N=1 prompt path.
    #[test]
    fn local_propose_resolves_committed_on_commit_advance() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let (mut node, _handle) = lone_voter_node();
            drive_to_leader(&mut node);

            // Propose locally with an ack parked. on_local_propose appends + (for N=1)
            // commits within this step, recording committed_through.
            let (tx, rx) = oneshot::channel();
            let mut effects = Effects::new();
            node.on_local_propose(EntryPayload::Noop, Some(tx), &mut effects)
                .await;
            let committed_through = effects.committed_through;
            // The ack must be PARKED, not yet answered (resolution happens post-drain).
            assert!(
                !node.pending_commits.is_empty(),
                "the ack must be parked awaiting commit, not answered at append"
            );

            // Resolve: the entry is committed (N=1), so the parked ack fires Some(index).
            node.resolve_pending_commits(committed_through).await;
            let index = rx
                .await
                .expect("the parked ack must resolve")
                .expect("a committed entry resolves Some(index)");
            assert!(index >= 1, "the committed index is 1-based");
            assert!(
                node.pending_commits.is_empty(),
                "a resolved ack must be removed (no leak)"
            );
        });
    }

    /// A parked ack whose entry is OVERWRITTEN before committing (a different term now
    /// occupies its index, or the index no longer exists) resolves `None` (NotLeader),
    /// even while this node is still leader. We park an ack at an index the log does NOT
    /// hold at the parked term, so `entry_overwritten` is true and pass 2 fails it.
    #[test]
    fn parked_ack_for_overwritten_index_resolves_not_leader() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let (mut node, _handle) = lone_voter_node();
            drive_to_leader(&mut node);

            // Park an ack at index 999 with a term the log never held there: term_at(999)
            // reads 0 (past the log end) != 7, so the entry is "overwritten" (never landed
            // / truncated away). The node is STILL leader, so this isolates the overwrite
            // predicate from the step-down path.
            let (tx, rx) = oneshot::channel();
            node.pending_commits
                .insert(999, PendingCommit { term: 7, ack: tx });

            // No commit advanced (committed_through None); pass 2 must FAIL the overwritten
            // entry None.
            node.resolve_pending_commits(None).await;
            let outcome = rx.await.expect("the parked ack must resolve");
            assert_eq!(
                outcome, None,
                "an overwritten (term-mismatched) parked index resolves NotLeader"
            );
            assert!(
                node.pending_commits.is_empty(),
                "the failed ack must be removed (no leak)"
            );
        });
    }

    /// A parked ack on a node that has STEPPED DOWN (no longer leader) resolves `None`
    /// (NotLeader): a deposed leader can no longer drive its uncommitted entry to commit,
    /// so the idempotent caller must retry. We park an ack, force the engine to Follower,
    /// then resolve: pass 2's step-down branch fails every pending ack.
    #[test]
    fn parked_ack_on_step_down_resolves_not_leader() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let (mut node, _handle) = lone_voter_node();
            drive_to_leader(&mut node);

            // Append a real entry so its index/term are genuine, parking its ack. For a
            // 1-voter leader it commits within the step; resolve + drain that first ack
            // (it resolves committed) so the maps are clean before we isolate step-down.
            let (tx, rx) = oneshot::channel();
            let mut effects = Effects::new();
            node.on_local_propose(EntryPayload::Bytes(vec![1, 2, 3]), Some(tx), &mut effects)
                .await;
            node.resolve_pending_commits(effects.committed_through)
                .await;
            rx.await
                .expect("the first ack resolves")
                .expect("the 1-voter leader commits the first entry");

            // Park a fresh ack at the REAL last log index with its REAL term, so the entry
            // is genuinely present (entry_overwritten is FALSE for it) -- this isolates the
            // STEP-DOWN branch as the sole reason the ack fails.
            let (tx2, rx2) = oneshot::channel();
            let idx = node.raft.storage().last_log_index();
            let term = node.raft.storage().term_at(idx);
            node.pending_commits
                .insert(idx, PendingCommit { term, ack: tx2 });
            // Force a step-down by observing a higher term via an AppendEntries from a peer.
            let mut effects = Effects::new();
            let now = node.env.now();
            let rng: &mut dyn RaftRng = node.env.rng();
            node.raft.on_message(
                now,
                rng,
                NodeId(2),
                RaftMsg::AppendEntries {
                    term: node.raft.current_term() + 10,
                    leader: NodeId(2),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: 0,
                },
                &mut effects,
            );
            assert_eq!(
                node.raft.role(),
                Role::Follower,
                "a higher term steps us down"
            );

            node.resolve_pending_commits(effects.committed_through)
                .await;
            let outcome = rx2.await.expect("the parked ack must resolve");
            assert_eq!(
                outcome, None,
                "a parked ack on a stepped-down node resolves NotLeader"
            );
            assert!(
                node.pending_commits.is_empty(),
                "every pending ack is failed on step-down (no leak)"
            );
        });
    }
}
