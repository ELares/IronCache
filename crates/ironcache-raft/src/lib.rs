// SPDX-License-Identifier: MIT OR Apache-2.0
//! Hand-rolled, Env-respecting Raft control plane (ADR-0027, CONTROL_PLANE.md #73).
//!
//! This crate is a PURE step engine for the Raft consensus algorithm (Ongaro &
//! Ousterhout, "In Search of an Understandable Consensus Algorithm", sections
//! 5.1 to 5.4). It performs no I/O, owns no clock, owns no RNG, and knows nothing
//! about transport. Every effect a step intends (outbound messages, timer arm /
//! cancel, persistence) is returned through the [`Effects`] set and the
//! [`RaftStorage`] seam; every read of virtual time arrives as a [`Monotonic`]
//! argument and every random draw goes through the narrow [`RaftRng`] seam. That
//! is what lets the SAME compiled engine be driven byte-identically in the HA-2
//! deterministic-simulation harness (`ironcache-sim`) and, later (HA-4), wired to
//! the real clusterbus and `ironcache-env`'s `Clock` / `Rng`; only the adapter
//! differs (ADR-0027).
//!
//! ## Scope: sub-slices 3a (election) + 3b (log replication + commit)
//!
//! Sub-slice 3a implemented LEADER ELECTION and TERM SAFETY, the foundation on
//! which the rest of Raft is built. A bug there is split-brain, so the correctness
//! bar is the Election Safety property (at most one leader per term), proven by the
//! DST scenarios in the test module across a seed sweep.
//!
//! Sub-slice 3b (this slice) adds LOG REPLICATION and COMMIT (sections 5.3 and
//! 5.4.2). [`RaftMsg::AppendEntries`] is now the real replication RPC: a leader
//! ships log entries to followers with the `(prev_log_index, prev_log_term)`
//! consistency check (Figure 2, AppendEntries rules 1-5), followers truncate
//! conflicts and append, and the leader advances its `commitIndex` under the
//! section-5.4.2 "commit-only-current-term" rule (THE Figure-8 safety rule:
//! entries from a prior term are never committed by counting replicas alone; they
//! commit transitively only once a current-term entry above them commits). Clients
//! propose entries through [`RaftNode::propose`]. A committed entry is "applied" to
//! a SINK in 3b (a `last_applied` watermark plus an applied counter); the real
//! state-machine apply that drives the SlotMap is HA-3e.
//!
//! Sub-slice 3d adds RAFT CLUSTER-MEMBERSHIP CHANGES (section 6): single-server
//! voter add / remove and non-voting LEARNERS with a catch-up-then-promote phase.
//! The configuration (voter set + learner set) is DERIVED FROM THE LOG and a node
//! adopts a new configuration the moment it APPENDS an [`EntryPayload::ConfigChange`]
//! entry (not when it commits) -- the section-6 rule that keeps single-server changes
//! safe because the old and new majorities always overlap. Only single-server changes
//! are implemented (NOT joint consensus), since those are provably safe without a joint
//! configuration. A cluster that never issues a `ConfigChange` is byte-identical to the
//! pre-3d engine (the variant is inert unless used). See [`MembershipChange`].
//!
//! What 3b still does NOT do: payloads remain opaque ([`EntryPayload::Noop`] plus
//! a minimal test-only [`EntryPayload::Bytes`]); there is no snapshotting / log
//! compaction and no real state machine. The conflict-index
//! fast-backup optimization (the dissertation's accelerated `nextIndex` rewind) is
//! deliberately NOT implemented; the receiver returns a simple last-index hint and
//! the leader decrements `nextIndex` by one per failed round, which is correct (if
//! slower to converge) and keeps the safety reasoning trivial. Those land in later
//! sub-slices.
//!
//! ## The step surface
//!
//! A node reacts to exactly two inputs, mirroring the [`ironcache_sim::SimNode`]
//! shape it is tested against:
//!
//! - [`RaftNode::on_message`] - an inbound [`RaftMsg`] from a peer.
//! - [`RaftNode::on_timer`] - an armed timer ([`ELECTION_TIMEOUT`] or
//!   [`HEARTBEAT`]) expiring.
//!
//! plus [`RaftNode::start`], called once when the node is first driven (the sim
//! adapter invokes it on the node's first callback) to arm the initial election
//! timer. Each entry point takes the current [`Monotonic`] time, a
//! [`RaftRng`], the input, and an `&mut Effects` to record sends and timer ops;
//! persistent state (current term, vote, log) is written through [`RaftStorage`]
//! DURING the step. The caller (the sim adapter, or production transport) drains
//! [`Effects`] after the call returns.
//!
//! ## Anchoring
//!
//! The term and vote rules are implemented verbatim from the paper's Figure 2
//! ("RequestVote RPC", "AppendEntries RPC", and "Rules for Servers") and section
//! 5.4.1 (the up-to-date-log comparison). The exact rules are restated at each
//! handler.

#![forbid(unsafe_code)]

use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use ironcache_env::Monotonic;

// Split-out modules (#625): pure relocations of what were inline definitions. Each is re-exported
// from this crate root so every existing `ironcache_raft::X` and in-crate path resolves unchanged.
mod raft_config;
mod raft_effects;
mod raft_sm;
mod raft_storage;
mod raft_types;
pub use raft_config::*;
pub use raft_effects::*;
pub use raft_sm::*;
pub use raft_storage::*;
pub use raft_types::*;

// ---------------------------------------------------------------------------
// The node.
// ---------------------------------------------------------------------------

/// The decoded fields of a [`RaftMsg::InstallSnapshot`], bundled so
/// [`RaftNode::on_install_snapshot`] stays under the argument cap (the message gained
/// HA-3d config-baseline fields and the PROD-9 `offset` / `done` chunk fields).
/// Constructed in the `on_message` dispatch and moved in.
struct InstallSnapshotArgs {
    /// The leader's term.
    term: u64,
    /// The leader id (recorded / redirected to, mirrors AppendEntries).
    leader_id: NodeId,
    /// The snapshot's last included log index.
    last_included_index: u64,
    /// The term of the snapshot's last included entry.
    last_included_term: u64,
    /// PROD-9: the byte offset of this chunk (`0` is the first chunk; restarts the buffer).
    offset: u64,
    /// The snapshot bytes for THIS chunk (`[offset, offset + data.len())`).
    data: Vec<u8>,
    /// PROD-9: `true` on the last chunk (install on this chunk only).
    done: bool,
    /// HA-3d: the config baseline voter set as of `last_included_index`.
    voters: BTreeSet<NodeId>,
    /// HA-3d: the config baseline learner set as of `last_included_index`.
    learners: BTreeSet<NodeId>,
}

/// PROD-9 chunked-InstallSnapshot RECEIVE BUFFER: a follower's in-progress reassembly of a
/// leader's snapshot that is arriving in [`RaftMsg::InstallSnapshot`] chunks (Raft Figure
/// 13). Held only WHILE a chunked transfer is in flight (`None` otherwise), it accumulates
/// the chunk bytes contiguously from offset 0 and is consumed (validated + installed) only
/// when the FINAL (`done`) chunk arrives. A fresh first chunk (`offset == 0`) REPLACES any
/// prior buffer -- so a leader change, a restarted transfer, or a follower that fell out of
/// sync simply starts over -- which is why a partial transfer can never be installed: the
/// install runs exclusively on `done` after a contiguous `0..len` accumulation.
///
/// The buffer is keyed by the snapshot meta (`last_included_index` / `last_included_term`)
/// the first chunk carried; a later chunk whose meta disagrees is treated as a NEW transfer
/// and (because the offset will not match) is rejected, forcing the leader to restart from
/// offset 0. This keeps the follower from splicing two different snapshots' bytes together.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotRx {
    /// The transferring snapshot's last included index (from the first chunk).
    last_included_index: u64,
    /// The transferring snapshot's last included term (from the first chunk).
    last_included_term: u64,
    /// The config baseline voter set the first chunk carried (HA-3d), adopted on install.
    voters: BTreeSet<NodeId>,
    /// The config baseline learner set the first chunk carried (HA-3d), adopted on install.
    learners: BTreeSet<NodeId>,
    /// The contiguously accumulated snapshot bytes so far (`data.len()` is the next
    /// expected chunk offset).
    data: Vec<u8>,
}

/// A single Raft node: the pure step engine.
///
/// Holds the node's identity, the CONFIGURATION (the voter set + the learner set,
/// DERIVED FROM THE LOG since HA-3d, section 6), the volatile role and the in-flight
/// vote tally, the timing config, and the persistent [`RaftStorage`]. It is driven by
/// [`RaftNode::start`] once, then [`RaftNode::on_message`] / [`RaftNode::on_timer`] per
/// event. It reads time only via the `now` argument and randomness only via the
/// [`RaftRng`] argument; it never blocks and performs no I/O.
#[derive(Debug)]
pub struct RaftNode<S: RaftStorage, M: StateMachine = CountingSm> {
    /// This node's id.
    id: NodeId,
    /// The current set of VOTING members (HA-3d). Counted in every majority (election
    /// and commit). Seeded from the constructor's argument, then RECOMPUTED from the log
    /// whenever the log changes: a node ADOPTS a new configuration the moment it APPENDS
    /// an [`EntryPayload::ConfigChange`] entry (Raft section 6, the append-time rule that
    /// keeps single-server changes safe). Never includes a node that is currently a
    /// learner. May or may not include `id` (a removed leader is no longer a voter).
    voters: BTreeSet<NodeId>,
    /// The current set of LEARNERS (HA-3d, non-voting members in their catch-up phase).
    /// A learner is replicated to (it receives AppendEntries / InstallSnapshot) but is
    /// NEVER counted in any majority. Like [`voters`](RaftNode::voters), it is derived
    /// from the log and adopted on append. A node is in AT MOST one of `voters` /
    /// `learners` (PromoteLearner moves it from the latter to the former). Empty unless a
    /// membership change has introduced a learner, so the default path is byte-unchanged.
    learners: BTreeSet<NodeId>,
    /// The CONFIGURATION BASELINE (voters, learners) as of the last snapshot point
    /// (HA-3d). The live config is `baseline` plus every `ConfigChange` entry surviving
    /// in the log above the snapshot. Kept so [`recompute_config_from_log`] can rebuild
    /// the config after a truncation (which may remove `ConfigChange` entries) without
    /// re-reading the compacted-away prefix. Seeded from the constructor's voter set
    /// (learners empty), or from the persisted baseline on a restart-from-snapshot.
    ///
    /// [`recompute_config_from_log`]: RaftNode::recompute_config_from_log
    config_baseline: (BTreeSet<NodeId>, BTreeSet<NodeId>),
    /// The current role (volatile; rebuilt from persistence + elections on boot).
    role: Role,
    /// Votes received in the current term while a `Candidate` (includes self).
    /// Empty unless `role == Candidate`.
    votes: BTreeSet<NodeId>,
    /// PRE-VOTE round state (Ongaro section 9.6). When a `Follower`'s election timer fires
    /// and [`RaftConfig::pre_vote`] is on, the node enters a pre-candidate state WITHOUT
    /// changing its `role` (it stays a `Follower` for every other purpose, so Election
    /// Safety reasoning and the public `Role` enum are untouched): it broadcasts
    /// [`RaftMsg::PreVote`] for the HYPOTHETICAL term `current_term + 1` and tallies grants
    /// here (its own id is inserted on entry, a quorum-of-1 single node converting at once).
    /// `Some(set)` means a pre-vote round is in flight at term `pre_vote_term`; `None` means
    /// no round is active. Cleared on conversion to a real candidate, on a real
    /// AppendEntries / step-down (a live leader aborts the round), and on a higher-term
    /// observation. Pre-vote NEVER persists term or vote on any node, so it perturbs no
    /// persistent state and the round is purely volatile.
    pre_votes: Option<BTreeSet<NodeId>>,
    /// The HYPOTHETICAL term the in-flight pre-vote round targets (the node's
    /// `current_term + 1` at the moment the round started). A [`RaftMsg::PreVoteResp`] is
    /// tallied only if its echoed `term` equals this, so a stale reply (from a prior round,
    /// or after the term advanced) is discarded. Meaningless when `pre_votes` is `None`.
    pre_vote_term: u64,
    /// CONSECUTIVE *engaged* pre-vote rounds that ELAPSED without reaching a pre-vote quorum
    /// (the etcd #8525 mixed-version fallback counter, volatile). Incremented when an election
    /// timeout fires on a node that was already mid pre-vote round (a pre-candidate that never
    /// reached quorum) AND that round received at least one `PreVoteResp` from a reachable peer
    /// (see [`pre_vote_round_responded`](RaftNode::pre_vote_round_responded)); once it reaches
    /// [`PRE_VOTE_FALLBACK_ROUNDS`] the NEXT timeout falls back to a real term-bumping election
    /// (`start_real_election`) instead of another pre-vote round, then resets to 0 (resuming
    /// pre-vote mode). RESET to 0 on ANY progress -- hearing a valid current-term leader,
    /// winning a pre-vote, adopting a higher term, or becoming leader -- so a HEALTHY
    /// all-pre-vote cluster always resets before the threshold and never falls back
    /// (steady-state is byte-identical). The fallback fires ONLY when the node IS in contact
    /// with peers but their pre-votes cannot form a grant-quorum (the mixed-version migration
    /// deadlock: pre-vote-aware peers reply / reject while a quorum of grants stays
    /// unreachable). A FULLY ISOLATED node receives no `PreVoteResp` at all, so this counter
    /// never advances and the node never inflates its term -- preserving the disruption-free
    /// property (a rejoining isolated node must not depose the standing leader, Ongaro section
    /// 9.6). Inert when [`RaftConfig::pre_vote`] is off (the path that touches it is not taken).
    failed_pre_vote_rounds: u32,
    /// Whether the CURRENT pre-vote round has received at least one `PreVoteResp` (grant OR
    /// rejection) from a reachable peer (volatile, the etcd #8525 engagement gate). Set false
    /// when a pre-vote round starts; set true the moment any in-round `PreVoteResp` arrives.
    /// The fallback counter advances only when a round elapses WITH this set -- a fully
    /// partitioned node (which receives nothing) never counts a failed round and so never
    /// term-bumps via the fallback, which is what keeps an isolated rejoining node from
    /// disrupting the standing leader. Meaningful only while a round is in flight.
    pre_vote_round_responded: bool,
    /// The instant this node last heard from a VALID CURRENT-TERM LEADER (an accepted
    /// AppendEntries or InstallSnapshot), if any (LEADER-STICKINESS + the pre-vote-grant
    /// gate, Ongaro section 9.6). A follower REFUSES a (pre-)vote when this is within the
    /// MINIMUM election timeout of `now`: a fresh leader means no election is warranted, so
    /// a single disruptive / flapping server cannot force the cluster to elect. Set on every
    /// accepted current-term AppendEntries / InstallSnapshot; left as `None` until the node
    /// first hears from a leader (so a brand-new node never spuriously rejects, which would
    /// stall the very first election). Read-only outside the (pre-)vote grant path.
    last_leader_contact: Option<Monotonic>,
    /// LEADER-side per-voter last SUCCESSFUL-contact instant (CHECK-QUORUM, Ongaro section
    /// 6.2 / 9.6). Each successful AppendEntries response (a live, in-sync follower) stamps
    /// `now` here for that peer; the leader's own id is implicitly always "in contact". On
    /// each heartbeat the leader counts how many voters it has heard from within an election
    /// timeout, and if that is NOT a quorum it STEPS DOWN (it has lost the majority and must
    /// not keep acting as leader). Only populated while `role == Leader`; cleared on
    /// step-down and re-seeded on election. Empty / inert when [`RaftConfig::check_quorum`]
    /// is off.
    quorum_contact: BTreeMap<NodeId, Monotonic>,
    /// The leader THIS node currently recognizes for its term, if any (HA-9
    /// leader-forwarding). This is a PASSIVE RECORD of information the engine already
    /// observes - the AppendEntries sender it accepts, the higher-term step-down
    /// sender, or self on winning - and is NEVER read by any vote / commit / append
    /// decision or by any [`Effects`] computation. It exists so a follower can be told
    /// (through the production adapter's status watch) WHICH peer to forward a proposal
    /// to. Because it feeds no decision and changes no effect, every DST scenario
    /// replays byte-identically with it present. Set when a current-term AppendEntries
    /// is accepted, set to self on becoming leader, cleared on starting an election
    /// (Candidate) and on the higher-term step-down (the new leader is not yet known).
    leader_id: Option<NodeId>,
    /// Timing parameters.
    config: RaftConfig,
    /// Persistent state (term, vote, log).
    storage: S,
    /// Volatile state on ALL servers (Figure 2): the highest log index known to be
    /// committed. Monotonic; initialized to 0. A follower advances it from
    /// `leader_commit`; the leader advances it under the section-5.4.2 rule.
    commit_index: u64,
    /// Volatile state on ALL servers (Figure 2): the highest log index applied to
    /// the state machine. `last_applied <= commit_index` always; the apply pipeline
    /// advances it toward `commit_index` (in 3b, apply is a SINK).
    last_applied: u64,
    /// Volatile LEADER state (Figure 2): per-peer index of the next log entry to
    /// send to that peer (initialized to `last_log_index + 1` on election). Only
    /// populated while `role == Leader`; cleared on step-down.
    next_index: BTreeMap<NodeId, u64>,
    /// Volatile LEADER state (Figure 2): per-peer highest log index known to be
    /// replicated on that peer (initialized to 0 on election). Only populated while
    /// `role == Leader`; cleared on step-down.
    match_index: BTreeMap<NodeId, u64>,
    /// The apply watermark counter: how many entries this node has applied. In 3b
    /// this WAS the whole apply (a sink); in 3e it remains as an apply-progress
    /// witness (the state-machine-safety checker proves the apply hook ran, not just
    /// that `last_applied` moved) and is kept in lockstep with the real
    /// [`StateMachine`] apply below. Exposed via [`RaftNode::applied_count`].
    applied_count: u64,
    /// PROD-9 chunked InstallSnapshot, LEADER state: the per-peer byte OFFSET of the NEXT
    /// snapshot chunk to send that peer (Raft Figure 13's per-follower snapshot progress).
    /// Set to `0` when the leader first decides to snapshot a peer (a fresh transfer starts
    /// at offset 0), advanced on each non-final ack to the offset the follower reported it
    /// next expects, and REMOVED on the install ack (transfer complete) or whenever a peer
    /// falls back to AppendEntries. Like [`next_index`](RaftNode::next_index) /
    /// [`match_index`](RaftNode::match_index) it is leader-only volatile state, cleared on
    /// step-down and reinitialized on election; a follower restart / term change makes the
    /// follower reject mismatched offsets and reply `next_offset == 0`, which restarts the
    /// transfer here. Empty unless a chunked install is in flight, so the non-snapshot path
    /// is byte-unchanged.
    snapshot_next_offset: BTreeMap<NodeId, u64>,
    /// PROD-9 chunked InstallSnapshot, FOLLOWER state: the in-progress receive buffer for a
    /// snapshot arriving in chunks (Raft Figure 13). `None` when no chunked transfer is in
    /// flight; `Some` while one is being reassembled. Replaced by a fresh first chunk
    /// (`offset == 0`) and consumed (validated + installed) only on the final (`done`)
    /// chunk, so a partial transfer is NEVER installed. Volatile: a crash mid-transfer
    /// drops it (the leader restarts the transfer from offset 0). Empty unless a chunked
    /// install is in flight, so the non-snapshot path is byte-unchanged.
    snapshot_rx: Option<SnapshotRx>,
    /// The replicated state machine (3e). Each committed entry is handed to
    /// [`StateMachine::apply`] exactly once, in index order, by
    /// [`RaftNode::apply_committed`]. With the default `M = CountingSm` this is the
    /// 3b sink; with the config state machine it drives the cluster
    /// `SlotMap`.
    sm: M,
}

impl<S: RaftStorage> RaftNode<S, CountingSm> {
    /// Construct a node `id` in a cluster of `voters` (must include `id`), backed by
    /// `storage` and timed by `config`, with the DEFAULT [`CountingSm`] state
    /// machine (the 3b apply-sink behavior). This is the constructor the election /
    /// log-replication tests use unchanged; callers that need a real state machine
    /// use [`RaftNode::with_state_machine`].
    ///
    /// The node starts as a `Follower`; call [`RaftNode::start`] to arm its first
    /// election timer.
    #[must_use]
    pub fn new(id: NodeId, voters: BTreeSet<NodeId>, storage: S, config: RaftConfig) -> Self {
        Self::with_state_machine(id, voters, storage, config, CountingSm::new())
    }
}

impl<S: RaftStorage, M: StateMachine> RaftNode<S, M> {
    /// Construct a node with an EXPLICIT state machine `sm` (3e). Identical to
    /// [`RaftNode::new`] except the caller supplies the `M: StateMachine` the apply
    /// pipeline drives (e.g. the config state machine over a
    /// `SlotMap`). The node starts as a `Follower`.
    #[must_use]
    pub fn with_state_machine(
        id: NodeId,
        voters: BTreeSet<NodeId>,
        storage: S,
        config: RaftConfig,
        mut sm: M,
    ) -> Self {
        // RECOVER FROM A PERSISTED SNAPSHOT (Raft section 7): if the storage holds a
        // snapshot, the state machine's state at `last_included_index` is in it, and the
        // surviving log is only the tail above it. Restore the machine and set the
        // commit / apply watermarks to the snapshot's index so the apply pipeline
        // replays ONLY the post-snapshot tail (never the compacted-away prefix). This is
        // the restart half of compaction: a node that compacted before crashing comes
        // back with its applied state intact without replaying the whole log. With no
        // snapshot (the default / pre-3c path) the watermarks start at 0, exactly as
        // before, so this is inert unless compaction was used.
        let (commit_index, last_applied) = match storage.load_snapshot() {
            Some((meta, data)) => {
                sm.restore(&data);
                (meta.last_included_index, meta.last_included_index)
            }
            None => (0, 0),
        };
        // HA-3d configuration recovery. The CONFIG BASELINE is what the surviving log's
        // `ConfigChange` entries are replayed on top of: a persisted baseline (saved at
        // the last snapshot, the config the compacted-away `ConfigChange` prefix
        // produced) when one exists, else the constructor's voter set with no learners (a
        // fresh node, or a node whose whole `ConfigChange` history is still in the log).
        // Consume the `voters` argument here (it is the fallback baseline when nothing was
        // persisted), so it is not needlessly cloned.
        let config_baseline = storage
            .load_config_baseline()
            .unwrap_or((voters, BTreeSet::new()));
        let mut node = RaftNode {
            id,
            voters: config_baseline.0.clone(),
            learners: config_baseline.1.clone(),
            config_baseline,
            role: Role::Follower,
            votes: BTreeSet::new(),
            pre_votes: None,
            pre_vote_term: 0,
            failed_pre_vote_rounds: 0,
            pre_vote_round_responded: false,
            last_leader_contact: None,
            quorum_contact: BTreeMap::new(),
            leader_id: None,
            config,
            storage,
            commit_index,
            last_applied,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            // PROD-9: no chunked snapshot transfer is in flight on a fresh / restarted node.
            // Leader per-peer offsets are seeded when a snapshot send first starts; the
            // follower buffer is filled by the first chunk. Both empty here.
            snapshot_next_offset: BTreeMap::new(),
            snapshot_rx: None,
            // The apply witness counts entries applied THIS process; a restore from a
            // snapshot did not stream entries through `apply`, so it starts at 0 (it is
            // a per-run hook-ran witness, not a persisted count).
            applied_count: 0,
            sm,
        };
        // Adopt the configuration the SURVIVING LOG implies: the baseline plus every
        // `ConfigChange` entry still present (Raft section 6, append-time adoption). With
        // no `ConfigChange` entries this is exactly `config_baseline`, so the default /
        // static-membership path keeps `voters` == the constructor argument byte-for-byte.
        node.recompute_config_from_log();
        node
    }

    /// Arm the initial election timer. Call exactly once, right after construction
    /// (the sim adapter does so immediately after `add_node`). Without this a fresh
    /// follower would never time out and the cluster would never elect a leader.
    pub fn start(&mut self, _now: Monotonic, rng: &mut dyn RaftRng, out: &mut Effects) {
        self.arm_election_timer(rng, out);
    }

    // -- read accessors -----------------------------------------------------

    /// This node's current role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    /// This node's persisted current term.
    #[must_use]
    pub fn current_term(&self) -> u64 {
        self.storage.current_term()
    }

    /// Whether this node currently believes it is leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// The leader this node currently recognizes for its term, if any (HA-9
    /// leader-forwarding). A PASSIVE record (see the `leader_id` field): it never
    /// influences a decision, so reading it cannot perturb consensus. A leader returns
    /// `Some(self)`; a follower returns the current-term leader it last accepted an
    /// AppendEntries from; a candidate (or a node that has just stepped down to a
    /// higher term whose new leader is not yet known) returns `None`. The production
    /// adapter surfaces this through its status watch so a follower can forward a
    /// proposal to the right peer.
    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// This node's id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Borrow the storage (for test inspection).
    #[must_use]
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// The highest log index known to be committed (volatile, monotonic; section
    /// 5.3 / Figure 2 `commitIndex`). A committed entry is durable: it is present
    /// on a majority and will never be overwritten (State Machine Safety).
    #[must_use]
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// The highest log index applied to the state machine (volatile; Figure 2
    /// `lastApplied`). Always `<= commit_index`. In 3b apply is a sink, so this is
    /// the watermark the apply pipeline has advanced to.
    #[must_use]
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    /// How many entries this node has applied. Equals `last_applied` in steady
    /// state; exposed separately so a test can prove the apply hook actually ran
    /// (not just that the watermark moved).
    #[must_use]
    pub fn applied_count(&self) -> u64 {
        self.applied_count
    }

    /// Borrow the state machine (3e), for test inspection of the applied config
    /// (e.g. projecting the `SlotMap` the config state
    /// machine has converged to).
    #[must_use]
    pub fn state_machine(&self) -> &M {
        &self.sm
    }

    // -- step entry points --------------------------------------------------

    /// Handle an inbound message `from` a peer at virtual time `now`.
    ///
    /// Dispatches on the message variant after applying the GLOBAL term rule
    /// ("All Servers": any RPC whose term exceeds ours steps us down and adopts
    /// it; see [`RaftNode::observe_term`]). Records sends / timer ops on `out` and
    /// writes persistence through storage.
    // Takes `msg` BY VALUE deliberately: this is the engine's step entry point and
    // transport hands ownership of the decoded message in. The 3a variants are all
    // small (scalars plus an always-empty `entries`), but later sub-slices carry
    // real `entries` payloads that the engine moves into the log, so the by-value
    // signature is the stable one. Matching by value here, not by reference.
    #[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
    pub fn on_message(
        &mut self,
        now: Monotonic,
        rng: &mut dyn RaftRng,
        from: NodeId,
        msg: RaftMsg,
        out: &mut Effects,
    ) {
        match msg {
            RaftMsg::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => {
                self.on_request_vote(
                    now,
                    rng,
                    term,
                    candidate,
                    last_log_index,
                    last_log_term,
                    out,
                );
            }
            RaftMsg::RequestVoteResp { term, vote_granted } => {
                self.on_request_vote_resp(now, rng, from, term, vote_granted, out);
            }
            RaftMsg::PreVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => self.on_pre_vote(now, term, candidate, last_log_index, last_log_term, out),
            RaftMsg::PreVoteResp { term, vote_granted } => {
                self.on_pre_vote_resp(now, from, term, vote_granted, out);
            }
            RaftMsg::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.on_append_entries(
                now,
                rng,
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                out,
            ),
            RaftMsg::AppendEntriesResp {
                term,
                success,
                match_index,
            } => {
                self.on_append_entries_resp(now, rng, from, term, success, match_index, out);
            }
            RaftMsg::Propose { payload } => {
                // A client proposal (not a peer RPC); forward to the real entry
                // point. The returned index is for a direct caller; here we ignore it.
                let _ = self.propose(payload, now, rng, out);
            }
            RaftMsg::ForwardPropose { .. } | RaftMsg::ForwardProposeResult { .. } => {
                // HA-9 transport-level forwarding (see the variant docs): the production
                // adapter intercepts these BEFORE the engine and never delivers them
                // here. If one ever reaches the pure engine it is an inert no-op - the
                // engine takes NO consensus action on it, so no decision or Effect can
                // depend on the forwarding path. This keeps the DST trace unchanged.
            }
            RaftMsg::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                offset,
                data,
                done,
                voters,
                learners,
            } => self.on_install_snapshot(
                now,
                rng,
                InstallSnapshotArgs {
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    offset,
                    data,
                    done,
                    voters,
                    learners,
                },
                out,
            ),
            RaftMsg::InstallSnapshotResp {
                term,
                last_included_index,
                installed,
                next_offset,
            } => {
                self.on_install_snapshot_resp(
                    rng,
                    from,
                    term,
                    last_included_index,
                    installed,
                    next_offset,
                    out,
                );
            }
        }
    }

    /// Handle a timer `token` ([`ELECTION_TIMEOUT`] or [`HEARTBEAT`]) firing at
    /// virtual time `now`.
    pub fn on_timer(
        &mut self,
        now: Monotonic,
        rng: &mut dyn RaftRng,
        token: u64,
        out: &mut Effects,
    ) {
        match token {
            ELECTION_TIMEOUT => self.on_election_timeout(now, rng, out),
            HEARTBEAT => self.on_heartbeat_timer(now, rng, out),
            _ => {}
        }
    }

    // -- handlers -----------------------------------------------------------

    /// ELECTION TIMEOUT (Figure 2, "Candidates": on conversion to candidate, start
    /// election). Fires on a `Follower` or `Candidate`. A `Leader` ignores it (it
    /// has no election timer armed; this guard is belt-and-suspenders against a
    /// stale event).
    ///
    /// With [`RaftConfig::pre_vote`] ON (the default, Ongaro section 9.6) the node first
    /// runs a PRE-VOTE round (`start_pre_vote`): it polls peers for a HYPOTHETICAL vote at
    /// `current_term + 1` WITHOUT incrementing any term, and only converts to a real
    /// candidate once a quorum grants the pre-vote. This stops a partitioned / rejoining
    /// node from disrupting a stable leader by inflating the term. With pre-vote OFF (or a
    /// trivial single-voter cluster, where the pre-vote quorum is self and passes at once)
    /// it starts a real election immediately (`start_real_election`): the pre-refinement
    /// behaviour. Either way the election timer is re-armed with fresh jitter.
    fn on_election_timeout(&mut self, now: Monotonic, rng: &mut dyn RaftRng, out: &mut Effects) {
        if self.role == Role::Leader {
            return;
        }
        // HA-3d: a node that is NOT a voter in its own (log-derived) configuration must not
        // start an election. A LEARNER is non-voting (it replicates but never campaigns,
        // Raft section 6) and a node already REMOVED from the cluster (or not yet added as
        // a voter) likewise cannot win and would only churn the term. We RE-ARM the timer
        // so it keeps a timer live (a learner being promoted, or a node about to be added,
        // becomes a voter the moment its log gains the AddVoter/PromoteLearner entry, after
        // which a later timeout campaigns normally). With a static voter set every node is
        // a voter, so this guard never fires and the default election path is byte-identical.
        if !self.voters.contains(&self.id) {
            self.arm_election_timer(rng, out);
            return;
        }

        // PRE-VOTE (Ongaro section 9.6): poll for a hypothetical vote BEFORE bumping the
        // term. A single-voter cluster's pre-vote quorum is self, which `start_pre_vote`
        // satisfies instantly and so converts straight to a real election (the trivial
        // self-elect is preserved). With pre-vote OFF we campaign immediately as before.
        if self.config.pre_vote {
            // MIXED-VERSION FALLBACK COUNTING (etcd #8525): if we time out while STILL a
            // pre-candidate (`pre_votes` is `Some` -- a prior round is in flight and never
            // reached quorum) AND that round was ENGAGED (it received at least one PreVoteResp
            // from a reachable peer), count it as a failed round. A round that HAD reached
            // quorum would have cleared `pre_votes` (via `maybe_promote_pre_candidate`) and
            // converted to a real candidate, so this never counts a successful round. The
            // engagement gate is the crucial disruption-free guard: a FULLY ISOLATED node gets
            // no replies, so `pre_vote_round_responded` stays false, the counter never advances,
            // and the node never inflates its term -- preserving "a rejoining isolated node does
            // not depose the standing leader". The fallback fires ONLY for the mixed-version
            // deadlock (peers reachable and answering, but a grant-quorum unreachable).
            if self.pre_votes.is_some() && self.pre_vote_round_responded {
                self.failed_pre_vote_rounds = self.failed_pre_vote_rounds.saturating_add(1);
            }
            // FALLBACK: after PRE_VOTE_FALLBACK_ROUNDS consecutive ungranted rounds, campaign
            // for REAL this once (a single term bump, the one-vote-per-term path) instead of
            // pre-voting forever, then reset to 0 to resume pre-vote mode (it only re-falls-back
            // after another threshold of failed rounds). This closes the rolling-upgrade /
            // unreachable-quorum lockout: a subset whose pre-votes can never be granted still
            // makes BOUNDED-rate progress rather than never campaigning. A partitioned node that
            // falls back still cannot WIN, so election safety is untouched; it just term-bumps
            // slowly (once per threshold of rounds) instead of every timeout (pre-vote off) or
            // never (no fallback). A healthy cluster resets the counter before reaching the
            // threshold (see the reset points), so this branch is never taken in steady state.
            if self.failed_pre_vote_rounds >= PRE_VOTE_FALLBACK_ROUNDS {
                self.failed_pre_vote_rounds = 0;
                self.pre_votes = None;
                self.start_real_election(now, out);
                self.arm_election_timer(rng, out);
            } else {
                self.start_pre_vote(now, rng, out);
            }
        } else {
            self.start_real_election(now, out);
            self.arm_election_timer(rng, out);
        }
    }

    /// Begin a PRE-VOTE round (Ongaro dissertation section 9.6). The node stays a
    /// `Follower` for every other purpose (its `role`, `current_term`, and `votedFor` are
    /// UNCHANGED -- a pre-vote NEVER persists state on the pre-candidate), enters the
    /// volatile pre-candidate state ([`pre_votes`](RaftNode::pre_votes) seeded with self at
    /// `pre_vote_term = current_term + 1`), and broadcasts [`RaftMsg::PreVote`] to every
    /// OTHER voter carrying its last-log `(index, term)`. Self counts as one pre-vote grant,
    /// so a single-voter cluster (or a cluster where every other voter is unreachable but a
    /// majority is self) reaches the pre-vote quorum at once and converts straight to a real
    /// election via [`maybe_promote_pre_candidate`]. The election timer is re-armed so a
    /// pre-vote round that wins no quorum simply expires and retries (no term was burned).
    fn start_pre_vote(&mut self, now: Monotonic, rng: &mut dyn RaftRng, out: &mut Effects) {
        let pre_term = self.storage.current_term() + 1;
        self.pre_vote_term = pre_term;
        let mut tally = BTreeSet::new();
        tally.insert(self.id);
        self.pre_votes = Some(tally);
        // A fresh round has heard from no peer yet (etcd #8525 engagement gate): the fallback
        // only counts this round if a real `PreVoteResp` arrives, so a fully isolated node
        // (which gets none) never term-bumps and so never disrupts a standing leader on heal.
        self.pre_vote_round_responded = false;

        let last_log_index = self.storage.last_log_index();
        let last_log_term = self.storage.last_log_term();
        for &peer in &self.voters {
            if peer != self.id {
                out.send(
                    peer,
                    RaftMsg::PreVote {
                        term: pre_term,
                        candidate: self.id,
                        last_log_index,
                        last_log_term,
                    },
                );
            }
        }
        // Re-arm so a lost pre-vote round retries later (no term was incremented, so this
        // re-arm is the ONLY cost of a failed round -- the disruption-free property).
        self.arm_election_timer(rng, out);
        // A trivial (single-voter) cluster's self pre-vote is already a quorum: promote now.
        self.maybe_promote_pre_candidate(now, out);
    }

    /// Convert a pre-candidate to a REAL candidate once its pre-vote tally is a quorum of
    /// the voter set (Ongaro section 9.6). Only meaningful while a pre-vote round is in
    /// flight ([`pre_votes`](RaftNode::pre_votes) is `Some`); a no-op otherwise. On reaching
    /// the quorum it clears the pre-vote state and runs the deferred real election
    /// (`start_real_election`), which is the FIRST point any term is incremented.
    fn maybe_promote_pre_candidate(&mut self, now: Monotonic, out: &mut Effects) {
        let Some(tally) = self.pre_votes.as_ref() else {
            return;
        };
        // Count only grants from CURRENT-CONFIG VOTERS (a learner that pre-grants is inert),
        // mirroring the real-election quorum in `maybe_become_leader`.
        let needed = self.voters.len() / 2 + 1;
        let grants = tally.iter().filter(|v| self.voters.contains(v)).count();
        if grants < needed {
            return;
        }
        // Pre-vote succeeded: the round is over and the deferred real election begins. Note
        // the election timer was already (re-)armed when the round started, so the real
        // election relies on that arm; `start_real_election` does NOT re-arm (avoiding a
        // second arm in the same logical timeout). This is the FIRST term increment.
        // PROGRESS: winning a pre-vote quorum is real progress, so reset the mixed-version
        // fallback counter (etcd #8525) -- a cluster that grants pre-votes never falls back.
        self.failed_pre_vote_rounds = 0;
        self.pre_votes = None;
        self.start_real_election(now, out);
    }

    /// Start a REAL election (the term-incrementing campaign, Figure 2 "Candidates"): bump
    /// and persist `currentTerm`; vote for self (persisted); become `Candidate`; reset the
    /// vote tally to `{self}`; broadcast [`RaftMsg::RequestVote`] to every OTHER voter; then
    /// (a single-voter cluster) win at once. Does NOT arm the election timer -- the caller
    /// owns timer arming (the timeout path re-arms; the pre-vote-success path relies on the
    /// arm the round already issued), so this never double-arms in one logical timeout.
    fn start_real_election(&mut self, now: Monotonic, out: &mut Effects) {
        let new_term = self.storage.current_term() + 1;
        self.storage.set_current_term(new_term);
        self.storage.set_voted_for(Some(self.id));
        self.role = Role::Candidate;
        self.votes.clear();
        self.votes.insert(self.id);
        // A real campaign ends any pre-vote round bookkeeping (we are past it now).
        self.pre_votes = None;
        // Starting an election: we no longer recognize any leader (HA-9 passive record).
        // Cleared here, set again to self on winning or to the sender on accepting a
        // current-term AppendEntries. This is a record only; it changes no decision.
        self.leader_id = None;

        let last_log_index = self.storage.last_log_index();
        let last_log_term = self.storage.last_log_term();
        for &peer in &self.voters {
            if peer != self.id {
                out.send(
                    peer,
                    RaftMsg::RequestVote {
                        term: new_term,
                        candidate: self.id,
                        last_log_index,
                        last_log_term,
                    },
                );
            }
        }

        // A single-voter cluster: self-vote is already a majority, win at once.
        self.maybe_become_leader(now, out);
    }

    /// REQUESTVOTE receiver (Figure 2, "RequestVote RPC, Receiver implementation",
    /// plus section 5.4.1 for the up-to-date check, plus the Ongaro section-4.2.3
    /// LEADER-STICKINESS disruptive-server mitigation).
    ///
    /// Order of operations:
    /// 0. LEADER-STICKINESS (section 4.2.3, when [`RaftConfig::pre_vote`] is on): if we have
    ///    heard from a VALID CURRENT LEADER within the MINIMUM election timeout, IGNORE this
    ///    RequestVote entirely -- do NOT adopt its term and do NOT grant. A fresh leader means
    ///    no election is warranted, so a single disruptive / flapping server cannot force the
    ///    cluster to elect by inflating the term. (Paired with pre-vote, a disruptor cannot
    ///    even reach a real RequestVote, so this is the belt-and-suspenders backstop. With
    ///    pre-vote off it is also off, restoring the byte-identical legacy term-adopt path.)
    /// 1. "All Servers": if `term > currentTerm`, step down and adopt the term
    ///    (clearing `votedFor`), so a fresh term's first vote is grant-eligible.
    /// 2. Reply false (no grant) if `term < currentTerm` (rule 1).
    /// 3. Otherwise grant IFF (`votedFor` is null or already the candidate) AND
    ///    the candidate's log is at least as up-to-date as ours (rule 2). On a
    ///    grant: persist `votedFor = candidate` and RESET the election timer
    ///    (granting a vote is "hearing from a valid leader-to-be").
    // PROD-9 added the `now` argument (leader-stickiness freshness), pushing the receiver
    // RPC's decoded fields one over the pedantic cap, same as `on_append_entries`.
    #[allow(clippy::too_many_arguments)]
    fn on_request_vote(
        &mut self,
        now: Monotonic,
        rng: &mut dyn RaftRng,
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
        out: &mut Effects,
    ) {
        // Step 0: leader-stickiness. A RequestVote arriving while a current leader is fresh
        // is the disruptive-server signature; refuse it WITHOUT adopting its (possibly
        // inflated) term, so a flapping node cannot depose a healthy leader. Gated on
        // `pre_vote` so disabling the refinements is a clean revert to the legacy path.
        if self.config.pre_vote && self.leader_is_fresh(now) {
            out.send(
                candidate,
                RaftMsg::RequestVoteResp {
                    term: self.storage.current_term(),
                    vote_granted: false,
                },
            );
            return;
        }

        self.observe_term(term, rng, out);
        let current = self.storage.current_term();

        let grant = if term < current {
            // Rule 1: stale candidate term, never grant.
            false
        } else {
            // term == current here (observe_term raised us if it was greater).
            let already = self.storage.voted_for();
            let free = already.is_none() || already == Some(candidate);
            free && self.candidate_log_up_to_date(last_log_index, last_log_term)
        };

        if grant {
            self.storage.set_voted_for(Some(candidate));
            // Granting a vote counts as recognizing a valid contender; back off our
            // own election timer so we do not immediately challenge the candidate
            // we just voted for (section 5.2).
            self.arm_election_timer(rng, out);
        }

        out.send(
            candidate,
            RaftMsg::RequestVoteResp {
                term: self.storage.current_term(),
                vote_granted: grant,
            },
        );
    }

    /// PREVOTE receiver (Ongaro section 9.6). A NON-BINDING poll: grant IFF the
    /// pre-candidate's log is at least as up-to-date as ours (section 5.4.1) AND no valid
    /// current leader is fresh (leader-stickiness, the same condition step 0 of
    /// [`on_request_vote`] uses). A pre-vote writes NO persistent state on EITHER side: it
    /// neither adopts the hypothetical `term` nor records a `votedFor`, so a node may grant
    /// pre-votes to several pre-candidates and a refused pre-vote costs nothing. The reply
    /// echoes the hypothetical `term` so the pre-candidate tallies only its current round.
    fn on_pre_vote(
        &mut self,
        now: Monotonic,
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
        out: &mut Effects,
    ) {
        // A pre-vote NEVER mutates persistent state: do not call `observe_term`, do not set
        // `votedFor`. Grant only if (a) the hypothetical term is not stale relative to ours
        // (a pre-vote at a term we already exceed cannot help), (b) the candidate's log is
        // up-to-date, and (c) no fresh leader makes an election pointless (stickiness).
        let grant = term > self.storage.current_term()
            && self.candidate_log_up_to_date(last_log_index, last_log_term)
            && !self.leader_is_fresh(now);
        out.send(
            candidate,
            RaftMsg::PreVoteResp {
                term,
                vote_granted: grant,
            },
        );
    }

    /// PREVOTE response handler (Ongaro section 9.6). Tally a granted pre-vote from `from`
    /// IFF a pre-vote round is in flight and this reply is for it (`term == pre_vote_term`),
    /// then promote to a real candidate once the tally is a quorum
    /// ([`maybe_promote_pre_candidate`]). A stale reply (wrong term, or no round in flight)
    /// is ignored. A pre-vote reply NEVER carries a higher term we must adopt: it echoes the
    /// HYPOTHETICAL term, which no node ever persisted, so there is no `observe_term` here.
    fn on_pre_vote_resp(
        &mut self,
        now: Monotonic,
        from: NodeId,
        term: u64,
        vote_granted: bool,
        out: &mut Effects,
    ) {
        if self.pre_votes.is_none() || term != self.pre_vote_term {
            // No round in flight, or a stale reply from a prior round: ignore. (Checked BEFORE
            // the grant filter so a stale reply does not falsely mark the round as engaged.)
            return;
        }
        // ENGAGEMENT (etcd #8525): any in-round reply -- grant OR rejection -- proves a peer is
        // reachable and answering, so this round MAY count toward the mixed-version fallback.
        // A rejection alone never advances the tally, but it does distinguish a reachable
        // (mixed-version) peer set from a true partition (which yields no replies at all).
        self.pre_vote_round_responded = true;
        if !vote_granted {
            return;
        }
        if let Some(tally) = self.pre_votes.as_mut() {
            tally.insert(from);
        }
        self.maybe_promote_pre_candidate(now, out);
    }

    /// REQUESTVOTE response handler (Figure 2, "Candidates": if votes from majority
    /// of servers, become leader).
    ///
    /// If the response's term exceeds ours, step down (the "All Servers" rule).
    /// Otherwise, only a `Candidate` in the SAME term cares: a granted vote from
    /// the responding voter `from` is tallied (by id, so a duplicate grant is
    /// idempotent), and if the tally reaches a strict majority of the voter set we
    /// become leader. A stale-term response and a non-granting response are
    /// ignored. The granter's id is taken from the delivery's `from`, since the
    /// wire `RequestVoteResp` does not carry it (matching real Raft, where the RPC
    /// reply's sender is known by the transport).
    fn on_request_vote_resp(
        &mut self,
        now: Monotonic,
        rng: &mut dyn RaftRng,
        from: NodeId,
        term: u64,
        vote_granted: bool,
        out: &mut Effects,
    ) {
        if self.observe_term(term, rng, out) {
            // We stepped down; a stale candidacy's tally is irrelevant now.
            return;
        }
        if vote_granted {
            self.record_vote(now, from, term, out);
        }
    }

    /// APPENDENTRIES receiver (Figure 2, "AppendEntries RPC, Receiver
    /// implementation", rules 1-5; section 5.3). This is now the real
    /// log-replication RPC (an empty `entries` is a heartbeat, the degenerate case
    /// of the same path).
    ///
    /// Order of operations:
    /// 1. "All Servers": adopt a strictly greater term (step down, clear vote).
    /// 2. RULE 1: reply false if `term < currentTerm` (a stale leader); the reply
    ///    carries our higher term so the stale leader steps down. We do NOT reset
    ///    our election timer for a stale leader.
    /// 3. `term == currentTerm`: a legitimate leader for this term. A `Candidate`
    ///    concedes and becomes a `Follower` (Figure 2, "Candidates"); RESET the
    ///    election timer (we have heard from the current leader).
    /// 4. RULE 2 (log consistency check): reply false if our log does not contain
    ///    an entry at `prev_log_index` whose term == `prev_log_term`. `prev_log_index
    ///    == 0` (the sentinel) always matches. On failure we reply false with a
    ///    `match_index` hint of our last index (no fast-backup; the leader decrements
    ///    `nextIndex` and retries).
    /// 5. RULE 3 (conflict truncation) + RULE 4 (append): walk the incoming entries
    ///    against our log; at the first index whose term differs (a conflict) we
    ///    TRUNCATE from there and append the remaining incoming entries. Entries we
    ///    already hold identically are left untouched (so a duplicated/retried
    ///    AppendEntries does not truncate committed suffix - the section-5.3
    ///    idempotence the paper's rule 3 "(same index but different terms)" wording
    ///    requires).
    /// 6. RULE 5: set `commit_index = min(leader_commit, index of last new entry)`,
    ///    then drive the apply pipeline. "Index of last new entry" is the index of
    ///    the LAST entry this RPC reconciled into our log (`prev_log_index +
    ///    entries.len()`), not our overall last index, so a lagging follower does
    ///    not over-advance commit past what this leader vouched for.
    // Takes `entries` by value to match `on_message`'s by-value step surface (the
    // transport hands ownership of the decoded message in); we only borrow/slice it
    // here, hence the needless_pass_by_value allow, same as `on_message`.
    #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
    fn on_append_entries(
        &mut self,
        now: Monotonic,
        rng: &mut dyn RaftRng,
        term: u64,
        leader: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        out: &mut Effects,
    ) {
        self.observe_term(term, rng, out);
        let current = self.storage.current_term();

        if term < current {
            // Rule 1: stale leader. Do not reset our timer; reply with our higher
            // term so the stale leader steps down. The match_index hint is moot on a
            // term-rejected reply (the leader will step down on the higher term).
            out.send(
                leader,
                RaftMsg::AppendEntriesResp {
                    term: current,
                    success: false,
                    match_index: self.storage.last_log_index(),
                },
            );
            return;
        }

        // term == current: a legitimate leader for this term. A candidate concedes.
        if self.role != Role::Follower {
            self.step_down_to_follower(out);
        }
        // Recognize the leader: reset the election timer (heard from leader). We do
        // this BEFORE the consistency check: even a check-failing AppendEntries is
        // proof of a live current-term leader, so we must not time out and disrupt it.
        self.arm_election_timer(rng, out);
        // Record WHO the current-term leader is (HA-9 passive record): this is the very
        // `leader` field Raft already ships in AppendEntries "so a follower can redirect
        // clients" (Figure 2). We capture it for proposal-forwarding. It is read by no
        // decision and changes no effect, so the DST trace is unchanged.
        self.leader_id = Some(leader);
        // LEADER-STICKINESS / PRE-VOTE freshness (Ongaro section 9.6): stamp the instant we
        // last heard from a valid current-term leader. A (pre-)vote received within the
        // minimum election timeout of this is refused, so a disruptive server cannot depose a
        // healthy leader. A live current-term leader also makes any in-flight pre-vote round
        // moot, so abort it (no term was burned). Both are decision-inert with pre-vote off.
        self.last_leader_contact = Some(now);
        self.pre_votes = None;
        // PROGRESS: a live current-term leader is the strongest "no election needed" signal,
        // so reset the mixed-version fallback counter (etcd #8525). A follower that keeps
        // hearing its leader never accumulates failed pre-vote rounds and so never falls back.
        self.failed_pre_vote_rounds = 0;

        // Rule 2: log consistency check. The log must contain an entry at
        // prev_log_index whose term matches prev_log_term. prev_log_index 0 (the
        // empty-log sentinel) always matches (term_at returns 0 == prev_log_term 0).
        if self.storage.term_at(prev_log_index) != prev_log_term {
            out.send(
                leader,
                RaftMsg::AppendEntriesResp {
                    term: current,
                    success: false,
                    // Hint our last index so the leader can bound its rewind. We do
                    // not implement the fast-backup conflict index; one decrement per
                    // round is correct, just slower.
                    match_index: self.storage.last_log_index(),
                },
            );
            return;
        }

        // Rules 3 + 4: reconcile `entries` into our log. Find the first incoming
        // entry that is NOT already present-and-identical; truncate any conflicting
        // suffix from there, then append the rest. Entries we already hold are left
        // in place so a retransmitted prefix does not truncate (and potentially lose)
        // an already-committed suffix.
        let mut append_from = 0usize; // index into `entries` of the first to append
        let mut truncated = false; // whether a conflict truncation dropped a suffix
        for (i, entry) in entries.iter().enumerate() {
            // The 1-based log index this incoming entry would occupy.
            let idx = prev_log_index + 1 + u64::try_from(i).unwrap_or(u64::MAX);
            let existing_term = self.storage.term_at(idx);
            if existing_term == 0 {
                // No existing entry here (term_at returns 0 past the end): from this
                // point on everything is genuinely new.
                append_from = i;
                break;
            }
            if existing_term != entry.term {
                // Rule 3: conflict (same index, different term). Truncate from here
                // and append from this incoming entry onward.
                self.storage.truncate_from(idx);
                truncated = true;
                append_from = i;
                break;
            }
            // Same index, same term: identical (Log Matching). Already present; skip.
            append_from = i + 1;
        }
        let appended = append_from < entries.len();
        if appended {
            self.storage.append_entries(&entries[append_from..]);
        }
        // HA-3d APPEND-TIME ADOPTION (Raft section 6): a follower adopts the configuration
        // its log implies the instant a `ConfigChange` entry lands (or a truncation drops
        // one). Recompute only when this RPC actually CHANGED the surviving log (a
        // truncation or an append), so an idempotent retransmit does no work. With no
        // `ConfigChange` entries anywhere this is a no-op over the baseline, keeping the
        // default path byte-identical (it only ever changes the sets when a membership
        // entry is present). A follower's vote eligibility then reflects its own log's
        // config, which is exactly the section-6 caveat: a joining node that already holds
        // the AddVoter entry can vote, and the cluster does not deadlock.
        if truncated || appended {
            self.recompute_config_from_log();
        }

        // Rule 5: advance commit_index toward leader_commit, capped at the index of
        // the last entry THIS RPC vouched for (prev_log_index + entries.len()).
        let last_new_index = prev_log_index + u64::try_from(entries.len()).unwrap_or(u64::MAX);
        if leader_commit > self.commit_index {
            let new_commit = leader_commit.min(last_new_index);
            if new_commit > self.commit_index {
                self.commit_index = new_commit;
                // HA-prod-commit-ack: notify the adapter of the new committed high-water
                // (additive, no I/O). A follower carries no parked LOCAL propose ack, but
                // the record is emitted uniformly at every commit-advance site so the
                // adapter's commit-drain logic is identical regardless of role.
                out.note_committed_through(new_commit);
                self.apply_committed();
            }
        }

        out.send(
            leader,
            RaftMsg::AppendEntriesResp {
                term: current,
                success: true,
                // The highest index we now agree with the leader on: the last entry
                // this RPC reconciled. Using last_new_index (not our overall last
                // index) keeps match_index honest if our log had a longer stale tail.
                match_index: last_new_index,
            },
        );
    }

    /// APPENDENTRIES response handler (Figure 2, "Leaders": on AppendEntries
    /// response, update `nextIndex`/`matchIndex` and advance `commitIndex`).
    ///
    /// 1. "All Servers": a response with a greater term steps the leader down. We
    ///    return immediately in that case (the stale-leader bookkeeping is moot).
    /// 2. Only a `Leader` in the SAME term as the response cares (a stale response
    ///    from an old term is ignored).
    /// 3. On success: set `match_index[from] = msg.match_index` and `next_index[from]
    ///    = match_index + 1` (taking the MAX so a reordered/duplicated older success
    ///    cannot rewind progress), then try to advance our `commit_index` under the
    ///    section-5.4.2 rule.
    /// 4. On failure (and we did not step down): DECREMENT `next_index[from]`
    ///    (floor 1) and immediately retry with the earlier prev (the paper's
    ///    "decrement nextIndex and retry" backup).
    #[allow(clippy::too_many_arguments)]
    fn on_append_entries_resp(
        &mut self,
        now: Monotonic,
        rng: &mut dyn RaftRng,
        from: NodeId,
        term: u64,
        success: bool,
        match_index: u64,
        out: &mut Effects,
    ) {
        if self.observe_term(term, rng, out) {
            // We stepped down on a higher term; we are no longer leader.
            return;
        }
        if self.role != Role::Leader || term != self.storage.current_term() {
            // Stale response (old term) or we are not leader: ignore.
            return;
        }

        if success {
            // CHECK-QUORUM (Ongaro section 6.2 / 9.6): a SUCCESSFUL response is proof this
            // peer reached us within this round; stamp the contact instant so the heartbeat
            // tick can count whether a quorum is still reachable. Only a success counts
            // (a rejected response over a healthy link still proves reachability, but using
            // success keeps the bar simple and never UNDER-counts contact -- a follower that
            // can reply success is reachable). Inert with check-quorum off (never read).
            self.quorum_contact.insert(from, now);
            // Advance this peer's replicated/next markers. Take the MAX so a delayed
            // or duplicated older success can never rewind an already-higher marker.
            let m = self.match_index.entry(from).or_insert(0);
            *m = (*m).max(match_index);
            let mi = *m;
            self.next_index.insert(from, mi + 1);
            // A peer made progress: maybe a new index is now on a majority.
            self.maybe_advance_commit(out);
        } else {
            // Rule: decrement nextIndex (floor 1) and retry with the earlier prev.
            let ni = self.next_index.entry(from).or_insert(1);
            if *ni > 1 {
                *ni -= 1;
            }
            self.send_append_entries_to(from, out);
        }
    }

    /// HEARTBEAT timer (Figure 2, "Leaders": send empty AppendEntries to each
    /// server, repeat during idle periods). Only a `Leader` acts; a stale timer on
    /// a stepped-down node is ignored (and was cancelled on step-down anyway).
    ///
    /// CHECK-QUORUM (Ongaro section 6.2 / 9.6, when [`RaftConfig::check_quorum`] is on): a
    /// leader that has NOT had successful contact from a QUORUM of voters within an election
    /// timeout has lost the majority and STEPS DOWN -- it stops believing it is leader, so it
    /// can no longer serve stale leader-only operations while partitioned away. We evaluate
    /// this BEFORE broadcasting: a stepped-down node sends no heartbeat. With check-quorum
    /// off the leader never self-deposes (the legacy behaviour), so the path is unchanged.
    fn on_heartbeat_timer(&mut self, now: Monotonic, rng: &mut dyn RaftRng, out: &mut Effects) {
        if self.role != Role::Leader {
            return;
        }
        if self.config.check_quorum && !self.has_quorum_contact(now) {
            // Lost quorum-contact: step down to follower (cancels HEARTBEAT) and re-arm the
            // election timer so this node can participate in the next election as a follower.
            // We do NOT bump the term -- this is a voluntary step-down, not a term advance.
            self.step_down_to_follower(out);
            self.leader_id = None;
            self.arm_election_timer(rng, out);
            return;
        }
        self.broadcast_heartbeat(out);
        out.set_timer(HEARTBEAT, self.config.heartbeat_interval);
    }

    /// Whether this LEADER has had successful contact from a QUORUM of the current-config
    /// voters within the minimum election timeout of `now` (CHECK-QUORUM, Ongaro section
    /// 6.2 / 9.6). The leader counts ITSELF (it is trivially in contact with itself) plus
    /// every OTHER voter whose last successful AppendEntries response is fresher than one
    /// minimum election timeout. A quorum is `voters/2 + 1`. A single-voter cluster is its
    /// own quorum, so it never steps down. Only voters count (learners are non-voting), and
    /// only peers in the CURRENT config (a contact record for a removed peer is ignored).
    fn has_quorum_contact(&self, now: Monotonic) -> bool {
        let window = self.min_election_timeout();
        // The leader counts itself if it is still a voter (a leader mid-self-removal is not).
        let mut fresh = usize::from(self.voters.contains(&self.id));
        for &peer in &self.voters {
            if peer == self.id {
                continue;
            }
            if let Some(when) = self.quorum_contact.get(&peer) {
                if now.saturating_duration_since(*when) < window {
                    fresh += 1;
                }
            }
        }
        let needed = self.voters.len() / 2 + 1;
        fresh >= needed
    }

    // -- internal transitions ----------------------------------------------

    /// The "All Servers" term rule (Figure 2): if `term > currentTerm`, set
    /// `currentTerm = term`, clear `votedFor`, and convert to `Follower`,
    /// re-arming the election timer. Returns `true` iff a step-down happened, so a
    /// caller can short-circuit stale-candidacy logic.
    ///
    /// This is the single chokepoint for term adoption; every handler calls it
    /// first, so no message is ever processed against a lower term, which is the
    /// core of term safety.
    ///
    /// TERM-INFLATION HARDENING (Pre-Vote + leader-stickiness, Ongaro section 9.6, now
    /// implemented): a stale node that cannot win NO LONGER forces term inflation here in the
    /// common case. With pre-vote on, such a node never reaches a real higher-term
    /// `RequestVote` (its pre-vote fails the up-to-date check / a fresh leader), so this
    /// chokepoint is not reached by its traffic; and a `RequestVote` arriving while a current
    /// leader is fresh is refused by [`on_request_vote`]'s stickiness gate BEFORE
    /// `observe_term`, so the term is not adopted. `observe_term` itself is unchanged (it
    /// remains the single, honest "adopt a strictly greater term" chokepoint for the cases
    /// that DO reach it -- a real higher-term leader / candidate); the refinements simply
    /// keep a disruptor's traffic from reaching it. `disruptive_stale_node_churns_term_*`
    /// pins both behaviours (the legacy churn with pre-vote OFF, the no-churn with it ON).
    fn observe_term(&mut self, term: u64, rng: &mut dyn RaftRng, out: &mut Effects) -> bool {
        if term > self.storage.current_term() {
            self.storage.set_current_term(term);
            self.storage.set_voted_for(None);
            let was_leader = self.role == Role::Leader;
            self.role = Role::Follower;
            self.votes.clear();
            // A new term ends any in-flight pre-vote round (it was for a now-stale term) and
            // clears leader quorum-contact tracking (we are no longer leader). Both volatile.
            self.pre_votes = None;
            self.quorum_contact.clear();
            // PROGRESS: observing a higher term means real activity is happening at a term
            // beyond ours (a real leader/candidate exists), so reset the mixed-version
            // fallback counter (etcd #8525); we adopt that term and give it time to complete.
            self.failed_pre_vote_rounds = 0;
            // A higher term invalidates the leader we recognized (HA-9 passive record):
            // the new term's leader is not yet known and will be set when its first
            // AppendEntries is accepted. A record only; changes no decision/effect.
            self.leader_id = None;
            // Drop leader-only volatile state (reinitialized on the next election).
            self.next_index.clear();
            self.match_index.clear();
            // PROD-9: per-follower chunked-snapshot progress is leader-only; drop it so a
            // future re-election restarts any transfer cleanly from offset 0.
            self.snapshot_next_offset.clear();
            if was_leader {
                out.cancel_timer(HEARTBEAT);
            }
            // Adopting a new term resets our election timer: we have just learned of
            // activity at a higher term and should give it time to complete.
            self.arm_election_timer(rng, out);
            true
        } else {
            false
        }
    }

    /// Become a `Follower` without a term change (a `Candidate` conceding to a
    /// same-term leader; Figure 2, "Candidates": convert to follower on receiving
    /// AppendEntries from a leader of the current term).
    ///
    /// A `Leader` must never reach this in 3a (two leaders in one term is the very
    /// thing Election Safety forbids, and `observe_term` handles strictly-greater
    /// terms). We nonetheless cancel the HEARTBEAT timer if we were somehow a
    /// Leader here: it is cheap defense-in-depth on the split-brain-critical path so
    /// a future change cannot silently leave a stale heartbeat armed on a
    /// stepped-down node (the canary is the election-safety assertion, which would
    /// fire first).
    fn step_down_to_follower(&mut self, out: &mut Effects) {
        if self.role == Role::Leader {
            out.cancel_timer(HEARTBEAT);
        }
        self.role = Role::Follower;
        self.votes.clear();
        // Leader-only volatile state is meaningless once we are not leader; clear it
        // so a future re-election reinitializes it cleanly (Figure 2 reinitializes
        // nextIndex/matchIndex on every election). The check-quorum contact map is also
        // leader-only; drop it so a later re-election re-seeds it fresh.
        self.next_index.clear();
        self.match_index.clear();
        // PROD-9: per-follower chunked-snapshot progress is leader-only; drop it too.
        self.snapshot_next_offset.clear();
        self.quorum_contact.clear();
    }

    /// Become `Leader` if the current vote tally is a strict majority of voters
    /// (Figure 2, "Candidates"). On winning: cancel the election timer; INITIALIZE
    /// the per-peer replication state (`nextIndex = lastLogIndex + 1`, `matchIndex =
    /// 0`, Figure 2 "Leaders"); append a no-op to our own log (section 8: a
    /// current-term entry the new leader can commit, which is also what lets the
    /// commit-only-current-term rule carry forward prior-term entries; see
    /// [`RaftNode::maybe_advance_commit`]); then broadcast the initial replication
    /// AppendEntries and arm the heartbeat timer.
    ///
    /// `now` is the election instant: it SEEDS the check-quorum contact window
    /// ([`quorum_contact`](RaftNode::quorum_contact)) for every current voter, because
    /// winning a majority of votes IS quorum contact at this instant -- without the seed a
    /// fresh leader would spuriously step down on its very first heartbeat (no follower has
    /// acked yet). Inert when check-quorum is off (the field is then never read).
    fn maybe_become_leader(&mut self, now: Monotonic, out: &mut Effects) {
        if self.role != Role::Candidate {
            return;
        }
        // HA-3d: win on a strict majority of the CURRENT-CONFIG VOTER SET, counting only
        // votes FROM voters (a learner that grants a vote is replicating, not voting, so it
        // must not count toward the election quorum). With a static voter set the granted
        // votes are all from voters and `self.votes.len()` already equals the voter count,
        // so this is byte-identical to the pre-3d `self.votes.len() >= needed` check.
        let needed = self.voters.len() / 2 + 1;
        let votes_from_voters = self
            .votes
            .iter()
            .filter(|v| self.voters.contains(v))
            .count();
        if votes_from_voters < needed {
            return;
        }
        self.role = Role::Leader;
        // We are the leader for this term (HA-9 passive record): a forwarded proposal
        // routed here proposes locally rather than chaining. A record only.
        self.leader_id = Some(self.id);
        // PROGRESS: becoming leader is the terminal success of an election, so reset the
        // mixed-version fallback counter (etcd #8525). (It is normally already 0 by here --
        // a pre-vote win reset it before the real election -- but the fallback path bumps the
        // term without a pre-vote win, so clear it explicitly when that path elects.)
        self.failed_pre_vote_rounds = 0;
        out.cancel_timer(ELECTION_TIMEOUT);

        // Initialize leader replication state for every peer (Figure 2, "Leaders":
        // on election, nextIndex = last log index + 1, matchIndex = 0). Our own
        // match is implicit (we always have our whole log); the commit counter
        // counts the leader itself separately. HA-3d: replicate to LEARNERS too (they
        // catch up the log) -- they get markers and AppendEntries but are never counted
        // toward a quorum; the chain over voters AND learners is what ships them the log.
        let next = self.storage.last_log_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        // PROD-9: a new leader has no in-flight chunked-snapshot transfers; clear any
        // residue so the first time it must snapshot a lagging peer it starts at offset 0.
        self.snapshot_next_offset.clear();
        for &peer in self.voters.iter().chain(self.learners.iter()) {
            if peer != self.id {
                self.next_index.insert(peer, next);
                self.match_index.insert(peer, 0);
            }
        }
        // CHECK-QUORUM seed (Ongaro section 6.2 / 9.6): winning a majority of votes is proof
        // of quorum contact AT THIS INSTANT, so stamp `now` for every current voter. This is
        // the freshness baseline the heartbeat tick measures against; without it a leader
        // would self-depose on its first heartbeat before any follower has had a chance to
        // ack. Voters only (learners are non-voting and never counted). Inert with
        // check-quorum off.
        self.quorum_contact.clear();
        for &peer in &self.voters {
            self.quorum_contact.insert(peer, now);
        }

        // Append the election no-op (section 8; a current-term entry the leader can
        // commit, which also makes prior-term entries committable transitively).
        let next_index = self.storage.last_log_index() + 1;
        let term = self.storage.current_term();
        self.storage.append(LogEntry {
            term,
            index: next_index,
            payload: EntryPayload::Noop,
        });

        // Replicate (the no-op plus any backlog) to every peer and start heartbeats.
        self.broadcast_heartbeat(out);
        out.set_timer(HEARTBEAT, self.config.heartbeat_interval);
    }

    /// Broadcast a replication `AppendEntries` to every other voter AND learner (Figure
    /// 2, "Leaders"; HA-3d for learners). Each peer's RPC carries `prev` and `entries`
    /// derived from that peer's `nextIndex`, so this is heartbeat AND log shipping in one:
    /// a caught-up peer gets an empty `entries` (a pure heartbeat), a lagging peer gets
    /// the entries it is missing. LEARNERS are replicated to exactly like voters (so they
    /// catch up); they simply never count toward a quorum. With no learners (the default)
    /// the iterated set is exactly the voter set, so the default path is byte-identical.
    fn broadcast_heartbeat(&mut self, out: &mut Effects) {
        // Collect the peer set first so the loop can take `&mut self` per send (PROD-9:
        // a send may now update the per-peer chunked-snapshot offset). The set is the
        // voters + learners minus self, in the same deterministic order as before.
        let peers: Vec<NodeId> = self
            .voters
            .iter()
            .chain(self.learners.iter())
            .copied()
            .filter(|&peer| peer != self.id)
            .collect();
        for peer in peers {
            self.send_append_entries_to(peer, out);
        }
    }

    /// Send a single replication `AppendEntries` to `peer` from its `nextIndex`
    /// (Figure 2, "Leaders": send AppendEntries with log entries starting at
    /// nextIndex). `prev_log_index = nextIndex - 1`, `prev_log_term =
    /// term_at(prev_log_index)`, `entries = entries_from(nextIndex)`, `leader_commit
    /// = commit_index`. Only meaningful while `role == Leader`.
    ///
    /// PROD-9: when the peer's needed prefix is below this leader's snapshot, this starts
    /// or resumes a CHUNKED [`RaftMsg::InstallSnapshot`] transfer (it is `&mut self` so it
    /// can track the per-peer chunk offset) instead of an AppendEntries.
    fn send_append_entries_to(&mut self, peer: NodeId, out: &mut Effects) {
        let term = self.storage.current_term();
        let next = self.next_index.get(&peer).copied().unwrap_or(1);

        // Raft section 7: if the entry the peer needs (`next`, and its `prev` at
        // `next - 1`) has been COMPACTED away on this leader, an AppendEntries can no
        // longer carry the missing prefix - send an InstallSnapshot instead. The
        // boundary is `log_start_index`: entries with index < log_start are gone, except
        // that `term_at(snapshot.last_included_index)` is still answerable. So we must
        // install a snapshot exactly when `prev_log_index` is BELOW the snapshot's
        // last_included_index (the prev entry's term is unknowable), i.e. when
        // `next - 1 < last_included_index`, equivalently `next <= last_included_index`.
        // When `prev_log_index == last_included_index` the snapshot still answers the
        // prev term, so a normal AppendEntries works.
        if let Some((meta, _)) = self.storage.load_snapshot() {
            if next <= meta.last_included_index {
                // PROD-9: start (or resume) a CHUNKED snapshot transfer. The first chunk
                // for a peer is at offset 0; `snapshot_next_offset` tracks where the next
                // chunk should pick up once acks come back. The actual byte slicing lives
                // in `send_snapshot_chunk_to`.
                let offset = self.snapshot_next_offset.get(&peer).copied().unwrap_or(0);
                self.snapshot_next_offset.insert(peer, offset);
                self.send_snapshot_chunk_to(peer, offset, out);
                return;
            }
        }
        // The peer's needed prefix is no longer below a snapshot, so any stale chunked
        // transfer progress for it is moot; drop it so a future snapshot restarts at 0.
        self.snapshot_next_offset.remove(&peer);

        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.storage.term_at(prev_log_index);
        let entries = self.storage.entries_from(next);
        out.send(
            peer,
            RaftMsg::AppendEntries {
                term,
                leader: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        );
    }

    /// PROD-9: emit ONE [`RaftMsg::InstallSnapshot`] chunk for `peer` at byte `offset`
    /// (Raft Figure 13). Reads the snapshot bytes from storage (the same bytes the
    /// pre-PROD-9 path shipped whole), slices `[offset, offset + chunk_len)` where
    /// `chunk_len = min(snapshot_chunk_bytes, remaining)`, and marks `done` when this chunk
    /// reaches the end of the snapshot. The slicing is a pure function of the snapshot bytes
    /// and the config chunk size, so chunk boundaries replay byte-identically. A snapshot
    /// smaller than (or a chunk size at/above) the snapshot size sends the whole thing in
    /// one `done` chunk, byte-equivalent to the old whole-snapshot install.
    ///
    /// `offset` is expected to be `<= data.len()` (the caller advances it only on the
    /// follower-reported next offset, which is bounded by what was sent); an out-of-range
    /// offset is clamped so the chunk is empty + `done`, which is inert (the follower
    /// already holds that prefix). No-op if the leader holds no snapshot.
    fn send_snapshot_chunk_to(&mut self, peer: NodeId, offset: u64, out: &mut Effects) {
        let Some((meta, data)) = self.storage.load_snapshot() else {
            // No snapshot to send (e.g. it was just dropped); a future tick re-evaluates.
            self.snapshot_next_offset.remove(&peer);
            return;
        };
        let term = self.storage.current_term();
        // HA-3d: the persisted CONFIG BASELINE the snapshot reflects, so the installing
        // follower can rebuild its configuration (its log below the snapshot is gone).
        // Empty for a static cluster (config-inert there). Repeated on every chunk; the
        // follower adopts it only on `done`.
        let (voters, learners) = self
            .storage
            .load_config_baseline()
            .unwrap_or_else(|| (BTreeSet::new(), BTreeSet::new()));

        let total = data.len();
        // Clamp the offset into range (a follower can never legitimately ask past the end,
        // but stay total): an out-of-range start yields an empty, `done` chunk.
        let start = usize::try_from(offset).unwrap_or(usize::MAX).min(total);
        // A chunk is at most `snapshot_chunk_bytes`; treat 0 as "the whole remainder" so a
        // misconfigured 0 never loops on empty chunks. The remainder bound keeps the last
        // chunk short.
        let chunk_cap = if self.config.snapshot_chunk_bytes == 0 {
            total
        } else {
            self.config.snapshot_chunk_bytes
        };
        let end = start.saturating_add(chunk_cap).min(total);
        let chunk = data[start..end].to_vec();
        // `done` exactly when this chunk reaches the end of the snapshot (the empty
        // end-of-stream case `start == end == total` is `done` too, so a zero-length
        // snapshot installs in a single empty `done` chunk).
        let done = end >= total;

        out.send(
            peer,
            RaftMsg::InstallSnapshot {
                term,
                leader_id: self.id,
                last_included_index: meta.last_included_index,
                last_included_term: meta.last_included_term,
                offset: start as u64,
                data: chunk,
                done,
                voters,
                learners,
            },
        );
    }

    /// Advance the leader's `commit_index` under the section-5.4.2 rule (THE
    /// Figure-8 safety rule). Find the highest index `N > commit_index` such that:
    ///
    /// - a MAJORITY of voters (counting the leader itself, whose whole log is
    ///   trivially replicated) have `match_index >= N`, AND
    /// - `log[N].term == currentTerm`.
    ///
    /// The second clause is the crux of 5.4.2: a leader NEVER commits an entry from
    /// a PRIOR term by counting replicas, because a later leader could still
    /// overwrite a prior-term entry that is merely present on a majority (Figure 8).
    /// A prior-term entry becomes committed only TRANSITIVELY: once a current-term
    /// entry above it reaches a majority and commits, every entry below it is
    /// committed by the Log Matching Property. Advancing commit then drives apply.
    fn maybe_advance_commit(&mut self, out: &mut Effects) {
        if self.role != Role::Leader {
            return;
        }
        let current_term = self.storage.current_term();
        let last = self.storage.last_log_index();
        // HA-3d: the majority is over the CURRENT-CONFIG VOTER SET (the latest config in
        // this leader's log), counting NEITHER learners NOR a self that is no longer a
        // voter (a leader mid-self-removal). With a static voter set this is exactly
        // `voters.len()/2+1` as before, so the default path is byte-identical.
        let majority = self.voters.len() / 2 + 1;
        // Does the leader count itself? Only if it is still a voter in the current config
        // (a leader that appended `RemoveVoter(self)` is not, and must not count itself
        // toward the very entry that removes it). It always holds its whole log, so when
        // it IS a voter it replicates every N <= last.
        let self_is_voter = self.voters.contains(&self.id);

        // Scan from the highest index downward; the first N that satisfies both
        // clauses is the new commit index (commit is monotone, so a higher N
        // dominates). Stop at commit_index + 1 (no point re-confirming what is
        // already committed).
        let mut new_commit = self.commit_index;
        let mut n = last;
        while n > self.commit_index {
            // Clause 2 (5.4.2): only current-term entries are committable by count.
            if self.storage.term_at(n) == current_term {
                // Count CURRENT-CONFIG VOTERS with match_index >= n. The leader counts
                // itself iff it is a voter; each peer counts iff it is a voter (learners
                // are excluded) AND its tracked match_index >= n.
                let mut replicated = u64::from(self_is_voter);
                for (&peer, &mi) in &self.match_index {
                    if self.voters.contains(&peer) && mi >= n {
                        replicated += 1;
                    }
                }
                if usize::try_from(replicated).unwrap_or(usize::MAX) >= majority {
                    new_commit = n;
                    break;
                }
            }
            n -= 1;
        }

        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            // HA-prod-commit-ack: notify the adapter that the committed high-water rose,
            // so it can resolve a parked propose ack on TRUE COMMIT. Additive, no I/O.
            out.note_committed_through(new_commit);
            self.apply_committed();
        }

        // HA-3d LEADER SELF-REMOVAL STEP-DOWN (Raft section 6): a leader that has
        // committed a `RemoveVoter(self)` is no longer part of the cluster and must yield.
        // The entry that removes it is committed iff the COMMITTED config (baseline +
        // ConfigChange entries up to commit_index) excludes self. We check after advancing
        // commit above, so the removal step-down fires exactly when its entry commits --
        // never before (it could otherwise step down while still needed to commit the very
        // removal entry on the new-config majority). With a static cluster the leader is
        // always in its committed voters, so this never fires (default path unchanged).
        if self.role == Role::Leader && !self.committed_config_contains_self() {
            self.step_down_to_follower(out);
            // Re-arm the election timer like any step-down: the node is now a plain
            // follower (out of the cluster, but a leftover follower until the adapter
            // tears it down), and must not sit with no timer armed.
            self.arm_election_timer_now(out);
        }
    }

    /// The CONFIGURATION (voter set, learner set) AS OF a given log index (HA-3d): the
    /// persisted [`config_baseline`](RaftNode::config_baseline) with every
    /// [`EntryPayload::ConfigChange`] entry whose index is `<= index` replayed in log order
    /// on top. Scans only the surviving log tail (the compacted-away prefix's deltas are
    /// already folded into the baseline), so it never reads a compacted index. Indices
    /// above `last_log_index` simply replay the whole surviving log; indices at or below
    /// the snapshot boundary replay nothing and return the baseline unchanged.
    ///
    /// This is the single source of truth for "what config did index N produce", used both
    /// to bound the COMMITTED config (`config_at(commit_index)`) and to capture the
    /// committed config at a snapshot point (`config_at(last_included_index)`), so the two
    /// callers can never disagree on the fold.
    fn config_at(&self, index: u64) -> (BTreeSet<NodeId>, BTreeSet<NodeId>) {
        let (mut voters, mut learners) = self.config_baseline.clone();
        let start = self.storage.log_start_index();
        let last = self.storage.last_log_index().min(index);
        let mut idx = start;
        while idx <= last {
            if let Some(entry) = self.storage.entry_at(idx) {
                if let EntryPayload::ConfigChange(change) = entry.payload {
                    Self::apply_membership_delta(&mut voters, &mut learners, change);
                }
            }
            idx += 1;
        }
        (voters, learners)
    }

    /// Whether THIS node is a voter in the COMMITTED configuration (HA-3d): the baseline
    /// plus every `ConfigChange` entry at-or-below `commit_index`. Used for the
    /// leader-self-removal step-down, which must fire only once the removing entry has
    /// COMMITTED (the live `self.voters` already excludes self at APPEND time, which is too
    /// early to step down). Delegates to [`config_at`](RaftNode::config_at) bounded at
    /// `commit_index`, so it scans only the surviving committed log tail.
    fn committed_config_contains_self(&self) -> bool {
        self.config_at(self.commit_index).0.contains(&self.id)
    }

    /// Arm the election timer with NO rng (HA-3d step-down convenience). The leader
    /// self-removal step-down happens inside `maybe_advance_commit`, which (matching the
    /// other commit-path callers) has no `RaftRng` handle; arming WITHOUT jitter here is
    /// safe because this node has just left the cluster and its election timing no longer
    /// affects consensus (it is being torn down by the adapter). Uses the base timeout, no
    /// random draw, so it perturbs no RNG stream. The normal jittered arm
    /// ([`arm_election_timer`]) is used everywhere a real election can result.
    fn arm_election_timer_now(&self, out: &mut Effects) {
        out.set_timer(ELECTION_TIMEOUT, self.config.election_timeout_base);
    }

    /// The apply pipeline (Figure 2, "All Servers": if `commitIndex > lastApplied`,
    /// increment `lastApplied` and apply `log[lastApplied]` to the state machine).
    ///
    /// 3e makes this the REAL apply: for each newly-committed index
    /// `last_applied+1..=commit_index`, fetch the full [`LogEntry`] from storage and
    /// hand it to [`StateMachine::apply`] (with the default [`CountingSm`] that is
    /// the 3b sink; with the config state machine it drives the
    /// `SlotMap`). The `applied_count` witness is kept
    /// in lockstep so a test can still prove the hook ran. Idempotent and monotone:
    /// `last_applied` never exceeds `commit_index` and never moves backward, so an
    /// entry is applied EXACTLY ONCE, which is what keeps the state machine a
    /// faithful image of the committed prefix.
    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            // Fetch the committed entry to apply. It MUST exist: `commit_index` never
            // exceeds the leader-vouched / majority-replicated last index, so every
            // index up to it is present in this node's log. A missing entry would be a
            // commit-bookkeeping bug; surface it loudly rather than silently skipping.
            let entry = self
                .storage
                .entry_at(next)
                .expect("a committed index must have a log entry to apply");
            self.last_applied = next;
            self.sm.apply(&entry);
            // Keep the apply witness in lockstep with the state machine (it counted
            // every applied entry in 3b; it still does, regardless of `M`).
            self.applied_count += 1;
        }
        // Raft section 7: once the applied prefix is large enough, snapshot + compact.
        self.maybe_compact();
    }

    /// THE COMPACTION TRIGGER (Raft section 7). When the number of log entries ABOVE
    /// the last snapshot exceeds [`RaftConfig::snapshot_threshold`] AND there is newly
    /// applied state to snapshot (`last_applied` is above the snapshot's
    /// `last_included_index`), serialize the state machine at `last_applied`, persist
    /// the snapshot, and compact the log to `last_applied`.
    ///
    /// DETERMINISM: this reads no clock and no RNG and emits no [`Effects`]; the
    /// decision is a pure function of the log length, the applied prefix, and the
    /// constant threshold, so two nodes (or two replays) driven through the SAME
    /// committed sequence compact at the SAME points. It is therefore safe inside the
    /// engine step and does not perturb the DST trace (which compares Effects, not
    /// storage internals); with the default `snapshot_threshold == 0` it never fires,
    /// so every existing scenario is byte-identical.
    ///
    /// SAFETY: it snapshots only `last_applied`, which is `<= commit_index`, i.e. a
    /// COMMITTED, durable prefix; compacting at-or-below it can never drop an entry the
    /// node still needs to apply, and the snapshot it leaves is a faithful image of a
    /// committed prefix.
    fn maybe_compact(&mut self) {
        let threshold = self.config.snapshot_threshold;
        if threshold == 0 {
            // Compaction disabled (the default): the log grows unbounded as before.
            return;
        }
        let snap_index = self
            .storage
            .load_snapshot()
            .map_or(0, |(meta, _)| meta.last_included_index);
        // Nothing new applied since the last snapshot: nothing to compact to.
        if self.last_applied <= snap_index {
            return;
        }
        // Entries currently above the last snapshot. The compaction point is the
        // APPLIED watermark, so we only ever compact committed-and-applied state.
        let above_snapshot = self.last_applied - snap_index;
        if above_snapshot <= threshold {
            return;
        }
        // Snapshot the state machine at `last_applied` and persist it with the matching
        // meta, then drop the now-redundant log prefix. `term_at(last_applied)` is read
        // BEFORE the compaction so the snapshot records the correct last_included_term
        // (after compaction the storage answers that term from the snapshot meta).
        let last_included_index = self.last_applied;
        let last_included_term = self.storage.term_at(last_included_index);
        let data = self.sm.snapshot();
        // HA-3d: persist the CONFIG BASELINE alongside the snapshot. The compacted-away
        // prefix folds its `ConfigChange` deltas into this baseline, so a restart restores
        // the config as `baseline + surviving-log ConfigChange entries`. The baseline is
        // the COMMITTED config AS OF the compaction point, NOT the live `self.voters`:
        // `self.voters` is derived from the ENTIRE log including UNCOMMITTED `ConfigChange`
        // entries above `last_applied` (append-time adoption), and if such an uncommitted
        // entry is later TRUNCATED (it was on a deposed leader's log) a baseline that folded
        // it could never un-adopt it -- the baseline is the recompute floor. We therefore
        // capture `config_at(last_included_index)`; since `last_included_index ==
        // last_applied <= commit_index`, this is exactly the COMMITTED config at the
        // snapshot point. Update the in-memory baseline to match (future recomputes scan
        // only the surviving tail on top of it).
        let baseline = self.config_at(last_included_index);
        self.storage.save_config_baseline(&baseline.0, &baseline.1);
        self.config_baseline = baseline;
        self.storage.save_snapshot(
            SnapshotMeta {
                last_included_index,
                last_included_term,
            },
            &data,
        );
        self.storage.compact_to(last_included_index);
    }

    /// INSTALLSNAPSHOT receiver (Raft section 7 / Figure 13), PROD-9 CHUNKED. A leader is
    /// shipping its snapshot in bounded chunks because the entries this follower needs were
    /// already compacted on the leader.
    ///
    /// Order of operations (mirrors the other receivers):
    /// 1. "All Servers": adopt a strictly greater term (step down, clear vote), via
    ///    [`observe_term`], exactly like AppendEntries / RequestVote.
    /// 2. Reply false-by-term if `term < currentTerm` (a stale leader): just reply our
    ///    higher term so the stale leader steps down; do NOT install or reset the timer.
    /// 3. `term == currentTerm`: a legitimate leader. Concede candidacy, reset the
    ///    election timer (we heard from the leader), record the leader.
    /// 4. CHUNK ASSEMBLY (Figure 13's `offset` / `done`): on `offset == 0` (re)start the
    ///    receive buffer with the chunk's snapshot meta; on a later chunk, accept it ONLY if
    ///    its `offset` equals our accumulated length AND its meta matches the buffer (else
    ///    reject and reply the offset we next expect, so the leader restarts / continues).
    ///    Append the chunk's bytes. If `done == false`, reply our accumulated length and
    ///    stop (no install yet). Only on `done` do we proceed to install the assembled
    ///    snapshot -- a PARTIAL transfer is NEVER installed.
    /// 5. If the assembled snapshot does not advance us (`last_included_index <=
    ///    commit_index`), it is stale/duplicate: discard the buffer, reply our term and stop
    ///    (never move backward).
    /// 6. Otherwise INSTALL ([`install_assembled_snapshot`](RaftNode::install_assembled_snapshot)):
    ///    persist the snapshot, restore the state machine, discard the log up to
    ///    `last_included_index` (keeping a longer VALID suffix per the paper), adopt the
    ///    config baseline, set `commit_index`/`last_applied`, then reply `installed`.
    ///
    /// FORWARD-ONLY SAFETY: a leader snapshots only its applied = committed prefix, so
    /// `last_included_index` is COMMITTED. Installing it moves this follower FORWARD to a
    /// committed prefix and can never overwrite a different committed entry at the same
    /// index (State-Machine-Safety), because two leaders can never commit conflicting
    /// entries at one index. The install is ATOMIC and happens ONLY on `done` after a
    /// contiguous `0..len` accumulation, so a dropped / reordered / duplicated chunk cannot
    /// install a half-received snapshot (the offset check rejects it).
    #[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
    fn on_install_snapshot(
        &mut self,
        now: Monotonic,
        rng: &mut dyn RaftRng,
        args: InstallSnapshotArgs,
        out: &mut Effects,
    ) {
        let InstallSnapshotArgs {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            offset,
            data,
            done,
            voters,
            learners,
        } = args;
        self.observe_term(term, rng, out);
        let current = self.storage.current_term();

        if term < current {
            // Stale leader: reply our higher term so it steps down; do not install or buffer.
            // A non-installed reply (the stale leader steps down on our higher term first, so
            // the offset is inert); `next_offset == 0` since we kept no buffer for it.
            out.send(
                leader_id,
                RaftMsg::InstallSnapshotResp {
                    term: current,
                    last_included_index,
                    installed: false,
                    next_offset: 0,
                },
            );
            return;
        }

        // term == current: a legitimate leader. A candidate concedes; reset the timer
        // (we heard from the leader) and record it - the same recognize-leader path
        // AppendEntries uses.
        if self.role != Role::Follower {
            self.step_down_to_follower(out);
        }
        self.arm_election_timer(rng, out);
        self.leader_id = Some(leader_id);
        // Leader-stickiness / pre-vote freshness: an InstallSnapshot is also proof of a live
        // current-term leader (the same recognize-leader path AppendEntries uses), so stamp
        // last-contact and abort any in-flight pre-vote round. Inert with pre-vote off.
        self.last_leader_contact = Some(now);
        self.pre_votes = None;
        // PROGRESS: hearing a live current-term leader resets the mixed-version fallback
        // counter (etcd #8525), the same as the AppendEntries recognize-leader path.
        self.failed_pre_vote_rounds = 0;

        // PROD-9 CHUNK ASSEMBLY (Figure 13). The first chunk (`offset == 0`) (re)starts a
        // fresh buffer keyed by THIS snapshot's meta; a later chunk extends the buffer iff it
        // arrives contiguously (its `offset` equals our accumulated length) AND its meta
        // matches the buffer. A mismatch (a reordered / duplicated / stale-snapshot chunk) is
        // rejected: we keep our buffer untouched and reply the offset we next expect so the
        // leader retransmits from the right place. This is what makes a partial transfer
        // unobservable -- the install runs only on the FINAL chunk of a contiguous run.
        if offset == 0 {
            // A fresh first chunk supersedes any prior partial buffer (a leader change or a
            // restarted transfer). Seed the buffer from this chunk's meta + bytes.
            self.snapshot_rx = Some(SnapshotRx {
                last_included_index,
                last_included_term,
                voters,
                learners,
                data,
            });
        } else {
            // A continuation chunk. It must match an in-flight buffer for the SAME snapshot
            // and land exactly at the accumulated length; otherwise reject + steer the leader.
            let accepted = match self.snapshot_rx.as_mut() {
                Some(rx)
                    if rx.last_included_index == last_included_index
                        && rx.last_included_term == last_included_term
                        && offset == rx.data.len() as u64 =>
                {
                    rx.data.extend_from_slice(&data);
                    true
                }
                _ => false,
            };
            if !accepted {
                // Reject: reply the offset we actually expect (the buffer's length, or 0 when
                // we hold no buffer for this snapshot) so the leader restarts / continues
                // correctly. No marker advances on a non-installed reply.
                let expect = self
                    .snapshot_rx
                    .as_ref()
                    .filter(|rx| {
                        rx.last_included_index == last_included_index
                            && rx.last_included_term == last_included_term
                    })
                    .map_or(0, |rx| rx.data.len() as u64);
                out.send(
                    leader_id,
                    RaftMsg::InstallSnapshotResp {
                        term: current,
                        last_included_index,
                        installed: false,
                        next_offset: expect,
                    },
                );
                return;
            }
        }

        // The chunk was buffered. If this is NOT the final chunk, ack our accumulated length
        // so the leader sends the next chunk from there; do NOT install yet.
        if !done {
            let received = self
                .snapshot_rx
                .as_ref()
                .map_or(0, |rx| rx.data.len() as u64);
            out.send(
                leader_id,
                RaftMsg::InstallSnapshotResp {
                    term: current,
                    last_included_index,
                    installed: false,
                    next_offset: received,
                },
            );
            return;
        }

        // FINAL chunk: the whole snapshot is now assembled in the buffer. Take it out
        // (consuming the buffer) and install atomically. A stale / duplicate complete
        // snapshot that does not advance our committed prefix is discarded, not installed.
        let rx = self
            .snapshot_rx
            .take()
            .expect("the buffer was just seeded / extended for this chunk");

        if rx.last_included_index <= self.commit_index {
            // Stale / duplicate: never move backward. We provably hold at least that prefix
            // (it is `<= commit_index`), so the leader advancing our markers to it is honest.
            out.send(
                leader_id,
                RaftMsg::InstallSnapshotResp {
                    term: current,
                    last_included_index: rx.last_included_index,
                    installed: true,
                    next_offset: 0,
                },
            );
            return;
        }

        // INSTALL the fully-received snapshot atomically (the same install the pre-PROD-9
        // whole-snapshot path ran, now fed from the reassembled buffer).
        self.install_assembled_snapshot(&rx, out);

        // ECHO the installed index (Figure 13): the leader advances our markers from exactly
        // what we installed, never from its own (possibly newer) snapshot meta.
        out.send(
            leader_id,
            RaftMsg::InstallSnapshotResp {
                term: current,
                last_included_index: rx.last_included_index,
                installed: true,
                next_offset: 0,
            },
        );
    }

    /// PROD-9: ATOMICALLY install a fully-received snapshot (the body the pre-chunking
    /// whole-snapshot path ran, now fed from a reassembled [`SnapshotRx`]). Called ONLY on
    /// the final chunk of a contiguous transfer whose `last_included_index > commit_index`,
    /// so it always moves the follower FORWARD. Persists the snapshot, restores the state
    /// machine, compacts the log (keeping a longer valid suffix per Figure 13 step 6), adopts
    /// the config baseline, and advances the committed / applied watermarks. Reassembling the
    /// SAME bytes and installing them here is byte-identical to installing the whole snapshot
    /// in one message, so chunk count never changes the installed state.
    fn install_assembled_snapshot(&mut self, rx: &SnapshotRx, out: &mut Effects) {
        let SnapshotRx {
            last_included_index,
            last_included_term,
            voters,
            learners,
            data,
        } = rx;
        let last_included_index = *last_included_index;
        let last_included_term = *last_included_term;

        // KEEP A VALID LONGER SUFFIX (Raft Figure 13 step 6): if our log already holds
        // an entry AT last_included_index whose term matches last_included_term, the
        // snapshot is a prefix of our log - keep the tail above it. Otherwise our tail
        // conflicts with the snapshot's committed prefix, so discard the WHOLE log (it
        // will be re-replicated from the leader above the snapshot).
        let suffix_valid = self.storage.term_at(last_included_index) == last_included_term;
        if !suffix_valid {
            // Drop the entire current log; the snapshot subsumes everything and the leader
            // re-ships the post-snapshot tail.
            self.storage.truncate_from(self.storage.log_start_index());
        }

        // Persist the snapshot FIRST so the storage answers term_at(last_included_index)
        // from the snapshot meta, then COMPACT the log to the snapshot index in BOTH
        // branches. The compaction is what advances the storage's log-start boundary to
        // `last_included_index + 1`: in the valid-suffix branch it drops the redundant
        // prefix and keeps the tail; in the discard branch (log already empty) it just
        // sets the boundary so the leader's next AppendEntries appends at the right index
        // and the apply loop never reads a compacted index. After this the next
        // AppendEntries (prev = last_included_index) passes its consistency check.
        self.storage.save_snapshot(
            SnapshotMeta {
                last_included_index,
                last_included_term,
            },
            data,
        );
        self.storage.compact_to(last_included_index);
        self.sm.restore(data);

        // HA-3d: ADOPT the config baseline the snapshot reflects. The snapshot subsumed the
        // compacted prefix's `ConfigChange` entries, so the follower's truncated log can no
        // longer rebuild the configuration alone; the leader shipped the committed baseline.
        // Persist it and recompute the live config as `baseline + surviving-tail
        // ConfigChange entries`. Empty sets (a static / pre-3d cluster) leave the config
        // governed by the constructor's voter set, so this is config-inert there.
        self.config_baseline = (voters.clone(), learners.clone());
        self.storage.save_config_baseline(voters, learners);
        self.recompute_config_from_log();

        // Advance the committed / applied watermarks to the snapshot's index (it is a
        // committed prefix). Forward-only: the caller entered this path only because
        // last_included_index > commit_index.
        self.commit_index = last_included_index;
        self.last_applied = last_included_index;
        // HA-prod-commit-ack: the snapshot install raised the committed high-water, so
        // record it for the adapter (additive, no I/O). Uniform with the other two
        // commit-advance sites; a snapshot-installing follower holds no local propose
        // ack, but the record is emitted at every site for a single drain path.
        out.note_committed_through(last_included_index);
    }

    /// INSTALLSNAPSHOT response handler (Raft section 7 / Figure 13), PROD-9 CHUNKED.
    /// Mirrors the AppendEntries response handler's leader bookkeeping, now also driving the
    /// per-follower chunk progress.
    ///
    /// 1. "All Servers": a higher-term reply steps the leader down (return then).
    /// 2. Only a `Leader` in the SAME term cares (a stale reply is ignored).
    /// 3. CHUNK PROGRESS (`installed == false`): the follower buffered a non-final chunk (or
    ///    rejected one). It reported the byte offset it next expects (`next_offset`); record
    ///    it and send the NEXT chunk from there. No replication marker advances (the follower
    ///    has not yet installed anything).
    /// 4. INSTALL (`installed == true`): the follower has now installed the snapshot, so it
    ///    holds everything up to the index it ECHOED (`installed_index`): advance
    ///    `match_index`/`next_index` past it (MAX-guarded so a reordered older reply cannot
    ///    rewind), drop the per-follower chunk progress (the transfer is done), then resume
    ///    AppendEntries for the post-snapshot tail and try to advance commit.
    ///
    /// FALSE-COMMIT SAFETY (Figure 13): on install the marker advance reads the follower's
    /// ECHOED `installed_index`, NOT this leader's CURRENT `load_snapshot()` meta. If the
    /// leader compacted AGAIN (to a higher `K'`) while this `InstallSnapshot(K)` was in
    /// flight, reading the current meta would set `match_index[from] = K' > K` -- claiming
    /// the follower holds entries it never installed -- and `maybe_advance_commit` could then
    /// commit an index that is NOT on a majority (a lost-committed-entry hazard on a later
    /// leader change). Echoing `K` keeps the marker honest. A non-`installed` chunk reply
    /// advances NO marker, so it can never over-commit.
    #[allow(clippy::too_many_arguments)]
    fn on_install_snapshot_resp(
        &mut self,
        rng: &mut dyn RaftRng,
        from: NodeId,
        term: u64,
        installed_index: u64,
        installed: bool,
        next_offset: u64,
        out: &mut Effects,
    ) {
        if self.observe_term(term, rng, out) {
            // We stepped down on a higher term; we are no longer leader.
            return;
        }
        if self.role != Role::Leader || term != self.storage.current_term() {
            // Stale reply (old term) or we are not leader: ignore.
            return;
        }
        if !installed {
            // PROD-9: a buffered-but-incomplete (or rejected) chunk. The follower told us the
            // offset it next expects; advance our per-follower progress to it and ship the
            // next chunk from there. Advance NO replication marker (nothing is installed yet).
            // Only track if a chunked transfer is actually in flight for this peer (a leader
            // that no longer needs to snapshot this peer has no entry); the `entry` API would
            // otherwise resurrect a stale one, so guard on presence.
            if let Some(slot) = self.snapshot_next_offset.get_mut(&from) {
                *slot = next_offset;
                self.send_snapshot_chunk_to(from, next_offset, out);
            }
            return;
        }
        // INSTALLED: the follower installed the snapshot at `installed_index` (the value it
        // echoed from our request), so it now holds the prefix up to there - and ONLY up to
        // there, regardless of any LATER compaction on this leader. The chunked transfer is
        // complete, so drop its per-follower progress. Advance its markers to the echoed
        // index (MAX-guarded against a reordered older reply), then continue with the tail.
        self.snapshot_next_offset.remove(&from);
        let m = self.match_index.entry(from).or_insert(0);
        *m = (*m).max(installed_index);
        let mi = *m;
        self.next_index.insert(from, mi + 1);
        self.maybe_advance_commit(out);
        // Resume normal replication of the entries above the snapshot at once.
        self.send_append_entries_to(from, out);
    }

    /// Accept a client proposal on a leader: append an opaque entry at the current
    /// term and replicate it (Figure 2, "Leaders": on a client command, append to
    /// the local log, then replicate). Returns the new entry's index on success, or
    /// `None` if this node is not the leader (the caller should redirect to the
    /// leader; 3b carries no redirect hint, the `None` IS the redirect signal).
    ///
    /// A single-voter cluster commits the entry immediately (the leader alone is a
    /// majority), which `maybe_advance_commit` handles.
    pub fn propose(
        &mut self,
        payload: EntryPayload,
        now: Monotonic,
        rng: &mut dyn RaftRng,
        out: &mut Effects,
    ) -> Option<u64> {
        let _ = (now, rng);
        if self.role != Role::Leader {
            return None;
        }
        let is_config_change = matches!(payload, EntryPayload::ConfigChange(_));
        // HA-3d ONE-CHANGE-IN-FLIGHT (Raft section 6): a leader refuses a new membership
        // change while a previous one is still uncommitted, because two overlapping
        // configuration transitions could yield disjoint majorities. Enforced HERE so it
        // holds no matter which entry point proposed the change (the direct
        // `propose_membership_change`, or a `RaftMsg::Propose` carrying a `ConfigChange`).
        if is_config_change && self.membership_change_in_flight() {
            return None;
        }
        let index = self.storage.last_log_index() + 1;
        let term = self.storage.current_term();
        self.storage.append(LogEntry {
            term,
            index,
            payload,
        });
        // HA-3d APPEND-TIME ADOPTION (Raft section 6): if this is a membership change,
        // the leader adopts the new configuration NOW (on append, not on commit) and
        // counts subsequent quora over it. Recompute before initializing replication
        // state below so a freshly-added voter / learner is replicated to at once.
        if is_config_change {
            self.recompute_config_from_log();
            // A newly-introduced peer (voter or learner) has no replication markers yet;
            // seed them so the very next broadcast ships it the log from the start. An
            // existing peer keeps its markers (the entry() guards do not overwrite).
            let next = self.storage.last_log_index() + 1;
            for &peer in self.voters.iter().chain(self.learners.iter()) {
                if peer != self.id {
                    self.next_index.entry(peer).or_insert(next);
                    self.match_index.entry(peer).or_insert(0);
                }
            }
        }
        // Replicate at once so a quiet cluster does not wait a heartbeat interval,
        // and so a single-voter leader's own append commits immediately.
        self.broadcast_heartbeat(out);
        self.maybe_advance_commit(out);
        Some(index)
    }

    /// Arm (reset) the election timer with a fresh randomized timeout in
    /// `[base, base + jitter)` (section 5.2). Drawing on every arm is what makes
    /// split votes self-resolve.
    fn arm_election_timer(&self, rng: &mut dyn RaftRng, out: &mut Effects) {
        let base = self.config.election_timeout_base;
        let jitter_ms =
            u64::try_from(self.config.election_timeout_jitter.as_millis()).unwrap_or(u64::MAX);
        let extra = rng.gen_below(jitter_ms);
        let after = base.saturating_add(Duration::from_millis(extra));
        out.set_timer(ELECTION_TIMEOUT, after);
    }

    /// The up-to-date comparison (section 5.4.1): the candidate's log is at least
    /// as up-to-date as ours iff its last entry has a HIGHER term, or the SAME term
    /// and an index `>=` ours.
    fn candidate_log_up_to_date(&self, cand_last_index: u64, cand_last_term: u64) -> bool {
        let my_term = self.storage.last_log_term();
        let my_index = self.storage.last_log_index();
        if cand_last_term == my_term {
            // Same last-entry term: the longer (>= index) log is at least as
            // up-to-date.
            cand_last_index >= my_index
        } else {
            // Different last-entry terms: the higher term is more up-to-date.
            cand_last_term > my_term
        }
    }

    /// The MINIMUM election timeout (the base, the floor before any jitter). This is the
    /// staleness window the leader-stickiness and check-quorum refinements measure against:
    /// "fresh" means "within one minimum election timeout". Using the BASE (not base +
    /// jitter) is the conservative choice -- a leader-contact older than the base could have
    /// let SOME follower (one drawing minimal jitter) time out, so the base is the right
    /// freshness bound for both directions of the disruptive-server fix.
    #[inline]
    fn min_election_timeout(&self) -> Duration {
        self.config.election_timeout_base
    }

    /// Whether this node has heard from a VALID CURRENT LEADER within the minimum election
    /// timeout of `now` (LEADER-STICKINESS, Ongaro section 4.2.3 / 9.6). Used to refuse a
    /// (pre-)vote when a leader is fresh: an election is pointless and a disruptive server
    /// must not force one. `None` last-contact (a node that has never heard from a leader,
    /// e.g. at first boot) is NOT fresh, so the very first election is never blocked.
    fn leader_is_fresh(&self, now: Monotonic) -> bool {
        match self.last_leader_contact {
            Some(when) => now.saturating_duration_since(when) < self.min_election_timeout(),
            None => false,
        }
    }

    /// Record a granted vote from `voter` while a `Candidate` in `term`, then
    /// promote to leader if the tally is now a majority. Only same-term votes for a
    /// live candidacy count. Idempotent per voter (the tally is a `BTreeSet`), so a
    /// duplicated `RequestVoteResp` cannot inflate the count.
    fn record_vote(&mut self, now: Monotonic, voter: NodeId, term: u64, out: &mut Effects) {
        if self.role != Role::Candidate || term != self.storage.current_term() {
            return;
        }
        self.votes.insert(voter);
        self.maybe_become_leader(now, out);
    }

    // -- HA-3d membership (Raft section 6) ----------------------------------

    /// RECOMPUTE the live configuration (voter set + learner set) from the log (HA-3d,
    /// Raft section 6 APPEND-TIME ADOPTION). The config is the persisted
    /// [`config_baseline`](RaftNode::config_baseline) with every surviving
    /// [`EntryPayload::ConfigChange`] delta replayed in log order on top. Called after
    /// EVERY log mutation that can add or remove a `ConfigChange` entry (a leader's own
    /// append, a follower's append / conflict-truncation, a snapshot install), so a node
    /// adopts a new configuration the instant the entry lands in its log -- BEFORE it is
    /// committed. This is the section-6 rule that makes single-server changes safe: the
    /// leader counts votes and commit-quora over the LATEST config in its OWN log.
    ///
    /// Scans only the SURVIVING log (above any snapshot); the compacted-away prefix's
    /// deltas are already folded into the baseline, so this never reads a compacted entry.
    /// With no `ConfigChange` entries anywhere the result equals the baseline, which for a
    /// static-membership cluster is the constructor's voter set -- so the default path is
    /// byte-identical (this only ever shrinks/grows the sets when a `ConfigChange` exists).
    fn recompute_config_from_log(&mut self) {
        let (mut voters, mut learners) = self.config_baseline.clone();
        let start = self.storage.log_start_index();
        let last = self.storage.last_log_index();
        let mut idx = start;
        while idx <= last {
            if let Some(entry) = self.storage.entry_at(idx) {
                if let EntryPayload::ConfigChange(change) = entry.payload {
                    Self::apply_membership_delta(&mut voters, &mut learners, change);
                }
            }
            idx += 1;
        }
        self.voters = voters;
        self.learners = learners;
    }

    /// Apply ONE [`MembershipChange`] delta to a `(voters, learners)` pair (HA-3d). Pure
    /// and total: each variant is a single-server add or remove, and a node is kept in at
    /// most one of the two sets (PromoteLearner removes from learners then adds to
    /// voters). Idempotent re-application yields the same sets, so replaying the same log
    /// prefix twice (a restart, a re-derive after truncation) converges identically.
    fn apply_membership_delta(
        voters: &mut BTreeSet<NodeId>,
        learners: &mut BTreeSet<NodeId>,
        change: MembershipChange,
    ) {
        match change {
            MembershipChange::AddVoter(node) => {
                // A direct voter add: also ensure it is not lingering as a learner.
                learners.remove(&node);
                voters.insert(node);
            }
            MembershipChange::RemoveVoter(node) => {
                voters.remove(&node);
            }
            MembershipChange::AddLearner(node) => {
                // ENGINE-AUTHORITATIVE NO-DEMOTE (F3): an AddLearner must NEVER demote a current
                // VOTER to a non-voting learner (that would silently SHRINK the quorum). If `node`
                // is already a voter, this is a NO-OP -- the voter stays a voter. Only a node that is
                // neither a voter NOR already a learner is staged as a new learner. The production
                // paths (the serve `apply_membership_intent` guard, the auto-promote driver) already
                // never feed an existing voter here, so this is BYTE-IDENTICAL on every reachable
                // input; it makes the no-demote invariant a property of the engine itself rather than
                // resting solely on the callers. Still pure / total / idempotent (re-applying yields
                // the same sets).
                if !voters.contains(&node) {
                    learners.insert(node);
                }
            }
            MembershipChange::PromoteLearner(node) => {
                // The catch-up phase is over: move it from learners into voters.
                learners.remove(&node);
                voters.insert(node);
            }
            MembershipChange::RemoveLearner(node) => {
                // Drop a non-voting learner. Never counted in a quorum, so always safe.
                learners.remove(&node);
            }
        }
    }

    /// Is there a membership change CURRENTLY IN FLIGHT (appended but not yet committed)?
    /// (HA-3d, the section-6 ONE-CHANGE-IN-FLIGHT rule.) A leader refuses to propose a new
    /// `ConfigChange` while one is uncommitted, because two overlapping configuration
    /// transitions could produce disjoint majorities (the very hazard single-server
    /// changes otherwise avoid). True iff any `ConfigChange` entry sits ABOVE the
    /// committed watermark in the surviving log.
    fn membership_change_in_flight(&self) -> bool {
        let start = self.storage.log_start_index().max(self.commit_index + 1);
        let last = self.storage.last_log_index();
        let mut idx = start;
        while idx <= last {
            if let Some(entry) = self.storage.entry_at(idx) {
                if matches!(entry.payload, EntryPayload::ConfigChange(_)) {
                    return true;
                }
            }
            idx += 1;
        }
        false
    }

    /// Whether `learner` has caught up enough to be promoted to a voter (HA-3d,
    /// [`LEARNER_CATCHUP_LAG`]). Only meaningful on a leader, which tracks each peer's
    /// `match_index`. A learner is "caught up" when its `match_index` is within
    /// [`LEARNER_CATCHUP_LAG`] of the leader's last log index. Returns false if the node
    /// is not a known learner or this node is not the leader.
    ///
    /// ADVISORY ONLY. This is a query the (future) production driver consults BEFORE
    /// proposing [`MembershipChange::PromoteLearner`]; the engine does NOT enforce it.
    /// `propose_membership_change(PromoteLearner(..))` succeeds at ANY lag, because a
    /// promotion is always SAFE (a new voter never breaks election safety) -- promoting a
    /// far-behind learner merely stalls commit briefly until the new voter catches up, since
    /// the now-larger quorum includes a lagging member. So this gate is a liveness hint, not
    /// a safety precondition, and skipping it cannot corrupt the cluster.
    #[must_use]
    pub fn learner_caught_up(&self, learner: NodeId) -> bool {
        if self.role != Role::Leader || !self.learners.contains(&learner) {
            return false;
        }
        let last = self.storage.last_log_index();
        let mi = self.match_index.get(&learner).copied().unwrap_or(0);
        mi + LEARNER_CATCHUP_LAG >= last
    }

    /// The current VOTER set (HA-3d), for test inspection. Derived from the log.
    #[must_use]
    pub fn voters(&self) -> &BTreeSet<NodeId> {
        &self.voters
    }

    /// The current LEARNER set (HA-3d, non-voting members), for test inspection.
    #[must_use]
    pub fn learners(&self) -> &BTreeSet<NodeId> {
        &self.learners
    }

    /// Whether a membership change is CURRENTLY IN FLIGHT (appended above the committed
    /// watermark but not yet committed): the section-6 ONE-CHANGE-IN-FLIGHT predicate, exposed
    /// READ-ONLY so the production driver can distinguish a one-change-in-flight refusal (retry
    /// after the in-flight change commits) from a genuine not-leader refusal WITHOUT proposing a
    /// doomed entry. Pure observation: it reads the log + commit watermark and changes NO state,
    /// so consulting it cannot perturb consensus and the determinism sweep is byte-identical.
    /// `propose_membership_change` enforces the rule itself regardless of whether the caller
    /// consults this first (the safety guard is in the engine, not the driver).
    #[must_use]
    pub fn has_membership_change_in_flight(&self) -> bool {
        self.membership_change_in_flight()
    }

    /// Propose a single-server membership change on a leader (HA-3d, Raft section 6).
    /// This is a thin wrapper over [`propose`](RaftNode::propose) that ALSO enforces the
    /// ONE-CHANGE-IN-FLIGHT rule: it refuses (returns `None`) if this node is not the
    /// leader OR if a previous membership change is still uncommitted. On success it
    /// appends an [`EntryPayload::ConfigChange`] entry, which (per append-time adoption)
    /// immediately updates THIS leader's voter / learner sets, and replicates it.
    ///
    /// Returns the new entry's 1-based log index, or `None` when the change was refused.
    pub fn propose_membership_change(
        &mut self,
        change: MembershipChange,
        now: Monotonic,
        rng: &mut dyn RaftRng,
        out: &mut Effects,
    ) -> Option<u64> {
        // Delegates to `propose`, which enforces BOTH the leader-only and the section-6
        // one-change-in-flight guards for a `ConfigChange` payload (so the rule holds for
        // every entry point). This wrapper exists as the named, intention-revealing public
        // API for proposing membership and to keep the `ConfigChange`-construction here.
        self.propose(EntryPayload::ConfigChange(change), now, rng, out)
    }
}

// ---------------------------------------------------------------------------
// Sim adapter + DST scenarios (test/dev only).
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
