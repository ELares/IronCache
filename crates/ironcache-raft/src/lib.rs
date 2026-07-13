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

// ---------------------------------------------------------------------------
// Identity, roles, messages, log.
// ---------------------------------------------------------------------------

/// A node's identity in the Raft cluster.
///
/// This is the raft engine's OWN id type, deliberately distinct from
/// [`ironcache_sim::NodeId`]: the engine is transport-agnostic, so the sim
/// adapter (and, later, the production clusterbus adapter) maps between this and
/// whatever id the transport uses. A thin `u64` newtype so ids are `Copy` and
/// totally ordered (the voter set is a `BTreeSet` for deterministic iteration).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

/// A server's role in the current term (Raft section 5.1).
///
/// Every node is in exactly one of these states. The transitions are: a
/// `Follower` whose election timer expires becomes a `Candidate`; a `Candidate`
/// that wins a majority becomes `Leader`; any node that sees a higher term steps
/// down to `Follower`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Passive: responds to leaders and candidates, runs an election timer.
    Follower,
    /// Standing for election in the current term, collecting votes.
    Candidate,
    /// Won the current term; sends heartbeats to maintain authority.
    Leader,
}

/// The payload of a [`LogEntry`].
///
/// [`EntryPayload::Noop`] is the no-op a fresh leader appends to its log on
/// election (so it has a current-term entry it can commit; see the paper's section
/// 8 and the "commit-only-current-term" rule of 5.4.2). [`EntryPayload::Bytes`] is
/// a minimal OPAQUE payload: the engine never interprets these; the tests use them
/// solely to give proposed entries a distinguishable identity when asserting log
/// convergence and state-machine safety. [`EntryPayload::Config`] is the REAL 3e
/// payload: a committed [`ConfigCmd`] that the config [`StateMachine`] applies to
/// the cluster's `SlotMap`, which is what makes Raft
/// the single source of truth for slot ownership (ADR-0027, CONTROL_PLANE.md).
///
/// The engine itself is STILL payload-agnostic FOR `Config`: it commits a `Config`
/// entry by the exact same replication + Figure-8 commit path as any other entry and
/// never looks inside it. Interpretation happens only in [`apply_committed`], which
/// hands each committed entry to the [`StateMachine`] seam; a non-`Config` payload
/// (Noop/Bytes) is a no-op for the config state machine.
///
/// The one payload the engine DOES read is [`EntryPayload::ConfigChange`] (HA-3d Raft
/// section 6 cluster-membership): the voter / learner sets the engine counts quora over
/// are DERIVED FROM THE LOG, adopted on APPEND (not commit). It still replicates and
/// commits by the identical path; the difference is only that appending one recomputes
/// [`RaftNode`]'s configuration. A `ConfigChange` payload is a no-op for the
/// [`StateMachine`] (it governs the Raft voter set, not the slot map).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryPayload {
    /// A leader's election no-op. Carries no data; advances the log index/term.
    Noop,
    /// An opaque client-proposed payload. The engine never interprets the bytes;
    /// they exist so a proposed entry has a distinguishable identity in tests.
    Bytes(Vec<u8>),
    /// A committed CONFIG command (3e): a slot-map / membership / epoch mutation the
    /// config [`StateMachine`] replays onto its `SlotMap`
    /// when the entry is applied. The engine treats this as just another opaque
    /// payload on the replication and commit paths; only the state machine reads it.
    Config(ConfigCmd),
    /// A RAFT CLUSTER-MEMBERSHIP change (HA-3d, Raft section 6): a single-server
    /// addition or removal of a voter, or a learner add / promotion. UNLIKE every
    /// other payload, the engine itself READS this one: the cluster CONFIGURATION (the
    /// voter set the engine counts votes and commits over, plus the learner set it
    /// replicates to but never counts) is DERIVED FROM THE LOG, and a node adopts a new
    /// configuration AS SOON AS THIS ENTRY IS APPENDED TO ITS LOG -- NOT when it commits
    /// (the section-6 rule that makes single-server changes safe, since the old and new
    /// majorities always overlap when adding or removing exactly one server). The
    /// payload still replicates and commits by the identical Figure-8 path as any other
    /// entry; only the moment a node recomputes its [`voters`]/[`learners`] from the log
    /// differs (on append, not on commit). See [`MembershipChange`] and
    /// [`RaftNode::recompute_config_from_log`].
    ///
    /// [`voters`]: RaftNode
    /// [`learners`]: RaftNode
    ConfigChange(MembershipChange),
}

/// A SINGLE-SERVER Raft cluster-membership change (HA-3d, Raft section 6).
///
/// Raft section 6 proves that adding or removing exactly ONE server at a time is safe
/// WITHOUT joint consensus: any majority of the OLD configuration and any majority of
/// the NEW configuration always overlap (they differ by one server), so two leaders
/// can never be elected in the same term across the change. This engine implements
/// THOSE single-server changes (never joint consensus): every variant adds or removes
/// at most one node from the voter or learner set.
///
/// LEARNERS (non-voting, section 6's catch-up phase): a brand-new server is first
/// added as a LEARNER ([`MembershipChange::AddLearner`]). A learner receives
/// AppendEntries / InstallSnapshot and replicates the full log, but is NOT counted in
/// ANY majority (neither an election quorum nor a commit quorum). Once a learner's
/// `match_index` is close enough to the leader (a lag gate, the round-based caught-up
/// check of section 6), the leader proposes [`MembershipChange::PromoteLearner`], which
/// moves it from the learner set INTO the voter set. This staged join avoids a fresh,
/// far-behind server stalling commit by being counted toward a quorum it cannot satisfy.
///
/// The engine NEVER interprets the node ids beyond set membership; the production
/// adapter maps these to real cluster nodes (a follow-up; HA-3d ships the engine + the
/// deterministic-simulation safety gate).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipChange {
    /// Add `0` to the VOTER set (a single-server addition). Counted in every majority
    /// from the moment the entry is appended. Typically a new node is added as a
    /// [`MembershipChange::AddLearner`] first and promoted via
    /// [`MembershipChange::PromoteLearner`]; a direct `AddVoter` is the degenerate
    /// "skip the catch-up phase" form (safe, but it can briefly stall commit if the new
    /// voter is far behind, which is exactly what learners avoid).
    AddVoter(NodeId),
    /// Remove `0` from the VOTER set (a single-server removal). Counted out of every
    /// majority from the moment the entry is appended. A LEADER that removes ITSELF
    /// steps down AFTER this entry commits (section 6: it must first replicate the entry
    /// that removes it to a majority of the NEW configuration, then it is no longer part
    /// of the cluster and yields).
    RemoveVoter(NodeId),
    /// Add `0` to the LEARNER set (a non-voting catch-up member). The leader begins
    /// replicating to it immediately, but it is excluded from every majority until a
    /// later [`MembershipChange::PromoteLearner`] turns it into a voter.
    AddLearner(NodeId),
    /// Promote `0` from the LEARNER set to the VOTER set (the catch-up phase is done).
    /// Equivalent to a `RemoveLearner` + `AddVoter` in one committed delta.
    ///
    /// CATCH-UP IS ADVISORY (HA-3d). The engine ACCEPTS this promotion at ANY lag: a
    /// promotion is always SAFE (a new voter never violates election safety), and promoting
    /// a far-behind learner only briefly stalls commit until it catches up (the larger
    /// quorum now includes a lagging voter). [`RaftNode::learner_caught_up`] is therefore an
    /// ADVISORY gate the engine does not enforce; consulting it before proposing
    /// `PromoteLearner` (so a far-behind learner is not promoted prematurely) is the
    /// (future) production driver's responsibility, not the engine's.
    PromoteLearner(NodeId),
    /// Remove `0` from the LEARNER set (the symmetric complement of [`MembershipChange::AddLearner`]).
    /// A learner is never counted in ANY majority (neither an election quorum nor a commit
    /// quorum), so dropping one is ALWAYS safe: it cannot shrink any quorum or create disjoint
    /// majorities. This is the membership change an operator's `CLUSTER FORGET <id>` proposes when
    /// the named node is still a non-voting learner (catching up but not yet promoted): without it,
    /// a learner could only be removed AFTER a `PromoteLearner` + `RemoveVoter` round-trip. Like
    /// every variant it is a single-server change subject to the same one-change-in-flight rule.
    RemoveLearner(NodeId),
}

/// A committed cluster-configuration command (CONTROL_PLANE.md #73): the deltas
/// that, replayed in committed-log order on every node, converge each node's
/// `SlotMap` to one identical global ownership view.
///
/// These are the Raft analog of the over-the-wire `CLUSTER MEET / SETSLOT /
/// ADDSLOTS / FORGET / SET-CONFIG-EPOCH` verbs (`ironcache-cluster` slice 3), but
/// instead of mutating one node's local view directly they go THROUGH the log:
/// a leader [`proposes`](RaftNode::propose) the command, Raft commits it, and EVERY
/// node applies the SAME committed sequence in the SAME order. Because committed
/// entries are byte-identical on every node and [`StateMachine::apply`] is
/// deterministic, the resulting `SlotMap`s are identical, which is the linearizable
/// slot-ownership property: no two nodes ever claim the same slot at the same config
/// epoch.
///
/// Ids are owned `String`s (not the engine's `NodeId`) because the `SlotMap`'s node
/// identity is its 40-hex string id, NOT the raft transport id; the sim adapter
/// documents the fixed `NodeId(u64)` -> string mapping it uses (`tests::slot_id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigCmd {
    /// Add a node to the cluster's node table (drives `SlotMap::meet`). Must be
    /// committed BEFORE any [`ConfigCmd::SetSlotOwner`] / [`ConfigCmd::AssignSlots`]
    /// that names this node; the committed-log order guarantees that ordering.
    AddNode {
        /// The node's stable 40-hex id (the `SlotMap` node identity).
        id: String,
        /// The advertised host clients dial.
        host: String,
        /// The advertised TCP port clients dial.
        port: u16,
    },
    /// Remove a node from the table (drives `SlotMap::forget`). The node must own no
    /// slots at apply time (the `SlotMap` guards this); ownership is moved away first
    /// by a prior committed [`ConfigCmd::SetSlotOwner`].
    RemoveNode {
        /// The id of the node to forget.
        id: String,
    },
    /// Flip a single slot's owner to `node` (drives `SlotMap::set_slot_node`). The
    /// node must already be known (a prior committed [`ConfigCmd::AddNode`]). This is
    /// the unit of slot-ownership transfer; it advances the config epoch on apply.
    SetSlotOwner {
        /// The slot to (re)assign.
        slot: u16,
        /// The id of the node that should own it.
        node: String,
    },
    /// Assign a batch of slots to one node: equivalent to a [`ConfigCmd::SetSlotOwner`]
    /// per slot, applied in `slots` order. A single committed entry so a whole shard
    /// hand-off is one atomic log record; it advances the config epoch ONCE on apply.
    AssignSlots {
        /// The id of the node that should own every slot in `slots`.
        node: String,
        /// The slots to assign to `node`, applied in order.
        slots: Vec<u16>,
    },
    /// UN-assign a batch of slots: clear each slot's owner so it is owned by NOBODY (drives
    /// `SlotMap::clear_slot_owner` per slot). This is the committed-log analog of Redis
    /// `CLUSTER DELSLOTS / DELSLOTSRANGE / FLUSHSLOTS`: once committed, EVERY node's config state
    /// machine sets each slot UNASSIGNED, so the slot disappears from `cluster_slots_assigned` and a
    /// key on it is served the unassigned (CLUSTERDOWN) behavior instead of being owned. The inverse
    /// of [`ConfigCmd::AssignSlots`]: a single committed entry so a whole release is one atomic log
    /// record, applied in `slots` order, advancing the config epoch ONCE on apply.
    ///
    /// On apply (in committed-log order, on EVERY node) each slot's `owner` is set UNASSIGNED and its
    /// `mine[]` bitmap is cleared in LOCKSTEP (the same invariant every ownership mutator keeps), so
    /// a node that previously owned a slot loses it (`owns()` goes false) and a node that did not is
    /// unaffected. NODE-RELATIVE is automatic: every node clears the SAME slots from the shared
    /// committed map, so no node id is carried. IDEMPOTENT: clearing an already-unassigned slot is a
    /// no-op, so re-applying a committed entry yields the identical map.
    UnassignSlots {
        /// The slots to UN-assign (clear the owner of), applied in order.
        slots: Vec<u16>,
    },
    /// Seed THIS node's config epoch (drives `SlotMap::set_config_epoch`, valid only
    /// on a fresh, alone node). Used to pin a starting epoch; ordinary ownership
    /// changes advance the epoch via `SlotMap::bump_epoch` instead.
    SetConfigEpoch(u64),
    /// Assign `node` as a REPLICA of every slot in `slots` (HA-7d; drives
    /// `SlotMap::set_slot_replica` per slot). This is the COMMITTED-log analog of "this
    /// node now replicates these slots from their primary": once committed, every node's
    /// config state machine records `node` in the slot's replica set (a NEW parallel
    /// structure, NOT the hot `owns()` bitmap), and the named node, seeing itself in a
    /// committed replica assignment, attaches its shards to the slot OWNER's primary
    /// (full-sync + tail) and serves READONLY reads. The named node MUST already be known
    /// (a prior committed [`ConfigCmd::AddNode`]); the committed-log order guarantees that.
    /// Advances the config epoch ONCE on apply (like [`ConfigCmd::AssignSlots`]).
    AssignReplica {
        /// The id of the node that should REPLICATE every slot in `slots`.
        node: String,
        /// The slots `node` should replicate, applied in order.
        slots: Vec<u16>,
    },
    /// PROMOTE `new_primary` to be the OWNER of every slot in `slots` (HA-8 failover; drives
    /// `SlotMap::set_slot_node` per slot, then `SlotMap::clear_slot_replica`). This is the SOLE
    /// ownership-transfer-on-failover path, and because it flows through the committed log it is
    /// atomic + crash-safe: Figure-8 commit safety guarantees a new leader can never lose a
    /// committed promotion.
    ///
    /// On apply (in committed-log order, on EVERY node), each slot's `owner` flips to
    /// `new_primary` (its `mine[]` bitmap kept in lockstep by the same `set_slot_node` path the
    /// other ownership commands use) and `new_primary` is CLEARED from the slot's replica set (it
    /// is now the owner, not a replica). The config epoch advances once on apply.
    ///
    /// THE SPLIT-BRAIN FENCE: when this committed entry applies on the OLD primary (once it
    /// rejoins and catches its Raft log up), the old primary's `owner[slot]` becomes
    /// `new_primary`, so its `mine[slot]` is false -> `owns()` is false -> it serves MOVED to the
    /// new owner. There is therefore NEVER a committed state in which two nodes both `owns()` a
    /// slot. The named node MUST already be known (a prior committed [`ConfigCmd::AddNode`]);
    /// IDEMPOTENT: re-applying yields the same owner. Advances the epoch ONCE on apply (like
    /// [`ConfigCmd::AssignSlots`]).
    PromoteReplica {
        /// The slots whose ownership transfers to `new_primary`, applied in order.
        slots: Vec<u16>,
        /// The id of the in-sync replica being promoted to OWNER of every slot in `slots`.
        new_primary: String,
    },
    /// Tag `slot` MIGRATING toward `dest` (HA-6 online slot migration; drives
    /// `SlotMap::set_migrating`). This is the SOURCE-side step of `CLUSTER SETSLOT <slot> MIGRATING
    /// <dest>`: the slot stays OWNED by its current owner, but the owner begins shipping its keys to
    /// `dest` and answers `-ASK <slot> <dest>` for a key that has already migrated away. It records
    /// migration STATE only (a NEW parallel structure, NOT the hot `owns()` bitmap), so ownership is
    /// unchanged and the hot path is byte-unchanged until the committed FLIP
    /// ([`ConfigCmd::SetSlotOwner`]). `dest` MUST already be known (a prior committed
    /// [`ConfigCmd::AddNode`]); the committed-log order guarantees that. Advances the config epoch
    /// once on apply (every committed config entry does). IDEMPOTENT (re-applying re-tags the same).
    SetSlotMigrating {
        /// The slot to tag MIGRATING.
        slot: u16,
        /// The id of the DESTINATION node (the `-ASK` target keys are being shipped to).
        dest: String,
    },
    /// Tag `slot` IMPORTING from `src` ON `dest` (HA-6; drives `SlotMap::set_importing`). The
    /// DESTINATION-side step of `CLUSTER SETSLOT <slot> IMPORTING <src>`: the `dest` node is
    /// RECEIVING the slot but does NOT yet own it (ownership stays with `src` until the committed
    /// FLIP). A normal command on the slot is MOVED to the real owner UNLESS the connection set the
    /// one-shot `ASKING` flag. Records migration STATE only (the parallel structure), so the hot path
    /// is byte-unchanged.
    ///
    /// `dest` is carried so apply tags IMPORTING on EXACTLY the destination node. The earlier dest-
    /// less form tagged IMPORTING on EVERY non-owner (anything where `!owns(slot)`), so in an N>=3
    /// cluster a BYSTANDER (a third node that is neither `src` nor the real `dest`) was ALSO tagged
    /// IMPORTING(src) -- which, combined with a leaked one-shot `ASKING`, would serve a key on a
    /// wrong-owner node (a lost write on migration abort). Apply now gates the IMPORTING tag on
    /// `SlotMap::is_self(dest)` (an endpoint compare against `me()`, mirroring how `set_slot_node` /
    /// `is_replica_of_self` recognize self under the dual announce-id / synth-id identity). MIGRATING
    /// stays gated on `owns(slot)`, which is uniquely the source, so it needs no dest.
    ///
    /// Both `src` and `dest` MUST already be known (prior committed [`ConfigCmd::AddNode`]s). Advances
    /// the config epoch once on apply. IDEMPOTENT.
    SetSlotImporting {
        /// The slot to tag IMPORTING.
        slot: u16,
        /// The id of the SOURCE node (the current owner the importer adopts from).
        src: String,
        /// The id of the DESTINATION node (the ONLY node that tags IMPORTING on apply).
        dest: String,
    },
    /// Clear `slot`'s migration state back to NONE (HA-6; drives `SlotMap::clear_migration`). The
    /// `CLUSTER SETSLOT <slot> STABLE` step, used to ABORT a migration that has not flipped (a
    /// committed FLIP [`ConfigCmd::SetSlotOwner`] clears the migration on its own, so STABLE is for
    /// the abort path). Records migration STATE only; ownership and the hot path are unchanged.
    /// Advances the config epoch once on apply. IDEMPOTENT (clearing an already-clear slot is a
    /// no-op).
    SetSlotStable {
        /// The slot whose migration state to clear.
        slot: u16,
    },
}

/// One entry in a node's replicated log.
///
/// `term` is the leader's term when the entry was created and `index` is its
/// position (1-based; index 0 is the empty-log sentinel). Together `(term,
/// index)` are what the up-to-date comparison (section 5.4.1) and the log-matching
/// property (section 5.3) are stated over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The term of the leader that created this entry.
    pub term: u64,
    /// The 1-based position of this entry in the log.
    pub index: u64,
    /// The entry's payload (only [`EntryPayload::Noop`] in 3a).
    pub payload: EntryPayload,
}

/// A Raft RPC message (the wire surface, modeled as plain values).
///
/// The four messages are the request / response pairs of the two Raft RPCs
/// (Figure 2). In 3a `AppendEntries` is the leader's heartbeat, so its `entries`
/// vector is always empty; the field exists so the type is stable across
/// sub-slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftMsg {
    /// A candidate solicits a vote (Figure 2, RequestVote RPC arguments).
    RequestVote {
        /// The candidate's term.
        term: u64,
        /// The candidate requesting the vote.
        candidate: NodeId,
        /// Index of the candidate's last log entry (section 5.4.1).
        last_log_index: u64,
        /// Term of the candidate's last log entry (section 5.4.1).
        last_log_term: u64,
    },
    /// A voter's reply to a [`RaftMsg::RequestVote`] (Figure 2, results).
    RequestVoteResp {
        /// The voter's current term, for the candidate to update itself.
        term: u64,
        /// `true` if the candidate received the vote.
        vote_granted: bool,
    },
    /// A PRE-VOTE solicitation (Ongaro dissertation section 9.6, the Pre-Vote
    /// extension). A follower whose election timer has fired asks every peer "WOULD
    /// you grant me a real vote at `term`?" WITHOUT first incrementing its own term
    /// or causing the peer to adopt one. The `term` carried here is the candidate's
    /// CURRENT term + 1 (the term it WOULD campaign at), but it is HYPOTHETICAL: a
    /// pre-vote NEVER mutates persistent term / vote state on EITHER side. Only if a
    /// quorum of peers reply [`RaftMsg::PreVoteResp`] with `vote_granted = true` does
    /// the pre-candidate actually bump its term and send real [`RaftMsg::RequestVote`]s.
    /// This stops a partitioned or rejoining node from disrupting a healthy cluster by
    /// repeatedly inflating the term (the disruptive-server hazard). Carries the same
    /// up-to-date fields as `RequestVote` so a peer can apply the section-5.4.1 log check.
    PreVote {
        /// The HYPOTHETICAL term the pre-candidate would campaign at (its current
        /// term + 1). Never adopted by the receiver; used only for the staleness check
        /// and to stamp the matching [`RaftMsg::PreVoteResp`].
        term: u64,
        /// The pre-candidate soliciting the pre-vote.
        candidate: NodeId,
        /// Index of the pre-candidate's last log entry (section 5.4.1 up-to-date check).
        last_log_index: u64,
        /// Term of the pre-candidate's last log entry (section 5.4.1 up-to-date check).
        last_log_term: u64,
    },
    /// A peer's reply to a [`RaftMsg::PreVote`] (Ongaro section 9.6). A peer grants the
    /// pre-vote IFF the pre-candidate's log is at least as up-to-date as the peer's
    /// (section 5.4.1) AND the peer has NOT heard from a valid current leader within the
    /// minimum election timeout (the leader-stickiness condition: a fresh leader means no
    /// election is warranted, so the cluster is not disrupted). A pre-vote grant writes NO
    /// persistent state on the granter: it is a non-binding "yes, I would" and a node may
    /// grant pre-votes to several pre-candidates. The `term` echoes the pre-candidate's
    /// hypothetical term so a stale reply (from before a term advance) is discarded.
    PreVoteResp {
        /// The HYPOTHETICAL term this reply is for (echoed from the [`RaftMsg::PreVote`]),
        /// so the pre-candidate tallies only replies for its current pre-vote round.
        term: u64,
        /// `true` if the peer WOULD grant a real vote (log up-to-date AND no fresh leader).
        vote_granted: bool,
    },
    /// A leader's log-replication / heartbeat RPC (Figure 2, AppendEntries
    /// arguments). In 3a `entries` is always empty (pure heartbeat).
    AppendEntries {
        /// The leader's term.
        term: u64,
        /// The leader, so a follower can redirect clients.
        leader: NodeId,
        /// Index of the log entry immediately preceding `entries`.
        prev_log_index: u64,
        /// Term of the `prev_log_index` entry.
        prev_log_term: u64,
        /// Log entries to store (empty for a heartbeat; always empty in 3a).
        entries: Vec<LogEntry>,
        /// The leader's `commitIndex`.
        leader_commit: u64,
    },
    /// A follower's reply to a [`RaftMsg::AppendEntries`] (Figure 2, results).
    AppendEntriesResp {
        /// The follower's current term, for the leader to update itself.
        term: u64,
        /// `true` if the follower accepted the entries (or heartbeat).
        success: bool,
        /// The follower's last index after applying (its `lastLogIndex`).
        match_index: u64,
    },
    /// A CLIENT PROPOSAL, not a peer RPC. This is NOT part of the Raft wire
    /// protocol; it is the local "append this command" request a client makes to a
    /// leader, modeled as a message so the DST harness can inject it through the
    /// same deterministic transport (a node self-`tell`s it). The engine's real
    /// entry point is [`RaftNode::propose`]; the dispatch for this variant just
    /// forwards to it. A non-leader recipient rejects it (no effect).
    Propose {
        /// The opaque payload to append (the engine never interprets it).
        payload: EntryPayload,
    },
    /// A FOLLOWER-TO-LEADER proposal FORWARD (HA-9 leader-forwarding). This is NOT a
    /// consensus RPC and the pure engine never originates, consumes, or reacts to it:
    /// it is a TRANSPORT-LEVEL request carried on the same cluster bus so a follower
    /// can hand a [`ConfigCmd`] proposal (a CLUSTER write, or a replica's
    /// self-promotion) to the node it recognizes as leader, instead of failing with a
    /// bare redirect. The production adapter (`ironcache-raft-net`) intercepts it
    /// BEFORE the engine, proposes locally on the leader, and replies
    /// [`RaftMsg::ForwardProposeResult`]. The variant lives on `RaftMsg` only because
    /// the wire codec and the cluster bus carry `RaftMsg` values; the engine's
    /// [`on_message`](RaftNode::on_message) treats it (and the result) as an inert
    /// no-op so no consensus decision or [`Effects`] ever depends on it.
    ForwardPropose {
        /// A correlation id, allocated by the forwarding follower (a monotonic run-loop
        /// counter, never random), echoed back in the result so the follower matches
        /// the reply to its pending await.
        corr: u64,
        /// The opaque payload to propose on the leader (the engine never interprets it).
        payload: EntryPayload,
    },
    /// The LEADER-TO-FOLLOWER reply to a [`RaftMsg::ForwardPropose`] (HA-9). Like its
    /// request, this is transport-level and inert to the pure engine. `outcome` is
    /// `Some(index)` when the leader accepted the forwarded proposal (the assigned
    /// 1-based log index, exactly as a local `Propose` ack reports) or `None` when the
    /// recipient was NOT the leader (ONE-HOP rule: a non-leader that receives a
    /// `ForwardPropose` does not chain it onward; it replies `None` and the origin
    /// retries, by then knowing the new leader).
    ForwardProposeResult {
        /// The correlation id from the originating [`RaftMsg::ForwardPropose`].
        corr: u64,
        /// `Some(index)` if the leader accepted the forwarded proposal, else `None`.
        outcome: Option<u64>,
    },
    /// The INSTALLSNAPSHOT RPC (Raft section 7 / Figure 13): a leader ships its
    /// state-machine snapshot to a follower whose required log entries the leader has
    /// ALREADY COMPACTED away (the follower's `next_index` fell below the leader's
    /// `log_start_index`, so an AppendEntries can no longer carry the missing prefix).
    ///
    /// PROD-9 CHUNKED transfer (Figure 13's `offset` / `done`): the snapshot is sent in
    /// BOUNDED SEQUENTIAL chunks, each well under the cluster-bus max-frame length, rather
    /// than as one giant frame (which risks the frame bound and a memory spike on both
    /// ends). `data` is the chunk at byte `offset` of the snapshot; `offset == 0` is the
    /// FIRST chunk (which (re)starts the follower's receive buffer), and `done == true`
    /// marks the LAST chunk (the only chunk on which the follower validates + installs the
    /// fully-received snapshot). The leader sends chunks one at a time and advances on each
    /// ack (see [`InstallSnapshotResp`](RaftMsg::InstallSnapshotResp)); a SINGLE chunk
    /// (`offset == 0`, `done == true`) carrying the whole snapshot is byte-equivalent to
    /// the pre-PROD-9 whole-snapshot install. `last_included_index` /
    /// `last_included_term` / `voters` / `learners` are the snapshot's metadata, repeated
    /// on every chunk (they are tiny and let a fresh receive buffer be keyed by them); the
    /// follower adopts them only when it installs on `done`.
    ///
    /// SAFETY (State-Machine-Safety): a leader snapshots ONLY its applied = committed
    /// prefix, so `last_included_index` is committed; installing it on a follower moves
    /// that follower FORWARD to a committed prefix and can never overwrite a different
    /// committed entry. The follower runs the standard [`observe_term`] higher-term
    /// step-down FIRST, exactly like the other RPC handlers. A PARTIAL transfer (not all
    /// chunks received) is NEVER installed: only a contiguous `0..len` sequence ending in
    /// `done` installs, so a dropped / reordered / duplicated chunk cannot corrupt the
    /// install (the follower rejects an out-of-order chunk and replies the offset it next
    /// expects so the leader retries from there).
    InstallSnapshot {
        /// The leader's term.
        term: u64,
        /// The leader, so the follower records / redirects to it (mirrors AppendEntries).
        leader_id: NodeId,
        /// The snapshot's last included log index (its state subsumes every entry
        /// at-or-below this).
        last_included_index: u64,
        /// The term of the snapshot's last included entry (the prev-log-term the entry
        /// FOLLOWING the snapshot is checked against).
        last_included_term: u64,
        /// PROD-9: the byte OFFSET of this chunk within the full snapshot (Figure 13).
        /// `0` is the first chunk and (re)starts the follower's receive buffer; a later
        /// chunk must arrive at exactly the follower's accumulated length or it is rejected.
        offset: u64,
        /// The snapshot bytes for THIS chunk (Figure 13 `data`): the slice of the
        /// [`StateMachine`]-serialized state at `[offset, offset + data.len())`. Bounded
        /// by [`RaftConfig::snapshot_chunk_bytes`].
        data: Vec<u8>,
        /// PROD-9: `true` on the LAST chunk (Figure 13 `done`). The follower validates +
        /// installs the assembled snapshot ONLY on `done`; a non-`done` chunk is buffered
        /// and acked so the leader sends the next one.
        done: bool,
        /// HA-3d: the CONFIG BASELINE (the voter set) as of `last_included_index`. The
        /// snapshot subsumes the `ConfigChange` entries of the compacted prefix, so the
        /// follower cannot rebuild the configuration from its (now-truncated) log alone;
        /// the leader ships the committed config baseline it persisted at the compaction
        /// point so the installing follower adopts exactly that, then applies any surviving
        /// post-snapshot `ConfigChange` tail on top. Empty for a pre-3d / static cluster
        /// (the leader's config equals the constructor voter set), which keeps the install
        /// path config-inert there.
        voters: BTreeSet<NodeId>,
        /// HA-3d: the LEARNER set as of `last_included_index` (companion to `voters`).
        learners: BTreeSet<NodeId>,
    },
    /// A follower's reply to a [`RaftMsg::InstallSnapshot`] (Raft Figure 13 results).
    /// Carries the follower's `term` (so the leader can step down on a higher one), the
    /// `last_included_index` of the snapshot being transferred (ECHOED from the request),
    /// and the PROD-9 CHUNK PROGRESS: `installed` (the final chunk was applied) plus
    /// `next_offset` (the byte offset the follower next expects when the transfer is NOT
    /// yet complete).
    ///
    /// PROD-9 chunked acks (Figure 13):
    /// - On a buffered non-final chunk the follower replies `installed == false` and
    ///   `next_offset == ` its accumulated length, so the leader sends the next chunk from
    ///   there.
    /// - On the final chunk (`done`) the follower installs and replies `installed == true`;
    ///   the leader then advances this follower's `match_index`/`next_index` from the
    ///   echoed `last_included_index`.
    /// - On a REJECTED chunk (a stale-term retransmit, or one that did not arrive at the
    ///   expected offset) the follower replies `installed == false` and `next_offset == `
    ///   the offset it actually expects (`0` for a not-yet-started transfer), so the leader
    ///   restarts / continues from the right place rather than corrupting the buffer.
    ///
    /// SAFETY (no false-commit): on `installed` the leader advances the follower's
    /// `match_index`/`next_index` from the echoed `last_included_index`, NOT from the
    /// leader's OWN current `load_snapshot()` meta. The leader may compact AGAIN (to a
    /// higher index `K'`) between sending `InstallSnapshot(K)` and receiving this reply;
    /// reading the leader's current meta would set `match_index[from] = K' > K`, claiming
    /// the follower replicated past what it actually installed and risking a commit on a
    /// non-majority (Figure 13). Echoing `K` keeps `match_index` honest. A non-`installed`
    /// reply advances NO marker -- it only steers the next chunk's offset.
    InstallSnapshotResp {
        /// The follower's current term, for the leader to update itself.
        term: u64,
        /// The `last_included_index` of the snapshot being transferred, echoed from the
        /// [`RaftMsg::InstallSnapshot`] request. On an `installed` reply the leader advances
        /// this follower's markers from exactly that index (and never past it).
        last_included_index: u64,
        /// PROD-9: `true` only on the reply to the FINAL (`done`) chunk, i.e. the snapshot
        /// was fully received and installed. `false` for a buffered-but-incomplete chunk or
        /// a rejected one (the leader advances no replication marker then).
        installed: bool,
        /// PROD-9: the byte offset the follower NEXT expects for this snapshot transfer
        /// (its accumulated received length), so the leader sends the next chunk from
        /// there. Meaningful only when `installed == false`; ignored on an `installed`
        /// reply (the transfer is complete).
        next_offset: u64,
    },
}

// ---------------------------------------------------------------------------
// Storage seam.
// ---------------------------------------------------------------------------

/// The persistent state a Raft node must durably store before responding to RPCs
/// (Figure 2, "Persistent state on all servers": `currentTerm`, `votedFor`,
/// `log[]`).
///
/// In production this is fsync-backed; in 3a tests it is the in-memory
/// [`MemStorage`]. The engine writes through this seam DURING a step (e.g. it
/// persists the new term and self-vote before sending `RequestVote`), matching
/// the paper's "Persistent state (Updated on stable storage before responding to
/// RPCs)" note.
pub trait RaftStorage {
    /// The latest term the server has seen (initialized to 0, monotonic).
    fn current_term(&self) -> u64;
    /// Persist `term` as the current term.
    fn set_current_term(&mut self, term: u64);
    /// The candidate this server voted for in the current term, if any.
    fn voted_for(&self) -> Option<NodeId>;
    /// Persist the vote for the current term (`None` clears it on a term change).
    fn set_voted_for(&mut self, v: Option<NodeId>);
    /// The index of the last log entry (0 if the log is empty).
    fn last_log_index(&self) -> u64;
    /// The term of the last log entry (0 if the log is empty).
    fn last_log_term(&self) -> u64;
    /// Append `entry` to the log. Used for the leader's own appends (the election
    /// no-op and client proposals); followers use [`RaftStorage::append_entries`].
    fn append(&mut self, entry: LogEntry);

    // -- 3b log-replication extensions (sections 5.3, 5.4.2) ----------------

    /// The term of the entry at `index`, or `0` for `index == 0` (the empty-log
    /// sentinel) or any `index` past the end of the log. This is what the
    /// AppendEntries consistency check (Figure 2, rule 2) compares `prev_log_term`
    /// against, and what the section-5.4.2 commit rule reads as `log[N].term`.
    fn term_at(&self, index: u64) -> u64;

    /// Every entry with `entry.index >= index`, in log order. A leader builds an
    /// AppendEntries `entries` vector from `entries_from(next_index)`; with
    /// `index == last_log_index + 1` this is empty (a pure heartbeat).
    fn entries_from(&self, index: u64) -> Vec<LogEntry>;

    /// Delete every entry with `entry.index >= index` (conflict resolution, Figure
    /// 2 AppendEntries rule 3). `index == 0` or `index == 1` clears the whole log;
    /// an `index` past the end is a no-op.
    fn truncate_from(&mut self, index: u64);

    /// Bulk-append `entries` to the log (Figure 2 AppendEntries rule 4), in order.
    /// The caller is responsible for having truncated any conflicting suffix first;
    /// this is a plain extend.
    fn append_entries(&mut self, entries: &[LogEntry]);

    // -- 3e apply extension -------------------------------------------------

    /// The full [`LogEntry`] at `index` (1-based), or `None` for `index == 0` (the
    /// empty-log sentinel) or any `index` past the end of the log. This is what the
    /// apply pipeline ([`RaftNode::apply_committed`]) reads to hand a committed entry
    /// to the [`StateMachine`]: 3b's apply was a counter and never needed the entry
    /// body, but 3e applies the entry's `payload` to the config state machine, so it
    /// must fetch the whole entry by index. The default impl is derived from the
    /// other 1-based accessors; [`MemStorage`] overrides it with a direct `Vec` index.
    fn entry_at(&self, index: u64) -> Option<LogEntry>;

    // -- 3c snapshot + log-compaction extensions (Raft section 7) -----------

    /// Durably persist a snapshot's metadata + `data`, REPLACING any prior snapshot.
    /// `meta` carries the snapshot's `last_included_index` / `last_included_term` (the
    /// last log entry the snapshot subsumes); `data` is the [`StateMachine`]-serialized
    /// state at that index. After this returns the store can answer
    /// [`term_at`](RaftStorage::term_at)`(last_included_index)` as `last_included_term`
    /// EVEN ONCE the underlying entries are compacted away (see [`compact_to`]). A
    /// snapshot is the durable "everything up to `last_included_index` is committed and
    /// applied" record a restart restores from.
    ///
    /// [`compact_to`]: RaftStorage::compact_to
    fn save_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]);

    /// The most recent persisted snapshot (metadata + data), or `None` if none was
    /// ever saved. On restart the node restores its [`StateMachine`] from this, sets
    /// `last_applied`/`commit_index` to the snapshot's `last_included_index`, then
    /// replays the surviving log tail.
    fn load_snapshot(&self) -> Option<(SnapshotMeta, Vec<u8>)>;

    /// Drop every log entry with `entry.index <= index` (Raft section 7 log
    /// compaction). After compaction the log STARTS just above `index`; the dropped
    /// prefix's prev-log consistency is answered by the snapshot's
    /// `last_included_term` ([`term_at`](RaftStorage::term_at)`(index)` must keep
    /// returning it). The caller compacts only to an index it has already snapshotted
    /// and applied (`index <= last_applied`), so a compacted entry is never needed for
    /// apply again. An `index` below the current log start, or `0`, is a no-op.
    fn compact_to(&mut self, index: u64);

    /// The 1-based index of the FIRST entry still present in the log (the entry just
    /// above the last compaction point), or `last_log_index + 1` when the log is
    /// empty. With no compaction this is `1` (the log is whole), so the default impl
    /// returns `1`; a store that compacts ([`MemStorage`] / `FileStorage`) overrides
    /// it. The leader compares a peer's `next_index` against this to decide whether the
    /// entries it needs were already compacted (and an InstallSnapshot is required
    /// instead of an AppendEntries).
    fn log_start_index(&self) -> u64 {
        1
    }

    // -- 3d membership-config baseline (Raft section 6) ---------------------

    /// Durably persist the cluster CONFIGURATION BASELINE: the voter and learner sets as
    /// of the last snapshot point (HA-3d). Because the live configuration is DERIVED FROM
    /// THE LOG (a node adopts a new config on appending each [`EntryPayload::ConfigChange`]),
    /// recovering it on restart needs the baseline the surviving log tail's `ConfigChange`
    /// entries are replayed ON TOP OF: everything below the snapshot was compacted away, so
    /// the baseline records the config the compacted-away `ConfigChange` prefix produced.
    /// The engine writes this beside [`save_snapshot`](RaftStorage::save_snapshot) (the
    /// snapshot and the baseline are taken at the same index). The DEFAULT is a no-op: a
    /// store that never compacts (or a pre-3d store) keeps the whole log, so the engine can
    /// rebuild the config from the constructor's voter set plus the whole `ConfigChange`
    /// log, and never needs a persisted baseline. `MemStorage` overrides it so the
    /// engine restart-via-snapshot path is exercised.
    fn save_config_baseline(&mut self, voters: &BTreeSet<NodeId>, learners: &BTreeSet<NodeId>) {
        let _ = (voters, learners);
    }

    /// The persisted configuration baseline (voter set, learner set) saved at the last
    /// snapshot, or `None` if none was saved (the default / never-compacted path). On
    /// restart the engine seeds its configuration from this baseline (when present) and
    /// then replays the surviving log's [`EntryPayload::ConfigChange`] entries on top.
    /// The default returns `None`; `MemStorage` overrides it.
    fn load_config_baseline(&self) -> Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)> {
        None
    }
}

/// A snapshot's metadata (Raft section 7): the index and term of the LAST log entry
/// the snapshot's state subsumes.
///
/// `(last_included_index, last_included_term)` is exactly the `(prev_log_index,
/// prev_log_term)` consistency pair for the entry that would FOLLOW the snapshot, so a
/// follower that installs the snapshot can still pass the AppendEntries log check for
/// the first post-snapshot entry. It is the snapshot analog of a [`LogEntry`]'s
/// `(index, term)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// The index of the last log entry the snapshot includes.
    pub last_included_index: u64,
    /// The term of the last log entry the snapshot includes.
    pub last_included_term: u64,
}

/// An in-memory [`RaftStorage`] backed by a `Vec` log plus the term and vote.
///
/// Deterministic and allocation-light; used by the DST tests. It is `Default`
/// (term 0, no vote, empty log). The log is 1-based on the wire: an empty log has
/// `last_log_index == 0` and `last_log_term == 0`, the section 5.4.1 sentinel.
#[derive(Debug, Default, Clone)]
pub struct MemStorage {
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    /// The 1-based log index of `log[0]` once a compaction has dropped a prefix
    /// (Raft section 7). `0` means "no compaction": the log is whole and starts at
    /// index 1 (so `log[0]` is index 1). After [`compact_to`](RaftStorage::compact_to)
    /// the first surviving entry is at this index, and `log[i].index == log_start + i`.
    /// Kept separate from the snapshot meta so an empty post-compaction log still knows
    /// where the next append lands.
    log_start: u64,
    /// The most recent persisted snapshot's metadata, if any (Raft section 7). Its
    /// `last_included_term` is what [`term_at`](RaftStorage::term_at) answers for
    /// `last_included_index` once the underlying entry has been compacted away, so the
    /// AppendEntries prev-log consistency check still passes for the entry that follows
    /// the snapshot.
    snap_meta: Option<SnapshotMeta>,
    /// The most recent persisted snapshot's serialized state-machine bytes, if any.
    snap_data: Vec<u8>,
    /// The membership CONFIGURATION BASELINE persisted at the last snapshot (HA-3d): the
    /// `(voters, learners)` the compacted-away `ConfigChange` prefix produced. `None`
    /// until the engine first persists one (it does so beside `save_snapshot`), so a
    /// never-compacted store has nothing here and the default config-rebuild path runs.
    config_baseline: Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)>,
}

impl MemStorage {
    /// A fresh store: term 0, no vote, empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The log entries, for test inspection.
    #[must_use]
    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }
}

impl MemStorage {
    /// The 1-based index of `log[0]` (Raft section 7 compaction). With no compaction
    /// (`log_start == 0`) the log is whole and starts at index 1; after a compaction it
    /// starts at the recorded `log_start`. The vec position of a present `index` is
    /// `index - start`.
    #[inline]
    fn start(&self) -> u64 {
        if self.log_start == 0 {
            1
        } else {
            self.log_start
        }
    }

    /// The vec position of 1-based `index`, or `None` if it is below the compacted log
    /// start or past the end (so the accessors stay total over a compacted log).
    #[inline]
    fn pos_of(&self, index: u64) -> Option<usize> {
        let start = self.start();
        if index < start {
            return None;
        }
        usize::try_from(index - start)
            .ok()
            .filter(|&p| p < self.log.len())
    }
}

impl RaftStorage for MemStorage {
    fn current_term(&self) -> u64 {
        self.current_term
    }

    fn set_current_term(&mut self, term: u64) {
        self.current_term = term;
    }

    fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    fn set_voted_for(&mut self, v: Option<NodeId>) {
        self.voted_for = v;
    }

    fn last_log_index(&self) -> u64 {
        // The last entry's index if the log is non-empty, else the snapshot's
        // last_included_index (a fully-compacted log still ends at the snapshot), else
        // 0 (the empty-log sentinel).
        self.log.last().map_or_else(
            || self.snap_meta.map_or(0, |m| m.last_included_index),
            |e| e.index,
        )
    }

    fn last_log_term(&self) -> u64 {
        // The last entry's term, or (for a fully-compacted log) the snapshot's
        // last_included_term, else 0.
        self.log.last().map_or_else(
            || self.snap_meta.map_or(0, |m| m.last_included_term),
            |e| e.term,
        )
    }

    fn append(&mut self, entry: LogEntry) {
        self.log.push(entry);
    }

    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            // The empty-log sentinel always "matches" prev_log_index 0 (Figure 2
            // AppendEntries rule 2): a leader replicating from the very start sends
            // prev_log_index 0, prev_log_term 0, which every log contains.
            return 0;
        }
        // A snapshot answers the term at its last_included_index even after the entry
        // was compacted away (Raft section 7): the prev-log check for the entry FOLLOWING
        // the snapshot must still pass.
        if let Some(meta) = self.snap_meta {
            if index == meta.last_included_index {
                return meta.last_included_term;
            }
        }
        // Otherwise read the surviving log; an index below the compacted start or past
        // the end yields 0 (no such entry here).
        self.pos_of(index).map_or(0, |pos| self.log[pos].term)
    }

    fn entries_from(&self, index: u64) -> Vec<LogEntry> {
        // Entries with `entry.index >= index`. Clamp the request to the compacted start
        // so a leader asking from a still-present index gets the surviving suffix; an
        // index below the start clamps to the whole surviving log. (A leader that needs
        // entries BELOW the start sends an InstallSnapshot instead, so this never has to
        // fabricate compacted entries.)
        let start = self.start();
        let from = index.max(start);
        let Ok(pos) = usize::try_from(from - start) else {
            return Vec::new();
        };
        self.log
            .get(pos..)
            .map_or_else(Vec::new, <[LogEntry]>::to_vec)
    }

    fn truncate_from(&mut self, index: u64) {
        // Delete entries with `entry.index >= index`. An index at or below the compacted
        // start clears the whole surviving log; an index past the end truncates nothing.
        let start = self.start();
        let keep = if index <= start {
            0
        } else {
            usize::try_from(index - start).unwrap_or(usize::MAX)
        };
        if keep < self.log.len() {
            self.log.truncate(keep);
        }
    }

    fn append_entries(&mut self, entries: &[LogEntry]) {
        self.log.extend_from_slice(entries);
    }

    fn entry_at(&self, index: u64) -> Option<LogEntry> {
        if index == 0 {
            // The empty-log sentinel: there is no entry at index 0.
            return None;
        }
        self.pos_of(index).map(|pos| self.log[pos].clone())
    }

    fn save_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]) {
        self.snap_meta = Some(meta);
        self.snap_data = data.to_vec();
    }

    fn load_snapshot(&self) -> Option<(SnapshotMeta, Vec<u8>)> {
        self.snap_meta.map(|meta| (meta, self.snap_data.clone()))
    }

    fn compact_to(&mut self, index: u64) {
        // Drop entries with entry.index <= index. The first surviving entry is at index
        // `index + 1`; if nothing survives, the next append lands at `index + 1`, so
        // log_start records that boundary. An index below the current start (a stale /
        // duplicate compaction) is a no-op.
        let start = self.start();
        if index < start || index == 0 {
            return;
        }
        // Number of entries to drop from the front: those with index in [start, index].
        let drop = usize::try_from(index - start + 1).unwrap_or(usize::MAX);
        let drop = drop.min(self.log.len());
        self.log.drain(..drop);
        // The new start is one past the compaction point (where the next surviving /
        // appended entry lives). Stored as the literal index so start() reads it back.
        self.log_start = index + 1;
    }

    fn log_start_index(&self) -> u64 {
        self.start()
    }

    fn save_config_baseline(&mut self, voters: &BTreeSet<NodeId>, learners: &BTreeSet<NodeId>) {
        self.config_baseline = Some((voters.clone(), learners.clone()));
    }

    fn load_config_baseline(&self) -> Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)> {
        self.config_baseline.clone()
    }
}

// ---------------------------------------------------------------------------
// RNG seam.
// ---------------------------------------------------------------------------

/// The narrow randomness seam the engine uses, solely for election-timeout jitter.
///
/// Raft randomizes each node's election timeout so split votes are rare and
/// resolve quickly (section 5.2). This is the ONLY randomness in the engine; it is
/// a single-method trait so the engine cannot reach a foreign RNG and so the seam
/// is trivial to drive from the sim ([`ironcache_sim::SimCtx::gen_below`]) or, in
/// production, from [`ironcache_env::Rng`]. A blanket impl makes any
/// `ironcache_env::Rng` usable directly.
pub trait RaftRng {
    /// A `u64` in `[0, bound)`. Returns `0` when `bound == 0`. Same contract as
    /// [`ironcache_env::Rng::gen_below`].
    fn gen_below(&mut self, bound: u64) -> u64;
}

impl<R: ironcache_env::Rng> RaftRng for R {
    fn gen_below(&mut self, bound: u64) -> u64 {
        ironcache_env::Rng::gen_below(self, bound)
    }
}

// ---------------------------------------------------------------------------
// State-machine seam (3e).
// ---------------------------------------------------------------------------

/// The replicated state machine a [`RaftNode`] drives from its committed log
/// (Raft Figure 2, "All Servers": apply `log[lastApplied]` to the state machine).
///
/// This is the 3e seam that turns the 3b apply SINK into a real apply. The engine
/// owns one `M: StateMachine` and, in [`RaftNode::apply_committed`], hands it each
/// newly-committed entry in index order exactly once. Apply MUST be deterministic
/// and side-effect-free beyond the machine's own state: the whole linearizable
/// slot-ownership guarantee rests on every node applying the SAME committed
/// sequence and reaching the SAME state, so any nondeterminism here (a clock, an
/// RNG, ordering on a hash map) would let two nodes diverge. ADR-0003 forbids those
/// in this crate; an implementor must honor the same bar.
///
/// The trivial [`CountingSm`] is the default for callers that do not care about
/// config (it preserves the 3b applied-counter behavior so the existing tests are
/// unchanged); the real implementor is the config state machine in the tests
/// (`tests::ConfigSm`), which drives a `SlotMap`.
pub trait StateMachine {
    /// Apply one committed `entry` to the state machine. Called exactly once per
    /// entry, in ascending index order, from [`RaftNode::apply_committed`]. The
    /// engine guarantees the entry is committed (durable on a majority) before this
    /// fires, so an apply is never speculative and never replays a rolled-back entry.
    fn apply(&mut self, entry: &LogEntry);

    /// Serialize the CURRENT applied state to bytes (Raft section 7 snapshotting). The
    /// bytes are an opaque, deterministic image of everything this machine has applied
    /// so far; the engine pairs them with the `(last_included_index, last_included_term)`
    /// of the log entry the state reflects ([`SnapshotMeta`]) and hands the pair to
    /// [`RaftStorage::save_snapshot`]. MUST be deterministic (a function of the applied
    /// prefix only): the whole point is that a follower [`restore`](StateMachine::restore)d
    /// from a leader's snapshot reaches a state IDENTICAL to having applied the same
    /// committed prefix entry-by-entry. The default panics so an implementor that opts
    /// into compaction supplies a real serialization; the trivial [`CountingSm`]
    /// overrides it.
    fn snapshot(&self) -> Vec<u8> {
        unimplemented!("StateMachine::snapshot is required for log compaction (Raft section 7)")
    }

    /// REPLACE this machine's state with the one serialized in `data` (Raft section 7).
    /// The inverse of [`snapshot`](StateMachine::snapshot): after a restore the machine
    /// is byte-identical to one that had applied the committed prefix the snapshot
    /// covers. Called by the engine when it installs a leader's snapshot on a lagging
    /// follower, or on restart from a persisted snapshot. MUST move the machine FORWARD
    /// only (the engine restores only from a snapshot of an applied = committed prefix),
    /// which is why it never violates State-Machine-Safety. The default panics; the
    /// trivial [`CountingSm`] overrides it.
    fn restore(&mut self, data: &[u8]) {
        let _ = data;
        unimplemented!("StateMachine::restore is required for log compaction (Raft section 7)")
    }
}

/// The trivial default [`StateMachine`]: it interprets NO payload and merely counts
/// the entries applied to it, reproducing 3b's apply-sink behavior verbatim.
///
/// This is the `M` the election / log-replication tests use, where the payload is
/// opaque and only the apply WATERMARK matters. Keeping a real (if trivial) state
/// machine here is what let 3e generalize [`RaftNode`] over `M` without perturbing
/// those tests: the count this keeps is surfaced through
/// [`RaftNode::applied_count`], exactly as the 3b sink was.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CountingSm {
    /// How many entries have been applied (the 3b sink counter).
    applied: u64,
}

impl CountingSm {
    /// A fresh counter at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many entries this machine has applied.
    #[must_use]
    pub fn applied(&self) -> u64 {
        self.applied
    }
}

impl StateMachine for CountingSm {
    fn apply(&mut self, _entry: &LogEntry) {
        // The 3b sink: count the entry, interpret nothing. Saturating so a
        // pathological replay can never wrap (it never decreases).
        self.applied = self.applied.saturating_add(1);
    }

    fn snapshot(&self) -> Vec<u8> {
        // The whole applied state is the counter: serialize it little-endian (8 bytes),
        // deterministic and tiny. A node restored from this resumes the same count, so
        // the apply WATERMARK the election / replication tests assert is preserved.
        self.applied.to_le_bytes().to_vec()
    }

    fn restore(&mut self, data: &[u8]) {
        // Restore the counter from the 8-byte little-endian image. A short / malformed
        // buffer (never produced by `snapshot`) restores to zero rather than panicking,
        // keeping restore total like the rest of the engine's decode paths.
        let counter = data
            .get(..8)
            .and_then(|b| <[u8; 8]>::try_from(b).ok())
            .map_or(0, u64::from_le_bytes);
        self.applied = counter;
    }
}

// ---------------------------------------------------------------------------
// Effects.
// ---------------------------------------------------------------------------

/// A timer arm or cancel a step intends, mirroring the sim's timer model.
///
/// The engine never touches a clock directly; it asks the caller to (re)arm or
/// cancel a logical timer identified by a `token` ([`ELECTION_TIMEOUT`] or
/// [`HEARTBEAT`]). Re-arming a token replaces it (latest arm wins), which is how
/// "reset my election timeout" is expressed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerOp {
    /// Arm `token` to fire `after` the current time. Replaces any prior arm.
    Set {
        /// The timer identifier ([`ELECTION_TIMEOUT`] / [`HEARTBEAT`]).
        token: u64,
        /// How long after "now" the timer should fire.
        after: Duration,
    },
    /// Cancel `token` if armed (a no-op otherwise).
    Cancel {
        /// The timer identifier to cancel.
        token: u64,
    },
}

/// The non-persistent effects a single step intends: outbound messages and timer
/// operations.
///
/// Persistence is NOT recorded here; it is written through [`RaftStorage`] during
/// the step (the paper requires persistent state be stable before the RPC reply
/// is sent, and modeling it as a deferred effect would break that ordering).
/// `sends` and `timer_ops` are in issue order; the caller drains them after the
/// step returns (timer ops then sends, by the sim's convention).
///
/// [`committed_through`](Effects::committed_through) is a PURELY ADDITIVE NOTIFICATION
/// (HA-prod-commit-ack): it records that this step raised `commit_index` to a new
/// high-water, so an adapter can resolve a parked proposal ack on TRUE COMMIT (not at
/// append) WITHOUT the engine doing any I/O. It is a function of the same inputs as
/// every other field, drives NO vote / append / commit DECISION, and is IGNORED by the
/// DST sim drain (which reads only `sends` + `timer_ops`), so every determinism /
/// safety scenario replays byte-identically with it present.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Effects {
    /// Messages to send, as `(destination, message)`, in issue order.
    pub sends: Vec<(NodeId, RaftMsg)>,
    /// Timer arm / cancel operations, in issue order.
    pub timer_ops: Vec<TimerOp>,
    /// The new committed high-water this step reached, if the step RAISED
    /// `commit_index` (HA-prod-commit-ack). `Some(n)` means "every entry with index
    /// `<= n` is now committed on a majority"; `None` means this step did not advance
    /// commit. A PURE record of a decision the engine already made (it mirrors the
    /// monotone `commit_index` after the step), emitting no I/O and changing no other
    /// effect, so the production adapter can fulfil a parked propose ack when the
    /// proposed index is at-or-below this value. The DST sim drain ignores this field
    /// entirely, so the determinism sweep is byte-identical. Always either `None` or a
    /// value strictly greater than the `commit_index` at step entry (commit is
    /// monotone), so the adapter never sees a stale or rewound high-water.
    pub committed_through: Option<u64>,
}

impl Effects {
    /// An empty effect set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn send(&mut self, to: NodeId, msg: RaftMsg) {
        self.sends.push((to, msg));
    }

    #[inline]
    fn set_timer(&mut self, token: u64, after: Duration) {
        self.timer_ops.push(TimerOp::Set { token, after });
    }

    #[inline]
    fn cancel_timer(&mut self, token: u64) {
        self.timer_ops.push(TimerOp::Cancel { token });
    }

    /// Record that this step raised `commit_index` to `index` (HA-prod-commit-ack).
    /// Commit is monotone within a step, so a later raise in the same step always
    /// dominates an earlier one; keep the MAX so a step that advances commit more than
    /// once (which the engine never does today, but the record stays correct if it ever
    /// did) reports the final high-water. Purely additive: it emits no I/O and changes
    /// no decision, so the DST sweep is byte-identical (the sim drain ignores it).
    #[inline]
    fn note_committed_through(&mut self, index: u64) {
        self.committed_through = Some(match self.committed_through {
            Some(prev) => prev.max(index),
            None => index,
        });
    }
}

// ---------------------------------------------------------------------------
// Config and timer tokens.
// ---------------------------------------------------------------------------

/// Timing parameters for the engine (section 5.2 / 5.6 "timing and availability").
///
/// The election timeout is drawn from `[base, base + jitter)` on every (re)arm so
/// nodes time out at different instants and split votes resolve. The heartbeat
/// interval must be comfortably below `base` so a live leader keeps followers from
/// timing out; the defaults (election base / jitter 150ms each, heartbeat 50ms)
/// satisfy `heartbeat << election_timeout`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftConfig {
    /// The minimum election timeout.
    pub election_timeout_base: Duration,
    /// The randomized span added on top of the base (drawn per arm).
    pub election_timeout_jitter: Duration,
    /// How often a leader sends heartbeats.
    pub heartbeat_interval: Duration,
    /// The log-compaction threshold (Raft section 7): once the number of log entries
    /// ABOVE the last snapshot exceeds this, the node snapshots its state machine at
    /// `last_applied` and compacts the log to there. A function of the applied prefix +
    /// this constant, so it is fully DETERMINISTIC (no time / RNG) and replays
    /// identically. `0` DISABLES compaction (the log grows unbounded, the pre-3c
    /// behaviour), which keeps every existing DST scenario byte-identical: they all use
    /// [`RaftConfig::default`], whose value is below.
    pub snapshot_threshold: u64,
    /// PROD-9 CHUNKED InstallSnapshot: the MAXIMUM number of snapshot bytes a single
    /// [`RaftMsg::InstallSnapshot`] chunk carries (Raft Figure 13's chunk size). The leader
    /// slices the snapshot into sequential chunks of at most this many bytes so no install
    /// frame approaches the cluster-bus max-frame length (a multi-hundred-MB snapshot would
    /// otherwise be one giant frame + a memory spike on both ends). MUST be well under the
    /// bus frame bound (`ironcache_runtime::MAX_CLUSTER_FRAME_LEN`); the default
    /// [`DEFAULT_SNAPSHOT_CHUNK_BYTES`] is a few hundred KB, which is. A function of the
    /// snapshot bytes + this constant only, so chunk boundaries are fully DETERMINISTIC (no
    /// time / RNG) and replay identically. A value at or above the snapshot size sends the
    /// whole snapshot in one chunk (byte-equivalent to the pre-PROD-9 path); `0` is treated
    /// as a single chunk (never a zero-length-chunk loop). Because the same bytes are sliced
    /// and reassembled, the chunk size NEVER changes the installed state -- only the
    /// framing -- so a follower installs a byte-identical snapshot at any chunk size.
    pub snapshot_chunk_bytes: usize,
    /// PRE-VOTE election hygiene (Ongaro dissertation section 9.6), default ON. When set,
    /// a follower whose election timer fires runs a PRE-VOTE round (a non-binding "would
    /// you grant me a vote at term+1?" poll) BEFORE incrementing its term and campaigning;
    /// only a quorum of pre-vote grants converts it to a real candidate. This prevents a
    /// partitioned / rejoining node from disrupting a stable leader by repeatedly inflating
    /// the term. Disabling it (`false`) restores the pre-refinement behaviour (immediate
    /// term-bump on timeout), which the regression-anchor tests pin; production keeps it ON.
    /// See [`RaftNode::on_election_timeout`].
    pub pre_vote: bool,
    /// CHECK-QUORUM leadership hygiene (Ongaro dissertation section 6.2 / 9.6), default ON.
    /// When set, a LEADER that has not received a successful AppendEntries (heartbeat) ack
    /// from a QUORUM of voters within an election timeout STEPS DOWN to follower, rather than
    /// indefinitely believing it is leader while partitioned away from the majority (which
    /// would let it keep serving stale leader-only reads). Tracked engine-side from the
    /// injected tick time; the leader evaluates it on each heartbeat. Disabling it (`false`)
    /// restores the pre-refinement behaviour (a leader never self-deposes); production keeps
    /// it ON. See [`RaftNode::on_heartbeat_timer`].
    pub check_quorum: bool,
}

/// The default log-compaction threshold ([`RaftConfig::snapshot_threshold`]). `0`
/// DISABLES compaction, so the DEFAULT config never snapshots and every existing DST
/// scenario (which builds its config from `RaftConfig::default`) replays exactly as
/// before; the snapshot tests opt IN by setting a small positive threshold. The config
/// state machine's snapshot is tiny, so production wiring can pick a modest value
/// (e.g. a few hundred) without the size of a snapshot being a concern.
pub const DEFAULT_SNAPSHOT_THRESHOLD: u64 = 0;

/// The default PROD-9 chunked-InstallSnapshot chunk size ([`RaftConfig::snapshot_chunk_bytes`]):
/// 256 KiB. Comfortably under the cluster-bus max-frame length
/// (`ironcache_runtime::MAX_CLUSTER_FRAME_LEN`, 512 MiB) with room for the chunk's framing
/// overhead, while large enough that a typical config snapshot (the `SlotMap`'s committed
/// state) ships in one or two chunks. It is a pure framing parameter -- the installed state
/// is byte-identical at any value -- so the DST sweep, which exercises both the single-chunk
/// (snapshot smaller than this) and multi-chunk (a small override) paths, converges to the
/// same state machine regardless.
pub const DEFAULT_SNAPSHOT_CHUNK_BYTES: usize = 256 * 1024;

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            election_timeout_base: Duration::from_millis(150),
            election_timeout_jitter: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(50),
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
            snapshot_chunk_bytes: DEFAULT_SNAPSHOT_CHUNK_BYTES,
            // Pre-Vote + check-quorum are strictly-better election hygiene (Ongaro
            // section 9.6); default them ON so production and the DST sweep both run the
            // hardened path. The disruptive-server regression-anchor tests opt OUT
            // explicitly to pin the legacy behaviour they were written against.
            pre_vote: true,
            check_quorum: true,
        }
    }
}

/// The election-timeout timer token. A `Follower` or `Candidate` whose
/// [`ELECTION_TIMEOUT`] fires starts a new election.
pub const ELECTION_TIMEOUT: u64 = 0;
/// The heartbeat timer token. A `Leader`'s [`HEARTBEAT`] fires periodically and it
/// broadcasts an empty [`RaftMsg::AppendEntries`].
pub const HEARTBEAT: u64 = 1;

/// The learner CATCH-UP LAG GATE (HA-3d, Raft section 6's round-based caught-up
/// check, simplified to a fixed lag bound). A leader will only propose
/// [`MembershipChange::PromoteLearner`] for a learner whose tracked `match_index` is
/// within this many entries of the leader's last log index. The paper bounds the join
/// by ROUNDS of replication taking less than an election timeout; a fixed small lag is
/// the deterministic, time-free analog the pure engine uses (it reads no clock), and is
/// sufficient for safety -- promotion is safe at ANY lag (a new voter never violates
/// election safety), the gate exists only to avoid promoting a far-behind voter that
/// would briefly stall commit. Exposed via [`RaftNode::learner_caught_up`].
pub const LEARNER_CATCHUP_LAG: u64 = 2;

/// PRE-VOTE -> REAL-ELECTION FALLBACK THRESHOLD (the etcd #8525 mixed-version safety net,
/// PROD-9 follow-up). When [`RaftConfig::pre_vote`] is on, a node that times out as a
/// pre-candidate WITHOUT having reached a pre-vote quorum normally just runs ANOTHER
/// pre-vote round forever (`start_pre_vote` re-arms the timer). That is correct in a
/// homogeneous pre-vote cluster, but it LOCKS OUT a subset whose pre-votes can never be
/// granted -- e.g. a rolling upgrade where old, pre-vote-UNAWARE peers drop the `PreVote`
/// frame and never reply, or any case where a quorum of GRANTS is simply unreachable. Such
/// a node could pre-vote indefinitely and never start a real, term-bumping election, so the
/// cluster can never elect.
///
/// The fix mirrors etcd (issues #8243 / #8501, fixed in #8525): count CONSECUTIVE pre-vote
/// rounds that won no quorum; after this many, fall back ONCE to a real term-bumping
/// election (`start_real_election`) instead of yet another pre-vote round, then resume
/// normal pre-vote mode. The counter resets to 0 on ANY progress (hearing a valid leader,
/// winning a pre-vote, adopting a higher term, becoming leader), so a HEALTHY all-pre-vote
/// cluster always resets before reaching the threshold and NEVER falls back -- steady-state
/// behaviour is byte-identical. A partitioned node that does fall back still cannot WIN (it
/// is partitioned); it merely term-bumps at this BOUNDED slow rate (once per this many
/// rounds) instead of never, which is strictly better liveness than lockout and far less
/// disruptive than running with pre-vote off (which bumps the term on EVERY timeout).
///
/// `3` is a small etcd-style constant: large enough that ordinary jitter / message loss in
/// a healthy cluster never accumulates that many ungranted rounds (a single granted round
/// resets it), small enough that a genuinely stuck subset recovers within a few election
/// timeouts. See [`RaftNode::on_election_timeout`].
pub const PRE_VOTE_FALLBACK_ROUNDS: u32 = 3;

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
