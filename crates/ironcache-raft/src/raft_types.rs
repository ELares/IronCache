// SPDX-License-Identifier: MIT OR Apache-2.0
//! Core Raft protocol TYPES split out of `lib.rs` (#625): the node identity + role, the log-entry
//! payloads + membership-change + `ConfigCmd` control taxonomy, the `LogEntry`, and the `RaftMsg`
//! wire message set. Pure data types (no engine logic). Behavior-preserving relocation: byte-
//! identical to their former in-`lib.rs` definitions; re-exported from the crate root so every
//! `ironcache_raft::X` path resolves unchanged.

use std::collections::BTreeSet;

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
