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
//! What 3b still does NOT do: payloads remain opaque ([`EntryPayload::Noop`] plus
//! a minimal test-only [`EntryPayload::Bytes`]); there is no snapshotting / log
//! compaction, no membership change, and no real state machine. The conflict-index
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
/// The engine itself is STILL payload-agnostic: it commits a `Config` entry by the
/// exact same replication + Figure-8 commit path as any other entry and never
/// looks inside it. Interpretation happens only in [`apply_committed`], which hands
/// each committed entry to the [`StateMachine`] seam; a non-`Config` payload
/// (Noop/Bytes) is a no-op for the config state machine.
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
        self.log.last().map_or(0, |e| e.index)
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
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
        // The log is 1-based and contiguous: the entry with `index` is at vec
        // position `index - 1`. An index past the end yields 0 (no such entry).
        usize::try_from(index - 1)
            .ok()
            .and_then(|pos| self.log.get(pos))
            .map_or(0, |e| e.term)
    }

    fn entries_from(&self, index: u64) -> Vec<LogEntry> {
        // Entries with `entry.index >= index`. With a contiguous 1-based log the
        // first such entry is at vec position `index - 1`; `index <= 1` means "from
        // the start" (whole log), and an index past the end yields an empty vec.
        let start = if index <= 1 {
            0
        } else {
            usize::try_from(index - 1).unwrap_or(usize::MAX)
        };
        self.log
            .get(start..)
            .map_or_else(Vec::new, <[LogEntry]>::to_vec)
    }

    fn truncate_from(&mut self, index: u64) {
        // Delete entries with `entry.index >= index`. `index <= 1` clears the whole
        // log (index 0 is the sentinel, index 1 is the first real entry); an index
        // past the end truncates nothing.
        let keep = if index <= 1 {
            0
        } else {
            usize::try_from(index - 1).unwrap_or(usize::MAX)
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
        // The log is 1-based and contiguous: the entry with `index` is at vec
        // position `index - 1`. An index past the end yields None.
        usize::try_from(index - 1)
            .ok()
            .and_then(|pos| self.log.get(pos))
            .cloned()
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
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Effects {
    /// Messages to send, as `(destination, message)`, in issue order.
    pub sends: Vec<(NodeId, RaftMsg)>,
    /// Timer arm / cancel operations, in issue order.
    pub timer_ops: Vec<TimerOp>,
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
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            election_timeout_base: Duration::from_millis(150),
            election_timeout_jitter: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(50),
        }
    }
}

/// The election-timeout timer token. A `Follower` or `Candidate` whose
/// [`ELECTION_TIMEOUT`] fires starts a new election.
pub const ELECTION_TIMEOUT: u64 = 0;
/// The heartbeat timer token. A `Leader`'s [`HEARTBEAT`] fires periodically and it
/// broadcasts an empty [`RaftMsg::AppendEntries`].
pub const HEARTBEAT: u64 = 1;

// ---------------------------------------------------------------------------
// The node.
// ---------------------------------------------------------------------------

/// A single Raft node: the pure step engine.
///
/// Holds the node's identity, the static voter set (membership changes are a later
/// sub-slice), the volatile role and the in-flight vote tally, the timing config,
/// and the persistent [`RaftStorage`]. It is driven by [`RaftNode::start`] once,
/// then [`RaftNode::on_message`] / [`RaftNode::on_timer`] per event. It reads time
/// only via the `now` argument and randomness only via the [`RaftRng`] argument;
/// it never blocks and performs no I/O.
#[derive(Debug)]
pub struct RaftNode<S: RaftStorage, M: StateMachine = CountingSm> {
    /// This node's id.
    id: NodeId,
    /// The static set of voting members (includes `id`). Static in 3a.
    voters: BTreeSet<NodeId>,
    /// The current role (volatile; rebuilt from persistence + elections on boot).
    role: Role,
    /// Votes received in the current term while a `Candidate` (includes self).
    /// Empty unless `role == Candidate`.
    votes: BTreeSet<NodeId>,
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
        sm: M,
    ) -> Self {
        RaftNode {
            id,
            voters,
            role: Role::Follower,
            votes: BTreeSet::new(),
            config,
            storage,
            commit_index: 0,
            last_applied: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            applied_count: 0,
            sm,
        }
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
    #[allow(clippy::needless_pass_by_value)]
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
            } => self.on_request_vote(rng, term, candidate, last_log_index, last_log_term, out),
            RaftMsg::RequestVoteResp { term, vote_granted } => {
                self.on_request_vote_resp(now, rng, from, term, vote_granted, out);
            }
            RaftMsg::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.on_append_entries(
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
            HEARTBEAT => self.on_heartbeat_timer(out),
            _ => {}
        }
    }

    // -- handlers -----------------------------------------------------------

    /// ELECTION TIMEOUT (Figure 2, "Candidates": on conversion to candidate, start
    /// election). Fires on a `Follower` or `Candidate`. A `Leader` ignores it (it
    /// has no election timer armed; this guard is belt-and-suspenders against a
    /// stale event).
    ///
    /// Start a new election: increment `currentTerm` (persisted); vote for self
    /// (persisted); become `Candidate`; reset the votes set to `{self}`; send a
    /// `RequestVote` carrying our last-log `(index, term)` to every OTHER voter;
    /// re-arm [`ELECTION_TIMEOUT`] with fresh jitter. A single-voter cluster is an
    /// instant majority, so it becomes leader immediately.
    fn on_election_timeout(&mut self, now: Monotonic, rng: &mut dyn RaftRng, out: &mut Effects) {
        let _ = now;
        if self.role == Role::Leader {
            return;
        }
        let new_term = self.storage.current_term() + 1;
        self.storage.set_current_term(new_term);
        self.storage.set_voted_for(Some(self.id));
        self.role = Role::Candidate;
        self.votes.clear();
        self.votes.insert(self.id);

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
        self.arm_election_timer(rng, out);

        // A single-voter cluster: self-vote is already a majority, win at once.
        self.maybe_become_leader(out);
    }

    /// REQUESTVOTE receiver (Figure 2, "RequestVote RPC, Receiver implementation",
    /// plus section 5.4.1 for the up-to-date check).
    ///
    /// Order of operations:
    /// 1. "All Servers": if `term > currentTerm`, step down and adopt the term
    ///    (clearing `votedFor`), so a fresh term's first vote is grant-eligible.
    /// 2. Reply false (no grant) if `term < currentTerm` (rule 1).
    /// 3. Otherwise grant IFF (`votedFor` is null or already the candidate) AND
    ///    the candidate's log is at least as up-to-date as ours (rule 2). On a
    ///    grant: persist `votedFor = candidate` and RESET the election timer
    ///    (granting a vote is "hearing from a valid leader-to-be").
    fn on_request_vote(
        &mut self,
        rng: &mut dyn RaftRng,
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
        out: &mut Effects,
    ) {
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
        let _ = now;
        if self.observe_term(term, rng, out) {
            // We stepped down; a stale candidacy's tally is irrelevant now.
            return;
        }
        if vote_granted {
            self.record_vote(from, term, out);
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
                append_from = i;
                break;
            }
            // Same index, same term: identical (Log Matching). Already present; skip.
            append_from = i + 1;
        }
        if append_from < entries.len() {
            self.storage.append_entries(&entries[append_from..]);
        }

        // Rule 5: advance commit_index toward leader_commit, capped at the index of
        // the last entry THIS RPC vouched for (prev_log_index + entries.len()).
        let last_new_index = prev_log_index + u64::try_from(entries.len()).unwrap_or(u64::MAX);
        if leader_commit > self.commit_index {
            let new_commit = leader_commit.min(last_new_index);
            if new_commit > self.commit_index {
                self.commit_index = new_commit;
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
        let _ = now;
        if self.observe_term(term, rng, out) {
            // We stepped down on a higher term; we are no longer leader.
            return;
        }
        if self.role != Role::Leader || term != self.storage.current_term() {
            // Stale response (old term) or we are not leader: ignore.
            return;
        }

        if success {
            // Advance this peer's replicated/next markers. Take the MAX so a delayed
            // or duplicated older success can never rewind an already-higher marker.
            let m = self.match_index.entry(from).or_insert(0);
            *m = (*m).max(match_index);
            let mi = *m;
            self.next_index.insert(from, mi + 1);
            // A peer made progress: maybe a new index is now on a majority.
            self.maybe_advance_commit();
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
    fn on_heartbeat_timer(&mut self, out: &mut Effects) {
        if self.role != Role::Leader {
            return;
        }
        self.broadcast_heartbeat(out);
        out.set_timer(HEARTBEAT, self.config.heartbeat_interval);
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
    /// KNOWN LIVENESS LIMITATION (deferred, not a safety gap): 3a implements
    /// neither Pre-Vote nor leader-stickiness (Raft dissertation section 9.6). A
    /// node with a stale log that cannot win can still force term inflation: each
    /// of its higher-term `RequestVote`s adopts the term here and steps a healthy
    /// leader down, even though the vote is then refused on the up-to-date check.
    /// Election Safety always holds (the stale node never gains a majority), but a
    /// flapping or adversarial node can churn leadership. Pre-Vote / stickiness is
    /// a later slice; `disruptive_stale_node_churns_term_but_never_wins` pins the
    /// current behavior so that work has a regression anchor.
    fn observe_term(&mut self, term: u64, rng: &mut dyn RaftRng, out: &mut Effects) -> bool {
        if term > self.storage.current_term() {
            self.storage.set_current_term(term);
            self.storage.set_voted_for(None);
            let was_leader = self.role == Role::Leader;
            self.role = Role::Follower;
            self.votes.clear();
            // Drop leader-only volatile state (reinitialized on the next election).
            self.next_index.clear();
            self.match_index.clear();
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
        // nextIndex/matchIndex on every election).
        self.next_index.clear();
        self.match_index.clear();
    }

    /// Become `Leader` if the current vote tally is a strict majority of voters
    /// (Figure 2, "Candidates"). On winning: cancel the election timer; INITIALIZE
    /// the per-peer replication state (`nextIndex = lastLogIndex + 1`, `matchIndex =
    /// 0`, Figure 2 "Leaders"); append a no-op to our own log (section 8: a
    /// current-term entry the new leader can commit, which is also what lets the
    /// commit-only-current-term rule carry forward prior-term entries; see
    /// [`RaftNode::maybe_advance_commit`]); then broadcast the initial replication
    /// AppendEntries and arm the heartbeat timer.
    fn maybe_become_leader(&mut self, out: &mut Effects) {
        if self.role != Role::Candidate {
            return;
        }
        let needed = self.voters.len() / 2 + 1;
        if self.votes.len() < needed {
            return;
        }
        self.role = Role::Leader;
        out.cancel_timer(ELECTION_TIMEOUT);

        // Initialize leader replication state for every peer (Figure 2, "Leaders":
        // on election, nextIndex = last log index + 1, matchIndex = 0). Our own
        // match is implicit (we always have our whole log); the commit counter
        // counts the leader itself separately.
        let next = self.storage.last_log_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for &peer in &self.voters {
            if peer != self.id {
                self.next_index.insert(peer, next);
                self.match_index.insert(peer, 0);
            }
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

    /// Broadcast a replication `AppendEntries` to every other voter (Figure 2,
    /// "Leaders"). Each peer's RPC carries `prev` and `entries` derived from that
    /// peer's `nextIndex`, so this is heartbeat AND log shipping in one: a
    /// caught-up peer gets an empty `entries` (a pure heartbeat), a lagging peer
    /// gets the entries it is missing. Replaces 3a's always-empty heartbeat.
    fn broadcast_heartbeat(&self, out: &mut Effects) {
        for &peer in &self.voters {
            if peer != self.id {
                self.send_append_entries_to(peer, out);
            }
        }
    }

    /// Send a single replication `AppendEntries` to `peer` from its `nextIndex`
    /// (Figure 2, "Leaders": send AppendEntries with log entries starting at
    /// nextIndex). `prev_log_index = nextIndex - 1`, `prev_log_term =
    /// term_at(prev_log_index)`, `entries = entries_from(nextIndex)`, `leader_commit
    /// = commit_index`. Only meaningful while `role == Leader`.
    fn send_append_entries_to(&self, peer: NodeId, out: &mut Effects) {
        let term = self.storage.current_term();
        let next = self.next_index.get(&peer).copied().unwrap_or(1);
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
    fn maybe_advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let current_term = self.storage.current_term();
        let last = self.storage.last_log_index();
        let majority = self.voters.len() / 2 + 1;

        // Scan from the highest index downward; the first N that satisfies both
        // clauses is the new commit index (commit is monotone, so a higher N
        // dominates). Stop at commit_index + 1 (no point re-confirming what is
        // already committed).
        let mut new_commit = self.commit_index;
        let mut n = last;
        while n > self.commit_index {
            // Clause 2 (5.4.2): only current-term entries are committable by count.
            if self.storage.term_at(n) == current_term {
                // Count voters with match_index >= n. The leader counts itself (it
                // holds every entry up to `last`, so it replicates N for any N <=
                // last); each peer counts if its tracked match_index >= n.
                let mut replicated = 1; // the leader itself
                for (&peer, &mi) in &self.match_index {
                    let _ = peer;
                    if mi >= n {
                        replicated += 1;
                    }
                }
                if replicated >= majority {
                    new_commit = n;
                    break;
                }
            }
            n -= 1;
        }

        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed();
        }
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
        let index = self.storage.last_log_index() + 1;
        let term = self.storage.current_term();
        self.storage.append(LogEntry {
            term,
            index,
            payload,
        });
        // Replicate at once so a quiet cluster does not wait a heartbeat interval,
        // and so a single-voter leader's own append commits immediately.
        self.broadcast_heartbeat(out);
        self.maybe_advance_commit();
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

    /// Record a granted vote from `voter` while a `Candidate` in `term`, then
    /// promote to leader if the tally is now a majority. Only same-term votes for a
    /// live candidacy count. Idempotent per voter (the tally is a `BTreeSet`), so a
    /// duplicated `RequestVoteResp` cannot inflate the count.
    fn record_vote(&mut self, voter: NodeId, term: u64, out: &mut Effects) {
        if self.role != Role::Candidate || term != self.storage.current_term() {
            return;
        }
        self.votes.insert(voter);
        self.maybe_become_leader(out);
    }
}

// ---------------------------------------------------------------------------
// Sim adapter + DST scenarios (test/dev only).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use core::time::Duration;
    use std::collections::BTreeMap;

    use ironcache_sim::NodeId as SimId;
    use ironcache_sim::{Network, SimCtx};

    // -- id mapping ---------------------------------------------------------
    //
    // The engine's `NodeId` and the sim's `NodeId` are distinct types (the engine
    // is transport-agnostic). The mapping is the identity on the inner `u64`, which
    // keeps test reasoning simple and is all an adapter has to commit to.

    fn to_sim(id: NodeId) -> SimId {
        SimId(id.0)
    }

    fn to_raft(id: SimId) -> NodeId {
        NodeId(id.0)
    }

    // -- RaftRng wrapper over SimCtx ---------------------------------------

    /// A [`RaftRng`] that draws from the sim's single seeded RNG via
    /// [`SimCtx::gen_below`], so the engine's election jitter is part of the
    /// reproducible run. It borrows the `SimCtx` for the duration of one engine
    /// call and is dropped before the effects are drained back onto the ctx.
    struct SimRng<'a, 'c> {
        ctx: &'a mut SimCtx<'c, RaftMsg>,
    }

    impl RaftRng for SimRng<'_, '_> {
        fn gen_below(&mut self, bound: u64) -> u64 {
            self.ctx.gen_below(bound)
        }
    }

    // -- the SimNode adapter -----------------------------------------------

    /// Wraps a pure [`RaftNode`] as an [`ironcache_sim::SimNode`].
    ///
    /// Each callback: reads `now` from the ctx; builds a [`SimRng`] borrowing the
    /// ctx and runs the engine into a local [`Effects`]; drops the borrow; then
    /// drains the effects onto the ctx (timer ops first, then sends, matching the
    /// sim's drain order). The initial election timer is armed by [`RaftSimNode`]'s
    /// own `start`, invoked by the [`RaftCluster`] builder right after `add_node`.
    struct RaftSimNode {
        engine: RaftNode<MemStorage>,
        started: bool,
    }

    impl RaftSimNode {
        fn new(id: NodeId, voters: BTreeSet<NodeId>, config: RaftConfig) -> Self {
            RaftSimNode {
                engine: RaftNode::new(id, voters, MemStorage::new(), config),
                started: false,
            }
        }

        /// Run the engine's [`RaftNode::start`] exactly once (idempotent), arming
        /// the initial election timer.
        ///
        /// The sim consumes a node in `add_node` and offers only a read accessor, so
        /// the harness cannot reach in and call `start` directly. Instead, the
        /// adapter drives `start` LAZILY on a node's first callback: the
        /// [`RaftCluster`] builder injects one harmless bootstrap delivery per node
        /// (a term-0 self `AppendEntries`, dropped by the engine as same-term noise
        /// but used here purely as the "you are now live, arm your timer" trigger),
        /// and this method runs `start` before that first message is processed. The
        /// engine reads no clock on `start`, so the ctx's `now` is the correct
        /// argument. A re-arm by the bootstrap message itself is harmless (latest
        /// arm wins).
        fn ensure_started(&mut self, ctx: &mut SimCtx<'_, RaftMsg>) {
            if self.started {
                return;
            }
            self.started = true;
            let now = ctx.now();
            let mut effects = Effects::new();
            {
                let mut rng = SimRng { ctx };
                self.engine.start(now, &mut rng, &mut effects);
            }
            drain(ctx, effects);
        }
    }

    impl ironcache_sim::SimNode for RaftSimNode {
        type Msg = RaftMsg;

        fn on_message(&mut self, from: SimId, msg: RaftMsg, ctx: &mut SimCtx<'_, RaftMsg>) {
            self.ensure_started(ctx);
            let now = ctx.now();
            let mut effects = Effects::new();
            {
                let mut rng = SimRng { ctx };
                self.engine
                    .on_message(now, &mut rng, to_raft(from), msg, &mut effects);
            }
            drain(ctx, effects);
        }

        fn on_timer(&mut self, token: u64, ctx: &mut SimCtx<'_, RaftMsg>) {
            self.ensure_started(ctx);
            let now = ctx.now();
            let mut effects = Effects::new();
            {
                let mut rng = SimRng { ctx };
                self.engine.on_timer(now, &mut rng, token, &mut effects);
            }
            drain(ctx, effects);
        }
    }

    /// Apply a finished step's [`Effects`] onto the sim ctx: timer ops first, then
    /// sends, mapping raft ids to sim ids. This mirrors the sim's own drain order.
    fn drain(ctx: &mut SimCtx<'_, RaftMsg>, effects: Effects) {
        for op in effects.timer_ops {
            match op {
                TimerOp::Set { token, after } => ctx.set_timer(token, after),
                TimerOp::Cancel { token } => ctx.cancel_timer(token),
            }
        }
        for (to, msg) in effects.sends {
            ctx.send(to_sim(to), msg);
        }
    }

    // -- cluster builder ----------------------------------------------------

    /// A small test harness: builds a [`Network`] of `n` Raft voters (ids 1..=n),
    /// arms each one's initial election timer, and exposes role/term reads via the
    /// new [`Network::node`] accessor.
    struct RaftCluster {
        net: Network<RaftSimNode>,
        ids: Vec<NodeId>,
    }

    impl RaftCluster {
        /// Build `n` voters (ids `1..=n`) with `config` on a network seeded with
        /// `seed`, then bootstrap each so its initial election timer is armed.
        fn new(n: u64, seed: u64, config: RaftConfig) -> Self {
            let ids: Vec<NodeId> = (1..=n).map(NodeId).collect();
            let voters: BTreeSet<NodeId> = ids.iter().copied().collect();
            let mut net = Network::new(seed);
            for &id in &ids {
                net.add_node(to_sim(id), RaftSimNode::new(id, voters.clone(), config));
            }
            let mut cluster = RaftCluster { net, ids };
            cluster.start_all();
            cluster
        }

        /// Bootstrap every node: inject one harmless self-addressed delivery so each
        /// node's first callback runs (which triggers its one-time
        /// [`RaftNode::start`], arming the initial election timer; see
        /// [`RaftSimNode::ensure_started`]).
        ///
        /// The bootstrap message is a term-0 `AppendEntries` from the node to
        /// itself. On a fresh term-0 follower this is `term == currentTerm`, so the
        /// engine's recognize-leader path re-arms the election timer and changes no
        /// role state; combined with the lazy `start`, the only durable effect is
        /// "the election timer is armed", which is exactly what is wanted. It is
        /// fully deterministic, so the seed sweep's replay assertion holds.
        fn start_all(&mut self) {
            for &id in &self.ids {
                self.net.tell(
                    to_sim(id),
                    to_sim(id),
                    RaftMsg::AppendEntries {
                        term: 0,
                        leader: id,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: Vec::new(),
                        leader_commit: 0,
                    },
                );
            }
        }

        fn run_until_idle(&mut self, max_steps: usize) -> usize {
            self.net.run_until_idle(max_steps)
        }

        fn role(&self, id: NodeId) -> Role {
            self.net
                .node(to_sim(id))
                .expect("node exists")
                .engine
                .role()
        }

        fn term(&self, id: NodeId) -> u64 {
            self.net
                .node(to_sim(id))
                .expect("node exists")
                .engine
                .current_term()
        }

        fn leaders(&self) -> Vec<NodeId> {
            self.ids
                .iter()
                .copied()
                .filter(|&id| self.role(id) == Role::Leader)
                .collect()
        }

        // -- 3b log/commit accessors ---------------------------------------

        /// A node's committed index (the 3b watermark; see [`RaftNode::commit_index`]).
        fn commit_index(&self, id: NodeId) -> u64 {
            self.net
                .node(to_sim(id))
                .expect("node exists")
                .engine
                .commit_index()
        }

        /// A node's last-applied watermark.
        fn last_applied(&self, id: NodeId) -> u64 {
            self.net
                .node(to_sim(id))
                .expect("node exists")
                .engine
                .last_applied()
        }

        /// A node's log entries, cloned for inspection (via the storage accessor).
        fn log(&self, id: NodeId) -> Vec<LogEntry> {
            self.net
                .node(to_sim(id))
                .expect("node exists")
                .engine
                .storage()
                .log()
                .to_vec()
        }

        /// Inject a client proposal at `leader` by self-`tell`ing a
        /// [`RaftMsg::Propose`] (delivered through the same deterministic transport
        /// as any message, so it is part of the reproducible run). On a non-leader
        /// it is a no-op (the engine rejects it), which is exactly the redirect
        /// behavior under test.
        fn propose(&mut self, leader: NodeId, payload: EntryPayload) {
            self.net
                .tell(to_sim(leader), to_sim(leader), RaftMsg::Propose { payload });
        }
    }

    // -- election-safety checker -------------------------------------------

    /// Election Safety (Raft section 5.2, the headline invariant): at most one
    /// leader can be elected in a given term. We assert it over the OBSERVABLE
    /// state: group the current leaders by their `currentTerm`; no term may have
    /// two. Run after every scenario (and at quiescent points within).
    ///
    /// Note this is the strongest property a state-snapshot checker can assert
    /// without a per-term history; because a node's term is monotonic and a leader
    /// holds the term it won, two distinct same-term leaders co-existing at ANY
    /// quiescent observation is exactly the split-brain this forbids.
    fn assert_election_safety(cluster: &RaftCluster) {
        let mut by_term: BTreeMap<u64, Vec<NodeId>> = BTreeMap::new();
        for &id in &cluster.ids {
            if cluster.role(id) == Role::Leader {
                by_term.entry(cluster.term(id)).or_default().push(id);
            }
        }
        for (term, leaders) in &by_term {
            assert!(
                leaders.len() <= 1,
                "election safety violated: term {term} has leaders {leaders:?}"
            );
        }
    }

    // -- 3b invariant checkers: Log Matching + State Machine Safety --------

    /// The Log Matching Property (Raft section 5.3): "if two logs contain an entry
    /// with the same index and term, then the logs are identical in all entries up
    /// through that index." We assert it pairwise over every node pair: for each
    /// common index, if both logs hold an entry there with the SAME term, then every
    /// entry at-or-before that index (term AND payload) must be identical between the
    /// two logs. A divergence here means replication corrupted a log; it is the
    /// structural invariant that underwrites the up-to-date check and commit safety.
    fn assert_log_matching(cluster: &RaftCluster) {
        let logs: Vec<(NodeId, Vec<LogEntry>)> = cluster
            .ids
            .iter()
            .map(|&id| (id, cluster.log(id)))
            .collect();
        for i in 0..logs.len() {
            for j in (i + 1)..logs.len() {
                let (id_a, log_a) = (&logs[i].0, &logs[i].1);
                let (id_b, log_b) = (&logs[j].0, &logs[j].1);
                let common = log_a.len().min(log_b.len());
                for k in 0..common {
                    let ea = &log_a[k];
                    let eb = &log_b[k];
                    // Logs are 1-based and contiguous, so position k is index k+1 in
                    // both; assert the index bookkeeping holds before comparing terms.
                    let idx = u64::try_from(k + 1).unwrap_or(u64::MAX);
                    assert_eq!(ea.index, idx, "node {id_a:?} log index bookkeeping");
                    assert_eq!(eb.index, idx, "node {id_b:?} log index bookkeeping");
                    if ea.term == eb.term {
                        // Same (index, term): every entry up through k must match
                        // exactly (term and payload) on both logs.
                        for m in 0..=k {
                            assert_eq!(
                                log_a[m],
                                log_b[m],
                                "log matching violated: nodes {id_a:?} and {id_b:?} \
                                 agree at index {idx} (term {}) but differ at index {}",
                                ea.term,
                                m + 1
                            );
                        }
                    }
                }
            }
        }
    }

    /// State Machine Safety (Raft section 5.4.3 / Figure 3): "if a server has
    /// applied a log entry at a given index to its state machine, no other server
    /// will ever apply a different log entry for the same index." Because 3b's apply
    /// is a sink, we assert the equivalent over the COMMITTED prefix: no two nodes
    /// hold a DIFFERENT entry at any index that BOTH consider committed (index <=
    /// their respective `commit_index`). A committed entry is, by definition, agreed;
    /// a divergence in a committed prefix is exactly the data loss 5.4.2 forbids.
    ///
    /// This is a snapshot check; the cross-TIME guarantee (a once-committed entry is
    /// never later overwritten) is enforced by [`CommitLedger`], which records every
    /// committed entry ever observed and re-checks it on each step.
    fn assert_state_machine_safety(cluster: &RaftCluster) {
        for i in 0..cluster.ids.len() {
            for j in (i + 1)..cluster.ids.len() {
                let id_a = cluster.ids[i];
                let id_b = cluster.ids[j];
                let log_a = cluster.log(id_a);
                let log_b = cluster.log(id_b);
                let committed = cluster.commit_index(id_a).min(cluster.commit_index(id_b));
                for idx in 1..=committed {
                    let pos = usize::try_from(idx - 1).unwrap_or(usize::MAX);
                    let ea = log_a.get(pos);
                    let eb = log_b.get(pos);
                    // Both nodes claim idx committed, so both MUST have the entry and
                    // the entries must be identical (a committed entry is agreed).
                    assert_eq!(
                        ea, eb,
                        "state machine safety violated: nodes {id_a:?} and {id_b:?} \
                         both committed index {idx} but hold different entries \
                         ({ea:?} vs {eb:?})"
                    );
                }
            }
        }
    }

    /// A cross-TIME ledger of every committed entry ever observed, to prove the
    /// strongest State Machine Safety statement: once an entry is committed at an
    /// index, NO node ever holds a different entry at that index again (it is never
    /// overwritten or lost). A snapshot check cannot see this; the ledger is sampled
    /// on every step chunk and remembers (index -> the committed entry), then asserts
    /// every node's current log is consistent with that history.
    ///
    /// This is THE Figure-8 gate's witness: the section-5.4.2 commit rule exists
    /// precisely so this ledger never has to overwrite an entry it already recorded.
    #[derive(Default)]
    struct CommitLedger {
        /// index -> the entry that was observed committed there (the durable truth).
        committed: BTreeMap<u64, LogEntry>,
    }

    impl CommitLedger {
        fn new() -> Self {
            Self::default()
        }

        /// Sample the cluster: for every node, record each entry at-or-below that
        /// node's `commit_index` as durable, and assert it never contradicts a
        /// previously recorded entry at the same index. Recording from EVERY node is
        /// safe because the committed prefix is, by the algorithm's correctness, the
        /// same on all nodes that have it (snapshot SMS guards the same-step case).
        fn observe_and_check(&mut self, cluster: &RaftCluster) {
            for &id in &cluster.ids {
                let ci = cluster.commit_index(id);
                let log = cluster.log(id);
                for idx in 1..=ci {
                    let pos = usize::try_from(idx - 1).unwrap_or(usize::MAX);
                    let Some(entry) = log.get(pos) else {
                        panic!("node {id:?} claims commit_index {ci} but lacks index {idx}");
                    };
                    match self.committed.get(&idx) {
                        Some(prev) => assert_eq!(
                            prev, entry,
                            "state machine safety violated ACROSS TIME: index {idx} was \
                             committed as {prev:?} but node {id:?} now holds {entry:?} \
                             (a committed entry was overwritten - Figure-8 failure)"
                        ),
                        None => {
                            self.committed.insert(idx, entry.clone());
                        }
                    }
                }
            }
        }
    }

    /// Convenience: run all three structural invariants at a quiescent point.
    fn assert_3b_invariants(cluster: &RaftCluster) {
        assert_election_safety(cluster);
        assert_log_matching(cluster);
        assert_state_machine_safety(cluster);
    }

    // -- scenario 1: clean start elects exactly one leader -----------------

    #[test]
    fn clean_start_elects_one_leader() {
        let mut cluster = RaftCluster::new(3, 1, RaftConfig::default());
        let ran = cluster.run_until_idle(100_000);
        assert!(ran > 0, "the cluster should have done work");
        assert_election_safety(&cluster);
        let leaders = cluster.leaders();
        assert_eq!(
            leaders.len(),
            1,
            "exactly one leader after a clean start, got {leaders:?}"
        );
        // Every node should agree on the term, and it is the leader's term.
        let leader = leaders[0];
        let lterm = cluster.term(leader);
        for &id in &cluster.ids {
            assert_eq!(
                cluster.term(id),
                lterm,
                "node {id:?} term disagrees with leader term {lterm}"
            );
        }
    }

    // -- scenario 2: no two leaders per term under a forced split vote -----

    #[test]
    fn no_two_leaders_per_term_under_split_vote() {
        // Across 50 seeds, the jittered election timeouts plus a message-latency
        // range produce many runs where two candidates stand close together and
        // split the vote. Election safety must hold throughout (asserted at every
        // quiescent checkpoint), and the cluster must STILL converge to exactly one
        // leader (the fresh jitter drawn on each RE-arm after a failed round
        // eventually breaks the tie). The base+jitter below give a [150ms, 300ms)
        // timeout window.
        let config = RaftConfig {
            election_timeout_base: Duration::from_millis(150),
            election_timeout_jitter: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(50),
        };
        for seed in 0..50u64 {
            let mut cluster = RaftCluster::new(5, seed, config);
            cluster
                .net
                .set_latency(Duration::from_millis(1), Duration::from_millis(20));
            // Step in chunks, asserting safety at every quiescent checkpoint.
            for _ in 0..40 {
                cluster.net.run_steps(200);
                assert_election_safety(&cluster);
            }
            cluster.run_until_idle(200_000);
            assert_election_safety(&cluster);
            let leaders = cluster.leaders();
            assert_eq!(
                leaders.len(),
                1,
                "seed {seed}: must converge to one leader, got {leaders:?}"
            );
        }
    }

    // -- scenario 3: leader isolation, partition, then heal ----------------

    /// Run until exactly one leader exists (or `max_rounds` chunks elapse).
    fn run_to_single_leader(cluster: &mut RaftCluster, chunk: usize, max_rounds: usize) -> NodeId {
        for _ in 0..max_rounds {
            cluster.net.run_steps(chunk);
            assert_election_safety(cluster);
            let leaders = cluster.leaders();
            if leaders.len() == 1 {
                return leaders[0];
            }
        }
        let leaders = cluster.leaders();
        panic!("did not converge to a single leader; leaders = {leaders:?}");
    }

    #[test]
    fn leader_isolation_partition_then_heal() {
        let config = RaftConfig::default();
        let mut cluster = RaftCluster::new(5, 7, config);
        let old_leader = run_to_single_leader(&mut cluster, 500, 200);
        let old_term = cluster.term(old_leader);
        assert_election_safety(&cluster);

        // Isolate the leader from the other four. The majority side (four nodes)
        // must elect a NEW leader at a HIGHER term; the isolated old leader cannot
        // get votes and cannot stay authoritative.
        let others: Vec<SimId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != old_leader)
            .map(to_sim)
            .collect();
        cluster.net.partition(&[to_sim(old_leader)], &others);

        // Let the majority side run an election. Assert safety throughout.
        let mut new_leader = None;
        for _ in 0..400 {
            cluster.net.run_steps(500);
            assert_election_safety(&cluster);
            let majority_leaders: Vec<NodeId> = cluster
                .leaders()
                .into_iter()
                .filter(|&id| id != old_leader)
                .collect();
            if majority_leaders.len() == 1 {
                new_leader = Some(majority_leaders[0]);
                break;
            }
        }
        let new_leader = new_leader.expect("majority side must elect a new leader");
        assert!(
            cluster.term(new_leader) > old_term,
            "new leader term {} must exceed old term {old_term}",
            cluster.term(new_leader)
        );
        assert_election_safety(&cluster);

        // Heal. The old leader, on hearing the higher term, steps down to Follower;
        // the cluster converges to exactly one leader.
        cluster.net.heal();
        for _ in 0..400 {
            cluster.net.run_steps(500);
            assert_election_safety(&cluster);
            if cluster.role(old_leader) == Role::Follower && cluster.leaders().len() == 1 {
                break;
            }
        }
        assert_election_safety(&cluster);
        assert_eq!(
            cluster.role(old_leader),
            Role::Follower,
            "the isolated old leader must step down after heal"
        );
        let leaders = cluster.leaders();
        assert_eq!(
            leaders.len(),
            1,
            "the cluster must converge to one leader after heal, got {leaders:?}"
        );
    }

    // -- scenario 4: single-voter cluster self-elects ----------------------

    #[test]
    fn single_voter_self_elects() {
        let mut cluster = RaftCluster::new(1, 3, RaftConfig::default());
        cluster.run_until_idle(10_000);
        assert_election_safety(&cluster);
        let only = cluster.ids[0];
        assert_eq!(
            cluster.role(only),
            Role::Leader,
            "a single-voter cluster must self-elect immediately"
        );
        assert_eq!(cluster.leaders(), vec![only]);
    }

    // -- scenario 5: determinism + safety over a seed sweep ----------------

    /// Replay scenario 1 (clean start) for `seed`, returning the final trace and
    /// the elected leader so two runs can be compared byte-for-byte.
    fn replay_clean_start(seed: u64) -> (Vec<ironcache_sim::TraceRecord>, Vec<NodeId>) {
        let mut cluster = RaftCluster::new(3, seed, RaftConfig::default());
        cluster.run_until_idle(200_000);
        assert_election_safety(&cluster);
        (cluster.net.trace().to_vec(), cluster.leaders())
    }

    /// Replay scenario 3 (partition then heal) for `seed`, returning the final
    /// trace. The fault script is fixed (partition the FIRST elected leader), so
    /// two same-seed runs are identical.
    fn replay_partition(seed: u64) -> Vec<ironcache_sim::TraceRecord> {
        let config = RaftConfig::default();
        let mut cluster = RaftCluster::new(5, seed, config);
        let old_leader = run_to_single_leader(&mut cluster, 500, 200);
        let others: Vec<SimId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != old_leader)
            .map(to_sim)
            .collect();
        cluster.net.partition(&[to_sim(old_leader)], &others);
        cluster.net.run_steps(50_000);
        assert_election_safety(&cluster);
        cluster.net.heal();
        cluster.net.run_steps(50_000);
        assert_election_safety(&cluster);
        cluster.net.trace().to_vec()
    }

    #[test]
    fn determinism_and_safety_seed_sweep() {
        for seed in 0..200u64 {
            // Scenario 1 across the sweep: each run elects exactly one leader, is
            // election-safe, and replays byte-identically.
            let (trace_a, leaders_a) = replay_clean_start(seed);
            let (trace_b, leaders_b) = replay_clean_start(seed);
            assert_eq!(
                leaders_a.len(),
                1,
                "seed {seed}: clean start must elect exactly one leader"
            );
            assert_eq!(
                trace_a, trace_b,
                "seed {seed}: clean-start trace must replay byte-identically"
            );
            assert_eq!(leaders_a, leaders_b, "seed {seed}: same leader on replay");

            // Scenario 3 across the sweep: same-seed replay is byte-identical and
            // election-safe (asserted inside replay_partition).
            let p_a = replay_partition(seed);
            let p_b = replay_partition(seed);
            assert_eq!(
                p_a, p_b,
                "seed {seed}: partition-then-heal trace must replay byte-identically"
            );
        }
    }

    // -- engine-direct safety unit tests -----------------------------------
    //
    // These drive the pure engine (no sim) to pin the exact vote-grant rules that
    // the integration scenarios only observe at the leader-count granularity.

    /// A deterministic [`RaftRng`] for engine-direct tests where the election
    /// jitter value is irrelevant (always 0).
    struct ZeroRng;
    impl RaftRng for ZeroRng {
        fn gen_below(&mut self, _bound: u64) -> u64 {
            0
        }
    }

    /// Whether `effects` contains a granted `RequestVoteResp` addressed to
    /// `candidate`.
    fn reply_granted(effects: &Effects, candidate: NodeId) -> bool {
        effects.sends.iter().any(|(to, msg)| {
            *to == candidate
                && matches!(
                    msg,
                    RaftMsg::RequestVoteResp {
                        vote_granted: true,
                        ..
                    }
                )
        })
    }

    #[test]
    fn a_voter_grants_at_most_one_candidate_per_term() {
        // The double-vote guard (Figure 2 RequestVote rule 2): votedFor in
        // {None, candidate}. A voter that granted candidate A in a term must refuse
        // a DIFFERENT candidate B in that same term, but may idempotently re-grant A.
        let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
        let mut node = RaftNode::new(NodeId(2), voters, MemStorage::new(), RaftConfig::default());
        let mut rng = ZeroRng;

        let mut e1 = Effects::new();
        node.on_request_vote(&mut rng, 5, NodeId(1), 0, 0, &mut e1);
        assert!(
            reply_granted(&e1, NodeId(1)),
            "first candidate in the term is granted"
        );
        assert_eq!(node.current_term(), 5);

        let mut e2 = Effects::new();
        node.on_request_vote(&mut rng, 5, NodeId(3), 0, 0, &mut e2);
        assert!(
            !reply_granted(&e2, NodeId(3)),
            "a second distinct candidate in the same term must be refused"
        );

        let mut e3 = Effects::new();
        node.on_request_vote(&mut rng, 5, NodeId(1), 0, 0, &mut e3);
        assert!(
            reply_granted(&e3, NodeId(1)),
            "the SAME candidate may be re-granted (idempotent)"
        );
    }

    #[test]
    fn up_to_date_check_is_term_then_index() {
        // Section 5.4.1: a candidate log is at least as up-to-date iff its last term
        // is higher, or the last term is equal and its index is >= ours.
        let voters: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
        let mut storage = MemStorage::new();
        storage.append(LogEntry {
            term: 1,
            index: 1,
            payload: EntryPayload::Noop,
        });
        storage.append(LogEntry {
            term: 2,
            index: 2,
            payload: EntryPayload::Noop,
        });
        storage.append(LogEntry {
            term: 2,
            index: 3,
            payload: EntryPayload::Noop,
        });
        let node = RaftNode::new(NodeId(1), voters, storage, RaftConfig::default());
        // Our last entry is (index 3, term 2).
        assert!(
            !node.candidate_log_up_to_date(99, 1),
            "a lower last term is stale even with a longer index"
        );
        assert!(
            !node.candidate_log_up_to_date(2, 2),
            "same term, shorter index is stale"
        );
        assert!(
            node.candidate_log_up_to_date(3, 2),
            "same term, equal index is up-to-date"
        );
        assert!(
            node.candidate_log_up_to_date(4, 2),
            "same term, longer index is up-to-date"
        );
        assert!(
            node.candidate_log_up_to_date(1, 3),
            "a higher last term is up-to-date even with a shorter index"
        );
    }

    #[test]
    fn disruptive_stale_node_churns_term_but_never_wins() {
        // ADR-0027 / observe_term KNOWN LIMITATION (no Pre-Vote): a higher-term
        // RequestVote from a node whose log is too stale to win still forces the
        // recipient to ADOPT the higher term and step down (the mechanism that
        // churns a healthy leader), yet the vote is REFUSED, so the disruptor never
        // actually wins. Election Safety is preserved; only liveness degrades.
        let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
        let mut storage = MemStorage::new();
        storage.set_current_term(5);
        // A non-empty log: strictly more up-to-date than the disruptor's empty log.
        storage.append(LogEntry {
            term: 5,
            index: 1,
            payload: EntryPayload::Noop,
        });
        let mut node = RaftNode::new(NodeId(1), voters, storage, RaftConfig::default());
        let mut rng = ZeroRng;
        let mut eff = Effects::new();
        // Disruptor (node 2) at a HIGHER term 9 with a STALE (empty) log.
        node.on_request_vote(&mut rng, 9, NodeId(2), 0, 0, &mut eff);
        assert_eq!(
            node.current_term(),
            9,
            "the higher term is adopted (the churn mechanism)"
        );
        assert_eq!(
            node.role(),
            Role::Follower,
            "the recipient steps down to follower"
        );
        assert!(
            !reply_granted(&eff, NodeId(2)),
            "the stale-log disruptor is refused the vote, so it cannot win"
        );
    }

    #[test]
    fn election_safety_holds_under_message_drops_and_converges_after_heal() {
        // The most valuable nemesis: dropped (and thus effectively reordered/retried)
        // RequestVote / RequestVoteResp messages are exactly where double-vote and
        // double-tally bugs hide. Election Safety must hold throughout a lossy run,
        // and once the drops stop the cluster must still converge to one leader.
        let config = RaftConfig::default();
        for seed in 0..50u64 {
            let mut cluster = RaftCluster::new(5, seed, config);
            cluster
                .net
                .set_latency(Duration::from_millis(1), Duration::from_millis(20));
            cluster.net.set_drop_prob(0.2);
            for _ in 0..60 {
                cluster.net.run_steps(300);
                assert_election_safety(&cluster);
            }
            // Heal the drops; the cluster must converge to exactly one leader.
            cluster.net.set_drop_prob(0.0);
            cluster.run_until_idle(500_000);
            assert_election_safety(&cluster);
            assert_eq!(
                cluster.leaders().len(),
                1,
                "seed {seed}: must converge to one leader once drops stop, got {:?}",
                cluster.leaders()
            );
        }
    }

    // =====================================================================
    // 3b DST scenarios (log replication + commit; sections 5.3, 5.4.2).
    // =====================================================================

    /// Run `cluster` to a single leader, then return it. A thin wrapper that also
    /// asserts the 3b structural invariants once quiescent.
    fn elect_one_leader(cluster: &mut RaftCluster) -> NodeId {
        let leader = run_to_single_leader(cluster, 500, 200);
        // Drain any in-flight replication so the no-op the leader appended on
        // election settles before we start proposing.
        cluster.net.run_steps(5_000);
        assert_3b_invariants(cluster);
        leader
    }

    fn payload(tag: u8) -> EntryPayload {
        EntryPayload::Bytes(vec![tag])
    }

    // -- scenario 1: a replicated entry commits and all logs converge ------

    #[test]
    fn replicated_entry_commits_and_converges() {
        // 3 voters. Elect a leader, propose several entries, run to quiescence.
        // Assert: every node's log converges to the same sequence, commit_index
        // advances past the proposals on a majority, last_applied tracks it, and the
        // two 3b structural invariants hold.
        let mut cluster = RaftCluster::new(3, 11, RaftConfig::default());
        let leader = elect_one_leader(&mut cluster);
        let commit_before = cluster.commit_index(leader);

        // Propose 5 opaque entries at the leader.
        for tag in 0..5u8 {
            cluster.propose(leader, payload(tag));
            cluster.net.run_steps(2_000);
            assert_3b_invariants(&cluster);
        }
        cluster.run_until_idle(100_000);
        assert_3b_invariants(&cluster);

        // The leader's commit_index advanced by at least the 5 proposals (plus the
        // election no-op committed transitively once a current-term entry committed).
        let leader_commit = cluster.commit_index(leader);
        assert!(
            leader_commit >= commit_before + 5,
            "leader commit_index must advance past the proposals: {commit_before} -> {leader_commit}"
        );

        // Every node converges to the leader's exact log, and every node commits and
        // applies up to (at least) the same watermark.
        let leader_log = cluster.log(leader);
        for &id in &cluster.ids {
            assert_eq!(
                cluster.log(id),
                leader_log,
                "node {id:?} log must converge to the leader's"
            );
            assert_eq!(
                cluster.commit_index(id),
                leader_commit,
                "node {id:?} commit_index must match the leader's once idle"
            );
            assert_eq!(
                cluster.last_applied(id),
                cluster.commit_index(id),
                "node {id:?} must have applied up to its commit_index (apply sink)"
            );
        }
    }

    // -- scenario 2 (D): log convergence after partition heal --------------

    #[test]
    fn log_convergence_after_partition_heal() {
        // 5 voters. The leader commits entries with a 3-node majority while 2 nodes
        // are partitioned off; on heal, the lagging nodes catch up via the nextIndex
        // decrement/retry backup and every log agrees up to commit_index.
        let mut cluster = RaftCluster::new(5, 23, RaftConfig::default());
        let leader = elect_one_leader(&mut cluster);

        // Choose two followers to isolate; keep the leader + two others as the
        // majority side (3 of 5 = a majority, so the leader stays authoritative and
        // can still commit).
        let followers: Vec<NodeId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != leader)
            .collect();
        let lagging = [followers[0], followers[1]];
        let majority_side: Vec<SimId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != lagging[0] && id != lagging[1])
            .map(to_sim)
            .collect();
        let lagging_side: Vec<SimId> = lagging.iter().copied().map(to_sim).collect();
        cluster.net.partition(&majority_side, &lagging_side);

        // Propose while partitioned; the majority side commits these.
        for tag in 0..6u8 {
            cluster.propose(leader, payload(tag));
            cluster.net.run_steps(3_000);
            assert_3b_invariants(&cluster);
        }
        cluster.net.run_steps(10_000);
        assert_3b_invariants(&cluster);

        let committed_while_partitioned = cluster.commit_index(leader);
        assert!(
            committed_while_partitioned > 0,
            "the majority side must commit while the minority is partitioned"
        );
        // The lagging nodes did NOT receive the new entries (they were partitioned).
        for &id in &lagging {
            assert!(
                cluster.commit_index(id) < committed_while_partitioned,
                "lagging node {id:?} must trail the committed index while partitioned"
            );
        }

        // Heal; the lagging nodes must catch up via nextIndex decrement/retry.
        cluster.net.heal();
        cluster.run_until_idle(200_000);
        assert_3b_invariants(&cluster);

        let leader_log = cluster.log(leader);
        let target_commit = cluster.commit_index(leader);
        for &id in &cluster.ids {
            assert_eq!(
                cluster.log(id),
                leader_log,
                "node {id:?} log must converge after heal"
            );
            assert_eq!(
                cluster.commit_index(id),
                target_commit,
                "node {id:?} must catch up to the committed index after heal"
            );
        }
    }

    // -- scenario 3 (E1): the Figure-8 commit-safety gate ------------------

    /// THE Figure-8 safety rule (section 5.4.2), proven on the PURE engine where the
    /// exact log states the paper draws can be constructed deterministically (the
    /// sim's single-partition model cannot force the precise 5-way leader hand-off
    /// Figure 8 requires; driving the engine directly is the faithful reproduction).
    ///
    /// Construction mirrors Figure 8 exactly. Cluster S1..S5. We make S1 the leader
    /// in term 4 with this log: index1=(term1), index2=(term2). S2 also has
    /// index2=(term2) (S1 replicated it to S2 back in term 2). S3,S4,S5 have only
    /// index1. The danger the rule guards: S1 must NOT commit index2 (a term-2 entry)
    /// just because index2 is now on a MAJORITY {S1,S2,S1-counts-3rd?}. We drive S1's
    /// commit logic with match_index showing index2 on a majority and assert S1 does
    /// NOT advance commit to index2 (it is a PRIOR-term entry). Then S1 appends a
    /// term-4 entry at index3, gets it onto a majority, and NOW commit jumps to
    /// index3, carrying index2 with it transitively. That committed state is then
    /// durable: a subsequent leader cannot overwrite it (it is on a majority with a
    /// current-or-newer term).
    #[test]
    fn figure_8_commit_safety() {
        let voters: BTreeSet<NodeId> = (1..=5).map(NodeId).collect();
        // Build S1's storage exactly as Figure 8 (c): index1 term1, index2 term2.
        let mut s1 = MemStorage::new();
        s1.set_current_term(4);
        s1.append(LogEntry {
            term: 1,
            index: 1,
            payload: EntryPayload::Noop,
        });
        s1.append(LogEntry {
            term: 2,
            index: 2,
            payload: payload(0xAA),
        });
        let mut leader = RaftNode::new(NodeId(1), voters.clone(), s1, RaftConfig::default());
        let mut rng = ZeroRng;
        let now = Monotonic::from_since_origin(Duration::ZERO);

        // Force S1 to leader in term 4 the way the engine reaches it: it just won an
        // election. We replay that by hand so next_index/match_index initialize.
        // (Directly flipping role would skip the Figure-2 leader init.) Win via a
        // crafted vote round at the engine boundary instead: easier and faithful is
        // to call the internal promotion path through a candidate transition.
        // Simplest deterministic path: set role to Candidate with a full tally, then
        // run maybe_become_leader, which initializes next/match and appends the
        // term-4 no-op. To avoid the no-op perturbing the index math below, we model
        // the post-election state by initializing leader markers ourselves and then
        // exercising ONLY the commit rule. The commit rule is the unit under test.
        promote_to_leader_for_test(&mut leader);

        // After promotion the leader appended a term-4 no-op at index3. Figure 8's
        // index2 (term 2) is now the SECOND entry; the no-op is index3 (term 4).
        assert_eq!(leader.storage().last_log_index(), 3);
        assert_eq!(
            leader.storage().term_at(2),
            2,
            "index2 is the prior-term entry"
        );
        assert_eq!(
            leader.storage().term_at(3),
            4,
            "index3 is the current-term no-op"
        );
        assert_eq!(leader.commit_index(), 0, "nothing committed yet");

        // STEP 1 (the dangerous one): a MAJORITY now stores index2 (the term-2
        // entry). Model S2 acknowledging up to index2, and S1 itself has it: that is
        // 2 of 5; bring in S3 acking index2 too -> 3 of 5 = a majority storing
        // index2. The section-5.4.2 rule MUST refuse to commit index2 by this count,
        // because index2 is from a PRIOR term (term 2 != currentTerm 4).
        let mut out = Effects::new();
        leader.on_append_entries_resp(now, &mut rng, NodeId(2), 4, true, 2, &mut out);
        leader.on_append_entries_resp(now, &mut rng, NodeId(3), 4, true, 2, &mut out);
        assert_eq!(
            leader.commit_index(),
            0,
            "FIGURE 8: a prior-term entry (index2, term2) on a MAJORITY must NOT be \
             committed by replica count (section 5.4.2)"
        );

        // STEP 2: the leader replicates its CURRENT-term entry (index3, term4) to a
        // majority. The moment index3 is on a majority, commit jumps to index3 - and
        // index2 commits TRANSITIVELY (Log Matching: index2 precedes the now-committed
        // index3). This is the ONLY way the prior-term entry becomes committed.
        leader.on_append_entries_resp(now, &mut rng, NodeId(2), 4, true, 3, &mut out);
        leader.on_append_entries_resp(now, &mut rng, NodeId(3), 4, true, 3, &mut out);
        assert_eq!(
            leader.commit_index(),
            3,
            "once a CURRENT-term entry (index3, term4) is on a majority, commit \
             advances to it and carries index2 with it transitively"
        );
        assert_eq!(
            leader.last_applied(),
            3,
            "apply pipeline follows commit_index"
        );

        // STEP 3 (durability): index2 is now committed. Assert the engine never
        // un-commits it and never overwrites it. Re-running the commit rule (more
        // acks, idle heartbeats) only ever advances or holds commit, never rewinds.
        let committed_log = leader.storage().log().to_vec();
        leader.on_append_entries_resp(now, &mut rng, NodeId(4), 4, true, 3, &mut out);
        leader.on_append_entries_resp(now, &mut rng, NodeId(5), 4, true, 3, &mut out);
        assert_eq!(
            leader.commit_index(),
            3,
            "commit_index is monotone: extra acks never rewind it"
        );
        assert_eq!(
            &leader.storage().log()[..2],
            &committed_log[..2],
            "the committed prefix (index1, index2) is never overwritten"
        );
    }

    /// A second, end-to-end Figure-8 witness over the FULL sim across a seed sweep:
    /// drive leader changes and partitions so old-term entries get replicated widely,
    /// and assert the cross-TIME [`CommitLedger`] never sees a committed entry
    /// overwritten (the safety property the 5.4.2 rule guarantees). This is the
    /// "closest deterministic reproduction" the spec asks for when a fully scripted
    /// 5-way Figure 8 cannot be forced through the single-partition sim model.
    #[test]
    fn figure_8_commit_safety_seed_sweep() {
        let config = RaftConfig::default();
        for seed in 0..40u64 {
            let mut cluster = RaftCluster::new(5, seed, config);
            cluster
                .net
                .set_latency(Duration::from_millis(1), Duration::from_millis(15));
            let mut ledger = CommitLedger::new();

            // Round after round: find the current leader, propose, then partition it
            // off so a NEW leader rises with the old leader's entries possibly only
            // partially replicated (the Figure-8 precondition: prior-term entries
            // scattered across a changing majority). Heal and repeat. The ledger is
            // sampled every chunk; it must never record an overwrite.
            for round in 0..6 {
                let leader = run_to_single_leader(&mut cluster, 500, 200);
                ledger.observe_and_check(&cluster);
                assert_3b_invariants(&cluster);

                // Propose a couple of entries tagged by round so they are distinct.
                cluster.propose(leader, payload(round));
                cluster.propose(leader, payload(round.wrapping_add(100)));
                // Let them partially replicate.
                cluster.net.run_steps(800);
                ledger.observe_and_check(&cluster);

                // Isolate the leader -> a new leader must rise on the majority side.
                let others: Vec<SimId> = cluster
                    .ids
                    .iter()
                    .copied()
                    .filter(|&id| id != leader)
                    .map(to_sim)
                    .collect();
                cluster.net.partition(&[to_sim(leader)], &others);
                for _ in 0..20 {
                    cluster.net.run_steps(500);
                    ledger.observe_and_check(&cluster);
                    assert_3b_invariants(&cluster);
                }
                // Heal; let everything reconcile, sampling the ledger throughout.
                cluster.net.heal();
                for _ in 0..20 {
                    cluster.net.run_steps(500);
                    ledger.observe_and_check(&cluster);
                    assert_3b_invariants(&cluster);
                }
            }
            cluster.run_until_idle(200_000);
            ledger.observe_and_check(&cluster);
            assert_3b_invariants(&cluster);
        }
    }

    // -- scenario 4: determinism replay of propose+partition+heal ----------

    /// Replay one propose+partition+heal run for `seed`, returning the trace plus a
    /// per-node (log, commit_index) snapshot, so two same-seed runs can be compared
    /// byte-for-byte. The fault script is fixed (partition the first elected leader
    /// after a fixed set of proposals), so a same-seed replay is identical.
    fn replay_propose_partition(
        seed: u64,
    ) -> (Vec<ironcache_sim::TraceRecord>, Vec<(Vec<LogEntry>, u64)>) {
        let mut cluster = RaftCluster::new(5, seed, RaftConfig::default());
        let leader = run_to_single_leader(&mut cluster, 500, 200);
        cluster.net.run_steps(5_000);

        // A fixed proposal + partition + heal script.
        for tag in 0..4u8 {
            cluster.propose(leader, payload(tag));
            cluster.net.run_steps(1_500);
        }
        let others: Vec<SimId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != leader)
            .map(to_sim)
            .collect();
        cluster.net.partition(&[to_sim(leader)], &others);
        cluster.net.run_steps(40_000);
        cluster.net.heal();
        cluster.net.run_steps(40_000);

        assert_3b_invariants(&cluster);
        let snapshot: Vec<(Vec<LogEntry>, u64)> = cluster
            .ids
            .iter()
            .map(|&id| (cluster.log(id), cluster.commit_index(id)))
            .collect();
        (cluster.net.trace().to_vec(), snapshot)
    }

    #[test]
    fn determinism_replay_3b() {
        // A propose+partition+heal scenario must replay byte-identically across a
        // 100-seed sweep, with the log-matching + state-machine-safety invariants
        // asserted (inside replay) each seed.
        for seed in 0..100u64 {
            let (trace_a, snap_a) = replay_propose_partition(seed);
            let (trace_b, snap_b) = replay_propose_partition(seed);
            assert_eq!(
                trace_a, trace_b,
                "seed {seed}: propose+partition+heal trace must replay byte-identically"
            );
            assert_eq!(
                snap_a, snap_b,
                "seed {seed}: per-node (log, commit_index) must replay identically"
            );
        }
    }

    /// Promote a candidate-free node directly to leader for the Figure-8 unit test:
    /// seat a full vote tally and run the engine's own promotion path so
    /// `next_index`/`match_index` initialize per Figure 2 and the election no-op is
    /// appended, exactly as a real election would leave the node. Test-only.
    fn promote_to_leader_for_test(node: &mut RaftNode<MemStorage>) {
        // Move to Candidate in the current term with every vote, then let the
        // engine's maybe_become_leader run via a self vote record. We reach the
        // private transition through the public step surface: simulate winning by
        // delivering granted RequestVoteResp from a majority. First become candidate
        // by timing out is wrong (it bumps the term); instead we seat the role and
        // votes through a direct election timeout would change term. To keep term 4,
        // we drive promotion by hand-seating the candidate state and invoking the
        // crate-internal maybe_become_leader (same module, so it is reachable).
        node.role = Role::Candidate;
        node.votes.clear();
        node.votes.insert(NodeId(1));
        node.votes.insert(NodeId(2));
        node.votes.insert(NodeId(3));
        let mut out = Effects::new();
        node.maybe_become_leader(&mut out);
        assert!(node.is_leader(), "promotion must reach Leader");
    }

    // -- AppendEntries reconciliation safety (engine-direct, closing review gaps) --
    //
    // The truncate-only-on-conflict loop and the follower commit cap are the most
    // safety-critical follower logic. These drive on_append_entries directly so a
    // regression is caught deterministically, without relying on the sim to stumble
    // into the precise race.

    /// Build a follower at `term` whose log holds the given (term, index) entries.
    fn follower_with_log(id: u64, term: u64, log: &[(u64, u64)]) -> RaftNode<MemStorage> {
        let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
        let mut storage = MemStorage::new();
        storage.set_current_term(term);
        for &(t, i) in log {
            storage.append(LogEntry {
                term: t,
                index: i,
                payload: EntryPayload::Noop,
            });
        }
        RaftNode::new(NodeId(id), voters, storage, RaftConfig::default())
    }

    fn noop(term: u64, index: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            payload: EntryPayload::Noop,
        }
    }

    fn log_terms(node: &RaftNode<MemStorage>) -> Vec<u64> {
        node.storage().log().iter().map(|e| e.term).collect()
    }

    #[test]
    fn identical_retransmit_does_not_truncate_the_log() {
        // G2(a): a duplicate/retransmitted AppendEntries whose entries the follower
        // already holds identically must leave the log byte-identical (truncate
        // nothing) - else a delayed RPC could drop a committed suffix.
        let mut node = follower_with_log(2, 5, &[(1, 1), (5, 2), (5, 3)]);
        let mut rng = ZeroRng;
        let mut out = Effects::new();
        // Leader (term 5) retransmits the whole log from the start.
        node.on_append_entries(
            &mut rng,
            5,
            NodeId(1),
            0,
            0,
            vec![noop(1, 1), noop(5, 2), noop(5, 3)],
            3,
            &mut out,
        );
        assert_eq!(
            log_terms(&node),
            vec![1, 5, 5],
            "identical retransmit must not alter the log"
        );
    }

    #[test]
    fn conflicting_entry_truncates_from_the_conflict_index() {
        // G2(b): a genuine conflict (same index, different term) truncates from that
        // index and appends the leader's entries.
        let mut node = follower_with_log(2, 5, &[(1, 1), (2, 2), (2, 3)]);
        let mut rng = ZeroRng;
        let mut out = Effects::new();
        // Leader (term 5) has [t1@1, t5@2]; prev (1, t1) matches, entry @2 is t5 != t2.
        node.on_append_entries(&mut rng, 5, NodeId(1), 1, 1, vec![noop(5, 2)], 0, &mut out);
        assert_eq!(
            log_terms(&node),
            vec![1, 5],
            "conflict at index 2 must truncate the stale t2 tail and append t5"
        );
    }

    #[test]
    fn stale_leader_commit_does_not_regress_commit_index() {
        // G2(c): a delayed/duplicate AppendEntries carrying a SMALLER leader_commit
        // must not lower the follower's commit_index.
        let mut node = follower_with_log(2, 5, &[(1, 1), (5, 2), (5, 3)]);
        let mut rng = ZeroRng;
        // First, a fresh leader_commit of 3 commits up to index 3.
        let mut out1 = Effects::new();
        node.on_append_entries(&mut rng, 5, NodeId(1), 3, 5, Vec::new(), 3, &mut out1);
        assert_eq!(node.commit_index(), 3, "commit advances to leader_commit");
        // Then a stale RPC with leader_commit 1: commit must hold at 3.
        let mut out2 = Effects::new();
        node.on_append_entries(&mut rng, 5, NodeId(1), 3, 5, Vec::new(), 1, &mut out2);
        assert_eq!(
            node.commit_index(),
            3,
            "a smaller leader_commit must not regress commit_index"
        );
    }

    #[test]
    fn follower_caps_commit_at_the_vouched_index_not_a_stale_tail() {
        // G3: a follower with a longer STALE tail must commit only up to the last
        // entry THIS RPC vouched for (prev_log_index + entries.len()), never up to
        // its own last_log_index. Also covers G4: the apply hook actually ran
        // (applied_count tracks commit_index), not just the watermark moving.
        let mut node = follower_with_log(2, 5, &[(1, 1), (2, 2), (2, 3)]);
        let mut rng = ZeroRng;
        let mut out = Effects::new();
        // Leader vouches only for index 1 (entries=[t1@1], prev 0) but leader_commit=3.
        node.on_append_entries(&mut rng, 5, NodeId(1), 0, 0, vec![noop(1, 1)], 3, &mut out);
        assert_eq!(
            node.commit_index(),
            1,
            "commit must be capped at the vouched index 1, not the stale tail at 3"
        );
        assert_eq!(
            node.applied_count(),
            1,
            "the apply hook ran for exactly the committed entry"
        );
        assert_eq!(
            node.last_applied(),
            node.commit_index(),
            "last_applied tracks commit_index"
        );
    }

    #[test]
    fn uncommitted_prior_term_entry_is_safely_overwritten() {
        // G1: the OVERWRITE path the CommitLedger guards is reachable and safe. A
        // follower holds an UNCOMMITTED prior-term entry (idx2=t2, commit_index=1).
        // A higher-term leader (term 3) that lacks it sends a conflicting entry at
        // idx2; the follower overwrites idx2=t2 with t3. This is SAFE precisely
        // because t2 was never committed (commit_index stays >= 1 and never covered
        // idx2), which is the section-5.4.2 guarantee: only an entry NOT yet
        // committed can be overwritten.
        let mut node = follower_with_log(2, 2, &[(1, 1), (2, 2)]);
        // Commit only index 1 (idx2=t2 is replicated but NOT committed).
        let mut rng = ZeroRng;
        let mut warm = Effects::new();
        node.on_append_entries(&mut rng, 2, NodeId(1), 1, 1, Vec::new(), 1, &mut warm);
        assert_eq!(
            node.commit_index(),
            1,
            "only index 1 is committed before the leader change"
        );
        // A term-3 leader without idx2's t2 entry conflicts at index 2 with t3.
        let mut out = Effects::new();
        node.on_append_entries(&mut rng, 3, NodeId(5), 1, 1, vec![noop(3, 2)], 1, &mut out);
        assert_eq!(node.current_term(), 3, "the higher term is adopted");
        assert_eq!(
            log_terms(&node),
            vec![1, 3],
            "the uncommitted prior-term entry is overwritten by the new leader"
        );
        assert!(
            node.commit_index() >= 1,
            "commit never regresses; no COMMITTED entry was overwritten (idx1 survives)"
        );
    }

    // =====================================================================
    // 3e: config state-machine apply -> SlotMap (CONTROL_PLANE.md #73).
    //
    // These exercise the REAL apply: committed ConfigCmd entries replayed onto each
    // node's own SlotMap, proving the headline property - LINEARIZABLE SLOT
    // OWNERSHIP: no two nodes ever claim the same slot at the same config epoch.
    // The engine is unchanged on the replication/commit paths; only the state
    // machine seam differs from the CountingSm scenarios above.
    // =====================================================================

    use ironcache_cluster::{NodeEntry, SlotMap};

    /// The fixed `NodeId(u64)` -> SlotMap-string-id mapping the 3e sim adapter
    /// commits to. The SlotMap's node identity is a 40-lowercase-hex string (Redis
    /// node-id shape, validated by the cluster crate), distinct from the engine's
    /// transport `NodeId(u64)`; this is the analog of [`to_sim`] / [`to_raft`] for
    /// the cluster layer. A `u64` is at most 16 hex digits, so `{:040x}` zero-pads to
    /// exactly 40 lowercase-hex characters - always a valid SlotMap id, and a
    /// bijection on the `u64`, so distinct raft ids map to distinct cluster ids.
    fn slot_id(id: NodeId) -> String {
        format!("{:040x}", id.0)
    }

    /// A deterministic advertised endpoint for a node (the SlotMap stores host/port
    /// for MOVED redirects; the values are irrelevant to ownership, but must be
    /// identical across nodes so the converged node tables match byte-for-byte).
    fn slot_host(id: NodeId) -> String {
        format!("10.0.0.{}", id.0)
    }

    const SLOT_PORT: u16 = 6379;

    /// The config state machine (3e): a [`StateMachine`] that replays committed
    /// [`ConfigCmd`]s onto an [`ironcache_cluster::SlotMap`].
    ///
    /// Each raft node owns its OWN `ConfigSm`, seeded with `SlotMap::empty_self` for
    /// THAT node's id (so `me()` / `owns()` are node-relative), but every node
    /// applies the SAME committed `ConfigCmd` sequence in the SAME order. Because
    /// `apply` is deterministic and the committed log is byte-identical on every
    /// node (Raft's Log Matching + State Machine Safety), the node tables and the
    /// slot->owner projection converge to one identical GLOBAL view: that is the
    /// linearizable-slot-ownership property under test.
    ///
    /// EPOCH POLICY (CONTROL_PLANE.md line 39, "every committed change advances the
    /// epoch"): on every committed slot-OWNERSHIP change (`SetSlotOwner` /
    /// `AssignSlots`) the machine calls [`SlotMap::bump_epoch`]. That is the
    /// Redis-faithful BUMPEPOCH primitive the cluster crate exposes; it is monotone
    /// and deterministic, so `current_epoch()` never decreases and is identical
    /// across nodes at any committed point. (BUMPEPOCH is idempotent once a node is
    /// already at the max epoch - the Redis "+STILL" reply - so the epoch is
    /// monotone-non-decreasing and convergent rather than strictly +1 per change;
    /// that is exactly what the no-two-owners-per-epoch and epoch-monotonic checkers
    /// require, since identical deterministic apply means all nodes agree on the
    /// owner at any shared epoch.) `AddNode` / `RemoveNode` are table-only and do not
    /// bump; `SetConfigEpoch` seeds the epoch directly (only valid while alone).
    ///
    /// Mutation errors from the SlotMap (e.g. a `forget` of a slot-owning node) are
    /// DETERMINISTIC across nodes (same map state + same command), so swallowing
    /// them keeps every node's apply identical; the scenarios are constructed so the
    /// committed order never produces a spurious error (AddNode precedes any
    /// reference to the node; ownership is moved away before a RemoveNode).
    struct ConfigSm {
        map: SlotMap,
        /// A monotonic config epoch driven by the COMMITTED LOG: incremented once per
        /// applied config entry, so it is a deterministic function of the applied
        /// prefix. NOT the SlotMap's Redis-client-facing epoch (whose
        /// bump_epoch/set_config_epoch carry admin-command STILL / guard semantics
        /// that are wrong for a log-driven counter; see apply).
        epoch: u64,
    }

    impl ConfigSm {
        /// Seed a fresh config state machine for the node `id`: an `empty_self`
        /// SlotMap owning ZERO slots, with this node alone in its table (peers arrive
        /// via committed `AddNode`s). Matches a fresh cluster-enabled node's boot map.
        fn seed(id: NodeId) -> Self {
            ConfigSm {
                map: SlotMap::empty_self(&slot_id(id), &slot_host(id), SLOT_PORT),
                epoch: 0,
            }
        }

        /// Borrow the converged slot map (test inspection).
        fn map(&self) -> &SlotMap {
            &self.map
        }

        /// The monotonic, log-driven config epoch (count of applied config entries).
        fn config_epoch(&self) -> u64 {
            self.epoch
        }
    }

    impl StateMachine for ConfigSm {
        fn apply(&mut self, entry: &LogEntry) {
            // Only Config payloads touch the slot map; Noop (election no-op) and
            // Bytes (opaque) are no-ops for the config machine, exactly as the engine
            // commits them without interpretation.
            let EntryPayload::Config(cmd) = &entry.payload else {
                return;
            };
            // Every committed config entry advances the monotonic config epoch
            // (CONTROL_PLANE.md line 39). The +1-per-applied-entry counter makes the
            // epoch a DETERMINISTIC FUNCTION OF THE APPLIED PREFIX: two nodes at the
            // same epoch have applied the identical config prefix and therefore agree
            // on every slot's owner (the linearizable-ownership property). We do NOT
            // use SlotMap::bump_epoch / set_config_epoch for this: those carry Redis
            // admin-command semantics (bump returns STILL once my_epoch == maxEpoch;
            // set is rejected once the node knows peers), which are wrong for a
            // log-driven counter and would let distinct ownership states share an
            // epoch (and trip the no-two-owners-per-epoch invariant).
            self.epoch += 1;
            match cmd {
                ConfigCmd::AddNode { id, host, port } => {
                    // Idempotent: a node applying AddNode for its OWN id (already in
                    // its empty_self table) is a no-op in the cluster crate.
                    self.map.meet(NodeEntry {
                        id: id.as_str().into(),
                        host: host.as_str().into(),
                        port: *port,
                    });
                }
                ConfigCmd::RemoveNode { id } => {
                    // Deterministic across nodes (same table + same command). The
                    // scenarios move ownership away first, so this never orphans.
                    let _ = self.map.forget(id);
                }
                ConfigCmd::SetSlotOwner { slot, node } => {
                    let _ = self.map.set_slot_node(*slot, node);
                }
                ConfigCmd::AssignSlots { node, slots } => {
                    for &slot in slots {
                        let _ = self.map.set_slot_node(slot, node);
                    }
                }
                ConfigCmd::AssignReplica { node, slots } => {
                    // HA-7d: record `node` as the slot's replica in the parallel structure
                    // (deterministic across nodes, like the owner assignment above).
                    for &slot in slots {
                        let _ = self.map.set_slot_replica(slot, node);
                    }
                }
                ConfigCmd::PromoteReplica { slots, new_primary } => {
                    // HA-8 FAILOVER: flip each slot's OWNER to `new_primary` (set_slot_node keeps
                    // mine[] in lockstep, so the OLD primary's owns() goes false on apply -- the
                    // split-brain fence) and CLEAR `new_primary` from the slot's replica set (it is
                    // the owner now). Deterministic + idempotent, like the assignment arms above.
                    for &slot in slots {
                        let _ = self.map.set_slot_node(slot, new_primary);
                        self.map.clear_slot_replica(slot, new_primary);
                    }
                }
                ConfigCmd::SetConfigEpoch(_epoch) => {
                    // The Raft-driven config epoch is the log-driven counter above;
                    // the SlotMap's own (Redis-client) epoch is not used for the
                    // linearizable-ownership property in 3e.
                }
            }
        }
    }

    // -- config-cluster sim harness (parallel to RaftCluster, ConfigSm-backed) --

    /// A [`SimNode`] wrapping a config-state-machine raft node. Mirrors
    /// [`RaftSimNode`] exactly (lazy `start`, effects drain), but the engine carries
    /// a [`ConfigSm`] instead of the default `CountingSm`, so committed `ConfigCmd`s
    /// drive a real `SlotMap`.
    ///
    /// [`SimNode`]: ironcache_sim::SimNode
    struct ConfigSimNode {
        engine: RaftNode<MemStorage, ConfigSm>,
        started: bool,
    }

    impl ConfigSimNode {
        fn new(id: NodeId, voters: BTreeSet<NodeId>, config: RaftConfig) -> Self {
            ConfigSimNode {
                engine: RaftNode::with_state_machine(
                    id,
                    voters,
                    MemStorage::new(),
                    config,
                    ConfigSm::seed(id),
                ),
                started: false,
            }
        }

        fn ensure_started(&mut self, ctx: &mut SimCtx<'_, RaftMsg>) {
            if self.started {
                return;
            }
            self.started = true;
            let now = ctx.now();
            let mut effects = Effects::new();
            {
                let mut rng = SimRng { ctx };
                self.engine.start(now, &mut rng, &mut effects);
            }
            drain(ctx, effects);
        }
    }

    impl ironcache_sim::SimNode for ConfigSimNode {
        type Msg = RaftMsg;

        fn on_message(&mut self, from: SimId, msg: RaftMsg, ctx: &mut SimCtx<'_, RaftMsg>) {
            self.ensure_started(ctx);
            let now = ctx.now();
            let mut effects = Effects::new();
            {
                let mut rng = SimRng { ctx };
                self.engine
                    .on_message(now, &mut rng, to_raft(from), msg, &mut effects);
            }
            drain(ctx, effects);
        }

        fn on_timer(&mut self, token: u64, ctx: &mut SimCtx<'_, RaftMsg>) {
            self.ensure_started(ctx);
            let now = ctx.now();
            let mut effects = Effects::new();
            {
                let mut rng = SimRng { ctx };
                self.engine.on_timer(now, &mut rng, token, &mut effects);
            }
            drain(ctx, effects);
        }
    }

    /// A config-cluster harness: `n` voters each with a [`ConfigSm`], built and
    /// bootstrapped exactly as [`RaftCluster`]. Exposes the role/term/commit reads
    /// the scenarios need plus SlotMap projections per node.
    struct ConfigCluster {
        net: Network<ConfigSimNode>,
        ids: Vec<NodeId>,
    }

    impl ConfigCluster {
        fn new(n: u64, seed: u64, config: RaftConfig) -> Self {
            let ids: Vec<NodeId> = (1..=n).map(NodeId).collect();
            let voters: BTreeSet<NodeId> = ids.iter().copied().collect();
            let mut net = Network::new(seed);
            for &id in &ids {
                net.add_node(to_sim(id), ConfigSimNode::new(id, voters.clone(), config));
            }
            let mut cluster = ConfigCluster { net, ids };
            cluster.start_all();
            cluster
        }

        /// Bootstrap every node (same harmless term-0 self-AppendEntries trigger as
        /// [`RaftCluster::start_all`]).
        fn start_all(&mut self) {
            for &id in &self.ids {
                self.net.tell(
                    to_sim(id),
                    to_sim(id),
                    RaftMsg::AppendEntries {
                        term: 0,
                        leader: id,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: Vec::new(),
                        leader_commit: 0,
                    },
                );
            }
        }

        fn engine(&self, id: NodeId) -> &RaftNode<MemStorage, ConfigSm> {
            &self.net.node(to_sim(id)).expect("node exists").engine
        }

        fn role(&self, id: NodeId) -> Role {
            self.engine(id).role()
        }

        fn leaders(&self) -> Vec<NodeId> {
            self.ids
                .iter()
                .copied()
                .filter(|&id| self.role(id) == Role::Leader)
                .collect()
        }

        fn commit_index(&self, id: NodeId) -> u64 {
            self.engine(id).commit_index()
        }

        fn log(&self, id: NodeId) -> Vec<LogEntry> {
            self.engine(id).storage().log().to_vec()
        }

        /// The node's converged SlotMap (via the state-machine accessor).
        fn map(&self, id: NodeId) -> &SlotMap {
            self.engine(id).state_machine().map()
        }

        fn current_epoch(&self, id: NodeId) -> u64 {
            // The Raft-driven, log-monotonic config epoch (a deterministic function
            // of the applied prefix), NOT the SlotMap's Redis-client epoch.
            self.engine(id).state_machine().config_epoch()
        }

        /// The node's slot->owner-string projection: for each ASSIGNED slot, the
        /// 40-hex id of its owner. The directly comparable global ownership view (the
        /// `ranges()` shape carries node INDICES that differ per node's table order,
        /// so we resolve to owner IDs for a node-independent comparison).
        fn owner_by_slot(&self, id: NodeId) -> BTreeMap<u16, String> {
            let map = self.map(id);
            let nodes = map.nodes();
            let mut out = BTreeMap::new();
            for (start, end, node_idx) in map.ranges() {
                let owner = nodes[node_idx].id.to_string();
                for slot in start..=end {
                    out.insert(slot, owner.clone());
                }
            }
            out
        }

        fn propose(&mut self, leader: NodeId, cmd: ConfigCmd) {
            self.net.tell(
                to_sim(leader),
                to_sim(leader),
                RaftMsg::Propose {
                    payload: EntryPayload::Config(cmd),
                },
            );
        }

        fn run_steps(&mut self, n: usize) -> usize {
            self.net.run_steps(n)
        }

        fn run_until_idle(&mut self, max_steps: usize) -> usize {
            self.net.run_until_idle(max_steps)
        }
    }

    // -- 3e checkers: linearizable slot ownership + epoch monotonicity ----------

    /// LINEARIZABLE SLOT OWNERSHIP (the headline #73 property): across all nodes,
    /// for each slot, no two nodes report a DIFFERENT owner while at the SAME
    /// `current_epoch()`.
    ///
    /// Because committed entries are byte-identical and `ConfigSm::apply` is
    /// deterministic, every node at a given committed epoch holds the same
    /// owner-per-slot; this checker proves that empirically and would catch an apply
    /// bug (a node mis-applying a `ConfigCmd` would expose a divergent owner at a
    /// shared epoch). It groups (slot, epoch) -> set-of-owners and asserts each group
    /// is a singleton.
    fn assert_no_two_owners_per_epoch(cluster: &ConfigCluster) {
        // (slot, epoch) -> the owner id first seen, plus the node that reported it.
        let mut seen: BTreeMap<(u16, u64), (String, NodeId)> = BTreeMap::new();
        for &id in &cluster.ids {
            let epoch = cluster.current_epoch(id);
            for (slot, owner) in cluster.owner_by_slot(id) {
                match seen.get(&(slot, epoch)) {
                    Some((prev_owner, prev_node)) => assert_eq!(
                        prev_owner, &owner,
                        "linearizable-ownership violated: slot {slot} at epoch {epoch} is owned by \
                         {prev_owner} per node {prev_node:?} but by {owner} per node {id:?}"
                    ),
                    None => {
                        seen.insert((slot, epoch), (owner, id));
                    }
                }
            }
        }
    }

    /// EPOCH MONOTONICITY: no node's `current_epoch()` ever decreases across
    /// observations. Sampled against a running per-node high-water map; each call
    /// asserts the current epoch is `>=` the highest previously seen for that node,
    /// then records the new high-water.
    #[derive(Default)]
    struct EpochMonotonic {
        high_water: BTreeMap<NodeId, u64>,
    }

    impl EpochMonotonic {
        fn new() -> Self {
            Self::default()
        }

        fn observe(&mut self, cluster: &ConfigCluster) {
            for &id in &cluster.ids {
                let epoch = cluster.current_epoch(id);
                let hw = self.high_water.entry(id).or_insert(0);
                assert!(
                    epoch >= *hw,
                    "epoch monotonicity violated: node {id:?} epoch went {hw} -> {epoch}"
                );
                *hw = epoch;
            }
        }
    }

    /// Run both 3e checkers at a quiescent point (the epoch-monotonic one against a
    /// supplied tracker so it spans the whole scenario).
    fn assert_3e_invariants(cluster: &ConfigCluster, epochs: &mut EpochMonotonic) {
        assert_no_two_owners_per_epoch(cluster);
        epochs.observe(cluster);
    }

    /// Elect a single leader on a config cluster and settle the election no-op.
    fn elect_config_leader(cluster: &mut ConfigCluster) -> NodeId {
        for _ in 0..200 {
            cluster.run_steps(500);
            let leaders = cluster.leaders();
            if leaders.len() == 1 {
                cluster.run_steps(5_000);
                return leaders[0];
            }
        }
        panic!("config cluster did not converge to a single leader");
    }

    /// Assert every node's committed log is a consistent prefix of the leader's (no
    /// committed config change is lost or reordered): for each node, every entry up
    /// to its commit_index equals the leader's entry at that index.
    fn assert_committed_prefix_agrees(cluster: &ConfigCluster, leader: NodeId) {
        let leader_log = cluster.log(leader);
        for &id in &cluster.ids {
            let ci = cluster.commit_index(id);
            let log = cluster.log(id);
            for idx in 1..=ci {
                let pos = usize::try_from(idx - 1).unwrap();
                assert_eq!(
                    log.get(pos),
                    leader_log.get(pos),
                    "node {id:?} committed index {idx} disagrees with the leader's log \
                     (a committed config change was lost or reordered)"
                );
            }
        }
    }

    // -- scenario H: config applies and converges (partition + heal) -----------

    #[test]
    fn config_applies_and_converges() {
        // 5 voters. Elect a leader; propose AddNode for every peer, then assign the
        // slot space across the nodes, partition the leader off and heal, and assert
        // every node's SlotMap projection is IDENTICAL at the final committed epoch,
        // the epoch is monotone everywhere, and no-two-owners holds throughout.
        let mut cluster = ConfigCluster::new(5, 101, RaftConfig::default());
        let mut epochs = EpochMonotonic::new();
        let leader = elect_config_leader(&mut cluster);
        assert_3e_invariants(&cluster, &mut epochs);

        // AddNode every node (including the leader's own id; meet is idempotent on
        // self). Committed BEFORE any slot assignment references them.
        for id in cluster.ids.clone() {
            cluster.propose(
                leader,
                ConfigCmd::AddNode {
                    id: slot_id(id),
                    host: slot_host(id),
                    port: SLOT_PORT,
                },
            );
            cluster.run_steps(1_500);
            assert_3e_invariants(&cluster, &mut epochs);
        }

        // Assign the 16384-slot space in contiguous bands, one band per node, via a
        // mix of AssignSlots (batches) and one SetSlotOwner (single slot), so both
        // apply paths are exercised.
        let n = cluster.ids.len() as u32;
        let band = u32::from(ironcache_cluster::CLUSTER_SLOTS) / n;
        for (k, id) in cluster.ids.clone().into_iter().enumerate() {
            let start = (k as u32) * band;
            let end = if k + 1 == cluster.ids.len() {
                u32::from(ironcache_cluster::CLUSTER_SLOTS) - 1
            } else {
                start + band - 1
            };
            let slots: Vec<u16> = (start..=end).map(|s| s as u16).collect();
            // Assign all but the last slot of the band as a batch, the last as a
            // single SetSlotOwner.
            let (head, tail) = slots.split_at(slots.len() - 1);
            cluster.propose(
                leader,
                ConfigCmd::AssignSlots {
                    node: slot_id(id),
                    slots: head.to_vec(),
                },
            );
            cluster.propose(
                leader,
                ConfigCmd::SetSlotOwner {
                    slot: tail[0],
                    node: slot_id(id),
                },
            );
            cluster.run_steps(2_000);
            assert_3e_invariants(&cluster, &mut epochs);
        }

        // A SetConfigEpoch is a no-op here (the leader knows other nodes, so the
        // SlotMap rejects it deterministically on every node); include it to prove
        // the command is handled uniformly and does not perturb convergence.
        cluster.propose(leader, ConfigCmd::SetConfigEpoch(99));
        cluster.run_steps(1_500);
        assert_3e_invariants(&cluster, &mut epochs);

        // Partition the leader off; a new leader rises and keeps committing nothing
        // new here (we stop proposing), then heal and let everything reconcile.
        let others: Vec<SimId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != leader)
            .map(to_sim)
            .collect();
        cluster.net.partition(&[to_sim(leader)], &others);
        for _ in 0..40 {
            cluster.run_steps(500);
            assert_3e_invariants(&cluster, &mut epochs);
        }
        cluster.net.heal();
        cluster.run_until_idle(200_000);
        assert_3e_invariants(&cluster, &mut epochs);

        // Final state: every node's slot->owner projection is IDENTICAL, the whole
        // space is assigned, and the epoch agrees everywhere.
        let reference = cluster.owner_by_slot(cluster.ids[0]);
        assert_eq!(
            reference.len(),
            usize::from(ironcache_cluster::CLUSTER_SLOTS),
            "the full slot space must be assigned after convergence"
        );
        let ref_epoch = cluster.current_epoch(cluster.ids[0]);
        for &id in &cluster.ids {
            assert_eq!(
                cluster.owner_by_slot(id),
                reference,
                "node {id:?} slot->owner projection must match every other node's"
            );
            assert_eq!(
                cluster.current_epoch(id),
                ref_epoch,
                "node {id:?} config epoch must match every other node's once converged"
            );
        }
        assert_no_two_owners_per_epoch(&cluster);
    }

    // -- scenario I: slot ownership under partition (THE headline gate) ---------

    /// Replay scenario I for one `seed`: a migration-shaped SetSlotOwner sequence
    /// proposed while the cluster is partitioned, then healed. Returns the final
    /// per-node owner-by-slot snapshot + epoch so a seed sweep can compare, and runs
    /// the no-two-owners + epoch-monotonic checkers throughout.
    fn run_slot_ownership_under_partition(seed: u64) -> Vec<(BTreeMap<u16, String>, u64)> {
        let mut cluster = ConfigCluster::new(5, seed, RaftConfig::default());
        cluster
            .net
            .set_latency(Duration::from_millis(1), Duration::from_millis(15));
        let mut epochs = EpochMonotonic::new();
        let leader = elect_config_leader(&mut cluster);

        // Build the node table first (committed before any slot reference).
        for id in cluster.ids.clone() {
            cluster.propose(
                leader,
                ConfigCmd::AddNode {
                    id: slot_id(id),
                    host: slot_host(id),
                    port: SLOT_PORT,
                },
            );
        }
        cluster.run_steps(5_000);
        assert_3e_invariants(&cluster, &mut epochs);

        // Claim a handful of slots for the leader, then run a MIGRATION-shaped
        // sequence (re-home each slot to successive nodes) WHILE PARTITIONED: the
        // majority side keeps committing the ownership flips; the minority cannot.
        let slots: [u16; 4] = [0, 4096, 8192, 12288];
        for &s in &slots {
            cluster.propose(
                leader,
                ConfigCmd::SetSlotOwner {
                    slot: s,
                    node: slot_id(leader),
                },
            );
        }
        cluster.run_steps(3_000);
        assert_3e_invariants(&cluster, &mut epochs);

        // Partition: leader + one follower as the minority (2 of 5, cannot commit);
        // the other three are the majority and elect their own leader.
        let minority_follower = *cluster
            .ids
            .iter()
            .find(|&&id| id != leader)
            .expect("a follower exists");
        let minority: Vec<SimId> = [leader, minority_follower]
            .iter()
            .copied()
            .map(to_sim)
            .collect();
        let majority: Vec<SimId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != leader && id != minority_follower)
            .map(to_sim)
            .collect();
        cluster.net.partition(&minority, &majority);

        // The majority elects a leader; migrate each slot to a NEW owner on that side.
        let mut maj_leader = None;
        for _ in 0..200 {
            cluster.run_steps(500);
            assert_3e_invariants(&cluster, &mut epochs);
            let ml: Vec<NodeId> = cluster
                .leaders()
                .into_iter()
                .filter(|id| majority.contains(&to_sim(*id)))
                .collect();
            if ml.len() == 1 {
                maj_leader = Some(ml[0]);
                break;
            }
        }
        let maj_leader = maj_leader.expect("the majority side must elect a leader");
        // Migrate every slot to the majority leader (the migration-shaped change
        // that must never produce two owners at one epoch).
        for &s in &slots {
            cluster.propose(
                maj_leader,
                ConfigCmd::SetSlotOwner {
                    slot: s,
                    node: slot_id(maj_leader),
                },
            );
        }
        for _ in 0..40 {
            cluster.run_steps(500);
            assert_3e_invariants(&cluster, &mut epochs);
        }

        // Heal; the minority side adopts the majority's committed config. Sample the
        // checkers throughout the reconciliation.
        cluster.net.heal();
        for _ in 0..80 {
            cluster.run_steps(500);
            assert_3e_invariants(&cluster, &mut epochs);
        }
        cluster.run_until_idle(200_000);
        assert_3e_invariants(&cluster, &mut epochs);

        // No committed config change is lost: every node's committed prefix agrees
        // with the (final) majority leader's log.
        let final_leader = {
            let ls = cluster.leaders();
            assert_eq!(ls.len(), 1, "exactly one leader after heal");
            ls[0]
        };
        assert_committed_prefix_agrees(&cluster, final_leader);

        cluster
            .ids
            .iter()
            .map(|&id| (cluster.owner_by_slot(id), cluster.current_epoch(id)))
            .collect()
    }

    #[test]
    fn slot_ownership_under_partition() {
        // THE headline gate, across a seed sweep: no epoch ever shows two owners for
        // one slot (asserted inside the run via assert_no_two_owners_per_epoch every
        // chunk) and, after heal, all nodes converge to one ownership view with no
        // committed change lost.
        for seed in 0..30u64 {
            let snaps = run_slot_ownership_under_partition(seed);
            let (ref_owner, ref_epoch) = &snaps[0];
            for (owner, epoch) in &snaps {
                assert_eq!(
                    owner, ref_owner,
                    "seed {seed}: all nodes must converge to one slot->owner view after heal"
                );
                assert_eq!(
                    epoch, ref_epoch,
                    "seed {seed}: all nodes must agree on the config epoch after heal"
                );
            }
            // The migration landed: every probed slot is owned (by the same node on
            // every replica, already asserted above).
            for s in [0u16, 4096, 8192, 12288] {
                assert!(
                    ref_owner.contains_key(&s),
                    "seed {seed}: migrated slot {s} must have an owner after convergence"
                );
            }
        }
    }

    // =====================================================================
    // HA-8: FAILOVER (promotion) -- THE SPLIT-BRAIN GATE.
    //
    // A committed PromoteReplica transfers a slot's ownership from a (dead) primary
    // to an in-sync replica. The danger is SPLIT-BRAIN (two owners of a slot) and
    // DATA LOSS (promoting a stale replica). These scenarios prove the APPLY-side
    // fence: across an entire partition/heal failover timeline, NO two nodes ever
    // `owns()` a slot at the SAME committed state, and the epoch advances on the
    // promotion. The pure engine here has no replication link, so it always feeds an
    // in-sync candidate; the DATA-LOSS half (a too-stale replica is NEVER proposed)
    // is the LAG GATE, proven directly where it lives by the unit test
    // `ironcache::replica_attach::tests::promotion_proposal_lag_gate_refuses_a_stale_replica`.
    // =====================================================================

    /// THE SPLIT-BRAIN ASSERTION (the merge-blocker): NO two nodes ever have
    /// `owns()==true` for the same slot AT THE SAME CONFIG EPOCH. It asserts the
    /// `owns()` PROPERTY but evaluates it via each node's COLD `owner_by_slot`
    /// projection (the `owner[]` array, coalesced from `ranges()`), which is
    /// equivalent to the hot `mine[]`/`owns()` bitmap by the separately-tested
    /// owner/`mine[]` lockstep invariant -- and O(assigned) per node, so the
    /// thousands-of-timelines sweep stays fast.
    ///
    /// This is the rigorous form of THE FENCE (CONTROL_PLANE.md / the HA-8 design point
    /// 2): the config epoch advances on every committed ownership change, so two nodes
    /// at the SAME epoch have applied the identical committed prefix and therefore agree
    /// on every slot's single owner. The qualifier "at the same committed state" is the
    /// EPOCH: a client (or node) at epoch E always sees exactly one owner of a slot.
    ///
    /// Why epoch-keyed and not unconditional: during an ACTIVE partition a stale
    /// minority node sits at an EARLIER epoch (it cannot commit), still showing
    /// `owns()==true` for a slot it last owned, while the majority commits a
    /// PromoteReplica that gives a NEW owner the slot at a HIGHER epoch. That transient
    /// is NOT split-brain -- the two believe they own at DIFFERENT epochs, and a client
    /// touching the stale node gets MOVED carrying the OLD epoch (the system as a whole
    /// has advanced). The DANGEROUS thing -- two owners a client could see as
    /// simultaneously authoritative -- is exactly two owners AT ONE EPOCH, which this
    /// forbids. (The post-heal convergence to ONE global owner is asserted separately,
    /// unconditionally, once every node has caught its log up.)
    ///
    /// Because each node's `ConfigSm` is seeded `empty_self` for THAT node's id,
    /// `map(id).owns(slot)` is true iff node `id` is the slot's owner in its OWN
    /// committed view; this scans all nodes and groups self-owned slots by (slot, epoch).
    ///
    /// It iterates each node's `owner_by_slot()` projection (the ASSIGNED slots only,
    /// coalesced from `ranges()`) rather than all 16384 raw slots, and counts a slot as
    /// owned by `id` only when that node's OWN view resolves the owner to `id` (i.e.
    /// `owns()` would be true) -- the same fact, but O(assigned) per node so the
    /// thousands-of-timelines sweep stays fast. A slot owned by two nodes at one epoch
    /// is exactly the split-brain a failover must never produce.
    fn assert_at_most_one_owner_per_slot(cluster: &ConfigCluster) {
        // (slot, epoch) -> the (single) node observed to own it at that epoch.
        let mut owner_of: BTreeMap<(u16, u64), NodeId> = BTreeMap::new();
        for &id in &cluster.ids {
            let epoch = cluster.current_epoch(id);
            let self_id = slot_id(id);
            // owner_by_slot resolves each ASSIGNED slot to its owner's 40-hex id in THIS
            // node's view; a slot whose owner is `self_id` is one this node `owns()`.
            for (slot, owner) in cluster.owner_by_slot(id) {
                if owner != self_id {
                    continue; // this node does not own the slot in its own view.
                }
                if let Some(&other) = owner_of.get(&(slot, epoch)) {
                    panic!(
                        "SPLIT-BRAIN: slot {slot} is owns()==true on BOTH node {other:?} and \
                         node {id:?} at the SAME config epoch {epoch} (two owners at one \
                         committed state)"
                    );
                }
                owner_of.insert((slot, epoch), id);
            }
        }
    }

    /// The UNCONDITIONAL post-convergence form: once the cluster has fully healed and
    /// every node has caught its committed log up, NO slot may be `owns()==true` on more
    /// than one node, FULL STOP (every node is at the same final epoch, so this is the
    /// epoch-keyed property collapsed to one epoch). Called only at the end of a
    /// scenario, after `run_until_idle`, to prove the failover converged to ONE owner.
    fn assert_exactly_one_owner_after_convergence(cluster: &ConfigCluster) {
        let mut owner_of: BTreeMap<u16, NodeId> = BTreeMap::new();
        for &id in &cluster.ids {
            let self_id = slot_id(id);
            for (slot, owner) in cluster.owner_by_slot(id) {
                if owner != self_id {
                    continue; // not self-owned in this node's view.
                }
                if let Some(&other) = owner_of.get(&slot) {
                    panic!(
                        "POST-HEAL SPLIT-BRAIN: slot {slot} is owns()==true on BOTH node \
                         {other:?} and node {id:?} after full convergence (the failover did \
                         not converge to one owner)"
                    );
                }
                owner_of.insert(slot, id);
            }
        }
    }

    /// Elect a single leader CONFINED to `group` (a partitioned side). Returns it, or
    /// `None` if the side does not elect one within the budget. Asserts the
    /// split-brain invariant at every chunk so a bad promotion mid-election trips.
    fn run_to_leader_in_group(
        cluster: &mut ConfigCluster,
        group: &[SimId],
        max_rounds: usize,
    ) -> Option<NodeId> {
        for _ in 0..max_rounds {
            cluster.run_steps(500);
            assert_at_most_one_owner_per_slot(cluster);
            let ls: Vec<NodeId> = cluster
                .leaders()
                .into_iter()
                .filter(|id| group.contains(&to_sim(*id)))
                .collect();
            if ls.len() == 1 {
                return Some(ls[0]);
            }
        }
        None
    }

    /// Replay the HA-8 split-brain failover gate for one `seed` with `partition_after`
    /// controlling WHEN (in chunks) the owner is partitioned away, so the seed sweep
    /// randomizes partition timing. Returns the final per-node owner-by-slot snapshot +
    /// epoch (for the seed-sweep convergence assertion). The split-brain checker
    /// (`assert_at_most_one_owner_per_slot`) and the epoch-monotonic checker run at
    /// EVERY quiescent step throughout.
    ///
    /// The lag gate is modeled at the DECISION LEVEL: the pure engine has no
    /// replication link, so the test plays the role of HA-8's failover controller and
    /// promotes ONLY the replica it has marked in-sync (`in_sync_replica`); it asserts
    /// a too-stale replica (`stale_replica`) is NEVER named in a promotion. This is the
    /// same gate `replica_is_in_sync` enforces in production (the controller proposes
    /// PromoteReplica only for an in-sync candidate); here we prove the COMMITTED
    /// PROMOTION itself is split-brain-safe given that gate.
    #[allow(clippy::too_many_lines)]
    fn run_failover_split_brain_gate(
        seed: u64,
        partition_after: usize,
    ) -> Vec<(BTreeMap<u16, String>, u64)> {
        // 3 voters: this is the N=3 cluster the gate spec calls for. The OWNER is the
        // first leader; the IN-SYNC replica + the stale replica are the other two.
        let mut cluster = ConfigCluster::new(3, seed, RaftConfig::default());
        cluster
            .net
            .set_latency(Duration::from_millis(1), Duration::from_millis(15));
        let mut epochs = EpochMonotonic::new();
        let owner = elect_config_leader(&mut cluster);
        assert_3e_invariants(&cluster, &mut epochs);
        assert_at_most_one_owner_per_slot(&cluster);

        // Build the node table (committed before any slot/replica reference).
        for id in cluster.ids.clone() {
            cluster.propose(
                owner,
                ConfigCmd::AddNode {
                    id: slot_id(id),
                    host: slot_host(id),
                    port: SLOT_PORT,
                },
            );
        }
        cluster.run_steps(3_000);
        assert_3e_invariants(&cluster, &mut epochs);

        // The slots under failover. The OWNER claims them; ONE peer is the in-sync
        // replica (promotable), the OTHER is a deliberately-stale replica (must NOT be
        // promoted -- the lag gate). The peers are the two non-owner ids.
        let slots: [u16; 3] = [0, 6000, 12000];
        let peers: Vec<NodeId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != owner)
            .collect();
        let in_sync_replica = peers[0];
        let stale_replica = peers[1];
        for &s in &slots {
            cluster.propose(
                owner,
                ConfigCmd::SetSlotOwner {
                    slot: s,
                    node: slot_id(owner),
                },
            );
        }
        // Record BOTH peers as replicas of the slots (AssignReplica). They are equal in
        // the committed map; the lag gate (which one is in-sync) lives in the failover
        // controller's decision, modeled below.
        cluster.propose(
            owner,
            ConfigCmd::AssignReplica {
                node: slot_id(in_sync_replica),
                slots: slots.to_vec(),
            },
        );
        cluster.propose(
            owner,
            ConfigCmd::AssignReplica {
                node: slot_id(stale_replica),
                slots: slots.to_vec(),
            },
        );
        cluster.run_steps(3_000);
        assert_3e_invariants(&cluster, &mut epochs);
        assert_at_most_one_owner_per_slot(&cluster);

        // The pre-promotion epoch (every node agrees once converged; sample the owner's).
        let pre_promotion_epoch = cluster.current_epoch(owner);

        // Let the cluster run a randomized number of chunks BEFORE the partition, so the
        // partition lands at different points relative to in-flight replication.
        for _ in 0..partition_after {
            cluster.run_steps(200);
            assert_at_most_one_owner_per_slot(&cluster);
        }

        // PARTITION the owner away from the other two (the majority). The owner (1 of 3)
        // cannot commit; the two-node majority elects a leader and runs the failover.
        let majority: Vec<SimId> = peers.iter().copied().map(to_sim).collect();
        cluster.net.partition(&[to_sim(owner)], &majority);

        // The majority elects a leader. The failover controller (this test) PROMOTES the
        // IN-SYNC replica -- NEVER the stale one (the lag gate). A spurious promotion
        // (the owner is actually alive across the partition) is SAFE for split-brain: the
        // committed entry atomically transfers ownership and the old owner steps down on
        // apply (proven by the checker, which runs through the whole timeline).
        let maj_leader = run_to_leader_in_group(&mut cluster, &majority, 200)
            .expect("the majority side must elect a leader");

        // THE PROMOTION: name the IN-SYNC replica as the new primary of the slots. The
        // lag gate is the choice of `in_sync_replica` here; assert we never name the
        // stale one.
        let new_primary = in_sync_replica;
        assert_ne!(
            new_primary, stale_replica,
            "the lag gate must never promote the too-stale replica"
        );
        cluster.propose(
            maj_leader,
            ConfigCmd::PromoteReplica {
                slots: slots.to_vec(),
                new_primary: slot_id(new_primary),
            },
        );
        for _ in 0..25 {
            cluster.run_steps(500);
            assert_at_most_one_owner_per_slot(&cluster);
            assert_3e_invariants(&cluster, &mut epochs);
        }

        // The new primary, on the majority side, now OWNS the slots (committed there).
        for &s in &slots {
            assert!(
                cluster.map(new_primary).owns(s),
                "seed {seed}: the promoted in-sync replica must own slot {s} after the commit"
            );
        }
        // The new owner's epoch advanced past the pre-promotion epoch (the fence's epoch bump).
        assert!(
            cluster.current_epoch(new_primary) > pre_promotion_epoch,
            "seed {seed}: the new owner's epoch ({}) must exceed the pre-promotion epoch ({})",
            cluster.current_epoch(new_primary),
            pre_promotion_epoch
        );

        // HEAL. The OLD primary, catching its Raft log up, applies the committed
        // PromoteReplica: its `owns()` for the slots goes FALSE (it serves MOVED). The
        // split-brain checker runs through the entire reconciliation.
        cluster.net.heal();
        for _ in 0..30 {
            cluster.run_steps(500);
            assert_at_most_one_owner_per_slot(&cluster);
            assert_3e_invariants(&cluster, &mut epochs);
        }
        cluster.run_until_idle(100_000);
        assert_at_most_one_owner_per_slot(&cluster);
        assert_3e_invariants(&cluster, &mut epochs);
        // Post-heal: every node has caught its committed log up, so there is now EXACTLY
        // one owner per slot across the whole cluster (the epoch-keyed transient is gone).
        assert_exactly_one_owner_after_convergence(&cluster);

        // The OLD primary lost ownership (the fence) and now resolves MOVED to the new
        // owner's endpoint for every promoted slot.
        for &s in &slots {
            assert!(
                !cluster.map(owner).owns(s),
                "seed {seed}: the OLD primary must lose ownership of slot {s} after heal (MOVED)"
            );
            let moved = cluster.map(owner).moved_target(s);
            assert_eq!(
                moved,
                Some((slot_host(new_primary), SLOT_PORT)),
                "seed {seed}: the old primary must MOVED slot {s} to the new owner's endpoint"
            );
        }
        // No committed change was lost: every node's committed prefix agrees with the leader's.
        let final_leader = {
            let ls = cluster.leaders();
            assert_eq!(ls.len(), 1, "seed {seed}: exactly one leader after heal");
            ls[0]
        };
        assert_committed_prefix_agrees(&cluster, final_leader);

        cluster
            .ids
            .iter()
            .map(|&id| (cluster.owner_by_slot(id), cluster.current_epoch(id)))
            .collect()
    }

    #[test]
    fn failover_split_brain_gate() {
        // THE MERGE-BLOCKER, across thousands of (seed, partition-timing) pairs. For
        // each seed we vary `partition_after` so the owner is isolated at different
        // points in the timeline (randomized partition timing). Throughout EVERY run:
        // - the split-brain checker (`assert_at_most_one_owner_per_slot`) runs at every
        //   quiescent step -> two simultaneous owners would panic immediately;
        // - the epoch is monotone everywhere and the new owner's epoch exceeds the
        //   pre-promotion epoch;
        // - the lag gate never promotes the stale replica;
        // and after heal all nodes converge to ONE ownership view (the old primary
        // having lost the slots and MOVED to the new owner).
        //
        // 200 seeds x 5 partition-timing offsets = 1000 distinct failover timelines,
        // each scanning all 3 nodes' ASSIGNED slots (epoch-keyed) for two owners at
        // every quiescent chunk across the partition/heal timeline.
        for seed in 0..200u64 {
            for partition_after in 0..5usize {
                let snaps = run_failover_split_brain_gate(seed, partition_after);
                let (ref_owner, ref_epoch) = &snaps[0];
                for (owner, epoch) in &snaps {
                    assert_eq!(
                        owner, ref_owner,
                        "seed {seed}/{partition_after}: all nodes must converge to one \
                         slot->owner view after the failover heals"
                    );
                    assert_eq!(
                        epoch, ref_epoch,
                        "seed {seed}/{partition_after}: all nodes must agree on the config \
                         epoch after the failover heals"
                    );
                }
            }
        }
    }

    // THE LAG GATE (no data loss): a too-stale replica is NEVER promoted. The gate
    // PREDICATE itself (`ironcache_repl::replica_is_in_sync`: link up AND lag <=
    // max_lag, the only promotion-eligible state) is unit-tested in
    // `ironcache-repl/src/lag.rs` (`in_sync_true_only_when_up_and_within_lag`), and is
    // NOT re-tested here to keep the pure engine crate free of an `ironcache-repl`
    // dependency. The split-brain DST gate above models that gate at the DECISION level
    // -- the failover controller (the test) promotes ONLY the in-sync replica and
    // asserts it never names the stale one -- which is exactly how the production
    // controller consumes the predicate before proposing a `PromoteReplica`.

    // -- scenario determinism_replay_3e ----------------------------------------

    /// A per-node replay snapshot: the SlotMap's slot ranges plus the node's
    /// current config epoch. Compared for byte-identical equality across two
    /// same-seed runs to prove deterministic replay of the config state machine.
    type NodeConfigSnapshot = (Vec<(u16, u16, usize)>, u64);

    /// One config-proposal + partition + heal run for `seed`, returning the trace
    /// plus a per-node (ranges, current_epoch) snapshot for byte-identical replay
    /// comparison. The fault script is fixed (partition the first leader after a
    /// fixed proposal set), so a same-seed replay is identical. The 3e checkers run
    /// inside the run (via the shared helper).
    fn replay_config_partition(
        seed: u64,
    ) -> (Vec<ironcache_sim::TraceRecord>, Vec<NodeConfigSnapshot>) {
        let mut cluster = ConfigCluster::new(5, seed, RaftConfig::default());
        let mut epochs = EpochMonotonic::new();
        let leader = elect_config_leader(&mut cluster);

        for id in cluster.ids.clone() {
            cluster.propose(
                leader,
                ConfigCmd::AddNode {
                    id: slot_id(id),
                    host: slot_host(id),
                    port: SLOT_PORT,
                },
            );
        }
        cluster.run_steps(3_000);
        // A fixed slot-assignment + migration script.
        for (k, id) in cluster.ids.clone().into_iter().enumerate() {
            cluster.propose(
                leader,
                ConfigCmd::AssignSlots {
                    node: slot_id(id),
                    slots: vec![(k as u16) * 1000, (k as u16) * 1000 + 1],
                },
            );
            cluster.run_steps(1_500);
            assert_3e_invariants(&cluster, &mut epochs);
        }

        let others: Vec<SimId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != leader)
            .map(to_sim)
            .collect();
        cluster.net.partition(&[to_sim(leader)], &others);
        cluster.run_steps(40_000);
        assert_3e_invariants(&cluster, &mut epochs);
        cluster.net.heal();
        cluster.run_steps(40_000);
        assert_3e_invariants(&cluster, &mut epochs);

        let snapshot: Vec<NodeConfigSnapshot> = cluster
            .ids
            .iter()
            .map(|&id| (cluster.map(id).ranges(), cluster.current_epoch(id)))
            .collect();
        (cluster.net.trace().to_vec(), snapshot)
    }

    #[test]
    fn determinism_replay_3e() {
        // A config-proposal + partition + heal scenario must replay byte-identically
        // across >=100 seeds: same trace AND same per-node (ranges, current_epoch)
        // snapshot, with the no-two-owners + epoch-monotonic checkers asserted each
        // seed (inside replay_config_partition).
        for seed in 0..100u64 {
            let (trace_a, snap_a) = replay_config_partition(seed);
            let (trace_b, snap_b) = replay_config_partition(seed);
            assert_eq!(
                trace_a, trace_b,
                "seed {seed}: config+partition+heal trace must replay byte-identically"
            );
            assert_eq!(
                snap_a, snap_b,
                "seed {seed}: per-node (ranges, current_epoch) must replay identically"
            );
        }
    }
}
