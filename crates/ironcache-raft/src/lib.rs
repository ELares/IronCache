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
//! ## Scope: sub-slice 3a (election + terms ONLY)
//!
//! This first sub-slice implements LEADER ELECTION and TERM SAFETY, the
//! foundation on which the rest of Raft is built. A bug here is split-brain, so
//! the correctness bar is the Election Safety property (at most one leader per
//! term), proven by the DST scenarios in the test module across a seed sweep.
//!
//! What 3a does NOT do: there are no real log payloads beyond a single
//! [`EntryPayload::Noop`] (the no-op a fresh leader appends per section 8 / the
//! "commit-only-current-term" machinery; in 3a it is just a placeholder that
//! advances the log so the up-to-date check has something to compare), no client
//! proposals, no log replication, no commit advancement, and no state-machine
//! apply. [`RaftMsg::AppendEntries`] is used purely as the leader's HEARTBEAT
//! (its `entries` vector is always empty in 3a). Those land in later sub-slices.
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
use std::collections::BTreeSet;

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
/// In sub-slice 3a the ONLY variant is [`EntryPayload::Noop`], the no-op a fresh
/// leader appends to its log on election (so it has a current-term entry; see the
/// paper's section 8 and the "commit-only-current-term" rule of 5.4.2). Real
/// config payloads (the slot map, epoch, roster, roles per ADR-0027) arrive in
/// later sub-slices as additional variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryPayload {
    /// A leader's election no-op. Carries no data; advances the log index/term.
    Noop,
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
    /// Append `entry` to the log. In 3a only the leader's election no-op is
    /// appended.
    fn append(&mut self, entry: LogEntry);
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
pub struct RaftNode<S: RaftStorage> {
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
}

impl<S: RaftStorage> RaftNode<S> {
    /// Construct a node `id` in a cluster of `voters` (must include `id`), backed
    /// by `storage` and timed by `config`. The node starts as a `Follower`; call
    /// [`RaftNode::start`] to arm its first election timer.
    #[must_use]
    pub fn new(id: NodeId, voters: BTreeSet<NodeId>, storage: S, config: RaftConfig) -> Self {
        RaftNode {
            id,
            voters,
            role: Role::Follower,
            votes: BTreeSet::new(),
            config,
            storage,
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
            RaftMsg::AppendEntries { term, leader, .. } => {
                self.on_append_entries(rng, term, leader, out);
            }
            RaftMsg::AppendEntriesResp { term, .. } => {
                self.on_append_entries_resp(rng, from, term, out);
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
    /// implementation", rule 1). In 3a this is the heartbeat handler.
    ///
    /// 1. "All Servers": adopt a strictly greater term (step down, clear vote).
    /// 2. Reply false if `term < currentTerm` (rule 1): a stale leader; the reply
    ///    carries our higher term so the stale leader steps down.
    /// 3. Otherwise (`term == currentTerm`): recognize the leader. A `Candidate`
    ///    in this term concedes to the leader and becomes a `Follower` (Figure 2,
    ///    "Candidates": if AppendEntries received from new leader, convert to
    ///    follower). RESET the election timer (we have heard from the leader) and
    ///    reply success with our last index.
    fn on_append_entries(
        &mut self,
        rng: &mut dyn RaftRng,
        term: u64,
        leader: NodeId,
        out: &mut Effects,
    ) {
        let _ = leader;
        self.observe_term(term, rng, out);
        let current = self.storage.current_term();

        if term < current {
            // Rule 1: stale leader. Do not reset our timer; reply with our higher
            // term so the stale leader steps down.
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
        // Recognize the leader: reset the election timer (heard from leader).
        self.arm_election_timer(rng, out);
        out.send(
            leader,
            RaftMsg::AppendEntriesResp {
                term: current,
                success: true,
                match_index: self.storage.last_log_index(),
            },
        );
    }

    /// APPENDENTRIES response handler. In 3a only the term check matters (no log
    /// replication bookkeeping yet): a response with a greater term steps the
    /// leader down. Everything else is a no-op until log replication lands.
    fn on_append_entries_resp(
        &mut self,
        rng: &mut dyn RaftRng,
        from: NodeId,
        term: u64,
        out: &mut Effects,
    ) {
        let _ = from;
        self.observe_term(term, rng, out);
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
    }

    /// Become `Leader` if the current vote tally is a strict majority of voters
    /// (Figure 2, "Candidates"). On winning: cancel the election timer, append a
    /// no-op to our own log (so the leader has a current-term entry), broadcast the
    /// initial empty `AppendEntries`, and arm the heartbeat timer.
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

        // Append the election no-op (section 8; a current-term entry the leader can
        // commit). In 3a it is the only thing in the log beyond prior no-ops.
        let next_index = self.storage.last_log_index() + 1;
        let term = self.storage.current_term();
        self.storage.append(LogEntry {
            term,
            index: next_index,
            payload: EntryPayload::Noop,
        });

        self.broadcast_heartbeat(out);
        out.set_timer(HEARTBEAT, self.config.heartbeat_interval);
    }

    /// Broadcast an empty (heartbeat) `AppendEntries` to every other voter.
    fn broadcast_heartbeat(&self, out: &mut Effects) {
        let term = self.storage.current_term();
        let prev_log_index = self.storage.last_log_index();
        let prev_log_term = self.storage.last_log_term();
        for &peer in &self.voters {
            if peer != self.id {
                out.send(
                    peer,
                    RaftMsg::AppendEntries {
                        term,
                        leader: self.id,
                        prev_log_index,
                        prev_log_term,
                        entries: Vec::new(),
                        leader_commit: 0,
                    },
                );
            }
        }
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
}
