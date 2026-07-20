// SPDX-License-Identifier: MIT OR Apache-2.0
//! Raft config + timer tokens split out of `lib.rs` (#625): the `RaftConfig` timing parameters and the pre-vote fallback constant. Behavior-preserving relocation; re-exported from the crate root.

use core::time::Duration;

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
