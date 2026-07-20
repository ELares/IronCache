// SPDX-License-Identifier: MIT OR Apache-2.0
//! Raft determinism seams split out of `lib.rs` (#625): the `RaftRng` random-draw seam and the `StateMachine` apply seam plus its `CountingSm` test impl. Behavior-preserving relocation; re-exported from the crate root.

use crate::LogEntry;

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
