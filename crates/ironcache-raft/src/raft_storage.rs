// SPDX-License-Identifier: MIT OR Apache-2.0
//! Raft storage seam split out of `lib.rs` (#625): the `RaftStorage` persistence trait, `SnapshotMeta`, and the in-memory `MemStorage` test impl. Behavior-preserving relocation; re-exported from the crate root.

use crate::{LogEntry, NodeId};
use std::collections::BTreeSet;

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
