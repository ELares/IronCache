// SPDX-License-Identifier: MIT OR Apache-2.0
//! Raft effects split out of `lib.rs` (#625): the `TimerOp` arm/cancel taxonomy and the `Effects` set a step returns (outbound messages, timer ops, persistence intents). Behavior-preserving relocation; re-exported from the crate root.

use core::time::Duration;

use crate::{NodeId, RaftMsg};

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
    pub(crate) fn send(&mut self, to: NodeId, msg: RaftMsg) {
        self.sends.push((to, msg));
    }

    #[inline]
    pub(crate) fn set_timer(&mut self, token: u64, after: Duration) {
        self.timer_ops.push(TimerOp::Set { token, after });
    }

    #[inline]
    pub(crate) fn cancel_timer(&mut self, token: u64) {
        self.timer_ops.push(TimerOp::Cancel { token });
    }

    /// Record that this step raised `commit_index` to `index` (HA-prod-commit-ack).
    /// Commit is monotone within a step, so a later raise in the same step always
    /// dominates an earlier one; keep the MAX so a step that advances commit more than
    /// once (which the engine never does today, but the record stays correct if it ever
    /// did) reports the final high-water. Purely additive: it emits no I/O and changes
    /// no decision, so the DST sweep is byte-identical (the sim drain ignores it).
    #[inline]
    pub(crate) fn note_committed_through(&mut self, index: u64) {
        self.committed_through = Some(match self.committed_through {
            Some(prev) => prev.max(index),
            None => index,
        });
    }
}
