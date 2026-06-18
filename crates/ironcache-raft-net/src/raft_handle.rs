// SPDX-License-Identifier: MIT OR Apache-2.0
//! [`RaftHandle`]: the clonable, `Send` handle the live serve path holds to PROPOSE cluster
//! configuration changes through the Raft control plane (HA-4c).
//!
//! The serve path (`ironcache`'s `ServerContext`) must not touch the `!Send` engine that the
//! control-plane run loop owns; it interacts ONLY through the `Send` [`NodeHandle`] (the inbox
//! sender + the status watch). [`RaftHandle`] wraps that one level up for the CLUSTER command
//! path: it turns a [`ConfigCmd`] into a proposal, awaits the commit, and reports leadership so
//! the mutator handler can redirect when this node is not the leader.
//!
//! It is deliberately THIN: all the consensus + I/O lives behind the [`NodeHandle`] inbox.
//! `RaftHandle` adds (a) the `ConfigCmd -> EntryPayload::Config` wrapping, (b) a typed
//! [`ProposeOutcome`] the mutator maps to `+OK` / `-CLUSTERDOWN`, and (c) the `is_leader()` /
//! `leader_hint()` reads the redirect uses.

use ironcache_raft::{ConfigCmd, EntryPayload};

use crate::NodeHandle;

/// The outcome of proposing a [`ConfigCmd`] through the Raft control plane.
///
/// The serve-layer CLUSTER mutator maps this to a wire reply: [`Committed`](ProposeOutcome::Committed)
/// -> `+OK`, [`NotLeader`](ProposeOutcome::NotLeader) -> a `-CLUSTERDOWN` redirect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeOutcome {
    /// The proposal is COMMITTED: the entry is durable on a MAJORITY at the carried 1-based log
    /// index (and is therefore being applied by every node's `ConfigSm`). The ack resolves on the
    /// COMMIT-ADVANCE, not at leader-accept (append) time (HA-prod-commit-ack): the run loop parks
    /// the proposal's ack keyed by its assigned log index and fulfils it only once the engine's
    /// [`Effects::committed_through`](ironcache_raft::Effects::committed_through) reaches that index
    /// (a majority committed it), so `+OK` MEANS committed. If this node loses leadership before the
    /// entry commits (a step-down), or the uncommitted entry is overwritten by a new leader, the
    /// parked ack resolves [`NotLeader`](ProposeOutcome::NotLeader) instead and the idempotent
    /// `ConfigCmd` is safely re-proposed. A single-voter cluster commits on append (the leader alone
    /// is a majority), so the ack still resolves promptly there.
    Committed(u64),
    /// This node was NOT the leader (or the control plane had stopped), so nothing was
    /// proposed. The client should retry against the leader.
    NotLeader,
}

/// A clonable, `Send` handle the serve path holds to propose cluster-config changes through
/// Raft (HA-4c). Wraps the lower-level [`NodeHandle`]; see the module docs.
#[derive(Clone)]
pub struct RaftHandle {
    inner: NodeHandle,
}

impl std::fmt::Debug for RaftHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftHandle")
            .field("node_id", &self.inner.id().0)
            .field("is_leader", &self.is_leader())
            .finish()
    }
}

impl RaftHandle {
    /// Wrap a [`NodeHandle`] (the one the control-plane run loop handed out at boot).
    #[must_use]
    pub fn new(inner: NodeHandle) -> Self {
        RaftHandle { inner }
    }

    /// Whether THIS node currently believes it is the Raft leader (a cheap, non-blocking read
    /// of the published status snapshot). A mutator proposes only when this is true; a follower
    /// redirects instead of proposing a doomed entry.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.inner.status().is_leader()
    }

    /// A best-effort leader HINT for a redirect reply, as a `host:port` string, or `None` when
    /// unknown.
    ///
    /// HA-9 made this real: the engine now records the leader it recognizes (the passive
    /// `RaftNode::leader_id`), the control-plane run loop publishes it through the status watch,
    /// and [`NodeHandle::leader_hint`](crate::NodeHandle::leader_hint) resolves it to the leader's
    /// cluster-bus `host:port` via the static peer map. The redirect therefore names the leader
    /// when one is known; it is still `None` (a bare `-CLUSTERDOWN` retry) only when NO leader is
    /// recognized (a forming cluster or an in-progress election). With forwarding live, a CLUSTER
    /// write to a follower usually COMMITS (forwarded) rather than redirecting, so this hint is
    /// taken only on the genuine no-leader / forward-timeout path.
    #[must_use]
    pub fn leader_hint(&self) -> Option<String> {
        self.inner.leader_hint()
    }

    /// Propose `cmd` through the Raft log (forwarding to the leader if this node is a follower,
    /// HA-9) and await TRUE COMMIT.
    ///
    /// Returns [`ProposeOutcome::Committed`] with the leader-assigned log index once the entry is
    /// COMMITTED on a majority (HA-prod-commit-ack: the ack resolves on the commit-advance, not at
    /// append; see the variant docs), or [`ProposeOutcome::NotLeader`] when the entry could not be
    /// committed here -- no leader was reachable (no leader recognized, a forward timed out, or the
    /// control plane stopped), this node lost leadership before the entry committed, or a new leader
    /// overwrote the still-uncommitted entry. In every `NotLeader` case the idempotent `ConfigCmd`
    /// is safely re-proposed. The await does NOT block the shard executor: it parks on the
    /// proposal's one-shot ack channel, which the single control-plane task fulfills when the entry
    /// commits (directly on the leader, or via the leader's forwarded reply on a follower).
    pub async fn propose(&self, cmd: ConfigCmd) -> ProposeOutcome {
        match self.inner.propose(EntryPayload::Config(cmd)).await {
            Some(index) => ProposeOutcome::Committed(index),
            None => ProposeOutcome::NotLeader,
        }
    }
}
