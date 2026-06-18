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
    /// The proposal was ACCEPTED by the leader and appended to its log at the carried 1-based
    /// index; it commits (durable on a majority, then applied by every node's `ConfigSm`) shortly
    /// after, once a majority acknowledges. Today the ack resolves at leader-accept (append) time,
    /// not on the commit advance, so on an immediate leadership loss the index could be overwritten;
    /// because every `ConfigCmd` is idempotent under re-apply, a proposer that does not observe the
    /// effect safely re-proposes (the self-promotion task does exactly this). Resolving this ack on
    /// the actual commit-advance is a tracked follow-up.
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
    /// HA-9) and await the leader's accept.
    ///
    /// Returns [`ProposeOutcome::Committed`] with the leader-assigned log index once the entry is
    /// accepted+appended by the leader (it commits on a majority shortly after; see the variant
    /// docs for the accept-vs-commit timing and why idempotency makes that safe), or
    /// [`ProposeOutcome::NotLeader`] when no leader was reachable (no leader recognized, or a
    /// forward timed out, or the control plane has stopped). The await does NOT block the shard
    /// executor: it parks on the proposal's one-shot ack channel, which the single control-plane
    /// task fulfills (directly on the leader, or via the leader's forwarded reply on a follower).
    pub async fn propose(&self, cmd: ConfigCmd) -> ProposeOutcome {
        match self.inner.propose(EntryPayload::Config(cmd)).await {
            Some(index) => ProposeOutcome::Committed(index),
            None => ProposeOutcome::NotLeader,
        }
    }
}
