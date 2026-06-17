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
    /// The proposal was accepted by the leader and committed; carries its 1-based log index.
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
    /// HA-4c does NOT yet surface the current leader's id through the status snapshot (the pure
    /// engine tracks the leader only transiently inside `AppendEntries` and does not expose it,
    /// and threading it out would touch the DST-verified engine). So the hint is `None` today
    /// and the redirect is a bare `-CLUSTERDOWN`; the client retries (the usual way a client
    /// finds the new leader in a forming cluster). Surfacing the concrete leader endpoint is a
    /// tracked follow-up; the API shape is here so that follow-up does not change call sites.
    #[must_use]
    pub fn leader_hint(&self) -> Option<String> {
        None
    }

    /// Propose `cmd` through the Raft log and await its commit.
    ///
    /// Returns [`ProposeOutcome::Committed`] with the assigned log index once the entry is
    /// committed (durable on a majority and applied by every node's `ConfigSm`), or
    /// [`ProposeOutcome::NotLeader`] when this node was not the leader (or the control plane has
    /// stopped). The await does NOT block the shard executor: it parks on the proposal's
    /// one-shot ack channel, which the single control-plane task fulfills.
    pub async fn propose(&self, cmd: ConfigCmd) -> ProposeOutcome {
        match self.inner.propose(EntryPayload::Config(cmd)).await {
            Some(index) => ProposeOutcome::Committed(index),
            None => ProposeOutcome::NotLeader,
        }
    }
}
