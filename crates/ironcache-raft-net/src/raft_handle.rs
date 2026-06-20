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

use ironcache_clusterbus::PeerEndpoint;
use ironcache_raft::{ConfigCmd, EntryPayload, MembershipChange, NodeId};

use crate::{ClusterConfig, MembershipOutcome, NodeHandle, Status};

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

    /// Construct a handle with a FIXED node id + recognized leader, for TESTS only (PROD-9): it lets
    /// a caller unit-test the leader-hint resolution and the CLUSTER introspection leader marking
    /// without standing up a real raft cluster. Wraps [`NodeHandle::for_test`].
    #[doc(hidden)]
    #[must_use]
    pub fn for_test(id: NodeId, leader_id: Option<NodeId>) -> Self {
        RaftHandle::new(NodeHandle::for_test(id, leader_id))
    }

    /// Like [`for_test`](RaftHandle::for_test) but with an explicit RECOVERED persisted-log last
    /// index (PROD-turnkey), so a test can model a node that RESTARTED onto persisted state and
    /// assert the turnkey driver refuses to re-bootstrap it. `0` is a truly fresh node (the
    /// [`for_test`](RaftHandle::for_test) default). Wraps
    /// [`NodeHandle::for_test_recovered`](crate::NodeHandle::for_test_recovered).
    #[doc(hidden)]
    #[must_use]
    pub fn for_test_recovered(
        id: NodeId,
        leader_id: Option<NodeId>,
        recovered_last_log_index: u64,
    ) -> Self {
        RaftHandle::new(NodeHandle::for_test_recovered(
            id,
            leader_id,
            recovered_last_log_index,
        ))
    }

    /// Whether THIS node currently believes it is the Raft leader (a cheap, non-blocking read
    /// of the published status snapshot). A mutator proposes only when this is true; a follower
    /// redirects instead of proposing a doomed entry.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.inner.status().is_leader()
    }

    /// The latest published [`Status`] snapshot (role / current term / commit index / leader),
    /// a cheap non-blocking `Copy` read of the control plane's status watch. The `/metrics`
    /// endpoint (OBSERVABILITY.md) reads this for the `ironcache_raft_*` gauges; the readiness
    /// probe reads `leader_id` to gate `/readyz` on a recognized leader.
    #[must_use]
    pub fn status(&self) -> Status {
        self.inner.status()
    }

    /// The [`NodeId`] this node currently recognizes as the Raft leader for its term, or `None`
    /// when no leader is recognized (a forming cluster / in-progress election), a cheap
    /// non-blocking read of the published status. `Some(self)` on a leader. The serve layer uses
    /// this to resolve the leader's ADVERTISED CLIENT endpoint (via the slot-map's announce ids) for
    /// a NOTLEADER hint and to mark the leader in the CLUSTER introspection projections; unlike
    /// [`leader_hint`](RaftHandle::leader_hint) (the cluster-bus endpoint via the static peer map),
    /// this exposes the id so the resolution can name the dial-able CLIENT address an operator uses.
    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        self.inner.status().leader_id
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

    /// This node's Raft [`NodeId`] (HA-prod-membership). The operator-membership path uses it to
    /// avoid registering / removing SELF and to know which voter the FORGET removal names.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.inner.id()
    }

    /// The engine's RECOVERED persisted-log last index, sampled ONCE at construction (PROD-turnkey).
    /// A CONSTRUCTION-TIME FACT (not the live, growing index): `0` for a truly fresh node, `> 0` for
    /// a node that RESTARTED onto persisted state -- including the COMMON no-snapshot restart, where
    /// the recovery path recovers raft membership but does NOT replay the `ConfigSm` slots/epoch/
    /// nodes, leaving the shared `SlotMap` transiently pristine while the committed config tail is
    /// un-applied. The turnkey bootstrap driver reads this -- not that racy shared-map projection --
    /// to decide freshness.
    #[must_use]
    pub fn recovered_last_log_index(&self) -> u64 {
        self.inner.recovered_last_log_index()
    }

    /// Whether this node booted with a NON-EMPTY persisted Raft log (PROD-turnkey): `true` for a
    /// node that RESTARTED onto persisted state (any committed bootstrap / runtime migration /
    /// failover left a log entry), `false` for a TRULY FRESH node (empty persisted log). This is the
    /// robust freshness gate the turnkey driver uses: a fresh cluster (empty log) still bootstraps
    /// turnkey; a restarted node (non-empty log) NEVER re-bootstraps, EVEN on the common no-snapshot
    /// restart path where the shared-map projection is transiently pristine. A construction-time
    /// fact, immutable for the life of the handle.
    #[must_use]
    pub fn has_persisted_log(&self) -> bool {
        self.inner.recovered_last_log_index() > 0
    }

    /// The live cluster CONFIGURATION (voter + learner sets) this node has adopted (HA-prod-membership).
    /// A cheap, non-blocking read of the published config-watch snapshot; the operator path consults it
    /// to apply the FORGET quorum-safety reasoning at the serve layer and to surface membership.
    #[must_use]
    pub fn config(&self) -> ClusterConfig {
        self.inner.config()
    }

    /// Propose a single-server Raft [`MembershipChange`] and await its outcome (HA-prod-membership).
    ///
    /// This is the OPERATOR PATH that grows / shrinks the Raft VOTER / LEARNER set at runtime, the
    /// quorum-affecting complement to [`propose`](RaftHandle::propose) (which moves the slot / node
    /// TABLE). `addr` is a newly-added node's cluster-bus [`PeerEndpoint`] (host + port) so the leader
    /// can replicate to a runtime-joined node not in the static topology, re-resolving the host per
    /// dial (k8s); pass `None` for a removal / known peer.
    ///
    /// Returns [`MembershipOutcome::Committed`] on true commit, [`MembershipOutcome::NotLeader`] when
    /// this node is not the leader (a membership change is NOT forwarded; it is issued on the leader),
    /// [`MembershipOutcome::InFlight`] when a previous change is still uncommitted (section-6
    /// one-change-in-flight -- retryable), or [`MembershipOutcome::Refused`] when the adapter's
    /// quorum-safety guard rejected a removal that would break majority.
    pub async fn propose_membership(
        &self,
        change: MembershipChange,
        addr: Option<PeerEndpoint>,
    ) -> MembershipOutcome {
        self.inner.propose_membership(change, addr).await
    }
}
