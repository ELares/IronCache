// SPDX-License-Identifier: MIT OR Apache-2.0
//! The live rolling-upgrade OBSERVERS (#392 Phase 3): translate a cluster snapshot into the
//! [`ironcache_repl::upgrade_step`] inputs + pick the promotion candidate.
//!
//! The pure driver `run_rolling_upgrade` (`ironcache-repl`, #501) drives the sequence through the
//! `UpgradeActions` trait. The LIVE impl of that trait has two halves: (1) OBSERVE the cluster (read
//! each node's role/version/lag + the raft leader) and turn it into the five `upgrade_step` inputs +
//! the "which replica to promote" choice -- this module, PURE + unit-tested; and (2) the I/O (fetch
//! each node's `/topology` + `INFO`, send `CLUSTER FAILOVER`, drive the per-node upgrade) -- a
//! following slice that assembles a [`ClusterView`] from the wire and acts on these decisions.
//!
//! Scope: ONE shard being rolled -- a primary owning a slot range + the replicas of that range,
//! matching `upgrade_step`'s single-primary model. Rolling a multi-primary cluster loops this per
//! shard (a following concern). The VERSION is each node's self-reported `ironcache_version`; the
//! caller supplies the explicit `target_version` to roll TO (needed because dev/lock builds pin the
//! version to `0.0.0`, so a live version-diff cannot be trusted without an explicit target).

use ironcache_repl::{CandidateReplica, LinkStatus, PromotionSafety, ReplicaLag, safe_to_promote};

/// A node's replication role for the shard being rolled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// Owns the shard's slots (the primary).
    Master,
    /// Mirrors the shard from the primary (a replica).
    Replica,
}

/// One node in the [`ClusterView`]: its id, self-reported version, role, and -- for a replica -- its
/// link + lag (the `safe_to_promote` inputs). A master carries `link = Down` / `lag = None` (unused).
#[derive(Debug, Clone)]
pub struct NodeView {
    /// The node's announce id (the `CLUSTER FAILOVER` target when this node is the chosen candidate).
    pub id: String,
    /// The node's self-reported `ironcache_version` (from `INFO` / `/topology.engine_version`).
    pub version: String,
    /// The node's role for the shard being rolled.
    pub role: NodeRole,
    /// A REPLICA's link status to the primary (its `/topology.replication.master_link`).
    pub link: LinkStatus,
    /// A REPLICA's lag behind the primary (`Some` for a replica, `None` for the master).
    pub lag: Option<ReplicaLag>,
}

impl NodeView {
    /// Whether this node is on the target version (upgraded).
    fn is_upgraded(&self, target_version: &str) -> bool {
        self.version == target_version
    }
    /// Whether this replica is an UPGRADED, IN-SYNC promotion candidate.
    fn is_promotable(&self, target_version: &str, max_lag: u64) -> bool {
        self.role == NodeRole::Replica
            && self.is_upgraded(target_version)
            && self.link.is_up()
            && self.lag.is_some_and(|l| l.in_sync(max_lag))
    }
}

/// A snapshot of the shard being rolled (#392): the primary + its replicas, the version to roll TO,
/// the in-sync lag bound, and whether the config-plane raft has a majority quorum. Assembled from the
/// wire by the I/O layer; the observer methods here are pure and mirror the `upgrade_step` inputs.
#[derive(Debug, Clone)]
pub struct ClusterView {
    /// The nodes of the shard being rolled (one master + its replicas).
    pub nodes: Vec<NodeView>,
    /// The version being rolled TO (explicit, not inferred -- dev builds pin `0.0.0`). Matched by
    /// EXACT STRING equality (`is_upgraded`), so the I/O layer that assembles this view MUST normalize
    /// both sides consistently (e.g. strip a `v` prefix / build metadata like `+build` / whitespace)
    /// -- otherwise a formatting skew leaves `replicas_to_upgrade` never reaching 0 and the roll
    /// stalls (`StalledAfterBudget`; fail-closed -- no unsafe action, but no progress).
    pub target_version: String,
    /// The min-replicas-max-lag bound (ADR-0026): a replica is in sync when its lag is `<= max_lag`.
    pub max_lag: u64,
    /// Whether the config-plane raft has a majority quorum (a recognized leader), so a
    /// `PromoteReplica` fence can commit.
    pub raft_quorum: bool,
    /// The id of the node that was the PRIMARY when the roll STARTED. The I/O layer records it once
    /// (roles flip on promotion, so it must be remembered): when this node's role flips to `Replica`,
    /// the promotion has committed (`primary_demoted`), and its version reaching the target is the
    /// final step (`old_primary_upgraded`). `None` before the roll has identified the primary.
    ///
    /// LOAD-BEARING CONTRACT for the I/O slice: once the roll begins, this MUST be set (to the
    /// pre-roll master's id) and that id MUST stay present in `nodes`. If it is left `None` after a
    /// real promotion, `primary_demoted` stays false and the demoted old primary (now a `Replica` on
    /// the old version) is re-counted by `replicas_to_upgrade` -> a spurious SECOND promotion. If the
    /// id vanishes from `nodes` (old primary removed/crashed), the driver believes the promotion never
    /// committed and could re-promote. The following slice guarantees set-at-start + present.
    pub old_primary_id: Option<String>,
}

impl ClusterView {
    /// Replica nodes NOT yet on the target version (Phase 1 work) -- `upgrade_step`'s
    /// `replicas_to_upgrade`.
    #[must_use]
    pub fn replicas_to_upgrade(&self) -> usize {
        self.nodes
            .iter()
            .filter(|n| n.role == NodeRole::Replica && !n.is_upgraded(&self.target_version))
            .count()
    }

    /// Is a just-UPGRADED replica still catching up (not yet back in sync)? `upgrade_step`'s
    /// `replica_catching_up`: an upgraded replica whose link is down or whose lag is not yet within
    /// the bound. (A NOT-yet-upgraded replica is Phase-1 work, not "catching up".)
    #[must_use]
    pub fn replica_catching_up(&self) -> bool {
        self.nodes.iter().any(|n| {
            n.role == NodeRole::Replica
                && n.is_upgraded(&self.target_version)
                && !(n.link.is_up() && n.lag.is_some_and(|l| l.in_sync(self.max_lag)))
        })
    }

    /// The [`safe_to_promote`] verdict for the chosen candidate (an upgraded in-sync replica) --
    /// `upgrade_step`'s `promotion`. With no promotable candidate: `NoQuorum` if the raft has no
    /// quorum (the blocking precondition), else `CandidateNotInSync`.
    #[must_use]
    pub fn promotion_safety(&self) -> PromotionSafety {
        match self.select_promote_candidate() {
            Some(c) => safe_to_promote(
                CandidateReplica {
                    link: c.link,
                    // A promotable candidate always has a known lag (checked in `is_promotable`).
                    lag: c.lag.unwrap_or_else(ReplicaLag::unknown),
                },
                self.max_lag,
                self.raft_quorum,
            ),
            None if !self.raft_quorum => PromotionSafety::NoQuorum,
            None => PromotionSafety::CandidateNotInSync,
        }
    }

    /// Has the promotion committed -- is the pre-roll primary now a REPLICA? `upgrade_step`'s
    /// `primary_demoted` (the `PromoteReplica` fence flipped ownership away from it).
    #[must_use]
    pub fn primary_demoted(&self) -> bool {
        self.old_primary_id.as_ref().is_some_and(|id| {
            self.nodes
                .iter()
                .any(|n| &n.id == id && n.role == NodeRole::Replica)
        })
    }

    /// Is the demoted old primary now on the target version? `upgrade_step`'s `old_primary_upgraded`.
    #[must_use]
    pub fn old_primary_upgraded(&self) -> bool {
        self.old_primary_id.as_ref().is_some_and(|id| {
            self.nodes
                .iter()
                .any(|n| &n.id == id && n.is_upgraded(&self.target_version))
        })
    }

    /// Is the CURRENT shard master already on the target version? `upgrade_step`'s `master_on_target`
    /// (the #392 slice-6 belt-and-suspenders guard against a driver-restart double-failover): a roll
    /// only ever promotes AWAY from an old-version master, so a master already on target -- with the
    /// replicas also upgraded (the state Phase 2 is reached in) -- means the shard is fully rolled and
    /// no (second) promotion is needed. With no observed master (a transient snapshot) this is false,
    /// leaving the promotion gate to defer safely.
    #[must_use]
    pub fn master_on_target(&self) -> bool {
        self.nodes
            .iter()
            .any(|n| n.role == NodeRole::Master && n.is_upgraded(&self.target_version))
    }

    /// Pick the promotion candidate: an upgraded, in-sync replica, preferring the LEAST-lagging (the
    /// most-caught-up), so the synchronous fence loses the least. `None` if none is promotable.
    #[must_use]
    pub fn select_promote_candidate(&self) -> Option<&NodeView> {
        self.nodes
            .iter()
            .filter(|n| n.is_promotable(&self.target_version, self.max_lag))
            .min_by_key(|n| n.lag.and_then(ReplicaLag::lag).unwrap_or(u64::MAX))
    }

    /// Whether the WHOLE shard -- the primary AND every replica -- is ALREADY on the target version.
    /// The driver MUST check this BEFORE rolling a shard: `upgrade_step` does not take the current
    /// primary's version, so with everything already upgraded it would still emit `Promote` (a
    /// gratuitous `CLUSTER FAILOVER` + client redirect) before reaching `Done`. Skipping a shard that
    /// is `shard_fully_upgraded()` makes the roll IDEMPOTENT (a re-run, or `target == current`, is a
    /// no-op) rather than costing a needless failover. Empty `nodes` is NOT "fully upgraded" (nothing
    /// observed -- do not treat an empty snapshot as done).
    #[must_use]
    pub fn shard_fully_upgraded(&self) -> bool {
        !self.nodes.is_empty()
            && self
                .nodes
                .iter()
                .all(|n| n.is_upgraded(&self.target_version))
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterView, NodeRole, NodeView};
    use ironcache_repl::{LinkStatus, PromotionSafety, ReplOffset, ReplicaLag};

    const TARGET: &str = "1.2.3";
    const OLD: &str = "1.2.2";

    /// An in-sync replica: link up, lag within the bound.
    fn in_sync_replica(id: &str, version: &str) -> NodeView {
        NodeView {
            id: id.to_owned(),
            version: version.to_owned(),
            role: NodeRole::Replica,
            link: LinkStatus::Up,
            lag: Some(ReplicaLag::compute(ReplOffset(10), ReplOffset(9))), // lag 1
        }
    }
    fn master(id: &str, version: &str) -> NodeView {
        NodeView {
            id: id.to_owned(),
            version: version.to_owned(),
            role: NodeRole::Master,
            link: LinkStatus::Down,
            lag: None,
        }
    }
    fn view(nodes: Vec<NodeView>, old_primary: Option<&str>) -> ClusterView {
        ClusterView {
            nodes,
            target_version: TARGET.to_owned(),
            max_lag: 2,
            raft_quorum: true,
            old_primary_id: old_primary.map(str::to_owned),
        }
    }

    #[test]
    fn replicas_to_upgrade_counts_only_old_version_replicas() {
        let v = view(
            vec![
                master("p", OLD),
                in_sync_replica("r1", OLD),    // needs upgrade
                in_sync_replica("r2", TARGET), // already upgraded
            ],
            None,
        );
        assert_eq!(
            v.replicas_to_upgrade(),
            1,
            "only the old-version replica counts"
        );
    }

    #[test]
    fn replica_catching_up_only_for_an_upgraded_not_yet_in_sync_replica() {
        // An UPGRADED replica whose link is down -> catching up.
        let mut lagging = in_sync_replica("r1", TARGET);
        lagging.link = LinkStatus::Down;
        assert!(view(vec![master("p", OLD), lagging], None).replica_catching_up());

        // An UPGRADED replica over the lag bound -> catching up.
        let mut over = in_sync_replica("r2", TARGET);
        over.lag = Some(ReplicaLag::compute(ReplOffset(10), ReplOffset(5))); // lag 5 > 2
        assert!(view(vec![master("p", OLD), over], None).replica_catching_up());

        // A NOT-yet-upgraded replica is Phase-1 work, NOT "catching up".
        assert!(
            !view(vec![master("p", OLD), in_sync_replica("r3", OLD)], None).replica_catching_up()
        );

        // An upgraded, in-sync replica is caught up.
        assert!(
            !view(vec![master("p", OLD), in_sync_replica("r4", TARGET)], None)
                .replica_catching_up()
        );
    }

    #[test]
    fn promotion_safety_gates_on_an_upgraded_in_sync_candidate_and_quorum() {
        // An upgraded in-sync replica + quorum -> Safe.
        let v = view(vec![master("p", OLD), in_sync_replica("r1", TARGET)], None);
        assert_eq!(v.promotion_safety(), PromotionSafety::Safe);

        // No upgraded in-sync replica (only an old-version replica) -> CandidateNotInSync.
        let v = view(vec![master("p", OLD), in_sync_replica("r1", OLD)], None);
        assert_eq!(v.promotion_safety(), PromotionSafety::CandidateNotInSync);

        // Quorum absent -> NoQuorum, even with a perfect candidate.
        let mut v = view(vec![master("p", OLD), in_sync_replica("r1", TARGET)], None);
        v.raft_quorum = false;
        assert_eq!(v.promotion_safety(), PromotionSafety::NoQuorum);
    }

    #[test]
    fn select_promote_candidate_prefers_the_least_lagging_upgraded_in_sync_replica() {
        let mut r_far = in_sync_replica("r_far", TARGET);
        r_far.lag = Some(ReplicaLag::compute(ReplOffset(10), ReplOffset(8))); // lag 2
        let mut r_near = in_sync_replica("r_near", TARGET);
        r_near.lag = Some(ReplicaLag::compute(ReplOffset(10), ReplOffset(10))); // lag 0
        let v = view(vec![master("p", OLD), r_far, r_near], None);
        assert_eq!(
            v.select_promote_candidate().map(|n| n.id.as_str()),
            Some("r_near"),
            "the most-caught-up replica is chosen"
        );
    }

    #[test]
    fn demotion_progress_tracks_the_pre_roll_primary_flipping_and_upgrading() {
        // Pre-roll: p is master (old). Not demoted, not old-primary-upgraded.
        let pre = view(
            vec![master("p", OLD), in_sync_replica("r1", TARGET)],
            Some("p"),
        );
        assert!(!pre.primary_demoted());
        assert!(!pre.old_primary_upgraded());

        // After promotion: r1 is now master, p flipped to replica (still old version) -> demoted, not
        // yet upgraded.
        let post = view(
            vec![
                master("r1", TARGET),
                NodeView {
                    role: NodeRole::Replica,
                    ..master("p", OLD) // p demoted to replica, still old
                },
            ],
            Some("p"),
        );
        assert!(post.primary_demoted());
        assert!(!post.old_primary_upgraded());

        // After the old primary is upgraded -> both true (the Done precondition).
        let done = view(
            vec![
                master("r1", TARGET),
                NodeView {
                    role: NodeRole::Replica,
                    ..master("p", TARGET)
                },
            ],
            Some("p"),
        );
        assert!(done.primary_demoted());
        assert!(done.old_primary_upgraded());
    }

    #[test]
    fn master_on_target_reflects_the_current_masters_version() {
        // Master on the OLD version (the normal pre-promotion posture) -> false, so the promotion
        // gate is free to fire.
        let old = view(
            vec![master("p", OLD), in_sync_replica("r1", TARGET)],
            Some("p"),
        );
        assert!(!old.master_on_target());

        // Master already on target (e.g. the driver restarted after promoting r1) -> true, the guard
        // input that keeps upgrade_step from driving a second failover.
        let promoted = view(
            vec![
                master("r1", TARGET),
                NodeView {
                    role: NodeRole::Replica,
                    ..master("p", OLD) // old primary demoted, still old version
                },
            ],
            None,
        );
        assert!(promoted.master_on_target());

        // No master observed (a transient snapshot) -> false (defer, do not assume rolled).
        let headless = view(vec![in_sync_replica("r1", TARGET)], None);
        assert!(!headless.master_on_target());
    }

    #[test]
    fn shard_fully_upgraded_is_the_is_a_roll_needed_precheck() {
        // Everything already on target -> fully upgraded (the driver skips it -> no spurious roll).
        let all = view(
            vec![master("p", TARGET), in_sync_replica("r1", TARGET)],
            None,
        );
        assert!(all.shard_fully_upgraded());
        // Any node still on the old version -> a roll IS needed.
        assert!(
            !view(vec![master("p", TARGET), in_sync_replica("r1", OLD)], None)
                .shard_fully_upgraded()
        );
        assert!(
            !view(vec![master("p", OLD), in_sync_replica("r1", TARGET)], None)
                .shard_fully_upgraded()
        );
        // An empty snapshot is NOT "done" (nothing observed).
        assert!(!view(vec![], None).shard_fully_upgraded());

        // WHY the pre-check exists (review Finding 1): with the shard already fully upgraded, the raw
        // observers would still drive a spurious Promote -- upgrade_step(replicas_to_upgrade=0,
        // catching_up=false, promotion=Safe, primary_demoted=false, ..) == Promote, a gratuitous
        // CLUSTER FAILOVER. `shard_fully_upgraded()` is exactly the gate the driver checks first to
        // keep a re-run (or target == current) a no-op.
        assert_eq!(all.replicas_to_upgrade(), 0);
        assert!(!all.replica_catching_up());
        assert_eq!(all.promotion_safety(), PromotionSafety::Safe);
        assert!(!all.primary_demoted());
    }
}
