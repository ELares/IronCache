// SPDX-License-Identifier: MIT OR Apache-2.0
//! The clustered rolling-upgrade ORCHESTRATION state machine (#392 Phase 3).
//!
//! #392's zero-downtime, RPO=0 upgrade for a raft cluster walks a fixed node-by-node sequence
//! (the etcd/Consul/ElastiCache pattern): upgrade the in-sync REPLICAS first (each catches up from
//! the primary), then PROMOTE an upgraded in-sync replica (ownership moves only via a committed raft
//! log + a monotonic epoch, a synchronous fence that loses no acknowledged write), then upgrade the
//! OLD PRIMARY last (now demoted to a replica). Clients redirect on the failover; the dataset is
//! never down.
//!
//! [`upgrade_step`] is the PURE next-step decision of that sequence, in the same shape as the
//! rebalance controller's `apply_step` (ironcache-cluster): it reads the authoritative committed
//! state + the driver's verdicts and returns the [`UpgradeStep`] the driver should take, holding no
//! private checkpoint (so a driver restart re-derives the same step and RESUMES). The promotion
//! guardrail it consumes is [`crate::lag::safe_to_promote`] (the #392 lag gate + quorum, computed by
//! the driver from the live repl state); keeping the SAFETY judgement in the driver and the STATE
//! TRANSITION here (pure) is the clean split that makes the sequence unit-testable to a truth table
//! rather than on a live cluster. The actual binary swap, the raft `PromoteReplica` commit, and the
//! redirect are the clustered/Linux layer the driver performs; this module decides WHAT to do next
//! and in what ORDER (replicas first, primary last), never HOW.

use crate::lag::PromotionSafety;

/// Why the rolling upgrade cannot safely make progress right now (it must wait, not force ahead).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockReason {
    /// The config-plane raft lacks a majority quorum, so the `PromoteReplica` fence cannot be
    /// committed. Wait for quorum to return.
    NoQuorum,
    /// No upgraded replica is currently in sync enough to promote without losing acknowledged writes
    /// (the RPO=0 lag gate). Wait for a candidate to catch up.
    NoInSyncCandidate,
}

/// The next step the rolling-upgrade driver should take (#392), derived purely from the committed
/// state + the driver's verdicts. The driver loops: compute the step, perform it (or poll), re-read
/// the state, repeat -- so re-deriving the same step after a restart resumes at the same place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeStep {
    /// Replicas are upgraded FIRST: swap the next not-yet-upgraded replica to the new binary in
    /// place (it re-attaches and catches up from the primary).
    UpgradeReplica,
    /// A just-upgraded replica is re-syncing (not yet back in sync): wait, do not start the next
    /// replica or promote until it has caught up.
    AwaitInSync,
    /// Every replica is upgraded and in sync: PROMOTE an upgraded in-sync replica (the synchronous
    /// committed-raft fence), so the current primary is freed to be upgraded last. Idempotent: the
    /// driver re-proposes until the committed ownership flip lands (the epoch fence makes a repeat a
    /// no-op), matching `apply_step`'s re-propose style.
    Promote,
    /// The old primary has been demoted (the promotion committed): upgrade it last, as a replica.
    UpgradeOldPrimary,
    /// Every node is on the new version, the primary upgraded last: the rolling upgrade is complete.
    Done,
    /// The upgrade cannot safely proceed right now; the driver waits and re-polls (never forces a
    /// promotion that would break quorum or lose writes).
    Blocked(BlockReason),
}

/// Decide the next rolling-upgrade step (#392 Phase 3), PURELY from the committed state + the
/// driver's verdicts:
/// - `replicas_to_upgrade`: how many replica nodes are NOT yet on the new version (Phase 1 work).
/// - `replica_catching_up`: is a just-upgraded replica still re-syncing (not yet back in sync)?
/// - `promotion`: the [`crate::lag::safe_to_promote`] verdict for promoting an upgraded in-sync
///   replica (Phase 2's lag gate + quorum), computed by the driver from the live repl/raft state.
/// - `primary_demoted`: has the promotion committed, so the OLD primary is now a replica (Phase 3)?
/// - `old_primary_upgraded`: is that demoted old primary now on the new version?
///
/// The ORDER encodes the #392 sequence: finish the old-primary (Phase 3) once demoted; otherwise
/// upgrade replicas first (Phase 1), waiting for each to re-sync; and only when all replicas are
/// upgraded + in sync, promote (Phase 2), deferring safely if quorum or an in-sync candidate is
/// missing. It never emits `Promote` while replicas remain to upgrade, so the primary is always
/// upgraded LAST.
#[must_use]
pub fn upgrade_step(
    replicas_to_upgrade: usize,
    replica_catching_up: bool,
    promotion: PromotionSafety,
    primary_demoted: bool,
    old_primary_upgraded: bool,
) -> UpgradeStep {
    // Phase 3: the promotion has committed (the old primary is demoted). Upgrade it last, then done.
    // Checked first because it is only reachable AFTER Phases 1+2; during them `primary_demoted` is
    // false and control falls through.
    if primary_demoted {
        return if old_primary_upgraded {
            UpgradeStep::Done
        } else {
            UpgradeStep::UpgradeOldPrimary
        };
    }

    // Phase 1: upgrade the replicas first, one at a time. A just-upgraded replica that is still
    // re-syncing blocks starting the next one (and blocks promotion), so the cluster never runs two
    // nodes down at once.
    if replica_catching_up {
        return UpgradeStep::AwaitInSync;
    }
    if replicas_to_upgrade > 0 {
        return UpgradeStep::UpgradeReplica;
    }

    // Phase 2: all replicas upgraded + in sync. Promote an upgraded in-sync replica so the old
    // primary can be upgraded last -- but only when the #392 guardrail says it is safe.
    match promotion {
        PromotionSafety::Safe => UpgradeStep::Promote,
        PromotionSafety::NoQuorum => UpgradeStep::Blocked(BlockReason::NoQuorum),
        PromotionSafety::CandidateNotInSync => UpgradeStep::Blocked(BlockReason::NoInSyncCandidate),
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockReason, UpgradeStep, upgrade_step};
    use crate::lag::PromotionSafety;

    // Shorthands for the promotion verdict (Phase 2 only cares about it once replicas are done).
    const SAFE: PromotionSafety = PromotionSafety::Safe;
    const NO_QUORUM: PromotionSafety = PromotionSafety::NoQuorum;
    const NOT_IN_SYNC: PromotionSafety = PromotionSafety::CandidateNotInSync;

    #[test]
    fn phase1_upgrades_replicas_first_one_at_a_time() {
        // Replicas remain + none catching up -> upgrade the next replica. Promotion verdict is
        // IRRELEVANT here (the primary is upgraded last), so even a Safe verdict does not promote.
        assert_eq!(
            upgrade_step(3, false, SAFE, false, false),
            UpgradeStep::UpgradeReplica
        );
        // A just-upgraded replica is re-syncing -> wait; do NOT start the next replica or promote.
        assert_eq!(
            upgrade_step(2, true, SAFE, false, false),
            UpgradeStep::AwaitInSync
        );
        // The catching-up gate wins even with zero replicas left to START (the in-flight one).
        assert_eq!(
            upgrade_step(0, true, SAFE, false, false),
            UpgradeStep::AwaitInSync
        );
    }

    #[test]
    fn phase2_promotes_only_after_all_replicas_upgraded_and_when_safe() {
        // All replicas upgraded + in sync + safe -> promote.
        assert_eq!(
            upgrade_step(0, false, SAFE, false, false),
            UpgradeStep::Promote
        );
        // Safe promotion is NOT attempted while a replica still needs upgrading (primary last).
        assert_eq!(
            upgrade_step(1, false, SAFE, false, false),
            UpgradeStep::UpgradeReplica
        );
        // Guardrail defers: no quorum / no in-sync candidate -> Blocked with the matching reason.
        assert_eq!(
            upgrade_step(0, false, NO_QUORUM, false, false),
            UpgradeStep::Blocked(BlockReason::NoQuorum)
        );
        assert_eq!(
            upgrade_step(0, false, NOT_IN_SYNC, false, false),
            UpgradeStep::Blocked(BlockReason::NoInSyncCandidate)
        );
    }

    #[test]
    fn phase3_upgrades_the_demoted_old_primary_last_then_done() {
        // Promotion committed (primary demoted), old primary not yet upgraded -> upgrade it.
        assert_eq!(
            upgrade_step(0, false, SAFE, true, false),
            UpgradeStep::UpgradeOldPrimary
        );
        // Old primary upgraded -> Done.
        assert_eq!(upgrade_step(0, false, SAFE, true, true), UpgradeStep::Done);
        // Phase 3 takes precedence: once demoted, the old-primary work runs regardless of the
        // (now-moot) replica/promotion inputs, so a stale Safe/replica count cannot re-trigger a
        // second promotion.
        assert_eq!(
            upgrade_step(5, true, NO_QUORUM, true, false),
            UpgradeStep::UpgradeOldPrimary
        );
    }

    #[test]
    fn primary_is_always_upgraded_last() {
        // Across the whole matrix, `Promote`/`UpgradeOldPrimary` never appear while replicas remain
        // to upgrade or a replica is catching up -- the invariant that keeps the primary last.
        for &replicas in &[1usize, 2, 5] {
            for &catching in &[false, true] {
                for promo in [SAFE, NO_QUORUM, NOT_IN_SYNC] {
                    let step = upgrade_step(replicas, catching, promo, false, false);
                    assert!(
                        !matches!(step, UpgradeStep::Promote | UpgradeStep::UpgradeOldPrimary),
                        "must not touch the primary while replicas remain: {step:?}"
                    );
                }
            }
        }
    }
}
