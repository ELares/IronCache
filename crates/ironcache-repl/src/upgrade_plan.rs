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
//!
//! ## Scope + generalizations (what this models, and what it deliberately does not)
//!
//! - MULTI-REPLICA ordering: the #392 body describes the minimal case ("upgrade AN in-sync replica,
//!   then promote IT"). This generalizes it to "upgrade ALL replicas first, then promote ANY upgraded
//!   in-sync one", which yields exactly ONE failover / one client redirect for the whole cluster (the
//!   ElastiCache pattern) instead of one per replica. That multi-replica ordering is this module's
//!   choice; the body is silent on it.
//! - FORWARD PATH ONLY: this models the happy-path sequence. It has NO state for a FAILED node
//!   upgrade, a ROLLBACK, or a mid-sequence ABORT (the single-node upgrade's auto-rollback,
//!   UPGRADE.md, is a per-node health-probe concern the driver owns). A replica that never returns to
//!   sync yields `AwaitInSync` indefinitely -- which FAILS CLOSED (it never promotes an unsafe
//!   candidate or forces ahead), so the driver owns the timeout / abort / rollback policy ON TOP of
//!   this sequencing. Adding an `Abort`/`Rollback` step is a clean future extension; it is left out
//!   here so the safe forward sequence can be pinned + tested first.

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

/// The outcome of driving the rolling upgrade to (attempted) completion (#392).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeReport {
    /// Every node upgraded, the primary LAST: the rolling upgrade completed.
    Completed,
    /// The upgrade did not finish within the tick budget -- a replica never caught up, or the
    /// promotion stayed BLOCKED (no quorum / no in-sync candidate). Carries the last step so the
    /// operator sees WHY it stalled. The driver fails loud here rather than looping forever.
    StalledAfterBudget(UpgradeStep),
}

/// The CLUSTER OPERATIONS the rolling-upgrade DRIVER invokes (#392 Phase 3). The live impl drives the
/// single-node self-updater per node, waits for re-attach/in-sync, and triggers the committed raft
/// `PromoteReplica` fence; it is unit-tested with a simulated-cluster mock. Splitting the cluster
/// ACTIONS behind this trait from the pure [`upgrade_step`] DECISION keeps the driver's SEQUENCING
/// testable without a live cluster (the live actions are the clustered layer).
pub trait UpgradeActions {
    /// A cluster action failure (a node upgrade timed out, a raft propose was rejected, ...). The
    /// driver surfaces it and STOPS rather than pressing on across a broken cluster.
    type Error;

    // -- observe the committed cluster state (the inputs [`upgrade_step`] decides on) --
    /// Replica nodes NOT yet on the new version.
    fn replicas_to_upgrade(&self) -> usize;
    /// Is a just-upgraded replica still re-syncing (not yet back in sync)?
    fn replica_catching_up(&self) -> bool;
    /// The [`safe_to_promote`](crate::lag::safe_to_promote) verdict for the chosen upgraded in-sync
    /// candidate (the lag gate + quorum).
    fn promotion_safety(&self) -> PromotionSafety;
    /// Has the `PromoteReplica` committed, so the old primary is now a replica?
    fn primary_demoted(&self) -> bool;
    /// Is the demoted old primary now on the new version?
    fn old_primary_upgraded(&self) -> bool;

    // -- execute a step's action --
    /// Upgrade the next not-yet-upgraded replica in place (drive its self-updater; it re-attaches).
    ///
    /// # Errors
    /// The cluster action's error if the replica upgrade cannot be started/completed.
    fn upgrade_next_replica(&mut self) -> Result<(), Self::Error>;
    /// Promote the chosen upgraded in-sync replica (the committed raft `PromoteReplica` fence).
    ///
    /// # Errors
    /// The cluster action's error if the promotion cannot be committed.
    fn promote_candidate(&mut self) -> Result<(), Self::Error>;
    /// Upgrade the demoted old primary (now a replica) -- the LAST node.
    ///
    /// # Errors
    /// The cluster action's error if the old-primary upgrade cannot be started/completed.
    fn upgrade_old_primary(&mut self) -> Result<(), Self::Error>;

    /// WAIT for the cluster to make progress before the next tick (a replica to catch up, quorum to
    /// return). The live impl polls the cluster with a backoff; the mock advances its simulated state.
    ///
    /// # Errors
    /// The cluster action's error if the wait/poll itself fails.
    fn wait_for_progress(&mut self) -> Result<(), Self::Error>;
}

/// Drive ONE step: read the cluster state, compute [`upgrade_step`], and execute the matching action.
/// Returns the step taken. `AwaitInSync` / `Blocked` / `Done` execute NO action (the caller waits or
/// finishes).
///
/// # Errors
///
/// Propagates the [`UpgradeActions::Error`] from the executed action (the upgrade stops on failure
/// rather than proceeding across a broken cluster).
pub fn drive_upgrade_step<A: UpgradeActions>(actions: &mut A) -> Result<UpgradeStep, A::Error> {
    let step = upgrade_step(
        actions.replicas_to_upgrade(),
        actions.replica_catching_up(),
        actions.promotion_safety(),
        actions.primary_demoted(),
        actions.old_primary_upgraded(),
    );
    match step {
        UpgradeStep::UpgradeReplica => actions.upgrade_next_replica()?,
        UpgradeStep::Promote => actions.promote_candidate()?,
        UpgradeStep::UpgradeOldPrimary => actions.upgrade_old_primary()?,
        // No action: the driver is waiting (AwaitInSync/Blocked) or the upgrade is finished (Done).
        UpgradeStep::AwaitInSync | UpgradeStep::Blocked(_) | UpgradeStep::Done => {}
    }
    Ok(step)
}

/// Drive the WHOLE rolling upgrade to completion (#392): loop [`drive_upgrade_step`], letting the
/// cluster make progress between ticks, until `Done`. Bounded by `max_ticks` so a stuck upgrade (a
/// replica that never catches up, or a promotion that stays `Blocked` because quorum never returns)
/// fails LOUD ([`UpgradeReport::StalledAfterBudget`]) instead of looping forever.
///
/// The safety invariants -- the primary is upgraded LAST, and a promotion happens only when
/// [`safe_to_promote`](crate::lag::safe_to_promote) says it is safe -- are enforced entirely by
/// [`upgrade_step`]; this loop merely executes its decisions and never reorders them.
///
/// # Errors
///
/// Propagates the first [`UpgradeActions::Error`] from an executed action or a wait.
pub fn run_rolling_upgrade<A: UpgradeActions>(
    actions: &mut A,
    max_ticks: usize,
) -> Result<UpgradeReport, A::Error> {
    let mut last = UpgradeStep::Done;
    for _ in 0..max_ticks {
        let step = drive_upgrade_step(actions)?;
        if step == UpgradeStep::Done {
            return Ok(UpgradeReport::Completed);
        }
        last = step;
        // Acted, AwaitInSync, or Blocked: let the cluster make progress (a node finishes upgrading, a
        // replica catches up, quorum returns) before re-evaluating.
        actions.wait_for_progress()?;
    }
    Ok(UpgradeReport::StalledAfterBudget(last))
}

#[cfg(test)]
mod tests {
    use super::{
        BlockReason, UpgradeReport, UpgradeStep, drive_upgrade_step, run_rolling_upgrade,
        upgrade_step,
    };
    use crate::lag::PromotionSafety;
    use crate::upgrade_plan::UpgradeActions;

    // Shorthands for the promotion verdict (Phase 2 only cares about it once replicas are done).
    const SAFE: PromotionSafety = PromotionSafety::Safe;
    const NO_QUORUM: PromotionSafety = PromotionSafety::NoQuorum;
    const NOT_IN_SYNC: PromotionSafety = PromotionSafety::CandidateNotInSync;

    /// A simulated cluster for driving [`run_rolling_upgrade`] without a live raft cluster. It evolves
    /// its state as the driver executes actions + waits, recording the action ORDER so a test can
    /// assert the driver drove the #392 sequence (replicas first -> promote -> old primary last).
    // A cluster-state simulation: each bool is an independent, named cluster condition, so bools are
    // the natural representation here (not the boolean-blind API `struct_excessive_bools` warns of).
    #[allow(clippy::struct_excessive_bools)]
    struct MockCluster {
        replicas_left: usize,
        catching_up: bool,
        /// An upgraded, in-sync replica exists to promote (set once a replica catches up).
        promotable_candidate: bool,
        /// The config-plane raft quorum is available (else `safe_to_promote` -> NoQuorum).
        quorum: bool,
        demoted: bool,
        old_primary_upgraded: bool,
        actions_log: Vec<&'static str>,
    }

    impl MockCluster {
        fn with_replicas(replicas: usize, quorum: bool) -> Self {
            MockCluster {
                replicas_left: replicas,
                catching_up: false,
                promotable_candidate: false,
                quorum,
                demoted: false,
                old_primary_upgraded: false,
                actions_log: Vec::new(),
            }
        }
    }

    impl UpgradeActions for MockCluster {
        type Error = ();

        fn replicas_to_upgrade(&self) -> usize {
            self.replicas_left
        }
        fn replica_catching_up(&self) -> bool {
            self.catching_up
        }
        fn promotion_safety(&self) -> PromotionSafety {
            if !self.quorum {
                PromotionSafety::NoQuorum
            } else if self.promotable_candidate {
                PromotionSafety::Safe
            } else {
                PromotionSafety::CandidateNotInSync
            }
        }
        fn primary_demoted(&self) -> bool {
            self.demoted
        }
        fn old_primary_upgraded(&self) -> bool {
            self.old_primary_upgraded
        }

        fn upgrade_next_replica(&mut self) -> Result<(), ()> {
            self.actions_log.push("upgrade_replica");
            self.replicas_left -= 1;
            self.catching_up = true; // a just-upgraded replica re-syncs before the next step
            Ok(())
        }
        fn promote_candidate(&mut self) -> Result<(), ()> {
            self.actions_log.push("promote");
            self.demoted = true; // the PromoteReplica fence committed
            Ok(())
        }
        fn upgrade_old_primary(&mut self) -> Result<(), ()> {
            self.actions_log.push("upgrade_old_primary");
            self.old_primary_upgraded = true;
            Ok(())
        }
        fn wait_for_progress(&mut self) -> Result<(), ()> {
            // A catching-up replica finishes re-syncing; once it is in sync it is a promotable
            // candidate. (Quorum is fixed per-test: if it never returns, the driver stays blocked.)
            if self.catching_up {
                self.catching_up = false;
                self.promotable_candidate = true;
            }
            Ok(())
        }
    }

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

    #[test]
    fn promote_never_fires_once_the_primary_is_demoted() {
        // The OTHER half of the "primary last" guarantee: once the promotion has committed
        // (primary_demoted), NO input combination can emit a SECOND `Promote` -- Phase 3 owns it, so
        // a stale/lying replica count or Safe verdict cannot re-trigger a failover. (A committed
        // demotion is only reachable after Phase 2 from a truthful driver; this pins the defensive
        // behavior regardless.)
        for &replicas in &[0usize, 1, 5] {
            for &catching in &[false, true] {
                for promo in [SAFE, NO_QUORUM, NOT_IN_SYNC] {
                    for &old_up in &[false, true] {
                        let step = upgrade_step(replicas, catching, promo, true, old_up);
                        assert!(
                            !matches!(step, UpgradeStep::Promote),
                            "no second promotion once demoted: {step:?}"
                        );
                        // Phase 3 emits only the old-primary work or Done.
                        assert!(
                            matches!(step, UpgradeStep::UpgradeOldPrimary | UpgradeStep::Done),
                            "phase 3 is old-primary-then-done: {step:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn run_rolling_upgrade_drives_replicas_first_then_promote_then_old_primary() {
        // Two replicas + quorum: the driver must upgrade BOTH replicas, then promote, then upgrade
        // the old primary LAST, and report Completed.
        let mut cluster = MockCluster::with_replicas(2, true);
        let report = run_rolling_upgrade(&mut cluster, 50).unwrap();
        assert_eq!(report, UpgradeReport::Completed);
        assert_eq!(
            cluster.actions_log,
            vec![
                "upgrade_replica",
                "upgrade_replica",
                "promote",
                "upgrade_old_primary",
            ],
            "the driver drove the #392 sequence: replicas first, promote, old primary last"
        );
    }

    #[test]
    fn run_rolling_upgrade_handles_a_single_replica_cluster() {
        let mut cluster = MockCluster::with_replicas(1, true);
        assert_eq!(
            run_rolling_upgrade(&mut cluster, 50).unwrap(),
            UpgradeReport::Completed
        );
        assert_eq!(
            cluster.actions_log,
            vec!["upgrade_replica", "promote", "upgrade_old_primary"]
        );
    }

    #[test]
    fn run_rolling_upgrade_stalls_loud_without_promoting_when_quorum_never_returns() {
        // Quorum absent for the whole run: once the replicas are upgraded the driver reaches the
        // promotion gate, which stays Blocked(NoQuorum) forever. It must NOT promote (the guardrail)
        // and must fail LOUD after the budget, not loop.
        let mut cluster = MockCluster::with_replicas(1, false);
        let report = run_rolling_upgrade(&mut cluster, 20).unwrap();
        assert_eq!(
            report,
            UpgradeReport::StalledAfterBudget(UpgradeStep::Blocked(BlockReason::NoQuorum))
        );
        // The replica was upgraded, but the promotion guardrail held: the ONLY action was the replica
        // upgrade -- NO promote, NO old-primary upgrade (the primary is never touched without a safe
        // promotion).
        assert_eq!(
            cluster.actions_log,
            vec!["upgrade_replica"],
            "the guardrail held: no promote / old-primary upgrade without quorum"
        );
    }

    #[test]
    fn drive_upgrade_step_executes_only_the_current_steps_action() {
        // A no-action step (Blocked: replicas done, no quorum) executes NOTHING.
        let mut blocked = MockCluster::with_replicas(0, false);
        let step = drive_upgrade_step(&mut blocked).unwrap();
        assert_eq!(step, UpgradeStep::Blocked(BlockReason::NoQuorum));
        assert!(blocked.actions_log.is_empty(), "Blocked executes no action");

        // An action step (UpgradeReplica) executes exactly its action.
        let mut acting = MockCluster::with_replicas(1, true);
        let step = drive_upgrade_step(&mut acting).unwrap();
        assert_eq!(step, UpgradeStep::UpgradeReplica);
        assert_eq!(acting.actions_log, vec!["upgrade_replica"]);
    }

    #[test]
    fn run_rolling_upgrade_surfaces_an_action_error_and_stops() {
        // A cluster whose upgrade action fails: the driver propagates the error and stops (does not
        // press on across a broken cluster).
        struct FailingCluster;
        impl UpgradeActions for FailingCluster {
            type Error = &'static str;
            fn replicas_to_upgrade(&self) -> usize {
                1
            }
            fn replica_catching_up(&self) -> bool {
                false
            }
            fn promotion_safety(&self) -> PromotionSafety {
                PromotionSafety::Safe
            }
            fn primary_demoted(&self) -> bool {
                false
            }
            fn old_primary_upgraded(&self) -> bool {
                false
            }
            fn upgrade_next_replica(&mut self) -> Result<(), &'static str> {
                Err("node upgrade timed out")
            }
            fn promote_candidate(&mut self) -> Result<(), &'static str> {
                Ok(())
            }
            fn upgrade_old_primary(&mut self) -> Result<(), &'static str> {
                Ok(())
            }
            fn wait_for_progress(&mut self) -> Result<(), &'static str> {
                Ok(())
            }
        }
        let mut cluster = FailingCluster;
        assert_eq!(
            run_rolling_upgrade(&mut cluster, 50),
            Err("node upgrade timed out")
        );
    }
}
