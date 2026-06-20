// SPDX-License-Identifier: MIT OR Apache-2.0
//! TURNKEY cluster formation (PROD-turnkey): auto-apply the SHIPPED static `cluster_topology`'s
//! node table + slot ownership on a FRESH raft cluster, so a deploy from the shipped artifacts
//! (`deploy/compose`, `deploy/helm`, `deploy/k8s`) reaches `cluster_state:ok` with all 16384 slots
//! assigned WITHOUT an operator hand-running `CLUSTER MEET` / `CLUSTER ADDSLOTS`.
//!
//! ## The gap this closes
//!
//! Raft formation already WORKS: three nodes with a consistent `cluster_topology` form a quorum and
//! elect a leader over the real bus. But a fresh raft cluster boots each node `empty_self` (owning
//! ZERO slots) and the topology's declared `slots` are NOT applied -- so the cluster sits at
//! `cluster_state:fail`, `cluster_slots_assigned:0`, `cluster_known_nodes:1` until an operator
//! manually issues `CLUSTER MEET` (each peer) + `CLUSTER ADDSLOTSRANGE` (per the topology). The
//! shipped static topology DECLARES each node's `slots` + the full peer list, but nothing applied
//! them: a fresh deploy elected a leader yet refused to serve. That is the non-turnkey gap.
//!
//! ## The fix (auto-apply, NOT a parallel bootstrap path)
//!
//! When a cluster forms and the COMMITTED cluster config is EMPTY (a truly fresh cluster), the
//! elected LEADER proposes the INITIAL node table + slot ownership EXACTLY as declared in
//! `cluster_topology`, through the SAME committed-log path a manual `CLUSTER MEET` /
//! `CLUSTER ADDSLOTS` uses ([`ConfigCmd::AddNode`] + [`ConfigCmd::AssignSlots`] via
//! [`RaftHandle::propose`](ironcache_server::RaftHandle::propose)). The commit replicates to every
//! node, whose `ConfigSm` applies it into the shared `Arc<SlotMap>`, so all nodes converge to
//! `cluster_state:ok` + full slot coverage -- turnkey. No new engine path, no new `ConfigCmd`: it
//! REUSES the existing slot-assignment machinery. `CLUSTER MEET` / `ADDSLOTS` / `SETSLOT` stay
//! exactly as before for RUNTIME changes (adding nodes, rebalancing, migrations).
//!
//! ## Why the SERVE layer (not the engine)
//!
//! The pure engine + `ConfigSm` are DST-deterministic and have NO access to `Config.cluster_topology`
//! (it is not part of the replicated state); injecting it would couple the pure consensus core to
//! deploy config. The serve layer, by contrast, already owns BOTH the `Config` and the
//! `RaftHandle`, and already turns a `CLUSTER ADDSLOTS` into a committed proposal. So the bootstrap
//! is a serve-layer background task that, once this node is the leader and the committed config is
//! empty, proposes the declared assignment through the unchanged propose path. The engine stays
//! BYTE-IDENTICAL (no engine source is touched); the DST sweep is unaffected.
//!
//! ## The idempotent + fresh-only guard (CRITICAL)
//!
//! A re-bootstrap on a node RESTART would be catastrophic (it would clobber runtime
//! migrations/rebalances). The guard is therefore HARD and rests on a PERSISTED FACT, not a
//! volatile projection: the leader proposes the bootstrap ONLY when this node booted with an EMPTY
//! persisted Raft log AND the committed config it can observe is still empty.
//!
//! The persisted-log gate ([`RaftHandle::has_persisted_log`](ironcache_server::RaftHandle::has_persisted_log))
//! is the load-bearing one. The earlier guard rested SOLELY on the shared-`SlotMap` projection
//! (`slots_assigned()` / `known_nodes()` / `current_epoch()`), which a node REPUBLISHES only when
//! its `ConfigSm` applies / restores. On the COMMON no-snapshot restart (the default
//! `raft_snapshot_threshold` is 1024, and a normally-sized cluster has a handful of log entries, so
//! it NEVER snapshots), the engine's recovery replays only the raft VOTER/learner membership; it
//! does NOT replay the committed `Config(ConfigCmd)` tail, so the shared map stays at its pristine
//! `empty_self` baseline (epoch 0, slots 0, known_nodes 1) until the run loop's `apply_committed`
//! catches up -- which happens AFTER the handle is returned and AFTER `is_leader()` can become true.
//! The driver thus had a window where a restarted, previously-migrated node sampled the projection
//! as FRESH and re-proposed the STATIC topology ABOVE the unapplied recovered tail, silently
//! reverting every runtime migration / failover when the tail later applied. The persisted-log fact
//! closes that window: a node that RESTARTED has a non-empty persisted log (`last_log_index > 0`)
//! captured at construction (BEFORE any apply races), so [`is_fresh_for_bootstrap`] returns false
//! regardless of the transient projection. A TRULY fresh node boots with an empty persisted log, so
//! it still bootstraps turnkey.
//!
//! The shared-map projection ([`is_fresh_committed_config`]) is kept as a SECONDARY belt: it makes a
//! node stand down the instant a PEER's bootstrap (or a runtime config) commits into the shared map
//! WITHIN this process's lifetime (where the persisted-log fact, frozen at boot, would not yet
//! reflect it). Both must hold to start: empty persisted log AND a fresh projection. The committed
//! bootstrap itself bumps the epoch / assigns slots, so the projection goes FALSE the instant the
//! bootstrap commits -- the task then exits and never proposes again. Freshness is RE-SAMPLED every
//! poll iteration (and again right before proposing), so a config committed by ANOTHER node between
//! iterations aborts this node's bootstrap.

use std::sync::Arc;
use std::time::Duration;

use ironcache_cluster::SlotMap;
use ironcache_config::ClusterTopology;
use ironcache_raft::ConfigCmd;
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::{ProposeOutcome, RaftHandle};

/// The poll cadence the bootstrap task waits between checks while it is NOT yet the leader or the
/// committed config is not yet observable as fresh. Coarse: formation takes election base+jitter
/// (150-300ms) plus a few RTTs, so a 200ms poll converges in well under a second once a leader
/// emerges, while costing almost nothing (a few cheap atomic reads per tick) until then.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Whether the committed cluster config is FRESH / UNINITIALIZED -- a truly fresh cluster that has
/// never committed any cluster config, so the static topology may be auto-applied (the fresh-only
/// guard). Returns `false` for ANY node that has already committed config (a runtime
/// MEET/ADDSLOTS/migration, OR a restart that recovered persisted committed state), so the
/// bootstrap NEVER clobbers a committed/migrated state.
///
/// All three conditions must hold (belt-and-suspenders against any single signal racing):
///   * `slots_assigned() == 0`: no slot has an owner yet (a committed AssignSlots would make this
///     non-zero);
///   * `known_nodes() <= 1`: only this node's own `empty_self` entry is in the committed table (a
///     committed AddNode of any peer would make this >= 2);
///   * `current_epoch() == 0`: the `ConfigSm` bumps the monotonic log-driven epoch +1 per applied
///     config entry AND re-publishes it on a snapshot restore, so a node that recovered ANY
///     committed config (a restart) reads a non-zero epoch even if its slot view were briefly
///     mid-apply.
#[must_use]
pub fn is_fresh_committed_config(cluster: &SlotMap) -> bool {
    cluster.slots_assigned() == 0 && cluster.known_nodes() <= 1 && cluster.current_epoch() == 0
}

/// The ROBUST fresh-only gate the bootstrap driver actually uses (PROD-turnkey): this node may
/// auto-apply the static topology ONLY when BOTH
///   * it booted with an EMPTY persisted Raft log (`!raft.has_persisted_log()`) -- the PERSISTED
///     FACT, captured at construction, that distinguishes a TRULY FRESH node from one that RESTARTED
///     onto persisted state. Crucially this holds EVEN on the COMMON no-snapshot restart, where the
///     engine recovers raft membership but does NOT replay the committed `ConfigSm` tail, so the
///     shared `SlotMap` is transiently pristine and the projection ([`is_fresh_committed_config`])
///     would FALSE-POSITIVE as fresh; AND
///   * the shared-map projection is still fresh ([`is_fresh_committed_config`]) -- the SECONDARY
///     belt that makes this node stand down the instant a PEER's bootstrap (or any runtime config)
///     commits into the shared map within THIS process's lifetime (which the boot-frozen persisted-
///     log fact alone would not reflect).
///
/// A restarted node has a non-empty persisted log, so it is NEVER fresh here and NEVER re-bootstraps
/// -- it cannot clobber runtime slot ownership / a migration / a failover. A truly fresh node (empty
/// log, pristine projection) still bootstraps turnkey.
#[must_use]
fn is_fresh_for_bootstrap(cluster: &SlotMap, raft: &RaftHandle) -> bool {
    !raft.has_persisted_log() && is_fresh_committed_config(cluster)
}

/// Build the committed-log batch that auto-applies the static `topology` on a fresh cluster: for
/// EVERY node, an [`ConfigCmd::AddNode`] (its id + advertised endpoint) so every node's table learns
/// it, FOLLOWED by, for every node that declares slots, an [`ConfigCmd::AssignSlots`] for that node's
/// declared slots. This is the committed analog of the operator running, for each node, a
/// `CLUSTER MEET <node>` + `CLUSTER ADDSLOTSRANGE <its ranges>` -- but issued ONCE by the leader
/// through the Raft log.
///
/// ORDER MATTERS (the committed-log invariant the `ConfigSm` relies on): EVERY `AddNode` is emitted
/// BEFORE ANY `AssignSlots`, so an assignment never references a node the table has not yet learned
/// (the same ordering `build_self_assign`'s `AddNode`-then-`AssignSlots` keeps). The declared
/// `[start, end]` ranges are expanded inclusively into the flat slot list `AssignSlots` carries; the
/// expansion mirrors `CLUSTER ADDSLOTSRANGE`'s. A node declaring no slots contributes only its
/// `AddNode` (a primary-less member, e.g. a pure replica host in a future topology).
///
/// This is a PURE function of the topology (no clock / entropy / I/O), so it is unit-testable and
/// the resulting batch is identical on every node that might be the leader.
#[must_use]
pub fn topology_bootstrap_cmds(topology: &ClusterTopology) -> Vec<ConfigCmd> {
    let mut cmds = Vec::with_capacity(topology.nodes.len() * 2);
    // (1) Every node's AddNode FIRST, in declaration order, so the table knows every id+endpoint
    // before any AssignSlots references it.
    for n in &topology.nodes {
        cmds.push(ConfigCmd::AddNode {
            id: n.id.clone(),
            host: n.host.clone(),
            port: n.port,
        });
    }
    // (2) Then each node's AssignSlots for its declared (inclusive-range-expanded) slots. A node
    // with no declared slots contributes nothing here (it is a member but owns no slot).
    for n in &topology.nodes {
        let slots = expand_ranges(&n.slots);
        if !slots.is_empty() {
            cmds.push(ConfigCmd::AssignSlots {
                node: n.id.clone(),
                slots,
            });
        }
    }
    cmds
}

/// Expand a list of inclusive `[start, end]` slot RANGES into the flat slot list `AssignSlots`
/// carries, in declaration order (mirroring `CLUSTER ADDSLOTSRANGE`'s expansion). A degenerate
/// `start > end` range (rejected by `Config::validate` / `SlotMap::build` before any node boots, so
/// unreachable here) contributes nothing rather than panicking, keeping this total.
fn expand_ranges(ranges: &[[u16; 2]]) -> Vec<u16> {
    let mut out = Vec::new();
    for &[start, end] in ranges {
        if start <= end {
            out.extend(start..=end);
        }
    }
    out
}

/// Spawn the TURNKEY-FORMATION background task on THIS shard's `LocalSet` (PROD-turnkey). Call it
/// ONCE per node (the coordinator gates it on shard 0 in raft-mode, mirroring the periodic-save
/// host), with the shared committed `cluster` map (== `ctx.cluster`), the `raft` handle, and the
/// shipped `topology`.
///
/// The task POLLS on a coarse cadence: each tick, if this node is the leader AND it is still
/// [`fresh-for-bootstrap`](is_fresh_for_bootstrap) (an EMPTY persisted Raft log AND a fresh
/// shared-map projection), it proposes the [`topology_bootstrap_cmds`] batch through the unchanged
/// propose path, driving to COMPLETION (all declared slots assigned). It RETURNS without proposing
/// when the node is non-fresh from the START (this node RESTARTED onto persisted committed state --
/// including the common no-snapshot restart -- OR a peer leader already bootstrapped), and RETURNS
/// once the declared slots are fully assigned. A follower tick is a cheap no-op. The whole task
/// short-circuits to a no-op when the topology declares no slots at all (nothing to bootstrap). See
/// [`run_bootstrap_driver`] for the exact two-phase (fresh-start guard, then drive-to-completion)
/// logic and the idempotent + fresh-only guarantees.
///
/// Idempotent + fresh-only by construction: the fresh-start guard is re-checked immediately before
/// the first propose, completion is judged by committed slot coverage (so a re-proposed idempotent
/// batch never double-assigns), and a per-node `Cell` guard ([`STARTED`]) makes a duplicate spawn
/// (defensive) a no-op.
pub fn spawn_on_shard(cluster: Arc<SlotMap>, raft: RaftHandle, topology: &ClusterTopology) {
    if STARTED.with(std::cell::Cell::get) {
        return; // already spawned on this node (idempotent, defensive).
    }
    STARTED.with(|c| c.set(true));

    // Nothing to bootstrap if the topology declares no slots at all (every node owns nothing): the
    // operator did not intend a static slot layout, so leave the cluster in its hands. This also
    // keeps the existing raft acceptance tests (which use an empty-`slots` topology) byte-unchanged:
    // they declare no slots, so this task does nothing and the manual MEET/ADDSLOTS path they drive
    // is untouched.
    let cmds = topology_bootstrap_cmds(topology);
    let has_assignment = cmds
        .iter()
        .any(|c| matches!(c, ConfigCmd::AssignSlots { .. }));
    if !has_assignment {
        return;
    }

    let slot_total = declared_slot_total(topology);
    let rt = TokioRuntime::new();
    let driver_rt = TokioRuntime::new();
    rt.spawn_on_shard(async move {
        run_bootstrap_driver(&driver_rt, &cluster, &raft, cmds, slot_total).await;
    });
}

/// The bootstrap driver loop (factored out of [`spawn_on_shard`] so it is unit-testable with a
/// for-test handle). `declared_slot_total` is the count of distinct slots the declared topology
/// assigns (the completion target).
///
/// The loop has two phases, divided by `started_bootstrap`:
///
///   * BEFORE starting: poll while this node is still [`fresh-for-bootstrap`](is_fresh_for_bootstrap)
///     -- an EMPTY persisted Raft log (the construction-time fact: not a restarted node) AND a fresh
///     shared-map projection. The instant it is NON-fresh from the START (this node RESTARTED onto
///     persisted committed state -- non-empty log, INCLUDING the common no-snapshot restart where
///     the projection is transiently pristine -- OR a peer leader already bootstrapped into the
///     shared map), return WITHOUT proposing. This is the fresh-only guard that makes a restart
///     never re-bootstrap / clobber a committed config / runtime migration / failover.
///   * AFTER this driver has STARTED proposing (`started_bootstrap = true`): drive to COMPLETION,
///     re-proposing the FULL idempotent batch on each leader tick until `slots_assigned()` reaches
///     `declared_slot_total`. Once this driver itself has committed PARTIAL progress (e.g. some
///     `AddNode`s before a leadership flap), the strict fresh guard no longer holds, but the work is
///     NOT yet done; so completion is judged by SLOT COVERAGE, not freshness. The ConfigCmds are
///     idempotent (re-applying an AddNode / AssignSlots yields the identical committed map), so a
///     re-proposed full batch -- by this node if it regains leadership, or, after a restart of this
///     node, NOT at all (it returns at the fresh guard, and the NEW leader's own driver, seeing
///     `slots_assigned() == 0` if nothing slot-committed yet, finishes it) -- converges safely with
///     no double-assignment.
///
/// NOTE on the partial-AddNode-then-this-node-dies case: if THIS leader committed some `AddNode`s but
/// no `AssignSlots` and then the process restarts, the restarted node booted with a NON-EMPTY
/// persisted log (those committed `AddNode` entries are on disk), so [`is_fresh_for_bootstrap`] is
/// FALSE on the construction-time persisted-log fact ALONE -- it does NOT depend on whether the
/// no-snapshot recovery republished the projection (the very gap this gate closes). The restarted
/// node will NOT resume; a NEW leader's own driver, if it too restarted, likewise stands down on its
/// non-empty log -- so the bootstrap would NOT auto-resume from this rare window (committed AddNode,
/// zero committed AssignSlots, then a full-leader-loss restart). It is vanishingly small (the batch
/// proposes all AddNodes then immediately the AssignSlots, all on one leader, committing in
/// milliseconds), and the safe fallback is the unchanged manual `CLUSTER ADDSLOTS` -- never an
/// incorrect or clobbered state. The common path (one leader commits the whole batch) and the
/// in-session leadership-flap path (this driver, still on its original empty-log boot, resumes to
/// completion via the slot-coverage gate) are both fully covered.
async fn run_bootstrap_driver<R: Runtime>(
    rt: &R,
    cluster: &SlotMap,
    raft: &RaftHandle,
    cmds: Vec<ConfigCmd>,
    declared_slot_total: u32,
) {
    let mut started_bootstrap = false;
    loop {
        // COMPLETION: once the declared slots are all assigned in the committed map, the bootstrap is
        // DONE (whether this driver, a peer's driver, or a recovered restart established it). Return
        // so the driver never proposes again.
        if cluster.slots_assigned() >= declared_slot_total {
            return;
        }
        // FRESH-ONLY START GUARD: until THIS driver has begun proposing, only start on a node that
        // is fresh for bootstrap -- an EMPTY persisted Raft log (the construction-time fact: this is
        // NOT a restarted node) AND a still-fresh shared-map projection. A non-fresh node we have NOT
        // touched means it RESTARTED onto persisted committed state (non-empty log, INCLUDING the
        // common no-snapshot restart where the projection is transiently pristine), or a peer
        // bootstrapped into the shared map -> stand down (never clobber a committed config / runtime
        // migration / failover). Re-sampled every iteration. Once we HAVE started, skip this guard:
        // our own partial commits make the projection non-fresh, but the slot-coverage check above
        // is the real completion gate, so we drive to finish.
        if !started_bootstrap && !is_fresh_for_bootstrap(cluster, raft) {
            return;
        }
        // Only the LEADER proposes the bootstrap (a single proposer); a follower waits. `is_leader`
        // is a cheap non-blocking status read.
        if raft.is_leader() {
            // Re-check the START guard right before proposing so a peer's bootstrap that landed since
            // the loop top (or this node's now-observable restarted state) makes this (not-yet-
            // started) node stand down on the next iteration.
            if started_bootstrap || is_fresh_for_bootstrap(cluster, raft) {
                started_bootstrap = true;
                // Propose each cmd in order, awaiting TRUE COMMIT. A NotLeader mid-batch (we lost
                // leadership) breaks out to re-poll; the idempotent batch is safely re-proposed when
                // we regain leadership (driving to completion via the slot-coverage gate above).
                let mut committed_all = true;
                for cmd in &cmds {
                    match raft.propose(cmd.clone()).await {
                        ProposeOutcome::Committed(_) => {}
                        ProposeOutcome::NotLeader => {
                            committed_all = false;
                            break;
                        }
                    }
                }
                if committed_all {
                    // The whole declared assignment committed; the next loop-top coverage check is now
                    // satisfied, so return promptly (do not propose again).
                    tracing::info!(
                        "turnkey formation: auto-applied the static cluster_topology (node table + \
                         slot ownership) on a fresh cluster; cluster_state should now converge to ok"
                    );
                    continue;
                }
            }
        }
        // Not yet the leader, or a mid-batch leadership loss: wait a tick and re-check. The timer is
        // the runtime SEAM (ADR-0003), never tokio::time directly.
        rt.timer(POLL_INTERVAL).await;
    }
}

/// The count of distinct slots a topology assigns across all its nodes (the bootstrap completion
/// target). A PURE helper over the declared `[start, end]` ranges; for the shipped full-coverage
/// topology this is 16384.
#[must_use]
fn declared_slot_total(topology: &ClusterTopology) -> u32 {
    topology
        .nodes
        .iter()
        .map(|n| expand_ranges(&n.slots).len() as u32)
        .sum()
}

thread_local! {
    /// Per-node (per shard-0 thread) guard so a DUPLICATE [`spawn_on_shard`] call (defensive) is a
    /// no-op, mirroring `replica_attach`'s `PRIMARY_STARTED`. The task is spawned only on shard 0 in
    /// raft-mode, so this is one cell on the one thread that runs it.
    static STARTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_config::ClusterNode;

    const ID0: &str = "0000000000000000000000000000000000000000";
    const ID1: &str = "1111111111111111111111111111111111111111";
    const ID2: &str = "2222222222222222222222222222222222222222";

    /// The shipped 3-node split: node0 [0,5460], node1 [5461,10922], node2 [10923,16383].
    fn shipped_topology() -> ClusterTopology {
        ClusterTopology {
            nodes: vec![
                ClusterNode {
                    id: ID0.to_owned(),
                    host: "ironcache-1".to_owned(),
                    port: 6379,
                    slots: vec![[0, 5460]],
                },
                ClusterNode {
                    id: ID1.to_owned(),
                    host: "ironcache-2".to_owned(),
                    port: 6379,
                    slots: vec![[5461, 10922]],
                },
                ClusterNode {
                    id: ID2.to_owned(),
                    host: "ironcache-3".to_owned(),
                    port: 6379,
                    slots: vec![[10923, 16383]],
                },
            ],
        }
    }

    /// `expand_ranges` expands inclusive ranges into the flat slot list, in order, and drops a
    /// degenerate `start > end` range (unreachable past validation) rather than panicking.
    #[test]
    fn expand_ranges_is_inclusive_ordered_and_total() {
        assert_eq!(expand_ranges(&[[0, 2]]), vec![0, 1, 2]);
        assert_eq!(expand_ranges(&[[5, 5]]), vec![5]);
        assert_eq!(expand_ranges(&[[0, 1], [10, 11]]), vec![0, 1, 10, 11]);
        assert_eq!(expand_ranges(&[]), Vec::<u16>::new());
        // Degenerate start>end contributes nothing (does not panic).
        assert_eq!(expand_ranges(&[[5, 3]]), Vec::<u16>::new());
        // The boundary slot 16383 is included.
        let last = expand_ranges(&[[16_380, 16_383]]);
        assert_eq!(last, vec![16_380, 16_381, 16_382, 16_383]);
    }

    /// The bootstrap batch emits EVERY node's AddNode FIRST (so the table knows every id before any
    /// assignment), THEN each node's AssignSlots for its declared (range-expanded) slots, and the
    /// assignments together cover all 16384 slots exactly once.
    #[test]
    fn topology_bootstrap_cmds_adds_all_nodes_then_assigns_full_coverage() {
        let cmds = topology_bootstrap_cmds(&shipped_topology());
        // 3 AddNode + 3 AssignSlots.
        assert_eq!(cmds.len(), 6);
        // The first three are AddNode, in declaration order, with the advertised endpoints.
        match &cmds[0] {
            ConfigCmd::AddNode { id, host, port } => {
                assert_eq!(id, ID0);
                assert_eq!(host, "ironcache-1");
                assert_eq!(*port, 6379);
            }
            other => panic!("cmds[0] must be AddNode, got {other:?}"),
        }
        assert!(matches!(&cmds[1], ConfigCmd::AddNode { id, .. } if id == ID1));
        assert!(matches!(&cmds[2], ConfigCmd::AddNode { id, .. } if id == ID2));
        // EVERY AddNode precedes EVERY AssignSlots (the committed-log ordering invariant).
        let first_assign = cmds
            .iter()
            .position(|c| matches!(c, ConfigCmd::AssignSlots { .. }))
            .expect("there is an AssignSlots");
        let last_add = cmds
            .iter()
            .rposition(|c| matches!(c, ConfigCmd::AddNode { .. }))
            .expect("there is an AddNode");
        assert!(
            last_add < first_assign,
            "all AddNode must precede all AssignSlots"
        );
        // The assignments together cover all 16384 slots, each exactly once, partitioned per node.
        let mut covered = vec![0u32; 16_384];
        for c in &cmds {
            if let ConfigCmd::AssignSlots { node, slots } = c {
                // Each node's slot block matches its declared range.
                let expected: Vec<u16> = match node.as_str() {
                    ID0 => (0..=5460).collect(),
                    ID1 => (5461..=10_922).collect(),
                    ID2 => (10_923..=16_383).collect(),
                    other => panic!("unexpected node {other}"),
                };
                assert_eq!(slots, &expected, "node {node} slot block");
                for &s in slots {
                    covered[s as usize] += 1;
                }
            }
        }
        assert!(
            covered.iter().all(|&c| c == 1),
            "every one of the 16384 slots is assigned exactly once"
        );
    }

    /// A topology where NO node declares slots yields a batch with AddNodes but NO AssignSlots, so
    /// `spawn_on_shard` would short-circuit to a no-op (the existing raft acceptance tests use such a
    /// topology, so turnkey leaves their manual MEET/ADDSLOTS flow untouched).
    #[test]
    fn topology_bootstrap_cmds_no_slots_yields_no_assignment() {
        let topo = ClusterTopology {
            nodes: vec![
                ClusterNode {
                    id: ID0.to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port: 7000,
                    slots: vec![],
                },
                ClusterNode {
                    id: ID1.to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port: 7001,
                    slots: vec![],
                },
            ],
        };
        let cmds = topology_bootstrap_cmds(&topo);
        assert_eq!(cmds.len(), 2, "two AddNode, no AssignSlots");
        assert!(
            !cmds
                .iter()
                .any(|c| matches!(c, ConfigCmd::AssignSlots { .. })),
            "no AssignSlots when no node declares slots"
        );
    }

    /// `declared_slot_total` counts the distinct assigned slots across the topology (the bootstrap
    /// completion target): 16384 for the shipped full-coverage split, and 0 for an empty-slots
    /// topology.
    #[test]
    fn declared_slot_total_counts_all_assigned_slots() {
        assert_eq!(declared_slot_total(&shipped_topology()), 16_384);
        let empty = ClusterTopology {
            nodes: vec![ClusterNode {
                id: ID0.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7000,
                slots: vec![],
            }],
        };
        assert_eq!(declared_slot_total(&empty), 0);
        // A partial-coverage topology counts only what it declares.
        let partial = ClusterTopology {
            nodes: vec![ClusterNode {
                id: ID0.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7000,
                slots: vec![[0, 9]],
            }],
        };
        assert_eq!(declared_slot_total(&partial), 10);
    }

    /// The driver RETURNS IMMEDIATELY (no propose, no timer) when the declared slots are ALREADY all
    /// assigned in the committed map -- the COMPLETION gate. This is the steady state a node observes
    /// after a peer leader bootstrapped, or after a RESTART that recovered the committed assignment:
    /// it must NOT re-bootstrap. We drive the real async loop on a tokio runtime; if the completion
    /// gate did not fire it would block on the poll timer and the test's bounded wait would catch it.
    #[test]
    fn driver_returns_immediately_when_slots_already_fully_assigned() {
        use ironcache_raft::NodeId;
        let map = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        // Simulate a fully-bootstrapped committed map: self owns every slot (the completion target
        // for a single-node declared total of 16384). add_slots claims them for self.
        let all: Vec<u16> = (0..16_384).collect();
        map.add_slots(&all).expect("self claims all slots");
        assert_eq!(map.slots_assigned(), 16_384);

        // A non-leader for-test handle (its propose lands NotLeader); the completion gate must return
        // BEFORE leadership / propose is ever consulted.
        let raft = RaftHandle::for_test(NodeId(0), None);
        let rt = TokioRuntime::new();
        let cmds = topology_bootstrap_cmds(&shipped_topology());

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            // A generous bound: the driver must RETURN (complete), not park on the timer.
            let res = tokio::time::timeout(
                Duration::from_secs(5),
                run_bootstrap_driver(&rt, &map, &raft, cmds, 16_384),
            )
            .await;
            assert!(
                res.is_ok(),
                "driver must return immediately when slots are already fully assigned (completion gate)"
            );
        });
    }

    /// The driver stands down (returns) when the committed config is NON-fresh from the START and it
    /// has NOT begun bootstrapping -- a peer already bootstrapped (or this node restarted onto
    /// committed state), so it must never propose. Here the map has a committed peer + a non-zero
    /// epoch (a restart-recovered signal) but slots are NOT yet fully assigned, so the completion gate
    /// does not fire; the fresh-only START guard is what returns.
    #[test]
    fn driver_stands_down_when_config_is_non_fresh_from_the_start() {
        use ironcache_raft::NodeId;
        let map = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        // Non-fresh: a peer is in the table and the committed epoch advanced (recovered state), yet
        // fewer than the declared 16384 slots are assigned (so it is NOT complete either).
        map.meet(ironcache_cluster::NodeEntry {
            id: ID1.into(),
            host: "127.0.0.1".into(),
            port: 7001,
        });
        map.set_committed_epoch(3);
        assert!(!is_fresh_committed_config(&map));
        assert!(map.slots_assigned() < 16_384);

        let raft = RaftHandle::for_test(NodeId(0), None);
        let rt = TokioRuntime::new();
        let cmds = topology_bootstrap_cmds(&shipped_topology());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let res = tokio::time::timeout(
                Duration::from_secs(5),
                run_bootstrap_driver(&rt, &map, &raft, cmds, 16_384),
            )
            .await;
            assert!(
                res.is_ok(),
                "driver must stand down (return) on a non-fresh committed config it did not start, \
                 NEVER re-bootstrapping / clobbering it"
            );
        });
    }

    /// THE NO-SNAPSHOT-RESTART HAZARD (the bug this fix closes). A node that RESTARTED onto persisted
    /// state but took the NO-SNAPSHOT recovery path leaves the shared `SlotMap` TRANSIENTLY PRISTINE
    /// (epoch 0, zero slots, only self) -- so the old shared-map-only guard FALSE-POSITIVED as fresh
    /// and, while this node was already the LEADER, re-proposed the static topology above the
    /// unapplied committed tail, clobbering runtime ownership. Now the driver consults the engine's
    /// RECOVERED PERSISTED-LOG fact: a non-empty log (`recovered_last_log_index > 0`) means RESTARTED,
    /// so [`is_fresh_for_bootstrap`] is false and the driver STANDS DOWN even though it IS the leader
    /// and the projection looks pristine. This is the regression guard for the data-loss race.
    #[test]
    fn driver_stands_down_on_no_snapshot_restart_even_with_pristine_projection_and_leadership() {
        use ironcache_raft::NodeId;
        // The shared map as a no-snapshot restart leaves it: PRISTINE (the ConfigSm has not yet
        // replayed the committed tail), so the shared-map projection alone reads FRESH.
        let map = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        assert!(
            is_fresh_committed_config(&map),
            "the no-snapshot restart leaves the projection transiently pristine (looks fresh)"
        );
        assert!(map.slots_assigned() < 16_384);

        // This node IS the leader (a quorum round-trip can win leadership before apply_committed
        // replays the tail) AND it RESTARTED: its engine recovered a NON-EMPTY persisted log
        // (recovered_last_log_index > 0). The old code would have re-bootstrapped here; the fix must
        // stand down on the persisted-log fact ALONE.
        let raft = RaftHandle::for_test_recovered(NodeId(0), Some(NodeId(0)), 7);
        assert!(
            raft.is_leader(),
            "the restarted node has already won leadership"
        );
        assert!(
            raft.has_persisted_log(),
            "a restarted node has a non-empty persisted log"
        );
        assert!(
            !is_fresh_for_bootstrap(&map, &raft),
            "a restarted node (non-empty persisted log) is NEVER fresh-for-bootstrap, even with a \
             transiently-pristine projection and leadership"
        );

        let rt = TokioRuntime::new();
        let cmds = topology_bootstrap_cmds(&shipped_topology());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let res = tokio::time::timeout(
                Duration::from_secs(5),
                run_bootstrap_driver(&rt, &map, &raft, cmds, 16_384),
            )
            .await;
            assert!(
                res.is_ok(),
                "driver must STAND DOWN (return without proposing) on a no-snapshot restart even \
                 though it is the leader and the shared-map projection is transiently pristine; it \
                 must NEVER re-bootstrap / clobber the (still-unapplied) recovered runtime ownership"
            );
            // The map was NOT mutated by a re-bootstrap (still pristine; no slots clobbered in).
            assert_eq!(
                map.slots_assigned(),
                0,
                "the driver must not have proposed/applied any slot assignment on a restarted node"
            );
        });
    }

    /// A TRULY FRESH node (empty persisted log) with a pristine projection IS fresh-for-bootstrap, so
    /// turnkey still forms on a genuinely fresh cluster (the persisted-log gate does not over-block).
    #[test]
    fn is_fresh_for_bootstrap_is_true_only_on_a_truly_fresh_node() {
        use ironcache_raft::NodeId;
        let map = SlotMap::empty_self(ID0, "127.0.0.1", 7000);

        // Truly fresh: empty persisted log AND pristine projection -> fresh-for-bootstrap.
        let fresh = RaftHandle::for_test_recovered(NodeId(0), Some(NodeId(0)), 0);
        assert!(!fresh.has_persisted_log());
        assert!(
            is_fresh_for_bootstrap(&map, &fresh),
            "a truly fresh node (empty log, pristine projection) is fresh-for-bootstrap"
        );

        // A non-empty persisted log alone (restart) flips it false, regardless of the projection.
        let restarted = RaftHandle::for_test_recovered(NodeId(0), Some(NodeId(0)), 1);
        assert!(restarted.has_persisted_log());
        assert!(
            !is_fresh_for_bootstrap(&map, &restarted),
            "a non-empty persisted log (a restart) is NOT fresh-for-bootstrap"
        );

        // A non-fresh projection (a peer landed in-process) also flips it false on a fresh-log node.
        let with_peer = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        with_peer.meet(ironcache_cluster::NodeEntry {
            id: ID1.into(),
            host: "127.0.0.1".into(),
            port: 7001,
        });
        assert!(
            !is_fresh_for_bootstrap(&with_peer, &fresh),
            "a peer committed into the shared map (in-process) is NOT fresh-for-bootstrap"
        );
    }

    /// The fresh-only guard: a freshly-seeded `empty_self` map (zero slots, only self in the table,
    /// epoch 0) is FRESH; once ANY committed config is applied (a slot assigned, a peer added, or the
    /// epoch advanced -- the restart signal), it is NO LONGER fresh, so the bootstrap stands down.
    #[test]
    fn is_fresh_committed_config_is_true_only_on_a_pristine_empty_self_map() {
        let map = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        assert!(
            is_fresh_committed_config(&map),
            "a pristine empty_self map is fresh"
        );

        // Assigning a slot makes it non-fresh (slots_assigned > 0).
        let assigned = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        assigned.add_slots(&[0]).expect("self can claim a slot");
        assert!(
            !is_fresh_committed_config(&assigned),
            "a map with an assigned slot is NOT fresh"
        );

        // Adding a peer makes it non-fresh (known_nodes > 1).
        let with_peer = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        with_peer.meet(ironcache_cluster::NodeEntry {
            id: ID1.into(),
            host: "127.0.0.1".into(),
            port: 7001,
        });
        assert!(
            !is_fresh_committed_config(&with_peer),
            "a map with a peer in the table is NOT fresh"
        );

        // A non-zero committed epoch (the restart-recovered-config signal) makes it non-fresh even
        // if the slot/table view momentarily looked empty.
        let restarted = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        restarted.set_committed_epoch(1);
        assert!(
            !is_fresh_committed_config(&restarted),
            "a non-zero committed epoch (a restart that recovered config) is NOT fresh"
        );
    }
}
