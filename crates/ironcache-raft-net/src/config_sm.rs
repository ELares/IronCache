// SPDX-License-Identifier: MIT OR Apache-2.0
//! The PRODUCTION config state machine (HA-4c): replay committed [`ConfigCmd`]s onto a
//! `SlotMap` shared with the live serve path.
//!
//! HA-3e proved the config state machine in the raft engine's own tests (`tests::ConfigSm`):
//! a [`StateMachine`] that drives an [`ironcache_cluster::SlotMap`] from the committed log so
//! every node converges to one identical ownership view. [`ConfigSm`] is that machine made
//! PRODUCTION-real and SHARED: instead of owning its `SlotMap` privately, it holds an
//! `Arc<SlotMap>` that is ALSO `ctx.cluster` on every shard. The single Raft control-plane
//! task is the sole WRITER (via [`StateMachine::apply`] off the committed log); the shards
//! READ the same map concurrently for routing (`owns` / `moved_target`) and the CLUSTER
//! projection. `SlotMap` already has the interior mutability (atomics + a cold-path node
//! lock) for exactly this single-writer / many-reader shape.
//!
//! ## Convergence (the whole point)
//!
//! Every node applies the SAME committed `ConfigCmd` sequence in the SAME order into ITS
//! `Arc<SlotMap>`. Because [`StateMachine::apply`] is deterministic and the committed log is
//! byte-identical on every node (Raft's Log Matching + State Machine Safety), the slot ->
//! owner projection and the node table converge to one identical GLOBAL view. That is the
//! linearizable slot-ownership property: no two nodes ever claim the same slot at the same
//! committed config epoch.
//!
//! ## Epoch policy (mirrors HA-3e)
//!
//! A MONOTONIC, LOG-DRIVEN epoch is bumped `+1` per applied config entry, so it is a
//! deterministic function of the applied prefix (two nodes at the same epoch have applied the
//! identical config prefix and therefore agree on every slot's owner). It is published into
//! the `SlotMap` via [`SlotMap::set_committed_epoch`](ironcache_cluster::SlotMap::set_committed_epoch)
//! so `CLUSTER INFO cluster_current_epoch` reflects it. We deliberately do NOT use
//! `SlotMap::bump_epoch` / `set_config_epoch`, whose Redis admin-command semantics (STILL once
//! at the max; rejected once peers are known) are wrong for a log-driven counter and would let
//! distinct ownership states share an epoch.

use std::sync::Arc;

use ironcache_cluster::{NodeEntry, SlotMap};
use ironcache_raft::{ConfigCmd, EntryPayload, LogEntry, StateMachine};

/// The production config state machine: drives the SHARED `Arc<SlotMap>` (which is also
/// `ctx.cluster` on every shard) from the committed log (HA-4c). See the module docs.
#[derive(Debug)]
pub struct ConfigSm {
    /// The slot map shared with the live serve path. This machine is the SOLE writer (off the
    /// committed log, on the single control-plane task); shards read it concurrently for
    /// routing + projection. `SlotMap`'s interior mutability makes that safe.
    map: Arc<SlotMap>,
    /// The monotonic, LOG-DRIVEN config epoch: `+1` per applied config entry (a deterministic
    /// function of the applied prefix). Published into the map via `set_committed_epoch`. NOT
    /// the `SlotMap`'s Redis-client epoch primitives (whose admin semantics are wrong here).
    epoch: u64,
}

impl ConfigSm {
    /// Wrap a SHARED `Arc<SlotMap>` (typically the same `Arc` the serve path holds as
    /// `ctx.cluster`). The machine seeds its log-driven epoch at `0`; the caller is expected
    /// to seed the map as `empty_self(self_id, host, port)` (a fresh cluster-enabled node
    /// owning zero slots), exactly like a fresh real-Redis node before `CLUSTER ADDSLOTS`.
    #[must_use]
    pub fn new(map: Arc<SlotMap>) -> Self {
        ConfigSm { map, epoch: 0 }
    }

    /// Borrow the shared slot map (test inspection / wiring).
    #[must_use]
    pub fn map(&self) -> &Arc<SlotMap> {
        &self.map
    }

    /// The monotonic, log-driven config epoch (count of applied config entries).
    #[must_use]
    pub fn config_epoch(&self) -> u64 {
        self.epoch
    }
}

impl StateMachine for ConfigSm {
    fn apply(&mut self, entry: &LogEntry) {
        // Only Config payloads touch the slot map; Noop (the leader's election no-op) and
        // Bytes (opaque) are no-ops for the config machine, exactly as the engine commits
        // them without interpretation.
        let EntryPayload::Config(cmd) = &entry.payload else {
            return;
        };
        // Every committed config entry advances the monotonic, log-driven config epoch
        // (CONTROL_PLANE.md). The +1-per-applied-entry counter makes the epoch a deterministic
        // function of the applied prefix, so two nodes at the same epoch have applied the
        // identical config prefix and agree on every slot's owner (the linearizable-ownership
        // property). Surface it through the unchecked `set_committed_epoch` setter, NOT
        // `bump_epoch` / `set_config_epoch` (whose Redis admin semantics would let distinct
        // ownership states share an epoch).
        self.epoch += 1;
        self.map.set_committed_epoch(self.epoch);
        // Mutation errors from the SlotMap are DETERMINISTIC across nodes (same map state +
        // same command), so swallowing them keeps every node's apply identical. The proposal
        // path validates a command's preconditions on the leader before proposing, so the
        // committed order never produces a spurious error (AddNode precedes any reference to
        // the node; ownership is moved away before a RemoveNode).
        match cmd {
            ConfigCmd::AddNode { id, host, port } => {
                // Idempotent: a node applying AddNode for its OWN id (already in its empty_self
                // table) is a no-op in the cluster crate.
                self.map.meet(NodeEntry {
                    id: id.as_str().into(),
                    host: host.as_str().into(),
                    port: *port,
                });
            }
            ConfigCmd::RemoveNode { id } => {
                let _ = self.map.forget(id);
            }
            ConfigCmd::SetSlotOwner { slot, node } => {
                let _ = self.map.set_slot_node(*slot, node);
            }
            ConfigCmd::AssignSlots { node, slots } => {
                for &slot in slots {
                    let _ = self.map.set_slot_node(slot, node);
                }
            }
            ConfigCmd::AssignReplica { node, slots } => {
                // HA-7d: record `node` as the replica of each slot in the NEW parallel structure
                // (set_slot_replica), NOT the owner bitmap. Deterministic across nodes (same map
                // state + same command), so swallowing the unknown-node error keeps every node's
                // apply identical; the leader proposes AddNode{node} before this, so the committed
                // order never produces a spurious error.
                for &slot in slots {
                    let _ = self.map.set_slot_replica(slot, node);
                }
            }
            ConfigCmd::PromoteReplica { slots, new_primary } => {
                // HA-8 FAILOVER (the SOLE ownership-transfer-on-failover path). For each slot:
                // (1) flip the OWNER to `new_primary` via the SAME `set_slot_node` path the other
                // ownership commands use, which keeps `mine[]` in lockstep with `owner[]` (so the
                // hot `owns()` bitmap is correct on every node, INCLUDING the old primary once it
                // catches its log up: its `mine[slot]` goes false -> `owns()` false -> it serves
                // MOVED); then (2) CLEAR `new_primary` from the slot's replica set (it is the owner
                // now, not a replica). Deterministic across nodes (same map state + same command),
                // so swallowing the unknown-node error keeps every node's apply identical; the
                // promotion is proposed only after `new_primary` is a committed AddNode + replica,
                // so the committed order never produces a spurious error. IDEMPOTENT: re-applying
                // sets the same owner and re-clears an already-clear replica entry (a no-op).
                //
                // THE SPLIT-BRAIN FENCE is exactly this apply: promotion ownership flows ONLY
                // through the committed log, so there is never a committed state in which two nodes
                // both `owns()` a slot. The per-entry epoch bump advances the cluster config epoch
                // monotonically, so a node holding stale ownership at an OLDER epoch is provably
                // behind; a client hitting the old owner gets a standard MOVED redirect to the new
                // owner (the redirect itself carries no epoch -- the epoch is the consensus-side
                // ordering that guarantees the old owner stops owning once it applies this entry).
                for &slot in slots {
                    let _ = self.map.set_slot_node(slot, new_primary);
                    self.map.clear_slot_replica(slot, new_primary);
                }
            }
            ConfigCmd::SetSlotMigrating { slot, dest } => {
                // HA-6 SOURCE-side: tag the slot MIGRATING toward `dest`. Because committed
                // ConfigCmds apply on EVERY node, the tag must be NODE-RELATIVE: ONLY the slot's
                // current OWNER (the SOURCE) carries a MIGRATING tag (a non-owner is not the source
                // and must not show MIGRATING -- otherwise a later SetSlotImporting on the same node
                // would clobber it, or a non-source would wrongly ASK). `owns()` is the node-relative
                // discriminator. Writes the parallel migration arrays only, NOT the owner bitmap, so
                // owns() is unchanged. Deterministic + idempotent; the unknown-node error is swallowed
                // (the leader proposes AddNode{dest} first, so the committed order never errors).
                if self.map.owns(*slot) {
                    let _ = self.map.set_migrating(*slot, dest);
                }
            }
            ConfigCmd::SetSlotImporting { slot, src, dest } => {
                // HA-6 DESTINATION-side: tag the slot IMPORTING from `src` on EXACTLY the `dest`
                // node. NODE-RELATIVE: a committed ConfigCmd applies on EVERY node, so the tag must
                // be set only where THIS node IS the destination. The discriminator is
                // `is_self(dest)` (an endpoint compare against `me()`, mirroring `set_slot_node` /
                // `is_replica_of_self` so the dual announce-id / synth-id identity is handled), NOT
                // `!owns()`: in an N>=3 cluster a BYSTANDER (a third node that is neither `src` nor
                // `dest`) is ALSO a non-owner, so the old `!owns()` form tagged IT importing too --
                // which, combined with a leaked one-shot ASKING, would serve a key on a wrong-owner
                // node. Gating on the dest tags EXACTLY the one importer. Writes the parallel arrays
                // only; owns() unchanged -- ownership stays with `src` until the committed FLIP.
                // Deterministic + idempotent; the unknown-node error is swallowed (the leader
                // proposes AddNode{src} first, so the committed order never errors). MIGRATING stays
                // gated on owns() (uniquely the source), so it needs no dest.
                if self.map.is_self(dest) {
                    let _ = self.map.set_importing(*slot, src);
                }
            }
            ConfigCmd::SetSlotStable { slot } => {
                // HA-6: clear the slot's migration state (the abort path; a committed FLIP clears
                // it on its own via set_slot_node). Idempotent; node-relative clears are harmless.
                self.map.clear_migration(*slot);
            }
            ConfigCmd::SetConfigEpoch(_epoch) => {
                // The Raft-driven config epoch is the log-driven counter above; the SlotMap's
                // own (Redis-client) epoch is not used for the linearizable-ownership property.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_raft::LogEntry;

    const ID0: &str = "0000000000000000000000000000000000000000";
    const ID1: &str = "1111111111111111111111111111111111111111";
    const ID2: &str = "2222222222222222222222222222222222222222";

    fn config_entry(index: u64, cmd: ConfigCmd) -> LogEntry {
        LogEntry {
            term: 1,
            index,
            payload: EntryPayload::Config(cmd),
        }
    }

    /// Applying committed ConfigCmds drives the SHARED Arc<SlotMap>: AddNode grows the table,
    /// AssignSlots claims slots for self, SetSlotOwner flips ownership, and the log-driven
    /// epoch is published into the map's current_epoch. A non-Config entry is a no-op (no
    /// epoch bump).
    #[test]
    fn apply_drives_the_shared_map_and_bumps_the_log_driven_epoch() {
        let map = Arc::new(SlotMap::empty_self(ID0, "127.0.0.1", 7000));
        let mut sm = ConfigSm::new(Arc::clone(&map));
        assert_eq!(sm.config_epoch(), 0);
        assert_eq!(map.current_epoch(), 0);

        // A Noop entry is a no-op for the config machine (no epoch bump, no map change).
        sm.apply(&LogEntry {
            term: 1,
            index: 1,
            payload: EntryPayload::Noop,
        });
        assert_eq!(sm.config_epoch(), 0);
        assert_eq!(map.slots_assigned(), 0);

        // AddNode a peer, then self claims [0, 2], then flip slot 1 to the peer.
        sm.apply(&config_entry(
            2,
            ConfigCmd::AddNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7001,
            },
        ));
        sm.apply(&config_entry(
            3,
            ConfigCmd::AssignSlots {
                node: ID0.to_owned(),
                slots: vec![0, 1, 2],
            },
        ));
        sm.apply(&config_entry(
            4,
            ConfigCmd::SetSlotOwner {
                slot: 1,
                node: ID1.to_owned(),
            },
        ));

        // The shared map (== ctx.cluster) reflects every committed change.
        assert_eq!(map.known_nodes(), 2);
        assert!(map.owns(0));
        assert!(!map.owns(1), "slot 1 was flipped to the peer");
        assert!(map.owns(2));
        // Three Config entries applied -> log-driven epoch 3, published into the map.
        assert_eq!(sm.config_epoch(), 3);
        assert_eq!(map.current_epoch(), 3);
    }

    /// Two independent ConfigSms over their OWN empty_self maps, fed the IDENTICAL committed
    /// sequence, converge to the same ownership projection (the linearizable-ownership
    /// property the production wiring rests on).
    ///
    /// The committed sequence AddNodes EVERY node BEFORE referencing it (the ordering Raft's
    /// committed log guarantees): each node self-seeds its own id via `empty_self`, but a
    /// cross-reference (assigning a slot to a peer) needs that peer in the local table first,
    /// so both ids are AddNode'd up front. Convergence is asserted by the owner-ID projection
    /// per slot, NOT `ranges()` (whose node-INDEX is table-order, which differs per node since
    /// each lists itself first).
    #[test]
    fn identical_committed_sequence_converges_two_nodes() {
        let seq = [
            ConfigCmd::AddNode {
                id: ID0.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7000,
            },
            ConfigCmd::AddNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7001,
            },
            ConfigCmd::AssignSlots {
                node: ID0.to_owned(),
                slots: vec![0, 5, 100],
            },
            ConfigCmd::AssignSlots {
                node: ID1.to_owned(),
                slots: vec![1, 2, 3],
            },
        ];
        // Node 0's view (self == ID0) and node 1's view (self == ID1).
        let map0 = Arc::new(SlotMap::empty_self(ID0, "127.0.0.1", 7000));
        let map1 = Arc::new(SlotMap::empty_self(ID1, "127.0.0.1", 7001));
        let mut sm0 = ConfigSm::new(Arc::clone(&map0));
        let mut sm1 = ConfigSm::new(Arc::clone(&map1));
        for (i, cmd) in seq.iter().enumerate() {
            let idx = i as u64 + 1;
            sm0.apply(&config_entry(idx, cmd.clone()));
            sm1.apply(&config_entry(idx, cmd.clone()));
        }
        // Same log-driven epoch on both nodes.
        assert_eq!(map0.current_epoch(), map1.current_epoch());
        // Identical owner projection per assigned slot (the index-independent witness of
        // convergence): both nodes agree slot s resolves to the same advertised owner
        // endpoint. `moved_target` returns the owner's `(host, port)`, and the two ids have
        // distinct ports (7000 / 7001), so the endpoint uniquely identifies the owner.
        for slot in [0u16, 1, 2, 3, 5, 100] {
            assert_eq!(
                map0.moved_target(slot),
                map1.moved_target(slot),
                "nodes disagree on the owner of slot {slot}"
            );
        }
        // And node-relative ownership is correct: ID0 owns 0/5/100, ID1 owns 1/2/3.
        assert!(map0.owns(0) && map0.owns(5) && map0.owns(100));
        assert!(map1.owns(1) && map1.owns(2) && map1.owns(3));
        assert!(!map0.owns(1) && !map1.owns(0));
    }

    /// HA-7d: a committed AssignReplica records the named node as the slot's REPLICA in the
    /// parallel structure (NOT the owner bitmap, so owns() is unchanged), bumps the log-driven
    /// epoch once, and is idempotent on re-apply.
    #[test]
    fn assign_replica_populates_replicas_bumps_epoch_and_is_idempotent() {
        let map = Arc::new(SlotMap::empty_self(ID0, "127.0.0.1", 7000));
        let mut sm = ConfigSm::new(Arc::clone(&map));
        // ID0 owns [0,1]; ID1 (a known peer) replicates them.
        sm.apply(&config_entry(
            1,
            ConfigCmd::AddNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7001,
            },
        ));
        sm.apply(&config_entry(
            2,
            ConfigCmd::AssignSlots {
                node: ID0.to_owned(),
                slots: vec![0, 1],
            },
        ));
        let epoch_before = sm.config_epoch();
        sm.apply(&config_entry(
            3,
            ConfigCmd::AssignReplica {
                node: ID1.to_owned(),
                slots: vec![0, 1],
            },
        ));
        // The replica is recorded; ownership (owns()) is untouched.
        assert!(map.is_replica_of(0, ID1) && map.is_replica_of(1, ID1));
        assert!(
            map.owns(0) && map.owns(1),
            "AssignReplica must not change owns()"
        );
        assert!(
            !map.is_replica_of(0, ID0),
            "the owner is not recorded as a replica"
        );
        // Exactly one epoch bump for the one AssignReplica entry.
        assert_eq!(sm.config_epoch(), epoch_before + 1);
        assert_eq!(map.current_epoch(), sm.config_epoch());

        // Idempotent re-apply: same replica, same epoch +1 per entry (deterministic), no change to
        // the projection beyond the bump.
        sm.apply(&config_entry(
            4,
            ConfigCmd::AssignReplica {
                node: ID1.to_owned(),
                slots: vec![0, 1],
            },
        ));
        assert!(map.is_replica_of(0, ID1) && map.is_replica_of(1, ID1));
        assert_eq!(sm.config_epoch(), epoch_before + 2);
    }

    /// HA-8 FAILOVER: a committed PromoteReplica flips each slot's OWNER to `new_primary`, CLEARS
    /// `new_primary` from the slot's replica set, bumps the log-driven epoch once, keeps `mine[]`
    /// in lockstep ON THE NEW OWNER (owns() true), and is idempotent on re-apply.
    #[test]
    fn promote_replica_flips_owner_clears_replica_bumps_epoch_and_is_idempotent() {
        // Node ID1's view (self == ID1): ID0 owns [0,1]; ID1 replicates them; then ID1 is promoted.
        let map = Arc::new(SlotMap::empty_self(ID1, "127.0.0.1", 7001));
        let mut sm = ConfigSm::new(Arc::clone(&map));
        sm.apply(&config_entry(
            1,
            ConfigCmd::AddNode {
                id: ID0.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7000,
            },
        ));
        sm.apply(&config_entry(
            2,
            ConfigCmd::AssignSlots {
                node: ID0.to_owned(),
                slots: vec![0, 1],
            },
        ));
        sm.apply(&config_entry(
            3,
            ConfigCmd::AssignReplica {
                node: ID1.to_owned(),
                slots: vec![0, 1],
            },
        ));
        // Pre-promotion: ID0 owns (so ID1 does not), ID1 is the replica.
        assert!(
            !map.owns(0) && !map.owns(1),
            "ID0 owns pre-promotion, not self (ID1)"
        );
        assert!(map.is_replica_of(0, ID1) && map.is_replica_of(1, ID1));
        let epoch_before = sm.config_epoch();
        let map_epoch_before = map.current_epoch();

        // PROMOTE ID1 (self) to owner of [0,1].
        sm.apply(&config_entry(
            4,
            ConfigCmd::PromoteReplica {
                slots: vec![0, 1],
                new_primary: ID1.to_owned(),
            },
        ));
        // The new owner is ID1 (self): owns() flips TRUE (mine[] in lockstep), replica cleared.
        assert!(
            map.owns(0) && map.owns(1),
            "the promoted node now owns() the slots"
        );
        assert!(
            !map.is_replica_of(0, ID1) && !map.is_replica_of(1, ID1),
            "the promoted node is cleared from the replica set"
        );
        // Exactly one epoch bump for the one PromoteReplica entry (the split-brain fence: a stale
        // client sees the advanced epoch in its MOVED).
        assert_eq!(sm.config_epoch(), epoch_before + 1);
        assert!(map.current_epoch() > map_epoch_before);
        assert_eq!(map.current_epoch(), sm.config_epoch());

        // Idempotent re-apply: same owner, replica stays clear, epoch advances +1 per entry.
        sm.apply(&config_entry(
            5,
            ConfigCmd::PromoteReplica {
                slots: vec![0, 1],
                new_primary: ID1.to_owned(),
            },
        ));
        assert!(map.owns(0) && map.owns(1));
        assert!(!map.is_replica_of(0, ID1) && !map.is_replica_of(1, ID1));
        assert_eq!(sm.config_epoch(), epoch_before + 2);
    }

    /// HA-8 THE SPLIT-BRAIN FENCE (apply-side): the OLD primary applying the SAME committed
    /// PromoteReplica LOSES ownership (owns() -> false), so once it catches its Raft log up it
    /// serves MOVED to the new owner -- there is no committed state with two owners.
    #[test]
    fn old_primary_loses_ownership_when_promotion_applies() {
        // Node ID0's view (self == ID0): ID0 OWNS [0,1] (the old primary); ID1 replicates them.
        let map = Arc::new(SlotMap::empty_self(ID0, "127.0.0.1", 7000));
        let mut sm = ConfigSm::new(Arc::clone(&map));
        sm.apply(&config_entry(
            1,
            ConfigCmd::AddNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7001,
            },
        ));
        sm.apply(&config_entry(
            2,
            ConfigCmd::AssignSlots {
                node: ID0.to_owned(),
                slots: vec![0, 1],
            },
        ));
        sm.apply(&config_entry(
            3,
            ConfigCmd::AssignReplica {
                node: ID1.to_owned(),
                slots: vec![0, 1],
            },
        ));
        // The old primary owns the slots before the promotion applies.
        assert!(map.owns(0) && map.owns(1), "ID0 is the owner pre-promotion");

        // The SAME committed PromoteReplica{ID1} applies here (the old primary's log catches up).
        sm.apply(&config_entry(
            4,
            ConfigCmd::PromoteReplica {
                slots: vec![0, 1],
                new_primary: ID1.to_owned(),
            },
        ));
        // THE FENCE: the old primary's owns() is now FALSE -> it serves MOVED to ID1, the new
        // owner. moved_target resolves to ID1's advertised endpoint (port 7001).
        assert!(
            !map.owns(0) && !map.owns(1),
            "the OLD primary loses ownership when the promotion applies (no two owners)"
        );
        assert_eq!(map.moved_target(0), Some(("127.0.0.1".to_owned(), 7001)));
        assert_eq!(map.moved_target(1), Some(("127.0.0.1".to_owned(), 7001)));
    }

    /// HA-8: two independent ConfigSms (the new owner's view and the old owner's view) fed the
    /// IDENTICAL committed sequence including a PromoteReplica converge so that EXACTLY ONE node
    /// owns each slot -- the linearizable-ownership property across a failover.
    #[test]
    fn promote_replica_converges_to_one_owner_across_nodes() {
        let seq = [
            ConfigCmd::AddNode {
                id: ID0.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7000,
            },
            ConfigCmd::AddNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7001,
            },
            ConfigCmd::AssignSlots {
                node: ID0.to_owned(),
                slots: vec![10, 20],
            },
            ConfigCmd::AssignReplica {
                node: ID1.to_owned(),
                slots: vec![10, 20],
            },
            ConfigCmd::PromoteReplica {
                slots: vec![10, 20],
                new_primary: ID1.to_owned(),
            },
        ];
        let map0 = Arc::new(SlotMap::empty_self(ID0, "127.0.0.1", 7000));
        let map1 = Arc::new(SlotMap::empty_self(ID1, "127.0.0.1", 7001));
        let mut sm0 = ConfigSm::new(Arc::clone(&map0));
        let mut sm1 = ConfigSm::new(Arc::clone(&map1));
        for (i, cmd) in seq.iter().enumerate() {
            let idx = i as u64 + 1;
            sm0.apply(&config_entry(idx, cmd.clone()));
            sm1.apply(&config_entry(idx, cmd.clone()));
        }
        // Same log-driven epoch on both nodes.
        assert_eq!(map0.current_epoch(), map1.current_epoch());
        for slot in [10u16, 20] {
            // The new owner (ID1) owns; the old owner (ID0) does NOT -> exactly one owner.
            assert!(!map0.owns(slot), "old owner ID0 lost the slot {slot}");
            assert!(map1.owns(slot), "new owner ID1 owns the slot {slot}");
            // Both nodes resolve MOVED / ownership to the SAME endpoint (ID1, port 7001).
            assert_eq!(
                map0.moved_target(slot),
                map1.moved_target(slot),
                "nodes disagree on the owner of slot {slot} after failover"
            );
            assert_eq!(
                map0.moved_target(slot),
                Some(("127.0.0.1".to_owned(), 7001))
            );
            // The promoted node is the replica of NEITHER slot anymore (it is the owner).
            assert!(!map0.is_replica_of(slot, ID1) && !map1.is_replica_of(slot, ID1));
        }
    }

    /// HA-6: SetSlotMigrating / SetSlotImporting / SetSlotStable drive the parallel migration
    /// arrays (NOT owns()), each bumps the log-driven epoch once, and SetSlotStable / the FLIP clear
    /// the state. The SOURCE owns the slot throughout MIGRATING; the committed FLIP (SetSlotOwner)
    /// transfers ownership AND clears the migration in one apply.
    #[test]
    fn migration_setslot_drives_state_without_touching_owns_and_flip_clears_it() {
        use ironcache_cluster::MigrationState;
        // Node ID0's view (self == ID0): ID0 OWNS [0]; ID1 is the migration DEST.
        let map = Arc::new(SlotMap::empty_self(ID0, "127.0.0.1", 7000));
        let mut sm = ConfigSm::new(Arc::clone(&map));
        sm.apply(&config_entry(
            1,
            ConfigCmd::AddNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7001,
            },
        ));
        sm.apply(&config_entry(
            2,
            ConfigCmd::AssignSlots {
                node: ID0.to_owned(),
                slots: vec![0],
            },
        ));
        assert!(map.owns(0));
        assert_eq!(map.migration_state(0), MigrationState::None);
        let epoch_before = sm.config_epoch();

        // MIGRATING 0 -> ID1: tagged MIGRATING, but ID0 STILL OWNS (owns() unchanged); epoch +1.
        sm.apply(&config_entry(
            3,
            ConfigCmd::SetSlotMigrating {
                slot: 0,
                dest: ID1.to_owned(),
            },
        ));
        assert_eq!(map.migration_state(0), MigrationState::Migrating);
        assert!(map.owns(0), "MIGRATING must not change owns()");
        assert_eq!(
            map.migration_peer_endpoint(0),
            Some(("127.0.0.1".to_owned(), 7001))
        );
        assert_eq!(sm.config_epoch(), epoch_before + 1);

        // THE FLIP (SetSlotOwner 0 -> ID1): ownership transfers AND migration clears in one apply.
        sm.apply(&config_entry(
            4,
            ConfigCmd::SetSlotOwner {
                slot: 0,
                node: ID1.to_owned(),
            },
        ));
        assert!(
            !map.owns(0),
            "the FLIP transfers ownership away from the source"
        );
        assert_eq!(
            map.migration_state(0),
            MigrationState::None,
            "the committed FLIP clears the migration state (no stale ASK)"
        );
        assert_eq!(sm.config_epoch(), epoch_before + 2);

        // SetSlotStable on a fresh slot (the abort path): IMPORTING then STABLE clears. This sm's
        // self is ID0, so dest = ID0 makes THIS node the importer (Finding 2: only the dest tags).
        sm.apply(&config_entry(
            5,
            ConfigCmd::SetSlotImporting {
                slot: 5,
                src: ID1.to_owned(),
                dest: ID0.to_owned(),
            },
        ));
        assert_eq!(map.migration_state(5), MigrationState::Importing);
        sm.apply(&config_entry(6, ConfigCmd::SetSlotStable { slot: 5 }));
        assert_eq!(map.migration_state(5), MigrationState::None);
        assert_eq!(sm.config_epoch(), epoch_before + 4);
    }

    /// HA-6: two independent ConfigSms (the SOURCE's view and the DEST's view) fed the IDENTICAL
    /// committed migration+FLIP sequence converge so EXACTLY ONE node owns the slot and NEITHER has
    /// a lingering migration tag -- the linearizable-ownership property across a slot migration.
    #[test]
    fn migration_then_flip_converges_to_one_owner_across_nodes() {
        use ironcache_cluster::MigrationState;
        let seq = [
            ConfigCmd::AddNode {
                id: ID0.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7000,
            },
            ConfigCmd::AddNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7001,
            },
            ConfigCmd::AssignSlots {
                node: ID0.to_owned(),
                slots: vec![42],
            },
            // SOURCE (ID0) MIGRATING 42 -> DEST (ID1).
            ConfigCmd::SetSlotMigrating {
                slot: 42,
                dest: ID1.to_owned(),
            },
            // DEST (ID1) IMPORTING 42 from SOURCE (ID0): dest = ID1, so ONLY map1 (self == ID1)
            // tags IMPORTING; map0 (the source) does not (Finding 2: only the dest is tagged).
            ConfigCmd::SetSlotImporting {
                slot: 42,
                src: ID0.to_owned(),
                dest: ID1.to_owned(),
            },
            // THE FLIP: ownership transfers to ID1 (clears migration on both nodes' apply).
            ConfigCmd::SetSlotOwner {
                slot: 42,
                node: ID1.to_owned(),
            },
        ];
        let map0 = Arc::new(SlotMap::empty_self(ID0, "127.0.0.1", 7000));
        let map1 = Arc::new(SlotMap::empty_self(ID1, "127.0.0.1", 7001));
        let mut sm0 = ConfigSm::new(Arc::clone(&map0));
        let mut sm1 = ConfigSm::new(Arc::clone(&map1));
        for (i, cmd) in seq.iter().enumerate() {
            let idx = i as u64 + 1;
            sm0.apply(&config_entry(idx, cmd.clone()));
            sm1.apply(&config_entry(idx, cmd.clone()));
        }
        assert_eq!(map0.current_epoch(), map1.current_epoch());
        // EXACTLY ONE owner (the new owner ID1); the old owner ID0 lost it.
        assert!(!map0.owns(42), "old owner ID0 lost the slot after the FLIP");
        assert!(map1.owns(42), "new owner ID1 owns the slot after the FLIP");
        // No lingering migration tag on EITHER node (the FLIP cleared it on both).
        assert_eq!(map0.migration_state(42), MigrationState::None);
        assert_eq!(map1.migration_state(42), MigrationState::None);
        // Both nodes resolve MOVED to the SAME endpoint (ID1, port 7001) for a stale client.
        assert_eq!(map0.moved_target(42), map1.moved_target(42));
        assert_eq!(map0.moved_target(42), Some(("127.0.0.1".to_owned(), 7001)));
    }

    /// HA-6 Finding 2: in an N>=3 cluster, `SetSlotImporting { src, dest }` tags IMPORTING on EXACTLY
    /// the `dest` node and on NO bystander. SOURCE = ID0 (owner), DEST = ID1, BYSTANDER = ID2 (a third
    /// node that owns nothing here and is NOT the dest). All three apply the SAME committed sequence.
    /// Only the DEST records IMPORTING; the bystander (also a non-owner, which the old `!owns()` gate
    /// wrongly tagged) records NONE. The source keeps its MIGRATING tag.
    #[test]
    fn set_slot_importing_tags_only_the_dest_not_a_bystander() {
        use ironcache_cluster::MigrationState;
        let seq = [
            ConfigCmd::AddNode {
                id: ID0.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7000,
            },
            ConfigCmd::AddNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7001,
            },
            ConfigCmd::AddNode {
                id: ID2.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7002,
            },
            // SOURCE (ID0) owns the slot.
            ConfigCmd::AssignSlots {
                node: ID0.to_owned(),
                slots: vec![7],
            },
            // SOURCE MIGRATING toward DEST (ID1).
            ConfigCmd::SetSlotMigrating {
                slot: 7,
                dest: ID1.to_owned(),
            },
            // DEST IMPORTING from SOURCE: dest == ID1. ONLY ID1 must tag IMPORTING; ID2 (a bystander
            // non-owner) must NOT, even though `!owns(7)` is true for it.
            ConfigCmd::SetSlotImporting {
                slot: 7,
                src: ID0.to_owned(),
                dest: ID1.to_owned(),
            },
        ];
        // One ConfigSm per node, each with its OWN self identity, all fed the identical committed log.
        let map_src = Arc::new(SlotMap::empty_self(ID0, "127.0.0.1", 7000));
        let map_dest = Arc::new(SlotMap::empty_self(ID1, "127.0.0.1", 7001));
        let map_bystander = Arc::new(SlotMap::empty_self(ID2, "127.0.0.1", 7002));
        let mut sm_src = ConfigSm::new(Arc::clone(&map_src));
        let mut sm_dest = ConfigSm::new(Arc::clone(&map_dest));
        let mut sm_bystander = ConfigSm::new(Arc::clone(&map_bystander));
        for (i, cmd) in seq.iter().enumerate() {
            let idx = i as u64 + 1;
            sm_src.apply(&config_entry(idx, cmd.clone()));
            sm_dest.apply(&config_entry(idx, cmd.clone()));
            sm_bystander.apply(&config_entry(idx, cmd.clone()));
        }
        // SOURCE: still owns + tagged MIGRATING (it is the source, gated on owns()).
        assert!(
            map_src.owns(7),
            "SOURCE still owns the slot during migration"
        );
        assert_eq!(
            map_src.migration_state(7),
            MigrationState::Migrating,
            "SOURCE is tagged MIGRATING"
        );
        // DEST: the ONE importer.
        assert!(!map_dest.owns(7), "DEST does not own the slot yet");
        assert_eq!(
            map_dest.migration_state(7),
            MigrationState::Importing,
            "DEST is tagged IMPORTING (it is the named dest)"
        );
        // BYSTANDER: a non-owner that is NOT the dest -> NO migration tag (the Finding 2 fix).
        assert!(!map_bystander.owns(7), "the bystander owns nothing here");
        assert_eq!(
            map_bystander.migration_state(7),
            MigrationState::None,
            "the bystander (a non-owner non-dest) must NOT be tagged IMPORTING"
        );
    }

    /// HA-7d: two independent ConfigSms fed the IDENTICAL committed sequence (including an
    /// AssignReplica) converge to the same (owner, replica) projection on both nodes -- the
    /// linearizable-ownership property extended to the replica leg.
    #[test]
    fn assign_replica_converges_two_nodes() {
        let seq = [
            ConfigCmd::AddNode {
                id: ID0.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7000,
            },
            ConfigCmd::AddNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 7001,
            },
            ConfigCmd::AssignSlots {
                node: ID0.to_owned(),
                slots: vec![0, 5, 100],
            },
            ConfigCmd::AssignReplica {
                node: ID1.to_owned(),
                slots: vec![0, 5, 100],
            },
        ];
        let map0 = Arc::new(SlotMap::empty_self(ID0, "127.0.0.1", 7000));
        let map1 = Arc::new(SlotMap::empty_self(ID1, "127.0.0.1", 7001));
        let mut sm0 = ConfigSm::new(Arc::clone(&map0));
        let mut sm1 = ConfigSm::new(Arc::clone(&map1));
        for (i, cmd) in seq.iter().enumerate() {
            let idx = i as u64 + 1;
            sm0.apply(&config_entry(idx, cmd.clone()));
            sm1.apply(&config_entry(idx, cmd.clone()));
        }
        assert_eq!(map0.current_epoch(), map1.current_epoch());
        // Both nodes agree: ID0 owns the slots, ID1 replicates them (the id-based projection is
        // index-independent, so it converges regardless of per-node table order).
        for slot in [0u16, 5, 100] {
            assert!(map0.owns(slot), "node0 view: ID0 owns {slot}");
            assert!(!map1.owns(slot), "node1 view: ID0 (not self) owns {slot}");
            assert!(
                map0.is_replica_of(slot, ID1) && map1.is_replica_of(slot, ID1),
                "both nodes agree ID1 replicates slot {slot}"
            );
        }
    }
}
