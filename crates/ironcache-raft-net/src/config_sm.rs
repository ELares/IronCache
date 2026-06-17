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
}
