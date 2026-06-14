# Design: Cluster bootstrap and node lifecycle (seed/MEET join, learner-to-voter-to-slot-owner)

Issue: #149. Decisions: ADR-0026 (async primary/replica default, replica
guardrails). Related: #73 (CONTROL_PLANE: Raft slot map and config epoch), #74
(MEMBERSHIP: SWIM plus Lifeguard), #69 (single-node-first staged path), #75
(migration), #1 (vision).

## Goal and scope

The roster owns steady-state membership (#74 SWIM), the authoritative map (#73
Raft), and migration (#75), but nothing owns how a node enters or leaves the
cluster. This spec owns the lifecycle: cold-start seed discovery and a CLUSTER
MEET-equivalent handshake, the staged promotion of a joining node from
SWIM-discovered to Raft learner to voter to slot owner, the operator/CLI
add-node and remove-node surface, and the single-node-to-first-replica
bootstrap that #69's staged path assumes. Scope is the transitions between
states; the SWIM signal itself (#74), the Raft commit semantics (#73), and the
slot-migration mechanics (#75) are owned elsewhere and only invoked here.

## Design

### Seed and MEET join

- A new node boots with a seed list (operator-supplied or CLI add-node). It
  contacts a seed, which performs a MEET-equivalent handshake: the seed
  introduces the joiner into the SWIM membership view (#74) so the rest of the
  ring learns of it through normal gossip, no full-mesh fan-out.
- SWIM membership is a hint, not authority: a node SWIM has discovered is not
  yet part of the cluster's committed state. Per #74's contract, SWIM proposes
  and Raft commits, so MEET only makes a node a candidate for promotion.

### Learner to voter to slot-owner promotion

- A SWIM-discovered node is first admitted to the Raft control plane (#73) as a
  non-voting learner: it receives committed slot-map deltas and config-epoch
  updates but does not vote, so it cannot affect commit latency or quorum while
  it catches up [raft-overview].
- A learner is promoted to voter only by an explicit committed control-plane
  decision (#73), keeping the voter set small (the #73 3-to-5 voter group) and
  the slot map linearizable. Voter promotion is a control-plane role change, not
  a data assignment.
- Becoming a slot owner is the last and separate step: the control plane assigns
  slots and, for a replica being promoted toward ownership, applies a
  replication-lag gate before the node is eligible, since replication is async
  (ADR-0026). Replica handoff reuses PSYNC2-style secondary-replid resync so
  promotion does not force a full resync [redis-psync2-secondary-replid]. Slot
  movement itself runs through migration (#75) under MOVED/ASK.

### Add/remove-node operator surface

- add-node (CLI/operator) supplies a seed and triggers the MEET handshake, then
  the staged learner-to-voter-to-owner path above; the operator observes each
  stage via CLUSTER SHARDS health/role (#74) rather than poking internal state.
- remove-node is the reverse and drains first: slots are migrated off (#75),
  the node is demoted from voter to learner to leave the quorum cleanly, then
  removed from the committed membership (#73) and finally from the SWIM view
  (#74). A node is never removed from the map while it still owns a slot.

### Single-node to first-replica bootstrap

- A single node boots as a degenerate one-voter control plane owning all 16384
  slots, consistent with #69's single-node-first staged layout. The first
  replica joins by the same seed/MEET path, enters as a learner, and attaches as
  an async replica of the primary under ADR-0026 (replica-read-only on,
  min-replicas guardrails inactive at one replica). This is the transition #69
  assumes but does not specify: the inter-stage step from standalone to a
  primary-with-replica pair.

## Open questions

- The replication-lag threshold that gates a replica from learner to
  slot-owner-eligible (ties to ADR-0026 min-replicas-max-lag and #73's
  promotion-policy open decision).
- Whether learner admission to Raft is automatic on SWIM discovery or requires
  an explicit operator add-node (#73 lists data-nodes-as-learners as open).
- Seed-list bootstrapping when all seeds are down, and how MEET interacts with a
  partitioned SWIM view.

## Acceptance and test hooks

- A node added by seed/MEET appears as a SWIM hint, then a Raft learner, then a
  voter, then a slot owner, with each stage visible in CLUSTER SHARDS and never
  skipped.
- remove-node drains all slots (#75) and demotes through learner before the
  node leaves committed membership; no slot is orphaned.
- A standalone node accepts a first replica via MEET and reaches a
  primary-with-async-replica pair per #69 and ADR-0026.

## References

- ADR-0026; issues #149, #73, #74, #69, #75, #1; specs CONTROL_PLANE (#73),
  MEMBERSHIP (#74).
- Claims: [raft-overview], [redis-psync2-secondary-replid].
