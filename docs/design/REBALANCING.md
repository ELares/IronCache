# Design: Rebalancing policy and orchestration

Issue: #148. Decisions: ADR-0012 (scale-out targets: per-partition rebalance
budget, slots-per-node, max-node count), ADR-0025 (the 16384-slot partition is
the single migration unit, no slot-to-partition translation). Related: #75
(MIGRATION: the atomic slot-migration mechanism this controller invokes), #80
(placement: the placement function that decides target ownership), #149
(NODE_LIFECYCLE: node add/remove and drain/decommission sequencing), #32
(hot-shard detection), #73 (CONTROL_PLANE: Raft slot map), #74 (MEMBERSHIP:
SWIM), #68 (clustering parent), #1 (vision).

## Goal and scope

#75 builds the atomic per-slot migration mechanism but explicitly scopes out
"rebalancing policy (which slots to move, when)", and #80 picks the placement
hash but not the controller that drives migration. This spec owns the
orchestration layer between them: the trigger that decides a rebalance is needed
(node add, node remove, and a hot-partition signal), the decision of which
partitions move and to where, the concurrency and throttle that keep many
simultaneous moves inside the ADR-0012 per-partition budget, and the drain
sequencing that node decommission (#149) depends on. The controller is a policy
layer only: it computes a migration plan and calls the #75 mechanism one slot at
a time; it never moves keys itself. Out of scope and owned elsewhere: the
migration state machine and cutover fence (#75), the placement function math
(#80), the SWIM signal and Raft commit (#74/#73), the node join/leave handshake
and learner/voter promotion (#149), and the per-core hot-key detection internals
(#32). Single-node deployments never rebalance: with one node owning all 16384
slots there is no target, so the controller is inert until a second node exists.

## Design

### Rebalance triggers

- The controller is event-driven, not periodic, so a stable cluster does no
  background slot churn. Three events arm it:
  - Node added: a new node reaches slot-owner eligibility in the lifecycle
    (#149), so the cluster is now under-spread and the new node owns zero slots.
  - Node removed: an operator remove-node (#149) marks a node draining, so its
    slots must be re-homed before it can leave committed membership.
  - Hot partition: the off-hot-path detector from #32 reports a partition whose
    load exceeds the imbalance threshold, and local reassignment to another
    execution shard on the same node (ADR-0025) is insufficient because the
    whole node is saturated, so the partition must move cross-node.
- Triggers coalesce. Overlapping add and remove events produce one plan over the
  post-event target ownership, not two competing plans, so a rolling replace
  (add then remove) does not move a slot twice.

### From trigger to migration decision

- The target ownership map is computed by the placement function selected in #80
  (MementoHash leading, HRW fallback), which is chosen precisely because it
  remaps the minimal fraction of slots on a single add or remove and supports
  arbitrary interior-node removal and per-node weighting
  [mementohash-vs-anchorhash]. The plan is the symmetric difference between the
  current Raft-committed slot map (#73) and the placement function's target map:
  exactly the set of partitions whose owner changes.
- For a balance trigger the plan moves only the minimally-remapped set so a join
  fills the new node and a leave empties the departing one without disturbing
  unrelated slots. For a hot-partition trigger the plan is a single partition
  moved to the least-loaded eligible node; because the 16384-slot partition is
  already the atomic migration unit (ADR-0025), a hot partition migrates whole,
  with no key splitting, and #32's hot-shard isolation feeds directly into a
  cross-node move here rather than dead-ending at the per-core boundary.
- The controller emits an ordered list of single-slot moves. Each move is one
  invocation of the #75 mechanism (snapshot, stream, Raft SETSLOT flip), so the
  controller inherits #75's crash-safety and exactly-one-owner guarantee per
  slot and never needs its own dual-ownership reasoning. Ownership flips remain
  client-correct because each completed move surfaces as MOVED and each in-flight
  move as ASK under the wire contract [redis-cluster-moved-ask].

### Concurrency and throttle against the ADR-0012 budget

- ADR-0012 sets a single-digit-second per-partition rebalance budget at a stated
  write rate, without a write freeze. The controller enforces this as an
  admission gate, not as Redis's migration-barrier knob: Redis throttles by
  gating how many replicas a primary keeps before a slot may move
  [redis-cluster-migration-barrier-default], whereas IronCache makes each
  individual move non-blocking by construction in #75 and limits only how many
  moves run at once.
- A bounded concurrency window N caps simultaneous in-flight migrations so their
  combined snapshot-stream bandwidth and the destination apply rate stay within
  the per-partition budget; the budget is per partition, so concurrency is capped
  by aggregate bandwidth rather than left unbounded. Moves beyond N queue. When
  #75 signals destination apply falling behind source writes (its open
  backpressure decision), the controller narrows N rather than letting any single
  move blow its budget, trading total rebalance wall-clock for a held
  per-partition guarantee.
- The controller spreads concurrent moves across distinct source and destination
  nodes where the plan allows, so no single node is both draining and absorbing
  at full bandwidth, which keeps steady-state traffic served during rebalance.
- A rebalance is interruptible and resumable: because each slot's state lives in
  the Raft slot map (#73), a controller or leader crash mid-plan leaves every
  not-yet-flipped slot owned by its source (#75 idempotent restart), and a new
  leader recomputes the remaining plan from the committed map. No move is lost
  and none is double-applied.

### Node drain and decommission

- remove-node (#149) puts a node in draining state. The controller computes the
  target map with that node weighted out of placement (the arbitrary-removal
  property [mementohash-vs-anchorhash]) and migrates every slot the node owns to
  the surviving owners, throttled by the same concurrency window.
- Drain is ordered before the lifecycle demotion: #149 demotes a node from voter
  to learner and then removes it from committed membership and SWIM, but only
  after the controller reports the node owns zero slots. The invariant, shared
  with #149, is that a node is never removed from the map while it still owns a
  slot, so decommission can never orphan a partition.
- A drain triggered by a removal and a concurrent hot-partition trigger on a
  surviving node coalesce into one target map, so draining does not fight
  hot-spot relief.

### Scale ceiling interaction

- At the ADR-0012 4096-node target each node owns only about four slots
  [redis-cluster-16384-slots], so one slot move is roughly a quarter of a node's
  data and the per-partition budget is the dominant cost; the controller's
  minimal-remap plan keeps the count of moves per topology change at its floor
  [mementohash-vs-anchorhash], which is what keeps rebalance feasible near the
  node ceiling that all-to-all gossip designs cannot reach
  [redis-cluster-max-nodes-recommendation][swim-scalability].

## Open questions

- The concurrency window N: a fixed cap, or adaptive to measured per-node
  snapshot/apply bandwidth headroom; ties to #75's backpressure open decision.
- The hot-partition threshold and dwell time before a cross-node move is armed,
  to avoid thrashing a partition between nodes under bursty load; shares the
  imbalance-ratio metric with #32.
- Whether a hot-partition move prefers the globally least-loaded node or the
  placement function's natural next owner, trading balance against future churn.
- Whether to expose a manual operator rebalance command and a dry-run plan
  preview, or keep rebalancing fully automatic from the three triggers.
- Drain-time admission: whether to slow accepting new writes to a draining node's
  slots to shorten drain, or keep them fully writable until each slot's cutover.

## Acceptance and test hooks

- Adding a node moves only the minimally-remapped set of partitions onto it and
  disturbs no unrelated slot, matched against the placement function's target
  map (#80) differentially.
- Removing a node drains every slot it owns to surviving owners and the node
  reaches zero owned slots before #149 demotes it from the voter set; no slot is
  ever orphaned across the drain.
- A reported hot partition (#32) on a saturated node migrates whole to a
  less-loaded node with no key splitting, and the imbalance ratio drops below
  threshold afterward.
- Concurrent migrations stay within the bounded window N, and each individual
  move completes inside the ADR-0012 per-partition budget at the stated write
  rate with no write freeze; raising offered write load narrows N rather than
  busting any single move's budget.
- A controller or Raft-leader crash mid-rebalance leaves exactly one owner per
  slot, and a new leader resumes the remaining plan from the committed slot map
  with no move lost or double-applied.
- Clients see MOVED for completed moves and ASK for in-flight ones throughout a
  rebalance, with no churny retry storm surfaced (matches the oracle, TESTING.md).
- A single-node cluster never initiates a rebalance.

## References

- ADR-0012, ADR-0025; issues #148, #75, #80, #149, #32, #73, #74, #68, #1;
  specs MIGRATION (#75), placement research (#80), NODE_LIFECYCLE (#149),
  CONTROL_PLANE (#73), MEMBERSHIP (#74), TESTING.md.
- Claims: [mementohash-vs-anchorhash], [redis-cluster-migration-barrier-default],
  [redis-cluster-moved-ask], [redis-cluster-16384-slots],
  [redis-cluster-max-nodes-recommendation], [swim-scalability].
