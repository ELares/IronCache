# Design: Raft control plane for the authoritative slot map

Issue: #73. Decisions: ADR-0025 (cluster partition map and slot ownership),
ADR-0011 (single-node-first, slot-ready layout), ADR-0012 (scale-out targets),
ADR-0002 (shared-nothing; per-shard slots are the migration and execution unit).
Related: #74/MEMBERSHIP.md (the SWIM signal this commits over), #70 (client
cluster contract), #75 (slot migration), #68 (clustering umbrella).

## Goal and scope

One authoritative answer to "which node owns slot N right now, and at what config
epoch." The Redis-compatible baseline gossips a per-node served-slots bitmap on a
cluster bus at base_port+10000 [redis-cluster-bus-port], which has no linearizable
arbiter and whose bitmap size caps the design near 1000 masters
[redis-cluster-why-16384]. This folds the slot map, config epoch, membership
roster, and replica promotion into a small in-binary Raft group; config changes
flow only through Raft. Scope is the control plane: the slot->node map, epoch,
membership commit, and promotion. Out of scope: the partition layout itself
(ADR-0025), the data-plane failure signal (#74), and per-slot data migration
mechanics (#75).

## Design

### Raft group and the replicated state machine

- A single Raft group of 3 to 5 voters owns the authoritative state; Raft is the
  multi-Paxos-equivalent decomposition into leader election, log replication, and
  safety [raft-overview]. The replicated state machine is exactly the config: the
  slot->node map of ADR-0025, the monotonic config epoch, the membership roster,
  and replica role assignments. No user data ever passes through the log, which
  keeps the blast radius config-sized and the log small enough to snapshot cheaply.
- Data nodes are non-voting learners: they receive committed map deltas and apply
  them but do not vote, so commit latency stays bounded by the 3-to-5 voters even
  at the several-thousand-node target of ADR-0012. The voter set is itself a
  committed entry, so growing or shrinking it is an ordinary config change.

### Config epoch and committed-map projection

- Every committed change bumps the config epoch monotonically. CLUSTER SLOTS and
  CLUSTER SHARDS are pure projections of the committed map at the current epoch
  (#70), never of in-flight log entries, so a client never observes a slot owner
  that Raft has not committed. The epoch is the tie-breaker for stale clients: a
  MOVED carries the destination at the committing epoch.

### Membership and promotion go only through Raft

- Adding, removing, or promoting a node is a Raft proposal, not a gossip event.
  This replaces the eventually-consistent, "last failover wins" posture of an
  external Sentinel quorum [redis-sentinel-quorum-vs-majority]: one quorum, one
  source of truth, and no second arbiter to disagree. A replica is eligible for
  promotion only when the leader records its replication offset as within a
  configured lag bound; promotion is a committed entry that flips the role and
  bumps the epoch atomically.

### The SWIM-proposes / Raft-commits handshake (with #74)

- The data-plane membership layer (#74) is a fast but unauthoritative suspicion
  source. A SWIM suspicion or confirmation is a *hint*: the Raft leader treats it
  as a proposal input, never as a commit. Only a committed Raft entry changes the
  authoritative roster or a slot owner. This is the contract MEMBERSHIP.md
  specifies from the membership side; here it means the leader is the sole writer
  and a healthy node cannot be evicted by gossip alone, only by a quorum commit.
- The committed map is therefore monotonic under transient suspicion: a node that
  SWIM marks suspect is demoted in the client-facing projection only after the
  leader commits the demotion, so CLUSTER SLOTS never regresses on a flap.

### Correctness bar

- The acceptance target is the Jepsen/Elle suite covering all 21 Redis-Raft
  failure classes (split-brain, lost updates, stale/aborted reads, total data
  loss on failover or membership change) [jepsen-redis-raft-21-issues]. Keeping
  user data out of the log shrinks the surface, but the config plane must still
  clear every class under partitions, pauses, clock skew, and membership churn.

## Open questions

- Fixed 3 voters or 5: higher fault tolerance versus commit latency under the
  ADR-0012 failover budget.
- Learner delivery: Raft learners on the same transport, or an out-of-band poll
  of committed map deltas to keep voter fan-out small.
- The node-count or commit-latency threshold that forces sharded/hierarchical
  multi-Raft instead of one group.
- Whether the cluster-bus port stays at base+10000 for compatibility once the
  gossip slot bitmap is gone [redis-cluster-bus-port].

## Acceptance and test hooks

- Slot ownership is linearizable: a committed map at epoch E is never contradicted
  by any node; a DST/Jepsen history shows no two nodes serving the same slot as
  owner at the same epoch.
- No gossip-propagated slot bitmap exists; internal node count is not bitmap-capped.
- Failover and promotion run in-binary with no Sentinel process
  [redis-sentinel-quorum-vs-majority]; promotion respects the lag gate.
- CLUSTER SLOTS/SHARDS render from the committed map and never regress under a
  transient SWIM suspicion (joint hook with #74).
- The Jepsen/Elle suite passes all 21 Redis-Raft failure classes
  [jepsen-redis-raft-21-issues] (#99).

## References

- ADR-0025, ADR-0011, ADR-0012, ADR-0002; issues #73, #74, #70, #75, #68, #99.
- Claims: [raft-overview], [redis-cluster-bus-port], [redis-cluster-why-16384],
  [redis-sentinel-quorum-vs-majority], [jepsen-redis-raft-21-issues].
