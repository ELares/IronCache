# Experiment: Per-shard Raft for an opt-in strongly-consistent tier

Issue: #78. Provisional decision: ADR-0026 pins strong consistency as opt-in,
never a tax on every write. CONTROL_PLANE.md (#73) pins a single 3-to-5-voter
Raft group for the CONTROL plane (slot map and membership) with data nodes as
non-voting learners; per-shard DATA-plane Raft is a direction #78 proposes, not
something CONTROL_PLANE.md pins, and this bake-off must confirm it. This doc
records the experiment that quantifies the write-latency and throughput cost of
that opt-in tier, gated on the #99 Jepsen/Elle suite before it can ship. It does
not re-decide the async default.

## Provisional decision (already pinned)

ADR-0026 (Accepted, issue #76) commits IronCache to asynchronous primary/replica
as the fast default and states that strong consistency is delivered through an
opt-in quorum/Raft tier (#78, #12), layered on the async baseline, never by
changing the default. The async default pays no quorum round trip
[redis-cluster-async-replication] and exposes WAIT only as a best-effort
durability floor, which the Redis docs are explicit can still lose a write
synchronously replicated to multiple replicas [redis-wait-since-and-caveat]. That
best-effort-versus-quorum-committed gap is exactly the differentiator this tier
reserves.

CONTROL_PLANE.md (#73) pins a single small Raft group of 3 to 5 voters for the
slot map and membership, kept off the data hot path, with data nodes as
non-voting learners [raft-overview]. That is a CONTROL-plane decision. This
experiment is about the DATA plane, which CONTROL_PLANE.md does not decide:
#78 proposes per-shard Raft (one group per slot range) so writes to independent
keys ride independent leaders and logs, and throughput scales with shard count
and cores rather than collapsing to one log. The direction this experiment must
confirm is to borrow Raft as the consensus core but run it per-shard on the data
plane, and to reject single-group data-plane Raft, which forfeits per-core
throughput by serializing all writes through one log.

The pinned correctness bar is non-negotiable: the 2020 Jepsen analysis of
Redis-Raft found 21 distinct issues, including split-brain with lost updates,
stale reads, and total data loss on failover [jepsen-redis-raft-21-issues], so
this tier rejects build-our-own-Raft and mandates a verified library plus a
passing #99 Elle/Jepsen gate before it ships.

## Why this is harness-blocked

The decision rule needs the measured p50/p99 write-latency and throughput delta
of per-shard Raft versus the async default, with and without log batching and
pipelining. That requires three things that do not exist yet:

- A verified Raft library wired per-shard behind the same write path as the async
  default, so only the replication mode varies.
- The benchmark harness and methodology of ADR-0016 (per-core throughput,
  open-loop tail latency); the harness is #8.
- The #99 Jepsen/Elle quorum suite, so the latency numbers are only ever read for
  a configuration that has cleared the 21 Redis-Raft failure classes
  [jepsen-redis-raft-21-issues]; an un-gated latency win is not a shippable
  result.

Until the harness runs per-shard Raft against the async baseline under one
accounting model AND the #99 gate is green, any latency claim is a number without
a correctness backing, which is the precise posture #99 commits against.

## Experiment to run

Corpus and workload:

- A register workload (SET/GET/INCR per key) and a list-append workload
  (RPUSH/LRANGE), the two workloads JEPSEN_PLAN.md (#99) drives, so the latency
  bake-off and the correctness gate exercise the same surface.
- A WAIT-equivalent on the quorum tier that means true quorum commit, adapted
  from the async best-effort WAIT [redis-wait-since-and-caveat], so existing
  clients get a real guarantee through a familiar verb.
- Writes spread across many independent shards, so per-shard Raft's
  throughput-scales-with-shard-count claim is exercised, not asserted.

Fixed parameters, held identical across async and Raft runs:

- The single-node engine, shard count, thread pinning, and the shared-nothing
  shard layout (ADR-0002), so the only variable is the replication mode.
- The ADR-0016 measurement methodology (open-loop, coordinated-omission-
  corrected), so the tail-latency numbers are honest.
- The durability setting (fsync policy) per run, so fsync-per-commit is isolated
  as its own swept variable rather than confounded with the quorum round trip.
- The fault-free baseline for the latency numbers, with correctness measured
  separately under the #99 fault catalog.

Varied parameters:

- Replication mode: async default [redis-cluster-async-replication] versus
  per-shard Raft [raft-overview], with single-group data-plane Raft included only
  as the rejected reference point to show the one-log throughput ceiling.
- Log batching: writes per Raft log entry, swept from one-per-entry upward.
- Pipeline depth: number of in-flight unacknowledged append batches.
- Per-node Raft group density: number of independent per-shard groups hosted on
  one node, swept until scheduling and fsync contention erodes per-core
  throughput.
- fsync policy: fsync-per-commit versus group commit across shards on shared
  storage.

Measured:

- p50 and p99 write latency and throughput for async versus per-shard Raft, the
  headline delta #78 asks for.
- The batch size and pipeline depth at which per-shard Raft recovers async-class
  throughput, the lever #78 names as the one that may hide most of the latency
  delta.
- The per-node group-count ceiling: how many independent per-shard Raft groups
  one node hosts before scheduling/fsync contention erodes per-core throughput,
  and how that interacts with the #73 config group.
- Whether fsync-per-commit dominates the latency, and whether group commit across
  shards on shared storage is both safe and worthwhile.

Decision rule:

- Recommend shipping the opt-in per-shard Raft tier only if (a) the #99
  Jepsen/Elle quorum suite clears all 21 Redis-Raft failure classes
  [jepsen-redis-raft-21-issues] for the chosen library and configuration, AND
  (b) at a batch size and pipeline depth that hold the WAIT-equivalent quorum
  guarantee, the per-shard tier recovers throughput close to the async default
  while keeping its p99 write-latency delta within an acceptable band.
- Reject single-group data-plane Raft regardless of its latency, because it
  serializes all writes through one log and forfeits the per-core throughput
  crown [raft-overview].
- If no batch/pipeline configuration recovers acceptable throughput, the tier
  stays unshipped and the async default (ADR-0026) remains the only model, with
  WAIT documented as best-effort [redis-wait-since-and-caveat].

## What would change the decision

- Log batching and pipelining recover async-class throughput at a batch size and
  pipeline depth that still satisfy the WAIT-equivalent quorum guarantee, making
  the latency cost of strong consistency small enough to ship as opt-in.
- The per-node Raft-group-density ceiling turns out high enough that one node
  hosts the target shard count without fsync/scheduling contention collapsing
  per-core throughput.
- Group commit across shards on shared storage is proven safe and recovers most
  of the fsync-per-commit cost, removing the dominant latency term.
- Conversely, if the verified Raft library cannot clear the 21 Redis-Raft
  failure classes under the #99 fault catalog [jepsen-redis-raft-21-issues], the
  tier does not ship at any latency, because an advertised guarantee it does not
  keep is worse than honest async.

## References

- ADR-0026 (async default plus WAIT; strong consistency is opt-in, never a
  default tax; issue #76); ADR-0002 (shared-nothing thread-per-core); ADR-0016
  (headline metrics and methodology, #7).
- docs/design/CONTROL_PLANE.md (#73, the single 3-to-5-voter control-plane Raft
  group this data-plane tier is distinct from); docs/design/JEPSEN_PLAN.md (#99,
  the quorum-suite gate); docs/design/CLUSTER_CONTRACT.md (#70, the
  WAIT-equivalent wire surface).
- Issues: #78 (this experiment); #76 (the async default it layers on); #73 (the
  control-plane Raft group); #99 (the consistency gate that must pass before this
  tier ships); #12 (the no-write-loss-in-async non-goal); #68 (clustering
  parent); #8 (benchmark harness); #1 (vision).
- Claims (resolved via docs/prior-art/claims.yaml): [raft-overview],
  [redis-cluster-async-replication], [redis-wait-since-and-caveat],
  [jepsen-redis-raft-21-issues].
