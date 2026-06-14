# ADR-0026: Default replication and consistency model (async primary/replica plus WAIT)

Status: Accepted
Issue: #76

## Context

IronCache must ship a single default replication and consistency model before
the opt-in tiers (#77, #78, #12) can be specified against a fixed baseline. The
default has to be correct under the failure modes operators actually hit and
stay drop-in compatible with how Redis clients already reason about durability.
The top two tenets, Compatible then Efficient, both bear on the choice: clients
should port over unchanged, and the hot write path should pay no quorum tax.
Scope here is the default model only; this ADR does not specify the streaming
protocol, replica handoff mechanics, or the read contract (#147 owns the
replica-read contract, #149 owns node lifecycle).

Redis Cluster replicates asynchronously between a primary and its replicas by
default, and exposes WAIT for callers that want bounded synchronous
acknowledgement [redis-cluster-async-replication]. WAIT confirms in-memory
replica receipt, not disk persistence, and the Redis docs are explicit that it
does not make the store strongly consistent: a write synchronously replicated
to several replicas can still be lost [redis-wait-not-cp]. The Jepsen analysis
reaches the same conclusion from the failure side, that default async
replication can drop acknowledged writes on failover [redis-wait-not-strongly-consistent].
The two strong-consistency alternatives are per-shard Raft or quorum writes,
which remove single-node-failover write loss at the cost of write latency and
operational weight on every write, and Dynamo-style leaderless quorums with a
sloppy quorum over the first N healthy nodes plus hinted handoff and
app-level conflict resolution [dynamo-quorum-sloppy-hinted].

## Decision

- **Default to asynchronous primary/replica replication, with WAIT exposed for
  bounded synchronous acks.** This is the Compatible and Efficient choice:
  clients that already speak Redis replication semantics and tooling port over
  unchanged, and the steady-state write path pays no quorum round-trip
  [redis-cluster-async-replication]. WAIT N timeout is offered as a per-command
  durability floor, not a consistency mode.
- **Document the default as best-effort, not CP, and name the loss window.**
  There is a write-loss window: a write acknowledged to the client but not yet
  replicated can be lost on primary failover or on the minority side of a
  partition. WAIT bounds this window but does not eliminate it, because it
  confirms in-memory receipt only and is not strong consistency
  [redis-wait-not-cp] [redis-wait-not-strongly-consistent]. This honesty is a
  shipped requirement, surfaced to clients per #147.
- **Ship three guardrail defaults.** `replica-read-only` is on, so replicas
  reject writes and cannot silently diverge [redis-replica-read-only-default].
  `min-replicas-to-write` is wired so a primary can stop accepting writes when
  too few replicas are in sync [redis-min-replicas-to-write-default].
  `min-replicas-max-lag` bounds how stale an in-sync replica may be before it
  stops counting toward that floor [redis-min-replicas-max-lag-default]. The
  shipped numeric defaults track the pinned upstream values in claims.yaml.
- **Strong consistency is opt-in, never a tax on every write.** No-acknowledged-
  write-loss on single-node failover is real value, but it is delivered through
  an opt-in quorum/Raft tier (#78, #12), layered on this async baseline, not by
  changing the default. Whether that becomes a headline differentiator is
  deferred to those issues; this ADR commits only to the async default.

## Rejected Alternatives

- **Per-shard Raft or quorum writes by default.** Removes acknowledged-write
  loss on single-node failover and gives a clean CP story. Rejected as the
  default: it adds write latency and operational weight to every write and
  diverges from Redis defaults, breaking Compatible, which ranks above the
  consistency gain. It survives as the opt-in tier in #78 and #12, layered on
  this baseline rather than replacing it.
- **Dynamo-style sloppy quorum with hinted handoff.** Stays writable during
  partitions via a sloppy quorum over the first N healthy nodes, hinted handoff,
  and vector-clock conflict resolution [dynamo-quorum-sloppy-hinted]. Rejected:
  its conflict, read-repair, and merge model is foreign to the Redis data model,
  surprising for compatibility-focused users, and complex, so it violates
  Compatible and Simple for a marginal availability gain. This is the rejection
  this ADR exists to freeze so it is not relitigated.

## Consequences

- Unmodified Redis clients and replication tooling work against IronCache with
  no protocol change, and the steady-state write path carries no quorum tax,
  satisfying Compatible then Efficient [redis-cluster-async-replication].
- The default is explicitly best-effort, not CP. The acknowledged-but-
  unreplicated write-loss window on failover and partition is documented and
  surfaced to clients (#147), and WAIT is positioned as a durability floor that
  bounds but does not close it [redis-wait-not-cp] [redis-wait-not-strongly-consistent].
- Three guardrails ship on by intent: read-only replicas, a min-in-sync-
  replicas write floor, and a max-replica-lag bound, so a misconfigured or
  lagging fleet fails toward refusing writes rather than silently diverging
  [redis-replica-read-only-default] [redis-min-replicas-to-write-default]
  [redis-min-replicas-max-lag-default].
- The opt-in strong-consistency tier (#78, #12) is unblocked to build on a
  fixed async baseline, and the replica-read contract (#147) and node lifecycle
  (#149) are specified against this decision rather than against an open one.
