# ADR-0012: Headline scale-out targets

Status: Accepted
Issue: #146

## Context

The Efficient tenet is pinned to falsifiable numbers (#7); the Scalable tenet had
no quantified counterpart, so the clustering mechanisms (#73 control plane, #74
membership, #75 migration) each optimize locally with no system-level SLO. This
pins the distributed scoreboard as a set of targets to design toward and later
validate, the Scalable analog of #7. These are targets, not guarantees; #99
(Jepsen) and the cluster benchmarks confirm or revise them.

## Decision

Adopt these headline scale-out targets for the multi-node system:

- **Max nodes:** target several thousand (aim 4096), past Redis Cluster's
  suggested ~1000-master ceiling [redis-cluster-max-nodes-recommendation]
  [redis-cluster-why-16384]. Going past 1000 is made feasible by an O(1)-load
  SWIM data plane whose detection time is independent of N [swim-scalability]
  with a Raft-managed slot map [raft-overview] (#73/#74), rather than Redis's
  all-to-all gossip.
- **Slots per node:** the 16384-slot space (ADR-0011) spread so a node owns a
  working range of tens to low-hundreds of slots in typical deployments, with the
  floor being a few slots per node at max node count.
- **Rebalance budget:** moving one partition completes within a single-digit-
  second budget at a stated write rate, without a write freeze (#75).
- **Failover budget:** end-to-end detect + promote + reconverge within a few
  seconds, bounded by the node-timeout (default 15000 ms in Redis
  [redis-cluster-node-timeout-default-rrc] is the upper bound we aim to beat).

## Rejected Alternatives

- **Leave Scalable unquantified.** Rejected: without committed numbers #73/#74/#75
  optimize local mechanisms with no system SLO, and the Scalable tenet stays a
  slogan while Efficient is measured.
- **Inherit Redis's ~1000-node ceiling as the target.** Rejected: the ceiling is
  an artifact of all-to-all gossip and the slot-bitmap cost [redis-cluster-why-16384];
  a SWIM + Raft design is chosen precisely to exceed it.

## Consequences

- #73 (control plane), #74 (membership), #75 (migration), and #80 (placement) now
  have system-level budgets to design against, not just local correctness.
- These targets are validated by the cluster benchmarks and the Jepsen plan (#99)
  once multi-node exists; they are revised by a superseding ADR if measurement
  forces it.
- The targets are realized in Wave 3 (clustering), consistent with the
  single-node-first sequencing (ADR-0011, roadmap).
