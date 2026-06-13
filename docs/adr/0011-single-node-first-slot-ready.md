# ADR-0011: Single-node-first with a slot-ready storage layout

Status: Accepted
Issue: #69

## Context

The clustering posture must be decided before the storage engine is written: it
dictates keyspace partitioning, the per-core execution model, and the on-disk
layout. We want a shippable single-node binary fast without a layout that forces
a storage rewrite to reach multi-node later. This builds on shared-nothing
(ADR-0002), where the keyspace is already sharded per core.

## Decision

Ship **single-node first, with a slot-ready storage layout**. The store is
structured as per-slot shards (the Redis Cluster 16384-slot space
[redis-cluster-hash-slots-16384]) that double as the per-core execution units of
ADR-0002. A single process owns all slots today; multi-node later assigns slot
ranges to nodes and adds replication and migration without re-laying-out data.

## Rejected Alternatives

- **Design the full cluster up front.** Rejected on Simple and time-to-usable:
  it delays a working binary and front-loads distributed complexity (membership,
  consensus, migration) before the single-node engine is even proven. The
  Scalable tenet is satisfied by a layout that grows out cleanly, not by building
  the cluster first.
- **Single-node with a layout indifferent to slots.** Rejected: reaching
  multi-node would then require re-partitioning the live keyspace, a storage
  rewrite; making the shard a slot range from day one avoids that.

## Consequences

- A usable single-node binary is the near-term deliverable; the per-slot shard is
  both the migration unit (#75) and the per-core execution unit.
- The Redis-Cluster client contract (16384 slots, CRC16, MOVED/ASK) is reachable
  without a data migration (#70), and the internal shard-to-slot mapping is
  decided in #71/#72.
- Scale-out targets for the eventual multi-node system are pinned in ADR-0012.
