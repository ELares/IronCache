# ADR-0025: Cluster keyspace partition count (16384 as the single dual-purpose unit)

Status: Accepted
Issue: #72

## Context

IronCache scales across cores now and across nodes later, and both axes pivot on
one number: how many keyspace partitions the dataset is split into. ADR-0011
already froze a slot-ready storage layout in which the store is structured as
per-slot shards on the Redis Cluster 16384-slot space, and those per-slot shards
double as the per-core execution units of ADR-0002 and as the future migration
unit. This ADR fixes the partition count and commits to what that count means,
so the data plane is not rebuilt twice.

Two decision issues reach opposite conclusions on this single axis (RISK R1), and
this ADR resolves both. Issue #72 argues to BIND the internal unit to the 16384
client slots: one fixed partition count that is simultaneously the per-core
execution shard now and the cluster migration unit later. Issue #71 argues to
DECOUPLE, defining a separately configurable internal partition count P (a power
of two sized to a small multiple of cores) folded deterministically onto the
16384 client slots, so the internal granularity tunes independently of the wire
protocol. Both target M1 under #68; #71 is resolved here, in favor of #72, and
this ADR is the single record for the axis.

The substantive question is whether the externally addressed hash slot and the
internal execution-shard/migration unit are one object or two. Valkey's kvstore
refactor splits the global dictionary into 16384 per-slot dictionaries, which
removed the two cluster slot-tracking pointers and saved 16 bytes per entry
[valkey-per-slot-dict-16b], so a per-slot dict is a cheap, self-contained unit to
detach and ship. Valkey 8.0 then made CLUSTER SETSLOT replicate to replicas
synchronously before applying on the primary and recover slot-migration state
across failover [valkey-8-setslot-replicated], and Valkey 9.0 ships atomic slot
migration where a child snapshots the migrating slots, the source streams
incremental mutations, and ownership is atomically handed off and broadcast,
avoiding the legacy per-key approach's round-trips and large-key OOM risk
[valkey-atomic-slot-migration]. The 16384 count itself is a wire-format choice:
Redis picked 16384 (not 65536) so the per-node served-slots bitmap in each
heartbeat is 2 KB rather than a prohibitive 8 KB, and because clusters are
unlikely to scale beyond about 1000 master nodes [redis-cluster-why-16384].

## Decision

- **Adopt the 16384-slot partitioning as the one unit.** The internal partition
  is `p = HASH(key) mod 16384`, and that slot is simultaneously the per-core
  execution shard of ADR-0002 and the atomic per-slot migration unit of
  ADR-0011. There is exactly one partitioning concept, not two. This is the BIND
  position of #72.
- **Each partition owns its own dictionary.** This mirrors Valkey's per-slot
  dict layout, which is what makes a partition cheap to detach and ship as a
  migration unit, having already shed the per-entry slot-tracking pointers
  [valkey-per-slot-dict-16b].
- **The wire-visible hash slot is the internal partition, with no translation.**
  Because 16384 is also the client-visible slot count, the externally addressed
  slot maps 1:1 to the internal partition. There is no slot-to-partition fold on
  the hot path and CLUSTER SLOTS/SHARDS is a direct projection of ownership.
- **Execution shards and partitions are not forced to be equinumerous.** With N
  execution shards where N is at most the core count (ADR-0002), partition p is
  owned by shard `p mod N` (or a contiguous range). Each shard owns a disjoint
  set of partitions and runs them single-threaded, so a partition is never
  touched by two cores and needs no intra-partition locking. Rebalancing across
  cores is reassigning partition ownership between shards, the same primitive
  cross-node migration uses.
- **Resharding follows Valkey atomic slot migration.** Ownership transfers as a
  unit with no dual-ownership window [valkey-atomic-slot-migration], built on the
  replicated SETSLOT handshake that makes the new owner consistently authoritative
  across the shard [valkey-8-setslot-replicated]. This model is the reason the
  migration unit must be the slot-aligned partition.
- **Reject a separately-configurable internal partition count P for v1.** The
  #71 decouple proposal is folded in and declined: a configurable P introduces a
  slot-to-partition mapping layer and a second rebalancing unit, for no measured
  win at the single-node-first scope of ADR-0011. P is fixed at 16384, the one
  unit. This is revisitable only if a future benchmark shows 16384 too coarse
  (see Consequences).

## Rejected Alternatives

- **Decouple: a configurable internal partition count P folded onto 16384 client
  slots (#71).** P tuned to core count would keep per-partition metadata small
  and let the internal map rebalance on its own cadence while clients still see
  16384. Rejected for v1: it adds a slot-to-partition fold that must stay stable
  and cheap, plus a second rebalancing unit and an invariant that the two layers
  never drift, all to be specified and tested. At single-node-first scope
  (ADR-0011) there is no measured win to justify that mapping layer, and the per-
  slot dict already shed the per-entry overhead that motivated a coarser count
  [valkey-per-slot-dict-16b]. The 16384 cost is a fixed wire-format artifact
  [redis-cluster-why-16384], not a per-key tax. Revisitable if a bench shows
  16384 too coarse for the execution shard.
- **Small fixed count equal to core count (e.g. 64 partitions).** Minimal per-
  partition overhead and the execution shard would literally be the partition.
  Rejected: the internal partition would no longer align with the 16384 client
  slots, so resharding could not reuse the Valkey atomic slot-migration model
  [valkey-atomic-slot-migration] and we would owe a bespoke key splitter at
  clustering time.
- **Two separate units: execution shards now, slot units bolted on at clustering
  time.** Smallest M1 surface. Rejected: it rebuilds the data plane at clustering
  time, the exact double-build ADR-0011's slot-ready layout exists to avoid, and
  it violates the Efficient and Simple tenets.

## Consequences

- One partitioning concept governs both axes: per-core execution today and cross-
  node migration later are the same 16384-slot unit, so the data plane is built
  once, honoring ADR-0011's slot-ready intent.
- No slot-to-partition translation exists on the hot path, and CLUSTER
  SLOTS/SHARDS is a direct projection of partition ownership, keeping the wire
  contract trivially derivable.
- Per-partition dictionary bookkeeping for 16384 dicts is accepted as fixed
  overhead; it is the same per-slot dict layout that already removed 16 bytes per
  entry [valkey-per-slot-dict-16b], so the overhead is in fixed metadata, not in
  a per-key tax, and 16384 is a one-time wire-format cost [redis-cluster-why-16384].
- Hot-shard mitigation (the ADR-0002 risk, owned by #32) has its unit of
  isolation for free: a hot partition is reassigned to a less-loaded execution
  shard locally, or migrated to another node, without splitting keys, because
  partition boundaries already exist.
- Resharding and replication inherit the Valkey model: ownership moves atomically
  with no dual-ownership window [valkey-atomic-slot-migration] over a replicated
  SETSLOT handshake [valkey-8-setslot-replicated]; #75 (migration) and #76
  (replication) build on this unit, and #70 owns the wire-facing guarantees.
- The #71 decouple option is closed for v1 but not erased: if a future benchmark
  shows 16384 too coarse for the per-core execution shard, a superseding ADR may
  reintroduce a configurable P, leaving this decision's single-unit commitment
  intact until measured.
