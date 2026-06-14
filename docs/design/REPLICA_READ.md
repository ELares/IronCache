# Design: Replica-read contract (READONLY/READWRITE, replica routing, bounded staleness)

Issue: #147. Decisions: ADR-0026 (async primary/replica default, best-effort not
CP, replica-read-only on). Related: #70 (CLUSTER_CONTRACT: 16384 slots, CRC16,
MOVED/ASK), #76 (replication default), #1 (vision).

## Goal and scope

Redis Cluster clients scale reads by sending READONLY on a connection and then
routing reads to replicas; this is part of the wire contract IronCache promises
to keep. ADR-0026 fixes replica-read-only on, so replicas reject writes, but it
does not decide whether clients may READ from replicas, how the
READONLY/READWRITE connection-state pair behaves, how a replica answers for the
slots it serves, or how async-replication staleness is bounded and surfaced.
This spec owns that command-pair and consistency contract. Scope: the
per-connection READONLY/READWRITE state machine, replica read routing under the
#70 slot view, and the bounded-staleness signal surfaced to clients. Out of
scope: the slot map authority (#73), migration redirection mechanics (#70), and
the write path (#76).

## Design

### READONLY/READWRITE connection state

- A connection carries one bit: read-write (default) or read-only. READONLY
  sets the bit; READWRITE clears it. The bit is per-connection, not global, and
  is unaffected by the node role. This mirrors the Redis Cluster READONLY/
  READWRITE pair that lets a replica serve reads for slots it replicates
  [redis-cluster-readonly-replica].
- On a replica, a read for an owned-or-replicated slot succeeds only when the
  read-only bit is set; otherwise the replica returns MOVED to the primary, so
  a default (read-write) connection keeps the strong-read behavior unmodified
  clients expect [redis-cluster-readonly-replica]. Writes on a replica are
  always rejected per ADR-0026's replica-read-only posture, independent of the
  bit.

### Replica read routing

- Slot ownership and the CLUSTER SLOTS/SHARDS projection come from
  CLUSTER_CONTRACT (#70); this spec only adds the replica leg. A read-only
  connection whose key hashes (CRC16 mod 16384, #70) to a slot this replica
  replicates is answered locally; a key for a slot this node neither owns nor
  replicates returns MOVED, driving the client's normal map refresh.
- Because replication is asynchronous (ADR-0026), a replica read may observe a
  value older than the primary. This is the Envoy ReadPolicy model: non-primary
  read targets may return stale data due to async replication
  [envoy-redis-readpolicy] [redis-cluster-async-replication]. IronCache does
  not silently proxy reads to the primary to hide this; the client chose the
  replica by setting READONLY and is told the staleness bound.

### Bounded staleness surfaced to clients

- Each replica tracks its replication lag against the primary using the same
  in-sync signal ADR-0026 bounds with min-replicas-max-lag. A replica whose lag
  exceeds the configured staleness bound stops serving read-only reads for its
  slots and returns MOVED, so a client never silently reads beyond the bound.
- The bound is observable, not just enforced: it is exposed through INFO
  replication fields and the CLUSTER SHARDS health/role projection (#70), so a
  client or operator can reason about the worst-case staleness of any replica
  read. This makes the best-effort-not-CP property of ADR-0026 legible at the
  read path rather than hidden.

## Open questions

- Whether to expose an Envoy-style per-request ReadPolicy hint
  (PREFER_REPLICA/PREFER_MASTER) beyond the binary READONLY/READWRITE bit
  [envoy-redis-readpolicy], or keep the Redis-native pair only for v1.
- The exact default staleness bound, and whether it is derived from
  min-replicas-max-lag (ADR-0026) or set independently per keyspace.
- How a replica that crosses the staleness bound interacts with the #70 ASK
  path during an in-flight slot migration.

## Acceptance and test hooks

- A READONLY connection reads from a replica for a replicated slot; the same
  connection after READWRITE gets MOVED to the primary for that slot.
- A replica past its staleness bound returns MOVED for read-only reads and its
  lag is visible in INFO/CLUSTER SHARDS before and after crossing the bound.
- Unmodified redis-cli, go-redis, lettuce, and ioredis route replica reads via
  READONLY without errors, matching the #70 contract.

## References

- ADR-0026; issues #147, #76, #70, #1; specs CLUSTER_CONTRACT (#70).
- Claims: [redis-cluster-readonly-replica], [envoy-redis-readpolicy],
  [redis-cluster-async-replication].
