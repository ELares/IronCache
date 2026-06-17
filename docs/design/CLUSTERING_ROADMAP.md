<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Clustering implementation roadmap

Issue: #68 (clustering parent). This is the implementation-status and roadmap
map for IronCache multi-node clustering: what has shipped, and what remains with
its concrete dependencies. It tracks code against the subsystem design specs
([CLUSTER_CONTRACT.md](CLUSTER_CONTRACT.md), [CONTROL_PLANE.md](CONTROL_PLANE.md),
[MEMBERSHIP.md](MEMBERSHIP.md), [NODE_LIFECYCLE.md](NODE_LIFECYCLE.md),
[MIGRATION.md](MIGRATION.md), [REBALANCING.md](REBALANCING.md),
[REPLICATION.md](REPLICATION.md), [REPLICA_READ.md](REPLICA_READ.md)); it does
not re-specify them.

## Status at a glance

| Slice | Capability | State | PR |
| --- | --- | --- | ---: |
| 1 | CRC16/XMODEM slot hashing + the `CLUSTER` command surface (gated on `cluster-enabled`) | shipped | #286 |
| 2 | Static config-driven multi-node slot map + MOVED/CROSSSLOT routing + multi-node `CLUSTER SLOTS`/`SHARDS`/`NODES` projection | shipped | #287 |
| - | 3-node horizontal-scaling benchmark on AWS Graviton (2.81x) | shipped | #288 |
| 3 | Runtime-mutable slot map + the `CLUSTER` mutator surface (self-formation, Option A) | shipped | #289 |
| 3b | Inter-node slot-map sync (coherent global `CLUSTER SLOTS`, `redis-cli --cluster create` auto-converges) | not built | - |
| 4 | Online resharding: ASK/ASKING + MIGRATING/IMPORTING + `MIGRATE` key transfer | not built | - |
| 5 | Replication (primary/replica, async default) | not built | - |
| 6 | Failover + replica reads | not built | - |

## What is shipped (slices 1-3 + benchmark)

A correct, benchmarked, operable multi-node **sharded** cluster that an unmodified
Redis-cluster client (go-redis, lettuce, ioredis, `redis-cli -c`,
`memtier_benchmark --cluster-mode`) routes across without changes:

- **Wire contract** ([CLUSTER_CONTRACT.md](CLUSTER_CONTRACT.md), #70): the 16384-slot
  space, CRC16/XMODEM hashing (byte-exact vs `redis/src/cluster.c`), hash-tag
  co-location, CROSSSLOT rejection, MOVED redirection, and the `CLUSTER SLOTS` /
  `SHARDS` / `NODES` / `INFO` topology projection.
- **Routing**: every keyed command is gated by slot ownership before any execution
  path (live, MULTI queue-time, cross-shard fan-out, pipelined); a non-owned slot
  returns MOVED, a multi-key span across slots returns CROSSSLOT.
- **Self-formation surface** (slice 3): a runtime-mutable slot map (`Arc` +
  interior atomics, mirroring `RuntimeConfig`; the `owns()` hot path is a single
  lock-free atomic load) and the `CLUSTER` mutators `ADDSLOTS`/`ADDSLOTSRANGE`,
  `DELSLOTS`/`DELSLOTSRANGE`, `SETSLOT <slot> NODE <id>`, `FLUSHSLOTS`, `MEET`,
  `FORGET`, `SET-CONFIG-EPOCH`, `BUMPEPOCH`, with byte-exact Redis error strings.
- **Benchmark** ([../bench/CLUSTER_BENCHMARK.md](../bench/CLUSTER_BENCHMARK.md)): a
  3-node Graviton cluster sustained 2.81x a single node (near-linear), with 0
  MOVED/sec in steady state (clients route by slot).

This is a complete clustering capability for the sharded-throughput use case. It
needs none of the slices below: a fixed-shard, masters-only cluster is formed by
static TOML topology (slice 2) or by driving the slice-3 mutators per node.

## What remains, and why each is blocked

Every remaining slice depends on one or more foundational pieces the engine does
not have yet. They are not incremental polish; each is a distinct subsystem.

### Foundational dependency: inter-node networking

The server has **no outbound TCP** today. The cross-shard coordinator (#107) is
in-process (mpsc between shard threads), and the only RESP client in the tree is
the benchmark harness. A node cannot currently act as a client to its peers.
Slices 3b (sync), 4 (`MIGRATE` transfer), and 5 (replication stream) all require
this. Building it is the highest-leverage next step because it unblocks the rest.

### Foundational decision: the control-plane architecture (#73)

[CONTROL_PLANE.md](CONTROL_PLANE.md) commits the authoritative slot map to a
**Raft**-replicated log. An alternative considered during slice-3 planning was a
point-to-point gossip/pull of slot ownership. These conflict: a pull mechanism
that the committed Raft design intends to delete is throwaway scaffolding. Slice
3b must not pre-empt this decision. Note also that the determinism seam
([RUNTIME_ABSTRACTION.md](RUNTIME_ABSTRACTION.md), ADR-0003) has no timer; a
periodic peer-pull is genuinely nondeterministic I/O and needs its own ADR and
test harness, not a rider on an existing slice.

### Slice 3b: inter-node slot-map sync (#73/#74)

Make `CLUSTER SLOTS` on any node reflect the **global** ownership, so
`redis-cli --cluster create`'s final cross-node convergence poll passes. Today
each node's local view is correct, but node1 does not see node2's ranges (a
negative test, `three_a_gap_node_does_not_see_a_peers_local_assignments`, pins
this boundary and will flip to a positive assertion when 3b lands).
Depends on: inter-node networking + the #73 architecture decision.

### Slice 4: online resharding (#75, ASK)

Live slot migration: `SETSLOT <slot> MIGRATING <dst>` / `IMPORTING <src>` slot
states, the ASK/ASKING redirection protocol, and `MIGRATE` key transfer
([MIGRATION.md](MIGRATION.md), [REBALANCING.md](REBALANCING.md), #148). The slice-3
`SETSLOT NODE` is an atomic ownership flip only; its transient
ownership-during-transfer window (documented at `set_slot_node`) is what this
slice's state machine closes.
Depends on: inter-node networking, **plus** `DUMP`/`RESTORE` (#129/#14, the RDB
serialization format) which `MIGRATE` uses to move a key's value and is not built.

### Slice 5: replication (#77, XL)

Primary/replica with the async default ([REPLICATION.md](REPLICATION.md),
ADR-0026). A replication stream (PSYNC-like) over the inter-node transport.
Depends on: inter-node networking. This is the HA tier.

### Slice 6: failover + replica reads (#147/#149, XL)

Failure detection, replica promotion/election, and replica-read routing
([REPLICA_READ.md](REPLICA_READ.md), [NODE_LIFECYCLE.md](NODE_LIFECYCLE.md)).
Depends on: slice 5 + membership/failure-detection (#74).

## Why the line is drawn here

Slices 5-6 (and the consensus underpinning 3b) are the consensus/replication tier.
A half-built consensus or replication layer is worse than a clean, documented gap:
it presents as working while being silently wrong under partition or concurrent
assignment. The shipped slices give a correct, benchmarked sharded cluster with no
half-built distributed-systems machinery behind it. The remaining slices should be
taken on deliberately, starting from the #73 architecture decision and the
inter-node networking layer, not bolted on piecemeal.
