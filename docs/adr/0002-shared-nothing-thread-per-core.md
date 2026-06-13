# ADR-0002: Shared-nothing thread-per-core as the core concurrency model

Status: Accepted
Issue: #24

## Context

Every subsystem (the store, the command path, replication, expiry) inherits one
concurrency model, so the model must be chosen once. The candidates from the
prior art are: a shared keyspace guarded by locks (KeyDB), a single command
thread with offloaded socket I/O (Redis and Valkey), or shared-nothing
thread-per-core with the keyspace sharded across cores (Seastar, ScyllaDB,
DragonflyDB). The choice is load-bearing for the Efficient tenet (throughput per
core) and for the determinism invariant.

## Decision

Adopt shared-nothing thread-per-core. The keyspace is partitioned into N shards
by `k = HASH(KEY) % N`, where N is at most the core count
[dragonfly-shard-formula]; each shard is owned by exactly one core, which is the
only thing that touches that shard's state. There is no shared mutable hot-path
state, and cross-shard work is explicit message passing, never shared mutation.

## Rejected Alternatives

- **Shared dict guarded by a ticket spinlock (KeyDB).** Rejected on Efficient:
  a shared, mutated keyspace under a spinlock has a contention ceiling
  [keydb-fastlock-ticket-spinlock]; it optimizes the lock instead of removing
  it.
- **Single command thread with offloaded I/O (Redis, Valkey).** Rejected on
  Efficient: command execution stays on one thread
  [redis-command-execution-single-threaded], so most cores sit idle; offloading
  sockets does not lift the keyspace ceiling.
- **Tokio work-stealing over a shared store.** Rejected: work-stealing forces
  every shared structure to be `Send + Sync` and re-introduces atomics and locks
  on the hot path [tokio-workstealing-readiness-model], the opposite of what the
  shared-nothing model buys.

## Consequences

- Locks become unnecessary on the hot path by construction
  [glommio-locks-never-necessary] and [seastar-shared-nothing]; this is
  invariant 1.
- Cross-shard operations (multi-key, scatter/gather, MULTI across shards) need an
  explicit coordinator (#29).
- A single hot key or skewed range can saturate one core; mitigation is owned by
  the hot-shard research (#32) and routing/migration design.
- This decision is the root of the dependency graph: it makes the per-shard
  store an unsynchronized map (#36), shapes the allocator (per-shard arenas,
  #41), and shapes the no-fork persistence stance (#59).
