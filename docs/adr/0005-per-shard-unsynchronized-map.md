# ADR-0005: Per-shard unsynchronized map, concurrent map only as a fallback

Status: Accepted
Issue: #36

## Context

Under shared-nothing thread-per-core (ADR-0002), each shard is owned by exactly
one core and that core is the only thing that ever touches the shard's key
space. This decides what data structure backs a single shard, and whether a
concurrent map is ever needed.

## Decision

The per-shard primary store is an **unsynchronized `hashbrown::HashMap`** owned
by one core. Reads and writes are plain memory operations with no lock, no
atomic, and no CAS on the hot path. A concurrent map (for example a sharded
`dashmap`, or a lock-free `papaya`) is kept only as a documented fallback, to be
adopted solely if shard affinity is ever abandoned.

## Rejected Alternatives

- **A concurrent map as the primary store (dashmap / scc / papaya).** Rejected
  on Efficient: these carry per-operation locks or atomics
  [dashmap-internal-design] [papaya-version-reclamation] that are pure overhead
  when a single owner already guarantees exclusive access. They solve a sharing
  problem ADR-0002 designed away.
- **`std::collections::HashMap`.** Not rejected on merit (it is the same
  SwissTable as `hashbrown`); `hashbrown` is chosen directly for its raw-entry
  and allocator-parameter APIs, which the per-shard allocator (#41) and custom
  object layout (#111) need.

## Consequences

- Correctness depends entirely on the shard-affinity invariant (invariant 1): a
  stray cross-core access is a data race, so CI will lint the hot-path crate to
  deny `std::sync` locks and shared atomics, and affinity is asserted at the
  shard boundary.
- Memory reclamation is trivial because nothing else can hold a reference
  (ADR-0004).
- Cross-shard work is message passing through the coordinator (#29), never
  shared mutation.
