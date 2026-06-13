# ADR-0006: Default global allocator and memory-accounting strategy

Status: Accepted
Issue: #41

## Context

IronCache competes on bytes-per-key and throughput-per-core, and both are
decided below the data structures, in the global allocator. The allocator also
determines whether `maxmemory` can be an honest bound (invariant 3): the limit
must be checked against allocator-attributed bytes, not naive object sizes
[redis-maxmemory-accounting]. The realistic candidates in Rust are
tikv-jemallocator (jemalloc) and the mimalloc crate.

## Decision

Ship **tikv-jemallocator (jemalloc 5.3.1)** [tikv-jemallocator-version] as the
default `#[global_allocator]`. Account `used_memory` per shard by reading
jemalloc's `mallctl` statistics (the `epoch` + `stats.allocated` path) so
`maxmemory` is enforced against allocator-attributed RSS rather than estimated
object sizes. Per-shard arenas keep accounting and fragmentation shard-local,
consistent with shared-nothing (ADR-0002).

## Rejected Alternatives

- **mimalloc.** Faster on some microbenchmarks (about 13 percent over tcmalloc on
  leanN, over 2.5x over jemalloc on sh6bench) [mimalloc-benchmarks] and used by
  DragonflyDB, and available in Rust [mimalloc-rust-version]. Rejected as the
  default because it does not expose jemalloc's rich `mallctl` introspection or
  the `je_get_defrag_hint()` path that honest accounting and online defrag (#43)
  depend on [redis-jemalloc-frag-hint]; a raw-speed allocator that cannot tell us
  our true RSS undercuts the memory-honesty invariant. It remains a build-time
  alternative to benchmark.
- **The system allocator.** Rejected: no introspection, no tuned decay; RSS
  under churny eviction is unpredictable.

## Consequences

- Honest `maxmemory` (invariant 3) is achievable via `mallctl`, and online
  defrag (#43) has the `je_get_defrag_hint()` hook it needs.
- jemalloc's dirty-page decay (`dirty_decay_ms=10000`, muzzy off)
  [jemalloc-decay-defaults] governs RSS return under churn; this is also what
  Redis and Valkey run [redis-bundled-jemalloc-version], so behavior is
  well understood. active-defrag requires jemalloc [redis-active-defrag-jemalloc],
  which this choice provides.
- The allocator-vs-mimalloc throughput and RSS comparison under a real cache
  workload is the empirical follow-up (#42's benchmark).
