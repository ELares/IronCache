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
jemalloc's `mallctl` statistics (the `epoch` + `stats.allocated` path), so
`maxmemory` is enforced against allocator-attributed **logical** bytes (the
analog of Redis `used_memory`), not naive object sizes [redis-maxmemory-accounting].
This is what invariant 3 means by "allocator-attributed bytes": it is the live
allocated total, not RSS. RSS (`stats.resident`) can exceed the logical ceiling
under fragmentation and unpurged dirty pages; the decay and defrag machinery
below keep RSS close to the logical bound rather than letting them diverge.
Enable jemalloc's **background purge thread** by default, flipping the
upstream-off default the way Redis does [jemalloc-background-thread-default]
[redis-jemalloc-bg-thread-default], and tune `dirty_decay_ms` below the stock
10 s for eviction churn (the exact value is a config knob, #85). Per-shard
arenas keep accounting and fragmentation shard-local, consistent with
shared-nothing (ADR-0002).

## Rejected Alternatives

- **mimalloc.** Faster on some microbenchmarks (about 13 percent over tcmalloc on
  leanN, over 2.5x over jemalloc on sh6bench) [mimalloc-benchmarks] and used by
  DragonflyDB [dragonfly-mimalloc-version], available in Rust
  [mimalloc-rust-version]. Rejected as the default on one decisive leg: it has no
  `je_get_defrag_hint()` equivalent, so the online defragmenter (#43) would have
  no per-allocation sparseness query to drive compaction [redis-jemalloc-frag-hint]
  [redis-active-defrag-jemalloc]. (mimalloc can report process RSS via
  `mi_process_info`, so accounting alone does not rule it out; the defrag-hint gap
  does.) It stays a build-time alternative to benchmark in #42.
- **The system allocator.** Rejected: no introspection, no tuned decay; RSS
  under churny eviction is unpredictable.

## Consequences

- Honest `maxmemory` (invariant 3) is achievable via `mallctl`, and online
  defrag (#43) has the `je_get_defrag_hint()` hook it needs.
- jemalloc's dirty-page decay (upstream `dirty_decay_ms=10000`, muzzy off)
  [jemalloc-decay-defaults] governs RSS return under churn; IronCache lowers it
  for eviction churn and enables the background purger (see the Decision), rather
  than running the stock value. jemalloc is also what Redis and Valkey run
  [redis-bundled-jemalloc-version], so behavior is well understood, and
  active-defrag requires jemalloc [redis-active-defrag-jemalloc], which this
  choice provides.
- The allocator-vs-mimalloc throughput and RSS comparison under a real cache
  workload is the empirical follow-up (#42's benchmark).
- Because both the store tables and the value blobs flow through jemalloc, the
  transparent-huge-page lever for the random-key hot path (#512) rides the same
  `malloc_conf` seam: a default-off `thp:always` (the `hugepages` build feature, or
  the `_RJEM_MALLOC_CONF=thp:always` runtime override) backs jemalloc's extents with
  2 MiB pages to cut TLB misses. It is Linux-only and opt-in because `thp:always` can
  raise the very RSS this ADR accounts against `maxmemory`; the RSS/latency tradeoff
  and the knobs are documented in docs/design/CONFIG.md ("Transparent huge pages").
