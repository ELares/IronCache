# Experiment: Allocator bake-off (jemalloc / mimalloc / snmalloc) under a cache workload

Issue: #42. Provisional decision: ADR-0006 (default global allocator and memory-accounting strategy).

## Provisional decision (already pinned)

ADR-0006 ships tikv-jemallocator (jemalloc 5.3.1) [tikv-jemallocator-version] as
the default `#[global_allocator]`, with the background purge thread enabled and
`dirty_decay_ms` lowered below the stock 10 s for eviction churn. The decision is
pinned on introspection and defrag grounds, not on raw alloc speed: jemalloc
exposes `mallctl` for honest `used_memory` accounting [redis-maxmemory-accounting]
and `je_get_defrag_hint()` for the online defragmenter (#43)
[redis-jemalloc-frag-hint] [redis-active-defrag-jemalloc], which mimalloc has no
equivalent for. mimalloc [mimalloc-rust-version], snmalloc [snmalloc-rs-version],
and a custom per-shard slab arena remain build-time alternatives that this
experiment must measure. This experiment does not re-decide the introspection
leg; it tests whether the throughput and RSS legs hold under a real workload.

## Why this is harness-blocked

The vendor numbers we would otherwise lean on are unrepresentative. mimalloc's
headline figures come from a single 2021 run on one fixed 16-core AMD Ryzen
5950x [mimalloc-benchmarks], and the comparators do not line up the way a casual
read suggests: the about 13 percent leanN speedup is over tcmalloc, while the
over 2.5x figure is the sh6bench result over jemalloc [mimalloc-benchmarks].
Neither is a churny eviction-driven cache, and the decisive axis for a cache is
RSS reclamation, not alloc speed. RSS return is governed by allocator defaults
that must be swept rather than assumed: jemalloc dirty/muzzy decay
[jemalloc-decay-defaults] and arena count [jemalloc-narenas-default], and
mimalloc's purge delay [mimalloc-purge-defaults]. None of this can be answered on
paper; it needs a harness driving the real shard structure, instrumented for
RSS/used_memory [redis-fragmentation-ratio], run across the canonical hardware
matrix and emitting machine-tagged CSV (#8).

## Experiment to run

Corpus and workload, driven through the actual shard data structure (not a
synthetic alloc loop) so the size-class distribution is real, in three phases:
- Phase A (fill): small mixed-KV fill, values dominated by <=64B (the cache
  common case).
- Phase B (churn): sustained eviction churn held at `maxmemory`.
- Phase C (peak-then-free): fill to peak, free about 40 percent, hold idle to
  observe reclamation.

Candidates (versions pinned by #8): tikv-jemallocator 0.7.0
[tikv-jemallocator-version], mimalloc crate 0.1.52 [mimalloc-rust-version],
snmalloc-rs 0.7.4 [snmalloc-rs-version], and the custom per-shard slab arena as a
real baseline (it is the only candidate that can natively answer an
is-this-slab-sparse query for #43).

Fixed: workload phases, key/value size distribution, `maxmemory`, seed, build
profile, per-machine warmup. Varied: allocator (the 4 candidates); decay/purge
knob across at least {fast, default, slow} (`dirty_decay_ms` for jemalloc,
`MIMALLOC_PURGE_DELAY` for mimalloc); `narenas` held at default
[jemalloc-narenas-default] first, then a per-shard-arena probe; every run
repeated across the full canonical hardware matrix including a 1-vCPU machine.

Measured, per candidate per machine: throughput-per-core, p99.9 latency,
steady-state RSS during Phase B, and post-peak RSS/used_memory after the Phase C
idle hold [redis-fragmentation-ratio]. Report per-core, never aggregate, since
single-core parity is the hard case [dragonfly-single-core-parity].

Decision rule: plot the RSS-vs-latency curve over the decay/purge sweep and pick
the knee for each allocator's default. Rank candidates on (1) post-peak
RSS/used_memory after idle hold, then (2) throughput-per-core and p99.9, with no
candidate excused on a single machine or on aggregate QPS. A challenger overturns
ADR-0006 only if it wins both legs across the matrix AND clears the #43
defrag-hint requirement; otherwise jemalloc stays the default.

## What would change the decision

- mimalloc or snmalloc reclaiming post-peak RSS as cleanly as jemalloc decay
  [jemalloc-decay-defaults] while beating it on throughput-per-core and p99.9
  across the matrix, and supplying a sparseness query usable by #43.
- The custom slab arena beating both on post-peak fragmentation by a margin that
  justifies its maintenance cost, tilting #41 toward build-vs-buy.
- A decay/purge default that drops steady-state RSS without a madvise-storm
  latency spike on the request path.
- A musl static-build penalty (#84) large enough to reorder the ranking.

## References

- Parent: #41. Related: #8 (harness + machine-tagged CSV), #43 (online defrag),
  #84 (musl build), #85 (decay knob). Vision: #1.
- ADR-0006 (default global allocator and memory-accounting strategy).
- `docs/research/memory-allocators.md`, `docs/research/dragonfly.md`.
- Claims: [tikv-jemallocator-version], [mimalloc-rust-version],
  [snmalloc-rs-version], [mimalloc-benchmarks], [jemalloc-decay-defaults],
  [jemalloc-narenas-default], [mimalloc-purge-defaults],
  [redis-fragmentation-ratio], [redis-maxmemory-accounting],
  [redis-jemalloc-frag-hint], [redis-active-defrag-jemalloc],
  [dragonfly-single-core-parity].