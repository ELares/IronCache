# Design: Online active defragmentation (native sparse-slab reclaimer)

Issue: #43. Decisions: ADR-0006 (jemalloc + accounting), ADR-0007 (memory
ceiling, cache mode). Related: #41 (allocator decision), #42 (allocator
benchmark numbers), #45 (memory ceiling), #35 (per-shard index), #111
(kvobj layout), #85 (config knobs), #86 (INFO/metrics).

## Goal and scope

External fragmentation is the dominant source of RSS bloat for a long-lived
cache with mixed object sizes: a heap that fills to peak then frees half still
pins near-peak RSS because freed objects are scattered across partially-used
slabs (jemalloc calls them runs; this spec says slab throughout), so the OS
never reclaims the empty extent [redis-fragmentation-ratio]. This
spec decides how IronCache reclaims that space online, and whether it needs an
online defragmenter at all in M1. It is the reclaim counterpart to eviction,
bounded by the allocator accounting of ADR-0006 / ADR-0007. Scope: the reclaim
mechanism, its allocator introspection contract, and the throttle and threshold
knobs. Out of scope: eviction (EVICTION.md), TTL reclamation (EXPIRATION.md),
and the background purge thread (an ADR-0006 setting).

## Design

### Bound structurally first, reclaim as a backstop

The first line of defense is layout, not a defragmenter: fine-grained slab
classes (jemalloc built with an 8-byte quantum the way Redis does
[redis-jemalloc-lg-quantum-3]) keep internal fragmentation low, and `maxmemory`
is enforced against allocator-attributed logical bytes, not RSS
[redis-maxmemory-accounting] (ADR-0006). The reclaimer is the residual answer
for the mixed-size, long-lived case structural bounding cannot fully prevent,
and is off by default (below).

### Native is-this-slab-sparse query

Redis asks its forked jemalloc whether the slab backing a pointer is sparse
enough that relocating the object would reduce fragmentation, via the
`je_get_defrag_hint()` patch behind `JEMALLOC_FRAG_HINT`
[redis-jemalloc-frag-hint] on a bundled jemalloc 5.3.0
[redis-bundled-jemalloc-version]. IronCache does NOT port that patch: carrying
an out-of-tree jemalloc patch on tikv-jemallocator 0.7.0 (jemalloc 5.3.1)
[tikv-jemallocator-version] across upstream bumps is exactly the allocator
lock-in ADR-0006 avoids. Instead IronCache specifies a native
`is-this-slab-sparse` query in the allocator introspection API: given a value
handle it answers whether the owning slab's free-bytes ratio is past a
threshold, computed from slab metadata the reclaimer maintains. The cost is that
metadata (snmalloc shows the floor is small, 64 bits per 64KiB slab
[snmalloc-metadata-overhead]); the payoff is no patch maintenance and no
allocator coupling.

### Copy-relocate through the owned per-core index

When the query flags a sparse slab, the reclaimer copies each live value out so
the slab empties and the allocator can free the run. This is safe and cheap
because IronCache owns its value handles: relocation updates the per-core value
index entry (HASHTABLE.md #35, kvobj #111) rather than rewriting any
caller-visible pointer. Because the runtime is shared-nothing thread-per-core
(ADR-0002) and each shard's index is unsynchronized and single-writer
(ADR-0005), the relocate runs on the owning core with no lock and no cross-core
coordination: read the live value, allocate a fresh copy in a dense slab, swap
the index entry, free the old. No reader on another core can observe the old
handle because no other core touches this shard.

### Redis-compatible throttle and thresholds, default OFF

The operator-facing knobs mirror Redis so existing tuning transfers:

| Knob | Default | Meaning |
| --- | --- | --- |
| active-defrag (on/off) | OFF [redis-activedefrag-default] | master enable |
| cycle-min .. cycle-max | 1% .. 25% CPU [redis-activedefrag-thresholds] | reclaimer CPU budget |
| threshold-lower | 10% [redis-activedefrag-thresholds] | do not reclaim below 10% fragmentation |
| threshold-upper | 100% [redis-activedefrag-thresholds] | full effort at 100% |
| ignore-bytes | 100MB [redis-activedefrag-thresholds] | floor before reclaiming |

The reclaimer is OFF by default [redis-activedefrag-default]; structural bounding
carries the common case and the reclaimer is a backstop, not the primary lever.
Gating is driven by `mem_fragmentation_ratio` (RSS / used_memory)
[redis-fragmentation-ratio], surfaced in INFO (#86). Unlike Redis, IronCache
does not require jemalloc for the feature to compile [redis-active-defrag-jemalloc]:
the sparse query is native to the introspection API, so a portable fallback
allocator can answer it too. Knob wiring lives in CONFIG (#85).

### Rejected: Mesh-style page compaction

Mesh (PLDI 2019) reclaims RSS by remapping physical pages whose live objects do
not overlap, avoiding any pointer rewrite. That win is largely moot here: because
IronCache controls the value index, copy-relocate already avoids invalidating
caller-visible handles, so we pay no pointer-rewrite cost that Mesh would save.
Mesh additionally requires meshed pages to share size class and have
non-overlapping live-bit offsets, a constraint harder to satisfy than a straight
intra-class copy. Mesh is documented as rejected.

## Open questions

- Whether per-slot-class structural bounding alone defers the reclaimer past M1
  (ship the introspection query and knobs, build the relocator later). Gated on
  the #41 / #42 allocator-under-cache-workload numbers: if fine-class jemalloc
  plus background purge holds RSS near the logical bound, M1 ships the contract
  and defaults OFF without the copy engine.
- Native sparse-query threshold shape: free-bytes ratio per slab vs absolute
  free count.
- Reclaimer scheduling: a dedicated reclaim slice per core vs cooperative
  per-shard time-slicing under the 25% cap.

## Acceptance and test hooks

- A native `is-this-slab-sparse` query is specified in the allocator
  introspection API and consumed by the reclaimer; it does not depend on the
  `je_get_defrag_hint` patch [redis-jemalloc-frag-hint].
- Throttle (1%-25% CPU) and thresholds (lower 10%, upper 100%, ignore-bytes
  100MB) are implemented with Redis-compatible defaults
  [redis-activedefrag-thresholds], default OFF [redis-activedefrag-default].
- `mem_fragmentation_ratio` (RSS / used_memory) [redis-fragmentation-ratio] is
  surfaced in INFO (#86) and drives reclaimer gating.
- Copy-relocate is single-core, lock-free, and rebinds the index entry without a
  caller-visible pointer change (a test fills then frees half a shard and
  asserts RSS drops toward used_memory).
- Mesh-style remapping is documented as rejected with the same-class /
  same-offset rationale.
- A decision is recorded on whether slab-class bounding alone defers the
  reclaimer build past M1, once #41 / #42 numbers land.

## References

- ADR-0006 (default allocator + accounting), ADR-0007 (memory ceiling, cache
  mode); issues #41, #42, #45, #35, #111, #85, #86; Mesh: Compacting Memory
  Management (PLDI 2019); `docs/research/memory-allocators.md`,
  `docs/research/redis-core.md`.
- Claims: [redis-fragmentation-ratio], [redis-jemalloc-lg-quantum-3],
  [redis-maxmemory-accounting], [redis-jemalloc-frag-hint],
  [redis-bundled-jemalloc-version], [tikv-jemallocator-version],
  [snmalloc-metadata-overhead], [redis-activedefrag-default],
  [redis-activedefrag-thresholds], [redis-active-defrag-jemalloc].
