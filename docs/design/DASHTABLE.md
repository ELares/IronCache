# Design: Dash-style extendible-hashing table (segmented, O(1) segment-local eviction)

Issue: #285. Decisions: ADR-0002 (shared-nothing thread-per-core), ADR-0003
(determinism / Env seam), ADR-0005 (per-shard unsynchronized index), ADR-0006
(allocator + accounting). Related: #35 (HASHTABLE.md, the current hashbrown
index), EVICTION.md (the freq-in-object pooled evictor), #284 (io_uring datapath),
#28.

## Goal and scope

IronCache competes on bytes-per-key and throughput-per-core. After the Redis and
DragonflyDB optimization campaigns (`docs/bench/OPTIMIZATION_LOG.md`) IronCache is
memory parity-or-better and faster per core than every competitor, with ONE
structural gap left versus Dragonfly. This spec designs a Dragonfly-style Dash
(extendible-hashing, segmented) per-shard table to replace
`hashbrown::HashTable<Entry>` (HASHTABLE.md, #35), closing that gap. It is the
LARGEST structural bet in the engine: a core store-table rewrite with hand-rolled
segment storage and a real throughput-regression risk versus hashbrown's SIMD
probe, so this document pins the design, the Big-O / memory analysis, the
integration constraints, and a staged, gated implementation plan BEFORE any code.

Scope: the directory + segment + bucket + slot layout, the fingerprint probe, the
incremental split growth, the O(1) segment-local cache eviction, and how all of it
preserves the frozen `Store` waist. Out of scope: persistent-memory crash
consistency (Dash's PM machinery; IronCache is DRAM-only, so the PM fences,
logging, and instant-recovery are dropped), and the io_uring datapath (#284).

## Why it matters: two "clear win" levers are the same lever

The #285 convergence, restated precisely:

- **(a) Uniform memory win: escape hashbrown's power-of-two DOUBLING TROUGH.**
  `hashbrown` (and Redis's `dict`) grow by DOUBLING the bucket array. Just past a
  doubling, a per-shard table sits at ~48% load: e.g. 1M keys / 2 shards =
  500k/shard, where roughly 9 of ~19 table bytes/key are empty buckets. Dragonfly's
  incrementally-split Dashtable never sits in that trough (it is why IronCache ties
  Dragonfly at 1M keys but WINS at 900k, and the apt 7.x-vs-8.x sweep confirmed both
  doubling engines oscillate with keycount). A segment-at-a-time growth eliminates
  the trough: the directory grows by one pointer per new segment (constant), and a
  segment is split only when full, so the steady-state load factor stays high
  (Dash reports >90% [dash-load-factor]) instead of oscillating between 50% and
  100%.

- **(b) True O(1) eviction.** The current pooled evictor (EVICTION.md) is
  O(N/CAP) amortized: `hashbrown::HashTable` exposes no cheap random bucket access,
  so each refill must SCAN the whole table for the coldest CAP candidates (the
  per-refill clone-all + sort within that scan was already removed by the
  bounded-heap refill, PR #434, but the O(N) scan itself remains). Dragonfly evicts
  one slot from the segment it is ALREADY touching on insert = O(1), zero per-key
  state [dragonfly-cache-mode-eviction]. A fair Dragonfly eviction head-to-head is even
  blocked at small scale (Dragonfly's 256 MiB/thread boot floor forces a multi-GB
  dataset, exactly where O(N/CAP) trails O(1)).

A Dash table provides BOTH at once: random segment/bucket access gives O(1)
segment-local eviction, and segment-at-a-time growth eliminates the doubling
trough. Per-slot metadata (~1 fingerprint byte) is roughly equal to hashbrown's
~1 control byte, so the density win does not cost metadata.

## Background: extendible hashing, Dash, and Dragonfly's Dashtable

Extendible hashing keeps a DIRECTORY of pointers to SEGMENTS. The top `G` bits of
a key's hash index the directory (`G` = the global depth); each segment carries a
LOCAL depth `L <= G`. A segment overflow triggers a SPLIT: a new segment is
allocated, the overflowing segment's records are repartitioned between the two by
their `(L+1)`-th hash bit, and the local depth bumps. When a segment's local depth
would exceed the global depth, the DIRECTORY doubles (cheap: it is a pointer array,
not the data) and `G` bumps. Multiple directory entries can point at one
lower-depth segment, so directory doubling moves no records.

Dash-EH [dragonfly-dash-paper-citation] refines this for scalability:

- **Segmentation with stash buckets.** A segment is a fixed array of NORMAL buckets
  plus a few STASH buckets that absorb overflow records that did not fit their
  target bucket, raising load factor before a split is forced.
- **Fingerprinting.** Each slot carries a 1-byte FINGERPRINT (a hash byte). A probe
  compares fingerprints first and only touches slots whose fingerprint matches, so a
  negative lookup or an insert uniqueness-check usually reads no keys at all. This is
  what lets buckets be large (more collisions tolerated, higher load factor) without
  paying cache misses per slot.
- **Bucketized probing.** A key targets one bucket and a neighbor PROBING bucket
  (balanced load); only on a double miss does it fall to a stash bucket.

Dragonfly's in-DRAM Dashtable [dashtable-segment-geometry] [dragonfly-cache-mode-eviction]
fixes concrete parameters and DROPS the PM crash-consistency machinery:

- A segment = **56 regular buckets + 4 stash buckets, 14 slots/bucket = 840
  records/segment**.
- The directory for ~1M items is ~1,200 segments = ~9,600 bytes of pointers,
  versus Redis's ~8 MB bucket array.
- Per-record metadata "tax" is short of **20 bits**, versus 64 bits for a Redis
  `dictEntry` pointer; net ~6-16 bytes overhead/item versus Redis's 16-32, ~30%
  leaner overall.
- **Segment-local cache eviction is O(1):** Dragonfly evicts only from a FULL
  segment (so the table never grows under cache pressure). A new item entering a
  full segment is placed at slot 0 of a stash bucket; the other slots shift right
  and the last slot's item is evicted. The bucket is a FIFO probationary queue.

## Design: the IronCache adaptation

### Directory + segments (DRAM-only)

A per-shard `DashTable<Entry>` holds:

- `directory: Box<[NonNull<Segment>]>` of length `1 << global_depth`. Multiple
  entries may alias one segment (local depth < global depth). The directory is the
  ONLY structure that doubles, and it carries no records, so a doubling is an
  `O(1 << G)` pointer copy, never a rehash.
- Each `Segment` is a fixed, cache-line-aligned block: `REGULAR` normal buckets +
  `STASH` stash buckets, `SLOTS` slots each, plus the per-slot fingerprint array and
  the segment's `local_depth`. Initial parameters mirror Dragonfly
  (`REGULAR=56, STASH=4, SLOTS=14`) and are tuning constants, validated by the
  head-to-head (below), not contract.

A slot stores IronCache's existing `Entry` (the #111 object: key bytes + value +
the 2-bit freq + expiry), unchanged: the Dash table changes only HOW entries are
indexed and laid out, not the object. The frozen `Store` waist
(`ValueRef`/`RmwEntry`/the side-traits, STORAGE_API.md) is preserved exactly so
the serve layer, encodings, and command handlers are untouched.

### Lookup / insert / delete

- **Hash split.** Reuse the existing key hasher for the directory index (top `G`
  bits) and derive the 1-byte fingerprint from a disjoint hash byte. `scan_hash`
  (ADR-0003) stays the SCAN cursor hash and is independent of the index hash, so
  SCAN cursor stability is unaffected by the table change.
- **Probe.** Index the directory, then within the segment compare the target +
  probing buckets' fingerprints, touching only matching slots; fall to the stash on
  a double miss. Negative lookups and insert uniqueness-checks usually read zero
  keys (the fingerprint win).
- **Insert.** Place into the least-loaded of (target, probing); on a full pair,
  use a stash slot; on a full segment, SPLIT (cache-mode: EVICT instead, below).
- **Delete.** Clear the slot + its fingerprint; segments do not shrink-merge in v1
  (a documented follow-up; Dragonfly also defers merge).

### Segment-local O(1) eviction (the freq-in-object integration)

Cache mode replaces split-on-full with EVICT-on-full, O(1), in the segment the
insert already touches, integrating the existing freq-in-object 2-bit frequency
(EVICTION.md) so it stays an approximate-LFU, NOT a pure FIFO:

- On inserting into a full segment, scan that segment's slots (a bounded
  `REGULAR*SLOTS + STASH*SLOTS` = O(1), one-or-two cache lines per bucket via the
  fingerprint array) for the slot with the LOWEST freq, breaking ties by the
  deterministic `(scan_hash, key)` order so two shards with identical state evict
  identically (ADR-0003), and evict it. This keeps the current victim QUALITY
  (coldest-by-freq) while dropping the O(N) table scan + the pool entirely: no
  `evict_pool`, no `refill_evict_pool`, no amortization bookkeeping.
- The 2-bit freq decay (the existing `dec_freq` aging) runs unchanged on access.

This is the (b) lever: eviction touches O(segment) = O(1) slots with zero per-key
side state, versus today's O(N/CAP)-amortized full-table scan.

### Accounting, determinism, and the gates it must hold

- **jemalloc accounting (ADR-0006).** Each segment is one sized allocation; the
  per-shard byte counter adds the segment size on allocation and the entry bytes on
  upsert, exactly as today, so `bytes_per_key` and the maxmemory admission gate are
  unchanged in semantics.
- **Determinism (ADR-0003).** SCAN walks segments + slots in directory-index then
  slot order, emitting `scan_hash`-ordered cursors as today; the eviction victim
  tie-break is the same `(freq, scan_hash, key, db)` total order. Two shards with
  identical histories produce identical SCAN cursors and identical eviction.
- **Unsafe + miri.** Raw segment slots and `NonNull` directory entries are
  hand-rolled `unsafe`; the whole table must pass `miri` under strict provenance,
  and the slot lifecycle (init / overwrite / evict / drop) must be leak-free and
  double-free-free.

## Big-O and memory

| | hashbrown (today) | Dash (this design) |
|---|---|---|
| lookup / insert / delete | O(1) expected | O(1) expected (fingerprint-gated) |
| growth step | DOUBLE + rehash all N: O(N) spike | split one segment: O(segment) = O(1), incremental |
| steady load factor | oscillates ~50-100% (doubling trough) | high + stable (no trough), Dash reports >90% |
| cache eviction | O(N/CAP) amortized table scan | O(1) segment-local |
| per-entry metadata | ~1 control byte | ~1 fingerprint byte (parity) |
| directory / table array | 2N buckets array (8 MB @ 1M) | N/840 segment pointers (~9.6 KB @ 1M) |

The memory win is the eliminated doubling trough (no ~48%-load empty-bucket waste)
plus the tiny directory; the eviction win is O(1) segment-local. Neither costs
extra per-entry metadata.

## Implementation plan (staged, gated)

The rewrite is too large and too perf-sensitive for one PR. Staged, each gated by
the FULL suite + `miri` + the per-PR perf-gate + the pinned-Linux and DragonflyDB
head-to-heads (`docs/bench/`), and each MUST NOT regress the current speed/latency
wins to gain memory (if it does, it is not worth it, per #285):

1. **`dashtable` crate, standalone.** Directory + segment + bucket + slot + the
   fingerprint probe + split growth, as a self-contained `unsafe` data structure
   with its own property tests (insert/lookup/delete/iterate parity against a
   `HashMap` oracle) and `miri`. NOT wired into the store. De-risks the unsafe core
   with zero blast radius.
2. **Cache-mode segment-local eviction** in the standalone crate, with a model test
   asserting the same victim as the current freq-in-object selection on shared
   inputs.
3. **Wire behind a `dashtable` feature flag** (default off): the store's index type
   becomes `DashTable<Entry>` under the flag, hashbrown otherwise. Run the full
   differential + property suites under BOTH, and the perf-gate + head-to-heads to
   measure throughput vs hashbrown's SIMD probe and memory vs the doubling trough.
4. **Flip the default** only once the head-to-heads show the uniform memory win AND
   no throughput regression; remove the flag in a later cleanup once soaked.

## Risks and open questions

- **Throughput vs hashbrown's SIMD probe** is the headline risk: hashbrown's
  `match_byte` SSE2/NEON group probe is extremely fast; Dash's per-bucket
  fingerprint scan must match it. The standalone-crate microbench (stage 1) gates
  this before any store wiring.
- **Validation needs a Linux box.** The acceptance head-to-heads (pinned-Linux,
  Dragonfly with its multi-GB boot floor) do not run on the macOS dev box, so
  stages 3-4 are CI/Linux-iterated and multi-session.
- **Segment merge on delete** is deferred (v1 never shrinks the directory); a
  delete-heavy workload could hold segments. Dragonfly also defers this.
- **Parameter tuning** (`REGULAR/STASH/SLOTS`, split threshold) is empirical; the
  Dragonfly numbers are the starting point, not contract.

## References

- Issues: #285, #35 (HASHTABLE.md), #28, #284; docs/design/HASHTABLE.md,
  docs/design/EVICTION.md, docs/design/STORAGE_API.md, docs/design/OBJECT_LAYOUT.md,
  docs/bench/OPTIMIZATION_LOG.md, docs/research/dragonfly.md.
- ADR-0002, ADR-0003, ADR-0005, ADR-0006.
- Claims: [dragonfly-dash-paper-citation] Lu, Hao, Wang, Lo, "Dash: Scalable Hashing on Persistent
  Memory", PVLDB 13(8):1147-1161, 2020 (http://www.vldb.org/pvldb/vol13/p1147-lu.pdf);
  [dash-load-factor] Dash >90% load factor (ibid., abstract);
  [dashtable-segment-geometry] DragonflyDB dashtable.md
  (https://github.com/dragonflydb/dragonfly/blob/main/docs/dashtable.md): 56 regular
  + 4 stash buckets, 14 slots, 840 records/segment, ~9.6 KB directory @ 1M, ~20-bit
  per-entry tax; [dragonfly-cache-mode-eviction] DragonflyDB "Dragonfly Cache Design"
  (https://www.dragonflydb.io/blog/dragonfly-cache-design): O(1) segment-local FIFO
  eviction from a full segment, ~30% leaner than Redis.
