# Design: Per-shard hash table and index

Issue: #35. Decisions: ADR-0005 (per-shard unsynchronized hashbrown map),
ADR-0006 (allocator + accounting), ADR-0018 (encoding thresholds). Related:
#111 (object layout), #112 (scalar encodings), #34 (storage API), #36.

## Goal and scope

IronCache competes on bytes-per-key and throughput-per-core, and both are decided
by the per-shard index and the object it stores. This is the headline layout: the
bucket table, its growth policy, and how per-entry metadata is folded in. The
object itself is #111 and the value encodings are #112. Frozen against Valkey 9.x
and Redis 8.x as oracles.

## Why it matters

Redis's classic 16-byte `robj` header [redis-robj-header-16-bytes-classic] plus a
24-byte `dictEntry` [redis-dictentry-size] dominate small-value workloads: on 20M
small items Dragonfly used 1 GB versus Redis 6's 1.73 GB, about 1.0 GB of it
metadata [dashtable-populate-memory]. Per-key overhead must be designed in.

## Design

### Bucket table

- The index is, per assigned slot, a single-threaded open-addressing SwissTable
  (stock `hashbrown::HashMap`, ADR-0005), owned by one core, no atomics on the hot
  path. A shard owns the tables for the slots assigned to it (the 16384-slot
  space, ADR-0011), and each slot has its own small table. This follows Valkey's
  move to cache-line-bucket open-addressing (7 entries per 64-byte bucket, ~91
  percent max fill) [valkey-hashtable-replaces-dict] [valkey-hashtable-bucket-layout]
  and its per-slot dictionaries [valkey-per-slot-dict-16b], and Dragonfly's
  Dashtable [dashtable-overhead-bytes]. Per-slot stock tables are chosen over
  Dash's extendible directory because the slot already partitions the keyspace
  finely, so a flat table per slot is enough and needs no segment-level
  extendibility (the Dash segment geometry [dashtable-segment-geometry] is
  studied, not copied).
- Keys are stored in the value object (no separate `dictEntry`): the table holds
  the kvobj pointer (and a small inline hash tag for probing), so there is no
  per-entry chaining allocation, the way Redis 8.x already avoids a `dictEntry`
  for single-key buckets via pointer tagging [redis-dict-bucket-pointer-tagging]
  and Valkey embeds the key [valkey-embedded-key-8b]. The full object layout is
  #111.

### Growth and rehash

- Each per-slot table grows by stock `hashbrown` power-of-two resize (a single
  all-at-once rehash on the triggering insert, which is what `hashbrown`
  provides). Because there are 16384 slots, a single slot's table holds only its
  fraction of the shard's keys, so that resize is bounded and cheap. This is
  exactly the property per-slot dictionaries buy: a rehash is confined to one
  slot, and Valkey measured the per-slot split saving ~16 bytes/entry
  [valkey-per-slot-dict-16b]. It is why IronCache does not need Redis's bespoke
  two-table incremental rehash with its ~48N peak [redis-dict-two-table-rehash]
  [redis-dictentry-size], and does not need a custom incremental table on top of
  stock `hashbrown`. The resize is a plain `hashbrown` operation on the owning
  core with no synchronization (ADR-0005); the latency bound is the size of one
  slot's table, kept small by the slot count (and, if a slot still grows hot, by
  the hot-shard mitigation, #32/#170, not by a bigger table).

### Per-entry metadata

- The eviction rank (S3-FIFO 2-bit counter, ADR-0008), the TTL presence/handle
  (#51), and a version stamp for the forkless snapshot cut (#60) are folded into
  the kvobj's metadata bits (#111), not stored in separate maps, mirroring how
  Redis packs LRU/LFU into the 24-bit object field [redis-lru-bits]
  [redis-lfu-counter-encoding]. `OBJECT ENCODING` reports the Redis-compatible
  encoding name [valkey-assert-encoding-vocab] regardless of the internal layout
  (ADR-0009 behavioral equivalence).

### Collections

- Small collections use compact inline encodings up to the ADR-0018 thresholds,
  then convert to the large representations (#113/#134/#135). The universal
  container and intset analog are #113; this index design only fixes that a
  collection value is a kvobj like any other, with its encoding surfaced through
  `OBJECT ENCODING`.

## Open questions

- The inline hash-tag width in the bucket (probe speed vs bytes), tuned against
  the memory harness (#8).
- The per-slot table size cap (the tail-latency lever for a stock-hashbrown
  resize) and whether a very hot slot sheds load across the shard, measured on
  the harness (#8) and tied to the hot-shard work (#32/#170).

## Acceptance and test hooks

- Bytes-per-stored-item at a fixed hit ratio is below Redis 8 on the value-size
  corpus (the Efficient gate, ADR-0016/0017), measured by the memory harness (#8).
- A resize touches only one slot's table, never the shard's whole keyspace, and
  the per-slot resize cost stays within the tail-latency budget at the target
  per-slot key count (a tail-latency test). The per-slot table size is the lever
  if that budget is threatened.
- `OBJECT ENCODING` matches the pinned oracle for every type/size (#97/#98).

## Addendum: bucket geometry and rehash policy (#110)

#110 (decomposed from #35) asked to pin the per-shard bucket geometry, an
"incremental two-table rehash with a per-operation budget," and correct expand
and shrink thresholds. The geometry is already fixed above and restated here; the
per-op-budget two-table machinery is **rejected**; expand is taken from the
bucket layout and shrink is an IronCache addition (stock `hashbrown` does not
auto-shrink); the only genuine open geometry parameter is the inline hash-tag
width.

### Bucket geometry (confirmed, not re-decided)

- The per-slot table is the cache-line-bucket open-addressing layout this spec
  already adopts from Valkey: 7 entries per 64-byte bucket, a presence bitmask
  plus 7 stored hash bytes, Swiss-table-inspired SIMD probing, soft max fill
  ~91.43% [valkey-hashtable-bucket-layout], realized by stock `hashbrown`
  (ADR-0005). The 7-slot bucket, the presence mask, and the stored hash bytes are
  properties of that layout, not a new structure to build. The specific bucket
  numbers are pinned at Valkey 8.1.8 [valkey-hashtable-bucket-layout]; the spec's
  "Valkey 9.x" oracle label is the house version line, while the 7-entry,
  91.43%-fill geometry is the 8.1.8 source.

### Rejected: incremental two-table rehash with a per-operation budget

- #110's premise (a custom dual-table rehash that migrates a budgeted number of
  buckets per operation) is **declined**. It directly contradicts the chosen
  stock-`hashbrown` all-at-once power-of-two resize (ADR-0005, "Growth and
  rehash" above), and that choice stands.
- Rationale: the 16384-slot partitioning (ADR-0011) bounds each per-slot table to
  a small fraction of the shard's keys, so a single all-at-once resize is small
  and bounded, and its latency is the size of one slot's table, not the shard's.
  Incremental rehash exists in Redis precisely because its one global dict is
  large and a stop-the-world rehash would stall; that is why Redis pays for the
  bespoke two-table scheme with its ~48N peak [redis-dict-two-table-rehash]
  [redis-dictentry-size]. Valkey's per-slot dictionaries already shrink the unit
  of rehash to one slot (and saved ~16 bytes/entry doing so)
  [valkey-per-slot-dict-16b]; IronCache inherits that bound, so the per-op budget
  buys nothing.
- Costs avoided: a dual-table read path (every GET probing both old and new
  tables until migration finishes), a migration cursor and budget accountant on
  the hot path, and a second live allocation during rehash, all for tables small
  enough that a single resize already meets the tail-latency budget. The
  extendible-directory alternative (Dash segment geometry) is likewise studied,
  not copied [dashtable-segment-geometry], for the same partitioning reason. If a
  slot's resize ever threatens the budget, the lever is the per-slot table size
  cap and the hot-shard mitigation (#32/#170), not a two-table machine.

### Expand and shrink thresholds

- Expand is the bucket layout's soft max fill (~91.43%)
  [valkey-hashtable-bucket-layout], the trigger stock `hashbrown` already applies
  on the owning core; IronCache does not hand-roll an expand trigger.
- Shrink is different and must be stated plainly: stock `hashbrown` does **not**
  automatically shrink a table when entries are removed (it releases capacity
  only on an explicit `shrink_to_fit`/`shrink_to`). An automatic shrink-on-delete
  is therefore an IronCache addition, not a property inherited from `hashbrown`.
  #110 references Redis's 1/32 shrink trigger [redis-dict-resize-ratios] as the
  prior-art shape; IronCache will provide an equivalent low-watermark shrink, but
  the exact watermark and how often a per-slot table is allowed to shrink (the
  churn vs resident-bytes trade) is harness-tuned (#8), not asserted here, and is
  listed as an open sub-item below. Whatever the policy, `OBJECT ENCODING` and
  observable behavior are unchanged by any resize [valkey-assert-encoding-vocab]
  (ADR-0009).

### Remaining open sub-items

- The inline hash-tag width stored beside the kvobj pointer in a bucket (probe
  speed vs bytes per entry), tuned against the memory harness (#8). This is the
  same open question listed above and is harness-tied.
- The automatic shrink-on-delete watermark and shrink cadence for a per-slot
  table (IronCache adds this on top of `hashbrown`, which does not auto-shrink),
  also harness-tuned (#8), grounded against Redis's 1/32 trigger
  [redis-dict-resize-ratios].

## References

- ADR-0005, ADR-0006, ADR-0008, ADR-0009, ADR-0011, ADR-0018; issues #111, #112,
  #113, #134, #135, #34, #51, #60, #8, #97, #98.
- Claims: [redis-robj-header-16-bytes-classic], [redis-dictentry-size],
  [dashtable-populate-memory], [valkey-hashtable-replaces-dict],
  [valkey-hashtable-bucket-layout], [dashtable-overhead-bytes],
  [dashtable-segment-geometry], [redis-dict-bucket-pointer-tagging],
  [valkey-embedded-key-8b], [redis-dict-two-table-rehash],
  [valkey-per-slot-dict-16b], [redis-lru-bits], [redis-lfu-counter-encoding],
  [valkey-assert-encoding-vocab].
