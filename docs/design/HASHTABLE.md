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

- The per-shard index is a single-threaded open-addressing SwissTable
  (`hashbrown`, ADR-0005), owned by one core, no atomics on the hot path. This
  follows the direction Valkey took replacing its chaining dict with a
  cache-line-bucket open-addressing table (7 entries per 64-byte bucket, ~91
  percent max fill) [valkey-hashtable-replaces-dict] [valkey-hashtable-bucket-layout]
  and Dragonfly's Dashtable [dashtable-overhead-bytes], but as a flat per-shard
  table rather than an extendible directory (the shard already partitions the
  keyspace, so segment-level extendibility is unnecessary; the Dash segment
  geometry [dashtable-segment-geometry] is studied, not copied).
- Keys are stored in the value object (no separate `dictEntry`): the table holds
  the kvobj pointer (and a small inline hash tag for probing), so there is no
  per-entry chaining allocation, the way Redis 8.x already avoids a `dictEntry`
  for single-key buckets via pointer tagging [redis-dict-bucket-pointer-tagging]
  and Valkey embeds the key [valkey-embedded-key-8b]. The full object layout is
  #111.

### Growth and rehash

- The table grows by power-of-two resize. To avoid the latency spike of a
  stop-the-world rehash (and Redis's two-table incremental rehash with its ~48N
  peak [redis-dict-two-table-rehash] [redis-dictentry-size]), resize is
  incremental: a new table is allocated and entries are migrated in bounded
  batches on subsequent operations, with reads checking both tables during the
  window. Because the shard is single-owner, no synchronization is needed across
  the migration (ADR-0005).
- Per-slot dictionaries (one table per the 16384 slots) are the slot-ready layout
  of ADR-0011; Valkey showed this saves ~16 bytes/entry and bounds rehash to one
  slot [valkey-per-slot-dict-16b]. IronCache's shard owns its slots' tables.

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
- Incremental-resize batch size (latency vs migration duration), measured on the
  harness.

## Acceptance and test hooks

- Bytes-per-stored-item at a fixed hit ratio is below Redis 8 on the value-size
  corpus (the Efficient gate, ADR-0016/0017), measured by the memory harness (#8).
- No operation incurs a full-table stop-the-world rehash; resize latency stays
  bounded (a tail-latency test).
- `OBJECT ENCODING` matches the pinned oracle for every type/size (#97/#98).

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
