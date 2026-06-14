# Design: Universal collection container and intset analog

Issue: #113 (split from #35). Decisions: ADR-0005 (per-shard unsynchronized map),
ADR-0018 (encoding thresholds), ADR-0009 (behavioral equivalence). Related: #111
(object layout), #112 (scalar encodings), #35 (index), #37 (conversion
thresholds, the numeric entry/value caps), #40 (`OBJECT ENCODING` reporting map,
the name strings), #134/#135 (large representations), #8 (harness). Thresholds
and reporting are two distinct deferrals: the numbers live in #37/ADR-0018, the
encoding-name strings live in #40; this spec restates neither.

## Goal and scope

Small list/hash/set/zset values share one compact contiguous container instead of
four bespoke ones, and all-integer sets get a sorted-array analog. This spec fixes
the container's byte layout, its scan model, and the property that makes it cheap
to mutate; it does not set the conversion thresholds (those are ADR-0018/#37) nor
the `OBJECT ENCODING` strings (#40). The container is a value living inside a
kvobj (#111); the index (#35, ADR-0005) is unaffected. Frozen against Redis 8.x
and Valkey 9.x as oracles (the house version line; specific layout numbers are
pinned to the exact source in each cited claim).

## Design

### One contiguous container (the listpack-equivalent)

- A single contiguous byte blob, the `pack`, backs the small encoding of hash,
  set, zset, and one list chunk, the way Redis collapsed ziplist into one
  listpack reused across types (a 6-byte header: 32-bit total bytes + 16-bit
  element count, then entries, then a 1-byte EOF) [redis-listpack-header-6-bytes].
  IronCache keeps that compact ~6-byte header (total-bytes + element-count) so
  the whole value is one allocation owned by one core (ADR-0005), with no
  per-element node and no chaining (contrast the 32-byte-per-node quicklist
  [redis-quicklist-node-32-bytes]).
- Type mapping: a hash stores field/value as adjacent entry pairs; a zset stores
  member/score pairs kept sorted by score; a set stores members; a list chunk
  stores elements in order. The count and the per-type pairing are the only
  type-specific logic; the byte layout and the scanner are shared.

### Cascade update designed out (fixed-width-or-length-prefixed entries)

- Redis's listpack stores a trailing back-length on each entry for reverse
  traversal [redis-listpack-header-6-bytes]; its ziplist ancestor (which listpack
  replaced) stored a prevlen that could change width when a neighbor grew,
  causing a chained recopy (the cascade update) on insert. IronCache designs this
  out: each entry is either fixed-width (integers, by a small type tag in the
  first byte) or a forward length-prefix only (a varint length then the bytes),
  with no back-pointer and no neighbor-dependent field. An insert or update
  therefore touches at most the entry plus a single tail memmove, never a width
  cascade across predecessors. Reverse iteration, when needed (RPOP, ZRANGE REV),
  is a forward scan to a remembered offset or a maintained tail cursor, not a
  per-entry back-length.

### SIMD-scannable linear layout

- Because entries are laid out head-to-tail with a leading type/length byte and
  no interior pointers, membership and field lookup are a linear forward scan,
  which vectorizes: a single core (ADR-0005) can probe the type tags and
  fixed-width keys with SIMD over the contiguous bytes, the same cache-friendly
  linear shape the index buckets use (7 entries per 64-byte bucket, Swiss-table
  inspired) [valkey-hashtable-bucket-layout]. SIMD here is IronCache's
  forward-looking design property, not a claim about the listpack source; the
  cited claim records the contiguous layout and bucket geometry, and the
  vectorized scan is the lever IronCache draws from it. At the small sizes these
  encodings are bounded to (ADR-0018), a linear SIMD scan beats a pointer-chasing
  structure, which is exactly why Redis keeps small collections in a flat pack at
  all.

### intset analog (all-integer sets)

- An all-integer set uses a sorted packed-integer array, mirroring Redis's intset
  (an 8-byte header of encoding + length then a sorted `contents[]` of
  int16/int32/int64 that upgrades width in place; binary-search lookup, O(n)
  insert) [redis-intset-layout]. IronCache keeps the same shape: a tiny header
  recording element width and count, then a width-homogeneous sorted array, width
  promoted in place when a wider integer is added. SISMEMBER is a branchless
  binary search (and SIMD-checkable for the narrow widths, again an IronCache
  scan property, not a property of the intset source); this is the all-integer
  fast path distinct from the mixed-member `pack` above.
- Selection: a set is an intset while every member parses as an integer; the
  first non-integer member converts it to the `pack` set encoding, and growth
  past the size thresholds converts either to the large hashtable set (ADR-0018,
  #134), matching Redis's intset/listpack/hashtable ladder
  [redis-set-encodings-thresholds].

### Conversion and reporting (owned elsewhere)

- When a collection exceeds the ADR-0018 thresholds (hash 512/64
  [redis-hash-max-listpack-entries-512], zset 128/64
  [redis-zset-max-listpack-entries-128], set intset 512 / listpack 128/64
  [redis-set-encodings-thresholds], list node ~8 KB
  [redis-list-max-listpack-size-neg2]) it converts to its large representation
  (#134/#135). Those numeric thresholds are the #37/ADR-0018 deferral and are not
  restated here. `OBJECT ENCODING` reports the Redis-compatible name
  (listpack/intset/quicklist/...) for the active container regardless of the
  shared internal layout [valkey-assert-encoding-vocab] (ADR-0009); the exact
  name-string reporting map is the separate #40 deferral.

## Open questions

- The in-entry integer type-tag set and the varint length encoding (how many
  fixed widths to special-case for SIMD vs a uniform length-prefix), tuned on the
  memory harness (#8).
- Whether the list small-chunk reuses this exact `pack` or a length-only variant
  sized to the ~8 KB node cap [redis-list-max-listpack-size-neg2], and how a
  large list chains chunks (#135).
- Whether a maintained tail cursor is worth its bytes for reverse-heavy commands
  vs an offset re-scan (#8).

## Acceptance and test hooks

- An insert into the middle of a `pack` performs at most one tail memmove and no
  predecessor rewrite (a cascade-free property test): the designed-out cascade
  never occurs.
- A small hash/set/zset/list occupies one allocation with the ~6-byte header and
  no per-element node, measured below the Redis listpack/quicklist baseline on the
  value-size corpus (memory harness #8, Efficient gate ADR-0016/0017).
- An all-integer set uses the intset analog with in-place width promotion;
  SISMEMBER is a binary search; adding a non-integer converts to the `pack`
  (oracle parity #97/#98).
- `OBJECT ENCODING` matches the pinned oracle across the size ladder and at each
  conversion boundary (#40, #97/#98).

## References

- ADR-0005, ADR-0009, ADR-0018; issues #35, #111, #112, #37, #40, #134, #135, #8,
  #97, #98.
- Claims: [redis-listpack-header-6-bytes], [redis-quicklist-node-32-bytes],
  [redis-intset-layout], [redis-set-encodings-thresholds],
  [redis-hash-max-listpack-entries-512], [redis-zset-max-listpack-entries-128],
  [redis-list-max-listpack-size-neg2], [valkey-hashtable-bucket-layout],
  [valkey-assert-encoding-vocab].
