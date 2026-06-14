# Design: One-allocation per-key object layout

Issue: #111 (split from #35). Decisions: ADR-0005 (per-shard map), ADR-0006
(allocator/accounting), ADR-0008 (eviction metadata). Related: #35 (index), #112
(scalar encodings), #51 (TTL), #60 (snapshot version).

## Goal and scope

Specify the single heap allocation that represents one key/value entry (the
`kvobj`): the header, the embedded key bytes, an inline small value, and the
folded metadata bits. Sets the inline-key threshold and the fixed small-key
byte-overhead target wired into the memory harness (#8). The scalar value
encodings themselves are #112.

## Design

### One allocation

A `kvobj` is one allocation holding, in order: a packed header, the key bytes
inline, and (for small values) the value inline; large values point to a
separate allocation. This eliminates both the classic 16-byte `robj`
[redis-robj-header-16-bytes-classic] and the 24-byte `dictEntry`
[redis-dictentry-size] as separate allocations, the way Redis 8.x repacked its
header and embedded the key into the object [redis-kvobj-header-redesign-8x] and
Valkey embedded the key's SDS into the entry (about 8 bytes saved per key)
[valkey-embedded-key-8b]. The index (#35) stores a pointer to the kvobj plus a
hash tag; there is no other per-entry allocation.

### Packed header and metadata bits

The header folds, in a few bytes:

- type and encoding (so `OBJECT ENCODING` is answerable, ADR-0009),
- the eviction rank (the S3-FIFO 2-bit counter, ADR-0008), packed like Redis's
  24-bit LRU/LFU field [redis-lru-bits],
- a TTL-present bit and handle into the timing wheel (#51),
- a version stamp for the forkless-snapshot cut (#60),
- the key length and the inline-value length.

Folding these into the header (rather than side maps) is what keeps per-key
overhead low and is the reason the index needs no parallel metadata structures
(#35).

### Inline-key / SSO threshold

Keys up to an inline threshold are stored in the kvobj allocation; longer keys
spill to a trailing variable-length region of the same allocation (still one
allocation, just sized to fit). The threshold and the resulting fixed small-key
byte-overhead are a committed target measured by the memory harness (#8): the
goal is a per-key overhead well below Redis's robj+dictEntry sum on small items
[dashtable-populate-memory] (set the numeric target in #8, validated by the
Efficient gate ADR-0016/0017).

### In-place resize trade-off

Growing an inline value past the inline capacity reallocates the kvobj (or
promotes the value to an out-of-line allocation). Because the shard is
single-owner (ADR-0005), the reallocation is a plain move with the index pointer
updated in place; no reader can hold a stale pointer (ADR-0004). The trade-off
(reallocate-on-grow vs reserve slack) is tuned against write-heavy workloads on
the harness.

## Open questions

- The exact inline-key and inline-value thresholds (bytes), and whether they are
  fixed or size-class-aligned to the allocator (ADR-0006), tuned in #8.
- Header bit budget: whether the version stamp shares bits with the eviction rank
  or gets its own word (interacts with #60's cut granularity).

## Acceptance and test hooks

- A small key/value (for example 16 B key, 16 B value) occupies one allocation
  with a measured fixed overhead below the committed target (memory harness #8).
- `OBJECT ENCODING`, TTL, eviction rank, and snapshot version are all readable
  from the single kvobj with no side lookup (a layout test).
- A grow-in-place then grow-past-inline sequence updates the index pointer with
  no dangling reference (property test #98).

## References

- ADR-0004, ADR-0005, ADR-0006, ADR-0008, ADR-0009; issues #35, #112, #51, #60,
  #8, #98.
- Claims: [redis-robj-header-16-bytes-classic], [redis-dictentry-size],
  [redis-kvobj-header-redesign-8x], [valkey-embedded-key-8b], [redis-lru-bits],
  [dashtable-populate-memory].
