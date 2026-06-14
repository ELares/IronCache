# Design: Transparent value compression framing and the GET decode path

Issue: #52. Decisions: ADR-0015 (zstd-low default codec, lz4/none policy),
ADR-0021 (C-bound zstd crate for the static binary). Related: #55 (per-prefix
dictionaries), #57 (size/entropy survey), #56 (in-place mutation), #92
(off-path compression decision), #7 (maxmemory ceiling), #48/#50 (eviction).

## Goal and scope

Redis stores values verbatim and ships no transparent in-memory value
compression [redis-no-transparent-value-compression], so large JSON, protobuf,
and HTML payloads sit in RAM uncompressed. IronCache compresses value bytes
transparently to win RAM while keeping latency-critical keyspaces on a
zero-overhead path. This spec owns the stored framing, the GET decode contract,
the maxmemory accounting rule, and the per-keyspace activation gate. It does NOT
re-decide the codec (ADR-0015 owns zstd-low with lz4/none as policy) or how zstd
is linked (ADR-0021 owns the C-bound `zstd`/`zstd-sys` crate). Scope is value
bytes only: keys, the hash table (#35), and the wire protocol are out of scope.

## Design

### Stored framing

- Every compressed value carries a fixed compact header ahead of the payload:
  a codec id (which ADR-0015 codec produced the bytes), a dictionary id (the
  per-prefix dict-version-id from #55, or a none sentinel), the uncompressed
  length, and an incompressible flag. The dict id is the internal monotonic id
  defined in DICTIONARIES.md (#55), not a raw zstd dictID, so framing does not
  couple to zstd internals.
- The incompressible flag is set when the codec fails to shrink the value: the
  raw bytes are stored verbatim and the flag records that no decode is needed.
  This is what lets the size/entropy gate (#57/#92) and the codec both bail
  without a second format. zstd output stays byte-for-byte reference compatible
  per ADR-0021, so the payload remains a standard zstd frame when codec=zstd.

### GET decode on the hot path

- GET reads the header and branches once. If the incompressible flag is set or
  codec=none, the stored bytes are returned directly: no allocation, no codec
  call, branch-predictable for the common uncompressed keyspace. Otherwise the
  single compressed branch resolves the codec id and dict id and decodes. This
  honors the access asymmetry ADR-0015 relies on: a value is compressed once on
  SET but decoded on every GET, and zstd decompression speed is near
  level-independent (2.896 ratio at 1550 MB/s decompress for zstd -1)
  [zstd-silesia-benchmark-l1], so a high-ratio low level never taxes reads.
- The uncompressed length in the header sizes the output buffer in one shot, so
  the compressed branch makes a single allocation and one codec call.

### maxmemory accounting counts compressed bytes

- Redis measures maxmemory against allocator logical bytes
  [redis-maxmemory-accounting]. IronCache adapts this: the bytes counted toward
  the ceiling (ADR-0007, #7) are the COMPRESSED stored bytes, because that is
  what actually occupies RAM and is truthful to the headline memory metric.
- Eviction-fairness note: counting compressed bytes skews eviction pressure
  toward compressible keyspaces (a highly compressible key occupies little
  budget and is therefore cheaper to keep), so the eviction engine (#48/#50)
  sees per-key cost in post-compression bytes. The alternative, counting
  uncompressed bytes, would be fairer across keyspaces but would lie about RAM
  and defeat the purpose; it is rejected. The fairness consequence is tracked
  with the off-path decision model in #92.

### Per-keyspace activation and the size gate

- Compression is opt-in per keyspace and defaulted OFF for latency-critical
  keys, so the zero-overhead GET path is the default and operators turn
  compression on only where the RAM win pays. A per-keyspace size threshold
  gates SET so small values never pay codec or framing cost; the inherited
  spymemcached 16384-byte client default [spymemcached-default-compression-threshold]
  is explicitly NOT adopted as the server policy and the evidence-based
  threshold is owned by #57.

### zstd long-range mode excluded

- zstd long-distance matching defaults to a 128 MiB window
  [zstd-long-range-window-default-27]; it is excluded from the per-value path.
  Per-value entries are far too small to benefit and the large window inflates
  decompressor memory on every GET. Per-segment compression amortized across a
  block (RocksDB amortizes dictionary and framing across a 4 KB block
  [rocksdb-block-size-4kb-dict]) is the small-value alternative studied in #57,
  not the default here.

## Open questions

- Final compressed-vs-uncompressed accounting and the measured eviction-fairness
  skew on the cachemon corpus (#92).
- Per-value vs per-segment for the small-value regime (#57, RocksDB block
  pattern [rocksdb-block-size-4kb-dict]).
- Interaction with in-place mutations APPEND/SETRANGE, which must decode, mutate,
  and re-frame or fall back to uncompressed (#56).

## Acceptance and test hooks

- Framing round-trips: codec id, dict id, uncompressed length, and incompressible
  flag are recovered exactly; an incompressible value stores raw and GET returns
  it with no codec call.
- GET is single-branch decode; a hot-path lint asserts the uncompressed/none
  path makes no codec call and no allocation, and the benchmark (#8) shows
  near-zero uncompressed-path overhead.
- maxmemory accounts compressed bytes; an eviction test confirms per-key budget
  reflects post-compression size [redis-maxmemory-accounting].
- Default-off per keyspace for latency-critical keys; the size gate skips small
  values; zstd long-range mode is never invoked on the per-value path
  [zstd-long-range-window-default-27] (conformance #95/#97).

## References

- ADR-0015, ADR-0021; issues #52, #55, #56, #57, #92, #7, #48, #50, #8, #95, #97.
- Claims: [redis-no-transparent-value-compression], [redis-maxmemory-accounting],
  [spymemcached-default-compression-threshold], [zstd-long-range-window-default-27],
  [rocksdb-block-size-4kb-dict], [zstd-silesia-benchmark-l1].
