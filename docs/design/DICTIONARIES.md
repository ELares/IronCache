# Design: ZDICT per-prefix dictionary training, versioning, and tagging

Issue: #55. Decisions: ADR-0015 (zstd-low codec, dictionaries are a zstd
strength), ADR-0021 (C-bound zstd crate supplies the ZDICT binding). Related:
#52 (framing and the dict-id field), #57 (PROVISIONAL defaults), #92 (off-path
decision), #7 (memory ceiling).

## Goal and scope

Generic per-value zstd barely moves sub-1 KB values: a ~1 KB record reaches only
2.8x alone but 6.9x against a pre-shared trained dictionary
[zstd-dictionary-small-data-6.9x]. This spec specifies how IronCache trains,
versions, tags, and hot-swaps per-prefix ZDICT dictionaries entirely off the
GET/SET hot path. In scope: the training trigger, sampling, dict-version-id
tagging and fail-closed resolve, atomic install with lazy re-encode, per-train
memory bounds, and metrics. Out of scope: the byte framing layout (COMPRESSION.md
#52 owns the dict-id field) and codec selection (ADR-0015). The ZDICT binding is
the same C-bound `zstd`/`zstd-sys` crate ADR-0021 already pins
[zstd-rust-crate-version]; this spec does not reopen the pure-Rust question.

## Design

### Per-prefix training off the hot path

- One active dictionary per key-prefix, grouping structurally similar values
  (session blobs, serialized objects, ID records) so shared structure (JSON
  keys, schema preambles) is front-loaded into the dictionary. Training uses
  ZDICT_trainFromBuffer (fastCover) [zstd-dictionary-default-size-110kb] and runs
  on a single background worker, never inline on SET. This adapts RocksDB's
  per-segment trained dictionary [rocksdb-block-size-4kb-dict] to per-prefix
  in-memory dictionaries trained from sampled live values.

### Bounded reservoir sampling and per-train memory

- The worker draws a reservoir sample of recent live values per prefix, capped
  in total sample bytes, so sampling memory and CPU are bounded and never block
  on a full prefix scan. The training arena is freed per train so there is no
  accrued leak; this explicitly avoids the apersson redis-compression-module
  failure mode, which trains synchronously and leaks ~6 MB per train
  [redis-zstd-module-ratio-7.7x]. The dictionary cache (retained versions) is
  bounded and counts against the memory ceiling (#7).

### Monotonic dict-version-id and fail-closed resolve

- Each prefix has a small monotonic dictionary version id. The active id is
  stamped into the COMPRESSION.md dict-id framing field per compressed value
  (#52), so the id is compact and versionable rather than coupling to the raw
  zstd dictID. On GET, the decoder resolves the stamped id to its dictionary; if
  that version is not resolvable the read fails closed (an explicit error, never
  a decode against the wrong dictionary), since decoding small values under a
  mismatched dictionary would silently corrupt.

### Atomic install and lazy re-encode

- A newly trained version is installed atomically and never invalidates values
  written under an older version: old dictionaries are retained until no live
  value references them. Re-encode is lazy, on the next write to a value, with an
  optional bounded background sweep; eager rewrite of all values on swap is
  rejected because it causes a write storm and read amplification. A retained
  version is freed once its reference count reaches zero, including when values
  pinning it are evicted (#48/#50).

### Metrics

- The OBSERVABILITY contract (#86, merged) carries the dictionary metrics:
  dictionary hit rate, training CPU time, training memory high-water mark, and
  live dictionary version count per prefix, so the off-path cost and the
  realized ratio are observable.

### PROVISIONAL defaults pending #57

- Starting sample size and dictionary size (~110 KB, the ZDICT/CLI default
  effective for values under ~100 KB) [zstd-dictionary-default-size-110kb], the
  retrain thresholds (new-value count, ratio-decay percentage, minimum sample
  count), and the per-prefix eligibility (only high-volume, structurally
  homogeneous prefixes earn a dictionary) are all PROVISIONAL and confirmed by
  the #57 value-size/compressibility survey. The spymemcached 16384-byte gate
  [spymemcached-default-compression-threshold] is adapted only as a coarse gate;
  trained dictionaries deliberately compress well below it, and the final
  threshold is #57's to set.

## Open questions

- Default sample size, dictionary size, and retrain thresholds, all PROVISIONAL
  pending #57 [zstd-dictionary-default-size-110kb].
- Maximum retained versions per prefix before a forced migration sweep, and
  lazy-only vs bounded background sweep with reference-count tracking.
- Whether evicted values that pinned an old dictionary are tracked precisely
  enough to free the dictionary promptly (#48/#50).

## Acceptance and test hooks

- A background worker trains a per-prefix dictionary from a bounded reservoir
  without blocking SET or GET, and per-train memory is bounded and released with
  no leak comparable to the apersson ~6 MB report [redis-zstd-module-ratio-7.7x].
- Each compressed value carries a per-prefix dict-version-id; GET resolves the
  correct dictionary or fails closed; installing a new version is atomic and
  never invalidates older-version values.
- On a representative small-value corpus, per-prefix dictionaries beat
  dictionary-less zstd, approaching the 6.9x reference
  [zstd-dictionary-small-data-6.9x] (benchmark #8).
- Metrics expose dictionary hit rate, training CPU time, training memory
  high-water mark, and live version count.

## References

- ADR-0015, ADR-0021; issues #55, #52, #57, #92, #7, #48, #50, #86, #8.
- Claims: [zstd-dictionary-small-data-6.9x], [zstd-dictionary-default-size-110kb],
  [rocksdb-block-size-4kb-dict], [redis-zstd-module-ratio-7.7x],
  [spymemcached-default-compression-threshold], [zstd-rust-crate-version].
