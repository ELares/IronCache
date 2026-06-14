# Experiment: Dash-style segmented extendible-hash index vs the M1 per-slot SwissTable

Issue: #38. Provisional decision: ADR-0005 pins the per-shard index, and
docs/design/HASHTABLE.md (#35) records the Dash geometry as studied, not copied.
This doc records the M2 bake-off that gates #38; it does not pre-commit the
segmented-extendible design, which HASHTABLE.md currently defers.

## Provisional decision (already pinned)

ADR-0005 (Accepted, issue #36) makes the per-shard primary store an
unsynchronized stock `hashbrown::HashMap`, owned by one core, no atomics on the
hot path. docs/design/HASHTABLE.md (#35) realizes that as a per-slot SwissTable
over the 16384-slot space and states the rationale plainly: the slot already
partitions the keyspace finely, so a flat table per slot needs no segment-level
extendibility, and the Dash segment geometry [dashtable-segment-geometry] is
studied, not copied. The bucket layout HASHTABLE.md adopts is the Valkey
cache-line bucket, 7 entries per 64-byte bucket with a presence bitmask and 7
stored hash bytes at ~91.43% soft max fill [valkey-hashtable-bucket-layout],
realized by stock `hashbrown`. That is the M1 baseline this experiment measures
against. This doc does not re-decide it; it records the M2 comparison that could
refine it.

This is the M2 refinement #38 reserves: a segmented extendible hash in the style
of Dragonfly's Dash table, itself based on the Dash persistent-memory hashing
paper [dragonfly-dash-paper-citation], adopted only if benchmarks prove the added
complexity earns its keep. #38 is a design issue, but its concrete geometry is
deliberately left undesigned: ADR-0005 has not been beaten, so HASHTABLE.md keeps
the segmented table studied, not copied, and the five #38 open decisions stay
open until a measured win justifies committing them. The candidate's published
compile-time geometry is 60 buckets per segment (56 regular plus 4 stash), 14
slots per bucket, 840 records per segment [dashtable-segment-geometry]; IronCache
would pin segment geometry to its own cache-line width and fingerprint scan
following the Dash split discipline [dragonfly-dash-paper-citation] rather than
reproduce that verbatim. The target per-item overhead is the 6 to 16 byte band
Dragonfly reports for Dashtable [dashtable-overhead-bytes], treated here as a goal
to verify by measurement, not an assumption.

## Why this is harness-blocked

The decision rule needs tail latency under growth and resident bytes per item,
both measured at equal conditions. That requires three things that do not exist
yet:

- The benchmark and memory-model harness of ADR-0016 (per-core throughput,
  bytes-at-fixed-hit-ratio, open-loop tail latency); the harness is #8.
- A working segmented-extendible index implemented behind the same per-slot
  index interface as the M1 per-slot SwissTable, so only the structure varies.
- A growth driver that forces table resizes under concurrent read load and a
  concurrent snapshot reader, so the split cost and bucket isolation for the
  forkless snapshot cut (#60) are observed, not argued.

Until the harness runs both structures under one accounting model, any ranking is
a citation comparison across mismatched home corpora. Dragonfly's reported
overhead and populate behavior are from its own machine and corpus
[dashtable-overhead-bytes] [dashtable-populate-memory]; they set the target, not
the verdict.

## Experiment to run

Corpus and workload:

- A small-value KV corpus in the 6 to 16 byte target band [dashtable-overhead-bytes],
  so the per-item overhead audit is on the values where metadata dominates, plus
  a mixed-size corpus so the result transfers off the small-value extreme.
- A populate-then-grow workload that drives each per-slot table from empty across
  several resize boundaries, mirroring the populate baseline Dragonfly used for
  its overhead audit [dashtable-populate-memory].
- A concurrent-growth pattern: continuous inserts forcing splits while a snapshot
  iterator (#60) walks the same shard, so bucket-level isolation is exercised
  under load rather than asserted.

Fixed parameters, held identical across both structures:

- Value codec, allocator and accounting (ADR-0006), shard count and pinning,
  hardware, and the hit ratio at which resident bytes are sampled.
- The 16384-slot assignment (ADR-0011) and shared-nothing shard layout (ADR-0002),
  so the only variable is the per-slot index structure.
- The metadata bit budget actually populated, version stamp, expiry presence, and
  eviction rank, so bytes per item is measured with all metadata enabled, not
  with empty reserved bits.
- ADR-0016 measurement methodology (open-loop, coordinated-omission-corrected).

Varied parameters:

- Index structure: the M1 per-slot stock-`hashbrown` SwissTable
  [valkey-hashtable-bucket-layout] versus the Dash-style segmented-extendible
  table, geometry pinned to IronCache cache-line width and fingerprint scan
  following the Dash split discipline [dragonfly-dash-paper-citation] rather than
  the verbatim 840-record segment [dashtable-segment-geometry].
- Per-slot key count, swept across the resize boundaries that trigger growth.
- Segment-side parameters for the candidate only: slots per bucket and buckets
  per segment, stash bucket count, and the split-trigger threshold.

Measured:

- p99 and p999 insert latency across each growth boundary (tail latency under
  growth), to see whether per-segment split bounds the spike that an all-at-once
  per-slot `hashbrown` resize produces.
- Resident bytes per item at the fixed hit ratio with all metadata enabled,
  reported against the 6 to 16 byte target [dashtable-overhead-bytes] and the
  populate baseline [dashtable-populate-memory]; the target is confirmed or
  refuted, never assumed.
- Split work and peak extra memory during a resize, to verify the candidate
  bounds both to one segment versus the per-slot table doubling on resize.
- Snapshot-iterator isolation under concurrent growth (#60): whether a split
  disturbs an in-flight iteration, on each structure.
- Hot-lookup key compares per probe, to confirm the SIMD fingerprint scan filters
  candidates before a full compare on the candidate as it already does on the M1
  bucket layout [valkey-hashtable-bucket-layout].

Decision rule:

- Adopt the Dash-style segmented-extendible index for M2 only on a measured win
  that justifies its complexity: it must bound the growth-time tail spike and the
  resize memory peak to one segment AND not regress resident bytes per item AND
  not regress hot-lookup throughput, versus the M1 per-slot SwissTable.
- Otherwise ADR-0005 stands and HASHTABLE.md is unchanged: the per-slot stock
  `hashbrown` SwissTable remains the index, the Dash geometry remains studied,
  not copied [dashtable-segment-geometry], and the #38 open decisions stay
  deferred.

## What would change the decision

- The candidate bounds the resize tail spike and memory peak to one segment by a
  margin large enough to matter under the concurrent snapshot reader, which a
  single all-at-once per-slot `hashbrown` resize cannot, and does so without
  costing resident bytes per item.
- The measured bytes per item with all metadata enabled lands inside the 6 to 16
  byte band [dashtable-overhead-bytes] on the candidate while the M1 per-slot
  table sits above it on the same corpus, charged against the populate baseline
  [dashtable-populate-memory].
- The snapshot iterator (#60) needs bucket-isolated, bounded-split growth that the
  per-slot all-at-once resize cannot provide cleanly, making segment-level split
  a correctness convenience rather than only a latency one.
- Conversely, if the per-slot partitioning already keeps each `hashbrown` resize
  small enough that its tail spike and memory peak meet the budget, the candidate
  buys nothing and ADR-0005 stands.

## References

- ADR-0005 (per-shard unsynchronized `hashbrown` map; issue #36); ADR-0002
  (shared-nothing thread-per-core); ADR-0011 (16384-slot space); ADR-0006
  (allocator and accounting); ADR-0016 (headline metrics and methodology, #7).
- docs/design/HASHTABLE.md (#35, the M1 index; Dash geometry studied, not copied).
- Issues: #38 (this experiment; its concrete geometry stays deferred until the
  bake-off justifies it); #35 (parent index design); #60 (forkless snapshot cut
  and its bucket isolation); #8 (benchmark and memory harness); #1 (vision);
  #32 / #170 (hot-shard mitigation, the alternative tail lever).
- Claims: [dashtable-segment-geometry], [dragonfly-dash-paper-citation],
  [valkey-hashtable-bucket-layout], [dashtable-overhead-bytes],
  [dashtable-populate-memory].
