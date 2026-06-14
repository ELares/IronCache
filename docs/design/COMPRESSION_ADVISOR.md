# Design: Off-path per-value compression decision model

Issue: #92. Decisions: this spec owns the offline classifier, its feature set,
its CPU-per-byte-vs-memory-saved objective, the encoded per-class snapshot the
hot path reads, and the build/refresh loop; it does NOT re-decide ADR-0015
(zstd-low default codec, lz4/none policy) or ADR-0021 (C-bound zstd crate), both
owned by COMPRESSION.md (#52). Related: #88 (parent advisor epic, decomposed),
#126 (the per-shard background advisor, ADVISOR.md, that hosts this loop), #52
(COMPRESSION.md, the SET path that consumes the snapshot and the framing it
stamps), #55 (DICTIONARIES.md, the per-prefix dict-version-id this table
references), #57 (the value-size/compressibility survey that supplies the
buckets and the heuristic baseline numbers), #91 (ADVISOR_SAFETY.md, the
kill-switch and atomic snapshot swap), #154 (ADVISOR_PROMOTION.md, the
beats-baseline-on-replay gate), #13 (no per-request inference).

## Goal and scope

The compression epic #52 ships a threshold-gated SET path: one size compare, one
codec call, one framing write. This spec specifies the brain that produces the
threshold and the codec choice so the hot path never decides policy at runtime.
A per-request entropy scan plus codec selection is exactly the per-access
inference IronCache rejects on the data path (#13): learned-policy work belongs
off the hot path and behind an opt-in [parrot-imitation-belady-icml20]
[lecar-regret-minimization-smallcache]. The model runs only in the background
advisor (#126), evaluates each value class against a cost objective offline, and
publishes a versioned snapshot the SET path reads atomically and applies with a
single array lookup.

In scope: the value-class key, the cheap feature set, the CPU-per-byte-vs-
memory-saved objective, the per-class `{codec, level, dict-id, size-floor}`
output tuple and its `u8`-codec + floor-array encoding, the offline build and
refresh loop, the size-plus-entropy heuristic baseline the classifier must beat,
and the kill-switch back to that heuristic. Out of scope: the codec
implementations, the stored framing, and the maxmemory accounting rule (#52,
ADR-0015, ADR-0021); the per-prefix dictionary lifecycle and the dict-version-id
itself (#55); the value-size/compressibility survey that fills the buckets and
fixes the heuristic numbers (#57); the snapshot RCU swap mechanism and the
kill-switch machinery (#91); the promotion/replay gate (#154). This spec does
NOT re-decide which codec is default or how zstd is linked: it selects among the
ADR-0015 codec points, it does not add or rank codecs.

## Design

### The value class is the unit of decision, computed offline

- The classifier is keyed by a value class, not a value. A class is
  (key-prefix, size bucket, 8-bit entropy estimate), where the size buckets are
  the #57 survey buckets and the entropy estimate is sampled at training time
  over a fixed-length head of representative values, never per request. The
  prefix is the same per-prefix grouping DICTIONARIES.md (#55) trains a
  dictionary against, so a class maps cleanly to at most one active dictionary.
  The cross product is bounded by capping size buckets to the #57 set and
  quantizing entropy to a single `u8`, so the table is enumerable and the class
  count is a documented constant rather than a function of the keyspace.

### The objective is CPU-per-byte spent vs memory saved, not ratio

- For each class the model scores every candidate codec point on the real cost
  metric, not raw compression ratio. This borrows Baleen, which optimizes a
  cost metric (disk-head time) rather than hit ratio [baleen-flash-admission-fast24]:
  the IronCache analogue is net bytes saved after framing overhead per compress
  CPU millisecond, the same decision rule the #57 survey measures and exports.
  Ratio alone over-compresses incompressible blobs and burns CPU for no RAM win,
  which is why a high-entropy class resolves to codec=none and the incompressible
  framing path (#52). A class is worth compressing only where the objective is
  positive at the chosen point.

### Candidate codec points come from ADR-0015, the model only picks among them

- The candidate set is the ADR-0015 frontier, not a new codec menu: zstd at a
  low or `--fast` level for ratio-leaning classes (`--fast=3` gives ratio 2.241
  at 635/1980 MB/s [zstd-fast-modes-benchmark], level -1 gives 2.896 at
  510/1550 [zstd-silesia-benchmark-l1]), lz4 for throughput-leaning classes
  (ratio 2.101 at 780/4970 MB/s [lz4-silesia-benchmark], and lz4_flex stays
  within about 10% of the C reference in safe mode [lz4-flex-safe-vs-c]), and
  none for incompressible classes. Per-class selection captures this frontier; a
  single fixed codec would leave either RAM or CPU on the table. Decode speed is
  near level-independent for zstd [zstd-silesia-benchmark-l1], so a high-ratio
  low level chosen here never taxes the GET path #52 owns.

### The output is a per-class tuple, encoded as a u8 codec id plus a floor array

- The model writes one `{codec id, level, dict-id, size-floor}` tuple per class.
  On the wire to the hot path this is a `u8` codec id and the size-floor in two
  arrays indexed by class id, with the dict-id being the monotonic per-prefix
  dict-version-id DICTIONARIES.md (#55) defines, never a raw zstd dictID. The SET
  path resolves the class id from the prefix and size bucket, does one size
  compare against the floor, and one array read for the codec id; that is the
  whole per-request decision. The branch-free array lookup is deliberate: a
  precomputed cheap score consumed by the fast path is the LRB pattern (a
  gradient-boosted tree runs off-path and the data path reads precomputed
  predictions [lrb-model-and-traffic-reduction]), and a predictable lookup is
  what keeps the path stall-free [parrot-imitation-belady-icml20].

### A size-plus-entropy heuristic is the floor the classifier must beat

- The first thing this spec ships is NOT the classifier; it is a documented
  size-plus-entropy heuristic baseline, parameterized by the #57 survey numbers
  (a workload-derived size floor plus a cheap entropy probe, explicitly not the
  inherited spymemcached 16384-byte client default
  [spymemcached-default-compression-threshold], which is a client-library GZIP
  heuristic and not a server policy; a static large floor is the wrong baseline
  because a per-prefix dictionary makes sub-1KB values worth compressing, 2.8x
  without a dictionary versus 6.9x with one [zstd-dictionary-small-data-6.9x],
  which is exactly why the floor is class-derived and dict-aware rather than a
  fixed constant). The offline classifier is enabled for a class only where it
  beats that heuristic by a margin on replayed value corpora (the cachemon
  corpus, via the #8 harness and the #154 promotion gate). This mirrors the
  W-TinyLFU-must-beat discipline: the cheap deterministic rule is the bar, and
  the model earns its refresh machinery only by clearing it. The kill-switch
  (#91) reverts any class, or the whole table, to the static heuristic with no
  correctness impact, because the heuristic and the classifier emit the same
  tuple shape and the same framing.

### The build and refresh loop lives in the advisor, never on a request

- The model is built and refreshed by the background advisor (#126), which
  already runs an off-path loop on a fixed cadence with hysteresis. The advisor
  trains the entropy estimates from sampled live values, scores classes against
  the objective, and builds an immutable snapshot; it then hands that snapshot to
  the atomic versioned RCU swap (#91/#85) the advisor already uses for eviction
  knobs. Refresh respects a hysteresis band and cooldown so the published table
  does not oscillate per workload shift, the same anti-flap shape #91 enforces.
  Per ADR-0013 the advisor is off/shadow by default, so out of the box the SET
  path uses the static heuristic snapshot and behaves identically to a build with
  the model disabled. Zero model calls happen on any request in any posture.

## Open questions

- Whether the offline classifier beats the size-plus-entropy heuristic by enough
  margin on the surveyed value distributions to justify its refresh machinery,
  resolved per class on the cachemon corpus via #57 and the #154 gate.
- Entropy-estimate width and head-sample size: the `u8` quantization and the
  N-byte training-time head sample, swept against realized-ratio correlation in
  the #57 survey (which may drop the probe entirely for a size-only gate if it
  does not correlate).
- Snapshot refresh cadence and the hysteresis band that keep the table from
  oscillating, deferred to the advisor cadence numbers (#126/#8).
- Class granularity: per-prefix, per-size-bucket, or the full cross product, and
  the resulting bounded table size and class-id width.
- Dict-id binding: how a class references the active per-prefix dict-version-id
  (#55) and what the table does for a class whose dictionary version is mid-swap
  or unresolvable, given DICTIONARIES.md fails closed on resolve.

## Acceptance and test hooks

- The classifier runs only in the background advisor; a hot-path profile shows
  zero per-request model calls and the SET decision is one size compare plus one
  array lookup (#13).
- The SET path consumes an atomically published, monotonically versioned
  snapshot via the #91/#85 swap; a torn or partially applied table is never
  observable to a request.
- The scored objective is net-bytes-saved-per-compress-CPU-millisecond, measured
  and logged per class, not ratio alone [baleen-flash-admission-fast24]; an
  incompressible class resolves to codec=none and the #52 incompressible path.
- A documented size-plus-entropy heuristic baseline exists with #57 numbers, and
  the classifier is enabled for a class only where it beats that baseline on the
  replayed corpus via the #154 gate, mirroring the W-TinyLFU-must-beat bar.
- The output tuple is the `{codec id, level, dict-id, size-floor}` the #52 SET
  path consumes, keyed off #57 buckets, with dict-id the #55 per-prefix
  dict-version-id and codec id drawn only from the ADR-0015 set.
- The kill-switch (#91) reverts a class or the whole table to the static
  heuristic atomically with no correctness impact; with the advisor off/shadow
  (ADR-0013) the SET path uses the static heuristic and behaves identically to a
  model-disabled build.

## References

- ADR-0015, ADR-0021, ADR-0013; issues #92, #88, #126, #52, #55, #57, #91,
  #154, #85, #13, #8, #1; specs COMPRESSION.md, DICTIONARIES.md, ADVISOR.md,
  ADVISOR_SAFETY.md, ADVISOR_PROMOTION.md, WTINYLFU.md; experiment
  value-size-compressibility-survey.md.
- Claims: [baleen-flash-admission-fast24], [lrb-model-and-traffic-reduction],
  [lecar-regret-minimization-smallcache], [parrot-imitation-belady-icml20],
  [zstd-fast-modes-benchmark], [zstd-silesia-benchmark-l1],
  [lz4-silesia-benchmark], [lz4-flex-safe-vs-c],
  [zstd-dictionary-small-data-6.9x], [spymemcached-default-compression-threshold].
