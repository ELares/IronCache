# Design: Continuous performance-regression CI gate

Issue: #159. Decisions: ADR-0016 (headline metrics: throughput-per-core,
bytes-per-key, open-loop tails), ADR-0017 (per-tenet acceptance gates, Efficient
half). Related: #8 (BENCHMARK.md, the published head-to-head harness), #96 (the
Valkey baseline), #91 (runtime advisor-rollback, not a build gate), #7 (headline
metrics EPIC), #1 (vision claims).

## Goal and scope

The Efficient tenet and the ADR-0016 headline numbers only stay true if a change
that silently regresses them fails the build. This specifies a per-PR CI gate
that runs a small criterion micro set plus a memtier/wrk2-style macro smoke pass
against the merge-base, compares throughput-per-core and bytes-per-key to stored
baselines under noise-aware thresholds, and ratchets: a regression past a budget
fails the PR. It is distinct from #8/BENCHMARK.md, which reproduces PUBLISHED
head-to-head numbers on pinned bare-metal hardware and scopes out tuning IronCache
itself (too heavy and noisy for per-PR CI); from #96, IronCache vs a pinned
Valkey, not IronCache vs its prior self; and from #91, a runtime advisor-rollback,
not a build gate. Scope: the smoke set, the baseline store and merge-base compare,
the noise model, the ratchet and its budgets, and the CI surface. The heavy
published harness stays in #8.

## Design

### What the gate measures (the same two numbers, smaller)

- The gate measures the ADR-0016 headline pair and nothing new: per-core
  throughput (peak QPS divided by cores used, so core count is not the lever) and
  resident bytes-per-stored-item at a fixed hit ratio, with p99/p999 reported
  alongside as a watched-but-soft signal. It reuses the BENCHMARK.md (#8) memory
  model: bytes are read from allocator introspection, not a logical-size sum,
  because maxmemory accounting must capture allocator rounding
  [redis-maxmemory-accounting]. The macro smoke pass uses the same pinned-flags
  memtier discipline as #8 (never inherited defaults; pipeline depth stated, since
  memtier defaults to 1, i.e. no pipelining [memtier-default-pipeline-1]) over a
  single short Zipfian point from the YCSB mix [ycsb-core-workloads], not the full
  sweep.

### Micro and macro split

- Micro: a criterion bench set over the hot paths (RESP parse, hashtable
  probe/insert, eviction victim selection, codec encode/decode), in the style of
  the criterion-derived benchmark IronCache already cites for a codec dependency
  choice [lz4-flex-safe-vs-c]. Criterion is the per-PR workhorse because it is
  in-process, statistical, and fast; it carries the bytes-per-key assertions via
  the memory model and the per-op cost assertions.
- Macro: one short open-loop constant-rate (wrk2-style) point reporting
  p99/p999/p9999 free of coordinated omission [coordinated-omission-closed-loop],
  plus a separate brief closed-loop peak-QPS pass; the two are never conflated, per
  ADR-0016. The macro pass is the throughput-per-core source of truth; criterion
  alone cannot measure cross-connection peak QPS or honest tails. Note memtier's
  default printed percentiles stop at p99.9 [memtier-default-percentiles], so the
  open-loop tail path emits its own HdrHistogram artifact rather than relying on
  the tool default.

### Baselines and merge-base compare

- Each PR run measures HEAD and compares to the merge-base. Two sources, in order:
  a committed baseline JSON for the merge-base commit if present (the fast path),
  else a fresh build-and-measure of the merge-base on the same runner in the same
  job (the correct-but-slower path), so the comparison is always same-hardware,
  same-toolchain. Baselines are stored as the machine-readable JSON +
  HdrHistogram artifacts BENCHMARK.md (#8) already emits, keyed by commit, so a
  gate result and a published run share one format. Baselines are refreshed only
  by explicit PR, never auto-committed from CI, mirroring the #8 competitor-matrix
  rule that version/number bumps require a human PR.

### Noise model and thresholds

- CI shared runners are noisy, so an absolute compare would flap. The gate runs N
  repetitions of each point, takes the median, and computes a per-metric noise
  band from the run-to-run variance (criterion's own confidence interval for the
  micro set; the inter-rep IQR for the macro point). A delta inside the band is
  not a regression. A delta outside the band but inside the budget is a warning; a
  delta outside both fails. Per-core throughput and bytes-per-key get tight
  budgets (small single-digit percent); p99/p999 is reported and trend-tracked but
  does not hard-fail per PR, because tail noise on shared CI is high and the
  honest tail bar lives in the bare-metal #8 run.

### The ratchet (direction and budget)

- The ratchet has a fixed sign per metric: per-core throughput may not fall and
  bytes-per-key may not rise beyond budget versus the merge-base baseline. This is
  the Efficient half of ADR-0017's per-tenet gates, made per-PR: ADR-0017 sets the
  release bar (per-core throughput strictly above Valkey single-core, bytes-per-item
  below Redis at a fixed hit ratio) against the live competitor matrix; this gate
  protects the slope between published runs so the numbers do not drift
  commit-by-commit. An intentional trade (accept N% throughput for M% memory) is
  landed by updating the committed baseline in the same PR with a rationale, which
  is the only sanctioned way to move the bar down. The gate reads native metrics
  (unlike Redis, which has no built-in Prometheus [redis-no-builtin-prometheus]),
  so measurement needs no sidecar.

### CI surface

- One required status check per PR emitting a compact table (metric, merge-base,
  HEAD, delta, band, budget, verdict) as a PR comment plus the JSON/HdrHistogram
  artifacts. The job pins toolchain and runner class; the macro pass uses taskset
  core isolation and loopback like #8, scaled to one short point so the job fits a
  per-PR time budget.

## Open questions

- Where merge-base baseline JSON lives (in-repo per-commit vs an external artifact
  store keyed by commit) and its retention, shared with #8's artifact-storage open
  question.
- The exact per-metric budgets and rep count N that keep false-positive flap below
  a target while still catching a real single-digit-percent regression; calibrated
  on the chosen CI runner class.
- Whether the macro point runs on every PR or only when files on the hot path
  change (a path filter), to keep CI fast without missing regressions.
- The hosted-runner instance class for the gate (distinct from #8's bare-metal
  matrix), and how much its noise floor widens the bands.

## Acceptance and test hooks

- A PR that regresses per-core throughput or raises bytes-per-key beyond budget
  versus the merge-base fails the required check; a delta inside the noise band
  does not.
- Micro (criterion) and macro (open-loop wrk2-style + closed-loop peak QPS) both
  run; the open-loop path reports p99/p999/p9999 free of coordinated omission
  [coordinated-omission-closed-loop] and emits an HdrHistogram artifact rather than
  relying on memtier's default p99.9 cutoff [memtier-default-percentiles].
- The compare is same-runner same-toolchain: merge-base is taken from a committed
  baseline or rebuilt in-job; bytes are from allocator introspection
  [redis-maxmemory-accounting], not a logical-size sum.
- An intentional trade-off lands only by updating the committed baseline in the
  same PR; CI never auto-commits a baseline.
- The macro pass uses pinned memtier flags with stated pipeline depth (not the
  default of 1, i.e. no pipelining [memtier-default-pipeline-1]) over a YCSB point
  [ycsb-core-workloads], reusing the #8 harness invocation.

## References

- ADR-0016, ADR-0017; issues #8, #96, #91, #7, #1; specs BENCHMARK.md, TESTING.md,
  OBSERVABILITY.md.
- Claims: [redis-maxmemory-accounting], [memtier-default-pipeline-1],
  [memtier-default-percentiles], [ycsb-core-workloads],
  [coordinated-omission-closed-loop], [lz4-flex-safe-vs-c],
  [redis-no-builtin-prometheus].
