# Design: Offline Belady optimal-eviction oracle in the benchmark harness

Issue: #93. Decisions: ADR-0008 (S3-FIFO default, the policy whose gap this oracle
measures), ADR-0016 (headline metrics and benchmark methodology the oracle
reports under). Related: #47 (eviction bake-off, the offline consumer of the
oracle table), #87 (the online Belady-gap estimator, which reuses this oracle as
its fixed reference), #48 (the EvictionPolicy trait fixtures the oracle replays
against), #8 (the benchmark harness this lives in), #88 (the advisor whose
hit-ratio claims need this denominator), #13 and #156 (the no-ML-on-the-hot-path
non-goal this oracle keeps benchmark-only).

## Goal and scope

IronCache's headline claim is that it is the most efficient cache. That claim is
only defensible if every cheap online policy can be scored against the
theoretical ceiling on the same trace, so a policy change is justified by measured
gap closure rather than assertion. This specifies a simulator-only oracle that, per
trace and per cache size, computes the optimal achievable miss ratio and the gap
between each online policy and that optimum. It has three levels: exact
Belady-MIN (the ground-truth ceiling), Sampled-Belady (a bounded-memory
approximation for traces too large to hold a full next-use index), and an
LRB-style gradient-boosted-tree oracle (a learned ceiling that shows how close an
implementable predictor gets to exact MIN). Scope: the three oracle levels and
their accuracy contract, the hard benchmark-only isolation boundary, and the
output schema consumed by #47 and #87. Out of scope: putting any predictive model
on the data path (the #13/#156 non-goal); the oracle exists precisely to quantify
what that choice costs, never to relax it. The trace corpus, replay driver, and
the head-to-head measurement methodology belong to #8 and #47; this doc owns the
oracle and its schema, not the harness around it.

## Design

### Exact Belady-MIN (the ground-truth ceiling)

- Belady-MIN evicts the resident object whose next reference is furthest in the
  future. This is optimal but clairvoyant, so it is computable only offline over a
  recorded trace. The oracle runs two passes over the replayed trace. Pass one
  builds a precomputed next-use index: for each access position, the position of
  that key's next reference (or infinity if it never recurs). Pass two replays
  forward at a fixed byte budget, and on a miss at the budget evicts the resident
  whose next-use position is the largest, using a max-heap (or an ordered
  next-use structure) keyed by next-use distance and refreshed as keys are
  re-referenced. Byte-accurate accounting, not slot-accurate: the budget is a byte
  budget so the ceiling is comparable to the bytes-per-key framing the policies are
  scored under (ADR-0008), and bytes come from the same allocator-attributed model
  the harness uses, not a logical-size sum [redis-maxmemory-accounting]. The output
  is the exact optimal miss ratio for that trace at that cache size.

### Sampled-Belady (bounded memory for very large traces)

- A full next-use index over a multi-billion-request trace does not fit in memory.
  Sampled-Belady trades a known, reported accuracy delta for tractable memory by
  two levers: a bounded lookahead window (next-use is resolved only within a
  forward horizon, beyond which a key is treated as furthest-future) and a sampled
  eviction-candidate set (the victim is the furthest-future among a sampled subset
  of residents rather than the global argmax). Both levers are configured per run,
  and on any trace where exact Belady-MIN also fits, the oracle reports the
  measured miss-ratio delta of Sampled-Belady versus exact MIN, so the
  approximation error is a number, not a hope. The lookahead horizon and sample
  size are the tunables; their defaults are calibrated so the reported delta stays
  inside a small band on the corpus.

### LRB-style gradient-boosted-tree oracle (the learned ceiling)

- A gradient-boosted-decision-tree model trained offline on per-object features
  predicts whether an object's next reference falls beyond a relaxed-Belady
  boundary, following LRB's relaxed-Belady objective
  [lrb-relaxed-belady-gbm][lrb-model-and-traffic-reduction]. This is the learned
  ceiling: it characterizes how close an implementable predictor (a GBT, not a
  clairvoyant) gets to exact MIN, and the oracle reports its hit-ratio gap to
  exact Belady-MIN on each trace where both run. Parrot's neural imitation of
  Belady is the alternative rejected here: it needs per-access NN inference and is
  a research simulator only [parrot-imitation-belady-icml20], so a GBT is the
  cheaper offline approximation that still characterizes the gap. The GBT trains
  and scores entirely offline against the trace; it is part of the oracle, never a
  policy, and never sees the data path.

### Hard benchmark-only isolation

- The oracle is a separate benchmark-only crate behind a feature, with no
  dependency edge from the server crate or any hot-path module. It imports nothing
  from the data path, the server crate imports nothing from it, and disabling it
  cannot change cache behavior. A build of the production binary excludes the
  oracle entirely: a release-binary symbol check (no oracle symbols present)
  enforces the boundary in CI, the same belt-and-braces posture the perf gate uses
  for its measurement crate. This is the mechanism that keeps the #13/#156
  no-ML-on-the-data-path non-goal true while still letting the oracle quantify what
  that non-goal costs. The GBT's model artifacts and any ML build dependencies live
  only in the benchmark crate, never in the server's dependency tree.

### Output schema (the #47 and #87 contract)

- The oracle emits, per trace and per cache-size point, a machine-readable record:
  the trace id, the byte budget, the exact optimal miss ratio (from Belady-MIN or,
  when MIN does not fit, Sampled-Belady with its reported delta and the lever
  settings used), and the per-policy gap for each online policy under test
  (SIEVE, S3-FIFO, W-TinyLFU), where gap is optimal-minus-online hit ratio. The
  learned-ceiling row carries the GBT's own gap to exact MIN. Per-trace headroom
  (the best online policy's distance from optimal) is reported so a policy change
  can be justified by measured gap closure. This record is the table #47 folds into
  the eviction bake-off summary (docs/experiments/eviction-bakeoff.md already
  reports a per-policy gap-to-Belady (#93)), and the same optimal-miss-ratio
  value is the fixed reference the #87 online estimator replays its sampled window
  against. The schema is versioned so a #47 table and a #87 dashboard read one
  format.

## Open questions

- The exact per-object feature set and GBT hyperparameters for the learned ceiling,
  and whether one model generalizes across corpus traces or needs per-trace
  retraining; settled by the #47 bake-off run.
- The Sampled-Belady default lookahead horizon and candidate-sample size that keep
  the reported delta inside a small band on the largest corpus trace, calibrated on
  traces where exact MIN also fits.
- Whether the oracle replays through the real EvictionPolicy trait fixtures (#48)
  for the online-policy rows or a faithful simulated copy, sharing the
  trait-fixture question with #47.
- The corpus and per-cache-size sweep points the oracle runs over, shared with the
  #47 and #8 corpus decision.

## Acceptance and test hooks

- Belady-MIN computes the exact optimal miss ratio on a replayed trace at a fixed
  byte budget, validated against a known small hand-checked trace.
- Sampled-Belady runs on the largest corpus trace within a bounded memory budget
  and reports its measured accuracy delta versus exact Belady-MIN on a trace where
  both fit.
- The LRB-style GBT oracle trains offline on per-object features and reports its
  hit-ratio gap to exact Belady-MIN [lrb-relaxed-belady-gbm].
- The oracle is a benchmark-only crate or feature with no dependency edge from the
  server or hot path; a production-binary build excludes it entirely and a
  release-binary symbol check asserts no oracle symbols are present.
- For each corpus trace the harness emits the versioned record (optimal miss ratio,
  per-policy gap for SIEVE/S3-FIFO/W-TinyLFU, learned-ceiling gap, headroom)
  consumed by #47, and the same optimal value feeds the #87 online estimator.
- Bytes are allocator-attributed [redis-maxmemory-accounting], not a logical-size
  sum, so the ceiling is comparable to the policies' bytes-per-key.

## References

- ADR-0008, ADR-0016; issues #47, #87, #48, #8, #88, #13, #156, #46.
- Specs: docs/experiments/eviction-bakeoff.md (#47), docs/design/EVICTION.md (#48),
  docs/design/WTINYLFU.md (#49), docs/design/BENCHMARK.md (#8),
  docs/NON_GOALS.md (#13/#156 no-ML-on-the-data-path), docs/AI_PIPELINE.md
  (the reproduction bar this oracle unblocks).
- Claims: [parrot-imitation-belady-icml20], [lrb-relaxed-belady-gbm],
  [lrb-model-and-traffic-reduction], [redis-maxmemory-accounting].
