# Experiment: Continuously-reported online Belady-MIN gap estimator

Issue: #87. Decisions: ADR-0016 (the hit-ratio and methodology this gap is
measured under), ADR-0017 (per-tenet acceptance gates; the native metrics feed
the gates, so this gauge slots into that observability surface). Related: #93
(the offline Belady oracle reused as the fixed reference), #86 and #152 (the
OBSERVABILITY surface and metric registry the belady_gap name is reserved in),
#46 (the most-efficient-cache claim this metric makes a live number), #88 (the
advisor whose hit-ratio lift needs this denominator), #13 and #156 (the
no-ML-and-no-replay-on-the-hot-path non-goal this estimator respects).

## Goal and scope

IronCache's most-efficient-cache claim is only credible if it is measurable in
production, not just on captured traces. This experiment asks whether the offline
Belady-MIN gap (the hit-rate distance between the live online policy and the
optimal clairvoyant policy) can be computed continuously and cheaply enough to
report as a live belady_gap metric, within a cost budget that never touches the
hot path, and whether the result is trustworthy enough to expose. Scope: the
reservoir sampler and its cost bound, the background replay against the #93
oracle, the windowed estimate with a confidence interval, the reservation of the
belady_gap metric name in the #152 registry, and the debug gate that keeps the
metric internal until its interval is validated. Out of scope: any exact unsampled
gap (rejected below), and any change to the online policy itself. This doc reuses
the #93 oracle as a fixed reference and adds no new prior-art claim.

## Design

### Hot-path cost: reservoir sample, bounded by buffer not rate

- The request stream is reservoir-sampled at a low fixed target rate (1 percent)
  into a bounded ring buffer, bounded further by a token budget so the sampler
  degrades to zero overhead under load: when tokens are exhausted the sample is
  dropped, so a traffic spike cannot turn sampling into a hot-path tax. The hot
  path pays only one atomic counter increment per sampled request; sampling cost
  is bounded by buffer size, not request rate, which is the Efficient-over-Scalable
  trade. The buffer holds key fingerprints and access metadata, not values, so its
  footprint is fixed and small. No replay, no oracle, and no ML run on the request
  path; all of that is off-path by construction, consistent with the #13/#156
  non-goal.

### Off-path replay against the #93 oracle

- A background worker periodically replays the sampled window through two
  references at the same configured cache size: the offline Belady oracle from #93
  (the optimal reference) and a simulated copy of the live online policy. The gap
  is belady_gap = hit_rate(optimal) - hit_rate(online) over the window. Reusing the
  #93 oracle as the fixed reference means the live number and the offline #47 table
  share one definition of optimal. The replay runs on a background worker off the
  hot path, so its superlinear-in-window cost never blocks a request; the worker is
  the only consumer of the ring buffer.

### Windowed estimate with a confidence interval

- Because the gap is sampled, it is reported as a windowed estimate with a
  confidence interval, never a point value. The window is a sliding window of the
  last N million sampled references, sized to the configured cache (default roughly
  8x capacity in distinct keys), recomputed on a fixed cadence (default every 60
  seconds). The confidence interval is the load-bearing honesty knob: a 1 percent
  sample on a skewed workload gives a band, and reporting the band rather than a
  bare number is what keeps the metric defensible. The estimator leans on the fact
  that a good online policy already estimates the quantity Belady uses: hit-density
  ranking tracks optimal closely on real workloads and maintains per-object density
  estimates [lhd-hit-density], so the comparison anchors on an already-maintained
  signal rather than new hot-path bookkeeping.

### Registry reservation and debug gate

- The belady_gap name (plus its confidence-interval companions and the window/
  cadence labels) is reserved in the #152 metric/INFO registry now, so the name is
  pinned before the M1 freeze and a later build cannot collide with it; the series
  is a gauge with a documented cardinality bound. The metric is debug-gated:
  shipped behind a debug flag and surfaced only on the native IronCache INFO
  section and /metrics endpoint while it is internal, not exposed as a
  customer-facing number until the confidence interval is validated on skewed
  workloads. Promoting it to customer-facing is a later, evidence-gated decision,
  not part of this experiment.

## Open questions

- Does a 1 percent sample give a tight enough interval on skewed (Zipf)
  workloads, or does the tail need stratified sampling to bound the band.
- Whether the gap is exposed per-tenant or only aggregate, given that replay
  reveals access patterns (a privacy decision shared with #86).
- Whether the background replay reuses the #93 oracle in-process or runs it as an
  isolated worker to bound its memory separately.
- When to lift the debug gate and promote belady_gap to a customer-facing metric,
  decided once the interval is validated.

## Acceptance and test hooks

- The hot path pays at most one atomic counter increment per sampled request; the
  sampler is bounded by buffer size and a token budget and degrades to zero
  overhead under load (a load test asserts no per-request replay or oracle call).
- belady_gap is reported as a windowed estimate with a confidence interval, not a
  point value, recomputed on the configured cadence over the sized window.
- The replay uses the #93 oracle as the optimal reference and a simulated copy of
  the live policy at the same cache size; the offline and live definitions of
  optimal agree.
- The belady_gap name and its labels are present in the #152 metric registry with
  a documented cardinality bound, and the metric is gated behind the debug flag
  until validated (not customer-facing by default).
- No new prior-art claim is introduced; the only cited number is the existing
  [lhd-hit-density] anchor.

## References

- ADR-0016, ADR-0017; issues #93, #86, #152, #46, #88, #13, #156.
- Specs: docs/design/EVICTION_ORACLE.md (#93), docs/design/OBSERVABILITY.md
  (#86/#152), docs/experiments/eviction-bakeoff.md (#47),
  docs/NON_GOALS.md (#13/#156).
- Claims: [lhd-hit-density].
