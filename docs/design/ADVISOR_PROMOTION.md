# Design: Advisor evaluation and promotion gate

Issue: #154. Decisions: ADR-0013 (advisor default posture is shadow/off).
Related: ADVISOR_SAFETY.md (#91, live rollback, distinct from this pre-promotion
gate), ADVISOR.md (#126, the controller whose candidates are gated),
ADVISOR_AUDIT.md (#153, which records each verdict), TESTING.md/BENCHMARK.md
(#95/#96/#93, the replay harness and oracle), CONFIG.md (#85, the snapshot store).

## Goal and scope

The hard project rule is that an advisor change must beat the tuned static
baseline on replayed traces before it may act. This spec owns that promotion gate:
an offline-replay plus shadow-A/B pipeline that proves a candidate config beats
the live static baseline by a quantified, harness-tuned margin before the
controller is allowed to publish it. It turns the one-time #90 headroom study and
the #93 offline oracle into a continuous gating pipeline that makes the
"no regression below baseline" target enforceable. #153 records what happened;
this decides what is allowed. In scope: the baseline definition, the two gate
stages, the acceptance margin, and the no-regression sign-off. Out of scope: live
rollback after promotion (#91), the controller internals (#126), and the oracle
implementation (#93).

## Design

### The baseline a candidate must beat

- The gate's reference is the tuned static baseline: W-TinyLFU admission
  [wtinylfu-caffeine-sketch] over the SIEVE/S3-FIFO eviction floor
  [sieve-simpler-than-lru-nsdi24] [s3fifo-small-main-split], with its own knobs
  tuned per trace first so the advisor competes against the best deterministic
  effort, not a strawman (the #90 measurement hazard). This is the same static
  baseline #91's kill-switch reverts to, so "beats baseline on replay" and
  "kill-switch target" name one config.

### Stage 1: offline replay against the oracle

- A candidate config is replayed over the trace corpus in the benchmark-only
  oracle harness (#93), scoring hit ratio at matched cache sizes and reporting the
  gap to the Belady-MIN ceiling and the per-policy gap table [lhd-hit-density].
  The candidate must close more of the baseline-to-MIN gap than the tuned baseline
  by the acceptance margin. Scoring is hit ratio off the hot path only; the gate
  never runs a per-access shadow simulator on a live request, because a higher hit
  ratio reached by hot-path surgery can lower throughput
  [hit-ratio-can-hurt-throughput]. Learned-Belady predictors appear here only as
  offline ceilings (the #13 non-goal), never as a deployable policy
  [parrot-imitation-belady-icml20] [lrb-relaxed-belady-gbm].

### Stage 2: shadow A/B against the live baseline

- A candidate that passes Stage 1 runs in shadow against live traffic (ADR-0013):
  the live baseline serves requests while the candidate is scored on the same
  access stream off the hot path. The gate compares candidate vs baseline hit
  ratio over a window and requires the candidate to win by the margin with a
  no-regression sign-off on the watched throughput-per-core signal. Only a
  candidate that clears both stages becomes eligible for the controller to publish
  as a new snapshot; in shadow posture it still publishes nothing live, it only
  records eligibility (#153).

### Acceptance margin and sign-off

- The margin is harness-tuned, not a slogan: a minimum marginal hit-ratio gain
  over the tuned baseline at the cache-to-working-set ratios IronCache actually
  runs, defended against the operational cost of an adaptive component (the #90
  open question). The margin is set conservatively because the adaptive gain
  concentrates on small caches and can evaporate or invert on the large,
  frequency-dominated caches IronCache expects [lecar-regret-minimization-smallcache]
  [cacheus-experts]; the expert pool here is the cheap O(1) controller, not a
  per-request ensemble [lecar-regret-min-18x]. A candidate inside the noise band,
  or that regresses throughput-per-core, is rejected, not promoted.

### Relationship to live rollback

- The promotion gate is pre-action and offline-plus-shadow; #91 rollback is
  post-action and live. A change must clear this gate to act at all; once acting,
  #91's regression detector can still revert it and the kill-switch can still drop
  to baseline. The two compose: this minimizes how often rollback fires by never
  letting an unproven change act, and rollback covers the residual case where
  replay and shadow did not predict the live result.

## Open questions

- The exact acceptance margin per knob class and the shadow-A/B window length,
  shared with #91's threshold/window open decision and calibrated on the corpus.
- Trace-corpus weighting for the verdict (the #90 in-memory-KV weighting), and
  whether Stage 1 must pass on every corpus trace or on a weighted majority.
- Whether shadow A/B is per-shard or global, and how candidate scoring is
  isolated from the live serving path's cache state.
- Re-promotion cadence: how often a previously rejected candidate may be re-tried
  as the workload drifts, without flapping.

## Acceptance and test hooks

- A candidate that does not beat the tuned static baseline by the margin in Stage
  1 replay is never promoted; the gap-to-MIN table (#93) is recorded for the
  verdict (#153).
- A candidate that passes Stage 1 but loses or only ties the shadow A/B, or
  regresses throughput-per-core, is rejected with a no-regression sign-off failure
  [hit-ratio-can-hurt-throughput].
- In shadow posture (ADR-0013) the gate records eligibility but the controller
  publishes nothing live.
- A seeded replay of the same candidate and trace yields an identical verdict (the
  #91 determinism invariant applied to the gate).

## References

- ADR-0013; issues #154, #91, #126, #153, #90, #93, #95, #96, #85, #13, #1; specs
  ADVISOR.md, ADVISOR_SAFETY.md, ADVISOR_AUDIT.md, TESTING.md, BENCHMARK.md,
  CONFIG.md.
- Claims: [wtinylfu-caffeine-sketch], [sieve-simpler-than-lru-nsdi24],
  [s3fifo-small-main-split], [lhd-hit-density], [hit-ratio-can-hurt-throughput],
  [parrot-imitation-belady-icml20], [lrb-relaxed-belady-gbm],
  [lecar-regret-minimization-smallcache], [cacheus-experts], [lecar-regret-min-18x].
