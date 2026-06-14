# Design: Advisor safety guardrails (bounds, hysteresis, rollback, kill-switch)

Issue: #91. Decisions: ADR-0013 (advisor off/shadow default posture), ADR-0008
(S3-FIFO default eviction). Related: #126 (advisor mechanism, ADVISOR.md), #85
(config sources, CONFIG.md), #48 (EvictionPolicy trait, EVICTION.md), #154
(promotion gate), #88 (parent, decomposed).

## Goal and scope

The background advisor is the only place ML touches IronCache and it never runs
on the hot path. This spec is the safety envelope around it: per-knob bounds,
anti-oscillation, automatic rollback, a hard kill-switch to a known-good static
baseline, and a seeded monotonic-versioned config-snapshot contract the hot path
reads. The governing guarantee is that the advisor can only ever match or improve
the static baseline, never regress below it. With the advisor off the cache is
correct and fast, which the queueing result on hit-path contention motivates
[hit-ratio-can-hurt-throughput].

In scope: knob min/max bounds, hysteresis band plus cooldown, the regression
detector and rollback, the kill-switch, and the immutable seeded snapshot the
advisor publishes and the hot path consumes. Out of scope: the advisor objective
function and the expert algorithms (#126); knob storage and reload semantics
(#85); the policies behind the EvictionPolicy trait (#48); the off/shadow default
posture (ADR-0013).

## Design

### Per-knob bounds

- Every tunable knob has a documented, enforced min and max; an out-of-range
  proposal is clamped or rejected, never applied. The knob set is the bounded set
  ADVISOR.md (#126) defines (active policy, Redis-style sample count
  [redis-maxmemory-samples-5], LFU log-factor and decay
  [redis-lfu-morris-counter-params], ghost size, slab/encoding/compression
  thresholds). Bounds are a property of the snapshot schema, so a malformed or
  adversarial proposal cannot widen them.

### Hysteresis and cooldown

- Each knob carries a hysteresis band and a cooldown timer. A change applies only
  when the measured signal crosses the band, and no further change to that knob is
  permitted until the cooldown elapses. This provably bounds change frequency and
  stops flapping near a threshold, which a rate limit alone does not.

### Regression detector and rollback

- After a swap the detector compares live throughput-per-core and hit ratio
  against the pre-change snapshot over the cooldown window. A measured regression
  in either rolls the active snapshot back to the immediately prior one. Both
  signals matter because a higher hit ratio can still lower throughput on a
  relink-bound policy [hit-ratio-can-hurt-throughput]; FIFO-class policies
  (S3-FIFO [s3fifo-small-main-split], SIEVE [sieve-algorithm]) avoid that, but the
  detector does not assume it.

### Kill-switch to the static baseline

- A persistent or repeated breach trips a kill-switch that atomically reverts to a
  static baseline of W-TinyLFU admission [wtinylfu-caffeine-sketch] over a
  FIFO-class core, the deterministic floor any learned change must first beat
  [sieve-simpler-than-lru-nsdi24]. The kill-switch is operator-forceable and is
  the boot default (ADR-0013). The baseline is chosen over last-known-good because
  a learned snapshot can itself be subtly bad, whereas the static path is
  provably correct and fast.

### Seeded versioned RCU snapshot contract

- The hot path reads an immutable config snapshot through a single atomic pointer
  swap (RCU-style); readers never block and never see a torn set. The advisor
  publishes a new snapshot only after a candidate beats the current one on a
  sampled replay (the gate detailed in #154). Each snapshot carries a seed and a
  strictly monotonic version, coordinated with the config layers in #85. A given
  seed plus an input replay yields identical eviction decisions, the determinism
  invariant rollback and audit depend on.

## Open questions

- Regression thresholds and window length per knob class (throughput vs hit-ratio
  sensitivity); numeric values deferred to the harness (#8).
- Cooldown duration and band width per knob, and whether they are themselves
  bounded-tunable.
- Whether a kill-switch trip is sticky until operator reset or auto-clears after
  a quiet period.
- Seed scope (per-shard vs global) and how it is recorded in the snapshot.
- Maximum knob delta per step (bounded step vs jump to any in-range value).

## Acceptance and test hooks

- Every knob has an enforced min/max; an out-of-range proposal is clamped or
  rejected (schema test).
- A soak under a shifting workload shows hysteresis and cooldown bound change
  frequency with no oscillation.
- An injected throughput or hit-ratio regression triggers rollback to the prior
  snapshot.
- The kill-switch reverts to the static baseline atomically, is the boot default,
  and is operator-forceable.
- With the advisor disabled the cache is correct and no path regresses below the
  static baseline [hit-ratio-can-hurt-throughput].
- A seeded snapshot plus input replay yields identical eviction decisions, and
  snapshots are published and consumed with monotonic versioning (#85).

## References

- ADR-0013, ADR-0008; issues #126, #85, #48, #154, #88; specs ADVISOR.md,
  CONFIG.md, EVICTION.md, WTINYLFU.md.
- Claims: [hit-ratio-can-hurt-throughput], [s3fifo-small-main-split],
  [sieve-algorithm], [wtinylfu-caffeine-sketch], [sieve-simpler-than-lru-nsdi24],
  [redis-maxmemory-samples-5], [redis-lfu-morris-counter-params],
  [lecar-regret-min-18x], [cacheus-experts].
