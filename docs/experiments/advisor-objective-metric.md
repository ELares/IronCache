# Research: The advisor objective metric (negative composite cost, not raw hit ratio)

Issue: #89. Provisional decision: ADR-0016 fixes the headline metrics (throughput-per-core, memory-at-a-fixed-hit-ratio) that the advisor objective must serve; ADR-0013 fixes the advisor's off/shadow default posture.

This doc defines what the per-shard background advisor (ADVISOR.md, #126) optimizes, before that advisor is built. It does not build the advisor and it does not re-decide ADR-0013 or ADR-0016. It fixes the scalar reward, rejects the obvious-but-wrong target, and states the validation gate the reward must pass.

## Provisional decision (already pinned)

ADR-0016 (Accepted, issue #7) pins the operator-visible efficiency metrics: per-core throughput and resident bytes-per-stored-item at a fixed hit ratio, with p99/p999 reported alongside. ADR-0013 (Accepted, issue #155) pins that the advisor is off by default and shadow when first enabled, so the reward defined here is a thing the advisor reports (shadow) before it is a thing the advisor acts on (active). What is not yet pinned is the scalar the advisor maximizes. This doc proposes that scalar and bounds the open questions.

The proposal, following Baleen's central lesson to optimize the real cost an operator pays rather than a proxy for it [baleen-flash-admission-fast24]:

- The advisor minimizes a composite cost and maximizes its negation as the reward:
  `cost = (miss_rate * miss_penalty) + (alpha * cpu_ns_per_op) + (beta * rss_bytes_per_served_key)`,
  `reward = -cost`.
- `miss_rate` and `cpu_ns_per_op` are read from per-shard hot-path counters; `rss_bytes_per_served_key` from the allocator and keyspace gauges. The advisor reads windowed deltas off the hot path, so the objective adds no per-request work and stays consistent with the no-per-request-inference non-goal (#13).
- `miss_penalty`, `alpha`, and `beta` are operator-set weights priced, by default, off the two ADR-0016 headline terms: `alpha` prices a core (the throughput-per-core term), `beta` prices a resident byte (the memory-at-fixed-hit-ratio term), and `miss_penalty` prices a backend round-trip. The defaults make per-core throughput and memory efficiency first-class terms of the objective rather than footnotes.

Raw hit ratio is rejected as the objective. A higher hit ratio can lower throughput: on the LRU/SLRU hit path the per-hit list operation is a contended bottleneck, so squeezing out extra hits past a point reduces throughput, whereas for FIFO higher hit ratio always raises throughput [hit-ratio-can-hurt-throughput]. An advisor that maximized raw hit ratio would therefore trade away the exact thing ADR-0016 measures. The composite cost encodes this trap directly into the gradient: a configuration that raises hit ratio by a point but raises the CPU or RSS terms by more is a higher-cost, lower-reward configuration, so the advisor rejects it by construction. This is the same proxy-versus-true-cost divergence Baleen avoids by training against backend load rather than hit ratio [baleen-flash-admission-fast24].

## Why this is harness-blocked

The proposal can be written down now, but its validation gate cannot be run yet. The gate is that the objective minimum must agree with a manual sweep on the ADR-0016 headline metrics; checking that needs three things that do not exist yet:

- The benchmark harness and ADR-0016 methodology (per-core throughput, memory-at-fixed-hit-ratio, open-loop coordinated-omission-corrected tails); harness is #8.
- A populated set of per-shard counters (`miss_rate`, `cpu_ns_per_op`, `rss_bytes_per_served_key`) wired to a windowed-delta reader, which lands with the advisor mechanism (#126) and its safety envelope (#91).
- A way to compute the headroom denominator the reward is judged against: the per-trace Belady-MIN ceiling and the per-policy gap to it, owned by EVICTION_ORACLE.md (#93) and the online gap estimator (#87).

Until the harness can replay a workload, sweep the knobs by hand, and report the headline triple, any claim that the reward minimum coincides with the manual optimum is an assertion, not a measurement. This doc records the gate so the reward is not shipped before that measurement exists.

## Experiment to run

Objective-tracking sweep (the validation gate):

- Pick a small, fixed knob grid the advisor would search (active policy, sample count, LFU log-factor and decay, ghost size; the bounded set in ADVISOR.md). For each grid point, replay a fixed workload through the #8 harness under ADR-0016 methodology and record the headline triple (p99/p999, throughput-per-core, RSS at the fixed hit-ratio target) plus the three objective inputs.
- Compute `reward = -cost` at each grid point from the recorded inputs, using the default ADR-0016-priced weights.
- The gate passes if the grid point that minimizes `cost` is the same point a human would choose from the headline triple, across every replayed workload. A reward whose minimum disagrees with the manual headline optimum on any workload does not ship.

Hit-ratio-trap regression (the rejection, made measurable):

- Construct or select a workload where a higher-hit-ratio configuration costs more CPU per op on the contended hit path [hit-ratio-can-hurt-throughput]. Confirm the raw-hit-ratio objective prefers it and the composite-cost objective rejects it. If the composite objective also prefers the lower-throughput configuration, the weights are mis-priced and must be retuned before the reward is used.

Weight-sensitivity sweep:

- Vary `miss_penalty`, `alpha`, `beta` around the ADR-0016-derived defaults and report how the argmin moves. The reward is acceptable only if, across the operator-plausible weight range, the argmin never lands on a configuration that the headline triple says is worse on both throughput-per-core and RSS.

Windowing check:

- Compare fixed-time versus fixed-op-count windows for the delta reads under bursty load, and report which yields a stabler argmin without lengthening the advisor's reaction time past the #91 cadence and hysteresis bounds.

## What would change the decision

- The objective-tracking sweep finds a workload where the cost minimum disagrees with the manual headline optimum, which would force a different functional form or a re-priced weight set before the reward is adopted.
- The hit-ratio-trap regression shows the composite objective still prefers a lower-throughput, higher-hit-ratio configuration under the default weights [hit-ratio-can-hurt-throughput], which would mean the CPU term is under-weighted relative to ADR-0016.
- A measured need for a separate tail-latency term: if p99 is not adequately constrained by the CPU term plus an admission guardrail, the objective gains an explicit tail term rather than leaning on the CPU term as a proxy.
- The default-weight question resolves the other way: if the #7 cost model cannot supply defensible defaults for `alpha`, `beta`, and `miss_penalty`, they become required operator config with no default rather than ADR-0016-derived defaults.

## References

- ADR-0016: headline metrics are throughput-per-core and memory-at-a-fixed-hit-ratio (issue #7). ADR-0013: advisor off/shadow default posture (issue #155).
- ADVISOR.md (#126): the per-shard background advisor that consumes this objective. EVICTION_ORACLE.md (#93) and #87: the Belady-MIN ceiling and online gap that denominate the reward. #90: the headroom study that explores the search space under this objective.
- #88: parent advisor design. #8: benchmark harness and ADR-0016 methodology. #91: advisor safety envelope (cadence, hysteresis, bounded knobs). #13: no per-request inference. #1: vision.
- Claims (resolved via docs/prior-art/claims.yaml): [baleen-flash-admission-fast24], [hit-ratio-can-hurt-throughput].
