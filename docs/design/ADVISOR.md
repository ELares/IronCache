# Design: Per-shard background advisor (expert selection + bounded knob autotuning)

Issue: #126. Decisions: ADR-0013 (advisor off/shadow default posture), ADR-0008
(S3-FIFO default eviction). Related: #91 (safety guardrails, ADVISOR_SAFETY.md),
#48 (EvictionPolicy trait, EVICTION.md), #49 (W-TinyLFU filter, WTINYLFU.md), #85
(config snapshot, CONFIG.md), #13 (no per-request inference), #88 (parent,
decomposed).

## Goal and scope

This specifies the per-shard background advisor: an off-path loop that weights
experts over the deterministic policy set and autotunes a bounded knob set, then
publishes its choice through the atomic config-snapshot swap that
ADVISOR_SAFETY.md (#91) defines. The request loop stays inference-free and
deterministic (#13); the advisor only observes counters and proposes snapshots.
The off/shadow default posture is fixed by ADR-0013 and is not re-decided here.

In scope: the expert-weighting mechanism, the policy set it selects among, the
bounded knob set, the cadence/hysteresis shape, the snapshot-swap coupling, and
the binding to the EvictionPolicy trait (#48). Out of scope: numeric tuning
(retune interval, marginal-gain threshold), deferred to the harness (#8); the
safety envelope (bounds enforcement, rollback, kill-switch, seeding), owned by
#91; the promotion gate (#154).

## Design

### Expert weighting

- Each shard runs a regret-minimizing / contextual-bandit controller that weights
  experts off the hot path. LeCaR maintains weights over two experts with regret
  minimization and beats ARC by more than 18x at small cache-to-working-set
  ratios [lecar-regret-min-18x]; CACHEUS generalizes this to an adaptive mixture
  selected per workload primitive [cacheus-experts]. We borrow the controller as
  the off-path selector and reject per-request ensemble evaluation, which would
  reintroduce hot-path cost (#13).

### Policy set

- The experts are the cheap deterministic policies already behind the trait:
  SIEVE (one FIFO, a hand, a visited bit) [sieve-algorithm], a W-TinyLFU
  admission filter [wtinylfu-caffeine-sketch] (the non-ML floor, WTINYLFU.md
  #49), and sampled LRU/LFU. The controller selects among them and tunes their
  knobs; it never invents a policy. The default eviction core remains S3-FIFO
  with its small/main split [s3fifo-small-main-split] (ADR-0008), which the
  advisor may select but does not replace as the baseline.

### Bounded knob set

- The advisor tunes a small, bounded set: the active policy, sample count, LFU
  log-factor [redis-lfu-log-factor] and decay, ghost size, and
  slab/encoding/compression thresholds. The set is deliberately small so the
  search space is enumerable and every proposal maps to a documented knob. Bounds
  enforcement, clamping, and rejection are the safety spec's job (#91).

### Cadence and hysteresis

- The loop retunes on a fixed cadence (interval deferred to #8), proposes at most
  the bounded knob deltas, and respects the per-knob hysteresis band and cooldown
  from #91 so it cannot flap. A proposal is published only after it beats the
  current snapshot on replay (the gate in #154).

### Snapshot swap and trait binding

- The advisor never mutates live policy directly. It builds an immutable seeded
  snapshot and hands it to the atomic RCU pointer swap (#91), monotonically
  versioned and coordinated with #85. The hot path reads the active policy and
  knobs through the EvictionPolicy trait (#48); a swap changes which trait impl
  and which knob values the shard uses on the next access, with no reader lock and
  no torn read.

## Open questions

- Retune interval and the marginal-gain threshold for accepting a proposal
  (deferred to the harness #8).
- Per-primitive context features the bandit conditions on, and whether the expert
  set is fixed or extensible per tenant.
- Whether expert weights persist across restart or reset to the static baseline.
- How shadow-mode recommendations (ADR-0013) are surfaced before active tuning.

## Acceptance and test hooks

- The request loop performs no inference and is deterministic under replay (#13).
- The advisor proposes only knobs in the bounded set, each within its #91 bounds.
- A proposal reaches live policy only via the atomic versioned snapshot swap
  (#91/#85), never by direct mutation.
- Policy selection routes through the EvictionPolicy trait (#48); a swap changes
  the active impl with no reader lock.
- With the advisor off or in shadow the engine behaves identically to the static
  baseline (ADR-0013).

## References

- ADR-0013, ADR-0008; issues #91, #48, #49, #85, #13, #154, #88; specs
  ADVISOR_SAFETY.md, EVICTION.md, WTINYLFU.md, CONFIG.md.
- Claims: [lecar-regret-min-18x], [cacheus-experts], [wtinylfu-caffeine-sketch],
  [sieve-algorithm], [s3fifo-small-main-split], [redis-lfu-log-factor].
