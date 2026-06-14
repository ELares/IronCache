# Experiment: Long-horizon soak and memory-stability correctness gate

Issue: #161. Provisional decision: the steady-state posture pinned by
docs/design/DEFRAG.md (#43), docs/design/EXPIRATION.md (#51), and
docs/design/CLIENT_TRACKING.md (#21), bounded by the ADR-0006 allocator
accounting and the ADR-0004 reclamation backbone. This doc records the
multi-hour/day soak that proves those steady-state claims hold; it re-decides
none of them.

## Provisional decision (already pinned)

Three design specs make steady-state promises that no short conformance,
differential, or property run can prove, because each is a no-leak-over-time
claim rather than a per-operation claim:

- DEFRAG.md (#43) pins structural bounding as the first line of defense and the
  copy-relocate reclaimer as an off-by-default backstop
  [redis-activedefrag-default], with the explicit open question of whether
  slab-class bounding alone defers the reclaimer build past M1. That deferral is
  only defensible if RSS actually converges toward the allocator-attributed
  logical bound under churn without the reclaimer running. The OS does not
  automatically reclaim freed heap, so a heap filled to peak then half-freed can
  pin near-peak RSS [redis-fragmentation-ratio]; whether fine-grained slab
  classes plus the allocator background purge hold RSS near the logical bound is
  the thing under test, not a settled number.
- EXPIRATION.md (#51) pins a per-shard timing wheel with a lazy backstop and a
  bounded background reclamation queue, promising resident memory for
  expired-but-not-reclaimed keys stays bounded under heavy-expiry churn and that
  the queue never grows without bound. The wheel itself, its slot handles, and
  the reclamation queue are all per-shard structures whose size must not drift
  upward across a long run.
- CLIENT_TRACKING.md (#21) pins a single global hard-capped tracking table that
  emits spurious invalidations on overflow rather than silently dropping
  entries. The cap is only real if the table count and bytes stay at or below
  the cap across a long CSC workload, with no slow leak of tracked-key entries
  past eviction.

The provisional decision this gate defends is therefore: IronCache ships these
three subsystems in their pinned default posture (reclaimer off, expiry wheel
plus lazy backstop plus background reclamation, hard-capped tracking table) and
asserts, by a long-horizon soak, that resident memory converges, fragmentation
stays bounded, and per-shard and global tracked structures (file descriptors,
timers/wheel slots, reclamation-queue depth, tracked-key entries) do not grow
without bound. The soak does not change the defaults; it is the evidence the
defaults are safe over the lifetime a cache actually runs.

## Why this is harness-blocked

The gate needs a leak signal and an RSS-trend signal taken over a multi-hour to
multi-day run under realistic churn, which requires four things that do not
exist yet:

- The benchmark and load harness (#8) to drive sustained mixed churn (set, get,
  expire, evict, defrag, CSC tracking, replication backlog) at a controlled
  rate for hours to days, with the ADR-0006 allocator-introspection memory model
  so resident bytes are read from the allocator, not summed from logical sizes.
- A build profile under AddressSanitizer plus LeakSanitizer (ASan/LSan) so an
  outright leak is caught directly, separate from the release build that carries
  the RSS-slope check, since the sanitizer build is too slow to run at full soak
  rate.
- The INFO and metrics surface (#86) exposing resident bytes, used_memory,
  mem_fragmentation_ratio [redis-fragmentation-ratio], per-shard
  reclamation-queue depth, wheel-slot occupancy, open file descriptors, and the
  tracking-table size and key count, sampled on a fixed cadence into a
  time-series the trend check consumes.
- Working implementations of the three subsystems under test (#43, #51, #21)
  plus the replication path that produces backlog pressure.

Until the harness can sustain churn for the full horizon and the metric surface
emits a sampled time-series, any leak or drift claim is an assertion, not a
measurement. The sibling gates already named in the repo do not cover this: the
advisor safety guardrails (#91) bound knob-change oscillation under a shifting
workload, and the Jepsen nightly soak (#99) watches consistency under faults;
neither owns the single-node long-horizon engine memory-stability gate, which
is why #161 exists.

## Experiment to run

Build profiles (two, run as separate arms):

- Sanitizer arm: ASan plus LSan, run at reduced rate for a bounded window long
  enough to exercise many full churn cycles. Purpose: catch any definite leak
  and any heap error directly. Decision input is binary (clean or not).
- Release arm: the shipping build with the allocator-introspection metric
  surface, run for the full multi-hour to multi-day horizon at production-like
  rate. Purpose: the RSS-slope and bounded-growth trend checks, which a
  sanitizer build is too slow to produce at scale.

Workload (held churn-heavy and adversarial across both arms, stated as the
design profile, not a measured result):

- Steady mixed read/write with a working set larger than maxmemory so eviction
  runs continuously, layered with a heavy-expiry stream that schedules many keys
  on shared and adversarial same-deadline TTLs so the timing wheel and lazy
  backstop are both exercised (#51).
- A fill-to-peak then free-half phase repeated across cycles to drive external
  fragmentation, the exact pattern that pins RSS when freed objects scatter
  across partially-used slabs [redis-fragmentation-ratio], run once with the
  reclaimer off (the pinned default) [redis-activedefrag-default] and once with
  it on under the Redis-compatible thresholds [redis-activedefrag-thresholds] so
  the off-default deferral question from #43 gets direct evidence.
- A CLIENT TRACKING stream that registers, invalidates, and churns tracked keys
  past the table cap so spurious-invalidation eviction fires repeatedly, testing
  that the tracked-key table count and bytes settle at the cap (#21).
- A replication-backlog phase that holds a replica behind far enough to grow the
  backlog, then lets it drain, asserting the backlog buffer and any per-replica
  state return to baseline rather than ratcheting upward.

Sampled on a fixed cadence into a time-series:

- Resident bytes and used_memory from allocator introspection, and
  mem_fragmentation_ratio = RSS / used_memory [redis-fragmentation-ratio].
- Per-shard background-reclamation-queue depth and timing-wheel slot occupancy.
- Open file descriptors and live timer/wheel-handle count.
- Tracking-table byte size and tracked-key entry count.

Decision rule (thresholds stated as IronCache acceptance design, not measured
numbers):

- Sanitizer arm must report zero definite leaks and zero heap errors over the
  bounded window. A single definite leak fails the gate outright.
- Release arm: after an initial warm-up window discarded to let the allocator
  decay settle, the linear-regression slope of resident bytes over the remaining
  horizon must be statistically indistinguishable from flat, i.e. no sustained
  upward RSS drift once the working set is stable. A persistent positive slope
  fails.
- mem_fragmentation_ratio must stay bounded under a fixed ceiling for the whole
  horizon; it must not climb monotonically. With the reclaimer off this is the
  evidence for or against the #43 deferral; with the reclaimer on it must hold
  at least as tight a bound.
- File descriptors, timer/wheel handles, reclamation-queue depth, and
  tracked-key entries must each be bounded above by their respective caps with
  no upward trend; the reclamation queue must drain (return toward zero) between
  churn bursts rather than monotonically deepen, and the tracking table must
  settle at or below the cap.
- A failure on any single signal fails the gate; the gate is a conjunction, not
  an average.

Run cadence: the sanitizer arm fits a per-merge or nightly window; the full
release-arm horizon runs on the slower cadence shared with the Jepsen nightly
soak (#99), not per-PR, because the multi-day horizon does not fit a per-PR
time budget.

## What would change the decision

- The release-arm RSS slope is non-flat with the reclaimer off, meaning
  structural bounding alone does not hold RSS near the logical bound. That would
  resolve the #43 open question against deferral and pull the copy-relocate
  reclaimer build forward, or default it on once its numbers land.
- mem_fragmentation_ratio climbs past the ceiling under the fill-then-free
  cycles even with the reclaimer on, forcing a tighter threshold, a different
  reclaim cadence, or an allocator-decay change (the dirty-page decay window
  [jemalloc-decay-defaults] is the lever here).
- The expiry reclamation queue deepens monotonically under same-deadline churn,
  indicating the bounded-queue plus admission back-pressure design (#51, #137)
  does not actually bound resident expired-key memory, forcing inline-free
  fallback or a deeper queue bound.
- The tracking-table count or bytes exceed the cap, or tracked-key entries leak
  past spurious-invalidation eviction, breaking the #21 hard-cap promise and
  forcing the eviction path to be fixed before the table ships as default.
- The sanitizer arm reports any definite leak, which blocks all of the above:
  the leak is fixed first, then the soak re-runs.

## References

- Issues: #161 (this gate); #43 (defrag, structural bounding and off-default
  reclaimer); #51 (timing-wheel TTL, lazy backstop, bounded reclamation queue);
  #21 (hard-capped CLIENT TRACKING table; covered here, not a #161 predecessor);
  #8 (benchmark and load harness); #86 (INFO/metrics surface); #137 (admission
  back-pressure); #91 (advisor safety guardrails, adjacent); #99 (Jepsen
  nightly soak cadence); #100/#160 (DST seeded replay); #1 (vision EPIC).
- Specs: docs/design/DEFRAG.md, docs/design/EXPIRATION.md,
  docs/design/CLIENT_TRACKING.md.
- ADRs: ADR-0006 (default allocator and memory accounting), ADR-0004
  (memory-reclamation backbone), ADR-0002/ADR-0005 (shared-nothing per-shard),
  ADR-0022 (no fork, no huge pages, RSS posture).
- Claims (resolved via docs/prior-art/claims.yaml): [redis-fragmentation-ratio],
  [redis-activedefrag-thresholds], [redis-activedefrag-default],
  [jemalloc-decay-defaults].
