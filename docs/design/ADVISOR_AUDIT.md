# Design: Advisor decision and audit trail

Issue: #153. Decisions: ADR-0013 (advisor default posture is shadow/off).
Related: ADVISOR_SAFETY.md (#91, the safety mechanism this records),
ADVISOR.md (#126, the controller that emits events), ADVISOR_PROMOTION.md
(#154, the gate whose verdicts are logged), OBSERVABILITY.md (#86/#152, the
INFO/metrics surfaces), CONFIG.md (#85, the versioned snapshot store).

## Goal and scope

The advisor retunes deterministic knobs (active eviction policy, sampled count,
LFU log-factor and decay, ghost size, slab/encoding/compression thresholds), so
an operator must be able to answer "what did it change, why, and did it help?"
after the fact. This spec owns the durable, tamper-evident decision/audit log and
its INFO + `/metrics` projection. It is the diagnostic backbone for the #91
rollback and the record of every #154 promotion verdict. In scope: the event
schema, durability and tamper-evidence, retention, the queryable surface, and
shadow-mode emission. Out of scope: the safety mechanism itself (#91), the
promotion decision (#154), and the metric-registry transport (#86/#152).

## Design

### What an event records

- One append-only record per advisor action and per safety event. A knob-change
  record carries: monotonic snapshot version (from #91/#85), wall and logical
  time, knob id, from-value, to-value, the triggering expert or objective delta
  (which bandit/regret expert won and by how much [cacheus-experts]
  [lecar-regret-minimization-smallcache]), the replay evidence that it beat the
  static baseline (the #154 margin), and the seed. Safety records cover rollback
  and kill-switch trips with cause (which metric regressed, by how much, over
  which window). The objective the delta is measured against is hit ratio scored
  off the hot path, never a per-request shadow simulation
  [hit-ratio-can-hurt-throughput].

### Tamper-evidence and durability

- The log is a hash-chained append-only journal: each record commits the prior
  record's digest, so any edit or deletion in the middle breaks the chain and is
  detectable on read. It is written through the same fail-closed io_uring write
  path the persistence umbrella defines (PERSISTENCE.md, #58), not a side file, so
  a crash cannot silently lose the tail. The chain is verified at boot and a break
  is surfaced as a distinct INFO field and metric rather than panicking.

### Surfaced via INFO and /metrics

- Current advisor state lives in the native `# IronCache` INFO section (#152): the
  posture (off/shadow/active per ADR-0013), the live snapshot version, the active
  expert, the count of changes/rollbacks/kill-switch trips, and the last verdict.
  The same counters are Prometheus series in the versioned registry (#152) under a
  bounded label set (knob id from a fixed allow-list, no free-form cardinality).
  The decision log is not a high-cardinality metric: `/metrics` exposes aggregate
  counters and gauges, while the per-record detail is read through the query
  surface, keeping the scrape cheap (the OBSERVABILITY.md cardinality rule).

### Queryable surface

- A read-only admin verb returns recent records filtered by knob, version range,
  or event type (rollback/kill-switch/promotion), bounded in count like SLOWLOG.
  Records are immutable; there is no mutating verb on the journal. The query path
  is gated by the same auth posture as other introspection (MONITOR/metrics auth
  decision, SECRETS.md #145), and any secret-bearing field is redacted there too.

### Emitted even in shadow mode

- In shadow mode the advisor mutates nothing live (ADR-0013) yet records every
  recommendation it would have applied, with the same schema and the would-be
  from/to and replay evidence. This is the evidence the #90 headroom study and the
  #154 gate consume to decide whether active tuning is ever justified
  [wtinylfu-caffeine-sketch]: shadow logging is the safe first rung of the
  off -> shadow -> active ladder, producing an auditable trail before any knob
  moves.

### Retention

- Retention is bounded and configurable: a ring of the last N records plus all
  records since the current snapshot version, whichever is larger, so the full
  causal history of the live config is always present even after the ring wraps.
  Rollback and kill-switch records are retained at a higher floor than routine
  knob changes, because they are the post-incident record. Eviction of old records
  re-anchors the hash chain with a checkpoint digest so tamper-evidence survives
  truncation.

## Open questions

- The admin verb's exact name/shape (a SLOWLOG-style RESP reply vs a CONFIG-style
  subcommand), settled with the #150 admin-command surface.
- Whether the journal is per-shard (matching the shared-nothing core) with a
  merged read view, or a single core-0-owned log, and the seed scope this implies
  (the #91 per-shard-vs-global seed open question).
- Default retention floors for routine vs safety records, and whether the chain
  checkpoint digest is itself exported for external verification.

## Acceptance and test hooks

- Every applied knob change and every rollback/kill-switch trip produces exactly
  one chained record carrying snapshot version, from/to, trigger, margin, seed,
  and cause; a mid-journal edit is detected as a chain break on read.
- In shadow mode no knob mutates live yet the recommendation log grows with full
  schema (asserted against ADR-0013 posture).
- INFO advisor fields and the `/metrics` counters agree with the journal contents
  and stay within the #152 cardinality bound under an adversarial knob workload.
- A seeded replay reproduces an identical event stream (the #91 determinism
  invariant projected onto the log).

## References

- ADR-0013; issues #153, #91, #126, #154, #90, #85, #86, #152, #150, #145, #58,
  #1; specs ADVISOR.md, ADVISOR_SAFETY.md, ADVISOR_PROMOTION.md, OBSERVABILITY.md,
  CONFIG.md, SECRETS.md, PERSISTENCE.md.
- Claims: [cacheus-experts], [lecar-regret-minimization-smallcache],
  [hit-ratio-can-hurt-throughput], [wtinylfu-caffeine-sketch].
