# Experiment: Eviction bake-off across SIEVE, S3-FIFO, W-TinyLFU, ARC, and LIRS

Issue: #47. Provisional decision: ADR-0008 pins S3-FIFO as the default eviction policy.

## Provisional decision (already pinned)

ADR-0008 (Accepted, issue #46) selects **S3-FIFO** as the default online eviction
policy, behind the pluggable `EvictionPolicy` trait (#48) so SIEVE and a
W-TinyLFU-fronted variant stay selectable. The pinned rationale: a small (~10
percent) probationary FIFO plus a large main FIFO with a ghost queue
[s3fifo-small-main-split], a 2-bit frequency counter capped at 3
[s3fifo-freq-counter-2bit-cap3], exploiting that most objects are one-hit wonders
[s3fifo-onehit-wonder-72pct]. SIEVE [sieve-miss-ratio-45pct]
[sieve-loc-and-stack-property] and a W-TinyLFU-fronted FIFO were considered and
kept selectable, not chosen as default. ADR-0008 states each algorithm's headline
"wins on N percent of traces" figure is its own home-corpus result, so #47
re-validates the call on a shared corpus; this doc holds that comparison at equal
memory, consistent with ADR-0008's bytes-per-key framing (bytes-per-key includes
per-entry policy metadata). This doc records that procedure. It does not re-decide.

## Why this is harness-blocked

The decision rule needs measured hit ratio and threaded throughput at equal
memory, which requires three things that do not exist yet:

- The benchmark harness and methodology of ADR-0016 (per-core throughput,
  memory-at-fixed-hit-ratio, open-loop tail latency); harness is #8.
- A trace-replay driver that ingests the cachemon 6594-trace corpus and emits
  byte-accurate (not slot-accurate) accounting per policy.
- Working implementations of all five policies plus the two LRU-class baselines
  behind the `EvictionPolicy` trait (#48).

Until the harness replays a shared corpus under one accounting model, any ranking
between S3-FIFO and the alternatives is a citation comparison across mismatched
home corpora, which is exactly what ADR-0008 flags as needing re-validation.

## Experiment to run

Corpus and workload:

- The cachemon 6594-trace corpus, replayed in full.
- Synthetic Redis-like KV traces carrying explicit value sizes and TTLs, since
  variable value sizes and TTL expiry are first-class in a Redis-compatible cache
  and absent from most academic traces. Both are injected so results transfer.
- Adversarial scan/churn set, run as three named patterns: sequential scan,
  periodic flush, and zipfian-with-churn.

Fixed parameters (held identical across all policies):

- Equal **byte** budget per run, not equal object count. Per-object policy
  metadata (counters, hands, ghost entries, pointers) is charged against the byte
  budget, applying ADR-0008's settled bytes-per-key rule so the accounting is
  honest about each policy's overhead.
- Hardware, thread pinning, and the shared-nothing shard layout (ADR-0002).
- Replay order and the TTL clock per trace.
- ADR-0016 measurement methodology (open-loop, coordinated-omission-corrected).

Varied parameters:

- Policy under test: S3-FIFO, SIEVE, W-TinyLFU-fronted FIFO, ARC, LIRS, plus
  segmented LRU [segmented-lru-defaults] and plain LRU as baselines.
- Byte budget swept across several cache-size points (small through large) to
  expose small-cache degradation.
- Thread count swept (single shard up to many shards) for the throughput curve.

Measured:

- Byte-accurate hit ratio per policy per size point, with the gap to the Belady
  optimal upper bound [parrot-imitation-belady-icml20] reported (#93).
- Per-core throughput and p99/p999 tail latency under ADR-0016 methodology.
- Per-policy metadata overhead, quantified under the ADR-0008 byte-accounting
  rule, so the bytes-per-key cost of each policy is a measured number.
- Hit-ratio retention under each adversarial scan/churn pattern (scan resistance).

Decision rule:

- Keep S3-FIFO as default if, at equal byte budget, it is within a small margin
  of the best hit ratio across the corpus AND leads or ties on per-core
  throughput AND does not collapse under the three adversarial patterns.
- Add an explicit scan-detection guard only if the chosen filter regresses under
  the adversarial set AND the guard does not regress common-case hit ratio
  (Compatible ranks above Scalable). ARC [arc-self-tuning-no-counts] and LIRS
  [lirs-irr-scan-resistance] stay as instrumented references for scan resistance.

## What would change the decision

- Another policy beats S3-FIFO on byte-accurate hit ratio by a margin that
  survives charging its per-object metadata against the byte budget.
- W-TinyLFU's admission filter and aging beat S3-FIFO's 2-bit counter by enough
  to justify its ~8-bytes-per-entry sketch [wtinylfu-cmsketch-4bit].
- S3-FIFO's hit ratio collapses under one of the adversarial patterns while a
  reference policy holds, forcing either a scan guard or a different default.
- The measured per-policy metadata overhead, charged into bytes-per-key under the
  ADR-0008 rule, reorders the top of the ranking once each policy pays its true
  cost.

## References

- ADR-0008: default eviction policy is S3-FIFO (provisional decision; issue #46).
- ADR-0016: headline metrics and benchmark methodology (issue #7).
- #8: benchmark harness. #48: `EvictionPolicy` trait. #49: W-TinyLFU filter.
- #93: Belady optimal upper bound. #46: parent.
- Claims (resolved via docs/prior-art/claims.yaml): [s3fifo-onehit-wonder-72pct],
  [s3fifo-small-main-split], [s3fifo-freq-counter-2bit-cap3],
  [sieve-miss-ratio-45pct], [sieve-loc-and-stack-property],
  [arc-self-tuning-no-counts], [lirs-irr-scan-resistance],
  [segmented-lru-defaults], [wtinylfu-cmsketch-4bit],
  [parrot-imitation-belady-icml20].