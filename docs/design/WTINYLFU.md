# Design: W-TinyLFU frequency admission filter (CM-sketch, doorkeeper, aging)

Issue: #49. Decisions: ADR-0008 (S3-FIFO default eviction, this filter is the
selectable W-TinyLFU-fronted variant, not the default), ADR-0005 (per-shard
unsynchronized map), ADR-0002 (shared-nothing thread-per-core), ADR-0003 (design
for determinism), ADR-0013 (advisor default posture). Related: #48 (EvictionPolicy
trait seam, EVICTION.md), #47 (the bake-off), #8 (harness), #126 (advisor), #111
(object layout).

## Goal and scope

This spec defines the frequency-admission filter that fronts cache admission for
the selectable W-TinyLFU-fronted eviction variant. The filter is consulted only
when a candidate is about to be admitted over an eviction victim: it estimates
whether the incoming key has been seen more often than the incumbent, and admits
only if so. It is the deterministic, non-ML state-of-the-art admission floor that
any learned admission idea (#126) must beat on replayed traces before it reaches
the data path. Scope is the estimator (sketch), its aging, the optional
doorkeeper, the decision-path contract, and the parameter knobs. The default
eviction core (S3-FIFO) is ADR-0008 and is not re-litigated here; the trait seam
this filter plugs into is #48 (EVICTION.md). Whether the filter is selected at all
is a per-tenant policy choice, not a default.

### Reconciliation with EVICTION.md (the window is dropped)

The merged EVICTION.md (#48) describes this same selectable variant as carrying a
small LRU admission window whose access may relink the window on a hit
[wtinylfu-window-main-split] (EVICTION.md lines 30-31, 38-39, 96-97). This spec is
the detailed design for the variant and REFINES that description: IronCache's
W-TinyLFU-fronted variant keeps only the frequency comparison at admission and has
NO SLRU window and NO per-hit relink, so its read path stays the FIFO-class core's
in-place metadata write. Where EVICTION.md and this spec differ on the window, this
spec governs; this same change amends EVICTION.md to drop the window-bearing
language and the per-access sketch increment so the two specs agree. The rationale is below
(Decision-path contract): adopting Caffeine's window would reintroduce the per-hit
list-relink contention the FIFO-class core exists to avoid.

## Design

### Estimator: 4-bit count-min sketch

Frequency lives in a 4-bit count-min sketch costing about 8 bytes per cache entry,
out of the object (not the per-entry 2-bit field S3-FIFO and SIEVE fold into the
kvobj, #111) [wtinylfu-cmsketch-4bit]. A counter saturates at 15; an
admission/eviction evaluation does a minimum-increment across the depth rows for
the evaluated key (bump only the smallest cells) to bound overestimation. That
increment runs on the decision path, not on the GET hot path (see Decision-path
contract below), so the sketch tracks the stream of admission candidates and
victims rather than every read. This decouples frequency from per-object storage so cold keys cost
near zero, unlike Redis's per-object Morris counter that pays an 8-bit field plus
a 16-bit decay timestamp on every key including one-hit-wonders
[redis-lfu-counter-encoding] [redis-lfu-log-factor], and at higher fidelity than
Redis's 5-key victim sampling [redis-lru-lfu-sampling] [redis-maxmemory-samples-5].
The sketch is the published non-ML SOTA, deterministic and inference-free
[wtinylfu-caffeine-sketch], borrowed wholesale.

### Aging: periodic halving

To bound staleness and let the estimate track phase changes, all counters are
halved (right-shift by one) once the running sample count reaches a threshold
proportional to the shard's cache maximum (Caffeine resets at 10x the maximum)
[wtinylfu-cmsketch-4bit]. Halving is a single linear pass over the sketch words,
run off the read path. The aging interval (counts vs wall-clock, and its bounds)
is an advisor knob, see below.

### Doorkeeper: optional, OFF by default

The TinyLFU paper fronts the sketch with a doorkeeper bloom filter so a first-seen
key is recorded in one bit instead of consuming sketch counters, cheaply filtering
one-hit-wonders (median about 72% on the first-10%-of-unique sequences that stress
admission) [s3fifo-onehit-wonder-72pct]. We adapt it as an OPTIONAL stage, OFF by
default, because Caffeine's production W-TinyLFU ships without a doorkeeper
[wtinylfu-cmsketch-4bit]; turn it on only where the measured one-hit-wonder share
is high enough to justify the extra bloom and its reset cadence.

### Decision-path contract (NOT the GET hot path)

The filter is invoked only at admission/eviction, when a shard is at its memory
budget and a candidate would displace a victim, never on the GET hot path. The
decision reads the sketch estimate for the candidate and for the victim; a sketch
lookup is a small bounded (depth-many) set of reads on the owning core, so the
decision path stays lock-free under shared-nothing (ADR-0002, ADR-0005)
[glommio-locks-never-necessary]. We deliberately reject Caffeine's SLRU window/main
scaffolding [wtinylfu-window-main-split]: its per-hit promotion would reintroduce
the list-relink contention that makes throughput fall at high hit ratio, the very
reason the FIFO-class core exists [hit-ratio-can-hurt-throughput]. We keep only the
frequency comparison at admission (see the reconciliation note above; this is the
spot where this spec drops EVICTION.md's window). The GET read path is unchanged:
the sketch is not bumped per read, so the read path remains a single in-place
metadata write with no list relink [hit-ratio-can-hurt-throughput], over the
FIFO-class core's in-object 2-bit counter [s3fifo-freq-counter-2bit-cap3].

### Tie-break: incumbent wins

When the candidate and victim estimate equal frequency, the newcomer is REJECTED
in favor of the incumbent (admit only on a strict win for the candidate). This
biases against churn from unproven scan keys, consistent with the one-hit-wonder
motivation [s3fifo-onehit-wonder-72pct], and makes the decision deterministic for
the DST and trace-replay harness (ADR-0003 determinism, #47).

### Provisional parameters (advisor-tunable knobs, NOT measured results)

Every number below is a PROVISIONAL starting point flagged for the #47 bake-off and
the #8 harness, and exposed as a bounded knob the advisor (#126) may tune within
ADR-0013's posture. None is a measured IronCache result.

- Sketch scope: lead with PER-SHARD sketches (lock-free, matches thread-per-core).
  A shared sketch sees cross-shard hot keys but every touch contends; per-shard is
  lock-free but a key hot globally yet thin per shard can be under-counted and
  wrongly rejected. The hit-ratio delta from partitioning the signal is measured in
  #47 before committing.
- Width and depth: provisional depth 4 rows, width sized to about 8 bytes per
  entry [wtinylfu-cmsketch-4bit]; width/depth and the false-positive budget under
  the minimal-memory goal are a #47/#8 knob.
- Doorkeeper cadence: provisional reset aligned to sketch aging; OFF by default,
  its bloom size and false-positive budget tuned only when enabled.
- Aging interval: provisional 10x cache-max sample count [wtinylfu-cmsketch-4bit];
  exposed as a bounded advisor knob (counts vs wall-clock, with min/max bounds).

## Open questions

- Per-shard vs shared sketch: the hit-ratio cost of partitioning the signal,
  measured on the trace corpus in #47 before the call is fixed.
- Doorkeeper default reset cadence relative to sketch aging, and the one-hit-wonder
  share at which enabling it pays for itself.
- Sketch width/depth and false-positive budget under the bytes-per-key target, and
  whether the advisor may move them per tenant or only globally (ADR-0013).
- Aging interval semantics (sample count vs wall-clock) and its safe min/max bounds
  as an advisor knob.

## Acceptance and test hooks

- The variant uses a 4-bit CM-sketch at about 8 B/entry with halving aging and an
  optional doorkeeper OFF by default [wtinylfu-cmsketch-4bit] [wtinylfu-caffeine-sketch].
- The filter is invoked only at admission/eviction, never per GET; a hot-path
  lint/test asserts no per-read sketch mutation [hit-ratio-can-hurt-throughput].
  (DEFERRED: the PR-3c first-cut implementation samples the full read stream with
  an inline O(depth) min-increment in `on_access` instead, a conscious divergence
  for a fuller frequency signal; the decision-path-only model, a read buffer drained
  off the GET critical path, and this no-per-read-mutation lint are tracked
  follow-ups for the harness bake-off (#47/#8) and are NOT yet met.)
- The sketch decision path is a small bounded (depth-many) lock-free set of reads
  on the owning core [glommio-locks-never-necessary]; the SLRU window is absent (no
  per-hit relink), refining EVICTION.md per the reconciliation note above.
- On a frequency tie the candidate is rejected (deterministic, replay-checked).
- All numeric parameters are exposed as bounded advisor knobs and validated in the
  #47 bake-off on the cachemon corpus via the #8 harness; the filter merges as the
  non-ML floor, with no learned admission idea admitted until it beats it on
  replayed traces.
- Plugs into the #48 EvictionPolicy trait as the W-TinyLFU front-end; the core
  defaults to S3-FIFO per ADR-0008.

## References

- ADR-0008, ADR-0005, ADR-0002, ADR-0003, ADR-0013; issues #48, #47, #8, #126, #111;
  specs EVICTION.md.
- Claims: [wtinylfu-cmsketch-4bit], [wtinylfu-caffeine-sketch],
  [wtinylfu-window-main-split], [redis-lfu-counter-encoding], [redis-lfu-log-factor],
  [redis-lru-lfu-sampling], [redis-maxmemory-samples-5], [s3fifo-onehit-wonder-72pct],
  [s3fifo-freq-counter-2bit-cap3], [hit-ratio-can-hurt-throughput],
  [glommio-locks-never-necessary].
