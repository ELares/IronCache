# ADR-0030: Probabilistic membership types beyond HLL (adopt Bloom-first, staged)

Status: Accepted
Issue: #416

## Context

IronCache already ships HyperLogLog (PFADD, PFCOUNT, PFMERGE), the cardinality
member of the probabilistic family. The rest of that family was brought into
Redis 8 core from the former RedisBloom module: Bloom filters (BF), Cuckoo
filters (CF), Count-Min Sketch (CMS), Top-K, and t-digest. Valkey ships the same
surface staged, leading with a Bloom module, and Dragonfly serves BF plus CMS and
Top-K. The question is which, if any, of these IronCache should adopt, and in
what order.

The tenet order is Compatible greater than Efficient greater than Simple greater
than Scalable greater than AI-Driven. The probabilistic family splits cleanly
along that order. Bloom and Cuckoo filters are directly cache-shaped: a Bloom
filter in front of a backing store is the canonical cache-penetration guard and
negative-lookup short-circuit, it is small and fixed-size, and it pulls on
Compatible and on Efficient (the second tenet) at a modest Simple cost (a bitset
plus a fixed hash scheme, with no graph index and no per-request tuning). CMS,
Top-K, and t-digest are analytics surfaces (frequency estimation, heavy hitters,
quantiles) with much weaker cache relevance and more representational surface
area. The project's own research corpus already studied Cuckoo filters and the
HLL sparse-versus-dense ladder, so the prior art is in hand.

## Decision

Adopt **Bloom-first**, staged, as the direction for the probabilistic family,
without committing the build to the current milestone:

- Bloom filters are accepted as a future native type (BF.RESERVE, BF.ADD,
  BF.MADD, BF.EXISTS, BF.MEXISTS, BF.CARD, BF.INFO, BF.INSERT), gated by a scoped
  design issue rather than landed here. This is the one probabilistic surface the
  tenet order actively favors building, because it serves a real caching pattern
  and is Efficient-aligned and binary-size-friendly.
- The design issue owns the open choices: whether scalable filters (BF.SCANDUMP,
  BF.LOADCHUNK) and DUMP/RESTORE interop are in the first cut, the false-positive
  and memory operating point it must prove under the benchmark harness per the
  no-claim-without-its-test rule, and the encoding for persistence and
  replication.
- Cuckoo filters are a fast follow to Bloom under the same design issue if and
  when deletion support is needed (the capability Bloom lacks), decided on the
  same Efficient grounds.
- CMS, Top-K, and t-digest are declined for now as analytics with low cache
  relevance. They are not a hard non-goal; they are deferred behind demonstrated
  demand, kept distinct from the Bloom decision so the analytics surface does not
  ride in on the membership-filter case.

## Rejected Alternatives

- **Adopt the full RedisBloom surface at once (BF plus CF plus CMS plus Top-K plus
  t-digest).** Rejected on Simple and Efficient: it bundles three analytics
  structures with weak cache relevance into the membership-filter decision,
  widening the type surface and the conformance burden for capabilities the cache
  use cases do not pull on. The tenet order favors the tight, staged scope.
- **Decline the whole family and keep only HLL.** Rejected on Efficient and
  Compatible: Bloom is the clearest Efficient-aligned data-type expansion in the
  modern Redis surface and a genuine cache primitive, so a blanket decline leaves
  a real, charter-favored capability on the table. The cost it adds is a bitset
  and a fixed hash scheme, not a standing complexity burden.
- **Build Bloom in this milestone rather than behind a design issue.** Rejected on
  process, not on direction: the membership-and-false-positive contract,
  scalable-filter behavior, and the persistence and replication encoding need a
  scoped design and a benchmark gate before code, per the project's
  research-before-architecture rule and the no-claim-without-its-test tenet.

## Consequences

- The probabilistic roadmap is explicit: HLL today, Bloom accepted and next
  (behind a design issue), Cuckoo as its deletion-capable follow, and CMS, Top-K,
  and t-digest deferred as analytics. NON_GOALS records the analytics deferral so
  it is not read as a permanent exclusion.
- A future Bloom design issue carries the memory-and-false-positive proof, the
  scalable-filter and interop scope, and the wire and on-disk encoding; no engine
  work is committed by this ADR beyond the direction.
- Reopening the analytics structures is cheap (new types, no engine change), so a
  later demand signal lands them without superseding this decision.
