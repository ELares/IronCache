# Experiment: Hot-shard skew (Zipfian theta where per-shard eviction loss exceeds remap cost)

Issue: #170. Provisional decision: ADR-0002 (shared-nothing thread-per-core), ADR-0004 (memory-reclamation backbone), and ADR-0005 (per-shard unsynchronized map) already pin the shard-per-core stance and its hot-shard mitigation posture.

## Provisional decision (already pinned)

The keyspace is partitioned into N shards by `k = HASH(KEY) % N`, N at most the core count, each shard owned by one core with no shared mutable hot-path state (ADR-0002). The per-shard store is therefore an unsynchronized `hashbrown::HashMap` with no lock, atomic, or CAS on the hot path (ADR-0005). ADR-0002 names the residual risk explicitly: a single hot key or skewed range can saturate one core, and mitigation is owned by the hot-shard research (#32) and routing/migration design rather than shared hot-path state. ADR-0004 pins the reclamation backbone for that mitigation: the hot path uses no SMR machinery, and any genuinely cross-core structure off the hot path (named example: a cross-shard frequency sketch for global hot-key detection) uses off-the-shelf `crossbeam-epoch`, not a bespoke framework. This experiment does not re-decide the model; it measures the boundary at which the pinned mitigation must actually fire, and which detection mechanism to use.

## Why this is harness-blocked

The pinned decisions rest on a paper argument from #32: that under shared-nothing, per-shard eviction plus routing/migration beats shared global state. The open empirical question is the crossover point: at what Zipfian theta and key-cardinality does per-shard eviction's hit-rate loss exceed the cost of remapping or splitting a hot shard, and whether cross-shard detection needs a shared sketch at all. That crossover is a function of skew, cardinality, cache-to-working-set ratio, and the running eviction policy (S3-FIFO, ADR-0008). It cannot be read off the literature because no published trace fixes our shard count, our hash function, and our policy together. It requires the benchmark harness (#8) to drive a sharded build under controlled skew and observe per-shard versus global behavior. No harness, no measurement.

## Experiment to run

Corpus and workload: synthetic key streams generated at controlled Zipfian theta, cross-checked against at least one real cache trace from the #47 shared corpus. Use a generator with verified Zipf support (memtier `--key-zipf-exp`, or a YCSB-style Zipfian) so the skew parameter is exact and reproducible.

Fixed parameters: shard count N pinned to the host core count; hash function and S3-FIFO policy (ADR-0008) held at defaults; value size fixed; total offered load held constant across runs so only distribution shape varies; warm-up discarded before measurement.

Varied parameters: Zipfian theta swept across a range from near-uniform to heavily skewed; key-cardinality swept across several decades; cache-size-to-working-set ratio swept across a small set of points (over-provisioned, matched, under-provisioned). Detection mechanism swept as a third axis: (a) per-shard local frequency counters only, (b) a shared cross-shard frequency sketch reclaimed via `crossbeam-epoch` per ADR-0004, (c) key-splitting of the identified hot key across shards.

Measured: per-shard hit rate versus global (single-policy) hit rate; tail latency (p99, p99.9) on the hottest shard versus the median shard; throughput per core and the load imbalance across shards; for each detection mechanism, its added CPU and memory overhead and its detection latency (requests until a hot key is flagged); the remap or split cost in latency and transient hit-rate dip.

Decision rule: hold the shard-per-core stance unless, within the realistic theta and cardinality range, per-shard eviction hit-rate loss versus the global policy exceeds the measured remap/split cost AND a shared-sketch detector both lifts hit rate materially and is shown shareable without hot-path contention. Report the theta or cardinality threshold (if any) at which mitigation must fire as a configuration default. Recommend exactly one detection mechanism by measured overhead.

## What would change the decision

ADR-0002 and ADR-0005 are amended only if the data contradicts the shard-per-core stance: that is, if per-shard eviction loss exceeds remap/split cost across the realistic skew range, or if a shared sketch is required and cannot be shared without reintroducing hot-path contention. If a shared sketch is required but stays off the hot path, that confirms ADR-0004 as written (`crossbeam-epoch` for the shared structure) and changes nothing in ADR-0002/0005. If per-shard counters or key-splitting suffice, no ADR changes; only a mitigation default and its trigger threshold are recorded.

## References

- Issue #170 (this experiment), split from #32 (closed by ADR-0002/0004/0005).
- ADR-0002 shared-nothing thread-per-core (#24): names hot-shard saturation and assigns mitigation to the hot-shard research (#32) and routing/migration design.
- ADR-0004 memory-reclamation backbone (#33): `crossbeam-epoch` for an off-hot-path cross-shard frequency sketch; no SMR on the hot path.
- ADR-0005 per-shard unsynchronized map (#36): the per-shard store under test.
- ADR-0008 default eviction policy S3-FIFO (#46): the policy held fixed in this sweep.
- Benchmark harness #8 (blocking); shared-corpus benchmark #47.
- Zipf workload generation: [memtier-supports-zipfian], [ycsb-default-zipfian-constant].