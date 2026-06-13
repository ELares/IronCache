# Research-question map

Each open research question harvested from the prior-art corpus
([`../research/`](../research/)) and the issue that resolves it. A research
issue closes by recording a conclusion (a recommendation grounded in the
pinned claims) and, where a question needs measurement we cannot do on paper,
by filing an empirical-validation follow-up issue rather than leaving the
question open.

| Research issue | Question | Feeds decision |
| --- | --- | --- |
| #6 prior-art foundations | Is the competitor landscape pinned and verified? | all |
| #9 single-core throughput bar | What is the honest per-core bar vs Redis/Valkey/Dragonfly/Garnet? | #7 |
| #26 runtime bake-off | monoio vs glommio vs tokio+epoll on GET/SET? | #24, #27 |
| #32 hot-shard mitigation | How to handle a hot shard and reclaim memory under shard-per-core? | #33, #36 (resolved on paper: ADR-0004/0005, reclamation needs no concurrent machinery on the owned hot path; empirical skew benchmark filed as #170) |
| #37 encoding thresholds | Adaptive vs fixed listpack/intset conversion thresholds? | #35, #37 |
| #42 allocator bench | jemalloc vs mimalloc vs snmalloc under a cache workload? | #41 (decided: ADR-0006 jemalloc on introspection/defrag-hint grounds; #42 re-milestoned to M1 as empirical validation, gated on #8) |
| #47 eviction bench | SIEVE vs S3-FIFO vs W-TinyLFU vs ARC/LIRS on real traces? | #46 (decided: ADR-0008 S3-FIFO default; #47 re-milestoned to M1 as empirical validation on cachemon + our traces) |
| #57 value-size survey | Value-size and compressibility distribution? | #52, #53 |
| #61 snapshot overhead | Bound snapshot memory overhead and fast restart? | #59, #60 |
| #78 per-shard Raft | Feasibility of an opt-in strongly-consistent tier? | #76, #78 |
| #80 consistent hashing | Post-ketama placement for internal sharding? | #71, #80 |
| #89 advisor objective | What objective metric should the advisor optimize? | #88 |
| #90 advisor headroom | Does an advisor beat a tuned W-TinyLFU+SIEVE baseline? | #88, #155 |
| #162 second-tier landscape | What do Aerospike/Tarantool/Kvrocks/Ignite teach us? | #65, #66, #68 |

When a research issue closes, its row notes the conclusion and any
empirical-validation follow-up filed.
