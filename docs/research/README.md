# IronCache research corpus

This directory holds the prior-art research that seeds IronCache. Each document
covers one dimension of caching, grounded in primary sources, with
version-pinned claims tracked in [`../prior-art/claims.yaml`](../prior-art/claims.yaml).
The full machine-readable dataset (summaries, mechanisms, claims, papers, and
proposed issues) is [`corpus.json`](corpus.json).

## How this was produced

A fan-out of AI research agents, one per dimension, each gathered claims from
official docs, source code, release notes, and papers. Every load-bearing or
non-high-confidence claim was then re-checked by an independent adversarial
verifier; corrections are folded into `claims.yaml`. This is the project method:
use AI to mine the world, then adversarially verify before trusting it.

## Dimensions

- [AI and ML for caching](ai-ml-caching.md) (`area:ai`) — 11 claims, 10 mechanisms, 9 papers
- [Benchmarking and correctness](benchmarking-correctness.md) (`area:testing`) — 22 claims, 11 mechanisms, 5 papers
- [Compression and compact encoding](compression-encoding.md) (`area:compression`) — 18 claims, 10 mechanisms, 5 papers
- [Rust concurrency and async runtimes](concurrency-runtime-rust.md) (`area:concurrency`) — 25 claims, 11 mechanisms, 5 papers
- [Distributed clustering and consistency](distributed-clustering.md) (`area:replication`) — 23 claims, 15 mechanisms, 8 papers
- [DragonflyDB (shared-nothing, thread-per-core)](dragonfly.md) (`area:concurrency`) — 20 claims, 11 mechanisms, 4 papers
- [Cache eviction and admission algorithms](eviction-algorithms.md) (`area:eviction`) — 19 claims, 10 mechanisms, 10 papers
- [Microsoft Garnet (Tsavorite log-structured store)](garnet.md) (`area:persistence`) — 20 claims, 12 mechanisms, 5 papers
- [KeyDB (multi-threaded Redis fork)](keydb.md) (`area:concurrency`) — 19 claims, 8 mechanisms, 5 papers
- [Memcached (slab allocator, segmented LRU)](memcached.md) (`area:memory`) — 13 claims, 8 mechanisms, 4 papers
- [Memory allocators and layout](memory-allocators.md) (`area:memory`) — 23 claims, 10 mechanisms, 5 papers
- [Operability, single-binary build, distribution](ops-build-distribution.md) (`area:build`) — 18 claims, 14 mechanisms, 3 papers
- [Storage engines and fork-less snapshots](persistence-storage-engines.md) (`area:storage`) — 20 claims, 10 mechanisms, 5 papers
- [Redis OSS core architecture](redis-core.md) (`area:protocol`) — 29 claims, 9 mechanisms, 6 papers
- [Redis object encodings and memory layout](redis-datastructures.md) (`area:datastructures`) — 18 claims, 10 mechanisms, 5 papers
- [Redis persistence and durability](redis-persistence.md) (`area:persistence`) — 16 claims, 10 mechanisms, 4 papers
- [Redis replication and Cluster](redis-replication-cluster.md) (`area:replication`) — 26 claims, 10 mechanisms, 4 papers
- [The RESP protocol and Redis contract](resp-protocol-compat.md) (`area:protocol`) — 16 claims, 15 mechanisms, 4 papers
- [Valkey (the Linux Foundation fork)](valkey.md) (`area:concurrency`) — 19 claims, 9 mechanisms, 5 papers
