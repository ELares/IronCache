# ADR index

Every accepted or proposed Architecture Decision Record, and the `[DECISION]`
issue it resolves. Format and rules: [README.md](README.md). Open decisions are
in [OPEN.md](OPEN.md); research questions in [QUESTIONS.md](QUESTIONS.md).

| ADR | Title | Status | Issue |
| --- | --- | --- | --- |
| [0001](0001-adopt-adrs-and-tenet-conflict-order.md) | Adopt ADRs and the ranked-tenet conflict order | Accepted | #4 |
| [0002](0002-shared-nothing-thread-per-core.md) | Shared-nothing thread-per-core concurrency model | Accepted | #24 |
| [0003](0003-design-for-determinism.md) | Design the runtime for determinism (DST) | Accepted | #31 |
| [0004](0004-memory-reclamation-backbone.md) | Memory-reclamation backbone | Accepted | #33 |
| [0005](0005-per-shard-unsynchronized-map.md) | Per-shard unsynchronized map | Accepted | #36 |
| [0006](0006-default-allocator-and-accounting.md) | Default allocator (jemalloc) and memory accounting | Accepted | #41 |
| [0007](0007-memory-ceiling-and-eviction-on-by-default.md) | Memory ceiling + eviction on by default | Accepted | #45 |
| [0008](0008-default-eviction-policy-s3fifo.md) | Default eviction policy is S3-FIFO | Accepted | #46 |

As `[DECISION]` issues close, each adds its row here and its `NNNN-*.md` record.
The numbering is monotonic and never reused, even after supersession.
