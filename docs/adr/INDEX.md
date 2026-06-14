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
| [0009](0009-compatibility-tiering.md) | Compatibility tiering (Tier 0-4), behavioral equivalence | Accepted | #16 |
| [0010](0010-transaction-and-scripting-surface.md) | Transaction + scripting surface scope | Accepted | #30 |
| [0011](0011-single-node-first-slot-ready.md) | Single-node-first, slot-ready layout | Accepted | #69 |
| [0012](0012-scale-out-targets.md) | Headline scale-out targets | Accepted | #146 |
| [0013](0013-advisor-default-posture.md) | Advisor default posture (shadow/off) | Accepted | #155 |
| [0014](0014-durability-stance.md) | Durability stance (ephemeral default) | Accepted | #59 |
| [0015](0015-default-value-codec.md) | Default value codec (zstd low) | Accepted | #53 |
| [0016](0016-headline-efficiency-metrics.md) | Headline metrics (per-core, memory-at-hit-ratio) | Accepted | #7 |
| [0017](0017-per-tenet-acceptance-gates.md) | Per-tenet acceptance targets and release gates | Accepted | #157 |
| [0018](0018-encoding-conversion-thresholds.md) | Fixed encoding-conversion thresholds | Accepted | #37 |
| [0019](0019-resp3-reply-shaping.md) | RESP3 reply-shaping policy and error fidelity | Accepted | #17 |
| [0020](0020-cli-dispatch-and-signing.md) | CLI mode dispatch (clap) and artifact signing (minisign) | Accepted | #82 |
| [0021](0021-c-bound-zstd-vs-pure-rust.md) | C-bound zstd default, pure-Rust behind a feature | Accepted | #54 |
| [0024](0024-geo-command-scope.md) | Geo command family scope (non-goal for v1) | Accepted | #133 |

As `[DECISION]` issues close, each adds its row here and its `NNNN-*.md` record.
The numbering is monotonic and never reused, even after supersession.
