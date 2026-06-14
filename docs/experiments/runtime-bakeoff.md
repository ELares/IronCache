# Experiment: Runtime bake-off, monoio vs glommio vs tokio+epoll on GET/SET

Issue: #26. Provisional decision: ADR-0002 (shared-nothing thread-per-core) and docs/design/RUNTIME.md pin the shape; the concrete crate is left open behind a swappable seam (#27).

## Provisional decision (already pinned)

ADR-0002 pins the concurrency model: shared-nothing thread-per-core, one runtime per pinned physical core, keyspace sharded by `k = HASH(KEY) % N`, no shared mutable hot-path state, cross-shard work as explicit message passing. RUNTIME.md (#25) pins the async/io shape on top of that: per-core stackless-async executors, an io_uring fast path with an epoll/kqueue portable fallback, and an Env seam for determinism. RUNTIME.md explicitly does NOT pin the concrete crate: it states the runtime is "monoio/glommio-class on io_uring, not tokio work-stealing," and defers the final choice to this bake-off behind the #27 swappable abstraction. This experiment does not re-open the thread-per-core decision; it only selects the crate (or confirms the tokio+epoll fallback as default) on measured GET/SET.

## Why this is harness-blocked

The published numbers that motivate thread-per-core come from echo and proxy benchmarks at high connection counts [monoio-vs-tokio-scaling] [glommio-context-switch-vs-io], not from a small-value GET/SET mix with a realistic key distribution and cross-shard traffic. No conclusion is possible from prior art alone because: the workload differs, the cross-shard coordinator cost (#29) is not captured by single-key echo, and the io_uring-vs-epoll gap shifts with the host kernel. Settling the crate requires building the harness on all three runtimes and running it on both an io_uring host and an epoll-only host; that work does not exist yet.

## Experiment to run

Workload and corpus:
- Two harnesses per runtime: (a) raw echo, to anchor against the published claims; (b) a minimal KV harness implementing GET and SET over the shard rule `k = HASH(KEY) % N`.
- Value sizes: a small-value distribution (e.g. mostly tens-to-hundreds of bytes) representative of cache traffic; record the exact distribution used.
- Key distribution: a realistic skewed distribution (record the chosen zipfian parameter), not uniform, so hot-shard effects are visible.
- Connection counts swept across low, medium, and high (record exact counts) to reproduce the regime where the 2x/3x claims are made.

Fixed vs varied parameters:
- Varied: runtime (monoio, glommio, tokio+epoll); core count (1, 4, 16, each pinned); multi-key traffic fraction (0%, 5%, 25%, 50%) routed through the cross-shard coordinator (#29); host backend (io_uring host vs epoll-only host).
- Fixed within a run: shard count N relative to cores, value-size and key distributions, connection count per core-count tier, build flags, request count and warmup.

What is measured:
- Throughput per core at each core count.
- Latency p50, p99, p99.9 at each core count.
- Cross-shard coordinator cost: throughput and tail latency at each multi-key fraction relative to the 0% baseline.
- Sensitivity: the same matrix on the io_uring host vs the epoll-only host.
- A maintainability and unsafe-surface scorecard per runtime: crate version and MSRV [monoio-version] [glommio-version-msrv] [tokio-version-msrv], kernel-feature coverage, and a count/classification of unsafe pushed into application code by each completion model.

Decision rule:
- Adopt a thread-per-core crate (monoio or glommio) as default ONLY if it shows a clear throughput-per-core and tail-latency win over tokio+epoll on the GET/SET mix at 4 and 16 cores that survives the realistic multi-key fraction; between monoio and glommio, prefer the one with the better win plus lower unsafe surface and acceptable MSRV.
- If no thread-per-core crate wins on GET/SET at 4 and 16 cores, or if its win is erased by the expected multi-key fraction, default to tokio+epoll and keep the thread-per-core crate behind #27 only as an opt-in.
- Regardless of winner, tokio+epoll remains a first-class fallback target (Compatible tenet) because it runs on kernels without io_uring.

## What would change the decision

- The 2x/3x scaling claims [monoio-vs-tokio-scaling] failing to reproduce for GET/SET (as opposed to echo) at 4 and 16 cores.
- A multi-key fraction at or below the expected production mix erasing the per-core win via cross-shard coordination cost.
- A kernel-feature gap in monoio or glommio that forces a fallback path we must maintain anyway, undercutting the simplicity argument.
- An unsafe-surface or MSRV cost on the winning crate judged too high against the measured gain.
- Results inverting between the io_uring host and the epoll-only host such that the default deployment target changes (feeds #28).

## References

- Issues: #26 (this spike), #25 (core runtime, RUNTIME.md), #27 (runtime abstraction seam), #29 (cross-shard coordinator), #28 (io_uring fast path), #9 (single-core throughput bar), #1.
- ADRs/docs: ADR-0002 (shared-nothing thread-per-core), docs/design/RUNTIME.md, docs/research/concurrency-runtime-rust.md, docs/research/dragonfly.md.
- Claims (docs/prior-art/claims.yaml): [monoio-vs-tokio-scaling], [glommio-context-switch-vs-io], [monoio-version], [glommio-version-msrv], [tokio-version-msrv].