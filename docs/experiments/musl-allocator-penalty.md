# Experiment: musl allocator-contention penalty (musl malloc / static mimalloc / static jemalloc) on the IronCache cache workload

Issue: #124. Provisional decision: ADR-0006 (default global allocator and
memory-accounting strategy); this SPIKE feeds, and does not own, that decision.

## Provisional decision (already pinned)

Two things are already pinned, and this experiment re-decides neither. First,
IronCache does NOT rely on musl malloc as-is: if a musl build ships, a real
allocator is statically linked INTO the binary, because musl's own malloc is
known to be contended under threads and the whole point of the static musl target
is a kernel-only, zero-runtime-dependency binary [rust-musl-crt-static-default]
(the Simple gate in ADR-0017, realized by CLI_BINARY.md). Second, ADR-0006
already pins tikv-jemallocator (jemalloc 5.3.1) [tikv-jemallocator-version] as the
default `#[global_allocator]` on introspection and online-defrag grounds
(`mallctl` accounting [redis-maxmemory-accounting], `je_get_defrag_hint()`
[redis-jemalloc-frag-hint] [redis-active-defrag-jemalloc]), with mimalloc
[mimalloc-rust-version] and snmalloc [snmalloc-rs-version] kept as build-time
alternatives.

What is deferred to this experiment's result is narrower: whether the static musl
build is the PERFORMANCE default or only a portable fallback to the
glibc-version-pinned gnu build, and which allocator gets linked into musl when it
ships. This SPIKE does not re-open the jemalloc-vs-mimalloc default decision of
ADR-0006 (that is the broader bake-off, #42); it isolates the musl-specific axis:
the penalty musl malloc carries under contention, and whether a statically linked
mimalloc or jemalloc erases it on a musl target. The result is recorded as a
comment on the closed #41 (ADR-0006), per #124's done-when, with an explicit
default-vs-fallback recommendation for musl.

## Why this is harness-blocked

The two load-bearing facts cannot be settled on paper, only on the real workload.

First, the musl-malloc penalty is a contention property, not a citable number.
The parent #84 asserts musl malloc is "notably slow under thread contention," but
the size of that penalty on the IronCache shard structure under eviction churn is
unknown until measured; vendor microbenchmarks do not stand in for it. mimalloc's
headline figures are unrepresentative: they come from a single 2021 run on one
fixed 16-core AMD Ryzen 5950x, and the comparators do not line up the way a casual
read suggests (the about 13 percent leanN speedup is over tcmalloc, while the over
2.5x sh6bench figure is the one over jemalloc) [mimalloc-benchmarks]. Neither is a
churny eviction-driven cache, and none of those runs is on a musl target, which
is exactly the axis #124 exists to measure.

Second, jemalloc's idle-RSS must be accounted honestly or the comparison is
rigged. jemalloc enables a per-thread cache by default and the default maximum
cached size class is 32 KiB [jemalloc-tcache-defaults]; with `narenas = 4 * ncpus`
[jemalloc-narenas-default], per-thread and per-arena retained pages inflate idle
RSS in a way that is invisible to a single-thread microbenchmark and that a naive
idle-RSS reading would charge to jemalloc unfairly (or, if the tcache is flushed
first, unfairly in its favor). RSS return under churn is governed by allocator
defaults that must be swept, not assumed: jemalloc dirty/muzzy decay
[jemalloc-decay-defaults] and mimalloc's purge delay [mimalloc-purge-defaults].
Resolving the penalty therefore needs a harness driving the real shard structure
on a musl-linked build, instrumented for RSS and used_memory
[redis-fragmentation-ratio], with the jemalloc tcache state defined explicitly
before each idle reading. That harness and the musl-linked builds do not exist
yet, so the issue is blocked on running the experiment.

## Experiment to run

Corpus and workload, driven through the actual shard data structure (not a
synthetic alloc loop) so the size-class distribution is real, in three phases
that map to #124's stated legs:
- Phase A (single-thread): small mixed-KV fill and steady GET/SET on one pinned
  core, values dominated by <=64B (the cache common case). This is the
  single-core-parity leg; report per-core, since single-core parity is the hard
  case [dragonfly-single-core-parity].
- Phase B (multi-thread churn): sustained eviction churn held at `maxmemory`
  across the full core count, where musl malloc's lock contention is expected to
  show; the headline 25x-style figures are exactly the kind of thread-asymmetric
  comparison this phase must avoid by reporting per-core, not aggregate
  [dragonfly-25x-thread-asymmetry].
- Phase C (idle-RSS): fill to peak, free about 40 percent, hold idle, and read
  RSS and used_memory after the idle hold [redis-fragmentation-ratio]. jemalloc
  idle-RSS is read in two explicitly labeled states: tcache-as-default (live
  per-thread caches and retained arena pages [jemalloc-tcache-defaults]
  [jemalloc-narenas-default]) and tcache-drained (caches flushed, decay forced),
  so the tcache contribution is reported as a line item rather than silently
  inflating or deflating the comparison.

Candidates (versions pinned by #84's build matrix): musl malloc as-is (the
baseline the experiment exists to beat), statically linked tikv-jemallocator
(jemalloc 5.3.1) [tikv-jemallocator-version], and statically linked mimalloc crate
[mimalloc-rust-version], each linked into the same `x86_64-unknown-linux-musl` and
`aarch64-unknown-linux-musl` binaries [rust-musl-crt-static-default] built on
cargo-zigbuild [cargo-zigbuild-version-features]. The glibc-version-pinned gnu
build running the ADR-0006 default jemalloc is carried as the reference the musl
build must match to earn "performance default." snmalloc [snmalloc-rs-version] is
out of scope here; it is a separate build-time bet in #42.

Fixed: workload phases, key/value size distribution, `maxmemory`, seed, build
profile, per-machine warmup, and the same shard structure across all candidates.
Varied: allocator linked into musl (the three candidates), thread count (Phase A
one core, Phase B full core count), decay/purge knob across at least
{fast, default, slow} (`dirty_decay_ms` for jemalloc [jemalloc-decay-defaults],
`MIMALLOC_PURGE_DELAY` for mimalloc [mimalloc-purge-defaults]), and the jemalloc
tcache state (default vs drained) for the Phase C reading only. Every run repeated
across the canonical hardware matrix including a 1-vCPU machine.

Measured, per candidate per machine: Phase A single-core throughput and p99.9
latency; Phase B throughput-per-core and p99.9 under churn (never aggregate QPS);
Phase C post-idle RSS and RSS/used_memory [redis-fragmentation-ratio], with the
jemalloc figure reported separately for the tcache-default and tcache-drained
states so the per-thread-cache inflation [jemalloc-tcache-defaults] is an
attributed line item. No numbers are recorded in this doc; it fixes the
procedure.

Decision rule, in two parts. (1) Allocator-into-musl: rank musl malloc, static
jemalloc, and static mimalloc on Phase B throughput-per-core and p99.9 first,
then Phase C post-idle RSS (jemalloc charged its tcache-default RSS, not the
drained figure, since that is what a running server pays). musl malloc as-is wins
the slot only if it is within noise of the best linked allocator on every leg;
otherwise the static allocator that ADR-0006 already prefers (jemalloc) is linked
in unless mimalloc beats it on both Phase B per-core throughput and Phase C
post-idle RSS across the matrix. (2) musl default-vs-fallback: musl is the
performance default only if the best musl-linked build matches the
glibc-pinned gnu reference within noise on Phase A and Phase B per-core across the
matrix including the 1-vCPU machine; if it trails on any machine, musl is recorded
as the portable fallback and gnu stays the performance default. Either way the
outcome is posted to #41 (ADR-0006) as required by #124, and it can only reorder
ADR-0006's own allocator default, not set it, since the broader bake-off (#42)
owns that.

## What would change the decision

- musl malloc landing within noise of the best statically linked allocator on
  Phase B throughput-per-core and p99.9 AND on Phase C post-idle RSS across the
  matrix, which would let musl ship its own malloc and drop the static-link step
  for #84.
- static mimalloc reclaiming Phase C post-idle RSS as cleanly as jemalloc decay
  [jemalloc-decay-defaults] [mimalloc-purge-defaults] while beating it on Phase B
  per-core throughput on the musl target, which would tilt the
  allocator-into-musl choice toward mimalloc (the build-time alternative ADR-0006
  already keeps) [dragonfly-mimalloc-version] [mimalloc-design-segments-pages].
- the jemalloc tcache-default idle-RSS line item [jemalloc-tcache-defaults]
  [jemalloc-narenas-default] proving large enough on small machines that a
  tcache-narrowing or `narenas`-capping knob is needed before jemalloc is the
  musl-linked default.
- the best musl-linked build matching the glibc-pinned gnu reference within noise
  on every machine including 1-vCPU, which promotes musl from portable fallback
  to performance default and makes the static musl binary the single shipped
  artifact for that arch (CLI_BINARY.md, ADR-0017).
- a decay/purge default that drops steady-state RSS without a madvise-storm
  latency spike on the request path, which would change the recommended knob
  carried into #85.

## References

- Issue #124: musl allocator-contention SPIKE (this experiment). Parent #84
  (packaging, cross-build matrix, reproducible builds, SBOM, musl penalty
  research). Decision it feeds: #41 / ADR-0006 (default global allocator and
  accounting), where the result is recorded. Related: #42 (broader allocator
  bake-off, docs/experiments/allocator-bakeoff.md), #85 (decay/purge config
  knob). Vision EPIC #1.
- ADR-0006 (default global allocator and memory-accounting strategy).
- ADR-0017 (per-tenet acceptance gates: the Simple gate's static-musl,
  kernel-only-at-runtime requirement).
- docs/design/CLI_BINARY.md (single static musl binary per arch, kernel-only at
  runtime).
- docs/experiments/allocator-bakeoff.md (sibling experiment-design record; this
  doc isolates the musl-specific axis it lists under "what would change the
  decision").
- docs/research/memory-allocators.md, docs/research/dragonfly.md.
- Claims (resolved via docs/prior-art/claims.yaml): [rust-musl-crt-static-default],
  [tikv-jemallocator-version], [mimalloc-rust-version], [snmalloc-rs-version],
  [mimalloc-benchmarks], [jemalloc-tcache-defaults], [jemalloc-narenas-default],
  [jemalloc-decay-defaults], [mimalloc-purge-defaults], [redis-fragmentation-ratio],
  [redis-maxmemory-accounting], [redis-jemalloc-frag-hint],
  [redis-active-defrag-jemalloc], [dragonfly-single-core-parity],
  [dragonfly-25x-thread-asymmetry], [dragonfly-mimalloc-version],
  [mimalloc-design-segments-pages], [cargo-zigbuild-version-features].
