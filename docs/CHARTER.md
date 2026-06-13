# IronCache charter

This is the charter for IronCache. It fixes the thesis, ratifies the five
tenets and their strict conflict order, and points to the documents that govern
everything downstream. The vision EPIC (#1) is the canonical statement; this
charter is the governing reference. When a design issue and this charter
disagree, the charter wins until amended here.

## Thesis

The most efficient Redis-wire-compatible cache in the world, shipped as a single
static Rust binary that is both the server and the CLI. Efficient is the reason
to exist; Compatible is the constraint that makes it useful. We borrow the
shared-nothing thread-per-core shape that lets one process beat a single-threaded
keyspace, and we measure ourselves honestly per core, not by fat-box aggregate
QPS.

## The five tenets

Ranked, with conflicts resolved strictly in this order:

**Compatible > Efficient > Simple > Scalable > AI-Driven.**

- **Compatible.** RESP2 and RESP3 wire format and observable command semantics,
  measured against a pinned Valkey rather than relicensed Redis. Valkey 8.x is
  wire-identical to Redis 7.2 RESP2/RESP3 [valkey-resp-identical] and is BSD-3,
  so it is a license-safe conformance oracle. A command is either supported with
  Valkey-identical observable behavior or documented as unsupported. We never
  bend the wire or a command's behavior to win a benchmark.
- **Efficient.** Throughput-per-core, memory-at-fixed-hit-ratio, and p99/p999
  tail latency. Never fat-box aggregate QPS: the marketing "25x" framing pits 64
  threads against 2 [dragonfly-25x-thread-asymmetry], and at one core the
  shared-nothing leader is only at parity with Redis [dragonfly-single-core-parity].
  Per-core is the bar we hold ourselves to.
- **Simple.** Zero-config single-binary operation: one binary, safe cache
  defaults, install to first GET in under a minute, kernel-only at runtime. No
  JVM, no .NET, no sidecar.
- **Scalable.** Single-node-first: one process uses every core. Multi-node is a
  real distributed design derived from the architecture spec, never an emulated
  single-process stopgap.
- **AI-Driven.** Off-path advisor only. AI mines prior art and verifies claims
  (this repository is the proof); in the engine, learned policies are allowed
  only off the hot path and only when they never compromise the contract,
  determinism, or tail latency.

## Conflict resolution, worked

When two designs conflict, the higher tenet wins:

- **Compatible vs Efficient:** a faster encoding that changes an observable
  reply loses. Keep the reply.
- **Efficient vs Simple:** a per-core win that needs a tuning knob ships, with a
  safe default, over a slower zero-knob path.
- **Simple vs Scalable:** no second binary or external coordinator to get
  multi-node; the single binary carries it.
- **Scalable vs AI-Driven:** a deterministic resharding primitive beats a
  learned placement heuristic.

## Governing documents

Read these before opening a design issue:

- The vision EPIC (#1): the canonical thesis and tenets.
- The committed non-goals (issue #10; the ratified register lands at
  `docs/NON_GOALS.md`): what IronCache will not do, each traced to a tenet or a
  deferred milestone.
- [Prior-art survey](PRIOR_ART.md) and [`prior-art/claims.yaml`](prior-art/claims.yaml):
  the version-pinned single source of truth for every numeric claim (#6).
- [ADR index](adr/INDEX.md) and issue #4: where load-bearing decisions are
  ratified and superseded.
- [Glossary](GLOSSARY.md) and [Invariants](INVARIANTS.md) (#3): the shared
  vocabulary and the properties we refuse to violate.
