# Design: Shared-nothing core runtime and the async/io stack

Issue: #25. Decisions: ADR-0002 (shared-nothing thread-per-core), ADR-0003
(determinism / Env seam). Related: #27 (runtime abstraction), #28 (io_uring fast
path), #34 (storage API), #29 (cross-shard coordinator).

## Goal and scope

The headline bet is that the fastest path to max throughput per core is to
eliminate hot-path synchronization entirely. This fixes the core runtime as
shared-nothing thread-per-core over an async/io stack and makes it the foundation
every other design is measured against. Scope: the task/runtime topology, the
connection-to-shard routing path, the io_uring fast path with a portable
fallback, and the Env seam for determinism. Out of scope: the concrete store
(#35) and the io_uring buffer-registration details (#28).

## Design

### Topology

- One runtime per physical core, pinned, each owning an exclusive set of shards
  (ADR-0002). A shard is never touched by another core, so the per-shard store
  is unsynchronized (ADR-0005) and reclamation is trivial (ADR-0004). Locks are
  unnecessary by construction [glommio-locks-never-necessary]
  [seastar-shared-nothing].
- Work is expressed as async tasks on the per-core executor (stackless Rust
  async, not stackful fibers, for a smaller footprint than the helio model
  [dragonfly-coordinator-fiber]). A connection lives its whole life on one core.

### Connection-to-shard routing

- Accept distributes connections across cores. A command's key selects its shard
  by `k = HASH(KEY) % N` (the glossary shard rule). A single-key command whose
  key maps to the connection's own core runs inline with no hop; a command whose
  key belongs to another core, or a multi-key command spanning cores, is a
  message to the owning core through the cross-shard coordinator (#29). There is
  no shared map and no work stealing.

### io_uring fast path with portable fallback

- On Linux 5.11+ the network and disk paths use io_uring (registered buffers,
  multishot accept/recv where available [io-uring-multishot-recv-kernel]),
  matching the helio approach [dragonfly-iouring-helio]; the low-level binding is
  the io-uring crate [io-uring-crate-version] (key opcodes need kernel 5.6+
  [io-uring-read-opcode-kernel]). On older kernels and on macOS dev machines the
  same runtime interface is served by an epoll/kqueue fallback
  [monoio-min-kernel-fallback], so the single binary runs everywhere; the fast
  path is an optimization behind a stable interface (#28).

### Runtime choice and abstraction

- The thread-per-core runtime is monoio/glommio-class on io_uring
  [monoio-vs-tokio-scaling] [glommio-version-msrv], not tokio work-stealing,
  which is rejected as the primary model because it forces every shared structure
  to be `Send + Sync` and re-introduces atomics and cross-core cache-line
  bouncing on the hot path [tokio-workstealing-readiness-model]
  [tokio-version-msrv]. The concrete runtime sits behind a swappable abstraction
  (#27) so the bake-off (#26) can pick monoio vs glommio vs tokio+epoll on
  measured GET/SET without changing the engine.

### Determinism (Env seam)

- All time, network, disk, and RNG access goes through the controllable `Env`
  seam (ADR-0003), so a seeded replay is byte-identical
  [dst-fdb-tigerbeetle-single-seed]. The runtime exposes a simulated scheduler
  and clock through the same seam for DST (#95).

## Open questions

- The default runtime pending the #26 bake-off (this design fixes the shape and
  the swappable seam, not the final crate).
- Connection-acceptance load balancing across cores (round-robin vs least-loaded)
  and how it interacts with hot-shard skew (#32/#170).

## Acceptance and test hooks

- A single-key GET/SET on the connection's own core takes no lock, no atomic, and
  no cross-core hop (asserted structurally by the hot-path lint, invariant 1).
- The same binary runs on a pre-5.11 kernel and on macOS via the fallback.
- A seeded DST run replays byte-identically through the Env seam (#160).

## References

- ADR-0002, ADR-0003, ADR-0004, ADR-0005; issues #27, #28, #26, #29, #34, #95,
  #160, #32.
- Claims: [glommio-locks-never-necessary], [seastar-shared-nothing],
  [dragonfly-coordinator-fiber], [dragonfly-iouring-helio],
  [io-uring-multishot-recv-kernel], [io-uring-crate-version],
  [io-uring-read-opcode-kernel], [monoio-min-kernel-fallback],
  [monoio-vs-tokio-scaling], [glommio-version-msrv],
  [tokio-workstealing-readiness-model], [tokio-version-msrv],
  [dst-fdb-tigerbeetle-single-seed].
