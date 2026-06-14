# Design: Runtime/IO abstraction keeping monoio/glommio/tokio swappable

Issue: #27. Decisions: ADR-0002 (shared-nothing thread-per-core), ADR-0003
(determinism / Env seam). Related: #25 (core runtime, RUNTIME.md), #26 (runtime
bake-off), #28 (io_uring fast path), #29 (cross-shard coordinator), #34 (storage
API).

## Goal and scope

The bake-off (#26) can only decide monoio vs glommio vs tokio+epoll cheaply if
the cache core is written once against a trait surface and swapping the concrete
runtime costs a Cargo feature, not a rewrite. This spec fixes that seam: a thin
`Runtime` trait the command core compiles against, with the concrete runtime
chosen at the binary's edge and zero indirection on the hot path. Scope: the
trait surface, the owned-buffer model, the monomorphization and feature-selection
strategy, and `spawn_on_shard`. This spec does NOT pick the default runtime; the
#26 bake-off owns that behind this seam (runtime-bakeoff.md). Out of scope: the
shard-per-core topology itself (#25) and the io_uring opcode/buffer design (#28).

## Design

### The Runtime trait surface

- A minimal trait: `accept`, `recv`, `send`, `timer`, and `spawn_on_shard`, with
  associated types fixing the listener, stream, and buffer concretely per
  backend. The surface is deliberately small because the thread-per-core
  backends produce `!Send` futures (a completion-model property of monoio and
  glommio); a fat ecosystem trait cannot be satisfied by a thread-per-core
  runtime, while a minimal set is implementable by all three including tokio.
  There is no global `spawn`: work pins to its owning core through
  `spawn_on_shard`, matching shared-nothing (ADR-0002) where locks are
  unnecessary by construction [glommio-locks-never-necessary]
  [seastar-shared-nothing].

### Owned-buffer model

- All I/O is owned-buffer (`IoBuf` / `IoBufMut`), never borrowed `&mut [u8]`.
  io_uring's completion model requires the buffer to outlive the kernel, so
  owned buffers are the only model the io_uring backends can satisfy; tokio's
  readiness model [tokio-workstealing-readiness-model] adapts by copying into an
  owned buffer. That copy is paid only on the portable fallback, never on the
  io_uring fast path (#28), so the seam costs the fast path nothing. The pool
  itself is per-shard and exposed through the trait (resolved below) so the
  io_uring datapath owns registration (#28).

### Monomorphization, not dyn dispatch

- The core is generic over the `Runtime` trait, so the request loop monomorphizes
  with no vtable: there is no `dyn Runtime` on the hot path. Trait objects are
  rejected because they add dynamic dispatch to every `recv`/`send`. The request
  loop carries no `cfg`; backend differences live entirely behind the associated
  types.

### Backend selection by Cargo feature

- Exactly one backend is active per build, chosen by `--features monoio | glommio
  | tokio`. Per-build selection keeps each binary monomorphized and lean; a
  runtime flag would link all three and reintroduce dispatch. This makes #26 a
  feature-flag swap with no core source change, and lets the win, which is
  conditional and workload-dependent [monoio-vs-tokio-scaling], be measured per
  workload rather than assumed.

### Resolved open decisions

- Single-binary story: ship per-backend builds, a Linux-io_uring build and a
  portable epoll/kqueue build, rather than one fat binary. Compile-time backend
  selection is incompatible with a single static artifact. The split is by
  runtime backend, not by architecture or kernel version: each build is still
  one binary per architecture (preserving CLI_BINARY.md's promise), and the
  io_uring build stays a single Linux artifact that spans kernel tiers via the
  #28 startup probe rather than forking per kernel. This scopes RUNTIME.md's
  "single binary runs everywhere" to per-backend, while keeping the fast path an
  optimization behind the stable interface.
- Timer abstraction: the trait's `timer` is backed by a per-shard timer wheel as
  the canonical timer (it is what TTL #51 and connection reaping already need),
  and the backend's native timer is used only to arm the wheel's next deadline.
  This keeps timer semantics identical across backends and keeps the Env seam
  (ADR-0003) the single source of time for deterministic replay.
- tokio+epoll is a first-class release target, not dev/portability only, because
  it is the only backend that runs on kernels without io_uring (the Compatible
  tenet) and on macOS/BSD via kqueue [monoio-min-kernel-fallback].
- Owned-buffer pool ownership: per-shard and exposed through the trait, not
  backend-internal, so the io_uring backend registers it once (#28) and the
  tokio adapter allocates an equivalent per-shard pool; the core sees one model.

## Open questions

- Whether `spawn_on_shard` needs a bounded mailbox depth surfaced to the core, or
  whether the cross-shard coordinator (#29) owns all back-pressure.
- The workspace MSRV is the max across active backends (glommio 1.70
  [glommio-version-msrv], tokio 1.71 [tokio-version-msrv]; monoio's MSRV is
  unpinned [monoio-version]); confirm monoio's floor and whether it raises the
  portable build's MSRV above tokio's.

## Acceptance and test hooks

- The `Runtime` trait defines `accept`, `recv`, `send`, `timer`, and
  `spawn_on_shard` with associated listener/stream/buffer types.
- The command core compiles against monoio, glommio, and tokio with no `cfg` in
  the request loop and no `dyn Runtime` in generated code (inspected).
- The tokio+epoll backend runs on a pre-5.6 Linux kernel and the kqueue backend
  on macOS [monoio-min-kernel-fallback].
- Exactly one backend is active per build, selected by a Cargo feature; #26 swaps
  backends by feature flag only.

## References

- ADR-0002, ADR-0003; issues #25, #26, #28, #29, #34; docs/design/RUNTIME.md,
  docs/design/CLI_BINARY.md, docs/experiments/runtime-bakeoff.md,
  docs/research/concurrency-runtime-rust.md.
- Claims: [monoio-version], [glommio-version-msrv], [tokio-version-msrv],
  [tokio-workstealing-readiness-model], [monoio-vs-tokio-scaling],
  [glommio-locks-never-necessary], [seastar-shared-nothing],
  [monoio-min-kernel-fallback], [io-uring-read-opcode-kernel].
