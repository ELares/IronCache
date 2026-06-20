# Design: io_uring net fast path with registered buffers and multishot ops

Issue: #28. Decisions: ADR-0002 (shared-nothing thread-per-core), ADR-0003
(determinism / Env seam). Related: #25 (core runtime, RUNTIME.md), #27 (runtime
abstraction seam), #26 (runtime bake-off), #67 (persistence write path).

## Goal and scope

IronCache's hot path is network I/O: accept, read a request, write a reply,
repeated across millions of ops per second per node. This spec designs the Linux
io_uring fast path so the steady state pays zero per-request heap allocation and
one `io_uring_enter` amortized across a batch. Scope: the per-shard ring
topology, the registered fixed-buffer slab and buffer groups, multishot accept
and recv with a one-shot fallback chosen by a startup probe, and how the path
degrades on older kernels. The cross-platform seam (the epoll/kqueue fallback
backend) lives in #27; this spec is the Linux datapath behind that seam. The
low-level binding is the `io-uring` crate [io-uring-crate-version].

## Design

### Ring topology

- One io_uring per shard, pinned to the same core as the shard's reactor
  (ADR-0002). A per-shard ring avoids cross-core completion routing, cache-line
  bouncing, and any locking on submission or completion; locks are unnecessary
  by construction [glommio-locks-never-necessary] [seastar-shared-nothing]. This
  matches Dragonfly's helio over io_uring with an epoll fallback
  [dragonfly-iouring-helio]. No buffer or completion ever crosses a shard.

### Registered fixed-buffer slab and buffer groups

- At shard init the shard allocates one contiguous slab, registers it with the
  ring once, and splits it into a kernel buffer group indexed by buffer-group id.
  Registration removes per-request pin/unpin and malloc. On recv completion the
  kernel returns the buffer id it filled; the shard parses the request in place
  and returns the buffer to the group on reply completion. No buffer leaves the
  shard, so there is no synchronization on the buffer pool. The pool is the
  per-shard owned-buffer pool the runtime seam exposes (#27).

### Multishot ops with one-shot fallback, chosen by startup probe

- Accept uses multishot accept where the kernel supports it: one SQE posts a CQE
  per new connection [io-uring-multishot-accept-kernel] (kernel ~5.19+), cutting
  SQE churn on the listener. Connection reads use multishot recv with the
  ring-provided buffer group [io-uring-multishot-recv-kernel] (kernel 6.0+),
  which removes per-read submission and per-read buffer handoff.
- A startup feature probe selects the path per ring rather than a compile-time
  cfg, so one Linux artifact runs across kernel versions: where multishot or
  provided buffers are absent, the ring falls back to re-armed one-shot accept
  and one-shot `Read`/`Recv` over owned fixed buffers [io-uring-read-opcode-kernel]
  (kernel 5.6+). Below 5.6 the io_uring path is not used at all; the #27
  epoll/kqueue backend serves that host [monoio-min-kernel-fallback].

### Pipelining and the shared persistence write path

- Multishot recv batches naturally: one completion can carry several pipelined
  commands, which the parser drains before the buffer returns to the group, and a
  batch of replies coalesces into one submission (#25). Persistence (#67) reuses
  this substrate rather than forking it: the snapshot/tiering writer gets its own
  registered write buffers from the same per-shard slab and submits fixed-buffer
  writes on the same per-shard ring, so durability shares the fast path.

### Resolved open decisions

- Buffer-group sizing under memory pressure: the per-shard slab is a fixed budget
  set at startup and counted against the shard's maxmemory share, not grown on
  demand; when the group is drained the shard applies read back-pressure (defers
  re-arming recv) instead of allocating, so a burst cannot blow the memory bound.
- SQPOLL vs explicit enter: default to explicit `io_uring_enter` with batched
  submission, because SQPOLL burns a kernel poller core at idle; SQPOLL is an
  opt-in for dedicated-core deployments where that idle cost is acceptable.
- Completion-queue overflow when a shard stalls: rely on the kernel CQ overflow
  list (no completions are lost), drain it on the next enter, and pair it with
  the read back-pressure above so a stalled shard sheds new reads rather than
  overrunning its CQ; a sustained-overflow counter feeds observability.
- Minimum kernel for the fast path: the multishot tier where the probe finds it,
  one-shot io_uring on 5.6+ otherwise, and the #27 epoll/kqueue backend below
  that. The fast path is always an optimization behind the #27 stable interface.

## Open questions

- The exact per-shard slab budget as a fraction of the shard memory share, and
  how it interacts with eviction (#48) under sustained read pressure.
- Whether reply-side writes should also use registered fixed buffers in the
  one-shot fallback tier, or only in the multishot tier.

## Acceptance and test hooks

- The steady-state read path performs zero heap allocations per request.
- The startup probe selects multishot accept/recv [io-uring-multishot-accept-kernel]
  [io-uring-multishot-recv-kernel] when present and falls back to one-shot ops
  [io-uring-read-opcode-kernel] otherwise, verified on two kernel versions.
- One ring per shard, pinned to the shard core, with no cross-core completion
  routing and no buffer crossing a shard.
- The read and persistence (#67) write paths share the same registered slab.
- A benchmark shows reduced syscalls/op versus the epoll baseline (#26 host
  sensitivity, runtime-bakeoff.md).

## Implementation status (PROD-10 v1)

The OPTIONAL io_uring backend has landed as an ADDITIVE, DEFAULT-OFF Linux path behind the
`io_uring` Cargo feature (in `ironcache-runtime`, target-gated to Linux) plus a `runtime =
"tokio" | "io_uring"` config knob (TOML / `IRONCACHE_RUNTIME` / `--runtime`, default `tokio`):

- A new `Runtime` impl `ironcache_runtime::io_uring_rt::IoUringRuntime` over `tokio-uring`
  (one current-thread io_uring per shard thread), satisfying the SAME `Runtime` trait the tokio
  backend does. The owned-buffer `recv`/`send` map directly onto `tokio_uring`'s owned-buffer
  `read`/`write_all`; `recv` APPENDS via `buf.slice(start..)` so it matches the tokio backend's
  framing. The pure engine + the determinism Env seam + the DST are UNTOUCHED (io_uring is purely
  a production `Runtime` impl).
- A per-shard io_uring bootstrap `run_shards_uring` mirroring the tokio bootstrap's shared-nothing
  topology (one userspace acceptor round-robins accepted sockets to per-shard channels; each shard
  thread runs `tokio_uring::start` and adopts its connections onto its own ring).
- Boot selection (in the binary's `run_server_observed`): `runtime = io_uring` is honored ONLY on
  a Linux build with the feature AND with TLS off; in every other case the boot logs a one-line
  fallback and uses the tokio backend, so the default build + non-Linux + TLS are byte-unchanged
  and selecting io_uring can never fail to start a node.
- The default (no-feature) build never pulls `tokio-uring`/`io-uring`; the pure-Rust `io-uring`
  binding needs no liburing/C library, so the static-musl default artifact is unaffected.

Deferred to a Linux soak (NOT in v1, no throughput claim made): the registered fixed-buffer slab +
buffer groups, multishot accept/recv with the startup probe and one-shot fallback, the shared
persistence write path, and the epoll-vs-io_uring benchmark. Also deferred on the io_uring serve
loop: PUB/SUB push-while-idle, the blocking-command park, and the idle-timeout timer race (each
degrades gracefully on that path; the core request/reply datapath is fully served). A CI job
(`io-uring` in `.github/workflows/rust.yml`, ubuntu) builds + clippy-lints + tests the feature.

## References

- ADR-0002, ADR-0003; issues #25, #27, #26, #67; docs/design/RUNTIME.md,
  docs/design/RUNTIME_ABSTRACTION.md, docs/experiments/runtime-bakeoff.md,
  docs/research/concurrency-runtime-rust.md, docs/research/dragonfly.md.
- Claims: [io-uring-crate-version], [io-uring-multishot-accept-kernel],
  [io-uring-multishot-recv-kernel], [io-uring-read-opcode-kernel],
  [monoio-min-kernel-fallback], [dragonfly-iouring-helio],
  [glommio-locks-never-necessary], [seastar-shared-nothing].
