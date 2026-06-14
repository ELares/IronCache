# Design: io_uring snapshot/tiering write path with SQPOLL off-default and fallback

Issue: #67. Decisions: ADR-0002 (shared-nothing thread-per-core), ADR-0003
(determinism / Env seam). Related: #28 (io_uring net datapath, IOURING_DATAPATH.md),
#27 (runtime abstraction seam, RUNTIME_ABSTRACTION.md), #58 (forkless snapshot),
#66 (tiered store), #34 (storage API), #86 (observability).

## Goal and scope

Snapshots (#58) and SSD tiering (#66) are write-heavy, batchable, and latency
tolerant relative to GET/SET, and both must write to disk without stealing cycles
from the cores that serve requests. This spec designs that async write path: a
`disk_io` trait abstracting submit/reap, an io_uring backend that reuses the #28
net fast-path substrate, a startup kernel feature probe that logs the chosen path,
SQPOLL off by default, fixed-buffer registration coordinated with #28 so there is
no double registration, a bounded I/O-core budget that never exceeds floor(cores/4),
and a tokio-uring then epoll/kqueue fallback for older kernels and macOS
development. Scope is the write path and its fallback only. The snapshot format is
#58, the tier layout and eviction coupling are #66, and the on-disk
segment/manifest layout is the persistence umbrella (PERSISTENCE.md, #58). This
spec composes those; it does not re-decide them. Conflicts resolve Efficient over
Scalable: the default posture protects the request cores.

## Design

### The disk_io trait: submit/reap, swappable backends

- A narrow `disk_io` trait abstracts submit and reap of durable writes so the
  io_uring backend and the portable fallback are swappable without touching the
  snapshot or tiering callers. The surface is the write-side analogue of the #27
  `Runtime` seam: it carries owned `IoBuf` buffers (never borrowed slices), because
  io_uring's completion model requires the buffer to outlive the kernel and that is
  the only model both backends can satisfy (RUNTIME_ABSTRACTION.md). The trait
  exposes append/write-at, an fsync/flush barrier, and a reap that drains
  completions and advances the durable cut; it does not expose ring internals, so
  the snapshot serializer and the tiering flusher are written once.
- Backend selection follows #27: exactly one backend is active per build, chosen by
  Cargo feature, so the io_uring datapath stays monomorphized with no `dyn` on the
  flush path and the portable build links no io_uring code. The trait is the single
  seam; the snapshot/tiering callers see one model.

### Reuse the #28 substrate, not a separate disk runtime

- The io_uring backend reuses the per-shard ring family and registration machinery
  of the network fast path (#28, IOURING_DATAPATH.md) rather than standing up a
  separate disk runtime. One ring family is simpler and shares buffer registration,
  and shared-nothing single-writer-per-shard [seastar-shared-nothing] is the
  prerequisite that lets each shard serialize its own data with no cross-core
  locking, so a write SQE and its CQE never leave the owning shard (ADR-0002).
  Dragonfly takes the same one-family posture: helio over io_uring (with an epoll
  fallback below its own io_uring floor of Linux 5.11+) [dragonfly-iouring-helio].
- Glommio is rejected as a separate substrate even though it isolates NVMe polling
  by running three io_uring rings per thread, a main ring, a latency ring, and a
  poll ring dedicated to NVMe [glommio-three-rings] at MSRV 1.70
  [glommio-version-msrv]. That isolation is real but it forks the runtime: a
  separate ring family would duplicate buffer registration and the back-pressure
  reasoning that #28 already owns. We keep one ring family and accept that disk and
  net submissions share it; the dedicated-I/O-core budget below is how we bound
  their interference instead of forking the runtime.

### SQPOLL off by default

- SQPOLL plus registered/fixed buffers removes the submit syscall from the hot path
  and reports roughly 2.3x tx/s with 20 to 40 percent p99 gains over epoll for
  batched I/O in one DBMS benchmark [io-uring-sqpoll-registered-buffers]. We borrow
  registered buffers unconditionally (below) but make SQPOLL opt-in behind an
  explicit per-deployment flag, default off, because the SQPOLL kernel poller burns
  a core at idle. The Efficient-per-core default refuses to spend a whole core on a
  poller that earns its keep only under sustained write pressure; the opt-in serves
  latency-critical single-tenant deployments where a dedicated poller core is
  acceptable. The default path uses explicit batched submission, matching the same
  SQPOLL-off default the net path takes for the identical idle-cost reason (#28).

### Fixed-buffer registration coordinated with #28, no double registration

- Fixed buffers are registered once and the registration is coordinated with the
  net path: the disk writer draws its registered write buffers from the same
  per-shard slab that #28 registers, rather than registering a second slab on the
  same ring [io-uring-sqpoll-registered-buffers]. Registration amortizes the
  kernel's buffer validation across many writes; doing it twice on one ring would
  double the memlock footprint and split ownership of the slab. The coordination is
  explicit: #28 owns slab allocation and registration, and the disk_io backend
  requests a write-buffer reservation from that owned pool, so there is exactly one
  registration per ring. Whether snapshot writes and tiering writes draw from one
  reservation or two within that single registered slab is open.

### Bounded I/O-core budget, default 1, never above floor(cores/4)

- The number of dedicated I/O cores is bounded: default 1, and it never exceeds
  floor(cores/4). The bound is a hard invariant, not a tuning hint, because the
  whole point of moving snapshot and tiering writes off the request cores is
  defeated if the I/O cores grow until they starve the cores serving GET/SET. The
  budget does not scale with shard count. Under low load the snapshot/tiering fiber
  may time-slice onto shard cores instead of holding a dedicated core idle; whether
  the default is a dedicated I/O core or low-load time-slicing is open.

### Startup feature probe and the fallback ladder

- At startup the io_uring backend feature-detects kernel support and logs the
  chosen path, the same per-ring runtime probe the net path uses rather than a
  compile-time cfg, so one Linux artifact spans kernel tiers. The basic Read/Write
  opcodes the write path needs require kernel 5.6+ [io-uring-read-opcode-kernel];
  ring-provided multishot recv, a net-path feature, needs 6.0
  [io-uring-multishot-recv-kernel] and is not on the write path. Where the io_uring
  write path is unavailable the backend falls back: tokio-uring is the next rung but
  itself requires a recent kernel, about 5.11+ with 5.4 failing
  [tokio-uring-min-kernel], so below that the portable epoll/kqueue backend serves
  the host. monoio takes the same shape, io_uring on Linux 5.6+ and a legacy epoll
  or kqueue fallback otherwise, and io_uring also needs memlock configured properly
  [monoio-min-kernel-fallback]. macOS development builds always run snapshot and
  tiering through the kqueue fallback [monoio-min-kernel-fallback]. The fallback is
  the #27 portable backend, so the disk_io trait is the stable interface and the
  io_uring write path is always an optimization behind it.

### Constant-memory snapshot preserved

- The write path preserves the forkless snapshot's constant memory overhead. The
  forkless versioned snapshot serializes each shard through a single-writer channel
  with an epoch cut and an on-write pre-image hook
  [dragonfly-forkless-versioned-snapshot], and its overhead stays constant
  regardless of dataset size by pushing entries to the serialization sink and
  letting that establish back-pressure rather than fork()
  [dragonfly-snapshot-constant-memory]. The disk_io path is that sink's drain: a
  back-pressured streaming writer, so a slow disk back-pressures the serializer
  rather than buffering an unbounded queue and reintroducing the memory spike the
  forkless design exists to avoid. This is the same shared back-pressured writer
  PERSISTENCE.md names.

## Open questions

- Dedicated I/O cores versus time-slicing the snapshot/tiering fiber onto shard
  cores under low load (the default within the floor(cores/4) bound).
- Whether tiering writes and snapshot writes hold one buffer reservation or two
  within the single #28-registered slab.
- Buffer-pool sizing and memlock limits when many shards each register fixed
  buffers [monoio-min-kernel-fallback].
- Fallback selection at compile time versus the runtime probe, against the
  single-static-binary promise (CLI_BINARY.md, resolved toward the probe by #27 and
  #28 but restated here for the write path).

## Acceptance and test hooks

- A `disk_io` trait abstracts submit/reap so the io_uring and fallback paths are
  swappable; the snapshot and tiering callers compile against the trait with no
  ring internals leaked.
- Startup feature-detects kernel support [io-uring-read-opcode-kernel] and logs the
  chosen path; verified on two kernel tiers (an io_uring tier and a fallback tier).
- SQPOLL is off by default and only enabled behind the explicit flag
  [io-uring-sqpoll-registered-buffers]; a test asserts no poller core is spawned at
  default settings.
- The I/O-core count is bounded, defaults to 1, and is verified never to exceed
  floor(cores/4) nor to reduce the request-serving core count.
- Fixed-buffer registration is coordinated with #28: a test asserts exactly one
  registration per ring and that the disk writer draws from the #28-owned slab (no
  double registration).
- macOS dev builds run snapshot and tiering through the epoll/kqueue fallback
  [monoio-min-kernel-fallback].
- A slow-disk fault injection shows the forkless serializer back-pressuring with
  bounded extra memory [dragonfly-snapshot-constant-memory], not an unbounded queue.

## References

- ADR-0002, ADR-0003; issues #67, #28, #27, #58, #66, #34, #86;
  docs/design/IOURING_DATAPATH.md, docs/design/RUNTIME_ABSTRACTION.md,
  docs/design/PERSISTENCE.md, docs/design/CLI_BINARY.md.
- Claims: [io-uring-sqpoll-registered-buffers], [io-uring-read-opcode-kernel],
  [io-uring-multishot-recv-kernel], [tokio-uring-min-kernel],
  [monoio-min-kernel-fallback], [glommio-three-rings], [glommio-version-msrv],
  [dragonfly-iouring-helio], [dragonfly-forkless-versioned-snapshot],
  [dragonfly-snapshot-constant-memory], [seastar-shared-nothing].
