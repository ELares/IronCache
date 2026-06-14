# Design: HybridLog cold engine (tri-region in-place log, swappable index)

Issue: #64. Decisions: ADR-0023 (cold engine: reject RocksDB/LSM, adopt a hybrid
log; engine internals owned here). Related: #58 (persistence umbrella,
PERSISTENCE.md), #34 (narrow-waist storage API, STORAGE_API.md), #111 (object
layout, OBJECT_LAYOUT.md), #66 (tiered RAM->SSD placement, TIERED_STORE.md), #60
(forkless snapshot epoch cut, SNAPSHOT.md), #67 (io_uring write path), #33
(reclamation/compression interaction), #38 (Dash index experiment).

## Goal and scope

ADR-0023 settled the cold-tier engine family: reject an embedded RocksDB/LSM and
adopt a hybrid log, leaving the engine internals to this spec. This is that spec.
It defines the FASTER-style HybridLog in Rust: a single logical address space cut
into a mutable region updated in place, a read-only region appended at the tail,
and a stable region that spills to disk, all sitting behind the four narrow-waist
primitives (#34) so every RMW command shares one hot path. It specifies record
ownership, the slab/arena container discipline that replaces a managed heap,
epoch-deferred reclamation in place of a collector, the default geometry, and it
keeps the hash-index geometry a swappable choice per ADR-0023 rather than freezing
it. It does not re-decide the engine family (ADR-0023 owns that), the
single-vs-F2 log shape (a harness-blocked bake-off feeding this issue owns that,
ADR-0023 Consequences), the disk placement policy (#66), or the snapshot
mechanism (#60); it composes them. Conflicts resolve Compatible over Efficient
over Simple.

## Design

### Tri-region address space

- One logical, monotonically growing address space split into three regions by
  two moving offsets, mutable / read-only / stable, the FASTER HybridLog
  structure [faster-hybridlog-three-regions]. The mutable region is the interval
  [ReadOnlyAddress, Tail): records here are updated in place. The read-only region
  is [HeadAddress, ReadOnlyAddress): records here are immutable and an update
  copies the record forward to the tail (RCU). The stable region is below
  HeadAddress and lives on disk [faster-hybridlog-three-regions]; mutable plus
  read-only are in RAM, stable is on flash, and the cold-log placement across that
  boundary is #66.
- The two boundary offsets shift forward only at epoch-safe points (below), never
  under a record read, so a reader never observes a half-moved boundary.

### In-place hot-set update versus RCU

- A write to a record whose address is in the mutable region and whose size is
  unchanged is an in-place metadata-and-value write on the owning core: no
  allocation, no append, no relink. In-place mutation of the hot set is the
  mechanism FASTER credits for beating pure in-memory structures and is why the
  family is fast enough to be the steady-state engine, with a measured
  single-machine peak of up to 160 million ops per second on YCSB
  [faster-peak-throughput].
- A write degrades to read-copy-update (append the new record at the tail and
  re-point the index) when the target is in the read-only region, or when the
  write changes the record size so the in-place slot no longer fits. Variable
  length values therefore fall back to RCU exactly when they grow or shrink past
  their slot; the precise degrade rule and its interaction with reclamation is
  coordinated with #33, and whether a compressed write (which can change encoded
  size on every update) forces RCU on every write is an open question below.

### Slab/arena records, no managed heap

- Records are laid out as explicit Rust in slab/arena pages bound to the unified
  operation log, the Garnet two-stores discipline adapted: keep the unified-log
  binding, reject the .NET GC-heap object representation and redesign containers
  as Rust slabs/arenas [garnet-two-stores]. There is no boxed-per-element
  container and no collector. Each record carries documented ownership: the log
  page owns the record bytes, the index holds a logical address, never a raw
  pointer, so a boundary shift or a forward copy never dangles. The per-key object
  bits this engine stores follow OBJECT_LAYOUT (#111).
- Reclamation is revivification plus Lookup-style liveness compaction, never a
  Scan compaction whose transient parallel index scales with the keyspace
  [garnet-compaction-default-none]. Append-only with no leveled compaction is the
  property ADR-0023 relies on for SSD endurance and the absence of
  compaction-induced tail latency.

### Epoch-protected boundaries and frees

- All deferred work, boundary shifts, forward-copy retirement, and page frees, run
  through FASTER epoch protection: a global epoch counter, per-thread epoch
  registration, and a drain list of actions that fire only once every thread has
  passed the epoch [faster-epoch-protection]. This is the backbone that replaces
  GC and the latch on address updates. A page is freed, and a stale record version
  reclaimed, only at an epoch-safe point, which is also the interlock the snapshot
  serializer (#60) and the reclamation owner (#33) hook so a version still owed to
  an in-flight serializer is never freed.

### Narrow-waist hot path

- The engine exposes only the four narrow-waist primitives, Read / Upsert /
  Delete / atomic Read-Modify-Write [garnet-narrow-waist-api] (#34). Every RESP
  RMW command (INCR, APPEND, LPUSH, expiry bump) is expressed as an RMW callback
  over this surface, so the engine has exactly one hot path and the command layer
  injects behavior rather than reaching into the log. No RESP command carries
  bespoke engine code.

### Swappable index geometry

- The hash index that maps a key to a logical log address is kept a swappable
  choice per ADR-0023, not frozen here. The FASTER candidate geometry is the
  cache-line-aligned 64-byte bucket holding seven 8-byte hash entries plus one
  8-byte overflow-bucket pointer (eight slots total, seven usable), each entry a
  15-bit tag, a tentative bit, and a 48-bit address, with bucket-granularity
  latch-free insert via the tentative bit [faster-hash-bucket-layout]. The
  alternative is the per-slot SwissTable the M1 index already ships (HASHTABLE
  #35). The choice between them is the #38 Dash bake-off and the open decision
  below; this spec keeps the engine structured so the index is a trait behind the
  log, not a baked-in layout, so the bake-off and the F2-vs-FASTER shape decision
  can both resolve without re-cutting the engine.

### Default geometry

- Defaults track Garnet for predictability, all tunable: total log memory 16 GiB
  [garnet-default-log-memory-size], mutable region 90 percent of the log
  [garnet-default-mutable-percent], hash index start size 128 MiB
  [garnet-index-default-size]. Auto-deriving these from host RAM is friendlier but
  surprises operators, so Garnet-parity fixed defaults win and host-derived sizing
  is an open question. Compaction defaults follow the Garnet posture: off by
  default, Lookup recommended, Scan rejected [garnet-compaction-default-none],
  matching the Tier-0 ephemeral default the persistence umbrella sets (#58).

## Open questions

- Index geometry: keep the FASTER 64-byte / seven-entry bucket
  [faster-hash-bucket-layout] or the per-slot SwissTable, resolved by the #38
  bake-off; the engine keeps it swappable until then.
- Variable-length values: the exact rule by which in-place mutation degrades to
  RCU when a write changes record size, coordinated with #33.
- Whether compression invalidates the stable-record-size assumption and forces RCU
  on every compressed write (#33, compression-mutation).
- Region-boundary alignment with the forkless snapshot epoch cut (#60) so a
  snapshot serializes mutable plus read-only plus stable metadata consistently.
- Default log sizing: Garnet parity [garnet-default-log-memory-size] versus
  host-derived.
- Single-log (FASTER) versus two-component (F2) shape, left to a harness-blocked
  bake-off feeding this issue per ADR-0023; the provisional hypothesis is F2 wins
  under cache skew [f2-hot-cold-log-two-tier] [f2-throughput-vs-rocksdb].

## Acceptance and test hooks

- The engine exposes only the narrow-waist primitives
  [garnet-narrow-waist-api]; RESP commands build on them with no bespoke engine
  code, asserted structurally.
- Mutable-region writes to a stable-size record are allocation-free and update in
  place; a write that changes size or targets the read-only region degrades to
  RCU, both verified.
- Record layout is explicit Rust with documented ownership: no GC, no boxed
  per-element container; the index holds logical addresses, not raw pointers.
- Region boundaries shift and reclaim only at epoch-safe points
  [faster-epoch-protection]; a fault test shows no free of a version owed to an
  in-flight serializer (#60, #33).
- Defaults are 16 GiB log [garnet-default-log-memory-size], 90 percent mutable
  [garnet-default-mutable-percent], 128 MiB index [garnet-index-default-size], all
  tunable; reclamation uses revivification plus Lookup compaction with no Scan
  path [garnet-compaction-default-none].
- The index is a swappable trait: the FASTER bucket and the SwissTable both
  satisfy it, so the #38 bake-off can switch geometry without re-cutting the
  engine.
- Snapshot region boundaries are documented against the forkless snapshot design
  (#60).

## References

- ADR-0023; issues #64, #58, #34, #111, #66, #60, #67, #33, #38, #1.
- Specs: PERSISTENCE.md, STORAGE_API.md, OBJECT_LAYOUT.md, HASHTABLE.md,
  TIERED_STORE.md, SNAPSHOT.md.
- Research: docs/research/garnet.md, docs/research/persistence-storage-engines.md.
- Claims: [faster-hybridlog-three-regions], [faster-peak-throughput],
  [faster-epoch-protection], [faster-hash-bucket-layout], [garnet-two-stores],
  [garnet-narrow-waist-api], [garnet-compaction-default-none],
  [garnet-default-log-memory-size], [garnet-default-mutable-percent],
  [garnet-index-default-size], [f2-hot-cold-log-two-tier],
  [f2-throughput-vs-rocksdb].
