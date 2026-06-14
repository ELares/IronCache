# Design: Persistence umbrella (durability tiers, forkless snapshot, on-disk engine)

Issue: #58. Decisions: ADR-0014 (ephemeral default, opt-in snapshot/warm-restart),
ADR-0022 (forkless serialization, no huge pages, no overcommit tuning), ADR-0023
(cold engine: reject RocksDB, adopt a hybrid log). Related: #60 (snapshot), #62
(warm restart), #63 (append-log), #64 (hybrid-log engine), #66 (placement), #65
(cold-engine decision), #67, #34 (storage hooks), #28 (io_uring).

## Goal and scope

IronCache is ephemeral by default (ADR-0014). This spec is the umbrella for the
opt-in durability menu layered on that default: three durability tiers with
honest loss windows, a live loss-visibility metric contract, a fail-closed posture
on persistence error, one shared io_uring write path, and the on-disk layout that
hosts both the snapshot and the cold engine decided in ADR-0023. It does not
re-decide the durability stance (ADR-0014 owns that) or the cold-engine choice
(ADR-0023 owns that); it composes them. Conflicts resolve Efficient over Scalable.

## Design

### Three durability tiers with honest loss windows

- **Tier 0, ephemeral (default).** No persistence on the hot path; loss window on
  crash is the whole dataset, which is the cache contract (ADR-0014). Like Garnet,
  the storage tier, the append-log, and compaction all ship off
  [garnet-storage-tier-default-off] [garnet-aof-default-off]
  [garnet-compaction-default-none], and checkpointing is opt-in rather than
  always-on [garnet-checkpoint-modes].
- **Tier 1, interval / os-buffered.** Writes are buffered and flushed on an
  interval; the loss window is the configured interval plus in-flight buffer. We do
  not advertise a single marketing number: Redis `appendfsync everysec`
  [redis-appendfsync-default] claims a 1 s window but its real worst case is about
  2 s when a background fsync is already in flight
  [redis-everysec-real-worst-case-2s]. We publish the live metric below instead.
- **Tier 2, strict group-commit.** A write is acknowledged only after its record
  is durable; the loss window is zero for acknowledged writes, paid as added ACK
  latency batched by a group-commit window (size is an open question). Whether the
  strict tier blocks the client ACK or pipelines behind `durable_offset` is open.

### durable_offset and fsync-lag metric contract

- The engine exports a monotonic `durable_offset` (the highest log offset known
  fsynced) and a `fsync_lag` gauge (appended-but-not-yet-durable bytes/age), both
  live, both surfaced through the observability registry (#86, fsync-lag is named
  there). This is the honest substitute for a static loss claim, the same reason we
  reject the everysec marketing window [redis-everysec-real-worst-case-2s]; a
  client or operator reads the actual window rather than trusting a label.

### Fail closed on persistence error

- Any persistence write error (fsync failure, ENOSPC, IO error) fails closed:
  IronCache rejects further writes that require that durability tier rather than
  silently degrading. Redis ships RDB checksums and a blocked-write-on-save-error
  default [redis-rdb-compression-checksum-defaults], but a blocked save is not the
  same as fail-closed on every persistence error path; we make the rejection
  explicit so no acknowledged write is silently non-durable. The reject reuses the
  OOM-write contract surface (ADMISSION #137) so clients see a defined error, not
  a stall.

### Shared io_uring write path

- Snapshot, append-log, warm-restart, and cold-tier writes all go through one
  io_uring submission path (#28, RUNTIME) rather than per-feature blocking
  `pwrite`/`fsync`. One back-pressured streaming writer means one place to reason
  about ordering, `durable_offset` advancement, and fail-closed, and it is what
  lets the forkless serializer stream without an RSS spike (below).

### Forkless snapshot and warm restart

- The only snapshot path is the forkless versioned serializer: an epoch cut plus a
  pre-image write-hook gives a consistent point-in-time view with no `fork()` and
  constant extra memory regardless of dataset size
  [dragonfly-forkless-snapshot-mechanism] [dragonfly-snapshot-constant-memory], so
  there is no COW RSS doubling [redis-cow-rss-doubling] and no visible memory spike
  during the snapshot [dragonfly-bgsave-memory-efficiency]. The page-size/overcommit
  consequences of forklessness are ADR-0022. Warm restart writes an mmap state file
  on graceful shutdown and rebuilds in seconds on restart
  [memcached-warm-restart-mmap-sigusr1] (#62); whether it shares one on-disk format
  with the snapshot is open.

### On-disk hybrid-log plus segment/manifest layout

- On disk the durable state is an append-only hybrid log written as fixed-size
  segments tracked by a manifest (the file of record listing live segments and the
  durable cut). The cold tier that backs warm-but-evicted keys on flash is the
  hybrid-log engine decided in ADR-0023: a hot/cold split (F2-style) of a hot log
  in memory and a cold log on disk with a read cache [f2-hot-cold-log-two-tier],
  append-only so there is no compaction tail latency and the design is fast enough
  to be the steady-state engine [faster-peak-throughput]. This spec only states
  that the snapshot, the append-log, and the cold tier share this segment/manifest
  layout and the io_uring writer; the engine internals and the F2-vs-FASTER
  benchmark are ADR-0023 and #64.

## Open questions

- Group-commit batching window for the strict tier (latency vs syscall amortization).
- Whether warm-restart mmap and forkless snapshot share one on-disk format or two.
- Segment size and manifest compaction cadence for the append-log (#63).
- Does the strict tier block the client ACK or pipeline behind `durable_offset`?

## Acceptance and test hooks

- Forkless snapshot is the only snapshot path; no fork+COW code exists, asserted
  structurally.
- Storage tier, append-log, and compaction are verified off by default
  [garnet-storage-tier-default-off] [garnet-aof-default-off]
  [garnet-compaction-default-none], matching the Tier-0 ephemeral posture.
- Three named tiers each have a documented loss window, and an injected fsync
  failure makes the next durability-requiring write fail closed (no silent loss).
- `durable_offset` and `fsync_lag` are exported live and advance only past a real
  fsync; a fault-injection test shows lag rising and the offset frozen.
- Snapshot, append-log, and cold-tier writes all flow through the shared io_uring
  path (#28); a no-blocking-pwrite lint guards it.

## References

- ADR-0014, ADR-0022, ADR-0023; issues #58, #60, #62, #63, #64, #66, #65, #67,
  #34, #28, #86, #137.
- Claims: [dragonfly-forkless-snapshot-mechanism], [dragonfly-snapshot-constant-memory],
  [dragonfly-bgsave-memory-efficiency], [redis-cow-rss-doubling],
  [garnet-storage-tier-default-off], [garnet-aof-default-off],
  [garnet-compaction-default-none], [garnet-checkpoint-modes],
  [redis-appendfsync-default], [redis-everysec-real-worst-case-2s],
  [redis-rdb-compression-checksum-defaults], [memcached-warm-restart-mmap-sigusr1],
  [f2-hot-cold-log-two-tier], [faster-peak-throughput].
