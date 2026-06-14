# Design: Segment + atomic-manifest durable log with corruption recovery

Issue: #63. Decisions: ADR-0014 (ephemeral default, opt-in durability), ADR-0023
(on-disk state is the append-only hybrid log this log persists). Related: #58
(persistence umbrella and durability tiers, PERSISTENCE.md), #64 (HybridLog
engine whose segments this log manages, HYBRIDLOG_ENGINE.md), #67 (io_uring write
path, fsync coordination), #61 (rewrite-trigger constants and parallel-restart
bake-off), #100 (crash-injection manifest tests), #60 (snapshot base record
format).

## Goal and scope

A single monolithic append-only file conflates three concerns: the compacted
snapshot, the live tail, and the index of what is current; rewriting it in place
is not crash safe and loading it is single threaded. This spec replaces it with
an immutable binary base segment, append-only incremental record logs, and an
atomically rewritten manifest, modeled on Redis 7.0 multi-part AOF
[redis-multipart-aof-since-7.0]. In scope: the file layout, manifest atomicity,
CRC granularity, the corruption-recovery rules, the rewrite trigger, and the
fsync coordination delegated to the durability tier (#58) and carried over the
shared io_uring writer (#67). Out of scope: the in-memory engine and record
layout (#64), the wire protocol, and the durability-tier definitions themselves
(#58 owns the loss windows). Conflicts resolve Compatible over Efficient over
Simple.

## Design

### Three-part layout

- Persistence is a directory of one immutable base segment, one or more
  append-only incremental segments, and a manifest that names the active set. This
  is the Redis 7.0 multi-part AOF split borrowed wholesale: a base file, incr
  files, and a manifest whose lines name file, sequence, and type
  [redis-multipart-aof-since-7.0]. The three-part split is the cleanest known
  crash-safe-compaction answer and it enables parallel restart load; the cost is
  more files than a single AOF.
- The base segment is a binary snapshot, not a replayed command log. Redis writes
  its base file as an RDB-format preamble by default rather than as replayed
  commands [redis-aof-use-rdb-preamble-default]; we adapt that to a binary
  snapshot so load is a scan rather than a replay, which loads faster than command
  replay. The base record format is RDB-compatible where the value type permits,
  shared with the snapshot base (#60). The on-disk live state these segments carry
  is the append-only hybrid log decided in ADR-0023 (#64).

### Atomic manifest swap

- The manifest is the file of record listing live segments and the durable cut.
  It is never edited in place. A swap writes a new manifest to a temp file, fsyncs
  it, then atomically renames it over the old manifest; rename is the only
  torn-write-free swap, at the cost of one extra fsync. Manifest generations are
  numbered and never overwritten in place, so a prior generation survives for
  rollback.

### CRC granularity

- CRC is per unit, not one trailing checksum, so damage is localized. Incremental
  segments carry a per-record CRC; the base segment carries a per-block CRC. This
  adapts the Redis default of a single trailing CRC64 over an RDB payload
  [redis-rdb-compression-checksum-defaults] to per-record and per-block placement,
  which is what lets a torn incremental tail be distinguished from a corrupt base
  block. The CRC algorithm (CRC32C hardware-accelerated versus CRC64 for Redis
  parity) and whether the base segment is compressed underneath the block CRC are
  open questions below.

### Corruption-recovery rules (testable)

- A torn incremental tail, the last record failing CRC, is truncated at the last
  valid record; the manifest is unchanged and load succeeds with bounded, asserted
  loss. This is the only lossy-but-successful path.
- A failed CRC inside a base block, or a base referenced by the manifest that is
  absent or short, is a hard fault: refuse to load that base, fall back to the
  prior manifest generation if one exists, else fail closed. This composes the
  persistence umbrella's fail-closed-on-persistence-error posture (#58): a corrupt
  base is never partially loaded.
- Because manifest generations are numbered and never overwritten, a crash injected
  at any step of a swap leaves either the old or the new manifest fully intact;
  no load ever observes a partial manifest (#100).

### Rewrite trigger

- Compaction (rewrite) fires on a growth-ratio plus absolute-floor trigger, the
  Redis shape borrowed: Redis triggers a rewrite at 100 percent growth over the
  size at the last rewrite, gated by a 64 MB floor
  [redis-auto-aof-rewrite-defaults]. We adopt that shape and retune the constants
  under #61. A rewrite emits a new base plus a fresh empty incremental, fsyncs
  both, then atomically swaps the manifest. Old segments are unlinked only after
  the swap durably commits, so compaction never blocks writes and never strands
  the dataset on a half-published rewrite; segment reclamation is tied to the
  engine free-space strategy (#64). The rewrite firing is exposed as a metric
  through the observability registry (#86).

### Fsync coordination

- Fsync policy on incrementals is delegated to the durability tier (#58): Tier 1
  defaults to interval fsync, mirroring the Redis everysec default
  [redis-appendfsync-default] but published as the live durable_offset / fsync_lag
  metric rather than a marketing window (#58); Tier 2 group-commits per the strict
  tier. Manifest swaps always fsync regardless of tier, because the manifest is
  the atomicity anchor. All segment and manifest writes flow through the one shared
  io_uring writer (#67), so there is one place that advances durable_offset and one
  place to reason about ordering and fail-closed.

## Open questions

- Per-shard base segments versus one base, and how shard count interacts with
  parallel restart load.
- CRC algorithm: CRC32C (hardware accelerated) versus CRC64 for Redis parity
  [redis-rdb-compression-checksum-defaults].
- Whether the base segment is compressed, and how that interacts with block-level
  CRC.
- Manifest generation retention: keep N prior generations for rollback or just one.
- The retuned rewrite-trigger constants (growth ratio and floor) under #61.

## Acceptance and test hooks

- Manifest swap is atomic across a crash injected at every step; no load ever
  observes a partial manifest (#100).
- A torn incremental tail truncates to the last valid record with bounded,
  asserted loss; the manifest is unchanged.
- A corrupt or missing base referenced by the manifest fails closed or rolls back
  to the prior generation, and never loads partial data.
- Compaction never blocks writes and never unlinks a segment before the manifest
  swap durably commits.
- The rewrite fires per the growth-ratio plus floor trigger
  [redis-auto-aof-rewrite-defaults] and is observable via a metric.
- Per-record CRC on incrementals and per-block CRC on the base localize a torn
  tail versus a corrupt base, verified by targeted byte corruption.
- All segment and manifest writes flow through the shared io_uring writer (#67),
  and manifest swaps always fsync regardless of tier.

## References

- ADR-0014, ADR-0023; issues #63, #58, #64, #67, #61, #100, #60, #86, #1.
- Specs: PERSISTENCE.md, HYBRIDLOG_ENGINE.md, SNAPSHOT.md.
- Research: docs/research/redis-persistence.md,
  docs/research/persistence-storage-engines.md.
- Claims: [redis-multipart-aof-since-7.0], [redis-aof-use-rdb-preamble-default],
  [redis-auto-aof-rewrite-defaults], [redis-rdb-compression-checksum-defaults],
  [redis-appendfsync-default].
