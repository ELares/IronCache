# Design: Forkless versioned point-in-time snapshot and diskless full-sync

Issue: #60. Decisions: ADR-0022 (forkless serialization, no huge pages, no
overcommit tuning), ADR-0014 (ephemeral default, the snapshot is opt-in).
Related: #58 (persistence umbrella, PERSISTENCE.md), #11 (per-shard index and the
metadata word the version counter shares), #64 (engine epoch cut and region
boundaries, HYBRIDLOG_ENGINE.md), #67 (io_uring streaming writer), #33 (epoch GC
reclamation interlock), #77 (op-log handoff for replication), #63 (RDB-compatible
base record format, DURABLE_LOG.md), #100 (crash injection).

## Goal and scope

A point-in-time snapshot must not fork. Redis BGSAVE forks a copy-on-write child
whose RSS can roughly double under write load [redis-cow-rss-doubling]
[redis-fork-cow-doubles-memory], with a page-table-copy stall of about 9 to 13 ms
per GB on physical hosts and far worse on virtualized hosts
[redis-fork-latency-per-gb], catastrophic under Transparent Huge Pages
[redis-thp-cow-blowup]; that is the single largest memory and tail-latency footgun
in the incumbent and is disqualifying for IronCache's bounded-memory goal. This
spec defines the forkless, versioned, in-process snapshot with constant memory
overhead, and the diskless full-sync built on it. In scope: the per-entry version
counter, the epoch cut, traversal serialization, the on-write pre-image push, the
per-shard serialization channel and its bounded back-pressure, the conservative
versus relaxed variants, RDB-compatible output, and the diskless receiver. Out of
scope: the segment+manifest on-disk log (#63), the durability tiers (#58), and
the tiered RAM-to-SSD store (#66). The page-size and overcommit consequences of
forklessness are ADR-0022. Conflicts resolve Efficient over Simple.

## Design

### Forkless versioned epoch cut

- The only snapshot path is the forkless versioned serializer, borrowed from
  Dragonfly: each shard owns its index and a monotonic epoch; snapshot start does
  cut = shard.epoch++, a traversal task walks the shard's segments in bucket order
  serializing every entry with version <= cut and setting version = cut+1 on emit
  so it is not re-sent, and an OnWrite hook pushes the pre-image before a
  concurrent mutation [dragonfly-forkless-snapshot-mechanism]
  [dragonfly-forkless-versioned-snapshot]. This gives snapshot isolation at index
  bucket granularity with no fork(). Because the shard is single-writer the
  OnWrite hook runs inline: before overwriting an entry with version <= cut it
  pushes the pre-image (conservative) or the post-image/diff (relaxed) into the
  serialization channel. Iteration is bucket-atomic so a record is never split
  across the cut; serialized blobs never partially cover an index bucket
  [dashtable-segment-geometry].
- Snapshot memory overhead is constant regardless of dataset size
  [dragonfly-snapshot-constant-memory], so there is no COW RSS doubling
  [redis-cow-rss-doubling] and no visible spike during the dump; on about 5 GB
  Dragonfly showed no visible bgsave spike while Redis peaked near 3X
  [dragonfly-bgsave-memory-efficiency]. The version counter's width, packing, and
  wrap policy at epoch overflow, and whether it shares a metadata word with the
  eviction rank and expiry from #11, are open questions below.

### Conservative versus relaxed variants

- Two variants, selected per consumer, matching Dragonfly's split: conservative
  pushes the previous value before a mutation and is for file backups; relaxed
  streams the new value or a diff and is for replication
  [dragonfly-conservative-vs-relaxed]. Conservative is the backup default;
  relaxed is the replication default because it avoids a separate changelog.
  Whether the relaxed incremental-diff path needs an extended (non-RDB) record
  type, and how that interacts with point-in-time correctness under heavy
  multi-key writes, is open.

### Bounded MPSC back-pressure

- Each shard has a single bounded MPSC channel from its writer to its serializer.
  A full channel back-pressures the OnWrite hook, which back-pressures the writer:
  that is the natural, hard memory cap. An unbounded buffer would reintroduce the
  spike forklessness exists to remove, so it is rejected. The channel depth and
  the back-pressure policy (block the writer versus shed the snapshot versus
  spill) are open; spill is the least preferred because it converts the memory
  bound into tail latency.

### Non-blocking iteration via epoch GC

- SCAN/KEYS and the snapshot traversal run concurrently with writes and never
  block the request path, reusing KeyDB's non-blocking-iteration goal
  [keydb-mvcc-nonblocking] but implementing it with the epoch-cut plus COW
  pre-image path and FASTER-style epoch protection [faster-epoch-protection]
  (#64), not fork-COW. Epoch GC retires old versions in process and bounded; the
  reclamation interlock with #33 is that epoch GC must not free a version still
  owed to an in-flight serializer.

### Diskless full-sync

- The replication source streams the relaxed snapshot directly over the socket,
  adapting Redis diskless source-side streaming, which is good
  [redis-repl-diskless-sync-default]. The receiver is redesigned: Redis's
  receiver defaults are dangerous, swapdb holds old and new datasets at once
  (doubling memory) and an IO error aborts [redis-repl-diskless-load-default-disabled],
  so IronCache loads into a separate receiver arena, switches the live pointer
  atomically, enforces a hard memory cap, and aborts gracefully on IO error
  without corrupting the live dataset (no swapdb doubling). The base snapshot
  output is RDB-loadable where the value type permits, validated against a stock
  Redis loader, against the RESP/Redis API baseline [dragonfly-protocol-surface];
  an extended type is used only for relaxed diffs (#63 owns the base record
  format). The hard receiver memory-cap value, resumability of an interrupted
  transfer, and the relaxed-stream handoff to the live op-log (snapshot offset to
  op-log offset, #77) are open. All snapshot and tiering writes flow through the
  shared io_uring writer (#67).

## Open questions

- Width, packing, and epoch-overflow wrap policy of the per-entry version counter,
  and whether it shares a metadata word with eviction rank and expiry (#11).
- Whether the relaxed incremental-diff path needs an extended (non-RDB) record
  type, and its correctness under heavy multi-key writes.
- Bounded channel depth and back-pressure policy: block the writer, shed the
  snapshot, or spill.
- Memory-reclamation interlock with #33: epoch GC must not free a version owed to
  an in-flight serializer.
- How the relaxed stream hands off to the live op-log for #77 (snapshot offset to
  op-log offset).
- Hard receiver memory-cap value and resumability of an interrupted transfer.

## Acceptance and test hooks

- Snapshot peak extra RSS is O(channel depth), independent of dataset size,
  verified under a fully write-heavy workload with no visible spike, matching
  [dragonfly-bgsave-memory-efficiency] and the constant-overhead claim
  [dragonfly-snapshot-constant-memory].
- Point-in-time consistency: every key reflects exactly its value as of the cut,
  with bucket-granularity isolation and no torn records.
- Conservative and relaxed variants are both implemented and selected per consumer
  (backup versus replication) [dragonfly-conservative-vs-relaxed].
- SCAN/iteration runs concurrently with writes with no blocking of the request
  path [keydb-mvcc-nonblocking].
- Diskless full-sync loads into a separate arena, switches atomically, enforces a
  hard memory cap, and aborts gracefully on IO error without corrupting the live
  dataset (no swapdb doubling) [redis-repl-diskless-load-default-disabled].
- The base snapshot output is RDB-loadable where the value type permits, validated
  against a stock Redis loader [dragonfly-protocol-surface].
- No fork() exists anywhere in the snapshot or full-sync path, asserted
  structurally (ADR-0022).

## References

- ADR-0022, ADR-0014; issues #60, #58, #11, #64, #67, #33, #77, #63, #100, #1.
- Specs: PERSISTENCE.md, HYBRIDLOG_ENGINE.md, DURABLE_LOG.md.
- Research: docs/research/dragonfly.md, docs/research/keydb.md,
  docs/research/redis-persistence.md.
- Claims: [redis-cow-rss-doubling], [redis-fork-cow-doubles-memory],
  [redis-fork-latency-per-gb], [redis-thp-cow-blowup],
  [dragonfly-forkless-snapshot-mechanism], [dragonfly-forkless-versioned-snapshot],
  [dragonfly-snapshot-constant-memory], [dragonfly-bgsave-memory-efficiency],
  [dragonfly-conservative-vs-relaxed], [dashtable-segment-geometry],
  [keydb-mvcc-nonblocking], [faster-epoch-protection],
  [redis-repl-diskless-sync-default], [redis-repl-diskless-load-default-disabled],
  [dragonfly-protocol-surface].
