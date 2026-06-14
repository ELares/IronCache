# Design: Tiered RAM->SSD value store (extstore-inspired, durable)

Issue: #66. Decisions: ADR-0023 (cold-tier engine family: reject RocksDB/LSM,
adopt a hybrid log), ADR-0014 (ephemeral default, durability opt-in). Related:
#58 (persistence umbrella and recovery contract, PERSISTENCE.md), #64 (HybridLog
engine, HYBRIDLOG_ENGINE.md), #65 (cold-engine decision, ADR-0023), #67 (io_uring
write path), #48/#50 (eviction trait and policy mapping, EVICTION.md), #49
(W-TinyLFU admission, WTINYLFU.md), #86 (metrics registry).

## Goal and scope

IronCache must hold working sets larger than RAM without paying the LSM tax. This
spec defines the tiered RAM-to-SSD value store: hot keys and a compact pointer
stay in RAM, cold values spill to append-only flash pages, the tier survives
restart, and write amplification stays far below LSM levels. It adapts memcached's
extstore model and adds real durability so the SSD tier is durable, not merely a
capacity extension. In scope: the value-store layer, the RAM index pointer, the
eviction-driven spill trigger, the append-only flash layout, the write-
amplification budget, and the durability and recovery of the pointer table. Out of
scope: the WAL and recovery contract owned by #58, the engine internals owned by
#64, and the eviction algorithm itself owned by EVICTION.md (#48). The migration
trigger reuses the cache eviction signal per ADR-0023 rather than a parallel
recency clock. Conflicts resolve Compatible over Efficient over Simple.

## Design

### RAM index plus compact pointer

- The RAM index holds the key plus a compact pointer (we target roughly 12 bytes,
  to confirm) carrying [page, offset, version]; the value lives in flash log
  pages, the extstore model [extstore-defaults]. The smaller index buys more
  RAM:flash reach at the cost of one flash read per cold hit. Confirming the
  roughly 12-byte width versus a wider pointer for larger flash devices is an open
  question. Because keys stay in RAM, the keys-in-RAM cost caps the practical
  RAM:flash ratio [keydb-flash-key-cache]; we publish the bytes-per-key figure and
  the resulting ceiling so operators can size DRAM against flash. The per-key
  object bits follow OBJECT_LAYOUT (#111) and the in-RAM index is the engine index
  (#64).

### Eviction-driven spill

- Migration is driven by the cache eviction signal at a documented spill
  threshold, not a parallel background recency sweeper [extstore-defaults]. The
  same eviction order the cache layer already computes (the EvictionPolicy trait,
  #48: S3-FIFO default, W-TinyLFU and others selectable per EVICTION.md) is the
  cold-tier migration trigger, so the tier adds no parallel recency bookkeeping
  and inherits its correctness from the eviction order (ADR-0023). A key crossing
  the eviction boundary spills its value to flash and leaves the pointer in RAM. A
  size-watermark background sweeper is the rejected alternative: simpler but
  coarser, and it would keep cold values resident past the eviction boundary. The
  exact migration eviction algorithm (W-TinyLFU versus segmented LRU versus LFU)
  is an open decision deferred to EVICTION.md [extstore-defaults].

### Append-only flash pages

- Values are written to append-only flash log pages, each with a per-page write
  buffer, the extstore layout [extstore-defaults]; writes are sequential so write
  amplification stays low. An in-place slab on flash is rejected: it fragments and
  forces read-modify-write. io_uring batches the page-buffer flushes through the
  shared writer (#67); whether tiering writes share one registered buffer pool
  with the snapshot path or hold a dedicated pool is owned by #67. Page size and
  write-buffer size relative to the extstore defaults [extstore-defaults] are open.

### Write-amplification budget

- Reclamation is drop-unread compaction over io_uring writes, not LSM leveled
  compaction: compaction reads live pointers, copies survivors forward, and drops
  unread pages, recycling whole pages so write amplification stays a small
  constant multiple of logical writes rather than scaling with LSM fan-out
  [keydb-flash-rocksdb]. We set and enforce a WA budget (target under 2x) and
  bound space amplification with a live-bytes-per-page reclamation threshold.
  Rejecting an embedded RocksDB/LSM backend is ADR-0023 (#65); the
  write-amplification argument against an LSM backend, leveled compaction burning
  SSD endurance and tail latency on cache churn, is what that decision freezes
  [keydb-flash-rocksdb]. The hard WA ceiling and the compaction trigger ratio that
  holds it are open.

### Durability

- The tier is durable across restart: flash log pages are fsync'd and a checkpoint
  pointer table is persisted, then recovered on restart, against the recovery
  contract in #58. This is the extstore model plus durability, the explicit
  addition over the volatile extstore tier. The checkpoint cadence for the pointer
  table and its overlap with #58 recovery is open. Metrics expose SSD bytes
  written, measured write amplification, and device endurance/wear through the
  observability registry (#86).

## Open questions

- Eviction algorithm for migration: W-TinyLFU versus segmented LRU versus LFU
  [extstore-defaults] (deferred to EVICTION.md, #48).
- WA-budget hard ceiling and the compaction trigger ratio that holds it.
- Page size and write-buffer size relative to extstore defaults [extstore-defaults].
- Pointer width: confirm roughly 12 bytes versus a wider page space for larger
  flash.
- Checkpoint cadence for the pointer table and its overlap with #58 recovery.
- Whether tiering writes route through #67's shared pool or a dedicated submission
  ring.

## Acceptance and test hooks

- RAM holds the key plus the roughly 12-byte pointer; values are served from flash
  log pages [extstore-defaults].
- Migration is driven by the cache eviction signal at a documented spill threshold
  [extstore-defaults], reusing the EvictionPolicy order (#48), with no parallel
  recency clock (ADR-0023).
- The tier is durable across restart via fsync'd flash pages and a recovered
  pointer table, against the #58 recovery contract.
- Measured SSD write amplification stays within the published budget and well
  below LSM levels [keydb-flash-rocksdb].
- The bytes-per-key figure and the resulting practical RAM:flash ratio ceiling are
  documented [keydb-flash-key-cache].
- Metrics expose SSD bytes written, write amplification, and device endurance/wear
  (#86).

## References

- ADR-0023, ADR-0014; issues #66, #58, #64, #65, #67, #48, #50, #49, #111, #86, #1.
- Specs: PERSISTENCE.md, HYBRIDLOG_ENGINE.md, EVICTION.md, WTINYLFU.md.
- Research: docs/research/persistence-storage-engines.md, docs/research/keydb.md.
- Claims: [extstore-defaults], [keydb-flash-key-cache], [keydb-flash-rocksdb].
