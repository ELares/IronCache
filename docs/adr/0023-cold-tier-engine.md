# ADR-0023: Cold-tier engine (reject RocksDB/LSM, adopt a hybrid log)

Status: Accepted
Issue: #65

## Context

IronCache needs a cold tier so that warm-but-evicted keys can spill to flash and
the working set can exceed RAM, without dragging a C++ build dependency into the
single static binary or burning SSD endurance on a high-churn cache workload.
This ADR settles only the engine family for that cold tier. It does not specify
the engine internals (#64 owns the HybridLog storage engine) and it does not
specify the placement policy (#66 owns when a key crosses into and out of the
cold tier). Parent: #58, program #1.

The obvious move is to embed RocksDB, an LSM tree, as the cold backend, the same
path KeyDB took for its FLASH feature. KeyDB FLASH proves Redis-on-RocksDB works:
it uses RocksDB as a pluggable storage provider with separate column families for
data and expires and point-in-time snapshots [keydb-flash-rocksdb], and it is
enabled with a small config surface bound to an allkeys-lru or allkeys-lfu
eviction posture [keydb-flash-config]. The case against it is endurance and
binary shape. Leveled compaction drives large write amplification, well above
10x on cache-like churn by the standard LSM property, which shortens flash life;
and compaction stalls are the documented operational risk behind tail-latency
behavior on large datasets, where the vendor benchmark that reaches about 85
percent of RAM throughput at 190 GB does so on direct-attached NVMe under a
single workload, not as a general SSD endurance guarantee
[keydb-flash-190gb-benchmark]. A C++ dependency also breaks the single static
binary and complicates cross-compilation, which the Compatible tenet ranks first.

The alternative engine family is a hybrid log. FASTER's HybridLog is append-only
with in-place updates of the hot set and no compaction, so it has no
compaction-induced tail latency, it is endurance-friendly, and it is
Rust-implementable, keeping the static binary; its measured single-machine peak
is up to 160 million ops per second on YCSB, so the design is fast enough to be
the steady-state engine [faster-peak-throughput]. The open question inside the
hybrid-log family is shape: a single log (FASTER) versus a two-component
hot-log/cold-log split (F2). F2 keeps write-hot keys in an in-memory hot log and
write-cold keys in an on-disk cold log with a read cache, migrating records
hot-to-cold as they age, targeting exactly the larger-than-memory skewed
workloads a cache produces [f2-hot-cold-log-two-tier], and it reports throughput
between 2x and 11.9x over existing key-value stores, about 11.8x over RocksDB on
average on skewed workloads [f2-throughput-vs-rocksdb]. That shape question is
not settled here; it is an experiment feeding #64 (see Consequences).

## Decision

- **Reject RocksDB, and any embedded LSM, as the primary cold backend.** The
  decisive reasons resolve in tenet order. Compatible first: an embedded C++
  RocksDB breaks the single static binary and the cross-compilation story, and
  the KeyDB FLASH precedent, while it works [keydb-flash-rocksdb], carries a
  config and provider surface we would inherit wholesale [keydb-flash-config].
  Efficient second: leveled-compaction write amplification well above 10x by the
  standard LSM property is poison for SSD endurance on a high-churn cache, and
  compaction stalls are the documented tail-latency risk on large datasets
  [keydb-flash-190gb-benchmark].
- **Adopt a hybrid log as the cold-tier engine.** It is append-only, so there is
  no compaction and no compaction-induced tail latency, it is pure Rust so the
  static binary holds, and FASTER's measured peak shows the family is fast enough
  for the steady-state engine [faster-peak-throughput]. The concrete engine
  (regions, ownership, index geometry) is specified in #64.
- **The hybrid-log shape (single-log FASTER vs two-component F2) is left to a
  gated experiment feeding #64.** The provisional hypothesis is that F2 wins
  under cache-grade access skew, because it sizes the in-memory hot log to the
  working set and pages cold records to the cold log, lowering memory overhead
  versus a single log that dilutes the in-memory hash index with cold keys
  [f2-hot-cold-log-two-tier], consistent with F2's reported advantage over an
  LSM on skewed workloads [f2-throughput-vs-rocksdb]. The exact experiment is:
  run both shapes on a Zipfian cache corpus that exceeds RAM, hold the flash
  device, total memory budget, and value-size mix fixed, vary the skew parameter,
  and measure (a) bytes of host RAM per resident working-set entry, (b) read and
  write throughput per core, and (c) cold-log read amplification under the read
  cache; F2 is adopted only if it lowers memory-per-entry under skew without
  losing throughput against the single log. No numbers are recorded here because
  this experiment is harness-blocked (it needs the engine and the benchmark
  harness from #64); only the hypothesis and the procedure are recorded.
- **Migration is driven by cache eviction signals, not a parallel recency
  clock.** A key crossing the eviction boundary the cache layer already computes
  is the cold-tier migration trigger, so the cold tier reuses an existing signal.
  #66 owns the placement policy.
- **Keep a lean Rust LSM as a fallback only, built behind a gate.** It is
  constructed only if the hybrid log fails its throughput or memory targets in
  #64. It is not built speculatively.

## Rejected Alternatives

- **Embed RocksDB as the primary cold backend.** Battle-tested, and KeyDB FLASH
  proves Redis-on-RocksDB works with a rich config surface [keydb-flash-rocksdb]
  [keydb-flash-config]. Rejected: the C++ dependency breaks the single static
  binary (Compatible), and leveled-compaction write amplification well above 10x
  by the standard LSM property, plus compaction stalls that are the documented
  tail-latency risk on large datasets [keydb-flash-190gb-benchmark], wreck SSD
  endurance and tail latency on a high-churn cache (Efficient). This is the
  rejection this ADR exists to freeze so it is not relitigated.
- **Build a lean Rust LSM as the primary engine.** Pure Rust would keep the
  static binary and give full control of compaction strategy. Rejected as the
  primary: it reinvents the hard part, compaction, that pushed us off RocksDB,
  and it still pays compaction write amplification and stalls in some form. It
  survives only as the gated fallback above, never built unless the hybrid log
  misses its #64 targets.
- **Commit now to a specific hybrid-log shape (single-log FASTER, or F2).**
  Rejected as premature: the single-log vs two-component choice turns on
  measured memory-per-entry and throughput under cache skew, which the bake-off
  in #64 will produce. The hypothesis favors F2 under skew
  [f2-hot-cold-log-two-tier] [f2-throughput-vs-rocksdb], but the ADR commits to
  the hybrid-log family, not to a shape, until that experiment runs.

## Consequences

- The single static binary is preserved: the cold tier carries no C++ toolchain,
  unlike the RocksDB path, satisfying the Compatible tenet that ranks above
  Efficient and Simple.
- SSD endurance and tail latency improve relative to an LSM cold tier, because an
  append-only hybrid log has no leveled compaction and so neither the
  greater-than-10x write amplification that the LSM property implies nor the
  compaction stalls documented as the tail-latency risk on large datasets
  [keydb-flash-190gb-benchmark].
- The cold-tier shape decision (FASTER single log vs F2 two-component) is
  deferred to a harness-blocked bake-off feeding #64; the engine in #64 must be
  structured so the shape is a swappable choice until that experiment resolves.
  The provisional hypothesis, F2 wins under skew, is recorded but not relied on
  as fact, and no throughput or memory numbers are asserted until measured.
- A lean Rust LSM fallback remains a live option, but unbuilt: if the hybrid log
  misses its #64 throughput or memory targets, that fallback is constructed under
  a superseding decision, keeping this ADR's rejection of LSM-as-primary intact
  while leaving a measured escape hatch.
- The cold tier inherits the cache eviction signal as its migration trigger
  (#66), so it adds no parallel recency bookkeeping; this binds the cold tier's
  correctness to the eviction order the cache layer already maintains.
