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

### Current implementation: snapshot-only durability and the RPO contract

What ships today is **SNAPSHOT-ONLY** durability (Tier 0 + a partial Tier 1), and it
is important to be honest about the data-loss window (RPO) so operators are not lulled
into a false sense of durability:

- **Durability is via SNAPSHOTS only.** There is **no AOF / append-only file** in this
  build. Data is persisted by an explicit `SAVE` / `BGSAVE` (a forkless per-shard dump
  committed via an fsync'd manifest), or by the periodic save policy when one is
  configured. `CONFIG SET appendonly yes` is therefore **rejected** (it is not silently
  accepted), and `CONFIG GET appendonly` is always `no`. The append-log / hybrid-log
  tiers above remain the roadmap (#63/#64).
- **The default save policy is OFF.** With no `data_dir` configured, persistence is
  entirely off (the ephemeral default, ADR-0014): nothing is ever written and a crash
  loses the whole dataset. Even with a `data_dir` set, the **periodic** save policy
  (`save_interval_secs` / `save_min_changes`, also settable at runtime via
  `CONFIG SET save "<seconds> <changes>"`) **defaults to disabled** -- so data persists
  ONLY on an explicit `SAVE` / `BGSAVE` until an interval is configured.
- **The RPO (data-loss window) equals the save interval.** Because the only durable
  points are explicit saves and the periodic cadence, a crash loses every write since
  the last committed snapshot. With the periodic policy enabled the worst-case RPO is
  approximately the configured `save_interval_secs` (plus the tail of writes admitted
  after the last tick); **with the default OFF policy and no explicit save, the RPO is
  UNBOUNDED** (the entire keyspace can be lost). `CONFIG GET save` reports the real
  active policy (empty when off), `LASTSAVE` / INFO `# Persistence` `rdb_last_save_time`
  report the last committed save time (seeded on boot from the loaded snapshot), and
  INFO `rdb_changes_since_last_save` reports how many writes are currently at risk -- so
  the live loss window is observable rather than guessed. The zero-RPO strict tier
  (Tier 2 above) is still roadmap.

### Yielding snapshot: a bounded save tail, and the fuzzy-consistency decision (#571)

The shipped per-shard dump is **forkless** and **memory-neutral** (a resumable, constant-memory
`snapshot_chunk` SCAN pull, #60), and it now also **yields the serving shard between snapshot
chunks** so a `SAVE`/`BGSAVE` does not block the shard for its whole keyspace dump:

- **Per-chunk borrow + yield.** The dump loop re-acquires the shard's store borrow **per chunk**
  (not across the whole dump), pulls one bounded `DUMP_CHUNK` batch, releases the borrow, and
  `yield`s. Between yields the shard's executor services queued writes (its connection serve loops,
  and -- for a `BGSAVE`, which runs off a spawned task -- its drain loop). So a write homed on a
  dumping shard is **serviced during the dump instead of blocked until it ends**: the save tail is
  bounded and predictable (one `DUMP_CHUNK` of work between servicing points) rather than a
  full-keyspace stall. This is the tail-latency moat versus Redis fork-stalls and Dragonfly
  snapshot-spikes -- a save no longer causes a p99.9 spike whenever it aligns with writes.

- **Consistency is DELIBERATELY fuzzy: an approximate warm-start restore point.** Because writes
  interleave between chunks, the snapshot is **no longer a strict point-in-time** view even within a
  single shard (an early chunk may hold a key's pre-write value while a later chunk holds another
  key's post-write value), and it was already cross-shard fuzzy (each shard dumps at a slightly
  different instant). We **accept** this: IronCache is a **cache**, so an approximate warm-start
  restore point that never stalls the request path is worth far more than a strict global
  point-in-time, and no cross-key transactional durability is promised. This is a conscious decision,
  not an accident. A strict point-in-time snapshot would require versioning / copy-on-write (the
  forkless epoch-cut serializer sketched in SNAPSHOT.md), a much larger change deliberately **out of
  scope** here.

- **The dump stays correct under concurrent mutation.** The chunk cursor is the resize-stable
  `scan_hash` **threshold** of the next un-examined key (KEYSPACE.md cursor-stability contract), not a
  table slot index, and the walk rebuilds its sorted order from current contents each chunk. So a key
  present for the **whole** dump is captured **at least once** (SCAN semantics); a key created or
  deleted mid-dump may or may not appear. This is the same iterator the replication full-sync already
  relies on while it awaits shipping each chunk to a replica, and it is unchanged by the per-slot
  table partition (#570). A regression test drives a dump chunk-by-chunk while inserting (forcing
  resizes) and deleting between chunks and asserts every stable key survives.

- **Crash-safety is independent of the fuzziness and still holds.** The **manifest is written LAST**
  (tmp -> fsync -> rename, after every per-shard file is durable), so a torn or partial dump is never
  loaded: a boot loads a fully committed (if fuzzy) snapshot or the prior one, never a half-written
  file. A CRC mismatch on a shard file is treated as no-snapshot for that file, and a foreign / newer
  format version fails closed loudly (#530) rather than silently starting empty.

### Save-backpressure throttle: the concurrent-snapshot p99.9 stopgap (#577)

Yielding between chunks (#571) does NOT bound the save tail: the `c7g` tail bench measured a
catastrophic concurrent-snapshot p99.9 of ~3.5s, because a full-speed dump contends with the datapath
for the whole save window. `save_backpressure_percent` was introduced on the HYPOTHESIS that throttling
the save would cut that tail.

WARNING (measured on c7g, the hypothesis was WRONG): **the throttle makes the tail WORSE, not better.**
A/B: snapshot p99.9 was 3.5s at `pct=100` (off), 6.8s at `pct=25`, 16.75s at `pct=10`. Throttling
STRETCHES the save's wall-time, and because the real bottleneck is the save SHARING MEMORY BANDWIDTH
with the datapath (not CPU duty), a longer save is a LONGER contention window and thus a WORSE tail.
The knob is retained ONLY for bounding background save CPU when the during-save tail is not a concern;
it must NOT be used to protect the tail. The actual fix is the per-slot Arc-COW (#588, below).

- **The knob.** `save_backpressure_percent` is a runtime config value in `1..=100`, **default 100 =
  no throttle** (a save dumps at full speed, byte-identical to the pre-#577 behavior, so the default
  deployment is unchanged -- this is strictly opt-in). It is live-settable with
  `CONFIG SET save-backpressure-percent <1-100>` (validated: a value outside `1..=100` is rejected,
  never silently clamped) and reported by `CONFIG GET save-backpressure-percent`.

- **The throttle.** In the per-shard dump loop (`coordinator::save_shard_local`), after each chunk the
  loop reads the live percent and, when it is below 100, **sleeps proportionally**:
  `sleep = chunk_time * (100 - pct) / pct`. The original intent was to share the core `pct : (100 - pct)`
  so the datapath throughput stays above the offered load. MEASUREMENT DISPROVED this: the bottleneck is
  not CPU duty but memory bandwidth, so throttling only lengthens the save (and the tail). The per-chunk store borrow
  is already dropped before the sleep (the no-borrow-across-await contract), so the sleep just lets the
  shard service its queued writes. `chunk_time` is measured on the shard's **Env monotonic clock** and
  the sleep is armed on the **Runtime timer seam** (ADR-0003), so the save path carries no `std::time`.
  At `pct == 100` no sleep is inserted and the loop is byte-identical to the #571 yielding dump.

- **The TRADEOFF (honest cost).** Throttling **stretches the save's wall-time to about `1/pct`**: at
  `pct = 10` a ~2s dump becomes a ~20s wall-time save (the save only gets a tenth of the core). That is
  fine at a **realistic 5-15 min save cadence** (a 20s save every 10 min is ~3% of the background
  window, and the tail is protected the whole time), and **wrong at an aggressive every-few-seconds
  cadence** (a 20s save on a 3s cadence never finishes before the next one is due). **The rule is
  `save-cadence >> save-duration`**: pick a `pct` low enough to protect the tail but high enough that a
  throttled save still completes comfortably inside the cadence. When the cadence cannot be relaxed,
  leave the throttle at 100 (or use a milder `pct`) and rely on the isolation fix below.

- **The actual fix that landed: per-slot Arc-COW (#588).** The throttle failed (above); an earlier
  off-thread-encode attempt (#576 PR-B, #586) ALSO measured no improvement (~3.9s) because it still
  copied the keyspace on the serving core. What worked is the per-slot Arc copy-on-write (#588): each
  slot table is an `Arc<HashTable>`; a save `Arc`-clones each slot into a frozen `Send` view read IN
  PLACE off-core by a dedicated persist thread, with NO O(N) serving-side copy (a write to a frozen
  slot copies just that slot). Measured on c7g: concurrent-snapshot p99.9 dropped from 3.5s to ~291ms
  (11.5x). That ~291ms is HISTORICAL -- it is the #588 Arc-COW change alone; the during-save tail was
  later taken to ~30ms by PR #742 (see docs/bench/TAIL_LATENCY.md for the current record).
  **SUPERSEDED:** the residual gap to ms-class was long believed to be a FUNDAMENTAL
  memory-bandwidth-headroom tradeoff of the multi-core datapath, reachable only by shrinking the
  save's data footprint. That was DISPROVEN by measurement (#676): a pacer built on the bandwidth
  theory was refuted on c7g and reverted (#740/#741), and a 1-shard-vs-8-shard ablation showed the
  BIGGER 1-shard save stalling ~60x LESS -- the opposite of any bandwidth story. The real cause was a
  cross-shard-hop head-of-line block (`__ICSAVE` running inline in each sibling shard's drain loop);
  PR #742 spawns it off the loop and the during-save p99.9 went 794ms -> 30ms, into Dragonfly's class.
  The historical framing is kept below for provenance (see CONFIG.md "Dedicated persist core" and
  #589), but reducing the save footprint (incremental/compressed snapshots) is an optimization,
  which is deferred. The durable-save tail is COMPETITIVE (sub-second), not category-leading.

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
  #34, #28, #86, #137, #571, #577, #576.
- Claims: [dragonfly-forkless-snapshot-mechanism], [dragonfly-snapshot-constant-memory],
  [dragonfly-bgsave-memory-efficiency], [redis-cow-rss-doubling],
  [garnet-storage-tier-default-off], [garnet-aof-default-off],
  [garnet-compaction-default-none], [garnet-checkpoint-modes],
  [redis-appendfsync-default], [redis-everysec-real-worst-case-2s],
  [redis-rdb-compression-checksum-defaults], [memcached-warm-restart-mmap-sigusr1],
  [f2-hot-cold-log-two-tier], [faster-peak-throughput].
