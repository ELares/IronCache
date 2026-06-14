# Experiment: Snapshot memory-overhead ceiling and parallel-restart bake-off

Issue: #61. Provisional decision: PERSISTENCE.md (#58) durability tiers and the
forkless snapshot (SNAPSHOT.md, #60), the segment+manifest log (DURABLE_LOG.md,
#63), and ADR-0022 (forkless serialization). This doc records the harness-blocked
experiment that fixes the snapshot-overhead ceiling, the strict-tier fsync
mechanism, and the restart-load strategy; it does not re-decide forklessness
(ADR-0022) or the durability stance (ADR-0014).

## Provisional decision (already pinned)

The forkless versioned serializer is the only snapshot path (SNAPSHOT.md, #60),
chosen because Redis fork-based RDB and AOF-rewrite copy pages on write during the
dump, so a write-heavy workload can drive RSS toward roughly 2x the dataset before
the child exits [redis-cow-rss-doubling]. In-process page-granular COW is rejected
as the headline mechanism for the same reason: its overhead is a function of
mutation rate, not a constant, so no fixed promise could be honored. The segment-
log engine (DURABLE_LOG.md, #63) is adapted instead: an immutable sealed base
segment plus a small mutable head means a snapshot copies only the head, so
steady-state overhead is the head size, not the dataset. That yields the
provisional ceiling recorded here: snapshot overhead at most 12.5 percent of the
resident set (one-eighth), enforced by sealing the head and rolling a new segment
once it crosses the watermark, with snapshot throttle plus targeted write back-
pressure as the enforcement lever rather than spill (spill converts a memory bound
into unpredictable tail latency and is rejected as the primary lever). A
versioned/MVCC memtable is the runner-up: it bounds overhead to live versions but
retains long-lived versions under read load, so its ceiling is softer. Restart
provisionally mmaps the immutable base so the page cache, not a parser, does the
work, loads remaining segments in parallel across cores, and materializes values
lazily on first access, contrasting with single-threaded RDB replay
[redis-multipart-aof-since-7.0]. For the strict tier, group-commit fdatasync
amortizes the per-write syscall that everysec avoids [redis-appendfsync-default],
with io_uring batched fsync the candidate to validate on NVMe (#67). This doc does
not re-decide those; it tests whether the 12.5 percent ceiling holds, which fsync
mechanism wins, and that parallel restart beats serial.

## Why this is harness-blocked

The numbers that would settle these are not on paper. The 12.5 percent ceiling is
a derived target from a sealed-head plus rolling-segment geometry, not a measured
quantity, and its defensibility depends on the value-size distribution: a corpus
dominated by tiny values keeps the mutable head small relative to the sealed base,
while large values inflate the head between rolls, so whether one-eighth holds
across distributions, or needs a per-tier value, can only be answered by holding a
real workload at the watermark and measuring peak head size versus resident set.
The strict-tier fsync choice is a measured crossover, not a ranking that can be
read off: group-commit fdatasync and io_uring batched fsync trade per-syscall
latency against batch amortization differently as the commit batch grows and as
the NVMe device's own queue depth saturates, so the crossover point is device- and
batch-size-specific [redis-appendfsync-default] [io-uring-sqpoll-registered-buffers].
Restart time is linear in dataset size on one core in the incumbent
[redis-multipart-aof-since-7.0]; whether mmap-plus-parallel-load actually beats it,
and by how much, depends on segment count, core count, and page-cache warmth, none
of which is answerable without the engine and the benchmark harness from #8. And
lazy materialization's effect on the p99 of the first post-restart request is an
empirical tail question, not a design one. No numbers are recorded here beyond the
provisional 12.5 percent target, which the experiment exists to confirm or revise.

## Experiment to run

Three measurement axes, each driven through the real segment-log engine
(DURABLE_LOG.md, #63) and the forkless serializer (SNAPSHOT.md, #60), never a
synthetic loop, instrumented for RSS and used_memory and emitting machine-tagged
CSV (#8):

- Overhead-ceiling sweep. Hold a write-heavy workload at the segment watermark and
  measure peak snapshot extra RSS as a fraction of resident set, across at least
  {tiny <=64B, mixed, large >=4KB} value-size distributions. Fixed: maxmemory,
  write rate, seed, segment roll watermark, build profile. Varied: value-size
  distribution; enforcement lever (seal-and-roll only, then seal-and-roll plus
  write back-pressure). Measured: peak extra RSS / resident set, and whether the
  watermark roll keeps pace without back-pressure.
- Strict-tier fsync crossover. Run the strict (group-commit) tier two ways,
  group-commit fdatasync and io_uring batched fsync (#67), on NVMe. Fixed:
  workload, durability tier, device, seed. Varied: fsync mechanism; group-commit
  batch window across at least {small, default, large}. Measured: acknowledged-
  write throughput and p99.9 ACK latency per mechanism per batch window; report
  the throughput crossover point.
- Restart-time versus dataset size. Restart from a multi-segment fixture two ways,
  mmap base plus parallel per-segment load versus serial single reader. Fixed:
  fixture content, value-size mix, build profile. Varied: load strategy; dataset
  size across the canonical size matrix; with and without lazy materialization.
  Measured: wall-clock restart time versus dataset size, and the p99 of the first
  request after restart with and without lazy materialization.

All three axes run across the full canonical hardware matrix including a 1-vCPU
machine, reporting per-core where throughput is involved.

Decision rule. (1) Adopt the 12.5 percent ceiling as a single published number
only if peak extra RSS / resident set stays at or below one-eighth across all
value-size distributions under seal-and-roll plus bounded back-pressure; if a
distribution breaches it, publish a per-tier ceiling instead. (2) Adopt whichever
strict-tier fsync mechanism wins acknowledged-write throughput at the chosen
group-commit batch window without losing p99.9, taking the measured crossover as
the documented default. (3) Adopt mmap-plus-parallel restart only if it beats
serial measurably on the multi-segment fixture, and enable lazy materialization by
default only if it does not regress the first-request p99 enough to warrant
prefetch.

## What would change the decision

- A value-size distribution under which the sealed-head geometry cannot hold one-
  eighth even with bounded back-pressure, forcing a per-tier ceiling rather than a
  single published number.
- io_uring batched fsync beating group-commit fdatasync on acknowledged-write
  throughput and p99.9 across the NVMe matrix (or the reverse), moving the strict-
  tier default [redis-appendfsync-default].
- Parallel mmap restart failing to beat serial replay [redis-multipart-aof-since-7.0]
  on the multi-segment fixture, leaving serial the simpler default.
- Lazy materialization regressing the first-request p99 enough that eager prefetch
  of the hot set on restart is warranted.
- A back-pressure lever that holds the ceiling without a madvise- or fsync-storm
  latency spike on the request path, which would let the ceiling tighten below
  one-eighth.

## References

- Parent: #58; vision: #1. Related: #60 (SNAPSHOT.md), #63 (DURABLE_LOG.md), #67
  (io_uring write path), #8 (harness + machine-tagged CSV), #11 (index).
- ADR-0022 (forkless serialization), ADR-0014 (durability stance).
- Specs: docs/design/PERSISTENCE.md, docs/design/SNAPSHOT.md,
  docs/design/DURABLE_LOG.md.
- Claims: [redis-cow-rss-doubling], [redis-multipart-aof-since-7.0],
  [redis-appendfsync-default], [io-uring-sqpoll-registered-buffers].
