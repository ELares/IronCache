<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# IronCache efficiency findings and optimization scope (A6, dated 2026-06-16)

This is the "scope the gap" step of the performance track (task A6): now that the
measurement harness exists (A1 memory model, A2 load generator, A3 reproducible
run, A4 head-to-head, A5 per-PR regression gate), this records where IronCache
actually stands against the bar and scopes the optimization work precisely,
rather than optimizing speculatively. The track's principle holds: you do not
optimize what you have not measured, and you do not rewrite the store waist
blind.

## How these numbers were taken (and their caveats)

An INDICATIVE head-to-head via `scripts/bench/headtohead.sh`:

- IronCache `0.0.0` vs **redis-server 7.2.1** as a wire-compatible STAND-IN. The
  published bar is the pinned **valkey-server 9.1.0** (docs/bench/COMPETITORS.md);
  Valkey 8.0+ embeds keys/values, so its memory will differ from this 7.2 redis.
- **Unpinned, on a 10-core macOS dev box** (no taskset), with the load generator
  co-resident on the same cores as the server. So the THROUGHPUT comparison is
  contention-bound and NOT authoritative.
- 300,000 keys, 128-byte values, zipf 0.99, 90% reads, 50 connections, 5s.

The authoritative verdict requires running the same harness on a pinned Linux box
(disjoint server/client cores) against valkey-server 9.1.0. That run is a
CI/dedicated-runner activity; the harness is ready for it.

## Indicative results

| Metric | IronCache | redis 7.2.1 | IronCache / competitor |
| --- | ---: | ---: | ---: |
| bytes-per-key (used_memory delta / N) | 527 | 245 | **2.15x (worse)** |
| qps-per-core (closed-loop, unpinned) | 7151 | 7528 | 0.95x |
| open-loop p50 | 1006 us | 4187 us | 0.24x (better) |
| open-loop p99 | 2513 us | 65663 us | 0.04x (better) |

### What is trustworthy vs not

- **bytes-per-key (~2.1x heavier) is RELIABLE.** It is a deterministic
  `used_memory` delta over an identical deterministic populate; it is not
  sensitive to pinning, contention, or the co-resident load generator. This is a
  real gap and the same direction the A1 memory model predicted.
- **qps-per-core (~parity) is NOT authoritative.** On an unpinned box with the
  load generator stealing cores from the server, this is contention/loopback
  bound, not a clean server-throughput measurement. IronCache's far lower p50/p99
  latency suggests headroom that a pinned run would expose; the pinned Linux run
  is needed before drawing a throughput conclusion.

## The memory gap, decomposed (A1 memory model)

The A1 `memmodel` decomposition (object vs table slack) locates the ~2.1x:

1. **Fat per-slot value.** The stored-value type (`KvObj`) is sized for its
   largest inline variant, so every hash-table slot reserves that footprint even
   for an int or a short string. Measured per-slot footprint is ~160 bytes. Redis
   and Valkey keep a pointer-sized slot and put the object behind it (Valkey 8.0+
   even embeds small ones in a single allocation), so their per-entry overhead is
   much smaller.
2. **Hash-table slack.** The Swiss-table (hashbrown) bucket array runs at up to
   7/8 load, so at the operating load factor the slot array contributes ~210
   bytes/key of amortized slack on top of the slot's own size. A fatter slot
   makes this slack proportionally worse.

Together these dominate the overhead: of ~527 bytes/key for a 128-byte value,
roughly 400 bytes is metadata + slack, versus roughly 120 bytes for redis.

## Optimization scope (prioritized; each its own effort)

These are scoped, NOT executed here: each touches the frozen Store waist or the
index and so needs its own design, PR, and review, targeted against the real
pinned-Linux-vs-valkey numbers. All are now protected by the A5 perf-gate, which
will catch any throughput regression an optimization introduces.

- **L1 (highest impact): shrink the per-slot footprint.** Box the large `KvObj`
  variants so the table slot holds a small (near pointer-sized) value, slashing
  both the slot size AND the amortized table slack per key. Expected to bring
  bytes-per-key toward the Redis/Valkey range. Risk: a pointer indirection on the
  read hot path; must be benchmarked against the throughput gate before it lands.
  This is the single biggest lever and the recommended first optimization PR.
- **L2: a more compact index.** The DragonflyDB-style Dashtable the README cites
  (extendible hashing, far less per-entry metadata than a Swiss table at high
  load) would cut the table slack structurally. Larger; later; its own design.
- **L3: load-factor / sizing tuning.** Cheaper than L1/L2 but bounded upside;
  only worth it after L1 since L1 changes the slot size the slack is computed on.
- **Throughput: confirm before optimizing.** The indicative parity is likely an
  unpinned-co-resident artifact. Run the pinned Linux head-to-head first; if a
  real per-core gap appears, the io_uring data path (issue #28, currently
  tokio/epoll) is the lever. Do not optimize throughput speculatively while the
  measurement says parity.

## Next step

Run `scripts/bench/headtohead.sh` on a pinned Linux runner against valkey-server
9.1.0 for the authoritative verdict, then execute L1 as the first optimization PR
under the A5 perf-gate. The measurement infrastructure (A1 to A5) is complete and
makes that work measurable and regression-safe.

## UPDATE (2026-06-16): the pinned-Linux throughput verdict, and where the gap actually is

The pinned-Linux head-to-head above was RUN (CI workflow `headtohead.yml`, ubuntu
runner, server `taskset` 0-1 / client 2-3, 200k keys, 128B). It OVERTURNED the
"throughput parity is probably a measurement artifact" guidance: on a clean pinned
box IronCache did **~9k qps/core vs redis ~65k/core (~7x slower)**. Memory stayed a
clear win on Linux too (0.92x). So the throughput gap is REAL, not a macOS artifact.

The root cause was then isolated (measure-first, before touching any code):

- **NOT the cross-shard coordinator.** Per-core throughput is FLAT as shards grow
  (local: 1/2/4 shards all ~35k/core) while the cross-shard fraction climbs
  0%->75%. The oneshot+mpsc hop is cheap.
- **NOT thread oversubscription.** A controlled CI probe (the `IRONCACHE_SHARDS`
  knob) ran 1 shard vs 2 shards on the SAME 2 pinned cores: 8.7k -> 17.2k qps, a
  near-linear ~2x. Adding a shard thread DOUBLES throughput, so there is no
  oversubscription collapse; IronCache scales cleanly with cores.
- **IT IS per-request datapath cost.** A CPU profile under load (150k qps, macOS
  `sample`) shows self-time dominated by syscalls + the async reactor + thread
  parking: `kevent` + `__semwait_signal` + `__sendto` + `__recvfrom` ~= 24k
  samples vs the IronCache compute + `memcmp`/`memmove` ~= 6k (~80% I/O+reactor,
  ~20% cache logic). IronCache pays ~one `recv` + one `send` + tokio reactor/
  task-scheduling round-trip PER request, where redis's hand-rolled epoll loop is
  far leaner per op. The cache logic itself is cheap; the I/O datapath is the cost.

So the throughput lever is the **I/O datapath, issue #28 (io_uring)** -- batched
submission/completion to cut the per-request syscall + reactor overhead -- and/or a
leaner event loop with read/write batching. It is NOT a memory, sharding, or
threading change (memory is already a clear win; sharding/threading scale fine).
The AUTHORITATIVE absolute number still wants a dedicated bare-metal pinned-Linux
box vs valkey 9.1.0 (a shared GitHub 4-vCPU VM is indicative only), but the SHAPE
of the gap (reactor/syscall-bound per-op) is now measured and clear.
