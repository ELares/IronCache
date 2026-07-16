# io_uring vs tokio datapath (2026-07-16)

The first COMMITTED io_uring-vs-tokio artifact (the +189% figure had circulated
without a reproducible log). Part of the "make io_uring real" campaign (#284).
Raw log: [iouring-vs-tokio-2026-07-16.raw.txt](iouring-vs-tokio-2026-07-16.raw.txt).

## Setup
- One c7g.4xlarge (16-core Graviton3, arm64, AL2023). SERVER pinned to cores 0-7
  (8 shards, thread-per-core); the repo `loadgen` (closed-loop) pinned to the
  disjoint cores 8-15 over loopback. SINGLE-ENDPOINT (one port; the client's
  random keys hop to the owning shard -- IronCache's WORST routing).
- 128B values, 1,000,000-key zipf(0.99) space, 90% GET / 10% SET, 128 connections,
  a warmup write pass then a 10s measured pass, pipeline depth swept. Persistence
  off. Reproduced twice; values are the mean.
- Both binaries built from the same commit: the default (tokio) build and the
  `--features io_uring` build run with `--runtime io_uring`. The io_uring server
  CONFIRMED it selected the ring at boot (`runtime = io_uring: using the Linux
  io_uring datapath`), not a silent tokio fallback.

## Result (single-endpoint peak qps, mean of 2)
| pipeline | tokio | io_uring | io_uring vs tokio |
| ---: | ---: | ---: | ---: |
| 1  | 262,211 | 241,601 | -7.9% |
| 8  | 1,785,296 | 1,778,753 | -0.4% (tie) |
| 16 | 1,461,484 | **2,601,825** | **+78.0%** |
| 32 | 1,094,650 | **3,135,965** | **+186.5%** |

## Read
1. **io_uring eliminates the single-endpoint deep-pipeline CLIFF.** tokio peaks at
   depth 8 then DECLINES (1.79M -> 1.46M -> 1.09M) -- the cross-shard-hop machinery
   saturating under deep pipelining. io_uring CLIMBS monotonically (1.78M -> 2.60M
   -> 3.14M). At depth 32 io_uring does 2.9x tokio.
2. **This is the single-endpoint lever.** IronCache's weakest column vs Dragonfly
   was single-endpoint deep pipeline (the Dragonfly re-bench had tokio at 1.26M @
   depth 64 vs Dragonfly 3.34M). io_uring at 3.14M @ depth 32 single-endpoint is
   competitive with Dragonfly there -- closing the gap on the shipped-datapath's
   worst config, WITHOUT a cluster-aware client.
3. **The honest cost: pipeline 1** (no pipelining), where io_uring is ~8% slower --
   the submission/completion machinery does not amortize at one op per round-trip.
   This is the regime the perf-gate must assert never-worse on for a ship-by-default
   decision; it is also the least representative of a high-throughput workload.

## Caveats
- io_uring is NOT in the published release binaries and falls back to tokio under
  TLS (default-on); this measures a from-source `--features io_uring` plaintext
  build. See #284 for the ship-by-default gates.
- This reflects current main (includes the OneShotFixed WRITE-tier wiring, #678).
  The registered-buffer zero-copy GET (#515) and multishot recv (#513) fast paths
  are still ahead; this is the baseline they build on, not the ceiling.
