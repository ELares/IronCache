# Cross-shard coalescing re-bench: the tokio deep-pipeline cliff is gone (2026-07-17)

The committed [IOURING_DATAPATH_BENCH.md](IOURING_DATAPATH_BENCH.md) baseline was
taken BEFORE the cross-shard hop coalescing (#674) and its lone-hop fast path
(#692) landed. That baseline named tokio's own weakness precisely: "tokio peaks at
depth 8 then DECLINES (1.79M to 1.46M to 1.09M), the cross-shard-hop machinery
saturating under deep pipelining." #674 collapses exactly that machinery (a run of
same-shard cross-shard hops becomes one batched message). This re-bench measures
what that did. Raw log:
[coalescing-rebench-2026-07-17.raw.txt](coalescing-rebench-2026-07-17.raw.txt).

## Setup

Identical to the baseline so the numbers are directly comparable: one c7g.4xlarge
(16-core Graviton3, arm64, AL2023), server pinned to cores 0-7 (8 shards,
thread-per-core), the repo `loadgen` pinned to the disjoint cores 8-15 over
loopback, SINGLE endpoint (client random keys hop to the owning shard, IronCache's
worst routing). 128B values, 1,000,000-key zipf(0.99), 90% GET / 10% SET, 128
connections, a 5s warmup then a 10s measured pass, pipeline depth swept, plaintext.
Mean of 2 reps. Three backends from the same commit (post #674 + #692 + the raw
migration): default tokio, `--features io_uring --runtime io_uring` (the old
backend), and `--features io_uring_raw --runtime io_uring_raw` (the new raw backend
with multishot recv #513). Each ring backend CONFIRMED its datapath at boot (no
silent tokio fallback).

## Headline: #674 roughly doubled the DEFAULT backend at deep pipeline

Single-endpoint peak qps, 90% GET, tokio, this run vs the pre-#674 baseline:

| pipeline | tokio (pre-#674) | tokio (now) | change |
| ---: | ---: | ---: | ---: |
| 1  | 262,211 | 259,913 | -0.9% (tie) |
| 8  | 1,785,296 | 1,903,620 | +6.6% |
| 16 | 1,461,484 | **2,811,757** | **+92.4%** |
| 32 | 1,094,650 | **2,473,897** | **+126.0%** |

The cliff is gone. tokio no longer collapses past depth 8: it climbs to 2.81M at
depth 16 (was 1.46M) and holds 2.47M at depth 32 (was 1.09M). This is the SHIPPED
default datapath, and it is the single-endpoint deep-pipeline column that was
IronCache's weakest against Dragonfly. #674 (plus the #692 lone-hop fast path)
closed most of that gap without touching the client or the datapath.

## The full matrix (90% GET, mean qps)

| pipeline | tokio | io_uring (old) | io_uring_raw (new) |
| ---: | ---: | ---: | ---: |
| 1  | 259,913 | 240,007 | 239,867 |
| 8  | 1,903,620 | 1,793,848 | 1,722,309 |
| 16 | **2,811,757** | 2,636,207 | 2,478,708 |
| 32 | 2,473,897 | **3,379,260** | 3,311,976 |

## Read

1. **The crossover moved.** In the baseline io_uring beat tokio from depth 16 up
   (+78% at 16). Now that #674 fixed tokio's mid-depth cliff, tokio WINS at depth 8
   and 16, and io_uring only pulls ahead at depth 32 (where tokio finally eases off:
   3.38M vs 2.47M, io_uring +37%). io_uring's remaining single-endpoint niche is
   narrower and lives at very deep pipelines (32+). This materially changes the
   io_uring ship-by-default calculus: with coalescing, the default is competitive
   through depth 16 on its own.
2. **The raw migration held the io_uring win.** io_uring_raw tracks the old io_uring
   backend within about 2 to 6% across the sweep (3.31M vs 3.38M at depth 32) and
   still climbs monotonically. The raw backend is not a throughput regression versus
   the backend it replaced, and it is what unlocks musl + multishot.
3. **Multishot (#513) is not a throughput lever at this config.** At 128 connections
   the workload is throughput-bound, not syscall-bound per connection, so multishot
   recv neither helps nor meaningfully hurts here (raw sits a few percent under the
   old fixed-buffer path). Its value is latency and deep single-connection regimes,
   not this peak-qps sweep. No throughput claim is made for it.
4. **pipe-1 is unchanged and honest.** Both ring backends are about 8% under tokio at
   depth 1 (240k vs 260k), same as the baseline: the submission/completion machinery
   does not amortize at one op per round-trip. This is the regime the perf-gate must
   hold never-worse for any ship-by-default flip, and it is the least representative
   of a high-throughput workload. It is also why the #692 lone-hop fast path matters:
   it keeps the default tokio cross-shard path lean exactly here.

## SET-heavy columns (#674's cross-shard SET squash)

#674 coalesces a run of same-shard cross-shard SETs, so a SET-heavy deep pipeline is
where its squash shows. tokio, by op-mix:

| pipeline | 90% GET | 50/50 | SET-only |
| ---: | ---: | ---: | ---: |
| 16 | 2,811,757 | 2,669,060 | 2,768,503 |
| 32 | 2,473,897 | 2,324,364 | 2,116,457 |

At depth 16 the SET-only column holds within 2% of the GET column (2.77M vs 2.81M):
the squash keeps a SET-heavy cross-shard deep pipeline from cliffing the way the
pre-#674 hop machinery would have. At depth 32 SET-only eases to 2.12M (tokio's
residual deep-pipeline ceiling), still roughly double the pre-#674 GET number at the
same depth.

## Bottom line

The worry that this campaign might have made things worse is answered by the default
column: cross-shard coalescing roughly DOUBLED tokio's single-endpoint deep-pipeline
throughput (+92% at depth 16, +126% at depth 32) and removed the cliff the baseline
had flagged as the weakest spot versus Dragonfly. The raw io_uring migration held its
predecessor's win, and the one honest cost (pipe-1, about 8% on the non-default ring
backends) is unchanged and off the default path.
