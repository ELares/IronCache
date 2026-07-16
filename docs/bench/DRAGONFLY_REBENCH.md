# Dragonfly fair re-bench (2026-07-15)

Reproducible artifact for the "par-or-better vs Dragonfly" epic (#665). Raw log:
[dragonfly-rebench-2026-07-15.raw.txt](dragonfly-rebench-2026-07-15.raw.txt).

## Why this run
The published README 16-core headline pitted IronCache's WORST config (single
endpoint, ~93% of random keys hop cross-shard -- the README itself calls it "a
benchmark CONFIG artifact") against Dragonfly's native config, with
redis-benchmark 3-byte values, an UNPINNED `latest` Dragonfly, and the io_uring
build. This run removes every one of those unfairnesses.

## Setup (matched, pinned, reproducible)
- One c7g.4xlarge (16-core Graviton3, arm64, AL2023). SERVER pinned to cores 0-7
  (8 shards / 8 proactor threads), LOAD GENERATOR pinned to the disjoint cores
  8-15. Both engines measured on the SAME box in sequence.
- Dragonfly PINNED to v1.39.0 (digest
  `sha256:0fa01a2b929e704c7a9300d23e7f52002ebd39e90996fb8bb63826aed92fa06f`).
- Tool: `memtier_benchmark` (the cross-engine standard), 8 threads x 16 conns =
  128 connections, 1,000,000-key space, `--random-data`.
- Value sizes 128B AND 256B; pipeline depth swept 1 / 16 / 32 / 64.
- IronCache reported for BOTH the shipped tokio binary AND the io_uring build
  (io_uring is NOT in the published release artifacts -- it needs a from-source
  `--features io_uring` build + `--runtime io_uring`).
- Persistence off, maxmemory off. Reproduced twice; values are the mean.

## Single-endpoint throughput (Mops/sec, GET / SET)

### 128B values
| pipe | IC-tokio (shipped) | IC-io_uring | Dragonfly v1.39.0 |
| ---: | ---: | ---: | ---: |
| 1  | 0.89 / 0.87 | 0.88 / 0.86 | **0.96 / 0.95** |
| 16 | 2.23 / 2.16 | **2.81** / 2.60 | 2.50 / **2.54** |
| 32 | 1.85 / 1.32 | **2.65** / 2.83 | 2.62 / **3.23** |
| 64 | 1.26 / 1.01 | 2.98 / 2.95 | **3.34 / 3.83** |

### 256B values
| pipe | IC-tokio (shipped) | IC-io_uring | Dragonfly v1.39.0 |
| ---: | ---: | ---: | ---: |
| 1  | 0.89 / 0.88 | 0.87 / 0.85 | **0.96 / 0.91** |
| 16 | 2.13 / 2.24 | **2.14** / 2.61 | 1.09 / 2.38 |
| 32 | 1.74 / 1.34 | **2.60** / 2.83 | 1.14 / **3.01** |
| 64 | 1.25 / 1.27 | **2.92** / 2.76 | 1.13 / **2.99** |

## Memory (bytes/key, exactly-N distinct keys, 128B values, same box + method)
| keys | IronCache (Dash default) | Dragonfly v1.39.0 |
| ---: | ---: | ---: |
| 700k | 173.42 | **156.95** |
| 900k | **163.17** | 182.91 |
| 1M   | **165.21** | 177.03 |

## Tail (memtier pipeline 1, ~500k ops/s, 90/10 GET/SET, 128B)
p50 / p99 / p99.9 (ms): IronCache 0.183 / 0.295 / 0.319; Dragonfly 0.183 /
0.295 / 0.319. Identical (parity) at this load.

## Honest read
1. **The fair methodology substantially improves the standing.** Against the
   published "IC loses GET 3.97M vs 4.92M and SET 3.31M vs 4.95M", the matched
   run shows IronCache (io_uring) PAR-OR-AHEAD on 128B GET through pipeline 32
   (wins pipe 16, ties pipe 32), WINNING 256B GET at every pipeline >= 16, and
   winning the headline 1M-key memory point.
2. **The shipped tokio binary CLIFFS at deep pipeline** (1.26M GET / 1.01M SET
   at pipe 64) -- the cross-shard-hop machinery. This is the real published-
   standing gap on what users actually run, and it is exactly what cluster-aware
   (zero-hop) routing or per-connection pipeline squashing removes.
3. **Dragonfly's real residual win is deep-pipeline SET** (3.83M vs 2.95M at
   128B pipe 64) -- its MultiCommandSquasher amortizes a 100%-write pipeline into
   one hop per shard, where IronCache pays one hop per command. This is the
   single mechanism worth mimicking.
4. **Memory: IronCache now wins the 1M headline point** (165.21 vs 177.03, the
   old 180.27 erased by the #285 Dash flip) with a FLATTER curve (163-173 vs DF
   157-183), but Dragonfly wins at 700k -- so NOT yet uniformly better. The
   bucketed-Dash + in-segment displacement work (#669) targets the uniform win.
5. **Dragonfly's 256B GET collapses** to ~1.1M at pipeline >= 16 (vs its 3.3M at
   128B, and vs IronCache's 2.1-2.9M at 256B). Striking and reproduced across
   pipelines under an identical harness; flagged for a focused confirm before it
   is leaned on.
6. **Tail is a genuine tie** at this load.

## What this run does NOT yet cover (see the epic)
- Cluster-aware / zero-hop table, both engines owner-routed (#667) -- the leg
  that tests whether the shipped tokio binary WINS once the hop is removed.
- The during-snapshot p99.9 tail (a known limitation at a bandwidth floor).
