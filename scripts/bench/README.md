<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# IronCache benchmark run script

`scripts/bench/run.sh` is the one scripted invocation that reproduces a published
benchmark run end to end (BENCHMARK.md #8, PR-A3 of the performance track). It builds
the release binaries, boots a real IronCache server, warms the hot keyset, runs three
measured passes against it, and writes machine-readable artifacts plus a manifest.

## Usage

```sh
# Full standard run (slow; the first release build dominates):
scripts/bench/run.sh

# Pick the output directory:
scripts/bench/run.sh --out-dir /tmp/my-run

# Fast tiny run for CI / local validation (a few seconds):
SMOKE=1 scripts/bench/run.sh --out-dir /tmp/bench-smoke
#   (or: scripts/bench/run.sh --smoke)
```

The default output directory is `bench-results/<ver>-<os>-<arch>` (gitignored), where
`<ver>` is parsed from `ironcache --version`, `<os>` is `uname -s`, and `<arch>` is
`uname -m`.

## What it produces

In the output directory:

| File | Contents |
| --- | --- |
| `memory.json` | A1 allocator-true memory model: per-encoding `object_bytes_per_key` / `table_bytes_per_key` / `total_bytes_per_key`, plus the raw `alloc_*` samples. |
| `closed.json` | Closed-loop peak throughput (`qps`) under the locked op-mix. |
| `open.json` | Open-loop latency tail (`p50_us`/`p99_us`/`p999_us`/`p9999_us`), the `achieved_rate`, and the `saturated` flag. |
| `open.hgrm` | The HdrHistogram percentile dump for the open-loop pass (the artifact milestones diff against). |
| `manifest.json` | Every run knob, host facts (`uname -a`, cpu count, whether pinned and the core sets), the ironcache version, a UTC timestamp, and a pointer to the committed competitor matrix. |
| `server.log` | The server's stdout/stderr for the run (boot banner, any warnings). |

## Pinning methodology (the reproducibility core)

BENCHMARK.md borrows Valkey's bare-metal methodology: pin every variable.

- **Disjoint server/client cores.** On Linux (where `taskset` exists) the script pins
  the SERVER to one core set and the loadgen CLIENT to a DISJOINT core set, so the two
  never steal each other's cycles. The defaults split the box in half (server = first
  half of cores, client = second half); override with `SERVER_CORES` / `CLIENT_CORES`
  (taskset core-list syntax, e.g. `0-3`, `0,2,4`).
- **Loopback.** The client always talks to `127.0.0.1`, isolating the network from the
  measurement.
- **No taskset (e.g. macOS).** The script prints a loud WARNING that the run is
  unpinned and INDICATIVE only, then runs anyway. It never fails just because taskset is
  missing. Treat such results as a smoke check, not a published number.

## Warmup and the 90% hit ratio

Before the measured read-heavy pass the script POPULATES the hot keyset with a
write-only warmup (a closed-loop loadgen pass at `--read-ratio 0`, all SETs, over the
keyspace for `WARMUP_SECS`). The measured reads draw from a zipf distribution that
CONCENTRATES on a small set of hot keys, so once the warmup has written across the
keyspace those hot keys (which dominate the measured GETs) are present, and the locked
90%-read pass sees a high hit ratio. This is why a write-only warmup is sufficient to
hit the ~90%-hit-ratio target.

## Open vs closed passes (never conflated)

The two latency stories are measured separately, on purpose (BENCHMARK.md "the two are
never conflated"):

- **Closed-loop** (`--mode closed`): N connections loop request->reply at full tilt;
  reports PEAK QPS. Good for throughput, useless for the tail (coordinated omission).
- **Open-loop** (`--mode open`): a wrk2-style constant-rate pass that measures each
  request's latency from its INTENDED send time, free of coordinated omission; reports
  p50/p99/p999/p9999. If the generator can't keep up, `saturated` is `true` and the
  tail reflects the generator, not the server.

## Locked knobs (the standard run)

Each is overridable by an environment variable; the defaults are what a published
number is measured at.

| Env var | Default | Meaning |
| --- | --- | --- |
| `SEED` | `6342047879154770157` | Workload RNG seed (fixed = byte-reproducible workload). |
| `KEYSPACE` | `1000000` | Distinct keys. |
| `THETA` | `0.99` | Zipf exponent (YCSB-default skew). Uniform keys are rejected as unlike cache reality. |
| `READ_RATIO` | `0.9` | 90% GET / 10% SET; the locked hit-ratio target. |
| `VALUE_SIZE` | `128` | SET value bytes. |
| `DURATION_SECS` | `10` | Measured-pass duration. |
| `CONNECTIONS` | `50` | Load fan-out (closed) / dispatch pool (open). |
| `RATE` | `50000` | Open-loop target ops/sec. |
| `WARMUP_SECS` | `3` | Write-only warmup duration. |
| `PORT` | `6399` | RESP port (loopback only). |
| `MAXMEMORY` | `1gb` | Server memory ceiling, via the `IRONCACHE_MAXMEMORY` overlay. |
| `SERVER_CORES` / `CLIENT_CORES` | half/half | taskset core lists (Linux only). |
| `SMOKE` | `0` | `1` shrinks every dimension for a few-second CI run. |

### A note on concurrency vs pipeline depth

Within-connection pipeline depth > 1 is a DEFERRED loadgen feature, so BENCHMARK.md's
pipeline-depth sweep is only partially covered. Until it lands, "concurrency" is
expressed purely via `--connections` (the manifest records `pipeline_depth: 1`).

### A note on `--maxmemory`

The `ironcache` binary has NO `--maxmemory` CLI flag. The memory ceiling is a config
key, so the script passes it through the `IRONCACHE_MAXMEMORY` environment overlay
(human sizes like `512mb`/`1gb` are accepted). `--port` and `--shards` ARE global CLI
flags and are passed directly.

## The competitor matrix and A4

The pinned baselines IronCache's numbers are compared against live in
[`docs/bench/COMPETITORS.md`](../../docs/bench/COMPETITORS.md): Valkey (the primary
oracle and head-to-head bar), Redis 8.x (the kvobj header), and Dragonfly (dashtable
per-entry overhead), with their pinned versions and the memory-overhead facts the A1
memory model is compared against. That matrix is committed and dated; bumps require an
explicit PR. A4 (#96) installs the pinned `valkey-server` version named there and runs
IronCache and Valkey side by side on identical hardware, emitting the headline
QPS-per-core and bytes-per-key metrics (ADR-0016).
