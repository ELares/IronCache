<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# IronCache benchmark run script

> Where IronCache stands today, and the prioritized optimization scope, are in
> [docs/bench/FINDINGS.md](../../docs/bench/FINDINGS.md) (A6). Headline: memory is
> the real gap (~2.1x heavier per key than redis in an indicative run, driven by a
> fat per-slot value + hash-table slack); throughput needs a pinned-Linux run vs
> valkey to judge.

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
| `PIPELINE` | `1` | Closed-loop RESP pipeline depth (commands sent per write). `1` = one op per round-trip. Closed-loop pass ONLY. |
| `RATE` | `50000` | Open-loop target ops/sec. |
| `WARMUP_SECS` | `3` | Write-only warmup duration. |
| `PORT` | `6399` | RESP port (loopback only). |
| `MAXMEMORY` | `1gb` | Server memory ceiling, via the `IRONCACHE_MAXMEMORY` overlay. |
| `SERVER_CORES` / `CLIENT_CORES` | half/half | taskset core lists (Linux only). |
| `PERSIST_CORE` | unset | headtohead/tail only: dedicate this core to IronCache's persist thread (#589, `IRONCACHE_PERSIST_CPU`); keep it OUTSIDE `SERVER_CORES`. |
| `SMOKE` | `0` | `1` shrinks every dimension for a few-second CI run. |

### A note on concurrency vs pipeline depth

Concurrency is expressed two ways: fan-out across `--connections`, and, within each
connection, RESP pipeline depth via `PIPELINE` (the loadgen's `--pipeline`). At the
default `PIPELINE=1` a connection sends one op per round-trip; at `PIPELINE=N` it sends
N commands in ONE write and reads N replies, amortizing the per-op syscall/round-trip.
A non-pipelined loadgen is syscall-bound at one op per round-trip, so pipelining is the
prerequisite for BENCHMARK.md's pipeline-depth sweep and for measuring where
batching/io_uring lifts throughput. Pipelining applies to the CLOSED-loop peak-QPS pass
only; the open-loop latency pass stays at depth 1 to keep its coordinated-omission-free
timing. The manifest records the `pipeline_depth` actually used.

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

## Head-to-head (A4)

`scripts/bench/headtohead.sh` is the A4 head-to-head (BENCHMARK.md #8 / #96, the
ADR-0017 bar). Where `run.sh` measures IronCache alone, the head-to-head boots
IronCache and a PINNED competitor (Valkey 9.1.0) one at a time, on the SAME box, under
identical knobs, and emits ONE comparison report of the two headline metrics plus a
PASS/FAIL verdict.

### Usage

```sh
# Full run vs the pinned valkey-server (CI installs it; the published bar):
COMPETITOR_BIN=$(command -v valkey-server) scripts/bench/headtohead.sh

# Fast tiny run for CI / local validation (a few seconds):
SMOKE=1 scripts/bench/headtohead.sh --out-dir /tmp/h2h-smoke
#   (or: scripts/bench/headtohead.sh --smoke)

# Local smoke with redis-server as a STAND-IN competitor (valkey-server not installed):
SMOKE=1 COMPETITOR_BIN=$(command -v redis-server) scripts/bench/headtohead.sh --out-dir /tmp/h2h-smoke
```

The default output directory is `bench-results/headtohead-<ver>-<os>-<arch>`
(gitignored). Every knob from the LOCKED-knobs table above is overridable by the same
environment variable; the head-to-head adds `KEYCOUNT` (the exact number of distinct
keys inserted for the bytes-per-key measurement, default `1000000`).

### Competitor-binary resolution

The competitor is resolved in order: the `COMPETITOR_BIN` environment variable, else
`valkey-server` on `PATH`, else `redis-server` on `PATH`. The script captures the
competitor's `--version` for the report. `redis-server` is RESP/Valkey-wire-compatible
and is fine for a local smoke, but the PUBLISHED bar is the pinned `valkey-server`
`9.1.0` (`docs/bench/COMPETITORS.md`): when the competitor is a `redis-server`, or a
`valkey-server` whose version is not the pinned one, the script prints a loud WARNING
and marks the verdict INDICATIVE (`indicative_only: true` in the JSON). If no
competitor binary is found at all, the script exits with a clear message.

### Pinning

Same methodology as `run.sh`, with one difference: because the two servers are compared
on EQUAL footing, BOTH are pinned to the SAME server core set (so each is measured on
identical cores), and the loadgen client is pinned to a DISJOINT set. `SERVER_CORES` /
`CLIENT_CORES` override the defaults (half/half). The server CORE COUNT (counted from
the pinned set) is both the per-core denominator AND the value passed to IronCache as
`--shards` and to the competitor as `--io-threads`, so each server runs on exactly the
cores it is measured on. Without `taskset` (e.g. macOS) the run is UNPINNED and
indicative, and the denominator falls back to the host CPU count.

### The two metrics (measured the SAME way on both)

- **bytes-per-key** (apples-to-apples memory): read `INFO memory` `used_memory` on the
  empty server, deterministically populate EXACTLY `KEYCOUNT` distinct keys
  (`key:0`..`key:N-1`) each with a fixed `VALUE_SIZE`-byte value via `redis-cli --pipe`,
  re-read `used_memory`; `bytes_per_key = (used_after - used_before) / N`. The loadgen
  is deliberately NOT used for the populate: its zipf SETs do not cover the keyspace
  uniformly, so they would not land N distinct keys. `redis-cli --pipe` works against
  IronCache too (it supports `ECHO` for the pipe sentinel). IronCache reports
  `used_memory` from jemalloc `stats.allocated`, exactly as Redis/Valkey do, so the
  delta is measured identically on both.
- **QPS-per-core** (throughput): a write-only warmup populates the hot keyset, then
  `loadgen --mode closed` (pinned to the client cores) runs the shared workload
  (`SEED`/`KEYSPACE`/`THETA`/`READ_RATIO`/`VALUE_SIZE`/`DURATION_SECS`/`CONNECTIONS`)
  against the server; `qps_per_core = qps / server_core_count`. An optional
  `loadgen --mode open` pass records the OVERALL op-latency tail
  (p50/p99/**p99.9**/p99.99) per server. The percentiles are whole-op-mix (the loadgen
  records GET and SET into one histogram; there is no GET-vs-SET split).

### Adversarial tail: EVICT and SNAPSHOT (the #518 moat, #574)

Two env knobs turn the head-to-head into the moat proof (see
[`docs/bench/TAIL_LATENCY.md`](../../docs/bench/TAIL_LATENCY.md)):

- **`EVICT=1`** boots every server in its evicting cache mode under a LOW `MAXMEMORY`
  (below the dataset) so eviction fires continuously during the pass.
- **`SNAPSHOT=1`** fires a background `BGSAVE` on the server under test every
  `SNAPSHOT_INTERVAL_SECS` (default 3) DURING the open-loop latency pass, so the measured
  p99.9/p99.99 CAPTURES the concurrent durable-save tail. Each server boots with a fresh,
  private snapshot dir (empty on boot so nothing is loaded, removed after) and a real save
  enabled (IronCache `IRONCACHE_DATA_DIR`; redis/valkey/keydb `--dir` with `BGSAVE` still
  honored under `--save ''`; Dragonfly a real `--dbfilename`). The QPS pass runs BEFORE the
  loop, so peak QPS is unchanged; only the latency tail reflects the save. The script PROVES
  a save actually executed (LASTSAVE advanced and/or a save line in the server log) and
  prints a `SNAPSHOT CONFIRMED FIRED` line; each fire + reply is logged to
  `<name>-bgsave.log`.

`SNAPSHOT=1 EVICT=1 scripts/bench/headtohead.sh` runs the full mix (mixed ratio + zipf
skew + eviction + concurrent snapshot). `scripts/bench/tail.sh` is a thin wrapper that
presets it. `SMOKE=1` shrinks every dimension and still fires at least one BGSAVE.

### The ADR-0017 bar (verdict)

IronCache PASSES when, on the same box under the same knobs:

1. its **qps_per_core EXCEEDS** the competitor's, AND
2. its **bytes_per_key is BELOW** the competitor's.

The script prints a readable table, the ic/competitor ratios, and a PASS/FAIL on each
metric plus OVERALL. It clearly notes when the competitor was a `redis-server`
stand-in (or a non-pinned valkey), so a stand-in verdict is read as INDICATIVE until
re-run against the pinned `valkey-server` `9.1.0`.

### What it produces

In the output directory:

| File | Contents |
| --- | --- |
| `headtohead.json` | The comparison: both servers' `{name, version, qps, qps_per_core, bytes_per_key, p50_us, p99_us, p999_us, p9999_us}`, the ratios (incl. the p99.9 `p999` moat ratio), the verdict, and a manifest of knobs (incl. `eviction`, `snapshot`, `snapshot_interval_secs`) / host / pinning / competitor resolution. |
| `ironcache-closed.json`, `<competitor>-closed.json` | Per-server closed-loop peak-QPS results. |
| `ironcache-open.json`, `<competitor>-open.json` | Per-server open-loop latency results (`p50/p99/p999/p9999_us`). |
| `ironcache-open.hgrm`, `<competitor>-open.hgrm` | Per-server HdrHistogram percentile dumps. |
| `ironcache-server.log`, `<competitor>-server.log` | Each server's stdout/stderr for the run. |
| `<name>-bgsave.log` (SNAPSHOT mode) | Each background `BGSAVE` fire's UTC timestamp + redis-cli reply, the audit trail behind the `SNAPSHOT CONFIRMED FIRED` line. |

ONE server runs at a time on the same port. A pre-launch `/dev/tcp` port-free check
rejects a stale listener (which `SO_REUSEPORT` would otherwise hide), and each server is
killed by PID and its port verified free before the next boots, so no orphan survives.
IronCache does not implement `SHUTDOWN`; servers are stopped by signaling their PID.

## Perf-regression gate (A5)

`perf_measure.sh` + `perf_compare.sh` + `.github/workflows/perf-gate.yml` are the per-PR
performance-regression gate (PERF_REGRESSION_GATE.md #159). The gate FAILS a pull request
that regresses the ADR-0016 headline metrics past budget, comparing HEAD against the PR's
merge-base.

### What it measures

The two HEADLINE metrics, both the *smaller* halves of ADR-0016, ratcheted per PR:

- **`bytes_per_key`** (per encoding class `int` / `embstr` / `raw`): the allocator-true
  `total_bytes_per_key` from the A1 `memmodel` binary. It is DETERMINISTIC (no server, no
  clock, no network), so the budget is TIGHT. Ratchet direction: it may NOT **rise** past
  budget.
- **`qps`** (peak, the per-core throughput proxy): a SHORT closed-loop `loadgen` point.
  It is NOISY on shared CI, so `perf_measure.sh` runs `QPS_REPS` reps (default 5, short
  duration), takes the **median**, and records the min/max so the compare step can derive
  a noise band. The budget is GENEROUS. Ratchet direction: it may NOT **fall** past budget.

Open-loop p99 tails and the criterion micro-benches are **reported, not failed** (tail
noise on shared CI is high; the doc tracks tails as a trend, not a per-PR hard gate). To
keep the per-PR macro point short they are not measured by `perf_measure.sh` at all; the
hard ratchet is `bytes_per_key` + `qps` only.

### The ratchet (budgets and bands)

`perf_compare.sh` computes, per metric, a signed delta (head vs base), a noise band, and a
verdict:

- **PASS** - the delta is inside the noise band (within-noise; not a real move).
- **WARN** - the delta is outside the band but inside the budget (a real move, tolerated;
  does NOT fail the PR).
- **FAIL** - the delta is outside the budget in the bad direction (qps fell beyond the
  budget, or a `bytes_per_key` class rose beyond the budget).

The compare step exits non-zero iff any metric FAILed; WARN and PASS exit 0. The workflow
makes the check red iff the compare exits non-zero.

Budgets are env-overridable (defaults in parentheses):

| Env | Default | Meaning |
| --- | --- | --- |
| `QPS_DROP_BUDGET` | `0.15` (15%) | Max tolerated qps **drop**. Generous (qps is noisy). |
| `BYTES_RISE_BUDGET` | `0.05` (5%) | Max tolerated `bytes_per_key` **rise**. Tight (deterministic). |
| `QPS_BAND_FLOOR` | `0.05` (5%) | Minimum qps noise band; the band is `max(base reps spread, floor)` so a single-rep base still has a sane within-noise tolerance. |

The qps band is derived from the base reps' `(max - min) / median` spread (floored), so a
genuinely flat-but-jittery run reads as PASS rather than WARN. `bytes_per_key` is
deterministic, so it gets only a tiny 0.5% within-noise band to absorb memmodel float
rounding; any real rise is at least a WARN.

### Same-runner, rebuild-the-merge-base

The comparison is SAME-RUNNER, SAME-TOOLCHAIN: the workflow measures BOTH the merge-base
AND HEAD in the same job on the same runner. It computes
`MERGE_BASE = git merge-base <PR base sha> HEAD`, creates a detached **git worktree** at
that commit (so the HEAD checkout is undisturbed), runs that worktree's own copy of
`perf_measure.sh` to produce `base.json`, then runs `perf_measure.sh` on the HEAD checkout
to produce `head.json`, and finally runs `perf_compare.sh`. Building release TWICE and
booting a server each time is the EXPECTED cost of a same-runner gate; the macro point is
kept short to fit a per-PR budget. (A committed-baseline fast path, skipping the
merge-base rebuild, is a documented FUTURE optimization; this implements the
rebuild-in-job path.)

The build is a NATIVE `cargo build --release` (we measure the runner's native arch), NOT
the cross-compiled `cargo-zigbuild` used by the release workflows, so the job needs only
the stable toolchain.

### How an intentional perf trade is landed

CI never auto-commits anything. Because the gate rebuilds the merge-base every run, there
is no stored baseline file to bump. An intentional regression (e.g. a feature that costs a
few percent qps, or an encoding change that adds bytes) is acknowledged by **raising the
relevant budget in the SAME PR with a documented reason** - bump `QPS_DROP_BUDGET` /
`BYTES_RISE_BUDGET` (in the workflow `env:` or as a repo/branch variable) and state why in
the PR description. The gate then reads the new budget and the trade lands as a PASS/WARN
rather than a FAIL.

### Running it locally

```sh
# Measure the current tree twice (two runs of the same code stay within band/budget):
SMOKE=1 bash scripts/bench/perf_measure.sh --out /tmp/base.json
SMOKE=1 bash scripts/bench/perf_measure.sh --out /tmp/head.json
bash scripts/bench/perf_compare.sh --base /tmp/base.json --head /tmp/head.json   # PASS, exit 0
```

`SMOKE=1` (or `--smoke`) shrinks to a single 1s rep over a tiny keyspace for fast local
validation; it is not a publishable measurement. Every knob (`QPS_REPS`, `DURATION_SECS`,
`KEYSPACE`, `CONNECTIONS`, `READ_RATIO`, `VALUE_SIZE`, `THETA`, `SEED`, `WARMUP_SECS`,
`PORT`, `MAXMEMORY`, `SHARDS`) is env-overridable. The same server-lifecycle discipline as
`run.sh` applies: a pre-launch `/dev/tcp` port-free check, a PID trap-kill on
EXIT/INT/TERM, and a `redis-cli PING` readiness probe, so no orphan server survives a run.

The PR workflow has a `[skip perf]` escape hatch (put it in the PR title) and a path
filter so docs-only PRs skip the gate; it runs only when `crates/**`, the `Cargo.*` /
`rust-toolchain.toml` manifests, `scripts/bench/**`, or the workflow itself change.
