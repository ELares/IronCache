<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# The p99.9 tail under durable load: methodology (#574, #518)

This is the methodology for the tail-latency head-to-head: how IronCache's
`scripts/bench/headtohead.sh` measures the **p99.9 (and p99.99) latency under a
concurrent durable save**, and how to reproduce it on real hardware. NOTE: this
metric was originally framed as IronCache's "moat"; the harness itself REFUTED that
(see the measured record below) -- IronCache's durable-save tail is COMPETITIVE
(sub-second after #588), not category-leading. The doc keeps the harness; the honest
result is stated plainly.

It is the committed, reproducible HARNESS + methodology. It claims **no numbers**:
the cross-competitor figures are produced by running this harness on a pinned
Linux box (a Graviton `c7g` in the plan), an owner-gated spend. What is validated
here is that the harness runs, fires the adversarial load, and reports the tail.

## TL;DR

```sh
# The full adversarial mix (mixed ratio + zipf skew + eviction + concurrent snapshot):
SNAPSHOT=1 EVICT=1 scripts/bench/headtohead.sh
# ...or the thin preset wrapper:
scripts/bench/tail.sh
# Fast local/CI self-test (seconds; still fires >=1 BGSAVE):
SMOKE=1 scripts/bench/tail.sh
```

Each server reports **p50 / p99 / p99.9 / p99.99** OVERALL open-loop op latency.
The p99.9 UNDER A CONCURRENT SNAPSHOT is the metric to win.

## Why the p99.9 under durable load (and not median GET)

On a plain uncontended GET, IronCache roughly TIES a tuned Redis/Valkey and, under a
proper thread-per-core config, now LEADS a thread-per-core Dragonfly (the corrected c7g
re-bench, 2026-07-10: about +19% single-endpoint and roughly 2x cluster-aware via #517
zero-hop; the earlier apparent GET deficit was a benchmark CONFIG artifact, shards
oversubscribed relative to cores -- see README.md and `docs/research/dragonfly.md`). Even
so, median GET is the axis where the incumbents are already strong, so it is not the most
architecturally differentiating one to lead on.

The axis where the ARCHITECTURE diverges is the **tail under real production
pressure**: a hot-key-skewed mixed workload, memory pressure forcing continuous
eviction, AND a periodic durable snapshot running concurrently. That is the
day-2-operations reality of a cache that is also a system of record's shield. And
it is where the incumbents' designs leak:

- **Redis / Valkey fork-COW stall.** `BGSAVE` `fork()`s; the child dumps the RDB
  while the parent keeps serving on copy-on-write pages. Under a write-heavy,
  skewed workload the parent faults and COW-copies hot pages during the save,
  and large-heap forks add page-table-copy latency.
- **Dragonfly snapshot-spike.** Dragonfly avoids `fork` with a versioned,
  shard-local point-in-time snapshot, but the serialization still competes with
  the serving fibers, so a save shows up as a throughput dip during the window.
- **IronCache forkless per-slot Arc-COW snapshot.** `#570` per-slot tables keep
  per-op work bounded (no rehash cliff); `#588` per-slot Arc copy-on-write hands
  a save a frozen point-in-time view read off-core by a dedicated persist thread,
  with no O(N) serving-side copy and no fork memory doubling.

**What this harness actually MEASURED (honest, c7g, do not oversell):** the
original thesis -- that IronCache WINS the durable-load tail -- was REFUTED by
this very harness. The first measurement showed a catastrophic concurrent-snapshot
p99.9 of ~3.5s (vs Dragonfly/Redis ~15ms), because the forkless save contended
with the datapath. That drove a real fix: the per-slot Arc-COW (#588) cut it 11.5x
to ~291ms. But ~291ms is still NOT parity: DURING a full-keyspace snapshot IronCache
trails Dragonfly/Redis (~15ms) by ~16-20x. The cause is FUNDAMENTAL, not a bug: the
persist thread reading the frozen keyspace shares MEMORY BANDWIDTH with the datapath,
and IronCache's multi-core datapath already consumes most of that bandwidth serving,
so a concurrent save has little headroom. Redis reaches ~15ms precisely because it
is single-threaded (one serving core leaves headroom for a fork child). Pinning the
persist thread and throttling the save were both MEASURED to make the tail WORSE (they
lengthen the bandwidth-contention window).

So the HONEST claim this harness supports is narrower and true: IronCache's baseline
p99.9 TIES Dragonfly (~15ms) and beats it on qps/core and memory, its durable-save
tail went from CATASTROPHIC (3.5s) to COMPETITIVE (sub-second, ~291ms), and it is
deterministic and tunable. It does NOT win the during-snapshot tail; closing that to
ms-class would require reducing the save's data footprint (incremental/compressed
snapshots), a large deferred lever. See CONFIG.md ("Dedicated persist core") and
issues #576/#588/#589 for the full measured record.

## The adversarial workload (the four dimensions)

All four run together; each maps to a knob (all overridable, so any dimension can
be ablated).

1. **Mixed op ratio.** `READ_RATIO` (default 0.9 = 90% GET / 10% SET). The 10%
   writes keep the keyspace dirty so a concurrent save has real work, and expose
   write-path tail under eviction/snapshot.
2. **Zipf hot-key skew.** `THETA` (default 0.99, the YCSB default). A small hot
   set concentrates traffic, which is what actually stresses per-slot contention,
   COW hot-page faults, and eviction churn.
3. **Concurrent eviction.** `EVICT=1` boots every server in its evicting cache
   mode under a LOW `MAXMEMORY` (below the dataset). Keys evict continuously
   during the pass. IronCache uses its default `allkeys-lru`; redis/valkey/keydb
   `--maxmemory <low> --maxmemory-policy allkeys-lru`; Dragonfly `--cache_mode=true`.
   An eviction-honesty guard requires a server to ACCEPT a write at the ceiling
   (evict-to-fit) rather than reply `-OOM` (which would post a dishonest inflated
   QPS).
4. **Concurrent snapshot.** `SNAPSHOT=1` fires a background `BGSAVE` on the server
   under test every `SNAPSHOT_INTERVAL_SECS` (default 3) DURING the open-loop
   latency pass, so the measured p99.9/p99.99 captures the durable-save tail. This
   is the #571 payoff.

## How SNAPSHOT works (and how it is verified)

- **Real saves, isolated per server.** Each server boots with a FRESH, PRIVATE,
  EMPTY snapshot dir (`mktemp -d` under the out dir, removed after the server
  stops and by the EXIT trap). Empty-on-boot means nothing stale is loaded, so the
  bytes-per-key baseline stays an honest empty server. The save is actually enabled:
  - IronCache: `IRONCACHE_DATA_DIR=<dir>` (the single enable switch for #58
    persistence; a `BGSAVE` then runs the forkless yielding cross-shard save).
    The PERIODIC save policy stays OFF (`save_interval_secs=0`), so the BGSAVE
    loop is the SOLE trigger and the measured window is clean.
  - redis / valkey / keydb: `--dir <dir>`. `BGSAVE` still writes an RDB there even
    under `--save ''` (that flag only disables the AUTOMATIC change-based snapshot).
  - Dragonfly: a real `--dbfilename dump --dir <dir>` (the non-snapshot run uses an
    empty `--dbfilename`).
- **Fires DURING the measured window.** A background loop starts JUST BEFORE the
  open-loop pass and is killed JUST AFTER it. It fires ONCE IMMEDIATELY (so even a
  sub-second SMOKE window captures at least one save) then every interval. The QPS
  pass has already finished, so peak QPS is unaffected; only the latency tail sees
  the save.
- **Verified, not assumed.** After the pass (server still up), the harness PROVES a
  save EXECUTED (not merely that `BGSAVE` was accepted) via two independent,
  time-robust signals: (1) `LASTSAVE` advanced beyond the pre-pass baseline (the
  open pass runs many wall-seconds after boot, so a completed save lands in a
  strictly later Unix second, even in SMOKE), and (2) a save-completion line in the
  server log. It prints `SNAPSHOT CONFIRMED FIRED: BGSAVE issued Nx ... LASTSAVE a
  -> b ... server-log save lines=k`, or a loud WARNING if neither signal appears.
  Each fire's timestamp + redis-cli reply is appended to `<name>-bgsave.log`.

## What is reported

The open-loop pass reports the OVERALL op latency tail. The loadgen records GET and
SET into ONE hdrhistogram (`crates/ironcache-bench/src/open_loop.rs`), so **there is
no GET-vs-SET percentile split** -- these are whole-op-mix percentiles. That is a
deliberate limitation of the loadgen, called out rather than faked.

- The readable table adds `p999_us (moat)` and `p9999_us` rows for each server, with
  an `ic/competitor` ratio for the tails (`<1` = IronCache's tail is TIGHTER).
- `headtohead.json` adds `p999_us` / `p9999_us` per server, a `p999`/`p9999` ratio,
  and `knobs.snapshot` / `knobs.snapshot_interval_secs` / `knobs.eviction`.

The tail is REPORTED, not a pass/fail gate: the ADR-0017 verdict stays qps-per-core
+ bytes-per-key. The p99.9 is the moat NARRATIVE the numbers back, published
alongside the verdict.

## Save-backpressure throttle: the stopgap that cuts the measured tail (#577)

The `c7g` run measured a concurrent-snapshot **p99.9 of ~3.6s**: a full-speed dump steals about half
the serving core, so once the offered load exceeds what the half-a-core datapath can drain the
open-loop queue **builds** for the whole save window. The `save-backpressure-percent` knob is the
cheap stopgap that cuts that tail (~3-4x) while the ms-class isolation fix is built.

- **What it does.** `CONFIG SET save-backpressure-percent <1-100>` (default **100 = no throttle**,
  byte-identical to today) makes the per-shard dump loop **sleep proportionally** after each chunk
  (`sleep = chunk_time * (100 - pct) / pct`), so the save consumes only about `pct`% of the core and
  the datapath stays **above** the offered load -- the queue drains instead of building. See
  `docs/design/PERSISTENCE.md` for the mechanism (Env clock elapsed, Runtime timer sleep, ADR-0003).

- **The TRADEOFF you must respect.** Throttling **stretches the save's wall-time to about `1/pct`**:
  at `pct = 10` a ~2s dump becomes a ~20s wall-time save. That is fine at a **realistic 5-15 min save
  cadence** (a 20s save every 10 min is ~3% background, tail protected throughout) and **wrong at the
  bench's aggressive 3s cadence** (a 20s save never finishes before the next is due). **The rule is
  `save-cadence >> save-duration`.** For the bench itself, this means the throttle is only meaningful
  when `SNAPSHOT_INTERVAL_SECS` is set to a realistic operational cadence, not the stress 3-5s used to
  *guarantee* a save lands in a short measured window.

- **It is a STOPGAP, not the isolation fix.** The throttle reaches **hundreds-of-ms**, not the
  **ms-class** isolation of a decoupled save. The durable fix is the epoch-cut copy-on-write snapshot
  on a dedicated persist thread (#576 PR-B); the throttle buys the tail-cut cheaply until it lands.

## Reproducing on real hardware

The measured claim needs a pinned Linux box (disjoint server/client cores) against
the pinned competitor. The plan is a Graviton `c7g` (owner-gated spend; see the AWS
usage rules). The harness is otherwise identical to the standard head-to-head:

```sh
# On a c7g (Linux, taskset present so the harness pins server/client to disjoint cores):
COMPETITOR_BIN=$(command -v valkey-server) \
  SERVER_CORES=0-3 CLIENT_CORES=4-7 \
  KEYSPACE=5000000 KEYCOUNT=5000000 VALUE_SIZE=128 THETA=0.99 READ_RATIO=0.9 \
  MAXMEMORY=1gb DURATION_SECS=60 RATE=200000 SNAPSHOT_INTERVAL_SECS=5 \
  SNAPSHOT=1 EVICT=1 scripts/bench/headtohead.sh --out-dir bench-results/tail-c7g
```

Notes for a real run:

- Set `MAXMEMORY` BELOW the dataset so eviction fires, but for a Dragonfly
  head-to-head keep `MAXMEMORY >= 256MiB * threads` (its boot floor) AND a dataset
  above that, or Dragonfly refuses to boot (the harness surfaces the exact reason).
- Run each competitor from `docs/bench/COMPETITORS.md`; a `redis-server` stand-in is
  INDICATIVE only.
- `RATE` is the open-loop target ops/sec; keep it below the closed-loop peak so the
  tail reflects the server, not a generator-limited run (`saturated` in the open
  JSON flags a generator-limited pass).

## Validation status

Harness + methodology only, validated in SMOKE locally (IronCache vs a
`redis-server` stand-in). SMOKE confirms: the run completes, reports p99.9/p99.99
for both servers, and the concurrent BGSAVE fires and is confirmed during the
measured window. No cross-competitor numbers are claimed until the `c7g` run.
