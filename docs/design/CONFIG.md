# Design: TOML config with CONFIG GET/SET/REWRITE parity and live reload

Issue: #85. Decisions: ADR-0020 (clap flags), ADR-0009 (compat). Related: #81
(binary), #86 (observability), #137 (admission knobs), #15 (CONFIG command).

## Goal and scope

Configuration must load from a file for reproducible deploys, be overridable at
the wire for compatibility with existing Redis tooling, and be reloadable without
a restart, an operational property Redis does not give (its CONFIG SET is not
persisted without CONFIG REWRITE, and there is no SIGHUP reload
[redis-config-set-rewrite-no-sighup]). This specifies the config system end to
end. (The #85 issue body carried an orchestrator "INPUT GAP" banner from the
old undefined-path bug; this design is grounded directly in the pinned claims.)

## Design

### Sources and precedence

The effective value of each key is resolved across ordered layers, highest
precedence first: runtime `CONFIG SET` > command-line flags (ADR-0020 root flags)
> environment variables > the TOML config file > built-in safe defaults
(ADR-0007 cache-mode posture). The layers are kept distinct rather than collapsed
into one mutable struct, so each source can change independently and the effective
value is always recomputed as the highest-precedence layer that set the key. A key
set by `CONFIG SET` wins until it is explicitly overwritten or cleared (a later
`CONFIG SET`, or a `CONFIG RESETSTAT`-style clear of the runtime layer); the
precedence is documented so an operator can reason about the effective value,
surfaced by `CONFIG GET`.

### Wire parity: CONFIG GET / SET / REWRITE

- `CONFIG GET <pattern>` returns the effective values with Redis-recognized
  parameter names (glob patterns supported), so existing tooling works
  (ADR-0009).
- `CONFIG SET` applies a runtime change to the live struct; whether it is durable
  depends on `CONFIG REWRITE`.
- `CONFIG REWRITE` writes the current effective config back to the TOML file,
  preserving comments and structure where possible. This matches Redis's
  set-then-rewrite-to-persist model [redis-config-set-rewrite-no-sighup].
- Parameters IronCache does not implement (or that are no-ops under its engine,
  for example `maxmemory-samples`, #50) are accepted and echoed for compatibility,
  documented as no-ops.

#### Deliberate divergence: `bind` and `port` are restart-required

`databases` and `io-threads` are `IMMUTABLE_CONFIG` in Redis and cannot change at
runtime in IronCache either, so reporting them restart-required matches Redis. By
contrast `bind` and `port` are `MODIFIABLE_CONFIG` in Redis (accepted at runtime,
where Redis re-binds the listening socket), but IronCache reports them
restart-required as a **deliberate divergence**: under the thread-per-core boot
model the listening sockets are bound once at startup and cannot be re-bound or
re-ported live, so a runtime `CONFIG SET bind`/`port` is rejected with the
restart-required error rather than silently accepted as a no-op (the
set-then-persist model Redis documents assumes the value actually takes effect
[redis-config-set-rewrite-no-sighup]). Re-bind-at-runtime is a possible future
capability; until then the restart-required reject is the faithful behavior.

### Live reload (beyond Redis)

Unlike Redis, IronCache supports reloading the config file without a restart: a
`config reload` admin action (and the `ironcache config` subcommand) re-reads the
file and refreshes **only the file layer**, then recomputes each effective value
across the layers above. A live `CONFIG SET` override therefore survives a reload
(it sits in a higher-precedence layer) and is not silently clobbered by the new
file contents; an operator who wants the file value back clears the runtime layer
explicitly. Reload applies to the parameters that are safely hot-swappable (memory
ceiling, eviction policy via #50, log level, slowlog/latency thresholds, admission
limits #137, codec policy #53). Parameters that cannot change at runtime (bind
address, the shard/core count) are reported as requiring a restart rather than
silently ignored. Reload is atomic per parameter and goes through the same
validation as startup.

### Container awareness

The default memory ceiling (ADR-0007) is derived from the cgroup limit when
present, not just host RAM, so a containerized IronCache sizes itself correctly.

### Durable data directory

`data_dir` (TOML key `data_dir`, env `IRONCACHE_DATA_DIR`, no CLI flag, defaulting
to unset) names the durable directory for on-disk state. Today its only consumer is
the raft-mode control plane, which writes its committed Raft log to
`<data_dir>/ironcache-raft-<bus-port>.log` (keyed by the bus port so co-located
nodes do not share a log) and creates the directory if missing. When `data_dir` is
unset the log lives under the OS temp directory, which is writable and ephemeral but
is NOT durable across a reboot that clears the temp dir, so a production raft node
should set it. An empty `data_dir` is rejected at boot (a likely operator mistake).

### Dedicated persist core

Issue: #589. Related: #588 (per-slot Arc-COW snapshot), ADR-0002 (thread-per-core), ADR-0003 (determinism), the tunability tenet.

`persist_cpu` (TOML key `persist_cpu`, env `IRONCACHE_PERSIST_CPU`, CLI `--persist-cpu`,
defaulting to unset) selects which CPU core(s) the off-datapath `ic-persist-<shard>`
thread pins to during a save. The per-slot Arc-COW snapshot (#588) moved a save's O(N)
encode+fsync off the serving core onto that dedicated thread, but the thread still runs
somewhere: under the thread-per-core model (ADR-0002) the datapath threads are confined
to a pinned cpuset, and at 16 shards on 8 pinned cores the persist thread is a 17th
runnable thread the scheduler otherwise places on one of those same serving cores.

WARNING (measured on c7g, 2026-07-09): pinning the persist thread to a single dedicated
core makes the concurrent-snapshot tail WORSE, not better. A/B at 1M keys: snapshot p99.9
was 291ms with the DEFAULT float behavior and 1,125ms (3.9x WORSE) pinned to one reserved
core. A single dedicated core encodes SLOWER than the thread floating opportunistically
across the datapath cores' spare cycles, so the save takes LONGER, and because the real
bottleneck is the persist thread SHARING MEMORY BANDWIDTH with the (memory-bound) datapath
while it reads the frozen keyspace, a longer save means a LONGER contention window and thus
a worse tail. The bandwidth contention is at the shared memory controller and cannot be
scheduled away by moving the thread. The DEFAULT (float) is best for the tail; this knob is
retained only for operators who want to isolate persist CPU from the datapath for OTHER
reasons (e.g. bounding datapath CPU jitter during a save) and accept the tail cost.

Accepted values:

- `off` (or an empty value / `none` / `disabled`): the DEFAULT. No pin; the persist
  thread floats exactly as it does today (byte-unchanged behavior).
- `auto`: reserve the HIGHEST core of the process's current affinity mask for
  persistence. Most useful when the datapath is confined (via `taskset`/cpuset) to the
  lower cores so the top core is genuinely free; otherwise it still parks the persist
  thread on one deterministic core instead of letting it float across every serving core.
- an explicit cpu list: a single id (`8`), a range (`6-7`), or a mix (`6-7,10`). The
  recommended deployment reserves a core OUTSIDE the datapath's `taskset` mask (see
  DEPLOY.md): pinning to that reserved id makes the persist thread escape onto it, which
  the kernel allows because `sched_setaffinity` is bounded by the process cpuset, not by
  the inherited `taskset` mask.

An explicit list is used as given (it is deliberately allowed to name a core outside the
current mask); a core the kernel ultimately rejects is logged once and the thread runs
unpinned rather than failing the save. A malformed value (a non-numeric id, an inverted
range) is rejected at boot.

Affinity is a SCHEDULING concern off the engine decision path: it changes only which core
a thread runs on, never a stored value, an ordering, or any output, so it stays clear of
the ADR-0003 determinism boundary (no clock or entropy is added). It is Linux-only
(`sched_setaffinity` is a Linux primitive); on macOS or any other target the knob is a
graceful no-op (a set value logs one warning and the thread runs unpinned). Per the
tunability tenet it is a knob with a SAFE default (the current float behavior), not a
baked-in choice, because dedicating a core trades a serving core (or spare core) for
persistence, which only pays off on hosts with a core to spare.

It is restart-required: the persist thread's affinity is set as the thread spawns from the
boot value, so `CONFIG GET persist-cpu` reports the effective value but `CONFIG SET
persist-cpu` returns restart-required (like `bind`/`port`/`shards`).

Why the snapshot tail is what it is: the per-slot Arc-COW (#588) cut the concurrent-snapshot
p99.9 from a catastrophic 3.5s to ~291ms (11.5x) by eliminating the O(N) serving-side copy.
The residual gap to millisecond-class snapshot tails (Redis/Dragonfly ~15ms) is a FUNDAMENTAL
memory-bandwidth-headroom tradeoff, not a scheduling bug: IronCache's multi-core datapath
already consumes most of the memory bandwidth serving, so a concurrent save's reads push
toward saturation. Redis reaches ~15ms because it is single-threaded (one serving core leaves
ample bandwidth for a fork() child to dump). Neither pinning (measured worse, above) nor
throttling the save (also measured worse: it stretches the contention window) helps; only
REDUCING the save's data footprint (incremental or compressed snapshots) could, which is a
large lever with diminishing returns against the shipped 11.5x and is deferred (#589 context).

### Transparent huge pages

Issue: #512. Related: ADR-0006 (allocator); the tunability tenet (env-dependent tradeoffs are config knobs with a safe default).

The `-r 1M` random-key benchmark (and real skewed workloads) thrash the TLB: each
GET touches a random hashbrown store-table bucket plus a random value blob, mostly
TLB misses with 4 KiB pages. Backing the allocator's extents with 2 MiB transparent
huge pages (THP) cuts the TLB-miss rate for the same coverage, an estimated 3 to 10
percent on the random-key hot path (measured on Linux, stacked with the other cheap
levers). Because the store tables and value blobs both flow through the global
allocator (jemalloc, ADR-0006), the cheapest effective mechanism is jemalloc's own
`thp:always` boot option (which madvises `MADV_HUGEPAGE` on its extents) rather than
per-allocation `madvise`, which hashbrown's global-allocator tables do not expose.

THP is a memory-layout HINT, not part of the engine decision path, so it stays clear
of the ADR-0003 determinism boundary (no clock or entropy is added). It is
Linux-only: jemalloc compiles THP support on Linux and nowhere else, so the option is
never emitted into `malloc_conf` on macOS or other targets (no "Invalid conf pair"
warning; the feature is simply inert there).

Per the tunability principle THP is a knob with a safe default, NOT a baked-in
choice, because it is a real tradeoff:

- Upside: fewer TLB misses on random access, and more predictable RSS granularity.
- Downside: `thp:always` can RAISE RSS (2 MiB allocation granularity rounds small
  extents up) and, on some kernels, khugepaged compaction adds occasional latency
  spikes. Because the `maxmemory` ceiling is enforced against the allocator RSS
  figure (ADR-0006), an inflated RSS is not free.

The default is therefore OFF (opt-in). Two knobs control it, both defaulting off:

- Build-time: the `hugepages` Cargo feature on the `ironcache` crate. Building with
  `--features hugepages` appends `thp:always,metadata_thp:auto` to the compile-time
  jemalloc `malloc_conf`, baking THP into that binary's default (Linux only).
- Runtime (no rebuild, works on ANY shipped binary): jemalloc's own environment
  override `_RJEM_MALLOC_CONF=thp:always` (or `thp:never` to force it off). jemalloc
  layers this on top of the compiled `malloc_conf`, overriding only the `thp` key, so
  the ADR-0006 `background_thread`/`dirty_decay_ms` defaults are preserved.

Verify it is live on Linux with `grep -c AnonHugePages /proc/<pid>/smaps` (a nonzero
huge-page total on the process maps) or by reading jemalloc's `opt.thp` mallctl (it
reports `always` when the option was honored). It is not a TOML `CONFIG SET` key:
jemalloc reads `malloc_conf` once at process init, before the config file is parsed,
so THP cannot be flipped live without a restart; the env override is the operational
knob and requires a process restart to take effect.

## Open questions

- The exact hot-swappable vs restart-required parameter partition (which knobs
  can change live without breaking an invariant), enumerated as each subsystem
  lands.
- Whether `CONFIG REWRITE` round-trips comments fully or only preserves a managed
  block.

## Acceptance and test hooks

- `CONFIG GET`/`SET` for the supported parameters match the pinned oracle
  (names, glob, reply shape) (#97); unsupported parameters are accepted+echoed and
  documented.
- `CONFIG SET` then `CONFIG REWRITE` persists to the TOML file and survives a
  restart.
- A `config reload` applies a file change to a hot-swappable parameter with no
  restart and reports a restart-required parameter rather than silently dropping
  it.
- A `CONFIG SET` override survives a subsequent `config reload` of a changed file
  (the runtime layer wins); clearing the runtime layer then lets the reloaded file
  value take effect (a precedence-composition test).

## References

- ADR-0007, ADR-0009, ADR-0020; issues #81, #86, #137, #50, #53, #15, #97.
- Claims: [redis-config-set-rewrite-no-sighup], [redis-no-config-file-default].
