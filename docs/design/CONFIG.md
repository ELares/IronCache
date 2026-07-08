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

### Transparent huge pages

Issue: #512. Related: ADR-0006 (allocator), [[tunability-principle]].

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
