# Design: Reproducible benchmark and memory-model harness

Issue: #8. Sub-task: #96 (Valkey baseline, head-to-head half). Decisions: ADR-0006
(jemalloc accounting), ADR-0003 (determinism / seeded runs), ADR-0009 (compat, so
the baseline runs matched configs). Related: #95 (the correctness counterpart),
#41 (memory targets), #1 (vision claims).

## Goal and scope

IronCache makes memory and tail-latency claims against Redis 8.x, Valkey, and
Dragonfly. Those claims are worthless unless anyone can reproduce them on pinned
hardware with a pinned method. This specifies one scripted harness that fixes
every variable and emits machine-readable artifacts, plus a per-key memory model
that explains, byte for byte, where IronCache wins or loses. Scope: the harness,
the memory model, and the living competitor matrix. Tuning IronCache itself is out
of scope; correctness is #95.

## Design

### Reproducible harness

- One scripted invocation reproduces a published run end to end. It pins every
  variable: exact memtier flags (never inherited defaults, which are inadequate for
  tail work [memtier-default-clients-threads-requests]), the instance type and
  kernel, the `taskset` layout (disjoint server/client cores, isolated loopback,
  borrowed wholesale from Valkey's bare-metal methodology
  [valkey-methodology-baremetal-pinning]), warmup duration, zipf exponent, pipeline
  depth, and target hit ratio. A zipf working-set generator with a frozen exponent
  defines the canonical "memory at fixed hit ratio" benchmark; uniform-random keys
  are rejected as unlike cache reality.

### Honest tail latency: open-loop

- Latency is measured open-loop at a constant rate (wrk2-style), reporting
  p99/p999/p9999, because closed-loop load generators suffer coordinated omission
  and understate the tail [coordinated-omission-closed-loop]. Throughput (peak QPS)
  is a separate closed-loop pass; the two are never conflated. Every run emits
  machine-readable JSON plus an HdrHistogram artifact so results diff across
  milestones.

### Memory model

- Memory is accounted from allocator introspection — jemalloc `stats.allocated`
  plus `malloc_usable_size` (ADR-0006) — not from a naive sum of logical value
  sizes, because `maxmemory` accounting must capture allocator rounding
  [redis-maxmemory-accounting]. The model decomposes each entry as header +
  container + allocator rounding and reports per-key, per-encoding bytes with
  competitor columns: Redis 8.x's redesigned kvobj header
  [redis-kvobj-header-redesign-8x], Valkey's up-to-8-byte embedded key
  [valkey-embedded-key-8b], and Dragonfly's dashtable per-entry overhead
  [dashtable-overhead-bytes].

### Valkey head-to-head baseline (#96)

- The same CI runner measures IronCache and a pinned Valkey side by side on
  identical hardware under matched configs (ADR-0009), emitting QPS-per-core and
  bytes-per-key in one machine-readable report. Valkey is the canonical baseline
  because it is wire-identical to Redis 7.2 [valkey-resp-identical] and BSD-3
  licensed [valkey-license-bsd3] (no SSPL/RSAL artifact is used). The same pinned
  binary that is the differential oracle (#95) is the benchmark bar to beat, so
  "correct" and "the bar" are defined by one reference.

### Living competitor matrix

- A committed, dated competitor table tracks each baseline's version and defaults
  (the `VALKEY_VERSIONS` pins plus Redis/Dragonfly rows), refreshed every milestone
  [valkey-version-landscape-2026], including allocator defaults that move the
  numbers (jemalloc decay/background-thread settings [jemalloc-decay-defaults]).
  Versions are bumped only by explicit PR, never floating tags.

### Determinism

- Workload generation (key stream, zipf draws, operation mix) is seeded through the
  same controllable path as the runtime (ADR-0003), so a published run's input is
  reproducible from its recorded seed, not just its flags.

## Open decisions

- The canonical instance type and kernel version for the published matrix.
- The frozen zipf exponent and key/value size distribution for the standard
  benchmark.
- The pipeline depths and target hit ratios in the standard sweep.
- Where HdrHistogram and JSON artifacts are stored and how they are versioned.

## Acceptance and test hooks

- One scripted invocation reproduces a published run end to end with pinned flags,
  cores, and loopback isolation [valkey-methodology-baremetal-pinning].
- The open-loop constant-rate path reports p99/p999/p9999 free of coordinated
  omission [coordinated-omission-closed-loop]; a separate pass reports peak QPS.
- Every run emits machine-readable JSON plus an HdrHistogram artifact; a
  hardware/config matrix is published.
- The memory model reports per-key, per-encoding bytes from allocator introspection
  [redis-maxmemory-accounting], with Redis 8.x [redis-kvobj-header-redesign-8x],
  Valkey [valkey-embedded-key-8b], and Dragonfly [dashtable-overhead-bytes]
  columns.
- Side-by-side QPS-per-core and bytes-per-key vs a pinned Valkey run on identical
  hardware, emitted as one report [valkey-resp-identical].
- The competitor version/defaults table is committed and dated, refreshed this
  milestone [valkey-version-landscape-2026][jemalloc-decay-defaults]; bumps require
  an explicit PR.

## References

- ADR-0003, ADR-0006, ADR-0009; issues #96, #95, #41, #1; spec TESTING.md.
- Claims: [valkey-methodology-baremetal-pinning],
  [memtier-default-clients-threads-requests], [coordinated-omission-closed-loop],
  [redis-kvobj-header-redesign-8x], [valkey-embedded-key-8b],
  [dashtable-overhead-bytes], [redis-maxmemory-accounting], [jemalloc-decay-defaults],
  [valkey-version-landscape-2026], [valkey-resp-identical], [valkey-license-bsd3].
