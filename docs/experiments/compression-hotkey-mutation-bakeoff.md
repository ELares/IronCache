# Experiment: Hot-key mutation compression-policy bake-off

Issue: #56. Provisional decision: COMPRESSION_MUTATION.md (#56) pins
disable-on-first-grow-mutation as the default and recompress-on-RCU as an opt-in
only, and pins an adaptive mutation-rate demotion with a server-default
threshold; this experiment supplies the throughput-per-core numbers that justify
the default and calibrates the demotion threshold. ADR-0016 (headline metric is
throughput-per-core under a fixed open-loop methodology) fixes how the numbers
are measured.

## Provisional decision (already pinned)

The policy is already chosen in COMPRESSION_MUTATION.md (#56): a value-growing or
in-place byte mutator (APPEND, SETRANGE, SETBIT, BITFIELD write, BITOP
destination) disables compression for the key on its first occurrence and then
mutates raw bytes in place; the INCR family and below-gate values are never
compressed; an adaptive per-key mutation-rate counter demotes a frequently
mutated key off compression. The design argues from the codec asymmetry that
recompression on the hot path is the wrong default: compression is the slow codec
direction ([zstd-silesia-benchmark-l1], [lz4-flex-safe-vs-c]) and recompressing
on every mutation also breaks the in-place fast path of the hybrid-log engine,
whose narrow-waist RMW primitive assumes a stable in-place-updatable record
[garnet-narrow-waist-api] (mutable region in place, read-only region RCU to tail
[faster-hybridlog-three-regions]). This experiment does NOT re-decide the policy
or the codec (ADR-0015). It produces the per-core throughput curves that show how
much each policy costs on a hot mutated key, and the mutations-per-second point
at which adaptive demotion should fire.

## Why this is harness-blocked

The decision rule is "keep the mutation hot path on the throughput-per-core
budget," and the budget is an empirical number on real silicon, not a citation.
The published codec speeds bound the cost but do not give it: [zstd-silesia-benchmark-l1]
and [lz4-flex-safe-vs-c] are corpus-and-CPU-specific compress/decompress rates,
not the per-mutation cost inside IronCache's RMW path with its allocation, log
append, and header reframing. No public claim states the throughput-per-core of a
hot APPEND loop under each policy, because it depends on this project's engine
(#64), its codec build (ADR-0015/ADR-0021), and its record framing (#52). The
demotion threshold is even more clearly local: it is the mutations-per-second at
which the recompress or RCU tax on a specific deployment's hardware overtakes the
memory it saves, which only a measurement can name. So the policy is justified by
argument now and must be confirmed by the harness (#8) on a pinned single-core
matrix.

## Experiment to run

Workload: a hot-key mutation loop driven through the #8 harness with pinned
memtier flags and an explicit pipeline depth (memtier defaults to no pipelining
[memtier-default-pipeline-1]), on a Zipfian keyspace with a small hot set so a
few keys absorb most mutations, over the YCSB-style mix adapted to be
mutation-heavy [ycsb-core-workloads]. Tails are read from an open-loop
coordinated-omission-corrected generator [coordinated-omission-closed-loop], with
a separate closed-loop pass for peak per-core QPS, per ADR-0016; the two are
never conflated.

Fixed parameters: single pinned core (throughput-per-core is total QPS divided by
cores, so core count is not the lever); codec = zstd at the ADR-0015 default low
level for the compressed arms, with one lz4 arm for contrast; engine = the #64
hybrid-log mutable region; value framing = the #52 header. A compressible value
class (structured JSON, the kind dictionaries lift [zstd-dictionary-small-data-6.9x])
and an incompressible class are both run, since the disable cost only matters when
the value was actually compressed.

Varied parameters, one policy per arm: (1) never-compressed baseline (the key was
never a compression candidate), (2) disable-on-first-grow-mutation (the default),
(3) recompress-on-RCU (the opt-in), (4) adaptive demotion with the demotion
threshold swept across a range of candidate mutations-per-second points. The
mutating command is swept across APPEND (grows every call), SETRANGE
(grow and non-grow variants), SETBIT, and a BITFIELD INCRBY, so the table in
COMPRESSION_MUTATION.md is exercised per command. Mutation rate per hot key is
swept from cold (occasional writes) to saturated (a tight loop on one key).

Measured, per arm and per command: peak per-core throughput (closed loop) and
p99/p999/p9999 (open loop); per-mutation codec-call count (must be zero on the
in-place path for arms 1, 2, and 4 after demotion, and nonzero per call for arm
3); resident bytes-per-key from allocator introspection [redis-maxmemory-accounting],
so the memory the disable policy gives up is quantified against the throughput it
buys; and for the adaptive arm, the demoted-keys count (#86) and the
mutations-per-second at which throughput recovers to the baseline.

Decision rule: confirm disable-on-mutation if its per-core throughput on a hot
mutated key sits at or near the never-compressed baseline and strictly above
recompress-on-RCU; quantify the memory it sacrifices on the compressible class so
the trade is documented, not assumed. Set the adaptive demotion threshold to the
lowest mutations-per-second at which the recompress-on-RCU arm falls a fixed
budget below baseline, so demotion fires just before the tax becomes visible.
Report whether the INCR-family arm shows any codec cost at all (it should not,
since those values are never compressed) as a check on the size gate (#57).

## What would change the decision

If recompress-on-RCU held the per-core budget within the gate on a hot key (for
example if the codec cost were dwarfed by network and RMW overhead at realistic
mutation rates), the disable-on-mutation default would weaken to a tunable and
recompress-on-RCU could become the default for compressible workloads. If the
memory given up by disabling compression on append-heavy keys were negligible
(the keys stay small), the policy would be uncontroversial; if it were large, the
opt-in recompress mode would be worth keeping. If the adaptive counter could not
separate hot from cold keys without flapping (demote/re-promote churn), the
adaptive policy would fall back to the static disable-on-first-grow-mutation rule
alone. If even an idle compressed key paid a measurable decode cost on the read
bit commands (BITCOUNT/BITPOS), the rule that reads keep compression on would be
revisited.

## References

- Issue #56 / COMPRESSION_MUTATION.md: the mutation contract and adaptive
  demotion this experiment confirms and calibrates.
- Issue #52 / COMPRESSION.md: the stored framing and GET decode the policy reuses.
- Issue #64 / HYBRIDLOG_ENGINE.md: the in-place/RCU engine whose stable-record-
  size assumption recompression would break.
- Issue #131 / BITMAPS.md: the bit mutators (SETBIT/BITFIELD/BITOP) whose
  compression rule is owned by #56 and exercised here per command.
- Issue #57 / value-size-compressibility-survey: the size gate that keeps INCR-
  family and small values raw.
- Issue #8 / BENCHMARK.md: the harness this experiment is blocked on.
- ADR-0016: throughput-per-core headline metric and open-loop methodology.
- ADR-0015: the zstd-low codec held fixed for the compressed arms.
- [zstd-silesia-benchmark-l1], [lz4-flex-safe-vs-c]: codec compress/decompress
  asymmetry that motivates disable-over-recompress.
- [zstd-dictionary-small-data-6.9x]: dictionary lift defining the compressible
  value class.
- [garnet-narrow-waist-api], [faster-hybridlog-three-regions]: the narrow-waist
  RMW primitive and the mutable/read-only region split whose stable-record-size
  assumption recompression would break.
- [redis-maxmemory-accounting]: allocator-introspection memory measurement.
- [coordinated-omission-closed-loop], [ycsb-core-workloads],
  [memtier-default-pipeline-1]: the open-loop tail methodology and workload.
