# Design: Troubleshooting introspection commands (MEMORY USAGE/STATS/DOCTOR, LATENCY DOCTOR)

Issue: #151. Decisions: ADR-0006 (allocator and memory accounting), ADR-0009
(behavioral equivalence, not bit-identical). Related: #41 (the accounting model
these expose), #86/OBSERVABILITY.md (INFO/metrics and LATENCY LATEST/HISTORY/RESET),
#150/ADMIN_COMMANDS.md (the CLIENT/COMMAND introspection family), #40 (OBJECT
ENCODING/DEBUG OBJECT), #52/COMPRESSION.md (compressed-bytes accounting), #97
(conformance oracle).

## Goal and scope

These are the diagnostic commands an operator reaches for during an incident:
`MEMORY USAGE`, `MEMORY STATS`, `MEMORY DOCTOR`, and `LATENCY DOCTOR`. Their output
is tightly coupled to IronCache's divergent internals (thread-per-core, the
jemalloc-attributed accounting of ADR-0006, transparent compression), so verbatim
Redis output is impossible: the contract is behavioral equivalence (ADR-0009), and
the synthesized fields and advice are specified here and conformance-mapped (#97).
Scope: the four diagnostic commands above. Out of scope: INFO/SLOWLOG and LATENCY
LATEST/HISTORY/RESET (owned by #86), OBJECT ENCODING/DEBUG OBJECT (#40), and the
CLIENT/COMMAND family (#150).

## Design

### MEMORY USAGE key [SAMPLES count]

- Returns the allocator-attributed bytes a key occupies (value plus key plus
  per-entry overhead), drawn from the same jemalloc accounting that backs
  `maxmemory` (ADR-0006, [redis-maxmemory-accounting]), not a naive logical size.
  For a compressed value it reports the compressed stored bytes (COMPRESSION.md
  #52), since that is what occupies RAM. For a container type it samples nested
  elements with `SAMPLES` (0 = exact), matching Redis's sampled estimate. The
  reply is an integer byte count in the Redis shape so existing tooling parses it.

### MEMORY STATS

- Returns a map of memory metrics with Redis-recognized field names where they
  map: total allocator-attributed bytes, dataset bytes, overhead, and the
  `mem_fragmentation_ratio` (RSS/used, [redis-fragmentation-ratio]). Fields with
  no thread-per-core analog are synthesized to a sane value rather than faked: a
  single-threaded `peak.allocated` becomes the per-shard peaks rolled up, and
  IronCache-native fields (per-shard balance/skew, compression ratio, tier
  occupancy) are additive so an older parser ignores them safely (ADR-0009). The
  field catalog is pinned in the OBSERVABILITY registry (#86, #152) and versioned.

### MEMORY DOCTOR

- An advisory-text generator: it inspects fragmentation
  [redis-fragmentation-ratio], large keys, eviction pressure, and the
  IronCache-specific signals (per-shard skew #170, compression ratio, tier
  pressure) and emits human-readable advice, or a healthy-state line when nothing
  is wrong. The text is deliberately not byte-identical to Redis (the internals it
  describes differ); it is specified by the conditions it must detect and the
  remediation it must name, verified on shape and trigger rather than exact
  wording (#97).

### LATENCY DOCTOR

- An advisory-text generator over the LATENCY monitor events that #86 owns
  (LATEST/HISTORY/RESET; the monitor is on by default in IronCache per the #86
  divergence, where Redis ships it off [redis-latency-monitor-default-off]). It
  analyzes recorded spike events and emits advice. Because IronCache does not fork
  (ADR-0022), it never emits Redis's fork-latency advice; instead it names the
  spike sources that do exist here (active defrag, mass eviction, snapshot
  serialization, TTL-cascade), composing with the per-command latency budget
  experiment (#141). Advice is conformance-checked on trigger, not wording.

## Open questions

- The exact `MEMORY STATS` field set and which Redis fields are reported verbatim
  vs synthesized vs IronCache-native (locked in the #86/#152 registry before the
  M1 freeze).
- Whether `MEMORY DOCTOR` / `LATENCY DOCTOR` advice strings are versioned so a
  dashboard scraping them survives an upgrade.
- Whether `MEMORY MALLOC-STATS` (a raw jemalloc `mallctl` dump) is exposed for
  deep debugging or kept behind a debug flag.

## Acceptance and test hooks

- `MEMORY USAGE` returns an allocator-bytes figure consistent with the value the
  same key contributes to `maxmemory` accounting [redis-maxmemory-accounting];
  compressed values report compressed stored bytes (a consistency test).
- `MEMORY STATS` field names parse with an unmodified Redis-aware tool; the
  fragmentation ratio matches RSS/used [redis-fragmentation-ratio]; native fields
  are additive and ignored by an older parser.
- `MEMORY DOCTOR` returns non-empty advice when fragmentation/big-key/skew is
  injected and a healthy line otherwise (a trigger test, not a wording match).
- `LATENCY DOCTOR` composes with the #86 monitor, names a defrag/eviction/snapshot
  spike when one is induced, and never emits fork-latency advice (ADR-0022).

## References

- ADR-0006, ADR-0009, ADR-0022; issues #41, #86, #152, #150, #40, #52, #170, #141,
  #97, #1; specs OBSERVABILITY.md, ADMIN_COMMANDS.md, COMPRESSION.md.
- Claims: [redis-maxmemory-accounting], [redis-fragmentation-ratio],
  [redis-latency-monitor-default-off].
