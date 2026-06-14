# Design: Observability (native Prometheus, INFO/SLOWLOG/LATENCY parity, metric registry)

Issues: #86 (observability surface), #152 (metric/label registry + INFO catalog +
cardinality bounds). Decisions: ADR-0009 (compat), ADR-0017 (native metrics feed
the gates). Related: #81 (in-process, no sidecar), #85 (config), #137 (admission
metrics), #88 (advisor state).

## Goal and scope

IronCache must be observable on day one without operators rewriting a dashboard:
existing `redis_exporter` scrape configs and Grafana boards keep working, while
operators who want native Prometheus get a first-class in-process endpoint and
IronCache-specific telemetry that older parsers ignore safely. This covers the
RESP observability commands, the native metrics endpoint, and (the #152 half) the
concrete metric/label/INFO-field registry with cardinality bounds.

## Design

### RESP parity: INFO / SLOWLOG / LATENCY

- `INFO` returns the standard sections [redis-info-sections] with Redis-recognized
  field names, so `redis_exporter` and existing parsers work unchanged (ADR-0009).
- `SLOWLOG` (default threshold 10000 us, 128 entries [redis-slowlog-defaults]) and
  `LATENCY` (monitor off by default [redis-latency-monitor-default-off]) match
  Redis behavior and defaults.

### Native Prometheus endpoint (no sidecar)

- Unlike Redis, which has no native Prometheus endpoint and relies on a separate
  `redis_exporter` process [redis-no-builtin-prometheus], IronCache serves
  `/metrics` in-process (an HTTP endpoint on a configurable `--metrics-addr`),
  the way Dragonfly serves metrics on its own port
  [dragonfly-native-prometheus-6379-metrics]. No exporter sidecar, no extra hop,
  no lag. The single-binary thesis (#81) requires this.

### Metric/label registry (#152)

- A single registry defines every Prometheus metric: exact name, label set, and
  series type (counter/gauge/histogram). Per-command series (`commandstats`,
  `latencystats`, `errorstats`) carry the command name as a label, so the registry
  imposes an explicit per-command cardinality bound: a fixed allow-list of known
  commands plus an `other` bucket, so a high-arity or adversarial workload cannot
  explode label cardinality. The registry is versioned so a dashboard survives an
  upgrade.

### Native INFO section (#152)

- An IronCache-native `# IronCache` INFO section carries telemetry Redis has no
  field for: hit ratio, per-shard balance/skew, fsync lag, compression ratio, SSD
  tier endurance, and advisor state (#88). It is additive: an older parser reading
  `INFO` sees the standard sections it expects and ignores the extra section
  safely. The field catalog (names, types, units) is pinned in the registry and
  versioned.

### Memory and admission telemetry

- `used_memory` is the allocator-attributed figure (ADR-0006,
  [redis-maxmemory-accounting]); the `mem_fragmentation_ratio` (RSS/used) is
  exposed. Admission/OOM counters (rejected connections, `-OOM` write rejections,
  per-class output-buffer trims) come from #137; the rejected-oversize-frame
  counter from #138.

## Open questions

- The exact metric names and label sets (locked in the registry before the M1
  freeze, per #152), and whether histograms use native Prometheus histograms or
  summaries.
- Whether the native INFO section is on by default or behind a flag (cardinality
  vs visibility).

## Acceptance and test hooks

- An unmodified `redis_exporter` scrape + a stock Grafana Redis board work
  against IronCache `INFO` (a compatibility test).
- `/metrics` serves in-process with no sidecar; metric names/labels match the
  versioned registry; per-command series cannot exceed the cardinality bound (a
  cardinality test feeding a high-arity workload).
- `SLOWLOG`/`LATENCY`/`INFO` defaults and reply shapes match the pinned oracle
  (#97).

## References

- ADR-0006, ADR-0009, ADR-0017; issues #81, #85, #137, #138, #88, #97.
- Claims: [redis-info-sections], [redis-slowlog-defaults],
  [redis-latency-monitor-default-off], [redis-no-builtin-prometheus],
  [dragonfly-native-prometheus-6379-metrics], [redis-maxmemory-accounting].
