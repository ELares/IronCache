<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# IronCache metrics reference

Issue: #555 (ship the dashboard, alert rules, and this catalog; default metrics
on). This is the operator/integrator reference for everything IronCache exposes on
its ops HTTP endpoint plus the key `INFO` fields. Every `ironcache_*` series listed
here is cross-checked against the render code in
`crates/ironcache-observe/src/lib.rs` and `crates/ironcache/src/metrics_http.rs`;
nothing here is aspirational.

Companion artifacts:

- `deploy/helm/ironcache/dashboards/ironcache-dashboard.json` -- a starter Grafana
  dashboard built on these series (p99/p99.9, ops/sec, hit ratio, evictions,
  connections, memory, per-shard hot-shard detection, replication, persistence).
  Set `metrics.grafanaDashboard.enabled=true` to auto-provision it as a
  sidecar-discoverable ConfigMap.
- `deploy/helm/ironcache/alerts/ironcache-alerts.yml` -- starter Prometheus alerting rules
  (also shipped as a PrometheusRule via `metrics.prometheusRule.enabled=true`).

## The ops endpoint

The metrics/health endpoint is a small hand-rolled HTTP/1.1 responder on a
dedicated port (`--metrics-addr`). It serves four fixed routes:

| Route        | Content type                          | Purpose |
|--------------|---------------------------------------|---------|
| `GET /metrics`  | `text/plain; version=0.0.4`        | Prometheus text exposition (all `ironcache_*` series below). |
| `GET /livez`    | `text/plain`                       | Liveness: `200` once the process is serving, else `503`. |
| `GET /readyz`   | `text/plain`                       | Readiness: `200` when load-on-boot is done and (raft mode) a leader is recognized, else `503`. |
| `GET /topology` | `application/json`                 | Structured membership/slots/epoch (the console reads this instead of parsing text). |

### Default-on (localhost)

Since #555 the endpoint is ON by default, bound to `127.0.0.1:9091` (the tunability
principle: an env-dependent tradeoff defaults SAFE, not off). So `/metrics` and the
k8s probes are scrapable out of the box WITHOUT exposing the ops port publicly.

- Override the bind: `--metrics-addr 0.0.0.0:9121` (expose it; put it behind a
  network policy). The shipped deployment artifacts do exactly this.
- Disable it entirely: `--metrics-addr off` (also `none` / `disabled` / an empty
  value). No socket is then bound.

`--metrics-addr` is a CLI flag (there is no env var or TOML key for it); resolution
lives in `cli::effective_metrics_addr`.

### Example Prometheus scrape config

```yaml
scrape_configs:
  - job_name: ironcache        # the rules in ironcache-alerts.yml match job="ironcache"
    static_configs:
      - targets: ["10.0.0.10:9121"]   # the --metrics-addr host:port of each node
```

Prometheus adds a `job` and an `instance` label to every series below at scrape
time; the per-shard series additionally carry a `shard` label emitted by IronCache.

## Metric families

The families are listed in the exact order `/metrics` renders them: the node
rollup (counters, then gauges, then the raft gauges in raft mode), the per-shard
counter/gauge detail, the node latency histogram, the per-shard latency
histogram, then the cross-shard inbox-depth gauges (#556) last. "Labels" lists
only the labels IronCache emits (Prometheus adds `job`/`instance` on top).

### Node rollup -- counters

These are summed across every shard (`MetricsRegistry::aggregate`). Counters are
monotonic; graph them with `rate()`.

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `ironcache_connections_received_total` | counter | (none) | Connections accepted since start. |
| `ironcache_commands_processed_total`   | counter | (none) | Commands processed since start. |
| `ironcache_evicted_keys_total`         | counter | (none) | Keys evicted to honor the memory ceiling. |
| `ironcache_expired_keys_total`         | counter | (none) | Keys reclaimed because their TTL passed. |
| `ironcache_keyspace_hits_total`        | counter | (none) | Read commands that found a live key. |
| `ironcache_keyspace_misses_total`      | counter | (none) | Read commands that found no live key. |
| `ironcache_hops_sent_total`            | counter | (none) | Cross-shard requests dispatched to a peer shard (the hop paid) (#556, the #517 zero-hop measurement harness). |
| `ironcache_hops_served_total`          | counter | (none) | Cross-shard requests received and served for a peer shard. |
| `ironcache_local_served_total`         | counter | (none) | Keyed requests served locally (owner is the home shard, no hop). |

### Node rollup -- gauges

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `ironcache_connected_clients`                    | gauge | (none) | Currently-open client connections. |
| `ironcache_keyspace_keys`                        | gauge | (none) | Live keys held across all shards and databases (eventually-consistent; published off the hot path). |
| `ironcache_uptime_seconds`                       | gauge | (none) | Seconds since the process started serving. |
| `ironcache_shards`                               | gauge | (none) | Configured shard (thread-per-core) count. |
| `ironcache_used_memory_bytes`                    | gauge | (none) | Allocator-attributed live allocated bytes (jemalloc `stats.allocated`). |
| `ironcache_used_memory_rss_bytes`                | gauge | (none) | Resident set size in bytes (jemalloc `stats.resident`). |
| `ironcache_maxmemory_bytes`                      | gauge | (none) | Effective `maxmemory` ceiling in bytes (`0` means unlimited). |
| `ironcache_persistence_last_save_unixtime`       | gauge | (none) | Unix seconds of the last successful save (`0` when persistence is off / no save yet). |
| `ironcache_persistence_rdb_changes_since_save`   | gauge | (none) | Changes since the last save (the dirty counter; `0` when persistence is off). |
| `ironcache_replication_link_up`                  | gauge | (none) | `1` when this node's replication link is up (a replica's link to its master; a master/standalone is `1`). |
| `ironcache_replication_lag_offset`               | gauge | (none) | Replication lag in LOGICAL WRITE OFFSETS (a replica's own lag; a master's worst replica; `0` when caught up / standalone / link down). |

### Node rollup -- raft control plane (raft-governance mode only)

Emitted ONLY when the node runs in raft-governance mode; a standalone node reports
no `ironcache_raft_*` series at all.

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `ironcache_raft_is_leader`    | gauge | (none) | `1` when this node currently believes it is the Raft leader, else `0`. |
| `ironcache_raft_current_term` | gauge | (none) | The node's persisted current Raft term. |
| `ironcache_raft_commit_index` | gauge | (none) | The highest Raft log index known committed. |
| `ironcache_raft_voters`       | gauge | (none) | The size of the current Raft voter set. |

### Node rollup -- command latency histogram

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `ironcache_command_duration_seconds` | histogram | `le` (on `_bucket`) | Command execution latency in seconds (all commands, all shards). |

A Prometheus histogram, so the family expands to three child series:

- `ironcache_command_duration_seconds_bucket{le="<upper bound>"}` -- cumulative
  count of observations with latency `<=` the bound; the final `le="+Inf"` bucket
  equals `_count`.
- `ironcache_command_duration_seconds_sum` -- total observed latency in seconds.
- `ironcache_command_duration_seconds_count` -- total observations.

Bucket `le` upper bounds (seconds), log-spaced ~25us..10s:

```
0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, +Inf
```

Derive tail latency with `histogram_quantile` (see the recipes below). This family
is NOT reset by `CONFIG RESETSTAT` (a cumulative counter-typed family runs for the
process lifetime; rate/quantile math assumes it only resets on restart).

### Per-shard detail

Additive to the node rollup, in a DISTINCT `ironcache_shard_*` namespace (so there
is no mixed-label double count within a node family). Each series carries a
`shard="i"` label (`i` is the thread-per-core shard index). Use these for
hot-shard / key-skew detection.

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `ironcache_shard_connections_received_total` | counter   | `shard` | Connections accepted since start, per shard. |
| `ironcache_shard_commands_processed_total`   | counter   | `shard` | Commands processed since start, per shard. |
| `ironcache_shard_evicted_keys_total`         | counter   | `shard` | Keys evicted to honor the memory ceiling, per shard. |
| `ironcache_shard_expired_keys_total`         | counter   | `shard` | Keys reclaimed because their TTL passed, per shard. |
| `ironcache_shard_keyspace_hits_total`        | counter   | `shard` | Read commands that found a live key, per shard. |
| `ironcache_shard_keyspace_misses_total`      | counter   | `shard` | Read commands that found no live key, per shard. |
| `ironcache_shard_hops_sent_total`            | counter   | `shard` | Cross-shard requests dispatched to a peer shard (the hop paid), per shard. |
| `ironcache_shard_hops_served_total`          | counter   | `shard` | Cross-shard requests received and served for a peer shard, per shard. |
| `ironcache_shard_local_served_total`         | counter   | `shard` | Keyed requests served locally (owner is the home shard, no hop), per shard. |
| `ironcache_shard_connected_clients`          | gauge     | `shard` | Currently-open client connections, per shard. |
| `ironcache_shard_keyspace_keys`              | gauge     | `shard` | Live keys held, per shard. |
| `ironcache_shard_command_duration_seconds`   | histogram | `shard`, `le` | Command execution latency in seconds, per shard (child series `_bucket{shard,le}`, `_sum{shard}`, `_count{shard}`). |

### Cross-shard inbox depth (#556) -- rendered last

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `ironcache_inbox_depth`       | gauge | (none)  | Cross-shard inbox occupancy (queued cross-shard work items) summed across all shards. |
| `ironcache_shard_inbox_depth` | gauge | `shard` | The same occupancy, per shard -- back-pressure BUILDING before the bounded inbox stalls a home core. |

The depth is SAMPLED from the channel length at scrape time (`render_inbox_depth` in
`crates/ironcache-observe/src/lib.rs`), so the gauge costs the cross-shard hop path
nothing; the hop COUNTERS above are the monotonic view of the same coordinator traffic.

That is the complete set: 39 metric families -- 9 node counters, 11 node gauges, the 4
`ironcache_raft_*` gauges (raft-governance mode ONLY; a standalone node emits none), the
2 inbox-depth gauges, 9 per-shard counters, 2 per-shard gauges, and the 2 latency
histograms. Everything except the raft gauges is always emitted.

## Key INFO fields

These are served by the RESP `INFO` command (Redis-compatible field names), NOT by
`/metrics`. The Redis exporter (`github.com/oliver006/redis_exporter`) parses this
INFO and bridges it to `redis_*` series if you want them in Prometheus. Only fields
IronCache actually emits are listed.

| Section          | Field | Meaning |
|------------------|-------|---------|
| `# Server`       | `uptime_in_seconds` | Seconds since start (matches `ironcache_uptime_seconds`). |
| `# Clients`      | `connected_clients` | Currently-open client connections. |
| `# Clients`      | `maxclients` | Configured client connection ceiling (NOT exported to `/metrics`; the "connections near limit" alert is an absolute threshold vs this). |
| `# Clients`      | `blocked_clients` | Clients blocked on a blocking command. |
| `# Memory`       | `used_memory` / `used_memory_rss` | Allocated bytes / RSS (match the `ironcache_used_memory*` gauges). |
| `# Memory`       | `maxmemory` / `maxmemory_policy` | Ceiling bytes and the eviction policy. |
| `# Memory`       | `mem_fragmentation_ratio` | RSS over allocated. |
| `# Persistence`  | `rdb_changes_since_last_save` | Dirty counter (matches `ironcache_persistence_rdb_changes_since_save`). |
| `# Persistence`  | `rdb_bgsave_in_progress` | `1` while a background save runs. |
| `# Persistence`  | `rdb_last_save_time` | Unix seconds of the last save (matches `ironcache_persistence_last_save_unixtime`). |
| `# Persistence`  | `rdb_last_bgsave_status` | `ok` / `err` of the last background save (NOT a `/metrics` series; see the commented rule in the alerts file). |
| `# Stats`        | `total_connections_received` / `total_commands_processed` | Since-start totals (match the `*_total` counters). |
| `# Stats`        | `instantaneous_ops_per_sec` | Recent commands-per-second rate (#549). |
| `# Stats`        | `expired_keys` / `evicted_keys` | Match the `*_keys_total` counters. |
| `# Stats`        | `keyspace_hits` / `keyspace_misses` | Match the `ironcache_keyspace_*_total` counters. |
| `# Replication`  | `role` | `master` or `replica`. |
| `# Replication`  | `connected_slaves` / `slaveN` | Replica count and per-replica `ip/port/state/offset/lag`. |
| `# Replication`  | `master_link_status` (replica) | `up` / `down` of the link to the master (drives `ironcache_replication_link_up`). |
| `# Replication`  | `master_repl_offset` / `slave_repl_offset` | Logical write offsets (drive `ironcache_replication_lag_offset`). |
| `# Cluster`      | `cluster_enabled` | `1` when clustering is enabled. |
| `CLUSTER INFO`   | `cluster_state` | `ok` / `fail` (NOT a `/metrics` series; see the commented rule in the alerts file). |
| `# Commandstats` | `cmdstat_<cmd>` | Per-command `calls,usec,usec_per_call,rejected_calls,failed_calls`. |
| `# Errorstats`   | `errorstat_<CODE>` | Per error-code `count`. |

## PromQL recipes

```promql
# p99 / p99.9 command latency (node).
histogram_quantile(0.99,  sum(rate(ironcache_command_duration_seconds_bucket[5m])) by (le))
histogram_quantile(0.999, sum(rate(ironcache_command_duration_seconds_bucket[5m])) by (le))

# p99 per shard (hot-shard detection).
histogram_quantile(0.99, sum(rate(ironcache_shard_command_duration_seconds_bucket[5m])) by (le, shard))

# Operations per second (node-wide).
sum(rate(ironcache_commands_processed_total[1m]))

# Keyspace hit ratio over 5m (clamp_min avoids 0/0 while idle).
sum(rate(ironcache_keyspace_hits_total[5m]))
  / clamp_min(sum(rate(ironcache_keyspace_hits_total[5m])) + sum(rate(ironcache_keyspace_misses_total[5m])), 1)

# Eviction rate (memory-ceiling pressure).
sum(rate(ironcache_evicted_keys_total[5m]))

# Cross-shard hop rate: the fraction of keyed requests that paid a cross-shard hop.
# In shard-owners mode with an owner-dialing client this trends to ~0 (#517 zero-hop).
sum(rate(ironcache_hops_sent_total[5m]))
  / clamp_min(sum(rate(ironcache_hops_sent_total[5m])) + sum(rate(ironcache_local_served_total[5m])), 1)

# Memory used as a fraction of the ceiling (only where maxmemory is set).
ironcache_used_memory_bytes / (ironcache_maxmemory_bytes > 0)

# Seconds since the last successful save (only after a save has happened).
time() - (ironcache_persistence_last_save_unixtime > 0)
```
