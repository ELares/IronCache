# IronCache operator runbook (symptom to action)

This is the 3am on-call guide: a symptom-to-action index of every operator-visible
error string, log line, and probe state IronCache can emit, each with what it means,
what to check, and how to resolve it. It is a companion to `DEPLOY.md` (how to deploy)
and `docs/design/` (why the system behaves this way).

Everything here is verified against the source; the leading wire tokens (`-NOAUTH`,
`-OOM`, `-MOVED`, ...) and the field/metric names are part of the contract, not
paraphrases. Where a signal is planned but not yet emitted, it says so explicitly.

## How to use this document

1. If you have a **client-facing error string** (starts with `-`), jump to
   [Client-facing wire errors](#client-facing-wire-errors).
2. If you have a **log line** (from stderr / the orchestrator log), jump to
   [Boot and runtime log lines](#boot-and-runtime-log-lines).
3. If a **probe** is failing (`/livez`, `/readyz`), jump to
   [Probe states](#probe-states).
4. If you have a **symptom** (slow, OOM, refused connections, hung stop), jump to
   [Common scenarios](#common-scenarios).
5. The tools you will reach for are in
   [Diagnostic commands](#diagnostic-commands) and [Key metrics](#key-metrics-to-watch).

Conventions: an arrow `->` means "results in"; `--` is a plain separator. Wire error
lines are shown exactly as the server sends them (without the trailing `\r\n`).

---

## First 60 seconds

```
# Is the process alive, and has it finished loading?
curl -s localhost:9121/livez     # 200 "OK" = process up; 503 "starting" = still booting
curl -s localhost:9121/readyz    # 200 "OK" = serving; 503 "not ready: <reason>"

# Is it answering RESP at all?
redis-cli -h <host> -p 6379 PING          # -> PONG

# One-shot health snapshot (no auth needed for these read fields):
redis-cli ... INFO server clients memory persistence stats replication

# What is it doing right now?
redis-cli ... INFO stats | grep instantaneous_ops_per_sec
redis-cli ... SLOWLOG GET 10
redis-cli ... CLIENT LIST
```

The ops HTTP endpoint (`/livez`, `/readyz`, `/metrics`) is ON by default at
`127.0.0.1:9091` (since #555); override the bind with `--metrics-addr <ip:port>` or
disable it with `--metrics-addr off` (see `DEPLOY.md` section 7). The RESP
introspection commands (`INFO`, `SLOWLOG`, `CLIENT`, ...) work regardless.

---

## Probe states

The ops HTTP endpoint (default `127.0.0.1:9091`, set by `--metrics-addr`) serves
three fixed routes. Semantics are in `crates/ironcache/src/metrics_http.rs`.

| Route | Code + body | Meaning |
|-------|-------------|---------|
| `GET /livez` | `200` `OK` | Process is up and serving. Set once at end of boot; never flips back. |
| `GET /livez` | `503` `starting` | Process is still booting (has not reached "ready" in `main`). |
| `GET /readyz` | `200` `OK` | Load-on-boot is complete for EVERY shard AND (raft mode) a leader is recognized. |
| `GET /readyz` | `503` `not ready: load-on-boot incomplete` | At least one shard has not finished restoring its snapshot. |
| `GET /readyz` | `503` `not ready: raft: no leader recognized` | Raft mode: the node has not yet joined a formed cluster / an election is unresolved. |
| `GET /metrics` | `200` | Prometheus exposition (see [Key metrics](#key-metrics-to-watch)). |
| `GET /topology` | `200` (JSON) | Structured membership/slots/epoch/raft state (read-only). |

`/livez` is the Kubernetes **liveness** probe (restart a hung pod). `/readyz` is the
**readiness** probe (hold traffic + the rolling update until genuinely ready).

### A `/readyz` stuck at `not ready: load-on-boot incomplete`

Readiness AND-reduces a per-shard countdown: it flips to ready only when every shard's
`load_shard_on_boot` has returned (`ReadyState` in `metrics_http.rs`). A stuck readiness
means a shard has not finished restoring its on-disk snapshot.

- Check: `du -h <data_dir>/dump-shard-*.icss` -- a large snapshot legitimately takes
  time to load; readiness deliberately holds traffic so you never serve an empty or
  partial keyspace.
- Check stderr for the version-mismatch error (below): a snapshot this binary cannot
  read is discarded, and depending on config either logged or refused (fail-closed).
- With persistence OFF every shard's load is an immediate no-op, so readiness should
  flip almost instantly; a stuck readiness with no `data_dir` points at a wedged shard
  thread (check for a panic in the log).

### A `/readyz` stuck at `not ready: raft: no leader recognized`

Only in raft-governance mode. Evaluated live from the raft handle at scrape time, so it
reflects the CURRENT leader state (a node can lose its leader after boot). See
[Lost raft quorum](#lost-raft-quorum-no-leader).

---

## Key metrics to watch

Two independent surfaces expose the same underlying counters: the Prometheus `/metrics`
endpoint and the RESP `INFO` command. Names below are verified in
`crates/ironcache-observe/src/lib.rs`.

### Prometheus `/metrics`

Node-wide series (from `render_prometheus`):

| Metric | Type | Watch for |
|--------|------|-----------|
| `ironcache_command_duration_seconds_bucket{le="..."}` / `_count` / `_sum` | histogram | Command latency (the p99 source, #546). |
| `ironcache_commands_processed_total` | counter | Throughput (rate = ops/sec). |
| `ironcache_connections_received_total` | counter | Connection churn. |
| `ironcache_connected_clients` | gauge | Live connection count (vs `maxclients`). |
| `ironcache_keyspace_hits_total` / `ironcache_keyspace_misses_total` | counter | Hit ratio. |
| `ironcache_expired_keys_total` | counter | TTL reclamation rate. |
| `ironcache_evicted_keys_total` | counter | maxmemory eviction rate (an eviction storm shows here). |
| `ironcache_keyspace_keys` | gauge | Total live keys. |
| `ironcache_used_memory_bytes` / `ironcache_used_memory_rss_bytes` | gauge | Memory + RSS. |
| `ironcache_maxmemory_bytes` | gauge | The effective ceiling. |
| `ironcache_persistence_last_save_unixtime` | gauge | Last successful save (staleness). |
| `ironcache_persistence_rdb_changes_since_save` | gauge | Dirty keys since last save (RPO exposure). |
| `ironcache_uptime_seconds` | gauge | Process uptime (a reset proves a real restart). |
| `ironcache_shards` | gauge | Configured shard count. |
| `ironcache_raft_is_leader` / `ironcache_raft_current_term` / `ironcache_raft_commit_index` / `ironcache_raft_voters` | gauge | Raft state (raft mode only). |

Per-shard detail (from `render_prometheus_shards`, #362) carries a `{shard="i"}` label
on the same names with an `ironcache_shard_` prefix, e.g.
`ironcache_shard_commands_processed_total{shard="0"}`,
`ironcache_shard_connected_clients{shard="0"}`,
`ironcache_shard_evicted_keys_total{shard="0"}`. This is how you find a **hot shard**
(one shard's counters far above the others).

Per-shard latency histogram: `ironcache_shard_command_duration_seconds_bucket{shard="i",le="..."}`.

**p99 query** (Prometheus): the histogram buckets are in seconds
(`0.000025 .. 10, +Inf`), so:

```
histogram_quantile(0.99, sum(rate(ironcache_command_duration_seconds_bucket[5m])) by (le))
```

Note: replication is exported both ways. `/metrics` carries `ironcache_replication_link_up`
(1 healthy / 0 down) and `ironcache_replication_lag_offset` (lag in logical write offsets),
added in #549; `INFO replication` (below) carries the same signal in RESP form.

### `INFO` sections (RESP)

Request specific sections: `INFO server clients memory persistence stats replication`.
Load-bearing fields per section:

- `# Server`: `redis_version:7.4.0` (compatibility tag), `ironcache_version:<real>`,
  `redis_mode:standalone`, `process_id`, `run_id`, `tcp_port`, `uptime_in_seconds`,
  `io_threads_active` (= shard count).
- `# Clients`: `connected_clients`, `maxclients`, `blocked_clients` (currently always
  `0`), `cluster_connections:0`. Connection saturation = `connected_clients` near
  `maxclients`.
- `# Memory`: `used_memory`, `used_memory_human`, `used_memory_rss`, `maxmemory`,
  `maxmemory_policy`, `mem_fragmentation_ratio` (RSS/used), `mem_allocator`.
- `# Persistence`: `loading:0` (always 0 -- the readiness gate holds traffic until load
  completes, so `INFO` is only served post-load), `rdb_changes_since_last_save`,
  `rdb_bgsave_in_progress:0`, `rdb_last_save_time`, `aof_enabled:0` (snapshots only, no
  AOF), `persistence_enabled`, `save` (the active policy `"<secs> <changes>"`, empty when
  off), `rdb_last_bgsave_status:ok|err` (the explicit last-save outcome, added in #549).
  A failed save flips it to `err`; corroborate with `rdb_last_save_time` staleness and the
  save-on-exit / BGSAVE log lines below.
- `# Stats`: `total_connections_received`, `total_commands_processed`,
  `instantaneous_ops_per_sec` (coarse ops/sec), `rejected_connections` (refused by the
  `maxclients` gate), `expired_keys`, `evicted_keys`, `keyspace_hits`, `keyspace_misses`.
- `# Replication`: master side `role:master`, `connected_slaves`, and one
  `slaveN:ip=..,port=..,state=online,offset=..,lag=..` line per replica (the `lag` is the
  master's `head - replica_acked` view); replica side `role:replica`, `master_host`,
  `master_port`, `master_link_status` (`up`/`down`), `slave_read_only:1`,
  `slave_repl_offset`. `master_repl_offset` is reported in both roles.
- `# Keyspace`: `dbN:keys=<n>,expires=<m>,avg_ttl=0` per non-empty DB (`expires`/`avg_ttl`
  are `0` today; `keys` is the load-bearing field).

---

## Diagnostic commands

Every command below is verified present in the dispatch table
(`crates/ironcache-server/src/dispatch.rs`, `cmd_config.rs`, `cmd_introspect.rs`,
`cmd_cluster.rs`, `cmd_acl.rs`).

### CLIENT
`CLIENT ID | GETNAME | SETNAME <name> | SETINFO <attr> <value> | INFO | LIST [ID id ...]
| KILL <ID id | ADDR addr | ...> | PAUSE <ms> [WRITE|ALL] | UNPAUSE | NO-EVICT <on|off>
| NO-TOUCH | TRACKING ... | TRACKINGINFO | CACHING <YES|NO>`.

`CLIENT LIST` / `CLIENT INFO` fields per connection: `id=`, `addr=<ip:port>`,
`laddr=<ip:port>`, `name=`, `db=`, `resp=<1|2|3>`. Use `CLIENT LIST` to find a noisy or
stuck peer, then `CLIENT KILL ADDR <ip:port>` (or `KILL ID <id>`) to drop it.
`CLIENT PAUSE <ms> WRITE` holds writes node-wide (this is what `ironcache upgrade` uses
for a lossless swap); `CLIENT UNPAUSE` releases early.

### INFO
`INFO [section ...]` -- sections above. `INFO all` (or an explicit `commandstats` /
`errorstats`) adds `# Commandstats` (per-command counts) and `# Errorstats`
(per-error-token counts), which pinpoint WHICH command or WHICH error is spiking.

### MEMORY
`MEMORY USAGE <key> [SAMPLES n]` (per-key byte footprint, nil if absent),
`MEMORY DOCTOR` (human-readable assessment), `MEMORY STATS` (map incl. `peak.allocated`,
`total.allocated`, `startup.allocated`, `maxmemory`, `maxmemory.policy`,
`allocator.allocated`, `allocator.resident`, `fragmentation`).

### SLOWLOG
`SLOWLOG GET [count]` (default 10; entry shape
`[id, unix_time, micros, [args...], client_addr, client_name]`), `SLOWLOG LEN`,
`SLOWLOG RESET`. Defaults: `slowlog-log-slower-than` = 10000 us (10 ms),
`slowlog-max-len` = 128 (`crates/ironcache-observe/src/ops.rs`). `-1` disables it, `0`
logs every command. This is the first stop for a p99 spike with a specific slow command.

### LATENCY
`LATENCY LATEST` (`[event, unix_secs, latest_ms, max_ms]`), `LATENCY HISTORY <event>`,
`LATENCY RESET [event ...]`, `LATENCY DOCTOR`. Tracked event: `command`. Latency
monitoring is ON by default in IronCache (a deliberate divergence from Redis).

### CONFIG
`CONFIG GET <pattern> [pattern...]`, `CONFIG SET <param> <value> [param value...]`,
`CONFIG RESETSTAT` (zero the `# Stats`/`# Clients` since-boot counters),
`CONFIG REWRITE` (returns `-ERR The server is running without a config file` today).
Live-tunable params include `maxmemory`, `maxmemory-policy`, `maxclients`, `timeout`,
`slowlog-log-slower-than`, `slowlog-max-len`, `save`, `requirepass`,
`notify-keyspace-events`, `proto-max-bulk-len`, `tcp-keepalive`. Restart-required params
(`bind`, `port`, `databases`, `shards`) reject a runtime `CONFIG SET` with
`-ERR CONFIG SET failed (possibly related to argument '<param>') - can't set immutable config`.

### DEBUG
`DEBUG OBJECT <key>` (encoding + serialized length), `DEBUG SLEEP <seconds>` (blocks the
owning shard -- use with care), `DEBUG SET-ACTIVE-EXPIRE <0|1>`,
`DEBUG STRINGMATCH-LEN <pattern> <string>`, `DEBUG JMAP` / `DEBUG QUICKLIST-PACKED-THRESHOLD`
(no-ops).

### Persistence + lifecycle
`SAVE` (blocking snapshot), `BGSAVE` (background snapshot), `LASTSAVE` (unix time of the
last successful save -- an upgrade watches this advance), `DBSIZE`, `SHUTDOWN [SAVE|NOSAVE]`.
`SHUTDOWN` with no modifier saves iff a save point is configured (see
[Hung or slow shutdown](#hung-or-slow-graceful-shutdown) and `docs/design/SHUTDOWN.md`).

### CLUSTER / ACL
`CLUSTER INFO | MYID | SLOTS | SHARDS | NODES | KEYSLOT <key> | COUNTKEYSINSLOT <slot>`
(read/introspection). Mutators require cluster mode (see the cluster errors below).
`ACL WHOAMI | LIST | USERS | CAT | GETUSER <name> | GENPASS`. `ACL WHOAMI` confirms which
user a connection authenticated as.

### `ironcache check` (preflight, the nginx `-t` analogue)
`ironcache check [--config ...]` resolves + validates the effective config WITHOUT
binding a port and prints it: `bind`, `shards`, `runtime`, `databases`, `maxmemory`,
`policy`, `requirepass` (set/unset), `tls`, and the live `allocator` line. A malformed
`maxmemory`, a bad `maxmemory-policy`, or an unresolvable overlay fails here with a clear
error instead of at boot (`crates/ironcache/src/main.rs` `cmd_check`). Run it before every
deploy and after every config edit. `ironcache config` prints the same resolved config in
TOML form.

---

## Client-facing wire errors

These are the `-<TOKEN> <message>` lines the server sends a client. Clients switch on the
leading token, so the spelling is fixed. The catalog lives in
`crates/ironcache-protocol/src/error.rs`; the operator-relevant ones are indexed here.
(Per-command argument/syntax errors -- `-ERR syntax error`, `-WRONGTYPE ...`,
`-ERR value is not an integer or out of range`, the ZSET/BITMAP/EXPIRE option errors, etc.
-- are application bugs on the client side, not node-health signals, and are omitted.)

### Authentication and permissions

| Wire line | Means | Check / fix |
|-----------|-------|-------------|
| `-NOAUTH Authentication required.` | The connection ran a command before `AUTH` and `requirepass`/ACL is set. | Client is not authenticating. Confirm the client is configured with the password; `ACL WHOAMI` on a working connection. A NOAUTH LOOP (every command fails) means the client library is not sending `AUTH`/`HELLO AUTH` -- see [NOAUTH loop](#noauth-loop). |
| `-WRONGPASS invalid username-password pair or user is disabled.` | Wrong password, unknown user, or a disabled ACL user. | Verify the secret; `ACL LIST` / `ACL GETUSER <name>` to confirm the user exists and is `on`. |
| `-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?` | Client sent `AUTH` but the node has NO password configured. | Either the client is pointed at the wrong node, or `requirepass` was expected but not set. `CONFIG GET requirepass`. |
| `-NOPERM User <user> has no permissions to run the '<cmd>' command` | ACL denies the command for this user. | `ACL GETUSER <user>` and widen the command rules if intended. |
| `-NOPERM No permissions to access a key` / `-NOPERM No permissions to access a channel` | ACL key/channel pattern denies this key/channel. | `ACL GETUSER <user>`; adjust `~pattern` / `&pattern`. |

### Capacity and admission

| Wire line | Means | Check / fix |
|-----------|-------|-------------|
| `-OOM command not allowed when used memory > 'maxmemory'.` | A write was refused at the memory ceiling: eviction could not free enough (cache mode), or the policy is `noeviction`. | See [OOM / eviction storm](#oom--eviction-storm). `INFO memory` (`used_memory` vs `maxmemory`), `INFO stats` (`evicted_keys`). Raise `maxmemory` or change `maxmemory-policy`. |
| `-ERR max number of clients reached` | The `maxclients` connection ceiling is full; the new connection is rejected and closed (`crates/ironcache/src/serve.rs`). | See [Connection exhaustion](#connection-exhaustion). `INFO clients` (`connected_clients` vs `maxclients`), `INFO stats` (`rejected_connections`). `CLIENT LIST` to find leaked connections; raise `maxclients` (mind the fd budget). |

### Cluster and replication (raft / multi-node modes)

| Wire line | Means | Check / fix |
|-----------|-------|-------------|
| `-MOVED <slot> <ip:port>` | The key's slot is permanently owned by another node; the client should refresh its slot map and retry at the address. | Normal in cluster mode. A MOVED STORM (constant redirects) means a stale client slot cache or an unstable map -- see [Cluster MOVED storm](#cluster-moved-storm). |
| `-ASK <slot> <ip:port>` | Transient migration redirect: the key already moved to the destination while the slot is migrating. Client sends `ASKING` then the command once to the destination. | Expected during an online slot migration; transient. |
| `-TRYAGAIN Multiple keys request during rehashing of slot` | A multi-key command hit a migrating slot whose keys are split across source and destination. | Transient; the client retries as the migration converges. |
| `-CROSSSLOT Keys in request don't hash to the same slot` | A multi-key command's keys do not all hash to one slot (cluster mode). | Application must group keys with hash tags `{...}` so multi-key ops stay in one slot. |
| `-CLUSTERDOWN Hash slot not served` | The addressed slot has no owner (the cluster is not fully covered). | The slot map has a gap; a complete static map is required. `CLUSTER SLOTS` / `CLUSTER INFO`. |
| `-CLUSTERDOWN <message>` | Raft mode: this node is not the current leader, so it cannot commit a config change; retry against the leader. | Expected while a cluster forms or a leader changes. If persistent, see [Lost raft quorum](#lost-raft-quorum-no-leader). |
| `-NOREPLICAS Not enough good replicas to write.` | `min-replicas-to-write` is unmet: fewer in-sync replicas than required. | `INFO replication` (`connected_slaves`, per-replica `lag`). A replica is down or lagging past `min-replicas-max-lag`; recover the replica or relax the guard. |
| `-ERR This instance has cluster support disabled` | A CLUSTER mutator (MEET/ADDSLOTS/SETSLOT/...) was run on a `cluster-enabled no` node. | Expected on a standalone node; the CLUSTER introspection verbs still work. |

### Internal degradation

| Wire line | Means | Check / fix |
|-----------|-------|-------------|
| `-ERR cross-shard target unavailable` | A cross-shard hop found the owning shard's drain loop / receiver gone -- only during shutdown or a shard-thread panic (`crates/ironcache/src/coordinator.rs`). | If seen outside a graceful stop, a shard thread panicked: check stderr for a panic and the exit path. The node returns a well-formed error rather than hanging, but a panicked shard means data on that shard is unavailable -- restart the node. |
| `-EXECABORT Transaction discarded because of previous errors.` | A queued command inside `MULTI` errored, so `EXEC` applied nothing. | Client-side; the transaction was dirtied at queue time. In single-node mode (`shards == 1`) the cross-shard `MULTI` guards never fire. |
| `-NOPROTO unsupported protocol version` | `HELLO <n>` asked for a protocol version the server does not support. | Client should request RESP2 or RESP3 only. |

---

## Boot and runtime log lines

IronCache logs to STDERR (orchestrator-friendly), filtered by `--log-level`
(`error`/`warn`/`info`/`debug`/`trace`). The operator-critical `warn!`/`error!` sites are
indexed below with the file that emits them. Message text is verbatim.

### Boot facts (info)
- `ironcache: binding` with `version`, `bind`, `port`, `shards` (`main.rs`).
- `ironcache: ready (PING -> +PONG). Ctrl-C to stop.` -- boot complete, process live.
- `metrics: serving /metrics, /livez, /readyz` with `addr` (`metrics_http.rs`) -- the ops
  endpoint bound.
- `ironcache: shutting down` -- a stop signal was received.

### FD budget / maxclients clamp (#532, `crates/ironcache/src/fd_budget.rs`)
- WARN `maxclients clamped to fit the open-file limit (RLIMIT_NOFILE): the requested
  maxclients plus reserved fds exceeds the file-descriptor budget; raise 'ulimit -n' or
  the systemd LimitNOFILE= to restore the requested ceiling` with fields
  `requested_maxclients`, `soft_limit`, `hard_limit`, `reserved_fds`, `clamped_maxclients`.
  -> Your effective `maxclients` is LOWER than configured. Raise `ulimit -n` /
  `LimitNOFILE=` and restart. This is why `INFO clients` `maxclients` can be below what you
  set. (Reserved headroom is 64 fds.)
- WARN `could not raise the open-file soft limit; clamping maxclients to the current limit`
  (fields `error`, `target`, `hard_limit`) -- `setrlimit` was denied; same remedy.
- INFO `raised the open-file soft limit (RLIMIT_NOFILE) to fit maxclients plus reserved fds`
  -- benign; the requested ceiling was preserved by raising the soft limit.

### Snapshot version mismatch / empty-boot risk (#530, `crates/ironcache-persist/src/lib.rs`)
- ERROR `ironcache: the on-disk snapshot has an unsupported format version and will NOT be
  loaded; the node would start with an EMPTY keyspace (set
  refuse_empty_start_on_version_mismatch = true to fail closed and refuse to boot instead
  of discarding the on-disk data)` (fields `error`, `dir`). -> A dump written by a NEWER
  binary is being loaded by an OLDER one (a downgrade / failed-upgrade rollback). See
  [Failed upgrade rollback / empty boot](#failed-upgrade-rollback--empty-boot). If
  `refuse_empty_start_on_version_mismatch` is set, boot ALSO fails closed with `refusing to
  boot: the on-disk snapshot has an unsupported format version ...` (`main.rs`) instead of
  starting empty.

### Load-on-boot / maxmemory (`crates/ironcache/src/coordinator.rs`)
- WARN `ironcache: load-on-boot snapshot exceeded maxmemory; evicted to fit the ceiling`
  (fields `shard`, `evicted`) -- the restored snapshot was larger than `maxmemory`; keys
  were evicted to fit.
- WARN `ironcache: load-on-boot left this shard OVER maxmemory (snapshot larger than the
  ceiling and the eviction policy could not free enough); the node is over budget`
  (fields `shard`, `budget_bytes`, `used_bytes`) -> raise `maxmemory` or the snapshot will
  not fully load; writes will hit `-OOM`.

### Graceful shutdown / save-on-exit (#139/#543, `serve.rs` + `coordinator.rs`)
- WARN `ironcache: second stop signal -> forcing immediate exit` -- a second SIGTERM/SIGINT
  during an in-progress drain escalated to an immediate exit.
- INFO `ironcache: save-on-exit complete -> exit 0` / `ironcache: SHUTDOWN -> exit 0`
  (field `mode`) -- a clean stop.
- WARN `ironcache: save-on-exit: a prior save did not finish within SHUTDOWN_SAVE_WAIT;
  exiting best-effort (the in-flight save may still commit)` -- a save was already running
  at stop.
- ERROR `ironcache: save-on-exit failed (the prior committed snapshot stays valid)`
  (field `error`) -> the exit save failed (e.g. full disk); the PREVIOUS committed snapshot
  is still intact but this stop lost the newest writes. See
  [Full-disk SAVE failure](#full-disk-save-failure).

### Raft / cluster (`crates/ironcache/src/raft_boot.rs`)
- ERROR `persisted raft state at <log> uses an incompatible node-id scheme; this build
  derives node ids from cluster_announce_id. Start a FRESH cluster (remove <log>,
  <log>.cfg, and any <log>.snap) or migrate the persisted state.` -> An in-place upgrade
  across the node-id scheme change refuses to boot rather than silently split-brain. Remove
  the named `ironcache-raft-<port>.log` plus its `.cfg`/`.snap` sidecars for a fresh
  cluster, or migrate the state (`docs/design/SHUTDOWN.md`).
- ERROR `raft control plane: failed to bind` (fields `listen_addr`, `error`) -- the cluster
  bus port could not bind (address in use / permissions).
- ERROR `raft control plane: failed to create data directory` / `failed to open storage`
  -- the `data_dir` is not writable; fix permissions / the mount.

### Socket activation (`crates/ironcache/src/sockact_log.rs`, `crates/ironcache-runtime/src/bootstrap.rs`)
- INFO `socket-activation: ADOPTED <n> systemd socket-activation listening fd(s) [<name=fd>...];
  systemd owns the listen queue, so it survives an upgrade restart with no connection-refused
  window` (#562) -- this boot ADOPTED the fd(s) systemd passed; the upgrade handoff is in effect.
- INFO `socket-activation: FELL BACK to self-binding its own listener: not socket-activated (no
  LISTEN_FDS in the environment)` -- the normal, non-socket-activated boot.
- WARN `socket-activation: FELL BACK to self-binding its own listener: the socket-activation
  environment was REJECTED and not adopted (<reason>)` -> a `LISTEN_*` env was PRESENT but rejected
  (a foreign/missing `LISTEN_PID`, a malformed count); the socket-activated upgrade silently
  degraded to a self-bind. Fix the unit / re-exec so `LISTEN_PID` names this process.
- The shard-owners listener mode returns `shard-owners mode is incompatible with systemd
  socket activation (LISTEN_FDS): it needs N distinct self-bound ports, but activation
  supplies one inherited socket for one port`. -> Do not combine systemd socket activation
  with the shard-owners listener mode; let IronCache bind its own ports. See
  `docs/UPGRADE.md` for the full rolling-upgrade + rollback procedure.

### Runtime backend fallback (`serve.rs`)
- WARN `runtime = io_uring requested with TLS on; the io_uring datapath does not support
  TLS in v1 -- falling back to the tokio backend for this node`.
- WARN `runtime = io_uring requested, but this build is not a Linux build with the
  `io_uring` feature; falling back to the tokio backend`. -> The io_uring request was
  ignored; the node runs on tokio. Expected on macOS / non-feature builds.

### Replication link (`crates/ironcache/src/replica_attach.rs`)
- ERROR `replica-attach: failed to bind repl listener` (fields `listen_addr`, `error`).
- WARN `replica-attach: repl source rejected a connection that failed the TLS/secret
  handshake` / `repl source TLS/secret handshake failed` / `repl dial TLS/secret handshake
  failed` -> a replica could not authenticate the replication link: the shared cluster
  secret or the TLS material differs between nodes.

### Metrics endpoint (`metrics_http.rs`)
- WARN `metrics: accept error; backing off` (field `error`) -- a transient accept error
  (e.g. EMFILE); the endpoint backs off and continues.
- ERROR `metrics: failed to build runtime; endpoint disabled` / `metrics: adopting listener
  failed; endpoint disabled` -> `/metrics` + probes are DOWN even though the data path is
  up; a livenessProbe on `/livez` would then restart a healthy node -- check the fd budget
  and the `--metrics-addr` value.

---

## Common scenarios

### High p99 latency
1. `redis-cli ... INFO stats` -- is `instantaneous_ops_per_sec` unusually high (load), or
   flat (so latency is per-command)?
2. `SLOWLOG GET 20` -- which commands crossed 10 ms? Look for O(N) commands (large `KEYS`,
   big-range `ZRANGE`, `SORT`, large multi-key ops).
3. `LATENCY LATEST` / `LATENCY DOCTOR` -- spike events for the `command` event.
4. Prometheus:
   `histogram_quantile(0.99, sum(rate(ironcache_command_duration_seconds_bucket[5m])) by (le))`
   for the trend; compare per-shard
   `ironcache_shard_command_duration_seconds_bucket{shard=...}` to isolate a
   [hot shard](#hot-shard).
5. `INFO memory` -- a high `mem_fragmentation_ratio` or eviction churn adds tail latency;
   see [OOM / eviction storm](#oom--eviction-storm).

### OOM / eviction storm
Symptom: clients get `-OOM command not allowed when used memory > 'maxmemory'.`, or
`evicted_keys` is climbing fast.
1. `INFO memory` -- `used_memory` vs `maxmemory`, and `maxmemory_policy`.
2. `INFO stats` -- `evicted_keys` rate; on Prometheus, `rate(ironcache_evicted_keys_total[1m])`.
3. If policy is `noeviction`, every write past the ceiling gets `-OOM` by design; switch to
   an eviction policy (`CONFIG SET maxmemory-policy allkeys-lru`) or raise the ceiling
   (`CONFIG SET maxmemory <bytes>`).
4. If policy already evicts but still OOMs, the working set exceeds `maxmemory` faster than
   eviction frees -- raise `maxmemory`, add shards/nodes, or shed load.
5. Check the boot log for the `load-on-boot left this shard OVER maxmemory` WARN: a restored
   snapshot larger than the ceiling starts you over budget.

### Hot shard
IronCache is shard-per-core and shared-nothing; one hot key or an uneven hash lands all
load on one shard.
1. Compare per-shard series:
   `ironcache_shard_commands_processed_total{shard=...}` and
   `ironcache_shard_connected_clients{shard=...}`. One shard far above the rest = hot shard.
2. Per-shard latency: `ironcache_shard_command_duration_seconds_bucket{shard=...}`.
3. A single hot KEY cannot be split by adding shards (it hashes to one slot); mitigate at
   the application (key sharding / client-side caching). Uneven load across many keys is
   helped by more shards/nodes.

### Connection exhaustion
Symptom: new clients get `-ERR max number of clients reached`; `rejected_connections`
climbs.
1. `INFO clients` -- `connected_clients` vs `maxclients`.
2. `CLIENT LIST` -- look for many idle connections from one `addr` (a client without
   pooling / a connection leak). `CLIENT KILL ADDR <ip:port>` to reclaim.
3. Raise the ceiling with `CONFIG SET maxclients <n>` -- BUT if the boot log shows the
   `maxclients clamped to fit the open-file limit (RLIMIT_NOFILE)` WARN, the fd budget is
   the real cap: raise `ulimit -n` / systemd `LimitNOFILE=` and restart, else the clamp
   re-applies.
4. Confirm the idle-connection `timeout` is set so dead peers are reaped.

### Hung or slow graceful shutdown
Symptom: `systemctl restart` / a k8s rollout takes a long time, or the supervisor SIGKILLs
the pod. Background: SIGTERM/SIGINT drive an ordered drain (stop admitting, refuse writes,
complete in-flight, drain connections, optional save-on-exit) -- see
`docs/design/SHUTDOWN.md`. #543 fixed a hang in this path.
1. In the log, find the last shutdown line: `ironcache: shutting down` then either
   `... -> exit 0` (clean) or the `second stop signal -> forcing immediate exit` WARN.
2. A slow stop is almost always the exit SAVE: a bare `SHUTDOWN` (and a signal-driven stop)
   saves iff a save point is configured. On a large keyspace the save can exceed the
   supervisor grace window. Raise Kubernetes `terminationGracePeriodSeconds` / systemd
   `TimeoutStopSec` to cover the worst-case save, or stop with `SHUTDOWN NOSAVE` when you do
   not need the exit snapshot.
3. If you must stop NOW, a second SIGTERM/SIGINT escalates to an immediate exit (the WARN
   above); this can truncate an in-flight save (non-zero exit).
4. `save-on-exit failed` ERROR at stop -> the save could not be written (see
   [Full-disk SAVE failure](#full-disk-save-failure)); the previous committed snapshot is
   still valid.

### Failed upgrade rollback / empty boot
Symptom: after a downgrade or a rolled-back `ironcache upgrade`, a node boots with an empty
keyspace, or refuses to boot.
1. Look for the ERROR `the on-disk snapshot has an unsupported format version and will NOT
   be loaded ...`. The on-disk dump was written by a NEWER binary than the one now booting.
2. The node started EMPTY (not corrupt) -- the newer dump is still on disk. Do NOT let it
   save over the dump: stop the node before the next save policy fires.
3. Roll FORWARD to the binary version that wrote the dump (the safe path), OR intentionally
   discard the newer dump if you accept the data loss.
4. To make this fail-closed in future, set `refuse_empty_start_on_version_mismatch = true`
   so the node refuses to boot (`refusing to boot: the on-disk snapshot has an unsupported
   format version ...`) instead of silently starting empty. `ironcache upgrade` itself is
   health-gated and auto-rolls-back (it watches `/readyz`, an `ironcache_uptime_seconds`
   reset, and a version match), so prefer it over a manual binary swap.

### NOAUTH loop
Symptom: a client's every command returns `-NOAUTH Authentication required.`.
1. The node has `requirepass`/ACL set and the client is not sending `AUTH` (or `HELLO ... AUTH`).
2. Confirm with a manual `redis-cli -a <pw> ... PING` -> `PONG`, then `ACL WHOAMI`.
3. Fix the client config (many libraries need the password in the connection URL / options,
   not a post-connect `AUTH`). If the node should NOT require auth, `CONFIG GET requirepass`
   and clear it deliberately.
4. `-WRONGPASS` instead of `-NOAUTH` means the client IS sending a credential but it is
   wrong / the user is disabled -- `ACL GETUSER <name>`.

### Cluster MOVED storm
Symptom: clients see constant `-MOVED <slot> <ip:port>` redirects and throughput drops.
1. A MOVED is normal once (the client updates its slot cache and retries). A STORM means the
   client is not caching the map, or the map is changing under it.
2. `CLUSTER SLOTS` / `CLUSTER NODES` (or `GET /topology`) on each node -- confirm they agree
   on ownership.
3. If ownership is unstable (a migration or a leadership flap), let it converge; `-ASK` /
   `-TRYAGAIN` during a migration are expected and transient.
4. If a slot is unowned you will see `-CLUSTERDOWN Hash slot not served` -- the map has a
   gap; a complete map is required.

### Lost raft quorum (no leader)
Symptom: `/readyz` returns `not ready: raft: no leader recognized`, and CLUSTER mutators
return `-CLUSTERDOWN <message>`.
1. `ironcache_raft_voters` and `ironcache_raft_is_leader` across nodes -- a majority must be
   up to elect a leader. With N voters you need floor(N/2)+1 alive.
2. `GET /topology` / `CLUSTER INFO` per node to see who is reachable.
3. Check the cluster-bus connectivity and the shared secret (a `repl ... handshake failed`
   WARN points at a secret/TLS mismatch that also blocks the bus).
4. Reads/writes on the data path continue where allowed, but config changes wait for a
   leader; restore the down voters to regain quorum.

### Full-disk SAVE failure
Symptom: `SAVE`/`BGSAVE` errors, the exit save logs `save-on-exit failed`, or
`rdb_last_save_time` stops advancing while `rdb_changes_since_last_save` climbs.
1. `df -h <data_dir mount>` -- a full or read-only volume is the usual cause.
2. `INFO persistence` -- `rdb_last_save_time` (staleness) and `rdb_changes_since_last_save`
   (unsaved dirty keys = your RPO exposure). On Prometheus,
   `ironcache_persistence_last_save_unixtime` and
   `ironcache_persistence_rdb_changes_since_save`.
3. The PREVIOUS committed snapshot stays valid on a failed save (fail-closed), so you do not
   lose the last good dump -- but new writes are not yet durable. Free space / fix the mount,
   then `BGSAVE` and confirm `LASTSAVE` advances.
4. `rdb_last_bgsave_status:ok|err` (INFO `# Persistence`, #549) is the explicit signal: it
   flips to `err` on a failed save. Corroborate with `rdb_last_save_time` staleness and the
   `save-on-exit failed` / BGSAVE log lines.

---

## Where to look next

- `DEPLOY.md` -- ports, config keys, the health/metrics endpoint, k8s/Helm probes, the
  persistence/RPO and Raft-formation sections.
- `docs/design/SHUTDOWN.md` -- the graceful-stop contract (SAVE-on-exit, grace timeouts,
  exit codes, the raft fresh-cluster-only refusal).
- `docs/design/OBSERVABILITY.md` -- the INFO/SLOWLOG/LATENCY parity and the metric registry.
- `docs/THREAT_MODEL.md` and `SECURITY.md` -- auth/TLS/ACL posture.
- The error catalog source of truth: `crates/ironcache-protocol/src/error.rs`.
