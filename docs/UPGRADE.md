# IronCache rolling-upgrade runbook (zero-downtime + rollback)

This is the operator procedure for replacing a running IronCache binary with a new
version WITHOUT dropping the working set or refusing client connections, and for
rolling back a bad one. It covers the two supported upgrade shapes:

- **Single node** via the systemd socket-activation handoff (#389 `LISTEN_FDS`, #390
  SAVE-to-durable-dir, #391 streamed restart, #388 lossless write-freeze). systemd holds
  the listening socket open across the restart, so clients queue in the kernel backlog
  instead of getting `ECONNREFUSED`.
- **HA cluster** (raft-governance mode) via a controlled replica failover (#392 / HA-8):
  promote an in-sync replica, upgrade the demoted node, repeat.

Every command, metric, `INFO` field, CLI flag, config key, and unit-file directive named
below was verified against the code; the source is cited inline and collected in
[Reference: verified against](#reference-verified-against) at the end. This document is a
peer of `docs/RUNBOOK.md` (symptom-to-action) and `DEPLOY.md` (install / config);
`docs/design/UPGRADE.md` is the mechanism DESIGN (verified-rollback swap, #83 / ADR-0020).

---

## Which procedure applies

| You run | Downtime lever | Procedure |
|---------|----------------|-----------|
| One node, no persistence | none available (data is in-memory) | [Single node](#single-node-rolling-upgrade); accept the cold working set, or add persistence first |
| One node + `data_dir` + `ironcache.socket` | socket-activation handoff | [Single node](#single-node-rolling-upgrade) -- zero refused connections, warm restart |
| A raft cluster (`cluster_mode = raft`) with replicas | replica failover | [HA cluster](#ha-cluster-rolling-upgrade) -- ownership moves off the node before you touch it |
| A Kubernetes StatefulSet (the Helm chart) | per-node availability gap (no-replica default), or out-of-band failover-freeze | [Kubernetes](#kubernetes-helm-rolling-upgrades) -- the native rollout has no failover-before-drain hook |

`cluster_mode` and `data_dir` are config keys (`Config::cluster_mode` / `Config::data_dir`
in `crates/ironcache-config/src/lib.rs`; TOML `cluster_mode = "raft"` / `data_dir = "..."`,
or `IRONCACHE_CLUSTER_MODE` / `IRONCACHE_DATA_DIR`). Confirm your posture with
`ironcache check` (below) and `CLUSTER INFO`.

---

## Pre-flight checks (run before every upgrade)

1. **Config is valid on the new binary.** Run the new binary's config self-check (the
   nginx `-t` analogue). It resolves + validates the effective config and prints it
   WITHOUT binding a port; a malformed `maxmemory`, a bad policy, or an unresolvable
   overlay fails HERE instead of at boot
   (`cmd_check` in `crates/ironcache/src/main.rs`):

   ```sh
   ironcache check
   # ironcache check: configuration OK
   #   bind        = 127.0.0.1:6379
   #   shards      = 8
   #   runtime     = tokio
   #   databases   = 16
   #   persist-cpu = off (no pin)
   #   maxmemory   = 0 bytes (unlimited)
   #   policy      = allkeys-lru
   #   requirepass = set
   #   tls         = off (plaintext)
   #   allocator   = jemalloc (background_thread=true, dirty_decay_ms=5000)
   ```

2. **The last snapshot committed cleanly.** In a persisted deployment, confirm the most
   recent background save succeeded before you swap, so a warm restart reloads a good
   dump. `INFO persistence` reports `rdb_last_bgsave_status:ok` (`err` after a failed
   save, e.g. a full disk), plus `rdb_last_save_time` and `rdb_changes_since_last_save`
   (`push_persistence_section` in `crates/ironcache-observe/src/lib.rs`; #549). Force a
   fresh, known-good snapshot immediately before the swap:

   ```sh
   redis-cli SAVE        # blocking; or BGSAVE for background
   redis-cli LASTSAVE    # watch this unix time advance
   redis-cli INFO persistence | grep -E 'rdb_last_bgsave_status|rdb_last_save_time'
   ```

   (`SAVE` / `BGSAVE` / `LASTSAVE` are verified in the dispatch table, `docs/RUNBOOK.md`
   "Persistence + lifecycle".) `ironcache upgrade` also does a SAVE-first itself, but a
   green pre-flight status means the swap does not depend on that final save being the
   first successful one.

3. **Replication is healthy (HA only).** Every replica you are relying on for failover
   must have its link up and its lag inside the promotion bound, or a `CLUSTER FAILOVER`
   will refuse (see [HA cluster](#ha-cluster-rolling-upgrade)). Check per node:

   ```sh
   redis-cli INFO replication      # master_link_status:up ; slave_repl_offset near master_repl_offset
   ```

   or scrape `/metrics`: `ironcache_replication_link_up` must be `1` and
   `ironcache_replication_lag_offset` small (`render_prometheus` in
   `crates/ironcache-observe/src/lib.rs`; #549). The promotion lag bound is
   `replica_max_lag` (`Config::replica_max_lag`, default `DEFAULT_REPLICA_MAX_LAG` = 256,
   `crates/ironcache-config/src/lib.rs`).

4. **The ops endpoint is reachable.** `ironcache upgrade`'s health gate probes `/readyz`
   and the RESP `PING`; the packaged unit exposes the endpoint on `127.0.0.1:9121`
   (`packaging/ironcache.service` `ExecStart ... --metrics-addr 127.0.0.1:9121`). Confirm
   it answers now:

   ```sh
   curl -fsS http://127.0.0.1:9121/readyz && echo   # expect: ready
   ```

   > PORT MISMATCH TRAP: `ironcache upgrade`'s `--readyz-addr` DEFAULTS to
   > `127.0.0.1:9121` -- the PACKAGED unit's `--metrics-addr` -- but a server booted
   > WITHOUT an explicit `--metrics-addr` serves the ops endpoint on `127.0.0.1:9091`
   > (`DEFAULT_METRICS_ADDR` in `crates/ironcache/src/cli.rs`). On such a node the health
   > gate can never turn green and a HEALTHY upgrade would be auto-rolled-back at the
   > `--health-timeout`; pass `--readyz-addr 127.0.0.1:9091` (or whatever your unit binds).

---

## Single node rolling upgrade

### How the handoff works

With socket activation enabled, **systemd** owns the RESP listening socket, not the
server. `packaging/ironcache.socket` opens `ListenStream=127.0.0.1:6379` with
`Backlog=1024` and `ReusePort=false` and hands the fd to `ironcache.service` via the
`sd_listen_fds` protocol (`LISTEN_FDS` / `LISTEN_PID`). The server ADOPTS that fd instead
of self-binding (`listener_for` -> `adopt_listener_fd` in
`crates/ironcache-runtime/src/tokio_rt.rs`). Because the single listen queue is never closed across the restart,
in-flight and new connections QUEUE in the kernel backlog during the brief swap window
rather than being refused (`packaging/ironcache.socket` header). This beats
`SO_REUSEPORT` for upgrades: a closed reuseport socket loses its queued connections; this
one does not.

The boot LOUDLY states which listener path it took, so you can confirm the handoff from
the logs (#562):

```
INFO socket-activation: ADOPTED 1 systemd socket-activation listening fd(s) [resp=fd3]; systemd owns the listen queue, so it survives an upgrade restart with no connection-refused window
```

If activation was not in effect you instead see `FELL BACK to self-binding its own
listener: not socket-activated (no LISTEN_FDS in the environment)`, and a rejected
activation environment (a foreign `LISTEN_PID`, a malformed count) logs at WARN naming the
reason (`crates/ironcache/src/sockact_log.rs`; the classification is
`classify` in `crates/ironcache-runtime/src/listen_fds.rs`). The packaged
`ironcache.socket` sets `FileDescriptorName=resp`, and adoption selects the fd NAMED
`resp` when `LISTEN_FDNAMES` disambiguates a multi-socket activation (falling back to the
first passed fd on an unnamed single-socket unit -- `resp_listener_fd` in
`crates/ironcache-runtime/src/listen_fds.rs`); that name is what the `[resp=fd3]` above
shows. Naming the fd future-proofs a second `[Socket]` (e.g. a replication listener as
`FileDescriptorName=repl`) without positional ambiguity.

### One-time setup (persisted, socket-activated)

```sh
# Enable durable snapshots so the restart reloads the working set (edit the config):
#   data_dir = "/var/lib/ironcache"          # (or IRONCACHE_DATA_DIR)
#   save_interval_secs = 900                  # a save point, so SHUTDOWN/upgrade saves
#   save_min_changes   = 1
# then install both units and enable the socket FIRST:
install -m0644 packaging/ironcache.socket  /etc/systemd/system/ironcache.socket
install -m0644 packaging/ironcache.service /etc/systemd/system/ironcache.service
systemctl daemon-reload
systemctl enable --now ironcache.socket
systemctl enable --now ironcache.service
```

`save_interval_secs` / `save_min_changes` are config keys
(`crates/ironcache-config/src/lib.rs`; TOML or `IRONCACHE_SAVE_INTERVAL_SECS` /
`IRONCACHE_SAVE_MIN_CHANGES`). Without a save point, `SHUTDOWN` and the upgrade restart do
NOT save (NOSAVE posture) and the restart comes back cold.

### The upgrade

`ironcache upgrade` is the single operator command (verified: `UpgradeArgs` in
`crates/ironcache/src/cli.rs`, `cmd_upgrade` in `crates/ironcache/src/main.rs`). It performs the whole
swap safely: sha256 INTEGRITY of the new artifact (minisign AUTHENTICITY once a public key
is pinned, #386), a lossless write-freeze (node-wide `CLIENT PAUSE WRITE`, #388) then a
final SAVE, an atomic never-absent binary swap keeping one `.old` slot, a systemd restart
that adopts the socket-activation fd, a health gate, and AUTO-ROLLBACK on any miss.

```sh
# From a locally staged artifact + its release checksum manifest:
ironcache upgrade \
  --binary  /tmp/ironcache-new \
  --sha256sums /tmp/SHA256SUMS \
  --target  /usr/local/bin/ironcache \
  --unit    ironcache \
  --readyz-addr 127.0.0.1:9121 \
  --resp-addr   127.0.0.1:6379 \
  --health-timeout 30
```

Verified flags and defaults (`UpgradeArgs` in `crates/ironcache/src/cli.rs`):

| Flag | Default | Purpose |
|------|---------|---------|
| `--binary` + `--sha256sums` | -- | LOCAL source + its release checksum manifest |
| `--from-url` + `--sums-url` | -- | REMOTE tarball + its `SHA256SUMS` URL (#394) |
| `--to <TAG\|latest>` `--repo <owner/repo>` | repo `ELares/IronCache` | fetch a GitHub release tag |
| `--target` | `/usr/local/bin/ironcache` | live binary path to swap onto (`.new`/`.old` live beside it) |
| `--unit` | `ironcache` | systemd unit to restart |
| `--readyz-addr` | `127.0.0.1:9121` | ops endpoint the health gate probes `/readyz` on |
| `--resp-addr` | `127.0.0.1:6379` | RESP addr for the SAVE-first + `PING` probe |
| `--auth-file` | -- | file holding `requirepass` (kept out of argv/logs) |
| `--health-timeout` | `30` (s) | how long to wait for the restarted server before failing/rolling back |
| `--no-rollback` | off | leave the new binary in place on a failed gate (debug in situ) |
| `--yes` | off | skip the confirm prompt; also allow persistence-off + same-version |
| `--allow-same` | off | permit re-installing the SAME version without `--yes` |
| `--no-freeze` | off | skip the `CLIENT PAUSE WRITE` freeze (SAVE-first only) |

Exactly one source (`--binary`, `--from-url`, or `--to`) must be given. If persistence is
NOT configured, `ironcache upgrade` refuses unless `--yes` (you are accepting a cold
restart).

> TAG SHAPE: `--to` takes the git TAG, and a FORMAL release tag includes the leading `v`
> (`--to v1.2.3`, NOT `--to 1.2.3`) even though `ironcache --version` prints the bare
> `1.2.3`; a rolling build is tagged by its calendar version (e.g. `--to 2026.0701.1`),
> and `--to latest` follows the `releases/latest` redirect to the newest rolling build
> (the `--to` doc on `UpgradeArgs` in `crates/ironcache/src/cli.rs`).

### In-server streamed live cutover (SIGUSR1, #391/#638) -- opt-in via `handoff_socket`

The default upgrade above SWAPS the binary and RESTARTS the process (the socket-activation
handoff keeps the listen queue open across the brief restart). An alternative shape keeps
the OLD process SERVING while it streams its live keyspace to a freshly spawned sibling
(a re-exec of the server binary in receiver role) and flips write authority at a single
committed linearization point -- no restart, no acknowledged-write loss, and no
orphaned-backlog RST because the sibling INHERITS the OLD's client listen fd. Both the
commit path and the abort path are verified end to end by the real two-process acceptance
tests (`crates/ironcache/tests/upgrade_streamed_sigusr1.rs` asserts a COMPLETED commit
with zero acked-write loss, no RST, a sub-second write stall, and OLD `exit(0)`;
`crates/ironcache/tests/upgrade_streamed_cutover.rs` drives the orchestrator primitives;
both are Linux-gated `#[ignore]` acceptance runs, not default-CI gates). The receiver boot
path drives the full commit protocol (`receive_shard_into` in
`crates/ironcache/src/coordinator.rs`: bulk -> `BulkStaged` -> delta -> `Prepared` ->
await `Commit`/`Abort` -> `Served`; only a COMMITTED shard is ever installed).

**Prerequisites:**

- `handoff_socket` is configured on the running server (TOML `handoff_socket = "..."` or
  `IRONCACHE_HANDOFF_SOCKET`; `Config::handoff_socket` in
  `crates/ironcache-config/src/lib.rs`) -- a node-local AF_UNIX rendezvous path both the
  OLD and the sibling agree on. Without it, SIGUSR1 is logged and IGNORED (the server
  keeps serving). This is the whole opt-in gate (`HandoffPlan::from_config` in
  `crates/ironcache/src/upgrade/drive.rs`).
- `data_dir` is OPTIONAL: when configured, each committed shard's post-cutover state is
  durably published to `dump-shard-<n>.icss` BEFORE the OLD exits (so a crash of the NEW
  right after the cutover cannot lose the adopted keyspace); without it the adopt is
  in-memory-only, matching a non-persistent node's steady state (`receive_shard_into`).
- Unix only (the handoff rides an AF_UNIX socket + fork/exec), and the single-listener
  default acceptor; the shard-owners N-listener fd-array inherit is a documented follow-up
  (`run_cutover_host` in `crates/ironcache/src/main.rs` passes the FIRST client listener fd).

**The trigger** is **SIGUSR1** to the running server pid (`wait_for_signal` ->
`SignalOutcome::Cutover` in `crates/ironcache/src/serve_signal.rs`; `drive_cutover` in
`crates/ironcache/src/main.rs`). A plain `SIGTERM`/`SIGINT` still does the unchanged
graceful stop:

```sh
# the running server must have handoff_socket configured; then:
kill -USR1 "$(redis-cli INFO server | sed -n 's/^process_id://p' | tr -d '\r')"
```

**What COMMIT looks like** (log lines verbatim from `drive_cutover` in `main.rs`):

```
INFO ironcache: streamed cutover COMMITTED; the new sibling now serves on the inherited listener. Draining in-flight connections and exiting.
```

The sibling serves on the SAME port (it adopted the inherited listen fd) and the OLD
drains briefly and `exit(0)`s -- the commit exit uses the SHORT cutover drain grace and
SKIPS the redundant save-on-exit (the NEW already durably promoted the state), so the
client-visible write stall is SUB-SECOND (asserted by the acceptance test via the
`ironcache-env` clock seam). A client that sees `-LOADING` or a closed connection during
the flip reconnects and lands on the NEW.

**What ABORT looks like** -- fail-safe toward keep-serving; the OLD never exits and writes
resume:

```
WARN ironcache: streamed cutover aborted; the OLD keeps serving
WARN ironcache: streamed cutover did not commit; resuming service (the OLD keeps serving). Waiting for the next signal.
```

A host-side error (rather than a clean abort) logs at ERROR:
`ironcache: streamed cutover ended without a confirmed commit (degraded standby, W3); NOT
exiting. Operator recovery: restart the OLD or the NEW.` And with no `handoff_socket`
configured the signal is a no-op:
`ironcache: SIGUSR1 cutover requested but no handoff_socket is configured; ignoring (the
server keeps serving)`.

**CLI selection (no `--streamed` flag exists).** On a node whose config has
`handoff_socket` set, `ironcache upgrade` AUTO-SELECTS the streamed path: it validates the
streamed configuration and reports `streamed live-cutover selected: handoff socket ...`,
and deliberately does NOT run the default destructive swap+restart (which would kill the
live process the cutover hands off from) -- see `cmd_upgrade` -> `cmd_upgrade_streamed` in
`crates/ironcache/src/main.rs`. The actual trigger remains the `kill -USR1` above, sent to
the running server. Corollary: a node with `handoff_socket` configured cannot be driven
through the default tmpfs swap+restart via `ironcache upgrade` on that config.

**Composition with socket activation.** The sibling adopts the inherited fd through the
SAME `adopt_listener_fd` path the systemd socket-activation boot uses; the orchestrator
passes it at a well-known fd via `IRONCACHE_HANDOFF_LISTEN_FD`, which `listener_for`
checks BEFORE the `LISTEN_FDS` and self-bind paths (`crates/ironcache-runtime/src/tokio_rt.rs`).
So the never-closed-listener guarantee holds whether the OLD self-bound its listener or
adopted a systemd socket-activation fd -- in the latter case the inherited duplicate IS the
systemd-owned socket, and the listen queue is still never closed. A default boot (no
handoff env) is byte-unchanged.

---

## HA cluster rolling upgrade

In a raft-governance cluster (`cluster_mode = raft`) one node OWNS a set of slots (serves
reads and writes) and others may be committed as its REPLICAS, mirroring the owner and
serving READONLY reads (`crates/ironcache/src/replica_attach.rs`). Replica assignment and
promotion go through the Raft control plane, NOT a `REPLICAOF`/`SLAVEOF` command (those do
not exist in IronCache). The operator levers are the raft-mode `CLUSTER` mutators, handled
by `try_raft_cluster_mutator` (`crates/ironcache/src/serve.rs`):

- `CLUSTER REPLICATE <node-id> <slot> [slot ...]` assigns a node as a replica of the
  listed slots (commits `AssignReplica`; `build_replicate` in `serve.rs`).
- `CLUSTER FAILOVER` promotes THIS in-sync replica to owner of the slots it replicates
  (commits `PromoteReplica`, which atomically transfers ownership and bumps the config
  epoch; `build_failover` in `serve.rs`).

The strategy: **move ownership off a node before you upgrade it.**

You can drive that whole sequence AUTOMATICALLY with `ironcache upgrade --cluster`
([Automated](#automated-ironcache-upgrade---cluster)), or perform it BY HAND with the
per-node commands ([Manual](#1-confirm-the-topology-and-write-safety)). The automated path
runs the same steps the manual sections describe, with the added failover-freeze fence.

### Automated: `ironcache upgrade --cluster`

`ironcache upgrade --cluster` runs the whole roll from ONE orchestrator invocation: it
discovers the live topology, upgrades the replicas first (each via the single-node flow on
its host), promotes an upgraded in-sync replica, then upgrades the old primary LAST. The
dynamic topology (roles, versions, lag, membership) is read live over the authenticated
RESP surface; a small static TOML **inventory** supplies what cannot be discovered: how to
reach each node (`resp_addr` + optional `auth`) and how to actuate its out-of-band binary
swap (`ssh_target` + `upgrade_source`), plus the observe `seeds`.

**Safety model (RPO 0).** Before each promotion the driver applies a *failover-freeze*
fence: `CLIENT PAUSE WRITE` on the OLD primary (no further write is acknowledged), drain
the chosen candidate's master-side lag to EXACTLY 0, and only THEN `CLUSTER FAILOVER`. If
the drain does not reach 0 within `--drain-timeout`, it FAILS CLOSED (unpause, no
promotion). So freeze-drain-failover loses **zero acknowledged writes** (RPO 0), and the
primary is always upgraded last. Only one node is ever down at a time, so reads stay served
throughout.

**Inventory format** (TOML; all values below are PLACEHOLDERS -- use your own hosts):

```toml
# Which node(s) to CLUSTER-observe from (the seed discovery order). Each must name a
# [[node]] id; at least one is required.
seeds = ["node-a"]

[[node]]
id             = "node-a"                 # announce id (the promotion / CLUSTER FAILOVER target)
resp_addr      = "10.0.0.1:6379"          # authenticated RESP host:port (INFO / CLUSTER / PAUSE)
auth           = "REQUIREPASS"            # optional requirepass (sent only over the RESP socket)
ssh_target     = "deploy@node-a.example"  # opaque ssh target (user@host or an ssh alias)
upgrade_source = "--to v1.2.3"            # the per-node `ironcache upgrade` source args

[[node]]
id             = "node-b"
resp_addr      = "10.0.0.2:6379"
ssh_target     = "deploy@node-b.example"
upgrade_source = "--to v1.2.3"

[[node]]
id             = "node-c"
resp_addr      = "10.0.0.3:6379"
ssh_target     = "deploy@node-c.example"
upgrade_source = "--to v1.2.3"
```

The inventory is validated fail-closed before any node is touched: it must be well-formed
TOML with at least one `[[node]]`, unique non-empty ids, well-formed `host:port` addresses,
and at least one seed that names a declared node. An unknown key or a bad address is a clear
error, not a silent default.

**Preview first, then run.** `--dry-run` OBSERVES the cluster once and prints the derived
plan (current versions, the replica roll order, the promotion candidate, the primary
upgraded LAST) then EXITS, taking NO action -- confirm primary-last before committing:

```sh
# Preview the plan (no upgrade, no failover):
ironcache upgrade --cluster --inventory cluster.toml --to v1.2.3 --dry-run

# Execute the roll:
ironcache upgrade --cluster --inventory cluster.toml --to v1.2.3
```

Both `--inventory <FILE>` and `--to <TAG>` are REQUIRED in cluster mode (dev / lock builds
pin a constant version, so the target cannot be inferred). The per-node local flags
(`--target`, `--unit`, `--resp-addr`, `--readyz-addr`, `--auth-file`) do NOT apply on the
orchestrator; each node's reach + actuation comes from the inventory.

Cluster-mode tuning flags (sensible defaults; match the driver / server defaults):

| Flag | Default | Purpose |
|------|---------|---------|
| `--inventory <FILE>` | -- (required) | the static actuation-map TOML |
| `--to <TAG>` | -- (required) | the explicit target version to roll to |
| `--max-lag <N>` | server `replica_max_lag` (256) | promotion candidate pre-filter (the freeze drains to lag 0 regardless) |
| `--drain-timeout <SECS>` | `60` | failover-freeze drain bound before failing closed |
| `--pause-ms <MS>` | `30000` | the `CLIENT PAUSE WRITE` window on the old primary during a promotion |
| `--per-node-timeout <SECS>` | `30` | bound on each per-node RESP exchange |
| `--max-ticks <N>` | `300` | tick budget before failing loud (`StalledAfterBudget`) instead of looping |
| `--dry-run` | off | observe once, print the plan, take no action |
| `--actuator-command <TEMPLATE>` | off (SSH) | actuate each node's swap by running a LOCAL command instead of SSH |

**Actuation: SSH by default, or a local command.** By default the driver actuates each
node's out-of-band binary swap over SSH (`ssh <ssh_target> ironcache upgrade <upgrade_source>
--yes`), so the node's own hardened single-node upgrade (verify -> SAVE -> swap -> restart ->
health-gate -> auto-rollback) runs on the node host. For deployments that actuate through a
container orchestrator, `systemd`, or config-management rather than an interactive SSH login,
`--actuator-command '<TEMPLATE>'` runs a local command per node instead, with `{id}` /
`{source}` / `{target}` replaced by that node's inventory `id` / `upgrade_source` /
`ssh_target`. The command is run directly (no shell), must exit 0 only once the node is up on
the new binary, and the orchestration (observe -> replicas first -> failover-freeze -> primary
last) is identical to the SSH path. For example, to roll a docker-composed cluster:

```sh
ironcache upgrade --cluster --inventory cluster.toml --to v1.2.3 \
  --actuator-command 'docker compose up -d --force-recreate --wait {id}'
```

On a stall or an action error the driver exits NONZERO and names the blocking step (no
quorum, no in-sync candidate, or a node upgrade that did not complete), fail-closed. The
manual sections below describe the same mechanism step by step (and are the fallback if you
prefer to drive it yourself).

### 1. Confirm the topology and write-safety

```sh
redis-cli CLUSTER INFO        # cluster_enabled:1, state ok
redis-cli CLUSTER NODES       # who owns what, who replicates what
redis-cli CLUSTER SHARDS
```

For an upgrade that must not lose an acknowledged write to the async-replication window,
require replica acknowledgement on the owner BEFORE you start
(`Config::min_replicas_to_write` in `crates/ironcache-config/src/lib.rs`, default 0 =
disabled):

```
min_replicas_to_write = 1     # owner rejects writes with -NOREPLICAS when under-replicated
min_replicas_max_lag  = 10    # in-sync bound, in LOGICAL WRITE OFFSETS (the default,
                              # DEFAULT_MIN_REPLICAS_MAX_LAG = 10)
```

`min_replicas_max_lag` counts LOGICAL WRITES (the same offset unit as
`ironcache_replication_lag_offset`), not seconds; a replica lagging past it stops counting
toward `min_replicas_to_write` (`Config::min_replicas_max_lag`, default
`DEFAULT_MIN_REPLICAS_MAX_LAG` = 10).

### 2. Upgrade the replicas first

A replica restart does not move ownership; the owner keeps serving throughout. Upgrade
each replica node one at a time with the single-node [`ironcache upgrade`](#the-upgrade)
flow. After each one comes back, confirm it re-attached before touching the next:

```sh
redis-cli INFO replication | grep -E 'role|master_link_status|slave_repl_offset'
# role:replica ; master_link_status:up ; slave_repl_offset catching up to master_repl_offset
```

`master_link_status` flips `up`/`down` in `push_replication_section`
(`crates/ironcache-observe/src/lib.rs`); the same signal is
`ironcache_replication_link_up` on `/metrics`.

### 3. Promote a replica, then upgrade the old owner

When only the owner is left on the old version, promote a healthy, in-sync replica so the
owner role moves off the node you are about to restart. Run ON the replica you want to
become owner:

```sh
redis-cli -h <replica-host> -p <replica-port> CLUSTER FAILOVER
```

`CLUSTER FAILOVER` REFUSES (so you cannot promote an unsafe node) when
(`build_failover` in `crates/ironcache/src/serve.rs`):

- the node is not an in-sync replica (not a replica, its link is down, or its lag exceeds
  the bound) -> `CLUSTER FAILOVER refused: this node is not an in-sync replica ...`;
- there is no cluster slot map -> `CLUSTER FAILOVER requires cluster mode with a slot map`;
- it replicates no slots -> `CLUSTER FAILOVER refused: this node replicates no slots to
  take over`;
- you passed `FORCE`/`TAKEOVER` (those bypass the safety gates and are unsupported) ->
  use a bare `CLUSTER FAILOVER`.

On a single-node cluster `CLUSTER FAILOVER` / `CLUSTER REPLICATE` return
`... is not supported on a single-node cluster` (`crates/ironcache-server/src/cmd_cluster.rs`).

After the promotion commits, ownership has transferred (the config epoch bumped). Now
upgrade the demoted old owner as an ordinary node with the single-node flow, and verify it
rejoins (`CLUSTER NODES`, `/readyz` green).

### Unplanned failover (what happens if a node just dies)

If a node is stopped without a controlled `CLUSTER FAILOVER`, its replicas detect the
master link down and, after `failover_timeout_secs` of CONTINUOUS down-time, an in-sync
replica SELF-proposes its own promotion through Raft (HA-8;
`Config::failover_timeout_secs` in `crates/ironcache-config/src/lib.rs`, default
`DEFAULT_FAILOVER_TIMEOUT_SECS` = 5 s). A replica is only promotable while its link was up
and its lag was `<= replica_max_lag` (`Config::replica_max_lag`, default
`DEFAULT_REPLICA_MAX_LAG` = 256), so a stale replica is never promoted. The controlled `CLUSTER FAILOVER` is
preferred for upgrades because it moves ownership with no down-timeout window and no
write-rejection blip.

### How the clustered driver is verified (#392)

The rolling-upgrade driver's guarantees are checked across layers, because an in-process
loopback harness cannot deterministically drive a real committed failover, and booting a
real raft cluster is load-sensitive (flaky as a hard CI gate). The split
(`crates/ironcache/tests/cluster_upgrade_live.rs`):

- **Always-on CI gate -- the FREEZE** (`freeze_seam_holds_a_real_write`): on a MINIMAL
  single-node server (no raft, no formation, so deterministic) the driver's exact freeze
  seam (`CLIENT PAUSE <ms> WRITE` via the shipped `Pauser`) is shown to HOLD a live
  concurrent write (no `+OK`) until `CLIENT UNPAUSE`, then release + apply it. This is the
  reliable hard gate for the load-bearing RPO=0 mechanism the failover-freeze fence uses.
- **On-demand full live acceptance** (`live_cluster_upgrade_acceptance`, `#[ignore]`):
  against a REAL 3-node raft cluster it proves real OBSERVE (the `INFO` / `CLUSTER INFO`
  parse assembles a correct cluster view) and primary-last SEQUENCING (both replicas
  upgraded before the primary, exactly one failover, `old_primary_id` fixed, terminates
  completed; the promotion EFFECT is made controllable since a loopback self-promotion is
  not deterministic). It PASSES when run but is gated off the default CI run (load-sensitive
  formation timing); run with `cargo test -- --ignored` / nightly.
- **DST promotion correctness**: the committed-promotion semantics (at most one owner per
  epoch across partition/heal timelines) are proven exhaustively by the deterministic
  split-brain gate `ironcache_raft::tests::failover_split_brain_gate`, not on a live cluster.
- **Docker smoke (local, not CI)**: a real committed promotion under sustained traffic plus
  the adversarial no-freeze control (which must SHOW acked-write loss when the freeze is
  disabled) are a docker-harness follow-up that exercises the real RESP transport and the
  real binary swap end to end.

---

## Kubernetes (Helm) rolling upgrades

A `helm upgrade` that changes `image.tag` triggers a **StatefulSet RollingUpdate**: the
controller deletes and recreates pods one at a time, highest ordinal first, gated by the
readiness probe + `minReadySeconds` (see `deploy/SCALING.md` for why the readiness gate --
NOT the PodDisruptionBudget -- is what paces the roll). **The native rollout has no
failover-before-drain hook**, so what an image bump costs a slot-owner depends entirely on
whether that slot has an in-sync replica.

### Case A -- the default chart (no per-slot replicas)

Out of the box the chart's static topology assigns each of the `replicas` nodes a slot range
as its **sole owner, with no replica** (`cluster.minReplicasToWrite: 0`; replicas are a
runtime-only, opt-in thing -- Case B). So there is **nothing to fail over to**: when the
rollout deletes a primary pod, its slots are **unavailable for the duration of that pod's
graceful restart** -- SIGTERM save-on-exit, pod recreated with the new image, snapshot +
raft-log reload from the **retained** PVC, then `/readyz` passes.

- **RPO = 0.** The save-on-exit persists the working set and the PVC is retained across the
  pod recreate (`persistentVolumeClaimRetentionPolicy`), so the node reloads exactly what it
  had. No data is lost.
- **Availability: a per-node gap.** Each node's slots are down for its restart window. The
  readiness gate + one-at-a-time ordinal serialization confine it to one node at a time; size
  `startupProbe.failureThreshold * periodSeconds` and `terminationGracePeriodSeconds` to your
  worst-case reload so a healthy node is not CrashLooped or SIGKILLed mid-save.

This is **safe** (no data loss) but **not zero-downtime** for the slots on the pod being
rolled. For many caches that is acceptable; if it is not, use Case B.

### Case B -- a replicated (HA) cluster

If you have assigned in-sync replicas (`CLUSTER REPLICATE <node-id> <slot>...` puts a replica
of a slot on another node -- see [HA cluster](#ha-cluster-rolling-upgrade)), a slot can fail
over instead of going dark. But note **how** it fails over under a bare `helm upgrade`:

- **Bare rollout = UNPLANNED failover.** Deleting a primary pod is an ungraceful owner loss
  from the cluster's view: an in-sync replica self-proposes promotion only after
  `failover_timeout_secs` of continuous downtime (default 5 s), so those slots see a brief
  write-rejection blip + the down-timeout window. RPO is bounded by the replica's lag
  (`replica_max_lag`, default 256) and is 0 for the graceful save-on-exit case, but the blip
  is client-visible.
- **Controlled failover = no blip, RPO = 0.** The `CLUSTER FAILOVER` fence moves ownership to
  a caught-up replica *before* the old primary is touched, with no down-timeout window. That
  is exactly what the `ironcache upgrade --cluster` driver orchestrates (pause writes -> drain
  the candidate to lag 0 -> `CLUSTER FAILOVER` -> commit; fail-closed on drain timeout). See
  [HA cluster rolling upgrade](#ha-cluster-rolling-upgrade).

**The gap on Kubernetes:** the native StatefulSet rollout does not invoke the controlled
fence, and the chart's `preStop` is a **lame-duck sleep only** (it deprograms the Service
endpoint; it does not fail over slots). A pod's `preStop` also *cannot* safely run the fence
itself: there is no self-failover command -- a controlled handoff has to identify the Raft
leader, poll a candidate's lag to 0 cluster-wide, and `CLUSTER FAILOVER` the *candidate*, none
of which a single terminating node can drive within its grace window. So for a zero-downtime
upgrade of a replicated cluster you must run the controlled failover **out of band**:

1. Before touching a primary, move its ownership to an upgraded, in-sync replica with the
   `ironcache upgrade --cluster` driver or the [manual](#3-promote-a-replica-then-upgrade-the-old-owner)
   `CLUSTER FAILOVER` sequence. On Kubernetes this means driving the roll from the driver
   (e.g. an `--actuator-command` that recreates each pod in order) rather than a bare
   `helm upgrade`, or performing the failover by hand and then letting the rollout proceed.
2. Automating failover-before-drain on *every* pod deletion (so a bare `helm upgrade` becomes
   safe) requires a controller reconciling cluster state -- it is out of scope for a
   declarative chart and is part of the planned operator (see `deploy/K8S_READINESS_PLAN.md`).

### RPO knob

`cluster.minReplicasToWrite` (`min_replicas_to_write`) is 0 by default, so a write is
acknowledged without waiting for any replica. With the default no-replica topology (Case A)
it MUST stay 0 -- `>= 1` would fail every write `-NOREPLICAS`. Once you have assigned runtime
replicas (Case B), raising it to `>= 1` makes an acknowledged write survive that many node
losses (bounding RPO under an ungraceful failover), at a write-latency cost.

---

## Verify the new version took over

After any upgrade, confirm the NEW binary is the one serving:

1. **Version.** `INFO server` reports `ironcache_version:<real>` (the load-bearing field,
   from the build's `CARGO_PKG_VERSION`) alongside a fixed `redis_version:7.4.0`
   compatibility tag (`build_info` in `crates/ironcache-observe/src/lib.rs`). Check the REAL one:

   ```sh
   redis-cli INFO server | grep -E 'ironcache_version|redis_version|uptime_in_seconds'
   ```

   `uptime_in_seconds` (and the `ironcache_uptime_seconds` gauge on `/metrics`,
   `render_prometheus` in `crates/ironcache-observe/src/lib.rs`) resetting toward zero
   PROVES a real restart happened. (A COMMITTED streamed cutover also resets it: the
   sibling is a new process.)

2. **Readiness.** `/readyz` returns `200` only once load-on-boot finished for every shard
   AND (in raft mode) a leader is recognized; otherwise `503` with
   `not ready: load-on-boot incomplete` or `not ready: raft: no leader recognized`
   (`ReadyState` in `crates/ironcache/src/metrics_http.rs`). Liveness is `/livez`; both plus
   `/metrics` are served on the ops endpoint (boot log `metrics: serving /metrics, /livez,
   /readyz`).

   ```sh
   curl -fsS http://127.0.0.1:9121/readyz && echo    # ready
   ```

3. **Health metrics stayed healthy.** The command-latency histogram
   `ironcache_command_duration_seconds` (#546, buckets `0.000025 .. 10, +Inf`) and the
   cross-shard hop counters `ironcache_hops_sent_total` / `ironcache_hops_served_total` /
   `ironcache_local_served_total` plus the `ironcache_inbox_depth` gauge (#556) should
   return to their pre-upgrade shape (`render_latency_histogram`, `render_prometheus`, and
   `render_inbox_depth` in `crates/ironcache-observe/src/lib.rs`). A p99 query:

   ```
   histogram_quantile(0.99, sum(rate(ironcache_command_duration_seconds_bucket[5m])) by (le))
   ```

   A hop-counter or inbox-depth jump that does not settle, or a p99 that stays elevated,
   is a sign the new binary is not behaving; consider rolling back.

4. **Persistence + replication (as applicable).** `rdb_last_bgsave_status:ok`, and on a
   replica `master_link_status:up` with `slave_repl_offset` catching up.

---

## Rollback

### Automatic (the default)

`ironcache upgrade` is the primary rollback path: it health-gates the restarted server for
`--health-timeout` seconds and, on ANY miss (process down, `/readyz` never green, `PING`
fails, or the reported version is not exactly the requested target), it AUTO-RESTORES the
retained `.old` binary, restarts, and re-probes -- with no operator action, so a bad build
never strands the node (`docs/design/UPGRADE.md` "Auto-rollback on any miss";
`UpgradeArgs::no_rollback` in `crates/ironcache/src/cli.rs` to opt OUT). The state directory is
never written during a rollback; the working set comes back through the normal load path.

### Manual

To go back deliberately, run the upgrade IN REVERSE toward the previous artifact:

```sh
ironcache upgrade --binary /path/to/OLD-ironcache --sha256sums /path/to/SHA256SUMS \
  --allow-same --yes
```

or restore the `ironcache.old` slot beside the target and `systemctl restart ironcache`.
For a persisted node, the working set reloads from the last committed snapshot.

### Downgrade / snapshot-version mismatch (#530, fail-closed)

A downgrade (or a rollback to a binary older than the one that wrote the on-disk dump) can
hit a snapshot whose FORMAT version this binary does not understand
(`FORMAT_VERSION` in `crates/ironcache-persist/src/format.rs`, currently `1`). IronCache
does NOT silently discard it. At boot, before binding any port,
`check_snapshot_loadable` inspects the committed dump and, on an unsupported version,
emits a LOUD error naming the risk (`crates/ironcache-persist/src/lib.rs`, verbatim):

```
ERROR ironcache: the on-disk snapshot has an unsupported format version and will NOT be
loaded; the node would start with an EMPTY keyspace (set
refuse_empty_start_on_version_mismatch = true to fail closed and refuse to boot instead of
discarding the on-disk data)
```

**What the operator sees, and does:**

- **Default (`refuse_empty_start_on_version_mismatch` unset / `false`):** the node LOGS
  the error above and boots with an EMPTY keyspace. This is the danger window: the next
  save could OVERWRITE the newer dump and lose it. If you see this after a downgrade, STOP
  writes and do not let a save run; either roll BACK UP to the newer binary (which can
  read its own dump), or restore a compatible snapshot for the older binary.
- **Fail-closed (`refuse_empty_start_on_version_mismatch = true`):** the node REFUSES to
  boot instead of starting empty, with
  `refusing to boot: the on-disk snapshot has an unsupported format version and
  refuse_empty_start_on_version_mismatch is set (fail closed rather than start with an
  empty keyspace)` (`cmd_server` in `crates/ironcache/src/main.rs`). The dump is untouched;
  boot the NEWER binary (or point `data_dir` at a compatible snapshot) and try again. Set
  this key (TOML `refuse_empty_start_on_version_mismatch = true` or
  `IRONCACHE_REFUSE_EMPTY_START_ON_VERSION_MISMATCH=1`;
  `Config::refuse_empty_start_on_version_mismatch` in `crates/ironcache-config/src/lib.rs`)
  on any node where a downgrade wiping the keyspace is worse than a refused boot -- which
  is almost always.

There is no CLI flag for this key; it is TOML/env only.

---

## Reference: verified against

Citations are SYMBOL names (functions / consts / types), not line numbers: line numbers
rot as the code moves; symbols are greppable.

| Item | Where confirmed |
|------|-----------------|
| `ironcache check` output | `cmd_check` in `crates/ironcache/src/main.rs` |
| `ironcache upgrade` flags/defaults | `UpgradeArgs` in `crates/ironcache/src/cli.rs`; `cmd_upgrade` in `crates/ironcache/src/main.rs` |
| Streamed-cutover CLI selection (no `--streamed` flag) | `cmd_upgrade` -> `cmd_upgrade_streamed` in `crates/ironcache/src/main.rs` |
| SIGUSR1 trigger + COMMIT/ABORT log lines | `wait_for_signal` in `crates/ironcache/src/serve_signal.rs`; `drive_cutover` / `run_cutover_host` in `crates/ironcache/src/main.rs` |
| Receiver commit protocol + durable publish | `receive_shard_into` / `receive_shard_from_handoff` in `crates/ironcache/src/coordinator.rs` |
| Streamed-cutover acceptance (commit + abort, zero loss, no RST, sub-second stall) | `crates/ironcache/tests/upgrade_streamed_sigusr1.rs`, `crates/ironcache/tests/upgrade_streamed_cutover.rs` |
| Sibling spawn + inherited-listener no-RST | `spawn_receiver_sibling` in `crates/ironcache/src/upgrade/orchestrator.rs` |
| Socket-activation adopt/self-bind | `listener_for` / `adopt_listener_fd` in `crates/ironcache-runtime/src/tokio_rt.rs` |
| Socket-activation boot log (#562) | `crates/ironcache/src/sockact_log.rs`; `classify` / `Activation::boot_summary` in `crates/ironcache-runtime/src/listen_fds.rs` |
| `ironcache.socket` `ListenStream`/`Backlog`/`ReusePort`/`FileDescriptorName=resp` | `packaging/ironcache.socket`; `resp_listener_fd` in `crates/ironcache-runtime/src/listen_fds.rs` |
| `ironcache.service` `ExecStart --metrics-addr 127.0.0.1:9121`, `Wants`/`After` socket | `packaging/ironcache.service` |
| Server default ops bind `127.0.0.1:9091` | `DEFAULT_METRICS_ADDR` / `effective_metrics_addr` in `crates/ironcache/src/cli.rs` |
| `INFO server` `ironcache_version` / `redis_version:7.4.0` | `build_info` in `crates/ironcache-observe/src/lib.rs` |
| `INFO persistence` `rdb_last_bgsave_status:ok\|err` (#549) | `push_persistence_section` in `crates/ironcache-observe/src/lib.rs` |
| `INFO replication` `master_link_status`, `role`, offsets | `push_replication_section` in `crates/ironcache-observe/src/lib.rs` |
| `/metrics` latency histogram (#546) | `render_latency_histogram` in `crates/ironcache-observe/src/lib.rs` |
| `/metrics` hop counters + inbox depth (#556) | `render_prometheus` / `render_inbox_depth` in `crates/ironcache-observe/src/lib.rs` |
| `/metrics` `ironcache_uptime_seconds`, repl gauges (#549) | `render_prometheus` in `crates/ironcache-observe/src/lib.rs` |
| `/readyz` / `/livez` semantics | `ReadyState` in `crates/ironcache/src/metrics_http.rs` |
| `CLUSTER REPLICATE` -> `AssignReplica` | `try_raft_cluster_mutator` / `build_replicate` in `crates/ironcache/src/serve.rs` |
| `CLUSTER FAILOVER` -> `PromoteReplica` + refusals | `try_raft_cluster_mutator` / `build_failover` in `crates/ironcache/src/serve.rs` |
| `failover_timeout_secs` (5), `replica_max_lag` (256), `min_replicas_to_write` (0), `min_replicas_max_lag` (10) | `Config` fields + `DEFAULT_FAILOVER_TIMEOUT_SECS` / `DEFAULT_REPLICA_MAX_LAG` / `DEFAULT_MIN_REPLICAS_MAX_LAG` in `crates/ironcache-config/src/lib.rs` |
| #530 `check_snapshot_loadable` error text | `check_snapshot_loadable` in `crates/ironcache-persist/src/lib.rs` |
| #530 fail-closed at boot | `cmd_server` in `crates/ironcache/src/main.rs` |
| #530 `refuse_empty_start_on_version_mismatch` key | `Config::refuse_empty_start_on_version_mismatch` in `crates/ironcache-config/src/lib.rs` |
| `FORMAT_VERSION` | `FORMAT_VERSION` in `crates/ironcache-persist/src/format.rs` |
| `cluster_mode`, `data_dir`, `handoff_socket`, save points | `Config::cluster_mode` / `Config::data_dir` / `Config::handoff_socket` in `crates/ironcache-config/src/lib.rs` |
</content>
