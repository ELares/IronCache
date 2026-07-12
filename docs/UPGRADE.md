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

`cluster_mode` and `data_dir` are config keys
(`crates/ironcache-config/src/lib.rs:437`, `:511`; TOML `cluster_mode = "raft"` /
`data_dir = "..."`, or `IRONCACHE_CLUSTER_MODE` / `IRONCACHE_DATA_DIR`). Confirm your
posture with `ironcache check` (below) and `CLUSTER INFO`.

---

## Pre-flight checks (run before every upgrade)

1. **Config is valid on the new binary.** Run the new binary's config self-check (the
   nginx `-t` analogue). It resolves + validates the effective config and prints it
   WITHOUT binding a port; a malformed `maxmemory`, a bad policy, or an unresolvable
   overlay fails HERE instead of at boot
   (`crates/ironcache/src/main.rs:354` `cmd_check`):

   ```sh
   ironcache check
   # ironcache check: configuration OK
   #   bind        = 127.0.0.1:6379
   #   shards      = 8
   #   runtime     = tokio
   #   databases   = 16
   #   maxmemory   = 0 bytes (unlimited)
   #   policy      = noeviction
   #   requirepass = set
   #   tls         = off (plaintext)
   #   allocator   = jemalloc (background_thread=true, dirty_decay_ms=5000)
   ```

2. **The last snapshot committed cleanly.** In a persisted deployment, confirm the most
   recent background save succeeded before you swap, so a warm restart reloads a good
   dump. `INFO persistence` reports `rdb_last_bgsave_status:ok` (`err` after a failed
   save, e.g. a full disk), plus `rdb_last_save_time` and `rdb_changes_since_last_save`
   (`crates/ironcache-observe/src/lib.rs:1971`, `:1509`; #549). Force a fresh, known-good
   snapshot immediately before the swap:

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
   `ironcache_replication_lag_offset` small (`crates/ironcache-observe/src/lib.rs:980`,
   `:985`; #549). The promotion lag bound is `replica_max_lag`
   (`crates/ironcache-config/src/lib.rs:453`).

4. **The ops endpoint is reachable.** `ironcache upgrade`'s health gate probes `/readyz`
   and the RESP `PING`; the packaged unit exposes the endpoint on `127.0.0.1:9121`
   (`packaging/ironcache.service` `ExecStart ... --metrics-addr 127.0.0.1:9121`). Confirm
   it answers now:

   ```sh
   curl -fsS http://127.0.0.1:9121/readyz && echo   # expect: ready
   ```

---

## Single node rolling upgrade

### How the handoff works

With socket activation enabled, **systemd** owns the RESP listening socket, not the
server. `packaging/ironcache.socket` opens `ListenStream=127.0.0.1:6379` with
`Backlog=1024` and `ReusePort=false` and hands the fd to `ironcache.service` via the
`sd_listen_fds` protocol (`LISTEN_FDS` / `LISTEN_PID`). The server ADOPTS that fd instead
of self-binding (`crates/ironcache-runtime/src/tokio_rt.rs:178` `listener_for` ->
`adopt_listener_fd`). Because the single listen queue is never closed across the restart,
in-flight and new connections QUEUE in the kernel backlog during the brief swap window
rather than being refused (`packaging/ironcache.socket` header). This beats
`SO_REUSEPORT` for upgrades: a closed reuseport socket loses its queued connections; this
one does not.

The boot LOUDLY states which listener path it took, so you can confirm the handoff from
the logs (#562):

```
INFO socket-activation: ADOPTED 1 systemd socket-activation listening fd(s) [ironcache.socket=fd3]; systemd owns the listen queue, so it survives an upgrade restart with no connection-refused window
```

If activation was not in effect you instead see `FELL BACK to self-binding its own
listener: not socket-activated (no LISTEN_FDS in the environment)`, and a rejected
activation environment (a foreign `LISTEN_PID`, a malformed count) logs at WARN naming the
reason (`crates/ironcache/src/sockact_log.rs`; the classification is
`crates/ironcache-runtime/src/listen_fds.rs` `classify`). Note the packaged
`ironcache.socket` sets no `FileDescriptorName=`, so `LISTEN_FDNAMES` defaults to the
socket unit name (`ironcache.socket`), which is what the fd name shows above.

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

`ironcache upgrade` is the single operator command (verified: `crates/ironcache/src/cli.rs:114`
`UpgradeArgs`, `crates/ironcache/src/main.rs:465` `cmd_upgrade`). It performs the whole
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

Verified flags and defaults (`crates/ironcache/src/cli.rs:125-214`):

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

### In-server streamed live cutover (SIGUSR1, #638) -- opt-in via `handoff_socket`

The default upgrade above SWAPS the binary and RESTARTS the process (the socket-activation
handoff keeps the listen queue open across the brief restart). An alternative shape keeps
the OLD process SERVING while it streams its live keyspace to a freshly spawned sibling
(a re-exec of the binary in receiver role) and flips write authority at a single committed
linearization point -- no restart, no acknowledged-write loss, and no orphaned-backlog RST
because the sibling INHERITS the OLD's client listen fd. This path is OPT-IN: it runs only
when a node is configured with a `handoff_socket` (TOML `handoff_socket = "..."` or
`IRONCACHE_HANDOFF_SOCKET`, `crates/ironcache-config/src/lib.rs`), a node-local AF_UNIX
rendezvous path both the OLD and the sibling agree on.

The trigger is **SIGUSR1** to the running server pid (`crates/ironcache/src/serve.rs`
`wait_for_signal` -> `SignalOutcome::Cutover`; `crates/ironcache/src/main.rs` `drive_cutover`).
On a plain `SIGTERM`/`SIGINT` the server still does the unchanged graceful stop; SIGUSR1 is
the streamed-cutover trigger:

```sh
# the running server must have handoff_socket configured; then:
kill -USR1 "$(redis-cli INFO server | sed -n 's/^process_id://p' | tr -d '\r')"
# on COMMIT: the sibling serves on the same port and the OLD process exits(0);
# on ABORT (a bad/unusable handoff socket, or a failed receiver): the OLD keeps serving,
#   never exits, and writes resume -- the trigger is fail-safe toward keep-serving.
```

`ironcache upgrade --streamed` is the intended CLI wrapper (it reads `INFO server`'s
`process_id` and sends the signal); until it is wired the raw `kill -USR1` above is the
trigger.

> STATUS (slice-5 acceptance, #638): the SIGUSR1 trigger, the sender-side barrier, the
> sibling spawn + inherited-listener no-RST, and the receiver-side serve-flip barrier are in
> place, and the ABORT path (OLD keeps serving with zero loss) is verified end to end by
> `crates/ironcache/tests/upgrade_streamed_sigusr1.rs`. The COMMIT path is NOT yet
> operational: the real-server acceptance surfaced that the receiver boot path still drives
> the legacy handoff receive (`stream::recv_shard`) instead of the PR-4 commit protocol the
> live sender speaks (`BulkStaged`/`Prepared`/`Served`), so a real cutover currently deadlocks
> and safely aborts. Use the default socket-activation `ironcache upgrade` above for
> production single-node upgrades until the receiver commit-protocol wiring lands.

---

## HA cluster rolling upgrade

In a raft-governance cluster (`cluster_mode = raft`) one node OWNS a set of slots (serves
reads and writes) and others may be committed as its REPLICAS, mirroring the owner and
serving READONLY reads (`crates/ironcache/src/replica_attach.rs`). Replica assignment and
promotion go through the Raft control plane, NOT a `REPLICAOF`/`SLAVEOF` command (those do
not exist in IronCache). The operator levers are the raft-mode `CLUSTER` mutators, handled
by `try_raft_cluster_mutator` (`crates/ironcache/src/serve.rs:4830`):

- `CLUSTER REPLICATE <node-id> <slot> [slot ...]` assigns a node as a replica of the
  listed slots (commits `AssignReplica`; `serve.rs:4873`, `:5601`).
- `CLUSTER FAILOVER` promotes THIS in-sync replica to owner of the slots it replicates
  (commits `PromoteReplica`, which atomically transfers ownership and bumps the config
  epoch; `serve.rs:4877`, `:5160`).

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
(`crates/ironcache-config/src/lib.rs:460`, default 0 = disabled):

```
min_replicas_to_write = 1     # owner rejects writes with -NOREPLICAS when under-replicated
min_replicas_max_lag  = <writes>
```

### 2. Upgrade the replicas first

A replica restart does not move ownership; the owner keeps serving throughout. Upgrade
each replica node one at a time with the single-node [`ironcache upgrade`](#the-upgrade)
flow. After each one comes back, confirm it re-attached before touching the next:

```sh
redis-cli INFO replication | grep -E 'role|master_link_status|slave_repl_offset'
# role:replica ; master_link_status:up ; slave_repl_offset catching up to master_repl_offset
```

`master_link_status` flips `up`/`down` at `crates/ironcache-observe/src/lib.rs` (INFO
replication section); the same signal is `ironcache_replication_link_up` on `/metrics`.

### 3. Promote a replica, then upgrade the old owner

When only the owner is left on the old version, promote a healthy, in-sync replica so the
owner role moves off the node you are about to restart. Run ON the replica you want to
become owner:

```sh
redis-cli -h <replica-host> -p <replica-port> CLUSTER FAILOVER
```

`CLUSTER FAILOVER` REFUSES (so you cannot promote an unsafe node) when
(`crates/ironcache/src/serve.rs:5160-5209`):

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
`crates/ironcache-config/src/lib.rs:454`, default `DEFAULT_FAILOVER_TIMEOUT_SECS`). A
replica is only promotable while its link was up and its lag was `<= replica_max_lag`
(`:445`), so a stale replica is never promoted. The controlled `CLUSTER FAILOVER` is
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

## Verify the new version took over

After any upgrade, confirm the NEW binary is the one serving:

1. **Version.** `INFO server` reports `ironcache_version:<real>` (the load-bearing field,
   from the build's `CARGO_PKG_VERSION`) alongside a fixed `redis_version:7.4.0`
   compatibility tag (`crates/ironcache-observe/src/lib.rs:1826-1827`). Check the REAL one:

   ```sh
   redis-cli INFO server | grep -E 'ironcache_version|redis_version|uptime_in_seconds'
   ```

   `uptime_in_seconds` (and the `ironcache_uptime_seconds` gauge on `/metrics`,
   `crates/ironcache-observe/src/lib.rs:941`) resetting toward zero PROVES a real restart
   happened.

2. **Readiness.** `/readyz` returns `200` only once load-on-boot finished for every shard
   AND (in raft mode) a leader is recognized; otherwise `503` with
   `not ready: load-on-boot incomplete` or `not ready: raft: no leader recognized`
   (`crates/ironcache/src/metrics_http.rs:296-307`). Liveness is `/livez`; both plus
   `/metrics` are served on the ops endpoint (boot log `metrics: serving /metrics, /livez,
   /readyz`).

   ```sh
   curl -fsS http://127.0.0.1:9121/readyz && echo    # ready
   ```

3. **Health metrics stayed healthy.** The command-latency histogram
   `ironcache_command_duration_seconds` (#546, buckets `0.000025 .. 10, +Inf`) and the
   cross-shard hop counters `ironcache_hops_sent_total` / `ironcache_hops_served_total` /
   `ironcache_local_served_total` plus the `ironcache_inbox_depth` gauge (#556) should
   return to their pre-upgrade shape (`crates/ironcache-observe/src/lib.rs:1174`, `:908`,
   `:1150`). A p99 query:

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
`crates/ironcache/src/cli.rs:192` `--no-rollback` to opt OUT). The state directory is
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
(`FORMAT_VERSION`, `crates/ironcache-persist/src/format.rs:51`, currently `1`). IronCache
does NOT silently discard it. At boot, before binding any port,
`check_snapshot_loadable` inspects the committed dump and, on an unsupported version,
emits a LOUD error naming the risk (`crates/ironcache-persist/src/lib.rs:265-271`, verbatim):

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
  empty keyspace)` (`crates/ironcache/src/main.rs:213-222`). The dump is untouched; boot
  the NEWER binary (or point `data_dir` at a compatible snapshot) and try again. Set this
  key (TOML `refuse_empty_start_on_version_mismatch = true` or
  `IRONCACHE_REFUSE_EMPTY_START_ON_VERSION_MISMATCH=1`;
  `crates/ironcache-config/src/lib.rs:647`) on any node where a downgrade wiping the
  keyspace is worse than a refused boot -- which is almost always.

There is no CLI flag for this key; it is TOML/env only.

---

## Reference: verified against

| Item | Where confirmed |
|------|-----------------|
| `ironcache check` output | `crates/ironcache/src/main.rs:354` `cmd_check` |
| `ironcache upgrade` flags/defaults | `crates/ironcache/src/cli.rs:114-214`; `crates/ironcache/src/main.rs:465` `cmd_upgrade` |
| Socket-activation adopt/self-bind | `crates/ironcache-runtime/src/tokio_rt.rs:178` `listener_for`, `:140` `adopt_listener_fd` |
| Socket-activation boot log (#562) | `crates/ironcache/src/sockact_log.rs`; `crates/ironcache-runtime/src/listen_fds.rs` `classify` / `Activation::boot_summary` |
| `ironcache.socket` `ListenStream`/`Backlog`/`ReusePort`, no `FileDescriptorName` | `packaging/ironcache.socket` |
| `ironcache.service` `ExecStart --metrics-addr 127.0.0.1:9121`, `Wants`/`After` socket | `packaging/ironcache.service` |
| `INFO server` `ironcache_version` / `redis_version:7.4.0` | `crates/ironcache-observe/src/lib.rs:1826-1827` |
| `INFO persistence` `rdb_last_bgsave_status:ok\|err` (#549) | `crates/ironcache-observe/src/lib.rs:1971`, `:1509` |
| `INFO replication` `master_link_status`, `role`, offsets | `crates/ironcache-observe/src/lib.rs` replication section |
| `/metrics` latency histogram (#546) | `crates/ironcache-observe/src/lib.rs:1174` |
| `/metrics` hop counters + inbox depth (#556) | `crates/ironcache-observe/src/lib.rs:908`, `:1150` |
| `/metrics` `ironcache_uptime_seconds`, repl gauges (#549) | `crates/ironcache-observe/src/lib.rs:941`, `:980`, `:985` |
| `/readyz` / `/livez` semantics | `crates/ironcache/src/metrics_http.rs:296-307` |
| `CLUSTER REPLICATE` -> `AssignReplica` | `crates/ironcache/src/serve.rs:4873`, `:5601` |
| `CLUSTER FAILOVER` -> `PromoteReplica` + refusals | `crates/ironcache/src/serve.rs:4877`, `:5160-5209` |
| `failover_timeout_secs`, `replica_max_lag`, `min_replicas_to_write` | `crates/ironcache-config/src/lib.rs:454`, `:445`, `:460` |
| #530 `check_snapshot_loadable` error text | `crates/ironcache-persist/src/lib.rs:265-271` |
| #530 fail-closed at boot | `crates/ironcache/src/main.rs:213-222` |
| #530 `refuse_empty_start_on_version_mismatch` key | `crates/ironcache-config/src/lib.rs:647` |
| `FORMAT_VERSION` | `crates/ironcache-persist/src/format.rs:51` |
| `cluster_mode`, `data_dir`, save points | `crates/ironcache-config/src/lib.rs:437`, `:511` |
</content>
