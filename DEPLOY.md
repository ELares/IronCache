<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# Deploying IronCache (PROD-10)

This guide covers the production deployment artifacts that ship in this repo:

- Two multi-stage, non-root container images (`Dockerfile` for the cache,
  `Dockerfile.console` for the console), published to GHCR by
  `.github/workflows/image.yml`.
- `deploy/compose/` -- single-node and 3-node Raft cluster docker-compose files,
  plus `docker-compose.console.yml` (two stateless console replicas behind an LB).
- `deploy/helm/ironcache/` -- a Helm chart deploying the cache StatefulSet and an
  optional stateless console Deployment (`console.enabled=true`).
- `deploy/k8s/` -- the same StatefulSet as raw, Helm-free YAML, plus
  `ironcache-console.yaml` (the console Deployment + Service).

The deploy artifacts are lint-gated on every PR that touches them
(`.github/workflows/deploy-lint.yml`: `helm lint` + `kubeconform` on the rendered
chart and the raw manifests, `hadolint` on both Dockerfiles, `docker compose
config` on the compose files).

It maps every knob to the REAL config key, explains the ports, and is honest
about what was validated offline versus what needs a live cluster to confirm.

---

## 1. The binary, the ports, and how it is configured

IronCache is one static binary, `ironcache`. The default subcommand is `server`.
Configuration is layered, highest precedence first:

```
runtime CONFIG SET  >  CLI flags  >  IRONCACHE_* env vars  >  TOML file  >  built-in defaults
```

The TOML file is `--config <path>` or, if unset, `/etc/ironcache/ironcache.toml`
when present. The structured cluster topology (`[[cluster_topology.nodes]]`) is
**TOML-only** -- it has no env/CLI form -- so cluster deployments mount a TOML
file. Single scalars (the announce id, the secret, the TLS toggles, data_dir, the
save policy) are settable by env, which is how the orchestrator injects per-pod
values without rewriting the file.

### Ports

| Port (default) | Purpose | How it is set / derived |
| --- | --- | --- |
| `6379` | client RESP listener | `port` / `IRONCACHE_PORT` / `--port` |
| `16379` | Raft cluster-bus (`RAFTMSG`) | `port + 10000` (raft mode only) |
| `26379` | replication data plane | `port + 20000` (raft mode only) |
| operator-chosen (e.g. `9121`) | HTTP `/metrics` + `/livez` + `/readyz` | `--metrics-addr <ip:port>` |

The cluster-bus and replication ports are DERIVED from the client port in code
(`BUS_PORT_OFFSET = 10000`, `REPL_PORT_OFFSET = 20000`); you do not configure them
separately. They are only used in raft-governance mode. The health/metrics HTTP
endpoint exists ONLY when `--metrics-addr` is passed (there is no env var for it);
all deployment artifacts here pass `--metrics-addr 0.0.0.0:9121`.

### The config keys you will actually set (REAL names)

| Key (TOML) | Env var | Meaning |
| --- | --- | --- |
| `bind` | `IRONCACHE_BIND` | listen address (use `0.0.0.0` in a container) |
| `port` | `IRONCACHE_PORT` | client RESP port (default 6379) |
| `shards` | `IRONCACHE_SHARDS` | per-core runtimes (default = available parallelism) |
| `maxmemory` | `IRONCACHE_MAXMEMORY` | memory ceiling ("512mb", "1gb", 0 = unlimited) |
| `maxmemory-policy` | `IRONCACHE_MAXMEMORY_POLICY` | eviction policy (default `allkeys-lru`) |
| `requirepass` | `IRONCACHE_REQUIREPASS` | client AUTH password (hashed at rest) |
| `maxclients` | `IRONCACHE_MAXCLIENTS` | max connections (default 10000; 0 = unlimited) |
| `data_dir` | `IRONCACHE_DATA_DIR` | durable snapshot + Raft log dir (enables persistence) |
| `save_interval_secs` | `IRONCACHE_SAVE_INTERVAL_SECS` | periodic save cadence (0 = off) |
| `save_min_changes` | `IRONCACHE_SAVE_MIN_CHANGES` | min writes before a periodic save fires |
| `refuse_empty_start_on_version_mismatch` | `IRONCACHE_REFUSE_EMPTY_START_ON_VERSION_MISMATCH` | fail closed (refuse to boot) on a newer-format snapshot instead of a loud empty start |
| `cluster_enabled` | `IRONCACHE_CLUSTER_ENABLED` | turn on cluster mode (boot-only) |
| `cluster_mode` | `IRONCACHE_CLUSTER_MODE` | `static` (default) or `raft` |
| `cluster_announce_id` | `IRONCACHE_CLUSTER_ANNOUNCE_ID` | this node's stable 40-hex id |
| `cluster_topology.nodes` | (TOML only) | the peer list + slot ownership |
| `min_replicas_to_write` | `IRONCACHE_MIN_REPLICAS_TO_WRITE` | write-side durability guardrail |
| `min_replicas_max_lag` | `IRONCACHE_MIN_REPLICAS_MAX_LAG` | lag bound for the in-sync quorum |
| `raft_snapshot_threshold` | `IRONCACHE_RAFT_SNAPSHOT_THRESHOLD` | Raft-log compaction threshold |
| `raft_snapshot_chunk_bytes` | `IRONCACHE_RAFT_SNAPSHOT_CHUNK_BYTES` | InstallSnapshot chunk size in bytes (default 256 KiB) |
| `cluster_secret` | `IRONCACHE_CLUSTER_SECRET` | shared peer-auth secret (bus + repl) |
| `cluster_tls` | `IRONCACHE_CLUSTER_TLS` | `off` (default) / `on` -- encrypt bus + repl |
| `cluster_tls_cert_path` | `IRONCACHE_CLUSTER_TLS_CERT_PATH` | cluster TLS cert (PEM chain) |
| `cluster_tls_key_path` | `IRONCACHE_CLUSTER_TLS_KEY_PATH` | cluster TLS private key |
| `cluster_ca_path` | `IRONCACHE_CLUSTER_CA_PATH` | cluster CA to verify peer certs |
| `tls` | `IRONCACHE_TLS` | `off` (default) / `on` -- TLS on the public client port |
| `tls_cert_path` | `IRONCACHE_TLS_CERT_PATH` | client TLS cert (PEM chain) |
| `tls_key_path` | `IRONCACHE_TLS_KEY_PATH` | client TLS private key |

Self-check any config without starting the server:

```sh
ironcache check --config /etc/ironcache/ironcache.toml
```

---

## 2. The container image

`Dockerfile` is a two-stage build:

- **stage** (`alpine`, throwaway): stages the prebuilt **static musl** binary for
  the target arch. The binary is the SAME one the release pipeline builds (the
  `*-unknown-linux-musl` targets, `crt-static`, no libc dependency); the image CI
  unpacks it into `dist/<arch>/ironcache`.
- **final** (`gcr.io/distroless/static:nonroot`): just CA certs + the nonroot user
  (uid 65532) + the binary. No shell, no package manager, no toolchain. Runs as
  `USER 65532:65532`. `data_dir` is a `VOLUME`. The client / bus / repl / metrics
  ports are `EXPOSE`d (metadata only).

Because the image is distroless (no shell/curl), container health checks probe via
the binary's own tiny TCP client (`ironcache cli -p 6379`); Kubernetes uses the
HTTP `/livez` + `/readyz` endpoints directly.

Publish: pushing a `v*` tag triggers `.github/workflows/image.yml`, which builds
the musl binaries (same reproducible recipe as `release.yml`), then `docker buildx`
builds a multi-arch (`linux/amd64` + `linux/arm64`) image and pushes
`ghcr.io/elares/ironcache:<version>` + `:latest` with build provenance + an SBOM.
It is a SEPARATE workflow from the binary release, so it cannot break or duplicate
the binary-release jobs.

---

## 3. Single node

### docker-compose

```sh
cd deploy/compose
docker compose up -d
redis-cli -p 6379 ping
curl localhost:9121/readyz
```

### Plain docker

```sh
docker run -d --name ironcache \
  -p 6379:6379 -p 9121:9121 \
  -v ironcache-data:/var/lib/ironcache \
  -e IRONCACHE_DATA_DIR=/var/lib/ironcache \
  ghcr.io/elares/ironcache:latest \
  server --bind 0.0.0.0 --metrics-addr 0.0.0.0:9121
```

---

## 4. docker-compose 3-node Raft cluster

See `deploy/compose/README.md`. In short: set a shared
`IRONCACHE_CLUSTER_SECRET` in a `.env` file, then
`docker compose -f docker-compose.cluster.yml up -d`. Each node mounts its own
`config/nodeN.toml` (full topology + its stable `cluster_announce_id`); peers
resolve by the compose service-name DNS.

---

## 5. Kubernetes

Two equivalent paths: the Helm chart (`deploy/helm/ironcache`) and the raw
manifests (`deploy/k8s/ironcache.yaml`). Both deploy a **StatefulSet** with:

- a **headless Service** giving every pod stable DNS
  `<pod>.<svc>.<ns>.svc.cluster.local`, which is how Raft peers find each other;
  `publishNotReadyAddresses: true` so a peer is resolvable during boot (Raft
  formation needs it before `/readyz`);
- a **client Service** (a single in-cluster endpoint);
- a **ConfigMap** with the base TOML (full topology) + an init-container script;
- a **Secret** for `cluster_secret`, `requirepass`, and optional TLS material;
- a **PodDisruptionBudget** (`maxUnavailable: 1`) so a node drain keeps the
  Raft majority quorum;
- a **PVC volumeClaimTemplate** mounting `data_dir`;
- **livenessProbe `/livez`** and **readinessProbe `/readyz`** on the metrics port;
- `podAntiAffinity` to spread pods across nodes;
- a non-root, read-only-rootfs, all-capabilities-dropped security context.

### Per-pod identity (how Raft forms)

The topology is TOML-only and lists every node by its headless-Service DNS name
with a deterministic 40-hex id `sha256("<name>-<ordinal>")[:40]`. Each pod must
announce the id matching its own topology entry. A small **BusyBox init
container** reads the pod's StatefulSet ordinal from its hostname, recomputes that
same id (`sha256sum | cut -c1-40`), and writes the final config (the id PREPENDED,
so it stays a top-level key and is not absorbed into the last topology table) into
an `emptyDir` the main container reads via `--config`. The runtime container stays
distroless / shell-free.

### Helm

```sh
helm install ic deploy/helm/ironcache --namespace cache --create-namespace \
  --set replicas=3 \
  --set clusterSecret.value="$(openssl rand -hex 24)" \
  --set auth.enabled=true --set auth.password="$(openssl rand -hex 24)" \
  --set image.tag=v0.1.0 \
  --set persistence.storageClassName=fast-ssd
```

Production guidance baked into `values.yaml`:

- Set a STABLE `clusterSecret.value` (or `clusterSecret.existingSecret`). A blank
  value auto-generates a RANDOM secret on install that a `helm upgrade` would
  rotate -- which would split the cluster.
- Enable `auth.*` and pin `image.tag` to an immutable version.
- Consider `clusterTls.enabled=true` for a zero-trust pod network (without it the
  `cluster_secret` travels in cleartext on the pod network).
- Keep `replicas` ODD (3, 5, 7) for an unambiguous majority; the 16384 slots are
  split evenly automatically.

### Raw manifests

```sh
kubectl create namespace ironcache
# Edit the Secret placeholders (cluster_secret / requirepass) first!
kubectl -n ironcache apply -f deploy/k8s/ironcache.yaml
```

### The console (optional, stateless)

The monitoring/management console (epic #352) is a SEPARATE, stateless workload,
off by default. It is a Deployment of N identical replicas behind a Service (no
PVC, no per-pod identity), so it scales horizontally and any replica serves any
request; see "Console HA" under section 7 for the statelessness details.

- **Helm:** enable it in the same release with `--set console.enabled=true`
  (tune `console.replicas`, `console.seeds`, `console.prometheusUrl`, and the
  `console.nodePasswordSecret` / `console.tokensSecret` references). It renders a
  Deployment + Service + (at 2+ replicas) a PodDisruptionBudget.
- **Raw manifests:** `kubectl -n ironcache apply -f deploy/k8s/ironcache-console.yaml`
  (edit the Secret placeholders + the seeds/Prometheus env first).
- **docker-compose:** overlay `docker-compose.console.yml` (two replicas + an nginx
  LB) onto a cache compose file.

In every form: point the console at the cache client Service/nodes
(`IRONCACHE_CONSOLE_SEEDS`), a SHARED Prometheus for consistent history
(`IRONCACHE_CONSOLE_PROMETHEUS_URL`), and the least-privilege node user
(`console_monitor`, section 6); set a read token so the privileged API is not open;
and keep the console Service behind a VPN-locked LB (#369).

---

## 6. Enabling auth and TLS

### Client AUTH (requirepass)

Set `requirepass` (TOML) or `IRONCACHE_REQUIREPASS` (env). The server hashes it at
rest (SHA-256); the plaintext never persists past config load. In Helm,
`auth.enabled=true` + `auth.password=...` (or `auth.existingSecret`). Richer ACL
users are loaded from an `aclfile` (`IRONCACHE_ACLFILE`) if you provide one.

### Least-privilege console users (aclfile)

The IronCache console (issue #352) should NOT dial nodes with a full-access
credential. `deploy/aclfile.console.example` is a ready-to-adapt aclfile defining
two scoped users the console authenticates as
(`IRONCACHE_CONSOLE_NODE_USER` + `IRONCACHE_CONSOLE_NODE_PASSWORD_FILE`):
`console_monitor` (read-only: PING/INFO/CLIENT LIST, no key access, no mutation)
for the polling replicas, and `console_admin` (the management surface: CONFIG
GET/SET, the CLUSTER mutators, INFO, SAVE, key CRUD) that is still denied the
destructive verbs (FLUSHALL, FLUSHDB, SHUTDOWN, KEYS, SWAPDB, DEBUG, MIGRATE, the
destructive CLUSTER slot ops, ...) and, by default, ACL (so a scoped admin cannot
rewrite the node's users to escalate itself; node-ACL management stays on a
separate credential). Replace the
`CHANGE_ME_*` passwords, decide how the `default` user is secured, and load it via
`IRONCACHE_ACLFILE`. The exact enforcement is pinned by the
`reference_console_aclfile_loads_and_enforces_least_privilege` test.

### Client-port TLS (public listener)

`tls=on` + `tls_cert_path` + `tls_key_path`. The client port becomes TLS-only
(plaintext clients are rejected). In Helm, `tls.enabled=true` + the cert/key.

### Cluster TLS + the shared secret (bus + replication)

`cluster_secret` is a token every node presents in a constant-time peer handshake
on the bus + repl links; a peer that does not present it is dropped, so a stranger
who reaches the port cannot join the bus, forge `RAFTMSG`, or pull replication.

`cluster_tls=on` additionally ENCRYPTS those links and REQUIRES
`cluster_tls_cert_path` + `cluster_tls_key_path`. Point `cluster_ca_path` at the
cluster CA so a dialed peer's cert is verified (this defeats an active MITM BEFORE
the secret is sent). A single self-signed cluster cert used as BOTH the cert and
the CA verifies against itself -- the simple no-PKI-but-secure setup. Without TLS
the secret travels in cleartext, so TLS + secret is the recommended pairing.

---

## 7. Health, readiness, and metrics

When `--metrics-addr` is set the server serves on that address:

- `GET /livez` -> `200` once the process is up and serving (liveness). The
  Kubernetes livenessProbe uses this -- it restarts a hung pod.
- `GET /readyz` -> `200` only when EVERY shard has finished load-on-boot AND, in
  raft mode, a leader is known; `503` otherwise. The readinessProbe uses this -- a
  node with a large snapshot to load, or one that has not yet joined a quorum,
  stays out of the client Service and pauses the rolling update until it is
  genuinely ready.
- `GET /metrics` -> Prometheus exposition (per-shard counter rollup + process and
  raft gauges). Scrape it directly, or enable the chart's `metrics.serviceMonitor`.

When something is wrong at 3am, [`docs/RUNBOOK.md`](docs/RUNBOOK.md) is the
symptom-to-action index: every operator-visible error string, log line, and probe
state, each with what it means, what to check, and how to resolve it.

### Console HA: stateless replicas behind a load balancer

The IronCache console (issue #352) is designed to run as **N identical stateless
replicas behind a load balancer** (issue #363), separate from the data path:

- **No per-instance session state.** Auth is a `Authorization: Bearer` header
  (never a cookie), resolved from config that is identical on every replica, so
  any replica can serve any request; there is no sticky-session requirement.
- **Topology is derived, not stored.** Each replica independently polls the seed
  nodes and rebuilds its view, so replicas converge without shared state.
- **Readiness is per-replica.** The console's own `GET /livez` / `GET /readyz`
  (its HTTP listener) are the load balancer's health checks: `/readyz` returns
  `503` until that replica's first successful poll, so a cold replica is held out
  of rotation, and a replica whose process is DOWN fails the probe and is routed
  around. (Readiness latches once ready, so a still-running replica that has only
  lost backend connectivity keeps serving a degraded view rather than ejecting
  itself; liveness/readiness gate startup + process health, not backend reachability.)
- **History must be SHARED for consistency.** Point every replica at one shared
  Prometheus with `IRONCACHE_CONSOLE_PROMETHEUS_URL`. The alternative,
  `IRONCACHE_CONSOLE_HISTORY_EMBEDDED_HOURS`, keeps a PER-REPLICA in-memory trend
  window, so behind an LB each replica shows a different `/api/timeseries` window
  and a replica loss drops its window. The console logs a boot WARNING if embedded
  history is used on a non-loopback bind for exactly this reason.
- **Least privilege + exposure.** Each replica dials nodes as the scoped
  `console_monitor` user (see the aclfile above), and the console is kept behind a
  VPN-locked, SG-restricted load balancer, not world-reachable (issue #369).

The reference container image + Helm/k8s manifests for the console are tracked as
follow-up packaging work under #363.

---

## 8. Persistence, RPO, and the PVC

`data_dir` is the SINGLE enable switch for durable persistence (the on-disk
snapshot `dump-shard-<n>.icss` + `dump.manifest`) AND the durable Raft log
(`ironcache-raft-<bus-port>.log`). With no `data_dir` the node is purely in-memory
and the Raft log lands in the OS temp dir (lost on a `/tmp`-clearing reboot) -- so
ALWAYS set `data_dir` (onto a PVC) for a cluster.

RPO is governed by the save policy: `save_interval_secs` + `save_min_changes` (the
Redis `save <seconds> <changes>` cadence). The defaults here (900s / >=1 change)
mean up to ~15 minutes of writes can be lost on an ungraceful crash of a single
node; tighten the interval for a smaller RPO at the cost of more I/O. A graceful
shutdown performs a final save-on-exit, and the StatefulSet's
`terminationGracePeriodSeconds` (60s) covers that drain. In a cluster,
`min_replicas_to_write` bounds how many ACKNOWLEDGED writes a failover can lose to
the async-replication window: an owner rejects a write unless it has that many
in-sync replicas, so an acked write is known to be on that many nodes.

Each pod gets its own PVC via the StatefulSet `volumeClaimTemplate`
(`ReadWriteOnce`, default 10Gi); set `persistence.storageClassName` to an SSD class
in production. PVCs are NOT deleted by `helm uninstall` / `kubectl delete sts` --
remove them explicitly when you mean to discard data.

### Dump-format compatibility policy

The on-disk snapshot (`dump.manifest` + `dump-shard-<n>.icss`) carries an integer
FORMAT VERSION, bumped only on a breaking layout change. A binary reads ONLY its own
format version: a genuinely absent, torn, or foreign dump degrades safely to an empty
start (the ephemeral-cache posture), but a WELL-FORMED dump written by a DIFFERENT
version is NOT silently discarded. On boot the node classifies such a mismatch (almost
always an older binary loading a NEWER dump -- a downgrade or a failed-upgrade rollback)
and emits a LOUD `ERROR` log; it never boots silently empty on a version it cannot read.
The default posture then starts with an empty keyspace (loud, not silent), which is
recoverable ONLY if you have not yet let a save overwrite the newer dump -- so set
`refuse_empty_start_on_version_mismatch = true`
(`IRONCACHE_REFUSE_EMPTY_START_ON_VERSION_MISMATCH=true`) to FAIL CLOSED: the node
refuses to boot on an unreadable dump, giving you the chance to roll the correct binary
forward again before any data is lost. Practical rule: upgrade format versions forward
only, and when you must roll a binary back, restore a snapshot the older binary can read
(or wipe the `data_dir`) rather than pointing it at a newer dump.

---

## 9. Raft cluster formation, quorum, and scaling

- **Formation (TURNKEY)**: in raft mode every node boots from the SAME topology
  (the voter set + the peer bus addresses, `host:(port+10000)`, AND each node's
  declared `slots`). It builds one shared slot map seeded with its own id, then the
  Raft control plane elects a leader. On a FRESH cluster (an empty committed
  config) the elected leader AUTO-APPLIES the topology's declared node table + slot
  ownership through the replicated log, so all nodes converge on one ownership view
  -- `cluster_state:ok` with all 16384 slots assigned -- with NO operator action.
  You do NOT hand-run `CLUSTER MEET` / `CLUSTER ADDSLOTS` for the shipped static
  topology; those commands are reserved for RUNTIME changes (adding a node,
  rebalancing). The auto-apply is fresh-only and idempotent: it fires once on a
  pristine cluster and never re-runs, so a node RESTART (which recovers its
  persisted committed config) does NOT re-bootstrap or clobber any runtime change /
  migration. Peer DNS is resolved LAZILY at dial time, so a peer that is not up yet
  does not abort another node's boot -- the adapter retries each heartbeat. (If a
  topology declares NO slots at all, no auto-apply happens and slot ownership is
  established entirely by runtime `CLUSTER ADDSLOTS` / `SETSLOT` proposals.)
- **Quorum / PDB**: a Raft cluster needs a majority alive. Keep an ODD node count.
  The PodDisruptionBudget (`maxUnavailable: 1`) ensures a voluntary disruption
  (drain, rolling node upgrade) takes at most one pod at a time, so a 3-node
  cluster always keeps its 2-node majority.
- **Scaling / durability**: `min_replicas_to_write` is the write-side durability
  knob -- raise it toward the replica count for stronger durability (an acked write
  is on more nodes) at the cost of write availability when a replica is down. Slot
  rebalancing onto newly added nodes is an online-migration operation (`CLUSTER
  SETSLOT` proposals through the Raft log); changing `replicas` in the chart adds
  pods + topology entries but the slot map is governed by Raft at runtime.

---

## 10. What is validated vs what needs a live cluster

**Validated offline (in this repo, against the real artifacts):**

- `cargo build --workspace` is green.
- TURNKEY formation has an automated integration test (`crates/ironcache/tests/
  turnkey_cluster.rs`): a fresh 3-node raft cluster booted from the shipped static
  topology reaches `cluster_state:ok` + all 16384 slots assigned + 3 known nodes
  with NO manual `CLUSTER MEET` / `ADDSLOTS`, the committed config is stable
  afterward (no re-bootstrap churn), and a runtime `SETSLOT` is not clobbered. It
  was also validated by hand with three local processes (fresh boot -> auto-ok,
  then a leader restart that recovered its committed config WITHOUT re-bootstrapping
  -- the config epoch stayed put).
- The docker-compose cluster + single-node TOML configs, the Helm-rendered cluster
  config, and the raw-manifest embedded config were each fed through the REAL
  `ironcache check --config ...` and pass -- this exercises the actual layered
  config loader AND `SlotMap::build` (slot gap/overlap/dup-id/bad-id validation and
  the announce-id-must-match-a-topology-entry rule).
- The init-container identity math (`sha256sum | cut -c1-40`) was confirmed to
  produce EXACTLY the topology ids the chart/manifests embed, for ordinals 0..2.
- `helm lint` is clean; `helm template` renders for replicas 1/3/5 (even slot
  split, contiguous, full 0..16383 coverage); `kubectl apply --dry-run=client`
  accepts both the Helm output and the raw manifests.
- `docker compose config` parses both compose files; `yamllint` is clean on the
  compose / k8s / workflow YAML; both workflow YAMLs parse.

**NOT run locally (the Docker daemon was unavailable on the authoring host) --
verify in CI / a live environment:**

- `docker build` / `docker buildx` of the image and the multi-arch GHCR push. The
  Dockerfile is syntactically reviewed and reuses the proven packaging-scaffold
  pattern, but it was not built here. The first `v*` tag exercises it.
- Actual Raft cluster formation, leader election, replication, failover, and a
  rolling upgrade under the PDB -- these need a real multi-node Kubernetes cluster
  with working pod DNS and PVCs. The manifests are dry-run-valid and the config is
  loader-valid, but live quorum behavior must be confirmed on a cluster.
- The HTTP probes returning 200/503 against a running pod (the endpoints and paths
  are taken from the server source; the probe wiring is dry-run-valid).

---

## 11. Crash troubleshooting (panics, backtraces, core dumps)

The release binary is built `panic = "abort"` (a panic terminates the process
immediately, with no orderly unwind) and is size-stripped, but it is tuned so a
crash is still diagnosable.

### What you always get: the panic hook

At boot, before any listener binds, the server installs a process-wide panic hook.
The instant a panic fires, and BEFORE the abort, it writes ONE actionable `ERROR`
line through the normal log sink (stderr, i.e. journald under the systemd unit):

```
ERROR ironcache::panic: ironcache PANICKED and is aborting: <message> (at
  <file>:<line>:<col>; build <version>). Please report this crash at
  https://github.com/ELares/IronCache/issues
```

The `file:line:col` LOCATION is baked into the binary as static string data, so it
is present even on the stripped release artifact regardless of `strip`. That single
line already tells you WHERE it crashed and on WHICH build, so a bug report is
actionable even without a backtrace.

### Turning on the backtrace: RUST_BACKTRACE

Set `RUST_BACKTRACE=1` (or `full` for unabridged frames) in the process
environment. The packaging systemd unit ships it on:

```ini
# packaging/ironcache.service, [Service]
Environment=RUST_BACKTRACE=1
```

With it set, the panic hook ALSO logs a captured backtrace after the summary line.
The release profile keeps the SYMBOL TABLE (`[profile.release] strip =
"debuginfo"` in the root `Cargo.toml`, NOT `strip = "symbols"`), so on a
system-linker build the backtrace frames resolve to FUNCTION NAMES.

Two caveats worth knowing up front:

- The published **static-musl** binary (the one the container image and the
  release tarballs ship) is linked by `zig cc`, which strips it FULLY, so its
  backtrace frames are raw addresses. On that artifact the panic hook's own
  `file:line` line is your crash site; for named frames, reproduce the crash with a
  from-source or glibc `cargo build --release` (which retains the symbol table).
- Full `file:line` on EVERY backtrace frame needs DWARF line tables, which the
  release build omits for size. Use the hook's `file:line` for the crash site, or a
  debug `cargo build` when you want line numbers throughout the trace.

Read it from the journal:

```sh
journalctl -u ironcache -n 100 --no-pager        # recent logs incl. the panic line
journalctl -u ironcache -p err --no-pager         # only ERROR (the panic summary + trace)
```

You can also force a panic on a `--release` build to see exactly what an operator
would get, using the shipped demonstration example:

```sh
RUST_BACKTRACE=1 cargo run -p ironcache --example forced_panic --release
```

### Core dumps

For a post-mortem beyond the backtrace (inspecting memory, all threads), enable
core dumps. On a `systemd-coredump`-equipped host they are captured automatically
and read with `coredumpctl` (no writable path needed in the sandbox, because
`systemd-coredump` collects the core out of process):

```sh
coredumpctl list ironcache            # crashes captured for the unit
coredumpctl info ironcache            # signal, command line, and a symbolized trace
coredumpctl gdb ironcache             # open the newest core in gdb (needs gdb installed)
```

Notes for this unit:

- The core is stored under `/var/lib/systemd/coredump/` (compressed) and indexed by
  the journal; `coredumpctl` finds it by unit name even though the service runs as a
  transient `DynamicUser`.
- `abort()` raises `SIGABRT`, which is a core-generating signal, so a
  `panic = "abort"` crash DOES produce a core when core dumps are enabled.
- If cores are disabled globally, raise the limit
  (`ulimit -c unlimited` / `LimitCORE=infinity` in the unit) and confirm
  `/proc/sys/kernel/core_pattern` routes to `systemd-coredump` (the distro default);
  the unit's strict `ProtectSystem`/`PrivateTmp` sandbox does not block out-of-process
  `systemd-coredump` collection.
- Symbols: `coredumpctl` resolves function names from the binary's symbol table.
  A system-linker build retains it (`strip = "debuginfo"`, as above); the shipping
  static-musl binary is stripped, so for named frames from a core, load it against a
  from-source / glibc build of the SAME commit. Either way, keep the exact binary
  that produced the core on hand for the cleanest symbolization.
