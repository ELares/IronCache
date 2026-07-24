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
| `127.0.0.1:9091` (default) | HTTP `/metrics` + `/livez` + `/readyz` | `--metrics-addr <ip:port>` |

The cluster-bus and replication ports are DERIVED from the client port in code
(`BUS_PORT_OFFSET = 10000`, `REPL_PORT_OFFSET = 20000`); you do not configure them
separately. They are only used in raft-governance mode. The health/metrics HTTP
endpoint is ON by DEFAULT since #555, bound to `127.0.0.1:9091` so a scrape and the
k8s probes work out of the box without exposing the port publicly (there is no env
var or TOML key for it, only the `--metrics-addr` flag). To make it reachable from
outside the pod/host, override the bind (all deployment artifacts here pass
`--metrics-addr 0.0.0.0:9121`, behind a NetworkPolicy); to turn it off entirely,
pass `--metrics-addr off`.

### The config keys you will actually set (REAL names)

| Key (TOML) | Env var | Meaning |
| --- | --- | --- |
| `bind` | `IRONCACHE_BIND` | listen address (use `0.0.0.0` in a container) |
| `port` | `IRONCACHE_PORT` | client RESP port (default 6379) |
| `shards` | `IRONCACHE_SHARDS` | per-core runtimes (default = available parallelism) |
| `maxmemory` | `IRONCACHE_MAXMEMORY` | memory ceiling ("512mb", "1gb", 0 = unlimited) |
| `maxmemory_policy` | `IRONCACHE_MAXMEMORY_POLICY` | eviction policy (default `allkeys-lru`) |
| `requirepass` | `IRONCACHE_REQUIREPASS` | client AUTH password (hashed at rest) |
| `maxclients` | `IRONCACHE_MAXCLIENTS` | max connections (default 10000; 0 = unlimited) |
| `timeout` | `IRONCACHE_TIMEOUT` | idle client disconnect in seconds (default 0 = never) |
| `tcp_keepalive_secs` | `IRONCACHE_TCP_KEEPALIVE` | SO_KEEPALIVE idle interval (default 300; 0 = off) |
| `output_buffer_limit` | `IRONCACHE_OUTPUT_BUFFER_LIMIT` | per-connection reply-buffer cap in bytes (default 1 GiB; 0 = unbounded) |
| `query_buffer_limit` | `IRONCACHE_QUERY_BUFFER_LIMIT` | per-connection request-buffer cap in bytes (default 1 GiB; 0 = unbounded) |
| `notify_keyspace_events` | `IRONCACHE_NOTIFY_KEYSPACE_EVENTS` | keyspace-notification flags, e.g. `"KEA"` (default `""` = off) |
| `databases` | (TOML only) | logical database count (default 16) |
| `slots_per_db` | (TOML only) | per-DB store partitions (default 256; NOT the 16384 cluster slots) |
| `runtime` | `IRONCACHE_RUNTIME` | async backend: `tokio` (default) or `io_uring` (Linux, build-feature-gated; see below) |
| `data_dir` | `IRONCACHE_DATA_DIR` | durable snapshot + Raft log dir (enables persistence) |
| `save_interval_secs` | `IRONCACHE_SAVE_INTERVAL_SECS` | periodic save cadence (0 = off) |
| `save_min_changes` | `IRONCACHE_SAVE_MIN_CHANGES` | min writes before a periodic save fires |
| `persist_cpu` | `IRONCACHE_PERSIST_CPU` | dedicate a core to the persist thread: `off` (default) / `auto` / a cpu list (`8`, `6-7`); Linux-only, see below |
| `handoff_socket` | `IRONCACHE_HANDOFF_SOCKET` | opt-in streamed live-cutover socket for upgrades (see `docs/UPGRADE.md`) |
| `refuse_empty_start_on_version_mismatch` | `IRONCACHE_REFUSE_EMPTY_START_ON_VERSION_MISMATCH` | fail closed (refuse to boot) on a newer-format snapshot instead of a loud empty start |
| `cluster_enabled` | `IRONCACHE_CLUSTER_ENABLED` | turn on cluster mode (boot-only) |
| `cluster_mode` | `IRONCACHE_CLUSTER_MODE` | `static` (default), `raft`, or `shard-owners` (see note below) |
| `cluster_announce_id` | `IRONCACHE_CLUSTER_ANNOUNCE_ID` | this node's stable 40-hex id |
| `cluster_topology.nodes` | (TOML only) | the peer list + slot ownership |
| `replica_max_lag` | `IRONCACHE_REPLICA_MAX_LAG` | promotion-eligibility + replica-read lag bound, in writes (default 256) |
| `failover_timeout_secs` | `IRONCACHE_FAILOVER_TIMEOUT_SECS` | continuous link-down seconds before a replica self-promotes (default 5) |
| `min_replicas_to_write` | `IRONCACHE_MIN_REPLICAS_TO_WRITE` | write-side durability guardrail |
| `min_replicas_max_lag` | `IRONCACHE_MIN_REPLICAS_MAX_LAG` | lag bound for the in-sync quorum (default 10) |
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

`cluster_mode = "shard-owners"` (#517) is the third mode: a SINGLE node advertises
its own shards as the slot owners, binding one client listener PER shard
(`port .. port+shards-1`) so a cluster-aware client routes each key straight to the
owning shard's port and skips the internal cross-shard hop -- the zero-hop mode
behind the cluster-aware benchmark numbers. Its constraints are enforced at boot
(`Config::validate` / `validate_shard_owners` in `ironcache-config`): it REQUIRES
`cluster_enabled = true`, takes NO `cluster_topology` (owners derive from the shard
count), is REJECTED with the io_uring runtime (per-shard listeners are a follow-up
there), and is incompatible with systemd socket activation (it needs N self-bound
ports, but activation passes one inherited socket -- the bootstrap refuses the
combo, `bind_shard_owner_listeners` in `ironcache-runtime`).

A documented single-node template with one comment per field is at
[`deploy/ironcache.example.toml`](deploy/ironcache.example.toml); copy it and edit the values.

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
The image tag STRIPS the leading `v` (image.yml `ver="${TAG#v}"`): a `v0.1.0` git
tag publishes `:0.1.0`, so pin `image.tag=0.1.0`, never `v0.1.0` (the latter is
manifest-unknown). Note the two release channels diverge here: images publish on
`v*` tags ONLY, while binary tarballs also roll continuously to `releases/latest`
on every push to main, so the GHCR `:latest` image can lag the rolling binary
channel. It is a SEPARATE workflow from the binary release, so it cannot break or
duplicate the binary-release jobs.

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

### Bare-metal install (any Linux host)

No container required: the release tarballs are static (musl) or glibc-2.17-pinned
Linux binaries. Two channels publish them (RELEASING.md): formal `v*` tags, and a
ROLLING CalVer build (`YYYY.MMDD.N`) on every push to main, which is what
`releases/latest` points at. Asset name:
`ironcache-<version>-linux-<amd64|arm64>-<musl|glibc>.tar.gz` (the version in the
asset name has NO leading `v`, matching what `ironcache --version` prints).

Fetch, verify against the consolidated `SHA256SUMS`, and install:

```sh
# Resolve the newest rolling tag (or set tag=v0.1.0 for a formal release).
tag="$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
  https://github.com/ELares/IronCache/releases/latest | sed 's#.*/tag/##')"
asset="ironcache-${tag#v}-linux-amd64-musl.tar.gz"   # or -arm64-, or -glibc
curl -fLO "https://github.com/ELares/IronCache/releases/download/${tag}/${asset}"
curl -fLO "https://github.com/ELares/IronCache/releases/download/${tag}/SHA256SUMS"
sha256sum -c --ignore-missing SHA256SUMS             # must print: ${asset}: OK
tar -xzf "$asset"                                    # extracts one file: ironcache
sudo install -m0755 ironcache /usr/local/bin/ironcache
ironcache --version
```

Install the hardened systemd units from the repo's `packaging/` directory (they are
tracked in git, not release assets -- without a checkout, fetch them raw from
`https://raw.githubusercontent.com/ELares/IronCache/main/packaging/<unit>`):

```sh
sudo install -m0644 packaging/ironcache.service /etc/systemd/system/ironcache.service
sudo install -m0644 packaging/ironcache.socket  /etc/systemd/system/ironcache.socket
sudo systemctl daemon-reload
sudo systemctl enable --now ironcache.socket ironcache.service
```

**Persistence needs a three-line unit edit.** The shipped unit is the in-memory
default with `ProtectSystem=strict` (the whole filesystem is read-only to the
service), so setting `data_dir` alone gets you a node that cannot write its
snapshot. In `[Service]`, uncomment the two prepared lines and add the env var:

```ini
StateDirectory=ironcache
ReadWritePaths=/var/lib/ironcache
Environment=IRONCACHE_DATA_DIR=/var/lib/ironcache
```

then `systemctl daemon-reload && systemctl restart ironcache`. Also set a save
cadence (`IRONCACHE_SAVE_INTERVAL_SECS` / `IRONCACHE_SAVE_MIN_CHANGES`, or the TOML
keys) -- the built-in periodic-save default is OFF (section 8).

**File descriptors.** At boot the server budgets `RLIMIT_NOFILE` against
`maxclients` (`fd_budget.rs`, `RESERVED_FDS = 64`): it RAISES the soft limit toward
the hard limit to fit `maxclients + 64`, and otherwise CLAMPS `maxclients` with a
loud WARN. Under the shipped unit the `@resources` syscall filter BLOCKS that
self-raise (`setrlimit` is filtered), so the unit provisions `LimitNOFILE=65535`
instead -- raise that line if you raise `maxclients` past ~65k. Running WITHOUT
systemd, make sure the HARD limit (`ulimit -Hn`) is `>= maxclients + 64`; the
server can only self-raise the soft limit up to it.

**The 9091-vs-9121 metrics trap.** The server's own metrics default is
`127.0.0.1:9091` (`DEFAULT_METRICS_ADDR`), but `ironcache upgrade` health-gates
against `127.0.0.1:9121` by default (its `--readyz-addr` default). The shipped unit
already runs `--metrics-addr 127.0.0.1:9121` for exactly this reason; if you run
the server with default flags instead, every default-flags upgrade fails its
pre-flight with connection-refused. Either keep the server on
`--metrics-addr 127.0.0.1:9121` or pass `--readyz-addr 127.0.0.1:9091` to
`ironcache upgrade`.

First boot + verify:

```sh
redis-cli -p 6379 ping            # PONG (or: ironcache cli -p 6379)
curl 127.0.0.1:9121/readyz        # 200 once every shard finished load-on-boot
journalctl -u ironcache -n 20     # boot banner, fd budget, socket-activation path
```

Later upgrades are then one command: `ironcache upgrade --to latest` (rolling) or
`ironcache upgrade --to v0.1.0` (formal tags KEEP the leading `v` here); see
[`docs/UPGRADE.md`](docs/UPGRADE.md).

### systemd socket activation (restart without a connection-refused window)

For a bare-metal / VM single node managed by systemd, run IronCache under socket
activation so an `ironcache upgrade` (or any `systemctl restart`) does NOT drop a
connection. The packaging ships both units:

- `packaging/ironcache.socket` opens the RESP listening socket and holds the listen
  queue.
- `packaging/ironcache.service` (`Wants=`/`After=ironcache.socket`) receives that
  socket's fd via the `sd_listen_fds` protocol (`LISTEN_FDS` / `LISTEN_PID`) and
  ADOPTS it instead of binding its own.

Install and enable both:

```sh
install -m0644 packaging/ironcache.socket   /etc/systemd/system/ironcache.socket
install -m0644 packaging/ironcache.service  /etc/systemd/system/ironcache.service
systemctl daemon-reload
systemctl enable --now ironcache.socket   # opens the listen socket
systemctl enable ironcache.service        # started on the first connection / boot
```

Why this removes the refused window: because SYSTEMD owns the listening socket, it
stays open ACROSS the service restart. While the old process exits and the new one
starts, incoming clients QUEUE in the kernel backlog (a brief added latency) instead
of getting `ECONNREFUSED`. Perceived downtime collapses to the new process's startup
time. This is stronger than `SO_REUSEPORT` for the restart case: a closed
`SO_REUSEPORT` socket loses its queued connections, whereas this single listen queue
is never closed.

Notes:

- The listen ADDRESS is authoritative from the `.socket` unit's `ListenStream=`, NOT
  from `--bind` / `IRONCACHE_BIND`, when socket-activated. The packaged default is
  `ListenStream=127.0.0.1:6379` (loopback, matching IronCache's safe bind default);
  to expose beyond loopback edit that line (e.g. `ListenStream=6379`), not `--bind`.
- Deepen the backlog that absorbs the restart with `Backlog=` in the socket unit;
  systemd caps it at `net.core.somaxconn`, so raise that sysctl to match.
- Not enabling `ironcache.socket` is fully supported: the service then self-binds
  exactly as before (byte-unchanged), so socket activation is strictly opt-in.
- The boot logs state which path was taken: `socket-activation: ADOPTED ...` when it
  adopted the passed fd, or `... FELL BACK to self-binding ...` (with the reason)
  otherwise, so a mis-set unit is diagnosable from `journalctl -u ironcache`.
- Scope: activation covers the RESP CLIENT listener. The Raft cluster-bus and
  replication listeners still self-bind (a follow-up), so this is aimed at the
  single-node upgrade path.

### io_uring runtime (Linux, opt-in, from-source only)

The per-shard async backend is tokio by default. An io_uring datapath exists as a
TWO-STEP opt-in -- both steps are required:

1. **Build** with the cargo feature: `cargo build --release -p ironcache
   --features io_uring`. The PUBLISHED release/rolling tarballs and the container
   image are built with DEFAULT features and EXCLUDE it, so io_uring is
   from-source only today.
2. **Select** it at run time: `--runtime io_uring`, `IRONCACHE_RUNTIME=io_uring`,
   or TOML `runtime = "io_uring"`.

Selecting it can never fail a boot: the server FALLS BACK to tokio with a one-line
log when the feature is absent from the build, the host is not Linux, the kernel
lacks io_uring, or client TLS is ON (`tls = "on"` forces tokio; rustls does not
compose with the io_uring path yet). It is also REJECTED at config validation with
`cluster_mode = "shard-owners"`. `ironcache check` reports the effective backend.

Honest performance guidance: opt in for PIPELINED workloads. On a 16-core arm64
bench host the io_uring path measured +189% throughput at pipeline depth 32, but
~6% SLOWER than tokio on a NON-pipelined (one request in flight per connection)
workload (`docs/bench/OPTIMIZATION_LOG.md`). If your clients do not pipeline, stay
on tokio; either way, A/B on your own hardware before flipping a fleet.

### Transparent hugepages (Linux)

The store tables and value blobs flow through jemalloc, and random-key workloads
are dTLB-bound on 4 KiB pages. Backing the allocator with 2 MiB THP is a measured
win: ~45% fewer dTLB misses and ~5% fewer CPU cycles on a 16-core arm64 bench host
(qps-neutral there because that workload was not TLB-bottlenecked;
`docs/bench/OPTIMIZATION_LOG.md`). It is OFF by default and there are two ways in:

- **Build-time**: `cargo build --release -p ironcache --features hugepages`
  appends `thp:always,metadata_thp:auto` to the compiled jemalloc `malloc_conf`
  (Linux-only; inert elsewhere).
- **Runtime, ANY binary including the shipped tarballs/images (no rebuild)**: set
  `_RJEM_MALLOC_CONF=thp:always` in the process environment (e.g. an
  `Environment=` line in the unit) and restart. jemalloc reads it once at process
  init, so it cannot be flipped live and there is no `CONFIG SET` for it.

Caveat -- RSS vs `maxmemory`: THP rounds resident memory up to 2 MiB granularity,
and the `maxmemory` ceiling is enforced against the allocator RSS figure, so an
inflated RSS eats eviction headroom -- leave margin between `maxmemory` and the
host/cgroup limit when enabling it. Some kernels also add occasional khugepaged
compaction latency. Verify it is live with
`grep AnonHugePages /proc/<pid>/smaps | awk '$2>0' | head`.

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
  --set image.tag=0.1.0 \
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
and keep the console Service behind a VPN-locked LB (#369). The dedicated runbook
(HA model, data-path isolation, security posture, 2-replica walkthroughs) is
[`deploy/CONSOLE_DEPLOY.md`](deploy/CONSOLE_DEPLOY.md).

---

## 6. Enabling auth and TLS

### Client AUTH (requirepass)

Set `requirepass` (TOML) or `IRONCACHE_REQUIREPASS` (env). The server hashes it at
rest (SHA-256); the plaintext never persists past config load. In Helm,
`auth.enabled=true` + `auth.password=...` (or `auth.existingSecret`). Richer ACL
users are loaded from an `aclfile` (`IRONCACHE_ACLFILE`) if you provide one.

### Least-privilege monitoring users (per-subcommand ACL)

A long-running monitoring service (the bundled console, a metrics poller, a
dashboard) should NOT hold a full-access credential: a single leaked monitoring
password must not be able to mutate or destroy anything. IronCache supports the
Redis 7 per-subcommand ACL rule `+command|subcommand` for exactly this:
`+slowlog|get` grants ONLY `SLOWLOG GET` (never `SLOWLOG RESET`), `+client|list`
grants ONLY `CLIENT LIST` (never `CLIENT KILL`), `+config|get` only `CONFIG GET`
(never `CONFIG SET`), and `+cluster|info` / `+cluster|slots` / `+cluster|shards`
/ `+cluster|nodes` / `+cluster|myid` grant the topology reads without the
`MEET` / `SETSLOT` / `FORGET` mutators a bare `+cluster` would include. The
rules compose Redis-style: `+cmd|sub` is additive on top of other rules, a later
`-cmd` removes the whole command including prior subcommand grants, a bare
`+cmd` grants every subcommand, and a `|sub` rule is rejected for a command that
has no subcommands. The containers with a per-subcommand surface today are
CLUSTER, CONFIG, CLIENT, and SLOWLOG. The generic read-only monitor shape is:

```
user monitor on >CHANGE_ME resetkeys resetchannels -@all +ping +info +slowlog|get +client|list
```

(no key access, no writes, no CONFIG, no KEYS/SCAN, no flush, no bare
`+client`/`+cluster`). Management actions belong on a SEPARATE scoped credential
supplied only at action time, never held by the long-running poller.

`deploy/aclfile.console.example` is a ready-to-adapt aclfile applying this model
to the bundled console, defining the two scoped users it authenticates as
(`IRONCACHE_CONSOLE_NODE_USER` + `IRONCACHE_CONSOLE_NODE_PASSWORD_FILE`):
`console_monitor` (read-only: PING/INFO/SLOWLOG GET/CLIENT LIST, no key access,
no mutation) for the polling replicas, and `console_admin` (the management
surface: CONFIG GET/SET, the CLUSTER mutators, INFO, SAVE, key CRUD) that is
still denied the destructive verbs (FLUSHALL, FLUSHDB, SHUTDOWN, KEYS, SWAPDB,
DEBUG, MIGRATE, SLOWLOG RESET, the destructive CLUSTER slot ops, ...) and, by
default, ACL (so a scoped admin cannot rewrite the node's users to escalate
itself; node-ACL management stays on a separate credential). Replace the
`CHANGE_ME_*` passwords, decide how the `default` user is secured, and load it via
`IRONCACHE_ACLFILE`. The exact enforcement is pinned by the
`reference_console_aclfile_loads_and_enforces_least_privilege` test.

TLS uses rustls + the `ring` provider (pure Rust, compiled into the binary by
default; no OpenSSL, no `--features` flag). The full guide (openssl cert generation,
what is and is NOT covered, the client side, and the rotation story) is
[`docs/TLS.md`](docs/TLS.md).

### Client-port TLS (public listener)

`tls=on` + `tls_cert_path` + `tls_key_path`. The client port becomes TLS-only
(plaintext clients are rejected). Server-auth only: client identity is AUTH / ACL
inside the session, NOT a client cert (no mTLS on the client port yet). In Helm,
`tls.enabled=true` + the cert/key. Connect with a TLS-capable Redis client
(`redis-cli --tls --cacert ...`); the built-in `ironcache cli` is plaintext-only.

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

### Rotating a certificate

**Client listener: hot reload on `SIGHUP` (#563), no restart.** Replace the
`tls_cert_path` / `tls_key_path` files in place and send the node `SIGHUP`
(`kill -HUP <pid>`). It re-reads those paths, rebuilds + validates the config, and
atomically swaps it in: new handshakes present the new cert, existing connections are
undisturbed. A bad or missing replacement is logged and REJECTED, keeping the previous
good cert live (the listener is never torn down), so re-issue a valid pair and `SIGHUP`
again.

**Cluster bus (`cluster_tls`): still restart-only.** `SIGHUP` does not reload the
intra-cluster cert. To rotate it, replace the cluster cert/key files and RESTART the
node; in a cluster do a rolling restart (one node at a time, waiting for each to rejoin
healthy). A node presenting a new cert signed by the same cluster CA is accepted by peers
that have not yet rotated, so a rolling rotation needs no flag-day. See
[`docs/TLS.md`](docs/TLS.md) for the full procedure.

---

## 7. Health, readiness, and metrics

The endpoint is ON by default at `127.0.0.1:9091` (override with `--metrics-addr
<ip:port>`, disable with `--metrics-addr off`). It serves:

- `GET /livez` -> `200` once the process is up and serving (liveness). The
  Kubernetes livenessProbe uses this -- it restarts a hung pod.
- `GET /readyz` -> `200` only when EVERY shard has finished load-on-boot AND, in
  raft mode, a leader is known; `503` otherwise. The readinessProbe uses this -- a
  node with a large snapshot to load, or one that has not yet joined a quorum,
  stays out of the client Service and pauses the rolling update until it is
  genuinely ready.
- `GET /metrics` -> Prometheus exposition (per-shard counter rollup + process and
  raft gauges). Scrape it directly, or enable the chart's `metrics.serviceMonitor`.

Full catalog of every `ironcache_*` series and the key `INFO` fields is in
[`docs/METRICS.md`](docs/METRICS.md); a starter Grafana dashboard ships in the
chart at
[`deploy/helm/ironcache/dashboards/`](deploy/helm/ironcache/dashboards/) (set
`metrics.grafanaDashboard.enabled=true` to auto-provision it) and the Prometheus
alert rules in [`deploy/helm/ironcache/alerts/`](deploy/helm/ironcache/alerts/) (set
`metrics.prometheusRule.enabled=true` to ship them as a PrometheusRule).

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

The reference container image (`Dockerfile.console`), the Helm values
(`console.*`), and the raw manifests (`deploy/k8s/ironcache-console.yaml`, including
an egress-restricting NetworkPolicy) ship the HA packaging (#363). The full
deployment runbook -- the HA model, the data-path-isolation guarantee, the security
posture, and the two-replicas-behind-an-LB walkthroughs for compose and k8s/Helm --
is [`deploy/CONSOLE_DEPLOY.md`](deploy/CONSOLE_DEPLOY.md).

---

## 8. Persistence, RPO, and the PVC

`data_dir` is the SINGLE enable switch for durable persistence (the on-disk
snapshot `dump-shard-<n>.icss` + `dump.manifest`) AND the durable Raft log
(`ironcache-raft-<bus-port>.log`). With no `data_dir` the node is purely in-memory
and the Raft log lands in the OS temp dir (lost on a `/tmp`-clearing reboot) -- so
ALWAYS set `data_dir` (onto a PVC) for a cluster.

RPO is governed by the save policy: `save_interval_secs` + `save_min_changes` (the
Redis `save <seconds> <changes>` cadence). Two different defaults are in play, so
be precise about which you are running. The BUILT-IN default is
`save_interval_secs = 0` -- periodic saves are OFF until you configure a cadence
(a final save on graceful shutdown STILL runs whenever `data_dir` is set, so a
data_dir-only setup has shutdown-save durability but loses everything since the
last explicit SAVE/BGSAVE on an ungraceful crash). The deploy artifacts in this
repo (`deploy/ironcache.example.toml`, compose, Helm, raw k8s) all configure
900s / >=1 change, which means up to ~15 minutes of writes can be lost on an
ungraceful crash of a single node; tighten the interval for a smaller RPO at the
cost of more I/O. A graceful shutdown performs a final save-on-exit, and the
StatefulSet's `terminationGracePeriodSeconds` (60s) covers that drain. In a cluster,
`min_replicas_to_write` bounds how many ACKNOWLEDGED writes a failover can lose to
the async-replication window: an owner rejects a write unless it has that many
in-sync replicas, so an acked write is known to be on that many nodes.

Each pod gets its own PVC via the StatefulSet `volumeClaimTemplate`
(`ReadWriteOnce`, default 10Gi); set `persistence.storageClassName` to an SSD class
in production. PVCs are NOT deleted by `helm uninstall` / `kubectl delete sts` --
remove them explicitly when you mean to discard data.

### Dedicated persist core (snapshot tail latency)

During a save the off-datapath `ic-persist-<shard>` thread reads the frozen keyspace to
encode + fsync it. Under the thread-per-core model that thread is an EXTRA runnable thread
on top of the shard threads, so on a fully-pinned box (all cores serving) it lands on a
serving core and steals serving time, stretching the request-latency tail during the save.
`persist_cpu` (env `IRONCACHE_PERSIST_CPU`, CLI `--persist-cpu`) dedicates a core to it so
the encode runs off the datapath cores. It is Linux-only (a no-op elsewhere) and defaults
to `off` (no pin, unchanged behavior).

The recommended deployment gives the server ONE extra core: pin the datapath (shards) to
`0..N-1` with `taskset` and set `persist_cpu` to core `N`. `sched_setaffinity` is bounded
by the process cpuset, not by the inherited `taskset` mask, so the persist thread escapes
onto core `N` even though the datapath is confined to `0..N-1`:

```sh
# 8 datapath cores (0-7) + 1 dedicated persist core (8). The persist thread escapes the
# taskset mask onto core 8; the shards stay on 0-7.
IRONCACHE_PERSIST_CPU=8 taskset -c 0-7 ironcache --port 6379 --shards 8 server
```

On Kubernetes, request one extra CPU beyond the shard count, set the pod's CPU manager
policy to `static` (so the datapath cores are exclusive), and set `IRONCACHE_PERSIST_CPU`
to a reserved core id. `auto` instead reserves the HIGHEST core of the process's current
affinity mask, which suits a deployment that has already confined the datapath to the
lower cores via a cpuset.

Trade-off (the tunability tenet: a knob with a safe default): dedicating a core costs you a
serving (or spare) core, so it only pays off on a host with a core to spare. And it is a
PARTIAL fix: pinning removes the persist thread's CPU steal but not its MEMORY-BANDWIDTH
share (it still reads the frozen keyspace), so it closes part, not all, of the gap to
millisecond-class snapshot tails. Confirm the pin is live: the server logs
`persist thread pinned to cpu(s) [...]` once on the first save, and
`CONFIG GET persist-cpu` reports the effective value.

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

A downgrade has a second boot hazard: the config FILE. If a newer build wrote a config
key the older binary does not recognize, the old binary hard-fails at boot (the config
file is STRICT by default, so an unknown key is a typo-guard, not a silent drop). To keep
a rollback from bricking on a forward-incompatible key, set the config-rollback escape
hatch on the OLD binary: `ignore_unknown_config_keys = true`
(`IRONCACHE_IGNORE_UNKNOWN_CONFIG_KEYS=true`, or `--ignore-unknown-config-keys`). It turns
an unknown FILE key into a loud WARN that NAMES the ignored key and lets the node boot,
while still applying every key the old binary DOES understand; it relaxes unknown keys
only, so a malformed file or a bad value still fails. Leave it OFF in steady state (so a
real typo is still caught) and set it only for the rollback (CONFIG.md, #527).

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

### Cluster on bare metal (three hosts)

The same turnkey formation works on three plain Linux hosts (VMs or metal, any
cloud) with no orchestrator -- the per-node configs below are the
`deploy/compose/config/node*.toml` pattern with real host addresses. Install the
binary + units on each host first (section 3 "Bare-metal install", including the
persistence edit -- a cluster node NEEDS `data_dir` for its durable Raft log).

**1. One TOML per node** at `/etc/ironcache/ironcache.toml` (read by default). All
three files are IDENTICAL except `cluster_announce_id`. Node ids are any stable
40-hex strings (`openssl rand -hex 20` once per node); each node's
`cluster_announce_id` MUST equal its own `[[cluster_topology.nodes]]` entry's `id`
(`Config::validate` enforces the match and rejects slot gaps/overlaps/dup ids --
pre-flight with `ironcache check --config /etc/ironcache/ironcache.toml`). Host A:

```toml
bind = "0.0.0.0"
port = 6379
cluster_enabled = true
cluster_mode = "raft"
cluster_announce_id = "<HOST-A-40-hex-id>"   # the ONLY per-node line
data_dir = "/var/lib/ironcache"
save_interval_secs = 900
save_min_changes = 1
min_replicas_to_write = 0                    # see the -NOREPLICAS trap below

[[cluster_topology.nodes]]
id = "<HOST-A-40-hex-id>"
host = "10.0.0.1"                            # DNS names also work (resolved lazily)
port = 6379
slots = [[0, 5460]]

[[cluster_topology.nodes]]
id = "<HOST-B-40-hex-id>"
host = "10.0.0.2"
port = 6379
slots = [[5461, 10922]]

[[cluster_topology.nodes]]
id = "<HOST-C-40-hex-id>"
host = "10.0.0.3"
port = 6379
slots = [[10923, 16383]]
```

**2. Network**: open THREE ports between the hosts -- `6379` (client), `16379`
(cluster bus, `port + 10000`), `26379` (replication, `port + 20000`) -- plus
`9121` from your monitoring network if you scrape metrics. The bus/repl ports are
derived, never configured.

**3. Secret**: set the SAME `IRONCACHE_CLUSTER_SECRET` on every node (a systemd
drop-in `Environment=` line, or TOML `cluster_secret`); without it the node boots
with a LOUD unauthenticated-bus warning. Add `cluster_tls` (section 6) when the
inter-host network is not trusted.

**4. Boot all three** (`systemctl restart ironcache`), in ANY order -- peer
addresses resolve lazily at dial time, so an early node waits for the others. On a
FRESH cluster the elected leader auto-applies the declared topology (the turnkey
formation above): no `CLUSTER MEET`, no `ADDSLOTS`.

**5. Verify** from any host:

```sh
redis-cli -c -h 10.0.0.1 cluster info    # cluster_state:ok, cluster_slots_assigned:16384,
                                         # cluster_known_nodes:3, cluster_raft_leader:<id>
redis-cli -c -h 10.0.0.1 cluster slots   # the three ranges above
redis-cli -c -h 10.0.0.1 set smoke ok    # -c follows the -MOVED redirect to the owner
```

**The -NOREPLICAS trap.** A static topology declares PRIMARY ownership only -- no
node starts as a replica of any slot. So `min_replicas_to_write >= 1` on a fresh
cluster fails EVERY write with `-NOREPLICAS Not enough good replicas to write.`
Keep it 0 until replicas exist, then add them at runtime:
`CLUSTER REPLICATE <node-id> <slot> [slot ...]` -- note the IronCache-specific
PER-SLOT argument shape (Redis's `CLUSTER REPLICATE` takes a whole node and there
is no `REPLICAOF`/`SLAVEOF` here). The replica full-syncs over the repl plane and
serves reads only on `READONLY` connections.

**Scale-out (adding a fourth host).** The Raft-native join flow is: the new node
boots as a NON-VOTER (`cluster_raft_joining = true` in its TOML, or
`IRONCACHE_CLUSTER_RAFT_JOINING=true` for per-pod injection in a stateful set, with
`cluster_mode = "raft"` and the FULL topology including itself), the operator runs
`CLUSTER MEET <host> <client-port>` ON THE LEADER (a membership change is NOT
forwarded like slot writes -- find the leader via `cluster_raft_leader` in
`CLUSTER INFO` or the `,leader` mark in `CLUSTER NODES`), the joiner is staged as a
non-voting LEARNER (the quorum stays 3), AUTO-PROMOTES to a voter once its log
catches up, and `CLUSTER FORGET <id>` removes it again (quorum-guarded). This is
integration-tested end to end
(`raft_mode_meet_stages_a_learner_auto_promotes_then_forget_removes_it` in
`crates/ironcache/tests/raft_cluster.rs`).

**Moving slots after growth**: `CLUSTER REBALANCE` (or `... DRYRUN`) prints the
read-only per-node plan (`current_slots` / `target_slots` / `slots_to_move`).
`CLUSTER REBALANCE APPLY` (raft mode only) ARMS up to 128 moves per call as
committed MIGRATING + IMPORTING pairs, which auto-copies each slot's keys to the
destination; it deliberately does NOT flip ownership. Finalize each slot with
`CLUSTER SETSLOT <slot> NODE <dest-id>` once `CLUSTER COUNTKEYSINSLOT <slot>` on
the destination shows it caught up, then re-run APPLY for the next batch
(idempotent and resumable; clients see standard `-ASK` redirects during the move,
which `redis-cli -c` and real cluster clients follow).

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
