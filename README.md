<p align="center">
  <img src="docs/assets/ironcache-logo.png" alt="IronCache" width="320">
</p>

# IronCache

**A Redis-compatible cache in one static Rust binary: thread-per-core, replicated, clustered.**

IronCache speaks the Redis wire protocol (RESP2 and RESP3) and keeps the observable
Redis contract for the commands it implements, so existing Redis clients, libraries,
and `redis-cli` work against it unchanged. It is a shared-nothing, thread-per-core
engine: the keyspace is sharded so each shard is owned and mutated by exactly one
core, with no hot-path locks. It ships as a single static binary that carries the
server plus the operator tooling (config preflight, a verified self-upgrade, a
connectivity smoke client).

The engine is functional and broad: 196 client-facing commands (what a live server
reports via `COMMAND COUNT`; the registry is `CLIENT_COMMAND_NAMES` in
`crates/ironcache-server/src/command_spec.rs`) across all the core
data types, transactions, pub/sub with keyspace notifications, blocking commands,
on-disk persistence, and an opt-in Raft-governed multi-node cluster with replication,
automatic failover, and online slot migration. It is exercised by 2,600+ in-tree
tests, a differential harness that proves byte-for-byte RESP parity against
`redis-server`, and a real client-driver matrix (redis-py, go-redis, ioredis) in both
single-node and cluster mode.

This project is also an experiment in method: it uses AI to mine prior art, propose
approaches, and adversarially verify every load-bearing claim before trusting it. The
[research corpus](docs/research/) and the version-pinned
[`claims.yaml`](docs/prior-art/claims.yaml) are the output of that process.

---

## Features

### Wire protocol and data types

- **RESP2 and RESP3**, negotiated by `HELLO`, with the verbatim Redis error catalog.
- **Strings and numerics**: GET/SET (with the full option set), GETSET, GETDEL,
  GETEX, SETEX/PSETEX/SETNX, APPEND, STRLEN, GETRANGE/SETRANGE/SUBSTR,
  INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT, MGET/MSET/MSETNX.
- **TTL / expiry**: EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT, TTL/PTTL,
  EXPIRETIME/PEXPIRETIME, PERSIST, with active and lazy reaping.
- **Lists**: LPUSH/RPUSH(/X), LPOP/RPOP, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM,
  LPOS, LMOVE/RPOPLPUSH, LMPOP.
- **Hashes**: HSET/HMSET/HSETNX, HGET/HMGET/HGETALL/HKEYS/HVALS, HDEL, HLEN, HEXISTS,
  HSTRLEN, HINCRBY/HINCRBYFLOAT, HRANDFIELD, HSCAN.
- **Sets**: SADD/SREM, SMEMBERS, SISMEMBER/SMISMEMBER, SCARD, SPOP, SRANDMEMBER,
  SMOVE, SINTER/SUNION/SDIFF (and the STORE + CARD variants), SSCAN.
- **Sorted sets**: ZADD, ZREM, ZSCORE/ZMSCORE, ZRANK/ZREVRANK, ZINCRBY, ZCARD,
  ZCOUNT/ZLEXCOUNT, the full ZRANGE family (by index/score/lex, plus ZRANGESTORE),
  ZPOPMIN/ZPOPMAX, ZMPOP, ZRANDMEMBER, ZUNION/ZINTER/ZDIFF (and the STORE / CARD
  variants), ZREMRANGEBY*, ZSCAN.
- **Bitmaps**: SETBIT/GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD/BITFIELD_RO.
- **HyperLogLog**: PFADD, PFCOUNT, PFMERGE (Redis-compatible dense representation).
- **Generic keyspace**: DEL/UNLINK, EXISTS, TYPE, KEYS, SCAN, DBSIZE, RANDOMKEY,
  RENAME/RENAMENX, COPY, MOVE, SWAPDB, TOUCH, FLUSHDB/FLUSHALL, OBJECT, SORT/SORT_RO.

### Transactions, pub/sub, and blocking

- **Transactions**: MULTI/EXEC/DISCARD with WATCH/UNWATCH dirty-CAS.
- **Pub/Sub**: SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE/PUNSUBSCRIBE, PUBLISH, PUBSUB
  introspection, fanned out across shards by a cross-shard coordinator.
- **Keyspace notifications**: the Redis `notify-keyspace-events` keyspace and
  keyevent events (including `expired` / `evicted`), delivered through the same
  Pub/Sub fan-out. Disabled by default; the write hot path pays nothing until a flag
  is set.
- **Blocking commands**: BLPOP/BRPOP, BLMOVE/BRPOPLPUSH, BLMPOP, BZPOPMIN/BZPOPMAX,
  BZMPOP, and WAIT.

### Architecture

- **Thread-per-core, shared-nothing**: each shard is owned by one pinned core and
  mutated by it alone, so there are no hot-path locks. Rust ownership makes the "one
  core owns one shard" rule a compile-time guarantee.
- **Per-shard accept** via `SO_REUSEPORT`, with a cross-shard coordinator for
  multi-key, whole-keyspace, and pub/sub commands.
- **A swappable Runtime seam**: the data path is written against a `Runtime` trait,
  with a portable tokio (epoll/kqueue) implementation and an **optional io_uring
  datapath** on Linux (default-off, opt-in; see [Build features](#build-features))
  behind the same seam.
- **A Dash extendible-hashing index by DEFAULT**: since the #285 flip, each slot's
  key index is the Dash table (the `ironcache-dashtable` crate) instead of a
  SwissTable. Segments grow one at a time, so there is no power-of-two doubling
  trough and bytes/key stays FLAT as the keyspace grows (measured, see
  [Benchmarks](#benchmarks)), at throughput parity. The SwissTable arm stays built
  and CI-gated behind the `hashbrown-index` build feature, so the flip is
  reversible.
- **Eviction**: a `maxmemory` ceiling with a configurable policy (default
  `allkeys-lru`).

### Durability and persistence

- **On-disk snapshot**: SAVE / BGSAVE / LASTSAVE write a per-shard snapshot
  (`dump-shard-<n>.icss`) plus a manifest under `data_dir`.
- **Load on boot**: a node with a `data_dir` restores its keyspace at startup;
  `/readyz` does not report ready until every shard has finished loading.
- **Save policy**: `save_interval_secs` + `save_min_changes` (the Redis
  `save <seconds> <changes>` cadence), with a final save on graceful shutdown.
- **Write-side durability bound** in a cluster: `min-replicas-to-write` /
  `min-replicas-max-lag` (Redis-style, default off) refuses a write (`-NOREPLICAS`)
  unless enough replicas are in sync, bounding the failover loss window.

### Clustering and high availability (opt-in)

- **Raft-governed control plane**: the 16384-slot ownership map, the config epoch,
  the node roster, and replica roles live in a replicated log. User data never enters
  the Raft log; only the cluster control state does.
- **Slot routing**: CRC16 slot hashing (Redis-identical), with `-MOVED` and `-ASK`
  redirects exactly like Redis Cluster.
- **Replication**: asynchronous per-slot replication with a forkless full-sync, plus
  bounded-staleness **read-replicas** (a `READONLY` client reads a replica only while
  it is within the lag bound, otherwise the read `MOVED`s to the owner).
- **Automatic failover**: an in-sync replica is promoted through a committed
  `PromoteReplica` entry (a stale replica is never promoted); the committed apply is
  the fence, so a promotion never creates two owners.
- **Online slot migration**: `MIGRATING` / `IMPORTING` + `ASK` / `ASKING` + a single
  committed ownership flip, with zero downtime and exactly one owner at the flip
  boundary.
- **Turnkey formation**: in raft mode a fresh cluster auto-applies its static
  topology (node table + slot ownership) through the log and reaches
  `cluster_state:ok` with no operator `CLUSTER MEET` / `ADDSLOTS`; the auto-apply is
  fresh-only and idempotent, so a restart never re-bootstraps.
- **Robustness**: Pre-Vote and check-quorum, a chunked `InstallSnapshot` path to
  catch up a far-behind or newly added node, a disk-backed (spillable) replication
  backlog with incremental resume, runtime voter-set reconfiguration with learners,
  and leader-hint forwarding (a follower forwards a cluster proposal to the leader and
  relays the commit).
- **Split-brain fence**: slot ownership moves only through the committed Raft log, and
  every change bumps a monotonic config epoch, so there is never a committed state
  with two owners of a slot. The failure-prone paths are proven in a deterministic
  simulation over thousands of seeded partition/crash/heal timelines, exercised over
  real TCP loopback, and validated end to end on a live multi-process AWS cluster.

The default single-node and static-topology paths are **byte-unchanged** when
clustering is off; a node run without `cluster_mode = "raft"` pays zero new hot-path
cost. See [Clustering and high availability](docs/design/) and `DEPLOY.md` for the
full contract.

### Security

- **AUTH / requirepass**, stored as a SHA-256 digest **at rest** (never plaintext)
  and compared in constant time.
- **Full ACL**: per-user enable/disable, password rules, command and category rules
  (`+@read`, `-@dangerous`, ...), key patterns, and channel patterns, via
  `ACL SETUSER/GETUSER/DELUSER/LIST/USERS/CAT/WHOAMI/GENPASS/LOAD/SAVE`, with an
  optional `aclfile`. ACL passwords are SHA-256 at rest.
- **TLS** on three planes: the public client port (`tls`), the **cluster bus**, and
  the **replication** link (`cluster_tls`, with peer-cert verification against a CA).
  rustls + `ring` (pure Rust, compiled in by default, no OpenSSL). Cert generation,
  mTLS scope, and the rotation story are in [`docs/TLS.md`](docs/TLS.md).
- **Cluster peer auth**: a shared `cluster_secret` presented in a constant-time
  handshake on the bus and replication links.
- **Secret hygiene**: secret arguments are redacted from SLOWLOG, INFO, and
  logs; the long-lived `cluster_secret` and transient plaintext are held in
  `Zeroizing` and scrubbed from the heap. The scope (what is and is not protected, and
  why) is documented in `SECURITY.md` and `docs/THREAT_MODEL.md`.

### Operability

- **HTTP health and metrics** (on by default at `127.0.0.1:9091`; override with
  `--metrics-addr`, disable with `--metrics-addr off`): `/livez` (liveness),
  `/readyz` (ready only when every shard has loaded and, in raft mode, a leader is
  known), and `/metrics` (Prometheus exposition: per-shard counters plus process and
  raft gauges). Every series is cataloged in `docs/METRICS.md`.
- **Introspection**: INFO, CLIENT, COMMAND (a real command table for cluster-aware
  clients), CLUSTER, OBJECT, SLOWLOG, MEMORY, LATENCY.
- **DoS guards**: `maxmemory` with eviction, `maxclients`, an idle-connection
  timeout, and a per-connection output-buffer bound.
- **On-call runbook**: [`docs/RUNBOOK.md`](docs/RUNBOOK.md) indexes every
  operator-visible error string, log line, and probe state with a symptom-to-action
  diagnostic sequence.

### Seamless upgrades

- **`ironcache upgrade`**: a verified, data-safe, self-rolling-back binary self-update
  that swaps a running node to a new version. It verifies the new artifact (SHA-256
  against `SHA256SUMS`, behind a pluggable verifier seam for signature anchors), takes
  an fsync'd snapshot first, swaps the binary atomically while keeping exactly one
  rollback slot (the live path is never absent, even if the process is killed
  mid-swap), restarts the node, and health-gates the result: `/readyz`, a real
  process-restart proof (the `ironcache_uptime_seconds` reset, so a no-op restart or a
  stale process cannot false-pass), a version match, and a stabilization window. Any
  miss auto-rolls-back to the previous binary.
- **Lossless across the restart**: before the snapshot it issues a node-wide
  `CLIENT PAUSE WRITE` (writes hold; reads and admin like SAVE keep serving) so no
  acknowledged write is lost in the save-to-reload window; `--no-freeze` opts out. A
  failed upgrade unpauses and leaves the node untouched.
- **Three upgrade shapes**: the in-place single-node upgrade above (fed from a local
  `--binary`, a `--from-url` tarball, or `--to <TAG>|latest` fetched from GitHub
  Releases); a whole-cluster rolling upgrade (`ironcache upgrade --cluster
  --inventory <FILE> --to <TAG>`: replicas first, promote an in-sync replica behind
  the failover freeze, old primary last, `--dry-run` to preview the plan); and an
  opt-in SIGUSR1 **streamed live cutover** on a node whose config sets
  `handoff_socket` (the old process streams its keyspace to a re-exec'd sibling that
  inherits the client listener, and exits only on commit; abort keeps the old
  process serving). The operator runbook is [`docs/UPGRADE.md`](docs/UPGRADE.md).
- Validated end to end on a live AWS node: an upgrade under continuous concurrent
  writes preserved every acknowledged write, the full keyspace, and the ACL users.

### Deployment

- A multi-stage, non-root, distroless container image (`Dockerfile`) published to
  GHCR.
- A **Helm chart** (`deploy/helm/ironcache`) and equivalent raw **Kubernetes**
  manifests (`deploy/k8s/`), deploying a StatefulSet with headless + client Services,
  a PDB, a PVC for `data_dir`, and `/livez` + `/readyz` probes.
- **docker-compose** for a single node and a 3-node Raft cluster (`deploy/compose/`).
- A bundled **monitoring console** (`crates/ironcache-console`, published as
  `ghcr.io/elares/ironcache-console`, built from `Dockerfile.console`): a separate,
  stateless dashboard server that polls the nodes as a scoped ACL user and never
  sits on the data path. The HA deployment runbook is
  [`deploy/CONSOLE_DEPLOY.md`](deploy/CONSOLE_DEPLOY.md).
- A shipped **Grafana dashboard** (`deploy/helm/ironcache/dashboards/ironcache-dashboard.json`) and
  **Prometheus alert rules** (`deploy/helm/ironcache/alerts/ironcache-alerts.yml`) over the
  `/metrics` series cataloged in [`docs/METRICS.md`](docs/METRICS.md).
- **CalVer rolling releases** on every push to `main` plus formal `v*` releases:
  reproducible `musl` + `glibc` tarballs for **amd64 and arm64**, a consolidated
  `SHA256SUMS`, and a keyless Sigstore build-provenance attestation on both
  channels; a formal `v*` release additionally attaches a CycloneDX SBOM and a
  minisign signature over `SHA256SUMS` (see [`RELEASING.md`](RELEASING.md)).

See [`DEPLOY.md`](DEPLOY.md) for the full deployment guide, every config key, the
ports, and what was validated offline versus on a live cluster.

---

## Compatibility

IronCache speaks RESP2 and RESP3 and honors the observable Redis contract for the
commands it implements. Compatibility is tiered and explicit: a command is either
supported with Redis-identical semantics, or it is documented as unsupported. We do
not bend the wire protocol or a command's observable behavior to win a benchmark.

- **Differential-tested**: a harness drives identical command streams at IronCache and
  a real `redis-server` and asserts byte-for-byte RESP equality, so a divergence
  surfaces as a reviewable failure (see
  [docs/design/DIFFERENTIAL_TESTING.md](docs/design/DIFFERENTIAL_TESTING.md)).
- **Real client drivers validated** in both single-node and cluster mode (54 checks,
  all passing): **redis-py 6.4.0**, **go-redis v9.7.0**, and **ioredis 5.11.1**. The
  cluster checks confirm topology discovery via `CLUSTER SLOTS` and `MOVED`-routing
  end to end. The one documented gap is a client limitation, not an IronCache defect:
  ioredis is RESP2-only and cannot decode the RESP3 map byte (redis-py and go-redis
  negotiate RESP3 against the same server cleanly). The full matrix is in
  [tests/drivers/DRIVER_MATRIX.md](tests/drivers/DRIVER_MATRIX.md).

A few deliberate model differences from single-node Redis are documented rather than
silently wrong: a single-node MULTI/EXEC (and a cross-shard multi-key move) requires
the keys to share a shard, mirroring the cluster contract that a transaction's keys
must share a slot (co-locate them with a `{hash tag}`).

- **Connecting a client**: copy-paste connect + SET/GET/PING snippets for redis-py, go-redis,
  and ioredis are in [docs/CLIENT_LIBRARIES.md](docs/CLIENT_LIBRARIES.md).
- **Command coverage**: the supported commands by category are in
  [docs/COMMAND_COVERAGE.md](docs/COMMAND_COVERAGE.md) (the command registry is the source of
  truth; a live server reports its set via `COMMAND LIST` / `COMMAND COUNT`).

---

## Install

**Platforms.** Linux is the production target (the io_uring datapath, transparent
huge pages, and systemd socket activation are Linux-only). macOS is supported for
development builds. Windows is unsupported (no artifacts, untested).

### Prebuilt binaries (Linux)

Every push to `main` publishes a CalVer **rolling release** (a normal, non-prerelease
release, so `releases/latest` is always the newest build), and formal `v*` releases
are cut from the same pipeline. Each release carries static `musl` tarballs for
x86_64 and aarch64 Linux plus glibc-2.17-pinned `gnu` fallbacks (asset scheme
`ironcache-<version>-linux-<amd64|arm64>-<musl|glibc>.tar.gz`, the binary at the
tarball root), a consolidated `SHA256SUMS`, and a keyless Sigstore build-provenance
attestation. Formal `v*` releases additionally attach a CycloneDX SBOM and a
minisign signature over `SHA256SUMS`; the verification recipes live in
[`RELEASING.md`](RELEASING.md).

```sh
# Fetch the newest build for your platform (here arm64 musl; swap in amd64 and/or
# glibc) plus the checksum manifest. `gh release download` defaults to releases/latest.
gh release download --repo ELares/IronCache \
  --pattern 'ironcache-[0-9]*-linux-arm64-musl.tar.gz' --pattern SHA256SUMS

# Verify integrity + provenance, then install.
sha256sum -c SHA256SUMS --ignore-missing
gh attestation verify ironcache-[0-9]*-linux-arm64-musl.tar.gz --repo ELares/IronCache
tar -xzf ironcache-[0-9]*-linux-arm64-musl.tar.gz
sudo install -m 0755 ironcache /usr/local/bin/ironcache
ironcache --version
```

Without `gh`, download the same two assets from the GitHub releases page (the asset
name embeds the version) and run the same `sha256sum -c SHA256SUMS --ignore-missing`.

### Docker

```sh
docker pull ghcr.io/elares/ironcache:latest
```

Images build only on `v*` tags, so `:latest` tracks the last formal `v*` release and
can LAG the rolling binary channel above by days of merges. Immutable pins drop the
`v` prefix: tag `v0.1.0` publishes `ghcr.io/elares/ironcache:0.1.0`.

### From source

You need `rustup` (the repo's `rust-toolchain.toml` auto-pins the exact stable
toolchain on the first `cargo` invocation; the workspace MSRV is 1.85, edition 2024)
and a C compiler such as gcc or clang (the `ring` crypto dependency compiles C).

```sh
git clone https://github.com/ELares/IronCache
cd IronCache
cargo build --release -p ironcache
target/release/ironcache server
```

### Build features

The published release binaries are built with **default features only**
(`default = []` on the `ironcache` crate). Three opt-in build features exist, all
from-source:

- **`io_uring`**: the Linux io_uring datapath. Enabling it is a TWO-step opt-in:
  build with `cargo build --release -p ironcache --features io_uring` AND select it
  at boot with `--runtime io_uring` (or `IRONCACHE_RUNTIME=io_uring`, or TOML
  `runtime = "io_uring"`). It is honored only on a Linux build with the feature and
  TLS off; in every other case (feature absent, non-Linux, TLS on) the boot LOGS a
  one-line fallback and serves on tokio, never failing to start. It is NOT in the
  published release binaries, and its measured wins are on PIPELINED workloads:
  it ELIMINATES tokio's single-endpoint deep-pipeline cliff (+187% at pipeline
  depth 32, artifact in
  [docs/bench/IOURING_DATAPATH_BENCH.md](docs/bench/IOURING_DATAPATH_BENCH.md)),
  at the cost of about 8% at pipeline 1 (no pipelining).
- **`hugepages`**: backs the store tables and value blobs with transparent huge
  pages by appending `thp:always,metadata_thp:auto` to the baked-in jemalloc
  `malloc_conf`. The same behavior is available at RUNTIME on any binary (including
  the shipped ones, no rebuild) via `_RJEM_MALLOC_CONF=thp:always`. Measured (A/B in
  [docs/bench/OPTIMIZATION_LOG.md](docs/bench/OPTIMIZATION_LOG.md)): ~45% fewer dTLB
  misses and ~5% fewer cycles on both index arms, qps-neutral. Default off because
  `thp:always` can raise RSS against the `maxmemory` ceiling.
- **`hashbrown-index`**: the SwissTable fallback arm for the store's per-slot index
  (the pre-#285 default), kept fully CI-gated so the Dash-index flip is reversible.

---

## Quick start

### Run it in one command (Docker)

The fastest way to a running, Redis-compatible server. This maps the client port and starts
the server in the foreground:

```sh
docker run --rm -p 6379:6379 ghcr.io/elares/ironcache:latest server --bind 0.0.0.0
```

In another terminal, point any Redis client at it (here `redis-cli`):

```sh
redis-cli -p 6379 PING             # -> PONG
redis-cli -p 6379 SET hello world  # -> OK
redis-cli -p 6379 GET hello        # -> "world"
```

That is a full Redis-compatible server: `redis-cli`, redis-py, go-redis, ioredis, and any
other RESP client work against it unchanged (see
[docs/CLIENT_LIBRARIES.md](docs/CLIENT_LIBRARIES.md)). For a persistent, metrics-exposed
container (a named data volume plus the `/metrics` + `/readyz` endpoints), see
[Run the container](#run-the-container) below.

### Build and run from source

You need `rustup` and a C compiler (see [Install: from source](#from-source); the
pinned toolchain installs itself, MSRV 1.85, edition 2024).

```sh
cargo build --workspace
cargo test --workspace          # 2,600+ tests

# boot the server on every core (sharded, thread-per-core) and talk to it with any
# Redis client
cargo run -p ironcache -- server
redis-cli -p 6379 SET hello world   # -> OK
redis-cli -p 6379 GET hello         # -> "world"

# one binary, six modes: server (default) | cli | check | config | upgrade | bench.
# `ironcache cli` is a PING-only connectivity smoke check (NOT a REPL); use
# redis-cli or any Redis client for real commands, as above.
cargo run -p ironcache -- cli -p 6379   # -> +PONG
cargo run -p ironcache -- check         # preflight: validate config + self-check
cargo run -p ironcache -- config        # print the effective configuration
cargo run -p ironcache -- upgrade --binary ./ironcache --sha256sums ./SHA256SUMS
# `bench` is a stub (tracked by #8); `upgrade` is the verified data-safe
# self-update (see "Seamless upgrades" and docs/UPGRADE.md)
```

### Runnable examples

Small, self-contained programs that connect to a running IronCache over RESP and demonstrate
common use. Start a server first (`cargo run -p ironcache -- server`, listening on
`127.0.0.1:6379`), then in another terminal run any of:

```sh
cargo run -p ironcache --example hello_world   # PING + SET / GET / DEL
cargo run -p ironcache --example expiry        # SET with EX, TTL, wait, then GONE
cargo run -p ironcache --example pipeline      # a pipelined batch of commands
cargo run -p ironcache --example pubsub        # SUBSCRIBE + PUBLISH across two connections
cargo run -p ironcache --example transactions  # MULTI / EXEC atomic block
```

Each prints what it does and asserts the result. They share a tiny standard-library-only RESP
helper (`crates/ironcache/examples/common/resp.rs`) so they add no dependencies; for real
applications use a real client library (see [docs/CLIENT_LIBRARIES.md](docs/CLIENT_LIBRARIES.md)).
Point `IRONCACHE_ADDR` at a different `host:port` to run them against another server.

### Run the container

```sh
docker run -d --name ironcache \
  -p 6379:6379 -p 9121:9121 \
  -v ironcache-data:/var/lib/ironcache \
  -e IRONCACHE_DATA_DIR=/var/lib/ironcache \
  ghcr.io/elares/ironcache:latest \
  server --bind 0.0.0.0 --metrics-addr 0.0.0.0:9121

redis-cli -p 6379 ping
curl localhost:9121/readyz
```

A turnkey 3-node Raft cluster is one command away:
`docker compose -f deploy/compose/docker-compose.cluster.yml up -d` (the formation,
ports, and teardown are in [`DEPLOY.md`](DEPLOY.md)).

### Configuration

Configuration is layered, highest precedence first:

```
runtime CONFIG SET  >  CLI flags  >  IRONCACHE_* env vars  >  TOML file  >  built-in defaults
```

The most common knobs (every key, with its env var, is in
[`DEPLOY.md`](DEPLOY.md)):

| Key (TOML) | Env var | Meaning |
| --- | --- | --- |
| `bind` / `port` | `IRONCACHE_BIND` / `IRONCACHE_PORT` | listen address and client port (default 6379) |
| `shards` | `IRONCACHE_SHARDS` | per-core runtimes (default = available parallelism) |
| `maxmemory` / `maxmemory_policy` | `IRONCACHE_MAXMEMORY` / `..._POLICY` | memory ceiling + eviction policy |
| `maxclients` | `IRONCACHE_MAXCLIENTS` | max connections (default 10000) |
| `requirepass` | `IRONCACHE_REQUIREPASS` | client AUTH password (hashed at rest) |
| `aclfile` | `IRONCACHE_ACLFILE` | ACL users loaded at boot |
| `data_dir` | `IRONCACHE_DATA_DIR` | durable snapshot + Raft-log dir (enables persistence) |
| `save_interval_secs` / `save_min_changes` | `IRONCACHE_SAVE_*` | periodic save cadence |
| `tls` + `tls_cert_path` + `tls_key_path` | `IRONCACHE_TLS*` | TLS on the client port |
| `cluster_enabled` / `cluster_mode` | `IRONCACHE_CLUSTER_*` | turn on clustering; `static` or `raft` |
| `cluster_secret` / `cluster_tls` | `IRONCACHE_CLUSTER_SECRET` / `_TLS` | peer auth + bus/repl encryption |
| `min_replicas_to_write` | `IRONCACHE_MIN_REPLICAS_TO_WRITE` | write-side durability guardrail |

TOML keys use underscores and the file parse is STRICT: `maxmemory-policy` (the
hyphen spelling) is the runtime `CONFIG GET` / `CONFIG SET` name, and a config file
that uses it fails boot with an unknown-key error (the accepted keys are
`KNOWN_TOML_KEYS` in `crates/ironcache-config/src/lib.rs`; the
`--ignore-unknown-config-keys` downgrade hatch relaxes unknown keys only, loudly).

A documented single-node config template you can copy and edit is at
[`deploy/ironcache.example.toml`](deploy/ironcache.example.toml) (run it with
`ironcache server --config <file>`; validate it with `ironcache check --config <file>`).

In raft mode the cluster-bus port is `port + 10000` and the replication port is
`port + 20000`, both derived automatically.

---

## Benchmarks

IronCache is built to be measured, not asserted. The headline is a **fair,
pinned head-to-head against Dragonfly v1.39.0** (the other thread-per-core
engine), with a reproducible artifact behind every number
([docs/bench/DRAGONFLY_REBENCH.md](docs/bench/DRAGONFLY_REBENCH.md)). Supporting
results follow: a **pipelined pinned-core depth sweep**, the **index-memory
measurement** behind the Dash default, and an earlier **small-node (2-vCPU)
worst-case** run. Baselines track the LATEST release of each engine, never a
distro-packaged older one (the version-pinned matrix is
[docs/bench/COMPETITORS.md](docs/bench/COMPETITORS.md)); Redis is compared at
**8.x**, not 7.x. Where a competitor wins a row, this section says so.

### Versus Dragonfly, fair and pinned (dated 2026-07-15)

The head-to-head against the other thread-per-core engine, run so the comparison
is airtight: **Dragonfly PINNED to v1.39.0** (image digest recorded), both
engines on the **same 8 pinned server cores** with a disjoint 8-core
`memtier_benchmark` generator, value sizes **128B and 256B**, pipeline depth
**swept 1 / 16 / 32 / 64**, persistence off, reproduced twice. Full method + raw
logs: [docs/bench/DRAGONFLY_REBENCH.md](docs/bench/DRAGONFLY_REBENCH.md). Every
number below has a reproducible artifact; IronCache figures are the **shipped
tokio binary** unless a row says io_uring. GET / SET in millions of ops/sec, mean
of two runs, 128B values.

**Cluster-aware (both engines owner-routed).** This is how a real cluster client
routes (go-redis, lettuce, `redis-cli -c`): each key goes straight to its owning
endpoint. IronCache runs `cluster_mode = shard-owners` (#517, one listener per
shard); Dragonfly runs `--cluster_mode=emulated`. Both driven by
`memtier --cluster-mode`.

| pipeline | IronCache (shard-owners) | Dragonfly v1.39.0 | IronCache GET |
| ---: | ---: | ---: | :--- |
| 1  | 0.70 / 0.69 | **1.02 / 0.98** | -31% |
| 16 | **3.35 / 2.91** | 2.82 / 2.61 | **+19%** |
| 32 | **4.08 / 3.46** | 2.83 / 3.34 | **+44%** |
| 64 | **4.31** / 3.95 | 3.45 / **3.98** | **+25%** |

At every pipeline depth of 16 or more, IronCache **leads GET by +19 to +60%**
(the +60% is the 256B leg), on the SHIPPED binary. At 256B IronCache leads GET at
every depth. SET also leads at 128B pipeline 16 to 32 (+4 to +12%) and across the
256B legs, with one honest exception: **128B pipeline 64, where SET is a
within-noise tie** (IronCache 3.95 vs Dragonfly 3.98). The single row Dragonfly
wins outright is **pipeline 1** (no pipelining), by about 30%: with no batch to amortize, its
leaner per-command path shows, and IronCache's own per-key cluster routing cost
is not yet paid back at depth 1 (single-endpoint IronCache is actually faster
than cluster-routed at depth 1). Real high-throughput deployments pipeline,
which is where the lead lives.

**Single-endpoint (both engines native, one port).** The client sends random
keys to one port; IronCache then pays a cross-shard hop that a cluster-aware
client avoids. This is IronCache's WORST routing, shown for honesty. The shipped
tokio binary cliffs at deep pipeline (the hop machinery); the io_uring build
(from-source, not in the published binaries) stays competitive and wins 256B GET
outright.

| pipeline | IC-tokio (shipped) | IC-io_uring | Dragonfly v1.39.0 |
| ---: | ---: | ---: | ---: |
| 16 | 2.23 / 2.16 | **2.81** / 2.60 | 2.50 / 2.54 |
| 32 | 1.85 / 1.32 | **2.65** / 2.83 | 2.62 / **3.23** |
| 64 | 1.26 / 1.01 | 2.98 / 2.95 | **3.34 / 3.83** |

**Memory** (`used_memory` delta over exactly-N distinct 128B keys, fine keycount
sweep in ONE environment: colima aarch64 Linux, IronCache on jemalloc vs Dragonfly
on mimalloc, populated via `redis-cli --pipe` / `DEBUG POPULATE`. Each engine's
allocator is inherent to it and materially shapes bytes/key):

| keys | IronCache (Dash index) | Dragonfly v1.39.0 |
| ---: | ---: | ---: |
| 550k | 175.4 | **169.2** |
| 700k | 160.6 | **157.0** |
| 800k | 161.3 | **154.6** |
| 900k | **162.1** | 182.8 |
| 1M   | **163.8** | 177.0 |

IronCache's Dash index holds a **FLAT 160 to 164 B/key** across the range;
Dragonfly's dashtable OSCILLATES hard (154 to 183). So IronCache wins the whole
850k to 1M range (by 13 to 21 B/key), and IronCache's WORST case (164) beats
Dragonfly's WORST case (183). Dragonfly wins a **narrow 550k to 800k window** by
about 4%, where its CompactObj inlines these short keys and lands the value in an
exact allocator bin. That window is an object-encoding difference, not a table
one (the index slot cost is about 12 to 14 B/key either way, measured
resize-free), so it is bounded and honest: predictable-flat versus lower-but-swingy.

**How to read this (honestly).** On the realistic pipelined, cluster-aware
config IronCache leads Dragonfly on GET at every pipelined depth and on SET at all
but one cell (the 128B pipeline-64 SET is a within-noise tie), on the shipped
binary, with a reproducible artifact behind every number. Baseline p99.9 latency
**ties** Dragonfly (measured identical at a matched load). Two rows honestly go the other
way: **pipeline 1** (no pipelining) throughput, where Dragonfly's leaner
per-command path wins about 30%, and -- historically -- the **during-snapshot
tail**. That tail is now CLOSED: it was a cross-shard-hop head-of-line block in the
drain loop, not a memory-bandwidth floor, and PR #742 took the during-save p99.9
from 794ms to **30ms** on c7g (Dragonfly 19ms, Valkey 510ms, Redis 719ms at the same
config), i.e. from worst-in-class into Dragonfly's class. See
`docs/bench/TAIL_LATENCY.md`. Memory is a wash decided by keycount: IronCache flat-and-predictable,
Dragonfly lower in a narrow short-key window and much higher in others. Versus
**Redis 8**, IronCache's thread-per-core write path parallelizes SET across all
shards where Redis serializes every write on its single main thread (regardless of
io-threads); a committed per-core SET A/B against Redis 8 is future work, so no
throughput multiple is claimed here. IronCache's decisive, reproducible edges are
pipelined cluster-aware throughput, memory, and determinism.

### Pipelined pinned-core depth sweep (dated 2026-07-13)

**Setup.** The first non-virtualized, PIPELINED throughput sweep: a 16-core
Graviton3 (c7g.4xlarge), the server pinned to 8 cores (8 shards, thread-per-core)
and the load generator to the disjoint 8 over loopback, 128 connections, 90% GET,
128-byte values, a 1M keyspace (zipf 0.99), sweeping the pipeline depth. Reproduced
twice. "Single-endpoint" is one 8-shard process behind one port (the client's random
keys hop to the owning shard); "zero-hop" is a cluster-aware load generator routing
each key to its owning endpoint, the way real Redis Cluster client libraries route.

| Pipeline depth | Single-endpoint (cross-shard hops) | Zero-hop (cluster-aware) |
| ---: | ---: | ---: |
| 1 | 256k | 291k |
| 8 | 1.84M | 1.61M |
| 16 | 1.44M | 2.98M |
| 32 | 1.07M | **5.59M** |

**How to read this (honestly).** Zero-hop scales MONOTONICALLY to **5.59M qps on 8
cores** (~700k qps/core) at depth 32, 5.2x the single-endpoint figure at the same
depth. Single-endpoint peaks at depth 8 and then CLIFFS; profiling shows that cliff
is the intrinsic cross-shard hop machinery (coordinator drain loops, per-hop channel
wakes), NOT an engine ceiling, and a cluster-aware client sidesteps all of it. That
makes the deep-pipeline cliff a CLIENT-ROUTING artifact; server-side hop batching was
analyzed and deliberately NOT pursued (it would trade the eager-hop overlap for
uncertain savings). The same sweep also overturned an earlier io_uring reading: the
io_uring datapath is materially faster once the load generator pipelines
(**+187% at depth 32** in the committed re-bench,
[docs/bench/IOURING_DATAPATH_BENCH.md](docs/bench/IOURING_DATAPATH_BENCH.md); this
earlier depth sweep read ~+189% before a reproducible log existed, and the ~2-point
delta is run noise). The prior "~6% slower than tokio" was a non-pipelined-loadgen
artifact. Lesson kept: a throughput verdict is meaningless without a pipelined load
generator. The full record is in [docs/bench/OPTIMIZATION_LOG.md](docs/bench/OPTIMIZATION_LOG.md).

### Index memory: flat bytes/key (dated 2026-07-15)

The measurement behind the Dash-index default (see Architecture): an organic
live-server sweep of `used_memory` per key across keycounts, both index arms.
hashbrown (SwissTable) OSCILLATES over a ~7.7 B/key band, peaking right after its
power-of-two doubling boundaries (112.5 to 115.8 B/key at the trough keycounts);
the Dash index stays FLAT (108.3 to 110.2 B/key): parity at hashbrown's best
points, **3.5 to 4.8% of TOTAL bytes better at the trough keycounts, never worse**.
Throughput is PARITY across five independent paired rounds (-2.2 / +0.6 / -3.0 /
+0.2 / -0.2%), full-table iteration (the eviction/snapshot/flush walks) is 2.2x
faster, and the accepted cost is a latent ~5% CPU-cycles premium (wider fingerprint
scan, absorbed by higher IPC) that no realistic bottleneck profile surfaces as qps.
The hashbrown arm stays CI-gated behind `hashbrown-index`, so the arms remain
comparable and the flip reversible.

### Small-node (2-vCPU) worst case

An earlier run (2026-06-21) benchmarked all four engines on 2-vCPU `t4g.medium`
nodes -- the worst case for a thread-per-core design (no core headroom), where
single-threaded Redis stays most competitive. It predated the Redis 8 baseline (it
used Redis 7.4.1) and, unlike every number above, its raw `memtier` logs were never
committed to `docs/bench/`, so those standings are NOT reproducible and are not
published here. The pinned, artifact-backed head-to-head above (real core count,
Redis 8, both engines owner-routed) is the authoritative comparison; on only two
cores the multi-threaded engines have no headroom to stretch, which is exactly why
that worst case is not the standing IronCache should be measured on.

---

## Repository layout

- [README.md](README.md): this overview.
- [DEPLOY.md](DEPLOY.md): the production deployment guide (container, Helm, k8s,
  compose) and every config key.
- [docs/RUNBOOK.md](docs/RUNBOOK.md): the on-call operator runbook (symptom to action
  for every operator-visible error string, log line, and probe state).
- [SECURITY.md](SECURITY.md) and [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md): the
  security policy and threat model.
- [CHANGELOG.md](CHANGELOG.md): notable changes.
- [docs/design/](docs/design/): the per-subsystem design records (protocol, runtime,
  persistence, ACL, TLS, clustering, observability, ...).
- [docs/adr/](docs/adr/): the architecture decision records.
- [docs/PRIOR_ART.md](docs/PRIOR_ART.md) and
  [docs/prior-art/claims.yaml](docs/prior-art/claims.yaml): the version-pinned
  comparative survey and the single source of truth for every numeric prior-art claim.
- [docs/bench/](docs/bench/): the competitor matrix and the optimization log.
- The [GitHub issues](https://github.com/ELares/IronCache/issues): the design record,
  indexed from the [vision EPIC (#1)](https://github.com/ELares/IronCache/issues/1).

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [GOVERNANCE.md](GOVERNANCE.md). Prose in
this project uses no em dashes or en dashes.

## License

Dual-licensed under your choice of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE). Copyright is held collectively by
"The IronCache Authors".
