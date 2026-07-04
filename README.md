<p align="center">
  <img src="docs/assets/ironcache-logo.png" alt="IronCache" width="320">
</p>

# IronCache

**A Redis-compatible cache in one static Rust binary: thread-per-core, replicated, clustered.**

IronCache speaks the Redis wire protocol (RESP2 and RESP3) and keeps the observable
Redis contract for the commands it implements, so existing Redis clients, libraries,
and `redis-cli` work against it unchanged. It is a shared-nothing, thread-per-core
engine: the keyspace is sharded so each shard is owned and mutated by exactly one
core, with no hot-path locks. It ships as a single static binary that is both the
server and the CLI.

The engine is functional and broad: 176 client-facing commands across all the core
data types, transactions, pub/sub with keyspace notifications, blocking commands,
on-disk persistence, and an opt-in Raft-governed multi-node cluster with replication,
automatic failover, and online slot migration. It is exercised by 1,500+ in-tree
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
  datapath** on Linux (default-off, opt-in) behind the same seam.
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
- **Cluster peer auth**: a shared `cluster_secret` presented in a constant-time
  handshake on the bus and replication links.
- **Secret hygiene**: secret arguments are redacted from SLOWLOG, MONITOR, INFO, and
  logs; the long-lived `cluster_secret` and transient plaintext are held in
  `Zeroizing` and scrubbed from the heap. The scope (what is and is not protected, and
  why) is documented in `SECURITY.md` and `docs/THREAT_MODEL.md`.

### Operability

- **HTTP health and metrics** (when `--metrics-addr` is set): `/livez` (liveness),
  `/readyz` (ready only when every shard has loaded and, in raft mode, a leader is
  known), and `/metrics` (Prometheus exposition: per-shard counters plus process and
  raft gauges).
- **Introspection**: INFO, CLIENT, COMMAND (a real command table for cluster-aware
  clients), CLUSTER, OBJECT, SLOWLOG, MEMORY, LATENCY.
- **DoS guards**: `maxmemory` with eviction, `maxclients`, an idle-connection
  timeout, and a per-connection output-buffer bound.

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
- Validated end to end on a live AWS node: an upgrade under continuous concurrent
  writes preserved every acknowledged write, the full keyspace, and the ACL users.

### Deployment

- A multi-stage, non-root, distroless container image (`Dockerfile`) published to
  GHCR.
- A **Helm chart** (`deploy/helm/ironcache`) and equivalent raw **Kubernetes**
  manifests (`deploy/k8s/`), deploying a StatefulSet with headless + client Services,
  a PDB, a PVC for `data_dir`, and `/livez` + `/readyz` probes.
- **docker-compose** for a single node and a 3-node Raft cluster (`deploy/compose/`).
- **CalVer rolling releases** on every push to `main` plus formal `v*` releases:
  reproducible `musl` + `glibc` tarballs for **amd64 and arm64**, a consolidated
  `SHA256SUMS`, a CycloneDX SBOM, and a keyless Sigstore build-provenance attestation.

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

---

## Quick start

### Build and run from source

You need a stable Rust toolchain (MSRV 1.85, edition 2024).

```sh
cargo build --workspace
cargo test --workspace          # 1,500+ tests

# boot the server on every core (sharded, thread-per-core) and talk to it with any
# Redis client
cargo run -p ironcache -- server
redis-cli -p 6379 SET hello world   # -> OK
redis-cli -p 6379 GET hello         # -> "world"

# other modes: the built-in CLI, the effective config, a config self-check, or a
# verified data-safe binary self-upgrade (see "Seamless upgrades")
cargo run -p ironcache -- cli GET hello
cargo run -p ironcache -- config
cargo run -p ironcache -- check
cargo run -p ironcache -- upgrade --binary ./ironcache --sha256sums ./SHA256SUMS
```

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
| `maxmemory` / `maxmemory-policy` | `IRONCACHE_MAXMEMORY` / `..._POLICY` | memory ceiling + eviction policy |
| `maxclients` | `IRONCACHE_MAXCLIENTS` | max connections (default 10000) |
| `requirepass` | `IRONCACHE_REQUIREPASS` | client AUTH password (hashed at rest) |
| `aclfile` | `IRONCACHE_ACLFILE` | ACL users loaded at boot |
| `data_dir` | `IRONCACHE_DATA_DIR` | durable snapshot + Raft-log dir (enables persistence) |
| `save_interval_secs` / `save_min_changes` | `IRONCACHE_SAVE_*` | periodic save cadence |
| `tls` + `tls_cert_path` + `tls_key_path` | `IRONCACHE_TLS*` | TLS on the client port |
| `cluster_enabled` / `cluster_mode` | `IRONCACHE_CLUSTER_*` | turn on clustering; `static` or `raft` |
| `cluster_secret` / `cluster_tls` | `IRONCACHE_CLUSTER_SECRET` / `_TLS` | peer auth + bus/repl encryption |
| `min_replicas_to_write` | `IRONCACHE_MIN_REPLICAS_TO_WRITE` | write-side durability guardrail |

In raft mode the cluster-bus port is `port + 10000` and the replication port is
`port + 20000`, both derived automatically.

---

## Benchmarks

IronCache is built to be measured, not asserted. Two dated runs are recorded below: a
**higher-core scaling run against the latest Redis 8.x** (the headline, where a
thread-per-core design is meant to earn its keep), and an earlier **small-node
(2-vCPU) worst-case** run. Baselines track the LATEST release of each engine, never a
distro-packaged older one (the version-pinned matrix is [docs/bench/COMPETITORS.md](docs/bench/COMPETITORS.md));
Redis is compared at **8.x**, not 7.x.

### Higher-core scaling, latest Redis 8 (dated 2026-07-03)

**Setup.** A single **AWS Graviton c7g.4xlarge** server (16 vCPU, arm64, kernel 6.17)
with a separate **c7g.8xlarge** load generator (32 vCPU, so the generator is never the
cap). The tool is `redis-benchmark` against a 1,000,000-key space, pipeline 64,
`-c 512 --threads 16`, persistence off. Each engine uses all 16 server cores: Redis 8
`--io-threads 8` (its peak; 16 did not improve), IronCache `--shards 15` (one core left
for the acceptor), Dragonfly `--proactor_threads 16`. Versions: **Redis 8.8.0**,
Dragonfly latest, IronCache (this build, io_uring datapath).

| Peak ops/sec | Redis 8 (1 thread) | Redis 8 (io-threads 8) | IronCache (shards 15) | Dragonfly (16) |
| --- | ---: | ---: | ---: | ---: |
| GET | 2,482,622 | 3,315,650 | **3,974,563** | 4,921,260 |
| SET | (n/a) | 1,328,374 | **3,311,258** | 4,945,598 |

IronCache's GET scales cleanly with shards (about 1.53M / 1.81M / 2.84M / 3.97M at 1 / 4
/ 8 / 15 shards), overtaking single-threaded Redis 8 at ~8 cores.

**How to read this (honestly).** On 16 cores IronCache **wins SET decisively** (3.31M vs
Redis 8's 1.33M, about 2.5x -- Redis's io-threads accelerate reads but the single main
thread still serializes the write mutation) and **edges GET** past Redis 8's best config
(3.97M vs 3.32M, about 1.2x). Redis 8 is a much stronger GET baseline than 7.x was: its
io-threads lift GET from 2.48M to 3.32M, closing most of the gap a 7.x comparison would
have shown -- which is exactly why we no longer benchmark against 7.x. **Dragonfly leads
both** (about 4.9M), measured directly on the same box rather than taken from its
marketing; its widely cited "25x" is a single-instance-versus-single-threaded-Redis
framing that does not hold against multi-threaded Redis 8. Closing the remaining gap to
Dragonfly is tracked optimization work (cross-shard-hop batching and per-op allocation
removal in the datapath), not an architectural ceiling -- at these throughputs the server
CPU is not saturated.

### Small-node (2-vCPU) worst case (dated 2026-06-21)

The earlier run below intentionally used 2-vCPU nodes -- the WORST case for a
thread-per-core design (no core headroom), where single-threaded Redis stays most
competitive. It predates the move to the Redis 8 baseline (it used Redis 7.4.1), and the
higher-core numbers above supersede its overall standings -- in particular Dragonfly,
which trails on 2 cores here, pulls AHEAD at 16 cores above.

**Setup.** Server nodes are **t4g.medium** (2 vCPU / 4 GB, arm64, AL2023); the load
generator is a separate **t4g.2xlarge**. The tool is `memtier_benchmark` against
32-byte values over a 1,000,000-key space, pipeline 16 for throughput and pipeline 1
for latency, peak across a connection sweep, persistence off. Each engine is given
both cores (Redis `io-threads 2`, KeyDB `server-threads 2`, Dragonfly
`proactor_threads 2`, IronCache `shards 2`). Versions: Redis 7.4.1, KeyDB 6.3.4,
Dragonfly v1.39.0, IronCache (this build).

### Single node, peak ops/sec

| Workload | Redis 7.4 | KeyDB 6.3 | Dragonfly 1.39 | IronCache |
| --- | ---: | ---: | ---: | ---: |
| SET | 570,912 | 361,198 | 517,079 | **596,495** |
| GET | 610,241 | 347,058 | 529,331 | **642,425** |
| MIX 1:10 | **574,344** | 346,011 | 453,481 | 562,124 |
| INCR | **924,908** | 541,577 | 548,804 | 663,373 |
| GET p99 ms (pipeline 1) | 0.447 | 0.455 | 0.431 | **0.407** |

### 3-node cluster, peak ops/sec

| Workload | Redis 7.4 | KeyDB 6.3 | Dragonfly 1.39 | IronCache |
| --- | ---: | ---: | ---: | ---: |
| SET | **1,223,353** | 665,630 | 1,026,420 | 1,067,433 |
| GET | 1,298,207 | 1,011,979 | 1,104,863 | **1,298,452** |
| MIX 1:10 | **1,222,915** | 969,486 | 936,071 | 1,057,888 |

### How to read this (honestly)

These are small (2-vCPU) nodes, chosen deliberately. On only two cores the
multi-threaded engines have very limited headroom, so single-threaded Redis stays
extremely competitive and in fact **wins the tiny-payload commands** (INCR
single-node, SET and MIX on the cluster) where its hand-tuned single-thread core has
the least overhead to amortize.

Where IronCache leads: it **tops SET and GET throughput and GET tail latency
single-node**, and it **ties Redis on cluster GET (about 1.30M ops/sec)**. KeyDB and
Dragonfly trail here, but note that is a 2-vCPU artifact: with only two cores the
multi-threaded engines cannot stretch, and Dragonfly in particular **pulls ahead once
given real core count** (see the 16-core run above). The picture is honest in both
directions: Redis wins the small-op rows, IronCache wins the bulk SET/GET and latency
rows on these nodes.

That "higher-core nodes would widen the multi-threaded engines' lead over
single-threaded Redis" is no longer a projection -- the 16-core run above measures it;
this run intentionally used small nodes to show the worst case for a thread-per-core
design, not its best. Reproduce a row with `memtier_benchmark` (32-byte
values, 1M keyspace, `--pipeline 16` for throughput / `--pipeline 1` for latency, both
cores per engine), sweeping connections for the peak.

---

## Repository layout

- [README.md](README.md): this overview.
- [DEPLOY.md](DEPLOY.md): the production deployment guide (container, Helm, k8s,
  compose) and every config key.
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
