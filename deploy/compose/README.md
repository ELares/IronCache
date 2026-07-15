<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# IronCache docker-compose

Two compose files:

| File | What it runs |
| --- | --- |
| `docker-compose.yml` | A single standalone node (no cluster). Quick local use. |
| `docker-compose.cluster.yml` | A 3-node Raft cluster on one host, peering by compose service DNS. |

Both use the published image `ghcr.io/elares/ironcache:latest`. Pin an immutable
tag (e.g. `:0.1.0` -- the registry tag has NO leading `v`; the image CI strips it
from the release tag) for anything but local play. Channel skew to know about:
images are published only on `v*` release tags, while binary tarballs roll on
every push to `main`, so `:latest` here can lag the rolling-binary channel.

## Single node

```sh
cd deploy/compose
docker compose up -d
redis-cli -p 6379 ping        # -> PONG
curl localhost:9121/readyz    # -> ready
docker compose down           # stop (keeps the data volume)
docker compose down -v        # stop + drop the data volume
```

Persistence is on (`IRONCACHE_DATA_DIR=/var/lib/ironcache` on the `ironcache-data`
volume) with a 900s / >=1-change background save. Set `IRONCACHE_REQUIREPASS` in a
`.env` file beside the compose file to enable client auth.

## 3-node Raft cluster

The cluster requires a shared secret. Create a `.env` file beside the compose file:

```sh
cd deploy/compose
printf 'IRONCACHE_CLUSTER_SECRET=%s\n' "$(openssl rand -hex 24)" > .env
# optional client auth:
# printf 'IRONCACHE_REQUIREPASS=%s\n' "$(openssl rand -hex 24)" >> .env

docker compose -f docker-compose.cluster.yml up -d
```

Each node mounts its own `config/nodeN.toml` (the full topology + its stable
`cluster_announce_id`) at `/etc/ironcache/ironcache.toml`, which the binary reads
by default. Peer hosts are the compose service names `ironcache-1/2/3`, resolved
lazily at Raft dial time, so nodes can start in any order.

Client ports are published as `6379` / `6380` / `6381` on the host. The 16384
slots are split: node1 `[0,5460]`, node2 `[5461,10922]`, node3 `[10923,16383]`.

Formation is TURNKEY: each `config/nodeN.toml` declares its `slots`, and on a
fresh cluster the elected Raft leader auto-applies that declared node table + slot
ownership through the replicated log, so the cluster reaches `cluster_state:ok`
with all 16384 slots assigned on its own. You do NOT run `CLUSTER MEET` /
`CLUSTER ADDSLOTS` by hand for the shipped topology -- those are reserved for
RUNTIME changes (adding a node, rebalancing). Wait for `/readyz` (or poll
`redis-cli -p 6379 CLUSTER INFO` for `cluster_state:ok`), then just use it:

```sh
redis-cli -c -p 6379 set foo bar   # -c follows MOVED redirects across nodes
redis-cli -c -p 6379 get foo
docker compose -f docker-compose.cluster.yml down      # keep data
docker compose -f docker-compose.cluster.yml down -v   # drop data
```

### Encrypting the bus (optional)

Without TLS the `cluster_secret` travels in cleartext on the (private) compose
network. To encrypt the bus + replication links, generate a self-signed cluster
cert, mount it, and set `IRONCACHE_CLUSTER_TLS=on` plus the cert/key/CA paths on
every service. See the commented block at the bottom of
`docker-compose.cluster.yml`.

## Console overlay (optional, HA)

`docker-compose.console.yml` adds the monitoring/management console as TWO stateless
replicas behind an nginx load balancer (`console-lb`), overlaid on either cache file.
The console stays OUT of the client data path -- it only polls the nodes -- so a
replica loss is transparent:

```sh
cd deploy/compose
docker compose -f docker-compose.cluster.yml -f docker-compose.console.yml up -d
curl localhost:9180/readyz    # served by whichever replica the LB picks
```

Point it at the cache node(s) (`IRONCACHE_CONSOLE_SEEDS`), a SHARED metrics backend
(`IRONCACHE_CONSOLE_PROMETHEUS_URL`), and set `IRONCACHE_CONSOLE_READ_TOKEN` so the
privileged API is not open. Full HA + security walkthrough:
[`../CONSOLE_DEPLOY.md`](../CONSOLE_DEPLOY.md).

## Ports reference

| Port | Purpose | Derivation |
| --- | --- | --- |
| 6379 | client RESP | `port` |
| 16379 | Raft cluster-bus (RAFTMSG) | `port + 10000` |
| 26379 | replication data plane | `port + 20000` |
| 9121 | `/metrics` + `/livez` + `/readyz` | `--metrics-addr` |
| 9180 | console UI + `/api/*` + `/livez` + `/readyz` (console overlay) | `IRONCACHE_CONSOLE_HTTP_ADDR` |

See `../../DEPLOY.md` for the full config-knob reference and the Kubernetes paths,
and `../CONSOLE_DEPLOY.md` for the console HA + security deployment guide.
