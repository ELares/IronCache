<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# IronCache docker-compose

Two compose files:

| File | What it runs |
| --- | --- |
| `docker-compose.yml` | A single standalone node (no cluster). Quick local use. |
| `docker-compose.cluster.yml` | A 3-node Raft cluster on one host, peering by compose service DNS. |

Both use the published image `ghcr.io/elares/ironcache:latest`. Pin an immutable
tag (e.g. `:v0.1.0`) for anything but local play.

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

## Ports reference

| Port | Purpose | Derivation |
| --- | --- | --- |
| 6379 | client RESP | `port` |
| 16379 | Raft cluster-bus (RAFTMSG) | `port + 10000` |
| 26379 | replication data plane | `port + 20000` |
| 9121 | `/metrics` + `/livez` + `/readyz` | `--metrics-addr` |

See `../../DEPLOY.md` for the full config-knob reference and the Kubernetes paths.
