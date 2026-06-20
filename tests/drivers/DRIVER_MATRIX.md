<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# IronCache client-driver compatibility matrix (PROD-8 / #158)

This harness runs **real Redis client libraries** against a live IronCache, in two modes:

* **single-node** -- one `ironcache server`, exercising the core data types, pipelining,
  MULTI/EXEC, pub/sub, and RESP3.
* **cluster** -- a **turnkey 3-node Raft cluster** (#347) that auto-forms to `cluster_state:ok`
  from a static topology with no manual `CLUSTER MEET` / `ADDSLOTS`, exercising cluster-aware
  client **topology discovery** (`CLUSTER SLOTS`) + **MOVED-routing** end to end.

It is the layer the existing **differential** harness does NOT cover. The differential proves
byte-for-byte RESP parity vs `redis-server`; this proves real client *libraries* (including
cluster-aware clients that follow `CLUSTER SLOTS` / `MOVED`) actually drive IronCache.

## How to reproduce

```sh
# Build + boot single-node and the 3-node cluster, run every available client, print the matrix.
tests/drivers/run.sh

# Use a prebuilt binary instead of building:
IRONCACHE_BIN=/path/to/ironcache tests/drivers/run.sh

# Restrict to a subset of clients:
DRIVERS=python,go tests/drivers/run.sh
```

Each per-language script emits one machine-readable line per `(client, mode, op-group)`:

```
RESULT <client> <mode> <op-group> <PASS|FAIL> [detail]
```

and `run.sh` collects them into the final matrix. A client whose toolchain is absent is **skipped**
(reported, never counted as a failure). Cleanup is unconditional: every spawned `ironcache` process
is killed by recorded PID (never `pkill -f`, which would self-match) and the temp dir wiped.

## Clients run vs skipped

| Client | Library | Status on the dev box | Notes |
|--------|---------|-----------------------|-------|
| **redis-py** | `redis` (PyPI) | **RAN** | Python 3.11, `redis` 6.4.0 in a venv |
| **go-redis** | `github.com/redis/go-redis/v9` | **RAN** | Go 1.25, go-redis v9.7.0 |
| **ioredis** | `ioredis` (npm) | **RAN** | Node 22, ioredis 5.11.1 |
| Lettuce / Jedis | JVM | **SKIPPED** | needs a JVM toolchain; not run (note only) |
| StackExchange.Redis | .NET | **SKIPPED** | needs .NET; not run (note only) |

## Results (latest local run: macOS, all three clients available)

**54 PASS, 0 FAIL.** Cluster-aware discovery + MOVED-routing works end to end for all three
clients.

### Single-node

| op-group | redis-py | go-redis | ioredis |
|----------|:--------:|:--------:|:-------:|
| connect (PING) | PASS | PASS | PASS |
| strings (SET/GET/APPEND/GETRANGE/INCR) | PASS | PASS | PASS |
| lists (LPUSH/RPUSH/LRANGE) | PASS | PASS | PASS |
| hashes (HSET/HGETALL) | PASS | PASS | PASS |
| sets (SADD/SMEMBERS) | PASS | PASS | PASS |
| zsets (ZADD/ZRANGE WITHSCORES) | PASS | PASS | PASS |
| expire-ttl (EXPIRE/TTL) | PASS | PASS | PASS |
| mget-mset | PASS | PASS | PASS |
| pipeline | PASS | PASS | PASS |
| multi-exec (MULTI/EXEC atomic) | PASS | PASS | PASS |
| pubsub (SUBSCRIBE receives PUBLISH) | PASS | PASS | PASS |
| resp3 (HELLO 3 map + push) | PASS | PASS | PASS* |

`*` ioredis is **RESP2-only** (its bundled parser cannot decode the RESP3 map byte `%`); the
group asserts HELLO 2 and records the RESP3 gap as a **client** limitation, not an IronCache
defect. See Findings.

### Cluster (turnkey 3-node Raft)

| op-group | redis-py | go-redis | ioredis |
|----------|:--------:|:--------:|:-------:|
| discovery (CLUSTER SLOTS -> 3 nodes) | PASS | PASS | PASS |
| routed-ops (60 keys, MOVED-routed) | PASS | PASS | PASS |
| routed-readback (values correct) | PASS | PASS | PASS |
| crossslot (multi-slot op rejected) | PASS | PASS | PASS |
| hashtag-coloc ({tag} co-located mget) | PASS | PASS | PASS |
| pipeline (cluster pipeline) | PASS | PASS | PASS |

The cluster-aware result is the high-value one: every client **discovers** the topology via
`CLUSTER SLOTS`, **routes** keyed ops to the owning node by following `MOVED`, reads the values
back correctly, and rejects/splits a cross-slot multi-key op as expected.

## Findings

### F1 (server defect, FIXED) -- empty `COMMAND` broke cluster-aware redis-py

* **Client / op:** redis-py `RedisCluster`, every routed keyed op (`SET`, `GET`, `MGET`, ...).
* **Observed:** `RedisError: SET command doesn't exist in Redis commands` -- the cluster client
  could discover the topology but **could not route a single keyed op**.
* **Root cause:** IronCache's `COMMAND` introspection was an empty PR-1 stub
  (`COMMAND COUNT` -> 0, `COMMAND` -> `[]`, `COMMAND INFO <x>` -> empty). redis-py's
  `RedisCluster` calls bare `COMMAND` at connect to build its command -> key-position table so it
  can compute each command's slot. With an empty table it has no key-spec for `SET`/`GET`/`MGET`
  and refuses to route. (Single-node redis-py is unaffected: it routes everything to one node and
  never consults the table.)
* **Fix (server, test-only-adjacent, default path unchanged):** project the **real** command
  table from the existing single-source `command_spec` registry (#89):
  * bare `COMMAND` / `COMMAND INFO [name...]` -> the Redis flat entry
    `[name, arity, [flags], first_key, last_key, step, [], [], [], []]`;
  * `COMMAND COUNT` -> the real count (176);
  * `COMMAND LIST` -> the name list;
  * `COMMAND GETKEYS <cmd> [args...]` -> the routable keys, via the same `extract_keys` the router
    uses (the movable-key fallback a cluster client uses for `numkeys`/option-scan commands).

  The mapping from the registry's `KeySpecKind` to the Redis `(first, last, step)` positions is in
  `command_spec::command_key_positions`, cross-checked against canonical Redis positions by
  `command_key_positions_match_redis`. After the fix all three cluster clients route cleanly.

### F2 (client limitation, NOT IronCache) -- ioredis is RESP2-only

* **Client / op:** ioredis 5.11.1, `HELLO 3` (RESP3 negotiation).
* **Observed:** `Protocol error, got "%" as reply type byte. Please report this.`
* **Root cause:** ioredis's bundled `redis-parser` only decodes the RESP2 type bytes
  (`$ + * : -`); it has **no case** for the RESP3 map (`%`), set (`~`), or push (`>`) bytes. When
  IronCache correctly answers `HELLO 3` with a RESP3 map, the client's parser throws. IronCache is
  not at fault: redis-py (`protocol=3`) and go-redis (`Protocol: 3`) both negotiate RESP3 against
  the **same** server and consume the map + push messages cleanly.
* **Disposition:** reported, no server change. The ioredis `resp3` group asserts HELLO 2 (the
  protocol ioredis supports) and records the RESP3 gap as a documented client limitation.

### F3 (model note, NOT a bug) -- single-node MULTI/EXEC is per-shard

* IronCache is internally sharded thread-per-core; **MULTI/EXEC requires every queued key to be on
  the connection's home shard** (cross-shard transactions are by design unsupported, returning
  `a queued command references a key on another shard`). On a default multi-shard single node a
  transaction whose keys land on a non-home shard aborts -- which a client cannot control.
* The harness therefore boots the single node with `--shards 1` so MULTI/EXEC is deterministic for
  the driver tests; every other op-group is shard-count-agnostic. This mirrors the cluster contract
  (a transaction's keys must share a slot, enforced there with a `{hash tag}`).

## What is NOT covered (follow-ups)

* JVM (Lettuce / Jedis) and .NET (StackExchange.Redis) clients -- need their toolchains; not run.
* AUTH was not exercised by default (the harness boots without `requirepass`); the redis-py script
  has the hooks to test it if `IRONCACHE_REQUIREPASS` is set on the server.
* A full `COMMAND DOCS` body (summaries / since / group) -- emitted empty; not needed for routing,
  and clients tolerate an empty map. A richer DOCS is a possible follow-up.
