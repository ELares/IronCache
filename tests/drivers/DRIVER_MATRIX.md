<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# IronCache client-driver compatibility matrix (PROD-8 / #158)

This harness runs **real Redis client libraries** against a live IronCache, in three modes:

* **single-node** -- one `ironcache server`, exercising the core data types, pipelining,
  MULTI/EXEC, pub/sub, and RESP3.
* **cluster** -- a **turnkey 3-node Raft cluster** (#347) that auto-forms to `cluster_state:ok`
  from a static topology with no manual `CLUSTER MEET` / `ADDSLOTS`, exercising cluster-aware
  client **topology discovery** (`CLUSTER SLOTS`) + **MOVED-routing** end to end.
* **shard-owners** (#517) -- ONE node in `cluster_mode = shard-owners` that exposes its N internal
  shards as N CRC16-hashslot owners on distinct ports (`base + i`). A cluster-aware client reads
  `CLUSTER SLOTS` and dials each key's **owner shard's port**, so the key lands on its home shard
  and the internal **cross-shard hop is eliminated** -- proven by scraping `/metrics` and asserting
  `ironcache_hops_sent_total` stays 0 while `ironcache_local_served_total` climbs, with a
  single-endpoint **contrast** node (same keys, one port) showing `hops_sent > 0`.

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

**54 PASS, 0 FAIL** for the single-node + Raft-cluster + restricted legs. Cluster-aware discovery +
MOVED-routing works end to end for all three clients. The **shard-owners** leg (#517) adds 6 routed
op-groups per cluster-aware client plus the two harness metric groups (`zero-hop`,
`single-endpoint-contrast`); see its own section below.

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

### Cluster restricted-user (per-subcommand ACL, #405)

Each cluster node also loads a shared aclfile with a **locked-down `svc` user**:

```
user svc on >svcpw ~* resetchannels +@read +@write +@connection +@transaction \
  -@dangerous +cluster|slots +cluster|shards +cluster|nodes +cluster|info
```

The `restricted` leg connects each client as `svc` and proves the per-subcommand ACL grant is
exactly what a real cluster client needs: introspection reads are allowed, every mutator is denied.
(`default` stays all-permissive so the orchestrator's unauthenticated `PING` / `CLUSTER INFO`
health probes and the existing legs are byte-identical; the restricted user is layered on top.)

| op-group | redis-py | go-redis | ioredis |
|----------|:--------:|:--------:|:-------:|
| discovery (CLUSTER SLOTS/INFO as `svc`) | PASS | PASS | PASS |
| rw-roundtrip (30 keys SET+GET, routed) | PASS | PASS | PASS |
| addslots-denied (`CLUSTER ADDSLOTS` -> NOPERM) | PASS | PASS | PASS |

The `addslots-denied` group PASSES *when the mutator is denied* (`-NOPERM User svc has no
permissions to run the 'cluster|addslots' command`) -- the ACL fires before the handler, so this
holds even where a single-node mutator would otherwise be inert. See finding **F4**.

### Shard-owners (single-node hop elimination, #517)

ONE node booted `cluster_mode = shard-owners` with `shards = 4` exposes its 4 internal shards as 4
CRC16-hashslot owners on ports `7511..7514` (metrics on a dedicated `:9092`). The SAME cluster-client
bodies that drive the Raft cluster leg run again pointed at those 4 owner ports -- discovery, routed
ops, readback, crossslot, hashtag-coloc, pipeline:

| op-group | redis-py | go-redis | ioredis |
|----------|:--------:|:--------:|:-------:|
| discovery (CLUSTER SLOTS -> 4 shard owners) | PASS | PASS | (RESP2-only; runs where node present) |
| routed-ops (60 keys, routed to owner ports) | PASS | PASS | " |
| routed-readback (values correct) | PASS | PASS | " |
| crossslot (multi-slot op rejected) | PASS | PASS | " |
| hashtag-coloc ({tag} co-located mget) | PASS | PASS | " |
| pipeline | PASS | PASS | " |

**The zero-hop assertion (`RESULT harness shard-owners zero-hop`).** After the client legs, `run.sh`
also drives its own **owner-dialed** keyed traffic -- a minimal MOVED-following client in bash, so the
assertion holds even when NO external client library is installed -- then scrapes `/metrics`:

* **shard-owners node:** `ironcache_hops_sent_total = 0`, `ironcache_local_served_total = 32`.
  Every owner-dialed key landed on its home shard and was served locally, with **zero internal hops**.
* **contrast node** (a NORMAL 4-shard node, same 32 keys through ONE port):
  `ironcache_hops_sent_total = 23`, `local_served = 9`. A single-endpoint client's foreign-shard keys
  HOP internally -- the baseline the shard-owners projection eliminates.

So the hop elimination is **measured**, not merely asserted: `hops_sent` 23 -> 0 for the same keys,
purely by routing them to the shard that owns their slot. The Rust
`crates/ironcache/tests/metrics_endpoint.rs::shard_owners_owner_dialed_client_shows_zero_hops`
asserts the identical property over a raw socket, and
`coordinator_hop_counters_increment_on_cross_shard_traffic` asserts the single-endpoint contrast.

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

### F4 (infra grant note, #405) -- ioredis cluster bootstrap also needs `CLUSTER INFO`

* **Client / op:** ioredis 5.11.x `Redis.Cluster`, bootstrap under the scoped `svc` user.
* **Observed:** ioredis's default `enableReadyCheck` issues **`CLUSTER INFO`** (it waits for
  `cluster_state:ok`) *in addition* to `CLUSTER SLOTS`. go-redis (`CLUSTER SLOTS`) and redis-py
  (`CLUSTER SLOTS` + bare `COMMAND`, see F1) do **not** need `CLUSTER INFO`.
* **Implication (the grant set):** the minimal per-subcommand read grant for the three mainstream
  cluster clients is **`+cluster|slots +cluster|shards +cluster|nodes +cluster|info`**, not just
  `slots`/`shards`/`nodes`. `CLUSTER INFO` is a **read** (no slot-map / node-table mutation), so
  granting it carries no escalation -- the security boundary (every `@dangerous` CLUSTER mutator
  denied) is unchanged. The shipped `svc` aclfile line includes `+cluster|info` for this reason.
* **Also confirmed safe under `svc`:** `COMMAND` / `COMMAND DOCS` (redis-py's connect-time routing
  table, F1) is `@admin`+`@connection` but **not** `@dangerous`, so `+@connection` grants it under
  `-@dangerous`; and `CLIENT SETINFO` (go-redis / redis-py lib-name handshake) is `@dangerous` and
  therefore NOPERM, but both clients treat a `SETINFO` failure as non-fatal -- so the connection
  still succeeds. No further grant was required.

### F5 (design note, #517) -- the client's CRC16 slots align with the internal shard owner

* **Why zero hops actually happens:** a cluster-aware client computes each key's slot with **CRC16
  (XMODEM) over the `{hashtag}`** and routes by the `CLUSTER SLOTS` map. IronCache's internal
  shard owner is `owner_shard(key) = slot_to_shard(key_slot(key), N)` -- the **same** CRC16
  `key_slot`, partitioned contiguously across the N shards (`route.rs`). The shard-owners projection
  advertises exactly that partition (shard `i` at `base + i` owns `[i*16384/N, (i+1)*16384/N)`), so a
  client that dials the slot's owner PORT lands on the shard that owns the slot INTERNALLY: `owner ==
  home`, no hop. The legacy FNV-1a hash (`fnv1a_shard`) is the pre-#517 internal owner; it is
  **superseded** by the slot-based owner in every cluster mode so the client-visible slot and the
  internal shard coincide. A misroute (dialing the wrong port) returns `MOVED` (no hop, no local
  serve), so a correct client converges to zero hops after at most one redirect.
* **Verified:** both go-redis and redis-py discovered all 4 shard owners and routed 60 keys with 0
  CROSSSLOT/MOVED-loop failures; the `hops_sent = 0` scrape confirms the alignment held for real
  client slot math, not just the harness's owner-dial.

## What is NOT covered (follow-ups)

* JVM (Lettuce / Jedis) and .NET (StackExchange.Redis) clients -- need their toolchains; not run.
* AUTH was not exercised by default (the harness boots without `requirepass`); the redis-py script
  has the hooks to test it if `IRONCACHE_REQUIREPASS` is set on the server.
* A full `COMMAND DOCS` body (summaries / since / group) -- emitted empty; not needed for routing,
  and clients tolerate an empty map. A richer DOCS is a possible follow-up.
