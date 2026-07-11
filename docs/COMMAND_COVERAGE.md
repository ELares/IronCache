<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# Command coverage

IronCache implements the Redis command set broadly, with Redis-identical observable semantics
for the commands it supports. A command is either supported (and behaves like Redis) or is
documented as unsupported; the wire protocol and a command's observable behavior are never bent.

**Source of truth.** This page is a scannable, by-category summary. The authoritative,
always-current list is the command registry in the code,
[`crates/ironcache-server/src/command_spec.rs`](../crates/ironcache-server/src/command_spec.rs)
(the `spec_of` match), which is also what a live server projects through introspection:

```sh
redis-cli -p 6379 COMMAND COUNT   # the number of client-facing commands (176)
redis-cli -p 6379 COMMAND LIST    # the full name list
redis-cli -p 6379 COMMAND INFO GET
```

If this table and the code ever disagree, the code and the live `COMMAND` output win.

**RESP2 / RESP3.** Every command works over both RESP2 and RESP3; a connection starts in RESP2
and switches to RESP3 on `HELLO 3`. The command set and the returned values are identical across
the two; only the reply framing differs (RESP3 adds map, set, and push reply types). See
[docs/CLIENT_LIBRARIES.md](CLIENT_LIBRARIES.md).

## By category

| Category | Commands |
| --- | --- |
| Connection / protocol | `PING` `ECHO` `HELLO` `AUTH` `SELECT` `RESET` `QUIT` `LOLWUT` |
| Server / introspection | `INFO` `CONFIG` `CLIENT` `COMMAND` `DBSIZE` `MEMORY` `OBJECT` `SLOWLOG` `LATENCY` `DEBUG` `HOTKEYS`* `FLUSHDB` `FLUSHALL` `SWAPDB` `SHUTDOWN` |
| Persistence | `SAVE` `BGSAVE` `LASTSAVE` |
| Strings / numerics | `GET` `SET` `GETSET` `GETDEL` `GETEX` `SETEX` `PSETEX` `SETNX` `APPEND` `STRLEN` `SUBSTR` `GETRANGE` `SETRANGE` `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT` `MGET` `MSET` `MSETNX` `MSETEX`* `DELIFEQ`* |
| Generic keyspace | `DEL` `UNLINK` `EXISTS` `TYPE` `KEYS` `SCAN` `RANDOMKEY` `RENAME` `RENAMENX` `COPY` `MOVE` `TOUCH` `SORT` `SORT_RO` `DUMP` `RESTORE` `OBJECT` |
| Expiry / TTL | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `EXPIRETIME` `PEXPIRETIME` `TTL` `PTTL` `PERSIST` |
| Hashes | `HSET` `HSETNX` `HMSET` `HGET` `HMGET` `HGETALL` `HKEYS` `HVALS` `HDEL` `HLEN` `HEXISTS` `HSTRLEN` `HINCRBY` `HINCRBYFLOAT` `HRANDFIELD` `HSCAN` `HGETEX` `HGETDEL` `HSETEX` |
| Hash-field TTL | `HEXPIRE` `HPEXPIRE` `HEXPIREAT` `HPEXPIREAT` `HEXPIRETIME` `HPEXPIRETIME` `HTTL` `HPTTL` `HPERSIST` |
| Lists | `LPUSH` `LPUSHX` `RPUSH` `RPUSHX` `LPOP` `RPOP` `LRANGE` `LINDEX` `LSET` `LINSERT` `LREM` `LTRIM` `LLEN` `LPOS` `LMOVE` `RPOPLPUSH` `LMPOP` |
| Sets | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SMISMEMBER` `SCARD` `SPOP` `SRANDMEMBER` `SMOVE` `SINTER` `SUNION` `SDIFF` `SINTERSTORE` `SUNIONSTORE` `SDIFFSTORE` `SINTERCARD` `SSCAN` |
| Sorted sets | `ZADD` `ZREM` `ZSCORE` `ZMSCORE` `ZRANK` `ZREVRANK` `ZINCRBY` `ZCARD` `ZCOUNT` `ZLEXCOUNT` `ZRANGE` `ZRANGEBYSCORE` `ZRANGEBYLEX` `ZREVRANGE` `ZREVRANGEBYSCORE` `ZREVRANGEBYLEX` `ZRANGESTORE` `ZPOPMIN` `ZPOPMAX` `ZMPOP` `ZRANDMEMBER` `ZUNION` `ZINTER` `ZDIFF` `ZUNIONSTORE` `ZINTERSTORE` `ZDIFFSTORE` `ZINTERCARD` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE` `ZREMRANGEBYLEX` `ZSCAN` |
| Bitmaps | `SETBIT` `GETBIT` `BITCOUNT` `BITPOS` `BITOP` `BITFIELD` `BITFIELD_RO` |
| HyperLogLog | `PFADD` `PFCOUNT` `PFMERGE` |
| Transactions | `MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH` |
| Pub/Sub | `SUBSCRIBE` `UNSUBSCRIBE` `PSUBSCRIBE` `PUNSUBSCRIBE` `PUBLISH` `PUBSUB` `SSUBSCRIBE` `SUNSUBSCRIBE` `SPUBLISH` |
| Blocking | `BLPOP` `BRPOP` `BLMOVE` `BRPOPLPUSH` `BLMPOP` `BZPOPMIN` `BZPOPMAX` `BZMPOP` `WAIT` |
| Cluster | `CLUSTER` `ASKING` `READONLY` `READWRITE` |

`*` = an IronCache operational extension beyond the Redis command set (`HOTKEYS` reports the
hottest keys; `MSETEX` is a multi-set with a shared expiry; `DELIFEQ` is a compare-and-delete).
The hash-field TTL family (`HEXPIRE` and friends) and `HGETEX` / `HGETDEL` / `HSETEX` are the
standard Redis 7.4+ commands.

## Notable behavior notes

- **Transactions are per-shard.** On a single node, every key queued in a `MULTI`/`EXEC` block must
  live on the connection's home shard; co-locate keys with a shared `{hash tag}` to guarantee it.
  This mirrors the cluster contract that a transaction's keys must share a slot. See
  [DRIVER_MATRIX.md](../tests/drivers/DRIVER_MATRIX.md) finding F3.
- **Cluster routing.** In cluster mode, keyed commands hash to one of 16384 CRC16 slots and route
  with `-MOVED` / `-ASK` exactly like Redis Cluster; a multi-key command that spans slots is
  rejected with `CROSSSLOT`.
- **Keyspace notifications.** `notify-keyspace-events` drives keyspace / keyevent Pub/Sub messages
  (including `expired` / `evicted`); disabled by default so the write hot path pays nothing.
- **DUMP emits STRING + SET + HASH + ZSET; RESTORE also accepts LIST.** `DUMP` (encode) emits the
  **STRING type** (a HyperLogLog counts, since an HLL is stored as a string) and the **SET, HASH, and
  ZSET types in their plain RDB forms** (`RDB_TYPE_SET`, `RDB_TYPE_HASH`, and `RDB_TYPE_ZSET_2`
  8-byte binary-double scores). The plain forms are always valid and always Redis-loadable at any
  cardinality (the compact `intset` / `listpack` / `skiplist` encodings are a size optimization, not a
  correctness requirement), so a set, hash, or zset `DUMP`ed here `RESTORE`s on a real Redis with
  identical members/fields/scores (+inf/-inf preserved). `DUMP` of a **LIST** still returns an error
  (the plain list form needs a listpack writer, a tracked follow-up). `RESTORE` (decode) accepts the
  **STRING type, the SET type in all three RDB encodings** (`intset`, `listpack`, and the plain
  length-prefixed set), **the HASH type in its two non-field-TTL encodings** (`listpack` and the plain
  length-prefixed hash), **the ZSET type in all three encodings** (`RDB_TYPE_ZSET_2` binary-double
  scores, the legacy `RDB_TYPE_ZSET` ASCII scores, and `listpack`), **and the LIST type in the modern
  `RDB_TYPE_LIST_QUICKLIST_2` encoding** (the quicklist of listpack + plain nodes that Redis 7.x
  DUMPs, insertion order preserved across nodes) **plus the trivial legacy `RDB_TYPE_LIST`**, so a
  set, a (non-field-TTL) hash, a sorted set, OR a list `DUMP`ed by a real Redis `RESTORE`s here with
  identical members/fields/scores/order (a NaN score is refused, matching `ZADD`; +inf/-inf are
  preserved). A HASH with per-field TTLs (Redis 7.4+ `listpack_ex` / `metadata` encodings) and the
  legacy ziplist-based list encodings (`RDB_TYPE_LIST_QUICKLIST` / `RDB_TYPE_LIST_ZIPLIST`, which
  modern Redis never DUMPs) are still refused as bad data, so do NOT assume full-fidelity `MIGRATE`
  across all types yet. The remaining follow-ups (LIST `DUMP`, hash field-TTL `RESTORE`, and the
  ziplist-based list `RESTORE` forms) are tracked in #612.

For the full type-by-type feature list see the [README](../README.md); for the byte-for-byte
parity story see [docs/design/DIFFERENTIAL_TESTING.md](design/DIFFERENTIAL_TESTING.md).
