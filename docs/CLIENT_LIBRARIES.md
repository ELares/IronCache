<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# Connecting a Redis client to IronCache

IronCache speaks the Redis wire protocol (RESP2 and RESP3) and keeps the observable Redis
contract for the commands it implements, so a standard Redis client library connects to it
with no IronCache-specific code: point the client at the host and port and use it exactly as
you would against Redis.

The three clients below are the ones the project validates on every change by running their
real driver code against a live IronCache (single-node and cluster) in the driver-compatibility
harness. The pinned versions are the ones that harness runs; the full pass/fail matrix, the
cluster (topology-discovery + `MOVED`-routing) results, and the known client limitations are in
[tests/drivers/DRIVER_MATRIX.md](../tests/drivers/DRIVER_MATRIX.md).

All snippets assume a server reachable at `127.0.0.1:6379` (see the top-level
[README](../README.md) quick start for a one-command Docker server).

## redis-py (Python)

Validated with **redis-py 6.4.0** on Python 3.11 (`pip install redis`).

```python
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

assert r.ping() is True
r.set("hello", "world")
assert r.get("hello") == "world"
```

redis-py negotiates **RESP3** when you ask for it; pass `protocol=3`:

```python
r3 = redis.Redis(host="127.0.0.1", port=6379, protocol=3, decode_responses=True)
r3.ping()   # HELLO 3 handshake, RESP3 replies (maps, push messages) decoded cleanly
```

## go-redis (Go)

Validated with **go-redis v9.7.0** (`github.com/redis/go-redis/v9`) on Go 1.25.

```go
import (
    "context"
    "github.com/redis/go-redis/v9"
)

ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

if err := rdb.Ping(ctx).Err(); err != nil {
    panic(err)
}
rdb.Set(ctx, "hello", "world", 0)
val, _ := rdb.Get(ctx, "hello").Result() // "world"
```

go-redis negotiates **RESP3** by default; set `Protocol: 2` to force RESP2, or `Protocol: 3`
to be explicit:

```go
r3 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", Protocol: 3})
```

## ioredis (Node.js)

Validated with **ioredis 5.11.1** on Node 22 (`npm install ioredis`).

```js
const Redis = require("ioredis");

const r = new Redis({ host: "127.0.0.1", port: 6379 });

await r.ping();               // "PONG"
await r.set("hello", "world");
await r.get("hello");         // "world"
```

Note: ioredis is **RESP2-only**. Its bundled parser has no case for the RESP3 map / set / push
type bytes, so it cannot use `HELLO 3`. This is a documented client limitation, not an IronCache
defect: redis-py (`protocol=3`) and go-redis (`Protocol: 3`) both negotiate RESP3 against the same
server and decode the RESP3 replies cleanly. See finding F2 in
[DRIVER_MATRIX.md](../tests/drivers/DRIVER_MATRIX.md).

## RESP2 vs RESP3

A new connection starts in **RESP2** and stays there unless the client sends `HELLO 3`, which
switches that connection to **RESP3** (map, set, and push reply types, and out-of-band push
messages). Use whichever your client supports; the commands and their values are identical, only
the reply framing differs. redis-py and go-redis negotiate RESP3 on request; ioredis stays on
RESP2.

## Other clients

Any RESP2/RESP3 client should work (Jedis, Lettuce, StackExchange.Redis, and `redis-cli`
included); the three above are simply the ones exercised in CI on every change. If you hit a
client that does not behave, that is worth an issue: the project treats a real-client divergence
as a bug to document or fix, not to paper over. The full validated matrix, including the JVM and
.NET clients that are noted-but-not-run, is in
[tests/drivers/DRIVER_MATRIX.md](../tests/drivers/DRIVER_MATRIX.md).
