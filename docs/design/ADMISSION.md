# Design: Connection admission, max-clients, output-buffer limits, OOM-write/DoS contract

Issue: #137 (merged from connection-admission, client-output-buffer-limits, and
DoS-and-abuse-limits). Decisions: ADR-0007 (cache mode), ADR-0002 (shared-nothing).
Related: #138 (per-frame parser limits), #51 (reclamation pressure), #86 (metrics),
#15 (connection lifecycle).

## Goal and scope

This bounds aggregate inbound and buffer pressure: a `maxclients` cap, the
OOM-write contract when eviction cannot keep pace, and per-class client
output-buffer limits. It complements per-frame hardening (#138): that bounds a
single request, this bounds connections and buffers in aggregate.

## Design

### Connection admission and maxclients

- A `maxclients` cap is enforced at accept: when the global live-connection count
  reaches the limit, a new connection is refused at the accept gate with the
  Redis-compatible error (`-ERR max number of clients reached`) and closed, rather
  than admitted and starved. `maxclients` is a single process-wide cap, matching
  Redis semantics, so the limit holds regardless of how accepts are balanced
  across cores (round-robin vs least-loaded is an open runtime question, #25) and
  it never refuses a client while capacity remains on another core.
- The count is one relaxed atomic accounting cell (or a sum of per-core cells read
  on the accept slow path), not a hot-path shared counter: accept is not the
  GET/SET fast path (ADR-0002), so a single relaxed increment/decrement at
  connect/disconnect is cheap and bounces no cache line per request. This keeps a
  true global cap without a static per-core split, which would cliff-edge under
  accept skew (refusing clients on a busy core while others sit idle).

### OOM-write contract

- In cache mode (ADR-0007) the default is to evict, so writes normally succeed. If
  the write rate outruns eviction (eviction cannot free space fast enough), a
  write that would exceed the ceiling is rejected with the Redis-recognized
  `-OOM ...` error (the exact byte string pinned to the oracle, #97), while reads
  and memory-releasing commands (`GET`, `DEL`, `UNLINK`, `EXPIRE`, `TTL`) are still
  served. In strict datastore mode
  (`noeviction`, ADR-0007) the `-OOM` rejection is the normal at-capacity behavior.
  The contract is explicit so a client sees a clean, Redis-recognized error rather
  than a stall or an OOM-kill.

### Per-class client output-buffer limits

- Each connection class (normal, replica, pubsub) has an output-buffer soft/hard
  limit: a slow consumer whose unsent reply buffer exceeds the hard limit (or the
  soft limit for a sustained window) is disconnected, so one slow client cannot
  grow unbounded memory and threaten the shard (the slow-consumer OOM vector).
  Defaults follow Redis's class-based limits; pubsub and replica get larger
  budgets than normal.
- Beyond the per-connection class limits, a `maxmemory-clients` aggregate cap
  (#137) bounds the total memory of all client buffers together (configurable as
  an absolute size or a percentage of `maxmemory`). When the aggregate is exceeded
  the largest-buffer consumers are disconnected first, so many individually
  under-limit slow clients cannot collectively exhaust memory. Buffers live
  per-core (shared-nothing, ADR-0002), so the aggregate is enforced from the same
  relaxed per-core accounting summed at the decision point, not a hot-path shared
  counter.

### Backpressure composition

- Reclamation pressure (#51, the background free queue backing up) and the memory
  ceiling feed admission: as the shard approaches the ceiling with reclamation
  behind, write admission tightens before a hard `-OOM`. This keeps tail latency
  bounded instead of cliff-edging. Every rejection (connection refused, `-OOM`
  write, output-buffer trim) increments a metric (#86) so the pressure is visible.

## Open questions

- Default numeric values: `maxclients`, the per-class output-buffer soft/hard
  limits and the soft window, tuned against real client behavior so no legitimate
  client trips a default.
- Whether write admission tightens gradually (a CoDel-style shed) before `-OOM`
  or only hard-rejects at the ceiling (latency vs simplicity), measured on the
  harness (#8).

## Acceptance and test hooks

- At `maxclients` a new connection gets `-ERR max number of clients reached` and
  is closed; existing connections are unaffected. The cap is global: connections
  arriving skewed onto one core are admitted up to the process-wide limit, not a
  per-core fraction (a skew test).
- Under a write flood that outruns eviction, writes get `-OOM` while `GET`/`DEL`/
  `EXPIRE` still succeed (cache mode); strict mode `-OOM`s at capacity.
- A slow consumer exceeding its class output-buffer hard limit is disconnected and
  memory stays bounded (a slow-consumer test); each rejection bumps its metric.
- With `maxmemory-clients` set, many individually under-limit slow clients are
  collectively bounded: the aggregate cap disconnects the largest buffers and
  total client-buffer memory stays under the cap (an aggregate-buffer test).

## References

- ADR-0002, ADR-0007; issues #138, #51, #86, #15, #8.
- Claims: [redis-maxmemory-policy-default-rc].
