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

- A `maxclients` cap is enforced at accept: when the per-process (and per-core)
  limit is reached, a new connection is refused at the accept gate with the
  Redis-compatible error (`-ERR max number of clients reached`) and closed, rather
  than admitted and starved. The cap is split across cores (each core admits its
  share) consistent with shared-nothing (ADR-0002); a per-core gate avoids a
  shared atomic counter on accept.

### OOM-write contract

- In cache mode (ADR-0007) the default is to evict, so writes normally succeed. If
  the write rate outruns eviction (eviction cannot free space fast enough), a
  write that would exceed the ceiling is rejected with `-OOM command not allowed
  when used memory > maxmemory`, while reads and memory-releasing commands (`GET`,
  `DEL`, `UNLINK`, `EXPIRE`, `TTL`) are still served. In strict datastore mode
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
  is closed; existing connections are unaffected.
- Under a write flood that outruns eviction, writes get `-OOM` while `GET`/`DEL`/
  `EXPIRE` still succeed (cache mode); strict mode `-OOM`s at capacity.
- A slow consumer exceeding its class output-buffer hard limit is disconnected and
  memory stays bounded (a slow-consumer test); each rejection bumps its metric.

## References

- ADR-0002, ADR-0007; issues #138, #51, #86, #15, #8.
- Claims: [redis-maxmemory-policy-default-rc].
