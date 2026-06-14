# Design: CLIENT TRACKING (BCAST + RESP3 push default, bounded table, RESP2 REDIRECT)

Issue: #21. Decisions: ADR-0002 (shared-nothing; a write on any shard must reach every tracking
client), ADR-0019 (RESP3-first reply shaping, RESP2 fallback). Related: #20/SERVER_PUSH.md (the
push delivery path this consumes), #15/PROTOCOL.md (HELLO proto negotiation, connection ids),
#107/COORDINATOR.md (cross-shard write notification), #86/OBSERVABILITY.md (table-size and
spurious-invalidation metrics), #137/ADMISSION.md (the global byte cap composes with memory
pressure).

## Goal and scope

Client-side caching moves hot reads off the server, so getting `CLIENT TRACKING` right is central
to the most-efficient-cache pitch. This spec owns the tracking state machine, the bounded global
tracking table with its cap and eviction, and the RESP2 REDIRECT path. It leads with BCAST plus
RESP3 same-connection invalidation push as the recommended mode and supports the per-client table
for compatibility behind a hard cap. The wire push encoding is out of scope and lives in
SERVER_PUSH.md (#20), which this spec consumes.

## Design

### The tracking state machine

- `CLIENT TRACKING ON [REDIRECT id] [BCAST] [OPTIN|OPTOUT] [NOLOOP] [PREFIX p ...]` drives two
  server profiles with very different memory cost [client-tracking-options]. A connection is in
  one of three states: tracking off (default), default tracking (per-client table), or BCAST
  tracking (prefix subscriptions). HELLO 3 negotiates RESP3 per connection [resp3-opt-in-via-hello];
  modern clients default to RESP3, so same-connection push is first-class day one (ADR-0019).
- BCAST plus RESP3 same-connection push is the recommended mode. It costs O(prefixes) server
  memory, needs no second connection, and is race-free, directly serving the minimal-memory goal.
  Non-overlapping PREFIX sets are honored; an empty prefix tracks everything.

### Bounded global tracking table

- Default (non-BCAST) tracking keeps a server-side map from key to the set of client-ids that read
  it, costing O(tracked_keys x clients). It is a single global structure keyed on client-id, not
  per-shard sub-tables: a write on any shard must invalidate every client that read the key, so
  under shared-nothing (ADR-0002) one global map gives a single enforceable cap and avoids fan-out
  across shard-local tables. Cross-shard writes notify the table through the coordinator
  (COORDINATOR.md).
- The table is bounded by a single global hard byte cap (config `tracking-table-max-bytes`) and an
  optional `tracking-table-max-keys` companion. The current size is exported as a metric
  (OBSERVABILITY.md). One global cap matches Redis's bounded-table model and the minimal-memory
  goal better than a per-client quota.

### Eviction under pressure

- When a cap is exceeded, IronCache evicts the oldest tracked key entries and sends each affected
  client a real (spurious) invalidation so the client refetches. It never silently drops an entry:
  a spurious invalidation is always safe, while a silent drop risks a stale client cache. The
  eviction reuses the invalidation push path in SERVER_PUSH.md and bumps a spurious-invalidation
  counter.

### RESP2 REDIRECT mode

- RESP2 clients cannot receive same-connection push, so invalidations go to the
  `__redis__:invalidate` Pub/Sub channel on a second connection named by `REDIRECT client-id`
  [resp2-invalidation-channel]. A `FLUSHALL` produces the contractual null-array invalidation on
  that channel. The channel name and null-on-flush semantics are reused exactly; clients are
  steered to the single-connection RESP3 path, with RESP2 REDIRECT kept for compatibility.

### Invalidation namespace and NOLOOP

- If SELECT / multi-DB is supported, invalidation uses a single cross-DB namespace identical to
  Redis; if multi-DB is not supported, tracking is documented as DB-agnostic and the question is
  moot. NOLOOP suppresses the invalidation to the connection that performed the write. The
  keyspace-notification write hook is shared but stays off by default
  [keyspace-notifications-off-by-default], so tracking does not implicitly enable notifications.

### Differential oracle

- Invalidation arrival timing and message shape must be observably indistinguishable from Valkey,
  which is wire-identical to Redis 7.2 RESP2/RESP3 [valkey-resp-identical], so Valkey is the
  differential oracle across write, expire, evict, and FLUSHALL.

## Open questions

- Default `tracking-table-max-bytes`, and whether it scales with `maxmemory`.
- Whether NOLOOP suppression applies per-connection only or also across a REDIRECT pair.
- Whether OPTIN/OPTOUT ship at M2 or only always-track plus BCAST initially.
- Whether SELECT / multi-DB exists at all (decides the single-namespace versus DB-agnostic wording).

## Acceptance and test hooks

- BCAST plus RESP3 same-connection invalidation works with non-overlapping PREFIX sets; redis-cli
  and a RESP3 client get pushes with no second connection [client-tracking-options].
- The default per-client table enforces the hard global byte cap and emits spurious invalidations
  on overflow; the table-size metric is exported.
- RESP2 clients receive invalidations on `__redis__:invalidate` via REDIRECT, and a FLUSHALL
  produces the null invalidation [resp2-invalidation-channel].
- If multi-DB is supported, invalidation uses a single cross-DB namespace identical to Redis;
  otherwise it is documented as DB-agnostic.
- A differential test asserts invalidation timing and message shape match pinned Valkey across
  write, expire, evict, and FLUSHALL [valkey-resp-identical].

## References

- ADR-0002, ADR-0019; issues #21, #20, #15, #107, #86, #137; specs SERVER_PUSH.md, PROTOCOL.md,
  COORDINATOR.md, OBSERVABILITY.md, ADMISSION.md.
- Claims: [client-tracking-options], [resp3-opt-in-via-hello], [resp2-invalidation-channel],
  [valkey-resp-identical], [keyspace-notifications-off-by-default].
