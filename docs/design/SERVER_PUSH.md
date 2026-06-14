# Design: Unified server-push channel (Pub/Sub, sharded Pub/Sub, keyspace notifications, CSC)

Issue: #20 (absorbs #108, the Pub/Sub fan-out topology under shared-nothing). Decisions:
ADR-0002 (shared-nothing thread-per-core), ADR-0019 (RESP3 reply-shaping / RESP2 fallback).
Related: #15/PROTOCOL.md (HELLO per-connection proto state), #107/COORDINATOR.md (cross-shard
fan-out and back-pressure), #70/CLUSTER_CONTRACT.md (slot routing), #86/OBSERVABILITY.md
(queue-depth and drop metrics), #137/ADMISSION.md (pubsub-class output-buffer limits), #21
(CLIENT TRACKING, the CSC state machine that feeds this path).

## Goal and scope

Four Redis features deliver data outside the request/reply flow: classic Pub/Sub, sharded
Pub/Sub, keyspace notifications, and client-side-caching (CSC) invalidations. On the wire they
are one shape: a RESP3 push frame (`>`) on RESP3 connections and a multi-bulk array on RESP2
connections [resp-type-prefixes]. This spec defines one internal push abstraction serving all
four: the `ServerPush` value and its per-connection renderer, the routing tables that feed it,
FIFO ordering, the per-shard fan-out topology absorbed from #108, and back-pressure. Out of
scope: the CSC tracking table and its state machine (#21, which consumes this path) and the
client-visible cluster contract (#70, the slot-math reference here).

## Design

### The ServerPush value and the per-connection renderer

- One internal `ServerPush { kind, payload }` value carries every push: classic message,
  pattern message, sharded message, keyspace/keyevent notification, and CSC invalidation. A
  single per-connection renderer turns it into bytes, so framing is written once and the four
  features cannot drift.
- RESP2 vs RESP3 is chosen at the connection writer from the per-connection negotiated proto, not
  at the publisher. A connection starts RESP2 and upgrades only on `HELLO 3` [resp3-opt-in-via-hello],
  so publishers stay proto-agnostic and the writer owns the same HELLO state PROTOCOL.md tracks
  (ADR-0019). RESP3 renders the `>` push type [resp-type-prefixes]; RESP2 renders the equivalent
  multi-bulk array.

### Routing tables and fan-out topology (absorbs #108)

- Subscription state is per-shard, not a global registry. Under shared-nothing thread-per-core
  (ADR-0002) a global subscriber table would be a cross-core hot structure on every PUBLISH; a
  per-shard channel-broker keeps fan-out local to the core that owns the channel. This is the
  #108 decision: a per-shard broker over a single broadcast-to-all-shards registry.
- Classic Pub/Sub channels and patterns are not slotted, so a PUBLISH must still reach subscribers
  on any core. Delivery is a fan-out message handed to peer shards through the cross-shard
  coordinator (COORDINATOR.md), never a shared lock: each shard renders to its own connections.
  Pattern matching (PSUBSCRIBE) is evaluated per shard against that shard's pattern table.
- Sharded Pub/Sub confines a message to the shard owning the channel's slot [sharded-pubsub-7.0].
  SSUBSCRIBE and SPUBLISH route by the same CRC16/XMODEM-over-16384 slotting with hash-tag
  co-location as keys [redis-cluster-crc16-xmodem-rrc][redis-cluster-hash-tag-rule][redis-cluster-hash-slots-16384],
  so a sharded channel and a same-tagged key resolve to one shard and no cross-shard fan-out
  occurs. The slot function and CROSSSLOT contract are owned by CLUSTER_CONTRACT.md; this spec
  reuses them by reference.

### Keyspace notifications (off by default)

- Keyspace notifications are off by default and activate only via the `notify-keyspace-events`
  config flag string [keyspace-notifications-off-by-default], publishing to `__keyspace@db__`
  and `__keyevent@db__`. Off preserves the hot-path CPU budget: the write hook that emits a
  notification is compiled out of the fast path until a non-empty flag is set. When enabled, the
  notification enters the same per-shard broker as classic Pub/Sub.

### CSC invalidation delivery

- CSC invalidations ride this path as a `ServerPush` of kind invalidation. On RESP3 they are a
  push on the same connection (the lead path); on RESP2 they are a message on the
  `__redis__:invalidate` channel delivered to a REDIRECT connection [resp2-invalidation-channel].
  This spec owns only the delivery encoding and FIFO placement; what to invalidate and when is
  owned by CLIENT_TRACKING.md (#21).

### FIFO ordering

- A push caused by a command is enqueued after that command's reply, in per-connection FIFO order,
  so a client never sees an invalidation or notification before the reply of the command that
  triggered it. Ordering is per connection only; no cross-connection ordering is promised, which
  avoids a global lock under shared-nothing (ADR-0002).

### Back-pressure (absorbs #108)

- The pubsub connection class has its own output-buffer soft/hard limit (ADMISSION.md): a slow
  push consumer whose unsent buffer exceeds the hard limit is disconnected rather than allowed to
  grow shard memory unbounded. Push delivery never blocks the publishing command's shard. Every
  drop or disconnect increments a metric and push-queue depth is exported (OBSERVABILITY.md).

## Open questions

- Whether CSC RESP3 invalidations interleave strictly after the writing command's reply on other
  connections, or only best-effort across connections.
- Per-DB versus single cross-DB namespace for keyspace notifications and CSC, pending the SELECT
  decision.
- Sharded Pub/Sub fan-out to a shard's replicas versus master-only delivery.
- Whether a sustained slow pubsub consumer is shed gradually before the hard disconnect.

## Acceptance and test hooks

- One internal push value renders byte-identical RESP3 push (`>`) and RESP2 multi-bulk frames,
  selected by per-connection proto [resp-type-prefixes][resp3-opt-in-via-hello].
- SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE, `__keyspace`/`__keyevent`, and CSC invalidations all flow
  through the single abstraction.
- Keyspace notifications default off and activate only via the config flag string
  [keyspace-notifications-off-by-default].
- SSUBSCRIBE and a key write to the same hash tag resolve to one shard via CRC16, with no
  cross-shard fan-out [redis-cluster-crc16-xmodem-rrc][redis-cluster-hash-tag-rule].
- A push caused by a command arrives after that command's reply on the same connection
  (per-connection FIFO).
- A slow pubsub consumer over its class output-buffer hard limit is disconnected and shard memory
  stays bounded; the drop and queue-depth metrics move.

## References

- ADR-0002, ADR-0019; issues #20, #108, #15, #107, #70, #21, #86, #137; specs PROTOCOL.md,
  CLUSTER_CONTRACT.md, COORDINATOR.md, OBSERVABILITY.md, ADMISSION.md, CLIENT_TRACKING.md.
- Claims: [resp-type-prefixes], [resp3-opt-in-via-hello], [sharded-pubsub-7.0],
  [keyspace-notifications-off-by-default], [resp2-invalidation-channel],
  [redis-cluster-crc16-xmodem-rrc], [redis-cluster-hash-slots-16384], [redis-cluster-hash-tag-rule].
