# Design: TTL expiration (per-shard timing wheel, lazy backstop, background reclamation)

Issue: #51. Decisions: ADR-0002/0005 (shared-nothing per-shard), ADR-0003
(determinism), ADR-0014 (no fork). Related: #34 (storage hooks), #111 (TTL bits),
#48 (volatile-* eviction), #60 (replica DEL propagation).

## Goal and scope

The TTL subsystem must bound the memory held by expired-but-not-reclaimed keys,
expire deterministically (so a seeded replay and a replica agree), support native
per-element TTLs, and never block the hot path while freeing large values. This
specifies a per-shard hierarchical timing wheel with a lazy-on-access backstop and
a background reclamation queue.

## Why not Redis's model

Redis expires lazily on access plus a periodic active-expire cycle driven by `hz`
[redis-hz-default] that samples random keys probabilistically rather than tracking
expiries exactly [redis-active-expire-keys-per-loop] [redis-active-expire-fast-duration],
so under churn it leaves expired keys resident and can burn CPU when many keys
share a deadline [redis-active-expire-slow-cpu]. It also runs on the master only,
so a replica's logical memory drifts until a `DEL` propagates. IronCache tracks
expiries exactly per shard instead.

## Design

### Per-shard hierarchical timing wheel

- Each shard owns a hierarchical timing wheel keyed by deadline; a key with a TTL
  is registered in the wheel slot for its expiry, with the slot index stored in
  the kvobj's TTL handle (OBJECT_LAYOUT #111). The owning core advances the wheel
  on its own clock (through the Env seam, ADR-0003, so a seeded replay fires the
  same expiries). Advancing a slot yields exactly the keys due, with no random
  sampling and no CPU scan of unrelated keys: O(due keys), not O(keyspace).
- The wheel is shard-local (no cross-core sharing, ADR-0005); there is no global
  expiry structure and no lock.

### Lazy backstop

- On `Read`/`RMW` (the storage hooks, #34), if a key's deadline has passed it is
  treated as absent and queued for reclamation, even if the wheel has not yet
  advanced to it. This is the correctness backstop that guarantees an expired key
  is never observed, independent of wheel granularity.

### Background reclamation (no hot-path stall, no fork)

- Freeing a large value can be expensive, so expiry enqueues the value on a
  per-shard background reclamation queue rather than freeing inline; the owning
  core drains it in bounded batches between commands. This keeps a single expiry
  of a multi-megabyte value off the command's latency, and uses background work,
  never `fork()` (invariant 4, ADR-0014). The queue is bounded; if it backs up,
  reclamation pressure feeds admission (#137).

### Determinism and replicas

- Because the wheel advances through the Env clock (ADR-0003), expiry decisions
  are byte-identical on a seeded replay and reproducible in DST (#95/#160). On a
  replica, expiry follows the propagated effect rather than firing independently
  (the master is authoritative for the expiry `DEL`, #60), so a replica never
  expires a key the master still holds, avoiding the master/replica logical-memory
  drift Redis has.

### Native per-element TTLs

- The wheel registers a (key, optional element) deadline, so per-element TTLs
  (hash-field expiry and similar) reuse the same mechanism rather than a bespoke
  side structure; it reuses the wheel for the per-element expiry KeyDB ships via
  EXPIREMEMBER/EXPIREMEMBERAT [keydb-subkey-expire], providing the same capability
  natively. Whether per-element TTL ships in v1 is gated by the command-surface
  tiering (#128); the wheel supports it.

## Open questions

- Wheel granularity and level count (slot resolution vs memory for the wheel),
  tuned on the harness (#8); finer resolution narrows the lazy-backstop window.
- Reclamation batch size (latency vs how fast freed memory returns), and how it
  composes with the allocator's background purge (ADR-0006).

## Acceptance and test hooks

- An expired key is never observed by a client (lazy backstop), and resident
  memory for expired keys stays bounded under a heavy-expiry workload (a memory
  test), unlike Redis's sampled cycle [redis-active-expire-slow-cpu].
- A seeded replay fires identical expiries (DST #160); a replica never expires a
  key the master still holds (#60).
- Expiring a large value does not spike the latency of the triggering command
  (background reclamation), with no `fork()` anywhere (invariant 4 lint).

## References

- ADR-0002, ADR-0003, ADR-0005, ADR-0006, ADR-0014; issues #34, #111, #48, #60,
  #137, #128, #8, #95, #160.
- Claims: [redis-hz-default], [redis-active-expire-keys-per-loop],
  [redis-active-expire-fast-duration], [redis-active-expire-slow-cpu],
  [keydb-subkey-expire].
