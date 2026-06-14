# Design: Pluggable EvictionPolicy trait and Redis policy-name mapping

Issues: #48 (trait + ghost queue), #50 (Redis maxmemory-policy mapping).
Decisions: ADR-0007 (cache mode default), ADR-0008 (S3-FIFO default), ADR-0005
(per-shard map). Related: #46 (parent decision), #49 (W-TinyLFU admission), #51
(TTL), #34 (storage hooks), #88 (advisor).

## Goal and scope

IronCache ships more than one eviction algorithm (S3-FIFO default, SIEVE and a
W-TinyLFU-fronted variant selectable) and lets the advisor tune per tenant, so
the engine needs a pluggable `EvictionPolicy` behind one interface that operates
over the same index data, swaps at runtime, and stays off the lock path. It must
also accept the ten Redis `maxmemory-policy` names and echo a Redis-recognized
value from `CONFIG GET`, mapping that vocabulary onto the internal FIFO-class
engine. Scope: the trait and its hot-path contract, the ghost queue, the
monomorphization strategy, runtime selection, and the Redis alias layer.

## Design

### The EvictionPolicy trait

- One trait with a small surface: `on_access(entry)`, `on_insert(entry)`,
  `evict_victim() -> key` (called only at the memory budget), and ghost-queue
  hooks. It operates over the per-entry metadata folded into the kvobj
  (OBJECT_LAYOUT #111): the S3-FIFO 2-bit counter [s3fifo-freq-counter-2bit-cap3],
  or a SIEVE visited bit [sieve-algorithm], or a W-TinyLFU sketch reference
  [wtinylfu-window-main-split], so no policy needs a parallel per-key structure.
- Hot-path contract: `on_access` is a single in-place metadata write on the
  owning core (a flag/counter bump), never a list relink, so it adds no lock and
  no allocation, which is exactly why FIFO-class policies beat LRU here
  [hit-ratio-can-hurt-throughput] and why locks are unnecessary
  [glommio-locks-never-necessary] (ADR-0005). Eviction selection runs only when
  the shard hits its budget (ADR-0007), off the read path.

### Ghost queue

- Policies that need a ghost (S3-FIFO's recently-evicted key set
  [s3fifo-small-main-split]) get an optional fixed-size ghost queue of key
  fingerprints (not values), per shard, sized as a fraction of the main capacity.
  SIEVE needs none; the trait makes the ghost optional so a policy pays for it
  only if it uses it.

### Monomorphization and runtime selection

- The default build monomorphizes the chosen policy (a generic over the
  `EvictionPolicy` trait, or an enum dispatch) so there is no vtable indirection
  on the hot path for the default. Runtime selection (the advisor #88 or
  `CONFIG SET maxmemory-policy`) switches policy per shard via the alias layer
  below; switching rebinds the policy and reinterprets the per-entry metadata
  bits, it does not move data.

### Redis policy-name mapping (#50)

- IronCache accepts all ten names Redis 8.8 defines
  [redis-maxmemory-policies-list] and `CONFIG GET maxmemory-policy` echoes a
  Redis-recognized value, defaulting in cache mode to a name that reflects
  eviction-on (not Redis's `noeviction` default [redis-maxmemory-policy-default-rc];
  the default-posture divergence is ADR-0007, documented in the compat tiering).
- Mapping: `noeviction` selects strict datastore mode (errors on write at the
  budget). The `allkeys-*` family maps onto the internal FIFO-class engine
  (S3-FIFO by default) over all keys; the `volatile-*` family restricts the
  victim set to keys with a TTL (#51). `*-lru`/`*-lfu` are served by the
  FIFO-class engine rather than Redis's sampled approximation
  [redis-lru-lfu-sampling] [redis-maxmemory-samples-5] (a default-internal
  difference, not an observable one: the wire contract is the accepted name and
  the eviction effect, not the sampling algorithm). `*-random` maps to a random
  victim; `*-ttl` and the newer `*-lrm` (least-recently-modified)
  [redis-lrm-policy-new] map to their respective orderings over the eligible set.
  `maxmemory-samples` is accepted and ignored (or used only to gate a SIEVE/LRU
  fallback), documented as a no-op under the FIFO engine.

## Open questions

- enum-dispatch vs generic monomorphization for the multi-policy build (binary
  size vs per-shard policy heterogeneity for the advisor), measured in #8.
- Whether `volatile-*` over a TTL-only victim set needs a separate index of
  TTL-bearing keys or can scan the shard's wheel (#51).

## Acceptance and test hooks

- `on_access` for every policy is a single in-place metadata write, no lock, no
  alloc (a hot-path lint/test).
- All ten Redis policy names are accepted, `CONFIG GET maxmemory-policy` echoes a
  Redis-recognized value, and the eviction effect matches the named family
  (conformance #95/#97).
- The eviction benchmark (#47, M1) validates the default and the alternatives on
  the cachemon corpus.

## References

- ADR-0005, ADR-0007, ADR-0008; issues #46, #49, #51, #34, #88, #8, #47, #95,
  #97, #111.
- Claims: [s3fifo-freq-counter-2bit-cap3], [s3fifo-small-main-split],
  [sieve-algorithm], [wtinylfu-window-main-split], [hit-ratio-can-hurt-throughput],
  [glommio-locks-never-necessary], [redis-maxmemory-policies-list],
  [redis-maxmemory-policy-default-rc], [redis-lru-lfu-sampling],
  [redis-maxmemory-samples-5], [redis-lrm-policy-new].
