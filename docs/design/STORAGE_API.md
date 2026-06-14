# Design: Narrow-waist storage API (Read / Upsert / Delete / RMW)

Issue: #34. Decisions: ADR-0002 (shared-nothing), ADR-0005 (per-shard map).
Related: #15 (RESP/command layer above), #35 (hash table below), #64 (log engine
below), #48 (eviction hook), #51 (expiration hook).

## Goal and scope

IronCache needs one storage contract every command flows through, so the RESP
surface and the storage engine evolve on independent schedules. Without a fixed
boundary, command handlers reach into store internals and every new data
structure reopens the command layer. This defines a four-primitive narrow waist
with callback hooks, sitting below the wire/command layer and above the per-shard
store. Out of scope: the concrete hash table (#35) and log engine (#64).

## Design

### The four primitives

Garnet's Tsavorite proves a tiny operation set backs a full RESP surface
[garnet-narrow-waist-api]. The command layer depends only on:

- **Read(key) -> view:** borrow the value (or absence) for read-only commands.
- **Upsert(key, value):** blind set, replacing any existing value.
- **Delete(key) -> existed:** remove a key.
- **RMW(key, mutator):** atomic read-modify-write, the single primitive behind
  INCR, APPEND, SETBIT, LPUSH, HSET, expiry-on-write, and every other in-place
  mutation. The mutator runs on the owning core with exclusive access, so it is
  atomic by construction with no lock.

Every keyspace read or mutation in the Tier 0/1 surface is expressed as a
sequence of these four against one or more keys; the engine exposes only these
four for keyspace access.

### What the waist does NOT cover

Three command classes are not per-key keyspace access and are handled outside the
four primitives, by design:

- **Iteration (`SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`).** A stable cursor over a shard's
  table is an iteration primitive, not a per-key op. It is a separate read-only
  engine entry point (a cursor/iterator over the shard map), not one of the four;
  this resolves #34's open question (a fifth read-only scan primitive, not
  `Read` composed) and is specified with the SCAN cursor-stability contract
  (#129).
- **Blocking commands (`BLPOP`/`BRPOP`/`BLMOVE`/`WAIT`/`XREAD BLOCK`).** Parking a
  connection until another client writes is cross-connection coordination with a
  timeout; a synchronous `RMW` callback cannot block. Blocking is a wait-queue
  concern above the synchronous store (owned by the blocking-command design), which
  uses the four primitives when it wakes but is not itself one of them.
- **Pub/Sub and keyspace notifications.** Channel and subscriber registries are
  not keyspace state; they are delivered as push frames (#20, PROTOCOL.md), not
  through the storage waist.

Multi-key and cross-shard atomic commands (`MSET`, `SINTERSTORE`, `MULTI`/`EXEC`
across shards) ARE expressed in the four primitives, but decomposed by the
coordinator (#29) into per-shard primitive calls.

### Composition with shared-nothing

- All four are synchronous calls on the shard's owning core (ADR-0002/0005):
  there is no `async` and no `await` inside the store, because the core has
  exclusive access and never blocks on another core. Cross-shard commands are
  decomposed by the coordinator (#29) into per-shard primitive calls that hop to
  each owning core; the storage API itself is always single-shard and
  single-threaded.
- Because access is exclusive, `Read` can hand out a borrow into the stored bytes
  (zero-copy to the serializer) for the duration of the command, and `RMW` can
  mutate in place.

### Callback hooks

The API carries hooks so cross-cutting concerns attach without the command layer
knowing the engine internals:

- **eviction hook** (#48): on insert/access the EvictionPolicy is notified and may
  select victims when the shard is at its memory budget (ADR-0007/0008).
- **expiration hook** (#51): TTL is read/written through the same entry; lazy
  expiry is checked on Read/RMW and active expiry runs in the background.
- **accounting hook** (#41/ADR-0006): every insert/delete updates the shard's
  allocator-attributed byte count for honest maxmemory (invariant 3).
- **snapshot hook** (#60): the forkless versioned snapshot observes writes via the
  store-internal write funnel (the OnWrite pre-image hook). This hook attaches at
  the store's internal `put_object`/`remove_object`/`rmw` funnel, which is NOT part
  of the frozen `Store` trait, so it lands WITH the Wave-3 snapshot feature without
  reopening the waist. The PR-2a store carries only the eviction and accounting
  hooks on the trait surface; expiration is folded into the per-entry deadline
  (not a separate hook), and the snapshot OnWrite hook is deferred to its feature.

### Layering contract

The command layer (#15/#128/#129) imports only the four primitives and the hook
types; it never names a concrete map or log type. The engine (#35 now, a hybrid
log #64 later) implements the four primitives. This is the seam that lets the
read-cache index and a future tiered/log backend swap without touching commands,
exactly as the narrow waist intends [garnet-narrow-waist-api]; it generalizes the
hot/read-only/disk regioning a hybrid log later adds
[faster-hybridlog-three-regions]. Forward-compatibility constraint (per #34): the
waist must admit a second store behind the same four primitives, as Garnet runs a
string store and an object store under one API [garnet-two-stores]; no primitive
may assume a single backend.

## Open questions

- Whether `RMW`'s mutator is a closure or a trait object (closure is simpler and
  monomorphizes; trait object eases dynamic command tables); decided with #35.
- The exact `Read` view lifetime/borrow type (raw slice vs a guard) pending the
  object layout (#111).

## Acceptance and test hooks

- Every keyspace read/mutation in the Tier 0/1 set is implementable as a
  composition of the four primitives (a layering test: the command crate depends
  only on the storage-API crate's four functions + hook types for keyspace
  access); iteration (SCAN), blocking, and pub/sub use their own entry points,
  not the four.
- An `RMW` mutator observes and writes atomically with no lock on the owning core.
- The eviction and accounting hooks fire through the four primitives, and
  expiration is enforced through the per-entry deadline on every read path (lazy
  backstop now, active timing wheel in PR-3); these are verified by the conformance
  and property suites (#95/#98). The snapshot OnWrite pre-image hook attaches at the
  store-internal write funnel (`put_object`/`remove_object`/`rmw`), NOT the frozen
  `Store` trait, and lands with the Wave-3 snapshot feature (#60) without reopening
  the waist, so spec and code agree: the PR-2a trait surface carries eviction +
  accounting only.

## References

- ADR-0002, ADR-0005, ADR-0006, ADR-0007, ADR-0008; issues #15, #35, #64, #48,
  #51, #41, #60, #29, #128, #129, #95.
- Claims: [garnet-narrow-waist-api], [garnet-two-stores],
  [faster-hybridlog-three-regions].
