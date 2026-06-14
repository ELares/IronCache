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

Every RESP command is expressed as a sequence of these four against one or more
keys; the engine exposes only these four.

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
  same path (the OnWrite hook).

### Layering contract

The command layer (#15/#128/#129) imports only the four primitives and the hook
types; it never names a concrete map or log type. The engine (#35 now, a hybrid
log #64 later) implements the four primitives. This is the seam that lets the
read-cache index and a future tiered/log backend swap without touching commands,
exactly as the narrow waist intends [garnet-narrow-waist-api]; it generalizes the
hot/read-only/disk regioning a hybrid log later adds
[faster-hybridlog-three-regions].

## Open questions

- Whether `RMW`'s mutator is a closure or a trait object (closure is simpler and
  monomorphizes; trait object eases dynamic command tables); decided with #35.
- The exact `Read` view lifetime/borrow type (raw slice vs a guard) pending the
  object layout (#111).

## Acceptance and test hooks

- Every command in the Tier 0/1 set is implementable as a composition of the four
  primitives with no other store entry point (a layering test: the command crate
  depends only on the storage-API crate's four functions + hook types).
- An `RMW` mutator observes and writes atomically with no lock on the owning core.
- The eviction, expiration, accounting, and snapshot hooks all fire through the
  primitives, verified by the conformance and property suites (#95/#98).

## References

- ADR-0002, ADR-0005, ADR-0006, ADR-0007, ADR-0008; issues #15, #35, #64, #48,
  #51, #41, #60, #29, #128, #129, #95.
- Claims: [garnet-narrow-waist-api], [garnet-two-stores],
  [faster-hybridlog-three-regions].
