# Design: Generic keyspace commands and the SCAN cursor-stability contract

Issue: #129. Decisions: ADR-0005 (per-shard map), ADR-0009 (behavioral
equivalence), ADR-0011 (slot-ready). Related: #35 (per-slot hash table), #75
(slot migration), #40 (DUMP/RESTORE consumers), #128 (per-type commands).

## Goal and scope

The generic keyspace commands (`DEL`, `UNLINK`, `EXISTS`, `TYPE`, `KEYS`,
`RANDOMKEY`, `RENAME`, `COPY`, `TOUCH`, `DUMP`/`RESTORE`) and, the load-bearing
part, the `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` cursor-stability contract. The cursor
guarantee is a cross-cutting concurrency/migration invariant, not a per-collection
behavior, which is why it is owned here separately from #128.

## Design

### Generic commands

`DEL`/`UNLINK` (multi-key delete; `UNLINK` enqueues large-value frees on the
background reclamation queue, #51), `EXISTS` (counts), `TYPE`, `TOUCH`,
`RANDOMKEY`, `RENAME`/`RENAMENX`, and `COPY` are compositions of the storage
primitives (#34); cross-shard forms (a `RENAME`/`COPY` whose two keys differ in
shard) go through the coordinator (#29). `KEYS` is supported but documented as
O(N) and discouraged, exactly as in Redis.

### SCAN cursor-stability contract

The contract IronCache must keep: every key present for the entire scan is
returned at least once; keys may be returned more than once; keys added or
removed during the scan may or may not appear. The challenge is keeping this
across a table resize.

Redis achieves it with reverse-binary-iteration over its dict bucket index, a
trick tied to its bucket layout and its incremental two-table rehash
[redis-dict-two-table-rehash]. IronCache's per-slot table is a stock `hashbrown`
SwissTable (ADR-0005, #35) that resizes all-at-once, so the Redis bucket trick
does not transfer. Instead the cursor is defined over the key hash, which a
resize does not change:

- A SCAN cursor encodes the slot id (high bits) plus an intra-slot position that
  is the last key-hash returned. Iteration within a slot proceeds in ascending
  key-hash order, using the hash IronCache already computes for placement (the
  hash tag in the bucket, full hash recomputable from the embedded key). Because a
  key's hash is invariant across a `hashbrown` resize, resuming at "next key whose
  hash exceeds the cursor" returns every key present throughout at least once,
  regardless of any resize between calls. Keys inserted mid-scan may or may not
  appear; that is within contract.
- Because the shard is single-owner (ADR-0005), no locking is needed; SCAN runs as
  bounded batches on the owning core (`COUNT` is a hint to batch size) and yields
  between batches so it never stalls the core.
- `HSCAN`/`SSCAN`/`ZSCAN` apply the same hash-ordered cursor within the
  collection's own table; for small listpack-encoded collections (below the
  ADR-0018 thresholds) the whole collection is returned in one reply with cursor
  0, as Redis does.

### SCAN under slot migration

When a slot is being migrated out (#75), its keys move to another node; the
cluster SCAN contract is that clients SCAN every node, so a node only owes the
guarantee for slots it owns at the time. A slot migrating mid-scan is handled by
the migration design (#75): the cursor's slot id lets the node report the slot as
moved (the client re-SCANs the new owner), so no key is silently dropped. This is
why the cursor encodes the slot explicitly.

### DUMP / RESTORE

`DUMP` emits, and `RESTORE` accepts, a byte-identical serialization blob
compatible with the Valkey/Redis format (the requirement #39/#40 lean on): the
value type, its version, and the payload, with the trailing version + CRC. This
is specified here as the canonical serializer so #39/#40 (intset/HLL/OBJECT
ENCODING) have one blob format to target, validated against the oracle
[valkey-resp-identical]. The 512 MB value bound [bulk-string-max-512mb] applies.

## Open questions

- The exact cursor encoding bit-split (slot id vs intra-slot hash position) given
  16384 slots and a 64-bit cursor, and whether very large slots need a secondary
  cursor field.
- Whether hash-ordered iteration costs enough over natural table order to warrant
  a per-slot secondary index, measured on the harness (#8).

## Acceptance and test hooks

- A SCAN that spans a resize returns every key present throughout at least once
  (a property test that forces a `hashbrown` resize mid-scan, #98).
- A SCAN that spans a slot migration drops no owned key and reports the migrated
  slot so the client re-scans the new owner (#75 integration test).
- `DUMP` then `RESTORE` round-trips byte-identically and matches the oracle for
  every type (#97).

## References

- ADR-0005, ADR-0009, ADR-0011; issues #35, #75, #40, #39, #128, #34, #29, #51,
  #8, #95, #97, #98.
- Claims: [redis-dict-two-table-rehash], [valkey-resp-identical],
  [bulk-string-max-512mb], [redis-cluster-moved-ask], [redis-cluster-hash-slots-16384].
