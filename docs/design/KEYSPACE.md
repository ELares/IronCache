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

- A SCAN cursor encodes the slot id (high bits) plus an intra-slot position: the
  last FULL 64-bit key-hash returned, plus a small discriminator counting how many
  keys sharing that exact hash were already emitted. Iteration within a slot
  proceeds in ascending full-key-hash order, using the full hash recomputed from
  the embedded key, never the truncated in-bucket tag. Resumption returns keys
  whose hash is strictly greater than the cursor hash, plus any not-yet-emitted
  keys whose hash equals the cursor hash (selected via the discriminator), so two
  distinct keys that collide on the same 64-bit hash are never skipped. Because a
  key's full hash is invariant across a `hashbrown` resize, this returns every key
  present throughout the scan at least once regardless of any resize between calls,
  including equal-hash keys. Keys inserted mid-scan may or may not appear; that is
  within contract.
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
moved with a MOVED-style redirection [redis-cluster-moved-ask] (the client
re-SCANs the new owner), so no key is silently dropped. This is why the cursor
encodes the slot explicitly.

### DUMP / RESTORE

`DUMP` emits, and `RESTORE` accepts, a byte-identical serialization blob
compatible with the Valkey/Redis format (the requirement #39/#40 lean on): the
value type, its version, and the payload, with the trailing version + CRC. This
is specified here as the canonical serializer so #39/#40 (intset/HLL/OBJECT
ENCODING) have one blob format to target, validated against the oracle
[valkey-resp-identical]. The 512 MB value bound [bulk-string-max-512mb] applies.

> **LOUD NOTE (current reality): DUMP is STRING-only; RESTORE also accepts SET, HASH, ZSET, and
> LIST.** As implemented today, `DUMP` (encode) emits the **STRING type ONLY** (a HyperLogLog counts,
> since an HLL is stored as a string); a `DUMP` of a list, hash, set, or zset returns an error.
> `RESTORE` (decode) accepts the **STRING type, the SET type in all three RDB encodings** (intset,
> listpack, and the plain length-prefixed set), **the HASH type in its two non-field-TTL encodings**
> (listpack and the plain length-prefixed hash), **the ZSET type in all three encodings**
> (`RDB_TYPE_ZSET_2` binary-double scores, the legacy `RDB_TYPE_ZSET` ASCII scores, and listpack),
> **and the LIST type in the modern `RDB_TYPE_LIST_QUICKLIST_2` encoding** (the quicklist of listpack
> + plain nodes that Redis 7.x DUMPs, insertion order preserved across nodes) **plus the trivial
> legacy `RDB_TYPE_LIST`**, so a set, a (non-field-TTL) hash, a sorted set, OR a list `DUMP`ed by a
> real Redis `RESTORE`s with identical members/fields/scores/order (a NaN score is refused, matching
> `ZADD`; +inf/-inf are preserved). A HASH carrying per-field TTLs (Redis 7.4+ `listpack_ex` /
> `metadata` encodings) and the legacy ziplist-based list encodings (`RDB_TYPE_LIST_QUICKLIST` /
> `RDB_TYPE_LIST_ZIPLIST`, which modern Redis never DUMPs) are still refused, so full multi-type
> `MIGRATE` compatibility does NOT hold yet. The remaining per-type codecs (hash field-TTLs, the
> ziplist-based list forms) and `DUMP` of the aggregate types are tracked in #612.

## Open questions

- The exact cursor encoding bit-split (slot id, full-hash, and the equal-hash
  discriminator) given 16384 slots [redis-cluster-hash-slots-16384] and a 64-bit
  cursor, and whether very large slots need a secondary cursor field (a 64-bit
  cursor may be too narrow to carry slot id + full hash + discriminator, so the
  cursor encoding itself is an open question, possibly an opaque token rather than
  a literal hash).
- Hash-ordered iteration is REQUIRED for the guarantee, not optional: a
  natural-table-order cursor position is meaningless after an all-at-once resize
  (entries rehash to new buckets), so table order would break stability. The open
  question is only the mechanism and its cost: a per-slot secondary index kept
  sorted by hash, versus sorting each batch on the fly (O(n log n) per batch,
  which is in tension with the bounded-batch yield property above), measured on
  the harness (#8).

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
