# Design: Per-data-type command semantics (strings, lists, hashes, sets, sorted sets)

Issue: #128. Decisions: ADR-0009 (compat tiering, behavioral equivalence),
ADR-0010 (no Lua), ADR-0019 (reply shaping). Related: #15 (dispatch), #34
(storage primitives), #35/#39/#112/#113/#134/#135 (encodings), #40 (OBJECT
ENCODING), #95/#98 (conformance/property), #130 (blocking).

## Goal and scope

The five core collection groups (strings, lists, hashes, sets, sorted sets) are
the bulk of the Tier 1 surface, and the top-ranked Compatible tenet had no owner
for their command-level contract. This specifies that contract and the method by
which it is made testable, rather than re-printing all ~240 commands
[redis-core-command-count]. In scope: arity/variadic rules, the `*STORE` family,
counted/`LIMIT` forms, reply shapes, the option flags, and the error edges. Out
of scope: the in-memory representations (#35/#112/#113/#134/#135), blocking
variants (#130), and pub/sub.

## Design

### Per-command spec table (the deliverable)

Each command has a machine-readable spec row: name, arity (fixed or variadic
with min/max), key positions (for routing and `COMMAND GETKEYS`), the option
flags it accepts, its reply shape under RESP2 and RESP3 (ADR-0019), and its error
edges. The dispatch layer (#15) is generated from this table (perfect-hash on the
name), and the conformance/differential suite (#95/#97) and property tests (#98)
validate every row against the pinned Valkey oracle [valkey-resp-identical]. The
table, not prose, is the source of truth; this document fixes its shape and the
cross-cutting rules below.

### Mapping to storage primitives

Every per-type collection command in this document is a composition of the four
storage primitives (#34) (iteration/SCAN, blocking, and pub/sub use their own
entry points, per STORAGE_API.md): reads use
`Read`, blind writes `Upsert`, removals `Delete`, and all in-place mutation
(`INCR`, `APPEND`, `SETRANGE`, `LPUSH`/`RPUSH`, `HSET`, `SADD`, `ZADD`, and the
counted/flagged forms) is one `RMW` whose mutator runs atomically on the owning
core. The `*STORE` family (`SINTERSTORE`, `ZRANGESTORE`, `SDIFFSTORE`, ...) reads
its source keys and `Upsert`s the destination; when sources and destination span
shards it is decomposed by the coordinator (#29).

### Cross-cutting option-flag semantics

Specified once, applied per command:

- `NX`/`XX` (set only if absent/present), `GT`/`LT` (ZADD: update only if greater/
  lesser), `CH` (return changed count), `INCR` (ZADD increment mode), `KEEPTTL`
  (SET preserves TTL), `EX`/`PX`/`EXAT`/`PXAT`/`PERSIST` (TTL on SET).
- Counted/`LIMIT` forms: `SINTERCARD numkeys ... LIMIT n`, `LMPOP`/`ZMPOP`/
  `SMISMEMBER`/`GETDEL`/`GETEX`, with exact reply shapes.

### Error and edge contract

- `WRONGTYPE` on an operation against a key of the wrong type (the catalog, #18),
  checked before mutation.
- Range commands clamp/normalize negative indices Redis-identically; numeric
  overflow on `INCR`/`INCRBY`/`INCRBYFLOAT` returns the Redis error text;
  out-of-range/`not an integer or out of range` edges match the oracle.
- Encoding transitions at the ADR-0018 thresholds (hash 512/64
  [redis-hash-max-listpack-entries-512], set [redis-set-encodings-thresholds],
  zset [redis-zset-max-listpack-entries-128], list [redis-list-max-listpack-size-neg2])
  are observable only through `OBJECT ENCODING` [valkey-assert-encoding-vocab]
  (#40); the command result is identical across the transition.

## Open questions

- Whether to ship one design child per group or one generated table (this doc
  takes the table approach; per-group children may still split the implementation
  work).
- Which Tier 2 commands are v1 vs deferred within each group (the long tail of
  rare commands), gated by the conformance tier (#16).

## Acceptance and test hooks

- The spec table drives dispatch (#15) and `COMMAND GETKEYS`/arity validation; an
  unmodified client's command set runs green against the oracle (#97).
- Every flag (`NX`/`XX`/`GT`/`LT`/`CH`/`INCR`/`KEEPTTL`) and counted form has a
  conformance case under RESP2 and RESP3; encoding transitions are property-tested
  (#98) with `OBJECT ENCODING` asserted at the boundary.
- `WRONGTYPE`, overflow, and range edges match the pinned oracle byte for byte on
  the leading token (#18).

## References

- ADR-0009, ADR-0010, ADR-0018, ADR-0019; issues #15, #34, #35, #39, #40, #112,
  #113, #134, #135, #95, #97, #98, #130, #16, #29, #18.
- Claims: [redis-core-command-count], [valkey-resp-identical],
  [valkey-assert-encoding-vocab], [redis-hash-max-listpack-entries-512],
  [redis-set-encodings-thresholds], [redis-zset-max-listpack-entries-128],
  [redis-list-max-listpack-size-neg2].
