# ADR-0024: Geo command family scope (non-goal for v1)

Status: Accepted
Issue: #133

## Context

The Geo command family (GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH,
GEOSEARCHSTORE) is a geohash-encoded veneer over the sorted-set type: a member's
longitude/latitude pair is encoded into a single 52-bit interleaved-geohash
score, stored in an ordinary zset, and the search commands are range queries over
that score space. ADR-0009 already places geo in Tier 3 (Extended), alongside
bitmaps, HyperLogLog, pub/sub, and Streams; what ADR-0009 did not settle is
whether v1 builds it or defers it. The pre-implementation coverage audit
(2026-06-13) found no decision at all on geo across the issue corpus, leaving a
silent hole in the published-compatibility contract: a Tier 3 surface with no
in-or-out call. This ADR closes that hole.

The relevant precedent is Streams. ADR-0009 tiers Streams in Tier 3 (an extended
data type, not a runtime surface) while NON_GOALS entry 12 defers building it for
v1; the non-goal defers construction, it does not reclassify the surface. Geo is
kept a distinct decision from Streams because the in/out tradeoff differs: Streams
is a new log-structured type with its own consumer-group machinery, whereas geo
rides the existing zset and adds no new engine type. v1 focuses on the core types
(Tiers 0-2) and a usable, certifiable binary, against a Redis surface of roughly
240 commands [redis-core-command-count].

## Decision

Geo is a **non-goal for v1**. It stays classified Tier 3 under ADR-0009 and is
deferred, consistent with the Streams precedent and the NON_GOALS posture:

- The geo commands are documented as unsupported for v1 (the ADR-0009 contract:
  a command is either supported with Valkey-identical observable behavior or
  documented as unsupported), not silently absent.
- Geo is recorded as a zset-scored convenience layer that can be added later
  **without engine changes**: it needs no new data type, no new on-disk or
  in-memory representation, and no change to the shard model. The deferred
  implementation path is to encode each point as the standard 52-bit
  interleaved-geohash score in an ordinary sorted set and serve GEOSEARCH and
  friends as zset range queries, matching the wire-observable scoring contract
  clients already depend on.
- Because the score encoding is a wire-compatibility contract, pinning that exact
  52-bit interleaved-geohash encoding is left to the future geo design issue, so
  that whenever geo is built it is built to the published-compatibility tenet
  rather than to an ad hoc score space.

## Rejected Alternatives

- **Implement geo in v1.** Build the full family now on top of the zset, encoding
  points as the 52-bit interleaved-geohash score and serving the search commands
  as range queries. Rejected as **scope for v1, not as infeasible** (it demands no
  engine changes, which is exactly why it defers cleanly): it widens the v1
  surface and the conformance burden (#95) for a feature that is a convenience
  layer, not a core cache type, and that can be added later without disturbing the
  engine. Per the tenet order, shipping and certifying Tiers 0-2 first beats
  broadening Tier 3 now.
- **Leave geo undecided.** Rejected on Compatible: an unstated in/out leaves a
  Tier 3 surface and the published-compatibility contract with a silent hole,
  which is exactly the unfalsifiability ADR-0009 exists to remove.

## Consequences

- The published-compatibility map gains an explicit, honest entry: geo is Tier 3,
  deferred, documented-unsupported for v1, matching how Streams (NON_GOALS entry
  12) is carried.
- The conformance suite (#95) and differential oracle (#96/#97) do not gate on
  geo for v1; geo conformance is added with the future geo design issue.
- Reopening geo is cheap and non-breaking: it is a new zset-backed command set
  with no engine change, so it can land in a later milestone without superseding
  this ADR's engine decisions; that future issue owns pinning the exact 52-bit
  interleaved-geohash scoring contract for wire compatibility.
