# ADR-0009: Compatibility tiering (Tier 0-4) and the behavioral-equivalence contract

Status: Accepted
Issue: #16

## Context

"Redis-compatible" is unfalsifiable until we say exactly which commands work and
what "work" means. The README, the vision EPIC (#1), and the conformance suite
(#95) all need one published, testable definition, and the Compatible tenet
(ranked first) demands it be honest. Redis exposes roughly 240 core commands
[redis-core-command-count]; targeting all of them at once would force Lua,
Functions, and Streams, which are committed non-goals.

## Decision

Publish a five-tier compatibility map, version-pinned, with **behavioral
equivalence** (not bit-identical replies) as the contract, measured against a
pinned Valkey oracle [valkey-resp-identical]:

- **Tier 0 (Connection):** the handshake and connection surface (PING, HELLO,
  AUTH, SELECT, CLIENT basics) so any client connects and negotiates RESP2/RESP3
  [resp3-opt-in-via-hello].
- **Tier 1 (Core):** strings and generic keyspace (GET/SET/DEL/EXISTS/EXPIRE/TTL/
  TYPE/SCAN), the 80/20 a cache client actually uses.
- **Tier 2 (Collections):** lists, sets, hashes, sorted sets.
- **Tier 3 (Extended):** bitmaps, HyperLogLog, geo, pub/sub
  [sharded-pubsub-7.0].
- **Tier 4 (Out of scope for v1):** Lua/Functions and Streams, the committed
  non-goals.

A command is supported with Valkey-identical observable behavior or documented
as unsupported; a tier is "met" only when the conformance suite (#95) passes for
it.

## Rejected Alternatives

- **"Broadly Redis-compatible" with no tiers.** Rejected on Compatible: it stays
  unfalsifiable, users hit gaps in production, and the conformance suite has no
  spec to test against.
- **All-or-nothing: the full ~240-command surface at once
  [redis-core-command-count].** Rejected: it forces the Tier 4 non-goals and
  delays a usable binary; tiers let Tier 0 and 1 ship and be certified first.
- **Bit-identical replies as the contract.** Rejected: it freezes internal
  representations the Efficient tenet must evolve; behavioral equivalence
  (structure, types, observable side effects, error class) preserves client
  compatibility without that freeze (invariant 5). Bulk-string and protocol
  limits (for example the 512 MB cap [bulk-string-max-512mb]) are honored as
  observable behavior.

## Consequences

- The README and EPIC cite this one map; the conformance suite (#95) and the
  differential oracle (#96/#97) test against it tier by tier.
- Default-behavior divergences (for example cache-mode-by-default, ADR-0007) are
  documented as default differences, distinct from command-semantics
  differences.
- The per-command semantics within each tier are owned by the command-surface
  design issues (the audit-filed #128 and siblings).
