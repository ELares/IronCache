# ADR-0019: RESP3 reply-shaping policy and error-string fidelity

Status: Accepted
Issue: #17

## Context

Modern official clients negotiate RESP3 by default (redis-py 8, node-redis 6)
while the server still defaults to RESP2 [client-default-resp3-redis8], so the
RESP3 emission path is exercised constantly. RESP3 is opt-in per connection via
`HELLO 3` [resp3-opt-in-via-hello]. The parser, the error catalog (#18), and the
conformance suite (#95) all need one ratified contract for what bytes IronCache
emits once a connection is upgraded.

## Decision

Match Redis per command. When `proto=3`, emit the native RESP3 aggregate types
(map `%`, set `~`, double `,`, big number `(`, verbatim `=`, push `>`) for
exactly the commands where Redis/Valkey emit them, and RESP2 shapes everywhere
else; under `proto=2` emit the RESP2 shapes [resp-type-prefixes]. Null is the
RESP3 null `_` under proto=3 and the null bulk/array (`$-1` / `*-1`) under proto=2
[resp2-null-encodings]. Error replies are byte-identical to Valkey on the leading
token and use the exact catalog text (#18); the contract is behavioral
equivalence against the pinned Valkey oracle [valkey-resp-identical], measured per
connection in both proto modes.

## Rejected Alternatives

- **Always emit RESP2 shapes even under proto=3 (ignore the upgrade).** Rejected
  on Compatible: RESP3-default clients would receive shapes they did not
  negotiate, breaking map/double-aware code paths; the upgrade must change the
  wire.
- **Emit RESP3 aggregates everywhere a reply is map-like, even where Redis stays
  RESP2.** Rejected: it diverges from the per-command observable contract;
  behavioral equivalence means matching Redis's actual per-command choice, not a
  cleaner-but-different scheme.

## Consequences

- The serializer is parameterized by per-connection proto (set by `HELLO`), and
  every command's reply has a defined shape in both modes, tested in both by the
  conformance/differential suite (#95/#97) which runs each case under proto=2 and
  post-`HELLO 3`.
- The error catalog (#18) is the single source of error text; this ADR fixes that
  errors are byte-identical on the leading token and that null/aggregate encodings
  switch by proto.
- Client-side caching push frames and pub/sub push (`>`) follow the same per-proto
  rule (RESP3 push vs the RESP2 invalidation channel) and are detailed in the
  protocol design (#15).
