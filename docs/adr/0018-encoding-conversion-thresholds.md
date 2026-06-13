# ADR-0018: Fixed Redis-compatible encoding-conversion thresholds, adaptive deferred

Status: Accepted
Issue: #37

## Context

IronCache's collections switch in-memory representation as they grow (a compact
listpack/intset-equivalent while small, a hashtable/skiplist when large), exactly
as Redis does. #37 asked whether the conversion thresholds should be fixed or
adaptive. (The #37 research doc was blocked by an orchestrator path bug; the
decision below is grounded directly in the pinned claims rather than that doc.)
The tension is Compatible and Simple (predictable, Redis-matching behavior) vs a
possible Efficient win from per-workload adaptive thresholds.

## Decision

Ship **fixed thresholds matching the Redis defaults**, surfaced through
`OBJECT ENCODING` (ADR-0009 behavioral equivalence): hash-max-listpack-entries
512 / value 64 [redis-hash-max-listpack-entries-512], the set intset and listpack
limits [redis-set-encodings-thresholds], zset-max-listpack-entries 128 / value 64
[redis-zset-max-listpack-entries-128], and list-max-listpack-size -2 (an 8 KB
node) [redis-list-max-listpack-size-neg2]. They are config knobs (#85), as in
Redis. **Adaptive thresholds are deferred to the off-path advisor** (#88),
never the hot path.

## Rejected Alternatives

- **Adaptive thresholds in the engine from day one.** Rejected on Compatible and
  Simple: per-workload thresholds make `OBJECT ENCODING` and memory behavior
  non-deterministic and hard to reason about, and any threshold logic that
  reacts on the hot path violates the determinism invariant. The Efficient upside
  is speculative and belongs to the advisor's evidence loop (#88/#90), off the
  hot path.
- **Non-Redis fixed thresholds tuned for our encodings.** Rejected for v1: it
  diverges from Redis-observable conversion points for no proven gain; revisit
  only if the encoding design (#35) shows a clear win, as a superseding ADR.

## Consequences

- Conversion points match Redis, so `OBJECT ENCODING`-sensitive clients and the
  differential oracle (#97) see familiar behavior.
- The thresholds are config knobs (#85); the advisor (#88) may later recommend
  per-keyspace overrides off the hot path, gated by ADR-0013's posture.
- The encoding design (#35) and the per-type representations (#113/#134/#135)
  implement these conversion points.
