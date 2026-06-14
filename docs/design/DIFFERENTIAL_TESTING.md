# Design: Differential testing against pinned Valkey and Redis

Issue: #97. Decisions: ADR-0009 (behavioral-equivalence tiering, leading-token
error bar), ADR-0019 (RESP3 matched per command), ADR-0003 (determinism / Env
seam). Related: #95 (correctness stack, parent), #96 (Valkey oracle + head-to-head
baseline task), #18 (error catalog, ERRORS.md), #98 (property suite, shared
reference model), #129 (SCAN cursor contract), #40 (OBJECT ENCODING name map).

## Goal and scope

IronCache reimplements the Redis wire and command contract from scratch, so the
cheapest credible proof of compatibility is to run the real thing as an oracle.
This spec defines a harness that replays identical command streams (curated plus
fuzzer-generated) against IronCache and a pinned `valkey-server` / `redis-server`,
then diffs the RESP replies, error tokens, and `OBJECT ENCODING` reporting. A
clean diff is the gate that lets us advertise a compatibility tier; a divergence
blocks the claim. Scope: single-node request/response and pipelined streams under
RESP2 and RESP3. Out of scope: cluster/replication correctness (Jepsen+Elle,
#99/#100) and throughput (#8). This is the enforcement arm of the per-command
conformance suite (#95) and consumes the oracle and error decisions of #96/#18.

## Design

### Shared reference model (resolves the #98 circular dependency)

- The store is #35; both test families (#97 here and #98) sit under the #95
  correctness stack and are its consumers, so #98's verbatim "where does the
  reference model live so #95 and #35 both consume it without a circular
  dependency?" and this doc's #35-vs-harness wording describe the same cycle.
- The reference model lives in a standalone `reference-model` crate that depends
  on neither the engine store (#35) nor this harness (#97). It is a small,
  deliberately naive in-memory model of Redis per-type semantics. This harness
  (#97) and the property suite (#98) both consume it as a library; the real store
  never depends on it. A leaf crate with no upward edge breaks the otherwise
  circular dependency and gives one source of truth for "correct" shared by both
  test families. Here the model is the local fast oracle for offline triage; the
  pinned upstream server is the authoritative oracle for the CI diff.

### Oracles and the diff bar

- Pinned `valkey-server` (8.x/9.x line) is the primary oracle and pinned
  `redis-server` 7.2 the secondary. Valkey is wire-identical to Redis 7.2
  RESP2/RESP3 [valkey-resp-identical] and BSD-3-Clause [valkey-license-bsd3], so a
  single harness drives both and surfaces any Redis/Valkey divergence as a
  first-class diff; relicensed Redis (SSPL/RSAL) is never vendored as the oracle.
- Replies, framing, and `OBJECT ENCODING` are compared byte-exact. Error text is
  compared at the leading uppercase token only by default (ADR-0009): clients
  pattern-match the `ERR`/`WRONGTYPE`/`NOPROTO` prefix [hello-noproto-error], and
  exact wording churns upstream. The handshake-critical and control-flow set
  (`NOPROTO`, `NOAUTH`/`WRONGPASS`, `EXECABORT`, `WRONGTYPE`, unknown-command,
  arity) is held to byte-exact full text per ERRORS.md (#18); that exact-text set
  is the explicit exception list to the leading-token default.

### Encoding mapping

- The Valkey/Redis TCL `assert_encoding` vocabulary [valkey-assert-encoding-vocab]
  validates `OBJECT ENCODING` (listpack/intset/quicklist/skiplist). IronCache's
  internal representations differ, so encoding diffs are compared against
  IronCache's own `OBJECT ENCODING` reporting (ADR-0009 behavioral equivalence,
  with the native-vs-synthetic name-string map deferred to #40) rather than
  asserted verbatim against an internal layout.

### Double-run over both protocols

- Every stream runs twice: once on the RESP2 default and once after `HELLO 3`
  [resp3-opt-in-via-hello]. Null and aggregate shapes differ by protocol
  [resp2-null-encodings], so each is diffed per proto; both are first-class
  because modern clients default to RESP3 [client-default-resp3-redis8]. Per
  ADR-0019 the matched proto shape is whatever Redis emits for that command, so
  when IronCache emits a RESP2-shaped reply under proto=3 where Redis would not it
  is a diff failure unless listed as a documented, reviewed deviation.

### Stream generation

- A curated per-command corpus over the ~240-command surface
  [redis-core-command-count] plus structure-aware mutation seeded from real RESP
  captures and the parser fuzz targets (#95). Pure random bytes mostly hit the
  parser; structured mutation reaches semantics. `redis-benchmark` is rejected as
  a stream source: its default single-key workload [redis-benchmark-single-key-default]
  exercises almost no keyspace, whereas a memtier-style corpus that supports skew
  [memtier-supports-zipfian] gives far wider coverage.

### Non-determinism normalization

- Diffs are made stable before comparison: `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` cursor
  values are canonicalized to the result set (the #129 cursor contract), not the
  raw cursor; `RANDOMKEY` and other randomized picks run against a seeded keyspace
  and are compared as set membership; hash and small-collection field ordering is
  sorted before diff; expiry-timing fields are bucketed. `INFO`, server-id
  fields, and run-specific values are on the exemption list and excluded from the
  byte-exact match.

### Drift baseline and exemption review

- Oracle versions are pinned in the run manifest and in the committed
  `VALKEY_VERSIONS` table, bumped only by explicit PR (never floating tags)
  [valkey-version-landscape-2026]. A scheduled re-pin job re-diffs the golden set
  on upstream bumps; because Valkey and Redis evolve independently after the fork
  [valkey-fork-origin], per-version diff baselines are retained so a new
  divergence surfaces as a reviewable baseline change, not silent noise. Each
  exemption-list entry carries a reason and is reviewed in the same PR that adds it.

## Open questions

- Cadence and ownership of the oracle re-pin / drift job (nightly vs on-bump).
- Whether any non-handshake error graduates from leading-token to exact-text as
  commands land (resolved case by case against the oracle, ERRORS.md #18).
- How aggressively to normalize expiry-timing fields without masking real bugs.

## Acceptance and test hooks

- The harness replays an identical stream to IronCache and both pinned oracles and
  reports per-command diffs of RESP frame bytes, error tokens, and `OBJECT
  ENCODING`.
- Every stream runs under RESP2 and post-`HELLO 3` RESP3; null/aggregate shapes
  are diffed per proto [resp2-null-encodings].
- Fuzz/mutation streams feed the same diff path, seeded from the shared RESP
  capture corpus; divergences emit a reproducible seed+trace artifact (#96).
- Error comparison enforces leading-token by default with the ERRORS.md exact-text
  set as the listed exception [hello-noproto-error].
- Oracle versions are pinned in the manifest; the scheduled job re-diffs on bumps
  and flags drift [valkey-version-landscape-2026].
- The suite runs in CI and a clean diff gates any published compatibility-tier
  claim (#95).

## References

- ADR-0003, ADR-0009, ADR-0019; issues #95, #96, #98, #18, #129, #40, #1
  (vision); specs TESTING.md, ERRORS.md, KEYSPACE.md, PROPERTY_TESTING.md.
- Claims: [valkey-resp-identical], [valkey-license-bsd3],
  [valkey-assert-encoding-vocab], [hello-noproto-error], [resp3-opt-in-via-hello],
  [resp2-null-encodings], [client-default-resp3-redis8], [redis-core-command-count],
  [redis-benchmark-single-key-default], [memtier-supports-zipfian],
  [valkey-version-landscape-2026], [valkey-fork-origin].
