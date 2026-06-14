# Design: Property-based and model-based tests for every data type

Issue: #98. Decisions: ADR-0018 (fixed Redis-compatible encoding-conversion
thresholds), ADR-0009 (behavioral-equivalence tiering), ADR-0003 (determinism /
Env seam). Related: #95 (correctness stack, parent), #97 (differential oracle,
shares the reference model), #35 (store), #37 (encoding-conversion thresholds
research, settled by ADR-0018), #40 (OBJECT ENCODING name-string mapping).

## Goal and scope

Examples prove a handful of paths; an efficiency-first reimplementation must prove
that every operation sequence keeps each data type correct and Redis-equivalent,
especially at the encoding transitions where IronCache's internal representations
diverge from Redis. This spec defines property-based plus model-based testing for
every type as a CI gate, with the ADR-0018 transition thresholds as the primary
target. Scope: single-node per-type correctness for string/list/set/zset/hash
plus intset and HLL. Out of scope: clustering safety (Jepsen/Elle, #99/#100) and
parser fuzzing (#95), tracked elsewhere.

## Design

### Shared reference model (resolves the circular dependency)

- The reference model lives in a standalone `reference-model` crate that depends
  on neither the engine store (#35) nor the differential harness (#97). It is a
  small, deliberately naive in-memory model of Redis per-type semantics. This
  property suite (#98) and the differential oracle (#97) both consume it as a
  library; the real store never depends on it. A leaf crate with no upward edge
  breaks the otherwise circular dependency ("where does the reference model live
  so #95 and #35 both consume it without a circular dependency?") and gives one
  source of truth for "correct" shared by both test families, rather than two
  diverging definitions.

### Generators per type

- proptest drives the properties, run under bolero so the same property doubles as
  a fast unit gate and a long libFuzzer campaign; quickcheck is rejected because
  it cannot drive libFuzzer. Each type (string/list/set/zset/hash, plus intset
  and HLL) gets a generator of random operation sequences applied in lockstep to
  IronCache and the reference model, asserting reply and state equivalence after
  every step.

### Threshold-straddling strategies

- Strategies are biased to straddle each ADR-0018 boundary (just below, at, just
  above), because uniform random sizes rarely land on a boundary and miss the
  off-by-one and overflow bugs that live there. The pinned thresholds are:
  hash-max-listpack-entries 512 / value 64 [redis-hash-max-listpack-entries-512],
  set-max-intset-entries 512 with set-max-listpack-entries 128 / value 64
  [redis-set-encodings-thresholds] over the sorted-int intset layout
  [redis-intset-layout], zset-max-listpack-entries 128 / value 64
  [redis-zset-max-listpack-entries-128], and list-max-listpack-size -2 (an 8 KB
  node) [redis-list-max-listpack-size-neg2]. Biased generation reliably flips
  listpack/intset/quicklist/skiplist.

### intset and HLL transitions

- The intset boundary is exercised explicitly: integer-only growth past
  set-max-intset-entries and the non-integer insert that converts away from
  intset [redis-intset-layout][redis-set-encodings-thresholds]. The HLL
  sparse-to-dense ladder is exercised at hll-sparse-max-bytes 3000
  [redis-hll-sparse-max-bytes-3000] over the P=14 / 16384-register dense layout
  [redis-hll-p14-registers], so the promotion is hit from both the count side and
  the byte-size side.

### Encoding assertion and CI

- `OBJECT ENCODING` is asserted after every mutation, not only on the final
  state, so a mistimed promotion is pinned to the exact operation that caused it
  [valkey-assert-encoding-vocab]. Encoding names are compared via the IronCache
  `OBJECT ENCODING` reporting (ADR-0009; the native-vs-synthetic name-string
  mapping is #40, the numeric thresholds being #37/ADR-0018), consistent with the
  differential harness.
- CI runs a fixed-seed gate plus an archived shrinking corpus so known failures
  replay deterministically and PR latency stays bounded; a nightly extended bolero
  campaign runs the same properties under libFuzzer. Determinism flows through the
  Env seam (ADR-0003) so a seed reproduces a failure byte-for-byte.

## Open questions

- Encoding-name compatibility: assert synthetic Redis names or IronCache-native
  names mapped in #40.
- One-way promotion only, or also demotion-on-shrink (and anti-thrash hysteresis)
  if #40 adopts it.
- Corpus retention and per-type shrinking budget in CI.

## Acceptance and test hooks

- A proptest/bolero suite per type generates random operation sequences against
  the shared in-memory reference model, asserting reply and state equivalence.
- Strategies straddle every threshold
  [redis-hash-max-listpack-entries-512][redis-set-encodings-thresholds][redis-zset-max-listpack-entries-128][redis-list-max-listpack-size-neg2]
  and the intset boundary [redis-intset-layout].
- The intset and HLL sparse-to-dense transitions are covered explicitly
  [redis-hll-sparse-max-bytes-3000][redis-hll-p14-registers].
- `OBJECT ENCODING` is asserted correct at and around each transition
  [valkey-assert-encoding-vocab].
- The reference model is the same standalone crate consumed by the differential
  oracle (#97, under the #95 stack).
- Property tests run as a required CI gate with a fixed seed and an archived
  shrinking corpus; failures replay deterministically.

## References

- ADR-0003, ADR-0009, ADR-0018; issues #95, #97, #35, #37, #40, #1 (vision);
  specs TESTING.md, DIFFERENTIAL_TESTING.md.
- Claims: [redis-hash-max-listpack-entries-512], [redis-set-encodings-thresholds],
  [redis-intset-layout], [redis-zset-max-listpack-entries-128],
  [redis-list-max-listpack-size-neg2], [redis-hll-sparse-max-bytes-3000],
  [redis-hll-p14-registers], [valkey-assert-encoding-vocab].
