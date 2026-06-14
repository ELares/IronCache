# Design: OBJECT ENCODING / DEBUG OBJECT compatibility mapping

Issue: #40. Decisions: ADR-0009 (behavioral equivalence via OBJECT ENCODING),
ADR-0018 (encoding thresholds). Related: #35 (index, parent), #111 (object
layout), #112 (scalar encodings), #113 (collection container), #134 (large
zset), #135 (large list), #95 (conformance), #150 (DEBUG OBJECT command).

## Goal and scope

Clients and conformance suites introspect storage through OBJECT ENCODING and
DEBUG OBJECT and branch on the exact synthetic name returned, so IronCache must
report Redis-vocabulary names even though its internal representations are chosen
for a Rust runtime, not Redis's C internals (ADR-0009). This spec fixes the total
function from every internal representation to one reported name, the DEBUG
OBJECT field synthesis, and the assert_encoding wiring. Out of scope are the
structures themselves (#35, #112, #113, #134, #135) and the thresholds at which
they convert (ADR-0018/#37); this spec reports the active representation's name,
it does not decide the representation.

## Design

### The representation-to-name table (total function)

- The reported vocabulary is the eight Redis synthetic names the conformance
  suite asserts on [valkey-assert-encoding-vocab]: embstr, int, raw, listpack,
  intset, hashtable, skiplist, quicklist. The mapping is a total function: each
  internal representation maps to exactly one name, never two. Issue #40's
  acceptance table collapsed embstr/raw into a single bullet and left the
  embstr-vs-raw split as an open decision; this spec keeps both names, matching
  ENCODINGS.md, which reports out-of-line strings as the `raw`-class.
- String types: a pointer-tagged inline integer (#112) reports `int`; an inline
  short string (SSO, the embstr-class up to the inline threshold
  [redis-embstr-threshold-44]) reports `embstr`; an out-of-line string with a
  variable-width header [redis-sds-header-variants] reports `raw`. The embstr/raw
  boundary is the inline-value threshold (#111), reported off the current
  representation, not recomputed from config.
- Collection types: the small universal `pack` container (#113) reports
  `listpack` for hash, list, set, and zset alike; the all-integer sorted-array
  analog [redis-intset-layout] reports `intset`. The large hash and set report
  `hashtable`, the large sorted set (#134) reports `skiplist`, and the chunked
  list deque (#135) reports `quicklist`. The borrowed name `quicklist` describes
  the chunked shape, not the 32-byte Redis node layout [redis-quicklist-node-32-bytes].

### Name derives from representation, not from thresholds

- The reported name is a pure function of the active internal representation, so
  reconfiguring an ADR-0018 threshold (which changes WHEN a value converts) never
  changes the name reported for a value that has not converted. Two keys of the
  same logical type report different names exactly when their representations
  differ (for example a 50-member zset listpack vs a 5000-member zset skiplist),
  matching the oracle (ADR-0009).

### DEBUG OBJECT field synthesis

- DEBUG OBJECT emits a line with `encoding:<name>` from the same function above,
  so OBJECT ENCODING and DEBUG OBJECT always agree on the name. Fields IronCache
  can compute honestly are synthesized: `serializedlength` from the value's
  encoded byte size, and for `quicklist` keys `ql_nodes` (the live chunk count)
  and `ql_avg_node` (elements per chunk), both derived from IronCache's chunking
  (#135) rather than a Redis node count [redis-quicklist-node-32-bytes]. Fields
  that name a Redis-internal IronCache does not have are omitted rather than
  emitted as fabricated zeros, so no test asserts on an invented internal.

### assert_encoding wiring and rejected alternatives

- The conformance suite adopts Valkey's assert_encoding helper, which runs OBJECT
  ENCODING and matches the expected name from the same vocabulary
  [valkey-assert-encoding-vocab], treating a mismatch as a correctness failure
  (#95). Reporting native names (`btree-zset`, `radix-hash`) even behind a flag
  is rejected: it would fork the test corpus and defeat compatibility. A separate
  read-only native-introspection verb for IronCache's own debugging is left open
  and would never be OBJECT ENCODING.

## Open questions

- The exact embstr-vs-raw byte boundary (the inline-value threshold shared with
  #111), and whether any string ever reports `raw` below it.
- Which DEBUG OBJECT fields beyond serializedlength/ql_nodes/ql_avg_node are
  load-bearing for the target suites, surfaced as #95 enumerates them.
- Whether a native-name introspection command is worth adding for debugging
  (separate verb, never OBJECT ENCODING).

## Acceptance and test hooks

- Every internal representation maps to exactly one name from {embstr, int, raw,
  listpack, intset, hashtable, skiplist, quicklist} (a documented total-function
  table, unit-tested for totality).
- OBJECT ENCODING and DEBUG OBJECT agree on the name for the same key, and the
  name does not change when only thresholds are reconfigured (property test).
- assert_encoding passes against IronCache across the size ladder and at every
  conversion boundary [valkey-assert-encoding-vocab] (#95/#97/#98).
- A `quicklist` key returns a plausible ql_nodes derived from IronCache chunking
  [redis-quicklist-node-32-bytes] (#135).

## References

- ADR-0009, ADR-0018; issues #35, #111, #112, #113, #134, #135, #95, #97, #98,
  #150.
- Claims: [valkey-assert-encoding-vocab], [redis-quicklist-node-32-bytes],
  [redis-embstr-threshold-44], [redis-sds-header-variants], [redis-intset-layout].
