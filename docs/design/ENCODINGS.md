# Design: Compact scalar value encodings

Issue: #112 (split from #35). Decisions: ADR-0009 (behavioral equivalence via
OBJECT ENCODING), ADR-0018 (encoding thresholds). Related: #35 (index), #111
(object layout).

## Goal and scope

Specify how scalar string values are encoded inside the kvobj (#111) to minimize
bytes-per-key: inline short strings, pointer-tagged small integers and floats,
and a variable-width string header. Collection encodings (listpack-equivalents,
intset) are #113; this covers the string type and the integer/float fast path.

## Design

### Inline short strings (SSO)

Short string values live inline in the kvobj allocation (#111), so a small string
needs no second allocation, generalizing Redis's `embstr` (embedded strings up to
44 bytes) [redis-embstr-threshold-44] to IronCache's single-allocation object.
Strings above the inline threshold point to an out-of-line buffer.

### Pointer-tagged small integers and floats

Integer (and small float) values that fit are stored directly in the value word
using pointer tagging (low bits flag "this word is an integer, not a pointer"),
so an integer value needs zero value bytes beyond the word already in the kvobj.
`INCR`/`DECR`/`INCRBYFLOAT` operate on the tagged word in place via `RMW` (#34).
`OBJECT ENCODING` reports `int` for these, matching Redis (ADR-0009).

### Variable-width string header

Out-of-line strings use a variable-width length header (1, 2, 4, or 8 byte length
field chosen by string size), the way Redis's SDS picks among `sdshdr5/8/16/32/64`
[redis-sds-header-variants], so a short out-of-line string pays a 1-byte length
header, not a fixed 16-byte one.

### Rejected: the shared-integer pool

Redis pre-allocates a pool of 10000 shared small-integer objects to dedupe common
integers [redis-shared-integers-10000]. IronCache **rejects** this: with
pointer-tagged inline integers (above) an integer value occupies no separate
allocation at all, so there is nothing to share and the pool would be pure
complexity and a refcount surface for no memory win. This is a deliberate
divergence (a default-internal-representation difference, not an observable one:
`OBJECT ENCODING` still reports `int`).

## Open questions

- The inline-string threshold (shared with #111's inline-value threshold) and
  whether floats are tagged inline or always out-of-line (precision vs space),
  tuned on the memory harness (#8).
- Whether to keep a tiny static cache for the few hottest integers (0, 1) despite
  rejecting the general pool (likely not, pending #8 data).

## Acceptance and test hooks

- An integer value (for example `SET k 12345`) occupies no value allocation
  beyond the kvobj word, and `OBJECT ENCODING k` reports `int` (matches oracle,
  #97).
- A 10-byte string occupies the kvobj inline with a 1-byte length header, no
  second allocation (layout test).
- `APPEND`/`SETRANGE` growing a tagged-int or inline string promotes it correctly
  and `OBJECT ENCODING` transitions match the oracle (property test #98).

## References

- ADR-0009, ADR-0018; issues #35, #111, #113, #34, #8, #97, #98.
- Claims: [redis-embstr-threshold-44], [redis-sds-header-variants],
  [redis-shared-integers-10000].
