# ADR-0031: Native JSON document type scope (decline in the core binary for now)

Status: Accepted
Issue: #417

## Context

A first-class JSON document type is now core in Redis 8 (the former RedisJSON
module merged in), and both other open competitors ship it: Valkey offers it as a
module, and Dragonfly serves JSON natively. The surface is large: JSON.SET,
JSON.GET, JSON.MGET, JSON.MSET, JSON.MERGE, JSON.DEL, the array family (JSON.ARRAPPEND,
JSON.ARRINSERT, JSON.ARRLEN, JSON.ARRPOP, JSON.ARRTRIM), JSON.NUMINCRBY, the string
family (JSON.STRAPPEND, JSON.STRLEN), the object family (JSON.OBJKEYS, JSON.OBJLEN),
plus JSON.TYPE and JSON.TOGGLE. Implementing it faithfully means a JSON parser, a
tree or binary document representation distinct from every existing type, and a
JSONPath evaluation engine, since most of the commands take a path argument.

The tenet order is Compatible greater than Efficient greater than Simple greater
than Scalable greater than AI-Driven. JSON pulls on Compatible (it is a real and
popular surface) but pushes hard against Simple and against the single-binary,
lean-artifact promise: a JSONPath engine and a document representation are a
standing subsystem, not an incremental command. The mitigating fact is adoption
shape: clients treat JSON as an optional capability they opt into (it was a
module for its entire history and remains a distinct subsystem in core), not as a
baseline wire-compatibility expectation the way strings, hashes, and sorted sets
are. A cache that lacks JSON is not broken for the clients that do not ask for it,
which is the opposite of, say, lacking hash-field TTL or client-side caching.

## Decision

IronCache does **not** build a native JSON document type into the core binary
now:

- Declined for the core artifact on Simple and binary size. The JSONPath engine
  and the document representation are a large standing subsystem for a capability
  that is opt-in for clients rather than a baseline compatibility expectation, so
  the tenet order favors keeping the default binary lean.
- Not a hard non-goal. JSON is permitted to land later, and the natural shape if
  it does is an optional, separately-compiled capability (a feature-gated module)
  so the default artifact stays lean while the clients that need document caching
  can opt in. This pairs with the vector decision (ADR-0029): both are heavy,
  opt-in surfaces best carried behind a module seam if a module seam is adopted.
- A future JSON design issue, if demand justifies it, owns the document
  representation, the JSONPath subset, the reply-shape conformance bar under the
  differential suite, and whether it ships in-core or feature-gated.

## Rejected Alternatives

- **Build native JSON.* in the core binary now.** Rejected on Simple and on the
  single-binary promise: a JSONPath engine plus a document type is a subsystem-
  scale commitment for an opt-in surface, taken on ahead of demonstrated demand.
  Per the tenet order it loses to keeping the core artifact lean and certifiable.
- **Declare JSON a permanent non-goal.** Rejected on Compatible and honesty: there
  is genuine demand for document caching, and a permanent exclusion overstates the
  call. The evidence supports "not in the core binary now," not "never."
- **Ship JSON as a thin string convenience (store and return raw JSON text with
  no path engine).** Rejected on Compatible: a path-less veneer that answered
  JSON.GET only at the document root would diverge from the observable contract
  clients depend on (path queries, typed sub-document updates), which violates the
  published-compatibility tenet more than honestly documenting JSON as
  unsupported. If JSON is built, it is built to the real contract.

## Consequences

- The compatibility map records JSON as permitted-but-not-built for the core
  binary, in the same category as vector sets (ADR-0029): heavy, opt-in, and a
  candidate for a future module seam, distinct from the hard non-goals that can
  never be built. NON_GOALS cites this ADR.
- No parser, document representation, or JSONPath engine is committed now; a
  future JSON design issue owns all of it and its conformance bar.
- If a module seam is later adopted for heavy types, JSON and vector sets are its
  first two candidates, decided together rather than piecemeal.
