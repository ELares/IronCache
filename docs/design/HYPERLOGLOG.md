# Design: HyperLogLog encoding (P=14 dense + sparse, PFADD/PFCOUNT/PFMERGE)

Issue: #115. Decisions: ADR-0009 (behavioral equivalence), ADR-0018 (encoding
thresholds). Related: #39 (decomposed parent), #35 (object dispatch), #40 (OBJECT
ENCODING mapping), #129 (DUMP/RESTORE serializer), #116 (SIMD kernels), #98
(model tests), #97 (differential oracle).

## Goal and scope

IronCache must accept the PFADD/PFCOUNT/PFMERGE payloads a Redis client sends,
return byte-identical DUMP/RESTORE blobs, and report the same OBJECT ENCODING
string. Compatibility outranks Efficiency and Simplicity here, so the on-wire and
on-disk byte layout IS the spec, not an implementation detail. A generic "store a
HashSet and a u64 count" shortcut is rejected: DUMP/RESTORE and replication ship
the raw encoding bytes, so it would break compat and OBJECT ENCODING would lie.
Scope: the dense and sparse byte layouts, the sparse-to-dense promotion, the
three PF commands, and the round-trip contract. Out of scope: the SIMD kernels
(#116) and the final encoding-string name (#40).

## Design

### Dense representation

- The HLL is a string value (HLL is a Redis string under the hood, which is why
  OBJECT ENCODING is a string, below). The dense body is 16384 registers of 6
  bits each at P=14, packed little-endian into a 12 KiB register block behind a
  16-byte header (magic, encoding byte, padding, cached-cardinality field), for a
  12304-byte dense object [redis-hll-p14-registers]. The 6-bit register at index
  i straddles byte boundaries; access is the exact shift/mask Redis uses so the
  packed bytes are identical. The standard error at this precision is about 0.81
  percent [redis-hll-p14-registers].

### Sparse representation

- A new or low-cardinality HLL starts sparse: a run-length opcode stream of three
  opcodes over the same 16384 logical registers. ZERO and XZERO encode runs of
  zero registers (XZERO covering the longer runs); VAL encodes a short run of
  registers that share one nonzero value. The exact per-opcode run maxima and
  byte widths are the Redis sparse wire geometry and are pinned to the Redis
  source by the implementation issue (#115); this spec fixes the opcode roles and
  the property that the stream reconstructs the same logical register vector the
  dense form holds, so a sparse HLL produced here round-trips through a real
  Redis.
- Promotion: the HLL stays sparse until the opcode stream would exceed
  hll-sparse-max-bytes (default ~3000 bytes), at which point it is converted to
  dense in place [redis-hll-sparse-max-bytes-3000]. A VAL whose value would
  exceed the sparse value cap also forces promotion. Promotion is one-way; dense
  never demotes back to sparse.

### PFADD / PFCOUNT / PFMERGE

- PFADD hashes each element, derives the register index and the leading-zero
  count, and updates the target register to the max of old and new. On a sparse
  HLL the update walks the opcode stream and rewrites the affected run, promoting
  to dense if the rewrite crosses the byte cap. PFADD reports whether any
  register changed, matching Redis.
- PFCOUNT builds the register histogram and applies the Redis estimator (the same
  bias-corrected raw estimate) so the returned cardinality matches Redis on a
  fixed corpus. The cached-cardinality header field is honored and invalidated
  on mutation exactly as Redis does, so repeated PFCOUNT is cheap.
- PFMERGE computes, per register, the max across all source HLLs (the dense
  register max-merge) into the destination, reading sparse sources through the
  same logical-register view. The merged result follows the same
  promotion rule and the destination encoding is whatever the merged size
  dictates.

### DUMP / RESTORE and OBJECT ENCODING

- Both the sparse and dense forms serialize through the one canonical DUMP/RESTORE
  blob format specified in KEYSPACE.md (#129): value type, version, payload, then
  the trailing version and CRC. Because the in-memory bytes already match Redis,
  DUMP is a copy of the raw encoding and RESTORE accepts a Redis-produced blob
  unchanged, validated against the differential oracle (#97).
- OBJECT ENCODING returns a string for an HLL (HLL is a string-typed value). The
  exact encoding-name token IronCache reports is owned by the OBJECT ENCODING /
  DEBUG OBJECT mapping in #40; this spec only fixes that the type is a string and
  the byte layout is the two forms above.

## Open questions

- Whether the sparse value cap and hll-sparse-max-bytes are exposed as live
  CONFIG knobs (#85) or pinned to the Redis defaults for strict round-trip.
- Whether PFCOUNT over multiple keys materializes a transient merged HLL or
  streams the per-register max without allocating, decided on the #116 bench.

## Acceptance and test hooks

- A sparse HLL written by IronCache and a dense HLL written by IronCache both
  DUMP to bytes that a pinned redis-server RESTOREs and PFCOUNTs identically, and
  the reverse direction round-trips too (#97).
- PFADD/PFCOUNT/PFMERGE match Redis cardinality estimates and merge results on a
  fixed corpus, including the dense register max-merge (#97).
- A model test drives PFADD past hll-sparse-max-bytes and asserts the sparse-to-
  dense promotion produces the same logical registers and stays dense (#98).
- OBJECT ENCODING reports a string-typed value consistent with #40.

## Implementation status

- DONE (dense): PFADD/PFCOUNT/PFMERGE over the real Redis dense byte layout,
  MurmurHash64A, and the Ertl estimator (PR-11, #241).
- DONE (sparse, #242): a fresh HLL is created SPARSE (18 bytes: header + one
  XZERO(16384), byte-identical to a redis-server fresh HLL); PFADD keeps it sparse and
  PROMOTES one-way to dense when a register would exceed the VAL cap of 32 or the stream
  would exceed hll-sparse-max-bytes [redis-hll-sparse-max-bytes-3000]; PFCOUNT/PFMERGE and
  the cross-shard coordinator read sparse sources through the same logical-register view, and
  PFMERGE writes the smallest valid result (sparse when it fits, else dense) via the shared
  `hll_from_regs` in both the single-shard and cross-shard paths, so the two agree exactly.
- Two deliberate, Redis-compatible divergences from the sketch above, documented in the
  `cmd_hll` module: (a) PFADD REBUILDS the sparse stream by a canonical re-encode of the
  logical registers (adjacent equal-value runs merged) rather than an in-place opcode
  rewrite; the output is a valid Redis sparse object, and the worst-case cost is the same
  O(sparse size) bounded by the promotion cap. (b) The cached-cardinality header field is
  ALWAYS left invalid and PFCOUNT always recomputes (avoiding a read-path write that would
  dirty a watched key); observably identical to Redis (same count).
- DEFERRED (#242): DUMP/RESTORE byte-interop against a pinned redis-server (needs the
  DUMP/RESTORE command plus the differential oracle #97) and the PFDEBUG / PFSELFTEST
  introspection verbs. The sparse bytes written here are already valid Redis sparse
  objects, so DUMP/RESTORE round-trips them without rework.

## References

- ADR-0009, ADR-0018; issues #39, #35, #40, #129, #116, #98, #97, #85.
- Claims: [redis-hll-p14-registers], [redis-hll-sparse-max-bytes-3000].
