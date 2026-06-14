# Design: Bitmap and BITFIELD semantics over the string type

Issue: #131 (merged from the command-surface and data-types-encodings lenses).
Decisions: ADR-0009 (behavioral equivalence via OBJECT ENCODING). Related: #112
(scalar string encodings), #111 (object layout), #56 (compression vs in-place
mutation), #40 (OBJECT ENCODING mapping), #34 (RMW storage verb), #97
(differential oracle), #98 (model tests), #1 (vision EPIC).

## Goal and scope

Bitmaps are not a distinct type in Redis: they are the string value addressed at
bit granularity, so TYPE returns string and OBJECT ENCODING is a string encoding
[redis-bitmap-is-string-type]. This spec layers the bit commands over the string
backing specified in ENCODINGS.md (#112) and OBJECT_LAYOUT.md (#111): the
addressing and growth rules for SETBIT/GETBIT, the BYTE|BIT ranges for
BITCOUNT/BITPOS, the allocation/length rules for BITOP, the signed/unsigned
widths and overflow modes for BITFIELD, and the size ceiling. It does NOT
re-decide the compression interaction: #56 owns whether a compressed value is
rematerialized or has compression disabled before an in-place bit mutation, and
this spec REFERENCES that decision rather than restating it.

## Design

### Bitmaps are the string type

A bitmap is just a string value. SETBIT on a missing key creates a string;
GETBIT/BITCOUNT on a string read it as bits; TYPE returns string and OBJECT
ENCODING reports a string encoding (raw or int per #40/#112), never a bespoke
bitmap encoding [redis-bitmap-is-string-type]. There is no separate bitmap object
in the kvobj (#111); the bit commands are a view over the existing string bytes.

### SETBIT / GETBIT addressing and zero-fill growth

- Bit offset N selects byte `N >> 3`; within that byte the most significant bit
  is bit 0, so addressing is big-endian within each byte
  [redis-setbit-zerofill-growth-bigendian].
- SETBIT past the current end grows the string to the largest touched byte and
  zero-fills every intervening bit, so the value length becomes `(N >> 3) + 1`
  bytes and all previously unset bits read as 0
  [redis-setbit-zerofill-growth-bigendian]. GETBIT past the end returns 0 without
  growing.
- A grow may move the value out of the inline SSO slot (#112) into an out-of-line
  raw buffer; the kvobj pointer is updated in place under single-owner ownership
  (OBJECT_LAYOUT in-place resize, #111). SETBIT is an RMW verb (#34).

### BITCOUNT / BITPOS with BYTE|BIT ranges

- BITCOUNT counts set bits; BITPOS finds the first 0 or 1 bit. Both take an
  optional start/end range, interpreted as either byte indices (BYTE, the
  default) or bit indices (BIT), with negative indices counting from the end and
  the per-byte bit numbering matching SETBIT (bit 0 is the MSB of byte 0)
  [redis-bitcount-bitpos-byte-bit-range-7-0].
- BITPOS edge rules follow Redis: searching for a 1 in an all-0 string returns
  -1; searching for a 0 with no explicit end on an all-1 string returns the bit
  just past the end (the implicit trailing zero), whereas an explicit end bounds
  the search to the stored bits [redis-bitcount-bitpos-byte-bit-range-7-0]. These
  asymmetric cases are pinned to the oracle (#97).

### BITOP allocation and result length

- BITOP AND/OR/XOR/NOT writes a destination string whose length equals the
  LONGEST source operand; shorter operands are treated as zero-padded to that
  length for the operation [redis-bitop-result-length-longest-zeropad]. NOT takes
  exactly one source and inverts it to an equal-length result.
- The destination is allocated once at the known result length (a single
  out-of-line buffer if it exceeds the inline threshold, #112) and overwritten;
  an empty/missing source contributes all-zero bytes of the padded length.

### BITFIELD widths and overflow modes

- BITFIELD addresses signed `i<N>` and unsigned `u<N>` integer fields at a bit
  offset within the string, with the documented width limits (signed up to 64
  bits, unsigned up to 63 bits), and supports GET, SET, and INCRBY subcommands in
  one call over the same value [redis-bitfield-widths-overflow-modes]. A `#`-
  prefixed offset is in field-width units; a plain offset is in bits.
- INCRBY and SET honor an OVERFLOW mode that applies to subsequent write ops in
  the same command: WRAP (modular two-complement wraparound, the default), SAT
  (saturate to the type min/max), and FAIL (leave the field unchanged and return
  a null for that op) [redis-bitfield-widths-overflow-modes]. Reading or writing
  a field past the current end grows and zero-fills exactly as SETBIT does
  (above). BITFIELD is an RMW verb (#34) and runs as one atomic command over the
  value.

### Size ceiling

- A bitmap is bounded by the string ceiling: 512 MB per value via
  proto-max-bulk-len [bulk-string-max-512mb], so the maximum addressable bit
  offset is around 4.3 billion (512 MB is 2^32 bits). SETBIT or BITFIELD at an
  offset that would grow the value past the ceiling is rejected with the
  Redis-recognized error, pinned to the oracle (#97), rather than silently
  truncated.

### Compression interaction (referenced, not decided here)

- Bit commands mutate in place, so on a compressed value (#52) they would pay a
  decompress-recompress tax. The rule for whether the value is rematerialized or
  has compression disabled on first bit mutation is owned by #56 and is NOT
  re-decided here; this spec only records that SETBIT/BITFIELD/BITOP-destination
  are in-place mutators subject to that contract.

## Open questions

- Whether BITFIELD field reads/writes go through a borrowed `Read` view or a
  scratch copy when the field straddles the SSO-to-raw boundary mid-command
  (#111 borrow-lifetime contract), measured on the harness (#8).
- Whether very large BITOP results stream the operation per chunk or materialize
  both inputs first, decided on the #8 bench against the 512 MB ceiling.

## Acceptance and test hooks

- SETBIT at a high offset on a missing key grows the value to `(N>>3)+1` bytes,
  zero-fills the gap, and GETBIT reads the big-endian-within-byte position back;
  TYPE returns string and OBJECT ENCODING is a string encoding (#40, oracle #97).
- BITCOUNT and BITPOS over BYTE and BIT ranges, including negative indices and
  the all-0 / all-1 / no-explicit-end edge cases, match the oracle (#97).
- BITOP AND/OR/XOR/NOT and NOT produce a destination of the longest-operand
  length with shorter operands zero-padded (#97); a model test asserts the
  length rule across mixed-length inputs (#98).
- BITFIELD i<N>/u<N> GET/SET/INCRBY with OVERFLOW WRAP, SAT, and FAIL match the
  oracle, FAIL returns null and leaves the field unchanged, and a value grown by
  BITFIELD zero-fills like SETBIT (#97/#98).
- SETBIT/BITFIELD past the 512 MB ceiling is rejected with the Redis-recognized
  error, not truncated [bulk-string-max-512mb] (#97).

## References

- ADR-0009; issues #131, #112, #111, #56, #40, #34, #97, #98, #8, #1.
- Claims: [redis-bitmap-is-string-type],
  [redis-setbit-zerofill-growth-bigendian],
  [redis-bitcount-bitpos-byte-bit-range-7-0],
  [redis-bitop-result-length-longest-zeropad],
  [redis-bitfield-widths-overflow-modes], [bulk-string-max-512mb].
