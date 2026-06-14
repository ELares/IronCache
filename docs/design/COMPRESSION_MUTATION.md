# Design: Compression interaction with mutating commands and hot-key cost

Issue: #56. Decisions: ADR-0015 (zstd-low default codec, lz4/none policy),
ADR-0016 (throughput-per-core is the headline metric the disable-on-mutation
policy protects), ADR-0007 (maxmemory ceiling that compression feeds). Related:
#52 (COMPRESSION.md, the stored framing and GET decode this builds on), #64
(HYBRIDLOG_ENGINE.md, the in-place/RCU engine whose stable-record-size assumption
this honors), #131 (BITMAPS.md, the bit mutators that defer the compression rule
here), #112 (string encodings), #111 (kvobj layout and in-place resize), #34
(narrow-waist RMW verb), #55 (per-prefix dictionaries), #92 (off-path
compression decision and eviction-fairness skew), #57 (size/entropy gate), #86
(the demoted-keys metric).

## Goal and scope

Compression is transparent to GET and SET, but it is not transparent to in-place
mutation. Redis stores values verbatim and ships no transparent in-memory value
compression [redis-no-transparent-value-compression], so it never faces this
problem; IronCache does, because COMPRESSION.md (#52) stores some value bytes
compressed and APPEND, SETRANGE, SETBIT, BITFIELD, and the INCR family turn a
stored value into a read-modify-write target. If that value is compressed, a
naive mutation pays a decompress-recompress tax on every call, and on a hot key
that tax lands directly on the ADR-0016 throughput-per-core budget. This spec
defines, command by command, whether a compressed value is mutated in place or
has compression disabled on first mutation, and it specifies an adaptive hot-key
policy that demotes a frequently-mutated key off compression without operator
tuning.

This spec owns the mutation contract and the hot-key policy only. It does NOT
re-decide the codec (ADR-0015), the stored framing or GET decode path (#52 owns
those; this reuses the header it defines), the bit-command semantics (#131 owns
SETBIT/BITFIELD growth and overflow and explicitly defers the compression rule
to this doc), or the engine region machinery (#64 owns the mutable/read-only/
stable log and the RCU mechanics this triggers). Scope is the string type and its
mutators; list/hash/set field compression is out of scope, as it is in #52.

## Design

### Why recompression must never sit on the in-place path

The hybrid-log engine (#64) gets its peak throughput from in-place updates of the
mutable region with no allocation, and the full RESP API is implemented over a
narrow storage API whose atomic read-modify-write primitive assumes a stable,
in-place-updatable record [garnet-narrow-waist-api]; in that engine the mutable
region is updated in place while the read-only region falls back to an RCU to the
log tail on update [faster-hybridlog-three-regions]. Recompression changes the
encoded length of the value, so a recompress-on-mutate design would change the
record size on almost every write and force a read-copy-update into a fresh record
at the log tail. That defeats the exact property the engine optimizes for: it is
an IronCache design observation that recompression on the hot path breaks in-place
update, turning every mutation of a compressed hot key into an allocation plus a
log append plus a codec round trip. The contract here is therefore absolute: no
mutating command recompresses on the in-place fast path. A mutation either runs
in place on raw bytes (compression already off for the key) or it disables
compression first and then runs in place on raw bytes. Recompression, when it
happens at all, is an off-hot-path event, never a per-mutation cost.

### The compressed/raw flag lives in the existing record header

COMPRESSION.md (#52) already frames a compressed value with a codec id, a dict
id, an uncompressed length, and an incompressible flag. The compressed/raw state
for a key is exactly the codec-id field of that header: codec=none (or the
incompressible flag set) means the stored bytes are raw and a mutation runs in
place with no decode; any other codec id means the value is compressed and a
mutation must first materialize raw bytes and flip the codec id to none. No new
record field is added and the narrow-waist record format (#64) is not widened: a
compression demotion is a one-time rewrite that stores the value raw and sets
codec=none, after which the key is indistinguishable from a value that was never
compressed. The flag is per-key, carried in the value's own header, so the engine
needs no side table to know whether a key is mutation-protected.

### Per-command mutation table

| Command | Read or mutate | Behavior on a compressed value | Recompress on the hot path |
| --- | --- | --- | --- |
| GET / GETRANGE / STRLEN | read | decode per the #52 GET path (single branch); no state change | never |
| BITCOUNT / BITPOS / GETBIT | read | decode once per the #52 GET path to read bits; value unchanged, compression stays on | never |
| APPEND | mutate (grows) | first APPEND decodes, stores raw, sets codec=none (disable-on-first-grow-mutation); subsequent APPENDs are in-place raw | never |
| SETRANGE | mutate (may grow) | first SETRANGE disables compression as APPEND does, then writes the range in place on raw bytes | never |
| SETBIT | mutate (may grow) | first SETBIT disables compression, then sets the bit in place on raw bytes (#131 growth/zero-fill rules) | never |
| BITFIELD SET / INCRBY | mutate (may grow) | first field write disables compression, then runs the BITFIELD ops in place on raw bytes (#131 widths/overflow) | never |
| BITOP (destination) | mutate (overwrites dest) | the op writes the destination by-policy raw at the #131 longest-operand length and sets codec=none; the op never compresses its own output, regardless of destination size | never |
| INCR / INCRBY / INCRBYFLOAT / DECR / DECRBY | mutate (integer-encoded) | never compressed in the first place: integer-encoded and tiny values are below the #57 size gate, so SET never frames them compressed and the op runs in place | never |

The rule reduces to two cases. First, value-growing or in-place byte mutators
(APPEND, SETRANGE, SETBIT, BITFIELD writes, BITOP destinations) use
disable-on-first-grow-mutation: the first such mutation rematerializes raw bytes,
flips codec=none, and from then on the key mutates in place at zero codec cost.
A BITOP destination is a special case of the same rule: the op writes its output
raw and sets codec=none by policy, so a large destination is left uncompressed
because the op never compresses its own output, not because of any size gate.
Second, integer mutators (the INCR family) and any value below the #57 size gate
are never compressed to begin with, because the size gate keeps small values raw
and compression on a tiny integer is pure overhead; for these the value is
already raw and the mutation is a plain in-place op. Read-only bit commands
(BITCOUNT, BITPOS, GETBIT) do not mutate, so they leave compression on and just
ride the #52 GET decode.

### Coordination with #64 RCU and in-place resize

Disabling compression and growing a value are two distinct size events, both
routed through the engine the same way COMPRESSION.md and BITMAPS.md already
assume. The first mutation of a compressed key does a one-time read-copy-update:
it decodes to raw (uncompressed length is in the header, so this is one
allocation and one codec call, the same shape as a GET decode), writes the raw
bytes, and updates the kvobj pointer in place under single-owner ownership
(OBJECT_LAYOUT in-place resize, #111). After that demotion the key is raw, so a
later APPEND or SETBIT that does not change the record size updates the mutable
region in place with no allocation, and only a size-growing mutation degrades to
RCU into a new record exactly as #64 specifies for any variable-length write.
Compression thus adds RCU pressure only once per key (the demotion), not per
mutation; this is the whole point of disable-on-first-grow-mutation over
recompress-on-RCU. The recompress-on-RCU alternative is kept only as an explicit
opt-in for memory-bound deployments (below), never the default.

### Adaptive hot-key demotion

Disable-on-first-grow-mutation already protects any key that is ever mutated by a
growing command. The adaptive policy covers the remaining risk: a value that is
mutated frequently but whose mutations do not (yet) trigger the disable rule, or
a deployment that opted into recompress-on-RCU and is now paying the tax on a hot
key. It is an IronCache design observation that mutation rate, not value size, is
the signal that should pull a key off compression: a large rarely-written value
is the ideal compression candidate, while a small frequently-written one is the
worst. The policy tracks a per-key mutation counter and demotes a key to raw
(codec=none) once its mutation rate crosses a demotion threshold, after which the
key behaves exactly as a disable-on-mutation demotion. To avoid an exact
per-key timestamp, the counter is a decaying value sampled on mutation: each
mutation increments it and the elapsed-time decay is applied at sample time, so a
burst raises it and a quiet window lets it fade, giving a mutations-per-second
estimate without a wall clock per key. The threshold is a server default that an
operator can tune but does not have to, and a demotion is sticky for the key
lifetime by default (a demoted key is not silently recompressed on a later
GET-only window; re-enabling compression for a cooled key is an open question
below). A counter (the demoted-keys gauge, exported via #86) records how many
keys the policy has demoted, so the cost of the policy is observable.

### Why disable beats recompress, with the codec speeds in view

The default is disable-on-mutation, not decompress-mutate-recompress, because the
access asymmetry compression relies on is inverted for a hot mutated key. zstd
decompression is near level-independent and fast (2.896 ratio at 1550 MB/s
decompress for zstd -1) [zstd-silesia-benchmark-l1], which is why the #52 GET
path can afford a decode; but compression is the slow direction (510 MB/s
compress at that level) [zstd-silesia-benchmark-l1], and a recompress-on-mutate
design pays that slow direction on every write plus an RCU. Even lz4_flex, the
faster policy codec, compresses far slower than it decompresses (about 1272 MiB/s
compress versus 4540 MiB/s decompress on 66 KB JSON) [lz4-flex-safe-vs-c], so no
codec choice makes per-mutation recompression cheap. Disabling compression
trades the memory on that one key (an append-heavy key loses its RAM win, which
is the documented cost in the per-command table) to keep the in-place path and
the throughput-per-core budget intact, which is the Compatible-first,
Efficient-second posture COMPRESSION.md already takes. The memory given up is
counted truthfully: maxmemory accounts the stored bytes [redis-maxmemory-accounting],
so a demoted key simply costs its raw size against the ceiling (ADR-0007), and
the eviction engine (#48/#50, fairness tracked in #92) sees the post-demotion
size.

## Open questions

- The exact demotion threshold: a fixed mutations-per-second cutoff versus the
  decay constant of the sampled counter, and how both are calibrated against the
  throughput-per-core budget on the hot-key bakeoff (compression-hotkey-mutation
  experiment).
- Whether a demoted key is ever re-enabled for compression after a long GET-only
  cold window, or whether demotion stays sticky for the key lifetime (the simpler
  default chosen here), decided on whether re-encode churn pays for itself in #92.
- Whether recompress-on-RCU stays a per-keyspace opt-in for memory-bound
  deployments, or is dropped entirely once the bakeoff shows its hot-key cost.
- Where the SETRANGE boundary sits between disable-on-mutation and a hole-fill
  that does not grow the value: whether a non-growing SETRANGE over a compressed
  value still disables compression (decode is required either way) or whether only
  growth triggers the flag.

## Acceptance and test hooks

- Per-command behavior is pinned for APPEND, SETRANGE, SETBIT, BITFIELD,
  INCR/INCRBY, and the read-only BITCOUNT/BITPOS: a test asserts the first
  growing mutation of a compressed key stores it raw with codec=none and leaves
  GET returning the same bytes, and that BITCOUNT/BITPOS leave compression on.
- No mutating command recompresses on the in-place fast path: a hot-path lint
  asserts the APPEND/SETBIT/BITFIELD write paths make no codec compress call, and
  the only codec call on a mutation is the one-time decode during demotion.
- INCR-family and below-gate values are never framed compressed: a test confirms
  SET of an integer-encoded or small value stores raw and INCR runs in place with
  no codec call.
- The compressed/raw flag is the #52 header codec id, not a new record field: a
  test confirms a demoted key is byte-for-byte indistinguishable from a never-
  compressed value and the narrow-waist record format (#64) is unchanged.
- The adaptive policy demotes a key once its sampled mutation rate crosses the
  threshold, the demotion is sticky by default, and the demoted-keys gauge (#86)
  increments; the hot-key APPEND-loop bakeoff (compression-hotkey-mutation
  experiment) shows throughput-per-core with disable-on-mutation on par with a
  never-compressed key and strictly above recompress-on-mutate.

## References

- ADR-0015, ADR-0016, ADR-0007; issues #56, #52, #64, #131, #112, #111, #34,
  #55, #92, #57, #86, #48, #50, #1; specs COMPRESSION.md, BITMAPS.md,
  HYBRIDLOG_ENGINE.md, OBJECT_LAYOUT.md, ENCODINGS.md, STORAGE_API.md,
  OBSERVABILITY.md.
- Claims: [redis-no-transparent-value-compression], [redis-maxmemory-accounting],
  [zstd-silesia-benchmark-l1], [lz4-flex-safe-vs-c], [garnet-narrow-waist-api],
  [faster-hybridlog-three-regions].
