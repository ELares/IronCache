# Design: List large representation (quicklist-equivalent chunked deque)

Issue: #135. Decisions: ADR-0018 (encoding thresholds), ADR-0005 (per-shard
unsynchronized map), ADR-0009 (behavioral equivalence). Related: #113 (small
listpack list chunk), #35 (index), #40 (OBJECT ENCODING name and ql_* fields),
#136 (large-collection-bakeoff), #128 (list command semantics), #52
(value compression), #8 (harness).

## Goal and scope

A list that outgrows a single small listpack chunk (ADR-0018) needs a structure
with O(1) head and tail operations and bounded per-node memory, the quicklist
contract. This spec fixes the chunked-deque shape, the node-size policy, the
traversal model for the interior commands, and how chunks split and merge, plus
the ql_nodes/ql_avg_node fields #40 must synthesize. Scope is the representation
above the listpack threshold; the small chunk is #113, the threshold is
ADR-0018/#37, and the flat-deque-versus-indexed-chunk choice is the #136
bake-off. This spec sets the provisional flat baseline and the contract.

## Design

### Chunked deque of listpack nodes

- The large list is a deque of compact listpack chunks, the quicklist shape
  Redis uses [redis-list-max-listpack-size-neg2]. Each chunk is one contiguous
  listpack with the ~6-byte header (total bytes plus element count) and a 1-byte
  terminator [redis-listpack-header-6-bytes], holding a run of elements in order;
  the chunks are linked head-to-tail. The whole list is one value on one core
  (ADR-0005), so no chunk link is synchronized.
- The provisional structure is a flat doubly linked deque of chunks (a plain
  prev/next chain). It is provisional because #136 evaluates an indexed chunk
  structure (a small B-tree or rope of chunks) for faster positional access; this
  spec commits to the chunk-deque trait, not to flat-versus-indexed.

### Node sizing (~8 KB)

- A chunk's byte budget maps to list-max-listpack-size -2, the Redis default that
  caps each node's listpack at 8 KB rather than an element count
  [redis-list-max-listpack-size-neg2]. A push that would exceed the budget starts
  a new chunk; the cap keeps each node cache-resident and bounds the cost of an
  interior memmove within one chunk. IronCache stores only the listpack bytes per
  chunk, contrasting Redis's 32-byte quicklistNode struct (prev/next, listpack
  ptr, sz, count, and bitfields) [redis-quicklist-node-32-bytes]; interior-node
  LZF compression [redis-quicklist-node-32-bytes] is a design choice deferred to
  COMPRESSION.md (#52), not adopted here.

### Head/tail O(1) and interior traversal

- LPUSH/RPUSH/LPOP/RPOP touch only the head or tail chunk: an append or pop
  inside that chunk's listpack, allocating or freeing a chunk only at the budget
  boundary, so end operations are O(1) amortized.
- LINDEX/LRANGE/LSET/LINSERT walk the chunk chain accumulating element counts to
  locate the target chunk, then scan within it. Each chunk carries its element
  count in the listpack header [redis-listpack-header-6-bytes], so locating a
  chunk by index is a walk over chunk counts, not over every element; the flat
  baseline makes this O(number of chunks), which is the cost #136 weighs against
  an indexed variant. LSET rewrites one entry in place; LINSERT inserts into the
  target chunk's listpack with at most one tail memmove within that chunk.

### Chunk split and merge

- An insert that pushes a chunk past the ~8 KB budget
  [redis-list-max-listpack-size-neg2] splits it into two chunks at an element
  boundary near the midpoint. Deletions that leave two adjacent chunks jointly
  under the budget merge them, bounding chunk count and keeping ql_avg_node
  meaningful. The merge low-watermark (how empty before merging) is harness-tuned
  (#8), a churn-versus-resident-bytes trade, not fixed here.

### ql_nodes and ql_avg_node derivation

- ql_nodes is the live chunk count; ql_avg_node is total element count divided by
  ql_nodes. Both are computed from the deque IronCache actually holds and
  surfaced through DEBUG OBJECT for `quicklist` keys [redis-quicklist-node-32-bytes],
  the synthesis #40 wires in. They reflect IronCache chunking, not a Redis node
  layout, and are a pure function of the current representation (#40).

## Open questions

- Flat doubly linked chunk chain vs an indexed chunk structure (small B-tree or
  rope) for positional access, decided by #136 on throughput-per-core and
  bytes-per-element.
- The chunk split point (strict midpoint vs fill-the-tail) and the merge
  low-watermark, tuned on the harness (#8).
- Whether a chunk reuses the #113 `pack` exactly or a length-only variant sized
  to the ~8 KB cap [redis-list-max-listpack-size-neg2].

## Acceptance and test hooks

- LPUSH/RPUSH/LPOP/RPOP touch only the end chunk and allocate or free a chunk
  only at the byte budget (O(1) amortized, structural test).
- An interior LINSERT performs at most one tail memmove within the target chunk
  and never rewrites another chunk; no chunk exceeds the ~8 KB budget after split
  [redis-list-max-listpack-size-neg2] (property test).
- DEBUG OBJECT reports ql_nodes equal to the live chunk count and a consistent
  ql_avg_node [redis-quicklist-node-32-bytes]; OBJECT ENCODING reports
  `quicklist` [valkey-assert-encoding-vocab] (ADR-0009, name map #40).
- LINDEX/LRANGE/LSET match the oracle across chunk boundaries (#97/#98, #128).

## References

- ADR-0005, ADR-0009, ADR-0018; issues #113, #35, #40, #136, #128, #52, #37,
  #8, #97, #98.
- Claims: [redis-list-max-listpack-size-neg2], [redis-listpack-header-6-bytes],
  [redis-quicklist-node-32-bytes], [valkey-assert-encoding-vocab].
