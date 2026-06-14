# Design: Sorted-set large representation (ordered index plus member map)

Issue: #134. Decisions: ADR-0018 (encoding thresholds), ADR-0005 (per-shard
unsynchronized map), ADR-0009 (behavioral equivalence). Related: #113 (small
listpack zset), #35 (index), #40 (OBJECT ENCODING name), #136
(large-collection-bakeoff), #128 (zset command semantics), #8 (harness).

## Goal and scope

A sorted set that outgrows the small listpack container (ADR-0018) needs a
structure that serves both an ordered range/rank query and an O(1) member point
lookup, the two access patterns the zset command set demands. This spec fixes the
two-structure shape, the sync invariant that keeps them consistent on one core,
the ordering contracts behind ZRANGEBYSCORE and ZRANGEBYLEX, and which knobs are
harness parameters rather than fixed numbers. Scope is the representation above
the listpack threshold only. The promotion thresholds are ADR-0018/#37, the small
container is #113, and the final choice of ordered-index structure is the #136
bake-off; this spec sets the provisional baseline and the contract every
candidate must satisfy, not the winner.

## Design

### Two structures, one value

- The large zset is a dual structure mirroring Redis: an ordered index keyed by
  (score, member) for range and rank, plus a parallel hashmap from member to
  score for O(1) ZSCORE and ZADD score-update [redis-zset-skiplist-plus-ht]. The
  member bytes are stored once and shared between both views, so a member is not
  duplicated per structure [redis-zset-skiplist-plus-ht]. The whole value lives
  in one kvobj on one core (ADR-0005), so neither structure takes a lock.
- The provisional ordered index is a skiplist [redis-zset-skiplist-plus-ht]. It
  is provisional because the #136 bake-off evaluates a cache-conscious B-tree and
  an ART against it on throughput-per-core and bytes-per-element; a B-tree packs
  many keys per cache line versus the skiplist's one element per tower node
  [skiplist-vs-btree-cache], and ART keeps keys ordered at a low per-key byte
  cost [art-adaptive-radix-tree-icde13]. This spec commits to the trait the index
  sits behind, not the structure that wins.

### The sync invariant

- Every member appears in exactly one of two states: present in BOTH the ordered
  index and the member map with the same score, or present in NEITHER. There is
  no transient single-structure state observable to a command, because all
  mutation runs inline on the owning core (ADR-0005) with no yield point inside a
  zset write. ZADD that updates a score is a remove-then-reinsert in the ordered
  index plus an in-place score rewrite in the map; ZREM deletes from both. A
  property test asserts the two views agree on membership and score after every
  operation (the sync invariant).

### Ordering: ZRANGEBYSCORE vs ZRANGEBYLEX

- The ordered index is sorted by (score, member): primarily ascending score,
  ties broken by member byte order, the ordering Redis defines for a skiplist
  zset [redis-zset-skiplist-plus-ht]. ZRANGEBYSCORE, ZRANK, and ZRANGE by index
  walk this order directly, forward or reversed.
- ZRANGEBYLEX assumes all members share one score and returns a purely
  lexicographic member range. Because the index already breaks score ties by
  member bytes, the equal-score run is contiguous and already in member order, so
  ZRANGEBYLEX is a sub-scan of that run with no second index. Its result is
  defined only when scores are equal, matching the oracle (ADR-0009, #128).

### Level and fanout as harness parameters

- For the skiplist baseline the max level and the level-promotion probability are
  harness parameters (#8), not fixed here; for a B-tree or ART candidate the
  analogous knob is node fanout. They are swept in the #136 bake-off because the
  right value depends on IronCache's value layout and the thread-per-core engine,
  where cross-paper numbers do not transfer.

## Open questions

- The final ordered-index structure (skiplist vs cache-conscious B-tree vs ART),
  decided by #136 on throughput-per-core and bytes-per-element.
- Whether the member map is a distinct per-zset hashbrown table or folds into the
  ordered index nodes once the structure is chosen (#136), and the score-update
  path's exact cost under each.
- Whether a maintained rank/size annotation is worth its bytes for O(log n) ZRANK
  versus a counted walk, tuned on the harness (#8).

## Acceptance and test hooks

- After any ZADD/ZREM/ZINCRBY the ordered index and the member map agree on
  membership and score for every member (the sync invariant, property test).
- ZSCORE is a single member-map lookup with no ordered-index walk; ZRANGEBYSCORE
  and ZRANK walk the (score, member) order and match the oracle (#97/#98).
- ZRANGEBYLEX over an equal-score set returns the lexicographic member range and
  matches the oracle; mixed scores follow the oracle's defined behavior
  (#97/#98, #128).
- OBJECT ENCODING reports `skiplist` for the large zset regardless of the chosen
  internal structure [valkey-assert-encoding-vocab] (ADR-0009, name map in #40).

## References

- ADR-0005, ADR-0009, ADR-0018; issues #113, #35, #40, #136, #128, #37, #8,
  #97, #98.
- Claims: [redis-zset-skiplist-plus-ht], [redis-zset-max-listpack-entries-128],
  [skiplist-vs-btree-cache], [art-adaptive-radix-tree-icde13],
  [valkey-assert-encoding-vocab].
