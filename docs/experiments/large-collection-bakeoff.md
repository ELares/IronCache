# Experiment: Large-collection structure bake-off (zset ordered index and list deque)

Issue: #136. Provisional baselines: skiplist plus parallel hashtable for the zset
ordered index, and a quicklist-style chunked deque for lists. The list baseline is
the one recorded as "adapt" in the redis-datastructures research doc; the zset
index baseline is only described in that doc's narrative and has no pinned stance,
which is exactly the open choice this experiment exists to resolve.

## Provisional baselines and where they come from

These layouts are NOT decided here. Their status in the research corpus differs,
and the difference is the point of this experiment:

- The large list is a quicklist-style doubly linked list of compact listpack
  chunks [redis-list-max-listpack-size-neg2]. This is the only one of the two with
  a recorded stance: the quicklist row of the Mechanisms table in
  `docs/research/redis-datastructures.md` marks it "adapt", and that row's own
  rationale is what flags a more cache-friendly chunk index (a small B-tree or
  rope of chunks) as the alternative to evaluate.
- The large zset is, in Redis, a skiplist as the ordered index plus a parallel
  member-to-score hashtable [redis-zset-skiplist-plus-ht]. This layout appears in
  the research doc only as a passing factual description in its Summary narrative
  ("sorted sets ... convert to ... skiplist+hashtable"); it has NO row and NO
  borrow/adapt stance in the Mechanisms table. So the large-zset index is an
  undecided structural choice, not an inherited decision.
- ADR-0018 (Issue #37) pins the small-to-large promotion thresholds
  (zset-max-listpack-entries 128 / value 64 [redis-zset-max-listpack-entries-128];
  list node 8 KB [redis-list-max-listpack-size-neg2]). This experiment concerns
  only the structure used ABOVE those thresholds, not the thresholds themselves.

The B-tree/ART-versus-skiplist comparison for the zset index below is this
experiment's own synthesis, not something pre-flagged for zsets in the research
doc (that doc raises B-tree/ART only for a future streams/rax ordered map, and the
chunk-index alternative only for the list). This doc records the procedure that
lets the M2 zset (#134) and list (#135) designs pick the large-collection
structure on evidence rather than defaulting to Redis C internals (the stance #35
warns against).

## Why this is harness-blocked

The decision rule is the #7 headline metrics: throughput-per-core and
bytes-per-element (ADR-0016). Those are measurable only on the reproducible
benchmark and memory-model harness (#8), with candidate structures actually
implemented behind a common ordered-index trait. Until the harness can drive a
real implementation of each candidate, any choice between skiplist, B-tree, and
ART rests on cross-paper extrapolation: ART claims cache-friendly adaptive nodes
for main-memory indexes [art-adaptive-radix-tree-icde13], and cache-conscious
B-trees claim a locality edge over pointer-chasing skiplists
[skiplist-vs-btree-cache], but neither was measured in IronCache's
thread-per-core engine on IronCache's value layout. The numbers do not transfer;
they must be run.

## Experiment to run

Corpus and workload:

- zset ordered index: build collections of varied member counts spanning well
  above the listpack promotion threshold up to large (the exact sizes are the
  varied parameter, set in the harness manifest). Member keys drawn from a
  Zipfian distribution; scores both clustered and uniformly spread.
- Operation mix: ZADD (insert and score-update), ZRANGEBYSCORE / ZRANGE (range
  scan), ZRANK / ZSCORE (point), ZREM, mixed read-heavy and write-heavy phases,
  driven through the pinned harness generator from #8 / #7.
- list deque: head/tail push and pop (LPUSH/RPUSH/LPOP/RPOP), LRANGE scans, and
  LINSERT/LSET interior mutation, across varied element counts and element sizes.

Candidates, all implemented behind one ordered-index trait so only the structure
varies:

- zset: skiplist plus parallel hashtable (provisional baseline); a
  cache-conscious B-tree index plus hashtable; an ART index plus hashtable.
- list: the quicklist-style chunked deque (provisional baseline) versus a
  chunk-index variant (small B-tree or rope of chunks), the alternative named in
  the research doc's quicklist row.

Fixed parameters: value codec, allocator and accounting (ADR-0006), shard count
and pinning, hardware, hit ratio at which memory is sampled, and pipelining depth,
all held identical across candidates per #7 methodology.

Varied parameters: the index structure, collection size, and key/score
distribution.

Measured: throughput-per-core for each operation class; resident
bytes-per-element at the fixed hit ratio (index overhead plus the parallel map);
and p99/p999 tail latency for range scans and for mutation under continuous
insert.

Decision rule: pick, per type, the structure that wins throughput-per-core on the
dominant operation mix without regressing bytes-per-element beyond a stated margin
and without a p999 tail regression. If the provisional baseline is not beaten on
both headline metrics, keep it (it carries the wire-compatibility and simplicity
advantage of #35).

## What would change the decision

- A candidate (B-tree or ART) that beats skiplist on throughput-per-core AND
  bytes-per-element for the dominant zset mix flips the zset index away from the
  skiplist baseline.
- A range-scan or tail-latency regression from a non-baseline candidate, even
  with a per-element memory win, keeps the baseline.
- Evidence that an indexed chunk structure dominates list cost would move #135
  from a flat quicklist to an indexed-chunk deque; otherwise the provisional
  quicklist stands.
- Any result here that contradicts ADR-0018's threshold assumptions reopens #37,
  not this issue.

## References

- Issue #136 (this research item); EPIC #1 (vision); overlaps #37.
- Feeds Issue #134 (zset large representation) and Issue #135 (list representation).
- ADR-0016 / Issue #7 (headline metrics: throughput-per-core, bytes-per-element).
- Issue #8 (reproducible benchmark and memory-model harness).
- ADR-0018 / Issue #37 (encoding-conversion thresholds, already pinned).
- Issue #35 (do not default to Redis C internals without evidence).
- `docs/research/redis-datastructures.md` (quicklist "adapt" stance; zset
  skiplist+ht described in narrative only; ART noted for a future streams index).
- Claims: [redis-zset-skiplist-plus-ht], [art-adaptive-radix-tree-icde13],
  [skiplist-vs-btree-cache], [redis-zset-max-listpack-entries-128],
  [redis-list-max-listpack-size-neg2].