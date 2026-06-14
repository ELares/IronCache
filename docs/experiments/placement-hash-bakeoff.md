# Experiment: Post-ketama consistent-hashing bake-off for internal replica-set placement

Issue: #80. Provisional decision: none is frozen yet; #71 (the internal shard
map) owns the placement-algorithm interface and has not yet committed one. This
doc records the tentative recommendation #80 reaches (MementoHash for placement,
HRW as the fallback) and the churn bake-off that must confirm it before #71
freezes its interface. It does not pre-commit the algorithm.

## Provisional posture (not yet pinned)

The client-facing scheme is fixed and out of scope: the wire slot space is the
16384-slot Redis Cluster space [redis-cluster-16384-slots], kept verbatim in
CLUSTER_CONTRACT.md (#70). Internal replica-set placement, deciding which
physical nodes hold each replica set, is distinct from that and is the only thing
this experiment ranks. Decoupling the public slot contract from internal
placement is what lets the placement algorithm be swapped without a client
migration; that boundary is the one #71 and #72 define.

#80 reaches a tentative recommendation, not a decision:

- **Reject ketama as the internal algorithm**, keeping it only as a measured
  baseline. Its virtual-node ring distributes unevenly and reshuffles more keys
  per topology change than the minimal-disruption families [twemproxy-ketama].
- **MementoHash is the provisional pick** for placement: it handles arbitrary
  node removal and full weighting at near-constant lookup cost without fixing a
  maximum cluster capacity up front [mementohash-vs-anchorhash].
- **HRW (rendezvous) is the fallback** if replica-set semantics or weighting
  prove cleaner to reason about at our node counts: it gives natural top-k
  replica-set selection and per-node weighting, at O(n) per lookup (O(log n) with
  a skeleton tree) [rendezvous-hrw-origin].
- **Jump hash is rejected for the placement layer** despite its ideal churn
  floor, because real clusters remove interior nodes and run heterogeneous
  instance sizes, which jump hash structurally cannot express
  [jump-hash-limitation], even though it moves exactly the minimal fraction of
  keys on a scale-up and needs zero per-node state [jump-hash-constant].

This is the M2 confirmation #80 reserves: the ranking above is a citation
comparison across mismatched home corpora until one harness measures churn,
lookup cost, and weighting fidelity for all candidates under one model.

## Why this is harness-blocked

The decision rule needs the measured fraction of slots remapped per topology
change, the measured lookup latency and resident memory per algorithm, and the
measured weighting fidelity for mixed instance sizes. None of those exist yet
without three things:

- Working implementations of each candidate (ketama, jump, HRW, AnchorHash,
  MementoHash) behind one common placement interface, the one #71 will freeze,
  so only the algorithm varies.
- A topology driver that applies single-add, single-remove, and rolling-replace
  changes at fixed node counts and diffs the slot-to-node assignment before and
  after, so churn is counted, not argued.
- The benchmark and memory-model harness of ADR-0016 (lookup throughput and
  resident bytes under one accounting model); the harness is #8.

Until one harness runs every candidate under one topology driver and one
accounting model, any ranking is a citation comparison across the candidates'
own papers.

## Experiment to run

Corpus and topology:

- The fixed 16384-slot space [redis-cluster-16384-slots] as the keyspace being
  placed, so churn is measured in slots remapped, which is exactly the migration
  unit #75 will move.
- Node counts swept at n = 3, 6, 12, 48, the counts #80 names, so the result
  covers small clusters through the mid-size target and exposes any O(n) lookup
  cost in HRW where it would actually bite [rendezvous-hrw-origin].
- Three topology-change patterns applied at each node count: a single node add, a
  single node remove (including an interior node, the case jump hash cannot
  express [jump-hash-limitation]), and a rolling-replace (remove one node and add
  one back, the realistic node-recycle pattern).
- A heterogeneous-weight topology (mixed instance sizes) layered on top, so
  weighting fidelity is measured, not assumed.

Fixed parameters, held identical across all algorithms:

- The 16384-slot assignment and the shared-nothing shard layout (ADR-0002), so
  the only variable is the placement algorithm.
- The replica-set size k (number of distinct nodes per slot), so the k-distinct
  property is tested at one fixed k.
- Hardware, thread pinning, and the ADR-0016 measurement methodology (open-loop,
  coordinated-omission-corrected) for the lookup-latency numbers.
- The seed and order of topology changes, so every algorithm sees the same
  sequence of adds and removes.

Varied parameters:

- Algorithm under test: ketama (baseline only [twemproxy-ketama]), jump
  [jump-hash-constant], HRW [rendezvous-hrw-origin], AnchorHash, and MementoHash
  [mementohash-vs-anchorhash].
- Node count n in {3, 6, 12, 48}.
- Topology-change pattern in {single-add, single-remove, rolling-replace}.
- Node-weight distribution in {uniform, mixed instance sizes}.

Measured:

- Churn: the fraction of slots remapped per topology change, per algorithm, per
  pattern, at each n. The minimal-disruption families should approach the jump
  hash churn floor [jump-hash-constant] while still expressing interior removal
  [jump-hash-limitation], and ketama should be measurably worse
  [twemproxy-ketama].
- Replica-set quality: whether MementoHash yields k distinct nodes as cleanly as
  HRW top-k [rendezvous-hrw-origin][mementohash-vs-anchorhash], or whether the
  candidate must be wrapped to guarantee k distinct nodes.
- Lookup latency and resident memory per algorithm at each n, to test whether
  HRW's O(n) per-lookup cost [rendezvous-hrw-origin] matters below the ~50-node
  range #80 flags.
- Weighting fidelity: the gap between configured weights and realized load under
  the mixed-instance-size topology.

Decision rule:

- Recommend MementoHash for #71 to freeze if, at n in {3, 6, 12, 48}, it matches
  the minimal-disruption churn of the best non-ketama candidate AND yields k
  distinct replica nodes cleanly (or with a thin, cheap wrapper) AND its lookup
  latency and memory are competitive AND its weighting fidelity holds under mixed
  instance sizes [mementohash-vs-anchorhash].
- Fall back to HRW if replica-set semantics or weighting prove cleaner to reason
  about at our node counts and its O(n) lookup cost is immaterial below ~50 nodes
  [rendezvous-hrw-origin].
- Either way, ketama is the rejected baseline kept only for the measured churn
  comparison [twemproxy-ketama], and jump hash stays rejected for placement
  because it cannot remove interior nodes or express weights
  [jump-hash-limitation] regardless of its churn floor [jump-hash-constant].

## What would change the decision

- HRW's top-k replica selection turns out so much cleaner than wrapping
  MementoHash for k distinct nodes that the O(n) cost is worth paying at our node
  counts [rendezvous-hrw-origin].
- AnchorHash matches MementoHash on churn and lookup but its fixed-max-capacity
  requirement is acceptable, making it competitive on a metric MementoHash wins
  on by not fixing capacity [mementohash-vs-anchorhash].
- MementoHash's weighting fidelity under mixed instance sizes is materially worse
  than HRW's weighted scores [rendezvous-hrw-origin], forcing the fallback.
- The measured churn gap between the minimal-disruption families and ketama is
  small enough at our node counts that ketama's even-distribution weakness, not
  its churn, becomes the deciding factor [twemproxy-ketama].

## References

- ADR-0002 (shared-nothing thread-per-core); ADR-0016 (headline metrics and
  methodology, #7); CLUSTER_CONTRACT.md (#70, the fixed 16384-slot client
  contract this experiment leaves untouched).
- Issues: #80 (this experiment); #71 (internal shard map, the consumer that
  freezes the placement interface once this bake-off confirms it); #72 (the
  internal-vs-client partition boundary); #75 (slot migration, which moves the
  slots whose churn is measured here); #68 (clustering parent); #8 (benchmark and
  memory harness); #1 (vision).
- Claims (resolved via docs/prior-art/claims.yaml): [jump-hash-constant],
  [jump-hash-limitation], [rendezvous-hrw-origin], [mementohash-vs-anchorhash],
  [redis-cluster-16384-slots], [twemproxy-ketama].
