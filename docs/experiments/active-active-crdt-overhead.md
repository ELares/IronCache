# Experiment: Opt-in active-active CRDT overhead (per-type CRDB plus HLC)

Issue: #79. Provisional decision: ADR-0026 pins active-active as an opt-in,
separate consistency contract layered on the async default, never the default
model. The CRDT/HLC foundation is grounded in docs/research/crdt-foundations.md
(#163), the literature layer under this issue. This doc records the bake-off that
quantifies the per-key metadata cost, the member cap, and the HLC epsilon that
#79 leaves open. It does not re-decide the async default.

## Provisional decision (already pinned)

ADR-0026 (Accepted, issue #76) makes the default single-node, minimal-memory,
async non-CRDT replication, with strong-consistency and multi-master modes opt-in
and layered on that baseline rather than changing it. The default carries zero
per-key CRDT metadata; CRDT cost is paid only by keyspaces that enable
active-active. Compatibility and efficiency rank above scalability here: the
default stays a lean Redis-compatible cache, with active-active as a documented,
separate consistency contract.

The foundation is pinned by the literature survey (#163,
docs/research/crdt-foundations.md) and the two product implementations #79
studies:

- **Adapt the CRDB per-type mapping**: LWW-register strings, op-based PN-counter
  counters, add-wins observed-remove OR-Set sets [redis-crdb-datatype-mapping]
  [redis-enterprise-crdb-counter-59bit] [redis-enterprise-crdb-orset]. This is
  correct by construction and strictly better than blanket LWW.
- **Reject KeyDB blanket LWW**: applying LWW to a counter silently drops
  concurrent increments, and its delete semantics leave tombstone gaps
  [keydb-multimaster-lww-undefined] [keydb-active-replica-lww].
- **Reject OS wall-clock LWW as the arbiter** [redis-enterprise-crdb-string-lww]
  in favor of Hybrid Logical Clocks, so causality is preserved and skew
  anomalies are bounded by a small epsilon
  [hybrid-logical-clocks-bounded-skew-epsilon].
- **Build the merge on the state-based lattice join and ship delta-mutators**,
  not full state, so the active-active channel stays the async one and the bytes
  on the wire stay bounded
  [crdt-state-vs-op-based-cvrdt-cmrdt]
  [delta-state-crdt-delta-mutators-reduce-shipped-state].
- **Garbage-collect OR-Set tombstones via the optimized tombstone-free version-
  vector encoding**, so removes leave no per-element residue and the KeyDB delete
  gap is closed by construction
  [optimized-or-set-tombstone-free-version-vectors]
  [keydb-multimaster-lww-undefined].

What is NOT pinned, and is exactly what this experiment measures: the
bytes-per-key overhead at N members, the member cap that keeps it memory-
competitive, the acceptable HLC epsilon ceiling, and whether multi-master must be
a fundamentally separate merge engine or a per-keyspace trait that coexists with
the hot single-node path.

## Why this is harness-blocked

The decision rule needs the measured resident bytes-per-key for each CRDT type at
N members, the convergence behavior under skew, and the metadata growth under
churn. That requires three things that do not exist yet:

- Working CRDT merge implementations for the LWW-register, PN-counter, and
  tombstone-free OR-Set behind a per-keyspace merge trait, selectable per
  keyspace so the default hot path carries no CRDT metadata.
- The benchmark and memory-model harness of ADR-0016 (resident bytes under one
  accounting model); the harness is #8.
- A multi-master test driver and the #99 Jepsen/Elle async suite with clock-skew
  faults (the HLC-sensitive path JEPSEN_PLAN.md calls out), so the HLC epsilon is
  measured under injected skew, not assumed.

Until the harness measures each CRDT type's resident bytes at N members under one
accounting model and the #99 skew faults exercise the HLC path, the per-key cost
and the epsilon are citations from the literature
[optimized-or-set-tombstone-free-version-vectors]
[hybrid-logical-clocks-bounded-skew-epsilon], not verdicts.

## Experiment to run

Corpus and workload:

- A per-type corpus exercising each mapped CRDT: HLC-stamped LWW-register strings
  and hash fields, PN-counter counters, and tombstone-free OR-Set sets and
  sorted-set members, the mapping grounded in #163.
- A churn workload that adds and removes set/hash members repeatedly, so the
  OR-Set causal context, not a tombstone pile, is what survives a remove
  [optimized-or-set-tombstone-free-version-vectors].
- A concurrent-increment workload across members, so the PN-counter's
  one-accumulator-slot-per-member growth is measured and concurrent INCRs are
  confirmed to all survive [redis-enterprise-crdb-counter-59bit], unlike blanket
  LWW [keydb-active-replica-lww].
- A clock-skew schedule (NTP step and monotonic offset, from the #99 fault
  catalog) so the HLC epsilon is exercised on the LWW path.

Fixed parameters, held identical across runs:

- The allocator and accounting (ADR-0006), shard count and pinning, and the
  ADR-0016 measurement methodology, so bytes-per-key is measured with all CRDT
  metadata enabled, not with empty reserved fields.
- The async non-CRDT default as the zero-CRDT-metadata baseline (ADR-0026), so
  the per-key cost is reported as the delta over the lean default.
- The HLC stamp width (physical part plus logical counter), so the LWW-register
  cost is one stamp per register regardless of member count
  [hybrid-logical-clocks-bounded-skew-epsilon].
- The seed and order of operations and skew injections.

Varied parameters:

- CRDT type under test: HLC LWW-register, PN-counter, tombstone-free OR-Set
  (and the OR-Set + per-member numeric merge that composes a sorted set, per
  #163).
- Member count N, swept across the active-active fleet sizes a geo deployment
  would use, since the PN-counter accumulator slots and the OR-Set causal context
  both scale with N [optimized-or-set-tombstone-free-version-vectors].
- Merge transport: delta-mutators versus full-state ship, to confirm the delta
  path's byte saving [delta-state-crdt-delta-mutators-reduce-shipped-state].
- Clock-skew magnitude, swept around a typical NTP bound (low tens of
  milliseconds), to find the epsilon at which staleness becomes unacceptable.
- Engine shape: a separate per-keyspace merge engine versus a per-keyspace merge
  trait threaded beside the default path, to test whether the CRDT path taxes the
  hot single-node path.

Measured:

- Resident bytes-per-key for the LWW-register, PN-counter, and OR-Set at each N,
  reported as the delta over the zero-CRDT async default (ADR-0026), the headline
  number #79 asks for.
- The member cap at which each type's bytes-per-key stops being memory-
  competitive, so the cap is a measured value, not a guess.
- OR-Set metadata after heavy churn: confirm the surviving cost is the causal
  context (scaling with N), not a tombstone pile (scaling with removes)
  [optimized-or-set-tombstone-free-version-vectors], closing the KeyDB delete gap
  [keydb-multimaster-lww-undefined].
- Convergence correctness and the observed staleness window under each skew
  magnitude, to fix the acceptable HLC epsilon ceiling
  [hybrid-logical-clocks-bounded-skew-epsilon], checked by the #99 async suite.
- The hot-path cost (if any) of the per-keyspace merge trait versus a separate
  engine, to settle whether multi-master must be a fundamentally separate engine.

Decision rule:

- Commit the per-type CRDB-plus-HLC mapping for the opt-in active-active mode if,
  at a member cap that keeps every type memory-competitive, the per-key overhead
  is an acceptable delta over the zero-CRDT default AND the OR-Set leaves no
  per-element residue after churn AND an HLC epsilon on the order of a typical
  NTP bound bounds the staleness anomaly AND the merge path can be a per-keyspace
  trait that does not tax the default hot path.
- Reject blanket LWW unconditionally: it drops concurrent increments and leaks
  delete tombstones [keydb-multimaster-lww-undefined], a correctness bug no
  latency or memory saving redeems.
- If the OR-Set causal context or the PN-counter accumulator bytes grow past the
  memory-competitive cap at the target member count, lower the member cap or move
  the CRDT merge to a fundamentally separate engine, keeping the default hot path
  CRDT-free (ADR-0026).
- For lists, follow #163: do not offer a general active-active list (its
  sequence-CRDT position metadata does not fit the minimal-memory tenet);
  document the restriction rather than fall back to blanket LWW.

## What would change the decision

- The measured bytes-per-key for the OR-Set causal context and the PN-counter
  accumulators at the target member count lands inside the memory-competitive
  band, confirming the per-type mapping is shippable as opt-in.
- The delta-mutator transport's byte saving over full-state ship
  [delta-state-crdt-delta-mutators-reduce-shipped-state] is large enough that the
  active-active wire cost stays inside the minimal-memory tenet at N members.
- A clock-skew sweep shows an HLC epsilon on the order of a typical NTP bound
  keeps the staleness anomaly acceptable for a cache
  [hybrid-logical-clocks-bounded-skew-epsilon], settling the open epsilon.
- Conversely, if threading the CRDT merge trait beside the default path measurably
  taxes the zero-CRDT hot path, multi-master is committed as a fundamentally
  separate per-keyspace engine instead, preserving the default (ADR-0026).

## References

- ADR-0026 (async default; active-active is opt-in, never the default; issue
  #76); ADR-0006 (allocator and accounting); ADR-0016 (headline metrics and
  methodology, #7).
- docs/research/crdt-foundations.md (#163, the literature layer this experiment
  rests on); docs/design/CONTROL_PLANE.md (#73, the committed control plane the
  active-active members register through); docs/design/JEPSEN_PLAN.md (#99, the
  async suite whose clock-skew faults exercise the HLC path).
- Issues: #79 (this experiment); #163 (the CRDT literature foundation); #76 (the
  async default it layers on); #99 (the consistency suite exercising the HLC
  path); #12 (geo/multi-master parent); #68 (clustering parent); #8 (benchmark
  and memory harness); #1 (vision).
- Claims (resolved via docs/prior-art/claims.yaml): new in this PR
  [crdt-state-vs-op-based-cvrdt-cmrdt],
  [delta-state-crdt-delta-mutators-reduce-shipped-state],
  [optimized-or-set-tombstone-free-version-vectors],
  [hybrid-logical-clocks-bounded-skew-epsilon]; existing
  [redis-crdb-datatype-mapping], [redis-enterprise-crdb-counter-59bit],
  [redis-enterprise-crdb-orset], [redis-enterprise-crdb-string-lww],
  [keydb-active-replica-lww], [keydb-multimaster-lww-undefined].
