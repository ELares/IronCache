# Research: Foundational CRDT literature and OR-Set tombstone GC for the Redis type surface

> Part of the IronCache prior-art research corpus (`docs/research/`). This
> document is DESCRIPTIVE: it surveys primary CRDT literature, with
> version-pinned claims tracked in [`../prior-art/claims.yaml`](../prior-art/claims.yaml).
> Prescriptive IronCache decisions live in the design and decision issues, not
> here.
>
> Area: `area:datastructures` / `area:memory` / `area:replication`. Claims
> gathered from the primary CRDT literature, then re-checked by an adversarial
> verifier before being trusted.

## Why this exists

The active-active research (#79) grounds its borrow/adapt/reject on exactly two
product implementations: Redis Enterprise CRDB and KeyDB. That is enough to pick
a direction, but not enough to bound the per-key metadata budget and the OR-Set
tombstone garbage-collection path that #79 explicitly leaves open. This document
is the literature layer UNDER #79, not a duplicate of it: it surveys the primary
CRDT papers (the state-based versus op-based versus delta-state tradeoff, the
optimized tombstone-free OR-Set, and Hybrid Logical Clocks), and maps the FULL
Redis type surface onto conflict-free types, including the three types CRDB does
not specify (sorted sets, hashes, lists). Streams are excluded here, per the
Streams non-goal.

The four CRDT claims this document leans on are introduced in this PR:
[crdt-state-vs-op-based-cvrdt-cmrdt],
[delta-state-crdt-delta-mutators-reduce-shipped-state],
[optimized-or-set-tombstone-free-version-vectors], and
[hybrid-logical-clocks-bounded-skew-epsilon]. They sit beneath the product
claims #79 already cites: [redis-crdb-datatype-mapping],
[redis-enterprise-crdb-counter-59bit], [redis-enterprise-crdb-orset],
[redis-enterprise-crdb-string-lww], [keydb-active-replica-lww], and
[keydb-multimaster-lww-undefined].

## The foundational tradeoff: state-based vs op-based vs delta-state

The primary distinction in the literature is between convergent (state-based,
CvRDT) and commutative (op-based, CmRDT) replicated data types. A CvRDT ships its
whole state and merges with a join over a semilattice, so it converges as long as
every update eventually reaches every replica, tolerating duplication and
reordering. A CmRDT ships operations and requires a reliable causal-order
broadcast so each operation is delivered exactly once in causal order
[crdt-state-vs-op-based-cvrdt-cmrdt]. The two models are equivalent in
expressive power, but they trade a fat merge against a strict delivery channel.

Delta-state CRDTs are the synthesis that matters for a memory-minimal cache.
Instead of shipping the entire state (CvRDT) or relying on an exactly-once causal
channel (CmRDT), a delta-state CRDT ships small delta-mutators, join-irreducible
fragments of the state that are merged with the same lattice join, so it keeps
the robust dissemination of state-based replication while shrinking the bytes on
the wire toward the op-based cost [delta-state-crdt-delta-mutators-reduce-shipped-state].
This is the lever #79 calls out as the open question of whether a delta merge
path keeps active-active memory inside the minimal-memory tenet.

| Model | Ships | Channel requirement | Cost shape | Stance for IronCache |
| --- | --- | --- | --- | --- |
| State-based (CvRDT) | whole state | eventual, idempotent, reorder-safe | fat sync, simple channel | **adapt** as the convergence backbone; the lattice join is the merge contract |
| Op-based (CmRDT) | operations | reliable, exactly-once, causal | thin wire, strict channel | **reject** as the transport requirement; exactly-once causal broadcast is heavy for a cache fleet |
| Delta-state | delta-mutators | eventual, idempotent, reorder-safe | thin wire, simple channel | **borrow** as the wire format; keeps the CvRDT channel while approaching CmRDT bytes [delta-state-crdt-delta-mutators-reduce-shipped-state] |

The stance: build the merge contract on the state-based lattice join so the
replication channel can stay the eventual, idempotent, reorder-tolerant one the
async default (ADR-0026) already provides, and ship deltas rather than full state
to keep the bytes-on-wire and the per-key resident metadata bounded
[crdt-state-vs-op-based-cvrdt-cmrdt][delta-state-crdt-delta-mutators-reduce-shipped-state].
This avoids importing the exactly-once causal-broadcast requirement an op-based
design would force onto every active-active keyspace.

## OR-Set tombstone GC: the gating question

#79 leaves OR-Set tombstone garbage collection open, and KeyDB's delete gap is
the cautionary case: its multi-master leaves tombstone gaps on deletes and
leaves identical-key conflicts undefined [keydb-multimaster-lww-undefined]. CRDB
follows the add-wins observed-remove rule, where a remove cancels only the adds
it has already observed [redis-enterprise-crdb-orset], but the naive OR-Set keeps
a unique tag per add and a tombstone per remove, so a churning set grows its
metadata without bound.

The literature answer is the optimized, tombstone-free OR-Set built on version
vectors (a dotted causal context). Instead of retaining a tombstone per removed
element, each replica keeps a compact causal context, a version vector of the
dots it has seen, and a remove is encoded as the absence of a dot under that
context rather than as a retained tombstone, so removed elements leave no
permanent per-element residue [optimized-or-set-tombstone-free-version-vectors].
This is the mechanism that closes the KeyDB delete gap
[keydb-multimaster-lww-undefined] by construction: a delete is causally
well-defined, and the metadata that survives a remove is bounded by the causal
context (which scales with replica count), not by the number of elements ever
removed.

The consequence for the metadata budget #79 leaves open: the dominant residual
cost in an OR-Set is the causal context, which scales with the number of
active-active members, not with the churn history
[optimized-or-set-tombstone-free-version-vectors]. That makes a member cap, not a
tombstone-reaper cadence, the primary lever, and it is exactly the cap and the
bytes-per-key-at-N-members the active-active experiment (#79) must quantify.

## Wall-clock LWW is the wrong arbiter; HLC bounds the anomaly

CRDB's one clock-skew-sensitive surface is string LWW keyed on a stored OS
wall-clock timestamp [redis-enterprise-crdb-string-lww], and KeyDB applies
timestamp LWW uniformly to all types [keydb-active-replica-lww], so a backward
clock step can lose a strictly-later write. The literature replacement is the
Hybrid Logical Clock, which combines a physical-time component with a logical
counter so it stays close to wall-clock time (within a bounded epsilon of
physical time) while never moving backward, preserving the happens-before order
for causally related events [hybrid-logical-clocks-bounded-skew-epsilon]. An HLC
timestamp is a fixed-width stamp (a physical part plus a small logical counter),
so adopting it as the LWW-register arbiter swaps the skew-sensitive OS timestamp
for a causally correct one at one stamp per register, with the skew anomaly
bounded by the epsilon rather than unbounded under a clock step. The exact
acceptable epsilon ceiling is the open question #79 records and the active-active
experiment measures.

## Mapping the full Redis type surface (borrow / adapt / reject)

CRDB specifies strings, counters, and sets [redis-crdb-datatype-mapping]; it does
not specify sorted sets, hashes, or lists. This is where the literature, rather
than the product, has to do the mapping. Streams are excluded per the Streams
non-goal.

| Redis type | Conflict-free type | Stance | Per-key metadata shape | Notes |
| --- | --- | --- | --- | --- |
| String / bitfield | LWW-register, HLC-stamped | **adapt** from CRDB | one HLC stamp [hybrid-logical-clocks-bounded-skew-epsilon] | CRDB is correct in shape but uses OS wall-clock [redis-enterprise-crdb-string-lww]; swap the arbiter for HLC |
| Counter (INCR/INCRBY) | op-based PN-counter | **borrow** from CRDB | one accumulator slot per member | concurrent increments all survive; CRDB caps at 59 bits to avoid concurrent overflow [redis-enterprise-crdb-counter-59bit] |
| Set | optimized OR-Set | **borrow** the rule, **adapt** the encoding | causal context (version vector) [optimized-or-set-tombstone-free-version-vectors] | add-wins observed-remove [redis-enterprise-crdb-orset], but tombstone-free so removes leave no per-element residue |
| Hash | map of per-field LWW-registers under one OR-Set of field names | **adapt** (CRDB does not specify) | one OR-Set context for the field set, one HLC stamp per present field | field presence is an OR-Set; each field value is an HLC LWW-register, composing the two mapped types above |
| Sorted set | OR-Set of members, score as a per-member counter or HLC register | **adapt** (CRDB does not specify) | OR-Set context plus one numeric merge slot per member | CRDB's sorted-set behavior is product-internal; the principled mapping is member-presence as OR-Set with the score merged as a counter (sum) or an HLC register (last-write), a decision the design must pin |
| List | the hard case: no order-preserving CRDT is metadata-cheap | **reject** a general ordered-list CRDT in active-active | would need per-element position identifiers | sequence CRDTs carry per-element position metadata that does not fit the minimal-memory tenet; active-active lists should be a documented restriction, not a silent best-effort merge |

The list row is the load-bearing rejection. A correct concurrent ordered-list
CRDT requires dense position identifiers per element, whose metadata cost does not
fit a memory-minimal cache, so the principled answer is to NOT offer a general
active-active list and to document that restriction rather than fall back to the
blanket LWW that #79 rejects [keydb-multimaster-lww-undefined].

## Implications for IronCache

- Build the active-active merge contract on the state-based lattice join so it
  rides the existing eventual, idempotent, reorder-tolerant async channel
  (ADR-0026), and ship delta-mutators rather than full state to keep bytes
  bounded [crdt-state-vs-op-based-cvrdt-cmrdt][delta-state-crdt-delta-mutators-reduce-shipped-state].
  Reject importing an exactly-once causal-broadcast requirement.
- Use the optimized tombstone-free OR-Set as the set foundation so deletes are
  causally well-defined and removed elements leave no permanent residue, closing
  the KeyDB delete gap [optimized-or-set-tombstone-free-version-vectors]
  [keydb-multimaster-lww-undefined]. The residual metadata is the causal
  context, which scales with member count, making a member cap the primary
  bounding lever.
- Replace OS wall-clock LWW with HLC-stamped LWW-registers for strings and hash
  fields, so the only conflict arbiter is causally correct and the skew anomaly
  is bounded by a small epsilon rather than unbounded under a clock step
  [hybrid-logical-clocks-bounded-skew-epsilon][redis-enterprise-crdb-string-lww].
- Map the three types CRDB does not specify: hashes as an OR-Set of field names
  over per-field HLC registers, sorted sets as an OR-Set of members with the
  score merged as a counter or an HLC register, and lists as a documented
  active-active restriction rather than a metadata-heavy sequence CRDT.
- Feed all of this into #79: the per-key metadata shapes above are the inputs to
  the bytes-per-key-at-N-members measurement and the member cap, and the HLC
  epsilon ceiling is the open number the active-active experiment must settle.

## Open questions

- What is the measured resident bytes-per-key for the HLC LWW-register, the
  PN-counter, and the tombstone-free OR-Set at N active-active members, and what
  member cap keeps each memory-competitive? (Quantified in #79's experiment.)
- Is the delta-state merge path's amplification (delta size versus the update it
  encodes) small enough at our member counts to stay inside the minimal-memory
  tenet, or does delta-interval bookkeeping erode the saving
  [delta-state-crdt-delta-mutators-reduce-shipped-state]?
- What HLC epsilon ceiling is acceptable for a cache, and how is clock skew
  surfaced or bounded operationally
  [hybrid-logical-clocks-bounded-skew-epsilon]?
- For sorted sets, is the score better merged as a PN-counter (sum) or an HLC
  LWW-register (last-write), and does the choice depend on the workload?
- Can the OR-Set causal context be safely truncated once all members have
  observed a prefix of the history, and what membership-stability assumption does
  that truncation require
  [optimized-or-set-tombstone-free-version-vectors]?
- Is a documented no-active-active-list restriction acceptable to the
  compatibility tenet, or does any client workload force a bounded list merge?

## Research papers and primary sources

- **A comprehensive study of Convergent and Commutative Replicated Data Types**
  (Shapiro, Preguica, Baquero, Zawirski; INRIA RR-7506, 2011). Establishes the
  CvRDT (state-based) and CmRDT (op-based) models, their equivalence, and the
  semilattice/join foundation. [source](https://hal.inria.fr/inria-00555588/document)
  Relevance: the foundational tradeoff this survey is built on
  [crdt-state-vs-op-based-cvrdt-cmrdt].
- **Delta State Replicated Data Types** (Almeida, Shoker, Baquero; JPDC 2018).
  Introduces delta-mutators and delta-interval dissemination that ship
  join-irreducible state fragments instead of full state. [source](https://arxiv.org/abs/1603.01529)
  Relevance: the wire-format lever for staying inside the minimal-memory tenet
  [delta-state-crdt-delta-mutators-reduce-shipped-state].
- **An Optimized Conflict-free Replicated Set** (Bieniusa, Zawirski, Preguica,
  Shapiro, Baquero, Balegas, Duarte; 2012). The tombstone-free OR-Set over a
  causal context / version vectors. [source](https://arxiv.org/abs/1210.3368)
  Relevance: the OR-Set tombstone-GC mechanism #79 leaves open
  [optimized-or-set-tombstone-free-version-vectors].
- **Logical Physical Clocks and Consistent Snapshots in Globally Distributed
  Databases** (Kulkarni, Demirbas, Madappa, Avva, Leone; 2014). Defines the
  Hybrid Logical Clock and its bounded divergence from physical time. [source](https://cse.buffalo.edu/tech-reports/2014-04.pdf)
  Relevance: the principled replacement for OS wall-clock LWW
  [hybrid-logical-clocks-bounded-skew-epsilon].
- **Conflict-free Replicated Data Types** (Shapiro, Preguica, Baquero,
  Zawirski; SSS 2011). The conference paper introducing strong eventual
  consistency. [source](https://hal.inria.fr/inria-00609399/document)
  Relevance: the correctness frame for active-active replacing KeyDB's
  admittedly undefined last-write-wins [keydb-multimaster-lww-undefined].

## References

- Consumers: #79 (active-active design; this is the literature layer under it),
  the active-active experiment doc, and #68 (clustering parent). Vision: #1.
- docs/research/keydb.md, docs/research/distributed-clustering.md,
  docs/research/redis-replication-cluster.md (the product-level prior art this
  literature sits beneath).
- Claims (resolved via docs/prior-art/claims.yaml): new in this PR
  [crdt-state-vs-op-based-cvrdt-cmrdt],
  [delta-state-crdt-delta-mutators-reduce-shipped-state],
  [optimized-or-set-tombstone-free-version-vectors],
  [hybrid-logical-clocks-bounded-skew-epsilon]; existing
  [redis-crdb-datatype-mapping], [redis-enterprise-crdb-counter-59bit],
  [redis-enterprise-crdb-orset], [redis-enterprise-crdb-string-lww],
  [keydb-active-replica-lww], [keydb-multimaster-lww-undefined].
