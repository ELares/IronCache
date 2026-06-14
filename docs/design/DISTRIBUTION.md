# Design: Single-node to multi-node distribution umbrella (the four-stage staircase)

Issue: #68. Decisions: ADR-0011 (single-node-first, slot-ready storage layout),
ADR-0012 (headline scale-out targets), ADR-0025 (16384 as the one dual-purpose
shard/migration partition), ADR-0026 (async primary/replica default, best-effort
not CP, WAIT as a durability floor). Related: #69 (single-node-first roadmap),
#70/CLUSTER_CONTRACT.md (client wire contract), #71/#72 (internal partition
count, resolved in ADR-0025), #73/CONTROL_PLANE.md (Raft slot map), #74/
MEMBERSHIP.md (SWIM + Lifeguard), #75 (atomic slot migration), #76 (replication
model, resolved in ADR-0026), #77 (offset-based async replication), #78
(opt-in per-shard Raft tier), #79/#163 (opt-in active-active CRDT), #80
(internal placement study), #147/REPLICA_READ.md (replica-read contract), #148
(rebalancing policy), #149/NODE_LIFECYCLE.md (bootstrap and node lifecycle), #1
(vision).

## Goal and scope

IronCache earns distribution in stages and ships a contract at each stage that
the next stage does not have to break. This is the connective umbrella for #68:
it states the four-stage staircase and the contract preserved at each step, and
it gives one consolidated decision table that links every child mechanism issue
to the tenet that broke its tie and the ADR that froze it. It owns no bytes and
no algorithm. Every distribution decision is already made: the cluster-wide
choices live in ADR-0011, ADR-0012, ADR-0025, and ADR-0026, and the mechanisms
live in the sibling specs (CLUSTER_CONTRACT, CONTROL_PLANE, MEMBERSHIP,
REPLICA_READ, NODE_LIFECYCLE) and their issues. This doc re-decides nothing and
introduces no new external claim; it only composes what those documents commit,
so a reader sees the whole path and the boundary each child owns. Conflicts
resolve Compatible over a cleaner internal scheme, the headline tenet conflict
#68 inherits to every child.

## Design

### The four-stage staircase, and the contract each stage preserves

- **Stage 1, single node.** A single process owns all 16384 slots and serves
  the Redis client contract from one box (ADR-0011, #69). The storage layout is
  already slot-ready: each slot is a per-slot dictionary that doubles as the
  per-core execution unit and the future migration unit (ADR-0025), so reaching
  later stages never re-partitions the live keyspace. *Contract preserved:* the
  full 16384-slot client view (CLUSTER_CONTRACT, #70) and the per-slot shard
  boundary that every later stage assigns and moves.

- **Stage 2, async read replica for HA.** A first replica joins by the same
  seed/MEET path NODE_LIFECYCLE owns (#149), attaches as an async replica of the
  primary under ADR-0026, and serves reads through the READONLY/READWRITE
  contract REPLICA_READ owns (#147). HA is in-binary: failover decisions live in
  the same Raft control plane, not an external Sentinel quorum
  [redis-sentinel-quorum-vs-majority]. *Contract preserved:* Stage 1's slot view
  is unchanged (one node still owns every slot); the replica only adds a read
  leg and a bounded-staleness signal, and replicas reject writes
  (ADR-0026 replica-read-only).

- **Stage 3, sharded cluster.** Slots are spread across nodes. The authoritative
  slot->node map lives in the in-binary Raft control plane (CONTROL_PLANE, #73);
  data-plane liveness comes from SWIM + Lifeguard (MEMBERSHIP, #74) under the
  SWIM-proposes / Raft-commits handshake both specs state; ownership moves by
  atomic per-slot migration (#75) under MOVED/ASK redirection (#70), governed by
  the rebalancing policy in #148 and the lifecycle transitions in #149.
  *Contract preserved:* the client still sees exactly 16384 slots and routes by
  CRC16/XMODEM with MOVED/ASK (#70); the wire view never regresses on a
  transient SWIM suspicion because it renders from the Raft-committed map.

- **Stage 4, optional active-active.** An opt-in, off-by-default multi-writer
  mode using principled CRDT/HLC merge rather than blanket last-writer-wins
  (#79, #163). It layers on the Stage 3 cluster and the Stage 2 async baseline;
  it does not change the single-writer-per-slot default. The opt-in strongly-
  consistent per-shard Raft tier (#78) is the parallel opt-in on the consistency
  axis, also layered, never a tax on the default write path (ADR-0026).
  *Contract preserved:* the async, best-effort-not-CP default of ADR-0026 stays
  the default; active-active and per-shard Raft are additive modes, so an
  operator who enables neither sees Stage 3 behavior unchanged.

### Consolidated decision table (re-decided nowhere; links the children)

Each row is owned by the named ADR and child issue/spec; this table only gathers
them and records the tenet that broke each tie. No row is decided here.

| Axis | Decision (owner) | Rejected alternative | Tenet that broke the tie |
| --- | --- | --- | --- |
| Sequencing | Single-node-first on a slot-ready layout (ADR-0011, #69) | Build the full cluster up front | Simple, time-to-usable over front-loaded distributed complexity |
| Scale targets | Several-thousand-node targets, aim 4096 (ADR-0012, #146) | Inherit Redis's ~1000-node ceiling | Scalable, quantified like Efficient rather than a slogan |
| Partition unit | 16384 as the one dual-purpose shard/migration unit (ADR-0025, #72; folds in #71) | A separately configurable internal count P | Simple, Efficient: one unit, no slot-to-partition fold, no double-build |
| Client routing | Slot model with smart-client MOVED/ASK (CLUSTER_CONTRACT, #70) | Mandatory proxy / pure consistent-hash ring | Compatible over a cleaner internal scheme |
| Control plane | In-binary Raft for slot map + epoch + promotion (CONTROL_PLANE, #73) | Gossip-only epoch bumps; external Sentinel | Correct (linearizable map), Simple (one binary, no sidecar) |
| Membership | SWIM + non-optional Lifeguard data plane (MEMBERSHIP, #74) | Reuse Raft heartbeats / Redis full-mesh gossip | Scalable: O(1) per-node cost, flat past the gossip ceiling |
| Slot migration | Atomic per-slot handoff, no write freeze (#75; ADR-0025 model) | Bulk move with brief unavailability | Compatible, Efficient: slot keeps serving through the move |
| Replication default | Async primary/replica, WAIT as a floor (ADR-0026, #76; #77) | Sync/quorum or Dynamo sloppy quorum by default | Compatible then Efficient: no quorum tax on the hot path |
| Replica reads | READONLY/READWRITE with bounded staleness surfaced (REPLICA_READ, #147) | Silently proxy replica reads to the primary | Compatible, and honest: the chosen staleness is legible |
| Node lifecycle | Seed/MEET join, learner->voter->slot-owner staging (NODE_LIFECYCLE, #149) | Direct join / gossip-only admission | Correct: SWIM proposes, only a Raft commit changes the roster |
| Rebalancing | Policy-driven moves, hot-slot trigger, drain on decommission (#148) | Manual-only or unconditional rebalancing | Scalable within the ADR-0012 rebalance budget |
| Strong consistency | Opt-in per-shard Raft tier, off by default (#78) | CP on every write by default | Compatible, Efficient: CP is a mode, not a default tax |
| Multi-writer | Opt-in active-active CRDT/HLC, off by default (#79, #163) | Single-writer only, or blanket LWW | Compatible default kept; active-active earned, not blanket |
| Internal placement | Slot ownership over a studied placement scheme (#80 study feeds #71/#73) | Ad hoc placement | Scalable, with the ring-vs-slot study informing, not overriding, #70 |
| Partial availability | Graceful per-key unavailable default; strict full-coverage opt-in (#68 charter) | All-or-nothing strict mode as default [redis-cluster-require-full-coverage-default] [redis-cluster-allow-reads-when-down-default] | Available: healthy slots keep serving; strict is opt-in |

### What this umbrella does not own

- It states no slot-hashing, redirection, projection, or Pub/Sub routing detail;
  those are CLUSTER_CONTRACT (#70). It states no consensus or epoch semantics;
  those are CONTROL_PLANE (#73). It states no failure-detector timing; that is
  MEMBERSHIP (#74). It states no read-state machine or staleness bound; that is
  REPLICA_READ (#147). It states no join/drain transition; that is
  NODE_LIFECYCLE (#149). Migration mechanics are #75, rebalancing policy is #148,
  the opt-in tiers are #78 and #79/#163, and the placement study is #80.

## Open questions

- None are opened or resolved here. Every open distribution decision is owned by
  a child: Raft voter count and learner delivery and the migration-vs-topology
  split (#73), SWIM suspicion multipliers and trait visibility (#74), the
  default staleness bound and per-request ReadPolicy hint (#147), learner-
  admission and lag-gate thresholds and seed-bootstrap-when-all-down (#149), the
  default replica count at the sharded stage and the CRDT type order (#68
  charter, dispatched to #79/#163). This umbrella tracks that those questions
  live in those documents; it adds no new one.

## Acceptance and test hooks

- The four-stage path is stated with the contract each stage preserves, and each
  stage's contract is the acceptance bar of its owning spec (#70, #147, #73/#74,
  #79); this doc adds no behavioral test, only the cross-document map.
- Every row of the decision table resolves to an accepted ADR or an open child
  issue/spec, and names a rejected alternative and a tenet; a doc-lint that the
  ADR and issue references in this file exist (ADR INDEX, issue-index) guards the
  links.
- No claim id in this file is new: every bracket reuses an id already in
  claims.yaml, enforced by check-prior-art-claims.sh, consistent with the
  re-decides-nothing intent.
- The staircase ordering matches the single-node-first sequencing (ADR-0011) and
  the scale-out targets (ADR-0012): Stage 1 ships before Stage 3, and Stage 4
  layers on Stage 3 without changing the ADR-0026 default.

## References

- ADR-0011, ADR-0012, ADR-0025, ADR-0026; issues #68, #69, #70, #71, #72, #73,
  #74, #75, #76, #77, #78, #79, #80, #147, #148, #149, #163, #1; specs
  CLUSTER_CONTRACT.md (#70), CONTROL_PLANE.md (#73), MEMBERSHIP.md (#74),
  REPLICA_READ.md (#147), NODE_LIFECYCLE.md (#149).
- Claims: [redis-sentinel-quorum-vs-majority],
  [redis-cluster-require-full-coverage-default],
  [redis-cluster-allow-reads-when-down-default].
