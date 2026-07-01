<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Design: rebalance APPLY (the slot-moving driver)

Issue: #371 (the last piece: `CLUSTER REBALANCE APPLY`). Decisions: ADR-0002 (shared
-nothing thread-per-core), ADR-0003 (determinism / Env seam). Related:
CLUSTER_CONTRACT.md (the slot map + the migration state machine), CONTROL_PLANE.md
(the raft-governed config apply path), SLOT_KEY_ENUMERATION.md (the read side, landed:
`COUNTKEYSINSLOT` / `GETKEYSINSLOT`), `crates/ironcache-cluster/src/lib.rs`
(`rebalance_plan`, `set_migrating`, `migration_state`), `crates/ironcache-raft-net`
(the `RAFTMSG` cluster bus).

## Where #371 stands

`#371` asks for an operator-triggered, epoch-checked failover AND a planned rebalance
that can be dry-run and then APPLIED. Delivered so far:

- Failover: `CLUSTER FAILOVER` proposes a committed `PromoteReplica`, gated on the same
  in-sync check the auto path uses (#443).
- Rebalance PLAN / DRYRUN: `CLUSTER REBALANCE [DRYRUN]` + `SlotMap::rebalance_plan`
  (pure, conservation-preserving per-node target counts) (#444), surfaced in the console
  (#445 / #446).
- The resharding READ side: honest cross-shard `CLUSTER COUNTKEYSINSLOT` /
  `GETKEYSINSLOT` (SLOT_KEY_ENUMERATION.md, this milestone).

The remaining gap is the one `#444` deferred: APPLY must actually MOVE the slots, WITH
their data. That is this spec.

## The two unbuilt pieces

### 1. The move planner (pure, small, macOS-testable)

`rebalance_plan` returns per-node deltas (`{node_id, current_slots, target_slots}`), not
concrete moves. APPLY needs an ORDERED list of `{slot, src_node, dst_node}`. A pure
`SlotMap::rebalance_moves() -> Vec<SlotMove>` derives it:

1. From the committed map, list each node's owned slots and its `target_slots`.
2. DONORS are nodes over target; RECEIVERS are under target. Walk donors in a
   deterministic order (node index, then slot ascending) and assign each surplus slot to
   the neediest receiver until every node reaches its target.
3. Conservation holds by construction (the targets sum to the assigned total), so the
   plan neither creates nor drops a slot; it only relocates `sum(surplus)` slots.

O(slots + nodes), pure, no I/O, unit-testable against a skewed map (assert the resulting
moves level every owner and touch no already-balanced slot). This is a clean first slice
with no cross-node dependency and no consumer risk: it can also enrich the DRYRUN with the
concrete src to dst transfer summary (a follow-up, once the console parser is updated).

### 2. The cross-node key transfer (the large, load-bearing piece)

Moving slot S from node A to node B means B must END UP with every live key in S that A
holds. Today NO primitive ships a key between nodes: there is no `MIGRATE`, no `DUMP` /
`RESTORE` command, and replication ships a primary's WHOLE write stream to a REPLICA (same
keyspace), not one slot to a DIFFERENT primary.

#### The serialization-format decision

Two distinct needs are often conflated; this spec separates them:

- **Redis-compatible `DUMP` / `RESTORE`** (issue #129, and #242 for the HLL case): a
  client-facing, BYTE-INTEROPERABLE blob a pinned redis-server must `RESTORE` identically.
  This is ORACLE-GATED: without a pinned redis-server in CI (#97) the byte-interop cannot
  be validated, which is exactly why #242 defers the intricate serializer. It is a real
  feature, but it is NOT on the critical path for resharding.
- **An INTERNAL self-consistent transfer encoding** for resharding: IronCache serializes a
  value on A and deserializes it on B, both the SAME build. It needs only ROUND-TRIP
  fidelity (encode then decode is identity), NOT Redis byte-interop, so it is fully
  testable on the macOS dev box with no external oracle. It can reuse the value encoders the
  persistence snapshot already uses (the on-disk `KvObj` form), which are self-consistent by
  construction.

RECOMMENDATION: build the INTERNAL encoding for APPLY (unblocks #371 on macOS); keep the
Redis-compatible `DUMP` / `RESTORE` (#129 / #242) as a SEPARATE client-facing track gated
on the differential oracle. When #129 lands, APPLY MAY switch to it, but must not BLOCK on
it. Document the internal format as private and versioned (a leading version byte), so it is
never mistaken for a stable wire contract.

#### The transport

The cluster bus (`RAFTMSG` over each node's bus port, `ironcache-raft-net`) already carries
peer-to-peer control traffic in RESP framing. A resharding transfer adds a bus verb (an
internal `__ICMIGRATEKEYS <slot> <encoded-batch>` analog to the existing internal verbs)
that the destination applies to its store. Alternatively the replication transport
(`StreamOp` / `drain_and_ship`, already a tested key-shipping mechanism) can be adapted.
The bus is the lower-risk first target (it is already the cross-node control channel and is
not on the data hot path); the choice is settled in the transfer-primitive slice.

## The controller (per slot move)

For each `{slot, src=A, dst=B}` in the move plan, driven from the raft leader (config
mutations already forward to the leader, `raft-net`):

1. **Commit `SETSLOT <slot> IMPORTING` on B and `SETSLOT <slot> MIGRATING` on A** (both
   exist as committed `ConfigCmd` proposals; `set_migrating` records the peer). While
   MIGRATING, A still OWNS the slot (reads/writes to present keys are served; a miss on a
   key A does not hold returns `-ASK B`, the redirect already modeled), so there is no
   ownership gap and no lost read.
2. **Drain**: loop `GETKEYSINSLOT <slot> <batch>` on A (the read side landed this
   milestone); for each returned key, encode it (internal format) + ship to B over the bus;
   B `RESTORE`s it (idempotent: re-applying an already-present key is a no-op or an
   overwrite with the same bytes); A DELETEs it AFTER B acknowledges. Repeat until
   `COUNTKEYSINSLOT <slot>` on A reaches 0.
3. **Commit `SETSLOT <slot> NODE B`** (the epoch-bumping ownership handover). Now B owns
   the slot; A redirects any lingering client with `-MOVED`.

### Safety (the cardinal properties)

- **No data loss**: a key exists on B (committed) BEFORE A deletes it; a crash mid-batch
  leaves the key on BOTH (B has it, A has not yet deleted), which the next drain pass
  reconciles (A re-ships; B overwrites identically). Never neither.
- **No split ownership**: ownership changes ONLY at the final committed `SETSLOT NODE`,
  under the epoch fence, so no client ever sees two owners for the slot in one epoch (the
  console's central-hazard guard, #368, continues to hold).
- **Resumable**: the controller is a pure function of the committed slot state + the live
  key contents. A restart re-reads `migration_state`, finds the in-flight slot, and
  re-drains from wherever `COUNTKEYSINSLOT` says it is. No durable controller checkpoint is
  needed beyond the committed `MIGRATING` / `IMPORTING` tags.
- **Bounded + cancellable**: one slot at a time (or a small fixed concurrency), so a
  rebalance never floods the bus; a `CLUSTER SETSLOT <slot> STABLE` (or an abort) clears
  the tags and stops the drain, leaving the slot on its current committed owner.

### Write consistency during the drain (the key open question for the controller slice)

While a slot is `MIGRATING`, the source A still OWNS it and serves client traffic, so a key
can be WRITTEN on A after it was already shipped to B, leaving B with a STALE copy. This is
the classic resharding hazard, and the controller slice must pin one of the standard
resolutions (to be settled there, not hand-waved here):

- Redis's model: A keeps serving reads AND writes to PRESENT keys during `MIGRATING`; the
  drain re-reads `GETKEYSINSLOT` and RE-SHIPS, and because the loop only exits when
  `COUNTKEYSINSLOT` reaches 0, a key deleted-on-ship then re-created is simply picked up
  again. The terminal step migrates the final keys while briefly holding the slot so no
  write races the handover.
- A stricter option: once `MIGRATING`, A serves reads + existing-key writes but the drain
  ships in a last-writer-wins pass (delete-on-A only AFTER B acknowledges the CURRENT
  bytes), so a concurrent write re-dirties the key and it re-ships next pass. New-key
  inserts into the migrating slot are redirected to B (`-ASK`) so they land at the
  destination directly and never need shipping.

The invariant the slice MUST preserve either way: at the committed `SETSLOT NODE`, B holds
the SAME bytes A would have served, for every key. The move planner + the transfer encoding
(slices 1-2) are independent of this choice; it is localized to the controller (slice 3),
which is why it is called out as that slice's central correctness question rather than a
blocker for the earlier ones.

### Determinism + perf (ADR-0002 / ADR-0003)

The controller is a COLD control-plane loop (bus traffic + committed proposals), entirely
off the RESP data hot path; the standalone deployment never runs it. The move plan is
deterministic (the pure planner); the drain order follows `GETKEYSINSLOT`'s deterministic
prefix. No new hot-path cost.

## Staged implementation

1. `SlotMap::rebalance_moves` (pure, unit-tested). Optionally surface the src to dst summary
   in the DRYRUN (with the console parser update).
2. The internal transfer encoding (round-trip unit tests over every value type; reuse the
   persistence `KvObj` encoders) + the bus transfer verb (integration-tested on a
   multi-shard node, then a two-node loopback cluster like `cluster_slice2`).
3. The controller: per-slot MIGRATING/IMPORTING -> drain -> NODE, resumable, leader-routed,
   one-at-a-time. Integration-tested end to end on a two-node loopback cluster: seed a slot
   on A, APPLY, assert the keys move to B, ownership commits, and a crash-mid-drain resumes
   without loss.
4. `CLUSTER REBALANCE APPLY`: wire the move plan into the controller behind the existing
   admin/dangerous ACL tier + the destructive-confirm the console mutating actions already
   use (#450 / #451 / #452).

Each slice is macOS-testable (a two-node loopback cluster is one process pair, no external
oracle). The Redis-compatible `DUMP` / `RESTORE` (#129 / #242) stays a separate,
oracle-gated track this driver does not block on.

## Non-goals

- Not Redis `DUMP` / `RESTORE` byte-interop (#129 / #242): the internal transfer encoding is
  private + versioned, not a client wire contract.
- Not automatic / continuous rebalancing: APPLY runs a bounded operator-triggered plan, not
  a background controller that reshards on load.
- Not multi-slot parallelism beyond a small fixed bound: correctness + bounded bus load
  first; throughput tuning is later.
