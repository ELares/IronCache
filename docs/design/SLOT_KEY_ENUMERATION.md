<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Design: cross-shard slot key enumeration (`COUNTKEYSINSLOT` / `GETKEYSINSLOT`)

Issue: #371 (the rebalance-APPLY foundation). Decisions: ADR-0002 (shared-nothing
thread-per-core), ADR-0003 (determinism / Env seam). Related: CLUSTER_CONTRACT.md
(the CRC16 slot hash + the slot map), COORDINATOR.md (the cross-shard scatter-gather
fan-out, `dispatch_remote_whole_keyspace`), `crates/ironcache-server/src/cmd_cluster.rs`
(the current placeholders), `crates/ironcache-server/src/route.rs` (`owner_shard`,
`CommandClass`).

## Goal and scope

`CLUSTER COUNTKEYSINSLOT <slot>` and `CLUSTER GETKEYSINSLOT <slot> <count>` are the
resharding read side: an operator (or a rebalance driver) asks "how many keys live in
this slot" and "give me up to N of them" so the slot's data can be drained during a
migration. Today both are DOCUMENTED PLACEHOLDERS: `COUNTKEYSINSLOT` always returns
`0` and `GETKEYSINSLOT` always returns an empty array (`cmd_cluster.rs`), because there
is no mechanism that maps a slot to the keys living in it.

This spec makes both commands HONEST. It is the foundation #371's rebalance-APPLY
needs: APPLY drains a slot by repeatedly calling `GETKEYSINSLOT` for a batch and moving
those keys, exactly as `redis-cli --cluster reshard` does. Making these two commands
real unblocks that loop; the APPLY driver itself (the committed `SETSLOT` sequence plus
per-key movement) is a later slice that consumes this one.

This spec is READ-ONLY introspection over the keyspace. It MUST NOT change the data hot
path, the routing, or the byte layout of any value. It MUST add ZERO cost to the
standalone deployment (the only path the perf gate and the competitor head-to-head
measure).

## Why this is not trivial: a slot's keys span every internal shard

Two independent hashes are in play, and conflating them is the trap:

- The CLIENT-visible CLUSTER SLOT is `CRC16(key) % 16384` with the `{hashtag}` rule
  (CLUSTER_CONTRACT.md). It decides which NODE owns the key.
- The INTERNAL OWNER SHARD is `owner_shard(key) = hash64(key) % n_shards` (FNV-1a,
  `route.rs`). It decides which thread-per-core SHARD inside a node holds the key.

These are DIFFERENT functions of the key, so the keys of one slot are spread across ALL
`n_shards` internal shards with no correlation. There is no shard that "owns" a slot.
Therefore any honest `COUNTKEYSINSLOT` must aggregate across every shard, and any
`GETKEYSINSLOT` must gather from every shard. This cross-shard aggregation, from a
command that is otherwise connection-local, is exactly why the commands were left as
placeholders ("a later slice, alongside real slot ownership").

## Two candidate mechanisms

### A. A maintained per-slot count index (REJECTED for the hot path)

Keep a `[u32; 16384]` per shard, incremented on insert and decremented on delete, so
`COUNTKEYSINSLOT` is O(1) (sum the shards' cells for the slot). This is what Redis does.

Rejected as the primary mechanism because it taxes the WRITE HOT PATH for a rarely-used
admin command:

- Every insert/delete must compute `CRC16(key)` (a second hash beyond `owner_shard`'s
  FNV) and touch a counter. CRC16 over the key bytes is not free at the qps this engine
  targets.
- It adds a fixed `4 * 16384 = 64 KiB` per shard of resident memory that exists even
  when no one ever calls `COUNTKEYSINSLOT`.
- ADR-0002's thread-per-core posture means this is per-shard state maintained on the
  core's critical path. The perf gate ratchets `bytes_per_key` and qps; a write-path
  hash for an introspection command is the wrong trade.

It would only ever pay off if `COUNTKEYSINSLOT` were hot. It is not: it is an operator /
resharding-driver command issued occasionally per slot, not per client op.

### B. On-demand cross-shard scan (CHOSEN)

Compute the answer only when asked, by scanning each shard's partition once and
filtering by slot. No write-path change, no resident index, no standalone cost.

- `COUNTKEYSINSLOT <slot>`: each shard counts the keys in the selected db whose
  `CRC16(key) % 16384 == slot`; the home core SUMS the per-shard integers (the same
  merge `DBSIZE` already uses).
- `GETKEYSINSLOT <slot> <count>`: each shard collects up to `count` of its keys matching
  the slot; the home core CONCATENATES and truncates to `count` (the same merge `KEYS`
  already uses, plus a bound).

Cost: O(keys in this node's selected db) per call, fanned out so each shard does
O(keys / n_shards) in parallel, on the COLD admin path only. For a node holding millions
of keys a `COUNTKEYSINSLOT` is a tens-of-milliseconds scan; that is acceptable for an
operator/resharding command and is strictly better than taxing every write. This is the
same complexity class as `KEYS` / `DBSIZE`, which are already whole-keyspace scans.

This is the perf-correct choice under ADR-0002: zero hot-path and zero standalone cost,
paid only by the caller, only in cluster mode.

> If a future workload makes `COUNTKEYSINSLOT` hot enough to matter (e.g. a controller
> that polls every slot in a tight loop), mechanism A can be added LATER as a cluster-mode
> -only, lazily-built index without changing these commands' contract. The scan is the
> correct default; the index is a conditional optimization, not a prerequisite.

## Reusing the whole-keyspace fan-out

The cross-shard scatter-gather already exists for the `WholeKeyspace` command class
(COORDINATOR.md): the home core sends the request to every shard, each runs its partial
via `dispatch_remote_whole_keyspace` against its own partition, and the home core merges
(`DBSIZE` sums integers, `KEYS` concatenates arrays, `SCAN` composes per-shard cursors).
`COUNTKEYSINSLOT` and `GETKEYSINSLOT` are the SAME shape: a per-shard partial plus a
sum / concatenate merge. The bulk of the machinery is already built and tested.

The one piece that does not fit cleanly: these are CLUSTER SUBCOMMANDS, and `CLUSTER` is
`CommandClass::AlwaysHome` (it is a control command that mostly answers from immutable
node facts). So the serve loop never fans `CLUSTER` out. The integration work is to route
EXACTLY these two subcommands through the scatter path while leaving every other `CLUSTER`
subcommand home.

### Routing options for the two subcommands

1. Recognize the `(CLUSTER, COUNTKEYSINSLOT | GETKEYSINSLOT)` pair in the serve loop's
   routing decision and dispatch it down the existing whole-keyspace scatter, with a
   per-shard partial that runs the slot-filtered count/collect. PREFERRED: it reuses the
   merge code paths and keeps the per-shard partial in the same `cmd_keyspace` family as
   `DBSIZE`/`KEYS`.
2. Synthesize internal whole-keyspace verbs (e.g. an internal `__ICCOUNTSLOT` /
   `__ICGETSLOT` the way spanning *STORE results use `__ICSTORE*`) that ARE classified
   `WholeKeyspace`, and have the `CLUSTER` handler delegate to them. Avoids special-casing
   the serve loop's `CLUSTER` routing, at the cost of two internal verbs.

Either keeps the change narrow and the per-shard partial identical. The choice is an
implementation detail settled in the first implementation PR; both preserve the contract.

## Per-shard partial

Against one shard's selected db (no `ConnState`, no admission/expiry, exactly like the
other whole-keyspace partials):

- COUNT: iterate the db's live keys; for each, compute the client CRC16 slot; if it equals
  the requested slot, increment a local counter. Return the integer.
- GET: iterate the db's live keys; for each matching the slot, push it into a result vec;
  stop once the vec reaches `count` (a shard need not return more than the global cap).
  Return the array.

Expired-but-not-reaped keys are skipped (the same liveness check `DBSIZE`/`KEYS` apply),
so the count matches what a client could actually read.

### Consistency semantics (not a global snapshot)

The fan-out visits shards at slightly different instants, and writes may land between a
shard's partial and the merge, so `COUNTKEYSINSLOT` is a CONSISTENT-ENOUGH estimate, not a
linearizable snapshot, under concurrent traffic. This is acceptable and matches Redis:
the resharding loop calls `COUNTKEYSINSLOT` only to decide WHEN A SLOT IS DRAINED, and it
loops until the count reaches `0` while it is also moving keys out, so transient skew never
strands data. `GETKEYSINSLOT` returns keys that were live in their shard at scan time; a key
deleted concurrently is simply absent next batch. The APPLY drain is self-correcting: moved
keys are deleted from the source, so each successive `GETKEYSINSLOT` returns the NEXT keys
with no cursor to thread, and `COUNTKEYSINSLOT` monotonically approaches `0` as the slot
empties (modulo concurrent inserts into a slot being migrated, which the source rejects once
it is `MIGRATING`).

### Determinism (ADR-0003)

`GETKEYSINSLOT`'s "up to `count`" makes the RESULT SET selection observable, so it must be
deterministic given the same store state: each shard iterates in `hashbrown`'s stable
explicit-hash order (the same order `KEYS` relies on) and takes a deterministic prefix; the
home core concatenates shards in shard-index order and truncates. No RNG enters the path
(unlike `RANDOMKEY`). `COUNTKEYSINSLOT` is order-independent (a sum), so it is trivially
deterministic.

## Cluster-mode gating (the zero-standalone-cost guarantee)

Both commands already return `-ERR cluster support disabled` in standalone
(`cmd_cluster.rs` gates every `CLUSTER` subcommand on `ctx.info.cluster_enabled`). So the
scan is UNREACHABLE in the standalone deployment, and the write path is untouched in BOTH
modes (mechanism B maintains nothing). The perf gate and the competitor head-to-head, which
run a single standalone node, see ZERO change. This is the central perf claim and it is
structural, not a tuning artifact.

## The #371 rebalance-APPLY consumer

With these two commands honest, the rebalance-APPLY driver (the remaining #371 work) drains
a slot the same way Redis resharding does:

1. `SETSLOT <slot> IMPORTING` on the destination, `SETSLOT <slot> MIGRATING` on the source
   (both already exist as committed `ConfigCmd` proposals; the source-keeps-ownership-until
   -commit semantics are already modelled, see the migration state machine).
2. Loop: `GETKEYSINSLOT <slot> <batch>` on the source; move each returned key to the
   destination; repeat until `COUNTKEYSINSLOT <slot>` reaches `0`.
3. `SETSLOT <slot> NODE <dest>` to commit the ownership change (epoch bump).

This spec delivers step 2's read side. The per-key MOVE (a cross-node key ship) and the
committed `SETSLOT` orchestration are the APPLY driver's own slices, sequenced after this.

## Staged implementation plan

1. The per-shard partials `cmd_keyspace::count_keys_in_slot` / `keys_in_slot` (pure over
   the `Keyspace` seam), unit-tested against a seeded shard for known slots, including the
   `{hashtag}` rule and the empty-slot and `count`-bound cases.
2. The cross-shard wiring (one of the two routing options above) so
   `CLUSTER COUNTKEYSINSLOT` / `GETKEYSINSLOT` fan out, sum/concatenate, and truncate. An
   integration test over a real multi-shard `run_server` (the `coordinator_spanning_move`
   harness pattern) that seeds keys known to span shards and asserts the aggregate count and
   the bounded, deterministic key list.
3. Drop the placeholder docs/returns in `cmd_cluster.rs`; update the differential-oracle
   expectations so a pinned redis-server and IronCache agree on the count/keys for a seeded
   slot.

Each slice is independently mergeable, keeps the write path untouched, and is testable on
the macOS dev box (no Linux, no cluster of separate processes; the multi-shard server is a
single process).

## Non-goals

- No write-path index (mechanism A) in this work; the scan is the contract.
- No change to `owner_shard`, the CRC16 slot hash, or any value byte layout.
- Not the rebalance-APPLY driver itself (the committed `SETSLOT` sequence + per-key ship);
  this is its read-side foundation.
- No per-slot key ship / cross-node `MIGRATE` primitive; that is the APPLY driver's slice.
