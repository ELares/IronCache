# Design: Atomic, snapshot-streamed online slot migration

Issue: #75. Decisions: ADR-0025 (the 16384-slot partition is the one migration
unit; resharding follows Valkey atomic slot migration over the replicated SETSLOT
handshake), ADR-0011 (single-node-first, slot-ready per-slot dictionaries),
ADR-0002 (shared-nothing single-writer per shard). Related: #72 (the slot-
granularity decision ADR-0025 records), SNAPSHOT.md (#60, the forkless per-slot
iterator and diskless stream this reuses), CONTROL_PLANE.md (#73, the Raft slot
map that owns the ownership flip), REPLICATION.md (#77, the offset stream the
mutation channel is modeled on), DISTRIBUTION.md (#68, clustering umbrella),
#148 (rebalancing policy, out of scope here), #70 (client MOVED/ASK contract).

## Goal and scope

Move a hash slot, and every key hashing to it, from a source node to a
destination while the cluster keeps serving traffic, with exactly one owner for
the slot at every instant and across any crash. Migration must not freeze writes,
must not surface retry churn to clients, and must not OOM or stall on a single
large key. In scope: the per-slot migration state machine, the per-key cutover
fence, chunked large-key transfer, and the single Raft SETSLOT ownership flip. Out
of scope: rebalancing policy (which slots move and when), owned by #148; the
placement hash that picks the destination node (#80); and client-side topology
caching (#70). The unit being moved is fixed by ADR-0025 (the decision recorded in
#72): one 16384-space slot, which is simultaneously the per-core execution shard
and the migration unit, and which already owns its own dictionary so it is cheap
to detach and ship [valkey-per-slot-dict-16b].

## Design

### Why not the legacy per-key path

The legacy Redis resharding path sets per-slot MIGRATING/IMPORTING markers and
moves keys with MIGRATE in batches [redis-cluster-legacy-migration]. It is
rejected wholesale: it serializes per key, dumps and restores whole values (a
large key stalls the slot and can OOM the transfer), and it leans on a
migration-barrier knob to throttle how aggressively slots move
[redis-cluster-migration-barrier-default]. IronCache makes migration
non-blocking by construction rather than throttled, so that barrier knob has no
analogue here. Instead IronCache borrows the Valkey 9.0 shape wholesale: snapshot
the migrating slot, stream incremental mutations to the destination, then flip
ownership in one replicated step [valkey-atomic-slot-migration].

### State machine

A migration of slot S from source A to destination B proceeds through committed,
idempotent phases. Each phase is restartable: a crash and restart re-derives the
current phase from durable state and the committed Raft slot map, never from
in-flight memory.

- IDLE: A owns S, B holds nothing for S.
- SNAPSHOTTING: A opens a forkless point-in-time iterator over S (below) and B
  begins loading the snapshot into a separate receiver arena.
- STREAMING: alongside the snapshot, A streams a per-slot mutation log of live
  writes to S; B applies snapshot entries and tail mutations until apply lag
  falls under a threshold.
- FENCING: A enters the per-key cutover fence, draining residual mutations so B
  is byte-for-byte caught up on the keys being released.
- FLIP: A proposes a single Raft SETSLOT entry transferring ownership of S to B.
- DONE: on commit, B owns S and serves it; A forwards any straggler request for
  S to B, then drops its copy of S.

The slot is owned by exactly one node in every phase: A is the committed owner
through FENCING, B becomes the committed owner the instant the FLIP entry commits,
and there is never a window in which both or neither own S because ownership is a
single committed log entry, not a pair of node-local edits.

### Bulk transfer reuses the forkless per-slot snapshot

The initial bulk copy reuses the forkless, versioned, point-in-time iterator from
SNAPSHOT.md (#60) rather than opening a second slot-scan path. That iterator sets
a per-shard epoch cut, emits every entry in S with version at or below the cut,
bumps each emitted entry's version so it is never re-sent, and runs in constant
extra memory independent of slot size because its serialization channel is
bounded and back-pressures the writer. Reusing it means migration inherits
snapshot isolation at bucket granularity and the no-fork, no-RSS-spike property
for free, and there is exactly one iteration path to test rather than two. A
fresh, non-versioned slot scan is rejected: it duplicates code and cannot give a
clean point-in-time cut under concurrent writes.

### Mutation stream during transfer

While the snapshot drains, A also feeds B a per-slot mutation stream: every write
that lands in S after the epoch cut is appended to a per-slot channel and shipped
to B. This is the relaxed (post-image or diff) variant of the SNAPSHOT.md stream,
scoped to one slot, and it carries the same offset-cursor shape as the
replication stream in REPLICATION.md so the two share framing. Streaming, not
re-scanning, is what lets the transfer converge: a re-scan of deltas can never
catch up under sustained write load, whereas a tailing stream's residual shrinks
to whatever arrives during the fence. B reports its apply offset back to A;
cutover is gated on the gap between A's write offset and B's apply offset falling
under a small threshold so the fence hold is short.

### Chunked large-key handling

A single value larger than the in-flight byte budget is transferred in bounded
chunks rather than as one whole-value blob. Each chunk carries the key, a byte
range, and a total length; B reassembles in its receiver arena and commits the
key only when the final chunk arrives. The in-flight byte budget is a hard cap
shared across keys, so one giant key cannot monopolize the channel and cannot
OOM either endpoint; it also removes the head-of-line stall a whole-value
DUMP/RESTORE would impose on the rest of the slot. Whole-value transfer is
rejected for exactly the OOM-and-stall reason the legacy path fails
[redis-cluster-legacy-migration]. A chunked key that is mutated mid-transfer is
handled by the mutation stream: the post-image supersedes any partially shipped
chunks, which B detects by key version.

### Per-key fence cutover, no write freeze

Cutover is per key, not per slot. When apply lag is under threshold, A walks the
keys of S; for each key it briefly fences that one key (pauses new writes to it,
drains its tail of the mutation stream to B), then releases. Writes to every
other key in S, fenced or not yet reached, continue to succeed throughout, so
there is no whole-slot write freeze. A whole-slot freeze is rejected: it is
simpler but stalls all writes to S for the cutover duration, violating the
no-freeze acceptance bar. Because the fence is per key within a single slot, and
because all keys of a slot live on one node and move together (ADR-0025),
multi-key and hash-tag-co-located operations stay correct across the window: a
multi-key command on S either runs entirely on A before the flip or entirely on B
after it, never split. The fence holds only for the residual drain of one key, so
no client ever sees TRYAGAIN-style churn; a fence-hold timeout falls back to a
client-visible redirect rather than an error, and the timeout and client behavior
on it are an open question below.

### Single Raft SETSLOT ownership flip

Ownership transfers as one committed entry on the Raft slot map of CONTROL_PLANE.md
(#73): A proposes SETSLOT(S -> B); when that entry commits, B is the authoritative
owner at the new config epoch and CLUSTER SLOTS/SHARDS projects B for S, while a
stale client that still asks A is answered with MOVED carrying the committing
epoch (#70). This adapts the Valkey model where SETSLOT is a replicated log entry
rather than two unsynchronized node-local edits [valkey-8-setslot-replicated],
onto IronCache's Raft slot map, and it is the reason the flip is atomic and
crash-safe [valkey-atomic-slot-migration]. Two node-local edits are rejected:
they can leave A and B disagreeing about ownership, the split-brain ADR-0025
exists to prevent.

### Crash safety: exactly one owner

The committed Raft slot map is the single source of truth for ownership, so a
crash of either endpoint at any step leaves exactly one owner. A crash before the
FLIP entry commits leaves A as the committed owner; on restart, A re-derives that
it still owns S, B discards its partial receiver arena, and the migration either
resumes from SNAPSHOTTING or is abandoned, with no half-migrated slot exposed. A
crash after the FLIP commits leaves B as the committed owner; A on restart sees S
is no longer its and drops its residual copy. There is no instant at which both
nodes believe they own S, because belief follows the committed log, not local
memory, and there is no instant at which neither owns S, because the prior owner
holds ownership until the instant the flip commits.

## Open questions

- The mutation-stream apply-lag threshold that triggers the move from STREAMING
  to FENCING (smaller threshold means a shorter fence but a longer streaming
  tail).
- Chunk size and the global in-flight byte budget for large-key transfer.
- Fence-hold timeout per key and the client contract on timeout: block briefly,
  or redirect to the destination with ASK.
- Backpressure policy when B's apply rate falls behind A's write rate for long
  enough that the mutation channel fills (shed snapshot, slow the writer, or
  abort and retry the migration).
- Whether straggler forwarding from A to B after the flip is bounded by a grace
  window or by draining the in-flight request set.

## Acceptance and test hooks

- No write freeze: under a write-heavy workload against S, writes to non-fenced
  keys in S succeed for the entire migration; only the single key currently in
  its fence pauses, and only briefly.
- No TRYAGAIN-style churn is surfaced to clients during the cutover window.
- Crash injection at every phase boundary (and mid-chunk, mid-fence) yields
  exactly one committed owner of S and no half-migrated slot, verified against
  the Raft slot map (joint hook with #73 and the Jepsen plan #99).
- A single key larger than the in-flight byte budget migrates via chunks with no
  OOM and no head-of-line stall of the rest of S.
- Ownership flips via one committed Raft SETSLOT entry; CLUSTER SLOTS/SHARDS and
  MOVED reflect B only after that commit [valkey-atomic-slot-migration]
  [valkey-8-setslot-replicated].
- Multi-key and hash-tag-co-located commands on S run wholly on one side of the
  flip and remain correct across the cutover, matching the differential oracle.
- The bulk-copy iterator is the same forkless per-slot snapshot path as #60 (no
  second slot-scan code path), confirmed by the constant-extra-memory property
  holding during migration.

## References

- ADR-0025, ADR-0011, ADR-0002; issues #75, #60, #68, #73, #70, #72, #148, #80,
  #99; specs SNAPSHOT.md, CONTROL_PLANE.md, REPLICATION.md, DISTRIBUTION.md.
- Claims: [valkey-atomic-slot-migration], [valkey-8-setslot-replicated],
  [redis-cluster-legacy-migration], [redis-cluster-migration-barrier-default],
  [valkey-per-slot-dict-16b].
