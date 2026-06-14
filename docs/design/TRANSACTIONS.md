# Design: MULTI/EXEC/DISCARD/WATCH with optimistic locking and no rollback

Issue: #19. Decisions: ADR-0010 (transaction and scripting surface scope, applied
not re-decided), ADR-0002 (shared-nothing thread-per-core). Related: #29/COORDINATOR.md
(cross-shard execution path), #130/BLOCKING_COMMANDS.md (no-block-inside-EXEC),
#137/ADMISSION.md (per-connection buffer accounting), #15/PROTOCOL.md (connection
state machine).

## Goal and scope

Redis transactions are not transactions in the rollback sense: MULTI opens a queue,
each staged command replies +QUEUED, and EXEC applies the batch atomically against
the keyspace with no interleaving; there is no rollback, and WATCH adds optimistic
concurrency so a watched key changed before EXEC aborts the batch [multi-exec-no-rollback].
This spec reproduces that surface exactly, including its sharp edges, on the
shared-nothing model. ADR-0010 already pinned that the surface is MULTI/EXEC/DISCARD/
WATCH with no Lua/Functions; this spec applies that decision and does not re-decide
it. Out of scope: Lua scripting (Tier 4 non-goal, ADR-0010) and the coordinator
mechanics themselves (COORDINATOR.md #29).

## Design

### Queue then apply

- MULTI marks the connection in transaction state; each subsequent command is
  validated for arity and name and, if valid, staged on a per-connection queue with
  a +QUEUED reply rather than executed [multi-exec-no-rollback]. EXEC applies the
  staged batch as one atomic unit with no interleaving from other connections; the
  late apply keeps the queue cheap and holds no keys until commit. DISCARD drops
  the queue and leaves transaction state without applying anything.
- A command that fails validation at queue time (unknown command, wrong arity)
  marks the transaction dirty; EXEC then refuses the whole batch with an error and
  applies nothing, faithful to Redis. This is distinct from a runtime error below.

### No rollback on runtime errors

- Errors discovered while EXEC runs a queued command (for example a type error
  against an existing key) do not unwind prior commands: EXEC runs the remaining
  queued commands and returns an array of per-command replies, some of which may be
  errors [multi-exec-no-rollback]. IronCache keeps no undo log; this is the
  deliberate Redis contract clients depend on, and the cost of true rollback (an
  undo log plus broken compatibility) is rejected.

### WATCH optimistic locking via per-key dirty-CAS

- WATCH records, for each watched key, a version stamp owned by the key's shard
  (ADR-0002): any write that modifies the key bumps its version. At EXEC the
  coordinator revalidates the recorded stamps; if any watched key's version moved,
  EXEC aborts and returns a null reply, leaving the keyspace untouched
  [multi-exec-no-rollback]. Versioning is O(watched keys), avoiding the O(db) cost
  of snapshotting the keyspace at WATCH. WATCH tracking is shard-local on the
  owning core (ADR-0010 consequence); DISCARD and a completed EXEC both clear all
  watches, matching Redis unwatch timing.

### Single-shard fast path vs cross-shard EXEC

- When every queued key hashes to one shard, the whole transaction runs on that
  owning core as the lock-free fast path (ADR-0010), with WATCH revalidation and
  apply in one serialized pass and no coordinator hop.
- A multi-shard batch declares its full key set up front so the coordinator
  (COORDINATOR.md #29) acquires a global txid and enqueues per shard in txid order
  [dragonfly-vll-citation]; each shard revalidates its watched keys and applies its
  subset at its turn, so the batch commits atomically relative to other
  transactions without a global lock.

### Cross-shard EXEC on an unreachable shard

- If a participating shard cannot accept the batch at apply time (its bounded
  coordinator channel is wedged, or in the multi-node future the shard is
  unreachable), EXEC fails closed: it aborts the whole batch with an error and
  applies nothing on any shard, rather than partially applying. Because apply is
  gated on reaching the txid head on every participating shard, a shard that never
  reaches ready blocks commit, so the coordinator detects it via the channel
  back-pressure / timeout path (COORDINATOR.md) and turns it into a clean abort.
  Atomicity is preserved by never letting some shards apply while another is
  unreachable.

### Per-connection queue caps and pipelining

- The staged queue is bounded in both total bytes and command count per connection;
  exceeding either cap marks the transaction dirty so EXEC errors instead of
  applying an unbounded batch, bounding memory against a hostile MULTI. The queue
  bytes are counted in the same per-connection accounting as input buffers
  (ADMISSION.md #137). Pipelined commands arriving in one flush are staged in
  order; EXEC is the single atomic flush point, so a pipelined MULTI ... EXEC
  applies as one unit.

## Open questions

- Per-connection queue cap values (bytes, command count) and the exact error byte
  string returned on overflow (pinned to the oracle, #97).
- WATCH version granularity: a per-key counter vs a per-shard epoch (precision vs
  memory per key).
- Whether cross-shard EXEC abort-on-unreachable surfaces a distinct error from a
  WATCH-null abort, or both collapse to the Redis null/err the client expects.

## Acceptance and test hooks

- EXEC applies queued commands atomically with no interleaving from other
  connections (an interleaving test).
- A dirtied WATCH makes EXEC return null and leaves the keyspace untouched
  [multi-exec-no-rollback] (a dirty-CAS abort test).
- Queue-time command errors abort the whole batch; runtime errors at EXEC do not
  unwind prior commands and EXEC returns the per-command reply array
  [multi-exec-no-rollback] (a no-rollback test).
- Single-shard and cross-shard batches both apply atomically via the coordinator
  [dragonfly-vll-citation]; a cross-shard EXEC with one shard unreachable aborts
  cleanly and applies nothing (an unreachable-shard test).
- A per-connection queue past its byte/count cap makes EXEC error (a queue-cap
  test); a pipelined MULTI ... EXEC applies as one unit (a pipeline test).

## References

- ADR-0010, ADR-0002; issues #29, #109, #130, #137, #15, #97, #1;
  specs COORDINATOR.md, BLOCKING_COMMANDS.md, ADMISSION.md, PROTOCOL.md.
- Claims: [multi-exec-no-rollback], [dragonfly-vll-citation].
