# Design: Blocking command semantics (BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOPMIN/BZMPOP, WAIT, XREAD BLOCK) under shared-nothing

Issue: #130. Decisions: ADR-0002 (shared-nothing thread-per-core, a connection
lives on one core). Related: #29/COORDINATOR.md (cross-shard wakeup path),
#137/ADMISSION.md (maxclients accounting, output buffers), #19/TRANSACTIONS.md
(no-block-inside-EXEC), #140/CONNECTION_LIFECYCLE.md (dead-peer reaping, already
shipped), #15/PROTOCOL.md (connection state machine), #86/OBSERVABILITY.md (metrics).

## Goal and scope

Blocking pops, WAIT, and BLOCK forms are a distinct interaction model: a client
parks until a key gains data or a timeout fires, with FIFO wakeup fairness, timeout
precision, and the rule that blocking commands do not block inside EXEC
[redis-blocking-commands-exist-and-block]. On a thread-per-core shared-nothing
engine, parking and cross-shard wakeup are a real concurrency-model problem
touching the connection state machine (#15, which has no blocked-client registry)
and the per-shard execution boundary. This spec defines the registry, the per-shard
wait queues, timeout handling, cross-shard wakeup, the no-block-inside-MULTI/EXEC
rule, and dead-peer reaping. Out of scope: the connection-lifecycle reaper itself
(CONNECTION_LIFECYCLE.md #140, already shipped) and the transaction surface
(TRANSACTIONS.md #19).

## Design

### Blocked-client registry and per-shard FIFO wait queues

- BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOPMIN/BZMPOP block the calling client when the
  target key has no element available, and unblock when an element is pushed or a
  timeout elapses [redis-blocking-commands-exist-and-block]. IronCache parks the
  blocked client in a registry on its home core (ADR-0002) and registers it on a
  per-key wait queue owned by the key's shard. The shard's wait queue is FIFO: when
  a key gains data the longest-waiting client is served first, matching Redis
  wakeup fairness [redis-blocking-fifo-wakeup-fairness]. The parked connection
  yields its core to other work rather than spinning.
- A producing command (LPUSH, ZADD, LMOVE, XADD, and similar) checks its shard's
  wait queue for the affected key after the write and, if a client is waiting,
  hands the new element to the head waiter in FIFO order before any later-arriving
  client [redis-blocking-fifo-wakeup-fairness].

### Cross-shard wakeup

- The waiter's connection lives on its home core but the watched key is owned by
  the key's shard, which may be a different core. The producing shard owns the
  wakeup decision (it sees the write and the wait queue) and signals the waiter's
  home core through the coordinator path (COORDINATOR.md #29): a bounded message
  tells the home core to complete the blocked command and write the reply. The
  element is reserved on the producing shard at wakeup so two waiters cannot both
  claim it. This keeps the wakeup an explicit message, never shared mutation across
  cores (ADR-0002).
- BLMOVE/BLMPOP that move between keys on different shards reuse the coordinator's
  txid-ordered hop (COORDINATOR.md) so the pop and push apply in a defined order.

### Timeout precision and the zero-timeout rule

- Each blocking call arms a per-connection timer on its home core for the requested
  timeout; the parked client is woken with a null/empty reply when the timer fires
  if no element arrived. A timeout of 0 means block forever
  [redis-blpop-timeout-double-zero-forever], so a 0 timeout arms no timer and the
  client parks until data or disconnect. Timeouts are a decimal number of seconds
  with sub-second resolution; the timer cadence is tuned so the effective wakeup is
  within a small bound of the requested time (precision is an open knob below).

### WAIT and XREAD BLOCK

- WAIT parks the client in the same registry but is satisfied by replication-ack
  progress rather than a key push: the WAIT contract is to return once the requested
  number of replicas acknowledge prior writes, or when its timeout elapses. WAIT
  does not make the system strongly consistent overall
  [redis-wait-not-strongly-consistent], so it is a bounded-acknowledgement wait,
  not a linearizability guarantee. (WAIT is inert until the multi-node replication
  path exists; this spec only fixes where it parks.)
- XREAD BLOCK parks a stream reader until new entries arrive past the requested
  id; the special `$` id means deliver only entries added after the call blocks,
  so the reader is registered against the stream's current last-id and woken by a
  later XADD beyond it [redis-xread-block-semantics-dollar-id]. It uses the same
  per-shard wait queue and cross-shard wakeup as the pops.

### No blocking inside MULTI/EXEC

- Inside a transaction a blocking command does not block: when a key has nothing
  available the command behaves as its non-blocking form and returns immediately
  (a null/empty reply) rather than parking the EXEC
  [redis-blocking-noop-inside-multi-exec]. Blocking inside EXEC would stall the
  atomic apply and the owning core, so the command is degraded to non-blocking at
  queue/apply time, consistent with TRANSACTIONS.md (#19). The same no-block
  degradation applies to a scripted/pipelined context where parking is unsafe.

### Dead-peer reaping of blocked clients

- A blocked client whose peer dies (keepalive failure or socket error) is woken
  and reaped rather than parked forever, so a blocking call cannot leak a
  connection slot against the maxclients cap (ADMISSION.md #137); the reaping path
  and its cause-tagged metric are already specified in CONNECTION_LIFECYCLE.md
  (#140), and this spec only states what that reaper must clean up for a blocked
  client. Reaping a blocked client removes its entries from every per-shard wait
  queue it was registered on, on the owning cores, with no cross-core scan.

## Open questions

- Timer precision vs cost: the wheel cadence backing blocking timeouts and the
  acceptable wakeup error, tuned on the harness (#8).
- Wait-queue storage per shard (per-key list vs a shared structure) and its memory
  under many simultaneous blocked clients on one hot key.
- Whether WAIT shares the same registry timer path as key-blocking or runs on a
  separate replication-ack signal.

## Acceptance and test hooks

- A BLPOP on an empty key parks and is woken in FIFO order by a later LPUSH; two
  blocked clients are served oldest-first
  [redis-blocking-fifo-wakeup-fairness][redis-blocking-commands-exist-and-block].
- BLPOP with timeout 0 blocks indefinitely until data or disconnect; a non-zero
  timeout returns null within a small bound of the requested seconds
  [redis-blpop-timeout-double-zero-forever] (a timeout-precision test).
- A cross-shard BLMOVE wakes the waiter on its home core via the coordinator and
  the element is claimed exactly once (a cross-shard wakeup test).
- BLPOP inside MULTI/EXEC returns immediately as non-blocking instead of parking
  the EXEC [redis-blocking-noop-inside-multi-exec] (a no-block-in-tx test).
- XREAD BLOCK with `$` delivers only entries added after the block and ignores
  pre-existing ones [redis-xread-block-semantics-dollar-id] (a dollar-id test).
- A blocked client on a dead peer is reaped, its wait-queue registrations removed,
  and the maxclients slot freed (a blocked-client reap test, #140/#137).

## References

- ADR-0002; issues #29, #19, #137, #140, #15, #86, #8, #1;
  specs COORDINATOR.md, ADMISSION.md, TRANSACTIONS.md, CONNECTION_LIFECYCLE.md,
  PROTOCOL.md, OBSERVABILITY.md.
- Claims: [redis-blocking-commands-exist-and-block],
  [redis-blocking-fifo-wakeup-fairness], [redis-blocking-noop-inside-multi-exec],
  [redis-blpop-timeout-double-zero-forever], [redis-xread-block-semantics-dollar-id],
  [redis-wait-not-strongly-consistent].
