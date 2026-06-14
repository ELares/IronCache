# Design: Cross-shard coordinator: topology, txid ordering, MGET/MSET atomicity, and back-pressure

Issue: #107. Decisions: ADR-0002 (shared-nothing thread-per-core), ADR-0010
(transaction and scripting surface scope). Related: #29 (parent coordinator +
txn-surface umbrella), #109 (apply ADR-0010 to the coordinator), #19/TRANSACTIONS.md,
#130/BLOCKING_COMMANDS.md, #137/ADMISSION.md, runtime-bakeoff.md (#26) crossover.

## Goal and scope

Shared-nothing thread-per-core makes single-key GET/SET lock-free, but every
multi-key command (MGET, MSET, MULTI/EXEC across shards, Pub/Sub fan-out) spans
shards owned by different cores, and a core cannot reach into another core's map
[dragonfly-shard-formula]. This spec defines the coordination path that preserves
the lock-free single-shard fast path, gives defined atomicity for cross-shard
operations, and applies back-pressure without deadlock. Out of scope: the
transaction command surface itself (TRANSACTIONS.md #19), blocking-command parking
(BLOCKING_COMMANDS.md #130), and the multi-node story.

## Design

### Coordinator placement and topology

- Each connection lives its whole life on one home core (ADR-0002); a per-connection
  async task on that core doubles as the cross-shard coordinator, fanning out
  per-shard subcommands and reassembling replies, adapting Dragonfly's connection
  fiber [dragonfly-coordinator-fiber] to a Rust stackless async task. Stackless
  tasks cut per-connection memory versus a stackful fiber, serving the minimal-memory
  tenet.
- Fan-out uses bounded MPSC channels, one inbound queue per shard core: the
  coordinator pushes a work item carrying its txid and the key subset for that shard;
  the owning core drains its queue, executes in its serialized single-threaded
  context (no lock, ADR-0002, [glommio-locks-never-necessary]), and returns a reply
  on a per-request response channel. The coordinator reassembles replies into the
  client-visible order the command expects (e.g. MGET preserves argument order).

### Global txid order and per-shard ordered apply

- A global atomic counter assigns each multi-shard operation a monotonic txid (the
  VLL scheme [dragonfly-vll-citation]). Each shard keeps an ordered apply queue
  keyed by txid, so a single-shard op needs no lock and multi-shard hops are
  deadlock-free by txid order: every shard applies competing transactions in the
  same global order, so no two coordinators can each hold one shard and wait on the
  other. The txid counter is the only shared hot-path cell and is touched once per
  multi-shard op, not per key or per single-shard GET/SET (which never allocate a
  txid and stay on the lock-free fast path).
- A transaction declares its full key set up front; the coordinator splits it by
  `k = HASH(KEY) % N` [dragonfly-shard-formula] into the participating shards and
  enqueues at the assigned txid on each. A shard applies the op only when it reaches
  the head of its txid-ordered queue, which is what makes a multi-shard batch apply
  as one atomic unit relative to other transactions.

### MGET/MSET atomicity

- MGET and MSET execute per shard atomically: each participating shard runs its
  key subset in its serialized context, and the coordinator gathers the parts.
  There is no global cross-shard snapshot and no stop-the-world barrier: MSET is
  visible per shard as each shard applies, matching Redis Cluster multi-key
  semantics rather than a single-instant global commit. This avoids a hop barrier
  on the common multi-key path; transactions that need cross-shard isolation use
  the txid-ordered apply above (TRANSACTIONS.md #19).

### Back-pressure and the cost crossover

- Per-shard channels are bounded: when a shard's inbound queue is full the
  coordinator awaits a slot rather than dropping work or growing unbounded memory,
  so a hot or slow shard throttles its producers instead of exhausting memory. The
  await-vs-reject policy (block the coordinator vs reply -BUSY when the queue stays
  full past a threshold) is an open knob below; either way memory is bounded.
- The coordinator hop costs roughly one context switch (~5us) plus channel
  latency per remote shard, and a context switch is more expensive than an io_uring
  op (<4us) [glommio-context-switch-vs-io], so the hop is the dominant cost on
  multi-key traffic. runtime-bakeoff.md (#26) measures throughput and tail latency
  at multi-key fractions of 0%, 5%, 25%, 50% to locate the crossover where this
  hop cost erases the per-core win and a shared concurrent map would do better;
  that crossover is an input to the #23/#19 and #24 decisions.

### Surface realized (ADR-0010 cross-reference)

- This coordinator realizes ADR-0010's scoped transaction surface: single-shard
  MULTI/EXEC/WATCH is the lock-free fast path run wholly on the owning core, and
  multi-shard transactions hop in txid order through this coordinator. Lua
  (EVAL/EVALSHA/SCRIPT) and Functions (FUNCTION/FCALL) are a Tier 4 non-goal
  because server-blocking script execution [functions-redis-7.0] is incompatible
  with thread-per-core; native atomic ops (#23) serve the common atomic use cases.
  This spec references ADR-0010 and does not re-decide the surface (discharging
  #109); the command semantics live in TRANSACTIONS.md (#19). Redis runs every
  command on one thread [redis-command-execution-single-threaded], making its
  MULTI/EXEC trivially atomic; IronCache reconstructs that atomicity over shards
  here instead of inheriting the single-thread ceiling.

## Open questions

- Back-pressure policy: await on a full shard channel (bound latency to fairness)
  vs reply -BUSY past a depth/time threshold (bound coordinator stall); decided on
  the harness (#8).
- Bounded channel depth per shard, and whether depth scales with shard fan-in.
- Pub/Sub topology: a dedicated channel-broker shard vs broadcast to all shards
  (#29 open decision), measured against the same hop budget.
- The exact multi-key fraction at which a shared map (papaya) wins, from
  runtime-bakeoff.md (#26) and the #24 bake-off.

## Acceptance and test hooks

- Single-key GET/SET never allocates a txid and never enters a shard channel
  (a fast-path assertion); only multi-shard ops take the coordinator path.
- MGET preserves argument order across shards; MSET is observed per shard with no
  global barrier (a cross-shard ordering test).
- Two coordinators issuing overlapping multi-shard batches in opposite key order
  do not deadlock: both apply in txid order on every shared shard (a
  deadlock-freedom test).
- A saturated shard channel back-pressures its coordinators (await or -BUSY per
  the chosen policy) and total queued memory stays bounded (a back-pressure test).
- The coordinator-hop cost and the multi-key-fraction crossover are recorded from
  runtime-bakeoff.md (#26) at 0/5/25/50% multi-key.

## References

- ADR-0002, ADR-0010; issues #29, #109, #19, #130, #137, #23, #24, #26, #8, #1;
  specs TRANSACTIONS.md, BLOCKING_COMMANDS.md, ADMISSION.md, RUNTIME.md,
  runtime-bakeoff.md.
- Claims: [dragonfly-coordinator-fiber], [dragonfly-vll-citation],
  [dragonfly-shard-formula], [glommio-locks-never-necessary],
  [glommio-context-switch-vs-io], [redis-command-execution-single-threaded],
  [functions-redis-7.0].
