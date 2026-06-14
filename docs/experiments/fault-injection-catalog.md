# Experiment: Seeded fault-injection catalog (crash, torn-write, corrupt-manifest, network injectors over the snapshot, warm-restart, and replication recovery paths)

Issue: #100. Provisional decision: ADR-0003 (deterministic single-thread-per-shard runtime behind the Env seam) and ADR-0014 (the durability stance whose tiers this catalog tests); this task builds the seeded fault scenarios on top of those decisions and does not re-open either.

## Provisional decision (already pinned)

Two decisions are pinned and this catalog re-decides neither. ADR-0003 (issue
#31) pins the deterministic runtime and the Env seam: every fault decision is
drawn from the same seeded PRNG that drives runtime scheduling, so a run is fully
described by its seed plus its configuration and any failure replays from one
seed [dst-fdb-tigerbeetle-single-seed]. ADR-0014 (issue #59) pins the durability
stance whose guarantees this catalog turns into tests: an ephemeral default with
an opt-in menu of forkless point-in-time snapshot (#60), mmap warm-restart
(#62), and the segment-plus-manifest durable log (#63). The fault hooks attach to
whatever runtime the DST build-vs-buy outcome (#31) selects, and the scenario
catalog is portable across that choice.

This catalog is the IronCache design for #100: a fixed set of injector classes
crossed over the recovery paths ADR-0014 promises, each scenario seeded so a
failure is reproducible from a single integer. It rejects two alternatives up
front, both stated in #100. It rejects unseeded random-only fault injection: it
finds bugs but cannot reproduce them, which is the property that makes
determinism worth building. And it rejects testing recovery only against clean
shutdowns, because the interesting corruption is exactly the mid-write and
mid-fsync case that a clean shutdown never exercises.

The two load-bearing invariants every scenario asserts, stated once here and
referenced by each path below:

- Recover-to-last-durable-cut: after any injected fault, recovery converges to
  the last durable, manifest-acknowledged state and never to a torn tail. A torn
  incremental tail truncates to the last CRC-valid record with bounded, asserted
  loss; a corrupt or missing base referenced by the manifest rolls back to the
  prior manifest generation or fails closed, never loading partial data (the
  recovery semantics specified in #63).
- Fail-closed-on-persistence-error: when a write or fsync cannot be confirmed
  durable, the node refuses the write and surfaces the error rather than
  acknowledging optimistically. An unconfirmed write is never acknowledged.

## The four injector classes

Each injector is a deterministic decision drawn from the run's seeded PRNG,
attached at the Env seam (disk, network, clock, scheduler) so it composes with
the deterministic interleaving rather than racing against it.

- Crash injector: aborts the process at any scheduled point, including mid-write
  and mid-fsync, followed by a warm restart. The schedule of crash points is
  drawn from the seed, so a given seed crashes at exactly the same instruction
  boundary on replay. This is the injector that makes the mid-write and mid-fsync
  case (the case a clean-shutdown test skips) a first-class, repeatable scenario.
- Torn-write injector: short writes, sub-record torn writes, fsync failures, and
  ENOSPC on the disk path. A torn write leaves a partial record at the tail of an
  active segment; an fsync failure leaves bytes unconfirmed. This injector is the
  primary driver of both invariants: the torn tail must truncate to the last valid
  record, and the unconfirmed fsync must fail closed.
- Corrupt-manifest injector: bit-flips and truncation in already-persisted bytes,
  aimed at the manifest log and at base-segment blocks. A truncated or corrupt
  manifest, or a manifest that references an absent or short base, must trigger
  the rollback-or-fail-closed rule rather than loading a partial dataset. This
  injector also flips bytes inside persisted base blocks so that corruption
  discovered on replay is exercised, not just corruption at the tail.
- Network injector: drops, reorders, duplicates, and partitions on both the
  replication path and the client path. On the replication path it drives the
  replica-failure scenarios; on the client path it stresses the acknowledgement
  contract so that a dropped or delayed ack never lets the node report durability
  it does not have. A clock-skew sub-mode (monotonic vs wall-clock divergence
  across simulated nodes) rides the same seeded schedule, since TTL and durability
  timing must hold under skew.

## The three recovery paths under test

Each injector class is crossed over the three opt-in durability paths from
ADR-0014, so every path has at least one seeded scenario per relevant injector.

- Snapshot path (#60, forkless point-in-time snapshot): a crash injected mid-
  snapshot (during the versioned cut traversal or the serialization stream) must
  leave the live dataset intact and the partial snapshot discardable, never
  half-applied. A torn-write or fsync-failure during snapshot output must fail
  closed rather than emit a snapshot that loads as partial data. The diskless
  full-sync receiver under the network injector (drops, partition mid-stream)
  must abort gracefully into its separate arena without corrupting the live
  dataset.
- Warm-restart path (#62, mmap state file plus pointer fixup): a crash injected
  during the graceful drain or during the `.meta` state-file write must abort the
  warm restart and fall back to a clean cold start with the reason logged, never a
  half-written state file silently reattached. A warm restart attempted over a
  torn segment (torn-write injector having damaged the on-disk tier the restart
  reloads) must detect the damage and refuse the warm path rather than reattach
  corrupt state. The clock-skew sub-mode exercises the warm-restart clock
  invalidation rule (abort on backward wall-clock movement or excessive skew,
  since TTLs are wall-clock relative).
- Replication path: a replica that crashes or partitions mid-stream (crash and
  network injectors on the replication channel) must resume or re-sync to the
  last durable, manifest-acknowledged cut, never to a torn tail, and an
  acknowledgement must never be sent for data that is not yet confirmed durable
  (fail-closed on the replication ack path mirrors the local persistence guard).

The durable log (#63) underlies all three paths: the segment-plus-manifest layout
and its recovery semantics (torn-tail truncation, manifest-generation rollback,
fail-closed on corrupt base) are the shared mechanism every scenario asserts
against.

## Why this is harness-blocked

The catalog cannot be run on paper. It requires the deterministic runtime and Env
seam from ADR-0003 actually built so the injectors can attach at the seam and the
interleaving is reproducible; it requires the snapshot (#60), warm-restart (#62),
and segment-plus-manifest (#63) paths implemented so there is recovery code to
fault; and it requires the conformance suite (#95) as the host harness in which
the seeded runner lives. None of that exists yet, so the scenarios are specified
here and run when those land. The single-seed reproduction the whole catalog
rests on is the established DST property that simulating all I/O and time makes
any bug reproducible from a single seed [dst-fdb-tigerbeetle-single-seed].

## Experiment to run

Runner and seeds:

- One seeded runner inside the conformance suite (#95) that, given a seed and a
  scenario configuration, drives the deterministic runtime and draws every fault
  decision from the same PRNG that drives scheduling. The seed plus the
  configuration is the entire run description, with no shared state and no
  wall-clock dependence.
- A seed library covering the cross product of the four injector classes and the
  three recovery paths, plus the boundary cases #100 calls out by name: torn
  appends to the active segment, a truncated or corrupt manifest log, corruption
  discovered on replay, snapshot interrupted by crash, warm restart over a torn
  segment, and a replica that crashes or partitions mid-stream.

What each scenario asserts (the pass condition, not a measured number):

- After the injected fault and recovery, the dataset equals the last durable,
  manifest-acknowledged cut. A torn incremental tail has truncated to the last
  CRC-valid record with the loss bounded and asserted; a corrupt or missing base
  has rolled back to the prior manifest generation or failed closed; recovery
  never lands on a torn tail (#63).
- Every write that could not be confirmed durable was refused and surfaced as an
  error, never acknowledged optimistically (fail-closed).
- The warm-restart and snapshot paths aborted cleanly into a safe fallback (cold
  start, or live dataset intact) on any invalidation or mid-operation crash,
  rather than reattaching or loading partial state.

Logging discipline (the repro contract): the runner records the seed on every
failure AND on the periodic green runs. A failing seed is the entire repro:
replaying it reconstructs the identical interleaving and fault sequence. No timing
or throughput numbers are recorded in this doc; every assertion is a recovery-
correctness or fail-closed predicate, not a benchmark.

Decision rule:

- The fault-injection gate passes only if every scenario in the seed library
  upholds both invariants (recover-to-last-durable-cut and fail-closed) on its
  path. Any scenario that recovers to a torn tail, loads partial data, or
  acknowledges an unconfirmed write fails the gate and is filed with its seed.
- Coverage is complete only when the disk, network, clock-skew, and process-crash
  injectors all run seeded on the deterministic runtime, AND the snapshot,
  warm-restart, and replication paths each have at least one passing seeded
  scenario, AND the hooks bind to the runtime chosen in #31 and run in CI (the
  #100 done-when list).

## What would change the decision

- A recovery path that cannot satisfy recover-to-last-durable-cut under a
  realizable fault sequence, which would force a change to the #63 segment or
  manifest design (for example the CRC granularity or the manifest-generation
  retention) rather than a change to the test.
- A fault sequence under which fail-closed cannot be upheld without an
  unacceptable latency cost on the durable path, which would feed the durability-
  tier fsync-policy decision rather than relax the invariant.
- A class of corruption the four injectors do not reach (for example a fault in a
  layer below the Env disk seam), which would extend the injector catalog rather
  than narrow the guarantee.
- The seeded runner proving too slow to run the full injector-by-path cross
  product per PR, which would split it into a per-PR smoke subset and a scheduled
  full-matrix soak, keeping single-seed reproduction for both.

## References

- ADR-0003: deterministic single-thread-per-shard runtime and the Env seam the
  injectors attach to (issue #31). ADR-0014: durability stance and the opt-in
  snapshot, warm-restart, and durable-log tiers this catalog tests (issue #59).
- Issues: #100 (this catalog), #31 (the determinism decision the hooks bind to),
  #95 (the conformance suite that hosts the runner), #63 (segment-plus-manifest
  durable log and its recovery semantics), #60 (forkless point-in-time snapshot
  and diskless full-sync), #62 (mmap warm-restart), #160 (the replay-contract
  verification this catalog's single-seed repro depends on), #1 (vision EPIC).
- Specs: docs/design/PERSISTENCE.md (the durability umbrella, #58),
  docs/design/TESTING.md (the correctness stack and DST path, #95),
  docs/design/JEPSEN_PLAN.md (the sibling consistency fault plan, #99).
- Claims (resolved via docs/prior-art/claims.yaml): [dst-fdb-tigerbeetle-single-seed].
