# Design: Jepsen + Elle consistency test plan for clustering and replication

Issue: #99. Decisions: ADR-0026 (async default plus WAIT; strong consistency is
opt-in, never a tax on every write), ADR-0003 (determinism / Env seam). Related:
#73/CONTROL_PLANE.md (the Raft control plane this gates), #68 (clustering
umbrella and the DST seed convention), #12 (the no-write-loss-in-async non-goal
this enforces), #75 (slot migration), #78 (per-shard quorum tier), #95/TESTING.md
(parent correctness stack), #100 (fault-injection harness).

## Goal and scope

No IronCache mode that spans more than one node may describe what it guarantees
until a black-box consistency test says so. This is that test: a Jepsen suite,
checked by Elle, run per multi-node tier, wired as the gate on every public
consistency claim. The motivation is direct. The 2020 Jepsen analysis of
Redis-Raft found 21 distinct issues, including split-brain with lost updates,
stale and aborted reads, and total data loss on failover or membership change
[jepsen-redis-raft-21-issues], and Redis itself is explicit that WAIT does not
make the store strongly consistent [redis-wait-not-strongly-consistent].
IronCache ships two tiers with two different promises (an async default per
ADR-0026 and an opt-in per-shard quorum tier per #78), so each is tested and
reported on its own. Scope: the harness, the fault catalog, the checker
configuration, the slot-migration scenario, and the gate. Out of scope: the
consensus implementation (#73, #78) and the generic seed/fault mechanics (#100).

## Design

### Checker: Elle over list-append and register histories

- Elle is the checker, not a bespoke linearizability tool. It is a black-box,
  near-linear cycle-detection checker over Adya-style dependency graphs that
  reports G0, G1a, G1b, G1c, G-single, and G2 [elle-cycle-detection-anomalies],
  so it grades histories without instrumenting IronCache internals. Knossos-style
  linearizability checking is rejected as the primary engine: it is exponential
  in concurrency where Elle is near-linear [elle-cycle-detection-anomalies].
- Two workloads run per tier. A list-append workload (RPUSH/LRANGE over many
  keys) gives Elle the append/read structure its anomaly taxonomy is built on; a
  register workload (SET/GET/INCR per key) exercises last-writer-wins and the
  acknowledged-write contract, and is where lost-update shows up. Histories carry
  per-op invoke/ok/info/fail so a crashed or timed-out op is recorded as
  indeterminate, not as a loss.

### Fault catalog

- The nemesis catalog is the one that reproduced the Redis-Raft failure classes
  [jepsen-redis-raft-21-issues]: network partitions (symmetric, asymmetric, and
  single-node isolation), process pauses (SIGSTOP/SIGCONT), process kills
  (SIGKILL plus restart), clock skew (NTP step and monotonic offset), and disk
  faults (fsync stall and ENOSPC). Membership changes are first-class faults:
  add-node, remove-node, and replica-to-primary promotion are injected mid-run
  through the same Raft path #73 commits. Partition-only nemesis is rejected as
  insufficient: it would not reach the failover and membership classes.
- Faults are scheduled, seeded, and archived using the DST seed convention from
  #68, so any failing run replays from a single seed [dst-fdb-tigerbeetle-single-seed].
  The schedule, seed, and command trace are emitted as one reproducible artifact.

### Separate async and quorum suites

- Each tier gets its own suite and its own asserted model, because merging them
  would hide async write loss behind the quorum bar. The async-default suite
  asserts only what ADR-0026 promises: WAIT is a durability floor, not strong
  consistency [redis-wait-not-strongly-consistent], so the suite records the
  observed Elle anomaly set and the acknowledged-write loss window rather than
  asserting linearizability. The quorum suite (#78) asserts no acknowledged write
  is lost and grades for the stronger model under the same fault catalog.
- The async suite is therefore a characterization gate (its job is to document the
  loss window the #12 non-goal admits, not to fail on async loss), while the
  quorum suite is a pass/fail correctness gate.

### Slot migration under partition

- The headline scenario injects a partition in the middle of an online slot
  migration (#75): a partition is started while a slot is moving between owners,
  then healed, and Elle plus a key-census check assert no acknowledged write is
  lost or duplicated across the slot move and no two nodes claim the slot as owner
  at one config epoch (the CONTROL_PLANE.md #73 linearizable-ownership hook).
  Migration tested only on a healthy cluster is rejected: partition-during-move is
  the realistic failure, and a healthy-only test proves nothing here.

### The 21-class acceptance bar and the gate

- The documented bar before any consistency level is published is clearing the 21
  Redis-Raft failure classes [jepsen-redis-raft-21-issues] under the full fault
  catalog. A passing run is wired as the precondition referenced by the
  no-consistency-claim-without-its-test non-goal (#12) and by the CONTROL_PLANE.md
  #73 correctness bar. Publishing a claim and testing later is rejected: that is
  exactly the #95 posture this issue commits against.

## Open questions

- Async-default: assert "no anomaly worse than read-committed/causal," or only
  record the observed Elle anomaly set without a pass bar.
- Quorum tier: assert strict serializability cluster-wide, or linearizable
  per key only.
- Clock-skew magnitude and source (NTP step versus monotonic offset) for the
  HLC-sensitive paths.
- CI cadence: per-merge smoke versus a nightly long-horizon soak, and how much of
  the fault catalog each tier runs.
- Whether the harness drives real nodes only, or also the DST simulator (#100) as
  a faster pre-Jepsen filter sharing the same seed format.

## Acceptance and test hooks

- The harness drives a real multi-node IronCache cluster under partitions,
  pauses, kills, clock skew, disk faults, and membership changes.
- Elle checks every list-append and register history and reports the anomaly
  taxonomy [elle-cycle-detection-anomalies].
- The async-default and quorum tiers each have their own suite and asserted
  model; the async suite records its loss window, the quorum suite asserts no
  acknowledged-write loss.
- A slot-migration-under-partition run asserts no acknowledged write is lost or
  duplicated across the slot move and no epoch shows two slot owners (#75, #73).
- Clearing the 21 Redis-Raft failure classes is the documented bar before any
  consistency level is published [jepsen-redis-raft-21-issues].
- A passing run is the gate referenced by the #12 non-goal and the #73
  correctness bar; failing runs emit a single-seed reproducible artifact
  [dst-fdb-tigerbeetle-single-seed].

## References

- ADR-0026, ADR-0003; issues #99, #73, #68, #12, #75, #78, #95, #100, #1
  (vision); specs CONTROL_PLANE.md, TESTING.md.
- Claims: [jepsen-redis-raft-21-issues], [elle-cycle-detection-anomalies],
  [redis-wait-not-strongly-consistent], [dst-fdb-tigerbeetle-single-seed].
