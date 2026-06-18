# Design: Graceful shutdown contract (SHUTDOWN, SIGTERM/SIGINT, drain, save-on-exit, orchestrator grace)

Issue: #139. Decisions: ADR-0014 (ephemeral default, opt-in snapshot/warm-restart),
ADR-0009 (behavioral equivalence for the Redis-compatible knobs), ADR-0002
(shared-nothing, a connection lives its whole life on one core). Related: #62
(mmap warm-restart, which owns the SIGUSR1 trigger and explicitly rejects a RESP
admin trigger), #58/PERSISTENCE.md (durability tiers and the forkless snapshot),
#140/CONNECTION_LIFECYCLE.md (connection teardown and per-core reclamation),
#137/ADMISSION.md (the OOM-write rejection surface reused for write refusal),
#86/OBSERVABILITY.md (shutdown phase/metric surface), #83 (binary self-upgrade,
the other planned-restart path). Part of the vision EPIC #1.

## Goal and scope

A single static binary that is redeployed often (ECS task replacement, a
Kubernetes rollout, a `systemctl restart`, an `ironcache upgrade` #83) is told to
stop far more often than it crashes, so the planned-stop path is the one operators
actually exercise. This spec owns that path: the `SHUTDOWN [NOSAVE|SAVE]` command,
SIGTERM and SIGINT as graceful-stop signals, in-flight command completion plus
connection drain plus write refusal during drain, an optional save-on-exit that
composes with the durability tiers rather than inventing a new one, the
WAIT-for-replicas-on-shutdown question, and the exit-code plus grace-timeout
contract an orchestrator needs to stop the process cleanly instead of SIGKILLing
it. It does not re-decide the durability stance (ADR-0014 owns that) or the
warm-restart mechanism and its SIGUSR1/RESP trigger boundary (#62 owns that); it
composes them into one stop sequence. Out of scope: crash recovery (#58), the idle
/dead-peer reaping of connections that are not being deliberately drained (#140),
and the binary swap choreography of self-upgrade (#83).

## Design

### The SHUTDOWN command and its save default

- `SHUTDOWN` with no argument performs a blocking save if and only if at least one
  save point is configured, and otherwise exits without saving
  [redis-shutdown-save-nosave-default]; `SHUTDOWN SAVE` forces a save even when no
  save point is configured and `SHUTDOWN NOSAVE` suppresses the save even when one
  is [redis-shutdown-save-nosave-default]. IronCache keeps this Redis-compatible
  surface (ADR-0009) but binds "a save point is configured" to its own durability
  model: because IronCache is ephemeral by default (ADR-0014), the unconfigured
  binary has no save point, so a bare `SHUTDOWN` on a default deployment exits
  without writing anything, which is the correct cache behavior and matches the
  Redis default-when-no-save-point branch exactly.
- `SAVE` and `NOSAVE` are the only two modifiers v1 honors. The Redis ABORT and
  FORCE/NOW spelling and the `SHUTDOWN` reply-or-no-reply edge are deferred to the
  admin command surface (#150); this spec fixes the save semantics and the drain
  contract, not the full modifier grammar.

### SIGTERM and SIGINT as graceful stop

- SIGTERM and SIGINT both initiate a graceful shutdown rather than terminating the
  process from inside the signal handler. The handler only records the request and
  the received signal; the running event loop observes that on its next turn and
  drives the stop sequence, which is how Redis turns a stop signal into a
  controlled `prepareForShutdown` plus `exit(0)` rather than an abrupt exit
  [redis-sigterm-sigint-graceful-shutdown]. A second stop signal arriving during a
  drain that is already in progress escalates to an immediate exit so an operator
  can always force the issue (the second-signal escalation is an IronCache choice,
  not a Redis-pinned behavior).
- The save decision a signal-triggered stop uses is the same default as a bare
  `SHUTDOWN`: save iff a save point is configured
  [redis-shutdown-save-nosave-default]. There is one stop sequence; the command
  modifiers and the signals are just two doors into it.

### In-flight completion, connection drain, and write refusal

The stop sequence runs in ordered phases, each on the owning core (ADR-0002) so
there is no cross-core stop barrier:

1. **Stop admitting.** The accept path stops taking new connections (it reuses the
   admission accounting in ADMISSION.md #137, just biased to zero), so the
   in-flight set can only shrink.
2. **Refuse writes.** Newly arriving write commands are rejected with a defined
   error rather than executed or silently dropped, reusing the same OOM-write
   rejection surface ADMISSION.md (#137) already defines for at-capacity writes,
   so a draining server looks like a server that refuses writes for a stated
   reason, not one that stalls or half-applies. Reads continue so clients can
   finish a read-mostly transaction and migrate. This write-refusal-during-drain
   is an IronCache design choice layered on the Redis-compatible error surface.
3. **Complete in flight.** Commands already accepted and executing are allowed to
   finish; the server does not abandon a half-run multi-key command.
4. **Drain connections.** Idle and finished connections are closed and their
   per-core resources reclaimed exactly as CONNECTION_LIFECYCLE.md (#140) reclaims
   a reaped connection (read/write buffers, pub/sub and blocking-key registrations,
   the maxclients accounting cell), so drain reuses the existing teardown rather
   than a second code path. Blocked clients (BLPOP/BRPOP/WAIT) are woken with the
   same mechanism CONNECTION_LIFECYCLE.md uses for a dead-peer wake, not parked
   past the grace window.
5. **Persist if asked, then exit** (next two sections).

### Optional save-on-exit composed with the durability tiers

- Save-on-exit is not a new persistence mechanism; it is a final invocation of the
  durability path ADR-0014 and PERSISTENCE.md already define. When the resolved
  save decision is "save," the stop sequence drives the same forkless versioned
  snapshot writer PERSISTENCE.md specifies (constant extra memory, no `fork()`),
  through the same shared io_uring write path, and waits for `durable_offset` to
  cover the snapshot before it reports success. There is no fork-and-COW exit save.
- If durability is configured at a tier (interval or strict, PERSISTENCE.md), exit
  additionally flushes any appended-but-not-yet-durable bytes so the on-exit
  `durable_offset` reflects everything acknowledged. A persistence error on the
  exit save is fail-closed in the same sense PERSISTENCE.md uses: the stop reports
  failure (non-zero exit, below) rather than exiting 0 over an unwritten snapshot,
  so an orchestrator does not record a clean stop that lost data.
- Warm-restart (#62) is the orthogonal planned-restart optimization: its mmap
  state file plus `.meta` is written on graceful shutdown and is what makes the
  next boot warm. This spec invokes that write as part of the exit sequence but
  does not re-decide its trigger; #62 owns the SIGUSR1 trigger and its deliberate
  rejection of a RESP admin trigger, and that boundary is referenced here, not
  reopened. Save-on-exit (a durable snapshot) and warm-restart (a fast-reattach
  heap image) can both run on the same stop and are not the same artifact.

### WAIT for replicas on shutdown

- When the node is a replication source, a graceful stop should optionally hold
  until in-flight writes have reached N replicas before it exits, so a planned
  failover does not silently drop the tail of the write stream. This reuses the
  WAIT durability-floor semantics, not a strong-consistency guarantee: WAIT is a
  replica-acknowledgement floor, not linearizability (JEPSEN_PLAN.md, ADR-0026),
  so shutdown-WAIT inherits exactly that contract and advertises nothing stronger.
  Whether the wait is bounded by the same grace timeout (below) and what the
  default replica count is are open questions; the v1 floor is that shutdown can
  refuse to exit-0 until either the replica floor or the grace timeout is reached.

### Raft node-id scheme: fresh-cluster-only across the in-place upgrade

- A graceful stop persists the Raft committed configuration baseline + log so the
  NEXT boot recovers the cluster's committed membership. That recovery assumes the
  same node-id derivation scheme: a `NodeId` is derived from the cluster announce
  id (CONTROL_PLANE.md), which is stable independent of topology position. A build
  that derived ids from the node's sorted position instead is INCOMPATIBLE with the
  announce-id scheme.
- Consequently a raft-mode node restart (a rollout, a `systemctl restart`, an
  `ironcache upgrade` #83) ACROSS that scheme change is fresh-cluster-only: the
  next boot refuses to start if the recovered committed config is non-empty yet
  disjoint from the topology-derived id set (the in-place-upgrade hazard, which
  would otherwise leave the node out of its own committed voter set -> silent split
  brain). The refusal names the files to remove (`<data_dir>/ironcache-raft-*.log`
  plus the `.cfg` / `.snap` sidecars) for a fresh start, or the operator migrates
  the persisted state. A same-scheme restart (the common case) recovers normally.

### Exit-code and grace-timeout contract for orchestrators

- The process exits `0` only on a fully completed graceful stop (drained, and the
  resolved save, if any, durable). Any failure that left the stop incomplete (save
  error, drain still outstanding at hard deadline, replica floor unmet at the
  deadline) exits non-zero, so a supervisor can distinguish a clean stop from a
  degraded one. The exact non-zero code map is an open question; the contract is
  the 0-iff-clean split.
- The stop sequence is bounded by an internal grace timeout chosen to fit inside
  the two supervisors IronCache is deployed under:
  - **Kubernetes** sends SIGTERM, runs the optional preStop hook first, waits
    `terminationGracePeriodSeconds` (default 30 seconds), then SIGKILLs the
    container if it has not exited [k8s-termination-grace-period-default-30s-sigterm-sigkill].
    IronCache's drain plus any exit save must therefore complete inside the
    operator's configured grace period (and the documentation must tell operators
    to raise `terminationGracePeriodSeconds` when they enable a large exit save),
    or the SIGKILL truncates it.
  - **systemd** sends the stop signal (SIGTERM by default) and, if the unit has
    not stopped within `TimeoutStopSec` (default 90 seconds, from
    `DefaultTimeoutStopSec`), escalates to SIGKILL
    [systemd-timeoutstopsec-default-90s-sigterm-sigkill]. The same internal grace
    budget applies; the documented unit file sets `TimeoutStopSec` to cover the
    worst-case exit save.
- IronCache's own grace timeout is set below the tighter of the two host budgets
  so the process always exits on its own terms (a clean exit code) before the
  supervisor's SIGKILL fires. On reaching its own grace deadline mid-drain it
  performs the resolved save attempt (or skips it for NOSAVE), then exits non-zero
  to signal the truncation, rather than blocking until SIGKILL.

### Shutdown observability

- The stop sequence emits its phase (stop-admitting, refuse-writes, draining,
  saving, waiting-replicas, exiting) and the resolved save decision through the
  OBSERVABILITY.md (#86) registry, and the final exit reason is logged, so an
  operator can see why a stop took the time it did and whether it completed cleanly
  or hit the grace deadline.

## Open questions

- The internal grace-timeout default and whether it is a single budget or split
  per phase (drain vs exit-save vs replica-wait), tuned on the harness (#8).
- The exact non-zero exit-code map (save-error vs drain-timeout vs unmet replica
  floor) an orchestrator can branch on.
- Whether shutdown-WAIT shares the global grace timeout or has its own bound, and
  the default replica count it waits for (interacts with ADR-0026 and #147).
- Whether write refusal during drain returns the literal OOM-write error byte
  string (ADMISSION.md #137) or a distinct shutting-down error tier (interacts
  with the error catalog ERRORS.md #18 and the differential oracle #97).
- The full `SHUTDOWN` modifier grammar beyond SAVE/NOSAVE (ABORT, FORCE/NOW) and
  its reply edge, deferred to the admin command surface (#150).
- Coordinating the stop sequence with the #83 binary swap so a self-upgrade reuses
  this drain rather than a parallel one.

## Acceptance and test hooks

- On a default (ephemeral, no save point) deployment, a bare `SHUTDOWN` exits
  without writing any artifact; `SHUTDOWN SAVE` writes a snapshot anyway and
  `SHUTDOWN NOSAVE` never does, even with a save point configured
  [redis-shutdown-save-nosave-default].
- SIGTERM and SIGINT each drive the full graceful sequence (not an in-handler
  exit), resolve the same save default as a bare `SHUTDOWN`, and a second stop
  signal during an in-progress drain escalates to an immediate exit
  [redis-sigterm-sigint-graceful-shutdown].
- During drain: new connections are refused, new writes get the defined refusal
  error while reads still succeed, in-flight commands complete, and a blocked
  BLPOP client is woken rather than parked past the grace window (reusing the
  #140 teardown and #137 surfaces).
- A configured exit save drives the forkless snapshot path (no fork+COW), advances
  `durable_offset` to cover it, and an injected fsync failure makes the stop
  fail-closed (non-zero exit, no clean exit over unwritten data), per
  PERSISTENCE.md.
- Exit code is 0 iff the stop completed cleanly (drained and any save durable);
  a drain that hits the grace deadline exits non-zero.
- The internal grace timeout completes drain plus exit save before the supervisor
  SIGKILL under both a Kubernetes 30 s `terminationGracePeriodSeconds`
  [k8s-termination-grace-period-default-30s-sigterm-sigkill] and a systemd 90 s
  `TimeoutStopSec` [systemd-timeoutstopsec-default-90s-sigterm-sigkill]; a sample
  Pod spec (with preStop) and a sample unit file are shipped and tested.
- A replication source with shutdown-WAIT enabled holds exit until the replica
  floor or the grace timeout is reached, and advertises only the WAIT floor, not
  strong consistency (ADR-0026, JEPSEN_PLAN.md).
- The stop sequence emits phase and exit-reason telemetry through the #86 registry.

## References

- ADR-0014, ADR-0009, ADR-0002, ADR-0026; issues #139, #62, #58, #140, #137, #86,
  #83, #147, #150, #18, #97, #8, #1; specs PERSISTENCE.md, CONNECTION_LIFECYCLE.md,
  ADMISSION.md, OBSERVABILITY.md, JEPSEN_PLAN.md, ERRORS.md.
- Claims: [redis-shutdown-save-nosave-default],
  [redis-sigterm-sigint-graceful-shutdown],
  [k8s-termination-grace-period-default-30s-sigterm-sigkill],
  [systemd-timeoutstopsec-default-90s-sigterm-sigkill].
