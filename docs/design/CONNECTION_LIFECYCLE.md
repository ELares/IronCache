# Design: Idle-connection timeout, TCP keepalive, and dead-peer reaping

Issue: #140. Decisions: ADR-0002 (shared-nothing, a connection lives its whole
life on one core), ADR-0009 (behavioral equivalence for the Redis-compatible
knobs). Related: #15/PROTOCOL.md (connection state machine), #25/RUNTIME.md
(per-core accept and shard-pinned state), #137/ADMISSION.md (admission caps),
#86/OBSERVABILITY.md (metrics), #51/EXPIRATION.md (background reclamation cadence).

## Goal and scope

A long-lived server accumulates connections that go idle, wedge, or die without a
clean close, and each one pins per-connection state on its owning core. This spec
covers reclaiming them: the Redis-compatible idle `timeout`, TCP keepalive for
dead-peer detection, and the reaping of dead or wedged connections (replicas and
blocked clients included), plus the metric that makes the reaping visible. It is
the complement of ADMISSION.md: #137 bounds how many *new* connections are
admitted, this reclaims *existing* ones that no longer carry useful work. Out of
scope: the per-frame parser limits (#138) and the maxclients cap itself (#137).

## Design

### Idle-connection timeout

- A Redis-compatible `timeout` knob closes a client connection after it has been
  idle (no command) for the configured number of seconds, defaulting to `0` which
  disables idle disconnection [redis-timeout-default-0]. The default matches Redis
  so an unconfigured IronCache does not surprise existing deployments by dropping
  idle clients (ADR-0009); an operator opts into reaping by setting a non-zero
  value (live-reloadable per CONFIG.md). Idleness is tracked from a per-connection
  last-activity timestamp on the owning core (ADR-0002), so the check needs no
  shared structure.

### TCP keepalive and dead-peer detection

- A Redis-compatible `tcp-keepalive` knob enables `SO_KEEPALIVE` and sets the idle
  time before the first TCP keepalive probe, defaulting to 300 seconds
  [redis-tcp-keepalive-default-300]: when non-zero the kernel sends keepalive
  probes to an otherwise-silent peer (the live peer ACKs them), which detects a
  peer that vanished without a FIN (power loss, network partition) and keeps
  middlebox connection state alive. With Redis's probe-count/interval settings a
  dead connection is torn down after roughly double the idle time
  [redis-tcp-keepalive-default-300] (this is Redis's probe configuration, not an
  inherent Linux property). Keepalive catches the silently-dead peer that the idle
  `timeout` would only catch much later (or never, if `timeout` is 0).

### Reaping dead and wedged connections

- The two mechanisms above plus an explicit reaper converge on one outcome: a
  connection that is idle past `timeout`, or whose peer keepalive has failed, or
  whose socket the kernel reports as errored, is closed and its resources freed.
  IronCache extends reaping beyond ordinary clients (a deliberate design choice,
  not a claimed Redis behavior):
  - A **replica** link that stops responding to keepalive/pings is reaped so a
    dead replica does not hold a replication buffer open (the per-class
    output-buffer budget is ADMISSION.md #137; this is the connection-level
    teardown).
  - A **blocked client** (BLPOP/BRPOP/WAIT and similar) is woken and reaped on
    timeout or dead-peer detection rather than parked forever, so a blocking call
    cannot leak a connection slot against the maxclients cap (#137).
- The reaper runs as a periodic per-core sweep (no cross-core scan, ADR-0002),
  reusing the background-reclamation cadence the shard already runs for TTL
  (#51) rather than a dedicated global thread.

### Resource reclamation on close

- Because a connection lives its whole life on one core (ADR-0002, RUNTIME.md
  #25), closing it is a local operation: the per-connection read/write buffers,
  any subscription/registration in that core's pub/sub or blocking-key tables, and
  the connection's slot in the per-core accounting (the `maxclients` cell, #137)
  are released on the owning core with no cross-core coordination. A reaped
  connection decrements the same relaxed admission counter a clean close does, so
  the global cap stays accurate.

### Observability

- Every reaped connection increments a metric tagged by cause (idle-timeout,
  keepalive-dead-peer, socket-error, blocked-client-timeout), feeding the
  admission/lifecycle telemetry in OBSERVABILITY.md (#86), so an operator can see
  whether connections are being dropped and why rather than guessing.

## Open questions

- The reaper sweep interval and whether it is a fixed cadence or scales with the
  live-connection count (latency of reclamation vs sweep cost), tuned on the
  harness (#8).
- Whether IronCache exposes the finer Linux keepalive knobs (probe count and
  inter-probe interval) or only the single Redis `tcp-keepalive` period for
  compatibility; default to the single Redis knob.
- Whether `CLIENT NO-EVICT`/`CLIENT NO-TOUCH` style exemptions interact with
  idle reaping (admin command surface is #150).

## Acceptance and test hooks

- With `timeout 0` (default) an idle client is never disconnected; with a non-zero
  `timeout` an idle client is closed after the configured seconds
  [redis-timeout-default-0] (an idle-reap test).
- With `tcp-keepalive 300` (default) a peer killed without a FIN is detected and
  its connection reaped, and the slot is returned to the `maxclients` budget
  [redis-tcp-keepalive-default-300] (a dead-peer test using a blackholed socket).
- A blocked client (BLPOP) on a dead peer is woken and reaped rather than parked
  indefinitely; the connection slot is freed (a blocked-client reap test).
- Each reap increments the cause-tagged metric (#86); a reaped connection
  decrements the admission counter exactly as a clean close does (#137).

## References

- ADR-0002, ADR-0009; issues #15, #25, #137, #138, #86, #51, #8, #150;
  specs PROTOCOL.md, RUNTIME.md, ADMISSION.md, OBSERVABILITY.md, CONFIG.md.
- Claims: [redis-timeout-default-0], [redis-tcp-keepalive-default-300].
