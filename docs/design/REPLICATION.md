# Design: Offset-based async replication with an adaptive disk-spillable backlog

Issue: #77. Decisions: ADR-0026 (asynchronous primary/replica is the default, WAIT
is a bounded durability floor not a consistency mode; the loss window is named and
guardrails ship on). Related: #76 (the consistency-model decision ADR-0026
records), SNAPSHOT.md (#60, the forkless diskless full-sync stream this reuses as
its bulk channel), CONTROL_PLANE.md (#73, where consensus and failover live, out
of scope here), MIGRATION.md (#75, whose per-slot mutation stream shares this
offset-cursor shape), DISTRIBUTION.md (#68, clustering umbrella), #147 (replica-
read contract), #78 (the opt-in strongly-consistent tier layered on this
baseline).

## Goal and scope

Primary-to-replica replication that survives a brief disconnect without forcing a
full resync, sizes its catch-up buffer to the write rate instead of a fixed RAM
cap, and shares one streaming path with the forkless snapshot. Concretely: a
(replid, byte-offset) stream with partial resync on reconnect, an adaptive
backlog ring that spills to disk past a RAM cap, a PSYNC2-generalized checkpoint
ring so promotion does not cascade full resyncs, and an opt-in diskless
dual-channel full-sync that reuses the SNAPSHOT.md stream. Scope is asynchronous
replication only, which ADR-0026 fixes as the default; the model is best-effort,
not CP, and acknowledged-but-unreplicated writes can be lost on failover
[redis-cluster-async-replication]. Consensus, failover, and the
strong-consistency tier are out of scope (CONTROL_PLANE.md #73, #78), as is the
read contract (#147).

## Design

### Stream identity: (replid, byte-offset)

The replication stream is identified by a (replid, byte-offset) cursor: replid
names the history lineage, offset is the byte position in that lineage. This is
the PSYNC2 cursor model [redis-psync2-secondary-replid], adopted so existing
Redis replication tooling and any PSYNC2-aware client reason about IronCache
unchanged. A logical operation-sequence number is rejected: it is simpler but
breaks resume compatibility with the byte-offset semantics tooling expects. A
replica that reconnects presents its (replid, offset); if that offset is still
covered by the primary's backlog the primary replies with a partial resync and
streams only the missing tail, avoiding a full transfer.

### Adaptive, disk-spillable backlog ring

Redis ships a fixed in-RAM replication backlog ring whose default size is 1mb
[redis-repl-backlog-size-default], freed by a TTL after the last replica detaches
whose default is 3600 seconds [redis-repl-backlog-ttl-default]. That fixed ring is
rejected on both ends of the load curve: under a high write rate 1mb covers far
too short an offset window, so a replica that blinks falls off it and pays a full
resync; under idle it pins RAM that nothing needs. IronCache keeps the offset
semantics and the detach-TTL idea but makes the ring adaptive: it grows with the
write rate so the covered offset window stays useful under load, and past a RAM
cap it spills older ring segments to disk rather than dropping them, so the
partial-resync window is bounded by disk, not by a small RAM constant. The cost is
a disk-spill path to maintain, accepted because it is what eliminates avoidable
full resyncs. The adaptive sizing inputs (write throughput, replica RTT, or both)
and the spill segment format (reuse the snapshot block format or a dedicated
segment file) are open questions below.

### PSYNC2-generalized checkpoint ring for promotion

When a replica is promoted it must keep serving its own downstream replicas
without forcing them into full resync. Redis solves the one-step case by keeping a
secondary replication id so a promoted replica and its old primary share history
[redis-psync2-secondary-replid]. IronCache generalizes that single prior id into
an N-entry checkpoint ring of (replid, offset) handoff points, so a chain of
failovers stays resumable rather than only the most recent one. A downstream
replica presenting an offset under any retained checkpoint gets a partial resync
across the promotion boundary. The cost is a few bytes of metadata per retained
checkpoint; the ring depth (how many historical replids to keep) is an open
question. A single prior replid is rejected as the design point because it cannot
survive chained promotions.

### Diskless dual-channel full-sync reuses the snapshot path

When a replica is too far behind for partial resync it takes a full sync, and that
full sync reuses the forkless diskless streaming path of SNAPSHOT.md (#60) rather
than staging an RDB to disk. The snapshot is the bulk channel; live writes that
arrive during the transfer accumulate on the backlog channel keyed to the
snapshot's end offset; when the bulk channel finishes the replica switches to
tailing the offset stream from that end offset with no second copy of the data.
This is the dual-channel split Valkey ships disabled by default
[valkey-dual-channel-default-off] but which measurably shortens full sync by
carrying snapshot bulk and backlog in parallel [valkey-dual-channel-syncgain];
IronCache implements the two-channel split and keeps it opt-in, with the default
(on or off) an open question. A disk-staged RDB is rejected as the default
transport: it adds a disk write-then-read on the critical sync path. On the
receiver side IronCache inherits the SNAPSHOT.md design that loads into a separate
arena with a hard memory cap and an atomic pointer switch, deliberately avoiding
the Redis receiver defaults where diskless load is disabled because swapdb doubles
memory and an IO error aborts [redis-repl-diskless-load-default-disabled].

### Overload protection

Under CPU saturation the primary sheds replication load rather than stalling
foreground traffic, borrowing KeyDB's CPU-based load-shedding signal
[keydb-fastsync-overload]. A shed replica may drop to a partial (or, if it falls
off the window, full) resync once headroom returns, which is preferable to letting
replication starve the request-serving core. Unbounded buffering is rejected: it
trades a stall for an eventual memory spike. The CPU threshold and the hysteresis
that prevents flapping between shedding and resuming are open questions.

### Defaults

Starting defaults track the pinned upstream values so operators porting from Redis
find familiar behavior: ping-replica period 10 seconds
[redis-repl-ping-replica-period-default], diskless-sync on, matching the Redis 7.0
default [redis-repl-diskless-sync-default-rrc], and a diskless-sync delay of 5
seconds to batch multiple joining replicas into one transfer
[redis-repl-diskless-sync-delay-default]. The async default itself, the WAIT
durability floor, and the three guardrails (replica-read-only, min-replicas-to-
write, min-replicas-max-lag) are fixed by ADR-0026 and inherited here rather than
re-decided [redis-cluster-async-replication]; the numeric guardrail defaults live
with that ADR.

## Open questions

- Backlog disk-spill format: reuse the SNAPSHOT.md block format or a dedicated
  segment file.
- Checkpoint-ring depth: how many historical (replid, offset) checkpoints to
  retain for chained-failover resume.
- Adaptive backlog sizing inputs: write throughput, replica RTT, or both, and the
  RAM cap at which spill begins.
- Default for dual-channel diskless sync: on or off for IronCache (Valkey ships
  off [valkey-dual-channel-default-off]).
- CPU threshold and hysteresis for load shedding [keydb-fastsync-overload].

## Acceptance and test hooks

- Partial resync succeeds for any disconnect whose offset is still inside the
  adaptive backlog window; the replica streams only the missing tail and takes no
  full transfer.
- Under sustained writes the backlog grows and spills to disk past its RAM cap
  without dropping the offset window, so a reconnect that would have fallen off a
  fixed 1mb ring [redis-repl-backlog-size-default] still partial-resyncs.
- After a chain of promotions, downstream replicas resume via the checkpoint ring
  and avoid full resync across each promotion boundary
  [redis-psync2-secondary-replid].
- Diskless full-sync shares the #60 streaming path with no disk staging and loads
  into a separate arena under a hard memory cap, aborting cleanly on IO error
  without corrupting the live dataset [redis-repl-diskless-load-default-disabled].
- Defaults are wired: ping-replica period [redis-repl-ping-replica-period-default],
  diskless-sync [redis-repl-diskless-sync-default-rrc], diskless-sync delay
  [redis-repl-diskless-sync-delay-default].
- Under CPU saturation the primary sheds replication load and foreground latency
  is protected rather than stalled [keydb-fastsync-overload].
- The async loss window of ADR-0026 is honored and surfaced: an acknowledged write
  not yet replicated can be lost on failover, and WAIT bounds but does not close
  the window [redis-cluster-async-replication].

## References

- ADR-0026; issues #77, #60, #68, #73, #76, #147, #78; specs SNAPSHOT.md,
  CONTROL_PLANE.md, MIGRATION.md, DISTRIBUTION.md.
- Claims: [redis-repl-backlog-size-default], [redis-repl-backlog-ttl-default],
  [redis-psync2-secondary-replid], [redis-repl-ping-replica-period-default],
  [redis-repl-diskless-sync-default-rrc], [redis-repl-diskless-sync-delay-default],
  [valkey-dual-channel-default-off], [valkey-dual-channel-syncgain],
  [keydb-fastsync-overload], [redis-repl-diskless-load-default-disabled],
  [redis-cluster-async-replication].
