<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Design of record: the live streamed-handoff cutover (#391 Phase 2c completion)

Status: DESIGNED, implementation DEFERRED (P2). This is the vetted, data-safe design
for completing the #391 streamed old->new upgrade handoff with a live serve-flip. It
was produced by a 3-design panel plus an adversarial data-loss review of each, then
synthesized. The already-merged pieces (`upgrade/stream.rs` core, `upgrade/drive.rs`
driver) are reused; this doc covers the deferred live-datapath integration.

## Why this is deferred (read this first)

The tmpfs handoff (#390) already ships and works: an upgrade costs a process restart
plus a fast tmpfs load, not a slow disk reload. Once the streamed live cutover honors
every data-safety constraint, it STILL has a write-quiesce outage plus a stop-before-
start flip gap, so it is NOT zero-downtime. Its only real win over #390 is eliminating
the tmpfs load time and overlapping the transfer, i.e. it shortens an already-short
window. That marginal availability gain costs an XL, medium-to-high-risk build whose
correctness hinges on two primitives (the atomic cut, the cross-process commit) that
all three initial designs got WRONG, and which can only be proven with a standing
Linux 2-process/cluster CI harness we do not yet run continuously. There is no P0 here
and the production-readiness audit lists no upgrade-downtime blocker.

Sequencing: (a) land this design; (b) instrument and MEASURE actual tmpfs-restart
downtime against a real SLO or customer pain; (c) only if that measurement shows the
tmpfs window is a genuine miss, execute PR-1..PR-6 below behind the `handoff_socket`
gate with the hero tests as the GA bar. If built anyway (e.g. as a determinism / zero-
interruption differentiator), do NOT claim "zero-downtime": claim "in-memory fast-
cutover with a bounded sub-second write pause and no acknowledged-write loss", which is
what this design can actually prove.

## The single invariant (enforced structurally, not by narration)

At every instant, exactly one process holds write authority, and every acknowledged
write exists in at least one live-recoverable or fsynced-durable copy.

The keystone that makes it provable: the per-shard replication ring is ALWAYS-ON, and
every mutator assigns its offset and appends to the ring in the SAME non-`await`
critical section in which it applies to the store. This is free on a single-threaded
shard (`Rc<RefCell<ShardStore>>` on a per-shard LocalSet) and every proof below rests
on it.

## The cutover protocol (ordered, explicitly-safe phases)

Terminology: `F` = freeze floor offset, `E` = end/cut offset.

- **Phase 0 SPAWN.** OLD (or systemd) spawns NEW with `IRONCACHE_HANDOFF_ROLE=receiver`
  and the socket path. OLD binds the handoff socket and KEEPS FULLY SERVING. NEW boots
  with `HandoffPlan=Some`; crucially NEW's client acceptor never starts and a global
  `serving=false` gate rejects every command until Phase 6. NEW's `data_dir` is
  untouched; it writes only to a `staging/` dir.
- **Phase 1 FREEZE + BULK (per shard, on the shard's own thread).** In a single
  synchronous non-`await` step: Arc-COW-freeze every non-empty slot table (O(slots)
  refcount bumps, reusing the #588 `begin_save`/`FrozenSlot` mechanism) AND capture
  `F = ring.head()`. Because it is one thread and one uninterrupted step,
  `frozen == state@F` exactly (no smear). `send_bulk_from_frozen` streams the frozen
  Arcs, yielding between chunks on the same LocalSet so the shard keeps serving;
  concurrent writes COW (`Arc::make_mut`) and are recorded in the ring at `offset > F`.
  `F` is transmitted as the delta floor. The sender runs on the shard thread, so
  nothing `Rc` crosses threads.
- **Phase 2 QUIESCE (write outage begins on OLD; reads still served).** Each shard
  thread sets `loading=true`, latches `E = ring.head()`, and thereafter REJECTS every
  mutator AT THE COMMIT POINT (not at dispatch entry) with `-LOADING`, no ack and no
  buffer. Because the `-LOADING` check, the offset assign, and the ring append are one
  critical section on the single shard thread, `acked implies offset <= E` with zero
  in-flight tail. The gate covers ALL mutators: normal writes, MULTI/EXEC, Lua, active
  TTL sweeps, S3-FIFO eviction removals, replication-apply.
- **Phase 3 FINAL-DELTA.** OLD ships `ring[F+1 .. E]`. If the always-on ring ever
  wrapped (overflow) during Phases 1-2 that is FAIL-CLOSED -> ABORT, never a silent
  drop. The receiver applies each op offset-gated (`apply iff offset > F`), asserts
  `first_delta == F+1`, asserts contiguity (no gap), asserts `applied == E`.
- **Phase 4 VERIFY + STAGE-PERSIST (receiver-authoritative).** NEW verifies per shard:
  CRC envelope, magic/version, db-count, contiguity, `applied == E`, and metadata
  fidelity (absolute expiry timestamps and CAS/version stamps survive `insert_object`,
  never rebased to load time). NEW then FSYNCS a fresh snapshot of every adopted store
  to `staging/`. Only when ALL shards pass verify AND are fsynced does NEW send
  `PREPARED`. Any failure -> NEW sends `ABORT` (or dies), discards staging, exits
  without ever serving.
- **Phase 5 DECISION + WRITE-AUTHORITY RELEASE (the linearization point, on OLD).** OLD
  waits for `PREPARED` with a bounded timeout. Timeout / `ABORT` / socket error ->
  ABORT: OLD clears `loading` on all shards, RESUMES FULL SERVING, does not exit (the
  ONLY place OLD can go back to writing). `PREPARED` received -> COMMIT: OLD stops
  accepting connections AND stops serving reads and writes on ALL shards (still holds
  its intact store + untouched `data_dir`), then sends `COMMIT`. After sending `COMMIT`
  OLD never accepts a write again, which structurally forecloses split-brain.
- **Phase 6 ATOMIC FLIP + DURABLE PROMOTE (on NEW).** NEW receives `COMMIT`,
  atomically promotes `staging/ -> data_dir/` (rename + dir fsync), flips the single
  global `serving=true` (all shards already adopted, so all-loading -> all-serving with
  no per-shard stagger visible), starts its acceptor, sends `SERVED`.
- **Phase 7 DRAIN-EXIT (on OLD).** OLD receives `SERVED` -> brief drain -> `exit(0)`.
  If `SERVED` does not arrive within a bounded timeout after `COMMIT`, OLD does NOT
  resume writes (split-brain) and does NOT blindly exit: it enters read-only degraded
  standby + operator alert, holding its intact store and untouched `data_dir`. This is
  data-safe because every acked write <= E already exists in three places (OLD store,
  NEW promoted store, NEW fsynced `data_dir`); the failure is availability, not loss.

## Rollback map (what each failure point falls back to)

| Failure at | Falls back to |
|---|---|
| Phase 1/2/3 (send/recv error, ring overflow, CRC/gap/offset mismatch, timeout) | NEW drops fresh store, exits unserving; OLD clears `loading`, resumes full serving; `data_dir` untouched |
| Phase 4 verify/persist fails | NEW sends ABORT/dies, discards staging; OLD resumes full serving |
| Phase 5 `PREPARED` never arrives | OLD ABORT -> resumes full serving |
| Phase 6 NEW crashes after `COMMIT` received | NEW restarts from PROMOTED `data_dir` (durable) and serves; OLD read-only-degraded then exits; no acked loss |
| Phase 6/7 `SERVED` never reaches OLD | OLD read-only-degraded standby + alert (no resume-writes, no destroy); no acked loss, availability event |

## Phased PR plan (each independently testable, de-riskable, abort-safe)

- **PR-1 Atomic cut + freeze-send primitive** (smallest safe increment; carries the
  riskiest primitive). `ShardStore::freeze_for_handoff() -> (Vec<FrozenSlot>, F)` in one
  non-`await` critical section; the always-on-ring invariant; `send_bulk_from_frozen`.
  Tested in isolation over a `UnixStream::pair`, no sibling. HERO TEST: a writer thread
  hammers the shard during freeze; assert `bulk union delta == exactly the acknowledged
  writes`, zero gaps, zero doubles, absolute-TTL preserved. Must run under load.
- **PR-2 Receiver load path + offset-gated apply.** `recv_shard` into a local
  `Option<LoadedShard>`, never adopted; offset-gate + `first==F+1` + contiguity +
  `applied==E` + CRC + db-count + TTL/CAS fidelity; drop-on-any-error. Paired
  sender/receiver, single process.
- **PR-3 Quiesce reject-gate.** Per-shard `loading`; commit-point `-LOADING` covering
  all mutators; `E` latch. Unit tests for `acked implies offset <= E` and in-flight-tail
  rejection; reads still served.
- **PR-4 Receiver-authoritative commit protocol + durability barrier.**
  `PREPARED/COMMIT/SERVED/ABORT` wire frames, staging fsync, staging->`data_dir` atomic
  promote, bounded timeouts. HERO TEST (mid-flip abort matrix): inject a failure at each
  handshake edge and assert the rollback target, especially kill-receiver-between-
  PREPARED-and-COMMIT -> OLD serves every shard, zero acked-write loss.
- **PR-5 Global serve-gate on NEW + stop-before-start on OLD.** HERO TEST (mixed-
  keyspace impossibility): a client issuing `MGET k0..kN` / cross-shard `MSET` /
  pipelines never observes a partial result; it sees all-old, `-LOADING`, or all-new,
  never a both-serving overlap.
- **PR-6 Lifecycle orchestrator + sibling spawn + socket-activation/SO_REUSEPORT +
  opt-in gate (THE live serve-flip, last PR).** Wires everything behind `handoff_socket`
  (default OFF). HERO TEST: real two-process end-to-end on Linux; `SIGKILL` NEW post-
  `COMMIT` and confirm restart serves all acked data from promoted `data_dir`; `SIGKILL`
  OLD mid-quiesce with no acked loss; sustained-write-load to exercise ring-overflow ->
  clean ABORT; 3-node cluster-harness upgrade run.
- **PR-7 (defer, separate review)** non-socket-activated SO_REUSEPORT drain-and-final-
  accept. Only if socket-activation is not mandatory in prod.

## Residual risks (remain even after every fix)

1. The atomic-cut primitive's correctness depends on NO hidden `.await`/lock-drop
   between slot-freeze and `F`-capture, and on the ring being genuinely always-on. One
   call carries the whole consistency argument; provable by construction on a single-
   threaded shard but MUST be nailed by the PR-1 concurrency test, not by review.
2. `systemd` `LISTEN_FDS` inheritance varies across distros; the stop-before-start flip
   gap is a real bounded unavailability window. This is short-downtime, not zero.
3. The post-`COMMIT`/pre-`SERVED` window is data-safe but resolves to operator-alerted
   degraded standby; it needs real alerting + a runbook, not just code.
4. Ring-overflow under sustained write load makes a live cutover impossible until write
   pressure drops (an availability/retry story, acceptable but real).

## Ship posture

Opt-in only behind the `handoff_socket` gate, default OFF. GA gated on the PR-4/5/6
hero tests passing on a real 2-process plus 3-node cluster run on Linux. Do not enable
by default until the mid-flip-abort matrix and the two-process durability tests are
green on real hardware. The in-process-barrier trap and the stale-`data_dir` trap both
hid behind "trivial proof" language in all three input designs; only a cross-process
kill test earns the safety claim.
