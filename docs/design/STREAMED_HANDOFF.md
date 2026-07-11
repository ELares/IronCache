# Streamed state handoff live cutover (upgrade epic Phase 2c, issue #391)

Status: DESIGNED + MEASURED. The abort-safe streaming core ships (`crates/ironcache/src/upgrade/stream.rs`, `drive.rs`, opt-in via `handoff_socket`); the live-traffic serve flip is deferred. This doc records the design of that deferred wiring and the measurement that scopes when it is worth building.

## Problem

An in-place binary upgrade restarts the process. The default path (Phase 2b, #390) saves the keyspace to a RAM-backed (tmpfs) data dir and the new process reloads it on boot. The client-visible outage is therefore the whole reload, which grows with the dataset. The streamed handoff (this doc) instead streams state from the old process directly to a sibling new process so the new one serves before the old drains, bounding the outage to a final delta plus an atomic flip, independent of dataset size.

## What already ships (the abort-safe core)

`stream.rs` / `drive.rs`: the CRC and version framed envelope codec, `send_bulk` / `send_cutover` (read-only on the old store), `recv_bulk` / `recv_cutover` (build a fresh store, adopt only on a complete verified path), and the pure `CutoverBarrier` that flips all shards or none. Every error path (bad magic or version or CRC, oversize, truncation, delta gap, delta ring overflow, offset mismatch, peer abort, timeout) is fail closed: the old store stays intact and serving, and a partially received store is dropped, never adopted. Real AF_UNIX socket tests cover the abort matrix. The one cross shard synchronization is the cutover flip.

## The deferred live wiring

Six phases, tied to the built `CutoverBarrier`:

0. SPAWN: the orchestrator spawns the sibling with a receiver role, the socket path, and the inherited listener fd. The old process keeps fully serving. The new process boots not serving (a single global flag rejects every command until the flip) and writes only to a staging dir.
1. FREEZE and BULK (per shard, on the shard thread): install or confirm the always on replication observer ring, capture the freeze floor offset `F = ring.head()`, take the Arc COW frozen slot view (#588), and stream the frozen slots while the shard keeps serving reads and writes (concurrent writes copy on write and land in the ring at offset greater than `F`). The receiver fsyncs the bulk snapshot to staging here, while the old process still serves, so the heavy fsync is outside the write outage.
2. QUIESCE (write outage begins, reads still served): a `ShardWork::Quiesce` delivered through the shard inbox runs on the shard thread and, in one uninterrupted step, latches the final cut offset `E = ring.head()` and sets a thread local loading flag. From this point every client write is rejected with `-LOADING` before it is assigned an offset.
3. FINAL DELTA: the old process ships the delta `ring[F+1..E]`; the receiver applies it in offset order and verifies `applied == E`.
4. PREPARE and COMMIT (receiver authoritative): the receiver, having verified and fsynced every shard, sends PREPARED; the old process releases write authority only on COMMIT; the barrier flips all shards or none. On any failure the old process resumes serving (Abort) and the sibling exits without serving.
5. FLIP and DRAIN: the sibling flips to serving; the old process drains in flight requests and exits.

### Decision 1: no orphaned backlog RST

An SO_REUSEPORT listener that closes can RST connections still queued in its accept backlog. Chosen: the guaranteed no RST path requires an inherited, never closed listener (reuse the shipped #389 fd adoption, generalized from systemd to an orchestrator held fd passed to the sibling); this is a by construction guarantee, not probabilistic. As a documented best effort for the plain self bind case, recommend `net.ipv4.tcp_migrate_req=1` (kernel 5.14+), which re hashes queued and incoming connections onto a surviving sibling. A self coded drain and final accept fd proxy is explicitly NOT recommended (genuinely racy, large surface).

### Decision 2: the `-LOADING` write quiesce

Chosen: a per shard thread local loading flag, latched together with the final cut offset `E` by an on shard thread `ShardWork::Quiesce` on the existing inbox. This is the cheapest hot path (a core local bool, not even an atomic, because a shard is single threaded under the shared nothing model) AND the only option that structurally guarantees "acked implies offset less than or equal to `E`", because delivering the quiesce as on shard thread work makes the flag set and the `E` latch a single critical section. A shared cross thread atomic gate races the `E` latch; reusing CLIENT PAUSE has the wrong semantics (a paused write applies after the window, at offset greater than `E`, and would be lost).

## Safety argument (zero acknowledged write loss)

Each link is structural, not by review. The always on observer ring is a total order of every applied mutation. `bulk(frozen at F)` union `delta[F+1..E]` equals exactly the mutations with offset less than or equal to `E`, because the quiesce rejects every client write with `-LOADING` before an offset is assigned, so nothing is acked at offset greater than `E`. Ring overflow in `F..E` is fail closed (abort, never a silent drop). The receiver adopts only on a CRC verified, contiguous, `applied == E`, cutover acked path, and the barrier makes the flip all or none across shards. The old process releases write authority only after PREPARED, which the receiver sends only after all shards are verified and fsynced; so at the commit point every acked write exists in at least three copies (the old store, the old untouched data dir, and the new fsynced staging), and the old process never writes after commit (no split brain).

The no RST guarantee is deployment mode scoped: it holds under the inherited listener; it is best effort under plain self bind SO_REUSEPORT even with `tcp_migrate_req`.

## Measurement: is it worth building?

The streamed handoff only wins over the shipped #390 reload path by shrinking the restart outage. So the deciding question is how large the #390 outage actually is. Measured on a release build in a Linux container (4 vCPU, 7.7 GB RAM), data dir on tmpfs (the RAM backed #390 path), 4 shards, values of 1 KiB, stop via SIGKILL after a committed SAVE to isolate the reload leg, readiness measured by a tight reconnect loop until the full keyspace is back:

| dataset | used_memory | SAVE (median) | client visible restart window (median) |
|---|---|---|---|
| 100 keys | 2.5 MiB | 0.3 ms | 3.6 ms |
| ~214k keys | 266 MiB | 182 ms | 592 ms |
| ~859k keys | 1060 MiB | 798 ms | 2389 ms |

The window scales cleanly linearly with `used_memory` at about 2.23 ms per MiB (a RAM backed reload throughput near 450 MiB/s on 4 cores), and is decode and insert bound (rebuilding the hash table), not disk bound. The crossover past a one second window is near 450 MiB (about 360k 1 KiB keys). SAVE also scales linearly but is mostly not client visible (the Arc COW frozen view lets the old process keep serving during it). Socket activation (#389) converts the refused connection symptom into backlog queueing of the same duration; it removes the drop, not the reload time. The tmpfs numbers are a best case floor (a disk data dir is slower).

## Conclusion

For deployments under about 400 MB of live data, the #390 reload path is already comfortably sub second and the streamed handoff is not worth its cost. For multi GB working sets (the common case for a cache), the #390 outage grows without bound (about 2.4 s at 1 GB, about 5 s at 2 GB) and is a genuine upgrade availability miss that only the streamed live cutover bounds to a size independent sub second flip. Build order when built: the riskiest primitives first (the freeze and bulk under load atomic cut, then the receiver authoritative cross process commit), each proven by a concurrency hero test and a kill between PREPARED and COMMIT test, not by review, because those two primitives carry the whole safety argument.
