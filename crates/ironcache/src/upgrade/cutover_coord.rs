// SPDX-License-Identifier: MIT OR Apache-2.0
//! The #638 per-shard cutover COORDINATION (slice 2): the piece that lets the #391 streamed live
//! cutover run over a LIVE server's `!Send` per-shard stores.
//!
//! ## The problem
//!
//! The single-thread [`super::orchestrator::run_sender_cutover`] sequencer drives ALL shards from
//! ONE thread over `&mut [SenderShard]`. But a live server's per-shard store + ring are thread-local
//! `Rc<RefCell<..>>` (`!Send`) and exist ONLY on that shard's own thread. So the sender's per-shard
//! work MUST run ON each shard thread, while a host coordinates the cross-shard
//! [`super::stream::CutoverBarrier`] (gather `Prepared` -> decide Commit/Abort -> broadcast) using
//! ONLY `Send` data.
//!
//! ## The shape (Decision 2, Option 2): a `Send` barrier + per-shard `spawn_local` tasks
//!
//! 1. [`CutoverCoord`] (an `Arc`, `Send + Sync`): a report channel shard->host whose payloads are
//!    `Copy`/`Send` ONLY (the shard index + its cut [`ReplOffset`] `E`, or a [`HandoffError`]), and a
//!    `watch<`[`Phase`]`>` host->shards. The `!Send` store/ring/stream NEVER enter the coord; only
//!    offsets, errors, and the phase cross the thread boundary.
//! 2. [`run_shard_cutover_task`]: the per-shard cutover task, an async fn intended to be
//!    `spawn_local`ed on a shard thread. Its FIRST synchronous action (before any `.await`) installs
//!    the shard's own thread-local ring (via `ensure_shard_ring` from slice 1, threaded in as the
//!    `ensure` closure) so the ring is present before [`super::stream::freeze_cut`] latches `F`. It
//!    then runs the per-shard phases reusing the tested primitives:
//!    [`super::orchestrator::sender_phase1_bulk`] (`F` + bulk while still serving) ->
//!    [`super::commit::quiesce_old_shard`] (latch its OWN `E`, set `-LOADING`) ->
//!    [`super::stream::send_delta_await_prepared`] (ship the delta `(F, E]` + `Prepared`) ->
//!    [`super::stream::send_commit`]/[`super::stream::await_served`] on Commit, or
//!    [`super::commit::resume_old_shard`] on Abort. Every `RefCell` borrow is taken synchronously and
//!    dropped before any await, so no borrow crosses an await and the future stays `!Send`-on-thread.
//! 3. [`drive_sender_cutover_host`]: the `Send`-only host coordinator. It broadcasts `Bulk`, then
//!    `Quiesce`, gathers `N` reports, runs [`super::commit::decide_cutover`] (which sets
//!    [`super::commit::WriteAuthority::Released`] BEFORE any Commit frame) into the
//!    [`super::stream::CutoverBarrier`], and broadcasts `Decide(Commit|Abort)`. It touches NO store.
//!
//! ## The staggered-quiesce window (the new safety concern this slice introduces)
//!
//! Unlike [`super::orchestrator::run_sender_cutover`]'s back-to-back quiesce loop, here each shard
//! quiesces at a DIFFERENT instant (when it observes the `Quiesce` watch on its own thread). During
//! the window where shard A is quiesced but shard B is not, a cross-shard write routed to a key OWNED
//! by quiesced shard A must be rejected with `-LOADING` (else it lands above A's `E` and is lost).
//! This is closed by the per-shard `-LOADING` flag ([`crate::serve::is_shard_loading`], a core-local
//! `Cell<bool>` on A's OWN thread) + the cross-shard write gate (`coordinator.rs`): a sibling-routed
//! write to a quiesced owner is rejected on the owner's thread BEFORE it reaches the store's write
//! funnel. Because each shard latches its own `E` and its delta covers up to ITS own `E`, and every
//! write to a not-yet-quiesced shard is still acked and rides that shard's delta, the stagger loses
//! nothing. See the module tests for the per-shard conservation proof.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use ironcache_repl::{ReplId, ReplOffset, ReplRing};
use ironcache_storage::{AccountingHook, EvictionHook, UnixMillis};
use ironcache_store::ShardStore;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};

use super::commit::{decide_cutover, quiesce_old_shard, resume_old_shard};
use super::orchestrator::{SenderDecision, SenderShard, sender_phase1_bulk};
use super::stream::{self, CutoverState, HandoffError};

/// The host's cross-shard DECISION, carried on the `watch` to every shard on the Commit/Abort edge.
/// `Copy` (no allocation): it crosses the host->shard thread boundary as a plain enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Every shard prepared: each shard sends `Commit` + awaits `Served`. Authority is already
    /// [`super::commit::WriteAuthority::Released`] (the quiesce is permanent).
    Commit,
    /// A shard failed (Phase 1 or Phase 3): every shard aborts + [`resume_old_shard`]s. Authority
    /// stays [`super::commit::WriteAuthority::Held`].
    Abort,
}

/// The host->shards phase broadcast (the `watch` value). `Copy`, so it crosses the thread boundary
/// with no allocation and the `!Send` store/ring never ride it.
///
/// The barrier walks `Bulk` (the initial value: shards run freeze + bulk while STILL serving) ->
/// `Quiesce` (the host gathered every floor; shards now latch their OWN `E`, ship the delta, and
/// prepare) -> `Decide(Outcome)` (the host folded every `Prepared` through [`decide_cutover`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// The initial phase: shards freeze + stream the bulk while still serving reads AND writes.
    Bulk,
    /// The host gathered every shard's floor `F`: shards quiesce (latch their own `E`), ship the
    /// bounded delta `(F, E]`, and send `Prepared`.
    Quiesce,
    /// The host's terminal cross-shard decision.
    Decide(Outcome),
}

/// ONE per-shard report crossing the shard->host boundary. Carries ONLY `Send` data: the shard index
/// and either a [`ReplOffset`] (the Phase-1 floor `F`, the Phase-3 cut `E`, or an ignored sentinel on
/// the final ack) or a [`HandoffError`]. The `!Send` store/ring/stream NEVER appear here.
#[derive(Debug)]
pub struct ShardReport {
    /// Which shard produced this report.
    pub shard: u32,
    /// The per-phase result: an offset on success, a [`HandoffError`] on a failed phase.
    pub result: Result<ReplOffset, HandoffError>,
}

/// The `Send + Sync` cutover COORD each shard task clones (via `Arc`). Holds the shard-side ends of
/// the two channels: the `report_tx` (shard->host, cloned per report) and a `watch::Receiver<Phase>`
/// template each shard subscribes from. It carries ONLY `Send` data, so it can be shared across the
/// host thread and every shard thread; the `!Send` store/ring/stream stay on their own threads.
#[derive(Debug)]
pub struct CutoverCoord {
    /// The shard->host report sender. `mpsc::UnboundedSender` is `Send + Sync + Clone`, so a shared
    /// `&self` can post a report from any shard thread without a lock.
    report_tx: mpsc::UnboundedSender<ShardReport>,
    /// The host->shards phase watch. Each shard subscribes its OWN owned receiver via
    /// [`CutoverCoord::phase_watch`] and drives its own `changed()`; the shared `&self` stays
    /// immutable (`Sync`).
    phase: watch::Receiver<Phase>,
}

impl CutoverCoord {
    /// Post a per-shard report to the host. Fire-and-forget: a closed channel (the host already
    /// returned / dropped) is ignored, so a late report never panics a shard task.
    pub fn report(&self, shard: u32, result: Result<ReplOffset, HandoffError>) {
        let _ = self.report_tx.send(ShardReport { shard, result });
    }

    /// Subscribe an OWNED [`watch::Receiver<Phase>`] for one shard task to `.await` phase changes on.
    /// The shared `&self` stays immutable; the shard drives its own clone.
    #[must_use]
    pub fn phase_watch(&self) -> watch::Receiver<Phase> {
        self.phase.clone()
    }
}

/// The HOST side of the coord (NOT shared): the `watch::Sender<Phase>` it broadcasts on and the
/// `report_rx` it gathers on. Owned by the single host coordinator thread; carries only `Send` data.
#[derive(Debug)]
pub struct CutoverHost {
    /// The host->shards phase broadcaster.
    phase_tx: watch::Sender<Phase>,
    /// The shard->host report gatherer.
    report_rx: mpsc::UnboundedReceiver<ShardReport>,
    /// How many shards the barrier must gather each phase.
    n_shards: usize,
}

/// Build the coord pair for `n_shards`: the `Arc<`[`CutoverCoord`]`>` every shard task clones, and
/// the [`CutoverHost`] the single host coordinator owns. The `watch` starts at [`Phase::Bulk`] so a
/// shard task begins Phase 1 immediately (it does not wait for a broadcast to start bulk).
#[must_use]
pub fn new_cutover(n_shards: usize) -> (Arc<CutoverCoord>, CutoverHost) {
    let (report_tx, report_rx) = mpsc::unbounded_channel();
    let (phase_tx, phase) = watch::channel(Phase::Bulk);
    let coord = Arc::new(CutoverCoord { report_tx, phase });
    let host = CutoverHost {
        phase_tx,
        report_rx,
        n_shards,
    };
    (coord, host)
}

/// Await until the shard's phase watch satisfies `pred`, returning the matching [`Phase`]. A dropped
/// host (the `watch` sender gone) is treated as [`Phase::Decide`]`(`[`Outcome::Abort`]`)`: the shard
/// resumes rather than hangs -- and it is SAFE, because a host that already decided `Commit` broadcast
/// that value BEFORE any drop, so the loop observes `Decide(Commit)` before it can ever hit the
/// sender-gone edge (the last watch value is retained).
async fn await_phase<F>(rx: &mut watch::Receiver<Phase>, pred: F) -> Phase
where
    F: Fn(Phase) -> bool,
{
    loop {
        // Read + mark the current value seen UNDER a short borrow that is dropped before the await
        // below (a `watch::Ref` must not be held across `.await`). `Phase` is `Copy`, so the value
        // is copied out and the borrow ends at the semicolon.
        let cur = *rx.borrow_and_update();
        if pred(cur) {
            return cur;
        }
        if rx.changed().await.is_err() {
            return Phase::Decide(Outcome::Abort);
        }
    }
}

/// The per-shard TASK's terminal step: on `Commit` send `Commit` + await `Served` (the OLD never
/// resumes here -- authority is already released); on `Abort` send the abort frame + [`resume_old_shard`]
/// (unquiesce + restore lazy expiry, idempotent even if this shard never quiesced). Reports the final
/// ack to the host: `Ok` on a clean finish, `Err` on a missing `Served` (W3 read-only standby -- it
/// NEVER resumes after a release). The `Ok` payload offset is a sentinel the host ignores.
async fn finish_shard<E, A, S>(
    shard: &mut SenderShard<E, A, S>,
    coord: &CutoverCoord,
    outcome: Outcome,
) where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let i = shard.shard;
    match outcome {
        Outcome::Commit => {
            // Authority was RELEASED at the host's decision (the quiesce is now permanent): on a
            // missing `Served` the OLD enters read-only degraded standby (W3) and MUST NOT resume.
            let served = async {
                stream::send_commit(&mut shard.stream).await?;
                stream::await_served(&mut shard.stream).await
            }
            .await;
            match served {
                Ok(()) => coord.report(i, Ok(ReplOffset::ZERO)),
                Err(e) => coord.report(i, Err(e)),
            }
        }
        Outcome::Abort => {
            // Best-effort abort frame (the receiver also sees the dropped socket), then RESUME full
            // serving. Idempotent: a shard that never quiesced (a Phase-1 sibling failure) no-ops.
            let _ = stream::send_abort_frame(&mut shard.stream).await;
            resume_old_shard(&mut shard.store.borrow_mut());
            coord.report(i, Ok(ReplOffset::ZERO));
        }
    }
}

/// The #638 PER-SHARD CUTOVER TASK: an async fn intended to be `spawn_local`ed on a shard's own
/// thread (slice 3 wires the delivery via a control-channel arm in the drain loop; this slice
/// structures the task so that wiring is just a spawn). It runs the tested per-shard sender
/// primitives on the shard's OWN `!Send` thread-local store + ring, coordinating the cross-shard
/// barrier through the `Send` [`CutoverCoord`].
///
/// `ensure` is the shard's ring-install seam, run as the FIRST synchronous action BEFORE any await so
/// the ring is present before [`super::stream::freeze_cut`] latches `F`. In production it calls
/// `crate::serve::ensure_shard_ring(ctx, i)` (slice 1: idempotently install a disk-backed observer
/// ring, reusing the raft ring when present) + `crate::serve::shard_store(..)` and returns the pair;
/// the tests pass a closure returning a seeded test store + ring. It is threaded as a closure (not a
/// hard-coded `ensure_shard_ring` call) so the `!Send` `ServerContext` never has to be constructed in
/// the in-process tests and the task stays generic over the store's hooks.
///
/// The phases, each reusing a PROVEN primitive, with every `RefCell` borrow taken synchronously and
/// dropped before the next report/await (so no borrow crosses an await and the future is
/// `!Send`-on-thread):
/// - Phase 1 (watch `Bulk`, OLD still serving): [`sender_phase1_bulk`] -> report the floor `F` (or
///   the error). Then await the host leaving `Bulk`.
/// - Phase 2 + 3 (watch `Quiesce`): [`quiesce_old_shard`] latches THIS shard's OWN `E` and sets its
///   `-LOADING` gate (STAGGERED across shards), then [`super::stream::send_delta_await_prepared`]
///   ships the bounded delta `(F, E]` and awaits `Prepared` -> report `E` (or the error). Then await
///   the host's `Decide`.
/// - Decide: [`finish_shard`] commits (send `Commit` + await `Served`) or aborts (resume).
///
/// On a Phase-1 failure, or when the host decides `Abort` before this shard quiesced, the task jumps
/// straight to [`finish_shard`]`(Abort)` (resume is idempotent).
#[allow(clippy::too_many_arguments)] // the per-shard cutover inputs; mirrors the sender primitives.
pub async fn run_shard_cutover_task<E, A, S, F>(
    coord: Arc<CutoverCoord>,
    shard: u32,
    stream: S,
    ensure: F,
    replid: ReplId,
    now: UnixMillis,
    chunk_max: usize,
) where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce() -> (Rc<RefCell<ShardStore<E, A>>>, Rc<RefCell<ReplRing>>),
{
    // FIRST synchronous action, BEFORE any await: install/adopt THIS shard's ring (production:
    // `ensure_shard_ring`), so the observer is active before `freeze_cut` latches `F` and every
    // post-freeze write lands in the ring at an offset above the cut.
    let (store, ring) = ensure();
    let mut shard = SenderShard {
        stream,
        store,
        ring,
        shard,
    };
    let mut phase = coord.phase_watch();

    // PHASE 1 (Bulk): freeze + frozen bulk while STILL serving; report the floor `F` (or the error).
    let floor = match sender_phase1_bulk(&mut shard, replid, now, chunk_max).await {
        Ok(f) => {
            coord.report(shard.shard, Ok(f));
            Some(f)
        }
        Err(e) => {
            coord.report(shard.shard, Err(e));
            None
        }
    };

    // Await the host's post-Bulk decision: `Quiesce` (proceed) or `Decide(Abort)` (a sibling failed
    // Phase 1, so the host skipped the quiesce entirely).
    let go = await_phase(&mut phase, |p| !matches!(p, Phase::Bulk)).await;

    match (go, floor) {
        (Phase::Quiesce, Some(floor)) => {
            // PHASE 2: quiesce -- latch THIS shard's OWN `E` + raise its `-LOADING` gate. STAGGERED:
            // each shard reaches this on its own thread at its own instant; the gate (checked on the
            // owner's thread) rejects a sibling-routed write to this quiesced owner, so nothing lands
            // above this `E`.
            let _e = quiesce_old_shard(&mut shard.store.borrow_mut(), &shard.ring);
            // PHASE 3: ship the bounded delta `(F, E]` and await `Prepared` -> report `E`.
            let prepared =
                stream::send_delta_await_prepared(&mut shard.stream, &shard.ring, floor, chunk_max)
                    .await;
            coord.report(shard.shard, prepared);
            // Await the host's cross-shard decision, then commit or abort.
            let outcome = match await_phase(&mut phase, |p| matches!(p, Phase::Decide(_))).await {
                Phase::Decide(o) => o,
                // Unreachable given the predicate; fail-closed to Abort (resume, never split-brain).
                _ => Outcome::Abort,
            };
            finish_shard(&mut shard, coord.as_ref(), outcome).await;
        }
        _ => {
            // Phase-1 failure, or the host aborted before this shard quiesced: resume (idempotent).
            finish_shard(&mut shard, coord.as_ref(), Outcome::Abort).await;
        }
    }
}

/// Gather exactly `n` per-shard reports from the host's report channel, in order. A closed channel
/// (every shard task dropped -- e.g. a panic) yields a fail-closed [`HandoffError::Aborted`] for each
/// missing report, so the host NEVER blocks forever on a slow/dead shard (the coordination-liveness
/// guarantee): the barrier resolves to Abort rather than hang.
async fn gather(
    rx: &mut mpsc::UnboundedReceiver<ShardReport>,
    n: usize,
) -> Vec<Result<ReplOffset, HandoffError>> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        match rx.recv().await {
            Some(rep) => out.push(rep.result),
            None => out.push(Err(HandoffError::Aborted)),
        }
    }
    out
}

/// The #638 HOST COORDINATOR: drive the cross-shard cutover barrier from the host thread using ONLY
/// `Send` data (it touches NO store, ring, or stream -- those stay on the shard threads). Consumes the
/// [`CutoverHost`] end of the coord.
///
/// The sequence (mirroring [`super::orchestrator::run_sender_cutover`]'s phase ordering, but fanned
/// out across threads through the coord):
/// 1. The watch starts at [`Phase::Bulk`]; gather `N` Phase-1 floor reports. If ANY is an error,
///    broadcast [`Phase::Decide`]`(`[`Outcome::Abort`]`)`, drain `N` final acks, and return
///    [`SenderDecision::Aborted`] (nothing quiesced; every shard resumes).
/// 2. Else broadcast [`Phase::Quiesce`] and gather `N` Phase-3 `Prepared` reports.
/// 3. Fold them through [`decide_cutover`] (the ONE place authority moves: `Commit` sets
///    [`super::commit::WriteAuthority::Released`] BEFORE any Commit frame). On `Commit`, broadcast
///    `Decide(Commit)` and gather `N` final acks: all `Ok` -> [`SenderDecision::Committed`]; a missing
///    `Served` returns that [`HandoffError`] (W3 read-only standby -- the caller does NOT resume and
///    does NOT exit). On `Abort`, broadcast `Decide(Abort)`, drain `N` final acks, return
///    [`SenderDecision::Aborted`].
///
/// CRASH-SIMPLE: no `unwrap`/`expect`; every error path returns the W3 outcome (an `Err`) or a clean
/// `Aborted`, never resume-after-release and never exit. `n_shards == 0` is a degenerate empty handoff
/// that commits (mirrors [`super::stream::CutoverBarrier::new`]`(0)`).
///
/// # Errors
/// A [`HandoffError`] ONLY when a committed cutover could not confirm every shard `Served` (W3): the
/// caller enters read-only degraded standby (it does not resume writes and does not exit). A shard
/// failing to prepare is NOT an error -- it is a clean [`SenderDecision::Aborted`].
pub async fn drive_sender_cutover_host(
    mut host: CutoverHost,
) -> Result<SenderDecision, HandoffError> {
    let n = host.n_shards;
    if n == 0 {
        return Ok(SenderDecision::Committed); // degenerate empty handoff (nothing to cut over).
    }

    // PHASE 1: gather every shard's floor. Any error -> abort BEFORE the quiesce (nothing quiesced).
    let floors = gather(&mut host.report_rx, n).await;
    if floors.iter().any(Result::is_err) {
        broadcast_and_drain(&mut host, Outcome::Abort).await;
        return Ok(SenderDecision::Aborted);
    }

    // PHASE 2 + 3: tell every shard to quiesce (latch its own E) + ship its delta + prepare.
    let _ = host.phase_tx.send_replace(Phase::Quiesce);
    let prepared = gather(&mut host.report_rx, n).await;

    // THE BARRIER + THE LINEARIZATION POINT: fold every Prepared through decide_cutover. On Commit it
    // RELEASES authority (the quiesce is now permanent on every shard) BEFORE any Commit frame is
    // broadcast; the host never touches a store to do this -- the release is the shards' standing
    // quiesce plus their not-resuming on Commit.
    let (state, _authority) = decide_cutover(&prepared);
    match state {
        CutoverState::Commit => {
            let _ = host.phase_tx.send_replace(Phase::Decide(Outcome::Commit));
            // Gather the final acks. A missing `Served` (W3) surfaces as the shard's error: return it
            // so the caller enters read-only degraded standby (no resume, no exit).
            let acks = gather(&mut host.report_rx, n).await;
            match acks.into_iter().find_map(Result::err) {
                Some(e) => Err(e),
                None => Ok(SenderDecision::Committed),
            }
        }
        CutoverState::Pending | CutoverState::Abort => {
            broadcast_and_drain(&mut host, Outcome::Abort).await;
            Ok(SenderDecision::Aborted)
        }
    }
}

/// Broadcast a terminal [`Outcome`] to every shard and drain their `N` final acks, so the host does
/// not return (and drop the watch) before every shard has finished resuming/committing. Used on both
/// abort edges (a Phase-1 error and a barrier Abort).
async fn broadcast_and_drain(host: &mut CutoverHost, outcome: Outcome) {
    let _ = host.phase_tx.send_replace(Phase::Decide(outcome));
    let _ = gather(&mut host.report_rx, host.n_shards).await;
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::upgrade::commit::{Staging, promote, receive_shard_to_prepared, reconstruct_shard};
    use ironcache_repl::{ReplObserver, encode_kvobj};
    use ironcache_storage::{ExpireWrite, NewValue, Store};
    use ironcache_store::{ShardStore, SnapshotCursor};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use tokio::net::UnixStream;

    const NOW: UnixMillis = UnixMillis(1_000);
    const DBS: u32 = 4;
    const CHUNK: usize = 64;

    /// A live OLD shard's store + always-on ring handle pair.
    type ShardHandles = (Rc<RefCell<ShardStore>>, Rc<RefCell<ReplRing>>);
    /// One shard's inputs to a per-shard task: its index, its sender-end stream, and its store + ring.
    type SenderSlot = (
        u32,
        UnixStream,
        Rc<RefCell<ShardStore>>,
        Rc<RefCell<ReplRing>>,
    );
    /// A shard's whole keyspace dumped as `(db, key) -> encoded-KvObj` (value + type + absolute TTL).
    type ShardDump = HashMap<(u32, Vec<u8>), Vec<u8>>;

    fn replid() -> ReplId {
        ReplId::from_bytes([0x63; 20])
    }

    fn tmp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ic-coord-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    /// A fresh store with an always-on observer ring installed BEFORE any write (the keystone the
    /// atomic cut needs), seeded with `n` keys spread across the databases.
    fn seeded(n: u32, tag: &str) -> ShardHandles {
        // `ReplRing::new` already returns an `Rc<RefCell<ReplRing>>` (the shared observer handle).
        let ring = ReplRing::new(200_000, ReplOffset::ZERO);
        let store = Rc::new(RefCell::new(ShardStore::new(DBS)));
        store
            .borrow_mut()
            .set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        {
            let mut s = store.borrow_mut();
            for i in 0..n {
                s.upsert(
                    i % DBS,
                    format!("{tag}-{i}").as_bytes(),
                    NewValue::Bytes(format!("v-{tag}-{i}").as_bytes()),
                    ExpireWrite::Clear,
                    NOW,
                );
            }
        }
        (store, ring)
    }

    fn dump_map<E: EvictionHook, A: AccountingHook>(
        store: &ShardStore<E, A>,
        now: UnixMillis,
    ) -> ShardDump {
        let mut m = HashMap::new();
        let dbs = store.databases();
        let mut c = SnapshotCursor::START;
        while !c.is_done(dbs) {
            let (chunk, next) = store.snapshot_chunk(c, 256, now);
            c = next;
            for (db, key, kv) in chunk {
                m.insert((db, key.into_vec()), encode_kvobj(&kv));
            }
        }
        m
    }

    /// Drive the NEW (receiver) side of a full cutover to a COMMIT, adopting every shard, WITHOUT
    /// flipping the process-global serve gate. This mirrors [`run_receiver_cutover`]'s phased structure
    /// (Phase 1 bulk for ALL shards, THEN Phase 3 delta for ALL shards -- the separation the sender's
    /// cross-shard barrier REQUIRES, else a per-shard receiver would deadlock waiting on shard 0's
    /// delta before reading shard 1's bulk), but deliberately OMITS `begin_serving_on_commit`: this
    /// slice tests the SENDER-side coordination, and the receiver serve-flip is a separate concern
    /// (PR-6). Omitting it also keeps these parallel tests from racing the process-global `SERVING`
    /// gate that other modules' hero tests assert on. Returns every adopted shard.
    async fn drive_receiver_commit_no_flip<E, A, M>(
        streams: &mut [UnixStream],
        mut make_store: M,
        staging: &Staging,
        data_dir: &std::path::Path,
    ) -> Vec<stream::LoadedShard<E, A>>
    where
        E: EvictionHook,
        A: AccountingHook,
        M: FnMut() -> ShardStore<E, A>,
    {
        // PHASE 1: bulk-load + fsync + BulkStaged, per shard (all before any delta).
        let mut staged = Vec::new();
        for s in streams.iter_mut() {
            let (store, shard, floor) = stream::recv_bulk(s, &mut make_store, DBS, NOW)
                .await
                .expect("recv bulk");
            let entry = staging.stage_bulk(&store, shard, NOW).expect("stage bulk");
            stream::send_bulk_staged(s).await.expect("bulk staged");
            staged.push((store, shard, floor, entry));
        }
        // PHASE 3: apply + verify + fsync the delta, send Prepared, per shard.
        let mut prepared = Vec::new();
        let mut entries = Vec::new();
        for (i, s) in streams.iter_mut().enumerate() {
            let (store, shard, floor, entry) = {
                let slot = &mut staged[i];
                (
                    std::mem::replace(&mut slot.0, make_store()),
                    slot.1,
                    slot.2,
                    slot.3.clone(),
                )
            };
            let p = stream::recv_prepare_only(s, store, shard, floor, NOW, |_s, delta| {
                staging.stage_delta(shard, delta)
            })
            .await
            .expect("recv prepared");
            prepared.push(p);
            entries.push(entry);
        }
        // AWAIT the per-shard Commit the sender tasks send, then promote ONCE (no serve flip), Served.
        let mut committed = Vec::new();
        for (i, s) in streams.iter_mut().enumerate() {
            let p = std::mem::replace(
                &mut prepared[i],
                stream::PreparedShard {
                    store: make_store(),
                    shard: 0,
                    final_offset: ReplOffset::ZERO,
                },
            );
            match stream::recv_await_commit(s, p).await.expect("await commit") {
                stream::ShardCommit::Committed(loaded) => committed.push(*loaded),
                stream::ShardCommit::Aborted => panic!("unexpected abort"),
            }
        }
        staging
            .write_manifest(NOW.0 / 1000, entries)
            .expect("manifest");
        promote(staging.dir(), data_dir).expect("promote");
        for s in streams.iter_mut() {
            stream::send_served(s).await.expect("served");
        }
        committed
    }

    /// THE COMMIT PATH (the full slice-2 stack, deterministic single runtime): N per-shard cutover
    /// TASKS (each on its OWN test store + ring + stream) + the `Send` host coordinator + the receiver
    /// driver, all driven concurrently over real socketpairs. Asserts the barrier commits, every shard
    /// is adopted, and each adopted NEW store == EXACTLY its OLD keyspace @ its own `E` (and the
    /// promoted durable dir reconstructs the same). Proves the `!Send` store/ring/stream never leave
    /// their task while the coord carries only `Send` offsets/phase across the barrier.
    #[tokio::test(flavor = "current_thread")]
    async fn coord_multi_shard_cutover_commits_and_adopts_every_shard() {
        const SHARDS: usize = 3;
        const PER: u32 = 250;
        crate::serve::unquiesce_shard();

        let root = tmp_root("commit");
        let _ = std::fs::remove_dir_all(&root);
        let staging = Staging::new(root.join("staging")).expect("staging");
        let data_dir = root.join("data");

        let mut references: Vec<ShardDump> = Vec::new();
        let mut senders: Vec<SenderSlot> = Vec::new();
        let mut recv_streams: Vec<UnixStream> = Vec::new();
        for shard in 0..SHARDS as u32 {
            let (store, ring) = seeded(PER, &format!("s{shard}"));
            references.push(dump_map(&store.borrow(), NOW));
            let (a, b) = UnixStream::pair().expect("socketpair");
            senders.push((shard, a, store, ring));
            recv_streams.push(b);
        }

        let (coord, host) = new_cutover(SHARDS);
        // Build the three per-shard task futures (each OWNS its !Send store/ring/stream).
        let (i2, a2, st2, rg2) = senders.pop().unwrap();
        let (i1, a1, st1, rg1) = senders.pop().unwrap();
        let (i0, a0, st0, rg0) = senders.pop().unwrap();
        let s0 = run_shard_cutover_task(
            Arc::clone(&coord),
            i0,
            a0,
            move || (st0, rg0),
            replid(),
            NOW,
            CHUNK,
        );
        let s1 = run_shard_cutover_task(
            Arc::clone(&coord),
            i1,
            a1,
            move || (st1, rg1),
            replid(),
            NOW,
            CHUNK,
        );
        let s2 = run_shard_cutover_task(
            Arc::clone(&coord),
            i2,
            a2,
            move || (st2, rg2),
            replid(),
            NOW,
            CHUNK,
        );
        drop(coord); // only the three tasks hold a coord clone now (so the channel closes on finish).

        let host_fut = drive_sender_cutover_host(host);
        let recv_fut = drive_receiver_commit_no_flip(
            &mut recv_streams,
            || ShardStore::new(DBS),
            &staging,
            &data_dir,
        );
        let (_r0, _r1, _r2, host_res, adopted) = tokio::join!(s0, s1, s2, host_fut, recv_fut);

        assert_eq!(
            host_res.expect("host drove to a decision"),
            SenderDecision::Committed,
            "every shard prepared -> the host commits"
        );
        assert_eq!(adopted.len(), SHARDS, "every shard was adopted");
        assert!(
            crate::serve::is_shard_loading(),
            "the OLD's per-shard quiesce is PERMANENT after the release"
        );
        for loaded in &adopted {
            let want = &references[loaded.shard as usize];
            assert_eq!(
                &dump_map(&loaded.store, NOW),
                want,
                "adopted shard {} == its OLD keyspace @ its own E",
                loaded.shard
            );
            let durable = reconstruct_shard(&data_dir, loaded.shard, NOW, || ShardStore::new(DBS))
                .expect("promoted dir reconstructs the shard");
            assert_eq!(
                &dump_map(&durable, NOW),
                want,
                "durable data_dir shard {} == OLD state@E (bulk UNION delta)",
                loaded.shard
            );
        }

        crate::serve::unquiesce_shard();
        let _ = std::fs::remove_dir_all(&root);
    }

    /// THE ABORT MATRIX (a shard fails to hand off): shard 1's receiver DROPS its socket, so shard 1's
    /// task fails Phase 1; the host gathers the floors, sees the error, and decides Abort BEFORE any
    /// quiesce. Every shard's task runs [`resume_old_shard`] (idempotent), the host returns
    /// [`SenderDecision::Aborted`], authority was NEVER released (no quiesce stuck), and the NEW
    /// promoted nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn coord_one_shard_failure_aborts_and_resumes_every_shard() {
        const SHARDS: usize = 3;
        const PER: u32 = 120;
        crate::serve::unquiesce_shard();

        let root = tmp_root("abort");
        let _ = std::fs::remove_dir_all(&root);
        let staging = Staging::new(root.join("staging")).expect("staging");
        let data_dir = root.join("data");

        let mut keepalive: Vec<ShardHandles> = Vec::new();
        let mut senders: Vec<SenderSlot> = Vec::new();
        let mut recv_streams: Vec<UnixStream> = Vec::new();
        for shard in 0..SHARDS as u32 {
            let (store, ring) = seeded(PER, &format!("s{shard}"));
            let (a, b) = UnixStream::pair().expect("socketpair");
            senders.push((shard, a, Rc::clone(&store), Rc::clone(&ring)));
            recv_streams.push(b);
            keepalive.push((store, ring));
        }

        let (coord, host) = new_cutover(SHARDS);
        let (i2, a2, st2, rg2) = senders.pop().unwrap();
        let (i1, a1, st1, rg1) = senders.pop().unwrap();
        let (i0, a0, st0, rg0) = senders.pop().unwrap();
        let s0 = run_shard_cutover_task(
            Arc::clone(&coord),
            i0,
            a0,
            move || (st0, rg0),
            replid(),
            NOW,
            CHUNK,
        );
        let s1 = run_shard_cutover_task(
            Arc::clone(&coord),
            i1,
            a1,
            move || (st1, rg1),
            replid(),
            NOW,
            CHUNK,
        );
        let s2 = run_shard_cutover_task(
            Arc::clone(&coord),
            i2,
            a2,
            move || (st2, rg2),
            replid(),
            NOW,
            CHUNK,
        );
        drop(coord);

        // The receiver: own the three ends; DROP shard 1's (a crash), run the real receive-to-prepared
        // for 0 and 2 (they get an Abort/EOF when the barrier aborts and adopt nothing).
        let mut it = recv_streams.into_iter();
        let mut b0 = it.next().unwrap();
        let b1 = it.next().unwrap();
        let mut b2 = it.next().unwrap();
        let host_fut = drive_sender_cutover_host(host);
        let recv_fut = async {
            drop(b1); // crash shard 1: its sender's HELLO/ack read hits EOF -> Phase-1 abort.
            let r0 =
                receive_shard_to_prepared(&mut b0, || ShardStore::new(DBS), DBS, NOW, &staging);
            let r2 =
                receive_shard_to_prepared(&mut b2, || ShardStore::new(DBS), DBS, NOW, &staging);
            let _ = tokio::join!(r0, r2);
        };
        let (_s0, _s1, _s2, host_res, ()) = tokio::join!(s0, s1, s2, host_fut, recv_fut);

        assert_eq!(
            host_res.expect("host drove to a decision"),
            SenderDecision::Aborted,
            "a shard failing to hand off aborts the whole flip"
        );
        assert!(
            !crate::serve::is_shard_loading(),
            "every OLD shard resumed after the abort (authority never released)"
        );
        assert!(
            !data_dir.exists(),
            "no partial cutover: the NEW promoted nothing"
        );

        crate::serve::unquiesce_shard();
        let _ = std::fs::remove_dir_all(&root);
    }

    /// STAGGERED-QUIESCE SAFETY (per-shard conservation): once a shard quiesces, a cross-shard write
    /// routed to that quiesced OWNER is rejected with `-LOADING` (the EXACT production gate:
    /// `is_shard_loading() && request_is_write_for_pause(..)`) BEFORE it reaches the store's write
    /// funnel, so nothing lands above the shard's latched `E`. An un-gated write BEFORE the quiesce
    /// does land (the gate is load-bearing, not a no-op).
    #[test]
    fn coord_staggered_quiesce_gate_conserves_e() {
        crate::serve::unquiesce_shard(); // fresh thread-local gate.
        let (store, ring) = seeded(64, "own");

        // Model the cross-shard write dispatch to THIS shard's store (coordinator.rs:583): the write
        // reaches the funnel (and the ring) ONLY if the owner's `-LOADING` gate is down.
        let gated_write = |db: u32, key: &str, val: &str| -> bool {
            if crate::serve::is_shard_loading()
                && ironcache_server::request_is_write_for_pause(b"SET", false, &[])
            {
                return false; // -LOADING: never assigned a ring offset.
            }
            store.borrow_mut().upsert(
                db,
                key.as_bytes(),
                NewValue::Bytes(val.as_bytes()),
                ExpireWrite::Clear,
                NOW,
            );
            true
        };

        // BEFORE the quiesce: an un-gated write lands and advances the ring.
        let h0 = ring.borrow().head();
        assert!(
            gated_write(0, "pre", "v"),
            "an un-quiesced write is accepted"
        );
        let h1 = ring.borrow().head();
        assert!(h1 > h0, "the accepted write advanced the ring head");

        // QUIESCE: latch E = ring.head() + raise the `-LOADING` gate (one on-thread step).
        let e = quiesce_old_shard(&mut store.borrow_mut(), &ring);
        assert_eq!(
            e,
            ring.borrow().head(),
            "E is the head at the quiesce instant"
        );
        assert!(crate::serve::is_shard_loading(), "the shard is quiescing");

        // DURING the staggered window: every cross-shard write to this quiesced owner is GATED and
        // NONE reaches the ring -- the head stays at E (per-shard acked-write conservation).
        for i in 0..32 {
            assert!(
                !gated_write(i % DBS, &format!("hot-{i}"), "x"),
                "a write to a quiesced owner is rejected (-LOADING)"
            );
        }
        assert_eq!(
            ring.borrow().head(),
            e,
            "nothing was acked above E while the owner was quiesced (conservation)"
        );

        crate::serve::unquiesce_shard();
    }

    /// STAGGERED-QUIESCE SAFETY (per-shard independence): the `-LOADING` flag is a per-shard THREAD-
    /// LOCAL, so quiescing shard A's thread leaves shard B's thread UN-quiesced. That is exactly what
    /// makes the staggered window safe: a write to a not-yet-quiesced owner B (dispatched on B's
    /// thread, reading B's own flag) is still acked while A is already quiesced -- no false rejection
    /// and, paired with the conservation test above, no write above any shard's E.
    #[test]
    fn coord_quiesce_flag_is_independent_per_shard_thread() {
        use std::sync::mpsc::channel;
        let (a_quiesced_tx, a_quiesced_rx) = channel::<()>();
        let (b_flag_tx, b_flag_rx) = channel::<bool>();
        let (release_a_tx, release_a_rx) = channel::<()>();

        let a = std::thread::spawn(move || {
            let ring = ReplRing::new(1_024, ReplOffset::ZERO);
            assert!(!crate::serve::is_shard_loading(), "A starts un-quiesced");
            let _e = crate::serve::quiesce_shard(&ring); // A raises ITS OWN thread-local gate.
            assert!(
                crate::serve::is_shard_loading(),
                "A is quiesced on its own thread"
            );
            a_quiesced_tx.send(()).unwrap();
            release_a_rx.recv().unwrap(); // stay quiesced until B has observed its independent flag.
            crate::serve::unquiesce_shard();
        });

        let b = std::thread::spawn(move || {
            a_quiesced_rx.recv().unwrap(); // A is now quiesced.
            // B's OWN flag is a SEPARATE thread-local: it must be false even while A is quiesced.
            let b_loading = crate::serve::is_shard_loading();
            b_flag_tx.send(b_loading).unwrap();
        });

        let b_loading_while_a_quiesced = b_flag_rx.recv().unwrap();
        release_a_tx.send(()).unwrap();
        a.join().unwrap();
        b.join().unwrap();
        assert!(
            !b_loading_while_a_quiesced,
            "shard B's -LOADING flag is INDEPENDENT of shard A's quiesce (per-shard thread-local)"
        );
    }

    // ---- pure barrier / liveness tests (mock shards; no sockets, no !Send -- always-on in CI) ----

    /// A MOCK shard task: reports a floor, awaits `Quiesce`, reports a `Prepared` (or an error),
    /// awaits `Decide`, reports the final ack -- recording every phase it observes. `slow` delays the
    /// floor report to model a lagging shard (the liveness case).
    async fn mock_shard(
        coord: Arc<CutoverCoord>,
        i: u32,
        slow: bool,
        fail_prepare: bool,
        seen: Rc<RefCell<Vec<Phase>>>,
    ) {
        let mut phase = coord.phase_watch();
        if slow {
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
        }
        coord.report(i, Ok(ReplOffset(u64::from(i) + 1))); // floor F
        let go = await_phase(&mut phase, |p| !matches!(p, Phase::Bulk)).await;
        seen.borrow_mut().push(go);
        if matches!(go, Phase::Quiesce) {
            if fail_prepare {
                coord.report(i, Err(HandoffError::Aborted));
            } else {
                coord.report(i, Ok(ReplOffset(100 + u64::from(i)))); // prepared E
            }
            let d = await_phase(&mut phase, |p| matches!(p, Phase::Decide(_))).await;
            seen.borrow_mut().push(d);
        }
        coord.report(i, Ok(ReplOffset::ZERO)); // final ack
    }

    /// COORDINATION LIVENESS: with one SLOW shard, the host does not deadlock -- it waits on the report
    /// channel (a bounded wait that resolves when the slow shard reports), gathers every phase in
    /// order, and commits. Every mock shard observes `Quiesce` then `Decide(Commit)` (phase ordering).
    #[tokio::test(flavor = "current_thread")]
    async fn coord_host_commits_with_a_slow_shard() {
        const SHARDS: usize = 3;
        let (coord, host) = new_cutover(SHARDS);
        let seen: Vec<Rc<RefCell<Vec<Phase>>>> = (0..SHARDS)
            .map(|_| Rc::new(RefCell::new(Vec::new())))
            .collect();
        let m0 = mock_shard(Arc::clone(&coord), 0, false, false, Rc::clone(&seen[0]));
        let m1 = mock_shard(Arc::clone(&coord), 1, true, false, Rc::clone(&seen[1])); // the slow one
        let m2 = mock_shard(Arc::clone(&coord), 2, false, false, Rc::clone(&seen[2]));
        drop(coord);
        let host_fut = drive_sender_cutover_host(host);
        let (_m0, _m1, _m2, host_res) = tokio::join!(m0, m1, m2, host_fut);

        assert_eq!(
            host_res.expect("host decided"),
            SenderDecision::Committed,
            "the host commits even with a slow shard (no deadlock)"
        );
        for (i, s) in seen.iter().enumerate() {
            assert_eq!(
                &*s.borrow(),
                &[Phase::Quiesce, Phase::Decide(Outcome::Commit)],
                "shard {i} observed Quiesce THEN Decide(Commit) (phase ordering)"
            );
        }
    }

    /// COORDINATION ABORT (barrier level, deterministic): one shard reports an error at `Prepared`, so
    /// the host folds it through [`decide_cutover`] to Abort and broadcasts `Decide(Abort)`; every
    /// mock shard observes the abort. No sockets, so this is an always-on CI guard on the decide fold.
    #[tokio::test(flavor = "current_thread")]
    async fn coord_host_aborts_when_a_shard_fails_to_prepare() {
        const SHARDS: usize = 3;
        let (coord, host) = new_cutover(SHARDS);
        let seen: Vec<Rc<RefCell<Vec<Phase>>>> = (0..SHARDS)
            .map(|_| Rc::new(RefCell::new(Vec::new())))
            .collect();
        let m0 = mock_shard(Arc::clone(&coord), 0, false, false, Rc::clone(&seen[0]));
        let m1 = mock_shard(Arc::clone(&coord), 1, false, true, Rc::clone(&seen[1])); // fails prepare
        let m2 = mock_shard(Arc::clone(&coord), 2, false, false, Rc::clone(&seen[2]));
        drop(coord);
        let host_fut = drive_sender_cutover_host(host);
        let (_m0, _m1, _m2, host_res) = tokio::join!(m0, m1, m2, host_fut);

        assert_eq!(
            host_res.expect("host decided"),
            SenderDecision::Aborted,
            "one failed Prepared aborts the whole barrier"
        );
        for (i, s) in seen.iter().enumerate() {
            let observed = s.borrow();
            assert_eq!(
                observed.first(),
                Some(&Phase::Quiesce),
                "shard {i} quiesced"
            );
            assert_eq!(
                observed.last(),
                Some(&Phase::Decide(Outcome::Abort)),
                "shard {i} observed the Abort decision"
            );
        }
    }
}
