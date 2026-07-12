// SPDX-License-Identifier: MIT OR Apache-2.0
//! The #391 PR-6 LIFECYCLE ORCHESTRATOR: the last slice that turns the tested transport + commit
//! primitives (PR-1..PR-5) into a REAL, two-process live cutover -- it spawns the NEW-version binary
//! as a sibling receiver, drives the whole streamed handoff old->new to a committed serve-flip with no
//! acknowledged-write loss, and guarantees NO client connection is reset across the flip by handing
//! the sibling the SAME never-closed client-listener fd.
//!
//! ## What this module owns
//!
//! 1. **The sibling spawn + no-RST listener inheritance** ([`spawn_receiver_sibling`]). The OLD
//!    process spawns the NEW binary with `IRONCACHE_HANDOFF_ROLE=receiver` +
//!    `IRONCACHE_HANDOFF_SOCKET=<path>` and, when it holds a client listener, DUPLICATES that listen
//!    socket into the child at a well-known fd (clearing close-on-exec) and names it in
//!    `IRONCACHE_HANDOFF_LISTEN_FD`. The child adopts it through the SAME shipped #389
//!    `ironcache_runtime::tokio_rt::adopt_listener_fd` path the systemd socket-activation boot uses, so the
//!    listen backlog is never closed and no queued/arriving client connection is `ECONNRESET` across
//!    the flip (Decision 1). See [`spawn_receiver_sibling`] for the exact mechanism.
//!
//! 2. **The OLD-side cutover driver** ([`run_sender_cutover`]). Drives, across ALL shards, the
//!    phase-ordered sequence the pieces already built: freeze+bulk while STILL SERVING (Phase 1),
//!    then a single tight write-outage covering quiesce (Phase 2) + final delta + PREPARED (Phase 3),
//!    then the cross-shard [`decide_cutover`] barrier. On all-prepared it RELEASES write authority and
//!    COMMITs every shard; on any failure it ABORTs and RESUMES full serving on every shard. Bulk (and
//!    its heavy fsync on the receiver) runs OUTSIDE the outage, so the client-visible write stall is
//!    bounded by the final delta + a bounded delta fsync + the flip, independent of keyspace size.
//!
//! 3. **The NEW-side cutover driver** ([`run_receiver_cutover`]). The mirror: per shard load the bulk
//!    into a fresh store + fsync it to staging (Phase 1), receive+verify+fsync the bounded delta and
//!    send PREPARED (Phase 3), await the coordinator's cross-shard COMMIT, and on all-committed
//!    atomically PROMOTE staging -> `data_dir`, flip the process-global serve gate to serving
//!    ([`begin_serving_on_commit`]), and answer SERVED. On any abort it adopts nothing and the sibling
//!    exits without ever serving.
//!
//! Both drivers are generic over the socket type so the unit tests below drive both ends over a
//! `tokio::net::UnixStream::pair` on one runtime, and the live wiring / the real-two-process acceptance
//! test drive them over real `tokio::net::UnixStream`s between two processes -- the SAME code path.
//!
//! ## Data-safety invariant (proved by the tests, not by narration)
//!
//! At the COMMIT linearization point every acknowledged write `<= E` lives in the OLD's intact store
//! (until the OLD drains + exits, which is ONLY after COMMIT), the OLD's untouched `data_dir`, AND the
//! NEW's fsynced staging (about to become `data_dir`). The OLD releases authority (its quiesce becomes
//! permanent) BEFORE it sends a single COMMIT frame, so no write is ever acked by both processes (no
//! split-brain), and a kill BEFORE commit loses nothing (the OLD resumes), a kill AFTER commit recovers
//! state@E from the promoted `data_dir` (the boot loader merges the promoted delta log, closing W2).

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use ironcache_persist::ShardManifestEntry;
use ironcache_repl::{ReplId, ReplOffset, ReplRing};
use ironcache_storage::{AccountingHook, EvictionHook, UnixMillis};
use ironcache_store::ShardStore;
use tokio::io::{AsyncRead, AsyncWrite};

use super::commit::{ReceiverFlipBarrier, Staging, decide_cutover, promote, resume_old_shard};
use super::stream::{self, CutoverState, HandoffError, LoadedShard, PreparedShard};

/// The environment variable naming the streamed-handoff ROLE of a spawned process
/// (`receiver`). The default (unset / `sender`) process is the OLD one already running; only the
/// sibling the orchestrator spawns is a `receiver`. Mirrors `ironcache-config`'s `IRONCACHE_HANDOFF_ROLE`.
pub const HANDOFF_ROLE_ENV: &str = "IRONCACHE_HANDOFF_ROLE";

/// The environment variable naming the node-local AF_UNIX rendezvous socket both ends of a streamed
/// handoff agree on. Mirrors `ironcache-config`'s `IRONCACHE_HANDOFF_SOCKET`.
pub const HANDOFF_SOCKET_ENV: &str = "IRONCACHE_HANDOFF_SOCKET";

/// The environment variable naming the INHERITED client-listener file descriptor the OLD process
/// hands the spawned sibling for the no-RST guarantee (Decision 1). When set, the boot adopts THIS fd
/// through the shipped `ironcache_runtime::tokio_rt::adopt_listener_fd` path (the same one systemd
/// socket-activation uses) instead of binding its own, so the listen queue is never closed across the
/// flip. Absent on every default boot, so the default listener path is byte-unchanged. Single source
/// of truth: the runtime crate's `HANDOFF_LISTEN_FD_ENV`, which [`listener_for`] consults at boot.
///
/// [`listener_for`]: ironcache_runtime::tokio_rt::listener_for
pub const HANDOFF_LISTEN_FD_ENV: &str = ironcache_runtime::tokio_rt::HANDOFF_LISTEN_FD_ENV;

/// The well-known fd number the inherited client listener is duplicated onto in the spawned child
/// (`SD_LISTEN_FDS_START`, matching the systemd socket-activation convention). The child reads
/// [`HANDOFF_LISTEN_FD_ENV`] to learn which fd to adopt; the orchestrator always places it here.
#[cfg(unix)]
const CHILD_LISTEN_FD: std::os::fd::RawFd = 3;

/// SPAWN the NEW-version binary as a sibling RECEIVER (#391 PR-6, Phase 0) and, when a client listener
/// fd is supplied, hand it the SAME listen socket so no client connection is reset across the flip.
///
/// The child is spawned with `IRONCACHE_HANDOFF_ROLE=receiver` + `IRONCACHE_HANDOFF_SOCKET=<socket>`
/// (which the config layer reads into `handoff_role` / `handoff_socket`, driving the coordinator's
/// receiver boot-substitution), plus any `server_args` (e.g. `server`, `--config ...`).
///
/// ## The no-RST listener inheritance (Decision 1), EXACTLY
///
/// When `listen_fd` is `Some`, a [`std::os::unix::process::CommandExt::pre_exec`] hook runs in the
/// forked child (before `exec`) and, using only async-signal-safe `dup2`/`fcntl`:
///
/// 1. `dup2(listen_fd, `[`CHILD_LISTEN_FD`]`)` duplicates the OLD's listen socket onto the well-known
///    child fd. `dup2` produces a duplicate with close-on-exec CLEARED, so it survives the `exec`
///    (the original inherited fd stays close-on-exec and is closed by `exec`, leaving exactly one copy
///    in the child). When the source already IS [`CHILD_LISTEN_FD`], close-on-exec is cleared in place.
/// 2. `IRONCACHE_HANDOFF_LISTEN_FD` is set to [`CHILD_LISTEN_FD`], so the child's boot adopts that fd
///    via the shipped `ironcache_runtime::tokio_rt::adopt_listener_fd` (#389) -- the listen queue is never
///    closed, so a client queued in the backlog or arriving during the flip is served by the NEW, not
///    reset (Decision 1's never-closed-listener guarantee).
///
/// This is a DEDICATED, minimal fd-passing rather than the full systemd `LISTEN_PID` protocol,
/// because the `LISTEN_PID` value must equal the CHILD's pid, which a fork+exec parent cannot know
/// before the fork and cannot inject into the already-built exec environment. Passing the fd number
/// explicitly (and duplicating the socket into place in `pre_exec`) reuses the exact `adopt_listener_fd`
/// no-close primitive without that pid race. systemd socket-activation is UNAFFECTED (the child still
/// honors `LISTEN_*` when this var is absent).
///
/// # Errors
/// The underlying [`std::process::Command::spawn`] error (spawn failure, or a `pre_exec` `dup2`
/// failure surfaced as the child failing to start).
#[cfg(unix)]
pub fn spawn_receiver_sibling(
    program: &Path,
    server_args: &[&str],
    socket: &Path,
    listen_fd: Option<std::os::fd::RawFd>,
) -> std::io::Result<std::process::Child> {
    let mut cmd = std::process::Command::new(program);
    cmd.args(server_args);
    cmd.env(HANDOFF_ROLE_ENV, "receiver");
    cmd.env(HANDOFF_SOCKET_ENV, socket);
    if let Some(fd) = listen_fd {
        cmd.env(HANDOFF_LISTEN_FD_ENV, CHILD_LISTEN_FD.to_string());
        // The unsafe `pre_exec` fd-duplication lives in `ironcache-runtime` (which permits unsafe);
        // this `#![forbid(unsafe_code)]` crate just drives the spawn. It dup2's the OLD's listen
        // socket onto CHILD_LISTEN_FD with close-on-exec cleared, so the child inherits it and adopts
        // it via `adopt_listener_fd` (the never-closed-listener no-RST path).
        ironcache_runtime::tokio_rt::command_inherit_listener(&mut cmd, fd, CHILD_LISTEN_FD);
    }
    cmd.spawn()
}

// ---------------------------------------------------------------------------------------------
// The OLD (sender) side: the whole-node cutover driver.
// ---------------------------------------------------------------------------------------------

/// One shard the OLD process streams to the NEW: its accepted handoff stream + the LIVE store/ring the
/// serve path keeps mutating during Phase 1. The store is shared (`Rc<RefCell<..>>`) so concurrent
/// clients keep writing through the bulk transfer; the driver borrows it only at the brief synchronous
/// cut/quiesce points, never across an `.await`.
pub struct SenderShard<E: EvictionHook, A: AccountingHook, S> {
    /// The accepted per-shard handoff stream to this shard's receiver.
    pub stream: S,
    /// The shard's LIVE store (kept serving reads AND writes during Phase 1).
    pub store: Rc<RefCell<ShardStore<E, A>>>,
    /// The shard's always-on replication observer ring (the atomic-cut floor + the delta source).
    pub ring: Rc<RefCell<ReplRing>>,
    /// The shard index carried in the HELLO.
    pub shard: u32,
}

/// The OLD process's terminal decision for a streamed cutover, returned by [`run_sender_cutover`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenderDecision {
    /// Every shard prepared, authority was released, every shard committed AND the NEW answered
    /// SERVED. The OLD has permanently quiesced and MUST now drain in-flight requests + `exit(0)`; it
    /// never acks a write again.
    Committed,
    /// The flip aborted (a shard failed, or the NEW aborted): every shard was told to abort and
    /// RESUMED full serving. The OLD keeps write authority and does NOT exit.
    Aborted,
}

/// Drive the OLD side of a streamed live cutover across every shard (#391 PR-6). Reuses the tested
/// PR-1..PR-5 primitives; the ONLY new logic is the phase ordering that keeps the write outage tight
/// and the cross-shard commit/abort fan-out.
///
/// - **Phase 1 (OLD still serving reads AND writes):** for each shard, atomically freeze the cut `F`
///   and stream the frozen bulk, then wait for the receiver's HEAVY bulk fsync (`BulkStaged`) so it is
///   OUTSIDE the outage (Refinement A). The freeze flag is cleared (`end_save`) in ALL cases.
/// - **Phase 2 (the write outage begins):** quiesce EVERY shard back-to-back -- latch each `E`, gate
///   client writes with `-LOADING`, and suspend internal mutators. Done together so the outage window
///   is one tight span rather than staggered per shard.
/// - **Phase 3 (still in the outage):** for each shard ship the bounded final delta `(F, E]` and await
///   `Prepared`.
/// - **Barrier + linearization:** [`decide_cutover`] over the per-shard results. On COMMIT it releases
///   authority (the quiesce is now permanent) and sends `Commit` to every shard, then gathers `Served`
///   -> [`SenderDecision::Committed`] (the caller drains + exits). On ABORT (any shard failed, or a
///   Phase-1/2 error) it sends `Abort` and [`resume_old_shard`]s every shard -> [`SenderDecision::Aborted`]
///   (the caller resumes full serving).
///
/// On ANY error the OLD store is intact (the sender only reads it), so the caller keeps serving on the
/// durable fallback.
///
/// # Errors
/// A [`HandoffError`] only when the cutover could not reach a clean commit-or-abort decision (e.g. a
/// `Served` never arrived after COMMIT -> the caller enters read-only degraded standby, W3: it does
/// NOT resume writes and does NOT exit). A shard failing to prepare is NOT an error -- it is a clean
/// [`SenderDecision::Aborted`].
pub async fn run_sender_cutover<E, A, S>(
    shards: &mut [SenderShard<E, A, S>],
    replid: ReplId,
    now: UnixMillis,
    chunk_max: usize,
) -> Result<SenderDecision, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    // PHASE 1: freeze + bulk + await the receiver's bulk fsync, per shard, WHILE STILL SERVING. A
    // failure here (socket error / peer abort) short-circuits to a clean abort: nothing has quiesced.
    let mut floors: Vec<ReplOffset> = Vec::with_capacity(shards.len());
    for s in shards.iter_mut() {
        match sender_phase1_bulk(s, replid, now, chunk_max).await {
            Ok(floor) => floors.push(floor),
            Err(_e) => {
                // Roll back cleanly: tell every receiver to abort and resume every shard. Nothing was
                // quiesced yet, but resume is idempotent (unquiesce + passive off).
                abort_all_and_resume(shards).await;
                return Ok(SenderDecision::Aborted);
            }
        }
    }

    // PHASE 2: quiesce EVERY shard (the outage begins). One tight span, not staggered.
    let mut ends: Vec<ReplOffset> = Vec::with_capacity(shards.len());
    for s in shards.iter_mut() {
        let e = super::commit::quiesce_old_shard(&mut s.store.borrow_mut(), &s.ring);
        ends.push(e);
    }

    // PHASE 3: ship the bounded final delta (F, E] and await PREPARED, per shard.
    let mut results: Vec<Result<ReplOffset, HandoffError>> = Vec::with_capacity(shards.len());
    for (i, s) in shards.iter_mut().enumerate() {
        let r =
            stream::send_delta_await_prepared(&mut s.stream, &s.ring, floors[i], chunk_max).await;
        results.push(r);
    }

    // THE BARRIER + THE LINEARIZATION POINT: all-prepared -> release authority, THEN commit.
    let (state, _authority) = decide_cutover(&results);
    match state {
        CutoverState::Commit => {
            // Authority is RELEASED: the quiesce is now permanent (the OLD never acks a write again).
            // Send COMMIT to every shard BEFORE gathering any Served, so the receiver can gather every
            // shard's Commit and promote once before it answers.
            for s in shards.iter_mut() {
                stream::send_commit(&mut s.stream).await?;
            }
            // Gather SERVED from every shard. A missing Served (W3) surfaces as an error: the caller
            // enters read-only degraded standby (no resume, no exit) -- it never resumes writes.
            for s in shards.iter_mut() {
                stream::await_served(&mut s.stream).await?;
            }
            Ok(SenderDecision::Committed)
        }
        CutoverState::Pending | CutoverState::Abort => {
            abort_all_and_resume(shards).await;
            Ok(SenderDecision::Aborted)
        }
    }
}

/// Phase 1 for ONE shard: atomic freeze-cut `F` + frozen bulk, then await the receiver's `BulkStaged`
/// (its heavy fsync), clearing the freeze flag in ALL cases. Returns the floor `F`.
///
/// `pub(crate)` so BOTH the single-thread [`run_sender_cutover`] sequencer (kept for the existing
/// hero tests) AND the #638 per-shard [`super::cutover_coord::run_shard_cutover_task`] drive the SAME
/// proven Phase-1 step -- the two sender paths share this one primitive so they cannot drift.
pub(crate) async fn sender_phase1_bulk<E, A, S>(
    s: &mut SenderShard<E, A, S>,
    replid: ReplId,
    now: UnixMillis,
    chunk_max: usize,
) -> Result<ReplOffset, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let databases = u32::try_from(s.store.borrow().databases()).unwrap_or(u32::MAX);
    let (frozen, floor) = {
        let mut store = s.store.borrow_mut();
        stream::freeze_cut(&mut store, &s.ring)
    };
    let outcome = async {
        stream::send_bulk_from_frozen(
            &mut s.stream,
            &frozen,
            s.shard,
            databases,
            replid,
            floor,
            now,
            chunk_max,
        )
        .await?;
        stream::await_bulk_staged(&mut s.stream).await
    }
    .await;
    drop(frozen);
    s.store.borrow_mut().end_save(); // clear the freeze flag even on a mid-bulk abort.
    outcome.map(|()| floor)
}

/// ABORT every shard's receiver (best-effort frame) and RESUME full serving on every OLD shard
/// ([`resume_old_shard`]: unquiesce + restore lazy expiry). Idempotent, so it is safe on the
/// Phase-1 abort edge (nothing quiesced yet) and the barrier abort edge (every shard quiesced).
async fn abort_all_and_resume<E, A, S>(shards: &mut [SenderShard<E, A, S>])
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    for s in shards.iter_mut() {
        let _ = stream::send_abort_frame(&mut s.stream).await;
        resume_old_shard(&mut s.store.borrow_mut());
    }
}

// ---------------------------------------------------------------------------------------------
// The NEW (receiver) side: the whole-node cutover driver.
// ---------------------------------------------------------------------------------------------

/// The NEW process's terminal outcome of a streamed cutover, returned by [`run_receiver_cutover`].
pub enum ReceiverOutcome<E: EvictionHook, A: AccountingHook> {
    /// Every shard committed: staging was promoted to `data_dir`, the process-global serve gate was
    /// flipped to serving, and SERVED was answered. Carries every shard's adopted store for the caller
    /// to install into its thread-local serve path.
    Committed(Vec<LoadedShard<E, A>>),
    /// The flip aborted: the NEW adopted NOTHING and must exit without ever serving.
    Aborted,
}

/// Drive the NEW side of a streamed live cutover across every shard (#391 PR-6). The mirror of
/// [`run_sender_cutover`], reusing the tested PR-4 receiver-authoritative primitives.
///
/// - **Phase 1:** per shard, load the bulk into a fresh store and FSYNC it to `staging` (the heavy
///   fsync, outside the outage), then signal `BulkStaged`.
/// - **Phase 3:** per shard, receive + verify the bounded delta `(F, E]`, FSYNC it to `staging`, and
///   send `Prepared`.
/// - **Await + flip:** per shard, await the coordinator's COMMIT/ABORT. On ALL committed, write the
///   staging manifest, atomically [`promote`] `staging -> data_dir` (rename + dir fsync), flip the
///   process-global serve gate to serving ([`begin_serving_on_commit`]), and answer `Served` on every
///   shard -> [`ReceiverOutcome::Committed`] with every adopted store. On ANY abort/error, adopt
///   nothing -> [`ReceiverOutcome::Aborted`].
///
/// `data_dir` MUST NOT already exist (a fresh receiver boot dir, or the caller cleared it) -- [`promote`]
/// renames staging onto it. `save_unix_secs` stamps the promoted manifest (`LASTSAVE`).
///
/// # Errors
/// A [`HandoffError`] from the transport (a socket failure, a verify/contiguity failure, a staging
/// fsync failure, or a peer abort). On error the NEW adopts nothing and the caller exits unserving.
pub async fn run_receiver_cutover<E, A, S, M>(
    streams: &mut [S],
    mut make_store: M,
    expected_databases: u32,
    now: UnixMillis,
    staging: &Staging,
    data_dir: &Path,
    save_unix_secs: u64,
) -> Result<ReceiverOutcome<E, A>, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
    M: FnMut() -> ShardStore<E, A>,
{
    // PHASE 1: bulk-load + heavy fsync + BulkStaged, per shard.
    let mut staged: Vec<(ShardStore<E, A>, u32, ReplOffset, ShardManifestEntry)> =
        Vec::with_capacity(streams.len());
    for stream_io in streams.iter_mut() {
        let (store, shard, floor) =
            stream::recv_bulk(stream_io, &mut make_store, expected_databases, now).await?;
        let entry = staging.stage_bulk(&store, shard, now)?;
        stream::send_bulk_staged(stream_io).await?;
        staged.push((store, shard, floor, entry));
    }

    // PHASE 3: apply + verify the delta, fsync the bounded delta, send PREPARED, per shard.
    let mut prepared: Vec<PreparedShard<E, A>> = Vec::with_capacity(streams.len());
    let mut entries: Vec<ShardManifestEntry> = Vec::with_capacity(streams.len());
    for (i, stream_io) in streams.iter_mut().enumerate() {
        let (store, shard, floor, entry) = staged
            .get_mut(i)
            .map(|slot| {
                (
                    std::mem::replace(&mut slot.0, make_store()),
                    slot.1,
                    slot.2,
                    slot.3.clone(),
                )
            })
            .expect("staged slot present for every stream");
        let p = stream::recv_prepare_only(stream_io, store, shard, floor, now, |_s, delta| {
            staging.stage_delta(shard, delta)
        })
        .await?;
        prepared.push(p);
        entries.push(entry);
    }

    // AWAIT the coordinator's cross-shard COMMIT/ABORT, per shard.
    let mut committed: Vec<LoadedShard<E, A>> = Vec::with_capacity(streams.len());
    let mut all_committed = true;
    for (i, stream_io) in streams.iter_mut().enumerate() {
        let p = std::mem::replace(
            &mut prepared[i],
            PreparedShard {
                store: make_store(),
                shard: 0,
                final_offset: ReplOffset::ZERO,
            },
        );
        match stream::recv_await_commit(stream_io, p).await? {
            stream::ShardCommit::Committed(loaded) => committed.push(*loaded),
            stream::ShardCommit::Aborted => {
                all_committed = false;
                break;
            }
        }
    }

    if !all_committed || committed.len() != streams.len() {
        // Adopt NOTHING: drop the received stores + discard staging. The sibling exits unserving.
        return Ok(ReceiverOutcome::Aborted);
    }

    // THE FLIP (all committed): manifest LAST, atomic promote, then begin serving, then SERVED. Every
    // acked write <= E for every shard is already fsync'd to staging (Phase 1 bulk + Phase 3 delta),
    // so the promote publishes a durable state@E, and only THEN does the process begin serving. The
    // single all-or-nothing SERVING flip is routed through a [`ReceiverFlipBarrier`] -- the SAME gather
    // the live per-shard sibling uses -- so this whole-node driver and the multi-shard sibling share
    // one flip authority: report one commit per adopted shard; the barrier flips serving EXACTLY ONCE,
    // on the Nth (last) report. Single-threaded here, so it is trivially all-or-nothing.
    staging.write_manifest(save_unix_secs, entries)?;
    promote(staging.dir(), data_dir)?;
    let flip = ReceiverFlipBarrier::new(committed.len());
    for _ in &committed {
        flip.report_committed();
    }
    for stream_io in streams.iter_mut() {
        stream::send_served(stream_io).await?;
    }
    Ok(ReceiverOutcome::Committed(committed))
}

/// The socket path the receiver sibling dials / the sender binds, resolved from
/// [`HANDOFF_SOCKET_ENV`] when present (a spawned sibling), else `None`. A convenience for a caller
/// that wants the orchestrator's env contract without threading the full config.
#[must_use]
pub fn handoff_socket_from_env() -> Option<PathBuf> {
    std::env::var_os(HANDOFF_SOCKET_ENV).map(PathBuf::from)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use ironcache_repl::{ReplObserver, encode_kvobj};
    use ironcache_storage::{ExpireWrite, NewValue, Store};
    use ironcache_store::{ShardStore, SnapshotCursor};
    use std::collections::HashMap;
    use tokio::net::UnixStream;

    const NOW: UnixMillis = UnixMillis(1_000);
    const DBS: u32 = 4;

    /// A live OLD shard's store + always-on ring handle pair.
    type ShardHandles = (Rc<RefCell<ShardStore>>, Rc<RefCell<ReplRing>>);
    /// The write ledger: the last acked value per `(shard, db, key)`.
    type Ledger = Rc<RefCell<HashMap<(u32, u32, Vec<u8>), Vec<u8>>>>;
    /// A shard's whole keyspace dumped as `(db, key) -> encoded-KvObj` (value + type + absolute TTL).
    type ShardDump = HashMap<(u32, Vec<u8>), Vec<u8>>;

    fn replid() -> ReplId {
        ReplId::from_bytes([0x33; 20])
    }

    fn tmp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ic-orch-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    /// A fresh store with an always-on observer ring, seeded with `n` keys.
    fn seeded(n: u32, tag: &str) -> (Rc<RefCell<ShardStore>>, Rc<RefCell<ReplRing>>) {
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

    /// The whole-node driver, COMMIT path, MULTIPLE shards over real socketpairs: every adopted NEW
    /// store equals EXACTLY its OLD store, staging is promoted, and the OLD's quiesce is permanent.
    #[tokio::test(flavor = "current_thread")]
    async fn multi_shard_cutover_commits_and_adopts_every_shard() {
        const SHARDS: u32 = 3;
        const PER: u32 = 400;
        crate::serve::unquiesce_shard();
        crate::serve::set_serving(true);

        let root = tmp_root("commit");
        let _ = std::fs::remove_dir_all(&root);
        let staging = Staging::new(root.join("staging")).expect("staging");
        let data_dir = root.join("data");

        // Build per-shard OLD state + a socketpair per shard. `keepalive` holds the original store/ring
        // Rc handles for the whole test (the sender clones them), so they outlive the drivers.
        let mut keepalive: Vec<ShardHandles> = Vec::new();
        let mut sender_shards: Vec<SenderShard<_, _, UnixStream>> = Vec::new();
        let mut recv_streams: Vec<UnixStream> = Vec::new();
        let mut references: Vec<ShardDump> = Vec::new();
        for shard in 0..SHARDS {
            let (store, ring) = seeded(PER, &format!("s{shard}"));
            references.push(dump_map(&store.borrow(), NOW));
            let (a, b) = UnixStream::pair().expect("socketpair");
            sender_shards.push(SenderShard {
                stream: a,
                store: Rc::clone(&store),
                ring: Rc::clone(&ring),
                shard,
            });
            recv_streams.push(b);
            keepalive.push((store, ring));
        }

        // Drive BOTH sides concurrently on one runtime.
        let sender = run_sender_cutover(&mut sender_shards, replid(), NOW, 4);
        let receiver = run_receiver_cutover(
            &mut recv_streams,
            || ShardStore::new(DBS),
            DBS,
            NOW,
            &staging,
            &data_dir,
            NOW.0 / 1000,
        );
        let (s_res, r_res) = tokio::join!(sender, receiver);

        assert_eq!(
            s_res.expect("sender drove to a decision"),
            SenderDecision::Committed,
            "every shard prepared -> the OLD commits + is served"
        );
        let adopted = match r_res.expect("receiver drove to a decision") {
            ReceiverOutcome::Committed(shards) => shards,
            ReceiverOutcome::Aborted => panic!("expected a commit"),
        };
        assert_eq!(adopted.len(), SHARDS as usize, "every shard was adopted");
        // (The `Committed` outcome already implies `begin_serving_on_commit` flipped the process-global
        // serve gate; we do not assert the shared global here since parallel tests also toggle it.)
        assert!(
            crate::serve::is_shard_loading(),
            "the OLD's quiesce is PERMANENT after the release"
        );

        // Every adopted NEW store == its OLD store, and the promoted durable dir reconstructs the same.
        for loaded in &adopted {
            let want = &references[loaded.shard as usize];
            assert_eq!(
                &dump_map(&loaded.store, NOW),
                want,
                "adopted shard {} == OLD state@E",
                loaded.shard
            );
            let durable =
                super::super::commit::reconstruct_shard(&data_dir, loaded.shard, NOW, || {
                    ShardStore::new(DBS)
                })
                .expect("promoted dir reconstructs the shard");
            assert_eq!(
                &dump_map(&durable, NOW),
                want,
                "durable data_dir shard {} == OLD state@E (bulk UNION delta)",
                loaded.shard
            );
        }

        crate::serve::unquiesce_shard();
        crate::serve::set_serving(true);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The whole-node driver, ABORT path: one shard's receiver drops its socket before PREPARED, so
    /// the barrier aborts, the OLD resumes EVERY shard (never releases authority), and the NEW adopts
    /// nothing (no promote).
    #[tokio::test(flavor = "current_thread")]
    async fn one_shard_failure_aborts_the_whole_flip_and_old_resumes() {
        const SHARDS: u32 = 3;
        const PER: u32 = 120;
        crate::serve::unquiesce_shard();
        crate::serve::set_serving(true);

        let root = tmp_root("abort");
        let _ = std::fs::remove_dir_all(&root);
        let staging = Staging::new(root.join("staging")).expect("staging");
        let data_dir = root.join("data");

        let mut keepalive: Vec<ShardHandles> = Vec::new();
        let mut sender_shards: Vec<SenderShard<_, _, UnixStream>> = Vec::new();
        let mut recv_streams: Vec<UnixStream> = Vec::new();
        for shard in 0..SHARDS {
            let (store, ring) = seeded(PER, &format!("s{shard}"));
            let (a, b) = UnixStream::pair().expect("socketpair");
            sender_shards.push(SenderShard {
                stream: a,
                store: Rc::clone(&store),
                ring: Rc::clone(&ring),
                shard,
            });
            recv_streams.push(b);
            keepalive.push((store, ring));
        }

        // The receiver for shard 1 will DROP its stream mid-handoff (a crash), so the sender's shard-1
        // stream sees EOF and the whole flip aborts. Drive shards 0/2 normally, all concurrent with
        // the sender.
        let sender = run_sender_cutover(&mut sender_shards, replid(), NOW, 1);
        let receiver = async {
            // Own the three receiver ends so the middle one can be DROPPED (a crash).
            let mut it = recv_streams.into_iter();
            let mut s0 = it.next().unwrap();
            let s1 = it.next().unwrap();
            let mut s2 = it.next().unwrap();
            // Crash shard 1: drop its stream at once, so the sender's shard-1 HELLO hits a broken pipe.
            drop(s1);
            // Shards 0 and 2 run their real receive-to-prepared; when the barrier aborts they get an
            // Abort/EOF and adopt nothing.
            let good0 = crate::upgrade::commit::receive_shard_to_prepared(
                &mut s0,
                || ShardStore::new(DBS),
                DBS,
                NOW,
                &staging,
            );
            let good2 = crate::upgrade::commit::receive_shard_to_prepared(
                &mut s2,
                || ShardStore::new(DBS),
                DBS,
                NOW,
                &staging,
            );
            let (r0, r2) = tokio::join!(good0, good2);
            (r0, r2)
        };
        let (s_res, _r) = tokio::join!(sender, receiver);

        assert_eq!(
            s_res.expect("sender drove to a decision"),
            SenderDecision::Aborted,
            "a shard crash aborts the whole flip"
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
        crate::serve::set_serving(true);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// ACCEPTANCE (deterministic core): under SUSTAINED write load, the client-visible WRITE STALL is
    /// sub-second AND there is ZERO acknowledged-write loss. A writer hammers the OLD stores recording
    /// every acked value into a ledger and STOPS the instant the shard quiesces (the `-LOADING`
    /// gate); a monitor timestamps the outage as [first quiesce -> the NEW serve-flip]. After the
    /// cutover EVERY ledgered value is present in the adopted NEW store, and the outage is `< 1s`.
    /// Bulk (and its fsync) runs while the writer is still writing, so the outage excludes it -- the
    /// measured stall is the delta + flip only, independent of keyspace size.
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::too_many_lines)]
    async fn cutover_write_stall_is_sub_second_and_zero_acked_loss() {
        use ironcache_env::Clock;
        use std::cell::Cell;

        const SHARDS: u32 = 2;
        const SEED: u32 = 5_000; // a non-trivial bulk, to show it does NOT widen the outage.
        crate::serve::unquiesce_shard();
        crate::serve::set_serving(false); // model the NEW boot: not serving until the commit flip.

        let root = tmp_root("stall");
        let _ = std::fs::remove_dir_all(&root);
        let staging = Staging::new(root.join("staging")).expect("staging");
        let data_dir = root.join("data");

        let mut keepalive: Vec<ShardHandles> = Vec::new();
        let mut writer_stores: Vec<Rc<RefCell<ShardStore>>> = Vec::new();
        let mut sender_shards: Vec<SenderShard<_, _, UnixStream>> = Vec::new();
        let mut recv_streams: Vec<UnixStream> = Vec::new();
        for shard in 0..SHARDS {
            let (store, ring) = seeded(SEED, &format!("s{shard}"));
            let (a, b) = UnixStream::pair().expect("socketpair");
            sender_shards.push(SenderShard {
                stream: a,
                store: Rc::clone(&store),
                ring: Rc::clone(&ring),
                shard,
            });
            recv_streams.push(b);
            writer_stores.push(Rc::clone(&store));
            keepalive.push((store, ring));
        }

        // The ledger: last acked value per (shard, db, key). SET-only, so every ledgered key MUST be
        // present with its acked value in the NEW store (no delete to reconcile).
        let ledger: Ledger = Rc::new(RefCell::new(HashMap::new()));
        let writer = {
            let ledger = Rc::clone(&ledger);
            async move {
                let mut n: u64 = 0;
                loop {
                    // The `-LOADING` gate, checked at the TOP of each SYNCHRONOUS batch (no await
                    // inside a batch, so a quiesce cannot land mid-batch): once quiescing, the writer
                    // does NO further write, so nothing is acked above E.
                    if crate::serve::is_shard_loading() || n >= 200_000 {
                        break;
                    }
                    for (shard, store) in writer_stores.iter().enumerate() {
                        let mut s = store.borrow_mut();
                        for _ in 0..32 {
                            let db = (n as u32) % DBS;
                            let key = format!("w-{shard}-{n}");
                            let val = format!("val-{shard}-{n}");
                            s.upsert(
                                db,
                                key.as_bytes(),
                                NewValue::Bytes(val.as_bytes()),
                                ExpireWrite::Clear,
                                NOW,
                            );
                            ledger
                                .borrow_mut()
                                .insert((shard as u32, db, key.into_bytes()), val.into_bytes());
                            n += 1;
                        }
                    }
                    tokio::task::yield_now().await;
                }
                n
            }
        };

        // The monitor timestamps the client-visible write outage: first quiesce (the OLD stops acking)
        // -> the NEW serve-flip (the NEW starts acking). Capped so it never spins if serving never
        // flips.
        let t_quiesce: Rc<Cell<Option<ironcache_env::Monotonic>>> = Rc::new(Cell::new(None));
        let t_serving: Rc<Cell<Option<ironcache_env::Monotonic>>> = Rc::new(Cell::new(None));
        let monitor = {
            let t_quiesce = Rc::clone(&t_quiesce);
            let t_serving = Rc::clone(&t_serving);
            async move {
                // ADR-0003: measure through the ironcache-env monotonic Clock seam, not
                // std::time::Instant directly (the invariant lint forbids raw time here).
                let clock = ironcache_env::SystemEnv::new();
                for _ in 0..50_000_000u64 {
                    if t_quiesce.get().is_none() && crate::serve::is_shard_loading() {
                        t_quiesce.set(Some(clock.now()));
                    }
                    if t_serving.get().is_none() && crate::serve::is_serving() {
                        t_serving.set(Some(clock.now()));
                    }
                    if t_quiesce.get().is_some() && t_serving.get().is_some() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
        };

        let sender = run_sender_cutover(&mut sender_shards, replid(), NOW, 64);
        let receiver = run_receiver_cutover(
            &mut recv_streams,
            || ShardStore::new(DBS),
            DBS,
            NOW,
            &staging,
            &data_dir,
            NOW.0 / 1000,
        );
        let (wrote, (), s_res, r_res) = tokio::join!(writer, monitor, sender, receiver);

        assert_eq!(
            s_res.expect("sender drove to a decision"),
            SenderDecision::Committed,
            "the cutover committed"
        );
        let mut adopted = match r_res.expect("receiver drove to a decision") {
            ReceiverOutcome::Committed(shards) => shards,
            ReceiverOutcome::Aborted => panic!("expected a commit"),
        };
        assert!(
            wrote > 1_000,
            "the writer genuinely hammered ({wrote} writes)"
        );

        // THE MEASURED STALL: quiesce -> serve-flip.
        let q = t_quiesce
            .get()
            .expect("the outage began (quiesce observed)");
        let s = t_serving.get().expect("the NEW flipped to serving");
        let stall_ms = s.saturating_duration_since(q).as_secs_f64() * 1000.0;
        println!(
            "MEASURED write-stall (quiesce -> serve-flip): {stall_ms:.3} ms (seed {SEED}/shard)"
        );
        assert!(
            stall_ms < 1000.0,
            "the client-visible write stall must be sub-second (was {stall_ms:.3} ms)"
        );

        // ZERO ACKNOWLEDGED-WRITE LOSS: every ledgered value is present in the adopted NEW store.
        let led = ledger.borrow();
        assert!(!led.is_empty(), "the writer acked at least one write");
        let mut checked = 0usize;
        for loaded in &mut adopted {
            for ((shard, db, key), val) in led.iter() {
                if *shard != loaded.shard {
                    continue;
                }
                let got = loaded
                    .store
                    .read(*db, key.as_slice(), NOW)
                    .unwrap_or_else(|| panic!("acked key on shard {shard} missing after cutover"));
                assert_eq!(
                    got.as_bytes(),
                    val.as_slice(),
                    "acked write must survive on the NEW (zero acked-write loss)"
                );
                checked += 1;
            }
        }
        assert_eq!(
            checked,
            led.len(),
            "every ledgered acked write was found on its shard"
        );
        drop(led);

        crate::serve::unquiesce_shard();
        crate::serve::set_serving(true);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// ACCEPTANCE (kill AFTER commit): a NEW that crashes after `Commit` RESTARTS from the promoted
    /// `data_dir` and recovers state@E via the boot-loader merge (bulk + promoted delta). This drives
    /// a real 1-shard cutover under a writer (so the promoted delta is NON-empty), then exercises the
    /// EXACT boot recovery wiring: load the bulk (state@F), then [`replay_promoted_delta`] the tail ->
    /// state@E. Closes W2 end to end.
    #[tokio::test(flavor = "current_thread")]
    async fn post_commit_crash_recovers_state_at_e_from_promoted_data_dir() {
        crate::serve::unquiesce_shard();
        crate::serve::set_serving(false);

        let root = tmp_root("recover");
        let _ = std::fs::remove_dir_all(&root);
        let staging = Staging::new(root.join("staging")).expect("staging");
        let data_dir = root.join("data");

        let (store, ring) = seeded(300, "s0");
        let (a, b) = UnixStream::pair().expect("socketpair");
        let mut sender_shards = vec![SenderShard {
            stream: a,
            store: Rc::clone(&store),
            ring: Rc::clone(&ring),
            shard: 0,
        }];
        let mut recv_streams = vec![b];

        // A writer creates a NON-empty (F, E] delta by writing during the transfer.
        let writer_store = Rc::clone(&store);
        let writer = async move {
            let mut n: u64 = 0;
            loop {
                if crate::serve::is_shard_loading() || n >= 50_000 {
                    break;
                }
                {
                    let mut s = writer_store.borrow_mut();
                    for _ in 0..16 {
                        s.upsert(
                            0,
                            format!("delta-{n}").as_bytes(),
                            NewValue::Bytes(format!("dv-{n}").as_bytes()),
                            ExpireWrite::Clear,
                            NOW,
                        );
                        n += 1;
                    }
                }
                tokio::task::yield_now().await;
            }
            n
        };
        let sender = run_sender_cutover(&mut sender_shards, replid(), NOW, 8);
        let receiver = run_receiver_cutover(
            &mut recv_streams,
            || ShardStore::new(DBS),
            DBS,
            NOW,
            &staging,
            &data_dir,
            NOW.0 / 1000,
        );
        let (_wrote, s_res, r_res) = tokio::join!(writer, sender, receiver);
        assert_eq!(s_res.expect("decision"), SenderDecision::Committed);
        let adopted = match r_res.expect("decision") {
            ReceiverOutcome::Committed(shards) => shards,
            ReceiverOutcome::Aborted => panic!("expected a commit"),
        };
        let reference = dump_map(&adopted[0].store, NOW); // state@E the NEW served before the "crash".

        // ---- SIMULATE the NEW crashing post-COMMIT and RESTARTING from the promoted data_dir. ----
        assert!(
            super::super::commit::has_promoted_delta(&data_dir, 0),
            "the promoted dir carries a delta tail"
        );
        let manifest = ironcache_persist::read_manifest(&data_dir).expect("promoted manifest");
        let entry = manifest
            .entries
            .iter()
            .find(|e| e.shard == 0)
            .expect("shard-0 manifest entry")
            .clone();
        // BOOT step 1: load ONLY the bulk (state@F), as today's loader would.
        let mut boot = ShardStore::new(DBS);
        ironcache_persist::load_shard_from_dir(&mut boot, &data_dir, &entry, NOW);
        let bulk_only = dump_map(&boot, NOW);
        // BOOT step 2 (the PR-6 wiring): merge the promoted delta -> state@E.
        let applied = super::super::commit::replay_promoted_delta(&mut boot, &data_dir, 0, NOW)
            .expect("replay the promoted delta");
        assert!(
            applied > 0,
            "the promoted delta was non-empty (writes during the outage window)"
        );
        assert_ne!(
            bulk_only, reference,
            "the bulk alone is state@F (missing the delta) -- the merge is load-bearing"
        );
        assert_eq!(
            dump_map(&boot, NOW),
            reference,
            "bulk + promoted-delta merge recovers EXACTLY state@E (W2 closed on the boot path)"
        );

        crate::serve::unquiesce_shard();
        crate::serve::set_serving(true);
        let _ = std::fs::remove_dir_all(&root);
    }
}
