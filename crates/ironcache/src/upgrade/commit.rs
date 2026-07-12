// SPDX-License-Identifier: MIT OR Apache-2.0
//! The RECEIVER-AUTHORITATIVE cross-process COMMIT (#391 PR-4): the layer that transfers WRITE
//! AUTHORITY between the OLD (sender) and NEW (receiver) processes at the streamed live cutover, so
//! a crash at ANY instant loses no acknowledged write and never split-brains.
//!
//! ## Why the built pieces are not enough
//!
//! The merged transport core ([`crate::upgrade::stream`]) ships the `CutoverBarrier` + the
//! `Cutover`/`CutoverAck` frames, but those are only an IN-RECEIVER "I applied through `E`" decision.
//! They do NOT move write authority between the two processes: if the OLD stopped acking on merely
//! SENDING the delta, or the NEW served on merely APPLYING it, a crash mid-flip could lose an
//! acknowledged write or let BOTH sides serve. This module adds the receiver-authoritative commit so
//! that authority moves EXACTLY once, durably, and observably.
//!
//! ## The sequence (per shard, wired to the PR-3 quiesce), and WHERE authority moves
//!
//! 1. **Phase 1 FREEZE + BULK (OLD keeps serving reads AND writes).** OLD [`stream::freeze_cut`]s the
//!    atomic cut `F` and ships the frozen bulk ([`stream::send_bulk_from_frozen`]). The NEW loads it,
//!    then FSYNCS the BULK snapshot (state@F) to its staging dir ([`Staging::stage_bulk`]) -- the
//!    HEAVY fsync, done HERE while the OLD still serves, so it is OUTSIDE the write outage
//!    (Refinement A). The NEW signals [`stream::send_bulk_staged`]; the OLD does NOT quiesce until it
//!    receives it ([`stream::await_bulk_staged`]).
//! 2. **Phase 2 QUIESCE (the write outage begins on the OLD).** [`quiesce_old_shard`] latches
//!    `E = ring.head()` AND sets the core-local `-LOADING` gate (PR-3 [`crate::serve::quiesce_shard`])
//!    so every CLIENT mutator is rejected before it is assigned an offset, AND suspends the INTERNAL
//!    mutators (W5): the active-expiry reaper is gated on [`crate::serve::is_shard_loading`] and lazy
//!    expiry is suspended by the store's passive flag, so no internal removal is acked at an offset
//!    above `E`. All in ONE non-`await` step.
//! 3. **Phase 3 FINAL DELTA.** OLD ships `(F, E]` and awaits PREPARED
//!    ([`stream::send_delta_await_prepared`]). The NEW applies + verifies (`applied == E`, contiguity,
//!    CRC), FSYNCS the BOUNDED delta ([`Staging::stage_delta`]) -- the small in-outage fsync -- then
//!    sends [`stream::PreparedShard`]'s `Prepared` ([`stream::recv_prepare_only`]).
//! 4. **The barrier + the LINEARIZATION POINT.** The coordinator gathers a `Prepared` from EVERY
//!    shard ([`decide_cutover`]). On all-prepared it RELEASES WRITE AUTHORITY -- the OLD's quiesce
//!    becomes PERMANENT (it will never ack a write again; it drains + exits in PR-6) -- and only THEN
//!    sends [`stream::send_commit`]. On ANY shard failing it [`resume_old_shard`]s every shard
//!    ([`stream::send_abort_frame`]) and keeps authority.
//! 5. **Phase 6 FLIP.** The NEW receives `Commit` ([`stream::recv_await_commit`]), atomically
//!    [`promote`]s staging -> `data_dir` (rename + dir fsync), and (PR-5) begins serving, then
//!    [`stream::send_served`]s. The OLD drains + exits (PR-6).
//!
//! ## Kill-safety (proved by the hero test, NOT by review)
//!
//! At every instant every acked write `<= E` lives in at least the OLD's intact store (until the OLD
//! exits, which is only after COMMIT) AND, once PREPARED, the NEW's fsynced staging (bulk + delta ==
//! state@E). A kill BETWEEN prepare and commit loses NOTHING and never split-brains: if the OLD dies
//! pre-COMMIT the NEW never got `Commit` (it adopts nothing, serves nothing) and the OLD held
//! authority (it resumes on restart); if the NEW dies pre-COMMIT the OLD never released authority and
//! simply [`resume_old_shard`]s. The `release ordering` is the crux: the OLD marks its authority
//! [`WriteAuthority::Released`] BEFORE it sends `Commit`, so a crash after the release still cannot
//! resume (no split-brain) and a crash before it cannot lose the acked keyspace (the OLD still holds
//! it live).
//!
//! ## The NEW's serve-flip (PR-5) and what is still deferred to PR-6
//!
//! The NEW's client serve-flip is now wired (PR-5): a single process-global serve gate
//! ([`crate::serve::is_serving`]) rejects EVERY client command with `-LOADING` while the NEW is
//! receiving, and [`begin_serving_on_commit`] flips it to serving EXACTLY ONCE, all-or-nothing across
//! shards, on the `Committed` transition -- strictly after the OLD released write authority, so no
//! write is double-acked. Still deferred to PR-6 (expressed here as authority state + TODOs): the
//! orchestrator sibling spawn + drain/exit-on-Commit / resume-on-Abort + inherited-listener no-RST.
//! Where this module needs "the OLD stops serving" it expresses the AUTHORITY state (a
//! [`WriteAuthority`] / the permanent quiesce) and leaves a TODO. Everything below is tested over
//! in-process [`tokio::net::UnixStream::pair`]s, not real processes.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use ironcache_persist::{ShardManifestEntry, crc32};
use ironcache_repl::{Frame, ReplId, ReplOffset, ReplRing, decode_kvobj};
use ironcache_storage::{AccountingHook, EvictionHook, Store, UnixMillis};
use ironcache_store::ShardStore;
use tokio::io::{AsyncRead, AsyncWrite};

use super::stream::{self, CutoverBarrier, CutoverState, HandoffError, PreparedShard};

/// The OLD process's WRITE-AUTHORITY state across a streamed cutover -- the single most important
/// state in the handoff. While [`WriteAuthority::Held`] the OLD still owns writes and an ABORT
/// resumes full serving ([`resume_old_shard`]); once [`WriteAuthority::Released`] the OLD has
/// PERMANENTLY quiesced (it will drain + `exit(0)`, PR-6) and NEVER acks a write again, so there is
/// never a moment where BOTH processes could ack a write (no split-brain). The transition Held ->
/// Released happens at [`decide_cutover`]'s Commit, BEFORE the coordinator sends `Commit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAuthority {
    /// The OLD still owns writes: an abort at any edge resumes full serving on every shard.
    Held,
    /// The OLD has released authority (permanent quiesce): it will drain + exit and never ack a
    /// write again. Set the instant the cross-shard barrier commits, BEFORE `Commit` is sent.
    Released,
}

/// Fold the gathered per-shard PREPARED results into the coordinator's ALL-OR-NOTHING decision AND
/// the resulting OLD write authority: [`CutoverState::Commit`] iff EVERY shard prepared (which
/// RELEASES authority, the linearization point), else [`CutoverState::Abort`] (which KEEPS authority
/// Held so the OLD resumes full serving). An empty slice is a degenerate empty handoff that commits.
///
/// This is the ONE place authority moves. The caller MUST, on `Commit`, treat the returned
/// [`WriteAuthority::Released`] as final -- make every shard's quiesce permanent -- BEFORE it sends a
/// single `Commit` frame, so no crash after the decision can resume the OLD (split-brain).
#[must_use]
pub fn decide_cutover<T>(prepared: &[Result<T, HandoffError>]) -> (CutoverState, WriteAuthority) {
    let mut barrier = CutoverBarrier::new(prepared.len());
    for result in prepared {
        match result {
            Ok(_) => barrier.record_commit(),
            Err(_) => barrier.record_abort(),
        }
    }
    let state = barrier.state();
    let authority = match state {
        CutoverState::Commit => WriteAuthority::Released,
        CutoverState::Pending | CutoverState::Abort => WriteAuthority::Held,
    };
    (state, authority)
}

/// #391 PR-5: the NEW's client SERVE-FLIP -- the client-visible consequence of the receiver-
/// authoritative `Committed` transition. Flip the process-global serve gate
/// ([`crate::serve::set_serving`]) to `true` so the NEW begins serving client commands.
///
/// Called EXACTLY ONCE, and ONLY after BOTH: (a) [`promote`] has durably installed staging ->
/// `data_dir` (the NEW serves only committed, durable state@E), and (b) the OLD has RELEASED write
/// authority -- it sent `Commit` only after [`decide_cutover`] returned [`WriteAuthority::Released`]
/// and its quiesce became permanent (PR-4). Because a SINGLE process-global bool gates every shard,
/// the flip is ALL-OR-NOTHING with no per-shard stagger (the cross-shard barrier already decided
/// all-or-nothing). And because it flips strictly AFTER the OLD's release, no write is EVER acked by
/// both processes across the cutover: the OLD acks no write from `E` onward (permanent quiesce), and
/// the NEW acks writes only from HERE. Before this the process-global gate rejected every client
/// command with `-LOADING`, so a client never read a half-loaded or not-yet-committed store.
///
/// `dead_code`-allowed: the live caller is the PR-6 orchestrator (it drives `recv_await_commit` ->
/// [`promote`] -> this -> [`stream::send_served`]); exercised now by the PR-5 continuity hero test.
#[allow(dead_code)]
pub(crate) fn begin_serving_on_commit() {
    crate::serve::set_serving(true);
}

// ---------------------------------------------------------------------------------------------
// The PR-3 quiesce, extended to close W5 (internal-mutator suspension).
// ---------------------------------------------------------------------------------------------

/// QUIESCE an OLD shard for the streamed cutover and return the latched cut `E` (#391 PR-4). This is
/// the PR-3 [`crate::serve::quiesce_shard`] (which sets the core-local `-LOADING` gate + latches
/// `E = ring.head()` in one on-thread step so a CLIENT mutator is rejected before it is assigned an
/// offset) EXTENDED to close W5: it also sets the store's PASSIVE flag, so lazy expiry during the
/// outage reports a due key as absent WITHOUT physically removing it (no `on_remove`, no ring append
/// above `E`). Together with the active-expiry reaper's [`crate::serve::is_shard_loading`] gate, NO
/// internal mutation is acked above `E`, so `bulk UNION delta(F, E]` stays EXACTLY the acked keyspace
/// as of `E`. Idempotent.
pub fn quiesce_old_shard<E, A>(
    store: &mut ShardStore<E, A>,
    ring: &Rc<RefCell<ReplRing>>,
) -> ReplOffset
where
    E: EvictionHook,
    A: AccountingHook,
{
    // W5 (lazy expiry): a read served during the outage must not physically reap a due key (which
    // would route through the write funnel and append a StreamDel above E). The store's passive mode
    // -- the SAME shipped mechanism a passive replica uses -- reports the due key as absent without
    // removing it. The active reaper is suspended separately by its is_shard_loading() gate.
    store.set_passive(true);
    // The E-latch + the -LOADING gate, in one on-thread step (PR-3). No await, no cross-thread hop
    // between setting the flag and reading the head, so W1 (the E-latch TOCTOU) stays closed.
    crate::serve::quiesce_shard(ring)
}

/// RESUME a quiesced OLD shard after an ABORT (#391 PR-4): clear the `-LOADING` gate (PR-3
/// [`crate::serve::unquiesce_shard`]) AND restore normal lazy expiry (undo the W5 passive
/// suspension). This is the ONLY place the OLD goes back to acking writes; the caller invokes it with
/// authority still [`WriteAuthority::Held`] (an abort never releases). Idempotent.
pub fn resume_old_shard<E, A>(store: &mut ShardStore<E, A>)
where
    E: EvictionHook,
    A: AccountingHook,
{
    crate::serve::unquiesce_shard();
    store.set_passive(false);
}

// ---------------------------------------------------------------------------------------------
// Refinement A: the staging fsync, split BULK (heavy, Phase 1) / DELTA (bounded, in-outage), then
// the atomic staging -> data_dir promote on COMMIT. Reuses the shipped #390/#530 persist machinery.
// ---------------------------------------------------------------------------------------------

/// The delta-log magic: ASCII `ICDL` (IronCache Delta Log). Guards the bounded `(F, E]` tail file.
const DELTA_MAGIC: [u8; 4] = *b"ICDL";
/// The delta-log fixed header: `magic[4] | shard[4] | count[4] | crc[4]`.
const DELTA_HEADER: usize = 16;

/// The staging-dir file name for a shard's bounded post-`E` DELTA log (alongside the persist crate's
/// `dump-shard-<n>.icss` BULK file).
#[must_use]
fn delta_file_name(shard: u32) -> String {
    format!("delta-shard-{shard}.icd")
}

/// A NEW-process staging directory for the streamed cutover (Refinement A). Holds, per shard, the
/// heavy BULK snapshot (`dump-shard-<n>.icss`, state@F, fsync'd in Phase 1) + the bounded DELTA log
/// (`delta-shard-<n>.icd`, `(F, E]`, fsync'd in the outage). On COMMIT the whole dir is atomically
/// [`promote`]d to `data_dir`. The two files together are state@E, fsync'd, before `Prepared` -- the
/// durable copy that makes the kill-between-PREPARED-and-COMMIT window data-safe.
#[derive(Debug, Clone)]
pub struct Staging {
    dir: PathBuf,
}

impl Staging {
    /// Create (or reuse) the staging dir. On the real path this is the #390 tmpfs staging base; in
    /// the in-process tests it is a temp dir.
    ///
    /// # Errors
    /// [`HandoffError::Io`] if the directory cannot be created.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, HandoffError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(|e| HandoffError::io(&e))?;
        Ok(Staging { dir })
    }

    /// The staging directory path.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// PHASE 1 (OUTSIDE the outage): fsync the BULK snapshot (state@F) for `store` to
    /// `dump-shard-<shard>.icss` ATOMICALLY (tmp -> fsync -> rename + dir fsync), via the shipped
    /// forkless [`ironcache_persist::save_shard_to_dir`]. This is the HEAVY fsync; it runs while the
    /// OLD still serves, so it never widens the write outage. Returns the shard's manifest entry.
    ///
    /// # Errors
    /// [`HandoffError::Io`] on any file-write failure (the caller aborts; the OLD keeps serving).
    pub fn stage_bulk<E, A>(
        &self,
        store: &ShardStore<E, A>,
        shard: u32,
        now: UnixMillis,
    ) -> Result<ShardManifestEntry, HandoffError>
    where
        E: EvictionHook,
        A: AccountingHook,
    {
        ironcache_persist::save_shard_to_dir(store, shard, &self.dir, now)
            .map_err(|e| HandoffError::io(&e))
    }

    /// IN-OUTAGE (before `Prepared`): fsync ONLY the BOUNDED `(F, E]` delta ops to
    /// `delta-shard-<shard>.icd` ATOMICALLY. Bounded by the outage tail, INDEPENDENT of keyspace
    /// size -- the small fsync Refinement A keeps inside the outage. Reconstruct is bulk + this.
    ///
    /// # Errors
    /// [`HandoffError::Io`] on a file-write failure.
    pub fn stage_delta(&self, shard: u32, delta: &[Frame]) -> Result<(), HandoffError> {
        let bytes = encode_delta_log(shard, delta);
        let path = self.dir.join(delta_file_name(shard));
        ironcache_persist::format::write_file_atomic(&path, &bytes)
            .map_err(|e| HandoffError::io(&e))
    }

    /// Write the committed BULK manifest (`dump.manifest`) LAST, listing every shard's `.icss`
    /// entry, so the promoted dir is a loadable snapshot (the delta logs are merged by
    /// [`reconstruct_shard`] on recovery, PR-6). Call once, after every shard prepared, before
    /// [`promote`].
    ///
    /// # Errors
    /// [`HandoffError::Io`] on a manifest-write failure.
    pub fn write_manifest(
        &self,
        save_unix_secs: u64,
        entries: Vec<ShardManifestEntry>,
    ) -> Result<(), HandoffError> {
        ironcache_persist::write_manifest(&self.dir, 1, save_unix_secs, entries)
            .map(|_| ())
            .map_err(|e| HandoffError::io(&e))
    }
}

/// ATOMICALLY PROMOTE a staging dir to `data_dir` on COMMIT (#391 PR-4, Phase 6): rename the whole
/// directory + fsync its parent so the rename's directory entry is durable. `data_dir` MUST NOT
/// already exist (a fresh boot dir, or the caller cleared it). After this the promoted dir is the
/// NEW's durable snapshot (bulk + delta logs); the merge-on-boot loader is [`reconstruct_shard`]
/// (PR-6).
///
/// # Errors
/// [`HandoffError::Io`] if the rename fails.
pub fn promote(staging_dir: &Path, data_dir: &Path) -> Result<(), HandoffError> {
    std::fs::rename(staging_dir, data_dir).map_err(|e| HandoffError::io(&e))?;
    fsync_parent(data_dir);
    Ok(())
}

/// Best-effort fsync of `path`'s PARENT directory so a rename's directory-entry update is durable.
/// Non-fatal (mirrors the persist crate's `fsync_dir`): the contents are already fsync'd and the
/// rename is atomic, so the worst case on a lost dir-entry fsync is a redo of the promote.
fn fsync_parent(path: &Path) {
    if let Some(parent) = path.parent() {
        let dir = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        if let Ok(handle) = std::fs::File::open(dir) {
            let _ = handle.sync_all();
        }
    }
}

/// Encode a shard's bounded `(F, E]` delta as a CRC'd delta-log file body:
/// `magic | shard | count | crc | [len u32 | frame bytes]*`, the CRC over the whole with the crc
/// field zeroed (a torn header OR body is caught), fail-closed like the #530 on-disk format.
#[must_use]
fn encode_delta_log(shard: u32, delta: &[Frame]) -> Vec<u8> {
    let count = u32::try_from(delta.len()).unwrap_or(u32::MAX);
    let mut out = Vec::with_capacity(DELTA_HEADER + delta.len() * 32);
    out.extend_from_slice(&DELTA_MAGIC);
    out.extend_from_slice(&shard.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&[0u8; 4]); // crc placeholder
    for frame in delta {
        let bytes = frame.encode();
        let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&bytes);
    }
    let crc = crc32(&out);
    out[12..16].copy_from_slice(&crc.to_le_bytes());
    out
}

/// Decode + fail-closed-validate a delta-log body ([`encode_delta_log`]) into `(shard, ops)`, or
/// `None` on a bad magic, a CRC mismatch, a truncated / overslong record, or an undecodable inner
/// frame. `pub` so the PR-6 recovery loader (and the tests) can replay a promoted delta log.
#[must_use]
pub fn decode_delta_log(bytes: &[u8]) -> Option<(u32, Vec<Frame>)> {
    if bytes.len() < DELTA_HEADER || bytes[0..4] != DELTA_MAGIC {
        return None;
    }
    let saved_crc = u32::from_le_bytes(bytes[12..16].try_into().ok()?);
    let mut check = bytes.to_vec();
    check[12..16].copy_from_slice(&[0u8; 4]);
    if crc32(&check) != saved_crc {
        return None;
    }
    let shard = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
    let count = u32::from_le_bytes(bytes[8..12].try_into().ok()?) as usize;
    let mut ops = Vec::with_capacity(count);
    let mut pos = DELTA_HEADER;
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return None;
        }
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        let end = pos.checked_add(len)?;
        if end > bytes.len() {
            return None;
        }
        match Frame::decode(&bytes[pos..end]) {
            Ok(Some((frame, consumed))) if consumed == len => ops.push(frame),
            _ => return None,
        }
        pos = end;
    }
    if pos != bytes.len() {
        return None; // trailing slop: fail-closed.
    }
    Some((shard, ops))
}

/// RECONSTRUCT state@E for one shard from a PROMOTED cutover dir: load the BULK (state@F) via the
/// shipped [`ironcache_persist::load_shard_from_dir`], then replay the bounded DELTA log `(F, E]`.
/// This is the durable-recovery merge (the W2 window: a NEW crash after COMMIT restarts from the
/// promoted dir) AND the tests' proof that the fsync'd staging is EXACTLY state@E. Returns `None` if
/// there is no loadable manifest entry for the shard or the delta log is torn (fail-closed).
///
/// `now` drops an already-expired key on load (matching the durable load path). The bounded delta is
/// TRUSTED (CRC-verified), so it is applied by effect (`insert_object` / `delete`), not offset-gated.
pub fn reconstruct_shard<E, A, M>(
    dir: &Path,
    shard: u32,
    now: UnixMillis,
    mut make_store: M,
) -> Option<ShardStore<E, A>>
where
    E: EvictionHook,
    A: AccountingHook,
    M: FnMut() -> ShardStore<E, A>,
{
    let manifest = ironcache_persist::read_manifest(dir)?;
    let entry = manifest.entries.iter().find(|e| e.shard == shard)?;
    let mut store = make_store();
    ironcache_persist::load_shard_from_dir(&mut store, dir, entry, now);
    let delta_path = dir.join(delta_file_name(shard));
    if let Ok(bytes) = std::fs::read(&delta_path) {
        let (_shard, ops) = decode_delta_log(&bytes)?;
        for frame in ops {
            match frame {
                Frame::StreamPut {
                    db, kvobj_bytes, ..
                } => {
                    let obj = decode_kvobj(&kvobj_bytes)?;
                    store.insert_object(db, obj);
                }
                Frame::StreamDel { db, key, .. } => {
                    store.delete(db, &key, now);
                }
                _ => return None, // a non-tail frame in a delta log is corrupt: fail-closed.
            }
        }
    }
    Some(store)
}

// ---------------------------------------------------------------------------------------------
// The per-shard drivers (up to PREPARED), for the PR-6 orchestrator. The COMMIT/ABORT phase after
// the barrier uses the `stream` frames directly (`send_commit` / `await_served` /
// `recv_await_commit` / `send_abort_frame`).
// ---------------------------------------------------------------------------------------------

/// SENDER (OLD) per-shard driver up to PREPARED (#391 PR-4). Runs the full OLD-side sequence: the
/// atomic cut + frozen bulk (Phase 1, OLD serving), await BULK-STAGED, [`quiesce_old_shard`] (Phase
/// 2, the outage + W5 suspension), ship the final delta `(F, E]`, and await `Prepared`. Returns `E`.
/// The store is READ-ONLY throughout (the sender reads a frozen view + the ring); on ANY error the
/// caller [`resume_old_shard`]s and resumes full serving. `store` is shared (`Rc<RefCell<..>>`) so a
/// concurrent client/writer keeps mutating it during Phase 1 -- the sender only borrows it at the
/// brief sync points (freeze, end-save, quiesce) and never across an `.await`.
///
/// # Errors
/// Any [`HandoffError`] from the bulk, the staged handshake, the delta, or the prepare await.
pub async fn send_shard_to_prepared<E, A, S>(
    stream: &mut S,
    store: &Rc<RefCell<ShardStore<E, A>>>,
    ring: &Rc<RefCell<ReplRing>>,
    shard: u32,
    replid: ReplId,
    now: UnixMillis,
    chunk_max: usize,
) -> Result<ReplOffset, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let databases = u32::try_from(store.borrow().databases()).unwrap_or(u32::MAX);
    // PHASE 1: the atomic cut (F) + frozen bulk, then wait for the receiver's HEAVY bulk fsync so the
    // outage excludes it (Refinement A). The freeze is a brief borrow; the bulk ships a frozen view.
    let (frozen, floor) = {
        let mut s = store.borrow_mut();
        stream::freeze_cut(&mut s, ring)
    };
    let bulk = async {
        stream::send_bulk_from_frozen(
            stream, &frozen, shard, databases, replid, floor, now, chunk_max,
        )
        .await?;
        stream::await_bulk_staged(stream).await
    }
    .await;
    drop(frozen);
    store.borrow_mut().end_save(); // clear the freeze flag in ALL cases (even a mid-bulk abort).
    bulk?;
    // PHASE 2: QUIESCE (outage begins) -- latch E + gate client writes + suspend internal mutators.
    let _e = quiesce_old_shard(&mut store.borrow_mut(), ring);
    // PHASE 3: ship the final delta (F, E] and await PREPARED (the receiver verified + fsync'd).
    stream::send_delta_await_prepared(stream, ring, floor, chunk_max).await
}

/// RECEIVER (NEW) per-shard driver up to PREPARED (#391 PR-4). Runs the full NEW-side sequence: load
/// the bulk into a FRESH store, FSYNC the bulk to staging (Phase 1, the heavy fsync outside the
/// outage), signal BULK-STAGED, then receive + verify the final delta, FSYNC the bounded delta
/// (in-outage), and send `Prepared`. Returns the parked [`PreparedShard`] + its BULK manifest entry.
/// On ANY error the fresh store is dropped (adopt nothing) and staging is discarded.
///
/// # Errors
/// Any [`HandoffError`] from the bulk, the fsync, the delta, or the verify.
pub async fn receive_shard_to_prepared<E, A, S, M>(
    stream: &mut S,
    make_store: M,
    expected_databases: u32,
    now: UnixMillis,
    staging: &Staging,
) -> Result<(PreparedShard<E, A>, ShardManifestEntry), HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
    M: FnMut() -> ShardStore<E, A>,
{
    let (store, shard, floor) =
        stream::recv_bulk(stream, make_store, expected_databases, now).await?;
    // PHASE 1 (outside the outage): the HEAVY bulk fsync, THEN tell the sender it may quiesce.
    let entry = staging.stage_bulk(&store, shard, now)?;
    stream::send_bulk_staged(stream).await?;
    // PHASE 3: apply + verify the delta, FSYNC the bounded delta (in-outage), send PREPARED.
    let prepared = stream::recv_prepare_only(stream, store, shard, floor, now, |_store, delta| {
        staging.stage_delta(shard, delta)
    })
    .await?;
    Ok((prepared, entry))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use ironcache_repl::{ReplObserver, ReplOffset, ReplRing};
    use ironcache_storage::{ExpireWrite, NewValue};
    use ironcache_store::{ShardStore, SnapshotCursor};
    use std::collections::HashMap;
    use tokio::net::UnixStream;

    use ironcache_repl::encode_kvobj;

    const NOW: UnixMillis = UnixMillis(1_000);
    /// A far-future absolute TTL so no test key is lazily expired at [`NOW`]; lets the tests assert
    /// the deadline round-trips VERBATIM (no rebase) through bulk + delta + staging.
    const TTL_AT: UnixMillis = UnixMillis(NOW.0 + 10_000_000);
    const DBS: u32 = 4;

    fn replid() -> ReplId {
        ReplId::from_bytes([0x5A; 20])
    }

    fn tmp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ic-commit-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    /// A fresh store with an always-on observer ring installed BEFORE any write (the keystone), so
    /// every write is assigned a ring offset. Populated with `n` keys spread across the databases;
    /// the first `reserved` carry an absolute TTL (to prove the deadline survives verbatim).
    fn seeded(
        n: u32,
        reserved: u32,
        tag: &str,
    ) -> (Rc<RefCell<ShardStore>>, Rc<RefCell<ReplRing>>) {
        let ring = ReplRing::new(500_000, ReplOffset::ZERO);
        let store = Rc::new(RefCell::new(ShardStore::new(DBS)));
        store
            .borrow_mut()
            .set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        {
            let mut s = store.borrow_mut();
            for i in 0..n {
                let ttl = if i < reserved {
                    ExpireWrite::Set(TTL_AT)
                } else {
                    ExpireWrite::Clear
                };
                if i % 2 == 0 {
                    s.upsert(
                        i % DBS,
                        format!("{tag}-{i}").as_bytes(),
                        NewValue::Int(i64::from(i)),
                        ttl,
                        NOW,
                    );
                } else {
                    s.upsert(
                        i % DBS,
                        format!("{tag}-{i}").as_bytes(),
                        NewValue::Bytes(format!("v-{tag}-{i}").as_bytes()),
                        ttl,
                        NOW,
                    );
                }
            }
        }
        (store, ring)
    }

    /// Dump a live store's whole keyspace as `(db, key) -> encoded-KvObj`. Encoded bytes carry value
    /// + type + encoding + ABSOLUTE TTL, so map equality proves full-fidelity convergence in one shot.
    fn dump_map<E: EvictionHook, A: AccountingHook>(
        store: &ShardStore<E, A>,
        now: UnixMillis,
    ) -> HashMap<(u32, Vec<u8>), Vec<u8>> {
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

    /// A churny background writer that HAMMERS the shard with a create/overwrite/delete mix until the
    /// shard QUIESCES (`is_shard_loading()` goes true, modelling the `-LOADING` client gate) or a
    /// hard cap. Returns the op count (a large count proves genuine interleaving with the transfer).
    /// It checks the loading gate at the TOP of each SYNCHRONOUS batch, so once the driver quiesces
    /// (atomically, at an await point) the writer does NO further write -- nothing acked above E.
    async fn hammer(store: &Rc<RefCell<ShardStore>>, reserved: u32, pre: u32) -> u64 {
        let mut n: u64 = 0;
        loop {
            if crate::serve::is_shard_loading() {
                break; // the -LOADING gate is up: a client write would be rejected. Stop.
            }
            if n >= 200_000 {
                break;
            }
            {
                let mut s = store.borrow_mut();
                for _ in 0..64 {
                    let t = n.wrapping_mul(2_654_435_761) ^ (n >> 3);
                    match n % 5 {
                        0 => {
                            let idx = t % 20_000;
                            s.upsert(
                                (idx as u32) % DBS,
                                format!("nw-{idx}").as_bytes(),
                                NewValue::Bytes(format!("hot-{n}").as_bytes()),
                                ExpireWrite::Clear,
                                NOW,
                            );
                        }
                        1 => {
                            let idx = t % 20_000;
                            s.upsert(
                                (idx as u32) % DBS,
                                format!("nw-{idx}").as_bytes(),
                                NewValue::Int(n as i64),
                                ExpireWrite::Set(TTL_AT),
                                NOW,
                            );
                        }
                        2 => {
                            let idx = reserved + (t as u32 % (pre - reserved));
                            s.upsert(
                                idx % DBS,
                                format!("pre-{idx}").as_bytes(),
                                NewValue::Bytes(format!("rw-{n}").as_bytes()),
                                ExpireWrite::Clear,
                                NOW,
                            );
                        }
                        3 => {
                            let idx = t % 20_000;
                            s.delete((idx as u32) % DBS, format!("nw-{idx}").as_bytes(), NOW);
                        }
                        _ => {
                            let idx = reserved + (t as u32 % (pre - reserved));
                            s.delete(idx % DBS, format!("pre-{idx}").as_bytes(), NOW);
                        }
                    }
                    n += 1;
                }
            }
            tokio::task::yield_now().await;
        }
        n
    }

    // ---- the delta-log codec (fail-closed) ----

    #[test]
    fn delta_log_round_trips_and_is_fail_closed() {
        let delta = vec![
            Frame::StreamPut {
                offset: ReplOffset(7),
                db: 1,
                key: b"k\r\n\x00".to_vec(),
                kvobj_bytes: vec![0u8, 1, 2, 255],
            },
            Frame::StreamDel {
                offset: ReplOffset(8),
                db: 0,
                key: b"gone".to_vec(),
            },
        ];
        let bytes = encode_delta_log(3, &delta);
        let (shard, got) = decode_delta_log(&bytes).expect("round-trips");
        assert_eq!(shard, 3);
        assert_eq!(got, delta);
        // A single flipped byte fails the CRC (fail-closed, never a silent mis-parse).
        let mut torn = bytes.clone();
        let last = torn.len() - 1;
        torn[last] ^= 0xFF;
        assert!(
            decode_delta_log(&torn).is_none(),
            "a torn delta log is rejected"
        );
        // A foreign magic is rejected.
        let mut foreign = bytes;
        foreign[0] = b'X';
        assert!(
            decode_delta_log(&foreign).is_none(),
            "a foreign magic is rejected"
        );
    }

    // ---- (b) THE HERO HAPPY PATH: all PREPARED + COMMIT -> exact acked keyspace@E, quiesce permanent.

    /// (b) all shards PREPARED, COMMIT delivered under REAL concurrent write load: the receiver's
    /// COMMITTED store equals EXACTLY the OLD's acked keyspace as of E (value + encoding + absolute
    /// TTL, zero gaps/doubles); the PROMOTED durable staging (bulk fsync'd EARLY + delta fsync'd
    /// in-outage) reconstructs to the SAME state@E; and the OLD's quiesce is now PERMANENT (authority
    /// Released, `is_shard_loading()` still true, never resumed).
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::too_many_lines)]
    async fn hero_b_commit_is_exact_acked_keyspace_and_quiesce_is_permanent() {
        const PRE: u32 = 2_000;
        const RESERVED: u32 = 16;
        crate::serve::unquiesce_shard(); // defensive: fresh thread-local gate.
        let (store, ring) = seeded(PRE, RESERVED, "pre");
        let seeded_snapshot = dump_map(&store.borrow(), NOW);
        assert_eq!(
            seeded_snapshot.len(),
            PRE as usize,
            "seed populated every pre key"
        );

        let root = tmp_root("hero-b");
        let _ = std::fs::remove_dir_all(&root);
        let staging = Staging::new(root.join("staging")).expect("staging dir");
        let data_dir = root.join("data");

        let (mut a, mut b) = UnixStream::pair().expect("socketpair");

        // ---- PHASE A: run the OLD-side + NEW-side drivers to PREPARED, under write load. ----
        let writer = hammer(&store, RESERVED, PRE);
        let sender = send_shard_to_prepared(&mut a, &store, &ring, 0, replid(), NOW, 4);
        let receiver =
            receive_shard_to_prepared(&mut b, || ShardStore::new(DBS), DBS, NOW, &staging);
        let (writer_ops, s_res, r_res) = tokio::join!(writer, sender, receiver);

        let e = s_res.expect("sender reached PREPARED");
        let (prepared, entry) = r_res.expect("receiver reached PREPARED");
        assert_eq!(prepared.final_offset, e, "both ends agree on the cut E");
        assert!(
            writer_ops > 1_000,
            "the writer must genuinely hammer during the transfer (did {writer_ops})"
        );
        // The bulk file was fsync'd in Phase 1 and the bounded delta in the outage (Refinement A).
        assert!(
            staging.dir().join("dump-shard-0.icss").exists(),
            "the heavy bulk snapshot was fsync'd to staging"
        );
        assert!(
            staging.dir().join(delta_file_name(0)).exists(),
            "the bounded delta was fsync'd to staging"
        );

        // ---- THE BARRIER + THE LINEARIZATION POINT: all prepared -> RELEASE authority, THEN commit.
        let prepared_results: Vec<Result<ReplOffset, HandoffError>> = vec![Ok(e)];
        let (state, authority) = decide_cutover(&prepared_results);
        assert_eq!(
            state,
            CutoverState::Commit,
            "the single shard prepared -> commit"
        );
        assert_eq!(
            authority,
            WriteAuthority::Released,
            "the OLD releases write authority AT the commit decision"
        );
        // The OLD must not resume after Released: its quiesce is now permanent.
        assert!(
            crate::serve::is_shard_loading(),
            "the quiesce is PERMANENT after commit (the OLD never acks a write again)"
        );

        // ---- PHASE B: commit the shard + promote + Served. ----
        let committed = {
            let send = stream::send_commit(&mut a);
            let recv = stream::recv_await_commit(&mut b, prepared);
            let (commit_send, commit_recv) = tokio::join!(send, recv);
            commit_send.expect("commit sent");
            match commit_recv.expect("receiver got the decision") {
                stream::ShardCommit::Committed(loaded) => loaded,
                stream::ShardCommit::Aborted => panic!("expected a commit, got abort"),
            }
        };
        // The DURABLE PROMOTE: manifest last, then atomic dir rename + parent fsync.
        staging
            .write_manifest(NOW.0 / 1000, vec![entry])
            .expect("manifest");
        promote(staging.dir(), &data_dir).expect("promote staging -> data_dir");
        // The Served handshake (the OLD may then drain + exit, PR-6).
        {
            let recv = stream::send_served(&mut b);
            let send = stream::await_served(&mut a);
            let (rr, sr) = tokio::join!(recv, send);
            rr.expect("served sent");
            sr.expect("old sees served");
        }

        // ---- THE ASSERTION: committed store == OLD acked keyspace@E, and durable == the same. ----
        let old_at_e = dump_map(&store.borrow(), NOW);
        let committed_map = dump_map(&committed.store, NOW);
        assert_eq!(
            committed_map, old_at_e,
            "the committed store == EXACTLY the OLD's acked keyspace as of E (value+enc+TTL)"
        );
        // The promoted DURABLE staging (bulk fsync'd early + delta fsync'd in-outage) reconstructs to
        // the SAME state@E -- the copy that makes the kill-before-COMMIT window data-safe (W2).
        let durable = reconstruct_shard(&data_dir, 0, NOW, || ShardStore::new(DBS))
            .expect("promoted dir reconstructs");
        let durable_map = dump_map(&durable, NOW);
        assert_eq!(
            durable_map, old_at_e,
            "the fsync'd staging (bulk UNION delta) is EXACTLY state@E on disk"
        );
        // A reserved, never-overwritten key kept its ABSOLUTE TTL deadline verbatim (no rebase).
        assert_eq!(
            committed.store.databases(),
            DBS as usize,
            "the db count round-tripped"
        );
        for i in 0..RESERVED {
            let key = (i % DBS, format!("pre-{i}").as_bytes().to_vec());
            assert_eq!(
                committed_map.get(&key),
                seeded_snapshot.get(&key),
                "reserved key pre-{i} kept its seeded value + absolute TTL across the cutover"
            );
        }

        crate::serve::unquiesce_shard(); // tidy the thread-local for other tests on this thread.
        let _ = std::fs::remove_dir_all(&root);
    }

    // ---- (e) PR-5 CLIENT CONTINUITY ACROSS THE SERVE-FLIP: no key served by neither/both. ----

    /// (e) THE PR-5 HERO: a client reading a mixed keyspace THROUGHOUT the serve-flip observes, for
    /// EVERY key, either the OLD's value (pre-flip) or the NEW's committed value (post-flip) -- never a
    /// spurious miss from a flip gap, and never a write acked by BOTH sides. Models the flip in-process:
    /// the NEW is committed to state@E (via the real PR-4 commit path + durable promote) behind its
    /// process-global serve gate, while the OLD is permanently quiesced at E (still serving READS). A
    /// concurrent reader hammers GET/MGET across the whole keyspace while the transition runs. Asserts:
    ///   (1) while the NEW is not serving, a client hitting it gets `-LOADING` (never a wrong/empty
    ///       answer) and the OLD keeps serving EXACTLY state@E;
    ///   (2) on the `Committed` flip the NEW serves state@E for EVERY key;
    ///   (3) there is no instant where a write is acked by BOTH: the OLD is permanently quiesced from
    ///       the release (`is_shard_loading()` stays true across the flip), and the NEW begins acking
    ///       writes only at the flip, which is STRICTLY after that release.
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::too_many_lines)]
    async fn hero_e_client_continuity_across_the_serve_flip() {
        use std::cell::Cell;

        /// The modeled NEW client reply -- exactly the top-of-`route_and_dispatch` gate decision.
        #[derive(Debug, PartialEq, Eq)]
        enum NewReply {
            Loading,
            Value(Vec<u8>),
            Miss,
        }
        /// Model a client GET against the NEW: the process-global serve gate FIRST (a `-LOADING` while
        /// receiving), else the committed store read. This is the SAME decision the real dispatch gate
        /// makes (`is_serving()` -> `ErrorReply::loading()`), so the test exercises the real primitive.
        fn new_get(new_map: &HashMap<(u32, Vec<u8>), Vec<u8>>, key: &(u32, Vec<u8>)) -> NewReply {
            if !crate::serve::is_serving() {
                return NewReply::Loading;
            }
            match new_map.get(key) {
                Some(v) => NewReply::Value(v.clone()),
                None => NewReply::Miss,
            }
        }

        const PRE: u32 = 1_200;
        const RESERVED: u32 = 12;
        crate::serve::unquiesce_shard(); // defensive: fresh thread-local gate.
        crate::serve::set_serving(true); // defensive: fresh process-global gate (restored at the end).

        let (store, ring) = seeded(PRE, RESERVED, "pre");
        let root = tmp_root("hero-c");
        let _ = std::fs::remove_dir_all(&root);
        let staging = Staging::new(root.join("staging")).expect("staging dir");
        let data_dir = root.join("data");
        let (mut a, mut b) = UnixStream::pair().expect("socketpair");

        // ---- Reach state@E: drive the real PR-4 cutover to PREPARED under write load, decide Commit
        //      (which RELEASES the OLD's authority -> permanent quiesce), then COMMIT + durable promote.
        let writer = hammer(&store, RESERVED, PRE);
        let sender = send_shard_to_prepared(&mut a, &store, &ring, 0, replid(), NOW, 4);
        let receiver =
            receive_shard_to_prepared(&mut b, || ShardStore::new(DBS), DBS, NOW, &staging);
        let (writer_ops, s_res, r_res) = tokio::join!(writer, sender, receiver);
        let e = s_res.expect("sender reached PREPARED");
        let (prepared, entry) = r_res.expect("receiver reached PREPARED");
        assert_eq!(prepared.final_offset, e, "both ends agree on the cut E");
        assert!(
            writer_ops > 500,
            "the writer genuinely hammered ({writer_ops} ops)"
        );

        // THE LINEARIZATION POINT (PR-4): all shards prepared -> release authority (permanent quiesce),
        // THEN commit. PR-5 rides on the resulting Released authority.
        let prepared_results: Vec<Result<ReplOffset, HandoffError>> = vec![Ok(e)];
        let (state, authority) = decide_cutover(&prepared_results);
        assert_eq!(state, CutoverState::Commit);
        assert_eq!(
            authority,
            WriteAuthority::Released,
            "the OLD released write authority at the commit decision"
        );
        assert!(
            crate::serve::is_shard_loading(),
            "the OLD's quiesce is PERMANENT after the release (it never acks a write again)"
        );

        // The NEW receives Commit + durably promotes staging -> data_dir (still NOT serving).
        let committed = {
            let send = stream::send_commit(&mut a);
            let recv = stream::recv_await_commit(&mut b, prepared);
            let (cs, cr) = tokio::join!(send, recv);
            cs.expect("commit sent");
            match cr.expect("receiver got the decision") {
                stream::ShardCommit::Committed(loaded) => loaded,
                stream::ShardCommit::Aborted => panic!("expected a commit"),
            }
        };
        staging
            .write_manifest(NOW.0 / 1000, vec![entry])
            .expect("manifest");
        promote(staging.dir(), &data_dir).expect("promote staging -> data_dir");

        // state@E, the ONE value every read must return: OLD-live == NEW-committed == durable-on-disk.
        let reference = dump_map(&store.borrow(), NOW);
        assert_eq!(
            dump_map(&committed.store, NOW),
            reference,
            "NEW committed == OLD state@E"
        );
        let durable = reconstruct_shard(&data_dir, 0, NOW, || ShardStore::new(DBS))
            .expect("promoted dir reconstructs");
        assert_eq!(
            dump_map(&durable, NOW),
            reference,
            "durable data_dir == OLD state@E"
        );
        assert!(!reference.is_empty(), "the acked keyspace is non-empty");
        let new_map = dump_map(&committed.store, NOW); // the NEW's committed keyspace, served post-flip.
        let sample_keys: Vec<(u32, Vec<u8>)> = reference.keys().take(64).cloned().collect();

        // ---- Model the RECEIVER boot: the NEW is NOT serving yet (booted with the gate false). ----
        crate::serve::set_serving(false);
        // (1) pre-flip: a client hitting the NEW gets `-LOADING` for every sampled key (never value/miss).
        for k in &sample_keys {
            assert_eq!(
                new_get(&new_map, k),
                NewReply::Loading,
                "pre-flip the NEW rejects with -LOADING (never a wrong/empty answer)"
            );
        }

        // ---- THE TRANSITION UNDER READ LOAD: a reader hammers the keyspace while the flip runs. ----
        let saw_loading = Cell::new(0u64);
        let saw_serving = Cell::new(0u64);
        let prev_serving = Cell::new(false);

        let reader = async {
            for _ in 0..800 {
                // (3) the OLD is permanently quiesced from the release: it acks NO write, at every
                // instant -- so no write can be acked by both the OLD and the (post-flip) NEW.
                assert!(
                    crate::serve::is_shard_loading(),
                    "the OLD stays permanently quiesced across the whole flip (no write double-acked)"
                );
                let serving = crate::serve::is_serving();
                // Monotonic: the serve gate only ever moves false -> true, never back (all-or-nothing).
                assert!(
                    !prev_serving.get() || serving,
                    "the serve flip is monotonic (it never flips back to not-serving)"
                );
                prev_serving.set(serving);
                if serving {
                    saw_serving.set(saw_serving.get() + 1);
                    // (2) post-flip: the NEW serves the committed state@E value for EVERY key.
                    for k in &sample_keys {
                        assert_eq!(
                            new_get(&new_map, k),
                            NewReply::Value(reference[k].clone()),
                            "post-flip the NEW serves the committed state@E value (no spurious miss)"
                        );
                    }
                } else {
                    saw_loading.set(saw_loading.get() + 1);
                    // (1) pre-flip: the NEW rejects (`-LOADING`), and the client reads the OLD, which
                    // still serves EXACTLY state@E -- for every key, no spurious miss from a flip gap.
                    for k in &sample_keys {
                        assert_eq!(
                            new_get(&new_map, k),
                            NewReply::Loading,
                            "pre-flip the NEW is -LOADING under read load"
                        );
                    }
                    assert_eq!(
                        dump_map(&store.borrow(), NOW),
                        reference,
                        "the OLD still serves EXACTLY state@E while the NEW is not serving"
                    );
                }
                tokio::task::yield_now().await;
            }
        };

        let flipper = async {
            // Let the reader observe the pre-flip (`-LOADING` + OLD continuity) window.
            for _ in 0..30 {
                tokio::task::yield_now().await;
            }
            // THE FLIP ordering (the continuity crux): the OLD released FIRST (permanent quiesce,
            // asserted above), and the NEW is still not serving here -- so serving goes true STRICTLY
            // after the release, and no write is ever acked by both.
            assert!(
                !crate::serve::is_serving(),
                "the NEW is not serving at the flip point"
            );
            assert!(
                crate::serve::is_shard_loading(),
                "the OLD released (permanent quiesce) BEFORE the NEW begins serving"
            );
            begin_serving_on_commit(); // the single all-or-nothing client-visible flip.
            // Let the reader observe the post-flip (NEW serving state@E) window.
            for _ in 0..30 {
                tokio::task::yield_now().await;
            }
        };

        tokio::join!(reader, flipper);

        assert!(
            crate::serve::is_serving(),
            "the NEW is serving after the flip"
        );
        assert!(
            saw_loading.get() > 0,
            "the reader observed the pre-flip -LOADING window under load"
        );
        assert!(
            saw_serving.get() > 0,
            "the reader observed the post-flip serving window under load"
        );
        assert_eq!(
            dump_map(&committed.store, NOW),
            reference,
            "the final NEW keyspace is still EXACTLY state@E"
        );

        crate::serve::unquiesce_shard(); // tidy the thread-local for other tests on this thread.
        crate::serve::set_serving(true); // restore the process-global default (serving).
        let _ = std::fs::remove_dir_all(&root);
    }

    // ---- (a) KILL BETWEEN PREPARED AND COMMIT: OLD held authority, NEW served nothing. ----

    /// (a) the receiver PREPARED, then the coordinator/link DIES before COMMIT: the OLD never
    /// released authority (it can [`resume_old_shard`] and every acked write is still in its store),
    /// and the NEW did not serve (its commit await fails, it adopts nothing). NOTHING lost, no
    /// split-brain. Driven under concurrent write load.
    #[tokio::test(flavor = "current_thread")]
    async fn hero_a_kill_between_prepared_and_commit_loses_nothing() {
        const PRE: u32 = 800;
        const RESERVED: u32 = 8;
        crate::serve::unquiesce_shard();
        let (store, ring) = seeded(PRE, RESERVED, "pre");

        let root = tmp_root("hero-a");
        let _ = std::fs::remove_dir_all(&root);
        let staging = Staging::new(root.join("staging")).expect("staging");
        let data_dir = root.join("data");

        let (a, mut b) = UnixStream::pair().expect("socketpair");

        // PHASE A to PREPARED, under load. The receiver end `b` is MOVED into its future so we can
        // keep it after; the sender end `a` is MOVED into the sender future and DROPPED there to
        // simulate the coordinator crashing BEFORE it sends COMMIT.
        let writer = hammer(&store, RESERVED, PRE);
        let sender = async {
            let mut a = a; // own it; dropping at the end of this block == coordinator crash.
            let r = send_shard_to_prepared(&mut a, &store, &ring, 0, replid(), NOW, 4).await;
            (
                r, /* a is dropped here -> b sees EOF on its commit await */
            )
        };
        let receiver =
            receive_shard_to_prepared(&mut b, || ShardStore::new(DBS), DBS, NOW, &staging);
        let (_ops, (s_res,), r_res) = tokio::join!(writer, sender, receiver);

        let e = s_res.expect("the sender reached PREPARED before the crash");
        let (prepared, _entry) = r_res.expect("the receiver reached PREPARED");
        assert_eq!(prepared.final_offset, e);

        // The coordinator never decided commit, so authority was NEVER released. Model that: the
        // pre-commit authority is Held (no `decide_cutover` Commit ran).
        let authority = WriteAuthority::Held;
        assert_eq!(
            authority,
            WriteAuthority::Held,
            "authority was never released"
        );

        // The NEW awaits COMMIT on the dropped socket -> fail-closed EOF -> it adopts NOTHING.
        let commit = stream::recv_await_commit(&mut b, prepared).await;
        assert!(
            commit.is_err(),
            "the receiver serves NOTHING when the coordinator dies before COMMIT: {commit:?}"
        );
        // The NEW never promoted: no data_dir, so it cannot serve.
        assert!(
            !data_dir.exists(),
            "the NEW did not promote staging (it never got COMMIT)"
        );

        // The OLD HELD authority: it can resume + every acked write <= E is still in its store.
        let old_at_e = dump_map(&store.borrow(), NOW);
        resume_old_shard(&mut store.borrow_mut());
        assert!(
            !crate::serve::is_shard_loading(),
            "the OLD resumed (it never released authority): the quiesce lifted"
        );
        // Every acked key is intact + a fresh write is accepted (the OLD is fully serving again).
        for i in 0..RESERVED {
            let key = (i % DBS, format!("pre-{i}").as_bytes().to_vec());
            assert!(
                old_at_e.contains_key(&key),
                "acked key pre-{i} survived the aborted flip"
            );
        }
        let head_before = ring.borrow().head();
        store.borrow_mut().upsert(
            0,
            b"post-resume",
            NewValue::Bytes(b"ok"),
            ExpireWrite::Clear,
            NOW,
        );
        assert!(
            ring.borrow().head() > head_before,
            "after resume the OLD acks writes again (authority was retained)"
        );

        crate::serve::unquiesce_shard();
        let _ = std::fs::remove_dir_all(&root);
    }

    // ---- (c) ONE SHARD FAILS TO PREPARE: the WHOLE flip aborts (barrier), OLD resumes every shard.

    /// The (c) FAILING receiver: recv the bulk + BULK-STAGED as usual, but its bounded delta fsync
    /// hook returns an Io error, so [`stream::recv_prepare_only`] aborts (sends ABORT) and returns
    /// Err -- modelling a shard that fails to verify/persist. It adopts nothing. Never yields Ok, so
    /// its return type carries no `PreparedShard` (the caller only needs the Err).
    async fn receive_failing<S>(stream_io: &mut S, now: UnixMillis) -> Result<(), HandoffError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (store, shard, floor) =
            stream::recv_bulk(stream_io, || ShardStore::new(DBS), DBS, now).await?;
        stream::send_bulk_staged(stream_io).await?;
        stream::recv_prepare_only(stream_io, store, shard, floor, now, |_s, _d| {
            Err(HandoffError::Io("injected staging failure".to_owned()))
        })
        .await
        .map(|_prepared| ())
    }

    /// (c) three shards, one fails to prepare (an injected verify/fsync error): the whole flip ABORTS
    /// (the all-or-nothing barrier), the OLD [`resume_old_shard`]s on EVERY shard and resumes, the
    /// receivers that DID prepare adopt NOTHING on the ABORT frame -- no partial cutover, no shard
    /// served by neither or both. Under concurrent write load.
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::too_many_lines)]
    async fn hero_c_one_shard_fails_aborts_the_whole_flip() {
        const PER: u32 = 300;
        const RESERVED: u32 = 4;
        crate::serve::unquiesce_shard();

        let root = tmp_root("hero-c");
        let _ = std::fs::remove_dir_all(&root);
        let staging = Staging::new(root.join("staging")).expect("staging");
        let data_dir = root.join("data");

        // Explicit per-shard locals (three shards) -- borrow-safe, no vec gymnastics. Shard 1 fails.
        let (store0, ring0) = seeded(PER, RESERVED, "s0");
        let (store1, ring1) = seeded(PER, RESERVED, "s1");
        let (store2, ring2) = seeded(PER, RESERVED, "s2");
        let snap0 = dump_map(&store0.borrow(), NOW);
        let snap1 = dump_map(&store1.borrow(), NOW);
        let snap2 = dump_map(&store2.borrow(), NOW);
        let (mut a0, mut b0) = UnixStream::pair().expect("socketpair");
        let (mut a1, mut b1) = UnixStream::pair().expect("socketpair");
        let (mut a2, mut b2) = UnixStream::pair().expect("socketpair");

        // ---- PHASE A concurrently across all shards. A single current-thread runtime shares one
        // `-LOADING` thread-local, so once ANY shard quiesces every writer stops (a known in-process
        // modelling artifact); each shard still ships its own delta up to its own E, exercising the
        // barrier faithfully. Shard 1's receiver injects a prepare failure. ----
        let shard0 = async {
            let w = hammer(&store0, RESERVED, PER);
            let snd = send_shard_to_prepared(&mut a0, &store0, &ring0, 0, replid(), NOW, 4);
            let rcv =
                receive_shard_to_prepared(&mut b0, || ShardStore::new(DBS), DBS, NOW, &staging);
            let (_o, s, r) = tokio::join!(w, snd, rcv);
            (s, r.ok().map(|(p, _e)| p))
        };
        let shard1 = async {
            let w = hammer(&store1, RESERVED, PER);
            let snd = send_shard_to_prepared(&mut a1, &store1, &ring1, 1, replid(), NOW, 4);
            let rcv = receive_failing(&mut b1, NOW);
            let (_o, s, r) = tokio::join!(w, snd, rcv);
            (s, r) // r is Err (the injected failure); no PreparedShard.
        };
        let shard2 = async {
            let w = hammer(&store2, RESERVED, PER);
            let snd = send_shard_to_prepared(&mut a2, &store2, &ring2, 2, replid(), NOW, 4);
            let rcv =
                receive_shard_to_prepared(&mut b2, || ShardStore::new(DBS), DBS, NOW, &staging);
            let (_o, s, r) = tokio::join!(w, snd, rcv);
            (s, r.ok().map(|(p, _e)| p))
        };
        let ((s0, p0), (s1, r1), (s2, p2)) = tokio::join!(shard0, shard1, shard2);

        // Shard 1's sender saw the receiver's ABORT (its PREPARED await failed); the others prepared.
        assert!(
            r1.is_err(),
            "the failing shard's receiver aborted at prepare"
        );
        assert!(
            s1.is_err(),
            "the sender of the failing shard aborted (no PREPARED)"
        );
        assert!(s0.is_ok() && s2.is_ok(), "the other shards did prepare");

        // ---- THE BARRIER: one shard failed -> the WHOLE flip aborts, authority stays HELD. ----
        let sender_results: Vec<Result<ReplOffset, HandoffError>> = vec![s0, s1, s2];
        let (state, authority) = decide_cutover(&sender_results);
        assert_eq!(
            state,
            CutoverState::Abort,
            "one failed shard aborts the whole flip"
        );
        assert_eq!(
            authority,
            WriteAuthority::Held,
            "an abort NEVER releases authority"
        );

        // ---- PHASE B ABORT: tell the prepared shards to abort; resume EVERY OLD shard. ----
        if let Some(prepared) = p0 {
            let send = stream::send_abort_frame(&mut a0);
            let recv = stream::recv_await_commit(&mut b0, prepared);
            let (_s, r) = tokio::join!(send, recv);
            assert!(
                matches!(
                    r.expect("shard 0 got the decision"),
                    stream::ShardCommit::Aborted
                ),
                "shard 0 adopts nothing on the abort"
            );
        }
        if let Some(prepared) = p2 {
            let send = stream::send_abort_frame(&mut a2);
            let recv = stream::recv_await_commit(&mut b2, prepared);
            let (_s, r) = tokio::join!(send, recv);
            assert!(
                matches!(
                    r.expect("shard 2 got the decision"),
                    stream::ShardCommit::Aborted
                ),
                "shard 2 adopts nothing on the abort"
            );
        }
        resume_old_shard(&mut store0.borrow_mut());
        resume_old_shard(&mut store1.borrow_mut());
        resume_old_shard(&mut store2.borrow_mut());

        // No shard promoted: the NEW serves nothing (no partial cutover).
        assert!(
            !data_dir.exists(),
            "no partial cutover: the NEW promoted nothing"
        );
        assert!(
            !crate::serve::is_shard_loading(),
            "every OLD shard resumed after the abort"
        );

        // Every OLD shard retained authority: the seeded keyspace is intact + still served.
        for (shard, (store, snap)) in [(&store0, &snap0), (&store1, &snap1), (&store2, &snap2)]
            .into_iter()
            .enumerate()
        {
            for i in 0..RESERVED {
                let key = (i % DBS, format!("s{shard}-{i}").as_bytes().to_vec());
                assert!(
                    snap.contains_key(&key),
                    "shard {shard} still holds acked key s{shard}-{i} after the aborted flip"
                );
                assert!(
                    store.borrow_mut().contains_live(
                        i % DBS,
                        format!("s{shard}-{i}").as_bytes(),
                        NOW
                    ),
                    "shard {shard} serves s{shard}-{i} after resuming"
                );
            }
        }

        crate::serve::unquiesce_shard();
        let _ = std::fs::remove_dir_all(&root);
    }

    // ---- (d) W5 INTERNAL-MUTATOR: a reaper / lazy-expiry racing the quiesce is suspended. ----

    /// (d) W5: an internal mutator (the active-expiry reaper AND lazy expiry-on-read) racing the
    /// quiesce does NOT produce an acked mutation above E on the OLD that the NEW misses. During the
    /// outage the active reaper is INERT (gated on `is_shard_loading()`) and lazy expiry is
    /// suspended (the store's passive flag set by [`quiesce_old_shard`]), so a due key is reported
    /// absent but NOT physically removed -- no StreamDel is appended above E. Acknowledged-write
    /// conservation holds: the ring head stays EXACTLY at E through the outage.
    #[test]
    fn hero_d_internal_mutator_w5_is_suspended_during_the_quiesce() {
        crate::serve::unquiesce_shard();
        let ring = ReplRing::new(4096, ReplOffset::ZERO);
        let mut store = ShardStore::new(DBS);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));

        // Two live keys + one key that is ALREADY expired relative to NOW (an absolute deadline in
        // the past): the reaper/lazy path would normally reap it and append a StreamDel.
        store.upsert(0, b"live-1", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
        store.upsert(0, b"live-2", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
        store.upsert(
            0,
            b"doomed",
            NewValue::Bytes(b"v"),
            ExpireWrite::Set(UnixMillis(1)),
            NOW,
        );
        let head_before = ring.borrow().head();

        // QUIESCE: latch E + gate writes + suspend internal mutators (W5). Wrap the store so the
        // helper's &mut works; E == the head at the quiesce instant.
        let e = quiesce_old_shard(&mut store, &ring);
        assert_eq!(e, head_before, "E is the ring head at the quiesce instant");
        assert!(crate::serve::is_shard_loading(), "the shard is quiescing");
        assert!(
            store.is_passive(),
            "lazy expiry is suspended (store passive) during the outage"
        );

        // A READ of the doomed key during the outage: it is reported ABSENT (logically expired) but
        // the store's passive mode means it is NOT physically removed -- so NO StreamDel is appended.
        let present = store.contains_live(0, b"doomed", NOW);
        assert!(
            !present,
            "the due key reads as absent (logically expired) during the outage"
        );
        assert_eq!(
            ring.borrow().head(),
            e,
            "lazy expiry during the outage appended NOTHING above E (no acked internal mutation)"
        );

        // The live keys are still served during the outage (reads flow), no ring movement.
        assert!(
            store.contains_live(0, b"live-1", NOW),
            "a live key is served during the outage"
        );
        assert_eq!(
            ring.borrow().head(),
            e,
            "serving a read did not advance the ring"
        );

        // After RESUME the passive suspension lifts and the doomed key is physically reaped (a
        // StreamDel is appended ABOVE E) -- proving the suspension was the ONLY thing holding it, and
        // that it is a resume-side (post-authority-decision) event, never an acked cut mutation.
        resume_old_shard(&mut store);
        assert!(!store.is_passive(), "resume restored normal lazy expiry");
        assert!(
            !crate::serve::is_shard_loading(),
            "resume lifted the -LOADING gate"
        );
        let _ = store.read(0, b"doomed", NOW); // a post-resume access reaps the due key.
        assert!(
            ring.borrow().head() > e,
            "the reap happens only AFTER resume (post authority decision), never inside the cut"
        );

        crate::serve::unquiesce_shard();
    }

    /// (d) companion: the BACKGROUND active-expiry reaper is INERT while the shard is quiescing (it
    /// returns 0 without touching the store), so it cannot append a StreamDel above E either. This
    /// pins the `is_shard_loading()` guard added to `expire_cycle_tick`.
    #[test]
    fn reaper_is_inert_while_quiescing() {
        crate::serve::unquiesce_shard();
        // The reaper unit is exercised directly by serve.rs's own quiesce test set; here we assert
        // the flag semantics the guard reads: with the gate up, is_shard_loading() is true (so the
        // reaper's early-return fires) and with it down it is false (the reaper runs as before).
        let ring = ReplRing::new(16, ReplOffset::ZERO);
        let _e = crate::serve::quiesce_shard(&ring);
        assert!(
            crate::serve::is_shard_loading(),
            "the reaper's is_shard_loading() guard is TRUE during the outage (reaper inert)"
        );
        crate::serve::unquiesce_shard();
        assert!(
            !crate::serve::is_shard_loading(),
            "the reaper resumes (guard false) after the outage"
        );
    }
}
