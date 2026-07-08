// SPDX-License-Identifier: MIT OR Apache-2.0
//! The binary's PERSISTENCE serve wiring (#58 durable on-disk snapshot): load-on-boot, the
//! cross-shard `SAVE` / `BGSAVE` fan-out (each shard dumps its OWN partition on its OWN thread via
//! the forkless `ironcache_persist::dump_shard_keyspace`), the manifest commit, `LASTSAVE`, and the
//! periodic save policy.
//!
//! This lives in the BINARY (not `ironcache-server`) because persistence needs the CONCRETE
//! per-shard [`crate::serve::ShardStoreImpl`] (to dump it via `snapshot_chunk` + `insert_object`),
//! the `data_dir` (the snapshot location), and the env Clock (the save timestamp / `LASTSAVE`
//! value) -- none of which the generic, store-waist-only dispatch layer has. The serve router
//! INTERCEPTS `SAVE` / `BGSAVE` / `LASTSAVE` BEFORE the generic dispatch (exactly like the raft
//! `CLUSTER` mutator + the `WholeKeyspace` fan-out); the `ironcache-server` dispatch arms for these
//! commands are the persistence-disabled fallback (no data_dir / a path that reaches dispatch
//! directly).
//!
//! ## Each shard dumps its own file (no cross-shard lock)
//!
//! A node runs `shards` per-core stores (ADR-0002, shared-nothing). A save FANS OUT an internal
//! `__ICSAVE` verb to every shard via the coordinator: each shard runs
//! [`ironcache_persist::save_shard_to_dir`] against ITS OWN store ON ITS OWN thread (the forkless,
//! memory-neutral `snapshot_chunk` pull), writes `<data_dir>/dump-shard-<n>.icss` ATOMICALLY (tmp ->
//! fsync -> rename), and returns its manifest entry. The home core then COMMITS the snapshot by
//! writing the manifest LAST (atomic + fsync'd), so a crash mid-save leaves the prior good snapshot.
//!
//! Because each shard dumps at its OWN instant, the cross-shard snapshot is PER-SHARD-CONSISTENT but
//! CROSS-SHARD FUZZY (no global point-in-time); acceptable for a cache, NOT a fork-COW RDB.
//!
//! ## `SAVE` vs `BGSAVE`
//!
//! - `SAVE` BLOCKS the issuing connection until every shard has written + the manifest is committed
//!   (Redis parity), then replies `+OK`. The per-shard dump uses the forkless `snapshot_chunk`, so
//!   it never double-memories the keyspace.
//! - `BGSAVE` kicks the SAME save off the ISSUING request path (spawned on the home shard's
//!   executor) and replies `+Background saving started` immediately, so the ISSUING connection is
//!   not blocked.
//! - YIELDING dump (#571): each dumping shard now re-acquires its store borrow PER CHUNK and
//!   `yield`s between snapshot chunks (`save_shard_local`), so a `SAVE`/`BGSAVE` no longer
//!   monopolizes the serving shard for its whole keyspace dump -- the shard services queued writes
//!   DURING the dump (a bounded, predictable save tail instead of a full-keyspace block). The
//!   tradeoff is that the snapshot is no longer a strict per-shard point-in-time; it is an
//!   APPROXIMATE warm-start restore point (a deliberate choice for a cache, see the
//!   `ironcache_persist` module consistency note). The crash-safety invariant is unaffected: the
//!   manifest is still written LAST, so a torn/partial dump is never loaded.
//!
//! ## Default-off (#58)
//!
//! [`PersistState::from_config`] returns `None` when no `data_dir` is configured. The serve router
//! only intercepts the persistence commands when `Some`; with `None` they fall through to the
//! generic persistence-disabled dispatch fallback, no files are ever written, and the hot path +
//! boot path are byte-unchanged.
//!
//! ## Determinism (ADR-0003)
//!
//! The save TIMESTAMP (`save_unix_secs`, what `LASTSAVE` reports) and the dump's lazy-expiry `now`
//! are read from the shard's `ironcache-env` Clock seam, never `std::time`.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ironcache_config::Config;
use ironcache_runtime::bootstrap::ShardId;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{Request, Value};

use crate::coordinator::{self, Inbox};

/// The INTERNAL cross-shard SAVE verb (mirrors `__ICEXISTS` / `__ICPUBLISH`): the home core
/// broadcasts `__ICSAVE <save_unix_secs>` to every shard via the coordinator inbox; each shard
/// dumps ITS OWN partition to its file and returns its manifest entry. NOT a client command (the
/// serve router gates it like the other internal verbs); it is dispatched DIRECTLY by the
/// coordinator's [`crate::coordinator::run_remote`], so it is DELIBERATELY ABSENT from the
/// `spec_of` registry (like `__ICEXISTS`).
pub const ICSAVE: &[u8] = b"__ICSAVE";

/// The bound for the EXIT-path save wait (#139, H1/L1): the longest an exit path (a `SHUTDOWN SAVE`,
/// a bare `SHUTDOWN` with a policy, or a SIGTERM save-on-exit) will (a) wait for a busy save latch to
/// free before its own fresh save, and (b) let its OWN save fan-out run before giving up. It is sized
/// like the drain grace (a few seconds): generous for a healthy save, far under any supervisor's
/// hard-kill grace. On a genuinely wedged save the exit proceeds best-effort rather than hanging
/// forever (the in-flight save MAY still commit its prior-or-partial state; we cannot do better
/// without unbounded waiting). The wait is driven through the Runtime timer SEAM (ADR-0003), never
/// wall-clock.
pub const SHUTDOWN_SAVE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// The poll cadence the exit-path latch wait uses while it waits for a busy save to free the latch
/// (#139, H1). On a single-threaded shard executor each timer await YIELDS to the in-flight save
/// task so it can run to completion + drop its `SaveGuard`; this short tick keeps the wait responsive
/// without busy-spinning. Driven through the Runtime timer seam (ADR-0003).
const SHUTDOWN_SAVE_POLL: std::time::Duration = std::time::Duration::from_millis(20);

/// The NODE-LEVEL persistence state: the on-disk snapshot location, the save policy, and the
/// runtime atomics (the last-save timestamp `LASTSAVE` reports, a monotone save id, the dirty
/// write counter the periodic policy reads, and a save-in-progress latch that serializes
/// concurrent saves). ONE per node, shared by `Arc` into the serve + drain + periodic-save paths.
///
/// `Some` ONLY when a `data_dir` is configured ([`Self::from_config`]); the default static path
/// carries `None` and never even allocates this, so the default posture is byte-unchanged.
#[derive(Debug)]
pub struct PersistState {
    /// The data directory: the snapshot location (`<dir>/dump-shard-<n>.icss` + `<dir>/dump.manifest`).
    pub dir: PathBuf,
    /// The LIVE node-level persistence stats (last-save time + dirty counter), shared by `Arc` with
    /// `ServerContext` (the INFO `# Persistence` path) and the `/metrics` gauges so all three read
    /// the SAME atomics. The last-save time is SEEDED ON BOOT from the loaded snapshot's
    /// `dump.manifest` ([`seed_last_save_from_manifest`], durability fix #2) and stamped on every
    /// committed save; the dirty counter is bumped per write while persistence is enabled and reset
    /// on a committed save. Lives in `ironcache-observe` (below this binary) so the server crate's
    /// `ServerContext` can hold it without an upward dependency.
    pub stats: Arc<ironcache_observe::PersistRuntime>,
    /// A monotone SAVE ID, incremented per committed save (informational; recorded in the manifest).
    pub save_id: AtomicU64,
    /// SERIALIZE concurrent saves: a save sets this `true` (compare-exchange) for its duration so a
    /// second concurrent SAVE/BGSAVE/periodic-tick does not race on the same files + manifest. A
    /// would-be concurrent save observes `true` and is a no-op (BGSAVE already running -> reply
    /// success; SAVE waits via the coordinator anyway, but the latch keeps the manifest write
    /// single-writer). Relaxed CAS; node-level cold state.
    pub saving: std::sync::atomic::AtomicBool,
}

impl PersistState {
    /// Build the node-level persistence state from the resolved config, or `None` when persistence
    /// is OFF (no `data_dir`). `Some` is the single enable switch for the whole persistence path.
    #[must_use]
    pub fn from_config(config: &Config) -> Option<Arc<PersistState>> {
        let dir = config.data_dir.clone()?;
        Some(Arc::new(PersistState {
            dir,
            stats: Arc::new(ironcache_observe::PersistRuntime::new()),
            save_id: AtomicU64::new(0),
            saving: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    /// The shared persistence-stats cell (last-save + dirty), for handing into `ServerContext` and
    /// the `/metrics` gauges so they read the SAME live atomics this state writes.
    #[must_use]
    pub fn stats(&self) -> Arc<ironcache_observe::PersistRuntime> {
        Arc::clone(&self.stats)
    }

    /// The unix-seconds of the last successful save (the `LASTSAVE` reply), relaxed read.
    #[must_use]
    pub fn last_save(&self) -> u64 {
        self.stats.last_save_unix_secs()
    }

    /// The dirty (changes-since-last-save) counter, relaxed read.
    #[must_use]
    pub fn dirty(&self) -> u64 {
        self.stats.dirty()
    }

    /// SEED the last-save time from a loaded snapshot's `dump.manifest` save timestamp on boot
    /// (durability footgun fix #2): after `load_on_boot` restores a snapshot, `LASTSAVE` /
    /// INFO `rdb_last_save_time` must report the snapshot's save time, not `0` -- otherwise external
    /// "snapshot stale" monitoring misfires the instant the node boots. Reads the committed manifest
    /// in `self.dir`; a missing / torn manifest (nothing was loaded) leaves the seed at `0`, the
    /// pre-fix posture. Idempotent + cheap (one manifest read); called ONCE at boot before serving.
    /// Does NOT overwrite a value a real save already set (it only seeds when still `0`), so it
    /// cannot clobber a fresher in-process save.
    pub fn seed_last_save_from_manifest(&self) {
        if self.stats.last_save_unix_secs() != 0 {
            return;
        }
        if let Some(manifest) = ironcache_persist::read_manifest(&self.dir) {
            self.stats.set_last_save_unix_secs(manifest.save_unix_secs);
        }
    }

    /// Bump the dirty write counter (relaxed). Called by the serve loop after a successful write
    /// command when persistence is enabled. The single allowed hot-adjacent atomic increment.
    pub fn note_write(&self) {
        self.stats.note_write();
    }

    /// Try to ACQUIRE the save latch, returning a [`SaveGuard`] RAII handle if this caller won it
    /// (the guard CLEARS the latch on drop -- normal completion, panic-unwind, OR task cancellation),
    /// or `None` if a save is already in progress.
    ///
    /// H3: the guard is the ONLY release path. The previous bare `release_save()` after the
    /// `.await` was NOT panic/cancel-safe: if `do_save_all` panicked, or a spawned BGSAVE task was
    /// cancelled at shutdown before the release ran, the `saving` flag stayed `true` FOREVER and
    /// every later save became a silent no-op (so a later restart lost everything since the stuck
    /// save). Releasing in `Drop` fixes all three (completion, unwind, cancel).
    pub fn try_begin_save(self: &Arc<Self>) -> Option<SaveGuard> {
        if self
            .saving
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Some(SaveGuard {
                persist: Arc::clone(self),
            })
        } else {
            None
        }
    }

    /// Release the save latch (the low-level primitive [`SaveGuard`] calls on drop). Prefer
    /// [`Self::try_begin_save`]'s guard; this is exposed only for the guard's `Drop`.
    pub fn release_save(&self) {
        self.saving.store(false, Ordering::Release);
    }

    /// Record a COMMITTED save: stamp the last-save time, bump the save id, reset the dirty counter
    /// to 0, and mark the last-save outcome OK (relaxed). Called on the home core after the manifest
    /// is written. The OK stamp clears any prior `err` so INFO `rdb_last_bgsave_status` recovers to
    /// `ok` on the next successful save (#549).
    pub fn record_committed(&self, save_unix_secs: u64) {
        self.stats.set_last_save_unix_secs(save_unix_secs);
        self.save_id.fetch_add(1, Ordering::Relaxed);
        self.stats.reset_dirty();
        self.stats.set_last_bgsave_ok(true);
    }

    /// Record a FAILED save (#549): mark the last-save outcome as `err` so INFO
    /// `rdb_last_bgsave_status` reports the failure (the canonical "last save failed" alert). The
    /// last-save TIME + dirty counter are left untouched (the prior committed snapshot stays valid).
    /// Called by the save fan-out on any failure arm.
    pub fn record_save_failed(&self) {
        self.stats.set_last_bgsave_ok(false);
    }

    /// The next monotone save id (the value the next save will record; read for the manifest).
    #[must_use]
    pub fn next_save_id(&self) -> u64 {
        self.save_id.load(Ordering::Relaxed).saturating_add(1)
    }
}

/// An RAII guard for the save latch (H3): held for the duration of one save, it CLEARS the `saving`
/// flag on [`Drop`] -- on normal completion, on a panic unwinding through the save, AND on a spawned
/// save task being CANCELLED (e.g. at shutdown). This guarantees ONE failed/cancelled save can never
/// permanently wedge the latch (which would silently disable every later SAVE/BGSAVE and the
/// periodic save). Obtain it from [`PersistState::try_begin_save`]; do NOT call `release_save`
/// manually while a guard is live (the guard owns the release).
#[derive(Debug)]
pub struct SaveGuard {
    persist: Arc<PersistState>,
}

impl Drop for SaveGuard {
    fn drop(&mut self) {
        self.persist.release_save();
    }
}

/// Encode one shard's [`ironcache_persist::ShardManifestEntry`] into the `__ICSAVE` reply Value:
/// `*3 [:shard :keys :crc]`, so the home core can reconstruct the manifest entry from each shard's
/// reply (the `crc` is a u32 widened to an i64 RESP integer, lossless). A shard that FAILED to
/// write its file replies an `Error`, which the home core surfaces as a failed SAVE.
#[must_use]
pub fn encode_save_reply(entry: &ironcache_persist::ShardManifestEntry) -> Value {
    Value::Array(Some(vec![
        Value::Integer(i64::from(entry.shard)),
        #[allow(clippy::cast_possible_wrap)]
        Value::Integer(entry.keys as i64),
        Value::Integer(i64::from(entry.crc)),
    ]))
}

/// Decode one shard's `__ICSAVE` reply Value back into a [`ironcache_persist::ShardManifestEntry`]
/// (the inverse of [`encode_save_reply`]), or `None` if the reply is not the `*3 [:shard :keys
/// :crc]` shape (a shard error / a shard-unavailable reply / a malformed reply). The `file` name is
/// recomputed from the shard index (the shard wrote `dump-shard-<shard>.icss`).
#[must_use]
pub fn decode_save_reply(value: &Value) -> Option<ironcache_persist::ShardManifestEntry> {
    let Value::Array(Some(items)) = value else {
        return None;
    };
    let [
        Value::Integer(shard),
        Value::Integer(keys),
        Value::Integer(crc),
    ] = items.as_slice()
    else {
        return None;
    };
    let shard = u32::try_from(*shard).ok()?;
    let keys = u64::try_from(*keys).ok()?;
    let crc = u32::try_from(*crc).ok()?;
    Some(ironcache_persist::ShardManifestEntry {
        shard,
        file: ironcache_persist::shard_file_name(shard),
        keys,
        crc,
    })
}

/// Build the `__ICSAVE <save_unix_secs> <shard_index> <dir>` request for one shard.
fn icsave_request(save_unix_secs: u64, shard: usize, dir: &std::path::Path) -> Request {
    Request {
        args: vec![
            bytes::Bytes::from_static(ICSAVE),
            bytes::Bytes::from(save_unix_secs.to_string()),
            bytes::Bytes::from(shard.to_string()),
            bytes::Bytes::copy_from_slice(dir.to_string_lossy().as_bytes()),
        ],
    }
}

/// PERFORM a full cross-shard SAVE (#58): fan an `__ICSAVE` out to EVERY shard (each dumps its OWN
/// partition to its file ON ITS OWN thread via the forkless `snapshot_chunk`), collect the per-shard
/// manifest entries, and COMMIT the snapshot by writing the manifest LAST (atomic + fsync'd). On
/// success records the committed save (stamps `LASTSAVE`, bumps the save id, resets the dirty
/// counter) and returns `Ok(())`; on any shard error or the manifest write failing returns an
/// `Err(message)` the caller surfaces.
///
/// SERIALIZED: the caller must hold the save latch ([`PersistState::try_begin_save`]'s [`SaveGuard`])
/// so two saves never race on the same files + manifest. The guard releases the latch on drop
/// (completion, panic, or cancellation), so this fn never needs to release it itself.
///
/// The fan-out uses [`coordinator::fan_out_save`] (a DIFFERENT sub-request per shard -- each carries
/// its own shard index for its file name): the home shard's `__ICSAVE` runs INLINE on the YIELDING
/// [`coordinator::run_local_save`], every other shard off its drain loop. Each per-shard dump yields
/// between snapshot chunks (#571) so the shard services queued writes DURING the dump. No `RefCell`
/// borrow is held across any await (the per-shard `save_shard_local` releases its per-chunk store
/// borrow before each yield).
///
/// `save_unix_secs` is the home core's wall-clock time (read from the env Clock seam by the caller,
/// ADR-0003); it is recorded in the manifest and reported by `LASTSAVE`.
pub async fn do_save_all(
    persist: &Arc<PersistState>,
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    save_unix_secs: u64,
) -> Result<(), String> {
    // Record the save OUTCOME so INFO `rdb_last_bgsave_status` is honest (#549): the success path
    // stamps OK inside `record_committed`; every FAILURE arm funnels through this ONE place, so a
    // failed save flips the status to `err` regardless of which arm failed (dir create, a shard
    // error, or the manifest write).
    let outcome = save_all_attempt(persist, inbox, ctx, home, db, save_unix_secs).await;
    if outcome.is_err() {
        persist.record_save_failed();
    }
    outcome
}

/// The inner cross-shard SAVE attempt: the actual fan-out + manifest commit (records the COMMITTED
/// save on success). [`do_save_all`] wraps it to record a FAILED save on any `Err` (#549).
async fn save_all_attempt(
    persist: &Arc<PersistState>,
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    save_unix_secs: u64,
) -> Result<(), String> {
    let n_shards = inbox.len();
    let dir = persist.dir.clone();
    // Ensure the data directory exists (idempotent; a create failure fails the save cleanly).
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return Err(format!("cannot create data dir {}: {e}", dir.display()));
    }
    // One `__ICSAVE` sub-request per shard, each carrying its own shard index for its file name.
    let subreqs: Vec<(usize, Request)> = (0..n_shards)
        .map(|shard| (shard, icsave_request(save_unix_secs, shard, &dir)))
        .collect();
    // Fan out via the SAVE-specific fan-out (#571): the home shard's own dump runs INLINE on the
    // YIELDING `run_local_save` (awaits between snapshot chunks), which a synchronous `fan_out_split`
    // local closure cannot express; every other shard dumps off its drain loop (also yielding).
    let replies = coordinator::fan_out_save(ctx, inbox, home, db, subreqs).await;

    // Collect the per-shard manifest entries; surface the FIRST shard error as a failed save (a
    // partial set of files without a committed manifest is harmless -- the prior manifest stays
    // committed, and load ignores files the manifest does not vouch for).
    let mut entries = Vec::with_capacity(replies.len());
    for (shard, reply) in replies {
        let Some(entry) = decode_save_reply(&reply.value) else {
            let detail = match &reply.value {
                Value::Error(e) => e.message().to_owned(),
                _ => "unexpected reply".to_owned(),
            };
            return Err(format!("shard {shard} save failed: {detail}"));
        };
        entries.push(entry);
    }

    // COMMIT: write the manifest LAST (the atomic commit point).
    let save_id = persist.next_save_id();
    match ironcache_persist::write_manifest(&dir, save_id, save_unix_secs, entries) {
        Ok(_) => {
            persist.record_committed(save_unix_secs);
            Ok(())
        }
        Err(e) => Err(format!("manifest write failed: {e}")),
    }
}

/// WAIT (BOUNDED) to acquire the save latch on an EXIT path (#139, H1: never exit OVER an in-flight
/// save). Polls [`PersistState::try_begin_save`] through the Runtime timer SEAM (ADR-0003, NOT
/// wall-clock) until it WINS the latch (returns `Some(guard)`) OR the `deadline` elapses (returns
/// `None`). On a single-threaded shard executor each `rt.timer(..)` await YIELDS to the in-flight
/// save task so it can finish its `do_save_all` (commit its manifest) and DROP its `SaveGuard`,
/// freeing the latch for this waiter on a later poll.
///
/// THE DATA-LOSS FIX: the old exit paths did `try_begin_save() else { exit(0) }` -- a busy latch made
/// them exit IMMEDIATELY over a concurrent save that had written some `.icss` files but NOT yet its
/// `write_manifest` (the atomic commit point), so the committed manifest still pointed at the PRIOR
/// snapshot and every write since was lost. Waiting for the latch to free guarantees that in-flight
/// save commits (or that we then run a fresh one) before exit.
///
/// A `None` return is the LOW-case wedged-save outcome: a genuinely stuck save never frees the latch,
/// so the caller proceeds to a BEST-EFFORT exit (it logs + exits rather than hanging forever; the
/// in-flight save may still commit its prior-or-partial state). This bounds the worst case.
///
/// Holds NO borrow across the await (it only touches the `saving` atomic + the timer), so it cannot
/// deadlock the in-flight save it is waiting on.
pub async fn wait_to_begin_save(
    persist: &Arc<PersistState>,
    deadline: std::time::Duration,
) -> Option<SaveGuard> {
    use ironcache_runtime::Runtime;
    // Fast path: the latch is free right now (no in-flight save), so win it without a single tick.
    if let Some(guard) = persist.try_begin_save() {
        return Some(guard);
    }
    let rt = ironcache_runtime::TokioRuntime::new();
    let mut waited = std::time::Duration::ZERO;
    while waited < deadline {
        // Yield to the in-flight save task (single-threaded executor) so it can commit + drop its
        // guard; then retry. The poll never exceeds the remaining budget (`waited < deadline` here,
        // so the saturating_sub is non-zero; it just keeps the arithmetic panic-free).
        let tick = SHUTDOWN_SAVE_POLL.min(deadline.saturating_sub(waited));
        rt.timer(tick).await;
        waited += tick;
        if let Some(guard) = persist.try_begin_save() {
            return Some(guard);
        }
    }
    None
}

/// Run [`do_save_all`] on an EXIT path with a BOUNDED fan-out (#139, L1: a wedged sibling whose drain
/// loop is ALIVE but stuck never answers + never drops its oneshot sender, so a bare `do_save_all`
/// awaits FOREVER -- the signal path escapes via the second-signal force-exit watcher, but a pure
/// `SHUTDOWN SAVE` command had no escape). This races the save against the Runtime timer SEAM
/// (ADR-0003): on timeout it surfaces a failed/partial save (`Err`) so the EXIT path can proceed
/// (reply an error and still exit, or best-effort exit) rather than hang.
///
/// The caller MUST hold the save latch (a [`SaveGuard`]) for the duration, exactly like
/// [`do_save_all`]. The NORMAL SAVE / BGSAVE / periodic paths are UNCHANGED (they keep the plain
/// `do_save_all`); only the exit paths use this bound, where a hang is the real hazard.
pub async fn do_save_all_bounded(
    persist: &Arc<PersistState>,
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    save_unix_secs: u64,
    timeout: std::time::Duration,
) -> Result<(), String> {
    use ironcache_runtime::Runtime;
    let rt = ironcache_runtime::TokioRuntime::new();
    tokio::select! {
        biased;
        result = do_save_all(persist, inbox, ctx, home, db, save_unix_secs) => result,
        () = rt.timer(timeout) => {
            // The timeout cancels the in-flight `do_save_all` (its own failure-recording never runs),
            // so record the failed outcome here so INFO `rdb_last_bgsave_status` reports `err` (#549).
            persist.record_save_failed();
            Err("save fan-out timed out (a shard did not answer in time)".to_owned())
        }
    }
}

/// LOAD a committed snapshot into the per-shard stores at boot (#58 load-on-boot), RE-SHARDING every
/// key into its OWNING shard for the CURRENT shard count (the C1 fix). Returns the total keys
/// loaded, or `0` (loads nothing) when there is no loadable snapshot (no manifest / a torn
/// manifest). EVERY manifest shard file is read and each key is inserted into the store for
/// `ironcache_server::owner_shard(key, stores.len())` -- the SAME owner-shard hash the router uses --
/// so a snapshot taken with a DIFFERENT shard count reconstructs the full keyspace correctly (no key
/// lost when the count shrinks, no GET misrouted when it grows). A torn / CRC-bad / missing shard
/// file is skipped (its keys are absent), never corrupt-loaded.
///
/// The binary's live boot path does NOT call this (each shard owns its store on its own thread, so
/// it calls [`ironcache_persist::load_shard_resharded`] per shard via the drain loop); this
/// all-stores wrapper is the single-thread convenience used where every store is reachable at once.
///
/// `now` is the boot wall-clock (the env Clock seam): an already-expired key is dropped on load.
pub fn load_on_boot(
    persist: &Arc<PersistState>,
    stores: &mut [&mut crate::serve::ShardStoreImpl],
    now: ironcache_storage::UnixMillis,
) -> u64 {
    ironcache_persist::load_all(stores, &persist.dir, now, ironcache_server::owner_shard)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;
    use std::sync::atomic::AtomicBool;

    /// A bare `PersistState` for latch tests (no real data dir / save fan-out needed).
    fn latch_state() -> Arc<PersistState> {
        Arc::new(PersistState {
            dir: PathBuf::from("/nonexistent-test-dir"),
            stats: Arc::new(ironcache_observe::PersistRuntime::new()),
            save_id: AtomicU64::new(0),
            saving: AtomicBool::new(false),
        })
    }

    /// Durability footgun fix #2: `seed_last_save_from_manifest` seeds `LASTSAVE` from a loaded
    /// snapshot's manifest save timestamp, so external "snapshot stale" monitoring does not misfire
    /// the instant a node boots from a snapshot. A node with no manifest stays at `0`; after a
    /// manifest is written with a save time, the seed reflects it; and the seed never clobbers a
    /// fresher in-process save (it only seeds when still `0`).
    #[test]
    fn seed_last_save_from_manifest_seeds_lastsave_on_boot() {
        let dir = std::env::temp_dir().join(format!("ic-seed-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let p = Arc::new(PersistState {
            dir: dir.clone(),
            stats: Arc::new(ironcache_observe::PersistRuntime::new()),
            save_id: AtomicU64::new(0),
            saving: AtomicBool::new(false),
        });
        // No manifest yet -> the seed leaves LASTSAVE at 0 (the pre-snapshot posture).
        p.seed_last_save_from_manifest();
        assert_eq!(p.last_save(), 0, "no manifest -> LASTSAVE stays 0");
        // Write a manifest carrying a save time, then seed: LASTSAVE reflects it.
        ironcache_persist::write_manifest(&dir, 1, 1_700_000_000, Vec::new())
            .expect("write manifest");
        p.seed_last_save_from_manifest();
        assert_eq!(
            p.last_save(),
            1_700_000_000,
            "the seed reflects the loaded snapshot's manifest save time"
        );
        // A fresher in-process save value is NOT clobbered by a (re-)seed.
        p.record_committed(1_700_009_999);
        p.seed_last_save_from_manifest();
        assert_eq!(
            p.last_save(),
            1_700_009_999,
            "the seed never overwrites a fresher in-process save"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The RAII guard serializes: while it is held a second `try_begin_save` is denied, and dropping
    /// it frees the latch so the next save proceeds.
    #[test]
    fn save_guard_serializes_and_releases_on_drop() {
        let p = latch_state();
        let g = p.try_begin_save().expect("first save wins the latch");
        assert!(
            p.saving.load(Ordering::Acquire),
            "latch held while guard live"
        );
        assert!(
            p.try_begin_save().is_none(),
            "a concurrent save is denied while the guard is held"
        );
        drop(g);
        assert!(
            !p.saving.load(Ordering::Acquire),
            "the latch releases when the guard drops"
        );
        // The next save now proceeds (the latch is free).
        let g2 = p
            .try_begin_save()
            .expect("the next save wins after release");
        drop(g2);
    }

    /// H3 REGRESSION: a save that PANICS (unwinds) through the held guard STILL releases the latch,
    /// so the next save is NOT permanently wedged. The old bare `release_save()` after the await was
    /// skipped on a panic / cancellation, leaving `saving == true` forever.
    #[test]
    fn panicking_save_still_releases_the_latch() {
        let p = latch_state();
        let p2 = Arc::clone(&p);
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = p2.try_begin_save().expect("won the latch");
            assert!(p2.saving.load(Ordering::Acquire));
            panic!("simulate a save panicking mid-flight");
        }));
        assert!(res.is_err(), "the save panicked");
        assert!(
            !p.saving.load(Ordering::Acquire),
            "the latch is released on unwind (not wedged), so the next save can proceed"
        );
        // Prove the next save proceeds.
        let g = p
            .try_begin_save()
            .expect("the next save proceeds after a panicked save");
        drop(g);
    }

    /// Run an async body on a current-thread tokio runtime + `LocalSet` (the shard executor shape the
    /// Runtime timer seam + `spawn_local` need). The test uses SHORT real timers (the wait polls on a
    /// 20ms cadence) through the exact Runtime seam the production wait uses, so each test resolves in
    /// well under a second.
    fn block_on_shard<F: std::future::Future>(body: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, body)
    }

    /// H1 (data loss) -- the EXIT-path wait does NOT acquire the latch WHILE an in-flight save holds
    /// it, and DOES acquire it once that save releases. We simulate the in-flight save by holding a
    /// `SaveGuard`, drive `wait_to_begin_save` as a spawned task on the SAME single-threaded executor,
    /// release the guard partway through, and confirm the waiter only then wins the latch -- never
    /// returning "acquired" while the latch was held. This is the property the data-loss fix needs:
    /// an exit never proceeds OVER an in-flight (uncommitted) save.
    #[test]
    fn wait_to_begin_save_blocks_while_held_then_acquires_on_release() {
        block_on_shard(async {
            use ironcache_runtime::Runtime;
            let p = latch_state();
            // The "in-flight save" holds the latch.
            let in_flight = p
                .try_begin_save()
                .expect("the in-flight save wins the latch");

            // A flag the waiter sets the instant it acquires, so we can assert it had NOT acquired
            // while the guard was still held.
            let acquired = Rc::new(std::cell::Cell::new(false));
            let acquired_in_task = Rc::clone(&acquired);
            let p_task = Arc::clone(&p);
            let rt = ironcache_runtime::TokioRuntime::new();
            rt.spawn_on_shard(async move {
                let guard = wait_to_begin_save(&p_task, std::time::Duration::from_secs(60)).await;
                assert!(guard.is_some(), "the waiter acquires after the latch frees");
                acquired_in_task.set(true);
                drop(guard); // release for tidiness.
            });

            // Let the waiter run a few polls WHILE we still hold the latch: it must NOT acquire.
            let rt2 = ironcache_runtime::TokioRuntime::new();
            for _ in 0..5 {
                rt2.timer(std::time::Duration::from_millis(20)).await;
            }
            assert!(
                !acquired.get(),
                "the waiter must NOT acquire while the in-flight save holds the latch (H1)"
            );
            assert!(p.saving.load(Ordering::Acquire), "latch still held");

            // Release the in-flight save's guard; the waiter should win the latch on its next poll.
            drop(in_flight);
            for _ in 0..5 {
                rt2.timer(std::time::Duration::from_millis(20)).await;
                if acquired.get() {
                    break;
                }
            }
            assert!(
                acquired.get(),
                "the waiter acquires once the in-flight save releases the latch (H1)"
            );
        });
    }

    /// L1 / the bounded-wait timeout -- when the latch stays held PAST the deadline (a genuinely
    /// wedged save), `wait_to_begin_save` returns `None` CLEANLY (no hang) so the exit path can
    /// proceed best-effort. We hold a `SaveGuard` for the whole test and assert the wait, given a
    /// short deadline, gives up with `None` (and never falsely returns a guard while the latch is
    /// held).
    #[test]
    fn wait_to_begin_save_times_out_cleanly_when_latch_stays_held() {
        block_on_shard(async {
            let p = latch_state();
            // Hold the latch for the entire wait (simulate a wedged in-flight save).
            let _held = p.try_begin_save().expect("hold the latch");
            let outcome = wait_to_begin_save(&p, std::time::Duration::from_millis(200)).await;
            assert!(
                outcome.is_none(),
                "a latch held past the deadline times out to None (no hang, no false acquire)"
            );
            assert!(
                p.saving.load(Ordering::Acquire),
                "the latch is STILL held by the simulated in-flight save (the waiter never took it)"
            );
        });
    }

    /// The fast path: with the latch FREE, `wait_to_begin_save` acquires immediately (returns a real
    /// guard) and that guard serializes a concurrent attempt, exactly like `try_begin_save`.
    #[test]
    fn wait_to_begin_save_acquires_immediately_when_free() {
        block_on_shard(async {
            let p = latch_state();
            let guard = wait_to_begin_save(&p, std::time::Duration::from_secs(5))
                .await
                .expect("a free latch is won at once");
            assert!(
                p.saving.load(Ordering::Acquire),
                "the waiter holds the latch"
            );
            assert!(
                p.try_begin_save().is_none(),
                "a concurrent save is denied while the waiter's guard is held"
            );
            drop(guard);
            assert!(
                !p.saving.load(Ordering::Acquire),
                "the latch frees when the waiter's guard drops"
            );
        });
    }
}
