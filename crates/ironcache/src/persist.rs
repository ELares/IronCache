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
//! - OFF-THREAD persist via PER-SLOT ARC-COW (#576): the shard no longer COPIES its keyspace at all.
//!   It FREEZES its store in O(slots) `Arc` refcount bumps (`ShardStore::begin_save`) and hands the
//!   frozen slot tables to a DEDICATED per-shard OS thread (`ic-persist-<n>`) that does the whole
//!   seconds-long ENCODE + FSYNC OFF the serving core, so the datapath stays ms-class DURING a save
//!   (the #576 p99.9 fix, which #571/#578/#586 could only blunt -- the O(N) serving-side copy WAS the
//!   contention). `save_shard_local` awaits the persist thread's file-write result on a
//!   `tokio::sync::oneshot` (a cross-thread wake, not a blocking join), so the shard keeps serving:
//!   reads share the frozen `Arc`s, and a write COW-copies its slot (a one-time deep clone) before
//!   mutating, so the frozen view the persist thread reads is never touched (`FrozenSlot`, the
//!   `unsafe impl Send` soundness argument). The detached persist thread touches ONLY the frozen
//!   `Arc`s + the filesystem, never a live shard cell, so the shared-nothing DATAPATH (ADR-0002) is
//!   unaffected; and it still writes the per-shard file before the home core commits the manifest
//!   LAST, so crash-safety is unchanged. The dump is now a per-shard POINT-IN-TIME as of the freeze
//!   (stronger than the #571 chunked walk); cross-shard it stays fuzzy.
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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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
    /// This is the DURABLE snapshot target (SAVE/BGSAVE/periodic + the crash-recovery source); the
    /// upgrade handoff (#390) NEVER touches it when it stages on tmpfs, so a crash mid-upgrade always
    /// recovers from the last durable snapshot here.
    pub dir: PathBuf,
    /// The resolved tmpfs BASE for the upgrade HANDOFF snapshot (#390), or `None` when tmpfs staging
    /// is unavailable (non-Linux, no `/dev/shm`, or a configured base that is not a tmpfs mount).
    /// The handoff staging dir is `ironcache::handoff::handoff_staging_dir(base)`; `None` makes the
    /// handoff save + boot load use the durable `data_dir`. Resolved ONCE at boot from
    /// `config.upgrade_handoff_dir`.
    pub handoff_base: Option<PathBuf>,
    /// The directory THIS boot LOADS its snapshot from: the tmpfs handoff staging dir when a valid,
    /// committed handoff manifest is present AND at least as fresh as the durable `data_dir`
    /// snapshot (the just-completed-upgrade case), else the durable `data_dir`. Resolved ONCE at
    /// construction (a cheap two-manifest read) so EVERY shard loads from the SAME source and the
    /// choice cannot flap mid-boot as shards load at different instants.
    pub boot_load_dir: PathBuf,
    /// The tmpfs handoff staging dir to REMOVE once every shard has finished load-on-boot -- `Some`
    /// ONLY when this boot loaded from tmpfs (`boot_load_dir` IS the staging dir). The ephemeral
    /// handoff must not leak across upgrades; cleanup is coordinated by `shards_pending_load` so it
    /// runs exactly once, AFTER the last shard has read the snapshot (never mid-load).
    pub handoff_cleanup_dir: Option<PathBuf>,
    /// Countdown of shards that have NOT yet finished load-on-boot, initialized to the shard count.
    /// Each shard decrements it after its load ([`Self::note_shard_loaded`]); the shard that drives
    /// it to zero removes `handoff_cleanup_dir`. This makes tmpfs cleanup safe under the
    /// shared-nothing per-shard load (a slower shard is never raced into an empty keyspace).
    pub shards_pending_load: AtomicUsize,
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
    /// Force the NEXT durable save to be a BASE, not a delta (#676). Set TRUE at boot -- per-shard
    /// dirty tracking is not armed until the FIRST save's epoch cut, so writes between boot/load and
    /// that first save are UNTRACKED and MUST be captured by a full base -- and set TRUE again after
    /// any EPHEMERAL handoff save (which drains the per-shard dirty set without advancing the durable
    /// base, so a subsequent delta's "since" point would otherwise skip the drained writes). Cleared
    /// once a durable BASE save re-establishes the base as the dirty "since" point. Irrelevant (never
    /// read) when `snapshot_deltas` is off. Relaxed; node-level cold state.
    pub needs_base: std::sync::atomic::AtomicBool,
}

impl PersistState {
    /// Build the node-level persistence state from the resolved config, or `None` when persistence
    /// is OFF (no `data_dir`). `Some` is the single enable switch for the whole persistence path.
    #[must_use]
    pub fn from_config(config: &Config) -> Option<Arc<PersistState>> {
        let dir = config.data_dir.clone()?;
        // Resolve the tmpfs handoff base (#390): the configured `upgrade_handoff_dir`, or the
        // built-in `/dev/shm` on Linux, but ONLY when it is a real RAM-backed tmpfs mount. `None`
        // (non-Linux / no tmpfs / a disk dir) self-binds every handoff to the durable `data_dir`.
        let handoff_base = crate::handoff::usable_tmpfs_base(config.upgrade_handoff_dir.as_deref());
        // Resolve, ONCE, which snapshot THIS boot loads from (tmpfs handoff vs durable data_dir).
        let (boot_load_dir, handoff_cleanup_dir) =
            resolve_boot_load_dir(&dir, handoff_base.as_deref());
        Some(Arc::new(PersistState {
            dir,
            handoff_base,
            boot_load_dir,
            handoff_cleanup_dir,
            shards_pending_load: AtomicUsize::new(config.shards.max(1)),
            stats: Arc::new(ironcache_observe::PersistRuntime::new()),
            save_id: AtomicU64::new(0),
            saving: std::sync::atomic::AtomicBool::new(false),
            // A base is OWED at boot: dirty tracking arms only at the first save's epoch cut, so the
            // first durable save must be a full base to capture the pre-tracking writes (#676).
            needs_base: std::sync::atomic::AtomicBool::new(true),
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
        // Read the manifest of the snapshot THIS boot actually loaded (`boot_load_dir`): the tmpfs
        // handoff when the node booted from it (#390), else the durable `data_dir`. So `LASTSAVE`
        // reflects the loaded snapshot's save time regardless of which source won the boot.
        if let Some(manifest) = ironcache_persist::read_manifest(&self.boot_load_dir) {
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

    /// Record a COMMITTED durable DELTA save (#676): stamp the last-save time, reset the dirty
    /// counter, and mark the outcome OK -- but do NOT bump `save_id`, because a delta STAYS on its
    /// base generation (the invariant a delta's `base_epoch` equals the manifest `save_id`, which the
    /// loader checks). `save_id` advances only when a fresh BASE is written ([`Self::record_committed`]
    /// / compaction). Like a base save, a committed delta IS a save for `LASTSAVE` + resets the
    /// changes-since-last-save counter (the delta persisted those changes).
    pub fn record_committed_delta(&self, save_unix_secs: u64) {
        self.stats.set_last_save_unix_secs(save_unix_secs);
        self.stats.reset_dirty();
        self.stats.set_last_bgsave_ok(true);
    }

    /// Whether the next durable save must be a full BASE (see [`Self::needs_base`]). Relaxed read.
    #[must_use]
    pub fn needs_base(&self) -> bool {
        self.needs_base.load(Ordering::Relaxed)
    }

    /// Clear the base-owed flag: a durable BASE save committed, so it is now the dirty "since" point
    /// and subsequent durable saves MAY be deltas (#676). Relaxed.
    pub fn clear_needs_base(&self) {
        self.needs_base.store(false, Ordering::Relaxed);
    }

    /// Force the next durable save to be a full BASE (#676): called after an ephemeral handoff save
    /// that drained the per-shard dirty set without advancing the durable base, so no write is
    /// skipped by a delta whose "since" point moved past it. Relaxed.
    pub fn force_needs_base(&self) {
        self.needs_base.store(true, Ordering::Relaxed);
    }

    /// Record a FAILED save (#549): mark the last-save outcome as `err` so INFO
    /// `rdb_last_bgsave_status` reports the failure (the canonical "last save failed" alert). The
    /// last-save TIME + dirty counter are left untouched (the prior committed snapshot stays valid).
    /// Called by the save fan-out on any failure arm.
    pub fn record_save_failed(&self) {
        self.stats.set_last_bgsave_ok(false);
    }

    /// Record a committed EPHEMERAL upgrade-handoff save on tmpfs (#390): stamp `LASTSAVE` (so the
    /// upgrade's SAVE-first confirmation sees it ADVANCE) and bump the save id, but DO NOT reset the
    /// durable dirty counter or the `rdb_last_bgsave_status`. The tmpfs handoff is ephemeral and does
    /// NOT touch the durable `data_dir`, so "changes since the last DURABLE save" and the durable-save
    /// health signal must stay honest: if the upgrade ABORTS and the process keeps running, the
    /// periodic policy still knows a durable save is owed. (The data_dir FALLBACK path uses
    /// [`Self::record_committed`] instead, since that write IS durable.)
    pub fn record_handoff_committed(&self, save_unix_secs: u64) {
        self.stats.set_last_save_unix_secs(save_unix_secs);
        self.save_id.fetch_add(1, Ordering::Relaxed);
    }

    /// Note that ONE shard has finished its load-on-boot; the shard that drives the countdown to
    /// zero cleans up the ephemeral tmpfs handoff (#390). Cleanup runs EXACTLY ONCE, AFTER the last
    /// shard has read the snapshot -- a per-shard load reads EVERY manifest file, so removing the
    /// staging dir mid-load would race a slower shard into an empty keyspace. `handoff_cleanup_dir`
    /// is `Some` only when this boot loaded FROM tmpfs, so a data_dir boot decrements harmlessly.
    pub fn note_shard_loaded(&self) {
        // fetch_sub returns the PRE-decrement value; `== 1` means this call took it to 0 (last shard).
        if self.shards_pending_load.fetch_sub(1, Ordering::AcqRel) == 1 {
            if let Some(dir) = self.handoff_cleanup_dir.as_deref() {
                match std::fs::remove_dir_all(dir) {
                    Ok(()) => tracing::info!(
                        dir = %dir.display(),
                        "ironcache upgrade: cleaned up the tmpfs handoff snapshot after load-on-boot"
                    ),
                    // Already gone (a concurrent cleanup / never written): nothing to do.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => tracing::warn!(
                        dir = %dir.display(),
                        error = %e,
                        "ironcache upgrade: could not remove the tmpfs handoff snapshot; it is \
                         ephemeral (a reboot clears it and the next handoff save truncates it)"
                    ),
                }
            }
        }
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

/// One shard's `__ICSAVE` reply, decoded (#676): either a BASE file (a full dump this shard wrote) or
/// a DELTA file (only the keys mutated since the base). The home core collects these into the
/// committed manifest -- base entries into `entries`, delta entries into the chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaveReply {
    /// A base file: `dump-shard-<shard>.icss`.
    Base(ironcache_persist::ShardManifestEntry),
    /// A delta file appended to the shard's chain: `dump-shard-<shard>-delta-<delta_epoch>.icsd`.
    Delta(ironcache_persist::DeltaManifestEntry),
}

/// Encode one shard's save result into the `__ICSAVE` reply Value, TAGGED so the home core knows
/// whether the shard wrote a base or a delta (#676). Base: `*4 [:0 :shard :keys :crc]`. Delta: `*7
/// [:1 :shard :puts :tombstones :crc :base_epoch :delta_epoch]`. Every field is a u32/u64 widened to
/// an i64 RESP integer (lossless for realistic key counts / epochs). A shard that FAILED to write its
/// file replies an `Error` instead, which the home core surfaces as a failed SAVE.
#[must_use]
#[allow(clippy::cast_possible_wrap)]
pub fn encode_save_reply(reply: &SaveReply) -> Value {
    match reply {
        SaveReply::Base(e) => Value::Array(Some(vec![
            Value::Integer(0), // tag: base
            Value::Integer(i64::from(e.shard)),
            Value::Integer(e.keys as i64),
            Value::Integer(i64::from(e.crc)),
        ])),
        SaveReply::Delta(d) => Value::Array(Some(vec![
            Value::Integer(1), // tag: delta
            Value::Integer(i64::from(d.shard)),
            Value::Integer(d.puts as i64),
            Value::Integer(d.tombstones as i64),
            Value::Integer(i64::from(d.crc)),
            Value::Integer(d.base_epoch as i64),
            Value::Integer(d.delta_epoch as i64),
        ])),
    }
}

/// Decode one shard's `__ICSAVE` reply Value back into a [`SaveReply`] (the inverse of
/// [`encode_save_reply`]), or `None` if the reply is not a recognized tagged shape (a shard error /
/// shard-unavailable / malformed reply). The `file` name is recomputed from the shard index (+
/// `delta_epoch` for a delta), matching what the shard wrote.
#[must_use]
pub fn decode_save_reply(value: &Value) -> Option<SaveReply> {
    let Value::Array(Some(items)) = value else {
        return None;
    };
    match items.as_slice() {
        [
            Value::Integer(0),
            Value::Integer(shard),
            Value::Integer(keys),
            Value::Integer(crc),
        ] => {
            let shard = u32::try_from(*shard).ok()?;
            Some(SaveReply::Base(ironcache_persist::ShardManifestEntry {
                shard,
                file: ironcache_persist::shard_file_name(shard),
                keys: u64::try_from(*keys).ok()?,
                crc: u32::try_from(*crc).ok()?,
            }))
        }
        [
            Value::Integer(1),
            Value::Integer(shard),
            Value::Integer(puts),
            Value::Integer(tombstones),
            Value::Integer(crc),
            Value::Integer(base_epoch),
            Value::Integer(delta_epoch),
        ] => {
            let shard = u32::try_from(*shard).ok()?;
            let delta_epoch = u64::try_from(*delta_epoch).ok()?;
            Some(SaveReply::Delta(ironcache_persist::DeltaManifestEntry {
                shard,
                file: ironcache_persist::delta_file_name(shard, delta_epoch),
                puts: u64::try_from(*puts).ok()?,
                tombstones: u64::try_from(*tombstones).ok()?,
                crc: u32::try_from(*crc).ok()?,
                base_epoch: u64::try_from(*base_epoch).ok()?,
                delta_epoch,
            }))
        }
        _ => None,
    }
}

/// Build the `__ICSAVE <save_unix_secs> <shard_index> <dir> <paced> [base_epoch delta_epoch]` request
/// for one shard (#676). `paced` (arg[4], `1`/`0`) is the #676 persist-read PACE flag: `1` for a
/// background SAVE/BGSAVE (the read is paced by `save-backpressure-percent` to spare datapath
/// bandwidth), `0` for a LATENCY-CRITICAL save (shutdown save-on-exit, upgrade handoff) that must run
/// at full speed. A DELTA save APPENDS `base_epoch` + `delta_epoch` (arg[5]/arg[6]), whose PRESENCE
/// is how [`crate::coordinator::save_shard_local`] recognizes a delta save. `__ICSAVE` is an
/// intra-node fan-out (never persisted, never cross-version), so this arg shape is free to evolve.
fn icsave_request(
    save_unix_secs: u64,
    shard: usize,
    dir: &std::path::Path,
    mode: ironcache_persist::SaveMode,
    paced: bool,
) -> Request {
    let mut args = vec![
        bytes::Bytes::from_static(ICSAVE),
        bytes::Bytes::from(save_unix_secs.to_string()),
        bytes::Bytes::from(shard.to_string()),
        bytes::Bytes::copy_from_slice(dir.to_string_lossy().as_bytes()),
        bytes::Bytes::from_static(if paced { b"1" } else { b"0" }),
    ];
    if let ironcache_persist::SaveMode::Delta {
        base_epoch,
        delta_epoch,
    } = mode
    {
        args.push(bytes::Bytes::from(base_epoch.to_string()));
        args.push(bytes::Bytes::from(delta_epoch.to_string()));
    }
    Request { args }
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
    paced: bool,
) -> Result<(), String> {
    // Record the save OUTCOME so INFO `rdb_last_bgsave_status` is honest (#549): the success path
    // stamps OK inside `record_committed`; every FAILURE arm funnels through this ONE place, so a
    // failed save flips the status to `err` regardless of which arm failed (dir create, a shard
    // error, or the manifest write).
    let outcome = save_all_attempt(persist, inbox, ctx, home, db, save_unix_secs, paced).await;
    if outcome.is_err() {
        persist.record_save_failed();
    }
    outcome
}

/// The inner cross-shard SAVE attempt against the DURABLE `data_dir`: the actual fan-out + manifest
/// commit (records the COMMITTED save on success). [`do_save_all`] wraps it to record a FAILED save
/// on any `Err` (#549). A thin wrapper over the dir-generic [`save_all_to_dir`].
async fn save_all_attempt(
    persist: &Arc<PersistState>,
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    save_unix_secs: u64,
    paced: bool,
) -> Result<(), String> {
    let dir = persist.dir.clone();
    save_all_to_dir(
        persist,
        inbox,
        ctx,
        home,
        db,
        save_unix_secs,
        &dir,
        true,
        paced,
    )
    .await
}

/// The dir-generic cross-shard SAVE: fan an `__ICSAVE` out to EVERY shard (each dumps its OWN
/// partition to `<dir>/dump-shard-<n>.icss` ON ITS OWN thread), collect the manifest entries, and
/// COMMIT by writing `<dir>/dump.manifest` LAST (atomic + fsync'd). `dir` is the DURABLE `data_dir`
/// for a normal SAVE/BGSAVE/periodic save and the TMPFS staging dir for the #390 upgrade handoff --
/// the write mechanics (per-shard atomic file, manifest-last commit, CRC fail-closed) are IDENTICAL
/// on both, so the tmpfs handoff inherits the same torn/foreign safety as the durable path.
///
/// `durable` selects the post-commit accounting: `true` records a full committed DURABLE save
/// ([`PersistState::record_committed`] -- stamps `LASTSAVE`, resets the dirty counter, marks
/// bgsave-ok); `false` records an EPHEMERAL handoff ([`PersistState::record_handoff_committed`] --
/// stamps `LASTSAVE` for the upgrade confirmation but leaves the durable dirty/health accounting
/// untouched, since the tmpfs write is not durable).
///
/// Assemble the committed manifest contents for the decided [`ironcache_persist::SaveMode`] (#676):
/// the base-generation `save_id`, the final base entries, and the final delta chain. PURE (no I/O),
/// so the base/delta bookkeeping + the fail-loud chain-integrity guard are unit-testable.
///
/// - BASE: `save_id` is a FRESH generation (`next_save_id`); the manifest is the shards' base entries
///   with an empty chain. A stray delta reply under a base mode is a bug (`Err`).
/// - DELTA: `save_id` STAYS the base generation (`base_epoch`); CARRY FORWARD the prior base entries
///   (their files are untouched on disk) + the prior delta chain, then append THIS round's deltas. A
///   stray base reply, a missing prior manifest, or ANY delta whose `base_epoch` != the base
///   generation is a fail-loud `Err` -- the last would make the loader's `base_epoch == save_id`
///   check silently DROP the whole chain (data loss), so we refuse to commit it and leave the prior
///   snapshot current.
fn assemble_commit(
    mode: ironcache_persist::SaveMode,
    next_save_id: u64,
    prior: Option<&ironcache_persist::Manifest>,
    base_entries: Vec<ironcache_persist::ShardManifestEntry>,
    delta_entries: Vec<ironcache_persist::DeltaManifestEntry>,
) -> Result<
    (
        u64,
        Vec<ironcache_persist::ShardManifestEntry>,
        Vec<ironcache_persist::DeltaManifestEntry>,
    ),
    String,
> {
    match mode {
        ironcache_persist::SaveMode::Base => {
            if !delta_entries.is_empty() {
                return Err(format!(
                    "base save received {} unexpected delta replies",
                    delta_entries.len()
                ));
            }
            Ok((next_save_id, base_entries, Vec::new()))
        }
        ironcache_persist::SaveMode::Delta { base_epoch, .. } => {
            if !base_entries.is_empty() {
                return Err(format!(
                    "delta save received {} unexpected base replies",
                    base_entries.len()
                ));
            }
            let Some(prior) = prior else {
                return Err("delta save decided with no prior manifest".to_owned());
            };
            let mut all_deltas = prior.deltas.clone();
            all_deltas.extend(delta_entries);
            if let Some(bad) = all_deltas.iter().find(|d| d.base_epoch != base_epoch) {
                return Err(format!(
                    "delta base_epoch {} != base generation {base_epoch}; refusing a manifest that \
                     would silently truncate the delta chain on load",
                    bad.base_epoch
                ));
            }
            Ok((base_epoch, prior.entries.clone(), all_deltas))
        }
    }
}

/// SERIALIZED: the caller must hold the save latch. No `RefCell` borrow is held across any await.
#[allow(clippy::too_many_arguments)]
async fn save_all_to_dir(
    persist: &Arc<PersistState>,
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    save_unix_secs: u64,
    dir: &Path,
    durable: bool,
    paced: bool,
) -> Result<(), String> {
    let n_shards = inbox.len();
    // Ensure the target directory exists (idempotent; a create failure fails the save cleanly).
    if let Err(e) = std::fs::create_dir_all(dir) {
        return Err(format!("cannot create snapshot dir {}: {e}", dir.display()));
    }
    // #676: decide whether THIS whole-snapshot save is a BASE or a DELTA, purely from the PRIOR
    // committed manifest. Deltas apply ONLY to DURABLE saves (an ephemeral handoff is always a
    // self-contained base -- simpler + faster for the receiver to reload), ONLY when `snapshot_deltas`
    // is configured, and NEVER when a base is owed (`needs_base`: post-boot/handoff, before dirty
    // tracking is re-armed with the base as the "since" point). `decide_snapshot_mode` additionally
    // forces a base on the first save (no prior) and on compaction (the chain reached the cap).
    let prior = if durable {
        ironcache_persist::read_manifest(dir)
    } else {
        None
    };
    let deltas_enabled = durable && ctx.boot.snapshot_deltas && !persist.needs_base();
    let mode = ironcache_persist::decide_snapshot_mode(
        prior.as_ref(),
        deltas_enabled,
        ironcache_persist::MAX_DELTAS_PER_BASE,
    );

    // One `__ICSAVE` sub-request per shard, each carrying its own shard index + the whole-save mode
    // (a delta appends base_epoch/delta_epoch so every shard appends the SAME chain position).
    let subreqs: Vec<(usize, Request)> = (0..n_shards)
        .map(|shard| {
            (
                shard,
                icsave_request(save_unix_secs, shard, dir, mode, paced),
            )
        })
        .collect();
    // Fan out via the SAVE-specific fan-out (#571): the home shard's own dump runs INLINE on the
    // YIELDING `run_local_save` (awaits between snapshot chunks), which a synchronous `fan_out_split`
    // local closure cannot express; every other shard dumps off its drain loop (also yielding).
    let replies = coordinator::fan_out_save(ctx, inbox, home, db, subreqs).await;

    // The epoch cut in EVERY shard's `__ICSAVE` DRAINED its per-shard dirty set (unconditionally when
    // `snapshot_deltas` is on), advancing the delta "since" point PAST the drained writes. A base is
    // now OWED until THIS save DURABLY commits: if it fails, tears mid-fan-out, or is only an ephemeral
    // handoff, the drained writes live in NO durable snapshot, so the next durable save MUST re-base to
    // recapture them (else a later delta silently drops them and a warm-start resurrects stale state).
    // Set the flag FAIL-SAFE here (the drain already happened); ONLY a committed durable save clears it
    // (below). This covers every error return, a failed/aborted handoff, and a partial fan-out failure.
    if ctx.boot.snapshot_deltas {
        persist.force_needs_base();
    }

    // Collect the per-shard replies; surface the FIRST shard error as a failed save (a partial set of
    // files without a committed manifest is harmless -- the prior manifest stays committed, and load
    // ignores files the manifest does not vouch for). Under a BASE mode every shard replies Base;
    // under a DELTA mode every shard replies Delta.
    let mut base_entries = Vec::with_capacity(replies.len());
    let mut delta_entries: Vec<ironcache_persist::DeltaManifestEntry> = Vec::new();
    for (shard, reply) in replies {
        match decode_save_reply(&reply.value) {
            Some(SaveReply::Base(e)) => base_entries.push(e),
            Some(SaveReply::Delta(d)) => delta_entries.push(d),
            None => {
                let detail = match &reply.value {
                    Value::Error(e) => e.message().to_owned(),
                    _ => "unexpected reply".to_owned(),
                };
                return Err(format!("shard {shard} save failed: {detail}"));
            }
        }
    }

    // Assemble the committed manifest contents (base-generation id + final base entries + final delta
    // chain) for the decided mode, with the fail-loud chain-integrity guard (pure; see below).
    let (save_id, base_final, delta_final) = assemble_commit(
        mode,
        persist.next_save_id(),
        prior.as_ref(),
        base_entries,
        delta_entries,
    )?;

    // COMMIT: write the manifest LAST (the atomic commit point).
    match ironcache_persist::write_manifest_v2(
        dir,
        save_id,
        save_unix_secs,
        base_final,
        delta_final,
    ) {
        Ok(manifest) => {
            if durable {
                match mode {
                    // A durable base bumps the base generation; a durable delta stays on its base
                    // generation (no save_id bump).
                    ironcache_persist::SaveMode::Base => persist.record_committed(save_unix_secs),
                    ironcache_persist::SaveMode::Delta { .. } => {
                        persist.record_committed_delta(save_unix_secs);
                    }
                }
                // A committed DURABLE save (base OR delta) durably persists everything up to the drain,
                // so it becomes the valid "since" point -> clear the base-owed flag (deltas may resume).
                persist.clear_needs_base();
            } else {
                // A handoff commits only to tmpfs (the durable base is NOT advanced), so `needs_base`
                // stays forced (set after the fan-out): the next durable save re-bases.
                persist.record_handoff_committed(save_unix_secs);
            }
            // Reclaim delta files the NEW manifest no longer references (#676 GC): a base compaction
            // orphans the whole prior chain, and each new base generation orphans the previous base's
            // deltas. Runs AFTER the manifest is committed (the sole commit point), so an unreferenced
            // delta is provably dead. Best-effort: the save has already succeeded, so a GC failure is
            // logged, never propagated.
            match ironcache_persist::gc_orphan_deltas(dir, &manifest) {
                Ok(n) if n > 0 => tracing::info!(reclaimed = n, "reclaimed orphan delta files"),
                Ok(_) => {}
                Err(e) => tracing::warn!("orphan delta GC failed (non-fatal): {e}"),
            }
            Ok(())
        }
        Err(e) => Err(format!("manifest write failed: {e}")),
    }
}

/// PERFORM the `ironcache upgrade` HANDOFF save (#390): stage the snapshot on tmpfs (`/dev/shm`,
/// RAM-backed) to shrink the reload window by removing the disk I/O legs, guarded against OOM. Falls
/// back to the durable `data_dir` when tmpfs staging is unavailable or the RAM/tmpfs-headroom guard
/// declines. The durable `data_dir` snapshot is UNTOUCHED on the tmpfs path (the crash-recovery
/// source stays intact); the new process's load-on-boot prefers this handoff when present + fresh.
///
/// SERIALIZED like [`do_save_all`]: the caller must hold the save latch. Records a FAILED save on any
/// `Err` (#549). Returns `Ok(())` on a committed handoff (tmpfs OR the data_dir fallback).
pub async fn do_handoff_save_all(
    persist: &Arc<PersistState>,
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    save_unix_secs: u64,
) -> Result<(), String> {
    match persist.resolve_handoff_target(ctx) {
        crate::handoff::HandoffTarget::Tmpfs(dir) => {
            tracing::info!(
                dir = %dir.display(),
                "ironcache upgrade: staging the handoff snapshot on tmpfs (RAM-backed, no disk I/O; \
                 the durable data_dir snapshot is left untouched)"
            );
            // Truncate any stale staging dir left by a crashed prior handoff so this handoff starts
            // from a clean tmpfs dir (a leftover file the new manifest does not reference is ignored
            // on load, but clearing it keeps /dev/shm tidy and bounds the RAM footprint).
            let _ = std::fs::remove_dir_all(&dir);
            // #676: the handoff save is NOT paced (last `false`). Its whole purpose is to MINIMIZE the
            // upgrade cutover window; pacing would stretch it ~100/pct, so it runs at full speed.
            let outcome = save_all_to_dir(
                persist,
                inbox,
                ctx,
                home,
                db,
                save_unix_secs,
                &dir,
                false,
                false,
            )
            .await;
            if outcome.is_err() {
                persist.record_save_failed();
            }
            outcome
        }
        crate::handoff::HandoffTarget::DataDir => {
            tracing::info!(
                dir = %persist.dir.display(),
                "ironcache upgrade: staging the handoff snapshot on the durable data_dir (tmpfs \
                 unavailable, non-Linux, or the RAM-headroom guard declined tmpfs)"
            );
            // The durable fallback IS a normal committed data_dir save, but still a HANDOFF: not
            // paced (`false`), so the cutover window is not stretched by `save-backpressure-percent`.
            do_save_all(persist, inbox, ctx, home, db, save_unix_secs, false).await
        }
    }
}

/// Resolve, ONCE, which snapshot dir THIS boot loads from and whether a tmpfs handoff must be cleaned
/// up afterward (#390). Prefers the tmpfs handoff staging dir when its manifest reads CLEANLY
/// (`read_manifest` enforces magic/version/CRC -- #530 fail-closed on a foreign/torn handoff) AND it
/// is at least as fresh as the durable `data_dir` manifest (a stale leftover from a crashed
/// upgrade must never shadow a newer durable save). Otherwise the durable `data_dir`, and no cleanup.
///
/// Returns `(load_dir, cleanup_dir)`: `cleanup_dir` is `Some(staging)` ONLY when the tmpfs handoff
/// won, so a data_dir boot never removes a staging dir it did not load from (which could be another
/// instance's, given the node-local well-known path).
fn resolve_boot_load_dir(
    data_dir: &Path,
    handoff_base: Option<&Path>,
) -> (PathBuf, Option<PathBuf>) {
    let Some(base) = handoff_base else {
        return (data_dir.to_path_buf(), None);
    };
    let staging = crate::handoff::handoff_staging_dir(base);
    let handoff_time = ironcache_persist::read_manifest(&staging).map(|m| m.save_unix_secs);
    let durable_time = ironcache_persist::read_manifest(data_dir).map(|m| m.save_unix_secs);
    match (handoff_time, durable_time) {
        // A valid handoff manifest at least as fresh as the durable one -> load from tmpfs + clean up
        // (the just-completed-upgrade case, and the crashed-mid-upgrade case where tmpfs is newer).
        (Some(h), Some(d)) if h >= d => (staging.clone(), Some(staging)),
        // A valid handoff with no durable snapshot at all -> load from tmpfs + clean up.
        (Some(_), None) => (staging.clone(), Some(staging)),
        // No handoff / a stale-or-foreign handoff -> the durable data_dir, no cleanup.
        _ => (data_dir.to_path_buf(), None),
    }
}

impl PersistState {
    /// Choose the upgrade-handoff SAVE target (#390): tmpfs when the RAM-headroom guard admits it,
    /// else the durable `data_dir`. The size estimate is a CONSERVATIVE upper bound on the on-tmpfs
    /// snapshot -- the live allocator memory (`>=` the logical dataset), falling back to `maxmemory`;
    /// an unknown size (both zero) never gambles on tmpfs. The available budget is `MemAvailable`
    /// (which already EXCLUDES the resident live dataset), so `snapshot + headroom <= MemAvailable`
    /// bounds the incremental tmpfs cost against OOM. (A too-small tmpfs mount that would ENOSPC on
    /// write is not pre-checked here -- it degrades cleanly: the tmpfs save errors and the upgrade
    /// CLI falls back to a plain durable `SAVE`.)
    fn resolve_handoff_target(&self, ctx: &ServerContext) -> crate::handoff::HandoffTarget {
        use crate::handoff;
        let Some(base) = self.handoff_base.as_deref() else {
            return handoff::HandoffTarget::DataDir;
        };
        // A FRESH synchronous live-allocator read (jemalloc epoch + stats.allocated), NOT the
        // periodically-published gauge -- so the estimate is valid even on a just-booted node before
        // the first expiry tick. The whole-process allocation is `>=` the logical dataset (a
        // conservative overestimate for the OOM guard); `maxmemory` is a secondary ceiling.
        let live_mem = ironcache_store::process_memory().0;
        let estimate = live_mem.max(ctx.maxmemory());
        if estimate == 0 {
            // No size signal at all -> do not gamble on tmpfs (the safe data_dir path).
            return handoff::HandoffTarget::DataDir;
        }
        handoff::handoff_target(
            estimate,
            handoff::available_ram_bytes(),
            handoff::headroom_for(estimate),
            base,
        )
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
        // #676: the EXIT save is NOT paced (`paced=false`). It runs against this 5s bound and there is
        // no live datapath left to protect, so a low `save-backpressure-percent` must NOT stretch it
        // past the budget (that would abandon the save-on-exit and lose writes since the last commit).
        result = do_save_all(persist, inbox, ctx, home, db, save_unix_secs, false) => result,
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

    #[test]
    fn save_reply_base_and_delta_round_trip() {
        // A base reply encodes + decodes back to the same ShardManifestEntry (file recomputed).
        let base = SaveReply::Base(ironcache_persist::ShardManifestEntry {
            shard: 2,
            file: ironcache_persist::shard_file_name(2),
            keys: 100,
            crc: 0xDEAD,
        });
        assert_eq!(decode_save_reply(&encode_save_reply(&base)), Some(base));
        // A delta reply encodes + decodes back to the same DeltaManifestEntry (file recomputed from
        // shard + delta_epoch).
        let delta = SaveReply::Delta(ironcache_persist::DeltaManifestEntry {
            shard: 2,
            file: ironcache_persist::delta_file_name(2, 3),
            puts: 5,
            tombstones: 2,
            crc: 0xBEEF,
            base_epoch: 9,
            delta_epoch: 3,
        });
        assert_eq!(decode_save_reply(&encode_save_reply(&delta)), Some(delta));
        // A shard error / an untagged / a wrong-arity reply -> None (a failed save).
        assert!(decode_save_reply(&Value::Integer(1)).is_none());
        assert!(
            decode_save_reply(&Value::Array(Some(vec![
                Value::Integer(9),
                Value::Integer(0)
            ])))
            .is_none(),
            "an unknown tag decodes to None"
        );
    }

    fn base_entry(shard: u32) -> ironcache_persist::ShardManifestEntry {
        ironcache_persist::ShardManifestEntry {
            shard,
            file: ironcache_persist::shard_file_name(shard),
            keys: 10,
            crc: 1,
        }
    }

    fn delta_entry(
        shard: u32,
        base_epoch: u64,
        delta_epoch: u64,
    ) -> ironcache_persist::DeltaManifestEntry {
        ironcache_persist::DeltaManifestEntry {
            shard,
            file: ironcache_persist::delta_file_name(shard, delta_epoch),
            puts: 1,
            tombstones: 0,
            crc: 1,
            base_epoch,
            delta_epoch,
        }
    }

    #[test]
    fn assemble_commit_base_uses_fresh_id_and_empty_chain() {
        let (save_id, base, deltas) = assemble_commit(
            ironcache_persist::SaveMode::Base,
            7,
            None,
            vec![base_entry(0), base_entry(1)],
            Vec::new(),
        )
        .expect("base commit");
        assert_eq!(save_id, 7, "a base takes a fresh generation id");
        assert_eq!(base.len(), 2);
        assert!(deltas.is_empty(), "a base has an empty chain");
    }

    #[test]
    fn assemble_commit_delta_carries_forward_prior_base_and_chain() {
        // A prior base generation 3 with one delta already on it.
        let prior = ironcache_persist::Manifest {
            version: ironcache_persist::MANIFEST_VERSION_DELTA,
            shards: 2,
            save_id: 3,
            save_unix_secs: 100,
            entries: vec![base_entry(0), base_entry(1)],
            deltas: vec![delta_entry(0, 3, 1), delta_entry(1, 3, 1)],
        };
        // This round appends delta_epoch 2 for each shard (the shards reply Delta -> no base entries).
        let (save_id, base, deltas) = assemble_commit(
            ironcache_persist::SaveMode::Delta {
                base_epoch: 3,
                delta_epoch: 2,
            },
            99, // next_save_id is IGNORED for a delta (stays on base_epoch)
            Some(&prior),
            Vec::new(),
            vec![delta_entry(0, 3, 2), delta_entry(1, 3, 2)],
        )
        .expect("delta commit");
        assert_eq!(save_id, 3, "a delta STAYS on its base generation");
        assert_eq!(base.len(), 2, "the prior base entries are carried forward");
        assert_eq!(deltas.len(), 4, "prior chain (2) + this round (2)");
    }

    #[test]
    fn assemble_commit_delta_guards_reject_corruption() {
        let prior = ironcache_persist::Manifest {
            version: ironcache_persist::MANIFEST_VERSION_BASE,
            shards: 1,
            save_id: 5,
            save_unix_secs: 100,
            entries: vec![base_entry(0)],
            deltas: Vec::new(),
        };
        // A delta reply carrying a base reply is a bug.
        assert!(
            assemble_commit(
                ironcache_persist::SaveMode::Delta {
                    base_epoch: 5,
                    delta_epoch: 1
                },
                6,
                Some(&prior),
                vec![base_entry(0)],
                Vec::new(),
            )
            .is_err(),
            "a base reply under a delta mode is rejected"
        );
        // A delta whose base_epoch != the base generation would be silently dropped by the loader.
        assert!(
            assemble_commit(
                ironcache_persist::SaveMode::Delta {
                    base_epoch: 5,
                    delta_epoch: 1
                },
                6,
                Some(&prior),
                Vec::new(),
                vec![delta_entry(0, 999, 1)],
            )
            .is_err(),
            "a mismatched base_epoch fails loud instead of truncating the chain"
        );
        // A delta with no prior manifest is impossible-by-decision, rejected defensively.
        assert!(
            assemble_commit(
                ironcache_persist::SaveMode::Delta {
                    base_epoch: 5,
                    delta_epoch: 1
                },
                6,
                None,
                Vec::new(),
                vec![delta_entry(0, 5, 1)],
            )
            .is_err(),
            "a delta with no prior manifest is rejected"
        );
        // A base mode that somehow got delta replies is a bug.
        assert!(
            assemble_commit(
                ironcache_persist::SaveMode::Base,
                6,
                None,
                Vec::new(),
                vec![delta_entry(0, 5, 1)],
            )
            .is_err(),
            "a delta reply under a base mode is rejected"
        );
    }

    /// A bare `PersistState` for latch tests (no real data dir / save fan-out needed).
    fn latch_state() -> Arc<PersistState> {
        Arc::new(PersistState {
            dir: PathBuf::from("/nonexistent-test-dir"),
            handoff_base: None,
            boot_load_dir: PathBuf::from("/nonexistent-test-dir"),
            handoff_cleanup_dir: None,
            shards_pending_load: AtomicUsize::new(1),
            stats: Arc::new(ironcache_observe::PersistRuntime::new()),
            save_id: AtomicU64::new(0),
            saving: AtomicBool::new(false),
            needs_base: AtomicBool::new(true),
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
            handoff_base: None,
            boot_load_dir: dir.clone(),
            handoff_cleanup_dir: None,
            shards_pending_load: AtomicUsize::new(1),
            stats: Arc::new(ironcache_observe::PersistRuntime::new()),
            save_id: AtomicU64::new(0),
            saving: AtomicBool::new(false),
            needs_base: AtomicBool::new(true),
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
