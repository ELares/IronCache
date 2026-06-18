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
//! memory-neutral, borrow-releasing `snapshot_chunk` pull), writes `<data_dir>/dump-shard-<n>.icss`
//! ATOMICALLY (tmp -> fsync -> rename), and returns its manifest entry. The home core then COMMITS
//! the snapshot by writing the manifest LAST (atomic + fsync'd), so a crash mid-save leaves the
//! prior good snapshot.
//!
//! ## `SAVE` vs `BGSAVE`
//!
//! - `SAVE` BLOCKS the issuing connection until every shard has written + the manifest is committed
//!   (Redis parity), then replies `+OK`. The per-shard dump still uses the forkless
//!   borrow-releasing `snapshot_chunk`, so it never double-memories the keyspace.
//! - `BGSAVE` kicks the SAME save off the request path (spawned on the home shard's executor) and
//!   replies `+Background saving started` immediately. The dump runs on the shards' executors via
//!   the coordinator, yielding between chunks, so it does not block any shard's hot path materially.
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
    /// The periodic save interval in seconds (`0` = the periodic policy is disabled; only explicit
    /// SAVE/BGSAVE persist). From `Config::save_interval_secs`.
    pub interval_secs: u64,
    /// The minimum dirty writes the periodic policy requires before it fires on a tick (`0` =
    /// fire unconditionally on each enabled tick). From `Config::save_min_changes`.
    pub min_changes: u64,
    /// The unix time (SECONDS) of the LAST successful save, what `LASTSAVE` returns. `0` until the
    /// first successful save in this process (boot does NOT set it; Redis `lastsave` starts at the
    /// process start time, but `0`-until-first-save is a faithful-enough integer for #58). Updated
    /// (relaxed) on every committed save.
    pub last_save_unix_secs: AtomicU64,
    /// A monotone SAVE ID, incremented per committed save (informational; recorded in the manifest).
    pub save_id: AtomicU64,
    /// The DIRTY counter: keyspace writes since the last save. Bumped (relaxed) by the serve loop
    /// AFTER a successful write command, ONLY when persistence is enabled (this `Arc` is `Some`).
    /// The periodic policy reads it to decide whether a tick should save; a committed save resets
    /// it. This is the single relaxed atomic the prompt allows; the store hot path is untouched
    /// (the bump is in the serve layer, gated on `Some`, NOT in the store primitives).
    pub dirty: AtomicU64,
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
            interval_secs: config.save_interval_secs,
            min_changes: config.save_min_changes,
            last_save_unix_secs: AtomicU64::new(0),
            save_id: AtomicU64::new(0),
            dirty: AtomicU64::new(0),
            saving: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    /// The unix-seconds of the last successful save (the `LASTSAVE` reply), relaxed read.
    #[must_use]
    pub fn last_save(&self) -> u64 {
        self.last_save_unix_secs.load(Ordering::Relaxed)
    }

    /// Bump the dirty write counter (relaxed). Called by the serve loop after a successful write
    /// command when persistence is enabled. The single allowed hot-adjacent atomic increment.
    pub fn note_write(&self) {
        self.dirty.fetch_add(1, Ordering::Relaxed);
    }

    /// Try to ACQUIRE the save latch; returns `true` if this caller won it (and must
    /// [`Self::release_save`] when done), `false` if a save is already in progress.
    pub fn try_begin_save(&self) -> bool {
        self.saving
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Release the save latch after a save completes (success or failure).
    pub fn release_save(&self) {
        self.saving.store(false, Ordering::Release);
    }

    /// Record a COMMITTED save: stamp the last-save time, bump the save id, and reset the dirty
    /// counter to 0 (relaxed). Called on the home core after the manifest is written.
    pub fn record_committed(&self, save_unix_secs: u64) {
        self.last_save_unix_secs
            .store(save_unix_secs, Ordering::Relaxed);
        self.save_id.fetch_add(1, Ordering::Relaxed);
        self.dirty.store(0, Ordering::Relaxed);
    }

    /// The next monotone save id (the value the next save will record; read for the manifest).
    #[must_use]
    pub fn next_save_id(&self) -> u64 {
        self.save_id.load(Ordering::Relaxed).saturating_add(1)
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
/// SERIALIZED: the caller must hold the save latch ([`PersistState::try_begin_save`]) so two saves
/// never race on the same files + manifest.
///
/// The fan-out reuses [`coordinator::fan_out_split`] (a DIFFERENT sub-request per shard -- each
/// carries its own shard index for its file name): the home shard's `__ICSAVE` runs LOCALLY +
/// synchronously ([`coordinator::run_local_save`]), every other shard via its drain loop. No
/// `RefCell` borrow is held across the awaits (the per-shard `save_shard_local` is synchronous).
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
    let replies = coordinator::fan_out_split(inbox, home, db, subreqs, |req| {
        coordinator::run_local_save(ctx, req)
    })
    .await;

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

/// LOAD a committed snapshot into the per-shard stores at boot (#58 load-on-boot). Returns the
/// total keys loaded, or `0` (loads nothing) when there is no loadable snapshot (no manifest / a
/// torn manifest). Each manifest entry's file is loaded into the store for `entry.shard %
/// stores.len()`, so a snapshot taken with a different shard count still reconstructs the full
/// keyspace (the store re-hashes each key into its owning db; SCAN order is recomputed from the key
/// bytes). A torn / CRC-bad shard file is skipped (that shard loads empty), never corrupt-loaded.
///
/// `now` is the boot wall-clock (the env Clock seam): an already-expired key is dropped on load.
pub fn load_on_boot(
    persist: &Arc<PersistState>,
    stores: &mut [&mut crate::serve::ShardStoreImpl],
    now: ironcache_storage::UnixMillis,
) -> u64 {
    ironcache_persist::load_all(stores, &persist.dir, now)
}
