// SPDX-License-Identifier: MIT OR Apache-2.0
//! Durable ON-DISK SNAPSHOT PERSISTENCE for IronCache (#58 persistence umbrella, #62
//! warm-restart, #63 durable log).
//!
//! IronCache is an in-memory cache: by default it loses all data on restart (the ephemeral
//! durability stance, ROADMAP #59). This crate adds OPT-IN durable persistence: a point-in-time
//! on-disk SNAPSHOT of the whole keyspace, atomic + crash-safe, loaded on boot. It is the engine
//! half of `SAVE` / `BGSAVE` / `LASTSAVE`; the serve layer wires the commands and the periodic
//! save policy on top.
//!
//! ## What it reuses (so this is well-scoped, not a new engine)
//!
//! - The FORKLESS, memory-neutral per-shard dump iterator
//!   [`ironcache_store::ShardStore::snapshot_chunk`] (HA-5b #60): a resumable, constant-memory
//!   SCAN dump that yields each live key as an owned [`ironcache_store::KvObj`]. [`dump_shard_keyspace`]
//!   drives it chunk by chunk, so a save never double-memories the keyspace (its transient memory is
//!   O(`DUMP_CHUNK`), not the whole keyspace). The serve path drives it through
//!   [`ShardDumpBuilder`], re-acquiring the store borrow PER CHUNK and `yield`ing the shard between
//!   chunks (#571), so `SAVE`/`BGSAVE` no longer BLOCK the dumping shard for its whole keyspace dump
//!   -- the shard services queued writes DURING the dump, a bounded/predictable save tail. (The sync
//!   [`crate::dump_shard_keyspace`] used by this crate's tests + [`crate::save_shard_to_dir`] still
//!   holds the borrow across the whole walk, so it stays per-shard point-in-time.)
//! - The KvObj WIRE CODEC ([`ironcache_repl::encode_kvobj`] / [`ironcache_repl::decode_kvobj`]):
//!   the SAME faithful, one-way-ratchet-preserving encoding the HA-7b replication full-sync uses
//!   is reused VERBATIM as the on-disk record value, so the snapshot file and the replication
//!   stream share one audited codec. (Each record also carries the logical `db`, which the
//!   `KvObj` does not, see [`format::put_record`].)
//! - [`ironcache_store::ShardStore::insert_object`] for load-on-boot: each decoded `KvObj` is
//!   replayed through the store's insert funnel (accounting / eviction / observer fire as for any
//!   insert), so a loaded keyspace is indistinguishable from one built by client writes.
//!
//! ## Crash-safety (see [`format`])
//!
//! Every shard file is written atomically (tmp -> fsync -> rename) and the COMMIT MANIFEST is
//! written + fsync'd LAST, so a crash mid-save leaves the PRIOR good snapshot. A torn file is
//! caught by its CRC and treated as no-snapshot (start empty), never as corrupt-load.
//!
//! ## Snapshot consistency: FUZZY (approximate warm-start restore point), by DECISION (#571)
//!
//! Each shard dumps its OWN partition on its OWN thread (no global lock, no fork-COW). The serve
//! path drives that dump chunk-by-chunk and YIELDS the shard between chunks (#571) so queued writes
//! run DURING the dump. Two consequences, both a DELIBERATE choice for a CACHE (not an accident):
//!
//! - WITHIN a shard the snapshot is NO LONGER a clean point-in-time: because writes interleave
//!   between chunks, an early chunk may hold a key's pre-write value while a late chunk holds another
//!   key's post-write value. The walk is `scan_hash`-cursor stable (SCAN semantics), so a key present
//!   for the WHOLE dump is captured at least once; a key created/deleted mid-dump may or may not
//!   appear.
//! - ACROSS shards it is also FUZZY: different shards dump at slightly different instants, so a write
//!   landing on shard A after A dumped but before B dumped is in B's file and not A's.
//!
//! We ACCEPT this: IronCache is a cache, so an APPROXIMATE warm-start restore point (bounded save
//! tail, writes never stalled behind a dump) is worth far more than a strict global point-in-time,
//! and no cross-key transactional durability is promised. A strict point-in-time snapshot would need
//! versioning / copy-on-write (the forkless epoch-cut serializer in SNAPSHOT.md, a much larger
//! change) -- deliberately OUT of scope here. The CRASH-SAFETY invariant is INDEPENDENT of this
//! fuzziness and STILL HOLDS: the manifest is written LAST (below), so a torn/partial dump is never
//! loaded -- a restart loads a fully-committed (if fuzzy) snapshot or the prior one, never a
//! half-written file.
//!
//! ## Re-shard on load (correct across a shard-count change)
//!
//! The per-shard files were partitioned by the shard count AT SAVE TIME. The loading node may have
//! a DIFFERENT shard count, so load does NOT blindly replay file-i into shard-i (that would lose
//! keys when the count shrinks and misroute every GET when it grows). Instead each shard reads
//! EVERY manifest file and keeps only the keys it OWNS under the current count, using the router's
//! own `owner_shard` hash (passed in as `route`). See [`load_shard_resharded`] / [`load_all`].
//!
//! ## Default-off (the hot path + boot path are byte-unchanged)
//!
//! Persistence is engaged ONLY when the serve layer calls these functions, which it does ONLY
//! when a `data_dir` is configured. With no `data_dir` the serve layer never calls in here, no
//! files are written, boot starts empty, and the store's hot write path is untouched (this crate
//! adds NO per-write cost: it observes the keyspace only through the read-only `snapshot_chunk`).
//!
//! ## Determinism (ADR-0003)
//!
//! This crate reads NO clock and NO RNG: `now` (the lazy-expiry basis the dump skips dead keys
//! at, and the load drops already-expired keys at) and `save_unix_secs` (the manifest timestamp
//! `LASTSAVE` reports) are passed in by the caller, sourced from the `ironcache-env` Clock seam
//! at the serve layer.

// No unsafe anywhere: this crate is pure file I/O + the reused safe codec/store APIs.
#![forbid(unsafe_code)]

pub mod delta;
pub mod format;

pub use format::{
    DeltaManifestEntry, MANIFEST_VERSION_BASE, MANIFEST_VERSION_DELTA, Manifest,
    ShardManifestEntry, SnapshotLoadError, crc32, delta_file_name, manifest_path,
    parse_delta_file_name, shard_file_name, shard_path,
};

use std::io;
use std::path::Path;

use ironcache_storage::{AccountingHook, EvictionHook, Store, UnixMillis};
use ironcache_store::{Entry, FrozenSlot, ShardStore, SnapshotCursor, SnapshotEntry};

/// The number of keys [`dump_shard_keyspace`] EXAMINES per
/// [`ironcache_store::ShardStore::snapshot_chunk`] call. Bounds the per-chunk owned `Vec` (and
/// its per-entry `KvObj` clones), so the dump's transient memory is O(`DUMP_CHUNK`), NEVER a
/// full-keyspace materialization (the forkless, memory-neutral property HA-5b provides). 512 is a
/// balance: large enough to amortize the per-chunk borrow/sort overhead, small enough to bound the
/// transient buffer.
///
/// The chunked iterator RELEASES the store borrow between chunks, and the yielding save path
/// EXPLOITS that (#571): [`ShardDumpBuilder`] lets the coordinator re-acquire the store borrow
/// PER CHUNK and `yield` the shard between chunks, so a `SAVE`/`BGSAVE` no longer monopolizes the
/// serving shard for its whole keyspace dump (a bounded, predictable save tail instead of a
/// full-keyspace block). The chunking bounds memory regardless of whether the caller yields.
pub const DUMP_CHUNK: usize = 512;

/// The result of dumping one shard's keyspace to a byte buffer ([`dump_shard_keyspace`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardDump {
    /// The complete shard snapshot file bytes (header + db-tagged kvcodec records), ready to be
    /// written atomically with [`format::write_file_atomic`].
    pub bytes: Vec<u8>,
    /// The number of live keys recorded (across all of this shard's databases).
    pub keys: u64,
    /// The CRC-32 of the file's RECORD BODY (the bytes after the header), recorded in the
    /// manifest and re-validated on load to detect a torn file.
    pub crc: u32,
}

/// DUMP one shard's keyspace to an in-memory snapshot file buffer, FORKLESS and memory-neutrally
/// (HA-5b #60): drive [`ironcache_store::ShardStore::snapshot_chunk`] chunk by chunk over the
/// shard's whole keyspace, encode each yielded [`ironcache_store::KvObj`] with the reused
/// [`ironcache_repl::encode_kvobj`] codec, and append it as a db-tagged record. The returned
/// [`ShardDump`] carries the file bytes (header + records), the live key count, and the body CRC
/// for the manifest.
///
/// `now` is the lazy-expiry basis: `snapshot_chunk` SKIPS a logically-dead key (so the snapshot
/// never persists an already-expired key), exactly as SCAN does.
///
/// ## Forkless + memory-neutral (this SYNC form is per-shard-consistent; the YIELDING form is not)
///
/// The chunked pull bounds the transient memory to O([`DUMP_CHUNK`]) (the per-chunk `Vec` + its
/// `KvObj` clones), never the whole keyspace, and `snapshot_chunk` RELEASES the store borrow
/// between chunks. The MEMORY property holds unconditionally: a `BGSAVE` or `SAVE` never doubles the
/// shard's memory.
///
/// This function holds the `&store` SHARED borrow across the WHOLE walk, so it is
/// PER-SHARD-CONSISTENT (a single point-in-time view of the shard) but BLOCKS the shard for the
/// full dump. It is retained for the persist crate's own round-trip tests + [`save_shard_to_dir`].
/// The SERVE path instead drives the dump chunk-by-chunk through [`ShardDumpBuilder`], re-acquiring
/// the store borrow per chunk and `yield`ing between chunks (#571) so the shard services queued
/// writes DURING the dump; that yielding form is NOT per-shard point-in-time (see
/// [`ShardDumpBuilder`] and the module-level consistency note), which is the accepted warm-start
/// tradeoff for a cache.
///
/// `&S` is a SHARED borrow: the dump never mutates the store (it does not even reap the
/// lazily-expired keys it skips, unlike a command-path read), so it is purely an observer.
#[must_use]
pub fn dump_shard_keyspace<E: EvictionHook, A: AccountingHook>(
    store: &ShardStore<E, A>,
    shard: u32,
    now: UnixMillis,
) -> ShardDump {
    let mut builder = ShardDumpBuilder::new();
    let databases = store.databases();

    let mut cursor = SnapshotCursor::START;
    while !cursor.is_done(databases) {
        let (chunk, next) = store.snapshot_chunk(cursor, DUMP_CHUNK, now);
        builder.push_chunk(&chunk);
        cursor = next;
    }
    builder.finish(shard)
}

/// DUMP a shard's FROZEN slot tables to an in-memory snapshot file buffer, the #576
/// per-slot Arc-COW off-thread SAVE path. The shard called
/// [`ironcache_store::ShardStore::begin_save`] to FREEZE its keyspace into `frozen`
/// ([`FrozenSlot`]s, `Arc` clones of its non-empty slot tables) in O(slots) with NO O(N)
/// serving-side copy; this runs ENTIRELY on the dedicated persist thread, iterating each
/// frozen slot's entries directly and encoding them ([`ShardDumpBuilder::push_entry`] ->
/// the `encode_kvobj` codec + CRC). The returned [`ShardDump`] carries the file bytes, the
/// live key count, and the body CRC for the manifest.
///
/// The dump reflects the store AS OF the freeze: because a datapath write COW-copies a still
/// frozen slot before mutating it (`ShardStore::slot_table_mut`), the entries `frozen` holds
/// are IMMUTABLE for this call, so the dump is a per-shard POINT-IN-TIME view (stronger than
/// the pre-#576 chunked walk) with NO risk of reading a torn / concurrently-mutated pointee.
///
/// `now` is the lazy-expiry basis (the shard's clock, ADR-0003): a logically-dead entry is
/// SKIPPED, exactly as `snapshot_chunk` / SCAN do, so the snapshot never persists an
/// already-expired key.
#[must_use]
pub fn dump_frozen_slots(frozen: &[FrozenSlot], shard: u32, now: UnixMillis) -> ShardDump {
    let mut builder = ShardDumpBuilder::new();
    for slot in frozen {
        let db = slot.db();
        for entry in slot.entries() {
            if entry.is_expired(now) {
                continue; // never persist a logically-dead key (matches snapshot_chunk / SCAN).
            }
            builder.push_entry(db, entry);
        }
    }
    builder.finish(shard)
}

/// Build a DELTA file from the frozen snapshot + the keys DIRTIED since the base (#676 Phase 1b): for
/// each dirty `(db, key)`, look it up in the frozen slots via [`FrozenSlot::find`] -- a PRESENT and
/// live key becomes a delta PUT (its frozen entry re-encoded, byte-identical to the base record for
/// that key), an ABSENT or EXPIRED one a TOMBSTONE (warm-start must remove it so it does not
/// resurrect from the base). It touches ONLY the dirty keys, never the whole keyspace -- that is the
/// point of #676 (the persist-thread READ shrinks to the dirty fraction, the lever that moves the
/// during-snapshot p99.9 bandwidth floor).
///
/// The keys are sorted for a DETERMINISTIC on-disk record order (ADR-0003; the taken dirty set is
/// unordered). `base_epoch` / `delta_epoch` are stamped into the delta header (the manifest chain
/// records the same, so the loader binds the delta to its base + orders the chain).
///
/// PERF NOTE: the lookup filters the flat frozen slice by `db` then probes each of that db's frozen
/// slots, so it is O(non-empty-slots-per-db) hash-probes per dirty key (most are immediate misses).
/// For a small dirty set this is cheap and runs OFF the serving core (the persist thread); a
/// slot-routed O(1)-per-key variant is a follow-up gated on the c7g tail measurement (measure-first).
#[must_use]
pub fn build_delta_from_frozen(
    frozen: &[FrozenSlot],
    dirty: &[(u32, Box<[u8]>)],
    shard: u32,
    base_epoch: u64,
    delta_epoch: u64,
    now: UnixMillis,
) -> delta::DeltaDump {
    // Deterministic record order: sort the unordered dirty set by (db, key).
    let mut sorted: Vec<&(u32, Box<[u8]>)> = dirty.iter().collect();
    sorted.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut builder = delta::DeltaBuilder::new();
    for (db, key) in sorted {
        let found = frozen
            .iter()
            .filter(|s| s.db() == *db)
            .find_map(|s| s.find(key));
        match found {
            // Present + live at the freeze -> a PUT carrying the frozen entry (the state as of the cut).
            Some(entry) if !entry.is_expired(now) => builder.push_put(*db, entry),
            // Absent, or present-but-expired (logically dead) -> a TOMBSTONE.
            _ => builder.push_tombstone(*db, key),
        }
    }
    builder.finish(shard, base_epoch, delta_epoch)
}

/// INCREMENTAL builder for a shard [`ShardDump`], so the caller can drive
/// [`ShardStore::snapshot_chunk`](ironcache_store::ShardStore::snapshot_chunk) itself and RELEASE
/// the store borrow (and, on the serve path, `yield` the shard) BETWEEN chunks (#571). It is the
/// chunk-at-a-time form of [`dump_shard_keyspace`]: feed each bounded chunk to [`push_chunk`], then
/// [`finish`] to seal the file bytes + the body CRC recorded in the manifest.
///
/// ## Cursor stability across a released borrow (the correctness crux)
///
/// Because the caller releases the store borrow between chunks, a concurrent write CAN mutate the
/// table (insert / delete / `hashbrown` resize, and post-#570 a per-slot resize) between chunks.
/// This is SAFE and gives SCAN semantics because `snapshot_chunk` walks the RESIZE-STABLE
/// `scan_hash` order and its cursor is the `scan_hash` THRESHOLD of the next un-examined key (NOT a
/// table slot index): it rebuilds the sorted order from the CURRENT contents each chunk and resumes
/// at `scan_hash >= cursor`. So a key present for the WHOLE dump (its `scan_hash` is fixed) is
/// emitted AT LEAST ONCE; a key inserted/deleted mid-dump MAY or may not appear (acceptable). This
/// is the SAME iterator (and the same guarantee) the replication full-sync already relies on while
/// it awaits shipping each chunk to a replica -- the yielding save just reuses that proven
/// mutation-tolerant walk.
///
/// [`push_chunk`]: ShardDumpBuilder::push_chunk
/// [`finish`]: ShardDumpBuilder::finish
#[derive(Debug, Default)]
pub struct ShardDumpBuilder {
    /// The db-tagged kvcodec record body accumulated so far (header prepended at `finish`).
    body: Vec<u8>,
    /// The live keys recorded so far (across all of this shard's databases).
    keys: u64,
    /// A REUSED per-entry encode buffer (#676 Phase 0): [`Self::push_entry`] encodes each entry
    /// into this and copies it into `body`, so the persist thread makes no fresh per-entry `Vec`
    /// allocation. `clear()` between entries keeps the one grown allocation.
    scratch: Vec<u8>,
}

impl ShardDumpBuilder {
    /// A fresh, empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// APPEND one bounded [`ShardStore::snapshot_chunk`](ironcache_store::ShardStore::snapshot_chunk)
    /// batch to the running dump body, encoding each yielded [`KvObj`](ironcache_store::KvObj) with
    /// the reused [`ironcache_repl::encode_kvobj`] codec as a db-tagged record. Byte-identical to the
    /// per-chunk work [`dump_shard_keyspace`] does inline, so the sealed file is the same whether the
    /// caller yielded between chunks or not.
    pub fn push_chunk(&mut self, chunk: &[SnapshotEntry]) {
        for (db, _key, kv) in chunk {
            let encoded = ironcache_repl::encode_kvobj(kv);
            format::put_record(&mut self.body, *db, &encoded);
            self.keys += 1;
        }
    }

    /// APPEND one stored [`Entry`] (read directly from a [`FrozenSlot`]) as a db-tagged
    /// record, the #576 off-thread FREEZE path's analogue of [`Self::push_chunk`]. It encodes
    /// the entry with [`ironcache_repl::encode_entry_into`], which for the common string family
    /// reads the frozen blob's key + value bytes IN PLACE (no intermediate
    /// [`ironcache_store::KvObj`] deep-clone), and for collections keeps the bounded per-entry
    /// clone. The record is byte-identical to the `encode_kvobj(to_kvobj)` one `push_chunk`
    /// produces for the same key (asserted by the parity test), so the sealed file and its CRC
    /// are unchanged. Halving the per-entry copy matters because the persist thread's
    /// full-keyspace read is the memory-bandwidth-bound cost behind the during-snapshot tail
    /// (#676). The caller applies the lazy-expiry skip (a logically-dead entry is not pushed).
    pub fn push_entry(&mut self, db: u32, entry: &Entry) {
        self.scratch.clear();
        ironcache_repl::encode_entry_into(&mut self.scratch, entry);
        format::put_record(&mut self.body, db, &self.scratch);
        self.keys += 1;
    }

    /// SEAL the accumulated body into a [`ShardDump`]: prepend the file header for `shard` and
    /// compute the body CRC recorded in the manifest (revalidated on load to detect a torn file).
    #[must_use]
    pub fn finish(self, shard: u32) -> ShardDump {
        let crc = format::crc32(&self.body);
        // Prepend the file header to the record body to form the complete file bytes.
        let mut bytes = Vec::with_capacity(format::SHARD_HEADER_LEN + self.body.len());
        format::put_shard_header(&mut bytes, shard);
        bytes.extend_from_slice(&self.body);
        ShardDump {
            bytes,
            keys: self.keys,
            crc,
        }
    }
}

/// SAVE one shard: dump its keyspace ([`dump_shard_keyspace`]) and write the resulting file
/// ATOMICALLY to `<dir>/dump-shard-<shard>.icss` (tmp -> fsync -> rename). Returns the manifest
/// entry the caller collects to commit the manifest LAST.
///
/// This is the per-shard half of a save; the manifest (the COMMIT POINT) is written by
/// [`write_manifest`] only AFTER every shard's file is on disk, so a crash between the shard
/// writes and the manifest write leaves the PRIOR committed snapshot intact.
///
/// # Errors
///
/// Returns any underlying [`io::Error`] from the atomic file write; the caller treats it as a
/// failed save (the prior committed snapshot stays current).
pub fn save_shard_to_dir<E: EvictionHook, A: AccountingHook>(
    store: &ShardStore<E, A>,
    shard: u32,
    dir: &Path,
    now: UnixMillis,
) -> io::Result<ShardManifestEntry> {
    let dump = dump_shard_keyspace(store, shard, now);
    write_shard_dump(&dump, shard, dir)
}

/// WRITE an already-assembled shard [`ShardDump`] ATOMICALLY to
/// `<dir>/dump-shard-<shard>.icss` (tmp -> fsync -> rename) and return its manifest entry. This is
/// the file-write half of a save, split out of [`save_shard_to_dir`] so the YIELDING save path
/// (#571) can build the dump INCREMENTALLY via [`ShardDumpBuilder`] with the store borrow released
/// between chunks, then hand the sealed bytes here for the one atomic write. The manifest (the
/// COMMIT POINT) is still written LAST by [`write_manifest`], so a crash between this per-shard
/// write and the manifest write leaves the PRIOR committed snapshot intact.
///
/// # Errors
///
/// Returns any underlying [`io::Error`] from the atomic file write; the caller treats it as a
/// failed save (the prior committed snapshot stays current).
pub fn write_shard_dump(
    dump: &ShardDump,
    shard: u32,
    dir: &Path,
) -> io::Result<ShardManifestEntry> {
    let path = format::shard_path(dir, shard);
    format::write_file_atomic(&path, &dump.bytes)?;
    Ok(ShardManifestEntry {
        shard,
        file: format::shard_file_name(shard),
        keys: dump.keys,
        crc: dump.crc,
    })
}

/// WRITE one shard's DELTA file ATOMICALLY to `<dir>/dump-shard-<shard>-delta-<delta_epoch>.icsd`
/// (tmp -> fsync -> rename) and return the [`DeltaManifestEntry`] the caller appends to the manifest
/// chain (#676). The delta `bytes` already carry the self-describing header (shard, base_epoch,
/// delta_epoch); `base_epoch` / `delta_epoch` are passed here only to populate the manifest entry.
/// The per-shard delta writes happen BEFORE the manifest is committed LAST, so a crash between them
/// leaves the prior committed snapshot intact (a leftover delta the not-yet-written manifest does not
/// reference is ignored on load), exactly like [`write_shard_dump`].
///
/// # Errors
///
/// Returns any underlying [`io::Error`] from the atomic file write; the caller treats it as a failed
/// save (the prior committed snapshot stays current).
pub fn write_delta_file(
    dump: &delta::DeltaDump,
    shard: u32,
    base_epoch: u64,
    delta_epoch: u64,
    dir: &Path,
) -> io::Result<format::DeltaManifestEntry> {
    let file = format::delta_file_name(shard, delta_epoch);
    let path = dir.join(&file);
    format::write_file_atomic(&path, &dump.bytes)?;
    Ok(format::DeltaManifestEntry {
        shard,
        file,
        puts: dump.puts,
        tombstones: dump.tombstones,
        crc: dump.crc,
        base_epoch,
        delta_epoch,
    })
}

/// COMMIT a save: write the manifest ATOMICALLY (tmp -> fsync -> rename) as the LAST step, after
/// every shard file is durably on disk. The manifest is the single point that makes the new
/// per-shard files the committed snapshot; a crash before this leaves the prior manifest (and
/// prior files), a crash after leaves the new one. `entries` is the per-shard
/// [`ShardManifestEntry`] set the shards returned (in any order; sorted by shard here).
///
/// # Errors
///
/// Returns any underlying [`io::Error`] from the atomic manifest write.
pub fn write_manifest(
    dir: &Path,
    save_id: u64,
    save_unix_secs: u64,
    entries: Vec<ShardManifestEntry>,
) -> io::Result<Manifest> {
    // A base-only save: no delta chain, so the manifest encodes byte-identically to a pre-delta v1
    // manifest (an older binary loads it unchanged).
    write_manifest_v2(dir, save_id, save_unix_secs, entries, Vec::new())
}

/// WRITE a v2-capable manifest that ties per-shard BASE files AND a per-shard DELTA CHAIN into one
/// committed snapshot (#676). `deltas` empty -> a byte-identical v1 base-only manifest (this is the
/// [`write_manifest`] path). `deltas` non-empty -> a v2 manifest; `save_id` MUST be the base
/// generation the deltas apply onto (each [`DeltaManifestEntry::base_epoch`] equals it). Both lists
/// are sorted deterministically -- base entries by shard, deltas by `(shard, delta_epoch)` -- so the
/// manifest is byte-stable regardless of the order shards reported in, and the delta list is in the
/// loader's authoritative apply order. Written atomically (tmp -> fsync -> rename), the LAST write of
/// a save (the single commit point).
///
/// # Errors
///
/// Returns any underlying [`io::Error`] from the atomic manifest write.
pub fn write_manifest_v2(
    dir: &Path,
    save_id: u64,
    save_unix_secs: u64,
    mut base_entries: Vec<ShardManifestEntry>,
    mut deltas: Vec<format::DeltaManifestEntry>,
) -> io::Result<Manifest> {
    base_entries.sort_by_key(|e| e.shard);
    deltas.sort_by(|a, b| {
        a.shard
            .cmp(&b.shard)
            .then(a.delta_epoch.cmp(&b.delta_epoch))
    });
    #[allow(clippy::cast_possible_truncation)]
    let manifest = Manifest {
        version: if deltas.is_empty() {
            format::MANIFEST_VERSION_BASE
        } else {
            format::MANIFEST_VERSION_DELTA
        },
        shards: base_entries.len() as u32,
        save_id,
        save_unix_secs,
        entries: base_entries,
        deltas,
    };
    let path = format::manifest_path(dir);
    format::write_file_atomic(&path, &manifest.encode())?;
    Ok(manifest)
}

/// RECLAIM orphan delta files (#676 GC): delete every `.icsd` delta file in `dir` that the
/// just-committed `manifest` does NOT reference. Delta files are epoch-unique
/// ([`format::delta_file_name`]) so they ACCUMULATE; a base compaction ([`SaveMode::Base`]) resets
/// the manifest to base-only, orphaning the whole prior chain, and a new base generation orphans the
/// previous base's deltas -- without this sweep those files live on disk forever.
///
/// MUST be called only AFTER `manifest` is durably committed (it is [`write_manifest_v2`]'s return,
/// written LAST as the sole commit point). That ordering is what makes this safe: the loader reads
/// ONLY files the committed manifest names, so any `.icsd` the manifest omits is provably dead, and a
/// crash mid-sweep merely leaves orphans for the next save's sweep (never loses a referenced file).
/// Only files that round-trip through [`format::parse_delta_file_name`] are ever considered, so a
/// base `.icss`, the manifest, or any foreign file is never touched. Best-effort per file: a removal
/// error (e.g. a concurrent unlink) is skipped, not propagated, so GC never fails an already-committed
/// save. Returns the number of files reclaimed.
///
/// # Errors
///
/// Returns an [`io::Error`] only if the directory itself cannot be READ (`read_dir`); per-file
/// removal errors are swallowed.
pub fn gc_orphan_deltas(dir: &Path, manifest: &Manifest) -> io::Result<usize> {
    // The keep-set: the live delta chain the committed manifest references, by file name.
    let keep: std::collections::HashSet<&str> =
        manifest.deltas.iter().map(|d| d.file.as_str()).collect();
    let mut removed = 0usize;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // Consider ONLY files this crate wrote as deltas (strict inverse of `delta_file_name`); a
        // base file / manifest / foreign name never parses and is left alone.
        if format::parse_delta_file_name(name).is_none() {
            continue;
        }
        if keep.contains(name) {
            continue; // still referenced by the live chain -> keep.
        }
        // Unreferenced delta => dead. Best-effort remove; ignore a race/ENOENT.
        if std::fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    Ok(removed)
}

/// The maximum number of DELTA rounds chained onto one base before a save COMPACTS to a fresh base
/// (#676 whole-snapshot). Bounds warm-start replay ([`fold_deltas`] over the chain) and on-disk delta
/// count. A "round" appends one delta per shard, so this counts rounds (the max `delta_epoch`), not
/// files.
pub const MAX_DELTAS_PER_BASE: u32 = 8;

/// The mode of the NEXT whole-snapshot save (#676): every shard does the SAME thing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaveMode {
    /// A full BASE save: every shard writes a fresh base file, `save_id` is a NEW base generation, the
    /// delta chain resets to empty. Forced on the first save, when deltas are disabled, or on
    /// compaction (the chain reached the cap).
    Base,
    /// A DELTA save onto the current base generation: every shard appends one delta, the manifest
    /// carries forward the base + prior deltas, and its `save_id` STAYS `base_epoch` (the base
    /// generation) so a delta's `base_epoch` equals the manifest `save_id`.
    Delta {
        /// The base generation this delta round applies onto (== the prior manifest's `save_id`).
        base_epoch: u64,
        /// This round's position in the chain (prior max `delta_epoch` + 1).
        delta_epoch: u64,
    },
}

/// Decide whether the NEXT whole-snapshot save is a BASE or a DELTA (#676), purely from the PRIOR
/// committed manifest. A BASE is forced when deltas are disabled, there is no prior snapshot (nothing
/// to delta onto), or the chain has reached `max_deltas` (compaction). Otherwise a DELTA onto the
/// prior base generation (`prior.save_id`) at the next `delta_epoch` (prior max + 1).
#[must_use]
pub fn decide_snapshot_mode(
    prior: Option<&Manifest>,
    deltas_enabled: bool,
    max_deltas: u32,
) -> SaveMode {
    if !deltas_enabled {
        return SaveMode::Base;
    }
    let Some(prior) = prior else {
        return SaveMode::Base; // no base generation to delta onto yet.
    };
    // The chain depth = the highest delta_epoch across the prior manifest's deltas (0 = base-only).
    let depth = prior
        .deltas
        .iter()
        .map(|d| d.delta_epoch)
        .max()
        .unwrap_or(0);
    if depth >= u64::from(max_deltas) {
        return SaveMode::Base; // compaction: the chain is full -> fold it back into a fresh base.
    }
    SaveMode::Delta {
        base_epoch: prior.save_id,
        delta_epoch: depth + 1,
    }
}

/// Read + validate the committed manifest in `dir`, or `None` if there is no loadable snapshot
/// (no manifest, an unreadable manifest, a wrong-magic / unknown-version / torn manifest). A
/// `None` means "start empty" (today's no-persistence behavior), the safe degradation; it is
/// NEVER an error the caller must handle.
#[must_use]
pub fn read_manifest(dir: &Path) -> Option<Manifest> {
    let bytes = format::read_file(&format::manifest_path(dir))?;
    Manifest::decode(&bytes)
}

/// CHECK, ONCE at node boot, whether the committed snapshot in `dir` is one THIS binary can load,
/// surfacing a version it CANNOT read LOUDLY (a `tracing::error!` + a classified
/// [`SnapshotLoadError`]) instead of the pre-#530 behavior of silently starting with an EMPTY
/// keyspace. Call this BEFORE the per-shard [`load_shard_resharded`] so the loud signal fires exactly
/// ONCE per node (the per-shard load stays silent-on-mismatch, which is correct because this node-level
/// gate has already reported it).
///
/// Returns:
/// - `Ok(())` -- there is no committed manifest (a fresh / empty data dir), the manifest is unreadable
///   / torn / foreign (all "start empty" degradations, unchanged), OR it is a manifest at the version
///   this binary reads. The per-shard load then proceeds normally.
/// - `Err(`[`SnapshotLoadError::UnknownVersion`]`)` -- the committed manifest records a format version
///   this binary does not support (almost always a NEWER dump an OLDER binary is loading: a downgrade
///   or a failed-upgrade rollback). The boot path decides POLICY: log-and-continue (start empty, but
///   no longer SILENTLY) or FAIL CLOSED (refuse to boot, `refuse_empty_start_on_version_mismatch`).
///   Either way this function has ALREADY emitted the `tracing::error!`, so the discard is never
///   silent.
///
/// The loud log lives HERE (co-located with detection) rather than only at the call site, so ANY
/// caller of the boot check inherits the never-silent guarantee STRUCTURALLY, not by remembering to
/// log. `tracing` is a log facade, not a clock / RNG, so this does not touch the ADR-0003 seam.
///
/// # Errors
///
/// [`SnapshotLoadError::UnknownVersion`] when the committed manifest's format version is unsupported.
pub fn check_snapshot_loadable(dir: &Path) -> Result<(), SnapshotLoadError> {
    let Some(bytes) = format::read_file(&format::manifest_path(dir)) else {
        return Ok(()); // no committed manifest: a genuinely empty / first boot, nothing to load.
    };
    match format::classify_manifest_version(&bytes) {
        Ok(()) => Ok(()),
        Err(err) => {
            tracing::error!(
                error = %err,
                dir = %dir.display(),
                "ironcache: the on-disk snapshot has an unsupported format version and will NOT be \
                 loaded; the node would start with an EMPTY keyspace (set \
                 refuse_empty_start_on_version_mismatch = true to fail closed and refuse to boot \
                 instead of discarding the on-disk data)"
            );
            Err(err)
        }
    }
}

/// Read + CRC-validate one shard's committed snapshot file, returning its RECORD BODY bytes, or
/// `None` when there is nothing loadable for that shard (a referenced-but-missing file, a foreign /
/// wrong-version / wrong-shard header, or a CRC mismatch = a TORN file).
///
/// CRASH-SAFETY: a CRC mismatch means the file is torn (a half-written file the manifest does not
/// vouch for, or bit-rot); the caller treats `None` as no-snapshot for that file (load nothing)
/// rather than feeding corrupt bytes to the decoder.
fn read_validated_shard_file(dir: &Path, entry: &ShardManifestEntry) -> Option<Vec<u8>> {
    let path = dir.join(&entry.file);
    let bytes = format::read_file(&path)?; // a referenced-but-missing file: load nothing.
    // split_shard_header borrows `bytes`; recompute the body range so we can return an OWNED body.
    let body = format::split_shard_header(&bytes, entry.shard)?; // foreign / wrong-version / shard.
    if format::crc32(body) != entry.crc {
        return None; // a torn file: never corrupt-load.
    }
    Some(bytes[format::SHARD_HEADER_LEN..].to_vec())
}

/// LOAD one shard's committed snapshot file into `store` (the load-on-boot path), replaying each
/// decoded [`ironcache_store::KvObj`] through [`ironcache_store::ShardStore::insert_object`] under
/// its recorded `db`. Returns the number of keys loaded.
///
/// The file is validated against its committed manifest `entry`:
/// - the file header must match (magic / version / shard index), else NO keys are loaded;
/// - the file's recomputed body CRC must equal the manifest's recorded CRC, else the file is
///   treated as TORN and NO keys are loaded (start-empty for that shard, never corrupt-load);
/// - a record whose recorded db is out of range for `store`, or that fails to decode, is skipped.
///
/// `now` drops an ALREADY-EXPIRED key on load (a key whose TTL deadline has strictly passed at
/// `now` is not inserted), so a snapshot taken long ago does not resurrect dead keys.
///
/// A missing file (the manifest references a file that is not present) loads nothing.
///
/// NOTE: this is the SAME-SHARD-COUNT helper used by the persist crate's own tests; the binary's
/// boot path uses [`load_all`], which RE-SHARDS every key by `route(key, shard_count)` and is
/// therefore correct across a shard-count change (see [`load_all`]).
pub fn load_shard_from_dir<E: EvictionHook, A: AccountingHook>(
    store: &mut ShardStore<E, A>,
    dir: &Path,
    entry: &ShardManifestEntry,
    now: UnixMillis,
) -> u64 {
    let Some(body) = read_validated_shard_file(dir, entry) else {
        return 0;
    };
    // No re-shard filter: accept every record (the caller is the single-store, same-layout path).
    load_records_into(store, &body, now, |_key| true)
}

/// LOAD this shard's slice of a committed snapshot, RE-SHARDING across a shard-count change (the
/// C1 fix). `this_shard` / `shard_count` are the CURRENT (loading-node) shard index + total; the
/// boot path calls this once per shard, EACH shard reading the WHOLE snapshot and keeping only the
/// keys it now owns. Returns the keys loaded into THIS shard's `store`.
///
/// ## Correct across a shard-count change
///
/// The snapshot's per-shard files were partitioned by the shard count AT SAVE TIME
/// (`manifest.shards`). The loading node may have a DIFFERENT count (`shard_count`): a key in
/// file-i at save time is NOT necessarily owned by shard-i now. So load does NOT blindly replay
/// file-i into shard-i (which would SILENTLY LOSE keys when the count shrinks, and MISROUTE every
/// GET when it grows). Instead THIS shard reads EVERY manifest shard file, decodes each key, and
/// inserts ONLY the keys where `route(key, shard_count) == this_shard` -- the SAME owner-shard hash
/// the router uses, so a reloaded key lives exactly where a fresh client write would put it. Every
/// shard doing this reconstructs the full keyspace for ANY N->M change; with N == M each shard's
/// own file is the only file that contributes keys it owns, so the result equals a per-file replay.
/// (Per-shard reads-all-files = boot-time read amplification, accepted.)
///
/// `route` MUST be the router's owner-shard function (`ironcache_server::owner_shard`); passing a
/// different hash would scatter keys to the wrong shards.
///
/// A torn / CRC-bad / missing shard file is skipped (its keys are absent), never corrupt-loaded.
/// `now` drops an already-expired key on load.
pub fn load_shard_resharded<E: EvictionHook, A: AccountingHook, R: Fn(&[u8], usize) -> usize>(
    store: &mut ShardStore<E, A>,
    dir: &Path,
    this_shard: usize,
    shard_count: usize,
    now: UnixMillis,
    route: R,
) -> u64 {
    let Some(manifest) = read_manifest(dir) else {
        return 0;
    };
    if shard_count == 0 {
        return 0;
    }
    let mut total = 0u64;
    // Read EVERY manifest shard file once (boot-time read amplification, accepted) and keep only the
    // keys this shard now owns under the CURRENT shard count.
    for entry in &manifest.entries {
        let Some(body) = read_validated_shard_file(dir, entry) else {
            continue; // a torn / missing file: its keys are absent (never corrupt-load).
        };
        total += load_records_into(store, &body, now, |key| {
            route(key, shard_count) == this_shard
        });
    }
    // v2 DELTA CHAIN (#676): apply the deltas ON TOP of the base, re-sharded by the SAME route.
    // Distinct save-time shards cover disjoint keysets, so folding all the good bodies together is
    // safe; the load-time reshard filter then keeps only the keys THIS shard now owns. Walk the chain
    // per save-time shard, TRUNCATING a shard to its good CONTIGUOUS PREFIX on the first bad link
    // (never fold a suffix across a hole -- the reviewer-flagged invariant). Two hardenings so a buggy
    // future save-path fails safe rather than silently corrupting a warm-start:
    //   * `truncated` holds EVERY truncated shard (not just one), so an interleaved / out-of-order
    //     manifest cannot un-truncate an earlier shard by clobbering a single slot.
    //   * a shard's links must have STRICTLY INCREASING delta_epoch; a non-increase (a reorder or a
    //     repeat) truncates the shard there. (Detecting a missing MIDDLE delta additionally needs the
    //     save-path to emit dense per-shard epochs -- a producer contract; this catches reorder/dup.)
    if !manifest.deltas.is_empty() {
        let mut delta_bodies: Vec<Vec<u8>> = Vec::new();
        let mut truncated: std::collections::HashSet<u32> = std::collections::HashSet::new();
        let mut last_epoch: std::collections::HashMap<u32, u64> = std::collections::HashMap::new();
        for d in &manifest.deltas {
            if truncated.contains(&d.shard) {
                continue; // the rest of a shard's chain after its first bad link.
            }
            // base_epoch binds a delta to its base generation (== this manifest's save_id); a mismatch
            // means the delta applies onto a DIFFERENT base.
            if d.base_epoch != manifest.save_id {
                truncated.insert(d.shard);
                continue;
            }
            // delta_epoch must strictly increase within a shard's chain (else a reorder / a repeat).
            if last_epoch
                .get(&d.shard)
                .is_some_and(|&prev| d.delta_epoch <= prev)
            {
                truncated.insert(d.shard);
                continue;
            }
            match read_validated_delta_file(dir, d) {
                Some(body) => {
                    last_epoch.insert(d.shard, d.delta_epoch);
                    delta_bodies.push(body);
                }
                None => {
                    truncated.insert(d.shard);
                }
            }
        }
        if !delta_bodies.is_empty() {
            let refs: Vec<&[u8]> = delta_bodies.iter().map(Vec::as_slice).collect();
            let net = apply_delta_chain(store, &refs, now, |key| {
                route(key, shard_count) == this_shard
            });
            total = total.saturating_add_signed(net);
        }
    }
    total
}

/// LOAD the WHOLE committed snapshot in `dir` into `stores` (one mutable store per shard, in
/// shard-index order), RE-SHARDING every key into its OWNING shard for the CURRENT shard count.
/// This is the all-stores convenience wrapper around [`load_shard_resharded`] (it loops the shards
/// for the caller); the binary's boot path instead calls [`load_shard_resharded`] per shard because
/// each shard owns its store on its own thread. Returns the total keys loaded, or `0` when there is
/// no loadable snapshot. See [`load_shard_resharded`] for the re-shard correctness argument.
///
/// `route` MUST be the router's owner-shard function (`ironcache_server::owner_shard`).
pub fn load_all<E: EvictionHook, A: AccountingHook, R: Fn(&[u8], usize) -> usize + Copy>(
    stores: &mut [&mut ShardStore<E, A>],
    dir: &Path,
    now: UnixMillis,
    route: R,
) -> u64 {
    let n = stores.len();
    if n == 0 {
        return 0;
    }
    let mut total = 0u64;
    for (shard, store) in stores.iter_mut().enumerate() {
        total += load_shard_resharded(store, dir, shard, n, now, route);
    }
    total
}

/// Decode every record in a shard file `body` and insert each into `store` under its recorded db,
/// dropping any key whose TTL has already passed at `now` and any key `keep` rejects. Returns the
/// count inserted. TOTAL: a record that fails to decode, or whose db is out of range, is skipped
/// (the decode is bounds-checked and never panics).
fn load_records_into<E: EvictionHook, A: AccountingHook, K: Fn(&[u8]) -> bool>(
    store: &mut ShardStore<E, A>,
    body: &[u8],
    now: UnixMillis,
    keep: K,
) -> u64 {
    let databases = store.databases() as u32;
    let mut loaded = 0u64;
    let mut rr = format::RecordReader::new(body);
    while let Some((db, rec)) = rr.next_record() {
        if db >= databases {
            continue; // a record for a db this store does not have (a reconfiguration): skip.
        }
        let Some(kv) = ironcache_repl::decode_kvobj(rec) else {
            continue; // a malformed record (cannot happen once the CRC matched): skip it.
        };
        if !keep(&kv.key) {
            continue; // a key this caller does not own (the re-shard filter): skip it.
        }
        // Drop an already-expired key on load: a deadline strictly in the past at `now` is dead.
        if let Some(UnixMillis(deadline)) = kv.expire_at {
            if now.0 > deadline {
                continue;
            }
        }
        store.insert_object(db, kv);
        loaded += 1;
    }
    loaded
}

/// Read + validate one delta file referenced by a v2 manifest entry (#676 loader): the delta header
/// must match (magic / delta-version / shard) and the body CRC must match `entry.crc`, else `None`
/// (a torn / missing / foreign delta -- the caller truncates THAT shard's chain to its good prefix).
/// Returns the OWNED record body (the bytes after the delta header), mirroring
/// [`read_validated_shard_file`] for base files.
fn read_validated_delta_file(dir: &Path, entry: &format::DeltaManifestEntry) -> Option<Vec<u8>> {
    let path = dir.join(&entry.file);
    let bytes = format::read_file(&path)?; // a referenced-but-missing delta: apply nothing.
    let (_base_epoch, _delta_epoch, body) = delta::split_delta_header(&bytes, entry.shard)?;
    if format::crc32(body) != entry.crc {
        return None; // a torn delta: never corrupt-apply.
    }
    Some(bytes[delta::DELTA_HEADER_LEN..].to_vec())
}

/// Apply a delta CHAIN onto an already-base-loaded store (#676 loader replay): fold the chain into
/// the net per-`(db, key)` effect (later-wins, tombstone-removes) and apply each effect this shard
/// OWNS -- a `Put` decodes + inserts (OVERWRITING the base value), a `Tombstone` deletes the base's
/// key. The deltas are re-sharded by the SAME `keep` filter the base load used, so an N->M shard
/// change routes delta keys exactly like base keys (the loader contract). An already-expired `Put` is
/// dropped, matching the base load. Returns the NET key change (+new keys, -removed keys) for the
/// load count.
///
/// `delta_bodies` are the chain's record bodies in manifest (apply) order, oldest first, ALREADY
/// truncated by the caller to a CONTIGUOUS good PREFIX per shard (a torn / missing / base-mismatched
/// link and everything after it in that shard's chain is dropped, so the fold never crosses a hole).
fn apply_delta_chain<E: EvictionHook, A: AccountingHook, K: Fn(&[u8]) -> bool>(
    store: &mut ShardStore<E, A>,
    delta_bodies: &[&[u8]],
    now: UnixMillis,
    keep: K,
) -> i64 {
    let databases = store.databases() as u32;
    let mut net: i64 = 0;
    for ((db, key), effect) in delta::fold_deltas(delta_bodies.iter().copied()) {
        if db >= databases {
            continue; // a record for a db this store does not have (a reconfiguration): skip.
        }
        if !keep(&key) {
            continue; // a key this shard does not own under the current shard count: skip (reshard).
        }
        match effect {
            delta::DeltaEffect::Put(blob) => {
                let Some(kv) = ironcache_repl::decode_kvobj(&blob) else {
                    continue; // a malformed value (cannot happen once the CRC matched): skip.
                };
                if let Some(UnixMillis(deadline)) = kv.expire_at {
                    if now.0 > deadline {
                        // An already-expired PUT: the key is logically DEAD. Do NOT merely skip -- if
                        // a value for this key survived in the base, skipping would RESURRECT the
                        // stale base value. DELETE it, so the result matches a full base of the
                        // post-delta state (which drops the expired record, leaving the key absent).
                        if store.delete(db, &key, now) {
                            net -= 1;
                        }
                        continue;
                    }
                }
                let existed = store.contains_live(db, &key, now);
                store.insert_object(db, kv);
                if !existed {
                    net += 1; // a brand-new key (an overwrite of a base key does not change the count).
                }
            }
            delta::DeltaEffect::Tombstone => {
                if store.delete(db, &key, now) {
                    net -= 1; // removed a base key.
                }
            }
        }
    }
    net
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::{
        CountingAccounting, EncodingThresholds, NewValueOwned, NullEviction, Store, UnixMillis,
    };
    use ironcache_store::{KvObj, ShardStore};

    type TestStore = ShardStore<NullEviction, CountingAccounting>;

    /// The shard count the C1 mismatch test SAVES with (re-shard then loads into 2 + 8).
    const SAVE_SHARDS: usize = 4;
    /// The DATABASE count for every test store (the `ShardStore::new` arg; unrelated to shards).
    const DBS: u32 = 4;

    // A STAND-IN owner-shard hash (FNV-1a 64-bit) used ONLY to exercise this crate's GENERIC
    // read-all-then-filter reshard algorithm (`load_shard_resharded`) with an arbitrary pure route
    // function. NOTE: this is NOT the live router anymore -- as of #517 `ironcache_server::owner_shard`
    // is slot-based (`slot_to_shard(key_slot(key), n)`), not this FNV. Production reload stays correct
    // because the binary passes the REAL `ironcache_server::owner_shard` (see `crate::persist`); this
    // crate has no dependency on `ironcache-server`, so the test just needs SOME deterministic route
    // fn to prove the reshard mechanism, and any pure fn (FNV here) suffices. The reshard is
    // route-fn-agnostic: whatever `owner_shard` is at load time, every key is re-homed by it.
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    fn test_hash64(key: &[u8]) -> u64 {
        let mut hash = FNV_OFFSET_BASIS;
        for &b in key {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }
    fn test_owner_shard(key: &[u8], n_shards: usize) -> usize {
        let n = n_shards.max(1) as u64;
        usize::try_from(test_hash64(key) % n).expect("modulo fits usize")
    }

    /// Load a single shard file body into one store with NO re-shard filter (the test helper for the
    /// same-shard-count path; mirrors `load_shard_from_dir`'s `|_| true`).
    fn load_body_all(store: &mut TestStore, body: &[u8], now: UnixMillis) -> u64 {
        load_records_into(store, body, now, |_key| true)
    }

    /// A throwaway temp directory unique to the test + process (so concurrent test runs do not
    /// collide). Created fresh; the caller removes it at the end.
    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "icpersist-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Populate a store with a representative keyspace across multiple dbs: strings (int / embstr
    /// / raw), a TTL'd key, and a collection (a hash). Returns the live key count.
    fn populate(store: &mut TestStore) -> u64 {
        // db 0: strings of each encoding.
        store.insert_object(0, KvObj::from_bytes(b"int", b"12345", None));
        store.insert_object(0, KvObj::from_bytes(b"emb", b"short string", None));
        store.insert_object(0, KvObj::from_bytes(b"raw", &vec![b'x'; 1024], None));
        // db 0: a key with a FAR-FUTURE TTL (survives load).
        store.insert_object(
            0,
            KvObj::from_bytes(b"ttl-alive", b"v", Some(UnixMillis(10_000_000))),
        );
        // db 1: a hash (a collection) + another string.
        store.insert_object(
            1,
            KvObj::from_new_owned(
                b"h",
                NewValueOwned::hash(vec![
                    (b"f1".to_vec(), b"v1".to_vec()),
                    (b"f2".to_vec(), b"v2".to_vec()),
                ]),
                None,
                &EncodingThresholds::defaults(),
            ),
        );
        store.insert_object(1, KvObj::from_bytes(b"db1-str", b"hello db1", None));
        6
    }

    /// #676 Phase 0 correctness gate: [`ironcache_repl::encode_entry_into`] (the direct,
    /// no-`KvObj`-clone path `push_entry` now uses) MUST be byte-identical to the prior
    /// `encode_kvobj(&entry.to_kvobj())` for EVERY encoding, across the string family (the fast
    /// path) AND collections (the fallback). Any drift would silently change the on-disk snapshot
    /// bytes and their manifest CRC and diverge from the replication codec, so this asserts exact
    /// equality over a frozen keyspace covering every case.
    #[test]
    #[allow(clippy::too_many_lines)] // a flat battery of insert cases; splitting hurts readability
    fn encode_entry_into_is_byte_identical_to_encode_kvobj() {
        let th = EncodingThresholds::defaults();
        let mut store: TestStore = ShardStore::new(2);
        // Strings of every encoding, plus the edge cases the fast path must get exactly right.
        store.insert_object(0, KvObj::from_bytes(b"int", b"12345", None));
        store.insert_object(0, KvObj::from_bytes(b"int-neg", b"-42", None));
        store.insert_object(0, KvObj::from_bytes(b"int-zero", b"0", None));
        // i64 boundary ints: format_i64's i128-negation / u64-magnitude parse is the trickiest
        // part of the canonical-digit invariant the fast path leans on (review LOW finding).
        store.insert_object(
            0,
            KvObj::from_bytes(b"int-min", b"-9223372036854775808", None),
        );
        store.insert_object(
            0,
            KvObj::from_bytes(b"int-max", b"9223372036854775807", None),
        );
        store.insert_object(0, KvObj::from_bytes(b"emb", b"short string", None));
        store.insert_object(0, KvObj::from_bytes(b"raw", &vec![b'x'; 1024], None));
        store.insert_object(0, KvObj::from_bytes(b"empty", b"", None));
        store.insert_object(0, KvObj::from_bytes(b"binkey\0\xff", b"\x00\x01\xfe", None));
        store.insert_object(
            0,
            KvObj::from_bytes(b"ttl", b"v", Some(UnixMillis(10_000_000))),
        );
        // A TTL'd INT: the fast path's most common numeric-with-expiry shape, exercising the
        // ttl marker AND the canonical-digit value together (review LOW finding).
        store.insert_object(
            0,
            KvObj::from_bytes(b"ttlint", b"98765", Some(UnixMillis(9_000_000))),
        );
        // Collections (the deep-clone fallback branch): one of each type, INCLUDING an intset set
        // (numeric members) and a field-TTL hash (the HASHTABLEEX enc tag the invariant relies on
        // being Hash-only) so the enc-tag surface is byte-checked, not just asserted in prose.
        store.insert_object(
            1,
            KvObj::from_new_owned(
                b"hash",
                NewValueOwned::hash(vec![
                    (b"f1".to_vec(), b"v1".to_vec()),
                    (b"f2".to_vec(), b"v2".to_vec()),
                ]),
                None,
                &th,
            ),
        );
        store.insert_object(
            1,
            KvObj::from_new_owned(
                b"list",
                NewValueOwned::list(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]),
                None,
                &th,
            ),
        );
        store.insert_object(
            1,
            KvObj::from_new_owned(
                b"set",
                NewValueOwned::set(vec![b"m1".to_vec(), b"m2".to_vec()]),
                None,
                &th,
            ),
        );
        store.insert_object(
            1,
            KvObj::from_new_owned(
                b"zset",
                NewValueOwned::zset(vec![(b"m1".to_vec(), 1.5), (b"m2".to_vec(), 2.0)]),
                None,
                &th,
            ),
        );
        store.insert_object(
            1,
            KvObj::from_new_owned(
                b"intset",
                NewValueOwned::set(vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]),
                None,
                &th,
            ),
        );
        store.insert_object(
            1,
            KvObj::from_new_owned(
                b"hash-ex",
                NewValueOwned::hash_ex(
                    vec![
                        (b"f1".to_vec(), b"v1".to_vec()),
                        (b"f2".to_vec(), b"v2".to_vec()),
                    ],
                    vec![(b"f1".to_vec(), UnixMillis(12_000_000))],
                ),
                None,
                &th,
            ),
        );

        let frozen = store.begin_save();
        let mut checked = 0usize;
        for slot in &frozen {
            for entry in slot.entries() {
                let mut direct = Vec::new();
                ironcache_repl::encode_entry_into(&mut direct, entry);
                let via_kvobj = ironcache_repl::encode_kvobj(&entry.to_kvobj());
                assert_eq!(
                    direct,
                    via_kvobj,
                    "encode_entry_into diverged from encode_kvobj for key {:?}",
                    entry.key()
                );
                checked += 1;
            }
        }
        assert_eq!(
            checked, 17,
            "expected all 17 inserted entries frozen and checked"
        );
    }

    #[test]
    fn dump_then_load_round_trips_key_for_key() {
        let now = UnixMillis(1_000);
        let mut src: TestStore = ShardStore::new(4);
        let expected = populate(&mut src);

        let dump = dump_shard_keyspace(&src, 0, now);
        assert_eq!(dump.keys, expected, "every live key is recorded");

        // Strip the header and replay into a FRESH store; assert identical contents.
        let body = format::split_shard_header(&dump.bytes, 0).expect("valid header");
        assert_eq!(
            format::crc32(body),
            dump.crc,
            "the recorded crc matches the body"
        );

        let mut dst: TestStore = ShardStore::new(4);
        let loaded = load_body_all(&mut dst, body, now);
        assert_eq!(loaded, expected);
        assert_eq!(dst.len(), src.len(), "DBSIZE-equivalent matches");

        // Spot-check values + TTL survive (read returns the same bytes; the TTL key is alive).
        assert_read_eq(&mut dst, 0, b"int", b"12345");
        assert_read_eq(&mut dst, 0, b"emb", b"short string");
        assert_read_eq(&mut dst, 1, b"db1-str", b"hello db1");
        let v = dst
            .read(0, b"ttl-alive", now)
            .expect("ttl key alive on load");
        assert_eq!(
            v.expire_at(),
            Some(UnixMillis(10_000_000)),
            "TTL round-trips"
        );
    }

    fn assert_read_eq(store: &mut TestStore, db: u32, key: &[u8], expected: &[u8]) {
        let v = store.read(db, key, UnixMillis(1_000)).expect("key present");
        assert_eq!(v.as_bytes(), expected, "value round-trips for {key:?}");
    }

    #[test]
    fn expired_key_is_dropped_on_load() {
        let now = UnixMillis(5_000);
        let mut src: TestStore = ShardStore::new(1);
        // A key whose deadline is in the FUTURE relative to the dump's `now` so the DUMP keeps
        // it, but in the PAST relative to the LOAD's later `now` so the LOAD drops it.
        src.insert_object(0, KvObj::from_bytes(b"soon", b"v", Some(UnixMillis(6_000))));
        src.insert_object(0, KvObj::from_bytes(b"keep", b"v", None));

        let dump = dump_shard_keyspace(&src, 0, now); // now < 6000: "soon" is still alive, dumped.
        assert_eq!(dump.keys, 2);

        let body = format::split_shard_header(&dump.bytes, 0).unwrap();
        let mut dst: TestStore = ShardStore::new(1);
        // Load LATER (now=7000 > 6000): the expired key is dropped, the permanent one kept.
        let loaded = load_body_all(&mut dst, body, UnixMillis(7_000));
        assert_eq!(loaded, 1, "the expired key is dropped on load");
        assert!(dst.read(0, b"soon", UnixMillis(7_000)).is_none());
        assert!(dst.read(0, b"keep", UnixMillis(7_000)).is_some());
    }

    #[test]
    fn torn_crc_file_is_rejected_load_as_empty() {
        let now = UnixMillis(1_000);
        let dir = temp_dir("torn");
        let mut src: TestStore = ShardStore::new(2);
        populate(&mut src);

        let entry = save_shard_to_dir(&src, 0, &dir, now).expect("save shard 0");
        // CORRUPT the on-disk file body (flip a byte past the header) WITHOUT updating the
        // manifest CRC: load must reject it as torn and load NOTHING (never corrupt-load, never
        // panic).
        let path = format::shard_path(&dir, 0);
        let mut bytes = format::read_file(&path).unwrap();
        let body_start = format::SHARD_HEADER_LEN;
        bytes[body_start] ^= 0xFF;
        format::write_file_atomic(&path, &bytes).unwrap();

        let mut dst: TestStore = ShardStore::new(2);
        let loaded = load_shard_from_dir(&mut dst, &dir, &entry, now);
        assert_eq!(loaded, 0, "a torn file is rejected (load as empty)");
        assert!(dst.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn build_delta_puts_present_and_tombstones_absent_or_expired() {
        // #676 delta build: resolve the dirty keys against the frozen snapshot into PUT/TOMBSTONE.
        let now = UnixMillis(10);
        let mut store: TestStore = ShardStore::new(1);
        // BASE (tracking OFF): keys that exist before the epoch cut.
        store.insert_object(0, KvObj::from_bytes(b"keep", b"base", None));
        store.insert_object(0, KvObj::from_bytes(b"del", b"base", None));

        // EPOCH CUT: track every write from here.
        store.enable_dirty_tracking();
        store.insert_object(0, KvObj::from_bytes(b"keep", b"updated", None)); // overwrite -> PUT
        store.insert_object(0, KvObj::from_bytes(b"new", b"fresh", None)); // create -> PUT
        store.insert_object(0, KvObj::from_bytes(b"gone-ttl", b"v", Some(UnixMillis(5)))); // expired at now=10
        assert!(store.delete(0, b"del", now)); // delete -> TOMBSTONE

        let dirty: Vec<(u32, Box<[u8]>)> = store
            .take_dirty_keys()
            .expect("tracking on")
            .into_iter()
            .collect();
        let frozen = store.begin_save();
        let dump = build_delta_from_frozen(&frozen, &dirty, 0, 1, 2, now);
        assert_eq!(
            dump.puts, 2,
            "keep (overwrite) + new (create) are live PUTs"
        );
        assert_eq!(
            dump.tombstones, 2,
            "del (deleted) + gone-ttl (expired) are TOMBSTONEs"
        );

        // Fold the delta and verify the net per-(db,key) effect + the header epochs.
        let (base_epoch, delta_epoch, body) =
            delta::split_delta_header(&dump.bytes, 0).expect("valid delta header");
        assert_eq!((base_epoch, delta_epoch), (1, 2));
        let net = delta::fold_deltas([body]);
        assert!(matches!(
            net.get(&(0, b"keep".to_vec())),
            Some(delta::DeltaEffect::Put(_))
        ));
        assert!(matches!(
            net.get(&(0, b"new".to_vec())),
            Some(delta::DeltaEffect::Put(_))
        ));
        assert_eq!(
            net.get(&(0, b"del".to_vec())),
            Some(&delta::DeltaEffect::Tombstone)
        );
        assert_eq!(
            net.get(&(0, b"gone-ttl".to_vec())),
            Some(&delta::DeltaEffect::Tombstone)
        );
        assert_eq!(net.len(), 4);
        drop(frozen);
        store.end_save();
    }

    #[test]
    fn apply_delta_chain_overwrites_inserts_removes_and_reshards() {
        let now = UnixMillis(0);
        let mut store: TestStore = ShardStore::new(1);
        // A "base": keep + del + foreign (foreign will be filtered out by the reshard `keep`).
        store.insert_object(0, KvObj::from_bytes(b"keep", b"base", None));
        store.insert_object(0, KvObj::from_bytes(b"del", b"base", None));
        store.insert_object(0, KvObj::from_bytes(b"foreign", b"base", None));

        // A delta body: overwrite keep, create new, tombstone del, and a PUT for foreign (which the
        // reshard filter drops, so foreign keeps its base value).
        let enc = |k: &[u8], v: &[u8]| ironcache_repl::encode_kvobj(&KvObj::from_bytes(k, v, None));
        let mut body = Vec::new();
        delta::put_put_record(&mut body, 0, b"keep", &enc(b"keep", b"updated"));
        delta::put_put_record(&mut body, 0, b"new", &enc(b"new", b"fresh"));
        delta::put_tombstone_record(&mut body, 0, b"del");
        delta::put_put_record(
            &mut body,
            0,
            b"foreign",
            &enc(b"foreign", b"should-be-skipped"),
        );

        // Reshard filter: this shard owns everything EXCEPT "foreign".
        let net = apply_delta_chain(&mut store, &[body.as_slice()], now, |k| k != b"foreign");
        // new (+1) + del removed (-1) + keep overwrite (0) + foreign skipped (0) = 0.
        assert_eq!(net, 0);
        assert!(
            store.contains_live(0, b"keep", now),
            "overwritten, still present"
        );
        assert!(
            store.contains_live(0, b"new", now),
            "delta PUT inserted a new key"
        );
        assert!(
            !store.contains_live(0, b"del", now),
            "delta TOMBSTONE removed it"
        );
        assert!(
            store.contains_live(0, b"foreign", now),
            "the un-owned delta PUT was reshard-filtered; the base value survives"
        );
    }

    #[test]
    fn v2_snapshot_base_plus_delta_loads_into_a_fresh_store() {
        // END TO END: build a base snapshot, then a delta from tracked writes, write a v2 manifest,
        // and load it all into a FRESH store -- the final state must be base-with-the-delta-applied.
        const SAVE_ID: u64 = 7; // the base generation; the delta's base_epoch must match it.
        let dir = temp_dir("v2-delta-load");
        let now = UnixMillis(0);

        // BASE at t0: {keep=base, del=base}.
        let mut src: TestStore = ShardStore::new(1);
        src.insert_object(0, KvObj::from_bytes(b"keep", b"base", None));
        src.insert_object(0, KvObj::from_bytes(b"del", b"base", None));
        let base_frozen = src.begin_save();
        let base_dump = dump_frozen_slots(&base_frozen, 0, now);
        let base_entry = write_shard_dump(&base_dump, 0, &dir).expect("write base file");
        drop(base_frozen);
        src.end_save();

        // WRITES at t1 (tracked): overwrite keep, create new, delete del.
        src.enable_dirty_tracking();
        src.insert_object(0, KvObj::from_bytes(b"keep", b"updated", None));
        src.insert_object(0, KvObj::from_bytes(b"new", b"fresh", None));
        assert!(src.delete(0, b"del", now));
        let dirty: Vec<(u32, Box<[u8]>)> = src
            .take_dirty_keys()
            .expect("tracking on")
            .into_iter()
            .collect();

        // DELTA at t2: build it from the frozen post-write snapshot, base_epoch == the manifest save_id.
        let delta_frozen = src.begin_save();
        let delta_dump = build_delta_from_frozen(&delta_frozen, &dirty, 0, SAVE_ID, 1, now);
        drop(delta_frozen);
        src.end_save();
        let delta_file = "dump-shard-0-delta-1.icsd".to_string();
        format::write_file_atomic(&dir.join(&delta_file), &delta_dump.bytes).expect("write delta");

        // v2 MANIFEST tying the base + the delta chain together.
        let manifest = format::Manifest {
            version: format::MANIFEST_VERSION_DELTA,
            shards: 1,
            save_id: SAVE_ID,
            save_unix_secs: 1,
            entries: vec![base_entry],
            deltas: vec![format::DeltaManifestEntry {
                shard: 0,
                file: delta_file,
                puts: delta_dump.puts,
                tombstones: delta_dump.tombstones,
                crc: delta_dump.crc,
                base_epoch: SAVE_ID,
                delta_epoch: 1,
            }],
        };
        format::write_file_atomic(&format::manifest_path(&dir), &manifest.encode())
            .expect("write v2 manifest");

        // LOAD into a fresh single-shard store (route always 0).
        let mut dst: TestStore = ShardStore::new(1);
        let loaded = load_shard_resharded(&mut dst, &dir, 0, 1, now, |_k, _n| 0);
        assert_eq!(loaded, 2, "base 2 + delta (new +1, del -1) = 2 live keys");
        assert!(dst.contains_live(0, b"keep", now));
        assert!(dst.contains_live(0, b"new", now));
        assert!(
            !dst.contains_live(0, b"del", now),
            "the tombstone removed del"
        );

        // VALUE check: re-dump the loaded store; keep must carry the UPDATED value, no stale base.
        let check_frozen = dst.begin_save();
        let bytes = dump_frozen_slots(&check_frozen, 0, now).bytes;
        assert!(
            bytes.windows(7).any(|w| w == b"updated"),
            "keep was overwritten to the delta value"
        );
        assert!(bytes.windows(5).any(|w| w == b"fresh"), "new is present");
        assert!(
            !bytes.windows(4).any(|w| w == b"base"),
            "no stale base value survived the delta"
        );
        drop(check_frozen);
        dst.end_save();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn apply_delta_chain_expired_put_deletes_the_base_key_no_resurrection() {
        // A delta PUT that is ALREADY EXPIRED at warm-start must DELETE the (base-resident) key, not
        // just skip -- else the stale base value would resurrect (reviewer-caught divergence from a
        // full base of the post-delta state, which drops the expired record).
        let now = UnixMillis(10);
        let mut store: TestStore = ShardStore::new(1);
        store.insert_object(0, KvObj::from_bytes(b"k", b"v1", None)); // base value, no TTL
        assert!(store.contains_live(0, b"k", now));

        let mut body = Vec::new();
        let expired =
            ironcache_repl::encode_kvobj(&KvObj::from_bytes(b"k", b"v2", Some(UnixMillis(5))));
        delta::put_put_record(&mut body, 0, b"k", &expired); // PUT k=v2 with deadline 5 < now 10
        let net = apply_delta_chain(&mut store, &[body.as_slice()], now, |_| true);

        assert_eq!(net, -1, "the expired PUT removed the base key");
        assert!(
            !store.contains_live(0, b"k", now),
            "no resurrection: the stale base value must not survive an expired delta PUT"
        );
    }

    #[test]
    fn loader_truncates_a_shard_chain_at_a_torn_delta() {
        // A torn MIDDLE delta (a manifest CRC that does not match its file) truncates the shard's
        // chain to the good prefix: the delta BEFORE the tear applies, the tear + everything after it
        // is dropped (never fold across a hole).
        const SID: u64 = 3;
        let dir = temp_dir("delta-truncate");
        let now = UnixMillis(0);
        let mut src: TestStore = ShardStore::new(1);
        src.insert_object(0, KvObj::from_bytes(b"a", b"base", None));
        src.insert_object(0, KvObj::from_bytes(b"b", b"base", None));
        let bf = src.begin_save();
        let base_entry = write_shard_dump(&dump_frozen_slots(&bf, 0, now), 0, &dir).unwrap();
        drop(bf);
        src.end_save();

        // Two delta files for shard 0: d1 (epoch 1) PUT a=d1; d2 (epoch 2) PUT b=d2. Write both files
        // correctly, but give d2's MANIFEST entry a WRONG crc so the loader sees d2 as torn.
        let enc = |k: &[u8], v: &[u8]| ironcache_repl::encode_kvobj(&KvObj::from_bytes(k, v, None));
        let write_delta = |name: &str, epoch: u64, recs: &[u8]| -> u32 {
            let mut file = Vec::new();
            delta::put_delta_header(&mut file, 0, SID, epoch);
            file.extend_from_slice(recs);
            format::write_file_atomic(&dir.join(name), &file).unwrap();
            format::crc32(recs)
        };
        let mut r1 = Vec::new();
        delta::put_put_record(&mut r1, 0, b"a", &enc(b"a", b"d1"));
        let crc1 = write_delta("d1.icsd", 1, &r1);
        let mut r2 = Vec::new();
        delta::put_put_record(&mut r2, 0, b"b", &enc(b"b", b"d2"));
        write_delta("d2.icsd", 2, &r2); // file is valid, but the manifest crc below is WRONG.

        let manifest = format::Manifest {
            version: format::MANIFEST_VERSION_DELTA,
            shards: 1,
            save_id: SID,
            save_unix_secs: 1,
            entries: vec![base_entry],
            deltas: vec![
                format::DeltaManifestEntry {
                    shard: 0,
                    file: "d1.icsd".to_string(),
                    puts: 1,
                    tombstones: 0,
                    crc: crc1,
                    base_epoch: SID,
                    delta_epoch: 1,
                },
                format::DeltaManifestEntry {
                    shard: 0,
                    file: "d2.icsd".to_string(),
                    puts: 1,
                    tombstones: 0,
                    crc: 0xDEAD_BEEF, // WRONG crc -> the loader treats d2 as torn.
                    base_epoch: SID,
                    delta_epoch: 2,
                },
            ],
        };
        format::write_file_atomic(&format::manifest_path(&dir), &manifest.encode()).unwrap();

        let mut dst: TestStore = ShardStore::new(1);
        let loaded = load_shard_resharded(&mut dst, &dir, 0, 1, now, |_k, _n| 0);
        assert_eq!(
            loaded, 2,
            "base a+b = 2; d1 overwrote a (0), d2 truncated (0)"
        );
        let bytes = dump_frozen_slots(&dst.begin_save(), 0, now).bytes;
        assert!(
            bytes.windows(2).any(|w| w == b"d1"),
            "d1 (before the tear) applied: a=d1"
        );
        assert!(
            !bytes.windows(2).any(|w| w == b"d2"),
            "d2 (the torn link) was NOT applied: b stays base"
        );
        dst.end_save();
        std::fs::remove_dir_all(&dir).ok();
    }

    fn base_manifest(save_id: u64, delta_depth: u64) -> Manifest {
        Manifest {
            version: if delta_depth == 0 {
                format::MANIFEST_VERSION_BASE
            } else {
                format::MANIFEST_VERSION_DELTA
            },
            shards: 1,
            save_id,
            save_unix_secs: 1,
            entries: vec![ShardManifestEntry {
                shard: 0,
                file: shard_file_name(0),
                keys: 1,
                crc: 0,
            }],
            deltas: (1..=delta_depth)
                .map(|e| format::DeltaManifestEntry {
                    shard: 0,
                    file: format!("d{e}.icsd"),
                    puts: 1,
                    tombstones: 0,
                    crc: 0,
                    base_epoch: save_id,
                    delta_epoch: e,
                })
                .collect(),
        }
    }

    #[test]
    fn decide_snapshot_mode_base_delta_and_compaction() {
        // Disabled -> always base.
        assert_eq!(decide_snapshot_mode(None, false, 8), SaveMode::Base);
        assert_eq!(
            decide_snapshot_mode(Some(&base_manifest(5, 0)), false, 8),
            SaveMode::Base
        );
        // Enabled but no prior snapshot -> base (nothing to delta onto).
        assert_eq!(decide_snapshot_mode(None, true, 8), SaveMode::Base);
        // Enabled + a prior BASE (no deltas) -> a delta onto that generation, epoch 1.
        assert_eq!(
            decide_snapshot_mode(Some(&base_manifest(5, 0)), true, 8),
            SaveMode::Delta {
                base_epoch: 5,
                delta_epoch: 1
            }
        );
        // A chain at depth 3 (< cap) -> the next delta is epoch 4, same base generation.
        assert_eq!(
            decide_snapshot_mode(Some(&base_manifest(9, 3)), true, 8),
            SaveMode::Delta {
                base_epoch: 9,
                delta_epoch: 4
            }
        );
        // A chain at the cap -> compaction (a fresh base).
        assert_eq!(
            decide_snapshot_mode(Some(&base_manifest(9, 8)), true, 8),
            SaveMode::Base
        );
    }

    #[test]
    fn write_manifest_v2_is_v1_base_only_and_v2_with_deltas() {
        let dir = temp_dir("wm-v2");
        let base = vec![ShardManifestEntry {
            shard: 0,
            file: shard_file_name(0),
            keys: 3,
            crc: 7,
        }];
        // No deltas -> a v1 base-only manifest (write_manifest delegates here, so byte-compatible).
        let m1 = write_manifest_v2(&dir, 4, 100, base.clone(), vec![]).unwrap();
        assert_eq!(m1.version, format::MANIFEST_VERSION_BASE);
        assert!(m1.deltas.is_empty());
        assert_eq!(read_manifest(&dir).unwrap(), m1);
        // With a delta -> a v2 manifest that round-trips + carries the delta.
        let deltas = vec![format::DeltaManifestEntry {
            shard: 0,
            file: "d1.icsd".to_string(),
            puts: 2,
            tombstones: 1,
            crc: 0xABCD,
            base_epoch: 4,
            delta_epoch: 1,
        }];
        let m2 = write_manifest_v2(&dir, 4, 100, base, deltas).unwrap();
        assert_eq!(m2.version, format::MANIFEST_VERSION_DELTA);
        assert_eq!(read_manifest(&dir).expect("v2 reads back"), m2);
        assert_eq!(m2.deltas.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parse_delta_file_name_is_the_strict_inverse_of_delta_file_name() {
        // Round-trips exactly what `delta_file_name` produces.
        assert_eq!(parse_delta_file_name(&delta_file_name(0, 1)), Some((0, 1)));
        assert_eq!(
            parse_delta_file_name(&delta_file_name(37, 900)),
            Some((37, 900))
        );
        // Rejects a base file, the manifest, and anything non-integer / foreign -> GC never touches it.
        assert_eq!(parse_delta_file_name(&shard_file_name(0)), None);
        assert_eq!(parse_delta_file_name("dump.manifest"), None);
        assert_eq!(parse_delta_file_name("dump-shard-x-delta-1.icsd"), None);
        assert_eq!(parse_delta_file_name("dump-shard-0-delta-y.icsd"), None);
        assert_eq!(parse_delta_file_name("unrelated.txt"), None);
    }

    #[test]
    fn gc_orphan_deltas_removes_only_unreferenced_deltas() {
        let dir = temp_dir("gc-orphan");
        // A base file, three delta files, and two files GC must NEVER touch.
        let base = shard_file_name(0);
        std::fs::write(dir.join(&base), b"base").unwrap();
        for e in [1u64, 2, 3] {
            std::fs::write(dir.join(delta_file_name(0, e)), b"delta").unwrap();
        }
        std::fs::write(dir.join("dump.manifest"), b"m").unwrap();
        std::fs::write(dir.join("unrelated.txt"), b"x").unwrap();

        // A committed manifest referencing ONLY delta epoch 3 (epochs 1 + 2 are now orphans).
        let base_entries = vec![ShardManifestEntry {
            shard: 0,
            file: base.clone(),
            keys: 1,
            crc: 0,
        }];
        let deltas = vec![DeltaManifestEntry {
            shard: 0,
            file: delta_file_name(0, 3),
            puts: 1,
            tombstones: 0,
            crc: 0,
            base_epoch: 1,
            delta_epoch: 3,
        }];
        let manifest = write_manifest_v2(&dir, 1, 100, base_entries, deltas).unwrap();

        assert_eq!(gc_orphan_deltas(&dir, &manifest).unwrap(), 2);
        assert!(dir.join(delta_file_name(0, 3)).exists(), "referenced kept");
        assert!(!dir.join(delta_file_name(0, 1)).exists(), "orphan 1 gone");
        assert!(!dir.join(delta_file_name(0, 2)).exists(), "orphan 2 gone");
        assert!(dir.join(&base).exists(), "base untouched");
        assert!(dir.join("dump.manifest").exists(), "manifest untouched");
        assert!(dir.join("unrelated.txt").exists(), "foreign untouched");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn gc_orphan_deltas_base_compaction_reclaims_whole_chain() {
        let dir = temp_dir("gc-compact");
        let base = shard_file_name(0);
        std::fs::write(dir.join(&base), b"base").unwrap();
        for e in [1u64, 2, 3, 4] {
            std::fs::write(dir.join(delta_file_name(0, e)), b"delta").unwrap();
        }
        // A base-only manifest (compaction): no deltas referenced -> the whole chain is orphaned.
        let base_entries = vec![ShardManifestEntry {
            shard: 0,
            file: base.clone(),
            keys: 1,
            crc: 0,
        }];
        let manifest = write_manifest_v2(&dir, 2, 100, base_entries, vec![]).unwrap();
        assert_eq!(gc_orphan_deltas(&dir, &manifest).unwrap(), 4);
        assert!(dir.join(&base).exists());
        for e in [1u64, 2, 3, 4] {
            assert!(!dir.join(delta_file_name(0, e)).exists());
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_delta_file_names_correctly_and_round_trips() {
        let dir = temp_dir("write-delta");
        let now = UnixMillis(0);
        // A store + a tracked write -> a real delta.
        let mut store: TestStore = ShardStore::new(1);
        store.insert_object(0, KvObj::from_bytes(b"base", b"v", None));
        store.enable_dirty_tracking();
        store.insert_object(0, KvObj::from_bytes(b"new", b"fresh", None));
        let dirty: Vec<(u32, Box<[u8]>)> = store.take_dirty_keys().unwrap().into_iter().collect();
        let frozen = store.begin_save();
        let dump = build_delta_from_frozen(&frozen, &dirty, 0, 5, 2, now);
        drop(frozen);
        store.end_save();

        // Write it -> a DeltaManifestEntry, named per the convention.
        let entry = write_delta_file(&dump, 0, 5, 2, &dir).expect("write delta file");
        assert_eq!(entry.file, "dump-shard-0-delta-2.icsd");
        assert_eq!(
            (entry.shard, entry.base_epoch, entry.delta_epoch),
            (0, 5, 2)
        );
        assert_eq!(entry.puts, dump.puts);
        assert_eq!(entry.crc, dump.crc);

        // The written file validates against the entry (magic/version/shard + body CRC) and folds.
        let body = read_validated_delta_file(&dir, &entry).expect("delta file validates on read");
        let net = delta::fold_deltas([body.as_slice()]);
        assert!(matches!(
            net.get(&(0, b"new".to_vec())),
            Some(delta::DeltaEffect::Put(_))
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn full_save_load_via_manifest_round_trips() {
        let now = UnixMillis(2_000);
        let dir = temp_dir("manifest");

        // Two shards: place each key on its REAL owner (the router hash), so the per-shard files
        // hold exactly the keys their shard owns -- the production invariant the dump preserves.
        let mut s: [TestStore; 2] = [ShardStore::new(2), ShardStore::new(2)];
        let keyvals: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"bee", b"99"), (b"c", b"three")];
        for (k, v) in keyvals {
            let owner = test_owner_shard(k, 2);
            s[owner].insert_object(0, KvObj::from_bytes(k, v, None));
        }

        // SAVE: per-shard files, then commit the manifest LAST.
        let e0 = save_shard_to_dir(&s[0], 0, &dir, now).unwrap();
        let e1 = save_shard_to_dir(&s[1], 1, &dir, now).unwrap();
        let manifest = write_manifest(&dir, 1, 1_700_000_000, vec![e1, e0]).unwrap();
        assert_eq!(manifest.shards, 2);
        assert_eq!(manifest.total_keys(), 3);
        assert_eq!(manifest.save_unix_secs, 1_700_000_000);
        // The manifest is sorted by shard regardless of the order entries were passed.
        assert_eq!(manifest.entries[0].shard, 0);
        assert_eq!(manifest.entries[1].shard, 1);

        // LOAD the whole snapshot into fresh stores via the manifest, RE-SHARDING by the router hash
        // (same shard count, so each key lands back on its owner). All three keys round-trip.
        let mut d0: TestStore = ShardStore::new(2);
        let mut d1: TestStore = ShardStore::new(2);
        let mut stores: Vec<&mut TestStore> = vec![&mut d0, &mut d1];
        let total = load_all(&mut stores, &dir, now, test_owner_shard);
        assert_eq!(total, 3);
        for (k, v) in keyvals {
            let owner = test_owner_shard(k, 2);
            let got = stores[owner]
                .read(0, k, now)
                .expect("key present on its owner");
            assert_eq!(got.as_bytes(), *v, "{k:?} round-trips on its owner shard");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// THE C1 TEST: a snapshot saved with N shards loads CORRECTLY into a node with M != N shards
    /// (re-shard on load). Save 4 shards, then reload into 2 and into 8, and assert EVERY key is
    /// readable on its (re-computed) owner -- no key lost when the count shrinks, no GET misrouted
    /// when it grows. This is the bug the old "file-i -> shard-i" load would have silently failed.
    #[test]
    fn shard_count_mismatch_reshards_every_key() {
        let now = UnixMillis(1_000);
        let dir = temp_dir("reshard");

        // Save with 4 shards: distribute 200 keys onto their real owners over 4 shards. (The store
        // ctor arg is the DATABASE count, unrelated to the logical shard count the route uses.)
        let total_keys = 200usize;
        let mut src: Vec<TestStore> = (0..SAVE_SHARDS).map(|_| ShardStore::new(DBS)).collect();
        for i in 0..total_keys {
            let key = format!("key:{i}");
            let owner = test_owner_shard(key.as_bytes(), SAVE_SHARDS);
            src[owner].insert_object(
                0,
                KvObj::from_bytes(key.as_bytes(), format!("v{i}").as_bytes(), None),
            );
        }
        let mut entries = Vec::new();
        for (shard, store) in src.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let e = save_shard_to_dir(store, shard as u32, &dir, now).unwrap();
            entries.push(e);
        }
        let manifest = write_manifest(&dir, 1, 1_700_000_000, entries).unwrap();
        assert_eq!(manifest.shards, 4);
        assert_eq!(manifest.total_keys() as usize, total_keys);

        // Reload into a DIFFERENT shard count and assert every key is present on its new owner.
        for &load_shards in &[2usize, 8usize] {
            let mut dst: Vec<TestStore> = (0..load_shards).map(|_| ShardStore::new(DBS)).collect();
            let mut refs: Vec<&mut TestStore> = dst.iter_mut().collect();
            let loaded = load_all(&mut refs, &dir, now, test_owner_shard) as usize;
            assert_eq!(
                loaded, total_keys,
                "all {total_keys} keys re-shard into {load_shards} shards (none lost)"
            );
            for i in 0..total_keys {
                let key = format!("key:{i}");
                let owner = test_owner_shard(key.as_bytes(), load_shards);
                let got = refs[owner]
                    .read(0, key.as_bytes(), now)
                    .unwrap_or_else(|| panic!("key:{i} missing after reshard to {load_shards}"));
                assert_eq!(got.as_bytes(), format!("v{i}").as_bytes());
            }
            // And no key landed on the WRONG shard (a misroute would leave it unreadable on owner).
            let dbsize: usize = refs.iter().map(|s| s.len()).sum();
            assert_eq!(dbsize, total_keys, "no duplicate / stray key after reshard");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn no_manifest_loads_nothing() {
        let dir = temp_dir("empty");
        let mut d0: TestStore = ShardStore::new(1);
        let mut stores: Vec<&mut TestStore> = vec![&mut d0];
        // No save has happened: read_manifest is None, load_all loads nothing (start-empty).
        assert!(read_manifest(&dir).is_none());
        assert_eq!(
            load_all(&mut stores, &dir, UnixMillis(1), test_owner_shard),
            0
        );
        assert!(d0.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crash_mid_save_keeps_prior_snapshot() {
        let now = UnixMillis(1_000);
        let dir = temp_dir("crash");

        // First good save: shard 0 has key "old".
        let mut v1: TestStore = ShardStore::new(1);
        v1.insert_object(0, KvObj::from_bytes(b"old", b"v1", None));
        let e1 = save_shard_to_dir(&v1, 0, &dir, now).unwrap();
        write_manifest(&dir, 1, 100, vec![e1]).unwrap();

        // Simulate a CRASH MID-SAVE of a SECOND save: a new shard file is half-written (we
        // overwrite the committed file with a TRUNCATED/garbage body) but the manifest is NEVER
        // updated (the commit point never reached). The committed manifest still points at the
        // file with the OLD crc, so load detects the mismatch and treats it as no-snapshot for
        // that shard -> the prior good data is what a fresh boot would NOT load (it rejects the
        // torn file), which is the SAFE outcome: never corrupt data.
        let path = format::shard_path(&dir, 0);
        let mut bytes = format::read_file(&path).unwrap();
        // Half-write: truncate the body to simulate a partial flush before a crash.
        bytes.truncate(format::SHARD_HEADER_LEN + 2);
        format::write_file_atomic(&path, &bytes).unwrap();

        // The committed manifest is still v1 (crc of the OLD body). Load rejects the torn file.
        let manifest = read_manifest(&dir).expect("the prior manifest survives");
        assert_eq!(manifest.save_id, 1, "the prior manifest is still committed");
        let mut dst: TestStore = ShardStore::new(1);
        let loaded = load_shard_from_dir(&mut dst, &dir, &manifest.entries[0], now);
        assert_eq!(
            loaded, 0,
            "the torn (crashed) file is rejected, never corrupt-loaded"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A `tracing_subscriber::fmt` WRITER that appends every formatted event to a shared buffer, so a
    /// test can assert an expected log event actually FIRED (in-process, no global logger installed).
    #[derive(Clone)]
    struct CaptureWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CaptureWriter {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("capture buffer lock")
                .extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// THE #530 ACCEPTANCE TEST: a committed manifest at `FORMAT_VERSION + 1` (a downgrade / a
    /// failed-upgrade rollback -- an older binary asked to load a newer dump) makes the boot check
    /// (1) return a CLASSIFIED [`SnapshotLoadError::UnknownVersion`] rather than the old SILENT
    /// no-snapshot, and (2) emit a LOUD `tracing::error!`, so the all-data discard is never silent.
    #[test]
    fn newer_version_snapshot_fails_loud_not_silent_empty() {
        let dir = temp_dir("version-mismatch");
        // Write a committed manifest at a version NEWER than any this binary reads
        // (MANIFEST_VERSION_DELTA + 1 -- v2 is now a supported delta manifest, so the "unknown" bar
        // moved up one), encoded exactly as a real save would (magic + trailing CRC VALID -- a
        // well-formed dump this binary just cannot read).
        let manifest = Manifest {
            version: format::MANIFEST_VERSION_DELTA + 1,
            shards: 1,
            save_id: 1,
            save_unix_secs: 1_700_000_000,
            entries: vec![ShardManifestEntry {
                shard: 0,
                file: shard_file_name(0),
                keys: 5,
                crc: 0x1234_5678,
            }],
            deltas: Vec::new(),
        };
        format::write_file_atomic(&format::manifest_path(&dir), &manifest.encode())
            .expect("write the newer-version manifest");

        // Capture tracing output on THIS thread while the boot check runs (no global logger).
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_ansi(false)
            .with_writer(CaptureWriter(std::sync::Arc::clone(&buf)))
            .finish();
        let result =
            tracing::subscriber::with_default(subscriber, || check_snapshot_loadable(&dir));

        // (1) The load path returns a CLASSIFIED error -- NOT a silent empty start.
        assert_eq!(
            result,
            Err(SnapshotLoadError::UnknownVersion {
                found: format::MANIFEST_VERSION_DELTA + 1,
                supported: format::MANIFEST_VERSION_DELTA,
            }),
            "a newer-version dump is a classified error, not silent empty"
        );

        // (2) The LOUD error log FIRED: an ERROR-level event naming the unsupported version.
        let logged = String::from_utf8(buf.lock().expect("capture buffer lock").clone())
            .expect("captured log is utf8");
        assert!(
            logged.contains("ERROR"),
            "an ERROR-level event was emitted (never silent): {logged:?}"
        );
        assert!(
            logged.contains("unsupported format version"),
            "the version-mismatch log fired: {logged:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A clean data dir (no manifest) and a manifest at the SUPPORTED version both pass the boot check
    /// (`Ok`): the loud path is reserved strictly for a well-formed UNKNOWN version, so a normal boot
    /// is byte-unchanged.
    #[test]
    fn loadable_snapshot_and_empty_dir_pass_the_boot_check() {
        let dir = temp_dir("version-ok");
        // No manifest yet -> Ok (a fresh / empty data dir).
        assert_eq!(check_snapshot_loadable(&dir), Ok(()));

        // A committed manifest at the CURRENT version -> Ok (the per-shard load handles it normally).
        let manifest = Manifest {
            version: format::MANIFEST_VERSION_BASE,
            shards: 1,
            save_id: 1,
            save_unix_secs: 1,
            entries: vec![ShardManifestEntry {
                shard: 0,
                file: shard_file_name(0),
                keys: 1,
                crc: 0,
            }],
            deltas: Vec::new(),
        };
        format::write_file_atomic(&format::manifest_path(&dir), &manifest.encode())
            .expect("write a current-version manifest");
        assert_eq!(check_snapshot_loadable(&dir), Ok(()));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// #571 CURSOR STABILITY (the correctness crux): the CHUNKED dump ([`ShardDumpBuilder`] driving
    /// [`ShardStore::snapshot_chunk`]) stays CORRECT when the table is MUTATED BETWEEN chunks. The
    /// yielding save releases the store borrow between chunks, so a concurrent write CAN
    /// insert/delete/RESIZE (and, post-#570, resize a per-slot table) between chunks. This test
    /// simulates that writer: it drives the dump chunk by chunk and, between chunks, INSERTS brand-new
    /// keys (forcing table growth/resizes) and DELETES churn keys. Every key present for the WHOLE
    /// dump MUST be captured at least once (SCAN semantics, `scan_hash`-threshold cursor); the walk
    /// must not panic, lose a stable key, or corrupt the file. Keys created/deleted mid-dump may or
    /// may not appear (acceptable). This is what makes the yielding save safe.
    #[test]
    fn chunked_dump_survives_mutation_between_chunks() {
        let now = UnixMillis(1_000);
        let mut store: TestStore = ShardStore::new(2);

        // The STABLE set (db 0): present for the ENTIRE dump, so every one must survive the snapshot.
        let stable = 600usize;
        for i in 0..stable {
            let k = format!("stable:{i}");
            store.insert_object(
                0,
                KvObj::from_bytes(k.as_bytes(), format!("v{i}").as_bytes(), None),
            );
        }
        // CHURN keys (db 1) that exist at dump-start but get DELETED mid-dump.
        for i in 0..200usize {
            let k = format!("churn:{i}");
            store.insert_object(1, KvObj::from_bytes(k.as_bytes(), b"x", None));
        }

        // Drive the dump CHUNK BY CHUNK (small chunk so the walk spans MANY mutation windows),
        // mutating the store BETWEEN chunks exactly as a concurrent writer would during a yield.
        let databases = store.databases();
        let mut builder = ShardDumpBuilder::new();
        let mut cursor = SnapshotCursor::START;
        let mut chunk_no = 0u32;
        // A generous cap: the cursor strictly advances, so this loop terminates well within it (the
        // cap just fails loudly instead of hanging if a regression breaks cursor progress).
        while !cursor.is_done(databases) {
            assert!(chunk_no < 100_000, "the chunked dump must terminate");
            let (chunk, next) = store.snapshot_chunk(cursor, 32, now);
            builder.push_chunk(&chunk);
            cursor = next;
            // MUTATE between chunks (bounded to the first 40 chunks so the walk still drains): insert
            // NEW keys (grow + resize the db-0 slot tables) and delete churn keys from db 1.
            if chunk_no < 40 {
                for j in 0..16u32 {
                    let k = format!("fresh:{chunk_no}:{j}");
                    store.insert_object(0, KvObj::from_bytes(k.as_bytes(), b"n", None));
                }
                let target = format!("churn:{chunk_no}");
                store.remove_keys_where(|key| key == target.as_bytes());
            }
            chunk_no += 1;
        }
        let dump = builder.finish(0);

        // The sealed file's recorded CRC matches its body (no corruption from the interleaved writes).
        let body = format::split_shard_header(&dump.bytes, 0).expect("valid header");
        assert_eq!(format::crc32(body), dump.crc, "recorded crc matches body");

        // LOAD the snapshot; EVERY stable key must be present with its value intact (at-least-once).
        let mut dst: TestStore = ShardStore::new(2);
        load_body_all(&mut dst, body, now);
        for i in 0..stable {
            let k = format!("stable:{i}");
            let got = dst.read(0, k.as_bytes(), now).unwrap_or_else(|| {
                panic!("stable key {k} missing from a dump taken under concurrent mutation")
            });
            assert_eq!(
                got.as_bytes(),
                format!("v{i}").as_bytes(),
                "{k} value intact through a mutating dump"
            );
        }
    }

    /// #576 COW CORRECTNESS through a real DUMP + RELOAD: a [`dump_frozen_slots`] over a
    /// `begin_save` freeze is a per-shard POINT-IN-TIME. Overwriting + deleting keys on the LIVE
    /// store AFTER the freeze must NOT change the dumped file (deep-clone-on-COW isolated it), so
    /// the RELOAD holds every key's PRE-freeze value; the LIVE store holds the POST-write values.
    /// A shallow clone would have let a post-freeze overwrite/delete corrupt/free the dumped
    /// entry's pointee (torn value / crash).
    #[test]
    fn dump_frozen_slots_is_point_in_time_across_later_writes() {
        let now = UnixMillis(1_000);
        let mut store: TestStore = ShardStore::new(2);
        let n = 400usize;
        for i in 0..n {
            store.insert_object(
                0,
                KvObj::from_bytes(format!("k{i}").as_bytes(), format!("v{i}").as_bytes(), None),
            );
        }

        // FREEZE, then DUMP the frozen slots to file bytes exactly as the persist thread does.
        let frozen = store.begin_save();
        let dump = dump_frozen_slots(&frozen, 0, now);
        assert_eq!(
            dump.keys, n as u64,
            "the dump records the whole pre-freeze keyspace"
        );

        // MUTATE the LIVE store AFTER the freeze: overwrite the even keys, delete a quarter.
        for i in 0..n {
            let k = format!("k{i}");
            if i % 2 == 0 {
                store.insert_object(
                    0,
                    KvObj::from_bytes(k.as_bytes(), format!("NEW{i}").as_bytes(), None),
                );
            } else if i % 4 == 3 {
                store.delete(0, k.as_bytes(), now);
            }
        }
        drop(frozen);
        store.end_save();

        // The DUMP is the frozen point-in-time: reload it and assert EVERY key has its PRE-freeze
        // value and the file is not corrupt (CRC matches).
        let body = format::split_shard_header(&dump.bytes, 0).expect("valid header");
        assert_eq!(
            format::crc32(body),
            dump.crc,
            "recorded crc matches the body (no corruption)"
        );
        let mut reloaded: TestStore = ShardStore::new(2);
        let loaded = load_body_all(&mut reloaded, body, now);
        assert_eq!(
            loaded, n as u64,
            "the dump holds the whole pre-freeze keyspace"
        );
        for i in 0..n {
            let k = format!("k{i}");
            let got = reloaded
                .read(0, k.as_bytes(), now)
                .expect("pre-freeze key present in the dump");
            assert_eq!(
                got.as_bytes(),
                format!("v{i}").as_bytes(),
                "the dump has the PRE-freeze value for {k} (COW isolation)"
            );
        }

        // The LIVE store reflects the POST-write state (writes landed on the fresh COW copies).
        for i in 0..n {
            let k = format!("k{i}");
            if i % 2 == 0 {
                assert_eq!(
                    store
                        .read(0, k.as_bytes(), now)
                        .expect("overwritten key live")
                        .as_bytes(),
                    format!("NEW{i}").as_bytes()
                );
            } else if i % 4 == 3 {
                assert!(
                    store.read(0, k.as_bytes(), now).is_none(),
                    "deleted key gone from the live store"
                );
            } else {
                assert_eq!(
                    store
                        .read(0, k.as_bytes(), now)
                        .expect("untouched key live")
                        .as_bytes(),
                    format!("v{i}").as_bytes()
                );
            }
        }
    }
}
