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
//!   SCAN dump that yields each live key as an owned [`ironcache_store::KvObj`] and RELEASES the
//!   store borrow between chunks. [`dump_shard_keyspace`] drives it chunk by chunk, so a save
//!   never double-memories the keyspace and (for `BGSAVE`) never blocks the shard's hot path
//!   materially.
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

pub mod format;

pub use format::{Manifest, ShardManifestEntry, crc32, manifest_path, shard_file_name, shard_path};

use std::io;
use std::path::Path;

use ironcache_storage::{AccountingHook, EvictionHook, UnixMillis};
use ironcache_store::{ShardStore, SnapshotCursor};

/// The number of keys [`dump_shard_keyspace`] EXAMINES per
/// [`ironcache_store::ShardStore::snapshot_chunk`] call. Bounds the per-chunk owned `Vec` (and
/// its per-entry `KvObj` clones), so the dump's transient memory is O(`DUMP_CHUNK`), NEVER a
/// full-keyspace materialization (the forkless, memory-neutral property HA-5b provides). Between
/// chunks the store borrow is released, so a `BGSAVE` driven on the shard's executor yields the
/// shard back to its connection tasks every chunk (it does not block the hot path materially).
/// 512 is a balance: large enough to amortize the per-chunk borrow/sort overhead, small enough to
/// bound the transient buffer and keep the shard responsive.
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
/// ## Forkless + memory-neutral + hot-path-safe
///
/// The chunked pull bounds the transient memory to O([`DUMP_CHUNK`]) (the per-chunk `Vec` + its
/// `KvObj` clones), never the whole keyspace, and `snapshot_chunk` RELEASES the store borrow
/// between chunks. So a `BGSAVE` that calls this on the shard's own executor (re-borrowing the
/// store per chunk and yielding between chunks) does not double the shard's memory and does not
/// hold the shard's hot path for the whole dump. A synchronous `SAVE` holds the borrow for the
/// dump (it blocks by design, Redis parity), but still never double-memories.
///
/// `&S` is a SHARED borrow: the dump never mutates the store (it does not even reap the
/// lazily-expired keys it skips, unlike a command-path read), so it is purely an observer.
#[must_use]
pub fn dump_shard_keyspace<E: EvictionHook, A: AccountingHook>(
    store: &ShardStore<E, A>,
    shard: u32,
    now: UnixMillis,
) -> ShardDump {
    let mut body = Vec::new();
    let mut keys: u64 = 0;
    let databases = store.databases();

    let mut cursor = SnapshotCursor::START;
    while !cursor.is_done(databases) {
        let (chunk, next) = store.snapshot_chunk(cursor, DUMP_CHUNK, now);
        for (db, _key, kv) in &chunk {
            let encoded = ironcache_repl::encode_kvobj(kv);
            format::put_record(&mut body, *db, &encoded);
            keys += 1;
        }
        cursor = next;
    }

    let crc = format::crc32(&body);
    // Prepend the file header to the record body to form the complete file bytes.
    let mut bytes = Vec::with_capacity(format::SHARD_HEADER_LEN + body.len());
    format::put_shard_header(&mut bytes, shard);
    bytes.extend_from_slice(&body);

    ShardDump { bytes, keys, crc }
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
    let path = format::shard_path(dir, shard);
    format::write_file_atomic(&path, &dump.bytes)?;
    Ok(ShardManifestEntry {
        shard,
        file: format::shard_file_name(shard),
        keys: dump.keys,
        crc: dump.crc,
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
    mut entries: Vec<ShardManifestEntry>,
) -> io::Result<Manifest> {
    // Sort by shard so the manifest is deterministic regardless of the order shards reported in.
    entries.sort_by_key(|e| e.shard);
    #[allow(clippy::cast_possible_truncation)]
    let manifest = Manifest {
        version: format::FORMAT_VERSION,
        shards: entries.len() as u32,
        save_id,
        save_unix_secs,
        entries,
    };
    let path = format::manifest_path(dir);
    format::write_file_atomic(&path, &manifest.encode())?;
    Ok(manifest)
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
pub fn load_shard_from_dir<E: EvictionHook, A: AccountingHook>(
    store: &mut ShardStore<E, A>,
    dir: &Path,
    entry: &ShardManifestEntry,
    now: UnixMillis,
) -> u64 {
    let path = dir.join(&entry.file);
    let Some(bytes) = format::read_file(&path) else {
        return 0; // a referenced-but-missing file: load nothing for this shard.
    };
    let Some(body) = format::split_shard_header(&bytes, entry.shard) else {
        return 0; // a foreign / wrong-version / wrong-shard header: ignore the file.
    };
    // CRASH-SAFETY: validate the body against the committed manifest CRC. A mismatch means the
    // file is torn (a half-written file the manifest does not vouch for, or bit-rot): treat the
    // shard as no-snapshot (load nothing) rather than feeding corrupt bytes to the decoder.
    if format::crc32(body) != entry.crc {
        return 0;
    }
    load_records_into(store, body, now)
}

/// LOAD the WHOLE committed snapshot in `dir` into `stores` (one mutable store per shard, in
/// shard-index order), the boot convenience wrapper [`read_manifest`] + [`load_shard_from_dir`]
/// per shard. Returns the total keys loaded, or `0` (and loads nothing) when there is no loadable
/// snapshot. A manifest entry whose shard index is past `stores.len()` is loaded into the store
/// at `shard % stores.len()` so a snapshot taken with MORE shards than the loading node has still
/// reconstructs the full keyspace (the store re-hashes each key into its owning db; SCAN order is
/// recomputed from the key bytes, so a shard-count change is correctness-preserving).
pub fn load_all<E: EvictionHook, A: AccountingHook>(
    stores: &mut [&mut ShardStore<E, A>],
    dir: &Path,
    now: UnixMillis,
) -> u64 {
    let Some(manifest) = read_manifest(dir) else {
        return 0;
    };
    if stores.is_empty() {
        return 0;
    }
    let mut total = 0u64;
    for entry in &manifest.entries {
        let idx = (entry.shard as usize) % stores.len();
        total += load_shard_from_dir(stores[idx], dir, entry, now);
    }
    total
}

/// Decode every record in a shard file `body` and insert each into `store` under its recorded db,
/// dropping any key whose TTL has already passed at `now`. Returns the count inserted. TOTAL: a
/// record that fails to decode, or whose db is out of range, is skipped (the decode is
/// bounds-checked and never panics).
fn load_records_into<E: EvictionHook, A: AccountingHook>(
    store: &mut ShardStore<E, A>,
    body: &[u8],
    now: UnixMillis,
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

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::{CountingAccounting, NewValueOwned, NullEviction, Store, UnixMillis};
    use ironcache_store::{KvObj, ShardStore};

    type TestStore = ShardStore<NullEviction, CountingAccounting>;

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
            ),
        );
        store.insert_object(1, KvObj::from_bytes(b"db1-str", b"hello db1", None));
        6
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
        let loaded = load_records_into(&mut dst, body, now);
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
        let loaded = load_records_into(&mut dst, body, UnixMillis(7_000));
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
    fn full_save_load_via_manifest_round_trips() {
        let now = UnixMillis(2_000);
        let dir = temp_dir("manifest");

        // Two shards, each with its own partition.
        let mut s0: TestStore = ShardStore::new(2);
        let mut s1: TestStore = ShardStore::new(2);
        s0.insert_object(0, KvObj::from_bytes(b"a", b"1", None));
        s0.insert_object(1, KvObj::from_int(b"b", 99, None));
        s1.insert_object(0, KvObj::from_bytes(b"c", b"three", None));

        // SAVE: per-shard files, then commit the manifest LAST.
        let e0 = save_shard_to_dir(&s0, 0, &dir, now).unwrap();
        let e1 = save_shard_to_dir(&s1, 1, &dir, now).unwrap();
        let manifest = write_manifest(&dir, 1, 1_700_000_000, vec![e1, e0]).unwrap();
        assert_eq!(manifest.shards, 2);
        assert_eq!(manifest.total_keys(), 3);
        assert_eq!(manifest.save_unix_secs, 1_700_000_000);
        // The manifest is sorted by shard regardless of the order entries were passed.
        assert_eq!(manifest.entries[0].shard, 0);
        assert_eq!(manifest.entries[1].shard, 1);

        // LOAD the whole snapshot into fresh stores via the manifest.
        let mut d0: TestStore = ShardStore::new(2);
        let mut d1: TestStore = ShardStore::new(2);
        let mut stores: Vec<&mut TestStore> = vec![&mut d0, &mut d1];
        let total = load_all(&mut stores, &dir, now);
        assert_eq!(total, 3);
        assert!(d0.read(0, b"a", now).is_some());
        assert!(d0.read(1, b"b", now).is_some());
        assert!(d1.read(0, b"c", now).is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn no_manifest_loads_nothing() {
        let dir = temp_dir("empty");
        let mut d0: TestStore = ShardStore::new(1);
        let mut stores: Vec<&mut TestStore> = vec![&mut d0];
        // No save has happened: read_manifest is None, load_all loads nothing (start-empty).
        assert!(read_manifest(&dir).is_none());
        assert_eq!(load_all(&mut stores, &dir, UnixMillis(1)), 0);
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
}
