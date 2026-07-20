// SPDX-License-Identifier: MIT OR Apache-2.0
//! The ON-DISK SNAPSHOT FORMAT (#58 persistence umbrella, #62 warm-restart): the
//! per-shard snapshot file (`dump-shard-<n>.icss`), the small commit MANIFEST
//! (`dump.manifest`), the CRC that detects a torn file, and the ATOMIC,
//! CRASH-SAFE write helpers (tmp -> fsync -> rename, then the manifest fsync'd LAST).
//!
//! ## Why two layers (per-shard files + one manifest)
//!
//! The store is shared-nothing thread-per-core (ADR-0002): each shard owns a PARTITION
//! of the keyspace on its own thread. So each shard writes its OWN file ON ITS OWN
//! THREAD with no cross-shard lock (the per-shard dump is driven by the forkless
//! `snapshot_chunk`, see [`crate::dump_shard_keyspace`]). The MANIFEST is the single
//! COMMIT POINT that ties a set of per-shard files into ONE loadable snapshot: it
//! records the format version, the shard count, and per-shard `(file, key count, CRC)`,
//! plus a monotone save id / timestamp the caller passes through the env seam (the store
//! reads no clock, ADR-0003).
//!
//! The snapshot is PER-SHARD-CONSISTENT but CROSS-SHARD FUZZY: each shard's file is a
//! consistent point-in-time view of THAT shard, but shards dump at slightly different
//! instants (no global lock / no fork-COW), so there is NO single global point-in-time.
//! This is acceptable for a cache; it is NOT a fork-COW point-in-time snapshot (Redis RDB).
//!
//! ## Crash-safety (the atomic manifest commit)
//!
//! A save writes EVERY shard file atomically FIRST (each: write `<file>.tmp` -> fsync ->
//! rename over `<file>`), and the manifest is written + fsync'd LAST. A crash MID-SAVE
//! therefore leaves the PRIOR good manifest pointing at the PRIOR good shard files: a
//! half-written new shard file (a leftover `.tmp`, or a renamed file the not-yet-written
//! manifest does not reference with a matching CRC) is simply ignored on the next load.
//! A torn shard file is caught by its CRC (the manifest's recorded CRC will not match the
//! file's recomputed CRC), and load treats a CRC mismatch as NO-SNAPSHOT (start empty),
//! never as corrupt-data.
//!
//! ## Determinism (ADR-0003)
//!
//! This module reads NO clock and NO RNG: the save id / save-unix-time is passed in by the
//! caller (sourced from the `ironcache-env` Clock seam at the serve layer). The CRC and the
//! byte layout are pure functions of the input.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// The magic at the head of EVERY snapshot file and the manifest: the ASCII `ICSS` (IronCache
/// Snapshot) tag, so a stray / foreign / truncated-to-zero file is rejected before any decode.
pub const MAGIC: [u8; 4] = *b"ICSS";

/// The on-disk FORMAT VERSION. Bumped only on a breaking layout change; the decode helpers reject an
/// unknown version rather than mis-parsing it, and the boot check ([`crate::check_snapshot_loadable`])
/// CLASSIFIES a well-formed-but-unknown version as [`SnapshotLoadError::UnknownVersion`] so a mismatch
/// fails LOUDLY instead of silently starting empty (#530).
pub const FORMAT_VERSION: u32 = 1;

/// The MANIFEST layout version written for a BASE-ONLY snapshot (no delta chain): byte-identical to
/// every pre-delta manifest (#676 Phase 1b), so an OLDER binary loads it unchanged. Equal to
/// [`FORMAT_VERSION`]; the shard-file header version and this base-manifest version move together.
pub const MANIFEST_VERSION_BASE: u32 = 1;

/// The MANIFEST layout version written when the snapshot carries a per-shard DELTA CHAIN (#676).
/// An older binary that does not understand deltas CLASSIFIES this as
/// [`SnapshotLoadError::UnknownVersion`] (fail-closed, #530) rather than loading the bases ALONE and
/// silently serving STALE data (it cannot apply the deltas). This binary reads BOTH versions; a
/// base-only save still writes [`MANIFEST_VERSION_BASE`] so nothing regresses until a delta is
/// actually produced.
pub const MANIFEST_VERSION_DELTA: u32 = 2;

/// A committed on-disk snapshot this binary CANNOT load, CLASSIFIED so the boot path can react
/// (surface it LOUDLY, and optionally FAIL CLOSED) instead of the pre-#530 behavior of silently
/// starting with an EMPTY keyspace. This is DISTINCT from a genuinely MISSING / TORN / FOREIGN file,
/// which stays the safe "start empty" degradation ([`Manifest::decode`] returns `None`); this error
/// is reserved for a WELL-FORMED snapshot whose FORMAT VERSION this binary does not understand.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotLoadError {
    /// The committed manifest is well-formed (magic + trailing CRC valid) but records a format
    /// version this binary does NOT support -- almost always a NEWER dump an OLDER binary is being
    /// asked to load (a DOWNGRADE, or a failed-upgrade ROLLBACK). Silently loading it would DISCARD
    /// the entire on-disk keyspace, so it is surfaced as this classified error instead.
    #[error(
        "on-disk snapshot format version {found} is not supported by this binary (it reads version \
         {supported}); refusing to silently start with an empty keyspace -- this is almost always \
         an older binary loading a newer dump (a downgrade or a failed-upgrade rollback)"
    )]
    UnknownVersion {
        /// The format version recorded in the committed manifest on disk.
        found: u32,
        /// The NEWEST manifest version THIS binary understands ([`MANIFEST_VERSION_DELTA`]; it reads
        /// every version up through it).
        supported: u32,
    },
}

/// The manifest file name within the data directory (the single COMMIT POINT, written LAST).
pub const MANIFEST_NAME: &str = "dump.manifest";

/// The per-shard snapshot file name for shard `n` within the data directory.
#[must_use]
pub fn shard_file_name(shard: u32) -> String {
    format!("dump-shard-{shard}.icss")
}

/// The per-shard DELTA file name for `shard` at chain position `delta_epoch` (#676): distinct from a
/// base file (`dump-shard-<n>.icss`) by the `-delta-<epoch>.icsd` suffix (and the `ICSD` magic
/// inside). Each delta round writes one such file per shard; the manifest names them in the chain.
#[must_use]
pub fn delta_file_name(shard: u32, delta_epoch: u64) -> String {
    format!("dump-shard-{shard}-delta-{delta_epoch}.icsd")
}

/// Parse a file name back into its `(shard, delta_epoch)` iff it is EXACTLY a
/// [`delta_file_name`] output; `None` for a base `.icss` file, the manifest, or any foreign name.
/// This is the strict inverse of [`delta_file_name`] (both integer components must round-trip), so
/// the orphan GC (#676, [`crate::gc_orphan_deltas`]) only ever considers reclaiming files it can
/// prove this crate wrote as deltas -- never a base file or an unrelated file that merely shares a
/// prefix.
#[must_use]
pub fn parse_delta_file_name(name: &str) -> Option<(u32, u64)> {
    let rest = name.strip_prefix("dump-shard-")?;
    let (shard, rest) = rest.split_once("-delta-")?;
    let epoch = rest.strip_suffix(".icsd")?;
    Some((shard.parse().ok()?, epoch.parse().ok()?))
}

/// The full path to the manifest within `dir`.
#[must_use]
pub fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(MANIFEST_NAME)
}

/// The full path to shard `shard`'s snapshot file within `dir`.
#[must_use]
pub fn shard_path(dir: &Path, shard: u32) -> PathBuf {
    dir.join(shard_file_name(shard))
}

/// A pure CRC-32 (IEEE 802.3 / zlib polynomial `0xEDB88320`, reflected) over `data`, computed
/// with a const-built 256-entry table. Hand-rolled (no third-party dep) and deterministic; it
/// detects a TORN shard file (a partial write a crash left, or bit-rot) so load rejects it as
/// no-snapshot rather than feeding corrupt bytes to the decoder. NOT a cryptographic checksum
/// (an adversary is out of scope for a local on-disk dump); it is an integrity check.
#[must_use]
pub fn crc32(data: &[u8]) -> u32 {
    // The reflected CRC-32 table, built once at compile time so the hot loop is a table lookup.
    const TABLE: [u32; 256] = build_crc_table();
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        let idx = ((crc ^ u32::from(b)) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[idx];
    }
    crc ^ 0xFFFF_FFFF
}

/// Build the reflected CRC-32 lookup table at compile time (a `const fn`, so it is evaluated
/// once during compilation and the runtime hot loop is a pure table lookup).
const fn build_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// One shard's entry in the manifest: which file holds the shard, how many live keys it
/// recorded, and the CRC of the file's RECORD BODY (everything after the file header), so load
/// can validate the file against the committed manifest before decoding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardManifestEntry {
    /// The shard index (0-based; matches the file name `dump-shard-<shard>.icss`).
    pub shard: u32,
    /// The file name (relative to the data directory).
    pub file: String,
    /// The number of live keys the shard dumped (across all its databases).
    pub keys: u64,
    /// The CRC-32 of the shard file's RECORD BODY (the bytes after the file header), recomputed
    /// on load and compared to this to detect a torn file.
    pub crc: u32,
}

/// One DELTA file's entry in a v2 manifest CHAIN (#676 Phase 1b): which file holds the delta, its
/// record counts, its body CRC (recomputed on load to catch a torn delta), and the two epochs that
/// BIND it -- `base_epoch` (the base snapshot it applies onto; must match the base's epoch) and
/// `delta_epoch` (its position in the shard's chain; the loader asserts these are strictly
/// increasing so a HOLE in the chain is detected).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaManifestEntry {
    /// The shard this delta belongs to (0-based).
    pub shard: u32,
    /// The delta file name (relative to the data directory).
    pub file: String,
    /// The number of PUT records in the delta.
    pub puts: u64,
    /// The number of TOMBSTONE records in the delta.
    pub tombstones: u64,
    /// The CRC-32 of the delta file's RECORD BODY (after its header), recomputed on load.
    pub crc: u32,
    /// The base snapshot epoch this delta applies ONTO: equals the manifest [`Manifest::save_id`] of
    /// the base generation (rejected on load if it does not match the loaded base's `save_id`).
    pub base_epoch: u64,
    /// This delta's own epoch, strictly increasing within a shard's chain (the loader's hole check).
    pub delta_epoch: u64,
}

/// The committed MANIFEST: the single point that ties a set of per-shard files into ONE
/// consistent, loadable snapshot. Written + fsync'd LAST in a save, so a crash mid-save leaves
/// the prior good manifest (and the prior good files) intact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// The on-disk manifest layout version, AUTHORITATIVE on encode: the writer sets
    /// [`MANIFEST_VERSION_BASE`] for a base-only save and [`MANIFEST_VERSION_DELTA`] for one with a
    /// delta chain, and [`Self::encode`] writes the v2 delta section iff this is
    /// [`MANIFEST_VERSION_DELTA`] (a `debug_assert` guards that it matches `deltas` presence). A
    /// base-only manifest (`MANIFEST_VERSION_BASE`, empty `deltas`) encodes byte-identically to a
    /// pre-delta manifest, so an older binary loads it unchanged.
    pub version: u32,
    /// The number of shards this snapshot was taken from (the partition count at SAVE TIME). The
    /// loading node may have a DIFFERENT shard count (a reconfiguration); load handles this by
    /// RE-SHARDING -- each loading shard reads EVERY listed shard file and keeps only the keys it
    /// OWNS under the CURRENT shard count, using the router's `owner_shard` hash (see
    /// [`crate::load_shard_resharded`]). So this count is recorded for diagnostics + to drive that
    /// re-shard; the WITHIN-store re-hash on `insert_object` only places a key into its owning DB,
    /// NOT across shards, so the across-shard placement MUST come from the re-shard on load.
    pub shards: u32,
    /// A monotone SAVE ID that ALSO serves as the BASE-GENERATION EPOCH a delta chain matches
    /// against (#676): the save-path convention is that `save_id` identifies the current BASE
    /// generation, so a [`DeltaManifestEntry::base_epoch`] equals it (there is exactly ONE per
    /// manifest, shared by every base entry -- no per-entry epoch field is needed). While the delta
    /// build ships dark every save is a base save, so this increments per save exactly as before; a
    /// later slice preserves it across delta-appends and advances it only when a fresh base is
    /// written (compaction), so the loader can reject a delta whose `base_epoch` does not match the
    /// base it is loading. Commit ordering is the manifest rename, not this id.
    pub save_id: u64,
    /// The unix-time (SECONDS) of this save, sourced from the env Clock seam by the caller
    /// (ADR-0003: this module reads no clock). Reported by `LASTSAVE`.
    pub save_unix_secs: u64,
    /// The per-shard entries (file + key count + CRC), in shard order.
    pub entries: Vec<ShardManifestEntry>,
    /// The per-shard DELTA CHAIN (#676 Phase 1b): the deltas that apply onto the base `entries`,
    /// in chain (apply) order. EMPTY for a base-only snapshot (the overwhelming default while the
    /// save-path delta build ships dark), in which case the manifest encodes byte-identically to a
    /// pre-delta (v1) manifest. Non-empty only once a save actually produces deltas, which flips the
    /// wire version to [`MANIFEST_VERSION_DELTA`]. The save-path MUST emit these in a DETERMINISTIC
    /// order (e.g. sorted by `(shard, delta_epoch)`, mirroring how `entries` is sorted by shard),
    /// since the manifest list-order is the authoritative chain-apply order (ADR-0003).
    pub deltas: Vec<DeltaManifestEntry>,
}

impl Manifest {
    /// The total live key count across every shard entry (introspection / tests).
    #[must_use]
    pub fn total_keys(&self) -> u64 {
        self.entries.iter().map(|e| e.keys).sum()
    }

    /// Serialize the manifest to its on-disk bytes: a fixed header (`MAGIC`, version, shards,
    /// save id, save time, entry count) then each entry (`shard`, `keys`, `crc`, name-len + name
    /// bytes), and a trailing CRC over everything before it (so a torn manifest is itself
    /// detected and treated as no-snapshot). All integers little-endian, matching the kvcodec.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        // `version` is authoritative: the writer sets BASE for a base-only save (byte-identical to a
        // pre-delta manifest) and DELTA for a chain; the delta section is written iff it is DELTA.
        // The guard catches a writer that set the version and the deltas inconsistently (a
        // hypothetical future version > DELTA is exempt, since its layout is not ours to constrain).
        let is_delta_version = self.version == MANIFEST_VERSION_DELTA;
        let has_deltas = !self.deltas.is_empty();
        debug_assert!(
            is_delta_version == has_deltas || self.version > MANIFEST_VERSION_DELTA,
            "manifest version must match delta presence"
        );
        let version = self.version;
        let mut out = Vec::with_capacity(64 + self.entries.len() * 48 + self.deltas.len() * 56);
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&version.to_le_bytes());
        out.extend_from_slice(&self.shards.to_le_bytes());
        out.extend_from_slice(&self.save_id.to_le_bytes());
        out.extend_from_slice(&self.save_unix_secs.to_le_bytes());
        #[allow(clippy::cast_possible_truncation)]
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            out.extend_from_slice(&e.shard.to_le_bytes());
            out.extend_from_slice(&e.keys.to_le_bytes());
            out.extend_from_slice(&e.crc.to_le_bytes());
            let name = e.file.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            out.extend_from_slice(&(name.len() as u32).to_le_bytes());
            out.extend_from_slice(name);
        }
        // v2 DELTA SECTION: present ONLY when deltas exist, so a base-only manifest carries no extra
        // bytes. Count-prefixed, then each entry; the trailing CRC below covers it as well.
        if version == MANIFEST_VERSION_DELTA {
            #[allow(clippy::cast_possible_truncation)]
            out.extend_from_slice(&(self.deltas.len() as u32).to_le_bytes());
            for d in &self.deltas {
                out.extend_from_slice(&d.shard.to_le_bytes());
                out.extend_from_slice(&d.puts.to_le_bytes());
                out.extend_from_slice(&d.tombstones.to_le_bytes());
                out.extend_from_slice(&d.crc.to_le_bytes());
                out.extend_from_slice(&d.base_epoch.to_le_bytes());
                out.extend_from_slice(&d.delta_epoch.to_le_bytes());
                let name = d.file.as_bytes();
                #[allow(clippy::cast_possible_truncation)]
                out.extend_from_slice(&(name.len() as u32).to_le_bytes());
                out.extend_from_slice(name);
            }
        }
        let crc = crc32(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decode a manifest from the bytes [`Self::encode`] produced, or `None` if the input is
    /// truncated, carries a wrong magic / unknown version, or its trailing CRC does not match
    /// (a torn manifest). TOTAL: never panics. A `None` is treated by load as NO-SNAPSHOT
    /// (start empty), the safe degradation.
    #[must_use]
    pub fn decode(buf: &[u8]) -> Option<Manifest> {
        // The trailing 4 bytes are the CRC over everything before them; validate it first.
        if buf.len() < 4 {
            return None;
        }
        let (body, crc_bytes) = buf.split_at(buf.len() - 4);
        let stored_crc = u32::from_le_bytes(crc_bytes.try_into().ok()?);
        if crc32(body) != stored_crc {
            return None;
        }
        let mut r = Reader::new(body);
        if r.take(4)? != MAGIC {
            return None;
        }
        let version = r.u32()?;
        if version != MANIFEST_VERSION_BASE && version != MANIFEST_VERSION_DELTA {
            return None; // an unknown version: do not guess at its layout.
        }
        let shards = r.u32()?;
        let save_id = r.u64()?;
        let save_unix_secs = r.u64()?;
        let entry_count = r.u32()? as usize;
        let mut entries = Vec::with_capacity(entry_count.min(4096));
        for _ in 0..entry_count {
            let shard = r.u32()?;
            let keys = r.u64()?;
            let crc = r.u32()?;
            let name_len = r.u32()? as usize;
            let name_bytes = r.take(name_len)?;
            let file = String::from_utf8(name_bytes.to_vec()).ok()?;
            entries.push(ShardManifestEntry {
                shard,
                file,
                keys,
                crc,
            });
        }
        // v2 DELTA SECTION: present only for MANIFEST_VERSION_DELTA; a v1 manifest ends after the
        // base entries (its `deltas` is empty), preserving exact v1 decode.
        let deltas = if version == MANIFEST_VERSION_DELTA {
            let delta_count = r.u32()? as usize;
            let mut ds = Vec::with_capacity(delta_count.min(4096));
            for _ in 0..delta_count {
                let shard = r.u32()?;
                let puts = r.u64()?;
                let tombstones = r.u64()?;
                let crc = r.u32()?;
                let base_epoch = r.u64()?;
                let delta_epoch = r.u64()?;
                let name_len = r.u32()? as usize;
                let name_bytes = r.take(name_len)?;
                let file = String::from_utf8(name_bytes.to_vec()).ok()?;
                ds.push(DeltaManifestEntry {
                    shard,
                    file,
                    puts,
                    tombstones,
                    crc,
                    base_epoch,
                    delta_epoch,
                });
            }
            ds
        } else {
            Vec::new()
        };
        if !r.is_done() {
            return None; // trailing slop in the body is malformed
        }
        Some(Manifest {
            version,
            shards,
            save_id,
            save_unix_secs,
            entries,
            deltas,
        })
    }
}

/// CLASSIFY a committed manifest's on-disk bytes as loadable-or-not WITHOUT a full decode (#530), so
/// the boot path can tell a snapshot it CANNOT read (a WELL-FORMED but UNKNOWN format version) apart
/// from a genuinely missing / torn / foreign file. Returns:
///
/// - `Ok(())` when `buf` is NOT a well-formed manifest (too short / wrong magic / a broken trailing
///   CRC -- all "start empty" degradations, unchanged from [`Manifest::decode`] returning `None`), OR
///   it IS a well-formed manifest recording a version this binary reads
///   ([`MANIFEST_VERSION_BASE`] or [`MANIFEST_VERSION_DELTA`]).
/// - `Err(`[`SnapshotLoadError::UnknownVersion`]`)` when `buf` IS a well-formed manifest (magic +
///   trailing CRC valid) whose recorded version is NEWER than any this binary reads (above
///   [`MANIFEST_VERSION_DELTA`]): a dump this binary must not silently discard.
///
/// The trailing CRC is validated FIRST so a torn manifest (whose version bytes are untrustworthy) is
/// treated as no-snapshot, never mis-classified as a version error. TOTAL: never panics.
pub fn classify_manifest_version(buf: &[u8]) -> Result<(), SnapshotLoadError> {
    // A torn / truncated manifest carries no trustworthy version: treat as no-snapshot (start empty).
    if buf.len() < 4 {
        return Ok(());
    }
    let (body, crc_bytes) = buf.split_at(buf.len() - 4);
    let Ok(crc_arr) = <[u8; 4]>::try_from(crc_bytes) else {
        return Ok(());
    };
    if crc32(body) != u32::from_le_bytes(crc_arr) {
        return Ok(()); // a torn manifest: the version bytes are not trustworthy.
    }
    // The magic + version are the first 8 bytes of the CRC-validated body.
    if body.len() < 8 || body[0..4] != MAGIC {
        return Ok(()); // foreign / too short: not our manifest, the safe start-empty path.
    }
    let Ok(ver_arr) = <[u8; 4]>::try_from(&body[4..8]) else {
        return Ok(());
    };
    let version = u32::from_le_bytes(ver_arr);
    if version == MANIFEST_VERSION_BASE || version == MANIFEST_VERSION_DELTA {
        return Ok(()); // a version this binary reads (base v1 or delta v2); the decode path handles it.
    }
    Err(SnapshotLoadError::UnknownVersion {
        found: version,
        supported: MANIFEST_VERSION_DELTA,
    })
}

/// The fixed header at the head of EACH per-shard snapshot file: `MAGIC`, the format version,
/// and the shard index. The records follow the header; the manifest's CRC covers ONLY the
/// record body (after this header), so the header is self-describing for a sanity check and the
/// body CRC catches a torn record stream.
pub const SHARD_HEADER_LEN: usize = 4 + 4 + 4;

/// Write the fixed per-shard file header into `out` (prefix of a shard snapshot file).
pub fn put_shard_header(out: &mut Vec<u8>, shard: u32) {
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&shard.to_le_bytes());
}

/// Validate + strip a shard file header, returning the RECORD BODY slice (the bytes the
/// kvcodec records were appended after). `None` if the header is short / wrong magic / unknown
/// version / wrong shard (so load treats the file as no-snapshot rather than mis-decoding it).
#[must_use]
pub fn split_shard_header(buf: &[u8], shard: u32) -> Option<&[u8]> {
    if buf.len() < SHARD_HEADER_LEN {
        return None;
    }
    if buf[0..4] != MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().ok()?);
    if version != FORMAT_VERSION {
        return None;
    }
    let file_shard = u32::from_le_bytes(buf[8..12].try_into().ok()?);
    if file_shard != shard {
        return None;
    }
    Some(&buf[SHARD_HEADER_LEN..])
}

/// One record in the shard file body: a `[u32 db LE][u32 record-len LE][encode_kvobj bytes]`.
/// The DB is carried EXPLICITLY because a [`ironcache_store::KvObj`] holds only the key, not the
/// logical database (KEYSPACE.md per-DB keyspace); load routes each record into its recorded db.
/// Appended by the dump, read back one at a time by load.
pub fn put_record(out: &mut Vec<u8>, db: u32, encoded: &[u8]) {
    out.extend_from_slice(&db.to_le_bytes());
    #[allow(clippy::cast_possible_truncation)]
    out.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    out.extend_from_slice(encoded);
}

/// Iterate the records in a shard file body, yielding each record's `(db, bytes)`.
/// TOTAL: a truncated db / length / record returns `None` from `next` and the iteration ends, so
/// a torn tail never panics (the body CRC already rejected a torn file before this is reached on
/// the load path; this is the belt-and-suspenders decode).
pub struct RecordReader<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> RecordReader<'a> {
    /// Start reading records from a shard file body (the slice [`split_shard_header`] returned).
    #[must_use]
    pub fn new(body: &'a [u8]) -> Self {
        RecordReader { body, pos: 0 }
    }

    /// The next record's `(db, bytes)`, or `None` at the end (or on a truncated record).
    pub fn next_record(&mut self) -> Option<(u32, &'a [u8])> {
        if self.pos == self.body.len() {
            return None;
        }
        let db_end = self.pos.checked_add(4)?;
        let db = u32::from_le_bytes(self.body.get(self.pos..db_end)?.try_into().ok()?);
        let len_end = db_end.checked_add(4)?;
        let len = u32::from_le_bytes(self.body.get(db_end..len_end)?.try_into().ok()?) as usize;
        let end = len_end.checked_add(len)?;
        let rec = self.body.get(len_end..end)?;
        self.pos = end;
        Some((db, rec))
    }
}

/// Write `bytes` to `path` ATOMICALLY and CRASH-SAFELY: write `<path>.tmp`, FSYNC the temp
/// file's contents to durable media, then RENAME it over `path` (an atomic replace on POSIX).
/// A crash before the rename leaves the prior `path` intact (only a stray `.tmp` remains); a
/// crash after the rename leaves the fully-written new file. The PARENT directory is also
/// fsync'd so the rename itself is durable (otherwise a crash could lose the directory entry
/// update even though the file bytes are on disk).
///
/// # Errors
///
/// Returns any underlying [`io::Error`] (create / write / fsync / rename / dir-fsync). The
/// caller treats a save error as a failed save (the prior snapshot stays the committed one).
pub fn write_file_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = tmp_path(path);
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?; // fsync the file contents before the rename commits it.
    }
    std::fs::rename(&tmp, path)?;
    fsync_dir(path);
    Ok(())
}

/// The `<path>.tmp` sibling used as the atomic-write staging file.
fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Best-effort fsync of `path`'s PARENT directory so a rename's directory-entry update is
/// durable. A failure here (e.g. a directory that cannot be opened for sync on some platforms)
/// is non-fatal: the file contents are already fsync'd, and the rename is atomic, so the worst
/// case is a slightly-less-durable directory entry, never corruption. Silently ignored.
fn fsync_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
    }
}

/// Read an entire file into a `Vec<u8>`, or `None` if it does not exist / cannot be read. The
/// load path treats a missing / unreadable file as no-snapshot (start empty), so this collapses
/// every read error to `None` rather than propagating it.
#[must_use]
pub fn read_file(path: &Path) -> Option<Vec<u8>> {
    let mut f = std::fs::File::open(path).ok()?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).ok()?;
    Some(buf)
}

/// A forward-only bounds-checked byte reader for the manifest decode (mirrors the kvcodec's
/// `Reader`: every read is bounds-checked and a short read returns `None`, never a panic).
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn is_done(&self) -> bool {
        self.pos == self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_known_vector() {
        // The canonical CRC-32 of ASCII "123456789" is 0xCBF43926 (the standard check value).
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        // The empty input is 0.
        assert_eq!(crc32(b""), 0);
        // A single bit flip changes the CRC (torn-file detection).
        assert_ne!(crc32(b"hello"), crc32(b"hellp"));
    }

    #[test]
    fn manifest_round_trips() {
        let m = Manifest {
            version: MANIFEST_VERSION_BASE,
            shards: 3,
            save_id: 42,
            save_unix_secs: 1_700_000_000,
            entries: vec![
                ShardManifestEntry {
                    shard: 0,
                    file: shard_file_name(0),
                    keys: 10,
                    crc: 0x1234_5678,
                },
                ShardManifestEntry {
                    shard: 1,
                    file: shard_file_name(1),
                    keys: 0,
                    crc: 0,
                },
                ShardManifestEntry {
                    shard: 2,
                    file: shard_file_name(2),
                    keys: 7,
                    crc: 0xDEAD_BEEF,
                },
            ],
            deltas: Vec::new(),
        };
        let bytes = m.encode();
        let decoded = Manifest::decode(&bytes).expect("a well-formed manifest decodes");
        assert_eq!(decoded, m);
        assert_eq!(decoded.total_keys(), 17);
    }

    #[test]
    fn manifest_decode_rejects_torn_and_foreign() {
        let m = Manifest {
            version: MANIFEST_VERSION_BASE,
            shards: 1,
            save_id: 1,
            save_unix_secs: 1,
            entries: vec![ShardManifestEntry {
                shard: 0,
                file: shard_file_name(0),
                keys: 1,
                crc: 9,
            }],
            deltas: Vec::new(),
        };
        let good = m.encode();

        // A flipped byte in the body breaks the trailing CRC -> None (torn manifest).
        let mut torn = good.clone();
        torn[10] ^= 0xFF;
        assert!(Manifest::decode(&torn).is_none());

        // A truncated manifest -> None.
        assert!(Manifest::decode(&good[..good.len() / 2]).is_none());

        // Empty -> None.
        assert!(Manifest::decode(b"").is_none());

        // Wrong magic (recompute a valid CRC so we exercise the magic check, not the CRC check).
        let mut foreign = good.clone();
        foreign[0] = b'X';
        let body_len = foreign.len() - 4;
        let new_crc = crc32(&foreign[..body_len]);
        foreign[body_len..].copy_from_slice(&new_crc.to_le_bytes());
        assert!(Manifest::decode(&foreign).is_none());
    }

    #[test]
    fn classify_manifest_version_flags_only_a_well_formed_unknown_version() {
        // A manifest at a version NEWER than any this binary reads (MANIFEST_VERSION_DELTA + 1 -- v2
        // is a supported delta manifest now, so the unknown bar moved up) is a downgrade / rollback:
        // well-formed (magic + trailing CRC valid) but unreadable, so it CLASSIFIES as UnknownVersion
        // rather than the silent no-snapshot `None` the old decode returned (#530).
        let newer = Manifest {
            version: MANIFEST_VERSION_DELTA + 1,
            shards: 1,
            save_id: 7,
            save_unix_secs: 1,
            entries: vec![ShardManifestEntry {
                shard: 0,
                file: shard_file_name(0),
                keys: 3,
                crc: 0xABCD,
            }],
            deltas: Vec::new(),
        };
        assert_eq!(
            classify_manifest_version(&newer.encode()),
            Err(SnapshotLoadError::UnknownVersion {
                found: MANIFEST_VERSION_DELTA + 1,
                supported: MANIFEST_VERSION_DELTA,
            }),
            "a well-formed newer-version manifest is a classified error, not silent empty"
        );

        // The CURRENT base version is loadable -> Ok (the normal decode path handles it).
        let current = Manifest {
            version: MANIFEST_VERSION_BASE,
            ..newer.clone()
        };
        assert_eq!(classify_manifest_version(&current.encode()), Ok(()));

        // A TORN manifest (its version bytes are untrustworthy) stays the safe start-empty path (Ok),
        // NOT mis-classified as a version error: the CRC is checked before the version is read.
        let mut torn = newer.encode();
        torn[10] ^= 0xFF; // corrupt a body byte so the trailing CRC no longer matches.
        assert_eq!(classify_manifest_version(&torn), Ok(()));

        // Foreign / too-short / empty buffers are likewise Ok (start empty, unchanged).
        assert_eq!(classify_manifest_version(b""), Ok(()));
        assert_eq!(classify_manifest_version(b"IC"), Ok(()));
    }

    #[test]
    fn manifest_v2_delta_chain_round_trips() {
        let m = Manifest {
            version: MANIFEST_VERSION_DELTA,
            shards: 2,
            save_id: 9,
            save_unix_secs: 1_700_000_100,
            entries: vec![ShardManifestEntry {
                shard: 0,
                file: shard_file_name(0),
                keys: 3,
                crc: 0x11,
            }],
            deltas: vec![
                DeltaManifestEntry {
                    shard: 0,
                    file: "dump-shard-0-delta-1.icsd".to_string(),
                    puts: 5,
                    tombstones: 2,
                    crc: 0xAABB,
                    base_epoch: 9,
                    delta_epoch: 10,
                },
                DeltaManifestEntry {
                    shard: 1,
                    file: "dump-shard-1-delta-1.icsd".to_string(),
                    puts: 0,
                    tombstones: 4,
                    crc: 0xCCDD,
                    base_epoch: 9,
                    delta_epoch: 11,
                },
            ],
        };
        let bytes = m.encode();
        // The wire version is the delta version, and this binary classifies it as loadable.
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            MANIFEST_VERSION_DELTA
        );
        assert_eq!(classify_manifest_version(&bytes), Ok(()));
        let decoded = Manifest::decode(&bytes).expect("a v2 delta manifest decodes");
        assert_eq!(decoded, m);
        assert_eq!(decoded.deltas.len(), 2);
    }

    #[test]
    fn base_only_manifest_stays_v1_and_omits_the_delta_section() {
        // A base-only manifest encodes at MANIFEST_VERSION_BASE with NO delta bytes, so an older
        // binary loads it unchanged (byte-compatible with a pre-delta manifest).
        let base = Manifest {
            version: MANIFEST_VERSION_BASE,
            shards: 1,
            save_id: 1,
            save_unix_secs: 1,
            entries: vec![ShardManifestEntry {
                shard: 0,
                file: shard_file_name(0),
                keys: 4,
                crc: 7,
            }],
            deltas: Vec::new(),
        };
        let bytes = base.encode();
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            MANIFEST_VERSION_BASE
        );
        let decoded = Manifest::decode(&bytes).expect("base-only decodes");
        assert!(decoded.deltas.is_empty());
        assert_eq!(decoded, base);
        // The same manifest promoted to carry a delta is STRICTLY LONGER: the section exists only
        // when deltas do.
        let mut with_delta = base.clone();
        with_delta.version = MANIFEST_VERSION_DELTA;
        with_delta.deltas.push(DeltaManifestEntry {
            shard: 0,
            file: "d.icsd".to_string(),
            puts: 1,
            tombstones: 0,
            crc: 1,
            base_epoch: 1,
            delta_epoch: 2,
        });
        assert!(
            with_delta.encode().len() > bytes.len(),
            "the delta section adds bytes only when present"
        );
    }

    #[test]
    fn v2_manifest_decode_rejects_torn_delta_section() {
        let m = Manifest {
            version: MANIFEST_VERSION_DELTA,
            shards: 1,
            save_id: 1,
            save_unix_secs: 1,
            entries: vec![ShardManifestEntry {
                shard: 0,
                file: shard_file_name(0),
                keys: 1,
                crc: 3,
            }],
            deltas: vec![DeltaManifestEntry {
                shard: 0,
                file: "d.icsd".to_string(),
                puts: 1,
                tombstones: 0,
                crc: 5,
                base_epoch: 1,
                delta_epoch: 2,
            }],
        };
        let good = m.encode();
        assert!(Manifest::decode(&good).is_some());
        // Truncating into the delta section -> None (no panic).
        assert!(Manifest::decode(&good[..good.len() - 8]).is_none());
        // A flipped byte anywhere in the body breaks the trailing CRC -> None.
        let mut torn = good.clone();
        let mid = torn.len() / 2;
        torn[mid] ^= 0xFF;
        assert!(Manifest::decode(&torn).is_none());
    }

    #[test]
    fn shard_header_round_trips_and_rejects_mismatch() {
        let mut buf = Vec::new();
        put_shard_header(&mut buf, 5);
        put_record(&mut buf, 0, b"abc");
        put_record(&mut buf, 7, b"defg");

        // The header validates for shard 5 and yields the record body.
        let body = split_shard_header(&buf, 5).expect("valid header");
        let mut rr = RecordReader::new(body);
        assert_eq!(rr.next_record(), Some((0u32, &b"abc"[..])));
        assert_eq!(rr.next_record(), Some((7u32, &b"defg"[..])));
        assert_eq!(rr.next_record(), None);

        // A wrong shard index is rejected (the manifest pairs a file with its shard).
        assert!(split_shard_header(&buf, 6).is_none());

        // A short / foreign buffer is rejected.
        assert!(split_shard_header(b"IC", 5).is_none());
        assert!(split_shard_header(b"XXXXxxxxyyyy", 5).is_none());
    }

    #[test]
    fn record_reader_tolerates_truncated_tail() {
        let mut buf = Vec::new();
        put_shard_header(&mut buf, 0);
        put_record(&mut buf, 0, b"complete");
        // Append a truncated record (a db prefix + a length promising 100 bytes, but no bytes).
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&100u32.to_le_bytes());
        let body = split_shard_header(&buf, 0).expect("valid header");
        let mut rr = RecordReader::new(body);
        assert_eq!(rr.next_record(), Some((0u32, &b"complete"[..])));
        // The truncated tail yields None (no panic), ending iteration.
        assert_eq!(rr.next_record(), None);
    }

    #[test]
    fn write_file_atomic_then_read_back() {
        let dir = std::env::temp_dir().join(format!("icpersist-fmt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("atomic.bin");
        let payload = b"durable bytes".to_vec();
        write_file_atomic(&path, &payload).expect("atomic write succeeds");
        assert_eq!(read_file(&path).as_deref(), Some(payload.as_slice()));
        // No stray .tmp remains after a successful rename.
        assert!(read_file(&tmp_path(&path)).is_none());
        std::fs::remove_dir_all(&dir).ok();
    }
}
