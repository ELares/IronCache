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

/// The on-disk FORMAT VERSION. Bumped only on a breaking layout change; load rejects an
/// unknown version (treated as no-snapshot, start empty) rather than mis-parsing it.
pub const FORMAT_VERSION: u32 = 1;

/// The manifest file name within the data directory (the single COMMIT POINT, written LAST).
pub const MANIFEST_NAME: &str = "dump.manifest";

/// The per-shard snapshot file name for shard `n` within the data directory.
#[must_use]
pub fn shard_file_name(shard: u32) -> String {
    format!("dump-shard-{shard}.icss")
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

/// The committed MANIFEST: the single point that ties a set of per-shard files into ONE
/// consistent, loadable snapshot. Written + fsync'd LAST in a save, so a crash mid-save leaves
/// the prior good manifest (and the prior good files) intact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// The on-disk format version ([`FORMAT_VERSION`] at write time; load rejects an unknown one).
    pub version: u32,
    /// The number of shards this snapshot was taken from (the partition count at SAVE TIME). The
    /// loading node may have a DIFFERENT shard count (a reconfiguration); load handles this by
    /// RE-SHARDING -- each loading shard reads EVERY listed shard file and keeps only the keys it
    /// OWNS under the CURRENT shard count, using the router's `owner_shard` hash (see
    /// [`crate::load_shard_resharded`]). So this count is recorded for diagnostics + to drive that
    /// re-shard; the WITHIN-store re-hash on `insert_object` only places a key into its owning DB,
    /// NOT across shards, so the across-shard placement MUST come from the re-shard on load.
    pub shards: u32,
    /// A monotone SAVE ID (incremented per successful save). Informational / debugging; the
    /// commit ordering is the manifest rename, not this id.
    pub save_id: u64,
    /// The unix-time (SECONDS) of this save, sourced from the env Clock seam by the caller
    /// (ADR-0003: this module reads no clock). Reported by `LASTSAVE`.
    pub save_unix_secs: u64,
    /// The per-shard entries (file + key count + CRC), in shard order.
    pub entries: Vec<ShardManifestEntry>,
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
        let mut out = Vec::with_capacity(64 + self.entries.len() * 48);
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&self.version.to_le_bytes());
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
        if version != FORMAT_VERSION {
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
        if !r.is_done() {
            return None; // trailing slop in the body is malformed
        }
        Some(Manifest {
            version,
            shards,
            save_id,
            save_unix_secs,
            entries,
        })
    }
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
            version: FORMAT_VERSION,
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
        };
        let bytes = m.encode();
        let decoded = Manifest::decode(&bytes).expect("a well-formed manifest decodes");
        assert_eq!(decoded, m);
        assert_eq!(decoded.total_keys(), 17);
    }

    #[test]
    fn manifest_decode_rejects_torn_and_foreign() {
        let m = Manifest {
            version: FORMAT_VERSION,
            shards: 1,
            save_id: 1,
            save_unix_secs: 1,
            entries: vec![ShardManifestEntry {
                shard: 0,
                file: shard_file_name(0),
                keys: 1,
                crc: 9,
            }],
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
