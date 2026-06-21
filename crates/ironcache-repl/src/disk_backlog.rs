// SPDX-License-Identifier: MIT OR Apache-2.0
//! The DISK-BACKED (spillable) replication backlog (HA-7e): a bounded, crash-safe on-disk
//! extension of the in-memory [`crate::observer::ReplRing`] resume window, so a reconnecting
//! replica that has fallen behind the IN-MEMORY ring can still catch up INCREMENTALLY from disk
//! instead of being forced into a full (chunked) snapshot re-sync.
//!
//! ## Why (the efficiency it buys, not a correctness fix)
//!
//! The in-memory ring retains the tail in the bounded window `(acked, head]`; when it overflows
//! `cap` the OLDEST op is evicted and the primary can no longer serve a replica resuming from
//! before it -- the MVP "full-resync-on-gap" policy ([`crate::observer`]). That is CORRECT (the
//! replica always converges) but re-ships the WHOLE keyspace on every gap. This module WIDENS the
//! incremental-resync window: an op the ring would have dropped is first SPILLED to a bounded
//! on-disk segment, so a replica behind the ring but within the on-disk range catches up by
//! replaying disk ops then handing off to the live in-memory stream. A replica behind even the
//! on-disk range falls back to the full snapshot -- EXACTLY today's behavior, just a wider window.
//!
//! ## Crash-safety + corruption tolerance (mirrors `ironcache-persist`)
//!
//! Each segment file is written ATOMICALLY (tmp -> fsync -> rename, parent dir fsync'd), carries a
//! self-describing header (magic / version / first+last offset) and a trailing CRC-32 over its
//! record body, and is sealed once (append-by-rewrite of a bounded in-memory buffer, never a
//! partial in-place append). On read a header / CRC / continuity mismatch makes that segment a
//! BACKLOG MISS (the replica falls back to the full snapshot) -- a torn segment is NEVER served as
//! data. This reuses the SAME `ironcache-persist::format::crc32` + the same tmp->fsync->rename
//! discipline the snapshot persistence proved.
//!
//! ## The single bound + eviction (oldest-segment-first)
//!
//! The total on-disk backlog is bounded by `max_bytes`. When sealing a new segment would push the
//! total past the bound, the OLDEST segment file is deleted (its ops fall out of the resume
//! window). The retained disk range stays CONTIGUOUS: segments cover one unbroken offset run
//! `(disk_floor, in_mem_oldest]` with no holes, so the disk->memory handoff is gap-free.
//!
//! ## Default-off / zero-size = byte-identical to in-memory-only
//!
//! `max_bytes == 0` (or no `data_dir`) means DISABLED: [`DiskBacklog::open`] returns `None`, the
//! ring never spills, and the hot replication path is byte-identical to the pre-HA-7e in-memory
//! -only behavior. The disk backlog is engaged ONLY when an operator sets a non-zero size.
//!
//! ## Determinism (ADR-0003)
//!
//! This module reads NO clock and NO RNG (file I/O is not a determinism-gated seam; only the
//! Clock/RNG go through `ironcache-env`). It is pure `std::fs` + the reused safe CRC/codec, exactly
//! like `ironcache-persist`. The byte layout + CRC are pure functions of the input.

#![allow(clippy::doc_markdown)]

use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};

use crate::cursor::ReplOffset;
use crate::observer::StreamOp;

/// The magic at the head of every backlog segment file: ASCII `ICRB` (IronCache Repl Backlog), so a
/// stray / foreign / truncated-to-zero file is rejected before any decode.
const MAGIC: [u8; 4] = *b"ICRB";

/// The on-disk segment FORMAT VERSION. Bumped only on a breaking layout change; a segment with an
/// unknown version is treated as a backlog miss (full snapshot), never mis-parsed.
const FORMAT_VERSION: u32 = 1;

/// The fixed segment header: `MAGIC`, version, first offset, last offset, record count. The records
/// follow; the trailing CRC covers the record body (the bytes after this header, before the CRC).
const HEADER_LEN: usize = 4 + 4 + 8 + 8 + 8;

/// The tag byte for a `StreamOp::Put` record on disk.
const TAG_PUT: u8 = 0;
/// The tag byte for a `StreamOp::Del` record on disk.
const TAG_DEL: u8 = 1;

/// The bookkeeping for one sealed on-disk segment file (the file holds a contiguous offset run).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentMeta {
    /// The lowest offset stored in the segment (strictly increasing across segments).
    first: ReplOffset,
    /// The highest offset stored in the segment. The next segment's `first` is `last + 1`.
    last: ReplOffset,
    /// The on-disk file name (relative to the backlog dir).
    file: String,
    /// The file's byte length (counted toward the total-size bound).
    bytes: u64,
}

/// The DISK-BACKED replication backlog: a bounded, contiguous run of sealed segment files under a
/// `data_dir`-rooted directory, holding the offset window the in-memory ring has spilled past its
/// `cap`. See the module docs for the crash-safety + handoff guarantees.
#[derive(Debug)]
pub struct DiskBacklog {
    /// The backlog directory (created under `data_dir`; segment files live here).
    dir: PathBuf,
    /// The total on-disk byte budget. Sealing past it evicts the oldest segment(s).
    max_bytes: u64,
    /// The sealed segments, OLDEST first, covering one contiguous offset run with no holes.
    segments: VecDeque<SegmentMeta>,
    /// The next segment's sequence number (monotonic; names the file, never reused).
    next_seq: u64,
    /// The running total of `segments`' file bytes (the value bounded by `max_bytes`).
    total_bytes: u64,
}

impl DiskBacklog {
    /// The directory name (under `data_dir`) the backlog segments live in.
    pub const DIR_NAME: &'static str = "repl-backlog";

    /// OPEN (or create) a disk backlog rooted at `<data_dir>/repl-backlog`, bounded at `max_bytes`.
    ///
    /// Returns `None` when DISABLED (`max_bytes == 0`): the caller then runs the in-memory-only
    /// path, byte-identical to pre-HA-7e. Returns `None` (logged degradation, never an error the
    /// caller must handle) if the directory cannot be created -- replication still runs, just
    /// without the wider window (the full-snapshot fallback is unaffected).
    ///
    /// A FRESH backlog: any pre-existing segment files from a prior process are PURGED on open (the
    /// offsets are stale relative to this process's fresh `head` at `ReplOffset::ZERO`; serving them
    /// would be a continuity violation). So the disk backlog never SURVIVES a restart as resumable
    /// state -- it is a within-process spill buffer; a replica reconnecting across a primary restart
    /// sees a changed `ReplId` and full-syncs regardless (the existing HA-7b identity rule).
    #[must_use]
    pub fn open(data_dir: &Path, max_bytes: u64) -> Option<Self> {
        if max_bytes == 0 {
            return None; // disabled: in-memory-only, byte-identical.
        }
        let dir = data_dir.join(Self::DIR_NAME);
        if std::fs::create_dir_all(&dir).is_err() {
            // The backlog dir could not be created: degrade to in-memory-only (the full-snapshot
            // fallback is unaffected). The safe degradation is the contract; no logger in this crate.
            return None;
        }
        let mut backlog = DiskBacklog {
            dir,
            max_bytes,
            segments: VecDeque::new(),
            next_seq: 0,
            total_bytes: 0,
        };
        backlog.purge_all();
        Some(backlog)
    }

    /// The lowest offset still recoverable FROM DISK (the front of the on-disk window), or `None`
    /// when the disk backlog is empty. A replica resuming from `from` can be served incrementally
    /// (memory + disk) iff `from + 1 >= this` (when non-`None`).
    #[must_use]
    pub fn oldest_offset(&self) -> Option<ReplOffset> {
        self.segments.front().map(|s| s.first)
    }

    /// The highest offset stored on disk, or `None` when empty. Invariant: this is exactly one
    /// below the in-memory ring's oldest retained offset (the disk + memory ranges are contiguous),
    /// so a replay that ends here hands off to the in-memory stream with NO gap and NO overlap.
    #[must_use]
    pub fn newest_offset(&self) -> Option<ReplOffset> {
        self.segments.back().map(|s| s.last)
    }

    /// The number of sealed segments retained (introspection / tests).
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// The total on-disk byte usage (introspection / tests); always `<= max_bytes` after a seal
    /// (a single segment larger than the bound is the one allowed exception, see [`Self::spill`]).
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// SPILL a contiguous batch of evicted ops (in strict offset order, oldest first) into a new
    /// sealed segment. Called by the ring when it evicts ops past `cap`: the ops are about to leave
    /// the in-memory window, so they are durably appended to disk FIRST, widening the resume window.
    ///
    /// CONTINUITY (the correctness crux): `ops` MUST be offset-contiguous (each `offset == prev + 1`)
    /// AND begin exactly one past the current [`Self::newest_offset`] (or be the first spill). The
    /// caller (the ring's eviction path) preserves this because it spills the SINGLE oldest op as it
    /// is evicted, in order. A non-contiguous batch is REFUSED (returns `Err`) rather than written:
    /// a hole on disk would corrupt a later incremental resume, so we never persist one.
    ///
    /// On success the segment is fsync'd + renamed into place, recorded, and the oldest segment(s)
    /// are evicted if the total now exceeds `max_bytes`. An I/O error returns `Err` and writes
    /// nothing durable (the caller treats a failed spill as "this op is lost from the disk window",
    /// which only NARROWS the window -- never a correctness loss, the full-snapshot fallback stands).
    ///
    /// # Errors
    /// Returns [`SpillError::Discontiguous`] if `ops` is empty / non-contiguous / does not abut the
    /// current newest offset, or [`SpillError::Io`] on any underlying file error.
    pub fn spill(&mut self, ops: &[StreamOp]) -> Result<(), SpillError> {
        if ops.is_empty() {
            return Err(SpillError::Discontiguous);
        }
        // Verify the batch is internally contiguous AND abuts the current newest disk offset, so the
        // on-disk run stays one unbroken sequence (the handoff invariant). NEVER write a hole.
        let first = ops[0].offset();
        if let Some(newest) = self.newest_offset() {
            if first.0 != newest.0 + 1 {
                return Err(SpillError::Discontiguous);
            }
        }
        for w in ops.windows(2) {
            if w[1].offset().0 != w[0].offset().0 + 1 {
                return Err(SpillError::Discontiguous);
            }
        }
        let last = ops[ops.len() - 1].offset();

        let bytes = encode_segment(first, last, ops);
        let seq = self.next_seq;
        let file = segment_file_name(seq);
        let path = self.dir.join(&file);
        write_file_atomic(&path, &bytes).map_err(SpillError::Io)?;

        self.next_seq += 1;
        let len = bytes.len() as u64;
        self.total_bytes += len;
        self.segments.push_back(SegmentMeta {
            first,
            last,
            file,
            bytes: len,
        });
        self.evict_to_bound();
        Ok(())
    }

    /// Read up to `max` ops with offset strictly greater than `cursor`, in offset order, from the
    /// on-disk segments. Used by the resume path when a replica's cursor is below the in-memory
    /// window but within the on-disk range: it replays disk ops until its cursor reaches the
    /// in-memory oldest retained, then the live in-memory stream takes over (no gap, no dup).
    ///
    /// A torn / corrupt / continuity-broken segment encountered mid-read is treated as a BACKLOG
    /// MISS: this returns whatever VALID, CONTIGUOUS ops it read BEFORE the bad segment (possibly
    /// empty). The caller detects the resulting offset shortfall (the next needed op is still
    /// missing) and falls back to the full snapshot -- a corrupt segment is never served as data.
    ///
    /// SYNCHRONOUS (no `.await`): the caller reads this batch under no borrow and then awaits the
    /// sends, exactly like the in-memory [`crate::observer::ReplRing::ops_after`] discipline.
    #[must_use]
    pub fn ops_after(&self, cursor: ReplOffset, max: usize) -> Vec<StreamOp> {
        let mut out = Vec::new();
        if max == 0 {
            return out;
        }
        // `expected` tracks the next offset we require, so a corrupt / non-contiguous segment ends
        // the read at a clean boundary (we only ever return a gap-free prefix above `cursor`).
        let mut expected: Option<u64> = None;
        for seg in &self.segments {
            if out.len() >= max {
                break;
            }
            // Skip whole segments entirely below the cursor (their ops are all duplicates).
            if seg.last.0 <= cursor.0 {
                continue;
            }
            let path = self.dir.join(&seg.file);
            let Some(ops) = read_segment(&path, seg.first, seg.last) else {
                break; // torn / missing segment: stop; the caller's shortfall -> full snapshot.
            };
            for op in ops {
                if out.len() >= max {
                    break;
                }
                let off = op.offset().0;
                if off <= cursor.0 {
                    continue; // already past this op (a duplicate below the resume point).
                }
                match expected {
                    None => expected = Some(off + 1),
                    Some(exp) => {
                        if off != exp {
                            return out; // a hole: return the contiguous prefix, force fallback.
                        }
                        expected = Some(off + 1);
                    }
                }
                out.push(op);
            }
        }
        out
    }

    /// Whether the disk backlog can serve a replica resuming from acked offset `from`: the next op
    /// it needs (`from + 1`) is within the on-disk range `[oldest, newest]`. False when the disk
    /// backlog is empty or `from + 1` predates even the oldest disk segment (-> full snapshot).
    #[must_use]
    pub fn can_serve_from(&self, from: ReplOffset) -> bool {
        match (self.oldest_offset(), self.newest_offset()) {
            (Some(oldest), Some(newest)) => from.next().0 >= oldest.0 && from.next().0 <= newest.0,
            _ => false,
        }
    }

    /// Drop every retained segment whose ops are at or below `cut` (a fresh snapshot cut / a replica
    /// ack the whole-disk-window can be pruned through). Keeps the on-disk window from holding ops no
    /// replica can still need. Only WHOLE segments below the cut are removed (a partial segment stays
    /// so the run stays contiguous); a half-covered segment's stale ops are harmless (they are
    /// below any resume point and `ops_after`'s `cursor` filter skips them).
    pub fn prune_through(&mut self, cut: ReplOffset) {
        while self.segments.front().is_some_and(|s| s.last.0 <= cut.0) {
            self.drop_oldest();
        }
    }

    /// Evict the oldest segment(s) until the total on-disk bytes are within `max_bytes`. A single
    /// segment larger than the whole bound is KEPT (evicting it would empty the backlog and a
    /// zero-capacity-with-a-too-big-segment state cannot make progress); the next seal then evicts
    /// it once a second segment exists. This keeps the retained run contiguous (always drop the
    /// FRONT) and bounded.
    fn evict_to_bound(&mut self) {
        while self.total_bytes > self.max_bytes && self.segments.len() > 1 {
            self.drop_oldest();
        }
    }

    /// Drop + delete the oldest segment file, updating the byte total. The remaining run stays
    /// contiguous (the new front's `first` is the old front+1's `first`). A delete error is logged
    /// and ignored (the worst case is a stale file lingering on disk, never a served-corruption).
    fn drop_oldest(&mut self) {
        if let Some(seg) = self.segments.pop_front() {
            self.total_bytes = self.total_bytes.saturating_sub(seg.bytes);
            let path = self.dir.join(&seg.file);
            // A delete error is ignored: the worst case is a stale segment file lingering on disk
            // (it is no longer in `segments`, so it is never read / served), never a corruption.
            let _ = std::fs::remove_file(&path);
        }
    }

    /// Purge ALL segment files (open-time reset): delete every `*.icrb` in the backlog dir and clear
    /// the in-memory bookkeeping. Best-effort; a file that cannot be removed is logged and skipped
    /// (it is simply ignored, since this process's segments are named by a fresh `next_seq` from 0).
    fn purge_all(&mut self) {
        if let Ok(rd) = std::fs::read_dir(&self.dir) {
            for entry in rd.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == SEGMENT_EXT) {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        self.segments.clear();
        self.total_bytes = 0;
        self.next_seq = 0;
    }
}

/// The error a [`DiskBacklog::spill`] can return.
#[derive(Debug)]
pub enum SpillError {
    /// The batch was empty, internally non-contiguous, or did not abut the current newest offset.
    /// The ring refuses to write a hole on disk (it would corrupt a later incremental resume).
    Discontiguous,
    /// An underlying file I/O error (create / write / fsync / rename). A failed spill only NARROWS
    /// the disk window; the full-snapshot fallback is unaffected.
    Io(io::Error),
}

/// The extension every backlog segment file carries.
const SEGMENT_EXT: &str = "icrb";

/// The segment file name for sequence `seq` (zero-padded so a directory listing sorts in order).
fn segment_file_name(seq: u64) -> String {
    format!("seg-{seq:020}.{SEGMENT_EXT}")
}

/// Encode a sealed segment: the fixed header (`MAGIC`, version, first, last, count) then each op as
/// a record, then a trailing CRC-32 over the record body (the bytes after the header). The CRC
/// covers ONLY the body so a torn record stream is caught; the header is self-describing for the
/// continuity check on read.
fn encode_segment(first: ReplOffset, last: ReplOffset, ops: &[StreamOp]) -> Vec<u8> {
    let mut body = Vec::new();
    for op in ops {
        put_op_record(&mut body, op);
    }
    let crc = crc32(&body);

    let mut out = Vec::with_capacity(HEADER_LEN + body.len() + 4);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&first.0.to_le_bytes());
    out.extend_from_slice(&last.0.to_le_bytes());
    out.extend_from_slice(&(ops.len() as u64).to_le_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Append one [`StreamOp`] record to `body`: `[u8 tag][u64 offset LE][u32 db LE][u32 key-len LE]
/// [key][u32 val-len LE][val]` (the val present only for a put). All integers little-endian, to
/// match the kvcodec + the persist format.
fn put_op_record(body: &mut Vec<u8>, op: &StreamOp) {
    match op {
        StreamOp::Put {
            offset,
            db,
            key,
            kvobj_bytes,
        } => {
            body.push(TAG_PUT);
            body.extend_from_slice(&offset.0.to_le_bytes());
            body.extend_from_slice(&db.to_le_bytes());
            body.extend_from_slice(&(key.len() as u32).to_le_bytes());
            body.extend_from_slice(key);
            body.extend_from_slice(&(kvobj_bytes.len() as u32).to_le_bytes());
            body.extend_from_slice(kvobj_bytes);
        }
        StreamOp::Del { offset, db, key } => {
            body.push(TAG_DEL);
            body.extend_from_slice(&offset.0.to_le_bytes());
            body.extend_from_slice(&db.to_le_bytes());
            body.extend_from_slice(&(key.len() as u32).to_le_bytes());
            body.extend_from_slice(key);
        }
    }
}

/// READ + validate one segment file, returning its decoded ops in offset order, or `None` when the
/// file is missing / foreign / wrong-version / torn (CRC mismatch) / continuity-inconsistent with
/// its recorded header. A `None` is a BACKLOG MISS: the caller stops the disk replay and the
/// resulting offset shortfall forces the replica to a full snapshot -- never a served corruption.
fn read_segment(
    path: &Path,
    expect_first: ReplOffset,
    expect_last: ReplOffset,
) -> Option<Vec<StreamOp>> {
    let bytes = read_file(path)?;
    if bytes.len() < HEADER_LEN + 4 {
        return None; // too short to hold a header + CRC.
    }
    if bytes[0..4] != MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
    if version != FORMAT_VERSION {
        return None;
    }
    let first = ReplOffset(u64::from_le_bytes(bytes[8..16].try_into().ok()?));
    let last = ReplOffset(u64::from_le_bytes(bytes[16..24].try_into().ok()?));
    let count = u64::from_le_bytes(bytes[24..32].try_into().ok()?);
    // The header must match what the manifest (in-memory `SegmentMeta`) recorded; a mismatch means
    // a stale / swapped / rewritten file -> reject (never serve an offset run we did not vouch for).
    if first != expect_first || last != expect_last {
        return None;
    }
    // CRC over the record body (everything between the header and the trailing 4 CRC bytes).
    let body = &bytes[HEADER_LEN..bytes.len() - 4];
    let stored_crc = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().ok()?);
    if crc32(body) != stored_crc {
        return None; // a torn file: never corrupt-load.
    }
    let ops = decode_op_records(body, count)?;
    // Final continuity check: the decoded ops must be exactly the contiguous run [first, last].
    if ops.first().map(StreamOp::offset) != Some(first)
        || ops.last().map(StreamOp::offset) != Some(last)
    {
        return None;
    }
    Some(ops)
}

/// Decode exactly `count` op records from a segment body, verifying each is offset-contiguous.
/// `None` on any truncation / bad tag / count mismatch / non-contiguous offset (a torn body the
/// CRC somehow passed, or a format bug) -- the caller treats it as a backlog miss.
fn decode_op_records(body: &[u8], count: u64) -> Option<Vec<StreamOp>> {
    let mut out = Vec::with_capacity(usize::try_from(count).ok()?.min(1 << 20));
    let mut pos = 0usize;
    let mut prev: Option<u64> = None;
    for _ in 0..count {
        let tag = *body.get(pos)?;
        pos += 1;
        let offset = u64::from_le_bytes(body.get(pos..pos + 8)?.try_into().ok()?);
        pos += 8;
        let db = u32::from_le_bytes(body.get(pos..pos + 4)?.try_into().ok()?);
        pos += 4;
        let key_len = u32::from_le_bytes(body.get(pos..pos + 4)?.try_into().ok()?) as usize;
        pos += 4;
        let key = body.get(pos..pos + key_len)?.to_vec();
        pos += key_len;
        // Offsets must be strictly +1 contiguous within the segment.
        if let Some(p) = prev {
            if offset != p + 1 {
                return None;
            }
        }
        prev = Some(offset);
        let op = match tag {
            TAG_PUT => {
                let val_len = u32::from_le_bytes(body.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                let kvobj_bytes = body.get(pos..pos + val_len)?.to_vec();
                pos += val_len;
                StreamOp::Put {
                    offset: ReplOffset(offset),
                    db,
                    key,
                    kvobj_bytes,
                }
            }
            TAG_DEL => StreamOp::Del {
                offset: ReplOffset(offset),
                db,
                key,
            },
            _ => return None, // unknown tag: torn / foreign.
        };
        out.push(op);
    }
    if pos != body.len() {
        return None; // trailing slop: malformed.
    }
    Some(out)
}

// ---------------------------------------------------------------------------------------------
// Crash-safe file I/O (the SAME tmp -> fsync -> rename + parent-dir fsync discipline
// `ironcache-persist::format::write_file_atomic` proved; reproduced here because that helper is
// private to the persist crate and this crate already depends on `ironcache-persist` only for the
// public `crc32`).
// ---------------------------------------------------------------------------------------------

/// Write `bytes` to `path` ATOMICALLY + crash-safely: write `<path>.tmp`, fsync its contents, then
/// rename it over `path` (atomic on POSIX), then fsync the parent dir so the rename is durable. A
/// crash before the rename leaves the prior `path` (only a stray `.tmp` remains); after, the new
/// file is fully present.
fn write_file_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let tmp = tmp_path(path);
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?; // fsync the contents before the rename commits the file.
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

/// Best-effort fsync of `path`'s parent directory so a rename's directory-entry update is durable.
/// Non-fatal: the file contents are already fsync'd + the rename is atomic, so the worst case is a
/// slightly-less-durable directory entry, never corruption.
fn fsync_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
    }
}

/// Read an entire file into a `Vec<u8>`, or `None` if it does not exist / cannot be read (the read
/// path treats a missing / unreadable segment as a backlog miss, the safe degradation).
fn read_file(path: &Path) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).ok()?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).ok()?;
    Some(buf)
}

/// A pure CRC-32 (IEEE 802.3 / zlib polynomial `0xEDB88320`, reflected) over `data`, computed with
/// a const-built 256-entry table. This is the SAME function (same polynomial, same check value) as
/// `ironcache_persist::format::crc32`; it is reproduced here (rather than imported) because
/// `ironcache-persist` DEPENDS ON `ironcache-repl`, so a `repl -> persist` edge would be a crate
/// cycle. It detects a TORN segment file (a partial write a crash left, or bit-rot) so a corrupt
/// segment is a backlog miss rather than served corruption. NOT cryptographic (an adversary is out
/// of scope for a local on-disk spill); it is an integrity check.
fn crc32(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = build_crc_table();
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        let idx = ((crc ^ u32::from(b)) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[idx];
    }
    crc ^ 0xFFFF_FFFF
}

/// Build the reflected CRC-32 lookup table at compile time (a `const fn`, evaluated once during
/// compilation so the runtime hot loop is a pure table lookup). Identical to the persist crate's.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "icrepl-backlog-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn put(offset: u64) -> StreamOp {
        StreamOp::Put {
            offset: ReplOffset(offset),
            db: 0,
            key: format!("k{offset}").into_bytes(),
            kvobj_bytes: format!("v{offset}").into_bytes(),
        }
    }

    fn del(offset: u64) -> StreamOp {
        StreamOp::Del {
            offset: ReplOffset(offset),
            db: 1,
            key: format!("d{offset}").into_bytes(),
        }
    }

    #[test]
    fn disabled_when_max_bytes_zero() {
        let dir = temp_dir("disabled");
        assert!(DiskBacklog::open(&dir, 0).is_none(), "zero-size = disabled");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn spill_then_read_round_trips_contiguous() {
        let dir = temp_dir("rt");
        let mut bl = DiskBacklog::open(&dir, 1 << 20).expect("enabled");
        // Spill offsets 1,2 then 3,4,5 in two segments (each batch abuts the prior).
        bl.spill(&[put(1), del(2)]).unwrap();
        bl.spill(&[put(3), put(4), del(5)]).unwrap();
        assert_eq!(bl.oldest_offset(), Some(ReplOffset(1)));
        assert_eq!(bl.newest_offset(), Some(ReplOffset(5)));
        assert_eq!(bl.segment_count(), 2);

        // Read from cursor 0: every op 1..=5 in order, gap-free.
        let ops = bl.ops_after(ReplOffset(0), usize::MAX);
        let offsets: Vec<_> = ops.iter().map(|o| o.offset().0).collect();
        assert_eq!(offsets, vec![1, 2, 3, 4, 5]);
        // The Put/Del classification + payloads round-trip.
        assert!(matches!(ops[0], StreamOp::Put { ref key, .. } if key == b"k1"));
        assert!(matches!(ops[1], StreamOp::Del { ref key, db: 1, .. } if key == b"d2"));

        // Read from a mid cursor: only the strictly-greater ops, no duplicate.
        let ops = bl.ops_after(ReplOffset(3), usize::MAX);
        assert_eq!(
            ops.iter().map(|o| o.offset().0).collect::<Vec<_>>(),
            vec![4, 5]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn spill_rejects_discontiguous() {
        let dir = temp_dir("discont");
        let mut bl = DiskBacklog::open(&dir, 1 << 20).expect("enabled");
        bl.spill(&[put(1), put(2)]).unwrap();
        // A batch that does not abut newest (3 expected, got 5).
        assert!(matches!(
            bl.spill(&[put(5)]),
            Err(SpillError::Discontiguous)
        ));
        // An internally non-contiguous batch.
        assert!(matches!(
            bl.spill(&[put(3), put(5)]),
            Err(SpillError::Discontiguous)
        ));
        // An empty batch.
        assert!(matches!(bl.spill(&[]), Err(SpillError::Discontiguous)));
        // The good newest is unchanged after the rejected spills.
        assert_eq!(bl.newest_offset(), Some(ReplOffset(2)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn eviction_keeps_total_bounded_and_contiguous() {
        let dir = temp_dir("evict");
        // Tiny bound so a couple of segments overflow it.
        let mut bl = DiskBacklog::open(&dir, 64).expect("enabled");
        // Each spill is one segment of a few ops; spill several so the oldest get evicted.
        let mut next = 1u64;
        for _ in 0..8 {
            let batch = vec![put(next), put(next + 1)];
            bl.spill(&batch).unwrap();
            next += 2;
        }
        assert!(bl.total_bytes() <= 64 || bl.segment_count() == 1, "bounded");
        // The retained run is still contiguous (newest - oldest + 1 == total ops retained), and the
        // oldest advanced (older segments were evicted).
        let oldest = bl.oldest_offset().unwrap();
        let newest = bl.newest_offset().unwrap();
        assert!(
            oldest.0 > 1,
            "the oldest segment(s) were evicted past the bound"
        );
        let ops = bl.ops_after(ReplOffset(oldest.0 - 1), usize::MAX);
        let offsets: Vec<_> = ops.iter().map(|o| o.offset().0).collect();
        let expected: Vec<u64> = (oldest.0..=newest.0).collect();
        assert_eq!(offsets, expected, "retained disk run is gap-free");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_segment_is_a_backlog_miss_not_served() {
        let dir = temp_dir("corrupt");
        let mut bl = DiskBacklog::open(&dir, 1 << 20).expect("enabled");
        bl.spill(&[put(1), put(2)]).unwrap();
        bl.spill(&[put(3), put(4)]).unwrap();
        // CORRUPT the SECOND segment file's body (flip a byte past the header) WITHOUT updating any
        // CRC: the read must reject it and return only the valid contiguous prefix (ops 1,2).
        let file = bl.dir.join(segment_file_name(1));
        let mut bytes = read_file(&file).unwrap();
        bytes[HEADER_LEN + 1] ^= 0xFF;
        write_file_atomic(&file, &bytes).unwrap();

        let ops = bl.ops_after(ReplOffset(0), usize::MAX);
        let offsets: Vec<_> = ops.iter().map(|o| o.offset().0).collect();
        assert_eq!(
            offsets,
            vec![1, 2],
            "the corrupt segment is a miss: only the valid prefix is served (forces full snapshot)"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_segment_file_is_a_backlog_miss() {
        let dir = temp_dir("missing");
        let mut bl = DiskBacklog::open(&dir, 1 << 20).expect("enabled");
        bl.spill(&[put(1), put(2)]).unwrap();
        bl.spill(&[put(3), put(4)]).unwrap();
        // Delete the second segment out from under the backlog (simulating fs loss).
        std::fs::remove_file(bl.dir.join(segment_file_name(1))).unwrap();
        let ops = bl.ops_after(ReplOffset(0), usize::MAX);
        assert_eq!(
            ops.iter().map(|o| o.offset().0).collect::<Vec<_>>(),
            vec![1, 2],
            "a missing segment stops the read at the gap (forces full snapshot)"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn can_serve_from_window_edges() {
        let dir = temp_dir("serve");
        let mut bl = DiskBacklog::open(&dir, 1 << 20).expect("enabled");
        bl.spill(&[put(5), put(6), put(7)]).unwrap();
        // from+1 must land within [5,7]: from=4 -> needs 5 (oldest) -> yes; from=3 -> needs 4 -> no.
        assert!(bl.can_serve_from(ReplOffset(4)));
        assert!(!bl.can_serve_from(ReplOffset(3)));
        // from=6 -> needs 7 (newest) -> yes; from=7 -> needs 8 (past newest) -> no (caught up).
        assert!(bl.can_serve_from(ReplOffset(6)));
        assert!(!bl.can_serve_from(ReplOffset(7)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn prune_through_drops_whole_segments_below_cut() {
        let dir = temp_dir("prune");
        let mut bl = DiskBacklog::open(&dir, 1 << 20).expect("enabled");
        bl.spill(&[put(1), put(2)]).unwrap(); // seg covers [1,2]
        bl.spill(&[put(3), put(4)]).unwrap(); // seg covers [3,4]
        bl.spill(&[put(5), put(6)]).unwrap(); // seg covers [5,6]
        bl.prune_through(ReplOffset(4)); // drop whole segments with last <= 4 -> [1,2] and [3,4].
        assert_eq!(bl.oldest_offset(), Some(ReplOffset(5)));
        assert_eq!(bl.segment_count(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn open_purges_stale_segments_from_a_prior_process() {
        let dir = temp_dir("purge");
        {
            let mut bl = DiskBacklog::open(&dir, 1 << 20).expect("enabled");
            bl.spill(&[put(1), put(2)]).unwrap();
            assert_eq!(bl.segment_count(), 1);
        }
        // A fresh open purges the prior segment files (a within-process spill buffer, not resumable
        // across a restart: the offsets are stale relative to the fresh head).
        let bl = DiskBacklog::open(&dir, 1 << 20).expect("enabled");
        assert_eq!(bl.segment_count(), 0);
        assert_eq!(bl.oldest_offset(), None);
        std::fs::remove_dir_all(&dir).ok();
    }
}
