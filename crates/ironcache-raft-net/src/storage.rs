// SPDX-License-Identifier: MIT OR Apache-2.0
//! A durable, fsync-backed [`RaftStorage`] for the production adapter (HA-4b).
//!
//! [`MemStorage`](ironcache_raft::MemStorage) (HA-4a) holds the persistent Raft state
//! (`currentTerm`, `votedFor`, `log[]`) in RAM, so a crashed-and-restarted node forgets
//! everything. That is a SAFETY hole, not just an availability one: Raft's Figure 2
//! lists those three as "Persistent state on all servers (Updated on stable storage
//! before responding to RPCs)" precisely because a node that forgets its term and vote
//! can vote a SECOND time in a term it already voted in, which breaks Election Safety
//! (at most one leader per term) and admits split brain. [`FileStorage`] closes that
//! hole: every mutation is appended to an fsync'd on-disk record log BEFORE the method
//! returns, and on restart the log is REPLAYED to rebuild the exact pre-crash state.
//!
//! ## Shape: append-only record log + an in-memory mirror
//!
//! The on-disk file is the SOURCE OF TRUTH across restarts; an in-memory mirror (the
//! same `(term, vote, Vec<LogEntry>)` shape as [`MemStorage`]) serves every READ so the
//! hot path never touches the disk. Each persistent mutation appends exactly one record
//! and is mirrored in memory. The four record kinds map one-to-one to the mutating
//! [`RaftStorage`] methods:
//!
//! - [`SetTerm`](Record::SetTerm) from [`set_current_term`](RaftStorage::set_current_term)
//! - [`SetVote`](Record::SetVote) from [`set_voted_for`](RaftStorage::set_voted_for)
//! - [`AppendEntry`](Record::AppendEntry) from [`append`](RaftStorage::append) and from
//!   [`append_entries`](RaftStorage::append_entries) (one record per entry)
//! - [`Truncate`](Record::Truncate) from [`truncate_from`](RaftStorage::truncate_from)
//!
//! A log is never rewritten in place: a `truncate_from` is itself an APPENDED record
//! (it shrinks the mirror's `Vec`, but the file only grows), so the file is strictly
//! append-only and a crash can only ever lose a trailing suffix, never corrupt a record
//! already fsync'd.
//!
//! ## Framing + checksum (torn-write detection)
//!
//! Each record is length-delimited and checksummed so a partially-written trailing
//! record (a "torn write": the process or machine died mid-append, after the length
//! went down but before the body was fully flushed) is DETECTED and dropped on replay
//! rather than mis-decoded into a fabricated state. The frame is:
//!
//! ```text
//!   [ u32 body_len ][ u32 crc32(body) ][ body_len bytes of body ]
//! ```
//!
//! all little-endian, where `body` is `[ u8 kind ][ kind-specific payload ]`. The body
//! reuses the SAME compact, length-delimited, hand-rolled binary style as the wire
//! [`codec`](crate::codec) (no serde); the only addition over the wire format is this
//! outer length+CRC framing, which the wire codec does not need (RESP frames the wire
//! message) but the on-disk log does (so replay can find record boundaries and reject a
//! torn tail). The checksum is a standard CRC-32 (IEEE 802.3 / zlib polynomial), small
//! and inline; it catches both a truncated body and bit-rot in a fully-written one.
//!
//! ## Persist-before-respond
//!
//! Every mutating method appends its record(s) and `fsync`s (`File::sync_all`) the file
//! BEFORE returning and BEFORE the mirror read paths can observe the change as durable,
//! exactly the Figure 2 ordering. `append_entries` batches all its records into a SINGLE
//! fsync (one stable-storage barrier for the whole AppendEntries RPC). This blocks the
//! caller until the data is on stable storage; that caller is the single Raft
//! control-plane task (`ironcache-raft-net`'s run loop), NOT a data-path request, so the
//! latency is acceptable and is the price of correctness.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use ironcache_raft::{
    ConfigCmd, EntryPayload, LogEntry, MembershipChange, NodeId, RaftStorage, SnapshotMeta,
};

// Record-kind discriminants (the first body byte). Distinct value space from the wire
// codec's message discriminants; these tag a PERSISTED MUTATION, not a wire message.
const REC_SET_TERM: u8 = 1;
const REC_SET_VOTE: u8 = 2;
const REC_APPEND_ENTRY: u8 = 3;
const REC_TRUNCATE: u8 = 4;

// Payload discriminants for an entry's EntryPayload, mirroring the wire codec so the
// on-disk entry body is byte-for-byte the same shape an AppendEntry record carries.
const PAYLOAD_NOOP: u8 = 0;
const PAYLOAD_BYTES: u8 = 1;
const PAYLOAD_CONFIG: u8 = 2;
// HA-3d raft cluster-membership change (mirror the wire codec's next free value).
const PAYLOAD_CONFIG_CHANGE: u8 = 3;

// MembershipChange discriminants, mirroring the wire codec (HA-3d).
const MEMBER_ADD_VOTER: u8 = 0;
const MEMBER_REMOVE_VOTER: u8 = 1;
const MEMBER_ADD_LEARNER: u8 = 2;
const MEMBER_PROMOTE_LEARNER: u8 = 3;
const MEMBER_REMOVE_LEARNER: u8 = 4;

// ConfigCmd discriminants, again mirroring the wire codec.
const CFG_ADD_NODE: u8 = 0;
const CFG_REMOVE_NODE: u8 = 1;
const CFG_SET_SLOT_OWNER: u8 = 2;
const CFG_ASSIGN_SLOTS: u8 = 3;
const CFG_SET_CONFIG_EPOCH: u8 = 4;
const CFG_ASSIGN_REPLICA: u8 = 5;
const CFG_PROMOTE_REPLICA: u8 = 6;
// HA-6 online slot migration (mirror the wire codec's discriminants, continuing from 7).
const CFG_SET_SLOT_MIGRATING: u8 = 7;
const CFG_SET_SLOT_IMPORTING: u8 = 8;
const CFG_SET_SLOT_STABLE: u8 = 9;
// UNASSIGN slots (mirror the wire codec's discriminant, continuing from 9).
const CFG_UNASSIGN_SLOTS: u8 = 10;

// The fixed frame header: a u32 body length followed by a u32 CRC of the body.
const FRAME_HEADER_LEN: usize = 8;

// ---------------------------------------------------------------------------
// CRC-32 (IEEE 802.3 / zlib polynomial), small and inline.
// ---------------------------------------------------------------------------

/// The reflected IEEE 802.3 CRC-32 polynomial (`0xEDB88320`), the same one zlib /
/// gzip / PNG use. We compute the table on the fly per call: a record body is tiny and
/// this storage is the control plane, not the data path, so a 256-entry table build per
/// record is irrelevant and it keeps the helper a pure, allocation-light function with
/// no `static`/`OnceLock` (which the determinism lint would have to reason about).
const CRC32_POLY: u32 = 0xEDB8_8320;

/// Standard CRC-32 of `data` (initial `0xFFFF_FFFF`, final XOR `0xFFFF_FFFF`).
///
/// Deterministic and pure: the same bytes always yield the same checksum, so a record
/// written on one run verifies on the next. Used only to detect a torn / corrupt
/// trailing record on replay, never for security.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (CRC32_POLY & mask);
        }
    }
    !crc
}

// ---------------------------------------------------------------------------
// Records.
// ---------------------------------------------------------------------------

/// One persisted mutation, the unit the log appends and replay applies.
///
/// Each variant corresponds to exactly one mutating [`RaftStorage`] method; replaying
/// the records in file order rebuilds the mirror's `(term, vote, log)` from empty.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Record {
    /// `set_current_term(term)`: the new persisted current term.
    SetTerm(u64),
    /// `set_voted_for(v)`: the new persisted vote for the current term (`None` clears).
    SetVote(Option<NodeId>),
    /// `append(entry)` / one element of `append_entries`: append this entry to the log.
    AppendEntry(LogEntry),
    /// `truncate_from(index)`: drop every log entry with `entry.index >= index`.
    Truncate(u64),
}

/// Encode a record's BODY (the bytes the frame's length + CRC cover): a kind byte then
/// the kind-specific payload, in the wire codec's compact little-endian style.
fn encode_body(record: &Record) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    match record {
        Record::SetTerm(term) => {
            out.push(REC_SET_TERM);
            put_u64(&mut out, *term);
        }
        Record::SetVote(vote) => {
            out.push(REC_SET_VOTE);
            // An Option<NodeId> as a presence byte (0 = None, 1 = Some) then the id.
            match vote {
                None => out.push(0),
                Some(id) => {
                    out.push(1);
                    put_u64(&mut out, id.0);
                }
            }
        }
        Record::AppendEntry(entry) => {
            out.push(REC_APPEND_ENTRY);
            put_entry(&mut out, entry);
        }
        Record::Truncate(index) => {
            out.push(REC_TRUNCATE);
            put_u64(&mut out, *index);
        }
    }
    out
}

/// Frame a record for the log: `[u32 body_len][u32 crc32(body)][body]`, all
/// little-endian. The header lets replay find the next record boundary; the CRC lets it
/// reject a torn or corrupt body.
fn encode_frame(record: &Record) -> Vec<u8> {
    let body = encode_body(record);
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + body.len());
    // body_len as u32: a single Raft record is tiny (an entry payload is bounded by the
    // command size), so u32 is ample and keeps the header fixed-width.
    let body_len = u32::try_from(body.len()).expect("a single record body fits in u32");
    frame.extend_from_slice(&body_len.to_le_bytes());
    frame.extend_from_slice(&crc32(&body).to_le_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// A length-prefixed byte blob: a `u64` length then the bytes (matches the wire codec).
fn put_blob(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// A length-prefixed UTF-8 string (its UTF-8 bytes as a blob).
fn put_str(out: &mut Vec<u8>, s: &str) {
    put_blob(out, s.as_bytes());
}

/// Append a [`LogEntry`]: term, index, then payload.
fn put_entry(out: &mut Vec<u8>, entry: &LogEntry) {
    put_u64(out, entry.term);
    put_u64(out, entry.index);
    put_payload(out, &entry.payload);
}

/// Append an [`EntryPayload`] led by its discriminant byte (mirrors the wire codec).
fn put_payload(out: &mut Vec<u8>, payload: &EntryPayload) {
    match payload {
        EntryPayload::Noop => out.push(PAYLOAD_NOOP),
        EntryPayload::Bytes(bytes) => {
            out.push(PAYLOAD_BYTES);
            put_blob(out, bytes);
        }
        EntryPayload::Config(cmd) => {
            out.push(PAYLOAD_CONFIG);
            put_config(out, cmd);
        }
        EntryPayload::ConfigChange(change) => {
            out.push(PAYLOAD_CONFIG_CHANGE);
            put_membership(out, *change);
        }
    }
}

/// Append a [`MembershipChange`] led by its discriminant byte, then its one NodeId
/// (HA-3d; mirrors the wire codec).
fn put_membership(out: &mut Vec<u8>, change: MembershipChange) {
    let (tag, node) = match change {
        MembershipChange::AddVoter(n) => (MEMBER_ADD_VOTER, n),
        MembershipChange::RemoveVoter(n) => (MEMBER_REMOVE_VOTER, n),
        MembershipChange::AddLearner(n) => (MEMBER_ADD_LEARNER, n),
        MembershipChange::PromoteLearner(n) => (MEMBER_PROMOTE_LEARNER, n),
        MembershipChange::RemoveLearner(n) => (MEMBER_REMOVE_LEARNER, n),
    };
    out.push(tag);
    put_u64(out, node.0);
}

/// Append a [`ConfigCmd`] led by its discriminant byte (mirrors the wire codec).
fn put_config(out: &mut Vec<u8>, cmd: &ConfigCmd) {
    match cmd {
        ConfigCmd::AddNode { id, host, port } => {
            out.push(CFG_ADD_NODE);
            put_str(out, id);
            put_str(out, host);
            put_u16(out, *port);
        }
        ConfigCmd::RemoveNode { id } => {
            out.push(CFG_REMOVE_NODE);
            put_str(out, id);
        }
        ConfigCmd::SetSlotOwner { slot, node } => {
            out.push(CFG_SET_SLOT_OWNER);
            put_u16(out, *slot);
            put_str(out, node);
        }
        ConfigCmd::AssignSlots { node, slots } => {
            out.push(CFG_ASSIGN_SLOTS);
            put_str(out, node);
            put_u64(out, slots.len() as u64);
            for slot in slots {
                put_u16(out, *slot);
            }
        }
        ConfigCmd::UnassignSlots { slots } => {
            // The inverse of AssignSlots; a length-prefixed slot list with NO node string. Must
            // round-trip through the fsync log so a committed UNASSIGN survives a restart.
            out.push(CFG_UNASSIGN_SLOTS);
            put_u64(out, slots.len() as u64);
            for slot in slots {
                put_u16(out, *slot);
            }
        }
        ConfigCmd::SetConfigEpoch(epoch) => {
            out.push(CFG_SET_CONFIG_EPOCH);
            put_u64(out, *epoch);
        }
        ConfigCmd::AssignReplica { node, slots } => {
            out.push(CFG_ASSIGN_REPLICA);
            put_str(out, node);
            put_u64(out, slots.len() as u64);
            for slot in slots {
                put_u16(out, *slot);
            }
        }
        ConfigCmd::PromoteReplica { slots, new_primary } => {
            // HA-8 failover; slots-then-node order (matches the wire codec + the variant's field
            // order). Must round-trip through the fsync log so a committed promotion survives a
            // restart (Figure-8: a new leader cannot lose it).
            out.push(CFG_PROMOTE_REPLICA);
            put_u64(out, slots.len() as u64);
            for slot in slots {
                put_u16(out, *slot);
            }
            put_str(out, new_primary);
        }
        ConfigCmd::SetSlotMigrating { slot, dest } => {
            // HA-6: slot-then-node (matches the wire codec + the variant's field order). Must
            // round-trip through the fsync log so a committed migration tag survives a restart.
            out.push(CFG_SET_SLOT_MIGRATING);
            put_u16(out, *slot);
            put_str(out, dest);
        }
        ConfigCmd::SetSlotImporting { slot, src, dest } => {
            // HA-6: slot-then-src-then-dest (matches the wire codec + the variant's field order).
            // The `dest` field is appended (discriminant unchanged) so a committed IMPORTING tag's
            // destination survives a restart.
            out.push(CFG_SET_SLOT_IMPORTING);
            put_u16(out, *slot);
            put_str(out, src);
            put_str(out, dest);
        }
        ConfigCmd::SetSlotStable { slot } => {
            out.push(CFG_SET_SLOT_STABLE);
            put_u16(out, *slot);
        }
    }
}

// ---------------------------------------------------------------------------
// Decoding a record body.
// ---------------------------------------------------------------------------

/// A forward-only, bounds-checked cursor over a record body, mirroring the wire codec's
/// `Cursor`. Every read returns `None` on a short buffer, so a body that passed the CRC
/// but is structurally malformed (which a correct writer never produces) still cannot
/// over-read; decode bubbles the `None` up and replay treats it as the end of the
/// valid log (defensive: a CRC match makes this path unreachable in practice).
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn u8(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn u16(&mut self) -> Option<u16> {
        let end = self.pos.checked_add(2)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(u16::from_le_bytes([slice[0], slice[1]]))
    }

    fn u64(&mut self) -> Option<u64> {
        let end = self.pos.checked_add(8)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(slice);
        Some(u64::from_le_bytes(bytes))
    }

    fn blob(&mut self) -> Option<Vec<u8>> {
        let len = usize::try_from(self.u64()?).ok()?;
        let end = self.pos.checked_add(len)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice.to_vec())
    }

    fn string(&mut self) -> Option<String> {
        String::from_utf8(self.blob()?).ok()
    }

    fn at_end(&self) -> bool {
        self.pos == self.buf.len()
    }
}

/// Decode a record from its body bytes (the CRC-verified frame payload). Returns `None`
/// for an unknown kind, a truncated body, or trailing bytes after a complete record;
/// the caller treats that as the end of the valid prefix.
fn decode_body(body: &[u8]) -> Option<Record> {
    let mut cur = Cursor::new(body);
    let record = match cur.u8()? {
        REC_SET_TERM => Record::SetTerm(cur.u64()?),
        REC_SET_VOTE => {
            let present = cur.u8()?;
            let vote = match present {
                0 => None,
                1 => Some(NodeId(cur.u64()?)),
                _ => return None,
            };
            Record::SetVote(vote)
        }
        REC_APPEND_ENTRY => Record::AppendEntry(get_entry(&mut cur)?),
        REC_TRUNCATE => Record::Truncate(cur.u64()?),
        _ => return None,
    };
    if cur.at_end() { Some(record) } else { None }
}

/// Read a [`LogEntry`] (term, index, payload).
fn get_entry(cur: &mut Cursor<'_>) -> Option<LogEntry> {
    let term = cur.u64()?;
    let index = cur.u64()?;
    let payload = get_payload(cur)?;
    Some(LogEntry {
        term,
        index,
        payload,
    })
}

/// Read an [`EntryPayload`] by its discriminant byte.
fn get_payload(cur: &mut Cursor<'_>) -> Option<EntryPayload> {
    match cur.u8()? {
        PAYLOAD_NOOP => Some(EntryPayload::Noop),
        PAYLOAD_BYTES => Some(EntryPayload::Bytes(cur.blob()?)),
        PAYLOAD_CONFIG => Some(EntryPayload::Config(get_config(cur)?)),
        PAYLOAD_CONFIG_CHANGE => Some(EntryPayload::ConfigChange(get_membership(cur)?)),
        _ => None,
    }
}

/// Read a [`MembershipChange`] by its discriminant byte, then its one NodeId (HA-3d).
fn get_membership(cur: &mut Cursor<'_>) -> Option<MembershipChange> {
    let tag = cur.u8()?;
    let node = NodeId(cur.u64()?);
    match tag {
        MEMBER_ADD_VOTER => Some(MembershipChange::AddVoter(node)),
        MEMBER_REMOVE_VOTER => Some(MembershipChange::RemoveVoter(node)),
        MEMBER_ADD_LEARNER => Some(MembershipChange::AddLearner(node)),
        MEMBER_PROMOTE_LEARNER => Some(MembershipChange::PromoteLearner(node)),
        MEMBER_REMOVE_LEARNER => Some(MembershipChange::RemoveLearner(node)),
        _ => None,
    }
}

/// Read a [`ConfigCmd`] by its discriminant byte.
fn get_config(cur: &mut Cursor<'_>) -> Option<ConfigCmd> {
    match cur.u8()? {
        CFG_ADD_NODE => Some(ConfigCmd::AddNode {
            id: cur.string()?,
            host: cur.string()?,
            port: cur.u16()?,
        }),
        CFG_REMOVE_NODE => Some(ConfigCmd::RemoveNode { id: cur.string()? }),
        CFG_SET_SLOT_OWNER => Some(ConfigCmd::SetSlotOwner {
            slot: cur.u16()?,
            node: cur.string()?,
        }),
        CFG_ASSIGN_SLOTS => {
            let node = cur.string()?;
            let count = usize::try_from(cur.u64()?).ok()?;
            let mut slots = Vec::with_capacity(count.min(16384));
            for _ in 0..count {
                slots.push(cur.u16()?);
            }
            Some(ConfigCmd::AssignSlots { node, slots })
        }
        CFG_SET_CONFIG_EPOCH => Some(ConfigCmd::SetConfigEpoch(cur.u64()?)),
        CFG_UNASSIGN_SLOTS => {
            let count = usize::try_from(cur.u64()?).ok()?;
            let mut slots = Vec::with_capacity(count.min(16384));
            for _ in 0..count {
                slots.push(cur.u16()?);
            }
            Some(ConfigCmd::UnassignSlots { slots })
        }
        CFG_ASSIGN_REPLICA => {
            let node = cur.string()?;
            let count = usize::try_from(cur.u64()?).ok()?;
            let mut slots = Vec::with_capacity(count.min(16384));
            for _ in 0..count {
                slots.push(cur.u16()?);
            }
            Some(ConfigCmd::AssignReplica { node, slots })
        }
        CFG_PROMOTE_REPLICA => {
            let count = usize::try_from(cur.u64()?).ok()?;
            let mut slots = Vec::with_capacity(count.min(16384));
            for _ in 0..count {
                slots.push(cur.u16()?);
            }
            let new_primary = cur.string()?;
            Some(ConfigCmd::PromoteReplica { slots, new_primary })
        }
        CFG_SET_SLOT_MIGRATING => Some(ConfigCmd::SetSlotMigrating {
            slot: cur.u16()?,
            dest: cur.string()?,
        }),
        CFG_SET_SLOT_IMPORTING => Some(ConfigCmd::SetSlotImporting {
            // Read in WIRE order: slot, src, then the appended dest (matches the encode order).
            slot: cur.u16()?,
            src: cur.string()?,
            dest: cur.string()?,
        }),
        CFG_SET_SLOT_STABLE => Some(ConfigCmd::SetSlotStable { slot: cur.u16()? }),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// The in-memory mirror (same shape as MemStorage).
// ---------------------------------------------------------------------------

/// The in-memory mirror of the persisted state: the SAME `(term, vote, Vec<LogEntry>)`
/// shape as [`MemStorage`](ironcache_raft::MemStorage), serving every read so the hot
/// path never hits the disk. Replay applies records into one of these; each mutation
/// updates it in lockstep with the appended record.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Mirror {
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    /// The 1-based index of `log[0]` once a compaction has dropped a prefix (Raft
    /// section 7), mirroring [`MemStorage`](ironcache_raft::MemStorage)'s `log_start`.
    /// `0` means "no compaction" (the log is whole, starting at index 1). Recovered on
    /// reopen from the persisted snapshot's `last_included_index + 1`.
    log_start: u64,
    /// The most recent snapshot's metadata, if any. Its `last_included_term` is what
    /// `term_at(last_included_index)` answers once the underlying entry is compacted.
    snap_meta: Option<SnapshotMeta>,
    /// The most recent snapshot's serialized state-machine bytes, if any.
    snap_data: Vec<u8>,
    /// HA-3d: the persisted CONFIGURATION BASELINE (voter set, learner set) saved beside
    /// the snapshot, or `None` if none was saved (the never-compacted / pre-3d path). The
    /// engine seeds its configuration from this on restart and replays the surviving log's
    /// `ConfigChange` deltas on top, so a membership history that was compacted away is not
    /// lost across a restart. Recovered in [`FileStorage::open`] from the `.cfg` sidecar.
    config_baseline: Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)>,
}

impl Mirror {
    /// The 1-based index of `log[0]` (Raft section 7 compaction). With no compaction
    /// (`log_start == 0`) the log starts at index 1.
    #[inline]
    fn start(&self) -> u64 {
        if self.log_start == 0 {
            1
        } else {
            self.log_start
        }
    }

    /// The vec position of 1-based `index`, or `None` if below the compacted start or
    /// past the end.
    #[inline]
    fn pos_of(&self, index: u64) -> Option<usize> {
        let start = self.start();
        if index < start {
            return None;
        }
        usize::try_from(index - start)
            .ok()
            .filter(|&p| p < self.log.len())
    }

    /// Apply one record to the mirror, exactly as the corresponding mutating method
    /// does. This is the single place replay and the live mutators agree, so the
    /// rebuilt-from-disk mirror is byte-identical to the live one.
    fn apply(&mut self, record: Record) {
        match record {
            Record::SetTerm(term) => self.current_term = term,
            Record::SetVote(vote) => self.voted_for = vote,
            // FIX 1 (snapshot-aware replay): DROP an entry that is BELOW the compacted
            // start. After `save_snapshot` writes the `.snap` sidecar at index K but BEFORE
            // `compact_log_to` physically rewrites the log, a crash leaves the sidecar PLUS
            // the full pre-compaction log on disk. On reopen, `open` seeds `log_start = K+1`
            // from the sidecar (BEFORE replay), so this guard skips the now-compacted prefix
            // (entries 1..=K) and only pushes entries at-or-above `start()`. Without it, the
            // prefix would land at `log[0]` while `log_start == K+1`, so `pos_of(K+1) == 0`
            // would return the WRONG entry (index 1) and `apply_committed` would apply the
            // wrong entry -- silent committed-state corruption. A live append (the only other
            // `apply` caller) is always at-or-above `start()`, so the guard is inert there
            // and the threshold-0 / no-sidecar path stays byte-identical (start() == 1, so
            // nothing is ever dropped).
            Record::AppendEntry(entry) => {
                if entry.index >= self.start() {
                    self.log.push(entry);
                }
            }
            Record::Truncate(index) => {
                // Drop entries with entry.index >= index. index at/below the compacted
                // start clears the whole surviving log; an index past the end truncates
                // nothing. Same rule as MemStorage::truncate_from over a compacted log.
                let start = self.start();
                let keep = if index <= start {
                    0
                } else {
                    usize::try_from(index - start).unwrap_or(usize::MAX)
                };
                if keep < self.log.len() {
                    self.log.truncate(keep);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FileStorage.
// ---------------------------------------------------------------------------

/// A durable, fsync-backed [`RaftStorage`]: an append-only record log on disk (the
/// source of truth across restarts) plus an in-memory [`Mirror`] that serves every
/// read.
///
/// Open it with [`FileStorage::open`], which replays the on-disk log to rebuild the
/// mirror (stopping at and truncating away any torn trailing record). Thereafter every
/// mutating [`RaftStorage`] method appends its record(s), `fsync`s, and updates the
/// mirror before returning; every read is served from the mirror.
#[derive(Debug)]
pub struct FileStorage {
    /// The append-only log file, kept open and positioned at the end for appends.
    file: File,
    /// The path, retained for diagnostics (and so the type is self-describing).
    path: PathBuf,
    /// The in-memory mirror that serves all reads.
    mirror: Mirror,
}

/// The filesystem suffix for a node's snapshot sidecar file (Raft section 7). The
/// snapshot lives BESIDE the record log (`<log>.snap`), so it can be written + fsync'd
/// independently and the log can then be physically rewritten to drop the compacted
/// prefix without losing the snapshot.
///
/// DATA_DIR CO-LOCATION (FIX 4): the sidecar path is derived purely from the record-log
/// path (`<log>.snap`), and the log path is `raft_log_path(data_dir, port)` -- so when a
/// `data_dir` is configured the sidecar lands at
/// `<data_dir>/ironcache-raft-<port>.log.snap` (durable across a `/tmp`-clearing reboot,
/// right next to its log), and when `data_dir` is unset it stays beside the temp-dir log
/// (the byte-unchanged default). There is no separate temp-dir fallback for the sidecar.
///
/// STALE-SIDECAR CAVEAT (FIX 4): the "threshold-0 == byte-identical" property holds only
/// when NO `.snap` sidecar pre-exists. A node that ran with compaction ON (a non-zero
/// `snapshot_threshold`) leaves a sidecar on disk; if it then restarts at threshold 0, the
/// sidecar is STILL loaded, so `load_snapshot()` is `Some` and the leader's
/// `send_append_entries_to` can still take the InstallSnapshot branch for a far-behind
/// peer. That is correct (the snapshot is a valid committed prefix), just not the
/// pristine no-snapshot path. Removing the sidecar is what truly resets to the pre-3c
/// shape.
const SNAPSHOT_SUFFIX: &str = ".snap";

/// The filesystem suffix for a node's CONFIG-BASELINE sidecar file (HA-3d). The baseline
/// (the committed voter + learner sets as of the last snapshot) lives BESIDE the record
/// log (`<log>.cfg`), written + fsync'd atomically (tmp + rename) exactly like the
/// `.snap` snapshot sidecar. It is small and rewritten whole on each save, so there is no
/// torn-tail concern beyond the CRC the frame already carries; a torn / absent file
/// recovers as "no baseline" (the engine falls back to the constructor voter set, the
/// byte-unchanged static-membership path). It is co-located with the log just like the
/// snapshot sidecar, so a configured `data_dir` keeps it durable across a `/tmp` clear.
const CONFIG_BASELINE_SUFFIX: &str = ".cfg";

impl FileStorage {
    /// Open (creating if absent) the record log at `path` and REPLAY it to recover the
    /// persisted Raft state.
    ///
    /// Replay reads frames in order, verifying each frame's length and CRC; it applies
    /// every valid record to the mirror and STOPS at the first record that is torn
    /// (a short header, a body shorter than its declared length, a CRC mismatch, or a
    /// body that does not decode). At that point it TRUNCATES the file to the offset
    /// just past the last good record, so the next append starts from a clean boundary
    /// and the torn tail can never be replayed again. A brand-new or empty file
    /// recovers to the initial state (term 0, no vote, empty log).
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        // Open read+write, creating if absent. We read the whole file to replay, then
        // keep the handle for appends.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        // RECOVER THE SNAPSHOT FIRST (Raft section 7): if the sidecar holds a valid
        // snapshot, seed the mirror's snapshot + `log_start` from it BEFORE replaying the
        // log, so the surviving log tail (the records still in the file after the last
        // compaction) is positioned just above `last_included_index`. A torn / absent
        // sidecar yields no snapshot (the whole-log path, byte-identical to pre-3c).
        let snapshot = load_snapshot_file(&snapshot_path(&path))?;
        let mut seed = Mirror::default();
        if let Some((meta, data)) = snapshot {
            seed.log_start = meta.last_included_index + 1;
            seed.snap_meta = Some(meta);
            seed.snap_data = data;
        }

        // HA-3d: RECOVER THE CONFIG BASELINE from its `.cfg` sidecar (independent of the
        // log replay; the engine reads it AFTER construction and replays the surviving
        // log's ConfigChange deltas on top). A torn / absent file yields no baseline, so a
        // node falls back to the constructor voter set -- the byte-unchanged pre-3d path.
        seed.config_baseline = load_config_baseline_file(&config_baseline_path(&path))?;

        let (mirror, good_len) = replay_into(seed, &bytes);

        // If a torn / partial tail was found, physically truncate the file to the last
        // good offset so future appends are clean and a re-open sees no torn record.
        if good_len < bytes.len() {
            file.set_len(good_len as u64)?;
            file.sync_all()?;
        }
        // Position the write cursor at the end (the last good offset) for appends.
        file.seek(SeekFrom::Start(good_len as u64))?;

        Ok(FileStorage { file, path, mirror })
    }

    /// The path of the underlying record log (for diagnostics / tests).
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Persist a snapshot (Raft section 7): write it to a sidecar `.snap` file
    /// ATOMICALLY (write a `.snap.tmp`, fsync it, rename over the real path) and update
    /// the mirror's in-memory snapshot. The atomic rename means a crash mid-write leaves
    /// either the old snapshot or the new one, never a torn one, so a reopen always loads
    /// a consistent snapshot. The log file is UNTOUCHED here; compaction
    /// ([`compact_log_to`](FileStorage::compact_log_to)) drops the redundant log prefix
    /// in a separate, idempotent step, so a crash between the two is safe (the worst case
    /// is a snapshot plus a log that still holds the snapshotted prefix, which replay
    /// reconciles).
    fn persist_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]) -> io::Result<()> {
        let snap_path = snapshot_path(&self.path);
        let tmp_path = {
            let mut p = snap_path.clone().into_os_string();
            p.push(".tmp");
            PathBuf::from(p)
        };
        let frame = encode_snapshot_frame(meta, data);
        {
            let mut tmp = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            tmp.write_all(&frame)?;
            tmp.sync_all()?;
        }
        std::fs::rename(&tmp_path, &snap_path)?;
        // Mirror the snapshot in memory (reads serve from the mirror).
        self.mirror.snap_meta = Some(meta);
        self.mirror.snap_data = data.to_vec();
        Ok(())
    }

    /// Persist the config baseline (HA-3d): write `(voters, learners)` to the `.cfg`
    /// sidecar ATOMICALLY (write a `.cfg.tmp`, fsync it, rename over the real path) and
    /// update the mirror's in-memory copy. The atomic rename means a crash mid-write leaves
    /// either the old baseline or the new one, never a torn one, so a reopen always loads a
    /// consistent baseline. The engine saves this beside the snapshot at the same
    /// compaction point, so on restart `recovered config = baseline + surviving-tail
    /// ConfigChange deltas` reconstructs the exact pre-restart configuration even though the
    /// membership history below the snapshot was compacted out of the log.
    fn persist_config_baseline(
        &mut self,
        voters: &BTreeSet<NodeId>,
        learners: &BTreeSet<NodeId>,
    ) -> io::Result<()> {
        let cfg_path = config_baseline_path(&self.path);
        let tmp_path = {
            let mut p = cfg_path.clone().into_os_string();
            p.push(".tmp");
            PathBuf::from(p)
        };
        let frame = encode_config_baseline_frame(voters, learners);
        {
            let mut tmp = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            tmp.write_all(&frame)?;
            tmp.sync_all()?;
        }
        std::fs::rename(&tmp_path, &cfg_path)?;
        // Mirror the baseline in memory (reads serve from the mirror).
        self.mirror.config_baseline = Some((voters.clone(), learners.clone()));
        Ok(())
    }

    /// Compact the log to `index` (Raft section 7): drop every entry with index `<=
    /// index` from the mirror, then PHYSICALLY REWRITE the on-disk log so it holds only
    /// the surviving suffix (plus the term + vote, re-stated as records so a fresh reopen
    /// recovers them). The rewrite goes through a `.tmp` + atomic rename, so a crash
    /// leaves either the pre-compaction log or the compacted one. On reopen the snapshot
    /// sidecar supplies `log_start`, so the rewritten suffix lines up just above
    /// `last_included_index`. An `index` below the current start (a stale / duplicate
    /// compaction) is a no-op. The caller persists the snapshot FIRST, so the dropped
    /// prefix is always recoverable from it.
    fn compact_log_to(&mut self, index: u64) -> io::Result<()> {
        let start = self.mirror.start();
        if index < start || index == 0 {
            return Ok(());
        }
        // Drop the prefix from the mirror (entries with index in [start, index]).
        let drop = usize::try_from(index - start + 1).unwrap_or(usize::MAX);
        let drop = drop.min(self.mirror.log.len());
        self.mirror.log.drain(..drop);
        self.mirror.log_start = index + 1;

        // Rebuild the on-disk log from the surviving mirror state: term, vote, then the
        // surviving entries. This is the ONE place the append-only file is rewritten; it
        // is safe because the snapshot already holds the dropped prefix and the rewrite
        // is atomic (tmp + rename).
        let mut records: Vec<Record> = Vec::with_capacity(self.mirror.log.len() + 2);
        records.push(Record::SetTerm(self.mirror.current_term));
        records.push(Record::SetVote(self.mirror.voted_for));
        for entry in &self.mirror.log {
            records.push(Record::AppendEntry(entry.clone()));
        }
        let mut buf = Vec::new();
        for record in &records {
            buf.extend_from_slice(&encode_frame(record));
        }

        let tmp_path = {
            let mut p = self.path.clone().into_os_string();
            p.push(".compact.tmp");
            PathBuf::from(p)
        };
        {
            let mut tmp = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            tmp.write_all(&buf)?;
            tmp.sync_all()?;
        }
        std::fs::rename(&tmp_path, &self.path)?;
        // Reopen the (now rewritten) log file for appends, positioned at the end.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)?;
        file.seek(SeekFrom::End(0))?;
        self.file = file;
        Ok(())
    }

    /// Append one framed record to the file, fsync, then apply it to the mirror.
    ///
    /// The fsync is the persist-before-respond barrier: the record is on stable storage
    /// before this returns, so a crash right after the call cannot lose it. The mirror
    /// is updated AFTER the durable write, so the in-memory view never reflects a
    /// mutation that is not yet durable.
    fn append_record(&mut self, record: Record) -> io::Result<()> {
        let frame = encode_frame(&record);
        self.file.write_all(&frame)?;
        self.file.sync_all()?;
        self.mirror.apply(record);
        Ok(())
    }

    /// Append several framed records, then a SINGLE fsync, then apply them all to the
    /// mirror. One stable-storage barrier for the whole batch (used by
    /// [`append_entries`](RaftStorage::append_entries): one AppendEntries RPC = one
    /// fsync).
    fn append_records(&mut self, records: Vec<Record>) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for record in &records {
            buf.extend_from_slice(&encode_frame(record));
        }
        self.file.write_all(&buf)?;
        self.file.sync_all()?;
        for record in records {
            self.mirror.apply(record);
        }
        Ok(())
    }
}

/// Replay raw file bytes into a fresh (default) [`Mirror`]. The no-snapshot entry
/// point used by the frame round-trip test; the live [`FileStorage::open`] path uses
/// [`replay_into`] with a snapshot-seeded mirror.
#[cfg(test)]
fn replay(bytes: &[u8]) -> (Mirror, usize) {
    replay_into(Mirror::default(), bytes)
}

/// Replay raw log-file bytes into `seed` (a mirror pre-seeded with the recovered
/// snapshot + `log_start`), returning the rebuilt mirror and the byte length of the
/// VALID prefix (the offset just past the last good record).
///
/// Stops at the first torn / invalid record: a header shorter than 8 bytes, a body
/// shorter than its declared length, a CRC mismatch, or a body that does not decode to
/// a known record. The returned length is where the file should be truncated so the
/// torn tail is discarded. Seeding (rather than starting from `Mirror::default`) is how
/// a compacted log's surviving suffix lines up just above the snapshot's
/// `last_included_index` (Raft section 7).
fn replay_into(seed: Mirror, bytes: &[u8]) -> (Mirror, usize) {
    let mut mirror = seed;
    let mut pos = 0usize;
    loop {
        // Need a full fixed header (u32 len + u32 crc) to even know the body length.
        if pos + FRAME_HEADER_LEN > bytes.len() {
            break;
        }
        let body_len =
            u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                as usize;
        let stored_crc = u32::from_le_bytes([
            bytes[pos + 4],
            bytes[pos + 5],
            bytes[pos + 6],
            bytes[pos + 7],
        ]);
        let body_start = pos + FRAME_HEADER_LEN;
        // A corrupt length that would overflow on add is a torn trailing record.
        let Some(body_end) = body_start.checked_add(body_len) else {
            break;
        };
        // A body that runs past the end of the file is a torn trailing write.
        if body_end > bytes.len() {
            break;
        }
        let body = &bytes[body_start..body_end];
        // CRC mismatch: bit-rot or a torn body whose length happened to fit. Reject.
        if crc32(body) != stored_crc {
            break;
        }
        // A CRC-clean body that nonetheless fails to decode would mean a writer bug;
        // treat it like a torn record (stop) rather than fabricate state.
        let Some(record) = decode_body(body) else {
            break;
        };
        mirror.apply(record);
        pos = body_end;
    }
    (mirror, pos)
}

// ---------------------------------------------------------------------------
// Snapshot sidecar file (Raft section 7).
// ---------------------------------------------------------------------------

/// The snapshot sidecar path for a given log path (`<log>.snap`).
fn snapshot_path(log_path: &Path) -> PathBuf {
    let mut p = log_path.to_path_buf().into_os_string();
    p.push(SNAPSHOT_SUFFIX);
    PathBuf::from(p)
}

/// Frame a snapshot for the sidecar file: the SAME `[u32 body_len][u32 crc32(body)][body]`
/// framing as a log record, where `body` is `[u64 last_included_index][u64
/// last_included_term][blob data]`. The CRC lets a reopen reject a torn snapshot (a crash
/// mid-write, though the atomic rename normally prevents that) and fall back to no
/// snapshot rather than fabricating one.
fn encode_snapshot_frame(meta: SnapshotMeta, data: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(16 + data.len());
    put_u64(&mut body, meta.last_included_index);
    put_u64(&mut body, meta.last_included_term);
    put_blob(&mut body, data);
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + body.len());
    let body_len = u32::try_from(body.len()).expect("a snapshot body fits in u32");
    frame.extend_from_slice(&body_len.to_le_bytes());
    frame.extend_from_slice(&crc32(&body).to_le_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// Load the snapshot sidecar at `path`, returning its `(meta, data)` or `None` if the
/// file is absent, empty, or torn (a CRC mismatch / short body / structurally invalid
/// body). A torn snapshot recovers as "no snapshot" rather than an error, so a node
/// boots from the log alone, exactly the pre-3c behaviour.
fn load_snapshot_file(path: &Path) -> io::Result<Option<(SnapshotMeta, Vec<u8>)>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if bytes.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    let body_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let stored_crc = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let Some(body_end) = FRAME_HEADER_LEN.checked_add(body_len) else {
        return Ok(None);
    };
    if body_end > bytes.len() {
        return Ok(None);
    }
    let body = &bytes[FRAME_HEADER_LEN..body_end];
    if crc32(body) != stored_crc {
        return Ok(None);
    }
    let mut cur = Cursor::new(body);
    let (Some(last_included_index), Some(last_included_term)) = (cur.u64(), cur.u64()) else {
        return Ok(None);
    };
    let Some(data) = cur.blob() else {
        return Ok(None);
    };
    if !cur.at_end() {
        return Ok(None);
    }
    Ok(Some((
        SnapshotMeta {
            last_included_index,
            last_included_term,
        },
        data,
    )))
}

/// The config-baseline sidecar path for a record log (`<log>.cfg`), HA-3d.
fn config_baseline_path(log_path: &Path) -> PathBuf {
    let mut p = log_path.to_path_buf().into_os_string();
    p.push(CONFIG_BASELINE_SUFFIX);
    PathBuf::from(p)
}

/// Frame a config baseline for its sidecar file (HA-3d): the SAME `[u32 body_len][u32
/// crc32(body)][body]` framing as a log record / snapshot sidecar, where `body` is `[u64
/// voter_count][voter ids...][u64 learner_count][learner ids...]`, each id a `u64`. The
/// CRC lets a reopen reject a torn baseline (a crash mid-write, though the atomic rename
/// normally prevents that) and fall back to no baseline rather than fabricating one.
fn encode_config_baseline_frame(voters: &BTreeSet<NodeId>, learners: &BTreeSet<NodeId>) -> Vec<u8> {
    let mut body = Vec::new();
    put_u64(&mut body, voters.len() as u64);
    for v in voters {
        put_u64(&mut body, v.0);
    }
    put_u64(&mut body, learners.len() as u64);
    for l in learners {
        put_u64(&mut body, l.0);
    }
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + body.len());
    let body_len = u32::try_from(body.len()).expect("a config-baseline body fits in u32");
    frame.extend_from_slice(&body_len.to_le_bytes());
    frame.extend_from_slice(&crc32(&body).to_le_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// Load the config-baseline sidecar at `path` (HA-3d), returning `(voters, learners)` or
/// `None` if the file is absent, empty, or torn (a CRC mismatch / short body /
/// structurally invalid body). A torn baseline recovers as "no baseline" rather than an
/// error, so a node falls back to the constructor voter set (the pre-3d behaviour) instead
/// of refusing to boot.
fn load_config_baseline_file(
    path: &Path,
) -> io::Result<Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if bytes.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    let body_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let stored_crc = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let Some(body_end) = FRAME_HEADER_LEN.checked_add(body_len) else {
        return Ok(None);
    };
    if body_end > bytes.len() {
        return Ok(None);
    }
    let body = &bytes[FRAME_HEADER_LEN..body_end];
    if crc32(body) != stored_crc {
        return Ok(None);
    }
    let mut cur = Cursor::new(body);
    let Some(set_voters) = read_node_set(&mut cur) else {
        return Ok(None);
    };
    let Some(set_learners) = read_node_set(&mut cur) else {
        return Ok(None);
    };
    if !cur.at_end() {
        return Ok(None);
    }
    Ok(Some((set_voters, set_learners)))
}

/// Read a length-prefixed set of `NodeId`s (a `u64` count then that many `u64` ids) from
/// `cur`, or `None` on a short / structurally invalid body (HA-3d baseline decode helper).
fn read_node_set(cur: &mut Cursor) -> Option<BTreeSet<NodeId>> {
    let count = cur.u64()?;
    let mut set = BTreeSet::new();
    for _ in 0..count {
        set.insert(NodeId(cur.u64()?));
    }
    Some(set)
}

impl RaftStorage for FileStorage {
    fn current_term(&self) -> u64 {
        self.mirror.current_term
    }

    fn set_current_term(&mut self, term: u64) {
        self.append_record(Record::SetTerm(term))
            .expect("FileStorage: fsync current_term to stable storage");
    }

    fn voted_for(&self) -> Option<NodeId> {
        self.mirror.voted_for
    }

    fn set_voted_for(&mut self, v: Option<NodeId>) {
        self.append_record(Record::SetVote(v))
            .expect("FileStorage: fsync voted_for to stable storage");
    }

    fn last_log_index(&self) -> u64 {
        self.mirror.log.last().map_or_else(
            || self.mirror.snap_meta.map_or(0, |m| m.last_included_index),
            |e| e.index,
        )
    }

    fn last_log_term(&self) -> u64 {
        self.mirror.log.last().map_or_else(
            || self.mirror.snap_meta.map_or(0, |m| m.last_included_term),
            |e| e.term,
        )
    }

    fn append(&mut self, entry: LogEntry) {
        self.append_record(Record::AppendEntry(entry))
            .expect("FileStorage: fsync appended entry to stable storage");
    }

    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            // The empty-log sentinel always "matches" prev_log_index 0 (Figure 2
            // AppendEntries rule 2), same as MemStorage.
            return 0;
        }
        // A snapshot answers the term at its last_included_index even after the entry
        // was compacted away (Raft section 7).
        if let Some(meta) = self.mirror.snap_meta {
            if index == meta.last_included_index {
                return meta.last_included_term;
            }
        }
        self.mirror
            .pos_of(index)
            .map_or(0, |pos| self.mirror.log[pos].term)
    }

    fn entries_from(&self, index: u64) -> Vec<LogEntry> {
        let start = self.mirror.start();
        let from = index.max(start);
        let Ok(pos) = usize::try_from(from - start) else {
            return Vec::new();
        };
        self.mirror
            .log
            .get(pos..)
            .map_or_else(Vec::new, <[LogEntry]>::to_vec)
    }

    fn truncate_from(&mut self, index: u64) {
        self.append_record(Record::Truncate(index))
            .expect("FileStorage: fsync truncate to stable storage");
    }

    fn append_entries(&mut self, entries: &[LogEntry]) {
        // One record per entry, one fsync for the whole batch (the AppendEntries RPC's
        // single stable-storage barrier).
        let records = entries
            .iter()
            .cloned()
            .map(Record::AppendEntry)
            .collect::<Vec<_>>();
        self.append_records(records)
            .expect("FileStorage: fsync appended entries to stable storage");
    }

    fn entry_at(&self, index: u64) -> Option<LogEntry> {
        if index == 0 {
            return None;
        }
        self.mirror
            .pos_of(index)
            .map(|pos| self.mirror.log[pos].clone())
    }

    fn save_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]) {
        self.persist_snapshot(meta, data)
            .expect("FileStorage: fsync snapshot to stable storage");
    }

    fn load_snapshot(&self) -> Option<(SnapshotMeta, Vec<u8>)> {
        self.mirror
            .snap_meta
            .map(|meta| (meta, self.mirror.snap_data.clone()))
    }

    fn compact_to(&mut self, index: u64) {
        self.compact_log_to(index)
            .expect("FileStorage: rewrite compacted log to stable storage");
    }

    fn save_config_baseline(&mut self, voters: &BTreeSet<NodeId>, learners: &BTreeSet<NodeId>) {
        self.persist_config_baseline(voters, learners)
            .expect("FileStorage: fsync config baseline to stable storage");
    }

    fn load_config_baseline(&self) -> Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)> {
        self.mirror.config_baseline.clone()
    }

    fn log_start_index(&self) -> u64 {
        self.mirror.start()
    }
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_raft::MemStorage;

    /// A deterministic temp-file path for a test, derived from the TEST NAME plus this
    /// process's id. NO clock and NO RNG (ADR-0003 / the determinism lint forbid both,
    /// in tests too): the test name makes the path unique ACROSS tests, and the pid
    /// makes it unique across concurrent `cargo test` runs / processes, so two tests
    /// never collide on one file. The caller removes any stale file at the start, so a
    /// previous run's leftover never contaminates a fresh open.
    fn temp_path(test_name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "ironcache-raft-net-filestorage-{}-{}.log",
            test_name,
            std::process::id()
        ));
        p
    }

    /// A fresh, removed path for `test_name`: derive the deterministic path and delete
    /// any leftover so the test starts from a guaranteed-absent file.
    fn fresh_path(test_name: &str) -> PathBuf {
        let p = temp_path(test_name);
        let _ = std::fs::remove_file(&p);
        p
    }

    /// Remove the test's file (best-effort cleanup at the end of a test).
    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
    }

    /// A small set of entries spanning every payload kind, for the log-recovery tests.
    fn sample_entries() -> Vec<LogEntry> {
        vec![
            LogEntry {
                term: 1,
                index: 1,
                payload: EntryPayload::Noop,
            },
            LogEntry {
                term: 2,
                index: 2,
                payload: EntryPayload::Bytes(b"hello-durable-world".to_vec()),
            },
            LogEntry {
                term: 3,
                index: 3,
                payload: EntryPayload::Config(ConfigCmd::AddNode {
                    id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                    host: "10.0.0.5".to_owned(),
                    port: 6379,
                }),
            },
        ]
    }

    #[test]
    fn crc32_matches_known_check_value() {
        // The canonical CRC-32 (IEEE / zlib) check value for the ASCII "123456789" is
        // 0xCBF43926; pinning it proves our inline implementation is the standard one.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        // Empty input is the post-final-XOR of the all-ones initial state: 0.
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn frame_round_trips_every_record_kind() {
        // Each record kind must frame -> replay back to itself. Build a buffer of one
        // frame per kind (including every payload / config shape via an AppendEntry)
        // and replay it; the mirror must reflect the applied sequence and the valid
        // prefix must be the whole buffer (nothing torn).
        let records = vec![
            Record::SetTerm(42),
            Record::SetVote(Some(NodeId(7))),
            Record::SetVote(None),
            Record::AppendEntry(LogEntry {
                term: 1,
                index: 1,
                payload: EntryPayload::Noop,
            }),
            Record::AppendEntry(LogEntry {
                term: 2,
                index: 2,
                payload: EntryPayload::Config(ConfigCmd::AssignSlots {
                    node: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                    slots: vec![0, 1, 16_383],
                }),
            }),
            Record::Truncate(2),
        ];
        let mut buf = Vec::new();
        for r in &records {
            buf.extend_from_slice(&encode_frame(r));
        }
        let (mirror, good_len) = replay(&buf);
        assert_eq!(good_len, buf.len(), "every frame must be valid");
        // After SetTerm(42), SetVote(7), SetVote(None), append 2 entries, Truncate(2):
        // term 42, no vote, log holds only the index-1 entry.
        assert_eq!(mirror.current_term, 42);
        assert_eq!(mirror.voted_for, None);
        assert_eq!(mirror.log.len(), 1);
        assert_eq!(mirror.log[0].index, 1);
    }

    #[test]
    fn recovers_term_vote_and_log_after_reopen() {
        let path = fresh_path("recovers_term_vote_and_log_after_reopen");

        let entries = sample_entries();
        {
            let mut s = FileStorage::open(&path).expect("open fresh");
            s.set_current_term(7);
            s.set_voted_for(Some(NodeId(3)));
            for e in &entries {
                s.append(e.clone());
            }
            // s dropped here (its File closes); the on-disk log is the source of truth.
        }

        let s = FileStorage::open(&path).expect("reopen recovers");
        assert_eq!(s.current_term(), 7, "current_term must survive a reopen");
        assert_eq!(
            s.voted_for(),
            Some(NodeId(3)),
            "voted_for must survive a reopen"
        );
        assert_eq!(s.last_log_index(), 3);
        assert_eq!(s.last_log_term(), 3);
        // The whole log must match, both via entry_at and entries_from.
        for e in &entries {
            assert_eq!(s.entry_at(e.index).as_ref(), Some(e));
        }
        assert_eq!(s.entries_from(1), entries);
        assert_eq!(s.entries_from(2), entries[1..].to_vec());

        cleanup(&path);
    }

    #[test]
    fn promote_replica_entry_survives_fsync_log_reopen() {
        // HA-8 crash-survival: a committed `PromoteReplica` (the failover ownership transfer) MUST
        // round-trip through the fsync log codec so a node that crashes mid-promotion replays the
        // committed entry on restart and converges to the NEW owner (it must NOT resurrect the old
        // one). The fsync log is a SEPARATE codec from the wire codec, so it is proven separately
        // here. Covers the empty-slots edge and a multi-slot batch (incl. the boundary slot).
        let path = fresh_path("promote_replica_entry_survives_fsync_log_reopen");
        let entries = vec![
            LogEntry {
                term: 4,
                index: 1,
                payload: EntryPayload::Config(ConfigCmd::PromoteReplica {
                    slots: vec![0, 1, 16_383],
                    new_primary: "cccccccccccccccccccccccccccccccccccccccc".to_owned(),
                }),
            },
            LogEntry {
                term: 4,
                index: 2,
                payload: EntryPayload::Config(ConfigCmd::PromoteReplica {
                    slots: vec![],
                    new_primary: "dddddddddddddddddddddddddddddddddddddddd".to_owned(),
                }),
            },
        ];
        {
            let mut s = FileStorage::open(&path).expect("open fresh");
            for e in &entries {
                s.append(e.clone());
            }
        }
        let s = FileStorage::open(&path).expect("reopen recovers");
        assert_eq!(s.last_log_index(), 2);
        for e in &entries {
            assert_eq!(
                s.entry_at(e.index).as_ref(),
                Some(e),
                "the committed PromoteReplica must replay byte-identical after a reopen"
            );
        }
        assert_eq!(s.entries_from(1), entries);
        cleanup(&path);
    }

    #[test]
    fn migration_entries_survive_fsync_log_reopen() {
        // HA-6 crash-survival: the three committed migration ConfigCmds (SetSlotMigrating /
        // SetSlotImporting / SetSlotStable) MUST round-trip through the fsync log codec so a node
        // that crashes mid-migration replays the committed migration tag / FLIP correctly. The
        // fsync log is a SEPARATE codec from the wire codec, so it is proven separately here.
        let path = fresh_path("migration_entries_survive_fsync_log_reopen");
        let entries = vec![
            LogEntry {
                term: 5,
                index: 1,
                payload: EntryPayload::Config(ConfigCmd::SetSlotMigrating {
                    slot: 16_383,
                    dest: "cccccccccccccccccccccccccccccccccccccccc".to_owned(),
                }),
            },
            LogEntry {
                term: 5,
                index: 2,
                payload: EntryPayload::Config(ConfigCmd::SetSlotImporting {
                    // HA-6: slot-then-src-then-dest must survive the fsync-log reopen; distinct
                    // src/dest ids prove both string fields recover in order.
                    slot: 0,
                    src: "dddddddddddddddddddddddddddddddddddddddd".to_owned(),
                    dest: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
                }),
            },
            LogEntry {
                term: 5,
                index: 3,
                payload: EntryPayload::Config(ConfigCmd::SetSlotStable { slot: 8192 }),
            },
        ];
        {
            let mut s = FileStorage::open(&path).expect("open fresh");
            for e in &entries {
                s.append(e.clone());
            }
        }
        let s = FileStorage::open(&path).expect("reopen recovers");
        assert_eq!(s.last_log_index(), 3);
        for e in &entries {
            assert_eq!(
                s.entry_at(e.index).as_ref(),
                Some(e),
                "the committed migration entry must replay byte-identical after a reopen"
            );
        }
        assert_eq!(s.entries_from(1), entries);
        cleanup(&path);
    }

    #[test]
    fn unassign_slots_entry_survives_fsync_log_reopen() {
        // A committed `UnassignSlots` (the CLUSTER DELSLOTS / DELSLOTSRANGE / FLUSHSLOTS analog) MUST
        // round-trip through the fsync log codec so a node that crashes after committing an UN-assign
        // replays it on restart and converges to the slot being unassigned (it must NOT resurrect the
        // old owner). The fsync log is a SEPARATE codec from the wire codec, so it is proven
        // separately here. Covers the empty-slots edge and a multi-slot batch (incl. the boundary
        // slot 16383).
        let path = fresh_path("unassign_slots_entry_survives_fsync_log_reopen");
        let entries = vec![
            LogEntry {
                term: 6,
                index: 1,
                payload: EntryPayload::Config(ConfigCmd::UnassignSlots {
                    slots: vec![0, 1, 16_383],
                }),
            },
            LogEntry {
                term: 6,
                index: 2,
                payload: EntryPayload::Config(ConfigCmd::UnassignSlots { slots: vec![] }),
            },
        ];
        {
            let mut s = FileStorage::open(&path).expect("open fresh");
            for e in &entries {
                s.append(e.clone());
            }
        }
        let s = FileStorage::open(&path).expect("reopen recovers");
        assert_eq!(s.last_log_index(), 2);
        for e in &entries {
            assert_eq!(
                s.entry_at(e.index).as_ref(),
                Some(e),
                "the committed UnassignSlots must replay byte-identical after a reopen"
            );
        }
        assert_eq!(s.entries_from(1), entries);
        cleanup(&path);
    }

    #[test]
    fn truncate_then_recover() {
        let path = fresh_path("truncate_then_recover");

        let new_at_3 = LogEntry {
            term: 9,
            index: 3,
            payload: EntryPayload::Bytes(b"post-truncate-entry-3".to_vec()),
        };
        {
            let mut s = FileStorage::open(&path).expect("open fresh");
            // Append entries 1..=5.
            for i in 1..=5u64 {
                s.append(LogEntry {
                    term: 1,
                    index: i,
                    payload: EntryPayload::Bytes(format!("orig-{i}").into_bytes()),
                });
            }
            assert_eq!(s.last_log_index(), 5);
            // Truncate from index 3 (drops 3, 4, 5), then append a NEW entry at 3.
            s.truncate_from(3);
            assert_eq!(s.last_log_index(), 2);
            s.append(new_at_3.clone());
            assert_eq!(s.last_log_index(), 3);
        }

        let s = FileStorage::open(&path).expect("reopen recovers post-truncate state");
        // The recovered log is [1, 2, 3'] (post-truncate), NOT the pre-truncate tail.
        assert_eq!(s.last_log_index(), 3);
        assert_eq!(
            s.entry_at(1).unwrap().payload,
            EntryPayload::Bytes(b"orig-1".to_vec())
        );
        assert_eq!(
            s.entry_at(2).unwrap().payload,
            EntryPayload::Bytes(b"orig-2".to_vec())
        );
        // Index 3 is the post-truncate entry, term 9, not the original term-1 "orig-3".
        assert_eq!(s.entry_at(3).as_ref(), Some(&new_at_3));
        // There is no index 4 or 5 anymore.
        assert_eq!(s.entry_at(4), None);
        assert_eq!(s.entry_at(5), None);

        cleanup(&path);
    }

    #[test]
    fn torn_trailing_record_is_ignored_on_replay() {
        let path = fresh_path("torn_trailing_record_is_ignored_on_replay");

        {
            let mut s = FileStorage::open(&path).expect("open fresh");
            s.set_current_term(4);
            s.set_voted_for(Some(NodeId(2)));
            s.append(LogEntry {
                term: 4,
                index: 1,
                payload: EntryPayload::Bytes(b"committed".to_vec()),
            });
        }

        // Append GARBAGE directly to the file: a plausible-looking but corrupt trailing
        // record (a header claiming a body plus a too-short / wrong-CRC body). This
        // simulates a write that died mid-append.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            // body_len says 16, crc is bogus, and we write only 4 body bytes: the body
            // runs past EOF AND the CRC is wrong, both of which replay must reject.
            f.write_all(&16u32.to_le_bytes()).unwrap();
            f.write_all(&0xDEAD_BEEFu32.to_le_bytes()).unwrap();
            f.write_all(&[0xAA, 0xBB, 0xCC, 0xDD]).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen: the torn record is dropped, recovery yields the last consistent state,
        // and the file is truncated to the last good offset.
        let len_before_reopen = std::fs::metadata(&path).unwrap().len();
        {
            let s = FileStorage::open(&path).expect("reopen ignores torn tail");
            assert_eq!(s.current_term(), 4);
            assert_eq!(s.voted_for(), Some(NodeId(2)));
            assert_eq!(s.last_log_index(), 1);
            assert_eq!(
                s.entry_at(1).unwrap().payload,
                EntryPayload::Bytes(b"committed".to_vec())
            );
        }
        let len_after_reopen = std::fs::metadata(&path).unwrap().len();
        assert!(
            len_after_reopen < len_before_reopen,
            "the torn tail must be truncated away ({len_after_reopen} < {len_before_reopen})"
        );

        // A subsequent append + reopen round-trips cleanly (the file is a clean
        // boundary again, so the new record is the new valid tail).
        {
            let mut s = FileStorage::open(&path).expect("reopen clean");
            s.append(LogEntry {
                term: 5,
                index: 2,
                payload: EntryPayload::Bytes(b"after-recovery".to_vec()),
            });
        }
        let s = FileStorage::open(&path).expect("final reopen");
        assert_eq!(s.last_log_index(), 2);
        assert_eq!(
            s.entry_at(2).unwrap().payload,
            EntryPayload::Bytes(b"after-recovery".to_vec())
        );

        cleanup(&path);
    }

    #[test]
    fn snapshot_save_load_and_compaction_survive_reopen() {
        // HA-3c: a FileStorage saves a snapshot, compacts the log below it, and on reopen
        // recovers the snapshot + the surviving log tail with term_at(last_included_index)
        // answered from the snapshot meta. Also clean up the sidecar.
        let path = fresh_path("snapshot_save_load_and_compaction_survive_reopen");
        let snap = snapshot_path(&path);
        let _ = std::fs::remove_file(&snap);

        let meta = SnapshotMeta {
            last_included_index: 3,
            last_included_term: 2,
        };
        let data = b"committed-config-snapshot-bytes".to_vec();
        let tail = vec![
            LogEntry {
                term: 2,
                index: 4,
                payload: EntryPayload::Bytes(b"tail-4".to_vec()),
            },
            LogEntry {
                term: 3,
                index: 5,
                payload: EntryPayload::Bytes(b"tail-5".to_vec()),
            },
        ];
        {
            let mut s = FileStorage::open(&path).expect("open fresh");
            s.set_current_term(3);
            s.set_voted_for(Some(NodeId(2)));
            // Append entries 1..=5, then snapshot at 3 and compact to 3. Entries 1..=3 are
            // term 2 (subsumed by the snapshot), the tail (4,5) is what `tail` declares.
            for i in 1..=3u64 {
                s.append(LogEntry {
                    term: 2,
                    index: i,
                    payload: EntryPayload::Noop,
                });
            }
            for entry in &tail {
                s.append(entry.clone());
            }
            s.save_snapshot(meta, &data);
            s.compact_to(3);
            // Post-compaction in-memory checks: the prefix is gone, the tail survives, and
            // term_at(3) is answered by the snapshot meta even though entry 3 is compacted.
            assert_eq!(s.log_start_index(), 4);
            assert_eq!(s.entry_at(3), None, "compacted entry is gone");
            assert_eq!(
                s.term_at(3),
                2,
                "snapshot answers term at last_included_index"
            );
            assert_eq!(s.entries_from(4), tail);
            assert_eq!(s.last_log_index(), 5);
        }

        // Reopen: the snapshot + the surviving tail recover; the compacted prefix stays gone.
        let s = FileStorage::open(&path).expect("reopen recovers");
        assert_eq!(s.current_term(), 3);
        assert_eq!(s.voted_for(), Some(NodeId(2)));
        let (loaded_meta, loaded_data) = s.load_snapshot().expect("snapshot recovered");
        assert_eq!(loaded_meta, meta);
        assert_eq!(loaded_data, data);
        assert_eq!(
            s.log_start_index(),
            4,
            "log starts above the snapshot after reopen"
        );
        assert_eq!(
            s.entry_at(3),
            None,
            "the compacted prefix stays gone after reopen"
        );
        assert_eq!(
            s.term_at(3),
            2,
            "term_at(last_included_index) survives reopen"
        );
        assert_eq!(s.entries_from(4), tail, "the surviving tail recovers");
        assert_eq!(s.last_log_index(), 5);

        cleanup(&path);
        let _ = std::fs::remove_file(&snap);
    }

    #[test]
    fn config_baseline_persists_across_reopen() {
        // FIX2 (HA-3d): FileStorage durably persists the config baseline beside the
        // snapshot and recovers it on reopen. Save a baseline + a snapshot, compact, drop
        // the store, REOPEN, and assert the loaded baseline is EXACTLY what was saved (and
        // the snapshot/tail recover alongside it). Before this fix FileStorage used the
        // trait-default no-op save/load, so the baseline was SILENTLY LOST on restart.
        let path = fresh_path("config_baseline_persists_across_reopen");
        let snap = snapshot_path(&path);
        let cfg = config_baseline_path(&path);
        let _ = std::fs::remove_file(&snap);
        let _ = std::fs::remove_file(&cfg);

        let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2)].into_iter().collect();
        let learners: BTreeSet<NodeId> = [NodeId(3)].into_iter().collect();
        let meta = SnapshotMeta {
            last_included_index: 3,
            last_included_term: 2,
        };
        {
            let mut s = FileStorage::open(&path).expect("open fresh");
            // No baseline saved yet -> the trait method returns None (the fallback path).
            assert_eq!(
                s.load_config_baseline(),
                None,
                "a fresh store has no persisted baseline"
            );
            for i in 1..=3u64 {
                s.append(LogEntry {
                    term: 2,
                    index: i,
                    payload: EntryPayload::Noop,
                });
            }
            // Save the baseline + snapshot, then compact (the engine's maybe_compact order).
            s.save_config_baseline(&voters, &learners);
            s.save_snapshot(meta, b"sm-bytes");
            s.compact_to(3);
            assert_eq!(
                s.load_config_baseline(),
                Some((voters.clone(), learners.clone())),
                "the live store serves the just-saved baseline from the mirror"
            );
        }

        // REOPEN: the durable baseline recovers EXACTLY, alongside the snapshot.
        let s = FileStorage::open(&path).expect("reopen recovers");
        assert_eq!(
            s.load_config_baseline(),
            Some((voters, learners)),
            "the persisted config baseline recovers byte-for-byte across a reopen"
        );
        assert!(s.load_snapshot().is_some(), "the snapshot recovers too");

        cleanup(&path);
        let _ = std::fs::remove_file(&snap);
        let _ = std::fs::remove_file(&cfg);
    }

    #[test]
    fn membership_config_reconstructs_via_snapshot_after_filestorage_restart() {
        // FIX2 end-to-end (HA-3d): a node drives the REAL engine on a FileStorage with
        // compaction ON; an AddLearner(2) commits then is compacted away; after a restart
        // the recovered config must STILL be `baseline + surviving-tail deltas`, i.e. the
        // exact pre-restart voter/learner set -- proving the durable baseline closes the
        // "membership history silently lost on restart" hole. Without FIX2 the restart
        // would seed the config from the constructor voter set (no learners) and the
        // compacted-away learner would VANISH.
        use ironcache_env::Monotonic;
        use ironcache_raft::{Effects, MembershipChange, RaftConfig, RaftNode, RaftRng};

        struct ZeroRng;
        impl RaftRng for ZeroRng {
            fn gen_below(&mut self, _bound: u64) -> u64 {
                0
            }
        }

        let path =
            fresh_path("membership_config_reconstructs_via_snapshot_after_filestorage_restart");
        let snap = snapshot_path(&path);
        let cfg = config_baseline_path(&path);
        let _ = std::fs::remove_file(&snap);
        let _ = std::fs::remove_file(&cfg);

        let config = RaftConfig {
            snapshot_threshold: 2,
            ..RaftConfig::default()
        };
        let voters: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
        {
            let storage = FileStorage::open(&path).expect("open fresh");
            let mut node = RaftNode::new(NodeId(1), voters.clone(), storage, config);
            let mut rng = ZeroRng;
            let now = Monotonic::ZERO;
            node.start(now, &mut rng, &mut Effects::new());
            node.on_timer(
                now,
                &mut rng,
                ironcache_raft::ELECTION_TIMEOUT,
                &mut Effects::new(),
            );
            // AddLearner(2) commits at once (single-voter majority), adopting {learner 2}.
            node.propose_membership_change(
                MembershipChange::AddLearner(NodeId(2)),
                now,
                &mut rng,
                &mut Effects::new(),
            )
            .expect("AddLearner accepted");
            // Pile on plain entries to cross the snapshot threshold and compact past the
            // AddLearner index.
            for tag in 0..6u8 {
                node.propose(
                    EntryPayload::Bytes(vec![tag]),
                    now,
                    &mut rng,
                    &mut Effects::new(),
                );
            }
            assert!(
                node.storage().log_start_index() > 1,
                "the log compacted past the AddLearner entry"
            );
            assert!(
                node.storage().load_config_baseline().is_some(),
                "the config baseline was persisted to the FileStorage sidecar"
            );
            assert_eq!(node.learners(), &[NodeId(2)].into_iter().collect());
        }

        // RESTART on the SAME FileStorage file: the recovered config restores the learner
        // from the durable baseline (its ConfigChange entry is compacted out of the log).
        let storage = FileStorage::open(&path).expect("reopen recovers");
        let restored = RaftNode::new(NodeId(1), voters, storage, config);
        assert_eq!(
            restored.learners(),
            &[NodeId(2)].into_iter().collect(),
            "the learner is reconstructed from the durable baseline after restart"
        );
        assert_eq!(
            restored.voters(),
            &[NodeId(1)].into_iter().collect(),
            "the voter set is reconstructed correctly after restart"
        );

        cleanup(&path);
        let _ = std::fs::remove_file(&snap);
        let _ = std::fs::remove_file(&cfg);
    }

    #[test]
    fn crash_in_save_compact_window_replays_without_log_start_desync() {
        // FIX 1: `maybe_compact` does `save_snapshot` (writes the `.snap` sidecar) THEN
        // `compact_to` (rewrites the log). A crash BETWEEN them leaves the sidecar at index
        // K PLUS the FULL, un-rewritten pre-compaction log (entries 1..N) on disk. On reopen
        // `open` seeds `log_start = K+1` from the sidecar, but a NON-snapshot-aware replay
        // would push ALL of 1..N so `log[0].index == 1` while `log_start == K+1`, making
        // `pos_of(K+1) == 0` return the WRONG entry (index 1) and corrupting the next apply.
        //
        // We reproduce the exact on-disk shape: a full log 1..=6 (NEVER compacted), then a
        // hand-written `.snap` sidecar at K=3, then reopen and assert the surviving tail
        // lines up correctly (no double-apply, no wrong entry at the boundary).
        let path = fresh_path("crash_in_save_compact_window_replays_without_log_start_desync");
        let snap = snapshot_path(&path);
        let _ = std::fs::remove_file(&snap);

        // Distinct payloads per index so a wrong-entry read is unambiguous.
        let entry = |i: u64| LogEntry {
            term: 2,
            index: i,
            payload: EntryPayload::Bytes(format!("entry-{i}").into_bytes()),
        };
        {
            let mut s = FileStorage::open(&path).expect("open fresh");
            s.set_current_term(2);
            s.set_voted_for(Some(NodeId(1)));
            for i in 1..=6u64 {
                s.append(entry(i));
            }
            // CRUCIALLY: do NOT call compact_to. The log on disk holds the FULL 1..=6.
        }

        // Write the `.snap` sidecar at K=3 directly (the save_snapshot half that survived a
        // crash before the compact_to rewrite). Its bytes are irrelevant to the log desync.
        {
            let meta = SnapshotMeta {
                last_included_index: 3,
                last_included_term: 2,
            };
            let frame = encode_snapshot_frame(meta, b"committed-prefix-snapshot");
            std::fs::write(&snap, frame).expect("write the surviving sidecar");
        }

        // Reopen: the sidecar seeds log_start = 4 BEFORE replay; the FIX 1 guard drops the
        // now-compacted prefix (entries 1..=3) and keeps only the tail 4..=6.
        let s = FileStorage::open(&path).expect("reopen reconciles the crash-window state");
        assert_eq!(
            s.log_start_index(),
            4,
            "log_start comes from the sidecar (K+1)"
        );
        // The boundary read is the CORRECT entry, not index 1 (the desync bug's symptom).
        assert_eq!(
            s.entry_at(4),
            Some(entry(4)),
            "entry_at(K+1=4) must return the RIGHT entry, not the compacted index-1 entry"
        );
        assert_eq!(s.entry_at(5), Some(entry(5)));
        assert_eq!(s.entry_at(6), Some(entry(6)));
        // The compacted prefix is gone; term_at(3) is answered from the snapshot meta.
        assert_eq!(
            s.entry_at(3),
            None,
            "the compacted prefix is dropped on replay"
        );
        assert_eq!(
            s.term_at(3),
            2,
            "snapshot answers term at last_included_index"
        );
        assert_eq!(s.last_log_index(), 6);
        // The surviving tail, fetched as a range, is exactly 4..=6 in order (log[0].index == 4).
        assert_eq!(s.entries_from(4), vec![entry(4), entry(5), entry(6)]);
        // The term + vote still recover from the surviving log records.
        assert_eq!(s.current_term(), 2);
        assert_eq!(s.voted_for(), Some(NodeId(1)));

        // A SUBSEQUENT apply uses the right entry: simulate the apply pipeline reading the
        // boundary index. entry_at(4) feeding StateMachine::apply would get "entry-4", not
        // the corrupt "entry-1" the desync would have produced.
        assert_eq!(
            s.entry_at(4).map(|e| e.payload),
            Some(EntryPayload::Bytes(b"entry-4".to_vec())),
            "the apply pipeline reads the correct payload at the boundary (no corruption)"
        );

        cleanup(&path);
        let _ = std::fs::remove_file(&snap);
    }

    #[test]
    fn fully_compacted_log_recovers_from_snapshot_alone() {
        // HA-3c edge: a snapshot whose last_included_index equals the last log index leaves an
        // EMPTY surviving log. A reopen must still report last_log_index / last_log_term from the
        // snapshot meta (a fully-compacted log ends at the snapshot), so the leader's prev-log
        // bookkeeping is correct.
        let path = fresh_path("fully_compacted_log_recovers_from_snapshot_alone");
        let snap = snapshot_path(&path);
        let _ = std::fs::remove_file(&snap);

        let meta = SnapshotMeta {
            last_included_index: 4,
            last_included_term: 3,
        };
        {
            let mut s = FileStorage::open(&path).expect("open fresh");
            s.set_current_term(3);
            for i in 1..=4u64 {
                s.append(LogEntry {
                    term: 3,
                    index: i,
                    payload: EntryPayload::Noop,
                });
            }
            s.save_snapshot(meta, b"snap-state");
            s.compact_to(4);
            assert_eq!(
                s.last_log_index(),
                4,
                "fully-compacted log ends at the snapshot"
            );
            assert_eq!(s.last_log_term(), 3);
            assert!(s.entries_from(5).is_empty());
        }
        let s = FileStorage::open(&path).expect("reopen recovers");
        assert_eq!(
            s.last_log_index(),
            4,
            "snapshot meta drives last_log_index after reopen"
        );
        assert_eq!(s.last_log_term(), 3);
        assert_eq!(s.term_at(4), 3);
        assert_eq!(s.log_start_index(), 5);
        let (m, _) = s.load_snapshot().expect("snapshot recovered");
        assert_eq!(m, meta);

        cleanup(&path);
        let _ = std::fs::remove_file(&snap);
    }

    #[test]
    fn empty_file_recovers_to_initial_state() {
        let path = fresh_path("empty_file_recovers_to_initial_state");

        // A brand-new path: open must create it and recover the initial state.
        let s = FileStorage::open(&path).expect("open brand-new path");
        assert_eq!(s.current_term(), 0, "fresh storage starts at term 0");
        assert_eq!(s.voted_for(), None, "fresh storage has no vote");
        assert_eq!(s.last_log_index(), 0, "fresh storage has an empty log");
        assert_eq!(s.last_log_term(), 0);
        assert_eq!(s.entries_from(1), Vec::<LogEntry>::new());
        assert_eq!(s.entry_at(1), None);
        // term_at(0) is the sentinel match; past-end is 0.
        assert_eq!(s.term_at(0), 0);
        assert_eq!(s.term_at(1), 0);

        cleanup(&path);
    }

    /// A parity test: a FileStorage and a MemStorage, driven through the IDENTICAL
    /// sequence of mutations, must answer every read the same way. This pins
    /// FileStorage as a drop-in for the in-memory store the engine is verified against,
    /// and a reopen of the FileStorage in the middle proves durability does not perturb
    /// that parity.
    #[test]
    fn parity_with_memstorage_across_a_mutation_sequence() {
        let path = fresh_path("parity_with_memstorage_across_a_mutation_sequence");

        let mut mem = MemStorage::new();
        let mut file = FileStorage::open(&path).expect("open fresh");

        // Apply a representative mutation sequence to BOTH, asserting parity throughout.
        // A closure cannot borrow both stores as &mut dyn easily, so apply inline.
        macro_rules! both {
            ($call:expr) => {{
                let _ = $call(&mut mem as &mut dyn RaftStorage);
                let _ = $call(&mut file as &mut dyn RaftStorage);
            }};
        }
        macro_rules! assert_parity {
            () => {{
                assert_eq!(
                    mem.current_term(),
                    file.current_term(),
                    "current_term parity"
                );
                assert_eq!(mem.voted_for(), file.voted_for(), "voted_for parity");
                assert_eq!(
                    mem.last_log_index(),
                    file.last_log_index(),
                    "last_log_index parity"
                );
                assert_eq!(
                    mem.last_log_term(),
                    file.last_log_term(),
                    "last_log_term parity"
                );
                for i in 0..=mem.last_log_index() + 2 {
                    assert_eq!(mem.term_at(i), file.term_at(i), "term_at({i}) parity");
                    assert_eq!(mem.entry_at(i), file.entry_at(i), "entry_at({i}) parity");
                    assert_eq!(
                        mem.entries_from(i),
                        file.entries_from(i),
                        "entries_from({i}) parity"
                    );
                }
            }};
        }

        both!(|s: &mut dyn RaftStorage| s.set_current_term(3));
        both!(|s: &mut dyn RaftStorage| s.set_voted_for(Some(NodeId(1))));
        assert_parity!();

        for e in sample_entries() {
            both!(|s: &mut dyn RaftStorage| s.append(e.clone()));
        }
        assert_parity!();

        // A bulk append (the append_entries path), then a truncate, then a fresh append.
        let bulk = vec![
            LogEntry {
                term: 3,
                index: 4,
                payload: EntryPayload::Noop,
            },
            LogEntry {
                term: 3,
                index: 5,
                payload: EntryPayload::Bytes(b"bulk-5".to_vec()),
            },
        ];
        both!(|s: &mut dyn RaftStorage| s.append_entries(&bulk));
        assert_parity!();

        both!(|s: &mut dyn RaftStorage| s.truncate_from(4));
        both!(|s: &mut dyn RaftStorage| s.set_current_term(4));
        both!(|s: &mut dyn RaftStorage| s.set_voted_for(None));
        both!(|s: &mut dyn RaftStorage| s.append(LogEntry {
            term: 4,
            index: 4,
            payload: EntryPayload::Bytes(b"new-4".to_vec()),
        }));
        assert_parity!();

        // Reopen the FileStorage from disk: durability must not perturb parity.
        drop(file);
        let file = FileStorage::open(&path).expect("reopen mid-sequence");
        assert_eq!(mem.current_term(), file.current_term());
        assert_eq!(mem.voted_for(), file.voted_for());
        assert_eq!(mem.last_log_index(), file.last_log_index());
        for i in 0..=mem.last_log_index() + 2 {
            assert_eq!(
                mem.entry_at(i),
                file.entry_at(i),
                "post-reopen entry_at({i})"
            );
            assert_eq!(
                mem.entries_from(i),
                file.entries_from(i),
                "post-reopen entries_from({i})"
            );
        }

        cleanup(&path);
    }

    /// A FileStorage drives the real engine and SURVIVES a restart with its persisted
    /// term + vote: open a store, run a node through an election (so it persists a term
    /// and a self-vote), drop the node, REOPEN the SAME FileStorage file into a fresh
    /// node, and assert the recovered node sees its prior term and vote (so it will not
    /// re-vote in that term). This is the safety property HA-4b exists for, exercised
    /// against the engine without needing the full TCP loopback harness.
    #[test]
    fn engine_recovers_persisted_term_and_vote_from_filestorage() {
        use ironcache_env::Monotonic;
        use ironcache_raft::{Effects, RaftConfig, RaftNode, RaftRng, Role};
        use std::collections::BTreeSet;

        // A deterministic, env-free RNG for the test (no real RNG: ADR-0003). Election
        // jitter does not affect the property under test; a fixed-zero draw is fine.
        // Declared before any statement so the items-after-statements lint is happy.
        struct ZeroRng;
        impl RaftRng for ZeroRng {
            fn gen_below(&mut self, _bound: u64) -> u64 {
                0
            }
        }

        let path = fresh_path("engine_recovers_persisted_term_and_vote_from_filestorage");

        // A single-voter cluster: an election timeout makes the node leader at once and,
        // crucially, PERSISTS term 1 and a self-vote through the FileStorage.
        let voters: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
        {
            let storage = FileStorage::open(&path).expect("open fresh");
            let mut node = RaftNode::new(NodeId(1), voters.clone(), storage, RaftConfig::default());
            let mut rng = ZeroRng;
            let mut out = Effects::new();
            // A fixed virtual time; the engine reads time only via this argument.
            let now = Monotonic::ZERO;
            node.start(now, &mut rng, &mut out);
            let mut out = Effects::new();
            // Fire the election timeout: increments term to 1, self-votes (both
            // persisted through FileStorage), and (single voter) wins leadership.
            node.on_timer(now, &mut rng, ironcache_raft::ELECTION_TIMEOUT, &mut out);
            assert_eq!(node.current_term(), 1);
            assert_eq!(node.role(), Role::Leader);
            assert_eq!(node.storage().voted_for(), Some(NodeId(1)));
            // node (and its FileStorage) dropped here, flushing nothing new (already
            // fsync'd); the on-disk log holds term 1 + the self-vote.
        }

        // Reopen the SAME file into a fresh engine: the recovered persistent state must
        // carry term 1 and the self-vote, so the node cannot grant a SECOND vote in
        // term 1 to some other candidate (the double-vote / split-brain hazard).
        let storage = FileStorage::open(&path).expect("reopen recovers");
        assert_eq!(
            storage.current_term(),
            1,
            "the recovered node must remember term 1"
        );
        assert_eq!(
            storage.voted_for(),
            Some(NodeId(1)),
            "the recovered node must remember it already voted in term 1"
        );

        // Prove it refuses a competing same-term vote. Rebuild a node on the recovered
        // storage and feed a RequestVote from a DIFFERENT candidate at term 1.
        let mut node = RaftNode::new(NodeId(1), voters, storage, RaftConfig::default());
        let mut rng = ZeroRng;
        let mut out = Effects::new();
        let now = Monotonic::ZERO;
        node.start(now, &mut rng, &mut out);
        let mut out = Effects::new();
        node.on_message(
            now,
            &mut rng,
            NodeId(2),
            ironcache_raft::RaftMsg::RequestVote {
                term: 1,
                candidate: NodeId(2),
                last_log_index: 0,
                last_log_term: 0,
            },
            &mut out,
        );
        // The reply must DENY the vote: we already voted for NodeId(1) in term 1.
        let granted = out.sends.iter().any(|(_, m)| {
            matches!(
                m,
                ironcache_raft::RaftMsg::RequestVoteResp {
                    vote_granted: true,
                    ..
                }
            )
        });
        assert!(
            !granted,
            "a recovered node must NOT grant a second vote in a term it already voted in"
        );

        cleanup(&path);
    }
}
