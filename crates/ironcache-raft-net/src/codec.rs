// SPDX-License-Identifier: MIT OR Apache-2.0
//! A compact, hand-rolled binary codec for [`RaftMsg`] (HA-4a).
//!
//! The pure engine ([`ironcache_raft`]) models a Raft RPC as a plain `RaftMsg`
//! value; to drive it over the real cluster bus the adapter must serialize that
//! value to bytes on the sending node and reconstruct the IDENTICAL value on the
//! receiving node. This module is that serialization, and it is the ONE place a
//! wire-level mistake could silently corrupt consensus (a flipped field, a
//! mis-framed entry vector, a truncated payload), so it is deliberately simple,
//! self-delimiting, and round-trip tested over EVERY variant
//! ([`super::tests::codec_round_trips_every_raftmsg_variant`]).
//!
//! ## Why hand-rolled (no serde)
//!
//! The workspace carries no serde dependency on the engine crates; adding one to a
//! boundary adapter would pull a derive-macro tree into the consensus path for no
//! benefit. The `RaftMsg` surface is small and stable (four RPC variants plus the
//! local `Propose`, a handful of scalar fields, and a `Vec<LogEntry>` whose payload
//! is one of three shapes), so a fixed-layout binary encoding is both smaller and
//! easier to audit than a generic format.
//!
//! ## Layout
//!
//! Everything is little-endian and length-delimited so decode never reads past a
//! field. A `u64` is 8 bytes; a [`NodeId`] is its inner `u64`; a variable-length
//! blob (a `Bytes` payload, a config id `String`, a slot list) is a `u64` length
//! prefix followed by that many bytes / elements. Each message, entry, and payload
//! leads with a single discriminant byte. Decode is total: any malformed,
//! truncated, or unknown-discriminant input yields `None` (the caller drops the
//! frame; Raft retries via heartbeat), never a panic.
//!
//! The encoded `RaftMsg` is carried as the third argument of the cluster-bus
//! command `["RAFTMSG", <self_node_id_decimal>, <encoded-bytes>]`
//! (see [`super::RAFTMSG`]); this module owns only the third argument's bytes, not
//! the RESP framing around it.

use std::collections::BTreeSet;

use ironcache_raft::{ConfigCmd, EntryPayload, LogEntry, MembershipChange, NodeId, RaftMsg};

// Message discriminants (the outer `RaftMsg` variant tag).
const MSG_REQUEST_VOTE: u8 = 1;
const MSG_REQUEST_VOTE_RESP: u8 = 2;
const MSG_APPEND_ENTRIES: u8 = 3;
const MSG_APPEND_ENTRIES_RESP: u8 = 4;
const MSG_PROPOSE: u8 = 5;
// HA-9 leader-forwarding: new transport-level message tags continue from 6 (the next free value).
const MSG_FORWARD_PROPOSE: u8 = 6;
const MSG_FORWARD_PROPOSE_RESULT: u8 = 7;
// HA-3c snapshot install: new RPC message tags continue from 8 (the next free value).
const MSG_INSTALL_SNAPSHOT: u8 = 8;
const MSG_INSTALL_SNAPSHOT_RESP: u8 = 9;
// PROD-9 Pre-Vote (Ongaro section 9.6): additive RPC tags continue from 10 (the next free
// value). A peer that predates pre-vote returns None on these unknown discriminants (a
// dropped frame), so a mixed-version cluster mid-upgrade degrades to no-pre-vote rather than
// misparsing -- the frames are strictly additive and never collide with an existing tag.
const MSG_PRE_VOTE: u8 = 10;
const MSG_PRE_VOTE_RESP: u8 = 11;

// Payload discriminants (the `EntryPayload` variant tag).
const PAYLOAD_NOOP: u8 = 0;
const PAYLOAD_BYTES: u8 = 1;
const PAYLOAD_CONFIG: u8 = 2;
// HA-3d raft cluster-membership change: the next free payload discriminant.
const PAYLOAD_CONFIG_CHANGE: u8 = 3;

// Membership-change discriminants (the `MembershipChange` variant tag, HA-3d).
const MEMBER_ADD_VOTER: u8 = 0;
const MEMBER_REMOVE_VOTER: u8 = 1;
const MEMBER_ADD_LEARNER: u8 = 2;
const MEMBER_PROMOTE_LEARNER: u8 = 3;
const MEMBER_REMOVE_LEARNER: u8 = 4;

// Config-command discriminants (the `ConfigCmd` variant tag).
const CFG_ADD_NODE: u8 = 0;
const CFG_REMOVE_NODE: u8 = 1;
const CFG_SET_SLOT_OWNER: u8 = 2;
const CFG_ASSIGN_SLOTS: u8 = 3;
const CFG_SET_CONFIG_EPOCH: u8 = 4;
const CFG_ASSIGN_REPLICA: u8 = 5;
const CFG_PROMOTE_REPLICA: u8 = 6;
// HA-6 online slot migration: new discriminants continue from 7 (the next free value).
const CFG_SET_SLOT_MIGRATING: u8 = 7;
const CFG_SET_SLOT_IMPORTING: u8 = 8;
const CFG_SET_SLOT_STABLE: u8 = 9;
// UNASSIGN slots (CLUSTER DELSLOTS / DELSLOTSRANGE / FLUSHSLOTS): the next free discriminant.
const CFG_UNASSIGN_SLOTS: u8 = 10;

// ---------------------------------------------------------------------------
// Encoding.
// ---------------------------------------------------------------------------

/// Serialize a [`RaftMsg`] to the wire bytes carried in the `RAFTMSG` command.
///
/// The inverse of [`decode_raft_msg`]; the pair round-trips every variant
/// byte-for-byte (the codec round-trip test is the gate). The output is a fresh
/// `Vec<u8>` the adapter hands to [`ironcache_clusterbus::PeerConn::request`] as a
/// bulk-string argument.
// One flat match arm per `RaftMsg` variant (PROD-9 added the two Pre-Vote arms), so the
// function is long by construction; the per-variant layout is the clearest form and the
// decode mirror is held in lockstep, so the length is intentional rather than refactorable.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn encode_raft_msg(msg: &RaftMsg) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    match msg {
        RaftMsg::RequestVote {
            term,
            candidate,
            last_log_index,
            last_log_term,
        } => {
            out.push(MSG_REQUEST_VOTE);
            put_u64(&mut out, *term);
            put_node(&mut out, *candidate);
            put_u64(&mut out, *last_log_index);
            put_u64(&mut out, *last_log_term);
        }
        RaftMsg::RequestVoteResp { term, vote_granted } => {
            out.push(MSG_REQUEST_VOTE_RESP);
            put_u64(&mut out, *term);
            out.push(u8::from(*vote_granted));
        }
        RaftMsg::PreVote {
            term,
            candidate,
            last_log_index,
            last_log_term,
        } => {
            // PROD-9 Pre-Vote: byte-identical layout to RequestVote (term, candidate, the
            // two up-to-date fields), distinguished only by the MSG_PRE_VOTE tag.
            out.push(MSG_PRE_VOTE);
            put_u64(&mut out, *term);
            put_node(&mut out, *candidate);
            put_u64(&mut out, *last_log_index);
            put_u64(&mut out, *last_log_term);
        }
        RaftMsg::PreVoteResp { term, vote_granted } => {
            out.push(MSG_PRE_VOTE_RESP);
            put_u64(&mut out, *term);
            out.push(u8::from(*vote_granted));
        }
        RaftMsg::AppendEntries {
            term,
            leader,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            out.push(MSG_APPEND_ENTRIES);
            put_u64(&mut out, *term);
            put_node(&mut out, *leader);
            put_u64(&mut out, *prev_log_index);
            put_u64(&mut out, *prev_log_term);
            put_u64(&mut out, *leader_commit);
            // The entries vector, length-prefixed so decode knows exactly how many
            // to read (a heartbeat is simply a zero-length vector).
            put_u64(&mut out, entries.len() as u64);
            for entry in entries {
                put_entry(&mut out, entry);
            }
        }
        RaftMsg::AppendEntriesResp {
            term,
            success,
            match_index,
        } => {
            out.push(MSG_APPEND_ENTRIES_RESP);
            put_u64(&mut out, *term);
            out.push(u8::from(*success));
            put_u64(&mut out, *match_index);
        }
        RaftMsg::Propose { payload } => {
            out.push(MSG_PROPOSE);
            put_payload(&mut out, payload);
        }
        RaftMsg::ForwardPropose { corr, payload } => {
            // HA-9: the correlation id then the opaque payload to propose on the leader.
            out.push(MSG_FORWARD_PROPOSE);
            put_u64(&mut out, *corr);
            put_payload(&mut out, payload);
        }
        RaftMsg::ForwardProposeResult { corr, outcome } => {
            // HA-9: the correlation id then the outcome as a present-flag + index. A
            // `None` writes flag 0 (and a zero index that decode ignores); `Some(i)`
            // writes flag 1 + the index. The fixed-width tail keeps decode total.
            out.push(MSG_FORWARD_PROPOSE_RESULT);
            put_u64(&mut out, *corr);
            if let Some(index) = outcome {
                out.push(1);
                put_u64(&mut out, *index);
            } else {
                out.push(0);
                put_u64(&mut out, 0);
            }
        }
        RaftMsg::InstallSnapshot {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            offset,
            data,
            done,
            voters,
            learners,
        } => {
            // HA-3c: term, leader, the snapshot's (index, term). PROD-9: the chunk OFFSET,
            // the chunk bytes as a length-prefixed blob, and the `done` last-chunk flag (a
            // single byte). HA-3d: the config baseline (voter + learner node sets) the
            // snapshot reflects, each a length-prefixed list of NodeIds, so an installing
            // follower can rebuild its configuration (its log below the snapshot is gone).
            // The new `offset` / `done` fields are positioned BETWEEN the meta and the config
            // baseline; they extend the frame additively (an old peer that predates chunking
            // returns None on decode rather than misparsing, see decode_raft_msg).
            out.push(MSG_INSTALL_SNAPSHOT);
            put_u64(&mut out, *term);
            put_node(&mut out, *leader_id);
            put_u64(&mut out, *last_included_index);
            put_u64(&mut out, *last_included_term);
            put_u64(&mut out, *offset);
            put_blob(&mut out, data);
            out.push(u8::from(*done));
            put_node_set(&mut out, voters);
            put_node_set(&mut out, learners);
        }
        RaftMsg::InstallSnapshotResp {
            term,
            last_included_index,
            installed,
            next_offset,
        } => {
            // HA-3c: the follower's term (the leader steps down on a higher one) PLUS the
            // ECHOED snapshot index. PROD-9: the `installed` flag (the final chunk was
            // applied) and the `next_offset` the follower next expects (for a buffered /
            // rejected chunk). The leader advances match_index/next_index from the echoed
            // index only on `installed`, NOT from its own current snapshot meta, so a second
            // compaction inside the in-flight InstallSnapshot window can never over-advance
            // the follower's match_index (Figure 13).
            out.push(MSG_INSTALL_SNAPSHOT_RESP);
            put_u64(&mut out, *term);
            put_u64(&mut out, *last_included_index);
            out.push(u8::from(*installed));
            put_u64(&mut out, *next_offset);
        }
    }
    out
}

/// Append a [`LogEntry`]: its term, index, then its payload.
fn put_entry(out: &mut Vec<u8>, entry: &LogEntry) {
    put_u64(out, entry.term);
    put_u64(out, entry.index);
    put_payload(out, &entry.payload);
}

/// Append an [`EntryPayload`] led by its discriminant byte.
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

/// Append a [`MembershipChange`] led by its discriminant byte, then the one NodeId it
/// names (HA-3d). Every variant carries exactly one node.
fn put_membership(out: &mut Vec<u8>, change: MembershipChange) {
    match change {
        MembershipChange::AddVoter(node) => {
            out.push(MEMBER_ADD_VOTER);
            put_node(out, node);
        }
        MembershipChange::RemoveVoter(node) => {
            out.push(MEMBER_REMOVE_VOTER);
            put_node(out, node);
        }
        MembershipChange::AddLearner(node) => {
            out.push(MEMBER_ADD_LEARNER);
            put_node(out, node);
        }
        MembershipChange::PromoteLearner(node) => {
            out.push(MEMBER_PROMOTE_LEARNER);
            put_node(out, node);
        }
        MembershipChange::RemoveLearner(node) => {
            out.push(MEMBER_REMOVE_LEARNER);
            put_node(out, node);
        }
    }
}

/// A length-prefixed set of [`NodeId`]s (a `u64` count then each id's inner `u64`), in
/// the set's deterministic ascending order (HA-3d config baseline on the wire).
fn put_node_set(out: &mut Vec<u8>, set: &BTreeSet<NodeId>) {
    put_u64(out, set.len() as u64);
    for &id in set {
        put_node(out, id);
    }
}

/// Append a [`ConfigCmd`] led by its discriminant byte.
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
            // The inverse of AssignSlots; a length-prefixed slot list with NO node string (every
            // node clears the same slots). A distinct discriminant keeps it unambiguous on the wire.
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
            // Same shape as AssignSlots (node string + length-prefixed slot list); a distinct
            // discriminant keeps the two unambiguous on the wire.
            out.push(CFG_ASSIGN_REPLICA);
            put_str(out, node);
            put_u64(out, slots.len() as u64);
            for slot in slots {
                put_u16(out, *slot);
            }
        }
        ConfigCmd::PromoteReplica { slots, new_primary } => {
            // HA-8 failover. Encoded length-prefixed-slots-then-node (the slots lead here, vs the
            // node-then-slots shape of AssignSlots/AssignReplica, matching the variant's field
            // order); a distinct discriminant keeps it unambiguous on the wire + in the log.
            out.push(CFG_PROMOTE_REPLICA);
            put_u64(out, slots.len() as u64);
            for slot in slots {
                put_u16(out, *slot);
            }
            put_str(out, new_primary);
        }
        ConfigCmd::SetSlotMigrating { slot, dest } => {
            // HA-6: a single slot + the dest node id (slot-then-node, matching the variant's field
            // order); a distinct discriminant keeps it unambiguous on the wire + in the log.
            out.push(CFG_SET_SLOT_MIGRATING);
            put_u16(out, *slot);
            put_str(out, dest);
        }
        ConfigCmd::SetSlotImporting { slot, src, dest } => {
            // HA-6: a single slot + the src node id + the dest node id (slot-then-src-then-dest,
            // matching the variant's field order). The discriminant is UNCHANGED; the `dest` field
            // is appended, so the decoder reads src then dest in the same order.
            out.push(CFG_SET_SLOT_IMPORTING);
            put_u16(out, *slot);
            put_str(out, src);
            put_str(out, dest);
        }
        ConfigCmd::SetSlotStable { slot } => {
            // HA-6: just the slot (clears its migration state).
            out.push(CFG_SET_SLOT_STABLE);
            put_u16(out, *slot);
        }
    }
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_node(out: &mut Vec<u8>, id: NodeId) {
    put_u64(out, id.0);
}

/// A length-prefixed byte blob: a `u64` length then the bytes.
fn put_blob(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// A length-prefixed UTF-8 string (encoded as a byte blob of its UTF-8 bytes).
fn put_str(out: &mut Vec<u8>, s: &str) {
    put_blob(out, s.as_bytes());
}

// ---------------------------------------------------------------------------
// Decoding.
// ---------------------------------------------------------------------------

/// A forward-only cursor over the encoded bytes. Every read is bounds-checked and
/// returns `None` on a short buffer, so a truncated or corrupt frame can never
/// over-read; the whole decode bubbles the `None` up to the caller, which drops the
/// frame (Raft re-sends on the next heartbeat).
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

    fn node(&mut self) -> Option<NodeId> {
        Some(NodeId(self.u64()?))
    }

    fn bool(&mut self) -> Option<bool> {
        // Any non-zero byte decodes as true; the encoder only ever writes 0/1.
        Some(self.u8()? != 0)
    }

    /// A length-prefixed byte blob: read the `u64` length, then exactly that many
    /// bytes (bounds-checked).
    fn blob(&mut self) -> Option<Vec<u8>> {
        let len = usize::try_from(self.u64()?).ok()?;
        let end = self.pos.checked_add(len)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice.to_vec())
    }

    /// A length-prefixed UTF-8 string; rejects invalid UTF-8 (returns `None`).
    fn string(&mut self) -> Option<String> {
        String::from_utf8(self.blob()?).ok()
    }

    /// Whether the cursor has consumed the whole buffer (a well-formed frame has no
    /// trailing bytes; trailing garbage is rejected so a corrupt tail is not
    /// silently ignored).
    fn at_end(&self) -> bool {
        self.pos == self.buf.len()
    }
}

/// Deserialize a [`RaftMsg`] from the `RAFTMSG` command's encoded-bytes argument.
///
/// The inverse of [`encode_raft_msg`]. Returns `None` for ANY input that is not a
/// byte-exact encoding of some `RaftMsg` (unknown discriminant, short buffer,
/// non-UTF-8 config string, or trailing bytes after a complete message); the caller
/// treats a `None` as a dropped frame.
#[must_use]
pub fn decode_raft_msg(buf: &[u8]) -> Option<RaftMsg> {
    let mut cur = Cursor::new(buf);
    let msg = match cur.u8()? {
        MSG_REQUEST_VOTE => RaftMsg::RequestVote {
            term: cur.u64()?,
            candidate: cur.node()?,
            last_log_index: cur.u64()?,
            last_log_term: cur.u64()?,
        },
        MSG_REQUEST_VOTE_RESP => RaftMsg::RequestVoteResp {
            term: cur.u64()?,
            vote_granted: cur.bool()?,
        },
        MSG_PRE_VOTE => RaftMsg::PreVote {
            term: cur.u64()?,
            candidate: cur.node()?,
            last_log_index: cur.u64()?,
            last_log_term: cur.u64()?,
        },
        MSG_PRE_VOTE_RESP => RaftMsg::PreVoteResp {
            term: cur.u64()?,
            vote_granted: cur.bool()?,
        },
        MSG_APPEND_ENTRIES => {
            let term = cur.u64()?;
            let leader = cur.node()?;
            let prev_log_index = cur.u64()?;
            let prev_log_term = cur.u64()?;
            let leader_commit = cur.u64()?;
            let count = usize::try_from(cur.u64()?).ok()?;
            let mut entries = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                entries.push(get_entry(&mut cur)?);
            }
            RaftMsg::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            }
        }
        MSG_APPEND_ENTRIES_RESP => RaftMsg::AppendEntriesResp {
            term: cur.u64()?,
            success: cur.bool()?,
            match_index: cur.u64()?,
        },
        MSG_PROPOSE => RaftMsg::Propose {
            payload: get_payload(&mut cur)?,
        },
        MSG_FORWARD_PROPOSE => RaftMsg::ForwardPropose {
            corr: cur.u64()?,
            payload: get_payload(&mut cur)?,
        },
        MSG_FORWARD_PROPOSE_RESULT => {
            let corr = cur.u64()?;
            // The present-flag (0/1) then a fixed u64 index (meaningful only when 1).
            let present = cur.bool()?;
            let index = cur.u64()?;
            RaftMsg::ForwardProposeResult {
                corr,
                outcome: if present { Some(index) } else { None },
            }
        }
        MSG_INSTALL_SNAPSHOT => RaftMsg::InstallSnapshot {
            // Read in WIRE order: term, leader, (index, term), PROD-9 (offset, data blob,
            // done), then the HA-3d config baseline (voters then learners). Field order in
            // this literal matches the encode order so the cursor reads sequentially correct;
            // a truncated frame (e.g. an old encoder that omitted offset/done) returns None at
            // the first missing field, dropping the frame rather than misparsing.
            term: cur.u64()?,
            leader_id: cur.node()?,
            last_included_index: cur.u64()?,
            last_included_term: cur.u64()?,
            offset: cur.u64()?,
            data: cur.blob()?,
            done: cur.bool()?,
            voters: get_node_set(&mut cur)?,
            learners: get_node_set(&mut cur)?,
        },
        MSG_INSTALL_SNAPSHOT_RESP => RaftMsg::InstallSnapshotResp {
            term: cur.u64()?,
            last_included_index: cur.u64()?,
            installed: cur.bool()?,
            next_offset: cur.u64()?,
        },
        _ => return None,
    };
    // Reject trailing bytes: a complete message must consume the whole frame.
    if cur.at_end() { Some(msg) } else { None }
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

/// Read a [`MembershipChange`] by its discriminant byte, then the one NodeId (HA-3d).
fn get_membership(cur: &mut Cursor<'_>) -> Option<MembershipChange> {
    match cur.u8()? {
        MEMBER_ADD_VOTER => Some(MembershipChange::AddVoter(cur.node()?)),
        MEMBER_REMOVE_VOTER => Some(MembershipChange::RemoveVoter(cur.node()?)),
        MEMBER_ADD_LEARNER => Some(MembershipChange::AddLearner(cur.node()?)),
        MEMBER_PROMOTE_LEARNER => Some(MembershipChange::PromoteLearner(cur.node()?)),
        MEMBER_REMOVE_LEARNER => Some(MembershipChange::RemoveLearner(cur.node()?)),
        _ => None,
    }
}

/// Read a length-prefixed set of [`NodeId`]s (HA-3d). The encoder wrote them in
/// ascending order; collecting into a `BTreeSet` restores the same set regardless.
fn get_node_set(cur: &mut Cursor<'_>) -> Option<BTreeSet<NodeId>> {
    let count = usize::try_from(cur.u64()?).ok()?;
    let mut set = BTreeSet::new();
    for _ in 0..count {
        // A bounds-checked read returns None on a short buffer, so a bogus huge count
        // bubbles up as a dropped frame rather than allocating; the set itself is small.
        set.insert(cur.node()?);
    }
    Some(set)
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
            // Read in WIRE order: slot, then src, then the appended dest. The struct-literal field
            // order here matches the encode order so the cursor reads sequentially correct.
            slot: cur.u16()?,
            src: cur.string()?,
            dest: cur.string()?,
        }),
        CFG_SET_SLOT_STABLE => Some(ConfigCmd::SetSlotStable { slot: cur.u16()? }),
        _ => None,
    }
}
