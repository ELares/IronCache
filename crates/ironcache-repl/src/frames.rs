// SPDX-License-Identifier: MIT OR Apache-2.0
//! The replication wire frames and their hand-rolled RESP codec (HA-7a).
//!
//! Replication speaks the SAME RESP-array-of-bulk-strings shape the cluster bus
//! and the Raft `RAFTMSG` codec speak (`*N\r\n$len\r\narg\r\n...`), but on its OWN
//! dedicated data-plane port and listener (see the crate root) so a replication
//! frame can never sit in front of a Raft heartbeat. This module owns only the two
//! frames HA-7a needs and is deliberately extensible: the bulk full-sync
//! (`FULLSYNC` / `SYNCKV`) and the steady-state KV stream are HA-7b/7c and add new
//! verbs without changing what is here.
//!
//! ## The frames (7a)
//!
//! - [`Frame::ReplConf`] = `["REPLCONF", <replica-node-id>, <ack-offset>]`, sent
//!   REPLICA -> PRIMARY. It is BOTH the attach handshake (the first frame a replica
//!   sends on connect) AND the steady-state ack: `ack` is the highest [`ReplOffset`]
//!   the replica has durably tracked, so on a reconnect it is the resume point the
//!   primary reads.
//! - [`Frame::ReplPing`] = `["REPLPING", <replid>, <offset>]`, sent
//!   PRIMARY -> REPLICA. The heartbeat carrying the primary's current [`ReplId`] and
//!   [`ReplOffset`]; the replica advances its observed offset from it.
//!
//! ## Why hand-rolled (no serde)
//!
//! Same rationale as the Raft codec ([`ironcache_raft_net::codec`] in spirit): the
//! workspace keeps serde off the transport adapters, and the frame surface is two
//! verbs over a node-id and a `u64`, so a fixed RESP encoding is smaller and easier
//! to audit than a generic format. The decoder is total: any malformed, truncated,
//! or unknown frame yields `None` / "need more bytes", never a panic.

use crate::cursor::{ReplId, ReplOffset};

/// The REPLCONF verb (replica -> primary: attach handshake + steady-state ack).
pub const REPLCONF: &[u8] = b"REPLCONF";
/// The REPLPING verb (primary -> replica: heartbeat carrying replid + offset).
pub const REPLPING: &[u8] = b"REPLPING";
/// The FULLSYNC verb (primary -> replica: begin a full sync, naming the resume offset).
pub const FULLSYNC: &[u8] = b"FULLSYNC";
/// The SYNCKV verb (primary -> replica: one snapshot entry in the full-sync stream).
pub const SYNCKV: &[u8] = b"SYNCKV";
/// The SYNCEND verb (primary -> replica: terminate the full-sync stream).
pub const SYNCEND: &[u8] = b"SYNCEND";
/// The STREAMPUT verb (primary -> replica: one steady-state put in the tail stream).
pub const STREAMPUT: &[u8] = b"STREAMPUT";
/// The STREAMDEL verb (primary -> replica: one steady-state delete in the tail stream).
pub const STREAMDEL: &[u8] = b"STREAMDEL";
/// The IMPORTREQ verb (IMPORTER -> SOURCE: scope this attach to ONE migrating slot).
///
/// HA-6 data-copy: sent by an IMPORTING destination right after dialing the source's repl
/// listener, BEFORE the [`Frame::ReplConf`] attach handshake. It names the single slot whose
/// keys + mutations the source should ship (a SCOPED snapshot+stream), so a slot migration
/// reuses the HA-5b snapshot + HA-7c tail transport WITHOUT shipping the source's whole
/// keyspace. A plain replica NEVER sends it (it wants the whole store), so the replica path is
/// byte-identical: the source defaults to the unfiltered full transfer when no IMPORTREQ arrives.
pub const IMPORTREQ: &[u8] = b"IMPORTREQ";

/// A replication frame is malformed: a RESP framing error, or a complete command
/// that is not a decodable replication frame (wrong verb / arity, a non-numeric
/// field, or a bad replid). The caller drops the connection on this error (the peer
/// reconnects); it is deliberately opaque (one cause) rather than a stringly error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameError;

/// A decoded replication frame. Only the two HA-7a frames live here; HA-7b/7c add
/// variants (full-sync, KV stream) WITHOUT changing these, so the codec is
/// forward-extensible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// `["REPLCONF", <replica-node-id>, <ack-offset>]` (replica -> primary).
    ///
    /// The attach handshake and the steady-state ack in one frame: `node` is the
    /// replica's id and `ack` is the highest offset it has durably tracked (its
    /// resume point on reconnect).
    ReplConf {
        /// The replica's node id (its identity to the primary).
        node: u64,
        /// The highest [`ReplOffset`] the replica has durably tracked.
        ack: ReplOffset,
    },
    /// `["REPLPING", <replid>, <offset>]` (primary -> replica).
    ///
    /// The primary's heartbeat: its current replication id and logical offset. The
    /// replica advances its observed offset from `offset`.
    ReplPing {
        /// The primary's replication id (40-hex).
        replid: ReplId,
        /// The primary's current logical [`ReplOffset`].
        offset: ReplOffset,
    },
    /// `["FULLSYNC", <replid>, <end_offset>]` (primary -> replica).
    ///
    /// BEGINS a full sync (HA-7b): the primary is about to stream its whole snapshot.
    /// `replid` names the stream the replica will be on; `end_offset` is the primary's
    /// [`ReplOffset`] at the snapshot CUT -- the point the snapshot is consistent through
    /// and where the HA-7c steady-state tail resumes. The replica creates a FRESH store and
    /// applies the [`Frame::SyncKv`] entries that follow until [`Frame::SyncEnd`].
    FullSync {
        /// The primary's replication id for this stream.
        replid: ReplId,
        /// The offset at the snapshot cut (the resume point for the 7c tail).
        end_offset: ReplOffset,
    },
    /// `["SYNCKV", <db>, <key>, <kvobj-bytes>]` (primary -> replica).
    ///
    /// One snapshot entry in the full-sync stream (HA-7b): the database index, the key, and
    /// the [`crate::kvcodec::encode_kvobj`] encoding of the entry's [`ironcache_store::KvObj`].
    /// The replica decodes `kvobj_bytes` and applies it to the in-progress fresh store via
    /// `insert_object`. One frame per live key; the stream is bounded by [`Frame::SyncEnd`].
    SyncKv {
        /// The database the entry belongs to.
        db: u32,
        /// The key bytes (also carried inside `kvobj_bytes`; here for routing/debugging).
        key: Vec<u8>,
        /// The [`crate::kvcodec`] wire encoding of the entry's `KvObj`.
        kvobj_bytes: Vec<u8>,
    },
    /// `["SYNCEND", <end_offset>]` (primary -> replica).
    ///
    /// TERMINATES the full-sync stream (HA-7b): every snapshot entry has been sent. The
    /// replica completes the sync (the fresh store is fully loaded) and adopts `end_offset`
    /// as its resume point for the HA-7c tail. `end_offset` repeats the [`Frame::FullSync`]
    /// value so the terminator is self-contained.
    SyncEnd {
        /// The offset at the snapshot cut (the resume point for the 7c tail).
        end_offset: ReplOffset,
    },
    /// `["STREAMPUT", <offset>, <db>, <key>, <kvobj-bytes>]` (primary -> replica).
    ///
    /// One STEADY-STATE put in the HA-7c tail (a create / overwrite / in-place collection
    /// edit / TTL change). `offset` is the LOGICAL write-sequence position this put occupies
    /// (the post-snapshot tail is strictly increasing, gap-free per shard), `db`+`key` route
    /// it, and `kvobj_bytes` is the [`crate::kvcodec::encode_kvobj`] post-image. The replica
    /// verifies `offset == applied_offset.next()`, applies the decoded `KvObj` via
    /// `insert_object` (idempotent: an overwrite replaces in place), and advances its
    /// `applied_offset`. An out-of-order offset triggers a full re-sync (HA-7b).
    StreamPut {
        /// The logical offset of this write in the tail (strictly increasing per shard).
        offset: ReplOffset,
        /// The database the write belongs to.
        db: u32,
        /// The key bytes (also inside `kvobj_bytes`; here for routing/debugging).
        key: Vec<u8>,
        /// The [`crate::kvcodec`] wire encoding of the post-image `KvObj`.
        kvobj_bytes: Vec<u8>,
    },
    /// `["STREAMDEL", <offset>, <db>, <key>]` (primary -> replica).
    ///
    /// One STEADY-STATE delete in the HA-7c tail (an explicit delete, an expiry, a flush, an
    /// eviction, a RENAME source removal, or an in-place edit that drained a collection to
    /// empty). `offset` is the logical write-sequence position; the replica verifies it is
    /// the next expected offset, removes `(db, key)` idempotently (a delete of an absent key
    /// is a no-op), and advances `applied_offset`.
    StreamDel {
        /// The logical offset of this delete in the tail (strictly increasing per shard).
        offset: ReplOffset,
        /// The database the key left.
        db: u32,
        /// The key removed.
        key: Vec<u8>,
    },
    /// `["IMPORTREQ", <slot>]` (IMPORTER -> SOURCE).
    ///
    /// HA-6 data-copy attach scope: the connecting node is the IMPORTING destination of `slot`
    /// and wants only THAT slot's keys + mutations, not the whole store. Sent before the
    /// [`Frame::ReplConf`] handshake; the source records the slot and FILTERS its full-sync
    /// snapshot and its steady-state tail to keys hashing to it (`key_slot(key) == slot`). A
    /// plain replica never sends it, so the source's default unfiltered transfer (and thus the
    /// replica path) is byte-identical.
    ImportReq {
        /// The single slot whose keys + mutations the source should ship (scoped transfer).
        slot: u16,
    },
}

impl Frame {
    /// Encode the frame as a RESP array of bulk strings, the exact shape the
    /// cluster bus and Raft codec use, so the same inbound RESP parser decodes it.
    ///
    /// The inverse of [`Frame::decode`]; the pair round-trips every variant
    /// byte-for-byte (the round-trip test is the gate).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Frame::ReplConf { node, ack } => encode_command(&[
                REPLCONF,
                node.to_string().as_bytes(),
                ack.0.to_string().as_bytes(),
            ]),
            Frame::ReplPing { replid, offset } => encode_command(&[
                REPLPING,
                replid.as_hex().as_bytes(),
                offset.0.to_string().as_bytes(),
            ]),
            Frame::FullSync { replid, end_offset } => encode_command(&[
                FULLSYNC,
                replid.as_hex().as_bytes(),
                end_offset.0.to_string().as_bytes(),
            ]),
            Frame::SyncKv {
                db,
                key,
                kvobj_bytes,
            } => encode_command(&[SYNCKV, db.to_string().as_bytes(), key, kvobj_bytes]),
            Frame::SyncEnd { end_offset } => {
                encode_command(&[SYNCEND, end_offset.0.to_string().as_bytes()])
            }
            Frame::StreamPut {
                offset,
                db,
                key,
                kvobj_bytes,
            } => encode_command(&[
                STREAMPUT,
                offset.0.to_string().as_bytes(),
                db.to_string().as_bytes(),
                key,
                kvobj_bytes,
            ]),
            Frame::StreamDel { offset, db, key } => encode_command(&[
                STREAMDEL,
                offset.0.to_string().as_bytes(),
                db.to_string().as_bytes(),
                key,
            ]),
            Frame::ImportReq { slot } => encode_command(&[IMPORTREQ, slot.to_string().as_bytes()]),
        }
    }

    /// Try to decode one frame from the front of `buf`.
    ///
    /// Returns `Ok(Some((frame, consumed)))` when a full, well-formed replication
    /// frame is present (with the byte length it occupied), `Ok(None)` when more
    /// bytes are needed, and `Err(`[`FrameError`]`)` for a framing error or a
    /// complete RESP command that is not a decodable replication frame (wrong verb /
    /// arity, a non-numeric field, or a malformed replid).
    pub fn decode(buf: &[u8]) -> Result<Option<(Frame, usize)>, FrameError> {
        let Some((args, consumed)) = parse_command_array(buf)? else {
            return Ok(None);
        };
        let frame = decode_args(&args)?;
        Ok(Some((frame, consumed)))
    }
}

/// Decode a fully-parsed RESP command's args into a [`Frame`], or [`FrameError`] if
/// the args are not a known replication frame.
fn decode_args(args: &[Vec<u8>]) -> Result<Frame, FrameError> {
    let verb = args.first().ok_or(FrameError)?;
    if verb.eq_ignore_ascii_case(REPLCONF) {
        if args.len() != 3 {
            return Err(FrameError);
        }
        let node = parse_u64(&args[1])?;
        let ack = ReplOffset(parse_u64(&args[2])?);
        Ok(Frame::ReplConf { node, ack })
    } else if verb.eq_ignore_ascii_case(REPLPING) {
        if args.len() != 3 {
            return Err(FrameError);
        }
        let replid = ReplId::from_hex(&args[1]).ok_or(FrameError)?;
        let offset = ReplOffset(parse_u64(&args[2])?);
        Ok(Frame::ReplPing { replid, offset })
    } else if verb.eq_ignore_ascii_case(FULLSYNC) {
        if args.len() != 3 {
            return Err(FrameError);
        }
        let replid = ReplId::from_hex(&args[1]).ok_or(FrameError)?;
        let end_offset = ReplOffset(parse_u64(&args[2])?);
        Ok(Frame::FullSync { replid, end_offset })
    } else if verb.eq_ignore_ascii_case(SYNCKV) {
        if args.len() != 4 {
            return Err(FrameError);
        }
        let db = u32::try_from(parse_u64(&args[1])?).map_err(|_| FrameError)?;
        // The key and the kvobj bytes are opaque binary (the RESP bulk length delimits
        // them, so embedded CRLF is safe); take them as-is.
        let key = args[2].clone();
        let kvobj_bytes = args[3].clone();
        Ok(Frame::SyncKv {
            db,
            key,
            kvobj_bytes,
        })
    } else if verb.eq_ignore_ascii_case(SYNCEND) {
        if args.len() != 2 {
            return Err(FrameError);
        }
        let end_offset = ReplOffset(parse_u64(&args[1])?);
        Ok(Frame::SyncEnd { end_offset })
    } else if verb.eq_ignore_ascii_case(STREAMPUT) {
        if args.len() != 5 {
            return Err(FrameError);
        }
        let offset = ReplOffset(parse_u64(&args[1])?);
        let db = u32::try_from(parse_u64(&args[2])?).map_err(|_| FrameError)?;
        // The key and kvobj bytes are opaque binary; the RESP bulk length delimits them.
        let key = args[3].clone();
        let kvobj_bytes = args[4].clone();
        Ok(Frame::StreamPut {
            offset,
            db,
            key,
            kvobj_bytes,
        })
    } else if verb.eq_ignore_ascii_case(STREAMDEL) {
        if args.len() != 4 {
            return Err(FrameError);
        }
        let offset = ReplOffset(parse_u64(&args[1])?);
        let db = u32::try_from(parse_u64(&args[2])?).map_err(|_| FrameError)?;
        let key = args[3].clone();
        Ok(Frame::StreamDel { offset, db, key })
    } else if verb.eq_ignore_ascii_case(IMPORTREQ) {
        if args.len() != 2 {
            return Err(FrameError);
        }
        let slot = u16::try_from(parse_u64(&args[1])?).map_err(|_| FrameError)?;
        Ok(Frame::ImportReq { slot })
    } else {
        Err(FrameError)
    }
}

/// Parse an ASCII unsigned `u64` argument, rejecting anything non-numeric.
fn parse_u64(arg: &[u8]) -> Result<u64, FrameError> {
    core::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(FrameError)
}

/// RESP-encode a command as an array of bulk strings (`*N\r\n$len\r\narg\r\n...`),
/// identical to the cluster-bus encoding so the shared inbound parser decodes it.
fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// A parsed RESP command: its bulk-string args plus the number of bytes it
/// occupied. Mirrors the inbound parser the Raft adapter carries with its consumer.
type ParsedCommand = (Vec<Vec<u8>>, usize);

/// Parse one RESP array-of-bulk-strings command from `buf`.
///
/// Returns the decoded args plus the bytes consumed, `Ok(None)` if the command is
/// not yet fully buffered, or `Err(`[`FrameError`]`)` on a malformed frame.
fn parse_command_array(buf: &[u8]) -> Result<Option<ParsedCommand>, FrameError> {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] != b'*' {
        return Err(FrameError);
    }
    let mut pos = 0usize;
    let Some((count, next)) = read_int_line(buf, pos)? else {
        return Ok(None);
    };
    pos = next;
    let count = usize::try_from(count).map_err(|_| FrameError)?;
    let mut args = Vec::with_capacity(count.min(16));
    for _ in 0..count {
        // Each arg is a bulk string: `$len\r\n<bytes>\r\n`.
        match buf.get(pos) {
            Some(b'$') => {}
            Some(_) => return Err(FrameError),
            None => return Ok(None),
        }
        let Some((len, next)) = read_int_line(buf, pos)? else {
            return Ok(None);
        };
        let len = usize::try_from(len).map_err(|_| FrameError)?;
        // FRAME BOUND (PROD-3, memory-DoS fix): reject a per-arg length over the cluster frame cap
        // BEFORE waiting for / allocating the body. The length is attacker-controlled on the
        // replication wire; without this an over-cap `$<huge>\r\n` header drives the replica/source
        // recv buffer to grow unboundedly trying to satisfy the claimed length, OOMing the node. A
        // real repl frame (one key's encoded value) is far under the 512 MiB cap. Enforced on the
        // (default) plaintext path too -- it is a parser-correctness fix, not a TLS feature.
        if len > ironcache_runtime::MAX_CLUSTER_FRAME_LEN {
            return Err(FrameError);
        }
        let body_start = next;
        let body_end = body_start.checked_add(len).ok_or(FrameError)?;
        let crlf_end = body_end.checked_add(2).ok_or(FrameError)?;
        if buf.len() < crlf_end {
            return Ok(None);
        }
        if &buf[body_end..crlf_end] != b"\r\n" {
            return Err(FrameError);
        }
        args.push(buf[body_start..body_end].to_vec());
        pos = crlf_end;
    }
    Ok(Some((args, pos)))
}

/// Read a `<prefix-char><int>\r\n` header line starting at `start` (the prefix char
/// is already validated by the caller), returning the parsed integer and the index
/// just past the `\r\n`, or `Ok(None)` if the line is not yet complete.
fn read_int_line(buf: &[u8], start: usize) -> Result<Option<(i64, usize)>, FrameError> {
    let rest = &buf[start + 1..];
    let Some(rel) = rest.windows(2).position(|w| w == b"\r\n") else {
        return Ok(None);
    };
    let line = &rest[..rel];
    let n: i64 = core::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(FrameError)?;
    // Absolute index just past the CRLF: start + 1 (prefix) + rel + 2 (CRLF).
    Ok(Some((n, start + 1 + rel + 2)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PROD-3 FRAME BOUND: a replication frame whose per-arg length header claims MORE than
    /// `MAX_CLUSTER_FRAME_LEN` is REJECTED (`Err(FrameError)`) at decode time, BEFORE the body is
    /// awaited / allocated, so a forged huge length on the replication wire cannot drive an
    /// unbounded recv-buffer growth (memory DoS). A length AT the cap is still accepted (the parser
    /// asks for more bytes), so the bound never rejects a legitimate large value.
    #[test]
    fn frame_over_the_length_cap_is_rejected() {
        let over = ironcache_runtime::MAX_CLUSTER_FRAME_LEN + 1;
        // A SYNCKV-shaped array header with a kvobj arg claiming an over-cap body length, header
        // only (no body). Without the cap this is "need more bytes"; with it, an immediate Err.
        let mut frame = Vec::new();
        frame.extend_from_slice(b"*4\r\n");
        frame.extend_from_slice(b"$6\r\nSYNCKV\r\n");
        frame.extend_from_slice(b"$1\r\n0\r\n");
        frame.extend_from_slice(b"$3\r\nfoo\r\n");
        frame.extend_from_slice(format!("${over}\r\n").as_bytes());
        assert!(
            Frame::decode(&frame).is_err(),
            "an over-cap per-arg length must be rejected, not awaited (memory DoS)"
        );

        // A length AT the cap (no body yet) is legitimate -> the parser asks for more bytes (Ok(None)).
        let at = ironcache_runtime::MAX_CLUSTER_FRAME_LEN;
        let mut ok_frame = Vec::new();
        ok_frame.extend_from_slice(b"*4\r\n");
        ok_frame.extend_from_slice(b"$6\r\nSYNCKV\r\n");
        ok_frame.extend_from_slice(b"$1\r\n0\r\n");
        ok_frame.extend_from_slice(b"$3\r\nfoo\r\n");
        ok_frame.extend_from_slice(format!("${at}\r\n").as_bytes());
        assert!(
            matches!(Frame::decode(&ok_frame), Ok(None)),
            "a length AT the cap is legitimate; the parser must ask for more bytes, not reject"
        );
    }

    /// A single round-trip assertion: encode then decode must reproduce the input
    /// byte-for-byte AND consume exactly the encoded length.
    fn assert_round_trips(frame: &Frame) {
        let bytes = frame.encode();
        let (decoded, consumed) = Frame::decode(&bytes)
            .expect("well-formed frame decodes")
            .expect("a complete frame is present");
        assert_eq!(&decoded, frame, "round-trip mismatch for {frame:?}");
        assert_eq!(consumed, bytes.len(), "must consume the whole frame");
    }

    /// THE codec gate: every frame survives encode -> decode unchanged, including
    /// the edge offsets (0 and u64::MAX) and the node-id / replid extremes.
    #[test]
    fn frame_round_trips() {
        // REPLCONF at a typical attach.
        assert_round_trips(&Frame::ReplConf {
            node: 7,
            ack: ReplOffset(42),
        });
        // REPLCONF at the zero edge (fresh replica, never acked) and the max edge.
        assert_round_trips(&Frame::ReplConf {
            node: 0,
            ack: ReplOffset(0),
        });
        assert_round_trips(&Frame::ReplConf {
            node: u64::MAX,
            ack: ReplOffset(u64::MAX),
        });

        // REPLPING with a real-shaped replid and edge offsets.
        assert_round_trips(&Frame::ReplPing {
            replid: ReplId::from_hex(b"1111111111111111111111111111111111111111").unwrap(),
            offset: ReplOffset(0),
        });
        assert_round_trips(&Frame::ReplPing {
            replid: ReplId::from_hex(b"abcdef0123456789abcdef0123456789abcdef01").unwrap(),
            offset: ReplOffset(u64::MAX),
        });

        // FULLSYNC (HA-7b): the begin-full-sync frame, at the offset edges.
        assert_round_trips(&Frame::FullSync {
            replid: ReplId::from_hex(b"abcdef0123456789abcdef0123456789abcdef01").unwrap(),
            end_offset: ReplOffset(0),
        });
        assert_round_trips(&Frame::FullSync {
            replid: ReplId::from_hex(b"2222222222222222222222222222222222222222").unwrap(),
            end_offset: ReplOffset(u64::MAX),
        });

        // SYNCKV (HA-7b): a snapshot entry. The key and kvobj bytes are OPAQUE BINARY,
        // including embedded CRLF and a NUL, which the length-delimited bulk codec carries
        // verbatim. Also the empty-key / empty-payload edge.
        assert_round_trips(&Frame::SyncKv {
            db: 0,
            key: b"plain-key".to_vec(),
            kvobj_bytes: b"some-encoded-kvobj-bytes".to_vec(),
        });
        assert_round_trips(&Frame::SyncKv {
            db: u32::MAX,
            key: b"binary\r\nkey\x00".to_vec(),
            kvobj_bytes: vec![0u8, 1, 2, b'\r', b'\n', 255, 13, 10],
        });
        assert_round_trips(&Frame::SyncKv {
            db: 3,
            key: Vec::new(),
            kvobj_bytes: Vec::new(),
        });

        // SYNCEND (HA-7b): the terminator, at the offset edges.
        assert_round_trips(&Frame::SyncEnd {
            end_offset: ReplOffset(0),
        });
        assert_round_trips(&Frame::SyncEnd {
            end_offset: ReplOffset(123_456),
        });
        assert_round_trips(&Frame::SyncEnd {
            end_offset: ReplOffset(u64::MAX),
        });

        // STREAMPUT (HA-7c): a steady-state put. The key and kvobj bytes are OPAQUE BINARY
        // (embedded CRLF + NUL) the length-delimited bulk codec carries verbatim; the offset
        // and db are at their edges, plus the empty-key / empty-payload edge.
        assert_round_trips(&Frame::StreamPut {
            offset: ReplOffset(1),
            db: 0,
            key: b"plain-key".to_vec(),
            kvobj_bytes: b"some-encoded-kvobj-bytes".to_vec(),
        });
        assert_round_trips(&Frame::StreamPut {
            offset: ReplOffset(u64::MAX),
            db: u32::MAX,
            key: b"binary\r\nkey\x00".to_vec(),
            kvobj_bytes: vec![0u8, 1, 2, b'\r', b'\n', 255, 13, 10],
        });
        assert_round_trips(&Frame::StreamPut {
            offset: ReplOffset(0),
            db: 7,
            key: Vec::new(),
            kvobj_bytes: Vec::new(),
        });

        // STREAMDEL (HA-7c): a steady-state delete, at the offset/db edges and a binary key.
        assert_round_trips(&Frame::StreamDel {
            offset: ReplOffset(0),
            db: 0,
            key: b"k".to_vec(),
        });
        assert_round_trips(&Frame::StreamDel {
            offset: ReplOffset(u64::MAX),
            db: u32::MAX,
            key: b"binary\r\nkey\x00".to_vec(),
        });
        assert_round_trips(&Frame::StreamDel {
            offset: ReplOffset(99),
            db: 3,
            key: Vec::new(),
        });

        // IMPORTREQ (HA-6 data-copy): the scoped-attach request, at the slot edges (0 and the
        // last valid slot 16383).
        assert_round_trips(&Frame::ImportReq { slot: 0 });
        assert_round_trips(&Frame::ImportReq { slot: 16_383 });
    }

    /// Decode rejects malformed input rather than panicking or fabricating a frame:
    /// an unknown verb, a wrong arity, a non-numeric offset, and a bad replid all
    /// yield `Err(())`; a truncated frame asks for more bytes.
    #[test]
    fn decode_rejects_malformed() {
        // An unknown verb (a complete, well-formed RESP command, just not ours).
        assert_eq!(Frame::decode(b"*1\r\n$4\r\nPING\r\n"), Err(FrameError));
        // REPLCONF with the wrong arity (two args).
        assert_eq!(
            Frame::decode(b"*2\r\n$8\r\nREPLCONF\r\n$1\r\n7\r\n"),
            Err(FrameError)
        );
        // REPLPING with a non-numeric offset.
        let bad = b"*3\r\n$8\r\nREPLPING\r\n$40\r\n1111111111111111111111111111111111111111\r\n$3\r\nfoo\r\n";
        assert_eq!(Frame::decode(bad), Err(FrameError));
        // REPLPING with a too-short replid.
        let short = b"*3\r\n$8\r\nREPLPING\r\n$3\r\nabc\r\n$1\r\n0\r\n";
        assert_eq!(Frame::decode(short), Err(FrameError));
        // A truncated REPLCONF (header present, body not yet arrived) needs more.
        let full = Frame::ReplConf {
            node: 1,
            ack: ReplOffset(2),
        }
        .encode();
        assert_eq!(Frame::decode(&full[..full.len() - 2]), Ok(None));
        // An empty buffer needs more bytes.
        assert_eq!(Frame::decode(b""), Ok(None));
    }

    /// A buffer holding two back-to-back frames decodes them one at a time, the
    /// `consumed` count letting the caller advance past each (the recv-loop shape).
    #[test]
    fn decode_advances_over_pipelined_frames() {
        let f1 = Frame::ReplConf {
            node: 3,
            ack: ReplOffset(10),
        };
        let f2 = Frame::ReplPing {
            replid: ReplId::from_hex(b"2222222222222222222222222222222222222222").unwrap(),
            offset: ReplOffset(11),
        };
        let mut buf = f1.encode();
        buf.extend_from_slice(&f2.encode());

        let (g1, c1) = Frame::decode(&buf).unwrap().unwrap();
        assert_eq!(g1, f1);
        let (g2, c2) = Frame::decode(&buf[c1..]).unwrap().unwrap();
        assert_eq!(g2, f2);
        assert_eq!(c1 + c2, buf.len());
    }
}
