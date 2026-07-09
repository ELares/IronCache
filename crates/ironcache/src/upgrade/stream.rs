// SPDX-License-Identifier: MIT OR Apache-2.0
//! Streamed state HANDOFF over a unix socket (#391 Phase 2c): the near-zero-downtime single-node
//! upgrade path that streams the live keyspace from the OLD process straight into a sibling NEW
//! process, so the new one can serve BEFORE the old drains, with no disk round-trip.
//!
//! This module lands the DATA-SAFE CORE of #391: the framed wire protocol, the per-shard bulk
//! transfer, the buffered-delta ship/apply, the cross-shard atomic cutover barrier, and the
//! abort-safety that keeps the OLD process serving on ANY failure. The live-serve WIRING (the
//! orchestrator that spawns the sibling, the coordinator that pulls over the socket instead of
//! loading from disk, the dispatch `-LOADING` quiesce, and the acceptor drain-and-final-accept) is
//! DEFERRED to the follow-up so it can carry its own data-loss-focused review (see "Deferred").
//!
//! ## Why this reuses the replication machinery
//!
//! A localized old->new full transfer is EXACTLY a Redis-style full-sync + tail, which
//! `ironcache-repl` already implements and proves converges (HA-7b/7c, `fullsync` + `stream` +
//! `observer` + `kvcodec`). Rather than re-derive a second snapshot/delta protocol, this module
//! ORCHESTRATES those reviewed pieces over a unix socket:
//!
//! - BULK: [`ironcache_store::ShardStore::snapshot_chunk`] pulled in bounded chunks and shipped as
//!   [`Frame::SyncKv`] (the constant-memory borrow discipline the repl crate uses: pull a chunk
//!   under the store borrow, DROP the borrow, then await the sends). The receiver replays each via
//!   [`ironcache_store::ShardStore::insert_object`], which round-trips type + encoding + TTL +
//!   collections.
//! - DELTA: writes on the old process DURING the transfer are captured by the HA-5a
//!   [`ironcache_repl::ReplObserver`] into a shard-local [`ironcache_repl::ReplRing`] as
//!   [`ironcache_repl::StreamOp`]s (offset-ordered), then shipped as [`Frame::StreamPut`] /
//!   `StreamDel` and applied by [`ironcache_repl::ReplicaApplier`] in offset order, idempotently.
//!   Snapshot + tail converges last-write-wins (the repl convergence gate proves it).
//!
//! ## The wire envelope (CRC'd + versioned + length-prefixed, fail-closed, like #530)
//!
//! The repl [`Frame`] codec is length-prefixed and fail-closed but carries NO integrity CRC and NO
//! protocol version. The handoff wraps every message in an outer ENVELOPE that adds both, in the
//! same spirit as the on-disk snapshot format (#530): a fixed 16-byte header
//! (`MAGIC "ICHO" | version u16 | kind u8 | reserved u8 | payload_len u32 | crc32 u32`) then the
//! payload. The CRC (the reused hand-rolled [`ironcache_persist::crc32`]) covers the WHOLE message
//! with the CRC field zeroed, so a torn header OR a torn payload is caught. Decode is TOTAL and
//! FAIL-CLOSED: a wrong magic, an unknown version, an over-cap length, a CRC mismatch, a truncated
//! read, or an undecodable inner frame each ABORT the handoff (never a silent mis-parse) -- the
//! same posture #530 took for the on-disk format.
//!
//! ## Abort-safety (the #1 rule: never lose data, never serve stale)
//!
//! The durable `data_dir` snapshot is NEVER touched by this path, so it always remains a valid
//! fallback. Beyond that:
//!
//! - The SENDER only READS its live [`ironcache_store::ShardStore`] (via `snapshot_chunk`) and its
//!   ring; it never mutates or drops the store. So on ANY sender error the OLD process's data is
//!   fully intact and it keeps serving. The old process must NOT stop serving until it has an
//!   acked cutover for EVERY shard (the [`CutoverBarrier`] `Commit`), so there is never a moment
//!   where neither side serves.
//! - The RECEIVER builds a FRESH store and RETURNS it only on the complete, CRC-verified,
//!   cutover-acked path. On ANY error (bad magic/version/CRC, over-cap, truncation/EOF, an
//!   undecodable frame, a delta gap, a final-offset mismatch, an explicit ABORT, a delta
//!   overflow) the fresh store is DROPPED (adopt nothing) and the new sibling exits without ever
//!   serving. A half-loaded store is never adopted.
//! - The client-visible flip is ALL-OR-NOTHING across shards ([`CutoverBarrier`]): the sibling
//!   adopts (serves) only when EVERY shard committed; a single shard's failure aborts the whole
//!   flip. There is never a window where some shards are new and some are old for a client, so no
//!   key is ever served by NEITHER or BOTH processes.
//!
//! ## The atomic cutover across shards (the one place shards synchronize, ADR-0002)
//!
//! Shared-nothing means per-shard streams (one [`send_shard`]/[`recv_shard`] pair per core). The
//! ONE cross-shard synchronization is the cutover flip, coordinated by the pure [`CutoverBarrier`]:
//! the sibling gathers every shard's cutover result and flips to serving iff ALL committed. In the
//! live wiring (deferred), the old process QUIESCES writes across all shards (returning `-LOADING`
//! briefly) before the final per-shard delta drain, so the snapshot the barrier commits is a
//! consistent post-quiesce cut with no acknowledged write outside it.
//!
//! ## SO_REUSEPORT / no orphaned backlog
//!
//! Two processes binding the same port via SO_REUSEPORT is already supported
//! (`ironcache_runtime::tokio_rt::bind_reuseport_std`). The caveat #391 flags is that connections
//! sitting in a DYING listener's accept queue are RST when that listener closes. The SUPPORTED
//! no-RST path is systemd SOCKET-ACTIVATION (#389): systemd owns the listening socket and passes
//! its fd to the new process (`LISTEN_FDS`), so the listener is NEVER closed across the handoff and
//! no backlog is orphaned. The non-socket-activated SO_REUSEPORT case needs a drain-and-final-accept
//! (hand the old listener's already-accepted connections through before close); that acceptor
//! integration is part of the deferred live wiring. This module is transport-agnostic over the
//! handoff socket and does not itself touch the client listener.
//!
//! ## Determinism (ADR-0003)
//!
//! This is an ops-path module OFF the engine decision path: it reads no clock and no RNG (the
//! `now` used for lazy-expiry on the snapshot/delta is the CALLER's clock, sourced from the
//! `ironcache-env` seam, exactly as `snapshot_chunk` and the applier already require). The socket
//! I/O and the `ReplId` are supplied by the caller.
//!
//! ## Deferred to the follow-up (Part of #391)
//!
//! - The ORCHESTRATOR spawning the sibling process and passing it the socket path.
//! - The COORDINATOR boot choosing "pull over the handoff socket" instead of load-from-disk, and
//!   installing the observer/ring on each old shard at handoff start.
//! - The dispatch-layer `-LOADING` write-QUIESCE across shards for the final delta cut, and the
//!   atomic serve-flip on the sibling.
//! - The acceptor DRAIN-AND-FINAL-ACCEPT for the non-socket-activated SO_REUSEPORT case.
//!
//! Each of those touches the live datapath / process lifecycle and is held for a dedicated review.

use std::cell::RefCell;
use std::rc::Rc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use ironcache_persist::crc32;
use ironcache_repl::{
    ApplyOutcome, Frame, ReplId, ReplOffset, ReplRing, ReplicaApplier, StreamOp, decode_kvobj,
    encode_kvobj,
};
use ironcache_storage::{AccountingHook, EvictionHook, UnixMillis};
use ironcache_store::{ShardStore, SnapshotCursor};

/// The magic at the head of EVERY handoff envelope: ASCII `ICHO` (IronCache HandOff), so a stray
/// or foreign byte stream is rejected before any decode (mirrors the on-disk `ICSS` magic).
const MAGIC: [u8; 4] = *b"ICHO";

/// The handoff wire protocol version. Bumped on any breaking envelope/message layout change; the
/// decoder REJECTS an unknown version (fail-closed) rather than mis-parsing it, so an old<->new
/// binary pair that disagrees on the wire aborts the stream and falls back to the durable path
/// instead of corrupting the transfer (the #530 posture).
const PROTO_VERSION: u16 = 1;

/// The fixed envelope header length: `magic[4] + version[2] + kind[1] + reserved[1] + len[4] +
/// crc[4]`.
const HEADER_LEN: usize = 16;

/// The maximum envelope payload accepted, a fail-closed memory-DoS bound (the inner repl frame is
/// itself bounded by [`ironcache_runtime::MAX_CLUSTER_FRAME_LEN`]; the outer cap adds slack for the
/// RESP framing + the tiny handshake payloads). A length header claiming more than this is rejected
/// BEFORE the body is read/allocated.
const MAX_PAYLOAD: usize = ironcache_runtime::MAX_CLUSTER_FRAME_LEN + 64 * 1024;

// The message KIND discriminators carried in the envelope header.
const K_HELLO: u8 = 1;
const K_HELLO_ACK: u8 = 2;
const K_FRAME: u8 = 3;
const K_CUTOVER: u8 = 4;
const K_CUTOVER_ACK: u8 = 5;
const K_ABORT: u8 = 6;

/// A typed handoff failure (ERRORS.md: no stringly-typed errors). EVERY variant is an ABORT: the
/// receiver drops its partial store and the sender keeps the old process serving. The variants pin
/// exactly which check failed so the operator log is unambiguous.
#[derive(Debug, thiserror::Error)]
pub enum HandoffError {
    /// A socket read/write failed or the peer disconnected MID-TRANSFER (an `UnexpectedEof` from a
    /// truncated frame): the stream is incomplete, so abort.
    #[error("handoff socket I/O failed (peer gone / truncated mid-transfer): {0}")]
    Io(String),
    /// An envelope did not start with the [`MAGIC`] tag: a foreign / corrupt stream.
    #[error("handoff envelope magic mismatch (foreign or corrupt stream)")]
    BadMagic,
    /// The envelope's protocol version is not the one this binary speaks. Fail-closed: an old<->new
    /// mismatch aborts rather than guessing at a layout it does not understand.
    #[error("handoff protocol version {found} unsupported (this binary speaks {supported})")]
    UnsupportedVersion {
        /// The version the peer sent.
        found: u16,
        /// The version this binary speaks ([`PROTO_VERSION`]).
        supported: u16,
    },
    /// An envelope's declared payload length exceeds [`MAX_PAYLOAD`] (a forged huge length; a
    /// memory-DoS guard). Rejected before the body is read/allocated.
    #[error("handoff envelope payload length {len} exceeds the {max}-byte cap")]
    Oversize {
        /// The declared length.
        len: usize,
        /// The cap ([`MAX_PAYLOAD`]).
        max: usize,
    },
    /// The envelope's CRC-32 did not match the recomputed CRC over the message (a torn header or
    /// payload). Fail-closed: abort rather than feed corrupt bytes to the decoder.
    #[error("handoff envelope CRC mismatch (torn frame)")]
    Crc,
    /// A well-framed, CRC-valid envelope carried a payload this binary could not decode into a
    /// known message (a bad kind, a short/garbled handshake payload, or an inner repl frame that
    /// did not decode). Fail-closed.
    #[error("handoff message payload malformed (undecodable)")]
    Malformed,
    /// A message arrived out of the expected protocol sequence (e.g. a data frame before the
    /// handshake, or a cutover before the bulk stream ended).
    #[error("handoff protocol sequence error: expected {expected}, got {got}")]
    Unexpected {
        /// What the state machine expected next.
        expected: &'static str,
        /// What actually arrived.
        got: &'static str,
    },
    /// The sender and receiver disagree on the database count, so `insert_object` routing would be
    /// wrong. Fail-closed (a configuration mismatch, not a recoverable stream error).
    #[error("handoff database-count mismatch: receiver has {expected}, sender sent {found}")]
    DbMismatch {
        /// The receiver's configured database count.
        expected: u32,
        /// The count the sender advertised.
        found: u32,
    },
    /// The old process's delta ring OVERFLOWED during the transfer (more writes than the bounded
    /// buffer could retain), so the tail has a gap the receiver cannot fill. Abort (the operator
    /// retries, or falls back to the durable-snapshot reload of #390).
    #[error("handoff delta overflowed the bounded ring (too many writes during transfer)")]
    DeltaOverflow,
    /// The receiver saw an out-of-order / undecodable delta frame (a gap in the offset sequence).
    /// Abort rather than apply out of order.
    #[error("handoff delta gap (out-of-order or corrupt tail frame)")]
    DeltaGap,
    /// At cutover the receiver's applied offset did not equal the sender's final offset: the tail
    /// is incomplete. Abort.
    #[error("handoff cutover offset mismatch: sender final {expected}, receiver applied {got}")]
    OffsetMismatch {
        /// The sender's final offset (the cut).
        expected: u64,
        /// The offset the receiver had actually applied.
        got: u64,
    },
    /// The peer sent an explicit ABORT message (it hit a failure on its side). Tear down.
    #[error("handoff aborted by the peer")]
    Aborted,
    /// The handshake was rejected (the receiver did not ack the HELLO, or acked the wrong message).
    #[error("handoff handshake rejected by the peer")]
    HelloRejected,
}

impl HandoffError {
    /// Map a socket I/O error to [`HandoffError::Io`] (a mid-transfer disconnect surfaces as an
    /// `UnexpectedEof` here, which is an abort like any other I/O failure).
    fn io(e: &std::io::Error) -> Self {
        HandoffError::Io(e.to_string())
    }
}

/// A decoded handoff message. The data-bearing `Frame` variants reuse the reviewed repl codec; the
/// handshake/control variants are the handoff's own tiny fixed payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HandoffMsg {
    /// Receiver-directed handshake: which shard this stream carries + the sender's database count.
    Hello {
        /// The shard index this stream transfers (0-based; one stream per shard).
        shard: u32,
        /// The sender's database count (must match the receiver's for `insert_object` routing).
        databases: u32,
    },
    /// The receiver accepted the handshake and is ready to receive the bulk stream.
    HelloAck,
    /// A data-plane repl frame (FULLSYNC / SYNCKV / SYNCEND for bulk, STREAMPUT / STREAMDEL for the
    /// delta tail). The payload is the reused [`Frame::encode`] blob.
    Frame(Frame),
    /// The sender has shipped the whole bulk + the buffered delta; `final_offset` is the cut the
    /// receiver must have applied through before it acks.
    Cutover {
        /// The final logical offset the tail reached (the receiver verifies it applied exactly
        /// through here).
        final_offset: ReplOffset,
    },
    /// The receiver has loaded the bulk + applied the delta through `final_offset` and is ready to
    /// adopt the store: the shard's half of the atomic flip is safe.
    CutoverAck,
    /// Either side hit a failure and is tearing down; the peer must abort too.
    Abort,
}

impl HandoffMsg {
    /// The `(kind, payload)` this message serializes to (the payload is the envelope body).
    fn to_payload(&self) -> (u8, Vec<u8>) {
        match self {
            HandoffMsg::Hello { shard, databases } => {
                let mut p = Vec::with_capacity(8);
                p.extend_from_slice(&shard.to_le_bytes());
                p.extend_from_slice(&databases.to_le_bytes());
                (K_HELLO, p)
            }
            HandoffMsg::HelloAck => (K_HELLO_ACK, Vec::new()),
            HandoffMsg::Frame(f) => (K_FRAME, f.encode()),
            HandoffMsg::Cutover { final_offset } => {
                (K_CUTOVER, final_offset.0.to_le_bytes().to_vec())
            }
            HandoffMsg::CutoverAck => (K_CUTOVER_ACK, Vec::new()),
            HandoffMsg::Abort => (K_ABORT, Vec::new()),
        }
    }

    /// Decode a `(kind, payload)` back into a message, or [`HandoffError::Malformed`] if the kind
    /// is unknown or the payload does not match the kind's fixed shape / a valid inner frame.
    fn from_payload(kind: u8, payload: &[u8]) -> Result<HandoffMsg, HandoffError> {
        match kind {
            K_HELLO => {
                if payload.len() != 8 {
                    return Err(HandoffError::Malformed);
                }
                let shard = u32::from_le_bytes(payload[0..4].try_into().unwrap_or([0; 4]));
                let databases = u32::from_le_bytes(payload[4..8].try_into().unwrap_or([0; 4]));
                Ok(HandoffMsg::Hello { shard, databases })
            }
            K_HELLO_ACK => {
                if !payload.is_empty() {
                    return Err(HandoffError::Malformed);
                }
                Ok(HandoffMsg::HelloAck)
            }
            K_FRAME => {
                // The inner repl frame MUST decode AND consume the whole payload (no trailing slop),
                // else the envelope is malformed.
                match Frame::decode(payload) {
                    Ok(Some((frame, consumed))) if consumed == payload.len() => {
                        Ok(HandoffMsg::Frame(frame))
                    }
                    _ => Err(HandoffError::Malformed),
                }
            }
            K_CUTOVER => {
                if payload.len() != 8 {
                    return Err(HandoffError::Malformed);
                }
                let off = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                Ok(HandoffMsg::Cutover {
                    final_offset: ReplOffset(off),
                })
            }
            K_CUTOVER_ACK => {
                if !payload.is_empty() {
                    return Err(HandoffError::Malformed);
                }
                Ok(HandoffMsg::CutoverAck)
            }
            K_ABORT => Ok(HandoffMsg::Abort),
            _ => Err(HandoffError::Malformed),
        }
    }

    /// A short label for sequence-error diagnostics.
    fn label(&self) -> &'static str {
        match self {
            HandoffMsg::Hello { .. } => "HELLO",
            HandoffMsg::HelloAck => "HELLO_ACK",
            HandoffMsg::Frame(_) => "FRAME",
            HandoffMsg::Cutover { .. } => "CUTOVER",
            HandoffMsg::CutoverAck => "CUTOVER_ACK",
            HandoffMsg::Abort => "ABORT",
        }
    }
}

/// Encode a message into its on-wire envelope bytes: `MAGIC | version | kind | reserved | len |
/// crc | payload`, where the CRC covers the WHOLE message with the CRC field zeroed (so a torn
/// header or payload is both caught). One pass, no payload copy.
fn encode_envelope(msg: &HandoffMsg) -> Vec<u8> {
    let (kind, payload) = msg.to_payload();
    // The payload length always fits: handshake/control payloads are tiny and a FRAME payload is
    // an already-bounded repl frame (`<= MAX_CLUSTER_FRAME_LEN` + small RESP overhead, under the
    // u32 ceiling). Clamp defensively so the cast is total.
    let len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&MAGIC); // [0..4]
    out.extend_from_slice(&PROTO_VERSION.to_le_bytes()); // [4..6]
    out.push(kind); // [6]
    out.push(0u8); // [7] reserved
    out.extend_from_slice(&len.to_le_bytes()); // [8..12]
    out.extend_from_slice(&[0u8; 4]); // [12..16] CRC placeholder (zero while computing)
    out.extend_from_slice(&payload); // [16..]
    let crc = crc32(&out);
    out[12..16].copy_from_slice(&crc.to_le_bytes());
    out
}

/// Decode + fail-closed-validate one envelope from a contiguous `buf` of exactly `HEADER_LEN + len`
/// bytes (the CRC field is zeroed in place to recompute). Returns the decoded message.
fn decode_envelope(buf: &mut [u8]) -> Result<HandoffMsg, HandoffError> {
    debug_assert!(buf.len() >= HEADER_LEN);
    if buf[0..4] != MAGIC {
        return Err(HandoffError::BadMagic);
    }
    let version = u16::from_le_bytes([buf[4], buf[5]]);
    if version != PROTO_VERSION {
        return Err(HandoffError::UnsupportedVersion {
            found: version,
            supported: PROTO_VERSION,
        });
    }
    let kind = buf[6];
    let saved_crc = u32::from_le_bytes(buf[12..16].try_into().unwrap_or([0; 4]));
    // Recompute over the message with the CRC field zeroed (the same image the encoder CRC'd).
    buf[12..16].copy_from_slice(&[0u8; 4]);
    if crc32(buf) != saved_crc {
        return Err(HandoffError::Crc);
    }
    HandoffMsg::from_payload(kind, &buf[HEADER_LEN..])
}

/// Write one message to the socket as a framed envelope.
async fn write_msg<W: AsyncWrite + Unpin>(w: &mut W, msg: &HandoffMsg) -> Result<(), HandoffError> {
    let bytes = encode_envelope(msg);
    w.write_all(&bytes)
        .await
        .map_err(|e| HandoffError::io(&e))?;
    w.flush().await.map_err(|e| HandoffError::io(&e))?;
    Ok(())
}

/// Read one framed envelope from the socket, fail-closed-validating magic / version / length-cap /
/// CRC before decoding. A truncated read (peer gone mid-transfer) surfaces as [`HandoffError::Io`].
async fn read_msg<R: AsyncRead + Unpin>(r: &mut R) -> Result<HandoffMsg, HandoffError> {
    // The whole message is read into ONE contiguous buffer (header then payload appended), so the
    // CRC-zero recompute in `decode_envelope` needs no extra allocation / no payload copy.
    let mut buf = vec![0u8; HEADER_LEN];
    r.read_exact(&mut buf)
        .await
        .map_err(|e| HandoffError::io(&e))?;
    // Validate magic + version + the length cap from the header BEFORE reading/allocating the body.
    if buf[0..4] != MAGIC {
        return Err(HandoffError::BadMagic);
    }
    let version = u16::from_le_bytes([buf[4], buf[5]]);
    if version != PROTO_VERSION {
        return Err(HandoffError::UnsupportedVersion {
            found: version,
            supported: PROTO_VERSION,
        });
    }
    let len = u32::from_le_bytes(buf[8..12].try_into().unwrap_or([0; 4])) as usize;
    if len > MAX_PAYLOAD {
        return Err(HandoffError::Oversize {
            len,
            max: MAX_PAYLOAD,
        });
    }
    buf.resize(HEADER_LEN + len, 0);
    r.read_exact(&mut buf[HEADER_LEN..])
        .await
        .map_err(|e| HandoffError::io(&e))?;
    decode_envelope(&mut buf)
}

/// Best-effort tell the peer we are aborting (so it tears down promptly), then return `err`. Any
/// failure sending the ABORT is ignored (the peer will also see the dropped socket).
async fn abort_with<W: AsyncWrite + Unpin>(w: &mut W, err: HandoffError) -> HandoffError {
    let _ = write_msg(w, &HandoffMsg::Abort).await;
    err
}

// ---------------------------------------------------------------------------------------------
// SENDER (the OLD process): read-only over its live store; never mutates or drops it.
// ---------------------------------------------------------------------------------------------

/// SENDER, phase 1 (BULK): handshake, then stream the shard's whole current keyspace old->new.
///
/// Sends [`HandoffMsg::Hello`], awaits the receiver's [`HandoffMsg::HelloAck`], captures the delta
/// CUT (`end_offset` = the ring's current head BEFORE the scan, so any write during the scan lands
/// in the delta and wins last-write-wins), then drives [`ironcache_store::ShardStore::snapshot_chunk`]
/// from [`SnapshotCursor::START`] to done in bounded chunks -- each `(db, key, KvObj)` shipped as a
/// [`Frame::SyncKv`] -- and closes with [`Frame::SyncEnd`]. Returns `end_offset` (the cut), which
/// the caller passes to [`send_cutover`] after quiescing writes.
///
/// CONSTANT MEMORY: each chunk is pulled under the store borrow, the borrow is RELEASED, and only
/// then are the frames awaited out -- no store borrow is ever held across an `.await`, peak memory
/// is one chunk.
///
/// # Errors
/// Any [`HandoffError`] on a socket failure, a rejected handshake, or a peer abort. The old store
/// is untouched, so the caller keeps the OLD process serving.
pub async fn send_bulk<E, A, S>(
    stream: &mut S,
    store: &ShardStore<E, A>,
    ring: &Rc<RefCell<ReplRing>>,
    shard: u32,
    replid: ReplId,
    now: UnixMillis,
    chunk_max: usize,
) -> Result<ReplOffset, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let databases = u32::try_from(store.databases()).unwrap_or(u32::MAX);
    write_msg(stream, &HandoffMsg::Hello { shard, databases }).await?;
    match read_msg(stream).await? {
        HandoffMsg::HelloAck => {}
        HandoffMsg::Abort => return Err(HandoffError::Aborted),
        _ => return Err(HandoffError::HelloRejected),
    }

    // Capture the cut BEFORE the scan: writes during the scan get offset > end_offset and are
    // carried by the delta (last-write-wins), so nothing acked is lost between the snapshot and the
    // tail.
    let end_offset = ring.borrow().head();
    write_msg(
        stream,
        &HandoffMsg::Frame(Frame::FullSync { replid, end_offset }),
    )
    .await?;

    let dbs = store.databases();
    let mut cursor = SnapshotCursor::START;
    while !cursor.is_done(dbs) {
        // --- Borrow the store, pull ONE bounded chunk, encode it, RELEASE the borrow. ---
        let frames: Vec<Frame> = {
            let (chunk, next) = store.snapshot_chunk(cursor, chunk_max, now);
            cursor = next;
            chunk
                .into_iter()
                .map(|(db, key, kv)| Frame::SyncKv {
                    db,
                    key: key.into_vec(),
                    kvobj_bytes: encode_kvobj(&kv),
                })
                .collect()
        }; // the store borrow ends here, before any await below.
        for frame in frames {
            if let Err(e) = write_msg(stream, &HandoffMsg::Frame(frame)).await {
                return Err(abort_with(stream, e).await);
            }
        }
    }

    write_msg(stream, &HandoffMsg::Frame(Frame::SyncEnd { end_offset })).await?;
    Ok(end_offset)
}

/// The next action the delta drain should take, DECIDED under the ring borrow so the borrow can be
/// released before any `.await` (keeps a `Ref` from ever crossing an await point).
enum DeltaStep {
    /// The tail is fully drained: send the cutover.
    Done,
    /// The ring overflowed: abort ([`HandoffError::DeltaOverflow`]).
    Overflow,
    /// A bounded batch of ops to ship (the borrow is already dropped by the time this is used).
    Batch(Vec<StreamOp>),
}

/// SENDER, phase 2 (DELTA + CUTOVER): drain the buffered delta, then the atomic flip handshake.
///
/// The CALLER must have QUIESCED writes (returned `-LOADING`) across all shards before calling this,
/// so the ring is a stable, final tail. This drains every op with offset > `end_offset` (shipped as
/// [`Frame::StreamPut`] / `StreamDel`), sends [`HandoffMsg::Cutover`] with the final offset, and
/// awaits [`HandoffMsg::CutoverAck`]. On the ack, this shard is safe to flip; the caller records it
/// with the [`CutoverBarrier`] and stops the OLD shard only when EVERY shard has committed.
///
/// A ring OVERFLOW (more writes than the bounded buffer retained) is a [`HandoffError::DeltaOverflow`]
/// abort -- the tail has a gap, so the operator retries or falls back to the durable path.
///
/// # Errors
/// Any [`HandoffError`] on a socket failure, a delta overflow, or a peer abort. The old store is
/// untouched.
pub async fn send_cutover<S>(
    stream: &mut S,
    ring: &Rc<RefCell<ReplRing>>,
    end_offset: ReplOffset,
    chunk_max: usize,
) -> Result<ReplOffset, HandoffError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut cursor = end_offset;
    loop {
        // Decide the next step UNDER the ring borrow, then DROP the borrow (the block ends) before
        // any `.await` -- so no `Ref` is ever alive across an await (the borrow discipline the repl
        // crate uses, and what `clippy::await_holding_refcell_ref` enforces).
        let step = {
            let r = ring.borrow();
            if r.needs_resync() {
                DeltaStep::Overflow
            } else if r.head() == cursor {
                DeltaStep::Done // the tail is fully drained: nothing past the cursor.
            } else if r.can_serve_from(cursor) {
                DeltaStep::Batch(r.ops_after(cursor, chunk_max))
            } else {
                DeltaStep::Overflow
            }
        };
        let batch: Vec<StreamOp> = match step {
            DeltaStep::Done => break,
            DeltaStep::Overflow => {
                return Err(abort_with(stream, HandoffError::DeltaOverflow).await);
            }
            DeltaStep::Batch(b) => b,
        };
        if batch.is_empty() {
            break;
        }
        for op in batch {
            let off = op.offset();
            let frame = match op {
                StreamOp::Put {
                    offset,
                    db,
                    key,
                    kvobj_bytes,
                } => Frame::StreamPut {
                    offset,
                    db,
                    key,
                    kvobj_bytes,
                },
                StreamOp::Del { offset, db, key } => Frame::StreamDel { offset, db, key },
            };
            if let Err(e) = write_msg(stream, &HandoffMsg::Frame(frame)).await {
                return Err(abort_with(stream, e).await);
            }
            cursor = cursor.max_with(off);
        }
    }

    write_msg(
        stream,
        &HandoffMsg::Cutover {
            final_offset: cursor,
        },
    )
    .await?;
    match read_msg(stream).await? {
        HandoffMsg::CutoverAck => Ok(cursor),
        HandoffMsg::Abort => Err(HandoffError::Aborted),
        other => Err(HandoffError::Unexpected {
            expected: "CUTOVER_ACK",
            got: other.label(),
        }),
    }
}

/// SENDER convenience: [`send_bulk`] then [`send_cutover`] with NO quiesce between (the simple
/// no-concurrent-writes case, e.g. tests and a read-mostly handoff). For the live near-zero-downtime
/// path the caller drives the two phases explicitly and quiesces between them.
///
/// # Errors
/// Any [`HandoffError`] from either phase. The old store is untouched on failure.
pub async fn send_shard<E, A, S>(
    stream: &mut S,
    store: &ShardStore<E, A>,
    ring: &Rc<RefCell<ReplRing>>,
    shard: u32,
    replid: ReplId,
    now: UnixMillis,
    chunk_max: usize,
) -> Result<ReplOffset, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let end_offset = send_bulk(stream, store, ring, shard, replid, now, chunk_max).await?;
    send_cutover(stream, ring, end_offset, chunk_max).await
}

// ---------------------------------------------------------------------------------------------
// RECEIVER (the NEW sibling): builds a FRESH store, adopts it ONLY on the fully-verified path.
// ---------------------------------------------------------------------------------------------

/// A shard's fully-received, cutover-acked store, ready to adopt into the live serve path. Produced
/// ONLY on the complete success path; on ANY error the partial store is dropped (never returned),
/// so a half-loaded store is never adopted.
#[derive(Debug)]
pub struct LoadedShard<E: EvictionHook, A: AccountingHook> {
    /// The freshly-loaded, delta-applied store for this shard.
    pub store: ShardStore<E, A>,
    /// The shard index this stream carried (from the HELLO).
    pub shard: u32,
    /// The final offset the tail was applied through (equals the sender's cut).
    pub final_offset: ReplOffset,
}

/// RECEIVER, phase 1 (BULK): handshake into a FRESH store, then load the whole shard keyspace.
///
/// Reads [`HandoffMsg::Hello`] (verifying the sender's database count matches `expected_databases`,
/// fail-closed), acks it, builds a fresh store via `make_store`, and applies each [`Frame::SyncKv`]
/// via `insert_object` until [`Frame::SyncEnd`]. Returns the loaded store + the cut `end_offset` for
/// [`recv_cutover`].
///
/// ABORT-SAFE: the fresh store is a local; on ANY error it is DROPPED (never returned), so a
/// half-loaded store is never adopted and the new sibling exits.
///
/// # Errors
/// Any [`HandoffError`] on a socket failure, a bad frame, a db-count mismatch, or a peer abort.
pub async fn recv_bulk<E, A, S, M>(
    stream: &mut S,
    mut make_store: M,
    expected_databases: u32,
    _now: UnixMillis,
) -> Result<(ShardStore<E, A>, u32, ReplOffset), HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
    M: FnMut() -> ShardStore<E, A>,
{
    let (shard, databases) = match read_msg(stream).await? {
        HandoffMsg::Hello { shard, databases } => (shard, databases),
        HandoffMsg::Abort => return Err(HandoffError::Aborted),
        other => {
            return Err(HandoffError::Unexpected {
                expected: "HELLO",
                got: other.label(),
            });
        }
    };
    if databases != expected_databases {
        // A db-count mismatch would mis-route `insert_object`: fail-closed, and tell the sender.
        return Err(abort_with(
            stream,
            HandoffError::DbMismatch {
                expected: expected_databases,
                found: databases,
            },
        )
        .await);
    }
    write_msg(stream, &HandoffMsg::HelloAck).await?;

    // The fresh store lives here; every early return below DROPS it (adopt nothing).
    let mut store = make_store();

    // The bulk stream MUST begin with FULLSYNC.
    let end_offset = match read_msg(stream).await? {
        HandoffMsg::Frame(Frame::FullSync { end_offset, .. }) => end_offset,
        HandoffMsg::Abort => return Err(HandoffError::Aborted),
        other => {
            return Err(HandoffError::Unexpected {
                expected: "FULLSYNC",
                got: other.label(),
            });
        }
    };

    loop {
        match read_msg(stream).await? {
            HandoffMsg::Frame(Frame::SyncKv {
                db, kvobj_bytes, ..
            }) => {
                let Some(obj) = decode_kvobj(&kvobj_bytes) else {
                    // A corrupt entry: drop the partial store (on return), abort.
                    return Err(HandoffError::Malformed);
                };
                store.insert_object(db, obj);
            }
            HandoffMsg::Frame(Frame::SyncEnd { .. }) => break,
            HandoffMsg::Abort => return Err(HandoffError::Aborted),
            other => {
                return Err(HandoffError::Unexpected {
                    expected: "SYNCKV/SYNCEND",
                    got: other.label(),
                });
            }
        }
    }

    Ok((store, shard, end_offset))
}

/// RECEIVER, phase 2 (DELTA + CUTOVER): apply the buffered tail, verify the cut, ack the flip.
///
/// Takes OWNERSHIP of the bulk-loaded `store`. Applies each [`Frame::StreamPut`] / `StreamDel` via
/// [`ironcache_repl::ReplicaApplier`] in offset order from `end_offset`. On [`HandoffMsg::Cutover`]
/// it verifies the applied offset EQUALS the sender's `final_offset` (fail-closed on a gap), sends
/// [`HandoffMsg::CutoverAck`], and RETURNS the adopted store. Any out-of-order delta frame
/// ([`ApplyOutcome::Gap`]), an offset mismatch, an EOF, or a peer abort DROPS the store and aborts:
/// a half-applied store is never adopted.
///
/// # Errors
/// Any [`HandoffError`] on a socket failure, a delta gap, an offset mismatch, or a peer abort.
pub async fn recv_cutover<E, A, S>(
    stream: &mut S,
    mut store: ShardStore<E, A>,
    shard: u32,
    end_offset: ReplOffset,
    now: UnixMillis,
) -> Result<LoadedShard<E, A>, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut applier = ReplicaApplier::new(end_offset);
    loop {
        match read_msg(stream).await? {
            HandoffMsg::Frame(frame @ (Frame::StreamPut { .. } | Frame::StreamDel { .. })) => {
                match applier.apply(&mut store, frame, now) {
                    // In-order apply, or a stale re-delivery (idempotent): keep going.
                    ApplyOutcome::Applied(_) | ApplyOutcome::Duplicate => {}
                    // A hole in the sequence: the tail is incomplete. Drop the store, abort.
                    ApplyOutcome::Gap => return Err(HandoffError::DeltaGap),
                }
            }
            HandoffMsg::Cutover { final_offset } => {
                if applier.applied() != final_offset {
                    return Err(HandoffError::OffsetMismatch {
                        expected: final_offset.0,
                        got: applier.applied().0,
                    });
                }
                write_msg(stream, &HandoffMsg::CutoverAck).await?;
                return Ok(LoadedShard {
                    store,
                    shard,
                    final_offset,
                });
            }
            HandoffMsg::Abort => return Err(HandoffError::Aborted),
            other => {
                return Err(HandoffError::Unexpected {
                    expected: "STREAMPUT/STREAMDEL/CUTOVER",
                    got: other.label(),
                });
            }
        }
    }
}

/// RECEIVER convenience: [`recv_bulk`] then [`recv_cutover`], for the simple no-quiesce case (the
/// mirror of [`send_shard`]). Returns the fully-loaded, cutover-acked [`LoadedShard`], or an error
/// that drops the partial store.
///
/// # Errors
/// Any [`HandoffError`] from either phase; the partial store is dropped on failure (adopt nothing).
pub async fn recv_shard<E, A, S, M>(
    stream: &mut S,
    make_store: M,
    expected_databases: u32,
    now: UnixMillis,
) -> Result<LoadedShard<E, A>, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
    M: FnMut() -> ShardStore<E, A>,
{
    let (store, shard, end_offset) = recv_bulk(stream, make_store, expected_databases, now).await?;
    recv_cutover(stream, store, shard, end_offset, now).await
}

// ---------------------------------------------------------------------------------------------
// CROSS-SHARD ATOMIC CUTOVER BARRIER (the one place shards synchronize, ADR-0002).
// ---------------------------------------------------------------------------------------------

/// The client-visible flip decision, gathered across all shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutoverState {
    /// Not every shard has reported yet: hold (do NOT flip, do NOT abort).
    Pending,
    /// EVERY shard committed (acked its cutover): the sibling adopts all shards + serves; the old
    /// process stops serving.
    Commit,
    /// At least one shard aborted: the WHOLE flip is off. The sibling exits without serving; the
    /// old process keeps serving every shard. No key is ever served by neither/both.
    Abort,
}

/// The pure cross-shard cutover barrier: the ONE synchronization point in the otherwise
/// shared-nothing handoff (ADR-0002). Each per-shard stream reports its outcome; the flip is
/// ALL-OR-NOTHING -- [`CutoverState::Commit`] only once every shard has committed, and
/// [`CutoverState::Abort`] the instant any shard aborts (an abort is sticky and wins over any number
/// of commits). This is what makes the atomic cutover atomic: there is never a window where some
/// shards are new and some are old for a client.
#[derive(Debug, Clone)]
pub struct CutoverBarrier {
    /// The total shard count the flip must gather.
    total: usize,
    /// How many shards have committed so far.
    committed: usize,
    /// Sticky: set the instant any shard aborts; an abort can never be un-set by a later commit.
    aborted: bool,
}

impl CutoverBarrier {
    /// A barrier awaiting `total` shards. `total == 0` is a degenerate empty handoff that commits
    /// immediately (nothing to transfer).
    #[must_use]
    pub fn new(total: usize) -> Self {
        CutoverBarrier {
            total,
            committed: 0,
            aborted: false,
        }
    }

    /// Record that a shard committed (its [`recv_cutover`] / [`send_cutover`] acked). Saturates at
    /// `total` (a duplicate report cannot over-count).
    pub fn record_commit(&mut self) {
        self.committed = self.committed.saturating_add(1).min(self.total);
    }

    /// Record that a shard aborted (any [`HandoffError`]). Sticky: the whole flip is now off.
    pub fn record_abort(&mut self) {
        self.aborted = true;
    }

    /// The current flip decision. [`CutoverState::Abort`] wins over everything (fail-closed); else
    /// [`CutoverState::Commit`] once every shard has committed; else [`CutoverState::Pending`].
    #[must_use]
    pub fn state(&self) -> CutoverState {
        if self.aborted {
            CutoverState::Abort
        } else if self.committed >= self.total {
            CutoverState::Commit
        } else {
            CutoverState::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- envelope codec round-trip + fail-closed ----

    fn sample_msgs() -> Vec<HandoffMsg> {
        vec![
            HandoffMsg::Hello {
                shard: 3,
                databases: 16,
            },
            HandoffMsg::HelloAck,
            HandoffMsg::Frame(Frame::FullSync {
                replid: ReplId::from_bytes([0xAB; 20]),
                end_offset: ReplOffset(42),
            }),
            HandoffMsg::Frame(Frame::SyncKv {
                db: 2,
                key: b"binary\r\nkey\x00".to_vec(),
                kvobj_bytes: vec![0u8, 1, 2, b'\r', b'\n', 255],
            }),
            HandoffMsg::Frame(Frame::SyncEnd {
                end_offset: ReplOffset(42),
            }),
            HandoffMsg::Frame(Frame::StreamPut {
                offset: ReplOffset(43),
                db: 0,
                key: b"k".to_vec(),
                kvobj_bytes: b"v".to_vec(),
            }),
            HandoffMsg::Frame(Frame::StreamDel {
                offset: ReplOffset(44),
                db: 1,
                key: b"gone".to_vec(),
            }),
            HandoffMsg::Cutover {
                final_offset: ReplOffset(u64::MAX),
            },
            HandoffMsg::CutoverAck,
            HandoffMsg::Abort,
        ]
    }

    #[test]
    fn envelope_round_trips_every_message() {
        for msg in sample_msgs() {
            let mut bytes = encode_envelope(&msg);
            let decoded = decode_envelope(&mut bytes).expect("a well-formed envelope decodes");
            assert_eq!(decoded, msg, "round-trip mismatch for {msg:?}");
        }
    }

    #[test]
    fn envelope_rejects_a_crc_flip() {
        // Flip a single payload byte after CRC-sealing: the recompute must not match -> Crc error.
        let msg = HandoffMsg::Frame(Frame::SyncKv {
            db: 0,
            key: b"key".to_vec(),
            kvobj_bytes: b"value".to_vec(),
        });
        let mut bytes = encode_envelope(&msg);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(matches!(
            decode_envelope(&mut bytes),
            Err(HandoffError::Crc)
        ));
    }

    #[test]
    fn envelope_rejects_a_header_flip() {
        // Corrupt the KIND byte after sealing: the CRC covers the header too -> Crc error (not a
        // silent mis-decode into the wrong message).
        let msg = HandoffMsg::HelloAck;
        let mut bytes = encode_envelope(&msg);
        bytes[6] ^= 0xFF; // the kind byte
        assert!(matches!(
            decode_envelope(&mut bytes),
            Err(HandoffError::Crc)
        ));
    }

    #[test]
    fn envelope_rejects_bad_magic() {
        let mut bytes = encode_envelope(&HandoffMsg::HelloAck);
        bytes[0] = b'X';
        // Re-seal a valid CRC so we exercise the MAGIC check, not the CRC check.
        bytes[12..16].copy_from_slice(&[0u8; 4]);
        let crc = crc32(&bytes);
        bytes[12..16].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_envelope(&mut bytes),
            Err(HandoffError::BadMagic)
        ));
    }

    #[test]
    fn envelope_rejects_unknown_version() {
        let mut bytes = encode_envelope(&HandoffMsg::HelloAck);
        bytes[4..6].copy_from_slice(&(PROTO_VERSION + 1).to_le_bytes());
        // Re-seal a valid CRC so the VERSION check (not the CRC) is what rejects it.
        bytes[12..16].copy_from_slice(&[0u8; 4]);
        let crc = crc32(&bytes);
        bytes[12..16].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_envelope(&mut bytes),
            Err(HandoffError::UnsupportedVersion { found, supported })
                if found == PROTO_VERSION + 1 && supported == PROTO_VERSION
        ));
    }

    #[test]
    fn envelope_rejects_malformed_inner_frame() {
        // A CRC-valid envelope of kind FRAME whose payload is not a decodable repl frame.
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&PROTO_VERSION.to_le_bytes());
        out.push(K_FRAME);
        out.push(0);
        let payload = vec![0xFFu8, 0xFF, 0xFF];
        out.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
        out.extend_from_slice(&[0u8; 4]);
        out.extend_from_slice(&payload);
        let crc = crc32(&out);
        out[12..16].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_envelope(&mut out),
            Err(HandoffError::Malformed)
        ));
    }

    // ---- the cross-shard cutover barrier ----

    #[test]
    fn barrier_commits_only_when_all_shards_commit() {
        let mut b = CutoverBarrier::new(3);
        assert_eq!(b.state(), CutoverState::Pending);
        b.record_commit();
        assert_eq!(b.state(), CutoverState::Pending, "1/3 is not enough");
        b.record_commit();
        assert_eq!(b.state(), CutoverState::Pending, "2/3 is not enough");
        b.record_commit();
        assert_eq!(b.state(), CutoverState::Commit, "3/3 commits the flip");
    }

    #[test]
    fn barrier_abort_is_sticky_and_wins_over_commits() {
        // Two shards commit, one aborts: the WHOLE flip is off (fail-closed), even though a majority
        // committed. This is the "atomic across shards" guarantee.
        let mut b = CutoverBarrier::new(3);
        b.record_commit();
        b.record_commit();
        b.record_abort();
        assert_eq!(b.state(), CutoverState::Abort);
        // A late commit cannot un-abort it.
        b.record_commit();
        assert_eq!(b.state(), CutoverState::Abort, "an abort is sticky");
    }

    #[test]
    fn barrier_empty_handoff_commits_immediately() {
        assert_eq!(CutoverBarrier::new(0).state(), CutoverState::Commit);
    }
}

/// End-to-end tests over a REAL AF_UNIX socket pair ([`tokio::net::UnixStream::pair`]): the same
/// transport the live handoff uses. Data-safety FIRST -- the abort tests assert the OLD store stays
/// intact and NO partial store is adopted on any failure. Run on a `current_thread` runtime because
/// [`ShardStore`] / [`ReplRing`] are the shared-nothing single-thread (`Rc`) types, so the futures
/// are `!Send` and are driven concurrently with `tokio::join!` on one thread.
#[cfg(all(test, unix))]
mod socket_tests {
    use super::*;
    use ironcache_repl::ReplObserver;
    use ironcache_storage::{ExpireWrite, NewValue, Store};
    use tokio::net::UnixStream;

    const NOW: UnixMillis = UnixMillis(1_000);
    const DBS: u32 = 4;

    fn replid() -> ReplId {
        ReplId::from_bytes([0xAB; 20])
    }

    /// A fresh store with an OBSERVED ring installed BEFORE the writes (so the ring tracks every
    /// write as a [`StreamOp`]), populated with `n` string keys spread across the databases.
    fn populated(n: u32, tag: &str) -> (ShardStore, Rc<RefCell<ReplRing>>) {
        let ring = ReplRing::new(4096, ReplOffset::ZERO);
        let mut s = ShardStore::new(DBS);
        s.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        for i in 0..n {
            let key = format!("{tag}-k{i}");
            let val = format!("{tag}-v{i}");
            s.upsert(
                i % DBS,
                key.as_bytes(),
                NewValue::Bytes(val.as_bytes()),
                ExpireWrite::Clear,
                NOW,
            );
        }
        (s, ring)
    }

    /// DATA-SAFETY: a populated multi-shard dataset streams old->new and the new stores serve EVERY
    /// key after the cutover; the cross-shard barrier commits (all shards handed off).
    #[tokio::test(flavor = "current_thread")]
    async fn streams_multi_shard_dataset_and_serves_every_key() {
        let shard_count = 3u32;
        let per_shard = 25u32;
        let mut barrier = CutoverBarrier::new(shard_count as usize);
        let mut adopted: Vec<ShardStore> = Vec::new();

        for shard in 0..shard_count {
            let tag = format!("s{shard}");
            let (src, ring) = populated(per_shard, &tag);
            let (mut a, mut b) = UnixStream::pair().expect("socketpair");

            let send = send_shard(&mut a, &src, &ring, shard, replid(), NOW, 4);
            let recv = recv_shard(&mut b, || ShardStore::new(DBS), DBS, NOW);
            let (sres, rres) = tokio::join!(send, recv);

            let final_off = sres.expect("send completes");
            let loaded = rres.expect("recv completes");
            assert_eq!(loaded.shard, shard, "the shard id round-trips");
            assert_eq!(
                loaded.final_offset, final_off,
                "sender + receiver agree on the cut"
            );
            barrier.record_commit();
            adopted.push(loaded.store);
        }

        assert_eq!(
            barrier.state(),
            CutoverState::Commit,
            "every shard committed -> the flip commits"
        );

        // Every key is served by the adopted (new) stores.
        for (shard, store) in adopted.iter_mut().enumerate() {
            let tag = format!("s{shard}");
            for i in 0..per_shard {
                let key = format!("{tag}-k{i}");
                let want = format!("{tag}-v{i}");
                assert_eq!(
                    store.read(i % DBS, key.as_bytes(), NOW).unwrap().as_bytes(),
                    want.as_bytes(),
                    "adopted shard {shard} serves {key}"
                );
            }
        }
    }

    /// DELTA CORRECTNESS: writes made DURING the transfer window (after the bulk cut, before
    /// cutover) are present after the cutover. Drives the two phases explicitly: bulk, then a set of
    /// post-cut writes (a create, an overwrite, a delete), then the delta+cutover phase.
    #[tokio::test(flavor = "current_thread")]
    async fn writes_during_transfer_are_present_after_cutover() {
        let (mut src, ring) = populated(10, "d");
        let (mut a, mut b) = UnixStream::pair().expect("socketpair");

        // Phase 1: BULK (concurrently). The cut is captured at the start of send_bulk.
        let (end_off, mut store) = {
            let send = send_bulk(&mut a, &src, &ring, 0, replid(), NOW, 4);
            let recv = recv_bulk(&mut b, || ShardStore::new(DBS), DBS, NOW);
            let (s, r) = tokio::join!(send, recv);
            let end_s = s.expect("bulk send completes");
            let (store, _shard, end_r) = r.expect("bulk recv completes");
            assert_eq!(end_s, end_r, "sender + receiver agree on the cut offset");
            (end_s, store)
        };

        // Writes AFTER the cut (offset > end_off): a NEW key, an OVERWRITE of an existing key, and a
        // DELETE. These are captured by the observer ring as the delta.
        // A CREATE (new key, db 0), an OVERWRITE of an existing bulk key in ITS home db (d-k4 was
        // written with i%DBS = 4%4 = 0, so db 0), and a DELETE of an existing key (d-k1 -> db 1).
        src.upsert(
            0,
            b"d-new",
            NewValue::Bytes(b"fresh"),
            ExpireWrite::Clear,
            NOW,
        );
        src.upsert(
            0,
            b"d-k4",
            NewValue::Bytes(b"overwritten"),
            ExpireWrite::Clear,
            NOW,
        );
        src.delete(1, b"d-k1", NOW);

        // Phase 2: DELTA + CUTOVER (concurrently).
        let send = send_cutover(&mut a, &ring, end_off, 4);
        let recv = recv_cutover(&mut b, store, 0, end_off, NOW);
        let (sres, rres) = tokio::join!(send, recv);
        let final_off = sres.expect("cutover send completes");
        let loaded = rres.expect("cutover recv completes");
        assert_eq!(loaded.final_offset, final_off);
        store = loaded.store;

        // The delta writes converged onto the new store (last-write-wins via the tail).
        assert_eq!(
            store.read(0, b"d-new", NOW).unwrap().as_bytes(),
            b"fresh",
            "a create during transfer is present after cutover"
        );
        assert_eq!(
            store.read(0, b"d-k4", NOW).unwrap().as_bytes(),
            b"overwritten",
            "an overwrite during transfer wins after cutover"
        );
        assert!(
            store.read(1, b"d-k1", NOW).is_none(),
            "a delete during transfer is applied after cutover"
        );
        // An untouched bulk key is still served.
        assert_eq!(
            store.read(0, b"d-k0", NOW).unwrap().as_bytes(),
            b"d-v0",
            "an untouched bulk key survives"
        );
    }

    /// ABORT-SAFETY (db-count mismatch): the receiver fail-closes on a config mismatch, aborts, and
    /// the SENDER's store stays intact + NO store is adopted -> the OLD process keeps serving.
    #[tokio::test(flavor = "current_thread")]
    async fn db_mismatch_aborts_and_leaves_old_serving() {
        let (mut src, ring) = populated(15, "m");
        let (mut a, mut b) = UnixStream::pair().expect("socketpair");

        // The receiver expects a DIFFERENT database count than the sender advertises.
        let send = send_shard(&mut a, &src, &ring, 0, replid(), NOW, 4);
        let recv = recv_shard(&mut b, || ShardStore::new(2), 2, NOW);
        let (sres, rres) = tokio::join!(send, recv);

        assert!(rres.is_err(), "the receiver fail-closes on the db mismatch");
        assert!(sres.is_err(), "the sender sees the abort and stops");

        // The OLD store is fully intact (the sender only READ it): every key still serves.
        for i in 0..15u32 {
            let key = format!("m-k{i}");
            let want = format!("m-v{i}");
            assert_eq!(
                src.read(i % DBS, key.as_bytes(), NOW).unwrap().as_bytes(),
                want.as_bytes(),
                "old process still serves {key} after the aborted handoff"
            );
        }
    }

    /// ABORT-SAFETY (delta overflow): a tiny ring that overflows during the transfer makes the
    /// sender abort at cutover; the fully-bulk-loaded receiver DROPS its store (adopts nothing) and
    /// the OLD store is intact.
    #[tokio::test(flavor = "current_thread")]
    async fn delta_overflow_aborts_and_adopts_nothing() {
        // A cap-1 ring, then 20 writes: the ring overflows (needs_resync latched), so the delta has
        // an unfillable gap.
        let ring = ReplRing::new(1, ReplOffset::ZERO);
        let mut src = ShardStore::new(DBS);
        src.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        for i in 0..20u32 {
            let key = format!("o-k{i}");
            src.upsert(
                i % DBS,
                key.as_bytes(),
                NewValue::Bytes(b"v"),
                ExpireWrite::Clear,
                NOW,
            );
        }
        assert!(ring.borrow().needs_resync(), "the cap-1 ring overflowed");

        let (mut a, mut b) = UnixStream::pair().expect("socketpair");
        let send = send_shard(&mut a, &src, &ring, 0, replid(), NOW, 4);
        let recv = recv_shard(&mut b, || ShardStore::new(DBS), DBS, NOW);
        let (sres, rres) = tokio::join!(send, recv);

        assert!(
            matches!(sres, Err(HandoffError::DeltaOverflow)),
            "the sender aborts on the overflowed delta: {sres:?}"
        );
        assert!(
            rres.is_err(),
            "the receiver drops its bulk-loaded store (adopts nothing) on the abort"
        );
        // The old store still serves every key (the snapshot only read it).
        for i in 0..20u32 {
            let key = format!("o-k{i}");
            assert!(
                src.read(i % DBS, key.as_bytes(), NOW).is_some(),
                "old process still serves {key} after the aborted handoff"
            );
        }
    }

    /// ABORT-SAFETY (peer crash mid-transfer): the receiver drops the socket right after the
    /// handshake; the sender's stream errors out and it aborts, leaving the OLD store intact.
    #[tokio::test(flavor = "current_thread")]
    async fn receiver_crash_mid_transfer_aborts_and_leaves_old_serving() {
        let (mut src, ring) = populated(60, "c");
        let (mut a, b) = UnixStream::pair().expect("socketpair");

        // A receiver that handshakes then CRASHES (drops the socket) before the bulk arrives.
        let crasher = async move {
            let mut b = b;
            let _ = read_msg(&mut b).await; // HELLO
            let _ = write_msg(&mut b, &HandoffMsg::HelloAck).await;
            // b is dropped here: the sender's bulk writes / cutover-ack read now fail.
        };
        // chunk_max = 1 so the bulk is many frames (the broken pipe bites promptly).
        let send = send_shard(&mut a, &src, &ring, 0, replid(), NOW, 1);
        let (sres, ()) = tokio::join!(send, crasher);

        assert!(
            sres.is_err(),
            "the sender aborts when the receiver crashes mid-transfer"
        );
        // The old store is intact.
        assert_eq!(
            src.read(0, b"c-k0", NOW).unwrap().as_bytes(),
            b"c-v0",
            "old process still serves after the peer crashed"
        );
    }
}
