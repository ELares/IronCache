// SPDX-License-Identifier: MIT OR Apache-2.0
//! The primary-side replication observer + its bounded op ring (HA-7c).
//!
//! This is the source of the steady-state replication tail. It plugs the HA-5a write
//! -observation seam ([`ironcache_store::WriteObserver`]) into the replication stream: an
//! installed [`ReplObserver`] turns EVERY applied write on the primary's shard store into a
//! [`StreamOp`] tagged with a strictly-increasing [`ReplOffset`], enqueued onto a BOUNDED,
//! shard-local [`ReplRing`] that the primary's stream task ([`crate::stream`]) drains and
//! ships in offset order.
//!
//! ## The per-write offset advance (replacing HA-7a's per-tick stub)
//!
//! HA-7a advanced the primary's offset trivially (per heartbeat tick) just to exercise the
//! cursor. HA-7c makes it REAL: the offset is the logical write-sequence number, advanced
//! ONCE per observed write, here in the observer. `on_put` (a create / overwrite / in-place
//! edit / TTL change) and `on_remove` (a delete / expiry / flush / eviction) each bump the
//! offset by one and enqueue the matching op carrying its assigned offset. The offset is
//! monotonic and gap-free per shard: the replica's apply loop relies on every offset in
//! `(end_offset, head]` being present exactly once.
//!
//! ## The bounded ring + full-on-overflow (never block the funnel)
//!
//! The observer runs INSIDE the store's write funnel, inline on the owning core (ADR-0002,
//! single-threaded shard). So the enqueue MUST be O(1) and non-blocking; it must NEVER block
//! waiting for the stream task to drain. The ring is therefore BOUNDED ([`ReplRing::cap`]):
//! when it is full, the observer DROPS the op and latches a `must_resync` flag instead of
//! blocking. A dropped op means the replica can no longer be brought current by the tail (a
//! gap), so the stream task, seeing `must_resync`, tears the replica down to a fresh HA-7b
//! full re-sync. This is the MVP "full-resync-on-gap" policy: correct (the replica always
//! converges) though not optimal (a partial-resync / disk backlog spill is HA-7e, deferred).
//!
//! The offset STILL advances on a dropped op (the write happened; the logical sequence must
//! account for it), so once a resync re-bases the replica at a fresh `end_offset` the
//! surviving tail is still gap-free above that cut.
//!
//! ## Shared-nothing sharing (ADR-0002)
//!
//! The ring is shared between the observer (which the store owns, boxed) and the stream task
//! (which drains it) via `Rc<RefCell<..>>` -- the single-shard, no-cross-core-lock idiom the
//! rest of the crate uses ([`crate::transport::ReplicaObserver`]). Both live on the SAME
//! shard core, so the `RefCell` borrow is never contended across threads.

use std::collections::VecDeque;
use std::rc::Rc;

use core::cell::RefCell;

use ironcache_store::{Entry, WriteObserver};

use crate::cursor::ReplOffset;
use crate::disk_backlog::DiskBacklog;
use crate::kvcodec::encode_kvobj;

/// One steady-state replication operation in the tail (HA-7c): a put or a delete, tagged with
/// the strictly-increasing [`ReplOffset`] the observer assigned it. The stream task ships
/// these in offset order as [`crate::frames::Frame::StreamPut`] / `StreamDel`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamOp {
    /// A create / overwrite / in-place edit / TTL change: the post-image, encoded.
    Put {
        /// The logical offset this write occupies (strictly increasing per shard).
        offset: ReplOffset,
        /// The database the write belongs to.
        db: u32,
        /// The key bytes.
        key: Vec<u8>,
        /// The [`crate::kvcodec::encode_kvobj`] encoding of the committed post-image.
        kvobj_bytes: Vec<u8>,
    },
    /// A delete / expiry / flush / eviction / emptied-collection: `(db, key)` left the
    /// keyspace.
    Del {
        /// The logical offset this delete occupies (strictly increasing per shard).
        offset: ReplOffset,
        /// The database the key left.
        db: u32,
        /// The key removed.
        key: Vec<u8>,
    },
}

impl StreamOp {
    /// The logical offset this op was assigned.
    #[must_use]
    pub fn offset(&self) -> ReplOffset {
        match self {
            StreamOp::Put { offset, .. } | StreamOp::Del { offset, .. } => *offset,
        }
    }
}

/// The BOUNDED, shard-local op buffer the observer enqueues into and the stream task both
/// drains and RE-SENDS from. It is the backlog between the inline (funnel) producer and the
/// async stream-task consumer, AND the primary's reconnect-resume window.
///
/// ## What it retains, and the single bound
///
/// It retains the ops in the offset window `(acked, head]`: every op the primary has produced
/// that the replica has NOT yet acked. That is exactly the set the primary may still need to
/// (re-)send -- a freshly produced op the stream task has not shipped yet, AND a shipped op
/// the replica might re-request after a reconnect (resume from its acked offset). An op is
/// dropped from the buffer only when the replica ACKS past it ([`Self::ack`]).
///
/// The buffer is bounded by `cap`: when producing an op would push the retained window beyond
/// `cap` ops, the OLDEST retained op is evicted to make room and `must_resync` is latched --
/// because evicting an un-acked op means the primary can no longer serve a replica that asks
/// to resume from before it (a GAP). The funnel NEVER blocks: it always makes room and moves
/// on. `head` (the highest offset ever assigned) advances per produced write regardless, so
/// once a resync re-bases the replica at a fresh cut the surviving window is gap-free above
/// it.
///
/// ## PER-CONNECTION drain (no shared fan-out cursor): read-only window reads
///
/// The ring holds NO single fan-out cursor. Each replica/import CONNECTION keeps its OWN local
/// send cursor (the highest offset IT has shipped) and reads ops it has not yet sent by reading
/// FORWARD of that local cursor with [`Self::ops_after`] -- a READ-ONLY scan that does NOT
/// mutate ring state, so two concurrent consumers (e.g. an HA-7 replica connection AND an HA-6
/// importer connection both draining this one source shard's ring) EACH see EVERY op in
/// `(their cursor, head]`. A single shared cursor would have split the tail: each op going to
/// whichever consumer drained it first, the other silently missing it (divergence / lost write).
/// Reading forward of a per-connection cursor instead fans the tail out faithfully to all.
///
/// Retention stays the bounded `(oldest_retained, head]` window: an op stays retained for a
/// possible re-send until the replica ACKS past it ([`Self::ack`]) or the window overflows
/// `cap` (evicting the oldest + latching `must_resync`). A connection whose local cursor falls
/// BELOW [`Self::oldest_retained`] (it fell behind the retained window) cannot be served the
/// next op it needs -> [`Self::can_serve_from`] is false and that connection FULL-RE-SYNCS
/// (the existing Gap path) rather than silently skipping. Pruning is by the replica's ack (the
/// single steady-state consumer's resume bound) plus the hard `cap`; a consumer the ack pruned
/// past simply fall-behind-resyncs, which is correct for any number of consumers.
#[derive(Debug)]
pub struct ReplRing {
    /// The retained ops in `(acked, head]`, oldest first. Length never exceeds `cap`.
    ops: VecDeque<StreamOp>,
    /// The bound on the retained window: producing past it evicts the oldest + latches resync.
    cap: usize,
    /// The highest offset ever ASSIGNED (the primary's current logical offset), advancing
    /// once per observed write whether or not the op was retained or evicted.
    head: ReplOffset,
    /// The highest offset the replica has ACKED; ops at or below it are pruned (the replica
    /// has them durably). Monotonic.
    acked: ReplOffset,
    /// Latched when an UN-ACKED op was evicted AND could not be spilled to disk (no disk backlog,
    /// or a spill failed / staging overflowed) -- the resume window has a gap the incremental path
    /// cannot fill, so the replica that needs it must drop to a fresh HA-7b full re-sync. Cleared
    /// by [`Self::take_resync`].
    must_resync: bool,
    /// HA-7e: the optional DISK-BACKED spill of evicted ops, widening the incremental-resync
    /// window. `None` (the DEFAULT) = in-memory-only, byte-identical to pre-HA-7e: `push` then
    /// latches `must_resync` on eviction exactly as before. `Some` = an evicted op is STAGED for a
    /// disk spill (see `spill_stage`) instead of latching resync.
    disk: Option<DiskBacklog>,
    /// HA-7e: ops EVICTED from the in-memory window awaiting a disk flush, oldest first. `push`
    /// (running INLINE in the write funnel, ADR-0002) only moves an evicted op here -- an O(1),
    /// non-blocking step, NO fsync -- and the off-funnel stream task drains it to a disk segment via
    /// [`Self::flush_spill`]. Empty + unused when `disk` is `None`.
    spill_stage: VecDeque<StreamOp>,
}

impl ReplRing {
    /// A fresh, empty buffer bounded at `cap` (clamped to at least 1 so progress is possible),
    /// starting at offset `start` (the primary's offset at install time; `ReplOffset::ZERO`
    /// for a fresh primary). Wrapped in an `Rc<RefCell<..>>` for sharing between the observer
    /// the store owns and the stream task that drains it (same shard core, no cross-core
    /// lock; ADR-0002).
    #[must_use]
    pub fn new(cap: usize, start: ReplOffset) -> Rc<RefCell<Self>> {
        Self::with_disk(cap, start, None)
    }

    /// A fresh ring bounded at `cap` from offset `start`, with an OPTIONAL disk-backed spill
    /// (HA-7e). `disk == None` is byte-identical to [`Self::new`] (the in-memory-only path);
    /// `disk == Some(..)` makes `push` STAGE an evicted op for a disk spill (widening the
    /// incremental-resync window) instead of latching `must_resync`. The disk backlog is built by
    /// the serve layer ([`DiskBacklog::open`], `None` when the size knob is 0 / no data_dir), so
    /// the default deployment passes `None` and nothing changes.
    #[must_use]
    pub fn with_disk(
        cap: usize,
        start: ReplOffset,
        disk: Option<DiskBacklog>,
    ) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(ReplRing {
            ops: VecDeque::new(),
            cap: cap.max(1),
            head: start,
            acked: start,
            must_resync: false,
            disk,
            spill_stage: VecDeque::new(),
        }))
    }

    /// The primary's current logical offset: the highest offset ever assigned (advancing per
    /// observed write, including evicted ones). This is the value the HA-7a heartbeat
    /// advertises and the offset a fresh full-sync cuts at.
    #[must_use]
    pub fn head(&self) -> ReplOffset {
        self.head
    }

    /// The highest offset the replica has acked (the pruned-through point).
    #[must_use]
    pub fn acked(&self) -> ReplOffset {
        self.acked
    }

    /// Whether the buffer has overflowed (evicted an un-acked op) since the last
    /// [`Self::take_resync`] -- a resume-window gap that forces a full re-sync.
    #[must_use]
    pub fn needs_resync(&self) -> bool {
        self.must_resync
    }

    /// The number of retained ops in the window `(acked, head]` (introspection / tests).
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether no ops are retained (introspection / tests).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// The lowest offset still retained (the front of the resume window), or `head` if the
    /// buffer is empty. The primary can serve a resume from `from` iff `from + 1 >= this`. A
    /// per-connection consumer whose local cursor falls below this has fallen behind the
    /// retained window and must full-re-sync (see [`Self::can_serve_from`]).
    #[must_use]
    pub fn oldest_retained(&self) -> ReplOffset {
        self.ops.front().map_or(self.head, StreamOp::offset)
    }

    /// Whether the primary can still serve a replica resuming from acked offset `from`: i.e.
    /// the next op it needs (`from + 1`) is recoverable from EITHER the in-memory window OR (HA-7e)
    /// the disk-backed spill (or there is nothing past `from`). False only once the op at `from + 1`
    /// predates even the DISK range (a resume gap the incremental path cannot fill -> full-resync).
    ///
    /// With no disk backlog (the DEFAULT) this is exactly the pre-HA-7e check: `from + 1 >=
    /// oldest_retained`. With a disk backlog the recoverable floor drops to the disk's oldest spilled
    /// offset, widening the incremental window; the disk + staging + memory ranges are contiguous, so
    /// any `from + 1` at or above the disk floor (up to `head`) can be served gap-free.
    #[must_use]
    pub fn can_serve_from(&self, from: ReplOffset) -> bool {
        if from.0 >= self.head.0 {
            return true; // caught up: nothing to serve
        }
        // The first op the replica still needs is from+1; it must be >= the lowest recoverable
        // offset (the disk floor when a disk backlog holds spilled ops, else the in-memory oldest).
        from.next().0 >= self.oldest_recoverable().0
    }

    /// The lowest offset still RECOVERABLE incrementally (HA-7e): the oldest disk-spilled offset
    /// when a disk backlog holds any (the wider window), else the oldest in-memory retained offset
    /// (or `head` when nothing is buffered). This is the floor [`Self::can_serve_from`] compares
    /// against. The disk, staging, and in-memory ranges are CONTIGUOUS, so the recoverable run is
    /// `[oldest_recoverable, head]` with no holes.
    #[must_use]
    pub fn oldest_recoverable(&self) -> ReplOffset {
        // Prefer the disk floor (it is below the in-memory + staging ranges). A staged-but-not-yet
        // -flushed op also lowers the floor; account for it so a replica is not told to full-resync
        // for an op that is still recoverable (it is in staging or about to be on disk).
        if let Some(disk) = &self.disk {
            if let Some(oldest) = disk.oldest_offset() {
                return oldest;
            }
        }
        if let Some(staged) = self.spill_stage.front() {
            return staged.offset();
        }
        self.oldest_retained()
    }

    /// Assign `op` the NEXT offset and retain it. Advances `head` unconditionally (the write
    /// happened). If the retained window is at `cap`, EVICT the oldest retained op. NEVER blocks
    /// (runs INLINE in the write funnel, ADR-0002). The op's offset field is overwritten with the
    /// freshly-assigned offset.
    ///
    /// ## HA-7e: spill-on-evict (when a disk backlog is configured)
    ///
    /// The evicted op leaves the in-memory window. With NO disk backlog (the DEFAULT) this latches
    /// `must_resync` (a resume gap -> the replica full-re-syncs), byte-identical to pre-HA-7e. With
    /// a disk backlog the evicted op is instead STAGED ([`Self::spill_stage`]) for an off-funnel
    /// disk flush ([`Self::flush_spill`]) -- an O(1), NON-BLOCKING move, NO fsync on the funnel --
    /// so the off-funnel stream task can durably append it to a segment and a behind replica can
    /// catch up incrementally from disk. Only if the staging buffer itself overflows its defensive
    /// bound (the flusher has not kept up) does this fall back to latching `must_resync`, so the
    /// funnel can never be back-pressured by disk I/O.
    ///
    /// Returns the offset assigned.
    fn push(&mut self, mut op: StreamOp) -> ReplOffset {
        let assigned = self.head.next();
        self.head = assigned;
        match &mut op {
            StreamOp::Put { offset, .. } | StreamOp::Del { offset, .. } => *offset = assigned,
        }
        if self.ops.len() >= self.cap {
            // The retained window is full: evict the oldest un-acked op to make room.
            let evicted = self.ops.pop_front();
            if self.disk.is_some() {
                // HA-7e: stage the evicted op for an off-funnel disk spill instead of latching a
                // resume gap. Strictly-increasing eviction order keeps the staging (and thus the
                // disk run) contiguous. A defensive bound stops unbounded growth if the flusher
                // stalls: past it, drop the latch (full-resync) rather than back-pressure the funnel.
                if let Some(ev) = evicted {
                    self.spill_stage.push_back(ev);
                }
                if self.spill_stage.len() > self.spill_stage_bound() {
                    // The flusher fell too far behind: drop the oldest staged op (it is lost from
                    // the disk window) and latch resync -- the safe fallback, never a funnel stall.
                    self.spill_stage.pop_front();
                    self.must_resync = true;
                }
            } else {
                // The in-memory-only path (DEFAULT): latch the resume gap, byte-identical to before.
                self.must_resync = true;
            }
        }
        self.ops.push_back(op);
        assigned
    }

    /// The defensive bound on the [`Self::spill_stage`] depth: the off-funnel flusher drains it on
    /// every stream pass, so it stays near-empty in practice; this caps it at `cap` so a stalled
    /// flusher cannot grow it without limit (past it `push` drops + latches resync, never blocks).
    fn spill_stage_bound(&self) -> usize {
        self.cap
    }

    /// READ-ONLY: copy up to `max` retained ops with offset strictly greater than `cursor`, in
    /// offset order, WITHOUT mutating any ring state (no shared cursor advance, no prune). Each
    /// CONNECTION calls this with ITS OWN local send cursor, so two concurrent consumers
    /// draining one ring EACH see every op past their own cursor -- the per-connection fan-out
    /// that the old shared `send_cursor` (which split the tail between consumers) is replaced
    /// by. The caller advances its OWN local cursor past the returned ops and ships them; they
    /// stay RETAINED for other consumers / a re-send until acked. `max == 0` returns nothing.
    ///
    /// The borrow-then-release discipline is unchanged: the caller holds the ring borrow only
    /// for this O(min(len, max)) copy, then drops it before awaiting the sends.
    #[must_use]
    pub fn ops_after(&self, cursor: ReplOffset, max: usize) -> Vec<StreamOp> {
        let mut out = Vec::new();
        for op in &self.ops {
            if out.len() >= max {
                break;
            }
            if op.offset().0 > cursor.0 {
                out.push(op.clone());
            }
        }
        out
    }

    /// Whether this ring has a disk-backed backlog configured (HA-7e). `false` is the DEFAULT
    /// in-memory-only path.
    #[must_use]
    pub fn has_disk(&self) -> bool {
        self.disk.is_some()
    }

    /// FLUSH any staged-but-not-yet-spilled evicted ops ([`Self::spill_stage`]) into the disk
    /// backlog as a sealed segment (HA-7e). Called OFF the write funnel by the stream task (e.g. at
    /// the top of each serve pass), so the fsync never blocks a write. A no-op when there is no disk
    /// backlog or nothing is staged.
    ///
    /// After a successful flush the disk run extends contiguously up to `oldest_retained - 1`, so the
    /// recoverable range is exactly disk `[disk_oldest, oldest_retained-1]` THEN memory
    /// `[oldest_retained, head]` -- the read path ([`Self::recover_ops_after`]) replays disk then
    /// hands off to memory with NO gap and NO duplicate.
    ///
    /// On a SPILL ERROR (I/O failure, or a contiguity refusal that should never happen given the
    /// in-order staging) the staged ops are dropped and `must_resync` is latched: the disk window
    /// could not be extended, so a replica that needed those ops full-re-syncs (the safe fallback,
    /// exactly today's behavior). NEVER serves a hole.
    pub fn flush_spill(&mut self) {
        if self.spill_stage.is_empty() {
            return;
        }
        let Some(disk) = self.disk.as_mut() else {
            // Disk disappeared (cannot happen once set, defensive): the staged ops are unrecoverable.
            self.spill_stage.clear();
            self.must_resync = true;
            return;
        };
        let batch: Vec<StreamOp> = self.spill_stage.drain(..).collect();
        if disk.spill(&batch).is_err() {
            // The disk window could not be extended (I/O error or an unexpected contiguity refusal):
            // latch a resume gap so a replica that needed those ops full-re-syncs (the safe fallback,
            // exactly today's behavior). The error is surfaced as the narrowed window, not logged
            // here (this low-level crate carries no logger; the safe degradation is the contract).
            self.must_resync = true;
        }
    }

    /// READ-ONLY recovery read for a resuming replica (HA-7e): copy up to `max` ops with offset
    /// strictly greater than `cursor`, in offset order, drawing from the DISK backlog FIRST (for the
    /// part of the range that has spilled out of memory) and then the in-memory window, with a
    /// GAP-FREE, DUPLICATE-FREE handoff at the boundary.
    ///
    /// The caller MUST [`Self::flush_spill`] before relying on this so the staging buffer is empty
    /// and the disk run is contiguous up to `oldest_retained - 1`. Then:
    /// - if `cursor + 1` is below the in-memory `oldest_retained`, disk ops are read first; the disk
    ///   read stops at `oldest_retained - 1` (its newest) and the in-memory read picks up exactly at
    ///   `oldest_retained` -- one unbroken sequence (the CONTINUITY CRUX, asserted in debug);
    /// - if `cursor + 1` is already within the in-memory window, this is exactly [`Self::ops_after`]
    ///   (disk contributes nothing), byte-identical to the non-disk path.
    ///
    /// A disk-side BACKLOG MISS (a torn / missing segment) yields a SHORT read: the returned ops are
    /// a gap-free prefix above `cursor` but may not reach the in-memory window. The caller's apply
    /// then sees the next needed offset still missing and full-re-syncs -- a corrupt segment is never
    /// served as data, and never bridged over a hole.
    #[must_use]
    pub fn recover_ops_after(&self, cursor: ReplOffset, max: usize) -> Vec<StreamOp> {
        if max == 0 {
            return Vec::new();
        }
        // No disk backlog (the DEFAULT): exactly the in-memory `ops_after`, byte-identical.
        let Some(disk) = &self.disk else {
            return self.ops_after(cursor, max);
        };
        let mem_oldest = self.oldest_retained();
        // If the next needed op is already in the in-memory window, the disk contributes nothing:
        // identical to ops_after (the default-path read).
        if cursor.next().0 >= mem_oldest.0 {
            return self.ops_after(cursor, max);
        }
        // Otherwise read from disk first (the part below the in-memory window).
        let mut out = disk.ops_after(cursor, max);
        // The disk read is a gap-free prefix above `cursor`. Continue into memory ONLY if the disk
        // read reached right up to the in-memory boundary (its last offset is mem_oldest - 1); a
        // short / torn disk read stops here, and the caller full-re-syncs on the resulting gap.
        let reached_boundary = out
            .last()
            .is_some_and(|op| op.offset().0 + 1 == mem_oldest.0);
        if reached_boundary && out.len() < max {
            let remaining = max - out.len();
            // Hand off to memory at exactly mem_oldest (no gap, no dup): read ops_after(mem_oldest-1).
            let from = ReplOffset(mem_oldest.0.saturating_sub(1));
            let mem = self.ops_after(from, remaining);
            // CONTINUITY ASSERT (the correctness crux): the first memory op must be exactly one past
            // the last disk op -- no gap, no overlap. Debug-only so release stays branch-light.
            debug_assert!(
                mem.first()
                    .is_none_or(|m| out.last().is_none_or(|d| m.offset().0 == d.offset().0 + 1)),
                "disk->memory handoff must be contiguous (no gap / dup)"
            );
            out.extend(mem);
        }
        out
    }

    /// Record the replica's ACK of `offset`: prune every retained op at or below it (the
    /// replica has them durably) and advance `acked` monotonically. A stale ack never lowers
    /// it. Called by the stream task when a `REPLCONF`/apply-ack arrives.
    pub fn ack(&mut self, offset: ReplOffset) {
        // Clamp the ack to `head`: a replica can only have applied ops the primary
        // actually produced. The sole in-tree ack source is the replica's own
        // applied offset (always <= head), so this is defensive against a buggy or
        // (future) untrusted peer whose over-ack would otherwise prune the entire
        // resume window and force needless full re-syncs.
        let offset = if offset.0 > self.head.0 {
            self.head
        } else {
            offset
        };
        self.acked = self.acked.max_with(offset);
        while self
            .ops
            .front()
            .is_some_and(|op| op.offset().0 <= self.acked.0)
        {
            self.ops.pop_front();
        }
        // HA-7e: prune whole disk segments fully below the ack too (the replica has them durably).
        // Partial segments stay so the disk run remains contiguous; their below-resume ops are
        // harmless (the recovery read's `cursor` filter skips them). NOTE: with multiple replicas a
        // single `acked` is the slowest-consumer bound the in-memory window already uses, so pruning
        // disk to it never drops an op a still-attached replica can resume from.
        if let Some(disk) = self.disk.as_mut() {
            disk.prune_through(self.acked);
        }
    }

    /// Take and CLEAR the `must_resync` latch (the stream task acknowledging it is doing a
    /// full re-sync). The caller re-bases the buffer afterwards with [`Self::rebase`]. Returns
    /// whether a resync was pending.
    pub fn take_resync(&mut self) -> bool {
        let pending = self.must_resync;
        self.must_resync = false;
        pending
    }

    /// RE-BASE the buffer at a fresh snapshot cut `cut` (the `end_offset` a new HA-7b
    /// full-sync just took): discard the entire retained window (it is below the cut and
    /// redundant) and advance `acked` to `cut`. `head` is UNCHANGED (writes that happened after
    /// the cut are already counted; the next produced op is `head + 1`, still gap-free above the
    /// cut). Ops produced between the cut and this call remain retained iff their offset is past
    /// `cut`. (Per-connection send cursors are owned by each connection and re-based by the
    /// caller to the cut after a fresh full-sync; the ring keeps no shared send cursor.)
    pub fn rebase(&mut self, cut: ReplOffset) {
        self.acked = self.acked.max_with(cut);
        while self.ops.front().is_some_and(|op| op.offset().0 <= cut.0) {
            self.ops.pop_front();
        }
        // HA-7e: discard the disk-spilled + staged ops at or below the cut too -- a fresh full-sync
        // re-bases every replica at `cut`, so anything below it is redundant. The staging buffer is
        // cleared of below-cut ops; the disk backlog drops whole below-cut segments (partials stay,
        // their stale ops harmless under the recovery read's cursor filter).
        while self
            .spill_stage
            .front()
            .is_some_and(|op| op.offset().0 <= cut.0)
        {
            self.spill_stage.pop_front();
        }
        if let Some(disk) = self.disk.as_mut() {
            disk.prune_through(cut);
        }
        self.must_resync = false;
    }
}

/// The primary-side write observer (HA-7c): the [`ironcache_store::WriteObserver`] the store
/// fires on every applied write, which assigns each write an offset and enqueues it onto the
/// shared [`ReplRing`].
///
/// Install it via [`ironcache_store::ShardStore::set_write_observer`]`(Box::new(observer))`.
/// It holds an `Rc` clone of the ring so the stream task (which holds the other clone) drains
/// what it enqueues. The store calls it `&mut self`, inline, single-threaded (ADR-0002), so
/// the enqueue is the O(1), non-blocking `ReplRing::push`.
#[derive(Debug)]
pub struct ReplObserver {
    ring: Rc<RefCell<ReplRing>>,
}

impl ReplObserver {
    /// A fresh observer feeding `ring`. The caller keeps its own `Rc` clone of `ring` for the
    /// stream task; this takes one for the boxed observer the store owns.
    #[must_use]
    pub fn new(ring: Rc<RefCell<ReplRing>>) -> Self {
        ReplObserver { ring }
    }

    /// A boxed observer ready for [`ironcache_store::ShardStore::set_write_observer`].
    #[must_use]
    pub fn boxed(ring: Rc<RefCell<ReplRing>>) -> Box<dyn WriteObserver> {
        Box::new(ReplObserver::new(ring))
    }
}

impl WriteObserver for ReplObserver {
    fn on_put(&mut self, db: u32, key: &[u8], new: &Entry) {
        // Reconstruct the committed post-image as an owned KvObj (the store's transfer type)
        // and encode it with the SAME HA-7b codec the full-sync uses, so the replica's
        // `insert_object` rebuilds the exact value/type/encoding/TTL. The encode happens
        // inline in the funnel; it is bounded by the value size (the store already paid that
        // memory) and never blocks.
        let kvobj_bytes = encode_kvobj(&new.to_kvobj());
        // Borrow the ring only for the O(1) push; the store funnel holds no other lock.
        self.ring.borrow_mut().push(StreamOp::Put {
            offset: ReplOffset::ZERO, // overwritten by push with the assigned offset
            db,
            key: key.to_vec(),
            kvobj_bytes,
        });
    }

    fn on_remove(&mut self, db: u32, key: &[u8]) {
        self.ring.borrow_mut().push(StreamOp::Del {
            offset: ReplOffset::ZERO, // overwritten by push with the assigned offset
            db,
            key: key.to_vec(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::{ExpireWrite, NewValue, Store, UnixMillis};
    use ironcache_store::ShardStore;

    const NOW: UnixMillis = UnixMillis(1_000);

    /// Installing the observer makes EVERY write enqueue a strictly-offset-tagged op, in the
    /// order the writes happened, with put/del classified correctly.
    #[test]
    fn observer_enqueues_offset_tagged_ops_in_order() {
        let ring = ReplRing::new(1024, ReplOffset::ZERO);
        let mut store: ShardStore = ShardStore::new(4);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));

        store.upsert(0, b"a", NewValue::Bytes(b"1"), ExpireWrite::Clear, NOW);
        store.upsert(0, b"b", NewValue::Bytes(b"2"), ExpireWrite::Clear, NOW);
        store.upsert(0, b"a", NewValue::Bytes(b"3"), ExpireWrite::Clear, NOW); // overwrite
        store.delete(0, b"b", NOW);

        let ops = ring.borrow().ops_after(ReplOffset::ZERO, usize::MAX);
        assert_eq!(ops.len(), 4, "four writes -> four ops");
        // Offsets are 1,2,3,4 in order (gap-free, strictly increasing).
        assert_eq!(ops[0].offset(), ReplOffset(1));
        assert_eq!(ops[1].offset(), ReplOffset(2));
        assert_eq!(ops[2].offset(), ReplOffset(3));
        assert_eq!(ops[3].offset(), ReplOffset(4));
        // Classification: three puts then a del.
        assert!(matches!(ops[0], StreamOp::Put { .. }));
        assert!(matches!(ops[2], StreamOp::Put { .. }));
        assert!(matches!(
            ops[3],
            StreamOp::Del { ref key, db: 0, .. } if key == b"b"
        ));
        // The head advanced to 4 and there is no resync pending.
        assert_eq!(ring.borrow().head(), ReplOffset(4));
        assert!(!ring.borrow().needs_resync());
    }

    /// A full retained window EVICTS the oldest un-acked op and latches `must_resync`, but the
    /// offset STILL advances (so the post-resync tail stays gap-free above the new cut). The
    /// funnel never blocks, and the window stays at `cap`.
    #[test]
    fn full_window_evicts_and_latches_resync_but_advances_offset() {
        let ring = ReplRing::new(2, ReplOffset::ZERO);
        let mut store: ShardStore = ShardStore::new(4);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));

        // Three writes into a cap-2 window: the third evicts offset 1.
        store.upsert(0, b"a", NewValue::Bytes(b"1"), ExpireWrite::Clear, NOW);
        store.upsert(0, b"b", NewValue::Bytes(b"2"), ExpireWrite::Clear, NOW);
        store.upsert(0, b"c", NewValue::Bytes(b"3"), ExpireWrite::Clear, NOW);

        let r = ring.borrow();
        assert!(r.needs_resync(), "overflow latched a resync");
        assert_eq!(r.len(), 2, "the retained window stays bounded at cap");
        assert_eq!(
            r.head(),
            ReplOffset(3),
            "the offset advanced for the evicted write too"
        );
        // The primary can no longer serve a replica resuming from offset 0 (op 1 was evicted).
        assert!(
            !r.can_serve_from(ReplOffset(0)),
            "the resume window has a gap"
        );
        // It can still serve a replica already at offset 1 (ops 2,3 retained).
        assert!(r.can_serve_from(ReplOffset(1)));
    }

    /// `take_resync` reports + clears only the latch; `rebase` discards the stale window at the
    /// fresh snapshot cut and advances `acked`/`send_cursor`.
    #[test]
    fn take_resync_and_rebase() {
        let ring = ReplRing::new(1, ReplOffset::ZERO);
        let mut store: ShardStore = ShardStore::new(4);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        store.upsert(0, b"a", NewValue::Bytes(b"1"), ExpireWrite::Clear, NOW);
        store.upsert(0, b"b", NewValue::Bytes(b"2"), ExpireWrite::Clear, NOW); // overflow

        assert!(ring.borrow().needs_resync());
        assert!(ring.borrow_mut().take_resync(), "resync was pending");
        assert!(!ring.borrow().needs_resync(), "latch cleared");
        // Rebase at the current head (the fresh snapshot cut): the stale window is discarded.
        let head = ring.borrow().head();
        ring.borrow_mut().rebase(head);
        assert!(
            ring.borrow().is_empty(),
            "stale window discarded at the cut"
        );
        assert_eq!(ring.borrow().acked(), head, "acked advanced to the cut");
        assert!(!ring.borrow_mut().take_resync(), "no longer pending");
    }

    /// Acking prunes the retained window: an acked-through op is dropped (the replica has it),
    /// freeing room so a later write does NOT overflow.
    #[test]
    fn ack_prunes_the_window() {
        let ring = ReplRing::new(2, ReplOffset::ZERO);
        let mut store: ShardStore = ShardStore::new(4);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        store.upsert(0, b"a", NewValue::Bytes(b"1"), ExpireWrite::Clear, NOW); // offset 1
        store.upsert(0, b"b", NewValue::Bytes(b"2"), ExpireWrite::Clear, NOW); // offset 2

        // The replica acks offset 1: it is pruned, leaving room for a third write.
        ring.borrow_mut().ack(ReplOffset(1));
        assert_eq!(ring.borrow().len(), 1, "the acked op was pruned");
        store.upsert(0, b"c", NewValue::Bytes(b"3"), ExpireWrite::Clear, NOW); // offset 3
        assert!(
            !ring.borrow().needs_resync(),
            "with the acked op pruned, the third write did not overflow"
        );
        assert_eq!(ring.borrow().len(), 2);
    }

    /// C1 (the per-connection fan-out): TWO independent local cursors reading `ops_after` the
    /// SAME ring EACH see EVERY retained op, with NO split between them. `ops_after` is read-only
    /// (it never advances a shared cursor), so consumer A reading every op does NOT consume them
    /// out from under consumer B -- the exact divergence the old shared `send_cursor` caused
    /// (each op went to whichever consumer drained it first). This is the unit proof of the C1
    /// fix; the live two-consumer wire proof is `raft_cluster.rs`.
    #[test]
    fn two_local_cursors_each_see_every_op_no_split() {
        let ring = ReplRing::new(1024, ReplOffset::ZERO);
        let mut store: ShardStore = ShardStore::new(4);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        store.upsert(0, b"a", NewValue::Bytes(b"1"), ExpireWrite::Clear, NOW);
        store.upsert(0, b"b", NewValue::Bytes(b"2"), ExpireWrite::Clear, NOW);
        store.upsert(0, b"c", NewValue::Bytes(b"3"), ExpireWrite::Clear, NOW);

        // Consumer A drains everything from its own cursor.
        let mut cursor_a = ReplOffset::ZERO;
        let a = ring.borrow().ops_after(cursor_a, usize::MAX);
        if let Some(last) = a.last() {
            cursor_a = last.offset();
        }
        // Consumer B, with its OWN cursor, STILL sees all three -- A's drain did not consume them.
        let mut cursor_b = ReplOffset::ZERO;
        let b = ring.borrow().ops_after(cursor_b, usize::MAX);
        if let Some(last) = b.last() {
            cursor_b = last.offset();
        }

        let a_offsets: Vec<_> = a.iter().map(StreamOp::offset).collect();
        let b_offsets: Vec<_> = b.iter().map(StreamOp::offset).collect();
        assert_eq!(
            a_offsets,
            vec![ReplOffset(1), ReplOffset(2), ReplOffset(3)],
            "consumer A sees every op"
        );
        assert_eq!(
            b_offsets,
            vec![ReplOffset(1), ReplOffset(2), ReplOffset(3)],
            "consumer B ALSO sees every op (no split with A)"
        );
        assert_eq!(cursor_a, ReplOffset(3));
        assert_eq!(cursor_b, ReplOffset(3));

        // A later write is seen by each, forward of its now-advanced cursor (still no split).
        store.upsert(0, b"d", NewValue::Bytes(b"4"), ExpireWrite::Clear, NOW);
        let a2 = ring.borrow().ops_after(cursor_a, usize::MAX);
        let b2 = ring.borrow().ops_after(cursor_b, usize::MAX);
        assert_eq!(
            a2.iter().map(StreamOp::offset).collect::<Vec<_>>(),
            vec![ReplOffset(4)]
        );
        assert_eq!(
            b2.iter().map(StreamOp::offset).collect::<Vec<_>>(),
            vec![ReplOffset(4)],
            "the new op fans out to BOTH cursors, not just whichever read first"
        );
    }

    /// The observer-off hot path is unchanged (HA-5a's gate): with no observer installed the
    /// store reports the fast-path flag false and nothing is enqueued.
    #[test]
    fn no_observer_is_inert() {
        let ring = ReplRing::new(8, ReplOffset::ZERO);
        let mut store: ShardStore = ShardStore::new(4);
        // Never install the observer.
        store.upsert(0, b"a", NewValue::Bytes(b"1"), ExpireWrite::Clear, NOW);
        assert!(!store.write_observer_active());
        assert!(ring.borrow().is_empty());
        assert_eq!(ring.borrow().head(), ReplOffset::ZERO);
    }

    // ===== HA-7e: disk-backed (spillable) backlog integration =====

    use crate::disk_backlog::DiskBacklog;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "icrepl-ring-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// DEFAULT-OFF is byte-identical: with NO disk backlog, an overflowing ring evicts + latches
    /// `must_resync` exactly as pre-HA-7e (the in-memory-only path), and `recover_ops_after` equals
    /// `ops_after`. Nothing is ever spilled.
    #[test]
    fn disk_off_is_byte_identical_to_in_memory_only() {
        let ring = ReplRing::new(2, ReplOffset::ZERO);
        let mut store: ShardStore = ShardStore::new(4);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        // cap-2; three writes overflow -> evict offset 1, latch resync (today's behavior).
        for (k, v) in [
            (b"a".as_slice(), b"1".as_slice()),
            (b"b", b"2"),
            (b"c", b"3"),
        ] {
            store.upsert(0, k, NewValue::Bytes(v), ExpireWrite::Clear, NOW);
        }
        let r = ring.borrow();
        assert!(!r.has_disk(), "no disk backlog by default");
        assert!(r.needs_resync(), "eviction latches resync (in-memory-only)");
        assert_eq!(
            r.oldest_recoverable(),
            ReplOffset(2),
            "floor is the in-memory oldest"
        );
        // recover_ops_after == ops_after when there is no disk.
        assert_eq!(
            r.recover_ops_after(ReplOffset::ZERO, usize::MAX),
            r.ops_after(ReplOffset::ZERO, usize::MAX)
        );
    }

    /// THE CRUX: a replica behind the IN-MEMORY ring but within the ON-DISK backlog catches up
    /// INCREMENTALLY (disk replay then in-memory handoff) with NO full-resync latch, NO gap, NO dup.
    #[test]
    fn behind_memory_within_disk_catches_up_incrementally_no_gap_no_dup() {
        let dir = temp_dir("incr");
        let disk = DiskBacklog::open(&dir, 1 << 20).expect("disk backlog enabled");
        // A small in-memory cap so the older ops spill to disk.
        let ring = ReplRing::with_disk(2, ReplOffset::ZERO, Some(disk));
        let mut store: ShardStore = ShardStore::new(4);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));

        // 6 writes: offsets 1..=6. With cap 2, offsets 1..=4 evict (spill to disk), 5..=6 stay in
        // memory. Flush after EACH write (the off-funnel flusher's real cadence is every drain pass),
        // so staging stays small and the spilled ops land on disk segment by segment.
        for i in 1..=6u8 {
            let k = [b'k', i];
            let v = [b'v', i];
            store.upsert(0, &k, NewValue::Bytes(&v), ExpireWrite::Clear, NOW);
            ring.borrow_mut().flush_spill();
        }

        let r = ring.borrow();
        // The eviction did NOT latch a resume gap (the ops went to disk, not dropped).
        assert!(
            !r.needs_resync(),
            "spilling to disk avoids the full-resync latch"
        );
        assert_eq!(
            r.oldest_retained(),
            ReplOffset(5),
            "memory holds the last 2 (5,6)"
        );
        assert_eq!(
            r.oldest_recoverable(),
            ReplOffset(1),
            "disk widens the floor to offset 1"
        );

        // A replica resuming from offset 0 (behind memory's oldest=5, within disk's oldest=1) CAN be
        // served, and the recovery read yields the WHOLE contiguous run 1..=6, gap-free, dup-free.
        assert!(r.can_serve_from(ReplOffset::ZERO));
        let ops = r.recover_ops_after(ReplOffset::ZERO, usize::MAX);
        let offsets: Vec<_> = ops.iter().map(|o| o.offset().0).collect();
        assert_eq!(
            offsets,
            vec![1, 2, 3, 4, 5, 6],
            "disk->memory handoff is one unbroken run"
        );
        // Each offset appears EXACTLY once (no dup at the boundary), and there is no hole.
        for w in offsets.windows(2) {
            assert_eq!(w[1], w[0] + 1, "strictly +1 contiguous: no gap, no overlap");
        }

        // A bounded read crossing the disk->memory boundary also hands off cleanly (read 5 ops: the
        // 4 disk + the first memory, no gap/dup at the seam).
        let five = r.recover_ops_after(ReplOffset::ZERO, 5);
        assert_eq!(
            five.iter().map(|o| o.offset().0).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5],
            "a bounded read hands off disk->memory with no gap/dup"
        );
        drop(r);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A replica behind even the ON-DISK range falls back to a full snapshot (today's behavior): the
    /// disk backlog is bounded, so once the oldest segments are evicted, a too-far-behind cursor
    /// `can_serve_from == false` -> the caller full-re-syncs.
    #[test]
    fn behind_disk_range_falls_back_to_full_snapshot() {
        let dir = temp_dir("over");
        // A tiny disk bound so the oldest spilled ops are evicted off disk.
        let disk = DiskBacklog::open(&dir, 48).expect("enabled");
        let ring = ReplRing::with_disk(1, ReplOffset::ZERO, Some(disk));
        let mut store: ShardStore = ShardStore::new(4);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        // Many writes: cap-1 memory spills almost everything; the tiny disk bound evicts the oldest
        // disk segments, so the disk floor rises well above offset 1.
        for i in 0..40u32 {
            let k = i.to_le_bytes();
            store.upsert(0, &k, NewValue::Bytes(b"x"), ExpireWrite::Clear, NOW);
            ring.borrow_mut().flush_spill();
        }
        let r = ring.borrow();
        let floor = r.oldest_recoverable();
        assert!(
            floor.0 > 1,
            "the disk bound evicted the oldest spilled ops (floor rose)"
        );
        // A replica at offset 0 needs op 1, which predates even the disk floor -> cannot serve.
        assert!(
            !r.can_serve_from(ReplOffset::ZERO),
            "behind even the disk range -> full-snapshot fallback (today's behavior)"
        );
        // A replica at the floor-1 CAN still be served incrementally (the wider window still works).
        assert!(r.can_serve_from(ReplOffset(floor.0 - 1)));
        drop(r);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A corrupt disk segment makes the recovery read SHORT (a gap-free prefix that does NOT reach
    /// the in-memory window), so the replica sees the next needed offset still missing and full-re
    /// -syncs -- a torn segment is never served as data nor bridged over a hole.
    #[test]
    fn corrupt_disk_segment_yields_short_read_forcing_resync() {
        let dir = temp_dir("torn");
        let disk = DiskBacklog::open(&dir, 1 << 20).expect("enabled");
        let ring = ReplRing::with_disk(1, ReplOffset::ZERO, Some(disk));
        let mut store: ShardStore = ShardStore::new(4);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        for i in 1..=6u8 {
            store.upsert(
                0,
                &[b'k', i],
                NewValue::Bytes(&[b'v', i]),
                ExpireWrite::Clear,
                NOW,
            );
            ring.borrow_mut().flush_spill();
        }
        // Corrupt the SECOND disk segment file (offsets in the middle of the spilled run).
        let backlog_dir = dir.join(DiskBacklog::DIR_NAME);
        let mut files: Vec<_> = std::fs::read_dir(&backlog_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "icrb"))
            .collect();
        files.sort();
        assert!(files.len() >= 2, "several spilled segments exist");
        let mut bytes = std::fs::read(&files[1]).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF; // flip a body byte; the CRC no longer matches.
        std::fs::write(&files[1], &bytes).unwrap();

        let r = ring.borrow();
        let ops = r.recover_ops_after(ReplOffset::ZERO, usize::MAX);
        let offsets: Vec<_> = ops.iter().map(|o| o.offset().0).collect();
        // The read stops at the corrupt segment: a gap-free prefix that does NOT reach the in-memory
        // window (offset 6), so the apply path's next-expected check fails -> full-resync.
        assert!(
            !offsets.contains(&6) || offsets.last() != Some(&6),
            "a corrupt segment is never bridged: the read is short of the in-memory tail"
        );
        // Whatever WAS returned is still gap-free above the cursor (never garbage / never a hole).
        for w in offsets.windows(2) {
            assert_eq!(
                w[1],
                w[0] + 1,
                "the served prefix is contiguous (no served corruption)"
            );
        }
        drop(r);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Acking through a spilled offset prunes whole disk segments below the ack (the replica has
    /// them durably), keeping the disk backlog from retaining ops no replica can still resume from.
    #[test]
    fn ack_prunes_disk_segments_below_it() {
        let dir = temp_dir("ackprune");
        let disk = DiskBacklog::open(&dir, 1 << 20).expect("enabled");
        let ring = ReplRing::with_disk(1, ReplOffset::ZERO, Some(disk));
        let mut store: ShardStore = ShardStore::new(4);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        for i in 1..=6u8 {
            store.upsert(
                0,
                &[b'k', i],
                NewValue::Bytes(&[b'v', i]),
                ExpireWrite::Clear,
                NOW,
            );
            ring.borrow_mut().flush_spill();
        }
        assert_eq!(ring.borrow().oldest_recoverable(), ReplOffset(1));
        // The replica acks through offset 4: whole disk segments at/below 4 are pruned.
        ring.borrow_mut().ack(ReplOffset(4));
        let floor = ring.borrow().oldest_recoverable();
        assert!(floor.0 > 4, "disk segments fully below the ack were pruned");
        std::fs::remove_dir_all(&dir).ok();
    }
}
