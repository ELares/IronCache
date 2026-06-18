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
    /// Latched when an UN-ACKED op was evicted (the retained window overflowed `cap`). Once
    /// set, the primary has lost part of the resume window, so the replica that needs it must
    /// drop to a fresh HA-7b full re-sync. Cleared by [`Self::take_resync`].
    must_resync: bool,
}

impl ReplRing {
    /// A fresh, empty buffer bounded at `cap` (clamped to at least 1 so progress is possible),
    /// starting at offset `start` (the primary's offset at install time; `ReplOffset::ZERO`
    /// for a fresh primary). Wrapped in an `Rc<RefCell<..>>` for sharing between the observer
    /// the store owns and the stream task that drains it (same shard core, no cross-core
    /// lock; ADR-0002).
    #[must_use]
    pub fn new(cap: usize, start: ReplOffset) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(ReplRing {
            ops: VecDeque::new(),
            cap: cap.max(1),
            head: start,
            acked: start,
            must_resync: false,
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
    /// the next op it needs (`from + 1`) is still retained (or there is nothing past `from`).
    /// False once the buffer has evicted the op at `from + 1` (a resume gap -> full-resync).
    #[must_use]
    pub fn can_serve_from(&self, from: ReplOffset) -> bool {
        if from.0 >= self.head.0 {
            return true; // caught up: nothing to serve
        }
        // The first op the replica still needs is from+1; it must be >= the oldest retained.
        from.next().0 >= self.oldest_retained().0
    }

    /// Assign `op` the NEXT offset and retain it. Advances `head` unconditionally (the write
    /// happened). If the retained window is at `cap`, EVICT the oldest retained op and latch
    /// `must_resync` (the primary lost part of the resume window). NEVER blocks. The op's
    /// offset field is overwritten with the freshly-assigned offset.
    ///
    /// Returns the offset assigned.
    fn push(&mut self, mut op: StreamOp) -> ReplOffset {
        let assigned = self.head.next();
        self.head = assigned;
        match &mut op {
            StreamOp::Put { offset, .. } | StreamOp::Del { offset, .. } => *offset = assigned,
        }
        if self.ops.len() >= self.cap {
            // The retained window is full: evict the oldest un-acked op to make room and latch
            // the resume gap. The evicted op is below where any consumer could now resume from;
            // a per-connection cursor below it will fall-behind-resync (see `can_serve_from`).
            self.ops.pop_front();
            self.must_resync = true;
        }
        self.ops.push_back(op);
        assigned
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
}
