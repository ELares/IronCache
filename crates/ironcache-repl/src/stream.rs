// SPDX-License-Identifier: MIT OR Apache-2.0
//! The steady-state replication stream + apply (HA-7c): ship a primary's post-snapshot
//! writes in offset order and apply them on the replica, converging under load / partition.
//!
//! This is the tail that follows the HA-7b full-sync. After SYNCEND cuts the snapshot at
//! `end_offset`, the primary streams every later write (offset > `end_offset`) as a
//! [`crate::frames::Frame::StreamPut`] / `StreamDel`, and the replica applies them IN ORDER
//! and IDEMPOTENTLY so its keyspace converges to the primary's.
//!
//! Two halves, both transport-agnostic (async send / recv closures, like [`crate::fullsync`])
//! so the SAME logic runs over real TCP and the in-process convergence harness:
//!
//! - PRIMARY ([`drain_and_ship`]): drain a bounded batch off the observer's [`ReplRing`]
//!   under the ring borrow, RELEASE the borrow, then await the sends -- the collect-then
//!   -drain discipline the whole crate uses (no borrow held across an `.await`). If the ring
//!   has overflowed ([`ReplRing::needs_resync`]) it returns [`ShipOutcome::ResyncNeeded`] so
//!   the caller drops the replica to a fresh HA-7b full re-sync.
//! - REPLICA ([`ReplicaApplier`]): a small state machine over `applied_offset`. Each inbound
//!   stream frame is verified `offset == applied_offset.next()`; an in-order frame is applied
//!   (a put via [`ShardStore::insert_object`], a delete via [`Store::delete`]) and the offset
//!   advances; an OUT-OF-ORDER frame ([`ApplyOutcome::Gap`]) means the replica fell behind the
//!   primary's bounded buffer and must FULL RE-SYNC. Apply is idempotent: a re-delivered
//!   offset (<= `applied_offset`) is ignored, an overwrite replaces in place, a delete of an
//!   absent key is a no-op.
//!
//! ## MVP: full-resync-on-gap (correct, not optimal)
//!
//! On ANY gap (a missing offset, or the primary's ring overflowing) the replica discards and
//! re-loads the whole snapshot, then resumes the tail at the new cut. This always CONVERGES
//! (the convergence gate proves it over many seeds) but re-ships the keyspace on every gap.
//! A partial re-sync (back-fill only the missing range) and a disk-backed backlog spill (so
//! the primary never has to drop) are HA-7e / deferred.

use core::future::Future;

use ironcache_storage::{Store, UnixMillis};
use ironcache_store::ShardStore;

use crate::cursor::ReplOffset;
use crate::frames::Frame;
use crate::kvcodec::decode_kvobj;
use crate::observer::{ReplRing, StreamOp};

use std::cell::RefCell;
use std::rc::Rc;

/// The outcome of one [`drain_and_ship`] pass on the primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShipOutcome {
    /// `n` ops were shipped (possibly 0 if the ring was empty); the link is healthy.
    Shipped(usize),
    /// The observer ring OVERFLOWED: the tail has a gap the replica cannot fill, so the
    /// caller must drop this replica to a fresh HA-7b full re-sync. The resync latch has been
    /// taken/cleared (and the stale buffered tail discarded) by this call.
    ResyncNeeded,
    /// A frame send failed (the link dropped); the caller reconnects (and the replica resumes
    /// from its acked offset, or full-syncs if it fell too far behind).
    LinkDown,
}

/// Convert a [`StreamOp`] into its wire [`Frame`] (the primary's ship encoding).
fn op_to_frame(op: StreamOp) -> Frame {
    match op {
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
    }
}

/// Drain up to `max` not-yet-shipped ops off `ring` and ship them in offset order through the
/// async frame `send` sink (PRIMARY, HA-7c).
///
/// THE BORROW DISCIPLINE: the ring borrow is taken ONLY to check the resync latch and to copy
/// a bounded batch of ops out (an O(max) read forward of the send cursor), then DROPPED before
/// any `.await`. The sends happen after the borrow ends, so the store funnel (which shares the
/// ring) is never blocked behind a network await.
///
/// Returns [`ShipOutcome::ResyncNeeded`] if the buffer overflowed (an un-acked op was evicted
/// from the resume window); the CALLER then performs a fresh HA-7b full-sync and calls
/// [`ReplRing::rebase`]. Returns [`ShipOutcome::LinkDown`] if a send fails, else
/// [`ShipOutcome::Shipped`]`(n)`. The shipped ops stay RETAINED (for a possible re-send) until
/// the replica acks them via [`ReplRing::ack`].
pub async fn drain_and_ship<S, Fut>(
    ring: &Rc<RefCell<ReplRing>>,
    max: usize,
    mut send: S,
) -> ShipOutcome
where
    S: FnMut(Frame) -> Fut,
    Fut: Future<Output = Result<(), ()>>,
{
    // --- Borrow the ring: check the gap latch, else copy a bounded batch out. RELEASE. ---
    let batch: Vec<StreamOp> = {
        let mut r = ring.borrow_mut();
        if r.needs_resync() {
            // A resume-window gap: signal the caller to full-resync. The latch + the buffer
            // re-base are left to the caller's resync path (it does the full-sync, then
            // `rebase`s the buffer at the new cut), so this call ships nothing and does not
            // mutate the buffer.
            return ShipOutcome::ResyncNeeded;
        }
        r.drain_batch(max)
    }; // the ring borrow ends here, before any await below.

    // --- Await the sends; the ring borrow is already dropped. ---
    let n = batch.len();
    for op in batch {
        if send(op_to_frame(op)).await.is_err() {
            return ShipOutcome::LinkDown;
        }
    }
    ShipOutcome::Shipped(n)
}

/// The outcome of applying one inbound stream frame on the replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The frame was the next expected offset and was applied; `applied_offset` advanced.
    Applied(ReplOffset),
    /// The frame was a DUPLICATE / stale re-delivery (offset <= `applied_offset`); ignored
    /// idempotently. `applied_offset` is unchanged.
    Duplicate,
    /// A GAP: the frame's offset is beyond `applied_offset.next()` (a missing offset, the
    /// replica fell behind the primary's bounded buffer). The caller must trigger a FULL
    /// RE-SYNC (HA-7b): discard, re-load the snapshot, resume the tail at the new cut. The
    /// applier does NOT apply the frame.
    Gap,
}

/// The replica-side steady-state apply state machine (HA-7c): tracks `applied_offset` and
/// applies inbound [`Frame::StreamPut`] / `StreamDel` to the live store IN ORDER and
/// IDEMPOTENTLY.
///
/// It owns no clock and no socket (the transport feeds it decoded frames); the only external
/// it needs is `now` for the store's lazy-expiry probe on a delete. Construct it at the
/// `end_offset` the HA-7b full-sync returned; the first tail frame must carry
/// `end_offset.next()`.
#[derive(Debug, Clone)]
pub struct ReplicaApplier {
    /// The highest offset durably applied (the replica's resume point + ack). Monotonic.
    applied: ReplOffset,
}

impl ReplicaApplier {
    /// A fresh applier resuming the tail from `start` (the HA-7b `end_offset`, i.e. the
    /// snapshot cut). The next in-order frame is `start.next()`.
    #[must_use]
    pub fn new(start: ReplOffset) -> Self {
        ReplicaApplier { applied: start }
    }

    /// The highest offset applied (the resume point a reconnect's `REPLCONF` advertises).
    #[must_use]
    pub fn applied(&self) -> ReplOffset {
        self.applied
    }

    /// Apply one inbound stream frame to `store`, returning the [`ApplyOutcome`].
    ///
    /// - A NON-stream frame (a stray heartbeat) is treated as a no-op [`ApplyOutcome::Applied`]
    ///   of the current offset (nothing to apply, no gap) -- the recv loop simply continues.
    /// - A frame whose offset is exactly `applied.next()` is applied (put -> `insert_object`,
    ///   del -> `Store::delete`) and `applied` advances. A put with an undecodable payload is
    ///   treated as a GAP (the stream is corrupt; full-resync is the safe recovery), never a
    ///   silent skip.
    /// - A frame whose offset is <= `applied` is a duplicate ([`ApplyOutcome::Duplicate`]):
    ///   ignored idempotently (re-applying it would still be correct, but skipping avoids the
    ///   redundant work and is the natural at-least-once handling).
    /// - A frame whose offset is > `applied.next()` is a [`ApplyOutcome::Gap`]: NOT applied;
    ///   the caller full-resyncs.
    pub fn apply<E, A>(
        &mut self,
        store: &mut ShardStore<E, A>,
        frame: Frame,
        now: UnixMillis,
    ) -> ApplyOutcome
    where
        E: ironcache_storage::EvictionHook,
        A: ironcache_storage::AccountingHook,
    {
        let (offset, is_put, db, key, kvobj_bytes) = match frame {
            Frame::StreamPut {
                offset,
                db,
                key,
                kvobj_bytes,
            } => (offset, true, db, key, kvobj_bytes),
            Frame::StreamDel { offset, db, key } => (offset, false, db, key, Vec::new()),
            // A non-stream frame on the tail (a stray heartbeat); nothing to apply, no gap.
            Frame::ReplConf { .. }
            | Frame::ReplPing { .. }
            | Frame::FullSync { .. }
            | Frame::SyncKv { .. }
            | Frame::SyncEnd { .. } => return ApplyOutcome::Applied(self.applied),
        };

        let expected = self.applied.next();
        if offset == expected {
            if is_put {
                let Some(obj) = decode_kvobj(&kvobj_bytes) else {
                    // A corrupt post-image: do not apply, do not advance. Full-resync is the
                    // safe recovery (a silent skip would leave a permanent divergence).
                    return ApplyOutcome::Gap;
                };
                store.insert_object(db, obj);
            } else {
                // Idempotent delete: a delete of an absent key is a harmless no-op.
                store.delete(db, &key, now);
            }
            self.applied = expected;
            ApplyOutcome::Applied(self.applied)
        } else if offset.0 <= self.applied.0 {
            // A stale re-delivery (<= what we already applied): ignore idempotently.
            ApplyOutcome::Duplicate
        } else {
            // offset > expected: a hole in the sequence. Full-resync.
            ApplyOutcome::Gap
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::{ExpireWrite, NewValue, Store, UnixMillis};
    use ironcache_store::ShardStore;

    use crate::observer::{ReplObserver, ReplRing};

    const NOW: UnixMillis = UnixMillis(1_000);

    /// A primary write -> the observer enqueues -> drain_and_ship ships the frame -> the
    /// replica applier applies it to its store, converging that one key.
    #[test]
    fn ship_then_apply_converges_one_key() {
        let ring = ReplRing::new(64, ReplOffset::ZERO);
        let mut primary: ShardStore = ShardStore::new(4);
        primary.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));

        primary.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);

        // Ship synchronously through a Vec sink (the in-memory channel is always Ready).
        let shipped: Rc<RefCell<Vec<Frame>>> = Rc::new(RefCell::new(Vec::new()));
        let sink = Rc::clone(&shipped);
        let out = block_on(drain_and_ship(&ring, 16, move |f| {
            let s = Rc::clone(&sink);
            async move {
                s.borrow_mut().push(f);
                Ok(())
            }
        }));
        assert_eq!(out, ShipOutcome::Shipped(1));

        // Apply on the replica from the cut at offset 0.
        let mut replica: ShardStore = ShardStore::new(4);
        let mut applier = ReplicaApplier::new(ReplOffset::ZERO);
        for f in shipped.borrow().iter().cloned() {
            assert_eq!(
                applier.apply(&mut replica, f, NOW),
                ApplyOutcome::Applied(ReplOffset(1))
            );
        }
        assert_eq!(applier.applied(), ReplOffset(1));
        assert_eq!(replica.read(0, b"k", NOW).unwrap().as_bytes(), b"v");
    }

    /// Out-of-order and duplicate frames are classified without applying: a future offset is
    /// a Gap (full-resync), a stale one is a Duplicate (ignored).
    #[test]
    fn gap_and_duplicate_are_classified() {
        let mut replica: ShardStore = ShardStore::new(4);
        let mut applier = ReplicaApplier::new(ReplOffset(5));

        // The next expected is 6; a frame at 8 is a gap.
        let gap = Frame::StreamPut {
            offset: ReplOffset(8),
            db: 0,
            key: b"x".to_vec(),
            kvobj_bytes: Vec::new(),
        };
        assert_eq!(applier.apply(&mut replica, gap, NOW), ApplyOutcome::Gap);
        assert_eq!(applier.applied(), ReplOffset(5), "a gap does not advance");

        // A frame at 3 (already applied) is a duplicate.
        let dup = Frame::StreamDel {
            offset: ReplOffset(3),
            db: 0,
            key: b"x".to_vec(),
        };
        assert_eq!(
            applier.apply(&mut replica, dup, NOW),
            ApplyOutcome::Duplicate
        );
        assert_eq!(applier.applied(), ReplOffset(5));
    }

    /// A corrupt put payload at the next offset is a Gap (full-resync), never a silent skip.
    #[test]
    fn corrupt_put_is_a_gap() {
        let mut replica: ShardStore = ShardStore::new(4);
        let mut applier = ReplicaApplier::new(ReplOffset(0));
        let bad = Frame::StreamPut {
            offset: ReplOffset(1),
            db: 0,
            key: b"k".to_vec(),
            kvobj_bytes: vec![0xFF, 0xFF], // not a valid kvobj encoding
        };
        assert_eq!(applier.apply(&mut replica, bad, NOW), ApplyOutcome::Gap);
        assert_eq!(applier.applied(), ReplOffset(0));
    }

    /// An overflowed ring makes drain_and_ship report ResyncNeeded; the latch is LEFT for the
    /// caller's resync path (which full-syncs then `rebase`s), and rebasing clears it.
    #[test]
    fn overflow_reports_resync_needed() {
        let ring = ReplRing::new(1, ReplOffset::ZERO);
        let mut primary: ShardStore = ShardStore::new(4);
        primary.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        primary.upsert(0, b"a", NewValue::Bytes(b"1"), ExpireWrite::Clear, NOW);
        primary.upsert(0, b"b", NewValue::Bytes(b"2"), ExpireWrite::Clear, NOW); // overflow

        let out = block_on(drain_and_ship(&ring, 16, move |_f| async move { Ok(()) }));
        assert_eq!(out, ShipOutcome::ResyncNeeded);
        assert!(
            ring.borrow().needs_resync(),
            "the latch stays set until the caller rebases after a full-sync"
        );
        // The resync path: re-base at the primary's current head (the snapshot cut).
        let head = ring.borrow().head();
        ring.borrow_mut().rebase(head);
        assert!(!ring.borrow().needs_resync(), "rebase clears the latch");
    }

    /// A send failure mid-ship reports LinkDown.
    #[test]
    fn send_failure_reports_link_down() {
        let ring = ReplRing::new(64, ReplOffset::ZERO);
        let mut primary: ShardStore = ShardStore::new(4);
        primary.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        primary.upsert(0, b"a", NewValue::Bytes(b"1"), ExpireWrite::Clear, NOW);

        let out = block_on(drain_and_ship(&ring, 16, move |_f| async move { Err(()) }));
        assert_eq!(out, ShipOutcome::LinkDown);
    }

    /// Run a future to completion on the stable no-op waker (the in-memory sinks never pend).
    fn block_on<F: Future>(fut: F) -> F::Output {
        use core::task::{Context, Poll, Waker};
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = core::pin::pin!(fut);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => v,
            Poll::Pending => panic!("test future pended; the in-memory sink is sync"),
        }
    }
}
