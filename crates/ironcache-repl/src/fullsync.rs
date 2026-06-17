// SPDX-License-Identifier: MIT OR Apache-2.0
//! Full-sync-on-attach (HA-7b): stream a primary's HA-5b snapshot to a fresh replica.
//!
//! When a replica attaches needing a full re-sync (a CHANGED replid, or a resume offset the
//! primary can no longer serve from its tail), the primary ships its ENTIRE keyspace as a
//! one-shot snapshot, and the replica loads it into a brand-new store. This module is the
//! transport-agnostic core of that exchange, split into the two halves:
//!
//! - [`drive_full_sync`] (PRIMARY): capture `end_offset` = the primary's CURRENT
//!   [`ReplOffset`] (the snapshot cut, where the HA-7c tail resumes), send
//!   [`Frame::FullSync`], then drive [`ShardStore::snapshot_chunk`] from
//!   [`SnapshotCursor::START`] to done in BOUNDED chunks -- encoding each `(db, key, KvObj)`
//!   as a [`Frame::SyncKv`] and sending it -- then send [`Frame::SyncEnd`]. Constant memory:
//!   a bounded chunk is pulled under the store borrow, the borrow is RELEASED, and only then
//!   are the frames awaited out (the collect-then-drain discipline the rest of the crate
//!   uses, so a send never holds the store borrow across an await).
//! - [`receive_full_sync`] (REPLICA): on [`Frame::FullSync`], create a FRESH temp
//!   [`ShardStore`] and apply each [`Frame::SyncKv`] via
//!   [`ShardStore::insert_object`]`(db, `[`decode_kvobj`]`(...))`; on [`Frame::SyncEnd`]
//!   complete the sync, returning the fully-loaded store + `end_offset` so the caller begins
//!   tailing at `end_offset` (HA-7c). On a MID-SYNC disconnect, an I/O error, or a malformed
//!   entry, the partial temp store is DISCARDED entirely (dropped) and an error is returned
//!   so the caller retries -- a half-loaded store is NEVER returned.
//!
//! ## Scope (7b, not 7d)
//!
//! This loads the snapshot into a temp [`ShardStore`] and returns it. The ATOMIC SWAP of
//! that temp store into the live serve path (so reads start hitting the synced data) is
//! HA-7d; here the caller receives the loaded store and the resume offset and does nothing
//! live with it yet.
//!
//! ## Why closures, not the `Runtime` directly
//!
//! Both halves take the I/O as async closures (a frame SINK on the primary, a frame SOURCE
//! on the replica) rather than a socket, so the SAME logic runs (a) over the real repl link
//! (the transport adapter passes send/recv closures) and (b) in-process under a test that
//! pumps frames through a channel, with no socket. It mirrors how [`crate::link`] keeps the
//! decision logic separable from [`crate::transport`].

use core::future::Future;

use ironcache_storage::UnixMillis;
use ironcache_store::{ShardStore, SnapshotCursor};

use crate::cursor::{ReplId, ReplOffset};
use crate::frames::Frame;
use crate::kvcodec::{decode_kvobj, encode_kvobj};

/// A full sync failed and must be RETRIED (HA-7b). The replica returns this on a mid-sync
/// disconnect, an I/O error, or a malformed [`Frame::SyncKv`] entry, having DISCARDED the
/// partial temp store; the primary returns it when a frame send fails (the link dropped).
/// Deliberately opaque (one cause: "the sync did not complete, start over") rather than a
/// stringly error -- the caller's response is the same for every case: tear down and retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FullSyncError;

impl core::fmt::Display for FullSyncError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("full sync did not complete; retry")
    }
}

impl std::error::Error for FullSyncError {}

/// Drive a full sync of `store` to a replica through the async frame `send` sink (PRIMARY,
/// HA-7b).
///
/// Captures `end_offset` (passed by the caller, the primary's current [`ReplOffset`] at the
/// snapshot cut -- the HA-7c resume point) and `replid`, sends [`Frame::FullSync`], streams
/// the whole snapshot as [`Frame::SyncKv`] frames in chunks of at most `chunk_max` entries,
/// then sends [`Frame::SyncEnd`]`{ end_offset }`. `now` is the caller's clock (ADR-0003: the
/// store reads none; a lazily-expired key is skipped by [`ShardStore::snapshot_chunk`]).
///
/// CONSTANT MEMORY + the borrow discipline: each iteration takes the `&store` borrow, pulls
/// ONE bounded chunk (at most `chunk_max` entries), ENCODES each entry into an owned
/// [`Frame::SyncKv`] while still borrowing, then DROPS the borrow before awaiting the sends.
/// So no store borrow is ever held across an `.await`, and peak memory is one chunk, never
/// the whole keyspace.
///
/// `send` returns `Err(())` when the link is gone; the driver stops and returns
/// [`FullSyncError`] (the caller retries / the replica reconnects).
///
/// # Errors
/// Returns [`FullSyncError`] if any frame send fails (the replica link dropped mid-sync).
pub async fn drive_full_sync<E, A, S, Fut>(
    store: &ShardStore<E, A>,
    replid: ReplId,
    end_offset: ReplOffset,
    now: UnixMillis,
    chunk_max: usize,
    mut send: S,
) -> Result<(), FullSyncError>
where
    E: ironcache_storage::EvictionHook,
    A: ironcache_storage::AccountingHook,
    S: FnMut(Frame) -> Fut,
    Fut: Future<Output = Result<(), ()>>,
{
    // Announce the full sync: the replid names the stream, end_offset names the cut.
    send(Frame::FullSync { replid, end_offset })
        .await
        .map_err(|()| FullSyncError)?;

    let databases = store.databases();
    let mut cursor = SnapshotCursor::START;
    while !cursor.is_done(databases) {
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
        }; // the `&store` borrow ends here, before any await below.

        // --- Now await the sends; the store borrow is already dropped. ---
        for frame in frames {
            send(frame).await.map_err(|()| FullSyncError)?;
        }
    }

    // Terminate the stream with the cut offset (repeated so SYNCEND is self-contained).
    send(Frame::SyncEnd { end_offset })
        .await
        .map_err(|()| FullSyncError)?;
    Ok(())
}

/// The outcome of a completed full sync (HA-7b): the freshly-loaded temp store and the
/// `end_offset` the replica resumes its HA-7c tail from. The atomic swap of `store` into the
/// live serve path is HA-7d; here the caller just holds it.
#[derive(Debug)]
pub struct LoadedSnapshot<E: ironcache_storage::EvictionHook, A: ironcache_storage::AccountingHook>
{
    /// The fully-loaded temp store (a FRESH store, never a half-loaded one).
    pub store: ShardStore<E, A>,
    /// The snapshot cut offset; the replica begins tailing here (HA-7c).
    pub end_offset: ReplOffset,
    /// The primary's replid this snapshot belongs to (from the FULLSYNC frame).
    pub replid: ReplId,
}

/// Receive a full sync into a FRESH store through the async frame `recv` source (REPLICA,
/// HA-7b).
///
/// `make_store` builds the fresh temp store (the caller supplies the database count / hooks);
/// it is called ONCE, on the [`Frame::FullSync`] that begins the sync. Each [`Frame::SyncKv`]
/// is decoded via [`decode_kvobj`] and applied via [`ShardStore::insert_object`]; the
/// terminating [`Frame::SyncEnd`] completes the sync and returns the loaded store + the
/// `end_offset` (preferring the SYNCEND value, which equals the FULLSYNC one).
///
/// `recv` yields `Some(frame)` for each inbound frame and `None` on a disconnect / clean EOF.
/// A `None` BEFORE [`Frame::SyncEnd`], or a [`Frame::SyncKv`] whose payload fails to decode,
/// is a MID-SYNC failure: the partial temp store is DISCARDED (dropped on the error return)
/// and [`FullSyncError`] is returned so the caller retries. A half-loaded store is never
/// returned. Any non-full-sync frame (e.g. a stray REPLPING) before SYNCEND is ignored.
///
/// # Errors
/// Returns [`FullSyncError`] on a mid-sync disconnect (`recv` yields `None` before SYNCEND),
/// a malformed [`Frame::SyncKv`] payload, or a [`Frame::SyncKv`]/[`Frame::SyncEnd`] arriving
/// before the [`Frame::FullSync`] that begins the sync.
pub async fn receive_full_sync<E, A, M, R, Fut>(
    mut make_store: M,
    mut recv: R,
) -> Result<LoadedSnapshot<E, A>, FullSyncError>
where
    E: ironcache_storage::EvictionHook,
    A: ironcache_storage::AccountingHook,
    M: FnMut() -> ShardStore<E, A>,
    R: FnMut() -> Fut,
    Fut: Future<Output = Option<Frame>>,
{
    // The in-progress temp store + the stream's replid, established by FULLSYNC. `None` until
    // the begin frame arrives; on ANY error path we simply return, dropping this Option and
    // thus the partial store (never exposing a half-loaded store).
    let mut pending: Option<(ShardStore<E, A>, ReplId)> = None;

    loop {
        let Some(frame) = recv().await else {
            // A disconnect / EOF. If it lands BEFORE SYNCEND, the sync is incomplete: the
            // partial store (if any) is dropped here. Retry.
            return Err(FullSyncError);
        };
        match frame {
            Frame::FullSync { replid, .. } => {
                // Begin (or RESTART) the sync: a fresh store, discarding any earlier partial.
                pending = Some((make_store(), replid));
            }
            Frame::SyncKv {
                db, kvobj_bytes, ..
            } => {
                let Some((store, _)) = pending.as_mut() else {
                    // A data frame before FULLSYNC: protocol violation, retry.
                    return Err(FullSyncError);
                };
                let Some(obj) = decode_kvobj(&kvobj_bytes) else {
                    // A malformed entry: discard the partial store (dropped on return), retry.
                    return Err(FullSyncError);
                };
                store.insert_object(db, obj);
            }
            Frame::SyncEnd { end_offset } => {
                let Some((store, replid)) = pending.take() else {
                    // SYNCEND before FULLSYNC: protocol violation, retry.
                    return Err(FullSyncError);
                };
                return Ok(LoadedSnapshot {
                    store,
                    end_offset,
                    replid,
                });
            }
            // Any other frame (a stray heartbeat, a replica-side frame) is not part of the
            // full-sync stream; ignore it and keep loading.
            Frame::ReplConf { .. } | Frame::ReplPing { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use ironcache_storage::{
        ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store,
    };
    use ironcache_store::SnapshotCursor as Cur;

    const NOW: UnixMillis = UnixMillis(1_000);
    const DBS: u32 = 4;

    fn store() -> ShardStore {
        ShardStore::new(DBS)
    }

    fn put(s: &mut ShardStore, db: u32, key: &[u8], val: &[u8]) {
        s.upsert(db, key, NewValue::Bytes(val), ExpireWrite::Clear, NOW);
    }

    fn put_ttl(s: &mut ShardStore, db: u32, key: &[u8], val: &[u8], deadline: u64) {
        s.upsert(
            db,
            key,
            NewValue::Bytes(val),
            ExpireWrite::Set(UnixMillis(deadline)),
            NOW,
        );
    }

    fn put_set(s: &mut ShardStore, db: u32, key: &[u8], members: &[&[u8]]) {
        let members: Vec<Vec<u8>> = members.iter().map(|m| m.to_vec()).collect();
        s.rmw_mut(db, key, NOW, move |entry| match entry {
            RmwEntry::Vacant => RmwStep {
                action: RmwAction::Insert(NewValueOwned::set(members)),
                expire: ExpireWrite::Keep,
                reply: (),
            },
            _ => RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Keep,
                reply: (),
            },
        });
    }

    fn put_hash(s: &mut ShardStore, db: u32, key: &[u8], pairs: &[(&[u8], &[u8])]) {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = pairs
            .iter()
            .map(|(f, v)| (f.to_vec(), v.to_vec()))
            .collect();
        s.rmw_mut(db, key, NOW, move |entry| match entry {
            RmwEntry::Vacant => RmwStep {
                action: RmwAction::Insert(NewValueOwned::hash(pairs)),
                expire: ExpireWrite::Keep,
                reply: (),
            },
            _ => RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Keep,
                reply: (),
            },
        });
    }

    fn put_list(s: &mut ShardStore, db: u32, key: &[u8], elems: &[&[u8]]) {
        let elems: Vec<Vec<u8>> = elems.iter().map(|e| e.to_vec()).collect();
        s.rmw_mut(db, key, NOW, move |entry| match entry {
            RmwEntry::Vacant => RmwStep {
                action: RmwAction::Insert(NewValueOwned::list(elems)),
                expire: ExpireWrite::Keep,
                reply: (),
            },
            _ => RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Keep,
                reply: (),
            },
        });
    }

    fn put_zset(s: &mut ShardStore, db: u32, key: &[u8], pairs: &[(&[u8], f64)]) {
        let pairs: Vec<(Vec<u8>, f64)> = pairs.iter().map(|(m, sc)| (m.to_vec(), *sc)).collect();
        s.rmw_mut(db, key, NOW, move |entry| match entry {
            RmwEntry::Vacant => RmwStep {
                action: RmwAction::Insert(NewValueOwned::zset(pairs)),
                expire: ExpireWrite::Keep,
                reply: (),
            },
            _ => RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Keep,
                reply: (),
            },
        });
    }

    /// Populate a primary with a mix of dbs, types, encodings, and TTLs.
    fn populate(s: &mut ShardStore) {
        put(s, 0, b"str-a", b"alpha");
        put(s, 0, b"str-int", b"4242"); // int-encoded
        put_ttl(s, 1, b"str-ttl", b"withttl", 9_999);
        put_list(s, 1, b"list", &[b"x", b"y", b"z"]);
        put_set(s, 2, b"set-int", &[b"1", b"2", b"3"]); // intset
        put_set(s, 2, b"set-str", &[b"red", b"green"]); // listpack
        put_hash(s, 3, b"hash", &[(b"f1", b"v1"), (b"f2", b"v2")]);
        put_zset(s, 3, b"zset", &[(b"m1", 1.0), (b"m2", 2.5)]);
        // A large set so the hashtable encoding is exercised end to end.
        let big: Vec<Vec<u8>> = (0..200u32)
            .map(|i| format!("e{i:04}").into_bytes())
            .collect();
        let big_refs: Vec<&[u8]> = big.iter().map(std::vec::Vec::as_slice).collect();
        put_set(s, 0, b"set-big", &big_refs);
    }

    /// Drain `s`'s entire snapshot into a sorted, comparable `(db, key, kvcodec-bytes)` form.
    /// The kvcodec bytes carry type + encoding + TTL + value, so two stores are equal iff
    /// their fingerprints are equal -- a faithful keyspace comparison without naming the
    /// private Val internals.
    fn fingerprint(s: &ShardStore) -> Vec<(u32, Vec<u8>, Vec<u8>)> {
        let databases = DBS as usize;
        let mut cursor = Cur::START;
        let mut out = Vec::new();
        let mut guard = 0;
        while !cursor.is_done(databases) {
            let (chunk, next) = s.snapshot_chunk(cursor, 64, NOW);
            for (db, key, kv) in chunk {
                out.push((db, key.into_vec(), encode_kvobj(&kv)));
            }
            cursor = next;
            guard += 1;
            assert!(guard < 100_000, "drain terminates");
        }
        out.sort();
        out
    }

    /// A test frame channel: the driver pushes frames in, the receiver pops them out. A plain
    /// in-process queue (no socket), so the full-sync logic is exercised deterministically.
    /// The shared frame queue (an `Rc<RefCell<..>>` for the single-thread test idiom).
    type SharedQueue = Rc<RefCell<VecDeque<Frame>>>;

    #[derive(Clone, Default)]
    struct Channel {
        q: SharedQueue,
        /// If set, the source returns `None` (disconnect) after this many frames are popped.
        cut_after: Rc<RefCell<Option<usize>>>,
        popped: Rc<RefCell<usize>>,
    }

    impl Channel {
        fn new() -> Self {
            Channel::default()
        }

        fn cut_after(n: usize) -> Self {
            let c = Channel::new();
            *c.cut_after.borrow_mut() = Some(n);
            c
        }

        fn push(&self, f: Frame) {
            self.q.borrow_mut().push_back(f);
        }

        fn pop(&self) -> Option<Frame> {
            if let Some(limit) = *self.cut_after.borrow() {
                if *self.popped.borrow() >= limit {
                    return None; // simulate a mid-sync disconnect
                }
            }
            let f = self.q.borrow_mut().pop_front();
            if f.is_some() {
                *self.popped.borrow_mut() += 1;
            }
            f
        }
    }

    /// Run a future to completion on a trivial executor (the test futures never pend: the
    /// channel is in-memory, so every await is Ready immediately). Uses the stable no-op
    /// [`core::task::Waker::noop`] so the crate's `#![forbid(unsafe_code)]` is honored.
    fn block_on<F: Future>(fut: F) -> F::Output {
        use core::task::{Context, Poll, Waker};
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = core::pin::pin!(fut);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => v,
            Poll::Pending => panic!("test future pended; the in-memory channel is sync"),
        }
    }

    #[test]
    fn full_sync_transfers_the_keyspace() {
        let mut primary = store();
        populate(&mut primary);
        let replid = ReplId::from_bytes([0xAB; 20]);
        let end_offset = ReplOffset(777);

        let chan = Channel::new();

        // Drive the full sync into the channel (constant-memory chunked, chunk_max = 3).
        let driver_chan = chan.clone();
        let drive = drive_full_sync(&primary, replid, end_offset, NOW, 3, move |f| {
            let c = driver_chan.clone();
            async move {
                c.push(f);
                Ok(())
            }
        });
        block_on(drive).expect("the driver completes");

        // Receive it into a fresh replica.
        let recv_chan = chan.clone();
        let loaded = block_on(receive_full_sync(
            || ShardStore::new(DBS),
            move || {
                let c = recv_chan.clone();
                async move { c.pop() }
            },
        ))
        .expect("the receiver completes");

        // The end_offset (resume point) is the primary's offset at the cut.
        assert_eq!(loaded.end_offset, end_offset);
        assert_eq!(loaded.replid, replid);

        // The replica's keyspace EQUALS the primary's: every key, value, type, encoding, TTL.
        assert_eq!(
            fingerprint(&loaded.store),
            fingerprint(&primary),
            "the replica's store matches the primary key-for-key"
        );

        // Spot-check a few reads through the store's read API directly.
        let mut rep = loaded.store;
        assert_eq!(rep.read(0, b"str-a", NOW).unwrap().as_bytes(), b"alpha");
        assert_eq!(rep.read(0, b"str-int", NOW).unwrap().as_bytes(), b"4242");
        assert_eq!(
            rep.read(0, b"str-int", NOW).unwrap().encoding(),
            primary.read(0, b"str-int", NOW).unwrap().encoding(),
            "int encoding round-trips"
        );
        assert_eq!(
            rep.read(1, b"str-ttl", NOW).unwrap().expire_at(),
            Some(UnixMillis(9_999)),
            "TTL round-trips"
        );
    }

    #[test]
    fn empty_keyspace_full_sync_is_just_begin_end() {
        let primary = store(); // empty
        let replid = ReplId::from_bytes([0x11; 20]);
        let chan = Channel::new();

        let dc = chan.clone();
        block_on(drive_full_sync(
            &primary,
            replid,
            ReplOffset(0),
            NOW,
            8,
            move |f| {
                let c = dc.clone();
                async move {
                    c.push(f);
                    Ok(())
                }
            },
        ))
        .expect("drive completes");

        // Exactly FULLSYNC then SYNCEND (no SYNCKV for an empty keyspace).
        assert_eq!(chan.q.borrow().len(), 2);

        let rc = chan.clone();
        let loaded = block_on(receive_full_sync(
            || ShardStore::new(DBS),
            move || {
                let c = rc.clone();
                async move { c.pop() }
            },
        ))
        .expect("receive completes");
        assert!(fingerprint(&loaded.store).is_empty(), "empty replica");
    }

    #[test]
    fn mid_sync_disconnect_discards_partial() {
        let mut primary = store();
        populate(&mut primary);
        let replid = ReplId::from_bytes([0x22; 20]);

        // Drive the WHOLE sync into the channel first (so the queue holds every frame), but
        // the source is CUT to disconnect partway (after a few frames), before SYNCEND.
        let chan = Channel::cut_after(3); // FULLSYNC + 2 SYNCKV, then None
        let dc = chan.clone();
        block_on(drive_full_sync(
            &primary,
            replid,
            ReplOffset(5),
            NOW,
            8,
            move |f| {
                let c = dc.clone();
                async move {
                    c.push(f);
                    Ok(())
                }
            },
        ))
        .expect("drive enqueues every frame");
        // There ARE more than 3 frames enqueued (so the cut is genuinely mid-stream).
        assert!(
            chan.q.borrow().len() > 3,
            "the populated sync has more than the 3 frames we let through"
        );

        let rc = chan.clone();
        let result = block_on(receive_full_sync(
            || ShardStore::new(DBS),
            move || {
                let c = rc.clone();
                async move { c.pop() }
            },
        ));
        // The receiver signals retry; the partial temp store was dropped (never returned).
        assert_eq!(
            result.err(),
            Some(FullSyncError),
            "a mid-sync disconnect discards the partial store and signals retry"
        );
    }

    #[test]
    fn malformed_synckv_discards_partial() {
        // A FULLSYNC, a garbage SYNCKV (undecodable bytes), then SYNCEND. The receiver must
        // reject at the bad entry, dropping the partial store, NOT return a half-loaded one.
        let replid = ReplId::from_bytes([0x33; 20]);
        let chan = Channel::new();
        chan.push(Frame::FullSync {
            replid,
            end_offset: ReplOffset(1),
        });
        chan.push(Frame::SyncKv {
            db: 0,
            key: b"k".to_vec(),
            kvobj_bytes: vec![0xFF, 0xFF, 0xFF], // not a valid kvobj encoding
        });
        chan.push(Frame::SyncEnd {
            end_offset: ReplOffset(1),
        });

        let rc = chan.clone();
        let result = block_on(receive_full_sync(
            || ShardStore::new(DBS),
            move || {
                let c = rc.clone();
                async move { c.pop() }
            },
        ));
        assert_eq!(
            result.err(),
            Some(FullSyncError),
            "a malformed SYNCKV entry aborts the sync with a retry signal"
        );
    }

    #[test]
    fn driver_send_failure_is_reported() {
        // If the link drops (a send fails) the driver returns FullSyncError.
        let mut primary = store();
        populate(&mut primary);
        let replid = ReplId::from_bytes([0x44; 20]);
        let sent = Rc::new(RefCell::new(0usize));
        let sent2 = Rc::clone(&sent);
        let result = block_on(drive_full_sync(
            &primary,
            replid,
            ReplOffset(0),
            NOW,
            8,
            move |_f| {
                let s = Rc::clone(&sent2);
                async move {
                    let mut n = s.borrow_mut();
                    *n += 1;
                    // Fail the third send (mid-stream).
                    if *n >= 3 { Err(()) } else { Ok(()) }
                }
            },
        ));
        assert_eq!(result.err(), Some(FullSyncError));
        assert!(*sent.borrow() >= 3, "the driver stopped at the failed send");
    }
}
