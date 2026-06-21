// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-7c steady-state-stream loopback acceptance test: a primary ships its full-sync then its
//! tail of writes to a replica over real loopback TCP; the replica converges key-for-key. The
//! link is then KILLED mid-tail and the replica RECONNECTS, resuming from its acked offset (the
//! bounded ring still holds the resume window), and STILL converges, with no gap / dup.
//!
//! Mirrors the HA-7b full-sync loopback shape (`tests/fullsync_loopback.rs`): both ends run on
//! a single current-thread tokio runtime + `LocalSet` (the shared-nothing, `!Send`,
//! thread-per-core shape, ADR-0002), driven through the [`ironcache_runtime::Runtime`] seam.
//!
//! ## The single-duplex-stream discipline
//!
//! Each end owns its stream sequentially (the `Runtime` seam's `recv`/`send` take `&mut
//! Stream`, no duplex split). To avoid a read/write stall on one socket, the PRIMARY produces
//! its whole write load BEFORE it streams (so the tail is a fixed, finite sequence), then ships
//! frames without reading mid-stream; the REPLICA reads + applies frames and sends its
//! REPLCONF acks, which the primary reads only at the next connection's attach. The ring cap is
//! generous, so the resume window survives the kill -- the reconnect RESUMES rather than
//! full-resyncs, which is exactly the property under test.

use core::cell::RefCell;
use core::time::Duration;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;

use ironcache_repl::observer::{ReplObserver, ReplRing};
use ironcache_repl::{
    ApplyOutcome, Frame, FrameError, ReplId, ReplOffset, ReplicaApplier, ShipOutcome,
    drain_and_ship, drive_full_sync, encode_kvobj, receive_full_sync,
};
use ironcache_runtime::Runtime;
use ironcache_runtime::TokioRuntime;
use ironcache_runtime::tokio_rt::bind_reuseport;
use ironcache_storage::{ExpireWrite, NewValue, Store, UnixMillis};
use ironcache_store::{ShardStore, SnapshotCursor};

const NOW: UnixMillis = UnixMillis(1_000);
const DBS: u32 = 4;

type Stream = <TokioRuntime as Runtime>::Stream;
type SharedStream = Rc<RefCell<Option<Stream>>>;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn fingerprint(s: &ShardStore) -> Vec<(u32, Vec<u8>, Vec<u8>)> {
    let mut cursor = SnapshotCursor::START;
    let mut out = Vec::new();
    let mut guard = 0;
    while !cursor.is_done(DBS as usize) {
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

/// A persistent frame source over `stream`: a byte buffer + a ready-frame queue, reading more
/// as needed. `None` on EOF / peer drop / malformed. Stream TAKEN out around recv (no borrow
/// across await).
async fn next_frame(
    rt: &TokioRuntime,
    stream: &SharedStream,
    pending: &Rc<RefCell<Vec<u8>>>,
    queue: &Rc<RefCell<VecDeque<Frame>>>,
) -> Option<Frame> {
    loop {
        if let Some(f) = queue.borrow_mut().pop_front() {
            return Some(f);
        }
        let mut drained_any = false;
        loop {
            let decoded = Frame::decode(&pending.borrow());
            match decoded {
                Ok(Some((frame, consumed))) => {
                    pending.borrow_mut().drain(..consumed);
                    queue.borrow_mut().push_back(frame);
                    drained_any = true;
                }
                Ok(None) => break,
                Err(FrameError) => return None,
            }
        }
        if drained_any {
            continue;
        }
        let taken: Vec<u8> = core::mem::take(&mut *pending.borrow_mut());
        let mut s = stream.borrow_mut().take().expect("stream present");
        let res = rt.recv(&mut s, taken).await;
        *stream.borrow_mut() = Some(s);
        match res {
            Ok(r) => {
                if r.n == 0 {
                    return None;
                }
                *pending.borrow_mut() = r.buf;
            }
            Err(_) => return None,
        }
    }
}

/// Send one frame over `stream` (TAKE/put-back; no borrow across await). `Err(())` if gone.
async fn send_frame(rt: &TokioRuntime, stream: &SharedStream, frame: Frame) -> Result<(), ()> {
    let bytes = frame.encode();
    let mut s = stream.borrow_mut().take().expect("stream present");
    let res = rt.send(&mut s, bytes).await;
    *stream.borrow_mut() = Some(s);
    res.map(|_| ()).map_err(|_| ())
}

#[test]
fn stream_tail_over_loopback_converges_and_resumes_after_reconnect() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        let listener = bind_reuseport(addr).unwrap();

        // The primary store + observer ring on this single core. A generous cap so the resume
        // window survives the mid-tail kill (so the reconnect RESUMES, not full-resyncs).
        let ring = ReplRing::new(4096, ReplOffset::ZERO);
        let mut primary = ShardStore::new(DBS);
        primary.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));

        // Seed an initial keyspace (so the attach does a real full-sync), then produce the
        // WHOLE tail of writes up front (so the streamed tail is a fixed, finite sequence and
        // the primary never needs to read mid-stream). ~50 + ~150 = 200 writes total.
        for i in 0..50u32 {
            primary.upsert(
                i % DBS,
                format!("seed-{i:03}").as_bytes(),
                NewValue::Bytes(format!("s{i}").as_bytes()),
                ExpireWrite::Clear,
                NOW,
            );
        }
        let snapshot_cut = ring.borrow().head(); // the full-sync cut: writes above are the tail
        for i in 0..150u32 {
            primary.upsert(
                i % DBS,
                format!("tail-{i:04}").as_bytes(),
                NewValue::Bytes(format!("t{i}").as_bytes()),
                ExpireWrite::Clear,
                NOW,
            );
        }
        let head = ring.borrow().head();
        let expected = fingerprint(&primary);

        // --- The PRIMARY server task: OWN the primary store + ring (no shared RefCell on the
        //     store, so no borrow crosses an await) and serve TWO connections; see
        //     `serve_connection`. ---
        let server = tokio::task::spawn_local(async move {
            let rt = TokioRuntime::new();
            for _conn in 0..2u32 {
                let Ok((stream, _peer)) = rt.accept(&listener).await else {
                    return;
                };
                serve_connection(&rt, stream, &primary, &ring, snapshot_cut).await;
                // Connection done (shipped everything or the replica killed it). Accept the
                // next. The replica's acks for this connection are read at the NEXT attach.
            }
        });

        // --- The REPLICA task: two sessions; session 0 full-syncs + applies a prefix then KILLs
        //     the link; session 1 reconnects, resumes, and converges (see `run_replica_session`).
        let replica_result = tokio::task::spawn_local(async move {
            let rt = TokioRuntime::new();
            let mut replica = ShardStore::new(DBS);
            let mut applier = ReplicaApplier::new(ReplOffset::ZERO);
            for session in 0..2u32 {
                run_replica_session(&rt, addr, session, head, &mut replica, &mut applier).await;
            }
            (fingerprint(&replica), applier.applied())
        });

        let (got, applied) = replica_result.await.expect("replica task joined");
        let _ = server.await;

        assert_eq!(
            applied, head,
            "the replica applied through the primary's head (converged, no lag)"
        );
        assert_eq!(
            got, expected,
            "the replica converged to the primary key-for-key across the reconnect"
        );
    });
}

/// Serve ONE accepted replica connection on the primary side: read the attach REPLCONF; if the
/// replica is fresh (or the primary cannot serve its resume offset) full-sync at `snapshot_cut`
/// and tail from there, else rewind the send cursor to the replica's ack and resume the tail;
/// then ship every not-yet-sent op up to the head until the link drops. `primary`/`ring` are
/// borrowed (NO `RefCell` on the store), so no borrow crosses an await.
async fn serve_connection(
    rt: &TokioRuntime,
    stream: <TokioRuntime as Runtime>::Stream,
    primary: &ShardStore,
    ring: &Rc<RefCell<ReplRing>>,
    snapshot_cut: ReplOffset,
) {
    let stream: SharedStream = Rc::new(RefCell::new(Some(stream)));
    let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
    let queue = Rc::new(RefCell::new(VecDeque::<Frame>::new()));

    // The attach REPLCONF names the resume offset.
    let Some(Frame::ReplConf {
        ack: resume_from, ..
    }) = next_frame(rt, &stream, &pending, &queue).await
    else {
        return;
    };

    let can_resume = resume_from.0 >= snapshot_cut.0
        && ring.borrow().can_serve_from(resume_from)
        && !ring.borrow().needs_resync();

    // C1: THIS connection's OWN send cursor (the ring keeps no shared one). It starts at the
    // replica's ack on a resume, or at the snapshot cut after a fresh full-sync.
    let mut send_cursor;
    if can_resume {
        // Resume the tail from the replica's ack: this connection's cursor starts there.
        send_cursor = resume_from;
    } else {
        // Full-sync at the cut, then tail from the cut.
        let fs_stream = Rc::clone(&stream);
        let replid = ReplId::from_bytes([0x7c; 20]);
        let ok = drive_full_sync(primary, replid, snapshot_cut, NOW, 8, move |frame| {
            let st = Rc::clone(&fs_stream);
            let rt3 = TokioRuntime::new();
            async move { send_frame(&rt3, &st, frame).await }
        })
        .await;
        if ok.is_err() {
            return;
        }
        send_cursor = snapshot_cut;
    }

    // Ship every not-yet-sent op up to the head, until the link drops or there is nothing left.
    loop {
        let ship_stream = Rc::clone(&stream);
        let outcome = drain_and_ship(ring, &mut send_cursor, 16, move |frame| {
            let st = Rc::clone(&ship_stream);
            let rt3 = TokioRuntime::new();
            async move { send_frame(&rt3, &st, frame).await }
        })
        .await;
        match outcome {
            ShipOutcome::Shipped(0) | ShipOutcome::LinkDown | ShipOutcome::ResyncNeeded => break,
            ShipOutcome::Shipped(_) => {}
        }
    }
}

/// Run ONE replica session: dial; attach with the resume offset; on session 0 receive the
/// full-sync into a fresh store; then apply the tail, acking each op. On session 0 KILL the
/// link after >= 30 applied tail ops (so the reconnect resumes mid-stream); on session 1 apply
/// until caught up to `head` or EOF. A gap is unexpected in this scenario (the resume window is
/// intact) and panics.
async fn run_replica_session(
    rt: &TokioRuntime,
    addr: SocketAddr,
    session: u32,
    head: ReplOffset,
    replica: &mut ShardStore,
    applier: &mut ReplicaApplier,
) {
    let stream: SharedStream = Rc::new(RefCell::new(Some(loop {
        match rt.connect(addr).await {
            Ok(s) => break s,
            Err(_) => rt.timer(Duration::from_millis(5)).await,
        }
    })));
    let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
    let queue = Rc::new(RefCell::new(VecDeque::<Frame>::new()));

    send_frame(
        rt,
        &stream,
        Frame::ReplConf {
            node: 1,
            ack: applier.applied(),
            resume_token: None,
        },
    )
    .await
    .expect("attach send");

    if session == 0 {
        let fs_stream = Rc::clone(&stream);
        let fs_pending = Rc::clone(&pending);
        let fs_queue = Rc::clone(&queue);
        let loaded = receive_full_sync(
            || ShardStore::new(DBS),
            move || {
                let rt3 = TokioRuntime::new();
                let st = Rc::clone(&fs_stream);
                let pd = Rc::clone(&fs_pending);
                let q = Rc::clone(&fs_queue);
                async move { next_frame(&rt3, &st, &pd, &q).await }
            },
        )
        .await
        .expect("full sync completes on session 0");
        *replica = loaded.store;
        *applier = ReplicaApplier::new(loaded.end_offset);
    }

    let mut applied_this_session = 0u32;
    loop {
        let Some(frame) = next_frame(rt, &stream, &pending, &queue).await else {
            break; // EOF / link dropped
        };
        match applier.apply(replica, frame, NOW) {
            ApplyOutcome::Applied(_) => applied_this_session += 1,
            ApplyOutcome::Duplicate => {}
            ApplyOutcome::Gap => panic!("unexpected gap in the tail (session {session})"),
        }
        let _ = send_frame(
            rt,
            &stream,
            Frame::ReplConf {
                node: 1,
                ack: applier.applied(),
                resume_token: None,
            },
        )
        .await;

        if session == 0 && applied_this_session >= 30 {
            drop(stream.borrow_mut().take()); // KILL the link mid-tail (the primary sees EOF)
            break;
        }
        if session == 1 && applier.applied().0 >= head.0 {
            break; // caught up to the primary head
        }
    }
}
