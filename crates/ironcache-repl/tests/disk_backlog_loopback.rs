// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-7e disk-backed-backlog loopback acceptance test: a replica disconnects, the primary's writes
//! have overflowed the SMALL in-memory backlog ring (so the missed tail is SPILLED to the disk
//! backlog), the replica reconnects, and it CATCHES UP INCREMENTALLY FROM DISK -- no second full
//! snapshot, no gap, no duplicate at the disk->memory handoff -- converging key-for-key with the
//! primary.
//!
//! This is the wire proof of the HA-7e widening: the SAME `drain_and_ship` / `ReplicaApplier`
//! transport the in-memory tail uses, now resuming from a window that overflowed memory but lives on
//! disk. It mirrors the HA-7c stream-loopback test (`tests/stream_loopback.rs`) shape: both ends run
//! on one current-thread tokio runtime + `LocalSet` (the shared-nothing, `!Send`, thread-per-core
//! shape, ADR-0002), through the `ironcache_runtime::Runtime` seam.
//!
//! ## The scenario that forces the disk path (and would have full-resynced before HA-7e)
//!
//! The in-memory ring `cap` is TINY (8) and the disk backlog is generous. The primary produces its
//! WHOLE keyspace + tail UP FRONT (flushing the spill periodically, the off-funnel flusher's real
//! cadence), so the bulk of the tail is EVICTED from the cap-8 memory ring and lives ON DISK. The
//! replica full-syncs at the snapshot cut, applies a short prefix, then KILLs the link. On reconnect
//! its applied offset is BELOW the in-memory ring's oldest retained but WITHIN the disk range, so
//! before HA-7e the primary would `ResyncNeeded` + full-resync; WITH the disk backlog the primary
//! serves the missed range from disk then hands off to the live in-memory tail. The test asserts the
//! reconnect received NO `FullSync` frame (it resumed incrementally) and converged key-for-key.

use core::cell::RefCell;
use core::time::Duration;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;

use ironcache_repl::observer::{ReplObserver, ReplRing};
use ironcache_repl::{
    ApplyOutcome, DiskBacklog, Frame, FrameError, ReplId, ReplOffset, ReplicaApplier, ShipOutcome,
    drain_and_ship, drive_full_sync, encode_kvobj, receive_full_sync,
};
use ironcache_runtime::Runtime;
use ironcache_runtime::TokioRuntime;
use ironcache_runtime::tokio_rt::bind_reuseport;
use ironcache_storage::{ExpireWrite, NewValue, Store, UnixMillis};
use ironcache_store::{ShardStore, SnapshotCursor};

const NOW: UnixMillis = UnixMillis(1_000);
const DBS: u32 = 4;
/// A TINY in-memory ring so the missed tail overflows memory and spills to disk.
const RING_CAP: usize = 8;
/// How many tail writes to produce (well past the cap, so the bulk spills to disk).
const TAIL_WRITES: u32 = 140;

type Stream = <TokioRuntime as Runtime>::Stream;
type SharedStream = Rc<RefCell<Option<Stream>>>;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "icrepl-disk-loopback-{tag}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
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

async fn send_frame(rt: &TokioRuntime, stream: &SharedStream, frame: Frame) -> Result<(), ()> {
    let bytes = frame.encode();
    let mut s = stream.borrow_mut().take().expect("stream present");
    let res = rt.send(&mut s, bytes).await;
    *stream.borrow_mut() = Some(s);
    res.map(|_| ()).map_err(|_| ())
}

/// Build the primary: seed a keyspace (the snapshot), install the observer, then produce the whole
/// tail UP FRONT, flushing the spill periodically so the bulk of the tail spills out of the cap-8
/// memory ring onto disk. Returns `(primary, ring, snapshot_cut, head)`.
fn build_primary(
    dir: &std::path::Path,
) -> (ShardStore, Rc<RefCell<ReplRing>>, ReplOffset, ReplOffset) {
    let disk = DiskBacklog::open(dir, 8 << 20).expect("disk backlog enabled");
    let ring = ReplRing::with_disk(RING_CAP, ReplOffset::ZERO, Some(disk));
    let mut primary = ShardStore::new(DBS);
    // Seed BEFORE installing the observer, so the seeds form the snapshot but do not flood the tiny
    // tail ring (snapshot_cut starts at 0).
    for i in 0..20u32 {
        primary.upsert(
            i % DBS,
            format!("seed-{i:03}").as_bytes(),
            NewValue::Bytes(format!("s{i}").as_bytes()),
            ExpireWrite::Clear,
            NOW,
        );
    }
    primary.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
    let snapshot_cut = ring.borrow().head(); // == 0.
    // The whole tail, flushing the spill periodically (the off-funnel flusher's real cadence) so the
    // staging buffer stays bounded and the overflowed ops spill onto disk segment by segment.
    for i in 0..TAIL_WRITES {
        primary.upsert(
            i % DBS,
            format!("tail-{i:04}").as_bytes(),
            NewValue::Bytes(format!("t{i}").as_bytes()),
            ExpireWrite::Clear,
            NOW,
        );
        if i % 4 == 0 {
            ring.borrow_mut().flush_spill();
        }
    }
    ring.borrow_mut().flush_spill();
    let head = ring.borrow().head();
    (primary, ring, snapshot_cut, head)
}

#[test]
fn replica_resumes_incrementally_from_disk_backlog_no_full_resync() {
    let dir = temp_dir("incr");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let dir2 = dir.clone();
    local.block_on(&rt, async move {
        let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        let listener = bind_reuseport(addr).unwrap();

        let (primary, ring, snapshot_cut, head) = build_primary(&dir2);
        // The disk backlog must actually hold the bulk of the tail (its oldest recoverable offset is
        // BELOW the in-memory ring's oldest retained), else the scenario does not exercise disk.
        assert!(
            ring.borrow().oldest_recoverable().0 < ring.borrow().oldest_retained().0,
            "the disk backlog widened the recoverable window below the in-memory ring"
        );
        let expected = fingerprint(&primary);

        // --- The PRIMARY server task: own the primary store + ring; serve TWO connections. ---
        let server = tokio::task::spawn_local(async move {
            let rt = TokioRuntime::new();
            for _conn in 0..2u32 {
                let Ok((stream, _peer)) = rt.accept(&listener).await else {
                    return;
                };
                serve_connection(&rt, stream, &primary, &ring, snapshot_cut).await;
            }
        });

        // --- The REPLICA task: session 0 full-syncs + applies a prefix then KILLs the link; session
        //     1 reconnects and MUST resume incrementally (NO FullSync frame) and converge. ---
        let replica_result = tokio::task::spawn_local(async move {
            let rt = TokioRuntime::new();
            let mut replica = ShardStore::new(DBS);
            let mut applier = ReplicaApplier::new(ReplOffset::ZERO);
            let mut session1_saw_fullsync = false;
            for session in 0..2u32 {
                let saw_fs =
                    run_replica_session(&rt, addr, session, head, &mut replica, &mut applier).await;
                if session == 1 {
                    session1_saw_fullsync = saw_fs;
                }
            }
            (
                fingerprint(&replica),
                applier.applied(),
                session1_saw_fullsync,
            )
        });

        let (got, applied, session1_fullsync) = replica_result.await.expect("replica joined");
        let _ = server.await;

        assert!(
            !session1_fullsync,
            "the reconnect resumed INCREMENTALLY from the disk backlog (no second full snapshot)"
        );
        assert_eq!(
            applied, head,
            "the replica applied through the primary's head (no lag)"
        );
        assert_eq!(
            got, expected,
            "the replica converged key-for-key via the disk->memory incremental resume"
        );
    });
    std::fs::remove_dir_all(&dir).ok();
}

/// Serve ONE accepted replica connection on the primary side (HA-7e disk-aware): read the attach
/// REPLCONF; if the replica can resume (its ack > 0 and within the RECOVERABLE window = memory OR
/// disk, no resync latched) rewind this connection's send cursor to the ack and resume the tail
/// (drawing from disk then memory); else full-sync at `snapshot_cut`. Then ship every not-yet-sent
/// op up to the head until the link drops. `primary`/`ring` are borrowed (no RefCell on the store),
/// so no borrow crosses an await.
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

    let Some(Frame::ReplConf {
        ack: resume_from, ..
    }) = next_frame(rt, &stream, &pending, &queue).await
    else {
        return;
    };

    // A fresh replica sends ack 0 (no store) -> full-sync. A reconnect sends its real applied offset
    // (> 0, it kept its store) -> resume incrementally IF within the recoverable (memory OR disk)
    // window and no resync is latched. flush_spill first so the disk run is complete.
    ring.borrow_mut().flush_spill();
    let can_resume = resume_from.0 > 0
        && ring.borrow().can_serve_from(resume_from)
        && !ring.borrow().needs_resync();

    let mut send_cursor;
    if can_resume {
        send_cursor = resume_from;
    } else {
        let fs_stream = Rc::clone(&stream);
        let replid = ReplId::from_bytes([0x7e; 20]);
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

/// Run ONE replica session; returns whether a `FullSync` frame was seen (so the test can assert the
/// RECONNECT resumed incrementally, NOT via a full snapshot). Session 0 receives the full-sync into
/// a fresh store, applies a short prefix, then KILLs the link. Session 1 reconnects, applies until
/// caught up to `head` or EOF, and MUST NOT see a FullSync. A gap is a test failure (the disk window
/// must bridge the missed range with no hole / dup).
async fn run_replica_session(
    rt: &TokioRuntime,
    addr: SocketAddr,
    session: u32,
    head: ReplOffset,
    replica: &mut ShardStore,
    applier: &mut ReplicaApplier,
) -> bool {
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

    let mut saw_fullsync = false;
    let mut applied_this_session = 0u32;
    loop {
        let Some(frame) = next_frame(rt, &stream, &pending, &queue).await else {
            break; // EOF / link dropped
        };
        if matches!(frame, Frame::FullSync { .. }) {
            saw_fullsync = true; // a FullSync on the reconnect = a full re-sync (HA-7e avoids this).
        }
        match applier.apply(replica, frame, NOW) {
            ApplyOutcome::Applied(_) => applied_this_session += 1,
            ApplyOutcome::Duplicate => {}
            ApplyOutcome::Gap => {
                panic!("unexpected gap (session {session}): disk window must bridge")
            }
        }
        let _ = send_frame(
            rt,
            &stream,
            Frame::ReplConf {
                node: 1,
                ack: applier.applied(),
            },
        )
        .await;

        if session == 0 && applied_this_session >= 5 {
            drop(stream.borrow_mut().take()); // KILL the link after a short prefix.
            break;
        }
        if session == 1 && applier.applied().0 >= head.0 {
            break; // caught up to the primary head via the disk + memory resume.
        }
    }
    saw_fullsync
}
