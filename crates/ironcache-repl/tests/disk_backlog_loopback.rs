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
        // ONE per-boot history token for the SAME primary across BOTH connections (the primary never
        // restarts in this happy-path test): the session-1 reconnect re-advertises the token it got on
        // session 0, it matches, so the gate RESUMES incrementally.
        let history_token = ReplId::from_bytes([0x7e; 20]);

        // --- The PRIMARY server task: own the primary store + ring; serve TWO connections. ---
        let server = tokio::task::spawn_local(async move {
            let rt = TokioRuntime::new();
            for _conn in 0..2u32 {
                let Ok((stream, _peer)) = rt.accept(&listener).await else {
                    return;
                };
                serve_connection(&rt, stream, &primary, &ring, snapshot_cut, history_token).await;
            }
        });

        // --- The REPLICA task: session 0 full-syncs + applies a prefix then KILLs the link; session
        //     1 reconnects and MUST resume incrementally (NO FullSync frame) and converge. ---
        let replica_result = tokio::task::spawn_local(async move {
            let rt = TokioRuntime::new();
            let mut replica = ShardStore::new(DBS);
            let mut applier = ReplicaApplier::new(ReplOffset::ZERO);
            let mut remembered_token: Option<ReplId> = None;
            let mut session1_saw_fullsync = false;
            for session in 0..2u32 {
                let saw_fs = run_replica_session(
                    &rt,
                    addr,
                    session,
                    head,
                    &mut replica,
                    &mut applier,
                    &mut remembered_token,
                )
                .await;
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

/// Serve ONE accepted replica connection on the primary side (HA-7e disk-aware), mirroring the live
/// `serve_replica_conn` resume gate: read the attach REPLCONF; RESUME (rewind this connection's send
/// cursor to the ack, draw the tail from disk then memory) ONLY when ALL of:
///   * the replica's remembered HISTORY TOKEN exactly matches this primary's per-boot `history_token`
///     (the silent-divergence fence: a restarted primary mints a new token, so a stale replica
///     mismatches and full-syncs);
///   * its ack is genuinely SERVEABLE (`ack > 0`, within the recoverable memory-OR-disk window, no
///     resync latched) AND NOT AHEAD of head (`ack <= head` -- a replica claiming more than the
///     primary has = the primary lost/reset history -> full-sync, never "caught up, ship nothing").
///
/// Otherwise full-sync at `snapshot_cut` (advertising `history_token` as the `FullSync.replid` the
/// replica then remembers). Then ship every not-yet-sent op up to the head until the link drops.
/// `primary`/`ring` are borrowed (no RefCell on the store), so no borrow crosses an await.
async fn serve_connection(
    rt: &TokioRuntime,
    stream: <TokioRuntime as Runtime>::Stream,
    primary: &ShardStore,
    ring: &Rc<RefCell<ReplRing>>,
    snapshot_cut: ReplOffset,
    history_token: ReplId,
) {
    let stream: SharedStream = Rc::new(RefCell::new(Some(stream)));
    let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
    let queue = Rc::new(RefCell::new(VecDeque::<Frame>::new()));

    let Some(Frame::ReplConf {
        ack: resume_from,
        resume_token,
        ..
    }) = next_frame(rt, &stream, &pending, &queue).await
    else {
        return;
    };

    // A fresh replica sends ack 0 + no token (no store) -> full-sync. A reconnect sends its real
    // applied offset (> 0) AND the token it last synced under -> resume incrementally IF the token
    // matches this primary's history, the offset is within the recoverable (memory OR disk) window,
    // is NOT ahead of head, and no resync is latched. flush_spill first so the disk run is complete.
    ring.borrow_mut().flush_spill();
    let head = ring.borrow().head();
    let token_matches = resume_token == Some(history_token);
    let can_resume = token_matches
        && resume_from.0 > 0
        && resume_from.0 <= head.0
        && ring.borrow().can_serve_from(resume_from)
        && !ring.borrow().needs_resync();

    let mut send_cursor;
    if can_resume {
        send_cursor = resume_from;
    } else {
        let fs_stream = Rc::clone(&stream);
        // The FullSync carries the per-boot history token as its replid: the replica REMEMBERS it and
        // re-advertises it on the next reconnect so this gate can verify the histories match.
        let ok = drive_full_sync(primary, history_token, snapshot_cut, NOW, 8, move |frame| {
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
    remembered_token: &mut Option<ReplId>,
) -> bool {
    let stream: SharedStream = Rc::new(RefCell::new(Some(loop {
        match rt.connect(addr).await {
            Ok(s) => break s,
            Err(_) => rt.timer(Duration::from_millis(5)).await,
        }
    })));
    let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
    let queue = Rc::new(RefCell::new(VecDeque::<Frame>::new()));

    // Advertise the offset AND the remembered history token: a fresh replica (session 0) has no
    // token (full-sync); a reconnect (session 1) re-presents the token it last synced under so the
    // primary's gate can verify the histories match before resuming.
    send_frame(
        rt,
        &stream,
        Frame::ReplConf {
            node: 1,
            ack: applier.applied(),
            resume_token: *remembered_token,
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
        // REMEMBER the history token the primary synced us under (the FullSync replid).
        *remembered_token = Some(loaded.replid);
        *replica = loaded.store;
        *applier = ReplicaApplier::new(loaded.end_offset);
    }

    let mut saw_fullsync = false;
    let mut applied_this_session = 0u32;
    loop {
        let Some(frame) = next_frame(rt, &stream, &pending, &queue).await else {
            break; // EOF / link dropped
        };
        if let Frame::FullSync { replid, .. } = &frame {
            saw_fullsync = true; // a FullSync on the reconnect = a full re-sync (HA-7e avoids this).
            *remembered_token = Some(*replid); // adopt the new history token on a re-sync.
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
                resume_token: *remembered_token,
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

/// THE SILENT-DIVERGENCE FENCE (the review-flagged gap): a PRIMARY RESTART must force a FULL re-sync,
/// never a blind resume against a reset history.
///
/// Scenario: a replica full-syncs from primary P1 (history token T1) and applies a non-zero prefix,
/// then the link drops while the replica holds `resume_from > 0` AND remembers T1. The primary then
/// RESTARTS as P2: a NEW replication source -- the offset space reset to `ReplOffset::ZERO`, a NEW
/// history token T2, the disk backlog purged, and a DIFFERENT (smaller) recovered store (it dropped a
/// key P1 had and changed a value), modeling a primary that lost/reset its writes across the boot.
/// The replica reconnects advertising (`resume_from`, T1).
///
/// Pre-fix, the resume gate keyed ONLY on the offset window: `resume_from > 0` and
/// `can_serve_from(resume_from)` returns true because `resume_from >= head(0)` ("caught up, nothing
/// to serve"), so the primary would RESUME and ship NOTHING -> the replica keeps its STALE P1 store
/// forever, silently diverged. WITH the fix, T1 != T2 (and `resume_from > head`) -> the gate
/// full-syncs: the replica receives a `FullSync`, adopts T2, and CONVERGES to P2's actual data (no
/// stale P1-only keys, the changed value updated).
#[test]
fn primary_restart_forces_full_resync_no_silent_divergence() {
    let dir = temp_dir("restart");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let dir2 = dir.clone();
    local.block_on(&rt, async move {
        let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        let listener = bind_reuseport(addr).unwrap();

        // P1: the ORIGINAL primary (history token T1). A handful of keys + a SHORT tail so the replica
        // ends session 0 at a non-zero applied offset.
        let token_p1 = ReplId::from_bytes([0x11; 20]);
        let disk1 = DiskBacklog::open(&dir2, 8 << 20).expect("disk backlog enabled");
        let ring1 = ReplRing::with_disk(RING_CAP, ReplOffset::ZERO, Some(disk1));
        let mut p1 = ShardStore::new(DBS);
        for i in 0..6u32 {
            p1.upsert(
                0,
                format!("k-{i}").as_bytes(),
                NewValue::Bytes(format!("p1-{i}").as_bytes()),
                ExpireWrite::Clear,
                NOW,
            );
        }
        // A key that ONLY P1 has (the stale key that must be GONE after converging to P2).
        p1.upsert(0, b"only-on-p1", NewValue::Bytes(b"stale"), ExpireWrite::Clear, NOW);
        p1.set_write_observer(ReplObserver::boxed(Rc::clone(&ring1)));
        let p1_cut = ring1.borrow().head();
        // A few tail writes so the replica advances to a NON-ZERO offset before the link drops.
        for i in 0..4u32 {
            p1.upsert(
                0,
                format!("tail1-{i}").as_bytes(),
                NewValue::Bytes(format!("tv{i}").as_bytes()),
                ExpireWrite::Clear,
                NOW,
            );
        }
        ring1.borrow_mut().flush_spill();

        // P2: the RESTARTED primary. SAME address (the operator restarted the pod), but a NEW source:
        // a FRESH ring at offset ZERO, a NEW token T2, the disk backlog PURGED (`open` purges prior
        // segments), and a DIFFERENT store -- it dropped `only-on-p1` and CHANGED `k-0`'s value, so a
        // blind resume (shipping nothing) would leave the replica visibly stale.
        let token_p2 = ReplId::from_bytes([0x22; 20]);
        let dir_p2 = dir2.clone();
        let disk2 = DiskBacklog::open(&dir_p2, 8 << 20).expect("disk backlog re-open purges prior");
        let ring2 = ReplRing::with_disk(RING_CAP, ReplOffset::ZERO, Some(disk2));
        let mut p2 = ShardStore::new(DBS);
        for i in 0..6u32 {
            let val = if i == 0 {
                "p2-CHANGED".to_string() // a value P1 and P2 disagree on.
            } else {
                format!("p1-{i}") // unchanged keys.
            };
            p2.upsert(0, format!("k-{i}").as_bytes(), NewValue::Bytes(val.as_bytes()), ExpireWrite::Clear, NOW);
        }
        // NOTE: P2 deliberately does NOT have `only-on-p1` (the restarted primary lost it). It also has
        // none of P1's tail keys. The replica must end up matching P2 exactly.
        p2.set_write_observer(ReplObserver::boxed(Rc::clone(&ring2)));
        let p2_cut = ring2.borrow().head(); // == 0 (a fresh boot's offset space).
        let expected_p2 = fingerprint(&p2);

        // The PRIMARY task: serve connection 0 as P1 (token T1), then connection 1 as P2 (token T2),
        // modeling the restart between the two accepts.
        let server = tokio::task::spawn_local(async move {
            let rt = TokioRuntime::new();
            // Connection 0: P1.
            if let Ok((stream, _peer)) = rt.accept(&listener).await {
                serve_connection(&rt, stream, &p1, &ring1, p1_cut, token_p1).await;
            }
            // Connection 1: P2 (the restarted primary, new token + reset offsets + smaller store).
            if let Ok((stream, _peer)) = rt.accept(&listener).await {
                serve_connection(&rt, stream, &p2, &ring2, p2_cut, token_p2).await;
            }
        });

        // The REPLICA: session 0 full-syncs from P1 + applies a prefix then KILLs the link (holding a
        // NON-ZERO resume_from + remembered token T1); session 1 reconnects to the RESTARTED P2.
        let replica_result = tokio::task::spawn_local(async move {
            let rt = TokioRuntime::new();
            let mut replica = ShardStore::new(DBS);
            let mut applier = ReplicaApplier::new(ReplOffset::ZERO);
            let mut remembered_token: Option<ReplId> = None;

            // Session 0 against P1: full-sync, then apply the short tail; KILL the link after a prefix.
            run_replica_session_until_kill(&rt, addr, 0, &mut replica, &mut applier, &mut remembered_token).await;
            let resume_from_after_p1 = applier.applied();
            let token_after_p1 = remembered_token;

            // Session 1 against the RESTARTED P2: reconnect advertising (resume_from, T1). It MUST be a
            // FULL re-sync (token T1 != P2's T2, and resume_from > P2's head(0)).
            let saw_fullsync =
                run_replica_session_until_kill(&rt, addr, 1, &mut replica, &mut applier, &mut remembered_token).await;

            (fingerprint(&replica), saw_fullsync, resume_from_after_p1, token_after_p1, remembered_token)
        });

        let (got, session1_fullsync, resume_from_after_p1, token_after_p1, token_after_p2) =
            replica_result.await.expect("replica joined");
        let _ = server.await;

        // The replica really did hold a NON-ZERO resume point + the P1 token going into the reconnect
        // (so this exercises the ack-ahead-of-head + token-mismatch gate, not a trivial ack-0 case).
        assert!(resume_from_after_p1.0 > 0, "the replica advanced to a non-zero offset under P1");
        assert_eq!(token_after_p1, Some(token_p1), "the replica remembered P1's history token");

        // THE FENCE: the reconnect to the restarted primary was a FULL re-sync, NOT a blind resume.
        assert!(
            session1_fullsync,
            "a primary RESTART (new history token + reset offsets) MUST force a FullSync, not a blind resume"
        );
        // It adopted P2's NEW token (so a later same-history reconnect could resume again).
        assert_eq!(token_after_p2, Some(token_p2), "the replica adopted the restarted primary's token");
        // And it CONVERGED to P2's actual data: the stale P1-only key is GONE and the changed value is
        // updated -- no silent divergence.
        assert_eq!(got, expected_p2, "the replica converged to the RESTARTED primary's data (no stale keys)");
    });
    std::fs::remove_dir_all(&dir).ok();
}

/// A replica session for the restart test, mirroring the production `attach_once`: dial, advertise
/// (offset, remembered token), then PEEK the first frame -- a `FullSync` means a full re-sync (rebuild
/// the store via `receive_full_sync`, adopt the new token), anything else means a RESUME (keep the
/// store, apply the tail). Returns whether a `FullSync` was seen this session.
///
/// Session 0 KILLs the link after a short tail prefix so the replica carries a NON-ZERO resume point
/// and the P1 token into the reconnect. Session >= 1 (the reconnect to the restarted primary) takes
/// the full-sync path and converges to the restarted primary's data, then returns at SYNCEND/EOF.
async fn run_replica_session_until_kill(
    rt: &TokioRuntime,
    addr: SocketAddr,
    session: u32,
    replica: &mut ShardStore,
    applier: &mut ReplicaApplier,
    remembered_token: &mut Option<ReplId>,
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
            resume_token: *remembered_token,
        },
    )
    .await
    .expect("attach send");

    // PEEK the first frame to learn the primary's decision (FullSync vs resume), exactly like the
    // live `attach_once`.
    let Some(first) = next_frame(rt, &stream, &pending, &queue).await else {
        return false; // link dropped before the first frame.
    };
    let is_full_sync = matches!(first, Frame::FullSync { .. });
    queue.borrow_mut().push_front(first);

    if is_full_sync {
        // FULL SYNC: rebuild the store from the FULLSYNC/SYNCKV*/SYNCEND stream via the production
        // decoder, adopting the new history token. This REPLACES any stale prior store -> convergence.
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
        .expect("full sync completes on a re-sync");
        *remembered_token = Some(loaded.replid);
        *replica = loaded.store;
        *applier = ReplicaApplier::new(loaded.end_offset);
    }

    // Apply the steady-state tail (after a full sync, or directly on a resume) until the link drops or
    // session 0's short-prefix kill.
    let mut applied_this_session = 0u32;
    loop {
        let Some(frame) = next_frame(rt, &stream, &pending, &queue).await else {
            break; // EOF / link dropped (the primary closed after SYNCEND on a full sync).
        };
        match applier.apply(replica, frame, NOW) {
            ApplyOutcome::Applied(_) => applied_this_session += 1,
            ApplyOutcome::Duplicate => {}
            ApplyOutcome::Gap => panic!("unexpected gap (session {session})"),
        }
        let _ = send_frame(
            rt,
            &stream,
            Frame::ReplConf {
                node: 1,
                ack: applier.applied(),
                resume_token: *remembered_token,
            },
        )
        .await;

        // Session 0: KILL the link after a short prefix so the replica carries a NON-ZERO resume point
        // + the P1 token into the reconnect.
        if session == 0 && applied_this_session >= 3 {
            drop(stream.borrow_mut().take());
            break;
        }
    }
    is_full_sync
}
