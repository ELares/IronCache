// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-7b full-sync loopback acceptance test: a primary streams its whole snapshot to a
//! replica over real loopback TCP, and the replica's freshly-loaded store matches the
//! primary key-for-key.
//!
//! Mirrors the HA-7a loopback shape (`tests/loopback.rs`): both ends run on a single
//! current-thread tokio runtime + `LocalSet` (the shared-nothing, `!Send`, thread-per-core
//! shape, ADR-0002), driven ENTIRELY through the [`ironcache_runtime::Runtime`] seam. The
//! primary binds a listener, accepts one connection, and runs [`drive_full_sync`] with a
//! `Runtime::send`-backed sink; the replica dials, runs [`receive_full_sync`] with a
//! `Runtime::recv`-backed source (a small frame-decode buffer), and on SYNCEND ends up with
//! a store equal to the primary's, having adopted the cut `end_offset`.

use core::cell::RefCell;
use core::time::Duration;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;

use ironcache_repl::{
    Frame, FrameError, ReplId, ReplOffset, drive_full_sync, encode_kvobj, receive_full_sync,
};
use ironcache_runtime::Runtime;
use ironcache_runtime::TokioRuntime;
use ironcache_runtime::tokio_rt::bind_reuseport;
use ironcache_storage::{
    ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};
use ironcache_store::{ShardStore, SnapshotCursor};

const NOW: UnixMillis = UnixMillis(1_000);
const DBS: u32 = 4;
const END_OFFSET: ReplOffset = ReplOffset(4242);

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
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

fn put_set(s: &mut ShardStore, db: u32, key: &[u8], members: &[Vec<u8>]) {
    let members = members.to_vec();
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

/// A primary with a mix of dbs, types, encodings, and TTLs (incl. a large set so the
/// hashtable encoding crosses the wire), plus enough keys that the chunked transfer spans
/// multiple chunks.
fn populate(s: &mut ShardStore) {
    put(s, 0, b"str-a", b"alpha");
    put(s, 0, b"str-int", b"4242");
    put_ttl(s, 1, b"str-ttl", b"withttl", 9_999);
    put_set(
        s,
        2,
        b"set-int",
        &[b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
    );
    put_set(
        s,
        2,
        b"set-str",
        &[b"red".to_vec(), b"green".to_vec(), b"blue".to_vec()],
    );
    let big: Vec<Vec<u8>> = (0..200u32)
        .map(|i| format!("e{i:04}").into_bytes())
        .collect();
    put_set(s, 3, b"set-big", &big);
    for i in 0..30u32 {
        put(
            s,
            i % DBS,
            format!("k{i:03}").as_bytes(),
            format!("v{i}").as_bytes(),
        );
    }
}

/// A comparable fingerprint of a store: `(db, key, encode_kvobj-bytes)` for every live key,
/// sorted. The kvcodec bytes carry type+encoding+ttl+value, so two stores are equal iff
/// their fingerprints are equal (no need to name private Val internals).
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

#[test]
fn full_sync_over_loopback_matches_primary() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        let listener = bind_reuseport(addr).unwrap();

        // Build the primary and capture its expected fingerprint before moving it into the
        // server task.
        let mut primary = ShardStore::new(DBS);
        populate(&mut primary);
        let expected = fingerprint(&primary);

        // --- The PRIMARY server task: accept one connection, drive the full sync. ---
        tokio::task::spawn_local(async move {
            let rt = TokioRuntime::new();
            let (stream, _peer) = rt.accept(&listener).await.expect("accept");
            // The stream lives behind an Rc<RefCell<Option<..>>>: each send TAKES it out, does
            // the I/O on the owned value, and puts it back -- so no RefCell borrow is ever held
            // across an await (the single-thread, sequential I/O idiom clippy wants).
            let stream = Rc::new(RefCell::new(Some(stream)));
            let replid = ReplId::from_bytes([0xCD; 20]);
            let send_stream = Rc::clone(&stream);
            // A small chunk_max so the transfer genuinely spans multiple chunks.
            let _ = drive_full_sync(&primary, replid, END_OFFSET, NOW, 4, move |frame| {
                let bytes = frame.encode();
                let stream = Rc::clone(&send_stream);
                async move {
                    let mut s = stream.borrow_mut().take().expect("stream present");
                    let res = rt.send(&mut s, bytes).await;
                    *stream.borrow_mut() = Some(s);
                    res.map(|_| ()).map_err(|_| ())
                }
            })
            .await;
        });

        // --- The REPLICA task: dial, receive the full sync into a fresh store. ---
        let result = tokio::task::spawn_local(async move {
            let rt = TokioRuntime::new();
            // Reconnect-poll the dial (the listener may not be ready the instant we dial).
            let stream = loop {
                match rt.connect(addr).await {
                    Ok(s) => break s,
                    Err(_) => rt.timer(Duration::from_millis(10)).await,
                }
            };
            let stream = Rc::new(RefCell::new(Some(stream)));

            // The frame source: a persistent byte buffer + a ready-frame queue. Each call
            // returns the next complete frame, reading more from the socket as needed. The
            // stream is TAKEN out around each recv so no RefCell borrow is held across await.
            let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
            let queue = Rc::new(RefCell::new(VecDeque::<Frame>::new()));

            let loaded = receive_full_sync(
                || ShardStore::new(DBS),
                move || {
                    let pending = Rc::clone(&pending);
                    let queue = Rc::clone(&queue);
                    let stream = Rc::clone(&stream);
                    async move {
                        loop {
                            if let Some(f) = queue.borrow_mut().pop_front() {
                                return Some(f);
                            }
                            // Decode any complete frames already buffered.
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
                                    Err(FrameError) => return None, // malformed: abort
                                }
                            }
                            if drained_any {
                                continue;
                            }
                            // Need more bytes: read a chunk from the socket (TAKE/put-back so
                            // no RefCell borrow crosses the await).
                            let taken: Vec<u8> = core::mem::take(&mut *pending.borrow_mut());
                            let mut s = stream.borrow_mut().take().expect("stream present");
                            let res = rt.recv(&mut s, taken).await;
                            *stream.borrow_mut() = Some(s);
                            match res {
                                Ok(r) => {
                                    if r.n == 0 {
                                        return None; // peer closed
                                    }
                                    *pending.borrow_mut() = r.buf;
                                }
                                Err(_) => return None,
                            }
                        }
                    }
                },
            )
            .await;
            loaded.map(|l| (fingerprint(&l.store), l.end_offset))
        })
        .await
        .expect("replica task joined");

        let (got, end_offset) = result.expect("the full sync completed");
        assert_eq!(end_offset, END_OFFSET, "the replica adopts the cut offset");
        assert_eq!(
            got, expected,
            "the replica's store matches the primary key-for-key after SYNCEND"
        );
    });
}
