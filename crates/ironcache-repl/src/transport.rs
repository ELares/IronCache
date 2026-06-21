// SPDX-License-Identifier: MIT OR Apache-2.0
//! The replication transport: the real-I/O adapter that drives the pure
//! [`crate::link`] state machines over the [`Runtime`] seam (HA-7a).
//!
//! This is the only layer that touches a real socket or timer. It mirrors the
//! Raft adapter's listener/dialer shape ([`ironcache_raft_net`] in spirit) but on a
//! SEPARATE, dedicated replication port and listener, so a replication frame never
//! shares a connection or a queue with a Raft heartbeat (the crate's reason to
//! exist): consensus and replication are physically distinct data planes.
//!
//! - [`run_primary_repl_listener`] binds the replication listener, accepts replica
//!   connections, and serves each on its own shard-local task: it reads the attach
//!   `REPLCONF` into a [`crate::link::PrimaryLink`] step, then emits `REPLPING`
//!   heartbeats on a [`Runtime::timer`] cadence.
//! - [`run_replica_link`] dials the primary's replication port, drives a
//!   [`crate::link::ReplicaLink`] (send `REPLCONF`, recv `REPLPING`s, advance the
//!   cursor), and on disconnect RECONNECTS, re-sending `REPLCONF` from the
//!   last-acked offset. The primary address is a static argument for now
//!   (`replicaof <host:port>`); the role-driven attach is HA-7d.
//!
//! Time is read through [`Runtime::timer`] only; there is no raw `std::time` here,
//! and the link step functions read no clock at all. Shard-local shared state (the
//! [`ReplicaObserver`]) uses `Rc<Cell<..>>` (the shared-nothing, single-shard idiom,
//! ADR-0002), never a cross-core lock.

use core::cell::Cell;
use core::time::Duration;
use std::net::SocketAddr;
use std::rc::Rc;

use ironcache_runtime::Runtime;

use crate::cursor::{ReplId, ReplOffset};
use crate::frames::{Frame, FrameError};
use crate::link::{DEFAULT_HEARTBEAT, LinkEvent, PrimaryLink, ReplState, ReplicaLink};

/// A shard-local, observable view of a running replica link: the last-acked offset
/// (the resume point) and the current link state, both updated after every step.
///
/// It is shared (`Rc`) between the run loop and any observer on the SAME shard (the
/// shared-nothing, single-shard idiom; no cross-core lock). A test reads it after
/// driving the `LocalSet`. `Cell`/`Copy` fields keep reads non-borrowing.
#[derive(Debug, Default)]
pub struct ReplicaObserver {
    acked: Cell<u64>,
    state: Cell<Option<ReplState>>,
}

impl ReplicaObserver {
    /// A fresh observer (offset 0, no state observed yet), wrapped in an `Rc` for
    /// sharing between the run loop and an observer on the same shard.
    #[must_use]
    pub fn new() -> Rc<Self> {
        Rc::new(ReplicaObserver::default())
    }

    /// The last-acked offset the replica link has published (its resume point).
    #[must_use]
    pub fn acked(&self) -> ReplOffset {
        ReplOffset(self.acked.get())
    }

    /// The link's last-published state, or `None` before the link has stepped.
    #[must_use]
    pub fn state(&self) -> Option<ReplState> {
        self.state.get()
    }

    /// Publish the link's current acked offset and state (called by the run loop).
    fn publish(&self, link: &ReplicaLink) {
        self.acked.set(link.acked().0);
        self.state.set(Some(link.state()));
    }
}

/// How long to wait between reconnect attempts after the replica link drops. A
/// small fixed backoff for 7a (no jitter; the link carries no election timing).
const RECONNECT_BACKOFF: Duration = Duration::from_millis(50);

/// Drive a replica's link to the primary at `primary_addr`, publishing its progress
/// to `observer`, until `should_run` returns `false`.
///
/// A long-running loop: it dials the primary, sends `REPLCONF` from the last-acked
/// offset, then recv-loops `REPLPING`s (advancing the cursor) until the socket
/// drops, then waits [`RECONNECT_BACKOFF`] and reconnects, resuming from the
/// (preserved) last-acked offset. Every transition goes through the pure
/// [`ReplicaLink::step`]; this function only performs the I/O the effects describe.
///
/// `node` is the replica's id; `start_acked` is its resume point at boot
/// (`ReplOffset::ZERO` when never synced).
pub async fn run_replica_link<R>(
    rt: R,
    primary_addr: SocketAddr,
    node: u64,
    start_acked: ReplOffset,
    observer: Rc<ReplicaObserver>,
    mut should_run: impl FnMut() -> bool,
) where
    R: Runtime,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
{
    let mut link = ReplicaLink::new(node, start_acked);
    observer.publish(&link);

    while should_run() {
        // Dial the primary's replication port. A failed dial leaves the link
        // Disconnected; back off before retrying.
        let Ok(mut stream) = rt.connect(primary_addr).await else {
            rt.timer(RECONNECT_BACKOFF).await;
            continue;
        };

        // Connected: the link sends REPLCONF (attach / resume from last-acked).
        let fx = link.step(LinkEvent::Connected);
        if let Some(frame) = fx.send {
            let buf: R::Buf = frame.encode().into();
            let _ = rt.send(&mut stream, buf).await;
        }
        observer.publish(&link);

        // Recv loop: read REPLPINGs and advance the cursor until the socket drops.
        serve_replica_recv(&rt, &mut stream, &mut link, &observer).await;

        // The socket dropped: the link goes Disconnected; back off and reconnect.
        link.step(LinkEvent::Disconnected);
        observer.publish(&link);
        if !should_run() {
            break;
        }
        rt.timer(RECONNECT_BACKOFF).await;
    }
}

/// Recv-loop one replica connection: decode `REPLPING` frames, step the link, and
/// publish the advancing offset/state, returning when the socket closes or errors.
async fn serve_replica_recv<R>(
    rt: &R,
    stream: &mut R::Stream,
    link: &mut ReplicaLink,
    observer: &ReplicaObserver,
) where
    R: Runtime,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
{
    let mut pending: Vec<u8> = Vec::new();
    loop {
        // Parse any complete frames already buffered before reading more.
        loop {
            match Frame::decode(&pending) {
                Ok(Some((frame, consumed))) => {
                    pending.drain(..consumed);
                    if let Frame::ReplPing { replid, offset } = frame {
                        link.step(LinkEvent::GotPing { replid, offset });
                        observer.publish(link);
                    }
                    // A REPLCONF on the replica side is unexpected; the link's step
                    // ignores it, so simply continue draining.
                }
                Ok(None) => break,         // need more bytes
                Err(FrameError) => return, // malformed frame: drop the connection
            }
        }
        // Read another chunk.
        let taken: R::Buf = core::mem::take(&mut pending).into();
        match rt.recv(stream, taken).await {
            Ok(res) => {
                if res.n == 0 {
                    return; // peer closed
                }
                pending = res.buf.into();
            }
            Err(_) => return,
        }
    }
}

/// Bind `addr` as the primary's replication listener and serve replica connections
/// until the listener errors, advertising `replid` and the offset `current_offset`
/// returns on each heartbeat.
///
/// One dedicated replication listener per primary, SEPARATE from the Raft listener.
/// Each accepted connection is served on its own shard-local task. `current_offset`
/// is a closure the primary supplies so each heartbeat advertises the live offset;
/// in 7a the caller advances it trivially (e.g. per tick), in 7c from the HA-5a
/// write seam.
pub async fn run_primary_repl_listener<R, F>(
    rt: R,
    listener: R::Listener,
    replid: ReplId,
    current_offset: F,
) where
    R: Runtime + Clone + 'static,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
    F: Fn() -> ReplOffset + Clone + 'static,
{
    loop {
        let Ok((stream, _peer)) = rt.accept(&listener).await else {
            return; // listener failed; the primary is going away
        };
        let rt2 = rt.clone();
        let offset = current_offset.clone();
        rt.spawn_on_shard(async move {
            serve_primary_conn::<R, F>(rt2, stream, replid, offset).await;
        });
    }
}

/// Serve one accepted replica connection on the primary side: read the attach
/// `REPLCONF`, then emit `REPLPING` heartbeats on the [`Runtime::timer`] cadence.
///
/// One task owns the stream throughout (the `Runtime` seam's `recv`/`send` take
/// `&mut Stream`, so the duplex is not split across tasks). The handler first reads
/// frames until the replica's attach `REPLCONF` arrives (the handshake that names
/// the resume point), stepping [`PrimaryLink`], and then enters a pure timer-driven
/// REPLPING send loop. In 7a the replica acks only on attach/reconnect (not per
/// ping), so the heartbeat loop need not read further; the steady-state ack stream
/// the link tracks is wired in HA-7c. A send error ends the loop (the replica is
/// gone); the replica reconnects, producing a fresh attach here.
async fn serve_primary_conn<R, F>(rt: R, mut stream: R::Stream, replid: ReplId, current_offset: F)
where
    R: Runtime + Clone + 'static,
    R::Buf: From<Vec<u8>> + Into<Vec<u8>>,
    F: Fn() -> ReplOffset + Clone + 'static,
{
    let mut link = PrimaryLink::new(replid);
    link.step(LinkEvent::Connected, current_offset());

    // Read the attach REPLCONF (the handshake). Drain frames until one arrives or the
    // socket closes / sends garbage.
    let mut pending: Vec<u8> = Vec::new();
    let mut attached = false;
    while !attached {
        loop {
            match Frame::decode(&pending) {
                Ok(Some((frame, consumed))) => {
                    pending.drain(..consumed);
                    if let Frame::ReplConf { node, ack, .. } = frame {
                        link.step(LinkEvent::GotReplconf { node, ack }, current_offset());
                        attached = true;
                        break;
                    }
                }
                Ok(None) => break,         // need more bytes
                Err(FrameError) => return, // malformed: drop the connection
            }
        }
        if attached {
            break;
        }
        let taken: R::Buf = core::mem::take(&mut pending).into();
        match rt.recv(&mut stream, taken).await {
            Ok(res) => {
                if res.n == 0 {
                    return; // replica closed before attaching
                }
                pending = res.buf.into();
            }
            Err(_) => return,
        }
    }

    // The heartbeat send loop: emit REPLPING advertising the live offset on cadence.
    loop {
        rt.timer(DEFAULT_HEARTBEAT).await;
        let fx = link.step(LinkEvent::Tick, current_offset());
        if let Some(frame) = fx.send {
            let buf: R::Buf = frame.encode().into();
            if rt.send(&mut stream, buf).await.is_err() {
                return; // replica gone
            }
        }
    }
}
