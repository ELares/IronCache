// SPDX-License-Identifier: MIT OR Apache-2.0
//! The RAW io_uring backend for [`crate::Runtime`] (#682, docs/design/IOURING_DATAPATH.md),
//! built DIRECTLY on the `io-uring` crate instead of `tokio-uring`.
//!
//! ## Why a raw backend
//!
//! `tokio-uring` does not cross-build for the flagship static-musl target (it names libc `statx`
//! types musl does not expose), and it cannot reach provided-buffer rings / multishot recv (the
//! #513 fast path). The raw `io-uring` crate builds clean on musl and exposes both. This module is
//! the migration's foundation (#682, sub-slice 1a): a `Runtime` impl over the raw ring, proven by a
//! round-trip + a cancel-safety test, cross-building for aarch64-musl. It is ADDITIVE + default-OFF
//! (`io_uring_raw` feature) + Linux-gated, and it COEXISTS with the `io_uring` (tokio-uring) backend:
//! it introduces no shared alias and is NOT wired into the serve loop in this slice (serve-loop
//! wiring is a later slice), so both features may be enabled together.
//!
//! ## The async model (musl-clean, no blocking submit_and_wait)
//!
//! The raw ring runs INSIDE a `tokio::runtime` current-thread runtime + `LocalSet` per shard thread
//! (the same per-shard shape tokio-uring uses). Completions are driven by wrapping the ring fd in
//! tokio's [`AsyncFd`]: the completion side of the ring fd is epoll-readable for ordinary IRQ-driven
//! socket I/O, so tokio's own reactor parks the thread in `epoll_wait`, and a small drain task reaps
//! CQEs non-blocking when the fd goes readable. Submission is non-blocking `ring.submit()`, flushed
//! right before the thread parks via `on_thread_park`. So the readiness plumbing is tokio's epoll
//! (musl-proven) and the data plane is the raw ring (musl-clean) -- exactly the split that unblocks
//! musl. No `submit_and_wait` blocks a serving thread.
//!
//! ## Cancel-safety (the load-bearing unsafe invariant)
//!
//! io_uring's completion model requires a submitted op's buffer to outlive the kernel call: the SQE
//! carries a raw pointer that the kernel may touch until the CQE arrives. An op-future OWNS its
//! buffer; if it is dropped BEFORE its CQE, the buffer is NOT freed -- it is moved into the op's slab
//! slot as [`Lifecycle::Ignored`], which owns it until the CQE lands and `complete` drops it. So a
//! cancelled recv/send can never free memory the kernel still references (no use-after-free). This
//! reproduces `tokio-uring`'s proven `Ignored` lifecycle, so the serve loop's cancel-on-drop
//! assumptions stay true when the raw backend is later wired in.

#![allow(clippy::module_name_repetitions)]

use crate::{IoBuf, RecvResult, Runtime};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::any::Any;
use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::rc::Rc;

use io_uring::{IoUring, cqueue, opcode, squeue, types};
use slab::Slab;
use tokio::io::unix::AsyncFd;

/// The default read window reserved per `recv`, matching the tokio + tokio-uring backends' 16 KiB
/// reservation so all three backends frame identically.
const READ_WINDOW: usize = 16 * 1024;

/// The submission-queue depth per shard ring. 256 in-flight SQEs is generous for one shard's
/// connection fan-out; the CQ is sized to 4x this (in `raw_uring_start`) so a mass-drop burst (each
/// op's own CQE plus its cancel's CQE) has headroom before the kernel's overflow backlog engages.
const SQ_ENTRIES: u32 = 256;

/// A reserved `user_data` for best-effort `AsyncCancel` CQEs (a cancelled op's cancellation request):
/// its completion routes here and is DISCARDED by the drain (it owns no slab slot).
const CANCEL_USER_DATA: u64 = u64::MAX;

// ---------------------------------------------------------------------------
// The ring fd wrapper for AsyncFd. NON-owning: the `IoUring` owns and closes the fd, so this must
// NOT close it on drop (it has no `Drop`), or the fd would be double-closed. AsyncFd only registers
// / deregisters the fd with tokio's epoll; it never closes an fd whose wrapper has no destructor.
// ---------------------------------------------------------------------------
struct RingFd(RawFd);

impl AsRawFd for RingFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

// ---------------------------------------------------------------------------
// The per-op lifecycle in the driver's slab. `user_data == slab index`.
// ---------------------------------------------------------------------------
enum Lifecycle {
    /// The SQE was pushed; no future has parked a waker yet (or the future was polled once before
    /// any completion and is between polls). The future will transition it to `Waiting` on poll.
    Submitted,
    /// A future is awaiting this op; holds the latest waker to wake when the CQE lands.
    Waiting(Waker),
    /// The CQE arrived; the result is parked here for the future to take on its next poll.
    Completed(cqueue::Entry),
    /// The future was DROPPED before its CQE. Owns the op's buffer/resources so they outlive the
    /// in-flight kernel op; `complete` drops this (freeing the memory) when the CQE finally lands.
    Ignored(Box<dyn Any>),
}

// ---------------------------------------------------------------------------
// The per-shard driver: the ring + the in-flight-op table. Lives in a thread-local `Rc<RefCell<>>`
// so the op-futures (submit / poll / drop) and the drain task all reach the SAME ring on the shard
// thread. `!Send` by construction (the ring and the whole model are thread-per-core, ADR-0002).
// ---------------------------------------------------------------------------
struct Driver {
    ring: IoUring,
    ops: Slab<Lifecycle>,
}

impl Driver {
    /// Flush locally-pushed SQEs to the kernel (non-blocking). Called from `on_thread_park` right
    /// before the thread sleeps in `epoll_wait`, so no SQE is left un-submitted going into the park
    /// (otherwise the ring fd would never go readable and the op would hang). On `EBUSY` (the CQ is
    /// full) reap completions to make room, then retry.
    fn flush(&mut self) {
        // Retry ONLY while `submit` returns EBUSY (the CQ is full): reap completions to make room,
        // then submit again. `Ok` exits the loop; any other error exits too (it surfaces on the op's
        // own CQE path).
        while let Err(e) = self.ring.submit() {
            if e.raw_os_error() == Some(libc::EBUSY) {
                self.dispatch_completions();
            } else {
                break;
            }
        }
    }

    /// Reap all currently-available CQEs, routing each to its slab slot and waking the future.
    /// Returns the number reaped (0 = the fd was spuriously readable / already drained).
    fn dispatch_completions(&mut self) -> usize {
        let mut count = 0;
        loop {
            // Re-borrow the completion queue each iteration so `complete` can mutate `ops` without
            // holding the CQ borrow across it.
            let cqe = {
                let mut cq = self.ring.completion();
                cq.sync();
                cq.next()
            };
            let Some(cqe) = cqe else { break };
            count += 1;
            let ud = cqe.user_data();
            if ud == CANCEL_USER_DATA {
                continue; // a best-effort AsyncCancel completion: it owns no slot, discard it.
            }
            self.complete(ud as usize, cqe);
        }
        count
    }

    /// Transition the slab slot for a landed CQE.
    ///
    /// P3 PREREQUISITE (multishot, #513): `user_data == slab index`, and slab indices are REUSED
    /// after `remove`. In P1 every op is single-shot -- exactly one CQE per index, and the slot is
    /// not freed until that CQE lands -- so an index cannot alias a live op. Multishot recv produces
    /// MANY CQEs per `user_data`; before wiring it, `user_data` must carry a generation/epoch tag
    /// (e.g. `idx | (gen << 32)`) so a stale CQE for a freed op is detected and dropped here rather
    /// than mutating a since-reused slot.
    fn complete(&mut self, idx: usize, cqe: cqueue::Entry) {
        let Some(slot) = self.ops.get_mut(idx) else {
            return; // a stale/duplicate completion for a freed slot: single-shot ops free on their
            // one CQE, so this is unreachable in P1, but tolerate it rather than panic.
        };
        match core::mem::replace(slot, Lifecycle::Completed(cqe)) {
            Lifecycle::Waiting(waker) => waker.wake(),
            Lifecycle::Ignored(_owned) => {
                // The future was dropped; the kernel is finally done with the op's memory. Drop the
                // owned resources HERE (this is the only place a cancelled op's buffer is freed) and
                // free the slot. `_owned` drops at end of scope; overwrite the slot we just set.
                self.ops.remove(idx);
            }
            // `Submitted`: the future polls later and takes the `Completed` we just stored.
            // `Completed`: two CQEs for one single-shot op -- impossible in P1; leave it stored.
            Lifecycle::Submitted | Lifecycle::Completed(_) => {}
        }
    }
}

impl Drop for Driver {
    /// Drain in-flight ops to QUIESCENCE before the ring fd and the `Ignored` buffers are freed.
    ///
    /// This is the teardown half of the cancel-safety invariant. During operation, a dropped-in-
    /// flight op's buffer lives in `Lifecycle::Ignored` until its CQE lands. At shutdown, if the
    /// ring fd were simply closed and `ops` dropped while an op is still in flight, the kernel's
    /// async release could touch a buffer we just freed (use-after-free), and any un-submitted
    /// cancel/read SQE would strand its `Ignored` buffer. So here -- with the tokio runtime already
    /// gone, on the shutdown path where blocking is fine -- we cancel every outstanding op and reap
    /// every CQE before the fields drop. Bounded so a wedged kernel op cannot hang shutdown forever.
    ///
    /// By the time this runs (see `raw_uring_start`'s teardown), all op-FUTURES have already dropped,
    /// so every remaining slab slot is `Ignored`; each just needs its (cancelled) CQE reaped.
    fn drop(&mut self) {
        // Cancel everything still outstanding so no op can block the drain waiting on data that will
        // never come (e.g. a recv parked on an idle socket). Re-pushing a cancel for an op that
        // already had one (from `OpFuture::drop`) is harmless: the duplicate completes -ENOENT and
        // routes to `CANCEL_USER_DATA` (discarded).
        let pending: Vec<usize> = self.ops.iter().map(|(idx, _)| idx).collect();
        for idx in pending {
            let cancel = opcode::AsyncCancel::new(idx as u64)
                .build()
                .user_data(CANCEL_USER_DATA);
            // SAFETY: AsyncCancel references no user buffer (only a user_data key), so it is always
            // valid to submit. On a full SQ, submit to make room and retry once.
            if unsafe { self.ring.submission().push(&cancel) }.is_err() {
                let _ = self.ring.submit();
                let _ = unsafe { self.ring.submission().push(&cancel) };
            }
        }
        // Reap to quiescence. `submit_and_wait(1)` submits the queued cancels (and any still-queued
        // original SQEs) and blocks for at least one CQE; `dispatch_completions` frees each `Ignored`
        // slot on its CQE. The idle-round bound is a paranoia backstop against a slot that never
        // completes -- it caps shutdown work rather than risking an unbounded hang.
        let mut idle_rounds = 0u32;
        while !self.ops.is_empty() && idle_rounds < 4096 {
            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(_) => break, // cannot make progress on the ring; stop rather than spin
            }
            if self.dispatch_completions() == 0 {
                idle_rounds += 1;
            } else {
                idle_rounds = 0;
            }
        }
    }
}

thread_local! {
    /// This shard thread's raw io_uring driver, installed by [`raw_uring_start`].
    static DRIVER: RefCell<Option<Rc<RefCell<Driver>>>> = const { RefCell::new(None) };
}

/// Run `f` with a mutable borrow of this thread's driver. Panics if called outside
/// [`raw_uring_start`] (no driver installed). Never called re-entrantly: `complete`'s `waker.wake()`
/// only SCHEDULES a task, it does not poll it synchronously, so no `with_driver` nests inside another.
fn with_driver<R>(f: impl FnOnce(&mut Driver) -> R) -> R {
    DRIVER.with(|cell| {
        let rc = cell
            .borrow()
            .as_ref()
            .expect("raw io_uring driver not installed on this thread (call raw_uring_start)")
            .clone();
        let mut d = rc.borrow_mut();
        f(&mut d)
    })
}

// ---------------------------------------------------------------------------
// The op-future: push an SQE, park until the CQE, hand back the owned resource + result.
// ---------------------------------------------------------------------------
struct OpFuture<B: 'static> {
    idx: usize,
    /// The owned resource whose memory the SQE references (a buffer, a sockaddr box, ...). Held
    /// until completion; moved into `Lifecycle::Ignored` if this future is dropped in flight.
    owned: Option<B>,
    /// Set once the result has been taken so `Drop` knows the slot is already freed.
    done: bool,
}

impl<B: 'static> OpFuture<B> {
    /// THE SINGLE unsafe SQE-push boundary. Takes the owned resource BY VALUE and builds a future
    /// that owns it until the CQE, so any pointer `sqe` carries into `owned` stays valid for the
    /// whole kernel op. Every op (recv/send/...) submits through here.
    fn submit(sqe: squeue::Entry, owned: B) -> Self {
        let idx = with_driver(|d| {
            let idx = d.ops.insert(Lifecycle::Submitted);
            let sqe = sqe.user_data(idx as u64);
            // SAFETY: `owned` is moved into the returned `OpFuture` below and, if that future is
            // dropped before its CQE, into `Lifecycle::Ignored` (which lives until the CQE). So any
            // pointer/length `sqe` carries into `owned`'s allocation is valid for the entire op.
            // This is the ONLY `SubmissionQueue::push` in the crate.
            loop {
                let pushed = unsafe { d.ring.submission().push(&sqe) };
                if pushed.is_ok() {
                    break;
                }
                // The submission queue is full: submit what is queued (making room), then retry.
                d.flush();
            }
            idx
        });
        OpFuture {
            idx,
            owned: Some(owned),
            done: false,
        }
    }
}

// `B: Unpin` lets `poll` take `&mut Self` via `get_mut` -- always satisfied here: every owned
// resource we submit (a `Vec<u8>` buffer, a boxed sockaddr) is `Unpin`. The op-future itself is a
// plain struct that is never self-referential; the kernel references the HEAP buffer behind the
// `Vec`, not the future, so moving the future is sound.
impl<B: 'static + Unpin> Future for OpFuture<B> {
    /// The raw op result (`>= 0` = bytes/fd, mapped to `Err(errno)` when negative) plus the owned
    /// resource handed back.
    type Output = (io::Result<i32>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        with_driver(|d| {
            let slot = d
                .ops
                .get_mut(this.idx)
                .expect("a live op-future's slab slot exists until it takes the result");
            match slot {
                Lifecycle::Submitted => {
                    *slot = Lifecycle::Waiting(cx.waker().clone());
                    Poll::Pending
                }
                Lifecycle::Waiting(w) => {
                    if !w.will_wake(cx.waker()) {
                        w.clone_from(cx.waker());
                    }
                    Poll::Pending
                }
                Lifecycle::Completed(_) => {
                    let Lifecycle::Completed(entry) = d.ops.remove(this.idx) else {
                        unreachable!("slot was Completed")
                    };
                    this.done = true;
                    let owned = this.owned.take().expect("owned present until completion");
                    let r = entry.result();
                    let res = if r < 0 {
                        Err(io::Error::from_raw_os_error(-r))
                    } else {
                        Ok(r)
                    };
                    Poll::Ready((res, owned))
                }
                Lifecycle::Ignored(_) => {
                    unreachable!(
                        "a live future's slot is never Ignored (only its own Drop sets it)"
                    )
                }
            }
        })
    }
}

impl<B: 'static> Drop for OpFuture<B> {
    fn drop(&mut self) {
        if self.done {
            return; // completed + result taken: the slot is already freed, nothing in flight.
        }
        let Some(owned) = self.owned.take() else {
            return;
        };
        let idx = self.idx;
        with_driver(|d| {
            let Some(slot) = d.ops.get_mut(idx) else {
                return;
            };
            if let Lifecycle::Completed(_) = slot {
                // The CQE already landed but we never polled it: drain + free the slot; `owned`
                // drops here (the kernel is done -- the CQE proves it).
                d.ops.remove(idx);
            } else {
                // In flight (Submitted / Waiting): the kernel may still touch `owned`'s memory.
                // MOVE `owned` into the slot so it outlives the op; free it when the CQE lands
                // (`complete`'s Ignored arm). Best-effort cancel to hurry the CQE along.
                *slot = Lifecycle::Ignored(Box::new(owned));
                let cancel = opcode::AsyncCancel::new(idx as u64)
                    .build()
                    .user_data(CANCEL_USER_DATA);
                // SAFETY: AsyncCancel references no user buffer (only a user_data key); it is
                // always valid to submit. A push failure (SQ full) is ignored -- cancel is
                // best-effort and the op still completes + frees via the Ignored arm regardless.
                let _ = unsafe { d.ring.submission().push(&cancel) };
            }
        });
    }
}

// ---------------------------------------------------------------------------
// The raw-fd stream + listener. Each OWNS its socket fd and closes it exactly once on drop.
// ---------------------------------------------------------------------------

/// A connected TCP stream for the raw io_uring backend: a sole-owner socket fd. `recv`/`send`
/// submit `Read`/`Write` SQEs against it; it closes the fd on drop.
#[derive(Debug)]
pub struct RawUringTcpStream {
    fd: RawFd,
}

impl RawUringTcpStream {
    /// Adopt an already-connected std socket (the acceptor hands off accepted sockets by fd, the
    /// same adoption the tokio-uring bootstrap does).
    #[must_use]
    pub fn from_std(stream: std::net::TcpStream) -> Self {
        RawUringTcpStream {
            fd: stream.into_raw_fd(),
        }
    }
}

impl AsRawFd for RawUringTcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for RawUringTcpStream {
    fn drop(&mut self) {
        // SAFETY: sole ownership of `fd` (adopted via `from_std`/accept, never duplicated), closed
        // exactly once here. Matches the fd lifecycle of the tokio-uring backend's stream.
        unsafe {
            libc::close(self.fd);
        }
    }
}

/// A listening socket for the raw io_uring backend: a sole-owner listener fd.
#[derive(Debug)]
pub struct RawUringTcpListener {
    fd: RawFd,
}

impl RawUringTcpListener {
    /// Adopt an already-bound std listener (the bootstrap binds with `SO_REUSEPORT` on the host and
    /// hands the fd here, mirroring the tokio-uring listener adoption).
    #[must_use]
    pub fn from_std(listener: std::net::TcpListener) -> Self {
        RawUringTcpListener {
            fd: listener.into_raw_fd(),
        }
    }
}

impl AsRawFd for RawUringTcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for RawUringTcpListener {
    fn drop(&mut self) {
        // SAFETY: sole ownership of the listener fd, closed exactly once.
        unsafe {
            libc::close(self.fd);
        }
    }
}

// ---------------------------------------------------------------------------
// The Runtime impl.
// ---------------------------------------------------------------------------

/// The raw io_uring runtime backend handle. Zero-sized like the other backends: the per-shard ring
/// lives in the thread-local [`Driver`] installed by [`raw_uring_start`], not in this handle.
#[derive(Debug, Clone, Copy, Default)]
pub struct RawIoUringRuntime;

impl RawIoUringRuntime {
    /// Construct the backend handle.
    #[must_use]
    pub fn new() -> Self {
        RawIoUringRuntime
    }
}

impl Runtime for RawIoUringRuntime {
    type Listener = RawUringTcpListener;
    type Stream = RawUringTcpStream;
    type Buf = Vec<u8>;
    type Error = io::Error;

    async fn accept(
        &self,
        _listener: &Self::Listener,
    ) -> Result<(Self::Stream, SocketAddr), Self::Error> {
        // The raw `Accept`/`Connect` op-futures are sub-slice 1b: this slice proves the reactor +
        // owned-buffer recv/send + cancel-safety, driven by a std-adopted socket pair in the test.
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "raw io_uring accept is a following slice (#682 sub-slice 1b)",
        ))
    }

    async fn connect(&self, _addr: SocketAddr) -> Result<Self::Stream, Self::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "raw io_uring connect is a following slice (#682 sub-slice 1b)",
        ))
    }

    async fn recv(
        &self,
        stream: &mut Self::Stream,
        mut buf: Self::Buf,
    ) -> Result<RecvResult<Self::Buf>, Self::Error> {
        // Owned-buffer APPEND, identical framing to the tokio + tokio-uring backends: read into the
        // buffer's spare capacity STARTING AT its current length.
        let start = IoBuf::len(&buf);
        buf.reserve(READ_WINDOW);
        let cap = buf.capacity();
        // SAFETY: `start <= len <= cap` after the reserve, so `add(start)` is in-bounds of the
        // allocation and `cap - start` bytes there are owned, uninitialized spare capacity. The
        // pointer is into the heap allocation, which does NOT move when `buf` (the Vec header) is
        // moved into the op-future below, so it stays valid for the whole kernel read.
        let ptr = unsafe { buf.as_mut_ptr().add(start) };
        #[allow(clippy::cast_possible_truncation)]
        let len = (cap - start) as u32;
        let sqe = opcode::Read::new(types::Fd(stream.fd), ptr, len).build();
        let (res, mut buf) = OpFuture::submit(sqe, buf).await;
        let n = res?;
        #[allow(clippy::cast_sign_loss)] // `res?` already rejected negatives.
        let n = n as usize;
        // SAFETY: the kernel wrote exactly `n` bytes into `[start, start + n)` (the CQE result IS
        // the byte count), and `start + n <= cap`, so those bytes are now initialized.
        unsafe {
            buf.set_len(start + n);
        }
        Ok(RecvResult { buf, n })
    }

    async fn send(
        &self,
        stream: &mut Self::Stream,
        buf: Self::Buf,
    ) -> Result<Self::Buf, Self::Error> {
        // Owned-buffer write-ALL: resubmit from the last-written offset until every byte is sent,
        // then hand the owned buffer back (symmetric with the other backends' `send`).
        let mut written = 0usize;
        let total = buf.len();
        let mut buf = buf;
        while written < total {
            // SAFETY: `written < total <= len`, so `add(written)` is in-bounds of the initialized
            // prefix; the pointer is into the heap allocation which is stable across the move of
            // `buf` into the op-future, valid for the whole kernel write.
            let ptr = unsafe { buf.as_ptr().add(written) };
            #[allow(clippy::cast_possible_truncation)]
            let len = (total - written) as u32;
            let sqe = opcode::Write::new(types::Fd(stream.fd), ptr, len).build();
            let (res, returned) = OpFuture::submit(sqe, buf).await;
            let n = res?;
            buf = returned;
            #[allow(clippy::cast_sign_loss)] // negatives rejected by `res?`.
            let n = n as usize;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            written += n;
        }
        Ok(buf)
    }

    async fn timer(&self, dur: Duration) -> () {
        // The raw ring runs inside a tokio current-thread runtime with the time driver enabled, so
        // the canonical tokio timer drives the seam's timer -- no io_uring timeout op needed, and no
        // raw `std::time` read (respects the ADR-0003 determinism seam like the other backends).
        tokio::time::sleep(dur).await;
    }

    fn spawn_on_shard<F>(&self, task: F)
    where
        F: Future<Output = ()> + 'static,
    {
        // Pin the task to THIS thread's `LocalSet` (the shard's ring runs there); never `Send`,
        // never migrates across cores (ADR-0002).
        tokio::task::spawn_local(task);
    }
}

// ---------------------------------------------------------------------------
// The per-shard bootstrap: the raw analog of `tokio_uring::start`.
// ---------------------------------------------------------------------------

/// Run `fut` to completion on a fresh raw io_uring runtime pinned to the CURRENT thread: build a
/// tokio current-thread runtime + `LocalSet`, install this thread's [`Driver`] (one ring), spawn the
/// completion-drain task, register the submit-on-park flush, and block on `fut`. This is the raw
/// analog of `tokio_uring::start`; the per-shard serve loop runs inside it (in a later slice). The
/// caller must be on a thread with no other tokio runtime.
///
/// # Panics
///
/// Panics if the io_uring ring cannot be created (e.g. the kernel lacks io_uring or it is disabled);
/// the boot-selection layer probes first (see `crate::uring_probe`) so a real deployment never
/// reaches here on an incapable kernel.
pub fn raw_uring_start<F: Future>(fut: F) -> F::Output {
    // Default (IRQ-driven) ring: completions raise the ring fd's epoll readiness, which is what the
    // `AsyncFd` drain relies on. Do NOT set `IORING_SETUP_IOPOLL` here -- that switches to busy-poll
    // completions with no fd wakeup, which would silently hang every op (the fd never goes readable).
    // The CQ is sized 4x the SQ so a mass-drop burst (every op's own CQE PLUS its cancel's CQE) has
    // headroom before the kernel's overflow backlog engages. Bounding in-flight ops to the CQ (so the
    // backlog is never needed) is a P2 prerequisite when the serve loop sets the per-shard conn cap.
    let ring = IoUring::builder()
        .setup_cqsize(SQ_ENTRIES * 4)
        .build(SQ_ENTRIES)
        .expect("io_uring_setup failed (kernel lacks io_uring or it is disabled)");
    let ring_fd = ring.as_raw_fd();
    let driver = Rc::new(RefCell::new(Driver {
        ring,
        ops: Slab::new(),
    }));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        // Flush pending SQEs to the kernel right before the thread parks in epoll_wait, so no op is
        // left un-submitted (which would hang: the ring fd would never go readable for it). This
        // callback runs on THIS worker thread, where `DRIVER` is installed once `block_on` starts;
        // reach it through the thread-local (a captured `Rc<RefCell<Driver>>` is `!Send`, which the
        // `on_thread_park` bound forbids). Before install (an early park ahead of the first poll)
        // there is nothing to flush -- no-op.
        .on_thread_park(|| {
            DRIVER.with(|cell| {
                if let Some(rc) = cell.borrow().as_ref() {
                    rc.borrow_mut().flush();
                }
            });
        })
        .build()
        .expect("failed to build the raw io_uring current-thread runtime");

    let local = tokio::task::LocalSet::new();
    // Move a CLONE into the block (installed in the thread-local); keep `driver` itself owned out
    // here so the teardown below can drop the LAST `Rc` and trigger `Driver::drop`'s shutdown drain.
    let driver_for_shard = Rc::clone(&driver);
    let out = local.block_on(&rt, async move {
        DRIVER.with(|cell| *cell.borrow_mut() = Some(driver_for_shard));

        // The completion-drain task: park on the ring fd via tokio's epoll; on readable, reap CQEs.
        let async_fd = AsyncFd::new(RingFd(ring_fd)).expect("register ring fd with tokio epoll");
        tokio::task::spawn_local(async move {
            loop {
                let Ok(mut guard) = async_fd.readable().await else {
                    return;
                };
                // Clear readiness BEFORE draining (re-arm epoll): any CQE arriving during/after the
                // drain re-triggers the fd, so no completion is missed.
                guard.clear_ready();
                with_driver(Driver::dispatch_completions);
            }
        });

        fut.await
    });

    // Deterministic teardown, ordered to uphold cancel-safety at shutdown:
    //   1. Drop the `LocalSet` FIRST -- this drops the drain task AND every serve task, so each
    //      in-flight op-future runs its `Drop` (moving its buffer into `Lifecycle::Ignored` and
    //      pushing a best-effort cancel). After this, every remaining slab slot is `Ignored`.
    //   2. Drop the runtime (its reactor is no longer needed; the ring is drained synchronously).
    //   3. Clear the thread-local's `Rc` clone so `driver` below is the SOLE owner -- otherwise the
    //      `Driver` (and its drain-on-`Drop`) would not run until this thread later exits.
    //   4. Drop `driver` -> `Driver::drop` blocks draining every `Ignored` op to its CQE BEFORE the
    //      ring fd and the buffers are freed (no use-after-free, no stranded buffer).
    drop(local);
    drop(rt);
    DRIVER.with(|cell| {
        cell.borrow_mut().take();
    });
    drop(driver);
    out
}

#[cfg(test)]
mod tests {
    use super::{RawIoUringRuntime, RawUringTcpStream, raw_uring_start};
    use crate::Runtime;
    use std::io::{Read, Write};
    use std::os::fd::{FromRawFd, IntoRawFd};

    /// A connected loopback socket PAIR without a listener/accept: bind a std listener, connect to
    /// it from a helper thread, accept the peer, and return the two ends. Lets the recv/send tests
    /// exercise the reactor WITHOUT the raw accept/connect op-futures (sub-slice 1b).
    fn socket_pair() -> (RawUringTcpStream, std::net::TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let (server, _peer) = listener.accept().unwrap();
        // The server end is driven by the raw runtime; the client end stays a plain std socket the
        // test reads/writes synchronously from the main thread.
        let server_fd = server.into_raw_fd();
        // SAFETY: `server_fd` is a freshly-accepted, sole-owned socket fd; adopt it as the raw
        // stream (which becomes its sole owner + closer).
        let raw =
            unsafe { RawUringTcpStream::from_std(std::net::TcpStream::from_raw_fd(server_fd)) };
        (raw, client)
    }

    /// recv APPENDS after a carryover prefix (proving `set_len(start + n)`, not `set_len(n)`), and
    /// send round-trips the owned buffer -- the exact owned-buffer contract the serve loop depends
    /// on, run on a REAL ring under the raw backend.
    #[test]
    fn recv_appends_after_carryover_then_send_round_trips() {
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let (mut server, mut client) = socket_pair();

            // The client writes a request; the raw server recv must APPEND it after a pre-seeded
            // carryover, not overwrite it.
            client.write_all(b"PING\r\n").unwrap();

            let mut buf = Vec::with_capacity(64);
            buf.extend_from_slice(b"CARRY"); // the pre-seeded partial-frame carryover
            let res = rt.recv(&mut server, buf).await.unwrap();
            assert_eq!(res.n, 6, "read the 6 request bytes");
            assert_eq!(
                &res.buf, b"CARRYPING\r\n",
                "recv appended after the carryover (set_len(start + n))"
            );

            // The raw server sends a reply; the client reads it back.
            let reply = rt.send(&mut server, b"+PONG\r\n".to_vec()).await.unwrap();
            assert_eq!(
                reply, b"+PONG\r\n",
                "send handed the owned buffer back unmodified"
            );
            let mut got = [0u8; 7];
            client.read_exact(&mut got).unwrap();
            assert_eq!(&got, b"+PONG\r\n");
        });
    }

    /// CANCEL-SAFETY: a recv future dropped BEFORE its CQE must not free the buffer while the kernel
    /// still references it. Drop a pending recv on an idle socket (no data -> the read never
    /// completes), then run another op on the SAME runtime: no use-after-free / corruption, and the
    /// second op reads correctly. Exercises the `Ignored` lifecycle end to end on a real ring.
    #[test]
    fn recv_dropped_before_completion_is_safe() {
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let (mut server, mut client) = socket_pair();

            // A recv on an empty socket stays pending; a short timeout DROPS the recv future mid-
            // flight (the realistic cancel path). Its buffer must move into `Ignored` and survive
            // until the cancelled read's CQE -- never freed while the kernel still references it.
            let buf = vec![0u8; 4096];
            let timed_out = tokio::time::timeout(
                std::time::Duration::from_millis(30),
                rt.recv(&mut server, buf),
            )
            .await;
            assert!(
                timed_out.is_err(),
                "recv on an empty socket times out (future dropped mid-flight)"
            );

            // Let the reactor process the cancellation CQE (which frees the Ignored buffer).
            rt.timer(std::time::Duration::from_millis(30)).await;

            // A fresh op on the SAME runtime must work: no corrupted slab, no freed-in-flight memory.
            client.write_all(b"OK\r\n").unwrap();
            let res = rt.recv(&mut server, Vec::with_capacity(16)).await.unwrap();
            assert_eq!(
                &res.buf[..res.n],
                b"OK\r\n",
                "the runtime is intact after a cancelled op"
            );
        });
    }

    /// FLUSH-UNDER-LOAD + multi-CQE edge re-arm: saturate the shard with many concurrent in-flight
    /// recvs, then satisfy them all at once. The submit-at-park model must still flush every SQE and
    /// the `AsyncFd` drain must re-arm across a BURST of completions -- if either starved, the joins
    /// below would hang (caught by the nextest per-test timeout). The single-op round-trip tests
    /// never exercise this (they hold at most one op in flight); this is the load path.
    #[test]
    fn many_concurrent_recvs_all_complete_under_load() {
        use std::rc::Rc;
        raw_uring_start(async {
            const N: usize = 64;
            let rt = Rc::new(RawIoUringRuntime::new());
            let mut clients = Vec::with_capacity(N);
            let mut tasks = Vec::with_capacity(N);
            for _ in 0..N {
                let (mut server, client) = socket_pair();
                let rt = Rc::clone(&rt);
                // Each task parks on its own recv -> N ops in flight on the ring simultaneously.
                tasks.push(tokio::task::spawn_local(async move {
                    rt.recv(&mut server, Vec::with_capacity(16))
                        .await
                        .unwrap()
                        .n
                }));
                clients.push(client);
            }
            // Let every task push its recv SQE and park before any data is available.
            tokio::task::yield_now().await;
            // Fire all N writes -> a burst of N concurrent completions the drain must reap.
            for c in &mut clients {
                c.write_all(b"x").unwrap();
            }
            let mut total = 0usize;
            for jh in tasks {
                total += jh.await.unwrap();
            }
            assert_eq!(
                total, N,
                "all concurrent recvs completed (no submit starvation)"
            );
            drop(clients);
        });
    }

    /// TEARDOWN cancel-safety: leave an op genuinely in flight when `raw_uring_start` returns. The
    /// deterministic teardown (drop tasks -> `Ignored`, then `Driver::drop` cancels + drains) must
    /// reap it BEFORE the ring fd and buffers free -- never close the ring under a live kernel op
    /// (use-after-free) and never hang. Reaching the assert proves the shutdown drain converged.
    #[test]
    fn teardown_drains_in_flight_op_without_hang() {
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let (mut server, client) = socket_pair();
            // Keep the peer OPEN (leak the client fd) so the recv cannot complete via EOF -- the only
            // thing that can resolve it is the teardown cancel, precisely exercising `Driver::drop`.
            std::mem::forget(client);
            tokio::task::spawn_local(async move {
                let _ = rt.recv(&mut server, Vec::with_capacity(16)).await;
            });
            // Let the recv push its SQE and park; return with it still in flight.
            tokio::task::yield_now().await;
        });
        // If control reaches here, teardown cancelled + drained the in-flight op with no hang/abort.
    }
}
