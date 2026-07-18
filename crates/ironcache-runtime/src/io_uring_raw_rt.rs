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
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
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

/// A reserved `user_data` for `ProvideBuffers` completions (initial provide + per-buffer re-provide,
/// #513 multishot). Discarded by the drain -- it owns no slot; a failed provide surfaces as a stalled
/// group, caught by the re-arm gate.
const PROVIDE_USER_DATA: u64 = u64::MAX - 1;

/// The tag bit distinguishing a MULTISHOT recv's `user_data` (`MSHOT_TAG | seq`, `seq` monotonic) from
/// a single-shot op's `user_data` (a small slab index). The drain checks `CANCEL`/`PROVIDE` (both
/// near `u64::MAX`) FIRST, then this tag, then falls through to the slab -- so the tag never collides
/// (a slab index never sets bit 62, and the reserved values are matched before the tag test).
const MSHOT_TAG: u64 = 1 << 62;

/// The per-shard multishot provided-buffer group: `MSHOT_NBUFS` buffers of `READ_WINDOW` bytes. One
/// group id per shard ring (only one is used).
const MSHOT_BGID: u16 = 0;
const MSHOT_NBUFS: u16 = 256;

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
    /// Like `Ignored`, but the op's RESULT is a freshly-created fd (a cancelled `accept` that raced a
    /// connection still yields a socket fd in its CQE). On completion `complete` closes that fd if it
    /// is non-negative before dropping the owned resources -- otherwise the accepted socket would leak.
    IgnoredCloseResultFd(Box<dyn Any>),
}

// ---------------------------------------------------------------------------
// The per-shard driver: the ring + the in-flight-op table. Lives in a thread-local `Rc<RefCell<>>`
// so the op-futures (submit / poll / drop) and the drain task all reach the SAME ring on the shard
// thread. `!Send` by construction (the ring and the whole model are thread-per-core, ADR-0002).
// ---------------------------------------------------------------------------
struct Driver {
    ring: IoUring,
    ops: Slab<Lifecycle>,
    /// The multishot recv connections, keyed by their (tagged, monotonic) `user_data`. SEPARATE from
    /// `ops` so the audited single-shot Slab is untouched (#513 design: no index reuse -> no ABA/UAF).
    /// A connection's slot is removed only on the TERMINAL CQE after cancel, never per data CQE.
    mshot: std::collections::HashMap<u64, MultishotConn>,
    /// The per-shard provided-buffer group (lazily created on the first multishot arm; a shard with
    /// only fallback/owned traffic never allocates it).
    pool: Option<MultishotPool>,
    /// Monotonic allocator for multishot `user_data` (never reused within a shard's life -> a stale
    /// CQE for a removed slot cannot alias a live one).
    next_mshot_seq: u64,
    /// Whether this kernel supports the multishot-recv + provided-buffer fast path (probed once at
    /// shard start). False -> `recv_batch` uses the single-shot owned recv (the shipped fallback).
    multishot_ok: bool,
}

/// One connection's multishot-recv state (#513). Lives in `Driver.mshot`, fed by the drain and
/// consumed by `recv_batch`.
struct MultishotConn {
    /// The socket fd, kept so the op can be re-armed after an `F_MORE`-clear / `-ENOBUFS` termination.
    fd: RawFd,
    /// Buffers the kernel filled + handed back (by id + byte length), not yet copied out by a
    /// `recv_batch` call. Bounded by the pool size (a conn cannot hold more than the whole group).
    ready: std::collections::VecDeque<(u16, usize)>,
    /// The parked `recv_batch` future's waker, woken when `ready`/`eof`/`err` changes.
    waker: Option<Waker>,
    /// A `res == 0` CQE landed (clean peer close).
    eof: bool,
    /// A fatal negative result (errno, NOT `-ENOBUFS` which is normal back-pressure).
    err: Option<i32>,
    /// An `F_MORE`-live `RecvMulti` is outstanding (the op is armed). False after a termination CQE,
    /// until `recv_batch` re-arms.
    armed: bool,
    /// An `AsyncCancel` was pushed (connection dropped); the slot is removed on the terminal CQE, and
    /// any buffers it still holds are returned to the group (else the shared group leaks -> shard DoS).
    cancelling: bool,
}

/// The per-shard multishot provided-buffer group: one stable heap slab the kernel writes into, plus a
/// per-buffer "is this buffer currently IN the kernel group" ledger. The kernel picks + returns a
/// specific buffer id per completion; we re-provide that SAME id, so (unlike the one-shot
/// [`crate::buffer_pool::BufferPool`] whose `acquire` picks an arbitrary free id) this tracks the
/// exact in-group set with a double-provide guard.
struct MultishotPool {
    /// `MSHOT_NBUFS * READ_WINDOW` bytes; stable (a `Box<[u8]>` never reallocates) so the pointers the
    /// `ProvideBuffers` SQEs hand the kernel stay valid for the shard's life.
    mem: Box<[u8]>,
    /// `in_group[bid]` is true while buffer `bid` is in the kernel group (available for the kernel to
    /// fill); false while checked out by us (filled, awaiting copy + re-provide).
    in_group: Vec<bool>,
    /// Count of `in_group == true` -- the re-arm gate (an armed `RecvMulti` needs >= 1 buffer to land
    /// into) and the exhaustion signal.
    in_group_count: u16,
}

impl MultishotPool {
    fn new() -> Self {
        let n = MSHOT_NBUFS as usize;
        MultishotPool {
            mem: vec![0u8; n * READ_WINDOW].into_boxed_slice(),
            in_group: vec![true; n], // all provided to the kernel at creation
            in_group_count: MSHOT_NBUFS,
        }
    }

    fn offset(bid: u16) -> usize {
        bid as usize * READ_WINDOW
    }

    /// The kernel pulled buffer `bid` out of the group to fill it (a data CQE): mark it checked-out.
    fn mark_out(&mut self, bid: u16) {
        let slot = &mut self.in_group[bid as usize];
        if *slot {
            *slot = false;
            self.in_group_count -= 1;
        }
    }

    /// We are returning buffer `bid` to the kernel group (a re-provide): mark it in. Returns false if
    /// it was already in-group (a double-provide -- rejected so the kernel is never told to reuse a
    /// buffer twice, which would alias a live read).
    fn mark_in(&mut self, bid: u16) -> bool {
        let slot = &mut self.in_group[bid as usize];
        if *slot {
            return false;
        }
        *slot = true;
        self.in_group_count += 1;
        true
    }

    /// Whether an armed `RecvMulti` has at least one buffer to land into.
    fn has_group_buffer(&self) -> bool {
        self.in_group_count > 0
    }
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
            // Route in reserved-first order (both reserved values sit just below u64::MAX, above every
            // slab index and every `MSHOT_TAG | seq`), then the multishot tag, then the single-shot
            // slab. So the tag test never misfires on a reserved ud.
            if ud == CANCEL_USER_DATA || ud == PROVIDE_USER_DATA {
                continue; // AsyncCancel / ProvideBuffers completion: owns no slot, discard it.
            }
            if ud & MSHOT_TAG != 0 {
                self.complete_multishot(ud, &cqe);
                continue;
            }
            self.complete(ud as usize, cqe);
        }
        count
    }

    // -----------------------------------------------------------------------
    // Multishot recv (#513): a per-shard provided-buffer group + per-connection persistent slots.
    // -----------------------------------------------------------------------

    /// Lazily create the per-shard provided-buffer group (on the first multishot arm) and provide all
    /// its buffers to the kernel. Idempotent. The pool is installed ONLY if the provide SQE was queued
    /// -- otherwise the ledger (`in_group` all true) would lie about a group the kernel never got, so
    /// on a full SQ we leave `pool` None and retry on the next arm.
    fn ensure_pool(&mut self) {
        if self.pool.is_some() {
            return;
        }
        let mut pool = MultishotPool::new();
        // ONE ProvideBuffers SQE hands the whole slab (bids 0..N) to group MSHOT_BGID. `pool.mem` is a
        // stable heap slab (a `Box<[u8]>` that never reallocs; moving `pool` into `self.pool` moves
        // only the Box header, not the heap), so the pointer stays valid for the shard's life.
        let sqe = opcode::ProvideBuffers::new(
            pool.mem.as_mut_ptr(),
            READ_WINDOW as i32,
            MSHOT_NBUFS,
            MSHOT_BGID,
            0,
        )
        .build()
        .user_data(PROVIDE_USER_DATA);
        if self.try_push(&sqe) {
            self.pool = Some(pool);
            // Belt-and-suspenders: if a PRIOR arm was stranded because its `ensure_pool` push failed
            // (SQ+CQ both full -- unreachable at startup, the CQ is empty then), its connection parked
            // un-armed with no pool. Now that the group exists, sweep + re-arm any such conn so it can
            // never stay permanently deaf. A no-op on the common first-arm path (no prior conns).
            self.rearm_all_starved();
        }
    }

    /// Push one SQE, retrying on a FULL submission queue. The whole multishot bookkeeping (mark a
    /// buffer in-group, mark a connection armed) is applied by callers ONLY when this returns `true`,
    /// so the ledger never claims a buffer/op the kernel did not actually receive (the full-SQ ledger-
    /// corruption class). On a full SQ, `submit()` flushes the queued SQEs to the kernel to free
    /// slots; it does NOT reap CQEs, so it never re-enters `dispatch_completions` (no recursion when
    /// called from `complete_multishot`). Returns false only if the retry also fails (SQ *and* CQ both
    /// full -- unreachable in practice, CQ is 4x the SQ + continuously drained).
    fn try_push(&mut self, sqe: &squeue::Entry) -> bool {
        // SAFETY: every `sqe` passed here is a fully-built io_uring op whose referenced memory (a
        // provided-buffer pointer into the stable pool slab, or none for RecvMulti/AsyncCancel)
        // outlives the op; pushing it is always valid.
        if unsafe { self.ring.submission().push(sqe) }.is_ok() {
            return true;
        }
        let _ = self.ring.submit();
        unsafe { self.ring.submission().push(sqe) }.is_ok()
    }

    /// Arm a multishot recv on `fd`: create the pool if needed, allocate a fresh tagged `user_data`,
    /// insert the connection slot (armed IFF the SQE actually went in), and return the `user_data`
    /// (stored on the stream so `recv_batch` finds its slot). If the push was dropped, `armed` is
    /// false and `multishot_pump` re-arms on its next poll (never parks un-armed).
    fn arm_multishot(&mut self, fd: RawFd) -> u64 {
        self.ensure_pool();
        let ud = MSHOT_TAG | self.next_mshot_seq;
        self.next_mshot_seq += 1;
        let armed = self.push_recvmulti(fd, ud);
        self.mshot.insert(
            ud,
            MultishotConn {
                fd,
                ready: std::collections::VecDeque::new(),
                waker: None,
                eof: false,
                err: None,
                armed,
                cancelling: false,
            },
        );
        ud
    }

    /// Push a `RecvMulti` SQE (arm or re-arm). Returns whether it was queued (callers set `armed`
    /// accordingly, so a dropped arm never lies about being in flight -- the armed-but-no-op class).
    fn push_recvmulti(&mut self, fd: RawFd, ud: u64) -> bool {
        let sqe = opcode::RecvMulti::new(types::Fd(fd), MSHOT_BGID)
            .build()
            .user_data(ud);
        self.try_push(&sqe)
    }

    /// Return buffer `bid` to the kernel group. Pushes the one-buffer `ProvideBuffers` FIRST and marks
    /// it in-group ONLY on success, so the ledger never claims a buffer the kernel did not receive.
    /// When this refills a group that was empty, re-arm every connection that parked un-armed waiting
    /// for a buffer (else it stays permanently DEAF -- no in-flight op means no CQE will ever wake it).
    fn reprovide(&mut self, bid: u16) {
        let (already_in, was_empty) = match self.pool.as_ref() {
            Some(p) => (p.in_group[bid as usize], p.in_group_count == 0),
            None => return,
        };
        if already_in {
            return; // already in-group: never double-provide (would alias a live read)
        }
        // SAFETY: `ptr` is within the stable pool slab (a `Box<[u8]>` that never reallocs); the buffer
        // is not in the group, so the kernel is never handed the same buffer twice.
        let ptr = unsafe {
            self.pool
                .as_mut()
                .unwrap()
                .mem
                .as_mut_ptr()
                .add(MultishotPool::offset(bid))
        };
        let sqe = opcode::ProvideBuffers::new(ptr, READ_WINDOW as i32, 1, MSHOT_BGID, bid)
            .build()
            .user_data(PROVIDE_USER_DATA);
        if !self.try_push(&sqe) {
            // SQ+CQ both full (unreachable in practice): leave the buffer checked-out (honest ledger,
            // never double-issued) rather than desyncing the count. It is retried by no one, so this
            // is a rare bounded shrink, not a corruption.
            return;
        }
        self.pool.as_mut().unwrap().mark_in(bid);
        if was_empty {
            self.rearm_all_starved();
        }
    }

    /// Re-arm every connection that is un-armed + not cancelling (its multishot terminated on
    /// exhaustion / churn and it is waiting for a buffer). Called when a `reprovide` refills a
    /// previously-empty group. A re-armed op consumes no buffer until the kernel fills one, so all
    /// starved conns can be armed against a single freed buffer; losers re-terminate on -ENOBUFS and
    /// are swept again on the next reprovide. No explicit wake needed: the re-armed op's next CQE
    /// wakes the parked `recv_batch`.
    fn rearm_all_starved(&mut self) {
        let starved: Vec<u64> = self
            .mshot
            .iter()
            .filter(|(_, c)| !c.armed && !c.cancelling)
            .map(|(ud, _)| *ud)
            .collect();
        for ud in starved {
            self.rearm_if_needed(ud);
        }
    }

    /// Re-arm the multishot recv for `ud` if it terminated (armed == false) AND the group has a buffer
    /// to land into. Sets `armed` only if the SQE actually went in. Called from `recv_batch` after it
    /// re-provides (upholding "never park un-armed") and from `rearm_all_starved` on a group refill.
    fn rearm_if_needed(&mut self, ud: u64) {
        let should = self
            .mshot
            .get(&ud)
            .is_some_and(|c| !c.armed && !c.cancelling)
            && self
                .pool
                .as_ref()
                .is_some_and(MultishotPool::has_group_buffer);
        if !should {
            return;
        }
        let fd = self.mshot.get(&ud).map(|c| c.fd).expect("slot present");
        let armed = self.push_recvmulti(fd, ud);
        if let Some(c) = self.mshot.get_mut(&ud) {
            c.armed = armed;
        }
    }

    /// Route a multishot recv CQE to its connection slot (or reclaim a straggler's buffer).
    fn complete_multishot(&mut self, ud: u64, cqe: &cqueue::Entry) {
        let flags = cqe.flags();
        let res = cqe.result();
        let more = cqueue::more(flags);
        let bid_opt = cqueue::buffer_select(flags);

        // A data CQE (res > 0) means the kernel pulled buffer `bid` OUT of the group to fill it.
        if res > 0 {
            if let (Some(bid), Some(pool)) = (bid_opt, self.pool.as_mut()) {
                pool.mark_out(bid);
            }
        }

        // A straggler for a since-removed (cancelled) slot: return its buffer to the group so the
        // shared group does not leak, then drop the data.
        if !self.mshot.contains_key(&ud) {
            if res > 0 {
                if let Some(bid) = bid_opt {
                    self.reprovide(bid);
                }
            }
            return;
        }

        // Update the connection slot. Extract the wake/terminal decision inside a scope so the `&mut
        // conn` borrow ends before we call `self`-mutating helpers (reprovide).
        let (waker, terminal_bids): (Option<Waker>, Option<Vec<u16>>) = {
            let conn = self.mshot.get_mut(&ud).expect("checked present");
            if !more {
                conn.armed = false; // F_MORE cleared: op terminated (normal / -ENOBUFS / error)
            }
            if res > 0 {
                conn.ready
                    .push_back((bid_opt.expect("data CQE carries a bid"), res as usize));
            } else if res == 0 {
                conn.eof = true;
            } else if res != -libc::ENOBUFS {
                conn.err = Some(-res); // fatal; -ENOBUFS is normal back-pressure (re-arm on refill)
            }
            if conn.cancelling && !conn.armed {
                // Terminal after cancel: reclaim the buffers this slot still holds, drop its waker,
                // and remove the slot. `next_mshot_seq` never reuses `ud`, so a later straggler hits
                // the not-contains_key path above (and reclaims its own bid) -- no ABA.
                let bids: Vec<u16> = conn.ready.drain(..).map(|(b, _)| b).collect();
                conn.waker.take();
                (None, Some(bids))
            } else {
                (conn.waker.take(), None)
            }
        };
        if let Some(bids) = terminal_bids {
            self.mshot.remove(&ud);
            for bid in bids {
                self.reprovide(bid);
            }
            return;
        }
        // Wake the parked recv_batch. Matches the shipped single-shot `complete`: wake INSIDE the
        // driver borrow is safe -- a tokio waker only schedules on the LocalSet, never re-enters.
        if let Some(w) = waker {
            w.wake();
        }
    }

    /// The `recv_batch` step for a multishot connection `ud`: drain all ready buffers into `read_buf`
    /// (the coalescing win), re-provide + re-arm, or report EOF/err, or park. Returns `Poll::Pending`
    /// after storing the waker. NEVER parks while un-armed (re-arms first) -> no lost wakeup.
    fn multishot_pump(
        &mut self,
        ud: u64,
        read_buf: &mut Vec<u8>,
        waker: &Waker,
    ) -> Poll<io::Result<usize>> {
        // Fatal error?
        if let Some(e) = self.mshot.get_mut(&ud).and_then(|c| c.err.take()) {
            return Poll::Ready(Err(io::Error::from_raw_os_error(e)));
        }
        // Drain ALL ready buffers into read_buf (one recv_batch call delivers every queued arrival).
        let ready: Vec<(u16, usize)> = self
            .mshot
            .get_mut(&ud)
            .map(|c| c.ready.drain(..).collect())
            .unwrap_or_default();
        if !ready.is_empty() {
            let mut total = 0usize;
            for (bid, len) in ready {
                {
                    let pool = self.pool.as_ref().expect("pool exists once armed");
                    let off = MultishotPool::offset(bid);
                    read_buf.extend_from_slice(&pool.mem[off..off + len]);
                }
                total += len;
                self.reprovide(bid); // return the buffer, then it may re-arm the op
            }
            self.rearm_if_needed(ud);
            return Poll::Ready(Ok(total));
        }
        // Nothing ready: EOF?
        if self.mshot.get(&ud).is_some_and(|c| c.eof) {
            return Poll::Ready(Ok(0));
        }
        // Ensure armed BEFORE parking (an op that terminated on churn/-ENOBUFS must re-arm now that
        // buffers are back), then park.
        self.rearm_if_needed(ud);
        if let Some(c) = self.mshot.get_mut(&ud) {
            c.waker = Some(waker.clone());
        }
        Poll::Pending
    }

    /// Cancel a connection's multishot op on stream drop: mark it cancelling + push an AsyncCancel.
    /// The slot is removed on the terminal CQE (see `complete_multishot`), reclaiming its buffers.
    fn cancel_multishot(&mut self, ud: u64) {
        let Some(conn) = self.mshot.get_mut(&ud) else {
            return;
        };
        conn.cancelling = true;
        conn.waker = None;
        if !conn.armed {
            // Already terminated + no outstanding op to cancel: reclaim its buffers + remove now.
            let bids: Vec<u16> = conn.ready.drain(..).map(|(b, _)| b).collect();
            self.mshot.remove(&ud);
            for bid in bids {
                self.reprovide(bid);
            }
            return;
        }
        let cancel = opcode::AsyncCancel::new(ud)
            .build()
            .user_data(CANCEL_USER_DATA);
        // Retry on a full SQ so the cancel is not silently dropped (a dropped cancel would strand the
        // op until teardown). AsyncCancel references only a user_data key (no user buffer).
        self.try_push(&cancel);
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
            Lifecycle::IgnoredCloseResultFd(_owned) => {
                // A cancelled accept: if it still yielded a socket fd (res >= 0), close it before the
                // owned sockaddr drops -- otherwise the accepted connection leaks.
                let Lifecycle::Completed(entry) = self.ops.remove(idx) else {
                    unreachable!("slot was just set to Completed")
                };
                let res = entry.result();
                if res >= 0 {
                    // SAFETY: `res` is a fresh, unowned socket fd the kernel just created for the
                    // (cancelled) accept; closing it here is its sole disposition.
                    unsafe {
                        libc::close(res);
                    }
                }
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
        // Cancel in-flight MULTISHOT ops too (#513): they live in `self.mshot`, NOT `self.ops`, so
        // the loop below must also drain them, else the ring fd would close while a multishot recv is
        // still armed against the pool -- the kernel's async release could write into `pool.mem` AFTER
        // it frees (teardown UAF). Mark each cancelling; AsyncCancel the armed ones so their terminal
        // CQE removes the slot (`complete_multishot`); drop the un-armed ones now (their ready buffers
        // were already filled -- the kernel is done with them).
        let mshot_uds: Vec<u64> = self.mshot.keys().copied().collect();
        for ud in mshot_uds {
            let armed = match self.mshot.get_mut(&ud) {
                Some(c) => {
                    c.cancelling = true;
                    c.waker = None;
                    c.armed
                }
                None => continue,
            };
            if armed {
                let cancel = opcode::AsyncCancel::new(ud)
                    .build()
                    .user_data(CANCEL_USER_DATA);
                if unsafe { self.ring.submission().push(&cancel) }.is_err() {
                    let _ = self.ring.submit();
                    let _ = unsafe { self.ring.submission().push(&cancel) };
                }
            } else {
                self.mshot.remove(&ud);
            }
        }

        // Reap to quiescence. `submit_and_wait(1)` submits the queued cancels (and any still-queued
        // original SQEs) and blocks for at least one CQE; `dispatch_completions` frees each `Ignored`
        // slot / removes each cancelled multishot slot on its CQE. The idle-round bound is a paranoia
        // backstop against a slot that never completes -- it caps shutdown work rather than hanging.
        let mut idle_rounds = 0u32;
        while (!self.ops.is_empty() || !self.mshot.is_empty()) && idle_rounds < 4096 {
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
    /// When this future is dropped in flight, whether its op's positive RESULT is a fresh fd the
    /// cancel path must close (true only for `accept`, whose CQE may still carry an accepted socket).
    close_result_fd_on_cancel: bool,
}

impl<B: 'static> OpFuture<B> {
    /// THE SINGLE unsafe SQE-push boundary. Takes the owned resource BY VALUE and builds a future
    /// that owns it until the CQE, so any pointer `sqe` carries into `owned` stays valid for the
    /// whole kernel op. Every op (recv/send/...) submits through here.
    fn submit(sqe: squeue::Entry, owned: B) -> Self {
        Self::submit_inner(sqe, owned, false)
    }

    /// As [`Self::submit`], but on cancel-in-flight the op's positive result is treated as a socket
    /// fd and closed (for `accept`: a cancelled accept can still yield a connection). See
    /// [`Lifecycle::IgnoredCloseResultFd`].
    fn submit_closing_result_fd(sqe: squeue::Entry, owned: B) -> Self {
        Self::submit_inner(sqe, owned, true)
    }

    fn submit_inner(sqe: squeue::Entry, owned: B, close_result_fd_on_cancel: bool) -> Self {
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
            close_result_fd_on_cancel,
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
                Lifecycle::Ignored(_) | Lifecycle::IgnoredCloseResultFd(_) => {
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
                // The CQE already landed but we never polled it. If this op's result is an fd (a
                // raced accept), close it before `owned` drops -- else the accepted socket leaks.
                if self.close_result_fd_on_cancel {
                    let Lifecycle::Completed(entry) = d.ops.remove(idx) else {
                        unreachable!("slot matched Completed")
                    };
                    let res = entry.result();
                    if res >= 0 {
                        // SAFETY: `res` is a fresh, unowned socket fd from the completed accept;
                        // closing it here is its sole disposition.
                        unsafe {
                            libc::close(res);
                        }
                    }
                } else {
                    // `owned` drops here (the kernel is done -- the CQE proves it).
                    d.ops.remove(idx);
                }
            } else {
                // In flight (Submitted / Waiting): the kernel may still touch `owned`'s memory.
                // MOVE `owned` into the slot so it outlives the op; free it when the CQE lands
                // (`complete`'s Ignored arm). Best-effort cancel to hurry the CQE along.
                *slot = if self.close_result_fd_on_cancel {
                    Lifecycle::IgnoredCloseResultFd(Box::new(owned))
                } else {
                    Lifecycle::Ignored(Box::new(owned))
                };
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
    /// The multishot recv `user_data` once `recv_batch` armed one for this connection (#513); `None`
    /// on the single-shot / not-yet-armed path. On drop, an armed multishot op is cancelled.
    mshot_ud: Option<u64>,
}

impl RawUringTcpStream {
    /// Adopt an already-connected std socket (the acceptor hands off accepted sockets by fd, the
    /// same adoption the tokio-uring bootstrap does).
    #[must_use]
    pub fn from_std(stream: std::net::TcpStream) -> Self {
        RawUringTcpStream {
            fd: stream.into_raw_fd(),
            mshot_ud: None,
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
        // Cancel an armed multishot recv BEFORE closing the fd (#513): its persistent slot lives in
        // the Driver keyed by `mshot_ud`, and any buffers it still holds must return to the shared
        // group. Cancel is keyed by user_data (not fd), so closing the fd next is safe -- the kernel
        // terminates the op and `complete_multishot` reclaims its buffers on the terminal CQE.
        if let Some(ud) = self.mshot_ud {
            with_driver(|d| d.cancel_multishot(ud));
        }
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
// Owned op payloads for accept/connect + small socket helpers.
// ---------------------------------------------------------------------------

/// The kernel-written peer address for a raw `accept`: the accept SQE points into `storage`/`len`,
/// which the kernel fills as the op completes. Boxed by the op-future so its address stays stable
/// while the op (and, on cancel, the `Ignored` slot) owns it.
struct AcceptAddr {
    storage: libc::sockaddr_storage,
    len: libc::socklen_t,
}

impl AcceptAddr {
    fn new() -> Self {
        Self {
            // SAFETY: `sockaddr_storage` is plain-old-data; an all-zero value is a valid placeholder
            // the kernel overwrites during accept.
            storage: unsafe { core::mem::zeroed() },
            len: core::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        }
    }

    /// Parse the kernel-filled storage into a `SocketAddr` after the accept CQE lands. Only the IP
    /// families are supported (the listener is a TCP socket); addresses/ports are read out of network
    /// byte order.
    fn to_socket_addr(&self) -> io::Result<SocketAddr> {
        use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
        match i32::from(self.storage.ss_family) {
            libc::AF_INET => {
                // SAFETY: family AF_INET means the kernel wrote a valid `sockaddr_in` into `storage`.
                let sin =
                    unsafe { &*core::ptr::addr_of!(self.storage).cast::<libc::sockaddr_in>() };
                let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
                Ok(SocketAddr::V4(SocketAddrV4::new(
                    ip,
                    u16::from_be(sin.sin_port),
                )))
            }
            libc::AF_INET6 => {
                // SAFETY: family AF_INET6 means the kernel wrote a valid `sockaddr_in6`.
                let sin6 =
                    unsafe { &*core::ptr::addr_of!(self.storage).cast::<libc::sockaddr_in6>() };
                let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
                Ok(SocketAddr::V6(SocketAddrV6::new(
                    ip,
                    u16::from_be(sin6.sin6_port),
                    sin6.sin6_flowinfo,
                    sin6.sin6_scope_id,
                )))
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("accepted peer has unsupported address family {other}"),
            )),
        }
    }
}

/// Owned state for a raw `connect`: the target address the connect SQE reads (stable heap storage)
/// and the client socket, held here so a cancelled connect closes the socket only when its CQE lands
/// -- never while the kernel's connect still references the fd.
struct ConnectState {
    storage: libc::sockaddr_storage,
    len: libc::socklen_t,
    sock: OwnedFd,
}

/// Serialize `addr` into a zeroed `sockaddr_storage`, returning the used length. Ports/addresses are
/// written in network byte order (`to_be` / `octets`), matching what the kernel's connect expects.
fn write_socket_addr(addr: SocketAddr, storage: &mut libc::sockaddr_storage) -> libc::socklen_t {
    match addr {
        SocketAddr::V4(v4) => {
            // SAFETY: `sockaddr_storage` is at least as large + aligned as `sockaddr_in`; we write
            // only the `sockaddr_in` prefix of the zeroed storage.
            let sin =
                unsafe { &mut *core::ptr::addr_of_mut!(*storage).cast::<libc::sockaddr_in>() };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = v4.port().to_be();
            sin.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(v4.ip().octets()),
            };
            core::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(v6) => {
            // SAFETY: as above for the larger `sockaddr_in6`.
            let sin6 =
                unsafe { &mut *core::ptr::addr_of_mut!(*storage).cast::<libc::sockaddr_in6>() };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = v6.port().to_be();
            sin6.sin6_addr = libc::in6_addr {
                s6_addr: v6.ip().octets(),
            };
            sin6.sin6_flowinfo = v6.flowinfo();
            sin6.sin6_scope_id = v6.scope_id();
            core::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    }
}

/// Best-effort `TCP_NODELAY` on a raw fd -- the low-latency default the other backends set at accept
/// and connect. Errors are ignored (a Nagle toggle failure is not fatal to the connection).
fn set_nodelay_raw(fd: RawFd) {
    let one: libc::c_int = 1;
    // SAFETY: `fd` is a live connected socket; `setsockopt` reads `size_of::<c_int>` bytes at `&one`.
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            core::ptr::addr_of!(one).cast::<libc::c_void>(),
            core::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
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
        listener: &Self::Listener,
    ) -> Result<(Self::Stream, SocketAddr), Self::Error> {
        // The kernel writes the peer address into `owned.storage` and its length into `owned.len`
        // during the op, so `owned` (a heap Box) must outlive the op -- exactly what `OpFuture`
        // guarantees. `submit_closing_result_fd` ensures a CANCELLED accept that still produced a
        // socket fd closes it rather than leaking (the fd rides the CQE, not `owned`).
        let mut owned = Box::new(AcceptAddr::new());
        let sa_ptr = core::ptr::addr_of_mut!(owned.storage).cast::<libc::sockaddr>();
        let len_ptr = core::ptr::addr_of_mut!(owned.len);
        // `SOCK_CLOEXEC`: the accepted DATA socket must not survive an `exec` (the streamed live
        // cutover #391 re-execs the server); only the inherited listen fd should. The std/tokio
        // siblings get this via `accept4(SOCK_CLOEXEC)`, and socket2 sets it on the connect socket.
        let sqe = opcode::Accept::new(types::Fd(listener.fd), sa_ptr, len_ptr)
            .flags(libc::SOCK_CLOEXEC)
            .build();
        let (res, owned) = OpFuture::submit_closing_result_fd(sqe, owned).await;
        let fd = res?;
        // Adopt the accepted fd into an OWNING stream BEFORE the fallible peer parse: if
        // `to_socket_addr` errors (a non-IP family), the early return then closes the fd via the
        // stream's Drop rather than leaking it. Match the other backends: disable Nagle.
        let stream = RawUringTcpStream { fd, mshot_ud: None };
        let peer = owned.to_socket_addr()?;
        set_nodelay_raw(stream.fd);
        Ok((stream, peer))
    }

    async fn connect(&self, addr: SocketAddr) -> Result<Self::Stream, Self::Error> {
        // Create the client socket up front and OWN it inside the op (`ConnectState.sock`): if the
        // connect future is cancelled in flight, the socket closes only when the (cancelled) CQE
        // lands, never while the kernel's connect still references it. The target address is likewise
        // owned so the SQE's pointer into it stays valid for the whole op.
        let domain = socket2::Domain::for_address(addr);
        let sock =
            socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
        // Adopt the socket as an OwnedFd so it closes exactly once -- and, held inside `ConnectState`,
        // only after the (possibly cancelled) connect's CQE lands.
        // SAFETY: `into_raw_fd` transferred sole ownership of the socket fd to us.
        let sock = unsafe { OwnedFd::from_raw_fd(sock.into_raw_fd()) };
        let fd = sock.as_raw_fd();
        let mut owned = Box::new(ConnectState {
            storage: unsafe { core::mem::zeroed() },
            len: 0,
            sock,
        });
        owned.len = write_socket_addr(addr, &mut owned.storage);
        let sa_ptr = core::ptr::addr_of!(owned.storage).cast::<libc::sockaddr>();
        let sa_len = owned.len;
        let sqe = opcode::Connect::new(types::Fd(fd), sa_ptr, sa_len).build();
        let (res, owned) = OpFuture::submit(sqe, owned).await;
        res?; // a successful connect completes with res == 0
        // Transfer sole ownership of the connected fd from the op state to the stream.
        let connected_fd = owned.sock.into_raw_fd();
        set_nodelay_raw(connected_fd);
        Ok(RawUringTcpStream {
            fd: connected_fd,
            mshot_ud: None,
        })
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
    // Probe the multishot-recv + provided-buffer fast path ONCE per shard (#513). Unsupported (older
    // kernel) -> `recv_batch` uses the shipped single-shot owned recv.
    let multishot_ok = crate::uring_probe::probe_uring_caps().is_ok_and(|caps| {
        matches!(
            crate::uring_probe::select_datapath(caps),
            crate::uring_probe::DataPath::MultishotProvided
        )
    });
    let driver = Rc::new(RefCell::new(Driver {
        ring,
        ops: Slab::new(),
        mshot: std::collections::HashMap::new(),
        pool: None,
        next_mshot_seq: 0,
        multishot_ok,
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

// ---------------------------------------------------------------------------
// Stream socket-option / address helpers (the raw analogs of the tokio-uring backend's
// `set_keepalive_uring` / `peer_local_addrs`). `RawUringTcpStream` exposes only its fd, so borrow it
// through a temporary std stream and `into_raw_fd` it back to AVOID closing the fd the raw stream
// still owns. These live here (not in `ironcache`) because the borrow needs `unsafe`, which the
// `ironcache` crate forbids.
// ---------------------------------------------------------------------------

/// Apply `SO_KEEPALIVE` (idle `secs`; `0` DISABLES) to a raw stream at accept, without taking
/// ownership of its fd. Mirrors [`crate::io_uring_rt::set_keepalive_uring`]. Errors ignored.
pub fn set_keepalive_raw(stream: &RawUringTcpStream, secs: u64) {
    let raw = stream.fd;
    // SAFETY: `raw` is the valid open TCP socket fd owned by `stream` for this borrow; the std
    // stream is `into_raw_fd`'d back so it does NOT close the fd the raw stream still owns.
    let s = unsafe { std::net::TcpStream::from_raw_fd(raw) };
    {
        let sock = socket2::SockRef::from(&s);
        if secs == 0 {
            let _ = sock.set_keepalive(false);
        } else {
            let _ = sock.set_keepalive(true);
            let _ = sock.set_tcp_keepalive(
                &socket2::TcpKeepalive::new().with_time(std::time::Duration::from_secs(secs)),
            );
        }
    }
    let _ = s.into_raw_fd();
}

/// Read the `(peer, local)` addresses of a raw stream as display strings (for `CLIENT INFO`),
/// without taking ownership of its fd. Mirrors [`crate::io_uring_rt::peer_local_addrs`]. A failure
/// yields an empty string (the addresses are cosmetic).
#[must_use]
pub fn peer_local_addrs_raw(stream: &RawUringTcpStream) -> (String, String) {
    let raw = stream.fd;
    // SAFETY: as in `set_keepalive_raw` -- borrow only, `into_raw_fd` relinquishes without closing.
    let s = unsafe { std::net::TcpStream::from_raw_fd(raw) };
    let peer = s.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    let local = s.local_addr().map(|a| a.to_string()).unwrap_or_default();
    let _ = s.into_raw_fd();
    (peer, local)
}

/// The owned resource for an in-flight `writev` (#515 P1): the iovec ARRAY (whose base pointer the
/// SQE carries) PLUS the buffers the iovecs point into. Held by the `OpFuture` until the CQE, and
/// moved into [`Lifecycle::Ignored`] if the future is dropped in flight, so BOTH the array and the
/// buffers outlive the kernel op -- cancel-safe exactly like the single-buffer [`send`]. Moving this
/// struct moves the OUTER `Vec`s' control blocks but not their heap allocations, so every iovec base
/// pointer (into `bufs`' inner allocations) and the SQE's iovec-array pointer stay valid.
struct OwnedWritev {
    iovecs: Vec<libc::iovec>,
    bufs: Vec<Vec<u8>>,
}

/// `writev(2)`/`IORING_OP_WRITEV` caps the iovec count at `UIO_MAXIOV` (1024); a longer array fails
/// `EINVAL`. `send_vectored` builds at most this many iovecs per submission and resubmits for the
/// remainder from the new write offset, so an arbitrarily long segment list is written correctly.
const WRITEV_IOV_MAX: usize = 1024;

/// Build the iovec array for the UNWRITTEN suffix `[skip, total)` of `bufs`: skip buffers already
/// fully written, and for the partially-written buffer point PAST its consumed prefix. Empty buffers
/// are dropped (a zero-length iovec is pointless). Capped at [`WRITEV_IOV_MAX`] iovecs (the caller's
/// loop resubmits for anything beyond). The returned iovec base pointers reference `bufs`' inner heap
/// allocations, which are STABLE across moving the outer `bufs` `Vec` into the op-future.
fn build_writev_iovecs(bufs: &[Vec<u8>], skip: usize) -> Vec<libc::iovec> {
    let mut iovecs = Vec::with_capacity(bufs.len().min(WRITEV_IOV_MAX));
    let mut acc = 0usize;
    for b in bufs {
        let blen = b.len();
        if blen == 0 || acc + blen <= skip {
            acc += blen;
            continue;
        }
        if iovecs.len() == WRITEV_IOV_MAX {
            break;
        }
        // `start` = bytes of THIS buffer already written: `skip - acc` while `acc < skip <= acc+blen`,
        // else 0 (we are past the write cursor). `start < blen` always (the `<= skip` skip above
        // handled fully-written buffers), so `add(start)` is in-bounds.
        let start = skip.saturating_sub(acc);
        // SAFETY: `start < blen`, so `b.as_ptr().add(start)` is within `b`'s initialized allocation.
        // The kernel only READS these bytes (a write op). The allocation outlives the op: `OwnedWritev`
        // owns `bufs` until the CQE (moved into `Lifecycle::Ignored` on cancel).
        let base = unsafe { b.as_ptr().add(start) };
        iovecs.push(libc::iovec {
            iov_base: base.cast::<core::ffi::c_void>().cast_mut(),
            iov_len: blen - start,
        });
        acc += blen;
    }
    iovecs
}

/// The owned resource for an in-flight zero-copy SPLICE `writev` (#515): the iovec ARRAY plus the
/// `out` scratch buffer some iovecs point into. Held by the `OpFuture` until the CQE (moved into
/// [`Lifecycle::Ignored`] on drop-in-flight), so `out` + the array outlive the op. The value-pointer
/// iovecs reference caller-PINNED store memory this struct does NOT own -- the caller's fence keeps
/// those valid until the CQE (`send_zc`'s safety contract), so a dropped op is sound for what it owns.
struct OwnedZcWritev {
    iovecs: Vec<libc::iovec>,
    out: Vec<u8>,
}

/// Build the iovec array for the UNWRITTEN suffix `[skip, total)` of the logical splice
/// `out[0..at1] + val1 + out[at1..at2] + val2 + ... + out[at_n..]`, capped at [`WRITEV_IOV_MAX`]. Each
/// segment (an `out` slice or a pinned `ZcInsert`) is emitted in order; a segment straddling `skip`
/// is trimmed to its unwritten tail. `inserts` must be sorted by `at` ascending with `at <= out.len()`.
fn build_zc_iovecs(out: &[u8], inserts: &[crate::ZcInsert], skip: usize) -> Vec<libc::iovec> {
    let mut iovecs = Vec::with_capacity((inserts.len() * 2 + 1).min(WRITEV_IOV_MAX));
    let mut logical = 0usize;
    // Emit the sub-range of segment `[base, base+len)` that lies past `skip` (an `out` slice or a
    // pinned value), advancing the logical cursor. Capped at WRITEV_IOV_MAX (the send loop resubmits).
    let mut emit = |base: *const u8, len: usize| {
        if len == 0 {
            return;
        }
        let seg_start = logical;
        logical += len;
        if logical <= skip || iovecs.len() >= WRITEV_IOV_MAX {
            return;
        }
        // `start_off` = bytes of this segment already written: `skip - seg_start` while the segment
        // straddles `skip`, else 0. `start_off < len` (the `logical <= skip` guard dropped
        // fully-written segments), so `add(start_off)` is in-bounds.
        let start_off = skip.saturating_sub(seg_start);
        // SAFETY: `start_off < len`, so `base.add(start_off)` is within the segment's allocation (an
        // `out` slice, or a pinned region the caller keeps valid per `send_zc`'s contract). The kernel
        // only READS these bytes (a write op).
        let p = unsafe { base.add(start_off) };
        iovecs.push(libc::iovec {
            iov_base: p.cast::<core::ffi::c_void>().cast_mut(),
            iov_len: len - start_off,
        });
    };
    let mut out_pos = 0usize;
    for ins in inserts {
        emit(out[out_pos..].as_ptr(), ins.at - out_pos); // framing slice out[out_pos..at]
        out_pos = ins.at;
        emit(ins.ptr, ins.len); // the pinned value
    }
    emit(out[out_pos..].as_ptr(), out.len() - out_pos); // trailing framing out[out_pos..]
    iovecs
}

/// The raw backend's [`crate::BatchedRecvSend`]: MULTISHOT recv over a provided-buffer group (#513)
/// when the kernel supports it, else the plain owned recv (the shipped fallback). Both APPEND into
/// `read_buf` and return the byte count (`0` = clean peer close); `send_batch` writes all + hands the
/// buffer back.
impl crate::BatchedRecvSend for RawIoUringRuntime {
    async fn recv_batch(
        &self,
        stream: &mut RawUringTcpStream,
        read_buf: &mut Vec<u8>,
    ) -> io::Result<usize> {
        // Fast path: multishot recv over the shared provided-buffer group. Arm ONCE per connection
        // (on the first call), then each call drains all buffers the kernel has delivered so far.
        if with_driver(|d| d.multishot_ok) {
            let ud = if let Some(ud) = stream.mshot_ud {
                ud
            } else {
                let ud = with_driver(|d| d.arm_multishot(stream.fd));
                stream.mshot_ud = Some(ud);
                ud
            };
            return core::future::poll_fn(|cx| {
                with_driver(|d| d.multishot_pump(ud, read_buf, cx.waker()))
            })
            .await;
        }
        // Fallback (older kernel, no multishot): single-shot owned recv into a FRESH buffer, then
        // APPEND. This is DROP-SAFE -- the subscriber idle-wait's `select!` can cancel this recv
        // without losing `read_buf`'s partial-frame carryover (a `mem::take(read_buf)` would strand
        // the carryover in the cancelled op). One copy on this cold path; the fast paths never take it.
        let res = self.recv(stream, Vec::new()).await?;
        read_buf.extend_from_slice(&res.buf[..res.n]);
        Ok(res.n)
    }

    async fn send_batch(
        &self,
        stream: &mut RawUringTcpStream,
        data: Vec<u8>,
    ) -> io::Result<Vec<u8>> {
        self.send(stream, data).await
    }

    /// SCATTER-GATHER write via a real `IORING_OP_WRITEV` (#515 P1): write the ordered `bufs` as ONE
    /// logical reply with NO concatenation -- the kernel gathers the segments directly. Write-ALL:
    /// resubmit from the running byte offset until every byte is sent (a short writev advances the
    /// offset and the next submission rebuilds the iovecs for the remaining suffix, also handling the
    /// `UIO_MAXIOV` cap). The iovec array + the buffers are owned by the `OpFuture` for the whole op
    /// (cancel-safe like [`send`]). Hands `bufs` back for reuse.
    async fn send_vectored(
        &self,
        stream: &mut RawUringTcpStream,
        bufs: Vec<Vec<u8>>,
    ) -> io::Result<Vec<Vec<u8>>> {
        let total: usize = bufs.iter().map(Vec::len).sum();
        if total == 0 {
            return Ok(bufs);
        }
        let mut bufs = bufs;
        let mut written = 0usize;
        while written < total {
            let iovecs = build_writev_iovecs(&bufs, written);
            debug_assert!(
                !iovecs.is_empty(),
                "written < total implies a non-empty unwritten suffix"
            );
            let owned = OwnedWritev { iovecs, bufs };
            let iov_ptr = owned.iovecs.as_ptr();
            #[allow(clippy::cast_possible_truncation)] // capped at WRITEV_IOV_MAX (1024).
            let iov_len = owned.iovecs.len() as u32;
            let sqe = opcode::Writev::new(types::Fd(stream.fd), iov_ptr, iov_len).build();
            let (res, owned) = OpFuture::submit(sqe, owned).await;
            bufs = owned.bufs;
            let n = res?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            #[allow(clippy::cast_sign_loss)] // negatives rejected by `res?`.
            let n = n as usize;
            written += n;
        }
        Ok(bufs)
    }

    /// ZERO-COPY SPLICE write via a real `IORING_OP_WRITEV` (#515): send the logical splice of `out`
    /// with the pinned `inserts` (see [`crate::BatchedRecvSend::send_zc`]) as ONE vectored write, with
    /// the value bytes written straight from store memory (no copy into `out`). Write-ALL: resubmit
    /// from the running offset across short writevs + the `UIO_MAXIOV` cap. The op owns `out` + the
    /// iovec array (cancel-safe); the pinned value regions are the caller's contract (kept valid until
    /// the CQE by the store fence). Empty `inserts` is exactly [`Self::send`] (byte-identical). The
    /// pinned-region precondition (each `ZcInsert`'s `(ptr, len)` valid until the CQE, incl. on cancel)
    /// is the caller's, upheld structurally by the store fence -- see
    /// [`crate::BatchedRecvSend::send_zc`] for why this is a safe fn with a precondition, not `unsafe`.
    async fn send_zc(
        &self,
        stream: &mut RawUringTcpStream,
        out: Vec<u8>,
        inserts: Vec<crate::ZcInsert>,
    ) -> io::Result<Vec<u8>> {
        if inserts.is_empty() {
            return self.send(stream, out).await;
        }
        let total: usize = out.len() + inserts.iter().map(|i| i.len).sum::<usize>();
        if total == 0 {
            return Ok(out);
        }
        let mut out = out;
        let mut written = 0usize;
        while written < total {
            let iovecs = build_zc_iovecs(&out, &inserts, written);
            debug_assert!(
                !iovecs.is_empty(),
                "written < total implies a non-empty unwritten suffix"
            );
            let owned = OwnedZcWritev { iovecs, out };
            let iov_ptr = owned.iovecs.as_ptr();
            #[allow(clippy::cast_possible_truncation)] // capped at WRITEV_IOV_MAX (1024).
            let iov_len = owned.iovecs.len() as u32;
            let sqe = opcode::Writev::new(types::Fd(stream.fd), iov_ptr, iov_len).build();
            let (res, owned) = OpFuture::submit(sqe, owned).await;
            out = owned.out;
            let n = res?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            #[allow(clippy::cast_sign_loss)] // negatives rejected by `res?`.
            let n = n as usize;
            written += n;
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Per-shard RAW io_uring bootstrap (the raw sibling of `io_uring_rt::run_shards_uring`).
// ---------------------------------------------------------------------------

pub use raw_uring_bootstrap::run_shards_raw_uring;

mod raw_uring_bootstrap {
    use super::{RawIoUringRuntime, RawUringTcpStream, raw_uring_start};
    use crate::bootstrap::{ShardConfig, ShardId, ShardSet};
    use crate::tokio_rt::listener_for;
    use std::cell::Cell;
    use std::future::Future;
    use std::os::fd::{FromRawFd, IntoRawFd};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    // NOTE: the acceptor loop / live-task drain here are duplicated from `io_uring_rt`'s
    // `uring_bootstrap` (they carry no backend types -- pure std + tokio-time). They are duplicated
    // rather than shared so the `io_uring_raw` feature builds WITHOUT the `io_uring` feature (whose
    // module is cfg'd out then). Factoring the shared acceptor into an `any(io_uring, io_uring_raw)`
    // module is a follow-up.

    type LiveTasks = Rc<Cell<usize>>;

    struct LiveGuard(LiveTasks);
    impl Drop for LiveGuard {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_sub(1));
        }
    }

    /// Run the shard set on the RAW io_uring backend (Linux, `runtime = io_uring_raw`, PLAINTEXT).
    /// Identical topology to [`crate::io_uring_rt::run_shards_uring`] -- one bound listener + a
    /// single userspace acceptor thread round-robining accepted std streams to per-shard channels --
    /// but each shard thread runs [`raw_uring_start`] (the raw ring reactor) instead of
    /// `tokio_uring::start`, and adopts each connection via [`RawUringTcpStream::from_std`].
    pub fn run_shards_raw_uring<S, Fut, I, D, DFut>(
        cfg: &ShardConfig,
        serve: S,
        inboxes: Vec<I>,
        drain: D,
    ) -> std::io::Result<ShardSet>
    where
        S: Fn(RawIoUringRuntime, RawUringTcpStream, ShardId, Arc<AtomicBool>) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = ()> + 'static,
        I: Send + 'static,
        D: Fn(usize, I, Arc<AtomicBool>) -> DFut + Clone + Send + 'static,
        DFut: Future<Output = ()> + 'static,
    {
        let shutdown = Arc::new(AtomicBool::new(false));
        let total = cfg.shards.max(1);
        assert_eq!(
            inboxes.len(),
            total,
            "run_shards_raw_uring: one inbox per shard required (got {}, need {total})",
            inboxes.len()
        );

        let listener = listener_for(cfg.bind)?;

        let mut conn_senders = Vec::with_capacity(total);
        let mut conn_receivers = Vec::with_capacity(total);
        for _ in 0..total {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<std::net::TcpStream>();
            conn_senders.push(tx);
            conn_receivers.push(rx);
        }

        let mut handles = Vec::with_capacity(total + 1);

        {
            let shutdown = Arc::clone(&shutdown);
            let acceptor = std::thread::Builder::new()
                .name("ironcache-acceptor-raw-uring".to_string())
                .spawn(move || acceptor_loop(&listener, &conn_senders, &shutdown))?;
            handles.push(acceptor);
        }

        for ((index, inbox), conn_rx) in inboxes.into_iter().enumerate().zip(conn_receivers) {
            let shutdown = Arc::clone(&shutdown);
            let drain_shutdown = Arc::clone(&shutdown);
            let serve = serve.clone();
            let drain = drain.clone();
            let shard = ShardId { index, total };
            let handle = std::thread::Builder::new()
                .name(format!("ironcache-shard-raw-uring-{index}"))
                .spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        // ONE raw io_uring runtime per shard thread (one ring per shard); all of the
                        // shard's tasks run on its `LocalSet`, interleaved never parallel (ADR-0002).
                        raw_uring_start(async move {
                            let drain_task =
                                tokio::task::spawn_local(drain(index, inbox, drain_shutdown));
                            serve_loop(conn_rx, &serve, shard, &shutdown).await;
                            let drain_grace =
                                tokio::time::sleep(crate::bootstrap::active_drain_grace());
                            tokio::pin!(drain_grace);
                            tokio::select! {
                                _ = drain_task => {}
                                () = &mut drain_grace => {
                                    eprintln!(
                                        "shard {index} (raw io_uring): drain task did not finish \
                                         within the grace window; proceeding with shutdown"
                                    );
                                }
                            }
                        });
                    }));
                    if let Err(panic) = result {
                        let shard_died: u64 = 1;
                        eprintln!(
                            "shard {index} (raw io_uring): serve loop panicked \
                             (shard_died={shard_died}); shard thread exiting"
                        );
                        std::panic::resume_unwind(panic);
                    }
                })?;
            handles.push(handle);
        }

        Ok(ShardSet::from_parts(shutdown, handles))
    }

    /// The single acceptor's loop (a copy of the tokio/io_uring bootstrap's: a blocking std accept
    /// with a non-blocking shutdown poll, round-robining to shard channels).
    fn acceptor_loop(
        listener: &std::net::TcpListener,
        conn_senders: &[tokio::sync::mpsc::UnboundedSender<std::net::TcpStream>],
        shutdown: &Arc<AtomicBool>,
    ) {
        if let Err(e) = listener.set_nonblocking(true) {
            eprintln!(
                "acceptor (raw io_uring): set_nonblocking failed: {e}; shutdown may be delayed"
            );
        }
        let poll = Duration::from_millis(1);
        let mut next: usize = 0;
        let n = conn_senders.len().max(1);
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _peer)) => {
                    let _ = stream.set_nodelay(true);
                    let target = next % n;
                    next = next.wrapping_add(1);
                    if let Err(e) = conn_senders[target].send(stream) {
                        eprintln!("acceptor (raw io_uring): shard {target} channel closed: {e}");
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(poll);
                }
                Err(e) => {
                    eprintln!("acceptor (raw io_uring): accept error: {e}");
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }

    /// The shard's serve loop: await std streams on the channel, adopt each onto THIS shard's raw
    /// ring via [`RawUringTcpStream::from_std`], and spawn `serve` per connection on the LocalSet.
    async fn serve_loop<S, Fut>(
        mut conn_rx: tokio::sync::mpsc::UnboundedReceiver<std::net::TcpStream>,
        serve: &S,
        shard: ShardId,
        shutdown: &Arc<AtomicBool>,
    ) where
        S: Fn(RawIoUringRuntime, RawUringTcpStream, ShardId, Arc<AtomicBool>) -> Fut
            + Clone
            + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let live: LiveTasks = Rc::new(Cell::new(0));

        while !shutdown.load(Ordering::Relaxed) {
            tokio::select! {
                maybe = conn_rx.recv() => {
                    match maybe {
                        Some(std_stream) => {
                            // Adopt onto THIS shard's ring. Round-trip the fd so exactly one owner
                            // closes it (the raw stream); the socket's non-blocking flag is
                            // irrelevant to io_uring submissions (the ring drives readiness).
                            let raw = std_stream.into_raw_fd();
                            // SAFETY: `raw` is a valid open TCP socket fd just handed off by the
                            // acceptor and retained nowhere else; the rewrap transfers sole
                            // ownership to the raw stream.
                            let std_stream = unsafe { std::net::TcpStream::from_raw_fd(raw) };
                            let stream = RawUringTcpStream::from_std(std_stream);
                            let fut = serve(
                                RawIoUringRuntime::new(),
                                stream,
                                shard,
                                Arc::clone(shutdown),
                            );
                            live.set(live.get() + 1);
                            let guard = LiveGuard(Rc::clone(&live));
                            tokio::task::spawn_local(async move {
                                let _guard = guard;
                                fut.await;
                            });
                        }
                        None => break,
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }

        drain_live_tasks(&live, shard).await;
    }

    /// Await the shard's in-flight connection tasks until zero or the grace window elapses.
    async fn drain_live_tasks(live: &LiveTasks, shard: ShardId) {
        if live.get() == 0 {
            return;
        }
        let deadline = tokio::time::Instant::now() + crate::bootstrap::active_drain_grace();
        let tick = Duration::from_millis(20);
        while live.get() > 0 {
            if tokio::time::Instant::now() >= deadline {
                eprintln!(
                    "shard {} (raw io_uring): drain grace elapsed with {} connection task(s) still \
                     live; proceeding with shutdown",
                    shard.index,
                    live.get()
                );
                break;
            }
            tokio::time::sleep(tick).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RawIoUringRuntime, RawUringTcpListener, RawUringTcpStream, raw_uring_start, with_driver,
    };
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

    /// #515 P1: `send_vectored` GATHERS an ordered segment list into ONE reply via a real `writev`
    /// (no concatenation); the peer reads the exact byte-concatenation. An EMPTY segment is dropped
    /// (not sent as a 0-len iovec). This is the framing + value shape the zero-copy GET flush builds.
    #[test]
    fn send_vectored_gathers_segments() {
        use crate::BatchedRecvSend;
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let (mut server, mut client) = socket_pair();

            let segs: Vec<Vec<u8>> = vec![
                b"$5\r\n".to_vec(), // bulk header (scratch)
                b"hello".to_vec(),  // the "value" segment
                Vec::new(),         // empty -> dropped
                b"\r\n".to_vec(),   // bulk trailer (scratch)
                b"+OK\r\n".to_vec(),
            ];
            let expected: Vec<u8> = segs.iter().flatten().copied().collect();

            let sent = rt.send_vectored(&mut server, segs).await.unwrap();
            assert_eq!(
                sent.iter().flatten().copied().collect::<Vec<u8>>(),
                expected,
                "segments handed back for reuse"
            );

            let mut got = vec![0u8; expected.len()];
            client.read_exact(&mut got).unwrap();
            assert_eq!(got, expected, "peer read the exact gathered concatenation");
        });
    }

    /// #515 P1: the write-ALL loop resubmits across SHORT writevs (a payload larger than the socket
    /// buffer) AND across the `WRITEV_IOV_MAX` (1024) iovec cap (2000 segments), delivering every byte
    /// in order. A reader thread drains concurrently so the single-threaded ring keeps making progress.
    #[test]
    fn send_vectored_writes_all_across_short_writevs_and_iov_cap() {
        use crate::BatchedRecvSend;
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let (mut server, client) = socket_pair();

            let nseg = 2000usize; // > WRITEV_IOV_MAX (1024): forces multiple writev submissions.
            let seg_len = 1024usize; // ~2 MiB total: exceeds the socket buffer -> short writevs.
            let segs: Vec<Vec<u8>> = (0..nseg)
                .map(|i| vec![u8::try_from(i % 256).unwrap(); seg_len])
                .collect();
            let total = nseg * seg_len;
            let expected: Vec<u8> = segs.iter().flatten().copied().collect();

            // Drain the client end concurrently; `read_exact(total)` returns once all bytes arrive.
            let reader = std::thread::spawn(move || {
                let mut client = client;
                let mut got = vec![0u8; total];
                client.read_exact(&mut got).unwrap();
                got
            });

            let sent = rt.send_vectored(&mut server, segs).await.unwrap();
            assert_eq!(sent.iter().map(Vec::len).sum::<usize>(), total);

            let got = reader.join().unwrap();
            assert_eq!(got.len(), total);
            assert_eq!(
                got, expected,
                "every byte written in order across short writevs + the IOV_MAX cap"
            );
        });
    }

    /// #515 send_zc: SPLICE pinned value regions into the `out` framing buffer and write the logical
    /// concatenation via one `writev` -- the value bytes go straight from their (pinned) memory to the
    /// socket, never copied into `out`. Two inserts (a GET-hit shape + a second reply) prove the
    /// interleave order; the pinned buffers stay alive for the whole send.
    #[test]
    fn send_zc_splices_pinned_values_into_the_framing() {
        use crate::{BatchedRecvSend, ZcInsert};
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let (mut server, mut client) = socket_pair();

            // Pinned values (kept alive by these locals for the whole send).
            let v1 = b"hello".to_vec();
            let v2 = b"worldworld".to_vec();
            // out = framing: `$5\r\n` | (splice v1) | `\r\n$10\r\n` | (splice v2) | `\r\n`.
            let mut out = Vec::new();
            out.extend_from_slice(b"$5\r\n");
            let at1 = out.len();
            out.extend_from_slice(b"\r\n$10\r\n");
            let at2 = out.len();
            out.extend_from_slice(b"\r\n");
            let inserts = vec![
                ZcInsert {
                    at: at1,
                    ptr: v1.as_ptr(),
                    len: v1.len(),
                },
                ZcInsert {
                    at: at2,
                    ptr: v2.as_ptr(),
                    len: v2.len(),
                },
            ];
            let expected = b"$5\r\nhello\r\n$10\r\nworldworld\r\n";

            // Precondition: v1/v2 (the pinned regions) outlive the whole send below.
            let returned = rt.send_zc(&mut server, out, inserts).await.unwrap();
            assert_eq!(
                returned, b"$5\r\n\r\n$10\r\n\r\n",
                "out (framing only) handed back for reuse"
            );

            let mut got = vec![0u8; expected.len()];
            client.read_exact(&mut got).unwrap();
            assert_eq!(
                got, expected,
                "pinned values spliced into the framing in order"
            );
            drop((v1, v2));
        });
    }

    /// #515 send_zc: empty `inserts` is byte-identical to a plain `send` of `out`; and a large spliced
    /// payload writes ALL bytes in order across short writevs (concurrent reader drains).
    #[test]
    fn send_zc_empty_inserts_and_write_all() {
        use crate::{BatchedRecvSend, ZcInsert};
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let (mut server, mut client) = socket_pair();
            // Empty inserts -> plain send of `out`.
            let out = rt
                .send_zc(&mut server, b"+PONG\r\n".to_vec(), Vec::new())
                .await
                .unwrap();
            assert_eq!(out, b"+PONG\r\n");
            let mut got = [0u8; 7];
            client.read_exact(&mut got).unwrap();
            assert_eq!(&got, b"+PONG\r\n");

            // Large spliced payload: a big pinned value + framing, exceeding the socket buffer.
            let value = vec![7u8; 3 * 1024 * 1024]; // ~3 MiB pinned value
            let mut out2 = Vec::new();
            out2.extend_from_slice(b"$3145728\r\n");
            let at = out2.len();
            out2.extend_from_slice(b"\r\n");
            let inserts = vec![ZcInsert {
                at,
                ptr: value.as_ptr(),
                len: value.len(),
            }];
            let mut expected = Vec::new();
            expected.extend_from_slice(b"$3145728\r\n");
            expected.extend_from_slice(&value);
            expected.extend_from_slice(b"\r\n");
            let total = expected.len();

            let reader = std::thread::spawn(move || {
                let mut client = client;
                let mut buf = vec![0u8; total];
                client.read_exact(&mut buf).unwrap();
                buf
            });
            // Precondition: `value` (the pinned region) outlives the send below.
            let _ = rt.send_zc(&mut server, out2, inserts).await.unwrap();
            let seen = reader.join().unwrap();
            assert_eq!(seen.len(), total);
            assert_eq!(
                seen, expected,
                "the large spliced payload arrived intact + in order"
            );
            drop(value);
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

    /// RAW accept + RAW connect end to end (sub-slice 1b): a raw `connect` op-future dials a raw
    /// `accept` op-future on a real listener, concurrently on the SAME shard ring; then data
    /// round-trips both directions over the accepted + connected sockets. Proves the kernel-written
    /// peer address parses and both op payloads (sockaddr + owned socket) are handled correctly.
    #[test]
    fn accept_and_connect_round_trip() {
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let raw_listener = RawUringTcpListener::from_std(listener);

            // Drive accept + connect concurrently: accept parks until the connect's SYN lands.
            let (accepted, connected) = tokio::join!(rt.accept(&raw_listener), rt.connect(addr));
            let (mut server, peer) = accepted.unwrap();
            let mut client = connected.unwrap();
            assert_eq!(
                peer.ip(),
                addr.ip(),
                "accepted peer address parsed (loopback)"
            );

            // Client -> server.
            let _ = rt.send(&mut client, b"PING".to_vec()).await.unwrap();
            let r = rt.recv(&mut server, Vec::with_capacity(16)).await.unwrap();
            assert_eq!(
                &r.buf[..r.n],
                b"PING",
                "server received over the accepted socket"
            );

            // Server -> client.
            let _ = rt.send(&mut server, b"PONG".to_vec()).await.unwrap();
            let r2 = rt.recv(&mut client, Vec::with_capacity(16)).await.unwrap();
            assert_eq!(
                &r2.buf[..r2.n],
                b"PONG",
                "client received over the connected socket"
            );
        });
    }

    /// ACCEPT cancel-safety: a raw `accept` dropped before any client connects must free its owned
    /// sockaddr cleanly (via `IgnoredCloseResultFd`) and leave the runtime intact -- a subsequent
    /// real accept + connect on the SAME listener/runtime still works.
    #[test]
    fn accept_cancelled_before_connection_is_safe() {
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let raw_listener = RawUringTcpListener::from_std(listener);

            // No client yet: the accept parks; a timeout drops it mid-flight (the cancel path).
            let timed_out = tokio::time::timeout(
                std::time::Duration::from_millis(30),
                rt.accept(&raw_listener),
            )
            .await;
            assert!(
                timed_out.is_err(),
                "accept with no client times out (future dropped mid-flight)"
            );
            rt.timer(std::time::Duration::from_millis(30)).await; // let the cancel CQE reap

            // The listener + runtime survive: a real accept + connect still succeeds.
            let (accepted, connected) = tokio::join!(rt.accept(&raw_listener), rt.connect(addr));
            let (_server, _peer) = accepted.unwrap();
            let _client = connected.unwrap();
        });
    }

    /// MULTISHOT `recv_batch` (#513) end to end: on a multishot-capable kernel it arms a multishot
    /// recv on the first call then COALESCES all delivered buffers into `read_buf`; on an older kernel
    /// it uses the owned fallback. Either way the `recv_batch` contract holds (append, byte count,
    /// `0`=EOF). Includes a payload LARGER than one 16 KiB provided buffer, so the multishot path must
    /// stitch several kernel buffers together.
    #[test]
    fn multishot_recv_batch_delivers_across_buffers_then_eof() {
        use crate::BatchedRecvSend;
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let (mut server, mut client) = socket_pair();

            // Small message.
            client.write_all(b"hello").unwrap();
            let mut buf = Vec::new();
            let n = rt.recv_batch(&mut server, &mut buf).await.unwrap();
            assert_eq!(
                &buf[..n],
                b"hello",
                "first recv_batch delivered the message"
            );

            // A payload larger than READ_WINDOW (16 KiB): the multishot path must coalesce multiple
            // kernel buffers. Loop recv_batch until all bytes arrive (arrivals may split across CQEs).
            let big = vec![b'x'; 40 * 1024];
            client.write_all(&big).unwrap();
            buf.clear();
            while buf.len() < big.len() {
                let n = rt.recv_batch(&mut server, &mut buf).await.unwrap();
                assert_ne!(n, 0, "peer still open; must not report EOF mid-stream");
            }
            assert_eq!(buf.len(), big.len(), "all 40 KiB delivered");
            assert!(
                buf.iter().all(|&b| b == b'x'),
                "payload intact across buffers"
            );

            // Clean peer close -> EOF (0).
            drop(client);
            buf.clear();
            let n = rt.recv_batch(&mut server, &mut buf).await.unwrap();
            assert_eq!(n, 0, "recv_batch reports EOF after peer close");
        });
    }

    /// MULTISHOT cancel-safety: a connection with an armed multishot recv (its persistent slot + its
    /// share of the provided-buffer group) is DROPPED mid-flight. The drop must cancel the op, reclaim
    /// its buffers to the shared group, and leave the runtime intact -- a fresh multishot connection
    /// on the SAME runtime still works (a leaked group would eventually stall).
    #[test]
    fn multishot_cancel_on_drop_keeps_runtime_intact() {
        use crate::BatchedRecvSend;
        raw_uring_start(async {
            let rt = RawIoUringRuntime::new();
            let (mut server, client) = socket_pair();
            std::mem::forget(client); // keep the peer open so the recv genuinely parks (armed)

            // Arm multishot + park (no data): a short timeout drops the recv_batch future mid-flight,
            // then dropping `server` cancels the multishot op.
            let timed_out = tokio::time::timeout(
                std::time::Duration::from_millis(30),
                rt.recv_batch(&mut server, &mut Vec::new()),
            )
            .await;
            assert!(
                timed_out.is_err(),
                "recv_batch on an idle socket parks (armed)"
            );
            drop(server); // cancels the multishot op + reclaims its buffers
            rt.timer(std::time::Duration::from_millis(30)).await; // let the cancel CQE reap

            // A fresh multishot connection on the SAME runtime still round-trips.
            let (mut server2, mut client2) = socket_pair();
            client2.write_all(b"OK").unwrap();
            let mut buf = Vec::new();
            let n = rt.recv_batch(&mut server2, &mut buf).await.unwrap();
            assert_eq!(
                &buf[..n],
                b"OK",
                "runtime intact after a cancelled multishot conn"
            );
        });
    }

    /// #513 DEAF-CONNECTION regression (the adversarial-review CRITICAL): a multishot connection that
    /// terminated un-armed while the shared group was EXHAUSTED must be re-armed when a `reprovide`
    /// refills the group -- else it hangs forever (no in-flight op => no CQE => its waker never fires).
    /// Drives the `Driver` directly: arm two conns, simulate exhaustion (un-arm both + empty the
    /// group), reprovide ONE buffer, assert BOTH were re-armed by the empty->non-empty sweep.
    #[test]
    fn multishot_reprovide_rearms_starved_connections() {
        raw_uring_start(async {
            let (s1, c1) = socket_pair();
            let (s2, c2) = socket_pair();
            std::mem::forget(c1); // keep peers open so the armed recvs genuinely park
            std::mem::forget(c2);
            if !with_driver(|d| d.multishot_ok) {
                return; // older kernel: multishot inactive, nothing to test
            }
            let ud1 = with_driver(|d| d.arm_multishot(s1.fd));
            let ud2 = with_driver(|d| d.arm_multishot(s2.fd));
            // Simulate exhaustion: both conns terminated un-armed, the whole group checked out.
            with_driver(|d| {
                d.mshot.get_mut(&ud1).unwrap().armed = false;
                d.mshot.get_mut(&ud2).unwrap().armed = false;
                let pool = d.pool.as_mut().unwrap();
                for bid in 0..super::MSHOT_NBUFS {
                    pool.mark_out(bid);
                }
                assert_eq!(pool.in_group_count, 0, "group fully exhausted");
            });
            // Reprovide ONE buffer -> the empty->non-empty transition must re-arm EVERY starved conn.
            with_driver(|d| d.reprovide(0));
            with_driver(|d| {
                assert!(
                    d.mshot.get(&ud1).unwrap().armed,
                    "conn1 re-armed on group refill (was permanently deaf before the fix)"
                );
                assert!(
                    d.mshot.get(&ud2).unwrap().armed,
                    "conn2 re-armed on group refill (was permanently deaf before the fix)"
                );
            });
        });
    }
}
