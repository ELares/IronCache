// SPDX-License-Identifier: MIT OR Apache-2.0
//! The OPTIONAL Linux io_uring backend for [`crate::Runtime`] (PROD-10 / #28,
//! docs/design/IOURING_DATAPATH.md).
//!
//! This is an ADDITIVE, DEFAULT-OFF, Linux-only production backend behind the SAME
//! [`crate::Runtime`] trait the portable tokio backend ([`crate::TokioRuntime`])
//! implements. It is gated `#[cfg(all(target_os = "linux", feature = "io_uring"))]`,
//! so:
//!
//! * the DEFAULT build (no `io_uring` feature) never compiles this module, never
//!   pulls `tokio-uring`/`io-uring`, and is byte-identical to before (the static-musl
//!   default artifact is unaffected);
//! * a non-Linux target (macOS/Windows/BSD) never compiles it even WITH the feature on
//!   (the tokio backend always serves those hosts);
//! * only a Linux build with `--features io_uring` AND a boot-time `runtime = io_uring`
//!   selection drives this path; everything else falls back to the tokio backend.
//!
//! ## Why it fits the seam cleanly (ADR-0002 shared-nothing thread-per-core)
//!
//! `tokio-uring` is itself a CURRENT-THREAD runtime: [`tokio_uring::start`] builds one
//! `tokio::runtime::Builder::new_current_thread()` + a `LocalSet` and drives one io_uring
//! per thread. That is exactly IronCache's per-shard topology: one OS thread per shard,
//! one ring per shard, no buffer or completion crossing a core (RUNTIME.md, ADR-0002).
//! So this backend is a NEW `Runtime` impl, not a rewrite of the engine/serve logic: the
//! engine compiles against the unchanged trait, and the per-shard io_uring boot
//! ([`run_shards_uring`]) reuses the same shared-nothing acceptor + per-shard-thread shape
//! the tokio bootstrap uses.
//!
//! ## Owned-buffer model (RUNTIME_ABSTRACTION.md)
//!
//! io_uring's completion model REQUIRES the buffer to outlive the kernel call: a submitted
//! read/write hands the kernel a raw pointer + length and the buffer must not move or free
//! until the CQE arrives. The `Runtime` seam was shaped for exactly this -- `recv`/`send`
//! take and RETURN the owned [`Self::Buf`], never a borrowed slice -- so the mapping onto
//! `tokio_uring::net::TcpStream::{read, write_all}` (which take an owned buffer and return
//! `(io::Result<_>, buf)`) is direct, with no extra copy.
//!
//! ## Scope of THIS v1 (honest)
//!
//! This delivers a CORRECT, CI-built io_uring `Runtime` + a per-shard io_uring bootstrap
//! that drives the PLAINTEXT datapath through the trait's owned-buffer `recv`/`send`. It is
//! the registered-buffer / multishot-recv fast path's SUBSTRATE (IOURING_DATAPATH.md), not
//! yet that fast path: the multishot/provided-buffer optimization and the per-shard
//! registered slab are deferred to the Linux soak/benchmark, where they can be measured. No
//! throughput claim is made here. TLS over io_uring is NOT composed in v1 (rustls' tokio
//! `AsyncRead`/`AsyncWrite` adapters do not drive io_uring submissions); a `runtime =
//! io_uring` boot with TLS enabled FALLS BACK to the tokio backend (documented in the boot
//! selection), so TLS is never broken.

#![allow(clippy::module_name_repetitions)]

use crate::{IoBuf, RecvResult, Runtime};
use core::future::Future;
use core::time::Duration;
use std::io;
use std::net::SocketAddr;
use tokio_uring::buf::BoundedBuf;
use tokio_uring::net::{TcpListener, TcpStream};

/// The default read window appended per `recv`, matching the tokio backend's 16 KiB read
/// reservation so the two backends frame identically. The registered-buffer slab
/// (IOURING_DATAPATH.md) is a later optimization layered on this same owned-buffer model.
const READ_WINDOW: usize = 16 * 1024;

/// The io_uring runtime backend. Zero-sized, exactly like [`crate::TokioRuntime`]: it
/// carries no shared state (it must not, per shared-nothing ADR-0002); the per-shard ring
/// lives in the `tokio_uring` runtime on the shard's thread, reached through the
/// thread-local runtime context, not through this handle.
#[derive(Debug, Clone, Copy, Default)]
pub struct IoUringRuntime;

impl IoUringRuntime {
    /// Construct the backend handle.
    #[must_use]
    pub fn new() -> Self {
        IoUringRuntime
    }
}

impl Runtime for IoUringRuntime {
    type Listener = TcpListener;
    type Stream = TcpStream;
    type Buf = Vec<u8>;
    type Error = io::Error;

    async fn accept(
        &self,
        listener: &Self::Listener,
    ) -> Result<(Self::Stream, SocketAddr), Self::Error> {
        let (stream, peer) = listener.accept().await?;
        // Disable Nagle to match the tokio backend: request/reply caches want low latency
        // over coalescing. tokio-uring exposes the raw fd via `as_raw_fd`; set TCP_NODELAY
        // through a borrowed std socket (no ownership transfer, so the fd is not closed).
        set_nodelay(&stream);
        Ok((stream, peer))
    }

    async fn connect(&self, addr: SocketAddr) -> Result<Self::Stream, Self::Error> {
        let stream = TcpStream::connect(addr).await?;
        set_nodelay(&stream);
        Ok(stream)
    }

    async fn recv(
        &self,
        stream: &mut Self::Stream,
        mut buf: Self::Buf,
    ) -> Result<RecvResult<Self::Buf>, Self::Error> {
        // Owned-buffer APPEND (RUNTIME_ABSTRACTION.md), matching `TokioRuntime::recv`: read
        // into the buffer's spare capacity STARTING AT its current length, then return the
        // grown buffer plus the count. io_uring fills the buffer in place (zero extra copy);
        // the kernel call holds the owned buffer for its whole lifetime, which is why the
        // seam is owned-buffer in the first place.
        let start = IoBuf::len(&buf);
        // Reserve the read window past the initialized prefix. `slice(start..)` requires
        // `start < capacity`, so reserve strictly more than `start` bytes of capacity.
        buf.reserve(READ_WINDOW);
        debug_assert!(start < buf.capacity());
        // `slice(start..)` yields a view over `[start, capacity)`: io_uring reads at most
        // `capacity - start` bytes there and advances the underlying Vec's initialized
        // length to `start + n` on completion (the `BoundedBufMut` set_init contract), so
        // recovering the inner Vec gives exactly the appended buffer.
        let (res, slice) = stream.read(buf.slice(start..)).await;
        let buf = slice.into_inner();
        let n = res?;
        Ok(RecvResult { buf, n })
    }

    async fn send(
        &self,
        stream: &mut Self::Stream,
        buf: Self::Buf,
    ) -> Result<Self::Buf, Self::Error> {
        // Owned-buffer write, symmetric with `recv` and with `TokioRuntime::send`: write
        // ALL of the buffer's bytes (io_uring `write_all` resubmits on a short write), then
        // hand the owned buffer back so the caller (or a pool) can reclaim the allocation.
        let (res, buf) = stream.write_all(buf).await;
        res?;
        Ok(buf)
    }

    async fn timer(&self, dur: Duration) -> () {
        // `tokio_uring::start` runs on a tokio current-thread runtime with the time driver
        // enabled (`enable_all`), so the canonical tokio timer drives the seam's timer here
        // too -- no separate io_uring timeout op is needed for the timer abstraction.
        tokio::time::sleep(dur).await;
    }

    fn spawn_on_shard<F>(&self, task: F)
    where
        F: Future<Output = ()> + 'static,
    {
        // `tokio_uring::spawn` pins the task to THIS thread's `LocalSet` (the shard's ring
        // runs there), never migrating across cores (ADR-0002). It does not require `Send`,
        // matching the thread-per-core `!Send`-futures property.
        tokio_uring::spawn(task);
    }
}

/// Set `TCP_NODELAY` on a `tokio_uring` stream without taking ownership of its fd.
///
/// `tokio_uring::net::TcpStream` does not expose a `set_nodelay`, so borrow the fd as a std
/// `TcpStream` via [`std::os::fd::BorrowedFd`] semantics: construct a std stream from the raw
/// fd, set the option, then `into_raw_fd` it back to AVOID closing the fd on drop (a plain
/// `from_raw_fd` std stream would close the descriptor the io_uring stream still owns). Errors
/// are ignored to match the tokio backend (`let _ = set_nodelay`): a node still functions
/// without the latency hint.
fn set_nodelay(stream: &TcpStream) {
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
    let raw = stream.as_raw_fd();
    // SAFETY: `raw` is a valid, open TCP socket fd owned by `stream` for the duration of this
    // borrow. We construct a std `TcpStream` over it ONLY to call `set_nodelay`, then
    // immediately `into_raw_fd` to relinquish ownership WITHOUT closing the fd, so the
    // io_uring stream remains the sole owner and closes it exactly once on its own drop.
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(raw) };
    let _ = std_stream.set_nodelay(true);
    let _ = std_stream.into_raw_fd();
}

/// Read the `(peer, local)` socket addresses of a `tokio_uring` stream as display strings,
/// WITHOUT taking ownership of its fd. `tokio_uring::net::TcpStream` exposes no `peer_addr` /
/// `local_addr`, so borrow the fd through a temporary std stream (the same no-ownership-transfer
/// dance [`set_nodelay`] uses) and `into_raw_fd` it back so the descriptor the io_uring stream
/// still owns is not closed. A failure yields an empty string (the addresses are cosmetic, used
/// only for `CLIENT INFO`).
///
/// This lives in the runtime crate because the borrowed-fd construction needs `unsafe`, which the
/// `ironcache` crate FORBIDS (`#![forbid(unsafe_code)]`); the io_uring serve loop calls this so the
/// forbidden `unsafe` stays out of `ironcache`.
#[must_use]
pub fn peer_local_addrs(stream: &TcpStream) -> (String, String) {
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
    let raw = stream.as_raw_fd();
    // SAFETY: `raw` is the valid, open TCP socket fd owned by `stream` for the duration of this
    // borrow. We construct a std `TcpStream` over it ONLY to read peer/local addr, then
    // immediately `into_raw_fd` to relinquish ownership WITHOUT closing the fd, so the io_uring
    // stream remains the sole owner and closes it exactly once on its own drop.
    let s = unsafe { std::net::TcpStream::from_raw_fd(raw) };
    let peer = s.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    let local = s.local_addr().map(|a| a.to_string()).unwrap_or_default();
    let _ = s.into_raw_fd();
    (peer, local)
}

// ---------------------------------------------------------------------------
// Per-shard io_uring bootstrap (the shared-nothing thread-per-core boot, ADR-0002).
// ---------------------------------------------------------------------------

pub use uring_bootstrap::run_shards_uring;

mod uring_bootstrap {
    use super::{IoUringRuntime, TcpStream};
    use crate::bootstrap::{ShardConfig, ShardId, ShardSet};
    use crate::tokio_rt::bind_reuseport_std;
    use std::cell::Cell;
    use std::future::Future;
    use std::os::fd::{FromRawFd, IntoRawFd};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    /// A core-local count of in-flight connection tasks on one shard (mirrors the tokio
    /// bootstrap's `LiveTasks`): a plain `Rc<Cell<_>>` suffices (single-threaded per shard,
    /// no atomics; shared-nothing ADR-0002).
    type LiveTasks = Rc<Cell<usize>>;

    /// RAII guard decrementing the shard's live-task count when a connection task ends,
    /// including on panic, so the bounded drain count stays accurate.
    struct LiveGuard(LiveTasks);
    impl Drop for LiveGuard {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_sub(1));
        }
    }

    /// Run the shard set on the io_uring backend (the Linux, `runtime = io_uring`,
    /// PLAINTEXT path). It mirrors the tokio [`crate::bootstrap::run_shards`] topology
    /// exactly -- ONE bound listener + a single userspace acceptor thread that round-robins
    /// accepted `std::net::TcpStream`s to per-shard channels (portable load balancing,
    /// shared-nothing ADR-0002) -- but each shard thread runs a `tokio_uring` current-thread
    /// runtime (one io_uring per shard) instead of a plain tokio current-thread runtime, and
    /// adopts each accepted connection onto ITS ring via [`TcpStream::from_std`].
    ///
    /// `serve` is invoked per connection with the shard's [`IoUringRuntime`], the adopted
    /// [`tokio_uring::net::TcpStream`], and the [`ShardId`]; it returns a `'static` future
    /// (the connection task). `inboxes` + `drain` mirror the tokio bootstrap's per-shard
    /// inbox/drain wiring (COORDINATOR.md #107). This is intentionally NOT generic over the
    /// `Runtime` trait at the bootstrap level (the trait has no listener-bind/adopt surface);
    /// it is the io_uring sibling of `run_shards`, sharing the acceptor + drain shape.
    pub fn run_shards_uring<S, Fut, I, D, DFut>(
        cfg: &ShardConfig,
        serve: S,
        inboxes: Vec<I>,
        drain: D,
    ) -> std::io::Result<ShardSet>
    where
        S: Fn(IoUringRuntime, TcpStream, ShardId) -> Fut + Clone + Send + 'static,
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
            "run_shards_uring: one inbox per shard required (got {}, need {total})",
            inboxes.len()
        );

        // Bind the ONE listening socket up front so a bind failure surfaces here, not inside
        // a spawned thread (same as the tokio bootstrap). The acceptor thread owns it.
        let listener = bind_reuseport_std(cfg.bind)?;

        // One connection channel per shard: the acceptor sends accepted std streams, the
        // shard receives them. Unbounded so the synchronous acceptor never blocks on a
        // shard's ring; the channel carries only the raw socket (shared-nothing intact).
        let mut conn_senders = Vec::with_capacity(total);
        let mut conn_receivers = Vec::with_capacity(total);
        for _ in 0..total {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<std::net::TcpStream>();
            conn_senders.push(tx);
            conn_receivers.push(rx);
        }

        let mut handles = Vec::with_capacity(total + 1);

        // The ACCEPTOR thread: identical to the tokio bootstrap's (a plain blocking std
        // accept loop with a shutdown-aware non-blocking poll), reused here so the io_uring
        // path inherits the exact same portable, round-robin connection spread.
        {
            let shutdown = Arc::clone(&shutdown);
            let acceptor = std::thread::Builder::new()
                .name("ironcache-acceptor-uring".to_string())
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
                .name(format!("ironcache-shard-uring-{index}"))
                .spawn(move || {
                    // Catch a panic escaping the serve loop so the thread logs it before
                    // exiting, then resume so `join()` still surfaces it (mirrors the tokio
                    // bootstrap's panic handling).
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        // ONE io_uring runtime per shard thread (one ring per shard). All of
                        // the shard's tasks run on its `LocalSet`, interleaved never parallel,
                        // so the shard-local `RefCell`s stay single-threaded (ADR-0002).
                        tokio_uring::start(async move {
                            let drain_task =
                                tokio_uring::spawn(drain(index, inbox, drain_shutdown));
                            serve_loop(conn_rx, &serve, shard, &shutdown).await;
                            // Bounded graceful join of the drain task (SHUTDOWN.md): the drain
                            // loop returns promptly on a flagged stop; this is the same final
                            // backstop bound the tokio bootstrap applies.
                            let drain_grace = tokio::time::sleep(crate::bootstrap::DRAIN_GRACE);
                            tokio::pin!(drain_grace);
                            tokio::select! {
                                _ = drain_task => {}
                                () = &mut drain_grace => {
                                    eprintln!(
                                        "shard {index} (io_uring): drain task did not finish \
                                         within the grace window; proceeding with shutdown"
                                    );
                                }
                            }
                        });
                    }));
                    if let Err(panic) = result {
                        let shard_died: u64 = 1;
                        eprintln!(
                            "shard {index} (io_uring): serve loop panicked \
                             (shard_died={shard_died}); shard thread exiting"
                        );
                        std::panic::resume_unwind(panic);
                    }
                })?;
            handles.push(handle);
        }

        Ok(ShardSet::from_parts(shutdown, handles))
    }

    /// The single acceptor's loop: accept on the one listener and round-robin each
    /// connection to a shard's channel. A copy of the tokio bootstrap's acceptor (it has no
    /// tokio-runtime dependency: a blocking std accept with a non-blocking shutdown poll),
    /// kept private here so the io_uring boot is self-contained.
    fn acceptor_loop(
        listener: &std::net::TcpListener,
        conn_senders: &[tokio::sync::mpsc::UnboundedSender<std::net::TcpStream>],
        shutdown: &Arc<AtomicBool>,
    ) {
        if let Err(e) = listener.set_nonblocking(true) {
            eprintln!("acceptor (io_uring): set_nonblocking failed: {e}; shutdown may be delayed");
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
                        eprintln!("acceptor (io_uring): shard {target} channel closed: {e}");
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(poll);
                }
                Err(e) => {
                    eprintln!("acceptor (io_uring): accept error: {e}");
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }

    /// The shard's serve loop: await the connection channel for std streams, adopt each onto
    /// THIS shard's io_uring via [`TcpStream::from_std`], and spawn `serve` per connection on
    /// the shard-local `LocalSet`. Mirrors the tokio bootstrap's serve loop + bounded drain.
    async fn serve_loop<S, Fut>(
        mut conn_rx: tokio::sync::mpsc::UnboundedReceiver<std::net::TcpStream>,
        serve: &S,
        shard: ShardId,
        shutdown: &Arc<AtomicBool>,
    ) where
        S: Fn(IoUringRuntime, TcpStream, ShardId) -> Fut + Clone + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let live: LiveTasks = Rc::new(Cell::new(0));

        while !shutdown.load(Ordering::Relaxed) {
            tokio::select! {
                maybe = conn_rx.recv() => {
                    match maybe {
                        Some(std_stream) => {
                            // Adopt onto THIS shard's ring. tokio-uring's `from_std` takes the
                            // std stream by value (consuming its fd ownership): pass the raw fd
                            // through `into_raw_fd`/`from_raw_fd` so exactly one owner closes it.
                            // The socket non-blocking flag is irrelevant for io_uring submissions
                            // (the ring drives readiness), unlike the tokio reactor path.
                            let raw = std_stream.into_raw_fd();
                            // SAFETY: `raw` is a valid open TCP socket fd just handed off by the
                            // acceptor and not retained anywhere else; reconstructing a std stream
                            // to feed `TcpStream::from_std` transfers sole ownership to the ring.
                            let std_stream = unsafe { std::net::TcpStream::from_raw_fd(raw) };
                            let stream = TcpStream::from_std(std_stream);
                            let fut = serve(IoUringRuntime::new(), stream, shard);
                            live.set(live.get() + 1);
                            let guard = LiveGuard(Rc::clone(&live));
                            tokio_uring::spawn(async move {
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

    /// Await the shard's in-flight connection tasks until the live count reaches zero or the
    /// grace window elapses (SHUTDOWN.md bounded drain), polling on a short tick. Mirrors the
    /// tokio bootstrap's `drain_live_tasks`.
    async fn drain_live_tasks(live: &LiveTasks, shard: ShardId) {
        if live.get() == 0 {
            return;
        }
        let deadline = tokio::time::Instant::now() + crate::bootstrap::DRAIN_GRACE;
        let tick = Duration::from_millis(20);
        while live.get() > 0 {
            if tokio::time::Instant::now() >= deadline {
                eprintln!(
                    "shard {} (io_uring): drain grace elapsed with {} connection task(s) still \
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
    use super::*;

    /// The io_uring backend satisfies the same `accept` / `recv` / `send` round-trip the
    /// tokio backend's test exercises, proving the owned-buffer append semantics map
    /// correctly onto `tokio_uring`'s start-of-buffer read + `set_init` (the subtle part:
    /// `recv` must APPEND, which we get via `buf.slice(start..)`). Runs entirely inside a
    /// `tokio_uring::start` shard-style runtime. Linux + feature-gated, so it runs only in CI
    /// on the io_uring path.
    #[test]
    fn accept_recv_send_roundtrip_uring() {
        tokio_uring::start(async {
            let runtime = IoUringRuntime::new();
            let std_listener =
                crate::tokio_rt::bind_reuseport_std("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = std_listener.local_addr().unwrap();
            let listener = TcpListener::from_std(std_listener);

            let server = tokio_uring::spawn(async move {
                let (mut stream, _peer) = runtime.accept(&listener).await.unwrap();
                // Pre-seed the buffer with one byte to prove `recv` APPENDS rather than
                // overwriting from index 0.
                let mut buf: Vec<u8> = Vec::with_capacity(64);
                buf.push(b'X');
                let res = runtime.recv(&mut stream, buf).await.unwrap();
                assert_eq!(res.n, 6);
                assert_eq!(&res.buf, b"XPING\r\n");
                let returned = runtime
                    .send(&mut stream, b"+PONG\r\n".to_vec())
                    .await
                    .unwrap();
                assert_eq!(returned, b"+PONG\r\n");
            });

            // Client side over the io_uring connect/send/recv seam.
            let client = IoUringRuntime::new();
            let mut peer = client.connect(addr).await.unwrap();
            let _ = client.send(&mut peer, b"PING\r\n".to_vec()).await.unwrap();
            let res = client
                .recv(&mut peer, Vec::with_capacity(16))
                .await
                .unwrap();
            assert_eq!(&res.buf[..res.n], b"+PONG\r\n");
            server.await.unwrap();
        });
    }
}
