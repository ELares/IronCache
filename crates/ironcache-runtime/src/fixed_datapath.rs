// SPDX-License-Identifier: MIT OR Apache-2.0
//! The OneShotFixed io_uring datapath substrate: recv over REGISTERED fixed buffers (#284,
//! IOURING_DATAPATH.md "Registered fixed-buffer slab and buffer groups").
//!
//! The baseline io_uring `recv` ([`crate::io_uring_rt`]) reads into an OWNED `Vec<u8>` the kernel
//! must pin/unpin per request. This is the mid tier [`crate::uring_probe::DataPath::OneShotFixed`]
//! selects when the kernel lacks multishot: a per-shard slab of buffers is REGISTERED with the ring
//! ONCE at init, and each read goes into a checked-out registered buffer via `read_fixed`, so the
//! bytes land directly in the pre-registered slab (no per-request pin/malloc) and the parser reads
//! them IN PLACE. No buffer ever leaves the shard, matching the shared-nothing model (ADR-0002).
//!
//! [`FixedRing`] wraps `tokio-uring`'s `FixedBufPool` (which does the index bookkeeping for this
//! high-level tier). The raw-io_uring MULTISHOT tier (provided-buffer rings, which tokio-uring does
//! not expose) is a following slice and uses the lower-level [`crate::buffer_pool`] ledger instead.
//!
//! Whole-module gated to `#[cfg(all(target_os = "linux", feature = "io_uring"))]` at the `pub mod`
//! in `lib.rs`, so it compiles + is functionally tested only on the io_uring path (a real ring).

use std::io;

use tokio_uring::buf::IoBufMut;
use tokio_uring::buf::fixed::{FixedBuf, FixedBufPool};
use tokio_uring::net::TcpStream;

/// A per-shard REGISTERED fixed-buffer group for the OneShotFixed datapath: `count` buffers of
/// `buf_size` bytes, registered once with the shard's ring.
///
/// Created + registered INSIDE the shard's `tokio_uring::start` (registration binds to the current
/// thread's ring). Because it is per-shard and single-threaded, it carries no synchronization.
pub struct FixedRing {
    pool: FixedBufPool<Vec<u8>>,
    buf_size: usize,
}

impl FixedRing {
    /// Create a group of `count` buffers of `buf_size` bytes and register it with the current
    /// thread's io_uring. The slab is a FIXED budget (IOURING_DATAPATH.md "the per-shard slab is a
    /// fixed budget set at startup"), counted against the shard's maxmemory share, never grown.
    ///
    /// # Errors
    ///
    /// Returns the `io::Error` if `io_uring_register_buffers` fails (e.g. `RLIMIT_MEMLOCK` too low).
    /// The caller falls back to the owned-buffer datapath rather than treating it as fatal.
    ///
    /// # Panics
    ///
    /// Panics ("not in a runtime context") if called OUTSIDE a `tokio_uring::start` context, since
    /// registration binds to the current thread's ring. The per-shard bootstrap always calls this on
    /// the shard thread inside its `tokio_uring::start`.
    pub fn register(count: u16, buf_size: usize) -> io::Result<Self> {
        let bufs = (0..count).map(|_| Vec::with_capacity(buf_size));
        let pool = FixedBufPool::new(bufs);
        pool.register()?;
        Ok(FixedRing { pool, buf_size })
    }

    /// Check out a free registered buffer, or `None` when the group is DRAINED. `None` is the
    /// back-pressure signal: the shard defers re-arming recv rather than allocating beyond the fixed
    /// slab (so a read burst cannot blow the memory bound), exactly the rule
    /// [`crate::buffer_pool::BufferPool::can_rearm`] encodes for the raw tier.
    pub fn checkout(&self) -> Option<FixedBuf> {
        // `try_next(cap)` keys on the buffer's exact `bytes_total()` (capacity). This matches because
        // the pool was registered with `Vec::with_capacity(buf_size)` (capacity == buf_size). If a
        // future refactor ever built the buffers via a path whose `capacity()` could EXCEED buf_size,
        // this key would never match and `checkout` would wedge on a permanent false `None` -- so the
        // register/check-out sizes must stay tied to the one `buf_size` field, as they are here.
        self.pool.try_next(self.buf_size)
    }

    /// The per-buffer byte size (the max a single fixed read can land).
    #[must_use]
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }
}

/// Read from `stream` into a checked-out REGISTERED buffer via `read_fixed`: the bytes land directly
/// in the pre-registered slab, so the caller parses them in place with no copy. Returns:
/// - `None` if the group is drained (apply read back-pressure, do not allocate); else
/// - `Some((Ok(n), buf))` with the filled buffer and byte count (`n == 0` is a clean peer close), or
/// - `Some((Err(e), buf))` on a read error (the buffer is returned so it goes back to the pool).
pub async fn recv_fixed(
    stream: &TcpStream,
    ring: &FixedRing,
) -> Option<(io::Result<usize>, FixedBuf)> {
    let buf = ring.checkout()?;
    Some(stream.read_fixed(buf).await)
}

/// Write `data` to `stream` from a checked-out REGISTERED buffer via `write_fixed_all`: the reply is
/// staged into the pre-registered slab and written without a per-write pin. The contract mirrors
/// `recv_fixed`'s fall-back shape -- `None` means "the fixed send does not apply, use the owned-buffer
/// send" -- so the caller has one clean branch:
/// - `None` if `data` does not fit ONE buffer (`> ring.buf_size()`, a rare large bulk reply) OR the
///   group is drained (no buffer to stage into); either way the caller falls back to the owned send.
/// - `Some(Ok(()))` on a fully-written fixed send; `Some(Err(e))` on a write error.
pub async fn send_fixed(
    stream: &TcpStream,
    ring: &FixedRing,
    data: &[u8],
) -> Option<io::Result<()>> {
    // A reply larger than one fixed buffer uses the owned send (signalled by None), not a multi-
    // buffer fixed write -- keeps this primitive single-buffer and the size decision in the caller.
    if data.len() > ring.buf_size() {
        return None;
    }
    let mut buf = ring.checkout()?; // None = drained -> owned-send fall-back
    // SAFETY: `data.len() <= ring.buf_size()` (guarded above) == the checked-out buffer's capacity
    // (`FixedBufPool` grouped it by that capacity), so `stable_mut_ptr()` is valid for `data.len()`
    // writes. `data` and `buf` are distinct allocations (non-overlapping). We write exactly
    // `data.len()` bytes then mark exactly that many initialized, so the subsequent `write_fixed_all`
    // reads only the bytes we just wrote (no uninitialized read).
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), buf.stable_mut_ptr(), data.len());
        buf.set_init(data.len());
    }
    let (res, _buf) = stream.write_fixed_all(buf).await;
    Some(res)
}

#[cfg(test)]
mod tests {
    use super::{FixedRing, recv_fixed, send_fixed};
    use crate::io_uring_rt::IoUringRuntime;
    use crate::{Runtime, tokio_rt};
    use tokio_uring::net::TcpListener;

    /// A registered fixed-buffer group serves a real read: the server registers a `FixedRing`, reads
    /// a request via `read_fixed` (into the registered slab), asserts the bytes arrived in place,
    /// then replies over the owned-buffer send. Proves the register + check-out + `read_fixed`
    /// mechanism end to end on a real ring. Linux + io_uring gated, so it runs in the CI io_uring
    /// datapath job + a local Linux container.
    #[test]
    fn fixed_ring_recv_reads_a_request_into_the_registered_slab() {
        tokio_uring::start(async {
            let std_listener =
                tokio_rt::bind_reuseport_std("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = std_listener.local_addr().unwrap();
            let listener = TcpListener::from_std(std_listener);

            let server = tokio_uring::spawn(async move {
                let runtime = IoUringRuntime::new();
                let (stream, _peer) = runtime.accept(&listener).await.unwrap();
                // Register a small per-shard fixed-buffer group and read the request INTO it.
                let ring = FixedRing::register(8, 4096).unwrap();
                let (res, buf) = recv_fixed(&stream, &ring)
                    .await
                    .expect("a buffer was available (pool not drained)");
                let n = res.unwrap();
                assert_eq!(n, 6);
                assert_eq!(
                    &buf[..n],
                    b"PING\r\n",
                    "the read landed in the registered buffer"
                );
                // Reply over the owned-buffer send (the fixed send path is a following slice).
                drop(buf); // return the fixed buffer to the pool before the reply
                runtime
                    .send(&mut { stream }, b"+PONG\r\n".to_vec())
                    .await
                    .unwrap();
            });

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

    /// The full FIXED round-trip: the server reads the request via `recv_fixed` AND writes the reply
    /// via `send_fixed` (both over the registered slab). Also asserts the fall-back contract: a reply
    /// larger than one buffer yields `None` (caller uses the owned send).
    #[test]
    fn fixed_ring_recv_and_send_round_trip_over_the_registered_slab() {
        tokio_uring::start(async {
            let std_listener =
                tokio_rt::bind_reuseport_std("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = std_listener.local_addr().unwrap();
            let listener = TcpListener::from_std(std_listener);

            let server = tokio_uring::spawn(async move {
                let runtime = IoUringRuntime::new();
                let (stream, _peer) = runtime.accept(&listener).await.unwrap();
                let ring = FixedRing::register(8, 4096).unwrap();

                let (res, buf) = recv_fixed(&stream, &ring).await.expect("buffer available");
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"PING\r\n");
                drop(buf);

                // A reply that does NOT fit one buffer -> None (caller falls back to owned send).
                let too_big = vec![b'x'; ring.buf_size() + 1];
                assert!(
                    send_fixed(&stream, &ring, &too_big).await.is_none(),
                    "oversized reply must signal owned-send fall-back"
                );

                // The fitting reply is written from the registered slab.
                send_fixed(&stream, &ring, b"+PONG\r\n")
                    .await
                    .expect("fits + not drained")
                    .unwrap();
            });

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

    /// Draining the group yields `None` from `checkout` (the back-pressure signal): after checking
    /// out all `count` buffers, the next check-out is `None`, and returning one makes it available
    /// again.
    #[test]
    fn checkout_drains_to_none_then_recovers_on_return() {
        tokio_uring::start(async {
            let ring = FixedRing::register(2, 1024).unwrap();
            let a = ring.checkout().expect("first");
            let _b = ring.checkout().expect("second");
            assert!(ring.checkout().is_none(), "drained -> back-pressure");
            drop(a); // returning a buffer lifts back-pressure
            assert!(ring.checkout().is_some(), "a returned buffer is reusable");
        });
    }
}
