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

use std::cell::RefCell;
use std::io;

use tokio_uring::buf::fixed::{FixedBuf, FixedBufPool};
use tokio_uring::buf::{BoundedBuf, IoBufMut};
use tokio_uring::net::TcpStream;

use crate::Runtime;
use crate::io_uring_rt::IoUringRuntime;
use crate::uring_probe::{DataPath, probe_uring_caps, select_datapath};

/// A per-shard REGISTERED fixed-buffer group for the OneShotFixed datapath: `count` buffers of
/// `buf_size` bytes, registered once with the shard's ring.
///
/// Created + registered INSIDE the shard's `tokio_uring::start` (registration binds to the current
/// thread's ring). Because it is per-shard and single-threaded, it carries no synchronization.
///
/// `Clone` is a cheap `Rc` clone of the same underlying pool (tokio-uring's `FixedBufPool` is
/// `Rc`-backed): a clone shares the one registered slab, so [`recv_batch`] can clone it out of the
/// per-shard thread-local and hold it across the recv `await` without holding the thread-local borrow.
#[derive(Clone)]
pub struct FixedRing {
    pool: FixedBufPool<Vec<u8>>,
    buf_size: usize,
}

impl FixedRing {
    /// Create a group of `count` buffers of `buf_size` bytes and register it with the current
    /// thread's io_uring. The slab is a FIXED budget (IOURING_DATAPATH.md "the per-shard slab is a
    /// fixed budget set at startup"), never grown. NOTE: charging this registered, `RLIMIT_MEMLOCK`
    /// -pinned slab against the shard's maxmemory share (the doc's intent) is NOT yet wired -- today
    /// it is a fixed budget OUTSIDE the maxmemory accounting; wiring the accounting is a follow-up.
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
    // SAFETY: `data.len() <= ring.buf_size() <= capacity` (the `>` guard above + `Vec::with_capacity`
    // never under-allocates), so `stable_mut_ptr()` is valid for `data.len()` writes. `data` and
    // `buf` are distinct allocations (non-overlapping). We write exactly `data.len()` bytes then grow
    // the init length to cover them, so the `0..data.len()` slice below has an INITIALIZED range.
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), buf.stable_mut_ptr(), data.len());
        buf.set_init(data.len());
    }
    // Write EXACTLY `data.len()` bytes by SLICING to `0..data.len()`: the SQE length is the slice
    // length, NOT the buffer's `bytes_init()`. tokio-uring's `set_init` is GROW-ONLY and the pool
    // RETAINS `init_len` across buffer reuse, so a reused buffer can carry a LARGER init_len from a
    // prior (longer) recv/reply; writing `bytes_init()` would then append those stale, possibly
    // cross-connection, trailing bytes after the reply. The slice pins the write to the fresh reply.
    let (res, _buf) = stream.write_fixed_all(buf.slice(0..data.len())).await;
    Some(res)
}

/// The per-shard fixed-buffer group geometry: `FIXED_RING_COUNT` buffers of `FIXED_RING_BUF_SIZE`
/// bytes. The buffer size matches the serve loop's owned read window (16 KiB) so a single fixed read
/// covers the same batch; the count bounds the per-shard registered slab (here 64 * 16 KiB = 1 MiB
/// per shard, a fixed budget). When the group drains, the read falls back to the owned recv for that
/// read (never a stall), so the count trades memory for how many concurrent reads stay on the fast
/// path before falling back.
const FIXED_RING_COUNT: u16 = 64;
const FIXED_RING_BUF_SIZE: usize = 16 * 1024;

/// This shard's resolved datapath (the three states the lazy per-shard resolution can be in).
enum ShardDatapath {
    /// Not yet resolved on this shard thread (the probe + registration have not run).
    Unresolved,
    /// The kernel selected the owned datapath (io_uring absent/disabled, the owned tier, or a slab
    /// registration failure): no registered slab; reads use the owned recv.
    Owned,
    /// The fixed datapath: this shard's registered slab.
    Fixed(FixedRing),
}

thread_local! {
    /// This shard's fixed datapath, resolved ONCE (lazily) on the shard thread.
    static SHARD_FIXED_RING: RefCell<ShardDatapath> = const { RefCell::new(ShardDatapath::Unresolved) };
}

/// This shard's fixed-buffer group, resolving it on first use: probe the running kernel and, if
/// [`select_datapath`] picks a registered-buffer tier, register the per-shard slab; otherwise the
/// owned datapath (returns `None`). Returns a cheap `Rc`-clone so the caller can hold it across the
/// recv `await` without keeping the thread-local borrowed. Must be called inside `tokio_uring::start`
/// (registration binds to the current thread's ring), which the shard serve loop always is.
fn shard_fixed_ring() -> Option<FixedRing> {
    SHARD_FIXED_RING.with(|cell| {
        if matches!(*cell.borrow(), ShardDatapath::Unresolved) {
            // First use on this shard: decide the datapath + (maybe) register the slab, ONCE.
            let resolved = match probe_uring_caps() {
                // Any registered-buffer tier -> register this shard's slab. NOTE: for now BOTH
                // non-owned tiers (OneShotFixed and MultishotProvided) run the SAME one-shot
                // `read_fixed` path here -- the multishot provided-buffer-ring datapath is an
                // unimplemented following slice -- so the tier distinction is coarse today; it is
                // safe because READ_FIXED (5.1) is available whenever any newer tier is. A
                // registration failure (e.g. RLIMIT_MEMLOCK) degrades to the owned path rather than
                // failing the shard.
                Ok(caps) if select_datapath(caps) != DataPath::OneShotOwned => {
                    match FixedRing::register(FIXED_RING_COUNT, FIXED_RING_BUF_SIZE) {
                        Ok(ring) => ShardDatapath::Fixed(ring),
                        Err(_) => ShardDatapath::Owned,
                    }
                }
                // io_uring absent/disabled, or the owned tier: no registered slab.
                _ => ShardDatapath::Owned,
            };
            *cell.borrow_mut() = resolved;
        }
        // Clone the ring out (cheap Rc clone) for the fixed datapath, releasing the borrow.
        match &*cell.borrow() {
            ShardDatapath::Fixed(ring) => Some(ring.clone()),
            ShardDatapath::Owned | ShardDatapath::Unresolved => None,
        }
    })
}

/// Read the next command batch into `read_buf`, APPENDING to any partial-frame carryover, using this
/// shard's REGISTERED fixed buffer when the kernel selected a registered-buffer datapath, else the
/// owned recv. Returns the bytes read (`0` = clean peer close / EOF).
///
/// The `read_buf` accumulator + partial-frame pipelining are IDENTICAL to the owned path: the fixed
/// read lands in a pre-registered buffer (no per-request page-pin -- the registered-read win) and its
/// bytes are appended to `read_buf`, exactly as the owned recv appends into `read_buf`'s spare
/// capacity. (A zero-copy parse-IN-PLACE from the registered buffer is a further optimization; this
/// slice keeps the accumulator model unchanged so the wiring is provably behavior-preserving.) If the
/// group is momentarily drained, this read falls back to the owned recv rather than stalling.
///
/// # Errors
///
/// Returns the underlying `io::Error` from the recv (a dead/broken peer); the caller closes.
pub async fn recv_batch(
    rt: &IoUringRuntime,
    stream: &mut TcpStream,
    read_buf: &mut Vec<u8>,
) -> io::Result<usize> {
    if let Some(ring) = shard_fixed_ring() {
        match recv_fixed(stream, &ring).await {
            Some((Ok(n), buf)) => {
                read_buf.extend_from_slice(&buf[..n]);
                return Ok(n);
            }
            Some((Err(e), _)) => return Err(e),
            // Drained (all buffers checked out by other in-flight reads on this shard): fall through
            // to the owned recv for this one read rather than blocking.
            None => {}
        }
    }
    // Owned datapath (or drained fall-back): the io_uring owned-buffer recv, appending into
    // `read_buf`'s spare capacity, then hand ownership back.
    let res = rt.recv(stream, std::mem::take(read_buf)).await?;
    *read_buf = res.buf;
    Ok(res.n)
}

/// Send a complete reply batch, staging it through this shard's REGISTERED fixed buffer when the
/// kernel selected a registered-buffer datapath AND the reply fits one buffer, else the owned send:
/// the OneShotFixed WRITE tier, the reply-side analogue of [`recv_batch`]. It is the missing half of
/// the OneShotFixed win -- [`recv_fixed`] already lands reads in the pre-registered slab, but replies
/// were flushed via the owned [`IoUringRuntime::send`], which pins the reply buffer with the kernel
/// per write. Staging through a pre-registered buffer removes that per-send pin (one memcpy into the
/// slab replaces the per-write registration).
///
/// BEHAVIOR-PRESERVING: the output is byte-identical either way ([`send_fixed`] writes EXACTLY
/// `data.len()` bytes; the stale-`init_len` hazard is handled there and regression-tested). It takes
/// the owned buffer and hands it BACK for reuse exactly like [`IoUringRuntime::send`], so the fixed
/// path adds no allocation and the owned fallback reuses the SAME buffer with no extra copy. A reply
/// larger than one fixed buffer (`> ring.buf_size()`), or a momentarily drained slab, transparently
/// falls back to the owned send.
///
/// # Errors
///
/// Returns the underlying `io::Error` from the write (a dead/broken peer); the caller closes.
pub async fn send_batch(
    rt: &IoUringRuntime,
    stream: &mut TcpStream,
    data: Vec<u8>,
) -> io::Result<Vec<u8>> {
    if let Some(ring) = shard_fixed_ring() {
        match send_fixed(stream, &ring, &data).await {
            // Fixed write done: hand the reply buffer back so the serve loop reuses its allocation.
            Some(Ok(())) => return Ok(data),
            Some(Err(e)) => return Err(e),
            // Oversize (> one fixed buffer) or drained slab: fall through to the owned send with the
            // SAME buffer (no extra allocation/copy), rather than staging.
            None => {}
        }
    }
    // Owned datapath (or fixed fall-back): the owned-buffer write, which hands the buffer back.
    rt.send(stream, data).await
}

#[cfg(test)]
mod tests {
    use super::{FixedRing, recv_batch, recv_fixed, send_fixed};
    use crate::io_uring_rt::IoUringRuntime;
    use crate::{Runtime, tokio_rt};
    use tokio_uring::net::TcpListener;

    /// `recv_batch` APPENDS successive reads onto `read_buf` (preserving a partial-frame carryover)
    /// and returns `0` at a clean peer close -- the exact accumulator + EOF contract the serve loop's
    /// pipelining depends on. Runs the real per-shard path (probe + register, or owned) on this
    /// kernel; either datapath must satisfy the same contract.
    #[test]
    fn recv_batch_appends_after_carryover_then_reports_eof() {
        tokio_uring::start(async {
            let std_listener =
                tokio_rt::bind_reuseport_std("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = std_listener.local_addr().unwrap();
            let listener = TcpListener::from_std(std_listener);

            let server = tokio_uring::spawn(async move {
                let rt = IoUringRuntime::new();
                let (mut stream, _peer) = rt.accept(&listener).await.unwrap();
                let mut read_buf = Vec::with_capacity(1024);
                // Pre-seed a partial-frame carryover: recv_batch must APPEND after it, never overwrite.
                read_buf.extend_from_slice(b"CARRY");
                // Read until the peer closes (EOF), appending every batch.
                loop {
                    let n = recv_batch(&rt, &mut stream, &mut read_buf).await.unwrap();
                    if n == 0 {
                        break; // clean EOF
                    }
                }
                assert_eq!(
                    &read_buf, b"CARRYPING\r\n",
                    "recv_batch appended the request after the carryover, then saw a clean EOF"
                );
            });

            let client = IoUringRuntime::new();
            let mut peer = client.connect(addr).await.unwrap();
            client.send(&mut peer, b"PING\r\n".to_vec()).await.unwrap();
            drop(peer); // close -> the server sees EOF after the 6 bytes
            server.await.unwrap();
        });
    }

    /// `recv_batch` accumulates a request LARGER than one fixed buffer across multiple reads -- the
    /// riskiest edge vs the owned path, since the fixed read is capped at `FIXED_RING_BUF_SIZE`
    /// (16 KiB) per call, so a 40 KiB payload takes several `recv_fixed`/extend cycles that must all
    /// land contiguously in `read_buf`.
    #[test]
    fn recv_batch_accumulates_a_payload_larger_than_one_fixed_buffer() {
        // > the 16 KiB fixed buffer -> the payload spans multiple fixed reads.
        const LEN: usize = 40 * 1024;
        tokio_uring::start(async {
            let std_listener =
                tokio_rt::bind_reuseport_std("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = std_listener.local_addr().unwrap();
            let listener = TcpListener::from_std(std_listener);

            let server = tokio_uring::spawn(async move {
                let rt = IoUringRuntime::new();
                let (mut stream, _peer) = rt.accept(&listener).await.unwrap();
                let mut read_buf = Vec::new();
                loop {
                    let n = recv_batch(&rt, &mut stream, &mut read_buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                }
                assert_eq!(
                    read_buf.len(),
                    LEN,
                    "every fixed read accumulated into read_buf"
                );
                assert!(
                    read_buf.iter().all(|&b| b == b'z'),
                    "payload intact + contiguous"
                );
            });

            let client = IoUringRuntime::new();
            let mut peer = client.connect(addr).await.unwrap();
            client.send(&mut peer, vec![b'z'; LEN]).await.unwrap();
            drop(peer);
            server.await.unwrap();
        });
    }

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

    /// Regression: a SHORT reply after a LONGER recv on the SAME reused buffer must write only the
    /// reply, never the stale trailing bytes of the prior read. tokio-uring's `set_init` is grow-only
    /// and the pool retains `init_len` across reuse, so `write_fixed_all(buf)` would send
    /// `bytes_init()` (= the prior 20) bytes; `send_fixed` slices to `data.len()` to prevent that
    /// leak. A pool of ONE buffer forces `send_fixed` to reuse the exact buffer `recv_fixed` filled.
    #[test]
    fn fixed_send_of_a_short_reply_after_a_longer_recv_leaks_no_stale_bytes() {
        tokio_uring::start(async {
            let std_listener =
                tokio_rt::bind_reuseport_std("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = std_listener.local_addr().unwrap();
            let listener = TcpListener::from_std(std_listener);

            let server = tokio_uring::spawn(async move {
                let runtime = IoUringRuntime::new();
                let (stream, _peer) = runtime.accept(&listener).await.unwrap();
                // ONE buffer: send_fixed necessarily reuses the buffer recv_fixed just filled, whose
                // retained init_len is 20 -- the exact grow-only trap.
                let ring = FixedRing::register(1, 4096).unwrap();
                let (res, buf) = recv_fixed(&stream, &ring).await.expect("buffer available");
                let n = res.unwrap();
                assert_eq!(
                    n, 20,
                    "the long request set the reused buffer's init_len to 20"
                );
                drop(buf);
                // A 5-byte reply through that same buffer: must be exactly 5 bytes on the wire.
                send_fixed(&stream, &ring, b"+OK\r\n")
                    .await
                    .expect("fits + not drained")
                    .unwrap();
            });

            let client = IoUringRuntime::new();
            let mut peer = client.connect(addr).await.unwrap();
            let _ = client.send(&mut peer, vec![b'q'; 20]).await.unwrap();
            let res = client
                .recv(&mut peer, Vec::with_capacity(64))
                .await
                .unwrap();
            // With the bug this would be 20 bytes ("+OK\r\n" + 15 stale 'q's); the slice makes it 5.
            assert_eq!(
                &res.buf[..res.n],
                b"+OK\r\n",
                "reply must be exactly the 5 fresh bytes, no stale trailing bytes leaked"
            );
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

    /// `send_batch` (the OneShotFixed WRITE tier wired into the serve loop) round-trips
    /// byte-identical output whether the reply takes the REGISTERED fixed path (fits one buffer) or
    /// the owned fall-back (oversize `> FIXED_RING_BUF_SIZE`), and hands the buffer back for reuse.
    /// It runs this shard's resolved datapath (probe + register, or owned) on the running kernel;
    /// either path must satisfy the exact byte-for-byte flush contract the serve loop depends on.
    #[test]
    fn send_batch_round_trips_small_and_oversize_byte_identical() {
        use super::send_batch;
        tokio_uring::start(async {
            let std_listener =
                tokio_rt::bind_reuseport_std("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = std_listener.local_addr().unwrap();
            let listener = TcpListener::from_std(std_listener);

            // A small reply (the fixed path when a ring resolves) and an oversize reply
            // (`> FIXED_RING_BUF_SIZE`, forcing the owned fall-back INSIDE send_batch).
            let small = b"+PONG\r\n".to_vec();
            let big = vec![b'y'; super::FIXED_RING_BUF_SIZE + 100];
            let (small_c, big_c) = (small.clone(), big.clone());

            let server = tokio_uring::spawn(async move {
                let rt = IoUringRuntime::new();
                let (mut stream, _peer) = rt.accept(&listener).await.unwrap();
                // The returned buffer is handed back for reuse (like the owned send).
                let reused = send_batch(&rt, &mut stream, small_c).await.unwrap();
                assert_eq!(
                    reused, b"+PONG\r\n",
                    "send_batch hands the buffer back unmodified"
                );
                send_batch(&rt, &mut stream, big_c).await.unwrap();
            });

            let client = IoUringRuntime::new();
            let mut peer = client.connect(addr).await.unwrap();
            let want = small.len() + big.len();
            let mut got = Vec::with_capacity(want);
            while got.len() < want {
                let res = client
                    .recv(&mut peer, Vec::with_capacity(want - got.len()))
                    .await
                    .unwrap();
                if res.n == 0 {
                    break;
                }
                got.extend_from_slice(&res.buf[..res.n]);
            }
            let mut expect = small.clone();
            expect.extend_from_slice(&big);
            assert_eq!(
                got, expect,
                "send_batch output must be byte-identical across the fixed + owned paths"
            );
            server.await.unwrap();
        });
    }
}
