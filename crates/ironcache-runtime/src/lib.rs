// SPDX-License-Identifier: MIT OR Apache-2.0
//! The IronCache runtime seam (RUNTIME.md, RUNTIME_ABSTRACTION.md) over the
//! shared-nothing thread-per-core model (ADR-0002) and the determinism Env seam
//! (ADR-0003).
//!
//! ## Two layers
//!
//! 1. The [`Runtime`] trait: the minimal `accept` / `connect` / `recv` / `send` /
//!    `timer` / `spawn_on_shard` surface the command core compiles against, with associated
//!    `Listener` / `Stream` / `Buf` types fixed per backend. The core is generic
//!    over this trait so it monomorphizes with no `dyn` on the hot path
//!    (RUNTIME_ABSTRACTION.md). PR-1 ships exactly one backend (tokio+epoll/kqueue);
//!    monoio/glommio are future Cargo features behind the same trait.
//!
//! 2. The [`bootstrap`] layer: spins up one OS thread per shard, each with its own
//!    current-thread tokio runtime, plus a single acceptor thread that binds the
//!    one listening socket and round-robins accepted connections to the shards in
//!    userspace (portable load-balancing; kernel `SO_REUSEPORT` does not balance on
//!    macOS/BSD). A connection lives its whole life on the shard that adopts it,
//!    with no shared hot-path state. The multi-thread work-stealing scheduler is
//!    deliberately NOT used: work-stealing forces `Send + Sync` and re-introduces
//!    cross-core atomics, the opposite of shared-nothing (ADR-0002, RUNTIME.md).
//!
//! ## Freeze point
//!
//! The [`Runtime`] trait signature is a freeze point. Downstream crates write
//! their accept/serve loops against it; changing the method set or the owned-buffer
//! model is a breaking change to every backend.

#![cfg_attr(not(feature = "tokio"), allow(dead_code))]

use core::future::Future;
use core::time::Duration;
use std::net::SocketAddr;

/// An owned, growable byte buffer handed across the I/O seam.
///
/// All runtime I/O is owned-buffer, never a borrowed `&mut [u8]`
/// (RUNTIME_ABSTRACTION.md "owned-buffer model"): io_uring's completion model
/// requires the buffer to outlive the kernel call, so owned buffers are the only
/// model every backend (including the future io_uring ones) can satisfy. The
/// tokio backend pays one copy into this buffer on the readiness path; that copy
/// is on the portable fallback only.
pub trait IoBuf: AsRef<[u8]> + AsMut<[u8]> {
    /// The number of initialized/usable bytes.
    fn len(&self) -> usize;
    /// Whether the buffer currently holds no usable bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl IoBuf for Vec<u8> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
}

/// The maximum byte length any single framed argument (a RESP bulk-string body) on an
/// intra-cluster link (the Raft cluster-bus `RAFTMSG` control plane OR the replication stream)
/// may claim, bounding a memory-DoS from a FORGED huge length (PROD-3, HIGH severity).
///
/// The bus / repl frame parsers read a per-argument length off the wire and would otherwise accept
/// any value up to `usize::MAX`, letting a malicious peer drive an unbounded buffer growth /
/// allocation with a single `$999999999999\r\n` header (the serve loop keeps reading to satisfy the
/// claimed body length). The parsers enforce this cap against the claimed length BEFORE any
/// allocation, so an over-cap frame is rejected (the connection is dropped) rather than OOMing the
/// node.
///
/// 512 MiB matches Redis's `proto-max-bulk-len` default: comfortably above any legitimate
/// intra-cluster frame -- a Raft `AppendEntries` batch (a handful of small log entries) or a single
/// replication entry (one key's encoded value) -- while bounding the forged-length vector to a
/// single sane allocation ceiling. It lives here (NOT behind the `tls` feature) because the bound
/// is a parser-correctness fix that applies to BOTH the TLS and the (default) plaintext path: a
/// plaintext cluster is hardened against the DoS too.
pub const MAX_CLUSTER_FRAME_LEN: usize = 512 * 1024 * 1024;

/// The result of one `recv`: the (possibly grown) buffer and the count of bytes
/// read into it. A read count of `0` signals a clean peer close (EOF).
pub struct RecvResult<B> {
    /// The buffer ownership returned to the caller (owned-buffer model).
    pub buf: B,
    /// Bytes read; `0` means the peer closed.
    pub n: usize,
}

/// The minimal runtime surface the command core compiles against
/// (RUNTIME_ABSTRACTION.md). Deliberately small: the thread-per-core backends
/// produce `!Send` futures, so a fat ecosystem trait cannot be satisfied by all
/// three backends, while this set can. There is no global `spawn`; work pins to
/// its core through [`Runtime::spawn_on_shard`] (shared-nothing, ADR-0002).
pub trait Runtime {
    /// The bound listening socket type.
    type Listener;
    /// The connected stream type.
    type Stream;
    /// The owned buffer type used by `recv`/`send`.
    type Buf: IoBuf;
    /// The error type for I/O operations.
    type Error;

    /// Accept the next inbound connection on `listener`, returning the stream and
    /// the peer address.
    fn accept(
        &self,
        listener: &Self::Listener,
    ) -> impl Future<Output = Result<(Self::Stream, SocketAddr), Self::Error>>;

    /// Open an OUTBOUND connection to `addr`, returning a connected stream of the
    /// same `Stream` type [`Runtime::accept`] yields, so `recv` / `send` operate on
    /// it uniformly.
    ///
    /// This is the node-to-node counterpart of `accept`: the cluster control plane,
    /// replication, and migration links (CONTROL_PLANE.md / REPLICATION.md /
    /// MIGRATION.md) each need a node to act as a client to its peers, which the
    /// inbound-only `accept` cannot provide. Like `accept`, it is real I/O on a
    /// production backend; a deterministic-simulation `Runtime` (TESTING.md) drives
    /// it through a virtual network for replayable multi-node tests.
    fn connect(&self, addr: SocketAddr) -> impl Future<Output = Result<Self::Stream, Self::Error>>;

    /// Read from `stream` into the owned `buf`, appending to its existing
    /// contents, and return the buffer plus the byte count (0 = peer closed).
    fn recv(
        &self,
        stream: &mut Self::Stream,
        buf: Self::Buf,
    ) -> impl Future<Output = Result<RecvResult<Self::Buf>, Self::Error>>;

    /// Write all of the bytes in the owned `buf` to `stream`, then RETURN the
    /// buffer so the caller (or a buffer pool) can reclaim it.
    ///
    /// `send` is owned-buffer and symmetric with `recv`, never a borrowed
    /// `&[u8]` (RUNTIME_ABSTRACTION.md "all I/O is owned-buffer, never borrowed").
    /// A future io_uring fixed-buffer backend needs the write buffer to outlive
    /// the kernel completion, which a borrow cannot honor; returning the owned
    /// buffer is the only model every backend can satisfy.
    fn send(
        &self,
        stream: &mut Self::Stream,
        buf: Self::Buf,
    ) -> impl Future<Output = Result<Self::Buf, Self::Error>>;

    /// Complete after `dur` elapses. The seam's canonical timer; backends arm
    /// their native timer under it (RUNTIME_ABSTRACTION.md "timer abstraction").
    fn timer(&self, dur: Duration) -> impl Future<Output = ()>;

    /// Spawn `task` on the current shard's executor. There is no cross-core
    /// `spawn`: a task pins to the core that spawned it (ADR-0002). The future may
    /// be `!Send` (thread-per-core property), so this does not require `Send`.
    fn spawn_on_shard<F>(&self, task: F)
    where
        F: Future<Output = ()> + 'static;
}

#[cfg(feature = "tokio")]
pub mod tokio_rt;
#[cfg(feature = "tokio")]
pub use tokio_rt::TokioRuntime;

// The OPTIONAL Linux io_uring backend (PROD-10 / #28, docs/design/IOURING_DATAPATH.md): a
// SECOND `Runtime` impl behind the default-OFF `io_uring` feature, gated to Linux so the
// feature is inert on macOS/Windows/BSD (the tokio backend always serves those). The default
// build (no `io_uring` feature) never compiles this module, never pulls tokio-uring/io-uring,
// and is byte-unchanged; tokio remains the default + the non-Linux/TLS fallback. See the
// module docs for the shared-nothing fit and the v1 scope.
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub mod io_uring_rt;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use io_uring_rt::{IoUringRuntime, run_shards_uring};

// The embedded rustls CLIENT-listener TLS layer (#105, docs/design/TLS.md): the
// `ClientStream` enum the serve loop reads/writes (plain TcpStream OR a rustls
// TlsStream) plus the cert/key acceptor builder. Behind the `tls` feature (default
// ON); with `tls` off the crate has no rustls dep and exposes only the plaintext
// path (byte-unchanged). It is part of the I/O seam, NOT the Runtime trait: the
// trait's `Stream` type is unchanged, so the cluster-bus / repl links are untouched.
#[cfg(feature = "tls")]
pub mod tls;
#[cfg(feature = "tls")]
pub use tls::{ClientStream, HANDSHAKE_TIMEOUT, TlsConfigError, accept_tls, build_acceptor};
// Intra-cluster transport security (PROD-3): the node-to-node secure stream + the client-side
// handshake + the shared-secret peer auth + the forged-length frame bound, reused by the Raft
// cluster-bus and the replication transports. Behind the same `tls` feature (default ON).
#[cfg(feature = "tls")]
pub use tls::{
    CLUSTER_TLS_SERVER_NAME, MAX_SECRET_LEN, SecretError, SecureStream, accept_cluster_tls,
    authenticate_peer, authenticate_peer_bounded, build_cluster_client_config, connect_tls,
    constant_time_eq, read_peer_secret, send_secret,
};

pub mod bootstrap;
pub use bootstrap::{ShardConfig, ShardId, ShardSet, available_shards};

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn timer_completes() {
        let rt = TokioRuntime::new();
        // A small timer should resolve promptly without panicking.
        rt.timer(Duration::from_millis(1)).await;
    }

    #[test]
    fn accept_recv_send_roundtrip() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // A current-thread runtime with a LocalSet, mirroring how a shard runs.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            let runtime = TokioRuntime::new();
            let listener = tokio_rt::bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();

            let server = tokio::task::spawn_local(async move {
                let (mut stream, _peer) = runtime.accept(&listener).await.unwrap();
                let buf: Vec<u8> = Vec::with_capacity(64);
                let res = runtime.recv(&mut stream, buf).await.unwrap();
                assert_eq!(&res.buf[..res.n], b"PING\r\n");
                let reply = b"+PONG\r\n".to_vec();
                // send returns the buffer (owned-buffer model); reclaim it.
                let returned = runtime.send(&mut stream, reply).await.unwrap();
                assert_eq!(returned, b"+PONG\r\n");
            });

            let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
            client.write_all(b"PING\r\n").await.unwrap();
            let mut reply = [0u8; 7];
            client.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"+PONG\r\n");
            server.await.unwrap();
        });
    }

    #[test]
    fn connect_send_recv_roundtrip() {
        // The outbound connect() seam: a client built ENTIRELY on Runtime methods
        // (connect + send + recv) talks to a Runtime accept loop, with no raw tokio
        // on the client side. This is the node-to-node path the cluster control
        // plane builds on.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            let runtime = TokioRuntime::new();
            let listener = tokio_rt::bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();

            let server = tokio::task::spawn_local(async move {
                let (mut stream, _peer) = runtime.accept(&listener).await.unwrap();
                let buf: Vec<u8> = Vec::with_capacity(64);
                let res = runtime.recv(&mut stream, buf).await.unwrap();
                assert_eq!(&res.buf[..res.n], b"PING\r\n");
                let _ = runtime
                    .send(&mut stream, b"+PONG\r\n".to_vec())
                    .await
                    .unwrap();
            });

            let client = TokioRuntime::new();
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
