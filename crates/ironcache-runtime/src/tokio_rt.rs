// SPDX-License-Identifier: MIT OR Apache-2.0
//! The tokio current-thread backend for [`crate::Runtime`].
//!
//! This is the portable epoll/kqueue backend (RUNTIME.md): the only backend that
//! runs on kernels without io_uring and on macOS/BSD, and a first-class release
//! target, not dev-only. It uses tokio in CURRENT-THREAD mode; the multi-thread
//! work-stealing scheduler is rejected because work-stealing violates the
//! shared-nothing model (ADR-0002). Each shard runs its own current-thread
//! runtime on its own OS thread (see [`crate::bootstrap`]).

use crate::{IoBuf, RecvResult, Runtime};
use core::future::Future;
use core::time::Duration;
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The tokio runtime backend. Zero-sized: it carries no shared state (it must
/// not, per shared-nothing); per-shard state lives on the shard's thread.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioRuntime;

impl TokioRuntime {
    /// Construct the backend handle.
    #[must_use]
    pub fn new() -> Self {
        TokioRuntime
    }
}

impl Runtime for TokioRuntime {
    type Listener = TcpListener;
    type Stream = TcpStream;
    type Buf = Vec<u8>;
    type Error = io::Error;

    async fn accept(
        &self,
        listener: &Self::Listener,
    ) -> Result<(Self::Stream, SocketAddr), Self::Error> {
        let (stream, peer) = listener.accept().await?;
        // Disable Nagle: request/reply caches want low latency over coalescing.
        let _ = stream.set_nodelay(true);
        Ok((stream, peer))
    }

    async fn connect(&self, addr: SocketAddr) -> Result<Self::Stream, Self::Error> {
        let stream = TcpStream::connect(addr).await?;
        // Disable Nagle to match accept: node-to-node request/reply wants low
        // latency over coalescing, same as the inbound data path.
        let _ = stream.set_nodelay(true);
        Ok(stream)
    }

    async fn recv(
        &self,
        stream: &mut Self::Stream,
        mut buf: Self::Buf,
    ) -> Result<RecvResult<Self::Buf>, Self::Error> {
        // Owned-buffer model: read appends into the caller's buffer. We reserve a
        // read window, read into it, then truncate to what arrived. The tokio
        // readiness path copies into this owned buffer (the portable-fallback copy
        // RUNTIME_ABSTRACTION.md describes).
        let start = IoBuf::len(&buf);
        let want = 16 * 1024;
        buf.resize(start + want, 0);
        let n = stream.read(&mut buf[start..]).await?;
        buf.truncate(start + n);
        Ok(RecvResult { buf, n })
    }

    async fn send(
        &self,
        stream: &mut Self::Stream,
        buf: Self::Buf,
    ) -> Result<Self::Buf, Self::Error> {
        // Owned-buffer model: write the buffer's bytes, then hand the buffer back
        // to the caller so it (or a pool) can reclaim/reuse the allocation. A
        // future io_uring fixed-buffer backend relies on this ownership return.
        stream.write_all(buf.as_ref()).await?;
        Ok(buf)
    }

    async fn timer(&self, dur: Duration) -> () {
        tokio::time::sleep(dur).await;
    }

    fn spawn_on_shard<F>(&self, task: F)
    where
        F: Future<Output = ()> + 'static,
    {
        // spawn_local pins to the current thread's LocalSet (the shard executor),
        // never migrating across cores (ADR-0002). It does not require Send.
        tokio::task::spawn_local(task);
    }
}

/// Bind a `std` TCP listener with `SO_REUSEPORT`, WITHOUT requiring a tokio
/// reactor. Use this for a pre-flight bind probe in synchronous (non-async)
/// context; convert to a tokio listener inside the shard's runtime with
/// [`bind_reuseport`].
///
/// On platforms without `SO_REUSEPORT` (e.g. Windows) only one shard can bind;
/// callers fall back to a single accept loop. macOS and Linux both support it.
pub fn bind_reuseport_std(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    // set_reuse_port exists on unix; on unsupported targets this is a no-op shim.
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    Ok(socket.into())
}

/// Bind a tokio TCP listener with `SO_REUSEPORT` so every shard can bind the same
/// address and the kernel load-balances accepts across them (RUNTIME.md
/// per-shard accept). MUST be called inside a tokio runtime (it registers the
/// listener with the reactor); use [`bind_reuseport_std`] for a non-async probe.
pub fn bind_reuseport(addr: SocketAddr) -> io::Result<TcpListener> {
    TcpListener::from_std(bind_reuseport_std(addr)?)
}

/// Bind a tokio TCP listener EXCLUSIVELY (plain `bind`, NO `SO_REUSEPORT`), so a second
/// binder of the same address FAILS with `EADDRINUSE` instead of silently SHARING the port
/// and having the kernel load-balance traffic across both sockets.
///
/// This is the right primitive for a SINGLE per-node listener that must NOT alias any other
/// service's port (HA-7d's replication listener): with `SO_REUSEPORT` two listeners on the
/// same address co-exist and the kernel splits incoming connections between them, so a repl
/// listener that happened to land on another service's port (e.g. the Raft cluster-bus port
/// of an adjacent ephemeral test port) would STEAL half that service's traffic. A plain
/// exclusive bind turns such a collision into a clean, observable bind error the caller can
/// log and degrade on, never a silent cross-wiring. MUST be called inside a tokio runtime.
pub fn bind_exclusive(addr: SocketAddr) -> io::Result<TcpListener> {
    let std_listener = std::net::TcpListener::bind(addr)?;
    // tokio's `from_std` requires a NONBLOCKING listener (a blocking one would stall the reactor);
    // `std::net::TcpListener::bind` returns a blocking socket, so flip it here (the reuseport
    // helper sets this via socket2; we must do the same for the plain path).
    std_listener.set_nonblocking(true)?;
    TcpListener::from_std(std_listener)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuseport_bind_allows_two_listeners_same_addr() {
        // from_std registers with the tokio reactor, so run inside a runtime.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let l1 = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = l1.local_addr().unwrap();
            // Binding the same concrete addr again must succeed under SO_REUSEPORT.
            let l2 = bind_reuseport(addr);
            assert!(l2.is_ok(), "SO_REUSEPORT second bind failed: {l2:?}");
        });
    }
}
