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

/// Adopt an INHERITED listening-socket fd (systemd socket-activation, #389 Phase 2a) as a `std`
/// `TcpListener`, instead of binding our own. `sd_listen_fds` (parsed by [`crate::listen_fds`]) hands
/// the fd that SYSTEMD opened + kept open across the upgrade restart, so the listen queue is never
/// closed and clients QUEUE in the backlog instead of getting `ECONNREFUSED`.
///
/// Fail-closed: validates the fd is a TCP STREAM socket (`SO_TYPE`) before use, and sets it
/// non-blocking to match the single acceptor loop ([`crate::bootstrap`]). Takes SOLE ownership of the
/// fd. (systemd with `Accept=no ListenStream=` always passes a LISTENING stream socket; the type
/// check rejects the realistic misconfig -- a non-socket fd or a `ListenDatagram=` UDP socket.)
///
/// # Errors
///
/// Returns `InvalidInput` if the fd is not a TCP stream socket, or the underlying `io::Error` if the
/// `SO_TYPE` query fails (e.g. a non-socket fd). The caller falls back to [`bind_reuseport_std`]
/// rather than failing the boot.
#[cfg(unix)]
pub fn adopt_listener_fd(fd: std::os::fd::RawFd) -> io::Result<std::net::TcpListener> {
    use std::os::fd::FromRawFd;
    // SAFETY: this is a systemd-inherited fd the boot owns EXCLUSIVELY -- the sd_listen_fds parser
    // confirmed LISTEN_PID names our pid, and each fd is adopted at most once, so taking ownership
    // here does not alias another owner.
    let socket = unsafe { Socket::from_raw_fd(fd) };
    // Fail closed if systemd handed us something that is not a TCP STREAM socket: a non-socket fd
    // makes `type()` error (`ENOTSOCK`), and a DGRAM/other socket (a `ListenDatagram=` misconfig)
    // returns the wrong type. systemd with `Accept=no ListenStream=` always passes a LISTENING stream
    // socket, so this guards a misconfigured unit; the caller self-binds rather than serve on a bad fd.
    if socket.r#type()? != Type::STREAM {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "inherited socket-activation fd is not a TCP stream socket",
        ));
    }
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// Choose the RESP listener: ADOPT a systemd socket-activation inherited fd if one was passed
/// (`LISTEN_FDS`, #389 -- the listen queue survives an upgrade restart, no `ECONNREFUSED`), else
/// SELF-BIND `addr` with `SO_REUSEPORT`.
///
/// FAIL-OPEN: not socket-activated, a malformed `LISTEN_*` environment, or an unusable inherited fd
/// all fall back to self-bind, so a non-socket-activated boot is byte-unchanged. The binary emits
/// the LOUD adopt-vs-fallback boot log (#562) from the pure [`crate::listen_fds::classify`] over the
/// SAME env parse this function acts on; the ONE case that log cannot see -- an inherited fd that
/// parsed fine but failed validation here (a `ListenDatagram=` UDP socket) -- is surfaced with an
/// `eprintln!` below, matching this crate's boot-diagnostic convention (it takes no logging dep).
///
/// ADDRESS AUTHORITY: when socket-activated, the effective listen address is whatever SYSTEMD opened
/// the socket on (the `.socket` unit's `ListenStream=`); `addr` (the server's `bind` config) is used
/// ONLY on the self-bind fallback. The packaged `ironcache.socket` defaults its `ListenStream` to
/// LOOPBACK to match the config's safe default, so enabling socket activation does not silently widen
/// exposure; an operator binding beyond loopback must set BOTH the unit and `--bind`.
///
/// # Errors
///
/// Returns the `io::Error` only if the self-bind itself fails (e.g. the address is in use).
#[cfg(unix)]
pub fn listener_for(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    match crate::listen_fds::from_env() {
        // Socket-activated: adopt the RESP `ListenStream` fd -- the one NAMED `resp` when
        // `LISTEN_FDNAMES` disambiguates a multi-socket unit, else the first passed fd (fd 3, the
        // single-socket default). A validation failure on the inherited fd degrades to a self-bind
        // rather than failing the boot, but is logged (the one adopt-vs-fallback case the binary's
        // #562 boot log cannot see, since the env parsed cleanly and only the fd itself is unusable).
        Ok(fds) if !fds.is_empty() => {
            let fd = crate::listen_fds::resp_listener_fd(&fds).map_or(fds[0].fd, |f| f.fd);
            adopt_listener_fd(fd).or_else(|e| {
                eprintln!(
                    "socket-activation: inherited fd {fd} could not be adopted ({e}); FELL BACK to \
                     self-binding {addr}"
                );
                bind_reuseport_std(addr)
            })
        }
        // Not socket-activated (or a malformed/foreign LISTEN_* env): self-bind, unchanged behavior.
        _ => bind_reuseport_std(addr),
    }
}

/// Non-Unix fallback: socket-activation is a Unix/systemd feature, so always self-bind.
#[cfg(not(unix))]
pub fn listener_for(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    bind_reuseport_std(addr)
}

/// Apply `SO_KEEPALIVE` with `secs` idle time to a tokio [`TcpStream`] at ACCEPT (Redis
/// `tcp-keepalive`). `secs == 0` DISABLES keepalive (the option is left off). This borrows the
/// stream's fd via [`socket2::SockRef`] WITHOUT taking ownership (no fd dance / `unsafe`), so the
/// tokio stream remains the sole owner. Errors are ignored to match the `set_nodelay` posture (a
/// connection still functions without the keepalive probe); a failure is non-fatal and merely means
/// a dead peer is not actively reaped on this connection.
///
/// The keepalive RETRY count / interval are left at the OS defaults (Redis sets only the idle time
/// portably; the retry tuning is platform-specific). This is read from the runtime overlay AT
/// ACCEPT, so a `CONFIG SET tcp-keepalive` applies to newly-accepted connections (an established
/// connection keeps the option it was accepted with, matching Redis).
pub fn set_keepalive(stream: &TcpStream, secs: u64) {
    let sock = socket2::SockRef::from(stream);
    if secs == 0 {
        let _ = sock.set_keepalive(false);
        return;
    }
    let _ = sock.set_keepalive(true);
    // The idle time before the first keepalive probe. socket2 handles any platform-specific
    // clamping of the duration; best-effort (errors are ignored, like the other socket opts).
    let idle = Duration::from_secs(secs);
    let _ = sock.set_tcp_keepalive(&socket2::TcpKeepalive::new().with_time(idle));
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

    /// Area C: `set_keepalive` enables SO_KEEPALIVE with a non-zero idle time and DISABLES it with
    /// `0`, applied on an accepted loopback stream. Asserts the kernel option state via the same
    /// `socket2::SockRef` borrow (non-flaky, no timing). Mirrors how the serve loop applies it at
    /// accept; the per-platform retry/interval tuning is left at OS defaults so only the on/off +
    /// idle-time portion is asserted.
    #[test]
    fn set_keepalive_enables_and_disables_on_accepted_stream() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            let _client = TcpStream::connect(addr).await.unwrap();
            let (server, _peer) = listener.accept().await.unwrap();
            // A non-zero interval turns keepalive ON.
            set_keepalive(&server, 120);
            let sock = socket2::SockRef::from(&server);
            assert_eq!(sock.keepalive().ok(), Some(true), "keepalive should be ON");
            // `0` turns it OFF.
            set_keepalive(&server, 0);
            let sock = socket2::SockRef::from(&server);
            assert_eq!(
                sock.keepalive().ok(),
                Some(false),
                "keepalive should be OFF"
            );
        });
    }

    /// #389: `adopt_listener_fd` takes over a REAL inherited listening socket (the systemd
    /// socket-activation case) -- it serves the same address and accepts connections. Uses
    /// `try_clone().into_raw_fd()` to hand `adopt` an independent, owned fd (no libc dup needed).
    #[cfg(unix)]
    #[test]
    fn adopt_listener_fd_takes_over_a_real_listening_socket() {
        use std::os::fd::IntoRawFd;
        let original = bind_reuseport_std("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = original.local_addr().unwrap();
        // A dup'd fd `adopt` can own exclusively (the original stays valid + drops normally).
        let fd = original.try_clone().unwrap().into_raw_fd();

        let adopted = adopt_listener_fd(fd).expect("adopts a listening socket");
        assert_eq!(
            adopted.local_addr().unwrap(),
            addr,
            "the adopted listener serves the inherited socket's address"
        );
        adopted.set_nonblocking(false).unwrap();
        let _client = std::net::TcpStream::connect(addr).unwrap();
        let (_conn, _peer) = adopted.accept().expect("the adopted listener accepts");
    }

    /// #389 fail-closed: `adopt_listener_fd` REJECTS a fd that is not a TCP STREAM socket (a
    /// `ListenDatagram=` misconfig hands a UDP socket), so the caller self-binds instead of serving on
    /// a bad fd.
    #[cfg(unix)]
    #[test]
    fn adopt_listener_fd_rejects_a_non_stream_socket() {
        use std::os::fd::IntoRawFd;
        // A UDP (DGRAM) socket -- the wrong socket type for a RESP listener.
        let udp = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
        let fd = udp.into_raw_fd();
        assert!(
            adopt_listener_fd(fd).is_err(),
            "a non-stream (UDP) socket must be rejected fail-closed"
        );
    }

    /// #389: `listener_for` self-binds when NOT socket-activated (no valid `LISTEN_PID`/`LISTEN_FDS`
    /// in the env), so a normal (non-systemd) boot is unchanged. The test harness has no `LISTEN_PID`
    /// naming this pid, so `from_env` returns an error and `listener_for` takes the self-bind path.
    #[cfg(unix)]
    #[test]
    fn listener_for_self_binds_when_not_socket_activated() {
        let listener = listener_for("127.0.0.1:0".parse().unwrap())
            .expect("self-binds when not socket-activated");
        assert!(
            listener.local_addr().is_ok(),
            "a working self-bound listener"
        );
    }
}
