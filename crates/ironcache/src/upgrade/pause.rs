// SPDX-License-Identifier: MIT OR Apache-2.0
//! THE LOSSLESS WRITE-FREEZE (#388): the data-loss-window-closing step of `ironcache upgrade`.
//!
//! v1 (#387) is SAVE-first: it issues a loopback `SAVE` then swaps + restarts. That is data-safe in
//! the sense that the snapshot survives the restart, but it leaves a small WINDOW: between the `SAVE`
//! completing and the old process actually dying at `systemctl restart`, the still-living old process
//! can ACKNOWLEDGE writes that are NOT in the snapshot, and those writes are lost on restart.
//!
//! This module closes that window using the engine primitive `CLIENT PAUSE <ms> WRITE` (PROD-7). The
//! orchestrator (see [`super::run_with`]) issues the freeze BEFORE the final `SAVE`, in this order:
//!
//! 1. `CLIENT PAUSE <ms> WRITE` -- node-wide; the serve loop holds every WRITE command before it is
//!    dispatched (reads and admin like `SAVE`/`INFO`/`PING` still proceed), so no further write is
//!    executed or acked while the freeze is active.
//! 2. let in-flight writes DRAIN -- a short fixed pause so any command already buffered on a
//!    connection at the instant the freeze landed finishes and is reflected in the snapshot.
//! 3. `SAVE` (confirm `LASTSAVE` advanced) -- captures a state AFTER which no write is acked.
//! 4. swap + `systemctl restart` -- the old process dies; the new one boots UNPAUSED from the
//!    complete snapshot and accepts writes once `/readyz` is `200`.
//!
//! ## Why this is lossless (the engine no-ack proof)
//!
//! The serve loop gates each command against the pause window BEFORE it is dispatched. Under a
//! `WRITE` pause a write command BLOCKS there (it does not reject/error) while reads and admin
//! commands (`SAVE`, `INFO`, `PING`) pass through -- so once the freeze is active no further WRITE on
//! any connection is executed or acknowledged, yet the orchestrator's own `SAVE` (step 3) still runs.
//! The only writes that can still be acked after the freeze are those ALREADY buffered on a
//! connection when the freeze landed; the DRAIN pause in step 2 lets those finish and land in the
//! snapshot. So the `SAVE` in step 3 captures a state after which NO acknowledged write exists
//! outside it. (A `WRITE` pause that also stalled reads/`SAVE` would deadlock this very save; the
//! engine pause is genuinely write-only for exactly this reason.)
//!
//! ## No unpause on the happy path; UNPAUSE on an abort
//!
//! On a normal upgrade the old process DIES at the restart, so its pause state evaporates with it and
//! the fresh new process boots unpaused -- NO explicit unpause is needed. But if the upgrade ABORTS
//! AFTER the freeze and BEFORE the restart (e.g. the final `SAVE` fails, or a preflight fails), the
//! old process keeps running, and leaving it WRITE-frozen would wedge production until the window
//! elapses. So the orchestrator issues a best-effort `CLIENT UNPAUSE` on any abort-after-pause (see
//! [`Pauser::unpause`]).
//!
//! ## Credentials + determinism seam
//!
//! Mirrors [`super::save`]: an optional `requirepass` rides only the loopback socket (never argv /
//! logs), and the connect/IO timeout + the drain pause are driven through the `ironcache-runtime`
//! timer seam (never wall-clock), so the determinism invariant lint is satisfied.

use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// How long the whole pause/unpause exchange (connect + AUTH + the one command) is allowed to take.
/// Generous but bounded so a wedged server does not hang the upgrade. Driven through the runtime
/// timer seam.
const PAUSE_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);

/// The fixed DRAIN pause between issuing `CLIENT PAUSE WRITE` and the final `SAVE`: long enough for
/// every connection to finish the command batch it had already buffered when the freeze landed and
/// reach its post-batch stall, so no acked write is left outside the snapshot. The serve loop re-
/// checks the pause within ~50ms, so this is comfortably above one batch-quantum. Driven through the
/// runtime timer seam (no wall-clock sleep).
pub const DRAIN_PAUSE: Duration = Duration::from_millis(500);

/// The safety MARGIN added on top of `--health-timeout` when deriving the freeze window: the old
/// process is killed early (right after the swap, at `systemctl restart`), so the window only has to
/// outlast pause -> restart, NOT the whole health gate; a window of `health_timeout + MARGIN` is
/// comfortably safe and self-cancels when the old process dies. See [`derive_pause_window`].
pub const PAUSE_WINDOW_MARGIN: Duration = Duration::from_secs(10);

/// Derive the freeze window (the `<ms>` for `CLIENT PAUSE <ms> WRITE`) from the health-gate budget.
///
/// The window must COMFORTABLY exceed the time from the freeze to the old process dying. The old
/// process dies at `systemctl restart`, which the orchestrator issues right after the swap -- i.e.
/// shortly after the freeze, NOT after the whole health gate. So `health_timeout + [`PAUSE_WINDOW_MARGIN`]`
/// is a generous upper bound (the restart almost always happens far sooner), and the window is moot
/// once the old process is gone. Saturating + clamped to a `u64` millis so an absurd `--health-timeout`
/// cannot overflow.
#[must_use]
pub fn derive_pause_window(health_timeout: Duration) -> u64 {
    let total = health_timeout.saturating_add(PAUSE_WINDOW_MARGIN);
    u64::try_from(total.as_millis()).unwrap_or(u64::MAX)
}

/// A typed write-freeze failure (ERRORS.md: no stringly-typed errors). Mirrors [`super::save::SaveError`]'s
/// shape: a freeze failure means we could not establish the no-ack window we promised, so it is FATAL
/// (the orchestrator must not swap over a server it could not freeze).
#[derive(Debug, thiserror::Error)]
pub enum PauseError {
    /// Could not connect to the running server on the RESP address.
    #[error("connecting to the running server at {addr}: {detail}")]
    Connect {
        /// The RESP address.
        addr: String,
        /// Why the connect failed.
        detail: String,
    },
    /// An IO error during the exchange (write/read on the socket).
    #[error("RESP IO during the write-freeze: {0}")]
    Io(String),
    /// The server rejected `AUTH` (wrong/missing password).
    #[error("AUTH failed during the write-freeze: {0}")]
    Auth(String),
    /// The server replied an error to `CLIENT PAUSE` / `CLIENT UNPAUSE` (it should never, but a
    /// typed error beats a silent assumption).
    #[error("the server rejected {verb}: {detail}")]
    Rejected {
        /// Which verb produced it (`CLIENT PAUSE` / `CLIENT UNPAUSE`).
        verb: String,
        /// The server's error text.
        detail: String,
    },
    /// A reply was not the RESP shape we expected.
    #[error("unexpected RESP reply during the write-freeze ({context}): {detail}")]
    Protocol {
        /// Which step produced it.
        context: String,
        /// The unexpected reply (lossy).
        detail: String,
    },
    /// The exchange exceeded [`PAUSE_EXCHANGE_TIMEOUT`].
    #[error("the write-freeze timed out after {0:?} (the server did not answer in time)")]
    Timeout(Duration),
}

/// What the write-freeze step needs: the RESP `host:port`, an optional loopback password, and the
/// freeze window in milliseconds.
#[derive(Debug, Clone)]
pub struct PauseTarget {
    /// The RESP `host:port` of the running server.
    pub resp_addr: String,
    /// An optional `requirepass` (sent only over loopback, never logged).
    pub auth: Option<String>,
    /// The `<ms>` for `CLIENT PAUSE <ms> WRITE` (see [`derive_pause_window`]).
    pub window_ms: u64,
}

/// The write-freeze seam, so the orchestrator's losslessness step is unit-testable with a mock (a
/// real loopback `CLIENT PAUSE` needs a live server). v1 ships [`LoopbackPauser`]; a streamed/handoff
/// lossless variant (Phase 2) can slot behind the same trait.
pub trait Pauser {
    /// FREEZE writes node-wide: issue `CLIENT PAUSE <window_ms> WRITE`, then let in-flight writes
    /// DRAIN, so a subsequent `SAVE` captures a state after which no acknowledged write exists.
    ///
    /// # Errors
    ///
    /// Returns a [`PauseError`] when the freeze could not be established (connect/auth/protocol/
    /// timeout) -- a FATAL condition (we never swap over a server we could not freeze).
    fn freeze(&self, target: &PauseTarget) -> Result<(), PauseError>;

    /// UN-FREEZE writes node-wide: issue `CLIENT UNPAUSE`. Called ONLY on an abort-after-freeze (the
    /// upgrade gave up before the restart), so the still-living old process is not left write-frozen.
    /// On a NORMAL upgrade this is never called -- the old process dies at the restart and its pause
    /// state dies with it.
    ///
    /// # Errors
    ///
    /// Returns a [`PauseError`] on connect/auth/protocol/timeout failure.
    fn unfreeze(&self, target: &PauseTarget) -> Result<(), PauseError>;
}

/// The v1 pauser: an explicit loopback RESP `CLIENT PAUSE <ms> WRITE` (then a drain) and
/// `CLIENT UNPAUSE`, reusing the [`super::save`] RESP/AUTH plumbing shape.
pub struct LoopbackPauser;

impl Pauser for LoopbackPauser {
    fn freeze(&self, target: &PauseTarget) -> Result<(), PauseError> {
        run_exchange(&target.resp_addr, target.auth.as_deref(), |stream| {
            Box::pin(freeze_then_drain(stream, target.window_ms))
        })
    }

    fn unfreeze(&self, target: &PauseTarget) -> Result<(), PauseError> {
        run_exchange(&target.resp_addr, target.auth.as_deref(), |stream| {
            Box::pin(unpause_only(stream))
        })
    }
}

/// A boxed future over a connected, already-AUTH'd stream -- the per-verb body of an exchange.
type ExchangeBody<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), PauseError>> + 'a>>;

/// Run one bounded loopback exchange: build a throwaway current-thread tokio runtime (this is the
/// short-lived CLI process; no shard executor is involved), connect, optionally `AUTH`, then drive
/// `body` over the stream, all under [`PAUSE_EXCHANGE_TIMEOUT`] via the runtime timer seam.
fn run_exchange<F>(resp_addr: &str, password: Option<&str>, body: F) -> Result<(), PauseError>
where
    F: for<'a> FnOnce(&'a mut tokio::net::TcpStream) -> ExchangeBody<'a>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| PauseError::Io(format!("building the write-freeze runtime: {e}")))?;
    rt.block_on(bounded_exchange(resp_addr, password, body))
}

/// The connect -> AUTH -> `body` conversation, wrapped in the bounded [`PAUSE_EXCHANGE_TIMEOUT`] via
/// the runtime timer seam.
async fn bounded_exchange<F>(
    resp_addr: &str,
    password: Option<&str>,
    body: F,
) -> Result<(), PauseError>
where
    F: for<'a> FnOnce(&'a mut tokio::net::TcpStream) -> ExchangeBody<'a>,
{
    use ironcache_runtime::Runtime as _;
    let rt = ironcache_runtime::TokioRuntime::new();
    tokio::select! {
        biased;
        result = connect_auth_then(resp_addr, password, body) => result,
        () = rt.timer(PAUSE_EXCHANGE_TIMEOUT) => Err(PauseError::Timeout(PAUSE_EXCHANGE_TIMEOUT)),
    }
}

/// Connect, optionally `AUTH` (the password rides only the loopback socket), then run `body`.
async fn connect_auth_then<F>(
    resp_addr: &str,
    password: Option<&str>,
    body: F,
) -> Result<(), PauseError>
where
    F: for<'a> FnOnce(&'a mut tokio::net::TcpStream) -> ExchangeBody<'a>,
{
    let mut stream = tokio::net::TcpStream::connect(resp_addr)
        .await
        .map_err(|e| PauseError::Connect {
            addr: resp_addr.to_owned(),
            detail: e.to_string(),
        })?;

    if let Some(pw) = password {
        write_command(&mut stream, &[b"AUTH", pw.as_bytes()]).await?;
        match read_reply(&mut stream).await? {
            Reply::Simple(s) if s.eq_ignore_ascii_case("OK") => {}
            Reply::Error(e) => return Err(PauseError::Auth(e)),
            other => {
                return Err(PauseError::Protocol {
                    context: "AUTH".to_owned(),
                    detail: other.to_string(),
                });
            }
        }
    }

    body(&mut stream).await
}

/// Issue `CLIENT PAUSE <window_ms> WRITE`, confirm `+OK`, then let in-flight writes DRAIN through the
/// runtime timer seam ([`DRAIN_PAUSE`]) so they land in the subsequent snapshot.
async fn freeze_then_drain(
    stream: &mut tokio::net::TcpStream,
    window_ms: u64,
) -> Result<(), PauseError> {
    use ironcache_runtime::Runtime as _;
    let window = window_ms.to_string();
    write_command(stream, &[b"CLIENT", b"PAUSE", window.as_bytes(), b"WRITE"]).await?;
    expect_ok(stream, "CLIENT PAUSE").await?;

    // Let any command already buffered on a connection when the freeze landed finish and reach its
    // post-batch stall, so the upcoming SAVE reflects it. Through the runtime timer SEAM (not wall-
    // clock), keeping the determinism invariant.
    let rt = ironcache_runtime::TokioRuntime::new();
    rt.timer(DRAIN_PAUSE).await;
    Ok(())
}

/// Issue `CLIENT UNPAUSE` and confirm `+OK` (the abort-after-freeze un-wedge).
async fn unpause_only(stream: &mut tokio::net::TcpStream) -> Result<(), PauseError> {
    write_command(stream, &[b"CLIENT", b"UNPAUSE"]).await?;
    expect_ok(stream, "CLIENT UNPAUSE").await
}

/// Read one reply and require `+OK`; map an error reply to [`PauseError::Rejected`] and any other
/// shape to [`PauseError::Protocol`].
async fn expect_ok(stream: &mut tokio::net::TcpStream, verb: &str) -> Result<(), PauseError> {
    match read_reply(stream).await? {
        Reply::Simple(s) if s.eq_ignore_ascii_case("OK") => Ok(()),
        Reply::Error(e) => Err(PauseError::Rejected {
            verb: verb.to_owned(),
            detail: e,
        }),
        other => Err(PauseError::Protocol {
            context: verb.to_owned(),
            detail: other.to_string(),
        }),
    }
}

/// Encode `args` as a RESP2 command array and write it (the request-side encoder, mirroring
/// [`super::save`]).
async fn write_command(
    stream: &mut tokio::net::TcpStream,
    args: &[&[u8]],
) -> Result<(), PauseError> {
    let mut buf = Vec::with_capacity(32);
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    stream
        .write_all(&buf)
        .await
        .map_err(|e| PauseError::Io(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| PauseError::Io(e.to_string()))
}

/// The minimal RESP reply shapes the freeze needs (simple string, error). An integer/bulk/array is
/// unexpected for `CLIENT PAUSE`/`UNPAUSE` and is surfaced as a protocol error by the caller.
#[derive(Debug)]
enum Reply {
    /// `+<text>\r\n`
    Simple(String),
    /// `-<text>\r\n`
    Error(String),
    /// Any other leading byte, captured for the error message.
    Other(u8),
}

impl std::fmt::Display for Reply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reply::Simple(s) => write!(f, "+{s}"),
            Reply::Error(e) => write!(f, "-{e}"),
            Reply::Other(b) => write!(f, "unexpected RESP type byte '{}'", *b as char),
        }
    }
}

/// Read ONE RESP reply of a type the freeze expects (`+`, `-`). Reads byte-by-byte to the first
/// line's terminating CRLF; these replies are single-line. A connection close mid-reply is an IO
/// error.
async fn read_reply(stream: &mut tokio::net::TcpStream) -> Result<Reply, PauseError> {
    let first = read_one(stream).await?;
    let line = read_line(stream).await?;
    match first {
        b'+' => Ok(Reply::Simple(line)),
        b'-' => Ok(Reply::Error(line)),
        other => Ok(Reply::Other(other)),
    }
}

/// Read one byte, mapping EOF to an IO error (a closed connection mid-reply).
async fn read_one(stream: &mut tokio::net::TcpStream) -> Result<u8, PauseError> {
    let mut b = [0u8; 1];
    let n = stream
        .read(&mut b)
        .await
        .map_err(|e| PauseError::Io(e.to_string()))?;
    if n == 0 {
        return Err(PauseError::Io("connection closed mid-reply".to_owned()));
    }
    Ok(b[0])
}

/// Read up to and including the line's terminating `\r\n`, returning the line WITHOUT the CRLF. A
/// bounded read (a single RESP status line is tiny); a stray lone `\n` is tolerated.
async fn read_line(stream: &mut tokio::net::TcpStream) -> Result<String, PauseError> {
    let mut out = Vec::with_capacity(32);
    loop {
        let b = read_one(stream).await?;
        if b == b'\n' {
            break;
        }
        if b == b'\r' {
            let nl = read_one(stream).await?;
            if nl == b'\n' {
                break;
            }
            out.push(b'\r');
            out.push(nl);
            continue;
        }
        out.push(b);
        if out.len() > 4096 {
            return Err(PauseError::Io("reply line exceeded 4 KiB".to_owned()));
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn window_derivation_adds_margin() {
        // 30s budget -> (30 + 10)s = 40000 ms.
        assert_eq!(
            derive_pause_window(Duration::from_secs(30)),
            40_000,
            "health_timeout + PAUSE_WINDOW_MARGIN, in millis"
        );
        // 0s budget -> just the margin (10000 ms).
        assert_eq!(derive_pause_window(Duration::ZERO), 10_000);
        // A huge budget saturates rather than overflowing.
        assert_eq!(derive_pause_window(Duration::MAX), u64::MAX);
    }

    /// A scripted fake server: reads each command array and replies with the next scripted raw reply.
    /// Mirrors `save.rs`'s test fake.
    async fn fake_server(listener: TcpListener, replies: Vec<&'static [u8]>) {
        let (mut sock, _) = listener.accept().await.unwrap();
        for reply in replies {
            read_one_command(&mut sock).await;
            sock.write_all(reply).await.unwrap();
            sock.flush().await.unwrap();
        }
        let _ = sock.shutdown().await;
    }

    async fn read_one_command(sock: &mut tokio::net::TcpStream) {
        let n = read_count(sock, b'*').await;
        for _ in 0..n {
            let len = read_count(sock, b'$').await;
            let mut body = vec![0u8; len as usize + 2]; // + CRLF
            sock.read_exact(&mut body).await.unwrap();
        }
    }

    async fn read_count(sock: &mut tokio::net::TcpStream, prefix: u8) -> i64 {
        let mut p = [0u8; 1];
        sock.read_exact(&mut p).await.unwrap();
        assert_eq!(p[0], prefix, "expected RESP prefix");
        let mut s = String::new();
        loop {
            let mut b = [0u8; 1];
            sock.read_exact(&mut b).await.unwrap();
            if b[0] == b'\r' {
                let mut nl = [0u8; 1];
                sock.read_exact(&mut nl).await.unwrap();
                break;
            }
            s.push(b[0] as char);
        }
        s.parse().unwrap()
    }

    async fn spawn_fake(replies: Vec<&'static [u8]>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_server(listener, replies));
        addr
    }

    #[tokio::test(flavor = "current_thread")]
    async fn freeze_sends_client_pause_write_and_confirms_ok() {
        // CLIENT PAUSE <ms> WRITE -> +OK.
        let addr = spawn_fake(vec![b"+OK\r\n"]).await;
        connect_auth_then(&addr, None, |s| Box::pin(freeze_then_drain(s, 40_000)))
            .await
            .expect("freeze ok");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn freeze_with_auth_then_ok() {
        // AUTH +OK, then CLIENT PAUSE +OK.
        let addr = spawn_fake(vec![b"+OK\r\n", b"+OK\r\n"]).await;
        connect_auth_then(&addr, Some("s3cr3t"), |s| {
            Box::pin(freeze_then_drain(s, 40_000))
        })
        .await
        .expect("freeze ok with auth");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn freeze_auth_failure_is_typed() {
        let addr = spawn_fake(vec![b"-WRONGPASS invalid password\r\n"]).await;
        let err = connect_auth_then(&addr, Some("bad"), |s| {
            Box::pin(freeze_then_drain(s, 40_000))
        })
        .await
        .expect_err("auth failure");
        assert!(matches!(err, PauseError::Auth(_)), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn freeze_server_rejection_is_typed() {
        // CLIENT PAUSE replies an error -> Rejected.
        let addr = spawn_fake(vec![b"-ERR nope\r\n"]).await;
        let err = connect_auth_then(&addr, None, |s| Box::pin(freeze_then_drain(s, 40_000)))
            .await
            .expect_err("rejection");
        assert!(
            matches!(err, PauseError::Rejected { ref verb, .. } if verb == "CLIENT PAUSE"),
            "{err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unpause_sends_client_unpause_and_confirms_ok() {
        let addr = spawn_fake(vec![b"+OK\r\n"]).await;
        connect_auth_then(&addr, None, |s| Box::pin(unpause_only(s)))
            .await
            .expect("unpause ok");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connect_failure_is_typed() {
        let err = connect_auth_then("127.0.0.1:1", None, |s| {
            Box::pin(freeze_then_drain(s, 40_000))
        })
        .await
        .expect_err("connect must fail");
        assert!(matches!(err, PauseError::Connect { .. }), "{err:?}");
    }
}
