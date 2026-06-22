// SPDX-License-Identifier: MIT OR Apache-2.0
//! SAVE-FIRST: the data-safety step of `ironcache upgrade` (the operator's core concern).
//!
//! Before swapping the binary and restarting, we make the running server's in-memory working set
//! durable so it survives the restart. We do this by connecting to the running server over RESP on
//! loopback and issuing a synchronous, fsync'd `SAVE` (the existing #58 persistence path: each shard
//! dumps its partition + the manifest commits before `+OK`). To KNOW the snapshot is current (not a
//! stale prior one) we read `LASTSAVE` before and after and confirm it ADVANCED (or, when the
//! pre-save value is `0`, that it became non-zero).
//!
//! ## Honest behavior with no persistence
//!
//! If the server has NO `data_dir`, `SAVE` returns an error (persistence disabled) -- there is no
//! snapshot to make current, so the restart WOULD lose the in-memory data. We surface that as
//! [`SaveOutcome::NoPersistence`] (a loud warning the orchestrator gates on `--yes`), NOT a silent
//! success. The lossless write-freeze that removes even this window is the #388 follow-up.
//!
//! ## Credentials
//!
//! An optional `requirepass` is sent via `AUTH` as the FIRST command. The password comes from a file
//! (read by the caller) and is sent only over the loopback socket -- it is never placed in argv or
//! logged. We treat a connect/auth/protocol failure as a hard [`SaveError`] (we will not swap over a
//! server we could not even talk to).
//!
//! ## Determinism seam
//!
//! The connect/IO timeout is driven through the `ironcache-runtime` timer seam (the sanctioned timer
//! boundary, like `persist.rs`), never wall-clock; no `std::time::Instant`/`SystemTime` is read here.

use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// How long the whole SAVE-first exchange (connect + AUTH + LASTSAVE + SAVE + LASTSAVE) is allowed
/// to take before we give up. A real `SAVE` blocks for the dump; this is generous but bounded so a
/// wedged server does not hang the upgrade indefinitely (the operator can re-run). Driven through
/// the runtime timer seam.
const SAVE_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(60);

/// A typed SAVE-first failure (no stringly-typed errors). Distinct from the "no persistence
/// configured" case, which is an [`SaveOutcome`], not an error: a `SaveError` means we could not
/// reason about data safety at all (connect/auth/protocol), which is fatal regardless of `--yes`.
#[derive(Debug, thiserror::Error)]
pub enum SaveError {
    /// Could not connect to the running server on the RESP address.
    #[error("connecting to the running server at {addr}: {detail}")]
    Connect {
        /// The RESP address.
        addr: String,
        /// Why the connect failed.
        detail: String,
    },
    /// An IO error during the exchange (write/read on the socket).
    #[error("RESP IO during SAVE-first: {0}")]
    Io(String),
    /// The server rejected `AUTH` (wrong/missing password).
    #[error("AUTH failed during SAVE-first: {0}")]
    Auth(String),
    /// A reply was not the RESP shape we expected.
    #[error("unexpected RESP reply during SAVE-first ({context}): {detail}")]
    Protocol {
        /// Which step produced it.
        context: String,
        /// The unexpected reply (lossy).
        detail: String,
    },
    /// The whole exchange exceeded [`SAVE_EXCHANGE_TIMEOUT`].
    #[error("SAVE-first timed out after {0:?} (the server did not complete the save in time)")]
    Timeout(Duration),
    /// `SAVE` returned `+OK` but `LASTSAVE` did NOT advance, so we cannot confirm the snapshot is
    /// current (a surprising server state; refuse to swap over an unconfirmed save).
    #[error(
        "SAVE reported success but LASTSAVE did not advance (before={before}, after={after}); \
         cannot confirm the snapshot is current"
    )]
    NotAdvanced {
        /// The LASTSAVE value before the save.
        before: i64,
        /// The LASTSAVE value after the save.
        after: i64,
    },
}

/// The result of the SAVE-first attempt.
#[derive(Debug, Clone)]
pub enum SaveOutcome {
    /// The save was triggered and `LASTSAVE` advanced: the on-disk snapshot is current, so the
    /// restart will not lose data.
    Confirmed {
        /// The post-save `LASTSAVE` unix-seconds.
        last_save: i64,
    },
    /// The server has no persistence configured (`SAVE` reported it disabled): the restart would
    /// lose the in-memory working set. The orchestrator turns this into a loud warning gated on
    /// `--yes` (an honest data-loss acknowledgement; the lossless path is #388).
    NoPersistence {
        /// The server's reason (its `SAVE` error text), for the warning.
        reason: String,
    },
}

/// What the SAVE-first step needs: the RESP `host:port` and an optional loopback password.
#[derive(Debug, Clone)]
pub struct SaveTarget {
    /// The RESP `host:port` of the running server.
    pub resp_addr: String,
    /// An optional `requirepass` (sent only over loopback, never logged).
    pub auth: Option<String>,
}

/// The SAVE-first seam, so the orchestrator's data-safety step is unit-testable with a mock (a real
/// loopback SAVE needs a live server). v1 ships [`LoopbackSaver`]; the #388 lossless write-freeze
/// will introduce a `WriteFreezeSaver` behind the same trait.
pub trait Saver {
    /// Make the running server's in-memory working set durable, returning a [`SaveOutcome`].
    ///
    /// # Errors
    ///
    /// Returns a [`SaveError`] when the save exchange could not be completed (connect/auth/protocol/
    /// timeout) -- a FATAL condition (we never swap over a save we could not reason about).
    fn save_first(&self, target: &SaveTarget) -> Result<SaveOutcome, SaveError>;
}

/// The v1 saver: an explicit loopback RESP `SAVE` + `LASTSAVE` confirmation ([`save_first`]).
pub struct LoopbackSaver;

impl Saver for LoopbackSaver {
    fn save_first(&self, target: &SaveTarget) -> Result<SaveOutcome, SaveError> {
        save_first(&target.resp_addr, target.auth.as_deref())
    }
}

/// Connect to the running server at `resp_addr`, optionally `AUTH` with `password`, then trigger a
/// synchronous `SAVE` and confirm `LASTSAVE` advanced. Returns [`SaveOutcome::Confirmed`] when the
/// snapshot is made current, [`SaveOutcome::NoPersistence`] when persistence is off (the honest
/// data-loss case), or a [`SaveError`] when we could not complete the exchange.
///
/// Runs the async exchange on a throwaway current-thread tokio runtime (this is the short-lived CLI
/// process; no shard executor is involved).
///
/// # Errors
///
/// Returns a [`SaveError`] on connect/auth/protocol/timeout failure.
pub fn save_first(resp_addr: &str, password: Option<&str>) -> Result<SaveOutcome, SaveError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| SaveError::Io(format!("building the SAVE-first runtime: {e}")))?;
    rt.block_on(save_first_async(resp_addr, password))
}

/// The async SAVE-first exchange, wrapped in the bounded [`SAVE_EXCHANGE_TIMEOUT`] via the runtime
/// timer seam.
async fn save_first_async(
    resp_addr: &str,
    password: Option<&str>,
) -> Result<SaveOutcome, SaveError> {
    use ironcache_runtime::Runtime as _;
    let rt = ironcache_runtime::TokioRuntime::new();
    tokio::select! {
        biased;
        result = save_exchange(resp_addr, password) => result,
        () = rt.timer(SAVE_EXCHANGE_TIMEOUT) => Err(SaveError::Timeout(SAVE_EXCHANGE_TIMEOUT)),
    }
}

/// The connect -> AUTH -> LASTSAVE -> SAVE -> LASTSAVE conversation over one TCP stream.
async fn save_exchange(resp_addr: &str, password: Option<&str>) -> Result<SaveOutcome, SaveError> {
    let mut stream = tokio::net::TcpStream::connect(resp_addr)
        .await
        .map_err(|e| SaveError::Connect {
            addr: resp_addr.to_owned(),
            detail: e.to_string(),
        })?;

    // AUTH first (if a password was supplied). The password rides only the loopback socket; it is
    // never logged or placed in argv.
    if let Some(pw) = password {
        write_command(&mut stream, &[b"AUTH", pw.as_bytes()]).await?;
        let reply = read_reply(&mut stream).await?;
        match reply {
            Reply::Simple(s) if s.eq_ignore_ascii_case("OK") => {}
            Reply::Error(e) => return Err(SaveError::Auth(e)),
            other => {
                return Err(SaveError::Protocol {
                    context: "AUTH".to_owned(),
                    detail: other.to_string(),
                });
            }
        }
    }

    // LASTSAVE before, to detect advancement.
    let before = lastsave(&mut stream).await?;

    // SAVE (blocking, fsync'd). A persistence-disabled server replies an error here -- that is the
    // honest no-persistence case, not a hard failure.
    write_command(&mut stream, &[b"SAVE"]).await?;
    match read_reply(&mut stream).await? {
        Reply::Simple(s) if s.eq_ignore_ascii_case("OK") => {}
        Reply::Error(e) => {
            return Ok(SaveOutcome::NoPersistence { reason: e });
        }
        other => {
            return Err(SaveError::Protocol {
                context: "SAVE".to_owned(),
                detail: other.to_string(),
            });
        }
    }

    // LASTSAVE after; confirm it advanced (or moved off 0).
    let after = lastsave(&mut stream).await?;
    if after > before || (before == 0 && after != 0) {
        Ok(SaveOutcome::Confirmed { last_save: after })
    } else {
        Err(SaveError::NotAdvanced { before, after })
    }
}

/// Issue `LASTSAVE` and parse its integer reply (the unix-seconds of the last successful save).
async fn lastsave(stream: &mut tokio::net::TcpStream) -> Result<i64, SaveError> {
    write_command(stream, &[b"LASTSAVE"]).await?;
    match read_reply(stream).await? {
        Reply::Integer(n) => Ok(n),
        Reply::Error(e) => Err(SaveError::Protocol {
            context: "LASTSAVE".to_owned(),
            detail: e,
        }),
        other => Err(SaveError::Protocol {
            context: "LASTSAVE".to_owned(),
            detail: other.to_string(),
        }),
    }
}

/// Encode `args` as a RESP2 command array and write it. Small, self-contained encoder (the codebase
/// already hand-builds RESP in `cmd_cli`); we only need the request side.
async fn write_command(
    stream: &mut tokio::net::TcpStream,
    args: &[&[u8]],
) -> Result<(), SaveError> {
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
        .map_err(|e| SaveError::Io(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| SaveError::Io(e.to_string()))
}

/// The minimal RESP reply shapes SAVE-first needs (simple string, error, integer). Everything else
/// (bulk/array) is unexpected for these commands and is surfaced as a protocol error by the caller.
#[derive(Debug)]
enum Reply {
    /// `+<text>\r\n`
    Simple(String),
    /// `-<text>\r\n`
    Error(String),
    /// `:<int>\r\n`
    Integer(i64),
    /// Any other leading byte (bulk/array/etc.), captured for the error message.
    Other(u8),
}

impl std::fmt::Display for Reply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reply::Simple(s) => write!(f, "+{s}"),
            Reply::Error(e) => write!(f, "-{e}"),
            Reply::Integer(n) => write!(f, ":{n}"),
            // Render the unexpected RESP leading byte (the captured field) so a bulk/array/other
            // reply to a status/integer command is diagnosable.
            Reply::Other(b) => write!(f, "unexpected RESP type byte '{}'", *b as char),
        }
    }
}

/// Read ONE RESP reply of a type SAVE-first expects (`+`, `-`, `:`). Reads byte-by-byte until the
/// terminating CRLF of the first line; these replies are single-line, so we never need to parse a
/// bulk/array body. A connection close mid-reply is an IO error.
async fn read_reply(stream: &mut tokio::net::TcpStream) -> Result<Reply, SaveError> {
    let first = read_one(stream).await?;
    let line = read_line(stream).await?;
    match first {
        b'+' => Ok(Reply::Simple(line)),
        b'-' => Ok(Reply::Error(line)),
        b':' => line
            .trim()
            .parse::<i64>()
            .map(Reply::Integer)
            .map_err(|_| SaveError::Protocol {
                context: "integer reply".to_owned(),
                detail: line,
            }),
        other => Ok(Reply::Other(other)),
    }
}

/// Read one byte, mapping EOF to an IO error (a closed connection mid-reply).
async fn read_one(stream: &mut tokio::net::TcpStream) -> Result<u8, SaveError> {
    let mut b = [0u8; 1];
    let n = stream
        .read(&mut b)
        .await
        .map_err(|e| SaveError::Io(e.to_string()))?;
    if n == 0 {
        return Err(SaveError::Io("connection closed mid-reply".to_owned()));
    }
    Ok(b[0])
}

/// Read up to and including the line's terminating `\r\n`, returning the line WITHOUT the CRLF. A
/// bounded read (a single RESP status/integer line is tiny); a stray lone `\n` is tolerated.
async fn read_line(stream: &mut tokio::net::TcpStream) -> Result<String, SaveError> {
    let mut out = Vec::with_capacity(32);
    loop {
        let b = read_one(stream).await?;
        if b == b'\n' {
            break;
        }
        if b == b'\r' {
            // Expect the following '\n'; consume it.
            let nl = read_one(stream).await?;
            if nl == b'\n' {
                break;
            }
            // A bare '\r' not followed by '\n': keep both bytes and continue (very defensive).
            out.push(b'\r');
            out.push(nl);
            continue;
        }
        out.push(b);
        if out.len() > 4096 {
            return Err(SaveError::Io("reply line exceeded 4 KiB".to_owned()));
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

#[cfg(test)]
mod tests {
    // `AsyncReadExt`/`AsyncWriteExt` come in via `super::*` (the module-level imports); the test
    // server uses `read_exact`/`write_all` through them.
    use super::*;
    use tokio::net::TcpListener;

    /// A scripted fake server: reads each command (an array of bulk strings) and replies with the
    /// next scripted raw reply. Used to drive the SAVE-first exchange without a real engine.
    async fn fake_server(listener: TcpListener, replies: Vec<&'static [u8]>) {
        let (mut sock, _) = listener.accept().await.unwrap();
        for reply in replies {
            // Read one full command array: `*N\r\n` then N bulk strings. We just drain until we've
            // consumed one command, tracking the bulk count, which is enough for these fixed scripts.
            read_one_command(&mut sock).await;
            sock.write_all(reply).await.unwrap();
            sock.flush().await.unwrap();
        }
        // Keep the socket open briefly so the client reads the last reply.
        let _ = sock.shutdown().await;
    }

    /// Drain exactly one RESP command array from the socket (enough to know when to send the next
    /// reply). Parses `*N` then N `$len\r\n<bytes>\r\n` items.
    async fn read_one_command(sock: &mut tokio::net::TcpStream) {
        let n = read_count(sock, b'*').await;
        for _ in 0..n {
            let len = read_count(sock, b'$').await;
            let mut body = vec![0u8; len as usize + 2]; // + CRLF
            sock.read_exact(&mut body).await.unwrap();
        }
    }

    /// Read a `<prefix>N\r\n` header and return N.
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
    async fn confirmed_when_lastsave_advances() {
        // LASTSAVE(before)=100, SAVE=+OK, LASTSAVE(after)=200 -> Confirmed.
        let addr = spawn_fake(vec![b":100\r\n", b"+OK\r\n", b":200\r\n"]).await;
        let out = save_exchange(&addr, None).await.expect("exchange ok");
        match out {
            SaveOutcome::Confirmed { last_save } => assert_eq!(last_save, 200),
            SaveOutcome::NoPersistence { .. } => panic!("expected Confirmed, got NoPersistence"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_persistence_when_save_errors() {
        // LASTSAVE=0, then SAVE replies an error (persistence disabled).
        let addr = spawn_fake(vec![
            b":0\r\n",
            b"-ERR persistence is disabled (no data_dir)\r\n",
        ])
        .await;
        let out = save_exchange(&addr, None).await.expect("exchange ok");
        match out {
            SaveOutcome::NoPersistence { reason } => {
                assert!(reason.contains("persistence"), "{reason}");
            }
            SaveOutcome::Confirmed { .. } => panic!("expected NoPersistence, got Confirmed"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn not_advanced_is_an_error() {
        // before=200, +OK, after=200 (did not advance) -> NotAdvanced.
        let addr = spawn_fake(vec![b":200\r\n", b"+OK\r\n", b":200\r\n"]).await;
        let err = save_exchange(&addr, None).await.expect_err("must error");
        assert!(
            matches!(
                err,
                SaveError::NotAdvanced {
                    before: 200,
                    after: 200
                }
            ),
            "{err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auth_is_sent_and_failure_is_typed() {
        // AUTH replies an error -> SaveError::Auth.
        let addr = spawn_fake(vec![b"-WRONGPASS invalid password\r\n"]).await;
        let err = save_exchange(&addr, Some("bad"))
            .await
            .expect_err("auth failure");
        assert!(matches!(err, SaveError::Auth(_)), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auth_ok_then_confirmed() {
        // AUTH +OK, LASTSAVE 0, SAVE +OK, LASTSAVE 50 -> Confirmed (off-zero counts as advanced).
        let addr = spawn_fake(vec![b"+OK\r\n", b":0\r\n", b"+OK\r\n", b":50\r\n"]).await;
        let out = save_exchange(&addr, Some("s3cr3t")).await.expect("ok");
        assert!(
            matches!(out, SaveOutcome::Confirmed { last_save: 50 }),
            "{out:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connect_failure_is_typed() {
        // Nothing listening on this port.
        let err = save_exchange("127.0.0.1:1", None)
            .await
            .expect_err("connect must fail");
        assert!(matches!(err, SaveError::Connect { .. }), "{err:?}");
    }
}
