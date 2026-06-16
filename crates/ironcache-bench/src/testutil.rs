// SPDX-License-Identifier: MIT OR Apache-2.0
//! In-test RESP stub server, shared by the closed-loop and open-loop tests.
//!
//! The stub binds `127.0.0.1:0` (an OS-assigned port), accepts any number of
//! connections, and for each inbound command replies a canned RESP frame: `+OK\r\n`
//! for a write (`SET`/anything starting with `S`/`s`) and a fixed bulk string for a
//! read (`GET`). An optional fixed `delay` is applied before each reply, which is
//! how the open-loop coordinated-omission guard injects latency.
//!
//! The stub does NOT fully parse RESP requests; it only needs to know how many
//! complete request frames have arrived so it can emit one reply per request. It
//! does this by counting top-level `*` array headers and consuming the exact bytes
//! of each request frame (`*N\r\n` then N bulk strings), which is the only shape the
//! load-generator client ever sends. This keeps the stub trivial and dependency-free
//! while staying in sync on a pipelined or coalesced read.
//!
//! ## Determinism
//!
//! The stub reads no clock and no RNG; the only time it touches is `tokio::time`
//! for the optional reply delay, which the invariant lint permits.

#![forbid(unsafe_code)]

use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The canned read reply: a 3-byte bulk `"val"`.
const READ_REPLY: &[u8] = b"$3\r\nval\r\n";
/// The canned write reply: `+OK`.
const WRITE_REPLY: &[u8] = b"+OK\r\n";

/// A handle to a running stub server: its bound port and a count of replies sent.
pub struct StubServer {
    /// The OS-assigned port the stub listens on.
    pub port: u16,
    /// Total replies the stub has sent across all connections.
    pub replies: Arc<AtomicU64>,
}

/// Spawn a stub server on `127.0.0.1:0`, replying to every request with `+OK` (for
/// a write) or a fixed bulk (for a read), optionally after `delay`. Returns once the
/// listener is bound (so the port is valid immediately); the accept loop runs in a
/// background task that lives for the test process.
pub async fn spawn(delay: Option<Duration>) -> StubServer {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let replies = Arc::new(AtomicU64::new(0));
    let replies_for_loop = Arc::clone(&replies);

    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            let replies = Arc::clone(&replies_for_loop);
            tokio::spawn(async move {
                handle_conn(sock, delay, replies).await;
            });
        }
    });

    StubServer { port, replies }
}

/// Serve one connection: read bytes, and for each complete top-level request frame
/// emit one reply (after the optional delay).
async fn handle_conn(mut sock: TcpStream, delay: Option<Duration>, replies: Arc<AtomicU64>) {
    let _ = sock.set_nodelay(true);
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];

    loop {
        // Consume as many complete request frames as the buffer holds.
        while let Some((is_read, consumed)) = take_request(&buf) {
            let reply = if is_read { READ_REPLY } else { WRITE_REPLY };
            if let Some(d) = delay {
                tokio::time::sleep(d).await;
            }
            if sock.write_all(reply).await.is_err() {
                return;
            }
            replies.fetch_add(1, Ordering::Relaxed);
            buf.drain(..consumed);
        }
        match sock.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

/// If `buf` begins with one complete RESP request array (`*N\r\n` then N bulk
/// strings), return `(is_read, bytes_consumed)`. `is_read` is true iff the first
/// argument's first byte is `G`/`g` (a GET). Returns `None` if the frame is incomplete.
fn take_request(buf: &[u8]) -> Option<(bool, usize)> {
    if buf.first() != Some(&b'*') {
        return None;
    }
    let mut pos = 0usize;
    let nargs = read_count(buf, &mut pos, b'*')?;
    let mut first_arg_lead: Option<u8> = None;
    for i in 0..nargs {
        let len = read_count(buf, &mut pos, b'$')?;
        if buf.len() < pos + len + 2 {
            return None;
        }
        if i == 0 {
            first_arg_lead = buf.get(pos).copied();
        }
        pos += len + 2; // body + CRLF
    }
    let is_read = matches!(first_arg_lead, Some(b'G' | b'g'));
    Some((is_read, pos))
}

/// Read a `<tag><number>\r\n` count line at `*pos`, advancing past it. Returns the
/// number, or `None` if the line is incomplete or malformed.
fn read_count(buf: &[u8], pos: &mut usize, tag: u8) -> Option<usize> {
    if buf.get(*pos) != Some(&tag) {
        return None;
    }
    let mut i = *pos + 1;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            let n: usize = std::str::from_utf8(&buf[*pos + 1..i]).ok()?.parse().ok()?;
            *pos = i + 2;
            return Some(n);
        }
        i += 1;
    }
    None
}
