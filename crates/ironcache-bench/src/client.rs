// SPDX-License-Identifier: MIT OR Apache-2.0
//! A minimal async RESP client over a `tokio::net::TcpStream`.
//!
//! The load generator only ever issues `GET key` and `SET key value`, so this
//! client is deliberately small: it encodes a command as a RESP array of bulk
//! strings (`*N\r\n$len\r\n<arg>\r\n...`) and reads exactly ONE reply, parsing just
//! enough RESP to consume a complete frame of any of the five leading bytes:
//!
//! - `+` simple string and `-` error: a single CRLF-terminated line.
//! - `:` integer: a single CRLF-terminated line.
//! - `$` bulk string: a length line, then (if non-negative) that many body bytes
//!   plus the trailing CRLF; `$-1\r\n` is the null bulk (a GET miss).
//! - `*` array: a count line, then that many nested replies (parsed recursively so
//!   a flat array reply is consumed safely; the load workload never asks for one,
//!   but consuming it keeps the connection in sync if a server sends one).
//!
//! This is NOT a full RESP3 client; it does not interpret push frames or maps. It
//! exists to complete one request/reply round-trip per call and keep a per-connection
//! read buffer so partial frames are carried across reads.
//!
//! ## Determinism (ADR-0003)
//!
//! No clock, no RNG here. The client is pure I/O; latency timing is the caller's
//! job (through `ironcache_env`).

#![forbid(unsafe_code)]

use std::io::{Error, ErrorKind, Result};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A single RESP connection: an owned TCP stream plus a carry-over read buffer.
pub struct Conn {
    stream: TcpStream,
    /// Bytes read from the socket but not yet consumed by a completed reply.
    buf: Vec<u8>,
    /// The parse cursor into `buf` (bytes before it are consumed).
    pos: usize,
}

/// The decoded shape of one RESP reply. The load generator only needs to know that
/// a reply completed; the variants carry just enough to be unit-testable and to
/// distinguish a GET hit from a miss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// `+OK` and friends (the simple-string body, without the leading `+`).
    Simple(Vec<u8>),
    /// `-ERR ...` (the error body, without the leading `-`).
    Error(Vec<u8>),
    /// `:<n>` integer.
    Integer(i64),
    /// `$<len>\r\n<body>\r\n`. `None` is the null bulk (`$-1`), e.g. a GET miss.
    Bulk(Option<Vec<u8>>),
    /// `*<count>` array of nested replies (consumed for safety; not used by the workload).
    Array(Vec<Reply>),
}

impl Conn {
    /// Connect to `host:port` and disable Nagle (so a request is not delayed waiting
    /// to coalesce; benchmarks want each request on the wire immediately).
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        Ok(Conn {
            stream,
            buf: Vec::with_capacity(4096),
            pos: 0,
        })
    }

    /// Encode `args` as a RESP array of bulk strings and write it to the socket.
    /// e.g. `["GET", "k:1"]` => `*2\r\n$3\r\nGET\r\n$3\r\nk:1\r\n`.
    pub async fn send_command(&mut self, args: &[&[u8]]) -> Result<()> {
        let mut out = Vec::with_capacity(16 + args.iter().map(|a| a.len() + 16).sum::<usize>());
        encode_command_into(&mut out, args);
        self.stream.write_all(&out).await?;
        Ok(())
    }

    /// Pipeline `commands` back-to-back: encode ALL of them into ONE buffer, issue a
    /// SINGLE `write_all` (one syscall on the wire, so the server reads and can batch
    /// them), then read exactly `commands.len()` replies in order. This is classic RESP
    /// pipelining: it amortizes the per-op round-trip/syscall over the whole batch, which
    /// is what lets the throughput pass measure batching wins instead of being syscall-
    /// bound at one op per round-trip.
    ///
    /// The read buffer already carries any trailing pipelined bytes across replies (see
    /// [`Conn::read_reply`]), so `commands.len()` sequential `read_reply` calls drain the
    /// batch's responses correctly even when several arrive in one socket read.
    ///
    /// Returns the `commands.len()` replies in send order. An empty `commands` slice is a
    /// no-op that writes nothing and returns no replies.
    pub async fn pipeline(&mut self, commands: &[&[&[u8]]]) -> Result<Vec<Reply>> {
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        // Build the whole batch into one buffer so it goes out in a single write.
        let cap: usize = commands
            .iter()
            .map(|args| 16 + args.iter().map(|a| a.len() + 16).sum::<usize>())
            .sum();
        let mut out = Vec::with_capacity(cap);
        for args in commands {
            encode_command_into(&mut out, args);
        }
        self.stream.write_all(&out).await?;
        // One reply per command, in order. read_reply keeps the carry-over buffer in sync.
        let mut replies = Vec::with_capacity(commands.len());
        for _ in 0..commands.len() {
            replies.push(self.read_reply().await?);
        }
        Ok(replies)
    }

    /// Issue `GET key` and read one reply.
    pub async fn get(&mut self, key: &[u8]) -> Result<Reply> {
        self.send_command(&[b"GET", key]).await?;
        self.read_reply().await
    }

    /// Issue `SET key value` and read one reply.
    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Reply> {
        self.send_command(&[b"SET", key, value]).await?;
        self.read_reply().await
    }

    /// Read exactly one complete RESP reply, filling the buffer from the socket as
    /// needed. Consumed bytes are dropped from the front of the buffer afterward so
    /// it does not grow without bound across a long-lived connection.
    pub async fn read_reply(&mut self) -> Result<Reply> {
        loop {
            // Try to parse a complete reply from what we already have.
            let start = self.pos;
            match parse_reply(&self.buf, &mut self.pos) {
                ParseOutcome::Done(reply) => {
                    // Drop consumed bytes; keep any trailing pipelined bytes.
                    self.buf.drain(..self.pos);
                    self.pos = 0;
                    return Ok(reply);
                }
                ParseOutcome::NeedMore => {
                    // Rewind the cursor; we will re-parse from `start` after reading.
                    self.pos = start;
                    self.fill().await?;
                }
                ParseOutcome::Protocol(msg) => {
                    return Err(Error::new(ErrorKind::InvalidData, msg));
                }
            }
        }
    }

    /// Read more bytes from the socket into the buffer. Errors on a clean EOF mid-reply.
    async fn fill(&mut self) -> Result<()> {
        let mut chunk = [0u8; 4096];
        let n = self.stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "connection closed mid-reply",
            ));
        }
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(())
    }
}

/// Encode `args` as a RESP array of bulk strings, APPENDING to `out` (it is not
/// cleared, so several commands can be encoded back-to-back into one buffer for a
/// pipelined write). e.g. `["GET", "k:1"]` => `*2\r\n$3\r\nGET\r\n$3\r\nk:1\r\n`.
pub(crate) fn encode_command_into(out: &mut Vec<u8>, args: &[&[u8]]) {
    out.extend_from_slice(b"*");
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for a in args {
        out.extend_from_slice(b"$");
        out.extend_from_slice(a.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

/// The result of attempting to parse one reply out of a buffer slice.
enum ParseOutcome {
    /// A complete reply was parsed; `*pos` was advanced past it.
    Done(Reply),
    /// Not enough bytes yet; the caller must read more and retry from the same start.
    NeedMore,
    /// A malformed frame (bad leading byte or unparseable length).
    Protocol(&'static str),
}

/// Find the next CRLF at or after `pos`, returning the index of the `\r`. `None` if
/// no complete line is present yet.
fn find_crlf(buf: &[u8], pos: usize) -> Option<usize> {
    let mut i = pos;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Parse one RESP reply starting at `*pos`, advancing `*pos` past it on success.
/// Recursive for the array case; the workload never triggers that branch, but it
/// keeps the parser total so an unexpected array reply does not desync the stream.
fn parse_reply(buf: &[u8], pos: &mut usize) -> ParseOutcome {
    if *pos >= buf.len() {
        return ParseOutcome::NeedMore;
    }
    let tag = buf[*pos];
    match tag {
        b'+' | b'-' | b':' => {
            let Some(cr) = find_crlf(buf, *pos + 1) else {
                return ParseOutcome::NeedMore;
            };
            let line = buf[*pos + 1..cr].to_vec();
            *pos = cr + 2;
            match tag {
                b'+' => ParseOutcome::Done(Reply::Simple(line)),
                b'-' => ParseOutcome::Done(Reply::Error(line)),
                _ => match std::str::from_utf8(&line).ok().and_then(|s| s.parse().ok()) {
                    Some(n) => ParseOutcome::Done(Reply::Integer(n)),
                    None => ParseOutcome::Protocol("invalid integer reply"),
                },
            }
        }
        b'$' => {
            let Some(cr) = find_crlf(buf, *pos + 1) else {
                return ParseOutcome::NeedMore;
            };
            let len: i64 = match std::str::from_utf8(&buf[*pos + 1..cr])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(n) => n,
                None => return ParseOutcome::Protocol("invalid bulk length"),
            };
            let body_start = cr + 2;
            if len < 0 {
                // Null bulk: `$-1\r\n` (a GET miss). No body follows.
                *pos = body_start;
                return ParseOutcome::Done(Reply::Bulk(None));
            }
            let len = len as usize;
            let body_end = body_start + len;
            // Need the body plus its trailing CRLF.
            if buf.len() < body_end + 2 {
                return ParseOutcome::NeedMore;
            }
            let body = buf[body_start..body_end].to_vec();
            *pos = body_end + 2;
            ParseOutcome::Done(Reply::Bulk(Some(body)))
        }
        b'*' => {
            let Some(cr) = find_crlf(buf, *pos + 1) else {
                return ParseOutcome::NeedMore;
            };
            let count: i64 = match std::str::from_utf8(&buf[*pos + 1..cr])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(n) => n,
                None => return ParseOutcome::Protocol("invalid array count"),
            };
            *pos = cr + 2;
            if count < 0 {
                // Null array; treat like an empty array reply.
                return ParseOutcome::Done(Reply::Array(Vec::new()));
            }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                match parse_reply(buf, pos) {
                    ParseOutcome::Done(r) => items.push(r),
                    ParseOutcome::NeedMore => return ParseOutcome::NeedMore,
                    err @ ParseOutcome::Protocol(_) => return err,
                }
            }
            ParseOutcome::Done(Reply::Array(items))
        }
        _ => ParseOutcome::Protocol("unknown RESP leading byte"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse one reply out of a full byte slice, asserting it completes and consumes
    /// exactly the expected number of bytes.
    fn parse_one(bytes: &[u8]) -> (Reply, usize) {
        let mut pos = 0;
        match parse_reply(bytes, &mut pos) {
            ParseOutcome::Done(r) => (r, pos),
            ParseOutcome::NeedMore => panic!("expected a complete reply, got NeedMore"),
            ParseOutcome::Protocol(m) => panic!("protocol error: {m}"),
        }
    }

    #[test]
    fn parses_ok_simple_string() {
        let (r, n) = parse_one(b"+OK\r\n");
        assert_eq!(r, Reply::Simple(b"OK".to_vec()));
        assert_eq!(n, 5);
    }

    #[test]
    fn parses_null_bulk_as_miss() {
        let (r, n) = parse_one(b"$-1\r\n");
        assert_eq!(r, Reply::Bulk(None));
        assert_eq!(n, 5);
    }

    #[test]
    fn parses_bulk_string_body() {
        let (r, n) = parse_one(b"$5\r\nhello\r\n");
        assert_eq!(r, Reply::Bulk(Some(b"hello".to_vec())));
        assert_eq!(n, 11);
    }

    #[test]
    fn parses_integer() {
        let (r, n) = parse_one(b":1\r\n");
        assert_eq!(r, Reply::Integer(1));
        assert_eq!(n, 4);
    }

    #[test]
    fn parses_error_line() {
        let (r, n) = parse_one(b"-ERR bad thing\r\n");
        assert_eq!(r, Reply::Error(b"ERR bad thing".to_vec()));
        assert_eq!(n, 16);
    }

    #[test]
    fn parses_flat_array() {
        // *2 of two bulks; consumed fully.
        let (r, n) = parse_one(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert_eq!(
            r,
            Reply::Array(vec![
                Reply::Bulk(Some(b"foo".to_vec())),
                Reply::Bulk(Some(b"bar".to_vec())),
            ])
        );
        // "*2\r\n" (4) + "$3\r\nfoo\r\n" (9) + "$3\r\nbar\r\n" (9) = 22 bytes.
        assert_eq!(n, 22);
    }

    #[test]
    fn partial_frame_needs_more() {
        // A bulk header with the body not yet arrived.
        let mut pos = 0;
        assert!(matches!(
            parse_reply(b"$5\r\nhel", &mut pos),
            ParseOutcome::NeedMore
        ));
        // A bare leading byte with no CRLF yet.
        let mut pos = 0;
        assert!(matches!(
            parse_reply(b"+OK", &mut pos),
            ParseOutcome::NeedMore
        ));
    }

    #[test]
    fn rejects_unknown_leading_byte() {
        let mut pos = 0;
        assert!(matches!(
            parse_reply(b"?bogus\r\n", &mut pos),
            ParseOutcome::Protocol(_)
        ));
    }

    #[test]
    fn encode_command_appends_one_resp_frame() {
        // The single-command encoder is exactly the wire shape send_command uses.
        let mut out = Vec::new();
        encode_command_into(&mut out, &[b"GET", b"k:1"]);
        assert_eq!(out, b"*2\r\n$3\r\nGET\r\n$3\r\nk:1\r\n");
    }

    #[test]
    fn encode_command_pipelines_n_frames_into_one_buffer() {
        // The pipeline path encodes N commands into ONE buffer (one write). Prove the
        // buffer is exactly the N frames concatenated, in order, with no separators.
        let mut out = Vec::new();
        encode_command_into(&mut out, &[b"SET", b"k:1", b"vv"]);
        encode_command_into(&mut out, &[b"GET", b"k:2"]);
        encode_command_into(&mut out, &[b"GET", b"k:3"]);
        let expected: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nk:1\r\n$2\r\nvv\r\n\
            *2\r\n$3\r\nGET\r\n$3\r\nk:2\r\n\
            *2\r\n$3\r\nGET\r\n$3\r\nk:3\r\n";
        assert_eq!(out, expected);
        // The buffer parses as exactly three complete, well-formed commands back-to-back:
        // walk it with the request framer used by the test stub's counterpart.
        let mut pos = 0usize;
        let mut frames = 0usize;
        while pos < out.len() {
            let start = pos;
            // Each command is a top-level array; parse_reply is a REPLY parser, but the
            // array/bulk framing is identical, so it advances past one full command frame.
            match parse_reply(&out, &mut pos) {
                ParseOutcome::Done(_) => frames += 1,
                other => panic!(
                    "frame {frames} did not parse cleanly at {start}: {:?}",
                    matches!(other, ParseOutcome::NeedMore)
                ),
            }
        }
        assert_eq!(
            frames, 3,
            "buffer must hold exactly three RESP command frames"
        );
    }

    #[tokio::test]
    async fn pipeline_sends_n_commands_and_reads_n_replies() {
        // End-to-end: against the in-test stub, one pipeline of N commands must come back
        // as N replies (a bulk for each GET, +OK for each SET), read from the carry-over
        // buffer that drains several replies out of one socket read.
        let stub = crate::testutil::spawn(None).await;
        let mut conn = Conn::connect("127.0.0.1", stub.port).await.unwrap();
        let batch: [&[&[u8]]; 4] = [
            &[b"GET", b"k:1"],
            &[b"SET", b"k:2", b"vv"],
            &[b"GET", b"k:3"],
            &[b"SET", b"k:4", b"vv"],
        ];
        let replies = conn.pipeline(&batch).await.unwrap();
        assert_eq!(replies.len(), 4, "N commands must yield N replies");
        // Stub canned replies: GET -> bulk "val", SET -> +OK. Order is preserved.
        assert_eq!(replies[0], Reply::Bulk(Some(b"val".to_vec())));
        assert_eq!(replies[1], Reply::Simple(b"OK".to_vec()));
        assert_eq!(replies[2], Reply::Bulk(Some(b"val".to_vec())));
        assert_eq!(replies[3], Reply::Simple(b"OK".to_vec()));
    }

    #[tokio::test]
    async fn pipeline_empty_batch_is_a_noop() {
        let stub = crate::testutil::spawn(None).await;
        let mut conn = Conn::connect("127.0.0.1", stub.port).await.unwrap();
        let empty: [&[&[u8]]; 0] = [];
        let replies = conn.pipeline(&empty).await.unwrap();
        assert!(
            replies.is_empty(),
            "an empty batch writes nothing and returns no replies"
        );
    }
}
