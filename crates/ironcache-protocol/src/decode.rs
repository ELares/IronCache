// SPDX-License-Identifier: MIT OR Apache-2.0
//! Incremental RESP request decoder (PROTOCOL.md "wire parser").
//!
//! Clients send either a multibulk array of bulk strings (`*<n>\r\n$<len>\r\n
//! <bytes>\r\n...`) or an inline command (a bare line split on whitespace). The
//! decoder is incremental and non-blocking: it reads from a byte slice and
//! returns one of [`DecodeOutcome::Incomplete`] (need more bytes),
//! [`DecodeOutcome::Complete`] (a [`Request`] plus how many bytes it consumed),
//! or [`DecodeOutcome::Error`] (a protocol error, from the catalog). It never
//! blocks and never partially consumes a malformed frame.
//!
//! Hardening caps (#138, PROTOCOL.md) are enforced here via [`Limits`]: the
//! multibulk element count, the per-bulk length (the `proto-max-bulk-len`
//! default), and the inline-line length. The RESP3 attribute marker (`|`) on
//! input is tolerated (parsed and skipped) per PROTOCOL.md.
//!
//! ## Freeze point
//!
//! The [`DecodeOutcome`]/[`Request`] surface and `decode` entry point are a
//! freeze point: the connection read loop and the differential harness depend on
//! these exact return shapes.

use crate::error::ErrorReply;
use bytes::Bytes;

/// Parser hardening limits (#138). Defaults mirror Redis where a default exists.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Max elements in a multibulk request. Redis caps at 1024 * 1024.
    pub max_multibulk: i64,
    /// Max bytes in a single bulk string (`proto-max-bulk-len`, default 512 MB).
    pub max_bulk_len: i64,
    /// Max bytes in an inline command line.
    pub max_inline_len: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_multibulk: 1024 * 1024,
            max_bulk_len: 512 * 1024 * 1024,
            max_inline_len: 64 * 1024,
        }
    }
}

/// A fully-parsed client request: a non-empty list of argument byte strings,
/// where `args[0]` is the command token (matched case-insensitively by dispatch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// The command and its arguments, in order. Always at least one element.
    pub args: Vec<Bytes>,
}

impl Request {
    /// The command token (`args[0]`) as raw bytes.
    #[must_use]
    pub fn command(&self) -> &[u8] {
        &self.args[0]
    }
}

/// The result of one `decode` call.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeOutcome {
    /// A complete request, plus the number of input bytes it consumed. The caller
    /// advances its read buffer by `consumed` and may call `decode` again for the
    /// next pipelined request.
    Complete { request: Request, consumed: usize },
    /// Not enough bytes yet; call again after reading more. Nothing is consumed.
    Incomplete,
    /// A protocol error (from the catalog). The connection should write this and
    /// close, per PROTOCOL.md/hardening.
    Error(ErrorReply),
}

/// What a single frame parse produced relative to a local cursor: either a real
/// request (with the bytes it consumed), a no-op frame that was skipped (consume
/// and continue), not enough bytes yet, or a protocol error.
enum FrameStep {
    /// A dispatchable request consuming `consumed` bytes from the cursor.
    Request { request: Request, consumed: usize },
    /// A no-op frame (empty/null multibulk, blank inline line, attribute) that
    /// consumed `consumed` bytes and carries nothing to dispatch.
    Skip { consumed: usize },
    /// Not enough bytes for this frame yet.
    Incomplete,
    /// A protocol error.
    Error(ErrorReply),
}

/// Decode at most one request from `input` using `limits`.
///
/// `input` is the connection's read buffer (a borrow; nothing is mutated). On
/// [`DecodeOutcome::Complete`] the caller advances by `consumed`. Bulk payloads
/// are copied into owned [`Bytes`]; PR-1 favors simplicity over the eventual
/// zero-copy borrow (PROTOCOL.md notes zero-copy is an optimization behind this
/// same interface).
///
/// No-op frames (empty/null multibulk `*0`/`*-1`, blank inline lines, and
/// tolerated RESP3 attribute frames `|n...`) are skipped ITERATIVELY by advancing
/// a local cursor, not by recursing. This keeps stack usage O(1) regardless of how
/// many leading no-op frames a peer sends, so a flood of `*0\r\n` cannot overflow
/// the stack and abort the shard (hardening, #138).
#[must_use]
pub fn decode(input: &[u8], limits: &Limits) -> DecodeOutcome {
    let mut cursor = 0usize;
    loop {
        let rest = &input[cursor..];
        if rest.is_empty() {
            return DecodeOutcome::Incomplete;
        }
        let step = match rest[0] {
            b'*' => decode_multibulk(rest, limits),
            // RESP3 attribute frames on input are tolerated: parse and skip.
            b'|' => decode_and_skip_attribute(rest, limits),
            // Anything else is an inline command line.
            _ => decode_inline(rest, limits),
        };
        match step {
            FrameStep::Request { request, consumed } => {
                return DecodeOutcome::Complete {
                    request,
                    consumed: cursor + consumed,
                };
            }
            FrameStep::Skip { consumed } => {
                // Advance past the no-op frame and parse the next one. The cursor
                // strictly advances (skipped frames consume at least their CRLF),
                // so this loop terminates.
                debug_assert!(consumed > 0, "a skipped frame must consume bytes");
                cursor += consumed;
            }
            FrameStep::Incomplete => return DecodeOutcome::Incomplete,
            FrameStep::Error(e) => return DecodeOutcome::Error(e),
        }
    }
}

/// Find a `\r\n` starting at `from`, returning the index of the `\r`.
fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut start = from;
    while let Some(rel) = memchr::memchr(b'\r', &buf[start..]) {
        let idx = start + rel;
        if idx + 1 < buf.len() {
            if buf[idx + 1] == b'\n' {
                return Some(idx);
            }
            start = idx + 1;
        } else {
            // '\r' is the last byte: need one more byte to confirm.
            return None;
        }
    }
    None
}

/// Parse a base-10 signed integer from a header line body (between the prefix and
/// CRLF). Returns `None` on any non-digit (Redis rejects these as protocol
/// errors).
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, digits) = match bytes[0] {
        b'-' => (true, &bytes[1..]),
        b'+' => (false, &bytes[1..]),
        _ => (false, bytes),
    };
    if digits.is_empty() {
        return None;
    }
    let mut acc: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(i64::from(b - b'0'))?;
    }
    Some(if neg { -acc } else { acc })
}

fn decode_multibulk(input: &[u8], limits: &Limits) -> FrameStep {
    // Header: *<count>\r\n
    let Some(crlf) = find_crlf(input, 1) else {
        return FrameStep::Incomplete;
    };
    let Some(count) = parse_i64(&input[1..crlf]) else {
        return FrameStep::Error(ErrorReply::protocol("invalid multibulk length"));
    };
    if count > limits.max_multibulk {
        return FrameStep::Error(ErrorReply::protocol("invalid multibulk length"));
    }
    // *0 or *-1: an empty/null multibulk. Redis treats these as a no-op request
    // (it reads them and waits for the next). An empty arg list is invalid for
    // dispatch, so we report a Skip and the caller advances the cursor past it and
    // parses the next frame (iteratively, never recursing).
    if count <= 0 {
        return FrameStep::Skip { consumed: crlf + 2 };
    }

    let mut pos = crlf + 2;
    let mut args: Vec<Bytes> = Vec::with_capacity(count.min(64) as usize);
    for _ in 0..count {
        if pos >= input.len() {
            return FrameStep::Incomplete;
        }
        if input[pos] != b'$' {
            return FrameStep::Error(ErrorReply::protocol(&format!(
                "expected '$', got '{}'",
                input[pos] as char
            )));
        }
        let Some(len_crlf) = find_crlf(input, pos + 1) else {
            return FrameStep::Incomplete;
        };
        let Some(blen) = parse_i64(&input[pos + 1..len_crlf]) else {
            return FrameStep::Error(ErrorReply::protocol("invalid bulk length"));
        };
        if blen < 0 || blen > limits.max_bulk_len {
            return FrameStep::Error(ErrorReply::protocol("invalid bulk length"));
        }
        let data_start = len_crlf + 2;
        let blen_usize = blen as usize;
        let data_end = data_start + blen_usize;
        // Need the payload plus its trailing CRLF.
        if data_end + 2 > input.len() {
            return FrameStep::Incomplete;
        }
        if input[data_end] != b'\r' || input[data_end + 1] != b'\n' {
            return FrameStep::Error(ErrorReply::protocol("expected CRLF after bulk payload"));
        }
        args.push(Bytes::copy_from_slice(&input[data_start..data_end]));
        pos = data_end + 2;
    }
    FrameStep::Request {
        request: Request { args },
        consumed: pos,
    }
}

fn decode_inline(input: &[u8], limits: &Limits) -> FrameStep {
    let Some(crlf) = find_crlf_or_lf(input) else {
        // No line terminator yet. Guard the inline-length cap so a peer cannot
        // make us buffer unboundedly waiting for a newline.
        if input.len() > limits.max_inline_len {
            return FrameStep::Error(ErrorReply::protocol("too big inline request"));
        }
        return FrameStep::Incomplete;
    };
    let (line_end, consumed) = crlf;
    if line_end > limits.max_inline_len {
        return FrameStep::Error(ErrorReply::protocol("too big inline request"));
    }
    let line = &input[..line_end];
    match split_inline(line) {
        Ok(args) => {
            if args.is_empty() {
                // A blank line: skip it (the caller advances and parses the next
                // frame). `consumed` includes the terminator, so it is > 0.
                FrameStep::Skip { consumed }
            } else {
                FrameStep::Request {
                    request: Request { args },
                    consumed,
                }
            }
        }
        Err(e) => FrameStep::Error(e),
    }
}

/// Find a line terminator, accepting both `\r\n` and a bare `\n` (redis-cli and
/// netcat ergonomics). Returns `(content_len, consumed)` where `content_len`
/// excludes the terminator and `consumed` includes it.
fn find_crlf_or_lf(input: &[u8]) -> Option<(usize, usize)> {
    let lf = memchr::memchr(b'\n', input)?;
    if lf > 0 && input[lf - 1] == b'\r' {
        Some((lf - 1, lf + 1))
    } else {
        Some((lf, lf + 1))
    }
}

/// Split an inline command line into arguments, honoring single and double
/// quotes. This covers enough of `sdssplitargs` for handshake and netcat probing;
/// full `sdssplitargs` escape semantics (`\xHH` hex, octal, the `\n\r\t\b\a`
/// escapes, single-quote `\'`) are out of PR-1 scope. Unbalanced quotes are a
/// protocol error.
fn split_inline(line: &[u8]) -> Result<Vec<Bytes>, ErrorReply> {
    let mut args: Vec<Bytes> = Vec::new();
    let mut i = 0;
    let n = line.len();
    while i < n {
        // Skip leading whitespace.
        while i < n && (line[i] == b' ' || line[i] == b'\t') {
            i += 1;
        }
        if i >= n {
            break;
        }
        let mut cur: Vec<u8> = Vec::new();
        loop {
            if i >= n {
                break;
            }
            match line[i] {
                b' ' | b'\t' => {
                    i += 1;
                    break;
                }
                b'"' => {
                    i += 1;
                    loop {
                        if i >= n {
                            return Err(ErrorReply::protocol("unbalanced quotes in request"));
                        }
                        match line[i] {
                            b'\\' if i + 1 < n => {
                                // Minimal escape handling: \x.. and common escapes
                                // are out of PR-1 scope; pass the next byte through.
                                cur.push(line[i + 1]);
                                i += 2;
                            }
                            b'"' => {
                                i += 1;
                                // Closing quote must be followed by space or end.
                                if i < n && line[i] != b' ' && line[i] != b'\t' {
                                    return Err(ErrorReply::protocol(
                                        "unbalanced quotes in request",
                                    ));
                                }
                                break;
                            }
                            c => {
                                cur.push(c);
                                i += 1;
                            }
                        }
                    }
                }
                b'\'' => {
                    i += 1;
                    loop {
                        if i >= n {
                            return Err(ErrorReply::protocol("unbalanced quotes in request"));
                        }
                        match line[i] {
                            b'\'' => {
                                i += 1;
                                if i < n && line[i] != b' ' && line[i] != b'\t' {
                                    return Err(ErrorReply::protocol(
                                        "unbalanced quotes in request",
                                    ));
                                }
                                break;
                            }
                            c => {
                                cur.push(c);
                                i += 1;
                            }
                        }
                    }
                }
                c => {
                    cur.push(c);
                    i += 1;
                }
            }
        }
        args.push(Bytes::from(cur));
    }
    Ok(args)
}

/// Parse and skip a single RESP3 attribute frame (`|<n>\r\n` followed by `n`
/// key/value pairs). IronCache tolerates but does not act on attributes in v1
/// (PROTOCOL.md): the frame is reported as a [`FrameStep::Skip`] so the iterative
/// `decode` loop advances past it and parses whatever follows, without recursion.
fn decode_and_skip_attribute(input: &[u8], limits: &Limits) -> FrameStep {
    let Some(crlf) = find_crlf(input, 1) else {
        return FrameStep::Incomplete;
    };
    let Some(pairs) = parse_i64(&input[1..crlf]) else {
        return FrameStep::Error(ErrorReply::protocol("invalid attribute length"));
    };
    if pairs < 0 {
        return FrameStep::Error(ErrorReply::protocol("invalid attribute length"));
    }
    // Bound `pairs` by the multibulk element cap (#138) BEFORE forming the element
    // count `pairs * 2`. Without this, a header like `|7177777777777777774\r\n`
    // (a near-i64::MAX pair count) makes `pairs * 2` overflow i64: a decode panic
    // under overflow checks (which, on the `panic = "abort"` server, is a
    // whole-process crash) and a silent wrap otherwise. A legitimate attribute
    // carries a handful of pairs, so this cap never trips a real client but keeps
    // the multiply and the skip loop finite. Found by the parser fuzz gate (#534).
    if pairs > limits.max_multibulk {
        return FrameStep::Error(ErrorReply::protocol("invalid attribute length"));
    }
    // Skip 2*pairs bulk strings. Attributes are client->server rare; we only need
    // to consume them. Each element here is expected to be a bulk string.
    let mut pos = crlf + 2;
    for _ in 0..(pairs * 2) {
        if pos >= input.len() {
            return FrameStep::Incomplete;
        }
        if input[pos] != b'$' {
            return FrameStep::Error(ErrorReply::protocol("invalid attribute element"));
        }
        let Some(len_crlf) = find_crlf(input, pos + 1) else {
            return FrameStep::Incomplete;
        };
        let Some(blen) = parse_i64(&input[pos + 1..len_crlf]) else {
            return FrameStep::Error(ErrorReply::protocol("invalid bulk length"));
        };
        if blen < 0 || blen > limits.max_bulk_len {
            return FrameStep::Error(ErrorReply::protocol("invalid bulk length"));
        }
        let data_end = len_crlf + 2 + blen as usize;
        if data_end + 2 > input.len() {
            return FrameStep::Incomplete;
        }
        pos = data_end + 2;
    }
    // The header `|n\r\n` is at least 4 bytes, so `pos > 0` and the cursor in
    // `decode` strictly advances.
    FrameStep::Skip { consumed: pos }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete(input: &[u8]) -> (Vec<Vec<u8>>, usize) {
        match decode(input, &Limits::default()) {
            DecodeOutcome::Complete { request, consumed } => {
                (request.args.iter().map(|b| b.to_vec()).collect(), consumed)
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn multibulk_ping() {
        let (args, consumed) = complete(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(consumed, 14);
    }

    #[test]
    fn multibulk_set_with_value() {
        let (args, consumed) = complete(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert_eq!(
            args,
            vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()]
        );
        assert_eq!(consumed, 31);
    }

    #[test]
    fn multibulk_binary_safe_value() {
        // A value containing CRLF must be length-respected, not line-split.
        let (args, _) = complete(b"*2\r\n$3\r\nSET\r\n$4\r\na\r\nb\r\n");
        assert_eq!(args[1], b"a\r\nb".to_vec());
    }

    #[test]
    fn incomplete_header() {
        assert_eq!(
            decode(b"*1\r\n$4\r\nPI", &Limits::default()),
            DecodeOutcome::Incomplete
        );
        assert_eq!(
            decode(b"*1\r\n", &Limits::default()),
            DecodeOutcome::Incomplete
        );
        assert_eq!(decode(b"*", &Limits::default()), DecodeOutcome::Incomplete);
        assert_eq!(decode(b"", &Limits::default()), DecodeOutcome::Incomplete);
    }

    #[test]
    fn incomplete_missing_trailing_crlf() {
        // Payload present but trailing CRLF not yet arrived.
        assert_eq!(
            decode(b"*1\r\n$4\r\nPING", &Limits::default()),
            DecodeOutcome::Incomplete
        );
    }

    #[test]
    fn inline_command() {
        let (args, consumed) = complete(b"PING\r\n");
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(consumed, 6);
    }

    #[test]
    fn inline_bare_lf() {
        let (args, consumed) = complete(b"PING\n");
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(consumed, 5);
    }

    #[test]
    fn inline_multiple_args_and_quotes() {
        let (args, _) = complete(b"SET foo \"hello world\"\r\n");
        assert_eq!(
            args,
            vec![b"SET".to_vec(), b"foo".to_vec(), b"hello world".to_vec()]
        );
    }

    #[test]
    fn inline_single_quotes() {
        let (args, _) = complete(b"SET k 'a b c'\r\n");
        assert_eq!(
            args,
            vec![b"SET".to_vec(), b"k".to_vec(), b"a b c".to_vec()]
        );
    }

    #[test]
    fn inline_unbalanced_quotes_is_error() {
        match decode(b"SET k \"unterminated\r\n", &Limits::default()) {
            DecodeOutcome::Error(e) => {
                assert!(e.message().contains("unbalanced quotes"), "{}", e.line());
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn bad_multibulk_length_is_error() {
        match decode(b"*x\r\n", &Limits::default()) {
            DecodeOutcome::Error(e) => assert!(e.line().contains("invalid multibulk length")),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn expected_dollar_is_error() {
        match decode(b"*1\r\n+oops\r\n", &Limits::default()) {
            DecodeOutcome::Error(e) => assert!(e.line().contains("expected '$'")),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn bulk_len_over_cap_is_error() {
        let limits = Limits {
            max_bulk_len: 4,
            ..Limits::default()
        };
        match decode(b"*1\r\n$5\r\nhello\r\n", &limits) {
            DecodeOutcome::Error(e) => assert!(e.line().contains("invalid bulk length")),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn bulk_len_header_over_cap_rejected_before_payload_buffered() {
        // Reject-before-alloc (#527): a bulk-string length HEADER that declares more than
        // `max_bulk_len` is a hard protocol error decided FROM THE HEADER ALONE. The decoder never
        // waits for (so never buffers / allocates) the claimed payload. Here the header announces
        // 600 MB but NO payload byte follows; against the default 512 MB ceiling the decode must
        // return `Error` IMMEDIATELY, not `Incomplete` (which would signal the caller to keep
        // reading toward a 600 MB buffer). The whole input is ~16 bytes yet the outcome is a
        // protocol error: proof the huge declared size is rejected on the header, never allocated.
        const OVER_CAP: usize = 600 * 1024 * 1024; // > the default 512 MB max_bulk_len
        let header = format!("*1\r\n${OVER_CAP}\r\n");
        assert!(
            header.len() < 32,
            "the header alone is tiny (no payload present)"
        );
        match decode(header.as_bytes(), &Limits::default()) {
            DecodeOutcome::Error(e) => assert!(
                e.line().contains("invalid bulk length"),
                "expected an invalid-bulk-length protocol error, got {}",
                e.line()
            ),
            other => panic!(
                "an oversized bulk-length header must be an immediate Error (reject-before-alloc), \
                 got {other:?}"
            ),
        }
    }

    #[test]
    fn bulk_len_at_cap_with_no_payload_is_incomplete_not_error() {
        // The dual of the reject-before-alloc test: a header whose declared length is WITHIN the cap
        // is legitimate, so with no payload yet the decoder returns `Incomplete` (await the bytes),
        // NOT an error. This confirms the cap rejects only the OVER-limit header, and that an
        // under-limit large value is accepted once its bytes arrive (bounded separately by the total
        // query-buffer cap in the serve loop, #528).
        let limits = Limits {
            max_bulk_len: 1024,
            ..Limits::default()
        };
        assert_eq!(
            decode(b"*1\r\n$1024\r\n", &limits),
            DecodeOutcome::Incomplete
        );
    }

    #[test]
    fn multibulk_count_over_cap_is_error() {
        let limits = Limits {
            max_multibulk: 2,
            ..Limits::default()
        };
        match decode(b"*3\r\n", &limits) {
            DecodeOutcome::Error(e) => assert!(e.line().contains("invalid multibulk length")),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn pipelined_requests_consume_one_at_a_time() {
        let buf = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        let (args1, consumed1) = complete(buf);
        assert_eq!(args1, vec![b"PING".to_vec()]);
        let (args2, _) = complete(&buf[consumed1..]);
        assert_eq!(args2, vec![b"PING".to_vec()]);
    }

    #[test]
    fn empty_multibulk_skipped_then_next_parsed() {
        // *0 is a no-op; the following PING must be returned.
        let (args, _) = complete(b"*0\r\n*1\r\n$4\r\nPING\r\n");
        assert_eq!(args, vec![b"PING".to_vec()]);
    }

    #[test]
    fn attribute_frame_tolerated_and_skipped() {
        // |1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n  then a real PING.
        let buf = b"|1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*1\r\n$4\r\nPING\r\n";
        let (args, _) = complete(buf);
        assert_eq!(args, vec![b"PING".to_vec()]);
    }

    #[test]
    fn huge_attribute_count_is_error_not_overflow_panic() {
        // Regression (#534, parser fuzz gate): an attribute header whose pair count is
        // near i64::MAX overflowed the `pairs * 2` element-count multiply, PANICKING the
        // decoder under overflow checks (a whole-process crash on the panic=abort
        // server). It must now be a bounded protocol error, never a panic. This exact
        // input is the crash the libFuzzer target `fuzz/fuzz_targets/decode.rs` found.
        match decode(b"|7177777777777777774\r\n$", &Limits::default()) {
            DecodeOutcome::Error(e) => assert!(e.line().contains("invalid attribute length")),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    // -- Regression: no-op-frame skipping is iterative (O(1) stack), not
    // recursive. A flood of leading no-op frames must NOT overflow the stack and
    // abort the process (uncatchable SIGABRT), which would kill the whole shard.
    // Pre-fix, `decode` recursed once per skipped frame.

    #[test]
    fn many_empty_multibulk_frames_do_not_overflow_stack() {
        // ~50k leading `*0\r\n` frames (each a 4-byte no-op), then a real PING.
        const N: usize = 50_000;
        let mut buf = Vec::with_capacity(N * 4 + 16);
        for _ in 0..N {
            buf.extend_from_slice(b"*0\r\n");
        }
        let ping = b"*1\r\n$4\r\nPING\r\n";
        buf.extend_from_slice(ping);
        let (args, consumed) = complete(&buf);
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(consumed, N * 4 + ping.len());
    }

    #[test]
    fn many_null_multibulk_frames_do_not_overflow_stack() {
        // `*-1\r\n` is also a no-op multibulk (5 bytes each).
        const N: usize = 50_000;
        let mut buf = Vec::with_capacity(N * 5 + 16);
        for _ in 0..N {
            buf.extend_from_slice(b"*-1\r\n");
        }
        let ping = b"*1\r\n$4\r\nPING\r\n";
        buf.extend_from_slice(ping);
        let (args, consumed) = complete(&buf);
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(consumed, N * 5 + ping.len());
    }

    #[test]
    fn many_blank_inline_lines_do_not_overflow_stack() {
        // ~50k blank `\r\n` lines (2 bytes each), then an inline PING.
        const N: usize = 50_000;
        let mut buf = Vec::with_capacity(N * 2 + 8);
        for _ in 0..N {
            buf.extend_from_slice(b"\r\n");
        }
        let ping = b"PING\r\n";
        buf.extend_from_slice(ping);
        let (args, consumed) = complete(&buf);
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(consumed, N * 2 + ping.len());
    }

    #[test]
    fn many_attribute_frames_do_not_overflow_stack() {
        // ~50k attribute frames `|1\r\n$1\r\na\r\n$1\r\nb\r\n` then a PING.
        const N: usize = 50_000;
        let attr = b"|1\r\n$1\r\na\r\n$1\r\nb\r\n";
        let ping = b"*1\r\n$4\r\nPING\r\n";
        let mut buf = Vec::with_capacity(N * attr.len() + ping.len());
        for _ in 0..N {
            buf.extend_from_slice(attr);
        }
        buf.extend_from_slice(ping);
        let (args, consumed) = complete(&buf);
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(consumed, N * attr.len() + ping.len());
    }

    #[test]
    fn all_noop_frames_with_no_real_frame_is_incomplete() {
        // A buffer of only no-op frames yields Incomplete (waiting for a real one),
        // and still must not recurse/overflow.
        let mut buf = Vec::new();
        for _ in 0..10_000 {
            buf.extend_from_slice(b"*0\r\n");
        }
        assert_eq!(decode(&buf, &Limits::default()), DecodeOutcome::Incomplete);
    }
}
