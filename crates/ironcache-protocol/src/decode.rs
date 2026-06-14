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

/// Decode at most one request from `input` using `limits`.
///
/// `input` is the connection's read buffer (a borrow; nothing is mutated). On
/// [`DecodeOutcome::Complete`] the caller advances by `consumed`. Bulk payloads
/// are copied into owned [`Bytes`]; PR-1 favors simplicity over the eventual
/// zero-copy borrow (PROTOCOL.md notes zero-copy is an optimization behind this
/// same interface).
#[must_use]
pub fn decode(input: &[u8], limits: &Limits) -> DecodeOutcome {
    if input.is_empty() {
        return DecodeOutcome::Incomplete;
    }
    match input[0] {
        b'*' => decode_multibulk(input, limits),
        // RESP3 attribute frames on input are tolerated: parse and skip, then
        // decode whatever follows (PROTOCOL.md).
        b'|' => decode_and_skip_attribute(input, limits),
        // Anything else is an inline command line.
        _ => decode_inline(input, limits),
    }
}

/// Add `prefix` consumed bytes to a sub-decode outcome. Used where a leading
/// no-op frame (empty multibulk, blank inline line, attribute) was skipped and we
/// recursed to decode what follows: the outer consumed count must include the
/// skipped prefix.
fn with_consumed_prefix(prefix: usize, outcome: DecodeOutcome) -> DecodeOutcome {
    match outcome {
        DecodeOutcome::Complete { request, consumed } => DecodeOutcome::Complete {
            request,
            consumed: prefix + consumed,
        },
        DecodeOutcome::Incomplete => DecodeOutcome::Incomplete,
        DecodeOutcome::Error(e) => DecodeOutcome::Error(e),
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

fn decode_multibulk(input: &[u8], limits: &Limits) -> DecodeOutcome {
    // Header: *<count>\r\n
    let Some(crlf) = find_crlf(input, 1) else {
        return DecodeOutcome::Incomplete;
    };
    let Some(count) = parse_i64(&input[1..crlf]) else {
        return DecodeOutcome::Error(ErrorReply::protocol("invalid multibulk length"));
    };
    if count > limits.max_multibulk {
        return DecodeOutcome::Error(ErrorReply::protocol("invalid multibulk length"));
    }
    // *0 or *-1: an empty/null multibulk. Redis treats these as a no-op request
    // (it reads them and waits for the next). We surface an empty request so the
    // caller can skip; but an empty arg list is invalid for dispatch, so we model
    // it as "consume and continue" by returning a Complete with a single empty
    // sentinel would be wrong. Instead we recurse past it.
    if count <= 0 {
        let consumed = crlf + 2;
        // Nothing meaningful to dispatch; skip and try to parse the next frame.
        return with_consumed_prefix(consumed, decode(&input[consumed..], limits));
    }

    let mut pos = crlf + 2;
    let mut args: Vec<Bytes> = Vec::with_capacity(count.min(64) as usize);
    for _ in 0..count {
        if pos >= input.len() {
            return DecodeOutcome::Incomplete;
        }
        if input[pos] != b'$' {
            return DecodeOutcome::Error(ErrorReply::protocol(&format!(
                "expected '$', got '{}'",
                input[pos] as char
            )));
        }
        let Some(len_crlf) = find_crlf(input, pos + 1) else {
            return DecodeOutcome::Incomplete;
        };
        let Some(blen) = parse_i64(&input[pos + 1..len_crlf]) else {
            return DecodeOutcome::Error(ErrorReply::protocol("invalid bulk length"));
        };
        if blen < 0 || blen > limits.max_bulk_len {
            return DecodeOutcome::Error(ErrorReply::protocol("invalid bulk length"));
        }
        let data_start = len_crlf + 2;
        let blen_usize = blen as usize;
        let data_end = data_start + blen_usize;
        // Need the payload plus its trailing CRLF.
        if data_end + 2 > input.len() {
            return DecodeOutcome::Incomplete;
        }
        if input[data_end] != b'\r' || input[data_end + 1] != b'\n' {
            return DecodeOutcome::Error(ErrorReply::protocol("expected CRLF after bulk payload"));
        }
        args.push(Bytes::copy_from_slice(&input[data_start..data_end]));
        pos = data_end + 2;
    }
    DecodeOutcome::Complete {
        request: Request { args },
        consumed: pos,
    }
}

fn decode_inline(input: &[u8], limits: &Limits) -> DecodeOutcome {
    let Some(crlf) = find_crlf_or_lf(input) else {
        // No line terminator yet. Guard the inline-length cap so a peer cannot
        // make us buffer unboundedly waiting for a newline.
        if input.len() > limits.max_inline_len {
            return DecodeOutcome::Error(ErrorReply::protocol("too big inline request"));
        }
        return DecodeOutcome::Incomplete;
    };
    let (line_end, consumed) = crlf;
    if line_end > limits.max_inline_len {
        return DecodeOutcome::Error(ErrorReply::protocol("too big inline request"));
    }
    let line = &input[..line_end];
    match split_inline(line) {
        Ok(args) => {
            if args.is_empty() {
                // A blank line: skip it and parse the next frame.
                return with_consumed_prefix(consumed, decode(&input[consumed..], limits));
            }
            DecodeOutcome::Complete {
                request: Request { args },
                consumed,
            }
        }
        Err(e) => DecodeOutcome::Error(e),
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
/// quotes the way redis-cli's `sdssplitargs` does (enough for handshake and
/// netcat use). Unbalanced quotes are a protocol error.
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
/// key/value pairs), then decode whatever follows. IronCache tolerates but does
/// not act on attributes in v1 (PROTOCOL.md).
fn decode_and_skip_attribute(input: &[u8], limits: &Limits) -> DecodeOutcome {
    let Some(crlf) = find_crlf(input, 1) else {
        return DecodeOutcome::Incomplete;
    };
    let Some(pairs) = parse_i64(&input[1..crlf]) else {
        return DecodeOutcome::Error(ErrorReply::protocol("invalid attribute length"));
    };
    if pairs < 0 {
        return DecodeOutcome::Error(ErrorReply::protocol("invalid attribute length"));
    }
    // Skip 2*pairs bulk strings. Attributes are client->server rare; we only need
    // to consume them. Each element here is expected to be a bulk string.
    let mut pos = crlf + 2;
    for _ in 0..(pairs * 2) {
        if pos >= input.len() {
            return DecodeOutcome::Incomplete;
        }
        if input[pos] != b'$' {
            return DecodeOutcome::Error(ErrorReply::protocol("invalid attribute element"));
        }
        let Some(len_crlf) = find_crlf(input, pos + 1) else {
            return DecodeOutcome::Incomplete;
        };
        let Some(blen) = parse_i64(&input[pos + 1..len_crlf]) else {
            return DecodeOutcome::Error(ErrorReply::protocol("invalid bulk length"));
        };
        if blen < 0 || blen > limits.max_bulk_len {
            return DecodeOutcome::Error(ErrorReply::protocol("invalid bulk length"));
        }
        let data_end = len_crlf + 2 + blen as usize;
        if data_end + 2 > input.len() {
            return DecodeOutcome::Incomplete;
        }
        pos = data_end + 2;
    }
    with_consumed_prefix(pos, decode(&input[pos..], limits))
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
}
