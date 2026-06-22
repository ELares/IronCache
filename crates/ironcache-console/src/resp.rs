// SPDX-License-Identifier: MIT OR Apache-2.0
//! A minimal RESP2 reply parser (issue #355).
//!
//! The console talks to an IronCache node only to issue a few read-only admin
//! commands (`AUTH`, `PING`, `INFO`), so it needs to PARSE replies, not be a full
//! client. This module is the pure, allocation-bounded parser: given a byte
//! buffer it returns one complete [`RespValue`] plus the number of bytes it
//! consumed, or "incomplete" so the caller reads more and retries.
//!
//! It handles the five RESP2 leading bytes only (`+ - : $ *`); RESP3 push/map/set
//! frames are out of scope (the console never enters RESP3 against a node). The
//! framing mirrors `ironcache-bench`'s client parser but is reimplemented here so
//! the console takes no dependency on the bench crate.
//!
//! ## Hardening
//!
//! A node reply is attacker-controlled (a hostile or compromised node), so the
//! parser is bounded against three abuse shapes before it does any work:
//! * nesting depth ([`MAX_DEPTH`]): the array case recurses, so a frame that is
//!   `*1\r\n` repeated millions of times would overflow the stack and abort the
//!   process; we cap the recursion and reject deeper frames. (The engine's own
//!   decoder is iterative for the same reason; this small admin parser caps the
//!   recursion instead of rewriting it.)
//! * declared array count ([`MAX_ELEMENTS`]): a huge `*<count>` cannot drive a
//!   large pre-allocation or a long element loop.
//! * declared bulk length ([`MAX_BULK`]) with checked arithmetic: a huge `$<len>`
//!   (or one near `usize::MAX`) cannot overflow the body-end computation; it is
//!   rejected up front rather than relying on wraparound plus the caller's read
//!   cap.
//!
//! ## Determinism (ADR-0003)
//!
//! Pure byte parsing: no clock, no RNG, no I/O. The invariant lint is satisfied
//! by construction.

/// Max nesting depth the parser will descend before rejecting a frame. The
/// console only ever parses flat admin replies (`+OK`, an `INFO` bulk, a small
/// array), so a generous cap rejects a stack-overflow attack (`*1\r\n` repeated)
/// without ever tripping on a real reply.
const MAX_DEPTH: usize = 32;

/// Max declared element count an array frame may carry. The console's admin
/// replies are tiny; a `*<count>` larger than this is rejected before the element
/// loop (and before any pre-allocation), so a hostile count cannot amplify work.
const MAX_ELEMENTS: usize = 1 << 20;

/// Max declared body length a bulk frame may carry (8 MiB). Matches the client's
/// per-reply read cap; a `$<len>` larger than this is rejected up front so a
/// hostile length cannot drive a large allocation or overflow the body-end math.
const MAX_BULK: usize = 8 * 1024 * 1024;

/// One decoded RESP2 value. The variants carry the raw bytes (a node may return
/// non-UTF-8 in a bulk string) so the caller decides how to interpret them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    /// `+<line>` simple string (the body, without the leading `+` or the CRLF).
    Simple(Vec<u8>),
    /// `-<line>` error (the body, without the leading `-` or the CRLF).
    Error(Vec<u8>),
    /// `:<n>` integer.
    Integer(i64),
    /// `$<len>\r\n<body>\r\n` bulk string. `None` is the null bulk (`$-1`).
    Bulk(Option<Vec<u8>>),
    /// `*<count>\r\n<elements...>` array of nested values. A null array (`*-1`)
    /// is decoded as an empty array (the console never distinguishes the two).
    Array(Vec<RespValue>),
}

impl RespValue {
    /// Borrow the bytes of a [`RespValue::Simple`] or [`RespValue::Bulk(Some)`],
    /// the shape the console's admin replies take (`+OK`, the `INFO` bulk).
    /// Returns `None` for an error, integer, null bulk, or array.
    #[must_use]
    pub fn as_text_bytes(&self) -> Option<&[u8]> {
        match self {
            RespValue::Simple(b) | RespValue::Bulk(Some(b)) => Some(b),
            _ => None,
        }
    }
}

/// The outcome of attempting to parse one reply from a buffer.
#[derive(Debug)]
pub enum ParseError {
    /// The frame was malformed: an unknown leading byte, an unparseable length /
    /// integer, or a count/length that cannot be represented.
    Protocol(&'static str),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Protocol(m) => write!(f, "RESP protocol error: {m}"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Parse ONE complete RESP2 reply from the FRONT of `buf`.
///
/// Returns:
/// * `Ok(Some((value, consumed)))` when a full reply is present (`consumed` bytes
///   from the front of `buf` made it up; the caller drops those),
/// * `Ok(None)` when the buffer holds only a partial reply (read more, retry),
/// * `Err(ParseError::Protocol)` on a malformed frame.
///
/// # Errors
///
/// Returns [`ParseError::Protocol`] on an unknown leading byte or an unparseable
/// length / integer / count.
pub fn parse_reply(buf: &[u8]) -> Result<Option<(RespValue, usize)>, ParseError> {
    let mut pos = 0usize;
    match parse_at(buf, &mut pos, 0)? {
        Some(value) => Ok(Some((value, pos))),
        None => Ok(None),
    }
}

/// Parse one value starting at `*pos`, advancing `*pos` past it on success.
/// `Ok(None)` means "need more bytes" (the cursor is left unspecified; the caller
/// retries from a fresh cursor). Recursive for the array case; `depth` is the
/// current nesting level, bounded by [`MAX_DEPTH`] so a deeply nested (hostile)
/// frame cannot overflow the stack.
fn parse_at(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Option<RespValue>, ParseError> {
    if depth > MAX_DEPTH {
        return Err(ParseError::Protocol("reply nesting too deep"));
    }
    let Some(&tag) = buf.get(*pos) else {
        return Ok(None);
    };
    match tag {
        b'+' | b'-' | b':' => parse_line(buf, pos, tag),
        b'$' => parse_bulk(buf, pos),
        b'*' => parse_array(buf, pos, depth),
        _ => Err(ParseError::Protocol("unknown RESP leading byte")),
    }
}

/// Parse a single CRLF-terminated line frame (`+`, `-`, or `:`).
fn parse_line(buf: &[u8], pos: &mut usize, tag: u8) -> Result<Option<RespValue>, ParseError> {
    let Some(cr) = find_crlf(buf, *pos + 1) else {
        return Ok(None);
    };
    let line = buf[*pos + 1..cr].to_vec();
    *pos = cr + 2;
    match tag {
        b'+' => Ok(Some(RespValue::Simple(line))),
        b'-' => Ok(Some(RespValue::Error(line))),
        _ => match std::str::from_utf8(&line).ok().and_then(|s| s.parse().ok()) {
            Some(n) => Ok(Some(RespValue::Integer(n))),
            None => Err(ParseError::Protocol("invalid integer reply")),
        },
    }
}

/// Parse a bulk-string frame (`$<len>\r\n<body>\r\n`, or `$-1\r\n` for null).
fn parse_bulk(buf: &[u8], pos: &mut usize) -> Result<Option<RespValue>, ParseError> {
    let Some(cr) = find_crlf(buf, *pos + 1) else {
        return Ok(None);
    };
    let len: i64 = match std::str::from_utf8(&buf[*pos + 1..cr])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return Err(ParseError::Protocol("invalid bulk length")),
    };
    let body_start = cr + 2;
    if len < 0 {
        // Null bulk: `$-1\r\n`. No body follows.
        *pos = body_start;
        return Ok(Some(RespValue::Bulk(None)));
    }
    // Reject an absurd declared length up front: the console only parses small
    // admin replies, and a huge `$<len>` must not drive a large allocation.
    let Ok(len) = usize::try_from(len) else {
        return Err(ParseError::Protocol("bulk length overflow"));
    };
    if len > MAX_BULK {
        return Err(ParseError::Protocol("bulk length too large"));
    }
    // Compute the body end (and the CRLF-included end) with CHECKED arithmetic so
    // a length near `usize::MAX` cannot wrap; never rely on wraparound plus the
    // caller's read cap to catch it.
    let Some(body_end) = body_start.checked_add(len) else {
        return Err(ParseError::Protocol("bulk length overflow"));
    };
    let Some(frame_end) = body_end.checked_add(2) else {
        return Err(ParseError::Protocol("bulk length overflow"));
    };
    // Need the body plus its trailing CRLF.
    if buf.len() < frame_end {
        return Ok(None);
    }
    let body = buf[body_start..body_end].to_vec();
    *pos = frame_end;
    Ok(Some(RespValue::Bulk(Some(body))))
}

/// Parse an array frame (`*<count>\r\n<elements...>`), recursing per element.
/// `depth` is the current nesting level; each element is parsed at `depth + 1`
/// so [`MAX_DEPTH`] bounds the recursion (a deeply nested hostile frame is
/// rejected, not stack-overflowed).
fn parse_array(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Option<RespValue>, ParseError> {
    let Some(cr) = find_crlf(buf, *pos + 1) else {
        return Ok(None);
    };
    let count: i64 = match std::str::from_utf8(&buf[*pos + 1..cr])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return Err(ParseError::Protocol("invalid array count")),
    };
    let after_header = cr + 2;
    if count < 0 {
        // Null array (`*-1`): decode as an empty array. No elements follow.
        *pos = after_header;
        return Ok(Some(RespValue::Array(Vec::new())));
    }
    let Ok(count) = usize::try_from(count) else {
        return Err(ParseError::Protocol("array too large"));
    };
    // Reject an absurd declared count BEFORE the element loop (and before the
    // pre-allocation below), so a hostile `*<count>` cannot amplify work.
    if count > MAX_ELEMENTS {
        return Err(ParseError::Protocol("array too large"));
    }
    // Parse elements from a LOCAL cursor so a mid-array "need more" leaves the
    // caller's cursor untouched (it retries the whole frame from the start).
    let mut local = after_header;
    let mut items = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        match parse_at(buf, &mut local, depth + 1)? {
            Some(v) => items.push(v),
            None => return Ok(None),
        }
    }
    *pos = local;
    Ok(Some(RespValue::Array(items)))
}

/// Find the next CRLF at or after `from`, returning the index of the `\r`. `None`
/// if no complete line terminator is present yet.
fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse one reply, asserting it completes, and return it with the byte count.
    fn parse_one(bytes: &[u8]) -> (RespValue, usize) {
        match parse_reply(bytes) {
            Ok(Some((v, n))) => (v, n),
            Ok(None) => panic!("expected a complete reply, got incomplete"),
            Err(e) => panic!("protocol error: {e}"),
        }
    }

    #[test]
    fn parses_simple_string() {
        let (v, n) = parse_one(b"+OK\r\n");
        assert_eq!(v, RespValue::Simple(b"OK".to_vec()));
        assert_eq!(n, 5);
    }

    #[test]
    fn parses_error() {
        let (v, n) = parse_one(b"-WRONGPASS invalid\r\n");
        assert_eq!(v, RespValue::Error(b"WRONGPASS invalid".to_vec()));
        assert_eq!(n, 20);
    }

    #[test]
    fn parses_integer_including_negative() {
        let (v, n) = parse_one(b":42\r\n");
        assert_eq!(v, RespValue::Integer(42));
        assert_eq!(n, 5);
        let (v, _) = parse_one(b":-7\r\n");
        assert_eq!(v, RespValue::Integer(-7));
    }

    #[test]
    fn parses_bulk_body() {
        let (v, n) = parse_one(b"$5\r\nhello\r\n");
        assert_eq!(v, RespValue::Bulk(Some(b"hello".to_vec())));
        assert_eq!(n, 11);
    }

    #[test]
    fn parses_empty_bulk() {
        let (v, n) = parse_one(b"$0\r\n\r\n");
        assert_eq!(v, RespValue::Bulk(Some(Vec::new())));
        assert_eq!(n, 6);
    }

    #[test]
    fn parses_null_bulk() {
        let (v, n) = parse_one(b"$-1\r\n");
        assert_eq!(v, RespValue::Bulk(None));
        assert_eq!(n, 5);
    }

    #[test]
    fn parses_bulk_with_embedded_crlf() {
        // A bulk body is length-delimited, so an embedded CRLF is part of the body:
        // a 6-byte body `a\r\nbcd` framed as `$6\r\na\r\nbcd\r\n`.
        let (v, n) = parse_one(b"$6\r\na\r\nbcd\r\n");
        assert_eq!(v, RespValue::Bulk(Some(b"a\r\nbcd".to_vec())));
        assert_eq!(n, 12);
    }

    #[test]
    fn parses_flat_array() {
        let (v, n) = parse_one(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"foo".to_vec())),
                RespValue::Bulk(Some(b"bar".to_vec())),
            ])
        );
        assert_eq!(n, 22);
    }

    #[test]
    fn parses_nested_array() {
        // *2 -> [ :1 , *2 -> [ +a , $1 b ] ]
        let frame = b"*2\r\n:1\r\n*2\r\n+a\r\n$1\r\nb\r\n";
        let (v, n) = parse_one(frame);
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::Array(vec![
                    RespValue::Simple(b"a".to_vec()),
                    RespValue::Bulk(Some(b"b".to_vec())),
                ]),
            ])
        );
        assert_eq!(n, frame.len());
    }

    #[test]
    fn parses_empty_and_null_arrays() {
        let (v, n) = parse_one(b"*0\r\n");
        assert_eq!(v, RespValue::Array(Vec::new()));
        assert_eq!(n, 4);
        let (v, n) = parse_one(b"*-1\r\n");
        assert_eq!(v, RespValue::Array(Vec::new()));
        assert_eq!(n, 5);
    }

    #[test]
    fn incomplete_inputs_return_none() {
        // Empty buffer.
        assert!(matches!(parse_reply(b""), Ok(None)));
        // A leading byte with no CRLF yet.
        assert!(matches!(parse_reply(b"+OK"), Ok(None)));
        // A bulk header whose body has not fully arrived.
        assert!(matches!(parse_reply(b"$5\r\nhel"), Ok(None)));
        // A bulk header with the body but no trailing CRLF yet.
        assert!(matches!(parse_reply(b"$5\r\nhello"), Ok(None)));
        // An array whose second element is still partial.
        assert!(matches!(
            parse_reply(b"*2\r\n$3\r\nfoo\r\n$3\r\nba"),
            Ok(None)
        ));
    }

    #[test]
    fn rejects_unknown_leading_byte() {
        assert!(matches!(
            parse_reply(b"?bogus\r\n"),
            Err(ParseError::Protocol(_))
        ));
    }

    #[test]
    fn rejects_bad_integer_and_length() {
        assert!(matches!(
            parse_reply(b":notanint\r\n"),
            Err(ParseError::Protocol(_))
        ));
        assert!(matches!(
            parse_reply(b"$abc\r\n"),
            Err(ParseError::Protocol(_))
        ));
        assert!(matches!(
            parse_reply(b"*xyz\r\n"),
            Err(ParseError::Protocol(_))
        ));
    }

    #[test]
    fn reports_consumed_bytes_for_a_pipelined_buffer() {
        // Two replies back to back: the first parse consumes exactly the first.
        let buf = b"+OK\r\n:1\r\n";
        let (v, n) = parse_one(buf);
        assert_eq!(v, RespValue::Simple(b"OK".to_vec()));
        assert_eq!(n, 5);
        // Parsing the remainder yields the second.
        let (v2, n2) = parse_one(&buf[n..]);
        assert_eq!(v2, RespValue::Integer(1));
        assert_eq!(n2, 4);
    }

    #[test]
    fn as_text_bytes_extracts_simple_and_bulk() {
        assert_eq!(
            RespValue::Simple(b"OK".to_vec()).as_text_bytes(),
            Some(&b"OK"[..])
        );
        assert_eq!(
            RespValue::Bulk(Some(b"data".to_vec())).as_text_bytes(),
            Some(&b"data"[..])
        );
        assert_eq!(RespValue::Bulk(None).as_text_bytes(), None);
        assert_eq!(RespValue::Integer(1).as_text_bytes(), None);
    }

    #[test]
    fn deeply_nested_array_is_rejected_not_stack_overflowed() {
        // A frame that is `*1\r\n` repeated: each level declares a 1-element array
        // whose sole element is the next array. ~100_000 levels would blow the
        // stack with an unbounded recursive parser; the depth cap must reject it
        // with a Protocol error instead (this test returning at all proves no
        // stack overflow).
        let frame = b"*1\r\n".repeat(100_000);
        match parse_reply(&frame) {
            Err(ParseError::Protocol(m)) => assert_eq!(m, "reply nesting too deep"),
            other => panic!("expected a nesting-too-deep protocol error, got {other:?}"),
        }
    }

    #[test]
    fn nesting_at_the_limit_parses_but_one_deeper_is_rejected() {
        // Exactly MAX_DEPTH nested 1-element arrays with an integer leaf parses.
        let mut ok = Vec::new();
        for _ in 0..MAX_DEPTH {
            ok.extend_from_slice(b"*1\r\n");
        }
        ok.extend_from_slice(b":1\r\n");
        assert!(matches!(parse_reply(&ok), Ok(Some(_))), "{ok:?}");
        // One level deeper exceeds the cap and is rejected.
        let mut too_deep = Vec::new();
        for _ in 0..(MAX_DEPTH + 2) {
            too_deep.extend_from_slice(b"*1\r\n");
        }
        too_deep.extend_from_slice(b":1\r\n");
        assert!(matches!(
            parse_reply(&too_deep),
            Err(ParseError::Protocol("reply nesting too deep"))
        ));
    }

    #[test]
    fn huge_declared_array_count_is_rejected() {
        // A `*<count>` larger than MAX_ELEMENTS is rejected before the element
        // loop, so a hostile count cannot amplify work or pre-allocate hugely.
        let frame = format!("*{}\r\n", (MAX_ELEMENTS as u64) + 1);
        assert!(matches!(
            parse_reply(frame.as_bytes()),
            Err(ParseError::Protocol("array too large"))
        ));
        // A large in-range count (fits i64, far above MAX_ELEMENTS) is rejected as
        // too large, never wrapping to a small one.
        let frame = format!("*{}\r\n", i64::MAX);
        assert!(matches!(
            parse_reply(frame.as_bytes()),
            Err(ParseError::Protocol("array too large"))
        ));
        // A count beyond i64 fails the count parse outright (invalid, not wrapped).
        let frame = format!("*{}\r\n", u64::MAX);
        assert!(matches!(
            parse_reply(frame.as_bytes()),
            Err(ParseError::Protocol("invalid array count"))
        ));
    }

    #[test]
    fn huge_declared_bulk_length_is_rejected() {
        // A `$<len>` larger than MAX_BULK is rejected up front.
        let frame = format!("${}\r\n", (MAX_BULK as u64) + 1);
        assert!(matches!(
            parse_reply(frame.as_bytes()),
            Err(ParseError::Protocol("bulk length too large"))
        ));
        // A len near i64::MAX would overflow `body_start + len`; checked math must
        // reject it as an overflow rather than wrapping.
        let frame = format!("${}\r\n", i64::MAX);
        assert!(matches!(
            parse_reply(frame.as_bytes()),
            Err(ParseError::Protocol(
                "bulk length too large" | "bulk length overflow"
            ))
        ));
    }
}
