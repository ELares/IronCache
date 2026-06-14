// SPDX-License-Identifier: MIT OR Apache-2.0
//! RESP serializer, parameterized by the connection's negotiated protocol
//! (PROTOCOL.md "serializer and reply shaping", ADR-0019).
//!
//! [`encode`] writes a [`Value`] into a byte buffer. Under [`ProtoVersion::Resp3`]
//! it emits the native RESP3 aggregates and scalars (map `%`, set `~`, double `,`,
//! boolean `#`, big number `(`, verbatim `=`, push `>`, null `_`); under
//! [`ProtoVersion::Resp2`] each degrades to its RESP2 equivalent:
//!
//! - `Null` -> `$-1` (or `*-1` is never produced by us; we use the bulk null).
//! - `Double` -> bulk string of the formatted number.
//! - `Boolean` -> `:1` / `:0`.
//! - `BigNumber` -> bulk string.
//! - `VerbatimString` -> bulk string of the data (format prefix dropped).
//! - `Map` -> flat array of `[k0, v0, k1, v1, ...]`.
//! - `Set` -> array.
//! - `Push` -> array.
//! - `BulkError` -> simple error line.
//!
//! ## Freeze point
//!
//! `encode` and the degradation rules are a freeze point: the differential
//! harness pins these bytes against the Valkey oracle in both proto modes.

use crate::error::ErrorReply;
use crate::value::{ProtoVersion, Value};
use bytes::{BufMut, BytesMut};

/// Encode `value` into `out` for protocol version `proto`.
// One arm per RESP type, each with its RESP2/RESP3 degradation inline; splitting
// the match into per-type fns would scatter the one authoritative shaping table
// (ADR-0019) across the file and hurt reviewability, so the length is deliberate.
#[allow(clippy::too_many_lines)]
pub fn encode(out: &mut BytesMut, value: &Value, proto: ProtoVersion) {
    match value {
        Value::SimpleString(s) => {
            out.put_u8(b'+');
            out.put_slice(sanitize_line(s).as_bytes());
            crlf(out);
        }
        Value::Error(e) => encode_error_line(out, e),
        Value::Integer(n) => {
            out.put_u8(b':');
            put_i64(out, *n);
            crlf(out);
        }
        Value::BulkString(opt) => match opt {
            Some(data) => {
                out.put_u8(b'$');
                put_i64(out, data.len() as i64);
                crlf(out);
                out.put_slice(data);
                crlf(out);
            }
            None => null_bulk(out, proto),
        },
        Value::Array(opt) => match opt {
            Some(items) => {
                out.put_u8(b'*');
                put_i64(out, items.len() as i64);
                crlf(out);
                for item in items {
                    encode(out, item, proto);
                }
            }
            None => null_array(out, proto),
        },
        Value::Null => match proto {
            ProtoVersion::Resp3 => {
                out.put_slice(b"_\r\n");
            }
            ProtoVersion::Resp2 => null_bulk(out, proto),
        },
        Value::Double(d) => match proto {
            ProtoVersion::Resp3 => {
                out.put_u8(b',');
                put_double(out, *d);
                crlf(out);
            }
            ProtoVersion::Resp2 => {
                let s = format_double(*d);
                encode(out, &Value::bulk_str(&s), proto);
            }
        },
        Value::Boolean(b) => match proto {
            ProtoVersion::Resp3 => {
                out.put_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
            }
            ProtoVersion::Resp2 => {
                encode(out, &Value::Integer(i64::from(*b)), proto);
            }
        },
        Value::BigNumber(s) => match proto {
            ProtoVersion::Resp3 => {
                out.put_u8(b'(');
                out.put_slice(sanitize_line(s).as_bytes());
                crlf(out);
            }
            ProtoVersion::Resp2 => {
                encode(out, &Value::bulk_str(s), proto);
            }
        },
        Value::BulkError(e) => match proto {
            ProtoVersion::Resp3 => {
                let line = e.line(); // "-TOKEN msg"
                let body = &line[1..]; // drop the leading '-'
                out.put_u8(b'!');
                put_i64(out, body.len() as i64);
                crlf(out);
                out.put_slice(body.as_bytes());
                crlf(out);
            }
            ProtoVersion::Resp2 => encode_error_line(out, e),
        },
        Value::VerbatimString { format, data } => match proto {
            ProtoVersion::Resp3 => {
                // =<len>\r\n<fmt>:<data>\r\n  where len counts "fmt:" + data.
                let total = 4 + data.len();
                out.put_u8(b'=');
                put_i64(out, total as i64);
                crlf(out);
                out.put_slice(format);
                out.put_u8(b':');
                out.put_slice(data);
                crlf(out);
            }
            ProtoVersion::Resp2 => {
                encode(out, &Value::BulkString(Some(data.clone())), proto);
            }
        },
        Value::Map(pairs) => match proto {
            ProtoVersion::Resp3 => {
                out.put_u8(b'%');
                put_i64(out, pairs.len() as i64);
                crlf(out);
                for (k, v) in pairs {
                    encode(out, k, proto);
                    encode(out, v, proto);
                }
            }
            ProtoVersion::Resp2 => {
                out.put_u8(b'*');
                put_i64(out, (pairs.len() * 2) as i64);
                crlf(out);
                for (k, v) in pairs {
                    encode(out, k, proto);
                    encode(out, v, proto);
                }
            }
        },
        Value::Set(items) => {
            let marker = if proto == ProtoVersion::Resp3 {
                b'~'
            } else {
                b'*'
            };
            out.put_u8(marker);
            put_i64(out, items.len() as i64);
            crlf(out);
            for item in items {
                encode(out, item, proto);
            }
        }
        Value::Push(items) => {
            let marker = if proto == ProtoVersion::Resp3 {
                b'>'
            } else {
                b'*'
            };
            out.put_u8(marker);
            put_i64(out, items.len() as i64);
            crlf(out);
            for item in items {
                encode(out, item, proto);
            }
        }
    }
}

/// Convenience: encode into a fresh [`BytesMut`] and return it.
#[must_use]
pub fn encode_to_vec(value: &Value, proto: ProtoVersion) -> Vec<u8> {
    let mut out = BytesMut::new();
    encode(&mut out, value, proto);
    out.to_vec()
}

fn encode_error_line(out: &mut BytesMut, e: &ErrorReply) {
    // line() is "-TOKEN message"; sanitize against embedded CR/LF which would
    // break framing (a simple error cannot contain a raw newline).
    let line = e.line();
    out.put_slice(sanitize_line(&line).as_bytes());
    crlf(out);
}

fn null_bulk(out: &mut BytesMut, _proto: ProtoVersion) {
    out.put_slice(b"$-1\r\n");
}

fn null_array(out: &mut BytesMut, proto: ProtoVersion) {
    match proto {
        ProtoVersion::Resp3 => out.put_slice(b"_\r\n"),
        ProtoVersion::Resp2 => out.put_slice(b"*-1\r\n"),
    }
}

fn crlf(out: &mut BytesMut) {
    out.put_slice(b"\r\n");
}

fn put_i64(out: &mut BytesMut, n: i64) {
    let mut buf = itoa_buf();
    let s = i64_to_str(n, &mut buf);
    out.put_slice(s);
}

/// Replace any CR or LF in a simple-string/error line with a space, so embedded
/// newlines cannot break RESP framing. Bulk strings are length-prefixed and are
/// never sanitized.
fn sanitize_line(s: &str) -> std::borrow::Cow<'_, str> {
    if s.bytes().any(|b| b == b'\r' || b == b'\n') {
        std::borrow::Cow::Owned(s.replace(['\r', '\n'], " "))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

fn put_double(out: &mut BytesMut, d: f64) {
    out.put_slice(format_double(d).as_bytes());
}

/// Format a double the way Redis does: `inf`/`-inf`/`nan` specials, otherwise the
/// shortest round-trip representation.
fn format_double(d: f64) -> String {
    if d.is_nan() {
        "nan".to_owned()
    } else if d.is_infinite() {
        if d > 0.0 {
            "inf".to_owned()
        } else {
            "-inf".to_owned()
        }
    } else {
        // Rust's default float formatting is shortest-round-trip.
        format!("{d}")
    }
}

// Minimal integer formatting without pulling the itoa crate (keeps the dep set
// small for a freeze-point crate). 20 bytes holds any i64 plus sign.
fn itoa_buf() -> [u8; 20] {
    [0u8; 20]
}

fn i64_to_str(n: i64, buf: &mut [u8; 20]) -> &[u8] {
    if n == 0 {
        buf[0] = b'0';
        return &buf[..1];
    }
    let neg = n < 0;
    // Work in u64 to handle i64::MIN without overflow.
    let mut v: u64 = if neg {
        (n as i128).unsigned_abs() as u64
    } else {
        n as u64
    };
    let mut i = buf.len();
    while v > 0 {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    // Shift to the front so we can return a contiguous slice from index 0.
    let len = buf.len() - i;
    buf.copy_within(i.., 0);
    &buf[..len]
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn enc(v: &Value, p: ProtoVersion) -> Vec<u8> {
        encode_to_vec(v, p)
    }

    #[test]
    fn simple_and_integer() {
        assert_eq!(enc(&Value::ok(), ProtoVersion::Resp2), b"+OK\r\n");
        assert_eq!(enc(&Value::Integer(123), ProtoVersion::Resp2), b":123\r\n");
        assert_eq!(enc(&Value::Integer(-9), ProtoVersion::Resp2), b":-9\r\n");
        assert_eq!(enc(&Value::Integer(0), ProtoVersion::Resp2), b":0\r\n");
        assert_eq!(
            enc(&Value::Integer(i64::MIN), ProtoVersion::Resp2),
            format!(":{}\r\n", i64::MIN).as_bytes()
        );
    }

    #[test]
    fn bulk_and_null() {
        assert_eq!(
            enc(&Value::bulk_str("hello"), ProtoVersion::Resp2),
            b"$5\r\nhello\r\n"
        );
        assert_eq!(
            enc(&Value::BulkString(None), ProtoVersion::Resp2),
            b"$-1\r\n"
        );
        // RESP3 null is `_`.
        assert_eq!(enc(&Value::Null, ProtoVersion::Resp3), b"_\r\n");
        // RESP2 null degrades to the null bulk.
        assert_eq!(enc(&Value::Null, ProtoVersion::Resp2), b"$-1\r\n");
    }

    #[test]
    fn array_nested() {
        let v = Value::Array(Some(vec![
            Value::Integer(1),
            Value::bulk_str("two"),
            Value::Array(Some(vec![Value::ok()])),
        ]));
        assert_eq!(
            enc(&v, ProtoVersion::Resp2),
            b"*3\r\n:1\r\n$3\r\ntwo\r\n*1\r\n+OK\r\n"
        );
    }

    #[test]
    fn null_array_degrades() {
        assert_eq!(enc(&Value::Array(None), ProtoVersion::Resp2), b"*-1\r\n");
        assert_eq!(enc(&Value::Array(None), ProtoVersion::Resp3), b"_\r\n");
    }

    #[test]
    fn boolean_shaping() {
        assert_eq!(enc(&Value::Boolean(true), ProtoVersion::Resp3), b"#t\r\n");
        assert_eq!(enc(&Value::Boolean(false), ProtoVersion::Resp3), b"#f\r\n");
        assert_eq!(enc(&Value::Boolean(true), ProtoVersion::Resp2), b":1\r\n");
        assert_eq!(enc(&Value::Boolean(false), ProtoVersion::Resp2), b":0\r\n");
    }

    #[test]
    fn double_shaping() {
        assert_eq!(enc(&Value::Double(3.5), ProtoVersion::Resp3), b",3.5\r\n");
        assert_eq!(
            enc(&Value::Double(f64::INFINITY), ProtoVersion::Resp3),
            b",inf\r\n"
        );
        // RESP2 degrades to a bulk string.
        assert_eq!(
            enc(&Value::Double(3.5), ProtoVersion::Resp2),
            b"$3\r\n3.5\r\n"
        );
    }

    #[test]
    fn map_shaping() {
        let m = Value::Map(vec![(Value::bulk_str("k"), Value::Integer(1))]);
        assert_eq!(enc(&m, ProtoVersion::Resp3), b"%1\r\n$1\r\nk\r\n:1\r\n");
        // RESP2: flat array of length 2.
        assert_eq!(enc(&m, ProtoVersion::Resp2), b"*2\r\n$1\r\nk\r\n:1\r\n");
    }

    #[test]
    fn set_and_push_shaping() {
        let s = Value::Set(vec![Value::Integer(1)]);
        assert_eq!(enc(&s, ProtoVersion::Resp3), b"~1\r\n:1\r\n");
        assert_eq!(enc(&s, ProtoVersion::Resp2), b"*1\r\n:1\r\n");
        let p = Value::Push(vec![Value::bulk_str("message")]);
        assert_eq!(enc(&p, ProtoVersion::Resp3), b">1\r\n$7\r\nmessage\r\n");
        assert_eq!(enc(&p, ProtoVersion::Resp2), b"*1\r\n$7\r\nmessage\r\n");
    }

    #[test]
    fn verbatim_shaping() {
        let v = Value::VerbatimString {
            format: *b"txt",
            data: Bytes::from_static(b"hi"),
        };
        assert_eq!(enc(&v, ProtoVersion::Resp3), b"=6\r\ntxt:hi\r\n");
        assert_eq!(enc(&v, ProtoVersion::Resp2), b"$2\r\nhi\r\n");
    }

    #[test]
    fn error_line_and_bulk_error() {
        let e = ErrorReply::wrong_type();
        assert_eq!(
            enc(&Value::Error(e.clone()), ProtoVersion::Resp2),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        // BulkError under RESP3 uses '!' with the length of "TOKEN msg".
        let be = Value::BulkError(e);
        let out = enc(&be, ProtoVersion::Resp3);
        assert!(out.starts_with(b"!"));
        assert!(out.ends_with(b"\r\n"));
    }

    #[test]
    fn simple_string_sanitizes_newlines() {
        let v = Value::SimpleString("a\r\nb".to_owned());
        assert_eq!(enc(&v, ProtoVersion::Resp2), b"+a  b\r\n");
    }
}
