// SPDX-License-Identifier: MIT OR Apache-2.0
//! RESP serializer, parameterized by the connection's negotiated protocol
//! (PROTOCOL.md "serializer and reply shaping", ADR-0019).
//!
//! [`encode`] writes a [`Value`] into a byte buffer. Under [`ProtoVersion::Resp3`]
//! it emits the native RESP3 aggregates and scalars (map `%`, set `~`, double `,`,
//! boolean `#`, big number `(`, verbatim `=`, push `>`, null `_`); under
//! [`ProtoVersion::Resp2`] each degrades to its RESP2 equivalent:
//!
//! - Null shaping by source type: a scalar `Value::Null` degrades to `$-1`
//!   (RESP2) and a `Value::Array(None)` degrades to `*-1` (RESP2); under RESP3
//!   both render as the single null marker `_`.
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

/// Format a double the way Redis does (`util.c` `d2string` -> `fpconv_dtoa`):
/// `inf`/`-inf`/`nan` specials, integral values with NO decimal point (`3`,
/// `100`), very large/small magnitudes in `e+NN`/`e-N` exponent form (`1e+100`,
/// `1e-7`), and everything else as the shortest round-trip decimal (`3.5`, `0.1`,
/// `3.141592653589793`).
///
/// We obtain the shortest significant digits from `ryu` and then re-emit them with
/// fpconv's exact `emit_digits` format-selection rule (the same thresholds Redis
/// vendors), so the listed handshake/score cases are byte-identical to Redis.
///
/// Note on `nan`: Redis rejects NaN scores before reaching the double encoder
/// (`ZADD`/`INCRBYFLOAT` validate first), so no command path actually emits
/// `,nan`. The arm exists for spec-completeness only; PR-2/PR-3 command authors
/// must NOT assume a NaN score round-trips through this encoder.
fn format_double(d: f64) -> String {
    if d.is_nan() {
        return "nan".to_owned();
    }
    if d.is_infinite() {
        return if d > 0.0 {
            "inf".to_owned()
        } else {
            "-inf".to_owned()
        };
    }
    if d == 0.0 {
        // fpconv emits integral zero with no decimal point, sign preserved.
        return if d.is_sign_negative() {
            "-0".to_owned()
        } else {
            "0".to_owned()
        };
    }

    let mut buf = ryu::Buffer::new();
    let (neg, digits, k) = decompose_ryu(buf.format(d));
    let body = emit_digits(&digits, k, neg);
    if neg { format!("-{body}") } else { body }
}

/// Decompose a finite, non-zero `ryu`-formatted f64 string into `(neg, digits, k)`
/// where `value == (digits parsed as an integer) * 10^k`, `digits` carries no
/// leading or trailing zeros. This is exactly fpconv's `(digits, ndigits, K)`
/// contract, derived from ryu's shortest output.
fn decompose_ryu(s: &str) -> (bool, String, i32) {
    let neg = s.starts_with('-');
    let s = s.strip_prefix('-').unwrap_or(s);
    let (mantissa, exp): (&str, i32) = match s.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse().unwrap_or(0)),
        None => (s, 0),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let mut k = exp - i32::try_from(frac_part.len()).unwrap_or(0);
    // Strip leading zeros (keep at least one digit).
    let lead_trimmed = digits.trim_start_matches('0');
    digits = if lead_trimmed.is_empty() {
        "0".to_owned()
    } else {
        lead_trimmed.to_owned()
    };
    // Strip trailing zeros, moving the count into K.
    if digits != "0" {
        let before = digits.len();
        let t = digits.trim_end_matches('0');
        let removed = before - t.len();
        k += i32::try_from(removed).unwrap_or(0);
        digits = t.to_owned();
    }
    (neg, digits, k)
}

/// fpconv's `emit_digits`: render the significand `digits` (with base-10 exponent
/// `k`) into Redis's exact spelling. Ported byte-for-byte from the fpconv copy
/// Redis vendors (`fpconv_dtoa.c`), including the `max_trailing_zeros` and the
/// `K < 0 && (K > -7 || exp < 4)` decimal thresholds and the unpadded `e+N`/`e-N`
/// exponent form. The leading sign is applied by the caller.
fn emit_digits(digits: &str, k: i32, neg: bool) -> String {
    let db = digits.as_bytes();
    let ndigits = i32::try_from(db.len()).unwrap_or(0);
    let mut exp = (k + ndigits - 1).abs();
    let max_trailing_zeros = if neg { 6 } else { 7 };

    let mut out = String::new();

    // Plain integer (no decimal point): e.g. 3, 100, 1234567.
    if k >= 0 && exp < (ndigits + max_trailing_zeros) {
        out.push_str(digits);
        for _ in 0..k {
            out.push('0');
        }
        return out;
    }

    // Decimal without scientific notation: e.g. 3.5, 0.1, 0.0001.
    if k < 0 && (k > -7 || exp < 4) {
        let offset = ndigits - k.abs();
        if offset <= 0 {
            // value < 1.0 -> 0.00...digits
            let zeros = (-offset) as usize;
            out.push('0');
            out.push('.');
            for _ in 0..zeros {
                out.push('0');
            }
            out.push_str(digits);
        } else {
            // value > 1.0 with a fractional part.
            let o = offset as usize;
            out.push_str(&digits[..o]);
            out.push('.');
            out.push_str(&digits[o..]);
        }
        return out;
    }

    // Scientific notation: d[.ddd]e±NN, exponent unpadded (1e+100, 1e-7).
    let nd = (ndigits as usize).min(18 - usize::from(neg));
    out.push(db[0] as char);
    if nd > 1 {
        out.push('.');
        for &c in &db[1..nd] {
            out.push(c as char);
        }
    }
    out.push('e');
    out.push(if k + ndigits - 1 < 0 { '-' } else { '+' });
    let mut cent = 0;
    if exp > 99 {
        cent = exp / 100;
        out.push((b'0' + u8::try_from(cent).unwrap_or(0)) as char);
        exp -= cent * 100;
    }
    if exp > 9 {
        let dec = exp / 10;
        out.push((b'0' + u8::try_from(dec).unwrap_or(0)) as char);
        exp -= dec * 10;
    } else if cent != 0 {
        out.push('0');
    }
    out.push((b'0' + u8::try_from(exp % 10).unwrap_or(0)) as char);
    out
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
    fn format_double_matches_fpconv() {
        // The fpconv/Redis spelling, verified against the vendored fpconv_dtoa:
        // integral doubles have no decimal point; large/small magnitudes use the
        // unpadded e+NN / e-N exponent form; otherwise shortest decimal.
        let cases: &[(f64, &str)] = &[
            (1e100, "1e+100"),
            (1e-7, "1e-7"),
            (3.0, "3"),
            (100.0, "100"),
            (3.5, "3.5"),
            (0.1, "0.1"),
            (core::f64::consts::PI, "3.141592653589793"),
            (0.0, "0"),
            (-0.0, "-0"),
            (-3.5, "-3.5"),
            (1e16, "1e+16"),
            (1e7, "10000000"),
            (1e8, "1e+8"),
            (0.000_01, "0.00001"),
            (0.000_001, "0.000001"),
            (-1e100, "-1e+100"),
        ];
        for &(d, want) in cases {
            assert_eq!(format_double(d), want, "format_double({d})");
        }
    }

    #[test]
    fn double_differential_resp2_and_resp3_shapes() {
        // The same fpconv string must appear as a RESP3 `,double` and as the body
        // of a RESP2 bulk string.
        let cases: &[(f64, &str)] = &[
            (1e100, "1e+100"),
            (1e-7, "1e-7"),
            (3.0, "3"),
            (100.0, "100"),
            (3.5, "3.5"),
            (0.1, "0.1"),
            (core::f64::consts::PI, "3.141592653589793"),
        ];
        for &(d, s) in cases {
            // RESP3: ,<s>\r\n
            let mut want3 = Vec::new();
            want3.push(b',');
            want3.extend_from_slice(s.as_bytes());
            want3.extend_from_slice(b"\r\n");
            assert_eq!(
                enc(&Value::Double(d), ProtoVersion::Resp3),
                want3,
                "RESP3 {d}"
            );
            // RESP2: $<len>\r\n<s>\r\n
            let want2 = format!("${}\r\n{}\r\n", s.len(), s).into_bytes();
            assert_eq!(
                enc(&Value::Double(d), ProtoVersion::Resp2),
                want2,
                "RESP2 {d}"
            );
        }
    }

    #[test]
    fn double_round_trips_through_parse() {
        // Every formatted double must parse back to the original f64 (shortest
        // round-trip property), across a spread of magnitudes and signs.
        let samples: &[f64] = &[
            0.1,
            3.5,
            core::f64::consts::PI,
            2.5e-8,
            9.9e22,
            -1.234_567_89e-15,
            123_456_789.012_345,
            f64::MIN_POSITIVE,
            -f64::MAX,
            1.0 / 3.0,
        ];
        for &d in samples {
            let s = format_double(d);
            let back: f64 = s.parse().unwrap();
            assert_eq!(
                back.to_bits(),
                d.to_bits(),
                "round-trip failed for {d} -> {s}"
            );
        }
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
