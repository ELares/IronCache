// SPDX-License-Identifier: MIT OR Apache-2.0
//! Small shared argument-parsing helpers for the command handlers (the dispatch
//! tier and the new string/keyspace modules). Kept in one place so the command
//! modules do not each re-roll case-folding and integer parsing.

/// ASCII-uppercase a byte slice into an owned `Vec<u8>` for case-insensitive
/// command/option matching (command and option tokens are ASCII per RESP).
#[must_use]
pub fn ascii_upper(b: &[u8]) -> Vec<u8> {
    b.iter().map(u8::to_ascii_uppercase).collect()
}

/// Parse a base-10 i64 from an argument the way Redis `string2ll` (src/util.c)
/// does: an optional single leading `-` then one or more ASCII digits, with NO
/// leading `+`, no whitespace, and no other characters. The FULL i64 range
/// including i64::MIN is accepted (the magnitude is accumulated as u64 and the
/// negative case allows up to `i64::MIN`). Returns `None` on any non-`[-]?digits`
/// form or overflow (the caller maps `None` to the appropriate error).
///
/// `str::parse::<i64>` is NOT used because it accepts a leading `+`, which Redis
/// rejects.
#[must_use]
pub fn parse_i64(arg: &[u8]) -> Option<i64> {
    if arg.is_empty() {
        return None;
    }
    let (neg, digits) = if arg[0] == b'-' {
        (true, &arg[1..])
    } else {
        (false, arg)
    };
    if digits.is_empty() {
        return None;
    }
    let mut acc: u64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
    }
    if neg {
        // Allow a magnitude up to (i64::MAX as u64) + 1, which is exactly i64::MIN.
        const MIN_MAGNITUDE: u64 = (i64::MAX as u64) + 1;
        if acc > MIN_MAGNITUDE {
            return None;
        }
        if acc == MIN_MAGNITUDE {
            return Some(i64::MIN);
        }
        Some(-(acc as i64))
    } else {
        if acc > i64::MAX as u64 {
            return None;
        }
        Some(acc as i64)
    }
}

/// Parse a base-10 i64 with the FULL `string2ll` (src/util.c) canonical rule that
/// Redis applies to the INCR/INCRBY existing value AND the increment argument
/// (`getLongLongFromObjectOrReply` -> `string2ll` for string objects). Stricter
/// than [`parse_i64`]: it ALSO rejects leading zeros (`"007"`), since `string2ll`
/// requires the first digit to be `1..=9` unless the whole string is exactly `"0"`.
///
/// Accepts: `"0"`, `"-0"` (string2ll sets the negative flag then takes the lone-`0`
/// path, so `"-0"` parses to `0`), a lone `1..=9` leading digit then digits, and a
/// single leading `-`. The full i64 range including `i64::MIN` is accepted.
///
/// Rejects: empty, a lone `"-"`, a leading `"+"`, leading zeros (`"007"`),
/// surrounding whitespace, a non-digit byte, any float form, and overflow.
///
/// This is the parser the numeric RMW commands MUST use for both operands so that a
/// non-canonical stored string (e.g. an embstr `"007"` or `"3.14"`) is the
/// not-an-integer error, matching Redis. (`parse_i64` is kept for the SET expire
/// arg, whose Redis path is `getLongLongFromObjectOrReply` too but where the
/// existing tests pin the looser behavior; the numeric commands need the strict
/// leading-zero rejection, so they call this.)
#[must_use]
pub fn parse_i64_strict(arg: &[u8]) -> Option<i64> {
    if arg.is_empty() {
        return None;
    }
    let (neg, digits) = if arg[0] == b'-' {
        (true, &arg[1..])
    } else {
        (false, arg)
    };
    if digits.is_empty() {
        return None;
    }
    // The lone "0" (and "-0", which string2ll accepts as 0) is the only form that
    // may start with '0'. Any other leading zero is rejected.
    if digits[0] == b'0' {
        return if digits.len() == 1 { Some(0) } else { None };
    }
    let mut acc: u64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
    }
    if neg {
        const MIN_MAGNITUDE: u64 = (i64::MAX as u64) + 1;
        if acc > MIN_MAGNITUDE {
            return None;
        }
        if acc == MIN_MAGNITUDE {
            return Some(i64::MIN);
        }
        Some(-(acc as i64))
    } else {
        if acc > i64::MAX as u64 {
            return None;
        }
        Some(acc as i64)
    }
}

/// Parse a float argument the way Redis `string2ld` (src/util.c) does for
/// INCRBYFLOAT, returning `None` on any input Redis rejects as not-a-valid-float.
///
/// IronCache uses `f64`, a documented precision divergence from Redis's 80-bit
/// long double (ENCODINGS.md "the integer/float fast path"); the validation rules
/// match `string2ld` as closely as `f64::from_str` allows:
///
/// - empty string -> `None` (Redis `slen == 0`).
/// - any leading OR trailing ASCII whitespace -> `None` (Redis rejects
///   `isspace(buf[0])` and requires `eptr` to consume the whole string).
/// - a parsed NaN -> `None` (Redis rejects `isnan(value)` at parse time, so a
///   `nan` increment is not-a-valid-float, NOT the result NaN/Inf error).
/// - a parsed Infinity is ALLOWED through here (Redis `strtold` parses the literal
///   `inf`/`-inf`/`infinity` tokens without ERANGE); the INCRBYFLOAT result check
///   then rejects an infinite RESULT with the NaN-or-Infinity error, matching the
///   literal-`inf` argument path.
///
/// DIVERGENCE (documented): a magnitude that OVERFLOWS to infinity on parse (e.g.
/// `1e4000`) is rejected by Redis `string2ld` as not-a-valid-float (it sees
/// `ERANGE` + `HUGE_VAL`), whereas here it parses to `f64::INFINITY` and is caught
/// one step later by the result NaN-or-Infinity check. Either way the operation is
/// rejected; only the error class differs at the extreme overflow edge.
#[must_use]
pub fn parse_f64(arg: &[u8]) -> Option<f64> {
    if arg.is_empty() {
        return None;
    }
    // Reject surrounding whitespace WITHOUT trimming (Redis does not trim): a
    // leading or trailing ASCII space/tab/newline is a parse failure. `f64::from_str`
    // would itself reject most of these, but it accepts a leading space on some
    // platforms historically; be explicit to match Redis byte for byte.
    if arg[0].is_ascii_whitespace() || arg[arg.len() - 1].is_ascii_whitespace() {
        return None;
    }
    let s = core::str::from_utf8(arg).ok()?;
    // DIVERGENCE (acknowledged extreme edge): Rust `f64::from_str` rejects C99
    // hex-float literals like "0x1p4" that Redis's strtold/string2ld accept; this is
    // practically irrelevant for a counter command and unchanged here by design.
    let v: f64 = s.parse().ok()?;
    // Redis rejects a NaN at parse time (it is not-a-valid-float, distinct from the
    // result NaN/Inf error). Infinity is allowed through (handled by the result
    // check), matching Redis's literal-`inf` argument path.
    if v.is_nan() {
        return None;
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_i64_accepts_canonical_forms_and_full_range() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"5"), Some(5));
        assert_eq!(parse_i64(b"-5"), Some(-5));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        // The full i64 range including i64::MIN (Redis string2ll parity).
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
    }

    #[test]
    fn parse_i64_rejects_leading_plus_and_non_canonical() {
        // Redis string2ll rejects a leading '+'.
        assert_eq!(parse_i64(b"+5"), None);
        assert_eq!(parse_i64(b""), None);
        assert_eq!(parse_i64(b"-"), None);
        assert_eq!(parse_i64(b" 5"), None);
        assert_eq!(parse_i64(b"5 "), None);
        assert_eq!(parse_i64(b"abc"), None);
        assert_eq!(parse_i64(b"1.5"), None);
        // Overflow past the i64 range.
        assert_eq!(parse_i64(b"9223372036854775808"), None);
        assert_eq!(parse_i64(b"-9223372036854775809"), None);
    }

    #[test]
    fn parse_i64_strict_matches_string2ll_canonical_rule() {
        // Accepts the canonical forms plus "-0" (string2ll parses "-0" as 0).
        assert_eq!(parse_i64_strict(b"0"), Some(0));
        assert_eq!(parse_i64_strict(b"-0"), Some(0));
        assert_eq!(parse_i64_strict(b"5"), Some(5));
        assert_eq!(parse_i64_strict(b"-5"), Some(-5));
        assert_eq!(parse_i64_strict(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64_strict(b"-9223372036854775808"), Some(i64::MIN));
        // Rejects leading zeros (the stricter-than-parse_i64 case), plus sign,
        // whitespace, lone '-', floats, non-digits, and overflow.
        assert_eq!(parse_i64_strict(b"007"), None);
        assert_eq!(parse_i64_strict(b"+7"), None);
        assert_eq!(parse_i64_strict(b" 7"), None);
        assert_eq!(parse_i64_strict(b"7 "), None);
        assert_eq!(parse_i64_strict(b"-"), None);
        assert_eq!(parse_i64_strict(b""), None);
        assert_eq!(parse_i64_strict(b"3.14"), None);
        assert_eq!(parse_i64_strict(b"abc"), None);
        assert_eq!(parse_i64_strict(b"9223372036854775808"), None);
        assert_eq!(parse_i64_strict(b"-9223372036854775809"), None);
    }

    #[test]
    fn parse_f64_accepts_valid_floats() {
        assert_eq!(parse_f64(b"10.5"), Some(10.5));
        assert_eq!(parse_f64(b"0"), Some(0.0));
        assert_eq!(parse_f64(b"-2.5"), Some(-2.5));
        assert_eq!(parse_f64(b"5"), Some(5.0));
        assert_eq!(parse_f64(b"3.0e3"), Some(3000.0));
        assert_eq!(parse_f64(b"+1.5"), Some(1.5));
        // Infinity literals parse (the result check rejects an infinite result).
        assert!(parse_f64(b"inf").unwrap().is_infinite());
        assert!(parse_f64(b"-inf").unwrap().is_sign_negative());
    }

    #[test]
    fn parse_f64_rejects_redis_invalid_floats() {
        // Empty, surrounding whitespace, NaN, and non-numeric are rejected.
        assert_eq!(parse_f64(b""), None);
        assert_eq!(parse_f64(b" 1.5"), None);
        assert_eq!(parse_f64(b"1.5 "), None);
        assert_eq!(parse_f64(b"\t1"), None);
        assert_eq!(parse_f64(b"abc"), None);
        assert_eq!(parse_f64(b"nan"), None);
        assert_eq!(parse_f64(b"NaN"), None);
    }
}
