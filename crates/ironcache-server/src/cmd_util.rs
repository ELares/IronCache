// SPDX-License-Identifier: MIT OR Apache-2.0
//! Small shared argument-parsing helpers for the command handlers (the dispatch
//! tier and the new string/keyspace modules). Kept in one place so the command
//! modules do not each re-roll case-folding and integer parsing.

/// Inline capacity of [`UpperToken`]'s stack buffer. Every real RESP command verb,
/// subcommand, and option token is far shorter than this (the longest internal token,
/// `__ICCOUNTKEYSINSLOT`, is 19 bytes), so the inline path covers every hot-path
/// uppercase; only a pathologically long token (never a real command) spills to the heap.
const UPPER_INLINE_CAP: usize = 32;

/// A stack-backed ASCII-uppercased token, the return of [`ascii_upper`].
///
/// The uppercased bytes live INLINE in a fixed `[u8; 32]` for the common short-token case
/// (every real command / subcommand / option token), so uppercasing the per-command token
/// on the dispatch hot path does ZERO heap allocation. A token longer than
/// [`UPPER_INLINE_CAP`] (never a real command) spills to a heap `Vec` fallback.
///
/// Derefs to `&[u8]` and offers [`as_slice`](UpperToken::as_slice), so callers `match` /
/// compare exactly as they did against the previous owned-`Vec<u8>` return.
pub enum UpperToken {
    /// The uppercased bytes held inline on the stack (`buf[..len]`).
    Inline {
        /// Backing storage; only the first `len` bytes are meaningful.
        buf: [u8; UPPER_INLINE_CAP],
        /// Number of valid bytes in `buf`.
        len: usize,
    },
    /// Heap fallback for a token longer than [`UPPER_INLINE_CAP`].
    Heap(Vec<u8>),
}

impl UpperToken {
    /// The uppercased bytes. Use `tok.as_slice()` to `match` against byte-string literals
    /// exactly as against the old `Vec<u8>` return.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            UpperToken::Inline { buf, len } => &buf[..*len],
            UpperToken::Heap(v) => v.as_slice(),
        }
    }
}

impl core::ops::Deref for UpperToken {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq<&[u8]> for UpperToken {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_slice() == *other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for UpperToken {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

/// ASCII-uppercase a byte slice for case-insensitive command/option matching (command and
/// option tokens are ASCII per RESP), WITHOUT a per-command heap allocation.
///
/// The result is a stack-backed [`UpperToken`]: for the common short token (`len <= 32`,
/// i.e. every real command) the uppercased bytes are written into an inline `[u8; 32]` with
/// no allocation; a longer token (never a real command) spills to a heap `Vec`. Uppercasing
/// is byte-identical to `[u8]::to_ascii_uppercase` (ASCII `a..=z` -> `A..=Z`, bytes `>= 0x80`
/// untouched); this is a pure function (ADR-0003: no clock, no entropy).
#[must_use]
pub fn ascii_upper(b: &[u8]) -> UpperToken {
    if b.len() <= UPPER_INLINE_CAP {
        let mut buf = [0u8; UPPER_INLINE_CAP];
        for (dst, &src) in buf.iter_mut().zip(b) {
            *dst = src.to_ascii_uppercase();
        }
        UpperToken::Inline { buf, len: b.len() }
    } else {
        // Pathologically long token (never a real command): fall back to the heap.
        UpperToken::Heap(b.iter().map(u8::to_ascii_uppercase).collect())
    }
}

/// ASCII-lowercase a byte slice into an owned `Vec<u8>` for case-insensitive matching
/// (e.g. the SCAN `TYPE` name, which Redis lowercases against the type vocabulary).
#[must_use]
pub fn ascii_lower(b: &[u8]) -> Vec<u8> {
    b.iter().map(u8::to_ascii_lowercase).collect()
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

/// Parse a ZSET SCORE the way Redis `zslParseRange` parses a range bound's float:
/// `+inf`/`inf`/`+infinity` -> `+f64::INFINITY`, `-inf`/`-infinity` ->
/// `-f64::INFINITY`, otherwise [`parse_f64`]. Used for score-range bounds (after the
/// `(` exclusive prefix is stripped). Returns `None` on a non-float (the caller maps it
/// to the byte-exact `min or max is not a float`). Distinct from [`parse_f64`] only in
/// the explicit `inf` token spellings Redis's range parser accepts.
#[must_use]
pub fn parse_score(arg: &[u8]) -> Option<f64> {
    let lower = ascii_lower(arg);
    match lower.as_slice() {
        b"+inf" | b"inf" | b"+infinity" | b"infinity" => Some(f64::INFINITY),
        b"-inf" | b"-infinity" => Some(f64::NEG_INFINITY),
        _ => parse_f64(arg),
    }
}

/// Parse one end of a ZSET SCORE range (ZRANGEBYSCORE / ZCOUNT / ZRANGE BYSCORE):
/// a leading `(` marks an EXCLUSIVE bound, otherwise the bound is INCLUSIVE; the rest is
/// a score parsed via [`parse_score`] (so `+inf`/`-inf`/`5`/`(5` all parse). Returns
/// `None` on a malformed bound (the caller maps it to `min or max is not a float`).
#[must_use]
pub fn parse_score_bound(arg: &[u8]) -> Option<ironcache_storage::ScoreBound> {
    use ironcache_storage::ScoreBound;
    if let Some(rest) = arg.strip_prefix(b"(") {
        parse_score(rest).map(ScoreBound::exclusive)
    } else {
        parse_score(arg).map(ScoreBound::inclusive)
    }
}

/// Parse one end of a ZSET LEX range (ZRANGEBYLEX / ZLEXCOUNT / ZRANGE BYLEX): `-` is the
/// minimum (before all members), `+` is the maximum (after all members), `[m` is an
/// inclusive bound at the member bytes `m`, `(m` an exclusive bound. Any other form
/// (including a bare member with no `[`/`(`) is invalid. Returns `None` on a malformed
/// bound (the caller maps it to `min or max not valid string range item`).
#[must_use]
pub fn parse_lex_bound(arg: &[u8]) -> Option<ironcache_storage::LexBound> {
    use ironcache_storage::LexBound;
    match arg.first()? {
        b'-' if arg.len() == 1 => Some(LexBound::NegInf),
        b'+' if arg.len() == 1 => Some(LexBound::PosInf),
        b'[' => Some(LexBound::Inclusive(arg[1..].to_vec())),
        b'(' => Some(LexBound::Exclusive(arg[1..].to_vec())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_upper_inline_is_byte_identical_to_std() {
        // The common short-token path (inline stack buffer, zero heap alloc) must match
        // `[u8]::to_ascii_uppercase` byte for byte: ASCII a-z -> A-Z, everything else
        // (digits, punctuation, bytes >= 0x80) untouched.
        for token in [
            &b""[..],
            b"GET",
            b"set",
            b"HsEt",
            b"WITHSCORES",
            b"__ICCOUNTKEYSINSLOT",
            b"Key-With_Digits123",
            b"\x00\x80\xffAbZ",
        ] {
            let up = ascii_upper(token);
            assert!(matches!(up, UpperToken::Inline { .. }));
            assert_eq!(up.as_slice(), token.to_ascii_uppercase().as_slice());
        }
        // A 32-byte token is the largest that still stays inline (boundary).
        let at_cap = vec![b'a'; UPPER_INLINE_CAP];
        let up = ascii_upper(&at_cap);
        assert!(matches!(up, UpperToken::Inline { .. }));
        assert_eq!(up.as_slice(), at_cap.to_ascii_uppercase().as_slice());
    }

    #[test]
    fn ascii_upper_long_token_uses_heap_fallback() {
        // A token longer than the inline capacity (never a real command) spills to the heap
        // and still uppercases byte-identically.
        let long = vec![b'z'; UPPER_INLINE_CAP + 1];
        let up = ascii_upper(&long);
        assert!(matches!(up, UpperToken::Heap(_)));
        assert_eq!(up.as_slice(), vec![b'Z'; UPPER_INLINE_CAP + 1].as_slice());

        // A much longer mixed token also matches std uppercasing exactly.
        let mixed: Vec<u8> = (0u16..300).map(|n| (n % 256) as u8).collect();
        let up = ascii_upper(&mixed);
        assert!(matches!(up, UpperToken::Heap(_)));
        assert_eq!(up.as_slice(), mixed.to_ascii_uppercase().as_slice());
    }

    #[test]
    fn upper_token_compares_like_the_old_vec() {
        // Deref + the PartialEq impls keep the call sites' `match`/`==` working unchanged.
        let tok = ascii_upper(b"count");
        assert_eq!(tok.as_slice(), b"COUNT");
        assert!(tok == b"COUNT"); // PartialEq<&[u8; N]>
        let as_bytes: &[u8] = b"COUNT";
        assert!(tok == as_bytes); // PartialEq<&[u8]>
        assert!(tok.eq_ignore_ascii_case(b"count")); // via Deref to [u8]
        assert_eq!(tok.len(), 5); // via Deref
    }

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
