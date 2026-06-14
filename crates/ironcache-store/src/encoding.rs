// SPDX-License-Identifier: MIT OR Apache-2.0
//! String value encoding classification (ENCODINGS.md #112, ADR-0009/ADR-0018).
//!
//! A string value is classified into one of three encodings, matching what
//! `OBJECT ENCODING` reports (even though the OBJECT command itself is a later PR):
//!
//! - `int`: the bytes are a canonical i64 (fits i64, round-trips with no leading
//!   zero, no `+`, no surrounding whitespace). Stored as the raw integer with NO
//!   value allocation (ENCODINGS.md "pointer-tagged small integers", realized here
//!   as an inline [`crate::kvobj::ValueRepr::Int`]).
//! - `embstr`: a short string (`<= EMBSTR_THRESHOLD` bytes) stored inline in the
//!   object (ENCODINGS.md "inline short strings (SSO)", generalizing Redis's 44
//!   -byte embstr threshold [redis-embstr-threshold-44]).
//! - `raw`: a longer string stored out-of-line.
//!
//! The threshold is documented as an #8-tunable; ENCODINGS.md pins it to Redis's
//! 44-byte embstr threshold for behavioral parity until the memory harness retunes
//! it. The no-shared-int-pool decision (ENCODINGS.md "Rejected: the shared-integer
//! pool") means an integer is just a stored i64; there is nothing to dedupe.

use ironcache_storage::Encoding;

/// The inline (embstr) threshold in bytes (ENCODINGS.md, #8-tunable). A string
/// value at or below this length is stored inline (`embstr`); above it is `raw`.
/// Pinned to Redis's 44-byte embstr threshold for parity.
pub const EMBSTR_THRESHOLD: usize = 44;

/// The classification of a candidate string value, with the parsed integer when
/// the bytes are a canonical i64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Classified {
    /// Canonical i64: store with no value allocation. `OBJECT ENCODING` -> int.
    Int(i64),
    /// Short string: store inline. `OBJECT ENCODING` -> embstr.
    EmbStr,
    /// Long string: store out-of-line. `OBJECT ENCODING` -> raw.
    Raw,
}

impl Classified {
    /// The [`Encoding`] this classification maps to.
    #[must_use]
    pub const fn encoding(self) -> Encoding {
        match self {
            Classified::Int(_) => Encoding::Int,
            Classified::EmbStr => Encoding::EmbStr,
            Classified::Raw => Encoding::Raw,
        }
    }
}

/// Classify raw value bytes into int/embstr/raw (ENCODINGS.md).
///
/// Integer classification is canonical: the bytes must be exactly what
/// formatting the i64 back to decimal produces, so `"007"`, `"+7"`, `" 7"`,
/// `"7 "`, `"-0"`, and the empty string are NOT ints (they are embstr/raw),
/// matching Redis's `string2ll` canonical-form rule.
#[must_use]
pub fn classify(bytes: &[u8]) -> Classified {
    if let Some(n) = parse_canonical_i64(bytes) {
        return Classified::Int(n);
    }
    if bytes.len() <= EMBSTR_THRESHOLD {
        Classified::EmbStr
    } else {
        Classified::Raw
    }
}

/// Parse `bytes` as a canonical i64, or `None` if it is not the canonical decimal
/// form of an i64. Canonical means: optional single leading `-` (never `+`), then
/// one or more ASCII digits, with no leading zero (except the single digit `"0"`),
/// no whitespace, no other characters, and `"-0"` rejected. This is exactly the
/// set of byte strings that round-trip through `i64::to_string`, so an int-encoded
/// value's decimal materialization equals its original bytes.
#[must_use]
pub fn parse_canonical_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    if digits.is_empty() {
        return None;
    }
    // No leading zeros except the lone "0". "-0" is not canonical.
    if digits[0] == b'0' {
        if digits.len() != 1 {
            return None;
        }
        // "0" is canonical; "-0" is not.
        return if neg { None } else { Some(0) };
    }
    let mut acc: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(i64::from(b - b'0'))?;
    }
    if neg {
        // acc is positive so far; negate. (i64::MIN's magnitude does not fit in a
        // positive i64, so "-9223372036854775808" would have overflowed the
        // checked_add above and returned None; that input is therefore classified
        // as a string, which is acceptable and matches no canonical positive acc.)
        Some(-acc)
    } else {
        Some(acc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_integers_are_int_encoded() {
        assert_eq!(classify(b"0"), Classified::Int(0));
        assert_eq!(classify(b"12345"), Classified::Int(12345));
        assert_eq!(classify(b"-1"), Classified::Int(-1));
        assert_eq!(classify(b"9223372036854775807"), Classified::Int(i64::MAX));
    }

    #[test]
    fn non_canonical_numbers_are_not_int() {
        // Leading zero, plus sign, whitespace, "-0", overflow, float, empty.
        assert_eq!(classify(b"007"), Classified::EmbStr);
        assert_eq!(classify(b"+7"), Classified::EmbStr);
        assert_eq!(classify(b" 7"), Classified::EmbStr);
        assert_eq!(classify(b"7 "), Classified::EmbStr);
        assert_eq!(classify(b"-0"), Classified::EmbStr);
        assert_eq!(classify(b"3.14"), Classified::EmbStr);
        // i64::MIN magnitude does not fit a positive accumulator -> not int.
        assert_eq!(classify(b"-9223372036854775808"), Classified::EmbStr);
        // Over i64::MAX -> not int (and long enough? no, 19 digits <= 44 -> embstr).
        assert_eq!(classify(b"99999999999999999999"), Classified::EmbStr);
        // Empty string is the empty embstr, never int.
        assert_eq!(classify(b""), Classified::EmbStr);
    }

    #[test]
    fn embstr_vs_raw_threshold() {
        let at = vec![b'x'; EMBSTR_THRESHOLD];
        let over = vec![b'x'; EMBSTR_THRESHOLD + 1];
        assert_eq!(classify(&at), Classified::EmbStr);
        assert_eq!(classify(&over), Classified::Raw);
        // A 10-byte string is embstr; a 100-byte string is raw.
        assert_eq!(classify(&[b'a'; 10]), Classified::EmbStr);
        assert_eq!(classify(&[b'a'; 100]), Classified::Raw);
    }

    #[test]
    fn classification_maps_to_encoding_names() {
        assert_eq!(classify(b"42").encoding(), Encoding::Int);
        assert_eq!(classify(b"short").encoding(), Encoding::EmbStr);
        assert_eq!(classify(&[b'z'; 100]).encoding(), Encoding::Raw);
    }

    #[test]
    fn parse_canonical_round_trips_to_string() {
        // Every classified int must equal its own decimal string (the property
        // that lets an int-encoded value materialize bytes equal to the original).
        for n in [0i64, 1, -1, 12345, -67890, i64::MAX] {
            let s = n.to_string();
            assert_eq!(parse_canonical_i64(s.as_bytes()), Some(n), "{s}");
        }
    }
}
