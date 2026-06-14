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
}
