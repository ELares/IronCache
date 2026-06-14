// SPDX-License-Identifier: MIT OR Apache-2.0
//! Binary-safe glob matcher for KEYS and SCAN MATCH (COMMANDS.md "glob matching").
//!
//! A direct port of Redis's `stringmatchlen` (src/util.c): the pattern language is
//! `*` (any byte sequence), `?` (exactly one byte), `[...]` a byte class, `[^...]`
//! a negated class, `[a-z]` a range inside a class, and `\` escaping the next
//! pattern byte literally. It is BINARY-SAFE (operates on `&[u8]`, not `&str`), so a
//! non-UTF-8 key or pattern matches by raw bytes, exactly like Redis.
//!
//! ## Why a hand port rather than a regex crate
//!
//! The semantics must be byte-identical to Redis's `stringmatchlen` (the conformance
//! oracle compares KEYS/SCAN MATCH results against it), and that function has
//! specific edge behaviors (an unterminated `[`, an empty pattern, `*` collapsing,
//! a `\` at end-of-pattern) that a general regex engine would not reproduce. A small
//! direct port keeps it exact, dependency-free, and deterministic.
//!
//! ## Recursion and the `*` fast path
//!
//! Redis's reference implementation recurses on `*`; this port uses the SAME
//! iterative `*`-skip Redis added to bound recursion (collapse consecutive `*`, then
//! try to match the remaining pattern at each suffix of the string). The collapse of
//! consecutive `*` is what keeps `a***b` linear rather than exponential.

/// Whether `pattern` matches the whole of `string`, byte for byte, under Redis glob
/// rules (`stringmatchlen` with `nocase = 0`). Binary-safe.
///
/// This mirrors Redis `stringmatchlen` exactly, including: an empty pattern matches
/// only the empty string; a trailing `\` (escape with nothing after) matches a
/// literal backslash region per Redis's fall-through; an unterminated `[` is treated
/// as the rest of the class running to end-of-pattern.
#[must_use]
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut p = pattern;
    let mut s = string;

    while !p.is_empty() {
        match p[0] {
            b'*' => {
                // Collapse consecutive '*' so `a***b` is linear (Redis: skip stars).
                while p.len() >= 2 && p[1] == b'*' {
                    p = &p[1..];
                }
                // A trailing '*' matches the rest of the string.
                if p.len() == 1 {
                    return true;
                }
                // Try to match the remaining pattern (after the '*') against every
                // suffix of the string, shortest first (Redis's iterative form).
                let rest = &p[1..];
                let mut i = 0;
                loop {
                    if glob_match(rest, &s[i..]) {
                        return true;
                    }
                    if i >= s.len() {
                        break;
                    }
                    i += 1;
                }
                return false;
            }
            b'?' => {
                // '?' consumes exactly one byte; fails if the string is exhausted.
                if s.is_empty() {
                    return false;
                }
                s = &s[1..];
                p = &p[1..];
            }
            b'[' => {
                if s.is_empty() {
                    return false;
                }
                // Walk the class body. A leading '^' negates. A '\' escapes the next
                // byte. A '-' between two bytes is a range. An unterminated class runs
                // to end-of-pattern (Redis falls out of the loop at p-end).
                let mut idx = 1; // past '['
                let negate = idx < p.len() && p[idx] == b'^';
                if negate {
                    idx += 1;
                }
                let mut matched = false;
                while idx < p.len() && p[idx] != b']' {
                    if p[idx] == b'\\' && idx + 1 < p.len() {
                        // Escaped class member: the next byte literally.
                        idx += 1;
                        if p[idx] == s[0] {
                            matched = true;
                        }
                        idx += 1;
                    } else if idx + 2 < p.len() && p[idx + 1] == b'-' {
                        // A range `a-z`. Redis orders the endpoints so `z-a` works too.
                        // stringmatchlen takes this branch whenever `pattern[1] == '-'`
                        // with NO guard that pattern[2] != ']', so `]` is consumed as a
                        // range endpoint too (e.g. `[a-]` is the range ']'..'a' after the
                        // swap below, since 'a' > ']').
                        let (mut lo, mut hi) = (p[idx], p[idx + 2]);
                        if lo > hi {
                            core::mem::swap(&mut lo, &mut hi);
                        }
                        if s[0] >= lo && s[0] <= hi {
                            matched = true;
                        }
                        idx += 3;
                    } else {
                        // A plain class member.
                        if p[idx] == s[0] {
                            matched = true;
                        }
                        idx += 1;
                    }
                }
                // Advance the pattern past the closing ']' (if present).
                if idx < p.len() {
                    idx += 1; // consume ']'
                }
                if negate {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                s = &s[1..];
                p = &p[idx..];
            }
            b'\\' if p.len() >= 2 => {
                // An escape: the next pattern byte must match the string byte literally.
                if s.is_empty() || s[0] != p[1] {
                    return false;
                }
                s = &s[1..];
                p = &p[2..];
            }
            c => {
                // A literal byte (this also covers a trailing lone '\\', which Redis
                // treats as a literal backslash when nothing follows it).
                if s.is_empty() || s[0] != c {
                    return false;
                }
                s = &s[1..];
                p = &p[1..];
            }
        }

        // Redis's loop also stops early once the string is exhausted; remaining
        // pattern is handled by the trailing-'*' collapse below.
        if s.is_empty() {
            // Any remaining pattern must be all '*' to still match (Redis collapses
            // trailing stars to the empty match).
            while !p.is_empty() && p[0] == b'*' {
                p = &p[1..];
            }
            break;
        }
    }

    // A match iff both the pattern and the string are fully consumed.
    p.is_empty() && s.is_empty()
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    #[test]
    fn literal_and_empty() {
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"x"));
        assert!(glob_match(b"abc", b"abc"));
        assert!(!glob_match(b"abc", b"abd"));
        assert!(!glob_match(b"abc", b"ab"));
        assert!(!glob_match(b"ab", b"abc"));
    }

    #[test]
    fn star_leading_trailing_collapsed_and_middle() {
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"a*", b"a"));
        assert!(glob_match(b"a*", b"abc"));
        assert!(glob_match(b"*c", b"abc"));
        assert!(glob_match(b"a*c", b"abc"));
        assert!(glob_match(b"a*c", b"ac"));
        assert!(!glob_match(b"a*c", b"abd"));
        // Collapsed consecutive stars stay linear and still match.
        assert!(glob_match(b"a***b", b"axxxxb"));
        assert!(glob_match(b"***", b"hello"));
        assert!(glob_match(b"h*l*o", b"hello"));
        assert!(!glob_match(b"h*x", b"hello"));
    }

    #[test]
    fn question_mark_one_byte() {
        assert!(glob_match(b"?", b"a"));
        assert!(!glob_match(b"?", b""));
        assert!(!glob_match(b"?", b"ab"));
        assert!(glob_match(b"a?c", b"abc"));
        assert!(!glob_match(b"a?c", b"ac"));
        assert!(glob_match(b"h?llo", b"hello"));
    }

    #[test]
    fn char_class_basic_and_range() {
        assert!(glob_match(b"[abc]", b"a"));
        assert!(glob_match(b"[abc]", b"c"));
        assert!(!glob_match(b"[abc]", b"d"));
        assert!(glob_match(b"[a-z]", b"m"));
        assert!(!glob_match(b"[a-z]", b"M"));
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
    }

    #[test]
    fn negated_class() {
        assert!(glob_match(b"[^abc]", b"d"));
        assert!(!glob_match(b"[^abc]", b"a"));
        assert!(glob_match(b"h[^x]llo", b"hello"));
        assert!(!glob_match(b"h[^e]llo", b"hello"));
    }

    #[test]
    fn inverted_range_is_normalized() {
        // Redis orders the endpoints, so `[z-a]` behaves like `[a-z]`.
        assert!(glob_match(b"[z-a]", b"m"));
        assert!(!glob_match(b"[z-a]", b"M"));
    }

    #[test]
    fn range_consumes_closing_bracket_as_endpoint() {
        // stringmatchlen takes the range branch whenever `pattern[1] == '-'` with NO
        // guard that `pattern[2] != ']'`, so `[a-]` is the range ']'..'a' (endpoints
        // swapped since 'a' > ']'). It matches `]` and `_` (both within ']'..='a') and
        // does NOT match `-` (0x2D, below ']' 0x5D).
        assert!(glob_match(b"[a-]", b"]"));
        assert!(glob_match(b"[a-]", b"_"));
        assert!(!glob_match(b"[a-]", b"-"));
    }

    #[test]
    fn escaped_metacharacters() {
        // A backslash escapes the next pattern byte to a literal.
        assert!(glob_match(b"\\*", b"*"));
        assert!(!glob_match(b"\\*", b"a"));
        assert!(glob_match(b"\\?", b"?"));
        assert!(glob_match(b"\\[", b"["));
        assert!(glob_match(b"a\\*b", b"a*b"));
        assert!(!glob_match(b"a\\*b", b"axb"));
        // An escape inside a class.
        assert!(glob_match(b"[\\]]", b"]"));
    }

    #[test]
    fn unterminated_class_runs_to_pattern_end() {
        // An unterminated '[' consumes the rest as the class body (Redis fall-through):
        // `[abc` matches a single byte in {a,b,c}.
        assert!(glob_match(b"[abc", b"a"));
        assert!(!glob_match(b"[abc", b"d"));
    }

    #[test]
    fn trailing_lone_backslash_is_literal() {
        // A lone trailing '\\' (escape with nothing after) is a literal backslash.
        assert!(glob_match(b"a\\", b"a\\"));
        assert!(!glob_match(b"a\\", b"a"));
    }

    #[test]
    fn binary_safe_non_utf8_bytes() {
        // The matcher is byte-based; non-UTF-8 bytes match by raw value.
        assert!(glob_match(&[0xFF, b'*'], &[0xFF, 0x00, 0x01]));
        assert!(glob_match(b"?", &[0x80]));
        assert!(!glob_match(&[0xFE], &[0xFF]));
    }

    #[test]
    fn star_then_class() {
        assert!(glob_match(b"key:*", b"key:1"));
        assert!(glob_match(b"key:[0-9]", b"key:7"));
        assert!(!glob_match(b"key:[0-9]", b"key:x"));
        assert!(glob_match(b"*:[0-9]*", b"key:7abc"));
    }
}
