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
//! ## Linear, stack-safe matching (NOT recursive)
//!
//! Redis's reference `stringmatchlen` RECURSES on `*`, which is exponential-time and
//! stack-deep on an adversarial `*a*a*a*...` pattern (a `KEYS`/`SCAN MATCH` DoS, #614).
//! This matcher instead uses the standard two-pointer greedy-with-backtrack: it walks
//! the string once, and on a mismatch it backtracks to the LAST `*` and lets that `*`
//! swallow one more string byte. That is O(1) stack (no recursion) and O(n*m) worst
//! case (NEVER exponential), while producing BYTE-IDENTICAL match/no-match results to
//! Redis's algorithm for the whole glob language. A single-string-byte pattern element
//! (`?`, a `[...]` class, a `\` escape, or a literal) is decoded by `match_one`, which
//! preserves Redis's exact per-byte class semantics (negation, ranges incl. the `z-a`
//! endpoint swap, a `]` consumed as a range endpoint, an unterminated class running to
//! end-of-pattern, and a trailing lone `\` treated as a literal backslash).

/// Whether `pattern` matches the whole of `string`, byte for byte, under Redis glob
/// rules (`stringmatchlen` with `nocase = 0`). Binary-safe.
///
/// This mirrors Redis `stringmatchlen` exactly, including: an empty pattern matches
/// only the empty string; a trailing `\` (escape with nothing after) matches a
/// literal backslash region per Redis's fall-through; an unterminated `[` is treated
/// as the rest of the class running to end-of-pattern.
#[must_use]
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let p = pattern;
    let s = string;
    let mut p_idx = 0usize;
    let mut s_idx = 0usize;
    // The last `*` we passed and the string position when we took it. On a mismatch we
    // return here and let that `*` swallow one more string byte (greedy-with-backtrack).
    // NO recursion: this is O(1) stack and O(n*m) worst case, never exponential.
    let mut star_p: Option<usize> = None;
    let mut star_s = 0usize;

    while s_idx < s.len() {
        if p_idx < p.len() && p[p_idx] == b'*' {
            // Record this `*` (consecutive stars collapse automatically: the next one
            // re-records at the same s_idx) and advance past it, matching zero bytes so
            // far; the backtrack below extends what it swallows as needed.
            star_p = Some(p_idx);
            star_s = s_idx;
            p_idx += 1;
            continue;
        }
        if p_idx < p.len() {
            let (matched, consumed) = match_one(&p[p_idx..], s[s_idx]);
            if matched {
                p_idx += consumed;
                s_idx += 1;
                continue;
            }
        }
        // No `?`/class/literal match at (p_idx, s_idx): backtrack to the last `*` and let
        // it consume one more byte. With no prior `*`, the match fails.
        if let Some(sp) = star_p {
            p_idx = sp + 1;
            star_s += 1;
            s_idx = star_s;
        } else {
            return false;
        }
    }

    // The string is exhausted: any pattern left must be all `*` to still match.
    while p_idx < p.len() && p[p_idx] == b'*' {
        p_idx += 1;
    }
    p_idx == p.len()
}

/// Match a SINGLE non-`*` pattern element at the start of `p` against one string byte
/// `sb`, returning `(matched, pattern_bytes_consumed)`. `p` is non-empty and `p[0]` is
/// not `*`. This reproduces Redis's `stringmatchlen` per-byte semantics EXACTLY for
/// `?`, a `[...]` class (negation, `z-a` range swap, `]` consumed as a range endpoint,
/// an unterminated class running to end-of-pattern), a `\` escape, and a literal (a
/// trailing lone `\`, with nothing after, is a literal backslash).
fn match_one(p: &[u8], sb: u8) -> (bool, usize) {
    match p[0] {
        b'?' => (true, 1),
        b'[' => {
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
                    if p[idx] == sb {
                        matched = true;
                    }
                    idx += 1;
                } else if idx + 2 < p.len() && p[idx + 1] == b'-' {
                    // A range `a-z`; Redis orders the endpoints so `z-a` works too, and
                    // takes this branch whenever `p[idx+1] == '-'` with no guard that
                    // `p[idx+2] != ']'` (so `]` can be a range endpoint).
                    let (mut lo, mut hi) = (p[idx], p[idx + 2]);
                    if lo > hi {
                        core::mem::swap(&mut lo, &mut hi);
                    }
                    if sb >= lo && sb <= hi {
                        matched = true;
                    }
                    idx += 3;
                } else {
                    // A plain class member.
                    if p[idx] == sb {
                        matched = true;
                    }
                    idx += 1;
                }
            }
            // Advance past the closing ']' (if present).
            if idx < p.len() {
                idx += 1;
            }
            if negate {
                matched = !matched;
            }
            (matched, idx)
        }
        b'\\' if p.len() >= 2 => (sb == p[1], 2),
        c => (sb == c, 1),
    }
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

    #[test]
    fn adversarial_patterns_terminate_in_bounded_time() {
        // #614: the old RECURSIVE matcher went exponential + stack-deep on a `*a*a*...`
        // pattern (a KEYS/SCAN MATCH DoS). The two-pointer matcher is O(1) stack and
        // O(n*m), so these all return near-instantly with the CORRECT result.

        // A long run of consecutive `*` collapses: matches anything; `*...*b` iff ends in b.
        let many_stars = vec![b'*'; 4096];
        assert!(glob_match(&many_stars, b"anything at all"));
        assert!(glob_match(&many_stars, b""));
        let mut stars_then_b = many_stars.clone();
        stars_then_b.push(b'b');
        assert!(glob_match(&stars_then_b, b"aaaaaaaaaaaab"));
        assert!(!glob_match(&stars_then_b, b"aaaaaaaaaaaac"));

        // The classic backtracking bomb: `(*a) x N`. The non-matching case (string ends in
        // a non-`a`) is what exploded the recursive matcher; here it terminates in O(N*M).
        let mut alternating = Vec::new();
        for _ in 0..512 {
            alternating.push(b'*');
            alternating.push(b'a');
        }
        let all_a = vec![b'a'; 512];
        assert!(glob_match(&alternating, &all_a));
        let mut a_then_b = vec![b'a'; 512];
        *a_then_b.last_mut().unwrap() = b'b';
        assert!(!glob_match(&alternating, &a_then_b));

        // A many-star pattern with literals between: `(*x) x N` vs a long non-matching run
        // (would have recursed N deep -> stack overflow).
        let mut star_x = Vec::new();
        for _ in 0..4096 {
            star_x.push(b'*');
            star_x.push(b'x');
        }
        assert!(!glob_match(&star_x, &vec![b'y'; 4096]));

        // A huge unterminated class + an escaped literal `*` still behave correctly.
        let mut big_class = vec![b'['];
        big_class.resize(big_class.len() + 4096, b'q');
        big_class.push(b'y');
        assert!(glob_match(&big_class, b"y"));
        assert!(!glob_match(&big_class, b"z"));
        assert!(glob_match(b"a\\*b", b"a*b"));
        assert!(!glob_match(b"a\\*b", b"axb"));
    }

    /// A small, obviously-correct exponential reference matcher (the old recursive form),
    /// used ONLY in the equivalence test on inputs small enough that it cannot blow up.
    fn glob_match_reference(p: &[u8], s: &[u8]) -> bool {
        if p.is_empty() {
            return s.is_empty();
        }
        if p[0] == b'*' {
            let mut rest = p;
            while rest.len() >= 2 && rest[1] == b'*' {
                rest = &rest[1..];
            }
            let rest = &rest[1..];
            if rest.is_empty() {
                return true;
            }
            for i in 0..=s.len() {
                if glob_match_reference(rest, &s[i..]) {
                    return true;
                }
            }
            false
        } else {
            if s.is_empty() {
                return false;
            }
            let (m, consumed) = super::match_one(p, s[0]);
            m && glob_match_reference(&p[consumed..], &s[1..])
        }
    }

    #[test]
    fn linear_matcher_agrees_with_the_reference_on_random_inputs() {
        // Deterministic xorshift (no rand dep; ADR-0003 fine in a test) drives thousands of
        // small pattern/string pairs over a tiny metacharacter-heavy alphabet, asserting the
        // two-pointer matcher agrees byte-for-byte with the exponential reference. The small
        // sizes keep the reference bounded while covering the whole glob language.
        let alphabet = b"ab*?[]^-\\";
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let plen = (next() % 8) as usize;
            let slen = (next() % 8) as usize;
            let pat: Vec<u8> = (0..plen)
                .map(|_| alphabet[(next() as usize) % alphabet.len()])
                .collect();
            let st: Vec<u8> = (0..slen)
                .map(|_| alphabet[(next() as usize) % alphabet.len()])
                .collect();
            assert_eq!(
                glob_match(&pat, &st),
                glob_match_reference(&pat, &st),
                "disagreement on pattern={pat:?} string={st:?}"
            );
        }
    }
}
