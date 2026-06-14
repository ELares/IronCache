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

/// Parse a base-10 i64 from an argument, returning `None` on any non-digit or
/// overflow (the caller maps `None` to the appropriate error).
#[must_use]
pub fn parse_i64(arg: &[u8]) -> Option<i64> {
    core::str::from_utf8(arg).ok()?.parse::<i64>().ok()
}
