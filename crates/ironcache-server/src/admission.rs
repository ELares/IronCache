// SPDX-License-Identifier: MIT OR Apache-2.0
//! maxmemory admission: the Redis `denyoom` command classification (ADMISSION.md
//! #128, ADR-0007).
//!
//! Redis tags each command with a `denyoom` flag and, in `processCommand`, runs the
//! eviction/`-OOM` decision BEFORE the command body for a `denyoom` command when the
//! server is over `maxmemory`. IronCache mirrors this ABOVE the storage waist: the
//! dispatch layer asks [`is_denyoom`] whether the incoming command may allocate, and
//! if so enforces the ceiling (evict-to-fit in cache mode, reply `-OOM` in
//! datastore/noeviction). Read-only and memory-RELEASING commands (`GET`, `DEL`,
//! `TTL`, ...) are never `denyoom`, so they are served even over the budget (a client
//! must be able to read and free under memory pressure).

/// Whether `cmd` (the UPPERCASED command token) is a `denyoom` write that the memory
/// ceiling gates (ADMISSION.md). `true` for the string write/RMW commands that can
/// grow memory; `false` for reads, the EXISTS/TYPE/STRLEN introspection, the
/// memory-RELEASING `DEL`, the Tier-0 connection commands, and the EXPIRE/TTL/PERSIST
/// family that 3b will add (those do not allocate value bytes).
///
/// This mirrors Redis's `CMD_DENYOOM` flag for the commands IronCache implements
/// today. As collection writes (LPUSH/HSET/SADD/...) land they JOIN this set; the
/// list is the single source of the classification so a new write cannot silently
/// bypass the ceiling.
#[must_use]
pub fn is_denyoom(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"SET"
            | b"SETNX"
            | b"GETSET"
            | b"APPEND"
            | b"INCR"
            | b"DECR"
            | b"INCRBY"
            | b"DECRBY"
            | b"INCRBYFLOAT"
            // SETEX/PSETEX are denyoom writes (they allocate a value), so they are
            // pre-classified here NOW even though their dispatch arms land in 3b. This
            // ordering is deliberate: until 3b wires them, an over-budget SETEX/PSETEX
            // is OOM'd by this gate BEFORE falling through to the unknown-command reply
            // (OOM-before-unknown), matching Redis (the denyoom check precedes command
            // lookup); the classification is the single source so the 3b arm cannot
            // silently bypass the ceiling.
            | b"SETEX"
            | b"PSETEX"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denyoom_set_is_the_write_family() {
        for w in [
            b"SET".as_slice(),
            b"SETNX",
            b"GETSET",
            b"APPEND",
            b"INCR",
            b"DECR",
            b"INCRBY",
            b"DECRBY",
            b"INCRBYFLOAT",
            b"SETEX",
            b"PSETEX",
        ] {
            assert!(is_denyoom(w), "{w:?} should be denyoom");
        }
    }

    #[test]
    fn reads_releases_and_tier0_are_not_denyoom() {
        for r in [
            // reads / introspection
            b"GET".as_slice(),
            b"STRLEN",
            b"EXISTS",
            b"TYPE",
            // memory-releasing
            b"DEL",
            // Tier-0 / connection
            b"INFO",
            b"PING",
            b"HELLO",
            b"SELECT",
            b"CONFIG",
            // the EXPIRE/TTL/PERSIST family 3b will add (no value allocation)
            b"EXPIRE",
            b"TTL",
            b"PTTL",
            b"PERSIST",
            b"EXPIREAT",
        ] {
            assert!(!is_denyoom(r), "{r:?} must not be denyoom");
        }
    }

    #[test]
    fn classification_is_case_sensitive_on_the_uppercased_token() {
        // The caller uppercases the token before classifying (RESP commands are
        // ASCII); a lowercase token here is a caller bug, so it classifies as
        // non-denyoom rather than matching.
        assert!(!is_denyoom(b"set"));
        assert!(is_denyoom(b"SET"));
    }
}
