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
            // RENAME/RENAMENX/COPY are `denyoom` writes in Redis (they materialize a
            // value at the destination). MOVE is NOT denyoom (Redis flags it `write
            // fast` without denyoom, since it relocates rather than duplicates), so it
            // is intentionally absent. SWAPDB/FLUSHDB/FLUSHALL/TOUCH/UNLINK/SCAN/KEYS/
            // DBSIZE/RANDOMKEY/OBJECT do not allocate value bytes, so they are not
            // gated either (FLUSH* and UNLINK/DEL are memory-RELEASING).
            | b"RENAME"
            | b"RENAMENX"
            | b"COPY"
            // List writes that allocate value bytes (PR-5). LPUSH/RPUSH/LPUSHX/RPUSHX
            // grow the list; LSET/LINSERT add/replace an element; LMOVE/RPOPLPUSH
            // materialize an element at the destination. All are `denyoom` in Redis.
            // LPOP/RPOP/LREM/LTRIM and the read commands (LLEN/LRANGE/LINDEX/LPOS) are
            // memory-RELEASING or read-only, so they are NOT gated.
            | b"LPUSH"
            | b"RPUSH"
            | b"LPUSHX"
            | b"RPUSHX"
            | b"LSET"
            | b"LINSERT"
            | b"LMOVE"
            | b"RPOPLPUSH"
            // Hash writes that allocate value bytes (PR-6). HSET/HMSET/HSETNX add or
            // replace fields; HINCRBY/HINCRBYFLOAT create-on-missing and grow a field's
            // value. All are `denyoom` in Redis. HDEL is memory-RELEASING and the hash
            // reads (HGET/HMGET/HGETALL/HKEYS/HVALS/HLEN/HEXISTS/HSTRLEN/HRANDFIELD/HSCAN)
            // are read-only, so they are NOT gated.
            | b"HSET"
            | b"HMSET"
            | b"HSETNX"
            | b"HINCRBY"
            | b"HINCRBYFLOAT"
            // Set writes that allocate value bytes (PR-7). SADD grows the set;
            // SINTERSTORE/SUNIONSTORE/SDIFFSTORE materialize the result set at the
            // destination. All are `denyoom` in Redis. SMOVE is NOT denyoom (Redis flags it
            // `write fast` without denyoom, like MOVE: it RELOCATES an existing member from
            // src to dst, materializing no new value bytes), so it is intentionally absent.
            // SREM/SPOP are memory-RELEASING and the set reads / algebra reads
            // (SMEMBERS/SISMEMBER/SMISMEMBER/SCARD/SRANDMEMBER/SINTER/SUNION/SDIFF/
            // SINTERCARD/SSCAN) are read-only, so they are NOT gated.
            | b"SADD"
            | b"SINTERSTORE"
            | b"SUNIONSTORE"
            | b"SDIFFSTORE"
            // Sorted-set writes that allocate value bytes (PR-8). ZADD/ZINCRBY grow the
            // zset (or create-on-missing); ZRANGESTORE and ZUNIONSTORE/ZINTERSTORE/
            // ZDIFFSTORE materialize a result zset at the destination. All are `denyoom`
            // in Redis. ZREM/ZPOPMIN/ZPOPMAX/ZREMRANGE* are memory-RELEASING and the zset
            // reads (ZSCORE/ZMSCORE/ZCARD/ZRANK/ZREVRANK/ZCOUNT/ZLEXCOUNT/ZRANGE*/
            // ZRANDMEMBER/ZSCAN/ZUNION/ZINTER/ZDIFF/ZINTERCARD) are read-only, so they are
            // NOT gated.
            | b"ZADD"
            | b"ZINCRBY"
            | b"ZRANGESTORE"
            | b"ZUNIONSTORE"
            | b"ZINTERSTORE"
            | b"ZDIFFSTORE"
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
            b"RENAME",
            b"RENAMENX",
            b"COPY",
            // List writes (PR-5).
            b"LPUSH",
            b"RPUSH",
            b"LPUSHX",
            b"RPUSHX",
            b"LSET",
            b"LINSERT",
            b"LMOVE",
            b"RPOPLPUSH",
            // Hash writes (PR-6).
            b"HSET",
            b"HMSET",
            b"HSETNX",
            b"HINCRBY",
            b"HINCRBYFLOAT",
            // Set writes (PR-7).
            b"SADD",
            b"SINTERSTORE",
            b"SUNIONSTORE",
            b"SDIFFSTORE",
            // Sorted-set writes (PR-8).
            b"ZADD",
            b"ZINCRBY",
            b"ZRANGESTORE",
            b"ZUNIONSTORE",
            b"ZINTERSTORE",
            b"ZDIFFSTORE",
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
            b"UNLINK",
            b"FLUSHDB",
            b"FLUSHALL",
            // generic keyspace reads / non-allocating (PR-4a)
            b"KEYS",
            b"SCAN",
            b"DBSIZE",
            b"RANDOMKEY",
            b"TOUCH",
            b"OBJECT",
            // MOVE relocates rather than duplicates (write fast, not denyoom in Redis)
            b"MOVE",
            b"SWAPDB",
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
            // List reads + memory-releasing list writes (PR-5): not denyoom.
            b"LPOP",
            b"RPOP",
            b"LREM",
            b"LTRIM",
            b"LLEN",
            b"LRANGE",
            b"LINDEX",
            b"LPOS",
            // Hash reads + memory-releasing HDEL (PR-6): not denyoom.
            b"HGET",
            b"HMGET",
            b"HGETALL",
            b"HKEYS",
            b"HVALS",
            b"HLEN",
            b"HEXISTS",
            b"HSTRLEN",
            b"HRANDFIELD",
            b"HSCAN",
            b"HDEL",
            // Set reads + memory-releasing SREM/SPOP (PR-7): not denyoom. SMOVE RELOCATES
            // an existing member (write fast, not denyoom in Redis, like MOVE).
            b"SREM",
            b"SPOP",
            b"SMOVE",
            b"SMEMBERS",
            b"SISMEMBER",
            b"SMISMEMBER",
            b"SCARD",
            b"SRANDMEMBER",
            b"SINTER",
            b"SUNION",
            b"SDIFF",
            b"SINTERCARD",
            b"SSCAN",
            // Sorted-set reads + memory-releasing zset writes (PR-8): not denyoom.
            b"ZREM",
            b"ZPOPMIN",
            b"ZPOPMAX",
            b"ZREMRANGEBYRANK",
            b"ZREMRANGEBYSCORE",
            b"ZREMRANGEBYLEX",
            b"ZSCORE",
            b"ZMSCORE",
            b"ZCARD",
            b"ZRANK",
            b"ZREVRANK",
            b"ZCOUNT",
            b"ZLEXCOUNT",
            b"ZRANGE",
            b"ZREVRANGE",
            b"ZRANGEBYSCORE",
            b"ZREVRANGEBYSCORE",
            b"ZRANGEBYLEX",
            b"ZREVRANGEBYLEX",
            b"ZRANDMEMBER",
            b"ZSCAN",
            b"ZUNION",
            b"ZINTER",
            b"ZDIFF",
            b"ZINTERCARD",
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
