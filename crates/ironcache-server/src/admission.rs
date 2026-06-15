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
/// ceiling gates (ADMISSION.md). `true` for the write/RMW commands that can grow memory;
/// `false` for reads, the EXISTS/TYPE/STRLEN introspection, the memory-RELEASING `DEL`,
/// the Tier-0 connection commands, and the EXPIRE/TTL/PERSIST family (those do not
/// allocate value bytes).
///
/// This is now a THIN WRAPPER over the #89 single-source-of-truth command registry
/// ([`crate::command_spec::spec_of`]): the flag is the `denyoom` field of the command's
/// [`crate::command_spec::CommandSpec`], so the `denyoom` classification cannot drift from
/// the arity / routing tables (which read the same registry), and a new write that is added
/// to the registry with `denyoom: true` cannot silently bypass the ceiling. The set
/// mirrors Redis's `CMD_DENYOOM` flag for the commands IronCache implements:
///
/// - String writes/RMW (SET/SETNX/GETSET/APPEND/INCR*/DECR*/SETEX/PSETEX/MSET) -- allocate
///   a value.
/// - RENAME/RENAMENX/COPY -- materialize a value at the destination. MOVE and SMOVE are NOT
///   denyoom (Redis flags them write-fast: they RELOCATE rather than duplicate).
/// - Collection writes that allocate value bytes: list (LPUSH/RPUSH/LPUSHX/RPUSHX/LSET/
///   LINSERT/LMOVE/RPOPLPUSH), hash (HSET/HMSET/HSETNX/HINCRBY/HINCRBYFLOAT), set
///   (SADD/SINTERSTORE/SUNIONSTORE/SDIFFSTORE), zset (ZADD/ZINCRBY/ZRANGESTORE/
///   ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE), bitmap (SETBIT/BITOP/BITFIELD -- Redis flags the
///   whole BITFIELD denyoom even all-GET), and HLL (PFADD/PFMERGE).
///
/// The memory-RELEASING writes (DEL/UNLINK/FLUSH*/LPOP/SREM/ZREM/...) and all reads are NOT
/// gated, so a client can read and free under memory pressure.
#[must_use]
pub fn is_denyoom(cmd: &[u8]) -> bool {
    crate::command_spec::spec_of(cmd).is_some_and(|s| s.denyoom)
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
            // MSET is a denyoom multi-key write.
            b"MSET",
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
            // Bitmap writes (PR-9).
            b"SETBIT",
            b"BITOP",
            b"BITFIELD",
            // HyperLogLog writes (PR-11).
            b"PFADD",
            b"PFMERGE",
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
            // MGET is a multi-key READ: never denyoom.
            b"MGET",
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
            // Bitmap reads (PR-9): GETBIT/BITCOUNT/BITPOS and BITFIELD_RO never grow.
            b"GETBIT",
            b"BITCOUNT",
            b"BITPOS",
            b"BITFIELD_RO",
            // HyperLogLog read (PR-11): PFCOUNT always recomputes, never writes.
            b"PFCOUNT",
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
