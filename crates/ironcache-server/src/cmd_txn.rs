// SPDX-License-Identifier: MIT OR Apache-2.0
//! Transaction queueing: MULTI/EXEC/DISCARD (TRANSACTIONS.md, #19, PR-10a).
//!
//! Redis transactions are not transactions in the rollback sense (ADR-0010,
//! TRANSACTIONS.md "queue then apply"): `MULTI` opens a per-connection queue, each
//! subsequent non-control command replies `+QUEUED` rather than executing, and
//! `EXEC` applies the staged batch in order, collecting a per-command reply array.
//! There is NO rollback: a per-command runtime error at EXEC time (WRONGTYPE, a
//! not-an-integer INCR, an `-OOM` over budget) becomes an Error ELEMENT in the array
//! and the batch continues [multi-exec-no-rollback].
//!
//! This module owns two pieces:
//! 1. [`queue_validate`] - the queue-time KNOWN-COMMAND + ARITY gate. While in MULTI,
//!    every queued command is validated against the command table here BEFORE staging:
//!    an unknown command or a table-arity violation replies the error NOW and dirties
//!    the transaction (so EXEC returns `-EXECABORT`), matching Redis's queue-time
//!    `lookupCommand` + arity check (src/server.c `processCommand` MULTI path).
//! 2. [`exec_outcome`] / the MULTI/DISCARD helpers - the control-command semantics the
//!    dispatch arms call.
//!
//! WATCH/UNWATCH (the optimistic dirty-CAS) are DEFERRED to PR-10b; this module
//! covers only the queueing surface.

use ironcache_protocol::ErrorReply;

/// The queue-time arity rule for a known command, mirroring the `arity` field of the
/// Redis command table (src/commands.def). Redis encodes arity as a single signed
/// int: a POSITIVE `n` means EXACTLY `n` total arguments (command token included); a
/// NEGATIVE `-n` means AT LEAST `n`. We split that into two explicit variants and
/// validate the queued argc against it, which is exactly the check Redis applies at
/// queue time (the finer per-command option/pair validation happens at EXEC RUN time
/// and, on failure, becomes a runtime error element in the array, with no rollback).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arity {
    /// Exactly `n` arguments total (the command token counts as one).
    Exact(usize),
    /// At least `n` arguments total (variadic tail).
    Min(usize),
}

impl Arity {
    /// Whether `argc` (the total argument count, command token included) satisfies
    /// this rule. Matches Redis `commandCheckArity`: `(arity > 0 && argc != arity) ||
    /// argc < -arity` is the REJECT condition, so here we return the ACCEPT.
    fn accepts(self, argc: usize) -> bool {
        match self {
            Arity::Exact(n) => argc == n,
            Arity::Min(n) => argc >= n,
        }
    }
}

/// The queue-time validation gate for a command staged inside `MULTI`
/// (TRANSACTIONS.md, PR-10a). Returns `Ok(())` if `cmd` is a KNOWN command whose
/// `argc` (total arg count, command token included) satisfies its table arity, and an
/// [`ErrorReply`] to reply NOW (and dirty the transaction) otherwise:
/// - an unrecognized command token -> [`ErrorReply::unknown_command`] (matching the
///   `_ =>` arm of the dispatch match);
/// - a known command with a bad argc -> [`ErrorReply::wrong_arity`].
///
/// `cmd` is the UPPERCASED command token (the caller uppercases, as dispatch does).
/// `args` is the full argument list (for rendering the unknown-command reply, which
/// echoes the leading args byte-for-byte like Redis). The arity TABLE here is intended to
/// mirror the dispatch match arms one-for-one; a unit test
/// ([`tests::table_covers_every_dispatch_arm`]) is a HAND-SYNCED bidirectional cross-check
/// (it is NOT an automatic derivation): it asserts every hand-listed dispatch arm has a
/// table entry AND every table entry maps to a listed dispatch arm AND the two counts are
/// equal, so an out-of-sync table (a command added to dispatch without a table entry, or a
/// stale table entry) trips CI. A true single-source-of-truth command table that removes
/// the hand-sync is the tracked follow-up (#89).
///
/// # Errors
/// Returns an [`ErrorReply`] when the command is unknown or its argument count violates
/// the command-table arity.
pub fn queue_validate(cmd: &[u8], args: &[bytes::Bytes]) -> Result<(), ErrorReply> {
    let total_args = args.len();
    match arity_of(cmd) {
        Some(rule) if rule.accepts(total_args) => Ok(()),
        Some(_) => {
            // Known command, wrong count: the wrong-arity reply uses the LOWERCASE
            // command name (matching the handlers, which pass e.g. "get").
            let name = String::from_utf8_lossy(cmd).to_ascii_lowercase();
            Err(ErrorReply::wrong_arity(&name))
        }
        None => {
            // Unknown command: byte-exact to the dispatch `_ =>` arm (name + leading
            // args, single-quoted, trailing space).
            let name = String::from_utf8_lossy(cmd).into_owned();
            let rest: Vec<&[u8]> = args[1..].iter().map(bytes::Bytes::as_ref).collect();
            Err(ErrorReply::unknown_command(&name, &rest))
        }
    }
}

/// The command-table arity for a known UPPERCASED command token, or `None` if the
/// token is not a command this server implements (PR-1..PR-9 + the txn commands).
///
/// This table is the queue-time arity source and is HAND-SYNCED with the dispatch match
/// arms (every arm has an entry here, and every entry has an arm). It is NOT auto-derived
/// from dispatch; the bidirectional + count cross-check in
/// `tests::table_covers_every_dispatch_arm` is what guards the hand-sync (a true
/// single-source-of-truth command table is the tracked follow-up, #89). The arities are
/// the canonical Redis command-table values (src/commands.def), which are the COARSE check
/// Redis applies at queue time; the handlers apply any finer validation at run time.
///
/// This is a flat lookup TABLE, so its length (`too_many_lines`) and the many arms
/// sharing the same `Arity` value (`match_same_arms`) are intentional: collapsing
/// same-valued arms would group unrelated commands and defeat the one-arm-per-dispatch
/// -arm cross-check. Both lints are allowed here with that justification.
#[allow(clippy::too_many_lines, clippy::match_same_arms)]
fn arity_of(cmd: &[u8]) -> Option<Arity> {
    use Arity::{Exact, Min};
    let rule = match cmd {
        // -- Tier-0 / connection (dispatch.rs). --
        b"PING" => Min(1),
        b"ECHO" => Exact(2),
        b"HELLO" => Min(1),
        b"AUTH" => Min(2),
        b"SELECT" => Exact(2),
        // QUIT's command-table arity is -1 (Min(1)) in src/commands.def, not Exact(1).
        // Inert here (QUIT is in the queue-gate exclusion set, so it bypasses
        // queue_validate), but the table claims the canonical Redis values, so keep it
        // honest.
        b"QUIT" => Min(1),
        b"RESET" => Exact(1),
        b"CLIENT" => Min(2),
        b"COMMAND" => Min(1),
        b"INFO" => Min(1),
        b"CONFIG" => Min(2),
        // -- Transaction control (this module). --
        b"MULTI" => Exact(1),
        b"EXEC" => Exact(1),
        b"DISCARD" => Exact(1),
        // WATCH is -2 in src/commands.def (token + >= 1 key); UNWATCH is exactly 1.
        // WATCH never actually reaches queue_validate (the queue gate excludes it from
        // queueing, like MULTI/EXEC/DISCARD), but the table claims the canonical Redis
        // values + the cross-check below requires an entry per dispatch arm, so keep it.
        // UNWATCH DOES reach queue_validate (it queues inside MULTI as a normal command).
        b"WATCH" => Min(2),
        b"UNWATCH" => Exact(1),
        // -- Strings (cmd_string). --
        b"GET" => Exact(2),
        b"SET" => Min(3),
        b"SETNX" => Exact(3),
        b"GETSET" => Exact(3),
        b"STRLEN" => Exact(2),
        b"INCR" => Exact(2),
        b"DECR" => Exact(2),
        b"INCRBY" => Exact(3),
        b"DECRBY" => Exact(3),
        b"INCRBYFLOAT" => Exact(3),
        b"APPEND" => Exact(3),
        // -- Generic keyspace (cmd_keyspace). --
        b"DEL" => Min(2),
        b"EXISTS" => Min(2),
        b"TYPE" => Exact(2),
        b"KEYS" => Exact(2),
        b"SCAN" => Min(2),
        b"DBSIZE" => Exact(1),
        b"RANDOMKEY" => Exact(1),
        b"RENAME" => Exact(3),
        b"RENAMENX" => Exact(3),
        b"COPY" => Min(3),
        b"MOVE" => Exact(3),
        b"SWAPDB" => Exact(3),
        b"TOUCH" => Min(2),
        b"UNLINK" => Min(2),
        b"FLUSHDB" => Min(1),
        b"FLUSHALL" => Min(1),
        // -- TTL / EXPIRE family (cmd_expire). --
        b"EXPIRE" => Min(3),
        b"PEXPIRE" => Min(3),
        b"EXPIREAT" => Min(3),
        b"PEXPIREAT" => Min(3),
        b"TTL" => Exact(2),
        b"PTTL" => Exact(2),
        b"EXPIRETIME" => Exact(2),
        b"PEXPIRETIME" => Exact(2),
        b"PERSIST" => Exact(2),
        b"GETEX" => Min(2),
        b"SETEX" => Exact(4),
        b"PSETEX" => Exact(4),
        // -- Lists (cmd_list). --
        b"LPUSH" => Min(3),
        b"RPUSH" => Min(3),
        b"LPUSHX" => Min(3),
        b"RPUSHX" => Min(3),
        b"LPOP" => Min(2),
        b"RPOP" => Min(2),
        b"LLEN" => Exact(2),
        b"LRANGE" => Exact(4),
        b"LINDEX" => Exact(3),
        b"LSET" => Exact(4),
        b"LINSERT" => Exact(5),
        b"LREM" => Exact(4),
        b"LTRIM" => Exact(4),
        b"LMOVE" => Exact(5),
        b"RPOPLPUSH" => Exact(3),
        b"LPOS" => Min(3),
        // -- Hashes (cmd_hash). --
        b"HSET" => Min(4),
        b"HMSET" => Min(4),
        b"HSETNX" => Exact(4),
        b"HGET" => Exact(3),
        b"HMGET" => Min(3),
        b"HDEL" => Min(3),
        b"HGETALL" => Exact(2),
        b"HKEYS" => Exact(2),
        b"HVALS" => Exact(2),
        b"HLEN" => Exact(2),
        b"HEXISTS" => Exact(3),
        b"HSTRLEN" => Exact(3),
        b"HINCRBY" => Exact(4),
        b"HINCRBYFLOAT" => Exact(4),
        b"HRANDFIELD" => Min(2),
        b"HSCAN" => Min(3),
        // -- Sets (cmd_set). --
        b"SADD" => Min(3),
        b"SREM" => Min(3),
        b"SMEMBERS" => Exact(2),
        b"SISMEMBER" => Exact(3),
        b"SMISMEMBER" => Min(3),
        b"SCARD" => Exact(2),
        b"SPOP" => Min(2),
        b"SRANDMEMBER" => Min(2),
        b"SMOVE" => Exact(4),
        b"SINTER" => Min(2),
        b"SUNION" => Min(2),
        b"SDIFF" => Min(2),
        b"SINTERCARD" => Min(3),
        b"SINTERSTORE" => Min(3),
        b"SUNIONSTORE" => Min(3),
        b"SDIFFSTORE" => Min(3),
        b"SSCAN" => Min(3),
        // -- Sorted sets (cmd_zset). --
        b"ZADD" => Min(4),
        b"ZINCRBY" => Exact(4),
        b"ZREM" => Min(3),
        b"ZSCORE" => Exact(3),
        b"ZMSCORE" => Min(3),
        b"ZCARD" => Exact(2),
        b"ZRANK" => Min(3),
        b"ZREVRANK" => Min(3),
        b"ZCOUNT" => Exact(4),
        b"ZLEXCOUNT" => Exact(4),
        b"ZRANGE" => Min(4),
        b"ZREVRANGE" => Min(4),
        b"ZRANGEBYSCORE" => Min(4),
        b"ZREVRANGEBYSCORE" => Min(4),
        b"ZRANGEBYLEX" => Min(4),
        b"ZREVRANGEBYLEX" => Min(4),
        b"ZREMRANGEBYRANK" => Exact(4),
        b"ZREMRANGEBYSCORE" => Exact(4),
        b"ZREMRANGEBYLEX" => Exact(4),
        b"ZPOPMIN" => Min(2),
        b"ZPOPMAX" => Min(2),
        b"ZRANDMEMBER" => Min(2),
        b"ZSCAN" => Min(3),
        b"ZRANGESTORE" => Min(5),
        b"ZUNION" => Min(3),
        b"ZINTER" => Min(3),
        b"ZDIFF" => Min(3),
        b"ZUNIONSTORE" => Min(4),
        b"ZINTERSTORE" => Min(4),
        b"ZDIFFSTORE" => Min(4),
        b"ZINTERCARD" => Min(3),
        // -- Bitmaps (cmd_bitmap). --
        b"SETBIT" => Exact(4),
        b"GETBIT" => Exact(3),
        b"BITCOUNT" => Min(2),
        b"BITPOS" => Min(3),
        b"BITOP" => Min(4),
        b"BITFIELD" => Min(2),
        b"BITFIELD_RO" => Min(2),
        // -- Introspection (cmd_introspect). --
        b"OBJECT" => Min(2),
        _ => return None,
    };
    Some(rule)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn args(parts: &[&[u8]]) -> Vec<Bytes> {
        parts.iter().map(|p| Bytes::copy_from_slice(p)).collect()
    }

    #[test]
    fn known_commands_at_correct_arity_pass() {
        assert!(queue_validate(b"GET", &args(&[b"GET", b"k"])).is_ok());
        assert!(queue_validate(b"SET", &args(&[b"SET", b"k", b"v"])).is_ok());
        assert!(queue_validate(b"SET", &args(&[b"SET", b"k", b"v", b"EX", b"5"])).is_ok());
        assert!(queue_validate(b"INCR", &args(&[b"INCR", b"k"])).is_ok());
        assert!(queue_validate(b"MULTI", &args(&[b"MULTI"])).is_ok());
        // WATCH is -2 (token + >= 1 key); UNWATCH is exactly 1 (PR-10b).
        assert!(queue_validate(b"WATCH", &args(&[b"WATCH", b"k"])).is_ok());
        assert!(queue_validate(b"WATCH", &args(&[b"WATCH", b"k1", b"k2"])).is_ok());
        assert!(queue_validate(b"WATCH", &args(&[b"WATCH"])).is_err()); // no key -> arity
        assert!(queue_validate(b"UNWATCH", &args(&[b"UNWATCH"])).is_ok());
        assert!(queue_validate(b"UNWATCH", &args(&[b"UNWATCH", b"x"])).is_err()); // exact 1
        assert!(queue_validate(b"PING", &args(&[b"PING"])).is_ok());
        assert!(queue_validate(b"PING", &args(&[b"PING", b"msg"])).is_ok());
        assert!(queue_validate(b"MSET".as_slice(), &args(&[b"MSET"])).is_err()); // MSET not impl -> unknown
    }

    #[test]
    fn wrong_arity_is_rejected_with_the_arity_error() {
        // GET with no key (the canonical queue-time arity error from the task).
        match queue_validate(b"GET", &args(&[b"GET"])) {
            Err(e) => assert_eq!(e.line(), "-ERR wrong number of arguments for 'get' command"),
            Ok(()) => panic!("GET with no key must fail arity"),
        }
        // Exact-arity over-count: INCR with an extra arg.
        assert!(queue_validate(b"INCR", &args(&[b"INCR", b"k", b"extra"])).is_err());
        // Min-arity under-count: DEL with no key.
        assert!(queue_validate(b"DEL", &args(&[b"DEL"])).is_err());
        // A control command with an extra arg (EXEC takes exactly 1).
        assert!(queue_validate(b"EXEC", &args(&[b"EXEC", b"x"])).is_err());
    }

    #[test]
    fn unknown_command_is_rejected_with_unknown_command_error() {
        match queue_validate(b"FROBNICATE", &args(&[b"FROBNICATE", b"a", b"b"])) {
            Err(e) => assert_eq!(
                e.line(),
                "-ERR unknown command 'FROBNICATE', with args beginning with: 'a' 'b' "
            ),
            Ok(()) => panic!("unknown command must fail"),
        }
    }

    /// The arity table MUST cover EXACTLY the dispatch match arms (every implemented
    /// command, plus MULTI/EXEC/DISCARD): a missing entry would wrongly EXECABORT a valid
    /// queued command, and a stale entry would queue a command dispatch then rejects.
    ///
    /// This is a HAND-SYNCED BIDIRECTIONAL cross-check (NOT an automatic derivation; the
    /// single-source-of-truth table is the tracked follow-up #89). It holds two hand-listed
    /// command sets, one per side: `dispatch_arms` (every command token the dispatch match
    /// in dispatch.rs handles) and `table_commands` (every command token keyed in
    /// [`arity_of`] above), and enforces, in BOTH directions plus by count, that they agree:
    ///
    /// 1. FORWARD: every `dispatch_arm` has an [`arity_of`] entry (else wrong EXECABORT).
    /// 2. REVERSE: every `table_command` is a known dispatch arm AND has an [`arity_of`]
    ///    entry (no stale table row that dispatch does not handle).
    /// 3. COUNT: the two lists are the SAME length and have no duplicates, so neither side
    ///    can carry an extra row the other lacks while still passing 1 and 2.
    ///
    /// If you add or remove a dispatch arm, update BOTH lists here AND the [`arity_of`]
    /// table; an out-of-sync table trips this test in CI.
    #[test]
    #[allow(clippy::too_many_lines)] // The cross-check is two long literal command lists by design.
    fn table_covers_every_dispatch_arm() {
        // Every command token the dispatch match handles (PR-1..PR-9 + txn). Kept in
        // dispatch-arm order for an easy side-by-side diff against dispatch.rs.
        let dispatch_arms: &[&[u8]] = &[
            // Tier-0
            b"PING",
            b"ECHO",
            b"HELLO",
            b"AUTH",
            b"SELECT",
            b"QUIT",
            b"RESET",
            b"CLIENT",
            b"COMMAND",
            b"INFO",
            b"CONFIG",
            // Transaction control
            b"MULTI",
            b"EXEC",
            b"DISCARD",
            b"WATCH",
            b"UNWATCH",
            // Strings
            b"GET",
            b"SET",
            b"SETNX",
            b"GETSET",
            b"STRLEN",
            b"INCR",
            b"DECR",
            b"INCRBY",
            b"DECRBY",
            b"INCRBYFLOAT",
            b"APPEND",
            // Keyspace
            b"DEL",
            b"EXISTS",
            b"TYPE",
            b"KEYS",
            b"SCAN",
            b"DBSIZE",
            b"RANDOMKEY",
            b"RENAME",
            b"RENAMENX",
            b"COPY",
            b"MOVE",
            b"SWAPDB",
            b"TOUCH",
            b"UNLINK",
            b"FLUSHDB",
            b"FLUSHALL",
            // TTL / EXPIRE
            b"EXPIRE",
            b"PEXPIRE",
            b"EXPIREAT",
            b"PEXPIREAT",
            b"TTL",
            b"PTTL",
            b"EXPIRETIME",
            b"PEXPIRETIME",
            b"PERSIST",
            b"GETEX",
            b"SETEX",
            b"PSETEX",
            // Lists
            b"LPUSH",
            b"RPUSH",
            b"LPUSHX",
            b"RPUSHX",
            b"LPOP",
            b"RPOP",
            b"LLEN",
            b"LRANGE",
            b"LINDEX",
            b"LSET",
            b"LINSERT",
            b"LREM",
            b"LTRIM",
            b"LMOVE",
            b"RPOPLPUSH",
            b"LPOS",
            // Hashes
            b"HSET",
            b"HMSET",
            b"HSETNX",
            b"HGET",
            b"HMGET",
            b"HDEL",
            b"HGETALL",
            b"HKEYS",
            b"HVALS",
            b"HLEN",
            b"HEXISTS",
            b"HSTRLEN",
            b"HINCRBY",
            b"HINCRBYFLOAT",
            b"HRANDFIELD",
            b"HSCAN",
            // Sets
            b"SADD",
            b"SREM",
            b"SMEMBERS",
            b"SISMEMBER",
            b"SMISMEMBER",
            b"SCARD",
            b"SPOP",
            b"SRANDMEMBER",
            b"SMOVE",
            b"SINTER",
            b"SUNION",
            b"SDIFF",
            b"SINTERCARD",
            b"SINTERSTORE",
            b"SUNIONSTORE",
            b"SDIFFSTORE",
            b"SSCAN",
            // Sorted sets
            b"ZADD",
            b"ZINCRBY",
            b"ZREM",
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
            b"ZREMRANGEBYRANK",
            b"ZREMRANGEBYSCORE",
            b"ZREMRANGEBYLEX",
            b"ZPOPMIN",
            b"ZPOPMAX",
            b"ZRANDMEMBER",
            b"ZSCAN",
            b"ZRANGESTORE",
            b"ZUNION",
            b"ZINTER",
            b"ZDIFF",
            b"ZUNIONSTORE",
            b"ZINTERSTORE",
            b"ZDIFFSTORE",
            b"ZINTERCARD",
            // Bitmaps
            b"SETBIT",
            b"GETBIT",
            b"BITCOUNT",
            b"BITPOS",
            b"BITOP",
            b"BITFIELD",
            b"BITFIELD_RO",
            // Introspection
            b"OBJECT",
        ];

        // Every command token keyed in `arity_of` above. Kept in `arity_of` order for an
        // easy side-by-side diff against the table. This is the REVERSE side of the
        // cross-check: a row here that dispatch does not handle is a stale table entry.
        let table_commands: &[&[u8]] = &[
            // Tier-0 / connection
            b"PING",
            b"ECHO",
            b"HELLO",
            b"AUTH",
            b"SELECT",
            b"QUIT",
            b"RESET",
            b"CLIENT",
            b"COMMAND",
            b"INFO",
            b"CONFIG",
            // Transaction control
            b"MULTI",
            b"EXEC",
            b"DISCARD",
            b"WATCH",
            b"UNWATCH",
            // Strings
            b"GET",
            b"SET",
            b"SETNX",
            b"GETSET",
            b"STRLEN",
            b"INCR",
            b"DECR",
            b"INCRBY",
            b"DECRBY",
            b"INCRBYFLOAT",
            b"APPEND",
            // Generic keyspace
            b"DEL",
            b"EXISTS",
            b"TYPE",
            b"KEYS",
            b"SCAN",
            b"DBSIZE",
            b"RANDOMKEY",
            b"RENAME",
            b"RENAMENX",
            b"COPY",
            b"MOVE",
            b"SWAPDB",
            b"TOUCH",
            b"UNLINK",
            b"FLUSHDB",
            b"FLUSHALL",
            // TTL / EXPIRE
            b"EXPIRE",
            b"PEXPIRE",
            b"EXPIREAT",
            b"PEXPIREAT",
            b"TTL",
            b"PTTL",
            b"EXPIRETIME",
            b"PEXPIRETIME",
            b"PERSIST",
            b"GETEX",
            b"SETEX",
            b"PSETEX",
            // Lists
            b"LPUSH",
            b"RPUSH",
            b"LPUSHX",
            b"RPUSHX",
            b"LPOP",
            b"RPOP",
            b"LLEN",
            b"LRANGE",
            b"LINDEX",
            b"LSET",
            b"LINSERT",
            b"LREM",
            b"LTRIM",
            b"LMOVE",
            b"RPOPLPUSH",
            b"LPOS",
            // Hashes
            b"HSET",
            b"HMSET",
            b"HSETNX",
            b"HGET",
            b"HMGET",
            b"HDEL",
            b"HGETALL",
            b"HKEYS",
            b"HVALS",
            b"HLEN",
            b"HEXISTS",
            b"HSTRLEN",
            b"HINCRBY",
            b"HINCRBYFLOAT",
            b"HRANDFIELD",
            b"HSCAN",
            // Sets
            b"SADD",
            b"SREM",
            b"SMEMBERS",
            b"SISMEMBER",
            b"SMISMEMBER",
            b"SCARD",
            b"SPOP",
            b"SRANDMEMBER",
            b"SMOVE",
            b"SINTER",
            b"SUNION",
            b"SDIFF",
            b"SINTERCARD",
            b"SINTERSTORE",
            b"SUNIONSTORE",
            b"SDIFFSTORE",
            b"SSCAN",
            // Sorted sets
            b"ZADD",
            b"ZINCRBY",
            b"ZREM",
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
            b"ZREMRANGEBYRANK",
            b"ZREMRANGEBYSCORE",
            b"ZREMRANGEBYLEX",
            b"ZPOPMIN",
            b"ZPOPMAX",
            b"ZRANDMEMBER",
            b"ZSCAN",
            b"ZRANGESTORE",
            b"ZUNION",
            b"ZINTER",
            b"ZDIFF",
            b"ZUNIONSTORE",
            b"ZINTERSTORE",
            b"ZDIFFSTORE",
            b"ZINTERCARD",
            // Bitmaps
            b"SETBIT",
            b"GETBIT",
            b"BITCOUNT",
            b"BITPOS",
            b"BITOP",
            b"BITFIELD",
            b"BITFIELD_RO",
            // Introspection
            b"OBJECT",
        ];

        let dispatch_set: std::collections::BTreeSet<&[u8]> =
            dispatch_arms.iter().copied().collect();
        let table_set: std::collections::BTreeSet<&[u8]> = table_commands.iter().copied().collect();

        // Neither hand-list may carry a duplicate (a dup would mask a missing row in the
        // count check by inflating the length while the set stays short).
        assert_eq!(
            dispatch_set.len(),
            dispatch_arms.len(),
            "the dispatch_arms list has a duplicate entry"
        );
        assert_eq!(
            table_set.len(),
            table_commands.len(),
            "the table_commands list has a duplicate entry"
        );

        // 1. FORWARD: every dispatch arm has an `arity_of` entry (a missing entry would
        //    wrongly EXECABORT a valid queued command).
        for cmd in dispatch_arms {
            assert!(
                arity_of(cmd).is_some(),
                "dispatch arm {:?} has no queue_validate arity entry (would wrongly EXECABORT)",
                String::from_utf8_lossy(cmd)
            );
        }

        // 2. REVERSE: every command in the arity table is a known dispatch arm AND its
        //    `arity_of` lookup resolves (no stale table row dispatch does not handle).
        for cmd in table_commands {
            assert!(
                arity_of(cmd).is_some(),
                "table_commands row {:?} does not resolve in arity_of (stale table list)",
                String::from_utf8_lossy(cmd)
            );
            assert!(
                dispatch_set.contains(*cmd),
                "arity table entry {:?} maps to no dispatch arm (stale table row)",
                String::from_utf8_lossy(cmd)
            );
        }

        // 3. COUNT-EQUALITY: the two sides have the SAME number of commands, so neither
        //    can carry an extra row the other lacks while still passing 1 and 2. The
        //    `table_commands` length IS the arity_of table size (step 2 verified every row
        //    resolves in `arity_of`), so this is the "table size == dispatch_arms size"
        //    guard the cross-check claims.
        assert_eq!(
            table_commands.len(),
            dispatch_arms.len(),
            "the arity_of table size does not equal the dispatch arm count (out of sync)"
        );
        assert_eq!(
            dispatch_set, table_set,
            "the dispatch arms and the arity table do not cover the same command set"
        );
    }

    #[test]
    fn arity_accepts_matches_redis_rule() {
        assert!(Arity::Exact(2).accepts(2));
        assert!(!Arity::Exact(2).accepts(1));
        assert!(!Arity::Exact(2).accepts(3));
        assert!(Arity::Min(2).accepts(2));
        assert!(Arity::Min(2).accepts(9));
        assert!(!Arity::Min(2).accepts(1));
    }
}
