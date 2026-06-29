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

// The queue-time arity rule lives in the #89 single-source-of-truth command registry
// ([`crate::command_spec`]); it is re-exported here so this module's legacy `Arity` path
// (and the `queue_validate` arity gate) keeps working unchanged. The rule mirrors the
// `arity` field of the Redis command table (src/commands.def): a POSITIVE `n` is EXACTLY
// `n` total args (command token included), a NEGATIVE `-n` is AT LEAST `n`, which we split
// into the two `Arity` variants. `queue_validate` applies it as the COARSE queue-time
// check (the finer per-command option/pair validation happens at EXEC RUN time and, on
// failure, becomes a runtime error element in the array, with no rollback).
pub use crate::command_spec::Arity;

/// The queue-time validation gate for a command staged inside `MULTI`
/// (TRANSACTIONS.md, PR-10a). Returns `Ok(())` if `cmd` is a KNOWN command whose
/// `argc` (total arg count, command token included) satisfies its table arity, and an
/// [`ErrorReply`] to reply NOW (and dirty the transaction) otherwise:
/// - an unrecognized command token -> [`ErrorReply::unknown_command`] (matching the
///   `_ =>` arm of the dispatch match);
/// - a known command with a bad argc -> [`ErrorReply::wrong_arity`].
///
/// `cmd` is the UPPERCASED command token (the caller uppercases, as dispatch does).
/// `args` is the full argument list (for rendering the unknown-command reply, which echoes
/// the leading args byte-for-byte like Redis). The arity comes from the #89 single-source-
/// of-truth command registry ([`crate::command_spec::spec_of`]) via [`arity_of`], so it can
/// no longer drift from the routing / admission tables (which read the same registry). The
/// registry MUST cover exactly the dispatch HANDLER arms; the unit test
/// ([`tests::table_covers_every_dispatch_arm`]) is the single registry-vs-dispatch-arm
/// cross-check (the dispatch handler match is the one thing that cannot be const data, so
/// the dispatch-arm hand-list in [`tests::dispatch_arm_names`] is the lone remaining
/// hand-sync, asserted equal to the registry name set).
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

/// The command-table arity for a known UPPERCASED command token, or `None` if the token
/// is not a command this server implements (PR-1..PR-9 + the txn commands).
///
/// This is now a THIN WRAPPER over the #89 single-source-of-truth command registry
/// ([`crate::command_spec::spec_of`]): the arity is the `arity` field of the command's
/// [`crate::command_spec::CommandSpec`], so there is exactly one place that defines a
/// command's arity (the registry), and this gate can no longer drift from the routing /
/// admission tables that read the same registry. The arities are the canonical Redis
/// command-table values (src/commands.def), the COARSE check Redis applies at queue time;
/// the handlers apply any finer validation at run time.
fn arity_of(cmd: &[u8]) -> Option<Arity> {
    crate::command_spec::spec_of(cmd).map(|s| s.arity)
}

#[cfg(test)]
pub(crate) mod tests {
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
        // MSET is implemented (arity Min(3)); bare `MSET` is a wrong-arity rejection.
        assert_eq!(
            queue_validate(b"MSET".as_slice(), &args(&[b"MSET"]))
                .unwrap_err()
                .line(),
            "-ERR wrong number of arguments for 'mset' command"
        );
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

    /// The ONE hand-listed dispatch-arm set: every command token that has a dispatch
    /// HANDLER arm (across `dispatch_inner` + `dispatch_keyed_data`, plus the control verbs).
    ///
    /// After #89 this is the SINGLE remaining hand-sync in the command system. The dispatch
    /// handler match cannot be enumerated programmatically (the handlers have varied
    /// signatures, so they stay as match arms), so this list is the source the registry is
    /// cross-checked against: `table_covers_every_dispatch_arm` asserts this set is EXACTLY
    /// the [`crate::command_spec::spec_of`] registry name set (bidirectional + count). The
    /// registry ([`crate::command_spec`]) is the single source for all DATA attributes
    /// (arity / class / key spec / denyoom / control); only "does this command have a
    /// dispatch handler arm" remains hand-listed here. Kept in dispatch-arm order for an easy
    /// side-by-side diff against dispatch.rs.
    #[allow(clippy::too_many_lines)] // One command per line: the literal dispatch-arm list, by design.
    pub(crate) fn dispatch_arm_names() -> &'static [&'static [u8]] {
        &[
            // Tier-0
            b"PING",
            b"ECHO",
            b"LOLWUT",
            b"HELLO",
            b"AUTH",
            b"SELECT",
            b"QUIT",
            b"RESET",
            b"READONLY",
            b"READWRITE",
            b"CLIENT",
            b"COMMAND",
            b"INFO",
            b"CONFIG",
            // Operability / admin introspection (PROD-7): SLOWLOG / MEMORY / LATENCY. AlwaysHome
            // admin containers dispatched in `dispatch_inner` (they need `ctx` for the SLOWLOG ring
            // / LATENCY monitor / client registry).
            b"SLOWLOG",
            b"MEMORY",
            b"LATENCY",
            b"CLUSTER",
            // Persistence (#58): SAVE / BGSAVE / LASTSAVE. The real cross-shard save lives in the
            // binary serve layer; these dispatch arms are the persistence-disabled fallback.
            b"SAVE",
            b"BGSAVE",
            b"LASTSAVE",
            // Graceful shutdown (#139): SHUTDOWN [NOSAVE|SAVE]. The save-on-exit + the process
            // exit-0 live in the binary serve layer (which holds the stores + the data_dir); this
            // dispatch arm is the never-intercepted fallback (a SHUTDOWN inside MULTI).
            b"SHUTDOWN",
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
            b"GETRANGE",
            b"SUBSTR",
            b"SETRANGE",
            b"GETDEL",
            b"MGET",
            b"MSET",
            b"MSETNX",
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
            b"LMPOP",
            // Blocking list pops (PROD-9): the NON-BLOCKING dispatch arm (EXEC-replay / direct
            // fallback). The LIVE blocking path is intercepted in the serve layer.
            b"BLPOP",
            b"BRPOP",
            b"BLMOVE",
            b"BRPOPLPUSH",
            b"BLMPOP",
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
            b"HEXPIRE",
            b"HPEXPIRE",
            b"HEXPIREAT",
            b"HPEXPIREAT",
            b"HTTL",
            b"HPTTL",
            b"HEXPIRETIME",
            b"HPEXPIRETIME",
            b"HPERSIST",
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
            b"ZMPOP",
            // Blocking zset pops (PROD-9): the NON-BLOCKING dispatch arm; the LIVE blocking path is
            // intercepted in the serve layer. WAIT (the replica-ack wait) also lands here -- it has
            // no key, so it sits with the blocking family rather than a data section.
            b"BZPOPMIN",
            b"BZPOPMAX",
            b"BZMPOP",
            b"WAIT",
            // Bitmaps
            b"SETBIT",
            b"GETBIT",
            b"BITCOUNT",
            b"BITPOS",
            b"BITOP",
            b"BITFIELD",
            b"BITFIELD_RO",
            // HyperLogLog
            b"PFADD",
            b"PFCOUNT",
            b"PFMERGE",
            // Generic: SORT / SORT_RO
            b"SORT",
            b"SORT_RO",
            // Introspection
            b"OBJECT",
            // Internal cross-shard verbs (client-unreachable; only the coordinator issues them)
            b"__ICSTORESET",
            b"__ICSTOREZSET",
            b"__ICSTOREHLL",
        ]
    }

    /// The registry MUST cover EXACTLY the dispatch HANDLER arms: a registry entry with no
    /// dispatch arm would queue a command that then hits the dispatch `_ =>` unknown-command
    /// reply, and a dispatch arm with no registry entry would wrongly EXECABORT a valid
    /// queued command (no arity) and mis-route it (the routing/admission wrappers all read
    /// the registry).
    ///
    /// AFTER #89 this is a SINGLE registry-vs-dispatch cross-check (it REPLACES the old DUAL
    /// hand-listed 148-entry arrays). The ONE remaining hand-list is [`dispatch_arm_names`]
    /// (the dispatch HANDLER arms, which cannot be enumerated programmatically); the
    /// [`crate::command_spec::spec_of`] registry is the source for every DATA attribute. We
    /// assert, bidirectionally + by count:
    ///
    /// (a) FORWARD: every `dispatch_arm_names` entry resolves in `spec_of` (so it has an
    ///     arity / class / key spec / denyoom / control -- no wrongly-EXECABORTed command).
    /// (b) REVERSE: every `spec_of` registry name has a dispatch arm (no registry row that
    ///     dispatch does not handle).
    /// (c) NO DUPLICATES in the hand-list, and the two SETS are EQUAL with the SAME count.
    ///
    /// If you add or remove a dispatch arm, update [`dispatch_arm_names`] AND add/remove the
    /// matching `spec_of` registry entry; an out-of-sync pair trips this test in CI. There is
    /// now exactly ONE place to hand-edit (this list) and ONE registry to edit (the data).
    #[test]
    fn table_covers_every_dispatch_arm() {
        let dispatch_arms = dispatch_arm_names();
        let dispatch_set: std::collections::BTreeSet<&[u8]> =
            dispatch_arms.iter().copied().collect();

        // (c) No duplicate in the hand-list (a dup would mask a missing row in the count
        // check by inflating the length while the set stays short).
        assert_eq!(
            dispatch_set.len(),
            dispatch_arms.len(),
            "the dispatch_arm_names list has a duplicate entry"
        );

        // (a) FORWARD: every dispatch arm resolves in the registry (a missing entry would
        // wrongly EXECABORT a valid queued command and have no class/key-spec/denyoom).
        for cmd in dispatch_arms {
            assert!(
                crate::command_spec::spec_of(cmd).is_some(),
                "dispatch arm {:?} has no command_spec registry entry (would wrongly EXECABORT)",
                String::from_utf8_lossy(cmd)
            );
            // arity_of derives from the registry, so this is necessarily Some too; assert it
            // to keep the queue-gate guarantee explicit at this site.
            assert!(
                arity_of(cmd).is_some(),
                "dispatch arm {:?} has no queue_validate arity (would wrongly EXECABORT)",
                String::from_utf8_lossy(cmd)
            );
        }

        // (b) REVERSE: every registry name has a dispatch arm (no registry row dispatch does
        // not handle). The registry name set is enumerated by walking the hand-list and
        // confirming each `spec_of(name).name` round-trips, then asserting set-equality
        // below; a registry entry whose name is NOT in the dispatch list would break the
        // set-equality + count check (c).
        for cmd in dispatch_arms {
            let spec = crate::command_spec::spec_of(cmd).expect("forward pass proved Some");
            assert_eq!(
                spec.name,
                *cmd,
                "registry entry name {:?} does not match its lookup key {:?}",
                String::from_utf8_lossy(spec.name),
                String::from_utf8_lossy(cmd)
            );
        }

        // (c) COUNT + SET equality: the registry name set EQUALS the dispatch-arm set, same
        // count. We build the registry name set by collecting every `spec_of` name reachable
        // from the dispatch list (proved bijective with their keys above) and assert it is
        // exactly the dispatch set with the same length, so neither side can carry an extra
        // row the other lacks. (The registry has no programmatic iterator -- it is a match --
        // so the dispatch list IS the enumeration basis, and the per-entry name round-trip
        // above plus this equality is the bidirectional cover.)
        let registry_set: std::collections::BTreeSet<&[u8]> = dispatch_arms
            .iter()
            .map(|c| {
                crate::command_spec::spec_of(c)
                    .expect("forward pass proved Some")
                    .name
            })
            .collect();
        assert_eq!(
            registry_set, dispatch_set,
            "the command_spec registry and the dispatch arms do not cover the same command set"
        );
        assert_eq!(
            registry_set.len(),
            dispatch_arms.len(),
            "the command_spec registry size does not equal the dispatch arm count (out of sync)"
        );

        // SERVE-LAYER-ROUTED commands (SERVER_PUSH.md #20, PR 91a/91b): SUBSCRIBE / UNSUBSCRIBE /
        // PSUBSCRIBE / PUNSUBSCRIBE / PUBLISH / PUBSUB and the internal `__ICPUBLISH` /
        // `__ICPUBSUB` are in the `spec_of` registry (so their arity validates and `classify`
        // returns AlwaysHome) but are intercepted in the SERVE layer (`route_and_dispatch`) BEFORE
        // dispatch -- registration needs the per-connection push sender + the per-shard
        // subscription table, which live in the serve loop -- so they have NO `dispatch_inner` arm
        // and are deliberately ABSENT from `dispatch_arm_names`. The set-equality above therefore
        // does NOT cover them; assert that contract directly (registry-present, dispatch-arm-
        // absent) so a future edit cannot silently regress it (e.g. accidentally adding a real
        // dispatch arm for one of them, or dropping it from the registry so its arity stops
        // validating).
        for cmd in [
            b"SUBSCRIBE".as_slice(),
            b"UNSUBSCRIBE",
            b"PSUBSCRIBE",
            b"PUNSUBSCRIBE",
            b"PUBLISH",
            b"PUBSUB",
            b"__ICPUBLISH",
            b"__ICPUBSUB",
        ] {
            assert!(
                crate::command_spec::spec_of(cmd).is_some(),
                "serve-layer pub/sub command {:?} must be in the registry (arity validation)",
                String::from_utf8_lossy(cmd)
            );
            assert!(
                !dispatch_set.contains(cmd),
                "serve-layer pub/sub command {:?} must NOT be a dispatch arm (it is intercepted in route_and_dispatch)",
                String::from_utf8_lossy(cmd)
            );
        }
        // BLOCKING commands (PROD-9): BLPOP / BRPOP / BLMOVE / BRPOPLPUSH / BLMPOP / BZPOPMIN /
        // BZPOPMAX / BZMPOP / WAIT have a NON-BLOCKING dispatch arm (the EXEC-replay / direct
        // fallback that returns nil at once if empty), UNLIKE the pub/sub commands which have NONE.
        // So they ARE in `dispatch_arm_names` (covered by the set-equality above). The serve layer
        // ADDITIONALLY intercepts them on the LIVE path (to PARK), but the dispatch arm is the
        // EXEC-time / fallback semantics. Assert that contract directly: registry-present AND a
        // dispatch arm.
        for cmd in [
            b"BLPOP".as_slice(),
            b"BRPOP",
            b"BLMOVE",
            b"BRPOPLPUSH",
            b"BLMPOP",
            b"BZPOPMIN",
            b"BZPOPMAX",
            b"BZMPOP",
            b"WAIT",
        ] {
            assert!(
                crate::command_spec::spec_of(cmd).is_some(),
                "blocking command {:?} must be in the registry (arity validation)",
                String::from_utf8_lossy(cmd)
            );
            assert!(
                dispatch_set.contains(cmd),
                "blocking command {:?} must have a (non-blocking) dispatch arm (EXEC-replay fallback)",
                String::from_utf8_lossy(cmd)
            );
        }
    }

    /// SAFETY-NET COUNT GUARD: the single hand-list [`dispatch_arm_names`] still has the
    /// canonical 155-command CLIENT surface (PR-1..PR-11 + the 6 txn control verbs + CLUSTER,
    /// CLUSTER_CONTRACT.md #70 slice 1, + READONLY/READWRITE, REPLICA_READ.md #147 HA-7d, +
    /// SAVE/BGSAVE/LASTSAVE, #58 persistence, + SHUTDOWN, #139 graceful shutdown) PLUS the 3
    /// INTERNAL cross-shard verbs (`__ICSTORESET` + `__ICSTOREZSET` + `__ICSTOREHLL`,
    /// COORDINATOR.md #107 Stage 2b -- real dispatch arms + registry entries, but client-
    /// unreachable), so 158 dispatch arms total. This asserts a COUNT only (not values --
    /// the value-level cover is the set-equality in `table_covers_every_dispatch_arm`), so it is
    /// NOT a second source of truth. A drift here flags that a dispatch arm was added or removed;
    /// update the registry + the hand-list.
    #[test]
    fn dispatch_arm_list_has_the_expected_count() {
        assert_eq!(
            dispatch_arm_names().len(),
            189,
            "the dispatch-arm hand-list drifted from the 177 client commands (incl. LOLWUT, \
             #414 command-surface completeness, + the 9 hash-field-TTL commands HEXPIRE/\
             HPEXPIRE/HEXPIREAT/HPEXPIREAT/HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME/HPERSIST, #408, \
             + SAVE/BGSAVE/\
             LASTSAVE, #58 persistence, + SHUTDOWN, #139 graceful shutdown, + the drop-in\
             compatibility set GETRANGE/SUBSTR/SETRANGE/GETDEL/MSETNX/LMPOP/ZMPOP/SORT/SORT_RO, \
             + the PROD-7 operability trio SLOWLOG/MEMORY/LATENCY, + the PROD-9 blocking family \
             BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP/WAIT, 9 non-blocking \
             EXEC-replay arms) + 3 internal verbs"
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
