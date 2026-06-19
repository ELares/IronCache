// SPDX-License-Identifier: MIT OR Apache-2.0
//! The canonical Redis-compatible error catalog (ERRORS.md, #18).
//!
//! Error strings are part of the wire contract: clients pattern-match on the
//! leading uppercase token (and sometimes the full text) to drive control flow.
//! This module is the single source of error text; no call site hand-writes an
//! error string (ERRORS.md "internal mapping" rule). The serializer renders
//! `-<TOKEN> <message>\r\n` from an [`ErrorReply`].
//!
//! ## Freeze point
//!
//! [`ErrorCode`] (the leading tokens) and the verbatim strings produced by
//! [`ErrorReply`] are a freeze point. The handshake-critical and control-flow
//! strings are pinned byte-exact against the Valkey/Redis oracle and covered by
//! table tests; do not edit them without updating the oracle pin (ERRORS.md
//! "fidelity rule").

use core::fmt::Write as _;

/// The canonical leading error tokens (ERRORS.md "canonical prefixes"). Each
/// renders as the uppercase token at the start of an error reply. Clients switch
/// on these tokens, so the set and spelling are part of the contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Generic error: unknown command, arity, syntax, parse failures.
    Err,
    /// Operation against a key holding the wrong kind of value.
    WrongType,
    /// Unsupported `HELLO` protocol version.
    NoProto,
    /// Authentication required.
    NoAuth,
    /// Invalid username/password pair or disabled user.
    WrongPass,
    /// ACL permission denied.
    NoPerm,
    /// Transaction discarded due to previous errors.
    ExecAbort,
    /// Command not allowed while out of memory (write under maxmemory).
    Oom,
    /// Key already exists (e.g. `RESTORE` without `REPLACE`).
    BusyKey,
    /// Reserved/unused: there is NO canonical `-OUTOFRANGE` leading token in
    /// Redis. Index/offset out-of-range and `SELECT` use the plain `ERR` token
    /// (`ERR value is not an integer or out of range`, `ERR index out of range`,
    /// `ERR DB index is out of range`), so this variant has no live constructor.
    /// It is kept only to avoid churning the freeze-point enum discriminants; do
    /// not introduce a constructor that emits `-OUTOFRANGE`.
    OutOfRange,
    /// `SCRIPT KILL` / `FUNCTION KILL` with nothing currently running
    /// (`-NOTBUSY No scripts in execution right now.`). NOT `UNWATCH`/`DISCARD`:
    /// those reply with the plain `ERR` token, not `NOTBUSY`.
    NotBusy,
    /// `-MOVED <slot> <ip:port>` cluster redirection: the slot is permanently owned by
    /// another node (CLUSTER_CONTRACT.md #70, slice 2). The client updates its cached slot
    /// map and retries at the advertised address.
    Moved,
    /// `-CROSSSLOT ...` a multi-key command whose keys do not all hash to one slot, rejected
    /// in cluster mode (CLUSTER_CONTRACT.md #70, slice 2) rather than scattered.
    CrossSlot,
    /// `-CLUSTERDOWN ...` a slot is unassigned / not served (the cluster is not fully
    /// covered). Slice-2 validation requires a complete static map, so this is reachable only
    /// on a (currently rejected) partial map; the constructor exists for completeness.
    ClusterDown,
    /// `-ASK <slot> <ip:port>` the TRANSIENT cluster redirection (HA-6 online slot migration):
    /// the slot is MIGRATING and the requested key has already moved to the destination. UNLIKE
    /// MOVED, the client does NOT update its slot map (ownership has not changed); it sends
    /// `ASKING` then the command ONCE to the destination. Verified against redis/redis
    /// `src/cluster.c` (`clusterRedirectClient`, `"-ASK %d %s:%d"`).
    Ask,
    /// `-TRYAGAIN ...` (HA-6): a MULTI-KEY command on a MIGRATING slot whose keys are SPLIT (some
    /// already migrated to the destination, some still local) cannot be served atomically on either
    /// side, so the client is asked to retry shortly (the migration will converge). Verified against
    /// redis/redis `src/cluster.c` (`"-TRYAGAIN Multiple keys request during rehashing of slot"`).
    TryAgain,
    /// `-NOREPLICAS Not enough good replicas to write.` (ADR-0026, the WRITE-SIDE replication
    /// guardrail): a `min-replicas-to-write` owner rejects a write because fewer than the required
    /// number of replicas are currently in sync. Verified against redis/redis `src/server.c`
    /// (`shared.noreplicaserr`, `"-NOREPLICAS Not enough good replicas to write."`).
    NoReplicas,
}

impl ErrorCode {
    /// The uppercase leading token, byte-identical to Valkey.
    #[must_use]
    pub const fn token(self) -> &'static str {
        match self {
            ErrorCode::Err => "ERR",
            ErrorCode::WrongType => "WRONGTYPE",
            ErrorCode::NoProto => "NOPROTO",
            ErrorCode::NoAuth => "NOAUTH",
            ErrorCode::WrongPass => "WRONGPASS",
            ErrorCode::NoPerm => "NOPERM",
            ErrorCode::ExecAbort => "EXECABORT",
            ErrorCode::Oom => "OOM",
            ErrorCode::BusyKey => "BUSYKEY",
            ErrorCode::OutOfRange => "OUTOFRANGE",
            ErrorCode::NotBusy => "NOTBUSY",
            ErrorCode::Moved => "MOVED",
            ErrorCode::CrossSlot => "CROSSSLOT",
            ErrorCode::ClusterDown => "CLUSTERDOWN",
            ErrorCode::Ask => "ASK",
            ErrorCode::TryAgain => "TRYAGAIN",
            ErrorCode::NoReplicas => "NOREPLICAS",
        }
    }
}

/// A fully-formed error reply: a [`ErrorCode`] plus the message text that follows
/// it. The complete on-wire line is `-<token> <message>\r\n`; [`ErrorReply::line`]
/// returns the `-...` portion without the trailing CRLF (the encoder appends it).
///
/// Construct these only through the catalog constructors below so the verbatim
/// strings stay in one place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorReply {
    code: ErrorCode,
    message: String,
}

impl ErrorReply {
    /// The leading token.
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// The message text that follows the token (no token, no CRLF).
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The full error line `-<token> <message>` WITHOUT the trailing CRLF.
    /// The encoder ([`crate::encode`]) is responsible for the CRLF and for
    /// escaping CR/LF inside the text (RESP simple/error strings cannot contain
    /// a raw newline).
    #[must_use]
    pub fn line(&self) -> String {
        format!("-{} {}", self.code.token(), self.message)
    }

    /// Build directly from a code and an already-correct message. Internal to the
    /// catalog; external callers use the named constructors so wording is pinned.
    fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        ErrorReply {
            code,
            message: message.into(),
        }
    }

    // -- Pinned, handshake-critical and control-flow strings (byte-exact). --

    /// `ERR unknown command '<name>', with args beginning with: '<a>' '<b>' `.
    ///
    /// Byte-exact to Redis `server.c` `unknownCommand`: the command name is
    /// single-quoted (truncated to 128 bytes), then each argument is rendered
    /// `'<value>' ` (single quote, value, single quote, trailing SPACE) with NO
    /// comma separators. Redis accumulates args only while the running
    /// accumulated-args length stays below a 128-byte budget over the WHOLE args
    /// string (`while sdslen(args) < 128`), and each appended arg's value is
    /// itself truncated to the remaining budget; there is no fixed arg-count cap.
    /// Clients pattern-match the `unknown command` phrase during handshake
    /// fallback.
    #[must_use]
    pub fn unknown_command(name: &str, args: &[&[u8]]) -> Self {
        // Command name truncated to 128 bytes (on a char boundary so the lossy
        // string stays valid UTF-8).
        let name_trunc = truncate_str(name, 128);

        // 128-byte budget over the whole accumulated args string, matching Redis's
        // `while (sdslen(args) < 128)` loop. Each arg is appended as `'<value>' `;
        // the value is truncated to the budget remaining before this arg.
        let mut shown = String::new();
        for a in args {
            if shown.len() >= 128 {
                break;
            }
            let remaining = 128 - shown.len();
            let text = String::from_utf8_lossy(&a[..a.len().min(remaining)]);
            let _ = write!(shown, "'{text}' ");
        }
        let mut s = String::new();
        let _ = write!(
            s,
            "unknown command '{name_trunc}', with args beginning with: {shown}"
        );
        ErrorReply::new(ErrorCode::Err, s)
    }

    /// `ERR wrong number of arguments for '<command>' command`.
    #[must_use]
    pub fn wrong_arity(command: &str) -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            format!("wrong number of arguments for '{command}' command"),
        )
    }

    /// `ERR string exceeds maximum allowed size (proto-max-bulk-len)`.
    ///
    /// Byte-exact to Redis `checkStringLength` (src/t_string.c): returned when a write
    /// (e.g. APPEND) would grow a string value past the `proto-max-bulk-len` ceiling
    /// (512 MB default). Matching Redis here both preserves compatibility AND keeps every
    /// single value below 4 GiB, which the store's manual Str-blob allocator relies on
    /// (its u32 length prefix must not truncate).
    #[must_use]
    pub fn string_exceeds_max() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "string exceeds maximum allowed size (proto-max-bulk-len)",
        )
    }

    /// `WRONGTYPE Operation against a key holding the wrong kind of value`.
    ///
    /// Note: the pinned Valkey wording is the SINGULAR "Operation"; ERRORS.md's
    /// prose example says "Operations", which is a doc typo. The fidelity rule
    /// ("byte-identical to Valkey") governs, so we emit the singular form.
    #[must_use]
    pub fn wrong_type() -> Self {
        ErrorReply::new(
            ErrorCode::WrongType,
            "Operation against a key holding the wrong kind of value",
        )
    }

    /// `NOPROTO unsupported protocol version` (the pinned Valkey wording for an
    /// out-of-range `HELLO` version).
    #[must_use]
    pub fn noproto() -> Self {
        ErrorReply::new(ErrorCode::NoProto, "unsupported protocol version")
    }

    /// `NOAUTH Authentication required.`
    #[must_use]
    pub fn noauth() -> Self {
        ErrorReply::new(ErrorCode::NoAuth, "Authentication required.")
    }

    /// `WRONGPASS invalid username-password pair or user is disabled.`
    #[must_use]
    pub fn wrongpass() -> Self {
        ErrorReply::new(
            ErrorCode::WrongPass,
            "invalid username-password pair or user is disabled.",
        )
    }

    /// `NOPERM User <user> has no permissions to run the '<cmd>' command` - the reply Redis
    /// emits (src/acl.c `ACLCheckAllPerm` -> `ACL_DENIED_CMD`) when an authenticated ACL
    /// user runs a command its command permissions deny (#106). Byte-compatible with Redis
    /// 7's wording. `user` and `cmd` are the authenticated username and the lowercased
    /// command token; neither is a secret.
    #[must_use]
    pub fn noperm_command(user: &str, cmd: &str) -> Self {
        ErrorReply::new(
            ErrorCode::NoPerm,
            format!("User {user} has no permissions to run the '{cmd}' command"),
        )
    }

    /// `NOPERM No permissions to access a key` - the reply Redis emits (src/acl.c
    /// `ACL_DENIED_KEY`) when an authenticated ACL user touches a key its key patterns do
    /// not allow (#106). Byte-compatible with Redis 7's wording.
    #[must_use]
    pub fn noperm_key() -> Self {
        ErrorReply::new(ErrorCode::NoPerm, "No permissions to access a key")
    }

    /// `NOPERM No permissions to access a channel` - the reply Redis emits (src/acl.c
    /// `ACL_DENIED_CHANNEL`) when an authenticated ACL user (un)subscribes to / publishes
    /// on a channel its channel patterns do not allow (#106).
    #[must_use]
    pub fn noperm_channel() -> Self {
        ErrorReply::new(ErrorCode::NoPerm, "No permissions to access a channel")
    }

    /// `EXECABORT Transaction discarded because of previous errors.`
    #[must_use]
    pub fn exec_abort() -> Self {
        ErrorReply::new(
            ErrorCode::ExecAbort,
            "Transaction discarded because of previous errors.",
        )
    }

    /// `ERR EXEC without MULTI` - the reply Redis emits (src/multi.c `execCommand`
    /// -> `addReplyError(c,"EXEC without MULTI")`) when EXEC is issued while the
    /// connection is NOT inside a transaction. Byte-exact (the plain `ERR` token, no
    /// trailing period). Clients use it to detect a stray EXEC.
    #[must_use]
    pub fn exec_without_multi() -> Self {
        ErrorReply::new(ErrorCode::Err, "EXEC without MULTI")
    }

    /// `ERR DISCARD without MULTI` - the reply Redis emits (src/multi.c
    /// `discardCommand` -> `addReplyError(c,"DISCARD without MULTI")`) when DISCARD is
    /// issued while the connection is NOT inside a transaction. Byte-exact (the plain
    /// `ERR` token, no trailing period). The DISCARD analog of
    /// [`Self::exec_without_multi`].
    #[must_use]
    pub fn discard_without_multi() -> Self {
        ErrorReply::new(ErrorCode::Err, "DISCARD without MULTI")
    }

    /// `ERR MULTI calls can not be nested` - the reply Redis emits (src/multi.c
    /// `multiCommand` -> `addReplyError(c,"MULTI calls can not be nested")`) when MULTI
    /// is issued while the connection is ALREADY inside a transaction. Byte-exact (note
    /// the two-word "can not", matching Redis verbatim). The transaction state and queue
    /// are left unchanged; the connection stays in MULTI.
    #[must_use]
    pub fn multi_nested() -> Self {
        ErrorReply::new(ErrorCode::Err, "MULTI calls can not be nested")
    }

    /// `ERR WATCH inside MULTI is not allowed` - the reply Redis emits (src/multi.c
    /// `watchCommand` -> `addReplyError(c,"WATCH inside MULTI is not allowed")`) when
    /// WATCH is issued while the connection is ALREADY inside a transaction. Byte-exact
    /// (the plain `ERR` token). The transaction is left OPEN and CLEAN: WATCH inside
    /// MULTI does NOT dirty the batch (it is rejected before the queue block and does not
    /// call flagTransaction), so a following EXEC still runs.
    #[must_use]
    pub fn watch_inside_multi() -> Self {
        ErrorReply::new(ErrorCode::Err, "WATCH inside MULTI is not allowed")
    }

    /// `ERR a queued command references a key on another shard; cross-shard
    /// transactions are not supported yet` - a TEMPORARY limitation of the
    /// cross-shard coordinator (COORDINATOR.md #107).
    ///
    /// Emitted at QUEUE time (inside a `MULTI`) when a keyed command's key(s) are
    /// not all owned by the connection's home shard. A correct transaction must
    /// reach EXEC with EVERY watched key and EVERY queued command's key home-owned,
    /// so home-only EXEC is always correct; a command that would violate that is
    /// rejected NOW and the transaction is dirtied (a later EXEC returns -EXECABORT
    /// and applies nothing), rather than silently executing eagerly + out of order.
    ///
    /// IronCache presents as a SINGLE NODE (not a cluster), so this is deliberately
    /// NOT Redis's `-CROSSSLOT` (which is a cluster-slot contract a client can
    /// observe); it is a plain `ERR` describing the temporary limitation. The guard
    /// is removed once Stage 3 (txid + ordered cross-shard apply) lands; with
    /// `shards == 1` every key is home-owned, so it never fires.
    #[must_use]
    pub fn txn_cross_shard_command() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "a queued command references a key on another shard; cross-shard transactions are not supported yet",
        )
    }

    /// `ERR WATCH of a key on another shard is not supported yet` - the companion
    /// TEMPORARY limitation for `WATCH` of a key owned by a remote shard
    /// (COORDINATOR.md #107). A cross-shard WATCH would snapshot the WRONG (home)
    /// store, so the dirty-CAS at EXEC would be meaningless; we reject the WATCH
    /// loudly instead and leave the connection un-watched (a following MULTI/EXEC
    /// still works). Like [`Self::txn_cross_shard_command`] this is a plain `ERR`
    /// (not `-CROSSSLOT`): IronCache is single-node, and the guard is removed by the
    /// Stage 3 cross-shard transaction work. With `shards == 1` every key is
    /// home-owned, so it never fires.
    #[must_use]
    pub fn watch_cross_shard() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "WATCH of a key on another shard is not supported yet",
        )
    }

    /// `ERR a whole-keyspace command in a transaction is not supported across shards
    /// yet` - the companion TEMPORARY limitation for a WHOLE-KEYSPACE command
    /// (KEYS/SCAN/DBSIZE/FLUSHALL/FLUSHDB/RANDOMKEY) queued inside `MULTI`
    /// (COORDINATOR.md #107). Outside a transaction these SCATTER-GATHER across all
    /// shards; inside `MULTI`, `EXEC` replays synchronously on the HOME store only, so
    /// they would return a PARTIAL (~1/N) result (a `MULTI; FLUSHALL; EXEC` would flush
    /// only the home partition -- a silent partial flush). We reject them loudly at
    /// queue time (dirtying the transaction, so `EXEC` returns `-EXECABORT`) rather than
    /// return a partial result. A plain `ERR` (not `-CROSSSLOT`): IronCache is
    /// single-node, and the guard is removed by the Stage 3 cross-shard transaction
    /// work. With `shards == 1` the home shard IS the whole keyspace, so it never fires.
    #[must_use]
    pub fn txn_whole_keyspace_unsupported() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "a whole-keyspace command in a transaction is not supported across shards yet",
        )
    }

    /// `ERR Can't execute '<cmd>': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT /
    /// RESET are allowed in this context` - the reply Redis emits (src/server.c
    /// `processCommand`, the `CLIENT_PUBSUB && resp == 2` gate -> `rejectCommandFormat(c,
    /// "Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are
    /// allowed in this context", c->cmd->fullname)`) when a RESP2 connection in SUBSCRIBE
    /// mode runs a command other than the pub/sub control set + PING/QUIT/RESET.
    ///
    /// Byte-exact to current Redis (8.x / unstable): the alternations are `(P|S)` (the
    /// sharded `S` form is in the message even where sharded pub/sub is not used), and the
    /// `%s` is `c->cmd->fullname`, the canonical LOWERCASE command name. RESP3 has NO such
    /// restriction (a RESP3 subscriber may run any command), so this fires only on RESP2.
    /// Clients pattern-match the `allowed in this context` phrase.
    #[must_use]
    pub fn subscribe_mode(cmd: &str) -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            format!(
                "Can't execute '{cmd}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
            ),
        )
    }

    /// `ERR <CMD> is not allowed in transactions` - the reply IronCache emits when a
    /// SERVE-LAYER pub/sub command (SUBSCRIBE / UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE /
    /// PUBLISH / PUBSUB) is issued INSIDE a `MULTI` (SERVER_PUSH.md #20, the pub/sub-in-MULTI
    /// reject). The `cmd` is rendered UPPERCASE (e.g. `SUBSCRIBE is not allowed in
    /// transactions`).
    ///
    /// ## Deliberate divergence from current Redis (documented)
    ///
    /// In CURRENT Redis the pub/sub commands do NOT carry `CMD_NO_MULTI` (verified against
    /// the redis/redis `src/commands/*.json` flags: SUBSCRIBE/PUBLISH carry `PUBSUB` +
    /// `LOADING`/`STALE`/... but NOT `NO_MULTI`), so Redis QUEUES them and runs them at EXEC.
    /// IronCache handles pub/sub in the SERVE layer (`route_and_dispatch`), NOT in
    /// `dispatch_inner` where EXEC replays the queued batch, so EXEC CANNOT replay a pub/sub
    /// command in this build. Rather than execute them EAGERLY inside MULTI (silently wrong,
    /// out of transaction order) or queue-then-fail-at-EXEC, we REJECT them LOUDLY at queue
    /// time and dirty the transaction (so EXEC returns `-EXECABORT`): the same
    /// "correct, or explicitly aborted, never silently wrong" contract as the cross-shard
    /// in-MULTI guards. Serve-layer EXEC replay of pub/sub is the tracked follow-up that
    /// removes this divergence. The verb is rendered UPPERCASE to read as a clear
    /// per-command rejection.
    ///
    /// The historical Redis `CMD_NO_MULTI` rejection (`-ERR Command not allowed inside a
    /// transaction`, `src/server.c processCommand`) is the nearest precedent; we phrase this
    /// per-command so the client can see which verb was rejected.
    #[must_use]
    pub fn not_allowed_in_transactions(cmd: &str) -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            format!(
                "{} is not allowed in transactions",
                cmd.to_ascii_uppercase()
            ),
        )
    }

    /// `ERR AUTH <password> called without any password configured for the
    /// default user. Are you sure your configuration is correct?` - the current
    /// canonical Redis string for `AUTH` when no `requirepass`/ACL password is
    /// configured. Clients fall back on this text.
    #[must_use]
    pub fn auth_no_password_set() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
        )
    }

    // -- Generic / parser errors. --

    /// A generic `ERR <message>`.
    #[must_use]
    pub fn err(message: impl Into<String>) -> Self {
        ErrorReply::new(ErrorCode::Err, message)
    }

    /// `ERR Protocol error: <detail>` - the parser error family (ERRORS.md
    /// `-ERR Protocol error`). The detail mirrors Redis phrasings such as
    /// `invalid multibulk length`, `invalid bulk length`,
    /// `expected '$', got '...'`, `too big mbulk count string`,
    /// `unbalanced quotes in request`.
    #[must_use]
    pub fn protocol(detail: &str) -> Self {
        ErrorReply::new(ErrorCode::Err, format!("Protocol error: {detail}"))
    }

    /// `ERR value is not an integer or out of range`.
    #[must_use]
    pub fn not_an_integer() -> Self {
        ErrorReply::new(ErrorCode::Err, "value is not an integer or out of range")
    }

    /// `ERR syntax error` - the canonical reply for malformed/conflicting command
    /// options (e.g. `SET k v NX XX`, `SET k v EX 1 PX 1`, an unknown SET flag).
    /// Byte-exact to Redis `addReplyError(c, "syntax error")`.
    #[must_use]
    pub fn syntax_error() -> Self {
        ErrorReply::new(ErrorCode::Err, "syntax error")
    }

    /// `ERR invalid expire time in '<cmd>' command` - the reply Redis emits (via
    /// `addReplyErrorExpireTime`) when an EX/PX/EXAT/PXAT value is `<= 0` or
    /// overflows the millisecond computation. This is DISTINCT from a syntax error
    /// (conflicting flags) and from the not-an-integer error (a non-integer expire
    /// argument, thrown earlier): three separate error classes.
    #[must_use]
    pub fn invalid_expire_time(cmd: &str) -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            format!("invalid expire time in '{cmd}' command"),
        )
    }

    /// `ERR DB index is out of range`.
    #[must_use]
    pub fn select_out_of_range() -> Self {
        ErrorReply::new(ErrorCode::Err, "DB index is out of range")
    }

    /// `ERR NX and XX, GT or LT options at the same time are not compatible` - the
    /// reply Redis emits (src/expire.c `parseExtendedExpireArgumentsOrReply`) when the
    /// EXPIRE-family `NX` option is combined with any of `XX`/`GT`/`LT`. DISTINCT from
    /// the generic syntax error so a client can tell apart the specific incompatibility.
    #[must_use]
    pub fn expire_nx_and_xx_gt_lt() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "NX and XX, GT or LT options at the same time are not compatible",
        )
    }

    /// `ERR GT and LT options at the same time are not compatible` - the reply Redis
    /// emits (src/expire.c `parseExtendedExpireArgumentsOrReply`) when the EXPIRE-family
    /// `GT` and `LT` options are combined.
    #[must_use]
    pub fn expire_gt_and_lt() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "GT and LT options at the same time are not compatible",
        )
    }

    /// `ERR Unsupported option <opt>` - the reply Redis emits (src/expire.c
    /// `parseExtendedExpireArgumentsOrReply`) for an unrecognized EXPIRE-family option
    /// token. The token is echoed verbatim (Redis prints the raw argument).
    #[must_use]
    pub fn expire_unsupported_option(opt: &str) -> Self {
        ErrorReply::new(ErrorCode::Err, format!("Unsupported option {opt}"))
    }

    /// `ERR increment or decrement would overflow` - the reply Redis emits (via
    /// `addReplyError(c,"increment or decrement would overflow")`, src/t_string.c
    /// `incrDecrCommand`) when an INCR/DECR/INCRBY/DECRBY would carry the i64 result
    /// past the i64 range. DISTINCT from the not-an-integer error: this is a result
    /// overflow, not a parse failure. Also the reply for the `DECRBY key i64::MIN`
    /// edge (the increment cannot be negated within i64).
    #[must_use]
    pub fn increment_overflow() -> Self {
        ErrorReply::new(ErrorCode::Err, "increment or decrement would overflow")
    }

    /// `ERR value is not a valid float` - the default reply Redis emits when the
    /// stored value or the increment argument of INCRBYFLOAT cannot be parsed as a
    /// float (`getLongDoubleFromObjectOrReply` with a NULL message, src/object.c,
    /// which defaults to `addReplyError(c,"value is not a valid float")`). The
    /// float analog of [`ErrorReply::not_an_integer`].
    #[must_use]
    pub fn not_a_valid_float() -> Self {
        ErrorReply::new(ErrorCode::Err, "value is not a valid float")
    }

    /// `ERR increment would produce NaN or Infinity` - the reply Redis emits (via
    /// `addReplyError(c,"increment would produce NaN or Infinity")`, src/t_string.c
    /// `incrbyfloatCommand`) when the INCRBYFLOAT result is NaN or +/-Infinity.
    #[must_use]
    pub fn increment_nan_or_inf() -> Self {
        ErrorReply::new(ErrorCode::Err, "increment would produce NaN or Infinity")
    }

    /// `ERR hash value is not an integer` - the reply Redis emits (src/t_hash.c
    /// `hincrbyCommand` -> `addReplyError(c,"hash value is not an integer")`) when
    /// HINCRBY's stored field value is not a canonical integer. The HASH analog of
    /// [`ErrorReply::not_an_integer`] (the field value, not a command argument). A
    /// non-integer INCREMENT argument is the generic [`ErrorReply::not_an_integer`]
    /// (thrown by the argument parse, like Redis); this is specifically the stored-value
    /// class.
    #[must_use]
    pub fn hash_value_not_an_integer() -> Self {
        ErrorReply::new(ErrorCode::Err, "hash value is not an integer")
    }

    /// `ERR hash value is not a float` - the reply Redis emits (src/t_hash.c
    /// `hincrbyfloatCommand` -> `addReplyError(c,"hash value is not a float")`) when
    /// HINCRBYFLOAT's stored field value cannot be parsed as a float. The HASH analog of
    /// [`ErrorReply::not_a_valid_float`] for the stored field value. A non-float
    /// INCREMENT argument is still [`ErrorReply::not_a_valid_float`] (Redis parses the
    /// argument with `getLongDoubleFromObjectOrReply`, the generic float error); this is
    /// the stored-value class.
    #[must_use]
    pub fn hash_value_not_a_float() -> Self {
        ErrorReply::new(ErrorCode::Err, "hash value is not a float")
    }

    /// `ERR unknown subcommand or wrong number of arguments for '<sub>'. Try
    /// <PARENT> HELP.` the wording clients see for an unrecognized
    /// `CLIENT`/`COMMAND`/`CONFIG` subcommand.
    ///
    /// Byte-exact to Redis `addReplySubcommandSyntaxError`: the leading word is
    /// LOWERCASE `unknown`, the subcommand is truncated to 128 bytes (`%.128s`),
    /// and the parent command name is uppercased.
    #[must_use]
    pub fn unknown_subcommand(parent: &str, sub: &str) -> Self {
        let sub_trunc = truncate_str(sub, 128);
        ErrorReply::new(
            ErrorCode::Err,
            format!(
                "unknown subcommand or wrong number of arguments for '{sub_trunc}'. Try {} HELP.",
                parent.to_uppercase()
            ),
        )
    }

    /// `ERR Unknown HELLO option '<opt>'` the syntax error for an unrecognized
    /// `HELLO` option keyword. Pinned here so the dispatch call site does not
    /// hand-write the string (ERRORS.md "no call site hand-writes an error").
    #[must_use]
    pub fn hello_syntax_error(opt: &str) -> Self {
        ErrorReply::new(ErrorCode::Err, format!("Unknown HELLO option '{opt}'"))
    }

    /// `ERR Client names cannot contain spaces, newlines or special characters.`
    /// for `CLIENT SETNAME` (and `HELLO SETNAME`) with an invalid name.
    #[must_use]
    pub fn client_name_invalid_chars() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "Client names cannot contain spaces, newlines or special characters.",
        )
    }

    /// `ERR The command has no key arguments` for `COMMAND GETKEYS` against a
    /// command that takes no keys.
    #[must_use]
    pub fn command_no_key_args() -> Self {
        ErrorReply::new(ErrorCode::Err, "The command has no key arguments")
    }

    /// `ERR no such key` - the reply Redis emits (via `shared.nokeyerr`) when
    /// RENAME/RENAMENX's source key does not exist (src/db.c `renameGenericCommand`
    /// -> `lookupKeyWriteOrReply(c, c->argv[1], shared.nokeyerr)`). Byte-exact.
    #[must_use]
    pub fn no_such_key() -> Self {
        ErrorReply::new(ErrorCode::Err, "no such key")
    }

    /// `ERR invalid cursor` - the reply Redis emits (src/db.c
    /// `parseScanCursorOrReply` -> `addReplyError(c, "invalid cursor")`) when a SCAN
    /// family cursor is not a valid unsigned-decimal token. Byte-exact.
    #[must_use]
    pub fn invalid_cursor() -> Self {
        ErrorReply::new(ErrorCode::Err, "invalid cursor")
    }

    /// `ERR An LFU maxmemory policy is not selected, access frequency not tracked...`,
    /// the reply Redis emits (src/object.c `objectCommandGetKey` FREQ branch) when
    /// OBJECT FREQ runs WITHOUT an LFU `maxmemory-policy`. Byte-exact (the full
    /// sentence including the runtime-switch note clients may surface).
    #[must_use]
    pub fn object_freq_requires_lfu() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
        )
    }

    /// `ERR An LFU maxmemory policy is selected, idle time not tracked...`, the reply
    /// Redis emits (src/object.c OBJECT IDLETIME branch) when OBJECT IDLETIME runs
    /// UNDER an LFU `maxmemory-policy` (idle time is not meaningful under LFU).
    /// Byte-exact.
    #[must_use]
    pub fn object_idletime_under_lfu() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
        )
    }

    /// `ERR Unknown option or number of arguments for CONFIG SET - '<param>'` - the
    /// byte-exact Redis reply (src/config.c `configSetCommand`,
    /// `addReplyErrorFormat(c, "Unknown option or number of arguments for CONFIG SET - '%s'", ...)`)
    /// for a `CONFIG SET` of an unrecognized parameter (or a missing value). The param
    /// name is echoed verbatim. Clients pattern-match on the `Unknown option` phrase.
    #[must_use]
    pub fn config_set_unknown_param(param: &str) -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            format!("Unknown option or number of arguments for CONFIG SET - '{param}'"),
        )
    }

    /// `ERR CONFIG SET failed (possibly related to argument '<param>') - <reason>` -
    /// the byte-exact Redis reply (src/config.c `configSetCommand`,
    /// `addReplyErrorFormat(c, "CONFIG SET failed (possibly related to argument '%s') - %s", ...)`)
    /// for a recognized parameter whose VALUE was rejected (e.g. a malformed
    /// `maxmemory` size, or an unrecognized `maxmemory-policy` name). The `reason` is
    /// the human-readable cause.
    #[must_use]
    pub fn config_set_failed(param: &str, reason: &str) -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            format!("CONFIG SET failed (possibly related to argument '{param}') - {reason}"),
        )
    }

    /// `ERR CONFIG SET failed (possibly related to argument '<param>') - can't set
    /// immutable config` - the byte-exact Redis reply for a `CONFIG SET` of an
    /// IMMUTABLE (restart-required) parameter (`bind`/`port`/`databases`/...). Redis
    /// marks these `IMMUTABLE_CONFIG` and rejects a runtime set with this reason rather
    /// than silently ignoring it (CONFIG.md "reported as requiring a restart").
    #[must_use]
    pub fn config_set_immutable(param: &str) -> Self {
        ErrorReply::config_set_failed(param, "can't set immutable config")
    }

    /// `ERR The server is running without a config file` - the byte-exact Redis reply
    /// (src/config.c `configRewriteCommand`, `addReplyError(c, "The server is running
    /// without a config file")`) for `CONFIG REWRITE` when no config file was given at
    /// boot. IronCache currently always boots without a config-file path threaded
    /// through, so REWRITE returns this faithfully (rather than a misleading +OK stub)
    /// until the config-file path is wired (CONFIG.md).
    #[must_use]
    pub fn config_rewrite_no_file() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "The server is running without a config file",
        )
    }

    /// `ERR index out of range` - the reply Redis emits (src/t_list.c
    /// `lsetCommand` -> `addReplyError(c,"index out of range")`) when LSET targets an
    /// index outside the list. Byte-exact (the plain `ERR` token, NOT `OUTOFRANGE`).
    #[must_use]
    pub fn index_out_of_range() -> Self {
        ErrorReply::new(ErrorCode::Err, "index out of range")
    }

    /// `ERR value is out of range, must be positive` - the reply Redis emits (src/t_list.c
    /// `lpopCommand`/`rpopCommand` -> `addReplyError(c,"value is out of range, must be
    /// positive")`) when the optional LPOP/RPOP `count` argument is negative. A NON-integer
    /// count is the separate not-an-integer error; this is specifically the negative case.
    #[must_use]
    pub fn value_out_of_range_must_be_positive() -> Self {
        ErrorReply::new(ErrorCode::Err, "value is out of range, must be positive")
    }

    /// `ERR RANK can't be zero: ...` - the reply Redis emits (src/t_list.c
    /// `lposCommand` -> `addReplyError`) when LPOS is given `RANK 0`. RANK selects
    /// which match to start from (1 = first match, negative = from the tail); zero is
    /// meaningless. Byte-exact to Redis. DISTINCT from the not-an-integer error (a
    /// non-integer RANK is thrown earlier by the integer parse).
    #[must_use]
    pub fn lpos_rank_zero() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
        )
    }

    /// `ERR COUNT can't be negative` - the reply Redis emits (src/t_list.c
    /// `lposCommand`) when LPOS is given a negative `COUNT`. Byte-exact. A non-integer
    /// COUNT is the separate not-an-integer error; this is specifically the negative case.
    #[must_use]
    pub fn lpos_count_negative() -> Self {
        ErrorReply::new(ErrorCode::Err, "COUNT can't be negative")
    }

    /// `ERR MAXLEN can't be negative` - the reply Redis emits (src/t_list.c
    /// `lposCommand`) when LPOS is given a negative `MAXLEN`. Byte-exact. A non-integer
    /// MAXLEN is the separate not-an-integer error; this is specifically the negative case.
    #[must_use]
    pub fn lpos_maxlen_negative() -> Self {
        ErrorReply::new(ErrorCode::Err, "MAXLEN can't be negative")
    }

    /// `ERR numkeys should be greater than 0` - the reply Redis emits (src/t_set.c
    /// `sintercardCommand` -> `addReplyError(c, "numkeys should be greater than 0")`) when
    /// SINTERCARD's `numkeys` argument is <= 0. Byte-exact. A NON-integer numkeys is the
    /// separate not-an-integer error; this is specifically the non-positive case.
    #[must_use]
    pub fn numkeys_should_be_positive() -> Self {
        ErrorReply::new(ErrorCode::Err, "numkeys should be greater than 0")
    }

    /// `ERR Number of keys can't be greater than number of args` - the reply Redis emits
    /// (src/t_set.c `sintercardCommand`) when SINTERCARD's `numkeys` exceeds the number of
    /// key arguments actually supplied. Byte-exact.
    #[must_use]
    pub fn numkeys_greater_than_args() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "Number of keys can't be greater than number of args",
        )
    }

    /// `ERR LIMIT can't be negative` - the reply Redis emits (src/t_set.c
    /// `sintercardCommand`) when SINTERCARD's optional `LIMIT` argument is negative.
    /// Byte-exact. A NON-integer LIMIT is the separate not-an-integer error; this is
    /// specifically the negative case.
    #[must_use]
    pub fn limit_cant_be_negative() -> Self {
        ErrorReply::new(ErrorCode::Err, "LIMIT can't be negative")
    }

    /// `ERR GT, LT, and/or NX options at the same time are not compatible` - the reply
    /// Redis emits (src/t_zset.c `zaddGenericCommand`) when ZADD is given an incompatible
    /// combination of the GT/LT/NX flags (NX+GT, NX+LT, GT+LT). Byte-exact. NX+XX is a
    /// separate generic syntax error (Redis checks NX+XX before this), handled by the
    /// caller with [`Self::syntax_error`].
    #[must_use]
    pub fn zadd_gt_lt_nx_incompatible() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "GT, LT, and/or NX options at the same time are not compatible",
        )
    }

    /// `ERR INCR option supports a single increment-element pair` - the reply Redis emits
    /// (src/t_zset.c `zaddGenericCommand`) when ZADD INCR is given more than one
    /// score-member pair. Byte-exact.
    #[must_use]
    pub fn zadd_incr_single_pair() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "INCR option supports a single increment-element pair",
        )
    }

    /// `ERR min or max is not a float` - the reply Redis emits (src/t_zset.c
    /// `zslParseRange`) when a ZRANGEBYSCORE / ZCOUNT / ZREMRANGEBYSCORE / ZRANGE BYSCORE
    /// score-range bound (`min`/`max`) does not parse as a float (after stripping an
    /// optional `(` exclusive prefix and accepting `+inf`/`-inf`). Byte-exact.
    #[must_use]
    pub fn min_or_max_not_a_float() -> Self {
        ErrorReply::new(ErrorCode::Err, "min or max is not a float")
    }

    /// `ERR min or max not valid string range item` - the reply Redis emits (src/t_zset.c
    /// `zslParseLexRange`) when a ZRANGEBYLEX / ZLEXCOUNT / ZREMRANGEBYLEX / ZRANGE BYLEX
    /// lex-range bound is not a valid lex item (it must be `-`, `+`, or start with `[` or
    /// `(`). Byte-exact.
    #[must_use]
    pub fn min_or_max_not_valid_string_range() -> Self {
        ErrorReply::new(ErrorCode::Err, "min or max not valid string range item")
    }

    /// `ERR weight value is not a float` - the reply Redis emits (src/t_zset.c
    /// `zunionInterDiffGenericCommand`) when a ZUNIONSTORE / ZINTERSTORE / ZUNION / ZINTER
    /// `WEIGHTS` value does not parse as a float. Byte-exact.
    #[must_use]
    pub fn weight_not_a_float() -> Self {
        ErrorReply::new(ErrorCode::Err, "weight value is not a float")
    }

    /// `ERR syntax error, WITHSCORES not supported in combination with BYLEX` - the reply
    /// Redis emits (src/t_zset.c `genericZrangebyscoreCommand` path / `zrangeGenericCommand`)
    /// when ZRANGE is given both BYLEX and WITHSCORES (a lex range carries no scores).
    /// DISTINCT from the generic syntax error so a client can tell the specific conflict.
    /// Byte-exact.
    #[must_use]
    pub fn zrange_withscores_not_with_bylex() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "syntax error, WITHSCORES not supported in combination with BYLEX",
        )
    }

    /// `ERR syntax error, LIMIT is only supported in combination with either BYSCORE or
    /// BYLEX` - the reply Redis emits (src/t_zset.c `zrangeGenericCommand`) when ZRANGE /
    /// ZRANGESTORE is given LIMIT without BYSCORE or BYLEX (LIMIT is meaningless for an
    /// index range). DISTINCT from the generic syntax error. Byte-exact.
    #[must_use]
    pub fn zrange_limit_only_with_byscore_or_bylex() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        )
    }

    /// `ERR syntax error` reused for the ZADD `INCR` no-op nil and conflicting-options
    /// cases is NOT this; this is the dedicated ZADD `nan` score path which Redis reports
    /// as the generic not-a-valid-float. (Kept as a named helper so the zset command code
    /// reads clearly; it delegates to the existing [`Self::not_a_valid_float`] message,
    /// which is byte-identical to Redis's ZADD bad-score reply.)
    #[must_use]
    pub fn zadd_score_not_a_float() -> Self {
        ErrorReply::not_a_valid_float()
    }

    /// `ERR resulting score is not a number (NaN)` - the reply Redis emits (src/t_zset.c
    /// `zaddGenericCommand`, INCR path) when a ZINCRBY / ZADD INCR would produce a NaN
    /// score (an existing `+inf` incremented by `-inf`, or vice versa). Redis returns this
    /// WITHOUT mutating the member. DISTINCT from the bad-score-input not-a-valid-float
    /// error: this is the resulting-score-is-NaN arithmetic case. Byte-exact.
    #[must_use]
    pub fn zadd_score_is_nan() -> Self {
        ErrorReply::new(ErrorCode::Err, "resulting score is not a number (NaN)")
    }

    /// `ERR One or more scalars selected for the SORT operation are not numbers.` - the
    /// reply Redis emits (src/sort.c `sortCommand`) when a NUMERIC (non-ALPHA) SORT
    /// encounters an element (or BY weight) that does not parse as a double. Byte-exact.
    #[must_use]
    pub fn sort_not_numbers() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "One or more scalars selected for the SORT operation are not numbers.",
        )
    }

    /// `ERR bit offset is not an integer or out of range` - the reply Redis emits
    /// (src/bitops.c `getBitOffsetFromArgument`) when a SETBIT/GETBIT/BITFIELD bit
    /// offset is not a non-negative integer, or would grow the value past the
    /// proto-max-bit-offset ceiling (2^32 bits, the 512 MB string limit). Byte-exact.
    /// This is the error that guards against a huge unbounded allocation. DISTINCT from
    /// the generic not-an-integer error (which is for non-offset integer arguments).
    #[must_use]
    pub fn bit_offset_out_of_range() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "bit offset is not an integer or out of range",
        )
    }

    /// `ERR bit is not an integer or out of range` - the reply Redis emits (src/bitops.c
    /// `setbitCommand`) when a SETBIT value is not exactly 0 or 1. Byte-exact. DISTINCT
    /// from [`Self::bit_offset_out_of_range`] (the OFFSET) and the generic
    /// not-an-integer error. NOTE: BITPOS does NOT reuse this string; a BITPOS bit
    /// argument that is non-integer is the generic [`Self::not_an_integer`], and a
    /// parsed-but-not-0/1 value is [`Self::bitpos_bit_arg`] ("The bit argument must be
    /// 1 or 0.").
    #[must_use]
    pub fn bit_not_integer_or_range() -> Self {
        ErrorReply::new(ErrorCode::Err, "bit is not an integer or out of range")
    }

    /// `ERR The bit argument must be 1 or 0.` - the reply Redis emits (src/bitops.c
    /// `bitposCommand`) when the BITPOS `bit` argument PARSES as an integer but is not
    /// exactly 0 or 1 (e.g. `2`, `-1`). Byte-exact (note the trailing period). A
    /// non-integer / leading-zero bit argument is the earlier generic
    /// [`Self::not_an_integer`] (the integer parse fails first).
    #[must_use]
    pub fn bitpos_bit_arg() -> Self {
        ErrorReply::new(ErrorCode::Err, "The bit argument must be 1 or 0.")
    }

    /// `ERR BITOP NOT must be called with a single source key.` - the reply Redis emits
    /// (src/bitops.c `bitopCommand`) when BITOP NOT is given more than one source key.
    /// Byte-exact (note the trailing period). NOT inverts exactly one source.
    #[must_use]
    pub fn bitop_not_single_source() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "BITOP NOT must be called with a single source key.",
        )
    }

    /// `ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not
    /// supported but i64 is.` - the reply Redis emits (src/bitops.c
    /// `getBitfieldTypeFromArgument`) for a malformed or out-of-range BITFIELD `i<N>` /
    /// `u<N>` type token. Byte-exact (the full instructional sentence clients surface).
    #[must_use]
    pub fn invalid_bitfield_type() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
        )
    }

    /// `ERR Invalid OVERFLOW type specified` - the reply Redis emits (src/bitops.c
    /// `bitfieldGeneric`) for a BITFIELD `OVERFLOW` keyword that is not WRAP/SAT/FAIL.
    /// Byte-exact.
    #[must_use]
    pub fn bitfield_invalid_overflow() -> Self {
        ErrorReply::new(ErrorCode::Err, "Invalid OVERFLOW type specified")
    }

    /// `ERR BITFIELD_RO only supports the GET subcommand` - the reply Redis emits
    /// (src/bitops.c `bitfieldGeneric` in the read-only path) when BITFIELD_RO is given a
    /// SET / INCRBY / OVERFLOW subcommand. Byte-exact. The read-only variant rejects any
    /// write op.
    #[must_use]
    pub fn bitfield_ro_no_writes() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "BITFIELD_RO only supports the GET subcommand",
        )
    }

    /// `WRONGTYPE Key is not a valid HyperLogLog string value.` - the reply Redis emits
    /// (src/hyperloglog.c `isHLLObjectOrReply`) when a PFADD / PFCOUNT / PFMERGE key
    /// holds a STRING that is not a valid HyperLogLog object (bad magic / encoding /
    /// length). The leading token is `WRONGTYPE` (the same code as the wrong-type error),
    /// so [`ErrorReply::line`] renders `-WRONGTYPE Key is not a valid HyperLogLog string
    /// value.`; the message passed here carries NO `WRONGTYPE` prefix (the code token is
    /// prepended by `line`, exactly as [`Self::wrong_type`] relies on). The trailing
    /// period is part of the canonical Redis string. Byte-exact.
    #[must_use]
    pub fn hll_invalid_value() -> Self {
        ErrorReply::new(
            ErrorCode::WrongType,
            "Key is not a valid HyperLogLog string value.",
        )
    }

    /// `OOM command not allowed when used memory > 'maxmemory'.` - the byte-exact
    /// Redis reply for a `denyoom` write rejected at the memory ceiling (ADMISSION.md
    /// OOM-write contract, ADR-0007). Emitted in cache mode when eviction cannot free
    /// enough, and always in strict datastore mode (`noeviction`) at capacity.
    ///
    /// Verified against redis/redis `src/server.c` `OOM_COMMAND_NOT_ALLOWED`:
    /// `"-OOM command not allowed when used memory > 'maxmemory'.\r\n"` (leading OOM
    /// token, single-quoted `maxmemory`, trailing period). Clients pattern-match the
    /// OOM token, so the spelling is part of the contract.
    #[must_use]
    pub fn oom() -> Self {
        ErrorReply::new(
            ErrorCode::Oom,
            "command not allowed when used memory > 'maxmemory'.",
        )
    }

    /// `ERR This instance has cluster support disabled` - the reply a real Redis emits
    /// (src/cluster.c `clusterCommand`, the `server.cluster_enabled == 0` gate) for a
    /// CLUSTER subcommand that requires cluster mode (MEET / ADDSLOTS / SETSLOT / FORGET /
    /// REPLICATE / FAILOVER / RESET / ... and the other mutating/cluster-only verbs) when
    /// the server runs with `cluster-enabled no`. Byte-exact (plain `ERR` token, no
    /// trailing period). IronCache slice 1 (CLUSTER_CONTRACT.md #70) is cluster-DISABLED,
    /// so it matches this Redis behavior exactly; the introspection subcommands
    /// (KEYSLOT / MYID / INFO / SLOTS / SHARDS / NODES / COUNTKEYSINSLOT / ...) still
    /// reply normally, like a real `cluster-enabled no` instance. NO MOVED/ASK/CROSSSLOT
    /// redirection codes are added in slice 1 (those are a later slice); this is the only
    /// cluster-specific error this slice introduces.
    #[must_use]
    pub fn cluster_disabled() -> Self {
        ErrorReply::new(ErrorCode::Err, "This instance has cluster support disabled")
    }

    /// `-MOVED <slot> <ip:port>` - the permanent cluster redirection a node returns when a
    /// keyed command's slot is owned by ANOTHER node (CLUSTER_CONTRACT.md #70, slice 2). The
    /// client updates its cached slot map and retries at `<addr>`.
    ///
    /// Verified against redis/redis `src/cluster.c` (`clusterRedirectClient`,
    /// `"-%s %d %s:%d"` with `"MOVED"`): the slot is rendered as a DECIMAL integer and the
    /// address is the unbracketed `ip:port` (the caller formats `host:port`). No trailing
    /// period. Clients pattern-match the `MOVED` token to drive their slot-cache refresh.
    #[must_use]
    pub fn moved(slot: u16, addr: &str) -> Self {
        ErrorReply::new(ErrorCode::Moved, format!("{slot} {addr}"))
    }

    /// `-CROSSSLOT Keys in request don't hash to the same slot` - the reply a node returns
    /// for a multi-key command whose keys do not all resolve to one slot, in cluster mode
    /// (CLUSTER_CONTRACT.md #70, slice 2). Best-effort scatter is rejected because it violates
    /// the client's atomicity expectation.
    ///
    /// Byte-exact to redis/redis `src/cluster.c` (`"-CROSSSLOT Keys in request don't hash to
    /// the same slot"`): the contraction `don't`, and NO trailing period. Clients pattern-match
    /// the `CROSSSLOT` token.
    #[must_use]
    pub fn crossslot() -> Self {
        ErrorReply::new(
            ErrorCode::CrossSlot,
            "Keys in request don't hash to the same slot",
        )
    }

    /// `-ASK <slot> <ip:port>` - the TRANSIENT cluster redirection a node returns for a key that
    /// has already migrated to `addr` while `slot` is MIGRATING (HA-6 online slot migration). The
    /// client sends `ASKING` then re-issues the command ONCE at `addr`, WITHOUT updating its cached
    /// slot map (ownership is unchanged until the committed FLIP; only then does the node serve
    /// MOVED). The wire shape mirrors MOVED exactly (`<slot> <ip:port>`); only the leading token
    /// differs (ASK vs MOVED), which is how a client distinguishes the one-time hint from the
    /// permanent redirect.
    ///
    /// Verified against redis/redis `src/cluster.c` (`clusterRedirectClient`, `"-%s %d %s:%d"` with
    /// `"ASK"`): the slot is a DECIMAL integer, the address the unbracketed `ip:port`, no trailing
    /// period. Clients pattern-match the `ASK` token.
    #[must_use]
    pub fn ask(slot: u16, addr: &str) -> Self {
        ErrorReply::new(ErrorCode::Ask, format!("{slot} {addr}"))
    }

    /// `-TRYAGAIN Multiple keys request during rehashing of slot` - the reply for a MULTI-KEY
    /// command on a MIGRATING slot whose keys are SPLIT across the source and destination (HA-6):
    /// some keys have already migrated, some have not, so the command cannot run atomically on
    /// either node. The client retries shortly (the migration converges, after which all keys are
    /// on one side and the command serves or ASKs cleanly).
    ///
    /// Byte-exact to redis/redis `src/cluster.c` (`"-TRYAGAIN Multiple keys request during rehashing
    /// of slot"`): no trailing period. Clients pattern-match the `TRYAGAIN` token.
    #[must_use]
    pub fn tryagain() -> Self {
        ErrorReply::new(
            ErrorCode::TryAgain,
            "Multiple keys request during rehashing of slot",
        )
    }

    /// `-CLUSTERDOWN Hash slot not served` - the reply a node returns when the addressed slot
    /// has no owner (the cluster is not fully covered). Slice-2 validation requires a COMPLETE
    /// static map (a gap hard-fails boot), so this is reachable only on a partial map; the
    /// constructor exists so the slot lookup has a faithful error to return if a gap is ever
    /// permitted.
    ///
    /// Byte-exact to redis/redis `src/cluster.c` (`"-CLUSTERDOWN Hash slot not served"`).
    #[must_use]
    pub fn clusterdown_slot_unserved() -> Self {
        ErrorReply::new(ErrorCode::ClusterDown, "Hash slot not served")
    }

    /// `-CLUSTERDOWN <message>` with a CUSTOM message (HA-4c). Used by the raft-mode CLUSTER
    /// mutator redirect: a node that is not the current Raft leader cannot commit a config
    /// change, so it replies `-CLUSTERDOWN ...` and the client retries against the leader (the
    /// usual way a client discovers the leader in a forming cluster). The `-CLUSTERDOWN` code is
    /// the Redis-idiomatic "the cluster cannot serve this right now, retry" signal.
    #[must_use]
    pub fn clusterdown(message: impl Into<String>) -> Self {
        ErrorReply::new(ErrorCode::ClusterDown, message)
    }

    /// `-NOREPLICAS Not enough good replicas to write.` - the WRITE-SIDE replication guardrail
    /// reply (Redis `min-replicas-to-write`, ADR-0026): an owner rejects a write because fewer
    /// than `min-replicas-to-write` replicas are currently in sync (lag within
    /// `min-replicas-max-lag`), so an acknowledged write is known to be on at least that many
    /// replicas (bounding the failover loss window).
    ///
    /// Byte-exact to redis/redis `src/server.c` (`shared.noreplicaserr`,
    /// `"-NOREPLICAS Not enough good replicas to write."`): the leading NOREPLICAS token and the
    /// trailing period are part of the contract. Clients pattern-match the NOREPLICAS token.
    #[must_use]
    pub fn no_replicas() -> Self {
        ErrorReply::new(ErrorCode::NoReplicas, "Not enough good replicas to write.")
    }
}

/// Truncate a `&str` to at most `max` bytes without splitting a UTF-8 char (so
/// the result stays valid UTF-8). Used to mirror Redis's `%.128s` byte caps on
/// command/subcommand display names.
fn truncate_str(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_handshake_strings_are_byte_exact() {
        assert_eq!(
            ErrorReply::wrong_type().line(),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(
            ErrorReply::noauth().line(),
            "-NOAUTH Authentication required."
        );
        assert_eq!(
            ErrorReply::wrongpass().line(),
            "-WRONGPASS invalid username-password pair or user is disabled."
        );
        assert_eq!(
            ErrorReply::exec_abort().line(),
            "-EXECABORT Transaction discarded because of previous errors."
        );
        assert_eq!(
            ErrorReply::noproto().line(),
            "-NOPROTO unsupported protocol version"
        );
        assert_eq!(
            ErrorReply::auth_no_password_set().line(),
            "-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?"
        );
    }

    #[test]
    fn transaction_control_strings_are_byte_exact() {
        // Verified against redis/redis src/multi.c: the stray-EXEC, stray-DISCARD, and
        // nested-MULTI control errors. All use the plain `ERR` token (NOT EXECABORT,
        // which is the dirtied-batch reply tested above). Note "can not" is two words,
        // matching Redis verbatim.
        assert_eq!(
            ErrorReply::exec_without_multi().line(),
            "-ERR EXEC without MULTI"
        );
        assert_eq!(
            ErrorReply::discard_without_multi().line(),
            "-ERR DISCARD without MULTI"
        );
        assert_eq!(
            ErrorReply::multi_nested().line(),
            "-ERR MULTI calls can not be nested"
        );
        // WATCH inside MULTI (PR-10b): verified against redis/redis src/multi.c
        // watchCommand. Plain `ERR` token; does NOT dirty the txn (the caller leaves the
        // transaction open + clean on this error).
        assert_eq!(
            ErrorReply::watch_inside_multi().line(),
            "-ERR WATCH inside MULTI is not allowed"
        );
        assert_eq!(ErrorReply::exec_without_multi().code(), ErrorCode::Err);
        assert_eq!(ErrorReply::discard_without_multi().code(), ErrorCode::Err);
        assert_eq!(ErrorReply::multi_nested().code(), ErrorCode::Err);
        assert_eq!(ErrorReply::watch_inside_multi().code(), ErrorCode::Err);
    }

    #[test]
    fn cross_shard_transaction_strings_are_byte_exact() {
        // The two TEMPORARY cross-shard transaction limitation errors (COORDINATOR.md
        // #107): an in-MULTI command whose key is owned by a remote shard, and a WATCH of
        // a remote-owned key before MULTI. Both use the plain `ERR` token (IronCache is
        // single-node, NOT a cluster, so deliberately NOT `-CROSSSLOT`); the leading code
        // is prepended by `line`, so the message carries no double prefix. These are
        // removed by the Stage 3 cross-shard transaction work; with shards == 1 they never
        // fire (every key is home-owned).
        assert_eq!(
            ErrorReply::txn_cross_shard_command().line(),
            "-ERR a queued command references a key on another shard; cross-shard transactions are not supported yet"
        );
        assert_eq!(
            ErrorReply::watch_cross_shard().line(),
            "-ERR WATCH of a key on another shard is not supported yet"
        );
        assert_eq!(
            ErrorReply::txn_whole_keyspace_unsupported().line(),
            "-ERR a whole-keyspace command in a transaction is not supported across shards yet"
        );
        assert_eq!(ErrorReply::txn_cross_shard_command().code(), ErrorCode::Err);
        assert_eq!(ErrorReply::watch_cross_shard().code(), ErrorCode::Err);
        assert_eq!(
            ErrorReply::txn_whole_keyspace_unsupported().code(),
            ErrorCode::Err
        );
    }

    #[test]
    fn not_allowed_in_transactions_is_byte_exact() {
        // The pub/sub-in-MULTI reject (SERVER_PUSH.md #20): a serve-layer pub/sub command
        // inside MULTI is rejected per-command and the verb is UPPERCASED. This is a
        // documented divergence from current Redis (which queues pub/sub at EXEC, since the
        // commands do not carry CMD_NO_MULTI) because IronCache cannot replay serve-layer
        // commands at EXEC; see the constructor doc. Plain `ERR` token.
        assert_eq!(
            ErrorReply::not_allowed_in_transactions("subscribe").line(),
            "-ERR SUBSCRIBE is not allowed in transactions"
        );
        assert_eq!(
            ErrorReply::not_allowed_in_transactions("publish").line(),
            "-ERR PUBLISH is not allowed in transactions"
        );
        assert_eq!(
            ErrorReply::not_allowed_in_transactions("subscribe").code(),
            ErrorCode::Err
        );
    }

    #[test]
    fn subscribe_mode_error_is_byte_exact() {
        // Verified against redis/redis src/server.c processCommand (the CLIENT_PUBSUB &&
        // resp == 2 gate): the (P|S) alternations and the lowercase command name. RESP3 has
        // no restriction, so this fires only on RESP2 subscribers.
        assert_eq!(
            ErrorReply::subscribe_mode("get").line(),
            "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
        );
        assert_eq!(ErrorReply::subscribe_mode("get").code(), ErrorCode::Err);
    }

    #[test]
    fn unknown_command_renders_name_and_args() {
        // Canonical Redis shape: space-separated single-quoted args, trailing
        // space, no commas. For `foo` with args `a b`:
        //   ERR unknown command 'foo', with args beginning with: 'a' 'b'
        let e = ErrorReply::unknown_command("foo", &[b"a", b"b"]);
        assert_eq!(
            e.line(),
            "-ERR unknown command 'foo', with args beginning with: 'a' 'b' "
        );
        // No-arg form: the phrase is present with no args after it.
        let e0 = ErrorReply::unknown_command("BAR", &[]);
        assert_eq!(
            e0.line(),
            "-ERR unknown command 'BAR', with args beginning with: "
        );
    }

    #[test]
    fn unknown_command_args_respect_128_byte_budget() {
        // Many small args: accumulation stops once the args string reaches the
        // 128-byte budget, with no fixed 20-arg cap. Each arg renders `'x' ` (4
        // bytes), so we expect floor(128/4) = 32 args shown.
        let many: Vec<&[u8]> = (0..200).map(|_| b"x".as_slice()).collect();
        let e = ErrorReply::unknown_command("Z", &many);
        assert_eq!(e.message().matches("'x'").count(), 32);
    }

    #[test]
    fn unknown_command_truncates_long_arg_to_remaining_budget() {
        // A single huge arg is truncated to the 128-byte budget.
        let big = vec![b'y'; 1000];
        let e = ErrorReply::unknown_command("Z", &[big.as_slice()]);
        // The accumulated args string holds at most the budget plus the quoting.
        assert_eq!(e.message().matches('y').count(), 128);
    }

    #[test]
    fn unknown_subcommand_is_lowercase_and_uppercases_parent() {
        let e = ErrorReply::unknown_subcommand("client", "BOGUS");
        assert_eq!(
            e.line(),
            "-ERR unknown subcommand or wrong number of arguments for 'BOGUS'. Try CLIENT HELP."
        );
    }

    #[test]
    fn arity_and_token_helpers() {
        assert_eq!(
            ErrorReply::wrong_arity("get").line(),
            "-ERR wrong number of arguments for 'get' command"
        );
        assert_eq!(ErrorCode::WrongType.token(), "WRONGTYPE");
        assert_eq!(ErrorCode::Err.token(), "ERR");
    }

    #[test]
    fn protocol_error_family() {
        assert_eq!(
            ErrorReply::protocol("invalid multibulk length").line(),
            "-ERR Protocol error: invalid multibulk length"
        );
    }

    #[test]
    fn oom_is_byte_exact() {
        // Byte-exact to redis/redis src/server.c OOM_COMMAND_NOT_ALLOWED. The encoder
        // appends the trailing CRLF, so `line()` is the reply without it.
        assert_eq!(
            ErrorReply::oom().line(),
            "-OOM command not allowed when used memory > 'maxmemory'."
        );
        assert_eq!(ErrorReply::oom().code(), ErrorCode::Oom);
        assert_eq!(ErrorCode::Oom.token(), "OOM");
    }

    #[test]
    fn cluster_disabled_is_byte_exact() {
        // Verified against redis/redis src/cluster.c clusterCommand (the
        // cluster_enabled == 0 gate): the plain `ERR` token, no trailing period. This is
        // the reply IronCache slice 1 (cluster-disabled, CLUSTER_CONTRACT.md #70) emits for
        // a mutating/cluster-only CLUSTER subcommand.
        assert_eq!(
            ErrorReply::cluster_disabled().line(),
            "-ERR This instance has cluster support disabled"
        );
        assert_eq!(ErrorReply::cluster_disabled().code(), ErrorCode::Err);
    }

    #[test]
    fn moved_is_byte_exact() {
        // Verified against redis/redis src/cluster.c (clusterRedirectClient, "-%s %d %s:%d"
        // with "MOVED"): the slot is a DECIMAL integer, the address is the unbracketed
        // ip:port, no trailing period. Clients pattern-match the MOVED token to refresh their
        // slot cache and retry at the advertised address.
        let e = ErrorReply::moved(866, "127.0.0.1:7001");
        assert_eq!(e.line(), "-MOVED 866 127.0.0.1:7001");
        assert_eq!(e.code(), ErrorCode::Moved);
        assert_eq!(ErrorCode::Moved.token(), "MOVED");
    }

    #[test]
    fn crossslot_is_byte_exact() {
        // Verified against redis/redis src/cluster.c: "-CROSSSLOT Keys in request don't hash
        // to the same slot". Note the contraction `don't` and NO trailing period.
        let e = ErrorReply::crossslot();
        assert_eq!(
            e.line(),
            "-CROSSSLOT Keys in request don't hash to the same slot"
        );
        assert_eq!(e.code(), ErrorCode::CrossSlot);
        assert_eq!(ErrorCode::CrossSlot.token(), "CROSSSLOT");
    }

    #[test]
    fn clusterdown_is_byte_exact() {
        // Verified against redis/redis src/cluster.c: "-CLUSTERDOWN Hash slot not served".
        let e = ErrorReply::clusterdown_slot_unserved();
        assert_eq!(e.line(), "-CLUSTERDOWN Hash slot not served");
        assert_eq!(e.code(), ErrorCode::ClusterDown);
        assert_eq!(ErrorCode::ClusterDown.token(), "CLUSTERDOWN");
    }

    #[test]
    fn no_replicas_is_byte_exact() {
        // Verified against redis/redis src/server.c (shared.noreplicaserr): the write-side
        // replication guardrail reply (min-replicas-to-write). The leading NOREPLICAS token and
        // the trailing period are part of the contract; clients pattern-match the token.
        let e = ErrorReply::no_replicas();
        assert_eq!(e.line(), "-NOREPLICAS Not enough good replicas to write.");
        assert_eq!(e.code(), ErrorCode::NoReplicas);
        assert_eq!(ErrorCode::NoReplicas.token(), "NOREPLICAS");
    }

    #[test]
    fn syntax_error_is_byte_exact() {
        assert_eq!(ErrorReply::syntax_error().line(), "-ERR syntax error");
    }

    #[test]
    fn config_set_errors_are_byte_exact() {
        // Verified against redis/redis src/config.c configSetCommand: the unknown-option
        // error, the value-rejection "CONFIG SET failed" error, and the immutable
        // (restart-required) variant. The param name is echoed verbatim.
        assert_eq!(
            ErrorReply::config_set_unknown_param("bogus").line(),
            "-ERR Unknown option or number of arguments for CONFIG SET - 'bogus'"
        );
        assert_eq!(
            ErrorReply::config_set_failed("maxmemory", "bad size").line(),
            "-ERR CONFIG SET failed (possibly related to argument 'maxmemory') - bad size"
        );
        assert_eq!(
            ErrorReply::config_set_immutable("databases").line(),
            "-ERR CONFIG SET failed (possibly related to argument 'databases') - can't set immutable config"
        );
    }

    #[test]
    fn config_rewrite_no_file_is_byte_exact() {
        // Verified against redis/redis src/config.c configRewriteCommand: the
        // no-config-file reply (CONFIG REWRITE without a config file).
        assert_eq!(
            ErrorReply::config_rewrite_no_file().line(),
            "-ERR The server is running without a config file"
        );
    }

    #[test]
    fn keyspace_introspection_errors_are_byte_exact() {
        // Verified against redis/redis: src/db.c (shared.nokeyerr, parseScanCursorOrReply)
        // and src/object.c (OBJECT FREQ / IDLETIME LFU gating).
        assert_eq!(ErrorReply::no_such_key().line(), "-ERR no such key");
        assert_eq!(ErrorReply::invalid_cursor().line(), "-ERR invalid cursor");
        assert_eq!(
            ErrorReply::object_freq_requires_lfu().line(),
            "-ERR An LFU maxmemory policy is not selected, access frequency not tracked. \
             Please note that when switching between policies at runtime LRU and LFU data \
             will take some time to adjust."
        );
        assert_eq!(
            ErrorReply::object_idletime_under_lfu().line(),
            "-ERR An LFU maxmemory policy is selected, idle time not tracked. \
             Please note that when switching between policies at runtime LRU and LFU data \
             will take some time to adjust."
        );
    }

    #[test]
    fn invalid_expire_time_is_byte_exact() {
        assert_eq!(
            ErrorReply::invalid_expire_time("set").line(),
            "-ERR invalid expire time in 'set' command"
        );
        // The PR-3b TTL-setting commands reuse the same constructor with their own
        // command name; pin the exact strings the EXPIRE family / GETEX / SETEX /
        // PSETEX emit (byte-exact to Redis addReplyErrorExpireTime).
        assert_eq!(
            ErrorReply::invalid_expire_time("expire").line(),
            "-ERR invalid expire time in 'expire' command"
        );
        assert_eq!(
            ErrorReply::invalid_expire_time("getex").line(),
            "-ERR invalid expire time in 'getex' command"
        );
        assert_eq!(
            ErrorReply::invalid_expire_time("setex").line(),
            "-ERR invalid expire time in 'setex' command"
        );
        assert_eq!(
            ErrorReply::invalid_expire_time("psetex").line(),
            "-ERR invalid expire time in 'psetex' command"
        );
    }

    #[test]
    fn expire_option_errors_are_byte_exact() {
        // Verified against redis/redis: src/expire.c
        // parseExtendedExpireArgumentsOrReply. The three EXPIRE-family option errors
        // are distinct from the generic syntax error.
        assert_eq!(
            ErrorReply::expire_nx_and_xx_gt_lt().line(),
            "-ERR NX and XX, GT or LT options at the same time are not compatible"
        );
        assert_eq!(
            ErrorReply::expire_gt_and_lt().line(),
            "-ERR GT and LT options at the same time are not compatible"
        );
        // The unknown-option token is echoed verbatim.
        assert_eq!(
            ErrorReply::expire_unsupported_option("BOGUS").line(),
            "-ERR Unsupported option BOGUS"
        );
    }

    #[test]
    fn list_errors_are_byte_exact() {
        // Verified against redis/redis src/t_list.c: LSET index-out-of-range and the
        // LPOP/RPOP negative-count error. Both use the plain `ERR` token.
        assert_eq!(
            ErrorReply::index_out_of_range().line(),
            "-ERR index out of range"
        );
        assert_eq!(
            ErrorReply::value_out_of_range_must_be_positive().line(),
            "-ERR value is out of range, must be positive"
        );
    }

    #[test]
    fn lpos_option_errors_are_byte_exact() {
        // Verified against redis/redis src/t_list.c lposCommand: the specific RANK-zero,
        // negative-COUNT, and negative-MAXLEN replies (DISTINCT from not-an-integer).
        assert_eq!(
            ErrorReply::lpos_rank_zero().line(),
            "-ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"
        );
        assert_eq!(
            ErrorReply::lpos_count_negative().line(),
            "-ERR COUNT can't be negative"
        );
        assert_eq!(
            ErrorReply::lpos_maxlen_negative().line(),
            "-ERR MAXLEN can't be negative"
        );
    }

    #[test]
    fn numeric_rmw_errors_are_byte_exact() {
        // Verified against redis/redis: src/t_string.c (incrDecrCommand,
        // incrbyfloatCommand) and src/object.c (getLongDoubleFromObjectOrReply
        // NULL-message default).
        assert_eq!(
            ErrorReply::not_an_integer().line(),
            "-ERR value is not an integer or out of range"
        );
        assert_eq!(
            ErrorReply::increment_overflow().line(),
            "-ERR increment or decrement would overflow"
        );
        assert_eq!(
            ErrorReply::not_a_valid_float().line(),
            "-ERR value is not a valid float"
        );
        assert_eq!(
            ErrorReply::increment_nan_or_inf().line(),
            "-ERR increment would produce NaN or Infinity"
        );
    }

    #[test]
    fn set_errors_are_byte_exact() {
        // Verified against redis/redis src/t_set.c sintercardCommand: the numkeys and LIMIT
        // option errors. All use the plain `ERR` token and are DISTINCT from the generic
        // not-an-integer error (a non-integer numkeys/LIMIT is thrown by the parse).
        assert_eq!(
            ErrorReply::numkeys_should_be_positive().line(),
            "-ERR numkeys should be greater than 0"
        );
        assert_eq!(
            ErrorReply::numkeys_greater_than_args().line(),
            "-ERR Number of keys can't be greater than number of args"
        );
        assert_eq!(
            ErrorReply::limit_cant_be_negative().line(),
            "-ERR LIMIT can't be negative"
        );
    }

    #[test]
    fn hash_value_errors_are_byte_exact() {
        // Verified against redis/redis src/t_hash.c: HINCRBY's stored-value-not-integer
        // and HINCRBYFLOAT's stored-value-not-float replies. Both use the plain `ERR`
        // token and are DISTINCT from the string-family not-an-integer / not-a-valid
        // -float replies (those are for command arguments).
        assert_eq!(
            ErrorReply::hash_value_not_an_integer().line(),
            "-ERR hash value is not an integer"
        );
        assert_eq!(
            ErrorReply::hash_value_not_a_float().line(),
            "-ERR hash value is not a float"
        );
    }

    #[test]
    fn zset_errors_are_byte_exact() {
        // Verified against redis/redis src/t_zset.c. The bound/weight parse errors, the
        // ZADD flag-combination errors, and the resulting-score-is-NaN error (the INCR
        // path: an existing +inf incremented by -inf). All use the plain `ERR` token.
        assert_eq!(
            ErrorReply::min_or_max_not_a_float().line(),
            "-ERR min or max is not a float"
        );
        assert_eq!(
            ErrorReply::weight_not_a_float().line(),
            "-ERR weight value is not a float"
        );
        assert_eq!(
            ErrorReply::zadd_gt_lt_nx_incompatible().line(),
            "-ERR GT, LT, and/or NX options at the same time are not compatible"
        );
        // The resulting-score-is-NaN reply (ZINCRBY / ZADD INCR producing NaN). DISTINCT
        // from the bad-score-input not-a-valid-float error.
        assert_eq!(
            ErrorReply::zadd_score_is_nan().line(),
            "-ERR resulting score is not a number (NaN)"
        );
        assert_eq!(ErrorReply::zadd_score_is_nan().code(), ErrorCode::Err);
        // The ZRANGE BYLEX+WITHSCORES and LIMIT-without-BYSCORE/BYLEX conflict replies.
        assert_eq!(
            ErrorReply::zrange_withscores_not_with_bylex().line(),
            "-ERR syntax error, WITHSCORES not supported in combination with BYLEX"
        );
        assert_eq!(
            ErrorReply::zrange_limit_only_with_byscore_or_bylex().line(),
            "-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
        );
    }

    #[test]
    fn bitmap_errors_are_byte_exact() {
        // Verified against redis/redis src/bitops.c: the bit-offset / bit-value range
        // errors, the BITOP NOT single-source error, the invalid bitfield-type and
        // invalid-OVERFLOW errors, and the BITFIELD_RO write-rejection error. All use the
        // plain `ERR` token. The bit-offset error is the one guarding against a huge
        // allocation (proto-max-bit-offset).
        assert_eq!(
            ErrorReply::bit_offset_out_of_range().line(),
            "-ERR bit offset is not an integer or out of range"
        );
        assert_eq!(
            ErrorReply::bit_not_integer_or_range().line(),
            "-ERR bit is not an integer or out of range"
        );
        // BITPOS's parsed-but-not-0/1 bit-argument error (note the trailing period).
        // DISTINCT from SETBIT's bit-not-integer string above.
        assert_eq!(
            ErrorReply::bitpos_bit_arg().line(),
            "-ERR The bit argument must be 1 or 0."
        );
        assert_eq!(
            ErrorReply::bitop_not_single_source().line(),
            "-ERR BITOP NOT must be called with a single source key."
        );
        assert_eq!(
            ErrorReply::invalid_bitfield_type().line(),
            "-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
        );
        assert_eq!(
            ErrorReply::bitfield_invalid_overflow().line(),
            "-ERR Invalid OVERFLOW type specified"
        );
        assert_eq!(
            ErrorReply::bitfield_ro_no_writes().line(),
            "-ERR BITFIELD_RO only supports the GET subcommand"
        );
    }

    #[test]
    fn hll_invalid_value_error_is_byte_exact() {
        // Verified against redis/redis src/hyperloglog.c `isHLLObjectOrReply`. The token
        // is WRONGTYPE (the same code as wrong_type), prepended by `line`, so the message
        // carries no WRONGTYPE prefix and the wire line has exactly one WRONGTYPE token.
        assert_eq!(
            ErrorReply::hll_invalid_value().line(),
            "-WRONGTYPE Key is not a valid HyperLogLog string value."
        );
        assert_eq!(ErrorReply::hll_invalid_value().code(), ErrorCode::WrongType);
    }
}
