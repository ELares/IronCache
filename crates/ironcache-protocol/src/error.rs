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

    /// `EXECABORT Transaction discarded because of previous errors.`
    #[must_use]
    pub fn exec_abort() -> Self {
        ErrorReply::new(
            ErrorCode::ExecAbort,
            "Transaction discarded because of previous errors.",
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
    fn syntax_error_is_byte_exact() {
        assert_eq!(ErrorReply::syntax_error().line(), "-ERR syntax error");
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
}
