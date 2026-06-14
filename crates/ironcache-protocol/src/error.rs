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
    /// Index/offset out of range.
    OutOfRange,
    /// `UNWATCH`/`DISCARD`-class "no transaction in progress".
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

    /// `ERR unknown command '<name>', with args beginning with: <args>`.
    ///
    /// Matches Valkey: the command name is single-quoted, then each argument is
    /// rendered single-quoted and comma-separated with a trailing comma. Clients
    /// pattern-match the `unknown command` phrase during handshake fallback.
    #[must_use]
    pub fn unknown_command(name: &str, args: &[&[u8]]) -> Self {
        let mut s = String::new();
        // Redis truncates each arg display to 128 bytes and shows at most 20 args;
        // we mirror that bound so adversarial input cannot blow up the reply.
        let mut shown = String::new();
        for (i, a) in args.iter().take(20).enumerate() {
            if i > 0 {
                shown.push_str(", ");
            }
            let text = String::from_utf8_lossy(&a[..a.len().min(128)]);
            let _ = write!(shown, "'{text}'");
        }
        let _ = write!(
            s,
            "unknown command '{name}', with args beginning with: {shown}"
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

    /// `ERR Client sent AUTH, but no password is set. Did you mean AUTH
    /// <username> <password>?` - the verbatim Redis string for `AUTH` with no
    /// password configured. Clients fall back on this exact text.
    #[must_use]
    pub fn auth_no_password_set() -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            "Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
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

    /// `ERR DB index is out of range`.
    #[must_use]
    pub fn select_out_of_range() -> Self {
        ErrorReply::new(ErrorCode::Err, "DB index is out of range")
    }

    /// `ERR <command>|<sub> ... ` style "unknown subcommand" message, the wording
    /// clients see for an unrecognized `CLIENT`/`COMMAND`/`CONFIG` subcommand.
    #[must_use]
    pub fn unknown_subcommand(parent: &str, sub: &str) -> Self {
        ErrorReply::new(
            ErrorCode::Err,
            format!(
                "Unknown subcommand or wrong number of arguments for '{sub}'. Try {} HELP.",
                parent.to_uppercase()
            ),
        )
    }
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
            "-ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?"
        );
    }

    #[test]
    fn unknown_command_renders_name_and_args() {
        let e = ErrorReply::unknown_command("FOO", &[b"a", b"b"]);
        assert_eq!(
            e.line(),
            "-ERR unknown command 'FOO', with args beginning with: 'a', 'b'"
        );
        // No-arg form.
        let e0 = ErrorReply::unknown_command("BAR", &[]);
        assert_eq!(
            e0.line(),
            "-ERR unknown command 'BAR', with args beginning with: "
        );
    }

    #[test]
    fn unknown_command_caps_arg_count() {
        let many: Vec<&[u8]> = (0..40).map(|_| b"x".as_slice()).collect();
        let e = ErrorReply::unknown_command("Z", &many);
        // 20 shown args -> 19 separators -> 20 quoted 'x'.
        assert_eq!(e.message().matches("'x'").count(), 20);
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
}
