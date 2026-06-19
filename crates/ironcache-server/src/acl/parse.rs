// SPDX-License-Identifier: MIT OR Apache-2.0
//! The Redis ACL RULE-GRAMMAR parser (#106): turns the `ACL SETUSER`/aclfile rule tokens
//! into mutations on a [`User`].
//!
//! Supported rules (a solid v1 of the Redis grammar):
//! - `on` / `off` - enable / disable the user.
//! - `>password` - add a CLEARTEXT password (hashed to SHA-256 at rest immediately).
//! - `<password` - remove a cleartext password (by its digest).
//! - `#hash` - add an already-SHA-256-hex password digest.
//! - `!hash` - remove a password digest.
//! - `nopass` - the user authenticates with any (or no) password; clears the digest list.
//! - `resetpass` - clear `nopass` AND all passwords (the user becomes unable to auth).
//! - `~pattern` / `allkeys` (`~*`) / `resetkeys` - key permissions.
//! - `&pattern` / `allchannels` (`&*`) / `resetchannels` - channel permissions.
//! - `+cmd` / `-cmd` / `+@cat` / `-@cat` / `allcommands` (`+@all`) / `nocommands` (`-@all`)
//!   - command permissions.
//! - `reset` - reset the user to the fresh locked-down baseline.
//!
//! DEFERRED (documented follow-up, rejected as unknown so a typo is loud, not silent):
//! `%R~`/`%W~`/`%RW~` read-write key sub-patterns, `(...)` command selectors, `clearselectors`,
//! `sanitize-payload`/`nosanitize-payload`. A `~pattern` grants full (read+write) key access.
//!
//! Each rule is validated; an unrecognized or malformed rule returns [`AclParseError`] so
//! `ACL SETUSER` replies the Redis-style `ERR Error in ACL SETUSER modifier '<rule>': ...`
//! and applies NOTHING (the caller parses into a SCRATCH copy and only commits on full success).

use super::categories::Category;
use super::perms::User;

/// A rule-parse failure: the offending rule token and a human reason. The caller renders
/// it into the Redis `ACL SETUSER` modifier-error string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclParseError {
    /// The verbatim rule token that failed (NEVER a plaintext password: a `>pw` failure
    /// reports the literal `>password` placeholder, see [`Self::redacted_rule`]).
    pub rule: String,
    /// Why it was rejected.
    pub reason: String,
}

impl AclParseError {
    fn new(rule: &str, reason: impl Into<String>) -> AclParseError {
        AclParseError {
            rule: rule.to_owned(),
            reason: reason.into(),
        }
    }

    /// The rule token with any plaintext password REDACTED (a `>...` / `<...` token never
    /// leaks its secret into an error string or log). Used to build the error reply.
    #[must_use]
    pub fn redacted_rule(&self) -> String {
        redacted_rule(&self.rule)
    }
}

/// Redact a single `ACL SETUSER` rule token if it carries a secret: the password rules
/// `>cleartext` / `<cleartext` and the digest rules `#hash` / `!hash` are replaced by their
/// `<prefix>(password)` placeholder; any non-secret rule (`on`, `~k:*`, `+get`, ...) is
/// returned verbatim. This is the canonical redaction reused both for the `ACL SETUSER`
/// modifier-error reply ([`AclParseError::redacted_rule`]) and for SLOWLOG argument
/// redaction, so the two never drift.
#[must_use]
pub fn redacted_rule(rule: &str) -> String {
    match rule.as_bytes().first() {
        Some(b'>') => ">(password)".to_owned(),
        Some(b'<') => "<(password)".to_owned(),
        Some(b'#') => "#(password)".to_owned(),
        Some(b'!') => "!(password)".to_owned(),
        _ => rule.to_owned(),
    }
}

/// Apply ONE rule token to `user`, mutating it. The token is the verbatim ACL rule (e.g.
/// `on`, `>pw`, `~k:*`, `+@read`, `-flushall`). Returns an [`AclParseError`] on an
/// unknown/malformed rule, leaving `user` in whatever partial state prior rules produced -
/// so callers MUST parse into a scratch [`User`] clone and commit only on full success.
///
/// Passwords given as `>cleartext` are hashed to SHA-256 hex AT REST here (the cleartext
/// is dropped immediately); `#hexdigest` is validated as 64 lowercase hex chars and stored
/// verbatim.
pub fn apply_rule(user: &mut User, rule: &[u8]) -> Result<(), AclParseError> {
    // A rule is matched on its first byte for the prefixed forms (`>`/`<`/`#`/`!`/`~`/`&`/
    // `+`/`-`), else on the whole keyword (`on`/`off`/`nopass`/`reset`/...).
    let rule_str = String::from_utf8_lossy(rule);
    match rule.first() {
        Some(b'>') => {
            // >cleartext: hash to SHA-256 hex at rest, add it, and disable nopass.
            let pw = &rule[1..];
            let digest = ironcache_config::sha256_hex(pw);
            user.nopass = false;
            if !user.passwords.contains(&digest) {
                user.passwords.push(digest);
            }
            Ok(())
        }
        Some(b'<') => {
            // <cleartext: remove the password whose digest matches.
            let digest = ironcache_config::sha256_hex(&rule[1..]);
            user.passwords.retain(|d| d != &digest);
            Ok(())
        }
        Some(b'#') => {
            // #hexdigest: add an already-hashed password digest (must be valid sha-256 hex).
            let hex = &rule[1..];
            let digest = validate_hex_digest(hex, &rule_str)?;
            user.nopass = false;
            if !user.passwords.contains(&digest) {
                user.passwords.push(digest);
            }
            Ok(())
        }
        Some(b'!') => {
            // !hexdigest: remove a password digest.
            let digest = validate_hex_digest(&rule[1..], &rule_str)?;
            user.passwords.retain(|d| d != &digest);
            Ok(())
        }
        Some(b'~') => {
            // ~pattern / ~* (allkeys).
            if rule == b"~*" {
                user.keys.set_allkeys();
            } else {
                user.keys.add_pattern(&rule[1..]);
            }
            Ok(())
        }
        Some(b'%') => Err(AclParseError::new(
            &rule_str,
            "read-write key patterns (%R~/%W~/%RW~) are not supported in this version \
             (use ~pattern for full access)",
        )),
        Some(b'&') => {
            // &pattern / &* (allchannels).
            if rule == b"&*" {
                user.channels.set_allchannels();
            } else {
                user.channels.add_pattern(&rule[1..]);
            }
            Ok(())
        }
        Some(b'(') => Err(AclParseError::new(
            &rule_str,
            "command selectors ((...)) are not supported in this version",
        )),
        Some(b'+') => apply_command_rule(user, &rule[1..], true, &rule_str),
        Some(b'-') => apply_command_rule(user, &rule[1..], false, &rule_str),
        _ => apply_keyword_rule(user, rule, &rule_str),
    }
}

/// Apply a `+`/`-` command or category rule (the `+`/`-` already stripped). `allow` is the
/// sign. `+@cat`/`-@cat` toggle a category; `+@all`/`-@all` (and the `allcommands`/
/// `nocommands` keywords) set the absolute baseline; `+cmd`/`-cmd` toggle a single command.
fn apply_command_rule(
    user: &mut User,
    body: &[u8],
    allow: bool,
    rule_str: &str,
) -> Result<(), AclParseError> {
    if let Some(cat_name) = body.strip_prefix(b"@") {
        // Category rule.
        if cat_name.eq_ignore_ascii_case(b"all") {
            if allow {
                user.commands.allow_all();
            } else {
                user.commands.deny_all();
            }
            return Ok(());
        }
        let name = String::from_utf8_lossy(cat_name);
        let Some(cat) = Category::from_name(&name) else {
            return Err(AclParseError::new(
                rule_str,
                format!("unknown command category '@{name}'"),
            ));
        };
        if allow {
            user.commands.allow_category(cat);
        } else {
            user.commands.deny_category(cat);
        }
        return Ok(());
    }
    // Single command rule: validate the command is one the server implements (Redis
    // rejects an unknown command in a +cmd rule). A command SUBcommand (`+config|get`)
    // is not supported in this version: reject the `|` form loudly.
    if body.contains(&b'|') {
        return Err(AclParseError::new(
            rule_str,
            "first-arg / subcommand command rules (cmd|sub) are not supported in this version",
        ));
    }
    let cmd_upper = body.to_ascii_uppercase();
    if crate::command_spec::spec_of(&cmd_upper).is_none() && !is_known_extra_command(&cmd_upper) {
        return Err(AclParseError::new(
            rule_str,
            format!(
                "unknown command '{}'",
                String::from_utf8_lossy(body).to_ascii_lowercase()
            ),
        ));
    }
    if allow {
        user.commands.allow_command(&cmd_upper);
    } else {
        user.commands.deny_command(&cmd_upper);
    }
    Ok(())
}

/// Commands that are legal targets of a `+cmd`/`-cmd` ACL rule but are NOT in the #89
/// dispatch registry (they are intercepted in the serve layer: ACL itself, the pub/sub
/// verbs). Without this a `-publish` or `+acl` rule would be wrongly rejected as unknown.
fn is_known_extra_command(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"ACL"
            | b"SUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"PUBLISH"
            | b"PUBSUB"
    )
}

/// Apply a bare-keyword rule (`on`/`off`/`nopass`/`resetpass`/`reset`/`allkeys`/`resetkeys`/
/// `allchannels`/`resetchannels`/`allcommands`/`nocommands`).
fn apply_keyword_rule(user: &mut User, rule: &[u8], rule_str: &str) -> Result<(), AclParseError> {
    // Keywords are case-insensitive in Redis.
    let kw = rule.to_ascii_lowercase();
    match kw.as_slice() {
        b"on" => user.enabled = true,
        b"off" => user.enabled = false,
        b"nopass" => {
            user.nopass = true;
            user.passwords.clear();
        }
        b"resetpass" => {
            user.nopass = false;
            user.passwords.clear();
        }
        b"reset" => reset_user(user),
        b"allkeys" => user.keys.set_allkeys(),
        b"resetkeys" => user.keys.reset(),
        b"allchannels" => user.channels.set_allchannels(),
        b"resetchannels" => user.channels.reset(),
        b"allcommands" => user.commands.allow_all(),
        b"nocommands" => user.commands.deny_all(),
        b"sanitize-payload" | b"nosanitize-payload" => {
            // Accepted as a no-op (IronCache has no RESTORE payload sanitizer); harmless.
        }
        _ => {
            return Err(AclParseError::new(
                rule_str,
                "syntax error: unknown ACL rule",
            ));
        }
    }
    Ok(())
}

/// `reset`: return the user to the fresh locked-down baseline (off, no passwords, `-@all`,
/// no keys, no channels) - keeping only the name. Mirrors Redis `ACL SETUSER <u> reset`.
fn reset_user(user: &mut User) {
    let fresh = User::new(&user.name);
    *user = fresh;
}

/// Validate a hex digest token (`#`/`!` body): exactly 64 lowercase hex chars (a SHA-256
/// hex). Returns the lowercased digest string. Rejects a malformed digest loudly.
fn validate_hex_digest(hex: &[u8], rule_str: &str) -> Result<String, AclParseError> {
    if hex.len() != 64 || !hex.iter().all(u8::is_ascii_hexdigit) {
        return Err(AclParseError::new(
            rule_str,
            "Error in ACL SETUSER: a password hash must be exactly 64 hex characters",
        ));
    }
    Ok(String::from_utf8_lossy(hex).to_ascii_lowercase())
}

/// Parse a full `user <name> <rule>...` aclfile/`ACL SETUSER` rule sequence into a NEW user,
/// starting from the fresh baseline. Each `rule` is applied in order; the FIRST error aborts
/// (the user is discarded by the caller). Used by `ACL LOAD` and `ACL SETUSER` (which seeds
/// from an existing user instead, see [`apply_rules_to`]).
///
/// # Errors
/// Returns the first [`AclParseError`] encountered.
pub fn build_user(name: &str, rules: &[&[u8]]) -> Result<User, AclParseError> {
    let mut user = User::new(name);
    apply_rules_to(&mut user, rules)?;
    Ok(user)
}

/// Apply a sequence of rules to an EXISTING (scratch) user in order. `ACL SETUSER` clones
/// the live user, applies here, and commits only on full success (so a mid-sequence error
/// leaves the live user untouched).
///
/// # Errors
/// Returns the first [`AclParseError`] encountered.
pub fn apply_rules_to(user: &mut User, rules: &[&[u8]]) -> Result<(), AclParseError> {
    for rule in rules {
        apply_rule(user, rule)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_on_password_keys_commands() {
        let u = build_user(
            "app",
            &[b"on", b">s3cr3t", b"~k:*", b"+@read", b"+get", b"-flushall"],
        )
        .expect("valid rules");
        assert!(u.enabled);
        assert!(u.verify_password(b"s3cr3t"));
        assert!(!u.verify_password(b"nope"));
        assert!(u.can_access_key(b"k:1"));
        assert!(!u.can_access_key(b"other"));
        assert!(u.can_run_command(b"GET"));
        assert!(!u.can_run_command(b"SET")); // not granted
    }

    #[test]
    fn nopass_and_resetpass() {
        let mut u = build_user("app", &[b"on", b"nopass"]).expect("ok");
        assert!(u.nopass);
        assert!(u.verify_password(b"anything"));
        apply_rules_to(&mut u, &[b"resetpass"]).expect("ok");
        assert!(!u.nopass);
        assert!(!u.verify_password(b"anything")); // no password + not nopass -> cannot auth
    }

    #[test]
    fn hash_digest_add_and_validate() {
        let digest = ironcache_config::sha256_hex(b"pw");
        let rule = format!("#{digest}");
        let u = build_user("app", &[b"on", rule.as_bytes()]).expect("ok");
        assert!(u.verify_password(b"pw"));
        // A bad-length hash is rejected.
        let err = build_user("app", &[b"#deadbeef"]).unwrap_err();
        assert!(err.reason.contains("64 hex"));
    }

    #[test]
    fn unknown_rule_and_unknown_category_and_command_rejected() {
        assert!(build_user("app", &[b"bogus"]).is_err());
        assert!(build_user("app", &[b"+@nosuchcat"]).is_err());
        assert!(build_user("app", &[b"+nosuchcmd"]).is_err());
        // %R~ deferred -> rejected loudly.
        assert!(build_user("app", &[b"%R~k:*"]).is_err());
    }

    #[test]
    fn allkeys_allchannels_allcommands() {
        let u = build_user("app", &[b"on", b"~*", b"&*", b"+@all"]).expect("ok");
        assert!(u.is_all_permissive());
    }

    #[test]
    fn reset_returns_to_baseline() {
        let mut u = build_user("app", &[b"on", b"nopass", b"~*", b"+@all"]).expect("ok");
        apply_rules_to(&mut u, &[b"reset"]).expect("ok");
        assert!(!u.enabled);
        assert!(!u.nopass);
        assert!(!u.can_run_command(b"GET"));
        assert!(!u.can_access_key(b"x"));
    }

    #[test]
    fn password_redacted_in_error_rule() {
        // A `>` rule never fails parse (it always hashes), but redaction is exercised here.
        let e = AclParseError::new(">supersecret", "x");
        assert_eq!(e.redacted_rule(), ">(password)");
    }
}
