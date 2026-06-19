// SPDX-License-Identifier: MIT OR Apache-2.0
//! The `ACL` command family (#106): WHOAMI / LIST / USERS / GETUSER / SETUSER / DELUSER /
//! CAT / GENPASS / SAVE / LOAD.
//!
//! These run in the SERVE layer (like CONFIG / persistence) because they mutate the shared
//! [`crate::acl::AclState`] registry and SAVE/LOAD do file I/O. The handler is PURE w.r.t.
//! time (no clock); GENPASS draws from the [`ironcache_env::Rng`] determinism seam passed
//! by the caller, NEVER `rand::thread_rng` directly (ADR-0003).
//!
//! ## What is in (honest scope)
//!
//! WHOAMI, LIST, USERS, GETUSER, SETUSER (the full `on`/`off`/`>pw`/`<pw`/`#hash`/`!hash`/
//! `nopass`/`resetpass`/`~pat`/`allkeys`/`resetkeys`/`&pat`/`allchannels`/`resetchannels`/
//! `+cmd`/`-cmd`/`+@cat`/`-@cat`/`allcommands`/`nocommands`/`reset` grammar), DELUSER (the
//! `default` user is protected), CAT, GENPASS, SAVE, LOAD. An unknown subcommand is rejected.
//!
//! DEFERRED (documented): `ACL GETUSER` renders the COMPACT Redis 7 reply shape (flags +
//! passwords + commands + keys + channels), not the per-selector detail; `ACL LOG` /
//! `ACL HELP` / command selectors / `%R~`/`%W~` key sub-patterns are follow-ups.
//!
//! ## SUBCOMMAND granularity (deferred, conservative)
//!
//! Enforcement gates the WHOLE `ACL` command as `@admin`/`@dangerous` (see the category map), so a
//! user WITHOUT `@admin` (or an explicit `+acl`) cannot run ANY `ACL` subcommand -- including the
//! unprivileged-in-Redis `WHOAMI`/`CAT`/`GENPASS`/`HELP`. This is a SAFE divergence (deny-more, not
//! allow-more): it never grants an admin verb to a non-admin. Per-subcommand ACL granularity
//! (`+acl|whoami`) is a documented follow-up; the `cmd|sub` rule form is already rejected loudly by
//! the parser so a typo is not silently ignored.

use crate::acl::{AclState, DEFAULT_USER};
use ironcache_env::Rng;
use ironcache_protocol::{ErrorReply, Request, Value};

/// The result of an `ACL` command that may need to PERSIST the registry to the aclfile.
/// `ACL SAVE` returns [`AclSideEffect::Save`] so the serve layer (which owns the aclfile
/// path + does the I/O) writes the file; everything else is [`AclSideEffect::None`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclSideEffect {
    /// No file side effect.
    None,
    /// `ACL SAVE`: the serve layer should write `text` to the configured aclfile, then reply
    /// the carried `reply` (`+OK` on success, or an error if no aclfile is configured / the
    /// write fails -- the serve layer maps the I/O outcome).
    Save(String),
    /// `ACL LOAD`: the serve layer should read the configured aclfile and call
    /// [`AclState::load_users`]; the reply is produced there.
    Load,
}

/// Dispatch an `ACL <subcommand> [args...]`. `acl` is the shared registry, `whoami` is the
/// CURRENT connection's authenticated username (for WHOAMI), and `rng` is the determinism-
/// seam RNG (for GENPASS). Returns the reply plus any file side effect the serve layer must
/// perform (SAVE/LOAD). Pure w.r.t. time.
///
/// `whoami` is the resolved username of the connection: the cached ACL user's name, or
/// `default` when the connection is the implicit all-permissive default (no narrowed user).
pub fn dispatch_acl(
    acl: &AclState,
    whoami: &str,
    rng: &mut impl Rng,
    req: &Request,
) -> (Value, AclSideEffect) {
    if req.args.len() < 2 {
        return (
            Value::error(ErrorReply::wrong_arity("acl")),
            AclSideEffect::None,
        );
    }
    let sub = req.args[1].to_ascii_uppercase();
    match sub.as_slice() {
        b"WHOAMI" => (Value::bulk_str(whoami), AclSideEffect::None),
        b"LIST" => acl_list(acl),
        b"USERS" => acl_users(acl),
        b"CAT" => acl_cat(),
        b"GENPASS" => acl_genpass(rng, req),
        b"GETUSER" => acl_getuser(acl, req),
        b"SETUSER" => acl_setuser(acl, req),
        b"DELUSER" => acl_deluser(acl, req),
        b"SAVE" => (Value::ok(), AclSideEffect::Save(acl.serialize_aclfile())),
        b"LOAD" => (Value::ok(), AclSideEffect::Load),
        other => {
            let sub_str = String::from_utf8_lossy(other).to_ascii_lowercase();
            (
                Value::error(ErrorReply::unknown_subcommand("acl", &sub_str)),
                AclSideEffect::None,
            )
        }
    }
}

/// `ACL LIST`: one bulk-string per user, its full `user <name> <rules>` line.
fn acl_list(acl: &AclState) -> (Value, AclSideEffect) {
    let items = acl
        .list_lines()
        .into_iter()
        .map(|l| Value::bulk_str(&l))
        .collect();
    (Value::Array(Some(items)), AclSideEffect::None)
}

/// `ACL USERS`: the user names, one bulk-string each (sorted).
fn acl_users(acl: &AclState) -> (Value, AclSideEffect) {
    let items = acl
        .user_names()
        .into_iter()
        .map(|n| Value::bulk_str(&n))
        .collect();
    (Value::Array(Some(items)), AclSideEffect::None)
}

/// `ACL CAT`: the recognized category names, one bulk-string each.
fn acl_cat() -> (Value, AclSideEffect) {
    let items = crate::acl::Category::all()
        .iter()
        .map(|c| Value::bulk_str(c.name()))
        .collect();
    (Value::Array(Some(items)), AclSideEffect::None)
}

/// `ACL GENPASS [bits]`: a random hex secret of `bits` bits (default 256), drawn from the
/// determinism-seam RNG (NOT thread_rng). `bits` must be 1..=4096 (Redis bound); the hex
/// length is `ceil(bits/4)` characters. PURE w.r.t. time.
fn acl_genpass(rng: &mut impl Rng, req: &Request) -> (Value, AclSideEffect) {
    use std::fmt::Write as _;
    let bits: u32 = if req.args.len() >= 3 {
        match std::str::from_utf8(&req.args[2])
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
        {
            Some(b) if (1..=4096).contains(&b) => b,
            _ => {
                return (
                    Value::error(ErrorReply::err(
                        "ACL GENPASS argument must be the number of bits for the output password, \
                         a positive number up to 4096",
                    )),
                    AclSideEffect::None,
                );
            }
        }
    } else {
        256
    };
    // One hex char encodes 4 bits; round up.
    let hex_len = bits.div_ceil(4) as usize;
    let mut out = String::with_capacity(hex_len);
    while out.len() < hex_len {
        // Pull 64 bits at a time from the seam and render as hex; truncate to hex_len. `write!`
        // to the String cannot fail (the `Write` impl for `String` is infallible), so the
        // Result is intentionally discarded.
        let chunk = rng.next_u64();
        let _ = write!(out, "{chunk:016x}");
    }
    out.truncate(hex_len);
    (Value::bulk_str(&out), AclSideEffect::None)
}

/// `ACL GETUSER <name>`: the user's flags + passwords + commands + keys + channels, in a
/// flat RESP map-as-array (Redis 7 compact shape). `nil` (a null array) when no such user.
fn acl_getuser(acl: &AclState, req: &Request) -> (Value, AclSideEffect) {
    if req.args.len() != 3 {
        return (
            Value::error(ErrorReply::wrong_arity("acl|getuser")),
            AclSideEffect::None,
        );
    }
    let name = String::from_utf8_lossy(&req.args[2]).into_owned();
    let Some(user) = acl.get_user(&name) else {
        return (Value::Array(None), AclSideEffect::None);
    };
    // flags: [on|off, nopass?] as a sub-array.
    let mut flags = vec![Value::bulk_str(if user.enabled { "on" } else { "off" })];
    if user.nopass {
        flags.push(Value::bulk_str("nopass"));
    }
    let passwords = user
        .passwords
        .iter()
        .map(|d| Value::bulk_str(d))
        .collect::<Vec<_>>();
    let items = vec![
        Value::bulk_str("flags"),
        Value::Array(Some(flags)),
        Value::bulk_str("passwords"),
        Value::Array(Some(passwords)),
        Value::bulk_str("commands"),
        Value::bulk_str(&user.commands.describe()),
        Value::bulk_str("keys"),
        Value::bulk_str(&user.keys.describe()),
        Value::bulk_str("channels"),
        Value::bulk_str(&user.channels.describe()),
    ];
    (Value::Array(Some(items)), AclSideEffect::None)
}

/// `ACL SETUSER <name> <rules...>`: create / modify the user atomically. A parse error
/// replies the Redis-style modifier error (with the password REDACTED) and changes nothing.
fn acl_setuser(acl: &AclState, req: &Request) -> (Value, AclSideEffect) {
    if req.args.len() < 3 {
        return (
            Value::error(ErrorReply::wrong_arity("acl|setuser")),
            AclSideEffect::None,
        );
    }
    let name = String::from_utf8_lossy(&req.args[2]).into_owned();
    let rules: Vec<&[u8]> = req.args[3..].iter().map(AsRef::as_ref).collect();
    match acl.set_user(&name, &rules) {
        Ok(()) => (Value::ok(), AclSideEffect::None),
        Err(e) => (
            Value::error(ErrorReply::err(format!(
                "Error in ACL SETUSER modifier '{}': {}",
                e.redacted_rule(),
                e.reason
            ))),
            AclSideEffect::None,
        ),
    }
}

/// `ACL DELUSER <name> [<name> ...]`: delete users; the `default` user is protected. Replies
/// the count deleted. Deleting a non-existent user is silently 0 for it (Redis counts only
/// removed users); deleting `default` is a hard error.
fn acl_deluser(acl: &AclState, req: &Request) -> (Value, AclSideEffect) {
    if req.args.len() < 3 {
        return (
            Value::error(ErrorReply::wrong_arity("acl|deluser")),
            AclSideEffect::None,
        );
    }
    let mut deleted = 0i64;
    for raw in &req.args[2..] {
        let name = String::from_utf8_lossy(raw).into_owned();
        if name == DEFAULT_USER {
            return (
                Value::error(ErrorReply::err("The 'default' user cannot be removed")),
                AclSideEffect::None,
            );
        }
        if acl.del_user(&name) {
            deleted += 1;
        }
    }
    (Value::Integer(deleted), AclSideEffect::None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ironcache_env::{Env, TestEnv};

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    fn run(acl: &AclState, whoami: &str, env: &mut TestEnv, parts: &[&[u8]]) -> Value {
        dispatch_acl(acl, whoami, env.rng(), &req(parts)).0
    }

    #[test]
    fn whoami_reports_user() {
        let acl = AclState::from_requirepass(None);
        let mut env = TestEnv::new(1);
        assert_eq!(
            run(&acl, "default", &mut env, &[b"ACL", b"WHOAMI"]),
            Value::bulk_str("default")
        );
    }

    #[test]
    fn setuser_getuser_deluser_round_trip() {
        let acl = AclState::from_requirepass(None);
        let mut env = TestEnv::new(1);
        assert_eq!(
            run(
                &acl,
                "default",
                &mut env,
                &[b"ACL", b"SETUSER", b"app", b"on", b">pw", b"~k:*", b"+get"]
            ),
            Value::ok()
        );
        // GETUSER returns the user.
        match run(&acl, "default", &mut env, &[b"ACL", b"GETUSER", b"app"]) {
            Value::Array(Some(items)) => assert!(!items.is_empty()),
            other => panic!("expected array, got {other:?}"),
        }
        // GETUSER of an absent user is a null array.
        assert_eq!(
            run(&acl, "default", &mut env, &[b"ACL", b"GETUSER", b"nope"]),
            Value::Array(None)
        );
        // DELUSER removes it.
        assert_eq!(
            run(&acl, "default", &mut env, &[b"ACL", b"DELUSER", b"app"]),
            Value::Integer(1)
        );
        // DELUSER default is refused.
        match run(&acl, "default", &mut env, &[b"ACL", b"DELUSER", b"default"]) {
            Value::Error(e) => assert!(e.message().contains("cannot be removed")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn setuser_bad_rule_redacts_password() {
        let acl = AclState::from_requirepass(None);
        let mut env = TestEnv::new(1);
        match run(
            &acl,
            "default",
            &mut env,
            &[b"ACL", b"SETUSER", b"app", b">secretpw", b"+boguscmd"],
        ) {
            Value::Error(e) => {
                assert!(e.message().contains("boguscmd"));
                // The password rule never appears verbatim in the error.
                assert!(!e.message().contains("secretpw"));
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn genpass_uses_seam_and_is_deterministic() {
        let acl = AclState::from_requirepass(None);
        let mut env1 = TestEnv::new(42);
        let mut env2 = TestEnv::new(42);
        let p1 = run(&acl, "default", &mut env1, &[b"ACL", b"GENPASS"]);
        let p2 = run(&acl, "default", &mut env2, &[b"ACL", b"GENPASS"]);
        // Same seed -> same password (the determinism seam, ADR-0003).
        assert_eq!(p1, p2);
        match p1 {
            Value::BulkString(Some(b)) => assert_eq!(b.len(), 64), // 256 bits / 4
            other => panic!("expected bulk, got {other:?}"),
        }
        // A bits argument shortens it.
        match run(&acl, "default", &mut env1, &[b"ACL", b"GENPASS", b"32"]) {
            Value::BulkString(Some(b)) => assert_eq!(b.len(), 8),
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    #[test]
    fn cat_lists_categories() {
        let acl = AclState::from_requirepass(None);
        let mut env = TestEnv::new(1);
        match run(&acl, "default", &mut env, &[b"ACL", b"CAT"]) {
            Value::Array(Some(items)) => assert!(items.len() >= 10),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn save_returns_serialized_text_side_effect() {
        let acl = AclState::from_requirepass(None);
        let mut env = TestEnv::new(1);
        let (_v, eff) = dispatch_acl(&acl, "default", env.rng(), &req(&[b"ACL", b"SAVE"]));
        match eff {
            AclSideEffect::Save(text) => assert!(text.contains("user default")),
            other => panic!("expected Save, got {other:?}"),
        }
    }

    #[test]
    fn unknown_subcommand_rejected() {
        let acl = AclState::from_requirepass(None);
        let mut env = TestEnv::new(1);
        match run(&acl, "default", &mut env, &[b"ACL", b"BOGUS"]) {
            Value::Error(e) => {
                assert!(e.message().contains("Unknown") || e.message().contains("subcommand"));
            }
            other => panic!("expected error, got {other:?}"),
        }
    }
}
