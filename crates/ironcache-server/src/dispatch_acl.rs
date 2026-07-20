// SPDX-License-Identifier: MIT OR Apache-2.0
//! ACL / pre-auth command gating split out of `dispatch.rs` (#625): the pre-auth allow-list, the
//! stale-ACL refresh, the per-command ACL enforcement, and the SORT external-key deref check. These
//! are the security boundary the dispatch engine + the serve-layer router both consult. Behavior-
//! preserving relocation: the bodies are byte-identical.

use super::{ServerContext, ascii_upper};
use crate::conn::ConnState;
use ironcache_protocol::{ErrorReply, Request};

/// The PRE-AUTH ALLOW-LIST (Redis: `HELLO`, `AUTH`, `QUIT`, `RESET`). With `requirepass`
/// configured, a connection that has NOT yet authenticated may run ONLY these commands;
/// every other command (data, admin, whole-keyspace, CLUSTER mutators, persistence,
/// SHUTDOWN, the cross-shard fan-outs) is `-NOAUTH`. This is the SINGLE SOURCE OF TRUTH
/// for that allow-list: both the downstream `dispatch_with_cmd` gate AND the hoisted
/// serve-layer router chokepoint (`crate::serve::route_and_dispatch`) call it, so the two
/// gates can NEVER diverge on which commands are allowed before auth. `cmd` MUST be the
/// uppercased command token (the only form the callers hold).
///
/// Keep this list IDENTICAL to Redis (`ACLCheckAllPerm` allow-set: HELLO/AUTH/RESET +
/// QUIT, which is connection teardown): do NOT add or remove a command here without a
/// deliberate parity change -- it is the security boundary.
#[inline]
#[must_use]
pub fn command_allowed_pre_auth(cmd: &[u8]) -> bool {
    matches!(cmd, b"AUTH" | b"HELLO" | b"QUIT" | b"RESET")
}

/// LIVE-REVOCATION RE-RESOLVE (F1): bring a connection's cached ACL identity up to date with the
/// registry when a mutation has happened SINCE the identity was cached, run ONCE per command at
/// the router chokepoint right BEFORE [`acl_enforce`]. This is what makes a mid-session `ACL
/// SETUSER app -@all` / `ACL DELUSER app` (or an `ACL SETUSER default ...` narrowing the implicit
/// default) take effect on the offending connection's VERY NEXT command, instead of being fail-open
/// until that client re-AUTHs or disconnects (the F1 finding; Redis revokes live).
///
/// ## Hot path: one relaxed load + integer compare
///
/// The COMMON case is no mutation since the connection cached its user: the live registry
/// `generation` equals `conn.acl_user_gen`, so this returns `true` after ONE relaxed atomic load
/// and an integer compare -- no lock, no allocation, byte-identical on the no-ACL path (where the
/// generation never moves at all). Only when the generation MOVED (rare: an `ACL` admin verb ran)
/// does it take the registry lock to re-resolve the connection's user by `acl_user_name`:
/// - user still present -> refresh `conn.acl_user` (`None` when all-permissive, so a back-to-
///   permissive default re-collapses to the byte-identical fast path; `Some` when narrowed, so a
///   fresh restriction is picked up) and update `conn.acl_user_gen`; returns `true`.
/// - user DELETED (`ACL DELUSER`) -> DEAUTHENTICATE the connection (clear `authenticated` + drop
///   the cached user back to the implicit default and reset the name) so its next command hits the
///   NOAUTH gate, and return `false` so the caller CLOSES it -- mirroring Redis, which kills a
///   deleted user's clients. Closing (vs silently reverting to the all-permissive default) is the
///   safe choice: a no-requirepass deployment would otherwise leave the connection running as the
///   permissive default after its narrowed user was deleted.
///
/// Returns `true` when the connection remains a valid identity (possibly refreshed), `false` when
/// it was deauthenticated and the caller should close it.
#[must_use]
pub fn acl_resolve_if_stale(ctx: &ServerContext, conn: &mut ConnState) -> bool {
    // HOT PATH: one relaxed load + compare. Unchanged generation -> nothing to do.
    if ctx.acl.generation() == conn.acl_user_gen {
        return true;
    }
    // COLD PATH (a mutation happened): re-resolve the connection's user by name under the lock.
    match ctx.acl.resolve_if_stale(&conn.acl_user_name) {
        crate::acl::AclResolution::Refresh { user, generation } => {
            conn.acl_user = user;
            conn.acl_user_gen = generation;
            true
        }
        crate::acl::AclResolution::Deauth => {
            // The user was DELUSER'd: this connection is no longer authenticated AS anyone. Drop
            // it back to the unauthenticated implicit-default baseline so a NEXT command (if the
            // caller did not close) hits NOAUTH, and signal the caller to close (Redis parity).
            conn.authenticated = !ctx.requires_auth();
            conn.acl_user = None;
            // Reuse the existing allocation rather than reassign (clippy::assigning_clones).
            conn.acl_user_name.clear();
            conn.acl_user_name.push_str(crate::acl::DEFAULT_USER);
            conn.acl_user_gen = ctx.acl.generation();
            false
        }
    }
}

/// Does this `SORT` / `SORT_RO` request use a BY/GET option that DEREFERENCES external keys?
///
/// `SORT key ... [BY pat] ... [GET pat ...]` reads keys built by substituting the source
/// element into `pat`. Those keys are NOT part of the command key-spec, so the ACL per-key
/// check cannot see them. The dereferencing forms are:
/// - `BY pat` where `pat` contains a `*` (a `BY` pattern with NO `*` is `nosort` -- it skips
///   sorting and does NOT read any external key, so it is EXEMPT, matching `cmd_sort`);
/// - any `GET pat` that is not exactly `#` (`GET #` projects the element ITSELF, not an
///   external key, so it is EXEMPT).
///
/// This mirrors the option scan in [`crate::cmd_sort`] (BY/GET each consume the next arg) so
/// the two never diverge. It runs ONLY for SORT/SORT_RO under an active, non-allkeys ACL --
/// off the hot path for every other command and for allkeys / ACL-off connections.
#[must_use]
fn sort_derefs_external_keys(req: &Request) -> bool {
    // The option tail begins after the command token and the source key (args[0], args[1]).
    let Some(opts) = req.args.get(2..) else {
        return false;
    };
    let mut i = 0;
    while i < opts.len() {
        let tok = &opts[i];
        if tok.eq_ignore_ascii_case(b"BY") {
            // BY consumes the next arg as its pattern. A `*` in the pattern means it
            // dereferences an external key per element; no `*` is the exempt `nosort` form.
            if let Some(pat) = opts.get(i + 1) {
                if pat.contains(&b'*') {
                    return true;
                }
            }
            i += 2;
        } else if tok.eq_ignore_ascii_case(b"GET") {
            // GET consumes the next arg as its pattern. `GET #` is the element itself (exempt);
            // ANY other GET pattern reads an external key.
            if let Some(pat) = opts.get(i + 1) {
                if pat.as_ref() != b"#" {
                    return true;
                }
            }
            i += 2;
        } else if tok.eq_ignore_ascii_case(b"LIMIT") {
            // LIMIT consumes two args (offset count); skip them so a numeric arg is never
            // mistaken for an option token. (ASC/DESC/ALPHA/STORE consume only themselves;
            // STORE's destination IS in the key-spec, so it is checked by the normal path.)
            i += 3;
        } else {
            i += 1;
        }
    }
    false
}

/// THE PER-COMMAND ACL ENFORCEMENT CHECK (#106). Given the connection's cached ACL identity
/// (`acl_user`, `None` == the implicit all-permissive default), the command token, and the
/// parsed request, decide whether the user may run it. Returns `None` when ALLOWED and
/// `Some(ErrorReply)` (a `-NOPERM`) when DENIED. This is the value the ACL engine adds: it is
/// wired at the router chokepoint right after the existing NOAUTH gate, so it covers EVERY
/// command path (home, cross-shard, whole-keyspace, pubsub, MULTI-queue, CLUSTER mutators).
///
/// ## Hot-path discipline (cheap)
///
/// The COMMON case is the no-ACL deployment: `acl_active` is `false` (one relaxed atomic
/// load) and/or the connection is the implicit default (`acl_user == None`), so this returns
/// `None` after a single bool test -- byte-identical, O(1), no per-command allocation. Only
/// an ACL-governed connection (a narrowed `Some(user)`) pays for the checks:
/// 1. the COMMAND test: the user's compiled command-rule replay (`can_run_command`, O(rules)).
/// 2. the KEY test: ONLY for a key-bearing command, a glob match over its FEW key args
///    (extracted via the #89 command-spec key spec) against the user's key patterns.
/// 3. the CHANNEL test: ONLY for a pub/sub command, over its channel args.
///
/// The pre-auth allow-list commands (AUTH/HELLO/QUIT/RESET) are NEVER denied here (they ran
/// before the user was even resolved); they are short-circuited by `acl_user == None` for the
/// default and explicitly exempted for a narrowed user so a locked-down user can still AUTH /
/// switch users / RESET (Redis: these are always permitted).
#[must_use]
pub fn acl_enforce(
    acl_active: bool,
    acl_user: Option<&crate::acl::User>,
    cmd_upper: &[u8],
    req: &Request,
) -> Option<ErrorReply> {
    // FAST GATE: the no-ACL deployment (no narrowed user anywhere) skips everything. A
    // connection with no cached narrowed user is the implicit all-permissive default and is
    // never denied (the `?` returns `None` == ALLOWED); if ACL is globally inactive there is
    // nothing to enforce either way.
    let user = acl_user?;
    if !acl_active {
        return None;
    }

    // The connection-control / handshake commands are ALWAYS allowed (Redis: a user can
    // always AUTH/HELLO/QUIT/RESET regardless of command perms, so it can re-authenticate or
    // disconnect). This mirrors the pre-auth allow-list.
    if command_allowed_pre_auth(cmd_upper) {
        return None;
    }

    // (a) COMMAND permission. For a CONTAINER command (CLUSTER today) that carries a SUBCOMMAND,
    // the PER-SUBCOMMAND grant decides (so CLUSTER SLOTS, tagged @slow only, is allowed for a
    // `-@dangerous` user while CLUSTER ADDSLOTS, tagged @admin+@dangerous, is NOPERM). The
    // subcommand is uppercased with the SAME `ascii_upper` cmd_cluster.rs uses to dispatch, so ACL
    // and dispatch agree on case. Every other command keeps the whole-command check unchanged --
    // this is the only behavioral change, and it fires only for a narrowed (non-None) user (the
    // acl_user==None default short-circuited above).
    if crate::command_spec::subcommands_of(cmd_upper).is_some() && req.args.len() >= 2 {
        let sub = ascii_upper(&req.args[1]);
        if !user.can_run_command_sub(cmd_upper, Some(&sub)) {
            // NOPERM names the `cmd|sub` pair (lowercased, pipe), Redis 7 parity.
            let cmd_lc = String::from_utf8_lossy(cmd_upper).to_ascii_lowercase();
            let sub_lc = String::from_utf8_lossy(&sub).to_ascii_lowercase();
            return Some(ErrorReply::noperm_command(
                &user.name,
                &format!("{cmd_lc}|{sub_lc}"),
            ));
        }
    } else if !user.can_run_command(cmd_upper) {
        let cmd_lc = String::from_utf8_lossy(cmd_upper).to_ascii_lowercase();
        return Some(ErrorReply::noperm_command(&user.name, &cmd_lc));
    }

    // (c) CHANNEL permission for pub/sub commands (the channel args are the message targets).
    // SUBSCRIBE/UNSUBSCRIBE/PUBLISH take channel name(s) at args[1..]; PUBLISH's args[1] is the
    // channel (args[2] is the message, not a channel, but it is harmless to also pattern-check
    // a non-channel here -- Redis checks only the channel, so restrict to the right arg).
    match cmd_upper {
        b"SUBSCRIBE" | b"UNSUBSCRIBE" | b"PSUBSCRIBE" | b"PUNSUBSCRIBE" => {
            if !user.channels.is_allchannels() {
                for ch in req.args.iter().skip(1) {
                    if !user.can_access_channel(ch) {
                        return Some(ErrorReply::noperm_channel());
                    }
                }
            }
            return None;
        }
        b"PUBLISH" => {
            if !user.channels.is_allchannels() {
                if let Some(ch) = req.args.get(1) {
                    if !user.can_access_channel(ch) {
                        return Some(ErrorReply::noperm_channel());
                    }
                }
            }
            return None;
        }
        _ => {}
    }

    // (b) KEY permission for key-bearing commands. The all-keys fast path skips the whole
    // extraction. Otherwise extract the command's keys via the #89 command-spec key spec and
    // require EVERY touched key to be allowed by the user's key patterns.
    //
    // ONLY genuine KEYED commands (`KeyedSingle`/`KeyedMulti`) are key-checked. A
    // `WholeKeyspace` command (KEYS/SCAN/FLUSHALL/FLUSHDB/DBSIZE/RANDOMKEY) owns no specific
    // key -- its `key_spec` is the `Arg1` fallback that would return the GLOB PATTERN (KEYS
    // <pattern>) as if it were a key -- so it is gated by COMMAND perms (it is @keyspace /
    // @dangerous), NOT key perms, exactly like Redis. `AlwaysHome` commands have no key.
    if !user.keys.is_allkeys() {
        // SORT / SORT_RO BY/GET external-key dereference gate (redis#10106 / redis 7.0).
        // A `BY pattern` containing `*` or a non-`#` `GET pattern` DEREFERENCES external
        // keys (`weight_*`, `data_*->field`) at runtime; those pattern-keys are NOT in the
        // command key-spec, so the per-key check below never sees them. Redis closes this
        // by denying such a SORT unless the user has FULL key-read permission (allkeys).
        // We are already inside `!is_allkeys()`, so any dereferencing form is denied here.
        // The non-dereferencing forms are EXEMPT: a `nosort` BY (no `*`, no deref) and
        // `GET #` (the element itself, not an external key). When ACL is off / the user is
        // allkeys, this block never runs -> default/allkeys/ACL-off byte-identical.
        if matches!(cmd_upper, b"SORT" | b"SORT_RO") && sort_derefs_external_keys(req) {
            return Some(ErrorReply::noperm_key());
        }
        if let Some(spec) = crate::command_spec::spec_of(cmd_upper) {
            if !matches!(
                spec.class,
                crate::command_spec::CommandClass::KeyedSingle
                    | crate::command_spec::CommandClass::KeyedMulti
            ) {
                return None;
            }
            match crate::command_spec::extract_keys(spec.key_spec, req) {
                crate::route::KeySpec::None => {}
                crate::route::KeySpec::One(k) => {
                    if !user.can_access_key(k) {
                        return Some(ErrorReply::noperm_key());
                    }
                }
                crate::route::KeySpec::Many(keys) => {
                    for k in keys {
                        if !user.can_access_key(k) {
                            return Some(ErrorReply::noperm_key());
                        }
                    }
                }
            }
        }
    }

    None
}
