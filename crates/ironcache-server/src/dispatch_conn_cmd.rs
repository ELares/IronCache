// SPDX-License-Identifier: MIT OR Apache-2.0
//! Connection + server-admin command handlers split out of `dispatch.rs` (#625): PING/ECHO/LOLWUT,
//! the SAVE/BGSAVE/LASTSAVE + SHUTDOWN + WAIT no-persistence fallbacks, HELLO + AUTH + SELECT, the
//! CLIENT subcommand family (TRACKING/CACHING/LIST/KILL/PAUSE/...), and COMMAND / COMMAND GETKEYS.
//! Each is a self-contained handler returning a RESP `Value`. Behavior-preserving relocation: the
//! bodies are byte-identical to their former in-`dispatch.rs` definitions.

use super::{ServerContext, ascii_upper, parse_int_arg};
use crate::command_spec;
use crate::conn::ConnState;
use ironcache_env::Clock;
use ironcache_protocol::{ErrorReply, ProtoVersion, Request, Value};
use std::sync::Arc;

/// `PING` -> `+PONG`; `PING msg` -> bulk `msg`.
pub(crate) fn cmd_ping(req: &Request) -> Value {
    match req.args.len() {
        1 => Value::simple("PONG"),
        2 => Value::BulkString(Some(req.args[1].clone())),
        _ => Value::error(ErrorReply::wrong_arity("ping")),
    }
}

/// `ECHO msg` -> bulk `msg`.
pub(crate) fn cmd_echo(req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("echo"));
    }
    Value::BulkString(Some(req.args[1].clone()))
}

/// `LOLWUT [VERSION version]` -> a bulk string naming the server and its version. Redis
/// renders generative ASCII art selected by the optional VERSION argument; IronCache returns
/// a small, stable banner so clients and health probes that call LOLWUT get a non-error bulk
/// reply (the observable contract here is a bulk string, never an error, for any probe form).
/// Redis is lenient about the arguments: it draws art for any argument shape and errors ONLY
/// when the VERSION option is given a non-integer value (it parses argv[2] as a long). This
/// matches that leniency, so the only error path is `LOLWUT VERSION <non-integer>`. The art
/// bytes themselves are server-specific and never asserted by clients.
pub(crate) fn cmd_lolwut(req: &Request) -> Value {
    if req.args.len() >= 3
        && req.args[1].eq_ignore_ascii_case(b"VERSION")
        && crate::cmd_util::parse_i64(&req.args[2]).is_none()
    {
        return Value::error(ErrorReply::not_an_integer());
    }
    let banner = format!("IronCache ver. {}\n", ironcache_observe::SERVER_VERSION);
    Value::bulk(banner)
}

/// `SAVE` PERSISTENCE-DISABLED fallback (#58): reached only when the serve layer did NOT
/// intercept the command (no data_dir configured / a path that reaches dispatch directly), so
/// there is nothing to dump through the storage waist. Redis replies `+OK` to a successful SAVE;
/// with persistence off a SAVE is a no-op success (there is no on-disk target). The cross-shard
/// dump + manifest commit is the binary serve layer's job (it holds the concrete stores).
pub(crate) fn cmd_persist_save_fallback(req: &Request) -> Value {
    if req.args.len() != 1 {
        return Value::error(ErrorReply::wrong_arity("save"));
    }
    Value::ok()
}

/// `BGSAVE [SCHEDULE]` PERSISTENCE-DISABLED fallback (#58): the serve-layer non-intercept path.
/// Redis replies `+Background saving started`; with persistence off there is no background save
/// to start, but the reply is the Redis-faithful acknowledgement (a no-op success). Accepts the
/// bare form and an optional trailing arg (Redis BGSAVE SCHEDULE), which is ignored here.
pub(crate) fn cmd_persist_bgsave_fallback(req: &Request) -> Value {
    if req.args.is_empty() {
        return Value::error(ErrorReply::wrong_arity("bgsave"));
    }
    Value::SimpleString("Background saving started".to_owned())
}

/// `LASTSAVE` PERSISTENCE-DISABLED fallback (#58): the serve-layer non-intercept path. Redis
/// returns the unix time of the last successful save as an integer; with no committed save (or
/// persistence off) that is `0`. The real value (the committed manifest's `save_unix_secs`) is
/// reported by the serve layer when persistence is configured.
pub(crate) fn cmd_persist_lastsave_fallback(req: &Request) -> Value {
    if req.args.len() != 1 {
        return Value::error(ErrorReply::wrong_arity("lastsave"));
    }
    Value::Integer(0)
}

/// The resolved save decision a `SHUTDOWN [NOSAVE|SAVE]` carries (#139, SHUTDOWN.md). The serve
/// layer resolves the modifier ONCE via [`parse_shutdown`], then drives the stop sequence: SAVE
/// forces a save-on-exit even with no save policy, NOSAVE suppresses it even with one, and the bare
/// form (`Default`) saves IFF a save policy is configured [redis-shutdown-save-nosave-default].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownMode {
    /// Bare `SHUTDOWN`: save iff a save policy is configured, else exit without saving.
    Default,
    /// `SHUTDOWN SAVE`: force a save-on-exit even when no save policy is configured.
    Save,
    /// `SHUTDOWN NOSAVE`: suppress the save-on-exit even when a save policy is configured.
    NoSave,
}

/// Parse a `SHUTDOWN [NOSAVE|SAVE]` request into its resolved [`ShutdownMode`], or an `-ERR syntax
/// error` for a bad/extra modifier (#139). `SAVE` and `NOSAVE` are the ONLY two modifiers v1 honors
/// (SHUTDOWN.md "the only two modifiers"); the Redis ABORT / FORCE / NOW grammar is deferred (#150).
/// The token is matched case-insensitively (RESP command args are byte slices; Redis matches the
/// SHUTDOWN modifier with a case-insensitive compare). Shared by the serve-layer interception and
/// the [`cmd_shutdown_fallback`] dispatch arm so the two cannot disagree on the grammar.
///
/// # Errors
///
/// Returns an `-ERR syntax error` when there is more than one modifier or the single modifier is
/// neither `SAVE` nor `NOSAVE`.
pub fn parse_shutdown(req: &Request) -> Result<ShutdownMode, ErrorReply> {
    match req.args.get(1..) {
        // Bare `SHUTDOWN`: the default save-iff-policy-configured decision.
        Some([]) | None => Ok(ShutdownMode::Default),
        Some([modifier]) => {
            if modifier.eq_ignore_ascii_case(b"SAVE") {
                Ok(ShutdownMode::Save)
            } else if modifier.eq_ignore_ascii_case(b"NOSAVE") {
                Ok(ShutdownMode::NoSave)
            } else {
                Err(ErrorReply::syntax_error())
            }
        }
        // More than one modifier (e.g. `SHUTDOWN SAVE NOSAVE`) is a syntax error.
        Some(_) => Err(ErrorReply::syntax_error()),
    }
}

/// `SHUTDOWN [NOSAVE|SAVE]` NEVER-INTERCEPTED fallback (#139, SHUTDOWN.md): reached ONLY when the
/// serve layer did NOT intercept the command (a SHUTDOWN reaching dispatch directly, e.g. an EXEC
/// replay inside a transaction). The actual stop sequence -- drain, save-on-exit, process exit-0 --
/// lives in the binary's serve layer, which owns the runtime + the per-shard stores; this generic
/// dispatch path has neither, so it does NOT exit the process here. It still VALIDATES the modifier
/// grammar (so a bad modifier replies `-ERR syntax error` consistently) and otherwise returns `+OK`
/// without acting. A documented minor divergence from Redis (which would exit); the serve-layer
/// interception is the live path for every non-MULTI SHUTDOWN.
pub(crate) fn cmd_shutdown_fallback(req: &Request) -> Value {
    match parse_shutdown(req) {
        Ok(_) => Value::ok(),
        Err(e) => Value::error(e),
    }
}

/// The CURRENT number of in-sync replicas (PROD-9 WAIT): the runtime quorum count the WAIT
/// command reports + the serve layer blocks on. `ctx.in_sync_replicas` is `Some` only in
/// raft-governance mode; on a single node / standalone (the default), it is `None`, so the
/// count is `0` (no replica has acknowledged anything), exactly the value `WAIT N timeout`
/// returns once it times out with no replicas.
#[must_use]
pub fn in_sync_replica_count(ctx: &ServerContext) -> i64 {
    ctx.in_sync_replicas
        .as_ref()
        .map_or(0, |c| i64::try_from(c.count()).unwrap_or(i64::MAX))
}

/// `WAIT numreplicas timeout` NON-BLOCKING fallback (PROD-9): the EXEC-replay / direct-dispatch
/// path. The LIVE blocking WAIT lives in the serve layer (it can park on the timer seam until the
/// quorum is met); this arm validates the two integer args and replies the CURRENT in-sync replica
/// count immediately. Inside an EXEC, Redis's WAIT does not block, so reporting the current count
/// is the faithful non-blocking behavior.
pub(crate) fn cmd_wait_fallback(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("wait"));
    }
    // numreplicas + timeout must both be integers (Redis parses them as longs); a bad value is
    // the not-an-integer error. The values themselves do not change the non-blocking reply (it is
    // the current count), but they must validate.
    if parse_int_arg(&req.args[1]).is_none() || parse_int_arg(&req.args[2]).is_none() {
        return Value::error(ErrorReply::not_an_integer());
    }
    Value::Integer(in_sync_replica_count(ctx))
}

/// `HELLO [proto] [AUTH user pass] [SETNAME name]` (CONNECTION_LIFECYCLE.md).
///
/// With no version it reports the server map and keeps the current proto;
/// `HELLO 2`/`HELLO 3` switch; any other version is `-NOPROTO`. AUTH and SETNAME
/// options are applied in order before the reply is built.
pub(crate) fn cmd_hello(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
    let mut idx = 1;
    let mut new_proto = state.proto;

    // Optional protocol version (must be the first arg if present and numeric).
    if idx < req.args.len() {
        // The version token is only consumed if it parses as a number; otherwise
        // it must be an option keyword (AUTH/SETNAME).
        if let Some(ver) = parse_int_arg(&req.args[idx]) {
            new_proto = match ver {
                2 => ProtoVersion::Resp2,
                3 => ProtoVersion::Resp3,
                _ => return Value::error(ErrorReply::noproto()),
            };
            idx += 1;
        } else if !is_hello_option(&req.args[idx]) {
            // A non-numeric, non-option first token is an unsupported version.
            return Value::error(ErrorReply::noproto());
        }
    }

    // Parse the option tail: AUTH <user> <pass> and SETNAME <name>, in any order.
    let mut pending_auth: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut pending_name: Option<String> = None;
    while idx < req.args.len() {
        let opt = ascii_upper(&req.args[idx]);
        match opt.as_slice() {
            b"AUTH" => {
                if idx + 2 >= req.args.len() {
                    return Value::error(ErrorReply::wrong_arity("hello"));
                }
                pending_auth = Some((req.args[idx + 1].to_vec(), req.args[idx + 2].to_vec()));
                idx += 3;
            }
            b"SETNAME" => {
                if idx + 1 >= req.args.len() {
                    return Value::error(ErrorReply::wrong_arity("hello"));
                }
                pending_name = Some(String::from_utf8_lossy(&req.args[idx + 1]).into_owned());
                idx += 2;
            }
            _ => {
                return Value::error(ErrorReply::hello_syntax_error(&String::from_utf8_lossy(
                    &req.args[idx],
                )));
            }
        }
    }

    // Apply AUTH if provided; a failed AUTH aborts HELLO without switching proto.
    if let Some((user, pass)) = pending_auth {
        match check_auth(ctx, &user, &pass) {
            AuthResult::Ok(u) => apply_auth_success(ctx, state, u),
            AuthResult::NoPasswordSet => {
                return Value::error(ErrorReply::auth_no_password_set());
            }
            AuthResult::WrongPass => return Value::error(ErrorReply::wrongpass()),
        }
    }

    // If auth is required and still not satisfied, HELLO is refused with NOAUTH.
    if ctx.requires_auth() && !state.authenticated {
        return Value::error(ErrorReply::noauth());
    }

    // Commit proto and name only after all checks pass.
    state.proto = new_proto;
    if let Some(name) = pending_name {
        state.name = name;
    }

    hello_map(ctx, state)
}

/// Build the HELLO reply map (server, version, proto, id, mode, role, modules).
fn hello_map(ctx: &ServerContext, state: &ConnState) -> Value {
    let pairs = vec![
        (Value::bulk_str("server"), Value::bulk_str("ironcache")),
        (
            Value::bulk_str("version"),
            Value::bulk_str(ironcache_observe::SERVER_VERSION),
        ),
        (
            Value::bulk_str("proto"),
            Value::Integer(state.proto.as_i64()),
        ),
        (Value::bulk_str("id"), Value::Integer(state.id as i64)),
        (Value::bulk_str("mode"), Value::bulk_str("standalone")),
        (Value::bulk_str("role"), Value::bulk_str("master")),
        (Value::bulk_str("modules"), Value::Array(Some(vec![]))),
    ];
    let _ = ctx;
    Value::Map(pairs)
}

fn is_hello_option(arg: &[u8]) -> bool {
    let u = ascii_upper(arg);
    matches!(u.as_slice(), b"AUTH" | b"SETNAME")
}

/// `AUTH [user] pass` (PROTOCOL.md Tier-0, ERRORS.md auth strings).
///
/// `AUTH <pass>` authenticates as `default` (the legacy single-password path); `AUTH <user>
/// <pass>` authenticates as that ACL user (#106). On success the resolved `Arc<User>` is
/// CACHED on the connection so the per-command authorization check reads it lock-free.
pub(crate) fn cmd_auth(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
    let (user, pass): (&[u8], &[u8]) = match req.args.len() {
        2 => (b"default", &req.args[1]),
        3 => (&req.args[1], &req.args[2]),
        _ => return Value::error(ErrorReply::wrong_arity("auth")),
    };
    match check_auth(ctx, user, pass) {
        AuthResult::Ok(u) => {
            apply_auth_success(ctx, state, u);
            Value::ok()
        }
        AuthResult::NoPasswordSet => Value::error(ErrorReply::auth_no_password_set()),
        AuthResult::WrongPass => Value::error(ErrorReply::wrongpass()),
    }
}

/// Commit a successful authentication onto the connection: mark authenticated and CACHE the
/// resolved ACL user (#106). When the resolved user is the all-permissive default, we cache
/// `None` (the implicit-default fast path) so the per-command enforcement gate skips it and
/// the no-ACL deployment stays byte-identical; a NARROWED user is cached as `Some(Arc<User>)`.
///
/// F1 (live revocation): also record the user's NAME and the registry GENERATION the user was
/// resolved against. The per-command path re-resolves by `acl_user_name` when the generation
/// moves, so a mid-session `ACL SETUSER`/`DELUSER` reaches this connection. The generation is
/// read from the SAME registry the user came from; a concurrent mutation between resolve and
/// this read only makes the cached generation conservatively stale (the next command re-checks).
fn apply_auth_success(ctx: &ServerContext, state: &mut ConnState, user: Arc<crate::acl::User>) {
    state.authenticated = true;
    // Reuse the existing allocation rather than reassign (clippy::assigning_clones).
    state.acl_user_name.clone_from(&user.name);
    state.acl_user_gen = ctx.acl.generation();
    state.acl_user = if user.is_all_permissive() {
        None
    } else {
        Some(user)
    };
}

/// The outcome of an authentication attempt: the resolved ACL user on success.
enum AuthResult {
    /// Authenticated; carries the resolved `Arc<User>` to cache on the connection.
    Ok(Arc<crate::acl::User>),
    /// `AUTH` was issued but no password / ACL user is configured for the target.
    NoPasswordSet,
    /// Wrong username/password pair, or the user is disabled.
    WrongPass,
}

/// Check credentials against the ACL registry (#106). `AUTH <pass>` targets `default`; `AUTH
/// <user> <pass>` targets that user. The registry resolves the user, verifies the password
/// in CONSTANT TIME against the stored SHA-256 digests (or accepts any for a `nopass` user),
/// and gates on the user being enabled. The plaintext guess lives only as `pass` during
/// hashing and is never stored or logged.
///
/// Backward compatibility: with NO requirepass and NO ACL config, the registry holds the
/// `default` `nopass` user, so a bare `AUTH <anything>` for `default` would succeed -- but
/// Redis instead replies `ERR Client sent AUTH, but no password is set` in that posture. We
/// preserve that by reporting [`AuthResult::NoPasswordSet`] when targeting `default` and no
/// requirepass is configured AND no narrower ACL is active. Once an ACL is active (a real
/// `default` password, or any other user), normal resolution applies.
fn check_auth(ctx: &ServerContext, user: &[u8], pass: &[u8]) -> AuthResult {
    let name = if user.is_empty() {
        crate::acl::DEFAULT_USER.to_owned()
    } else {
        String::from_utf8_lossy(user).into_owned()
    };

    let targets_default = name.eq_ignore_ascii_case(crate::acl::DEFAULT_USER);

    // Redis parity: `AUTH <pass>` against the bare default with no password set is an ERR,
    // not a silent success. This is true exactly when targeting `default`, no requirepass is
    // configured, and the ACL registry is otherwise inactive (only the all-permissive
    // default exists). Any active ACL (a default password, or another user) skips this.
    if targets_default && ctx.runtime.requirepass().is_none() && !ctx.acl.is_acl_active() {
        return AuthResult::NoPasswordSet;
    }

    // LEGACY requirepass compatibility for the `default` user (see [`check_default_requirepass`]).
    // `Some(result)` = the requirepass path DECIDED the auth (matched -> Ok, or a nopass default
    // mismatch -> WrongPass); `None` = fall through to the normal ACL verify below.
    if targets_default {
        if let Some(result) = check_default_requirepass(ctx, pass) {
            return result;
        }
    }

    match ctx.acl.authenticate(&name, pass) {
        Some(u) => AuthResult::Ok(u),
        None => AuthResult::WrongPass,
    }
}

/// The LEGACY `requirepass` path for the `default` user (#106 back-compat). `CONFIG SET
/// requirepass` (and boot requirepass) live in the runtime overlay, NOT the ACL registry, so for
/// `default` we ALSO accept the CURRENT runtime requirepass digest -- a `CONFIG SET requirepass`
/// takes effect LIVE for `AUTH <pass>` alongside any ACL `>pass` digests the registry holds. The
/// compare is constant-time over the fixed-width hex digests.
///
/// SECURITY: when a runtime requirepass IS configured it is AUTHORITATIVE. A `CONFIG SET
/// requirepass` does not touch the registry, so the boot-default is still `nopass` (it would
/// accept ANY password). We must NOT let that implicit `nopass` bypass the live requirepass: so a
/// mismatch against the requirepass digest, when the default carries NO explicit ACL password, is
/// `-WRONGPASS` here (not a fall-through to the nopass ACL verify).
///
/// Returns `Some(AuthResult)` when this path DECIDES the auth (digest match -> `Ok` with the live
/// default user; or a nopass-default mismatch -> `WrongPass`); `None` when there is no requirepass,
/// or the default carries explicit ACL passwords (let the caller's ACL verify run those).
fn check_default_requirepass(ctx: &ServerContext, pass: &[u8]) -> Option<AuthResult> {
    let configured_hash = ctx.runtime.requirepass()?;
    let guess_hash = ironcache_config::sha256_hex(pass);
    if constant_time_eq(guess_hash.as_bytes(), configured_hash.as_bytes()) {
        // Resolve the live `default` user to cache (its perms apply); fall back to the all-
        // permissive default if somehow absent (it cannot be deleted).
        let u = ctx
            .acl
            .get_user(crate::acl::DEFAULT_USER)
            .unwrap_or_else(|| Arc::new(crate::acl::User::default_nopass()));
        return Some(AuthResult::Ok(u));
    }
    // Mismatch: only an EXPLICIT ACL password on the default may still authenticate; the implicit
    // boot `nopass` must NOT (it would defeat the requirepass).
    let default_is_nopass = ctx
        .acl
        .get_user(crate::acl::DEFAULT_USER)
        .is_none_or(|u| u.nopass);
    if default_is_nopass {
        Some(AuthResult::WrongPass)
    } else {
        None
    }
}

/// Compare two byte slices in CONSTANT TIME with respect to their CONTENTS: the running
/// time depends only on the slice LENGTHS, never on WHERE the first differing byte is,
/// so an attacker cannot learn a correct password byte-by-byte from response timing
/// (the timing-leak finding). No new dependency: a hand-rolled fold.
///
/// Mechanism: if the lengths differ, return false immediately (length is not secret in
/// this model). Otherwise fold every byte pair into an XOR accumulator and check it is
/// zero at the END, examining ALL bytes regardless of an early mismatch. The accumulator
/// is read through [`std::hint::black_box`] before the final compare so the optimizer
/// cannot prove the loop short-circuitable and re-introduce a data-dependent early exit.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }
    // Defeat any optimization that would let the compiler reintroduce an early-out:
    // force the accumulator to be materialized before the zero test.
    std::hint::black_box(acc) == 0
}

/// `SELECT index` (PROTOCOL.md Tier-0). Validates the range `[0, databases)`.
pub(crate) fn cmd_select(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("select"));
    }
    let Some(idx) = parse_int_arg(&req.args[1]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    if idx < 0 || idx >= i64::from(ctx.databases) {
        return Value::error(ErrorReply::select_out_of_range());
    }
    state.db = idx as u32;
    // Mirror the selected DB into the registry record so CLIENT LIST / CLIENT INFO for a PEER
    // connection reports the live db (PROD-7). A no-op for a direct-dispatch caller not in the
    // registry (tests).
    if let Some(h) = ctx.clients.by_id(state.id) {
        h.db.store(u64::from(state.db), core::sync::atomic::Ordering::Relaxed);
    }
    Value::ok()
}

/// `CLIENT <subcommand>` (handshake-critical subset, PROTOCOL.md).
pub(crate) fn cmd_client<E: Clock>(
    ctx: &ServerContext,
    state: &mut ConnState,
    env: &E,
    req: &Request,
) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("client"));
    }
    // ===================== CO-EDIT CONTRACT with the PER-SUBCOMMAND ACL =====================
    // These match arms are the AUTHORITATIVE list of CLIENT subcommands and the privileged-vs-plain
    // split. Per-subcommand ACL (`+client|info`) mirrors them in `command_spec::CLIENT_SUBCOMMANDS`
    // (the @admin/@dangerous flags) and pins them in
    // `command_spec::tests::client_subcommand_table_matches_dispatch_arms`. If you ADD, REMOVE, or
    // RECLASSIFY an arm, you MUST update BOTH in the same change. SECURITY: LIST/KILL/PAUSE/UNPAUSE/
    // NO-EVICT are @admin+@dangerous (denied by -@dangerous); ID/GETNAME/SETNAME/SETINFO/INFO/
    // NO-TOUCH are @slow @connection (NOT dangerous). A privileged arm mistagged as a plain read in
    // CLIENT_SUBCOMMANDS would let a -@dangerous user run it -- an escalation the pin test cannot
    // catch (it cannot read these arms at runtime).
    // =======================================================================================
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        b"ID" => Value::Integer(state.id as i64),
        b"GETNAME" => Value::bulk_str(&state.name),
        b"SETNAME" => {
            if req.args.len() != 3 {
                return Value::error(ErrorReply::wrong_arity("client|setname"));
            }
            // The name may not contain spaces or newlines (Redis rule).
            if req.args[2]
                .iter()
                .any(|&b| b == b' ' || b == b'\n' || b == b'\r')
            {
                return Value::error(ErrorReply::client_name_invalid_chars());
            }
            state.name = String::from_utf8_lossy(&req.args[2]).into_owned();
            // Mirror the new name into the registry record so CLIENT LIST / CLIENT INFO for THIS
            // connection (and a peer's CLIENT KILL filtering) sees the live name. The registry is
            // node-level; if this connection is not registered (a direct dispatch caller / a test)
            // this is a harmless no-op.
            if let Some(h) = ctx.clients.by_id(state.id) {
                if let Ok(mut g) = h.name.lock() {
                    state.name.clone_into(&mut g);
                }
            }
            Value::ok()
        }
        b"SETINFO" => {
            // CLIENT SETINFO lib-name/lib-ver: accept and ack (clients send it on
            // connect). Arity is `CLIENT SETINFO <attr> <value>`.
            if req.args.len() != 4 {
                return Value::error(ErrorReply::wrong_arity("client|setinfo"));
            }
            Value::ok()
        }
        b"INFO" => Value::bulk_str(&client_info_line(state)),
        // CLIENT LIST [ID id ...] (PROD-7): one text line per live connection from the node-level
        // registry. The optional `ID <id> [id...]` filter selects specific connections.
        b"LIST" => cmd_client_list(ctx, state, req),
        // CLIENT KILL <ID id|ADDR addr|...> (PROD-7): flag a matching connection for close via the
        // registry; the target's serve loop observes the flag and closes after its current batch.
        b"KILL" => cmd_client_kill(ctx, state, req),
        // CLIENT PAUSE ms [WRITE|ALL] (PROD-7): pause command processing node-wide for `ms` ms; the
        // serve loop honors the pause window after each batch. UNPAUSE clears it.
        b"PAUSE" => cmd_client_pause(ctx, env, req),
        b"UNPAUSE" => {
            if req.args.len() != 2 {
                return Value::error(ErrorReply::wrong_arity("client|unpause"));
            }
            ctx.clients.unpause();
            Value::ok()
        }
        // CLIENT NO-EVICT on|off (PROD-7): accept + ack. IronCache does not evict client connection
        // buffers to free memory (the output-buffer cap closes an over-budget connection instead),
        // so the flag is a no-op acked for client compatibility. Arity `CLIENT NO-EVICT <on|off>`.
        b"NO-EVICT" => {
            if req.args.len() != 3 {
                return Value::error(ErrorReply::wrong_arity("client|no-evict"));
            }
            match ascii_upper(&req.args[2]).as_slice() {
                b"ON" | b"OFF" => Value::ok(),
                _ => Value::error(ErrorReply::syntax_error()),
            }
        }
        b"NO-TOUCH" => Value::ok(),
        // CLIENT TRACKING ON|OFF [NOLOOP] (#409): toggle server-assisted client-side caching for
        // this connection. Sets the per-connection flags; the serve layer's read/write hooks do the
        // registration + invalidation, and the OFF/RESET/disconnect transition purges the table.
        b"TRACKING" => cmd_client_tracking(ctx, state, req),
        // CLIENT TRACKINGINFO (#409): the current tracking state (flags / redirect / prefixes).
        b"TRACKINGINFO" => cmd_client_trackinginfo(state, req),
        // CLIENT CACHING YES|NO (#409 stage 3): the one-shot OPTIN/OPTOUT caching gate.
        b"CACHING" => cmd_client_caching(state, req),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "CLIENT",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// The parsed option tail of `CLIENT TRACKING` (everything after `ON`/`OFF`), produced by
/// [`parse_tracking_options`]. Kept separate so the command handler stays short.
// lint-allow: the four flags are independent Redis TRACKING toggles (NOLOOP/BCAST/OPTIN/OPTOUT),
// each a distinct protocol option mirrored 1:1 onto `ConnState`; a state machine would not model
// them more faithfully (OPTIN/OPTOUT exclusivity is validated separately, not encoded in the type).
#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
struct TrackingOpts {
    noloop: bool,
    bcast: bool,
    optin: bool,
    optout: bool,
    /// The `REDIRECT` target id (stage 4); `0` means no redirection.
    redirect: u64,
    /// The `BCAST` `PREFIX` list (stage 2).
    prefixes: Vec<bytes::Bytes>,
}

/// Parse the option tail of `CLIENT TRACKING` (`req.args[3..]`): NOLOOP/BCAST/OPTIN/OPTOUT/PREFIX/
/// REDIRECT (#409). Returns the parsed [`TrackingOpts`] or, on a malformed option, the error `Value`
/// to return to the client.
fn parse_tracking_options(req: &Request) -> Result<TrackingOpts, Value> {
    let mut o = TrackingOpts::default();
    let mut i = 3;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"NOLOOP" => {
                o.noloop = true;
                i += 1;
            }
            b"BCAST" => {
                o.bcast = true;
                i += 1;
            }
            b"OPTIN" => {
                o.optin = true;
                i += 1;
            }
            b"OPTOUT" => {
                o.optout = true;
                i += 1;
            }
            b"PREFIX" => {
                if i + 1 >= req.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                o.prefixes.push(req.args[i + 1].clone());
                i += 2;
            }
            // Stage 4: REDIRECT <id> routes invalidations to another connection (id 0 = no redirect).
            b"REDIRECT" => {
                if i + 1 >= req.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                o.redirect = match core::str::from_utf8(&req.args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(id) => id,
                    None => return Err(Value::error(ErrorReply::err("Invalid client ID"))),
                };
                i += 2;
            }
            _ => return Err(Value::error(ErrorReply::syntax_error())),
        }
    }
    Ok(o)
}

/// `CLIENT TRACKING <ON|OFF> [REDIRECT id] [PREFIX p [PREFIX p ...]] [BCAST] [OPTIN] [OPTOUT]
/// [NOLOOP]` (#409): enable/disable server-assisted client-side caching for this connection. `ON`
/// requires RESP3 mode OR a `REDIRECT` target (stage 4): a RESP2 client has no push type, so its
/// invalidations are routed to a SECOND connection (the redirect target, which `SUBSCRIBE`d
/// `__redis__:invalidate`) as a Pub/Sub `message`. `REDIRECT 0` means no redirection. The redirect
/// target must be a live connection (looked up in the client registry), matching Redis.
fn cmd_client_tracking(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("client|tracking"));
    }
    let on = match ascii_upper(&req.args[2]).as_slice() {
        b"ON" => true,
        b"OFF" => false,
        _ => return Value::error(ErrorReply::syntax_error()),
    };
    let TrackingOpts {
        noloop,
        bcast,
        optin,
        optout,
        redirect,
        prefixes,
    } = match parse_tracking_options(req) {
        Ok(o) => o,
        Err(e) => return e,
    };
    // OPTIN and OPTOUT are mutually exclusive, and neither combines with BCAST (Redis).
    if optin && optout {
        return Value::error(ErrorReply::err(
            "You can't specify both OPTIN mode and OPTOUT mode",
        ));
    }
    if (optin || optout) && bcast {
        return Value::error(ErrorReply::err(
            "OPTIN and OPTOUT are not compatible with BCAST",
        ));
    }
    // PREFIX requires BCAST (Redis), and the prefixes must not overlap (one being a prefix of
    // another would double-deliver an invalidation).
    if !prefixes.is_empty() && !bcast {
        return Value::error(ErrorReply::err(
            "PREFIX option requires BCAST mode to be enabled",
        ));
    }
    for a in 0..prefixes.len() {
        for b in 0..prefixes.len() {
            if a != b && prefixes[a].starts_with(prefixes[b].as_ref()) {
                return Value::error(ErrorReply::err(format!(
                    "Prefix '{}' overlaps with an existing prefix '{}'. Prefixes for a single \
                     client must not overlap.",
                    String::from_utf8_lossy(&prefixes[a]),
                    String::from_utf8_lossy(&prefixes[b])
                )));
            }
        }
    }
    // A non-zero REDIRECT target must be a LIVE connection (Redis looks it up in the client table).
    // Checked only when enabling: `OFF` ignores any REDIRECT, and `REDIRECT 0` means no redirect.
    if on && redirect != 0 && ctx.clients.by_id(redirect).is_none() {
        return Value::error(ErrorReply::err(
            "The client ID you want redirect to does not exist",
        ));
    }
    if on {
        // RESP3 is required UNLESS a redirect target is given: a RESP2 client cannot receive bare
        // `invalidate` pushes, but it CAN route them to a redirect target's SUBSCRIBE.
        if state.proto != ProtoVersion::Resp3 && redirect == 0 {
            return Value::error(ErrorReply::err(
                "Client tracking can be enabled only in RESP3 mode or when a redirection client is \
                 specified via the 'REDIRECT' option",
            ));
        }
        state.tracking_on = true;
        state.tracking_noloop = noloop;
        state.tracking_bcast = bcast;
        state.tracking_prefixes = prefixes;
        state.tracking_optin = optin;
        state.tracking_optout = optout;
        state.tracking_redirect = redirect;
        // A fresh CLIENT TRACKING ON drops any dangling one-shot CACHING flag.
        state.caching_next = None;
    } else {
        state.tracking_on = false;
        state.tracking_noloop = false;
        state.tracking_bcast = false;
        state.tracking_prefixes.clear();
        state.tracking_optin = false;
        state.tracking_optout = false;
        state.tracking_redirect = 0;
        state.caching_next = None;
    }
    Value::ok()
}

/// `CLIENT CACHING YES|NO` (#409 stage 3): set the ONE-SHOT caching flag that the NEXT command's
/// track decision consumes. Valid ONLY when the connection is tracking in OPTIN or OPTOUT mode
/// (Redis errors otherwise). In OPTIN, `YES` opts the next read's keys IN; in OPTOUT, `NO` opts
/// them OUT.
fn cmd_client_caching(state: &mut ConnState, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("client|caching"));
    }
    let yes = match ascii_upper(&req.args[2]).as_slice() {
        b"YES" => true,
        b"NO" => false,
        _ => return Value::error(ErrorReply::syntax_error()),
    };
    if !(state.tracking_optin || state.tracking_optout) {
        return Value::error(ErrorReply::err(
            "CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or \
             OPTOUT mode enabled",
        ));
    }
    state.caching_next = Some(yes);
    Value::ok()
}

/// `CLIENT TRACKINGINFO` (#409): a map of this connection's tracking state. `flags` is `[off]` or
/// `[on]` (plus `bcast`/`optin`/`optout`/`noloop`/`caching-yes`/`caching-no` as set); `redirect` is
/// `-1` when tracking is off, `0` when on with no redirect, or the REDIRECT target id (stage 4);
/// `prefixes` lists the BCAST prefixes. Rendered as a [`Value::Map`] (RESP3 `%`, degrading to a flat
/// array under RESP2).
fn cmd_client_trackinginfo(state: &ConnState, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("client|trackinginfo"));
    }
    let flags = if state.tracking_on {
        let mut f = vec![Value::bulk_str("on")];
        if state.tracking_bcast {
            f.push(Value::bulk_str("bcast"));
        }
        if state.tracking_optin {
            f.push(Value::bulk_str("optin"));
        }
        if state.tracking_optout {
            f.push(Value::bulk_str("optout"));
        }
        if state.tracking_noloop {
            f.push(Value::bulk_str("noloop"));
        }
        // The pending one-shot CLIENT CACHING decision, if set (Redis exposes caching-yes/-no).
        match state.caching_next {
            Some(true) => f.push(Value::bulk_str("caching-yes")),
            Some(false) => f.push(Value::bulk_str("caching-no")),
            None => {}
        }
        f
    } else {
        vec![Value::bulk_str("off")]
    };
    // `redirect`: -1 when tracking is off, the REDIRECT target id when on with a redirect (stage 4),
    // 0 when on with no redirect.
    let redirect = if state.tracking_on {
        i64::try_from(state.tracking_redirect).unwrap_or(i64::MAX)
    } else {
        -1
    };
    // In BCAST mode the prefixes are reported (an empty list means the EMPTY prefix = all keys).
    let prefixes: Vec<Value> = state
        .tracking_prefixes
        .iter()
        .map(|p| Value::bulk(p.clone()))
        .collect();
    Value::Map(vec![
        (Value::bulk_str("flags"), Value::Array(Some(flags))),
        (Value::bulk_str("redirect"), Value::Integer(redirect)),
        (Value::bulk_str("prefixes"), Value::Array(Some(prefixes))),
    ])
}

/// `CLIENT LIST [ID id [id ...]]` (PROD-7): a bulk string of one `id=.. addr=.. ...` line per live
/// connection (Redis CLIENT LIST shape, a subset of fields), newline-separated. The optional `ID`
/// filter restricts the output to the named connection ids. The line for THIS connection reflects
/// its live name/db from `state` (the registry copy is updated on SETNAME/SELECT but `state` is the
/// freshest); other connections render from their registry records.
fn cmd_client_list(ctx: &ServerContext, state: &ConnState, req: &Request) -> Value {
    // Parse an optional `ID <id> [id...]` filter.
    let mut filter: Option<Vec<u64>> = None;
    if req.args.len() >= 3 {
        if !ascii_upper(&req.args[2]).eq_ignore_ascii_case(b"ID") {
            return Value::error(ErrorReply::syntax_error());
        }
        if req.args.len() == 3 {
            return Value::error(ErrorReply::syntax_error());
        }
        let mut ids = Vec::new();
        for a in &req.args[3..] {
            match core::str::from_utf8(a)
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
            {
                Some(id) => ids.push(id),
                None => return Value::error(ErrorReply::err("Invalid client ID")),
            }
        }
        filter = Some(ids);
    }
    let mut body = String::new();
    for h in ctx.clients.snapshot() {
        if let Some(ids) = &filter {
            if !ids.contains(&h.id) {
                continue;
            }
        }
        // For THIS connection, prefer the live `state` name/db/resp (freshest); others render from
        // the registry record.
        if h.id == state.id {
            body.push_str(&client_info_line(state));
        } else {
            body.push_str(&registry_info_line(&h));
        }
        body.push('\n');
    }
    Value::bulk(body.into_bytes())
}

/// `CLIENT KILL ...` (PROD-7). Supports the OLD form `CLIENT KILL addr:port` (returns +OK or an
/// error if no match) and the NEW filter form `CLIENT KILL <ID id|ADDR addr|LADDR addr> [...]`
/// (returns the integer count of connections killed). A connection cannot reach KILL unless it is
/// authorized (the ACL/admin gate ran upstream); the actual close happens in the target's serve
/// loop, which observes the registry kill flag after its current batch.
fn cmd_client_kill(ctx: &ServerContext, state: &ConnState, req: &Request) -> Value {
    // OLD form: exactly one argument that is an addr (`CLIENT KILL 1.2.3.4:5`).
    if req.args.len() == 3 {
        let addr = String::from_utf8_lossy(&req.args[2]).into_owned();
        return if ctx.clients.kill_addr(&addr) {
            Value::ok()
        } else {
            Value::error(ErrorReply::err("No such client"))
        };
    }
    // NEW filter form: `CLIENT KILL <filter value> [<filter value> ...]`, an EVEN tail of
    // filter/value pairs. We support ID, ADDR, and LADDR (the common operator filters); a SKIPME
    // option is accepted for compatibility (default yes -> never kill the caller).
    let rest = &req.args[2..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return Value::error(ErrorReply::syntax_error());
    }
    let mut want_id: Option<u64> = None;
    let mut want_peer_addr: Option<String> = None;
    let mut want_local_addr: Option<String> = None;
    let mut skipme = true;
    for pair in rest.chunks_exact(2) {
        let opt = ascii_upper(&pair[0]);
        let val = String::from_utf8_lossy(&pair[1]).into_owned();
        match opt.as_slice() {
            b"ID" => match val.parse::<u64>() {
                Ok(id) => want_id = Some(id),
                Err(_) => {
                    return Value::error(ErrorReply::err("client-id should be greater than 0"));
                }
            },
            b"ADDR" => want_peer_addr = Some(val),
            b"LADDR" => want_local_addr = Some(val),
            b"SKIPME" => match val.to_ascii_lowercase().as_str() {
                "yes" => skipme = true,
                "no" => skipme = false,
                _ => return Value::error(ErrorReply::syntax_error()),
            },
            // TYPE / USER / MAXAGE: accepted-but-ignored filters for compatibility (a single-tier
            // connection model has no client TYPE distinction). They never match-narrow here.
            b"TYPE" | b"USER" | b"MAXAGE" => {}
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    }
    let mut killed = 0i64;
    for h in ctx.clients.snapshot() {
        if skipme && h.id == state.id {
            continue;
        }
        if let Some(id) = want_id {
            if h.id != id {
                continue;
            }
        }
        if let Some(addr) = &want_peer_addr {
            if &h.addr != addr {
                continue;
            }
        }
        if let Some(laddr) = &want_local_addr {
            if &h.laddr != laddr {
                continue;
            }
        }
        h.kill();
        killed += 1;
    }
    Value::Integer(killed)
}

/// `CLIENT PAUSE <ms> [WRITE|ALL]` (PROD-7): pause command processing node-wide for `ms`
/// milliseconds. `ALL` (the default) pauses all commands; `WRITE` pauses only writes. The serve
/// loop reads the pause window (a monotonic deadline) after each batch and stalls while it is
/// active. The deadline basis is the Env monotonic clock (ADR-0003), passed in by the caller.
fn cmd_client_pause<E: Clock>(ctx: &ServerContext, env: &E, req: &Request) -> Value {
    if req.args.len() != 3 && req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("client|pause"));
    }
    let Some(ms) = core::str::from_utf8(&req.args[2])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return Value::error(ErrorReply::err("timeout is not an integer or out of range"));
    };
    let writes_only = if req.args.len() == 4 {
        match ascii_upper(&req.args[3]).as_slice() {
            b"WRITE" => true,
            b"ALL" => false,
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    } else {
        false
    };
    // The monotonic-millis basis the serve loop also reads: the Env clock's `now()` as millis.
    let now_mono_ms = env.now().as_millis();
    ctx.clients.pause(now_mono_ms, ms, writes_only);
    Value::ok()
}

/// A single-line CLIENT INFO description for THIS connection (subset of Redis fields).
fn client_info_line(state: &ConnState) -> String {
    format!(
        "id={} addr={} laddr={} name={} db={} resp={}",
        state.id,
        state.addr,
        state.laddr,
        state.name,
        state.db,
        state.proto.as_i64()
    )
}

/// A CLIENT LIST line for a PEER connection rendered from its registry record (this connection's
/// `ConnState` is not reachable cross-connection, so the registry holds the load-bearing fields:
/// id / addr / laddr / name / db).
fn registry_info_line(h: &ironcache_observe::ClientHandle) -> String {
    format!(
        "id={} addr={} laddr={} name={} db={}",
        h.id,
        h.addr,
        h.laddr,
        h.name(),
        h.db.load(core::sync::atomic::Ordering::Relaxed),
    )
}

/// `COMMAND [COUNT|INFO|DOCS|LIST|GETKEYS|...]` command introspection (PROTOCOL.md, #158).
///
/// CLUSTER-AWARE CLIENTS need a REAL command table here: a `RedisCluster` (redis-py), go-redis, or
/// ioredis calls bare `COMMAND` at connect to learn each command's key positions so it can compute
/// the slot of a command's keys and route to the owning node. The prior PR-1 stub returned an EMPTY
/// table, which made redis-py raise `"<CMD> command doesn't exist in Redis commands"` and refuse to
/// route ANY keyed op against a cluster. We now project the real table from the single-source
/// [`command_spec`] registry. The SINGLE-NODE path is functionally unaffected (a non-cluster client
/// does not consult the command table to route), so this is purely additive correctness.
pub(crate) fn cmd_command(req: &Request) -> Value {
    if req.args.len() == 1 {
        // Bare COMMAND: the full command table, one flat entry per client-visible command.
        let entries = command_spec::CLIENT_COMMAND_NAMES
            .iter()
            .filter_map(|name| command_spec::spec_of(name).map(command_table_entry))
            .collect();
        return Value::Array(Some(entries));
    }
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        // COUNT: the number of client-visible commands (the real count, not 0).
        b"COUNT" => Value::Integer(command_spec::CLIENT_COMMAND_NAMES.len() as i64),
        // LIST: a flat array of every command name (lowercased, as Redis renders names).
        b"LIST" => Value::Array(Some(
            command_spec::CLIENT_COMMAND_NAMES
                .iter()
                .map(|n| Value::bulk(n.to_ascii_lowercase()))
                .collect(),
        )),
        // INFO [name ...]: one table entry per requested command (NULL array element for an
        // unknown name, matching Redis). Bare `COMMAND INFO` (no names) returns the full table.
        b"INFO" => {
            if req.args.len() == 2 {
                let entries = command_spec::CLIENT_COMMAND_NAMES
                    .iter()
                    .filter_map(|name| command_spec::spec_of(name).map(command_table_entry))
                    .collect();
                return Value::Array(Some(entries));
            }
            let entries = req.args[2..]
                .iter()
                .map(|name| {
                    let upper = name.to_ascii_uppercase();
                    command_spec::spec_of(&upper).map_or(Value::Array(None), command_table_entry)
                })
                .collect();
            Value::Array(Some(entries))
        }
        // GETKEYS <command> [args ...]: extract the routable keys of the supplied command line via
        // the registry's key-spec (the SAME extraction the router uses). This is what a cluster
        // client falls back to for a `movablekeys` command. Errors match Redis's classes.
        b"GETKEYS" => cmd_command_getkeys(req),
        // DOCS: an empty map is well-formed and accepted by clients at startup. (A full DOCS body
        // -- summaries/since/group -- is not needed for routing; clients tolerate an empty map.)
        b"DOCS" => Value::Map(vec![]),
        // Any other subcommand: an empty, well-formed array (COMMAND is probed at client startup
        // with assorted subcommands; an empty array is more tolerant than an error).
        _ => Value::Array(Some(vec![])),
    }
}

/// One `COMMAND` table entry for a [`command_spec::CommandSpec`], as the Redis flat array
/// `[name, arity, [flags], first_key, last_key, step, [acl-cats], [tips], [key-specs], [subcmds]]`
/// (#158). A cluster client reads `name`/`arity`/`flags`/`first_key`/`last_key`/`step` to route;
/// the trailing three (acl-cats/tips/key-specs/subcommands) are emitted EMPTY (well-formed and
/// tolerated -- redis-py reads them only when present).
///
/// `arity` follows the Redis encoding: a POSITIVE n for `Exact(n)`, a NEGATIVE -n for `Min(n)`.
/// `flags` carry the routing-relevant set: `write`/`readonly`, `denyoom`, and `movablekeys` for a
/// command whose keys are option/numkeys-dependent (so the client falls back to `COMMAND GETKEYS`).
fn command_table_entry(spec: &command_spec::CommandSpec) -> Value {
    let arity = match spec.arity {
        // Redis arity encoding: positive n = exactly n total args; negative -n = at least n.
        command_spec::Arity::Exact(n) => i64::try_from(n).unwrap_or(i64::MAX),
        command_spec::Arity::Min(n) => -i64::try_from(n).unwrap_or(i64::MAX),
    };
    let (first_key, last_key, step, movable) = command_spec::command_key_positions(spec);
    let mut flags: Vec<Value> = Vec::new();
    flags.push(Value::simple(if spec.is_write {
        "write"
    } else {
        "readonly"
    }));
    if spec.denyoom {
        flags.push(Value::simple("denyoom"));
    }
    if movable {
        flags.push(Value::simple("movablekeys"));
    }
    Value::Array(Some(vec![
        Value::bulk(spec.name.to_ascii_lowercase()),
        Value::Integer(arity),
        Value::Array(Some(flags)),
        Value::Integer(first_key),
        Value::Integer(last_key),
        Value::Integer(step),
        // acl-categories, tips, key-specs, subcommands: empty (well-formed; not needed for routing).
        Value::Array(Some(vec![])),
        Value::Array(Some(vec![])),
        Value::Array(Some(vec![])),
        Value::Array(Some(vec![])),
    ]))
}

/// `COMMAND GETKEYS <command> [arg ...]` -> the routable keys of the supplied command line (#158).
/// Reuses the registry's [`command_spec::extract_keys`] (the SAME key extraction the cluster router
/// uses), so a cluster client's movable-key fallback agrees byte-for-byte with how the server would
/// route. Redis error parity: a missing inner command line is `wrong_arity`; an unknown inner
/// command is `Invalid command specified`; a known command with no key args is the
/// `command_no_key_args` message.
pub(crate) fn cmd_command_getkeys(req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("command|getkeys"));
    }
    // Build the inner request (the command + its args) the extraction operates on: args[2..].
    let inner = Request {
        args: req.args[2..].to_vec(),
    };
    let upper = ascii_upper(&inner.args[0]);
    let Some(spec) = command_spec::spec_of(&upper) else {
        return Value::error(ErrorReply::err("Invalid command specified"));
    };
    match command_spec::extract_keys(spec.key_spec, &inner) {
        crate::route::KeySpec::None => Value::error(ErrorReply::command_no_key_args()),
        crate::route::KeySpec::One(k) => Value::Array(Some(vec![Value::bulk(k.to_vec())])),
        crate::route::KeySpec::Many(keys) => Value::Array(Some(
            keys.into_iter().map(|k| Value::bulk(k.to_vec())).collect(),
        )),
    }
}
