// SPDX-License-Identifier: MIT OR Apache-2.0
//! Tier-0 command dispatch (COMMANDS.md, PROTOCOL.md "Tier 0 connection
//! commands"). Maps a parsed [`Request`] to a [`Value`] reply, mutating the
//! per-connection [`ConnState`] where a command does (HELLO, SELECT, RESET,
//! CLIENT SETNAME, AUTH, QUIT).
//!
//! Dispatch is case-insensitive on the command token. Unknown commands return the
//! verbatim `ERR unknown command '...'` from the catalog. PR-1 implements only the
//! handshake/connection tier; data commands (GET/SET/...) arrive with the store
//! in PR-2.

use crate::conn::ConnState;
use crate::{cmd_keyspace, cmd_string};
use ironcache_env::Clock;
use ironcache_observe::{CounterSnapshot, ServerInfo, build_info};
use ironcache_protocol::{ErrorReply, ProtoVersion, Request, Value};
use ironcache_storage::{Store, UnixMillis};

/// Immutable, server-wide context a handler may read. It is cloned cheaply onto
/// each shard; the dynamic per-rollup counters are passed in separately.
#[derive(Debug, Clone)]
pub struct ServerContext {
    /// The configured password, if any. `None` means auth is not required.
    pub requirepass: Option<String>,
    /// Number of logical databases (`SELECT` range is `[0, databases)`).
    pub databases: u32,
    /// Static server facts for INFO/HELLO.
    pub info: ServerInfo,
}

impl ServerContext {
    /// Whether a password is configured (and therefore auth is required).
    #[must_use]
    pub fn requires_auth(&self) -> bool {
        self.requirepass.is_some()
    }
}

/// A source of the rolled-up counters for INFO. The serve loop supplies the
/// current per-shard snapshot (PR-1 reports the local shard's view; the
/// cross-shard rollup wires in with the coordinator later).
pub type RollupFn<'a> = &'a dyn Fn() -> CounterSnapshot;

/// Dispatch one request to its handler, returning the reply [`Value`].
///
/// `clock` provides INFO uptime through the Env seam (no direct time). `store` is
/// the per-shard storage waist (#34) the data commands run against; `now` is the
/// absolute wall-clock deadline basis for this command, computed once per command
/// by the caller from the Env clock (ADR-0003: the store reads no clock). `state`
/// is the mutable per-connection state. `rollup` yields the counters for INFO.
///
/// Tier-0 (connection) commands ignore `store`/`now`; the data commands use them.
/// The function is generic over `S: Store` for monomorphization, consistent with
/// the existing `C: Clock` generic.
pub fn dispatch<C: Clock, S: Store>(
    ctx: &ServerContext,
    state: &mut ConnState,
    clock: &C,
    store: &mut S,
    now: UnixMillis,
    rollup: RollupFn<'_>,
    req: &Request,
) -> Value {
    let cmd = ascii_upper(req.command());

    // Auth gate: before authenticating, only a small set of commands is allowed
    // (Redis: HELLO, AUTH, QUIT, RESET). Everything else (including the data
    // commands) is NOAUTH.
    if ctx.requires_auth()
        && !state.authenticated
        && !matches!(cmd.as_slice(), b"AUTH" | b"HELLO" | b"QUIT" | b"RESET")
    {
        return Value::error(ErrorReply::noauth());
    }

    let db = state.db;
    match cmd.as_slice() {
        b"PING" => cmd_ping(req),
        b"ECHO" => cmd_echo(req),
        b"HELLO" => cmd_hello(ctx, state, req),
        b"AUTH" => cmd_auth(ctx, state, req),
        b"SELECT" => cmd_select(ctx, state, req),
        b"QUIT" => {
            state.should_close = true;
            Value::ok()
        }
        b"RESET" => {
            state.reset(ctx.requires_auth());
            Value::SimpleString("RESET".to_owned())
        }
        b"CLIENT" => cmd_client(state, req),
        b"COMMAND" => cmd_command(req),
        b"INFO" => cmd_info(ctx, clock, rollup, req),
        b"CONFIG" => cmd_config_stub(req),
        // -- Data commands (PR-2a) over the storage waist. --
        b"GET" => cmd_string::cmd_get(store, db, now, req),
        b"SET" => cmd_string::cmd_set(store, db, now, req),
        b"SETNX" => cmd_string::cmd_setnx(store, db, now, req),
        b"GETSET" => cmd_string::cmd_getset(store, db, now, req),
        b"STRLEN" => cmd_string::cmd_strlen(store, db, now, req),
        b"DEL" => cmd_keyspace::cmd_del(store, db, now, req),
        b"EXISTS" => cmd_keyspace::cmd_exists(store, db, now, req),
        b"TYPE" => cmd_keyspace::cmd_type(store, db, now, req),
        _ => {
            let name = String::from_utf8_lossy(req.command()).into_owned();
            let rest: Vec<&[u8]> = req.args[1..].iter().map(bytes::Bytes::as_ref).collect();
            Value::error(ErrorReply::unknown_command(&name, &rest))
        }
    }
}

/// `PING` -> `+PONG`; `PING msg` -> bulk `msg`.
fn cmd_ping(req: &Request) -> Value {
    match req.args.len() {
        1 => Value::simple("PONG"),
        2 => Value::BulkString(Some(req.args[1].clone())),
        _ => Value::error(ErrorReply::wrong_arity("ping")),
    }
}

/// `ECHO msg` -> bulk `msg`.
fn cmd_echo(req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("echo"));
    }
    Value::BulkString(Some(req.args[1].clone()))
}

/// `HELLO [proto] [AUTH user pass] [SETNAME name]` (CONNECTION_LIFECYCLE.md).
///
/// With no version it reports the server map and keeps the current proto;
/// `HELLO 2`/`HELLO 3` switch; any other version is `-NOPROTO`. AUTH and SETNAME
/// options are applied in order before the reply is built.
fn cmd_hello(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
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
            AuthResult::Ok => state.authenticated = true,
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
fn cmd_auth(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
    let (user, pass): (&[u8], &[u8]) = match req.args.len() {
        2 => (b"default", &req.args[1]),
        3 => (&req.args[1], &req.args[2]),
        _ => return Value::error(ErrorReply::wrong_arity("auth")),
    };
    match check_auth(ctx, user, pass) {
        AuthResult::Ok => {
            state.authenticated = true;
            Value::ok()
        }
        AuthResult::NoPasswordSet => Value::error(ErrorReply::auth_no_password_set()),
        AuthResult::WrongPass => Value::error(ErrorReply::wrongpass()),
    }
}

enum AuthResult {
    Ok,
    NoPasswordSet,
    WrongPass,
}

/// Check credentials against the configured password. PR-1 supports the single
/// `requirepass`/default-user model (full ACL is later). The username must be
/// `default` (or empty) when a password is set.
fn check_auth(ctx: &ServerContext, user: &[u8], pass: &[u8]) -> AuthResult {
    match &ctx.requirepass {
        None => AuthResult::NoPasswordSet,
        Some(configured) => {
            let user_ok = user.is_empty() || user.eq_ignore_ascii_case(b"default");
            if user_ok && pass == configured.as_bytes() {
                AuthResult::Ok
            } else {
                AuthResult::WrongPass
            }
        }
    }
}

/// `SELECT index` (PROTOCOL.md Tier-0). Validates the range `[0, databases)`.
fn cmd_select(ctx: &ServerContext, state: &mut ConnState, req: &Request) -> Value {
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
    Value::ok()
}

/// `CLIENT <subcommand>` (handshake-critical subset, PROTOCOL.md).
fn cmd_client(state: &mut ConnState, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("client"));
    }
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
        b"NO-EVICT" | b"NO-TOUCH" => Value::ok(),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "CLIENT",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// A single-line CLIENT INFO description (subset of Redis fields).
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

/// `COMMAND [DOCS|COUNT|...]` (startup stubs, PROTOCOL.md).
fn cmd_command(req: &Request) -> Value {
    if req.args.len() == 1 {
        // Bare COMMAND returns the (empty in PR-1) command table as an array.
        return Value::Array(Some(vec![]));
    }
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        // COUNT: number of supported commands. PR-1 reports 0 (table not yet
        // generated); clients that call COUNT tolerate any integer.
        b"COUNT" => Value::Integer(0),
        // DOCS: an empty map is well-formed and accepted by clients at startup.
        b"DOCS" => Value::Map(vec![]),
        b"GETKEYS" => Value::error(ErrorReply::command_no_key_args()),
        // INFO and any other subcommand: an empty, well-formed array. DELIBERATE
        // divergence from the sibling stubs (CLIENT/CONFIG return an
        // unknown_subcommand error for an unknown sub): COMMAND is probed at client
        // startup with assorted subcommands, and an empty array is more tolerant
        // than an error. Do not "fix" this to unknown_subcommand without checking
        // client startup probes (PR-1 has no command table yet).
        _ => Value::Array(Some(vec![])),
    }
}

/// `INFO [section]` -> delegates to ironcache-observe.
fn cmd_info<C: Clock>(
    ctx: &ServerContext,
    clock: &C,
    rollup: RollupFn<'_>,
    req: &Request,
) -> Value {
    let section = if req.args.len() >= 2 {
        Some(String::from_utf8_lossy(&req.args[1]).into_owned())
    } else {
        None
    };
    let body = build_info(clock, &ctx.info, rollup(), section.as_deref());
    Value::bulk(body.into_bytes())
}

/// `CONFIG GET/SET` minimal stub. PR-1 has no live config command surface; reply
/// well-formed so client startup (which sometimes probes `CONFIG GET save`)
/// does not error: GET returns an empty map/array, SET acks.
fn cmd_config_stub(req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("config"));
    }
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        b"GET" => Value::Map(vec![]),
        b"SET" | b"RESETSTAT" | b"REWRITE" => Value::ok(),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "CONFIG",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

// -- helpers --

/// ASCII-uppercase a byte slice into an owned `Vec<u8>` for case-insensitive
/// command matching (the command token is ASCII per RESP).
fn ascii_upper(b: &[u8]) -> Vec<u8> {
    b.iter().map(u8::to_ascii_uppercase).collect()
}

/// Parse a base-10 i64 from an argument, returning `None` on any non-digit.
fn parse_int_arg(arg: &[u8]) -> Option<i64> {
    let s = core::str::from_utf8(arg).ok()?;
    s.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ironcache_env::{Monotonic, TestEnv};
    use ironcache_store::ShardStore;

    fn ctx(pass: Option<&str>) -> ServerContext {
        ServerContext {
            requirepass: pass.map(str::to_owned),
            databases: 16,
            info: ServerInfo {
                tcp_port: 6379,
                shards: 1,
                pid: 1,
                started_at: Monotonic::ZERO,
                maxmemory: 0,
                mem_allocator: "jemalloc",
            },
        }
    }

    fn state(ctx: &ServerContext) -> ConnState {
        ConnState::new(
            7,
            ProtoVersion::Resp2,
            ctx.requires_auth(),
            "127.0.0.1:1".to_owned(),
            "127.0.0.1:6379".to_owned(),
        )
    }

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    fn run(ctx: &ServerContext, st: &mut ConnState, parts: &[&[u8]]) -> Value {
        let env = TestEnv::new(1);
        let mut store = ShardStore::new(ctx.databases);
        let zero = || CounterSnapshot::default();
        dispatch(ctx, st, &env, &mut store, UnixMillis(0), &zero, &req(parts))
    }

    /// Like [`run`] but threads a caller-owned store and `now`, for the data-command
    /// tests that need state to persist across calls (SET then GET) and a clock to
    /// advance (EX/lazy expiry).
    fn run_on(
        ctx: &ServerContext,
        st: &mut ConnState,
        store: &mut ShardStore,
        now: UnixMillis,
        parts: &[&[u8]],
    ) -> Value {
        let env = TestEnv::new(1);
        let zero = || CounterSnapshot::default();
        dispatch(ctx, st, &env, store, now, &zero, &req(parts))
    }

    #[test]
    fn ping_variants() {
        let c = ctx(None);
        let mut s = state(&c);
        assert_eq!(run(&c, &mut s, &[b"PING"]), Value::simple("PONG"));
        assert_eq!(
            run(&c, &mut s, &[b"ping", b"hi"]),
            Value::BulkString(Some(Bytes::from_static(b"hi")))
        );
        assert_eq!(run(&c, &mut s, &[b"PinG"]), Value::simple("PONG")); // case-insensitive
    }

    #[test]
    fn unknown_command_is_byte_exact() {
        let c = ctx(None);
        let mut s = state(&c);
        let v = run(&c, &mut s, &[b"FROBNICATE", b"a", b"b"]);
        match v {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-ERR unknown command 'FROBNICATE', with args beginning with: 'a' 'b' "
            ),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn hello_no_version_keeps_proto_and_returns_map() {
        let c = ctx(None);
        let mut s = state(&c);
        let v = run(&c, &mut s, &[b"HELLO"]);
        assert!(matches!(v, Value::Map(_)));
        assert_eq!(s.proto, ProtoVersion::Resp2);
    }

    #[test]
    fn hello_3_upgrades_proto() {
        let c = ctx(None);
        let mut s = state(&c);
        let v = run(&c, &mut s, &[b"HELLO", b"3"]);
        assert!(matches!(v, Value::Map(_)));
        assert_eq!(s.proto, ProtoVersion::Resp3);
    }

    #[test]
    fn hello_bad_version_is_noproto_and_does_not_switch() {
        let c = ctx(None);
        let mut s = state(&c);
        let v = run(&c, &mut s, &[b"HELLO", b"4"]);
        match v {
            Value::Error(e) => assert_eq!(e.line(), "-NOPROTO unsupported protocol version"),
            other => panic!("expected NOPROTO, got {other:?}"),
        }
        assert_eq!(s.proto, ProtoVersion::Resp2);
    }

    #[test]
    fn hello_with_setname() {
        let c = ctx(None);
        let mut s = state(&c);
        let _ = run(&c, &mut s, &[b"HELLO", b"3", b"SETNAME", b"app1"]);
        assert_eq!(s.name, "app1");
        assert_eq!(s.proto, ProtoVersion::Resp3);
    }

    #[test]
    fn hello_auth_success_and_failure() {
        let c = ctx(Some("s3cr3t"));
        let mut s = state(&c);
        // Wrong pass -> wrongpass, proto unchanged, not authenticated.
        let v = run(&c, &mut s, &[b"HELLO", b"3", b"AUTH", b"default", b"nope"]);
        assert!(matches!(v, Value::Error(_)));
        assert!(!s.authenticated);
        // Correct pass -> map, authenticated, proto upgraded.
        let v = run(
            &c,
            &mut s,
            &[b"HELLO", b"3", b"AUTH", b"default", b"s3cr3t"],
        );
        assert!(matches!(v, Value::Map(_)));
        assert!(s.authenticated);
        assert_eq!(s.proto, ProtoVersion::Resp3);
    }

    #[test]
    fn auth_no_password_configured() {
        let c = ctx(None);
        let mut s = state(&c);
        let v = run(&c, &mut s, &[b"AUTH", b"whatever"]);
        match v {
            Value::Error(e) => assert!(e.line().starts_with(
                "-ERR AUTH <password> called without any password configured for the default user"
            )),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn noauth_gate_blocks_until_authenticated() {
        let c = ctx(Some("pw"));
        let mut s = state(&c);
        // PING before auth is refused.
        let v = run(&c, &mut s, &[b"PING"]);
        match v {
            Value::Error(e) => assert_eq!(e.line(), "-NOAUTH Authentication required."),
            other => panic!("expected NOAUTH, got {other:?}"),
        }
        // AUTH then PING works.
        assert_eq!(run(&c, &mut s, &[b"AUTH", b"pw"]), Value::ok());
        assert_eq!(run(&c, &mut s, &[b"PING"]), Value::simple("PONG"));
    }

    #[test]
    fn select_range_validation() {
        let c = ctx(None);
        let mut s = state(&c);
        assert_eq!(run(&c, &mut s, &[b"SELECT", b"3"]), Value::ok());
        assert_eq!(s.db, 3);
        match run(&c, &mut s, &[b"SELECT", b"16"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR DB index is out of range"),
            other => panic!("expected range error, got {other:?}"),
        }
        match run(&c, &mut s, &[b"SELECT", b"-1"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR DB index is out of range"),
            other => panic!("expected range error, got {other:?}"),
        }
        match run(&c, &mut s, &[b"SELECT", b"abc"]) {
            Value::Error(e) => assert!(e.line().contains("not an integer")),
            other => panic!("expected int error, got {other:?}"),
        }
    }

    #[test]
    fn quit_sets_close_and_replies_ok() {
        let c = ctx(None);
        let mut s = state(&c);
        assert_eq!(run(&c, &mut s, &[b"QUIT"]), Value::ok());
        assert!(s.should_close);
    }

    #[test]
    fn reset_clears_state() {
        let c = ctx(None);
        let mut s = state(&c);
        let _ = run(&c, &mut s, &[b"HELLO", b"3", b"SETNAME", b"x"]);
        let _ = run(&c, &mut s, &[b"SELECT", b"5"]);
        let v = run(&c, &mut s, &[b"RESET"]);
        assert_eq!(v, Value::SimpleString("RESET".to_owned()));
        assert_eq!(s.proto, ProtoVersion::Resp2);
        assert_eq!(s.db, 0);
        assert_eq!(s.name, "");
    }

    #[test]
    fn client_subcommands() {
        let c = ctx(None);
        let mut s = state(&c);
        assert_eq!(run(&c, &mut s, &[b"CLIENT", b"ID"]), Value::Integer(7));
        assert_eq!(
            run(&c, &mut s, &[b"CLIENT", b"SETNAME", b"app"]),
            Value::ok()
        );
        assert_eq!(s.name, "app");
        assert_eq!(
            run(&c, &mut s, &[b"CLIENT", b"GETNAME"]),
            Value::bulk_str("app")
        );
        // Name with space rejected.
        assert!(matches!(
            run(&c, &mut s, &[b"CLIENT", b"SETNAME", b"a b"]),
            Value::Error(_)
        ));
        // INFO is a bulk string mentioning the id.
        match run(&c, &mut s, &[b"CLIENT", b"INFO"]) {
            Value::BulkString(Some(b)) => {
                assert!(String::from_utf8_lossy(&b).contains("id=7"));
            }
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    #[test]
    fn command_stubs_well_formed() {
        let c = ctx(None);
        let mut s = state(&c);
        assert!(matches!(
            run(&c, &mut s, &[b"COMMAND"]),
            Value::Array(Some(_))
        ));
        assert_eq!(run(&c, &mut s, &[b"COMMAND", b"COUNT"]), Value::Integer(0));
        assert!(matches!(
            run(&c, &mut s, &[b"COMMAND", b"DOCS"]),
            Value::Map(_)
        ));
    }

    #[test]
    fn info_delegates_and_includes_port() {
        let c = ctx(None);
        let mut s = state(&c);
        match run(&c, &mut s, &[b"INFO"]) {
            Value::BulkString(Some(b)) => {
                assert!(String::from_utf8_lossy(&b).contains("tcp_port:6379"));
            }
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    // -- Data commands (PR-2a) through dispatch over a real ShardStore. --

    fn bulk(b: &[u8]) -> Value {
        Value::BulkString(Some(Bytes::copy_from_slice(b)))
    }

    #[test]
    fn set_then_get_round_trips() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"foo", b"bar"]),
            Value::ok()
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"foo"]),
            bulk(b"bar")
        );
        // Missing key -> null.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"nope"]),
            Value::Null
        );
    }

    #[test]
    fn set_nx_only_when_absent() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v1", b"NX"]),
            Value::ok()
        );
        // Second NX on a present key -> nil, value unchanged.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v2", b"NX"]),
            Value::Null
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"v1"));
    }

    #[test]
    fn set_xx_only_when_present() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        // XX on absent key -> nil, nothing written.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v", b"XX"]),
            Value::Null
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
        // Create, then XX overwrite works.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v1"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v2", b"XX"]),
            Value::ok()
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"v2"));
    }

    #[test]
    fn set_get_returns_old_value() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"old"]);
        // SET k new XX GET -> returns old, writes new.
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", b"k", b"new", b"XX", b"GET"]
            ),
            bulk(b"old")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]),
            bulk(b"new")
        );
        // SET GET on an absent key returns null and writes the new value.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SET", b"fresh", b"v", b"GET"]),
            Value::Null
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"fresh"]),
            bulk(b"v")
        );
    }

    #[test]
    fn set_keepttl_preserves_deadline() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        // Set with a 100-second TTL at t=0 (deadline 100000ms).
        run_on(
            &c,
            &mut s,
            &mut st,
            UnixMillis(0),
            &[b"SET", b"k", b"a", b"EX", b"100"],
        );
        // KEEPTTL overwrite at t=1000: value changes, deadline preserved.
        run_on(
            &c,
            &mut s,
            &mut st,
            UnixMillis(1_000),
            &[b"SET", b"k", b"b", b"KEEPTTL"],
        );
        // Alive AT the original deadline (Valkey boundary is `now > deadline`).
        assert_eq!(
            run_on(&c, &mut s, &mut st, UnixMillis(100_000), &[b"GET", b"k"]),
            bulk(b"b")
        );
        // Expired one ms past the original deadline (KEEPTTL kept it, did not extend).
        assert_eq!(
            run_on(&c, &mut s, &mut st, UnixMillis(100_001), &[b"GET", b"k"]),
            Value::Null
        );
    }

    #[test]
    fn set_ex_stores_deadline_and_lazy_expires() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        // EX 10 at t=0 -> deadline 10000ms.
        run_on(
            &c,
            &mut s,
            &mut st,
            UnixMillis(0),
            &[b"SET", b"k", b"v", b"EX", b"10"],
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, UnixMillis(9_999), &[b"GET", b"k"]),
            bulk(b"v")
        );
        // Alive AT the deadline (Valkey boundary is `now > deadline`).
        assert_eq!(
            run_on(&c, &mut s, &mut st, UnixMillis(10_000), &[b"GET", b"k"]),
            bulk(b"v")
        );
        // Expired one ms past the deadline.
        assert_eq!(
            run_on(&c, &mut s, &mut st, UnixMillis(10_001), &[b"GET", b"k"]),
            Value::Null
        );
    }

    #[test]
    fn set_conflicting_options_is_syntax_error() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        for opts in [
            vec![b"SET".as_slice(), b"k", b"v", b"NX", b"XX"],
            vec![b"SET", b"k", b"v", b"EX", b"1", b"PX", b"1"],
            vec![b"SET", b"k", b"v", b"EX", b"1", b"KEEPTTL"],
            vec![b"SET", b"k", b"v", b"BOGUS"],
        ] {
            match run_on(&c, &mut s, &mut st, t, &opts) {
                Value::Error(e) => assert_eq!(e.line(), "-ERR syntax error", "{opts:?}"),
                other => panic!("expected syntax error for {opts:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn set_non_positive_or_overflowing_expire_is_invalid_expire_time() {
        // Redis emits `-ERR invalid expire time in 'set' command` (a class DISTINCT
        // from syntax error) for an EX/PX/EXAT/PXAT value <= 0 or one that overflows
        // the millisecond computation. Nothing is written.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        for opts in [
            vec![b"SET".as_slice(), b"k", b"v", b"EX", b"0"],
            vec![b"SET", b"k", b"v", b"EX", b"-1"],
            vec![b"SET", b"k", b"v", b"PX", b"0"],
            vec![b"SET", b"k", b"v", b"EXAT", b"0"],
            vec![b"SET", b"k", b"v", b"PXAT", b"0"],
            // EX * 1000 overflows i64 -> invalid expire (an integer, but out of ms range).
            vec![b"SET", b"k", b"v", b"EX", b"9223372036854775807"],
        ] {
            match run_on(&c, &mut s, &mut st, t, &opts) {
                Value::Error(e) => assert_eq!(
                    e.line(),
                    "-ERR invalid expire time in 'set' command",
                    "{opts:?}"
                ),
                other => panic!("expected invalid expire time for {opts:?}, got {other:?}"),
            }
        }
        // No key was ever written.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
    }

    #[test]
    fn set_non_integer_expire_is_not_an_integer_error() {
        // A NON-integer expire argument is the shared not-an-integer error, thrown
        // BEFORE the <= 0 check (a distinct class from invalid expire time). A
        // leading '+' is also rejected (Redis string2ll rejects '+').
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        for opts in [
            vec![b"SET".as_slice(), b"k", b"v", b"EX", b"abc"],
            vec![b"SET", b"k", b"v", b"PX", b"1.5"],
            vec![b"SET", b"k", b"v", b"EX", b"+5"],
        ] {
            match run_on(&c, &mut s, &mut st, t, &opts) {
                Value::Error(e) => assert_eq!(
                    e.line(),
                    "-ERR value is not an integer or out of range",
                    "{opts:?}"
                ),
                other => panic!("expected not-an-integer for {opts:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn setnx_and_getset() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SETNX", b"k", b"v1"]),
            Value::Integer(1)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"SETNX", b"k", b"v2"]),
            Value::Integer(0)
        );
        // GETSET returns old and writes new.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GETSET", b"k", b"v3"]),
            bulk(b"v1")
        );
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"v3"));
        // GETSET on absent key returns null.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GETSET", b"new", b"x"]),
            Value::Null
        );
    }

    #[test]
    fn del_and_exists_variadic_counts() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
        // EXISTS counts repeats (Redis semantics).
        assert_eq!(
            run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"EXISTS", b"a", b"a", b"b", b"missing"]
            ),
            Value::Integer(3)
        );
        // DEL removes live keys, returns count removed.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DEL", b"a", b"b", b"missing"]),
            Value::Integer(2)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"a", b"b"]),
            Value::Integer(0)
        );
    }

    #[test]
    fn type_and_strlen() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"k"]),
            Value::simple("none")
        );
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"hello"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"k"]),
            Value::simple("string")
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"k"]),
            Value::Integer(5)
        );
        // STRLEN of an int value is the decimal length.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"-12345"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"n"]),
            Value::Integer(6)
        );
        // STRLEN of an absent key is 0.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"gone"]),
            Value::Integer(0)
        );
    }

    #[test]
    fn wrongtype_on_get_against_non_string() {
        use ironcache_storage::{DataType, Encoding};
        use ironcache_store::kvobj::{Header, KvObj, ValueRepr};

        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);

        // Plant a non-String value directly (PR-2a commands only ever produce
        // Strings, so this is the only way to reach the WRONGTYPE branch before
        // collections land). A List-typed kvobj under key "lst".
        let mut obj = KvObj::from_bytes(b"lst", b"x", None);
        obj.header = Header {
            data_type: DataType::List,
            encoding: Encoding::ListPack,
            eviction_rank: 0,
            ttl_present: false,
            snapshot_version: 0,
        };
        obj.value = ValueRepr::Inline(ironcache_store::kvobj::InlineBuf::from_bytes(b"x"));
        st.insert_object(0, obj);

        // GET / STRLEN / GETSET against the non-string -> WRONGTYPE.
        match run_on(&c, &mut s, &mut st, t, &[b"GET", b"lst"]) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-WRONGTYPE Operation against a key holding the wrong kind of value"
            ),
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
        assert!(matches!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"lst"]),
            Value::Error(_)
        ));
        assert!(matches!(
            run_on(&c, &mut s, &mut st, t, &[b"GETSET", b"lst", b"v"]),
            Value::Error(_)
        ));
        // TYPE never returns WRONGTYPE; it reports the type name.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"lst"]),
            Value::simple("list")
        );
    }

    #[test]
    fn arity_errors_on_data_commands() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = ShardStore::new(c.databases);
        let t = UnixMillis(0);
        for cmd in [
            vec![b"GET".as_slice()],
            vec![b"SET", b"k"],
            vec![b"DEL"],
            vec![b"EXISTS"],
            vec![b"TYPE"],
            vec![b"STRLEN"],
            vec![b"SETNX", b"k"],
            vec![b"GETSET", b"k"],
        ] {
            assert!(
                matches!(run_on(&c, &mut s, &mut st, t, &cmd), Value::Error(_)),
                "expected arity error for {cmd:?}"
            );
        }
    }
}
