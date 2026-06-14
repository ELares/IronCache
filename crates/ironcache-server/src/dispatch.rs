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

use crate::admission::is_denyoom;
use crate::conn::ConnState;
use crate::{cmd_expire, cmd_keyspace, cmd_string};
use ironcache_env::Clock;
use ironcache_expiry::TimingWheel;
use ironcache_observe::{CounterDeltas, CounterSnapshot, MemoryInfo, ServerInfo, build_info};
use ironcache_protocol::{ErrorReply, ProtoVersion, Request, Value};
use ironcache_storage::{ActiveExpiry, Admit, Store, UnixMillis};

/// The bounded number of expired keys the active timing-wheel drain reclaims per
/// command (EXPIRATION.md "bounded reclamation"). A small cap keeps the drain off the
/// command path's critical section: a flood of co-expiring keys is reclaimed across
/// several commands rather than stalling one. The lazy backstop still prevents
/// OBSERVING an expired key, so this bound only governs how fast resident memory for
/// expired keys returns, never correctness.
///
/// This is the cap for the OPPORTUNISTIC (per-command) drain. The PR-3c background
/// timer task for IDLE shards calls the SAME [`drain_due_keys`] helper with its own
/// per-cycle cap ([`crate::MAX_RECLAIM_PER_CYCLE`]); both paths share the one bounded
/// drain so there is no duplicate reclamation logic (EXPIRATION.md idle-shard memory
/// boundedness).
pub const MAX_RECLAIM_PER_CALL: usize = 20;

/// The bounded number of expired keys the PR-3c per-shard BACKGROUND timer task
/// reclaims per cycle (EXPIRATION.md idle-shard memory boundedness). The timer task is
/// what keeps an IDLE shard's resident memory bounded: an opportunistic
/// (per-command) drain only fires when a command arrives, so a shard with no traffic
/// would otherwise accumulate expired-but-not-reclaimed values until the next command.
/// The per-cycle cap is larger than [`MAX_RECLAIM_PER_CALL`] (the background task is
/// off the command critical section, so it may reclaim more aggressively per cycle),
/// but still bounded so one cycle never monopolizes the shard's single thread. It is a
/// #8-tunable internal default, not a wire-exposed knob.
pub const MAX_RECLAIM_PER_CYCLE: usize = 100;

/// The interval between background active-expiry cycles on each shard (the Redis `hz`
/// analog, EXPIRATION.md). The PR-3c timer task awaits `Runtime::timer(EXPIRE_CYCLE_INTERVAL)`
/// then drains a bounded batch, so an idle shard reclaims expired memory roughly every
/// interval even with no traffic. 100ms matches the timing-wheel bottom-level
/// resolution ([`ironcache_expiry::TICK_MS`]), so the active drain keeps pace with the
/// finest deadline bucket. A #8-tunable internal default, not a wire knob; the timer
/// FIRING schedule is wall-clock and does NOT affect observable behavior (the lazy
/// backstop guarantees no expired key is ever observed regardless of when cleanup runs).
pub const EXPIRE_CYCLE_INTERVAL: core::time::Duration = core::time::Duration::from_millis(100);

/// Drain a BOUNDED batch of due keys from the timing `wheel` at `now` and reap the
/// ones whose stored deadline has actually passed (EXPIRATION.md active reclamation).
/// Returns the number of keys ACTUALLY reaped (the `expired_keys` contribution).
///
/// This is the SINGLE bounded-drain helper SHARED by both active-reclamation paths
/// (EXPIRATION.md "runs on the owning core"):
/// - the OPPORTUNISTIC per-command drain in [`dispatch`] (cap [`MAX_RECLAIM_PER_CALL`]),
/// - the PR-3c per-shard BACKGROUND timer task for idle shards (its own per-cycle cap),
///
/// so the advance-and-reap logic lives in one place. The wheel may offer a STALE entry
/// (a re-TTL'd / PERSISTed / overwritten key); [`ActiveExpiry::reap_if_expired`]
/// re-checks the store's real `expire_at`, so only a genuinely-expired key is reaped
/// and counted. `max` caps the work so neither path stalls. The lazy backstop in the
/// store remains the correctness guarantee; this is purely the memory optimization.
///
/// Determinism (ADR-0003): the WORK (which keys are due) is decided entirely by the
/// `now` the caller reads from the Env clock; the helper itself reads no clock. So a
/// background timer firing on wall-clock time does not change observable behavior, but
/// the keys it reaps for a given `now` are byte-identical on a seeded replay.
pub fn drain_due_keys<S: Store + ActiveExpiry>(
    wheel: &mut TimingWheel,
    store: &mut S,
    now: UnixMillis,
    max: usize,
) -> u64 {
    let mut reaped = 0u64;
    for (db, key) in wheel.advance(now, max) {
        if store.reap_if_expired(db, &key, now) {
            reaped += 1;
        }
    }
    reaped
}

/// Immutable, server-wide context a handler may read. It is cloned cheaply onto
/// each shard; the dynamic per-rollup counters are passed in separately.
#[derive(Debug, Clone)]
pub struct ServerContext {
    /// The configured password, if any. `None` means auth is not required.
    pub requirepass: Option<String>,
    /// Number of logical databases (`SELECT` range is `[0, databases)`).
    pub databases: u32,
    /// The resolved memory ceiling in bytes (`maxmemory`). `0` means unlimited: the
    /// admission gate is OFF and every write is served (ADR-0007 unlimited posture).
    pub maxmemory: u64,
    /// The PER-SHARD byte budget the admission gate enforces against this shard's
    /// `used_memory()`: `maxmemory / shards`, computed ONCE at boot. The maxmemory
    /// ceiling is split evenly across shards (shared-nothing, ADR-0002): each shard
    /// owns a slice of the budget and evicts/`-OOM`s against its own slice, with no
    /// cross-shard counter on the hot path. Exact per-arena attribution (ADR-0006) is
    /// a later follow-up; the even split is the honest per-shard approximation for
    /// 3a. `0` when `maxmemory == 0` (unlimited).
    pub per_shard_budget: u64,
    /// Static server facts for INFO/HELLO.
    pub info: ServerInfo,
}

impl ServerContext {
    /// Whether a password is configured (and therefore auth is required).
    #[must_use]
    pub fn requires_auth(&self) -> bool {
        self.requirepass.is_some()
    }

    /// Whether the memory ceiling is enabled (a non-zero `maxmemory`). When `false`,
    /// admission is a no-op and every write is served.
    #[must_use]
    pub fn ceiling_enabled(&self) -> bool {
        self.maxmemory > 0 && self.per_shard_budget > 0
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
/// is the mutable per-connection state. `rollup` yields the counters for INFO;
/// `mem` is the process-global allocator snapshot (ADR-0006) the caller read ONCE at
/// the binary edge for INFO `used_memory`/`used_memory_rss` (the server crate cannot
/// read the concrete store's mallctl by the layering contract, so the figure is
/// supplied in).
///
/// Tier-0 (connection) commands ignore `store`/`now`; the data commands use them.
/// The function is generic over `S: Store + Admit` for monomorphization, consistent
/// with the existing `C: Clock` generic. The [`Admit`] bound lets the dispatcher
/// enforce the maxmemory ceiling (evict-to-fit / `-OOM`) without naming the concrete
/// store or policy.
///
/// `deltas` is an out-parameter dispatch accumulates this command's dynamic counter
/// changes into (eviction count, active-expiry reclamation count, keyspace hits/misses);
/// it starts zeroed and the serve loop folds it into the shard's [`ShardCounters`]
/// AFTER dispatch returns. It is a `&mut` out-parameter rather than a counter handle so
/// dispatch does not alias the `rollup` closure's borrow of the same per-shard counters.
///
/// `wheel` is the per-shard timing wheel (#51): dispatch drains a BOUNDED batch of due
/// keys from it BEFORE the command body (the active reclamation), and the TTL-setting
/// commands register their new deadline into it.
///
/// The arguments are each a distinct, orthogonal seam (ctx/state/clock/store/wheel/now/
/// rollup/mem/deltas/req) the dispatcher fans out to handlers; bundling them into a
/// struct would just move the same fields behind one name and obscure the per-command
/// borrows (the lifetime-parameterized `rollup` closure in particular). The over-7-args
/// lint is allowed here with that justification.
#[allow(clippy::too_many_arguments)]
pub fn dispatch<C: Clock, S: Store + Admit + ActiveExpiry>(
    ctx: &ServerContext,
    state: &mut ConnState,
    clock: &C,
    store: &mut S,
    wheel: &mut TimingWheel,
    now: UnixMillis,
    rollup: RollupFn<'_>,
    mem: MemoryInfo,
    deltas: &mut CounterDeltas,
    req: &Request,
) -> Value {
    *deltas = CounterDeltas::default();
    let cmd = ascii_upper(req.command());

    // Active TTL reclamation (EXPIRATION.md #51), BEFORE the command body: drain a
    // BOUNDED batch of due keys from the timing wheel and reap the ones whose stored
    // deadline has actually passed (the wheel may offer a stale entry; the store
    // re-checks). This bounds resident memory for expired keys under traffic; the lazy
    // backstop in the store still prevents OBSERVING an expired key, so this is purely
    // a memory optimization. MAX_RECLAIM_PER_CALL caps the work per command so the
    // drain never stalls the command path. The SAME [`drain_due_keys`] helper backs the
    // PR-3c background timer task for idle shards (no duplicate drain logic).
    deltas.expired += drain_due_keys(wheel, store, now, MAX_RECLAIM_PER_CALL);

    // Auth gate: before authenticating, only a small set of commands is allowed
    // (Redis: HELLO, AUTH, QUIT, RESET). Everything else (including the data
    // commands) is NOAUTH.
    if ctx.requires_auth()
        && !state.authenticated
        && !matches!(cmd.as_slice(), b"AUTH" | b"HELLO" | b"QUIT" | b"RESET")
    {
        return Value::error(ErrorReply::noauth());
    }

    // maxmemory admission (ADMISSION.md #128, ADR-0007). For a `denyoom` write, before
    // the command body: if the ceiling is enabled and this shard is STRICTLY OVER its
    // budget, either evict-to-fit (cache mode) or reply `-OOM` (datastore/noeviction).
    // The comparison is strict `>` to match Redis's getMaxmemoryState (evict.c):
    // memory is "under limit" when `used <= maxmemory`, so a write at EXACTLY
    // used==budget is served, and only used>budget triggers eviction/-OOM (the -OOM
    // string itself reads "used memory > 'maxmemory'"). Non-denyoom commands (reads,
    // DEL, Tier-0) are ALWAYS served, even over budget, so a client can read and free
    // under pressure.
    if ctx.ceiling_enabled() && is_denyoom(cmd.as_slice()) {
        let budget = ctx.per_shard_budget;
        if store.used_memory() > budget {
            if store.policy_evicts() {
                // Cache mode: try to free space, then re-check. If eviction cannot get
                // us down to budget (write outruns eviction, or only ineligible keys
                // remain), reject -OOM. The freed count is reported for INFO.
                deltas.evicted = store.evict_to_fit(budget, now);
                if store.used_memory() > budget {
                    return Value::error(ErrorReply::oom());
                }
            } else {
                // Strict datastore / noeviction: -OOM is the over-capacity behavior.
                return Value::error(ErrorReply::oom());
            }
        }
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
        b"INFO" => cmd_info(ctx, clock, rollup, mem, req),
        b"CONFIG" => cmd_config_stub(req),
        // -- Data commands (PR-2a) over the storage waist. The two pure reads (GET,
        // STRLEN) feed the keyspace hit/miss counters (PR-3b): a found live key is a
        // hit, an absent/expired key a miss. --
        b"GET" => keyspace_counted(deltas, cmd_string::cmd_get(store, db, now, req)),
        b"SET" => cmd_string::cmd_set(store, wheel, db, now, req),
        b"SETNX" => cmd_string::cmd_setnx(store, db, now, req),
        b"GETSET" => cmd_string::cmd_getset(store, db, now, req),
        // STRLEN is intentionally NOT keyspace-counted: its absent reply Integer(0) is
        // indistinguishable from STRLEN of an empty string, so a reply-shape signal
        // would misclassify; the lookup-side hit/miss is left as a later refinement.
        b"STRLEN" => cmd_string::cmd_strlen(store, db, now, req),
        // -- Numeric RMW + APPEND (PR-2b) over the storage waist. --
        b"INCR" => cmd_string::cmd_incr(store, db, now, req),
        b"DECR" => cmd_string::cmd_decr(store, db, now, req),
        b"INCRBY" => cmd_string::cmd_incrby(store, db, now, req),
        b"DECRBY" => cmd_string::cmd_decrby(store, db, now, req),
        b"INCRBYFLOAT" => cmd_string::cmd_incrbyfloat(store, db, now, req),
        b"APPEND" => cmd_string::cmd_append(store, db, now, req),
        b"DEL" => cmd_keyspace::cmd_del(store, db, now, req),
        b"EXISTS" => cmd_keyspace::cmd_exists(store, db, now, req),
        b"TYPE" => cmd_keyspace::cmd_type(store, db, now, req),
        // -- TTL / EXPIRE family (PR-3b) over the frozen waist. TTL-setting commands
        // also register their new deadline in the per-shard timing wheel. --
        b"EXPIRE" => cmd_expire::cmd_expire(store, wheel, db, now, req),
        b"PEXPIRE" => cmd_expire::cmd_pexpire(store, wheel, db, now, req),
        b"EXPIREAT" => cmd_expire::cmd_expireat(store, wheel, db, now, req),
        b"PEXPIREAT" => cmd_expire::cmd_pexpireat(store, wheel, db, now, req),
        // TTL / PTTL / EXPIRETIME / PEXPIRETIME are TTL-family INTROSPECTION and use
        // LOOKUP_NOTOUCH in Redis: they do NOT update keyspace_hits/keyspace_misses
        // (src/expire.c ttlGenericCommand / expiretimeGenericCommand). Only GET/GETEX
        // count (the #8 fix).
        b"TTL" => cmd_expire::cmd_ttl(store, db, now, req),
        b"PTTL" => cmd_expire::cmd_pttl(store, db, now, req),
        b"EXPIRETIME" => cmd_expire::cmd_expiretime(store, db, now, req),
        b"PEXPIRETIME" => cmd_expire::cmd_pexpiretime(store, db, now, req),
        b"PERSIST" => cmd_expire::cmd_persist(store, db, now, req),
        b"GETEX" => keyspace_counted(deltas, cmd_expire::cmd_getex(store, wheel, db, now, req)),
        b"SETEX" => cmd_expire::cmd_setex(store, wheel, db, now, req),
        b"PSETEX" => cmd_expire::cmd_psetex(store, wheel, db, now, req),
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

/// `INFO [section]` -> delegates to ironcache-observe. `mem` is the process-global
/// allocator snapshot (ADR-0006) the caller read once at the binary edge (the
/// server crate has no access to the concrete store's mallctl readers, by the
/// layering contract; the binary supplies the figure).
fn cmd_info<C: Clock>(
    ctx: &ServerContext,
    clock: &C,
    rollup: RollupFn<'_>,
    mem: MemoryInfo,
    req: &Request,
) -> Value {
    let section = if req.args.len() >= 2 {
        Some(String::from_utf8_lossy(&req.args[1]).into_owned())
    } else {
        None
    };
    let body = build_info(clock, &ctx.info, rollup(), mem, section.as_deref());
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

/// Fold a read command's hit/miss into the keyspace counters (PR-3b, INFO
/// `keyspace_hits`/`keyspace_misses`), then return the reply unchanged.
///
/// A MISS is the "key not found" reply shape: a `Null` bulk (GET/GETEX absent). An
/// `Error` reply (e.g. WRONGTYPE) is NEITHER a hit nor a miss (it is not a successful
/// lookup result). Everything else is a HIT (the key was found live). This is applied
/// only to GET / GETEX, whose reply shape is an UNAMBIGUOUS found/not-found signal and
/// which Redis counts (a real keyspace LOOKUP). The TTL-family introspection commands
/// (TTL/PTTL/EXPIRETIME/PEXPIRETIME) use LOOKUP_NOTOUCH and are NOT counted (the #8
/// fix); STRLEN's reply collides with a real value (0) so it is also not counted.
fn keyspace_counted(deltas: &mut CounterDeltas, reply: Value) -> Value {
    match &reply {
        Value::Error(_) => {}
        // A `Null` bulk (GET/GETEX absent) is a miss; anything else (a found value) is
        // a hit.
        Value::Null => deltas.keyspace_misses += 1,
        _ => deltas.keyspace_hits += 1,
    }
    reply
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ironcache_env::{Monotonic, TestEnv};
    use ironcache_eviction::{Policy, map_policy_name};
    use ironcache_storage::CountingAccounting;
    use ironcache_store::ShardStore;

    /// The store type the dispatch tests drive: the concrete per-shard store wired
    /// with a real eviction policy (so it satisfies the `Admit` bound dispatch now
    /// requires). Defaults to the cache-mode S3-FIFO policy.
    type TestStore = ShardStore<Policy, CountingAccounting>;

    /// A test store with `databases` DBs and the given policy.
    fn store_with(databases: u32, policy: Policy) -> TestStore {
        ShardStore::with_hooks(databases, policy, CountingAccounting::new())
    }

    /// The default test store (cache-mode S3-FIFO, ceiling off).
    fn test_store(databases: u32) -> TestStore {
        store_with(databases, Policy::cache_default())
    }

    fn ctx(pass: Option<&str>) -> ServerContext {
        ServerContext {
            requirepass: pass.map(str::to_owned),
            databases: 16,
            maxmemory: 0,
            per_shard_budget: 0,
            info: ServerInfo {
                tcp_port: 6379,
                shards: 1,
                pid: 1,
                started_at: Monotonic::ZERO,
                maxmemory: 0,
                maxmemory_policy: "allkeys-lru",
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
        let mut store = test_store(ctx.databases);
        let mut wheel = TimingWheel::new();
        let zero = || CounterSnapshot::default();
        let mut deltas = CounterDeltas::default();
        dispatch(
            ctx,
            st,
            &env,
            &mut store,
            &mut wheel,
            UnixMillis(0),
            &zero,
            MemoryInfo::default(),
            &mut deltas,
            &req(parts),
        )
    }

    /// Like [`run`] but threads a caller-owned store and `now`, for the data-command
    /// tests that need state to persist across calls (SET then GET) and a clock to
    /// advance (EX/lazy expiry).
    fn run_on(
        ctx: &ServerContext,
        st: &mut ConnState,
        store: &mut TestStore,
        now: UnixMillis,
        parts: &[&[u8]],
    ) -> Value {
        let mut wheel = TimingWheel::new();
        run_on_wheel(ctx, st, store, &mut wheel, now, parts)
    }

    /// Like [`run_on`] but threads a caller-owned [`TimingWheel`] (and surfaces the
    /// counter deltas), for the EXPIRE / active-drain tests that need the wheel to
    /// persist across calls (register on one command, drain on a later one).
    fn run_on_wheel(
        ctx: &ServerContext,
        st: &mut ConnState,
        store: &mut TestStore,
        wheel: &mut TimingWheel,
        now: UnixMillis,
        parts: &[&[u8]],
    ) -> Value {
        let (reply, _deltas) = run_on_wheel_deltas(ctx, st, store, wheel, now, parts);
        reply
    }

    /// Like [`run_on_wheel`] but also returns the [`CounterDeltas`] dispatch produced
    /// (the active-drain expiry count and keyspace hit/miss), for the counter tests.
    fn run_on_wheel_deltas(
        ctx: &ServerContext,
        st: &mut ConnState,
        store: &mut TestStore,
        wheel: &mut TimingWheel,
        now: UnixMillis,
        parts: &[&[u8]],
    ) -> (Value, CounterDeltas) {
        let env = TestEnv::new(1);
        let zero = || CounterSnapshot::default();
        let mut deltas = CounterDeltas::default();
        let reply = dispatch(
            ctx,
            st,
            &env,
            store,
            wheel,
            now,
            &zero,
            MemoryInfo::default(),
            &mut deltas,
            &req(parts),
        );
        (reply, deltas)
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
        let mut st = test_store(c.databases);
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
            // PR-2b numeric/append arity.
            vec![b"INCR"],
            vec![b"DECR", b"a", b"b"],
            vec![b"INCRBY", b"k"],
            vec![b"DECRBY", b"k"],
            vec![b"INCRBYFLOAT", b"k"],
            vec![b"APPEND", b"k"],
        ] {
            assert!(
                matches!(run_on(&c, &mut s, &mut st, t, &cmd), Value::Error(_)),
                "expected arity error for {cmd:?}"
            );
        }
    }

    // -- Numeric RMW + APPEND (PR-2b). --

    /// The store-level encoding of `key` in db 0 (for int-encoding assertions). The
    /// command layer only ever sees bytes; the test reaches the store directly to
    /// confirm the result is stored int-encoded, which is the ENCODINGS.md contract.
    fn encoding_of(st: &mut TestStore, key: &[u8]) -> Option<ironcache_storage::Encoding> {
        st.read(0, key, UnixMillis(0)).map(|v| v.encoding())
    }

    fn err_line(v: Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn incr_decr_from_absent_and_existing() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Absent key starts at 0: INCR -> 1.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCR", b"n"]),
            Value::Integer(1)
        );
        // The result is int-encoded.
        assert_eq!(
            encoding_of(&mut st, b"n"),
            Some(ironcache_storage::Encoding::Int)
        );
        // STRLEN reflects the decimal length of the result.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"n"]),
            Value::Integer(1)
        );
        // INCRBY and DECR/DECRBY.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"5"]),
            Value::Integer(6)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DECR", b"n"]),
            Value::Integer(5)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DECRBY", b"n", b"10"]),
            Value::Integer(-5)
        );
        // After several ops the decimal length is 2 ("-5").
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"n"]),
            Value::Integer(2)
        );
        // A negative increment via INCRBY works.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"-5"]),
            Value::Integer(-10)
        );
    }

    #[test]
    fn incr_on_existing_string_set_value() {
        // SET n 10 (stored int-encoded), then INCR/INCRBY/DECR through dispatch.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"10"]);
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCR", b"n"]),
            Value::Integer(11)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"5"]),
            Value::Integer(16)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"DECR", b"n"]),
            Value::Integer(15)
        );
    }

    #[test]
    fn incr_non_integer_value_and_arg_are_not_an_integer() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Non-integer EXISTING value (an embstr) -> not-an-integer.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"s", b"hello"]);
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"s"])),
            "-ERR value is not an integer or out of range"
        );
        // A leading-zero / non-canonical existing string is also rejected (string2ll).
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"z", b"007"]);
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"z"])),
            "-ERR value is not an integer or out of range"
        );
        // Non-integer INCREMENT argument -> not-an-integer.
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"1.5"])),
            "-ERR value is not an integer or out of range"
        );
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"abc"])),
            "-ERR value is not an integer or out of range"
        );
    }

    #[test]
    fn incr_overflow_and_decr_underflow_and_decrby_min_edge() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // INCR of i64::MAX overflows.
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", b"max", b"9223372036854775807"],
        );
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"max"])),
            "-ERR increment or decrement would overflow"
        );
        // DECR of i64::MIN underflows.
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", b"min", b"-9223372036854775808"],
        );
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"DECR", b"min"])),
            "-ERR increment or decrement would overflow"
        );
        // DECRBY key i64::MIN: the decrement cannot be negated -> overflow error.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"x", b"0"]);
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"DECRBY", b"x", b"-9223372036854775808"]
            )),
            "-ERR increment or decrement would overflow"
        );
        // The value was not modified by any of the failed ops.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"x"]), bulk(b"0"));
    }

    #[test]
    fn incr_wrongtype_against_non_string() {
        use ironcache_storage::{DataType, Encoding};
        use ironcache_store::kvobj::{Header, KvObj, ValueRepr};
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
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
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"lst"])),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"INCRBYFLOAT", b"lst", b"1"]
            )),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(
            err_line(run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"lst", b"x"])),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    #[test]
    fn incrbyfloat_wrongtype_beats_invalid_increment() {
        // Redis `incrbyfloatCommand` checks the TYPE before parsing the increment
        // argument, so `INCRBYFLOAT <list-key> abc` is WRONGTYPE, NOT
        // "value is not a valid float" (the malformed increment is irrelevant once
        // the key is the wrong type). Plant a non-string via the store seam as the
        // other WRONGTYPE tests do.
        use ironcache_storage::{DataType, Encoding};
        use ironcache_store::kvobj::{Header, KvObj, ValueRepr};
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
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
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"INCRBYFLOAT", b"lst", b"abc"]
            )),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    #[test]
    fn incrbyfloat_absent_format_and_storage() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Absent -> 0 + 10.5 = "10.5" (bulk string).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBYFLOAT", b"f", b"10.5"]),
            bulk(b"10.5")
        );
        // Stored as a STRING (its decimal); GET returns the same bytes.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"f"]),
            bulk(b"10.5")
        );
        // Add 0.1 -> "10.6" (shortest round-trip, no trailing zeros).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBYFLOAT", b"f", b"0.1"]),
            bulk(b"10.6")
        );
    }

    #[test]
    fn incrbyfloat_integer_valued_result_round_trips_to_incr() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // 5.0e3 -> "5000" (integer-valued result, no dot), stored as a string that
        // is int-encoded (since "5000" is a canonical integer), so a later INCR
        // works (matching Redis INCRBYFLOAT -> INCR round-trip for integer results).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCRBYFLOAT", b"v", b"5.0e3"]),
            bulk(b"5000")
        );
        assert_eq!(
            encoding_of(&mut st, b"v"),
            Some(ironcache_storage::Encoding::Int)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"INCR", b"v"]),
            Value::Integer(5001)
        );
    }

    #[test]
    fn incrbyfloat_invalid_float_and_nan_inf() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Non-float existing value -> not-a-valid-float.
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"s", b"hello"]);
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"INCRBYFLOAT", b"s", b"1.0"]
            )),
            "-ERR value is not a valid float"
        );
        // Non-float increment argument -> not-a-valid-float.
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"INCRBYFLOAT", b"f", b"abc"]
            )),
            "-ERR value is not a valid float"
        );
        // An infinite increment produces an infinite result -> NaN/Inf error.
        assert_eq!(
            err_line(run_on(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"INCRBYFLOAT", b"f", b"inf"]
            )),
            "-ERR increment would produce NaN or Infinity"
        );
        // None of the failed ops created the key.
        assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"f"]), Value::Null);
    }

    #[test]
    fn append_absent_existing_and_binary_safe() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // Absent: APPEND creates and returns len(value).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"s", b"abc"]),
            Value::Integer(3)
        );
        // Existing string: appends, returns new len.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"s", b"de"]),
            Value::Integer(5)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"s"]),
            bulk(b"abcde")
        );
        // DIVERGENCE (documented in cmd_append): the frozen waist classifies the
        // rebuilt value by LENGTH, so a SHORT append result is embstr where Redis
        // (which never re-embstrs an appended SDS) would report raw. A result over
        // the embstr threshold is raw, which is the promotion the brief pins; assert
        // that explicitly below.
        assert_eq!(
            encoding_of(&mut st, b"s"),
            Some(ironcache_storage::Encoding::EmbStr)
        );
        // Appending past the embstr threshold promotes the result to raw.
        let big = vec![b'q'; 60];
        run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"s", &big]);
        assert_eq!(
            encoding_of(&mut st, b"s"),
            Some(ironcache_storage::Encoding::Raw)
        );
        // Binary-safe append (NUL bytes preserved).
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"b", b"\x00\x01"]),
            Value::Integer(2)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"b", b"\x02"]),
            Value::Integer(3)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"b"]),
            bulk(b"\x00\x01\x02")
        );
    }

    #[test]
    fn append_promotes_int_off_the_int_encoding() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        // SET n 10 is int-encoded; APPEND promotes the concatenation OFF int (to a
        // string encoding). The exact string encoding is length-based in the frozen
        // waist (embstr here for the short "10x"; raw past the threshold).
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"10"]);
        assert_eq!(
            encoding_of(&mut st, b"n"),
            Some(ironcache_storage::Encoding::Int)
        );
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"n", b"x"]),
            Value::Integer(3)
        );
        // "10x" is not an integer -> a string encoding (no longer int), and GET sees
        // the decimal+suffix.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"n"]),
            bulk(b"10x")
        );
        assert_ne!(
            encoding_of(&mut st, b"n"),
            Some(ironcache_storage::Encoding::Int),
            "APPEND must promote off the int encoding"
        );
    }

    // -- maxmemory admission (PR-3a, ADMISSION.md #128, ADR-0007). --

    /// Run a command against a caller-owned store with the ceiling ON, returning the
    /// reply and the number of keys the admission gate evicted.
    fn run_admit(
        ctx: &ServerContext,
        st: &mut ConnState,
        store: &mut TestStore,
        now: UnixMillis,
        parts: &[&[u8]],
    ) -> (Value, u64) {
        let env = TestEnv::new(1);
        let mut wheel = TimingWheel::new();
        let zero = || CounterSnapshot::default();
        let mut deltas = CounterDeltas::default();
        let reply = dispatch(
            ctx,
            st,
            &env,
            store,
            &mut wheel,
            now,
            &zero,
            MemoryInfo::default(),
            &mut deltas,
            &req(parts),
        );
        (reply, deltas.evicted)
    }

    /// A context with the ceiling enabled at `per_shard_budget` bytes (single-shard
    /// tests, so maxmemory == per_shard_budget).
    fn ctx_with_budget(per_shard_budget: u64) -> ServerContext {
        let mut c = ctx(None);
        c.maxmemory = per_shard_budget;
        c.per_shard_budget = per_shard_budget;
        c
    }

    fn err_of(v: Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn noeviction_over_budget_rejects_denyoom_write_with_byte_exact_oom() {
        // Strict datastore mode: a denyoom write at/over the budget gets the exact
        // -OOM string, and nothing is written.
        let c = ctx_with_budget(50);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::NoEviction);
        let t = UnixMillis(0);
        let big = vec![b'v'; 100];
        // The first SET: used_memory starts at 0 (< 50), so the gate lets it through;
        // the store is now over budget.
        let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert_eq!(r, Value::ok());
        assert_eq!(ev, 0);
        assert!(st.used_memory() >= 50);
        // A SECOND denyoom write is rejected -OOM (byte-exact), nothing evicted.
        let (r2, ev2) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k2", &big]);
        assert_eq!(
            err_of(r2),
            "-OOM command not allowed when used memory > 'maxmemory'."
        );
        assert_eq!(ev2, 0, "noeviction evicts nothing");
        // k2 was not written.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"k2"]),
            Value::Null
        );
    }

    #[test]
    fn denyoom_write_at_exactly_used_equals_budget_is_served() {
        // Strict-over semantics (Redis getMaxmemoryState: under-limit at
        // `used <= maxmemory`). With used == budget EXACTLY, a denyoom write is served
        // (the gate's `used > budget` is false), NOT OOM'd, even under `noeviction`.
        let mut probe = store_with(16, Policy::NoEviction);
        let t = UnixMillis(0);
        let big = vec![b'v'; 100];
        // Plant one key with no ceiling, then read the resulting footprint and set the
        // budget to EXACTLY that, so used == budget on the next gated write.
        probe.upsert(
            0,
            b"k",
            ironcache_storage::NewValue::Bytes(&big),
            ironcache_storage::ExpireWrite::Clear,
            t,
        );
        let exact = probe.used_memory();
        assert!(exact > 0);

        let c = ctx_with_budget(exact);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::NoEviction);
        // Replay the same plant against the gated store so used == budget exactly.
        let (r0, ev0) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert_eq!(r0, Value::ok());
        assert_eq!(ev0, 0);
        assert_eq!(
            st.used_memory(),
            exact,
            "used must equal the budget exactly"
        );

        // A denyoom write that does NOT grow memory (overwrite same key, same size) at
        // used == budget is SERVED: the gate is strict `>`, so used==budget passes.
        let (r1, ev1) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert_eq!(r1, Value::ok(), "at used==budget the write must be served");
        assert_eq!(ev1, 0);

        // Now push STRICTLY over the budget (a second, larger key with no ceiling
        // would not be gated; instead grow via the gated path: the first overwrite was
        // served and left used==budget, so a NEW key now tips strictly over and the
        // NEXT denyoom write is OOM'd under noeviction).
        let (r2, _ev2) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k2", &big]);
        // The k2 write happened at used==budget (served), pushing used strictly over.
        assert_eq!(r2, Value::ok());
        assert!(st.used_memory() > exact, "used is now strictly over budget");
        // The FOLLOWING denyoom write is rejected -OOM (strictly over, noeviction).
        let (r3, ev3) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k3", &big]);
        assert_eq!(
            err_of(r3),
            "-OOM command not allowed when used memory > 'maxmemory'."
        );
        assert_eq!(ev3, 0);
    }

    #[test]
    fn cache_mode_at_exactly_budget_serves_without_evicting() {
        // Cache mode mirror of the strict-over boundary: at used == budget the gate is
        // not entered, so evict_to_fit does NOT run and nothing is evicted.
        let mut probe = store_with(16, Policy::cache_default());
        let t = UnixMillis(0);
        let big = vec![b'v'; 100];
        probe.upsert(
            0,
            b"k",
            ironcache_storage::NewValue::Bytes(&big),
            ironcache_storage::ExpireWrite::Clear,
            t,
        );
        let exact = probe.used_memory();

        let c = ctx_with_budget(exact);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::cache_default());
        run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert_eq!(st.used_memory(), exact);
        // Overwrite at used==budget: served, and the eviction gate did not fire.
        let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert_eq!(r, Value::ok());
        assert_eq!(ev, 0, "at used==budget cache mode must not evict");
    }

    #[test]
    fn reads_and_del_are_served_over_budget() {
        // Non-denyoom commands are ALWAYS served even over budget (a client must be
        // able to read and free under memory pressure).
        let c = ctx_with_budget(50);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::NoEviction);
        let t = UnixMillis(0);
        let big = vec![b'v'; 100];
        run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
        assert!(st.used_memory() >= 50);
        // GET still works over budget.
        let (got_get, _) = run_admit(&c, &mut s, &mut st, t, &[b"GET", b"k"]);
        assert_eq!(
            got_get,
            Value::BulkString(Some(Bytes::copy_from_slice(&big)))
        );
        // DEL (memory-releasing) still works over budget and frees space.
        let (got_del, _) = run_admit(&c, &mut s, &mut st, t, &[b"DEL", b"k"]);
        assert_eq!(got_del, Value::Integer(1));
        assert!(st.used_memory() < 50, "DEL freed space");
    }

    #[test]
    fn cache_mode_over_budget_evicts_then_the_write_succeeds() {
        // Cache mode: a denyoom write at the budget triggers evict-to-fit; once there
        // is room the write proceeds and the evicted count is reported.
        let c = ctx_with_budget(300);
        let mut s = state(&c);
        let mut st = store_with(c.databases, Policy::cache_default());
        let t = UnixMillis(0);
        let val = vec![b'v'; 100];
        // Write several keys to get over the 300-byte budget.
        for i in 0u32..5 {
            run_admit(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", format!("k{i}").as_bytes(), &val],
            );
        }
        assert!(
            st.used_memory() >= 300,
            "should be over budget after the fills"
        );
        // The next denyoom write evicts to fit, then succeeds.
        let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"new", &val]);
        assert_eq!(r, Value::ok(), "the write should succeed after eviction");
        assert!(ev > 0, "cache mode should have evicted at least one key");
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"new"]),
            Value::BulkString(Some(Bytes::copy_from_slice(&val)))
        );
    }

    #[test]
    fn wtinylfu_eviction_preserves_a_hot_key_under_the_ceiling() {
        // End-to-end W-TinyLFU through the real evict_to_fit flow (PR-3c): a frequently
        // GET'd key survives eviction under memory pressure while cold keys are evicted
        // (scan resistance). Configure the `allkeys-lfu` policy (now real W-TinyLFU).
        let c = ctx_with_budget(400);
        let mut s = state(&c);
        let mut st = store_with(
            c.databases,
            map_policy_name("allkeys-lfu", 1).expect("allkeys-lfu maps"),
        );
        // Sanity: it is genuinely the W-TinyLFU engine, not a stand-in.
        assert_eq!(st.policy_name(), "allkeys-lfu");
        let t = UnixMillis(0);
        let val = vec![b'v'; 100];

        // Plant the hot key and access it many times so the sketch records high frequency.
        run_admit(&c, &mut s, &mut st, t, &[b"SET", b"hot", &val]);
        for _ in 0..20 {
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"hot"]);
        }
        // Now stream many cold keys, each written once. Eviction must target the cold
        // keys (lowest estimated frequency), never the hot key. Tally the evictions so we
        // can assert eviction actually happened.
        let mut total_evicted = 0u64;
        for i in 0u32..15 {
            let (_r, ev) = run_admit(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", format!("cold{i}").as_bytes(), &val],
            );
            total_evicted += ev;
        }
        // The hot key must still be present (it survived the cold-key flood): scan
        // resistance, the headline W-TinyLFU property.
        assert_eq!(
            run_on(&c, &mut s, &mut st, t, &[b"GET", b"hot"]),
            Value::BulkString(Some(Bytes::copy_from_slice(&val))),
            "the frequently-accessed key must survive W-TinyLFU eviction"
        );
        // Eviction actually happened (the budget is small, so the cold flood forced
        // several evictions). Each was a COLD key, never the hot one (asserted above).
        assert!(
            total_evicted > 0,
            "the cold-key flood must have triggered W-TinyLFU eviction"
        );
        // The keyspace stayed small (bounded by the budget): far fewer than the 16 keys
        // written, since cold keys were continually evicted to make room.
        assert!(
            st.len() < 8,
            "W-TinyLFU kept the resident set bounded under the ceiling ({} keys)",
            st.len()
        );
    }

    #[test]
    fn ceiling_off_serves_every_write() {
        // maxmemory == 0 (unlimited): the gate is off; writes always succeed.
        let c = ctx(None);
        assert!(!c.ceiling_enabled());
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let t = UnixMillis(0);
        let big = vec![b'v'; 10_000];
        for i in 0u32..5 {
            let (r, ev) = run_admit(
                &c,
                &mut s,
                &mut st,
                t,
                &[b"SET", format!("k{i}").as_bytes(), &big],
            );
            assert_eq!(r, Value::ok());
            assert_eq!(ev, 0);
        }
    }

    // -- TTL / EXPIRE family (PR-3b). --

    fn int(v: Value) -> i64 {
        match v {
            Value::Integer(n) => n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    #[test]
    fn expire_sets_ttl_and_ttl_pttl_reflect_it_then_lazy_expires() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        // SET then EXPIRE 10 at t=0 -> deadline 10000ms.
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"SET", b"k", b"v"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"EXPIRE", b"k", b"10"]
            )),
            1
        );
        // TTL ~10s, PTTL ~10000ms.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"TTL", b"k"]
            )),
            10
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"PTTL", b"k"]
            )),
            10_000
        );
        // Alive AT the deadline (Valkey boundary now > deadline).
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(10_000),
                &[b"GET", b"k"]
            ),
            bulk(b"v")
        );
        // Expired one ms past the deadline.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(10_001),
                &[b"GET", b"k"]
            ),
            Value::Null
        );
    }

    #[test]
    fn pexpire_expireat_pexpireat_set_ttl() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"a", b"v"]);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"b", b"v"]);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"d", b"v"]);
        // PEXPIRE a 5000 -> 5000ms.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRE", b"a", b"5000"]
            )),
            1
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PTTL", b"a"]
            )),
            5_000
        );
        // EXPIREAT b 100 (absolute seconds) -> 100000ms.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIREAT", b"b", b"100"]
            )),
            1
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRETIME", b"b"]
            )),
            100_000
        );
        // PEXPIREAT d 250000 (absolute ms).
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIREAT", b"d", b"250000"]
            )),
            1
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRETIME", b"d"]
            )),
            250_000
        );
    }

    #[test]
    fn expire_on_missing_key_replies_zero() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"nope", b"10"]
            )),
            0
        );
    }

    #[test]
    fn expire_past_deadline_deletes_the_key_and_replies_one() {
        // A resolved deadline strictly in the PAST deletes the key and replies 1.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        // now = 100000ms.
        let t = UnixMillis(100_000);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        // EXPIREAT in the past (unix second 1 -> 1000ms, well before now): reply 1,
        // key deleted.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIREAT", b"k", b"1"]
            )),
            1
        );
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]),
            Value::Null
        );
    }

    #[test]
    fn expire_nx_xx_gt_lt_accept_and_reject() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);

        // NX on a key with NO TTL: applies (reply 1). Sets deadline 10000.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"10", b"NX"]
            )),
            1
        );
        // NX again now that a TTL exists: rejected (reply 0).
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"20", b"NX"]
            )),
            0
        );
        // XX with a TTL present: applies (reply 1). Set to 20000.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"20", b"XX"]
            )),
            1
        );
        // GT with a GREATER new expiry (30 > 20): applies.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"30", b"GT"]
            )),
            1
        );
        // GT with a LESSER new expiry (5 < 30): rejected.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"5", b"GT"]
            )),
            0
        );
        // LT with a LESSER new expiry (5 < 30): applies.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"5", b"LT"]
            )),
            1
        );
        // LT with a GREATER new expiry (100 > 5): rejected.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"100", b"LT"]
            )),
            0
        );
    }

    #[test]
    fn expire_gt_lt_treat_no_ttl_as_infinite() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        // A key with NO TTL is treated as +infinity.
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"g", b"v"]);
        // GT against a no-TTL key NEVER applies (nothing is greater than infinity).
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"g", b"10", b"GT"]
            )),
            0
        );
        // Still no TTL.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"g"]
            )),
            -1
        );
        // LT against a no-TTL key ALWAYS applies (anything is less than infinity).
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"l", b"v"]);
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"l", b"10", b"LT"]
            )),
            1
        );
    }

    #[test]
    fn expire_conflicting_options_are_specific_errors() {
        // The three EXPIRE-option conflicts / the unknown token each map to their
        // SPECIFIC Redis message (src/expire.c parseExtendedExpireArgumentsOrReply),
        // NOT the generic syntax error (the #7 fix).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        let cases: &[(&[&[u8]], &str)] = &[
            (
                &[b"EXPIRE", b"k", b"10", b"NX", b"XX"],
                "-ERR NX and XX, GT or LT options at the same time are not compatible",
            ),
            (
                &[b"EXPIRE", b"k", b"10", b"NX", b"GT"],
                "-ERR NX and XX, GT or LT options at the same time are not compatible",
            ),
            (
                &[b"EXPIRE", b"k", b"10", b"NX", b"LT"],
                "-ERR NX and XX, GT or LT options at the same time are not compatible",
            ),
            (
                &[b"EXPIRE", b"k", b"10", b"GT", b"LT"],
                "-ERR GT and LT options at the same time are not compatible",
            ),
            // The unknown-option token is echoed verbatim.
            (
                &[b"EXPIRE", b"k", b"10", b"BOGUS"],
                "-ERR Unsupported option BOGUS",
            ),
        ];
        for (opts, want) in cases {
            match run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, opts) {
                Value::Error(e) => assert_eq!(&e.line(), want, "{opts:?}"),
                other => panic!("expected {want} for {opts:?}, got {other:?}"),
            }
        }
        // GT+XX and LT+XX are LEGAL (no error). With a TTL present XX is satisfied.
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"10"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"20", b"GT", b"XX"]
            )),
            1
        );
    }

    #[test]
    fn expire_lt_xx_independent_gates_drop_xx_on_no_ttl() {
        // The #1 fix: EXPIRE evaluates the existence gate (NX/XX) and the ordering gate
        // (GT/LT) INDEPENDENTLY, and BOTH must pass. `LT XX` on a key with NO current
        // TTL: XX fails (no TTL), so the timeout is NOT set and the reply is 0 even
        // though LT alone (no-TTL = +infinity) would have applied. The old collapsed
        // enum dropped the XX gate and (wrongly) set the TTL.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        // LT XX on a no-TTL key -> reply 0, nothing set.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"10", b"LT", b"XX"]
            )),
            0,
            "LT XX must fail the XX gate on a key with no TTL"
        );
        // TTL is still -1 (no TTL was set).
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            -1,
            "no TTL was set"
        );
        // Now give it a TTL, then LT XX with a SMALLER deadline applies (both gates
        // pass: XX has a TTL, LT is strictly less).
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"100"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRE", b"k", b"50", b"LT", b"XX"]
            )),
            1,
            "LT XX applies when a TTL exists and the new deadline is smaller"
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            50
        );
    }

    #[test]
    fn ttl_pttl_minus_two_minus_one_conventions() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        // Missing key -> -2.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"missing"]
            )),
            -2
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PTTL", b"missing"]
            )),
            -2
        );
        // Present, no TTL -> -1.
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            -1
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PTTL", b"k"]
            )),
            -1
        );
        // EXPIRETIME/PEXPIRETIME conventions too.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRETIME", b"missing"]
            )),
            -2
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRETIME", b"k"]
            )),
            -1
        );
    }

    #[test]
    fn expiretime_pexpiretime_are_absolute() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        // PEXPIREAT to an absolute ms; EXPIRETIME is that / 1000, PEXPIRETIME is it.
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIREAT", b"k", b"123456"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRETIME", b"k"]
            )),
            123_456
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRETIME", b"k"]
            )),
            123 // (123456 + 500) / 1000 = 123 (ms component < 500 rounds down)
        );
    }

    #[test]
    fn expiretime_rounds_to_nearest_second() {
        // EXPIRETIME rounds the absolute ms deadline to the NEAREST second
        // (`(ms + 500) / 1000`, Redis ttlGenericCommand output_abs), so an ms component
        // >= 500 rounds UP. PEXPIRETIME stays exact ms (the #5 fix).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        // 123556ms: ms component 556 >= 500 -> EXPIRETIME rounds up to 124.
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIREAT", b"k", b"123556"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"EXPIRETIME", b"k"]
            )),
            124,
            "(123556 + 500) / 1000 = 124"
        );
        // PEXPIRETIME is the exact ms, unrounded.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRETIME", b"k"]
            )),
            123_556
        );
    }

    #[test]
    fn expire_deadline_equal_to_now_deletes_immediately() {
        // The #6 command-time boundary: a resolved deadline EQUAL to now is treated as
        // already past (Redis checkAlreadyExpired, `when <= now`), so PEXPIREAT k <now>
        // replies 1 and the key is gone the same tick. This is DISTINCT from the store's
        // lazy-read backstop (`now > deadline`, alive at now==deadline), which governs a
        // SET deadline reached later.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let now = UnixMillis(100_000);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, now, &[b"SET", b"k", b"v"]);
        // PEXPIREAT to exactly `now` -> reply 1, key deleted immediately.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                now,
                &[b"PEXPIREAT", b"k", b"100000"]
            )),
            1,
            "deadline == now deletes and replies 1 (checkAlreadyExpired <= now)"
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                now,
                &[b"EXISTS", b"k"]
            )),
            0,
            "key is gone same-tick"
        );
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, now, &[b"GET", b"k"]),
            Value::Null
        );
    }

    #[test]
    fn getex_exat_in_the_past_returns_value_then_deletes() {
        // The #6 boundary for GETEX: an ABSOLUTE EXAT/PXAT deadline at or before now
        // returns the value AND deletes the key (Redis checkAlreadyExpired). A past
        // RELATIVE EX/PX is still the invalid-expire error, not this path.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let now = UnixMillis(100_000);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, now, &[b"SET", b"k", b"v"]);
        // PXAT exactly at now (100000ms): value returned, key deleted.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                now,
                &[b"GETEX", b"k", b"PXAT", b"100000"]
            ),
            bulk(b"v"),
            "GETEX returns the value even when the absolute deadline is past"
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                now,
                &[b"EXISTS", b"k"]
            )),
            0,
            "the key is deleted after the read (past absolute deadline)"
        );
    }

    #[test]
    fn persist_removes_ttl_and_stops_expiring() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"SET", b"k", b"v", b"EX", b"10"],
        );
        // PERSIST removes the TTL -> reply 1.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PERSIST", b"k"]
            )),
            1
        );
        // TTL now -1 (no TTL).
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            -1
        );
        // PERSIST again (no TTL) -> reply 0.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PERSIST", b"k"]
            )),
            0
        );
        // PERSIST on a missing key -> 0.
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PERSIST", b"gone"]
            )),
            0
        );
        // The key no longer expires at the old deadline.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(20_000),
                &[b"GET", b"k"]
            ),
            bulk(b"v")
        );
    }

    #[test]
    fn getex_matrix() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"SET", b"k", b"v", b"EX", b"100"],
        );
        // Bare GETEX returns the value and does NOT change the TTL.
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"k"]),
            bulk(b"v")
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            100
        );
        // GETEX EX 5 sets a new TTL.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"GETEX", b"k", b"EX", b"5"]
            ),
            bulk(b"v")
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            5
        );
        // GETEX PERSIST clears the TTL.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"GETEX", b"k", b"PERSIST"]
            ),
            bulk(b"v")
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            -1
        );
        // GETEX on an absent key -> nil.
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"absent"]),
            Value::Null
        );
        // GETEX PXAT (absolute ms).
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"GETEX", b"k", b"PXAT", b"50000"],
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PEXPIRETIME", b"k"]
            )),
            50_000
        );
    }

    #[test]
    fn getex_wrongtype_and_invalid_expire() {
        use ironcache_storage::{DataType, Encoding};
        use ironcache_store::kvobj::{Header, KvObj, ValueRepr};
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        // GETEX against a non-string -> WRONGTYPE.
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
        match run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"lst"]) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-WRONGTYPE Operation against a key holding the wrong kind of value"
            ),
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
        // GETEX with an invalid (<= 0) expire -> invalid expire time in 'getex'.
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        match run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"GETEX", b"k", b"EX", b"0"],
        ) {
            Value::Error(e) => {
                assert_eq!(e.line(), "-ERR invalid expire time in 'getex' command");
            }
            other => panic!("expected invalid expire time, got {other:?}"),
        }
        // GETEX with conflicting options -> syntax error.
        match run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"GETEX", b"k", b"EX", b"5", b"PERSIST"],
        ) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR syntax error"),
            other => panic!("expected syntax error, got {other:?}"),
        }
    }

    #[test]
    fn setex_psetex_set_value_and_ttl() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        // SETEX k 10 v -> +OK, value set, TTL 10s.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"SETEX", b"k", b"10", b"v"]
            ),
            Value::ok()
        );
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]),
            bulk(b"v")
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"TTL", b"k"]
            )),
            10
        );
        // PSETEX p 5000 v -> TTL 5000ms.
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PSETEX", b"p", b"5000", b"v"]
            ),
            Value::ok()
        );
        assert_eq!(
            int(run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                t,
                &[b"PTTL", b"p"]
            )),
            5_000
        );
    }

    #[test]
    fn setex_psetex_non_positive_is_invalid_expire_and_writes_nothing() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        match run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"SETEX", b"k", b"0", b"v"],
        ) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR invalid expire time in 'setex' command"),
            other => panic!("expected invalid expire time, got {other:?}"),
        }
        match run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PSETEX", b"k", b"-1", b"v"],
        ) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR invalid expire time in 'psetex' command"),
            other => panic!("expected invalid expire time, got {other:?}"),
        }
        // Nothing was written.
        assert_eq!(
            run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]),
            Value::Null
        );
    }

    #[test]
    fn expire_family_arity_errors() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        for cmd in [
            vec![b"EXPIRE".as_slice(), b"k"],
            vec![b"PEXPIRE", b"k"],
            vec![b"TTL"],
            vec![b"PTTL", b"a", b"b"],
            vec![b"PERSIST"],
            vec![b"EXPIRETIME"],
            vec![b"GETEX"],
            vec![b"SETEX", b"k", b"10"],
            vec![b"PSETEX", b"k", b"10"],
        ] {
            assert!(
                matches!(
                    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &cmd),
                    Value::Error(_)
                ),
                "expected arity error for {cmd:?}"
            );
        }
    }

    // -- Active drain + counters (PR-3b). --

    #[test]
    fn active_drain_reclaims_expired_keys_and_bumps_expired_counter() {
        // Set short TTLs, advance now via the dispatch `now`, then issue a command:
        // the active drain pops the due keys from the wheel and reaps them, bumping the
        // expired delta.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        // Establish the wheel origin at t=0 (the first advance only sets the base).
        let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
        // Three keys each with a 1s TTL (deadline 1000ms), registered in the wheel.
        for k in [b"a".as_slice(), b"b", b"c"] {
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"SET", k, b"v", b"EX", b"1"],
            );
        }
        assert_eq!(st.len(), 3);
        // Advance well past the deadline and issue a command: the active drain reaps
        // all three before the command body. The drain count is in the expired delta.
        let (_r, deltas) = run_on_wheel_deltas(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(5_000),
            &[b"PING"],
        );
        assert_eq!(
            deltas.expired, 3,
            "active drain reaped the three expired keys"
        );
        // The store no longer holds them (the drain deleted them, not just the lazy
        // backstop on a read).
        assert_eq!(
            st.len(),
            0,
            "expired keys are resident-evicted by the drain"
        );
    }

    #[test]
    fn active_drain_skips_re_ttld_key_via_store_recheck() {
        // A stale wheel entry (a key whose TTL was extended) must NOT be reaped early:
        // the store re-checks the real expire_at, so the drain skips it.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
        // SET with a 1s TTL (deadline 1000), then EXTEND to 100s (deadline 100000).
        // The wheel still holds the OLD 1000ms registration (a stale entry).
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"SET", b"k", b"v", b"EX", b"1"],
        );
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"EXPIRE", b"k", b"100"],
        );
        // Advance past the OLD deadline (2000ms) but not the new one: the drain offers
        // the stale entry, but the store re-check finds the key NOT expired and skips.
        let (_r, deltas) = run_on_wheel_deltas(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(2_000),
            &[b"PING"],
        );
        assert_eq!(
            deltas.expired, 0,
            "stale wheel entry must not reap a re-TTL'd key"
        );
        assert_eq!(
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(2_000),
                &[b"GET", b"k"]
            ),
            bulk(b"v"),
            "the re-TTL'd key is still alive"
        );
    }

    #[test]
    fn drain_due_keys_helper_reaps_bounded_batch_deterministically() {
        // The SHARED bounded-drain helper (PR-3c) both the opportunistic per-command
        // path and the background timer task call. Drive it directly: register keys with
        // deadlines, advance the TestEnv-equivalent `now` past them, and assert it reaps
        // exactly the due keys, bumps the count, and respects the `max` bound.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        // Establish the wheel origin at t=0.
        let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
        // 5 keys each with a 1s TTL (deadline 1000ms), registered in the wheel via SET EX.
        for k in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"SET", k, b"v", b"EX", b"1"],
            );
        }
        assert_eq!(st.len(), 5);
        // Drain with a small bound (max=2): the helper reaps at most 2 per call.
        let now = UnixMillis(5_000);
        let first = drain_due_keys(&mut wheel, &mut st, now, 2);
        assert!(first <= 2, "the helper respects the max bound");
        // Keep draining until nothing more is due; total reaped is exactly the 5 keys.
        let mut total = first;
        loop {
            let n = drain_due_keys(&mut wheel, &mut st, now, 2);
            if n == 0 {
                break;
            }
            assert!(n <= 2, "every call respects the max bound");
            total += n;
        }
        assert_eq!(total, 5, "the helper reaps exactly the due keys");
        assert_eq!(st.len(), 0, "all expired keys are resident-evicted");

        // Determinism (ADR-0003): a fresh replay against the same registrations + the
        // same `now` reaps the identical count (the helper reads time only via `now`).
        let mut st2 = test_store(c.databases);
        let mut wheel2 = TimingWheel::new();
        let mut s2 = state(&c);
        let _ = run_on_wheel_deltas(
            &c,
            &mut s2,
            &mut st2,
            &mut wheel2,
            UnixMillis(0),
            &[b"PING"],
        );
        for k in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            run_on_wheel(
                &c,
                &mut s2,
                &mut st2,
                &mut wheel2,
                UnixMillis(0),
                &[b"SET", k, b"v", b"EX", b"1"],
            );
        }
        let replay = drain_due_keys(&mut wheel2, &mut st2, now, 100);
        assert_eq!(
            replay, 5,
            "same now + same registrations => same reclamation"
        );
    }

    #[test]
    fn drain_due_keys_helper_skips_stale_re_ttld_entry() {
        // The helper reaps ONLY genuinely-expired keys: a re-TTL'd key whose stale wheel
        // entry is offered is re-checked by the store and skipped (no false reap).
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"SET", b"k", b"v", b"EX", b"1"],
        );
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"EXPIRE", b"k", b"100"],
        );
        // Past the OLD deadline (2000ms) but not the new one: the stale entry is offered,
        // the store re-check finds it live, the helper reaps nothing.
        let reaped = drain_due_keys(&mut wheel, &mut st, UnixMillis(2_000), 100);
        assert_eq!(reaped, 0, "stale wheel entry must not reap a re-TTL'd key");
        assert_eq!(st.len(), 1, "the re-TTL'd key survives");
    }

    #[test]
    fn keyspace_hits_and_misses_are_counted_for_reads() {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        // GET hit.
        let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]);
        assert_eq!((d.keyspace_hits, d.keyspace_misses), (1, 0));
        // GET miss.
        let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"absent"]);
        assert_eq!((d.keyspace_hits, d.keyspace_misses), (0, 1));
        // GETEX is also counted (a real keyspace lookup): a hit on a present key.
        let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"k"]);
        assert_eq!((d.keyspace_hits, d.keyspace_misses), (1, 0));
        let (_r, d) =
            run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"absent"]);
        assert_eq!((d.keyspace_hits, d.keyspace_misses), (0, 1));
    }

    #[test]
    fn ttl_family_does_not_count_keyspace_hits_or_misses() {
        // TTL-family introspection (TTL/PTTL/EXPIRETIME/PEXPIRETIME) uses LOOKUP_NOTOUCH
        // and must NOT bump keyspace_hits/keyspace_misses (the #8 fix), unlike GET/GETEX.
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let t = UnixMillis(0);
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
        for cmd in [
            vec![b"TTL".as_slice(), b"k"],
            vec![b"TTL", b"absent"],
            vec![b"PTTL", b"k"],
            vec![b"PTTL", b"absent"],
            vec![b"EXPIRETIME", b"k"],
            vec![b"EXPIRETIME", b"absent"],
            vec![b"PEXPIRETIME", b"k"],
            vec![b"PEXPIRETIME", b"absent"],
        ] {
            let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &cmd);
            assert_eq!(
                (d.keyspace_hits, d.keyspace_misses),
                (0, 0),
                "{cmd:?} must not count keyspace hits/misses (LOOKUP_NOTOUCH)"
            );
        }
    }

    #[test]
    fn determinism_replay_drives_identical_expiry_sets() {
        // The same command + now sequence replays the identical expiry outcome (the
        // determinism contract, ADR-0003: the wheel + store read time only via `now`).
        let run = || -> (usize, u64) {
            let c = ctx(None);
            let mut s = state(&c);
            let mut st = test_store(c.databases);
            let mut wheel = TimingWheel::new();
            let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
            for i in 0..10u32 {
                run_on_wheel(
                    &c,
                    &mut s,
                    &mut st,
                    &mut wheel,
                    UnixMillis(0),
                    &[b"SET", format!("k{i}").as_bytes(), b"v", b"PX", b"500"],
                );
            }
            let mut total_expired = 0u64;
            for step in [200u64, 600, 1_000, 5_000] {
                let (_r, d) = run_on_wheel_deltas(
                    &c,
                    &mut s,
                    &mut st,
                    &mut wheel,
                    UnixMillis(step),
                    &[b"PING"],
                );
                total_expired += d.expired;
            }
            (st.len(), total_expired)
        };
        let a = run();
        let b = run();
        assert_eq!(a, b, "identical now sequence => identical expiry outcome");
        // All ten keys expired (deadline 500ms, drained by step 600+).
        assert_eq!(a.0, 0);
        assert_eq!(a.1, 10);
    }
}
