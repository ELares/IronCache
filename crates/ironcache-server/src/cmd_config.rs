// SPDX-License-Identifier: MIT OR Apache-2.0
//! The `CONFIG` command family (CONFIG.md "wire parity", ADMIN_COMMANDS.md, #15/#85,
//! PR-4b): `CONFIG GET`, `CONFIG SET`, `CONFIG RESETSTAT`, `CONFIG REWRITE`,
//! `CONFIG HELP`.
//!
//! `CONFIG GET <pattern...>` globs the parameter registry names (case-insensitive)
//! and returns the matching name->value pairs from the registry's effective-value
//! resolver (the runtime overlay over the boot config). `CONFIG SET <param value
//! ...>` applies one or more sets to the runtime overlay through the registry; an
//! unknown param or a rejected value is the canonical Redis error. A
//! `CONFIG SET maxmemory-policy` bumps the runtime generation so every shard hot-swaps
//! its policy (the swap itself happens at the top of [`crate::dispatch`], not here:
//! this command only mutates the shared cell). `CONFIG RESETSTAT` signals the reset via
//! `deltas.reset_stats`; the serve loop zeroes the serving shard's cell AND (since INFO's
//! stats are now the NODE-WIDE rollup, #531) fans the reset across every shard's cell so
//! INFO's `# Stats` actually zero. `CONFIG REWRITE` returns the Redis no-config-file
//! error (the server currently boots without a config-file path).

use crate::cmd_util::ascii_upper;
use crate::dispatch::ServerContext;
use crate::glob::glob_match;
use ironcache_config::{SetOutcome, apply_set, effective_value, param_specs};
use ironcache_observe::CounterDeltas;
use ironcache_protocol::{ErrorReply, Request, Value};
use zeroize::Zeroizing;

/// `CONFIG <subcommand> [args]` (CONFIG.md / ADMIN_COMMANDS.md). `deltas` carries the
/// per-command counter signal the serve loop folds in; `CONFIG RESETSTAT` sets
/// `deltas.reset_stats` so the serve loop zeroes the serving shard's stat counters AND fans the
/// reset across every shard's cell (#531, so the node-wide INFO rollup zeroes).
pub fn cmd_config(ctx: &ServerContext, deltas: &mut CounterDeltas, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("config"));
    }
    // ===================== CO-EDIT CONTRACT with the PER-SUBCOMMAND ACL =====================
    // These match arms are the AUTHORITATIVE list of CONFIG subcommands. Per-subcommand ACL
    // (`+config|get`) mirrors them in `command_spec::CONFIG_SUBCOMMANDS` (the @admin/@dangerous
    // flags) and pins them in `command_spec::tests::config_subcommand_table_matches_dispatch_arms`.
    // If you ADD or REMOVE an arm here, you MUST update BOTH in the same change. SECURITY: GET/SET/
    // REWRITE/RESETSTAT are @admin+@dangerous (denied by -@dangerous); HELP is @slow only. An arm
    // mistagged in CONFIG_SUBCOMMANDS would mis-gate the ACL; the pin test cannot read these arms.
    // =======================================================================================
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        b"GET" => config_get(ctx, req),
        b"SET" => config_set(ctx, req),
        b"RESETSTAT" => config_resetstat(deltas, req),
        b"REWRITE" => config_rewrite(req),
        b"HELP" => config_help(),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "CONFIG",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// `CONFIG GET <pattern> [pattern ...]` -> the name->value pairs for every registered
/// param whose name matches ANY pattern (glob, case-insensitive). Redis 7 accepts
/// multiple patterns; a param matching more than one is returned ONCE. Unknown/
/// non-matching params are simply omitted (no error). The reply is a `Value::Map`,
/// which the encoder renders as a flat array in RESP2 and a map in RESP3, exactly the
/// Redis CONFIG GET shape.
fn config_get(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("config|get"));
    }
    // The patterns, lowercased so the case-insensitive match works against the
    // lowercase registry names (Redis matches CONFIG GET patterns case-insensitively).
    let patterns: Vec<Vec<u8>> = req.args[2..]
        .iter()
        .map(|a| a.iter().map(u8::to_ascii_lowercase).collect())
        .collect();

    let mut pairs: Vec<(Value, Value)> = Vec::new();
    for spec in param_specs() {
        let name_bytes = spec.name.as_bytes();
        // A param is included if its name matches ANY of the patterns.
        let hit = patterns.iter().any(|p| glob_match(p, name_bytes));
        if !hit {
            continue;
        }
        // The effective value resolves the runtime overlay over the boot config, so a
        // prior CONFIG SET is reflected here (the precedence CONFIG.md mandates).
        if let Some(val) = effective_value(spec.name, &ctx.runtime, &ctx.boot) {
            pairs.push((Value::bulk_str(spec.name), Value::bulk_str(&val)));
        }
    }
    Value::Map(pairs)
}

/// `CONFIG SET <param value> [param value ...]` -> `+OK` if every pair applies, else
/// the FIRST failing pair's canonical error. Redis validates arity and applies
/// atomically-enough: it validates and sets each in turn. For PR-4b we apply each in
/// order; the first failure short-circuits with the canonical error (a later coordinator
/// can make multi-set transactional).
///
/// Arity, matching Redis 7.4 `configSetCommand` exactly:
/// - an ODD number of param/value tokens after SET -> `-ERR syntax error`
///   (`shared.syntaxerr`); the unknown-option message is ONLY for an unrecognized PARAM
///   NAME, never for malformed arity.
/// - ZERO param/value tokens (just `CONFIG SET`) -> `+OK` (a no-op set of nothing).
fn config_set(ctx: &ServerContext, req: &Request) -> Value {
    // `CONFIG SET name value [name value ...]`.
    let rest = &req.args[2..];
    // An ODD token count is a syntax error (Redis `shared.syntaxerr`), NOT the
    // unknown-param message (that is reserved for an unrecognized param NAME below).
    if rest.len() % 2 != 0 {
        return Value::error(ErrorReply::syntax_error());
    }
    // ZERO pairs is a no-op set of nothing: Redis replies +OK (the even-but-empty case).
    if rest.is_empty() {
        return Value::ok();
    }

    for pair in rest.chunks_exact(2) {
        let name = String::from_utf8_lossy(&pair[0]).into_owned();
        // ZEROIZE-ON-DROP (#145): this owned copy of the SET value may be a PLAINTEXT secret (a
        // `CONFIG SET requirepass <pw>`). Wrapping it in `Zeroizing<String>` scrubs the backing
        // bytes the moment this loop iteration drops it, right after `apply_set` has hashed it to a
        // SHA-256 digest at rest in the runtime overlay, so the cleartext password does not linger on
        // the heap for a later core dump / memory disclosure. `Zeroizing` derefs to `str`, so
        // `apply_set` (and the unknown-param / invalid-value error paths, which name only the param)
        // are byte-unchanged. Off the hot data path: this is the admin CONFIG SET command.
        let value = Zeroizing::new(String::from_utf8_lossy(&pair[1]).into_owned());
        match apply_set(&name, &value, &ctx.runtime) {
            SetOutcome::Applied => {
                // PROD-7: mirror a SLOWLOG knob into the LIVE `ctx.slowlog` so the per-command
                // hook + the SLOWLOG command see the new threshold / cap immediately (the runtime
                // overlay is the canonical CONFIG source `CONFIG GET` reads; the SlowLog holds the
                // hot-path copy the hook reads with a single relaxed load). The `apply_set` above
                // already validated + stored into the runtime overlay, so we re-read the parsed
                // value from it rather than re-parsing.
                mirror_slowlog_param(ctx, &name);
            }
            SetOutcome::UnknownParam => {
                return Value::error(ErrorReply::config_set_unknown_param(&name));
            }
            SetOutcome::RestartRequired => {
                return Value::error(ErrorReply::config_set_immutable(&name));
            }
            // A rejected value (e.g. a malformed `save` directive) AND a recognized-but-unsupported
            // param (e.g. `appendonly` -- the #58 durability footgun fix: REFUSE rather than silently
            // accept a durability claim this build cannot honor) both surface the canonical
            // Redis-shaped CONFIG SET failed error carrying the specific reason.
            SetOutcome::InvalidValue(reason) | SetOutcome::Unsupported(reason) => {
                return Value::error(ErrorReply::config_set_failed(&name, &reason));
            }
        }
    }
    Value::ok()
}

/// Mirror a just-applied SLOWLOG `CONFIG SET` (`slowlog-log-slower-than` / `slowlog-max-len`) from
/// the runtime overlay into the live [`ironcache_observe::SlowLog`] (PROD-7), so the per-command
/// timing hook + the SLOWLOG command observe the change without a restart. A no-op for any other
/// param. Lowering `slowlog-max-len` trims the ring immediately (via `set_max_len`).
fn mirror_slowlog_param(ctx: &ServerContext, name: &str) {
    match name.to_ascii_lowercase().as_str() {
        "slowlog-log-slower-than" => ctx
            .slowlog
            .set_log_slower_than_micros(ctx.runtime.slowlog_log_slower_than()),
        "slowlog-max-len" => ctx.slowlog.set_max_len(ctx.runtime.slowlog_max_len()),
        _ => {}
    }
}

/// `CONFIG RESETSTAT` -> `+OK`. Signals the reset through `deltas.reset_stats` (CONFIG.md / Redis
/// `resetServerStats`). The serve loop honors it NODE-WIDE (#531): it zeroes the serving shard's
/// counter cell (via `ShardCounters::apply`) AND fans the reset across every shard's cell (via the
/// always-present metrics registry), so INFO's node-wide `# Stats` rollup actually zeroes rather
/// than only the ~1/N the serving shard held.
fn config_resetstat(deltas: &mut CounterDeltas, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("config|resetstat"));
    }
    deltas.reset_stats = true;
    Value::ok()
}

/// `CONFIG REWRITE` -> `-ERR The server is running without a config file` (PR-4b). Redis
/// rewrites the on-disk config file; IronCache currently always boots WITHOUT a
/// config-file path threaded through, so the faithful Redis behavior is the
/// no-config-file error (src/config.c `configRewriteCommand`), NOT a +OK stub. A runtime
/// `CONFIG SET` already takes effect immediately (it lives in the highest-precedence
/// overlay), so nothing is lost by REWRITE not persisting yet. When a config-file path
/// is threaded later (CONFIG.md, the `ironcache config` subcommand), REWRITE can
/// actually rewrite the file.
fn config_rewrite(req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("config|rewrite"));
    }
    Value::error(ErrorReply::config_rewrite_no_file())
}

/// `CONFIG HELP` -> the subcommand summary array (like Redis `addReplyHelp`).
fn config_help() -> Value {
    let lines: &[&str] = &[
        "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GET <pattern>",
        "    Return parameters matching the glob-like <pattern> and their values.",
        "SET <directive> <value>",
        "    Set the configuration <directive> to <value>.",
        "RESETSTAT",
        "    Reset statistics reported by the INFO command.",
        "REWRITE",
        "    Rewrite the configuration file.",
        "HELP",
        "    Print this help.",
    ];
    Value::Array(Some(lines.iter().map(|l| Value::bulk_str(l)).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ironcache_config::{Config, RuntimeConfig};
    use ironcache_env::Monotonic;
    use ironcache_observe::ServerInfo;

    fn ctx_with(boot: Config) -> ServerContext {
        let runtime = RuntimeConfig::from_config(&boot);
        ServerContext {
            runtime,
            acl: crate::acl::AclState::from_requirepass(boot.requirepass.as_deref()),
            databases: boot.databases,
            shards: boot.shards,
            info: ServerInfo {
                tcp_port: boot.port,
                shards: boot.shards,
                pid: 1,
                started_at: Monotonic::ZERO,
                maxmemory: boot.maxmemory,
                maxmemory_policy: "allkeys-lru",
                mem_allocator: "jemalloc",
                cluster_node_id: "0000000000000000000000000000000000000000",
                run_id: "0000000000000000000000000000000000000000",
                cluster_enabled: false,
            },
            cluster: None,
            raft: None,
            repl_status: None,
            in_sync_replicas: None,
            repl_history_id: None,
            metrics_registry: None,
            persist_stats: None,
            process_memory: std::sync::Arc::new(ironcache_observe::ProcessMemoryGauge::new()),
            conn_gate: std::sync::Arc::new(ironcache_observe::ConnectionGate::new()),
            slowlog: std::sync::Arc::new(ironcache_observe::SlowLog::new()),
            latency: std::sync::Arc::new(ironcache_observe::LatencyMonitor::new()),
            clients: std::sync::Arc::new(ironcache_observe::ClientRegistry::new()),
            hotkeys: std::sync::Arc::new(ironcache_observe::Hotkeys::new()),
            boot,
        }
    }

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    fn run(ctx: &ServerContext, parts: &[&[u8]]) -> (Value, CounterDeltas) {
        let mut deltas = CounterDeltas::default();
        let v = cmd_config(ctx, &mut deltas, &req(parts));
        (v, deltas)
    }

    /// Pull the name->value pairs out of a CONFIG GET Map reply.
    fn get_pairs(v: &Value) -> Vec<(String, String)> {
        match v {
            Value::Map(pairs) => pairs
                .iter()
                .map(|(k, val)| (bulk_string(k), bulk_string(val)))
                .collect(),
            other => panic!("expected Map, got {other:?}"),
        }
    }

    fn bulk_string(v: &Value) -> String {
        match v {
            Value::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    #[test]
    fn config_get_exact_and_glob() {
        let c = ctx_with(Config {
            maxmemory: 1024,
            maxmemory_policy: "allkeys-lru".to_owned(),
            ..Config::default()
        });
        // Exact name.
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"maxmemory"]);
        let pairs = get_pairs(&v);
        assert_eq!(pairs, vec![("maxmemory".to_owned(), "1024".to_owned())]);
        // Glob matches the subset.
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"maxmemory*"]);
        let names: Vec<String> = get_pairs(&v).into_iter().map(|(n, _)| n).collect();
        assert!(names.contains(&"maxmemory".to_owned()));
        assert!(names.contains(&"maxmemory-policy".to_owned()));
        assert!(names.contains(&"maxmemory-samples".to_owned()));
        // An unknown param is OMITTED (not an error).
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"nonsense"]);
        assert!(get_pairs(&v).is_empty());
        // Case-insensitive pattern.
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"MAXMEMORY"]);
        assert_eq!(get_pairs(&v).len(), 1);
        // Multiple patterns (Redis 7).
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"maxmemory", b"requirepass"]);
        let names: Vec<String> = get_pairs(&v).into_iter().map(|(n, _)| n).collect();
        assert!(names.contains(&"maxmemory".to_owned()));
        assert!(names.contains(&"requirepass".to_owned()));
    }

    #[test]
    fn config_set_round_trips_and_reflects_in_get() {
        let c = ctx_with(Config::default());
        // maxmemory.
        assert_eq!(
            run(&c, &[b"CONFIG", b"SET", b"maxmemory", b"100mb"]).0,
            Value::ok()
        );
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"maxmemory"]);
        assert_eq!(
            get_pairs(&v),
            vec![("maxmemory".to_owned(), (100 * 1024 * 1024).to_string())]
        );
        // maxmemory-policy.
        assert_eq!(
            run(
                &c,
                &[b"CONFIG", b"SET", b"maxmemory-policy", b"allkeys-lfu"]
            )
            .0,
            Value::ok()
        );
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"maxmemory-policy"]);
        assert_eq!(
            get_pairs(&v),
            vec![("maxmemory-policy".to_owned(), "allkeys-lfu".to_owned())]
        );
        // requirepass (empty disables).
        assert_eq!(
            run(&c, &[b"CONFIG", b"SET", b"requirepass", b"pw"]).0,
            Value::ok()
        );
        assert!(c.runtime.requires_auth());
        assert_eq!(
            run(&c, &[b"CONFIG", b"SET", b"requirepass", b""]).0,
            Value::ok()
        );
        assert!(!c.runtime.requires_auth());
        // Multi-set in one command.
        assert_eq!(
            run(
                &c,
                &[
                    b"CONFIG",
                    b"SET",
                    b"maxmemory",
                    b"50mb",
                    b"maxmemory-policy",
                    b"volatile-ttl"
                ]
            )
            .0,
            Value::ok()
        );
        assert_eq!(c.runtime.maxmemory(), 50 * 1024 * 1024);
        assert_eq!(c.runtime.policy_name(), "volatile-ttl");
    }

    #[test]
    fn config_set_errors_are_canonical() {
        let c = ctx_with(Config::default());
        // Unknown param.
        match run(&c, &[b"CONFIG", b"SET", b"bogus", b"1"]).0 {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-ERR Unknown option or number of arguments for CONFIG SET - 'bogus'"
            ),
            other => panic!("expected unknown-param error, got {other:?}"),
        }
        // Restart-required (immutable) param.
        match run(&c, &[b"CONFIG", b"SET", b"databases", b"8"]).0 {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-ERR CONFIG SET failed (possibly related to argument 'databases') - can't set immutable config"
            ),
            other => panic!("expected immutable error, got {other:?}"),
        }
        // Invalid value (bad maxmemory size).
        match run(&c, &[b"CONFIG", b"SET", b"maxmemory", b"1.5gb"]).0 {
            Value::Error(e) => assert!(
                e.line().starts_with(
                    "-ERR CONFIG SET failed (possibly related to argument 'maxmemory') -"
                ),
                "got {}",
                e.line()
            ),
            other => panic!("expected failed error, got {other:?}"),
        }
        // Invalid policy name.
        match run(
            &c,
            &[b"CONFIG", b"SET", b"maxmemory-policy", b"allkeys-ttl"],
        )
        .0
        {
            Value::Error(e) => assert!(e.line().contains("CONFIG SET failed"), "got {}", e.line()),
            other => panic!("expected failed error, got {other:?}"),
        }
        // Odd arity -> `-ERR syntax error` (Redis shared.syntaxerr), NOT the
        // unknown-option message (that is reserved for an unrecognized param NAME).
        match run(&c, &[b"CONFIG", b"SET", b"maxmemory"]).0 {
            Value::Error(e) => assert_eq!(e.line(), "-ERR syntax error"),
            other => panic!("expected syntax error, got {other:?}"),
        }
        // A longer odd token count is also a syntax error.
        match run(
            &c,
            &[
                b"CONFIG",
                b"SET",
                b"maxmemory",
                b"100mb",
                b"maxmemory-policy",
            ],
        )
        .0
        {
            Value::Error(e) => assert_eq!(e.line(), "-ERR syntax error"),
            other => panic!("expected syntax error, got {other:?}"),
        }
    }

    #[test]
    fn config_set_zero_pairs_is_ok_noop() {
        // CONFIG SET with NO param/value tokens (argc=2, even-but-empty) is a no-op set
        // of nothing: Redis replies +OK.
        let c = ctx_with(Config::default());
        assert_eq!(run(&c, &[b"CONFIG", b"SET"]).0, Value::ok());
    }

    #[test]
    fn config_set_noop_params_are_echoed() {
        let c = ctx_with(Config::default());
        // `maxmemory-samples` is still an accepted no-op: ack +OK, echoed by GET.
        assert_eq!(
            run(&c, &[b"CONFIG", b"SET", b"maxmemory-samples", b"10"]).0,
            Value::ok()
        );
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"maxmemory-samples"]);
        assert_eq!(
            get_pairs(&v),
            vec![("maxmemory-samples".to_owned(), "5".to_owned())]
        );
    }

    /// #58 durability footgun fix: `CONFIG SET save` ACTUALLY updates the runtime save policy and
    /// `CONFIG GET save` reports the REAL policy (not a fixed empty stub). A booted-off node reports
    /// empty; a `SET save "900 1"` reports back `900 1`; `SET save ""` disables it again.
    #[test]
    fn config_set_save_updates_and_reports_the_real_policy() {
        let c = ctx_with(Config::default());
        // Default boot: the periodic save is OFF, so GET save reports the empty string.
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"save"]);
        assert_eq!(get_pairs(&v), vec![("save".to_owned(), String::new())]);
        // SET save "900 1" ACTUALLY updates the policy (no longer a silent no-op).
        assert_eq!(
            run(&c, &[b"CONFIG", b"SET", b"save", b"900 1"]).0,
            Value::ok()
        );
        assert_eq!(c.runtime.save_policy(), (900, 1));
        // GET save now reports the REAL configured policy.
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"save"]);
        assert_eq!(get_pairs(&v), vec![("save".to_owned(), "900 1".to_owned())]);
        // Multiple save points collapse to the SHORTEST-interval (tightest-RPO) one.
        assert_eq!(
            run(
                &c,
                &[b"CONFIG", b"SET", b"save", b"3600 1 300 100 60 10000"]
            )
            .0,
            Value::ok()
        );
        assert_eq!(c.runtime.save_policy(), (60, 10000));
        // SET save "" DISABLES the periodic save.
        assert_eq!(run(&c, &[b"CONFIG", b"SET", b"save", b""]).0, Value::ok());
        assert_eq!(c.runtime.save_policy(), (0, 0));
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"save"]);
        assert_eq!(get_pairs(&v), vec![("save".to_owned(), String::new())]);
        // A malformed directive is a CONFIG SET failed error (never a silent accept).
        match run(&c, &[b"CONFIG", b"SET", b"save", b"900"]).0 {
            Value::Error(e) => assert!(e.line().contains("CONFIG SET failed"), "got {}", e.line()),
            other => panic!("expected CONFIG SET failed, got {other:?}"),
        }
    }

    /// #58 durability footgun fix: `CONFIG SET appendonly yes` is REFUSED with an explicit error
    /// (IronCache has no AOF) instead of being silently accepted, and `CONFIG GET appendonly` is
    /// `no`. This closes the false-durability trap where an operator believed AOF was on.
    #[test]
    fn config_set_appendonly_is_refused_and_get_is_no() {
        let c = ctx_with(Config::default());
        match run(&c, &[b"CONFIG", b"SET", b"appendonly", b"yes"]).0 {
            Value::Error(e) => {
                assert!(e.line().contains("appendonly"), "got {}", e.line());
                assert!(
                    e.line().contains("not supported"),
                    "the refusal must say it is unsupported, got {}",
                    e.line()
                );
            }
            other => panic!("expected an unsupported-appendonly error, got {other:?}"),
        }
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"appendonly"]);
        assert_eq!(
            get_pairs(&v),
            vec![("appendonly".to_owned(), "no".to_owned())]
        );
    }

    /// CONFIG durability LOW fix: `CONFIG SET appendonly no` MUST reply +OK (it is a no-op-OK:
    /// the feature is already off, and a client / ops tool that defensively sets `appendonly no`
    /// at startup expects success, matching Redis). Only turning it ON (`yes`) is refused; a
    /// non-boolean value is a CONFIG SET failed error. `CONFIG GET appendonly` stays `no`
    /// throughout.
    #[test]
    fn config_set_appendonly_no_is_ok_noop() {
        let c = ctx_with(Config::default());
        // `appendonly no` -> +OK (the no-op-OK), case-insensitively, plus the `0`/`false` spellings.
        for off in [b"no".as_slice(), b"NO", b"No", b"0", b"false"] {
            assert_eq!(
                run(&c, &[b"CONFIG", b"SET", b"appendonly", off]).0,
                Value::ok(),
                "appendonly {} must be +OK",
                String::from_utf8_lossy(off)
            );
        }
        // GET still reports `no` (nothing actually changed).
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"appendonly"]);
        assert_eq!(
            get_pairs(&v),
            vec![("appendonly".to_owned(), "no".to_owned())]
        );
        // A non-boolean value is a CONFIG SET failed error (never a silent accept).
        match run(&c, &[b"CONFIG", b"SET", b"appendonly", b"maybe"]).0 {
            Value::Error(e) => assert!(e.line().contains("CONFIG SET failed"), "got {}", e.line()),
            other => panic!("expected CONFIG SET failed, got {other:?}"),
        }
    }

    /// CONFIG durability LOW fix: `CONFIG GET save` reports the EMPTY string when the periodic
    /// save policy is OFF (the default boot posture), matching Redis's "" for no save points --
    /// NOT a bogus value. (The active-policy reporting is covered by
    /// `config_set_save_updates_and_reports_the_real_policy`; this pins the OFF-reports-empty
    /// contract explicitly.)
    #[test]
    fn config_get_save_is_empty_when_off() {
        let c = ctx_with(Config::default());
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"save"]);
        assert_eq!(get_pairs(&v), vec![("save".to_owned(), String::new())]);
    }

    #[test]
    fn config_set_requirepass_empty_clears_auth() {
        // CONFIG SET requirepass <pw> enables auth; CONFIG SET requirepass "" disables
        // it (Redis parity). Asserted against the runtime cell the auth path reads.
        let c = ctx_with(Config::default());
        assert!(!c.runtime.requires_auth());
        assert_eq!(
            run(&c, &[b"CONFIG", b"SET", b"requirepass", b"pw"]).0,
            Value::ok()
        );
        assert!(c.runtime.requires_auth());
        // SECURITY (#65): the overlay holds the SHA-256 hex of the plaintext, not "pw".
        assert_eq!(
            c.runtime.requirepass().as_deref(),
            Some(ironcache_config::sha256_hex(b"pw").as_str())
        );
        assert_ne!(c.runtime.requirepass().as_deref(), Some("pw"));
        assert_eq!(
            run(&c, &[b"CONFIG", b"SET", b"requirepass", b""]).0,
            Value::ok()
        );
        assert!(!c.runtime.requires_auth());
    }

    #[test]
    fn config_get_requirepass_returns_hash_and_empty_when_unset() {
        // SECURITY DIVERGENCE (#65): CONFIG GET requirepass returns the SHA-256 hex
        // digest (NOT the plaintext Redis echoes), and the empty string when unset (Redis
        // parity for unset, not nil). Only an authenticated client reaches CONFIG GET.
        let c = ctx_with(Config::default());
        // Unset -> empty string.
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"requirepass"]);
        assert_eq!(
            get_pairs(&v),
            vec![("requirepass".to_owned(), String::new())]
        );
        // After SET, GET returns the hex digest of the plaintext, never the plaintext.
        assert_eq!(
            run(&c, &[b"CONFIG", b"SET", b"requirepass", b"s3cr3t"]).0,
            Value::ok()
        );
        let (v, _) = run(&c, &[b"CONFIG", b"GET", b"requirepass"]);
        let pairs = get_pairs(&v);
        assert_eq!(
            pairs,
            vec![(
                "requirepass".to_owned(),
                ironcache_config::sha256_hex(b"s3cr3t")
            )]
        );
        assert_ne!(pairs[0].1, "s3cr3t");
        assert_eq!(pairs[0].1.len(), 64);
    }

    #[test]
    fn config_resetstat_signals_reset() {
        let c = ctx_with(Config::default());
        let (v, deltas) = run(&c, &[b"CONFIG", b"RESETSTAT"]);
        assert_eq!(v, Value::ok());
        assert!(deltas.reset_stats);
    }

    #[test]
    fn config_rewrite_and_help_and_unknown_sub() {
        let c = ctx_with(Config::default());
        // REWRITE without a config file is the Redis no-config-file error (the server
        // currently always boots without a config-file path threaded through).
        match run(&c, &[b"CONFIG", b"REWRITE"]).0 {
            Value::Error(e) => {
                assert_eq!(e.line(), "-ERR The server is running without a config file");
            }
            other => panic!("expected no-config-file error, got {other:?}"),
        }
        assert!(matches!(
            run(&c, &[b"CONFIG", b"HELP"]).0,
            Value::Array(Some(_))
        ));
        match run(&c, &[b"CONFIG", b"BOGUS"]).0 {
            Value::Error(e) => assert!(e.line().contains("unknown subcommand")),
            other => panic!("expected unknown subcommand, got {other:?}"),
        }
    }
}
