// SPDX-License-Identifier: MIT OR Apache-2.0
//! Introspection command handlers split out of `dispatch.rs` (#625): SLOWLOG, HOTKEYS, MEMORY,
//! LATENCY, and INFO (plus the replication-info + endpoint helpers INFO renders). Each is a
//! self-contained `cmd_*` returning a RESP `Value`, driven by the dispatch engine. Behavior-
//! preserving relocation: the bodies are byte-identical to their former in-`dispatch.rs` definitions.

use super::{ServerContext, ascii_upper};
use crate::{CmdStatsFn, KeyspaceFn, RollupFn};
use ironcache_env::Clock;
use ironcache_observe::{
    EffectiveMemoryConfig, KeyspaceDbLine, MemoryInfo, PersistenceInfo, ReplicaLine,
    ReplicationInfo, build_info,
};
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{Keyspace, Store, UnixMillis};

/// `SLOWLOG GET [count] | LEN | RESET | HELP` (PROD-7). Reads / resets the node-level ring
/// (`ctx.slowlog`); the per-command timing HOOK that POPULATES the ring lives in the serve layer
/// (it needs the client addr/name + the Env clock). The `slowlog-log-slower-than` / `slowlog-max-len`
/// knobs are CONFIG params, not SLOWLOG subcommands.
pub(crate) fn cmd_slowlog(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("slowlog"));
    }
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        b"GET" => {
            // SLOWLOG GET [count]: default 10 (Redis); `-1` means ALL. A non-integer count is the
            // not-an-integer error.
            let count: Option<usize> = if req.args.len() >= 3 {
                match core::str::from_utf8(&req.args[2])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(n) if n < 0 => None, // -1 (or any negative) -> all entries
                    Some(n) => Some(usize::try_from(n).unwrap_or(usize::MAX)),
                    None => return Value::error(ErrorReply::not_an_integer()),
                }
            } else {
                Some(10)
            };
            let entries = ctx.slowlog.get(count);
            let arr: Vec<Value> = entries.iter().map(slowlog_entry_value).collect();
            Value::Array(Some(arr))
        }
        b"LEN" => Value::Integer(ctx.slowlog.len() as i64),
        b"RESET" => {
            ctx.slowlog.reset();
            Value::ok()
        }
        b"HELP" => slowlog_help(),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "SLOWLOG",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// One SLOWLOG GET entry as the Redis 6-element array: `[id, unix-ts, micros, [args...],
/// client-addr, client-name]`.
fn slowlog_entry_value(e: &ironcache_observe::SlowLogEntry) -> Value {
    let args: Vec<Value> = e
        .args
        .iter()
        .map(|a| Value::bulk(bytes::Bytes::copy_from_slice(a)))
        .collect();
    Value::Array(Some(vec![
        Value::Integer(e.id as i64),
        Value::Integer(e.unix_time_secs as i64),
        Value::Integer(e.micros as i64),
        Value::Array(Some(args)),
        Value::bulk_str(&e.client_addr),
        Value::bulk_str(&e.client_name),
    ]))
}

/// `SLOWLOG HELP` -> the subcommand summary array (Redis shape).
fn slowlog_help() -> Value {
    let lines: &[&str] = &[
        "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GET [<count>]",
        "    Return top <count> entries from the slowlog (default: 10, -1 means all).",
        "LEN",
        "    Return the length of the slowlog.",
        "RESET",
        "    Reset the slowlog.",
        "HELP",
        "    Print this help.",
    ];
    Value::Array(Some(lines.iter().map(|l| Value::bulk_str(l)).collect()))
}

// -- HOTKEYS (#428): the faithful Redis 8.6 hot-key tracking container -------------------------

/// `HOTKEYS START METRICS count [CPU] [NET] [COUNT k] [DURATION s] [SAMPLE ratio] [SLOTS count
/// slot...] | STOP | GET | RESET | HELP` (#428): drive the node-level [`ironcache_observe::Hotkeys`]
/// tracker in `ctx.hotkeys`. `now` carries the Env-clock unix-ms used for the session timestamps; the
/// per-command RECORDING hook that POPULATES the sketches lives in the serve layer (it needs each
/// command's elapsed micros + keys).
pub(crate) fn cmd_hotkeys(ctx: &ServerContext, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("hotkeys"));
    }
    match ascii_upper(&req.args[1]).as_slice() {
        b"START" => match parse_hotkeys_start(req) {
            Ok(cfg) => match ctx.hotkeys.start(cfg, now.0) {
                Ok(()) => Value::ok(),
                Err(e) => Value::error(ErrorReply::err(e)),
            },
            Err(e) => e,
        },
        b"STOP" => {
            if req.args.len() != 2 {
                return Value::error(ErrorReply::syntax_error());
            }
            match ctx.hotkeys.stop(now.0) {
                Ok(()) => Value::ok(),
                Err(e) => Value::error(ErrorReply::err(e)),
            }
        }
        b"RESET" => {
            if req.args.len() != 2 {
                return Value::error(ErrorReply::syntax_error());
            }
            match ctx.hotkeys.reset() {
                Ok(()) => Value::ok(),
                Err(e) => Value::error(ErrorReply::err(e)),
            }
        }
        b"GET" => {
            if req.args.len() != 2 {
                return Value::error(ErrorReply::syntax_error());
            }
            // Null when no session exists (never started / after RESET), matching Redis.
            ctx.hotkeys
                .snapshot(now.0)
                .map_or(Value::Null, |snap| hotkeys_get_value(&snap))
        }
        b"HELP" => hotkeys_help(),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "HOTKEYS",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// Parse `HOTKEYS START` options into a [`ironcache_observe::HotkeysConfig`], or return the error
/// `Value` to reply. `METRICS count [CPU] [NET]` is required (at least one metric); `COUNT`,
/// `DURATION` (seconds), `SAMPLE` (ratio), and `SLOTS` (parsed + validated; a single node owns all
/// slots so the selection is informational) are optional.
fn parse_hotkeys_start(req: &Request) -> Result<ironcache_observe::HotkeysConfig, Value> {
    // args: [0]=HOTKEYS [1]=START [2]=METRICS [3]=count [4..4+count]=CPU/NET tokens, then options.
    if req.args.len() < 4 || !ascii_upper(&req.args[2]).eq_ignore_ascii_case(b"METRICS") {
        return Err(Value::error(ErrorReply::err(
            "HOTKEYS START requires METRICS <count> [CPU] [NET]",
        )));
    }
    let metric_count = parse_u64_arg(&req.args[3]).ok_or_else(syntax_err)? as usize;
    if metric_count == 0 || metric_count > 2 || 4 + metric_count > req.args.len() {
        return Err(Value::error(ErrorReply::syntax_error()));
    }
    let (mut cpu, mut net) = (false, false);
    for tok in &req.args[4..4 + metric_count] {
        match ascii_upper(tok).as_slice() {
            b"CPU" => cpu = true,
            b"NET" => net = true,
            _ => return Err(Value::error(ErrorReply::syntax_error())),
        }
    }
    if !cpu && !net {
        return Err(Value::error(ErrorReply::err(
            "HOTKEYS START requires at least one of CPU or NET",
        )));
    }
    let mut count = ironcache_observe::DEFAULT_HOTKEYS_COUNT;
    let mut sample_ratio: u64 = 1;
    let mut duration_ms: u64 = 0;
    let mut i = 4 + metric_count;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"COUNT" => {
                let k = parse_u64_arg(req.args.get(i + 1).ok_or_else(syntax_err)?)
                    .filter(|&k| k >= 1)
                    .ok_or_else(syntax_err)?;
                count = usize::try_from(k).unwrap_or(usize::MAX);
                i += 2;
            }
            b"DURATION" => {
                let secs = parse_u64_arg(req.args.get(i + 1).ok_or_else(syntax_err)?)
                    .ok_or_else(syntax_err)?;
                duration_ms = secs.saturating_mul(1000);
                i += 2;
            }
            b"SAMPLE" => {
                sample_ratio = parse_u64_arg(req.args.get(i + 1).ok_or_else(syntax_err)?)
                    .filter(|&r| r >= 1)
                    .ok_or_else(syntax_err)?;
                i += 2;
            }
            b"SLOTS" => {
                // `SLOTS count slot [slot ...]`: validate the shape (a single node owns all slots, so
                // the selection is accepted but informational; selected-slots reports the full range).
                let n = parse_u64_arg(req.args.get(i + 1).ok_or_else(syntax_err)?)
                    .ok_or_else(syntax_err)? as usize;
                let end = i + 2 + n;
                if n == 0 || end > req.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                for s in &req.args[i + 2..end] {
                    if parse_u64_arg(s).is_none_or(|s| s > 16383) {
                        return Err(Value::error(ErrorReply::err("Invalid slot")));
                    }
                }
                i = end;
            }
            _ => return Err(Value::error(ErrorReply::syntax_error())),
        }
    }
    Ok(ironcache_observe::HotkeysConfig {
        cpu,
        net,
        count,
        sample_ratio,
        duration_ms,
    })
}

/// Parse a non-negative decimal integer argument, or `None` if it is not a valid `u64`.
fn parse_u64_arg(arg: &[u8]) -> Option<u64> {
    core::str::from_utf8(arg).ok().and_then(|s| s.parse().ok())
}

/// A zero-arg closure that yields the standard syntax error `Value` (for `ok_or_else`).
fn syntax_err() -> Value {
    Value::error(ErrorReply::syntax_error())
}

/// Build the `HOTKEYS GET` reply from a snapshot (the Redis 8.6 field set). Rendered as a
/// [`Value::Map`] so RESP3 emits a `%` map and RESP2 degrades to the flat `[k, v, ...]` array.
fn hotkeys_get_value(snap: &ironcache_observe::HotkeysSnapshot) -> Value {
    let mut fields: Vec<(Value, Value)> = vec![
        (
            Value::bulk_str("tracking-active"),
            Value::Integer(i64::from(snap.active)),
        ),
        (
            Value::bulk_str("sample-ratio"),
            Value::Integer(i64::try_from(snap.sample_ratio).unwrap_or(i64::MAX)),
        ),
        // A single node owns the whole slot range; report it as one [start, end] pair.
        (
            Value::bulk_str("selected-slots"),
            Value::Array(Some(vec![Value::Array(Some(vec![
                Value::Integer(0),
                Value::Integer(16383),
            ]))])),
        ),
        (
            Value::bulk_str("all-commands-all-slots-us"),
            Value::Integer(i64::try_from(snap.all_us).unwrap_or(i64::MAX)),
        ),
        (
            Value::bulk_str("net-bytes-all-commands-all-slots"),
            Value::Integer(i64::try_from(snap.all_net_bytes).unwrap_or(i64::MAX)),
        ),
        (
            Value::bulk_str("collection-start-time-unix-ms"),
            Value::Integer(i64::try_from(snap.start_unix_ms).unwrap_or(i64::MAX)),
        ),
        (
            Value::bulk_str("collection-duration-ms"),
            Value::Integer(i64::try_from(snap.duration_ms).unwrap_or(i64::MAX)),
        ),
    ];
    if let Some(by_cpu) = &snap.cpu {
        // IronCache attributes monotonic command-execution time as the CPU metric (the same clock
        // SLOWLOG/COMMANDSTATS use); it does not split user/sys via getrusage, so user carries the
        // measured time and sys is 0.
        fields.push((
            Value::bulk_str("total-cpu-time-user-ms"),
            Value::Integer(i64::try_from(snap.all_us / 1000).unwrap_or(i64::MAX)),
        ));
        fields.push((Value::bulk_str("total-cpu-time-sys-ms"), Value::Integer(0)));
        fields.push((
            Value::bulk_str("by-cpu-time-us"),
            hotkeys_pairs_array(by_cpu),
        ));
    }
    if let Some(by_net) = &snap.net {
        fields.push((
            Value::bulk_str("total-net-bytes"),
            Value::Integer(i64::try_from(snap.all_net_bytes).unwrap_or(i64::MAX)),
        ));
        fields.push((Value::bulk_str("by-net-bytes"), hotkeys_pairs_array(by_net)));
    }
    Value::Map(fields)
}

/// Render a top-K list as the Redis flat `[key, value, key, value, ...]` array.
fn hotkeys_pairs_array(pairs: &[(bytes::Bytes, u64)]) -> Value {
    let mut out = Vec::with_capacity(pairs.len() * 2);
    for (key, val) in pairs {
        out.push(Value::bulk(key.clone()));
        out.push(Value::Integer(i64::try_from(*val).unwrap_or(i64::MAX)));
    }
    Value::Array(Some(out))
}

/// `HOTKEYS HELP` -> the subcommand summary array (Redis shape).
fn hotkeys_help() -> Value {
    let lines: &[&str] = &[
        "HOTKEYS <subcommand> [<arg> ...]. Subcommands are:",
        "START METRICS <count> [CPU] [NET] [COUNT <k>] [DURATION <s>] [SAMPLE <ratio>] [SLOTS ...]",
        "    Begin tracking the top hot keys by the chosen metric(s).",
        "STOP",
        "    Stop tracking but keep the collected data.",
        "GET",
        "    Return the tracking results and metadata (null if no session).",
        "RESET",
        "    Release the tracking resources (only when stopped).",
        "HELP",
        "    Print this help.",
    ];
    Value::Array(Some(lines.iter().map(|l| Value::bulk_str(l)).collect()))
}

// -- MEMORY (PROD-7) ---------------------------------------------------------------------------

/// `MEMORY USAGE key [SAMPLES n] | DOCTOR | STATS | HELP` (PROD-7). USAGE estimates one key's byte
/// footprint via the store; STATS reuses the observe gauges + the process-global allocator figure
/// `mem`; DOCTOR is a human string.
pub(crate) fn cmd_memory<S: Store>(
    ctx: &ServerContext,
    store: &mut S,
    db: u32,
    now: UnixMillis,
    mem: MemoryInfo,
    req: &Request,
) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("memory"));
    }
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        b"USAGE" => memory_usage(store, db, now, req),
        b"DOCTOR" => memory_doctor(mem),
        b"STATS" => memory_stats(ctx, mem),
        b"HELP" => memory_help(),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "MEMORY",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// `MEMORY USAGE key [SAMPLES n]` -> an integer estimate of the key's total byte footprint
/// (key bytes + value bytes + a per-key overhead constant), or nil if the key is absent. The
/// `SAMPLES n` option (used by Redis to bound nested-collection sampling) is parsed + accepted; the
/// estimate is a deterministic figure that does not depend on it for the v1 surface (documented).
fn memory_usage<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("memory|usage"));
    }
    // Parse an optional `SAMPLES <n>` (accepted for compatibility; see the fn docs).
    if req.args.len() > 3 {
        if req.args.len() != 5 || !ascii_upper(&req.args[3]).eq_ignore_ascii_case(b"SAMPLES") {
            return Value::error(ErrorReply::syntax_error());
        }
        if core::str::from_utf8(&req.args[4])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .is_none()
        {
            return Value::error(ErrorReply::not_an_integer());
        }
    }
    let key = &req.args[2];
    match store.read(db, key, now) {
        Some(v) => {
            // A deterministic estimate: the key bytes + the string value bytes + a fixed per-key
            // overhead (the robj/dictEntry/SDS-header analog Redis's `objectComputeSize` adds).
            // LIMITATION (documented follow-up): for COLLECTION types (list/hash/set/zset) the
            // value-bytes figure `v.len()` is currently 0 (the string-value view is empty for a
            // collection), so the estimate reports only the per-key overhead + key bytes and
            // UNDERCOUNTS collections. String values are counted exactly. Per-type element sizing
            // (cardinality x element bytes) is tracked for a later pass.
            let est = MEMORY_USAGE_PER_KEY_OVERHEAD + key.len() as u64 + v.len() as u64;
            Value::Integer(est as i64)
        }
        None => Value::Null,
    }
}

/// The fixed per-key overhead the MEMORY USAGE estimate adds (the robj + dictEntry + key-SDS
/// header analog). A conservative constant in the same ballpark Redis reports for a small key.
const MEMORY_USAGE_PER_KEY_OVERHEAD: u64 = 64;

/// `MEMORY DOCTOR` -> a human-readable health string. With no allocator figure (the system-allocator
/// build / before the first publish) it reports the no-data message; otherwise a terse "sane"
/// assessment with the live used/RSS figures (a real fragmentation-ratio judgment is a follow-up).
fn memory_doctor(mem: MemoryInfo) -> Value {
    if mem.used_memory == 0 {
        return Value::bulk_str(
            "Sam, I detected a few issues in this Redis instance memory implants:\n\n \
             * No memory figure is available yet (no allocator stats published). Run me again \
             after the instance has served some traffic.\n",
        );
    }
    let frag = if mem.used_memory > 0 {
        mem.used_memory_rss as f64 / mem.used_memory as f64
    } else {
        0.0
    };
    let msg = format!(
        "Sam, I have observed the memory profile of this instance: used_memory={} bytes, \
         used_memory_rss={} bytes, fragmentation_ratio={:.2}. Nothing alarming; memory usage \
         looks healthy.",
        mem.used_memory, mem.used_memory_rss, frag
    );
    Value::bulk(msg.into_bytes())
}

/// `MEMORY STATS` -> a flat field/value array (Redis MEMORY STATS shape, a subset) reusing the
/// observe figures: the process-global allocator `used_memory` / RSS, the effective `maxmemory`
/// ceiling, the policy, the live connection count, and the fragmentation ratio. RESP2 renders the
/// `Map` as a flat array; RESP3 as a map (the canonical MEMORY STATS shapes).
fn memory_stats(ctx: &ServerContext, mem: MemoryInfo) -> Value {
    let frag = if mem.used_memory > 0 {
        mem.used_memory_rss as f64 / mem.used_memory as f64
    } else {
        0.0
    };
    let policy = ctx.runtime.policy_name();
    let pairs: Vec<(Value, Value)> = vec![
        (
            Value::bulk_str("peak.allocated"),
            Value::Integer(mem.used_memory_rss as i64),
        ),
        (
            Value::bulk_str("total.allocated"),
            Value::Integer(mem.used_memory as i64),
        ),
        (Value::bulk_str("startup.allocated"), Value::Integer(0)),
        (
            Value::bulk_str("clients.normal"),
            Value::Integer(ctx.clients.len() as i64),
        ),
        (
            Value::bulk_str("maxmemory"),
            Value::Integer(ctx.runtime.maxmemory() as i64),
        ),
        (
            Value::bulk_str("maxmemory.policy"),
            Value::bulk_str(&policy),
        ),
        (
            Value::bulk_str("allocator.allocated"),
            Value::Integer(mem.used_memory as i64),
        ),
        (
            Value::bulk_str("allocator.resident"),
            Value::Integer(mem.used_memory_rss as i64),
        ),
        (
            Value::bulk_str("number.of.cached.scripts"),
            Value::Integer(0),
        ),
        (Value::bulk_str("fragmentation"), Value::Double(frag)),
    ];
    Value::Map(pairs)
}

/// `MEMORY HELP` -> the subcommand summary array (Redis shape).
fn memory_help() -> Value {
    let lines: &[&str] = &[
        "MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "DOCTOR",
        "    Return memory problems reports.",
        "STATS",
        "    Return information about the memory usage of the server.",
        "USAGE <key> [SAMPLES <count>]",
        "    Return memory in bytes used by <key> and its value.",
        "HELP",
        "    Print this help.",
    ];
    Value::Array(Some(lines.iter().map(|l| Value::bulk_str(l)).collect()))
}

// -- LATENCY (PROD-7) --------------------------------------------------------------------------

/// `LATENCY RESET [event...] | HISTORY event | LATEST | DOCTOR | HELP` (PROD-7). Reads / resets the
/// node-level monitor (`ctx.latency`); the per-command SAMPLE that feeds the `command` event lives
/// in the serve layer.
pub(crate) fn cmd_latency(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("latency"));
    }
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        b"RESET" => {
            let events: Vec<String> = req.args[2..]
                .iter()
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .collect();
            Value::Integer(ctx.latency.reset(&events) as i64)
        }
        b"HISTORY" => {
            if req.args.len() != 3 {
                return Value::error(ErrorReply::wrong_arity("latency|history"));
            }
            let event = String::from_utf8_lossy(&req.args[2]).into_owned();
            let samples = ctx.latency.history(&event);
            // Each sample is a 2-element [unix-secs, ms] array (Redis LATENCY HISTORY shape).
            let arr: Vec<Value> = samples
                .iter()
                .map(|(ts, ms)| {
                    Value::Array(Some(vec![
                        Value::Integer(*ts as i64),
                        Value::Integer(*ms as i64),
                    ]))
                })
                .collect();
            Value::Array(Some(arr))
        }
        b"LATEST" => {
            let latest = ctx.latency.latest();
            // Each event is a 4-element [name, unix-secs, latest-ms, max-ms] array (Redis shape).
            let arr: Vec<Value> = latest
                .iter()
                .map(|(name, ts, latest_ms, max_ms)| {
                    Value::Array(Some(vec![
                        Value::bulk_str(name),
                        Value::Integer(*ts as i64),
                        Value::Integer(*latest_ms as i64),
                        Value::Integer(*max_ms as i64),
                    ]))
                })
                .collect();
            Value::Array(Some(arr))
        }
        b"DOCTOR" => {
            let n = ctx.latency.event_count();
            let msg = if n == 0 {
                "Dave, I have observed the system, no worrysome latency spikes. Everything seems \
                 fine."
                    .to_owned()
            } else {
                format!(
                    "Dave, I have observed the system, {n} latency event(s) tracked. Use LATENCY \
                     LATEST and LATENCY HISTORY <event> to inspect the worst spikes."
                )
            };
            Value::bulk(msg.into_bytes())
        }
        b"GRAPH" => {
            // LATENCY GRAPH <event>: the ASCII spark-graph is a cosmetic follow-up; return an empty
            // bulk rather than an error so a client probing it does not fail (documented partial).
            Value::bulk_str("")
        }
        b"HELP" => latency_help(),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "LATENCY",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// `LATENCY HELP` -> the subcommand summary array (Redis shape).
fn latency_help() -> Value {
    let lines: &[&str] = &[
        "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "HISTORY <event>",
        "    Return time-latency samples for the <event> class.",
        "LATEST",
        "    Return the latest latency samples for all events.",
        "RESET [<event> ...]",
        "    Reset latency data of one or more <event> classes (default: reset all).",
        "DOCTOR",
        "    Return a human readable latency analysis report.",
        "HELP",
        "    Print this help.",
    ];
    Value::Array(Some(lines.iter().map(|l| Value::bulk_str(l)).collect()))
}

/// `INFO [section]` -> delegates to ironcache-observe. `mem` is the process-global
/// allocator snapshot (ADR-0006) the caller read once at the binary edge (the
/// server crate has no access to the concrete store's mallctl readers, by the
/// layering contract; the binary supplies the figure).
///
/// Each argument is an orthogonal INFO input the serve layer threads in (ctx / clock / store /
/// the counter rollup / the commandstats closure / the #531 node-wide keyspace rollup / the memory
/// snapshot / the request); bundling them into a struct would only obscure the per-section borrows,
/// so the over-7-args lint is allowed here with that justification.
#[allow(clippy::too_many_arguments)]
pub(crate) fn cmd_info<C: Clock, S: Keyspace>(
    ctx: &ServerContext,
    clock: &C,
    store: &S,
    rollup: RollupFn<'_>,
    cmdstats: CmdStatsFn<'_>,
    keyspace_rollup: KeyspaceFn<'_>,
    mem: MemoryInfo,
    req: &Request,
) -> Value {
    let section = if req.args.len() >= 2 {
        Some(String::from_utf8_lossy(&req.args[1]).into_owned())
    } else {
        None
    };
    // PR-4b: report the CURRENT effective maxmemory + policy (read from the runtime
    // overlay), so a `CONFIG SET maxmemory`/`maxmemory-policy` is reflected in INFO.
    // The policy name is cloned once here (off the per-command hot path: INFO is rare).
    let policy = ctx.runtime.policy_name();
    let effective = EffectiveMemoryConfig {
        maxmemory: ctx.runtime.maxmemory(),
        maxmemory_policy: &policy,
    };
    // The `# Replication` section facts (HA-7e): translate the node-level repl status snapshot to
    // the observe POD. `None` (the default static path, no status cell) -> the byte-compatible
    // standalone master-at-offset-0 posture.
    let replication = replication_info(ctx);
    // The `# Persistence` section facts (durability footgun fix #5): the last-save time + dirty
    // counter from the shared persistence-stats cell (`None` -> the honest persistence-disabled
    // section), and the LIVE save policy from the runtime overlay (so a `CONFIG SET save` is
    // reflected). `rdb_last_save_time` is seeded on boot from the loaded manifest (fix #2).
    let persistence = match ctx.persist_stats.as_ref() {
        Some(stats) => {
            let (interval_secs, min_changes) = ctx.runtime.save_policy();
            PersistenceInfo {
                enabled: true,
                rdb_last_save_time: stats.last_save_unix_secs(),
                rdb_changes_since_last_save: stats.dirty(),
                // #549: the last-save OUTCOME the persistence subsystem recorded (ok before any save).
                last_bgsave_ok: stats.last_bgsave_ok(),
                save_interval_secs: interval_secs,
                save_min_changes: min_changes,
            }
        }
        None => PersistenceInfo::disabled(),
    };
    // The `# Keyspace` section facts (operability fix #5, now NODE-WIDE #531): one line per
    // non-empty database with its live DBSIZE. The serve loop supplies the cross-shard sum via
    // `keyspace_rollup` (the SAME whole-keyspace scatter-gather DBSIZE uses), so on a multi-shard
    // node these `dbN:keys=...` counts equal DBSIZE and no longer vary by which shard homed the
    // connection. `None` (a single-shard node -- the serving shard IS the whole keyspace -- or an
    // EXEC-replay / unit-test path that cannot fan out) falls back to THIS shard's local `db_len`,
    // byte-identical to the pre-#531 behavior. `expires` is 0 (per-db expiry counting is an O(n)
    // scan, a follow-up); `keys` is the load-bearing field operators monitor.
    let keyspace: Vec<KeyspaceDbLine> = keyspace_rollup().unwrap_or_else(|| {
        (0..ctx.databases)
            .filter_map(|db| {
                let keys = store.db_len(db) as u64;
                (keys > 0).then_some(KeyspaceDbLine {
                    db,
                    keys,
                    expires: 0,
                })
            })
            .collect()
    });
    // The node-wide counter rollup (summed across every shard's cell via the always-present
    // `MetricsRegistry`, #531). Read ONCE here: it feeds both the `# Stats`/`# Clients` fields and the
    // ops/sec sampler below (the sampler must see the SAME total the section reports).
    let rolled = rollup();
    // The PROD-7 completeness facts for the `# Clients` / `# Stats` / `# CPU` sections: the effective
    // `maxclients` (read from the runtime overlay so a `CONFIG SET maxclients` is reflected) and the
    // rejected-connection count off the connection gate. `blocked_clients` is the node-wide count of
    // clients currently parked on a blocking command (#661), summed from the per-shard
    // `blocked_clients` gauge via the SAME `rolled` aggregate the other node-wide figures use.
    // `instantaneous_ops_per_sec` is a REAL recent rate now (#549): sample the node-wide command
    // total against the Env WALL clock (`now_unix_millis`, ADR-0003 -- comparable across the shards
    // that may each serve an INFO read into the shared ring) and read the rate over the sampling
    // window. This is the COLD INFO read path, so the clock read + the sampler's node-level lock are
    // off the per-command hot path. Falls back to 0 when there is no registry (a bare unit-test ctx).
    let instantaneous_ops_per_sec = ctx.metrics_registry.as_ref().map_or(0, |reg| {
        reg.ops_rate()
            .observe(clock.now_unix_millis(), rolled.commands_processed)
    });
    let runtime_stats = ironcache_observe::RuntimeStats {
        maxclients: ctx.runtime.maxclients(),
        blocked_clients: rolled.blocked_clients,
        instantaneous_ops_per_sec,
        rejected_connections: ctx.conn_gate.rejected(),
    };
    let mut body = build_info(
        clock,
        &ctx.info,
        rolled,
        mem,
        effective,
        &replication,
        &persistence,
        &keyspace,
        runtime_stats,
        section.as_deref(),
    );
    // COMMANDSTATS / ERRORSTATS (#413): appended for an EXPLICIT `commandstats` / `errorstats`
    // request OR `INFO all` / `INFO everything`, NOT the default `INFO` (Redis excludes them from
    // default to keep the reply small). Rendered by the serve layer from the SERVING shard's
    // `CommandStats` via the `cmdstats` closure, invoked ONLY here (zero cost on the common path).
    if let Some(sec) = section.as_deref() {
        let sl = sec.to_ascii_lowercase();
        let all = sl == "all" || sl == "everything";
        if all || sl == "commandstats" || sl == "errorstats" {
            let (commandstats, errorstats) = cmdstats();
            if (all || sl == "commandstats") && !commandstats.is_empty() {
                body.push_str("# Commandstats\r\n");
                body.push_str(&commandstats);
                body.push_str("\r\n");
            }
            if (all || sl == "errorstats") && !errorstats.is_empty() {
                body.push_str("# Errorstats\r\n");
                body.push_str(&errorstats);
                body.push_str("\r\n");
            }
        }
    }
    Value::bulk(body.into_bytes())
}

/// Build the INFO `# Replication` facts (HA-7e) from `ctx`'s node-level replication status. When
/// no status cell is present (the DEFAULT static path / standalone), returns
/// [`ReplicationInfo::standalone`] -- a master with no slaves at offset 0, byte-compatible with a
/// standalone Redis. In raft-mode it reads a [`ReplStatusSnapshot`] and maps it to Redis's field
/// shape: a master reports its head + a `slaveN:` line (with the per-replica lag) per connected
/// replica; a replica reports its master endpoint, link status, and applied offset.
/// Resolve a connected replica's advertised endpoint from the `NodeId` it captured at attach
/// (#365 stage 3): find the cluster slot-map member whose announce id DERIVES to that `NodeId`
/// (`node_id_from_announce`, the SAME mapping the leader-hint resolution and the slot-map use), and
/// return its advertised `(host, port)`. `None` when there is no cluster (standalone), the id is
/// unset (`0`, no replica advertised), or no member matches (a replica not yet in this node's map).
///
/// O(M) over the members on the rare INFO read (off the data path). With a single modeled replica
/// today this is one scan; the N-replica follow-up should derive each member's id once into a map.
fn resolve_replica_endpoint(ctx: &ServerContext, slave_id: u64) -> Option<(String, u16)> {
    if slave_id == 0 {
        return None;
    }
    let map = ctx.cluster.as_ref()?;
    map.nodes().into_iter().find_map(|n| {
        (ironcache_raft_net::node_id_from_announce(&n.id).0 == slave_id)
            .then(|| (n.host.to_string(), n.port))
    })
}

fn replication_info(ctx: &ServerContext) -> ReplicationInfo {
    let Some(status) = ctx.repl_status.as_ref() else {
        return ReplicationInfo::standalone();
    };
    let snap = status.snapshot();
    match snap.role {
        ironcache_repl::ReplRole::Master => {
            // One `slaveN:` line PER connected replica (#365 N-replica): the transport serves N
            // replicas, each its own entry. The lag is the master's view (`head - replica_acked`),
            // known while connected; the endpoint is resolved from the replica's advertised `NodeId`
            // via the cluster slot map (`("", 0)` when standalone / id unset / not yet a member; the
            // offset + lag, the load-bearing fields, are always real).
            let mut slaves = Vec::with_capacity(snap.replicas.len());
            for r in &snap.replicas {
                let lag = snap.slave_lag_of(r.acked).lag().unwrap_or(0);
                let (ip, port) =
                    resolve_replica_endpoint(ctx, r.node_id).unwrap_or((String::new(), 0));
                slaves.push(ReplicaLine {
                    ip,
                    port,
                    offset: r.acked.0,
                    lag,
                });
            }
            ReplicationInfo {
                is_master: true,
                master_repl_offset: snap.node_offset.0,
                slaves,
                master_endpoint: None,
                master_link_up: false,
                slave_repl_offset: 0,
            }
        }
        ironcache_repl::ReplRole::Replica => ReplicationInfo {
            is_master: false,
            // master_repl_offset on a replica = the master's head as last observed on the link.
            master_repl_offset: snap.master_offset.0,
            slaves: Vec::new(),
            master_endpoint: snap.master_endpoint.clone(),
            master_link_up: snap.master_link.is_up(),
            // slave_repl_offset = this replica's own applied offset.
            slave_repl_offset: snap.node_offset.0,
        },
    }
}
