// SPDX-License-Identifier: MIT OR Apache-2.0
//! Per-command observability + client-side hooks split out of `serve.rs` (#625): the INFO
//! COMMANDSTATS/ERRORSTATS + LATENCY recorder, the CLIENT TRACKING (#409) register/invalidate path
//! and its helpers, the HOTKEYS attribution, and the SLOWLOG record + arg redaction. These run
//! AFTER a command's reply is encoded (off the per-key hot path). Behavior-preserving relocation:
//! the bodies are byte-identical to their former in-`serve.rs` definitions.

use super::{ShardState, TRACKING, ascii_upper, shard_pubsub, shard_tracking};
use ironcache_env::{Clock, SystemEnv};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, Request};
use std::cell::RefCell;
use std::rc::Rc;

/// Record a command into the SLOWLOG ring + the LATENCY `command` event IF it met the threshold
/// (PROD-7). Called ONLY when the SLOWLOG was enabled at the start of the command (the hot-path hook
/// short-circuits otherwise), so the elapsed-time read + the threshold compare are the only cost on
/// a fast command, and the ring/monitor locks are touched ONLY for a genuinely slow command (rare).
///
/// `start` is the monotonic instant captured before dispatch; the elapsed micros are measured here
/// through the SAME Env clock seam (ADR-0003). The unix TIMESTAMP for the entry is read from the Env
/// wall clock. The args + this connection's addr/name are copied into the entry (capped by the ring
/// builder). The LATENCY `command` event samples the same elapsed time in milliseconds, gated on a
/// fixed floor so the monitor only records meaningful spikes.
/// Record one command into the serving shard's INFO COMMANDSTATS / ERRORSTATS tables (#413),
/// driven off the already-encoded reply so there is no second dispatch. `out_before` is the
/// offset where THIS command's reply began in `out`; an error reply starts with `-`, and its
/// CODE is the first token after the `-` (up to a space or CR). Only REGISTRY commands are
/// tracked (an unknown command has no canonical name and was rejected); the name key is the
/// registry `&'static`, so a record allocates nothing. `elapsed_us` is this command's measured
/// micros (shared with the slowlog timing read). Off the per-key hot path; one map update.
pub(crate) fn record_command_stats(
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out_before: usize,
    out: &[u8],
    elapsed_us: u64,
) {
    // LATENCY HISTOGRAM (#546): record this command's elapsed micros into the shard's per-shard
    // histogram FIRST, before the `spec_of` gate below early-returns for an unknown command. The
    // histogram is the operator-facing tail-latency view (p99/p99.9 graphable from `/metrics`), so
    // it must count EVERY command that reached the timing site -- known or not -- to stay consistent
    // with the commands-processed total. This is the single per-command choke point every serve path
    // funnels through (the tokio loop, the io_uring loop, and the deferred-hop drain all call it with
    // the SAME reused SLOWLOG/COMMANDSTATS elapsed), so recording here covers them all with no new
    // clock read: a branchless find-bucket + one relaxed atomic increment (see `observe_latency`).
    state_rc.borrow().counters.observe_latency(elapsed_us);
    let cmd_upper = ascii_upper(request.command());
    let Some(spec) = ironcache_server::spec_of(&cmd_upper) else {
        return;
    };
    // The reply for THIS command begins at `out_before`; a leading `-` is an error reply (the
    // command ran but failed). A push/array/status/integer/bulk lead byte is a success.
    let failed = out.get(out_before) == Some(&b'-');
    // COMMANDSTATS node-wide (#527): record this command's calls/usec/failed into THIS shard's
    // per-command atomic slot in the metrics registry (indexed by the command's STABLE ordinal), so
    // INFO COMMANDSTATS sums it across shards via `aggregate_command_stats` -- the per-command analog
    // of the #545 `# Stats` rollup. ONE relaxed atomic add, no lock, no allocation (the slot is
    // pre-allocated). Only registry commands reach here (the `spec_of` gate above), so the ordinal is
    // always present; a defensive miss is a no-op.
    if let Some(index) = ironcache_server::command_stat_index(spec.name) {
        state_rc
            .borrow()
            .counters
            .on_command_stat(index, elapsed_us, failed);
    }
    if failed {
        // The error CODE: the first whitespace/CR-delimited token after the `-`. ERRORSTATS stays
        // serving-shard-scoped (#527 follow-up): record it into THIS shard's local error table for
        // the `errorstat_*` section (cross-shard error aggregation is the remaining smaller follow-up).
        let code_start = out_before + 1;
        let rest = &out[code_start..];
        let code_len = rest
            .iter()
            .position(|&b| b == b' ' || b == b'\r')
            .unwrap_or(rest.len());
        state_rc
            .borrow_mut()
            .command_stats
            .record_error(&rest[..code_len]);
    }
}

/// CLIENT TRACKING read-register / write-invalidate hook (#409), run after each command. A READ by
/// a tracking connection registers its read keys in THIS shard's tracking table; ANY write (by any
/// connection) invalidates its keys for every tracking client (NOLOOP skips the writer's own
/// connection); FLUSHALL/FLUSHDB invalidate everything.
///
/// PERF: the common no-tracking path is gated to a single bool + one thread-local borrow + `is_none`
/// (the table is never created until a tracking client reads), so a server with no tracking clients
/// pays one cheap check per command and allocates nothing. Only when tracking is active does it
/// uppercase the command + consult the key spec.
///
/// SCOPE (this stage): SINGLE-SHARD-correct. A tracking client's read and the matching write both
/// run on the key's owner shard, which IS this shard when `shards == 1` (the default + the
/// differential bar). The cross-shard case (a read routed to a remote owner shard) is a documented
/// follow-up; a stale foreign-shard entry self-heals when its key next changes (the push to a gone
/// connection fails and is shed).
/// Whether a DEFAULT-mode tracking read should register its keys, given the OPTIN/OPTOUT mode and
/// the one-shot `CLIENT CACHING` flag (#409 stage 3). Default mode (neither OPTIN nor OPTOUT) tracks
/// every read; OPTIN tracks only after `CACHING YES`; OPTOUT tracks unless `CACHING NO`.
fn tracking_should_register_read(conn: &ConnState) -> bool {
    if conn.tracking_optin {
        conn.caching_next == Some(true)
    } else if conn.tracking_optout {
        conn.caching_next != Some(false)
    } else {
        true
    }
}

/// Consume the one-shot `CLIENT CACHING` flag (#409 stage 3): it is cleared after the command that
/// FOLLOWS `CLIENT CACHING` (i.e. every command except `CLIENT CACHING` itself, which sets it), so
/// the OPTIN/OPTOUT decision applies to exactly one command. A single `is_some` check when no flag
/// is pending (the common case), so the non-OPTIN hot path is unaffected.
pub(crate) fn consume_caching_flag(conn: &mut ConnState, request: &Request) {
    if conn.caching_next.is_none() {
        return;
    }
    let is_caching_cmd = request.command().eq_ignore_ascii_case(b"CLIENT")
        && request
            .args
            .get(1)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"CACHING"));
    if !is_caching_cmd {
        conn.caching_next = None;
    }
}

pub(crate) fn apply_client_tracking(
    conn: &ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    request: &Request,
    was_tracking: bool,
    was_bcast: bool,
) {
    // CLIENT TRACKING OFF / RESET transition: the connection WAS tracking and now is not. Purge it
    // from this shard's table (both the per-key and the BCAST prefix sets) so a later write does
    // not push to a connection that opted out.
    if was_tracking && !conn.tracking_on {
        purge_conn_tracking(conn.id);
        return;
    }

    // BCAST mode transitions (#409 stage 2). Entering BCAST registers the connection's prefixes
    // (the EMPTY prefix when none were given = track all keys); leaving BCAST (a re-issue of
    // TRACKING ON without BCAST) purges the stale prefix entries. Both happen on the CLIENT TRACKING
    // command itself (it registers no read and writes no key), so we fall through after.
    if was_bcast && !conn.tracking_bcast {
        purge_conn_tracking(conn.id);
    }
    if conn.tracking_bcast && !was_bcast {
        // A REDIRECT client (stage 4) whose target is not (yet) subscribed registers nothing; for
        // BCAST that means the target must SUBSCRIBE `__redis__:invalidate` BEFORE enabling tracking
        // (BCAST registers once here, not per read, so it does not self-heal on a later read).
        if let Some(entry) = make_track_entry(conn, push_tx, shed_flag) {
            let tbl = shard_tracking();
            let mut t = tbl.borrow_mut();
            if conn.tracking_prefixes.is_empty() {
                t.track_prefix(bytes::Bytes::new(), conn.id, entry);
            } else {
                for p in &conn.tracking_prefixes {
                    t.track_prefix(p.clone(), conn.id, entry.clone());
                }
            }
        }
    }

    // The cheap gate: skip entirely unless THIS connection is tracking (it may need to register a
    // read) OR the per-shard table already holds trackers (a write may need to invalidate).
    let table_has_trackers = TRACKING.with(|cell| {
        cell.borrow()
            .as_ref()
            .is_some_and(|t| !t.borrow().is_empty())
    });
    if !conn.tracking_on && !table_has_trackers {
        return;
    }

    let cmd_upper = ascii_upper(request.command());
    if ironcache_server::is_write(&cmd_upper) {
        if !table_has_trackers {
            return;
        }
        let tbl = shard_tracking();
        let mut t = tbl.borrow_mut();
        // NOLOOP: a tracking writer does not get the echo for its OWN change.
        let skip = conn.tracking_noloop.then_some(conn.id);
        if cmd_upper == b"FLUSHALL" || cmd_upper == b"FLUSHDB" {
            t.invalidate_all(skip);
            return;
        }
        // Conservative: invalidate every key the write NAMED (a no-op write over-invalidates, which
        // is safe -- the client just re-reads; it never MISSES a real change).
        match ironcache_server::command_keys(&cmd_upper, request) {
            ironcache_server::KeySpec::One(k) => {
                t.invalidate(k, skip);
            }
            ironcache_server::KeySpec::Many(ks) => {
                for k in ks {
                    t.invalidate(k, skip);
                }
            }
            ironcache_server::KeySpec::None => {}
        }
    } else if conn.tracking_on && !conn.tracking_bcast && tracking_should_register_read(conn) {
        // A READ by a DEFAULT-mode tracking connection (and, in OPTIN/OPTOUT, one the one-shot
        // CLIENT CACHING gate admits): register every key it read so a later change pushes an
        // invalidation. (A BCAST connection tracks PREFIXES, not reads, so it skips this.) A
        // non-keyed read (PING/INFO/...) registers nothing.
        let keys: Vec<bytes::Bytes> = match ironcache_server::command_keys(&cmd_upper, request) {
            ironcache_server::KeySpec::One(k) => vec![bytes::Bytes::copy_from_slice(k)],
            ironcache_server::KeySpec::Many(ks) => ks
                .iter()
                .map(|k| bytes::Bytes::copy_from_slice(k))
                .collect(),
            ironcache_server::KeySpec::None => Vec::new(),
        };
        if keys.is_empty() {
            return;
        }
        // A REDIRECT client (stage 4) registers the TARGET's handle; if the target is not currently
        // subscribed to `__redis__:invalidate`, skip (it self-heals when the client next reads).
        let Some(entry) = make_track_entry(conn, push_tx, shed_flag) else {
            return;
        };
        let tbl = shard_tracking();
        let mut t = tbl.borrow_mut();
        for k in keys {
            t.track(k, conn.id, entry.clone());
        }
    }
}

/// Build the tracking-table entry for THIS connection's registration (#409). A REDIRECT client
/// (stage 4, `tracking_redirect != 0`) registers the redirect TARGET's push handle with
/// `redirect = true` (the target must be SUBSCRIBEd to `__redis__:invalidate`); a non-redirect
/// client registers its OWN push handle. Returns `None` for a redirect client whose target is not
/// currently subscribed there, so the caller skips registration (it self-heals on a later read).
fn make_track_entry(
    conn: &ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
) -> Option<crate::pubsub::TrackEntry> {
    if conn.tracking_redirect != 0 {
        let sub = resolve_redirect_target(conn.tracking_redirect)?;
        Some(crate::pubsub::TrackEntry {
            sub,
            redirect: true,
        })
    } else {
        Some(crate::pubsub::TrackEntry {
            sub: crate::pubsub::Subscriber {
                sender: push_tx.clone(),
                shed: std::sync::Arc::clone(shed_flag),
            },
            redirect: false,
        })
    }
}

/// Resolve the push handle of a CLIENT TRACKING REDIRECT target (#409 stage 4): the target must have
/// SUBSCRIBEd `__redis__:invalidate`, so its [`crate::pubsub::Subscriber`] lives in THIS shard's
/// Pub/Sub channel table. Returns `None` when the target is not (currently) subscribed there. SCOPE:
/// single-shard-correct, exactly like the rest of tracking (the redirect target's SUBSCRIBE and the
/// key's owner shard coincide when `shards == 1`).
fn resolve_redirect_target(target_id: u64) -> Option<crate::pubsub::Subscriber> {
    let tbl = shard_pubsub();
    let t = tbl.borrow();
    t.channels
        .get(crate::pubsub::REDIRECT_INVALIDATE_CHANNEL)
        .and_then(|subs| subs.get(&target_id))
        .cloned()
}

/// HOTKEYS recording hook (#428): attribute one command's resource use to its keys while a tracking
/// session is active. The CALLER gates this on `ctx.hotkeys.is_active()` (one relaxed atomic), so the
/// default (no session) path never reaches here. Piggybacks the already-measured `cmd_elapsed_us`
/// (the CPU metric) and the reply byte delta; the request payload bytes are summed here. A command
/// with no routable key (HOTKEYS itself, PING, ...) attributes nothing to any key, only to the
/// session totals.
pub(crate) fn record_hotkeys(
    ctx: &ServerContext,
    env: &Rc<RefCell<SystemEnv>>,
    request: &Request,
    cmd_elapsed_us: u64,
    reply_bytes: u64,
) {
    let cmd_upper = ascii_upper(request.command());
    let req_bytes: u64 = request.args.iter().map(|a| a.len() as u64).sum();
    let net_bytes = req_bytes.saturating_add(reply_bytes);
    let now_ms = env.borrow().now_unix_millis();
    let keys: Vec<&[u8]> = match ironcache_server::command_keys(&cmd_upper, request) {
        ironcache_server::KeySpec::One(k) => vec![k],
        ironcache_server::KeySpec::Many(ks) => ks,
        ironcache_server::KeySpec::None => Vec::new(),
    };
    ctx.hotkeys.record(&keys, cmd_elapsed_us, net_bytes, now_ms);
}

pub(crate) fn record_slow_command(
    ctx: &ServerContext,
    env: &Rc<RefCell<SystemEnv>>,
    conn: &ConnState,
    request: &Request,
    start: ironcache_env::Monotonic,
    threshold_micros: i64,
) {
    // Read the clock ONCE (monotonic now + wall unix) under a single short borrow.
    let (elapsed, unix_secs) = {
        let e = env.borrow();
        let elapsed = e.now().saturating_duration_since(start);
        let unix_secs = e.now_unix_millis() / 1000;
        (elapsed, unix_secs)
    };
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    // Threshold compare: `threshold_micros` is >= 0 here (the caller gated on enabled). `0` logs
    // everything; a positive value logs only commands at/above it.
    if micros >= u64::try_from(threshold_micros).unwrap_or(0) {
        let raw_args: Vec<Vec<u8>> = request.args.iter().map(|a| a.to_vec()).collect();
        // SECRET-LEAK FIX: SLOWLOG entries are readable by any @admin via `SLOWLOG GET`, and with
        // `slowlog-log-slower-than 0` EVERY command (including auth) is logged. Redact the secret
        // args of auth/password-setting commands BEFORE they enter the ring, so a password never
        // sits in the slow log in cleartext (Redis applies the same per-command sensitive-arg rule).
        let cmd_upper = ascii_upper(request.command());
        let raw_args = redact_args_for_slowlog(&cmd_upper, raw_args);
        ctx.slowlog.record(
            unix_secs,
            micros,
            &raw_args,
            conn.addr.clone(),
            conn.name.clone(),
        );
    }
    // LATENCY `command` event (PROD-7): sample this command's elapsed time in MILLISECONDS, gated on
    // a fixed floor so only meaningful spikes are recorded (a sub-millisecond command is never a
    // latency event). This is the always-tracked event; subsystem events are a follow-up.
    if micros >= ironcache_observe::LATENCY_COMMAND_FLOOR_MICROS {
        ctx.latency.record("command", unix_secs, micros / 1000);
    }
}

/// The placeholder a redacted SLOWLOG argument is replaced with (Redis convention).
const SLOWLOG_REDACTED: &[u8] = b"(redacted)";

/// Redact the secret arguments of `args` (the verbatim request, `args[0]` = command) for a
/// SLOWLOG entry, based on the UPPERCASED command `cmd_upper`. Returns the (possibly rewritten)
/// argument vector; non-sensitive commands are returned UNCHANGED.
///
/// This runs ONLY inside [`record_slow_command`], i.e. only for a command already deemed slow,
/// so it is off the hot path. It mirrors the Redis per-command sensitive-arg rule:
/// - `AUTH`: every arg after the verb is the credential (`AUTH pass` or `AUTH user pass`); redact
///   all of them. (Redis only redacts the password; redacting every post-verb arg is a strict
///   superset that can never leak the password and never reveals a username either.)
/// - `HELLO`: redact the two args following an `AUTH` token (the username and password).
/// - `CONFIG SET`: when the parameter (arg2, case-insensitive) is `requirepass` or `masterauth`,
///   redact its value (arg3).
/// - `ACL SETUSER`: redact every password/hash rule token (`>`/`<`/`#`/`!`) via the shared
///   [`ironcache_server::acl::redacted_rule`] so the redaction matches the ACL error reply.
fn redact_args_for_slowlog(cmd_upper: &[u8], mut args: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    match cmd_upper {
        b"AUTH" => {
            // Redact every arg after the verb (the credential(s)).
            for a in args.iter_mut().skip(1) {
                *a = SLOWLOG_REDACTED.to_vec();
            }
        }
        b"HELLO" => {
            // Redact the (user, pass) pair following an AUTH token, wherever it sits.
            let mut i = 1;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"AUTH") {
                    for a in args.iter_mut().skip(i + 1).take(2) {
                        *a = SLOWLOG_REDACTED.to_vec();
                    }
                    break;
                }
                i += 1;
            }
        }
        b"CONFIG" => {
            // CONFIG SET <param> <value> [<param> <value> ...]: redact the value of every
            // requirepass/masterauth pair (case-insensitive param match).
            if args.len() >= 2 && args[1].eq_ignore_ascii_case(b"SET") {
                let mut i = 2;
                while i + 1 < args.len() {
                    if args[i].eq_ignore_ascii_case(b"requirepass")
                        || args[i].eq_ignore_ascii_case(b"masterauth")
                    {
                        args[i + 1] = SLOWLOG_REDACTED.to_vec();
                    }
                    i += 2;
                }
            }
        }
        b"ACL" => {
            // ACL SETUSER <name> <rule>...: redact each password/hash rule token, reusing the
            // canonical ACL redactor so SLOWLOG and the ACL error reply never drift.
            if args.len() >= 3 && args[1].eq_ignore_ascii_case(b"SETUSER") {
                for a in args.iter_mut().skip(3) {
                    let token = String::from_utf8_lossy(a);
                    let redacted = ironcache_server::acl::redacted_rule(&token);
                    if redacted != token {
                        *a = redacted.into_bytes();
                    }
                }
            }
        }
        _ => {}
    }
    args
}

/// Purge a connection from this shard's CLIENT TRACKING table (#409): `CLIENT TRACKING OFF` /
/// RESET / disconnect. Accesses the table only if it EXISTS (no tracking client ever -> no-op, no
/// allocation), so the common no-tracking close path is one thread-local borrow + `is_none`.
pub(crate) fn purge_conn_tracking(conn_id: u64) {
    TRACKING.with(|cell| {
        if let Some(t) = cell.borrow().as_ref() {
            t.borrow_mut().forget_conn(conn_id);
        }
    });
}

