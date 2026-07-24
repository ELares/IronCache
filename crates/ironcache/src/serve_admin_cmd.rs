// SPDX-License-Identifier: MIT OR Apache-2.0
//! Administrative command handlers split out of `serve.rs` (#625): SAVE/BGSAVE/LASTSAVE
//! persistence (#58), the ACL command surface, and the graceful SHUTDOWN path. Behavior-preserving
//! relocation: the bodies are byte-identical to their former in-`serve.rs` definitions.

use super::{encode_into, shard_env, shard_state};
use crate::coordinator;
use ironcache_env::{Clock, Env, SystemEnv};
use ironcache_runtime::bootstrap::ShardId;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, Request};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// Handle SAVE / BGSAVE / LASTSAVE when persistence is ENABLED (#58). Bumps `commands_processed`
/// (matching every other route), then:
///
/// - `SAVE`: BLOCKS until every shard has dumped its partition AND the manifest is committed (Redis
///   parity), then replies `+OK` (or an `-ERR` on a shard / manifest failure). The fan-out is the
///   forkless, borrow-releasing per-shard dump, so it never double-memories the keyspace.
/// - `BGSAVE`: SPAWNS the SAME save off the request path on the home shard's executor and replies
///   `+Background saving started` IMMEDIATELY, so the ISSUING connection is not blocked. The per-shard
///   dump now YIELDS between snapshot chunks (#571), re-acquiring the store borrow per chunk, so a
///   dumping shard services queued writes DURING its dump instead of being blocked for the whole
///   keyspace dump -- a bounded, predictable save tail. (The snapshot is then an approximate
///   warm-start point rather than a strict per-shard point-in-time; see `crate::persist`.)
/// - `LASTSAVE`: replies `:<unix_secs>` of the last committed save (`:0` until the first save).
///
/// Concurrent saves are serialized by [`crate::persist::PersistState::try_begin_save`]: a SAVE /
/// BGSAVE / periodic tick that finds a save already in progress is a no-op success (BGSAVE) or
/// proceeds once the latch is free. The save TIMESTAMP is read from the home shard's Env Clock seam
/// (the determinism boundary, ADR-0003).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_persist_command(
    persist: &Arc<crate::persist::PersistState>,
    ctx: &ServerContext,
    conn: &ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    cmd_upper: &[u8],
    request: &Request,
    out: &mut Vec<u8>,
) {
    use ironcache_runtime::Runtime;
    use ironcache_server::Value;
    shard_state().borrow_mut().counters.on_command();

    // -- AUTH (H2). The old inline NOAUTH gate that lived here was REMOVED when the gate was hoisted
    // to the single router chokepoint at the top of `route_and_dispatch`: an unauthenticated client
    // (requirepass set) is now short-circuited with `-NOAUTH` THERE, before SAVE/BGSAVE/LASTSAVE is
    // ever intercepted, so this handler is unreachable unauth and the inline gate was dead code. The
    // chokepoint covers this path AND every other (cross-shard, fan-out, CLUSTER mutator, SHUTDOWN),
    // which the point-fix here never could. See the hoisted gate for the full rationale.

    match cmd_upper {
        b"LASTSAVE" => {
            if request.args.len() != 1 {
                encode_into(
                    out,
                    &Value::error(ironcache_protocol::ErrorReply::wrong_arity("lastsave")),
                    conn.proto,
                );
                return;
            }
            #[allow(clippy::cast_possible_wrap)]
            let secs = persist.last_save() as i64;
            encode_into(out, &Value::Integer(secs), conn.proto);
        }
        b"SAVE" => {
            // `SAVE` -> the normal DURABLE data_dir save (Redis parity). `SAVE HANDOFF` -> the #390
            // upgrade-handoff save, staged on tmpfs when the RAM-headroom guard admits it (else the
            // durable data_dir); it is issued by `ironcache upgrade` to shrink the reload window and
            // is client-reachable only over the auth-gated loopback the upgrade CLI uses. Any other
            // argument shape is a syntax error.
            let handoff = match request.args.len() {
                1 => false,
                2 if request.args[1].eq_ignore_ascii_case(b"HANDOFF") => true,
                _ => {
                    encode_into(
                        out,
                        &Value::error(ironcache_protocol::ErrorReply::wrong_arity("save")),
                        conn.proto,
                    );
                    return;
                }
            };
            // The save timestamp from the home shard's Env clock (ADR-0003), in unix seconds.
            let now_secs = shard_env().borrow().now_unix_millis() / 1_000;
            // Serialize against a concurrent save. If one is already running, wait for the latch by
            // proceeding once free is overkill here; a SAVE that races a BGSAVE simply runs after the
            // latch frees on the next attempt. Acquire-or-bail: if busy, report the in-progress save
            // as a success (its data is being written), matching the "save is happening" intent.
            // The RAII guard releases the latch on completion AND on a panic unwinding the save (H3).
            let Some(_guard) = persist.try_begin_save() else {
                encode_into(out, &Value::ok(), conn.proto);
                return;
            };
            let result = if handoff {
                crate::persist::do_handoff_save_all(persist, inbox, ctx, home, conn.db, now_secs)
                    .await
            } else {
                crate::persist::do_save_all(persist, inbox, ctx, home, conn.db, now_secs).await
            };
            match result {
                Ok(()) => encode_into(out, &Value::ok(), conn.proto),
                Err(msg) => encode_into(
                    out,
                    &Value::error(ironcache_protocol::ErrorReply::err(msg)),
                    conn.proto,
                ),
            }
        }
        // BGSAVE [SCHEDULE]: kick the save off the request path and reply immediately.
        _ => {
            // The save timestamp captured NOW (on the request path) so the background save records
            // a faithful start time; the dump runs after, but LASTSAVE reporting the request time is
            // Redis-faithful enough (Redis stamps lastsave at fork time).
            let now_secs = shard_env().borrow().now_unix_millis() / 1_000;
            if let Some(guard) = persist.try_begin_save() {
                // Spawn the save on THIS (home) shard's executor; it owns the borrow-free fan-out.
                // The RAII guard is MOVED into the task so the latch releases when the task finishes
                // OR when it is CANCELLED at shutdown (the bare release_save() before could be
                // skipped on cancel, wedging the latch forever -- H3).
                let persist = Arc::clone(persist);
                let inbox = inbox.clone();
                let ctx = ctx.clone();
                let db = conn.db;
                let rt = ironcache_runtime::TokioRuntime::new();
                rt.spawn_on_shard(async move {
                    let _guard = guard; // dropped on task completion or cancellation -> releases.
                    let _ = crate::persist::do_save_all(&persist, &inbox, &ctx, home, db, now_secs)
                        .await;
                });
            }
            // Whether we won the latch or a save was already running, the Redis-faithful reply is
            // the same acknowledgement (a save is in progress).
            encode_into(
                out,
                &Value::SimpleString("Background saving started".to_owned()),
                conn.proto,
            );
        }
    }
}

/// Handle the `ACL` admin command family (#106) in the serve layer. Resolves the connection's
/// WHOAMI (the cached ACL user's name, or `default` for the implicit all-permissive default),
/// runs [`ironcache_server::dispatch_acl`] against the shared registry with the determinism-seam
/// RNG (for GENPASS), then performs any aclfile SAVE/LOAD I/O the handler asks for (the server
/// crate cannot touch `std::fs` on the data path, so the file I/O lives here, next to boot LOAD).
///
/// SAVE writes [`AclState::serialize_aclfile`] to the configured `aclfile`; LOAD reads it and
/// calls [`AclState::load_users`]. With NO `aclfile` configured both reply the Redis-faithful
/// `-ERR This Redis instance is not configured to use an ACL file...`. Passwords are persisted
/// only as `#<sha256-hex>` digests; an I/O or parse error is surfaced (never a plaintext secret).
pub(crate) fn handle_acl_command(
    ctx: &ServerContext,
    conn: &ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    use ironcache_server::{AclSideEffect, Value};
    shard_state().borrow_mut().counters.on_command();

    // WHOAMI: the cached ACL identity's name, or `default` (the implicit all-permissive default
    // / legacy-requirepass posture caches `None`). Resolved here, not on the data path.
    let whoami: &str = conn
        .acl_user
        .as_deref()
        .map_or(ironcache_server::DEFAULT_USER, |u| u.name.as_str());

    // Run the pure ACL handler with the determinism-seam RNG (GENPASS draws from it, ADR-0003).
    let (reply, effect) = {
        let mut env_ref = env.borrow_mut();
        ironcache_server::dispatch_acl(&ctx.acl, whoami, env_ref.rng(), request)
    };

    let reply = match effect {
        AclSideEffect::None => reply,
        AclSideEffect::Save(text) => match ctx.boot.aclfile.as_ref() {
            None => Value::error(ironcache_protocol::ErrorReply::err(
                "This Redis instance is not configured to use an ACL file. \
                 You may want to specify users via the ACL SETUSER command and then issue a \
                 CONFIG REWRITE (assuming you have a Redis configuration file set) in order to \
                 store users in the Redis configuration.",
            )),
            Some(path) => match std::fs::write(path, text.as_bytes()) {
                Ok(()) => reply,
                Err(e) => Value::error(ironcache_protocol::ErrorReply::err(format!(
                    "ACL SAVE failed writing the aclfile: {e}"
                ))),
            },
        },
        AclSideEffect::Load => match ctx.boot.aclfile.as_ref() {
            None => Value::error(ironcache_protocol::ErrorReply::err(
                "This Redis instance is not configured to use an ACL file. \
                 You may want to specify users via the ACL SETUSER command and then issue a \
                 CONFIG REWRITE (assuming you have a Redis configuration file set) in order to \
                 store users in the Redis configuration.",
            )),
            Some(path) => match std::fs::read_to_string(path) {
                Ok(text) => match ctx.acl.load_users(&text) {
                    Ok(_) => reply,
                    // The error never includes a plaintext password (the file holds only
                    // #digests / the redacted rule), so it is safe to surface verbatim.
                    Err((lineno, e)) => Value::error(ironcache_protocol::ErrorReply::err(format!(
                        "ACL LOAD failed at aclfile line {lineno}: {}",
                        e.reason
                    ))),
                },
                Err(e) => Value::error(ironcache_protocol::ErrorReply::err(format!(
                    "ACL LOAD failed reading the aclfile: {e}"
                ))),
            },
        },
    };
    encode_into(out, &reply, conn.proto);
}

/// Handle the `SHUTDOWN [NOSAVE|SAVE]` graceful-shutdown command (#139, SHUTDOWN.md). This is the
/// LIVE path for every non-MULTI SHUTDOWN (the serve router intercepts it before generic dispatch,
/// which cannot exit the process). The sequence (Redis-faithful):
///
/// 1. AUTH is enforced UPSTREAM by the hoisted NOAUTH chokepoint at the top of `route_and_dispatch`
///    (an UNAUTHENTICATED client with `requirepass` set is short-circuited with `-NOAUTH` before
///    SHUTDOWN is ever intercepted), so a public port still cannot be killed by an anonymous
///    SHUTDOWN -- the gate moved upstream + now covers every command, so the old inline gate here
///    was removed as dead code.
/// 2. PARSE the modifier ([`ironcache_server::parse_shutdown`], shared with the dispatch fallback so
///    the grammar cannot diverge): a bad/extra modifier replies `-ERR syntax error` and does NOT
///    exit.
/// 3. RESOLVE the save decision [redis-shutdown-save-nosave-default]:
///      * `SHUTDOWN SAVE`   -> save-on-exit ALWAYS. If persistence is NOT configured (no `data_dir`)
///        there is nowhere to save, so it replies `-ERR ... no data_dir configured` and does NOT
///        exit (Redis errors when it cannot honor a forced SAVE -- we surface the same fail rather
///        than exit-0 over unwritten data).
///      * `SHUTDOWN NOSAVE` -> exit 0 IMMEDIATELY without saving (even with a save policy).
///      * bare `SHUTDOWN`   -> save IFF a save policy is configured (persistence on +
///        `has_save_policy`), else exit without saving.
/// 4. If saving was resolved, perform the SYNCHRONOUS cross-shard save reusing the SAME atomic
///    persistence path SAVE uses ([`crate::persist::do_save_all`] -- forkless per-shard dump +
///    manifest committed LAST via a tmp->rename, so there is never a half-written file). A save
///    FAILURE replies `-ERR ...` and does NOT exit (fail-closed: an orchestrator must not record a
///    clean stop that lost data).
/// 5. On a resolved clean stop the process exits with code 0 (the orchestrator contract): SHUTDOWN
///    does NOT reply on success (Redis: the process is gone). The committed manifest is durable
///    BEFORE the exit, and the atomic rename means killed background tasks leave no torn file.
///
/// On any refused / failed save this returns normally (a reply is in `out`); on a clean stop it
/// NEVER returns (`std::process::exit`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_shutdown_command(
    persist: Option<&Arc<crate::persist::PersistState>>,
    ctx: &ServerContext,
    conn: &ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    request: &Request,
    out: &mut Vec<u8>,
) {
    use ironcache_server::{ShutdownMode, Value};
    shard_state().borrow_mut().counters.on_command();

    // 1. AUTH. The old inline NOAUTH gate here was REMOVED when the gate was hoisted to the single
    // router chokepoint at the top of `route_and_dispatch`: an unauthenticated client (requirepass
    // set) is short-circuited with `-NOAUTH` THERE, before SHUTDOWN is ever intercepted, so a public
    // port still cannot be killed by an anonymous SHUTDOWN -- the protection moved upstream and now
    // covers every path uniformly, not just this one. See the hoisted gate for the full rationale.

    // 2. PARSE the modifier (shared grammar with the dispatch fallback). A bad modifier is a syntax
    // error and does NOT shut down.
    let mode = match ironcache_server::parse_shutdown(request) {
        Ok(mode) => mode,
        Err(e) => {
            encode_into(out, &Value::error(e), conn.proto);
            return;
        }
    };

    // 3. RESOLVE whether this stop saves. SAVE forces it (and errors if it cannot); NOSAVE never
    // saves; the bare form saves iff a save policy is configured.
    let want_save = match mode {
        ShutdownMode::NoSave => false,
        ShutdownMode::Save => {
            if persist.is_none() {
                // A forced SAVE with no data_dir cannot be honored: error, do NOT exit (Redis errors
                // when it cannot save rather than silently exiting over unwritten data).
                encode_into(
                    out,
                    &Value::error(ironcache_protocol::ErrorReply::err(
                        "Errors trying to SHUTDOWN. Check logs. (no data_dir configured for SAVE)",
                    )),
                    conn.proto,
                );
                return;
            }
            true
        }
        // Bare SHUTDOWN: save iff persistence is on AND a save policy (a periodic cadence) exists.
        // The policy is the LIVE runtime one (`CONFIG SET save` may have changed it since boot).
        ShutdownMode::Default => persist.is_some() && ctx.runtime.has_save_policy(),
    };

    // 4. If saving was resolved, perform the SYNCHRONOUS atomic save reusing the SAVE path. A save
    // FAILURE is fail-closed: reply the error and do NOT exit, so the connection keeps serving and
    // the orchestrator does not see a clean stop over unwritten data.
    if want_save {
        // `want_save` is only ever true with persistence configured (the Save-with-no-data_dir case
        // returned above, and Default gates on `persist.is_some()`), so this expect documents that
        // invariant rather than guarding a reachable None.
        let persist = persist.expect("want_save implies persistence is configured");
        // The save timestamp from the home shard's Env clock (ADR-0003), in unix seconds.
        let now_secs = shard_env().borrow().now_unix_millis() / 1_000;
        // H1 (data loss): the OLD code did `try_begin_save() else { /* covered */ }` and FELL THROUGH
        // to exit(0) when the latch was busy. But a concurrent BGSAVE / periodic save may be mid-
        // `do_save_all` with some `.icss` files written and the manifest (the atomic COMMIT point)
        // NOT yet run, so exiting over it KILLS that save before it commits -- the committed manifest
        // still points at the PRIOR snapshot and every write since is LOST despite this explicit
        // SAVE-on-exit. The fix: BOUNDED-WAIT for the busy latch to free (the in-flight save commits
        // + drops its guard; on a single-threaded executor the timer await yields to it), THEN run a
        // FRESH save (the operator demanded a CURRENT save), THEN exit. No borrow is held across the
        // wait (it only touches the `saving` atomic + the timer seam), so it cannot deadlock.
        let Some(_guard) =
            crate::persist::wait_to_begin_save(persist, crate::persist::SHUTDOWN_SAVE_WAIT).await
        else {
            // The wait TIMED OUT: a genuinely wedged save never freed the latch (the LOW case). Do
            // NOT hang forever -- proceed to a BEST-EFFORT exit. The in-flight save MAY still commit
            // its prior-or-partial state; we cannot do better without unbounded waiting.
            tracing::warn!(
                ?mode,
                "ironcache: SHUTDOWN: a prior save did not finish within SHUTDOWN_SAVE_WAIT; \
                 exiting best-effort (the in-flight save may still commit)"
            );
            std::process::exit(0);
        };
        // We hold the freed latch: run a FRESH save, BOUNDED so a wedged sibling drain loop (alive
        // but stuck) cannot hang the exit (L1). A failure (or fan-out timeout) is fail-closed: reply
        // the error and do NOT exit, so the orchestrator does not record a clean stop over unwritten
        // data and the connection keeps serving.
        match crate::persist::do_save_all_bounded(
            persist,
            inbox,
            ctx,
            home,
            conn.db,
            now_secs,
            crate::persist::SHUTDOWN_SAVE_WAIT,
        )
        .await
        {
            Ok(()) => {} // committed; fall through to the clean exit.
            Err(msg) => {
                encode_into(
                    out,
                    &Value::error(ironcache_protocol::ErrorReply::err(format!(
                        "Errors trying to SHUTDOWN. Check logs. ({msg})"
                    ))),
                    conn.proto,
                );
                return;
            }
        }
    }

    // 5. CLEAN STOP. The resolved save (if any) is committed + durable; exit 0 (the orchestrator
    // contract). SHUTDOWN does NOT reply on success (Redis: the process is gone). `process::exit`
    // is faithful to Redis's own SHUTDOWN handler (it exits from the command path after the save);
    // the committed manifest's atomic rename means the killed background tasks leave no torn file.
    tracing::info!(?mode, "ironcache: SHUTDOWN -> exit 0");
    std::process::exit(0);
}
