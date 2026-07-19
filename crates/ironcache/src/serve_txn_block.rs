// SPDX-License-Identifier: MIT OR Apache-2.0
//! Transaction (MULTI queue-time routing) + blocking-command (BLPOP-family / WAIT park) handling
//! split out of `serve.rs` (#625), plus the shard-spanning gather-combine dispatch. These own the
//! block-park request shape, the queue-time home/hop gate for an in-MULTI command, and the park
//! loop the OWNING serve loop drives. Behavior-preserving relocation: the bodies are byte-identical.

use super::{
    MigrationCtx, ShardState, ShardStoreImpl, all_keys_home_owned, cluster_redirect, encode_into,
    handle_request, is_fan_out_spanning_zset, publish_pending_keyspace_events,
    replica_read_in_sync, shard_blocking, shard_owner_home, shard_state,
};
use crate::coordinator;
use ironcache_env::{Clock, SystemEnv};
use ironcache_runtime::bootstrap::ShardId;
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, ProtoVersion, Request, TimingWheel, UnixMillis, route};
use std::cell::RefCell;
use std::rc::Rc;

/// A request to PARK a connection on a blocking command (PROD-9). When [`route_and_dispatch`]
/// finds a blocking pop's keys all empty (and the connection is NOT in MULTI), it sets the
/// serve loop's `block_request` out-param to this instead of replying, so the OWNING serve loop
/// (which holds the stream + the timer + the read buffer) runs the park loop: it registers a
/// waiter, `select!`s on (the wake / the timeout / a peer close), and on a wake re-attempts the
/// pop. The spec + db are everything the re-attempt needs; the home shard owns the keys.
pub(crate) struct BlockPark {
    /// The parsed blocking command (timeout + keys + op): the re-attempt reuses it. `pub(crate)` so
    /// the io_uring serve loop's FIX1 immediate-reply path can read `spec.op` (#625).
    pub(crate) spec: ironcache_server::BlockSpec,
    /// The connection's selected DB at park time (the re-attempt + the waiter key are db-scoped).
    pub(crate) db: u32,
}
/// Dispatch ONE shard-spanning gather-combine command to its per-command fan-out
/// (COORDINATOR.md #107, Stage 2b), split out of [`route_and_dispatch`] so the router stays
/// small. The caller has already established the command is a supported gather-combine token
/// whose keys SPAN shards (the `spanning_set_fan_out` gate) and bumped `commands_processed`.
///
/// BITOP / PFCOUNT / PFMERGE (Stage 2b-3) each have their OWN parse + combine, so each gets a
/// dedicated fan-out; the eight zset tokens (Stage 2b-2) share `fan_out_zset`; the seven set
/// tokens (Stage 2b-1) share `fan_out_set`. The fan-out gathers each source from its owner
/// (the home subset LOCALLY + sync, the rest via their drain loops), combines with the PURE
/// combiner shared with the single-shard handler, and for the write forms writes the result
/// to the dest owner, encoding the reply into `out`.
pub(crate) async fn dispatch_spanning_combine(
    ctx: &ServerContext,
    conn: &ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    cmd_upper: &[u8],
    request: &Request,
    out: &mut Vec<u8>,
) {
    match cmd_upper {
        b"BITOP" => {
            crate::spanning_combine::fan_out_bitop(
                inbox, ctx, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        b"PFCOUNT" => {
            crate::spanning_combine::fan_out_pfcount(
                inbox, ctx, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        b"PFMERGE" => {
            crate::spanning_combine::fan_out_pfmerge(
                inbox, ctx, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        _ if is_fan_out_spanning_zset(cmd_upper) => {
            crate::spanning_combine::fan_out_zset(
                inbox, ctx, cmd_upper, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        _ => {
            crate::spanning_combine::fan_out_set(
                inbox, ctx, cmd_upper, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
    }
}

/// The in-MULTI transaction-correctness guards (COORDINATOR.md #107, the critical fix), split
/// out of [`route_and_dispatch`] so the router stays small. Returns the connection-close flag
/// (always `false` here; in-MULTI commands never close).
///
/// A command issued inside a transaction must be QUEUED (reply `+QUEUED`), not executed:
/// routing it remotely (the dispatch_via / multikey / whole-keyspace branches) would EXECUTE it
/// eagerly and out of transaction order, since the queue gate lives in `dispatch` on the HOME
/// path only. So EVERY in-MULTI command goes to the HOME path EXCEPT the two reject-loudly
/// cases below. The KEY INVARIANT: a transaction reaches real (home-only) EXEC ONLY when ALL its
/// watched keys AND all queued commands' keys are HOME-OWNED, so home execution is always
/// correct; otherwise it is rejected LOUDLY (correct, or explicitly aborted -- never silently
/// wrong). True cross-shard transactions (txid + ordered apply) are Stage 3. With `shards == 1`
/// every key is home-owned, so the guards never fire and this is the pre-coordinator behavior.
#[allow(clippy::too_many_arguments)]
pub(crate) fn route_in_multi(
    ctx: &ServerContext,
    conn: &mut ConnState,
    home: ShardId,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    route: route::CommandClass,
    request: &Request,
    out: &mut Vec<u8>,
) -> bool {
    let keyed = matches!(
        route,
        route::CommandClass::KeyedSingle | route::CommandClass::KeyedMulti
    );
    // CLUSTER QUEUE-TIME REDIRECT (CLUSTER_CONTRACT.md #70, slice 2). A queued data command
    // whose key(s) are not served by THIS node must honor cluster routing too, or a
    // `MULTI; SET foreign-key v; EXEC` would silently execute a non-owned write. Redis replies
    // the MOVED / CROSSSLOT error at QUEUE time AND dirties the transaction (so `EXEC` returns
    // `-EXECABORT` and applies nothing). We run the SAME `cluster_redirect` predicate as the
    // live path (one source of truth, no second key extractor), and on a redirect reply the
    // error for the queued command and mark the transaction dirty. This is checked BEFORE the
    // intra-node `all_keys_home_owned` gate: cluster ownership (which NODE) is the outer
    // question, internal-shard ownership (which of MY shards) the inner one.
    if let Some(map) = ctx.cluster.as_deref() {
        let in_sync = replica_read_in_sync(ctx);
        // HA-6: the in-MULTI QUEUE-TIME redirect honors ASKING exactly like the non-MULTI live path,
        // by building the SAME `MigrationCtx { asking, key_present }` and passing `Some(&mig)` to the
        // shared `cluster_redirect`. The `asking` is the TRANSACTION-SCOPED `conn.txn_asking` (the
        // PRE-MULTI one-shot the router carried into this transaction), NOT the per-command one-shot
        // (consumed at the top of `route_and_dispatch` and gone by the time commands queue). This
        // mirrors Redis, whose cluster redirect runs at QUEUE time with `CLIENT_ASKING` still live:
        // `ASKING; MULTI; <cmd on an IMPORTING slot>; EXEC` is QUEUED + served on the importing
        // destination, while WITHOUT ASKING the same queued command MOVEDs/dirties (the migration arm
        // is inert unless the slot is actually MIGRATING/IMPORTING, so a non-migrating slot is
        // byte-identical to before -- the static MOVED/CROSSSLOT decision). The key-presence resolver
        // reads THIS connection's accept-shard store at the current time, consulted ONLY when a slot
        // is mid-migration (the cold path).
        //
        // MULTI-SHARD note: unlike the non-MULTI live path (which pre-resolves a sibling-shard key's
        // presence via the coordinator for an EXACT ASK), the QUEUE-TIME path uses the LOCAL read and
        // does NOT hop. It does not need to: this redirect runs BEFORE the `all_keys_home_owned` gate
        // below, which REJECTS (and dirties the transaction with the cross-shard error) any queued
        // keyed command with a key on a SIBLING shard -- such a command can never EXEC correctly
        // home-only (cross-shard transactions are Stage 3), so it is aborted regardless of presence.
        // The only key the local read can mis-classify (a present sibling-shard key) is one that is
        // about to be rejected anyway; for a HOME-owned key the local read is already exact. So the
        // queue-time path is correctly conservative without a hop (and `route_in_multi` stays sync).
        let now = UnixMillis(env.borrow().now_unix_millis());
        let db = conn.db;
        let key_present = |k: &[u8]| store_rc.borrow().contains_live(db, k, now);
        let mig = MigrationCtx {
            asking: conn.txn_asking,
            key_present: &key_present,
        };
        if let Some(reply) = cluster_redirect(
            map,
            route,
            cmd_upper,
            request,
            conn.readonly,
            in_sync,
            Some(&mig),
            shard_owner_home(ctx, home),
        ) {
            state_rc.borrow_mut().counters.on_command();
            conn.dirty_exec = true;
            encode_into(out, &ironcache_server::Value::error(reply), conn.proto);
            return false;
        }
    }
    // A KEYED DATA command whose keys are not ALL home-owned is rejected at queue time (Redis's
    // queue-time-error behavior): reply the cross-shard error NOW and dirty the transaction, so
    // EXEC returns -EXECABORT and applies nothing. Bump commands_processed like the other paths.
    if keyed && !all_keys_home_owned(cmd_upper, request, home) {
        state_rc.borrow_mut().counters.on_command();
        conn.dirty_exec = true;
        encode_into(
            out,
            &ironcache_server::Value::error(
                ironcache_protocol::ErrorReply::txn_cross_shard_command(),
            ),
            conn.proto,
        );
        return false;
    }
    // A WHOLE-KEYSPACE command (KEYS/SCAN/DBSIZE/FLUSHALL/FLUSHDB/RANDOMKEY) cannot run correctly
    // home-only at EXEC when the keyspace is partitioned: EXEC replays synchronously on the HOME
    // store, so it would cover only the home shard's ~1/N (a `MULTI; FLUSHALL; EXEC` would
    // partially flush -- silent data RETENTION). There is no single owner to hop to and EXEC
    // cannot fan out (it is synchronous), so reject at queue time (dirty -> -EXECABORT), the same
    // "correct or explicitly aborted, never silently wrong" contract as the cross-shard keyed
    // case. Gate on `home.total > 1`: with one shard the home shard IS the whole keyspace, so
    // they run correctly home-only and must keep working (shards == 1 byte-identical parity).
    if matches!(route, route::CommandClass::WholeKeyspace) && home.total > 1 {
        state_rc.borrow_mut().counters.on_command();
        conn.dirty_exec = true;
        encode_into(
            out,
            &ironcache_server::Value::error(
                ironcache_protocol::ErrorReply::txn_whole_keyspace_unsupported(),
            ),
            conn.proto,
        );
        return false;
    }
    // All-home keyed command OR a control verb: HOME path. `dispatch`'s queue gate queues the
    // keyed command (`+QUEUED`) and runs EXEC/DISCARD/etc. specially. This is the ONLY routing
    // branch taken while in_multi (no remote hop, no fan-out), so a transaction that reaches real
    // EXEC has ALL queued keys home-owned -> home-only EXEC is correct.
    //
    // #531: `None` node-keyspace here -- the MULTI queue path cannot fan out (EXEC replays
    // synchronously), so an INFO queued in a transaction falls back to the serving shard's local
    // `db_len` keyspace (a documented edge, consistent with the rest of the serving-shard-scoped
    // EXEC-replay data). A bare non-transaction INFO takes the async home branch above with the
    // node-wide gather.
    handle_request(
        ctx, conn, env, store_rc, wheel_rc, state_rc, request, cmd_upper, None, out,
    )
}

/// The LIVE (non-MULTI) blocking-command handler (PROD-9): the FIRST attempt + the park
/// decision. WAIT is handled inline (it touches no keys); the pop family parses + attempts the
/// non-blocking op. Returns the connection-close flag (always `false` here -- a blocking command
/// never closes the connection) and, when the command must PARK, sets `*block_request` to the
/// [`BlockPark`] the serve loop's park loop consumes.
///
/// On the FAST path (data present, or a parse / WRONGTYPE error) it replies immediately and
/// leaves `block_request` `None`. On the PARK path it leaves `out` EMPTY and sets
/// `block_request`. The `commands_processed` counter is bumped exactly once (on the immediate
/// reply OR when the park is set up), matching every other reply path.
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_blocking_live(
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    request: &Request,
    out: &mut Vec<u8>,
    block_request: &mut Option<BlockPark>,
) -> bool {
    // WAIT numreplicas timeout (PROD-9): block until at least `numreplicas` replicas have acked,
    // or `timeout` ms elapse; reply the integer count of in-sync replicas. It touches NO keyspace,
    // so it has no pop attempt / waiter key. If the quorum is ALREADY met (or numreplicas == 0),
    // reply the current count immediately; else PARK on the replica-ack count (the serve loop polls
    // the count under the timer seam).
    if cmd_upper == b"WAIT" {
        return handle_wait_live(ctx, conn, state_rc, request, out, block_request);
    }

    // Parse + ATTEMPT the blocking pop. A parse error replies immediately (no park).
    let spec = match ironcache_server::parse_block(cmd_upper, request) {
        Ok(s) => s,
        Err(e) => {
            state_rc.borrow_mut().counters.on_command();
            encode_into(out, &ironcache_server::Value::error(e), conn.proto);
            return false;
        }
    };
    state_rc.borrow_mut().counters.on_command();
    let now = UnixMillis(env.borrow().now_unix_millis());
    let attempt = {
        let mut store = store_rc.borrow_mut();
        ironcache_server::try_block_op(&mut *store, conn.db, now, &spec)
    };
    // Data present (or a WRONGTYPE error): reply immediately. The store mutation recorded any
    // keyspace event(s); the caller (`route_and_dispatch`) drains + publishes them right after this
    // returns (via `publish_pending_keyspace_events`), so a blocking pop fires the same lpop/rpop/
    // zpopmin notification as the non-blocking pop. Every key empty/absent (`None`): PARK -- leave
    // `out` empty and set `block_request`; the serve loop runs the park loop (re-attempt on a wake,
    // or the nil-array on timeout).
    if let Some(reply) = attempt {
        encode_into(out, &reply, conn.proto);
    } else {
        *block_request = Some(BlockPark { spec, db: conn.db });
    }
    false
}

/// WAIT's LIVE handler (PROD-9): parse `numreplicas` + `timeout`, and either reply the current
/// in-sync replica count immediately (the quorum is already met, or numreplicas == 0) or PARK on
/// it. Parking is represented by a `BlockPark` with NO keys and the WAIT op carried via the spec's
/// `keys`/`op` being unused; the serve loop's WAIT park loop polls the count under the timer seam.
fn handle_wait_live(
    ctx: &ServerContext,
    conn: &mut ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out: &mut Vec<u8>,
    block_request: &mut Option<BlockPark>,
) -> bool {
    state_rc.borrow_mut().counters.on_command();
    if request.args.len() != 3 {
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity("wait")),
            conn.proto,
        );
        return false;
    }
    let (Some(numreplicas), Some(timeout_ms)) = (
        parse_wait_int(&request.args[1]),
        parse_wait_int(&request.args[2]),
    ) else {
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::not_an_integer()),
            conn.proto,
        );
        return false;
    };
    if timeout_ms < 0 {
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::err(
                "timeout is negative",
            )),
            conn.proto,
        );
        return false;
    }
    let current = ironcache_server::in_sync_replica_count(ctx);
    // The quorum is already met (or 0 requested): reply the count now, no park.
    if numreplicas <= current {
        encode_into(out, &ironcache_server::Value::Integer(current), conn.proto);
        return false;
    }
    // PARK: carry the WAIT parameters in a BlockPark. The op + keys are a WAIT marker (no keys);
    // the serve loop polls the in-sync count vs `numreplicas` under the timer.
    *block_request = Some(BlockPark {
        spec: ironcache_server::BlockSpec {
            timeout_ms: if timeout_ms == 0 {
                None
            } else {
                Some(timeout_ms as u64)
            },
            keys: Vec::new(),
            op: ironcache_server::BlockOp::Wait {
                numreplicas: numreplicas.max(0) as u64,
            },
        },
        db: conn.db,
    });
    false
}

/// Parse a WAIT integer arg (numreplicas / timeout) the strict Redis way.
fn parse_wait_int(arg: &[u8]) -> Option<i64> {
    core::str::from_utf8(arg).ok()?.parse::<i64>().ok()
}

/// The poll quantum for a WAIT park (PROD-9): how often to re-check the in-sync replica count + a
/// kill while parked, so an UNPAUSE / a newly-attached replica / a CLIENT KILL is observed
/// promptly. WAIT polls because its quorum is published by the repl tasks, not via a waiter
/// registry.
const WAIT_POLL_QUANTUM: core::time::Duration = core::time::Duration::from_millis(50);

/// The kill-poll quantum for a POP park (PROD-9 FIX2): the UPPER BOUND on a pop park's timer arm,
/// so a forever-parked (no timeout) blocked client on an idle key still reaches its loop-top
/// `is_killed()` check within ~50ms of a `CLIENT KILL` and is torn down promptly. The pop park is
/// otherwise spin-free (it parks on the waiter `Notify` for the wake); this bounded re-check exists
/// ONLY so a kill of an idle-key forever-park is not deferred until the next push / pipelined bytes
/// / peer close. `ClientHandle` is deliberately runtime-agnostic (it depends only on
/// `ironcache-env`, no tokio), so it carries no wake handle of its own; a bounded poll -- mirroring
/// WAIT's existing quantum -- is the runtime-coupling-free way to make a kill prompt. A push still
/// wakes the park immediately via the `Notify` (no added latency on the data path).
const KILL_POLL_QUANTUM: core::time::Duration = core::time::Duration::from_millis(50);

/// Run the BLOCKING PARK loop (PROD-9): park this connection until a wake (a push to a waited key
/// makes it ready), the timeout elapses, the connection is closed/killed, or (for WAIT) the
/// replica-ack quorum is met. Returns the connection-CLOSE flag (`true` to tear the connection
/// down: a peer close or an I/O error while parked).
///
/// ## Mechanism (the core of PROD-9)
///
/// POP PARK (BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP):
/// 1. Register a per-shard FIFO [`crate::blocking::Waiter`] on EVERY key (the RAII
///    [`crate::blocking::WaiterGuard`] deregisters on EVERY exit -- success, timeout, close, kill,
///    or a panic -- so a parked connection never leaks a registry entry and a push never wakes a
///    gone connection).
/// 2. `select!` on (the waiter's `Notify` wake / the runtime timer to the deadline / a stream read,
///    which detects a PEER CLOSE while parked). NO busy-wait: the wake arm parks on the `Notify`.
/// 3. On a WAKE re-attempt the pop. Success -> encode + flush the reply, drop the guard, return.
///    Still empty (another waiter raced it, or a spurious wake) -> loop and re-park on the SAME
///    `Notify` (the guard is held the whole time, so the waiter keeps its FIFO position).
/// 4. On TIMEOUT -> encode + flush the nil-array reply, drop the guard, return.
///
/// WAIT PARK: no waiter (it touches no keys); POLL the in-sync replica count vs `numreplicas` under
/// a short timer quantum until the quorum is met or the timeout elapses, then reply the count.
///
/// A KILL (CLIENT KILL flagged this connection) or a peer close ends the park early.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn run_block_park(
    stream: &mut ironcache_runtime::ClientStream,
    timer_rt: &TokioRuntime,
    ctx: &ServerContext,
    conn: &ConnState,
    client_handle: &std::sync::Arc<ironcache_observe::ClientHandle>,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    inbox: &coordinator::Inbox,
    home: ShardId,
    read_buf: &mut Vec<u8>,
    out: &mut Vec<u8>,
    park: BlockPark,
) -> bool {
    // The absolute DEADLINE (a monotonic instant), or None for "block forever". Computed ONCE from
    // the Env clock seam (ADR-0003, NOT wall clock) so a re-park after a spurious wake counts the
    // already-elapsed time toward the same deadline (the timer re-arms with the REMAINING duration).
    let start = env.borrow().now();
    let deadline: Option<ironcache_env::Monotonic> = park
        .spec
        .timeout_ms
        .map(|ms| start.saturating_add(core::time::Duration::from_millis(ms)));

    // WAIT parks on the replica-ack quorum, not a key waiter.
    if let ironcache_server::BlockOp::Wait { numreplicas } = park.spec.op {
        return wait_park(
            stream,
            timer_rt,
            ctx,
            conn,
            client_handle,
            env,
            out,
            numreplicas,
            deadline,
        )
        .await;
    }

    // POP PARK. Register a FIFO waiter on every key BEFORE the first attempt; the guard deregisters
    // on EVERY exit (RAII). Registering first is what makes the loop below an ATTEMPT-THEN-PARK
    // (register-then-recheck) rather than a park-then-attempt: see the re-attempt at the top of the
    // loop and the lost-wakeup note there.
    let registry = shard_blocking();
    let (_guard, wake) =
        crate::blocking::WaiterGuard::park(&registry, park.db, park.spec.wait_keys(), conn.id);

    // Whether to PROBE the store this iteration. Set on the FIRST iteration (the register-then-
    // recheck) and after a WAKE / pipelined BYTES (a push may have made a key ready). NOT set after a
    // bare kill-poll timer tick: a periodic tick must only re-check `is_killed()`, NEVER re-probe the
    // store -- otherwise a NON-front waiter could grab a pushed element off its own poll tick,
    // breaking FIFO fairness. The pop is driven by the WAKE path (FIFO: a push wakes only the FRONT
    // waiter); the poll exists solely for prompt kill detection on a forever-park.
    let mut attempt_pop = true;

    loop {
        // A kill observed between iterations ends the park (the reply is abandoned; the connection
        // is torn down). Cold relaxed load. With the bounded KILL_POLL_QUANTUM arm in the select!
        // below, a forever-park (no timeout) on an idle key still reaches this check within ~50ms
        // of a `CLIENT KILL`, so a killed blocked client is torn down promptly (PROD-9 FIX2) rather
        // than only on the next push / pipelined bytes / peer close.
        if client_handle.is_killed() {
            return true;
        }

        // RE-ATTEMPT THE POP (register-then-recheck, PROD-9 FIX1), but ONLY when `attempt_pop` is set
        // (the first iteration, or after a wake / pipelined bytes -- never on a bare kill-poll tick).
        // The waiter is ALREADY registered (above, on the first iteration; the same guard is held on
        // every later iteration), so the FIRST-iteration probe closes the LOST-WAKEUP window: when
        // this blocking command is pipelined behind a reply-producing command, the serve loop FLUSHES
        // that earlier reply (an `await` that can yield) BEFORE calling this function. A concurrent
        // push during that pre-registration flush would have called `wake_one` and found NO waiter
        // (ours not yet registered) -> woken nobody. Because the waiter is registered before this
        // recheck, that push's element is observed HERE on the first iteration instead of being lost
        // until timeout. The recheck also covers the cross-shard wake path (a sibling-shard push that
        // ran here through `run_remote`) and any number of awaits that preceded registration. Cost:
        // one extra store probe per park (a COLD path, never the hot per-command path). On a WAKE this
        // is the same re-attempt the prior park-then-attempt loop did, so the keep-FIFO-position
        // behavior is unchanged (the guard is held the whole time).
        if attempt_pop {
            let now = UnixMillis(env.borrow().now_unix_millis());
            let attempt = {
                let mut store = store_rc.borrow_mut();
                ironcache_server::try_block_op(&mut *store, park.db, now, &park.spec)
            };
            if let Some(reply) = attempt {
                // A successful blocked pop fires the SAME lpop/rpop/zpopmin keyspace event as a
                // non-blocking pop; publish it AFTER the reply is flushed (per-connection FIFO).
                let closed = flush_block_reply(stream, out, conn.proto, reply).await;
                publish_pending_keyspace_events(inbox, home.index);
                return closed;
            }
        }

        // Compute the REMAINING time to the deadline; if already past, reply the nil-array (timeout).
        // This is evaluated EVERY iteration (including after a kill-poll tick) so a real timeout still
        // fires even though the poll tick itself does not re-probe the store.
        let remaining: Option<core::time::Duration> = match deadline {
            None => None,
            Some(dl) => {
                let now = env.borrow().now();
                if now >= dl {
                    // Timed out: reply the nil-array and finish.
                    return flush_block_reply(stream, out, conn.proto, block_timeout_value()).await;
                }
                Some(dl.saturating_duration_since(now))
            }
        };

        // PARK: select on the wake, the timer, and a stream read (peer-close detection). The read is
        // into a FRESH buffer and APPENDED to `read_buf` so a partial frame already in `read_buf`
        // survives a cancelled read (the same pattern the idle wait uses). NO RefCell borrow is held
        // across the await.
        //
        // The timer duration is the remaining time CAPPED at KILL_POLL_QUANTUM (for a finite
        // deadline) or exactly KILL_POLL_QUANTUM (for a forever-park), so a killed forever-parked
        // client notices within ~50ms (mirrors the WAIT poll's bounded quantum). The timer arm fires
        // either at the real deadline (-> the next iteration's remaining-time check replies the
        // nil-array) OR at the bounded poll quantum before the deadline (-> loop, re-check
        // `is_killed()` ONLY, no store probe). The two are distinguished by re-reading the clock at
        // the top of the loop.
        let timer_dur = match remaining {
            Some(dur) => dur.min(KILL_POLL_QUANTUM),
            None => KILL_POLL_QUANTUM,
        };
        let woken = tokio::select! {
            () = wake.notified() => WakeOutcome::Wake,
            () = timer_rt.timer(timer_dur) => WakeOutcome::Timer,
            res = stream.recv(Vec::new()) => match res {
                Ok(r) if r.n == 0 => return true, // peer closed while parked
                Ok(r) => {
                    read_buf.extend_from_slice(&r.buf[..r.n]);
                    // #527: net input for pipelined bytes read while parked on a blocking command.
                    shard_state().borrow().counters.on_net_input(r.n as u64);
                    WakeOutcome::Bytes
                }
                Err(_) => return true,
            },
        };

        // Decide whether the NEXT iteration probes the store. A WAKE (a push woke THIS front waiter)
        // or pipelined BYTES drive a re-attempt; a bare kill-poll TIMER tick does NOT (it only loops
        // to re-check `is_killed()` + the deadline, preserving FIFO -- only the woken front waiter
        // races for the element).
        attempt_pop = matches!(woken, WakeOutcome::Wake | WakeOutcome::Bytes);
    }
}

/// The outcome of a single park `select!` (PROD-9): which arm fired.
enum WakeOutcome {
    /// The waiter `Notify` fired (a push to a waited key): re-attempt the pop.
    Wake,
    /// The park timer elapsed: either the real deadline (the next loop iteration replies the
    /// nil-array once it confirms the deadline is past) OR a bounded kill-poll tick (the next
    /// iteration re-checks `is_killed()` ONLY -- it does NOT re-probe the store, so a poll tick
    /// never lets a non-front waiter steal a pushed element, preserving FIFO fairness). The two are
    /// distinguished by re-reading the clock at the top of the loop.
    Timer,
    /// New bytes arrived while parked (a pipelined command): re-attempt (harmless) and keep the
    /// bytes in `read_buf` for the decode loop to process after the park ends.
    Bytes,
}

/// The WAIT park (PROD-9): poll the in-sync replica count vs `numreplicas` under a short timer
/// quantum until the quorum is met or the deadline elapses, then reply the CURRENT count. A peer
/// close or a kill ends it early. WAIT touches no keys, so there is no waiter registry entry; the
/// quorum is published by the repl tasks (a relaxed atomic load), so a poll is the right model.
#[allow(clippy::too_many_arguments)]
async fn wait_park(
    stream: &mut ironcache_runtime::ClientStream,
    timer_rt: &TokioRuntime,
    ctx: &ServerContext,
    conn: &ConnState,
    client_handle: &std::sync::Arc<ironcache_observe::ClientHandle>,
    env: &Rc<RefCell<SystemEnv>>,
    out: &mut Vec<u8>,
    numreplicas: u64,
    deadline: Option<ironcache_env::Monotonic>,
) -> bool {
    loop {
        if client_handle.is_killed() {
            return true;
        }
        let current = ironcache_server::in_sync_replica_count(ctx);
        // Quorum met: reply the count.
        if current >= 0 && (current as u64) >= numreplicas {
            return flush_block_reply(
                stream,
                out,
                conn.proto,
                ironcache_server::Value::Integer(current),
            )
            .await;
        }
        // Remaining time to the deadline; if past, reply the current count (Redis: WAIT returns the
        // count it achieved on timeout, typically below `numreplicas`).
        let wait = match deadline {
            None => WAIT_POLL_QUANTUM,
            Some(dl) => {
                let now = env.borrow().now();
                if now >= dl {
                    return flush_block_reply(
                        stream,
                        out,
                        conn.proto,
                        ironcache_server::Value::Integer(current),
                    )
                    .await;
                }
                dl.saturating_duration_since(now).min(WAIT_POLL_QUANTUM)
            }
        };
        // Race a short poll quantum against a peer close (so a disconnect ends the wait promptly).
        tokio::select! {
            () = timer_rt.timer(wait) => {}
            res = stream.recv(Vec::new()) => {
                match res {
                    Ok(r) if r.n == 0 => return true, // peer closed
                    // Bytes while parked in WAIT: Redis would not process a new command until WAIT
                    // returns; we drop them (a rare edge -- a client pipelining behind WAIT). The
                    // poll loop continues. (Buffering them safely is a documented follow-up.) They
                    // WERE read off the socket, so #527 still counts them as net input.
                    Ok(r) => shard_state().borrow().counters.on_net_input(r.n as u64),
                    Err(_) => return true,
                }
            }
        }
    }
}

/// Encode `reply` into a FRESH `out` and flush it over the stream, returning the connection-CLOSE
/// flag (`true` on an I/O error). `out` is cleared first (any pipelined replies were already
/// flushed before the park), so this writes exactly the blocking command's reply.
async fn flush_block_reply(
    stream: &mut ironcache_runtime::ClientStream,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
    reply: ironcache_server::Value,
) -> bool {
    out.clear();
    encode_into(out, &reply, proto);
    let sent = out.len();
    match stream.send(std::mem::take(out)).await {
        Ok(returned) => {
            *out = returned;
            // #527: net output for a blocking command's reply (BLPOP/WAIT/... timeout or result).
            shard_state().borrow().counters.on_net_output(sent as u64);
            false
        }
        Err(_) => true,
    }
}

/// The nil-array a blocking pop replies on timeout (Redis NULL ARRAY: RESP2 `*-1`, RESP3 `_`).
pub(crate) fn block_timeout_value() -> ironcache_server::Value {
    ironcache_server::block_timeout_reply()
}
