// SPDX-License-Identifier: MIT OR Apache-2.0
//! Transaction command handling split out of `dispatch.rs` (#625): WATCH / UNWATCH (per-key
//! dirty-CAS registration) and the EXEC transaction runner. Behavior-preserving relocation: the
//! bodies are byte-identical to their former in-`dispatch.rs` definitions.

use super::{ServerContext, acl_enforce, acl_resolve_if_stale, ascii_upper, dispatch_inner};
use crate::conn::ConnState;
use crate::{CmdStatsFn, KeyspaceFn, RollupFn};
use ironcache_env::Env;
use ironcache_expiry::TimingWheel;
use ironcache_observe::{CounterDeltas, MemoryInfo};
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{ActiveExpiry, Admit, Keyspace, PolicySwap, Store, UnixMillis, Watch};

/// `WATCH key [key ...]` (TRANSACTIONS.md per-key dirty-CAS, PR-10b). Marks each key
/// watched on the connection's accept shard: per key, [`Watch::watch_snapshot`] records
/// the key's current version + present/absent status into a [`WatchEntry`] pushed onto
/// `state.watch`. Replies `+OK`.
///
/// A WELL-FORMED WATCH inside MULTI is rejected with `-ERR WATCH inside MULTI is not
/// allowed` WITHOUT dirtying the transaction (the txn stays open + clean, so a following
/// EXEC still runs): the queue gate excludes WATCH from queueing, and this arm returns the
/// error when `in_multi`. WATCH is arity -2 (the command token + at least one key); the
/// queue-gate arity table holds the same value, but WATCH outside MULTI is validated here
/// too (the gate only runs inside MULTI), so check it explicitly. The arity check runs
/// BEFORE the in-MULTI rejection (matching the MULTI/DISCARD/EXEC arms and Redis's
/// pre-queue commandCheckArity -> flagTransaction), so a MALFORMED WATCH (no keys) inside
/// MULTI DIRTIES the transaction (`dirty_exec`), and a later clean EXEC returns EXECABORT.
///
/// SINGLE-SHARD-PER-CONNECTION (PR-10b scope): every watched key is registered on this
/// connection's accept shard `store`; a watched key on a different shard is out of scope
/// (cross-shard EXEC, COORDINATOR.md #29).
pub(crate) fn cmd_watch<S: Store + Watch>(
    state: &mut ConnState,
    store: &mut S,
    now: UnixMillis,
    req: &Request,
) -> Value {
    // Arity -2: command token + >= 1 key. Checked FIRST (matching the MULTI/DISCARD/EXEC
    // arms): Redis runs commandCheckArity -> flagTransaction at the pre-queue arity check,
    // so a malformed WATCH issued INSIDE a transaction DIRTIES it (a later clean EXEC then
    // returns EXECABORT). Only AFTER arity passes does the WATCH-inside-MULTI rejection
    // apply (a well-formed WATCH inside MULTI is the legal-but-disallowed case that leaves
    // the txn OPEN + CLEAN).
    if req.args.len() < 2 {
        if state.in_multi {
            state.dirty_exec = true;
        }
        return Value::error(ErrorReply::wrong_arity("watch"));
    }
    // WATCH inside MULTI: error, txn left OPEN + CLEAN (no dirty_exec).
    if state.in_multi {
        return Value::error(ErrorReply::watch_inside_multi());
    }
    let db = state.db;
    for key in &req.args[1..] {
        let entry = store.watch_snapshot(db, key, now);
        state.watch.push(entry);
    }
    Value::ok()
}

/// `UNWATCH` (TRANSACTIONS.md, PR-10b). Flushes the connection's watch set: deregister
/// every snapshot from the store ([`Watch::unwatch`]), then clear the connection-side
/// list. Always replies `+OK` (UNWATCH never errors; an empty watch set is a clean
/// no-op). Arity is exactly 1.
///
/// UNWATCH is a NORMAL command (NOT control-flow): inside MULTI it QUEUES like any other
/// and is REPLAYED at EXEC, where it runs as a no-op-ish (the dirty-CAS already cleared
/// the watches at EXEC entry, so `state.watch` is already empty). Outside MULTI it runs
/// here directly. Either way it just clears whatever watch set is present.
pub(crate) fn cmd_unwatch<S: Watch>(state: &mut ConnState, store: &mut S, req: &Request) -> Value {
    if req.args.len() != 1 {
        return Value::error(ErrorReply::wrong_arity("unwatch"));
    }
    store.unwatch(&state.watch);
    state.clear_watch();
    Value::ok()
}

/// Run `EXEC` (TRANSACTIONS.md "queue then apply", PR-10a). Decides the three Redis
/// outcomes and, on the apply path, REPLAYS each queued command against the SAME
/// already-borrowed store/wheel/env by calling [`dispatch_inner`] per command (the
/// re-entrancy the dispatch split exists for):
/// - NOT in a transaction -> `-ERR EXEC without MULTI`;
/// - in a transaction but DIRTIED (a queue-time error) -> `-EXECABORT ...`, drop the
///   queue, exit MULTI, apply nothing;
/// - otherwise -> run every queued command in order, collect each reply into an array
///   element, and return the array (an empty MULTI;EXEC is an empty array `*0`). There
///   is NO rollback: a per-command runtime error (WRONGTYPE, not-an-integer, `-OOM`
///   over budget) is an Error element and the batch continues.
///
/// Determinism (ADR-0003): the whole batch reuses the SINGLE `now` the serve loop read
/// once for this EXEC command, so a replay reaps/expires identically; no per-queued-
/// command clock read. Exiting the transaction clears the queue in all three branches.
#[allow(clippy::too_many_arguments)]
pub(crate) fn exec_transaction<
    E: Env,
    S: Store + Admit + ActiveExpiry + Keyspace + PolicySwap + Watch,
>(
    ctx: &ServerContext,
    state: &mut ConnState,
    env: &mut E,
    store: &mut S,
    wheel: &mut TimingWheel,
    now: UnixMillis,
    rollup: RollupFn<'_>,
    cmdstats: CmdStatsFn<'_>,
    keyspace: KeyspaceFn<'_>,
    mem: MemoryInfo,
    deltas: &mut CounterDeltas,
    req: &Request,
) -> Value {
    if req.args.len() != 1 {
        // A wrong-arity EXEC issued INSIDE a transaction DIRTIES it (Redis runs
        // commandCheckArity before the MULTI queue block, so an arity failure on the EXEC
        // verb itself calls flagTransaction -> CLIENT_DIRTY_EXEC). The txn stays OPEN +
        // dirty, so a later clean EXEC returns EXECABORT. When NOT in_multi this is just a
        // plain wrong-arity error (and a clean EXEC then returns EXEC-without-MULTI).
        if state.in_multi {
            state.dirty_exec = true;
        }
        return Value::error(ErrorReply::wrong_arity("exec"));
    }
    if !state.in_multi {
        return Value::error(ErrorReply::exec_without_multi());
    }
    if state.dirty_exec {
        // A queue-time error dirtied the batch: refuse the whole thing and apply
        // nothing (clear_txn drops the queue + exits MULTI). EXEC clears the WATCH set on
        // EVERY exit path (TRANSACTIONS.md, PR-10b), the EXECABORT path included:
        // deregister the watches from the store, then clear the connection-side list.
        store.unwatch(&state.watch);
        state.clear_watch();
        state.clear_txn();
        return Value::error(ErrorReply::exec_abort());
    }
    // WATCH dirty-CAS check (TRANSACTIONS.md per-key dirty-CAS, PR-10b), BEFORE running
    // the batch. If ANY watched key was modified between WATCH and now (its version moved,
    // or its present/absent status changed), the optimistic lock failed: EXEC ABORTS,
    // returning a NULL ARRAY (`Value::Array(None)`, which the encoder renders as RESP2
    // `*-1` / RESP3 `_`) and applying NOTHING. NOTE this is the null ARRAY, not the null
    // bulk (`Value::Null` -> RESP2 `$-1`): Redis's abort reply is `addReply(c,
    // shared.nullarray[...])` (src/multi.c execCommand). The watches are deregistered +
    // cleared (EXEC always clears watches) and the transaction exits. The check is
    // O(watched keys), each a version compare + a present/absent probe.
    if state.watch.iter().any(|e| store.watch_is_dirty(e, now)) {
        store.unwatch(&state.watch);
        state.clear_watch();
        state.clear_txn();
        return Value::Array(None);
    }
    // The CAS passed: the watches have served their purpose. Deregister + clear them
    // BEFORE running the batch (EXEC clears the watch set on the run path too), so the
    // batch's own writes do not re-trigger a watch and a queued UNWATCH at EXEC is a
    // clean no-op against an already-empty set.
    store.unwatch(&state.watch);
    state.clear_watch();
    // Take the queue OUT of `state` so we can pass `&mut state` to `dispatch_inner` per
    // command without aliasing `state.queued`. Exit the transaction NOW (before running)
    // so a queued RESET/MULTI/etc. sees a clean post-EXEC connection state, matching
    // Redis (EXEC ends the transaction, then runs the batch).
    let queued = std::mem::take(&mut state.queued);
    state.clear_txn();
    // F1 (live revocation, EXEC sub-case): bring the connection's cached ACL identity up to date
    // BEFORE replaying the batch, then RE-CHECK ACL per queued command below. This closes the race
    // where an EXTERNAL `ACL SETUSER` / `DELUSER` revokes a permission BETWEEN `MULTI` and `EXEC`:
    // the commands were queued under the OLD permissions, but Redis re-checks ACL at EXEC time
    // (per queued command), so a now-forbidden command must be denied at replay. `deauthed` is
    // `true` when the connection's user was DELUSER'd mid-transaction: every queued command is then
    // denied (the identity is gone). The HOT PATH (no mutation since MULTI opened) is the same one
    // relaxed load + compare as the router, so a transaction that ran with no concurrent ACL change
    // pays nothing here.
    let deauthed = !acl_resolve_if_stale(ctx, state);
    let mut replies = Vec::with_capacity(queued.len());
    for q in &queued {
        // Re-derive the uppercased token per queued command (cheap; the request was
        // validated at queue time). Reuse the SAME borrowed store/wheel/env + `now`:
        // no re-borrow of the thread-locals, no double-borrow. Counter deltas
        // ACCUMULATE across the batch (eviction / keyspace hits-misses use `+=`).
        let qcmd = ascii_upper(q.command());
        // F1: per-queued-command ACL re-check at EXEC. When the user was DELUSER'd mid-txn, deny
        // every command (NOPERM on the now-nonexistent identity); otherwise run the same
        // `acl_enforce` the router runs for a live command, so a permission revoked between MULTI
        // and EXEC denies the affected command (the rest of the batch still runs, Redis parity).
        let reply = if deauthed {
            let cmd_lc = String::from_utf8_lossy(&qcmd).to_ascii_lowercase();
            Value::error(ErrorReply::noperm_command(&state.acl_user_name, &cmd_lc))
        } else if let Some(deny) =
            acl_enforce(ctx.acl.is_acl_active(), state.acl_user.as_deref(), &qcmd, q)
        {
            Value::error(deny)
        } else {
            dispatch_inner(
                ctx, state, env, store, wheel, now, rollup, cmdstats, keyspace, mem, deltas, q,
                &qcmd,
            )
        };
        replies.push(reply);
    }
    Value::Array(Some(replies))
}
