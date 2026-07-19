// SPDX-License-Identifier: MIT OR Apache-2.0
//! Routing/dispatch CLASSIFICATION predicates split out of `serve.rs` (#625): the small, pure
//! command-shape gates the serve router consults to pick a route (home-owned vs hop, serve-layer
//! pub/sub, the multi-key + spanning-combine fan-out families, the spanning-MOVE reject), plus the
//! two internal-verb / spanning-MOVE reject encoders. Leaf helpers with no serve state beyond the
//! reply-encoder shim. Behavior-preserving relocation: the bodies are byte-identical.

use super::{ShardState, encode_into};
use ironcache_runtime::bootstrap::ShardId;
use ironcache_server::{ConnState, Request, route};
use std::cell::RefCell;
use std::rc::Rc;

/// Whether EVERY routing key of a KEYED data command (`KeyedSingle`/`KeyedMulti`) is owned
/// by the HOME shard (COORDINATOR.md #107, the in-MULTI cross-shard guard). Used inside a
/// transaction to decide whether a queued command is safe to run home-only at EXEC: only a
/// command whose keys are ALL home-owned may queue (and later EXEC correctly home-only); any
/// key on a remote shard means home-only EXEC would silently lose the write, so the caller
/// rejects + dirties the transaction instead.
///
/// It reuses the SAME key-extraction the router uses ([`route::single_key`] for the single-key
/// fast path, [`route::command_keys`] for multi-key), so "which bytes are keys" cannot drift
/// from the routing decision. A command with NO extractable key (a malformed / short request,
/// `KeySpec::None`) has no remote key, so it is treated as home-owned: it queues and the home
/// handler emits the proper runtime error as the EXEC array element (matching Redis, where a
/// queued command's argument error surfaces at run time, not queue time).
pub(crate) fn all_keys_home_owned(cmd_upper: &[u8], request: &Request, home: ShardId) -> bool {
    let is_home = |key: &[u8]| route::owner_shard(key, home.total) == home.index;
    match route::classify(cmd_upper) {
        route::CommandClass::KeyedSingle => route::single_key(request).is_none_or(is_home),
        route::CommandClass::KeyedMulti => match route::command_keys(cmd_upper, request) {
            route::KeySpec::None => true,
            route::KeySpec::One(k) => is_home(k),
            route::KeySpec::Many(keys) => keys.iter().all(|k| is_home(k)),
        },
        // Only keyed commands reach this helper (the caller gates on `keyed`); a control /
        // whole-keyspace command has no owned key, so treat it as home (it never routes
        // remotely from inside MULTI anyway).
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => true,
    }
}

/// Whether `cmd_upper` is one of the SERVE-LAYER pub/sub commands intercepted by
/// [`try_handle_pubsub`] (SERVER_PUSH.md #20): SUBSCRIBE / UNSUBSCRIBE / PSUBSCRIBE /
/// PUNSUBSCRIBE / PUBLISH / PUBSUB. These are handled in the serve layer (not `dispatch_inner`),
/// so EXEC cannot replay them; the in-MULTI reject (FIX C) uses this to decide which commands to
/// reject + dirty inside a transaction. PING is NOT in this set (a subscribed PING is handled by
/// `try_handle_pubsub` but PING is a normal command that DOES reach `dispatch_inner`, so it
/// queues + replays at EXEC like any other command).
pub(crate) fn is_serve_pubsub_command(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"SUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"PUBLISH"
            | b"PUBSUB"
            // Sharded Pub/Sub (#410): SSUBSCRIBE / SUNSUBSCRIBE / SPUBLISH are serve-routed too.
            | b"SSUBSCRIBE"
            | b"SUNSUBSCRIBE"
            | b"SPUBLISH"
    )
}

/// Whether `cmd_upper` is one of the SIX multi-key DATA commands the coordinator fans out
/// across shards when its keys SPAN shards (COORDINATOR.md #107, Stage 2a): MGET, MSET,
/// DEL, EXISTS, UNLINK, TOUCH. Every OTHER spanning multi-key command (SINTER*/SUNION*/
/// SDIFF*/ZUNION*/ZINTER*/ZDIFF*/BITOP/PFCOUNT/PFMERGE spanning, RENAME/RENAMENX/COPY/MOVE/
/// SMOVE/LMOVE/RPOPLPUSH) is DEFERRED (Stage 2b/2c) and stays on the home sync fall-through;
/// MSETNX is DEFERRED to Stage 3 (it needs cross-shard atomicity), so it is NOT here. This
/// list is the single gate the serve loop and [`crate::multikey::fan_out_multikey`]'s match
/// agree on.
pub(crate) fn is_fan_out_multikey(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"MGET" | b"MSET" | b"DEL" | b"EXISTS" | b"UNLINK" | b"TOUCH"
    )
}

/// Reply the standard unknown-command error for a CLIENT-issued INTERNAL verb (the
/// coordinator's `__ICSTORESET`, COORDINATOR.md #107 Stage 2b). The verb is in the command
/// registry so the coordinator's internal path can dispatch it, but a client must never reach
/// it: this renders the SAME `-ERR unknown command ...` reply the dispatch `_ =>` arm renders
/// for a genuinely unknown token (name + leading args, single-quoted), and bumps
/// commands_processed like every other reply path so the rejection still counts.
pub(crate) fn reject_internal_verb(
    conn: &ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    state_rc.borrow_mut().counters.on_command();
    let name = String::from_utf8_lossy(request.command()).into_owned();
    let rest: Vec<&[u8]> = request.args[1..].iter().map(bytes::Bytes::as_ref).collect();
    encode_into(
        out,
        &ironcache_server::Value::error(ironcache_protocol::ErrorReply::unknown_command(
            &name, &rest,
        )),
        conn.proto,
    );
}

/// Whether `cmd_upper` is one of the set-algebra OR sorted-set-algebra commands the
/// coordinator GATHERS + (shared) COMBINES + STOREs across shards when its keys SPAN shards
/// (COORDINATOR.md #107, Stage 2b-1 + 2b-2). This is the single gate the serve loop uses to
/// route to the spanning-combine path; [`is_fan_out_spanning_zset`] then splits the zset
/// subset (dispatched to [`crate::spanning_combine::fan_out_zset`]) from the set subset
/// (dispatched to [`crate::spanning_combine::fan_out_set`]).
///
/// Set forms (Stage 2b-1): SINTER, SUNION, SDIFF, SINTERCARD (read) + SINTERSTORE,
/// SUNIONSTORE, SDIFFSTORE (store). Zset forms (Stage 2b-2): ZUNION, ZINTER, ZDIFF,
/// ZINTERCARD (read) + ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE (store) + ZRANGESTORE (a 2-key
/// copy-range). BITOP + HyperLogLog forms (Stage 2b-3): BITOP (write), PFCOUNT (read),
/// PFMERGE (write). Every OTHER spanning multi-key command (RENAME/COPY/MOVE/SMOVE/LMOVE/
/// RPOPLPUSH) stays on the home sync fall-through (deferred). The command set is DISJOINT
/// from [`is_fan_out_multikey`]'s, so the fan-out branches are mutually exclusive.
pub(crate) fn is_fan_out_spanning_combine(cmd_upper: &[u8]) -> bool {
    is_fan_out_spanning_zset(cmd_upper)
        || matches!(
            cmd_upper,
            b"SINTER"
                | b"SUNION"
                | b"SDIFF"
                | b"SINTERCARD"
                | b"SINTERSTORE"
                | b"SUNIONSTORE"
                | b"SDIFFSTORE"
                | b"BITOP"
                | b"PFCOUNT"
                | b"PFMERGE"
        )
}

/// Whether `cmd_upper` is one of the EIGHT sorted-set-algebra commands the coordinator gathers,
/// (shared) combines, and stores across shards (COORDINATOR.md #107, Stage 2b-2). The read
/// forms are ZUNION, ZINTER, ZDIFF, ZINTERCARD; the store forms are ZUNIONSTORE, ZINTERSTORE,
/// ZDIFFSTORE, and ZRANGESTORE (a 2-key copy-range). This splits the zset subset of
/// [`is_fan_out_spanning_combine`] so the serve loop dispatches it to
/// [`crate::spanning_combine::fan_out_zset`] (the set subset goes to `fan_out_set`).
pub(crate) fn is_fan_out_spanning_zset(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"ZUNION"
            | b"ZINTER"
            | b"ZDIFF"
            | b"ZINTERCARD"
            | b"ZUNIONSTORE"
            | b"ZINTERSTORE"
            | b"ZDIFFSTORE"
            | b"ZRANGESTORE"
    )
}

/// Whether `cmd_upper` is one of the THREE element-move commands the coordinator applies
/// ATOMICALLY across the two owner shards when its keys span shards (COORDINATOR.md #107, the
/// PROD-9 cross-shard atomicity slice): SMOVE (set member move), LMOVE / RPOPLPUSH (list
/// element move). The serve loop dispatches a spanning invocation of these to
/// [`crate::spanning_move::fan_out_spanning_move`] (the gather-validate-then-commit), ending
/// the prior SILENT home-subset partial-apply. A co-located invocation routes via Stage 1
/// (the single-shard handler), unchanged. The set is DISJOINT from [`is_fan_out_multikey`] /
/// [`is_fan_out_spanning_combine`] / [`is_spanning_move_reject`], so the branches are mutually
/// exclusive.
pub(crate) fn is_fan_out_spanning_move(cmd_upper: &[u8]) -> bool {
    matches!(cmd_upper, b"SMOVE" | b"LMOVE" | b"RPOPLPUSH")
}

/// Whether `cmd_upper` is a spanning multi-key command this slice REJECTS LOUDLY (rather than
/// silently home-subset partial-apply) when its keys span internal shards (COORDINATOR.md
/// #107). These need more than a two-hop element move: RENAME / RENAMENX / COPY transfer an
/// ARBITRARY-typed value object intact (no cross-shard serialize/restore primitive exists
/// yet -- `Keyspace::move_object` is same-shard only by design); LMPOP / ZMPOP are
/// first-non-empty multi-key pops; SORT ... STORE writes a sorted projection. A spanning
/// invocation is rejected with a clear, descriptive error naming the co-location (hash-tag)
/// remedy (see [`reject_spanning_move`]), the SAME "correct, or explicitly aborted, never
/// silently wrong" contract the cross-shard MULTI/EXEC + WATCH guards follow. NOTE: SORT is
/// only rejected when it carries a STORE dest on a DIFFERENT owner than the source (the gate
/// caller checks `owner_shard_set == None`, which a SORT without STORE -- one key -- never
/// triggers). The set is DISJOINT from the fan-out predicates.
///
/// MSETEX (#412) joins this set: it is an ATOMIC all-or-nothing multi-key set (the NX/XX gate
/// must see every key before any write, and a shared TTL applies to all), so a naive per-shard
/// fan-out (like MSET) would break the gate's atomicity. A spanning MSETEX is therefore
/// rejected loudly rather than partial-applied; the cross-shard atomic MSETEX (the gather-then
/// -conditional-fan-out that `spanning_msetnx` does for MSETNX) is a deferred follow-up. On a
/// single-node deployment (every key home-owned) MSETEX never spans, so this never fires there.
pub(crate) fn is_spanning_move_reject(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"RENAME" | b"RENAMENX" | b"COPY" | b"LMPOP" | b"ZMPOP" | b"SORT" | b"MSETEX"
    )
}

/// REJECT a SHARD-SPANNING invocation of a multi-key command this slice cannot apply
/// atomically (RENAME/RENAMENX/COPY/LMPOP/ZMPOP/SORT...STORE), encoding a clear LOUD error
/// rather than letting it fall through to the home shard and SILENTLY operate on only the
/// home subset (the cardinal safety bug). Bumps `commands_processed` like every reply path.
/// The error names the co-location (hash-tag) remedy so a client can make the command
/// single-shard. This is a plain `ERR` (not `-CROSSSLOT`): IronCache presents as a SINGLE
/// NODE, matching the existing cross-shard MULTI/EXEC + WATCH guards' deliberate choice
/// ([`ironcache_protocol::ErrorReply::txn_cross_shard_command`] et al). With `shards == 1`
/// every key is home-owned, so this never fires (byte-identical single-shard parity).
pub(crate) fn reject_spanning_move(
    conn: &ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    out: &mut Vec<u8>,
) {
    state_rc.borrow_mut().counters.on_command();
    let name = String::from_utf8_lossy(cmd_upper).into_owned();
    encode_into(
        out,
        &ironcache_server::Value::error(ironcache_protocol::ErrorReply::err(format!(
            "{name} across internal shards is not supported yet; \
             use a hash tag so the keys co-locate on one shard \
             (e.g. {{tag}}key1 {{tag}}key2)"
        ))),
        conn.proto,
    );
}
