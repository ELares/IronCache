// SPDX-License-Identifier: MIT OR Apache-2.0
//! Home-core ATOMIC apply for the SHARD-SPANNING SRC->DST move commands and the spanning
//! all-or-nothing MSETNX (COORDINATOR.md #107, the PROD-9 cross-shard atomicity slice).
//!
//! ## The bug this closes (a silent partial-apply)
//!
//! On a multi-shard node (the default; shards == cores) a 2-key src/dst command whose two
//! keys hash to DIFFERENT internal shards used to FALL THROUGH to the HOME shard's
//! `handle_request`, which operates on ONLY the home shard's partition. So a spanning
//! `RENAME src dst` whose `src` lived on a sibling shard saw `src` as ABSENT and replied
//! `-ERR no such key` (or, if `dst` was the foreign key, wrote `dst` onto the WRONG shard,
//! which a later `GET dst` -- routed to dst's REAL owner -- would never find: a SILENT lost
//! write). `SMOVE` / `LMOVE` / `RPOPLPUSH` across shards likewise touched only the
//! home-owned key. A spanning `MSETNX` checked + set ONLY the home subset and MISREPORTED
//! its 1/0. Every one of these is a SILENT partial-apply -- the cardinal safety bug.
//!
//! ## What this module does (safety first, then correctness)
//!
//! IronCache presents as a SINGLE NODE, so a spanning move is TRANSPARENT (it must NOT
//! reject with `-CROSSSLOT` where the single-shard form would succeed). For the commands
//! whose decomposition into per-owner primitive sub-ops is clean, this module applies the
//! move ATOMICALLY across the two owner shards via a GATHER + VALIDATE-then-COMMIT, so the
//! result is all-or-nothing and matches single-node Redis:
//!
//! - **SMOVE src dst member** (element move): probe `src` (a missing/empty src or a member
//!   not in src -> `:0` with NO write; a non-set src or non-set dst -> WRONGTYPE with NO
//!   write, source-first like Redis). On a present member, COMMIT `SADD dst member` FIRST,
//!   then `SREM src member`: the add-before-remove order means the member is never absent
//!   from BOTH sets at any instant (no element loss), and both commit unconditionally.
//! - **LMOVE / RPOPLPUSH src dst** (element move, source-first): pop one element from
//!   `src`'s end (this also WRONGTYPE-checks src; a missing/empty src -> nil WITHOUT
//!   touching dst). Then type-check dst: a non-list dst RESTORES the popped element to src
//!   and replies WRONGTYPE (the move is a no-op). Else PUSH the element to dst's end and
//!   reply the element. The element is held on the home core between the pop and the push,
//!   so it can never be lost.
//! - **MSETNX k v [k v ...]** (strided, all-or-nothing): EXISTS-probe EVERY key on its
//!   owner shard FIRST; if ANY exists, reply `:0` and write NOTHING (Redis's
//!   "abort-before-any-write" scan). Else fan a per-owner `MSET` of that shard's pairs out
//!   to every owner and reply `:1`. Because the existence scan completes BEFORE any write,
//!   the command is all-or-nothing on the no-conflict path.
//!
//! ## Deterministic, deadlock-free ordering
//!
//! The home core drives the whole sequence; each sub-op is a SINGLE cross-shard hop
//! ([`coordinator::dispatch_one_value`], or a synchronous [`coordinator::run_local_keyed`]
//! for a home-owned key) that the owner shard's drain loop answers WITHOUT re-entering the
//! home core. A sub-op's `RefCell` borrow is taken + released inside the synchronous owner
//! call (the no-borrow-across-await contract). The home core awaits AT MOST ONE outstanding
//! hop at a time (the sub-ops run strictly in sequence), so there is no cycle of two
//! coordinators each awaiting the other -- the same single-key hop mechanism Stage 1 routing
//! already proved deadlock-free. When two keys SHARE an owner shard, the home visits that
//! shard once per sub-op (still sequential), never holding two borrows of it at once.
//!
//! ## What is FAIL-LOUD (honest)
//!
//! RENAME / RENAMENX / COPY move an ARBITRARY-typed value object intact (encoding + TTL).
//! Transferring a whole value object between shards needs a serialize/restore primitive the
//! engine does not expose yet (no DUMP/RESTORE; `Keyspace::move_object` is same-shard only
//! by design). LMPOP / ZMPOP (first-non-empty multi-key pop with COUNT) and SORT ... STORE
//! likewise need more than a two-hop element move. Rather than apply a SILENT home-subset
//! partial, the serve loop REJECTS a spanning invocation of these LOUDLY with a clear,
//! descriptive error (see [`crate::serve`]'s spanning-move reject) that names the
//! co-location (hash-tag) remedy -- the SAME "correct, or explicitly aborted, never
//! silently wrong" contract the cross-shard MULTI/EXEC + WATCH guards already follow. These
//! are the tracked follow-up (a value-object cross-shard transfer), NOT a silent gap.
//!
//! ## shards == 1 parity (byte-identical)
//!
//! With one shard every key is home-owned, so `owner_shard_set` ALWAYS returns `Some(0)` and
//! a spanning move NEVER enters this module -- it routes co-located via Stage 1 (the local
//! fast path / the single-shard handler). So this module is dormant at `shards == 1` and the
//! wire reply is byte-identical to the single-shard handler.

use crate::coordinator::{self, Inbox};
use ironcache_runtime::bootstrap::ShardId;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ProtoVersion, Request, Value, owner_shard};

/// Build a one-shot sub-[`Request`] from owned byte parts (the verb + args), for routing to
/// an owner shard. The parts are cloned into `Bytes` (cheap; refcounted), so the caller can
/// keep its originals.
fn subreq(parts: &[&[u8]]) -> Request {
    Request {
        args: parts
            .iter()
            .map(|p| bytes::Bytes::copy_from_slice(p))
            .collect(),
    }
}

/// Route ONE keyed sub-request to the shard that OWNS `key` and return its un-encoded reply
/// [`Value`]. A home-owned key runs LOCALLY + SYNCHRONOUSLY ([`coordinator::run_local_keyed`],
/// no self-channel hop); a remote key hops via [`coordinator::dispatch_one_value`] (the
/// single-key mechanism Stage 1 routing uses). The borrow taken inside the owner call is
/// released before this returns (the no-borrow-across-await contract).
async fn route_to_owner(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    key: &[u8],
    req: &Request,
) -> Value {
    let owner = owner_shard(key, home.total);
    if owner == home.index {
        coordinator::run_local_keyed(ctx, req, db).value
    } else {
        coordinator::dispatch_one_value(inbox, owner, req, db).await
    }
}

/// The home-core dispatch for a SHARD-SPANNING src/dst move command (SMOVE / LMOVE /
/// RPOPLPUSH), encoding the reply into `out` with the home connection's `proto`
/// (COORDINATOR.md #107). The serve loop calls this when the command's two keys span shards
/// (`owner_shard_set == None`); the co-located case routes via Stage 1, unchanged.
///
/// `cmd_upper` is the uppercased command token (computed by the serve loop for routing). The
/// keys + options are parsed directly from `request` here on the home core.
///
/// Each argument is a distinct orthogonal seam the dispatch threads through (mirroring
/// [`crate::multikey::fan_out_multikey`] / [`crate::spanning_combine::fan_out_set`]); bundling
/// them would only obscure the per-call borrows, so the over-7-args lint is allowed here.
#[allow(clippy::too_many_arguments)]
pub async fn fan_out_spanning_move(
    inbox: &Inbox,
    ctx: &ServerContext,
    cmd_upper: &[u8],
    request: &Request,
    db: u32,
    home: ShardId,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    let reply = match cmd_upper {
        b"SMOVE" => smove_spanning(inbox, ctx, request, db, home).await,
        b"LMOVE" => lmove_spanning(inbox, ctx, request, db, home, false).await,
        b"RPOPLPUSH" => lmove_spanning(inbox, ctx, request, db, home, true).await,
        // The serve loop only routes the three supported move tokens here; any other is a
        // routing bug. Reply a well-formed error rather than panicking.
        _ => Value::error(ironcache_protocol::ErrorReply::err(
            "non-spanning-move command",
        )),
    };
    encode_into(out, &reply, proto);
}

/// ATOMIC cross-shard `SMOVE src dst member` (COORDINATOR.md #107). Preserves single-node
/// Redis `smoveCommand` semantics: source-first probe (a missing src short-circuits to `:0`
/// BEFORE dst's type is checked; a non-set src is WRONGTYPE), THEN dst's type is checked (a
/// non-set dst is WRONGTYPE), THEN the member is moved.
///
/// VALIDATE (no write): `SISMEMBER src member` on src owner -> a WRONGTYPE error aborts with
/// WRONGTYPE; `:0` (missing src OR member-absent) replies `:0` with NO write. `TYPE dst` on
/// dst owner -> a non-set, non-none dst aborts with WRONGTYPE, NO write.
///
/// COMMIT (member present, both types ok): `SADD dst member` on dst owner FIRST, then
/// `SREM src member` on src owner. The add-before-remove order means the member is never
/// missing from BOTH shards at once. Reply `:1`.
async fn smove_spanning(
    inbox: &Inbox,
    ctx: &ServerContext,
    request: &Request,
    db: u32,
    home: ShardId,
) -> Value {
    if request.args.len() != 4 {
        return Value::error(ironcache_protocol::ErrorReply::wrong_arity("smove"));
    }
    let src = request.args[1].clone();
    let dst = request.args[2].clone();
    let member = request.args[3].clone();

    // (1) Probe src FIRST (Redis source-first order). SISMEMBER src member: Integer(0/1), or
    // a WRONGTYPE error if src is a non-set. A missing src is Integer(0) (absent member).
    let probe = route_to_owner(
        inbox,
        ctx,
        home,
        db,
        &src,
        &subreq(&[b"SISMEMBER", &src, &member]),
    )
    .await;
    match probe {
        Value::Integer(1) => {} // member present in src -> proceed to the dst type check.
        Value::Integer(0) => return Value::Integer(0), // missing src OR member-absent: :0, no write.
        e @ Value::Error(_) => return e,               // WRONGTYPE (non-set src): abort, no write.
        // SISMEMBER only ever replies Integer or WRONGTYPE; anything else is the
        // shard-unavailable degradation -> surface it (no write).
        other => return other,
    }

    // (2) dst type check (only after src confirmed). A non-set, non-missing dst is WRONGTYPE.
    let dst_type = route_to_owner(inbox, ctx, home, db, &dst, &subreq(&[b"TYPE", &dst])).await;
    if let Some(reply) = wrongtype_if_not(&dst_type, b"set") {
        return reply;
    }

    // (3) COMMIT: add to dst FIRST (so the member is never missing from both), then remove
    // from src. Both are unconditional on a present member. A dst-write error (shard
    // unavailable) aborts BEFORE the src removal, so the member is not lost.
    let add = route_to_owner(
        inbox,
        ctx,
        home,
        db,
        &dst,
        &subreq(&[b"SADD", &dst, &member]),
    )
    .await;
    if let Value::Error(e) = add {
        return Value::Error(e);
    }
    let rem = route_to_owner(
        inbox,
        ctx,
        home,
        db,
        &src,
        &subreq(&[b"SREM", &src, &member]),
    )
    .await;
    if let Value::Error(e) = rem {
        // The dst SADD committed but the src SREM hop failed (src owner unavailable): the member
        // is transiently in BOTH src and dst. Compensate by removing it from dst (best-effort) so
        // the visible state rolls back to the pre-move state (member still in src), then surface
        // the error rather than falsely report a clean `:1` move over a duplicated state. A
        // double-fault (dst now also unavailable) leaves a duplicate, which the returned error
        // signals to the client.
        let _ = route_to_owner(
            inbox,
            ctx,
            home,
            db,
            &dst,
            &subreq(&[b"SREM", &dst, &member]),
        )
        .await;
        return Value::Error(e);
    }
    Value::Integer(1)
}

/// ATOMIC cross-shard `LMOVE src dst <from> <to>` / `RPOPLPUSH src dst` (COORDINATOR.md
/// #107). Preserves single-node Redis `lmoveGenericCommand` source-first semantics: pop one
/// element from src's end (this WRONGTYPE-checks src; a missing/empty src replies nil WITHOUT
/// inspecting dst). Then type-check dst: a non-list dst RESTORES the popped element to src and
/// replies WRONGTYPE (a no-op move). Else push to dst's end and reply the element.
///
/// The popped element is held ON THE HOME CORE between the src pop and the dst push, so it can
/// never be lost (a dst-WRONGTYPE restores it; a dst-write failure restores it). `is_rpoplpush`
/// selects the fixed `RIGHT LEFT` ends (RPOPLPUSH == LMOVE src dst RIGHT LEFT); otherwise the
/// `from`/`to` tokens are parsed from args[3]/args[4].
async fn lmove_spanning(
    inbox: &Inbox,
    ctx: &ServerContext,
    request: &Request,
    db: u32,
    home: ShardId,
    is_rpoplpush: bool,
) -> Value {
    // Parse the ends. RPOPLPUSH is exactly `LMOVE src dst RIGHT LEFT`.
    let (src, dst, from_left, to_left) = if is_rpoplpush {
        if request.args.len() != 3 {
            return Value::error(ironcache_protocol::ErrorReply::wrong_arity("rpoplpush"));
        }
        (
            request.args[1].clone(),
            request.args[2].clone(),
            false,
            true,
        )
    } else {
        if request.args.len() != 5 {
            return Value::error(ironcache_protocol::ErrorReply::wrong_arity("lmove"));
        }
        let from = ascii_upper(&request.args[3]);
        let to = ascii_upper(&request.args[4]);
        let from_left = match from.as_slice() {
            b"LEFT" => true,
            b"RIGHT" => false,
            _ => return Value::error(ironcache_protocol::ErrorReply::syntax_error()),
        };
        let to_left = match to.as_slice() {
            b"LEFT" => true,
            b"RIGHT" => false,
            _ => return Value::error(ironcache_protocol::ErrorReply::syntax_error()),
        };
        (
            request.args[1].clone(),
            request.args[2].clone(),
            from_left,
            to_left,
        )
    };

    // (a) Pop one element from src's `from` end (LPOP/RPOP). This WRONGTYPE-checks src and,
    // on a missing/empty src, replies a nil bulk (Null) WITHOUT inspecting dst (source-first).
    let pop_verb: &[u8] = if from_left { b"LPOP" } else { b"RPOP" };
    let popped = route_to_owner(inbox, ctx, home, db, &src, &subreq(&[pop_verb, &src])).await;
    let elem = match popped {
        Value::BulkString(Some(b)) => b,
        // A nil bulk -> src missing or empty: nil, no dst inspection (Redis source-first).
        Value::BulkString(None) | Value::Null => return Value::Null,
        e @ Value::Error(_) => return e, // WRONGTYPE (non-list src) or shard-unavailable.
        // LPOP/RPOP only reply a bulk, nil, or WRONGTYPE; anything else surfaces.
        other => return other,
    };

    // (b) The src element is now POPPED + held here. Type-check dst: a non-list, non-missing
    // dst is WRONGTYPE -> RESTORE the element to src's `from` end (so src is unchanged) and
    // reply WRONGTYPE (the move is a no-op).
    let dst_type = route_to_owner(inbox, ctx, home, db, &dst, &subreq(&[b"TYPE", &dst])).await;
    if wrongtype_if_not(&dst_type, b"list").is_some() {
        restore_to_src(inbox, ctx, home, db, &src, &elem, from_left).await;
        return Value::error(ironcache_protocol::ErrorReply::wrong_type());
    }

    // (c) Push the held element to dst's `to` end (LPUSH/RPUSH; creates dst if absent). A
    // dst-write failure (shard unavailable) RESTORES the element to src so it is not lost.
    let push_verb: &[u8] = if to_left { b"LPUSH" } else { b"RPUSH" };
    let push = route_to_owner(
        inbox,
        ctx,
        home,
        db,
        &dst,
        &subreq(&[push_verb, &dst, &elem]),
    )
    .await;
    if let Value::Error(e) = push {
        restore_to_src(inbox, ctx, home, db, &src, &elem, from_left).await;
        return Value::Error(e);
    }
    Value::BulkString(Some(elem))
}

/// Restore a popped element to `src`'s `from` end (the LMOVE abort path), so a dst-WRONGTYPE
/// or a dst-write failure leaves `src` byte-unchanged (the move was a no-op). The same end the
/// element was popped from: `from_left` -> LPUSH (front), else RPUSH (back).
async fn restore_to_src(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    src: &[u8],
    elem: &[u8],
    from_left: bool,
) {
    let verb: &[u8] = if from_left { b"LPUSH" } else { b"RPUSH" };
    let _ = route_to_owner(inbox, ctx, home, db, src, &subreq(&[verb, src, elem])).await;
}

/// Map a `TYPE key` reply to a WRONGTYPE abort iff the key exists AND is not `expected` (a
/// `Simple` type name). A `Simple("none")` (missing key) is fine (the move creates it). A
/// shard-unavailable error on the TYPE probe surfaces that error. Returns `Some(error_value)`
/// to abort, or `None` to proceed.
fn wrongtype_if_not(type_reply: &Value, expected: &[u8]) -> Option<Value> {
    match type_reply {
        Value::SimpleString(s) => {
            if s.as_bytes() == b"none" || s.as_bytes() == expected {
                None
            } else {
                Some(Value::error(ironcache_protocol::ErrorReply::wrong_type()))
            }
        }
        // A shard-unavailable degradation on the type probe: surface it (no write).
        e @ Value::Error(_) => Some(e.clone()),
        // TYPE only ever replies a Simple string; treat anything else as "proceed"
        // defensively (cannot occur).
        _ => None,
    }
}

/// The home-core ATOMIC apply for a SHARD-SPANNING `MSETNX k v [k v ...]` (COORDINATOR.md
/// #107), encoding the reply into `out`. Redis `msetnxCommand` scans EVERY key first and
/// aborts (writing nothing) if any is present; only if NONE exist does it write them all.
///
/// VALIDATE (no write): `EXISTS k` on each key's owner shard. If ANY replies `:1`, reply
/// `:0` and write NOTHING. COMMIT (none exist): group the pairs by `owner(key)` and fan a
/// per-owner `MSET` of that shard's pairs out; reply `:1`. Because the existence scan
/// completes BEFORE any write, the no-conflict path is all-or-nothing.
pub async fn fan_out_spanning_msetnx(
    inbox: &Inbox,
    ctx: &ServerContext,
    request: &Request,
    db: u32,
    home: ShardId,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    // Arity -3 (token + >= 1 pair) AND an EVEN number of key/value args (mirrors cmd_msetnx).
    if request.args.len() < 3 || (request.args.len() - 1) % 2 != 0 {
        encode_into(
            out,
            &Value::error(ironcache_protocol::ErrorReply::wrong_arity("msetnx")),
            proto,
        );
        return;
    }
    let pairs = &request.args[1..];
    let n_pairs = pairs.len() / 2;

    // (1) Existence scan: probe EACH key on its owner. The FIRST present key (or a
    // shard-unavailable error) aborts -- :0 (key present) or the surfaced error -- with NO
    // write. The scan is strictly sequential (one hop at a time), so it completes fully
    // before any write below (the all-or-nothing barrier).
    for i in 0..n_pairs {
        let key = &pairs[2 * i];
        let existed = route_to_owner(inbox, ctx, home, db, key, &subreq(&[b"EXISTS", key])).await;
        match existed {
            Value::Integer(0) => {} // absent: keep scanning.
            Value::Integer(_) => {
                // A present key: Redis aborts before any write and replies :0.
                encode_into(out, &Value::Integer(0), proto);
                return;
            }
            // A shard-unavailable degradation on the probe: surface it, write nothing.
            e @ Value::Error(_) => {
                encode_into(out, &e, proto);
                return;
            }
            other => {
                encode_into(out, &other, proto);
                return;
            }
        }
    }

    // (2) None exist -> write every pair on its owner. Group the pairs by owner(key) and fan
    // a per-owner MSET out (a sub-MSET is atomic on its owner shard). The existence scan
    // above already proved no key was present, so even though the writes are not under one
    // global lock, the no-conflict path applies ALL pairs and replies :1 (Redis MSETNX). A
    // racing writer that creates one of these keys between the scan and the write is the same
    // narrow window single-node Redis closes with its single-threaded apply; we accept it as
    // the documented Stage-3 limitation (still never a SILENT partial: the keys we write are
    // exactly the command's keys, on their real owners).
    let subreqs = group_pairs_by_owner(pairs, home.total);
    let replies = coordinator::fan_out_split(inbox, home, db, subreqs, |r| {
        coordinator::run_local_keyed(ctx, r, db)
    })
    .await;
    // Surface a shard-unavailable error if any sub-MSET failed; else :1 (all pairs written).
    for (_, r) in &replies {
        if let Value::Error(e) = &r.value {
            encode_into(out, &Value::error(e.clone()), proto);
            return;
        }
    }
    encode_into(out, &Value::Integer(1), proto);
}

/// Group `MSETNX`'s `[k v k v ...]` pairs by `owner(key)`, building one per-owner sub-`MSET`
/// of that shard's pairs flattened (`[MSET, k, v, k, v, ...]`). Mirrors
/// [`crate::multikey`]'s `group_pairs_by_owner` (kept here so this module is self-contained);
/// pairs are bucketed in original relative order (deterministic).
fn group_pairs_by_owner(pairs: &[bytes::Bytes], n_shards: usize) -> Vec<(usize, Request)> {
    let n_pairs = pairs.len() / 2;
    let owners: Vec<usize> = (0..n_pairs)
        .map(|i| owner_shard(&pairs[2 * i], n_shards))
        .collect();

    let mut shard_order: Vec<usize> = Vec::new();
    for &o in &owners {
        if !shard_order.contains(&o) {
            shard_order.push(o);
        }
    }

    let mut subreqs: Vec<(usize, Request)> = Vec::with_capacity(shard_order.len());
    for &shard in &shard_order {
        let mut args: Vec<bytes::Bytes> = vec![bytes::Bytes::from_static(b"MSET")];
        for (i, &o) in owners.iter().enumerate() {
            if o == shard {
                args.push(pairs[2 * i].clone()); // key
                args.push(pairs[2 * i + 1].clone()); // value
            }
        }
        subreqs.push((shard, Request { args }));
    }
    subreqs
}

/// ASCII-uppercase a token (the LMOVE direction parse), delegating to the canonical
/// [`ironcache_server::cmd_util::ascii_upper`] so the direction token uppercases into a stack
/// buffer with no per-command heap allocation.
fn ascii_upper(b: &[u8]) -> ironcache_server::cmd_util::UpperToken {
    ironcache_server::cmd_util::ascii_upper(b)
}

/// Encode `value` for `proto` and append to `out` (the home-core encode; mirrors the serve
/// loop / coordinator / multikey encode). Encoding stays on the home core with the home proto.
fn encode_into(out: &mut Vec<u8>, value: &Value, proto: ProtoVersion) {
    // Vec<u8> is a bytes::BufMut sink: encode writes straight into `out` (no temp BytesMut + copy).
    ironcache_protocol::encode(out, value, proto);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(parts: &[&[u8]]) -> Vec<bytes::Bytes> {
        parts
            .iter()
            .map(|p| bytes::Bytes::copy_from_slice(p))
            .collect()
    }

    #[test]
    fn subreq_builds_request_from_parts() {
        let r = subreq(&[b"SADD", b"k", b"m"]);
        assert_eq!(r.args.len(), 3);
        assert_eq!(r.args[0].as_ref(), b"SADD");
        assert_eq!(r.args[2].as_ref(), b"m");
    }

    #[test]
    fn wrongtype_if_not_handles_none_match_and_mismatch() {
        // A missing key (none) -> proceed (the move creates it).
        assert!(wrongtype_if_not(&Value::simple("none"), b"set").is_none());
        // The expected type -> proceed.
        assert!(wrongtype_if_not(&Value::simple("set"), b"set").is_none());
        // A different type -> WRONGTYPE abort.
        assert!(matches!(
            wrongtype_if_not(&Value::simple("list"), b"set"),
            Some(Value::Error(_))
        ));
        // A shard-unavailable error surfaces.
        assert!(matches!(
            wrongtype_if_not(
                &Value::error(ironcache_protocol::ErrorReply::err(
                    coordinator::SHARD_UNAVAILABLE_MSG
                )),
                b"set"
            ),
            Some(Value::Error(_))
        ));
    }

    #[test]
    fn group_pairs_by_owner_keeps_pairs_on_their_key_owner() {
        let n = 4usize;
        // MSETNX k1 v1 k2 v2 k3 v3 (pairs only; the token is stripped by the caller).
        let pairs = bytes(&[b"k1", b"v1", b"k2", b"v2", b"k3", b"v3"]);
        let subreqs = group_pairs_by_owner(&pairs, n);
        let mut seen_keys: Vec<Vec<u8>> = Vec::new();
        for (shard, req) in &subreqs {
            assert_eq!(req.args[0].as_ref(), b"MSET");
            assert_eq!((req.args.len() - 1) % 2, 0, "flattened pairs stay even");
            let mut i = 1;
            while i + 1 < req.args.len() {
                let key = &req.args[i];
                assert_eq!(owner_shard(key, n), *shard, "pair lives on its key's owner");
                seen_keys.push(key.to_vec());
                i += 2;
            }
        }
        seen_keys.sort();
        assert_eq!(
            seen_keys,
            vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
            "every pair placed once"
        );
    }
}
