// SPDX-License-Identifier: MIT OR Apache-2.0
//! Home-core GATHER + (shared) COMBINE + STORE for the SHARD-SPANNING set-algebra,
//! sorted-set-algebra, BITOP, and HyperLogLog (PFCOUNT/PFMERGE) commands (COORDINATOR.md
//! #107, coordinator Stage 2b-1 + 2b-2 + 2b-3).
//!
//! IronCache presents as a SINGLE NODE, so a spanning multi-key command is TRANSPARENT
//! (gather-combine, NOT `-CROSSSLOT`): the coordinator GATHERS each source value from its
//! owner shard, COMBINES with a PURE function SHARED with the single-shard handler
//! ([`ironcache_server::set_combine`], the one source of truth so the cross-shard and
//! single-shard results CANNOT drift), and for the `*STORE` forms WRITES the result to the
//! dest owner via the internal [`ironcache_server::ICSTORESET`] verb.
//!
//! This pass handles SEVEN commands when their keys SPAN shards (the co-located case still
//! routes via Stage 1, `owner_shard_set == Some`, unchanged):
//!
//! - **SINTER / SUNION / SDIFF key [key ...]**: gather each source's members, combine, reply
//!   the result as a [`Value::Set`] (RESP3 `~` / RESP2 `*`, like single-shard SMEMBERS).
//! - **SINTERCARD numkeys key [key ...] [LIMIT n]**: gather, intersect, reply the cardinality
//!   capped at `LIMIT` (`LIMIT 0` = unlimited), parsed on the home core.
//! - **SINTERSTORE / SUNIONSTORE / SDIFFSTORE dest src [src ...]**: gather the SOURCES,
//!   combine, then write the result to `dest`'s owner. An EMPTY result routes `DEL dest`
//!   (Redis deletes dest on an empty `*STORE`) and replies `0`; a non-empty result routes
//!   `__ICSTORESET dest m...` (a BLIND OVERWRITE clearing any prior type/TTL, the EXACT
//!   single-shard `*STORE` write) and replies the result cardinality.
//!
//! ## WRONGTYPE aborts BEFORE any store write
//!
//! Every source is gathered + type-validated FIRST. A `Value::Error` from any source
//! (a WRONGTYPE on a non-set, or a shard-unavailable degradation) ABORTS the whole command
//! with that error and performs NO store write -- so a spanning `*STORE` with a wrong-type
//! source leaves `dest` untouched (neither written nor deleted), matching single-node Redis.
//!
//! ## Borrow discipline (ADR-0002/0005)
//!
//! The home shard's gathers run via [`coordinator::run_local_keyed`], which is SYNCHRONOUS
//! and releases every `RefCell` borrow before returning; remote gathers run via
//! [`coordinator::dispatch_one_value`] (a channel hop). The COMBINE + the result encode run
//! on the home core AFTER all gather awaits complete, so NO borrow of the home thread-locals
//! is held across an `.await` (the no-borrow-across-await contract the rest of the
//! coordinator follows).
//!
//! ## shards == 1 parity (byte-identical)
//!
//! With one shard every key is home-owned, so `owner_shard_set` ALWAYS returns `Some(0)` and
//! a multi-key command NEVER enters this spanning path -- it routes co-located via Stage 1.
//! So this module is dormant at `shards == 1` and the wire reply is byte-identical to the
//! single-shard handler.

use crate::coordinator::{self, Inbox};
use ironcache_protocol::ErrorCode;
use ironcache_runtime::bootstrap::ShardId;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{
    AggOp, Aggregate, HLL_REGISTERS, ProtoVersion, Request, ScoredMember, SetOp, Value,
    WeightedSource, bitop_compute, bitop_validate_op, estimate_reply, hll_from_regs, is_valid_hll,
    merge_into, owner_shard, regs_reghisto, set_combine, zset_combine,
};

/// GATHER one source key's set members from its OWNER shard (COORDINATOR.md #107, Stage 2b):
/// route `SMEMBERS key` to the owner (home-owned keys run LOCALLY + synchronously via
/// [`coordinator::run_local_keyed`]; remote keys hop via [`coordinator::dispatch_one_value`]),
/// then map the reply to the key's members.
///
/// Returns `Ok(members)` for a set (or an empty `Vec` for a missing key -- a missing source
/// is an EMPTY set, matching the single-shard algebra), or `Err(error_value)` if the reply is
/// an `Error` (a WRONGTYPE on a non-set source, OR a shard-unavailable degradation): the
/// caller ABORTS the whole command with that error before any store write.
///
/// `run_local_keyed` returns before any `.await` and `dispatch_one_value` holds no home-core
/// borrow across its hop, so this respects the no-borrow-across-await contract.
async fn gather_members(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    key: &[u8],
) -> Result<Vec<Vec<u8>>, Value> {
    let owner = owner_shard(key, home.total);
    // The sub-command: SMEMBERS of this one key, run on its owner shard. SMEMBERS on a
    // missing key replies an empty set; on a non-set, WRONGTYPE.
    let subreq = Request {
        args: vec![
            bytes::Bytes::from_static(b"SMEMBERS"),
            bytes::Bytes::copy_from_slice(key),
        ],
    };
    let value = if owner == home.index {
        // Home-owned: run synchronously on the home thread-locals (no self-channel hop).
        coordinator::run_local_keyed(ctx, &subreq, db).value
    } else {
        // Remote: hop to the owning shard's drain loop and await the un-encoded Value.
        coordinator::dispatch_one_value(inbox, owner, &subreq, db).await
    };
    members_of(value)
}

/// Map an `SMEMBERS` reply [`Value`] to a key's members, or signal an abort. A
/// [`Value::Set`] / [`Value::Array`] -> its bulk members as `Vec<Vec<u8>>` (a missing key's
/// empty set -> an empty `Vec`); a [`Value::Error`] (WRONGTYPE on a non-set source, or a
/// shard-unavailable degradation) -> `Err(error_value)` so the caller aborts with it BEFORE
/// any store write. Any other shape is a routing bug; treat it as an empty set defensively
/// (SMEMBERS only ever replies a set or a WRONGTYPE error).
fn members_of(value: Value) -> Result<Vec<Vec<u8>>, Value> {
    match value {
        Value::Set(items) | Value::Array(Some(items)) => {
            let mut out: Vec<Vec<u8>> = Vec::with_capacity(items.len());
            for it in items {
                if let Value::BulkString(Some(b)) = it {
                    out.push(b.to_vec());
                }
            }
            Ok(out)
        }
        e @ Value::Error(_) => Err(e),
        // SMEMBERS never replies any other shape; treat defensively as an empty set.
        _ => Ok(Vec::new()),
    }
}

/// Write the spanning `*STORE` result set to `dest`'s OWNER shard with the EXACT
/// single-shard blind-overwrite-clearing-TTL semantics (COORDINATOR.md #107, Stage 2b),
/// returning the integer reply [`Value`]:
/// - EMPTY result -> route `DEL dest` to the dest owner and reply `Integer(0)` (Redis
///   deletes dest on an empty `*STORE` result).
/// - non-empty result -> route the internal `__ICSTORESET dest m...` verb to the dest owner
///   (a blind overwrite that clears any prior type/TTL, the EXACT single-shard `*STORE`
///   write) and reply the result cardinality.
///
/// The dest is routed to its OWNER like any keyed write: a home-owned dest runs LOCALLY +
/// synchronously, a remote dest hops via [`coordinator::dispatch_one_value`].
async fn store_result(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    dest: &[u8],
    result: Vec<Vec<u8>>,
) -> Value {
    let owner = owner_shard(dest, home.total);
    let subreq = if result.is_empty() {
        // Redis deletes the destination on an empty result. DEL is a keyed command that
        // routes to the dest owner like any other; its integer reply (0 or 1) is discarded
        // -- a spanning *STORE replies the result cardinality (0 here), not DEL's count.
        Request {
            args: vec![
                bytes::Bytes::from_static(b"DEL"),
                bytes::Bytes::copy_from_slice(dest),
            ],
        }
    } else {
        // Blind overwrite via the internal verb: [__ICSTORESET, dest, m...].
        let mut args: Vec<bytes::Bytes> = Vec::with_capacity(2 + result.len());
        args.push(bytes::Bytes::copy_from_slice(ironcache_server::ICSTORESET));
        args.push(bytes::Bytes::copy_from_slice(dest));
        for m in &result {
            args.push(bytes::Bytes::copy_from_slice(m));
        }
        Request { args }
    };

    let card = result.len() as i64;
    let write_reply = if owner == home.index {
        coordinator::run_local_keyed(ctx, &subreq, db).value
    } else {
        coordinator::dispatch_one_value(inbox, owner, &subreq, db).await
    };
    // A shard-unavailable degradation on the dest write is surfaced (the result was NOT
    // stored); otherwise the *STORE reply is the result cardinality (NOT the sub-command's
    // own integer reply: DEL's 0/1, or __ICSTORESET's echoed cardinality).
    match write_reply {
        Value::Error(e) => Value::Error(e),
        _ if result.is_empty() => Value::Integer(0),
        _ => Value::Integer(card),
    }
}

/// The home-core GATHER + COMBINE + (optional) STORE for a SHARD-SPANNING set-algebra
/// command, encoding the reply into `out` with the home connection's `proto`
/// (COORDINATOR.md #107, Stage 2b-1). The serve loop calls this for the SEVEN commands when
/// their keys span shards (`owner_shard_set == None`); the co-located case routes via Stage 1.
///
/// `cmd_upper` is the uppercased command token (computed by the serve loop for routing). The
/// keys / dest / numkeys / LIMIT are parsed directly from `request` here on the home core.
///
/// Each argument is a distinct orthogonal seam (mirroring [`crate::multikey::fan_out_multikey`]);
/// bundling them would obscure the per-call borrows, so the over-7-args lint is allowed.
#[allow(clippy::too_many_arguments)]
pub async fn fan_out_set(
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
        // READ forms: SINTER/SUNION/SDIFF dest-less; keys are args[1..]. Gather, combine,
        // reply the result as a set.
        b"SINTER" | b"SUNION" | b"SDIFF" => {
            let op = read_op(cmd_upper);
            let keys: Vec<bytes::Bytes> = request.args[1..].to_vec();
            match gather_all(inbox, ctx, home, db, &keys).await {
                Ok(sources) => Value::Set(
                    set_combine(op, &sources)
                        .into_iter()
                        .map(|m| Value::BulkString(Some(bytes::Bytes::from(m))))
                        .collect(),
                ),
                Err(e) => e,
            }
        }
        // SINTERCARD numkeys key [key ...] [LIMIT n]: parse on the home core, gather the
        // numkeys keys, intersect, reply the (capped) cardinality.
        b"SINTERCARD" => match parse_sintercard(request) {
            Ok((keys, limit)) => match gather_all(inbox, ctx, home, db, &keys).await {
                Ok(sources) => {
                    let card = set_combine(SetOp::Inter, &sources).len();
                    let capped = if limit == 0 { card } else { card.min(limit) };
                    Value::Integer(capped as i64)
                }
                Err(e) => e,
            },
            Err(e) => e,
        },
        // STORE forms: SINTERSTORE/SUNIONSTORE/SDIFFSTORE dest src [src ...]. dest=args[1],
        // sources=args[2..]. Gather the SOURCES + validate ALL before any write; on a
        // wrong-type/unavailable source ABORT (dest untouched); else combine + store.
        b"SINTERSTORE" | b"SUNIONSTORE" | b"SDIFFSTORE" => {
            let op = store_op(cmd_upper);
            let dest = request.args[1].clone();
            let sources: Vec<bytes::Bytes> = request.args[2..].to_vec();
            match gather_all(inbox, ctx, home, db, &sources).await {
                Ok(gathered) => {
                    let result = set_combine(op, &gathered);
                    store_result(inbox, ctx, home, db, &dest, result).await
                }
                Err(e) => e,
            }
        }
        // The serve loop only routes the seven supported commands here; any other token is a
        // routing bug. Reply a well-formed error rather than panicking.
        _ => Value::error(ironcache_protocol::ErrorReply::err(
            "non-fan-out spanning set command",
        )),
    };
    encode_into(out, &reply, proto);
}

/// GATHER every source key's members in ORIGINAL KEY ORDER, validating ALL sources FIRST: a
/// single `Value::Error` (WRONGTYPE on a non-set source, or a shard-unavailable degradation)
/// short-circuits to `Err` so the caller aborts BEFORE any store write (no partial). The
/// gathers run sequentially in key order; ordering is preserved so `set_combine`'s
/// first-source semantics (SDIFF's first-minus-rest) stay correct.
async fn gather_all(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    keys: &[bytes::Bytes],
) -> Result<Vec<Vec<Vec<u8>>>, Value> {
    let mut sources: Vec<Vec<Vec<u8>>> = Vec::with_capacity(keys.len());
    for k in keys {
        sources.push(gather_members(inbox, ctx, home, db, k).await?);
    }
    Ok(sources)
}

/// The [`SetOp`] for a READ-form command token (SINTER/SUNION/SDIFF). The caller guarantees
/// the token is one of the three (the serve loop's `is_fan_out_spanning_combine` gate).
fn read_op(cmd_upper: &[u8]) -> SetOp {
    match cmd_upper {
        b"SUNION" => SetOp::Union,
        b"SDIFF" => SetOp::Diff,
        _ => SetOp::Inter, // SINTER (and the gated default)
    }
}

/// The [`SetOp`] for a STORE-form command token (SINTERSTORE/SUNIONSTORE/SDIFFSTORE). The
/// caller guarantees the token is one of the three.
fn store_op(cmd_upper: &[u8]) -> SetOp {
    match cmd_upper {
        b"SUNIONSTORE" => SetOp::Union,
        b"SDIFFSTORE" => SetOp::Diff,
        _ => SetOp::Inter, // SINTERSTORE
    }
}

/// Parse `SINTERCARD numkeys key [key ...] [LIMIT n]` on the home core, returning the source
/// keys and the LIMIT (`0` = unlimited), or an error [`Value`] for a malformed request. This
/// MIRRORS the single-shard `cmd_sintercard` parse EXACTLY (the same error catalog entries
/// and ordering), so a spanning SINTERCARD's argument errors are byte-identical to the
/// single-shard ones. The serve loop only reaches this when the keys SPAN shards (so
/// `owner_shard_set == None` and there are `Many` keys), but we re-validate fully here so a
/// malformed spanning SINTERCARD still gets the proper error.
fn parse_sintercard(request: &Request) -> Result<(Vec<bytes::Bytes>, usize), Value> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() < 3 {
        return Err(Value::error(ErrorReply::wrong_arity("sintercard")));
    }
    let Some(numkeys) = parse_i64(&request.args[1]) else {
        return Err(Value::error(ErrorReply::not_an_integer()));
    };
    if numkeys <= 0 {
        return Err(Value::error(ErrorReply::numkeys_should_be_positive()));
    }
    let numkeys = numkeys as usize;
    if 2 + numkeys > request.args.len() {
        return Err(Value::error(ErrorReply::numkeys_greater_than_args()));
    }
    let keys: Vec<bytes::Bytes> = request.args[2..2 + numkeys].to_vec();
    let mut limit: usize = 0; // 0 = no limit
    let mut i = 2 + numkeys;
    while i < request.args.len() {
        let opt = ascii_upper(&request.args[i]);
        match opt.as_slice() {
            b"LIMIT" => {
                if i + 1 >= request.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                match parse_i64(&request.args[i + 1]) {
                    Some(n) if n < 0 => {
                        return Err(Value::error(ErrorReply::limit_cant_be_negative()));
                    }
                    Some(n) => limit = n as usize,
                    None => return Err(Value::error(ErrorReply::not_an_integer())),
                }
                i += 2;
            }
            _ => return Err(Value::error(ErrorReply::syntax_error())),
        }
    }
    Ok((keys, limit))
}

/// ASCII-uppercase a token (for the SINTERCARD `LIMIT` option compare). Local to the home
/// core; delegates to the canonical [`ironcache_server::cmd_util::ascii_upper`] so the
/// option token uppercases into a stack buffer with no per-command heap allocation.
fn ascii_upper(token: &[u8]) -> ironcache_server::cmd_util::UpperToken {
    ironcache_server::cmd_util::ascii_upper(token)
}

/// Parse a decimal `i64` (the SINTERCARD/ZINTERCARD numkeys + LIMIT). Delegates to the
/// server's canonical `cmd_util::parse_i64` so the spanning parse is BYTE-IDENTICAL to the
/// single-shard handler: that parser is strict (rejects surrounding whitespace and a leading
/// `+`, only special-casing a leading `-`), unlike Rust's `i64::from_str`. Using a lenient
/// local parse here would accept a non-canonical LIMIT token the single-shard command rejects
/// (a Stage 2b parity break).
fn parse_i64(b: &[u8]) -> Option<i64> {
    ironcache_server::cmd_util::parse_i64(b)
}

/// Encode `value` for `proto` and append to `out` (the home-core encode; mirrors the serve
/// loop / coordinator / multikey encode). Encoding stays on the home core with the home proto.
fn encode_into(out: &mut Vec<u8>, value: &Value, proto: ProtoVersion) {
    // Vec<u8> is a bytes::BufMut sink: encode writes straight into `out` (no temp BytesMut + copy).
    ironcache_protocol::encode(out, value, proto);
}

// ===========================================================================
// SORTED-SET algebra (COORDINATOR.md #107, coordinator Stage 2b-2).
//
// ZUNION / ZINTER / ZDIFF / ZINTERCARD (read) and ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE
// (write to dest) and ZRANGESTORE (dest, src) when their keys SPAN shards. Same design as
// the set algebra above: GATHER each source's `(member, score)` pairs from its owner shard,
// COMBINE with the PURE [`zset_combine`] SHARED with the single-shard handler (the one source
// of truth so the cross-shard and single-shard results CANNOT drift), and for the `*STORE` /
// ZRANGESTORE forms WRITE the result to the dest owner via the internal
// [`ironcache_server::ICSTOREZSET`] verb.
//
// SET-source-as-score-1.0 (gather fidelity, cmd_zset.rs read_agg_source): a source that is a
// PLAIN SET is treated as a zset of all-1.0 scores. `ZRANGE` on a set returns WRONGTYPE, so
// the gather falls back to `SMEMBERS` and synthesizes score-1.0 pairs; only if BOTH ZRANGE
// AND SMEMBERS error (a genuine string/list/hash) is it a real WRONGTYPE -> abort the whole
// command BEFORE any store write. All sources are validated first.
// ===========================================================================

/// GATHER one source key's `(member, score)` pairs from its OWNER shard, with the EXACT
/// single-shard `read_agg_source` fidelity (cmd_zset.rs): a ZSET yields its members+scores; a
/// PLAIN SET is treated as a zset with all scores `1.0`; a missing key is an EMPTY result; any
/// OTHER type (string/list/hash) is a genuine WRONGTYPE that ABORTS the whole command.
///
/// Implementation: route `ZRANGE key 0 -1 WITHSCORES` to the owner. On success map the pairs.
/// On WRONGTYPE (the key is a set, OR a genuine wrong type) FALL BACK to `SMEMBERS key`: a
/// set reply -> score-1.0 pairs; a SMEMBERS WRONGTYPE -> the key is truly a non-zset/non-set,
/// so return `Err(error)` (the caller aborts BEFORE any store write). A non-WRONGTYPE error
/// from ZRANGE (a shard-unavailable degradation) is surfaced directly.
///
/// `run_local_keyed` returns before any `.await` and `dispatch_one_value` holds no home-core
/// borrow across its hop, so this respects the no-borrow-across-await contract.
async fn gather_zset_pairs(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    key: &[u8],
) -> Result<Vec<ScoredMember>, Value> {
    // ZRANGE key 0 -1 WITHSCORES: the full zset as (member, score) pairs, on the owner shard.
    let zrange = Request {
        args: vec![
            bytes::Bytes::from_static(b"ZRANGE"),
            bytes::Bytes::copy_from_slice(key),
            bytes::Bytes::from_static(b"0"),
            bytes::Bytes::from_static(b"-1"),
            bytes::Bytes::from_static(b"WITHSCORES"),
        ],
    };
    let value = route_to_owner(inbox, ctx, home, db, key, &zrange).await;
    match value {
        // A WRONGTYPE from ZRANGE means the key is NOT a zset: it is either a plain SET (which
        // the single-shard path treats as score-1.0) or a genuine non-zset/non-set. Resolve
        // by SMEMBERS: a set reply -> score-1.0 pairs; a SMEMBERS WRONGTYPE -> genuine abort.
        Value::Error(ref e) if e.code() == ErrorCode::WrongType => {
            gather_set_as_score_one(inbox, ctx, home, db, key).await
        }
        // Any other error (a shard-unavailable degradation) aborts with that error.
        e @ Value::Error(_) => Err(e),
        other => Ok(pairs_of(other)),
    }
}

/// The SET-source-as-score-1.0 fallback (cmd_zset.rs read_agg_source): route `SMEMBERS key`
/// and synthesize a `(member, 1.0)` pair per member. A set/array reply -> score-1.0 pairs (a
/// missing key -> empty); a WRONGTYPE (the key is a genuine string/list/hash, not a set) ->
/// `Err(error)` so the caller aborts the whole command BEFORE any store write.
async fn gather_set_as_score_one(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    key: &[u8],
) -> Result<Vec<ScoredMember>, Value> {
    let smembers = Request {
        args: vec![
            bytes::Bytes::from_static(b"SMEMBERS"),
            bytes::Bytes::copy_from_slice(key),
        ],
    };
    let value = route_to_owner(inbox, ctx, home, db, key, &smembers).await;
    // SMEMBERS replies a set/array or a WRONGTYPE (a genuine non-set/non-zset). `members_of`
    // maps the set/array to members and a WRONGTYPE to `Err` -- map the members to score 1.0.
    members_of(value).map(|members| members.into_iter().map(|m| (m, 1.0)).collect())
}

/// Route a single keyed sub-request to its OWNER shard and return the un-encoded reply
/// [`Value`]: a home-owned key runs LOCALLY + synchronously via [`coordinator::run_local_keyed`]
/// (no self-channel hop, every borrow released before return); a remote key hops via
/// [`coordinator::dispatch_one_value`]. The shared owner-routing primitive for the zset gathers
/// and the dest write.
async fn route_to_owner(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    key: &[u8],
    subreq: &Request,
) -> Value {
    let owner = owner_shard(key, home.total);
    if owner == home.index {
        coordinator::run_local_keyed(ctx, subreq, db).value
    } else {
        coordinator::dispatch_one_value(inbox, owner, subreq, db).await
    }
}

/// Map a `ZRANGE ... WITHSCORES` reply [`Value`] to `(member, score)` pairs. The WITHSCORES
/// reply is a [`Value::Pairs`] of `(member-bulk, Value::Double)` (the in-process shape, which
/// the encoder later renders RESP3-nested or RESP2-flat); a missing key is an empty
/// [`Value::Array`]. We also accept a flat [`Value::Array`] of `[member, score, ...]`
/// defensively (the RESP2 flattening), pairing adjacent elements. Any other shape is treated
/// as empty (ZRANGE never replies another shape after the WRONGTYPE is handled upstream).
fn pairs_of(value: Value) -> Vec<ScoredMember> {
    match value {
        Value::Pairs(items) => items
            .into_iter()
            .filter_map(|(m, s)| Some((bulk_bytes(m)?, score_f64(s)?)))
            .collect(),
        // A missing key (empty) -> empty; a flat [member, score, ...] array -> adjacent pairs.
        Value::Array(Some(items)) => {
            let mut out = Vec::with_capacity(items.len() / 2);
            let mut it = items.into_iter();
            while let (Some(m), Some(s)) = (it.next(), it.next()) {
                if let (Some(member), Some(score)) = (bulk_bytes(m), score_f64(s)) {
                    out.push((member, score));
                }
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Extract the bytes of a member [`Value::BulkString`]. `None` for any other shape (skipped).
fn bulk_bytes(v: Value) -> Option<Vec<u8>> {
    match v {
        Value::BulkString(Some(b)) => Some(b.to_vec()),
        _ => None,
    }
}

/// Extract the `f64` of a score, accepting BOTH a [`Value::Double`] (the WITHSCORES typed
/// double) AND a [`Value::BulkString`] holding the human score spelling (the RESP2 flattened
/// shape). `None` for any other shape or an unparseable bulk.
fn score_f64(v: Value) -> Option<f64> {
    match v {
        Value::Double(d) => Some(d),
        Value::BulkString(Some(b)) => std::str::from_utf8(&b).ok()?.parse::<f64>().ok(),
        _ => None,
    }
}

/// Encode a finite/infinite `f64` score for the `__ICSTOREZSET` verb arg so it ROUND-TRIPS
/// EXACTLY through the verb's `parse_f64` (cmd_zset.rs): Rust's `f64` `Display` is the shortest
/// round-trip decimal (no exponent), preserves the SIGN OF ZERO (`-0` stays `-0`, unlike the
/// human `format_human_double` which normalizes `-0 -> 0`), and renders the infinities as
/// `inf` / `-inf` (which `parse_f64` accepts). `zset_combine` coerces every NaN to `0.0`, so a
/// NaN never reaches here. This exact round-trip is what keeps the cross-shard stored zset
/// byte-identical to the single-shard one.
fn score_arg(score: f64) -> bytes::Bytes {
    bytes::Bytes::from(format!("{score}").into_bytes())
}

/// Write the spanning zset `*STORE` / `ZRANGESTORE` result to `dest`'s OWNER shard with the
/// EXACT single-shard blind-overwrite-clearing-TTL semantics (COORDINATOR.md #107, Stage
/// 2b-2), returning the integer reply [`Value`]:
/// - EMPTY result -> route `DEL dest` to the dest owner and reply `Integer(0)` (Redis deletes
///   dest on an empty `*STORE` / `ZRANGESTORE` result).
/// - non-empty result -> route the internal `__ICSTOREZSET dest m1 s1 ...` verb to the dest
///   owner (a blind overwrite that clears any prior type/TTL, the EXACT single-shard write)
///   and reply the result cardinality.
async fn store_zset_result(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    dest: &[u8],
    result: Vec<ScoredMember>,
) -> Value {
    let subreq = if result.is_empty() {
        // Redis deletes the destination on an empty result. DEL routes to the dest owner like
        // any keyed command; its integer reply is discarded -- a spanning *STORE replies the
        // result cardinality (0 here), not DEL's count.
        Request {
            args: vec![
                bytes::Bytes::from_static(b"DEL"),
                bytes::Bytes::copy_from_slice(dest),
            ],
        }
    } else {
        // Blind overwrite via the internal verb: [__ICSTOREZSET, dest, m1, s1, m2, s2, ...].
        let mut args: Vec<bytes::Bytes> = Vec::with_capacity(2 + result.len() * 2);
        args.push(bytes::Bytes::copy_from_slice(ironcache_server::ICSTOREZSET));
        args.push(bytes::Bytes::copy_from_slice(dest));
        for (m, s) in &result {
            args.push(bytes::Bytes::copy_from_slice(m));
            args.push(score_arg(*s));
        }
        Request { args }
    };

    let card = result.len() as i64;
    let write_reply = route_to_owner(inbox, ctx, home, db, dest, &subreq).await;
    // A shard-unavailable degradation on the dest write is surfaced (the result was NOT
    // stored); otherwise the reply is the result cardinality (NOT the sub-command's own
    // integer reply: DEL's 0/1, or __ICSTOREZSET's echoed cardinality).
    match write_reply {
        Value::Error(e) => Value::Error(e),
        _ if result.is_empty() => Value::Integer(0),
        _ => Value::Integer(card),
    }
}

/// The home-core GATHER + (shared) COMBINE + (optional) STORE for a SHARD-SPANNING zset
/// algebra command, encoding the reply into `out` with the home connection's `proto`
/// (COORDINATOR.md #107, Stage 2b-2). The serve loop calls this for the EIGHT commands when
/// their keys SPAN shards (`owner_shard_set == None`); the co-located case routes via Stage 1.
///
/// `cmd_upper` is the uppercased command token (computed by the serve loop for routing). The
/// numkeys / keys / dest / WEIGHTS / AGGREGATE / WITHSCORES / LIMIT are parsed directly from
/// `request` here on the home core (mirroring the single-shard cmd_zset parse byte-for-byte).
///
/// Each argument is a distinct orthogonal seam (mirroring [`fan_out_set`]); bundling them
/// would obscure the per-call borrows, so the over-7-args lint is allowed.
#[allow(clippy::too_many_arguments)]
pub async fn fan_out_zset(
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
        // READ forms: ZUNION/ZINTER/ZDIFF numkeys key... [WEIGHTS][AGGREGATE][WITHSCORES].
        b"ZUNION" | b"ZINTER" | b"ZDIFF" => {
            let op = read_agg_op(cmd_upper);
            // ZDIFF has no WEIGHTS/AGGREGATE; the read forms accept WITHSCORES. numkeys at 1.
            match parse_agg_args(request, 1, op != AggOp::Diff, true) {
                Ok(args) => match gather_agg_sources(inbox, ctx, home, db, &args.keys).await {
                    Ok(sources) => {
                        let pairs = combine_with_weights(op, &args, sources);
                        zset_members_reply(pairs, args.with_scores)
                    }
                    Err(e) => e,
                },
                Err(e) => e,
            }
        }
        // ZINTERCARD numkeys key... [LIMIT n]: parse, gather, intersect, reply (capped) card.
        b"ZINTERCARD" => match parse_zintercard(request) {
            Ok((keys, limit)) => match gather_agg_sources(inbox, ctx, home, db, &keys).await {
                Ok(sources) => {
                    // ZINTERCARD has no WEIGHTS (all 1.0) and the default SUM aggregate (the
                    // count is membership-only; the scores do not affect the cardinality).
                    let with_weights: Vec<WeightedSource> =
                        sources.into_iter().map(|pairs| (pairs, 1.0)).collect();
                    let card = zset_combine(AggOp::Inter, Aggregate::Sum, &with_weights).len();
                    let capped = if limit == 0 { card } else { card.min(limit) };
                    Value::Integer(capped as i64)
                }
                Err(e) => e,
            },
            Err(e) => e,
        },
        // STORE forms: ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE dest numkeys key...
        b"ZUNIONSTORE" | b"ZINTERSTORE" | b"ZDIFFSTORE" => {
            let op = store_agg_op(cmd_upper);
            let dest = request.args[1].clone();
            // dest at 1, numkeys at 2; the *STORE forms do NOT accept WITHSCORES.
            match parse_agg_args(request, 2, op != AggOp::Diff, false) {
                Ok(args) => match gather_agg_sources(inbox, ctx, home, db, &args.keys).await {
                    Ok(sources) => {
                        let pairs = combine_with_weights(op, &args, sources);
                        store_zset_result(inbox, ctx, home, db, &dest, pairs).await
                    }
                    Err(e) => e,
                },
                Err(e) => e,
            }
        }
        // ZRANGESTORE dst src start stop [opts]: a 2-key copy-range.
        b"ZRANGESTORE" => fan_out_zrangestore(inbox, ctx, request, db, home).await,
        // The serve loop only routes the eight supported commands here; any other token is a
        // routing bug. Reply a well-formed error rather than panicking.
        _ => Value::error(ironcache_protocol::ErrorReply::err(
            "non-fan-out spanning zset command",
        )),
    };
    encode_into(out, &reply, proto);
}

/// GATHER every aggregation source key's `(member, score)` pairs in ORIGINAL KEY ORDER,
/// validating ALL sources FIRST: a single source whose ZRANGE AND SMEMBERS both WRONGTYPE
/// (a genuine non-zset/non-set), or a shard-unavailable degradation, short-circuits to `Err`
/// so the caller aborts BEFORE any store write (no partial). Each source carries its pairs
/// (the WEIGHTS factor is applied later by [`zset_combine`], so the gather is weight-free).
async fn gather_agg_sources(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    keys: &[bytes::Bytes],
) -> Result<Vec<Vec<ScoredMember>>, Value> {
    let mut sources: Vec<Vec<ScoredMember>> = Vec::with_capacity(keys.len());
    for k in keys {
        sources.push(gather_zset_pairs(inbox, ctx, home, db, k).await?);
    }
    Ok(sources)
}

/// Pair each gathered source's pairs with its WEIGHTS factor IN ORDER, then delegate to the
/// PURE [`zset_combine`] (the one source of truth the single-shard handler also calls).
fn combine_with_weights(
    op: AggOp,
    args: &AggArgs,
    sources: Vec<Vec<ScoredMember>>,
) -> Vec<ScoredMember> {
    let with_weights: Vec<WeightedSource> = sources
        .into_iter()
        .enumerate()
        .map(|(i, pairs)| (pairs, args.weights[i]))
        .collect();
    zset_combine(op, args.aggregate, &with_weights)
}

/// The [`AggOp`] for a READ-form zset command (ZUNION/ZINTER/ZDIFF). The caller guarantees
/// the token is one of the three (the serve loop's `is_fan_out_spanning_combine` gate).
fn read_agg_op(cmd_upper: &[u8]) -> AggOp {
    match cmd_upper {
        b"ZUNION" => AggOp::Union,
        b"ZDIFF" => AggOp::Diff,
        _ => AggOp::Inter, // ZINTER (and the gated default)
    }
}

/// The [`AggOp`] for a STORE-form zset command (ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE). The
/// caller guarantees the token is one of the three.
fn store_agg_op(cmd_upper: &[u8]) -> AggOp {
    match cmd_upper {
        b"ZUNIONSTORE" => AggOp::Union,
        b"ZDIFFSTORE" => AggOp::Diff,
        _ => AggOp::Inter, // ZINTERSTORE
    }
}

/// A parsed aggregation request: the source keys, the per-source WEIGHTS, the AGGREGATE
/// function, and the WITHSCORES flag (the read forms). MIRRORS cmd_zset's private `AggArgs`.
struct AggArgs {
    keys: Vec<bytes::Bytes>,
    weights: Vec<f64>,
    aggregate: Aggregate,
    with_scores: bool,
}

/// Parse the `numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]`
/// grammar shared by ZUNION/ZINTER/ZDIFF and their STORE forms, on the home core. This MIRRORS
/// the single-shard cmd_zset `parse_agg_args` (~:1591) EXACTLY (the same error catalog entries
/// and ordering), so a spanning command's argument errors are byte-identical to the
/// single-shard ones. `allow_weights` is false for ZDIFF/ZDIFFSTORE (no WEIGHTS/AGGREGATE);
/// `allow_withscores` is false for the *STORE forms; `numkeys_at` is the arg index of
/// `numkeys` (1 for the read forms, 2 for the STORE forms with a leading dest).
fn parse_agg_args(
    req: &Request,
    numkeys_at: usize,
    allow_weights: bool,
    allow_withscores: bool,
) -> Result<AggArgs, Value> {
    use ironcache_protocol::ErrorReply;
    // Arity: the read forms need >= numkeys + 1 args (cmd, numkeys, >=1 key); the store forms
    // need the dest too. The single-shard handlers enforce a minimum before parsing; mirror it.
    let min_len = numkeys_at + 2; // numkeys arg + at least one key after it.
    if req.args.len() < min_len {
        // cmd_zset surfaces wrong_arity on the command name; replicate via the per-command
        // arity check upstream. Defensive floor here: a syntax/arity-shaped short request.
        return Err(Value::error(ErrorReply::wrong_arity(
            &String::from_utf8_lossy(req.command()).to_lowercase(),
        )));
    }
    let Some(numkeys) = parse_i64(&req.args[numkeys_at]) else {
        return Err(Value::error(ErrorReply::not_an_integer()));
    };
    if numkeys <= 0 {
        return Err(Value::error(ErrorReply::numkeys_should_be_positive()));
    }
    let numkeys = numkeys as usize;
    let keys_start = numkeys_at + 1;
    if keys_start + numkeys > req.args.len() {
        return Err(Value::error(ErrorReply::numkeys_greater_than_args()));
    }
    let keys: Vec<bytes::Bytes> = req.args[keys_start..keys_start + numkeys].to_vec();
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = Aggregate::Sum;
    let mut with_scores = false;
    let mut i = keys_start + numkeys;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"WEIGHTS" if allow_weights => {
                if i + 1 + numkeys > req.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                for (k, slot) in weights.iter_mut().enumerate() {
                    let Some(w) = parse_f64_arg(&req.args[i + 1 + k]) else {
                        return Err(Value::error(ErrorReply::weight_not_a_float()));
                    };
                    *slot = w;
                }
                i += 1 + numkeys;
            }
            b"AGGREGATE" if allow_weights => {
                if i + 1 >= req.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                aggregate = match ascii_upper(&req.args[i + 1]).as_slice() {
                    b"SUM" => Aggregate::Sum,
                    b"MIN" => Aggregate::Min,
                    b"MAX" => Aggregate::Max,
                    _ => return Err(Value::error(ErrorReply::syntax_error())),
                };
                i += 2;
            }
            b"WITHSCORES" if allow_withscores => {
                with_scores = true;
                i += 1;
            }
            _ => return Err(Value::error(ErrorReply::syntax_error())),
        }
    }
    Ok(AggArgs {
        keys,
        weights,
        aggregate,
        with_scores,
    })
}

/// Parse `ZINTERCARD numkeys key [key ...] [LIMIT n]` on the home core, returning the source
/// keys and the LIMIT (`0` = unlimited), or an error [`Value`]. MIRRORS the single-shard
/// `cmd_zintercard` parse EXACTLY (the same error catalog entries and ordering).
fn parse_zintercard(req: &Request) -> Result<(Vec<bytes::Bytes>, usize), Value> {
    use ironcache_protocol::ErrorReply;
    if req.args.len() < 3 {
        return Err(Value::error(ErrorReply::wrong_arity("zintercard")));
    }
    let Some(numkeys) = parse_i64(&req.args[1]) else {
        return Err(Value::error(ErrorReply::not_an_integer()));
    };
    if numkeys <= 0 {
        return Err(Value::error(ErrorReply::numkeys_should_be_positive()));
    }
    let numkeys = numkeys as usize;
    if 2 + numkeys > req.args.len() {
        return Err(Value::error(ErrorReply::numkeys_greater_than_args()));
    }
    let keys: Vec<bytes::Bytes> = req.args[2..2 + numkeys].to_vec();
    let mut limit: usize = 0;
    let mut i = 2 + numkeys;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"LIMIT" => {
                if i + 1 >= req.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                match parse_i64(&req.args[i + 1]) {
                    Some(n) if n < 0 => {
                        return Err(Value::error(ErrorReply::limit_cant_be_negative()));
                    }
                    Some(n) => limit = n as usize,
                    None => return Err(Value::error(ErrorReply::not_an_integer())),
                }
                i += 2;
            }
            _ => return Err(Value::error(ErrorReply::syntax_error())),
        }
    }
    Ok((keys, limit))
}

/// Build the ZUNION/ZINTER/ZDIFF reply from the combined `(member, score)` pairs: with
/// WITHSCORES a [`Value::Pairs`] of `(member-bulk, Value::Double)` (RESP3-nested / RESP2-flat,
/// like ZRANGE WITHSCORES); without it a plain array of member bulks. Mirrors cmd_zset's
/// `members_reply` so the wire shape is byte-identical to the single-shard reply.
fn zset_members_reply(pairs: Vec<ScoredMember>, with_scores: bool) -> Value {
    if with_scores {
        Value::Pairs(
            pairs
                .into_iter()
                .map(|(m, s)| {
                    (
                        Value::BulkString(Some(bytes::Bytes::from(m))),
                        Value::Double(s),
                    )
                })
                .collect(),
        )
    } else {
        Value::Array(Some(
            pairs
                .into_iter()
                .map(|(m, _)| Value::BulkString(Some(bytes::Bytes::from(m))))
                .collect(),
        ))
    }
}

/// Validate the `ZRANGESTORE dst src start stop [opts]` OPTION grammar on the home core,
/// mirroring the single-shard `cmd_zrangestore` (cmd_zset.rs) EXACTLY so a malformed spanning
/// ZRANGESTORE returns the byte-identical error. Accepts ONLY BYSCORE/BYLEX/REV/LIMIT (the
/// options are at `args[5..]`); rejects WITHSCORES and any unknown token with `syntax_error`,
/// BYSCORE+BYLEX with `syntax_error`, LIMIT without BY* with `zrange_limit_only_with_byscore_or_bylex`,
/// LIMIT with a missing operand with `syntax_error`, and a non-integer LIMIT operand with
/// `not_an_integer` (via the same canonical `parse_i64`). Returns `Ok(by_lex)` on success.
fn validate_zrangestore_opts(request: &Request) -> Result<bool, Value> {
    use ironcache_protocol::ErrorReply;
    let mut by_score = false;
    let mut by_lex = false;
    let mut has_limit = false;
    let mut i = 5;
    while i < request.args.len() {
        match ascii_upper(&request.args[i]).as_slice() {
            b"BYSCORE" => by_score = true,
            b"BYLEX" => by_lex = true,
            b"REV" => {}
            b"LIMIT" => {
                if i + 2 >= request.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                if parse_i64(&request.args[i + 1]).is_none()
                    || parse_i64(&request.args[i + 2]).is_none()
                {
                    return Err(Value::error(ErrorReply::not_an_integer()));
                }
                has_limit = true;
                i += 2;
            }
            _ => return Err(Value::error(ErrorReply::syntax_error())),
        }
        i += 1;
    }
    if by_score && by_lex {
        return Err(Value::error(ErrorReply::syntax_error()));
    }
    if has_limit && !(by_score || by_lex) {
        return Err(Value::error(
            ErrorReply::zrange_limit_only_with_byscore_or_bylex(),
        ));
    }
    Ok(by_lex)
}

/// The home-core GATHER + STORE for a SHARD-SPANNING `ZRANGESTORE dst src start stop [opts]`
/// (COORDINATOR.md #107, Stage 2b-2): a 2-key copy-range. Gather the SELECTED range from the
/// `src` owner (route the SAME range args with WITHSCORES; for a BYLEX range, where WITHSCORES
/// is illegal, gather the members then look their scores up from `src`'s full zset), then write
/// the result to the `dst` owner via [`store_zset_result`] (empty -> DEL dst, reply 0; else
/// __ICSTOREZSET, reply the stored count). Matches the single-shard `cmd_zrangestore`.
async fn fan_out_zrangestore(
    inbox: &Inbox,
    ctx: &ServerContext,
    request: &Request,
    db: u32,
    home: ShardId,
) -> Value {
    use ironcache_protocol::ErrorReply;
    // ZRANGESTORE dst src start stop [opts]: at least dst + src + start + stop.
    if request.args.len() < 5 {
        return Value::error(ErrorReply::wrong_arity("zrangestore"));
    }
    let dst = request.args[1].clone();
    let src = request.args[2].clone();
    // Validate the ZRANGESTORE option grammar ON THE HOME CORE, mirroring the single-shard
    // `cmd_zrangestore` EXACTLY. ZRANGESTORE accepts ONLY BYSCORE/BYLEX/REV/LIMIT; WITHSCORES
    // (a legal ZRANGE option but NOT a ZRANGESTORE one) and any unknown token are a syntax
    // error. Without this, the non-BYLEX path below forwards the tail to `ZRANGE ... WITHSCORES`,
    // and ZRANGE accepts a stray WITHSCORES, so a spanning `ZRANGESTORE dst src 0 -1 WITHSCORES`
    // would SUCCEED while the single-shard form errors (a parity break). The bounds (start/stop,
    // LIMIT integers) are still validated by the owner's ZRANGE when forwarded; here we only gate
    // the option tokens. Returns by_lex (to pick the gather path).
    let is_bylex = match validate_zrangestore_opts(request) {
        Ok(by_lex) => by_lex,
        Err(e) => return e,
    };

    let gathered = if is_bylex {
        match gather_zrange_bylex(inbox, ctx, home, db, &src, request).await {
            Ok(pairs) => pairs,
            Err(e) => return e,
        }
    } else {
        // ZRANGE src start stop [opts] WITHSCORES, routed to the src owner. A WRONGTYPE (src is
        // a non-zset) aborts BEFORE the dst write; a missing src -> empty -> dst deleted.
        let mut args: Vec<bytes::Bytes> = Vec::with_capacity(request.args.len());
        args.push(bytes::Bytes::from_static(b"ZRANGE"));
        args.push(src.clone());
        args.extend(request.args[3..].iter().cloned());
        args.push(bytes::Bytes::from_static(b"WITHSCORES"));
        let value = route_to_owner(inbox, ctx, home, db, &src, &Request { args }).await;
        match value {
            // A syntax/range error from the src ZRANGE (bad bound, BYSCORE+BYLEX, LIMIT without
            // BY*, etc.) or a WRONGTYPE is surfaced as-is BEFORE any dst write.
            e @ Value::Error(_) => return e,
            other => pairs_of(other),
        }
    };

    store_zset_result(inbox, ctx, home, db, &dst, gathered).await
}

/// Gather a BYLEX ZRANGESTORE's selected range from the `src` owner WITH the real scores.
/// `ZRANGE ... BYLEX` cannot carry WITHSCORES, so route the BYLEX selection (members only),
/// then route a full `ZRANGE src 0 -1 WITHSCORES` to build a member->score map and attach each
/// selected member's score (matching the single-shard `read_range_pairs` BYLEX path, which
/// re-reads each member's score). A WRONGTYPE on either sub-ZRANGE aborts with that error.
async fn gather_zrange_bylex(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    src: &[u8],
    request: &Request,
) -> Result<Vec<ScoredMember>, Value> {
    // The BYLEX selection (members in the selected order), no WITHSCORES.
    let mut sel_args: Vec<bytes::Bytes> = Vec::with_capacity(request.args.len());
    sel_args.push(bytes::Bytes::from_static(b"ZRANGE"));
    sel_args.push(bytes::Bytes::copy_from_slice(src));
    sel_args.extend(request.args[3..].iter().cloned());
    let sel = route_to_owner(inbox, ctx, home, db, src, &Request { args: sel_args }).await;
    let members = match sel {
        e @ Value::Error(_) => return Err(e),
        Value::Array(Some(items)) => items.into_iter().filter_map(bulk_bytes).collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    if members.is_empty() {
        return Ok(Vec::new());
    }
    // The full zset's scores, to look each selected member's real score up.
    let score_map = gather_zset_pairs(inbox, ctx, home, db, src).await?;
    let lookup: std::collections::HashMap<Vec<u8>, f64> = score_map.into_iter().collect();
    Ok(members
        .into_iter()
        .map(|m| {
            let s = lookup.get(&m).copied().unwrap_or(0.0);
            (m, s)
        })
        .collect())
}

/// Parse an `f64` WEIGHTS value the way cmd_zset's `parse_f64` does (no surrounding
/// whitespace, NaN rejected, infinities allowed). Local mirror so the spanning WEIGHTS parse
/// is byte-identical to the single-shard one.
fn parse_f64_arg(b: &[u8]) -> Option<f64> {
    if b.is_empty() {
        return None;
    }
    if b[0].is_ascii_whitespace() || b[b.len() - 1].is_ascii_whitespace() {
        return None;
    }
    let v: f64 = std::str::from_utf8(b).ok()?.parse().ok()?;
    if v.is_nan() { None } else { Some(v) }
}

// ===========================================================================
// BITOP + HyperLogLog (COORDINATOR.md #107, coordinator Stage 2b-3).
//
// BITOP op dest src... (write), PFCOUNT key... (read), PFMERGE dest src... (write) when
// their keys SPAN shards. Same design as the set/zset algebra above: GATHER each source's
// RAW STRING bytes from its owner shard (route `GET key`), COMBINE with a PURE function
// SHARED with the single-shard handler (the one source of truth so cross-shard and
// single-shard results CANNOT drift) -- [`bitop_compute`] for BITOP, the
// [`merge_into`]/[`hll_from_regs`]/[`regs_reghisto`]/[`estimate_reply`] register-array ops
// for the HLLs -- and for the WRITE forms WRITE the result to the dest owner. BITOP's dest
// write reuses a plain routed `SET dest <bytes>` (SET clears the dest TTL by default, which
// is exactly BITOP's blind-overwrite-clear-TTL); PFMERGE's dest write uses the internal
// [`ironcache_server::ICSTOREHLL`] verb (TTL-PRESERVING, the one semantic that differs from
// a plain SET and from the set/zset *STORE verbs).
//
// GATHER fidelity: a present STRING -> its bytes; a missing key -> None (BITOP treats a
// missing source as an empty string the zero-pad covers; PFCOUNT/PFMERGE skip it); a
// NON-STRING -> WRONGTYPE (a `GET` on a non-string replies WRONGTYPE) -> ABORT the whole
// command BEFORE any write. For the HLLs a PRESENT string is additionally validated as a
// valid HLL (dense OR sparse) on the HOME core (`is_valid_hll`); an invalid one ->
// hll_invalid_value -> abort. All sources are validated FIRST, so a write never partially mutates.
// ===========================================================================

/// GATHER one key's RAW STRING bytes from its OWNER shard by routing `GET key` (the home
/// subset runs LOCALLY + synchronously, remote keys hop), returning:
/// - `Ok(Some(bytes))` for a present STRING;
/// - `Ok(None)` for a MISSING key (`GET` replies a null) -- the caller treats it as an
///   empty BITOP source / a skipped HLL;
/// - `Err(error)` for an `Error` reply (a WRONGTYPE on a non-string, OR a shard-unavailable
///   degradation): the caller ABORTS the whole command with that error before any write.
///
/// `run_local_keyed` returns before any `.await` and `dispatch_one_value` holds no home-core
/// borrow across its hop, so this respects the no-borrow-across-await contract.
async fn gather_string_bytes(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    key: &[u8],
) -> Result<Option<Vec<u8>>, Value> {
    let get = Request {
        args: vec![
            bytes::Bytes::from_static(b"GET"),
            bytes::Bytes::copy_from_slice(key),
        ],
    };
    let value = route_to_owner(inbox, ctx, home, db, key, &get).await;
    match value {
        // A present string -> its raw bytes (an HLL object is a string).
        Value::BulkString(Some(b)) => Ok(Some(b.to_vec())),
        // A WRONGTYPE (the key is a non-string) or a shard-unavailable degradation -> abort.
        e @ Value::Error(_) => Err(e),
        // A missing key (Null) -- and defensively any other shape -> no contribution.
        _ => Ok(None),
    }
}

/// The home-core ARITY/OP check + GATHER + (shared) COMBINE + STORE for a SHARD-SPANNING
/// `BITOP op dest src [src ...]` (COORDINATOR.md #107, Stage 2b-3), encoding the reply into
/// `out`. The serve loop calls this when BITOP's keys (dest + sources) span shards; the
/// co-located case routes via Stage 1.
///
/// MIRRORS the single-shard `cmd_bitop` EXACTLY: it validates the op + per-op source count
/// through the SHARED `bitop_validate_op` (AND/OR/XOR/ONE take >= 1 source, NOT takes exactly
/// one, DIFF/DIFF1/ANDOR take at least two, an unknown op is a syntax error); every source is
/// read + type-validated FIRST (a non-string aborts with WRONGTYPE, dest untouched); the
/// result is [`bitop_compute`] (the SHARED combiner, zero-extending to the
/// longest source); an EMPTY result routes `DEL dest` and replies 0 (BITOP deletes dest on
/// empty); a non-empty result routes `SET dest <bytes>` (a blind overwrite CLEARING the dest
/// TTL, exactly BITOP's dest write) and replies the result length in BYTES.
#[allow(clippy::too_many_arguments)]
pub async fn fan_out_bitop(
    inbox: &Inbox,
    ctx: &ServerContext,
    request: &Request,
    db: u32,
    home: ShardId,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    use ironcache_protocol::ErrorReply;
    let reply = 'reply: {
        // BITOP op dest src [src ...]: at least op + dest + one source (Min(4)).
        if request.args.len() < 4 {
            break 'reply Value::error(ErrorReply::wrong_arity("bitop"));
        }
        let op = request.args[1].to_ascii_uppercase();
        // The op allow-list and per-op source-count rule come from the SHARED validator in
        // `cmd_bitop`, so this spanning path cannot drift from the single-shard one (#414
        // review). The arity check runs on the home core BEFORE any gather, matching cmd_bitop.
        if let Err(e) = bitop_validate_op(op.as_slice(), request.args.len() - 3) {
            break 'reply Value::error(e);
        }
        let dest = request.args[2].clone();
        let src_keys: Vec<bytes::Bytes> = request.args[3..].to_vec();

        // GATHER every source's bytes, validating ALL first: a missing source is an empty
        // string (the zero-pad in bitop_compute covers it), a non-string aborts WRONGTYPE
        // BEFORE any dest write (dest untouched).
        let mut sources: Vec<Vec<u8>> = Vec::with_capacity(src_keys.len());
        for k in &src_keys {
            match gather_string_bytes(inbox, ctx, home, db, k).await {
                Ok(Some(b)) => sources.push(b),
                Ok(None) => sources.push(Vec::new()),
                Err(e) => break 'reply e,
            }
        }

        // COMBINE via the SHARED pure combiner, then STORE to the dest owner.
        let result = bitop_compute(op.as_slice(), &sources);
        store_bitop_result(inbox, ctx, home, db, &dest, result).await
    };
    encode_into(out, &reply, proto);
}

/// Write the spanning BITOP result to `dest`'s OWNER shard with the EXACT single-shard
/// blind-overwrite-CLEARING-TTL semantics (COORDINATOR.md #107, Stage 2b-3), returning the
/// integer reply [`Value`]:
/// - EMPTY result -> route `DEL dest` (BITOP deletes dest on an empty result) and reply
///   `Integer(0)`.
/// - non-empty result -> route `SET dest <bytes>` (a plain SET clears the dest TTL by
///   default, exactly BITOP's dest write -- so NO internal verb is needed) and reply the
///   result LENGTH in bytes.
///
/// A shard-unavailable degradation on the dest write is surfaced (the result was NOT stored).
async fn store_bitop_result(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    dest: &[u8],
    result: Vec<u8>,
) -> Value {
    let len = result.len() as i64;
    let subreq = if result.is_empty() {
        // BITOP deletes the destination on an empty result. DEL routes to the dest owner like
        // any keyed command; its 0/1 reply is discarded -- BITOP replies the byte length (0).
        Request {
            args: vec![
                bytes::Bytes::from_static(b"DEL"),
                bytes::Bytes::copy_from_slice(dest),
            ],
        }
    } else {
        // Blind overwrite via a plain SET (TTL CLEARED by default -- the EXACT BITOP dest
        // write). SET dest <result-bytes>.
        Request {
            args: vec![
                bytes::Bytes::from_static(b"SET"),
                bytes::Bytes::copy_from_slice(dest),
                bytes::Bytes::from(result),
            ],
        }
    };
    let write_reply = route_to_owner(inbox, ctx, home, db, dest, &subreq).await;
    match write_reply {
        Value::Error(e) => Value::Error(e),
        _ if len == 0 => Value::Integer(0),
        _ => Value::Integer(len),
    }
}

/// GATHER one HLL key's registers from its OWNER shard into `max_regs` (per-register max),
/// validating on the HOME core (COORDINATOR.md #107, Stage 2b-3). Route `GET key`: a present
/// STRING is validated as a valid HLL (dense OR sparse) via [`is_valid_hll`] (an invalid one
/// -> hll_invalid_value abort) then unioned with the SHARED [`merge_into`]; a missing key
/// contributes nothing; a non-string -> WRONGTYPE abort. Returns `Ok(())` on a clean gather,
/// or `Err(error)` to abort the whole command BEFORE any write.
async fn gather_hll_into(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    key: &[u8],
    max_regs: &mut [u8; HLL_REGISTERS],
) -> Result<(), Value> {
    use ironcache_protocol::ErrorReply;
    match gather_string_bytes(inbox, ctx, home, db, key).await? {
        Some(bytes) => {
            // Validate on the home core with the EXACT single-shard check; an invalid HLL
            // (dense OR sparse) aborts before any union/write (matching cmd_pfcount/
            // cmd_pfmerge). `merge_into` then dispatches on the gathered object's encoding.
            if !is_valid_hll(&bytes) {
                return Err(Value::error(ErrorReply::hll_invalid_value()));
            }
            merge_into(max_regs, &bytes);
            Ok(())
        }
        // A missing key contributes nothing to the union.
        None => Ok(()),
    }
}

/// The home-core GATHER + (shared) UNION + ESTIMATE for a SHARD-SPANNING `PFCOUNT key
/// [key ...]` (COORDINATOR.md #107, Stage 2b-3), encoding the integer reply into `out`. The
/// serve loop calls this when PFCOUNT's keys span shards; the co-located case routes via
/// Stage 1.
///
/// READ-ONLY (it NEVER writes -- no cross-shard cache-header update, matching the single
/// -shard PFCOUNT). UNION the registers across every present valid HLL (a missing key
/// contributes nothing; a non-string -> WRONGTYPE; a present-but-invalid string ->
/// hll_invalid_value -- all validated FIRST), then estimate via the SHARED
/// [`regs_reghisto`] + [`estimate_reply`], so the cross-shard count is the SAME as the
/// single-shard one.
pub async fn fan_out_pfcount(
    inbox: &Inbox,
    ctx: &ServerContext,
    request: &Request,
    db: u32,
    home: ShardId,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    use ironcache_protocol::ErrorReply;
    let reply = 'reply: {
        // PFCOUNT key [key ...]: at least one key (Min(2)).
        if request.args.len() < 2 {
            break 'reply Value::error(ErrorReply::wrong_arity("pfcount"));
        }
        // HEAP-allocate the 16384-byte working register array so it does NOT inflate this
        // (awaited) future's stack frame (the single-shard handler keeps it on the stack, but
        // it never awaits; here a 16 KiB array in the future trips clippy::large_futures and
        // bloats every enclosing future). The union + estimate are otherwise identical.
        let mut max_regs = Box::new([0u8; HLL_REGISTERS]);
        for key in &request.args[1..] {
            if let Err(e) = gather_hll_into(inbox, ctx, home, db, key, &mut max_regs).await {
                break 'reply e;
            }
        }
        let reghisto = regs_reghisto(&max_regs);
        Value::Integer(estimate_reply(&reghisto))
    };
    encode_into(out, &reply, proto);
}

/// The home-core GATHER + (shared) UNION + (TTL-preserving) STORE for a SHARD-SPANNING
/// `PFMERGE dest src [src ...]` (COORDINATOR.md #107, Stage 2b-3), encoding the `+OK` reply
/// into `out`. The serve loop calls this when PFMERGE's keys span shards; the co-located
/// case routes via Stage 1.
///
/// MIRRORS the single-shard `cmd_pfmerge` EXACTLY: the dest counts as BOTH a source (its
/// current registers join the union) and the write target, so gather the dest + every source
/// FIRST (a non-string -> WRONGTYPE, a present-but-invalid -> hll_invalid_value -- validated
/// before any write); UNION via the SHARED [`merge_into`]; build the merged object via the
/// SHARED [`hll_from_regs`] (sparse when it fits, else dense -- the SAME encoding the
/// single-shard handler writes); write it to the dest owner via the internal
/// [`ironcache_server::ICSTOREHLL`] verb, which PRESERVES the dest's existing TTL (the one
/// semantic that differs from a plain SET) and NEVER deletes (an empty merge still ensures an
/// empty HLL at the dest). Reply `+OK`.
pub async fn fan_out_pfmerge(
    inbox: &Inbox,
    ctx: &ServerContext,
    request: &Request,
    db: u32,
    home: ShardId,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    use ironcache_protocol::ErrorReply;
    let reply = 'reply: {
        // PFMERGE dest [src ...]: at least the dest (Min(2)). The dest is args[1] and joins
        // the union as both a source and the target.
        if request.args.len() < 2 {
            break 'reply Value::error(ErrorReply::wrong_arity("pfmerge"));
        }
        let dest = request.args[1].clone();
        // GATHER dest + every source into the union, validating ALL first (so a WRONGTYPE /
        // invalid-HLL on ANY input aborts before the dest write -- no partial merge). The
        // 16384-byte register array is HEAP-allocated to keep this awaited future small
        // (see fan_out_pfcount).
        let mut max_regs = Box::new([0u8; HLL_REGISTERS]);
        for key in &request.args[1..] {
            if let Err(e) = gather_hll_into(inbox, ctx, home, db, key, &mut max_regs).await {
                break 'reply e;
            }
        }
        // Build the merged object (sparse when it fits, else dense) and write it
        // TTL-PRESERVINGLY to the dest owner via the internal verb. PFMERGE never deletes on
        // empty: an empty union still writes an (empty, hence sparse) HLL, matching the
        // single-shard handler and its encoding exactly.
        let merged = hll_from_regs(&max_regs);
        store_hll_result(inbox, ctx, home, db, &dest, merged).await
    };
    encode_into(out, &reply, proto);
}

/// Write the spanning PFMERGE merged dense HLL `obj` to `dest`'s OWNER shard via the internal
/// `__ICSTOREHLL dest <obj>` verb (COORDINATOR.md #107, Stage 2b-3), which uses
/// `RmwAction::Replace` + `ExpireWrite::Unchanged` -- the EXACT single-shard PFMERGE write,
/// PRESERVING any existing dest TTL and creating a fresh (TTL-less) dest when vacant. The
/// verb replies `+OK`; a shard-unavailable degradation is surfaced (the merge was NOT
/// stored).
async fn store_hll_result(
    inbox: &Inbox,
    ctx: &ServerContext,
    home: ShardId,
    db: u32,
    dest: &[u8],
    obj: Vec<u8>,
) -> Value {
    let subreq = Request {
        args: vec![
            bytes::Bytes::copy_from_slice(ironcache_server::ICSTOREHLL),
            bytes::Bytes::copy_from_slice(dest),
            bytes::Bytes::from(obj),
        ],
    };
    match route_to_owner(inbox, ctx, home, db, dest, &subreq).await {
        // Surface a shard-unavailable degradation; otherwise echo the verb's +OK.
        e @ Value::Error(_) => e,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_protocol::ErrorReply;

    fn bytes(parts: &[&[u8]]) -> Vec<bytes::Bytes> {
        parts
            .iter()
            .map(|p| bytes::Bytes::copy_from_slice(p))
            .collect()
    }

    #[test]
    fn members_of_extracts_set_and_array_members() {
        let set = Value::Set(vec![
            Value::BulkString(Some(bytes::Bytes::from_static(b"a"))),
            Value::BulkString(Some(bytes::Bytes::from_static(b"b"))),
        ]);
        assert_eq!(members_of(set).unwrap(), vec![b"a".to_vec(), b"b".to_vec()]);
        // RESP2 degrades a set to an array; both must parse.
        let arr = Value::Array(Some(vec![Value::BulkString(Some(
            bytes::Bytes::from_static(b"x"),
        ))]));
        assert_eq!(members_of(arr).unwrap(), vec![b"x".to_vec()]);
        // A missing key (empty set) -> empty members.
        assert!(members_of(Value::Set(Vec::new())).unwrap().is_empty());
    }

    #[test]
    fn members_of_aborts_on_error() {
        let e = Value::error(ErrorReply::wrong_type());
        assert!(matches!(members_of(e), Err(Value::Error(_))));
    }

    #[test]
    fn read_and_store_op_mapping() {
        assert_eq!(read_op(b"SINTER"), SetOp::Inter);
        assert_eq!(read_op(b"SUNION"), SetOp::Union);
        assert_eq!(read_op(b"SDIFF"), SetOp::Diff);
        assert_eq!(store_op(b"SINTERSTORE"), SetOp::Inter);
        assert_eq!(store_op(b"SUNIONSTORE"), SetOp::Union);
        assert_eq!(store_op(b"SDIFFSTORE"), SetOp::Diff);
    }

    #[test]
    fn parse_sintercard_keys_and_limit() {
        // SINTERCARD 2 a b LIMIT 5 -> keys [a,b], limit 5.
        let req = Request {
            args: bytes(&[b"SINTERCARD", b"2", b"a", b"b", b"LIMIT", b"5"]),
        };
        let (keys, limit) = parse_sintercard(&req).unwrap();
        assert_eq!(keys, bytes(&[b"a", b"b"]));
        assert_eq!(limit, 5);

        // No LIMIT -> 0 (unlimited).
        let req = Request {
            args: bytes(&[b"SINTERCARD", b"1", b"a"]),
        };
        let (keys, limit) = parse_sintercard(&req).unwrap();
        assert_eq!(keys, bytes(&[b"a"]));
        assert_eq!(limit, 0);

        // numkeys 0 -> error.
        let req = Request {
            args: bytes(&[b"SINTERCARD", b"0", b"a"]),
        };
        assert!(matches!(parse_sintercard(&req), Err(Value::Error(_))));

        // numkeys > available keys -> error.
        let req = Request {
            args: bytes(&[b"SINTERCARD", b"3", b"a", b"b"]),
        };
        assert!(matches!(parse_sintercard(&req), Err(Value::Error(_))));

        // A negative LIMIT -> error.
        let req = Request {
            args: bytes(&[b"SINTERCARD", b"1", b"a", b"LIMIT", b"-1"]),
        };
        assert!(matches!(parse_sintercard(&req), Err(Value::Error(_))));
    }
}
