// SPDX-License-Identifier: MIT OR Apache-2.0
//! Home-core GATHER + (shared) COMBINE + STORE for the SHARD-SPANNING set-algebra commands
//! (COORDINATOR.md #107, coordinator Stage 2b-1).
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
use ironcache_runtime::bootstrap::ShardId;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ProtoVersion, Request, SetOp, Value, owner_shard, set_combine};

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
/// core; mirrors the server's own token handling.
fn ascii_upper(token: &[u8]) -> Vec<u8> {
    token.to_ascii_uppercase()
}

/// Parse a decimal `i64` (the SINTERCARD numkeys / LIMIT). `None` on a non-numeric token.
/// Mirrors the server's `parse_i64` (the single-shard SINTERCARD uses the same parse).
fn parse_i64(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.trim().parse::<i64>().ok()
}

/// Encode `value` for `proto` and append to `out` (the home-core encode; mirrors the serve
/// loop / coordinator / multikey encode). Encoding stays on the home core with the home proto.
fn encode_into(out: &mut Vec<u8>, value: &Value, proto: ProtoVersion) {
    let mut bm = bytes::BytesMut::with_capacity(64);
    ironcache_protocol::encode(&mut bm, value, proto);
    out.extend_from_slice(&bm);
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
