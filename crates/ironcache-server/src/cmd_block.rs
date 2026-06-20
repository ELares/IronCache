// SPDX-License-Identifier: MIT OR Apache-2.0
//! BLOCKING list/zset command parsing + the non-blocking ATTEMPT (PROD-9 HA polish).
//!
//! The blocking commands (BLPOP / BRPOP / BLMOVE / BRPOPLPUSH / BLMPOP / BZPOPMIN /
//! BZPOPMAX / BZMPOP) first try the non-blocking op; only when it would have returned
//! nil/empty does the client PARK (the serve layer owns the parking, because it needs the
//! per-connection waker + the runtime timer seam, which this runtime-agnostic crate does
//! not depend on). This module is the SHARED, PURE half:
//!
//! - parse each blocking command's grammar (the SAME grammar as its non-blocking sibling
//!   plus a leading/trailing `timeout`), returning a [`BlockSpec`] (the timeout, the keys,
//!   the direction, the operation) or an [`ErrorReply`] for a malformed command;
//! - [`try_block_op`]: ATTEMPT the non-blocking op against the store and return either a
//!   ready [`Value`] reply (data present, an error, or a WRONGTYPE) to send immediately, or
//!   `None` (every key empty/absent) to signal the serve layer to PARK.
//!
//! The pop bodies REUSE the exact reply shapes the non-blocking siblings build (the
//! `[key, element]` BLPOP shape, the `[key, member, score]` BZPOPMIN shape, the BLMPOP /
//! BZMPOP `[key, [...]]` shapes), so a blocked pop that succeeds is byte-identical to the
//! non-blocking pop it stands in for. Keyspace notifications are recorded by the SAME store
//! mutation path (the rmw closures), so a blocking pop fires the same `lpop`/`rpop`/
//! `zpopmin`/`zpopmax` event as the non-blocking pop.

use crate::cmd_list::{MpopArgs, parse_mpop_args};
use crate::cmd_util::{ascii_upper, parse_f64};
use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{ExpireWrite, RmwAction, RmwEntry, RmwStep, Store, UnixMillis};

/// The blocking commands, recognized by the serve-layer router so it can PARK the
/// connection. A thin predicate (a single match) shared by the router's interception gate,
/// the in-MULTI no-block path, and the registry cross-check contract.
#[must_use]
pub fn is_blocking_command(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"BLPOP"
            | b"BRPOP"
            | b"BLMOVE"
            | b"BRPOPLPUSH"
            | b"BLMPOP"
            | b"BZPOPMIN"
            | b"BZPOPMAX"
            | b"BZMPOP"
            | b"WAIT"
    )
}

/// The maximum blocking `timeout` in MILLISECONDS we will arm a timer for. Redis caps the
/// timeout at a large finite value; a `0` timeout means "block forever" (we represent that
/// as `None`). A value past this (or a non-finite one) is rejected at parse time exactly
/// like Redis's `LLONG_MAX`-style overflow guard, so a parked client always has a bounded
/// timer or an honest forever-wait.
const MAX_TIMEOUT_MS: u128 = 100_000_000 * 1000; // ~3.1 years in ms, Redis's practical ceiling.

/// A parsed blocking timeout in MILLISECONDS. `None` is the Redis `0` "block forever"
/// sentinel (the serve layer parks with NO timer arm); `Some(ms)` arms a timer for `ms`.
pub type BlockTimeoutMs = Option<u64>;

/// Parse a blocking SECONDS timeout (BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZ* families) into
/// milliseconds. `0` -> `None` (block forever). A negative, non-numeric, NaN, or
/// out-of-range timeout is the byte-exact Redis error. The seconds value may be fractional
/// (Redis accepts `BLPOP k 0.1`).
fn parse_timeout_secs(arg: &[u8]) -> Result<BlockTimeoutMs, ErrorReply> {
    let secs = parse_f64(arg).ok_or_else(timeout_not_a_float)?;
    if secs.is_nan() || secs < 0.0 {
        // Redis: a negative timeout is "ERR timeout is negative".
        if secs < 0.0 {
            return Err(ErrorReply::err("timeout is negative"));
        }
        return Err(timeout_not_a_float());
    }
    if secs == 0.0 {
        return Ok(None);
    }
    let ms = (secs * 1000.0).round();
    if !ms.is_finite() || ms < 0.0 || ms as u128 > MAX_TIMEOUT_MS {
        return Err(timeout_not_a_float());
    }
    Ok(Some(ms as u64))
}

/// The Redis "timeout is not a float or out of range" reply.
fn timeout_not_a_float() -> ErrorReply {
    ErrorReply::err("timeout is not a float or out of range")
}

/// A no-write rmw step that returns `reply` (value untouched). The shared short-circuit for
/// the blocking pop bodies (WRONGTYPE, a skip sentinel).
fn keep(reply: Value) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply,
    }
}

/// The WRONGTYPE no-write step.
fn wrong_type() -> RmwStep<Value> {
    keep(Value::error(ErrorReply::wrong_type()))
}

fn bulk(bytes: Vec<u8>) -> Value {
    Value::BulkString(Some(Bytes::from(bytes)))
}

/// The parsed, validated form of a blocking command: its timeout, the operation, and the
/// keys the serve layer registers a waiter on. Built by [`parse_block`]; consumed by
/// [`try_block_op`] (the attempt) and by the serve layer (the park keys + the timer arm).
#[derive(Debug, Clone)]
pub struct BlockSpec {
    /// The blocking timeout (ms), or `None` for "block forever".
    pub timeout_ms: BlockTimeoutMs,
    /// The keys this command may block on, in PRIORITY order (Redis serves the first
    /// non-empty key). The serve layer registers a waiter on each (co-located case) and
    /// re-attempts [`try_block_op`] on a wake.
    pub keys: Vec<Vec<u8>>,
    /// The concrete operation + its parsed parameters.
    pub op: BlockOp,
}

impl BlockSpec {
    /// The keys the serve layer should register a blocking WAITER on (a subset of [`Self::keys`]).
    /// For the pop families this is every key (any key gaining an element makes the command ready).
    /// For BLMOVE / BRPOPLPUSH ([`BlockOp::Move`]) it is ONLY the SOURCE key (`keys[0]`): a Move
    /// blocks waiting for the source to become non-empty, so a push to the DESTINATION must NOT wake
    /// it (that would be a spurious re-park that also perturbs the destination key's waiter FIFO).
    /// The full [`Self::keys`] is still used by [`try_block_op`] (the move needs both src and dst);
    /// only the WAITER REGISTRATION is narrowed here. PROD-9 FIX3.
    #[must_use]
    pub fn wait_keys(&self) -> &[Vec<u8>] {
        match self.op {
            // A Move blocks only on the source; register on `keys[0]` alone. (`keys` always holds
            // [src, dst] for a Move, so the slice is non-empty; guard defensively all the same.)
            BlockOp::Move { .. } => &self.keys[..self.keys.len().min(1)],
            // Pop families: every key can make the command ready.
            _ => &self.keys,
        }
    }
}

/// The blocking OPERATION, carrying the per-command parsed parameters [`try_block_op`] needs.
#[derive(Debug, Clone)]
pub enum BlockOp {
    /// BLPOP / BRPOP: pop one element from the first non-empty list. `left` selects the end.
    Pop { left: bool },
    /// BLMOVE / BRPOPLPUSH: move one element src->dst. `keys` holds `[src, dst]`.
    Move { from_left: bool, to_left: bool },
    /// BLMPOP: pop up to `count` from the first non-empty list at the chosen end.
    LMPop { left: bool, count: usize },
    /// BZPOPMIN / BZPOPMAX: pop one extreme member from the first non-empty zset.
    ZPop { max: bool },
    /// BZMPOP: pop up to `count` extreme members from the first non-empty zset.
    ZMPop { max: bool, count: usize },
    /// WAIT: block until `numreplicas` replicas have acknowledged. It touches NO keys (so a
    /// [`BlockSpec`] carrying this op has empty `keys`); the serve layer polls the in-sync replica
    /// count under the timer seam rather than calling [`try_block_op`]. Carried in the enum so the
    /// serve layer can represent a WAIT park uniformly with the pop parks.
    Wait { numreplicas: u64 },
}

/// Parse + validate a blocking command (NOT `WAIT`, which the serve layer handles directly:
/// it touches no keys). Returns the [`BlockSpec`] on success, or the byte-exact error reply
/// for a malformed command (wrong arity, a bad timeout, a bad direction/COUNT). The serve
/// layer calls this ONCE; an `Err` is replied immediately and the client never parks.
pub fn parse_block(cmd_upper: &[u8], req: &Request) -> Result<BlockSpec, ErrorReply> {
    match cmd_upper {
        b"BLPOP" | b"BRPOP" => parse_bpop(req, cmd_upper == b"BLPOP", cmd_name(cmd_upper)),
        b"BLMOVE" => parse_blmove(req),
        b"BRPOPLPUSH" => parse_brpoplpush(req),
        b"BLMPOP" => parse_blmpop(req),
        b"BZPOPMIN" | b"BZPOPMAX" => {
            parse_bzpop(req, cmd_upper == b"BZPOPMAX", cmd_name(cmd_upper))
        }
        b"BZMPOP" => parse_bzmpop(req),
        // The router only calls this for `is_blocking_command` minus WAIT, so this is
        // unreachable in practice; treat any stray token as a wrong-arity defensive error.
        _ => Err(ErrorReply::wrong_arity(&String::from_utf8_lossy(cmd_upper))),
    }
}

/// The lowercase command name for arity errors.
fn cmd_name(cmd_upper: &[u8]) -> &'static str {
    match cmd_upper {
        b"BLPOP" => "blpop",
        b"BRPOP" => "brpop",
        b"BZPOPMIN" => "bzpopmin",
        b"BZPOPMAX" => "bzpopmax",
        _ => "blocking",
    }
}

/// `BLPOP key [key ...] timeout` / `BRPOP ...`: keys are args[1..len-1], the timeout is the
/// LAST arg. `left == true` for BLPOP (pop the head), `false` for BRPOP (pop the tail).
fn parse_bpop(req: &Request, left: bool, name: &str) -> Result<BlockSpec, ErrorReply> {
    // Minimum: command + 1 key + timeout = 3 args.
    if req.args.len() < 3 {
        return Err(ErrorReply::wrong_arity(name));
    }
    let timeout_ms = parse_timeout_secs(&req.args[req.args.len() - 1])?;
    let keys: Vec<Vec<u8>> = req.args[1..req.args.len() - 1]
        .iter()
        .map(|b| b.to_vec())
        .collect();
    Ok(BlockSpec {
        timeout_ms,
        keys,
        op: BlockOp::Pop { left },
    })
}

/// `BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout`.
fn parse_blmove(req: &Request) -> Result<BlockSpec, ErrorReply> {
    if req.args.len() != 6 {
        return Err(ErrorReply::wrong_arity("blmove"));
    }
    let from_left = parse_lr(&req.args[3])?;
    let to_left = parse_lr(&req.args[4])?;
    let timeout_ms = parse_timeout_secs(&req.args[5])?;
    let keys = vec![req.args[1].to_vec(), req.args[2].to_vec()];
    Ok(BlockSpec {
        timeout_ms,
        keys,
        op: BlockOp::Move { from_left, to_left },
    })
}

/// `BRPOPLPUSH src dst timeout` == `BLMOVE src dst RIGHT LEFT timeout`.
fn parse_brpoplpush(req: &Request) -> Result<BlockSpec, ErrorReply> {
    if req.args.len() != 4 {
        return Err(ErrorReply::wrong_arity("brpoplpush"));
    }
    let timeout_ms = parse_timeout_secs(&req.args[3])?;
    let keys = vec![req.args[1].to_vec(), req.args[2].to_vec()];
    Ok(BlockSpec {
        timeout_ms,
        keys,
        op: BlockOp::Move {
            from_left: false,
            to_left: true,
        },
    })
}

/// Parse a `LEFT`/`RIGHT` direction token (case-insensitive) into `from_left`.
fn parse_lr(arg: &[u8]) -> Result<bool, ErrorReply> {
    match ascii_upper(arg).as_slice() {
        b"LEFT" => Ok(true),
        b"RIGHT" => Ok(false),
        _ => Err(ErrorReply::syntax_error()),
    }
}

/// `BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n]`: the leading `timeout` is
/// stripped, then the REMAINDER is exactly the LMPOP grammar, so we reuse `parse_mpop_args`
/// on a synthesized `LMPOP numkeys key... LEFT|RIGHT [COUNT n]` view.
fn parse_blmpop(req: &Request) -> Result<BlockSpec, ErrorReply> {
    // Minimum: command + timeout + numkeys + 1 key + direction = 5 args.
    if req.args.len() < 5 {
        return Err(ErrorReply::wrong_arity("blmpop"));
    }
    let timeout_ms = parse_timeout_secs(&req.args[1])?;
    let inner = mpop_inner_request(req);
    let parsed: MpopArgs = parse_mpop_args(&inner, "blmpop", &[b"LEFT", b"RIGHT"])?;
    let left = parsed.direction == b"LEFT";
    Ok(BlockSpec {
        timeout_ms,
        keys: parsed.keys,
        op: BlockOp::LMPop {
            left,
            count: parsed.count,
        },
    })
}

/// `BZPOPMIN key [key ...] timeout` / `BZPOPMAX ...`: keys args[1..len-1], timeout LAST.
fn parse_bzpop(req: &Request, max: bool, name: &str) -> Result<BlockSpec, ErrorReply> {
    if req.args.len() < 3 {
        return Err(ErrorReply::wrong_arity(name));
    }
    let timeout_ms = parse_timeout_secs(&req.args[req.args.len() - 1])?;
    let keys: Vec<Vec<u8>> = req.args[1..req.args.len() - 1]
        .iter()
        .map(|b| b.to_vec())
        .collect();
    Ok(BlockSpec {
        timeout_ms,
        keys,
        op: BlockOp::ZPop { max },
    })
}

/// `BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT n]`: strip the leading timeout, then
/// reuse the ZMPOP grammar via `parse_mpop_args`.
fn parse_bzmpop(req: &Request) -> Result<BlockSpec, ErrorReply> {
    if req.args.len() < 5 {
        return Err(ErrorReply::wrong_arity("bzmpop"));
    }
    let timeout_ms = parse_timeout_secs(&req.args[1])?;
    let inner = mpop_inner_request(req);
    let parsed: MpopArgs = parse_mpop_args(&inner, "bzmpop", &[b"MIN", b"MAX"])?;
    let max = parsed.direction == b"MAX";
    Ok(BlockSpec {
        timeout_ms,
        keys: parsed.keys,
        op: BlockOp::ZMPop {
            max,
            count: parsed.count,
        },
    })
}

/// Build the LMPOP/ZMPOP "inner" request for the shared `parse_mpop_args`: the B*MPOP
/// command is `<CMD> timeout numkeys key... DIR [COUNT n]`, so the inner view is `<CMD>` (as
/// the token placeholder) followed by `args[2..]` (everything after the timeout). The token
/// `args[0]` is irrelevant to `parse_mpop_args` (it reads args[1] = numkeys onward).
fn mpop_inner_request(req: &Request) -> Request {
    let mut args: Vec<Bytes> = Vec::with_capacity(req.args.len() - 1);
    // Placeholder token (parse_mpop_args ignores args[0]).
    args.push(req.args[0].clone());
    args.extend(req.args[2..].iter().cloned());
    Request { args }
}

/// ATTEMPT the non-blocking operation against `store`. Returns:
/// - `Some(value)` -> a READY reply to send immediately (data found, OR a WRONGTYPE error):
///   the client does NOT park.
/// - `None` -> EVERY key was empty/absent: the serve layer PARKS the client (or, on the
///   final timeout, replies the nil-array via [`block_timeout_reply`]).
///
/// A WRONGTYPE on the first existing key is returned as a ready error (Redis: a blocking pop
/// against a wrong-type key errors immediately, it does not block). The pop bodies REUSE the
/// exact reply shapes + the store mutation path of the non-blocking siblings, so keyspace
/// notifications fire identically.
pub fn try_block_op<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    spec: &BlockSpec,
) -> Option<Value> {
    match &spec.op {
        BlockOp::Pop { left } => try_bpop(store, db, now, &spec.keys, *left),
        BlockOp::Move { from_left, to_left } => {
            try_bmove(store, db, now, &spec.keys, *from_left, *to_left)
        }
        BlockOp::LMPop { left, count } => try_blmpop(store, db, now, &spec.keys, *left, *count),
        BlockOp::ZPop { max } => try_bzpop(store, db, now, &spec.keys, *max),
        BlockOp::ZMPop { max, count } => try_bzmpop(store, db, now, &spec.keys, *max, *count),
        // WAIT touches no keys: it is parked on the replica-ack count by the serve layer and never
        // reaches this store-attempt path. Return `None` defensively (no pop result) if ever called.
        BlockOp::Wait { .. } => None,
    }
}

/// The reply a blocking command sends when its timeout elapses with no element delivered.
/// Every blocking pop family uses the Redis NULL ARRAY (RESP2 `*-1`, RESP3 `_`) on timeout.
#[must_use]
pub fn block_timeout_reply() -> Value {
    Value::Array(None)
}

/// The NON-BLOCKING dispatch entry for a blocking command (the EXEC-replay / in-MULTI path):
/// parse + ATTEMPT, replying the result if data is present (or a parse / WRONGTYPE error),
/// else the NIL-ARRAY timeout reply IMMEDIATELY (Redis: a blocking command inside MULTI/EXEC
/// behaves non-blocking and returns nil at once if empty). It NEVER parks -- the serve layer's
/// live path intercepts the blocking command BEFORE dispatch and does the parking; this arm is
/// reached ONLY via EXEC replay (and any direct dispatch with no serve-layer interception),
/// where blocking is not allowed. A malformed command replies the byte-exact parse error.
pub fn cmd_block_nonblocking<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    cmd_upper: &[u8],
    req: &Request,
) -> Value {
    let spec = match parse_block(cmd_upper, req) {
        Ok(s) => s,
        Err(e) => return Value::error(e),
    };
    // Data present (or a WRONGTYPE) -> that reply; every key empty -> the nil-array (no park).
    try_block_op(store, db, now, &spec).unwrap_or_else(block_timeout_reply)
}

/// BLPOP/BRPOP attempt: pop one element from the FIRST non-empty list (in `keys` order),
/// replying `[key, element]`. `None` if every key is empty/absent (park).
fn try_bpop<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    keys: &[Vec<u8>],
    left: bool,
) -> Option<Value> {
    for key in keys {
        let key_b = Bytes::copy_from_slice(key);
        let outcome = store.rmw_mut(db, &key_b, now, |entry| match entry {
            // Absent: skip (the Null sentinel keeps scanning).
            RmwEntry::Vacant => keep(Value::Null),
            RmwEntry::OccupiedMut(mut o) => {
                let Some(list) = o.as_list_mut() else {
                    return wrong_type();
                };
                let popped = if left {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                let Some(elem) = popped else {
                    // An empty list is never stored, so this is unreachable; skip defensively.
                    return keep(Value::Null);
                };
                let reply = Value::Array(Some(vec![bulk(key.clone()), bulk(elem)]));
                let action = if list.is_empty() {
                    RmwAction::Delete
                } else {
                    RmwAction::Mutated
                };
                RmwStep {
                    action,
                    expire: ExpireWrite::Unchanged,
                    reply,
                }
            }
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        });
        match outcome {
            // Skip a missing/empty key; return any real reply or WRONGTYPE.
            Value::Null => {}
            other => return Some(other),
        }
    }
    None
}

/// BLMOVE/BRPOPLPUSH attempt: move one element src->dst, replying the moved element. `None`
/// if src is absent/empty (park). A WRONGTYPE on src or dst is a ready error. Reuses the
/// non-blocking [`crate::cmd_list::cmd_lmove`] body shape via a synthesized LMOVE request so
/// the SOURCE-FIRST ordering + the dst-restore-on-WRONGTYPE are byte-identical.
fn try_bmove<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    keys: &[Vec<u8>],
    from_left: bool,
    to_left: bool,
) -> Option<Value> {
    let src = &keys[0];
    let dst = &keys[1];
    let from = if from_left {
        b"LEFT".as_slice()
    } else {
        b"RIGHT"
    };
    let to = if to_left {
        b"LEFT".as_slice()
    } else {
        b"RIGHT"
    };
    let lmove = Request {
        args: vec![
            Bytes::from_static(b"LMOVE"),
            Bytes::copy_from_slice(src),
            Bytes::copy_from_slice(dst),
            Bytes::copy_from_slice(from),
            Bytes::copy_from_slice(to),
        ],
    };
    let reply = crate::cmd_list::cmd_lmove(store, db, now, &lmove);
    match reply {
        // src absent/empty -> nil from LMOVE -> park.
        Value::Null => None,
        // The moved element, OR a WRONGTYPE error: ready.
        other => Some(other),
    }
}

/// BLMPOP attempt: the non-blocking LMPOP over `keys` at the chosen end, popping up to
/// `count`. `None` if every key is empty/absent (park).
fn try_blmpop<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    keys: &[Vec<u8>],
    left: bool,
    count: usize,
) -> Option<Value> {
    let dir = if left { b"LEFT".as_slice() } else { b"RIGHT" };
    let lmpop = synth_mpop(b"LMPOP", keys, dir, count);
    match crate::cmd_list::cmd_lmpop(store, db, now, &lmpop) {
        // Every key empty/absent -> the null array -> park.
        Value::Array(None) => None,
        other => Some(other),
    }
}

/// BZPOPMIN/BZPOPMAX attempt: pop one extreme member from the FIRST non-empty zset (in
/// `keys` order), replying `[key, member, score]`. `None` if every key is empty/absent.
// `member`/`max` read as similar to clippy but are distinct (the popped member vs the
// min/max direction flag); the names mirror the zset handler's spelling.
#[allow(clippy::similar_names)]
fn try_bzpop<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    keys: &[Vec<u8>],
    max: bool,
) -> Option<Value> {
    for key in keys {
        let key_b = Bytes::copy_from_slice(key);
        let outcome = store.rmw_mut(db, &key_b, now, |entry| match entry {
            RmwEntry::Vacant => keep(Value::Null),
            RmwEntry::OccupiedMut(mut o) => {
                let Some(zset) = o.as_zset_mut() else {
                    return wrong_type();
                };
                let popped = if max {
                    zset.pop_max(1)
                } else {
                    zset.pop_min(1)
                };
                let Some((member, score)) = popped.into_iter().next() else {
                    // An empty zset is never stored; skip defensively.
                    return keep(Value::Null);
                };
                // BZPOPMIN/BZPOPMAX reply is a FLAT 3-element [key, member, score] in BOTH
                // protocols (the score a RESP3 double / RESP2 bulk), unlike ZPOPMIN's
                // WITHSCORES nested pair shape.
                let reply = Value::Array(Some(vec![
                    bulk(key.clone()),
                    bulk(member),
                    Value::Double(score),
                ]));
                let action = if zset.is_empty() {
                    RmwAction::Delete
                } else {
                    RmwAction::Mutated
                };
                RmwStep {
                    action,
                    expire: ExpireWrite::Unchanged,
                    reply,
                }
            }
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        });
        match outcome {
            Value::Null => {}
            other => return Some(other),
        }
    }
    None
}

/// BZMPOP attempt: the non-blocking ZMPOP over `keys` at the chosen end, popping up to
/// `count`. `None` if every key is empty/absent (park).
fn try_bzmpop<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    keys: &[Vec<u8>],
    max: bool,
    count: usize,
) -> Option<Value> {
    let dir = if max { b"MAX".as_slice() } else { b"MIN" };
    let zmpop = synth_mpop(b"ZMPOP", keys, dir, count);
    match crate::cmd_zset::cmd_zmpop(store, db, now, &zmpop) {
        Value::Array(None) => None,
        other => Some(other),
    }
}

/// Synthesize a non-blocking `LMPOP`/`ZMPOP` request from the parsed keys + direction +
/// count, so the blocking attempt reuses the canonical non-blocking handler unchanged
/// (preserving its first-non-empty-wins order, WRONGTYPE rules, and keyspace events).
fn synth_mpop(token: &'static [u8], keys: &[Vec<u8>], dir: &[u8], count: usize) -> Request {
    let mut args: Vec<Bytes> = Vec::with_capacity(keys.len() + 5);
    args.push(Bytes::copy_from_slice(token));
    args.push(Bytes::copy_from_slice(keys.len().to_string().as_bytes()));
    for k in keys {
        args.push(Bytes::copy_from_slice(k));
    }
    args.push(Bytes::copy_from_slice(dir));
    args.push(Bytes::from_static(b"COUNT"));
    args.push(Bytes::copy_from_slice(count.to_string().as_bytes()));
    Request { args }
}

/// Whether a successful WRITE command may make a blocked list/zset key READY (so the serve
/// layer should wake a waiter on the command's destination key(s)). This is the WAKE-TRIGGER
/// gate: it covers the push/move/add commands that ADD an element to a list or zset. A woken
/// waiter RE-ATTEMPTS its pop and re-parks if still empty, so an over-broad wake is safe (it
/// is at worst a spurious re-check); we keep the gate to the element-adding commands so the
/// common path does not wake on every write. The returned keys are the DESTINATION key(s) a
/// blocked client could be waiting on.
#[must_use]
pub fn wake_keys_for_write(cmd_upper: &[u8], req: &Request) -> Vec<Vec<u8>> {
    match cmd_upper {
        // List pushes: the key is args[1].
        b"LPUSH" | b"RPUSH" | b"LPUSHX" | b"RPUSHX" | b"LINSERT" | b"LSET" => req
            .args
            .get(1)
            .map(|k| vec![k.to_vec()])
            .unwrap_or_default(),
        // Moves: the DESTINATION (args[2]) is the key that gains an element. The blocking
        // forms (BLMOVE/BRPOPLPUSH) push to dst too, so a chained blocked waiter wakes.
        b"LMOVE" | b"RPOPLPUSH" => req
            .args
            .get(2)
            .map(|k| vec![k.to_vec()])
            .unwrap_or_default(),
        b"BLMOVE" => req
            .args
            .get(2)
            .map(|k| vec![k.to_vec()])
            .unwrap_or_default(),
        b"BRPOPLPUSH" => req
            .args
            .get(2)
            .map(|k| vec![k.to_vec()])
            .unwrap_or_default(),
        // Zset adds: the key is args[1].
        b"ZADD" | b"ZINCRBY" => req
            .args
            .get(1)
            .map(|k| vec![k.to_vec()])
            .unwrap_or_default(),
        // Generic key-creating commands that can materialize a list/zset on the dest: the
        // store-into commands. RENAME/COPY dest=args[2]; the *STORE dest=args[1].
        b"RENAME" | b"COPY" | b"SMOVE" => req
            .args
            .get(2)
            .map(|k| vec![k.to_vec()])
            .unwrap_or_default(),
        b"ZRANGESTORE" | b"ZUNIONSTORE" | b"ZINTERSTORE" | b"ZDIFFSTORE" | b"SORT" => {
            // ZRANGESTORE/Z*STORE dest is args[1]; SORT's STORE dest is after a STORE token.
            // For SORT we conservatively wake on args[1] is wrong (that is the source); skip
            // SORT here (it rarely feeds a blocking list and the dest needs an option scan).
            if cmd_upper == b"SORT" {
                Vec::new()
            } else {
                req.args
                    .get(1)
                    .map(|k| vec![k.to_vec()])
                    .unwrap_or_default()
            }
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::{CountingAccounting, NewValue};
    use ironcache_store::ShardStore;

    type TestStore = ShardStore<ironcache_eviction::Policy, CountingAccounting>;

    fn test_store() -> TestStore {
        ShardStore::with_hooks(
            1,
            ironcache_eviction::Policy::cache_default(),
            CountingAccounting::new(),
        )
    }

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    const NOW: UnixMillis = UnixMillis(0);

    fn rpush(store: &mut TestStore, key: &[u8], elems: &[&[u8]]) {
        let mut args = vec![b"RPUSH".as_slice(), key];
        args.extend_from_slice(elems);
        let r = req(&args);
        crate::cmd_list::cmd_rpush(store, 0, NOW, &r);
    }

    #[test]
    fn timeout_zero_is_block_forever() {
        assert_eq!(parse_timeout_secs(b"0").unwrap(), None);
    }

    #[test]
    fn timeout_fractional_seconds_to_ms() {
        assert_eq!(parse_timeout_secs(b"0.1").unwrap(), Some(100));
        assert_eq!(parse_timeout_secs(b"1").unwrap(), Some(1000));
    }

    #[test]
    fn timeout_negative_is_error() {
        assert_eq!(
            parse_timeout_secs(b"-1").unwrap_err().line(),
            "-ERR timeout is negative"
        );
    }

    #[test]
    fn timeout_non_float_is_error() {
        assert_eq!(
            parse_timeout_secs(b"xx").unwrap_err().line(),
            "-ERR timeout is not a float or out of range"
        );
    }

    #[test]
    fn blpop_on_present_list_pops_immediately() {
        let mut store = test_store();
        rpush(&mut store, b"k", &[b"a", b"b"]);
        let spec = parse_block(b"BLPOP", &req(&[b"BLPOP", b"k", b"0"])).unwrap();
        let reply = try_block_op(&mut store, 0, NOW, &spec).unwrap();
        assert_eq!(
            reply,
            Value::Array(Some(vec![
                Value::bulk(Bytes::from_static(b"k")),
                Value::bulk(Bytes::from_static(b"a")),
            ]))
        );
    }

    #[test]
    fn blpop_on_empty_returns_none_to_park() {
        let mut store = test_store();
        let spec = parse_block(b"BLPOP", &req(&[b"BLPOP", b"missing", b"0"])).unwrap();
        assert!(try_block_op(&mut store, 0, NOW, &spec).is_none());
    }

    #[test]
    fn blpop_first_non_empty_wins() {
        let mut store = test_store();
        rpush(&mut store, b"k2", &[b"x"]);
        // k1 empty, k2 present: BLPOP serves k2.
        let spec = parse_block(b"BLPOP", &req(&[b"BLPOP", b"k1", b"k2", b"0"])).unwrap();
        let reply = try_block_op(&mut store, 0, NOW, &spec).unwrap();
        assert_eq!(
            reply,
            Value::Array(Some(vec![
                Value::bulk(Bytes::from_static(b"k2")),
                Value::bulk(Bytes::from_static(b"x")),
            ]))
        );
    }

    #[test]
    fn brpop_pops_the_tail() {
        let mut store = test_store();
        rpush(&mut store, b"k", &[b"a", b"b"]);
        let spec = parse_block(b"BRPOP", &req(&[b"BRPOP", b"k", b"0"])).unwrap();
        let reply = try_block_op(&mut store, 0, NOW, &spec).unwrap();
        assert_eq!(
            reply,
            Value::Array(Some(vec![
                Value::bulk(Bytes::from_static(b"k")),
                Value::bulk(Bytes::from_static(b"b")),
            ]))
        );
    }

    #[test]
    fn blpop_wrongtype_is_ready_error() {
        let mut store = test_store();
        store.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
        let spec = parse_block(b"BLPOP", &req(&[b"BLPOP", b"k", b"0"])).unwrap();
        let reply = try_block_op(&mut store, 0, NOW, &spec).unwrap();
        assert!(matches!(reply, Value::Error(_)));
    }

    #[test]
    fn bzpopmin_present_replies_key_member_score() {
        let mut store = test_store();
        crate::cmd_zset::cmd_zadd(
            &mut store,
            0,
            NOW,
            &req(&[b"ZADD", b"z", b"1", b"a", b"2", b"b"]),
        );
        let spec = parse_block(b"BZPOPMIN", &req(&[b"BZPOPMIN", b"z", b"0"])).unwrap();
        let reply = try_block_op(&mut store, 0, NOW, &spec).unwrap();
        assert_eq!(
            reply,
            Value::Array(Some(vec![
                Value::bulk(Bytes::from_static(b"z")),
                Value::bulk(Bytes::from_static(b"a")),
                Value::Double(1.0),
            ]))
        );
    }

    #[test]
    fn bzpopmax_pops_highest() {
        let mut store = test_store();
        crate::cmd_zset::cmd_zadd(
            &mut store,
            0,
            NOW,
            &req(&[b"ZADD", b"z", b"1", b"a", b"2", b"b"]),
        );
        let spec = parse_block(b"BZPOPMAX", &req(&[b"BZPOPMAX", b"z", b"0"])).unwrap();
        let reply = try_block_op(&mut store, 0, NOW, &spec).unwrap();
        assert_eq!(
            reply,
            Value::Array(Some(vec![
                Value::bulk(Bytes::from_static(b"z")),
                Value::bulk(Bytes::from_static(b"b")),
                Value::Double(2.0),
            ]))
        );
    }

    #[test]
    fn blmove_present_moves_and_empty_parks() {
        let mut store = test_store();
        rpush(&mut store, b"src", &[b"a"]);
        let spec = parse_block(
            b"BLMOVE",
            &req(&[b"BLMOVE", b"src", b"dst", b"LEFT", b"RIGHT", b"0"]),
        )
        .unwrap();
        let reply = try_block_op(&mut store, 0, NOW, &spec).unwrap();
        assert_eq!(reply, Value::bulk(Bytes::from_static(b"a")));
        // src now empty -> a second attempt parks.
        assert!(try_block_op(&mut store, 0, NOW, &spec).is_none());
    }

    #[test]
    fn brpoplpush_is_right_left_move() {
        let mut store = test_store();
        rpush(&mut store, b"src", &[b"a", b"b"]);
        let spec =
            parse_block(b"BRPOPLPUSH", &req(&[b"BRPOPLPUSH", b"src", b"dst", b"0"])).unwrap();
        let reply = try_block_op(&mut store, 0, NOW, &spec).unwrap();
        // RIGHT pop -> 'b'.
        assert_eq!(reply, Value::bulk(Bytes::from_static(b"b")));
    }

    #[test]
    fn blmpop_parses_and_pops() {
        let mut store = test_store();
        rpush(&mut store, b"k", &[b"a", b"b", b"c"]);
        let spec = parse_block(
            b"BLMPOP",
            &req(&[b"BLMPOP", b"0", b"1", b"k", b"LEFT", b"COUNT", b"2"]),
        )
        .unwrap();
        let reply = try_block_op(&mut store, 0, NOW, &spec).unwrap();
        assert_eq!(
            reply,
            Value::Array(Some(vec![
                Value::bulk(Bytes::from_static(b"k")),
                Value::Array(Some(vec![
                    Value::bulk(Bytes::from_static(b"a")),
                    Value::bulk(Bytes::from_static(b"b")),
                ])),
            ]))
        );
    }

    #[test]
    fn bzmpop_parses_and_pops() {
        let mut store = test_store();
        crate::cmd_zset::cmd_zadd(
            &mut store,
            0,
            NOW,
            &req(&[b"ZADD", b"z", b"1", b"a", b"2", b"b"]),
        );
        let spec = parse_block(b"BZMPOP", &req(&[b"BZMPOP", b"0", b"1", b"z", b"MIN"])).unwrap();
        let reply = try_block_op(&mut store, 0, NOW, &spec).unwrap();
        // [key, [[member, score]]] shape (ZMPOP).
        assert!(matches!(reply, Value::Array(Some(_))));
    }

    #[test]
    fn wake_keys_cover_pushes_and_moves() {
        assert_eq!(
            wake_keys_for_write(b"LPUSH", &req(&[b"LPUSH", b"k", b"v"])),
            vec![b"k".to_vec()]
        );
        assert_eq!(
            wake_keys_for_write(b"LMOVE", &req(&[b"LMOVE", b"s", b"d", b"LEFT", b"RIGHT"])),
            vec![b"d".to_vec()]
        );
        assert_eq!(
            wake_keys_for_write(b"ZADD", &req(&[b"ZADD", b"z", b"1", b"m"])),
            vec![b"z".to_vec()]
        );
        assert!(wake_keys_for_write(b"GET", &req(&[b"GET", b"k"])).is_empty());
    }

    #[test]
    fn wait_keys_pop_registers_on_every_key_move_only_on_src() {
        // BLPOP registers a waiter on EVERY key.
        let bpop = parse_block(b"BLPOP", &req(&[b"BLPOP", b"k1", b"k2", b"0"])).unwrap();
        assert_eq!(bpop.wait_keys(), &[b"k1".to_vec(), b"k2".to_vec()]);
        // BLMOVE registers a waiter ONLY on the SOURCE (keys[0]), not the destination (FIX3).
        let blmove = parse_block(
            b"BLMOVE",
            &req(&[b"BLMOVE", b"src", b"dst", b"LEFT", b"RIGHT", b"0"]),
        )
        .unwrap();
        assert_eq!(blmove.keys, vec![b"src".to_vec(), b"dst".to_vec()]);
        assert_eq!(blmove.wait_keys(), &[b"src".to_vec()]);
        // BRPOPLPUSH is a Move too: source-only waiter.
        let brpoplpush =
            parse_block(b"BRPOPLPUSH", &req(&[b"BRPOPLPUSH", b"src", b"dst", b"0"])).unwrap();
        assert_eq!(brpoplpush.wait_keys(), &[b"src".to_vec()]);
    }

    #[test]
    fn arity_errors() {
        assert!(parse_block(b"BLPOP", &req(&[b"BLPOP", b"k"])).is_err());
        assert!(parse_block(b"BLMOVE", &req(&[b"BLMOVE", b"s", b"d"])).is_err());
        assert!(parse_block(b"BLMPOP", &req(&[b"BLMPOP", b"0"])).is_err());
    }
}
