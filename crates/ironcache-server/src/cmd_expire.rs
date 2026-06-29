// SPDX-License-Identifier: MIT OR Apache-2.0
//! The TTL / EXPIRE-family command handlers over the storage waist (COMMANDS.md,
//! KEYSPACE.md, EXPIRATION.md). PR-3b: EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT (with
//! the Redis 7 NX/XX/GT/LT options), TTL / PTTL, EXPIRETIME / PEXPIRETIME, PERSIST,
//! GETEX, SETEX / PSETEX.
//!
//! Every handler is a composition of the FROZEN four primitives (STORAGE_API.md), no
//! Store-trait change:
//!
//! - EXPIRE-family / PERSIST / GETEX -> `rmw`: the closure observes the current
//!   `expire_at`, applies the option logic, and returns either `RmwAction::Keep` with
//!   the resolved [`ExpireWrite`], or `RmwAction::Delete` for a deadline already in
//!   the past (Redis deletes a key whose new expiry is already past and replies 1).
//! - TTL / PTTL / EXPIRETIME / PEXPIRETIME -> `read` (a pure observation).
//! - SETEX / PSETEX -> `upsert` with [`ExpireWrite::Set`] (SET with a mandatory TTL).
//!
//! Time enters only as the `now` deadline basis (ADR-0003); the handlers convert
//! relative EX/PX against `now` into the absolute [`UnixMillis`] the waist stores, and
//! after any SUCCESSFUL TTL set they register the deadline in the per-shard timing
//! wheel ([`ironcache_expiry::TimingWheel`]) so the active drain can reclaim it. The
//! wheel is an optimization; the store's lazy backstop is the correctness guarantee,
//! so a stale registration (a re-TTL'd or PERSISTed key) is harmless.

use crate::cmd_util::{ascii_upper, parse_i64};
use bytes::Bytes;
use ironcache_expiry::TimingWheel;
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{
    DataType, ExpireWrite, NewValue, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};

// ---------------------------------------------------------------------------
// The EXPIRE family: EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT with NX/XX/GT/LT.
// ---------------------------------------------------------------------------

/// Which absolute-deadline basis an EXPIRE-family command uses.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ExpireKind {
    /// EXPIRE: argument is seconds RELATIVE to `now`.
    Seconds,
    /// PEXPIRE: argument is milliseconds RELATIVE to `now`.
    Millis,
    /// EXPIREAT: argument is an ABSOLUTE unix time in seconds.
    SecondsAt,
    /// PEXPIREAT: argument is an ABSOLUTE unix time in milliseconds.
    MillisAt,
}

/// The Redis 7 conditional flags on an EXPIRE-family command, parsed as the RAW set
/// of flags rather than collapsed into one mutually-exclusive choice.
///
/// Redis evaluates the existence gate (`NX`/`XX`) and the ordering gate (`GT`/`LT`)
/// INDEPENDENTLY (src/expire.c): both gates must pass for the timeout to be set. The
/// legal combinations `GT XX` and `LT XX` therefore carry BOTH an ordering and an
/// existence condition, and BOTH must hold. Collapsing the four flags into a single
/// enum would silently drop the `XX` gate on an `LT XX` (the only observably divergent
/// pairing, since `GT` already rejects a no-TTL key the way `XX` would), so the flags
/// are kept separate here. Conflicts (`NX` with any of `XX`/`GT`/`LT`; `GT` with `LT`)
/// are still rejected at parse time.
///
/// The four bools mirror the four INDEPENDENT Redis option bits exactly (each present
/// or absent on its own); collapsing them into two-variant enums or a state machine
/// (the `struct_excessive_bools` lint's suggestion) would re-introduce the very
/// coupling the #1 fix removes, so the lint is allowed here with that rationale.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct ExpireCond {
    /// `NX`: set only if the key has NO current TTL.
    nx: bool,
    /// `XX`: set only if the key HAS a current TTL.
    xx: bool,
    /// `GT`: set only if the new expiry is GREATER than the current (no-TTL =
    /// +infinity, so `GT` against a no-TTL key NEVER applies).
    gt: bool,
    /// `LT`: set only if the new expiry is LESS than the current (no-TTL = +infinity,
    /// so `LT` against a no-TTL key ALWAYS applies).
    lt: bool,
}

/// Why parsing the EXPIRE-family option tail failed: a flag conflict (one of the two
/// Redis incompatibility messages) or an unknown option token. Each maps to a specific
/// Redis error string (src/expire.c `parseExtendedExpireArgumentsOrReply`).
enum ExpireCondError {
    /// `NX` combined with any of `XX`/`GT`/`LT`.
    NxWithOther,
    /// `GT` combined with `LT`.
    GtWithLt,
    /// An unrecognized option token (echoed verbatim).
    Unsupported(String),
}

/// Parse the optional trailing condition flags of an EXPIRE-family command into the
/// RAW [`ExpireCond`] flag set. Rejects the Redis conflicts (NX with any of XX/GT/LT;
/// GT with LT) and unknown tokens, mapping each to a specific [`ExpireCondError`].
fn parse_expire_cond(args: &[Bytes]) -> Result<ExpireCond, ExpireCondError> {
    let mut cond = ExpireCond::default();
    for a in args {
        match ascii_upper(a).as_slice() {
            b"NX" => cond.nx = true,
            b"XX" => cond.xx = true,
            b"GT" => cond.gt = true,
            b"LT" => cond.lt = true,
            _ => {
                // Echo the token verbatim (Redis prints the raw argument).
                return Err(ExpireCondError::Unsupported(
                    String::from_utf8_lossy(a).into_owned(),
                ));
            }
        }
    }
    // Conflicts (Redis: NX with any of XX/GT/LT is an error; GT+LT is an error). The
    // NX conflict is checked first to match Redis's argument-parse order.
    if cond.nx && (cond.xx || cond.gt || cond.lt) {
        return Err(ExpireCondError::NxWithOther);
    }
    if cond.gt && cond.lt {
        return Err(ExpireCondError::GtWithLt);
    }
    Ok(cond)
}

/// Resolve an EXPIRE-family argument into an absolute deadline in milliseconds, as a
/// SIGNED i64 (it MAY be zero or negative: a resolved deadline in the past deletes
/// the key, it is NOT an "invalid expire time"). Overflow of the seconds->ms or the
/// now+delta computation is the only true invalid-expire failure here. Returns
/// `Err(())` on overflow.
pub(crate) fn resolve_expire_at(kind: ExpireKind, n: i64, now: UnixMillis) -> Result<i64, ()> {
    let now_ms = i64::try_from(now.0).map_err(|_| ())?;
    match kind {
        ExpireKind::Seconds => n
            .checked_mul(1_000)
            .and_then(|ms| now_ms.checked_add(ms))
            .ok_or(()),
        ExpireKind::Millis => now_ms.checked_add(n).ok_or(()),
        ExpireKind::SecondsAt => n.checked_mul(1_000).ok_or(()),
        ExpireKind::MillisAt => Ok(n),
    }
}

/// The shared body of EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT.
///
/// Reply: `1` if the timeout was set/updated (or the past-deadline delete fired), `0`
/// if not (key missing or condition not met). One atomic `rmw` over the frozen waist:
/// the closure observes the current `expire_at`, applies the NX/XX/GT/LT condition,
/// and returns `RmwAction::Keep` + `ExpireWrite::Set` (or `RmwAction::Delete` for a
/// past deadline). After a successful SET (not a delete) the deadline is registered in
/// the wheel.
fn expire_generic<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
    kind: ExpireKind,
    cmd_name: &str,
) -> Value {
    // EXPIRE key seconds [NX|XX|GT|LT]: arity is at least 3 (cmd, key, time).
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let Some(n) = parse_i64(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    let cond = match parse_expire_cond(&req.args[3..]) {
        Ok(c) => c,
        Err(ExpireCondError::NxWithOther) => {
            return Value::error(ErrorReply::expire_nx_and_xx_gt_lt());
        }
        Err(ExpireCondError::GtWithLt) => return Value::error(ErrorReply::expire_gt_and_lt()),
        Err(ExpireCondError::Unsupported(opt)) => {
            return Value::error(ErrorReply::expire_unsupported_option(&opt));
        }
    };
    let Ok(deadline_ms) = resolve_expire_at(kind, n, now) else {
        return Value::error(ErrorReply::invalid_expire_time(cmd_name));
    };

    let key = req.args[1].clone();
    // Tracks the absolute deadline (only when a SET, not a delete, actually fired) so
    // the deadline is registered in the wheel after the rmw returns.
    let mut to_register: Option<UnixMillis> = None;
    let reply = store.rmw(db, &key, now, |entry| {
        let current = match &entry {
            RmwEntry::Vacant => {
                // Missing key: reply 0, no write (Redis EXPIRE on a missing key).
                return keep_int(0);
            }
            RmwEntry::Occupied(o) => o.expire_at(),
            // Unreachable: the EXPIRE family uses the read-only `rmw`, not `rmw_mut`.
            RmwEntry::OccupiedMut(_) => unreachable!("expire uses rmw, not rmw_mut"),
        };

        // Apply the NX/XX/GT/LT condition. Redis (src/expire.c) evaluates the
        // existence gate (NX/XX) and the ordering gate (GT/LT) INDEPENDENTLY: BOTH must
        // pass. `current` is None for a key with no TTL, which GT/LT treat as +infinity.
        // `LT XX` is the observably divergent legal pairing the old collapsed enum
        // dropped the XX gate on; both gates are now required.
        let existence_ok = (!cond.nx || current.is_none()) && (!cond.xx || current.is_some());
        let ordering_ok = match current {
            // No current TTL is +infinity: GT never applies, LT always does.
            None => !cond.gt,
            Some(cur) => {
                // GT: new must be strictly greater; LT: strictly less. With neither
                // flag the gate is open.
                (!cond.gt || deadline_ms > cur.0 as i64) && (!cond.lt || deadline_ms < cur.0 as i64)
            }
        };
        if !(existence_ok && ordering_ok) {
            return keep_int(0);
        }

        // A resolved deadline at or before `now` deletes the key and replies 1 (Redis
        // src/expire.c `checkAlreadyExpired`: `when <= now`). This COMMAND-TIME boundary
        // is `<=`, distinct from the store's lazy-read backstop (`now > deadline`, alive
        // at now==deadline): a deadline EQUAL to now is treated as already past HERE and
        // the key is deleted immediately. Only a strictly-FUTURE deadline is set (and
        // registered in the wheel below).
        if deadline_ms <= now.0 as i64 {
            return RmwStep {
                action: RmwAction::Delete,
                expire: ExpireWrite::Unchanged,
                reply: Value::Integer(1),
            };
        }

        let at = UnixMillis(deadline_ms as u64);
        RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Set(at),
            reply: Value::Integer(1),
        }
    });

    // Register the deadline in the wheel only when a SET (not a delete / no-op) fired.
    // A SET fired iff the reply is 1 AND the deadline is strictly in the FUTURE
    // (`deadline_ms > now`): a deadline at or before now took the past-deadline delete
    // branch above, so only a strictly-future deadline is a live registration.
    if matches!(reply, Value::Integer(1)) && deadline_ms > now.0 as i64 {
        to_register = Some(UnixMillis(deadline_ms as u64));
    }
    if let Some(at) = to_register {
        wheel.register(db, &key, at);
    }
    reply
}

/// `EXPIRE key seconds [NX|XX|GT|LT]`.
pub fn cmd_expire<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    expire_generic(store, wheel, db, now, req, ExpireKind::Seconds, "expire")
}

/// `PEXPIRE key milliseconds [NX|XX|GT|LT]`.
pub fn cmd_pexpire<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    expire_generic(store, wheel, db, now, req, ExpireKind::Millis, "pexpire")
}

/// `EXPIREAT key unix-time-seconds [NX|XX|GT|LT]`.
pub fn cmd_expireat<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    expire_generic(
        store,
        wheel,
        db,
        now,
        req,
        ExpireKind::SecondsAt,
        "expireat",
    )
}

/// `PEXPIREAT key unix-time-milliseconds [NX|XX|GT|LT]`.
pub fn cmd_pexpireat<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    expire_generic(
        store,
        wheel,
        db,
        now,
        req,
        ExpireKind::MillisAt,
        "pexpireat",
    )
}

// ---------------------------------------------------------------------------
// TTL / PTTL / EXPIRETIME / PEXPIRETIME: pure reads.
// ---------------------------------------------------------------------------

/// The reply convention for the four read-only TTL commands: `-2` if the key is
/// missing (or lazily expired), `-1` if it exists with no TTL, else the value the
/// per-command `map` derives from the absolute `expire_at` deadline.
fn ttl_read<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    cmd_name: &str,
    map: impl FnOnce(UnixMillis) -> i64,
) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    match store.read(db, &req.args[1], now) {
        None => Value::Integer(-2),
        Some(v) => match v.expire_at() {
            None => Value::Integer(-1),
            Some(at) => Value::Integer(map(at)),
        },
    }
}

/// `TTL key` -> remaining seconds (`(ms + 500) / 1000`, Redis rounding), -2/-1.
pub fn cmd_ttl<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    ttl_read(store, db, now, req, "ttl", |at| {
        let remaining_ms = at.0.saturating_sub(now.0);
        // Redis rounds to the nearest second: (ms + 500) / 1000.
        ((remaining_ms + 500) / 1_000) as i64
    })
}

/// `PTTL key` -> remaining milliseconds, -2/-1.
pub fn cmd_pttl<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    ttl_read(store, db, now, req, "pttl", |at| {
        at.0.saturating_sub(now.0) as i64
    })
}

/// `EXPIRETIME key` -> the ABSOLUTE unix expiry in SECONDS, -2/-1 (Redis 7).
///
/// Redis rounds the absolute ms deadline to the NEAREST second (`(expire_ms + 500) /
/// 1000`, src/expire.c `ttlGenericCommand` output_abs), so a deadline with an ms
/// component >= 500 rounds UP. PEXPIRETIME stays exact ms.
pub fn cmd_expiretime<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    ttl_read(store, db, now, req, "expiretime", |at| {
        ((at.0 + 500) / 1_000) as i64
    })
}

/// `PEXPIRETIME key` -> the ABSOLUTE unix expiry in MILLISECONDS, -2/-1 (Redis 7).
pub fn cmd_pexpiretime<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    ttl_read(store, db, now, req, "pexpiretime", |at| at.0 as i64)
}

// ---------------------------------------------------------------------------
// PERSIST: remove a TTL.
// ---------------------------------------------------------------------------

/// `PERSIST key` -> `1` if a TTL was removed, `0` if the key is missing or had no TTL.
/// One atomic `rmw`: observe the current TTL, clear it iff present.
pub fn cmd_persist<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("persist"));
    }
    store.rmw(db, &req.args[1], now, |entry| match entry {
        RmwEntry::Occupied(o) if o.expire_at().is_some() => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Clear,
            reply: Value::Integer(1),
        },
        // Present with no TTL, or absent: nothing to remove, reply 0.
        _ => keep_int(0),
    })
}

// ---------------------------------------------------------------------------
// GETEX: read the value, optionally changing the TTL.
// ---------------------------------------------------------------------------

/// The TTL effect a GETEX option requests.
#[derive(Debug, Clone, Copy)]
enum GetexTtl {
    /// No option: do NOT change the TTL (bare GETEX is a pure read, unlike SET).
    Unchanged,
    /// EX/PX/EXAT/PXAT resolved to a STRICTLY-FUTURE absolute deadline.
    Set(UnixMillis),
    /// EXAT/PXAT resolved to an absolute deadline already at or before `now`
    /// (`deadline <= now`, the Redis `checkAlreadyExpired` command-time boundary): the
    /// key is read (the value is returned) and then DELETED, like an EXPIREAT in the
    /// past. Only the absolute forms reach this; a non-positive relative EX/PX is the
    /// `invalid expire time` error instead.
    SetPast,
    /// PERSIST: clear any TTL.
    Persist,
}

/// `GETEX key [EX s | PX ms | EXAT ts | PXAT tms | PERSIST]`.
///
/// Returns the value (nil if absent; WRONGTYPE if non-string). Bare GETEX does NOT
/// change the TTL. EX/PX/EXAT/PXAT set the TTL; PERSIST clears it. An invalid expire
/// (<= 0 / overflow) is `-ERR invalid expire time in 'getex' command`; conflicting
/// options are a syntax error. GETEX is NOT denyoom (it never grows memory).
pub fn cmd_getex<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("getex"));
    }
    let ttl = match parse_getex_ttl(&req.args[2..], now) {
        Ok(t) => t,
        Err(GetexParseError::Syntax) => return Value::error(ErrorReply::syntax_error()),
        Err(GetexParseError::NotInteger) => return Value::error(ErrorReply::not_an_integer()),
        Err(GetexParseError::InvalidExpire) => {
            return Value::error(ErrorReply::invalid_expire_time("getex"));
        }
    };

    let key = req.args[1].clone();
    let mut to_register: Option<UnixMillis> = None;
    let reply = store.rmw(db, &key, now, |entry| match entry {
        RmwEntry::Vacant => keep_value(Value::Null),
        RmwEntry::Occupied(o) if o.data_type() != DataType::String => {
            keep_value(Value::error(ErrorReply::wrong_type()))
        }
        RmwEntry::Occupied(o) => {
            let value = Value::BulkString(Some(Bytes::copy_from_slice(o.as_bytes())));
            // A past absolute deadline (EXAT/PXAT with `deadline <= now`) returns the
            // value and DELETES the key (Redis checkAlreadyExpired command-time
            // boundary); every other case keeps the key and adjusts the TTL.
            let (action, expire) = match ttl {
                GetexTtl::Unchanged => (RmwAction::Keep, ExpireWrite::Unchanged),
                GetexTtl::Set(at) => (RmwAction::Keep, ExpireWrite::Set(at)),
                GetexTtl::Persist => (RmwAction::Keep, ExpireWrite::Clear),
                GetexTtl::SetPast => (RmwAction::Delete, ExpireWrite::Unchanged),
            };
            RmwStep {
                action,
                expire,
                reply: value,
            }
        }
        // Unreachable: GETEX uses the read-only `rmw`, never `rmw_mut`.
        RmwEntry::OccupiedMut(_) => unreachable!("getex uses rmw, not rmw_mut"),
    });

    // Register the deadline only when GETEX actually set one on a live string (the
    // reply is the value bulk, not nil / WRONGTYPE).
    if let GetexTtl::Set(at) = ttl {
        if matches!(reply, Value::BulkString(Some(_))) {
            to_register = Some(at);
        }
    }
    if let Some(at) = to_register {
        wheel.register(db, &key, at);
    }
    reply
}

/// Why parsing the GETEX option tail failed (the three Redis error classes, as for
/// SET: syntax / not-an-integer / invalid-expire).
#[derive(Debug, PartialEq, Eq)]
enum GetexParseError {
    Syntax,
    NotInteger,
    InvalidExpire,
}

/// Parse the GETEX option tail (at most one of EX/PX/EXAT/PXAT/PERSIST). Bare (empty)
/// resolves to [`GetexTtl::Unchanged`].
fn parse_getex_ttl(args: &[Bytes], now: UnixMillis) -> Result<GetexTtl, GetexParseError> {
    if args.is_empty() {
        return Ok(GetexTtl::Unchanged);
    }
    let opt = ascii_upper(&args[0]);
    match opt.as_slice() {
        b"PERSIST" => {
            if args.len() != 1 {
                return Err(GetexParseError::Syntax);
            }
            Ok(GetexTtl::Persist)
        }
        kw @ (b"EX" | b"PX" | b"EXAT" | b"PXAT") => {
            // Exactly one argument after the keyword, and nothing more.
            if args.len() != 2 {
                return Err(GetexParseError::Syntax);
            }
            let n = parse_i64(&args[1]).ok_or(GetexParseError::NotInteger)?;
            let (kind, absolute) = match kw {
                b"EX" => (ExpireKind::Seconds, false),
                b"PX" => (ExpireKind::Millis, false),
                b"EXAT" => (ExpireKind::SecondsAt, true),
                _ => (ExpireKind::MillisAt, true),
            };
            // A non-positive / overflowing RELATIVE expire (EX/PX) is invalid and the
            // option is rejected outright (Redis does NOT delete the key for it). The
            // ABSOLUTE forms (EXAT/PXAT) only reject a non-positive timestamp; an
            // in-the-past-but-positive absolute deadline DELETES the key (handled below
            // via SetPast, the checkAlreadyExpired command-time boundary).
            let abs =
                resolve_expire_at(kind, n, now).map_err(|()| GetexParseError::InvalidExpire)?;
            if abs <= 0 {
                return Err(GetexParseError::InvalidExpire);
            }
            // checkAlreadyExpired command-time boundary (`deadline <= now`): a resolved
            // ABSOLUTE deadline at or before now expires the key on this command. EX/PX
            // resolve against `now`, so a positive relative value is always strictly
            // future and never hits this; only EXAT/PXAT can.
            if absolute && abs <= now.0 as i64 {
                return Ok(GetexTtl::SetPast);
            }
            Ok(GetexTtl::Set(UnixMillis(abs as u64)))
        }
        _ => Err(GetexParseError::Syntax),
    }
}

// ---------------------------------------------------------------------------
// SETEX / PSETEX: SET with a mandatory TTL.
// ---------------------------------------------------------------------------

/// `SETEX key seconds value` / `PSETEX key milliseconds value`.
///
/// SET with a mandatory TTL. A non-positive seconds/ms is
/// `-ERR invalid expire time in '<setex|psetex>' command` and nothing is written.
/// Reply `+OK`. Implemented as `upsert` with [`ExpireWrite::Set`] (a blind set with a
/// deadline), then a wheel registration.
fn setex_generic<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
    millis: bool,
    cmd_name: &str,
) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let Some(n) = parse_i64(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    // SETEX/PSETEX require a STRICTLY positive timeout (a zero or negative timeout is
    // an invalid expire time; nothing is written). Redis checks this before the write.
    if n <= 0 {
        return Value::error(ErrorReply::invalid_expire_time(cmd_name));
    }
    let kind = if millis {
        ExpireKind::Millis
    } else {
        ExpireKind::Seconds
    };
    let Ok(deadline_ms) = resolve_expire_at(kind, n, now) else {
        return Value::error(ErrorReply::invalid_expire_time(cmd_name));
    };
    // A positive timeout added to `now` is always positive, but guard the cast.
    if deadline_ms <= 0 {
        return Value::error(ErrorReply::invalid_expire_time(cmd_name));
    }
    let at = UnixMillis(deadline_ms as u64);
    store.upsert(
        db,
        &req.args[1],
        NewValue::Bytes(&req.args[3]),
        ExpireWrite::Set(at),
        now,
    );
    wheel.register(db, &req.args[1], at);
    Value::ok()
}

/// `SETEX key seconds value`.
pub fn cmd_setex<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    setex_generic(store, wheel, db, now, req, false, "setex")
}

/// `PSETEX key milliseconds value`.
pub fn cmd_psetex<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    setex_generic(store, wheel, db, now, req, true, "psetex")
}

// ---------------------------------------------------------------------------
// Small rmw-step helpers (no value/TTL change; just a reply).
// ---------------------------------------------------------------------------

/// An rmw step that touches nothing and replies with the integer `n`.
fn keep_int(n: i64) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply: Value::Integer(n),
    }
}

/// An rmw step that touches nothing and replies with `v`.
fn keep_value(v: Value) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply: v,
    }
}
