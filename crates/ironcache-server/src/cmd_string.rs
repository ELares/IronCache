// SPDX-License-Identifier: MIT OR Apache-2.0
//! String-type command handlers over the storage waist (COMMANDS.md strings,
//! ENCODINGS.md, EXPIRATION.md). PR-2a: GET, SET (NX/XX/GET/EX/PX/EXAT/PXAT/
//! KEEPTTL), SETNX, GETSET, STRLEN.
//!
//! Every handler is a composition of the four storage primitives (STORAGE_API.md):
//! GET/STRLEN use `read`; plain SET uses `upsert`; conditional SET / SETNX / GETSET
//! use `rmw` so the observe-then-conditionally-write is atomic on the owning core.
//! WRONGTYPE is checked before any mutation (COMMANDS.md error contract). Time
//! enters only as the `now` deadline basis (ADR-0003); the command layer converts
//! relative EX/PX against `now` into the absolute [`UnixMillis`] the waist stores.

use crate::cmd_util::{ascii_upper, parse_f64, parse_i64, parse_i64_strict};
use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value, format_human_double};
use ironcache_storage::{
    DataType, ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};

/// `GET key` -> bulk value or null; WRONGTYPE if the key holds a non-string.
pub fn cmd_get<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("get"));
    }
    match store.read(db, &req.args[1], now) {
        Some(v) if v.data_type() == DataType::String => {
            Value::BulkString(Some(Bytes::copy_from_slice(v.as_bytes())))
        }
        Some(_) => Value::error(ErrorReply::wrong_type()),
        None => Value::Null,
    }
}

/// `STRLEN key` -> length of the string value (decimal length for an int), 0 if
/// absent; WRONGTYPE if non-string.
pub fn cmd_strlen<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("strlen"));
    }
    match store.read(db, &req.args[1], now) {
        Some(v) if v.data_type() == DataType::String => Value::Integer(v.len() as i64),
        Some(_) => Value::error(ErrorReply::wrong_type()),
        None => Value::Integer(0),
    }
}

/// The parsed SET option set (COMMANDS.md cross-cutting flags). Conflicting
/// options (`NX`+`XX`, more than one of `EX`/`PX`/`EXAT`/`PXAT`/`KEEPTTL`) are a
/// syntax error.
#[derive(Debug, Default)]
struct SetOptions {
    nx: bool,
    xx: bool,
    get: bool,
    ttl: TtlOption,
}

#[derive(Debug, Default, PartialEq, Eq)]
enum TtlOption {
    /// No TTL option: a default SET clears any existing TTL.
    #[default]
    None,
    /// KEEPTTL: preserve the existing deadline.
    Keep,
    /// EX/PX/EXAT/PXAT resolved to an absolute deadline against `now`.
    Set(UnixMillis),
}

/// Why parsing the SET option tail failed. Redis distinguishes THREE error
/// classes here (do not collapse them):
///
/// - [`SetParseError::Syntax`] - conflicting flags (`NX`+`XX`), more than one
///   expire option, or an unknown token. Emits `-ERR syntax error`.
/// - [`SetParseError::NotInteger`] - a non-integer EX/PX/EXAT/PXAT argument
///   (thrown BEFORE the `<= 0` check, matching Redis's `getLongLongFromObjectOrReply`
///   ordering). Emits `-ERR value is not an integer or out of range`.
/// - [`SetParseError::InvalidExpire`] - an expire value `<= 0` or one that
///   overflows the millisecond computation. Emits
///   `-ERR invalid expire time in 'set' command`.
#[derive(Debug, PartialEq, Eq)]
enum SetParseError {
    /// Conflicting/duplicate/unknown options.
    Syntax,
    /// A non-integer (or out-of-i64-range) expire argument.
    NotInteger,
    /// An expire value `<= 0` or one that overflows the ms computation.
    InvalidExpire,
}

/// Parse the SET option tail (args after key and value) into a [`SetOptions`], or
/// the specific [`SetParseError`] class so the caller can emit the right Redis
/// error (Redis maps these to three DISTINCT replies; see [`SetParseError`]).
fn parse_set_options(args: &[Bytes], now: UnixMillis) -> Result<SetOptions, SetParseError> {
    let mut opts = SetOptions::default();
    let mut ttl_seen = false;
    let mut i = 0;
    while i < args.len() {
        let up = ascii_upper(&args[i]);
        match up.as_slice() {
            b"NX" => {
                if opts.nx || opts.xx {
                    return Err(SetParseError::Syntax);
                }
                opts.nx = true;
                i += 1;
            }
            b"XX" => {
                if opts.nx || opts.xx {
                    return Err(SetParseError::Syntax);
                }
                opts.xx = true;
                i += 1;
            }
            b"GET" => {
                if opts.get {
                    return Err(SetParseError::Syntax);
                }
                opts.get = true;
                i += 1;
            }
            b"KEEPTTL" => {
                if ttl_seen {
                    return Err(SetParseError::Syntax);
                }
                ttl_seen = true;
                opts.ttl = TtlOption::Keep;
                i += 1;
            }
            kw @ (b"EX" | b"PX" | b"EXAT" | b"PXAT") => {
                // A duplicate expire option or a missing argument is a syntax error.
                if ttl_seen || i + 1 >= args.len() {
                    return Err(SetParseError::Syntax);
                }
                ttl_seen = true;
                // Redis parses the expire arg as an integer FIRST: a non-integer
                // (or out-of-i64-range) value is the not-an-integer error, thrown
                // before the <= 0 / overflow checks below.
                let n = parse_i64(&args[i + 1]).ok_or(SetParseError::NotInteger)?;
                // A <= 0 value or an overflowing ms computation is invalid expire.
                opts.ttl = TtlOption::Set(resolve_ttl(kw, n, now)?);
                i += 2;
            }
            _ => return Err(SetParseError::Syntax),
        }
    }
    Ok(opts)
}

/// Resolve a TTL keyword + integer argument into an absolute deadline. EX/PX are
/// relative to `now`; EXAT/PXAT are absolute. A non-positive relative TTL, a
/// non-positive resolved deadline, or an out-of-range/overflowing value is
/// [`SetParseError::InvalidExpire`] (Redis's "invalid expire time", rejected
/// before any write). The argument is already known to be a valid i64.
fn resolve_ttl(kw: &[u8], n: i64, now: UnixMillis) -> Result<UnixMillis, SetParseError> {
    let abs_millis: i64 = match kw {
        b"EX" => now_plus(now, mul_1000(n)?)?,
        b"PX" => now_plus(now, n)?,
        b"EXAT" => mul_1000(n)?,
        b"PXAT" => n,
        // Unreachable: parse_set_options only calls this for the four keywords.
        _ => return Err(SetParseError::Syntax),
    };
    if abs_millis <= 0 {
        return Err(SetParseError::InvalidExpire);
    }
    Ok(UnixMillis(abs_millis as u64))
}

/// `n * 1000`, mapping overflow to an invalid-expire error.
fn mul_1000(n: i64) -> Result<i64, SetParseError> {
    n.checked_mul(1_000).ok_or(SetParseError::InvalidExpire)
}

/// `now + delta_millis` as an i64, rejecting a non-positive delta (Redis rejects a
/// non-positive EX/PX as an invalid expire time) and overflow.
fn now_plus(now: UnixMillis, delta_millis: i64) -> Result<i64, SetParseError> {
    if delta_millis <= 0 {
        return Err(SetParseError::InvalidExpire);
    }
    i64::try_from(now.0)
        .ok()
        .and_then(|t| t.checked_add(delta_millis))
        .ok_or(SetParseError::InvalidExpire)
}

/// `SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT tms|KEEPTTL]`.
///
/// Plain SET is a blind `upsert`. Any conditional (NX/XX) or observing (GET) form,
/// or KEEPTTL, goes through `rmw` so the observe-then-write is atomic. Reply is
/// `+OK`/null per Redis, or (with GET) the old value / WRONGTYPE.
pub fn cmd_set<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("set"));
    }
    let opts = match parse_set_options(&req.args[3..], now) {
        Ok(opts) => opts,
        Err(SetParseError::Syntax) => return Value::error(ErrorReply::syntax_error()),
        Err(SetParseError::NotInteger) => return Value::error(ErrorReply::not_an_integer()),
        Err(SetParseError::InvalidExpire) => {
            return Value::error(ErrorReply::invalid_expire_time("set"));
        }
    };

    let expire = match opts.ttl {
        TtlOption::None => ExpireWrite::Clear,
        TtlOption::Keep => ExpireWrite::Keep,
        TtlOption::Set(at) => ExpireWrite::Set(at),
    };

    // The fast path: plain SET with no NX/XX/GET is a blind upsert.
    if !opts.nx && !opts.xx && !opts.get {
        store.upsert(db, &req.args[1], NewValue::Bytes(&req.args[2]), expire, now);
        return Value::ok();
    }

    // The conditional / observing path: one atomic rmw observes the old value,
    // applies NX/XX, captures GET's old value, and writes.
    let key = req.args[1].clone();
    let new_val = req.args[2].clone();
    store.rmw(db, &key, now, move |entry| {
        let occupied = matches!(entry, RmwEntry::Occupied(_));

        // GET (and the WRONGTYPE check it forces) observes the old value first.
        let mut get_reply: Option<Value> = None;
        if opts.get {
            match &entry {
                RmwEntry::Occupied(o) if o.data_type() == DataType::String => {
                    get_reply = Some(Value::BulkString(Some(Bytes::copy_from_slice(
                        o.as_bytes(),
                    ))));
                }
                RmwEntry::Occupied(_) => {
                    // WRONGTYPE on the old value aborts the SET with no write.
                    return RmwStep {
                        action: RmwAction::Keep,
                        expire: ExpireWrite::Unchanged,
                        reply: Value::error(ErrorReply::wrong_type()),
                    };
                }
                RmwEntry::Vacant => get_reply = Some(Value::Null),
            }
        }

        // NX (only if absent) / XX (only if present) gate the write.
        let allowed = (!opts.nx || !occupied) && (!opts.xx || occupied);
        if !allowed {
            // Condition not met: no write. Reply is the GET old value if GET was
            // given, else null (Redis: SET ... NX/XX that does not fire -> nil).
            return RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: get_reply.unwrap_or(Value::Null),
            };
        }

        // Write the new value with the resolved TTL effect.
        let reply = get_reply.unwrap_or_else(Value::ok);
        RmwStep {
            action: RmwAction::Replace(NewValueOwned::Bytes(new_val)),
            expire,
            reply,
        }
    })
}

/// `SETNX key value` -> 1 if set (key was absent), 0 otherwise. An rmw insert-if
/// -vacant; never overwrites, never sets a TTL.
pub fn cmd_setnx<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("setnx"));
    }
    let new_val = req.args[2].clone();
    store.rmw(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::Bytes(new_val)),
            expire: ExpireWrite::Clear,
            reply: Value::Integer(1),
        },
        RmwEntry::Occupied(_) => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: Value::Integer(0),
        },
    })
}

/// `GETSET key value` -> the old value (or null), sets the new value, clears any
/// TTL (Redis semantics). WRONGTYPE if the old value is a non-string (no write).
pub fn cmd_getset<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("getset"));
    }
    let new_val = req.args[2].clone();
    store.rmw(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Occupied(o) if o.data_type() != DataType::String => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: Value::error(ErrorReply::wrong_type()),
        },
        RmwEntry::Occupied(o) => {
            let old = Value::BulkString(Some(Bytes::copy_from_slice(o.as_bytes())));
            RmwStep {
                action: RmwAction::Replace(NewValueOwned::Bytes(new_val)),
                expire: ExpireWrite::Clear,
                reply: old,
            }
        }
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::Bytes(new_val)),
            expire: ExpireWrite::Clear,
            reply: Value::Null,
        },
    })
}

// ---------------------------------------------------------------------------
// Numeric read-modify-write commands (COMMANDS.md strings, ENCODINGS.md int fast
// path). INCR/DECR/INCRBY/DECRBY operate on the value parsed as a canonical i64 and
// store the result INT-encoded; INCRBYFLOAT operates on the value as an f64 and
// stores the result as a STRING (ADR-0019: integer reply for INCR*, bulk-string
// reply for INCRBYFLOAT). Each is one atomic `rmw` over the frozen waist: the
// closure observes the old bytes, validates, computes, and returns the write.
// ---------------------------------------------------------------------------

/// The shared body of INCR/DECR/INCRBY/DECRBY: add `incr` to the value at `key`
/// (absent -> 0), int-encode the result, reply with the new value as a RESP
/// integer. A non-canonical-integer existing value, or an i64 overflow, is the
/// matching Redis error (no write). `incr` is already parsed (the caller validated
/// the argument form, including the DECRBY `i64::MIN` negation edge).
fn incr_by<S: Store>(store: &mut S, db: u32, now: UnixMillis, key: &[u8], incr: i64) -> Value {
    let key = Bytes::copy_from_slice(key);
    store.rmw(db, &key, now, move |entry| {
        // The current value as i64 (absent -> 0). A non-string is WRONGTYPE; a
        // non-canonical-integer string is the not-an-integer error.
        let current: i64 = match &entry {
            RmwEntry::Vacant => 0,
            RmwEntry::Occupied(o) if o.data_type() != DataType::String => {
                return keep_err(ErrorReply::wrong_type());
            }
            RmwEntry::Occupied(o) => match parse_i64_strict(o.as_bytes()) {
                Some(n) => n,
                None => return keep_err(ErrorReply::not_an_integer()),
            },
        };
        // Checked add: an i64 overflow is the overflow error (no write).
        let Some(next) = current.checked_add(incr) else {
            return keep_err(ErrorReply::increment_overflow());
        };
        // Store the result int-encoded (NewValueOwned::Int, no value allocation).
        // INCR does NOT touch the TTL (Redis keeps the existing expire).
        RmwStep {
            action: RmwAction::Replace(NewValueOwned::Int(next)),
            expire: ExpireWrite::Unchanged,
            reply: Value::Integer(next),
        }
    })
}

/// A no-write rmw step that just returns an error reply (value untouched, TTL
/// untouched). The shared abort path for the numeric/append validators.
fn keep_err(e: ErrorReply) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply: Value::error(e),
    }
}

/// `INCR key` -> the new value (old + 1) as a RESP integer.
pub fn cmd_incr<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("incr"));
    }
    incr_by(store, db, now, &req.args[1], 1)
}

/// `DECR key` -> the new value (old - 1) as a RESP integer.
pub fn cmd_decr<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("decr"));
    }
    incr_by(store, db, now, &req.args[1], -1)
}

/// `INCRBY key increment` -> the new value (old + increment) as a RESP integer. A
/// non-integer increment argument is the not-an-integer error (no write).
pub fn cmd_incrby<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("incrby"));
    }
    let Some(incr) = parse_i64_strict(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    incr_by(store, db, now, &req.args[1], incr)
}

/// `DECRBY key decrement` -> the new value (old - decrement) as a RESP integer.
///
/// The decrement is negated before adding. `i64::MIN` cannot be negated within
/// i64, so `DECRBY key -9223372036854775808` is the overflow error (matching
/// Redis, which detects `incr == LLONG_MIN` and replies with the overflow text
/// rather than wrapping). A non-integer decrement argument is the not-an-integer
/// error (checked first, like Redis).
pub fn cmd_decrby<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("decrby"));
    }
    let Some(decr) = parse_i64_strict(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    // Negate the decrement; i64::MIN has no positive i64, so its negation overflows
    // and is the overflow error (the DECRBY i64::MIN edge).
    let Some(incr) = decr.checked_neg() else {
        return Value::error(ErrorReply::increment_overflow());
    };
    incr_by(store, db, now, &req.args[1], incr)
}

/// `INCRBYFLOAT key increment` -> the new value as a bulk string (ADR-0019: bulk in
/// both RESP2 and RESP3).
///
/// Operates on the value parsed as an f64 (absent -> 0). A non-float existing value
/// or increment argument is `-ERR value is not a valid float`; a NaN/Infinity
/// result is `-ERR increment would produce NaN or Infinity`. The checks run in
/// Redis order (type -> existing value -> increment argument), so a non-string key
/// is WRONGTYPE even when the increment argument is itself malformed. The result is stored
/// as a STRING (its human-formatted decimal, classified embstr/raw by length, so a
/// later INCR on an integer-valued result still works), NOT int-encoded
/// (ENCODINGS.md: the INCRBYFLOAT result is a string). The TTL is left unchanged.
///
/// IronCache uses f64, a documented precision divergence from Redis's 80-bit long
/// double; the result is formatted with [`format_human_double`] (the ld2string
/// HUMAN spelling), NOT the RESP3 fpconv double encoder.
pub fn cmd_incrbyfloat<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("incrbyfloat"));
    }
    // Redis `incrbyfloatCommand` checks the TYPE first (checkType OBJ_STRING),
    // THEN parses the existing value, THEN parses the increment argument. So the
    // raw increment Bytes is carried INTO the rmw closure and parsed only after the
    // type and existing-value checks pass, matching Redis's order: a non-string key
    // is WRONGTYPE even with a malformed increment (e.g. `INCRBYFLOAT <list> abc`).
    // (This differs from INCR/INCRBY/DECRBY, where Redis parses the integer increment
    // argument BEFORE checkType, so those keep their parse-first order.)
    let key = req.args[1].clone();
    let incr_arg = req.args[2].clone();
    store.rmw(db, &key, now, move |entry| {
        let current: f64 = match &entry {
            RmwEntry::Vacant => 0.0,
            RmwEntry::Occupied(o) if o.data_type() != DataType::String => {
                return keep_err(ErrorReply::wrong_type());
            }
            RmwEntry::Occupied(o) => match parse_f64(o.as_bytes()) {
                Some(n) => n,
                None => return keep_err(ErrorReply::not_a_valid_float()),
            },
        };
        // The increment argument is parsed AFTER the type + existing-value checks
        // (Redis parses argv[2] last in incrbyfloatCommand).
        let Some(incr) = parse_f64(&incr_arg) else {
            return keep_err(ErrorReply::not_a_valid_float());
        };
        let result = current + incr;
        // A NaN/Infinity result is rejected before any write (matching Redis).
        if result.is_nan() || result.is_infinite() {
            return keep_err(ErrorReply::increment_nan_or_inf());
        }
        // Store the result as a STRING (the human-formatted decimal). It is
        // classified int/embstr/raw by the store, so an integer-valued result like
        // "5" is int-encoded and a later INCR on it still works (matching Redis,
        // where INCRBYFLOAT then INCR round-trips for integer-valued results).
        let formatted = format_human_double(result);
        let bytes = Bytes::from(formatted.into_bytes());
        RmwStep {
            action: RmwAction::Replace(NewValueOwned::Bytes(bytes.clone())),
            expire: ExpireWrite::Unchanged,
            reply: Value::BulkString(Some(bytes)),
        }
    })
}

/// `APPEND key value` -> the new string length as a RESP integer.
///
/// If `key` is absent, APPEND behaves like SET (creates `key = value`) and returns
/// `len(value)`. If `key` holds a string, `value` is appended and the new length is
/// returned; an int-encoded value is promoted OFF the int encoding (the
/// concatenation of decimal digits + suffix is no longer a canonical integer).
/// WRONGTYPE if the existing value is not a string. The TTL is left unchanged
/// (Redis APPEND preserves it).
///
/// ENCODING DIVERGENCE (documented): Redis always reports `raw` after APPEND
/// (an appended SDS is never re-`embstr`'d). IronCache writes the rebuilt value
/// back through the frozen waist's `NewValueOwned::Bytes`, which the store
/// classifies by LENGTH (ENCODINGS.md): a short append result is therefore `embstr`
/// and only a result over the embstr threshold is `raw`. Forcing an always-`raw`
/// result would require a new write-value variant on the waist (a waist change,
/// out of scope and explicitly forbidden for PR-2b); the in-place spare-capacity
/// append that would also fix this is the #8/Efficiency follow-up below. OBJECT
/// ENCODING is a later PR, so this internal-representation difference is not yet
/// observable through a command.
///
/// FOLLOW-UP (#8/Efficiency): PR-2b implements APPEND as read-old + concat +
/// Replace (one owned rebuilt value through the frozen waist). The in-place spare
/// -capacity append (growing the existing buffer without a full rebuild) is the
/// documented value-internal-mutation extension to the waist (STORAGE_API.md notes
/// the "APPEND/SETRANGE efficiency path" as the additive `RmwAction` follow-up); it
/// is intentionally NOT done here.
pub fn cmd_append<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("append"));
    }
    let suffix = req.args[2].clone();
    store.rmw(db, &req.args[1], now, move |entry| match entry {
        // Absent: APPEND behaves like SET, returns len(value), clears TTL (SET
        // semantics on create).
        RmwEntry::Vacant => {
            let len = suffix.len() as i64;
            RmwStep {
                action: RmwAction::Insert(NewValueOwned::Bytes(suffix)),
                expire: ExpireWrite::Clear,
                reply: Value::Integer(len),
            }
        }
        // Non-string: WRONGTYPE, no write.
        RmwEntry::Occupied(o) if o.data_type() != DataType::String => {
            keep_err(ErrorReply::wrong_type())
        }
        // String: concatenate old + value (binary-safe), return the new length.
        // The TTL is preserved (Redis APPEND does not touch the expire).
        RmwEntry::Occupied(o) => {
            let old = o.as_bytes();
            let mut combined = Vec::with_capacity(old.len() + suffix.len());
            combined.extend_from_slice(old);
            combined.extend_from_slice(&suffix);
            let len = combined.len() as i64;
            RmwStep {
                action: RmwAction::Replace(NewValueOwned::Bytes(Bytes::from(combined))),
                expire: ExpireWrite::Unchanged,
                reply: Value::Integer(len),
            }
        }
    })
}
