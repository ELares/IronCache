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

use crate::cmd_util::{ascii_upper, parse_i64};
use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value};
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

/// Parse the SET option tail (args after key and value), or `None` on a syntax
/// error (conflicting/unknown options or a bad/overflowing TTL value).
fn parse_set_options(args: &[Bytes], now: UnixMillis) -> Option<SetOptions> {
    let mut opts = SetOptions::default();
    let mut ttl_seen = false;
    let mut i = 0;
    while i < args.len() {
        let up = ascii_upper(&args[i]);
        match up.as_slice() {
            b"NX" => {
                if opts.nx || opts.xx {
                    return None;
                }
                opts.nx = true;
                i += 1;
            }
            b"XX" => {
                if opts.nx || opts.xx {
                    return None;
                }
                opts.xx = true;
                i += 1;
            }
            b"GET" => {
                if opts.get {
                    return None;
                }
                opts.get = true;
                i += 1;
            }
            b"KEEPTTL" => {
                if ttl_seen {
                    return None;
                }
                ttl_seen = true;
                opts.ttl = TtlOption::Keep;
                i += 1;
            }
            kw @ (b"EX" | b"PX" | b"EXAT" | b"PXAT") => {
                if ttl_seen || i + 1 >= args.len() {
                    return None;
                }
                ttl_seen = true;
                let n = parse_i64(&args[i + 1])?;
                opts.ttl = TtlOption::Set(resolve_ttl(kw, n, now)?);
                i += 2;
            }
            _ => return None,
        }
    }
    Some(opts)
}

/// Resolve a TTL keyword + numeric argument into an absolute deadline. EX/PX are
/// relative to `now`; EXAT/PXAT are absolute. A non-positive relative TTL or an
/// out-of-range value is a syntax/expire error -> `None` (the caller maps it to a
/// syntax error, matching Redis's "invalid expire time" being rejected before any
/// write). PR-2a treats any non-positive or overflowing value as rejected.
fn resolve_ttl(kw: &[u8], n: i64, now: UnixMillis) -> Option<UnixMillis> {
    let abs_millis: i64 = match kw {
        b"EX" => now_plus(now, n.checked_mul(1_000)?)?,
        b"PX" => now_plus(now, n)?,
        b"EXAT" => n.checked_mul(1_000)?,
        b"PXAT" => n,
        _ => return None,
    };
    if abs_millis <= 0 {
        return None;
    }
    Some(UnixMillis(abs_millis as u64))
}

/// `now + delta_millis` as an i64, rejecting a non-positive delta (Redis rejects a
/// non-positive EX/PX as an invalid expire time) and overflow.
fn now_plus(now: UnixMillis, delta_millis: i64) -> Option<i64> {
    if delta_millis <= 0 {
        return None;
    }
    i64::try_from(now.0).ok()?.checked_add(delta_millis)
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
    let Some(opts) = parse_set_options(&req.args[3..], now) else {
        return Value::error(ErrorReply::syntax_error());
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
