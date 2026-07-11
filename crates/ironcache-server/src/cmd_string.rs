// SPDX-License-Identifier: MIT OR Apache-2.0
//! String-type command handlers over the storage waist (COMMANDS.md strings,
//! ENCODINGS.md, EXPIRATION.md). PR-2a: GET, SET (NX/XX/GET/EX/PX/EXAT/PXAT/
//! KEEPTTL), SETNX, GETSET, STRLEN. Drop-in compatibility additions: GETRANGE /
//! SUBSTR (signed-range substring), SETRANGE (zero-pad-extend overwrite), GETDEL
//! (GET-then-DEL atomically), MSETNX (set all only if none exist, atomic).
//!
//! Every handler is a composition of the four storage primitives (STORAGE_API.md):
//! GET/STRLEN use `read`; plain SET uses `upsert`; conditional SET / SETNX / GETSET
//! use `rmw` so the observe-then-conditionally-write is atomic on the owning core.
//! WRONGTYPE is checked before any mutation (COMMANDS.md error contract). Time
//! enters only as the `now` deadline basis (ADR-0003); the command layer converts
//! relative EX/PX against `now` into the absolute [`UnixMillis`] the waist stores.

use crate::cmd_util::{ascii_upper, parse_f64, parse_i64, parse_i64_strict};
use bytes::Bytes;
use ironcache_expiry::TimingWheel;
use ironcache_protocol::{ErrorReply, Request, Value, format_human_double};
use ironcache_storage::{
    DataType, ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};

/// Normalize a Redis-style signed range `[start, end]` against a string of `len` bytes
/// into a HALF-OPEN `[lo, hi)` byte range (GETRANGE / SUBSTR semantics, src/t_string.c
/// `getrangeCommand`). A negative index counts from the end (`-1` is the last byte);
/// out-of-range ends are clamped to the string bounds. Returns `None` when the range is
/// empty (start past end after clamping, or an empty string), which the caller maps to the
/// empty bulk string. The arithmetic is done in `i64` so a huge negative index cannot
/// underflow.
fn clamp_range(start: i64, end: i64, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len_i = len as i64;
    // A negative index is relative to the end; then clamp to [0, len-1] for start and
    // [0, len-1] for end (Redis clamps both ends into the string before comparing).
    let mut s = if start < 0 { len_i + start } else { start };
    let mut e = if end < 0 { len_i + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        e = 0;
    }
    if e >= len_i {
        e = len_i - 1;
    }
    // An empty result: start clamped past the (clamped) end, or start beyond the string.
    if s > e || s >= len_i {
        return None;
    }
    // s..=e is non-empty and in-bounds; return the half-open [s, e+1).
    Some((s as usize, (e + 1) as usize))
}

/// `GETRANGE key start end` (and its deprecated alias `SUBSTR`) -> the substring of the
/// value at `key` in the inclusive signed byte range `[start, end]`, or the empty bulk
/// string when the key is missing, the value is empty, or the range is empty after
/// clamping. Negative indices count from the end (`-1` is the last byte). WRONGTYPE on a
/// non-string. `cmd_name` selects the arity-error spelling (`getrange` vs `substr`); the
/// two are otherwise byte-identical (Redis `SUBSTR` is a literal alias of `GETRANGE`).
fn getrange_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    cmd_name: &str,
) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let (Some(start), Some(end)) = (parse_i64(&req.args[2]), parse_i64(&req.args[3])) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    match store.read(db, &req.args[1], now) {
        Some(v) if v.data_type() == DataType::String => {
            let bytes = v.as_bytes();
            match clamp_range(start, end, bytes.len()) {
                Some((lo, hi)) => Value::bulk(Bytes::copy_from_slice(&bytes[lo..hi])),
                // An empty range / empty value -> the EMPTY bulk string (NOT nil), matching
                // Redis (`getrangeCommand` replies an empty bulk for an empty result).
                None => Value::bulk(Bytes::new()),
            }
        }
        Some(_) => Value::error(ErrorReply::wrong_type()),
        // A MISSING key -> the empty bulk string (Redis GETRANGE on a missing key is "").
        None => Value::bulk(Bytes::new()),
    }
}

/// `GETRANGE key start end` -> the inclusive signed-range substring (empty bulk on miss /
/// empty range). Negative indices count from the end. WRONGTYPE on a non-string.
pub fn cmd_getrange<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    getrange_generic(store, db, now, req, "getrange")
}

/// `SUBSTR key start end` -> the deprecated alias of [`cmd_getrange`] (Redis keeps SUBSTR
/// as a literal alias, identical semantics; only the arity-error spelling differs).
pub fn cmd_substr<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    getrange_generic(store, db, now, req, "substr")
}

/// `SETRANGE key offset value` -> the new string length after overwriting the bytes
/// starting at `offset` with `value`, zero-padding the gap if `offset` is past the current
/// end (Redis `setrangeCommand`).
///
/// - A non-integer or NEGATIVE offset is an error (`offset is out of range` for negative).
/// - An empty `value` is a no-op that returns the CURRENT length (0 on a missing key),
///   NEVER creating the key (Redis: SETRANGE with an empty string on a missing key returns
///   0 and creates nothing).
/// - Otherwise the value is created/extended (zero-padded up to `offset`), the bytes are
///   overwritten, and the new length is returned. The TTL is preserved (Redis SETRANGE
///   does not touch the expire). WRONGTYPE on a non-string. The result is capped at the LIVE
///   `proto-max-bulk-len` (`max_bulk_len`, default 512 MB, runtime-settable), matching Redis
///   `checkStringLength`.
pub fn cmd_setrange<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    max_bulk_len: usize,
) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("setrange"));
    }
    // The offset is a non-negative integer; a negative offset is the out-of-range error
    // (Redis `setrangeCommand` rejects offset < 0 with "offset is out of range").
    let Some(offset) = parse_i64(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    if offset < 0 {
        return Value::error(ErrorReply::err("offset is out of range"));
    }
    let offset = offset as usize;
    let value = req.args[3].clone();
    store.rmw(db, &req.args[1], now, move |entry| {
        // The current bytes (absent -> empty), with the WRONGTYPE check first.
        let current: &[u8] = match &entry {
            RmwEntry::Vacant => b"",
            RmwEntry::Occupied(o) if o.data_type() != DataType::String => {
                return keep_err(ErrorReply::wrong_type());
            }
            RmwEntry::Occupied(o) => o.as_bytes(),
            RmwEntry::OccupiedMut(_) => unreachable!("cmd_setrange uses rmw, not rmw_mut"),
        };
        // An empty value is a no-op: return the CURRENT length, write nothing (Redis returns
        // the existing length and does NOT create a missing key).
        if value.is_empty() {
            return RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: Value::Integer(current.len() as i64),
            };
        }
        // The result length is max(current_len, offset + value_len). Reject if it would
        // exceed proto-max-bulk-len (Redis checkStringLength), before allocating.
        let end = offset.saturating_add(value.len());
        let new_len = current.len().max(end);
        if new_len > max_bulk_len {
            return keep_err(ErrorReply::string_exceeds_max());
        }
        // Build the new value: the prefix (old bytes, zero-padded up to offset), then the
        // overwrite, then any old tail beyond the overwrite (preserved).
        let mut buf = vec![0u8; new_len];
        buf[..current.len()].copy_from_slice(current);
        buf[offset..end].copy_from_slice(&value);
        RmwStep {
            // The TTL is PRESERVED (Redis SETRANGE keeps the existing expire); a created
            // key has no TTL, which Unchanged also yields on the Vacant path.
            action: RmwAction::Replace(NewValueOwned::Bytes(Bytes::from(buf))),
            expire: ExpireWrite::Unchanged,
            reply: Value::Integer(new_len as i64),
        }
    })
}

/// `GETDEL key` -> the value at `key` (then atomically DELETES it), or nil if the key is
/// missing (Redis `getdelCommand`). WRONGTYPE on a non-string (no delete). One atomic
/// `rmw`: observe + delete together on the owning core.
pub fn cmd_getdel<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("getdel"));
    }
    store.rmw(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: Value::Null,
        },
        RmwEntry::Occupied(o) if o.data_type() != DataType::String => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: Value::error(ErrorReply::wrong_type()),
        },
        RmwEntry::Occupied(o) => {
            let val = Value::bulk(Bytes::copy_from_slice(o.as_bytes()));
            RmwStep {
                action: RmwAction::Delete,
                expire: ExpireWrite::Unchanged,
                reply: val,
            }
        }
        RmwEntry::OccupiedMut(_) => unreachable!("cmd_getdel uses rmw, not rmw_mut"),
    })
}

/// `MSETNX key value [key value ...]` -> 1 if EVERY key was set, 0 if NONE were set
/// (Redis `msetnxCommand`: the set is all-or-nothing -- if ANY key already exists, NOTHING
/// is written and the reply is 0). Every value clears any TTL on create (a created key has
/// no TTL).
///
/// Arity is -3 (the token + >= 1 key/value pair) AND `argc - 1` must be EVEN; an odd count
/// is the wrong-arity error (matching Redis `msetnxCommand`, which shares the MSET arity
/// check). A DUPLICATE key listed twice in the same MSETNX does NOT by itself fail (Redis
/// checks existence in the keyspace, not within the arg list; the second write wins), but if
/// that key already exists the whole command is a no-op 0.
///
/// SINGLE-SHARD handler: this runs the all-or-nothing check + write against the connection's
/// accept shard. A cross-shard SPANNING MSETNX needs a coordinator pre-check + write (the
/// atomic all-or-nothing across shards is the documented Stage-3 follow-up, like cross-shard
/// MULTI/EXEC); when the keys span shards the serve loop keeps the command HOME (the keys all
/// live on the one store there), preserving the atomic semantics.
pub fn cmd_msetnx<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    // Arity -3 (token + >= 1 pair) AND an EVEN number of key/value args.
    if req.args.len() < 3 || (req.args.len() - 1) % 2 != 0 {
        return Value::error(ErrorReply::wrong_arity("msetnx"));
    }
    // PASS 1: if ANY target key already exists (live), the whole command is a no-op 0.
    // (Redis `msetnxCommand` first scans every key with lookupKeyWrite and aborts before
    // writing anything if one is present.)
    let mut i = 1;
    while i + 1 < req.args.len() {
        if store.contains(db, &req.args[i], now) {
            return Value::Integer(0);
        }
        i += 2;
    }
    // PASS 2: none exist -> write them all (blind upsert per pair; a created key has no TTL).
    // A key listed twice in the args is written twice (last wins), matching Redis.
    let mut i = 1;
    while i + 1 < req.args.len() {
        store.upsert(
            db,
            &req.args[i],
            NewValue::Bytes(&req.args[i + 1]),
            ExpireWrite::Clear,
            now,
        );
        i += 2;
    }
    Value::Integer(1)
}

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

/// `MGET key [key ...]` -> a RESP array with one element per key: the bulk-string
/// value if the key holds a STRING, else the null bulk string.
///
/// MGET NEVER errors on a wrong type (matching Redis `mgetCommand`, which calls
/// `lookupKeyRead` + an `OBJ_STRING` check and emits a NULL for a non-string, NOT a
/// WRONGTYPE): both a MISSING key and a key holding a non-string value yield
/// [`Value::Null`] (the RESP2 `$-1` / RESP3 `_` null). It is read-only (no admission,
/// no write). Arity is -2 (the command token + at least one key).
///
/// This is the SINGLE-SHARD handler: it returns the values in `req.args[1..]` order.
/// The cross-shard SPANNING case is reassembled by [`crate::multikey`] from per-shard
/// sub-MGETs (which each call this), preserving each key's ORIGINAL argument position.
pub fn cmd_mget<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("mget"));
    }
    let mut out: Vec<Value> = Vec::with_capacity(req.args.len() - 1);
    for key in &req.args[1..] {
        // A missing key OR a non-string key both yield Null: MGET never WRONGTYPEs.
        let v = match store.read(db, key, now) {
            Some(v) if v.data_type() == DataType::String => {
                Value::BulkString(Some(Bytes::copy_from_slice(v.as_bytes())))
            }
            _ => Value::Null,
        };
        out.push(v);
    }
    Value::Array(Some(out))
}

/// The parsed SET option set (COMMANDS.md cross-cutting flags). Conflicting
/// options (`NX`+`XX`, more than one of `EX`/`PX`/`EXAT`/`PXAT`/`KEEPTTL`) are a
/// syntax error.
///
/// The condition options (`NX`, `XX`, `IFEQ`, `IFNE`) are MUTUALLY EXCLUSIVE (Redis
/// 8.4 compare-and-set, #412): at most one may appear. `IFEQ`/`IFNE` each carry their
/// comparison value (the arg after the keyword). The digest variants `IFDEQ`/`IFDNE`
/// are NOT implemented (they hash the value with Redis's internal digest, which we do
/// not reproduce byte-for-byte; left as a documented follow-up).
#[derive(Debug, Default)]
struct SetOptions {
    nx: bool,
    xx: bool,
    /// `IFEQ value`: write ONLY if the current value equals this. A missing key fails
    /// the condition (Redis: "if the key doesn't exist, it won't be created").
    ifeq: Option<Bytes>,
    /// `IFNE value`: write ONLY if the current value does NOT equal this. A missing key
    /// SATISFIES the condition (Redis: the key "will be created").
    ifne: Option<Bytes>,
    get: bool,
    ttl: TtlOption,
}

impl SetOptions {
    /// Whether any mutually-exclusive write condition (`NX`/`XX`/`IFEQ`/`IFNE`) is set.
    /// A second condition keyword is a syntax error (Redis 8.4: the condition options are
    /// mutually exclusive).
    fn has_condition(&self) -> bool {
        self.nx || self.xx || self.ifeq.is_some() || self.ifne.is_some()
    }
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
                if opts.has_condition() {
                    return Err(SetParseError::Syntax);
                }
                opts.nx = true;
                i += 1;
            }
            b"XX" => {
                if opts.has_condition() {
                    return Err(SetParseError::Syntax);
                }
                opts.xx = true;
                i += 1;
            }
            // IFEQ/IFNE (Redis 8.4 compare-and-set, #412): each consumes the NEXT arg as
            // its comparison value POSITIONALLY, so a comparison value that looks like an
            // option (e.g. `IFEQ NX`) is taken literally, never re-parsed. A second
            // condition keyword or a missing value is a syntax error.
            b"IFEQ" => {
                if opts.has_condition() || i + 1 >= args.len() {
                    return Err(SetParseError::Syntax);
                }
                opts.ifeq = Some(args[i + 1].clone());
                i += 2;
            }
            b"IFNE" => {
                if opts.has_condition() || i + 1 >= args.len() {
                    return Err(SetParseError::Syntax);
                }
                opts.ifne = Some(args[i + 1].clone());
                i += 2;
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

/// A no-write rmw step returning `reply` (value and TTL untouched): the SET abort path
/// (a WRONGTYPE old value, or a write condition that did not fire).
fn keep_reply(reply: Value) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply,
    }
}

/// Evaluate the SET write condition against the observed old value. `old` is the current
/// STRING value (`Some`) for an occupied string key, or `None` for a vacant key OR a plain
/// NX/XX SET that did not observe the value (there `occupied` carries presence). The
/// condition options are mutually exclusive (the parser enforces it), so at most one branch
/// applies; a SET with no condition (the fast path) never reaches here.
///
/// - **IFEQ** (Redis 8.4): write only if present AND equal; a missing key FAILS (never
///   created).
/// - **IFNE**: write only if absent OR (present AND not equal); a missing key is created.
/// - **NX** (only if absent) / **XX** (only if present).
fn write_condition(opts: &SetOptions, old: Option<&[u8]>, occupied: bool) -> bool {
    if let Some(expected) = &opts.ifeq {
        old == Some(expected.as_ref())
    } else if let Some(expected) = &opts.ifne {
        old != Some(expected.as_ref())
    } else {
        (!opts.nx || !occupied) && (!opts.xx || occupied)
    }
}

/// `SET key value [NX|XX|IFEQ cmp|IFNE cmp] [GET] [EX s|PX ms|EXAT ts|PXAT tms|KEEPTTL]`.
///
/// Plain SET is a blind `upsert`. Any conditional (NX/XX/IFEQ/IFNE) or observing (GET)
/// form, or KEEPTTL, goes through `rmw` so the observe-then-write is atomic. Reply is
/// `+OK`/null per Redis, or (with GET) the old value / WRONGTYPE.
///
/// IFEQ/IFNE are the Redis 8.4 native compare-and-set options (#412): the write fires only
/// when the current string value equals (IFEQ) or differs from (IFNE) the comparison value,
/// collapsing the WATCH/MULTI/EXEC CAS round trip into one server-side step.
///
/// `wheel` registers the resolved deadline after a SET that actually wrote a TTL
/// (EX/PX/EXAT/PXAT), so the active drain can reclaim it; the lazy backstop in the
/// store remains the correctness guarantee, so this registration is best-effort.
pub fn cmd_set<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
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
    // The absolute deadline an EX/PX/EXAT/PXAT SET sets (for the wheel registration).
    let set_deadline = if let TtlOption::Set(at) = opts.ttl {
        Some(at)
    } else {
        None
    };

    // The fast path: plain SET with no NX/XX/IFEQ/IFNE/GET is a blind upsert.
    if !opts.has_condition() && !opts.get {
        store.upsert(db, &req.args[1], NewValue::Bytes(&req.args[2]), expire, now);
        if let Some(at) = set_deadline {
            wheel.register(db, &req.args[1], at);
        }
        return Value::ok();
    }

    // The conditional / observing path: one atomic rmw observes the old value,
    // applies the write condition (NX/XX/IFEQ/IFNE), captures GET's old value, and writes.
    let key = req.args[1].clone();
    let new_val = req.args[2].clone();
    // Tracks whether the rmw actually performed the write (so the deadline is
    // registered only when a TTL was really set, not on a condition no-op or WRONGTYPE).
    let mut wrote = false;
    let reply = store.rmw(db, &key, now, |entry| {
        let occupied = matches!(entry, RmwEntry::Occupied(_));
        // We must observe the old value only when GET returns it or IFEQ/IFNE compares it.
        // A plain NX/XX SET skips the read (presence alone gates it).
        let need_old = opts.get || opts.ifeq.is_some() || opts.ifne.is_some();

        // Observe ONCE: compute the write condition and GET's reply together. A non-string
        // old value is WRONGTYPE for GET or an IFEQ/IFNE compare (aborts with no write).
        // IFEQ/IFNE compare the value IN PLACE (no copy on the CAS hot path); only GET
        // copies it out for the reply.
        let (allowed, get_reply) = if need_old {
            match &entry {
                RmwEntry::Occupied(o) if o.data_type() == DataType::String => {
                    let cur = o.as_bytes();
                    (
                        write_condition(&opts, Some(cur), occupied),
                        opts.get
                            .then(|| Value::BulkString(Some(Bytes::copy_from_slice(cur)))),
                    )
                }
                RmwEntry::Occupied(_) => {
                    // WRONGTYPE on the old value aborts the SET with no write.
                    return keep_reply(Value::error(ErrorReply::wrong_type()));
                }
                RmwEntry::Vacant => (
                    write_condition(&opts, None, occupied),
                    opts.get.then_some(Value::Null),
                ),
                // The read-only `rmw` primitive never yields the in-place-mutation
                // arm (that comes only from `rmw_mut`); this is unreachable here.
                RmwEntry::OccupiedMut(_) => unreachable!("cmd_set uses rmw, not rmw_mut"),
            }
        } else {
            (write_condition(&opts, None, occupied), None)
        };

        if !allowed {
            // Condition not met: no write. Reply is the GET old value if GET was given,
            // else null (Redis: a SET condition that does not fire -> nil).
            return keep_reply(get_reply.unwrap_or(Value::Null));
        }

        // Write the new value with the resolved TTL effect.
        wrote = true;
        let reply = get_reply.unwrap_or_else(Value::ok);
        RmwStep {
            action: RmwAction::Replace(NewValueOwned::Bytes(new_val)),
            expire,
            reply,
        }
    });

    if wrote {
        if let Some(at) = set_deadline {
            wheel.register(db, &key, at);
        }
    }
    reply
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
        // Unreachable: SETNX uses the read-only `rmw`, never `rmw_mut`.
        RmwEntry::OccupiedMut(_) => unreachable!("cmd_setnx uses rmw, not rmw_mut"),
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
        // Unreachable: GETSET uses the read-only `rmw`, never `rmw_mut`.
        RmwEntry::OccupiedMut(_) => unreachable!("cmd_getset uses rmw, not rmw_mut"),
    })
}

/// `MSET key value [key value ...]` -> always `+OK`.
///
/// Sets each `key` to its `value` (a string), CLEARING any existing TTL
/// ([`ExpireWrite::Clear`], the default SET semantics) and overwriting any existing
/// value/type (a blind `upsert` per pair, like plain SET). It is a `denyoom` write.
///
/// Arity is -3 (the command token + at least one key/value pair) AND `argc - 1` must
/// be EVEN (the key/value args pair up): an odd count is the wrong-arity error
/// `wrong number of arguments for 'mset'`, matching Redis (`msetGenericCommand`
/// rejects an even total argc -- i.e. an odd number of key+value args -- with the
/// wrong-arity reply). The Min(3) table arity catches the empty case; the even-pairs
/// check is enforced HERE (the command table has no "even" rule).
///
/// `wheel` mirrors [`cmd_set`]'s signature (SET takes the wheel to register TTLs); MSET
/// never sets a TTL (it CLEARS), so no wheel registration is needed, but the parameter
/// is kept so the dispatch arm threads it uniformly and a future MSET-with-TTL variant
/// has the seam. This is the SINGLE-SHARD handler; [`crate::multikey`] reassembles the
/// cross-shard SPANNING case from per-shard sub-MSETs (each calling this).
pub fn cmd_mset<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    // Arity -3 (token + >= 1 pair) AND an EVEN number of key/value args.
    if req.args.len() < 3 || (req.args.len() - 1) % 2 != 0 {
        return Value::error(ErrorReply::wrong_arity("mset"));
    }
    // The wheel is unused (MSET clears TTLs, registering none); name it so the unused
    // -parameter lint stays quiet while keeping cmd_set's signature shape.
    let _ = &wheel;
    let mut i = 1;
    while i + 1 < req.args.len() {
        // Blind upsert per pair: overwrite any existing value/type, clear any TTL.
        store.upsert(
            db,
            &req.args[i],
            NewValue::Bytes(&req.args[i + 1]),
            ExpireWrite::Clear,
            now,
        );
        i += 2;
    }
    Value::ok()
}

/// `DELIFEQ key value` -> `1` if the key was deleted (it held a STRING exactly equal to
/// `value`), `0` if it was not (the key is missing, or its value differs). WRONGTYPE if the
/// key holds a non-string value (Valkey 9.0 compare-and-delete, #412).
///
/// The lock-release / leader-key pattern: a holder deletes its key ONLY if the value is
/// still its own token, atomically, instead of GET-then-DEL (which can delete another
/// holder's freshly-written token after a TTL flip). One atomic `rmw`: observe the value,
/// compare IN PLACE (no copy), and delete on a match. NOT denyoom (a delete only frees
/// memory, like DEL/GETDEL).
pub fn cmd_delifeq<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("delifeq"));
    }
    let expected = req.args[2].clone();
    store.rmw(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep_reply(Value::Integer(0)),
        RmwEntry::Occupied(o) if o.data_type() != DataType::String => {
            keep_reply(Value::error(ErrorReply::wrong_type()))
        }
        RmwEntry::Occupied(o) => {
            if o.as_bytes() == expected.as_ref() {
                RmwStep {
                    action: RmwAction::Delete,
                    expire: ExpireWrite::Unchanged,
                    reply: Value::Integer(1),
                }
            } else {
                keep_reply(Value::Integer(0))
            }
        }
        // Unreachable: DELIFEQ uses the read-only `rmw`, never `rmw_mut`.
        RmwEntry::OccupiedMut(_) => unreachable!("cmd_delifeq uses rmw, not rmw_mut"),
    })
}

/// `MSETEX numkeys key value [key value ...] [NX | XX] [EX s | PX ms | EXAT ts | PXAT tms |
/// KEEPTTL]` -> `1` if all keys were set, `0` if the NX/XX condition was not met (Redis 8.4
/// atomic multi-key set with a shared expiration, #412; extends MSET/MSETNX with the SET
/// expiration options).
///
/// - **No NX/XX**: always sets every key (reply `1`).
/// - **NX**: sets all only if NONE of the keys exist; **XX**: only if ALL exist. The gate is
///   evaluated over every key BEFORE any write, so it is all-or-nothing (reply `0` on a miss,
///   nothing written). An expired key counts as absent.
/// - The expiration option applies the SAME deadline to every key; with KEEPTTL each key
///   keeps its existing TTL; with NO option the keys are written WITHOUT a TTL (MSET default,
///   clearing any prior TTL). A `denyoom` write.
///
/// `numkeys` is the COUNT of key/value PAIRS; the keys sit at `args[2]`, `args[4]`, ... (the
/// strided `MsetexNumkeysStrided` key spec extracts exactly these for the ACL key-pattern
/// check, so no key bypasses it). This is the SINGLE-SHARD handler (all keys on the accept
/// shard, like MSETNX); a cross-shard spanning form is deferred to the coordinator.
#[allow(clippy::too_many_lines)]
pub fn cmd_msetex<S: Store>(
    store: &mut S,
    wheel: &mut TimingWheel,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    // MSETEX numkeys key value [...] [options]: at least the token + numkeys + one pair.
    if req.args.len() < 4 {
        return Value::error(ErrorReply::wrong_arity("msetex"));
    }
    // numkeys: a positive integer (the count of key/value pairs).
    let Some(numkeys) = parse_i64(&req.args[1]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    if numkeys <= 0 {
        return Value::error(ErrorReply::numkeys_should_be_positive());
    }
    let numkeys = numkeys as usize;
    // The pairs occupy args[2 .. pairs_end]; reject an overflow or too-few-args-for-numkeys.
    let Some(pairs_end) = numkeys.checked_mul(2).and_then(|n| n.checked_add(2)) else {
        return Value::error(ErrorReply::wrong_arity("msetex"));
    };
    if pairs_end > req.args.len() {
        return Value::error(ErrorReply::wrong_arity("msetex"));
    }

    // Parse the option tail (after the pairs): NX|XX (mutually exclusive) and at most one
    // expiration option. GET is NOT an MSETEX option (unlike SET).
    let mut nx = false;
    let mut xx = false;
    let mut ttl = TtlOption::None;
    let mut ttl_seen = false;
    let mut i = pairs_end;
    while i < req.args.len() {
        let up = ascii_upper(&req.args[i]);
        match up.as_slice() {
            b"NX" => {
                if nx || xx {
                    return Value::error(ErrorReply::syntax_error());
                }
                nx = true;
                i += 1;
            }
            b"XX" => {
                if nx || xx {
                    return Value::error(ErrorReply::syntax_error());
                }
                xx = true;
                i += 1;
            }
            b"KEEPTTL" => {
                if ttl_seen {
                    return Value::error(ErrorReply::syntax_error());
                }
                ttl_seen = true;
                ttl = TtlOption::Keep;
                i += 1;
            }
            kw @ (b"EX" | b"PX" | b"EXAT" | b"PXAT") => {
                if ttl_seen || i + 1 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                ttl_seen = true;
                let Some(n) = parse_i64(&req.args[i + 1]) else {
                    return Value::error(ErrorReply::not_an_integer());
                };
                match resolve_ttl(kw, n, now) {
                    Ok(at) => ttl = TtlOption::Set(at),
                    Err(_) => return Value::error(ErrorReply::invalid_expire_time("msetex")),
                }
                i += 2;
            }
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    }

    let expire = match ttl {
        TtlOption::None => ExpireWrite::Clear,
        TtlOption::Keep => ExpireWrite::Keep,
        TtlOption::Set(at) => ExpireWrite::Set(at),
    };
    let set_deadline = if let TtlOption::Set(at) = ttl {
        Some(at)
    } else {
        None
    };

    // The NX/XX gate over EVERY key first (atomic all-or-nothing): NX requires every key
    // absent, XX requires every key present. An expired key counts as absent (lazy expiry via
    // `contains(now)`). On a miss, nothing is written and the reply is 0. There is no
    // concurrency within this single-shard handler, so the gather-then-write is atomic.
    if nx || xx {
        let mut j = 2;
        while j < pairs_end {
            let exists = store.contains(db, &req.args[j], now);
            if (nx && exists) || (xx && !exists) {
                return Value::Integer(0);
            }
            j += 2;
        }
    }

    // Gate passed (or unconditional): blind-upsert every pair with the shared expire effect,
    // registering each key's deadline in the wheel when a TTL was set.
    let mut j = 2;
    while j < pairs_end {
        store.upsert(
            db,
            &req.args[j],
            NewValue::Bytes(&req.args[j + 1]),
            expire,
            now,
        );
        if let Some(at) = set_deadline {
            wheel.register(db, &req.args[j], at);
        }
        j += 2;
    }
    Value::Integer(1)
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
            // Unreachable: the numeric RMW uses the read-only `rmw`, never `rmw_mut`.
            RmwEntry::OccupiedMut(_) => unreachable!("incr_by uses rmw, not rmw_mut"),
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
            // Unreachable: INCRBYFLOAT uses the read-only `rmw`, never `rmw_mut`.
            RmwEntry::OccupiedMut(_) => unreachable!("cmd_incrbyfloat uses rmw, not rmw_mut"),
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
/// The DEFAULT `proto-max-bulk-len` string ceiling (512 MB, the Redis default). A single
/// string value may not exceed this; the only commands that can grow a value are APPEND /
/// SETRANGE, capped exactly like Redis `checkStringLength` (src/t_string.c). The LIVE
/// ceiling is RUNTIME-SETTABLE (`CONFIG SET proto-max-bulk-len`): the dispatch reads
/// `ctx.runtime.proto_max_bulk_len()` and threads it into the handlers as `max_bulk_len`.
/// This default keeps every value below 4 GiB, which the store's manual Str-blob allocator
/// depends on (its u32 length prefix must not truncate); a `CONFIG SET` cannot exceed the
/// decoder Limits the same connection was built with, which is itself bounded. Mirrors the
/// bitmap module's `PROTO_MAX_BIT_OFFSET`, which derives the same 512 MB default ceiling.
/// Kept as the test-side pin of the default (the live value is sourced from
/// `ironcache_config::DEFAULT_PROTO_MAX_BULK_LEN`, threaded in as `max_bulk_len`).
#[cfg(test)]
pub(crate) const PROTO_MAX_BULK_LEN: usize = 512 * 1024 * 1024;

/// Whether an APPEND of `suffix_len` bytes onto an `old_len`-byte string would exceed
/// the LIVE `proto-max-bulk-len` ceiling `max_bulk_len` (Redis `checkStringLength`: reject
/// when the RESULT would be larger). `checked_add` treats a `usize` overflow as "exceeds"
/// (defensive; unreachable for real values bounded by the per-bulk decode limit).
#[must_use]
pub(crate) fn append_would_exceed_max(
    old_len: usize,
    suffix_len: usize,
    max_bulk_len: usize,
) -> bool {
    old_len
        .checked_add(suffix_len)
        .is_none_or(|total| total > max_bulk_len)
}

pub fn cmd_append<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    max_bulk_len: usize,
) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("append"));
    }
    let suffix = req.args[2].clone();
    store.rmw(db, &req.args[1], now, move |entry| match entry {
        // Absent: APPEND behaves like SET, returns len(value), clears TTL (SET
        // semantics on create). The suffix alone is bounded by the per-bulk decode
        // limit (proto-max-bulk-len), so no ceiling check is needed on create.
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
            // Reject (BEFORE allocating the combined buffer) when the result would
            // exceed proto-max-bulk-len, exactly like Redis checkStringLength. This
            // both matches Redis and keeps the value below the store's 4 GiB blob
            // limit; no write occurs.
            if append_would_exceed_max(old.len(), suffix.len(), max_bulk_len) {
                return keep_err(ErrorReply::string_exceeds_max());
            }
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
        // Unreachable: APPEND uses the read-only `rmw`, never `rmw_mut`.
        RmwEntry::OccupiedMut(_) => unreachable!("cmd_append uses rmw, not rmw_mut"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::CountingAccounting;
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

    fn err_line(v: &Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    #[test]
    fn append_ceiling_matches_proto_max_bulk_len() {
        // Exactly at the ceiling is allowed; one byte over is rejected (Redis
        // checkStringLength rejects when the RESULT would EXCEED the limit).
        assert!(!append_would_exceed_max(
            PROTO_MAX_BULK_LEN,
            0,
            PROTO_MAX_BULK_LEN
        ));
        assert!(!append_would_exceed_max(
            PROTO_MAX_BULK_LEN - 1,
            1,
            PROTO_MAX_BULK_LEN
        ));
        assert!(append_would_exceed_max(
            PROTO_MAX_BULK_LEN,
            1,
            PROTO_MAX_BULK_LEN
        ));
        assert!(append_would_exceed_max(
            PROTO_MAX_BULK_LEN - 1,
            2,
            PROTO_MAX_BULK_LEN
        ));
        // A small append onto a small value is never rejected (under the default ceiling).
        assert!(!append_would_exceed_max(0, 0, PROTO_MAX_BULK_LEN));
        assert!(!append_would_exceed_max(10, 10, PROTO_MAX_BULK_LEN));
        // A usize-overflowing pair is treated as "exceeds" (defensive, unreachable).
        assert!(append_would_exceed_max(usize::MAX, 1, PROTO_MAX_BULK_LEN));
        // A LOWERED ceiling (Area B `CONFIG SET proto-max-bulk-len`) rejects past the new bound:
        // an append that fit under 512 MB now fails under a 16-byte ceiling.
        assert!(!append_would_exceed_max(8, 8, 16));
        assert!(append_would_exceed_max(8, 9, 16));
    }

    // ---- Competitor-regression lock-in: SET condition mutual-exclusivity. ----

    #[test]
    fn set_write_conditions_are_mutually_exclusive() {
        // Class of bug: a competitor's SET parser accepted invalid syntax that MIXED the
        // compare-and-set conditions (IFEQ/IFNE) with the presence conditions (NX/XX), which
        // are mutually exclusive. Our defense: `parse_set_options` gates every one of
        // NX/XX/IFEQ/IFNE behind `has_condition()`, so a second write condition is a syntax
        // error. (IFEQ/IFNE consume their comparison value POSITIONALLY, so `IFEQ hello`
        // reads `hello` as the value and a trailing `NX` is the illegal second condition.)
        let mut store = test_store();
        let mut wheel = TimingWheel::new();

        // Each of these combines two write conditions and must be `-ERR syntax error`.
        let bad_ifeq_then_nx = cmd_set(
            &mut store,
            &mut wheel,
            0,
            NOW,
            &req(&[b"SET", b"k", b"v", b"IFEQ", b"hello", b"NX"]),
        );
        assert_eq!(err_line(&bad_ifeq_then_nx), "-ERR syntax error");
        let bad_nx_xx = cmd_set(
            &mut store,
            &mut wheel,
            0,
            NOW,
            &req(&[b"SET", b"k", b"v", b"NX", b"XX"]),
        );
        assert_eq!(err_line(&bad_nx_xx), "-ERR syntax error");
        let bad_ifeq_ifne = cmd_set(
            &mut store,
            &mut wheel,
            0,
            NOW,
            &req(&[b"SET", b"k", b"v", b"IFEQ", b"a", b"IFNE", b"b"]),
        );
        assert_eq!(err_line(&bad_ifeq_ifne), "-ERR syntax error");
        // No write leaked through from any rejected form.
        assert_eq!(
            cmd_get(&mut store, 0, NOW, &req(&[b"GET", b"k"])),
            Value::Null
        );

        // A SINGLE condition still parses and applies. NX on an absent key writes.
        assert_eq!(
            cmd_set(
                &mut store,
                &mut wheel,
                0,
                NOW,
                &req(&[b"SET", b"a", b"1", b"NX"])
            ),
            Value::ok()
        );
        assert_eq!(
            cmd_get(&mut store, 0, NOW, &req(&[b"GET", b"a"])),
            Value::BulkString(Some(Bytes::from_static(b"1")))
        );
        // IFEQ against the real current value applies (compare-and-set).
        cmd_set(
            &mut store,
            &mut wheel,
            0,
            NOW,
            &req(&[b"SET", b"b", b"old"]),
        );
        assert_eq!(
            cmd_set(
                &mut store,
                &mut wheel,
                0,
                NOW,
                &req(&[b"SET", b"b", b"new", b"IFEQ", b"old"])
            ),
            Value::ok()
        );
        assert_eq!(
            cmd_get(&mut store, 0, NOW, &req(&[b"GET", b"b"])),
            Value::BulkString(Some(Bytes::from_static(b"new")))
        );
    }
}
