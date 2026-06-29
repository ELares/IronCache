// SPDX-License-Identifier: MIT OR Apache-2.0
//! Hash-type command handlers over the storage waist (COMMANDS.md hash semantics,
//! COLLECTIONS.md / OBJECT_ENCODING_MAPPING.md #40, the in-place-mutation RMW
//! extension, PR-6).
//!
//! Every MUTATING hash command routes through [`Store::rmw_mut`] (the collection
//! in-place-mutation arm): the closure edits the stored hash through the typed
//! [`HashValue`] view on [`RmwEntry::OccupiedMut`] and returns [`RmwAction::Mutated`]
//! (the store measures the byte delta, recomputes the encoding, and deletes the key
//! if the edit emptied the hash), or [`RmwAction::Insert`] to create the hash on a
//! missing key (HSET/HSETNX/HINCRBY/... on a vacant key), or [`RmwAction::Delete`]
//! when the handler knows the post-edit count is zero (e.g. HDEL that drains the last
//! field). READ-ONLY hash commands (HGET/HMGET/HGETALL/HKEYS/HVALS/HLEN/HEXISTS/
//! HSTRLEN/HRANDFIELD/HSCAN) also go through `rmw_mut` with [`RmwAction::Keep`] (no
//! write, no accounting change): the typed view is the only way to read hash contents
//! through the waist, and `Keep` leaves the value untouched.
//!
//! WRONGTYPE is checked before any mutation: a hash command on a non-hash key returns
//! `-WRONGTYPE` with no write (the typed [`OccupiedEntryMut::as_hash_mut`] returns
//! `None` for a non-hash, and the handler maps that to WRONGTYPE + `Keep`).
//!
//! A hash is NEVER stored empty: when the last field is removed (HDEL to empty) the
//! key is deleted (the store's empty-collection-deletes-key backstop, plus the explicit
//! `Delete` action where the handler can tell). So an empty hash is never observable,
//! matching Redis.
//!
//! ## HRANDFIELD determinism (ADR-0003)
//!
//! HRANDFIELD's randomness enters through the Env RNG seam: the CALLER (dispatch)
//! draws a seed `u64` and passes it in (mirroring RANDOMKEY); the store reads no RNG.
//! The handler derives its index choices deterministically from that one seed, so a
//! seeded replay is byte-identical.
//!
//! ## HSCAN cursor (KEYSPACE.md)
//!
//! For a HASHTABLE-encoded hash, HSCAN reuses the SAME hash-ordered cursor mechanism the
//! keyspace SCAN uses (`scan_plan`'s algorithm), applied to the hash's OWN field table:
//! the fields are ordered by a fixed-seed stable field hash ([`field_scan_hash`], the
//! command-layer analog of the store's `scan_hash`, kept here because the command layer
//! cannot name the concrete store, the layering contract), the cursor is the resume
//! threshold in that order, and an equal-hash group is never split.
//!
//! For a LISTPACK-encoded (small) hash, HSCAN returns the WHOLE hash in ONE reply with
//! next-cursor `0`, IGNORING COUNT, matching Redis's small-collection SCAN behavior
//! (KEYSPACE.md). The handler distinguishes the encoding via [`HashValue::is_listpack`].
//!
//! [`HashValue::is_listpack`]: ironcache_storage::HashValue::is_listpack

use crate::cmd_expire::{ExpireKind, resolve_expire_at};
use crate::cmd_util::{ascii_upper, parse_f64, parse_i64, parse_i64_strict};
use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value, format_human_double};
use ironcache_storage::{
    ExpireWrite, HashValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, ScanCursor, Store,
    UnixMillis,
};

/// One owned `(field, value)` byte pair, the shape [`HashValue::pairs`] returns and the
/// HSCAN/HRANDFIELD helpers operate on. A type alias so the helper signatures do not
/// repeat the (clippy-flagged) nested-tuple type.
///
/// [`HashValue::pairs`]: ironcache_storage::HashValue::pairs
type FieldValue = (Vec<u8>, Vec<u8>);

/// A no-write rmw step that returns `reply` (value untouched, TTL untouched). The
/// shared abort/short-circuit path for the hash handlers (WRONGTYPE, read replies).
fn keep(reply: Value) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply,
    }
}

/// The WRONGTYPE no-write step (a hash command against a non-hash key).
fn wrong_type() -> RmwStep<Value> {
    keep(Value::error(ErrorReply::wrong_type()))
}

/// The rmw step for a hash READ that lazily reaped expired fields first (#408). When the reap
/// removed a field the value CHANGED, so the step must be `Mutated` (the store re-measures the
/// size, recomputes the encoding, and deletes the key if the reap emptied the hash); with no
/// reap it is a pure `Keep`. `reply` is computed by the caller AFTER the reap, so it never
/// observes an expired field.
fn reaped_read(changed: bool, reply: Value) -> RmwStep<Value> {
    RmwStep {
        action: if changed {
            RmwAction::Mutated
        } else {
            RmwAction::Keep
        },
        expire: ExpireWrite::Unchanged,
        reply,
    }
}

/// Lazily reap fields expired at `now`, UNLESS the store is a PASSIVE replica (HA-7d). A
/// passive replica must NEVER expire on its own (key- or field-level): the authoritative
/// removal arrives only through the replication stream, so a replica that reaped here would
/// pre-empt the primary's expiry and diverge its local accounting (exactly what the key-level
/// `expire_if_due` guards against). On a passive replica this is a no-op returning `false` (no
/// physical removal, no change); on a primary/standalone it removes the expired fields and
/// returns whether any were removed. `passive` is read from [`Store::is_passive`] before the
/// `rmw` (the store is borrowed by the closure) and captured in. (#408)
fn reap_on_read(hash: &mut dyn HashValue, now: UnixMillis, passive: bool) -> bool {
    if passive {
        return false;
    }
    !hash.reap_expired_fields(now).is_empty()
}

/// A bulk reply from owned bytes.
fn bulk(bytes: Vec<u8>) -> Value {
    Value::BulkString(Some(Bytes::from(bytes)))
}

/// Build a CREATE-path hash value from `(field, value)` pairs (HSET/HSETNX/HINCRBY on a
/// missing key). The store builds the concrete hash via [`NewValueOwned::Hash`].
fn new_hash(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> NewValueOwned {
    NewValueOwned::hash(pairs)
}

// ---------------------------------------------------------------------------
// HSET / HMSET / HSETNX: write fields, creating the hash on a missing key.
// ---------------------------------------------------------------------------

/// `HSET key field value [field value ...]` -> the number of NEW fields added (existing
/// fields are updated in place and do NOT count). HMSET is the same write but replies
/// `+OK` (deprecated alias). `is_hmset` selects the reply shape.
fn hset_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    cmd_name: &str,
    is_hmset: bool,
) -> Value {
    // HSET key f v [f v ...]: at least one field/value pair, and the field/value tail
    // must be EVEN (Redis: "wrong number of arguments for HMSET" on an odd tail).
    if req.args.len() < 4 || (req.args.len() - 2) % 2 != 0 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    // The (field, value) pairs in argument order.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = req.args[2..]
        .chunks_exact(2)
        .map(|c| (c[0].to_vec(), c[1].to_vec()))
        .collect();
    let ok_reply = if is_hmset {
        Value::ok()
    } else {
        // Placeholder; the real integer is computed below.
        Value::Integer(0)
    };

    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => {
            // Create the hash from the pairs. Count the DISTINCT new fields (a repeated
            // field in one HSET counts once and keeps the LAST value, like Redis).
            let added = if is_hmset {
                0
            } else {
                count_distinct_fields(&pairs)
            };
            RmwStep {
                action: RmwAction::Insert(new_hash(pairs)),
                // A freshly created hash has no TTL (Redis: a created key has no TTL).
                expire: ExpireWrite::Clear,
                reply: if is_hmset {
                    ok_reply
                } else {
                    Value::Integer(added)
                },
            }
        }
        RmwEntry::OccupiedMut(mut o) => {
            let th = o.thresholds();
            let Some(hash) = o.as_hash_mut() else {
                return wrong_type();
            };
            let mut added: i64 = 0;
            for (f, v) in &pairs {
                if hash.set(f, v, &th) {
                    added += 1;
                }
                // A plain HSET/HMSET write removes the field's TTL (Redis hashTypeSet without
                // HASH_SET_KEEP_TTL): an overwritten field becomes persistent. A no-op on a
                // field that had no TTL (incl. every newly created field). HSETNX (only-if-
                // absent) and HINCRBY/HINCRBYFLOAT (which keep the TTL) do not go through here.
                hash.persist_field(f);
            }
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: if is_hmset {
                    ok_reply
                } else {
                    Value::Integer(added)
                },
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// Count the DISTINCT fields in a pair list (for HSET's new-field count on a created
/// hash: a repeated field counts once). O(n^2) over the (small) pair list, which is
/// fine for the create path; the OccupiedMut path counts via `set` returns instead.
fn count_distinct_fields(pairs: &[(Vec<u8>, Vec<u8>)]) -> i64 {
    let mut seen: Vec<&[u8]> = Vec::new();
    for (f, _) in pairs {
        if !seen.contains(&f.as_slice()) {
            seen.push(f);
        }
    }
    seen.len() as i64
}

/// `HSET key field value [field value ...]` -> the number of new fields added.
pub fn cmd_hset<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    hset_generic(store, db, now, req, "hset", false)
}

/// `HMSET key field value [field value ...]` -> `+OK` (deprecated alias of HSET).
pub fn cmd_hmset<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    hset_generic(store, db, now, req, "hmset", true)
}

/// `HSETNX key field value` -> 1 if the field was set (was absent), 0 if it already
/// existed. Creates the hash on a missing key. WRONGTYPE on a non-hash.
pub fn cmd_hsetnx<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("hsetnx"));
    }
    let field = req.args[2].to_vec();
    let value = req.args[3].to_vec();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(new_hash(vec![(field, value)])),
            expire: ExpireWrite::Clear,
            reply: Value::Integer(1),
        },
        RmwEntry::OccupiedMut(mut o) => {
            let th = o.thresholds();
            let Some(hash) = o.as_hash_mut() else {
                return wrong_type();
            };
            if hash.set_nx(&field, &value, &th) {
                RmwStep {
                    action: RmwAction::Mutated,
                    expire: ExpireWrite::Unchanged,
                    reply: Value::Integer(1),
                }
            } else {
                // Field already present: no write, reply 0.
                keep(Value::Integer(0))
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

// ---------------------------------------------------------------------------
// Read commands: HGET / HMGET / HGETALL / HKEYS / HVALS / HLEN / HEXISTS / HSTRLEN.
// They use rmw_mut with Keep (no write): the typed hash view reads through the waist.
// ---------------------------------------------------------------------------

/// `HGET key field` -> the field's value, or nil if the field/key is absent; WRONGTYPE
/// on a non-hash.
pub fn cmd_hget<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("hget"));
    }
    let passive = store.is_passive();
    let field = req.args[2].clone();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Null),
        RmwEntry::OccupiedMut(mut o) => match o.as_hash_mut() {
            Some(hash) => {
                let changed = reap_on_read(hash, now, passive);
                let reply = hash.get(&field).map_or(Value::Null, |v| bulk(v.to_vec()));
                reaped_read(changed, reply)
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `HMGET key field [field ...]` -> an array with each field's value or nil per missing
/// field (a missing KEY yields an array of nils, one per requested field); WRONGTYPE on
/// a non-hash.
pub fn cmd_hmget<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("hmget"));
    }
    let passive = store.is_passive();
    let fields: Vec<Bytes> = req.args[2..].to_vec();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        // Missing key: a nil per requested field (Redis HMGET on a missing key).
        RmwEntry::Vacant => keep(Value::Array(Some(
            fields.iter().map(|_| Value::Null).collect(),
        ))),
        RmwEntry::OccupiedMut(mut o) => match o.as_hash_mut() {
            Some(hash) => {
                let changed = reap_on_read(hash, now, passive);
                let out: Vec<Value> = fields
                    .iter()
                    .map(|f| hash.get(f).map_or(Value::Null, |v| bulk(v.to_vec())))
                    .collect();
                reaped_read(changed, Value::Array(Some(out)))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `HGETALL key` -> all field/value pairs. Under RESP3 this is a MAP (`%`); under RESP2
/// the encoder degrades the map to a flat `[field, value, ...]` array (ADR-0019). A
/// missing key is an empty map/array. WRONGTYPE on a non-hash.
pub fn cmd_hgetall<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("hgetall"));
    }
    let passive = store.is_passive();
    store.rmw_mut(db, &req.args[1], now, |entry| match entry {
        RmwEntry::Vacant => keep(Value::Map(Vec::new())),
        RmwEntry::OccupiedMut(mut o) => match o.as_hash_mut() {
            Some(hash) => {
                let changed = reap_on_read(hash, now, passive);
                let pairs: Vec<(Value, Value)> = hash
                    .pairs()
                    .into_iter()
                    .map(|(f, v)| (bulk(f), bulk(v)))
                    .collect();
                reaped_read(changed, Value::Map(pairs))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `HKEYS key` -> the array of fields (empty if absent); WRONGTYPE on a non-hash.
pub fn cmd_hkeys<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("hkeys"));
    }
    let passive = store.is_passive();
    store.rmw_mut(db, &req.args[1], now, |entry| match entry {
        RmwEntry::Vacant => keep(Value::Array(Some(Vec::new()))),
        RmwEntry::OccupiedMut(mut o) => match o.as_hash_mut() {
            Some(hash) => {
                let changed = reap_on_read(hash, now, passive);
                let out = hash.fields().into_iter().map(bulk).collect();
                reaped_read(changed, Value::Array(Some(out)))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `HVALS key` -> the array of values (empty if absent); WRONGTYPE on a non-hash.
pub fn cmd_hvals<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("hvals"));
    }
    let passive = store.is_passive();
    store.rmw_mut(db, &req.args[1], now, |entry| match entry {
        RmwEntry::Vacant => keep(Value::Array(Some(Vec::new()))),
        RmwEntry::OccupiedMut(mut o) => match o.as_hash_mut() {
            Some(hash) => {
                let changed = reap_on_read(hash, now, passive);
                let out = hash.values().into_iter().map(bulk).collect();
                reaped_read(changed, Value::Array(Some(out)))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `HLEN key` -> the field count, 0 if absent; WRONGTYPE on a non-hash.
pub fn cmd_hlen<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("hlen"));
    }
    let passive = store.is_passive();
    store.rmw_mut(db, &req.args[1], now, |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => match o.as_hash_mut() {
            Some(hash) => {
                let changed = reap_on_read(hash, now, passive);
                reaped_read(changed, Value::Integer(hash.len() as i64))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `HEXISTS key field` -> 1 if the field is present, 0 otherwise; WRONGTYPE on a
/// non-hash.
pub fn cmd_hexists<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("hexists"));
    }
    let passive = store.is_passive();
    let field = req.args[2].clone();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => match o.as_hash_mut() {
            Some(hash) => {
                let changed = reap_on_read(hash, now, passive);
                reaped_read(changed, Value::Integer(i64::from(hash.contains(&field))))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `HSTRLEN key field` -> the byte length of the field's value, 0 if the field/key is
/// absent; WRONGTYPE on a non-hash.
pub fn cmd_hstrlen<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("hstrlen"));
    }
    let passive = store.is_passive();
    let field = req.args[2].clone();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => match o.as_hash_mut() {
            Some(hash) => {
                let changed = reap_on_read(hash, now, passive);
                reaped_read(changed, Value::Integer(hash.strlen(&field) as i64))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

// ---------------------------------------------------------------------------
// HDEL: variadic field removal; emptying the hash deletes the key.
// ---------------------------------------------------------------------------

/// `HDEL key field [field ...]` -> the number of fields actually removed. Removing the
/// last field deletes the key (empty-collection-deletes-key). 0 on a missing key;
/// WRONGTYPE on a non-hash.
pub fn cmd_hdel<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("hdel"));
    }
    let fields: Vec<Bytes> = req.args[2..].to_vec();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(hash) = o.as_hash_mut() else {
                return wrong_type();
            };
            let mut removed: i64 = 0;
            for f in &fields {
                if hash.del(f) {
                    removed += 1;
                }
            }
            // If the removals emptied the hash, delete the key explicitly; else Mutated
            // (the store measures the delta). A no-op removal is still a Mutated with a
            // zero delta, which the store accounts as no change.
            let action = if hash.is_empty() {
                RmwAction::Delete
            } else {
                RmwAction::Mutated
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: Value::Integer(removed),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

// ---------------------------------------------------------------------------
// HINCRBY / HINCRBYFLOAT: read-modify-write a single field's numeric value.
// ---------------------------------------------------------------------------

/// `HINCRBY key field increment` -> the new value as a RESP integer. The field's
/// current value (absent -> 0) is parsed as a canonical i64; a non-integer stored value
/// is `-ERR hash value is not an integer`; an i64 overflow is the overflow error. A
/// non-integer INCREMENT argument is the generic not-an-integer error (parsed first).
/// Creates the hash/field on a missing key. WRONGTYPE on a non-hash.
pub fn cmd_hincrby<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("hincrby"));
    }
    let field = req.args[2].to_vec();
    // Redis parses the increment argument FIRST (getLongLongFromObjectOrReply); a
    // non-integer argument is the generic not-an-integer error, before any lookup.
    let Some(incr) = parse_i64_strict(&req.args[3]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    store.rmw_mut(db, &req.args[1], now, move |entry| {
        match entry {
            RmwEntry::Vacant => {
                // Absent key/field: current is 0, result is the increment. Create the
                // hash with the single field set to the decimal result.
                let result = incr;
                let pairs = vec![(field, result.to_string().into_bytes())];
                RmwStep {
                    action: RmwAction::Insert(new_hash(pairs)),
                    expire: ExpireWrite::Clear,
                    reply: Value::Integer(result),
                }
            }
            RmwEntry::OccupiedMut(mut o) => {
                let th = o.thresholds();
                let Some(hash) = o.as_hash_mut() else {
                    return wrong_type();
                };
                // The field's current value as i64 (absent field -> 0). A non-canonical
                // -integer stored value is the hash-value-not-an-integer error.
                let current: i64 = match hash.get(&field) {
                    None => 0,
                    Some(v) => match parse_i64_strict(v) {
                        Some(n) => n,
                        None => return keep(Value::error(ErrorReply::hash_value_not_an_integer())),
                    },
                };
                let Some(next) = current.checked_add(incr) else {
                    return keep(Value::error(ErrorReply::increment_overflow()));
                };
                hash.set(&field, next.to_string().as_bytes(), &th);
                RmwStep {
                    action: RmwAction::Mutated,
                    expire: ExpireWrite::Unchanged,
                    reply: Value::Integer(next),
                }
            }
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        }
    })
}

/// `HINCRBYFLOAT key field increment` -> the new value as a bulk string (the
/// human-formatted decimal). The field's current value (absent -> 0) is parsed as an
/// f64; a non-float stored value is `-ERR hash value is not a float`; a non-float
/// INCREMENT argument is the generic not-a-valid-float error; a NaN/Infinity result is
/// the NaN-or-Infinity error. Creates the hash/field on a missing key. WRONGTYPE on a
/// non-hash. Uses [`format_human_double`] (the same human spelling INCRBYFLOAT uses).
pub fn cmd_hincrbyfloat<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("hincrbyfloat"));
    }
    let field = req.args[2].to_vec();
    // Redis parses the increment argument FIRST (getLongDoubleFromObjectOrReply); a
    // non-float argument is the generic not-a-valid-float error, before any lookup.
    let Some(incr) = parse_f64(&req.args[3]) else {
        return Value::error(ErrorReply::not_a_valid_float());
    };
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => {
            let result = 0.0 + incr;
            if result.is_nan() || result.is_infinite() {
                return keep(Value::error(ErrorReply::increment_nan_or_inf()));
            }
            let formatted = format_human_double(result);
            let pairs = vec![(field, formatted.clone().into_bytes())];
            RmwStep {
                action: RmwAction::Insert(new_hash(pairs)),
                expire: ExpireWrite::Clear,
                reply: bulk(formatted.into_bytes()),
            }
        }
        RmwEntry::OccupiedMut(mut o) => {
            let th = o.thresholds();
            let Some(hash) = o.as_hash_mut() else {
                return wrong_type();
            };
            let current: f64 = match hash.get(&field) {
                None => 0.0,
                Some(v) => match parse_f64(v) {
                    Some(n) => n,
                    None => return keep(Value::error(ErrorReply::hash_value_not_a_float())),
                },
            };
            let result = current + incr;
            if result.is_nan() || result.is_infinite() {
                return keep(Value::error(ErrorReply::increment_nan_or_inf()));
            }
            let formatted = format_human_double(result);
            hash.set(&field, formatted.as_bytes(), &th);
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: bulk(formatted.into_bytes()),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

// ---------------------------------------------------------------------------
// HRANDFIELD: pseudo-random field selection. Caller draws the seed (Env RNG seam).
// ---------------------------------------------------------------------------

/// `HRANDFIELD key [count [WITHVALUES]]` -> one random field (no count), or `count`
/// fields (positive: DISTINCT, up to the hash length; negative: WITH REPEATS, exactly
/// `|count|`). WITHVALUES interleaves each field with its value. A missing key is nil
/// (no count) or an empty array (with count). `seed` is the random seed the dispatch
/// layer drew from the Env RNG (ADR-0003: the store/handler read no RNG; randomness
/// enters through the determinism seam).
pub fn cmd_hrandfield<S: Store>(
    store: &mut S,
    db: u32,
    seed: u64,
    now: UnixMillis,
    req: &Request,
) -> Value {
    // HRANDFIELD key | HRANDFIELD key count | HRANDFIELD key count WITHVALUES.
    if req.args.len() < 2 || req.args.len() > 4 {
        return Value::error(ErrorReply::wrong_arity("hrandfield"));
    }
    let passive = store.is_passive();
    // Parse the optional count + WITHVALUES.
    let count: Option<i64> = if req.args.len() >= 3 {
        match parse_i64(&req.args[2]) {
            Some(n) => Some(n),
            None => return Value::error(ErrorReply::not_an_integer()),
        }
    } else {
        None
    };
    let with_values = if req.args.len() == 4 {
        if ascii_upper(&req.args[3]).as_slice() == b"WITHVALUES" {
            true
        } else {
            return Value::error(ErrorReply::syntax_error());
        }
    } else {
        false
    };
    // WITHVALUES is only valid with a count (Redis: HRANDFIELD key WITHVALUES is a
    // syntax error; the count form is required). The arity check above already requires
    // 4 args for WITHVALUES, so a count is always present here.

    store.rmw_mut(db, &req.args[1], now, move |entry| {
        let (changed, hash) = match entry {
            RmwEntry::Vacant => {
                // Missing key: nil (no count) or empty array (with count).
                let reply = match count {
                    None => Value::Null,
                    Some(_) => Value::Array(Some(Vec::new())),
                };
                return keep(reply);
            }
            RmwEntry::OccupiedMut(mut o) => match o.as_hash_mut() {
                // Reap expired fields first, then take the deterministic (field, value) order
                // out of the view so the selection logic below can index it (the borrow of `o`
                // ends here).
                Some(hash) => {
                    let changed = reap_on_read(hash, now, passive);
                    (changed, hash.pairs())
                }
                None => return wrong_type(),
            },
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        };
        reaped_read(changed, hrandfield_reply(&hash, count, with_values, seed))
    })
}

/// Build the HRANDFIELD reply from the hash's (field, value) pairs in deterministic
/// order, the parsed `count`, the WITHVALUES flag, and the caller-drawn `seed`.
///
/// Determinism (ADR-0003): every index is derived from `seed` via a pure splitmix64
/// step, so a seeded replay is byte-identical. No std rand, no time.
fn hrandfield_reply(
    pairs: &[FieldValue],
    count: Option<i64>,
    with_values: bool,
    seed: u64,
) -> Value {
    let n = pairs.len();
    // n == 0 cannot happen (an empty hash is never stored), but be defensive.
    if n == 0 {
        return match count {
            None => Value::Null,
            Some(_) => Value::Array(Some(Vec::new())),
        };
    }
    let mut rng = SeedRng::new(seed);
    match count {
        // No count: one random field as a bulk.
        None => {
            let idx = (rng.next() % n as u64) as usize;
            bulk(pairs[idx].0.clone())
        }
        // Positive count: DISTINCT fields, up to min(count, n). A partial Fisher-Yates
        // shuffle of the indices gives a uniform distinct sample deterministically.
        Some(c) if c >= 0 => {
            let want = (c as usize).min(n);
            let mut idxs: Vec<usize> = (0..n).collect();
            for i in 0..want {
                let j = i + (rng.next() % (n - i) as u64) as usize;
                idxs.swap(i, j);
            }
            let chosen = &idxs[..want];
            build_field_array(pairs, chosen.iter().copied(), with_values)
        }
        // Negative count: WITH REPEATS, exactly |count| fields (each drawn independently).
        Some(c) => {
            let want = c.unsigned_abs() as usize;
            let chosen = (0..want).map(|_| (rng.next() % n as u64) as usize);
            build_field_array(pairs, chosen, with_values)
        }
    }
}

/// Build the HRANDFIELD reply from the chosen indices into `pairs`.
///
/// WITHOUT values: a flat array of fields (`Value::Array`). WITH values: a
/// [`Value::Pairs`] of `(field, value)` so the encoder NESTS each pair under RESP3 (an
/// array of 2-element arrays) and FLATTENS to a single `[field, value, ...]` array under
/// RESP2, matching Redis's WITHVALUES RESP2/RESP3 shapes.
fn build_field_array(
    pairs: &[FieldValue],
    chosen: impl Iterator<Item = usize>,
    with_values: bool,
) -> Value {
    if with_values {
        let out: Vec<(Value, Value)> = chosen
            .map(|i| (bulk(pairs[i].0.clone()), bulk(pairs[i].1.clone())))
            .collect();
        Value::Pairs(out)
    } else {
        let out: Vec<Value> = chosen.map(|i| bulk(pairs[i].0.clone())).collect();
        Value::Array(Some(out))
    }
}

/// A deterministic splitmix64 PRNG seeded from the caller's Env-drawn seed (ADR-0003:
/// no std rand; the seed is the ONLY entropy and enters through the determinism seam).
struct SeedRng {
    state: u64,
}

impl SeedRng {
    fn new(seed: u64) -> Self {
        SeedRng { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

// ---------------------------------------------------------------------------
// HSCAN: cursored iteration over the hash's field table.
// ---------------------------------------------------------------------------

/// The default COUNT hint for HSCAN when none is given (Redis SCAN-family default 10).
const HSCAN_DEFAULT_COUNT: usize = 10;

/// `HSCAN key cursor [MATCH pattern] [COUNT n] [NOVALUES]` -> the 2-element reply
/// `[next_cursor_bulkstring, [field, value, ...]]` (or `[next_cursor, [field, ...]]`
/// with NOVALUES). The cursor is the decimal wire token; `0` starts and a returned `0`
/// means complete. MATCH globs the FIELD; NOVALUES omits the values. A missing key is
/// `[0, []]`. WRONGTYPE on a non-hash.
///
/// A LISTPACK-encoded (small) hash returns ALL fields at once with cursor 0, ignoring
/// COUNT (Redis small-collection SCAN). A HASHTABLE-encoded hash reuses the SAME
/// hash-ordered cursor mechanism the keyspace SCAN uses: the fields are ordered by the
/// fixed-seed stable [`field_scan_hash`], the cursor is the resume threshold in that
/// order, and an equal-hash group is never split (so a colliding pair of fields is
/// returned together).
pub fn cmd_hscan<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("hscan"));
    }
    let passive = store.is_passive();
    let Some(cursor) = ScanCursor::from_token(&req.args[2]) else {
        return Value::error(ErrorReply::invalid_cursor());
    };
    // Parse the option tail: MATCH <pattern>, COUNT <n>, NOVALUES (a bare flag).
    let mut pattern: Option<Bytes> = None;
    let mut count: usize = HSCAN_DEFAULT_COUNT;
    let mut novalues = false;
    let mut i = 3;
    while i < req.args.len() {
        let opt = ascii_upper(&req.args[i]);
        match opt.as_slice() {
            b"MATCH" => {
                if i + 1 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                pattern = Some(req.args[i + 1].clone());
                i += 2;
            }
            b"COUNT" => {
                if i + 1 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                match parse_i64(&req.args[i + 1]) {
                    Some(n) if n > 0 => count = n as usize,
                    _ => return Value::error(ErrorReply::syntax_error()),
                }
                i += 2;
            }
            b"NOVALUES" => {
                novalues = true;
                i += 1;
            }
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    }

    store.rmw_mut(db, &req.args[1], now, move |entry| {
        let (changed, pairs, is_listpack) = match entry {
            // Missing key: complete (cursor 0) with an empty field list.
            RmwEntry::Vacant => {
                return keep(hscan_reply(ScanCursor::START, Vec::new(), novalues));
            }
            RmwEntry::OccupiedMut(mut o) => match o.as_hash_mut() {
                Some(hash) => {
                    let changed = reap_on_read(hash, now, passive);
                    (changed, hash.pairs(), hash.is_listpack())
                }
                None => return wrong_type(),
            },
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        };
        let (next, batch) = hscan_step(&pairs, cursor, count, pattern.as_deref(), is_listpack);
        reaped_read(changed, hscan_reply(next, batch, novalues))
    })
}

/// One bounded HSCAN batch over the hash's `(field, value)` pairs in ascending
/// [`field_scan_hash`] order, starting after `cursor`, applying the MATCH glob to the
/// field. Returns the next cursor (`ScanCursor(0)` = complete) and the kept pairs.
/// This is the hash analog of the keyspace `scan_step` + `scan_plan` (the same cursor
/// algorithm), implemented at the command layer because the command layer cannot name
/// the concrete store's `scan_plan` (the layering contract).
///
/// `is_listpack` selects the Redis small-collection behavior: a LISTPACK-encoded hash
/// returns the WHOLE hash in ONE batch with next-cursor 0, IGNORING COUNT (Redis returns
/// a small/listpack collection in a single SCAN reply); a HASHTABLE-encoded hash uses the
/// COUNT-budgeted hash-ordered cursor below.
fn hscan_step(
    pairs: &[FieldValue],
    cursor: ScanCursor,
    count: usize,
    pattern: Option<&[u8]>,
    is_listpack: bool,
) -> (ScanCursor, Vec<FieldValue>) {
    // Small (listpack) hash: return everything at once with cursor 0, COUNT ignored
    // (Redis small-collection HSCAN). The cursor is irrelevant for a one-shot reply, but a
    // non-START resume cursor on a listpack hash yields nothing (the whole hash was already
    // returned on the cursor-0 call), matching "the first reply completes".
    if is_listpack {
        if !cursor.is_start() {
            return (ScanCursor::START, Vec::new());
        }
        let kept: Vec<FieldValue> = pairs
            .iter()
            .filter(|(f, _)| pattern.is_none_or(|p| crate::glob::glob_match(p, f)))
            .cloned()
            .collect();
        return (ScanCursor::START, kept);
    }
    // Build the sorted (field_hash, index) order. Sorting by (hash, field bytes) gives a
    // total order even for equal-hash fields, identical run-to-run (ADR-0003).
    let mut order: Vec<(u64, usize)> = pairs
        .iter()
        .enumerate()
        .map(|(idx, (f, _))| (field_scan_hash(f), idx))
        .collect();
    order.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| pairs[a.1].0.cmp(&pairs[b.1].0)));

    let total = order.len();
    // Resume position: the first field whose hash is >= the cursor (group boundary; an
    // equal-hash group is never split, so `>=` returns the whole resumed group).
    let start = if cursor.is_start() {
        0
    } else {
        order.partition_point(|&(h, _)| h < cursor.0)
    };
    if start >= total {
        return (ScanCursor::START, Vec::new());
    }
    let count = count.max(1);
    let mut examined: Vec<usize> = Vec::new();
    let mut i = start;
    let mut n = 0usize;
    while i < total {
        let (h, _) = order[i];
        // Stop once the per-call budget is spent AND we are at a group boundary (the hash
        // differs from the last examined), so an equal-hash group is never split.
        if n >= count && i > start && h != order[i - 1].0 {
            break;
        }
        examined.push(order[i].1);
        n += 1;
        i += 1;
    }
    // The next cursor: 0 (complete) if the whole order was consumed; else the hash of the
    // first un-examined field (a strictly-greater group start, never 0 because a 0-hash
    // field sorts first and is examined on the start batch).
    let next = if i >= total {
        ScanCursor::START
    } else {
        ScanCursor(order[i].0)
    };
    // Realize the examined fields, applying the MATCH glob to the field.
    let kept: Vec<FieldValue> = examined
        .into_iter()
        .filter(|&idx| pattern.is_none_or(|p| crate::glob::glob_match(p, &pairs[idx].0)))
        .map(|idx| pairs[idx].clone())
        .collect();
    (next, kept)
}

/// Build the HSCAN reply `[cursor, [field, value, ...]]` (or `[cursor, [field, ...]]`
/// with NOVALUES).
fn hscan_reply(next: ScanCursor, batch: Vec<FieldValue>, novalues: bool) -> Value {
    let mut items: Vec<Value> = Vec::with_capacity(batch.len() * if novalues { 1 } else { 2 });
    for (f, v) in batch {
        items.push(bulk(f));
        if !novalues {
            items.push(bulk(v));
        }
    }
    Value::Array(Some(vec![
        Value::bulk(next.to_token().into_bytes()),
        Value::Array(Some(items)),
    ]))
}

/// The fixed-seed stable hash that orders a hash's FIELDS for HSCAN (the command-layer
/// analog of the store's `scan_hash`, KEYSPACE.md "the same hash-ordered cursor within
/// the collection"). It is a small wyhash/FNV-style mix over the field bytes, fully
/// determined by the bytes (no table state, no OS entropy, ADR-0003): recomputable
/// across calls and processes, so the HSCAN order is stable and resize-invariant.
///
/// Kept here (NOT imported from `ironcache-store`) because the command layer names only
/// the storage waist, never the concrete store (the layering contract): the store's
/// `scan_hash` is for KEY ordering and lives below the waist; this is the FIELD-ordering
/// analog above it. They use the same construction so behavior is identical.
fn field_scan_hash(field: &[u8]) -> u64 {
    const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
    const SECRET: u64 = 0xA076_1D64_78BD_642F;
    let mut h: u64 = SEED ^ SECRET;
    for &b in field {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
        h ^= h >> 33;
    }
    h = h.wrapping_add(0x9E37_79B9_7F4A_7C15);
    h = (h ^ (h >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^ (h >> 31)
}

// ---------------------------------------------------------------------------
// Hash field TTL (HEXPIRE family, Redis 7.4, #408).
//
// Per-field expiry: HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT set a TTL on individual hash
// fields, HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME read them, and HPERSIST removes them. The
// deadlines live in an optional side-map on the hash value (zero cost for a hash with no
// field TTLs). Expiry is LAZY: every access reaps fields whose deadline is at or before now
// (matching Redis hashTypeIsExpired); when reaping empties the hash the key is deleted, as
// Redis does. Proactive (timing-wheel) reaping is the tracked fast-follow.
// ---------------------------------------------------------------------------

/// The Redis 7 per-field conditional flags (NX/XX/GT/LT), applied against each field's
/// CURRENT deadline (None = no TTL = +infinity for the ordering gate). Mirrors the key-level
/// EXPIRE flag semantics in cmd_expire, evaluated per field here. The four bools mirror the
/// four INDEPENDENT Redis option bits exactly (the existence and ordering gates are evaluated
/// separately), so collapsing them into enums would re-couple them; the lint is allowed with
/// that rationale, as in cmd_expire's ExpireCond.
#[allow(clippy::struct_excessive_bools)]
#[derive(Default, Clone, Copy)]
struct HCond {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
}

/// Parse the trailing `FIELDS numfields field [field ...]` block (the slice STARTING at the
/// FIELDS keyword) shared by every HEXPIRE-family command, returning the field arguments.
/// The error strings are byte-faithful to Redis (src/t_hash.c).
fn parse_hash_fields_block(tail: &[Bytes]) -> Result<&[Bytes], ErrorReply> {
    if tail
        .first()
        .is_none_or(|t| !t.eq_ignore_ascii_case(b"FIELDS"))
    {
        return Err(ErrorReply::err(
            "Mandatory keyword FIELDS is missing or not at the right position",
        ));
    }
    let Some(n) = tail.get(1).and_then(|a| parse_i64(a)) else {
        return Err(ErrorReply::err(
            "Parameter `numFields` should be greater than 0",
        ));
    };
    if n <= 0 {
        return Err(ErrorReply::err(
            "Parameter `numFields` should be greater than 0",
        ));
    }
    let fields = &tail[2..];
    if (n as usize) != fields.len() {
        return Err(ErrorReply::err(
            "Parameter `numFields` is more than number of arguments",
        ));
    }
    Ok(fields)
}

/// The shared body of HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT. Returns a per-field array
/// of codes (-2 no field, 0 condition not met, 1 set, 2 field deleted because the deadline is
/// in the past), or an empty array if the key is missing (Redis returns an empty array, not an
/// error). One atomic rmw_mut over the hash.
fn hexpire_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    kind: ExpireKind,
    cmd_name: &str,
) -> Value {
    // HEXPIRE key ttl [NX|XX|GT|LT] FIELDS numfields field [field ...]: arity >= 6.
    if req.args.len() < 6 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let Some(ttl) = parse_i64(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    // An OPTIONAL single condition token precedes the FIELDS block.
    let mut idx = 3;
    let mut cond = HCond::default();
    match ascii_upper(&req.args[idx]).as_slice() {
        b"NX" => {
            cond.nx = true;
            idx += 1;
        }
        b"XX" => {
            cond.xx = true;
            idx += 1;
        }
        b"GT" => {
            cond.gt = true;
            idx += 1;
        }
        b"LT" => {
            cond.lt = true;
            idx += 1;
        }
        // No condition: the FIELDS-block parser validates the next token.
        _ => {}
    }
    let fields: Vec<Bytes> = match parse_hash_fields_block(&req.args[idx..]) {
        Ok(f) => f.to_vec(),
        Err(e) => return Value::error(e),
    };
    let Ok(deadline_ms) = resolve_expire_at(kind, ttl, now) else {
        return Value::error(ErrorReply::invalid_expire_time(cmd_name));
    };

    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        // Missing key: Redis replies an empty array (NOT an error).
        RmwEntry::Vacant => keep(Value::Array(Some(Vec::new()))),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(hash) = o.as_hash_mut() else {
                return wrong_type();
            };
            // Lazy reap first so an already-expired field reads as absent (code -2). HEXPIRE is
            // a write (primary-only), so it always reaps; `changed` tracks whether anything was
            // actually set/deleted/reaped, so a pure no-op (e.g. an XX miss) stays a Keep and
            // does not dirty a WATCH or emit a replication post-image.
            let mut changed = !hash.reap_expired_fields(now).is_empty();
            let mut out: Vec<Value> = Vec::with_capacity(fields.len());
            for f in &fields {
                if !hash.contains(f) {
                    out.push(Value::Integer(-2));
                    continue;
                }
                let cur = hash.field_ttl(f);
                // Existence (NX/XX) and ordering (GT/LT) gates are independent; both must
                // pass (Redis src/expire.c). A field with no current TTL is +infinity.
                let existence_ok = (!cond.nx || cur.is_none()) && (!cond.xx || cur.is_some());
                let ordering_ok = match cur {
                    None => !cond.gt,
                    Some(c) => {
                        (!cond.gt || deadline_ms > c.0 as i64)
                            && (!cond.lt || deadline_ms < c.0 as i64)
                    }
                };
                if !(existence_ok && ordering_ok) {
                    out.push(Value::Integer(0));
                    continue;
                }
                if deadline_ms <= now.0 as i64 {
                    // A deadline at or before now deletes the field (Redis code 2).
                    hash.del(f);
                    out.push(Value::Integer(2));
                    changed = true;
                } else {
                    hash.set_field_ttl(f, UnixMillis(deadline_ms as u64));
                    out.push(Value::Integer(1));
                    changed = true;
                }
            }
            let action = if hash.is_empty() {
                RmwAction::Delete
            } else if changed {
                RmwAction::Mutated
            } else {
                RmwAction::Keep
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: Value::Array(Some(out)),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]`.
pub fn cmd_hexpire<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    hexpire_generic(store, db, now, req, ExpireKind::Seconds, "hexpire")
}

/// `HPEXPIRE key milliseconds [NX|XX|GT|LT] FIELDS numfields field [field ...]`.
pub fn cmd_hpexpire<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    hexpire_generic(store, db, now, req, ExpireKind::Millis, "hpexpire")
}

/// `HEXPIREAT key unix-time-seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]`.
pub fn cmd_hexpireat<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    hexpire_generic(store, db, now, req, ExpireKind::SecondsAt, "hexpireat")
}

/// `HPEXPIREAT key unix-time-milliseconds [NX|XX|GT|LT] FIELDS numfields field [field ...]`.
pub fn cmd_hpexpireat<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    hexpire_generic(store, db, now, req, ExpireKind::MillisAt, "hpexpireat")
}

/// How an HTTL-family reader maps a field's absolute deadline to its reply value.
#[derive(Clone, Copy)]
enum HTtlKind {
    /// HTTL: remaining seconds, Redis rounding `(ms + 500) / 1000`.
    TtlSeconds,
    /// HPTTL: remaining milliseconds.
    TtlMillis,
    /// HEXPIRETIME: absolute deadline in seconds, `(ms + 500) / 1000`.
    ExpireSeconds,
    /// HPEXPIRETIME: absolute deadline in milliseconds.
    ExpireMillis,
}

/// The shared body of HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME. Returns a per-field array:
/// -2 (no such field), -1 (field has no TTL), or the TTL/timestamp value. Empty array if the
/// key is missing.
fn httl_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    kind: HTtlKind,
    cmd_name: &str,
) -> Value {
    // HTTL key FIELDS numfields field [field ...]: arity >= 5.
    if req.args.len() < 5 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let fields: Vec<Bytes> = match parse_hash_fields_block(&req.args[2..]) {
        Ok(f) => f.to_vec(),
        Err(e) => return Value::error(e),
    };
    let passive = store.is_passive();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Array(Some(Vec::new()))),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(hash) = o.as_hash_mut() else {
                return wrong_type();
            };
            let changed = reap_on_read(hash, now, passive);
            let mut out: Vec<Value> = Vec::with_capacity(fields.len());
            for f in &fields {
                if !hash.contains(f) {
                    out.push(Value::Integer(-2));
                    continue;
                }
                match hash.field_ttl(f) {
                    None => out.push(Value::Integer(-1)),
                    Some(at) => {
                        let v = match kind {
                            HTtlKind::TtlSeconds => {
                                (at.0.saturating_sub(now.0) as i64 + 500) / 1_000
                            }
                            HTtlKind::TtlMillis => at.0.saturating_sub(now.0) as i64,
                            HTtlKind::ExpireSeconds => (at.0 as i64 + 500) / 1_000,
                            HTtlKind::ExpireMillis => at.0 as i64,
                        };
                        out.push(Value::Integer(v));
                    }
                }
            }
            // A reader only mutates if the lazy reap removed a field; otherwise it is a pure
            // read (Keep), so it does not dirty the change counter.
            let action = if hash.is_empty() {
                RmwAction::Delete
            } else if changed {
                RmwAction::Mutated
            } else {
                RmwAction::Keep
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: Value::Array(Some(out)),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `HTTL key FIELDS numfields field [field ...]`.
pub fn cmd_httl<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    httl_generic(store, db, now, req, HTtlKind::TtlSeconds, "httl")
}

/// `HPTTL key FIELDS numfields field [field ...]`.
pub fn cmd_hpttl<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    httl_generic(store, db, now, req, HTtlKind::TtlMillis, "hpttl")
}

/// `HEXPIRETIME key FIELDS numfields field [field ...]`.
pub fn cmd_hexpiretime<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    httl_generic(store, db, now, req, HTtlKind::ExpireSeconds, "hexpiretime")
}

/// `HPEXPIRETIME key FIELDS numfields field [field ...]`.
pub fn cmd_hpexpiretime<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    httl_generic(store, db, now, req, HTtlKind::ExpireMillis, "hpexpiretime")
}

/// `HPERSIST key FIELDS numfields field [field ...]` -> per-field: -2 (no field), -1 (no TTL
/// to remove), 1 (TTL removed).
pub fn cmd_hpersist<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 5 {
        return Value::error(ErrorReply::wrong_arity("hpersist"));
    }
    let fields: Vec<Bytes> = match parse_hash_fields_block(&req.args[2..]) {
        Ok(f) => f.to_vec(),
        Err(e) => return Value::error(e),
    };
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Array(Some(Vec::new()))),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(hash) = o.as_hash_mut() else {
                return wrong_type();
            };
            let reaped = hash.reap_expired_fields(now);
            let mut changed = !reaped.is_empty();
            let mut out: Vec<Value> = Vec::with_capacity(fields.len());
            for f in &fields {
                if !hash.contains(f) {
                    out.push(Value::Integer(-2));
                } else if hash.persist_field(f) {
                    changed = true;
                    out.push(Value::Integer(1));
                } else {
                    out.push(Value::Integer(-1));
                }
            }
            let action = if hash.is_empty() {
                RmwAction::Delete
            } else if changed {
                RmwAction::Mutated
            } else {
                RmwAction::Keep
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: Value::Array(Some(out)),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::{CountingAccounting, DataType, Store};
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
    const SEED: u64 = 0xABCD_1234_5678_9EF0;

    fn err_line(v: &Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    fn int(v: &Value) -> i64 {
        match v {
            Value::Integer(n) => *n,
            other => panic!("expected an integer, got {other:?}"),
        }
    }

    fn bulk_bytes(v: &Value) -> Option<Vec<u8>> {
        match v {
            Value::BulkString(Some(b)) => Some(b.to_vec()),
            Value::Null => None,
            other => panic!("expected a bulk or nil, got {other:?}"),
        }
    }

    fn ints(v: &Value) -> Vec<i64> {
        match v {
            Value::Array(Some(items)) => items.iter().map(int).collect(),
            other => panic!("expected an array, got {other:?}"),
        }
    }

    // ---- HEXPIRE family (#408): per-field TTL set/read/persist/expire + errors. ----

    #[test]
    #[allow(clippy::too_many_lines)] // one end-to-end table covering all the per-field codes.
    fn hexpire_family_set_read_persist_and_expire() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3"]),
        );
        // HEXPIRE 100s on a,b (NOW=0 -> deadline 100000ms); missing field z -> -2.
        assert_eq!(
            ints(&cmd_hexpire(
                &mut s,
                0,
                NOW,
                &req(&[b"HEXPIRE", b"h", b"100", b"FIELDS", b"3", b"a", b"b", b"z"]),
            )),
            vec![1, 1, -2]
        );
        // HTTL: a,b -> 100s; c -> -1 (no TTL); z -> -2 (no field).
        assert_eq!(
            ints(&cmd_httl(
                &mut s,
                0,
                NOW,
                &req(&[b"HTTL", b"h", b"FIELDS", b"4", b"a", b"b", b"c", b"z"]),
            )),
            vec![100, 100, -1, -2]
        );
        // HPTTL a -> 100000 ms; HEXPIRETIME a -> 100 (absolute seconds).
        assert_eq!(
            ints(&cmd_hpttl(
                &mut s,
                0,
                NOW,
                &req(&[b"HPTTL", b"h", b"FIELDS", b"1", b"a"])
            )),
            vec![100_000]
        );
        assert_eq!(
            ints(&cmd_hexpiretime(
                &mut s,
                0,
                NOW,
                &req(&[b"HEXPIRETIME", b"h", b"FIELDS", b"1", b"a"]),
            )),
            vec![100]
        );
        // GT: a new 50s deadline is NOT greater than the current 100s -> 0 (not set).
        assert_eq!(
            ints(&cmd_hexpire(
                &mut s,
                0,
                NOW,
                &req(&[b"HEXPIRE", b"h", b"50", b"GT", b"FIELDS", b"1", b"a"]),
            )),
            vec![0]
        );
        // HPERSIST a -> 1 (removed), c -> -1 (no TTL), z -> -2 (no field).
        assert_eq!(
            ints(&cmd_hpersist(
                &mut s,
                0,
                NOW,
                &req(&[b"HPERSIST", b"h", b"FIELDS", b"3", b"a", b"c", b"z"]),
            )),
            vec![1, -1, -2]
        );
        // A past absolute deadline (HPEXPIREAT 0, with NOW=0) deletes the field (code 2).
        assert_eq!(
            ints(&cmd_hpexpireat(
                &mut s,
                0,
                NOW,
                &req(&[b"HPEXPIREAT", b"h", b"0", b"FIELDS", b"1", b"b"]),
            )),
            vec![2]
        );
        // b is gone now (the field was deleted).
        assert_eq!(
            ints(&cmd_httl(
                &mut s,
                0,
                NOW,
                &req(&[b"HTTL", b"h", b"FIELDS", b"1", b"b"])
            )),
            vec![-2]
        );
        // Missing key -> empty array (NOT an error).
        assert_eq!(
            ints(&cmd_hexpire(
                &mut s,
                0,
                NOW,
                &req(&[b"HEXPIRE", b"nope", b"100", b"FIELDS", b"1", b"a"]),
            )),
            Vec::<i64>::new()
        );
        // A bare 4-arg form fails arity first (HEXPIRE is -6, like Redis).
        assert_eq!(
            err_line(&cmd_hexpire(
                &mut s,
                0,
                NOW,
                &req(&[b"HEXPIRE", b"h", b"100", b"a"])
            )),
            "-ERR wrong number of arguments for 'hexpire' command"
        );
        // With enough args but FIELDS misplaced -> the Redis-faithful keyword error.
        assert_eq!(
            err_line(&cmd_hexpire(
                &mut s,
                0,
                NOW,
                &req(&[b"HEXPIRE", b"h", b"100", b"NOPE", b"x", b"y"]),
            )),
            "-ERR Mandatory keyword FIELDS is missing or not at the right position"
        );
        // numFields that does not match the provided field count -> the Redis-faithful error.
        assert_eq!(
            err_line(&cmd_hexpire(
                &mut s,
                0,
                NOW,
                &req(&[b"HEXPIRE", b"h", b"100", b"FIELDS", b"2", b"a"]),
            )),
            "-ERR Parameter `numFields` is more than number of arguments"
        );
    }

    #[test]
    fn hexpire_lazy_reaps_an_expired_field_on_access() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"a", b"1", b"b", b"2"]),
        );
        // 10s TTL on a (deadline 10000ms at NOW=0).
        cmd_hexpire(
            &mut s,
            0,
            NOW,
            &req(&[b"HEXPIRE", b"h", b"10", b"FIELDS", b"1", b"a"]),
        );
        let later = UnixMillis(20_000);
        // At a later time, a is past its deadline: a TTL-family access lazily reaps it -> -2.
        assert_eq!(
            ints(&cmd_httl(
                &mut s,
                0,
                later,
                &req(&[b"HTTL", b"h", b"FIELDS", b"1", b"a"])
            )),
            vec![-2]
        );
        // The reap removed a from the hash; only b remains.
        assert_eq!(int(&cmd_hlen(&mut s, 0, later, &req(&[b"HLEN", b"h"]))), 1);
    }

    #[test]
    fn expired_field_is_invisible_to_regular_hash_reads() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"a", b"1", b"b", b"2"]),
        );
        cmd_hexpire(
            &mut s,
            0,
            NOW,
            &req(&[b"HEXPIRE", b"h", b"10", b"FIELDS", b"1", b"a"]),
        );
        let later = UnixMillis(20_000);
        // After expiry the regular reads no longer see a (the read reaps it): HGET nil,
        // HEXISTS 0, HSTRLEN 0; b is untouched, and HLEN drops to 1.
        assert_eq!(
            bulk_bytes(&cmd_hget(&mut s, 0, later, &req(&[b"HGET", b"h", b"a"]))),
            None
        );
        assert_eq!(
            int(&cmd_hexists(
                &mut s,
                0,
                later,
                &req(&[b"HEXISTS", b"h", b"a"])
            )),
            0
        );
        assert_eq!(int(&cmd_hlen(&mut s, 0, later, &req(&[b"HLEN", b"h"]))), 1);
        assert_eq!(
            bulk_bytes(&cmd_hget(&mut s, 0, later, &req(&[b"HGET", b"h", b"b"]))),
            Some(b"2".to_vec())
        );
    }

    #[test]
    fn reaping_the_last_field_on_a_read_deletes_the_key() {
        let mut s = test_store();
        cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", b"a", b"1"]));
        cmd_hexpire(
            &mut s,
            0,
            NOW,
            &req(&[b"HEXPIRE", b"h", b"10", b"FIELDS", b"1", b"a"]),
        );
        let later = UnixMillis(20_000);
        // HGET reaps a, which empties the hash; the empty-collection-deletes-key backstop
        // removes the key, so a subsequent HLEN sees a missing key (0).
        assert_eq!(
            bulk_bytes(&cmd_hget(&mut s, 0, later, &req(&[b"HGET", b"h", b"a"]))),
            None
        );
        assert_eq!(int(&cmd_hlen(&mut s, 0, later, &req(&[b"HLEN", b"h"]))), 0);
    }

    #[test]
    fn passive_replica_does_not_reap_expired_fields_on_read() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"a", b"1", b"b", b"2"]),
        );
        cmd_hexpire(
            &mut s,
            0,
            NOW,
            &req(&[b"HEXPIRE", b"h", b"10", b"FIELDS", b"1", b"a"]),
        );
        // Mark the store a PASSIVE replica (HA-7d): it must never expire on its own, since the
        // authoritative removal arrives only via the replication stream. So a read at a later
        // time must NOT physically drop the expired field (no independent expiry, no divergence).
        s.set_passive(true);
        let later = UnixMillis(20_000);
        assert_eq!(int(&cmd_hlen(&mut s, 0, later, &req(&[b"HLEN", b"h"]))), 2);
        assert_eq!(
            int(&cmd_hexists(
                &mut s,
                0,
                later,
                &req(&[b"HEXISTS", b"h", b"a"])
            )),
            1
        );
        // Back as a primary, a read reaps the expired field (HLEN drops to 1).
        s.set_passive(false);
        assert_eq!(int(&cmd_hlen(&mut s, 0, later, &req(&[b"HLEN", b"h"]))), 1);
    }

    #[test]
    fn hset_overwrite_clears_a_field_ttl() {
        let mut s = test_store();
        cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", b"a", b"1"]));
        cmd_hexpire(
            &mut s,
            0,
            NOW,
            &req(&[b"HEXPIRE", b"h", b"100", b"FIELDS", b"1", b"a"]),
        );
        assert_eq!(
            ints(&cmd_httl(
                &mut s,
                0,
                NOW,
                &req(&[b"HTTL", b"h", b"FIELDS", b"1", b"a"])
            )),
            vec![100]
        );
        // A plain HSET overwrite removes the field's TTL (Redis hashTypeSet) -> -1 (no TTL).
        cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", b"a", b"2"]));
        assert_eq!(
            ints(&cmd_httl(
                &mut s,
                0,
                NOW,
                &req(&[b"HTTL", b"h", b"FIELDS", b"1", b"a"])
            )),
            vec![-1]
        );
    }

    // ---- HSET: new-vs-update count, HMSET alias. ----

    #[test]
    fn hset_counts_only_new_fields() {
        let mut s = test_store();
        // Two new fields -> 2.
        assert_eq!(
            int(&cmd_hset(
                &mut s,
                0,
                NOW,
                &req(&[b"HSET", b"h", b"a", b"1", b"b", b"2"])
            )),
            2
        );
        // One existing (updated in place), one new -> 1.
        assert_eq!(
            int(&cmd_hset(
                &mut s,
                0,
                NOW,
                &req(&[b"HSET", b"h", b"a", b"9", b"c", b"3"])
            )),
            1
        );
        // The updated value is the new one.
        assert_eq!(
            bulk_bytes(&cmd_hget(&mut s, 0, NOW, &req(&[b"HGET", b"h", b"a"]))),
            Some(b"9".to_vec())
        );
        // TYPE is hash.
        assert_eq!(s.type_of(0, b"h", NOW), Some(DataType::Hash));
    }

    #[test]
    fn hset_odd_args_is_wrong_arity() {
        let mut s = test_store();
        // An odd field/value tail (a value missing) is a wrong-arity error.
        assert_eq!(
            err_line(&cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", b"a"]))),
            "-ERR wrong number of arguments for 'hset' command"
        );
    }

    #[test]
    fn hmset_replies_ok() {
        let mut s = test_store();
        assert_eq!(
            cmd_hmset(
                &mut s,
                0,
                NOW,
                &req(&[b"HMSET", b"h", b"a", b"1", b"b", b"2"])
            ),
            Value::ok()
        );
        assert_eq!(int(&cmd_hlen(&mut s, 0, NOW, &req(&[b"HLEN", b"h"]))), 2);
    }

    // ---- HSETNX. ----

    #[test]
    fn hsetnx_sets_only_when_absent() {
        let mut s = test_store();
        assert_eq!(
            int(&cmd_hsetnx(
                &mut s,
                0,
                NOW,
                &req(&[b"HSETNX", b"h", b"f", b"v1"])
            )),
            1
        );
        // Second HSETNX on the existing field -> 0, value unchanged.
        assert_eq!(
            int(&cmd_hsetnx(
                &mut s,
                0,
                NOW,
                &req(&[b"HSETNX", b"h", b"f", b"v2"])
            )),
            0
        );
        assert_eq!(
            bulk_bytes(&cmd_hget(&mut s, 0, NOW, &req(&[b"HGET", b"h", b"f"]))),
            Some(b"v1".to_vec())
        );
    }

    // ---- HGET / HMGET nil handling. ----

    #[test]
    fn hget_and_hmget_nil_handling() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"a", b"1", b"b", b"2"]),
        );
        // HGET of a missing field -> nil.
        assert_eq!(
            cmd_hget(&mut s, 0, NOW, &req(&[b"HGET", b"h", b"missing"])),
            Value::Null
        );
        // HGET of a missing KEY -> nil.
        assert_eq!(
            cmd_hget(&mut s, 0, NOW, &req(&[b"HGET", b"nope", b"a"])),
            Value::Null
        );
        // HMGET: a value, a nil per missing field.
        let reply = cmd_hmget(
            &mut s,
            0,
            NOW,
            &req(&[b"HMGET", b"h", b"a", b"missing", b"b"]),
        );
        match reply {
            Value::Array(Some(items)) => {
                assert_eq!(bulk_bytes(&items[0]), Some(b"1".to_vec()));
                assert_eq!(items[1], Value::Null);
                assert_eq!(bulk_bytes(&items[2]), Some(b"2".to_vec()));
            }
            other => panic!("HMGET not an array: {other:?}"),
        }
        // HMGET of a missing KEY -> a nil per requested field.
        let reply = cmd_hmget(&mut s, 0, NOW, &req(&[b"HMGET", b"nope", b"a", b"b"]));
        assert_eq!(reply, Value::Array(Some(vec![Value::Null, Value::Null])));
    }

    // ---- HDEL variadic + empty-deletes-key. ----

    #[test]
    fn hdel_variadic_and_empty_deletes_key() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3"]),
        );
        // Delete two present + one absent -> 2 removed.
        assert_eq!(
            int(&cmd_hdel(
                &mut s,
                0,
                NOW,
                &req(&[b"HDEL", b"h", b"a", b"b", b"zzz"])
            )),
            2
        );
        assert_eq!(int(&cmd_hlen(&mut s, 0, NOW, &req(&[b"HLEN", b"h"]))), 1);
        // Delete the last field -> the key is GONE.
        assert_eq!(
            int(&cmd_hdel(&mut s, 0, NOW, &req(&[b"HDEL", b"h", b"c"]))),
            1
        );
        assert!(
            !s.contains(0, b"h", NOW),
            "emptying the hash deletes the key"
        );
        assert_eq!(s.used_memory(), 0, "accounting returns to zero");
        // HDEL on a missing key -> 0.
        assert_eq!(
            int(&cmd_hdel(&mut s, 0, NOW, &req(&[b"HDEL", b"h", b"a"]))),
            0
        );
    }

    // ---- HGETALL: RESP2 flat (degraded by the encoder) + RESP3 map (built here). ----

    #[test]
    fn hgetall_returns_a_map_value() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"a", b"1", b"b", b"2"]),
        );
        // The handler builds a Value::Map (the encoder degrades it to a flat array under
        // RESP2 and emits a `%` map under RESP3, ADR-0019). Assert the pairs.
        match cmd_hgetall(&mut s, 0, NOW, &req(&[b"HGETALL", b"h"])) {
            Value::Map(pairs) => {
                assert_eq!(pairs.len(), 2);
                let mut got: Vec<(Vec<u8>, Vec<u8>)> = pairs
                    .iter()
                    .map(|(f, v)| (bulk_bytes(f).unwrap(), bulk_bytes(v).unwrap()))
                    .collect();
                got.sort();
                assert_eq!(
                    got,
                    vec![
                        (b"a".to_vec(), b"1".to_vec()),
                        (b"b".to_vec(), b"2".to_vec())
                    ]
                );
            }
            other => panic!("HGETALL not a map: {other:?}"),
        }
        // Missing key -> empty map.
        assert_eq!(
            cmd_hgetall(&mut s, 0, NOW, &req(&[b"HGETALL", b"nope"])),
            Value::Map(Vec::new())
        );
    }

    // ---- HKEYS / HVALS / HLEN / HEXISTS / HSTRLEN. ----

    #[test]
    fn hkeys_hvals_hlen_hexists_hstrlen() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"a", b"xx", b"b", b"yyy"]),
        );
        // HKEYS / HVALS (order is the deterministic pairs() order; sort to compare).
        let mut keys = match cmd_hkeys(&mut s, 0, NOW, &req(&[b"HKEYS", b"h"])) {
            Value::Array(Some(items)) => items
                .iter()
                .map(|v| bulk_bytes(v).unwrap())
                .collect::<Vec<_>>(),
            other => panic!("HKEYS not an array: {other:?}"),
        };
        keys.sort();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
        let mut vals = match cmd_hvals(&mut s, 0, NOW, &req(&[b"HVALS", b"h"])) {
            Value::Array(Some(items)) => items
                .iter()
                .map(|v| bulk_bytes(v).unwrap())
                .collect::<Vec<_>>(),
            other => panic!("HVALS not an array: {other:?}"),
        };
        vals.sort();
        assert_eq!(vals, vec![b"xx".to_vec(), b"yyy".to_vec()]);
        // HLEN.
        assert_eq!(int(&cmd_hlen(&mut s, 0, NOW, &req(&[b"HLEN", b"h"]))), 2);
        // HEXISTS present / absent.
        assert_eq!(
            int(&cmd_hexists(
                &mut s,
                0,
                NOW,
                &req(&[b"HEXISTS", b"h", b"a"])
            )),
            1
        );
        assert_eq!(
            int(&cmd_hexists(
                &mut s,
                0,
                NOW,
                &req(&[b"HEXISTS", b"h", b"zzz"])
            )),
            0
        );
        // HSTRLEN: present -> value byte len, absent field/key -> 0.
        assert_eq!(
            int(&cmd_hstrlen(
                &mut s,
                0,
                NOW,
                &req(&[b"HSTRLEN", b"h", b"b"])
            )),
            3
        );
        assert_eq!(
            int(&cmd_hstrlen(
                &mut s,
                0,
                NOW,
                &req(&[b"HSTRLEN", b"h", b"zzz"])
            )),
            0
        );
        assert_eq!(
            int(&cmd_hstrlen(
                &mut s,
                0,
                NOW,
                &req(&[b"HSTRLEN", b"nope", b"a"])
            )),
            0
        );
        // HLEN / HKEYS on a missing key -> 0 / empty.
        assert_eq!(int(&cmd_hlen(&mut s, 0, NOW, &req(&[b"HLEN", b"nope"]))), 0);
        assert_eq!(
            cmd_hkeys(&mut s, 0, NOW, &req(&[b"HKEYS", b"nope"])),
            Value::Array(Some(Vec::new()))
        );
    }

    // ---- HINCRBY edges. ----

    #[test]
    fn hincrby_creates_increments_and_errors() {
        let mut s = test_store();
        // Create-on-missing: HINCRBY on an absent key/field starts from 0.
        assert_eq!(
            int(&cmd_hincrby(
                &mut s,
                0,
                NOW,
                &req(&[b"HINCRBY", b"h", b"n", b"5"])
            )),
            5
        );
        assert_eq!(
            int(&cmd_hincrby(
                &mut s,
                0,
                NOW,
                &req(&[b"HINCRBY", b"h", b"n", b"-2"])
            )),
            3
        );
        // A non-integer STORED value is the hash-value-not-an-integer error.
        cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", b"s", b"abc"]));
        assert_eq!(
            err_line(&cmd_hincrby(
                &mut s,
                0,
                NOW,
                &req(&[b"HINCRBY", b"h", b"s", b"1"])
            )),
            "-ERR hash value is not an integer"
        );
        // A non-integer INCREMENT argument is the generic not-an-integer error.
        assert_eq!(
            err_line(&cmd_hincrby(
                &mut s,
                0,
                NOW,
                &req(&[b"HINCRBY", b"h", b"n", b"xx"])
            )),
            "-ERR value is not an integer or out of range"
        );
        // Overflow.
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"big", b"9223372036854775807"]),
        );
        assert_eq!(
            err_line(&cmd_hincrby(
                &mut s,
                0,
                NOW,
                &req(&[b"HINCRBY", b"h", b"big", b"1"])
            )),
            "-ERR increment or decrement would overflow"
        );
    }

    // ---- HINCRBYFLOAT. ----

    #[test]
    fn hincrbyfloat_creates_increments_and_errors() {
        let mut s = test_store();
        // Create-on-missing.
        assert_eq!(
            bulk_bytes(&cmd_hincrbyfloat(
                &mut s,
                0,
                NOW,
                &req(&[b"HINCRBYFLOAT", b"h", b"f", b"10.5"])
            )),
            Some(b"10.5".to_vec())
        );
        assert_eq!(
            bulk_bytes(&cmd_hincrbyfloat(
                &mut s,
                0,
                NOW,
                &req(&[b"HINCRBYFLOAT", b"h", b"f", b"0.1"])
            )),
            Some(b"10.6".to_vec())
        );
        // A non-float STORED value is the hash-value-not-a-float error.
        cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", b"s", b"abc"]));
        assert_eq!(
            err_line(&cmd_hincrbyfloat(
                &mut s,
                0,
                NOW,
                &req(&[b"HINCRBYFLOAT", b"h", b"s", b"1.0"])
            )),
            "-ERR hash value is not a float"
        );
        // A non-float INCREMENT argument is the generic not-a-valid-float error.
        assert_eq!(
            err_line(&cmd_hincrbyfloat(
                &mut s,
                0,
                NOW,
                &req(&[b"HINCRBYFLOAT", b"h", b"f", b"xx"])
            )),
            "-ERR value is not a valid float"
        );
    }

    // ---- WRONGTYPE on a string key. ----

    #[test]
    fn wrongtype_on_a_string_key() {
        let mut s = test_store();
        s.upsert(
            0,
            b"str",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        for reply in [
            cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"str", b"a", b"1"])),
            cmd_hget(&mut s, 0, NOW, &req(&[b"HGET", b"str", b"a"])),
            cmd_hdel(&mut s, 0, NOW, &req(&[b"HDEL", b"str", b"a"])),
            cmd_hgetall(&mut s, 0, NOW, &req(&[b"HGETALL", b"str"])),
            cmd_hincrby(&mut s, 0, NOW, &req(&[b"HINCRBY", b"str", b"a", b"1"])),
            cmd_hscan(&mut s, 0, NOW, &req(&[b"HSCAN", b"str", b"0"])),
        ] {
            assert_eq!(
                err_line(&reply),
                "-WRONGTYPE Operation against a key holding the wrong kind of value"
            );
        }
    }

    // ---- HRANDFIELD: no count, +count distinct, -count repeats, WITHVALUES, nil. ----

    fn arr_strs(v: &Value) -> Vec<Vec<u8>> {
        match v {
            Value::Array(Some(items)) => items.iter().map(|x| bulk_bytes(x).unwrap()).collect(),
            other => panic!("expected an array, got {other:?}"),
        }
    }

    #[test]
    fn hrandfield_variants() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3"]),
        );
        // No count -> one field (a member of the hash).
        let one = cmd_hrandfield(&mut s, 0, SEED, NOW, &req(&[b"HRANDFIELD", b"h"]));
        let one = bulk_bytes(&one).unwrap();
        assert!([b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].contains(&one));

        // Positive count: DISTINCT, capped at the hash length (count 5 on a 3-field hash
        // returns 3 distinct).
        let pos = arr_strs(&cmd_hrandfield(
            &mut s,
            0,
            SEED,
            NOW,
            &req(&[b"HRANDFIELD", b"h", b"5"]),
        ));
        assert_eq!(pos.len(), 3, "positive count is DISTINCT, capped at length");
        let mut sorted = pos.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 3, "distinct fields");

        // Negative count: WITH REPEATS, exactly |count| (may repeat).
        let neg = arr_strs(&cmd_hrandfield(
            &mut s,
            0,
            SEED,
            NOW,
            &req(&[b"HRANDFIELD", b"h", b"-10"]),
        ));
        assert_eq!(
            neg.len(),
            10,
            "negative count returns exactly |count| with repeats"
        );

        // WITHVALUES: a Value::Pairs of (field, value). It carries `count` pairs (2 here),
        // which FLATTEN to 4 elements under RESP2 and NEST as 2 sub-arrays under RESP3.
        // (The dedicated shape test below pins the exact RESP2/RESP3 bytes.)
        let wv = cmd_hrandfield(
            &mut s,
            0,
            SEED,
            NOW,
            &req(&[b"HRANDFIELD", b"h", b"2", b"WITHVALUES"]),
        );
        match &wv {
            Value::Pairs(p) => assert_eq!(p.len(), 2, "WITHVALUES returns `count` pairs"),
            other => panic!("WITHVALUES should be Value::Pairs, got {other:?}"),
        }

        // Missing key: nil (no count) / empty array (with count).
        assert_eq!(
            cmd_hrandfield(&mut s, 0, SEED, NOW, &req(&[b"HRANDFIELD", b"nope"])),
            Value::Null
        );
        assert_eq!(
            cmd_hrandfield(&mut s, 0, SEED, NOW, &req(&[b"HRANDFIELD", b"nope", b"3"])),
            Value::Array(Some(Vec::new()))
        );
    }

    #[test]
    fn hrandfield_withvalues_resp2_flat_and_resp3_nested_bytes() {
        // The WITHVALUES reply is a Value::Pairs: it must FLATTEN to a single
        // [field, value, ...] array under RESP2 and NEST as an array of 2-element
        // [field, value] arrays under RESP3 (Redis WITHVALUES RESP2/RESP3 shapes). A
        // negative count gives a deterministic, repeatable selection so the exact bytes are
        // pinnable.
        use ironcache_protocol::{ProtoVersion, encode_to_vec};
        let mut s = test_store();
        // A single-field hash makes the negative-count selection fully determined: every
        // draw picks field "a" with value "1", so the bytes are exact regardless of seed.
        cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", b"a", b"1"]));
        let reply = cmd_hrandfield(
            &mut s,
            0,
            SEED,
            NOW,
            &req(&[b"HRANDFIELD", b"h", b"-2", b"WITHVALUES"]),
        );
        assert!(
            matches!(reply, Value::Pairs(ref p) if p.len() == 2),
            "WITHVALUES is a Value::Pairs of `|count|` pairs, got {reply:?}"
        );
        // RESP2: FLAT array of 2n=4 elements (a 1 a 1).
        assert_eq!(
            encode_to_vec(&reply, ProtoVersion::Resp2),
            b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n1\r\n",
            "RESP2 WITHVALUES flattens to a single array"
        );
        // RESP3: NESTED outer *2, each pair a *2 [field, value] sub-array.
        assert_eq!(
            encode_to_vec(&reply, ProtoVersion::Resp3),
            b"*2\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n",
            "RESP3 WITHVALUES nests each (field, value) pair"
        );

        // WITHOUT values stays a flat array of fields in BOTH protos (no nesting).
        let plain = cmd_hrandfield(&mut s, 0, SEED, NOW, &req(&[b"HRANDFIELD", b"h", b"-2"]));
        assert!(
            matches!(plain, Value::Array(_)),
            "no WITHVALUES -> flat array"
        );
        assert_eq!(
            encode_to_vec(&plain, ProtoVersion::Resp3),
            b"*2\r\n$1\r\na\r\n$1\r\na\r\n",
            "no WITHVALUES: a flat array of fields under RESP3 too"
        );
    }

    #[test]
    fn hrandfield_is_deterministic_under_a_fixed_seed() {
        // The SAME seed yields the SAME selection (ADR-0003: the caller-drawn seed is the
        // only entropy; the handler derives indices deterministically).
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[
                b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3", b"d", b"4",
            ]),
        );
        let a = cmd_hrandfield(&mut s, 0, SEED, NOW, &req(&[b"HRANDFIELD", b"h", b"-8"]));
        let b = cmd_hrandfield(&mut s, 0, SEED, NOW, &req(&[b"HRANDFIELD", b"h", b"-8"]));
        assert_eq!(a, b, "same seed -> same HRANDFIELD selection");
    }

    // ---- HSCAN: small all-at-once, MATCH, NOVALUES, large cursored. ----

    /// Drive HSCAN to completion, returning every (field, value) seen.
    fn hscan_all(s: &mut TestStore, key: &[u8], extra: &[&[u8]]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        let mut cursor = b"0".to_vec();
        loop {
            let mut parts: Vec<&[u8]> = vec![b"HSCAN", key, cursor.as_slice()];
            parts.extend_from_slice(extra);
            let reply = cmd_hscan(s, 0, NOW, &req(&parts));
            let Value::Array(Some(items)) = reply else {
                panic!("HSCAN reply not an array: {reply:?}");
            };
            let (Value::BulkString(Some(next)), Value::Array(Some(kv))) = (&items[0], &items[1])
            else {
                panic!("bad HSCAN shape: {items:?}");
            };
            // The kv array is [field, value, field, value, ...].
            let mut i = 0;
            while i + 1 < kv.len() {
                out.push((bulk_bytes(&kv[i]).unwrap(), bulk_bytes(&kv[i + 1]).unwrap()));
                i += 2;
            }
            if next.as_ref() == b"0" {
                break;
            }
            cursor = next.to_vec();
        }
        out
    }

    #[test]
    fn hscan_small_all_at_once() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"a", b"1", b"b", b"2"]),
        );
        // A small hash returns everything; the first call should complete (cursor 0).
        let reply = cmd_hscan(&mut s, 0, NOW, &req(&[b"HSCAN", b"h", b"0"]));
        let Value::Array(Some(items)) = reply else {
            panic!("not an array");
        };
        assert_eq!(
            items[0],
            Value::bulk(b"0".to_vec()),
            "small hash completes at once"
        );
        let mut got = hscan_all(&mut s, b"h", &[]);
        got.sort();
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
        // Missing key -> [0, []].
        let reply = cmd_hscan(&mut s, 0, NOW, &req(&[b"HSCAN", b"nope", b"0"]));
        assert_eq!(
            reply,
            Value::Array(Some(vec![
                Value::bulk(b"0".to_vec()),
                Value::Array(Some(Vec::new()))
            ]))
        );
    }

    #[test]
    fn hscan_listpack_returns_all_at_once_ignoring_count() {
        // A listpack-encoded (small) hash returns the WHOLE hash in ONE reply with cursor 0,
        // IGNORING COUNT, even when the field count exceeds COUNT (Redis small-collection
        // HSCAN). 20 small fields stay listpack (under the 512 entry / 64 byte caps); a
        // COUNT of 3 (< 20) must still complete in a SINGLE call at cursor 0.
        let mut s = test_store();
        for i in 0..20 {
            let f = format!("f{i:02}");
            cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", f.as_bytes(), b"v"]));
        }
        // Confirm the hash is still listpack-encoded (precondition for all-at-once).
        assert_eq!(
            s.read(0, b"h", NOW).unwrap().encoding().encoding_name(),
            "listpack",
            "20 small fields stay listpack (under the 512/64 caps)"
        );
        // ONE HSCAN call with COUNT 3 must return ALL 20 fields and complete (cursor 0).
        let reply = cmd_hscan(
            &mut s,
            0,
            NOW,
            &req(&[b"HSCAN", b"h", b"0", b"COUNT", b"3"]),
        );
        let Value::Array(Some(items)) = reply else {
            panic!("not an array");
        };
        assert_eq!(
            items[0],
            Value::bulk(b"0".to_vec()),
            "listpack hash completes in one call (cursor 0), COUNT ignored"
        );
        let Value::Array(Some(kv)) = &items[1] else {
            panic!("bad shape");
        };
        // 20 fields x (field, value) = 40 entries in the one batch.
        assert_eq!(
            kv.len(),
            40,
            "all 20 (field, value) pairs returned at once despite COUNT 3"
        );
    }

    #[test]
    fn hscan_large_cursored_visits_every_field_once() {
        let mut s = test_store();
        // 300 fields -> hashtable (cursored). Drive to completion with a small COUNT.
        for i in 0..300 {
            let f = format!("f{i:03}");
            cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", f.as_bytes(), b"v"]));
        }
        let got = hscan_all(&mut s, b"h", &[b"COUNT", b"7"]);
        assert_eq!(
            got.len(),
            300,
            "HSCAN to completion visits every field once"
        );
        let mut fields: Vec<Vec<u8>> = got.into_iter().map(|(f, _)| f).collect();
        fields.sort();
        fields.dedup();
        assert_eq!(fields.len(), 300, "no field visited twice / skipped");
    }

    #[test]
    fn hscan_match_and_novalues() {
        let mut s = test_store();
        cmd_hset(
            &mut s,
            0,
            NOW,
            &req(&[b"HSET", b"h", b"foo", b"1", b"bar", b"2", b"foobar", b"3"]),
        );
        // MATCH foo* -> foo, foobar.
        let mut matched: Vec<Vec<u8>> = hscan_all(&mut s, b"h", &[b"MATCH", b"foo*"])
            .into_iter()
            .map(|(f, _)| f)
            .collect();
        matched.sort();
        assert_eq!(matched, vec![b"foo".to_vec(), b"foobar".to_vec()]);

        // NOVALUES: the kv array is [field, field, ...] (no values). Drive one call.
        let reply = cmd_hscan(&mut s, 0, NOW, &req(&[b"HSCAN", b"h", b"0", b"NOVALUES"]));
        let Value::Array(Some(items)) = reply else {
            panic!("not an array");
        };
        let Value::Array(Some(fields)) = &items[1] else {
            panic!("bad shape");
        };
        // 3 fields, no values interleaved.
        assert_eq!(fields.len(), 3, "NOVALUES omits the values");
        let mut got: Vec<Vec<u8>> = fields.iter().map(|f| bulk_bytes(f).unwrap()).collect();
        got.sort();
        assert_eq!(
            got,
            vec![b"bar".to_vec(), b"foo".to_vec(), b"foobar".to_vec()]
        );
    }

    #[test]
    fn hscan_is_deterministic_across_a_resize() {
        // Determinism (ADR-0003): the HSCAN order is stable and resize-invariant. Build a
        // hashtable-form hash and drive HSCAN to completion twice; the field order must be
        // identical (the hashtable form is sorted by the fixed-seed field hash).
        fn collect(seed_fields: usize) -> Vec<Vec<u8>> {
            let mut s = test_store();
            for i in 0..seed_fields {
                let f = format!("field-{i}");
                cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", f.as_bytes(), b"v"]));
            }
            hscan_all(&mut s, b"h", &[b"COUNT", b"5"])
                .into_iter()
                .map(|(f, _)| f)
                .collect()
        }
        // Same workload -> identical HSCAN field order (both runs cross into hashtable).
        assert_eq!(collect(200), collect(200));
    }

    #[test]
    fn hscan_invalid_cursor() {
        let mut s = test_store();
        cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", b"a", b"1"]));
        assert_eq!(
            err_line(&cmd_hscan(
                &mut s,
                0,
                NOW,
                &req(&[b"HSCAN", b"h", b"notanumber"])
            )),
            "-ERR invalid cursor"
        );
    }

    // ---- OBJECT ENCODING listpack -> hashtable at both thresholds (via the store). ----

    #[test]
    fn encoding_transitions_at_both_thresholds() {
        // The encoding is read off the store's value; mirror what OBJECT ENCODING reports
        // (store.read(...).encoding()). Small -> listpack; over either threshold ->
        // hashtable.
        let mut s = test_store();
        cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", b"a", b"1"]));
        assert_eq!(
            s.read(0, b"h", NOW).unwrap().encoding().encoding_name(),
            "listpack"
        );
        // A value over the 64-byte cap -> hashtable.
        let big = vec![b'q'; 65];
        cmd_hset(&mut s, 0, NOW, &req(&[b"HSET", b"h", b"big", &big]));
        assert_eq!(
            s.read(0, b"h", NOW).unwrap().encoding().encoding_name(),
            "hashtable"
        );
    }
}
