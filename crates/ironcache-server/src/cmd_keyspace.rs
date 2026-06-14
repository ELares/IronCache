// SPDX-License-Identifier: MIT OR Apache-2.0
//! Generic keyspace command handlers over the storage waist (KEYSPACE.md). PR-2a:
//! DEL (variadic), EXISTS (variadic), TYPE.
//!
//! DEL/EXISTS compose the `delete`/`contains` primitives per key; TYPE uses
//! `type_of` (never WRONGTYPE). Lazy expiry-on-read applies inside each primitive,
//! so an expired key counts as not-existing (Redis semantics).

use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{Store, UnixMillis};

/// `DEL key [key ...]` -> the number of keys actually removed (live keys only).
pub fn cmd_del<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("del"));
    }
    let mut removed: i64 = 0;
    for key in &req.args[1..] {
        if store.delete(db, key, now) {
            removed += 1;
        }
    }
    Value::Integer(removed)
}

/// `EXISTS key [key ...]` -> the count of existing keys, counting repeats (Redis:
/// `EXISTS k k` on a present `k` returns 2).
pub fn cmd_exists<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("exists"));
    }
    let mut count: i64 = 0;
    for key in &req.args[1..] {
        if store.contains(db, key, now) {
            count += 1;
        }
    }
    Value::Integer(count)
}

/// `TYPE key` -> a simple string of the type name (`string`/...), or `none` if the
/// key is absent/expired. Never returns WRONGTYPE.
pub fn cmd_type<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("type"));
    }
    match store.type_of(db, &req.args[1], now) {
        Some(t) => Value::simple(t.type_name()),
        None => Value::simple("none"),
    }
}
