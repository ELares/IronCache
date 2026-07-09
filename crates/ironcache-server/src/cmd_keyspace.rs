// SPDX-License-Identifier: MIT OR Apache-2.0
//! Generic keyspace command handlers over the storage waist (KEYSPACE.md).
//!
//! - PR-2a: DEL (variadic), EXISTS (variadic), TYPE.
//! - PR-4a: KEYS, SCAN, DBSIZE, RANDOMKEY, RENAME, RENAMENX, COPY, MOVE, SWAPDB,
//!   TOUCH, UNLINK, FLUSHDB, FLUSHALL.
//!
//! DEL/EXISTS/TOUCH compose the `delete`/`contains` primitives per key; TYPE uses
//! `type_of` (never WRONGTYPE). The iteration / bulk commands compose the additive
//! [`Keyspace`] seam (SCAN/KEYS/DBSIZE/RANDOMKEY/RENAME/COPY/MOVE/SWAPDB/FLUSH). Lazy
//! expiry-on-read applies inside each primitive, so an expired key counts as
//! not-existing (Redis semantics).
//!
//! ## Cross-shard scope (single-shard-per-connection)
//!
//! KEYS / SCAN / DBSIZE / RANDOMKEY / FLUSHDB operate on the connection's accept
//! shard's DB, which IS that connection's whole keyspace today: no cross-shard key
//! routing exists yet (ADR-0011 single-node-first), so a single-shard scan covers the
//! whole keyspace. A true cross-shard SCAN/KEYS (fan out to every node and merge, with
//! the cursor's reserved slot bits driving a MOVED-style redirection for a migrating
//! slot) is DEFERRED to the coordinator/clustering work (#29/#75). RENAME/COPY/MOVE
//! are same-shard only for the same reason (a cross-shard form routes through the
//! coordinator later, KEYSPACE.md).

use crate::cmd_util::parse_i64;
use crate::dispatch::ServerContext;
use crate::glob::glob_match;
use ironcache_protocol::{ErrorReply, Request, Value, key_slot};
use ironcache_storage::{DataType, Keyspace, MoveMode, MoveOutcome, ScanCursor, Store, UnixMillis};

/// The default COUNT hint for SCAN when none is given (Redis SCAN default is 10).
const SCAN_DEFAULT_COUNT: usize = 10;

/// `DEL key [key ...]` -> the number of keys actually removed (live keys only).
pub fn cmd_del<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("del"));
    }
    let mut removed: i64 = 0;
    for key in &req.args[1..] {
        if store.delete(db, key, now) {
            removed += 1;
            // KEYSPACE NOTIFICATION (PROD-8): fire `del` (class g) PER actually-deleted key. DEL is
            // recorded HERE rather than in the central reply-driven table because the reply is the
            // removed COUNT, not WHICH keys existed; only this loop knows which deletes really
            // happened. `record` short-circuits on the disabled default, so this is zero-cost off.
            ironcache_config::notify::record(ironcache_config::EventClass::Generic, "del", key, db);
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

/// `TOUCH key [key ...]` -> the count of keys that EXIST and are live (alters no LRU
/// here; like EXISTS it counts repeats). Composes `contains` per key.
pub fn cmd_touch<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("touch"));
    }
    let mut count: i64 = 0;
    for key in &req.args[1..] {
        if store.contains(db, key, now) {
            count += 1;
        }
    }
    Value::Integer(count)
}

/// `UNLINK key [key ...]` -> the number of keys removed. SEMANTICALLY IDENTICAL to
/// DEL today: there is NO async background free yet (#51 reclamation queue is
/// deferred), so UNLINK removes synchronously and counts the same live keys. When the
/// background free lands, UNLINK enqueues large-value frees; the wire result (the
/// removed count) is unchanged, so this is forward-compatible.
pub fn cmd_unlink<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("unlink"));
    }
    let mut removed: i64 = 0;
    for key in &req.args[1..] {
        if store.delete(db, key, now) {
            removed += 1;
            // KEYSPACE NOTIFICATION (PROD-8): UNLINK fires the same `del` event as DEL (Redis emits
            // `del` for both), per actually-deleted key. Zero-cost when notifications are disabled.
            ironcache_config::notify::record(ironcache_config::EventClass::Generic, "del", key, db);
        }
    }
    Value::Integer(removed)
}

/// `KEYS pattern` -> an array of every live key whose bytes match the glob `pattern`.
///
/// O(N) and discouraged exactly as in Redis (it scans the whole keyspace): it loops
/// [`Keyspace::scan_step`] to completion internally with the glob filter. The result
/// order follows the SCAN hash order (Redis does not promise KEYS order either).
pub fn cmd_keys<S: Store + Keyspace>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("keys"));
    }
    let pattern = req.args[1].clone();
    // Loop scan_step to completion (cursor 0 -> ... -> 0) with the glob filter. KEYS is
    // O(N) and discouraged like Redis; a large COUNT per step keeps the loop count low.
    let mut out: Vec<Value> = Vec::new();
    let mut cursor = ScanCursor::START;
    loop {
        let (next, batch) =
            store.scan_step(db, cursor, 256, now, |key, _ty| glob_match(&pattern, key));
        for k in batch {
            out.push(Value::bulk(k.into_vec()));
        }
        if next.is_start() {
            break;
        }
        cursor = next;
    }
    Value::Array(Some(out))
}

/// `SCAN cursor [MATCH pattern] [COUNT n] [TYPE t]` -> the 2-element reply
/// `[next_cursor_bulkstring, [key bulkstrings...]]` (Redis SCAN reply shape).
///
/// The cursor is the decimal wire token ([`ScanCursor`]); `0` starts and a returned
/// `0` means complete. COUNT is a hint on keys EXAMINED (an empty batch with a
/// non-zero cursor is legal). MATCH globs the key; TYPE filters by type name.
pub fn cmd_scan<S: Store + Keyspace>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("scan"));
    }
    let Some(cursor) = ScanCursor::from_token(&req.args[1]) else {
        return Value::error(ErrorReply::invalid_cursor());
    };

    // Parse the option tail: MATCH <pattern>, COUNT <n>, TYPE <name>, in any order.
    let mut pattern: Option<bytes::Bytes> = None;
    let mut count: usize = SCAN_DEFAULT_COUNT;
    let mut type_filter = TypeFilter::Any;
    let mut i = 2;
    while i < req.args.len() {
        let opt = crate::cmd_util::ascii_upper(&req.args[i]);
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
                // COUNT must be a positive integer (Redis errors on <= 0 / non-int with
                // the syntax error).
                match parse_i64(&req.args[i + 1]) {
                    Some(n) if n > 0 => count = n as usize,
                    _ => return Value::error(ErrorReply::syntax_error()),
                }
                i += 2;
            }
            b"TYPE" => {
                if i + 1 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                // The TYPE filter is matched against the canonical type name. A
                // recognized name becomes `Is(DataType)` (an exact type comparison); an
                // unknown name becomes `MatchNothing` so it yields no keys (Redis does
                // not error; it just yields no keys of that type).
                type_filter = type_filter_from_name(&req.args[i + 1]);
                i += 2;
            }
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    }

    let (next, batch) = store.scan_step(db, cursor, count, now, |key, ty| {
        let type_ok = match type_filter {
            TypeFilter::Any => true,
            TypeFilter::Is(want) => ty == want,
            TypeFilter::MatchNothing => false,
        };
        let match_ok = pattern.as_ref().is_none_or(|p| glob_match(p, key));
        type_ok && match_ok
    });

    let keys: Vec<Value> = batch
        .into_iter()
        .map(|k| Value::bulk(k.into_vec()))
        .collect();
    // The reply: [next_cursor_bulkstring, [key bulkstrings...]].
    Value::Array(Some(vec![
        Value::bulk(next.to_token().into_bytes()),
        Value::Array(Some(keys)),
    ]))
}

/// The SCAN `TYPE` filter state. A dedicated enum (rather than overloading a real
/// [`DataType`] as a never-match sentinel) so a recognized type name routes through a
/// REAL type comparison: `TYPE stream` is [`TypeFilter::Is`]`(DataType::Stream)` (it
/// matches nothing TODAY only because no Stream-typed values exist yet, not because of a
/// sentinel collision), while a truly unknown name is [`TypeFilter::MatchNothing`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TypeFilter {
    /// No `TYPE` argument: every type passes.
    Any,
    /// A recognized type name: keep only keys of exactly this [`DataType`].
    Is(DataType),
    /// An unrecognized type name: keep nothing (Redis yields no keys, never an error).
    MatchNothing,
}

/// Route a SCAN `TYPE <name>` argument to a [`TypeFilter`]: a recognized type name
/// (including `stream`) becomes [`TypeFilter::Is`] so it filters via a REAL type
/// comparison (`stream` matches nothing TODAY only because no Stream-typed values exist,
/// NOT via a sentinel that would collide once they do); an unrecognized name becomes
/// [`TypeFilter::MatchNothing`].
fn type_filter_from_name(name: &[u8]) -> TypeFilter {
    match type_name_to_data_type(name) {
        Some(t) => TypeFilter::Is(t),
        None => TypeFilter::MatchNothing,
    }
}

/// Map a Redis type name (`string`/`list`/`set`/`hash`/`zset`/`stream`) to a
/// [`DataType`], or `None` for an unknown name (SCAN TYPE then matches nothing).
fn type_name_to_data_type(name: &[u8]) -> Option<DataType> {
    let lower = crate::cmd_util::ascii_lower(name);
    match lower.as_slice() {
        b"string" => Some(DataType::String),
        b"list" => Some(DataType::List),
        b"set" => Some(DataType::Set),
        b"hash" => Some(DataType::Hash),
        b"zset" => Some(DataType::ZSet),
        b"stream" => Some(DataType::Stream),
        _ => None,
    }
}

/// `DBSIZE` -> the number of keys in the current DB. A RAW count: Redis does NOT
/// actively expire on DBSIZE (it returns the dict size including not-yet-reaped
/// expired keys), matching [`Keyspace::db_len`].
pub fn cmd_dbsize<S: Store + Keyspace>(store: &mut S, db: u32, req: &Request) -> Value {
    if req.args.len() != 1 {
        return Value::error(ErrorReply::wrong_arity("dbsize"));
    }
    Value::Integer(store.db_len(db) as i64)
}

/// The per-`scan_step` examine batch for the slot-filter scans (the same large hint `KEYS` uses, so
/// the loop count stays low over a big keyspace; `count`/`limit` bounds the RESULT, this bounds work
/// per step).
const KEYS_SCAN_BATCH: usize = 256;

/// One shard's partial of `CLUSTER COUNTKEYSINSLOT <slot>` (#371, SLOT_KEY_ENUMERATION.md): the
/// number of LIVE keys in `db` on THIS shard whose client cluster slot (`CRC16(hash_tag(key)) %
/// 16384`, the `{hashtag}` rule) equals `slot`. The cross-shard coordinator SUMS the per-shard
/// integers, exactly as it does for [`cmd_dbsize`].
///
/// Implemented as an on-demand scan over the [`Keyspace`] seam (NOT a maintained write-path index):
/// it taxes only this cold admin call, never the write path or the standalone deployment (the
/// command is cluster-mode-only). Lazily-expired keys are skipped by `scan_step`, so the count is
/// what a client could actually read. O(keys in this shard's `db`).
#[must_use]
pub fn count_keys_in_slot<S: Keyspace>(store: &mut S, db: u32, slot: u16, now: UnixMillis) -> u64 {
    let mut total: u64 = 0;
    let mut cursor = ScanCursor::START;
    loop {
        let (next, batch) = store.scan_step(db, cursor, KEYS_SCAN_BATCH, now, |key, _ty| {
            key_slot(key) == slot
        });
        total += batch.len() as u64;
        if next.is_start() {
            break;
        }
        cursor = next;
    }
    total
}

/// One shard's partial of `CLUSTER GETKEYSINSLOT <slot> <count>` (#371, SLOT_KEY_ENUMERATION.md): up
/// to `limit` LIVE keys in `db` on THIS shard whose client cluster slot equals `slot`, in the stable
/// `scan_step` order (ADR-0003 determinism). The cross-shard coordinator CONCATENATES the per-shard
/// vecs and truncates to the global `count`, exactly as it does for [`cmd_keys`]; a shard never needs
/// to return more than `limit`, so it short-circuits once it has that many. O(keys examined until
/// `limit` matches in this shard's `db`).
#[must_use]
pub fn keys_in_slot<S: Keyspace>(
    store: &mut S,
    db: u32,
    slot: u16,
    limit: usize,
    now: UnixMillis,
) -> Vec<Box<[u8]>> {
    let mut out: Vec<Box<[u8]>> = Vec::new();
    if limit == 0 {
        return out;
    }
    let mut cursor = ScanCursor::START;
    loop {
        let (next, batch) = store.scan_step(db, cursor, KEYS_SCAN_BATCH, now, |key, _ty| {
            key_slot(key) == slot
        });
        for k in batch {
            out.push(k);
            if out.len() >= limit {
                return out;
            }
        }
        if next.is_start() {
            break;
        }
        cursor = next;
    }
    out
}

/// `RANDOMKEY` -> a pseudo-random live key, or null if the DB is empty. `pick` is the
/// random index the dispatch layer drew from the Env RNG (ADR-0003: the store reads no
/// RNG; the caller passes the index in).
pub fn cmd_randomkey<S: Store + Keyspace>(
    store: &mut S,
    db: u32,
    pick: u64,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() != 1 {
        return Value::error(ErrorReply::wrong_arity("randomkey"));
    }
    match store.random_key(db, pick, now) {
        Some(k) => Value::bulk(k.into_vec()),
        None => Value::Null,
    }
}

/// `RENAME src dst` -> `+OK`, or `-ERR no such key` if `src` is absent. Moves the
/// value object INTACT (encoding + remaining TTL preserved); overwrites `dst` if it
/// exists.
pub fn cmd_rename<S: Store + Keyspace>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("rename"));
    }
    let outcome = store.move_object(
        db,
        &req.args[1],
        db,
        &req.args[2],
        MoveMode::Rename,
        true, // RENAME overwrites the destination unconditionally.
        now,
    );
    match outcome {
        // RENAME never declines on a destination (replace=true), so DestExists/Copied
        // cannot occur; they fold into the success arm defensively.
        MoveOutcome::Moved | MoveOutcome::DestExists | MoveOutcome::Copied => Value::ok(),
        MoveOutcome::NoSource => Value::error(ErrorReply::no_such_key()),
    }
}

/// `RENAMENX src dst` -> `:1` on success, `:0` if `dst` already exists, `-ERR no such
/// key` if `src` is absent. Preserves the value object intact.
pub fn cmd_renamenx<S: Store + Keyspace>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("renamenx"));
    }
    let outcome = store.move_object(
        db,
        &req.args[1],
        db,
        &req.args[2],
        MoveMode::Rename,
        false, // RENAMENX declines if the destination exists.
        now,
    );
    match outcome {
        // Copied cannot occur for a Rename mode; folded into success defensively.
        MoveOutcome::Moved | MoveOutcome::Copied => Value::Integer(1),
        MoveOutcome::DestExists => Value::Integer(0),
        MoveOutcome::NoSource => Value::error(ErrorReply::no_such_key()),
    }
}

/// `COPY src dst [DB destination-db] [REPLACE]` -> `:1` on copy, `:0` if `dst` exists
/// without REPLACE or `src` is absent. Copies the value object INTACT (encoding +
/// remaining TTL); the source is left in place.
pub fn cmd_copy<S: Store + Keyspace>(
    store: &mut S,
    ctx: &ServerContext,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("copy"));
    }
    let mut dst_db = db;
    let mut replace = false;
    let mut i = 3;
    while i < req.args.len() {
        let opt = crate::cmd_util::ascii_upper(&req.args[i]);
        match opt.as_slice() {
            b"DB" => {
                if i + 1 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                match parse_i64(&req.args[i + 1]) {
                    Some(n) if n >= 0 && n < i64::from(ctx.databases) => dst_db = n as u32,
                    _ => return Value::error(ErrorReply::select_out_of_range()),
                }
                i += 2;
            }
            b"REPLACE" => {
                replace = true;
                i += 1;
            }
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    }
    // COPY src dst with src == dst in the SAME db is an error in Redis ("source and
    // destination objects are the same"); treat same-(db,key) as DestExists -> 0 here
    // when not replacing. The store maps src==dst to Copied, so guard it explicitly.
    if dst_db == db && req.args[1] == req.args[2] {
        return Value::error(ErrorReply::err(
            "source and destination objects are the same",
        ));
    }
    let outcome = store.move_object(
        db,
        &req.args[1],
        dst_db,
        &req.args[2],
        MoveMode::Copy,
        replace,
        now,
    );
    match outcome {
        // Moved cannot occur for a Copy mode; folded into success defensively.
        MoveOutcome::Copied | MoveOutcome::Moved => Value::Integer(1),
        MoveOutcome::DestExists | MoveOutcome::NoSource => Value::Integer(0),
    }
}

/// `MOVE key db` -> `:1` if moved, `:0` if `key` is absent in the source db or already
/// present in the destination db (Redis MOVE semantics). Moves the value object INTACT
/// across logical DBs (same shard).
pub fn cmd_move<S: Store + Keyspace>(
    store: &mut S,
    ctx: &ServerContext,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("move"));
    }
    let Some(dst) = parse_i64(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    if dst < 0 || dst >= i64::from(ctx.databases) {
        return Value::error(ErrorReply::select_out_of_range());
    }
    let dst_db = dst as u32;
    // MOVE to the SAME db is an error in Redis ("source and destination objects are
    // the same").
    if dst_db == db {
        return Value::error(ErrorReply::err(
            "source and destination objects are the same",
        ));
    }
    // MOVE never overwrites the destination (replace=false): it is a no-op (0) if the
    // destination already holds the key.
    let outcome = store.move_object(
        db,
        &req.args[1],
        dst_db,
        &req.args[1],
        MoveMode::Rename,
        false,
        now,
    );
    match outcome {
        // Copied cannot occur for a Rename mode; folded into success defensively.
        MoveOutcome::Moved | MoveOutcome::Copied => Value::Integer(1),
        MoveOutcome::NoSource | MoveOutcome::DestExists => Value::Integer(0),
    }
}

/// `SWAPDB index1 index2` -> `+OK`. Swaps the entire contents of two logical DBs in
/// O(1) ([`Keyspace::swap_db`]).
pub fn cmd_swapdb<S: Store + Keyspace>(store: &mut S, ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("swapdb"));
    }
    // `invalid first DB index` / `invalid second DB index` are ONLY for a non-integer
    // PARSE failure (Redis getIntFromObjectOrReply with the per-position message).
    let Some(a) = parse_i64(&req.args[1]) else {
        return Value::error(ErrorReply::err("invalid first DB index"));
    };
    let Some(b) = parse_i64(&req.args[2]) else {
        return Value::error(ErrorReply::err("invalid second DB index"));
    };
    // An out-of-range (but integer) index returns `DB index is out of range` (Redis
    // dbSwapDatabases -> C_ERR), NOT the per-position parse message.
    if a < 0 || a >= i64::from(ctx.databases) {
        return Value::error(ErrorReply::select_out_of_range());
    }
    if b < 0 || b >= i64::from(ctx.databases) {
        return Value::error(ErrorReply::select_out_of_range());
    }
    store.swap_db(a as u32, b as u32);
    Value::ok()
}

/// `FLUSHDB [ASYNC|SYNC]` -> `+OK`. Empties the current DB. ASYNC/SYNC are accepted
/// and treated as SYNC (there is no background free yet, #51); an unknown trailing
/// option is a syntax error.
pub fn cmd_flushdb<S: Store + Keyspace>(store: &mut S, db: u32, req: &Request) -> Value {
    if !flush_mode_ok(req) {
        return Value::error(ErrorReply::syntax_error());
    }
    store.flush_db(db);
    Value::ok()
}

/// `FLUSHALL [ASYNC|SYNC]` -> `+OK`. Empties EVERY DB. ASYNC/SYNC accepted as SYNC.
pub fn cmd_flushall<S: Store + Keyspace>(store: &mut S, req: &Request) -> Value {
    if !flush_mode_ok(req) {
        return Value::error(ErrorReply::syntax_error());
    }
    store.flush_all();
    Value::ok()
}

/// Validate the optional FLUSHDB/FLUSHALL mode arg: nothing, or a single ASYNC/SYNC
/// token (both treated as SYNC). Anything else is a syntax error.
fn flush_mode_ok(req: &Request) -> bool {
    match req.args.len() {
        1 => true,
        2 => {
            let opt = crate::cmd_util::ascii_upper(&req.args[1]);
            matches!(opt.as_slice(), b"ASYNC" | b"SYNC")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::ServerContext;
    use ironcache_eviction::Policy;
    use ironcache_observe::ServerInfo;
    use ironcache_storage::CountingAccounting;
    use ironcache_store::ShardStore;
    use ironcache_store::kvobj::KvObj;

    type TestStore = ShardStore<Policy, CountingAccounting>;

    fn test_store(databases: u32) -> TestStore {
        ShardStore::with_hooks(
            databases,
            Policy::cache_default(),
            CountingAccounting::new(),
        )
    }

    /// A minimal context; `cmd_swapdb` reads only `databases`.
    fn ctx(databases: u32) -> ServerContext {
        let boot = ironcache_config::Config {
            databases,
            shards: 1,
            ..ironcache_config::Config::default()
        };
        let runtime = ironcache_config::RuntimeConfig::from_config(&boot);
        ServerContext {
            runtime,
            acl: crate::acl::AclState::from_requirepass(boot.requirepass.as_deref()),
            databases,
            shards: 1,
            info: ServerInfo {
                tcp_port: 6379,
                shards: 1,
                pid: 1,
                started_at: ironcache_env::Monotonic::ZERO,
                maxmemory: 0,
                maxmemory_policy: "allkeys-lru",
                mem_allocator: "system",
                cluster_node_id: "0000000000000000000000000000000000000000",
                run_id: "0000000000000000000000000000000000000000",
                cluster_enabled: false,
            },
            cluster: None,
            raft: None,
            repl_status: None,
            in_sync_replicas: None,
            repl_history_id: None,
            metrics_registry: None,
            persist_stats: None,
            process_memory: std::sync::Arc::new(ironcache_observe::ProcessMemoryGauge::new()),
            conn_gate: std::sync::Arc::new(ironcache_observe::ConnectionGate::new()),
            slowlog: std::sync::Arc::new(ironcache_observe::SlowLog::new()),
            latency: std::sync::Arc::new(ironcache_observe::LatencyMonitor::new()),
            clients: std::sync::Arc::new(ironcache_observe::ClientRegistry::new()),
            hotkeys: std::sync::Arc::new(ironcache_observe::Hotkeys::new()),
            boot,
        }
    }

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts
                .iter()
                .map(|p| bytes::Bytes::copy_from_slice(p))
                .collect(),
        }
    }

    /// A slot guaranteed to hold NO seeded key: the first slot index that is neither `a` nor `b`.
    fn an_empty_slot(a: u16, b: u16) -> u16 {
        (0..ironcache_protocol::CLUSTER_SLOTS)
            .find(|&s| s != a && s != b)
            .expect("16384 slots, at most 2 excluded")
    }

    #[test]
    fn count_keys_in_slot_counts_only_the_matching_slot() {
        let mut store = test_store(1);
        let now = UnixMillis(0);
        // The `{hashtag}` rule routes `{tag}x` to `key_slot("tag")`, so these are deterministic.
        let s1 = key_slot(b"tag1");
        let s2 = key_slot(b"tag2");
        assert_ne!(
            s1, s2,
            "the two tags must occupy distinct slots for the test"
        );
        for k in [b"{tag1}a".as_ref(), b"{tag1}b", b"{tag1}c"] {
            store.insert_object(0, KvObj::from_bytes(k, b"v", None));
        }
        store.insert_object(0, KvObj::from_bytes(b"{tag2}x", b"v", None));

        assert_eq!(count_keys_in_slot(&mut store, 0, s1, now), 3);
        assert_eq!(count_keys_in_slot(&mut store, 0, s2, now), 1);
        // A slot with no keys is 0, not an error.
        assert_eq!(
            count_keys_in_slot(&mut store, 0, an_empty_slot(s1, s2), now),
            0
        );
        // The per-slot counts sum to the whole db (conservation: every key lands in exactly one slot).
        assert_eq!(u64::from(store.db_len(0) as u32), 4);
    }

    #[test]
    fn keys_in_slot_returns_the_matching_keys_bounded_and_deterministic() {
        let mut store = test_store(1);
        let now = UnixMillis(0);
        let s1 = key_slot(b"tag1");
        let s2 = key_slot(b"tag2");
        for k in [b"{tag1}a".as_ref(), b"{tag1}b", b"{tag1}c"] {
            store.insert_object(0, KvObj::from_bytes(k, b"v", None));
        }
        store.insert_object(0, KvObj::from_bytes(b"{tag2}x", b"v", None));

        // A generous limit returns exactly the 3 keys in s1, and ONLY those (not the s2 key).
        let mut got: Vec<Vec<u8>> = keys_in_slot(&mut store, 0, s1, 10, now)
            .into_iter()
            .map(Vec::from)
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                b"{tag1}a".to_vec(),
                b"{tag1}b".to_vec(),
                b"{tag1}c".to_vec()
            ]
        );

        // The limit bounds the result; a 0 limit returns nothing.
        assert_eq!(keys_in_slot(&mut store, 0, s1, 1, now).len(), 1);
        assert!(keys_in_slot(&mut store, 0, s1, 0, now).is_empty());

        // Deterministic (ADR-0003): the same store state yields the same bounded prefix every call.
        assert_eq!(
            keys_in_slot(&mut store, 0, s1, 2, now),
            keys_in_slot(&mut store, 0, s1, 2, now)
        );

        // An empty slot yields no keys.
        assert!(keys_in_slot(&mut store, 0, an_empty_slot(s1, s2), 10, now).is_empty());
    }

    #[test]
    fn slot_partials_skip_lazily_expired_keys() {
        let mut store = test_store(1);
        let s = key_slot(b"tag");
        // A key with a deadline in the past is lazily expired: scan_step skips it, so the slot
        // count/keys exclude it (it matches what a client could read).
        store.insert_object(0, KvObj::from_bytes(b"{tag}live", b"v", None));
        store.insert_object(
            0,
            KvObj::from_bytes(b"{tag}dead", b"v", Some(UnixMillis(10))),
        );
        let now = UnixMillis(1000); // past the dead key's deadline.
        assert_eq!(count_keys_in_slot(&mut store, 0, s, now), 1);
        let keys = keys_in_slot(&mut store, 0, s, 10, now);
        assert_eq!(keys.len(), 1);
        assert_eq!(&*keys[0], b"{tag}live");
    }

    /// Drive SCAN to completion and return every key seen (as owned byte vecs).
    fn scan_all(store: &mut TestStore, parts: &[&[u8]]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut cursor = b"0".to_vec();
        loop {
            let mut full: Vec<&[u8]> = vec![b"SCAN", cursor.as_slice()];
            full.extend_from_slice(parts);
            let reply = cmd_scan(store, 0, UnixMillis(0), &req(&full));
            let Value::Array(Some(items)) = reply else {
                panic!("SCAN reply is not an array: {reply:?}");
            };
            let (Value::BulkString(Some(next)), Value::Array(Some(keys))) = (&items[0], &items[1])
            else {
                panic!("unexpected SCAN reply shape: {items:?}");
            };
            for k in keys {
                if let Value::BulkString(Some(b)) = k {
                    out.push(b.to_vec());
                }
            }
            if next.as_ref() == b"0" {
                break;
            }
            cursor = next.to_vec();
        }
        out
    }

    #[test]
    fn swapdb_out_of_range_index_is_db_index_out_of_range() {
        // An out-of-range (but integer) index returns `DB index is out of range`
        // (dbSwapDatabases C_ERR), NOT the per-position `invalid first/second DB index`
        // message (which is reserved for a non-integer parse failure).
        let mut store = test_store(2);
        let c = ctx(2);
        match cmd_swapdb(&mut store, &c, &req(&[b"SWAPDB", b"99", b"0"])) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR DB index is out of range"),
            other => panic!("expected DB-index-out-of-range, got {other:?}"),
        }
        // The non-integer parse arm STILL uses the per-position message.
        match cmd_swapdb(&mut store, &c, &req(&[b"SWAPDB", b"x", b"0"])) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR invalid first DB index"),
            other => panic!("expected invalid-first-DB-index, got {other:?}"),
        }
    }

    #[test]
    fn type_filter_stream_routes_through_is_not_a_sentinel() {
        // `TYPE stream` must route through a REAL `Is(DataType::Stream)` comparison, not
        // a never-match sentinel that overloads a real DataType: a recognized name maps
        // to `Is(..)`, an unknown name to `MatchNothing`.
        assert_eq!(
            type_filter_from_name(b"stream"),
            TypeFilter::Is(DataType::Stream)
        );
        assert_eq!(
            type_filter_from_name(b"string"),
            TypeFilter::Is(DataType::String)
        );
        assert_eq!(type_filter_from_name(b"bogus"), TypeFilter::MatchNothing);

        // End to end: a recognized type filters by exact type (a String key passes
        // `TYPE string`), `TYPE stream` yields nothing today (no Stream values exist) yet
        // does so via the real comparison, and an unknown `TYPE bogus` also yields
        // nothing (never an error).
        let mut store = test_store(2);
        store.insert_object(0, KvObj::from_bytes(b"k", b"v", None));
        assert_eq!(
            scan_all(&mut store, &[b"TYPE", b"string"]),
            vec![b"k".to_vec()]
        );
        assert!(scan_all(&mut store, &[b"TYPE", b"stream"]).is_empty());
        assert!(scan_all(&mut store, &[b"TYPE", b"bogus"]).is_empty());
    }
}
