// SPDX-License-Identifier: MIT OR Apache-2.0
//! Keyspace-notification EVENT MAPPING for the command dispatcher (PROD-8, the
//! canonical Redis event names from the pinned claim [keyspace-notifications-off-by-default]).
//!
//! The recording machinery (the per-shard pending buffer, the flag gate, the channel
//! formatting) lives in `ironcache_config::notify`; THIS module owns the
//! command-specific KNOWLEDGE: given a just-executed command + its reply, which Redis
//! event(s) fire, on which key(s), and of which class. [`notify_for_command`] is called
//! ONCE after [`crate::dispatch::dispatch_inner`] returns, so the handlers stay untouched
//! and the whole command->event table lives in one reviewable place.
//!
//! ## Why reply-driven, and where it is precise
//!
//! Redis fires a keyspace event from INSIDE each command, gated on the command actually
//! having mutated something. We reconstruct that gate from the command + its REPLY: a
//! SETNX that returns `0` did not write (no event); an EXPIRE that returns `0` set no
//! TTL (no event); an LPUSH that returns a positive length pushed (event). This is exact
//! for the single-key commands whose reply unambiguously signals success. The few
//! commands whose success is NOT recoverable from the reply (multi-key DEL/UNLINK: the
//! reply is a COUNT, not WHICH keys) record their events INSIDE the handler instead (see
//! `cmd_keyspace`), so this table does NOT cover them. A WRONGTYPE / error reply never
//! fires (the command did not mutate). Read-only commands and admin commands are absent
//! from the table, so they record nothing.
//!
//! ## Skipped (documented)
//!
//! - Streams (`t`) and modules (`d`): IronCache has no streams or modules, so no `t`/`d`
//!   event ever fires (the flag chars parse for Redis parity but select nothing).
//! - `keymiss` (`m`) + `new` (`n`): the read-miss + new-key events are NOT emitted this
//!   pass (they require per-command miss / create detection the reply does not carry);
//!   the flag chars are recognized so a `CONFIG SET notify-keyspace-events Km`/`...n`
//!   round-trips, but no `keymiss`/`new` event is produced. Documented as a follow-up.
//! - Blocking list ops (`BLPOP` etc.), `SORT ... STORE`, `BITOP`, `GEO*`, `PF*`: not in
//!   the table this pass (they are lower-traffic and several are absent or partial); they
//!   record nothing. The core string/list/set/hash/zset/generic/expiry surface IS mapped.
//! - The SECONDARY `del` Redis fires when a collection command (LPOP/RPOP/SPOP/LREM/LTRIM/
//!   SREM/HDEL/ZPOPMIN/ZPOPMAX/...) removes the LAST element and the key is deleted is NOT
//!   emitted; only the primary type event fires. Likewise `EXPIRE`/`PEXPIREAT` with an
//!   already-past time emits `expire` (reply 1) rather than the `del` Redis fires on the
//!   immediate delete. Both are documented follow-ups (need post-mutation existence checks).

use ironcache_config::notify::{EventClass, record};
use ironcache_protocol::{Request, Value};

/// Whether `reply` is a non-error, "the command did something" reply for the purpose of
/// the success gate. An error reply (WRONGTYPE / wrong arity / syntax) never fires an
/// event (the command did not mutate). Used by the arms that fire unconditionally on a
/// successful execution (e.g. plain SET always writes).
fn is_ok_reply(reply: &Value) -> bool {
    !matches!(reply, Value::Error(_))
}

/// Whether `reply` is a positive integer (the success signal for the count-returning
/// mutators: SETNX/EXPIRE/PERSIST/SADD/SREM/HSET/HDEL/ZADD/ZREM/RENAMENX/MOVE/COPY return
/// `1`/a positive count on the mutating path and `0` on the no-op path).
fn positive_int(reply: &Value) -> bool {
    matches!(reply, Value::Integer(n) if *n > 0)
}

/// Whether `reply` is a NON-nil bulk / array reply (the success signal for the pop
/// commands: LPOP/RPOP/SPOP return a bulk (or array) on a real pop and nil on an empty /
/// absent key). A `Null` / `Array(None)` / `BulkString(None)` is the no-op.
fn non_nil(reply: &Value) -> bool {
    !matches!(
        reply,
        Value::Null | Value::BulkString(None) | Value::Array(None)
    )
}

/// Record the keyspace event(s) a just-executed `cmd` produced, given its `req` and
/// `reply`, into the per-shard pending buffer (PROD-8). The FIRST thing the underlying
/// `record` does is the disabled-flags short-circuit, so on the default deployment this
/// is a single `Cell` read per recorded call and most commands record nothing at all
/// (they are not in the match). `db` is the connection's logical DB.
///
/// `cmd` is the UPPERCASED command token (the dispatcher's `cmd`). The key, for the
/// single-key commands, is `req.args[1]`; the match guards each arm on the success signal
/// recoverable from `reply`.
///
/// `too_many_lines` is allowed: this is the COMMAND->EVENT TABLE, one match arm per mapped
/// command (the intended big-match shape, exactly like `dispatch::dispatch_inner`); splitting
/// it would scatter the single source of truth for the event names.
#[allow(clippy::too_many_lines)]
pub fn notify_for_command(cmd: &[u8], req: &Request, reply: &Value, db: u32) {
    // ZERO-COST-WHEN-DISABLED GATE: on the default deployment (`notify-keyspace-events` empty) the
    // per-command flags snapshot is the disabled set, so return BEFORE the command->event match is
    // even entered. This is the single cheap check (a thread-local `Cell` read + the
    // already-disabled `is_enabled`) that keeps the write hot path byte-identical when off -- the
    // big match below never runs unless notifications are actually enabled.
    if !ironcache_config::notify::flags_for_command().is_enabled() {
        return;
    }
    // The single-key commands all key on args[1]; bail if it is absent (a wrong-arity
    // command already errored, so this is just defensive).
    let key = match req.args.get(1) {
        Some(k) => k.as_ref(),
        None => return,
    };

    match cmd {
        // -- STRING class ($) --
        // SET / SETEX / PSETEX / GETSET all land a string write named `set`. SET with GET
        // returns the old value (a bulk/nil) but still wrote on the fire path; an NX/XX SET
        // that did not fire returns nil with NO write. We approximate: a non-error reply
        // fires `set` -- which is exact for plain SET / SETEX / GETSET, and for SET NX/XX it
        // OVER-fires on the no-op nil case. To stay precise for the conditional forms, SETNX
        // (the dedicated NX command) is gated on the integer below; plain SET with inline
        // NX/XX is the documented edge where reply-driven cannot tell a no-op nil from a GET
        // nil, so it is mapped on the common (non-conditional) path only.
        b"SET" => {
            // Only fire for the unconditional SET (no NX/XX inline option) so a not-fired
            // conditional SET does not spuriously emit. Inline NX/XX is detected by scanning
            // the options for the NX/XX tokens.
            if is_ok_reply(reply) && !has_conditional_set_option(req) {
                record(EventClass::String, "set", key, db);
                // SET with an EX/PX/EXAT/PXAT option also sets a TTL, so Redis fires `expire`
                // (generic) in addition to `set`.
                if set_has_expire_option(req) {
                    record(EventClass::Generic, "expire", key, db);
                }
            }
        }
        b"SETEX" | b"PSETEX" => {
            // SETEX/PSETEX always set a TTL, so Redis fires BOTH `set` (string) and `expire`
            // (generic), in that order.
            if is_ok_reply(reply) {
                record(EventClass::String, "set", key, db);
                record(EventClass::Generic, "expire", key, db);
            }
        }
        b"GETSET" => {
            // GETSET always writes on a non-error reply (WRONGTYPE on a non-string aborts).
            if is_ok_reply(reply) {
                record(EventClass::String, "set", key, db);
            }
        }
        b"SETNX" => {
            // SETNX fires `set` only when it actually set (reply 1).
            if positive_int(reply) {
                record(EventClass::String, "set", key, db);
            }
        }
        b"APPEND" => {
            // APPEND always writes (creates or extends) on a non-error integer reply.
            if matches!(reply, Value::Integer(_)) {
                record(EventClass::String, "append", key, db);
            }
        }
        b"SETRANGE" => {
            // SETRANGE writes when the resulting length is > 0 (a zero-length no-op on an
            // absent key with empty value writes nothing -> reply 0).
            if positive_int(reply) {
                record(EventClass::String, "setrange", key, db);
            }
        }
        b"INCR" | b"INCRBY" | b"DECR" | b"DECRBY" => {
            // The integer counters always write on a non-error integer reply. Redis routes all
            // four (INCR/INCRBY/DECR/DECRBY) through one shared body that fires `incrby` -- so
            // DECR/DECRBY ALSO emit `incrby`, not `decrby` (a channel no real client subscribes).
            if matches!(reply, Value::Integer(_)) {
                record(EventClass::String, "incrby", key, db);
            }
        }
        b"INCRBYFLOAT" => {
            // Redis fires `incrbyfloat` on a successful float increment (a bulk reply).
            if non_nil(reply) && is_ok_reply(reply) {
                record(EventClass::String, "incrbyfloat", key, db);
            }
        }
        b"GETDEL" => {
            // GETDEL deletes when it returned the old value (a non-nil bulk); a nil means the
            // key was absent (nothing deleted). The event is `del` (a deletion), class g.
            if non_nil(reply) && is_ok_reply(reply) {
                record(EventClass::Generic, "del", key, db);
            }
        }

        // -- LIST class (l) --
        b"LPUSH" | b"LPUSHX" => {
            if positive_int(reply) {
                record(EventClass::List, "lpush", key, db);
            }
        }
        b"RPUSH" | b"RPUSHX" => {
            if positive_int(reply) {
                record(EventClass::List, "rpush", key, db);
            }
        }
        b"LPOP" => {
            if non_nil(reply) && is_ok_reply(reply) {
                record(EventClass::List, "lpop", key, db);
            }
        }
        b"RPOP" => {
            if non_nil(reply) && is_ok_reply(reply) {
                record(EventClass::List, "rpop", key, db);
            }
        }
        b"LSET" => {
            // LSET fires `lset` on success (+OK); an out-of-range / WRONGTYPE errors.
            if is_ok_reply(reply) {
                record(EventClass::List, "lset", key, db);
            }
        }
        b"LINSERT" => {
            // LINSERT returns the new length (>0) on insert, 0 when the pivot was not found,
            // -1 when the key is absent. Only a positive length inserted.
            if positive_int(reply) {
                record(EventClass::List, "linsert", key, db);
            }
        }
        b"LREM" => {
            // LREM removed elements iff it returned a positive count.
            if positive_int(reply) {
                record(EventClass::List, "lrem", key, db);
            }
        }
        b"LTRIM" => {
            if is_ok_reply(reply) {
                record(EventClass::List, "ltrim", key, db);
            }
        }

        // -- SET class (s) --
        b"SADD" => {
            if positive_int(reply) {
                record(EventClass::Set, "sadd", key, db);
            }
        }
        b"SREM" => {
            if positive_int(reply) {
                record(EventClass::Set, "srem", key, db);
            }
        }
        b"SPOP" => {
            if non_nil(reply) && is_ok_reply(reply) {
                record(EventClass::Set, "spop", key, db);
            }
        }
        b"SMOVE" => {
            // SMOVE returns 1 when it moved a member: it fires `srem` on the source AND `sadd`
            // on the destination (Redis). The source is args[1], the destination args[2].
            if positive_int(reply) {
                record(EventClass::Set, "srem", key, db);
                if let Some(dst) = req.args.get(2) {
                    record(EventClass::Set, "sadd", dst.as_ref(), db);
                }
            }
        }

        // -- HASH class (h) --
        b"HSET" | b"HMSET" | b"HSETNX" => {
            // HSET returns the count of NEW fields (may be 0 on pure overwrite, but it still
            // wrote); HMSET returns +OK; HSETNX returns 1 on set. Fire on any non-error reply
            // for HSET/HMSET (a write happened) and on a positive int for HSETNX.
            let fired = if cmd == b"HSETNX" {
                positive_int(reply)
            } else {
                is_ok_reply(reply)
            };
            if fired {
                record(EventClass::Hash, "hset", key, db);
            }
        }
        b"HDEL" => {
            if positive_int(reply) {
                record(EventClass::Hash, "hdel", key, db);
            }
        }
        b"HINCRBY" => {
            if matches!(reply, Value::Integer(_)) {
                record(EventClass::Hash, "hincrby", key, db);
            }
        }
        b"HINCRBYFLOAT" => {
            if non_nil(reply) && is_ok_reply(reply) {
                record(EventClass::Hash, "hincrbyfloat", key, db);
            }
        }

        // -- ZSET class (z) --
        b"ZADD" => {
            // ZADD returns the count of ADDED members (or the changed count with CH); a
            // positive reply means a write. (A pure no-op ZADD with all members present and
            // unchanged returns 0 and is treated as no-event, matching the common case.)
            if positive_int(reply) {
                record(EventClass::Zset, "zadd", key, db);
            }
        }
        b"ZINCRBY" => {
            if non_nil(reply) && is_ok_reply(reply) {
                record(EventClass::Zset, "zincr", key, db);
            }
        }
        b"ZREM" => {
            if positive_int(reply) {
                record(EventClass::Zset, "zrem", key, db);
            }
        }
        b"ZPOPMIN" => {
            if non_nil(reply) && is_ok_reply(reply) {
                record(EventClass::Zset, "zpopmin", key, db);
            }
        }
        b"ZPOPMAX" => {
            if non_nil(reply) && is_ok_reply(reply) {
                record(EventClass::Zset, "zpopmax", key, db);
            }
        }

        // -- GENERIC class (g) --
        b"EXPIRE" | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT" => {
            // The TTL setters fire `expire` when they actually set a TTL (reply 1); reply 0
            // means the condition (NX/XX/GT/LT) was not met or the key is absent.
            if positive_int(reply) {
                record(EventClass::Generic, "expire", key, db);
            }
        }
        b"PERSIST" => {
            // PERSIST fires `persist` when it removed a TTL (reply 1).
            if positive_int(reply) {
                record(EventClass::Generic, "persist", key, db);
            }
        }
        b"RENAME" => {
            // RENAME fires `rename_from` on the source + `rename_to` on the destination on
            // success (+OK). The source is args[1], the destination args[2].
            if is_ok_reply(reply) {
                record(EventClass::Generic, "rename_from", key, db);
                if let Some(dst) = req.args.get(2) {
                    record(EventClass::Generic, "rename_to", dst.as_ref(), db);
                }
            }
        }
        b"RENAMENX" => {
            // RENAMENX fires the same pair only when it actually renamed (reply 1).
            if positive_int(reply) {
                record(EventClass::Generic, "rename_from", key, db);
                if let Some(dst) = req.args.get(2) {
                    record(EventClass::Generic, "rename_to", dst.as_ref(), db);
                }
            }
        }
        b"MOVE" => {
            // MOVE fires `move_from` on the source DB + `move_to` on the destination DB when
            // it moved (reply 1). The destination DB is args[2]; `move_to`'s channel uses the
            // destination DB index, so it is emitted with that db.
            if positive_int(reply) {
                record(EventClass::Generic, "move_from", key, db);
                if let Some(dst_db) = req.args.get(2).and_then(|d| parse_db(d.as_ref())) {
                    record(EventClass::Generic, "move_to", key, dst_db);
                }
            }
        }
        b"COPY" => {
            // COPY fires `copy_to` on the DESTINATION key when it copied (reply 1). The
            // destination is args[2] (a DB option may change the db, but the common form
            // copies within the same db).
            if positive_int(reply) {
                if let Some(dst) = req.args.get(2) {
                    record(EventClass::Generic, "copy_to", dst.as_ref(), db);
                }
            }
        }
        b"RESTORE" => {
            if is_ok_reply(reply) {
                record(EventClass::Generic, "restore", key, db);
            }
        }

        // Everything else (reads, admin, pub/sub, cluster, ...) records nothing.
        _ => {}
    }
}

/// Whether a `SET` request carries an inline NX or XX conditional option (so the
/// reply-driven success gate cannot tell a not-fired nil from a GET nil). Scans the
/// options `req.args[3..]` case-insensitively for the `NX`/`XX` tokens.
fn has_conditional_set_option(req: &Request) -> bool {
    req.args
        .iter()
        .skip(3)
        .any(|a| a.eq_ignore_ascii_case(b"NX") || a.eq_ignore_ascii_case(b"XX"))
}

/// True if a `SET` carries an EX/PX/EXAT/PXAT expiration option (so it also fires `expire`).
fn set_has_expire_option(req: &Request) -> bool {
    req.args.iter().skip(3).any(|a| {
        a.eq_ignore_ascii_case(b"EX")
            || a.eq_ignore_ascii_case(b"PX")
            || a.eq_ignore_ascii_case(b"EXAT")
            || a.eq_ignore_ascii_case(b"PXAT")
    })
}

/// Parse a DB index argument (MOVE / COPY DB) as a `u32`, returning `None` on a malformed
/// value (the command already validated it; this is the notification-side re-parse).
fn parse_db(arg: &[u8]) -> Option<u32> {
    std::str::from_utf8(arg).ok()?.trim().parse::<u32>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ironcache_config::notify::{NotifyFlags, drain, set_command_flags};

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    /// Enable all classes + both channels for the duration of a test body, run `f`, then
    /// drain + return the recorded events. Resets the per-thread flags after.
    fn with_all_enabled(f: impl FnOnce()) -> Vec<ironcache_config::KeyspaceEvent> {
        set_command_flags(NotifyFlags::parse("KEA").unwrap());
        ironcache_config::notify::clear_pending();
        f();
        let events = drain();
        set_command_flags(NotifyFlags::empty());
        events
    }

    #[test]
    fn set_fires_set_event() {
        let events = with_all_enabled(|| {
            notify_for_command(b"SET", &req(&[b"SET", b"k", b"v"]), &Value::ok(), 0);
        });
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, "set");
        assert_eq!(events[0].key, b"k");
    }

    #[test]
    fn setnx_fires_only_on_success() {
        // reply 1 -> fires; reply 0 -> no event.
        let fired = with_all_enabled(|| {
            notify_for_command(
                b"SETNX",
                &req(&[b"SETNX", b"k", b"v"]),
                &Value::Integer(1),
                0,
            );
        });
        assert_eq!(fired.len(), 1);
        let not_fired = with_all_enabled(|| {
            notify_for_command(
                b"SETNX",
                &req(&[b"SETNX", b"k", b"v"]),
                &Value::Integer(0),
                0,
            );
        });
        assert!(not_fired.is_empty());
    }

    #[test]
    fn expire_fires_only_when_ttl_set() {
        let fired = with_all_enabled(|| {
            notify_for_command(
                b"EXPIRE",
                &req(&[b"EXPIRE", b"k", b"10"]),
                &Value::Integer(1),
                0,
            );
        });
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].event, "expire");
        let not_fired = with_all_enabled(|| {
            notify_for_command(
                b"EXPIRE",
                &req(&[b"EXPIRE", b"k", b"10"]),
                &Value::Integer(0),
                0,
            );
        });
        assert!(not_fired.is_empty());
    }

    #[test]
    fn lpush_fires_on_positive_length() {
        let events = with_all_enabled(|| {
            notify_for_command(
                b"LPUSH",
                &req(&[b"LPUSH", b"l", b"a"]),
                &Value::Integer(1),
                0,
            );
        });
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, "lpush");
    }

    #[test]
    fn rename_fires_from_and_to() {
        let events = with_all_enabled(|| {
            notify_for_command(b"RENAME", &req(&[b"RENAME", b"a", b"b"]), &Value::ok(), 0);
        });
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event, "rename_from");
        assert_eq!(events[0].key, b"a");
        assert_eq!(events[1].event, "rename_to");
        assert_eq!(events[1].key, b"b");
    }

    #[test]
    fn wrongtype_error_fires_nothing() {
        let events = with_all_enabled(|| {
            notify_for_command(
                b"LPUSH",
                &req(&[b"LPUSH", b"k", b"a"]),
                &Value::error(ironcache_protocol::ErrorReply::wrong_type()),
                0,
            );
        });
        assert!(events.is_empty());
    }

    #[test]
    fn conditional_set_does_not_fire_on_the_common_path() {
        // SET k v NX with a nil reply (not fired) records nothing (the conditional guard).
        let events = with_all_enabled(|| {
            notify_for_command(b"SET", &req(&[b"SET", b"k", b"v", b"NX"]), &Value::Null, 0);
        });
        assert!(events.is_empty());
    }
}
