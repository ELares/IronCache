// SPDX-License-Identifier: MIT OR Apache-2.0
//! List-type command handlers over the storage waist (COMMANDS.md list semantics,
//! COLLECTIONS.md / LIST_LARGE.md, the in-place-mutation RMW extension, PR-5).
//!
//! Every MUTATING list command routes through [`Store::rmw_mut`] (the collection
//! in-place-mutation arm): the closure edits the stored list through the typed
//! [`ListValue`] view on [`RmwEntry::OccupiedMut`] and returns [`RmwAction::Mutated`]
//! (the store measures the byte delta, recomputes the encoding, and deletes the key
//! if the edit emptied the list), or [`RmwAction::Insert`] to create the list on a
//! missing key (LPUSH/RPUSH/... on a vacant key), or [`RmwAction::Delete`] when the
//! handler knows the post-edit count is zero (e.g. LPOP that drains the last element).
//! READ-ONLY list commands (LLEN/LRANGE/LINDEX/LPOS) also go through `rmw_mut` with
//! [`RmwAction::Keep`] (no write, no accounting change): the typed view is the only
//! way to read list contents through the waist, and `Keep` leaves the value untouched.
//!
//! WRONGTYPE is checked before any mutation: a list command on a non-list key returns
//! `-WRONGTYPE` with no write (the typed [`OccupiedEntryMut::as_list_mut`] returns
//! `None` for a non-list, and the handler maps that to WRONGTYPE + `Keep`).
//!
//! A list is NEVER stored empty: when the last element is removed (LPOP/RPOP/LREM/
//! LTRIM to empty) the key is deleted (the store's empty-collection-deletes-key
//! backstop, plus the explicit `Delete` action where the handler can tell). So an
//! empty list is never observable, matching Redis.
//!
//! Blocking variants (BLPOP/BRPOP/BLMOVE/LMPOP/BLMPOP) are DEFERRED (they need
//! blocking infrastructure) and are NOT implemented here.

use crate::cmd_util::{ascii_upper, parse_i64_strict};
use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{
    DataType, ExpireWrite, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};

/// A no-write rmw step that returns `reply` (value untouched, TTL untouched). The
/// shared abort/short-circuit path for the list handlers (WRONGTYPE, read replies).
fn keep(reply: Value) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply,
    }
}

/// The WRONGTYPE no-write step (a list command against a non-list key).
fn wrong_type() -> RmwStep<Value> {
    keep(Value::error(ErrorReply::wrong_type()))
}

/// A bulk reply from owned bytes.
fn bulk(bytes: Vec<u8>) -> Value {
    Value::BulkString(Some(Bytes::from(bytes)))
}

/// Build a serialized list value (one element-vector) from `elems` for the CREATE
/// path (LPUSH/RPUSH/... on a missing key). The store classifies the bytes as a list
/// via [`NewValueOwned::List`].
fn new_list(elems: Vec<Vec<u8>>) -> NewValueOwned {
    NewValueOwned::list(elems)
}

// ---------------------------------------------------------------------------
// Push commands: LPUSH / RPUSH (create-on-missing) and LPUSHX / RPUSHX (only if the
// key already holds a list). Variadic; return the new length.
// ---------------------------------------------------------------------------

/// Shared body for LPUSH/RPUSH/LPUSHX/RPUSHX. `front` selects the end; `only_existing`
/// gates the X variants (no create on a missing key). Elements are pushed in argument
/// order, each becoming the NEW head for the front variants (so `LPUSH k a b c` yields
/// `c b a` at the head, matching Redis).
fn push_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    front: bool,
    only_existing: bool,
    cmd_name: &str,
) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    // The elements to push, in argument order.
    let elems: Vec<Vec<u8>> = req.args[2..].iter().map(|b| b.to_vec()).collect();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => {
            if only_existing {
                // LPUSHX/RPUSHX on a missing key: no-op, reply 0.
                return keep(Value::Integer(0));
            }
            // Create the list. For a front push, the elements are prepended in order,
            // so the LAST argument ends up at the head: building head-to-tail means
            // reversing the args for a front push, appending for a back push.
            let mut ordered: Vec<Vec<u8>> = Vec::with_capacity(elems.len());
            if front {
                for e in elems.iter().rev() {
                    ordered.push(e.clone());
                }
            } else {
                ordered.extend(elems.iter().cloned());
            }
            let len = ordered.len() as i64;
            RmwStep {
                action: RmwAction::Insert(new_list(ordered)),
                // A freshly created list has no TTL (Redis: a created key has no TTL).
                expire: ExpireWrite::Clear,
                reply: Value::Integer(len),
            }
        }
        RmwEntry::OccupiedMut(mut o) => {
            let Some(list) = o.as_list_mut() else {
                return wrong_type();
            };
            for e in &elems {
                if front {
                    list.push_front(e);
                } else {
                    list.push_back(e);
                }
            }
            let len = list.len() as i64;
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: Value::Integer(len),
            }
        }
        // `rmw_mut` yields Vacant or OccupiedMut, never the read-only Occupied arm.
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `LPUSH key element [element ...]` -> the new list length.
pub fn cmd_lpush<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    push_generic(store, db, now, req, true, false, "lpush")
}

/// `RPUSH key element [element ...]` -> the new list length.
pub fn cmd_rpush<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    push_generic(store, db, now, req, false, false, "rpush")
}

/// `LPUSHX key element [element ...]` -> the new length, or 0 if the key is absent
/// (only pushes if the key already holds a list).
pub fn cmd_lpushx<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    push_generic(store, db, now, req, true, true, "lpushx")
}

/// `RPUSHX key element [element ...]` -> the new length, or 0 if the key is absent.
pub fn cmd_rpushx<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    push_generic(store, db, now, req, false, true, "rpushx")
}

// ---------------------------------------------------------------------------
// Pop commands: LPOP / RPOP, with the optional count form.
// ---------------------------------------------------------------------------

/// Shared body for LPOP/RPOP. `front` selects the end. Without a count, returns the
/// single popped element as a bulk (or nil if absent). With a count, returns an array
/// of up to `count` popped elements (or nil array if the key is absent); a count of 0
/// returns an empty array on a present key. When the pop drains the list, the key is
/// deleted (RmwAction::Delete, or the store's empty backstop on the Mutated path).
fn pop_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    front: bool,
    cmd_name: &str,
) -> Value {
    // Arity: `LPOP key` or `LPOP key count`.
    if req.args.len() < 2 || req.args.len() > 3 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    // Parse the optional count. A non-integer is the not-an-integer error; a negative
    // count is the must-be-positive error (Redis classes these distinctly).
    let count: Option<i64> = if req.args.len() == 3 {
        match parse_i64_strict(&req.args[2]) {
            Some(n) if n < 0 => {
                return Value::error(ErrorReply::value_out_of_range_must_be_positive());
            }
            Some(n) => Some(n),
            None => return Value::error(ErrorReply::not_an_integer()),
        }
    } else {
        None
    };

    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => {
            // Absent key: nil for the single form, the NULL ARRAY for the count form
            // (Redis LPOP/RPOP with count on a missing key returns a nil array).
            let reply = if count.is_some() {
                Value::Array(None)
            } else {
                Value::Null
            };
            keep(reply)
        }
        RmwEntry::OccupiedMut(mut o) => {
            let Some(list) = o.as_list_mut() else {
                return wrong_type();
            };
            match count {
                None => {
                    // Single-element pop -> a bulk (the list is non-empty here, since an
                    // empty list is never stored).
                    let popped = if front {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    };
                    let reply = popped.map_or(Value::Null, bulk);
                    finish_pop(list, reply)
                }
                Some(n) => {
                    let n = n as usize;
                    let mut out: Vec<Value> = Vec::with_capacity(n.min(list.len()));
                    for _ in 0..n {
                        let popped = if front {
                            list.pop_front()
                        } else {
                            list.pop_back()
                        };
                        match popped {
                            Some(e) => out.push(bulk(e)),
                            None => break,
                        }
                    }
                    finish_pop(list, Value::Array(Some(out)))
                }
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// After a pop edit, choose the action: `Delete` if the list is now empty (the
/// handler knows the post-edit count is zero), else `Mutated` (the store measures the
/// delta). Both honor the empty-collection-deletes-key contract; `Delete` is the
/// explicit form the task says to support alongside the store backstop.
fn finish_pop(list: &mut dyn ironcache_storage::ListValue, reply: Value) -> RmwStep<Value> {
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

/// `LPOP key [count]` -> the popped element(s) or nil.
pub fn cmd_lpop<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    pop_generic(store, db, now, req, true, "lpop")
}

/// `RPOP key [count]` -> the popped element(s) or nil.
pub fn cmd_rpop<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    pop_generic(store, db, now, req, false, "rpop")
}

// ---------------------------------------------------------------------------
// Read commands: LLEN / LRANGE / LINDEX / LPOS. They use rmw_mut with Keep (no write):
// the typed list view is the only way to read list contents through the waist.
// ---------------------------------------------------------------------------

/// `LLEN key` -> the list length, 0 if absent; WRONGTYPE on a non-list.
pub fn cmd_llen<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("llen"));
    }
    store.rmw_mut(db, &req.args[1], now, |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => match o.as_list_mut() {
            Some(list) => keep(Value::Integer(list.len() as i64)),
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `LRANGE key start stop` -> the inclusive sub-range as a bulk array (empty array if
/// absent or the range is empty); WRONGTYPE on a non-list.
pub fn cmd_lrange<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("lrange"));
    }
    let (Some(start), Some(stop)) = (
        parse_i64_strict(&req.args[2]),
        parse_i64_strict(&req.args[3]),
    ) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Array(Some(Vec::new()))),
        RmwEntry::OccupiedMut(mut o) => match o.as_list_mut() {
            Some(list) => {
                let items = list.range(start, stop).into_iter().map(bulk).collect();
                keep(Value::Array(Some(items)))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `LINDEX key index` -> the element at `index` (negative from the tail), or nil if
/// out of range / absent; WRONGTYPE on a non-list.
pub fn cmd_lindex<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("lindex"));
    }
    let Some(index) = parse_i64_strict(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Null),
        RmwEntry::OccupiedMut(mut o) => match o.as_list_mut() {
            Some(list) => keep(list.get(index).map_or(Value::Null, bulk)),
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]` -> the matched
/// index/indices (a single integer without COUNT, an array with COUNT), or nil /
/// empty array when no match; WRONGTYPE on a non-list.
pub fn cmd_lpos<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("lpos"));
    }
    let element = req.args[2].clone();
    // Parse the option tail: RANK r (non-zero), COUNT c (>= 0), MAXLEN m (>= 0).
    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0;
    let mut i = 3;
    while i < req.args.len() {
        let opt = ascii_upper(&req.args[i]);
        match opt.as_slice() {
            b"RANK" => {
                if i + 1 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                match parse_i64_strict(&req.args[i + 1]) {
                    // A non-integer RANK is the generic not-an-integer error (Redis
                    // throws it in the integer parse, before the zero check).
                    None => return Value::error(ErrorReply::not_an_integer()),
                    // RANK 0 has its own specific reply (the rank can't be zero).
                    Some(0) => return Value::error(ErrorReply::lpos_rank_zero()),
                    Some(r) => rank = r,
                }
                i += 2;
            }
            b"COUNT" => {
                if i + 1 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                match parse_i64_strict(&req.args[i + 1]) {
                    // A non-integer COUNT is the generic not-an-integer error.
                    None => return Value::error(ErrorReply::not_an_integer()),
                    // A negative COUNT has its own specific reply.
                    Some(c) if c < 0 => return Value::error(ErrorReply::lpos_count_negative()),
                    Some(c) => count = Some(c as usize),
                }
                i += 2;
            }
            b"MAXLEN" => {
                if i + 1 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                match parse_i64_strict(&req.args[i + 1]) {
                    // A non-integer MAXLEN is the generic not-an-integer error.
                    None => return Value::error(ErrorReply::not_an_integer()),
                    // A negative MAXLEN has its own specific reply.
                    Some(m) if m < 0 => return Value::error(ErrorReply::lpos_maxlen_negative()),
                    Some(m) => maxlen = m as usize,
                }
                i += 2;
            }
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    }

    store.rmw_mut(db, &req.args[1], now, move |entry| {
        let with_count = count.is_some();
        match entry {
            RmwEntry::Vacant => {
                // Absent key: nil without COUNT, empty array with COUNT.
                let reply = if with_count {
                    Value::Array(Some(Vec::new()))
                } else {
                    Value::Null
                };
                keep(reply)
            }
            RmwEntry::OccupiedMut(mut o) => {
                let Some(view) = o.as_list_mut() else {
                    return wrong_type();
                };
                let positions = view.pos(&element, rank, count, maxlen);
                let reply = if with_count {
                    Value::Array(Some(
                        positions
                            .into_iter()
                            .map(|p| Value::Integer(p as i64))
                            .collect(),
                    ))
                } else {
                    positions
                        .first()
                        .map_or(Value::Null, |&p| Value::Integer(p as i64))
                };
                keep(reply)
            }
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        }
    })
}

// ---------------------------------------------------------------------------
// In-place edit commands: LSET / LINSERT / LREM / LTRIM.
// ---------------------------------------------------------------------------

/// `LSET key index element` -> +OK; `-ERR no such key` if absent; `-ERR index out of
/// range` if the index is out of bounds; WRONGTYPE on a non-list.
pub fn cmd_lset<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("lset"));
    }
    let Some(index) = parse_i64_strict(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    let element = req.args[3].clone();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        // LSET on a missing key is `-ERR no such key` (NOT a create).
        RmwEntry::Vacant => keep(Value::error(ErrorReply::no_such_key())),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(list) = o.as_list_mut() else {
                return wrong_type();
            };
            if list.set(index, &element) {
                RmwStep {
                    action: RmwAction::Mutated,
                    expire: ExpireWrite::Unchanged,
                    reply: Value::ok(),
                }
            } else {
                keep(Value::error(ErrorReply::index_out_of_range()))
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `LINSERT key BEFORE|AFTER pivot element` -> the new length, `-1` if `pivot` is not
/// found, `0` if the key is absent; WRONGTYPE on a non-list.
pub fn cmd_linsert<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 5 {
        return Value::error(ErrorReply::wrong_arity("linsert"));
    }
    let where_ = ascii_upper(&req.args[2]);
    let before = match where_.as_slice() {
        b"BEFORE" => true,
        b"AFTER" => false,
        _ => return Value::error(ErrorReply::syntax_error()),
    };
    let pivot = req.args[3].clone();
    let element = req.args[4].clone();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        // LINSERT on a missing key returns 0 (no create).
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(list) = o.as_list_mut() else {
                return wrong_type();
            };
            let result = if before {
                list.insert_before(&pivot, &element)
            } else {
                list.insert_after(&pivot, &element)
            };
            match result {
                Some(len) => RmwStep {
                    action: RmwAction::Mutated,
                    expire: ExpireWrite::Unchanged,
                    reply: Value::Integer(len as i64),
                },
                // Pivot not found: -1, no edit (Keep, no accounting change).
                None => keep(Value::Integer(-1)),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `LREM key count element` -> the number removed. `count > 0` head->tail, `count < 0`
/// tail->head, `count == 0` all matches. Removing the last element deletes the key.
/// WRONGTYPE on a non-list; 0 on a missing key.
pub fn cmd_lrem<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("lrem"));
    }
    let Some(count) = parse_i64_strict(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    let element = req.args[3].clone();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(list) = o.as_list_mut() else {
                return wrong_type();
            };
            let removed = list.remove_matching(count, &element);
            // If the removals emptied the list, delete the key explicitly; else Mutated
            // (the store measures the delta). A no-op removal (removed == 0) is still a
            // Mutated with a zero delta, which the store accounts as no change.
            let action = if list.is_empty() {
                RmwAction::Delete
            } else {
                RmwAction::Mutated
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: Value::Integer(removed as i64),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `LTRIM key start stop` -> +OK. Trims to the inclusive range; an out-of-range range
/// trims to empty, which DELETES the key. WRONGTYPE on a non-list; +OK on a missing key.
pub fn cmd_ltrim<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("ltrim"));
    }
    let (Some(start), Some(stop)) = (
        parse_i64_strict(&req.args[2]),
        parse_i64_strict(&req.args[3]),
    ) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        // LTRIM on a missing key is +OK (no-op).
        RmwEntry::Vacant => keep(Value::ok()),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(list) = o.as_list_mut() else {
                return wrong_type();
            };
            list.trim(start, stop);
            let action = if list.is_empty() {
                RmwAction::Delete
            } else {
                RmwAction::Mutated
            };
            RmwStep {
                action,
                expire: ExpireWrite::Unchanged,
                reply: Value::ok(),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

// ---------------------------------------------------------------------------
// LMOVE / RPOPLPUSH: pop from src end, push to dst end. src == dst rotates.
// ---------------------------------------------------------------------------

/// Shared body for LMOVE/RPOPLPUSH. Pops one element from `src` at the `from_left`
/// end and pushes it to `dst` at the `to_left` end. Returns the moved element (bulk),
/// or nil if `src` is absent/empty. When `src == dst` this is a rotation (a single
/// list edited in one rmw). WRONGTYPE if either key holds a non-list.
///
/// SAME-SHARD only: both keys live on this connection's store (no cross-shard routing
/// exists yet). The cross-key case follows Redis `lmoveGenericCommand`'s SOURCE-FIRST
/// ordering: pop from src FIRST (a missing/empty src replies nil without inspecting dst;
/// a wrong-type src is WRONGTYPE), then type-check dst (a wrong-type dst is WRONGTYPE
/// and the popped element is RESTORED to src so nothing is lost), then push to dst.
fn move_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    src: &[u8],
    dst: &[u8],
    from_left: bool,
    to_left: bool,
) -> Value {
    // The src == dst rotation: one list, one rmw (pop one end, push the other).
    if src == dst {
        return rotate_in_place(store, db, now, src, from_left, to_left);
    }

    // Cross-key: match Redis lmoveGenericCommand's SOURCE-FIRST ordering. Redis looks
    // up the source first; if the source is missing OR an empty list it replies nil and
    // returns WITHOUT ever inspecting the destination. Only when the source is a present,
    // non-empty list does it type-check the destination. So `LMOVE missing wrongtype-dst`
    // is NIL (not WRONGTYPE), and a destination-WRONGTYPE with a present source must NOT
    // lose the element.
    //
    // (a) Pop one element from src (this also WRONGTYPE-checks src). We do NOT inspect
    // dst yet.
    let elem = match pop_one_from_src(store, db, now, src, from_left) {
        // src is the wrong type -> WRONGTYPE (no dst inspection).
        Err(()) => return Value::error(ErrorReply::wrong_type()),
        // src is missing or empty -> nil, WITHOUT inspecting dst (Redis source-first).
        Ok(None) => return Value::Null,
        Ok(Some(e)) => e,
    };

    // (b) The source was present and non-empty: NOW type-check the destination. If dst
    // holds a non-list, Redis aborts with WRONGTYPE and does NOT pop/lose the element.
    // Since we have already committed the pop on src, RESTORE the element to the end it
    // came from before returning WRONGTYPE, so the move is a no-op on a dst-WRONGTYPE.
    match store.type_of(db, dst, now) {
        Some(DataType::List) | None => {}
        Some(_) => {
            // Restore to the end it was popped from (from_left), so src is unchanged.
            push_one_to_end(store, db, now, src, &elem, from_left);
            return Value::error(ErrorReply::wrong_type());
        }
    }

    // (c) Push the element to dst (create the list if dst is absent). dst's type was
    // just validated above.
    push_one_to_end(store, db, now, dst, &elem, to_left);
    bulk(elem)
}

/// The `src == dst` rotation for LMOVE/RPOPLPUSH: one list, one rmw (pop one end, push
/// the other). Replies the moved element as a bulk, or nil if the key is absent.
fn rotate_in_place<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    key: &[u8],
    from_left: bool,
    to_left: bool,
) -> Value {
    store.rmw_mut(db, key, now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Null),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(list) = o.as_list_mut() else {
                return wrong_type();
            };
            let moved = if from_left {
                list.pop_front()
            } else {
                list.pop_back()
            };
            let Some(elem) = moved else {
                // An empty list is never stored, so this is unreachable in practice;
                // be defensive and reply nil with no edit.
                return keep(Value::Null);
            };
            if to_left {
                list.push_front(&elem);
            } else {
                list.push_back(&elem);
            }
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: bulk(elem),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// Pop one element from `src` at the `from_left` end (the cross-key move's step (a)).
/// The pop deletes src if it drains the last element. Returns `Ok(Some(elem))` on a
/// successful pop, `Ok(None)` if src is missing or empty, and `Err(())` if src holds a
/// non-list (WRONGTYPE). The typed reply is carried out of the closure directly.
fn pop_one_from_src<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    src: &[u8],
    from_left: bool,
) -> Result<Option<Vec<u8>>, ()> {
    store.rmw_mut(db, src, now, move |entry| {
        let kept = |r: Result<Option<Vec<u8>>, ()>| RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: r,
        };
        match entry {
            RmwEntry::Vacant => kept(Ok(None)),
            RmwEntry::OccupiedMut(mut o) => {
                let Some(list) = o.as_list_mut() else {
                    return kept(Err(()));
                };
                let moved = if from_left {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                match moved {
                    Some(elem) => {
                        let action = if list.is_empty() {
                            RmwAction::Delete
                        } else {
                            RmwAction::Mutated
                        };
                        RmwStep {
                            action,
                            expire: ExpireWrite::Unchanged,
                            reply: Ok(Some(elem)),
                        }
                    }
                    None => kept(Ok(None)),
                }
            }
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        }
    })
}

/// Push one `elem` to `key` at the `to_left` end, creating the list if the key is
/// absent. Used both to push to the destination (move step (c)) AND to restore the
/// element to src on a dst-WRONGTYPE (move step (b)). The caller has already validated
/// that `key` is a list or absent, so the OccupiedMut non-list arm is defensive.
fn push_one_to_end<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    key: &[u8],
    elem: &[u8],
    to_left: bool,
) {
    let push_elem = elem.to_vec();
    store.rmw_mut(db, key, now, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(new_list(vec![push_elem])),
            expire: ExpireWrite::Clear,
            reply: (),
        },
        RmwEntry::OccupiedMut(mut o) => {
            if let Some(list) = o.as_list_mut() {
                if to_left {
                    list.push_front(&push_elem);
                } else {
                    list.push_back(&push_elem);
                }
            }
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: (),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    });
}

/// `LMOVE source destination LEFT|RIGHT LEFT|RIGHT` -> the moved element, or nil.
pub fn cmd_lmove<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 5 {
        return Value::error(ErrorReply::wrong_arity("lmove"));
    }
    let from = ascii_upper(&req.args[3]);
    let to = ascii_upper(&req.args[4]);
    let from_left = match from.as_slice() {
        b"LEFT" => true,
        b"RIGHT" => false,
        _ => return Value::error(ErrorReply::syntax_error()),
    };
    let to_left = match to.as_slice() {
        b"LEFT" => true,
        b"RIGHT" => false,
        _ => return Value::error(ErrorReply::syntax_error()),
    };
    let src = req.args[1].clone();
    let dst = req.args[2].clone();
    move_generic(store, db, now, &src, &dst, from_left, to_left)
}

/// `RPOPLPUSH source destination` -> the moved element, or nil. Equivalent to
/// `LMOVE source destination RIGHT LEFT`.
pub fn cmd_rpoplpush<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("rpoplpush"));
    }
    let src = req.args[1].clone();
    let dst = req.args[2].clone();
    move_generic(store, db, now, &src, &dst, false, true)
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

    /// LRANGE k 0 -1 as a Vec of owned element bytes (for asserting list contents).
    fn contents(store: &mut TestStore, key: &[u8]) -> Vec<Vec<u8>> {
        let reply = cmd_lrange(store, 0, NOW, &req(&[b"LRANGE", key, b"0", b"-1"]));
        match reply {
            Value::Array(Some(items)) => items
                .into_iter()
                .map(|v| match v {
                    Value::BulkString(Some(b)) => b.to_vec(),
                    other => panic!("non-bulk element: {other:?}"),
                })
                .collect(),
            other => panic!("LRANGE not an array: {other:?}"),
        }
    }

    fn err_line(v: &Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    // ---- Fix 1: LMOVE/RPOPLPUSH SOURCE-FIRST ordering. ----

    #[test]
    fn lmove_missing_src_wrongtype_dst_returns_nil_not_wrongtype() {
        // Redis lmoveGenericCommand looks up the SOURCE first; a missing source replies
        // nil and returns WITHOUT inspecting the destination. So even a wrong-type dst
        // does NOT turn this into WRONGTYPE.
        let mut store = test_store();
        // dst holds a STRING (wrong type for a list push), src is absent.
        store.upsert(
            0,
            b"dst",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        let reply = cmd_lmove(
            &mut store,
            0,
            NOW,
            &req(&[b"LMOVE", b"missing", b"dst", b"LEFT", b"RIGHT"]),
        );
        assert_eq!(
            reply,
            Value::Null,
            "missing src -> nil (dst never inspected)"
        );
    }

    #[test]
    fn rpoplpush_missing_src_wrongtype_dst_returns_nil() {
        let mut store = test_store();
        store.upsert(
            0,
            b"dst",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        let reply = cmd_rpoplpush(
            &mut store,
            0,
            NOW,
            &req(&[b"RPOPLPUSH", b"missing", b"dst"]),
        );
        assert_eq!(
            reply,
            Value::Null,
            "missing src -> nil (dst never inspected)"
        );
    }

    #[test]
    fn lmove_present_src_wrongtype_dst_is_wrongtype_and_does_not_lose_element() {
        // Source present + non-empty, destination wrong type: WRONGTYPE, and the popped
        // element must be RESTORED to src (the move is a no-op on a dst-WRONGTYPE).
        let mut store = test_store();
        // src = [a, b, c]
        cmd_rpush(
            &mut store,
            0,
            NOW,
            &req(&[b"RPUSH", b"src", b"a", b"b", b"c"]),
        );
        // dst is a STRING.
        store.upsert(
            0,
            b"dst",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        let reply = cmd_lmove(
            &mut store,
            0,
            NOW,
            &req(&[b"LMOVE", b"src", b"dst", b"LEFT", b"RIGHT"]),
        );
        assert_eq!(
            err_line(&reply),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        // src is UNCHANGED: the element popped from the LEFT was restored to the LEFT.
        assert_eq!(
            contents(&mut store, b"src"),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            "the popped element is restored to src on a dst-WRONGTYPE"
        );
    }

    #[test]
    fn lmove_present_src_wrongtype_dst_restores_when_src_drains_to_empty() {
        // The restore path also covers the case where the pop drained src to empty (so
        // src was deleted): the element is re-created as a fresh single-element src.
        let mut store = test_store();
        cmd_rpush(&mut store, 0, NOW, &req(&[b"RPUSH", b"src", b"only"]));
        store.upsert(
            0,
            b"dst",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        let reply = cmd_lmove(
            &mut store,
            0,
            NOW,
            &req(&[b"LMOVE", b"src", b"dst", b"LEFT", b"RIGHT"]),
        );
        assert_eq!(
            err_line(&reply),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(
            contents(&mut store, b"src"),
            vec![b"only".to_vec()],
            "the sole element is restored even though src was momentarily drained"
        );
    }

    #[test]
    fn lmove_wrongtype_src_is_wrongtype() {
        // A wrong-type SOURCE is WRONGTYPE (the pop's type check catches it).
        let mut store = test_store();
        store.upsert(
            0,
            b"src",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        let reply = cmd_lmove(
            &mut store,
            0,
            NOW,
            &req(&[b"LMOVE", b"src", b"dst", b"LEFT", b"RIGHT"]),
        );
        assert_eq!(
            err_line(&reply),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    #[test]
    fn lmove_happy_path_moves_one_element() {
        let mut store = test_store();
        cmd_rpush(
            &mut store,
            0,
            NOW,
            &req(&[b"RPUSH", b"src", b"a", b"b", b"c"]),
        );
        // LMOVE src dst LEFT RIGHT: pop 'a' off the left of src, push to right of dst.
        let reply = cmd_lmove(
            &mut store,
            0,
            NOW,
            &req(&[b"LMOVE", b"src", b"dst", b"LEFT", b"RIGHT"]),
        );
        assert_eq!(reply, Value::BulkString(Some(Bytes::from_static(b"a"))));
        assert_eq!(
            contents(&mut store, b"src"),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(contents(&mut store, b"dst"), vec![b"a".to_vec()]);
    }

    // ---- Fix 3: LPOS specific option error strings. ----

    #[test]
    fn lpos_rank_zero_is_specific_error() {
        let mut store = test_store();
        cmd_rpush(&mut store, 0, NOW, &req(&[b"RPUSH", b"k", b"a"]));
        let reply = cmd_lpos(
            &mut store,
            0,
            NOW,
            &req(&[b"LPOS", b"k", b"a", b"RANK", b"0"]),
        );
        assert_eq!(
            err_line(&reply),
            "-ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"
        );
    }

    #[test]
    fn lpos_negative_count_is_specific_error() {
        let mut store = test_store();
        cmd_rpush(&mut store, 0, NOW, &req(&[b"RPUSH", b"k", b"a"]));
        let reply = cmd_lpos(
            &mut store,
            0,
            NOW,
            &req(&[b"LPOS", b"k", b"a", b"COUNT", b"-1"]),
        );
        assert_eq!(err_line(&reply), "-ERR COUNT can't be negative");
    }

    #[test]
    fn lpos_negative_maxlen_is_specific_error() {
        let mut store = test_store();
        cmd_rpush(&mut store, 0, NOW, &req(&[b"RPUSH", b"k", b"a"]));
        let reply = cmd_lpos(
            &mut store,
            0,
            NOW,
            &req(&[b"LPOS", b"k", b"a", b"MAXLEN", b"-1"]),
        );
        assert_eq!(err_line(&reply), "-ERR MAXLEN can't be negative");
    }

    #[test]
    fn lpos_non_integer_option_is_not_an_integer() {
        // A NON-integer COUNT/RANK/MAXLEN is still the generic not-an-integer error,
        // DISTINCT from the negative/zero specific replies.
        let mut store = test_store();
        cmd_rpush(&mut store, 0, NOW, &req(&[b"RPUSH", b"k", b"a"]));
        let reply = cmd_lpos(
            &mut store,
            0,
            NOW,
            &req(&[b"LPOS", b"k", b"a", b"COUNT", b"xx"]),
        );
        assert_eq!(
            err_line(&reply),
            "-ERR value is not an integer or out of range"
        );
    }

    // ---- Fix 5: list integer args use the strict (string2ll) parser. ----

    #[test]
    fn lindex_rejects_leading_zero_index() {
        // Redis parses list index args via string2ll, which rejects leading zeros: a
        // `007` index is the not-an-integer error, not a parse-to-7.
        let mut store = test_store();
        cmd_rpush(
            &mut store,
            0,
            NOW,
            &req(&[b"RPUSH", b"k", b"a", b"b", b"c"]),
        );
        let reply = cmd_lindex(&mut store, 0, NOW, &req(&[b"LINDEX", b"k", b"007"]));
        assert_eq!(
            err_line(&reply),
            "-ERR value is not an integer or out of range"
        );
        // A canonical index still works.
        let ok = cmd_lindex(&mut store, 0, NOW, &req(&[b"LINDEX", b"k", b"1"]));
        assert_eq!(ok, Value::BulkString(Some(Bytes::from_static(b"b"))));
    }

    #[test]
    fn lpop_rejects_leading_plus_count() {
        // string2ll rejects a leading '+': `+2` is not-an-integer, not parse-to-2.
        let mut store = test_store();
        cmd_rpush(&mut store, 0, NOW, &req(&[b"RPUSH", b"k", b"a", b"b"]));
        let reply = cmd_lpop(&mut store, 0, NOW, &req(&[b"LPOP", b"k", b"+2"]));
        assert_eq!(
            err_line(&reply),
            "-ERR value is not an integer or out of range"
        );
    }
}
