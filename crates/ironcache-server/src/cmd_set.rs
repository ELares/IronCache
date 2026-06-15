// SPDX-License-Identifier: MIT OR Apache-2.0
//! Set-type command handlers over the storage waist (COMMANDS.md set semantics,
//! COLLECTIONS.md / OBJECT_ENCODING_MAPPING.md #40, the in-place-mutation RMW
//! extension, PR-7).
//!
//! Every MUTATING set command routes through [`Store::rmw_mut`] (the collection
//! in-place-mutation arm): the closure edits the stored set through the typed
//! [`SetValue`] view on [`RmwEntry::OccupiedMut`] and returns [`RmwAction::Mutated`]
//! (the store measures the byte delta, recomputes the encoding, and deletes the key if
//! the edit emptied the set), or [`RmwAction::Insert`] to create the set on a missing
//! key (SADD/SMOVE-into-dst/*STORE on a vacant key), or [`RmwAction::Delete`] when the
//! handler knows the post-edit count is zero (e.g. SREM/SPOP that drains the last
//! member). READ-ONLY set commands (SMEMBERS/SISMEMBER/SMISMEMBER/SCARD/SRANDMEMBER/
//! SSCAN, and the source reads of SINTER/SUNION/SDIFF) also go through `rmw_mut` with
//! [`RmwAction::Keep`] (no write, no accounting change): the typed view is the only way
//! to read set contents through the waist, and `Keep` leaves the value untouched.
//!
//! WRONGTYPE is checked before any mutation: a set command on a non-set key returns
//! `-WRONGTYPE` with no write (the typed [`OccupiedEntryMut::as_set_mut`] returns `None`
//! for a non-set, and the handler maps that to WRONGTYPE + `Keep`). A MISSING key is
//! treated as an EMPTY set for the read/algebra commands (SISMEMBER/SCARD/SMEMBERS/
//! SINTER/SUNION/SDIFF), matching Redis.
//!
//! A set is NEVER stored empty: when the last member is removed (SREM/SPOP to empty) the
//! key is deleted (the store's empty-collection-deletes-key backstop, plus the explicit
//! `Delete` action where the handler can tell). So an empty set is never observable,
//! matching Redis.
//!
//! ## SPOP / SRANDMEMBER determinism (ADR-0003)
//!
//! SPOP/SRANDMEMBER randomness enters through the Env RNG seam: the CALLER (dispatch)
//! draws a seed `u64` and passes it in (mirroring RANDOMKEY/HRANDFIELD); the store reads
//! no RNG. The handler derives its index choices deterministically from that one seed
//! (a splitmix64 step), so a seeded replay is byte-identical.
//!
//! ## SSCAN cursor (KEYSPACE.md)
//!
//! For a HASHTABLE-encoded set, SSCAN reuses the SAME hash-ordered cursor mechanism the
//! keyspace SCAN uses (`scan_plan`'s algorithm), applied to the set's OWN member table:
//! members are ordered by the fixed-seed stable member hash ([`member_scan_hash`], the
//! command-layer analog of the store's `scan_hash`), the cursor is the resume threshold
//! in that order, and an equal-hash group is never split. For a SMALL (intset/listpack)
//! set, SSCAN returns the WHOLE set in ONE reply with next-cursor `0`, IGNORING COUNT
//! (Redis small-collection SCAN). The handler distinguishes via [`SetValue::is_listpack`].
//!
//! ## Multi-key scope (single-shard-per-connection)
//!
//! The multi-key reads (SINTER/SUNION/SDIFF/SINTERCARD/SMISMEMBER's single key) and the
//! *STORE writes (SINTERSTORE/SUNIONSTORE/SDIFFSTORE) and SMOVE operate on the
//! connection's accept shard: the store IS this connection's whole keyspace (no
//! cross-shard fan-out exists yet, ADR-0011 single-node-first), so all the named keys
//! live on the one store. A true cross-shard set-algebra fan-out is deferred to the
//! coordinator (KEYSPACE.md), the same posture as the keyspace commands.
//!
//! [`SetValue::is_listpack`]: ironcache_storage::SetValue::is_listpack
//! [`OccupiedEntryMut::as_set_mut`]: ironcache_storage::OccupiedEntryMut::as_set_mut

use crate::cmd_util::{ascii_upper, parse_i64};
use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{
    ExpireWrite, NewValueOwned, RmwAction, RmwEntry, RmwStep, ScanCursor, Store, UnixMillis,
};
use std::collections::BTreeSet;

/// A no-write rmw step that returns `reply` (value untouched, TTL untouched). The shared
/// abort/short-circuit path for the set handlers (WRONGTYPE, read replies).
fn keep(reply: Value) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply,
    }
}

/// The WRONGTYPE no-write step (a set command against a non-set key).
fn wrong_type() -> RmwStep<Value> {
    keep(Value::error(ErrorReply::wrong_type()))
}

/// A bulk reply from owned bytes.
fn bulk(bytes: Vec<u8>) -> Value {
    Value::BulkString(Some(Bytes::from(bytes)))
}

/// Build a CREATE-path set value from `members` (SADD/*STORE on a missing key). The store
/// builds the concrete set via [`NewValueOwned::Set`] (dedup + the encoding ladder).
fn new_set(members: Vec<Vec<u8>>) -> NewValueOwned {
    NewValueOwned::set(members)
}

/// A deterministic splitmix64 PRNG seeded from the caller's Env-drawn seed (ADR-0003: no
/// std rand; the seed is the ONLY entropy and enters through the determinism seam). The
/// SET analog of the hash handler's `SeedRng`.
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
// SADD / SREM: variadic member add / remove. SADD creates on a missing key; SREM
// emptying the set deletes the key.
// ---------------------------------------------------------------------------

/// `SADD key member [member ...]` -> the number of NEW members added (already-present
/// members do NOT count). Creates the set on a missing key. WRONGTYPE on a non-set.
pub fn cmd_sadd<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("sadd"));
    }
    let members: Vec<Vec<u8>> = req.args[2..].iter().map(|b| b.to_vec()).collect();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => {
            // Create the set from the members. Count DISTINCT new members (a repeated
            // member in one SADD counts once, like Redis).
            let added = count_distinct(&members);
            RmwStep {
                action: RmwAction::Insert(new_set(members)),
                // A freshly created set has no TTL (Redis: a created key has no TTL).
                expire: ExpireWrite::Clear,
                reply: Value::Integer(added),
            }
        }
        RmwEntry::OccupiedMut(mut o) => {
            let Some(set) = o.as_set_mut() else {
                return wrong_type();
            };
            let mut added: i64 = 0;
            for m in &members {
                if set.add(m) {
                    added += 1;
                }
            }
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: Value::Integer(added),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// Count the DISTINCT members in a member list (for SADD's new-member count on a created
/// set: a repeated member counts once). O(n^2) over the (small) create list, fine for the
/// create path; the OccupiedMut path counts via `add` returns instead.
fn count_distinct(members: &[Vec<u8>]) -> i64 {
    let mut seen: Vec<&[u8]> = Vec::new();
    for m in members {
        if !seen.contains(&m.as_slice()) {
            seen.push(m);
        }
    }
    seen.len() as i64
}

/// `SREM key member [member ...]` -> the number of members actually removed. Removing the
/// last member deletes the key (empty-collection-deletes-key). 0 on a missing key;
/// WRONGTYPE on a non-set.
pub fn cmd_srem<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("srem"));
    }
    let members: Vec<Bytes> = req.args[2..].to_vec();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(set) = o.as_set_mut() else {
                return wrong_type();
            };
            let mut removed: i64 = 0;
            for m in &members {
                if set.remove(m) {
                    removed += 1;
                }
            }
            let action = if set.is_empty() {
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
// Read commands: SMEMBERS / SISMEMBER / SMISMEMBER / SCARD. They use rmw_mut with Keep
// (no write): the typed set view reads through the waist. A missing key reads as empty.
// ---------------------------------------------------------------------------

/// `SMEMBERS key` -> the array of members (empty if absent); WRONGTYPE on a non-set. The
/// order is the set's deterministic `members()` order (unspecified to clients, stable here).
pub fn cmd_smembers<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("smembers"));
    }
    store.rmw_mut(db, &req.args[1], now, |entry| match entry {
        RmwEntry::Vacant => keep(Value::Array(Some(Vec::new()))),
        RmwEntry::OccupiedMut(mut o) => match o.as_set_mut() {
            Some(set) => {
                let out = set.members().into_iter().map(bulk).collect();
                keep(Value::Array(Some(out)))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `SISMEMBER key member` -> 1 if the member is present, 0 otherwise (0 on a missing key);
/// WRONGTYPE on a non-set.
pub fn cmd_sismember<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("sismember"));
    }
    let member = req.args[2].clone();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => match o.as_set_mut() {
            Some(set) => keep(Value::Integer(i64::from(set.contains(&member)))),
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `SMISMEMBER key member [member ...]` -> an array of 1/0 per requested member (all 0 on
/// a missing key); WRONGTYPE on a non-set.
pub fn cmd_smismember<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("smismember"));
    }
    let members: Vec<Bytes> = req.args[2..].to_vec();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Array(Some(
            members.iter().map(|_| Value::Integer(0)).collect(),
        ))),
        RmwEntry::OccupiedMut(mut o) => match o.as_set_mut() {
            Some(set) => {
                let out: Vec<Value> = members
                    .iter()
                    .map(|m| Value::Integer(i64::from(set.contains(m))))
                    .collect();
                keep(Value::Array(Some(out)))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `SCARD key` -> the member count, 0 if absent; WRONGTYPE on a non-set.
pub fn cmd_scard<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("scard"));
    }
    store.rmw_mut(db, &req.args[1], now, |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => match o.as_set_mut() {
            Some(set) => keep(Value::Integer(set.len() as i64)),
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

// ---------------------------------------------------------------------------
// SPOP / SRANDMEMBER: random member selection. Caller draws the seed (Env RNG seam).
// SPOP removes; SRANDMEMBER does not.
// ---------------------------------------------------------------------------

/// `SPOP key [count]` -> one random member (no count) removed and returned (nil if the
/// key is absent), or up to `count` DISTINCT random members removed and returned as an
/// array. Emptying the set deletes the key. WRONGTYPE on a non-set. `seed` is drawn from
/// the Env RNG by dispatch (ADR-0003); the handler derives indices deterministically.
pub fn cmd_spop<S: Store>(
    store: &mut S,
    db: u32,
    seed: u64,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() < 2 || req.args.len() > 3 {
        return Value::error(ErrorReply::wrong_arity("spop"));
    }
    // The optional count: a negative count is the value-out-of-range error (SPOP, unlike
    // SRANDMEMBER, does not accept a negative count).
    let count: Option<i64> = if req.args.len() == 3 {
        match parse_i64(&req.args[2]) {
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
            // Missing key: nil (no count) or empty array (with count).
            keep(match count {
                None => Value::Null,
                Some(_) => Value::Array(Some(Vec::new())),
            })
        }
        RmwEntry::OccupiedMut(mut o) => {
            let Some(set) = o.as_set_mut() else {
                return wrong_type();
            };
            // Snapshot the deterministic member order, choose the members to pop, then
            // remove them through the same typed view.
            let members = set.members();
            let chosen = choose_distinct(&members, count, seed);
            for m in &chosen {
                set.remove(m);
            }
            let reply = match count {
                None => chosen.into_iter().next().map_or(Value::Null, bulk),
                Some(_) => Value::Array(Some(chosen.into_iter().map(bulk).collect())),
            };
            let action = if set.is_empty() {
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
    })
}

/// `SRANDMEMBER key [count]` -> one random member (no count: a bulk, nil if absent), or
/// `count` members: a POSITIVE count returns up to `min(count, card)` DISTINCT members; a
/// NEGATIVE count returns exactly `|count|` members WITH REPEATS. Does NOT remove.
/// WRONGTYPE on a non-set. `seed` from the Env RNG (ADR-0003).
pub fn cmd_srandmember<S: Store>(
    store: &mut S,
    db: u32,
    seed: u64,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() < 2 || req.args.len() > 3 {
        return Value::error(ErrorReply::wrong_arity("srandmember"));
    }
    let count: Option<i64> = if req.args.len() == 3 {
        match parse_i64(&req.args[2]) {
            Some(n) => Some(n),
            None => return Value::error(ErrorReply::not_an_integer()),
        }
    } else {
        None
    };

    store.rmw_mut(db, &req.args[1], now, move |entry| {
        let members = match entry {
            RmwEntry::Vacant => {
                return keep(match count {
                    None => Value::Null,
                    Some(_) => Value::Array(Some(Vec::new())),
                });
            }
            RmwEntry::OccupiedMut(mut o) => match o.as_set_mut() {
                Some(set) => set.members(),
                None => return wrong_type(),
            },
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        };
        keep(srandmember_reply(&members, count, seed))
    })
}

/// Choose DISTINCT members for SPOP from the deterministic `members` order: `None` ->
/// one member; `Some(c)` (c >= 0) -> up to `min(c, len)` distinct members. A partial
/// Fisher-Yates shuffle of the indices gives a uniform distinct sample deterministically
/// from `seed`.
fn choose_distinct(members: &[Vec<u8>], count: Option<i64>, seed: u64) -> Vec<Vec<u8>> {
    let n = members.len();
    if n == 0 {
        return Vec::new();
    }
    let mut rng = SeedRng::new(seed);
    let want = match count {
        None => 1,
        Some(c) => (c as usize).min(n),
    };
    let mut idxs: Vec<usize> = (0..n).collect();
    for i in 0..want {
        let j = i + (rng.next() % (n - i) as u64) as usize;
        idxs.swap(i, j);
    }
    idxs[..want].iter().map(|&i| members[i].clone()).collect()
}

/// Build the SRANDMEMBER reply from the deterministic `members` order, the parsed `count`,
/// and the caller-drawn `seed`. No count -> one bulk; positive count -> DISTINCT; negative
/// count -> WITH REPEATS, exactly `|count|`.
fn srandmember_reply(members: &[Vec<u8>], count: Option<i64>, seed: u64) -> Value {
    let n = members.len();
    if n == 0 {
        return match count {
            None => Value::Null,
            Some(_) => Value::Array(Some(Vec::new())),
        };
    }
    let mut rng = SeedRng::new(seed);
    match count {
        None => bulk(members[(rng.next() % n as u64) as usize].clone()),
        Some(c) if c >= 0 => {
            let chosen = choose_distinct(members, Some(c), seed);
            Value::Array(Some(chosen.into_iter().map(bulk).collect()))
        }
        Some(c) => {
            // Negative: WITH REPEATS, exactly |count| members (each drawn independently).
            let want = c.unsigned_abs() as usize;
            let out: Vec<Value> = (0..want)
                .map(|_| bulk(members[(rng.next() % n as u64) as usize].clone()))
                .collect();
            Value::Array(Some(out))
        }
    }
}

// ---------------------------------------------------------------------------
// SMOVE: move a member from src to dst (same-shard). Removing the last member deletes
// src; creates dst. WRONGTYPE if either is a non-set.
// ---------------------------------------------------------------------------

/// `SMOVE source destination member` -> 1 if the member was moved, 0 if it was not a
/// member of source. WRONGTYPE if either key holds a non-set. Removing the last member
/// from source deletes source; the member is added to destination (created if absent). If
/// the member is already in destination, it is just removed from source (Redis parity).
/// SAME-SHARD only.
pub fn cmd_smove<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("smove"));
    }
    let src = req.args[1].clone();
    let dst = req.args[2].clone();
    let member = req.args[3].clone();

    // (1) Type-check BOTH keys first (Redis checks src and dst types before any edit). A
    // non-set src or dst is WRONGTYPE with no change. A missing key is fine (treated as an
    // empty set: src-missing -> member absent -> 0; dst-missing -> created on add).
    if let Some(reply) = wrongtype_if_non_set(store, db, now, &src) {
        return reply;
    }
    if let Some(reply) = wrongtype_if_non_set(store, db, now, &dst) {
        return reply;
    }

    // src == dst: SMOVE of a member to its own set is a no-op success iff present.
    if src == dst {
        return store.rmw_mut(db, &src, now, move |entry| match entry {
            RmwEntry::Vacant => keep(Value::Integer(0)),
            RmwEntry::OccupiedMut(mut o) => match o.as_set_mut() {
                Some(set) => keep(Value::Integer(i64::from(set.contains(&member)))),
                None => wrong_type(),
            },
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        });
    }

    // (2) Remove the member from src. If it was not present, reply 0 WITHOUT touching dst.
    let m = member.clone();
    let removed = store.rmw_mut(db, &src, now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(set) = o.as_set_mut() else {
                return wrong_type();
            };
            let was = set.remove(&m);
            if was {
                let action = if set.is_empty() {
                    RmwAction::Delete
                } else {
                    RmwAction::Mutated
                };
                RmwStep {
                    action,
                    expire: ExpireWrite::Unchanged,
                    reply: Value::Integer(1),
                }
            } else {
                keep(Value::Integer(0))
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    });
    if removed != Value::Integer(1) {
        // Not a member of src (or src was a non-set, already returned above) -> 0, dst
        // untouched.
        return removed;
    }

    // (3) Add the member to dst (create if absent). dst's type was validated above.
    let m2 = member.to_vec();
    store.rmw_mut(db, &dst, now, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(new_set(vec![m2])),
            expire: ExpireWrite::Clear,
            reply: (),
        },
        RmwEntry::OccupiedMut(mut o) => {
            if let Some(set) = o.as_set_mut() {
                set.add(&m2);
            }
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: (),
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    });
    Value::Integer(1)
}

/// If `key` holds a NON-set value, return `Some(WRONGTYPE)`; otherwise `None` (a set, or
/// absent). Used by SMOVE to type-check both keys before any edit. Routes through
/// `rmw_mut` + Keep so it makes no write.
fn wrongtype_if_non_set<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    key: &[u8],
) -> Option<Value> {
    let is_non_set = store.rmw_mut(db, key, now, |entry| match entry {
        RmwEntry::Vacant => keep_bool(false),
        RmwEntry::OccupiedMut(mut o) => keep_bool(o.as_set_mut().is_none()),
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    });
    if is_non_set {
        Some(Value::error(ErrorReply::wrong_type()))
    } else {
        None
    }
}

/// A no-write rmw step returning a `bool` reply (used by the type-probe helpers).
fn keep_bool(reply: bool) -> RmwStep<bool> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply,
    }
}

// ---------------------------------------------------------------------------
// Set algebra: SINTER / SUNION / SDIFF (variadic keys -> array) and SINTERCARD
// (numkeys key... [LIMIT n]). A missing source = empty set.
// ---------------------------------------------------------------------------

/// Read the members of `key` as a snapshot, or signal a WRONGTYPE. Returns `Ok(members)`
/// for a set or a missing key (an empty Vec for missing), `Err(())` for a non-set. Routes
/// through `rmw_mut` + Keep (no write). The single source-read used by the set-algebra and
/// *STORE commands.
fn read_members<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    key: &[u8],
) -> Result<Vec<Vec<u8>>, ()> {
    store.rmw_mut(db, key, now, |entry| {
        let r: Result<Vec<Vec<u8>>, ()> = match entry {
            RmwEntry::Vacant => Ok(Vec::new()),
            RmwEntry::OccupiedMut(mut o) => match o.as_set_mut() {
                Some(set) => Ok(set.members()),
                None => Err(()),
            },
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        };
        RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: r,
        }
    })
}

/// The set-algebra operation requested.
#[derive(Clone, Copy)]
enum SetOp {
    Inter,
    Union,
    Diff,
}

/// Compute the result of a set-algebra op over the source keys, in a DETERMINISTIC order:
/// `Ok(members)` (the sorted result) or `Err(())` if any source is a non-set. A missing
/// source key is an EMPTY set: SINTER with any missing/empty key is the empty result;
/// SUNION/SDIFF skip a missing key (SDIFF subtracts nothing for it). The result is sorted
/// (the algebra uses a `BTreeSet`) so the reply / stored value is deterministic.
fn compute_algebra<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    keys: &[Bytes],
    op: SetOp,
) -> Result<Vec<Vec<u8>>, ()> {
    match op {
        SetOp::Inter => {
            // SINTER: read the FIRST key; if it is empty/missing the result is empty
            // (still type-checking the rest is Redis's behavior, but a short-circuit on an
            // empty first key after reading the rest matches the observable result; we read
            // all to surface a WRONGTYPE on any key, then intersect).
            let mut sets: Vec<BTreeSet<Vec<u8>>> = Vec::with_capacity(keys.len());
            for k in keys {
                let members = read_members(store, db, now, k)?;
                sets.push(members.into_iter().collect());
            }
            // Any empty source -> empty intersection.
            if sets.iter().any(BTreeSet::is_empty) {
                return Ok(Vec::new());
            }
            // Intersect, starting from the smallest set for efficiency.
            let start = sets
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| s.len())
                .map_or(0, |(i, _)| i);
            let result: BTreeSet<Vec<u8>> = sets[start]
                .iter()
                .filter(|m| sets.iter().all(|s| s.contains(*m)))
                .cloned()
                .collect();
            Ok(result.into_iter().collect())
        }
        SetOp::Union => {
            let mut result: BTreeSet<Vec<u8>> = BTreeSet::new();
            for k in keys {
                let members = read_members(store, db, now, k)?;
                result.extend(members);
            }
            Ok(result.into_iter().collect())
        }
        SetOp::Diff => {
            // SDIFF: the first set minus all the rest. A missing first key -> empty result.
            let first = read_members(store, db, now, &keys[0])?;
            let mut result: BTreeSet<Vec<u8>> = first.into_iter().collect();
            for k in &keys[1..] {
                let members = read_members(store, db, now, k)?;
                for m in members {
                    result.remove(&m);
                }
            }
            Ok(result.into_iter().collect())
        }
    }
}

/// `SINTER key [key ...]` / `SUNION key [key ...]` / `SDIFF key [key ...]` -> the result
/// set as an array (a missing source key = empty set). WRONGTYPE if any key is a non-set.
fn algebra_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    op: SetOp,
    cmd_name: &str,
) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let keys: Vec<Bytes> = req.args[1..].to_vec();
    match compute_algebra(store, db, now, &keys, op) {
        Ok(members) => Value::Array(Some(members.into_iter().map(bulk).collect())),
        Err(()) => Value::error(ErrorReply::wrong_type()),
    }
}

/// `SINTER key [key ...]` -> the intersection as an array.
pub fn cmd_sinter<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    algebra_generic(store, db, now, req, SetOp::Inter, "sinter")
}

/// `SUNION key [key ...]` -> the union as an array.
pub fn cmd_sunion<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    algebra_generic(store, db, now, req, SetOp::Union, "sunion")
}

/// `SDIFF key [key ...]` -> the difference (first minus the rest) as an array.
pub fn cmd_sdiff<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    algebra_generic(store, db, now, req, SetOp::Diff, "sdiff")
}

/// `SINTERCARD numkeys key [key ...] [LIMIT limit]` -> the cardinality of the
/// intersection, capped at `limit` (0 = no limit). WRONGTYPE if any key is a non-set. A
/// missing source key = empty set (so any missing key -> 0). Redis 7.
pub fn cmd_sintercard<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    // SINTERCARD numkeys key [key ...] [LIMIT n]: at least numkeys + 1 key.
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("sintercard"));
    }
    let Some(numkeys) = parse_i64(&req.args[1]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    if numkeys <= 0 {
        return Value::error(ErrorReply::numkeys_should_be_positive());
    }
    let numkeys = numkeys as usize;
    // The keys occupy args[2 .. 2+numkeys]; a LIMIT tail may follow.
    if 2 + numkeys > req.args.len() {
        return Value::error(ErrorReply::numkeys_greater_than_args());
    }
    let keys: Vec<Bytes> = req.args[2..2 + numkeys].to_vec();
    // Parse the optional LIMIT tail.
    let mut limit: usize = 0; // 0 = no limit
    let mut i = 2 + numkeys;
    while i < req.args.len() {
        let opt = ascii_upper(&req.args[i]);
        match opt.as_slice() {
            b"LIMIT" => {
                if i + 1 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                match parse_i64(&req.args[i + 1]) {
                    Some(n) if n < 0 => {
                        return Value::error(ErrorReply::limit_cant_be_negative());
                    }
                    Some(n) => limit = n as usize,
                    None => return Value::error(ErrorReply::not_an_integer()),
                }
                i += 2;
            }
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    }

    match compute_algebra(store, db, now, &keys, SetOp::Inter) {
        Ok(members) => {
            let card = if limit == 0 {
                members.len()
            } else {
                members.len().min(limit)
            };
            Value::Integer(card as i64)
        }
        Err(()) => Value::error(ErrorReply::wrong_type()),
    }
}

// ---------------------------------------------------------------------------
// SINTERSTORE / SUNIONSTORE / SDIFFSTORE: store the result in dest, return cardinality.
// An empty result DELETES dest; dest is overwritten. WRONGTYPE if any source is a non-set.
// ---------------------------------------------------------------------------

/// Shared body for SINTERSTORE/SUNIONSTORE/SDIFFSTORE: compute the algebra over the source
/// keys, then upsert the result into `dest` (or DELETE dest if the result is empty -- Redis
/// deletes dest on an empty result), returning the cardinality. WRONGTYPE if any source is
/// a non-set (checked BEFORE the dest write, so a WRONGTYPE leaves dest untouched). dest is
/// OVERWRITTEN on a non-empty result (even if it was a non-set, matching Redis: the
/// destination type is not checked). SAME-SHARD only.
fn store_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    op: SetOp,
    cmd_name: &str,
) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let dest = req.args[1].clone();
    let keys: Vec<Bytes> = req.args[2..].to_vec();
    // A WRONGTYPE on any source leaves dest untouched (checked before the write).
    let Ok(members) = compute_algebra(store, db, now, &keys, op) else {
        return Value::error(ErrorReply::wrong_type());
    };
    let card = members.len() as i64;
    if members.is_empty() {
        // Redis deletes the destination key on an empty result.
        store.delete(db, &dest, now);
    } else {
        // Overwrite dest with the result set (build fresh via the create path, which
        // applies the encoding ladder). A blind upsert replaces any existing value /
        // type.
        store.rmw_mut(db, &dest, now, move |_entry| RmwStep {
            action: RmwAction::Insert(new_set(members)),
            // The destination of a *STORE has no TTL (Redis: STORE clears the dest TTL).
            expire: ExpireWrite::Clear,
            reply: (),
        });
    }
    Value::Integer(card)
}

/// `SINTERSTORE destination key [key ...]` -> the cardinality stored at destination.
pub fn cmd_sinterstore<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    store_generic(store, db, now, req, SetOp::Inter, "sinterstore")
}

/// `SUNIONSTORE destination key [key ...]` -> the cardinality stored at destination.
pub fn cmd_sunionstore<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    store_generic(store, db, now, req, SetOp::Union, "sunionstore")
}

/// `SDIFFSTORE destination key [key ...]` -> the cardinality stored at destination.
pub fn cmd_sdiffstore<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    store_generic(store, db, now, req, SetOp::Diff, "sdiffstore")
}

// ---------------------------------------------------------------------------
// SSCAN: cursored iteration over the set's member table.
// ---------------------------------------------------------------------------

/// The default COUNT hint for SSCAN when none is given (Redis SCAN-family default 10).
const SSCAN_DEFAULT_COUNT: usize = 10;

/// `SSCAN key cursor [MATCH pattern] [COUNT n]` -> the 2-element reply
/// `[next_cursor_bulkstring, [member, ...]]`. The cursor is the decimal wire token; `0`
/// starts and a returned `0` means complete. MATCH globs the member. A missing key is
/// `[0, []]`. WRONGTYPE on a non-set.
///
/// A SMALL (intset/listpack) set returns ALL members at once with cursor 0, ignoring COUNT
/// (Redis small-collection SCAN). A HASHTABLE-encoded set reuses the SAME hash-ordered
/// cursor mechanism the keyspace SCAN uses: members are ordered by the fixed-seed stable
/// [`member_scan_hash`], the cursor is the resume threshold in that order, and an
/// equal-hash group is never split.
pub fn cmd_sscan<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("sscan"));
    }
    let Some(cursor) = ScanCursor::from_token(&req.args[2]) else {
        return Value::error(ErrorReply::invalid_cursor());
    };
    // Parse the option tail: MATCH <pattern>, COUNT <n>.
    let mut pattern: Option<Bytes> = None;
    let mut count: usize = SSCAN_DEFAULT_COUNT;
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
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    }

    store.rmw_mut(db, &req.args[1], now, move |entry| {
        let (members, is_small) = match entry {
            RmwEntry::Vacant => {
                return keep(sscan_reply(ScanCursor::START, Vec::new()));
            }
            RmwEntry::OccupiedMut(mut o) => match o.as_set_mut() {
                Some(set) => (set.members(), set.is_listpack()),
                None => return wrong_type(),
            },
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        };
        let (next, batch) = sscan_step(&members, cursor, count, pattern.as_deref(), is_small);
        keep(sscan_reply(next, batch))
    })
}

/// One bounded SSCAN batch over the set's `members` in ascending [`member_scan_hash`]
/// order, starting after `cursor`, applying the MATCH glob. Returns the next cursor
/// (`ScanCursor(0)` = complete) and the kept members. The set analog of the keyspace
/// `scan_step` + `scan_plan` (the same cursor algorithm), at the command layer because the
/// command layer cannot name the concrete store's `scan_plan` (the layering contract).
///
/// `is_small` selects the Redis small-collection behavior: a SMALL (intset/listpack) set
/// returns the WHOLE set in ONE batch with next-cursor 0, IGNORING COUNT; a hashtable set
/// uses the COUNT-budgeted hash-ordered cursor below.
fn sscan_step(
    members: &[Vec<u8>],
    cursor: ScanCursor,
    count: usize,
    pattern: Option<&[u8]>,
    is_small: bool,
) -> (ScanCursor, Vec<Vec<u8>>) {
    // Small (intset/listpack) set: everything at once, cursor 0, COUNT ignored. A
    // non-START resume cursor on a small set yields nothing (the whole set was already
    // returned on the cursor-0 call).
    if is_small {
        if !cursor.is_start() {
            return (ScanCursor::START, Vec::new());
        }
        let kept: Vec<Vec<u8>> = members
            .iter()
            .filter(|m| pattern.is_none_or(|p| crate::glob::glob_match(p, m)))
            .cloned()
            .collect();
        return (ScanCursor::START, kept);
    }
    // Build the sorted (member_hash, index) order. Sorting by (hash, member bytes) gives a
    // total order even for equal-hash members, identical run-to-run (ADR-0003).
    let mut order: Vec<(u64, usize)> = members
        .iter()
        .enumerate()
        .map(|(idx, m)| (member_scan_hash(m), idx))
        .collect();
    order.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| members[a.1].cmp(&members[b.1])));

    let total = order.len();
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
    let next = if i >= total {
        ScanCursor::START
    } else {
        ScanCursor(order[i].0)
    };
    let kept: Vec<Vec<u8>> = examined
        .into_iter()
        .filter(|&idx| pattern.is_none_or(|p| crate::glob::glob_match(p, &members[idx])))
        .map(|idx| members[idx].clone())
        .collect();
    (next, kept)
}

/// Build the SSCAN reply `[cursor, [member, ...]]`.
fn sscan_reply(next: ScanCursor, batch: Vec<Vec<u8>>) -> Value {
    let items: Vec<Value> = batch.into_iter().map(bulk).collect();
    Value::Array(Some(vec![
        Value::bulk(next.to_token().into_bytes()),
        Value::Array(Some(items)),
    ]))
}

/// The fixed-seed stable hash that orders a set's MEMBERS for SSCAN (the command-layer
/// analog of the store's `scan_hash`, KEYSPACE.md "the same hash-ordered cursor within the
/// collection"). A small wyhash/FNV-style mix over the member bytes, fully determined by
/// the bytes (no table state, no OS entropy, ADR-0003): recomputable across calls and
/// processes, so the SSCAN order is stable and resize-invariant. Kept here (NOT imported
/// from `ironcache-store`) because the command layer names only the storage waist, never
/// the concrete store (the layering contract); identical construction to the store's
/// `scan_hash` and the hash handler's `field_scan_hash`.
fn member_scan_hash(member: &[u8]) -> u64 {
    const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
    const SECRET: u64 = 0xA076_1D64_78BD_642F;
    let mut h: u64 = SEED ^ SECRET;
    for &b in member {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
        h ^= h >> 33;
    }
    h = h.wrapping_add(0x9E37_79B9_7F4A_7C15);
    h = (h ^ (h >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^ (h >> 31)
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

    fn int(v: &Value) -> i64 {
        match v {
            Value::Integer(n) => *n,
            other => panic!("expected an integer, got {other:?}"),
        }
    }

    fn err_line(v: &Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    fn bulk_bytes(v: &Value) -> Option<Vec<u8>> {
        match v {
            Value::BulkString(Some(b)) => Some(b.to_vec()),
            Value::Null => None,
            other => panic!("expected a bulk or nil, got {other:?}"),
        }
    }

    /// Sorted members from an array reply.
    fn sorted_array(v: &Value) -> Vec<Vec<u8>> {
        match v {
            Value::Array(Some(items)) => {
                let mut out: Vec<Vec<u8>> = items
                    .iter()
                    .map(|i| match i {
                        Value::BulkString(Some(b)) => b.to_vec(),
                        other => panic!("non-bulk in array: {other:?}"),
                    })
                    .collect();
                out.sort();
                out
            }
            other => panic!("expected an array, got {other:?}"),
        }
    }

    // ---- SADD: new-member count, dedup, TYPE, WRONGTYPE. ----

    #[test]
    fn sadd_counts_only_new_members_and_dedups() {
        let mut s = test_store();
        assert_eq!(
            int(&cmd_sadd(
                &mut s,
                0,
                NOW,
                &req(&[b"SADD", b"k", b"a", b"b", b"a"])
            )),
            2,
            "a repeated member in one SADD counts once"
        );
        // One existing, one new -> 1.
        assert_eq!(
            int(&cmd_sadd(
                &mut s,
                0,
                NOW,
                &req(&[b"SADD", b"k", b"a", b"c"])
            )),
            1
        );
        assert_eq!(int(&cmd_scard(&mut s, 0, NOW, &req(&[b"SCARD", b"k"]))), 3);
        assert_eq!(s.type_of(0, b"k", NOW), Some(DataType::Set));
    }

    #[test]
    fn sadd_wrongtype_on_a_string_key() {
        let mut s = test_store();
        s.upsert(
            0,
            b"str",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        assert_eq!(
            err_line(&cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"str", b"a"]))),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        // The string is untouched.
        assert_eq!(s.read(0, b"str", NOW).unwrap().as_bytes(), b"v");
    }

    // ---- SREM + empty-deletes-key. ----

    #[test]
    fn srem_variadic_and_empty_deletes_key() {
        let mut s = test_store();
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"k", b"a", b"b", b"c"]));
        assert_eq!(
            int(&cmd_srem(
                &mut s,
                0,
                NOW,
                &req(&[b"SREM", b"k", b"a", b"b", b"zzz"])
            )),
            2
        );
        assert_eq!(int(&cmd_scard(&mut s, 0, NOW, &req(&[b"SCARD", b"k"]))), 1);
        // Remove the last member -> key gone.
        assert_eq!(
            int(&cmd_srem(&mut s, 0, NOW, &req(&[b"SREM", b"k", b"c"]))),
            1
        );
        assert!(
            !s.contains(0, b"k", NOW),
            "emptying the set deletes the key"
        );
        assert_eq!(s.used_memory(), 0);
        // SREM on a missing key -> 0.
        assert_eq!(
            int(&cmd_srem(&mut s, 0, NOW, &req(&[b"SREM", b"k", b"a"]))),
            0
        );
    }

    // ---- SISMEMBER / SMISMEMBER. ----

    #[test]
    fn sismember_and_smismember() {
        let mut s = test_store();
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"k", b"a", b"b"]));
        assert_eq!(
            int(&cmd_sismember(
                &mut s,
                0,
                NOW,
                &req(&[b"SISMEMBER", b"k", b"a"])
            )),
            1
        );
        assert_eq!(
            int(&cmd_sismember(
                &mut s,
                0,
                NOW,
                &req(&[b"SISMEMBER", b"k", b"z"])
            )),
            0
        );
        // Missing key -> 0.
        assert_eq!(
            int(&cmd_sismember(
                &mut s,
                0,
                NOW,
                &req(&[b"SISMEMBER", b"nope", b"a"])
            )),
            0
        );
        // SMISMEMBER: per-member 1/0.
        match cmd_smismember(
            &mut s,
            0,
            NOW,
            &req(&[b"SMISMEMBER", b"k", b"a", b"z", b"b"]),
        ) {
            Value::Array(Some(items)) => {
                assert_eq!(
                    items,
                    vec![Value::Integer(1), Value::Integer(0), Value::Integer(1)]
                );
            }
            other => panic!("SMISMEMBER not an array: {other:?}"),
        }
        // Missing key -> all 0.
        assert_eq!(
            cmd_smismember(&mut s, 0, NOW, &req(&[b"SMISMEMBER", b"nope", b"a", b"b"])),
            Value::Array(Some(vec![Value::Integer(0), Value::Integer(0)]))
        );
    }

    // ---- SPOP: single, count, nil, empty-deletes. ----

    #[test]
    fn spop_single_count_nil_and_empty_deletes() {
        let mut s = test_store();
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"k", b"a", b"b", b"c"]));
        // SPOP (no count) returns one member that WAS in the set and removes it.
        let popped = bulk_bytes(&cmd_spop(&mut s, 0, SEED, NOW, &req(&[b"SPOP", b"k"]))).unwrap();
        assert!([b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].contains(&popped));
        assert_eq!(int(&cmd_scard(&mut s, 0, NOW, &req(&[b"SCARD", b"k"]))), 2);
        // SPOP key 5 (count > card) returns the remaining 2 DISTINCT members and deletes
        // the key (drained).
        let rest = sorted_array(&cmd_spop(
            &mut s,
            0,
            SEED,
            NOW,
            &req(&[b"SPOP", b"k", b"5"]),
        ));
        assert_eq!(rest.len(), 2);
        assert!(
            !s.contains(0, b"k", NOW),
            "SPOP that drains deletes the key"
        );
        // SPOP on a missing key: nil (no count) / empty array (with count).
        assert_eq!(
            cmd_spop(&mut s, 0, SEED, NOW, &req(&[b"SPOP", b"k"])),
            Value::Null
        );
        assert_eq!(
            cmd_spop(&mut s, 0, SEED, NOW, &req(&[b"SPOP", b"k", b"3"])),
            Value::Array(Some(Vec::new()))
        );
        // SPOP with a negative count is the value-out-of-range error.
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"k2", b"x"]));
        assert_eq!(
            err_line(&cmd_spop(
                &mut s,
                0,
                SEED,
                NOW,
                &req(&[b"SPOP", b"k2", b"-1"])
            )),
            "-ERR value is out of range, must be positive"
        );
    }

    // ---- SRANDMEMBER: +count distinct, -count repeats, nil. ----

    #[test]
    fn srandmember_distinct_repeats_and_nil() {
        let mut s = test_store();
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"k", b"a", b"b", b"c"]));
        // No count -> one bulk; the set is NOT modified.
        let one = bulk_bytes(&cmd_srandmember(
            &mut s,
            0,
            SEED,
            NOW,
            &req(&[b"SRANDMEMBER", b"k"]),
        ));
        assert!(one.is_some());
        assert_eq!(
            int(&cmd_scard(&mut s, 0, NOW, &req(&[b"SCARD", b"k"]))),
            3,
            "no removal"
        );
        // +count: DISTINCT, up to card. count 10 > card 3 -> exactly 3 distinct.
        let pos = sorted_array(&cmd_srandmember(
            &mut s,
            0,
            SEED,
            NOW,
            &req(&[b"SRANDMEMBER", b"k", b"10"]),
        ));
        assert_eq!(pos, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        // -count: WITH REPEATS, exactly |count| (may repeat; length is exact).
        match cmd_srandmember(&mut s, 0, SEED, NOW, &req(&[b"SRANDMEMBER", b"k", b"-5"])) {
            Value::Array(Some(items)) => assert_eq!(items.len(), 5),
            other => panic!("not an array: {other:?}"),
        }
        // Missing key: nil (no count) / empty array (with count).
        assert_eq!(
            cmd_srandmember(&mut s, 0, SEED, NOW, &req(&[b"SRANDMEMBER", b"nope"])),
            Value::Null
        );
        assert_eq!(
            cmd_srandmember(&mut s, 0, SEED, NOW, &req(&[b"SRANDMEMBER", b"nope", b"3"])),
            Value::Array(Some(Vec::new()))
        );
    }

    #[test]
    fn spop_and_srandmember_are_deterministic_under_a_fixed_seed() {
        // ADR-0003: a fixed seed yields the same selection on a fresh store with the same
        // contents. SRANDMEMBER does not mutate, so two calls with the same seed match.
        let mut s = test_store();
        cmd_sadd(
            &mut s,
            0,
            NOW,
            &req(&[b"SADD", b"k", b"a", b"b", b"c", b"d", b"e"]),
        );
        let a = cmd_srandmember(&mut s, 0, SEED, NOW, &req(&[b"SRANDMEMBER", b"k", b"3"]));
        let b = cmd_srandmember(&mut s, 0, SEED, NOW, &req(&[b"SRANDMEMBER", b"k", b"3"]));
        assert_eq!(a, b, "same seed + same contents -> identical SRANDMEMBER");
    }

    // ---- SMOVE: moved / not-member / WRONGTYPE / last-deletes-src / creates-dst. ----

    #[test]
    fn smove_moved_notmember_wrongtype_and_edges() {
        let mut s = test_store();
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"src", b"a", b"b"]));
        // Move a present member -> 1; dst created.
        assert_eq!(
            int(&cmd_smove(
                &mut s,
                0,
                NOW,
                &req(&[b"SMOVE", b"src", b"dst", b"a"])
            )),
            1
        );
        assert_eq!(
            int(&cmd_sismember(
                &mut s,
                0,
                NOW,
                &req(&[b"SISMEMBER", b"src", b"a"])
            )),
            0
        );
        assert_eq!(
            int(&cmd_sismember(
                &mut s,
                0,
                NOW,
                &req(&[b"SISMEMBER", b"dst", b"a"])
            )),
            1
        );
        // Move a NON-member -> 0, dst untouched.
        assert_eq!(
            int(&cmd_smove(
                &mut s,
                0,
                NOW,
                &req(&[b"SMOVE", b"src", b"dst", b"zzz"])
            )),
            0
        );
        // Moving the LAST member from src deletes src.
        assert_eq!(
            int(&cmd_smove(
                &mut s,
                0,
                NOW,
                &req(&[b"SMOVE", b"src", b"dst", b"b"])
            )),
            1
        );
        assert!(!s.contains(0, b"src", NOW), "emptying src deletes it");
        // WRONGTYPE if either key is a non-set.
        s.upsert(
            0,
            b"str",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        assert_eq!(
            err_line(&cmd_smove(
                &mut s,
                0,
                NOW,
                &req(&[b"SMOVE", b"str", b"dst", b"a"])
            )),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"src2", b"m"]));
        assert_eq!(
            err_line(&cmd_smove(
                &mut s,
                0,
                NOW,
                &req(&[b"SMOVE", b"src2", b"str", b"m"])
            )),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        // A WRONGTYPE dst must NOT have removed the member from src.
        assert_eq!(
            int(&cmd_sismember(
                &mut s,
                0,
                NOW,
                &req(&[b"SISMEMBER", b"src2", b"m"])
            )),
            1
        );
    }

    // ---- SINTER / SUNION / SDIFF incl. missing-key-as-empty. ----

    #[test]
    fn set_algebra_with_missing_key_as_empty() {
        let mut s = test_store();
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"a", b"1", b"2", b"3"]));
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"b", b"2", b"3", b"4"]));
        // SINTER.
        assert_eq!(
            sorted_array(&cmd_sinter(&mut s, 0, NOW, &req(&[b"SINTER", b"a", b"b"]))),
            vec![b"2".to_vec(), b"3".to_vec()]
        );
        // SINTER with a missing key -> empty (missing = empty set).
        assert_eq!(
            cmd_sinter(&mut s, 0, NOW, &req(&[b"SINTER", b"a", b"missing"])),
            Value::Array(Some(Vec::new()))
        );
        // SUNION (missing key skipped).
        assert_eq!(
            sorted_array(&cmd_sunion(
                &mut s,
                0,
                NOW,
                &req(&[b"SUNION", b"a", b"b", b"missing"])
            )),
            vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec(), b"4".to_vec()]
        );
        // SDIFF: a minus b -> {1}.
        assert_eq!(
            sorted_array(&cmd_sdiff(&mut s, 0, NOW, &req(&[b"SDIFF", b"a", b"b"]))),
            vec![b"1".to_vec()]
        );
        // SDIFF with a missing first key -> empty.
        assert_eq!(
            cmd_sdiff(&mut s, 0, NOW, &req(&[b"SDIFF", b"missing", b"a"])),
            Value::Array(Some(Vec::new()))
        );
        // WRONGTYPE if any source is a non-set.
        s.upsert(
            0,
            b"str",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        assert_eq!(
            err_line(&cmd_sinter(
                &mut s,
                0,
                NOW,
                &req(&[b"SINTER", b"a", b"str"])
            )),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    // ---- SINTERCARD + LIMIT. ----

    #[test]
    fn sintercard_and_limit() {
        let mut s = test_store();
        cmd_sadd(
            &mut s,
            0,
            NOW,
            &req(&[b"SADD", b"a", b"1", b"2", b"3", b"4"]),
        );
        cmd_sadd(
            &mut s,
            0,
            NOW,
            &req(&[b"SADD", b"b", b"2", b"3", b"4", b"5"]),
        );
        // Intersection {2,3,4} -> 3.
        assert_eq!(
            int(&cmd_sintercard(
                &mut s,
                0,
                NOW,
                &req(&[b"SINTERCARD", b"2", b"a", b"b"])
            )),
            3
        );
        // LIMIT caps the cardinality.
        assert_eq!(
            int(&cmd_sintercard(
                &mut s,
                0,
                NOW,
                &req(&[b"SINTERCARD", b"2", b"a", b"b", b"LIMIT", b"2"])
            )),
            2
        );
        // LIMIT 0 = no limit.
        assert_eq!(
            int(&cmd_sintercard(
                &mut s,
                0,
                NOW,
                &req(&[b"SINTERCARD", b"2", b"a", b"b", b"LIMIT", b"0"])
            )),
            3
        );
        // numkeys <= 0.
        assert_eq!(
            err_line(&cmd_sintercard(
                &mut s,
                0,
                NOW,
                &req(&[b"SINTERCARD", b"0", b"a"])
            )),
            "-ERR numkeys should be greater than 0"
        );
        // numkeys > supplied keys.
        assert_eq!(
            err_line(&cmd_sintercard(
                &mut s,
                0,
                NOW,
                &req(&[b"SINTERCARD", b"3", b"a", b"b"])
            )),
            "-ERR Number of keys can't be greater than number of args"
        );
        // negative LIMIT.
        assert_eq!(
            err_line(&cmd_sintercard(
                &mut s,
                0,
                NOW,
                &req(&[b"SINTERCARD", b"2", b"a", b"b", b"LIMIT", b"-1"])
            )),
            "-ERR LIMIT can't be negative"
        );
    }

    // ---- SINTERSTORE / SUNIONSTORE / SDIFFSTORE incl. empty-result-deletes-dest. ----

    #[test]
    fn store_variants_cardinality_overwrite_and_empty_deletes_dest() {
        let mut s = test_store();
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"a", b"1", b"2", b"3"]));
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"b", b"2", b"3", b"4"]));
        // SINTERSTORE dest a b -> 2 stored.
        assert_eq!(
            int(&cmd_sinterstore(
                &mut s,
                0,
                NOW,
                &req(&[b"SINTERSTORE", b"dest", b"a", b"b"])
            )),
            2
        );
        assert_eq!(
            sorted_array(&cmd_smembers(&mut s, 0, NOW, &req(&[b"SMEMBERS", b"dest"]))),
            vec![b"2".to_vec(), b"3".to_vec()]
        );
        // SUNIONSTORE overwrites dest.
        assert_eq!(
            int(&cmd_sunionstore(
                &mut s,
                0,
                NOW,
                &req(&[b"SUNIONSTORE", b"dest", b"a", b"b"])
            )),
            4
        );
        assert_eq!(
            int(&cmd_scard(&mut s, 0, NOW, &req(&[b"SCARD", b"dest"]))),
            4
        );
        // SDIFFSTORE with an EMPTY result DELETES dest (a minus a = empty).
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"dest", b"keepme"]));
        assert_eq!(
            int(&cmd_sdiffstore(
                &mut s,
                0,
                NOW,
                &req(&[b"SDIFFSTORE", b"dest", b"a", b"a"])
            )),
            0
        );
        assert!(!s.contains(0, b"dest", NOW), "an empty result deletes dest");
        // *STORE WRONGTYPE on a non-set source leaves dest untouched.
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"dest2", b"x"]));
        s.upsert(
            0,
            b"str",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        assert_eq!(
            err_line(&cmd_sinterstore(
                &mut s,
                0,
                NOW,
                &req(&[b"SINTERSTORE", b"dest2", b"a", b"str"])
            )),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(
            int(&cmd_sismember(
                &mut s,
                0,
                NOW,
                &req(&[b"SISMEMBER", b"dest2", b"x"])
            )),
            1,
            "dest untouched on WRONGTYPE"
        );
    }

    // ---- SSCAN small-all-at-once + large-cursored + MATCH. ----

    #[test]
    fn sscan_small_all_at_once_and_match() {
        let mut s = test_store();
        cmd_sadd(
            &mut s,
            0,
            NOW,
            &req(&[b"SADD", b"k", b"apple", b"banana", b"avocado"]),
        );
        // A small (listpack) set returns ALL at cursor 0.
        let reply = cmd_sscan(&mut s, 0, NOW, &req(&[b"SSCAN", b"k", b"0"]));
        let (cursor, members) = parse_sscan(&reply);
        assert_eq!(cursor, "0", "small set completes in one batch");
        let mut m = members;
        m.sort();
        assert_eq!(
            m,
            vec![b"apple".to_vec(), b"avocado".to_vec(), b"banana".to_vec()]
        );
        // MATCH globs the member.
        let (_c, matched) = parse_sscan(&cmd_sscan(
            &mut s,
            0,
            NOW,
            &req(&[b"SSCAN", b"k", b"0", b"MATCH", b"a*"]),
        ));
        let mut matched = matched;
        matched.sort();
        assert_eq!(matched, vec![b"apple".to_vec(), b"avocado".to_vec()]);
        // Missing key -> [0, []].
        let (c, empty) = parse_sscan(&cmd_sscan(&mut s, 0, NOW, &req(&[b"SSCAN", b"nope", b"0"])));
        assert_eq!(c, "0");
        assert!(empty.is_empty());
    }

    #[test]
    fn sscan_large_cursored_visits_every_member_once() {
        let mut s = test_store();
        // Build a hashtable-encoded set (a long member forces hashtable).
        let big = vec![b'q'; 100];
        cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"k", &big]));
        for i in 0..300u32 {
            cmd_sadd(
                &mut s,
                0,
                NOW,
                &req(&[b"SADD", b"k", format!("m{i}").as_bytes()]),
            );
        }
        assert_eq!(
            s.read(0, b"k", NOW).unwrap().encoding().encoding_name(),
            "hashtable"
        );
        // Drive SSCAN to completion with a small COUNT; collect every member exactly once.
        let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut cursor = "0".to_string();
        for _ in 0..1000 {
            let reply = cmd_sscan(
                &mut s,
                0,
                NOW,
                &req(&[b"SSCAN", b"k", cursor.as_bytes(), b"COUNT", b"7"]),
            );
            let (next, members) = parse_sscan(&reply);
            for m in members {
                assert!(seen.insert(m), "a member was returned twice across SSCAN");
            }
            if next == "0" {
                break;
            }
            cursor = next;
        }
        assert_eq!(seen.len(), 301, "SSCAN visited every member exactly once");
    }

    /// Parse an SSCAN reply `[cursor_bulk, [member, ...]]` into (cursor, members).
    fn parse_sscan(v: &Value) -> (String, Vec<Vec<u8>>) {
        match v {
            Value::Array(Some(items)) => {
                assert_eq!(items.len(), 2, "SSCAN reply is a 2-tuple");
                let cursor = match &items[0] {
                    Value::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                    other => panic!("cursor not a bulk: {other:?}"),
                };
                let members = match &items[1] {
                    Value::Array(Some(ms)) => ms
                        .iter()
                        .map(|m| match m {
                            Value::BulkString(Some(b)) => b.to_vec(),
                            other => panic!("member not a bulk: {other:?}"),
                        })
                        .collect(),
                    other => panic!("members not an array: {other:?}"),
                };
                (cursor, members)
            }
            other => panic!("SSCAN reply not an array: {other:?}"),
        }
    }

    // ---- Arity. ----

    #[test]
    fn arity_errors() {
        let mut s = test_store();
        assert_eq!(
            err_line(&cmd_sadd(&mut s, 0, NOW, &req(&[b"SADD", b"k"]))),
            "-ERR wrong number of arguments for 'sadd' command"
        );
        assert_eq!(
            err_line(&cmd_smembers(&mut s, 0, NOW, &req(&[b"SMEMBERS"]))),
            "-ERR wrong number of arguments for 'smembers' command"
        );
        assert_eq!(
            err_line(&cmd_smove(&mut s, 0, NOW, &req(&[b"SMOVE", b"a", b"b"]))),
            "-ERR wrong number of arguments for 'smove' command"
        );
    }
}
