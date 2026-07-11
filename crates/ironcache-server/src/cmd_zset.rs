// SPDX-License-Identifier: MIT OR Apache-2.0
//! Sorted-set (ZSET) command handlers over the storage waist (COMMANDS.md zset
//! semantics, ZSET_LARGE.md, COLLECTIONS.md / OBJECT_ENCODING_MAPPING.md #40, the
//! in-place-mutation RMW extension, PR-8).
//!
//! Every MUTATING zset command routes through [`Store::rmw_mut`] (the collection
//! in-place-mutation arm): the closure edits the stored zset through the typed
//! [`ZSetValue`] view on [`RmwEntry::OccupiedMut`] and returns [`RmwAction::Mutated`]
//! (the store measures the byte delta, recomputes the encoding, and deletes the key if
//! the edit emptied the zset), or [`RmwAction::Insert`] to create the zset on a missing
//! key (ZADD/ZINCRBY/*STORE on a vacant key), or [`RmwAction::Delete`] when the handler
//! knows the post-edit count is zero (e.g. ZREM/ZPOPMIN that drains the last member).
//! READ-ONLY zset commands also go through `rmw_mut` with [`RmwAction::Keep`] (no write,
//! no accounting change): the typed view is the only way to read zset contents through
//! the waist, and `Keep` leaves the value untouched.
//!
//! WRONGTYPE is checked before any mutation: a zset command on a non-zset key returns
//! `-WRONGTYPE` with no write (the typed [`OccupiedEntryMut::as_zset_mut`] returns `None`
//! for a non-zset, and the handler maps that to WRONGTYPE + `Keep`). A MISSING key is
//! treated as an EMPTY zset for the read/range commands, matching Redis.
//!
//! A zset is NEVER stored empty: when the last member is removed (ZREM/ZPOPMIN/
//! ZREMRANGE* to empty) the key is deleted (the store's empty-collection-deletes-key
//! backstop, plus the explicit `Delete` action where the handler can tell). So an empty
//! zset is never observable, matching Redis.
//!
//! ## Ordering and WITHSCORES (ZSET_LARGE.md)
//!
//! Members order by (score ASC, member-bytes ASC for equal scores) [redis-zset-skiplist
//! -plus-ht]. A NaN INPUT score is rejected at parse time ([`parse_score`] /
//! [`cmd_util::parse_f64`] reject NaN); a NaN ARITHMETIC RESULT (ZINCRBY / ZADD INCR of an
//! existing `+inf` by `-inf`) is caught in the store ([`IncrOutcome::Nan`]) and surfaced as
//! `-ERR resulting score is not a number (NaN)` WITHOUT mutating; `+inf`/`-inf` are
//! allowed. A SCALAR score reply (ZSCORE / ZMSCORE / ZINCRBY / ZADD INCR) is a
//! [`Value::Double`] (RESP3 wire `,` with `inf`/`-inf`; a bulk string under RESP2), the
//! Redis reply type for these commands. The WITHSCORES nested shape is a [`Value::Double`]
//! inside a [`Value::Pairs`] (which nests each (member, score) pair under RESP3 and
//! flattens to `[member, score, ...]` under RESP2). ZSCAN scores STAY bulk strings in both
//! protocols (the SCAN family replies flat bulk arrays).
//!
//! ## ZRANDMEMBER determinism (ADR-0003)
//!
//! ZRANDMEMBER randomness enters through the Env RNG seam: the CALLER (dispatch) draws a
//! seed `u64` and passes it in (mirroring SRANDMEMBER/HRANDFIELD); the store reads no
//! RNG. The handler derives its index choices deterministically from that one seed (a
//! splitmix64 step), so a seeded replay is byte-identical.
//!
//! ## ZSCAN cursor (KEYSPACE.md)
//!
//! For a SKIPLIST-encoded zset, ZSCAN reuses the SAME hash-ordered cursor mechanism the
//! keyspace SCAN / SSCAN use, applied to the zset's members. For a SMALL (listpack) zset,
//! ZSCAN returns the WHOLE zset in ONE reply with next-cursor `0`, IGNORING COUNT (Redis
//! small-collection SCAN). ZSCAN interleaves member + score (like HSCAN's field+value).
//!
//! ## Multi-key scope (single-shard-per-connection)
//!
//! The aggregation reads/writes (ZUNION/ZINTER/ZDIFF + their *STORE forms, ZRANGESTORE,
//! ZINTERCARD) operate on the connection's accept shard: the store IS this connection's
//! whole keyspace (no cross-shard fan-out exists yet, ADR-0011), so all named keys live
//! on the one store. A true cross-shard fan-out is deferred to the coordinator.
//!
//! [`ZSetValue`]: ironcache_storage::ZSetValue
//! [`OccupiedEntryMut::as_zset_mut`]: ironcache_storage::OccupiedEntryMut::as_zset_mut
//! [`format_human_double`]: ironcache_protocol::format_human_double

use crate::cmd_util::{ascii_upper, parse_f64, parse_i64, parse_lex_bound, parse_score_bound};
use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value, format_human_double};
use ironcache_storage::{
    ExpireWrite, IncrOutcome, LexBound, NewValueOwned, RmwAction, RmwEntry, RmwStep, ScanCursor,
    ScoreBound, Store, UnixMillis, ZAddFlags,
};
use std::collections::BTreeMap;

/// A no-write rmw step that returns `reply` (value untouched, TTL untouched). The shared
/// abort/short-circuit path for the zset handlers (WRONGTYPE, read replies).
fn keep(reply: Value) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply,
    }
}

/// The WRONGTYPE no-write step (a zset command against a non-zset key).
fn wrong_type() -> RmwStep<Value> {
    keep(Value::error(ErrorReply::wrong_type()))
}

/// A bulk reply from owned bytes.
fn bulk(bytes: Vec<u8>) -> Value {
    Value::BulkString(Some(Bytes::from(bytes)))
}

/// A score reply as a bulk string in the Redis HUMAN spelling: no trailing zeros, no
/// scientific notation, `-0 -> 0`, `inf`/`-inf` for infinities. Used ONLY for ZSCAN, whose
/// scores stay flat bulk strings in BOTH protocols (the SCAN family replies flat bulk
/// arrays, never a RESP3 typed double). The SCALAR score replies (ZSCORE / ZMSCORE /
/// ZINCRBY / ZADD INCR) use [`score_double`] instead.
fn score_bulk(score: f64) -> Value {
    bulk(format_human_double(score).into_bytes())
}

/// A SCALAR score reply as a [`Value::Double`]: the Redis reply type for ZSCORE / ZMSCORE /
/// ZINCRBY / ZADD INCR. `Value::Double` encodes a RESP3 double (wire `,`, with `inf`/`-inf`
/// for the infinities) and degrades to a bulk string under RESP2, so a RESP2 client still
/// sees the human spelling while a RESP3 client gets the typed double Redis sends.
fn score_double(score: f64) -> Value {
    Value::Double(score)
}

/// Build the WITHSCORES reply from ordered `(member, score)` pairs: a [`Value::Pairs`] of
/// `(member-bulk, Value::Double(score))`. The encoder NESTS each pair under RESP3 (an
/// array of [member, ,double] 2-arrays) and FLATTENS to `[member, score, ...]` under
/// RESP2, matching Redis's WITHSCORES RESP2/RESP3 shapes. Without WITHSCORES the reply is
/// a plain array of member bulks.
fn members_reply(pairs: Vec<(Vec<u8>, f64)>, with_scores: bool) -> Value {
    if with_scores {
        let out: Vec<(Value, Value)> = pairs
            .into_iter()
            .map(|(m, s)| (bulk(m), Value::Double(s)))
            .collect();
        Value::Pairs(out)
    } else {
        Value::Array(Some(pairs.into_iter().map(|(m, _)| bulk(m)).collect()))
    }
}

/// A deterministic splitmix64 PRNG seeded from the caller's Env-drawn seed (ADR-0003: no
/// std rand; the seed is the ONLY entropy and enters through the determinism seam). The
/// ZSET analog of the set/hash handlers' `SeedRng`.
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

// ===========================================================================
// ZADD: the full NX/XX/GT/LT/CH/INCR matrix.
// ===========================================================================

/// `ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]` -> the integer
/// number of NEW members added (or, with CH, the number CHANGED = added + updated); with
/// INCR a bulk score (or nil if the op was suppressed by NX/XX/GT/LT). Creates the zset on
/// a missing key. WRONGTYPE on a non-zset. The GT/LT/NX conflict, NX+XX, the INCR
/// single-pair rule, and a bad score are byte-exact errors.
// `score`/`scores`-style bindings and the full flag/INCR/create/update matrix make this
// the longest of the zset handlers; the structure (flag parse -> validate -> pair parse
// -> rmw closure) is linear and clear, so the length/name lints are allowed here.
#[allow(clippy::too_many_lines, clippy::similar_names)]
pub fn cmd_zadd<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    // ZADD key [opts...] score member [score member ...]: at least key + one pair.
    if req.args.len() < 4 {
        return Value::error(ErrorReply::wrong_arity("zadd"));
    }
    // Parse the leading flag tokens (they precede the first score, which is numeric).
    let mut flags = ZAddFlags::default();
    let mut ch = false;
    let mut incr = false;
    let mut i = 2;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"NX" => flags.nx = true,
            b"XX" => flags.xx = true,
            b"GT" => flags.gt = true,
            b"LT" => flags.lt = true,
            b"CH" => ch = true,
            b"INCR" => incr = true,
            // The first non-flag token is the first score; stop flag scanning.
            _ => break,
        }
        i += 1;
    }
    // Validate the flag combinations (Redis order: NX+XX is a generic syntax error; the
    // GT/LT/NX trio is the dedicated incompatibility error).
    if flags.nx && flags.xx {
        return Value::error(ErrorReply::syntax_error());
    }
    if (flags.gt && flags.lt) || (flags.nx && (flags.gt || flags.lt)) {
        return Value::error(ErrorReply::zadd_gt_lt_nx_incompatible());
    }
    // The remaining args must be score/member pairs (an even count, at least one pair).
    let rest = &req.args[i..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return Value::error(ErrorReply::syntax_error());
    }
    let pair_count = rest.len() / 2;
    if incr && pair_count != 1 {
        return Value::error(ErrorReply::zadd_incr_single_pair());
    }
    // Parse every score up front (a bad score is an error BEFORE any mutation, matching
    // Redis which validates all scores first).
    let mut pairs: Vec<(f64, Vec<u8>)> = Vec::with_capacity(pair_count);
    let mut p = 0;
    while p < rest.len() {
        let Some(score) = parse_f64(&rest[p]) else {
            return Value::error(ErrorReply::zadd_score_not_a_float());
        };
        pairs.push((score, rest[p + 1].to_vec()));
        p += 2;
    }

    store.rmw_mut(db, &req.args[1], now, move |entry| {
        match entry {
            RmwEntry::Vacant => {
                // INCR on a missing key with XX is suppressed -> nil + no create.
                if incr {
                    let (score, member) = &pairs[0];
                    if flags.xx {
                        return keep(Value::Null);
                    }
                    // A NaN INCR delta was rejected at parse time, so the create score is
                    // never NaN; guard defensively so a NaN is never stored.
                    if score.is_nan() {
                        return keep(Value::error(ErrorReply::zadd_score_is_nan()));
                    }
                    // GT/LT on a missing member still ADD (they only gate UPDATES); NX adds.
                    return RmwStep {
                        action: RmwAction::Insert(NewValueOwned::zset(vec![(
                            member.clone(),
                            *score,
                        )])),
                        expire: ExpireWrite::Clear,
                        // INCR replies the new score as a Double (RESP3 `,`; RESP2 bulk).
                        reply: score_double(*score),
                    };
                }
                // Non-INCR create: apply XX (suppresses all adds -> empty, key not
                // created) and the per-pair flags. With XX no member is added, so reply 0
                // and do not create.
                if flags.xx {
                    return keep(Value::Integer(0));
                }
                // Build the created set by feeding the pairs through the SAME sequential
                // add()+gate logic the OccupiedMut branch uses (an empty starting zset),
                // so a repeated member with GT/LT/NX gates identically on the create path
                // (e.g. `ZADD k GT 2 m 1 m` -> m=2). A plain last-write-wins BTreeMap would
                // wrongly let the later `1` overwrite the gated `2`.
                let (created, added, changed) = resolve_create_pairs(&pairs, flags);
                if created.is_empty() {
                    // Every pair was gated out (e.g. all repeats failed GT/LT on members
                    // that themselves never landed): nothing to create.
                    return keep(Value::Integer(if ch { changed } else { added }));
                }
                RmwStep {
                    action: RmwAction::Insert(NewValueOwned::zset(created)),
                    expire: ExpireWrite::Clear,
                    reply: Value::Integer(if ch { changed } else { added }),
                }
            }
            RmwEntry::OccupiedMut(mut o) => {
                let th = o.thresholds();
                let Some(zset) = o.as_zset_mut() else {
                    return wrong_type();
                };
                if incr {
                    let (delta, member) = &pairs[0];
                    // A NaN RESULT (existing +inf incremented by -inf) returns the
                    // resulting-score-is-NaN error WITHOUT mutating (RmwAction::Keep, so the
                    // store records no delta and the member keeps its prior score).
                    return match zset.incr(member, *delta, flags, &th) {
                        IncrOutcome::Updated(new) => RmwStep {
                            action: RmwAction::Mutated,
                            expire: ExpireWrite::Unchanged,
                            reply: score_double(new),
                        },
                        IncrOutcome::Suppressed => RmwStep {
                            action: RmwAction::Mutated,
                            expire: ExpireWrite::Unchanged,
                            reply: Value::Null,
                        },
                        IncrOutcome::Nan => keep(Value::error(ErrorReply::zadd_score_is_nan())),
                    };
                }
                let mut added: i64 = 0;
                let mut changed: i64 = 0;
                for (score, member) in &pairs {
                    let out = zset.add(member, *score, flags, &th);
                    if out.added {
                        added += 1;
                    }
                    if out.changed {
                        changed += 1;
                    }
                }
                RmwStep {
                    action: RmwAction::Mutated,
                    expire: ExpireWrite::Unchanged,
                    reply: Value::Integer(if ch { changed } else { added }),
                }
            }
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        }
    })
}

/// Whether a score UPDATE from `cur` to `new` passes the GT/LT gate. Mirrors the store's
/// private `gate_passes` so the ZADD CREATE path gates a repeated member identically to the
/// in-place [`ZSetValue::add`] update path. GT permits the update only when `new` is
/// strictly greater than `cur`; LT only when strictly less; with neither set it always
/// passes. Positive comparisons (no negated partial-ord) so a non-finite score compares the
/// IEEE way (GT vs `+inf` never updates).
fn create_gate_passes(cur: f64, new: f64, flags: ZAddFlags) -> bool {
    if flags.gt {
        new > cur
    } else if flags.lt {
        new < cur
    } else {
        true
    }
}

/// Resolve the ZADD CREATE-path `(score, member)` pairs into the `(member, score)` set to
/// store, plus the `(added, changed)` counts, by feeding the pairs through the SAME
/// sequential add()+gate logic [`ZSetValue::add`] applies on an empty zset. This makes a
/// REPEATED member with GT/LT/NX gate identically on the create path: the first occurrence
/// of a member is a new add (GT/LT do NOT gate a brand-new member), and a later occurrence
/// is an UPDATE that NX suppresses and GT/LT gate against the running score. (A plain
/// last-write-wins map would let a later, gated-out score win instead.) Insertion order is
/// preserved (the store re-sorts into (score, member) order anyway). NaN was already
/// rejected at parse time on this path, so no NaN check is needed here.
#[allow(clippy::float_cmp)] // `score != cur` is the exact Redis CH "changed" check.
fn resolve_create_pairs(
    pairs: &[(f64, Vec<u8>)],
    flags: ZAddFlags,
) -> (Vec<(Vec<u8>, f64)>, i64, i64) {
    // (member, current-score) in first-insertion order; a linear scan matches the small
    // per-command pair count and keeps the order deterministic.
    let mut built: Vec<(Vec<u8>, f64)> = Vec::with_capacity(pairs.len());
    let mut added: i64 = 0;
    let mut changed: i64 = 0;
    for (score, member) in pairs {
        match built.iter_mut().find(|(m, _)| m == member) {
            None => {
                // Absent: XX suppresses adding a new member; GT/LT alone still ADD (they
                // only gate UPDATES), matching ZSetVal::add's vacant arm.
                if flags.xx {
                    continue;
                }
                built.push((member.clone(), *score));
                added += 1;
                changed += 1;
            }
            Some((_, cur)) => {
                // Present (a repeat within this same ZADD): NX suppresses; GT/LT gate.
                if flags.nx || !create_gate_passes(*cur, *score, flags) {
                    continue;
                }
                if *score != *cur {
                    *cur = *score;
                    changed += 1;
                }
            }
        }
    }
    (built, added, changed)
}

/// `ZINCRBY key increment member` -> the member's NEW score as a bulk string. Creates the
/// zset / the member (starting from 0) on a missing key/member. WRONGTYPE on a non-zset; a
/// bad increment is `not a valid float`.
pub fn cmd_zincrby<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("zincrby"));
    }
    let Some(delta) = parse_f64(&req.args[2]) else {
        return Value::error(ErrorReply::not_a_valid_float());
    };
    let member = req.args[3].to_vec();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        // A missing key/member starts from 0; the parse already rejected a NaN delta, so the
        // create score is never NaN. The new score replies as a Double (RESP3 `,`; RESP2 bulk).
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::zset(vec![(member.clone(), delta)])),
            expire: ExpireWrite::Clear,
            reply: score_double(delta),
        },
        RmwEntry::OccupiedMut(mut o) => {
            let th = o.thresholds();
            let Some(zset) = o.as_zset_mut() else {
                return wrong_type();
            };
            // ZINCRBY has no NX/XX/GT/LT, so it never suppresses; a NaN RESULT (an existing
            // +inf incremented by -inf) returns the resulting-score-is-NaN error WITHOUT
            // mutating (Keep, no accounting delta, member keeps its prior score).
            match zset.incr(&member, delta, ZAddFlags::default(), &th) {
                IncrOutcome::Updated(new) => RmwStep {
                    action: RmwAction::Mutated,
                    expire: ExpireWrite::Unchanged,
                    reply: score_double(new),
                },
                IncrOutcome::Nan => keep(Value::error(ErrorReply::zadd_score_is_nan())),
                IncrOutcome::Suppressed => {
                    unreachable!("default-flag incr never suppresses")
                }
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

// ===========================================================================
// Scalar reads: ZSCORE / ZMSCORE / ZCARD / ZRANK / ZREVRANK.
// ===========================================================================

/// `ZSCORE key member` -> the member's score as a bulk string, or nil if the key/member is
/// absent. WRONGTYPE on a non-zset.
pub fn cmd_zscore<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("zscore"));
    }
    let member = req.args[2].clone();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Null),
        RmwEntry::OccupiedMut(mut o) => match o.as_zset_mut() {
            // ZSCORE replies the score as a Double (RESP3 `,`; RESP2 bulk), nil if absent.
            Some(zset) => keep(zset.score(&member).map_or(Value::Null, score_double)),
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `ZMSCORE key member [member ...]` -> an array of score-or-nil per requested member (all
/// nil on a missing key). WRONGTYPE on a non-zset.
pub fn cmd_zmscore<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("zmscore"));
    }
    let members: Vec<Bytes> = req.args[2..].to_vec();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Array(Some(
            members.iter().map(|_| Value::Null).collect(),
        ))),
        RmwEntry::OccupiedMut(mut o) => match o.as_zset_mut() {
            Some(zset) => {
                // ZMSCORE replies each score as a Double (RESP3 `,`; RESP2 bulk), nil if
                // the member is absent.
                let out: Vec<Value> = members
                    .iter()
                    .map(|m| zset.score(m).map_or(Value::Null, score_double))
                    .collect();
                keep(Value::Array(Some(out)))
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `ZCARD key` -> the member count, 0 if absent. WRONGTYPE on a non-zset.
pub fn cmd_zcard<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("zcard"));
    }
    store.rmw_mut(db, &req.args[1], now, |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => match o.as_zset_mut() {
            Some(zset) => keep(Value::Integer(zset.len() as i64)),
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `ZRANK key member [WITHSCORE]` / `ZREVRANK key member [WITHSCORE]` -> the 0-based rank
/// (forward / reverse), or nil if the member/key is absent. With WITHSCORE (Redis 7.2) the
/// reply is `[rank, score]` (or nil). WRONGTYPE on a non-zset.
fn rank_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    rev: bool,
    cmd_name: &str,
) -> Value {
    if req.args.len() < 3 || req.args.len() > 4 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let with_score = if req.args.len() == 4 {
        if ascii_upper(&req.args[3]).as_slice() == b"WITHSCORE" {
            true
        } else {
            return Value::error(ErrorReply::syntax_error());
        }
    } else {
        false
    };
    let member = req.args[2].clone();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        // A missing key/member replies nil (Redis: a nil rank, no [rank, score] array
        // even with WITHSCORE).
        RmwEntry::Vacant => keep(Value::Null),
        RmwEntry::OccupiedMut(mut o) => match o.as_zset_mut() {
            Some(z) => {
                let r = z.rank(&member, rev);
                let s = z.score(&member);
                keep(match r {
                    None => Value::Null,
                    Some(rank) if with_score => Value::Array(Some(vec![
                        Value::Integer(rank as i64),
                        Value::Double(s.unwrap_or(0.0)),
                    ])),
                    Some(rank) => Value::Integer(rank as i64),
                })
            }
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `ZRANK key member [WITHSCORE]`.
pub fn cmd_zrank<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    rank_generic(store, db, now, req, false, "zrank")
}

/// `ZREVRANK key member [WITHSCORE]`.
pub fn cmd_zrevrank<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    rank_generic(store, db, now, req, true, "zrevrank")
}

// ===========================================================================
// ZREM and the ZREMRANGEBY* family.
// ===========================================================================

/// `ZREM key member [member ...]` -> the number removed. Removing the last member deletes
/// the key. 0 on a missing key; WRONGTYPE on a non-zset.
pub fn cmd_zrem<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("zrem"));
    }
    let members: Vec<Bytes> = req.args[2..].to_vec();
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(zset) = o.as_zset_mut() else {
                return wrong_type();
            };
            let mut removed: i64 = 0;
            for m in &members {
                if zset.remove(m) {
                    removed += 1;
                }
            }
            let action = if zset.is_empty() {
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

/// The kind of ZREMRANGEBY* range.
enum RemRange {
    Rank(i64, i64),
    Score(ScoreBound, ScoreBound),
    Lex(LexBound, LexBound),
}

/// Shared body for the ZREMRANGEBY{RANK,SCORE,LEX} family: remove a range, delete the key
/// if drained, reply the count removed. WRONGTYPE on a non-zset, 0 on a missing key.
fn remrange_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    key: &[u8],
    range: RemRange,
) -> Value {
    store.rmw_mut(db, key, now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(zset) = o.as_zset_mut() else {
                return wrong_type();
            };
            let removed = match &range {
                RemRange::Rank(s, e) => zset.remove_range_by_rank(*s, *e),
                RemRange::Score(min, max) => zset.remove_range_by_score(*min, *max),
                RemRange::Lex(min, max) => zset.remove_range_by_lex(min, max),
            };
            let action = if zset.is_empty() {
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

/// `ZREMRANGEBYRANK key start stop` -> the count removed.
pub fn cmd_zremrangebyrank<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("zremrangebyrank"));
    }
    let (Some(start), Some(stop)) = (parse_i64(&req.args[2]), parse_i64(&req.args[3])) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    remrange_generic(store, db, now, &req.args[1], RemRange::Rank(start, stop))
}

/// `ZREMRANGEBYSCORE key min max` -> the count removed.
pub fn cmd_zremrangebyscore<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("zremrangebyscore"));
    }
    let (Some(min), Some(max)) = (
        parse_score_bound(&req.args[2]),
        parse_score_bound(&req.args[3]),
    ) else {
        return Value::error(ErrorReply::min_or_max_not_a_float());
    };
    remrange_generic(store, db, now, &req.args[1], RemRange::Score(min, max))
}

/// `ZREMRANGEBYLEX key min max` -> the count removed.
pub fn cmd_zremrangebylex<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("zremrangebylex"));
    }
    let (Some(min), Some(max)) = (parse_lex_bound(&req.args[2]), parse_lex_bound(&req.args[3]))
    else {
        return Value::error(ErrorReply::min_or_max_not_valid_string_range());
    };
    remrange_generic(store, db, now, &req.args[1], RemRange::Lex(min, max))
}

// ===========================================================================
// ZCOUNT / ZLEXCOUNT.
// ===========================================================================

/// `ZCOUNT key min max` -> the count of members whose score is within `[min, max]` (with
/// `(` exclusive / `inf`). 0 on a missing key; WRONGTYPE on a non-zset.
pub fn cmd_zcount<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("zcount"));
    }
    let (Some(min), Some(max)) = (
        parse_score_bound(&req.args[2]),
        parse_score_bound(&req.args[3]),
    ) else {
        return Value::error(ErrorReply::min_or_max_not_a_float());
    };
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => match o.as_zset_mut() {
            Some(zset) => keep(Value::Integer(zset.count_by_score(min, max) as i64)),
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `ZLEXCOUNT key min max` -> the count of members within the lex range `[min, max]`
/// (`[`/`(`/`-`/`+`). 0 on a missing key; WRONGTYPE on a non-zset.
pub fn cmd_zlexcount<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("zlexcount"));
    }
    let (Some(min), Some(max)) = (parse_lex_bound(&req.args[2]), parse_lex_bound(&req.args[3]))
    else {
        return Value::error(ErrorReply::min_or_max_not_valid_string_range());
    };
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => keep(Value::Integer(0)),
        RmwEntry::OccupiedMut(mut o) => match o.as_zset_mut() {
            Some(zset) => keep(Value::Integer(zset.count_by_lex(&min, &max) as i64)),
            None => wrong_type(),
        },
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

// ===========================================================================
// The ZRANGE family: the unified ZRANGE (BYSCORE/BYLEX/REV/LIMIT/WITHSCORES) plus the
// legacy ZREVRANGE / ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX / ZREVRANGEBYLEX
// aliases. All resolve to the same range vocabulary on the ZSetValue view.
// ===========================================================================

/// What kind of range a (parsed) ZRANGE request selects.
enum RangeKind {
    /// By index/rank: signed start/stop.
    Rank(i64, i64),
    /// By score: min/max score bounds.
    Score(ScoreBound, ScoreBound),
    /// By lex: min/max lex bounds.
    Lex(LexBound, LexBound),
}

/// A fully-parsed ZRANGE-family request: the range kind, the REV flag, an optional
/// `(offset, count)` LIMIT, and the WITHSCORES flag.
struct RangeSpec {
    kind: RangeKind,
    rev: bool,
    limit: Option<(i64, i64)>,
    with_scores: bool,
}

/// Evaluate a [`RangeSpec`] against the zset under `key`, replying with the member array
/// (or WITHSCORES pairs). A missing key is an empty array; WRONGTYPE on a non-zset.
fn eval_range<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    key: &[u8],
    spec: RangeSpec,
) -> Value {
    store.rmw_mut(db, key, now, move |entry| match entry {
        RmwEntry::Vacant => keep(members_reply(Vec::new(), spec.with_scores)),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(zset) = o.as_zset_mut() else {
                return wrong_type();
            };
            let pairs: Vec<(Vec<u8>, f64)> = match &spec.kind {
                RangeKind::Rank(s, e) => zset.range_by_rank(*s, *e, spec.rev),
                RangeKind::Score(min, max) => zset.range_by_score(*min, *max, spec.rev, spec.limit),
                RangeKind::Lex(min, max) => zset
                    .range_by_lex(min, max, spec.rev, spec.limit)
                    .into_iter()
                    // Lex ranges never carry scores in the reply, but WITHSCORES is
                    // illegal with BYLEX (rejected at parse); fill 0.0 defensively.
                    .map(|m| (m, 0.0))
                    .collect(),
            };
            keep(members_reply(pairs, spec.with_scores))
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
    })
}

/// `ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]` -- the
/// unified Redis 6.2 range command. Parses the mode + options, then evaluates. LIMIT is
/// only valid with BYSCORE/BYLEX; WITHSCORES is illegal with BYLEX. Byte-exact errors for
/// a bad score/lex bound and for a misused option.
pub fn cmd_zrange<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 4 {
        return Value::error(ErrorReply::wrong_arity("zrange"));
    }
    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut with_scores = false;
    let mut i = 4;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"BYSCORE" => by_score = true,
            b"BYLEX" => by_lex = true,
            b"REV" => rev = true,
            b"WITHSCORES" => with_scores = true,
            b"LIMIT" => {
                if i + 2 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                let (Some(off), Some(cnt)) =
                    (parse_i64(&req.args[i + 1]), parse_i64(&req.args[i + 2]))
                else {
                    return Value::error(ErrorReply::not_an_integer());
                };
                limit = Some((off, cnt));
                i += 2;
            }
            _ => return Value::error(ErrorReply::syntax_error()),
        }
        i += 1;
    }
    if by_score && by_lex {
        return Value::error(ErrorReply::syntax_error());
    }
    // WITHSCORES is illegal with BYLEX (a lex range carries no scores) -> the specific
    // Redis conflict message, NOT the generic syntax error.
    if with_scores && by_lex {
        return Value::error(ErrorReply::zrange_withscores_not_with_bylex());
    }
    // LIMIT requires BYSCORE or BYLEX (Redis: "syntax error, LIMIT is only supported in
    // combination with either BYSCORE or BYLEX").
    if limit.is_some() && !(by_score || by_lex) {
        return Value::error(ErrorReply::zrange_limit_only_with_byscore_or_bylex());
    }
    let (lo, hi) = (&req.args[2], &req.args[3]);
    // For a REV range the client passes max first then min (ZRANGE REV BYSCORE max min),
    // so swap the bound argument order before parsing into (min, max).
    let kind = if by_score {
        let (min_arg, max_arg) = if rev { (hi, lo) } else { (lo, hi) };
        let (Some(min), Some(max)) = (parse_score_bound(min_arg), parse_score_bound(max_arg))
        else {
            return Value::error(ErrorReply::min_or_max_not_a_float());
        };
        RangeKind::Score(min, max)
    } else if by_lex {
        let (min_arg, max_arg) = if rev { (hi, lo) } else { (lo, hi) };
        let (Some(min), Some(max)) = (parse_lex_bound(min_arg), parse_lex_bound(max_arg)) else {
            return Value::error(ErrorReply::min_or_max_not_valid_string_range());
        };
        RangeKind::Lex(min, max)
    } else {
        let (Some(start), Some(stop)) = (parse_i64(lo), parse_i64(hi)) else {
            return Value::error(ErrorReply::not_an_integer());
        };
        RangeKind::Rank(start, stop)
    };
    eval_range(
        store,
        db,
        now,
        &req.args[1],
        RangeSpec {
            kind,
            rev,
            limit,
            with_scores,
        },
    )
}

/// `ZREVRANGE key start stop [WITHSCORES]` -> the by-index range in DESCENDING order.
pub fn cmd_zrevrange<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 4 || req.args.len() > 5 {
        return Value::error(ErrorReply::wrong_arity("zrevrange"));
    }
    let with_scores = parse_trailing_withscores(req, 4);
    let Some(with_scores) = with_scores else {
        return Value::error(ErrorReply::syntax_error());
    };
    let (Some(start), Some(stop)) = (parse_i64(&req.args[2]), parse_i64(&req.args[3])) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    eval_range(
        store,
        db,
        now,
        &req.args[1],
        RangeSpec {
            kind: RangeKind::Rank(start, stop),
            rev: true,
            limit: None,
            with_scores,
        },
    )
}

/// Parse an optional trailing `WITHSCORES` starting at arg index `at` (the legacy
/// ZRANGEBYSCORE/ZREVRANGE forms). Returns `Some(flag)`, or `None` if the trailing token
/// is present but not WITHSCORES (a syntax error the caller surfaces). When `at` is past
/// the end, returns `Some(false)`.
fn parse_trailing_withscores(req: &Request, at: usize) -> Option<bool> {
    if at >= req.args.len() {
        return Some(false);
    }
    if ascii_upper(&req.args[at]).as_slice() == b"WITHSCORES" {
        Some(true)
    } else {
        None
    }
}

/// Shared body for the legacy ZRANGEBYSCORE / ZREVRANGEBYSCORE: `key min max [WITHSCORES]
/// [LIMIT offset count]`. `rev` selects the direction (and swaps the min/max argument
/// order for ZREVRANGEBYSCORE, where the client passes max first).
fn rangebyscore_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    rev: bool,
    cmd_name: &str,
) -> Value {
    if req.args.len() < 4 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    // ZREVRANGEBYSCORE max min: the larger bound comes first, so swap into (min, max).
    let (min_arg, max_arg) = if rev {
        (&req.args[3], &req.args[2])
    } else {
        (&req.args[2], &req.args[3])
    };
    let (Some(min), Some(max)) = (parse_score_bound(min_arg), parse_score_bound(max_arg)) else {
        return Value::error(ErrorReply::min_or_max_not_a_float());
    };
    // Parse the option tail: WITHSCORES and LIMIT offset count, in any order.
    let mut with_scores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 4;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"WITHSCORES" => with_scores = true,
            b"LIMIT" => {
                if i + 2 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                let (Some(off), Some(cnt)) =
                    (parse_i64(&req.args[i + 1]), parse_i64(&req.args[i + 2]))
                else {
                    return Value::error(ErrorReply::not_an_integer());
                };
                limit = Some((off, cnt));
                i += 2;
            }
            _ => return Value::error(ErrorReply::syntax_error()),
        }
        i += 1;
    }
    eval_range(
        store,
        db,
        now,
        &req.args[1],
        RangeSpec {
            kind: RangeKind::Score(min, max),
            rev,
            limit,
            with_scores,
        },
    )
}

/// `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`.
pub fn cmd_zrangebyscore<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    rangebyscore_generic(store, db, now, req, false, "zrangebyscore")
}

/// `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]`.
pub fn cmd_zrevrangebyscore<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    rangebyscore_generic(store, db, now, req, true, "zrevrangebyscore")
}

/// Shared body for the legacy ZRANGEBYLEX / ZREVRANGEBYLEX: `key min max [LIMIT offset
/// count]`. `rev` swaps the min/max argument order for ZREVRANGEBYLEX.
fn rangebylex_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    rev: bool,
    cmd_name: &str,
) -> Value {
    if req.args.len() < 4 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let (min_arg, max_arg) = if rev {
        (&req.args[3], &req.args[2])
    } else {
        (&req.args[2], &req.args[3])
    };
    let (Some(min), Some(max)) = (parse_lex_bound(min_arg), parse_lex_bound(max_arg)) else {
        return Value::error(ErrorReply::min_or_max_not_valid_string_range());
    };
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 4;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"LIMIT" => {
                if i + 2 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                let (Some(off), Some(cnt)) =
                    (parse_i64(&req.args[i + 1]), parse_i64(&req.args[i + 2]))
                else {
                    return Value::error(ErrorReply::not_an_integer());
                };
                limit = Some((off, cnt));
                i += 2;
            }
            _ => return Value::error(ErrorReply::syntax_error()),
        }
        i += 1;
    }
    eval_range(
        store,
        db,
        now,
        &req.args[1],
        RangeSpec {
            kind: RangeKind::Lex(min, max),
            rev,
            limit,
            with_scores: false,
        },
    )
}

/// `ZRANGEBYLEX key min max [LIMIT offset count]`.
pub fn cmd_zrangebylex<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    rangebylex_generic(store, db, now, req, false, "zrangebylex")
}

/// `ZREVRANGEBYLEX key max min [LIMIT offset count]`.
pub fn cmd_zrevrangebylex<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    rangebylex_generic(store, db, now, req, true, "zrevrangebylex")
}

// ===========================================================================
// ZPOPMIN / ZPOPMAX.
// ===========================================================================

/// Shared body for ZPOPMIN/ZPOPMAX: pop up to `count` (default 1) extreme members and
/// reply with the member+score pairs (always WITHSCORES-shaped). Emptying deletes the key.
fn pop_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    max: bool,
    cmd_name: &str,
) -> Value {
    if req.args.len() < 2 || req.args.len() > 3 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    // The optional count (a negative count is a value-out-of-range error in Redis 7).
    let count: Option<i64> = if req.args.len() == 3 {
        match parse_i64(&req.args[2]) {
            Some(n) if n < 0 => {
                return Value::error(ErrorReply::err("value is out of range, must be positive"));
            }
            Some(n) => Some(n),
            None => return Value::error(ErrorReply::not_an_integer()),
        }
    } else {
        None
    };
    let want = count.unwrap_or(1).max(0) as usize;
    store.rmw_mut(db, &req.args[1], now, move |entry| match entry {
        // Missing key: an empty array (Redis ZPOPMIN on a missing key is an empty array,
        // both with and without count).
        RmwEntry::Vacant => keep(Value::Array(Some(Vec::new()))),
        RmwEntry::OccupiedMut(mut o) => {
            let Some(zset) = o.as_zset_mut() else {
                return wrong_type();
            };
            let popped = if max {
                zset.pop_max(want)
            } else {
                zset.pop_min(want)
            };
            // WITHSCORES-shaped: a Value::Pairs (member, ,double) -> RESP3 nests, RESP2
            // flattens to [member, score, ...].
            let reply = members_reply(popped, true);
            let action = if zset.is_empty() {
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

/// `ZPOPMIN key [count]` -> the popped lowest-score member+score pairs.
pub fn cmd_zpopmin<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    pop_generic(store, db, now, req, false, "zpopmin")
}

/// `ZPOPMAX key [count]` -> the popped highest-score member+score pairs.
pub fn cmd_zpopmax<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    pop_generic(store, db, now, req, true, "zpopmax")
}

// ===========================================================================
// ZMPOP: pop min/max from the first non-empty zset among several keys (the
// non-blocking multi-key pop, Redis 7). `ZMPOP numkeys key [key ...] MIN|MAX
// [COUNT count]`.
// ===========================================================================

/// Build the ZMPOP element list: an ARRAY of `[member, score]` 2-element arrays (the score a
/// bulk string in the Redis HUMAN spelling). Redis ZMPOP nests each (member, score) pair as
/// its OWN 2-element array in BOTH protocols (unlike the WITHSCORES flat/nested split), so we
/// build the explicit nested shape here rather than reuse the WITHSCORES `Value::Pairs`.
fn zmpop_members(pairs: Vec<(Vec<u8>, f64)>) -> Value {
    let out: Vec<Value> = pairs
        .into_iter()
        .map(|(m, s)| Value::Array(Some(vec![bulk(m), score_bulk(s)])))
        .collect();
    Value::Array(Some(out))
}

/// `ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]` -> a two-element reply
/// `[key, [[member, score], ...]]` for the FIRST key (in argument order) that holds a
/// non-empty sorted set, popping up to `count` (default 1) members from the MIN (lowest
/// scores) or MAX (highest scores) end; or nil if EVERY listed key is missing/empty (Redis
/// `zmpopCommand` / `zmpopGenericCommand`).
///
/// Parsing reuses the shared `<numkeys> key... <DIR> [COUNT count]` grammar (here `DIR` is
/// `MIN`/`MAX`). WRONGTYPE if the FIRST existing key holds a non-zset. SINGLE-SHARD handler:
/// it scans the named keys on the connection's accept shard (a spanning ZMPOP is kept HOME
/// by the serve loop), preserving the "first non-empty wins" order.
pub fn cmd_zmpop<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    let parsed = match crate::cmd_list::parse_mpop_args(req, "ZMPOP", &[b"MIN", b"MAX"]) {
        Ok(p) => p,
        Err(e) => return Value::error(e),
    };
    let max = parsed.direction == b"MAX";
    for key in &parsed.keys {
        let key = Bytes::copy_from_slice(key);
        let outcome = store.rmw_mut(db, &key, now, |entry| match entry {
            // A missing key contributes nothing; keep scanning (the Null sentinel signals skip).
            RmwEntry::Vacant => keep(Value::Null),
            RmwEntry::OccupiedMut(mut o) => {
                let Some(zset) = o.as_zset_mut() else {
                    return wrong_type();
                };
                let popped = if max {
                    zset.pop_max(parsed.count)
                } else {
                    zset.pop_min(parsed.count)
                };
                let reply = Value::Array(Some(vec![bulk(key.to_vec()), zmpop_members(popped)]));
                let action = if zset.is_empty() {
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
        });
        match outcome {
            // Skip a missing key and keep scanning; return any real reply / WRONGTYPE.
            Value::Null => {}
            other => return other,
        }
    }
    // No key held a non-empty zset: the NULL ARRAY reply (Redis ZMPOP calls addReplyNullArray
    // -> RESP2 `*-1`, RESP3 `_`). DISTINCT from a missing-key skip's `Value::Null` sentinel.
    Value::Array(None)
}

// ===========================================================================
// ZRANDMEMBER: random member selection (Env-rng seam). Does NOT remove.
// ===========================================================================

/// `ZRANDMEMBER key [count [WITHSCORES]]` -> one random member (no count: a bulk, nil if
/// absent), or `count` members: a POSITIVE count returns up to `min(count, card)` DISTINCT
/// members; a NEGATIVE count returns exactly `|count|` members WITH REPEATS. WITHSCORES
/// interleaves each member with its score. WRONGTYPE on a non-zset. `seed` from the Env
/// RNG (ADR-0003).
pub fn cmd_zrandmember<S: Store>(
    store: &mut S,
    db: u32,
    seed: u64,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() < 2 || req.args.len() > 4 {
        return Value::error(ErrorReply::wrong_arity("zrandmember"));
    }
    let count: Option<i64> = if req.args.len() >= 3 {
        match parse_i64(&req.args[2]) {
            Some(n) => Some(n),
            None => return Value::error(ErrorReply::not_an_integer()),
        }
    } else {
        None
    };
    let with_scores = if req.args.len() == 4 {
        if ascii_upper(&req.args[3]).as_slice() == b"WITHSCORES" {
            true
        } else {
            return Value::error(ErrorReply::syntax_error());
        }
    } else {
        false
    };
    store.rmw_mut(db, &req.args[1], now, move |entry| {
        let members = match entry {
            RmwEntry::Vacant => {
                return keep(match count {
                    None => Value::Null,
                    Some(_) => Value::Array(Some(Vec::new())),
                });
            }
            RmwEntry::OccupiedMut(mut o) => match o.as_zset_mut() {
                Some(zset) => zset.members_with_scores(),
                None => return wrong_type(),
            },
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        };
        keep(zrandmember_reply(&members, count, with_scores, seed))
    })
}

/// Build the ZRANDMEMBER reply from the deterministic `members` order, the parsed `count`,
/// the WITHSCORES flag, and the caller-drawn `seed`.
fn zrandmember_reply(
    members: &[(Vec<u8>, f64)],
    count: Option<i64>,
    with_scores: bool,
    seed: u64,
) -> Value {
    let n = members.len();
    if n == 0 {
        return match count {
            None => Value::Null,
            Some(_) => Value::Array(Some(Vec::new())),
        };
    }
    let mut rng = SeedRng::new(seed);
    match count {
        // No count: a single bulk member (never WITHSCORES; arity forbids it).
        None => bulk(members[(rng.next() % n as u64) as usize].0.clone()),
        Some(c) if c >= 0 => {
            // Positive: DISTINCT, up to min(c, n). A partial Fisher-Yates of the indices.
            let want = (c as usize).min(n);
            let mut idxs: Vec<usize> = (0..n).collect();
            for i in 0..want {
                let j = i + (rng.next() % (n - i) as u64) as usize;
                idxs.swap(i, j);
            }
            let chosen: Vec<(Vec<u8>, f64)> =
                idxs[..want].iter().map(|&i| members[i].clone()).collect();
            members_reply(chosen, with_scores)
        }
        Some(c) => {
            // Negative: WITH REPEATS, exactly |count|, each drawn independently.
            let want = c.unsigned_abs() as usize;
            let chosen: Vec<(Vec<u8>, f64)> = (0..want)
                .map(|_| members[(rng.next() % n as u64) as usize].clone())
                .collect();
            members_reply(chosen, with_scores)
        }
    }
}

// ===========================================================================
// ZSCAN: cursored iteration over the zset's members (member + score interleaved).
// ===========================================================================

/// The default COUNT hint for ZSCAN when none is given (Redis SCAN-family default 10).
const ZSCAN_DEFAULT_COUNT: usize = 10;

/// `ZSCAN key cursor [MATCH pattern] [COUNT n]` -> the 2-element reply
/// `[next_cursor_bulkstring, [member, score, member, score, ...]]`. The cursor is the
/// decimal wire token; `0` starts and a returned `0` means complete. MATCH globs the
/// member. A missing key is `[0, []]`. WRONGTYPE on a non-zset.
///
/// A SMALL (listpack) zset returns ALL members at once with cursor 0, ignoring COUNT
/// (Redis small-collection SCAN). A SKIPLIST-encoded zset reuses the SAME hash-ordered
/// cursor mechanism the keyspace SCAN / SSCAN use over the zset's members.
pub fn cmd_zscan<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("zscan"));
    }
    let Some(cursor) = ScanCursor::from_token(&req.args[2]) else {
        return Value::error(ErrorReply::invalid_cursor());
    };
    let mut pattern: Option<Bytes> = None;
    let mut count: usize = ZSCAN_DEFAULT_COUNT;
    let mut i = 3;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
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
                return keep(zscan_reply(ScanCursor::START, Vec::new()));
            }
            RmwEntry::OccupiedMut(mut o) => match o.as_zset_mut() {
                Some(zset) => (zset.members_with_scores(), zset.is_listpack()),
                None => return wrong_type(),
            },
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        };
        let (next, batch) = zscan_step(&members, cursor, count, pattern.as_deref(), is_small);
        keep(zscan_reply(next, batch))
    })
}

/// One bounded ZSCAN batch over the zset's `members` in ascending [`member_scan_hash`]
/// order, starting after `cursor`, applying the MATCH glob to the MEMBER. Returns the next
/// cursor (`ScanCursor(0)` = complete) and the kept (member, score) pairs. A SMALL
/// (listpack) zset returns the WHOLE zset in ONE batch with next-cursor 0, IGNORING COUNT.
fn zscan_step(
    members: &[(Vec<u8>, f64)],
    cursor: ScanCursor,
    count: usize,
    pattern: Option<&[u8]>,
    is_small: bool,
) -> (ScanCursor, Vec<(Vec<u8>, f64)>) {
    if is_small {
        if !cursor.is_start() {
            return (ScanCursor::START, Vec::new());
        }
        let kept: Vec<(Vec<u8>, f64)> = members
            .iter()
            .filter(|(m, _)| pattern.is_none_or(|p| crate::glob::glob_match(p, m)))
            .cloned()
            .collect();
        return (ScanCursor::START, kept);
    }
    let mut order: Vec<(u64, usize)> = members
        .iter()
        .enumerate()
        .map(|(idx, (m, _))| (member_scan_hash(m), idx))
        .collect();
    order.sort_unstable_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| members[a.1].0.cmp(&members[b.1].0))
    });
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
    let kept: Vec<(Vec<u8>, f64)> = examined
        .into_iter()
        .filter(|&idx| pattern.is_none_or(|p| crate::glob::glob_match(p, &members[idx].0)))
        .map(|idx| members[idx].clone())
        .collect();
    (next, kept)
}

/// Build the ZSCAN reply `[cursor, [member, score, member, score, ...]]`. The score is the
/// HUMAN bulk spelling (Redis ZSCAN interleaves the member with its score as a bulk).
fn zscan_reply(next: ScanCursor, batch: Vec<(Vec<u8>, f64)>) -> Value {
    let mut items: Vec<Value> = Vec::with_capacity(batch.len() * 2);
    for (m, s) in batch {
        items.push(bulk(m));
        items.push(score_bulk(s));
    }
    Value::Array(Some(vec![
        Value::bulk(next.to_token().into_bytes()),
        Value::Array(Some(items)),
    ]))
}

/// The fixed-seed stable hash that orders a zset's MEMBERS for ZSCAN (the command-layer
/// analog of the store's `scan_hash`, KEYSPACE.md). Identical construction to the set
/// handler's `member_scan_hash` and the hash handler's `field_scan_hash`: a small
/// FNV-style mix fully determined by the member bytes (no table state, no OS entropy,
/// ADR-0003), so the ZSCAN order is stable and resize-invariant. Kept here (NOT imported
/// from another command module) because each command module names only the storage waist.
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

// ===========================================================================
// ZRANGESTORE: evaluate a ZRANGE into a destination zset.
// ===========================================================================

/// `ZRANGESTORE dst src start stop [BYSCORE|BYLEX] [REV] [LIMIT off cnt]` -> the number of
/// members stored at `dst`. An empty result DELETES dst; dst is overwritten otherwise.
/// WRONGTYPE if src is a non-zset (checked before the dst write). SAME-SHARD only.
pub fn cmd_zrangestore<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    // ZRANGESTORE dst src start stop [opts...]: at least dst + src + start + stop.
    if req.args.len() < 5 {
        return Value::error(ErrorReply::wrong_arity("zrangestore"));
    }
    let dst = req.args[1].clone();
    let src = req.args[2].clone();
    // Parse the same option grammar as ZRANGE, but offset by one (src at [2], start/stop at
    // [3]/[4], options from [5]).
    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 5;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"BYSCORE" => by_score = true,
            b"BYLEX" => by_lex = true,
            b"REV" => rev = true,
            b"LIMIT" => {
                if i + 2 >= req.args.len() {
                    return Value::error(ErrorReply::syntax_error());
                }
                let (Some(off), Some(cnt)) =
                    (parse_i64(&req.args[i + 1]), parse_i64(&req.args[i + 2]))
                else {
                    return Value::error(ErrorReply::not_an_integer());
                };
                limit = Some((off, cnt));
                i += 2;
            }
            _ => return Value::error(ErrorReply::syntax_error()),
        }
        i += 1;
    }
    // BYSCORE+BYLEX is the generic syntax error; LIMIT without BYSCORE/BYLEX is the same
    // specific message ZRANGE uses (factored into a shared named constructor).
    if by_score && by_lex {
        return Value::error(ErrorReply::syntax_error());
    }
    if limit.is_some() && !(by_score || by_lex) {
        return Value::error(ErrorReply::zrange_limit_only_with_byscore_or_bylex());
    }
    let (lo, hi) = (&req.args[3], &req.args[4]);
    let kind = if by_score {
        let (min_arg, max_arg) = if rev { (hi, lo) } else { (lo, hi) };
        let (Some(min), Some(max)) = (parse_score_bound(min_arg), parse_score_bound(max_arg))
        else {
            return Value::error(ErrorReply::min_or_max_not_a_float());
        };
        RangeKind::Score(min, max)
    } else if by_lex {
        let (min_arg, max_arg) = if rev { (hi, lo) } else { (lo, hi) };
        let (Some(min), Some(max)) = (parse_lex_bound(min_arg), parse_lex_bound(max_arg)) else {
            return Value::error(ErrorReply::min_or_max_not_valid_string_range());
        };
        RangeKind::Lex(min, max)
    } else {
        let (Some(start), Some(stop)) = (parse_i64(lo), parse_i64(hi)) else {
            return Value::error(ErrorReply::not_an_integer());
        };
        RangeKind::Rank(start, stop)
    };

    // Read the source range as (member, score) pairs. For BYLEX the score is read
    // separately (the lex range drops scores), so re-read each member's score from src.
    let result = read_range_pairs(store, db, now, &src, kind, rev, limit);
    let Ok(pairs) = result else {
        return Value::error(ErrorReply::wrong_type());
    };
    let card = pairs.len() as i64;
    if pairs.is_empty() {
        store.delete(db, &dst, now);
    } else {
        store.rmw_mut(db, &dst, now, move |_entry| RmwStep {
            action: RmwAction::Insert(NewValueOwned::zset(pairs)),
            expire: ExpireWrite::Clear,
            reply: (),
        });
    }
    Value::Integer(card)
}

/// Read a ZRANGE-family selection from `key` as `(member, score)` pairs (for ZRANGESTORE),
/// or `Err(())` if `key` is a non-zset. A missing key is an empty result. For a BYLEX
/// range the per-member score is looked up so the stored zset keeps the real scores.
fn read_range_pairs<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    key: &[u8],
    kind: RangeKind,
    rev: bool,
    limit: Option<(i64, i64)>,
) -> Result<Vec<(Vec<u8>, f64)>, ()> {
    store.rmw_mut(db, key, now, move |entry| {
        let r: Result<Vec<(Vec<u8>, f64)>, ()> = match entry {
            RmwEntry::Vacant => Ok(Vec::new()),
            RmwEntry::OccupiedMut(mut o) => match o.as_zset_mut() {
                Some(zset) => Ok(match &kind {
                    RangeKind::Rank(s, e) => zset.range_by_rank(*s, *e, rev),
                    RangeKind::Score(min, max) => zset.range_by_score(*min, *max, rev, limit),
                    RangeKind::Lex(min, max) => zset
                        .range_by_lex(min, max, rev, limit)
                        .into_iter()
                        .map(|m| {
                            let s = zset.score(&m).unwrap_or(0.0);
                            (m, s)
                        })
                        .collect(),
                }),
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

// ===========================================================================
// Aggregations: ZUNION / ZINTER / ZDIFF (+ *STORE) + ZINTERCARD.
// ===========================================================================

/// A `(member, score)` pair: a zset member with its score. The unit the zset handlers and the
/// shared [`zset_combine`] pass around (members + scores, in result order).
pub type ScoredMember = (Vec<u8>, f64);

/// One aggregation source for [`zset_combine`]: its already-gathered `(member, score)` pairs
/// plus its WEIGHTS factor (applied inside the combine). The cross-shard coordinator builds
/// these from its cross-shard gathers; the single-shard handler from its local store reads.
pub type WeightedSource = (Vec<ScoredMember>, f64);

/// The aggregate function for combining scores of a member present in multiple source
/// zsets (the AGGREGATE option; default SUM). PUBLIC so the cross-shard coordinator (which
/// gathers each source's `(member, score)` pairs itself, then combines via the SHARED
/// [`zset_combine`]) names the same aggregate the single-shard handlers do; re-exported from
/// the crate root.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Aggregate {
    /// AGGREGATE SUM (the default): combined score is the sum (NaN coerced to 0).
    Sum,
    /// AGGREGATE MIN: combined score is the minimum.
    Min,
    /// AGGREGATE MAX: combined score is the maximum.
    Max,
}

/// The aggregation operation requested. PUBLIC so the cross-shard coordinator names the same
/// operation the single-shard handlers do (it gathers each source's pairs, then combines via
/// the SHARED [`zset_combine`]); re-exported from the crate root.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AggOp {
    /// ZUNION / ZUNIONSTORE: accumulate every member of every source.
    Union,
    /// ZINTER / ZINTERSTORE / ZINTERCARD: keep members present in all sources.
    Inter,
    /// ZDIFF / ZDIFFSTORE: the first source minus all the rest (scores from the first).
    Diff,
}

/// Read a zset (or a plain SET, which Redis treats as a zset of score 1.0 in aggregations)
/// under `key` as `(member, score)` pairs, or `Err(())` on a non-zset/non-set. A missing
/// key is an empty result. NOTE: for parity simplicity this v1 reads only ZSET sources;
/// a SET source is treated via its members at score 1.0.
fn read_agg_source<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    key: &[u8],
) -> Result<Vec<(Vec<u8>, f64)>, ()> {
    store.rmw_mut(db, key, now, |entry| {
        let r: Result<Vec<(Vec<u8>, f64)>, ()> = match entry {
            RmwEntry::Vacant => Ok(Vec::new()),
            RmwEntry::OccupiedMut(mut o) => {
                if let Some(zset) = o.as_zset_mut() {
                    Ok(zset.members_with_scores())
                } else if let Some(set) = o.as_set_mut() {
                    // Redis: a SET source counts each member with score 1.0.
                    Ok(set.members().into_iter().map(|m| (m, 1.0)).collect())
                } else {
                    Err(())
                }
            }
            RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
        };
        RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: r,
        }
    })
}

/// Combine two scores under the aggregate function. SUM coerces a NaN result to 0, matching
/// Redis (`zunionInterAggregate`: `if (isnan(*target)) *target = 0;` after each SUM step);
/// `+inf + -inf` is the case that produces NaN. MIN/MAX never produce a NaN from finite or
/// infinite inputs (a NaN input cannot occur: scores enter finite/infinite only).
fn combine(agg: Aggregate, a: f64, b: f64) -> f64 {
    match agg {
        Aggregate::Sum => {
            let r = a + b;
            if r.is_nan() { 0.0 } else { r }
        }
        Aggregate::Min => a.min(b),
        Aggregate::Max => a.max(b),
    }
}

/// A parsed aggregation request: the source keys, the per-source WEIGHTS, the AGGREGATE
/// function, and (for the non-STORE / ZINTERCARD forms) the WITHSCORES / LIMIT tails.
struct AggArgs {
    keys: Vec<Bytes>,
    weights: Vec<f64>,
    aggregate: Aggregate,
    with_scores: bool,
}

/// Parse the `numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]`
/// grammar shared by ZUNION/ZINTER/ZDIFF and their STORE forms. `allow_weights` is false
/// for ZDIFF/ZDIFFSTORE (Redis ZDIFF has no WEIGHTS/AGGREGATE). `allow_withscores` is false
/// for the *STORE forms (ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE do NOT accept WITHSCORES -> a
/// generic `-ERR syntax error`); it is true for the read ZUNION/ZINTER/ZDIFF forms.
/// `numkeys_at` is the arg index of `numkeys` (1 for the non-store forms, 2 for the STORE
/// forms with a leading dest). Returns `Ok(args)` or an `Err(error_value)`.
fn parse_agg_args(
    req: &Request,
    numkeys_at: usize,
    allow_weights: bool,
    allow_withscores: bool,
) -> Result<AggArgs, Value> {
    let Some(numkeys) = parse_i64(&req.args[numkeys_at]) else {
        return Err(Value::error(ErrorReply::not_an_integer()));
    };
    if numkeys <= 0 {
        return Err(Value::error(ErrorReply::numkeys_should_be_positive()));
    }
    let numkeys = numkeys as usize;
    let keys_start = numkeys_at + 1;
    if keys_start + numkeys > req.args.len() {
        return Err(Value::error(ErrorReply::numkeys_greater_than_args()));
    }
    let keys: Vec<Bytes> = req.args[keys_start..keys_start + numkeys].to_vec();
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = Aggregate::Sum;
    let mut with_scores = false;
    let mut i = keys_start + numkeys;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
            b"WEIGHTS" if allow_weights => {
                // Need exactly `numkeys` weight values after the WEIGHTS token.
                if i + 1 + numkeys > req.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                for (k, slot) in weights.iter_mut().enumerate() {
                    let Some(w) = parse_f64(&req.args[i + 1 + k]) else {
                        return Err(Value::error(ErrorReply::weight_not_a_float()));
                    };
                    *slot = w;
                }
                i += 1 + numkeys;
            }
            b"AGGREGATE" if allow_weights => {
                if i + 1 >= req.args.len() {
                    return Err(Value::error(ErrorReply::syntax_error()));
                }
                aggregate = match ascii_upper(&req.args[i + 1]).as_slice() {
                    b"SUM" => Aggregate::Sum,
                    b"MIN" => Aggregate::Min,
                    b"MAX" => Aggregate::Max,
                    _ => return Err(Value::error(ErrorReply::syntax_error())),
                };
                i += 2;
            }
            b"WITHSCORES" if allow_withscores => {
                with_scores = true;
                i += 1;
            }
            // WITHSCORES in a *STORE context (or any unrecognized token) is a syntax error.
            _ => return Err(Value::error(ErrorReply::syntax_error())),
        }
    }
    Ok(AggArgs {
        keys,
        weights,
        aggregate,
        with_scores,
    })
}

/// The PURE zset-aggregation combiner: given each source's already-gathered
/// `(members+scores, weight)` IN ORIGINAL KEY ORDER (`sources[i].0` is key `i`'s pairs,
/// `sources[i].1` is key `i`'s WEIGHTS multiplier; a missing/empty source key is an EMPTY
/// pair list), return the combined result in deterministic `(score, member)` order. This is
/// the ONE SOURCE OF TRUTH for ZUNION/ZINTER/ZDIFF semantics: BOTH the single-shard handler
/// ([`compute_agg`], which reads each key's pairs from the local store) AND the cross-shard
/// coordinator (which gathers each key's pairs from its owner shard via ZRANGE WITHSCORES,
/// with a SET-source-as-score-1.0 fallback) call this, so the two CANNOT drift
/// (COORDINATOR.md #107, Stage 2b-2: gather + shared combine).
///
/// The semantics, preserved EXACTLY from the prior in-line `compute_agg`:
/// - Each source's score is multiplied by its WEIGHTS factor; a NaN product (e.g. `0 * inf`)
///   is coerced to `0.0`, matching Redis (`zunionInterAggregate`: `if (isnan(score)) score
///   = 0;` after the WEIGHTS multiply).
/// - **Union**: accumulate every member of every source, combining duplicate scores with
///   the AGGREGATE function ([`combine`]).
/// - **Inter**: keep a member of the FIRST source only if present in EVERY other source,
///   combining its scores left-to-right with AGGREGATE.
/// - **Diff**: members of the FIRST source not present in any other (scores from the first).
/// - SUM coerces a NaN result to 0 ([`combine`]); MIN/MAX never produce a NaN.
/// - The result is sorted by `(score, member)` with `f64::total_cmp` so the reply / stored
///   value is deterministic.
///
/// An empty `sources` yields the empty result for every op (the single-shard callers
/// validate arity before gathering, so this is the defensive floor).
#[must_use]
pub fn zset_combine(
    op: AggOp,
    aggregate: Aggregate,
    sources: &[WeightedSource],
) -> Vec<ScoredMember> {
    // Apply each source's WEIGHTS factor up front (weight multiplies the score; a NaN product
    // is coerced to 0), collapsing duplicate members WITHIN a source via the same insert (the
    // store yields each member once, so this also serves the defensive floor).
    let weighted: Vec<BTreeMap<Vec<u8>, f64>> = sources
        .iter()
        .map(|(members, w)| {
            let mut m: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
            for (member, score) in members {
                // weight * score; weight*inf etc. follow IEEE. Redis coerces a NaN product to
                // 0 (`zunionInterAggregate`: `if (isnan(score)) score = 0;` after the WEIGHTS
                // multiply); `0 * inf` is the case that produces NaN.
                let p = score * w;
                m.insert(member.clone(), if p.is_nan() { 0.0 } else { p });
            }
            m
        })
        .collect();
    let mut acc: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
    match op {
        AggOp::Union => {
            for src in &weighted {
                for (member, score) in src {
                    acc.entry(member.clone())
                        .and_modify(|s| *s = combine(aggregate, *s, *score))
                        .or_insert(*score);
                }
            }
        }
        AggOp::Inter => {
            if let Some((first, rest)) = weighted.split_first() {
                'members: for (member, score) in first {
                    let mut combined = *score;
                    for src in rest {
                        let Some(other) = src.get(member) else {
                            continue 'members;
                        };
                        combined = combine(aggregate, combined, *other);
                    }
                    acc.insert(member.clone(), combined);
                }
            }
        }
        AggOp::Diff => {
            // ZDIFF: members of the FIRST set not present in any other (scores from first).
            if let Some((first, rest)) = weighted.split_first() {
                for (member, score) in first {
                    if !rest.iter().any(|s| s.contains_key(member)) {
                        acc.insert(member.clone(), *score);
                    }
                }
            }
        }
    }
    // Order by (score, member).
    let mut out: Vec<(Vec<u8>, f64)> = acc.into_iter().collect();
    out.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    out
}

/// Compute the aggregation result over the source keys, in deterministic (score, member)
/// order. `Ok(pairs)` or `Err(())` if any required source is a non-zset/non-set. This is the
/// SINGLE-SHARD path: it READS each key's pairs from the local store (surfacing a WRONGTYPE
/// on a non-zset/non-set source), then delegates the PURE combine to the SHARED
/// [`zset_combine`] (the one source of truth the cross-shard coordinator also calls), so the
/// two cannot drift.
fn compute_agg<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    args: &AggArgs,
    op: AggOp,
) -> Result<Vec<(Vec<u8>, f64)>, ()> {
    // Read each source's pairs in ORIGINAL KEY ORDER, pairing each with its WEIGHTS factor;
    // any non-zset/non-set source aborts with Err BEFORE the combine.
    let mut sources: Vec<WeightedSource> = Vec::with_capacity(args.keys.len());
    for (idx, k) in args.keys.iter().enumerate() {
        let members = read_agg_source(store, db, now, k)?;
        sources.push((members, args.weights[idx]));
    }
    Ok(zset_combine(op, args.aggregate, &sources))
}

/// Shared body for the non-STORE ZUNION/ZINTER/ZDIFF: reply with the result members (or
/// WITHSCORES pairs). WRONGTYPE if any source is a non-zset/non-set.
fn agg_read_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    op: AggOp,
    cmd_name: &str,
) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    // The READ forms (ZUNION/ZINTER/ZDIFF) accept WITHSCORES.
    let args = match parse_agg_args(req, 1, op != AggOp::Diff, true) {
        Ok(a) => a,
        Err(e) => return e,
    };
    match compute_agg(store, db, now, &args, op) {
        Ok(pairs) => members_reply(pairs, args.with_scores),
        Err(()) => Value::error(ErrorReply::wrong_type()),
    }
}

/// Shared body for ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE: store the result at dest, reply the
/// cardinality. An empty result DELETES dest; dest is overwritten otherwise. WRONGTYPE if
/// any source is a non-zset/non-set (checked before the dest write). SAME-SHARD only.
fn agg_store_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    op: AggOp,
    cmd_name: &str,
) -> Value {
    if req.args.len() < 4 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let dest = req.args[1].clone();
    // The *STORE forms do NOT accept WITHSCORES (Redis -> `-ERR syntax error`).
    let args = match parse_agg_args(req, 2, op != AggOp::Diff, false) {
        Ok(a) => a,
        Err(e) => return e,
    };
    let Ok(pairs) = compute_agg(store, db, now, &args, op) else {
        return Value::error(ErrorReply::wrong_type());
    };
    let card = pairs.len() as i64;
    if pairs.is_empty() {
        store.delete(db, &dest, now);
    } else {
        store.rmw_mut(db, &dest, now, move |_entry| RmwStep {
            action: RmwAction::Insert(NewValueOwned::zset(pairs)),
            expire: ExpireWrite::Clear,
            reply: (),
        });
    }
    Value::Integer(card)
}

/// `ZUNION numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE ...] [WITHSCORES]`.
pub fn cmd_zunion<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    agg_read_generic(store, db, now, req, AggOp::Union, "zunion")
}

/// `ZINTER numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE ...] [WITHSCORES]`.
pub fn cmd_zinter<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    agg_read_generic(store, db, now, req, AggOp::Inter, "zinter")
}

/// `ZDIFF numkeys key [key ...] [WITHSCORES]`.
pub fn cmd_zdiff<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    agg_read_generic(store, db, now, req, AggOp::Diff, "zdiff")
}

/// `ZUNIONSTORE dest numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]`.
pub fn cmd_zunionstore<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    agg_store_generic(store, db, now, req, AggOp::Union, "zunionstore")
}

/// `ZINTERSTORE dest numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]`.
pub fn cmd_zinterstore<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    agg_store_generic(store, db, now, req, AggOp::Inter, "zinterstore")
}

/// `ZDIFFSTORE dest numkeys key [key ...]`.
pub fn cmd_zdiffstore<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    agg_store_generic(store, db, now, req, AggOp::Diff, "zdiffstore")
}

/// `ZINTERCARD numkeys key [key ...] [LIMIT n]` -> the cardinality of the intersection,
/// capped at `limit` (0 = no limit). WRONGTYPE if any source is a non-zset/non-set.
pub fn cmd_zintercard<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 3 {
        return Value::error(ErrorReply::wrong_arity("zintercard"));
    }
    let Some(numkeys) = parse_i64(&req.args[1]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    if numkeys <= 0 {
        return Value::error(ErrorReply::numkeys_should_be_positive());
    }
    let numkeys = numkeys as usize;
    if 2 + numkeys > req.args.len() {
        return Value::error(ErrorReply::numkeys_greater_than_args());
    }
    let keys: Vec<Bytes> = req.args[2..2 + numkeys].to_vec();
    let mut limit: usize = 0;
    let mut i = 2 + numkeys;
    while i < req.args.len() {
        match ascii_upper(&req.args[i]).as_slice() {
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
    let args = AggArgs {
        keys,
        weights: vec![1.0; numkeys],
        aggregate: Aggregate::Sum,
        with_scores: false,
    };
    match compute_agg(store, db, now, &args, AggOp::Inter) {
        Ok(pairs) => {
            let card = if limit == 0 {
                pairs.len()
            } else {
                pairs.len().min(limit)
            };
            Value::Integer(card as i64)
        }
        Err(()) => Value::error(ErrorReply::wrong_type()),
    }
}

/// The internal token the cross-shard coordinator uses to write a zset *STORE / ZRANGESTORE
/// result to the dest owner shard (COORDINATOR.md #107, Stage 2b-2). NOT a client command:
/// the decoder / router gate it so a client sending it gets `unknown command`; only the
/// coordinator issues it (via `dispatch_one_value` / `run_local_keyed`) after it has GATHERED
/// the sources cross-shard and COMBINED them with the shared [`zset_combine`] (or, for
/// ZRANGESTORE, gathered the selected range). See [`cmd_icstorezset`]. This is the zset
/// counterpart of [`crate::cmd_set::ICSTORESET`].
pub const ICSTOREZSET: &[u8] = b"__ICSTOREZSET";

/// INTERNAL: `__ICSTOREZSET dest m1 s1 m2 s2 ...` -> the dest cardinality. The dest-write
/// half of the cross-shard zset *STORE / ZRANGESTORE (COORDINATOR.md #107, Stage 2b-2): write
/// the supplied NON-EMPTY `(member, score)` pairs to `dest` on its OWNER shard with the EXACT
/// blind-overwrite-clearing-TTL semantics the single-shard [`agg_store_generic`] /
/// [`cmd_zrangestore`] use (`RmwAction::Insert(zset)` + `ExpireWrite::Clear`), so a spanning
/// ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE/ZRANGESTORE is byte-identical to the single-shard write.
/// The coordinator gathers + combines the sources itself (the shared [`zset_combine`]) and
/// routes the EMPTY-result case as a `DEL dest` (Redis deletes dest on an empty result), so
/// this verb is only ever issued with a non-empty pair list; it defensively DELETES dest on an
/// empty arg list too (so the empty case stays correct even if a future caller routes it
/// here). It is a single-key write keyed on `dest` (`args[1]`), so it routes + admits like any
/// keyed write through the existing substrate.
///
/// CLIENT-UNREACHABLE: this is gated out of the client command path (the serve-loop router and
/// the queue-time validator reject [`ICSTOREZSET`] before routing), so a client sending
/// `__ICSTOREZSET` gets the standard unknown-command error; only the coordinator reaches this
/// arm, via the internal `dispatch_remote_keyed` / `run_local_keyed` path.
// `store` (the param) and `score` (the parsed pair value) read as similar bindings; both are
// the clear, established names in the zset handlers, so the similar-names lint is allowed here.
#[allow(clippy::similar_names)]
pub fn cmd_icstorezset<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    // `__ICSTOREZSET dest [m s ...]`: at least the dest key (arity Min(2) in the registry).
    // The pairs are member/score couples, so the post-dest tail must have an EVEN length.
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("__icstorezset"));
    }
    let dest = req.args[1].clone();
    let tail = &req.args[2..];
    if tail.is_empty() {
        // Defensive: an empty pair list deletes dest (Redis deletes dest on an empty *STORE /
        // ZRANGESTORE result). The coordinator normally routes the empty case as `DEL dest`
        // and only issues this verb with pairs, but keep the semantics correct here too.
        store.delete(db, &dest, now);
        return Value::Integer(0);
    }
    if tail.len() % 2 != 0 {
        return Value::error(ErrorReply::syntax_error());
    }
    let mut pairs: Vec<(Vec<u8>, f64)> = Vec::with_capacity(tail.len() / 2);
    let mut i = 0;
    while i < tail.len() {
        let member = tail[i].to_vec();
        let Some(score) = parse_f64(&tail[i + 1]) else {
            return Value::error(ErrorReply::zadd_score_not_a_float());
        };
        pairs.push((member, score));
        i += 2;
    }
    let card = pairs.len() as i64;
    // Blind OVERWRITE of dest with the result zset, CLEARING any prior type/TTL -- the EXACT
    // single-shard *STORE / ZRANGESTORE write: Insert replaces any existing value/type,
    // ExpireWrite::Clear drops any prior TTL.
    store.rmw_mut(db, &dest, now, move |_entry| RmwStep {
        action: RmwAction::Insert(NewValueOwned::zset(pairs)),
        expire: ExpireWrite::Clear,
        reply: (),
    });
    Value::Integer(card)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_protocol::{ProtoVersion, encode_to_vec};
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

    /// The human score spelling from a scalar score reply, accepting BOTH a `Value::Double`
    /// (ZSCORE / ZMSCORE / ZINCRBY / ZADD INCR now reply a Double, which degrades to this
    /// same bulk spelling under RESP2) and a real bulk string (ZSCAN scores, the cursor
    /// token). `None` for nil. The explicit RESP2/RESP3 wire shapes are asserted separately
    /// by `scalar_score_resp2_bulk_resp3_double`.
    fn bulk_str(v: &Value) -> Option<String> {
        match v {
            Value::BulkString(Some(b)) => Some(String::from_utf8(b.to_vec()).unwrap()),
            Value::Double(d) => Some(format_human_double(*d)),
            Value::Null => None,
            other => panic!("expected a bulk, double, or nil, got {other:?}"),
        }
    }

    /// The member strings from a plain-array members reply.
    fn members(v: &Value) -> Vec<String> {
        match v {
            Value::Array(Some(items)) => items
                .iter()
                .map(|i| match i {
                    Value::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
                    other => panic!("non-bulk member: {other:?}"),
                })
                .collect(),
            other => panic!("expected an array, got {other:?}"),
        }
    }

    /// The (member, score) pairs from a WITHSCORES Value::Pairs reply.
    fn pairs(v: &Value) -> Vec<(String, f64)> {
        match v {
            Value::Pairs(p) => p
                .iter()
                .map(|(m, s)| {
                    let member = match m {
                        Value::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
                        other => panic!("non-bulk member: {other:?}"),
                    };
                    let score = match s {
                        Value::Double(d) => *d,
                        other => panic!("non-double score: {other:?}"),
                    };
                    (member, score)
                })
                .collect(),
            other => panic!("expected Value::Pairs, got {other:?}"),
        }
    }

    fn zadd(s: &mut TestStore, parts: &[&[u8]]) -> Value {
        cmd_zadd(s, 0, NOW, &req(parts))
    }

    // ---- ZADD matrix: counts, dedup, NX/XX/GT/LT/CH/INCR, conflicts, bad score. ----

    #[test]
    fn zadd_basic_count_and_type_and_score() {
        let mut s = test_store();
        assert_eq!(
            int(&zadd(&mut s, &[b"ZADD", b"z", b"1", b"a", b"2", b"b"])),
            2
        );
        assert_eq!(s.type_of(0, b"z", NOW), Some(DataType::ZSet));
        assert_eq!(int(&cmd_zcard(&mut s, 0, NOW, &req(&[b"ZCARD", b"z"]))), 2);
        assert_eq!(
            bulk_str(&cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"a"]))),
            Some("1".to_owned())
        );
        // Re-adding an existing member with a new score updates but is NOT counted (no CH).
        assert_eq!(int(&zadd(&mut s, &[b"ZADD", b"z", b"5", b"a"])), 0);
        assert_eq!(
            bulk_str(&cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"a"]))),
            Some("5".to_owned())
        );
        // CH counts the updated member.
        assert_eq!(int(&zadd(&mut s, &[b"ZADD", b"z", b"CH", b"6", b"a"])), 1);
    }

    #[test]
    fn zadd_nx_xx_gt_lt_and_incr() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"5", b"a"]);
        // NX never updates an existing member.
        assert_eq!(int(&zadd(&mut s, &[b"ZADD", b"z", b"NX", b"9", b"a"])), 0);
        assert_eq!(
            bulk_str(&cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"a"]))),
            Some("5".to_owned())
        );
        // XX never adds a new member.
        assert_eq!(int(&zadd(&mut s, &[b"ZADD", b"z", b"XX", b"1", b"new"])), 0);
        assert!(
            bulk_str(&cmd_zscore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZSCORE", b"z", b"new"])
            ))
            .is_none()
        );
        // GT only updates if greater: 3 < 5 -> no change.
        zadd(&mut s, &[b"ZADD", b"z", b"GT", b"3", b"a"]);
        assert_eq!(
            bulk_str(&cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"a"]))),
            Some("5".to_owned())
        );
        // GT updates if greater: 9 > 5.
        zadd(&mut s, &[b"ZADD", b"z", b"GT", b"9", b"a"]);
        assert_eq!(
            bulk_str(&cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"a"]))),
            Some("9".to_owned())
        );
        // INCR returns the new score as a bulk.
        assert_eq!(
            bulk_str(&zadd(&mut s, &[b"ZADD", b"z", b"INCR", b"1", b"a"])),
            Some("10".to_owned())
        );
        // INCR suppressed by NX on an existing member -> nil.
        assert_eq!(
            zadd(&mut s, &[b"ZADD", b"z", b"NX", b"INCR", b"1", b"a"]),
            Value::Null
        );
    }

    #[test]
    fn zadd_flag_conflicts_and_bad_score_are_byte_exact_errors() {
        let mut s = test_store();
        assert_eq!(
            err_line(&zadd(&mut s, &[b"ZADD", b"z", b"NX", b"GT", b"1", b"a"])),
            "-ERR GT, LT, and/or NX options at the same time are not compatible"
        );
        assert_eq!(
            err_line(&zadd(&mut s, &[b"ZADD", b"z", b"GT", b"LT", b"1", b"a"])),
            "-ERR GT, LT, and/or NX options at the same time are not compatible"
        );
        // NX + XX is the generic syntax error.
        assert_eq!(
            err_line(&zadd(&mut s, &[b"ZADD", b"z", b"NX", b"XX", b"1", b"a"])),
            "-ERR syntax error"
        );
        // INCR with multiple pairs.
        assert_eq!(
            err_line(&zadd(
                &mut s,
                &[b"ZADD", b"z", b"INCR", b"1", b"a", b"2", b"b"]
            )),
            "-ERR INCR option supports a single increment-element pair"
        );
        // A bad score.
        assert_eq!(
            err_line(&zadd(&mut s, &[b"ZADD", b"z", b"notanumber", b"a"])),
            "-ERR value is not a valid float"
        );
        // NaN is rejected as not-a-valid-float.
        assert_eq!(
            err_line(&zadd(&mut s, &[b"ZADD", b"z", b"nan", b"a"])),
            "-ERR value is not a valid float"
        );
    }

    #[test]
    fn zadd_inf_scores_allowed_and_ordered() {
        let mut s = test_store();
        zadd(
            &mut s,
            &[b"ZADD", b"z", b"+inf", b"hi", b"-inf", b"lo", b"0", b"mid"],
        );
        assert_eq!(
            members(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"z", b"0", b"-1"])
            )),
            vec!["lo", "mid", "hi"]
        );
        assert_eq!(
            bulk_str(&cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"hi"]))),
            Some("inf".to_owned())
        );
    }

    #[test]
    fn zadd_wrongtype_on_a_string_key() {
        let mut s = test_store();
        s.upsert(
            0,
            b"str",
            ironcache_storage::NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            NOW,
        );
        assert_eq!(
            err_line(&zadd(&mut s, &[b"ZADD", b"str", b"1", b"a"])),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    // ---- ZINCRBY / ZMSCORE. ----

    #[test]
    fn zincrby_and_zmscore() {
        let mut s = test_store();
        assert_eq!(
            bulk_str(&cmd_zincrby(
                &mut s,
                0,
                NOW,
                &req(&[b"ZINCRBY", b"z", b"2.5", b"a"])
            )),
            Some("2.5".to_owned())
        );
        assert_eq!(
            bulk_str(&cmd_zincrby(
                &mut s,
                0,
                NOW,
                &req(&[b"ZINCRBY", b"z", b"2.5", b"a"])
            )),
            Some("5".to_owned())
        );
        // ZMSCORE: present + absent.
        match cmd_zmscore(&mut s, 0, NOW, &req(&[b"ZMSCORE", b"z", b"a", b"missing"])) {
            Value::Array(Some(items)) => {
                assert_eq!(bulk_str(&items[0]), Some("5".to_owned()));
                assert_eq!(items[1], Value::Null);
            }
            other => panic!("ZMSCORE not an array: {other:?}"),
        }
        // ZMSCORE on a missing key -> all nil.
        assert_eq!(
            cmd_zmscore(&mut s, 0, NOW, &req(&[b"ZMSCORE", b"nope", b"a", b"b"])),
            Value::Array(Some(vec![Value::Null, Value::Null]))
        );
    }

    // ---- ZRANK / ZREVRANK (+WITHSCORE). ----

    #[test]
    fn zrank_revrank_withscore() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        assert_eq!(
            int(&cmd_zrank(&mut s, 0, NOW, &req(&[b"ZRANK", b"z", b"a"]))),
            0
        );
        assert_eq!(
            int(&cmd_zrank(&mut s, 0, NOW, &req(&[b"ZRANK", b"z", b"c"]))),
            2
        );
        assert_eq!(
            int(&cmd_zrevrank(
                &mut s,
                0,
                NOW,
                &req(&[b"ZREVRANK", b"z", b"a"])
            )),
            2
        );
        // Missing member -> nil.
        assert_eq!(
            cmd_zrank(&mut s, 0, NOW, &req(&[b"ZRANK", b"z", b"zzz"])),
            Value::Null
        );
        // WITHSCORE -> [rank, score].
        match cmd_zrank(&mut s, 0, NOW, &req(&[b"ZRANK", b"z", b"b", b"WITHSCORE"])) {
            Value::Array(Some(items)) => {
                assert_eq!(items[0], Value::Integer(1));
                assert_eq!(items[1], Value::Double(2.0));
            }
            other => panic!("WITHSCORE not an array: {other:?}"),
        }
    }

    // ---- ZRANGE family: index / BYSCORE / BYLEX / REV / LIMIT / WITHSCORES. ----

    #[test]
    fn zrange_index_rev_and_withscores() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        assert_eq!(
            members(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"z", b"0", b"-1"])
            )),
            vec!["a", "b", "c"]
        );
        // REV.
        assert_eq!(
            members(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"z", b"0", b"-1", b"REV"])
            )),
            vec!["c", "b", "a"]
        );
        // ZREVRANGE alias.
        assert_eq!(
            members(&cmd_zrevrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZREVRANGE", b"z", b"0", b"-1"])
            )),
            vec!["c", "b", "a"]
        );
        // WITHSCORES.
        assert_eq!(
            pairs(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"z", b"0", b"1", b"WITHSCORES"])
            )),
            vec![("a".to_owned(), 1.0), ("b".to_owned(), 2.0)]
        );
    }

    #[test]
    fn zrange_byscore_with_exclusive_inf_and_limit() {
        let mut s = test_store();
        zadd(
            &mut s,
            &[
                b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d",
            ],
        );
        // BYSCORE inclusive [2,3].
        assert_eq!(
            members(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"z", b"2", b"3", b"BYSCORE"])
            )),
            vec!["b", "c"]
        );
        // Exclusive lower (2 -> excludes b.
        assert_eq!(
            members(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"z", b"(2", b"+inf", b"BYSCORE"])
            )),
            vec!["c", "d"]
        );
        // Legacy ZRANGEBYSCORE with LIMIT.
        assert_eq!(
            members(&cmd_zrangebyscore(
                &mut s,
                0,
                NOW,
                &req(&[
                    b"ZRANGEBYSCORE",
                    b"z",
                    b"-inf",
                    b"+inf",
                    b"LIMIT",
                    b"1",
                    b"2"
                ])
            )),
            vec!["b", "c"]
        );
        // ZREVRANGEBYSCORE: max first.
        assert_eq!(
            members(&cmd_zrevrangebyscore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZREVRANGEBYSCORE", b"z", b"+inf", b"-inf"])
            )),
            vec!["d", "c", "b", "a"]
        );
        // Bad bound.
        assert_eq!(
            err_line(&cmd_zrangebyscore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGEBYSCORE", b"z", b"bad", b"3"])
            )),
            "-ERR min or max is not a float"
        );
    }

    #[test]
    fn zrange_bylex_inclusive_exclusive_neg_pos() {
        let mut s = test_store();
        // Equal scores for a lex range.
        zadd(
            &mut s,
            &[
                b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c", b"0", b"d",
            ],
        );
        // [b (d -> b, c.
        assert_eq!(
            members(&cmd_zrangebylex(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGEBYLEX", b"z", b"[b", b"(d"])
            )),
            vec!["b", "c"]
        );
        // - + -> all.
        assert_eq!(
            members(&cmd_zrangebylex(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGEBYLEX", b"z", b"-", b"+"])
            )),
            vec!["a", "b", "c", "d"]
        );
        // ZRANGE BYLEX REV.
        assert_eq!(
            members(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"z", b"+", b"-", b"BYLEX", b"REV"])
            )),
            vec!["d", "c", "b", "a"]
        );
        // Bad lex bound (missing [ or ().
        assert_eq!(
            err_line(&cmd_zrangebylex(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGEBYLEX", b"z", b"b", b"d"])
            )),
            "-ERR min or max not valid string range item"
        );
    }

    #[test]
    fn zcount_and_zlexcount() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        assert_eq!(
            int(&cmd_zcount(
                &mut s,
                0,
                NOW,
                &req(&[b"ZCOUNT", b"z", b"(1", b"3"])
            )),
            2
        );
        assert_eq!(
            int(&cmd_zcount(
                &mut s,
                0,
                NOW,
                &req(&[b"ZCOUNT", b"z", b"-inf", b"+inf"])
            )),
            3
        );
        let mut s2 = test_store();
        zadd(
            &mut s2,
            &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c"],
        );
        assert_eq!(
            int(&cmd_zlexcount(
                &mut s2,
                0,
                NOW,
                &req(&[b"ZLEXCOUNT", b"z", b"-", b"+"])
            )),
            3
        );
        assert_eq!(
            int(&cmd_zlexcount(
                &mut s2,
                0,
                NOW,
                &req(&[b"ZLEXCOUNT", b"z", b"[b", b"+"])
            )),
            2
        );
    }

    // ---- ZPOPMIN/ZPOPMAX (count + empty-deletes). ----

    #[test]
    fn zpopmin_zpopmax_count_and_empty_deletes() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        // ZPOPMIN (no count) -> lowest as [member, score].
        assert_eq!(
            pairs(&cmd_zpopmin(&mut s, 0, NOW, &req(&[b"ZPOPMIN", b"z"]))),
            vec![("a".to_owned(), 1.0)]
        );
        // ZPOPMAX count 5 -> remaining highest-first; drains the key.
        assert_eq!(
            pairs(&cmd_zpopmax(
                &mut s,
                0,
                NOW,
                &req(&[b"ZPOPMAX", b"z", b"5"])
            )),
            vec![("c".to_owned(), 3.0), ("b".to_owned(), 2.0)]
        );
        assert!(!s.contains(0, b"z", NOW), "draining deletes the key");
        assert_eq!(s.used_memory(), 0);
        // Missing key -> empty array.
        assert_eq!(
            cmd_zpopmin(&mut s, 0, NOW, &req(&[b"ZPOPMIN", b"z"])),
            Value::Array(Some(Vec::new()))
        );
    }

    // ---- ZREM + ZREMRANGEBY*. ----

    #[test]
    fn zrem_and_remrange_family() {
        let mut s = test_store();
        zadd(
            &mut s,
            &[
                b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d",
            ],
        );
        assert_eq!(
            int(&cmd_zrem(
                &mut s,
                0,
                NOW,
                &req(&[b"ZREM", b"z", b"a", b"zzz"])
            )),
            1
        );
        // ZREMRANGEBYRANK 0 0 -> removes the now-lowest (b).
        assert_eq!(
            int(&cmd_zremrangebyrank(
                &mut s,
                0,
                NOW,
                &req(&[b"ZREMRANGEBYRANK", b"z", b"0", b"0"])
            )),
            1
        );
        // ZREMRANGEBYSCORE (3 +inf -> removes d (score 4); c (3) excluded.
        assert_eq!(
            int(&cmd_zremrangebyscore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZREMRANGEBYSCORE", b"z", b"(3", b"+inf"])
            )),
            1
        );
        assert_eq!(
            members(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"z", b"0", b"-1"])
            )),
            vec!["c"]
        );
        // Remove the last -> key gone.
        assert_eq!(
            int(&cmd_zremrangebyrank(
                &mut s,
                0,
                NOW,
                &req(&[b"ZREMRANGEBYRANK", b"z", b"0", b"-1"])
            )),
            1
        );
        assert!(!s.contains(0, b"z", NOW));
    }

    #[test]
    fn zremrangebylex_drains() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c"]);
        assert_eq!(
            int(&cmd_zremrangebylex(
                &mut s,
                0,
                NOW,
                &req(&[b"ZREMRANGEBYLEX", b"z", b"-", b"+"])
            )),
            3
        );
        assert!(!s.contains(0, b"z", NOW));
    }

    // ---- ZRANGESTORE. ----

    #[test]
    fn zrangestore_stores_and_empty_deletes_dst() {
        let mut s = test_store();
        zadd(
            &mut s,
            &[b"ZADD", b"src", b"1", b"a", b"2", b"b", b"3", b"c"],
        );
        assert_eq!(
            int(&cmd_zrangestore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGESTORE", b"dst", b"src", b"0", b"1"])
            )),
            2
        );
        assert_eq!(
            members(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"dst", b"0", b"-1"])
            )),
            vec!["a", "b"]
        );
        // Pre-populate dst, then an empty range result deletes it.
        zadd(&mut s, &[b"ZADD", b"dst2", b"1", b"x"]);
        assert_eq!(
            int(&cmd_zrangestore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGESTORE", b"dst2", b"src", b"(10", b"+inf", b"BYSCORE"])
            )),
            0
        );
        assert!(!s.contains(0, b"dst2", NOW), "empty result deletes dst");
    }

    // ---- Aggregations: ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE + WEIGHTS + AGGREGATE. ----

    #[test]
    fn zunionstore_weights_aggregate_and_empty_deletes() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"a", b"1", b"x", b"2", b"y"]);
        zadd(&mut s, &[b"ZADD", b"b", b"10", b"y", b"20", b"z"]);
        // SUM (default): x=1, y=2+10=12, z=20.
        assert_eq!(
            int(&cmd_zunionstore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZUNIONSTORE", b"dest", b"2", b"a", b"b"])
            )),
            3
        );
        assert_eq!(
            bulk_str(&cmd_zscore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZSCORE", b"dest", b"y"])
            )),
            Some("12".to_owned())
        );
        // WEIGHTS 2 3: x=2, y=4+30=34, z=60.
        cmd_zunionstore(
            &mut s,
            0,
            NOW,
            &req(&[
                b"ZUNIONSTORE",
                b"dest",
                b"2",
                b"a",
                b"b",
                b"WEIGHTS",
                b"2",
                b"3",
            ]),
        );
        assert_eq!(
            bulk_str(&cmd_zscore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZSCORE", b"dest", b"y"])
            )),
            Some("34".to_owned())
        );
        // AGGREGATE MAX: y = max(2, 10) = 10.
        cmd_zunionstore(
            &mut s,
            0,
            NOW,
            &req(&[
                b"ZUNIONSTORE",
                b"dest",
                b"2",
                b"a",
                b"b",
                b"AGGREGATE",
                b"MAX",
            ]),
        );
        assert_eq!(
            bulk_str(&cmd_zscore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZSCORE", b"dest", b"y"])
            )),
            Some("10".to_owned())
        );
    }

    #[test]
    fn zinterstore_zdiffstore_and_intercard() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"a", b"1", b"x", b"2", b"y", b"3", b"z"]);
        zadd(
            &mut s,
            &[b"ZADD", b"b", b"10", b"y", b"20", b"z", b"30", b"w"],
        );
        // INTER: y, z.
        assert_eq!(
            int(&cmd_zinterstore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZINTERSTORE", b"i", b"2", b"a", b"b"])
            )),
            2
        );
        assert_eq!(
            members(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"i", b"0", b"-1"])
            )),
            vec!["y", "z"]
        );
        // DIFF a - b: x only.
        assert_eq!(
            int(&cmd_zdiffstore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZDIFFSTORE", b"d", b"2", b"a", b"b"])
            )),
            1
        );
        assert_eq!(
            members(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"d", b"0", b"-1"])
            )),
            vec!["x"]
        );
        // ZINTERCARD.
        assert_eq!(
            int(&cmd_zintercard(
                &mut s,
                0,
                NOW,
                &req(&[b"ZINTERCARD", b"2", b"a", b"b"])
            )),
            2
        );
        assert_eq!(
            int(&cmd_zintercard(
                &mut s,
                0,
                NOW,
                &req(&[b"ZINTERCARD", b"2", b"a", b"b", b"LIMIT", b"1"])
            )),
            1
        );
        // Empty intersection deletes the dest.
        zadd(&mut s, &[b"ZADD", b"only", b"1", b"q"]);
        zadd(&mut s, &[b"ZADD", b"pre", b"9", b"keep"]);
        assert_eq!(
            int(&cmd_zinterstore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZINTERSTORE", b"pre", b"2", b"a", b"only"])
            )),
            0
        );
        assert!(
            !s.contains(0, b"pre", NOW),
            "empty inter result deletes dest"
        );
    }

    #[test]
    fn zunion_zdiff_withscores_reply() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"a", b"1", b"x", b"2", b"y"]);
        zadd(&mut s, &[b"ZADD", b"b", b"10", b"y"]);
        // ZUNION WITHSCORES -> a Value::Pairs ordered by (score, member): x=1, y=12.
        assert_eq!(
            pairs(&cmd_zunion(
                &mut s,
                0,
                NOW,
                &req(&[b"ZUNION", b"2", b"a", b"b", b"WITHSCORES"])
            )),
            vec![("x".to_owned(), 1.0), ("y".to_owned(), 12.0)]
        );
        // ZDIFF a - b -> x.
        assert_eq!(
            members(&cmd_zdiff(
                &mut s,
                0,
                NOW,
                &req(&[b"ZDIFF", b"2", b"a", b"b"])
            )),
            vec!["x"]
        );
        // Bad weight.
        assert_eq!(
            err_line(&cmd_zunion(
                &mut s,
                0,
                NOW,
                &req(&[b"ZUNION", b"2", b"a", b"b", b"WEIGHTS", b"nan", b"1"])
            )),
            "-ERR weight value is not a float"
        );
    }

    // ---- ZSCAN reuse + small-collection one-shot + determinism. ----

    #[test]
    fn zscan_small_returns_all_at_cursor_zero() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        match cmd_zscan(&mut s, 0, NOW, &req(&[b"ZSCAN", b"z", b"0"])) {
            Value::Array(Some(items)) => {
                assert_eq!(bulk_str(&items[0]), Some("0".to_owned()), "complete cursor");
                let inner = match &items[1] {
                    Value::Array(Some(v)) => v,
                    other => panic!("inner not array: {other:?}"),
                };
                // member + score interleaved -> 6 elements for 3 members.
                assert_eq!(inner.len(), 6);
            }
            other => panic!("ZSCAN not the 2-element reply: {other:?}"),
        }
    }

    #[test]
    fn zscan_large_cursored_visits_every_member_once_deterministically() {
        let mut s = test_store();
        // Force the skiplist form (>128 members).
        for i in 0..200 {
            zadd(
                &mut s,
                &[
                    b"ZADD",
                    b"z",
                    i.to_string().as_bytes(),
                    format!("m{i:04}").as_bytes(),
                ],
            );
        }
        // OBJECT ENCODING would report skiplist; drive the cursor to completion twice and
        // assert the same full set of members both times (determinism + resize-invariance).
        let collect_all = |s: &mut TestStore| -> Vec<String> {
            let mut seen = Vec::new();
            let mut cursor = b"0".to_vec();
            loop {
                let reply = cmd_zscan(s, 0, NOW, &req(&[b"ZSCAN", b"z", &cursor, b"COUNT", b"7"]));
                let items = match reply {
                    Value::Array(Some(v)) => v,
                    other => panic!("not array: {other:?}"),
                };
                cursor = match &items[0] {
                    Value::BulkString(Some(b)) => b.to_vec(),
                    other => panic!("cursor: {other:?}"),
                };
                if let Value::Array(Some(inner)) = &items[1] {
                    let mut i = 0;
                    while i < inner.len() {
                        if let Value::BulkString(Some(m)) = &inner[i] {
                            seen.push(String::from_utf8(m.to_vec()).unwrap());
                        }
                        i += 2; // skip the score
                    }
                }
                if cursor == b"0" {
                    break;
                }
            }
            seen.sort();
            seen
        };
        let first = collect_all(&mut s);
        let second = collect_all(&mut s);
        assert_eq!(first.len(), 200, "every member visited once");
        assert_eq!(first, second, "ZSCAN is deterministic across replays");
    }

    #[test]
    fn zrandmember_determinism_distinct_repeats_withscores() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        // Same seed -> byte-identical reply (determinism, ADR-0003).
        let r1 = cmd_zrandmember(&mut s, 0, SEED, NOW, &req(&[b"ZRANDMEMBER", b"z", b"2"]));
        let r2 = cmd_zrandmember(&mut s, 0, SEED, NOW, &req(&[b"ZRANDMEMBER", b"z", b"2"]));
        assert_eq!(r1, r2, "seeded ZRANDMEMBER replays identically");
        // +count distinct: count 10 > card 3 -> exactly 3 distinct.
        let mut distinct = members(&cmd_zrandmember(
            &mut s,
            0,
            SEED,
            NOW,
            &req(&[b"ZRANDMEMBER", b"z", b"10"]),
        ));
        distinct.sort();
        assert_eq!(distinct, vec!["a", "b", "c"]);
        // -count with repeats: exactly |count|.
        assert_eq!(
            members(&cmd_zrandmember(
                &mut s,
                0,
                SEED,
                NOW,
                &req(&[b"ZRANDMEMBER", b"z", b"-5"])
            ))
            .len(),
            5
        );
        // WITHSCORES -> Value::Pairs.
        match cmd_zrandmember(
            &mut s,
            0,
            SEED,
            NOW,
            &req(&[b"ZRANDMEMBER", b"z", b"2", b"WITHSCORES"]),
        ) {
            Value::Pairs(p) => assert_eq!(p.len(), 2),
            other => panic!("WITHSCORES should be Value::Pairs: {other:?}"),
        }
        // No count -> a single bulk; the zset is not modified.
        assert!(
            bulk_str(&cmd_zrandmember(
                &mut s,
                0,
                SEED,
                NOW,
                &req(&[b"ZRANDMEMBER", b"z"])
            ))
            .is_some()
        );
        assert_eq!(int(&cmd_zcard(&mut s, 0, NOW, &req(&[b"ZCARD", b"z"]))), 3);
    }

    // ---- WITHSCORES RESP2 flat vs RESP3 nested-pairs (encode in both modes). ----

    #[test]
    fn withscores_resp2_flat_resp3_nested() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"1", b"a", b"2", b"b"]);
        let reply = cmd_zrange(
            &mut s,
            0,
            NOW,
            &req(&[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"]),
        );
        assert!(matches!(reply, Value::Pairs(ref p) if p.len() == 2));
        // RESP2: a single flat array [a, 1, b, 2] (scores degrade to bulk strings).
        let resp2 = encode_to_vec(&reply, ProtoVersion::Resp2);
        assert_eq!(resp2, b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");
        // RESP3: an array of [member, ,double] 2-arrays.
        let resp3 = encode_to_vec(&reply, ProtoVersion::Resp3);
        assert_eq!(
            resp3,
            b"*2\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2\r\n"
        );
    }

    // ---- Scalar score replies: RESP3 Double (`,`) vs RESP2 bulk string. ----

    #[test]
    #[allow(clippy::float_cmp)] // exact binary-representable scores; the value IS the assert.
    fn scalar_score_resp2_bulk_resp3_double() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"3.5", b"a"]);
        // ZSCORE: a Double value -> RESP3 `,3.5`, RESP2 bulk `$3\r\n3.5`.
        let score = cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"a"]));
        assert!(matches!(score, Value::Double(d) if d == 3.5));
        assert_eq!(encode_to_vec(&score, ProtoVersion::Resp3), b",3.5\r\n");
        assert_eq!(encode_to_vec(&score, ProtoVersion::Resp2), b"$3\r\n3.5\r\n");
        // ZINCRBY: the new score, same Double shape.
        let inc = cmd_zincrby(&mut s, 0, NOW, &req(&[b"ZINCRBY", b"z", b"1.5", b"a"]));
        assert!(matches!(inc, Value::Double(d) if d == 5.0));
        assert_eq!(encode_to_vec(&inc, ProtoVersion::Resp3), b",5\r\n");
        assert_eq!(encode_to_vec(&inc, ProtoVersion::Resp2), b"$1\r\n5\r\n");
        // ZADD ... INCR: also a Double.
        let zadd_inc = zadd(&mut s, &[b"ZADD", b"z", b"INCR", b"2", b"a"]);
        assert!(matches!(zadd_inc, Value::Double(d) if d == 7.0));
        assert_eq!(encode_to_vec(&zadd_inc, ProtoVersion::Resp3), b",7\r\n");
        // +inf scores encode as `,inf` (RESP3) / `$3\r\ninf` (RESP2).
        zadd(&mut s, &[b"ZADD", b"z", b"inf", b"big"]);
        let inf = cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"big"]));
        assert_eq!(encode_to_vec(&inf, ProtoVersion::Resp3), b",inf\r\n");
        assert_eq!(encode_to_vec(&inf, ProtoVersion::Resp2), b"$3\r\ninf\r\n");
    }

    #[test]
    fn zscan_scores_stay_bulk_strings_in_both_protocols() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"3.5", b"a"]);
        let reply = cmd_zscan(&mut s, 0, NOW, &req(&[b"ZSCAN", b"z", b"0"]));
        let inner = match &reply {
            Value::Array(Some(items)) => match &items[1] {
                Value::Array(Some(v)) => v.clone(),
                other => panic!("inner not array: {other:?}"),
            },
            other => panic!("ZSCAN not the 2-element reply: {other:?}"),
        };
        // [member, score]; the score stays a BULK STRING (NOT a Double) in BOTH protocols.
        assert!(matches!(&inner[1], Value::BulkString(Some(_))));
        assert_eq!(
            encode_to_vec(&inner[1], ProtoVersion::Resp3),
            b"$3\r\n3.5\r\n"
        );
        assert_eq!(
            encode_to_vec(&inner[1], ProtoVersion::Resp2),
            b"$3\r\n3.5\r\n"
        );
    }

    // ---- A NaN arithmetic RESULT is rejected without mutating (ZINCRBY / ZADD INCR). ----

    #[test]
    fn zincrby_nan_result_errors_and_does_not_mutate() {
        let mut s = test_store();
        // ZADD k INCR inf m -> m=+inf, then ZINCRBY k -inf m -> NaN error, m UNCHANGED.
        assert!(matches!(
            zadd(&mut s, &[b"ZADD", b"z", b"INCR", b"inf", b"m"]),
            Value::Double(d) if d.is_infinite() && d > 0.0
        ));
        assert_eq!(
            err_line(&cmd_zincrby(
                &mut s,
                0,
                NOW,
                &req(&[b"ZINCRBY", b"z", b"-inf", b"m"])
            )),
            "-ERR resulting score is not a number (NaN)"
        );
        // The member is UNCHANGED at +inf (no NaN stored).
        let score = cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"m"]));
        assert!(matches!(score, Value::Double(d) if d.is_infinite() && d > 0.0));
    }

    #[test]
    fn zadd_incr_nan_result_errors_and_does_not_mutate() {
        let mut s = test_store();
        // ZADD k INCR -inf m on a +inf member -> NaN error, m UNCHANGED at +inf.
        zadd(&mut s, &[b"ZADD", b"z", b"inf", b"m"]);
        assert_eq!(
            err_line(&zadd(&mut s, &[b"ZADD", b"z", b"INCR", b"-inf", b"m"])),
            "-ERR resulting score is not a number (NaN)"
        );
        let score = cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"m"]));
        assert!(matches!(score, Value::Double(d) if d.is_infinite() && d > 0.0));
        // The symmetric case: a -inf member, INCR by +inf -> NaN error, unchanged.
        zadd(&mut s, &[b"ZADD", b"z2", b"-inf", b"m"]);
        assert_eq!(
            err_line(&zadd(&mut s, &[b"ZADD", b"z2", b"INCR", b"inf", b"m"])),
            "-ERR resulting score is not a number (NaN)"
        );
        let s2 = cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z2", b"m"]));
        assert!(matches!(s2, Value::Double(d) if d.is_infinite() && d < 0.0));
    }

    // ---- Competitor-regression lock-in: ZADD/ZINCRBY reject NaN scores (input and result). ----

    #[test]
    fn zadd_and_zincrby_reject_nan_scores() {
        // Class of bug: a competitor accepted a NaN score (parsed as INPUT, or produced by
        // ARITHMETIC) and then CRASHED converting the zset to a skiplist, since NaN breaks the
        // total score order the skiplist relies on. Our defense: `parse_f64` rejects a NaN
        // INPUT (`value is not a valid float`), and a NaN ARITHMETIC RESULT (e.g. +inf
        // incremented by -inf) is rejected with `resulting score is not a number (NaN)` WITHOUT
        // mutating, so a NaN never reaches the ordered structure.
        let mut s = test_store();
        // A NaN input score is rejected at parse time (never stored).
        assert_eq!(
            err_line(&zadd(&mut s, &[b"ZADD", b"z", b"nan", b"m"])),
            "-ERR value is not a valid float"
        );
        // A ZINCRBY whose result would be NaN (+inf then -inf) is rejected; member unchanged.
        zadd(&mut s, &[b"ZADD", b"z", b"inf", b"m"]);
        assert_eq!(
            err_line(&cmd_zincrby(
                &mut s,
                0,
                NOW,
                &req(&[b"ZINCRBY", b"z", b"-inf", b"m"])
            )),
            "-ERR resulting score is not a number (NaN)"
        );
        // The member keeps its +inf score (no NaN was stored).
        assert!(matches!(
            cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"m"])),
            Value::Double(d) if d.is_infinite() && d > 0.0
        ));
        // A normal finite score still works.
        assert_eq!(int(&zadd(&mut s, &[b"ZADD", b"z2", b"1.5", b"m"])), 1);
        assert_eq!(
            bulk_str(&cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z2", b"m"]))),
            Some("1.5".to_owned())
        );
    }

    // ---- Aggregate NaN -> 0 coercion (SUM of +inf/-inf; WEIGHTS 0 * inf). ----

    #[test]
    #[allow(clippy::float_cmp)] // exact 0.0 coercion result; the value IS the assert.
    fn zunionstore_sum_inf_minus_inf_coerces_nan_to_zero() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"a", b"inf", b"m"]);
        zadd(&mut s, &[b"ZADD", b"b", b"-inf", b"m"]);
        // ZUNIONSTORE dest 2 a b AGGREGATE SUM -> m's score is 0 (NaN coerced), not NaN.
        assert_eq!(
            int(&cmd_zunionstore(
                &mut s,
                0,
                NOW,
                &req(&[
                    b"ZUNIONSTORE",
                    b"dest",
                    b"2",
                    b"a",
                    b"b",
                    b"AGGREGATE",
                    b"SUM"
                ])
            )),
            1
        );
        let score = cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"dest", b"m"]));
        assert!(matches!(score, Value::Double(d) if d == 0.0 && !d.is_nan()));
    }

    #[test]
    #[allow(clippy::float_cmp)] // exact 0.0 coercion result; the value IS the assert.
    fn zunionstore_weight_zero_times_inf_coerces_nan_to_zero() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"a", b"inf", b"m"]);
        // WEIGHTS 0 against an inf score: 0 * inf = NaN -> coerced to 0.
        assert_eq!(
            int(&cmd_zunionstore(
                &mut s,
                0,
                NOW,
                &req(&[b"ZUNIONSTORE", b"dest", b"1", b"a", b"WEIGHTS", b"0"])
            )),
            1
        );
        let score = cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"dest", b"m"]));
        assert!(matches!(score, Value::Double(d) if d == 0.0 && !d.is_nan()));
    }

    // ---- ZADD create-path duplicate-member GT/LT gates like the in-place path. ----

    #[test]
    fn zadd_create_path_gt_gates_repeated_member() {
        let mut s = test_store();
        // Fresh key: GT 2 m then 1 m. The first lands m=2; the repeat 1 < 2 fails the GT
        // gate, so m stays 2 (NOT last-write-wins 1).
        zadd(&mut s, &[b"ZADD", b"z", b"GT", b"2", b"m", b"1", b"m"]);
        assert_eq!(
            bulk_str(&cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z", b"m"]))),
            Some("2".to_owned())
        );
        // LT mirrors: LT 1 m then 2 m -> the repeat 2 > 1 fails the LT gate, m stays 1.
        zadd(&mut s, &[b"ZADD", b"z2", b"LT", b"1", b"m", b"2", b"m"]);
        assert_eq!(
            bulk_str(&cmd_zscore(&mut s, 0, NOW, &req(&[b"ZSCORE", b"z2", b"m"]))),
            Some("1".to_owned())
        );
    }

    // ---- *STORE rejects WITHSCORES; ZRANGE BYLEX+WITHSCORES; ZRANGESTORE LIMIT. ----

    #[test]
    fn store_aggregations_reject_withscores() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"a", b"1", b"x"]);
        for cmd in [
            &[b"ZUNIONSTORE".as_slice(), b"d", b"1", b"a", b"WITHSCORES"][..],
            &[b"ZINTERSTORE".as_slice(), b"d", b"1", b"a", b"WITHSCORES"][..],
            &[b"ZDIFFSTORE".as_slice(), b"d", b"1", b"a", b"WITHSCORES"][..],
        ] {
            let v = match cmd[0] {
                b"ZUNIONSTORE" => cmd_zunionstore(&mut s, 0, NOW, &req(cmd)),
                b"ZINTERSTORE" => cmd_zinterstore(&mut s, 0, NOW, &req(cmd)),
                _ => cmd_zdiffstore(&mut s, 0, NOW, &req(cmd)),
            };
            assert_eq!(err_line(&v), "-ERR syntax error");
        }
        // The READ forms STILL accept WITHSCORES (sanity: not over-rejected).
        assert!(matches!(
            cmd_zunion(
                &mut s,
                0,
                NOW,
                &req(&[b"ZUNION", b"1", b"a", b"WITHSCORES"])
            ),
            Value::Pairs(_)
        ));
    }

    #[test]
    fn zrange_bylex_withscores_is_specific_syntax_error() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"0", b"a"]);
        assert_eq!(
            err_line(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"z", b"-", b"+", b"BYLEX", b"WITHSCORES"])
            )),
            "-ERR syntax error, WITHSCORES not supported in combination with BYLEX"
        );
    }

    #[test]
    fn zrange_and_zrangestore_limit_without_by_is_specific_syntax_error() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"0", b"a"]);
        let expect = "-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX";
        assert_eq!(
            err_line(&cmd_zrange(
                &mut s,
                0,
                NOW,
                &req(&[b"ZRANGE", b"z", b"0", b"-1", b"LIMIT", b"0", b"1"])
            )),
            expect
        );
        assert_eq!(
            err_line(&cmd_zrangestore(
                &mut s,
                0,
                NOW,
                &req(&[
                    b"ZRANGESTORE",
                    b"d",
                    b"z",
                    b"0",
                    b"-1",
                    b"LIMIT",
                    b"0",
                    b"1"
                ])
            )),
            expect
        );
    }

    // ---- OBJECT ENCODING transition via the store + TYPE. ----

    #[test]
    fn encoding_transition_listpack_to_skiplist() {
        let mut s = test_store();
        zadd(&mut s, &[b"ZADD", b"z", b"1", b"a"]);
        assert_eq!(
            s.read(0, b"z", NOW).unwrap().encoding().encoding_name(),
            "listpack"
        );
        // A member over the 64-byte cap flips to skiplist.
        let big = vec![b'q'; 65];
        cmd_zadd(&mut s, 0, NOW, &req(&[b"ZADD", b"z", b"2", &big]));
        assert_eq!(
            s.read(0, b"z", NOW).unwrap().encoding().encoding_name(),
            "skiplist"
        );
        assert_eq!(s.type_of(0, b"z", NOW), Some(DataType::ZSet));
    }
}
