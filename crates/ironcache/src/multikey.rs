// SPDX-License-Identifier: MIT OR Apache-2.0
//! Home-core SCATTER-GATHER for the SHARD-SPANNING multi-key DATA commands
//! (COORDINATOR.md #107, coordinator Stage 2a).
//!
//! Stage 1 routes a multi-key command whose keys ALL co-locate on one shard to that
//! shard (a single hop / the local fast path). A key-SPANNING invocation (the keys land
//! on more than one shard, so [`ironcache_server::owner_shard_set`] returns `None`) used
//! to fall to the home-only sync path -- correct only for the home shard's subset of the
//! keys (the documented Stage 2 gap). This module fills that gap for exactly SIX commands:
//!
//! - **MGET key [key ...]**: per shard, a sub-`MGET` of ONLY that shard's keys; each
//!   sub-reply is an array of values in the sub-request's key order. Reassemble into one
//!   array of length = num keys, placing each value back at its key's ORIGINAL argument
//!   index (NOT shard order). A shard-unavailable sub-reply -> that shard's keys are Null
//!   (degraded; shutdown only). Returns [`Value::Array`].
//! - **MSET key value [key value ...]**: group the (key, value) PAIRS by `owner(key)`;
//!   per shard a sub-`MSET` of its pairs (flattened). Returns `+OK` when every shard
//!   succeeds; per-shard atomic, NO global barrier (see "Atomicity" below). A non-OK
//!   sub-reply (a shard-unavailable Error, or a genuine `-OOM` from a shard's per-shard
//!   denyoom gate) is SURFACED.
//! - **DEL / EXISTS / UNLINK / TOUCH key [key ...]**: group keys by `owner(key)`; per
//!   shard a sub-`CMD` of its keys; each sub-reply is an Integer; SUM them into one
//!   Integer. A shard-unavailable sub-reply contributes 0 (degraded; shutdown only).
//!
//! ## Atomicity (MSET is NOT cross-shard atomic)
//!
//! Each shard's sub-MSET applies atomically ON THAT SHARD, but the fan-out has NO global
//! barrier across shards: a reader on shard B can observe B's pairs before A's pairs land
//! (the sub-requests run concurrently on independent shard threads). A second consequence:
//! because `MSET` is `denyoom` and admission runs PER SHARD, a spanning `MSET` under
//! maxmemory can PARTIALLY apply (some shards commit) and STILL reply `-OOM` (a shard over
//! budget rejects its subset) -- single-node Redis instead checks `denyoom` once before any
//! write, so its `MSET` is all-or-nothing. Both divergences are the same Stage 2a no-barrier
//! limitation. A TRUE cross-shard-atomic multi-key write (a pre-flight all-shards budget
//! check; the basis a correct MSETNX needs) is Stage 3 and is explicitly deferred. MSETNX is
//! therefore NOT implemented here (it would need the cross-shard atomicity Stage 3 provides).
//!
//! ## shards == 1 parity (byte-identical)
//!
//! With one shard every key is home-owned, so `owner_shard_set` ALWAYS returns `Some(0)`
//! and a multi-key command NEVER enters this spanning path -- it routes co-located via
//! Stage 1 (the local fast path). So this module is dormant at `shards == 1` and the wire
//! reply is byte-identical to the single-shard handler.

use crate::coordinator::{self, Inbox, ShardReply};
use ironcache_runtime::bootstrap::ShardId;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ProtoVersion, Request, Value, owner_shard};

/// One key's routing fact: its OWNER shard and its ORIGINAL position in the command's key
/// list (the index used to place its reply back in the requested order, MGET's invariant).
struct KeyAt {
    /// The shard that owns this key (`owner_shard(key, n_shards)`).
    shard: usize,
    /// The key's 0-based position among the command's KEYS (not its arg index): for MGET
    /// `MGET k0 k1 k2` these are 0,1,2, i.e. the output-array slot the value belongs in.
    pos: usize,
}

/// Group a command's keys by OWNER shard, building one per-shard sub-[`Request`] of the
/// keys that shard owns AND recording each key's ORIGINAL position so the caller can map
/// each shard's reply back to the requested order (COORDINATOR.md #107, Stage 2a).
///
/// `verb` is the command token (`b"MGET"` / `b"DEL"` / ...); `keys` are the command's
/// keys in their ORIGINAL order (already extracted by the caller, typically
/// `req.args[1..]`). Returns:
/// - `subreqs`: one `(shard, Request)` per shard that owns at least one key, each request
///   being `[verb, that shard's keys in their original relative order...]`. A shard owning
///   no key is absent (so [`coordinator::fan_out_split`] does not contact it).
/// - `positions`: for each `(shard, _)` in `subreqs` (SAME order), the ORIGINAL positions
///   of that shard's keys, in the SAME order they appear in the sub-request. So
///   `positions[i][j]` is the original position of the j-th key in `subreqs[i]`'s request.
///
/// Determinism: the per-shard key order is preserved as the keys' original RELATIVE order
/// (a stable bucketing), so the sub-request and its `positions` stay in lockstep.
fn group_by_owner(
    verb: &[u8],
    keys: &[bytes::Bytes],
    n_shards: usize,
) -> (Vec<(usize, Request)>, Vec<Vec<usize>>) {
    // Bucket each key into its owner shard, preserving original position. Using a Vec of
    // (shard, pos) then a stable partition keeps the per-shard key order = original order.
    let placed: Vec<KeyAt> = keys
        .iter()
        .enumerate()
        .map(|(pos, k)| KeyAt {
            shard: owner_shard(k, n_shards),
            pos,
        })
        .collect();

    // Collect the DISTINCT participating shards in first-seen order (deterministic).
    let mut shard_order: Vec<usize> = Vec::new();
    for p in &placed {
        if !shard_order.contains(&p.shard) {
            shard_order.push(p.shard);
        }
    }

    let mut subreqs: Vec<(usize, Request)> = Vec::with_capacity(shard_order.len());
    let mut positions: Vec<Vec<usize>> = Vec::with_capacity(shard_order.len());
    for &shard in &shard_order {
        // The sub-request: the verb + this shard's keys, in original relative order.
        let mut args: Vec<bytes::Bytes> = Vec::new();
        args.push(bytes::Bytes::copy_from_slice(verb));
        let mut pos_for_shard: Vec<usize> = Vec::new();
        for p in &placed {
            if p.shard == shard {
                args.push(keys[p.pos].clone());
                pos_for_shard.push(p.pos);
            }
        }
        subreqs.push((shard, Request { args }));
        positions.push(pos_for_shard);
    }
    (subreqs, positions)
}

/// Group MSET's (key, value) PAIRS by `owner(key)`, building one per-shard sub-`MSET` of
/// that shard's pairs flattened (`[MSET, k, v, k, v, ...]`). Mirrors [`group_by_owner`]
/// but carries the value alongside each key so the pair stays together on its owner shard.
/// Pairs are bucketed in original relative order (deterministic). `pairs` are the flat
/// `[k0, v0, k1, v1, ...]` args (the caller has validated an even count).
fn group_pairs_by_owner(pairs: &[bytes::Bytes], n_shards: usize) -> Vec<(usize, Request)> {
    // The owner of each PAIR is the owner of its key (pairs[2i]).
    let n_pairs = pairs.len() / 2;
    let owners: Vec<usize> = (0..n_pairs)
        .map(|i| owner_shard(&pairs[2 * i], n_shards))
        .collect();

    let mut shard_order: Vec<usize> = Vec::new();
    for &o in &owners {
        if !shard_order.contains(&o) {
            shard_order.push(o);
        }
    }

    let mut subreqs: Vec<(usize, Request)> = Vec::with_capacity(shard_order.len());
    for &shard in &shard_order {
        let mut args: Vec<bytes::Bytes> = vec![bytes::Bytes::from_static(b"MSET")];
        for (i, &o) in owners.iter().enumerate() {
            if o == shard {
                args.push(pairs[2 * i].clone()); // key
                args.push(pairs[2 * i + 1].clone()); // value
            }
        }
        subreqs.push((shard, Request { args }));
    }
    subreqs
}

/// Reassemble per-shard sub-`MGET` array replies into ONE array of length `num_keys`,
/// placing each returned value back at its key's ORIGINAL position (NOT shard order) --
/// the order-preservation invariant the MGET fan-out exists to uphold (COORDINATOR.md
/// #107, Stage 2a).
///
/// `replies` are the `(shard, ShardReply)` pairs from [`coordinator::fan_out_split`];
/// `positions[i]` are the original positions of `subreqs[i]`'s keys (SAME order). A
/// shard-unavailable reply (degraded; shutdown only) leaves that shard's positions as Null.
/// A per-shard sub-MGET ALWAYS returns an array (its own `cmd_mget` never errors on type),
/// so a non-array reply can only be a shard-unavailable Error, handled as Null.
#[must_use]
fn reassemble_mget(
    num_keys: usize,
    subreqs: &[(usize, Request)],
    positions: &[Vec<usize>],
    replies: Vec<(usize, ShardReply)>,
) -> Value {
    // The output, pre-filled with Null so any position a degraded shard could not fill
    // stays Null (the documented shutdown degradation).
    let mut out: Vec<Value> = vec![Value::Null; num_keys];

    for (shard, reply) in replies {
        // Find this shard's index in subreqs to recover its key positions. fan_out_split
        // returns each shard at most once, so the first match is the one.
        let Some(idx) = subreqs.iter().position(|(s, _)| *s == shard) else {
            continue; // a shard not in subreqs cannot occur, but ignore defensively.
        };
        let pos_list = &positions[idx];
        // A per-shard sub-MGET ALWAYS returns an array; a non-array reply can only be a
        // shard-unavailable Error (degraded; shutdown only), whose positions stay Null.
        if let Value::Array(Some(values)) = reply.value {
            // Place each returned value back at its key's ORIGINAL position. The sub-reply
            // is in the sub-request's key order, which equals pos_list's order, so the
            // j-th value belongs at pos_list[j].
            for (j, v) in values.into_iter().enumerate() {
                if let Some(&orig) = pos_list.get(j) {
                    if orig < out.len() {
                        out[orig] = v;
                    }
                }
            }
        }
    }
    Value::Array(Some(out))
}

/// SUM per-shard Integer sub-replies into one Integer (DEL / EXISTS / UNLINK / TOUCH).
/// A shard-unavailable reply contributes 0 (degraded; shutdown only -- documented). A
/// GENUINE command Error (e.g. a wrong-arity reply, identical on every shard) is SURFACED
/// rather than summed (it cannot occur here -- the caller validates arity before fanning
/// out -- but the classifier is kept consistent with the whole-keyspace merges).
#[must_use]
fn sum_integers(replies: Vec<(usize, ShardReply)>) -> Value {
    if let Some((_, r)) = replies
        .iter()
        .find(|(_, r)| matches!(r.value, Value::Error(_)))
    {
        if let Value::Error(e) = &r.value {
            if !coordinator::is_shard_unavailable(e) {
                return Value::error(e.clone());
            }
        }
    }
    let mut total: i64 = 0;
    for (_, r) in replies {
        if let Value::Integer(n) = r.value {
            total = total.saturating_add(n);
        }
    }
    Value::Integer(total)
}

/// Merge per-shard sub-`MSET` `+OK` replies: `+OK` IFF every shard succeeded; otherwise
/// SURFACE the error so a client is never told `+OK` when a shard did not apply its pairs.
/// A sub-`MSET` non-OK reply can be EITHER a shard-unavailable Error (shutdown / shard
/// panic) OR a genuine `-OOM`: `MSET` is `denyoom`, and the admission gate runs PER SHARD,
/// so a shard over its budget rejects its sub-`MSET` with `-OOM`. We surface any error
/// uniformly (the right behavior). Note the divergence this exposes: because the fan-out
/// has NO global barrier (see the module-level "Atomicity" note) and admission is per-shard,
/// a spanning `MSET` can PARTIALLY apply on some shards and still reply `-OOM` -- unlike
/// single-node Redis, where `denyoom` is checked once before any write so `MSET` is
/// all-or-nothing. A pre-flight cross-shard budget check (all-or-nothing under maxmemory) is
/// deferred to the Stage 3 cross-shard-atomic write path.
#[must_use]
fn merge_mset(replies: &[(usize, ShardReply)]) -> Value {
    for (_, r) in replies {
        if let Value::Error(e) = &r.value {
            return Value::error(e.clone());
        }
    }
    Value::ok()
}

/// The home-core SCATTER-GATHER for a SHARD-SPANNING multi-key command (one of the SIX:
/// MGET / MSET / DEL / EXISTS / UNLINK / TOUCH), encoding the reassembled [`Value`] into
/// `out` with the home connection's `proto` (COORDINATOR.md #107, Stage 2a).
///
/// The home shard's subset runs LOCALLY + SYNCHRONOUSLY via [`coordinator::run_local_keyed`]
/// (the `local` closure); every other participating shard runs its subset via its drain
/// loop ([`dispatch_remote_keyed`](ironcache_server::dispatch_remote_keyed)).
/// [`coordinator::fan_out_split`] gathers the `(shard, reply)` pairs, then the per-command
/// reassembly runs. The keys are extracted directly here (`req.args[1..]`, or the strided
/// keys for MSET) -- the caller (the serve loop) has already classified this as a SPANNING
/// KeyedMulti command, so the args are well-formed for these six.
///
/// `cmd_upper` is the uppercased command token (the serve loop computed it for routing).
/// Each argument is a distinct orthogonal seam the fan-out threads through (mirroring the
/// whole-keyspace `fan_out_and_merge` shape); bundling them into a struct would only
/// obscure the per-call borrows, so the over-7-args lint is allowed here with that reason.
#[allow(clippy::too_many_arguments)]
pub async fn fan_out_multikey(
    inbox: &Inbox,
    ctx: &ServerContext,
    cmd_upper: &[u8],
    request: &Request,
    db: u32,
    home: ShardId,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    let n_shards = inbox.len();

    let merged = match cmd_upper {
        b"MGET" => {
            // Keys are args[1..]; one sub-MGET per owning shard, reassembled to original order.
            let keys = &request.args[1..];
            let (subreqs, positions) = group_by_owner(b"MGET", keys, n_shards);
            let replies =
                coordinator::fan_out_split(inbox, home, db, clone_subreqs(&subreqs), |r| {
                    coordinator::run_local_keyed(ctx, r, db)
                })
                .await;
            reassemble_mget(keys.len(), &subreqs, &positions, replies)
        }
        b"MSET" => {
            // Pairs are args[1..]; group by owner(key), one sub-MSET per owning shard.
            let pairs = &request.args[1..];
            let subreqs = group_pairs_by_owner(pairs, n_shards);
            let replies = coordinator::fan_out_split(inbox, home, db, subreqs, |r| {
                coordinator::run_local_keyed(ctx, r, db)
            })
            .await;
            merge_mset(&replies)
        }
        // DEL / EXISTS / UNLINK / TOUCH: keys are args[1..]; one sub-CMD per owning shard,
        // SUM the integer replies.
        b"DEL" | b"EXISTS" | b"UNLINK" | b"TOUCH" => {
            let keys = &request.args[1..];
            let (subreqs, _positions) = group_by_owner(cmd_upper, keys, n_shards);
            let replies = coordinator::fan_out_split(inbox, home, db, subreqs, |r| {
                coordinator::run_local_keyed(ctx, r, db)
            })
            .await;
            sum_integers(replies)
        }
        // The serve loop only routes the six supported commands here; any other token is a
        // routing bug. Reply a well-formed error rather than panicking.
        _ => Value::error(ironcache_protocol::ErrorReply::err(
            "non-fan-out multi-key command",
        )),
    };
    encode_into(out, &merged, proto);
}

/// Clone the per-shard sub-requests (cheap: `Request` is `Vec<Bytes>`, the `Bytes` are
/// refcounted), so the MGET path can keep the originals for the reassembly position lookup
/// while handing owned sub-requests to [`coordinator::fan_out_split`].
fn clone_subreqs(subreqs: &[(usize, Request)]) -> Vec<(usize, Request)> {
    subreqs.iter().map(|(s, r)| (*s, r.clone())).collect()
}

/// Encode `value` for `proto` and append to `out` (the home-core encode; mirrors the serve
/// loop / coordinator / whole_keyspace encode). Encoding stays on the home core with the
/// home proto.
fn encode_into(out: &mut Vec<u8>, value: &Value, proto: ProtoVersion) {
    let mut bm = bytes::BytesMut::with_capacity(64);
    ironcache_protocol::encode(&mut bm, value, proto);
    out.extend_from_slice(&bm);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_observe::CounterDeltas;
    use ironcache_protocol::ErrorReply;

    fn reply(value: Value) -> ShardReply {
        ShardReply {
            value,
            deltas: CounterDeltas::default(),
        }
    }

    fn bytes(parts: &[&[u8]]) -> Vec<bytes::Bytes> {
        parts
            .iter()
            .map(|p| bytes::Bytes::copy_from_slice(p))
            .collect()
    }

    fn bulk(s: &str) -> Value {
        Value::bulk(s.as_bytes().to_vec())
    }

    #[test]
    fn group_by_owner_partitions_keys_and_records_positions() {
        // Over n shards, keys are bucketed to their owner and each sub-request carries the
        // keys in original relative order with matching positions.
        let n = 4usize;
        let keys = bytes(&[b"a", b"b", b"c", b"d", b"e"]);
        let (subreqs, positions) = group_by_owner(b"MGET", &keys, n);
        // Every original key appears exactly once across the sub-requests, and the verb
        // leads each sub-request.
        let mut seen_positions: Vec<usize> = Vec::new();
        for ((shard, req), pos_list) in subreqs.iter().zip(positions.iter()) {
            assert_eq!(req.args[0].as_ref(), b"MGET", "verb leads sub-request");
            assert_eq!(req.args.len() - 1, pos_list.len(), "keys match positions");
            for (j, &orig) in pos_list.iter().enumerate() {
                // The j-th key in the sub-request is keys[orig], and orig's owner is shard.
                assert_eq!(req.args[1 + j], keys[orig]);
                assert_eq!(owner_shard(&keys[orig], n), *shard);
                seen_positions.push(orig);
            }
        }
        seen_positions.sort_unstable();
        assert_eq!(seen_positions, vec![0, 1, 2, 3, 4], "every key placed once");
    }

    #[test]
    fn reassemble_mget_restores_original_argument_order_not_shard_order() {
        // Two shards, keys interleaved: k0->shard A, k1->shard B, k2->shard A. The shard
        // replies arrive in arbitrary order; the reassembly must place each value at its
        // ORIGINAL position (k0, k1, k2), NOT grouped by shard.
        // Build subreqs/positions by hand to control the mapping precisely.
        let subreqs = vec![
            (
                0usize,
                Request {
                    args: bytes(&[b"MGET", b"k0", b"k2"]),
                },
            ),
            (
                1usize,
                Request {
                    args: bytes(&[b"MGET", b"k1"]),
                },
            ),
        ];
        let positions = vec![vec![0usize, 2usize], vec![1usize]];
        // Shard 0 returns [v0, v2] (its keys in order); shard 1 returns [v1]. Deliver them
        // OUT of shard order (shard 1 first) to prove order comes from positions, not arrival.
        let replies = vec![
            (1usize, reply(Value::Array(Some(vec![bulk("v1")])))),
            (
                0usize,
                reply(Value::Array(Some(vec![bulk("v0"), bulk("v2")]))),
            ),
        ];
        let merged = reassemble_mget(3, &subreqs, &positions, replies);
        let Value::Array(Some(items)) = merged else {
            panic!("MGET reassembly must be an array");
        };
        assert_eq!(
            items,
            vec![bulk("v0"), bulk("v1"), bulk("v2")],
            "original order"
        );
    }

    #[test]
    fn reassemble_mget_missing_and_unavailable_become_null() {
        // Shard 0 returns [v0, Null] (k2 absent); shard 1 is unavailable -> its key (k1)
        // stays Null. Output: [v0, Null, Null].
        let subreqs = vec![
            (
                0usize,
                Request {
                    args: bytes(&[b"MGET", b"k0", b"k2"]),
                },
            ),
            (
                1usize,
                Request {
                    args: bytes(&[b"MGET", b"k1"]),
                },
            ),
        ];
        let positions = vec![vec![0usize, 2usize], vec![1usize]];
        let replies = vec![
            (
                0usize,
                reply(Value::Array(Some(vec![bulk("v0"), Value::Null]))),
            ),
            (
                1usize,
                reply(Value::error(ErrorReply::err(
                    coordinator::SHARD_UNAVAILABLE_MSG,
                ))),
            ),
        ];
        let merged = reassemble_mget(3, &subreqs, &positions, replies);
        let Value::Array(Some(items)) = merged else {
            panic!("expected array");
        };
        assert_eq!(items, vec![bulk("v0"), Value::Null, Value::Null]);
    }

    #[test]
    fn group_pairs_by_owner_keeps_pairs_on_their_key_owner() {
        let n = 4usize;
        // MSET k1 v1 k2 v2 k3 v3
        let pairs = bytes(&[b"k1", b"v1", b"k2", b"v2", b"k3", b"v3"]);
        let subreqs = group_pairs_by_owner(&pairs, n);
        let mut seen_keys: Vec<Vec<u8>> = Vec::new();
        for (shard, req) in &subreqs {
            assert_eq!(req.args[0].as_ref(), b"MSET");
            // The sub-request is [MSET, k, v, k, v, ...]: an odd number of trailing args
            // would be a flatten bug.
            assert_eq!((req.args.len() - 1) % 2, 0, "flattened pairs stay even");
            let mut i = 1;
            while i + 1 < req.args.len() {
                let key = &req.args[i];
                assert_eq!(owner_shard(key, n), *shard, "pair lives on its key's owner");
                seen_keys.push(key.to_vec());
                i += 2;
            }
        }
        seen_keys.sort();
        assert_eq!(
            seen_keys,
            vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
            "every pair placed once"
        );
    }

    #[test]
    fn sum_integers_sums_and_treats_unavailable_as_zero() {
        let replies = vec![
            (0usize, reply(Value::Integer(2))),
            (1usize, reply(Value::Integer(3))),
        ];
        assert_eq!(sum_integers(replies), Value::Integer(5));

        let with_dead = vec![
            (0usize, reply(Value::Integer(4))),
            (
                1usize,
                reply(Value::error(ErrorReply::err(
                    coordinator::SHARD_UNAVAILABLE_MSG,
                ))),
            ),
        ];
        assert_eq!(sum_integers(with_dead), Value::Integer(4));
    }

    #[test]
    fn merge_mset_ok_unless_a_shard_unavailable() {
        let all_ok = vec![(0usize, reply(Value::ok())), (1usize, reply(Value::ok()))];
        assert_eq!(merge_mset(&all_ok), Value::ok());

        let one_dead = vec![
            (0usize, reply(Value::ok())),
            (
                1usize,
                reply(Value::error(ErrorReply::err(
                    coordinator::SHARD_UNAVAILABLE_MSG,
                ))),
            ),
        ];
        assert!(matches!(merge_mset(&one_dead), Value::Error(_)));
    }
}
