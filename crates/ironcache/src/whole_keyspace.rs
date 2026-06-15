// SPDX-License-Identifier: MIT OR Apache-2.0
//! Home-core SCATTER-GATHER for the whole-keyspace commands (COORDINATOR.md #107, the
//! final piece of coordinator Stage 1).
//!
//! [`CommandClass::WholeKeyspace`] commands (KEYS / SCAN / DBSIZE / FLUSHALL / FLUSHDB /
//! RANDOMKEY) used to run HOME-ONLY, so after the keyspace was PARTITIONED across shards
//! (ADR-0002) they covered only the home shard's ~1/N of keys. This module fans them out
//! across ALL shards and MERGES the per-shard partials so they cover the WHOLE keyspace
//! again (no regression):
//!
//! - **DBSIZE**: [`fan_out_all`] -> SUM the per-shard integer counts -> one Integer.
//! - **KEYS pattern**: [`fan_out_all`] -> CONCATENATE the per-shard key arrays (order is
//!   irrelevant; Redis gives no KEYS order guarantee). Each shard already glob-filtered.
//! - **FLUSHDB / FLUSHALL**: [`fan_out_all`] (broadcast) -> `+OK` IFF every shard
//!   succeeded; any shard error (e.g. shard-unavailable) surfaces as the error.
//! - **RANDOMKEY**: [`fan_out_all`] -> collect the NON-nil per-shard keys -> pick ONE
//!   UNIFORMLY among the shards-that-have-a-key using the HOME shard's Env RNG seam
//!   (ADR-0003; NO std rand), or nil if every shard was empty.
//! - **SCAN cursor [MATCH] [COUNT] [TYPE]**: NOT a broadcast. A COMPOSITE cursor walks
//!   shards one at a time (high bits = shard index, low bits = that shard's inner
//!   [`ScanCursor`]); each call hops to ONE shard via [`dispatch_one_value`]. See
//!   [`scan_cross_shard`] for the composite-cursor algorithm and its full-traversal /
//!   termination / single-shard-byte-identity guarantees.
//!
//! ## shards == 1 parity (byte-identical)
//!
//! With one shard the home shard IS the only shard: [`fan_out_all`] degenerates to the
//! single local call (no channel), and the SCAN composite cursor passes the inner cursor
//! through verbatim ([`ScanCursor::compose`]/[`ScanCursor::decompose`] are identities at
//! `n_shards == 1`). So every merge is the single-shard reply unchanged and the wire SCAN
//! token is bit-identical to before this layer.

use crate::coordinator::{self, Inbox, ShardReply};
use ironcache_env::{Env, Rng};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ProtoVersion, Request, ScanCursor, Value};

/// MERGE the per-shard `DBSIZE` partials into one Integer: SUM the per-shard counts. A
/// per-shard error reply (shard-unavailable) contributes 0 (that shard's count is unknown;
/// summing 0 is the least-surprising degradation for a count under partial unavailability,
/// and unavailability only happens during shutdown). A non-Integer non-Error reply cannot
/// occur (the per-shard `cmd_dbsize` always returns an Integer or a wrong-arity Error).
#[must_use]
pub fn merge_dbsize(replies: Vec<(usize, ShardReply)>) -> Value {
    // A wrong-arity DBSIZE returns the SAME Error on every shard; surface it (not a sum).
    if let Some((_, r)) = replies
        .iter()
        .find(|(_, r)| matches!(r.value, Value::Error(_)))
    {
        if let Value::Error(e) = &r.value {
            // Only surface a genuine command Error (e.g. wrong arity), not the
            // shard-unavailable degradation, which we treat as a 0 contribution below.
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

/// MERGE the per-shard `KEYS pattern` partials: CONCATENATE the per-shard key arrays into
/// one array. Order is irrelevant (Redis promises none). A per-shard Error that is NOT the
/// shard-unavailable degradation (e.g. wrong arity, identical on every shard) is surfaced;
/// shard-unavailable contributes no keys.
#[must_use]
pub fn merge_keys(replies: Vec<(usize, ShardReply)>) -> Value {
    let mut out: Vec<Value> = Vec::new();
    for (_, r) in replies {
        match r.value {
            Value::Array(Some(keys)) => out.extend(keys),
            Value::Error(e) if !coordinator::is_shard_unavailable(&e) => {
                // A genuine command error (wrong arity) is identical on every shard: surface it.
                return Value::error(e);
            }
            // shard-unavailable (or an empty array): contribute nothing.
            _ => {}
        }
    }
    Value::Array(Some(out))
}

/// MERGE the per-shard `FLUSHDB`/`FLUSHALL` partials: `+OK` IFF EVERY shard succeeded; any
/// shard error (a syntax error on a bad option, identical on every shard, OR a
/// shard-unavailable) is surfaced. A flush must be all-or-surfaced so a client never
/// believes the keyspace is empty when a shard failed to clear.
#[must_use]
pub fn merge_flush(replies: &[(usize, ShardReply)]) -> Value {
    for (_, r) in replies {
        if let Value::Error(e) = &r.value {
            return Value::error(e.clone());
        }
    }
    Value::ok()
}

/// MERGE the per-shard `RANDOMKEY` partials: collect the NON-nil per-shard keys and pick
/// ONE UNIFORMLY among the shards that HAVE a key, drawing the choice from `pick` (a u64
/// the HOME core drew from its Env RNG seam, ADR-0003; NEVER std rand). Returns nil
/// ([`Value::Null`] degraded per proto by the encoder) if every shard's db was empty.
///
/// The pick is UNIFORM OVER THE SHARDS-THAT-HAVE-A-KEY, NOT over all keys (a
/// proportional-to-size pick is explicitly not required this pass; documented). A
/// per-shard wrong-arity Error is surfaced; shard-unavailable contributes no candidate.
#[must_use]
pub fn merge_randomkey(replies: Vec<(usize, ShardReply)>, pick: u64) -> Value {
    let mut candidates: Vec<Value> = Vec::new();
    for (_, r) in replies {
        match r.value {
            // A non-nil bulk-string key is a candidate.
            Value::BulkString(Some(_)) => candidates.push(r.value),
            // A genuine command error (e.g. wrong arity, identical on every shard) is
            // surfaced; shard-unavailable is the degradation we ignore (no candidate).
            Value::Error(e) if !coordinator::is_shard_unavailable(&e) => {
                return Value::error(e);
            }
            // RESP-null variants (that shard's db is empty) + the ignored degradation:
            // contribute no candidate.
            _ => {}
        }
    }
    if candidates.is_empty() {
        return Value::Null;
    }
    // Uniform pick among the shards-that-have-a-key, indexed by the home Env RNG draw.
    let idx = (pick % candidates.len() as u64) as usize;
    candidates.swap_remove(idx)
}

/// The home-core SCATTER-GATHER for a broadcast whole-keyspace command (DBSIZE / KEYS /
/// FLUSHDB / FLUSHALL / RANDOMKEY): [`fan_out_all`] then the per-command merge, encoding
/// the merged [`Value`] into `out` with the home connection's `proto`.
///
/// `cmd_upper` is the uppercased command token (the caller already computed it for
/// routing). `randomkey_pick` is a u64 the caller drew from the home Env RNG seam, used
/// ONLY by RANDOMKEY to choose a shard (ignored otherwise). SCAN is NOT handled here (it
/// is a single-target walk, see [`scan_cross_shard`]).
///
/// Each argument is a distinct orthogonal seam (inbox/ctx/cmd/request/db/home/pick/out/
/// proto) the fan-out threads through, mirroring the serve loop's dispatch call shape;
/// bundling them into a struct would only obscure the per-call borrows, so the
/// over-7-args lint is allowed here with that justification.
#[allow(clippy::too_many_arguments)]
pub async fn fan_out_and_merge(
    inbox: &Inbox,
    ctx: &ServerContext,
    cmd_upper: &[u8],
    request: &Request,
    db: u32,
    home: usize,
    randomkey_pick: u64,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    // The home shard's partial runs LOCALLY + SYNCHRONOUSLY (the `local` closure); every
    // other shard runs it via its drain loop. `fan_out_all` gathers the pairs by shard id.
    let replies = coordinator::fan_out_all(inbox, request, db, home, || {
        coordinator::run_local_whole_keyspace(ctx, request, db)
    })
    .await;

    let merged = match cmd_upper {
        b"DBSIZE" => merge_dbsize(replies),
        b"KEYS" => merge_keys(replies),
        b"FLUSHDB" | b"FLUSHALL" => merge_flush(&replies),
        b"RANDOMKEY" => merge_randomkey(replies, randomkey_pick),
        // SCAN is routed to `scan_cross_shard`, never here; any other token cannot reach
        // here (the serve loop only sends WholeKeyspace commands to the fan-out path).
        _ => Value::error(ironcache_protocol::ErrorReply::err(
            "non-fan-out whole-keyspace command",
        )),
    };
    encode_into(out, &merged, proto);
}

/// The cross-shard `SCAN cursor [MATCH] [COUNT] [TYPE]` (COORDINATOR.md #107). SCAN routes
/// to ONE shard per call (the current composite-cursor shard index), NOT a broadcast, so
/// it uses a SINGLE-TARGET hop ([`dispatch_one_value`]) for the remote case and the home
/// local partial for the home shard.
///
/// ## The composite cursor (the full-traversal contract)
///
/// The wire cursor is COMPOSITE ([`ScanCursor::compose`]/[`ScanCursor::decompose`]): high
/// bits = the current shard index, low bits = that shard's inner [`ScanCursor`] position.
/// Per call:
///   1. DECODE the wire cursor into `(shard_idx, inner_resume)`.
///   2. Rewrite `args[1]` to `inner_resume`'s decimal token and run SCAN's per-shard step
///      on shard `shard_idx` (LOCAL if it is home, else a single hop to that ONE shard).
///   3. Read the returned `(next_inner, batch)` from the shard's reply.
///   4. If `next_inner == 0` (that shard is fully scanned): if `shard_idx` is the LAST
///      shard, return the composite cursor `"0"` (DONE); else ADVANCE to
///      `(shard_idx + 1, START)`. Otherwise STAY on `(shard_idx, next_inner)`.
///   5. ENCODE the composite cursor back to the wire token; reply `[cursor, batch]`.
///
/// This preserves SCAN's full-traversal-across-calls guarantee (every key visited at least
/// once) shard by shard, and TERMINATES at composite `0` (finite per-shard steps x finite
/// shards). The inner resume threshold is rounded DOWN by [`ScanCursor::decompose`], which
/// under the store's INCLUSIVE `scan_hash >= cursor` resume only re-visits keys, never
/// skips (see [`ScanCursor::compose`]'s safety argument). With `n_shards == 1` the
/// composite cursor IS the inner cursor (byte-identical), so a single-shard SCAN is
/// unchanged.
///
/// A malformed cursor (non-decimal / out-of-range) surfaces the canonical invalid-cursor
/// error WITHOUT hopping (the home shard's own SCAN handler would return the same).
pub async fn scan_cross_shard(
    inbox: &Inbox,
    ctx: &ServerContext,
    request: &Request,
    db: u32,
    home: usize,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    let n_shards = inbox.len();

    // ARITY first (FIX 4): SCAN is arity -2, so a bare `SCAN` (no cursor) is a
    // wrong-arity error, NOT an invalid-cursor error. The home `cmd_scan` checks arity
    // before parsing the cursor, so this match keeps the cross-shard path byte-identical
    // to the single-shard reply (and correct at shards == 1, preserving parity).
    if request.args.len() < 2 {
        encode_into(
            out,
            &Value::error(ironcache_protocol::ErrorReply::wrong_arity("scan")),
            proto,
        );
        return;
    }

    // Parse + validate the COMPOSITE wire cursor up front (args[1]); a bad token is the
    // canonical invalid-cursor error, matching the single-shard handler, with no hop.
    let Some(composite) = request.args.get(1).and_then(|a| ScanCursor::from_token(a)) else {
        encode_into(
            out,
            &Value::error(ironcache_protocol::ErrorReply::invalid_cursor()),
            proto,
        );
        return;
    };
    let (shard_idx, inner_resume) = composite.decompose(n_shards);
    // Defensive: a composite cursor whose decoded shard index is out of range (a corrupted
    // / hand-crafted token) is treated as the invalid-cursor error rather than indexing OOB.
    if shard_idx >= n_shards {
        encode_into(
            out,
            &Value::error(ironcache_protocol::ErrorReply::invalid_cursor()),
            proto,
        );
        return;
    }

    // Rewrite args[1] to the per-shard INNER cursor token, leaving the MATCH/COUNT/TYPE
    // option tail untouched, so the targeted shard runs the plain single-shard SCAN over
    // ITS partition with the inner cursor.
    let mut inner_req = request.clone();
    inner_req.args[1] = bytes::Bytes::from(inner_resume.to_token().into_bytes());

    // Run the per-shard SCAN step: LOCAL on the home shard, else a single hop to shard_idx.
    let reply_value = if shard_idx == home {
        coordinator::run_local_whole_keyspace(ctx, &inner_req, db).value
    } else {
        coordinator::dispatch_one_value(inbox, shard_idx, &inner_req, db).await
    };

    // Decode the per-shard reply `[next_inner_cursor_bulkstring, [keys...]]`, rewrite the
    // inner cursor into the COMPOSITE wire cursor advancing shard-by-shard, and re-emit.
    let merged = rewrite_scan_reply(reply_value, shard_idx, n_shards);
    encode_into(out, &merged, proto);
}

/// Rewrite a per-shard SCAN reply `[next_inner_bulkstring, [keys...]]` into the cross-shard
/// reply with the COMPOSITE wire cursor (COORDINATOR.md #107). Pure (no I/O), so it is
/// unit-testable: it embodies the advance-shard-or-stay decision and the composite encode.
///
/// - If the shard's `next_inner` is 0 (that shard is fully scanned): advance to the NEXT
///   shard at its START cursor, encoded as `compose(shard_idx + 1, 0)`; or, if this was the
///   LAST shard, the global-complete sentinel `"0"`.
/// - Else stay on this shard at `compose(shard_idx, next_inner)`.
///
/// A non-array reply (a shard-unavailable Error, or a wrong-arity Error from the per-shard
/// handler) is passed through unchanged so the client sees a well-formed error.
#[must_use]
fn rewrite_scan_reply(reply: Value, shard_idx: usize, n_shards: usize) -> Value {
    // Decompose the per-shard reply shape; anything else (an Error) passes through.
    let Value::Array(Some(items)) = reply else {
        return reply;
    };
    if items.len() != 2 {
        return Value::Array(Some(items));
    }
    let mut it = items.into_iter();
    let cursor_item = it.next().expect("len checked == 2");
    let keys_item = it.next().expect("len checked == 2");

    // The per-shard next-inner cursor is a decimal bulk string; parse it back.
    let next_inner = match &cursor_item {
        Value::BulkString(Some(b)) => ScanCursor::from_token(b).unwrap_or(ScanCursor::START),
        _ => ScanCursor::START,
    };

    let composite_next = if next_inner.is_start() {
        // This shard is fully scanned. Advance to the next shard's START, or finish.
        if shard_idx + 1 >= n_shards {
            ScanCursor::START // global complete: the wire cursor "0".
        } else {
            ScanCursor::compose(shard_idx + 1, ScanCursor::START, n_shards)
        }
    } else {
        // Stay on this shard, resuming at its next inner position.
        ScanCursor::compose(shard_idx, next_inner, n_shards)
    };

    Value::Array(Some(vec![
        Value::bulk(composite_next.to_token().into_bytes()),
        keys_item,
    ]))
}

/// Encode `value` for `proto` and append to `out` (the home-core encode; mirrors the serve
/// loop / coordinator encode). Encoding stays on the home core with the home proto.
fn encode_into(out: &mut Vec<u8>, value: &Value, proto: ProtoVersion) {
    let mut bm = bytes::BytesMut::with_capacity(64);
    ironcache_protocol::encode(&mut bm, value, proto);
    out.extend_from_slice(&bm);
}

/// Draw the RANDOMKEY shard-pick u64 from the home shard's Env RNG seam (ADR-0003), the
/// SAME seam the single-shard `dispatch_keyed_data` RANDOMKEY arm draws its index from.
/// Drawn ONCE on the home core, ONLY for a well-formed (arity-1) RANDOMKEY, so the
/// per-command RNG stream is not perturbed by anything else (mirrors the dispatch draw).
#[must_use]
pub fn randomkey_pick(request: &Request) -> u64 {
    if request.args.len() == 1 {
        crate::serve::shard_env().borrow_mut().rng().next_u64()
    } else {
        0
    }
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

    fn bulk(s: &str) -> Value {
        Value::bulk(s.as_bytes().to_vec())
    }

    #[test]
    fn dbsize_merge_sums_per_shard_counts() {
        let replies = vec![
            (0, reply(Value::Integer(3))),
            (1, reply(Value::Integer(5))),
            (2, reply(Value::Integer(0))),
        ];
        assert_eq!(merge_dbsize(replies), Value::Integer(8));
    }

    #[test]
    fn dbsize_merge_unavailable_shard_contributes_zero() {
        let replies = vec![
            (0, reply(Value::Integer(4))),
            (
                1,
                // The shared wording (FIX 6) so this test stays coupled to the real
                // shard-unavailable message the merge classifier recognizes.
                reply(Value::error(ErrorReply::err(
                    coordinator::SHARD_UNAVAILABLE_MSG,
                ))),
            ),
        ];
        assert_eq!(merge_dbsize(replies), Value::Integer(4));
    }

    #[test]
    fn keys_merge_concatenates_all_shard_arrays() {
        let replies = vec![
            (0, reply(Value::Array(Some(vec![bulk("a"), bulk("b")])))),
            (1, reply(Value::Array(Some(vec![bulk("c")])))),
            (2, reply(Value::Array(Some(vec![])))),
        ];
        let Value::Array(Some(keys)) = merge_keys(replies) else {
            panic!("merge_keys must return an array");
        };
        let got: Vec<Vec<u8>> = keys
            .into_iter()
            .map(|v| match v {
                Value::BulkString(Some(b)) => b.to_vec(),
                other => panic!("non-bulk key {other:?}"),
            })
            .collect();
        assert_eq!(got, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn flush_merge_ok_only_when_all_ok() {
        let all_ok = vec![(0, reply(Value::ok())), (1, reply(Value::ok()))];
        assert_eq!(merge_flush(&all_ok), Value::ok());

        let one_err = vec![
            (0, reply(Value::ok())),
            (1, reply(Value::error(ErrorReply::syntax_error()))),
        ];
        assert!(matches!(merge_flush(&one_err), Value::Error(_)));
    }

    #[test]
    fn randomkey_merge_empty_is_nil_and_pick_selects_a_present_key() {
        // Every shard empty -> nil.
        let empty = vec![(0, reply(Value::Null)), (1, reply(Value::BulkString(None)))];
        assert_eq!(merge_randomkey(empty, 0), Value::Null);

        // Two shards have a key; the pick selects deterministically among them. (ShardReply
        // is not Clone, so build the input twice.)
        let some = || {
            vec![
                (0, reply(bulk("k0"))),
                (1, reply(Value::Null)),
                (2, reply(bulk("k2"))),
            ]
        };
        // pick 0 -> first candidate (k0); pick 1 -> second candidate (k2).
        assert_eq!(merge_randomkey(some(), 0), bulk("k0"));
        assert_eq!(merge_randomkey(some(), 1), bulk("k2"));
    }

    #[test]
    fn scan_rewrite_advances_to_next_shard_when_inner_complete() {
        // Shard 0 returns inner 0 (complete) with some keys; not the last shard (n=3) ->
        // the composite cursor advances to (shard 1, START).
        let per_shard = Value::Array(Some(vec![
            Value::bulk(b"0".to_vec()), // inner complete
            Value::Array(Some(vec![bulk("a")])),
        ]));
        let Value::Array(Some(items)) = rewrite_scan_reply(per_shard, 0, 3) else {
            panic!("expected array reply");
        };
        let Value::BulkString(Some(tok)) = &items[0] else {
            panic!("expected cursor bulkstring");
        };
        let composite = ScanCursor::from_token(tok).unwrap();
        let (shard, inner) = composite.decompose(3);
        assert_eq!(shard, 1, "advance to the next shard");
        assert!(inner.is_start(), "next shard starts at START");
    }

    #[test]
    fn scan_rewrite_stays_on_shard_when_inner_incomplete() {
        // Shard 1 returns a non-zero inner cursor -> stay on shard 1 at that inner position.
        let inner_hash = 0x1234_5678_9ABC_DEF0u64;
        let per_shard = Value::Array(Some(vec![
            Value::bulk(ScanCursor(inner_hash).to_token().into_bytes()),
            Value::Array(Some(vec![bulk("x")])),
        ]));
        let Value::Array(Some(items)) = rewrite_scan_reply(per_shard, 1, 4) else {
            panic!("expected array reply");
        };
        let Value::BulkString(Some(tok)) = &items[0] else {
            panic!("expected cursor bulkstring");
        };
        let composite = ScanCursor::from_token(tok).unwrap();
        let (shard, inner) = composite.decompose(4);
        assert_eq!(shard, 1, "stay on the same shard");
        assert!(!inner.is_start(), "resume mid-shard");
        // The resume is the inner hash rounded DOWN to a SHARD_BITS multiple.
        let expected = inner_hash & !((1u64 << ScanCursor::SHARD_BITS) - 1);
        assert_eq!(inner.0, expected);
    }

    #[test]
    fn scan_rewrite_last_shard_complete_returns_global_zero() {
        // The LAST shard (index 2 of 3) returning inner 0 -> the global complete cursor "0".
        let per_shard = Value::Array(Some(vec![
            Value::bulk(b"0".to_vec()),
            Value::Array(Some(vec![])),
        ]));
        let Value::Array(Some(items)) = rewrite_scan_reply(per_shard, 2, 3) else {
            panic!("expected array reply");
        };
        let Value::BulkString(Some(tok)) = &items[0] else {
            panic!("expected cursor bulkstring");
        };
        assert_eq!(tok.as_ref(), b"0", "last shard complete -> global cursor 0");
    }

    #[test]
    fn scan_rewrite_single_shard_cursor_is_byte_identical() {
        // n_shards == 1: a mid-scan inner cursor passes through UNCHANGED (the composite
        // cursor IS the inner cursor), so the single-shard wire token is byte-identical.
        let inner_hash = 0xDEAD_BEEF_0000_1234u64;
        let per_shard = Value::Array(Some(vec![
            Value::bulk(ScanCursor(inner_hash).to_token().into_bytes()),
            Value::Array(Some(vec![bulk("k")])),
        ]));
        let Value::Array(Some(items)) = rewrite_scan_reply(per_shard, 0, 1) else {
            panic!("expected array reply");
        };
        let Value::BulkString(Some(tok)) = &items[0] else {
            panic!("expected cursor bulkstring");
        };
        // The token must be exactly the inner hash's token (no shard bits, no rounding).
        assert_eq!(tok.as_ref(), ScanCursor(inner_hash).to_token().as_bytes());

        // And a completed single-shard scan returns "0".
        let done = Value::Array(Some(vec![
            Value::bulk(b"0".to_vec()),
            Value::Array(Some(vec![])),
        ]));
        let Value::Array(Some(items)) = rewrite_scan_reply(done, 0, 1) else {
            panic!("expected array reply");
        };
        let Value::BulkString(Some(tok)) = &items[0] else {
            panic!("expected cursor bulkstring");
        };
        assert_eq!(tok.as_ref(), b"0");
    }

    #[test]
    fn scan_rewrite_passes_through_error_replies() {
        // A shard-unavailable / wrong-arity Error from the per-shard step passes through.
        let err = Value::error(ErrorReply::err("cross-shard target unavailable"));
        assert!(matches!(rewrite_scan_reply(err, 0, 4), Value::Error(_)));
    }
}
