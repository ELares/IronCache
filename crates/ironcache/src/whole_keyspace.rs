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
//!
//! ## shard-owners: HOME-ONLY scope (#526)
//!
//! In `cluster_mode = shard-owners` the node advertises its N internal shards as N cluster
//! nodes (one per port). Because each shard's store holds EXACTLY its slot range (#520), a
//! whole-keyspace command issued to shard `i`'s port must answer for shard `i` ONLY -- the
//! per-node Redis Cluster view -- NOT the global fan-out (which would make a per-node
//! aggregator over-count DBSIZE by N and return N copies from SCAN). The serve loop routes
//! those commands to [`run_home_only`] (DBSIZE / KEYS / RANDOMKEY / FLUSHDB / FLUSHALL) and
//! [`scan_cross_shard`] with `home_only` (SCAN pinned to the home shard). Static/Raft keep
//! the global scatter-gather above (one logical keyspace), byte-unchanged.

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

/// MERGE the per-shard `__ICGETKEYSINSLOT <slot> <count>` partials (#371, the cross-shard half of
/// `CLUSTER GETKEYSINSLOT`): CONCATENATE the per-shard key arrays (each already bounded to `limit`)
/// and TRUNCATE the union to `limit`, so the client never receives more than it asked for even though
/// every shard could supply up to `limit`. Order across shards is irrelevant (Redis promises no
/// `GETKEYSINSLOT` order); within a shard it is the stable scan order, so the result is deterministic.
/// A genuine per-shard command Error is surfaced; shard-unavailable contributes no keys.
#[must_use]
pub fn merge_getkeysinslot(replies: Vec<(usize, ShardReply)>, limit: usize) -> Value {
    let mut out: Vec<Value> = Vec::new();
    for (_, r) in replies {
        match r.value {
            Value::Array(Some(keys)) => {
                for k in keys {
                    if out.len() >= limit {
                        break;
                    }
                    out.push(k);
                }
            }
            Value::Error(e) if !coordinator::is_shard_unavailable(&e) => {
                return Value::error(e);
            }
            _ => {}
        }
    }
    out.truncate(limit);
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

/// SUM the per-shard `__ICINFOKEYSPACE <databases>` partials into the NODE-WIDE per-db key counts
/// (#531): each shard replied an Array of `databases` Integers (`[db_len(0), db_len(1), ...]`);
/// fold them element-wise so `totals[db]` is the whole node's DBSIZE for `db` -- EXACTLY the sum
/// the cross-shard DBSIZE merge ([`merge_dbsize`]) produces per db, since each shard reports the
/// SAME `db_len` its DBSIZE partial does. A shard-unavailable / error reply contributes 0 (that
/// shard's counts are unknown; 0 is the least-surprising degradation and unavailability only
/// happens during shutdown), mirroring [`merge_dbsize`].
#[must_use]
pub fn merge_keyspace_counts(replies: Vec<(usize, ShardReply)>, databases: usize) -> Vec<u64> {
    let mut totals = vec![0u64; databases];
    for (_, r) in replies {
        // A non-Array reply (a shard-unavailable Error, or an out-of-shape reply that cannot occur)
        // contributes nothing; only the per-db Integer array is summed.
        if let Value::Array(Some(items)) = r.value {
            for (db, item) in items.into_iter().enumerate() {
                if db >= databases {
                    break;
                }
                if let Value::Integer(n) = item {
                    totals[db] = totals[db].saturating_add(u64::try_from(n).unwrap_or(0));
                }
            }
        }
    }
    totals
}

/// Gather the NODE-WIDE INFO `# Keyspace` lines (#531): fan `__ICINFOKEYSPACE <databases>` out to
/// EVERY shard (the SAME scatter-gather DBSIZE uses -- the home shard's partial runs LOCALLY +
/// synchronously via the `local` closure, the rest via their drain loops), SUM the per-db partials
/// with [`merge_keyspace_counts`], then build one [`KeyspaceDbLine`] per NON-EMPTY db (Redis omits
/// empty DBs). The result feeds INFO's keyspace section so its `dbN:keys=...` counts equal DBSIZE
/// on a multi-shard node. Called ONLY for INFO on a >1-shard node whose reply includes the keyspace
/// section (a cold, rare command), never on the data hot path; `db` is the issuing connection's
/// selected DB (threaded to the fan-out purely as the `ShardWork.db`, ignored by the per-db gather).
pub async fn gather_node_keyspace(
    inbox: &Inbox,
    ctx: &ServerContext,
    databases: u32,
    db: u32,
    home: usize,
) -> Vec<ironcache_observe::KeyspaceDbLine> {
    // The internal broadcast request carries the db COUNT so every shard reports db_len(0..databases).
    let request = Request {
        args: vec![
            bytes::Bytes::from_static(ironcache_server::ICINFOKEYSPACE),
            bytes::Bytes::from(databases.to_string()),
        ],
    };
    let replies = coordinator::fan_out_all(inbox, &request, db, home, || {
        coordinator::run_local_whole_keyspace(ctx, &request, db)
    })
    .await;
    merge_keyspace_counts(replies, databases as usize)
        .into_iter()
        .enumerate()
        .filter(|&(_, keys)| keys > 0)
        .map(|(db, keys)| ironcache_observe::KeyspaceDbLine {
            db: db as u32,
            keys,
            expires: 0,
        })
        .collect()
}

/// The `<count>` limit of an `__ICGETKEYSINSLOT <slot> <count>` internal request: `args[2]` as a
/// non-negative count. The serve loop only routes a VALIDATED command here (the slot + count already
/// parsed by `parse_slot_scan`), so this re-read is defensive and falls back to `0` (an empty result,
/// never a panic) if the arg is somehow missing or malformed.
fn getkeysinslot_limit(request: &Request) -> usize {
    request
        .args
        .get(2)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&n| n >= 0)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0)
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
        // `__ICCOUNTKEYSINSLOT` (#371) SUMS the per-shard counts exactly like DBSIZE: same merge.
        b"DBSIZE" | b"__ICCOUNTKEYSINSLOT" => merge_dbsize(replies),
        b"KEYS" => merge_keys(replies),
        b"FLUSHDB" | b"FLUSHALL" => merge_flush(&replies),
        b"RANDOMKEY" => merge_randomkey(replies, randomkey_pick),
        // `__ICGETKEYSINSLOT` (#371) concatenates the per-shard key arrays and truncates the union
        // to the `<count>` arg (the cross-shard half of CLUSTER GETKEYSINSLOT).
        b"__ICGETKEYSINSLOT" => merge_getkeysinslot(replies, getkeysinslot_limit(request)),
        // SCAN is routed to `scan_cross_shard`, never here; any other token cannot reach
        // here (the serve loop only sends WholeKeyspace commands to the fan-out path).
        _ => Value::error(ironcache_protocol::ErrorReply::err(
            "non-fan-out whole-keyspace command",
        )),
    };
    encode_into(out, &merged, proto);
}

/// Serve a broadcast whole-keyspace command (DBSIZE / KEYS / RANDOMKEY / FLUSHDB / FLUSHALL)
/// from the HOME shard ONLY -- NO fan-out (#526, shard-owners). Since the slot-owner
/// alignment (#520) each internal shard's store holds EXACTLY the keys in its slot range, so
/// the connecting shard's local partial IS the per-node Redis Cluster answer:
/// - `DBSIZE`: this shard's key count (a per-node sum over the N ports equals the true total,
///   each port distinct -- no more over-count by N).
/// - `KEYS pattern`: only this shard's matching keys (the union across ports is the keyspace,
///   each key once).
/// - `RANDOMKEY`: a key OWNED by this shard (its own Env RNG seam draws it, ADR-0003), or nil
///   if this shard is empty.
/// - `FLUSHDB` / `FLUSHALL`: clears ONLY this shard's slice, matching a real Redis Cluster
///   node flushing its own slots (an operator wiping the whole dataset uses a cluster-aware
///   tool that visits every node, exactly as with Redis Cluster).
///
/// This is the SAME per-shard partial [`fan_out_and_merge`] runs on every shard, but for the
/// home shard alone and encoded directly (no scatter-gather, no merge, no RNG shard-pick).
/// Static/Raft never reach here -- they keep the global [`fan_out_and_merge`], so their single
/// logical keyspace stays byte-unchanged.
pub fn run_home_only(
    ctx: &ServerContext,
    request: &Request,
    db: u32,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    let reply = coordinator::run_local_whole_keyspace(ctx, request, db);
    encode_into(out, &reply.value, proto);
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
///
/// ## `home_only` (shard-owners, #526)
///
/// When `home_only` is set (`cluster_mode = shard-owners`), the walk is PINNED to the
/// connecting `home` shard: the decoded cursor's shard field is IGNORED (only its inner
/// position is used) and the scan NEVER advances to a sibling shard -- it terminates at the
/// composite `"0"` as soon as the home shard is exhausted. Since each internal shard's store
/// holds EXACTLY its slot range (#520), this enumerates precisely the keys THIS per-shard
/// port owns (the per-node Redis Cluster view), never the global fan-out. `home_only == false`
/// is the Static/Raft global walk across every shard, byte-unchanged.
// One cohesive scatter-gather over the SCAN state (cursor/count/match/type/db) plus the
// shard-owners `home_only` scoping bit; splitting it into a params struct would just shuffle the
// same fields. Same allowance the cluster-redirect path uses (#517 PR4).
#[allow(clippy::too_many_arguments)]
pub async fn scan_cross_shard(
    inbox: &Inbox,
    ctx: &ServerContext,
    request: &Request,
    db: u32,
    home: usize,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
    home_only: bool,
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
    let (decoded_shard, inner_resume) = composite.decompose(n_shards);
    // shard-owners (#526): PIN the walk to the home shard -- ignore the cursor's shard field
    // and only ever scan `home`, so this per-shard port enumerates its OWN slice and stops
    // there. The inner resume is still taken from the cursor (it is `START` for the initial
    // "0" token and the home shard's mid-scan position otherwise). In the Static/Raft global
    // walk the decoded shard drives the cross-shard advance, unchanged.
    let shard_idx = if home_only { home } else { decoded_shard };
    // Defensive: a composite cursor whose decoded shard index is out of range (a corrupted
    // / hand-crafted token) is treated as the invalid-cursor error rather than indexing OOB.
    // (In `home_only` mode `shard_idx == home` is always in range, so this never fires there;
    // a garbage inner cursor is rounded DOWN and only re-visits keys, never skips.)
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
    // In `home_only` mode the rewrite stays on `home` and finishes when it is exhausted
    // (never advancing to a sibling shard).
    let merged = rewrite_scan_reply(reply_value, shard_idx, n_shards, home_only);
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
/// When `home_only` (shard-owners, #526) the walk is PINNED to `shard_idx` (the home shard):
/// exhausting it returns the global-complete `"0"` DIRECTLY (never advancing to a sibling),
/// so a per-shard port enumerates ONLY its own slice and then terminates.
///
/// A non-array reply (a shard-unavailable Error, or a wrong-arity Error from the per-shard
/// handler) is passed through unchanged so the client sees a well-formed error.
#[must_use]
fn rewrite_scan_reply(reply: Value, shard_idx: usize, n_shards: usize, home_only: bool) -> Value {
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
        // This shard is fully scanned. In `home_only` mode the walk is pinned to the home
        // shard, so an exhausted home shard is the WHOLE per-node scan complete -> "0"
        // (#526), never advancing to a sibling. Otherwise advance to the next shard's START,
        // or finish if this was the last shard (the Static/Raft global walk).
        if home_only || shard_idx + 1 >= n_shards {
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
    // Vec<u8> is a bytes::BufMut sink: encode writes straight into `out` (no temp BytesMut + copy).
    ironcache_protocol::encode(out, value, proto);
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
    fn getkeysinslot_merge_concatenates_then_truncates_to_the_limit() {
        // A generous limit returns the whole cross-shard union.
        let replies = vec![
            (0, reply(Value::Array(Some(vec![bulk("a"), bulk("b")])))),
            (1, reply(Value::Array(Some(vec![bulk("c")])))),
            (2, reply(Value::Array(Some(vec![bulk("d"), bulk("e")])))),
        ];
        assert_eq!(
            merge_getkeysinslot(replies, 10),
            Value::Array(Some(vec![
                bulk("a"),
                bulk("b"),
                bulk("c"),
                bulk("d"),
                bulk("e")
            ]))
        );
        // The limit BOUNDS the union even though each shard supplied at most `limit` (so the union
        // could be `n_shards * limit`): 3 of the 4 keys.
        let replies = vec![
            (0, reply(Value::Array(Some(vec![bulk("a"), bulk("b")])))),
            (1, reply(Value::Array(Some(vec![bulk("c"), bulk("d")])))),
        ];
        assert_eq!(
            merge_getkeysinslot(replies, 3),
            Value::Array(Some(vec![bulk("a"), bulk("b"), bulk("c")]))
        );
        // A 0 limit yields the empty array.
        let replies = vec![(0, reply(Value::Array(Some(vec![bulk("a")]))))];
        assert_eq!(
            merge_getkeysinslot(replies, 0),
            Value::Array(Some(Vec::new()))
        );
    }

    #[test]
    fn getkeysinslot_merge_surfaces_a_real_error_not_unavailable() {
        // A genuine command error (identical on every shard) is surfaced.
        let replies = vec![
            (0, reply(Value::Array(Some(vec![bulk("a")])))),
            (1, reply(Value::error(ErrorReply::err("Invalid slot")))),
        ];
        assert!(matches!(merge_getkeysinslot(replies, 10), Value::Error(_)));
        // A shard-unavailable degradation contributes nothing (not surfaced).
        let replies = vec![
            (0, reply(Value::Array(Some(vec![bulk("a")])))),
            (
                1,
                reply(Value::error(ErrorReply::err(
                    coordinator::SHARD_UNAVAILABLE_MSG,
                ))),
            ),
        ];
        assert_eq!(
            merge_getkeysinslot(replies, 10),
            Value::Array(Some(vec![bulk("a")]))
        );
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
        let Value::Array(Some(items)) = rewrite_scan_reply(per_shard, 0, 3, false) else {
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
        let Value::Array(Some(items)) = rewrite_scan_reply(per_shard, 1, 4, false) else {
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
        let Value::Array(Some(items)) = rewrite_scan_reply(per_shard, 2, 3, false) else {
            panic!("expected array reply");
        };
        let Value::BulkString(Some(tok)) = &items[0] else {
            panic!("expected cursor bulkstring");
        };
        assert_eq!(tok.as_ref(), b"0", "last shard complete -> global cursor 0");
    }

    #[test]
    fn scan_rewrite_home_only_stops_at_the_home_shard() {
        // shard-owners (#526): the home shard is NOT the last shard (index 1 of 4), but with
        // `home_only` an exhausted home shard is the whole per-node scan complete -> "0",
        // NEVER advancing to shard 2. This is what pins a per-shard port to its own slice.
        let per_shard = Value::Array(Some(vec![
            Value::bulk(b"0".to_vec()), // home shard inner complete
            Value::Array(Some(vec![bulk("a")])),
        ]));
        let Value::Array(Some(items)) = rewrite_scan_reply(per_shard, 1, 4, true) else {
            panic!("expected array reply");
        };
        let Value::BulkString(Some(tok)) = &items[0] else {
            panic!("expected cursor bulkstring");
        };
        assert_eq!(
            tok.as_ref(),
            b"0",
            "home_only: exhausting the home shard finishes the scan (no sibling advance)"
        );

        // Mid-scan the home_only walk STAYS on the home shard (same as the global walk): a
        // non-zero inner cursor re-composes onto shard 1, resuming there next call.
        let inner_hash = 0x0BAD_F00D_0000_2222u64;
        let per_shard = Value::Array(Some(vec![
            Value::bulk(ScanCursor(inner_hash).to_token().into_bytes()),
            Value::Array(Some(vec![bulk("x")])),
        ]));
        let Value::Array(Some(items)) = rewrite_scan_reply(per_shard, 1, 4, true) else {
            panic!("expected array reply");
        };
        let Value::BulkString(Some(tok)) = &items[0] else {
            panic!("expected cursor bulkstring");
        };
        let (shard, inner) = ScanCursor::from_token(tok).unwrap().decompose(4);
        assert_eq!(shard, 1, "home_only stays on the home shard mid-scan");
        assert!(!inner.is_start(), "resume mid-shard");
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
        let Value::Array(Some(items)) = rewrite_scan_reply(per_shard, 0, 1, false) else {
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
        let Value::Array(Some(items)) = rewrite_scan_reply(done, 0, 1, false) else {
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
        assert!(matches!(
            rewrite_scan_reply(err, 0, 4, false),
            Value::Error(_)
        ));
    }
}
