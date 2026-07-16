// SPDX-License-Identifier: MIT OR Apache-2.0
//! Cross-shard hop overlap (#8): the [`DeferredHop`] parked-reply state and the
//! [`drain_deferred_hops`] FIFO drain. Extracted verbatim from `serve.rs` as a cohesive group; both
//! serve loops (tokio + io_uring) call [`drain_deferred_hops`] at every barrier and end of batch.
//! The per-command hooks it fans out to (`record_command_stats`, `record_hotkeys`,
//! `apply_client_tracking`, `consume_caching_flag`, `record_slow_command`) stay in `serve` and are
//! reached here as `pub(crate)` (crate-internal, no wider exposure).

use crate::coordinator;
// The per-command hooks + the crate-local `ShardState` live in `serve` (widened to `pub(crate)` for
// this sibling); the wire/context types come from their origin crates, exactly as `serve` imports
// them.
use crate::serve::{
    ShardState, apply_client_tracking, consume_caching_flag, record_command_stats, record_hotkeys,
    record_slow_command,
};
use ironcache_env::{Clock, SystemEnv};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, ProtoVersion, Request};
use std::cell::RefCell;
use std::rc::Rc;

/// One DEFERRED cross-shard hop (#8 overlap): a remote single-key command whose `ShardWork` was
/// enqueued but whose reply is not yet awaited, so the next command's hop can be issued while this
/// owner is still working. The serve loop parks a RUN of these and drains them together (in order)
/// at the next barrier / end of batch, encoding each reply into `out` and running its per-command
/// hooks then -- because those hooks read the reply bytes (`record_command_stats`/`record_hotkeys`),
/// they cannot run until the reply is materialized. Every field is the per-command state the hooks
/// need, captured at defer time.
///
/// KNOWN BEST-EFFORT EDGE (client tracking): the drain-time hooks read the deferred command's own
/// snapshotted `was_tracking`/`was_bcast`, but the CLIENT-TRACKING / CLIENT-CACHING hooks
/// (`apply_client_tracking`, `consume_caching_flag`) also read LIVE `conn` tracking state. If a
/// tracking-CONTROL command (`CLIENT CACHING`/`TRACKING`) is pipelined BETWEEN deferred remote hops
/// in the same batch, a hop's tracking hook can observe that control command's mutation (it runs as
/// a barrier before the drain). This only affects CLIENT-side tracking of REMOTE keys, which is
/// ALREADY documented single-shard-only / best-effort (see `apply_client_tracking`); it never
/// affects the data reply bytes or their wire order. Snapshotting the full tracking sub-state into
/// this struct is a clean follow-up; deferred here to keep the overlap focused on the FIFO property.
pub(crate) struct DeferredHop {
    /// The OWNING shard for this hop (#674): recorded at defer time; the send is deferred to
    /// [`drain_deferred_hops`], which groups a run's hops per shard + coalesces same-shard ones.
    pub(crate) target: usize,
    /// The DB the connection had selected. Constant across a run (a `SELECT` is a barrier that drains
    /// the run first), so the drain uses it for the per-shard batch send.
    pub(crate) db: u32,
    /// The request, for the hooks (cheap clone: `Request` is `Vec<Bytes>`, refcounted).
    pub(crate) request: Request,
    /// The monotonic start stamp for this command's elapsed-time (slowlog + commandstats).
    pub(crate) cmd_start: ironcache_env::Monotonic,
    /// Tracking-state snapshot taken BEFORE dispatch (so the tracking hook sees an ON->OFF flip).
    pub(crate) was_tracking: bool,
    pub(crate) was_bcast: bool,
    /// The slowlog threshold snapshot (negative = disabled) for this command.
    pub(crate) slow_threshold: i64,
    /// The connection's negotiated proto, to encode the reply on the home core.
    pub(crate) proto: ProtoVersion,
}

/// A per-shard reply RECEIVER collected by [`drain_deferred_hops`] after it issues the run's sends: a
/// coalesced `Batch` (#674) or a lone `Single` (`None` = the owning shard's queue was gone at send).
enum Rx {
    Batch(Option<coordinator::BatchReceiver>),
    Single(Option<coordinator::HopReceiver>),
}

/// A per-shard AWAITED reply, ready for the demux: a `Batch`'s replies wrapped `Option` per index so
/// each hop can `take()` ITS slot; a `Single`'s one reply; or `Gone` (owner gone/errored -> every hop
/// for that shard encodes shard-unavailable, in order).
enum ShardResult {
    Batch(Vec<Option<coordinator::ShardReply>>),
    Single(Option<coordinator::ShardReply>),
    Gone,
}

/// Drain a run of [`DeferredHop`]s (in FIFO order) into `out`: for each, await + encode its reply,
/// then run its per-command hooks (commandstats, hotkeys, client-tracking, caching, slowlog) exactly
/// as the inline path does -- so a deferred remote command is observably identical to a
/// non-deferred one, only its reply is assembled later (still in order). Called at every barrier and
/// at end of batch, so `out` stays strictly append-in-command-order (FIFO on the wire).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_deferred_hops(
    pending: &mut Vec<DeferredHop>,
    out: &mut Vec<u8>,
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    state_rc: &Rc<RefCell<ShardState>>,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    inbox: &coordinator::Inbox,
) {
    if pending.is_empty() {
        return;
    }
    // The run's DB is constant: `SELECT`/`RESET` (the only commands that change `conn.db`) are
    // `AlwaysHome`, so they force a barrier that drains the run BEFORE they run -- no db change ever
    // appears mid-run. The batch is sent with `pending[0].db`; assert the invariant so a future change
    // that made a db-mutating command deferrable trips a test instead of silently mis-db'ing a batch.
    let db = pending[0].db;
    debug_assert!(
        pending.iter().all(|d| d.db == db),
        "a deferred cross-shard run must share one db (SELECT/RESET are barriers)"
    );

    // 1) GROUP each parked hop's request into its owning shard's bucket, recording per hop the
    //    `(target, index-within-that-bucket)` so the reply can be demuxed back to wire order.
    let mut by_shard: std::collections::HashMap<usize, Vec<Request>> =
        std::collections::HashMap::new();
    let mut slots: Vec<(usize, usize)> = Vec::with_capacity(pending.len());
    for d in pending.iter() {
        let bucket = by_shard.entry(d.target).or_default();
        slots.push((d.target, bucket.len()));
        bucket.push(d.request.clone());
    }

    // 2) SEND one message per shard -- a coalesced `Batch` for >= 2 hops (#674), a `Single` for a lone
    //    one (byte-identical to the pre-#674 path). Issue ALL sends first so the owners work
    //    concurrently across shards, then collect the receivers.
    let mut rxs: Vec<(usize, Rx)> = Vec::with_capacity(by_shard.len());
    for (target, mut requests) in by_shard {
        let rx = if requests.len() >= 2 {
            Rx::Batch(coordinator::dispatch_batch_send(inbox, target, requests, db).await)
        } else {
            let req = requests.pop().expect("a bucket holds >= 1 request");
            Rx::Single(coordinator::dispatch_via_send_owned(inbox, target, req, db).await)
        };
        rxs.push((target, rx));
    }

    // 3) AWAIT each shard's reply. A `Batch` reply becomes a takeable `Vec<Option<_>>` so each hop can
    //    pull ITS index; a gone/errored owner becomes `Gone` (all its hops encode shard-unavailable).
    let mut results: std::collections::HashMap<usize, ShardResult> =
        std::collections::HashMap::new();
    for (target, rx) in rxs {
        let r = match rx {
            Rx::Batch(Some(rx)) => match rx.await {
                Ok(v) => ShardResult::Batch(v.into_iter().map(Some).collect()),
                Err(_) => ShardResult::Gone,
            },
            Rx::Single(Some(rx)) => match rx.await {
                Ok(rep) => ShardResult::Single(Some(rep)),
                Err(_) => ShardResult::Gone,
            },
            Rx::Batch(None) | Rx::Single(None) => ShardResult::Gone,
        };
        results.insert(target, r);
    }

    // 4) DEMUX in FIFO (wire) order: for each parked hop, pull its reply from its shard's result at the
    //    recorded index, encode it (or shard-unavailable), then run its per-command hooks -- exactly as
    //    the un-coalesced path did, so a coalesced hop is observably identical, only assembled later.
    for (i, d) in pending.drain(..).enumerate() {
        let (target, idx) = slots[i];
        let value = match results.get_mut(&target) {
            Some(ShardResult::Batch(v)) => v.get_mut(idx).and_then(Option::take).map(|r| r.value),
            Some(ShardResult::Single(o)) => o.take().map(|r| r.value),
            Some(ShardResult::Gone) | None => None,
        };
        let out_before = out.len();
        coordinator::encode_hop_reply(value, out, d.proto);
        let cmd_elapsed_us = u64::try_from(
            env.borrow()
                .now()
                .saturating_duration_since(d.cmd_start)
                .as_micros(),
        )
        .unwrap_or(u64::MAX);
        record_command_stats(state_rc, &d.request, out_before, out, cmd_elapsed_us);
        if ctx.hotkeys.is_active() {
            let reply_bytes =
                u64::try_from(out.len().saturating_sub(out_before)).unwrap_or(u64::MAX);
            record_hotkeys(ctx, env, &d.request, cmd_elapsed_us, reply_bytes);
        }
        apply_client_tracking(
            conn,
            push_tx,
            shed_flag,
            &d.request,
            d.was_tracking,
            d.was_bcast,
        );
        consume_caching_flag(conn, &d.request);
        if d.slow_threshold >= 0 {
            record_slow_command(ctx, env, conn, &d.request, d.cmd_start, d.slow_threshold);
        }
    }
}
