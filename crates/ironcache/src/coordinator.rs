// SPDX-License-Identifier: MIT OR Apache-2.0
//! The cross-shard coordinator substrate (COORDINATOR.md #107, PASS 1).
//!
//! The server is shared-nothing thread-per-core (ADR-0002): each shard owns a
//! PARTITION of the keyspace (by [`ironcache_server::owner_shard`]) and per-shard state
//! (STORE/WHEEL/ENV/ShardState) lives in thread-local `Rc<RefCell<..>>` on that shard's
//! single thread. A connection is pinned for life to the random "home" shard the kernel
//! SO_REUSEPORT-routed it to. So a single-key command whose key is NOT home-owned must
//! HOP to the owning shard, run there against that shard's partition, and return its
//! reply for the home connection to encode.
//!
//! This module is that hop's substrate:
//! - [`ShardWork`] / [`ShardReply`]: the request-in / reply-out envelope (all `Send`:
//!   [`Request`] is `Vec<Bytes>`, [`Value`]/[`CounterDeltas`] are `Send`).
//! - [`Inbox`] + [`build_inboxes`]: one bounded MPSC queue PER shard (the cross-thread
//!   channel; back-pressure is await-on-full).
//! - [`run_drain_loop`]: the per-shard consumer the bootstrap spawns on each shard's
//!   LocalSet; it runs each unit of remote work against THIS shard's thread-locals.
//! - [`dispatch_via`]: the home-core side that enqueues work to the owning shard and
//!   awaits the oneshot reply, then encodes on the home core with the home proto.
//!
//! ## Borrow discipline (critical, ADR-0002/0005)
//!
//! The drain loop runs on the SAME single-threaded LocalSet as the shard's connection
//! tasks and its expiry timer. A `RefCell` borrow of any per-shard cell held ACROSS an
//! `.await` would double-borrow-panic when an interleaved connection task on the same
//! thread borrows the same cell. So [`run_remote`] takes and releases every borrow
//! INSIDE one synchronous call and holds NOTHING across the `rx.recv().await` in the
//! drain loop, exactly the contract the expiry timer task already follows.

use crate::serve::{ShardState, ShardStoreImpl, shard_env, shard_state, shard_store, shard_wheel};
use ironcache_env::Clock;
use ironcache_runtime::bootstrap::ShardId;
use ironcache_server::dispatch::ServerContext;
// `Admit` (evict_to_fit) + `Store` (used_memory) are brought into scope for the load-on-boot
// maxmemory enforcement (durability fix #4); the concrete shard store implements both.
use ironcache_server::{
    CommandClass, CounterDeltas, ProtoVersion, Request, UnixMillis, Value, classify,
    dispatch_remote_keyed, dispatch_remote_whole_keyspace,
};
use ironcache_storage::{Admit, Store};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

// #391 PR-2 RECEIVER-role boot substitution. The streamed handoff rides an AF_UNIX socket, so the
// receive path is unix-only; these types back the receive-load helpers + their socket-pair tests.
#[cfg(unix)]
use crate::upgrade::stream::HandoffError;
#[cfg(unix)]
use ironcache_repl::ReplOffset;
#[cfg(unix)]
use ironcache_storage::{AccountingHook, EvictionHook};
#[cfg(unix)]
use ironcache_store::ShardStore;
#[cfg(unix)]
use std::time::Duration;
#[cfg(unix)]
use tokio::io::{AsyncRead, AsyncWrite};

/// The bounded depth of each shard's cross-shard inbound queue (COORDINATOR.md #107).
///
/// A bounded channel gives back-pressure for free: when a shard's queue is full, the
/// enqueuing home core AWAITS in [`dispatch_via`] until the owning shard drains one,
/// rather than growing an unbounded backlog under a cross-shard hot-key flood. 1024 is a
/// deliberate first cut: deep enough that a momentary burst does not serialize home
/// cores, shallow enough to bound memory. A fast `-BUSY`-style rejection threshold (fail
/// rather than await past a high-water mark) is a deferred knob (the open `-BUSY` knob,
/// COORDINATOR.md); PASS 1 uses pure await-on-full.
pub const INBOX_DEPTH: usize = 1024;

/// One unit of cross-shard work: a single-key command to run on the shard that OWNS its
/// key, plus the oneshot the owning shard sends the reply back on.
///
/// All fields are `Send` so the envelope crosses the thread boundary: [`Request`] is
/// `Vec<Bytes>` (refcounted byte buffers), `db` is a `u32`, and the oneshot sender is
/// `Send`. The reply travels back as a [`ShardReply`].
#[derive(Debug)]
pub struct ShardWork {
    /// The decoded request to run on the owning shard (cloned/moved from the home core;
    /// the clone is cheap, `Bytes` are refcounted).
    pub request: Request,
    /// The logical database the issuing connection had selected (`SELECT`), so the
    /// remote command runs against the right DB on the owning shard.
    pub db: u32,
    /// The channel the owning shard sends the reply back on (consumed once).
    pub reply: oneshot::Sender<ShardReply>,
}

/// The reply for one [`ShardWork`]: the command's [`Value`] plus the counter deltas it
/// produced on the owning shard.
///
/// The `deltas` are carried back ONLY so the home core does not DOUBLE-COUNT the data
/// deltas: the owning shard has ALREADY folded them into its own counters (where the
/// data lives), so the home core ignores `deltas` for the data figures and only
/// attributes the connection-level `commands_processed`. They are returned (not dropped
/// remotely) so a future observability pass can attribute cross-shard work if desired.
#[derive(Debug)]
pub struct ShardReply {
    /// The reply value to encode on the home core with the home connection's proto.
    pub value: Value,
    /// The counter deltas the command produced on the owning shard (already folded
    /// there; see the struct docs for why they ride back).
    pub deltas: CounterDeltas,
}

/// The set of per-shard inbound queues, indexed by shard. Shared (cloned) into every
/// shard's serve closure so any home core can enqueue to any owning shard.
///
/// `Arc<[Sender]>` (a shared SLICE, not a `Vec`) is the right shape: it is built once at
/// boot, never resized, and cloned cheaply per connection; the senders are `Send + Sync`
/// (tokio MPSC). This is NOT a `std::sync` lock (the invariant the hot-path lint guards):
/// it is an `Arc` over lock-free channel senders.
pub type Inbox = Arc<[mpsc::Sender<ShardWork>]>;

/// The receiver end of a single deferred cross-shard hop (from [`dispatch_via_send`]); the serve
/// loop parks a run of these and drains them together via [`finish_hop`].
pub type HopReceiver = oneshot::Receiver<ShardReply>;

/// The out-param `route_and_dispatch` sets to tell the serve loop whether the command was DEFERRED
/// as a cross-shard hop (#8 overlap): `NotHop` = a synchronous/barrier command whose reply is
/// already in `out` (run its hooks now); `Deferred(rx)` = a remote hop that was enqueued but not
/// awaited (park it, drain the reply in order later). `Deferred(None)` means the owner was gone at
/// send time, so `finish_hop` will encode the shard-unavailable error in order.
#[derive(Debug, Default)]
pub enum HopOutcome {
    /// The command produced its reply synchronously (or is a park/close); not a deferred hop.
    #[default]
    NotHop,
    /// A remote single-key hop was enqueued; its reply receiver (`None` = owner gone at send).
    Deferred(Option<HopReceiver>),
}

/// Build `n` bounded per-shard inbound queues, returning the shared [`Inbox`] of senders
/// (one per shard, captured into the serve closure) and the matching receivers (one per
/// shard, handed to that shard's [`run_drain_loop`] by the bootstrap).
///
/// Each channel is bounded to [`INBOX_DEPTH`] for await-on-full back-pressure. The
/// returned `Vec<Receiver>` is in shard-index order, so `receivers[i]` belongs to shard
/// `i`; the bootstrap moves each out by index.
///
/// # Panics
///
/// Panics if `n == 0` (a running server has at least one shard; the caller passes
/// `config.shards.max(1)`).
#[must_use]
pub fn build_inboxes(n: usize) -> (Inbox, Vec<mpsc::Receiver<ShardWork>>) {
    assert!(n >= 1, "build_inboxes requires at least one shard");
    let mut senders = Vec::with_capacity(n);
    let mut receivers = Vec::with_capacity(n);
    for _ in 0..n {
        let (tx, rx) = mpsc::channel::<ShardWork>(INBOX_DEPTH);
        senders.push(tx);
        receivers.push(rx);
    }
    (Inbox::from(senders), receivers)
}

/// Sample each shard's cross-shard inbox OCCUPANCY (#556, the coordinator back-pressure gauge):
/// `max_capacity - capacity` per shard, i.e. the number of cross-shard work items currently queued
/// and not yet drained. Returned in shard-index order (`[i]` is shard `i`'s inbox).
///
/// This is the SAMPLING side of the inbox-depth gauge: it is read ONLY at `/metrics` scrape time
/// (the cold observability path), NEVER on the hop hot path. The tokio mpsc channel already tracks
/// its own length, so this needs no per-enqueue/dequeue atomic and adds ZERO cost to
/// [`dispatch_via_send`] / [`run_drain_loop`] -- the reason inbox depth is SAMPLED rather than
/// counted (the four-series design in #556). `capacity()` counts free slots (queued items plus any
/// outstanding reserved permits); the coordinator only ever uses plain `send().await` (no long-lived
/// permits), so `max_capacity - capacity` equals the queued-item count in practice.
#[must_use]
pub fn inbox_depths(inbox: &Inbox) -> Vec<u64> {
    inbox
        .iter()
        .map(|tx| (tx.max_capacity().saturating_sub(tx.capacity())) as u64)
        .collect()
}

/// The per-shard DRAIN LOOP (COORDINATOR.md #107): consume cross-shard work for the keys
/// THIS shard owns, run each unit against this shard's thread-locals, and reply.
///
/// Spawned once per shard on the shard's LocalSet by the bootstrap (alongside the accept
/// loop), parameterized by `ctx` (the shard's [`ServerContext`], for the admission budget
/// / policy generation / databases / boot policy name). It loops until every [`Inbox`]
/// sender is dropped (server shutdown), running [`run_remote`] per unit and sending the
/// reply on the unit's oneshot (a dropped receiver -- the home connection went away -- is
/// ignored).
///
/// ## Borrow discipline
///
/// NO `RefCell` borrow is held across the `rx.recv().await`: [`run_remote`] is a
/// synchronous call that acquires + releases every per-shard borrow before returning, so
/// when the loop suspends on `recv()` nothing of this shard's state is borrowed and an
/// interleaved connection task can borrow freely (the same contract the expiry timer
/// follows). See the module docs.
// This is the per-shard boot sequence + the steady-state AND post-shutdown drain loops in one
// function (they share every captured handle); it sits at the line budget by nature, so the #391
// PR-2 receiver-role branch tips it just over -- allow it, matching the house style for the other
// long boot/serve functions (serve.rs / replica_attach.rs).
#[allow(clippy::too_many_lines)]
pub async fn run_drain_loop(
    shard_index: usize,
    mut rx: mpsc::Receiver<ShardWork>,
    ctx: ServerContext,
    inbox: Inbox,
    persist: Option<Arc<crate::persist::PersistState>>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    ready: Option<Arc<crate::metrics_http::ReadyState>>,
) {
    // The runtime TIMER seam (ADR-0003) the shutdown-flag poll below arms; imported at the top of
    // the fn (not mid-body) so clippy's items-after-statements stays happy.
    use ironcache_runtime::Runtime;
    // Bring up THIS shard's background tasks AT SHARD BOOT (COORDINATOR.md #107): lazily
    // init the per-shard handles + spawn the active-expiry timer ONCE. The drain loop is
    // spawned on the shard's LocalSet, so this is the shard-boot hook a connectionless
    // (but key-owning) shard needs -- a shard can now own keys without ever accepting a
    // connection, so its expiry timer must start here, not on first connection. Idempotent
    // (guarded), so the serve loop calling it again per connection is harmless.
    // Adopt THIS shard's metrics cell (OBSERVABILITY.md, #152) BEFORE `ensure_shard_started`
    // builds the `ShardState` (whose `ShardCounters` must wrap the adopted cell). A no-op when
    // the `/metrics` endpoint is disabled (`metrics_registry` is `None`).
    crate::serve::adopt_metrics_cell(ctx.metrics_registry.as_ref(), shard_index);
    // Adopt the shared process-global allocator-memory gauge (PROD-SAFETY #1/#2) at shard boot so a
    // key-owning shard's expiry tick publishes the live jemalloc figure even if it never accepts a
    // connection. A no-op once adopted; the figure feeds the maxmemory admission gate.
    crate::serve::adopt_process_memory_gauge(&ctx.process_memory);
    crate::serve::ensure_shard_started(
        ctx.databases,
        ctx.info.maxmemory_policy,
        crate::serve::scan_reserved_bits(ctx.shards),
        ctx.runtime.clone(),
    );

    // #391 PR-2 RECEIVER-ROLE BOOT SUBSTITUTION (see [`resolve_receive_role`]): when this process was
    // booted as the streamed-handoff receiver, THIS shard PULLS + INSTALLS its store over the socket
    // instead of loading from disk. `Ok(false)` on the DEFAULT path (the common case, and every
    // non-unix build), so the disk-load block below runs exactly as today (one cheap branch). A
    // receive FAILURE returns `Err(())`: we install nothing and return WITHOUT signaling this shard
    // ready, so `/readyz` never reports a half-loaded receiver as ready (data-safe; PR-6 turns this
    // into an explicit unserving abort/exit).
    let Ok(received) = resolve_receive_role(&ctx, shard_index).await else {
        return;
    };

    // PERSISTENCE LOAD-ON-BOOT (#58): when a data_dir is configured, THIS shard loads ITS OWN
    // committed snapshot file (`dump-shard-<shard_index>.icss`) into its store BEFORE the drain
    // loop services any remote work and before the shard accepts connections (the drain loop is
    // spawned ahead of the serve loop). A missing / torn / wrong-version file loads NOTHING (the
    // shard starts empty, today's behavior). With persistence OFF (`None`) this whole block is
    // skipped, so the boot path is byte-unchanged. The store borrow is taken + released inside the
    // synchronous load (no `.await` held across it). SKIPPED in the receiver role (`received`): the
    // store was already installed from the stream above, so a disk load would clobber it.
    if let Some(persist) = persist.as_ref().filter(|_| !received) {
        load_shard_on_boot(&ctx, persist, shard_index);
        // LASTSAVE seed (durability footgun fix #2): once, on shard 0, seed the node-level last-save
        // time from the LOADED snapshot's `dump.manifest` save timestamp, so `LASTSAVE` and the INFO
        // `rdb_last_save_time` reflect the on-disk snapshot the node booted from instead of `0` (an
        // operator's "snapshot stale" monitor would otherwise misfire the instant the node boots). A
        // missing / torn manifest (nothing loaded) leaves it at `0`. Shard 0 is the single seed point
        // (one manifest read), mirroring shard 0 hosting the periodic-save timer.
        if shard_index == 0 {
            persist.seed_last_save_from_manifest();
        }
        // The PERIODIC SAVE timer (#58 save policy) is hosted on SHARD 0 only (one timer per node,
        // not N). It is spawned whenever PERSISTENCE is enabled (a data_dir exists) and reads the
        // LIVE save policy from the runtime overlay (`ctx.runtime.save_policy()`) each tick, so a
        // runtime `CONFIG SET save "900 1"` takes effect even on a node booted with the policy OFF
        // (the durability footgun fix: the policy is no longer frozen at boot). While the policy is
        // OFF (interval 0) the loop just idles on a coarse poll and never saves, so a default
        // persistence-off node (no data_dir -> no PersistState) still has NO timer at all. Shard
        // 0's executor (this drain loop) is the natural home-core host for the fan-out (it OWNS the
        // inbox passed here).
        if shard_index == 0 {
            spawn_periodic_save(ctx.clone(), inbox.clone(), Arc::clone(persist));
        }
        // TMPFS HANDOFF CLEANUP (#390): mark this shard's load-on-boot done. The shard that drives
        // the node-wide countdown to zero removes the ephemeral tmpfs handoff staging dir (a no-op
        // unless THIS boot loaded from tmpfs). Placed AFTER the shard-0 seed above, so the durable
        // LASTSAVE seed reads the staging manifest BEFORE cleanup can remove it (shard 0's decrement
        // is required for the count to reach zero, so the seed always precedes any cleanup).
        persist.note_shard_loaded();
    }
    // READINESS SIGNAL (OBSERVABILITY.md, #152): this shard has now finished its load-on-boot --
    // either `load_shard_on_boot` restored its snapshot above (persistence on), or there was nothing
    // to load (persistence off, the `if let` was skipped). Decrement the per-shard readiness
    // countdown so `/readyz` reports 200 only AFTER every shard reaches this point, never while a
    // snapshot restore is still in flight (k8s would otherwise route to an empty/partial keyspace).
    // This MUST come after the load above and before the drain loop services any work. `None` when
    // the metrics endpoint is off (a no-op).
    if let Some(ready) = ready.as_ref() {
        ready.signal_shard_loaded();
    }
    // HA-7d LIVE replica attach: ONLY in raft-governance mode (`ctx.raft.is_some()`). The
    // DEFAULT static path and the raft-control-plane-WITHOUT-replicas path are byte-unchanged:
    // this is the SOLE invocation of `replica_attach`, gated here, and it does no work until an
    // `AssignReplica` naming this node is committed into `ctx.cluster`. It installs this shard's
    // primary repl observer + listener and spawns the replica control task on the shard's
    // LocalSet (the drain loop runs there), exactly the executor `spawn_on_shard` needs. It is
    // idempotent per shard (guarded), so a connection arriving before the drain loop's first
    // poll calling it again is harmless. The store handle is the SAME `Rc` the serve loop holds.
    if ctx.raft.is_some() {
        let store_rc = crate::serve::shard_store(
            ctx.databases,
            ctx.info.maxmemory_policy,
            crate::serve::scan_reserved_bits(ctx.shards),
        );
        let (bind, port) = (ctx.boot.bind, ctx.info.tcp_port);
        crate::replica_attach::spawn_on_shard(&ctx, store_rc, bind, port, shard_index);
    }
    // TURNKEY cluster formation (PROD-turnkey): on SHARD 0 only (one driver per node, mirroring the
    // periodic-save host) in raft-mode WITH a static topology, spawn the bootstrap driver. Once this
    // node is the leader AND the committed config is still FRESH (a truly fresh cluster), it proposes
    // the topology's declared node table + slot ownership through the unchanged Raft propose path, so
    // a deploy from the shipped static topology converges to cluster_state:ok + full slot coverage
    // WITHOUT a manual CLUSTER MEET / ADDSLOTS. It is fresh-only + idempotent (the guard goes false
    // the instant the bootstrap commits, and a restart recovers a non-empty committed config), so it
    // NEVER re-bootstraps / clobbers a committed config / runtime migration. A topology that declares
    // no slots (e.g. the existing acceptance tests) makes the driver a no-op, leaving their manual
    // MEET/ADDSLOTS flow untouched. The store handle is the SAME shard-0 LocalSet the driver pins to.
    if shard_index == 0 {
        if let (Some(cluster), Some(raft), Some(topology)) = (
            ctx.cluster.clone(),
            ctx.raft.clone(),
            ctx.boot.cluster_topology.as_ref(),
        ) {
            crate::turnkey_bootstrap::spawn_on_shard(cluster, raft, topology);
        }
    }
    // SAVE-ON-EXIT WATCH (#139, SHUTDOWN.md): EVERY shard's drain loop watches the shared shutdown
    // flag concurrently with the cross-shard work recv so a SIGTERM/SIGINT-triggered graceful stop is
    // observed promptly and the drain loop RETURNS (the bootstrap awaits this task before the shard
    // thread joins, so a returned drain loop lets shutdown proceed quickly -- no 5s park). The flag
    // load is a single relaxed atomic on a cold timer tick, so the steady-state hot recv path is
    // unaffected (the default persistence-off posture also takes this loop, but the post-flag SAVE is
    // gated on a save policy below, so it is a pure no-op there).
    //
    // SHARD 0 with a SAVE POLICY is the SAVE host: when the flag is set it performs ONE final save
    // (reusing the SAME atomic save path SAVE/the periodic timer use) BEFORE it returns, while the
    // OTHER shards' drain loops are STILL servicing the fan-out (they observe the flag too but keep a
    // brief post-flag drain so shard 0's `__ICSAVE` reaches them). It then `exit(0)`s -- the
    // orchestrator contract -- so the committed manifest is durable before exit and the atomic
    // tmp->rename leaves no torn file even as sibling tasks are torn down. With NO save policy shard 0
    // simply returns like any other shard and the binary's `shutdown_and_join` drives the clean exit.
    let rt = ironcache_runtime::TokioRuntime::new();
    // Poll the flag on a short cadence through the runtime timer SEAM (NOT tokio::time, ADR-0003),
    // racing it against the work recv so live cross-shard work is still serviced until the stop.
    let poll = std::time::Duration::from_millis(20);
    let stop_requested = loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(work) => {
                        let reply = run_drained_unit(&ctx, &work.request, work.db).await;
                        let _ = work.reply.send(reply);
                        // BLOCKING WAKE (PROD-9): a cross-shard WRITE that ran on THIS shard and may
                        // have ADDED an element to a list/zset wakes a blocking waiter parked on
                        // THIS shard's registry (a BLPOP/BZPOPMIN/... blocked on a key whose owner
                        // is this shard, while the WRITER issued the push from a connection homed on
                        // a different shard). Cheap: a single match + an empty-Vec check off the hot
                        // path (`wake_keys_for_write` is empty for every read / non-adding command).
                        crate::serve::wake_blocking_waiters_for_shard(work.db, &work.request);
                        // KEYSPACE NOTIFICATIONS (PROD-8): a cross-shard keyed write that ran on
                        // THIS shard recorded its keyspace events into this shard's pending buffer;
                        // drain + publish them AFTER the reply is sent. Short-circuits on an empty
                        // buffer (the common case: a read, or notifications disabled).
                        publish_pending_keyspace_events(&inbox, shard_index);
                    }
                    // All senders dropped (the process is already tearing down): stop the loop. Not a
                    // flag-driven stop, so no save is attempted here.
                    None => break false,
                }
            }
            () = rt.timer(poll) => {
                if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                    break true;
                }
            }
        }
    };

    if stop_requested {
        // A save policy is the LIVE runtime policy (a `CONFIG SET save` may have turned it on/off
        // since boot), so read it from the runtime overlay, gated on persistence being enabled.
        let is_save_host = shard_index == 0 && persist.is_some() && ctx.runtime.has_save_policy();
        if is_save_host {
            // SHARD 0 SAVE HOST: run the final save (fan-out to the still-alive sibling drain loops),
            // then exit 0. A save FAILURE is logged inside the helper; we still exit 0 (the prior
            // committed snapshot stays valid; the design's non-zero-on-truncation exit-code map is
            // #139's open follow-up).
            let _ = save_on_exit_if_configured(persist.as_ref(), &ctx, &inbox).await;
            tracing::info!("ironcache: save-on-exit complete -> exit 0");
            std::process::exit(0);
        }
        // A NON-save-host shard on a graceful stop: keep servicing the cross-shard fan-out (so shard
        // 0's `__ICSAVE` is answered) for a BOUNDED post-flag window, then return. The window is
        // bounded by an idle gap (return after a short stretch with no work) AND a hard tick cap, so
        // it covers the fast save fan-out without ever parking shutdown. On a process the save host
        // `exit(0)`s, this shard is simply torn down mid-window (harmless); on a no-save-policy stop
        // it returns on the first idle gap, so non-persistence shutdown stays prompt.
        let mut idle_ticks: u32 = 0;
        // ~1s hard cap (50 * 20ms) -- generous for the save fan-out, far under any supervisor grace.
        let hard_cap_ticks: u32 = 50;
        // Return after ~120ms idle (6 * 20ms) with no fan-out work: the save has reached us already.
        let idle_gap_ticks: u32 = 6;
        let mut total_ticks: u32 = 0;
        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some(work) => {
                            idle_ticks = 0;
                            let reply = run_drained_unit(&ctx, &work.request, work.db).await;
                            let _ = work.reply.send(reply);
                            // KEYSPACE NOTIFICATIONS (PROD-8): publish any events this cross-shard
                            // write recorded, exactly as the steady-state arm above (the
                            // post-shutdown window still services live cross-shard work).
                            publish_pending_keyspace_events(&inbox, shard_index);
                        }
                        None => return,
                    }
                }
                () = rt.timer(poll) => {
                    idle_ticks = idle_ticks.saturating_add(1);
                    total_ticks = total_ticks.saturating_add(1);
                    if idle_ticks >= idle_gap_ticks || total_ticks >= hard_cap_ticks {
                        return;
                    }
                }
            }
        }
    }
}

/// Dispatch ONE drained cross-shard unit, routing the async YIELDING save path for `__ICSAVE`
/// and the SYNCHRONOUS [`run_remote`] for everything else (#571).
///
/// This is the single drain-loop entry point. The `__ICSAVE` partial dumps the WHOLE shard
/// partition, and the dump now YIELDS between snapshot chunks (a bounded, predictable BGSAVE tail
/// instead of a full-keyspace block) via [`run_local_save`], so it MUST run on an async path. Every
/// OTHER unit -- the hot cross-shard keyed commands and the whole-keyspace partials -- stays on the
/// SYNCHRONOUS `run_remote` (which returns before any await, holding no borrow across one), so the
/// hot path is NOT dragged onto the async state machine: the `.await` here resolves immediately for
/// the non-save common case.
async fn run_drained_unit(ctx: &ServerContext, request: &Request, db: u32) -> ShardReply {
    if crate::serve::ascii_upper(request.command()) == crate::persist::ICSAVE {
        return run_local_save(ctx, request).await;
    }
    run_remote(ctx, request, db)
}

/// Run ONE unit of remote keyed work against THIS shard's thread-local state, returning
/// the reply + the deltas it produced (already folded into this shard's counters).
///
/// This is the synchronous heart of the drain loop: it lazily inits + BRIEFLY borrows
/// this shard's thread-local ENV / STORE / WHEEL / ShardState (the SAME accessors
/// `handle_request` uses, so the per-shard lazy-init is shared), reads `now` from THIS
/// shard's Env clock (the determinism seam, ADR-0003 -- NOT a home-supplied now), runs
/// [`dispatch_remote_keyed`], folds the resulting [`CounterDeltas`] into THIS shard's
/// counters (the data lives here, so the data counters live here too), and returns the
/// reply + a COPY of the deltas (so the home core can avoid double-counting).
///
/// Every borrow is taken and dropped inside this function: nothing escapes to be held
/// across the caller's `.await` (the no-borrow-across-await contract).
fn run_remote(ctx: &ServerContext, request: &Request, db: u32) -> ShardReply {
    // INTERNAL `__ICPUBLISH <channel> <payload>` (SERVER_PUSH.md #20, PR 91a): the cross-shard
    // PUBLISH fan-out. It touches the per-shard SUBSCRIPTION table, NOT the store/wheel/env, so
    // it is handled BEFORE any store borrow and produces NO counter deltas (a PUBLISH is not a
    // keyed write). Delivery is synchronous non-blocking `try_send` (see `run_local_publish`),
    // so no RefCell borrow is held across an `.await` (the drain loop's no-borrow-across-await
    // contract). It returns the count of LOCAL subscribers that received the message; the home
    // core SUMS the per-shard counts into the PUBLISH integer reply.
    if crate::serve::ascii_upper(request.command()) == ironcache_server::ICPUBLISH {
        return ShardReply {
            value: run_local_publish(request),
            deltas: CounterDeltas::default(),
        };
    }

    // INTERNAL `__ICSPUBLISH <channel> <payload>` (#410): the cross-shard SHARDED PUBLISH
    // fan-out, the SPUBLISH analog of `__ICPUBLISH`. Delivers to THIS shard's LOCAL shard-channel
    // subscribers (NO pattern delivery) and returns the local receiver count; the home core SUMS
    // the per-shard counts into the SPUBLISH integer reply. Same no-store / no-deltas /
    // no-borrow-across-await contract as `__ICPUBLISH`.
    if crate::serve::ascii_upper(request.command()) == ironcache_server::ICSPUBLISH {
        return ShardReply {
            value: run_local_spublish(request),
            deltas: CounterDeltas::default(),
        };
    }

    // NOTE (#571): `__ICSAVE` is NOT handled here. Because the per-shard dump now YIELDS between
    // snapshot chunks (a bounded save tail, not a full-keyspace block), it is an ASYNC unit and is
    // dispatched by [`run_drained_unit`] on the drain loop BEFORE this synchronous `run_remote` is
    // reached (via the yielding [`run_local_save`]). Keeping `run_remote` fully SYNCHRONOUS keeps the
    // hot cross-shard keyed / whole-keyspace path off the async state machine; only the save path is
    // async. A stray `__ICSAVE` reaching here would fall through to the keyed dispatcher, which
    // defensively refuses a mis-routed internal verb -- but `run_drained_unit` intercepts it first.

    // INTERNAL `__ICPUBSUB <subcommand> [args]` (SERVER_PUSH.md #20, PR 91b): the cross-shard
    // PUBSUB-introspection gather. Like `__ICPUBLISH` it touches ONLY the per-shard SUBSCRIPTION
    // table (read-only) and produces NO counter deltas (introspection is not a keyed write), so it
    // is handled BEFORE any store borrow. It returns THIS shard's local partial (channels with a
    // local sub / per-channel local counts / local patterns); the home core MERGES the partials.
    if crate::serve::ascii_upper(request.command()) == ironcache_server::ICPUBSUB {
        return ShardReply {
            value: run_local_pubsub(request),
            deltas: CounterDeltas::default(),
        };
    }

    // INTERNAL `__ICEXISTS <key>` (HA-6 online slot migration on a MULTI-SHARD node,
    // COORDINATOR.md #107): the cross-shard PRESENCE query. The migration SOURCE shard's ASK
    // decision needs to know whether a migrating-slot key is still PRESENT on the shard that
    // OWNS it (the FNV `owner_shard`), which on a multi-shard node may be a SIBLING of the
    // accept shard. This runs a PURE `contains_live` read against THIS shard's store: it never
    // reaps the key, fires a hook, or folds a counter (so NO deltas, like the pub/sub verbs),
    // exactly the cold-redirect-safe semantics of the local `contains_live`. It reads `now` from
    // THIS shard's Env clock (the determinism seam, ADR-0003 -- the owning shard's wall clock,
    // not a home-supplied timestamp) and borrows the store read-only, both taken + released
    // inside this synchronous call (the no-borrow-across-await contract). It replies `:1` when
    // the key is present-and-live here, else `:0` (a malformed `__ICEXISTS` with no key -> `:0`).
    if crate::serve::ascii_upper(request.command()) == ironcache_server::ICEXISTS {
        let present = request.args.get(1).is_some_and(|key| {
            let env = shard_env();
            let store_rc = shard_store(
                ctx.databases,
                ctx.info.maxmemory_policy,
                crate::serve::scan_reserved_bits(ctx.shards),
            );
            let now = UnixMillis(env.borrow().now_unix_millis());
            // A SHORT read-only borrow that drops before this closure returns; no `.await` runs
            // while it is held (this whole `run_remote` is synchronous).
            store_rc.borrow().contains_live(db, key.as_ref(), now)
        });
        return ShardReply {
            value: Value::Integer(i64::from(present)),
            deltas: CounterDeltas::default(),
        };
    }

    // Lazily init + clone the per-shard handles (Rc clones, cheap), exactly as
    // serve_connection / handle_request do. These accessors are the shared per-shard
    // lazy-init, so the drain loop and the connection tasks see the SAME store/wheel/env.
    let env = shard_env();
    let store_rc = shard_store(
        ctx.databases,
        ctx.info.maxmemory_policy,
        crate::serve::scan_reserved_bits(ctx.shards),
    );
    let wheel_rc = shard_wheel();
    let state_rc = shard_state();

    // Read `now` once from THIS shard's wall clock (ADR-0003: the determinism seam is the
    // owning shard's Env, not a home-supplied timestamp), via a SHORT shared borrow that
    // drops before the mutable store/wheel borrows below (distinct RefCells, no alias).
    let now = UnixMillis(env.borrow().now_unix_millis());

    // Copy the shard's last-seen policy generation OUT into a local so dispatch can take
    // `&mut` it without holding a state_rc borrow across the store/wheel borrows (mirrors
    // handle_request's discipline; the rollup closure does not exist here, but the
    // separate-borrow discipline is identical).
    let mut shard_generation = state_rc.borrow().last_policy_generation;

    // Pick the per-shard dispatcher by command class. KEYED commands (single/multi) run
    // the full keyed path (policy hot-swap + active drain + admission gate); WHOLE-KEYSPACE
    // partials (the scatter-gather fan-out, COORDINATOR.md #107) run the lean keyspace path
    // (no admission/expiry: a count/iterate/flush/random is not a denyoom write). Anything
    // else never reaches the drain loop (the serve loop only enqueues those two classes);
    // dispatch_remote_* refuses a mis-routed command defensively.
    let cmd_upper = crate::serve::ascii_upper(request.command());

    // -- THE `-LOADING` WRITE-QUIESCE GATE ON THE CROSS-SHARD PATH (#391, Decision 2 Option C). A
    // write to a key THIS shard OWNS but that a SIBLING home core received is dispatched HERE (via
    // the inbox), NOT through this shard's `route_and_dispatch`, so the quiesce must ALSO reject it
    // on the OWNING shard -- BEFORE `dispatch_remote_keyed` reaches the store's write funnel and
    // assigns a ring offset. Without this second gate a cross-shard write could land above this
    // shard's latched cut offset E during the brief window where the owner is quiesced but the
    // sender's home shard is not yet (the flag is PER-SHARD, so the two do not flip atomically). It
    // reads THIS (owning) shard's core-local flag on THIS shard's thread: one predictable-not-taken
    // bool load on the default path, short-circuiting the write classifier. Cross-shard hops are
    // never inside a MULTI (a cross-shard transaction is rejected at queue time), so the classifier
    // runs with `in_multi = false` / no staged batch. Reads pass straight through.
    if crate::serve::is_shard_loading()
        && ironcache_server::request_is_write_for_pause(&cmd_upper, false, &[])
    {
        return ShardReply {
            value: Value::error(ironcache_protocol::ErrorReply::loading()),
            deltas: CounterDeltas::default(),
        };
    }

    // The internal whole-keyspace partials run `cmd_keyspace` / `db_len` reads over THIS shard's
    // partition, but are NOT in `spec_of`, so `classify` returns `AlwaysHome` for them; allow-list
    // them alongside the classified set: the two #371 slot-scans and the #531 `__ICINFOKEYSPACE`
    // per-db key-count gather for the node-wide INFO `# Keyspace`.
    let is_whole_keyspace = matches!(classify(&cmd_upper), CommandClass::WholeKeyspace)
        || cmd_upper == ironcache_server::ICCOUNTKEYSINSLOT
        || cmd_upper == ironcache_server::ICGETKEYSINSLOT
        || cmd_upper == ironcache_server::ICINFOKEYSPACE;

    let mut deltas = CounterDeltas::default();
    let lazy_expired;
    let value = {
        let mut store = store_rc.borrow_mut();
        let mut wheel = wheel_rc.borrow_mut();
        // The Env is a SEPARATE RefCell from store/wheel; the mutable borrow here (for the
        // RNG-drawing members + the policy hot-swap seed) does not alias the held
        // store/wheel borrows. `now` was read above from a distinct, now-dropped borrow.
        let mut env_ref = env.borrow_mut();
        let v = if is_whole_keyspace {
            // The whole-keyspace partial reads no wheel / generation; it runs the SAME
            // cmd_keyspace::* handlers against THIS shard's partition.
            dispatch_remote_whole_keyspace(&mut *env_ref, &mut *store, db, now, request)
        } else {
            dispatch_remote_keyed(
                ctx,
                &mut *env_ref,
                &mut *store,
                &mut wheel,
                db,
                now,
                &mut shard_generation,
                &mut deltas,
                request,
            )
        };
        drop(env_ref);
        // Drain the lazy-backstop expiry the command produced (the store accumulates it
        // inside the primitives), folding it into expired_keys alongside the active drain,
        // exactly like handle_request.
        lazy_expired = store.take_lazy_expired();
        v
        // store + wheel borrows DROP here, before the state borrow below.
    };

    // Fold this command's deltas into THIS shard's counters (the data lives here) and
    // write back the possibly-advanced policy generation. The home core will NOT re-apply
    // these data deltas (it only attributes commands_processed for the issuing conn).
    {
        deltas.expired += lazy_expired;
        let mut st = state_rc.borrow_mut();
        if deltas != CounterDeltas::default() {
            st.counters.apply(deltas);
        }
        // COORDINATOR HOP OBSERVABILITY (#556, the #517 zero-hop measurement harness): THIS shard
        // just SERVED a peer's cross-shard request off its inbox (a remote keyed or whole-keyspace
        // unit -- the pure-infra verbs `__ICPUBLISH`/`__ICEXISTS`/`__ICSAVE`/... returned earlier and
        // are not client requests). ONE relaxed atomic on the borrow the drain path ALREADY takes, so
        // no new alloc / clock / lock. Symmetric with the home shard's `hops_sent` bump in
        // `route_and_dispatch`; an operator sees how much work each shard does FOR its peers.
        st.counters.on_hop_served();
        st.last_policy_generation = shard_generation;
    }

    ShardReply { value, deltas }
}

/// Run a [`CommandClass::WholeKeyspace`](ironcache_server::CommandClass) command's PARTIAL
/// against THIS (home) shard's thread-local state, SYNCHRONOUSLY, returning the home
/// shard's [`ShardReply`] (COORDINATOR.md #107, the whole-keyspace fan-out). This is the
/// `local` closure [`fan_out_all`] runs for the home shard -- the home core does NOT
/// round-trip its OWN partial through its channel; it runs it inline, exactly like the
/// single-key local fast path.
///
/// It reads `now` from THIS shard's Env clock (the determinism seam, ADR-0003) and runs
/// the SAME [`dispatch_remote_whole_keyspace`] the remote shards run, so the home shard's
/// partial is byte-identical to every other shard's. Whole-keyspace partials produce no
/// counter deltas to fold (a count/iterate/flush/random is not counted), so the returned
/// [`ShardReply`] carries default deltas. Every per-shard borrow is taken + released inside
/// this synchronous call (the no-borrow-across-await contract; the caller awaits remote
/// replies AFTER this returns).
pub fn run_local_whole_keyspace(ctx: &ServerContext, request: &Request, db: u32) -> ShardReply {
    let env = shard_env();
    let store_rc = shard_store(
        ctx.databases,
        ctx.info.maxmemory_policy,
        crate::serve::scan_reserved_bits(ctx.shards),
    );
    let now = UnixMillis(env.borrow().now_unix_millis());
    let lazy_expired;
    let value = {
        let mut store = store_rc.borrow_mut();
        let mut env_ref = env.borrow_mut();
        let v = dispatch_remote_whole_keyspace(&mut *env_ref, &mut *store, db, now, request);
        drop(env_ref);
        // A whole-keyspace read may lazily expire keys it skips; drain + fold the backstop
        // count into THIS shard's expired_keys, exactly as run_remote / handle_request do.
        lazy_expired = store.take_lazy_expired();
        v
    };
    if lazy_expired > 0 {
        let state_rc = shard_state();
        state_rc.borrow_mut().counters.apply(CounterDeltas {
            expired: lazy_expired,
            ..CounterDeltas::default()
        });
    }
    ShardReply {
        value,
        deltas: CounterDeltas::default(),
    }
}

/// The HOME-CORE side of a cross-shard hop (COORDINATOR.md #107): enqueue `request` to
/// the shard that owns its key, await the reply, and encode it on the home core with the
/// home connection's `proto`.
///
/// Build a oneshot, send the [`ShardWork`] to `inbox[target]` (AWAITS if that shard's
/// queue is full -- the back-pressure), then await the reply. If the send fails or the
/// oneshot errs (the owning shard's drain loop is gone, e.g. during shutdown), encode a
/// proto-shaped error so the connection gets a well-formed reply rather than a hang.
///
/// The home core does NOT re-apply `reply.deltas` (the owning shard already folded the
/// data deltas into its own counters); attributing the issuing connection's
/// `commands_processed` is the serve loop's job (it does so the same way for the local
/// fast path), so this function only produces the encoded reply bytes.
pub async fn dispatch_via(
    inbox: &Inbox,
    target: usize,
    request: &Request,
    db: u32,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    // The full hop = the SEND half then the AWAIT half, back to back (the non-pipelined path).
    let rx = dispatch_via_send(inbox, target, request, db).await;
    finish_hop(rx, out, proto).await;
}

/// The SEND half of a cross-shard hop (COORDINATOR.md #107): build the oneshot, enqueue the
/// [`ShardWork`] to `inbox[target]`, and return the reply receiver WITHOUT awaiting it -- so a
/// PIPELINE of hops can be issued back-to-back (the owner is a single FIFO consumer that drains the
/// whole run) and their replies awaited together in [`finish_hop`], collapsing N serialized
/// round-trips into one. This is the same enqueue-all-then-await-all shape [`fan_out_all`] already
/// uses. Returns `None` if the owning shard's receiver is gone (shutdown / shard died); the caller
/// records that and [`finish_hop`] encodes the shard-unavailable error IN ORDER at assembly time.
///
/// `send().await` still applies the bounded-queue back-pressure (suspends only if the owner's queue
/// is full), exactly as the non-pipelined path did.
pub async fn dispatch_via_send(
    inbox: &Inbox,
    target: usize,
    request: &Request,
    db: u32,
) -> Option<oneshot::Receiver<ShardReply>> {
    let (tx, rx) = oneshot::channel::<ShardReply>();
    let work = ShardWork {
        // Clone is cheap: Request is Vec<Bytes> (refcounted buffers).
        request: request.clone(),
        db,
        reply: tx,
    };
    if inbox[target].send(work).await.is_err() {
        return None;
    }
    Some(rx)
}

/// The AWAIT half of a cross-shard hop: await the reply from [`dispatch_via_send`] and encode it
/// into `out` with the home connection's `proto`. A `None` receiver (owner already gone at send) or
/// an oneshot error (owner died mid-flight) both encode the proto-shaped shard-unavailable error --
/// byte-identical to the fused [`dispatch_via`]. The home core deliberately does NOT re-apply
/// `reply.deltas` (the owning shard already folded those data counters into its own `ShardState`;
/// applying them here too would double-count); the issuing connection's `commands_processed` is
/// bumped by the serve loop, matching the local fast path.
pub async fn finish_hop(
    rx: Option<oneshot::Receiver<ShardReply>>,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    match rx {
        Some(rx) => match rx.await {
            Ok(reply) => {
                let _ = &reply.deltas;
                encode_into(out, &reply.value, proto);
            }
            Err(_) => encode_into(out, &Value::error(shard_unavailable_error()), proto),
        },
        None => encode_into(out, &Value::error(shard_unavailable_error()), proto),
    }
}

/// A SINGLE-TARGET cross-shard hop that returns the owning shard's reply [`Value`] (NOT
/// encoded), so the home core can POST-PROCESS it before encoding -- used by the
/// cross-shard SCAN, which hops to ONE shard per call (the current composite-cursor shard
/// index) and must REWRITE the returned inner cursor into the composite wire cursor before
/// encoding. On a send/await failure (the owning shard's drain loop is gone) it returns
/// the shard-unavailable error [`Value`] so the caller still produces a well-formed reply.
///
/// Like [`dispatch_via`], the home core does NOT re-apply the reply's deltas (the owning
/// shard already folded them); the serve loop bumps the issuing connection's
/// `commands_processed` separately.
pub async fn dispatch_one_value(inbox: &Inbox, target: usize, request: &Request, db: u32) -> Value {
    let (tx, rx) = oneshot::channel::<ShardReply>();
    let work = ShardWork {
        request: request.clone(),
        db,
        reply: tx,
    };
    if inbox[target].send(work).await.is_err() {
        return Value::error(shard_unavailable_error());
    }
    match rx.await {
        Ok(reply) => reply.value,
        Err(_) => Value::error(shard_unavailable_error()),
    }
}

/// CROSS-SHARD KEY-PRESENCE query (HA-6 online slot migration on a MULTI-SHARD node,
/// COORDINATOR.md #107): ask the shard that OWNS `key` whether it is PRESENT and LIVE, returning
/// the bool. This is the cross-shard counterpart of a local `contains_live`, reusing the EXACT
/// single-target hop mechanism a coordinated single-key op already uses ([`dispatch_one_value`]):
/// it builds an `__ICEXISTS <key>` request, enqueues it to `inbox[target]` (await-on-full
/// back-pressure), and awaits the owning shard's `:1` / `:0` reply.
///
/// ## Deadlock-free (the same reasoning as every other single-key cross-shard hop)
///
/// The migration SOURCE shard calls this from its serve loop (the cold migration redirect, BEFORE
/// holding any `RefCell` borrow -- the caller drops every store borrow before awaiting). The owning
/// shard's drain loop runs on a SEPARATE LocalSet / core and answers `__ICEXISTS` in [`run_remote`]
/// (a synchronous `contains_live` read that holds no borrow across an `.await`), so the awaiting
/// source shard never blocks the owner's progress and the owner never re-enters the source. This is
/// byte-for-byte the [`dispatch_via`] / [`dispatch_one_value`] pattern that Stage 1 routing already
/// proved deadlock-free; the presence verb is just a different, side-effect-free unit of work.
///
/// On a dead owner shard (send error / cancelled oneshot, only at shutdown or a shard panic) it
/// returns `false` -- "treat as absent" -- so the SOURCE emits the SAFE, idempotent `-ASK` the
/// client retries (the pre-fix conservative behavior), never a wrong-owner serve.
pub async fn presence_via(inbox: &Inbox, target: usize, key: &[u8], db: u32) -> bool {
    let request = Request {
        args: vec![
            bytes::Bytes::from_static(ironcache_server::ICEXISTS),
            bytes::Bytes::copy_from_slice(key),
        ],
    };
    let (tx, rx) = oneshot::channel::<ShardReply>();
    let work = ShardWork {
        request,
        db,
        reply: tx,
    };
    if inbox[target].send(work).await.is_err() {
        return false;
    }
    match rx.await {
        Ok(reply) => matches!(reply.value, Value::Integer(1)),
        Err(_) => false,
    }
}

/// SCATTER-GATHER a [`CommandClass::WholeKeyspace`](ironcache_server::CommandClass)
/// command across ALL `n_shards` shards and gather the per-shard replies, paired by shard
/// index (COORDINATOR.md #107, the whole-keyspace fan-out). The home core MERGES the
/// returned partials per command (DBSIZE sums, KEYS concatenates, FLUSH all-OK, RANDOMKEY
/// picks one); SCAN uses the single-target [`dispatch_via`] instead (it hops to ONE shard
/// per call, so fan-out is overkill for it).
///
/// The HOME shard (`home`) runs LOCALLY and SYNCHRONOUSLY via the `local` closure (the
/// caller runs `dispatch_remote_whole_keyspace` against the home thread-locals, like the
/// existing local fast path) -- it does NOT round-trip through the home shard's own
/// channel. Every OTHER shard gets a [`ShardWork`] (the same `request` + `db` + a oneshot)
/// and the home core awaits its reply with the usual await-on-full back-pressure. A shard
/// whose drain loop is gone (send error / oneshot cancelled, e.g. during shutdown) yields
/// a SHARD-UNAVAILABLE error reply for that shard rather than hanging or panicking; the
/// caller's merge surfaces it (FLUSH turns any error into a surfaced error; DBSIZE/KEYS
/// treat it as that shard contributing nothing -- documented at each merge site).
///
/// The returned vector is sorted by shard index `0..n_shards` (ordering is irrelevant for
/// DBSIZE/KEYS/FLUSH/RANDOMKEY but the deterministic order keeps the merge reproducible).
/// The requests are dispatched concurrently (all oneshots are created and enqueued, then
/// awaited), so a slow shard does not serialize the others beyond the await-on-full bound.
pub async fn fan_out_all(
    inbox: &Inbox,
    request: &Request,
    db: u32,
    home: usize,
    local: impl FnOnce() -> ShardReply,
) -> Vec<(usize, ShardReply)> {
    let n_shards = inbox.len();
    let mut replies: Vec<(usize, ShardReply)> = Vec::with_capacity(n_shards);

    // Enqueue the work to every NON-home shard first (creating each oneshot), collecting
    // the receivers, so the shards process concurrently while the home core then runs its
    // OWN partial locally and finally gathers the remote replies in shard order.
    let mut pending: Vec<(usize, oneshot::Receiver<ShardReply>)> = Vec::with_capacity(n_shards);
    for target in 0..n_shards {
        if target == home {
            continue;
        }
        let (tx, rx) = oneshot::channel::<ShardReply>();
        let work = ShardWork {
            request: request.clone(),
            db,
            reply: tx,
        };
        // Await-on-full back-pressure. A send error means the owning shard's receiver is
        // gone (shutdown / shard died): record a shard-unavailable reply for it directly
        // (no receiver to await) rather than hang.
        if inbox[target].send(work).await.is_err() {
            replies.push((target, shard_unavailable_reply()));
        } else {
            pending.push((target, rx));
        }
    }

    // The HOME shard's partial: run LOCALLY + SYNCHRONOUSLY on the home thread-locals (the
    // `local` closure), exactly like the single-key local fast path -- no self-channel hop.
    replies.push((home, local()));

    // Gather the remote replies. A cancelled oneshot (the owning shard's drain loop went
    // away after we enqueued) maps to a shard-unavailable reply, never a hang/panic.
    for (target, rx) in pending {
        match rx.await {
            Ok(reply) => replies.push((target, reply)),
            Err(_) => replies.push((target, shard_unavailable_reply())),
        }
    }

    // Sort by shard index so the merge is deterministic (irrelevant for DBSIZE/KEYS/FLUSH/
    // RANDOMKEY, but reproducible). n_shards is small (one per core), so this is cheap.
    replies.sort_by_key(|&(shard, _)| shard);
    replies
}

/// SCATTER a DIFFERENT sub-request to each participating shard concurrently and gather
/// the per-shard replies (COORDINATOR.md #107, Stage 2a -- the multi-key DATA fan-out).
///
/// This GENERALIZES [`fan_out_all`] (which broadcasts the SAME request to every shard)
/// to the multi-key case, where each shard must run a DIFFERENT sub-request (only the
/// keys that shard OWNS): `subreqs` is one `(shard_index, sub_request)` pair PER
/// PARTICIPATING shard (the caller groups the command's keys by owner and builds one
/// sub-request per shard that owns at least one key; a shard owning none is simply
/// absent from `subreqs`).
///
/// The entry whose `shard == home.index` runs LOCALLY + SYNCHRONOUSLY via the `local`
/// closure on the home thread-locals (mirroring [`fan_out_all`] / [`run_local_whole_keyspace`]
/// -- NO self-channel hop). Every OTHER entry is sent as a [`ShardWork`] (that shard's
/// sub-request + `db` + a oneshot) and the home core awaits all the replies concurrently
/// (all enqueued first, then awaited, with the usual await-on-full back-pressure). A dead
/// shard (send error / cancelled oneshot, only at shutdown / a shard panic) yields a
/// SHARD-UNAVAILABLE [`ShardReply`] for that shard rather than a hang.
///
/// Returns the `(shard_index, reply)` pairs in NO guaranteed order (the caller maps each
/// shard's reply back to the original key positions via the index bookkeeping it created
/// when it built `subreqs`, so ordering here is irrelevant -- unlike [`fan_out_all`],
/// which sorts by shard for a reproducible merge). The `local` closure runs SYNCHRONOUSLY
/// and returns before any `.await`, so NO `RefCell` borrow of the home thread-locals is
/// held across the awaits (the no-borrow-across-await contract, exactly as [`fan_out_all`]).
pub async fn fan_out_split(
    inbox: &Inbox,
    home: ShardId,
    db: u32,
    subreqs: Vec<(usize, Request)>,
    local: impl FnOnce(&Request) -> ShardReply,
) -> Vec<(usize, ShardReply)> {
    let mut replies: Vec<(usize, ShardReply)> = Vec::with_capacity(subreqs.len());
    let mut pending: Vec<(usize, oneshot::Receiver<ShardReply>)> =
        Vec::with_capacity(subreqs.len());
    // The home shard's sub-request, deferred so its `local` closure runs AFTER every
    // remote sub-request is enqueued (so the shards process concurrently while the home
    // core then runs its own subset locally and finally gathers the remote replies).
    let mut home_subreq: Option<Request> = None;

    for (shard, req) in subreqs {
        if shard == home.index {
            // The home shard's subset: run it LOCALLY + SYNCHRONOUSLY below (no self hop).
            home_subreq = Some(req);
            continue;
        }
        let (tx, rx) = oneshot::channel::<ShardReply>();
        let work = ShardWork {
            request: req,
            db,
            reply: tx,
        };
        // Await-on-full back-pressure. A send error means the owning shard's receiver is
        // gone (shutdown / shard died): record a shard-unavailable reply for it directly.
        if inbox[shard].send(work).await.is_err() {
            replies.push((shard, shard_unavailable_reply()));
        } else {
            pending.push((shard, rx));
        }
    }

    // The HOME shard's subset (if it owns any key): run LOCALLY + SYNCHRONOUSLY on the
    // home thread-locals, exactly like the single-key local fast path -- no self-channel
    // hop. The closure returns before any await, so no borrow is held across the awaits.
    if let Some(req) = home_subreq {
        replies.push((home.index, local(&req)));
    }

    // Gather the remote replies. A cancelled oneshot (the owning shard's drain loop went
    // away after we enqueued) maps to a shard-unavailable reply, never a hang/panic.
    for (shard, rx) in pending {
        match rx.await {
            Ok(reply) => replies.push((shard, reply)),
            Err(_) => replies.push((shard, shard_unavailable_reply())),
        }
    }

    replies
}

/// Fan an `__ICSAVE` out to EVERY shard for a full cross-shard SAVE (#58/#571), running the home
/// shard's dump on the YIELDING save path INLINE (no self-channel hop) and every other shard off
/// its own drain loop. Returns the `(shard_index, reply)` pairs in NO guaranteed order (the caller
/// maps each shard's reply to its manifest entry by the index it built).
///
/// This mirrors [`fan_out_split`] structurally (enqueue every REMOTE sub-request first so the shards
/// dump concurrently, then run the HOME shard's partial locally, then gather the remote replies), but
/// the home partial is the ASYNC yielding [`run_local_save`] -- which awaits between snapshot chunks
/// so shard 0 services its OWN queued writes during its dump too (the entire benefit for a
/// single-shard node) -- and a synchronous `FnOnce` local closure cannot express that await. NO
/// `RefCell` borrow is held across any await: `run_local_save` releases its per-chunk store borrow
/// before each yield, exactly like `fan_out_split`'s no-borrow-across-await contract.
///
/// A dead shard (send error / cancelled oneshot, only at shutdown / a shard panic) yields a
/// SHARD-UNAVAILABLE [`ShardReply`] for that shard rather than a hang.
pub async fn fan_out_save(
    ctx: &ServerContext,
    inbox: &Inbox,
    home: ShardId,
    db: u32,
    subreqs: Vec<(usize, Request)>,
) -> Vec<(usize, ShardReply)> {
    let mut replies: Vec<(usize, ShardReply)> = Vec::with_capacity(subreqs.len());
    let mut pending: Vec<(usize, oneshot::Receiver<ShardReply>)> =
        Vec::with_capacity(subreqs.len());
    // The home shard's __ICSAVE, deferred so its inline yielding dump runs AFTER every remote
    // sub-request is enqueued (so the shards dump concurrently), exactly as fan_out_split orders it.
    let mut home_subreq: Option<Request> = None;

    for (shard, req) in subreqs {
        if shard == home.index {
            home_subreq = Some(req);
            continue;
        }
        let (tx, rx) = oneshot::channel::<ShardReply>();
        let work = ShardWork {
            request: req,
            db,
            reply: tx,
        };
        // Await-on-full back-pressure; a send error means the owning shard's receiver is gone
        // (shutdown / shard died): record a shard-unavailable reply for it directly.
        if inbox[shard].send(work).await.is_err() {
            replies.push((shard, shard_unavailable_reply()));
        } else {
            pending.push((shard, rx));
        }
    }

    // The HOME shard dumps its OWN partition INLINE on the YIELDING path (no self hop): it awaits
    // between chunks with the per-chunk store borrow released, so shard 0 services its queued writes
    // during its own dump too.
    if let Some(req) = home_subreq {
        replies.push((home.index, run_local_save(ctx, &req).await));
    }

    // Gather the remote replies. A cancelled oneshot (the owning shard's drain loop went away after
    // we enqueued) maps to a shard-unavailable reply, never a hang/panic.
    for (shard, rx) in pending {
        match rx.await {
            Ok(reply) => replies.push((shard, reply)),
            Err(_) => replies.push((shard, shard_unavailable_reply())),
        }
    }

    replies
}

/// Run ONE keyed sub-request's subset against THIS (home) shard's thread-local state,
/// SYNCHRONOUSLY, for the multi-key DATA fan-out (COORDINATOR.md #107, Stage 2a). This is
/// the `local` closure [`fan_out_split`] runs for the home shard: the home core does NOT
/// round-trip its OWN subset through its channel; it runs it inline, exactly like the
/// single-key local fast path and [`run_local_whole_keyspace`].
///
/// It is the byte-identical home-core counterpart of the per-shard [`run_remote`] keyed
/// path: it reads `now` from THIS shard's Env clock (the determinism seam, ADR-0003), runs
/// the SAME [`dispatch_remote_keyed`] every remote shard runs (so the home shard's subset
/// is byte-identical to a remote shard's), and FOLDS the produced [`CounterDeltas`] into
/// THIS shard's counters (the data the sub-MGET/sub-MSET touched lives here, so its data
/// counters live here too). The returned [`ShardReply`] carries a COPY of the deltas so a
/// future observability pass could attribute the home subset (the merge layer ignores
/// them, like every other home-core path). Every per-shard borrow is taken + released
/// inside this synchronous call (the no-borrow-across-await contract).
#[must_use]
pub fn run_local_keyed(ctx: &ServerContext, request: &Request, db: u32) -> ShardReply {
    let env = shard_env();
    let store_rc = shard_store(
        ctx.databases,
        ctx.info.maxmemory_policy,
        crate::serve::scan_reserved_bits(ctx.shards),
    );
    let wheel_rc = shard_wheel();
    let state_rc = shard_state();

    let now = UnixMillis(env.borrow().now_unix_millis());
    let mut shard_generation = state_rc.borrow().last_policy_generation;

    let mut deltas = CounterDeltas::default();
    let lazy_expired;
    let value = {
        let mut store = store_rc.borrow_mut();
        let mut wheel = wheel_rc.borrow_mut();
        let mut env_ref = env.borrow_mut();
        let v = dispatch_remote_keyed(
            ctx,
            &mut *env_ref,
            &mut *store,
            &mut wheel,
            db,
            now,
            &mut shard_generation,
            &mut deltas,
            request,
        );
        drop(env_ref);
        lazy_expired = store.take_lazy_expired();
        v
        // store + wheel borrows DROP here, before the state borrow below.
    };

    {
        deltas.expired += lazy_expired;
        let mut st = state_rc.borrow_mut();
        if deltas != CounterDeltas::default() {
            st.counters.apply(deltas);
        }
        st.last_policy_generation = shard_generation;
    }

    ShardReply { value, deltas }
}

/// Deliver an `__ICPUBLISH <channel> <payload>` to THIS shard's LOCAL subscribers and
/// return the count delivered as a [`Value::Integer`] (SERVER_PUSH.md #20, PR 91a). This is
/// the per-shard delivery the cross-shard PUBLISH fan-out runs on EVERY shard: the home shard
/// runs it LOCALLY via [`fan_out_publish`]'s closure (no self-channel hop), every other shard
/// runs it inside [`run_remote`] off the inbox.
///
/// It looks the channel up in this shard's [`crate::serve::shard_pubsub`] table and
/// `try_send`s a [`crate::pubsub::ServerPush::Message`] to each local subscriber (NEVER
/// `send().await`: a push must not block the publishing shard); a slow consumer past the
/// channel bound is SHED inside `ShardPubSub::deliver` (its sender dropped, so its serve loop
/// disconnects), keeping shard memory bounded. The borrow of the subscription table is taken +
/// released entirely within this SYNCHRONOUS call (try_send is non-blocking), so nothing is
/// held across the caller's `.await` (the no-borrow-across-await contract).
///
/// A malformed `__ICPUBLISH` (missing channel/payload) delivers to nobody and returns 0; the
/// coordinator only ever issues it well-formed (the PUBLISH arity is validated client-side).
///
/// PATTERN delivery (PR 91b): in addition to the exact `channel` subscribers, this also delivers
/// to every LOCAL pattern subscriber whose pattern `glob_match`es `channel` (a `pmessage`), via
/// [`crate::pubsub::ShardPubSub::deliver_patterns`] with the binary-safe Redis matcher. The
/// returned count is exact-channel + pattern receivers (a connection subscribed to BOTH the
/// exact channel AND a matching pattern is counted TWICE -- it receives BOTH a `message` and a
/// `pmessage`, Redis semantics, NO dedup). Both fan-outs hold the table borrow only across the
/// synchronous `try_send`s (no `.await` between borrow and release).
#[must_use]
pub fn run_local_publish(request: &Request) -> Value {
    let (Some(channel), Some(payload)) = (request.args.get(1), request.args.get(2)) else {
        return Value::Integer(0);
    };
    let push = crate::pubsub::ServerPush::Message {
        channel: channel.clone(),
        payload: payload.clone(),
    };
    let pubsub = crate::serve::shard_pubsub();
    let mut count = pubsub.borrow_mut().deliver(channel.as_ref(), &push);
    // Pattern subscribers: each matching pattern delivers a `pmessage` and counts toward the
    // PUBLISH receiver total IN ADDITION to the exact-channel delivery above (no dedup).
    count += pubsub.borrow_mut().deliver_patterns(
        channel.as_ref(),
        payload,
        ironcache_server::glob::glob_match,
    );
    Value::Integer(count)
}

/// Deliver an `__ICSPUBLISH <channel> <payload>` to THIS shard's LOCAL SHARD-channel subscribers
/// and return the count (#410), the SPUBLISH analog of [`run_local_publish`]. UNLIKE the regular
/// publish there is NO pattern delivery (sharded Pub/Sub has no PSSUBSCRIBE) and the SHARD-channel
/// table (`shard_channels`) is consulted, NOT `channels` -- so an SPUBLISH never reaches a regular
/// SUBSCRIBE subscriber. The table borrow is held only across the synchronous `try_send`s (no
/// `.await` between borrow and release). A malformed verb delivers to nobody and returns 0.
#[must_use]
pub fn run_local_spublish(request: &Request) -> Value {
    let (Some(channel), Some(payload)) = (request.args.get(1), request.args.get(2)) else {
        return Value::Integer(0);
    };
    let push = crate::pubsub::ServerPush::SMessage {
        channel: channel.clone(),
        payload: payload.clone(),
    };
    let pubsub = crate::serve::shard_pubsub();
    let count = pubsub.borrow_mut().deliver_shard(channel.as_ref(), &push);
    Value::Integer(count)
}

// #576 FROZEN-SLOT soundness: the shard hands its FROZEN slot tables to a dedicated persist OS thread,
// so what it ships MUST be `Send`. An `Arc<HashTable<Entry>>` is `!Send` (`Entry` is a `!Send` tagged
// `NonNull<u8>`), so the store wraps each frozen slot in `ironcache_store::FrozenSlot`, whose
// `unsafe impl Send` is justified by the freeze invariant (a frozen slot's entries are de-facto
// immutable for the save -- every datapath write COWs first and the shared freq bump is gated off; see
// the `FrozenSlot` type doc). This static assertion pins that the shipped type IS `Send`, so a
// regression that broke the wrapper would fail HERE at compile time rather than as an `unsafe` footgun.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<Vec<ironcache_store::FrozenSlot>>();
};

/// Run ONE shard's `__ICSAVE <save_unix_secs> <shard_index> <dir>` against THIS shard's store
/// (#58 persistence, #576 per-slot Arc-COW off-thread persist): DUMP this shard's whole partition to
/// `<dir>/dump-shard-<shard_index>.icss` ATOMICALLY (tmp -> fsync -> rename) and return its manifest
/// entry encoded as `*3 [:shard :keys :crc]` (see [`crate::persist::encode_save_reply`]). On an I/O
/// error it returns a proto error the home core surfaces as a failed SAVE.
///
/// ## Per-slot Arc-COW freeze (#576): the shard does NO O(N) copy; the datapath stays uncontended
///
/// The seconds-long p99.9 stalls (#576) were NOT the encode+fsync (#586 already moved those off-thread)
/// -- they were the O(N) `snapshot_chunk` COPY of the whole keyspace into owned records, which HAD to
/// stay on the serving core (the store is `!Send`) and contended the datapath on memory bandwidth
/// (throttling only STRETCHED that contention). This fix removes the serving-side copy entirely:
///
/// - FREEZE: the shard calls [`ShardStore::begin_save`](ironcache_store::ShardStore::begin_save), which
///   takes an `Arc` clone of every non-empty slot table (O(slots) atomic refcount bumps, NOT an
///   O(N-keys) copy) and flips the store's `saving` flag. Returns a `Vec<FrozenSlot>`. A SHORT
///   `borrow_mut`, dropped immediately.
/// - HAND OFF: the frozen slots move to a DEDICATED persist thread (`std::thread`, `ic-persist-<n>`)
///   which does ALL the O(N) work OFF the serving core --
///   [`dump_frozen_slots`](ironcache_persist::dump_frozen_slots) iterates each frozen slot's entries,
///   reconstructs + encodes each (`to_kvobj` + `encode_kvobj` + CRC), then
///   [`write_shard_dump`](ironcache_persist::write_shard_dump)s the sealed file ATOMICALLY. It touches
///   ONLY the frozen `Arc`s + the filesystem, never a live shard cell.
/// - SERVE THROUGHOUT: while the persist thread runs, the shard task AWAITS the result on a
///   `tokio::sync::oneshot` (a cross-thread wake, NOT a blocking join), so the shard keeps serving.
///   Datapath reads share the frozen `Arc`s (a shared deref, no atomic); datapath writes COW a still
///   frozen slot on first touch (`Arc::make_mut`, a one-time ~0.7ms deep-clone per written slot) and
///   then mutate the fresh copy, so a write is NEVER visible in the concurrent dump and NEVER touches a
///   pointee the persist thread reads.
/// - END: once the result arrives (success OR error -- both mean the persist thread's closure has fully
///   exited, so all its reads are done and its `FrozenSlot`s are dropped), the shard clears the `saving`
///   flag via [`ShardStore::end_save`](ironcache_store::ShardStore::end_save). It is cleared ONLY here
///   (not on task cancellation): if this task is cancelled at shutdown the flag stays set, which is the
///   SAFE direction (the datapath keeps COWing + skips freq bumps, never racing the still-running
///   persist thread) on an already-exiting process.
///
/// So the ONLY shard-thread cost is O(slots) refcount bumps at freeze plus a one-time COW per slot that
/// is actually written DURING the save; the whole O(N) encode+fsync is off the serving core. That is
/// what reaches ms-class datapath latency during a save (the p99.9 lever #571/#578/#586 could not).
///
/// ## Consistency (#576): a per-shard POINT-IN-TIME as of the freeze (stronger than #571)
///
/// Because a write COWs a still-frozen slot before mutating it, the entries the persist thread reads are
/// IMMUTABLE for the save, so the dump is a per-shard POINT-IN-TIME view AS OF `begin_save`: a key
/// written mid-save keeps its PRE-freeze value in the dump (or is omitted if newly created), while the
/// live store keeps the new value. This is STRONGER than the pre-#576 chunked walk (which could capture
/// a mid-dump write). Cross-shard it stays FUZZY (each shard freezes at its own instant), the accepted
/// cache warm-start tradeoff (see [`ironcache_persist`]).
///
/// ## Crash-safety (unchanged, #530): the manifest is written LAST
///
/// The persist thread writes only this shard's per-shard file (atomic tmp -> fsync -> rename). The
/// manifest -- the COMMIT POINT -- is still written LAST by the home core via
/// [`ironcache_persist::write_manifest`] only AFTER every shard's reply (each returned only once its
/// persist thread FINISHED its file write). So a crash between the per-shard writes and the manifest
/// leaves the PRIOR committed snapshot intact, and a torn/un-committed `.icss` (a cancelled/panicked
/// save) is ignored by load. `now` is read from THIS shard's Env clock (ADR-0003, the lazy-expiry
/// basis); the save produces NO counter deltas.
///
/// ## Save-backpressure throttle (#578): now INERT for the tail (the copy it throttled is gone)
///
/// The `save-backpressure-percent` knob stays settable (CONFIG), but the freeze-based save does NO
/// serving-side copy loop for it to throttle, so it no longer paces the datapath -- consistent with the
/// #576 finding that throttling only STRETCHED (never bounded) the during-save tail. It is not read
/// here.
async fn save_shard_local(ctx: &ServerContext, request: &Request) -> Value {
    // Parse `__ICSAVE <save_unix_secs> <shard_index> <dir>`.
    let (Some(secs_arg), Some(shard_arg), Some(dir_arg)) = (
        request.args.get(1),
        request.args.get(2),
        request.args.get(3),
    ) else {
        return Value::error(ironcache_protocol::ErrorReply::err("malformed __ICSAVE"));
    };
    let parse_u64 = |b: &bytes::Bytes| {
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
    };
    let (Some(_save_secs), Some(shard_index)) = (parse_u64(secs_arg), parse_u64(shard_arg)) else {
        return Value::error(ironcache_protocol::ErrorReply::err("malformed __ICSAVE"));
    };
    #[allow(clippy::cast_possible_truncation)]
    let shard_index = shard_index as u32;
    let dir = std::path::PathBuf::from(String::from_utf8_lossy(dir_arg).into_owned());

    let env = shard_env();
    let store_rc = shard_store(
        ctx.databases,
        ctx.info.maxmemory_policy,
        crate::serve::scan_reserved_bits(ctx.shards),
    );
    // `now` from THIS shard's wall clock (ADR-0003), a short borrow dropped immediately.
    let now = UnixMillis(env.borrow().now_unix_millis());

    // FREEZE (#576): Arc-clone every non-empty slot table (O(slots), no O(N-keys) copy) and set the
    // store's `saving` flag, so the datapath COWs a frozen slot on its first write and skips the shared
    // freq bump for the save window. A SHORT `borrow_mut`, dropped at the end of this statement -- no
    // store borrow is held across the await below.
    let frozen: Vec<ironcache_store::FrozenSlot> = store_rc.borrow_mut().begin_save();

    // The oneshot carries the persist thread's file-write result back to this shard task via a
    // cross-thread wake (does NOT block the executor).
    let (done_tx, done_rx) =
        oneshot::channel::<std::io::Result<ironcache_persist::ShardManifestEntry>>();
    let dir_for_thread = dir.clone();
    // The `persist-cpu` knob (#589): which core(s) to pin THIS persist thread to. Cloned into the
    // closure so the pin is applied ON the persist thread (affinity is per-thread). Empty = the
    // default no-pin (float, byte-unchanged); a no-op on non-Linux (see `crate::affinity`).
    let persist_cpu_spec = ctx.boot.persist_cpu.clone();
    let spawned = std::thread::Builder::new()
        .name(format!("ic-persist-{shard_index}"))
        .spawn(move || {
            // #589: pin this persist thread to the configured dedicated persist core(s) BEFORE the
            // encode, so its off-core read+encode+fsync stops competing for a pinned datapath serving
            // core. A graceful no-op when the knob is unset (default) or on a non-Linux host; a bad
            // core just logs once and runs unpinned. Purely a scheduling action (ADR-0003 untouched).
            crate::affinity::apply_persist_pin(&persist_cpu_spec);
            // OFF the serving core: iterate the frozen slots, reconstruct + encode each entry (the
            // `to_kvobj` deep-clone + `encode_kvobj` codec + CRC), and write the sealed file ATOMICALLY.
            // Touches ONLY the frozen `Arc`s (de-facto immutable for the save -- the datapath COWs before
            // any write) + the filesystem, never a live shard cell (ADR-0002 datapath isolation).
            //
            // #585: run the encode+write under `catch_unwind` so a panic here (an encode bug, a
            // filesystem edge) is LOGGED with its cause instead of surfacing to the shard only as an
            // opaque `RecvError`. On a panic the save fails (turned into an `io::Error`); the DURABLE
            // prior snapshot is intact (the manifest is written last), so this is a failed save, never
            // data loss. `AssertUnwindSafe` is sound: the caught closure only READS the frozen `Arc`s
            // (immutable for the save) + the filesystem, so a mid-encode unwind leaves no shared state
            // observably broken (the frozen slots are dropped below regardless).
            let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let dump = ironcache_persist::dump_frozen_slots(&frozen, shard_index, now);
                ironcache_persist::write_shard_dump(&dump, shard_index, &dir_for_thread)
            })) {
                Ok(r) => r,
                Err(panic) => {
                    let cause = panic
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "non-string panic payload".to_string());
                    tracing::error!(
                        shard = shard_index,
                        cause = %cause,
                        "ic-persist thread panicked during save; save FAILED (durable prior snapshot intact)"
                    );
                    Err(std::io::Error::other(format!(
                        "persist thread panicked: {cause}"
                    )))
                }
            };
            // Release the frozen `Arc`s NOW (all reads + the file write are done): a slot that the
            // datapath COW'd away is freed here (no live reader remains); a still-shared slot just
            // decrements its atomic refcount. Done BEFORE the send so that by the time the shard clears
            // `saving` every live slot is back to strong_count 1.
            drop(frozen);
            // The receiver may be gone (this save task was cancelled at shutdown): the send error is
            // harmless -- the un-committed `.icss` is ignored by load (the manifest is written last).
            let _ = done_tx.send(result);
        });
    if let Err(e) = spawned {
        // The spawn consumed + dropped `frozen` (releasing the frozen `Arc`s), but the `saving` flag is
        // still set: clear it so the datapath resumes its fast path, then surface the failure.
        store_rc.borrow_mut().end_save();
        return Value::error(ironcache_protocol::ErrorReply::err(format!(
            "save failed: cannot spawn persist thread: {e}"
        )));
    }

    // AWAIT the persist thread's atomic file-write result WITHOUT blocking the executor. The shard keeps
    // serving throughout: reads share the frozen `Arc`s, writes COW. A `RecvError` (the result sender
    // dropped) means the persist thread panicked -- surfaced as a failed save, not a hang.
    let result = done_rx.await;
    // END (#576): clear the `saving` flag. All three arms below mean the persist thread's closure has
    // FULLY exited (it dropped `frozen` and its `done_tx` before/at return), so its reads are complete
    // and no freq bump resuming here can race them. Cleared only on this normal-completion path (a
    // cancelled task never reaches here -- the flag then safely stays set on an exiting process).
    store_rc.borrow_mut().end_save();
    match result {
        Ok(Ok(entry)) => crate::persist::encode_save_reply(&entry),
        Ok(Err(e)) => Value::error(ironcache_protocol::ErrorReply::err(format!(
            "save failed: {e}"
        ))),
        Err(_) => Value::error(ironcache_protocol::ErrorReply::err(
            "save failed: persist thread ended without a result",
        )),
    }
}

/// Run the HOME shard's `__ICSAVE` partial LOCALLY (#58 persistence, #571 yielding, #576 off-thread
/// persist), returning the home shard's [`ShardReply`]. This is the async local step [`fan_out_save`]
/// runs for the home shard: the home core does NOT round-trip its OWN save through its channel; it
/// dumps inline via the SAME [`save_shard_local`] every remote shard runs (so the home shard's file is
/// byte-identical to a remote shard's), COPYING chunk-by-chunk to its dedicated persist thread and
/// awaiting between chunks so the home shard's OWN queued writes are serviced during its dump too. A
/// save produces no counter deltas, so the reply carries default deltas. The per-chunk store borrow is
/// released before each yield (the no-borrow-across-await contract).
pub async fn run_local_save(ctx: &ServerContext, request: &Request) -> ShardReply {
    ShardReply {
        value: save_shard_local(ctx, request).await,
        deltas: CounterDeltas::default(),
    }
}

/// LOAD this shard's slice of the committed snapshot into its store at boot (#58 load-on-boot),
/// RE-SHARDING across any shard-count change (the C1 fix). THIS shard reads EVERY manifest shard
/// file and keeps only the keys it OWNS under the CURRENT shard count (`ctx.shards`), using the
/// SAME `ironcache_server::owner_shard` hash the router routes a single-key command with. So a
/// snapshot saved with N shards loads CORRECTLY into a node with M != N shards: with fewer shards
/// no key is lost (every file is read), with more shards no GET misroutes (each key lands on its
/// real owner). A missing manifest / a missing or torn shard file loads NOTHING for that file (the
/// shard's keys from it are absent, never corrupt-loaded). Synchronous: the store borrow is taken +
/// released here (no `.await` held across it), and `now` is read from THIS shard's Env clock (the
/// determinism seam, ADR-0003: an already-expired key is dropped on load).
fn load_shard_on_boot(
    ctx: &ServerContext,
    persist: &Arc<crate::persist::PersistState>,
    shard_index: usize,
) {
    let env = shard_env();
    let store_rc = shard_store(
        ctx.databases,
        ctx.info.maxmemory_policy,
        crate::serve::scan_reserved_bits(ctx.shards),
    );
    let now = UnixMillis(env.borrow().now_unix_millis());
    // The CURRENT shard count: the router computes owner_shard(key, shard_count), so re-shard with
    // the SAME count + hash the live serve loop routes with.
    let shard_count = ctx.shards.max(1);
    // Load from the dir THIS boot resolved (#390): the tmpfs upgrade-handoff staging dir when a
    // valid, fresher-or-equal handoff is present, else the durable `data_dir`. Resolved ONCE at
    // `PersistState::from_config`, so every shard loads from the SAME source (no mid-boot flap).
    let load_dir = &persist.boot_load_dir;
    let (loaded, evicted, over_budget) = {
        let mut store = store_rc.borrow_mut();
        let loaded = ironcache_persist::load_shard_resharded(
            &mut store,
            load_dir,
            shard_index,
            shard_count,
            now,
            ironcache_server::owner_shard,
        );
        // MAXMEMORY ENFORCEMENT ON LOAD (durability footgun fix #4): a snapshot LARGER than
        // `maxmemory` would otherwise load fully and boot the node ALREADY over the ceiling (an OOM
        // risk the live admission path never gets a chance to prevent, since admission only runs on
        // WRITES). After the load, if the ceiling is enabled and this shard is over its even-split
        // per-shard budget, run the SAME `evict_to_fit` path the live admission gate uses so the
        // loaded keyspace respects `maxmemory` from the first served command. With the ceiling OFF
        // (`per_shard_budget() == 0`, the default) this is a no-op, so the default boot is
        // byte-unchanged. The store borrow is held only across this synchronous evict (no `.await`).
        let budget = ctx.per_shard_budget();
        let (evicted, over_budget) = if budget > 0 && store.used_memory() > budget {
            let evicted = store.evict_to_fit(budget, now);
            // If eviction could not get under budget (e.g. a policy that protects every key), the
            // node is still over the ceiling -> surface it loudly rather than silently OOM-risking.
            (evicted, store.used_memory() > budget)
        } else {
            (0, false)
        };
        (loaded, evicted, over_budget)
    };
    if loaded > 0 {
        tracing::info!(
            shard = shard_index,
            loaded,
            evicted,
            dir = %load_dir.display(),
            "ironcache: shard loaded keys from snapshot"
        );
    }
    if evicted > 0 {
        tracing::warn!(
            shard = shard_index,
            evicted,
            "ironcache: load-on-boot snapshot exceeded maxmemory; evicted to fit the ceiling"
        );
    }
    if over_budget {
        // Eviction ran but could not bring this shard under its budget (the loaded snapshot is
        // larger than maxmemory and the policy cannot evict enough): LOUD warning so an operator
        // sees the node booted over the ceiling rather than discovering an OOM later.
        tracing::warn!(
            shard = shard_index,
            budget_bytes = ctx.per_shard_budget(),
            used_bytes = store_rc.borrow().used_memory(),
            "ironcache: load-on-boot left this shard OVER maxmemory (snapshot larger than the \
             ceiling and the eviction policy could not free enough); the node is over budget"
        );
    }

    // #391 PR-6 DURABLE-RECOVERY MERGE (W2): when this boot loaded from a PROMOTED streamed-cutover
    // dir, the bulk above is state@F; MERGE the bounded promoted delta log `(F, E]` so the recovered
    // store is state@E -- exactly what a NEW that crashed AFTER `Commit` must come back with (the
    // cutover promotes bulk + delta together, and today's loader read only the bulk). A normal
    // (non-cutover) `data_dir` has NO delta file, so this is a cheap existence probe that no-ops and
    // the default boot is byte-unchanged. Single-node upgrade keeps the shard layout, so shard N's
    // delta aligns with shard N's bulk. A torn delta is logged LOUDLY (fail-closed: the tail is not
    // half-applied) but does not block boot (matching the bulk loader's lenient torn-file posture).
    let shard_u32 = u32::try_from(shard_index).unwrap_or(u32::MAX);
    if crate::upgrade::commit::has_promoted_delta(load_dir, shard_u32) {
        let mut store = store_rc.borrow_mut();
        match crate::upgrade::commit::replay_promoted_delta(&mut store, load_dir, shard_u32, now) {
            Ok(applied) if applied > 0 => tracing::info!(
                shard = shard_index,
                applied,
                "ironcache: merged promoted streamed-cutover delta log (recovered state@E)"
            ),
            Ok(_) => {}
            Err(e) => tracing::error!(
                shard = shard_index,
                error = %e,
                "ironcache: promoted streamed-cutover delta log is torn; recovered the bulk only \
                 (state@F). This is a NEW-crash-after-commit recovery with a corrupt tail."
            ),
        }
    }
}

/// #391 PR-2: the RECEIVER-role boot decision for [`run_drain_loop`], resolving to whether the shard's
/// store was loaded over the streamed-handoff socket (vs. the default disk load).
///
/// Returns:
/// - `Ok(false)` when this process is NOT the streamed-handoff receiver (no `handoff_socket`, or the
///   default sender role) -- the caller then takes the unchanged disk load. This is the common path
///   and the ONLY one a default deployment (or any non-unix build) ever takes.
/// - `Ok(true)` when this shard's store was pulled over the socket and INSTALLED (adopted) into its
///   thread-local store on the fully verified, cutover-acked path.
/// - `Err(())` when the receive FAILED at any point: NOTHING was installed (the live store handle is
///   untouched) and the caller must abort this shard's boot WITHOUT serving or signaling readiness.
///
/// SCOPE (PR-2 is the LOAD + install only): a received shard MUST NOT start serving client traffic
/// yet. The global serving gate + the serve-flip are PR-5, and the orchestrator that spawns the
/// sibling (and keeps its client acceptor closed until the flip) is PR-6; no live deployment enters
/// receive mode before PR-6 wires that spawn, so this path is dormant until then.
///
/// The streamed handoff is AF_UNIX-only, so the receive machinery is `#[cfg(unix)]`; on other
/// platforms the sibling stub always returns `Ok(false)` (disk load).
#[cfg(unix)]
async fn resolve_receive_role(ctx: &ServerContext, shard_index: usize) -> Result<bool, ()> {
    let Some(plan) = crate::upgrade::drive::HandoffPlan::receiver_from_config(&ctx.boot) else {
        return Ok(false); // not the receiver -> the caller takes the unchanged disk load.
    };
    match receive_shard_from_handoff(ctx, &plan, shard_index).await {
        Ok(final_offset) => {
            tracing::info!(
                shard = shard_index,
                final_offset = final_offset.0,
                "ironcache: shard loaded via streamed handoff (receiver role); store installed"
            );
            Ok(true)
        }
        Err(e) => {
            tracing::error!(
                shard = shard_index,
                error = %e,
                "ironcache: streamed-handoff receive-load FAILED; installing nothing (data-safe \
                 abort, shard left unready). TODO(#391 PR-6): abort the sibling unserving."
            );
            Err(())
        }
    }
}

/// The non-unix stub of [`resolve_receive_role`]: the streamed handoff rides an AF_UNIX socket, so on
/// a non-unix build there is never a receiver role -- this always resolves to `Ok(false)` (disk load),
/// keeping the boot path byte-unchanged there.
#[cfg(not(unix))]
async fn resolve_receive_role(_ctx: &ServerContext, _shard_index: usize) -> Result<bool, ()> {
    Ok(false)
}

/// #391 PR-2: connect to the handoff socket for THIS shard and pull its store into the thread-local
/// [`ShardStoreImpl`], adopting ONLY on the fully verified, cutover-acked path.
///
/// The receiver dials the sender's node-local socket (retrying while it is not yet bound, bounded by
/// the plan timeout), then drives the transport's [`recv_shard_timed`](crate::upgrade::drive::recv_shard_timed)
/// bulk + delta + cutover into a FRESH store (offset-gated apply, `first == F+1` contiguity,
/// `applied == final_offset`, CRC + db-count verified inside the transport). Only that Ok result is
/// installed; on ANY error nothing is installed and the live store is untouched (see
/// [`receive_shard_into`]). `now` is read from THIS shard's Env clock (the determinism seam,
/// ADR-0003), and `make_store` builds the SAME concrete [`ShardStoreImpl`] the live serve path uses,
/// so the installed store is type-identical.
///
/// # Errors
/// Any [`HandoffError`] from the connect or the transport (socket error, timeout, verify/contiguity
/// failure, or a peer abort).
#[cfg(unix)]
async fn receive_shard_from_handoff(
    ctx: &ServerContext,
    plan: &crate::upgrade::drive::HandoffPlan,
    shard_index: usize,
) -> Result<ReplOffset, HandoffError> {
    let reserved_bits = crate::serve::scan_reserved_bits(ctx.shards);
    let policy = ctx.info.maxmemory_policy;
    let dbs = ctx.databases;
    let store_rc = shard_store(dbs, policy, reserved_bits);
    let now = UnixMillis(shard_env().borrow().now_unix_millis());
    // CONNECT to the sender's PER-SHARD handoff socket `<base>.<shard_index>` (#638 PR-1): shard i
    // binds/connects its own suffixed path so its stream is reactor-bound to shard i and never
    // crosses a thread (deterministic i<->i pairing). The sender's HELLO still carries the shard id
    // the transport verifies. The base `plan.socket` stays the well-known rendezvous both ends agree
    // on; only the per-shard suffix is derived (inside `connect_handoff_for_shard`).
    tracing::debug!(
        shard = shard_index,
        base_socket = %plan.socket.display(),
        "ironcache: receiver role connecting to per-shard handoff socket (<base>.<shard>) for shard boot-load"
    );
    let mut stream =
        crate::upgrade::drive::connect_handoff_for_shard(&plan.socket, shard_index, plan.timeout)
            .await?;
    receive_shard_into(
        &mut stream,
        &store_rc,
        move || crate::serve::fresh_shard_store(dbs, policy, reserved_bits),
        dbs,
        now,
        plan.timeout,
    )
    .await
}

/// #391 PR-2 (the testable seam): pull a shard's store from `stream` into a LOCAL fresh store and, on
/// the fully verified cutover-acked path ONLY, INSTALL it as the thread-local store `store_rc`.
///
/// This is the one place the receiver adopts: [`recv_shard_timed`](crate::upgrade::drive::recv_shard_timed)
/// builds the fresh store, enforcing db-count, `first == F+1` contiguity, `applied == final_offset`,
/// and the CRC/version envelope, and DROPS its partial store on any error (adopt nothing). Only its
/// `Ok(LoadedShard)` is swapped into `store_rc` in ONE statement -- mirroring the HA-7d full-sync
/// atomic store swap ([`crate::replica_attach`]) -- so a mid-stream error leaves `store_rc` exactly as
/// it was (never half-populated). Generic over the store hooks + stream so it is driven directly over
/// a `UnixStream::pair` in the unit tests, and concretely over the real socket by
/// [`receive_shard_from_handoff`].
///
/// # Errors
/// Any [`HandoffError`] from the transport; on error nothing is installed.
#[cfg(unix)]
async fn receive_shard_into<E, A, S, M>(
    stream: &mut S,
    store_rc: &Rc<RefCell<ShardStore<E, A>>>,
    make_store: M,
    expected_databases: u32,
    now: UnixMillis,
    timeout: Duration,
) -> Result<ReplOffset, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
    M: FnMut() -> ShardStore<E, A>,
{
    // Build into a LOCAL LoadedShard (never adopted until fully verified + cutover-acked).
    let loaded = crate::upgrade::drive::recv_shard_timed(
        stream,
        make_store,
        expected_databases,
        now,
        timeout,
    )
    .await?;
    // ADOPT: install the verified store as THIS shard's thread-local store in one statement. On any
    // error above we returned early and never reached here, so the live handle stays untouched.
    *store_rc.borrow_mut() = loaded.store;
    Ok(loaded.final_offset)
}

/// The coarse poll cadence the periodic-save loop ticks on while the save policy is OFF (or between
/// the per-interval deadlines), so a runtime `CONFIG SET save` is noticed within a bounded delay
/// even when the node booted with the policy disabled. A second is a fine granularity for a cadence
/// expressed in seconds and is far off any hot path (this is a cold home-core timer). Driven through
/// the Runtime timer seam (ADR-0003), never wall-clock.
const PERIODIC_SAVE_POLL: std::time::Duration = std::time::Duration::from_secs(1);

/// Spawn the PERIODIC SAVE timer (#58 save policy) on SHARD 0's executor (one timer per node). It
/// reads the LIVE save policy from the runtime overlay (`ctx.runtime.save_policy()`) so a runtime
/// `CONFIG SET save "<seconds> <changes>"` takes effect even on a node booted with the policy OFF
/// (the durability footgun fix). It ticks on a coarse poll and, once the configured `interval_secs`
/// of wall-clock has elapsed AND at least `min_changes` writes happened since the last save, it
/// triggers a full cross-shard save (the SAME [`crate::persist::do_save_all`] SAVE/BGSAVE use).
/// While the policy is OFF (interval 0) it simply idles -- no save. This is spawned whenever
/// persistence is enabled; a default persistence-off node has no [`crate::persist::PersistState`],
/// so it gets no timer at all (byte-unchanged).
///
/// ## Borrow / determinism discipline
///
/// The loop awaits through the [`Runtime`] timer SEAM (NOT `tokio::time`, ADR-0003) and holds NO
/// RefCell borrow across the awaits (the save fan-out's per-shard `save_shard_local` is synchronous
/// and runs on each shard's own executor, so this home-core task only awaits channel replies). The
/// elapsed-interval accounting + the save timestamp come from the env Clock seam (no `Instant`).
fn spawn_periodic_save(
    ctx: ServerContext,
    inbox: Inbox,
    persist: Arc<crate::persist::PersistState>,
) {
    use ironcache_runtime::Runtime;
    let rt = ironcache_runtime::TokioRuntime::new();
    let home = ShardId {
        index: 0,
        total: inbox.len(),
    };
    rt.spawn_on_shard(async move {
        // The unix-seconds at which the current interval started accumulating; reset whenever the
        // policy changes or a save fires. Read from shard 0's Env clock (ADR-0003), never Instant.
        let mut window_start = shard_env().borrow().now_unix_millis() / 1_000;
        loop {
            rt.timer(PERIODIC_SAVE_POLL).await;
            // Read the LIVE policy each tick so a `CONFIG SET save` is honored without a restart.
            let (interval_secs, min_changes) = ctx.runtime.save_policy();
            let now_secs = shard_env().borrow().now_unix_millis() / 1_000;
            if interval_secs == 0 {
                // The periodic save is OFF: keep the window anchored at now so turning the policy
                // ON later starts a fresh interval rather than firing immediately on stale elapsed.
                window_start = now_secs;
                continue;
            }
            // Not enough wall-clock has elapsed for this interval yet.
            if now_secs.saturating_sub(window_start) < interval_secs {
                continue;
            }
            // The interval elapsed: open a fresh window regardless of whether we save below, so a
            // skipped (too-few-changes) tick does not re-fire every poll.
            window_start = now_secs;
            // Skip if too few writes happened since the last save (the `changes` half of the Redis
            // `save <seconds> <changes>` policy). `min_changes == 0` always fires.
            let dirty = persist.dirty();
            if dirty < min_changes {
                continue;
            }
            // Serialize against a concurrent SAVE/BGSAVE; if one is already running, skip this tick.
            // The RAII guard releases the latch on completion, panic, OR cancellation (H3).
            let Some(_guard) = persist.try_begin_save() else {
                continue;
            };
            // The save timestamp from shard 0's Env clock (the determinism seam, ADR-0003).
            let _ = crate::persist::do_save_all(&persist, &inbox, &ctx, home, 0, now_secs).await;
        }
    });
}

/// SAVE-ON-EXIT for the SIGNAL-driven graceful shutdown (#139, SHUTDOWN.md): perform a final
/// synchronous cross-shard save IFF a save POLICY is configured, reusing the SAME atomic save path
/// SAVE/BGSAVE/the periodic timer use ([`crate::persist::do_save_all`] -- forkless per-shard dump +
/// the manifest committed LAST via a tmp->rename, so a killed task never leaves a torn file). This
/// is the save decision a bare `SHUTDOWN` (and thus a SIGTERM/SIGINT stop) resolves: save iff a save
/// point is configured [redis-shutdown-save-nosave-default]. With persistence OFF (`persist` is
/// `None`) or with persistence on but NO periodic policy ([`has_save_policy`](crate::persist::PersistState::has_save_policy)
/// false, i.e. explicit-SAVE-only), this is a NO-OP -- so a default deployment exits without writing
/// anything.
///
/// MUST run on SHARD 0's executor (it owns `home == shard 0` + the inbox the fan-out needs); the
/// drain loop is that executor. Returns `true` iff a save was attempted-and-committed, `false` if no
/// save was warranted, the latch wait TIMED OUT (a wedged in-flight save, the LOW case -- proceed
/// best-effort), or the save failed. If a save was ALREADY running this BOUNDED-WAITS for it to
/// commit + free the latch (H1: bytes are NOT durable until `write_manifest`, so exiting OVER an
/// in-flight save would lose every write since the prior commit), THEN runs a FRESH save before
/// returning. A save FAILURE is logged (the signal path has no client to reply to) and returns
/// `false`; the process still exits (the prior committed manifest stays valid).
///
/// ## Borrow / determinism discipline
///
/// Holds NO RefCell borrow across the `.await` (the per-shard `save_shard_local` is synchronous on
/// each shard's own executor); the save timestamp comes from shard 0's Env Clock seam (ADR-0003).
pub async fn save_on_exit_if_configured(
    persist: Option<&Arc<crate::persist::PersistState>>,
    ctx: &ServerContext,
    inbox: &Inbox,
) -> bool {
    // Save iff persistence is on AND a save policy (a periodic cadence) is configured -- the bare
    // SHUTDOWN / signal-stop decision [redis-shutdown-save-nosave-default]. The policy is the LIVE
    // runtime one (`CONFIG SET save` may have changed it since boot), read from the runtime overlay.
    let Some(persist) = persist.filter(|_| ctx.runtime.has_save_policy()) else {
        return false;
    };
    // H1 (data loss): the OLD code did `try_begin_save() else { return false }` and then the caller
    // exited(0). But a concurrent BGSAVE / periodic save may be mid-`do_save_all` with some `.icss`
    // files written and the manifest (the atomic COMMIT point) NOT yet run, so exiting over it KILLS
    // that save before it commits -- the committed manifest still points at the PRIOR snapshot and
    // every write since is LOST despite this save-on-exit. The fix: BOUNDED-WAIT for the busy latch
    // to free (the in-flight save commits + drops its guard; on a single-threaded executor the timer
    // await yields to it), THEN run a FRESH save (guarantees CURRENT data), THEN return so the caller
    // exits. No borrow is held across the wait, so it cannot deadlock the save it waits on.
    let Some(_guard) =
        crate::persist::wait_to_begin_save(persist, crate::persist::SHUTDOWN_SAVE_WAIT).await
    else {
        // The wait TIMED OUT: a genuinely wedged save never freed the latch (the LOW case). Do NOT
        // hang the exit forever -- return false best-effort (the caller exits; the in-flight save MAY
        // still commit its prior-or-partial state, and the prior committed manifest stays valid).
        tracing::warn!(
            "ironcache: save-on-exit: a prior save did not finish within SHUTDOWN_SAVE_WAIT; \
             exiting best-effort (the in-flight save may still commit)"
        );
        return false;
    };
    let home = ShardId {
        index: 0,
        total: inbox.len(),
    };
    // The save timestamp from shard 0's Env clock (the determinism seam, ADR-0003).
    let now_secs = shard_env().borrow().now_unix_millis() / 1_000;
    // BOUNDED so a wedged sibling drain loop (alive but stuck) cannot hang the exit save fan-out (L1)
    // -- the signal path escapes via the second-signal force-exit watcher, but bounding here makes a
    // single SIGTERM self-terminating too.
    match crate::persist::do_save_all_bounded(
        persist,
        inbox,
        ctx,
        home,
        0,
        now_secs,
        crate::persist::SHUTDOWN_SAVE_WAIT,
    )
    .await
    {
        Ok(()) => true,
        Err(msg) => {
            tracing::error!(
                error = %msg,
                "ironcache: save-on-exit failed (the prior committed snapshot stays valid)"
            );
            false
        }
    }
}

/// Fan a `PUBLISH <channel> <payload>` out to EVERY shard's LOCAL subscriber table and SUM the
/// per-shard receiver counts into the PUBLISH integer reply (SERVER_PUSH.md #20 / COORDINATOR.md
/// #107, PR 91a). Modeled on [`fan_out_all`]: classic Pub/Sub channels are NOT slotted, so a
/// PUBLISH must reach subscribers on ANY core; it broadcasts the SAME `__ICPUBLISH channel
/// payload` request to every shard.
///
/// The HOME shard's delivery runs LOCALLY + SYNCHRONOUSLY via [`run_local_publish`] (no
/// self-channel hop, exactly like `fan_out_all`'s `local` closure); every OTHER shard gets a
/// [`ShardWork`] carrying the `__ICPUBLISH` request (its [`run_remote`] pub/sub branch delivers
/// to that shard's local subscribers and returns its local count). The home core then SUMS all
/// the per-shard integer counts. A shard whose drain loop is gone (send error / cancelled
/// oneshot, only at shutdown / a shard panic) contributes 0 (it cannot have delivered), so a
/// degraded shard never hangs the PUBLISH.
///
/// `db` is carried for envelope symmetry with the other fan-outs (classic Pub/Sub channels are
/// a single cross-DB namespace this pass, so delivery itself ignores it). The `local` closure
/// (`run_local_publish`) returns before any `.await`, so NO RefCell borrow of the home shard's
/// subscription table is held across the awaits (the no-borrow-across-await contract).
pub async fn fan_out_publish(
    inbox: &Inbox,
    channel: &[u8],
    payload: &[u8],
    db: u32,
    home: usize,
) -> i64 {
    let n_shards = inbox.len();
    // The broadcast request the coordinator issues to every shard: the internal verb the
    // run_remote pub/sub branch + the home `run_local_publish` both decode.
    let request = Request {
        args: vec![
            bytes::Bytes::from_static(ironcache_server::ICPUBLISH),
            bytes::Bytes::copy_from_slice(channel),
            bytes::Bytes::copy_from_slice(payload),
        ],
    };

    // Enqueue to every NON-home shard first (each with its own oneshot), so the shards deliver
    // concurrently while the home core then delivers its OWN subset locally and gathers.
    let mut pending: Vec<oneshot::Receiver<ShardReply>> = Vec::with_capacity(n_shards);
    let mut total: i64 = 0;
    for target in 0..n_shards {
        if target == home {
            continue;
        }
        let (tx, rx) = oneshot::channel::<ShardReply>();
        let work = ShardWork {
            request: request.clone(),
            db,
            reply: tx,
        };
        // Await-on-full back-pressure on the INBOX (the cross-shard queue, NOT the push
        // channel). A send error means that shard's drain loop is gone (shutdown): it
        // contributes 0 (it delivered to nobody), never a hang.
        if inbox[target].send(work).await.is_ok() {
            pending.push(rx);
        }
    }

    // The HOME shard's delivery: LOCAL + SYNCHRONOUS (no self-channel hop), exactly like the
    // single-key local fast path. The closure returns before the awaits below.
    total += publish_count(&run_local_publish(&request));

    // Gather the remote per-shard counts. A cancelled oneshot (a shard's drain loop went away
    // after we enqueued) contributes 0, never a hang/panic.
    for rx in pending {
        if let Ok(reply) = rx.await {
            total += publish_count(&reply.value);
        }
    }
    total
}

/// FIRE-AND-FORGET fan-out for KEYSPACE NOTIFICATIONS (#543). The notification path IGNORES the
/// delivery count (a notification's value is the delivery, not a reply), so unlike [`fan_out_publish`]
/// this ENQUEUES the `__ICPUBLISH` to every non-home shard and RETURNS WITHOUT awaiting the per-shard
/// reply oneshots.
///
/// WHY (the deadlock this removes): notification fan-out is driven from the coordinator DRAIN loop
/// (a cross-shard keyed write that recorded events) AND the home serve loop. When it AWAITED the
/// replies, two shards' DRAIN loops could each block inside their own fan-out awaiting the other's
/// `__ICPUBLISH` reply -- and while a drain loop is parked in its fan-out it is NOT back at its inbox
/// `recv`, so it cannot service the OTHER shard's `__ICPUBLISH`. That is a cross-shard request/reply
/// CYCLE: both drain loops wait forever and the mutator's reply (assembled after the inline publish)
/// never flushes. Enqueue-and-return breaks the cycle: no drain loop ever blocks on a sibling's
/// reply, so every inbox keeps draining.
///
/// ORDERING is preserved where it is observable: the `__ICPUBLISH`s a single source shard enqueues
/// go into each target shard's inbox in event order, and the target drain loop delivers its inbox
/// FIFO, so a subscriber sees a given source's events in order. The HOME shard's own subset is still
/// delivered LOCALLY + SYNCHRONOUSLY (no self-channel hop).
///
/// BEST-EFFORT enqueue (`try_send`, NOT `send().await`): a keyspace/pub-sub notification is delivered
/// on a best-effort basis (Redis keyspace events are explicitly lossy under subscriber pressure), so
/// when a target inbox is FULL we DROP this event rather than block. Blocking would re-introduce the
/// very deadlock this function removes from the reply path: if two drain loops each `send().await`
/// into the other's full inbox, neither is back at its own `recv` to drain it, so neither send ever
/// gets capacity -- a send-side cross-shard cycle. `try_send` cannot block, so a drain loop always
/// returns to servicing its inbox; back-pressure is bounded by the inbox depth (a flood is dropped,
/// never queued unboundedly). A closed inbox (a shard tearing down) is likewise a silent miss, as the
/// awaited path already tolerated. Because it never awaits, this is a plain synchronous function.
pub fn fan_out_publish_notify(inbox: &Inbox, channel: &[u8], payload: &[u8], db: u32, home: usize) {
    let n_shards = inbox.len();
    let request = Request {
        args: vec![
            bytes::Bytes::from_static(ironcache_server::ICPUBLISH),
            bytes::Bytes::copy_from_slice(channel),
            bytes::Bytes::copy_from_slice(payload),
        ],
    };
    // Enqueue to every NON-home shard, non-blocking. The reply oneshot is created (ShardWork requires
    // one) but its receiver is DROPPED immediately: the count is ignored, so the target's `reply.send`
    // fails harmlessly into a dropped receiver (the drain loop already does `let _ = reply.send(..)`).
    // A `try_send` Err (Full = best-effort drop, or Closed = shard tearing down) simply misses this
    // delivery.
    for target in 0..n_shards {
        if target == home {
            continue;
        }
        let (tx, _rx) = oneshot::channel::<ShardReply>();
        let work = ShardWork {
            request: request.clone(),
            db,
            reply: tx,
        };
        let _ = inbox[target].try_send(work);
    }
    // The HOME shard's delivery: LOCAL + SYNCHRONOUS (no self-channel hop), exactly like
    // `fan_out_publish`.
    let _ = run_local_publish(&request);
}

/// Fan a SHARDED publish (`SPUBLISH channel payload`) out to every shard's LOCAL shard-channel
/// table and SUM the per-shard receiver counts (#410), the SPUBLISH analog of [`fan_out_publish`].
/// Broadcasts the internal `__ICSPUBLISH <channel> <payload>` (whose [`run_remote`] branch calls
/// [`run_local_spublish`], delivering to `shard_channels` with NO pattern delivery); the home
/// shard delivers its own subset LOCALLY + synchronously. Because IronCache Pub/Sub is node-local
/// (no cross-node bus), an SPUBLISH is already confined to this node, which is the sharded-Pub/Sub
/// "no cluster-bus amplification" property at the node boundary.
pub async fn fan_out_spublish(
    inbox: &Inbox,
    channel: &[u8],
    payload: &[u8],
    db: u32,
    home: usize,
) -> i64 {
    let n_shards = inbox.len();
    let request = Request {
        args: vec![
            bytes::Bytes::from_static(ironcache_server::ICSPUBLISH),
            bytes::Bytes::copy_from_slice(channel),
            bytes::Bytes::copy_from_slice(payload),
        ],
    };
    let mut pending: Vec<oneshot::Receiver<ShardReply>> = Vec::with_capacity(n_shards);
    let mut total: i64 = 0;
    for target in 0..n_shards {
        if target == home {
            continue;
        }
        let (tx, rx) = oneshot::channel::<ShardReply>();
        let work = ShardWork {
            request: request.clone(),
            db,
            reply: tx,
        };
        if inbox[target].send(work).await.is_ok() {
            pending.push(rx);
        }
    }
    total += publish_count(&run_local_spublish(&request));
    for rx in pending {
        if let Ok(reply) = rx.await {
            total += publish_count(&reply.value);
        }
    }
    total
}

/// DRAIN this (owner) shard's pending keyspace events (PROD-8) and PUBLISH each through the
/// existing Pub/Sub fan-out, from the SHARD's drain loop after a CROSS-SHARD keyed write recorded
/// them (the home-path analog lives in `crate::serve::publish_pending_keyspace_events`). `home` is
/// THIS shard's index, so `fan_out_publish` runs this shard's delivery LOCALLY (no self-channel
/// hop) and fans out to the others -- no re-entrant send to our own inbox.
///
/// FAST PATH: the drain returns an empty Vec when nothing was recorded (a read, a cross-shard
/// command that mutated nothing, or notifications disabled), so the common drain-loop turn pays a
/// single thread-local `is_empty` check. Only an actually-recorded event builds a channel + fans
/// out. The `__ICPUBLISH` delivery a fan-out enqueues to THIS shard later is handled BEFORE any
/// store borrow + does NOT go through dispatch, so it records nothing -- no notification loop.
fn publish_pending_keyspace_events(inbox: &Inbox, home: usize) {
    let events = ironcache_config::notify::drain();
    if events.is_empty() {
        return;
    }
    for ev in events {
        // FIRE-AND-FORGET (#543): a synchronous, reply-awaiting fan-out FROM the drain loop deadlocks
        // (two drain loops each block awaiting the other's `__ICPUBLISH` reply). The delivery count is
        // ignored for notifications, so `fan_out_publish_notify` enqueues + returns, keeping per
        // source->target FIFO order while never blocking this drain loop's inbox recv.
        if ev.keyspace {
            let channel = ev.keyspace_channel();
            fan_out_publish_notify(inbox, &channel, ev.event.as_bytes(), ev.db, home);
        }
        if ev.keyevent {
            let channel = ev.keyevent_channel();
            fan_out_publish_notify(inbox, &channel, &ev.key, ev.db, home);
        }
    }
}

/// Extract the integer receiver count a shard's `__ICPUBLISH` delivery returned. The pub/sub
/// branch always replies a [`Value::Integer`]; anything else (the shard-unavailable error, only
/// at shutdown) counts as 0 (that shard delivered to nobody).
fn publish_count(value: &Value) -> i64 {
    match value {
        Value::Integer(n) => *n,
        _ => 0,
    }
}

/// Run an `__ICPUBSUB <subcommand> [args]` PUBSUB-introspection PARTIAL against THIS shard's
/// LOCAL subscription table and return the shard's contribution as a [`Value`] (SERVER_PUSH.md
/// #20, PR 91b). The home core MERGES the per-shard partials (see [`fan_out_pubsub`]); the wire
/// shape of each partial is chosen so the merge is a simple union / sum / union-count:
///
/// - `CHANNELS [pat]` -> a [`Value::Array`] of this shard's channel names that have >= 1 LOCAL
///   subscriber (glob-filtered by `pat` if present). Home UNIONS + dedups.
/// - `NUMSUB [ch ...]` -> a FLAT [`Value::Array`] `[ch1, n1, ch2, n2, ...]` of this shard's LOCAL
///   per-channel subscriber counts, in the REQUESTED order (a channel with no local sub -> 0).
///   Home SUMS the counts per channel position.
/// - `NUMPAT` -> a [`Value::Array`] of this shard's LOCAL pattern names (with >= 1 subscriber).
///   Home UNIONS them and COUNTS the DISTINCT total (the same pattern on two shards is ONE).
///
/// It borrows the per-shard subscription table briefly (read-only) and returns before any
/// `.await` (the drain loop's no-borrow-across-await contract). A malformed `__ICPUBSUB` (no
/// subcommand) contributes an empty array (the home merge surfaces the client-side error; the
/// coordinator only issues it well-formed, validated client-side).
#[must_use]
pub fn run_local_pubsub(request: &Request) -> Value {
    let Some(sub) = request.args.get(1) else {
        return Value::Array(Some(Vec::new()));
    };
    let pubsub = crate::serve::shard_pubsub();
    let table = pubsub.borrow();
    match crate::serve::ascii_upper(sub.as_ref()).as_slice() {
        b"CHANNELS" => {
            // Optional glob filter (args[2]); None -> every locally-subscribed channel.
            let pat = request.args.get(2).map(bytes::Bytes::as_ref);
            let names = table.local_channels(pat, ironcache_server::glob::glob_match);
            Value::Array(Some(names.into_iter().map(Value::bulk).collect()))
        }
        b"NUMSUB" => {
            // One [name, local_count] pair per requested channel, in the requested order.
            let mut flat: Vec<Value> = Vec::with_capacity((request.args.len() - 2) * 2);
            for ch in &request.args[2..] {
                flat.push(Value::bulk(ch.clone()));
                flat.push(Value::Integer(table.local_numsub(ch.as_ref())));
            }
            Value::Array(Some(flat))
        }
        b"NUMPAT" => {
            let names = table.local_patterns();
            Value::Array(Some(names.into_iter().map(Value::bulk).collect()))
        }
        // SHARDCHANNELS / SHARDNUMSUB (#410): the sharded analogs of CHANNELS / NUMSUB over the
        // SHARD-channel table. Same wire-shape so the home core reuses the CHANNELS / NUMSUB
        // merges (union+dedup / sum-per-channel).
        b"SHARDCHANNELS" => {
            let pat = request.args.get(2).map(bytes::Bytes::as_ref);
            let names = table.local_shard_channels(pat, ironcache_server::glob::glob_match);
            Value::Array(Some(names.into_iter().map(Value::bulk).collect()))
        }
        b"SHARDNUMSUB" => {
            let mut flat: Vec<Value> = Vec::with_capacity((request.args.len() - 2) * 2);
            for ch in &request.args[2..] {
                flat.push(Value::bulk(ch.clone()));
                flat.push(Value::Integer(table.local_shard_numsub(ch.as_ref())));
            }
            Value::Array(Some(flat))
        }
        // An unknown subcommand never reaches the coordinator (the serve layer validates it
        // before fanning out); contribute an empty array defensively.
        _ => Value::Array(Some(Vec::new())),
    }
}

/// Fan a `PUBSUB <subcommand> [args]` introspection request out to EVERY shard's LOCAL
/// subscription table and MERGE the per-shard partials into the Redis-shaped reply
/// (SERVER_PUSH.md #20 / COORDINATOR.md #107, PR 91b). Modeled on [`fan_out_all`]: subscription
/// state is per-shard (a channel may have subscribers on several shards), so introspection must
/// gather from every core. It broadcasts the SAME internal `__ICPUBSUB <subcommand> [args]`
/// request (built from `request.args[1..]`) to every shard.
///
/// The HOME shard's partial runs LOCALLY + SYNCHRONOUSLY via [`run_local_pubsub`] (no
/// self-channel hop, exactly like `fan_out_all`'s `local` closure); every OTHER shard runs it in
/// its [`run_remote`] `__ICPUBSUB` branch. The home core then MERGES per subcommand:
///
/// - CHANNELS: UNION + dedup the per-shard channel-name arrays -> array of bulk strings.
/// - NUMSUB: SUM the per-shard `[ch, n]` pairs per channel, preserving the REQUESTED order ->
///   flat `[ch1, n1, ch2, n2, ...]` array.
/// - NUMPAT: UNION the per-shard pattern-name arrays + COUNT the distinct total -> integer.
///
/// A shard whose drain loop is gone (send error / cancelled oneshot, only at shutdown / a shard
/// panic) contributes an empty partial (it had no subscribers to report), so a degraded shard
/// never hangs the introspection. The `local` closure returns before any `.await`, so NO RefCell
/// borrow of the home shard's table is held across the awaits (the no-borrow-across-await
/// contract).
pub async fn fan_out_pubsub(inbox: &Inbox, request: &Request, home: usize) -> Value {
    // The broadcast request: the internal verb + the original subcommand and its args.
    let mut args: Vec<bytes::Bytes> = Vec::with_capacity(request.args.len());
    args.push(bytes::Bytes::from_static(ironcache_server::ICPUBSUB));
    args.extend(request.args[1..].iter().cloned());
    let ic_request = Request { args };

    // `db` is irrelevant to introspection (channels are a single cross-DB namespace this pass);
    // pass 0 for envelope symmetry with the other fan-outs.
    let replies = fan_out_all(inbox, &ic_request, 0, home, || ShardReply {
        value: run_local_pubsub(&ic_request),
        deltas: CounterDeltas::default(),
    })
    .await;

    // The subcommand drives the merge (the serve layer already validated it is one of the three).
    match crate::serve::ascii_upper(request.args.get(1).map_or(&b""[..], bytes::Bytes::as_ref))
        .as_slice()
    {
        // NUMSUB and the sharded SHARDNUMSUB (#410) both SUM per channel across shards.
        b"NUMSUB" | b"SHARDNUMSUB" => merge_pubsub_numsub(&request.args[2..], &replies),
        b"NUMPAT" => merge_pubsub_numpat(replies),
        // CHANNELS and the sharded SHARDCHANNELS (#410) both UNION+dedup across shards (the
        // fall-through); any other token cannot reach here post-validation.
        _ => merge_pubsub_channels(replies),
    }
}

/// MERGE the per-shard `PUBSUB CHANNELS` partials: UNION + DEDUP the per-shard channel-name
/// arrays into one array of bulk strings (a channel may have subscribers on more than one shard,
/// so the same name can appear in two partials -> dedup). Order is irrelevant (Redis gives none).
fn merge_pubsub_channels(replies: Vec<(usize, ShardReply)>) -> Value {
    let mut seen: std::collections::HashSet<bytes::Bytes> = std::collections::HashSet::new();
    let mut out: Vec<Value> = Vec::new();
    for (_, r) in replies {
        if let Value::Array(Some(items)) = r.value {
            for item in items {
                if let Value::BulkString(Some(name)) = item {
                    if seen.insert(name.clone()) {
                        out.push(Value::bulk(name));
                    }
                }
            }
        }
    }
    Value::Array(Some(out))
}

/// MERGE the per-shard `PUBSUB NUMSUB` partials: each partial is a flat `[ch, n, ch, n, ...]`
/// array in the SAME requested order, so the home SUMS the counts position by position and emits
/// the flat `[ch1, sum1, ch2, sum2, ...]` reply in the requested order. `requested` is the
/// original channel list (the source of truth for order + names); a channel with no subscriber on
/// any shard sums to 0. A shard-unavailable empty partial contributes 0 to every channel.
fn merge_pubsub_numsub(requested: &[bytes::Bytes], replies: &[(usize, ShardReply)]) -> Value {
    let mut totals: Vec<i64> = vec![0; requested.len()];
    for (_, r) in replies {
        if let Value::Array(Some(items)) = &r.value {
            // The partial is [ch0, n0, ch1, n1, ...]; the counts sit at odd indices, aligned to
            // `requested` by position (run_local_pubsub built it in the requested order).
            for (pos, total) in totals.iter_mut().enumerate() {
                if let Some(Value::Integer(n)) = items.get(pos * 2 + 1) {
                    *total = total.saturating_add(*n);
                }
            }
        }
    }
    let mut flat: Vec<Value> = Vec::with_capacity(requested.len() * 2);
    for (ch, total) in requested.iter().zip(totals) {
        flat.push(Value::bulk(ch.clone()));
        flat.push(Value::Integer(total));
    }
    Value::Array(Some(flat))
}

/// MERGE the per-shard `PUBSUB NUMPAT` partials: UNION the per-shard pattern-name arrays and
/// COUNT the DISTINCT total (the same pattern subscribed on two shards is ONE pattern, NOT two),
/// returning a [`Value::Integer`]. A shard-unavailable empty partial contributes no pattern.
fn merge_pubsub_numpat(replies: Vec<(usize, ShardReply)>) -> Value {
    let mut seen: std::collections::HashSet<bytes::Bytes> = std::collections::HashSet::new();
    for (_, r) in replies {
        if let Value::Array(Some(items)) = r.value {
            for item in items {
                if let Value::BulkString(Some(name)) = item {
                    seen.insert(name);
                }
            }
        }
    }
    Value::Integer(i64::try_from(seen.len()).unwrap_or(i64::MAX))
}

/// A [`ShardReply`] carrying the cross-shard unavailable error (the owning shard's drain
/// loop / receiver is gone, only during shutdown or a shard panic). Used by
/// [`fan_out_all`] so a dead shard contributes a well-formed error rather than a hang;
/// no counter deltas are attributed (the command never ran on that shard).
fn shard_unavailable_reply() -> ShardReply {
    ShardReply {
        value: Value::error(shard_unavailable_error()),
        deltas: CounterDeltas::default(),
    }
}

/// The SINGLE canonical message text for the shard-unavailable degradation (the owning
/// shard's drain loop / receiver is gone, only during shutdown or a shard panic). The
/// PRODUCER ([`shard_unavailable_error`]) and every CONSUMER (the whole-keyspace merge
/// classifiers that must tell a genuine command Error apart from this degradation) both
/// reference this one item via [`is_shard_unavailable`], so the wording lives in ONE
/// place and a hand-copied literal can never drift out of sync (FIX 6). This is the
/// `ErrorReply` MESSAGE (the text after `-ERR `), not the full wire line.
pub const SHARD_UNAVAILABLE_MSG: &str = "cross-shard target unavailable";

/// Whether `e` is the shard-unavailable degradation (vs a genuine command Error such as
/// a wrong-arity reply, which is identical on every shard and must be SURFACED). The
/// single classifier the producer and all three whole-keyspace merges share, comparing
/// the `ErrorReply` MESSAGE against [`SHARD_UNAVAILABLE_MSG`] (no `line()` String
/// allocation). FIX 6: replaces the hand-copied `"-ERR cross-shard target unavailable"`
/// literals that were coupled by convention only.
#[must_use]
pub fn is_shard_unavailable(e: &ironcache_protocol::ErrorReply) -> bool {
    e.message() == SHARD_UNAVAILABLE_MSG
}

/// The error a home core encodes when the owning shard is unreachable (its drain loop /
/// receiver is gone, only during shutdown or a shard panic). A generic `-ERR` so the
/// client gets a well-formed RESP reply instead of a stalled connection. Built from the
/// shared [`SHARD_UNAVAILABLE_MSG`] so the wording matches [`is_shard_unavailable`].
fn shard_unavailable_error() -> ironcache_protocol::ErrorReply {
    ironcache_protocol::ErrorReply::err(SHARD_UNAVAILABLE_MSG)
}

/// Encode `value` for `proto` and append to `out` (the home-core encode, mirroring the
/// serve loop's `encode_into`). Encoding stays on the home core and uses the home
/// connection's negotiated proto, never the owning shard's.
fn encode_into(out: &mut Vec<u8>, value: &Value, proto: ProtoVersion) {
    // Vec<u8> is a bytes::BufMut sink: encode writes straight into `out` (no temp BytesMut + copy).
    ironcache_protocol::encode(out, value, proto);
}

// A tiny compile-time anchor that the per-shard handle types stay reachable from this
// module (the coordinator owns the concrete ShardStoreImpl + ShardState references via
// the serve accessors). Kept as a type alias use so a future refactor that moves the
// thread-locals breaks here loudly rather than silently.
#[allow(dead_code)]
type _ShardHandles = (Rc<RefCell<ShardStoreImpl>>, Rc<RefCell<ShardState>>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_inboxes_makes_one_queue_per_shard() {
        let (inbox, rxs) = build_inboxes(4);
        assert_eq!(inbox.len(), 4);
        assert_eq!(rxs.len(), 4);
    }

    #[test]
    #[should_panic(expected = "at least one shard")]
    fn build_inboxes_rejects_zero() {
        let _ = build_inboxes(0);
    }
}

/// #391 PR-2 RECEIVER-role load tests: the boot-substitution install step ([`receive_shard_into`])
/// driven over a `UnixStream::pair` by the merged PR-1 frozen sender (`send_shard_from_frozen` /
/// `send_bulk_from_frozen`), in ONE process (no sibling). The data-safety focus: a successful receive
/// installs EXACTLY the sender's keyspace (value + encoding + absolute TTL) as the thread-local store,
/// and on ANY error NOTHING is installed (the live store is never left half-populated). The wire-level
/// contiguity / `first == F+1` / `applied == final_offset` gates live in and are exercised by
/// `upgrade::stream`; here we prove the ADOPT-ONLY-ON-ACK / DROP-ON-ERROR install contract this PR
/// adds, plus a real receiver-side verify failure (a db-count mismatch) that installs nothing.
#[cfg(all(test, unix))]
mod receiver_load_tests {
    use super::*;
    use crate::upgrade::stream::{freeze_cut, send_bulk_from_frozen, send_shard_from_frozen};
    use ironcache_repl::{ReplId, ReplObserver, ReplRing};
    use ironcache_storage::{ExpireWrite, NewValue, Store};
    use ironcache_store::SnapshotCursor;
    use std::collections::HashMap;
    use tokio::net::UnixStream;

    const DBS: u32 = 4;
    const NOW: UnixMillis = UnixMillis(1_000);
    /// A far-future absolute TTL deadline, so no key lazily expires at [`NOW`] and the tests can assert
    /// the deadline round-trips VERBATIM (proving no rebase to load time).
    const TTL_AT: UnixMillis = UnixMillis(NOW.0 + 10_000_000);
    /// A deadline far above a healthy socket-pair handoff (ms), so the timer never fires on a green run.
    const GENEROUS: Duration = Duration::from_secs(10);

    fn replid() -> ReplId {
        ReplId::from_bytes([0xCD; 20])
    }

    /// A fresh store with an OBSERVED ring installed BEFORE the writes (so every write is tracked as a
    /// delta `StreamOp`), returned with its ring for the frozen sender.
    fn observed(dbs: u32) -> (ShardStore, Rc<RefCell<ReplRing>>) {
        let ring = ReplRing::new(4096, ReplOffset::ZERO);
        let mut store = ShardStore::new(dbs);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        (store, ring)
    }

    /// Populate `store` with `n` keys spread across databases, mixing INT-encodable and RAW values
    /// (so the round-trip proves ENCODING fidelity) and giving every third key an ABSOLUTE TTL.
    fn populate(store: &mut ShardStore, n: u32) {
        for i in 0..n {
            let db = i % DBS;
            let key = format!("k{i}");
            let val = if i % 2 == 0 {
                format!("{}", i * 7) // an integer string -> int encoding
            } else {
                format!("val-{i}") // a non-numeric string -> raw encoding
            };
            let exp = if i % 3 == 0 {
                ExpireWrite::Set(TTL_AT)
            } else {
                ExpireWrite::Clear
            };
            store.upsert(
                db,
                key.as_bytes(),
                NewValue::Bytes(val.as_bytes()),
                exp,
                NOW,
            );
        }
    }

    /// Dump a store's whole keyspace as `(db, key) -> encoded-KvObj`. The encoded bytes carry value +
    /// type + encoding + ABSOLUTE TTL, so map equality proves full-fidelity convergence in ONE assert.
    fn dump_map(store: &ShardStore, now: UnixMillis) -> HashMap<(u32, Vec<u8>), Vec<u8>> {
        let mut m = HashMap::new();
        let dbs = store.databases();
        let mut c = SnapshotCursor::START;
        while !c.is_done(dbs) {
            let (chunk, next) = store.snapshot_chunk(c, 256, now);
            c = next;
            for (db, key, kv) in chunk {
                m.insert((db, key.into_vec()), ironcache_repl::encode_kvobj(&kv));
            }
        }
        m
    }

    /// Dump a store's whole keyspace as `(db, key) -> KvObj` (to read an absolute-TTL deadline back).
    fn kv_map(
        store: &ShardStore,
        now: UnixMillis,
    ) -> HashMap<(u32, Vec<u8>), ironcache_store::KvObj> {
        let mut m = HashMap::new();
        let dbs = store.databases();
        let mut c = SnapshotCursor::START;
        while !c.is_done(dbs) {
            let (chunk, next) = store.snapshot_chunk(c, 256, now);
            c = next;
            for (db, key, kv) in chunk {
                m.insert((db, key.into_vec()), kv);
            }
        }
        m
    }

    /// SUCCESS: a full frozen handoff installs EXACTLY the sender's keyspace as the thread-local store
    /// -- value + encoding + absolute TTL -- and REPLACES the whole store (a pre-existing sentinel is
    /// gone), adopting through the sender's exact cut offset.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_role_load_installs_exact_keyspace_with_encoding_and_ttl() {
        let (mut src, ring) = observed(DBS);
        populate(&mut src, 40);

        // The receiver's live store handle, pre-seeded with a STALE sentinel to prove the install
        // REPLACES the whole store (adopt, not merge).
        let mut recv_store = ShardStore::new(DBS);
        recv_store.upsert(
            0,
            b"sentinel",
            NewValue::Bytes(b"stale"),
            ExpireWrite::Clear,
            NOW,
        );
        let store_rc = Rc::new(RefCell::new(recv_store));

        let (mut a, mut b) = UnixStream::pair().expect("socket pair");
        let sender = send_shard_from_frozen(&mut a, &mut src, &ring, 0, replid(), NOW, 4);
        let receiver = receive_shard_into(
            &mut b,
            &store_rc,
            || ShardStore::new(DBS),
            DBS,
            NOW,
            GENEROUS,
        );
        let (sres, rres) = tokio::join!(sender, receiver);

        let final_off = sres.expect("sender completes");
        let recv_off = rres.expect("receiver installs the store");
        assert_eq!(
            recv_off, final_off,
            "the receiver adopted through the sender's exact cut offset"
        );

        // The installed store equals the sender's keyspace across value + encoding + absolute TTL.
        let want = dump_map(&src, NOW);
        let got = dump_map(&store_rc.borrow(), NOW);
        assert_eq!(
            got, want,
            "installed store == sender keyspace (value + encoding + absolute TTL)"
        );
        // The stale sentinel is GONE: the install replaced the whole store, it did not merge.
        assert!(
            store_rc.borrow_mut().read(0, b"sentinel", NOW).is_none(),
            "install REPLACED the whole thread-local store (sentinel gone), not merged"
        );
        // An absolute TTL deadline survived VERBATIM (k0 carries TTL_AT; no rebase to load time).
        let kv = kv_map(&store_rc.borrow(), NOW);
        assert_eq!(
            kv.get(&(0u32, b"k0".to_vec())).and_then(|o| o.expire_at),
            Some(TTL_AT),
            "the absolute TTL deadline round-trips verbatim (no rebase)"
        );
    }

    /// DROP-ON-ERROR (mid-stream): the sender streams the BULK then DROPS the socket WITHOUT the
    /// cutover, so the receiver's cutover read hits EOF after a partial load. NOTHING is installed --
    /// the live store keeps its pre-existing keyspace exactly (never left half-populated).
    #[tokio::test(flavor = "current_thread")]
    async fn receive_role_load_drops_store_on_midstream_error() {
        let (mut src, ring) = observed(DBS);
        populate(&mut src, 40);

        // Pre-seed the receiver's live store with a known keyspace that MUST survive a failed receive.
        let mut recv_store = ShardStore::new(DBS);
        recv_store.upsert(
            0,
            b"keep",
            NewValue::Bytes(b"safe"),
            ExpireWrite::Clear,
            NOW,
        );
        let store_rc = Rc::new(RefCell::new(recv_store));
        let before = dump_map(&store_rc.borrow(), NOW);

        let (mut a, mut b) = UnixStream::pair().expect("socket pair");
        // The sender ships the BULK (chunk_max = 1 -> many frames, a real partial transfer), then DROPS
        // `a` with NO cutover; the receiver's `recv_cutover` then sees EOF mid-stream and aborts.
        let sender = async move {
            let (frozen, cut) = freeze_cut(&mut src, &ring);
            let _ = send_bulk_from_frozen(&mut a, &frozen, 0, DBS, replid(), cut, NOW, 1).await;
            // `a` drops here (no `send_cutover`) -> the receiver's cutover read hits EOF.
        };
        let receiver = receive_shard_into(
            &mut b,
            &store_rc,
            || ShardStore::new(DBS),
            DBS,
            NOW,
            GENEROUS,
        );
        let ((), rres) = tokio::join!(sender, receiver);

        assert!(
            rres.is_err(),
            "a mid-stream drop (bulk received, no cutover) aborts the receive-load"
        );
        assert_eq!(
            dump_map(&store_rc.borrow(), NOW),
            before,
            "the live store is UNCHANGED on a mid-stream error (never left half-populated)"
        );
        assert_eq!(
            store_rc
                .borrow_mut()
                .read(0, b"keep", NOW)
                .unwrap()
                .as_bytes(),
            b"safe",
            "the pre-existing keyspace survives the failed receive intact"
        );
    }

    /// FAIL-CLOSED VERIFY: a receiver expecting more databases than the sender advertises aborts the
    /// receive at the HELLO (before any key is loaded), installing NOTHING. This exercises a real
    /// receiver-side verification failure (the db-count gate) end-to-end through the install seam.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_role_load_fail_closed_on_db_count_mismatch_installs_nothing() {
        let (mut src, ring) = observed(DBS);
        populate(&mut src, 12);

        // The receiver expects TWICE the sender's database count.
        let mut recv_store = ShardStore::new(DBS * 2);
        recv_store.upsert(
            0,
            b"keep",
            NewValue::Bytes(b"safe"),
            ExpireWrite::Clear,
            NOW,
        );
        let store_rc = Rc::new(RefCell::new(recv_store));
        let before = dump_map(&store_rc.borrow(), NOW);

        let (mut a, mut b) = UnixStream::pair().expect("socket pair");
        let sender = send_shard_from_frozen(&mut a, &mut src, &ring, 0, replid(), NOW, 4);
        let receiver = receive_shard_into(
            &mut b,
            &store_rc,
            || ShardStore::new(DBS * 2),
            DBS * 2,
            NOW,
            GENEROUS,
        );
        let (sres, rres) = tokio::join!(sender, receiver);

        assert!(
            rres.is_err(),
            "a db-count mismatch fails the receive-load closed"
        );
        assert!(sres.is_err(), "the sender observes the receiver's abort");
        assert_eq!(
            dump_map(&store_rc.borrow(), NOW),
            before,
            "a fail-closed verify installs NOTHING (the live store is untouched)"
        );
    }
}
