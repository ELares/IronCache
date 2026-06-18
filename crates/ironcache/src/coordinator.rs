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
use ironcache_server::{
    CommandClass, CounterDeltas, ProtoVersion, Request, UnixMillis, Value, classify,
    dispatch_remote_keyed, dispatch_remote_whole_keyspace,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

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
pub async fn run_drain_loop(
    shard_index: usize,
    mut rx: mpsc::Receiver<ShardWork>,
    ctx: ServerContext,
    inbox: Inbox,
    persist: Option<Arc<crate::persist::PersistState>>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
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
    crate::serve::ensure_shard_started(
        ctx.databases,
        ctx.info.maxmemory_policy,
        crate::serve::scan_reserved_bits(ctx.shards),
    );

    // PERSISTENCE LOAD-ON-BOOT (#58): when a data_dir is configured, THIS shard loads ITS OWN
    // committed snapshot file (`dump-shard-<shard_index>.icss`) into its store BEFORE the drain
    // loop services any remote work and before the shard accepts connections (the drain loop is
    // spawned ahead of the serve loop). A missing / torn / wrong-version file loads NOTHING (the
    // shard starts empty, today's behavior). With persistence OFF (`None`) this whole block is
    // skipped, so the boot path is byte-unchanged. The store borrow is taken + released inside the
    // synchronous load (no `.await` held across it).
    if let Some(persist) = persist.as_ref() {
        load_shard_on_boot(&ctx, persist, shard_index);
        // The PERIODIC SAVE timer (#58 save policy) is hosted on SHARD 0 only (one timer per node,
        // not N): when the interval is non-zero it ticks every `interval_secs` and, if at least
        // `min_changes` writes happened since the last save, triggers a full cross-shard save. With
        // `interval_secs == 0` (the default) NO timer is spawned. Shard 0's executor (this drain
        // loop) is the natural home-core host for the fan-out (it OWNS the inbox passed here).
        if shard_index == 0 && persist.interval_secs > 0 {
            spawn_periodic_save(ctx.clone(), inbox.clone(), Arc::clone(persist));
        }
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
        crate::replica_attach::spawn_on_shard(&ctx, store_rc, ctx.boot.bind, ctx.info.tcp_port);
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
                        let reply = run_remote(&ctx, &work.request, work.db);
                        let _ = work.reply.send(reply);
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
        let is_save_host =
            shard_index == 0 && persist.as_ref().is_some_and(|p| p.has_save_policy());
        if is_save_host {
            // SHARD 0 SAVE HOST: run the final save (fan-out to the still-alive sibling drain loops),
            // then exit 0. A save FAILURE is logged inside the helper; we still exit 0 (the prior
            // committed snapshot stays valid; the design's non-zero-on-truncation exit-code map is
            // #139's open follow-up).
            let _ = save_on_exit_if_configured(persist.as_ref(), &ctx, &inbox).await;
            eprintln!("ironcache: save-on-exit complete -> exit 0");
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
                            let reply = run_remote(&ctx, &work.request, work.db);
                            let _ = work.reply.send(reply);
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

    // INTERNAL `__ICSAVE <save_unix_secs> <shard_index> <dir>` (#58 persistence): the cross-shard
    // SAVE fan-out. This shard DUMPS ITS OWN partition (the forkless, memory-neutral
    // `snapshot_chunk` pull) to `<dir>/dump-shard-<shard_index>.icss` ATOMICALLY and returns its
    // manifest entry encoded in the reply. It reads only the store (read-only SHARED borrow) + the
    // Env clock for the lazy-expiry `now`, produces NO counter deltas (a save is not a keyed
    // write), and is handled BEFORE any mutable store borrow. The home core (the SAVE/BGSAVE
    // orchestrator) collects every shard's entry + commits the manifest LAST.
    if crate::serve::ascii_upper(request.command()) == crate::persist::ICSAVE {
        return ShardReply {
            value: save_shard_local(ctx, request),
            deltas: CounterDeltas::default(),
        };
    }

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
    let is_whole_keyspace = matches!(
        classify(&crate::serve::ascii_upper(request.command())),
        CommandClass::WholeKeyspace
    );

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
    let (tx, rx) = oneshot::channel::<ShardReply>();
    let work = ShardWork {
        // Clone is cheap: Request is Vec<Bytes> (refcounted buffers).
        request: request.clone(),
        db,
        reply: tx,
    };
    // Await-on-full back-pressure. A send error means the owning shard's receiver is gone
    // (shutdown / shard died); reply with a proto-shaped error rather than hang.
    if inbox[target].send(work).await.is_err() {
        encode_into(out, &Value::error(shard_unavailable_error()), proto);
        return;
    }
    match rx.await {
        Ok(reply) => {
            // The home core deliberately does NOT re-apply `reply.deltas`: the OWNING
            // shard already folded those data counters into its own ShardState (the data
            // lives there), so applying them here too would double-count. They ride back
            // only so a future observability pass could attribute cross-shard work; PASS 1
            // discards them here. The issuing connection's commands_processed is bumped by
            // the serve loop (matching the local fast path), not from these deltas.
            let _ = &reply.deltas;
            encode_into(out, &reply.value, proto);
        }
        Err(_) => encode_into(out, &Value::error(shard_unavailable_error()), proto),
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

/// Run ONE shard's `__ICSAVE <save_unix_secs> <shard_index> <dir>` against THIS shard's store
/// (#58 persistence): DUMP this shard's whole partition to `<dir>/dump-shard-<shard_index>.icss`
/// ATOMICALLY (the forkless, memory-neutral `snapshot_chunk` pull + tmp -> fsync -> rename) and
/// return its manifest entry encoded as `*3 [:shard :keys :crc]` (see
/// [`crate::persist::encode_save_reply`]). On an I/O error it returns a proto error the home core
/// surfaces as a failed SAVE.
///
/// It reads `now` from THIS shard's Env clock (the determinism seam, ADR-0003: the lazy-expiry
/// basis the dump skips dead keys at) via a SHORT shared borrow, and borrows the store READ-ONLY
/// for the dump (the dump never mutates). Both borrows are taken + released inside this synchronous
/// call (the no-borrow-across-await contract). NOTE (M4): this holds the store borrow across the
/// ENTIRE `save_shard_to_dir` call -- the whole keyspace dump AND the file fsync -- so this shard is
/// BLOCKED for its full dump + fsync latency (per-shard-consistent). The chunked `snapshot_chunk`
/// bounds the dump's MEMORY to O(`DUMP_CHUNK`), but the borrow is NOT released between chunks here,
/// so BGSAVE blocks this shard exactly as SAVE does (BGSAVE only frees the ISSUING connection).
/// Produces NO counter deltas (a save is not a keyed write).
#[must_use]
fn save_shard_local(ctx: &ServerContext, request: &Request) -> Value {
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
    // `now` from THIS shard's wall clock (ADR-0003), a short borrow dropped before the store dump.
    let now = UnixMillis(env.borrow().now_unix_millis());
    // Read-only store borrow for the dump (the forkless snapshot pull). save_shard_to_dir runs the
    // chunked snapshot_chunk + writes the file atomically.
    let result = {
        let store = store_rc.borrow();
        ironcache_persist::save_shard_to_dir(&*store, shard_index, &dir, now)
    };
    match result {
        Ok(entry) => crate::persist::encode_save_reply(&entry),
        Err(e) => Value::error(ironcache_protocol::ErrorReply::err(format!(
            "save failed: {e}"
        ))),
    }
}

/// Run the HOME shard's `__ICSAVE` partial LOCALLY + SYNCHRONOUSLY (#58 persistence), returning the
/// home shard's [`ShardReply`]. This is the `local` closure [`fan_out_split`] runs for the home
/// shard: the home core does NOT round-trip its OWN save through its channel; it dumps inline via
/// the SAME [`save_shard_local`] every remote shard runs (so the home shard's file is byte-identical
/// to a remote shard's). A save produces no counter deltas, so the reply carries default deltas.
/// Every per-shard borrow is taken + released inside the synchronous `save_shard_local` (the
/// no-borrow-across-await contract; the caller awaits remote replies AFTER this returns).
#[must_use]
pub fn run_local_save(ctx: &ServerContext, request: &Request) -> ShardReply {
    ShardReply {
        value: save_shard_local(ctx, request),
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
    let loaded = {
        let mut store = store_rc.borrow_mut();
        ironcache_persist::load_shard_resharded(
            &mut store,
            &persist.dir,
            shard_index,
            shard_count,
            now,
            ironcache_server::owner_shard,
        )
    };
    if loaded > 0 {
        eprintln!(
            "ironcache: shard {shard_index} loaded {loaded} keys from {}",
            persist.dir.display()
        );
    }
}

/// Spawn the PERIODIC SAVE timer (#58 save policy) on SHARD 0's executor (one timer per node). Every
/// `persist.interval_secs` seconds it checks the dirty-write counter: if at least `min_changes`
/// writes happened since the last save, it triggers a full cross-shard save (the SAME
/// [`crate::persist::do_save_all`] SAVE/BGSAVE use). With `interval_secs == 0` this is never spawned
/// (the caller gates on it), so the default posture has NO timer.
///
/// ## Borrow / determinism discipline
///
/// The loop awaits the interval through the [`Runtime`] timer SEAM (NOT `tokio::time`, ADR-0003) and
/// holds NO RefCell borrow across the awaits (the save fan-out's per-shard `save_shard_local` is
/// synchronous and runs on each shard's own executor, so this home-core task only awaits channel
/// replies). The save id / timestamp come from the env Clock seam.
fn spawn_periodic_save(
    ctx: ServerContext,
    inbox: Inbox,
    persist: Arc<crate::persist::PersistState>,
) {
    use ironcache_runtime::Runtime;
    let rt = ironcache_runtime::TokioRuntime::new();
    let interval = std::time::Duration::from_secs(persist.interval_secs);
    let home = ShardId {
        index: 0,
        total: inbox.len(),
    };
    rt.spawn_on_shard(async move {
        loop {
            rt.timer(interval).await;
            // Skip this tick if too few writes happened since the last save (the `changes` half of
            // the Redis `save <seconds> <changes>` policy). `min_changes == 0` always fires.
            let dirty = persist.dirty.load(std::sync::atomic::Ordering::Relaxed);
            if dirty < persist.min_changes {
                continue;
            }
            // Serialize against a concurrent SAVE/BGSAVE; if one is already running, skip this tick.
            // The RAII guard releases the latch on completion, panic, OR cancellation (H3).
            let Some(_guard) = persist.try_begin_save() else {
                continue;
            };
            // The save timestamp from shard 0's Env clock (the determinism seam, ADR-0003).
            let now_secs = shard_env().borrow().now_unix_millis() / 1_000;
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
    // SHUTDOWN / signal-stop decision [redis-shutdown-save-nosave-default].
    let Some(persist) = persist.filter(|p| p.has_save_policy()) else {
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
        eprintln!(
            "ironcache: save-on-exit -- a prior save did not finish within SHUTDOWN_SAVE_WAIT; \
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
            eprintln!(
                "ironcache: save-on-exit failed: {msg} (the prior committed snapshot stays valid)"
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
        b"NUMSUB" => merge_pubsub_numsub(&request.args[2..], &replies),
        b"NUMPAT" => merge_pubsub_numpat(replies),
        // CHANNELS (or any other token, which cannot reach here post-validation).
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
    let mut bm = bytes::BytesMut::with_capacity(64);
    ironcache_protocol::encode(&mut bm, value, proto);
    out.extend_from_slice(&bm);
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
