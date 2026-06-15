// SPDX-License-Identifier: MIT OR Apache-2.0
//! Server wiring: config -> runtime -> per-shard accept -> per-connection
//! read/dispatch/write loop (CLI_BINARY.md "zero-config boot", RUNTIME.md).
//!
//! Each shard runs on its own OS thread with its own current-thread tokio runtime
//! (ADR-0002, shared-nothing). Per-shard state (the client-id counter and the
//! observability counters) is core-local: it lives in `Rc<RefCell<..>>` owned by
//! the shard's tasks, never shared across cores, so there is no cross-core
//! synchronization. The connection loop decodes RESP, dispatches Tier-0 commands,
//! and writes the encoded reply.

use crate::coordinator;
use ironcache_config::{Config, RuntimeConfig};
use ironcache_env::{Clock, Env, Rng, SystemEnv};
use ironcache_eviction::{Policy, map_policy_name};
use ironcache_observe::{CounterSnapshot, MemoryInfo, ServerInfo, ShardCounters};
use ironcache_runtime::bootstrap::{ShardConfig, ShardId, ShardSet};
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{
    ConnState, CounterDeltas, DecodeOutcome, EXPIRE_CYCLE_INTERVAL, Limits, MAX_RECLAIM_PER_CYCLE,
    ProtoVersion, Request, ScanCursor, TimingWheel, UnixMillis, decode, dispatch_with_cmd,
    drain_due_keys, route,
};
use ironcache_storage::CountingAccounting;
use ironcache_store::{ShardStore, process_memory};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// The name of the global allocator selected at build time, for INFO
/// `mem_allocator`. This MUST track the `#[global_allocator]` cfg in `main.rs`
/// (jemalloc on every target except MSVC, where it falls back to the system
/// allocator), so INFO never claims jemalloc on a build that linked the system
/// allocator.
#[cfg(not(target_env = "msvc"))]
pub const GLOBAL_ALLOCATOR_NAME: &str = "jemalloc";
#[cfg(target_env = "msvc")]
pub const GLOBAL_ALLOCATOR_NAME: &str = "libc";

/// The concrete per-shard store the binary wires: the `ShardStore` over the
/// configured eviction [`Policy`] and the logical-byte accounting hook. The generic
/// dispatch runs against this through the `Store` + `Admit` waist traits.
///
/// `pub(crate)` so the [`crate::coordinator`] drain loop names the same concrete store
/// type the per-shard thread-locals hold (it runs remote keyed work against it).
pub(crate) type ShardStoreImpl = ShardStore<Policy, CountingAccounting>;

/// Per-shard, core-local mutable state. Single-threaded access on the shard's
/// thread (no `Send`/`Sync` needed, no locks; shared-nothing ADR-0002).
///
/// `pub(crate)` so the [`crate::coordinator`] drain loop can fold a remote command's
/// counter deltas into the OWNING shard's counters (the data lives there).
pub(crate) struct ShardState {
    pub(crate) next_client_id: u64,
    pub(crate) counters: ShardCounters,
    /// The last runtime-config GENERATION this shard observed (PR-4b). Dispatch compares
    /// the shared `RuntimeConfig::generation()` against this once per command (a relaxed
    /// atomic load + integer compare, NO lock when unchanged) and, on a change, rebuilds
    /// this shard's eviction policy from the new `maxmemory-policy` name. Core-local
    /// (per shard, shared-nothing ADR-0002): each shard catches up to a `CONFIG SET
    /// maxmemory-policy` on its next command.
    pub(crate) last_policy_generation: u64,
}

/// Boot the server: derive the shard config from `config`, start the shard set,
/// and return the [`ShardSet`] handle for shutdown. Errors if the listener cannot
/// bind (e.g. port in use).
pub fn run_server(config: &Config) -> anyhow::Result<ShardSet> {
    let bind: SocketAddr = SocketAddr::new(config.bind, config.port);
    let shard_cfg = ShardConfig {
        shards: config.shards,
        bind,
    };

    // The BOOT eviction policy NAME is leaked to a 'static str so INFO/ServerInfo can
    // hold it cheaply for the process lifetime as the STATIC boot fact. The CURRENT
    // effective policy (which a `CONFIG SET maxmemory-policy` changes) lives in the
    // RuntimeConfig cell; INFO reads it from there (PR-4b). One small leak at boot.
    let policy_name: &'static str = Box::leak(config.maxmemory_policy.clone().into_boxed_str());

    // The process-wide runtime-config overlay (PR-4b, the highest-precedence layer):
    // ONE Arc shared (cloned) into every shard's context, exactly like the shutdown
    // AtomicBool precedent. A `CONFIG SET` mutates it; the per-command reads are cheap
    // atomic loads (maxmemory/generation) with the string params behind a lock taken
    // only on CONFIG SET. Seeded from the boot-resolved config.
    let runtime = RuntimeConfig::from_config(config);

    // Static, cheaply-cloned server context shared by value onto each shard. The
    // mutable cross-shard state is ONLY the runtime cell (an Arc); the rest is
    // immutable, so cloning per shard does not violate shared-nothing.
    let ctx_template = ServerContext {
        runtime,
        boot: config.clone(),
        databases: config.databases,
        shards: config.shards,
        info: ServerInfo {
            tcp_port: config.port,
            shards: config.shards,
            pid: std::process::id(),
            // started_at is filled in per shard at boot via the shard's clock so
            // uptime is measured from when the shard's Env started.
            started_at: ironcache_env::Monotonic::ZERO,
            maxmemory: config.maxmemory,
            maxmemory_policy: policy_name,
            mem_allocator: GLOBAL_ALLOCATOR_NAME,
        },
    };
    let default_proto = if config.default_resp3 {
        ProtoVersion::Resp3
    } else {
        ProtoVersion::Resp2
    };

    // The cross-shard coordinator substrate (COORDINATOR.md #107): one bounded inbound
    // queue PER shard. `inbox` (the shared senders) is captured into the per-connection
    // serve closure so any home core can route a single-key command to the shard that
    // OWNS the key; `rxs` (the matching receivers, one per shard, in shard-index order)
    // are handed to `run_shards`, which moves each into its shard's drain loop. With
    // shards == 1 every key is home-owned, so the queues carry no traffic and the path is
    // byte-identical to before this layer (verified by the coordinator_stage1 parity test).
    let total = config.shards.max(1);
    let (inbox, rxs) = coordinator::build_inboxes(total);

    // Clone the (immutable-after-boot) context for the drain closure BEFORE the serve
    // closure moves `ctx_template` in. Each shard's drain loop gets this clone so it has
    // the admission budget / policy generation / databases it needs to run remote keyed
    // work; the per-connection serve closure clones the original per connection.
    let drain_ctx = ctx_template.clone();

    let serve = {
        let inbox = inbox.clone();
        move |rt: TokioRuntime, stream: tokio::net::TcpStream, shard: ShardId| {
            let ctx = ctx_template.clone();
            let inbox = inbox.clone();
            async move {
                serve_connection(rt, stream, shard, ctx, default_proto, inbox).await;
            }
        }
    };

    // The per-shard drain closure: turn a shard's receiver into its drain-loop future.
    // run_shards spawns it on each shard's LocalSet alongside the accept loop, BEFORE
    // accepting (a shard can own keys without ever accepting a connection).
    let drain = move |rx: tokio::sync::mpsc::Receiver<coordinator::ShardWork>| {
        let ctx = drain_ctx.clone();
        coordinator::run_drain_loop(rx, ctx)
    };

    let set = ironcache_runtime::bootstrap::run_shards(&shard_cfg, serve, rxs, drain)?;
    Ok(set)
}

thread_local! {
    // The shard's core-local state. Created lazily on first use on each shard
    // thread; never shared across threads.
    static SHARD: RefCell<Option<Rc<RefCell<ShardState>>>> = const { RefCell::new(None) };
    // The shard's per-shard store: the per-DB hashbrown kvobj map (ADR-0005) wired
    // with the configured eviction policy. Held as Rc<RefCell<..>> exactly like ENV,
    // so it is core-local and unsynchronized; created lazily per shard thread. The
    // concrete ShardStore implements the Store + Admit waist traits the generic
    // dispatch runs against.
    static STORE: RefCell<Option<Rc<RefCell<ShardStoreImpl>>>> = const { RefCell::new(None) };
    // The shard's per-shard TTL timing wheel (#51), held as Rc<RefCell<..>> exactly
    // like STORE/ENV so it is core-local and unsynchronized (ADR-0002/0005). The
    // active drain pops due keys from it before each command; TTL-setting commands
    // register deadlines into it. Created lazily per shard thread.
    static WHEEL: RefCell<Option<Rc<RefCell<TimingWheel>>>> = const { RefCell::new(None) };
    // One SystemEnv per shard thread (the sanctioned real-time boundary). It is
    // wrapped in a RefCell so the determinism seam's RNG half is REACHABLE: the
    // shard is single-threaded (current-thread runtime, !Send tasks), so clock
    // reads go through `.borrow()` and `Env::rng` through `.borrow_mut()` with no
    // cross-core synchronization. A bare `Rc<SystemEnv>` would make `.rng()`
    // (which needs `&mut self`) structurally uncallable; PR-2/PR-3 need RNG on the
    // hot path (S3-FIFO sampling, TTL jitter).
    static ENV: RefCell<Option<Rc<RefCell<SystemEnv>>>> = const { RefCell::new(None) };
    static STARTED_AT: RefCell<Option<ironcache_env::Monotonic>> = const { RefCell::new(None) };
    // Whether THIS shard thread has already spawned its background active-expiry timer
    // task (PR-3c). Spawned exactly ONCE per shard, lazily on the first connection (the
    // shard's tokio LocalSet must exist, which it does once a connection is being
    // served), so an idle shard that has had at least one connection still reclaims
    // expired memory with no further commands. A plain Cell suffices (single-threaded
    // per shard; shared-nothing ADR-0002).
    static EXPIRE_TASK_SPAWNED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    // The shard's PER-SHARD Pub/Sub subscription table (SERVER_PUSH.md #20, PR 91a): channel
    // -> {conn id -> push sender}. Core-local (per shard, shared-nothing ADR-0002) with NO
    // lock; held as Rc<RefCell<..>> exactly like STORE/WHEEL/ENV so a connection task, the
    // coordinator drain loop's `__ICPUBLISH` delivery, and the disconnect cleanup all reach
    // the SAME table on this shard. Created lazily per shard thread. The only cross-core
    // handle it stores is the `Send` mpsc push sender of each subscriber (a PUBLISH fans out
    // to every shard, so each shard renders to its own connections from its own table).
    static PUBSUB: RefCell<Option<Rc<RefCell<crate::pubsub::ShardPubSub>>>> =
        const { RefCell::new(None) };
}

/// The shard's per-shard Pub/Sub subscription table handle (SERVER_PUSH.md #20, PR 91a),
/// lazily created on first use on this shard thread (mirrors [`shard_store`] / [`shard_state`]).
/// `pub(crate)` so the [`crate::coordinator`] `__ICPUBLISH` delivery reaches the SAME table the
/// connection tasks register into.
pub(crate) fn shard_pubsub() -> Rc<RefCell<crate::pubsub::ShardPubSub>> {
    PUBSUB.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(crate::pubsub::ShardPubSub::default())));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

/// Spawn the per-shard BACKGROUND active-expiry timer task ONCE on this shard's
/// executor (EXPIRATION.md idle-shard memory boundedness, PR-3c). Idempotent per shard:
/// guarded by [`EXPIRE_TASK_SPAWNED`] so repeated connections do not spawn duplicates.
///
/// The task loops: `rt.timer(EXPIRE_CYCLE_INTERVAL).await` (the Runtime timer SEAM, NOT
/// `tokio::time` directly, ADR-0003), then reads `now` from the shard's Env clock (NOT
/// std time) and drains a BOUNDED batch from the wheel via the SAME [`drain_due_keys`]
/// helper the opportunistic per-command path uses. The reclaimed count folds into the
/// shard's `expired_keys` counter so idle reclamation shows up in INFO alongside the
/// command-path drain.
///
/// ## Borrow discipline (critical, ADR-0002/0005)
///
/// Each tick borrows the per-shard ENV / STORE / WHEEL / STATE RefCells ONLY briefly
/// and DROPS every borrow BEFORE the next `.await`. A RefCell borrow held across an
/// await would double-borrow-panic when a concurrently-scheduled command handler runs
/// on the same single thread between the timer firing and resuming. The tick body is a
/// single non-async block (`expire_cycle_tick`) that takes and releases all borrows and
/// returns a plain `u64`, so no `Ref`/`RefMut` is alive when the loop awaits the timer.
/// Bring up THIS shard's background tasks at shard boot: lazily init the per-shard
/// store/wheel/env/state handles and spawn the active-expiry timer task ONCE.
///
/// Called from the coordinator's per-shard drain-loop setup at SHARD BOOT (not on the
/// first connection), because a shard can now OWN keys (and so need active expiry) even
/// if it never accepts a connection (COORDINATOR.md #107 partitions the keyspace across
/// shards). It is idempotent (the spawn is guarded by [`EXPIRE_TASK_SPAWNED`]) and runs
/// on the shard's LocalSet (the drain loop is spawned there), which is exactly what
/// `spawn_on_shard` needs. `databases`/`policy_name` are the boot facts the store
/// lazy-init needs (the same values `serve_connection` passes).
///
/// The [`TokioRuntime`] backend is zero-sized (it carries no state; the shard's tasks
/// live on the LocalSet), so it is constructed here rather than threaded in.
pub(crate) fn ensure_shard_started(databases: u32, policy_name: &str, reserved_bits: u32) {
    let env = shard_env();
    let store_rc = shard_store(databases, policy_name, reserved_bits);
    let wheel_rc = shard_wheel();
    let state_rc = shard_state();
    spawn_expire_task(TokioRuntime::new(), env, store_rc, wheel_rc, state_rc);
}

fn spawn_expire_task(
    rt: TokioRuntime,
    env: Rc<RefCell<SystemEnv>>,
    store_rc: Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: Rc<RefCell<TimingWheel>>,
    state_rc: Rc<RefCell<ShardState>>,
) {
    if EXPIRE_TASK_SPAWNED.with(std::cell::Cell::get) {
        return;
    }
    EXPIRE_TASK_SPAWNED.with(|c| c.set(true));
    rt.spawn_on_shard(async move {
        loop {
            // Await the cycle interval through the Runtime timer seam (NOT tokio::time
            // directly). No RefCell borrow is held across this await.
            rt.timer(EXPIRE_CYCLE_INTERVAL).await;
            // One tick: take + release all borrows inside this call, returning a u64.
            // Nothing borrowed survives to the next await iteration.
            expire_cycle_tick(&env, &store_rc, &wheel_rc, &state_rc);
        }
    });
}

/// Run ONE background active-expiry cycle: read `now` from the Env clock, drain a
/// bounded batch from the wheel (reusing [`drain_due_keys`]), and fold the reclaimed
/// count into the shard's `expired_keys` counter. Returns the number of keys reaped
/// (for the wiring smoke test).
///
/// This is a SYNCHRONOUS function: it acquires every RefCell borrow and releases it
/// before returning, so the async caller never holds a borrow across an `.await` (the
/// borrow-discipline contract above). The clock read (`env.borrow()`) and the
/// store/wheel mutation (separate RefCells) do not alias.
fn expire_cycle_tick(
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
) -> u64 {
    // The WORK (which keys are due) is decided by the Env clock (ADR-0003), so a DST
    // replay reaps the identical keys; only the FIRING schedule is wall-clock.
    let now = UnixMillis(env.borrow().now_unix_millis());
    let reaped = {
        let mut store = store_rc.borrow_mut();
        let mut wheel = wheel_rc.borrow_mut();
        // The `&mut *` derefs THROUGH the RefMut to the concrete ShardStore/TimingWheel
        // the generic `drain_due_keys` bound needs (a bare `&mut wheel` would be
        // `&mut RefMut<..>`, which does not satisfy `S: Store + ActiveExpiry`). The
        // deref is load-bearing, so the auto-deref lint is silenced here.
        #[allow(clippy::explicit_auto_deref)]
        drain_due_keys(&mut *wheel, &mut *store, now, MAX_RECLAIM_PER_CYCLE)
        // store + wheel borrows DROP here, before the state borrow below and before
        // the caller's next await.
    };
    if reaped > 0 {
        let deltas = CounterDeltas {
            expired: reaped,
            ..CounterDeltas::default()
        };
        state_rc.borrow_mut().counters.apply(deltas);
    }
    reaped
}

pub(crate) fn shard_state() -> Rc<RefCell<ShardState>> {
    SHARD.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(ShardState {
                next_client_id: 1,
                counters: ShardCounters::new(),
                // Start at 0 (the RuntimeConfig generation also starts at 0): the first
                // CONFIG SET maxmemory-policy bumps it, and this shard notices on its
                // next command.
                last_policy_generation: 0,
            })));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

/// The number of LOW `scan_hash` bits the cross-shard composite SCAN cursor must reserve
/// for the shard index, given the total shard count (COORDINATOR.md #107, FIX 1). `0` for
/// a single (or degenerate zero) shard server -- SCAN is then byte-identical to the
/// pre-coordinator behavior (the inner cursor passes through verbatim) -- and
/// [`ScanCursor::SHARD_BITS`] when more than one shard is configured, so `scan_step`
/// returns BAND-ALIGNED next cursors the composite cursor round-trips losslessly.
pub(crate) fn scan_reserved_bits(total_shards: usize) -> u32 {
    if total_shards > 1 {
        ScanCursor::SHARD_BITS
    } else {
        0
    }
}

pub(crate) fn shard_store(
    databases: u32,
    policy_name: &str,
    reserved_bits: u32,
) -> Rc<RefCell<ShardStoreImpl>> {
    STORE.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            // Build the shard's eviction policy from the configured name, seeding the
            // Random variant from THIS shard's Env RNG (ADR-0003: no std rand; the
            // seed comes through the determinism seam). The name was validated at
            // config time, so map_policy_name cannot return None here; fall back to
            // the cache default defensively if a future un-validated path slips in.
            let seed = shard_env().borrow_mut().rng().next_u64();
            let policy = map_policy_name(policy_name, seed).unwrap_or_else(Policy::cache_default);
            // The reserved-band width makes `scan_step` return band-aligned next cursors
            // for the cross-shard composite cursor (0 on a single-shard server, so SCAN
            // stays byte-identical to before the coordinator layer; FIX 1).
            let store = ShardStore::with_hooks(databases, policy, CountingAccounting::new())
                .with_scan_band_bits(reserved_bits);
            *b = Some(Rc::new(RefCell::new(store)));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

pub(crate) fn shard_wheel() -> Rc<RefCell<TimingWheel>> {
    WHEEL.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(TimingWheel::new())));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

pub(crate) fn shard_env() -> Rc<RefCell<SystemEnv>> {
    ENV.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            let env = SystemEnv::new();
            // Record the shard's boot instant for uptime.
            STARTED_AT.with(|s| {
                use ironcache_env::Clock;
                *s.borrow_mut() = Some(env.now());
            });
            *b = Some(Rc::new(RefCell::new(env)));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

fn shard_started_at() -> ironcache_env::Monotonic {
    STARTED_AT.with(|s| s.borrow().unwrap_or(ironcache_env::Monotonic::ZERO))
}

async fn serve_connection(
    rt: TokioRuntime,
    mut stream: tokio::net::TcpStream,
    home: ShardId,
    mut ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
) {
    let env = shard_env();
    let state_rc = shard_state();
    // The reserved-band width is derived from the configured TOTAL shard count so SCAN's
    // composite cursor is band-aligned when shards > 1 (FIX 1); 0 keeps single-shard SCAN
    // byte-identical.
    let reserved_bits = scan_reserved_bits(ctx.shards);
    let store_rc = shard_store(ctx.databases, ctx.info.maxmemory_policy, reserved_bits);
    let wheel_rc = shard_wheel();
    // Ensure this shard's background active-expiry timer is up (PR-3c, idempotent). The
    // canonical spawn point is now SHARD BOOT (the coordinator drain loop calls
    // `ensure_shard_started` before its recv loop, COORDINATOR.md #107: a key-owning shard
    // must reclaim even with no connection). This call is the same idempotent helper, so a
    // connection arriving before the drain loop's first poll still gets the timer started;
    // the EXPIRE_TASK_SPAWNED guard makes the duplicate call a no-op.
    ensure_shard_started(ctx.databases, ctx.info.maxmemory_policy, reserved_bits);
    // Correct the context's started_at to this shard's boot instant.
    ctx.info.started_at = shard_started_at();

    let addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let laddr = stream
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();

    let client_id = {
        let mut s = state_rc.borrow_mut();
        let id = s.next_client_id;
        s.next_client_id += 1;
        s.counters.on_connection_open();
        id
    };

    let mut conn = ConnState::new(client_id, default_proto, ctx.requires_auth(), addr, laddr);

    // The per-connection PUSH channel (SERVER_PUSH.md #20, PR 91a). `push_tx` is the `Send`
    // handle registered into the home shard's subscription table on SUBSCRIBE (so a PUBLISH on
    // any core can hand this connection a message); `push_rx` is owned by THIS serve loop and
    // drained in the subscribe-mode idle-wait below. Bounded for back-pressure: a slow consumer
    // past the bound is shed by the publisher (its sender dropped), so `push_rx.recv()` then
    // returns None and this loop exits (treated as a disconnect). Created for EVERY connection
    // (cheap), but only WIRED into the select! once the connection enters subscribe mode, so the
    // non-subscriber hot path never touches it.
    let (push_tx, mut push_rx) =
        tokio::sync::mpsc::channel::<crate::pubsub::ServerPush>(crate::pubsub::PUSH_CHANNEL_BOUND);

    let limits = Limits::default();
    let mut read_buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);

    'conn: loop {
        // Drain every complete request currently buffered (pipelining), building
        // one combined output buffer, then flush once.
        out.clear();
        loop {
            match decode(&read_buf, &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    // Route + dispatch one decoded request (COORDINATOR.md #107, Stage 1),
                    // appending its encoded reply to `out`; returns whether to close (QUIT).
                    // Factored out of the serve loop so the connection loop stays small.
                    let close = route_and_dispatch(
                        &ctx, &mut conn, home, &inbox, &push_tx, &env, &store_rc, &wheel_rc,
                        &state_rc, &request, &mut out,
                    )
                    .await;
                    read_buf.drain(..consumed);
                    if close {
                        // Flush the QUIT reply then close. send returns the owned
                        // buffer (owned-buffer model); we are closing, so the
                        // returned buffer is dropped rather than reclaimed.
                        let _ = rt.send(&mut stream, std::mem::take(&mut out)).await;
                        break 'conn;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Error(e) => {
                    // Protocol error: write it and close the connection (hardening).
                    encode_into(&mut out, &ironcache_server::Value::Error(e), conn.proto);
                    let _ = rt.send(&mut stream, std::mem::take(&mut out)).await;
                    break 'conn;
                }
            }
        }

        if !out.is_empty() {
            // Owned-buffer send: hand `out` over and take the returned buffer back.
            match rt.send(&mut stream, std::mem::take(&mut out)).await {
                Ok(returned) => out = returned,
                Err(_) => break,
            }
        }

        // IDLE WAIT. The NON-subscriber path (the common, hot path) is BYTE-IDENTICAL to before
        // pub/sub: just await `rt.recv`, no select! overhead. Only a connection in SUBSCRIBE mode
        // pays for the select! that ALSO drains the push channel (`subscriber_idle_wait`). FIFO
        // ordering holds because `out` was already flushed above before we reach this idle wait,
        // so a push is rendered and sent only AFTER the in-flight command batch's reply went out
        // -- a push never precedes a command reply on the connection (SERVER_PUSH.md FIFO).
        if conn.is_subscriber() {
            if subscriber_idle_wait(
                &rt,
                &mut stream,
                &mut push_rx,
                &mut read_buf,
                &mut out,
                conn.proto,
            )
            .await
            {
                break;
            }
        } else {
            // Need more bytes: read.
            let Ok(res) = rt.recv(&mut stream, std::mem::take(&mut read_buf)).await else {
                break;
            };
            read_buf = res.buf;
            if res.n == 0 {
                break; // peer closed
            }
        }
    }

    // Connection close: deregister this connection's PUB/SUB subscriptions from THIS shard's
    // subscription table (SERVER_PUSH.md #20, PR 91a). The connection's subscriptions are
    // home-shard-local, so this runs on the home shard, driven off `conn.sub_channels` /
    // `sub_patterns` (O(subs)). Like the WATCH cleanup below, this is the only exit that bypasses
    // the per-command deregistration, so a QUIT / error close / peer close all prune the table
    // here. Then DROP `push_tx`: with both the registered table senders and this owned handle
    // gone, the channel is fully closed (a no-op for a non-subscriber). A no-op when not
    // subscribed. The borrow of the subscription table is taken + released inside the helper.
    deregister_all_subscriptions(&conn);
    drop(push_tx);

    // Connection close: deregister this connection's WATCHes from the shard store
    // (TRANSACTIONS.md, PR-10b). `ConnState` holds the watch SNAPSHOTS but not the store
    // handle (the store carries the per-key watcher counts), so the deregistration is
    // done explicitly here in the serve loop before `conn` drops. This is the only exit
    // that bypasses the dispatch arms (which deregister on EXEC/DISCARD/UNWATCH/RESET), so
    // it prevents a watch from lingering in the store after a client disconnects mid-WATCH
    // (a QUIT, an error close, or the peer closing the socket all land here). A no-op when
    // the connection has no active watch set. Borrow the store separately from the state
    // counter borrow below (distinct RefCells, no alias).
    if !conn.watch.is_empty() {
        use ironcache_storage::Watch;
        store_rc.borrow_mut().unwatch(&conn.watch);
        conn.clear_watch();
    }
    state_rc.borrow_mut().counters.on_connection_close();
}

/// The SUBSCRIBE-mode idle wait (SERVER_PUSH.md #20, PR 91a): `select!` between draining the
/// per-connection push channel and reading the next command. Returns `true` when the connection
/// should CLOSE (the push consumer was shed -> sender dropped, a peer close, or an I/O error),
/// `false` to keep looping. Split out of [`serve_connection`] so the hot non-subscriber path
/// stays a plain `rt.recv` and this select! lives in one place.
///
/// The push branch is `biased` first (a ready push is delivered promptly), but the read branch
/// is polled each iteration too, so a command is never starved. A delivered push is COALESCED
/// with any further already-queued pushes (`try_recv`, non-blocking) into ONE write. The read
/// branch reads into a FRESH buffer and APPENDS to `read_buf`: had the push branch won instead,
/// a `read_buf` moved into the CANCELLED recv future would be lost with any partial frame it
/// held; reading into a temporary keeps `read_buf`'s partial bytes safe across cancellation. No
/// RefCell borrow is held across the `.await`s (render is pure; the subscription table is untouched).
async fn subscriber_idle_wait(
    rt: &TokioRuntime,
    stream: &mut tokio::net::TcpStream,
    push_rx: &mut tokio::sync::mpsc::Receiver<crate::pubsub::ServerPush>,
    read_buf: &mut Vec<u8>,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) -> bool {
    tokio::select! {
        biased;
        maybe_push = push_rx.recv() => {
            let Some(push) = maybe_push else {
                // Sender dropped: the publisher SHED this slow consumer (its push channel
                // overflowed past the bound). Treat as a disconnect and close.
                return true;
            };
            // `out` was flushed before the idle wait, so it is empty here; rendering into it
            // preserves the flush-before-idle FIFO ordering.
            out.clear();
            encode_into(out, &push.render(proto), proto);
            while let Ok(next) = push_rx.try_recv() {
                encode_into(out, &next.render(proto), proto);
            }
            rt.send(stream, std::mem::take(out)).await.map_or(true, |returned| {
                *out = returned;
                false
            })
        }
        res = rt.recv(stream, Vec::new()) => {
            let Ok(res) = res else { return true; };
            if res.n == 0 {
                return true; // peer closed
            }
            read_buf.extend_from_slice(&res.buf);
            false
        }
    }
}

/// Whether the SUBSCRIBE-MODE gate BLOCKS `cmd_upper` for this connection (SERVER_PUSH.md #20,
/// PR 91a), writing the byte-exact Redis error into `out` and bumping `commands_processed` when
/// it does. A RESP2 subscriber may run ONLY the pub/sub control set + PING/QUIT/RESET; RESP3 has
/// NO restriction. Returns `false` (does nothing) when the connection is not a RESP2 subscriber
/// or the command is allowed.
///
/// This gate is enforced in TWO places by necessity: `dispatch` checks it on the HOME path, but a
/// subscriber's KEYED command on a REMOTE-owned key takes the `dispatch_via` hop (and the multikey
/// / spanning / whole-keyspace fan-outs), which calls `dispatch_remote_keyed` DIRECTLY and BYPASSES
/// the dispatch gate -- so a RESP2 subscriber's remote GET would wrongly execute. Checking it HERE,
/// BEFORE the routing decision (exactly like the in-MULTI guards), gates EVERY route uniformly. The
/// pub/sub commands + the subscriber PING are already handled by `try_handle_pubsub`; QUIT/RESET are
/// AlwaysHome (allowed). With `shards == 1` the dispatch gate alone suffices, but this check is
/// byte-identical (same error), so single-shard parity holds.
fn subscriber_gate_blocks(
    conn: &ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    out: &mut Vec<u8>,
) -> bool {
    if !(conn.is_subscriber()
        && conn.proto == ProtoVersion::Resp2
        && !matches!(
            cmd_upper,
            b"SUBSCRIBE"
                | b"UNSUBSCRIBE"
                | b"PSUBSCRIBE"
                | b"PUNSUBSCRIBE"
                | b"PING"
                | b"QUIT"
                | b"RESET"
        ))
    {
        return false;
    }
    state_rc.borrow_mut().counters.on_command();
    let name = String::from_utf8_lossy(cmd_upper).to_ascii_lowercase();
    encode_into(
        out,
        &ironcache_server::Value::error(ironcache_protocol::ErrorReply::subscribe_mode(&name)),
        conn.proto,
    );
    true
}

/// ROUTE + DISPATCH one decoded request (COORDINATOR.md #107, Stage 1), appending its
/// encoded reply to `out` and returning whether the connection should close (QUIT). Split
/// out of the serve loop so the connection loop stays small; the routing decision is:
///
/// - KEYED (single/multi) command whose key(s) ALL resolve to ONE shard -> that shard:
///   the LOCAL fast path (sync `handle_request`) when it is home, else a single remote HOP
///   ([`coordinator::dispatch_via`]). A key-SPANNING multi-key command stays HOME (the
///   documented Stage 2 fan-out gap).
/// - WHOLE-KEYSPACE (KEYS/SCAN/DBSIZE/FLUSHALL/FLUSHDB/RANDOMKEY) -> SCATTER-GATHER across
///   ALL shards so it covers the WHOLE keyspace (not just the home shard's ~1/N): SCAN is a
///   single-shard-per-call COMPOSITE-cursor walk ([`crate::whole_keyspace::scan_cross_shard`]),
///   the rest broadcast + merge ([`crate::whole_keyspace::fan_out_and_merge`]).
/// - AlwaysHome (control/conn/txn, SWAPDB, unknown) -> HOME (sync `handle_request`).
///
/// With shards == 1 every key is home-owned and the fan-out degenerates to the single local
/// call, so the whole path is byte-identical (no channel) to before this layer.
///
/// The per-connection `commands_processed` is bumped here for the remote / fan-out paths
/// (matching the bump `handle_request` does on the home path), so every command is counted
/// exactly once regardless of route.
///
/// `too_many_lines` is allowed: this is the connection's central ROUTING HUB (the internal-verb
/// gate, the pub/sub interception, the subscribe-mode gate, the in-MULTI/WATCH guards, then the
/// keyed / multikey / spanning / whole-keyspace / home branches), each a documented decision the
/// router must make in one place; splitting it further would scatter the routing contract. The
/// same precedent as `dispatch_inner` / `command_spec::spec_of`.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn route_and_dispatch(
    ctx: &ServerContext,
    conn: &mut ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out: &mut Vec<u8>,
) -> bool {
    let cmd_upper = ascii_upper(request.command());
    let route = route::classify(&cmd_upper);

    // -- INTERNAL-VERB CLIENT GATE (COORDINATOR.md #107, Stage 2b). `__ICSTORESET` /
    // `__ICSTOREZSET` / `__ICSTOREHLL` are the coordinator's INTERNAL cross-shard *STORE
    // dest-write verbs (set / zset / PFMERGE-HLL): each lives in the command registry + has a
    // real dispatch arm (so it routes / admits like any keyed write and the registry-vs-dispatch
    // cross-check stays exact) but must be UNREACHABLE from clients -- only the coordinator
    // issues them (via `dispatch_one_value` / `run_local_keyed`, which call
    // `dispatch_remote_keyed` DIRECTLY and never pass through this router). A CLIENT socket only
    // ever reaches dispatch THROUGH this router, so rejecting them here -- before any routing or
    // queueing -- makes a client `__ICSTORE*` (in or out of MULTI) get the standard
    // unknown-command error while the coordinator's internal path is untouched.
    if cmd_upper == ironcache_server::ICSTORESET
        || cmd_upper == ironcache_server::ICSTOREZSET
        || cmd_upper == ironcache_server::ICSTOREHLL
        // `__ICPUBLISH` is the INTERNAL cross-shard PUBLISH fan-out verb (SERVER_PUSH.md #20, PR
        // 91a): in the registry so the cross-check stays exact, but client-unreachable -- only
        // the coordinator issues it (via the inbox). Reject a CLIENT `__ICPUBLISH` here with the
        // same unknown-command reply as the *STORE verbs.
        || cmd_upper == ironcache_server::ICPUBLISH
    {
        reject_internal_verb(conn, state_rc, request, out);
        return false;
    }

    // -- PUB/SUB SERVE-LAYER INTERCEPTION (SERVER_PUSH.md #20, PR 91a). SUBSCRIBE / UNSUBSCRIBE /
    // PUBLISH (and PING-while-subscribed under RESP2) are handled HERE, before routing/queueing,
    // because registration needs the per-connection push sender + the per-shard subscription
    // table that live in this serve layer (the server crate has no tokio dep). Intercepting
    // BEFORE the in-MULTI gate runs a subscriber's pub/sub commands immediately (they are not
    // queueable). When `try_handle_pubsub` handled the command it returns `Some(close)`; every
    // other command (`None`) falls through to the normal routing + dispatch (where the RESP2
    // subscribe-mode gate rejects disallowed commands). Split out so this router stays small.
    if let Some(close) = try_handle_pubsub(
        conn, home, inbox, push_tx, state_rc, &cmd_upper, request, out,
    )
    .await
    {
        return close;
    }

    // -- SUBSCRIBE-MODE GATE under partitioning (SERVER_PUSH.md #20, PR 91a). A RESP2 subscriber
    // may run ONLY the pub/sub control set + PING/QUIT/RESET; any other command is rejected. The
    // check + reply live in `subscriber_gate_blocks` (kept out of this router so it stays small);
    // it returns true (and has written the error to `out`) when the command is blocked. See that
    // helper for WHY the gate is ALSO here and not only in `dispatch` (a remote keyed hop bypasses
    // the dispatch gate).
    if subscriber_gate_blocks(conn, state_rc, &cmd_upper, out) {
        return false;
    }

    // -- TRANSACTION CORRECTNESS UNDER PARTITIONING (COORDINATOR.md #107, the critical fix).
    //
    // The coordinator routes each command to its key's OWNER shard. But a command issued
    // INSIDE a `MULTI` must be QUEUED (reply `+QUEUED`), not executed: routing it remotely
    // (the dispatch_via / multikey / whole-keyspace branches below) would EXECUTE it eagerly
    // and out of transaction order. The queue gate lives in `dispatch` (the server crate) on
    // the HOME path only, so the remote/fan-out branches bypass it entirely. We close that
    // hole here, BEFORE the routing decision.
    //
    // The KEY INVARIANT we establish: a transaction reaches real (home-only) EXEC ONLY when
    // ALL its watched keys AND all its queued commands' keys are HOME-OWNED, so home
    // execution is always correct. Otherwise we reject it LOUDLY (a transaction is correct,
    // or explicitly aborted -- never silently wrong). True cross-shard transactions (txid +
    // ordered apply) are Stage 3, out of scope here.
    //
    // With `shards == 1` every key is home-owned, so the guards below NEVER fire and the
    // `in_multi -> home path` branch is exactly the pre-coordinator behavior (home dispatch
    // was always the path): byte-identical, and every existing transaction test stays green.

    // (1) QUEUE GATE + (2) CROSS-SHARD-IN-MULTI / WHOLE-KEYSPACE GUARDS. Inside a transaction
    // a command must be QUEUED (or a control verb handled), NEVER routed/executed remotely, and
    // a transaction may reach real (home-only) EXEC ONLY when all its keys are home-owned. That
    // transaction-correctness logic lives in `route_in_multi` (kept out of this router so it
    // stays small); it returns the close flag when it handled the in-MULTI case.
    if conn.in_multi {
        return route_in_multi(
            ctx, conn, home, env, store_rc, wheel_rc, state_rc, &cmd_upper, route, request, out,
        );
    }

    // (3) CROSS-SHARD WATCH GUARD (only when NOT in_multi; WATCH inside MULTI already errors
    // via dispatch's watch_inside_multi path). A `WATCH` of a key owned by a remote shard
    // would snapshot the WRONG (home) store, making the dirty-CAS meaningless. `route::classify`
    // treats WATCH as AlwaysHome and `command_keys` does not extract its keys, so we read
    // WATCH's keys (args[1..]) DIRECTLY here. If any is not home-owned, reply the cross-shard
    // WATCH error and do NOT run WATCH (no snapshot, no conn.watch mutation); the connection is
    // left un-watched so a following MULTI/EXEC works. A WATCH of only home-owned keys (or a
    // malformed/arity-wrong WATCH) falls through to the home dispatch -> cmd_watch unchanged.
    if cmd_upper == b"WATCH"
        && request.args.len() >= 2
        && request.args[1..]
            .iter()
            .any(|k| route::owner_shard(k.as_ref(), home.total) != home.index)
    {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::watch_cross_shard()),
            conn.proto,
        );
        return false;
    }

    // A SHARD-SPANNING KeyedMulti command (its keys land on >1 shard, so `owner_shard_set`
    // is None) that is one of the SIX fan-out-supported commands routes to the multi-key
    // SCATTER-GATHER (COORDINATOR.md #107, Stage 2a). Co-located KeyedMulti (Some(shard))
    // routes via Stage 1 below; any OTHER spanning multi-key command stays on the home sync
    // fall-through (the documented Stage 2b/2c gap), unchanged. We compute this BEFORE the
    // single-target `target` so the two are mutually exclusive (a spanning command has no
    // single owner, so `target` would be None anyway).
    let multikey_fan_out =
        matches!(route, route::CommandClass::KeyedMulti) && is_fan_out_multikey(&cmd_upper) && {
            let spec = route::command_keys(&cmd_upper, request);
            // None from owner_shard_set means EITHER a malformed/short request (keep home,
            // the handler emits the proper error) OR a genuine spanning command. We only
            // fan out when the spec actually has MULTIPLE keys spanning shards; a malformed
            // command (KeySpec::None) must stay home. `command_keys` returns None/One for
            // the degenerate cases, so require Many AND a None owner set (truly spanning).
            matches!(spec, route::KeySpec::Many(_))
                && route::owner_shard_set(&spec, home.total).is_none()
        };

    // A SHARD-SPANNING gather-combine command -- set algebra (SINTER/SUNION/SDIFF/SINTERCARD
    // + the three *STORE), zset algebra (ZUNION/ZINTER/ZDIFF/ZINTERCARD + the three *STORE +
    // ZRANGESTORE), BITOP, or HyperLogLog (PFCOUNT/PFMERGE) -- routes to the GATHER + (shared)
    // COMBINE + STORE path (COORDINATOR.md #107, Stage 2b-1 + 2b-2 + 2b-3). The gate is the
    // SAME shape as `multikey_fan_out`: KeyedMulti, one of the supported tokens, and the keys
    // genuinely SPAN shards (`Many` AND a `None` owner set). Co-located invocations route via
    // Stage 1 below; a malformed/short request stays home (the handler emits the proper
    // error). The two predicates are mutually exclusive (their command sets are disjoint). The
    // remaining spanning multi-key commands (RENAME/COPY/MOVE/SMOVE/LMOVE/RPOPLPUSH moves)
    // stay on the home fall-through (deferred).
    let spanning_set_fan_out = matches!(route, route::CommandClass::KeyedMulti)
        && is_fan_out_spanning_combine(&cmd_upper)
        && {
            let spec = route::command_keys(&cmd_upper, request);
            matches!(spec, route::KeySpec::Many(_))
                && route::owner_shard_set(&spec, home.total).is_none()
        };

    // The routing TARGET shard, if a KEYED command routes to exactly one NON-home shard
    // (else `None` -> the home path). The single-key case keeps the zero-alloc fast path
    // (one hash + compare); only the genuinely multi-key commands pay the `command_keys`
    // walk. WholeKeyspace is NOT a single-target hop (it fans out in its own branch).
    let target = match route {
        route::CommandClass::KeyedSingle => route::single_key(request).and_then(|key| {
            let owner = route::owner_shard(key, home.total);
            (owner != home.index).then_some(owner)
        }),
        route::CommandClass::KeyedMulti => {
            let spec = route::command_keys(&cmd_upper, request);
            route::owner_shard_set(&spec, home.total).filter(|&owner| owner != home.index)
        }
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => None,
    };

    if matches!(route, route::CommandClass::WholeKeyspace) {
        // WHOLE-KEYSPACE SCATTER-GATHER: cover EVERY shard's partition. SCAN walks one shard
        // per call (composite cursor); the rest broadcast + merge on the home core. The home
        // shard's partial runs LOCALLY + synchronously (no self-channel hop). These were
        // never on the single-key hot path, so awaiting here is fine.
        state_rc.borrow_mut().counters.on_command();
        if cmd_upper == b"SCAN" {
            crate::whole_keyspace::scan_cross_shard(
                inbox, ctx, request, conn.db, home.index, out, conn.proto,
            )
            .await;
        } else {
            // RANDOMKEY draws its shard-pick from the home Env RNG seam ONCE (ADR-0003);
            // the other whole-keyspace merges (DBSIZE / KEYS / FLUSHDB / FLUSHALL) ignore
            // it. Gate the draw to RANDOMKEY (FIX 3): drawing unconditionally (for a bare
            // arity-1 DBSIZE / FLUSHALL / FLUSHDB) would PERTURB the per-shard SplitMix64
            // stream that RANDOMKEY / SPOP / *-random eviction read from, breaking ADR-0003
            // replay AND the shards == 1 byte-identical parity (the home path draws 0 for
            // these). Non-RANDOMKEY -> 0, no draw.
            let pick = if cmd_upper == b"RANDOMKEY" {
                crate::whole_keyspace::randomkey_pick(request)
            } else {
                0
            };
            crate::whole_keyspace::fan_out_and_merge(
                inbox, ctx, &cmd_upper, request, conn.db, home.index, pick, out, conn.proto,
            )
            .await;
        }
        false
    } else if multikey_fan_out {
        // SHARD-SPANNING multi-key SCATTER-GATHER (COORDINATOR.md #107, Stage 2a): one of
        // the six (MGET/MSET/DEL/EXISTS/UNLINK/TOUCH) whose keys span shards. The multikey
        // module groups the keys by owner, runs a per-shard sub-request (the home shard's
        // subset LOCALLY + sync, the rest via their drain loops), and reassembles the reply.
        // Bump commands_processed here (matching the home / remote / whole-keyspace paths);
        // the owning shards fold their own data counters.
        state_rc.borrow_mut().counters.on_command();
        crate::multikey::fan_out_multikey(
            inbox, ctx, &cmd_upper, request, conn.db, home, out, conn.proto,
        )
        .await;
        false
    } else if spanning_set_fan_out {
        // SHARD-SPANNING gather-combine (COORDINATOR.md #107, Stage 2b-1/2b-2/2b-3): set /
        // zset algebra, BITOP, or HyperLogLog (PFCOUNT/PFMERGE) whose keys span shards. The
        // spanning_combine module gathers each source from its owner (the home subset LOCALLY
        // + sync, the rest via their drain loops), combines with the PURE combiner shared with
        // the single-shard handler, and for the write forms writes the result to the dest
        // owner. Bump commands_processed here (matching the home / remote / whole-keyspace /
        // multikey paths); the owning shards fold their own data counters. The per-command
        // dispatch is split out so this router stays small.
        state_rc.borrow_mut().counters.on_command();
        dispatch_spanning_combine(ctx, conn, home, inbox, &cmd_upper, request, out).await;
        false
    } else if let Some(target) = target {
        // REMOTE keyed hop: enqueue to the owning shard, await its reply, encode here. The
        // owning shard folded the data counters; here we only attribute commands_processed.
        state_rc.borrow_mut().counters.on_command();
        coordinator::dispatch_via(inbox, target, request, conn.db, out, conn.proto).await;
        false
    } else {
        // HOME path: the SYNC fast path (zero await/channel). Covers the home-owned keyed
        // commands, AlwaysHome, and the key-SPANNING multi-key commands (Stage 2 gap).
        // Pass the ALREADY-uppercased command (FIX 5): we computed `cmd_upper` above for
        // routing, so the home dispatch reuses it instead of re-uppercasing + re-allocating.
        handle_request(
            ctx, conn, env, store_rc, wheel_rc, state_rc, request, &cmd_upper, out,
        )
    }
}

/// Dispatch ONE shard-spanning gather-combine command to its per-command fan-out
/// (COORDINATOR.md #107, Stage 2b), split out of [`route_and_dispatch`] so the router stays
/// small. The caller has already established the command is a supported gather-combine token
/// whose keys SPAN shards (the `spanning_set_fan_out` gate) and bumped `commands_processed`.
///
/// BITOP / PFCOUNT / PFMERGE (Stage 2b-3) each have their OWN parse + combine, so each gets a
/// dedicated fan-out; the eight zset tokens (Stage 2b-2) share `fan_out_zset`; the seven set
/// tokens (Stage 2b-1) share `fan_out_set`. The fan-out gathers each source from its owner
/// (the home subset LOCALLY + sync, the rest via their drain loops), combines with the PURE
/// combiner shared with the single-shard handler, and for the write forms writes the result
/// to the dest owner, encoding the reply into `out`.
async fn dispatch_spanning_combine(
    ctx: &ServerContext,
    conn: &ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    cmd_upper: &[u8],
    request: &Request,
    out: &mut Vec<u8>,
) {
    match cmd_upper {
        b"BITOP" => {
            crate::spanning_combine::fan_out_bitop(
                inbox, ctx, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        b"PFCOUNT" => {
            crate::spanning_combine::fan_out_pfcount(
                inbox, ctx, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        b"PFMERGE" => {
            crate::spanning_combine::fan_out_pfmerge(
                inbox, ctx, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        _ if is_fan_out_spanning_zset(cmd_upper) => {
            crate::spanning_combine::fan_out_zset(
                inbox, ctx, cmd_upper, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
        _ => {
            crate::spanning_combine::fan_out_set(
                inbox, ctx, cmd_upper, request, conn.db, home, out, conn.proto,
            )
            .await;
        }
    }
}

/// The in-MULTI transaction-correctness guards (COORDINATOR.md #107, the critical fix), split
/// out of [`route_and_dispatch`] so the router stays small. Returns the connection-close flag
/// (always `false` here; in-MULTI commands never close).
///
/// A command issued inside a transaction must be QUEUED (reply `+QUEUED`), not executed:
/// routing it remotely (the dispatch_via / multikey / whole-keyspace branches) would EXECUTE it
/// eagerly and out of transaction order, since the queue gate lives in `dispatch` on the HOME
/// path only. So EVERY in-MULTI command goes to the HOME path EXCEPT the two reject-loudly
/// cases below. The KEY INVARIANT: a transaction reaches real (home-only) EXEC ONLY when ALL its
/// watched keys AND all queued commands' keys are HOME-OWNED, so home execution is always
/// correct; otherwise it is rejected LOUDLY (correct, or explicitly aborted -- never silently
/// wrong). True cross-shard transactions (txid + ordered apply) are Stage 3. With `shards == 1`
/// every key is home-owned, so the guards never fire and this is the pre-coordinator behavior.
#[allow(clippy::too_many_arguments)]
fn route_in_multi(
    ctx: &ServerContext,
    conn: &mut ConnState,
    home: ShardId,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    route: route::CommandClass,
    request: &Request,
    out: &mut Vec<u8>,
) -> bool {
    let keyed = matches!(
        route,
        route::CommandClass::KeyedSingle | route::CommandClass::KeyedMulti
    );
    // A KEYED DATA command whose keys are not ALL home-owned is rejected at queue time (Redis's
    // queue-time-error behavior): reply the cross-shard error NOW and dirty the transaction, so
    // EXEC returns -EXECABORT and applies nothing. Bump commands_processed like the other paths.
    if keyed && !all_keys_home_owned(cmd_upper, request, home) {
        state_rc.borrow_mut().counters.on_command();
        conn.dirty_exec = true;
        encode_into(
            out,
            &ironcache_server::Value::error(
                ironcache_protocol::ErrorReply::txn_cross_shard_command(),
            ),
            conn.proto,
        );
        return false;
    }
    // A WHOLE-KEYSPACE command (KEYS/SCAN/DBSIZE/FLUSHALL/FLUSHDB/RANDOMKEY) cannot run correctly
    // home-only at EXEC when the keyspace is partitioned: EXEC replays synchronously on the HOME
    // store, so it would cover only the home shard's ~1/N (a `MULTI; FLUSHALL; EXEC` would
    // partially flush -- silent data RETENTION). There is no single owner to hop to and EXEC
    // cannot fan out (it is synchronous), so reject at queue time (dirty -> -EXECABORT), the same
    // "correct or explicitly aborted, never silently wrong" contract as the cross-shard keyed
    // case. Gate on `home.total > 1`: with one shard the home shard IS the whole keyspace, so
    // they run correctly home-only and must keep working (shards == 1 byte-identical parity).
    if matches!(route, route::CommandClass::WholeKeyspace) && home.total > 1 {
        state_rc.borrow_mut().counters.on_command();
        conn.dirty_exec = true;
        encode_into(
            out,
            &ironcache_server::Value::error(
                ironcache_protocol::ErrorReply::txn_whole_keyspace_unsupported(),
            ),
            conn.proto,
        );
        return false;
    }
    // All-home keyed command OR a control verb: HOME path. `dispatch`'s queue gate queues the
    // keyed command (`+QUEUED`) and runs EXEC/DISCARD/etc. specially. This is the ONLY routing
    // branch taken while in_multi (no remote hop, no fan-out), so a transaction that reaches real
    // EXEC has ALL queued keys home-owned -> home-only EXEC is correct.
    handle_request(
        ctx, conn, env, store_rc, wheel_rc, state_rc, request, cmd_upper, out,
    )
}

/// Dispatch one request and append its encoded reply to `out`. Returns whether
/// the connection should close after flushing (QUIT).
///
/// `env` is the shard's owned-mutable env handle; `store_rc` is the shard's store.
/// The absolute `now` deadline basis is computed ONCE here from the Env wall clock
/// (ADR-0003: the store reads no clock) and passed into dispatch wrapped in
/// [`UnixMillis`]; the data commands convert relative EX/PX against it. Clock reads
/// go through `env.borrow()`; the store is mutated through `store_rc.borrow_mut()`.
#[allow(clippy::too_many_arguments)]
fn handle_request(
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    cmd_upper: &[u8],
    out: &mut Vec<u8>,
) -> bool {
    state_rc.borrow_mut().counters.on_command();
    let snapshot_fn = || state_rc.borrow().counters.snapshot();
    let rollup: &dyn Fn() -> CounterSnapshot = &snapshot_fn;
    // Compute `now` once per command from the shard's wall clock, then run dispatch
    // against the per-shard store. `env` and `store` are SEPARATE RefCells, so the
    // env clock read at the dispatch call site can overlap the held store
    // borrow_mut with no conflict: overlapping borrows of distinct RefCells never
    // alias the same cell.
    let now = UnixMillis(env.borrow().now_unix_millis());
    // The process-global allocator figures for INFO (ADR-0006). One call advances
    // the jemalloc epoch (a mallctl) ONCE and reads allocated + resident from the
    // SAME snapshot, so the two INFO figures are mutually consistent. Read it ONLY
    // for INFO (once, on the shard serving the command) and keep it off every other
    // command's hot path. A process-global figure must NOT be summed across shards;
    // one read on the serving shard is the honest total.
    let mem = if request.command().eq_ignore_ascii_case(b"INFO") {
        let (used_memory, used_memory_rss) = process_memory();
        MemoryInfo {
            used_memory,
            used_memory_rss,
        }
    } else {
        MemoryInfo::default()
    };
    let mut deltas = CounterDeltas::default();
    // The shard's last-seen runtime-config generation (PR-4b), copied OUT of state_rc
    // into a plain local so dispatch can take `&mut` it WITHOUT borrowing state_rc
    // (the rollup closure already captured state_rc immutably for INFO; a held mutable
    // borrow of the same cell would conflict). Dispatch updates the local on a
    // generation-change policy swap; we write it back after dispatch returns.
    let mut shard_generation = state_rc.borrow().last_policy_generation;
    // The lazy-backstop expiry count this command produced (a separate signal from the
    // dispatch deltas): the store accumulates it inside the four primitives, and we
    // drain it after dispatch returns and fold it into `expired_keys` alongside the
    // active-drain count, so both expiry paths feed the INFO counter.
    let lazy_expired;
    let reply = {
        let mut store = store_rc.borrow_mut();
        let mut wheel = wheel_rc.borrow_mut();
        // dispatch now takes `env: &mut E` (clock + RNG, ADR-0003): RANDOMKEY draws a
        // random index through the RNG half, so the env handle must be MUTABLE. `env`
        // is a SEPARATE RefCell from store/wheel, so `env.borrow_mut()` here does not
        // alias the held store/wheel borrows. `now` was already read above from a
        // distinct, now-dropped `env.borrow()`.
        let mut env_ref = env.borrow_mut();
        // Use the cross-shard serve loop's already-computed uppercased command (FIX 5):
        // `dispatch_with_cmd` skips the second `ascii_upper` allocation on this hot path.
        let r = dispatch_with_cmd(
            ctx,
            conn,
            &mut *env_ref,
            &mut *store,
            &mut wheel,
            now,
            &mut shard_generation,
            rollup,
            mem,
            &mut deltas,
            request,
            cmd_upper,
        );
        drop(env_ref);
        lazy_expired = store.take_lazy_expired();
        r
        // The store/wheel borrows end here, BEFORE the counter apply below borrows
        // `state_rc` mutably (the rollup closure captured `state_rc` too, so the two
        // borrows must not overlap; they do not, the dispatch call has returned).
    };
    // Fold this command's dynamic counters into the shard's totals for INFO and write
    // back the (possibly advanced) policy generation. Each is a cheap no-op on the
    // common hot path (no deltas, no generation change).
    {
        deltas.expired += lazy_expired;
        let mut st = state_rc.borrow_mut();
        if deltas != CounterDeltas::default() {
            st.counters.apply(deltas);
        }
        st.last_policy_generation = shard_generation;
    }
    encode_into(out, &reply, conn.proto);
    conn.should_close
}

/// Encode `value` and append the bytes to `out`. PR-1 encodes into a fresh
/// `BytesMut` per reply and appends; pooling is a later optimization behind this
/// same call site (PROTOCOL.md notes zero-copy/pooling sit behind the interface).
fn encode_into(out: &mut Vec<u8>, value: &ironcache_server::Value, proto: ProtoVersion) {
    let mut bm = bytes::BytesMut::with_capacity(64);
    ironcache_protocol::encode(&mut bm, value, proto);
    out.extend_from_slice(&bm);
}

/// Whether EVERY routing key of a KEYED data command (`KeyedSingle`/`KeyedMulti`) is owned
/// by the HOME shard (COORDINATOR.md #107, the in-MULTI cross-shard guard). Used inside a
/// transaction to decide whether a queued command is safe to run home-only at EXEC: only a
/// command whose keys are ALL home-owned may queue (and later EXEC correctly home-only); any
/// key on a remote shard means home-only EXEC would silently lose the write, so the caller
/// rejects + dirties the transaction instead.
///
/// It reuses the SAME key-extraction the router uses ([`route::single_key`] for the single-key
/// fast path, [`route::command_keys`] for multi-key), so "which bytes are keys" cannot drift
/// from the routing decision. A command with NO extractable key (a malformed / short request,
/// `KeySpec::None`) has no remote key, so it is treated as home-owned: it queues and the home
/// handler emits the proper runtime error as the EXEC array element (matching Redis, where a
/// queued command's argument error surfaces at run time, not queue time).
fn all_keys_home_owned(cmd_upper: &[u8], request: &Request, home: ShardId) -> bool {
    let is_home = |key: &[u8]| route::owner_shard(key, home.total) == home.index;
    match route::classify(cmd_upper) {
        route::CommandClass::KeyedSingle => route::single_key(request).is_none_or(is_home),
        route::CommandClass::KeyedMulti => match route::command_keys(cmd_upper, request) {
            route::KeySpec::None => true,
            route::KeySpec::One(k) => is_home(k),
            route::KeySpec::Many(keys) => keys.iter().all(|k| is_home(k)),
        },
        // Only keyed commands reach this helper (the caller gates on `keyed`); a control /
        // whole-keyspace command has no owned key, so treat it as home (it never routes
        // remotely from inside MULTI anyway).
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => true,
    }
}

/// Whether `cmd_upper` is one of the SIX multi-key DATA commands the coordinator fans out
/// across shards when its keys SPAN shards (COORDINATOR.md #107, Stage 2a): MGET, MSET,
/// DEL, EXISTS, UNLINK, TOUCH. Every OTHER spanning multi-key command (SINTER*/SUNION*/
/// SDIFF*/ZUNION*/ZINTER*/ZDIFF*/BITOP/PFCOUNT/PFMERGE spanning, RENAME/RENAMENX/COPY/MOVE/
/// SMOVE/LMOVE/RPOPLPUSH) is DEFERRED (Stage 2b/2c) and stays on the home sync fall-through;
/// MSETNX is DEFERRED to Stage 3 (it needs cross-shard atomicity), so it is NOT here. This
/// list is the single gate the serve loop and [`crate::multikey::fan_out_multikey`]'s match
/// agree on.
fn is_fan_out_multikey(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"MGET" | b"MSET" | b"DEL" | b"EXISTS" | b"UNLINK" | b"TOUCH"
    )
}

/// Reply the standard unknown-command error for a CLIENT-issued INTERNAL verb (the
/// coordinator's `__ICSTORESET`, COORDINATOR.md #107 Stage 2b). The verb is in the command
/// registry so the coordinator's internal path can dispatch it, but a client must never reach
/// it: this renders the SAME `-ERR unknown command ...` reply the dispatch `_ =>` arm renders
/// for a genuinely unknown token (name + leading args, single-quoted), and bumps
/// commands_processed like every other reply path so the rejection still counts.
fn reject_internal_verb(
    conn: &ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    state_rc.borrow_mut().counters.on_command();
    let name = String::from_utf8_lossy(request.command()).into_owned();
    let rest: Vec<&[u8]> = request.args[1..].iter().map(bytes::Bytes::as_ref).collect();
    encode_into(
        out,
        &ironcache_server::Value::error(ironcache_protocol::ErrorReply::unknown_command(
            &name, &rest,
        )),
        conn.proto,
    );
}

// -- Pub/Sub serve-layer handlers (SERVER_PUSH.md #20, PR 91a). These live in the SERVE layer
// (not `dispatch_inner`) because registration needs the per-connection push SENDER (`push_tx`,
// a tokio handle the server crate has no dependency for) and the per-shard subscription table
// (`shard_pubsub()`, a serve thread-local). SUBSCRIBE/UNSUBSCRIBE are HOME-LOCAL (the
// connection's subscriptions live on its home shard); PUBLISH fans out via the coordinator.

/// Intercept and handle the SERVE-LAYER pub/sub commands (SERVER_PUSH.md #20, PR 91a), returning
/// `Some(close)` when `cmd_upper` is one of them (always `false`: a pub/sub command never closes
/// the connection) and `None` when it is NOT a pub/sub command (the caller falls through to the
/// normal routing + dispatch). Split out of [`route_and_dispatch`] so the router stays small.
///
/// `commands_processed` is bumped here for every handled command (matching every other reply
/// path's single count). SUBSCRIBE / PUBLISH validate arity inline (the registry arity, mirroring
/// the dispatch arity path); UNSUBSCRIBE accepts zero channel args (unsubscribe-all). PING is
/// intercepted ONLY when the connection is a RESP2 subscriber (the `["pong", ...]` array shape);
/// a non-subscriber / RESP3 PING returns `None` so the normal `cmd_ping` arm handles it unchanged.
#[allow(clippy::too_many_arguments)]
async fn try_handle_pubsub(
    conn: &mut ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    request: &Request,
    out: &mut Vec<u8>,
) -> Option<bool> {
    match cmd_upper {
        b"SUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            // Arity (>= 2) is the registry's; a bare SUBSCRIBE with no channel is a wrong-arity
            // error, mirroring the dispatch arity path for the other serve-routed commands.
            if request.args.len() >= 2 {
                handle_subscribe(conn, push_tx, request, out);
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "subscribe",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        b"UNSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            handle_unsubscribe(conn, request, out);
            Some(false)
        }
        b"PUBLISH" => {
            state_rc.borrow_mut().counters.on_command();
            if request.args.len() == 3 {
                handle_publish(conn, inbox, home, request, out).await;
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "publish",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        b"PING" if conn.is_subscriber() && conn.proto == ProtoVersion::Resp2 => {
            // PING while subscribed (RESP2): the `["pong", <arg>]` array shape, NOT `+PONG`. Bump
            // commands_processed like the dispatch path would, then encode the array. PING arity
            // is Min(1); a >2-arg PING is a wrong-arity error (Redis), matching `cmd_ping`.
            state_rc.borrow_mut().counters.on_command();
            let reply = if request.args.len() <= 2 {
                ping_subscribed_reply(request)
            } else {
                ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity("ping"))
            };
            encode_into(out, &reply, conn.proto);
            Some(false)
        }
        _ => None,
    }
}

/// Append a `["subscribe", channel, count]` confirmation (one per channel; SERVER_PUSH.md). It
/// is rendered through [`ironcache_protocol::Value::Push`], so the encoder writes RESP3 `>` /
/// RESP2 `*` from the connection proto (ADR-0019), matching Redis's subscribe-confirmation shape.
fn push_confirm(kind: &str, channel: &[u8], count: i64) -> ironcache_server::Value {
    ironcache_server::Value::Push(vec![
        ironcache_server::Value::bulk_str(kind),
        ironcache_server::Value::bulk(bytes::Bytes::copy_from_slice(channel)),
        ironcache_server::Value::Integer(count),
    ])
}

/// The running subscription count for a connection (`channels + patterns`), the integer in
/// each subscribe/unsubscribe confirmation (Redis reports the TOTAL of both, post-mutation).
fn running_count(conn: &ConnState) -> i64 {
    i64::try_from(conn.sub_channels.len() + conn.sub_patterns.len()).unwrap_or(i64::MAX)
}

/// `SUBSCRIBE channel [channel ...]` (SERVER_PUSH.md #20, PR 91a). For EACH channel: insert it
/// into `conn.sub_channels` and register `(channel, conn.id, push_tx.clone())` into THIS shard's
/// subscription table, then append a `["subscribe", channel, running_count]` confirmation. The
/// running count is `sub_channels.len() + sub_patterns.len()` AFTER the insert; a re-subscribe to
/// an already-subscribed channel does NOT bump the count (the `HashSet`/table inserts are
/// idempotent), matching Redis. One confirmation message per channel argument, in order.
fn handle_subscribe(
    conn: &mut ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let pubsub = shard_pubsub();
    for channel in &request.args[1..] {
        conn.sub_channels.insert(channel.clone());
        pubsub
            .borrow_mut()
            .subscribe(channel.clone(), conn.id, push_tx.clone());
        let count = running_count(conn);
        encode_into(
            out,
            &push_confirm("subscribe", channel.as_ref(), count),
            conn.proto,
        );
    }
}

/// `UNSUBSCRIBE [channel ...]` (SERVER_PUSH.md #20, PR 91a). With channel args, unsubscribe each
/// named channel; with NO args, unsubscribe ALL currently-subscribed channels. Reply one
/// `["unsubscribe", channel, running_count]` per AFFECTED channel; the no-args-and-none-subscribed
/// edge replies a single `["unsubscribe", nil, 0]` (matching Redis). Deregister each from THIS
/// shard's subscription table (the connection's subscriptions are home-shard-local).
fn handle_unsubscribe(conn: &mut ConnState, request: &Request, out: &mut Vec<u8>) {
    let pubsub = shard_pubsub();
    // The channels to drop: the named args, or ALL currently-subscribed when none are named.
    let targets: Vec<bytes::Bytes> = if request.args.len() > 1 {
        request.args[1..].to_vec()
    } else {
        conn.sub_channels.iter().cloned().collect()
    };

    if targets.is_empty() {
        // No args AND nothing subscribed: Redis replies a single nil-channel confirmation.
        encode_into(
            out,
            &ironcache_server::Value::Push(vec![
                ironcache_server::Value::bulk_str("unsubscribe"),
                ironcache_server::Value::Null,
                ironcache_server::Value::Integer(0),
            ]),
            conn.proto,
        );
        return;
    }

    for channel in targets {
        conn.sub_channels.remove(&channel);
        pubsub.borrow_mut().unsubscribe(channel.as_ref(), conn.id);
        let count = running_count(conn);
        encode_into(
            out,
            &push_confirm("unsubscribe", channel.as_ref(), count),
            conn.proto,
        );
    }
}

/// `PUBLISH channel payload` (SERVER_PUSH.md #20, PR 91a) -> the total number of receivers across
/// ALL shards. Classic Pub/Sub channels are not slotted, so delivery FANS OUT to every shard's
/// local subscriber table via [`coordinator::fan_out_publish`] (the home shard delivers locally,
/// peers via their drain loops), summing the per-shard counts. Encodes a [`Value::Integer`].
async fn handle_publish(
    conn: &ConnState,
    inbox: &coordinator::Inbox,
    home: ShardId,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let channel = request.args[1].clone();
    let payload = request.args[2].clone();
    let total = coordinator::fan_out_publish(
        inbox,
        channel.as_ref(),
        payload.as_ref(),
        conn.db,
        home.index,
    )
    .await;
    encode_into(out, &ironcache_server::Value::Integer(total), conn.proto);
}

/// The PING reply for a connection in SUBSCRIBE mode under RESP2 (SERVER_PUSH.md #20, PR 91a).
/// Redis replies a 2-element ARRAY `["pong", ""]` (or `["pong", <arg>]`) rather than the usual
/// `+PONG` simple string while subscribed, so a client multiplexing pushes and replies can tell
/// the PONG apart from a pushed message. RESP3 and non-subscriber PING are unchanged (handled by
/// the normal `cmd_ping` dispatch arm). The reply is a plain `Array` (NOT a push frame): Redis
/// sends it as a normal multi-bulk reply.
fn ping_subscribed_reply(request: &Request) -> ironcache_server::Value {
    let second = request
        .args
        .get(1)
        .map_or_else(|| bytes::Bytes::from_static(b""), bytes::Bytes::clone);
    ironcache_server::Value::Array(Some(vec![
        ironcache_server::Value::bulk_str("pong"),
        ironcache_server::Value::bulk(second),
    ]))
}

/// Deregister EVERY subscription a connection holds from THIS shard's subscription table
/// (SERVER_PUSH.md #20, PR 91a), driven off `conn.sub_channels` / `conn.sub_patterns` (O(subs)).
/// Called on connection close (and could be reused on RESET): the connection's subscriptions are
/// home-shard-local, so this runs on the connection's home shard. A no-op when not subscribed.
fn deregister_all_subscriptions(conn: &ConnState) {
    if conn.sub_channels.is_empty() && conn.sub_patterns.is_empty() {
        return;
    }
    let pubsub = shard_pubsub();
    let mut table = pubsub.borrow_mut();
    for channel in &conn.sub_channels {
        table.unsubscribe(channel.as_ref(), conn.id);
    }
    for pattern in &conn.sub_patterns {
        // patterns is empty this pass (PR 91b wires PSUBSCRIBE); the loop is here so the
        // cleanup already covers patterns when they land.
        table.unsubscribe_pattern(pattern.as_ref(), conn.id);
    }
}

/// Whether `cmd_upper` is one of the set-algebra OR sorted-set-algebra commands the
/// coordinator GATHERS + (shared) COMBINES + STOREs across shards when its keys SPAN shards
/// (COORDINATOR.md #107, Stage 2b-1 + 2b-2). This is the single gate the serve loop uses to
/// route to the spanning-combine path; [`is_fan_out_spanning_zset`] then splits the zset
/// subset (dispatched to [`crate::spanning_combine::fan_out_zset`]) from the set subset
/// (dispatched to [`crate::spanning_combine::fan_out_set`]).
///
/// Set forms (Stage 2b-1): SINTER, SUNION, SDIFF, SINTERCARD (read) + SINTERSTORE,
/// SUNIONSTORE, SDIFFSTORE (store). Zset forms (Stage 2b-2): ZUNION, ZINTER, ZDIFF,
/// ZINTERCARD (read) + ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE (store) + ZRANGESTORE (a 2-key
/// copy-range). BITOP + HyperLogLog forms (Stage 2b-3): BITOP (write), PFCOUNT (read),
/// PFMERGE (write). Every OTHER spanning multi-key command (RENAME/COPY/MOVE/SMOVE/LMOVE/
/// RPOPLPUSH) stays on the home sync fall-through (deferred). The command set is DISJOINT
/// from [`is_fan_out_multikey`]'s, so the fan-out branches are mutually exclusive.
fn is_fan_out_spanning_combine(cmd_upper: &[u8]) -> bool {
    is_fan_out_spanning_zset(cmd_upper)
        || matches!(
            cmd_upper,
            b"SINTER"
                | b"SUNION"
                | b"SDIFF"
                | b"SINTERCARD"
                | b"SINTERSTORE"
                | b"SUNIONSTORE"
                | b"SDIFFSTORE"
                | b"BITOP"
                | b"PFCOUNT"
                | b"PFMERGE"
        )
}

/// Whether `cmd_upper` is one of the EIGHT sorted-set-algebra commands the coordinator gathers,
/// (shared) combines, and stores across shards (COORDINATOR.md #107, Stage 2b-2). The read
/// forms are ZUNION, ZINTER, ZDIFF, ZINTERCARD; the store forms are ZUNIONSTORE, ZINTERSTORE,
/// ZDIFFSTORE, and ZRANGESTORE (a 2-key copy-range). This splits the zset subset of
/// [`is_fan_out_spanning_combine`] so the serve loop dispatches it to
/// [`crate::spanning_combine::fan_out_zset`] (the set subset goes to `fan_out_set`).
fn is_fan_out_spanning_zset(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"ZUNION"
            | b"ZINTER"
            | b"ZDIFF"
            | b"ZINTERCARD"
            | b"ZUNIONSTORE"
            | b"ZINTERSTORE"
            | b"ZDIFFSTORE"
            | b"ZRANGESTORE"
    )
}

/// ASCII-uppercase the command token for routing classification (RESP command tokens are
/// ASCII; mirrors the dispatcher's own case-insensitive token handling). The classified
/// token is used ONLY to pick a route; dispatch re-uppercases its own copy. `pub(crate)`
/// so the [`crate::coordinator`] drain loop classifies the same way (keyed vs whole-keyspace).
pub(crate) fn ascii_upper(b: &[u8]) -> Vec<u8> {
    b.iter().map(u8::to_ascii_uppercase).collect()
}

/// Wait for a shutdown signal (SIGINT/SIGTERM) and then stop the shard set.
///
/// Signal handling lives in the binary only (CLI_BINARY.md): the library crates
/// never touch raw signals, preserving the determinism boundary. We use a small
/// blocking wait on a self-pipe-free `libc::sigwait`-style loop via tokio's signal
/// support on the main thread.
pub fn install_shutdown(set: &ShardSet) -> Arc<std::sync::atomic::AtomicBool> {
    set.shutdown_flag()
}

/// Block the calling (main) thread until a termination signal arrives, flipping
/// `flag` so the shard accept loops drain. Uses tokio's signal handling on a
/// small dedicated current-thread runtime.
pub fn wait_for_signal(flag: &Arc<std::sync::atomic::AtomicBool>) {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    rt.block_on(async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let Ok(mut sigint) = signal(SignalKind::interrupt()) else {
                return;
            };
            let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
                return;
            };
            tokio::select! {
                _ = sigint.recv() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    });
    flag.store(true, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_env::{Clock, Env, Rng};
    use ironcache_storage::{ExpireWrite, NewValue, Store};

    /// The per-shard handles the timer-task tests drive (the same Rc<RefCell<..>> set
    /// `spawn_expire_task` / `expire_cycle_tick` consume).
    type TimerFixtures = (
        Rc<RefCell<SystemEnv>>,
        Rc<RefCell<ShardStoreImpl>>,
        Rc<RefCell<TimingWheel>>,
        Rc<RefCell<ShardState>>,
    );

    /// Build a fresh per-shard store + wheel + env + state for the timer-task tests, but
    /// independent of the shard thread-locals so a test can plant entries directly.
    fn timer_fixtures() -> TimerFixtures {
        let env = Rc::new(RefCell::new(SystemEnv::new()));
        let store = Rc::new(RefCell::new(ShardStore::with_hooks(
            16,
            Policy::cache_default(),
            CountingAccounting::new(),
        )));
        let wheel = Rc::new(RefCell::new(TimingWheel::new()));
        let state = Rc::new(RefCell::new(ShardState {
            next_client_id: 1,
            counters: ShardCounters::new(),
            last_policy_generation: 0,
        }));
        (env, store, wheel, state)
    }

    /// Plant a key with a deadline already in the PAST relative to the real wall clock
    /// (deadline 1ms after the Unix epoch), and register it in the wheel, so the next
    /// active-expiry cycle finds it due regardless of the precise SystemEnv `now`.
    fn plant_expired(
        store: &Rc<RefCell<ShardStoreImpl>>,
        wheel: &Rc<RefCell<TimingWheel>>,
        key: &[u8],
    ) {
        let deadline = UnixMillis(1);
        // now=0 so the upsert itself does not lazily reap it before the cycle runs.
        store.borrow_mut().upsert(
            0,
            key,
            NewValue::Bytes(b"v"),
            ExpireWrite::Set(deadline),
            UnixMillis(0),
        );
        wheel.borrow_mut().register(0, key, deadline);
    }

    #[test]
    fn expire_cycle_tick_reaps_expired_and_bumps_counter() {
        // The background cycle FUNCTION (driven directly, deterministically): a key whose
        // deadline is in the past is reaped and folded into the shard's expired_keys
        // counter, with NO command issued (the idle-shard boundedness guarantee).
        let (env, store, wheel, state) = timer_fixtures();
        // Establish the wheel origin in the past so the elapsed-to-now walk retires the
        // entry (the first advance only sets the base).
        wheel.borrow_mut().advance(UnixMillis(0), 0);
        plant_expired(&store, &wheel, b"k1");
        plant_expired(&store, &wheel, b"k2");
        assert_eq!(store.borrow().len(), 2);

        let reaped = expire_cycle_tick(&env, &store, &wheel, &state);
        assert_eq!(
            reaped, 2,
            "the cycle reaped both expired keys with no command"
        );
        assert_eq!(store.borrow().len(), 0, "resident memory bounded when idle");
        assert_eq!(
            state.borrow().counters.snapshot().expired_keys,
            2,
            "the cycle folds reclamation into the shard expired_keys counter"
        );
    }

    #[test]
    fn expire_cycle_tick_is_a_noop_when_nothing_due() {
        // A cycle with nothing due reaps nothing and leaves the counter untouched (the
        // common idle case: an empty wheel fast-forwards in O(1)).
        let (env, store, wheel, state) = timer_fixtures();
        wheel.borrow_mut().advance(UnixMillis(0), 0);
        let reaped = expire_cycle_tick(&env, &store, &wheel, &state);
        assert_eq!(reaped, 0);
        assert_eq!(state.borrow().counters.snapshot().expired_keys, 0);
    }

    #[test]
    fn spawn_expire_task_drains_an_idle_shard_via_the_timer_seam() {
        // Wiring smoke for the SPAWNED async task: run it on a current-thread LocalSet
        // (as a shard does), plant an expired key, and assert the timer task reclaims it
        // with NO command ever issued. This exercises spawn_on_shard + Runtime::timer +
        // the borrow discipline (a held RefCell borrow across the await would panic here
        // because the test thread reborrows the same cells between ticks).
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let (env, store, wheel, state) = timer_fixtures();
            wheel.borrow_mut().advance(UnixMillis(0), 0);
            plant_expired(&store, &wheel, b"idle");
            assert_eq!(store.borrow().len(), 1);

            let runtime = TokioRuntime::new();
            // EXPIRE_TASK_SPAWNED is thread-local; this test thread spawns exactly once.
            spawn_expire_task(
                runtime,
                Rc::clone(&env),
                Rc::clone(&store),
                Rc::clone(&wheel),
                Rc::clone(&state),
            );

            // Drive the LocalSet: the timer task awaits EXPIRE_CYCLE_INTERVAL (100ms) then
            // drains. Yield-sleep past a BOUNDED number of cycles (no wall-clock deadline,
            // so this stays off std::time per the determinism lint). While we sleep we ALSO
            // reborrow the shared cells (as a command handler would), proving the task does
            // not hold a borrow across its await.
            for _ in 0..40 {
                tokio::time::sleep(EXPIRE_CYCLE_INTERVAL).await;
                // Reborrow the cells between the task's awaits: would panic if the task
                // held a borrow across .await.
                if store.borrow().is_empty() {
                    break;
                }
            }
            assert!(
                store.borrow().is_empty(),
                "the background timer task reclaimed the idle shard's expired key"
            );
            assert!(
                state.borrow().counters.snapshot().expired_keys >= 1,
                "idle reclamation folded into expired_keys"
            );
        });
    }

    #[test]
    fn shard_env_rng_is_reachable_as_wired() {
        // Regression for the determinism seam: the shard hands out an
        // owned-mutable env handle (Rc<RefCell<SystemEnv>>), so BOTH halves of the
        // seam are reachable. A bare Rc<SystemEnv> would make `.rng()` (which needs
        // `&mut self`) structurally uncallable. Prove the RNG path works through
        // the borrow, as the per-connection code is wired.
        let env = shard_env();
        // Clock half: reachable via shared borrow.
        let _ = env.borrow().now();
        // RNG half: reachable via mutable borrow. Two draws differ (the stream
        // advances), confirming we hold a live, mutable RNG and not a no-op.
        let mut handle = env.borrow_mut();
        let a = handle.rng().next_u64();
        let b = handle.rng().next_u64();
        assert_ne!(a, b, "RNG stream did not advance through the env handle");
    }

    #[test]
    fn dbsize_flush_do_not_advance_rng_only_randomkey_does() {
        // FIX 3 (deterministic regression guard): the whole-keyspace fan-out's RNG-draw
        // decision -- the EXACT gate `route_and_dispatch` uses -- must draw the home Env
        // RNG ONLY for RANDOMKEY. Drawing for DBSIZE / FLUSHALL / FLUSHDB (all arity-1)
        // would advance the per-shard SplitMix64 stream that RANDOMKEY / SPOP / *-random
        // eviction read from, breaking ADR-0003 replay AND the shards == 1 byte-identical
        // parity (the home path draws 0 for these). We snapshot the thread-local RNG by
        // CLONING it before and after each gate evaluation: if a non-RANDOMKEY command did
        // not draw, the two clones are at the SAME state, so their next draw matches.
        use ironcache_server::Request;

        // The gate, lifted verbatim from `route_and_dispatch` (kept in sync by review): a
        // non-RANDOMKEY whole-keyspace command must yield 0 WITHOUT touching the RNG.
        fn gate_pick(cmd_upper: &[u8], request: &Request) -> u64 {
            if cmd_upper == b"RANDOMKEY" {
                crate::whole_keyspace::randomkey_pick(request)
            } else {
                0
            }
        }

        fn req(parts: &[&[u8]]) -> Request {
            Request {
                args: parts
                    .iter()
                    .map(|p| bytes::Bytes::copy_from_slice(p))
                    .collect(),
            }
        }

        let env = shard_env();

        // Snapshot = a CLONE of the live RNG state (cloning does NOT advance the real
        // stream). Two snapshots taken with NO draw between them are at the same state, so
        // their next draw matches; a draw in between makes the post-snapshot's next draw
        // differ (the stream advanced).
        let snapshot = |env: &Rc<RefCell<SystemEnv>>| -> ironcache_env::SplitMix64 {
            env.borrow_mut().rng().clone()
        };

        // Non-RANDOMKEY arity-1 whole-keyspace commands must NOT draw: the stream stays put.
        for cmd in [b"DBSIZE".as_slice(), b"FLUSHALL", b"FLUSHDB"] {
            let mut before = snapshot(&env);
            let pick = gate_pick(cmd, &req(&[cmd]));
            assert_eq!(
                pick,
                0,
                "{} must yield pick 0",
                String::from_utf8_lossy(cmd)
            );
            let mut after = snapshot(&env);
            assert_eq!(
                before.next_u64(),
                after.next_u64(),
                "{} must NOT advance the RNG stream (FIX 3)",
                String::from_utf8_lossy(cmd)
            );
        }

        // RANDOMKEY (arity 1) MUST draw: the live stream advances, so a snapshot before vs
        // after the gate is at a DIFFERENT state (their next draws differ).
        let mut before = snapshot(&env);
        let _ = gate_pick(b"RANDOMKEY", &req(&[b"RANDOMKEY"]));
        let mut after = snapshot(&env);
        assert_ne!(
            before.next_u64(),
            after.next_u64(),
            "RANDOMKEY MUST advance the RNG stream (the draw the gate exists to gate)"
        );
    }
}
