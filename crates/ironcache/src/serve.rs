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

use ironcache_config::Config;
use ironcache_env::{Clock, Env, Rng, SystemEnv};
use ironcache_eviction::{Policy, map_policy_name};
use ironcache_observe::{CounterSnapshot, MemoryInfo, ServerInfo, ShardCounters};
use ironcache_runtime::bootstrap::{ShardConfig, ShardId, ShardSet};
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{
    ConnState, CounterDeltas, DecodeOutcome, EXPIRE_CYCLE_INTERVAL, Limits, MAX_RECLAIM_PER_CYCLE,
    ProtoVersion, Request, TimingWheel, UnixMillis, decode, dispatch, drain_due_keys,
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
type ShardStoreImpl = ShardStore<Policy, CountingAccounting>;

/// Per-shard, core-local mutable state. Single-threaded access on the shard's
/// thread (no `Send`/`Sync` needed, no locks; shared-nothing ADR-0002).
struct ShardState {
    next_client_id: u64,
    counters: ShardCounters,
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

    // The eviction policy NAME is leaked to a 'static str so INFO/ServerInfo can hold
    // it cheaply for the process lifetime (it never changes in 3a; the CONFIG SET
    // runtime switch is 3c). One small leak at boot, not per request.
    let policy_name: &'static str = Box::leak(config.maxmemory_policy.clone().into_boxed_str());

    // The PER-SHARD byte budget: the maxmemory ceiling split evenly across shards
    // (shared-nothing, ADR-0002). 0 when maxmemory is 0 (unlimited). Computed ONCE
    // here, carried in the context.
    let per_shard_budget = if config.maxmemory == 0 {
        0
    } else {
        (config.maxmemory / config.shards.max(1) as u64).max(1)
    };

    // Static, cheaply-cloned server context shared by value onto each shard. It is
    // immutable, so cloning it per shard does not violate shared-nothing (no
    // mutable cross-core state).
    let ctx_template = ServerContext {
        requirepass: config.requirepass.clone(),
        databases: config.databases,
        maxmemory: config.maxmemory,
        per_shard_budget,
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

    let serve = move |rt: TokioRuntime, stream: tokio::net::TcpStream, shard: ShardId| {
        let ctx = ctx_template.clone();
        async move {
            serve_connection(rt, stream, shard, ctx, default_proto).await;
        }
    };

    let set = ironcache_runtime::bootstrap::run_shards(&shard_cfg, serve)?;
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

fn shard_state() -> Rc<RefCell<ShardState>> {
    SHARD.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(ShardState {
                next_client_id: 1,
                counters: ShardCounters::new(),
            })));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

fn shard_store(databases: u32, policy_name: &str) -> Rc<RefCell<ShardStoreImpl>> {
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
            let store = ShardStore::with_hooks(databases, policy, CountingAccounting::new());
            *b = Some(Rc::new(RefCell::new(store)));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

fn shard_wheel() -> Rc<RefCell<TimingWheel>> {
    WHEEL.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(TimingWheel::new())));
        }
        Rc::clone(b.as_ref().unwrap())
    })
}

fn shard_env() -> Rc<RefCell<SystemEnv>> {
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
    _shard: ShardId,
    mut ctx: ServerContext,
    default_proto: ProtoVersion,
) {
    let env = shard_env();
    let state_rc = shard_state();
    let store_rc = shard_store(ctx.databases, ctx.info.maxmemory_policy);
    let wheel_rc = shard_wheel();
    // Spawn this shard's BACKGROUND active-expiry timer task ONCE (PR-3c, idempotent):
    // it keeps an idle shard's resident memory bounded by draining the wheel on a timer
    // even when no command arrives (EXPIRATION.md). It is spawned here (not at thread
    // boot) because spawn_on_shard needs the shard's running LocalSet, which exists by
    // the time the first connection is being served on this thread.
    //
    // FORWARD-LOOKING: spawning on the first connection means a shard that never
    // receives a connection never starts its drain. Harmless today (no cross-shard key
    // routing exists, so a connectionless shard owns no keys), but when cluster routing
    // lands a data-bearing connectionless shard could accumulate expired memory; at that
    // point spawn this at shard-thread boot or on first key insert instead.
    spawn_expire_task(
        rt,
        Rc::clone(&env),
        Rc::clone(&store_rc),
        Rc::clone(&wheel_rc),
        Rc::clone(&state_rc),
    );
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
                    let close = handle_request(
                        &ctx, &mut conn, &env, &store_rc, &wheel_rc, &state_rc, &request, &mut out,
                    );
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

        // Need more bytes: read.
        let Ok(res) = rt.recv(&mut stream, std::mem::take(&mut read_buf)).await else {
            break;
        };
        read_buf = res.buf;
        if res.n == 0 {
            break; // peer closed
        }
    }

    state_rc.borrow_mut().counters.on_connection_close();
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
    // The lazy-backstop expiry count this command produced (a separate signal from the
    // dispatch deltas): the store accumulates it inside the four primitives, and we
    // drain it after dispatch returns and fold it into `expired_keys` alongside the
    // active-drain count, so both expiry paths feed the INFO counter.
    let lazy_expired;
    let reply = {
        let mut store = store_rc.borrow_mut();
        let mut wheel = wheel_rc.borrow_mut();
        let r = dispatch(
            ctx,
            conn,
            &*env.borrow(),
            &mut *store,
            &mut wheel,
            now,
            rollup,
            mem,
            &mut deltas,
            request,
        );
        lazy_expired = store.take_lazy_expired();
        r
        // The store/wheel borrows end here, BEFORE the counter apply below borrows
        // `state_rc` mutably (the rollup closure captured `state_rc` too, so the two
        // borrows must not overlap; they do not, the dispatch call has returned).
    };
    // Fold this command's dynamic counters into the shard's totals for INFO: the
    // dispatch deltas (eviction / active-drain expiry / keyspace hit-miss) plus the
    // lazy-backstop expiry count. Each is zero on a command that did not trigger it,
    // so this is a cheap no-op on the common hot path.
    {
        deltas.expired += lazy_expired;
        if deltas != CounterDeltas::default() {
            state_rc.borrow_mut().counters.apply(deltas);
        }
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
}
