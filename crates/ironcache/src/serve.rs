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
use ironcache_env::{Clock, SystemEnv};
use ironcache_observe::{CounterSnapshot, MemoryInfo, ServerInfo, ShardCounters};
use ironcache_runtime::bootstrap::{ShardConfig, ShardId, ShardSet};
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{
    ConnState, DecodeOutcome, Limits, ProtoVersion, Request, UnixMillis, decode, dispatch,
};
use ironcache_store::{ShardStore, process_allocated_bytes, process_resident_bytes};
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

    // Static, cheaply-cloned server context shared by value onto each shard. It is
    // immutable, so cloning it per shard does not violate shared-nothing (no
    // mutable cross-core state).
    let ctx_template = ServerContext {
        requirepass: config.requirepass.clone(),
        databases: config.databases,
        info: ServerInfo {
            tcp_port: config.port,
            shards: config.shards,
            pid: std::process::id(),
            // started_at is filled in per shard at boot via the shard's clock so
            // uptime is measured from when the shard's Env started.
            started_at: ironcache_env::Monotonic::ZERO,
            maxmemory: config.maxmemory,
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
    // The shard's per-shard store: the per-DB hashbrown kvobj map (ADR-0005). Held
    // as Rc<RefCell<..>> exactly like ENV, so it is core-local and unsynchronized;
    // created lazily per shard thread. The concrete ShardStore implements the
    // ironcache-storage::Store waist the generic dispatch runs against.
    static STORE: RefCell<Option<Rc<RefCell<ShardStore>>>> = const { RefCell::new(None) };
    // One SystemEnv per shard thread (the sanctioned real-time boundary). It is
    // wrapped in a RefCell so the determinism seam's RNG half is REACHABLE: the
    // shard is single-threaded (current-thread runtime, !Send tasks), so clock
    // reads go through `.borrow()` and `Env::rng` through `.borrow_mut()` with no
    // cross-core synchronization. A bare `Rc<SystemEnv>` would make `.rng()`
    // (which needs `&mut self`) structurally uncallable; PR-2/PR-3 need RNG on the
    // hot path (S3-FIFO sampling, TTL jitter).
    static ENV: RefCell<Option<Rc<RefCell<SystemEnv>>>> = const { RefCell::new(None) };
    static STARTED_AT: RefCell<Option<ironcache_env::Monotonic>> = const { RefCell::new(None) };
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

fn shard_store(databases: u32) -> Rc<RefCell<ShardStore>> {
    STORE.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            *b = Some(Rc::new(RefCell::new(ShardStore::new(databases))));
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
    let store_rc = shard_store(ctx.databases);
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
                        &ctx, &mut conn, &env, &store_rc, &state_rc, &request, &mut out,
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
fn handle_request(
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStore>>,
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
    // The process-global allocator figures for INFO (ADR-0006). They advance the
    // jemalloc epoch (a mallctl call), so read them ONLY for INFO (read once, on the
    // shard serving the command) and keep them off every other command's hot path.
    // A process-global figure must NOT be summed across shards; one read on the
    // serving shard is the honest total.
    let mem = if request.command().eq_ignore_ascii_case(b"INFO") {
        MemoryInfo {
            used_memory: process_allocated_bytes(),
            used_memory_rss: process_resident_bytes(),
        }
    } else {
        MemoryInfo::default()
    };
    let mut store = store_rc.borrow_mut();
    let reply = dispatch(
        ctx,
        conn,
        &*env.borrow(),
        &mut *store,
        now,
        rollup,
        mem,
        request,
    );
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
