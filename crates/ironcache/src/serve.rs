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
use ironcache_env::SystemEnv;
use ironcache_observe::{CounterSnapshot, ServerInfo, ShardCounters};
use ironcache_runtime::bootstrap::{ShardConfig, ShardId, ShardSet};
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, DecodeOutcome, Limits, ProtoVersion, Request, decode, dispatch};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;

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
    // One SystemEnv per shard thread (the sanctioned real-time boundary).
    static ENV: RefCell<Option<Rc<SystemEnv>>> = const { RefCell::new(None) };
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

fn shard_env() -> Rc<SystemEnv> {
    ENV.with(|cell| {
        let mut b = cell.borrow_mut();
        if b.is_none() {
            let env = Rc::new(SystemEnv::new());
            // Record the shard's boot instant for uptime.
            STARTED_AT.with(|s| {
                use ironcache_env::Clock;
                *s.borrow_mut() = Some(env.now());
            });
            *b = Some(env);
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
                    let close =
                        handle_request(&ctx, &mut conn, &env, &state_rc, &request, &mut out);
                    read_buf.drain(..consumed);
                    if close {
                        // Flush the QUIT reply then close.
                        let _ = rt.send(&mut stream, &out).await;
                        break 'conn;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Error(e) => {
                    // Protocol error: write it and close the connection (hardening).
                    encode_into(&mut out, &ironcache_server::Value::Error(e), conn.proto);
                    let _ = rt.send(&mut stream, &out).await;
                    break 'conn;
                }
            }
        }

        if !out.is_empty() && rt.send(&mut stream, &out).await.is_err() {
            break;
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
fn handle_request(
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &SystemEnv,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out: &mut Vec<u8>,
) -> bool {
    state_rc.borrow_mut().counters.on_command();
    let snapshot_fn = || state_rc.borrow().counters.snapshot();
    let rollup: &dyn Fn() -> CounterSnapshot = &snapshot_fn;
    let reply = dispatch(ctx, conn, env, rollup, request);
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
