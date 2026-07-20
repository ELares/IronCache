// SPDX-License-Identifier: MIT OR Apache-2.0
//! The per-connection SERVE LOOPS split out of `serve.rs` (#625): the tokio datapath loop
//! (`serve_connection`), the io_uring datapath loop generic over the backend
//! (`serve_connection_generic`) + its tokio-uring / raw-uring thin wrappers, and the subscribe-mode
//! idle wait (`subscriber_idle_wait` + its io_uring twin + the `subscriber_gate_blocks` gate). Each
//! loop decodes RESP, drives route+dispatch, and writes the reply. Behavior-preserving relocation:
//! the bodies are byte-identical to their former in-`serve.rs` definitions.

use super::{
    BlockPark, ClientRegistryGuard, ConnGateGuard, DeferredHop, ShardState, adopt_metrics_cell,
    adopt_process_memory_gauge, apply_client_tracking, consume_caching_flag,
    deregister_all_subscriptions, drain_deferred_hops, encode_into, ensure_shard_started,
    pause_stall, record_command_stats, record_hotkeys, record_slow_command, route_and_dispatch,
    run_block_park, scan_reserved_bits, shard_env, shard_started_at, shard_state, shard_store,
    shard_wheel,
};
use crate::coordinator;
use bytes::{Buf, Bytes};
use ironcache_env::Clock;
use ironcache_runtime::bootstrap::ShardId;
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, DecodeOutcome, Limits, ProtoVersion, decode_shared};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
// The io_uring serve loop installs/drains the #515 zero-copy GET sink and writes the FIX1 immediate
// blocking reply; those items exist ONLY on the io_uring datapath, so this import is cfg-gated.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
use super::{ZC_SINK, ZcSink, block_timeout_value, drain_zc_sink};

// `too_many_lines` is allowed: this is the per-connection WIRING + read/dispatch/write loop --
// the shard-handle lazy-inits, the per-connection push channel + shed signal (FIX D), the
// pipelined decode/route/flush loop, the subscribe-mode idle wait, and the close-path cleanup
// (subscription deregistration + WATCH deregistration + counter close). Each is a documented step
// the connection lifecycle must run in one place; splitting it would scatter the loop's control
// flow across helpers that all need the same locals.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) async fn serve_connection(
    tcp: tokio::net::TcpStream,
    home: ShardId,
    mut ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
    tls_acceptor: Option<ironcache_runtime::ReloadableAcceptor>,
    persist: Option<Arc<crate::persist::PersistState>>,
    // The per-shard graceful-shutdown flag (#543), the SAME `Arc<AtomicBool>` the acceptor and the
    // drain loop watch. The subscribe-mode idle wait races a SHORT poll of it so a connection PARKED
    // in `subscriber_idle_wait` (a pub/sub subscriber blocked on its push channel, or a CLIENT
    // TRACKING client) observes a graceful stop within one poll interval and CLOSES -- instead of
    // sitting on the `select!` forever and blocking the shard-thread join until the DRAIN_GRACE
    // backstop expires. The non-subscriber hot path never reads it (it closes when the acceptor drops
    // the connection channel on shutdown, exactly as before).
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    // TLS HANDSHAKE (or plaintext passthrough), #105. When TLS is enabled the accepted TCP
    // connection is upgraded RIGHT HERE, before any RESP byte is read: a rustls handshake runs
    // and yields a `ClientStream::Tls` the serve loop reads/writes transparently. A plaintext
    // client to a TLS port FAILS this handshake -> the connection is dropped (rejected, not hung).
    // TCP KEEPALIVE (Area C, Redis `tcp-keepalive`): apply SO_KEEPALIVE with the LIVE keepalive
    // idle interval at ACCEPT, on the raw accepted TcpStream BEFORE the TLS wrap (the option lives
    // on the kernel socket, beneath any TLS layering). Read from the runtime overlay so a
    // `CONFIG SET tcp-keepalive` applies to NEWLY-accepted connections (an established connection
    // keeps the option it was accepted with, matching Redis). `0` disables keepalive. A single cold
    // accept-path call; the helper ignores socket errors (a connection still functions without the
    // probe), and lives in the runtime crate so the socket2 borrow (no `unsafe` here) stays out of
    // this `#![forbid(unsafe_code)]` crate.
    ironcache_runtime::set_keepalive(&tcp, ctx.runtime.tcp_keepalive_secs());
    // When TLS is OFF (the default) we wrap the same TcpStream in `ClientStream::Plain`, a thin
    // passthrough to the identical TcpStream read/write code -- the plaintext hot path is
    // byte-unchanged. The client stream's own recv/send carry the data bytes from here on.
    let mut stream = match tls_acceptor {
        Some(reloadable) => {
            // Read the CURRENTLY published acceptor right before the handshake (#563): a hot cert
            // reload that swapped in a new ServerConfig is picked up by THIS new connection, while
            // any in-flight connection keeps the config it already handshook with. A cheap lock-free
            // ArcSwap load.
            let acceptor = reloadable.acceptor();
            match ironcache_runtime::accept_tls(&acceptor, tcp).await {
                Ok(s) => s,
                Err(_e) => {
                    // A failed handshake (a non-TLS client, an unsupported version, a truncated
                    // ClientHello): close the connection. The bytes that arrived were never RESP and
                    // never reached the engine, so there is nothing to flush -- just return.
                    return;
                }
            }
        }
        None => ironcache_runtime::ClientStream::plain(tcp),
    };
    let env = shard_env();
    // Adopt THIS shard's metrics cell (OBSERVABILITY.md, #152) BEFORE `shard_state()` first builds
    // the `ShardState`, so its `ShardCounters` wrap the registry cell the metrics task reads. The
    // drain-loop boot usually adopts first; this is the idempotent fallback for a connection that
    // races ahead of the drain loop's first poll. A no-op when `/metrics` is disabled.
    adopt_metrics_cell(ctx.metrics_registry.as_ref(), home.index);
    // Adopt THIS shard's reference to the shared process-global allocator-memory gauge (PROD-SAFETY
    // #1/#2) so this shard's expiry tick publishes the live jemalloc figure the admission gate reads.
    adopt_process_memory_gauge(&ctx.process_memory);
    let state_rc = shard_state();
    // The reserved-band width is derived from the configured TOTAL shard count so SCAN's
    // composite cursor is band-aligned when shards > 1 (FIX 1); 0 keeps single-shard SCAN
    // byte-identical.
    let reserved_bits = scan_reserved_bits(ctx.shards);
    let store_rc = shard_store(ctx.databases, ctx.info.maxmemory_policy, reserved_bits);
    // Seed this shard store's collection-encoding thresholds (#40) from the runtime overlay (which
    // was itself seeded from the boot config). The store is built lazily with the COMPILED defaults;
    // this idempotent per-connection field write installs the BOOT-configured thresholds so a node
    // started with a non-default `*-max-listpack-*` (via TOML/env) honors it from the first command
    // (a later `CONFIG SET` then refreshes via the generation-change check in `maybe_hot_swap_policy`).
    store_rc
        .borrow_mut()
        .set_encoding_thresholds(ctx.runtime.encoding_thresholds());
    let wheel_rc = shard_wheel();
    // Ensure this shard's background active-expiry timer is up (PR-3c, idempotent). The
    // canonical spawn point is now SHARD BOOT (the coordinator drain loop calls
    // `ensure_shard_started` before its recv loop, COORDINATOR.md #107: a key-owning shard
    // must reclaim even with no connection). This call is the same idempotent helper, so a
    // connection arriving before the drain loop's first poll still gets the timer started;
    // the EXPIRE_TASK_SPAWNED guard makes the duplicate call a no-op.
    ensure_shard_started(
        ctx.databases,
        ctx.info.maxmemory_policy,
        reserved_bits,
        ctx.runtime.clone(),
    );
    // Correct the context's started_at to this shard's boot instant.
    ctx.info.started_at = shard_started_at();

    // MAXCLIENTS gate (PROD-SAFETY #3, the connection-exhaustion DoS fix). Atomically admit this
    // connection against the process-GLOBAL live-connection count vs the `maxclients` ceiling (read
    // from the runtime overlay, so a `CONFIG SET maxclients` takes effect for new connections). When
    // the node is already AT the cap, reject: write the byte-exact Redis `-ERR max number of clients
    // reached` reply, then close, WITHOUT counting this connection (it was never admitted) and
    // WITHOUT entering the serve loop. `maxclients == 0` disables the cap (the pre-fix behavior).
    // This is a COLD accept-path check (once per connection, never per command). On admit, the
    // returned `ConnGateGuard` (L1) frees the slot on Drop -- on the normal close path, an early
    // return, OR a panic unwinding through the serve loop -- so a panic can never leak the slot.
    if !ctx.conn_gate.try_admit(ctx.runtime.maxclients()) {
        let mut reject = Vec::with_capacity(64);
        encode_into(
            &mut reject,
            &ironcache_server::Value::Error(ironcache_server::ErrorReply::err(
                "max number of clients reached",
            )),
            default_proto,
        );
        let _ = stream.send(reject).await;
        return;
    }
    // Admitted: hold the slot in an RAII guard so it is released on EVERY exit path (the normal
    // close below, any early return, or a panic). The guard lives for the rest of the function.
    let _conn_gate_guard = ConnGateGuard {
        gate: Arc::clone(&ctx.conn_gate),
    };

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

    // Register this connection in the node-level client registry (PROD-7) so CLIENT LIST sees it
    // and CLIENT KILL / CLIENT PAUSE can act on it. `client_handle` is THIS connection's record; the
    // serve loop checks its `is_killed()` flag after each command batch (a cold relaxed load) and
    // closes if set. The RAII guard deregisters on every exit path. A cold accept-path registration,
    // never touched per command (except the post-batch kill/pause check, which is also cold).
    let client_handle = ctx
        .clients
        .register(client_id, addr.clone(), laddr.clone(), 0);
    let _client_registry_guard = ClientRegistryGuard {
        registry: Arc::clone(&ctx.clients),
        id: client_id,
    };

    let mut conn = ConnState::new(client_id, default_proto, ctx.requires_auth(), addr, laddr);

    // The per-connection PUSH channel (SERVER_PUSH.md #20, PR 91a). `push_tx` is the `Send`
    // handle registered into the home shard's subscription table on SUBSCRIBE (so a PUBLISH on
    // any core can hand this connection a message); `push_rx` is owned by THIS serve loop and
    // drained in the subscribe-mode idle-wait below. Bounded for back-pressure: a slow consumer
    // past the bound is shed by the publisher. Created for EVERY connection (cheap), but only
    // WIRED into the select! once the connection enters subscribe mode, so the non-subscriber
    // hot path never touches it.
    let (mut push_tx, mut push_rx) =
        tokio::sync::mpsc::channel::<crate::pubsub::ServerPush>(crate::pubsub::PUSH_CHANNEL_BOUND);
    // The per-connection SHED/kill signal (SERVER_PUSH.md #20, FIX D). When the publisher sheds
    // this connection (its bounded push channel overflowed past the bound), it both drops the
    // table sender AND trips this signal; the subscriber idle-wait observes it and CLOSES the
    // connection. This is necessary because the serve loop holds its OWN `push_tx` clone, so
    // `push_rx.recv()` would NEVER return None on a shed alone -- the signal is the disconnect
    // trigger. Registered into the table alongside `push_tx` on each (P)SUBSCRIBE. Shared
    // cross-core (the publisher runs on any shard), so an `Arc<ShedSignal>` (an atomic latch +
    // a `Notify` for a spin-free wake).
    let mut shed_flag = std::sync::Arc::new(crate::pubsub::ShedSignal::default());

    // The decoder hardening Limits (#138): the `proto-max-bulk-len` ceiling is RUNTIME-SETTABLE, so
    // build the Limits from the live overlay at connection setup (a single cold relaxed load) rather
    // than the compiled default. A `CONFIG SET proto-max-bulk-len` then applies to NEWLY-accepted
    // connections; an established connection keeps the ceiling it was built with (re-reading per
    // decode would add a hot-path load for a knob that effectively never changes mid-connection).
    let limits = Limits {
        max_bulk_len: i64::try_from(ctx.runtime.proto_max_bulk_len()).unwrap_or(i64::MAX),
        ..Limits::default()
    };
    let mut read_buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);
    // CROSS-SHARD HOP OVERLAP (#8): the run of DEFERRED remote hops awaiting assembly. Parked as they
    // are decoded and drained (in order) at the next barrier / end of batch. Always empty between
    // batches (drained before every flush), so it carries no state across the outer loop.
    let mut pending: Vec<DeferredHop> = Vec::new();

    // The IDLE TIMEOUT (PROD-SAFETY #4): a connection that sits idle (no command) longer than
    // `timeout` seconds is CLOSED, so idle connections cannot accumulate. `0` (the Redis default)
    // DISABLES it -- the non-subscriber idle wait then stays a plain `recv` with no timer, the
    // byte-unchanged hot path. The timeout is RUNTIME-SETTABLE (Redis `timeout` is a MODIFIABLE
    // config): the serve loop RE-READS `ctx.runtime.timeout_secs()` at the top of each connection-
    // loop iteration (just below, before the idle wait), so a `CONFIG SET timeout` takes effect LIVE
    // for an already-connected client on its next idle wait -- a non-zero<->0 change switches between
    // the timer-select arm and the plain-recv arm. One relaxed atomic load per (cold, post-batch)
    // iteration. It is measured via the Runtime timer SEAM (NOT wall-clock) and the deadline RE-ARMS
    // on each loop iteration -- i.e. after each command batch is served -- which is the per-command
    // deadline reset (an active connection is never closed). A zero-sized `TokioRuntime` backend
    // supplies the timer (the shard's tasks live on the LocalSet; this carries no state). The
    // OUTPUT-BUFFER cap (PROD-SAFETY #5) is likewise read from the runtime overlay each flush (a
    // `CONFIG SET` takes effect).
    let timer_rt = TokioRuntime::new();

    'conn: loop {
        // Drain every complete request currently buffered (pipelining), building one combined output
        // buffer, then flush once. #510: the batch's bytes are wrapped as a shared `Bytes` and each
        // decoded frame ADVANCES that view (`decode_shared` slices args zero-copy from it), so there
        // is no per-command `read_buf` drain (which would memmove all remaining pipelined bytes to the
        // front each time -- O(P^2) over a depth-P pipeline); the unconsumed tail is restored to
        // `read_buf` once after the batch.
        out.clear();
        // OUTPUT-BUFFER hard cap (PROD-SAFETY #5 / #529): read the live cap ONCE per batch (one cold
        // relaxed load, off the per-command hot path) and enforce it in TWO places -- after each
        // command's reply is appended INSIDE the decode loop below (#529: bound resident output so a
        // single pipelined batch of large-reply commands cannot OOM the host before the post-batch
        // check), and again after the batch is assembled (the original PROD-SAFETY #5 pre-flush
        // check). Reading once per batch matches the documented "takes effect for subsequent batches"
        // semantics (a `CONFIG SET output-buffer-limit` applies from the next batch), so both checks
        // use one consistent value. `0` disables the cap (the pre-fix unbounded behavior).
        let obl = ctx.runtime.output_buffer_limit();
        // #510 ZERO-COPY PARSE: wrap the accumulated read buffer as a shared `Bytes` for the
        // duration of this batch (O(1) move, no copy) so each decoded argument is a refcounted
        // SLICE of it rather than a fresh per-arg heap allocation + memcpy -- the deep-pipeline
        // win. `read_buf` is emptied here; the unconsumed trailing frame is moved back into it
        // after the batch. We `advance` `batch` past each consumed frame in place of a running
        // `consumed_total` cursor (the old per-batch `drain` is likewise replaced below).
        let mut batch = Bytes::from(std::mem::take(&mut read_buf));
        loop {
            match decode_shared(&batch, &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    // SLOWLOG TIMING HOOK (PROD-7). When the SLOWLOG is ENABLED (threshold >= 0) read
                    // the monotonic clock ONCE before dispatch; when DISABLED (-1, the default) this
                    // is a single relaxed atomic load + branch and NOTHING else runs (no clock read,
                    // no ring touch), so the hot path is byte-unchanged. The threshold is read from
                    // the runtime overlay (the canonical CONFIG source); `ctx.slowlog` mirrors it for
                    // the hot-path copy, but reading the overlay directly here keeps a single source.
                    let slow_threshold = ctx.slowlog.log_slower_than_micros();
                    // COMMANDSTATS timing (#413): capture the monotonic start ALWAYS (one read),
                    // SHARED with the SLOWLOG hook below so the slowlog-enabled path adds no start
                    // read. The end is read once after dispatch to record this command's usec.
                    let cmd_start = env.borrow().now();
                    // The offset where THIS command's reply will be appended, so the commandstats
                    // hook can tell an error reply (leading `-`) from a success without re-parsing.
                    // `mut`: the cross-shard-hop overlap (#8) may re-base this when a run of pending
                    // hop replies is spliced in ahead of a barrier command's output (see below).
                    let mut out_before = out.len();
                    // CLIENT TRACKING (#409): snapshot whether this connection was tracking / in
                    // BCAST mode BEFORE dispatch, so the hook can detect the ON->OFF / RESET and the
                    // BCAST enter/leave transitions (purge or register prefixes accordingly).
                    let was_tracking = conn.tracking_on;
                    let was_bcast = conn.tracking_bcast;
                    // Route + dispatch one decoded request (COORDINATOR.md #107, Stage 1),
                    // appending its encoded reply to `out`; returns whether to close (QUIT).
                    // Factored out of the serve loop so the connection loop stays small.
                    // `block_request` (PROD-9) is set by the blocking-command interception when the
                    // command must PARK; it stays `None` for every non-blocking command (the hot
                    // path), so this is a single stack `Option` write per command.
                    let mut block_request: Option<BlockPark> = None;
                    // CROSS-SHARD HOP OVERLAP (#8): route_and_dispatch sets this (the tokio loop opts
                    // IN below) when the command is a single-key REMOTE hop it ENQUEUED but did not
                    // await -- we park it in `pending` and drain the run at the next barrier / end of
                    // batch, instead of blocking here on the cross-thread round-trip (which serialized
                    // N pipelined hops into 2N wakeups and made 2 shards slower than 1).
                    let mut deferred_hop = coordinator::HopOutcome::NotHop;
                    // CLIENT PAUSE (#388, write-aware): hold this command HERE while a pause that
                    // applies to it is active -- an ALL pause holds every command, a WRITE pause
                    // holds only writes (reads + admin like SAVE proceed). This is the single point
                    // both kinds are honored, so it is correct under pipelining and holds the very
                    // next command after a pause begins. Default (no pause): a single relaxed atomic
                    // load returns immediately, so the hot path is byte-unchanged (no clock read, no
                    // classify).
                    pause_stall(&ctx, &conn, &request, &env, &timer_rt, &client_handle).await;
                    let close = route_and_dispatch(
                        &ctx,
                        &mut conn,
                        home,
                        &inbox,
                        &mut push_tx,
                        &mut push_rx,
                        &mut shed_flag,
                        &env,
                        &store_rc,
                        &wheel_rc,
                        &state_rc,
                        persist.as_ref(),
                        &request,
                        &mut out,
                        &mut block_request,
                        // #8: opt IN to hop deferral -- this is the default tokio serve loop.
                        true,
                        &mut deferred_hop,
                    )
                    .await;
                    // #8 OVERLAP: a DEFERRED remote hop -- park it and keep decoding, so the next
                    // command's hop is issued while this owner works. `out` is UNTOUCHED (the reply is
                    // encoded later, in order), so FIFO on the wire is preserved. A hop is never a
                    // blocking command and never closes the connection, so we skip the hooks + park +
                    // close handling and go straight to the next command; its hooks run at drain time.
                    if let coordinator::HopOutcome::Deferred(target) = deferred_hop {
                        pending.push(DeferredHop {
                            target,
                            db: conn.db,
                            request,
                            cmd_start,
                            was_tracking,
                            was_bcast,
                            slow_threshold,
                            proto: conn.proto,
                        });
                        batch.advance(consumed);
                        continue;
                    }
                    // BARRIER (any synchronous / control / home / fan-out command): if a run of hops is
                    // pending, splice their replies into `out` BEFORE this command's already-encoded
                    // output (FIFO), running each hop's deferred hooks in order. `split_off` lifts this
                    // command's bytes aside; after draining the run we re-append them and re-base
                    // `out_before` so this command's own hooks read the right reply slice.
                    if !pending.is_empty() {
                        let barrier_bytes = out.split_off(out_before);
                        drain_deferred_hops(
                            &mut pending,
                            &mut out,
                            &ctx,
                            &mut conn,
                            &env,
                            &state_rc,
                            &push_tx,
                            &shed_flag,
                            &inbox,
                        )
                        .await;
                        out_before = out.len();
                        out.extend_from_slice(&barrier_bytes);
                    }
                    // SLOWLOG record (PROD-7): only reached when the SLOWLOG was enabled at the start
                    // of this command. Measure elapsed micros through the SAME Env clock seam
                    // (ADR-0003), and if it met the threshold, push the command (args + this
                    // connection's addr/name) into the node-level ring and feed the LATENCY
                    // `command` event. This whole block is skipped entirely when SLOWLOG is disabled.
                    // COMMANDSTATS / ERRORSTATS (#413): record this command's elapsed micros +
                    // outcome on the serving shard (ALWAYS, the call/usec/failed tally INFO reads).
                    // The end clock read is shared with the slowlog hook (same `cmd_start`).
                    let cmd_elapsed_us = u64::try_from(
                        env.borrow()
                            .now()
                            .saturating_duration_since(cmd_start)
                            .as_micros(),
                    )
                    .unwrap_or(u64::MAX);
                    record_command_stats(&state_rc, &request, out_before, &out, cmd_elapsed_us);
                    // HOTKEYS (#428): attribute this command's CPU micros + net bytes to its keys
                    // when a tracking session is active. The gate is ONE relaxed atomic load, so the
                    // default (no session) path -- and the perf-gate -- never reach the recorder.
                    if ctx.hotkeys.is_active() {
                        let reply_bytes =
                            u64::try_from(out.len().saturating_sub(out_before)).unwrap_or(u64::MAX);
                        record_hotkeys(&ctx, &env, &request, cmd_elapsed_us, reply_bytes);
                    }
                    // CLIENT TRACKING (#409): register this command's read keys for a tracking
                    // connection, or invalidate a write's keys for every tracking client. The
                    // common no-tracking path is one cheap gate inside (see `apply_client_tracking`).
                    apply_client_tracking(
                        &conn,
                        &push_tx,
                        &shed_flag,
                        &request,
                        was_tracking,
                        was_bcast,
                    );
                    // OPTIN/OPTOUT (#409 stage 3): consume the one-shot CLIENT CACHING flag now that
                    // this command's track decision has used it (cleared except on CLIENT CACHING).
                    consume_caching_flag(&mut conn, &request);
                    if slow_threshold >= 0 {
                        record_slow_command(&ctx, &env, &conn, &request, cmd_start, slow_threshold);
                    }
                    batch.advance(consumed);
                    // -- BLOCKING PARK (PROD-9). The blocking-command interception in
                    // `route_and_dispatch` set `block_request`: the command's non-blocking attempt
                    // found every key empty (or WAIT's quorum not yet met), so PARK this connection
                    // here, where the serve loop owns the stream (to observe a peer close), the
                    // runtime timer (the timeout), and the read buffer (to keep pipelined bytes). The
                    // park loop FLUSHES any pending pipelined replies in `out` FIRST (FIFO), then
                    // registers a per-shard FIFO waiter and `select!`s on (the wake / the timeout /
                    // a peer close), re-attempting the pop on a wake. It returns the close flag.
                    // `block_request == None` on the hot path (every non-blocking command), so this
                    // is a single `is_some` check then skipped.
                    if let Some(park) = block_request {
                        // #510: `batch` was already advanced past the blocking command (above), so it
                        // now holds exactly the UNprocessed tail. Move that tail into `read_buf` so
                        // `run_block_park` -- which owns `read_buf` during the park and re-decodes
                        // bytes that arrive -- starts at the next unprocessed byte. `read_buf` was
                        // emptied when `batch` was formed, so this restores it. (`batch` itself is
                        // re-formed from `read_buf` after the park returns, below.)
                        read_buf.extend_from_slice(&batch);
                        // Flush the pipelined replies that preceded the blocking command, so a
                        // blocked client still receives the earlier commands' replies before it
                        // parks (FIFO, never a blocking command holding up prior replies).
                        if !out.is_empty() {
                            let sent = out.len();
                            match stream.send(std::mem::take(&mut out)).await {
                                Ok(returned) => {
                                    out = returned;
                                    // #527: net output for the pre-park pipelined flush.
                                    state_rc.borrow().counters.on_net_output(sent as u64);
                                }
                                Err(_) => break 'conn,
                            }
                        }
                        // #661: count this connection in the node-wide `blocked_clients` gauge for
                        // the ENTIRE park. The guard clones the shard counters cell (no borrow held
                        // across the await) and decrements on Drop, so a wake, a timeout, or a peer
                        // close all clear the count -- INFO `blocked_clients` reflects the live
                        // parked set leak-free, on BOTH the pop-park and WAIT-park paths inside.
                        let _blocked = state_rc.borrow().counters.block_guard();
                        let park_close = run_block_park(
                            &mut stream,
                            &timer_rt,
                            &ctx,
                            &conn,
                            &client_handle,
                            &env,
                            &store_rc,
                            &inbox,
                            home,
                            &mut read_buf,
                            &mut out,
                            park,
                        )
                        .await;
                        if park_close {
                            break 'conn;
                        }
                        // The park completed (a pop succeeded, or the timeout / quorum reply was
                        // sent). `out` was flushed inside the park loop. `run_block_park` owned
                        // `read_buf` and left the still-unprocessed bytes in it, so re-form `batch`
                        // from it (#510) and continue the decode loop to process whatever arrived.
                        batch = Bytes::from(std::mem::take(&mut read_buf));
                        continue;
                    }
                    if close {
                        // Flush the QUIT reply then close. send returns the owned
                        // buffer (owned-buffer model); we are closing, so the
                        // returned buffer is dropped rather than reclaimed. Sent over the
                        // CLIENT stream (plain or TLS); the plain arm is byte-identical to the
                        // prior `rt.send` (it calls the same TcpStream write_all), #105.
                        let sent = out.len();
                        if stream.send(std::mem::take(&mut out)).await.is_ok() {
                            // #527: net output for the QUIT reply.
                            state_rc.borrow().counters.on_net_output(sent as u64);
                        }
                        break 'conn;
                    }
                    // OUTPUT-BUFFER intra-batch cap (#529): this command's reply is now appended to
                    // `out`. If the accumulated reply buffer has grown past the cap MID-BATCH, stop
                    // decoding more commands and fall through to close -- the post-batch drain + the
                    // pre-flush check below then close the connection. This bounds resident output so
                    // a single pipelined batch of large-reply commands cannot drive unbounded server
                    // memory before the (post-batch) check runs. At the default cap (1 GiB) / `0`
                    // (disabled) this is a single compare against a local that is never true on the
                    // hot path, so the branch is predicted not-taken and the batch decodes as before.
                    if obl > 0 && out.len() as u64 > obl {
                        break;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Error(e) => {
                    // Protocol error: write it and close the connection (hardening). #8: FIRST drain
                    // any pending cross-shard hops so the replies for the VALID commands that preceded
                    // the malformed frame still go out (in order) before the error + close -- otherwise
                    // the overlap would silently drop them (the inline path flushed them).
                    if !pending.is_empty() {
                        drain_deferred_hops(
                            &mut pending,
                            &mut out,
                            &ctx,
                            &mut conn,
                            &env,
                            &state_rc,
                            &push_tx,
                            &shed_flag,
                            &inbox,
                        )
                        .await;
                    }
                    encode_into(&mut out, &ironcache_server::Value::Error(e), conn.proto);
                    let sent = out.len();
                    if stream.send(std::mem::take(&mut out)).await.is_ok() {
                        // #527: net output for the protocol-error reply before close.
                        state_rc.borrow().counters.on_net_output(sent as u64);
                    }
                    break 'conn;
                }
            }
        }

        // CROSS-SHARD HOP OVERLAP (#8): the batch ended (decode Incomplete) with a run of remote hops
        // still pending (no trailing barrier drained them). Assemble their replies into `out` NOW, in
        // order, BEFORE the flush -- so every reply for this batch is on the wire in command order.
        if !pending.is_empty() {
            drain_deferred_hops(
                &mut pending,
                &mut out,
                &ctx,
                &mut conn,
                &env,
                &state_rc,
                &push_tx,
                &shed_flag,
                &inbox,
            )
            .await;
        }

        // #510: `batch` now holds exactly the unconsumed trailing bytes (a partial frame, or empty).
        // Move them back into `read_buf` for the next recv to append to. `read_buf` was emptied when
        // `batch` was formed at the top of this batch (or by the block-park refresh above), so this
        // restores it to just the carry-over -- the same buffer state the old post-batch drain left.
        read_buf.extend_from_slice(&batch);

        // OUTPUT-BUFFER hard cap (PROD-SAFETY #5): before flushing, if the pending reply buffer has
        // grown past the configured `output_buffer_limit`, CLOSE the connection rather than let a
        // slow consumer / a huge reply / a pipelined flood drive unbounded server memory. `0`
        // disables the cap (the pre-fix unbounded behavior); the default is a high ceiling so a
        // legitimate large reply / deep pipeline is never affected. Uses the `obl` read once at the
        // top of this batch (the intra-batch check #529 shares it), so a `CONFIG SET
        // output-buffer-limit` takes effect for subsequent batches. We drop the oversized buffer
        // unsent and close (matching Redis closing a client over the limit).
        if obl > 0 && out.len() as u64 > obl {
            break;
        }

        if !out.is_empty() {
            // Owned-buffer send: hand `out` over and take the returned buffer back. Over the
            // client stream (plain or TLS); the plain arm is the same TcpStream write the prior
            // `rt.send` did (#105).
            let sent = out.len();
            match stream.send(std::mem::take(&mut out)).await {
                Ok(returned) => {
                    out = returned;
                    // #527: count the reply bytes written to the client socket (net output). ONE
                    // relaxed atomic add through this shard's counters, summed node-wide for INFO.
                    state_rc.borrow().counters.on_net_output(sent as u64);
                }
                Err(_) => break,
            }
        }

        // CLIENT KILL (PROD-7): if a peer's `CLIENT KILL` flagged THIS connection, close it now
        // (after its in-flight batch's reply was flushed above). A single relaxed atomic load on a
        // cold post-batch path; the default (never killed) is byte-unchanged.
        if client_handle.is_killed() {
            break;
        }

        // CLIENT PAUSE (PROD-7, #388): the pause stall is now PER-COMMAND, done in the decode loop
        // above (`pause_stall`) right before each command is dispatched, so a WRITE pause holds only
        // writes (reads + admin like SAVE proceed) and an ALL pause holds every command -- and the
        // VERY NEXT command after a pause begins is held (a post-batch stall here would only hold the
        // command AFTER the current batch was already served). The default (no pause) cost is a single
        // relaxed atomic load per command at that gate; there is no post-batch pause work.

        // The LIVE idle timeout (PROD-SAFETY #4): re-read the runtime overlay (one relaxed atomic
        // load) so a `CONFIG SET timeout` takes effect for this already-connected client on THIS
        // idle wait. `0` (the default) is `None` -> the plain-recv arm below (byte-unchanged hot
        // path); a non-zero value is `Some(Duration)` -> the timer-select arm. Because this is
        // recomputed each iteration, a runtime non-zero<->0 change correctly switches arms.
        let idle_timeout: Option<core::time::Duration> = {
            let secs = ctx.runtime.timeout_secs();
            if secs == 0 {
                None
            } else {
                Some(core::time::Duration::from_secs(secs))
            }
        };

        // IDLE WAIT. The NON-subscriber path (the common, hot path) is BYTE-IDENTICAL to before
        // pub/sub when no idle timeout is configured: just await `rt.recv`, no select! overhead.
        // Only a connection in SUBSCRIBE mode pays for the select! that ALSO drains the push channel
        // (`subscriber_idle_wait`). FIFO ordering holds because `out` was already flushed above
        // before we reach this idle wait, so a push is rendered and sent only AFTER the in-flight
        // command batch's reply went out -- a push never precedes a command reply on the connection
        // (SERVER_PUSH.md FIFO). A CLIENT TRACKING connection (#409) drains the push channel the
        // SAME way (its `invalidate` pushes ride the same channel), without entering subscribe MODE
        // (the RESP2 command gate stays `is_subscriber()`-only, so a tracking client runs any command).
        if conn.is_subscriber() || conn.tracking_on {
            if subscriber_idle_wait(
                &mut stream,
                &mut push_rx,
                &shed_flag,
                &mut read_buf,
                &mut out,
                conn.proto,
                // #543: race a short poll of the shard shutdown flag so a parked subscriber closes
                // promptly on a graceful stop. `timer_rt` is the same Runtime timer seam the idle-
                // timeout arm uses (ADR-0003: no wall-clock on the decision path).
                &shutdown,
                &timer_rt,
            )
            .await
            {
                break;
            }
        } else if let Some(timeout) = idle_timeout {
            // IDLE-TIMEOUT path (PROD-SAFETY #4): race the read against the Runtime timer seam. The
            // deadline is fresh on each iteration (re-armed after the command batch above), so an
            // active connection never trips it; only a connection idle for `timeout` seconds with no
            // new bytes does, and is then closed. The read is into a FRESH buffer and the new bytes
            // are APPENDED to `read_buf` (NOT moved into the recv): had the timer won, a `read_buf`
            // moved into the cancelled recv future would be dropped along with any partial frame it
            // held; reading into a temporary keeps `read_buf`'s partial bytes safe across
            // cancellation (the same pattern the subscriber idle-wait uses).
            tokio::select! {
                res = stream.recv(Vec::new()) => {
                    let Ok(res) = res else { break; };
                    if res.n == 0 {
                        break; // peer closed
                    }
                    read_buf.extend_from_slice(&res.buf[..res.n]);
                    // #527: count the command bytes read off the client socket (net input).
                    state_rc.borrow().counters.on_net_input(res.n as u64);
                }
                () = timer_rt.timer(timeout) => {
                    // Idle past the timeout with no new command bytes: close the connection
                    // (PROD-SAFETY #4). `read_buf` (with any buffered partial frame) is simply
                    // dropped on the close path below.
                    break;
                }
            }
        } else {
            // Need more bytes: read over the client stream (plain or TLS). The plain arm is the
            // same TcpStream read the prior `rt.recv` did, so the plaintext hot path is unchanged
            // (no idle timeout configured -> no timer, byte-identical to before PROD-SAFETY #4).
            let Ok(res) = stream.recv(std::mem::take(&mut read_buf)).await else {
                break;
            };
            read_buf = res.buf;
            if res.n == 0 {
                break; // peer closed
            }
            // #527: count the command bytes read off the client socket (net input).
            state_rc.borrow().counters.on_net_input(res.n as u64);
        }

        // QUERY-BUFFER hard cap (#528): every idle-wait arm above (subscriber drain / idle-timeout
        // race / plain recv) APPENDS the newly-arrived bytes into `read_buf`, then this loop
        // re-decodes. If the accumulated inbound query buffer has grown past the configured
        // `query_buffer_limit`, CLOSE the connection rather than let a client that announces a large
        // multibulk (`*<huge>\r\n`) and then DRIBBLES the elements force unbounded pre-auth inbound
        // buffering (a memory-amplification DoS: the frame never completes, so decode keeps returning
        // Incomplete and `read_buf` grows without bound). A single cold relaxed load on this
        // post-batch / idle path (never per command); `0` disables the cap (Redis parity, the pre-fix
        // unbounded behavior). The oversized buffer is dropped and the connection closed.
        let qbl = ctx.runtime.query_buffer_limit();
        if qbl > 0 && read_buf.len() as u64 > qbl {
            break;
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
    // This connection's slot in the process-GLOBAL connection gate (PROD-SAFETY #3) is released by
    // the `_conn_gate_guard` RAII guard on Drop (L1) as this function returns, keeping the live count
    // accurate so the `maxclients` cap is enforced against a true figure over the node's lifetime.
    // The guard also covers any earlier return / panic, which the prior bare `release()` here did
    // not (a panic would have leaked the slot permanently).
}

/// The per-connection serve loop for the OPTIONAL io_uring datapath (PROD-10 / #28,
/// docs/design/IOURING_DATAPATH.md). Compiled ONLY on a Linux build with the `io_uring` feature;
/// reached ONLY when `runtime = io_uring` is selected at boot AND TLS is off (the selection branch
/// in [`run_server_observed`] falls back to the tokio [`serve_connection`] otherwise).
///
/// It drives the SAME engine the tokio path does: it REUSES [`route_and_dispatch`] (the route +
/// dispatch core is stream-agnostic -- it works on a decoded `Request` and an output `Vec<u8>`),
/// the same per-shard setup helpers ([`shard_env`] / [`shard_state`] / [`shard_store`] /
/// [`shard_wheel`] / [`ensure_shard_started`]), the same `maxclients` admission gate + RAII
/// guards, the same client-registry registration, and the same RESP `decode`. The ONLY difference
/// is the transport: bytes flow through the io_uring [`ironcache_runtime::Runtime`] owned-buffer
/// `recv`/`send` (one ring per shard) instead of a tokio `TcpStream` / `ClientStream`. The pure
/// command engine (the determinism seam, the store, the coordinator) is UNTOUCHED.
///
/// ## v1 scope and the io_uring-path behavior (honest, documented)
///
/// This v1 serves the CORE request/reply RESP datapath (pipelined decode -> dispatch -> single
/// coalesced flush), which is the bulk of cache traffic and the path the io_uring throughput lever
/// targets. The serve-loop features that the tokio path drives via `tokio::select!` over the stream
/// behave as follows on the io_uring loop (each is SAFE -- a defined reply, no hang, no silent drop,
/// close-on-shed):
///   * PUB/SUB (SERVER_PUSH.md) -- FULLY WIRED. A subscriber's idle wait runs
///     [`subscriber_idle_wait_uring`], which `select!`s the per-connection push channel alongside
///     the io_uring `recv`, so a PUSH-while-idle is delivered PROMPTLY (not silently dropped, not
///     deferred to the next command). Queued pushes coalesce into one write; the SHED kill-signal
///     is observed in the select! AND re-checked at the post-batch top of the loop, so a flooded
///     subscriber is CLOSED rather than accumulating forever.
///   * BLOCKING (PROD-9, BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP/WAIT) --
///     IMMEDIATE-REPLY (NOT a true park). The non-blocking attempt runs in `route_and_dispatch`; if
///     it would PARK (every key empty / WAIT quorum not yet met), the io_uring loop writes the
///     command's IMMEDIATE non-blocking reply instead of leaving `out` empty: the BLPOP-family pops
///     reply the nil-array (the zero-timeout result), WAIT replies the CURRENT in-sync replica
///     count. So a blocking command on this path RETURNS AT ONCE and never hangs. LIMITATION: true
///     block-until-data (parking past the first attempt) is NOT yet supported on the io_uring path
///     (it needs a `select!` over the io_uring `recv` future whose cancel-on-drop semantics are
///     unvalidated here); that is a documented Linux follow-up. The tokio path's `run_block_park`
///     remains the full implementation.
///   * The IDLE-TIMEOUT (PROD-SAFETY #4) timer race is deferred; the `maxclients` cap + peer-close
///     detection still bound connections.
///
/// CLIENT PAUSE (PROD-7) IS enforced: a pause window stalls this connection's command processing
/// (the same conservative-superset stall the tokio loop applies, re-checking via the Runtime timer
/// seam so UNPAUSE / CLIENT KILL take effect promptly).
///
/// The registered-buffer / multishot fast path (IOURING_DATAPATH.md) and a perf benchmark are
/// deferred to a Linux soak; no throughput claim is made here.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
/// The io_uring serve loop, GENERIC over the [`ironcache_runtime::Runtime`] backend so BOTH the
/// tokio-uring (`IoUringRuntime`) and the raw (`RawIoUringRuntime`) backends share ONE implementation
/// (#682 P2). The backend-specific per-connection setup -- peer/local address + `SO_KEEPALIVE`, which
/// need a borrowed-fd `unsafe` dance done in the runtime crate -- is performed by the thin wrappers
/// `serve_connection_uring` / `serve_connection_raw_uring` and handed in as `addr`/`laddr`. Both
/// backends fix `Buf = Vec<u8>`, so the loop's owned buffers are concrete `Vec<u8>`.
async fn serve_connection_generic<R>(
    rt: R,
    mut stream: R::Stream,
    home: ShardId,
    mut ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
    persist: Option<Arc<crate::persist::PersistState>>,
    addr: String,
    laddr: String,
    // The per-shard graceful-shutdown flag (#543), mirrored from the tokio serve path: the
    // subscribe-mode idle wait races a short poll of it so a PARKED io_uring subscriber closes
    // promptly on a graceful stop instead of blocking the shard-thread join until the drain grace.
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) where
    R: ironcache_runtime::BatchedRecvSend,
{
    // No `use` of the traits is needed: `rt`'s methods (`recv_batch`/`send_batch`, and the `Runtime`
    // supertrait's `send`/`timer`) resolve through the generic `R: BatchedRecvSend` bound directly.

    let env = shard_env();
    adopt_metrics_cell(ctx.metrics_registry.as_ref(), home.index);
    adopt_process_memory_gauge(&ctx.process_memory);
    let state_rc = shard_state();
    let reserved_bits = scan_reserved_bits(ctx.shards);
    let store_rc = shard_store(ctx.databases, ctx.info.maxmemory_policy, reserved_bits);
    // Seed the encoding thresholds (#40) from the runtime overlay, same as the tokio serve path.
    store_rc
        .borrow_mut()
        .set_encoding_thresholds(ctx.runtime.encoding_thresholds());
    let wheel_rc = shard_wheel();
    ensure_shard_started(
        ctx.databases,
        ctx.info.maxmemory_policy,
        reserved_bits,
        ctx.runtime.clone(),
    );
    ctx.info.started_at = shard_started_at();

    // MAXCLIENTS gate (PROD-SAFETY #3), identical to the tokio path: admit against the global
    // live-connection count vs the `maxclients` ceiling, reject (byte-exact reply) + close when at
    // the cap, hold the slot in a Drop-release guard otherwise.
    if !ctx.conn_gate.try_admit(ctx.runtime.maxclients()) {
        let mut reject = Vec::with_capacity(64);
        encode_into(
            &mut reject,
            &ironcache_server::Value::Error(ironcache_server::ErrorReply::err(
                "max number of clients reached",
            )),
            default_proto,
        );
        let _ = rt.send(&mut stream, reject).await;
        return;
    }
    let _conn_gate_guard = ConnGateGuard {
        gate: Arc::clone(&ctx.conn_gate),
    };

    // `addr`/`laddr` (peer/local for CLIENT INFO) and the SO_KEEPALIVE application are done by the
    // backend-specific wrapper before this generic loop is entered -- they need a borrowed-fd
    // `unsafe` dance on the concrete stream type, which lives in the runtime crate.

    let client_id = {
        let mut s = state_rc.borrow_mut();
        let id = s.next_client_id;
        s.next_client_id += 1;
        s.counters.on_connection_open();
        id
    };
    let client_handle = ctx
        .clients
        .register(client_id, addr.clone(), laddr.clone(), 0);
    let _client_registry_guard = ClientRegistryGuard {
        registry: Arc::clone(&ctx.clients),
        id: client_id,
    };

    let mut conn = ConnState::new(client_id, default_proto, ctx.requires_auth(), addr, laddr);

    // The per-connection PUSH channel + SHED signal are still created (route_and_dispatch needs
    // the `push_tx`/`push_rx`/`shed_flag` handles for SUBSCRIBE bookkeeping), but the push-while-
    // idle drain is deferred on this path (see the fn doc): pushes are delivered on the next
    // command round-trip rather than mid-idle.
    let (mut push_tx, mut push_rx) =
        tokio::sync::mpsc::channel::<crate::pubsub::ServerPush>(crate::pubsub::PUSH_CHANNEL_BOUND);
    let mut shed_flag = std::sync::Arc::new(crate::pubsub::ShedSignal::default());

    // Build the decoder Limits from the live `proto-max-bulk-len` overlay (Area B), same as the
    // tokio serve path: a single cold load at connection setup; the value applies to new connections.
    let limits = Limits {
        max_bulk_len: i64::try_from(ctx.runtime.proto_max_bulk_len()).unwrap_or(i64::MAX),
        ..Limits::default()
    };
    let mut read_buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);
    // ZERO-COPY GET splice list (#515 P3b, INERT until P4): the ordered `ZcInsert`s (a pinned stored
    // value + its offset in `out`) to interleave into `out` at flush, so a large present value is
    // written store->socket with no copy into `out`. EMPTY in P3b -- nothing pushes yet -- so every
    // flush is `send_zc(out, [])`, exactly a `send_batch(out)` (byte-identical). Always drained (taken)
    // at every flush, so it carries no state across the outer loop. P4's `get_home_by_ref` pushes here.
    let mut zc_inserts: Vec<ironcache_runtime::ZcInsert> = Vec::new();
    // The zero-copy PINS (#515 P4): the frozen-slot handles (store `ZcPin`s, type-erased) that keep
    // each `zc_inserts` region alive until the send's CQE -- the io_uring send OWNS them, so even a
    // cancelled send reads valid memory. Drained (taken) with `zc_inserts` at every flush; EMPTY in
    // P4b (nothing pins yet), so the flush is byte-identical. P4c's `get_home_by_ref` pushes here.
    let mut zc_pins: Vec<Box<dyn core::any::Any>> = Vec::new();
    // #515 P4c: INSTALL this shard thread's zero-copy GET sink (once per thread -- idempotent), so
    // `get_home_by_ref` splices a large String hit's value straight from the store instead of copying
    // it into `out`. Installed ONLY here (the io_uring serve loop); the tokio loop never installs one,
    // so `get_home_by_ref` there always copies. Left installed for the thread's life -- every command
    // drains it back to empty (see `drain_zc_sink` / `ZC_SINK`), so sharing it across the connections
    // multiplexed on this shard thread is sound.
    ZC_SINK.with(|c| {
        let mut g = c.borrow_mut();
        if g.is_none() {
            *g = Some(ZcSink::default());
        }
    });
    // CROSS-SHARD HOP OVERLAP (#8 / #514): the run of DEFERRED remote hops awaiting assembly, mirrored
    // from the tokio serve loop. Parked as they are decoded and drained (in order) at the next barrier /
    // error / end of batch, so a run of pipelined remote-key ops overlaps the owner's work instead of
    // serializing on each cross-thread round-trip. Always empty between batches (drained before every
    // flush), so it carries no state across the outer loop.
    let mut pending: Vec<DeferredHop> = Vec::new();
    // The Runtime timer seam for the CLIENT PAUSE stall (FIX3). Under `tokio_uring::start` the
    // canonical tokio time driver is enabled (the io_uring backend's `timer` is `tokio::time::sleep`),
    // so the SAME zero-sized `TokioRuntime` timer the tokio serve loop uses drives the pause poll
    // here too -- no io_uring timeout op needed for the timer abstraction.
    let timer_rt = TokioRuntime::new();

    'conn: loop {
        out.clear();
        // OUTPUT-BUFFER hard cap (PROD-SAFETY #5 / #529), identical to the tokio path: read the live
        // cap ONCE per batch and enforce it both INSIDE the decode loop after each command's reply is
        // appended (#529: bound resident output so one pipelined batch of large-reply commands cannot
        // OOM the host) and again pre-flush after the batch is assembled. `0` disables the cap.
        let obl = ctx.runtime.output_buffer_limit();
        // #510 ZERO-COPY PARSE (mirrors the tokio loop): wrap the accumulated read buffer as a
        // shared `Bytes` for this batch (O(1) move) so each decoded argument is a refcounted SLICE
        // of it, not a per-arg heap allocation + memcpy. `read_buf` is emptied here and the
        // unconsumed trailing frame is moved back into it after the batch; we `advance` `batch` per
        // consumed frame in place of a running cursor (and replace the old post-batch `drain`).
        let mut batch = Bytes::from(std::mem::take(&mut read_buf));
        loop {
            match decode_shared(&batch, &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    let slow_threshold = ctx.slowlog.log_slower_than_micros();
                    // COMMANDSTATS timing (#413): capture the monotonic start ALWAYS (one read),
                    // SHARED with the SLOWLOG hook below so the slowlog-enabled path adds no start
                    // read. The end is read once after dispatch to record this command's usec.
                    let cmd_start = env.borrow().now();
                    // The offset where THIS command's reply will be appended, so the commandstats
                    // hook can tell an error reply (leading `-`) from a success without re-parsing.
                    // `mut`: the cross-shard-hop overlap (#8 / #514) re-bases this when a run of pending
                    // hop replies is spliced in ahead of a barrier command's output (see below).
                    let mut out_before = out.len();
                    // CLIENT TRACKING (#409): snapshot whether this connection was tracking / in
                    // BCAST mode BEFORE dispatch, so the hook can detect the ON->OFF / RESET and the
                    // BCAST enter/leave transitions (purge or register prefixes accordingly).
                    let was_tracking = conn.tracking_on;
                    let was_bcast = conn.tracking_bcast;
                    // PROD-9 blocking-park (FIX1): a blocking command (BLPOP/.../WAIT) whose
                    // non-blocking attempt found no data sets `block_request` and leaves `out`
                    // EMPTY, expecting the OWNING serve loop to PARK (the tokio loop runs
                    // `run_block_park`). The io_uring loop does NOT support a true park yet, so we
                    // write the IMMEDIATE non-blocking reply below instead of leaving `out` empty
                    // (which would HANG the client and desync later replies). See the fn doc: true
                    // blocking is a documented io_uring-path limitation; the reply is the same one
                    // the command would have produced on a zero timeout.
                    let mut block_request: Option<BlockPark> = None;
                    // #8 / #514: the io_uring loop opts IN to hop deferral, mirroring the tokio serve
                    // loop -- route_and_dispatch ENQUEUES a single-key remote hop and sets this to
                    // `Deferred(rx)` without awaiting, so a run of pipelined hops overlaps the owner's
                    // work; we park it in `pending` and drain the run in order at the next barrier.
                    let mut deferred_hop = coordinator::HopOutcome::NotHop;
                    // CLIENT PAUSE (#388, write-aware), identical to the tokio path: hold this
                    // command while a pause that applies to it is active -- an ALL pause holds
                    // everything, a WRITE pause holds only writes (reads + admin like SAVE proceed).
                    // Default (no pause): one relaxed atomic load returns immediately -- the
                    // byte-unchanged hot path.
                    pause_stall(&ctx, &conn, &request, &env, &timer_rt, &client_handle).await;
                    let close = route_and_dispatch(
                        &ctx,
                        &mut conn,
                        home,
                        &inbox,
                        &mut push_tx,
                        &mut push_rx,
                        &mut shed_flag,
                        &env,
                        &store_rc,
                        &wheel_rc,
                        &state_rc,
                        persist.as_ref(),
                        &request,
                        &mut out,
                        &mut block_request,
                        // #8 / #514: io_uring loop opts IN to hop deferral (mirrors the tokio loop).
                        true,
                        &mut deferred_hop,
                    )
                    .await;
                    // #8 / #514 OVERLAP: a DEFERRED remote hop -- park it and keep decoding, so the next
                    // command's hop is issued while this owner works. `out` is UNTOUCHED (the reply is
                    // encoded later, in order), so FIFO on the wire is preserved. A hop is never a
                    // blocking command and never closes the connection, so we skip the hooks + FIX1 +
                    // close handling and go straight to the next command; its hooks run at drain time.
                    if let coordinator::HopOutcome::Deferred(target) = deferred_hop {
                        pending.push(DeferredHop {
                            target,
                            db: conn.db,
                            request,
                            cmd_start,
                            was_tracking,
                            was_bcast,
                            slow_threshold,
                            proto: conn.proto,
                        });
                        batch.advance(consumed);
                        continue;
                    }
                    // #515 P4c: DRAIN this command's zero-copy splices (a large home-GET hit may have
                    // pinned its value + pushed a splice) into the flush lists. This runs with NO
                    // `.await` since `route_and_dispatch` returned, so the sink holds exactly THIS
                    // command's splices and no other multiplexed connection can have raced it. Capture
                    // the pre-drain length so the barrier below can re-base only this command's offsets.
                    let zc_added_from = zc_inserts.len();
                    drain_zc_sink(&mut zc_inserts, &mut zc_pins);
                    // BARRIER (any synchronous / control / home / fan-out / blocking command): if a run of
                    // hops is pending, splice their replies into `out` BEFORE this command's already-
                    // encoded output (FIFO), running each hop's deferred hooks in order. `split_off` lifts
                    // this command's bytes aside; after draining the run we re-append them and re-base
                    // `out_before` so this command's own hooks read the right reply slice. A blocking
                    // command left `out` empty here (FIX1 writes its immediate reply below, AFTER this
                    // drain), so the drained hop replies still precede it on the wire.
                    if !pending.is_empty() {
                        let barrier_bytes = out.split_off(out_before);
                        drain_deferred_hops(
                            &mut pending,
                            &mut out,
                            &ctx,
                            &mut conn,
                            &env,
                            &state_rc,
                            &push_tx,
                            &shed_flag,
                            &inbox,
                        )
                        .await;
                        // #515 P4c: the hop replies were spliced BEFORE this command's bytes, shifting
                        // this command's region forward by exactly the bytes inserted ahead of it. Its
                        // zero-copy splice offsets (`ZcInsert::at`, absolute into `out`) must move by the
                        // same delta; offsets from EARLIER commands sit before `out_before` and are
                        // untouched by the `split_off`, so only `[zc_added_from..]` is re-based.
                        let zc_shift = out.len() - out_before;
                        out_before = out.len();
                        out.extend_from_slice(&barrier_bytes);
                        for zi in &mut zc_inserts[zc_added_from..] {
                            zi.at += zc_shift;
                        }
                    }
                    // COMMANDSTATS / ERRORSTATS (#413): record this command's elapsed micros +
                    // outcome on the serving shard (ALWAYS, the call/usec/failed tally INFO reads).
                    // The end clock read is shared with the slowlog hook (same `cmd_start`).
                    let cmd_elapsed_us = u64::try_from(
                        env.borrow()
                            .now()
                            .saturating_duration_since(cmd_start)
                            .as_micros(),
                    )
                    .unwrap_or(u64::MAX);
                    record_command_stats(&state_rc, &request, out_before, &out, cmd_elapsed_us);
                    // HOTKEYS (#428): attribute this command's CPU micros + net bytes to its keys
                    // when a tracking session is active. The gate is ONE relaxed atomic load, so the
                    // default (no session) path -- and the perf-gate -- never reach the recorder.
                    if ctx.hotkeys.is_active() {
                        let reply_bytes =
                            u64::try_from(out.len().saturating_sub(out_before)).unwrap_or(u64::MAX);
                        record_hotkeys(&ctx, &env, &request, cmd_elapsed_us, reply_bytes);
                    }
                    // CLIENT TRACKING (#409): register this command's read keys for a tracking
                    // connection, or invalidate a write's keys for every tracking client. The
                    // common no-tracking path is one cheap gate inside (see `apply_client_tracking`).
                    apply_client_tracking(
                        &conn,
                        &push_tx,
                        &shed_flag,
                        &request,
                        was_tracking,
                        was_bcast,
                    );
                    // OPTIN/OPTOUT (#409 stage 3): consume the one-shot CLIENT CACHING flag now that
                    // this command's track decision has used it (cleared except on CLIENT CACHING).
                    consume_caching_flag(&mut conn, &request);
                    if slow_threshold >= 0 {
                        record_slow_command(&ctx, &env, &conn, &request, cmd_start, slow_threshold);
                    }
                    batch.advance(consumed);
                    // FIX1: the blocking command parked (`out` is empty). Write its IMMEDIATE
                    // non-blocking reply so it returns at once rather than hanging: the BLPOP-family
                    // pops get the nil-array timeout reply; WAIT gets the CURRENT in-sync replica
                    // count. The command counter was already bumped inside `handle_blocking_live` /
                    // `handle_wait_live` when the park was set up, so we ONLY encode here (no double
                    // count). `block_request == None` for every non-blocking command (the hot path),
                    // so this is a single `is_some` check then skipped.
                    if let Some(park) = block_request {
                        let reply = match park.spec.op {
                            ironcache_server::BlockOp::Wait { .. } => {
                                ironcache_server::Value::Integer(
                                    ironcache_server::in_sync_replica_count(&ctx),
                                )
                            }
                            _ => block_timeout_value(),
                        };
                        encode_into(&mut out, &reply, conn.proto);
                    }
                    if close {
                        // #515: the flush SPLICES the pinned zero-copy value regions (`zc_inserts`)
                        // into the wire stream straight from the store, so the bytes actually SENT are
                        // `out` PLUS those pinned regions -- count both for the net-output stat. Empty
                        // `zc_inserts` (no large GET in this batch) makes the flush byte-identical to a
                        // plain `send_batch(out)`. The pins (`zc_pins`) are OWNED by `send_zc` until its
                        // CQE, so the pinned store bytes stay valid for the kernel read even if the send
                        // is cancelled (the #576-COW keeps the frozen value immutable meanwhile).
                        let sent = out.len() + zc_inserts.iter().map(|i| i.len).sum::<usize>();
                        let flushed_ok = rt
                            .send_zc(
                                &mut stream,
                                std::mem::take(&mut out),
                                std::mem::take(&mut zc_inserts),
                                std::mem::take(&mut zc_pins),
                            )
                            .await
                            .is_ok();
                        if flushed_ok {
                            // #527: net output for the io_uring QUIT reply.
                            state_rc.borrow().counters.on_net_output(sent as u64);
                        }
                        break 'conn;
                    }
                    // OUTPUT-BUFFER intra-batch cap (#529), identical to the tokio path: this
                    // command's reply is now in `out`; if the accumulated reply buffer has grown past
                    // the cap MID-BATCH, stop decoding and fall through to the post-batch drain +
                    // pre-flush check, which close the connection. Bounds resident output so a single
                    // pipelined batch of large-reply commands cannot OOM the host. At the default cap
                    // / `0` (disabled) this is a single never-taken compare on the hot path.
                    if obl > 0 && out.len() as u64 > obl {
                        break;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Error(e) => {
                    // Protocol error: write it and close the connection (hardening). #8 / #514: FIRST
                    // drain any pending cross-shard hops so the replies for the VALID commands that
                    // preceded the malformed frame still go out (in order) before the error + close --
                    // otherwise the overlap would silently drop them (the inline path flushed them).
                    if !pending.is_empty() {
                        drain_deferred_hops(
                            &mut pending,
                            &mut out,
                            &ctx,
                            &mut conn,
                            &env,
                            &state_rc,
                            &push_tx,
                            &shed_flag,
                            &inbox,
                        )
                        .await;
                    }
                    encode_into(&mut out, &ironcache_server::Value::Error(e), conn.proto);
                    // #515: bytes SENT = `out` plus the spliced pinned regions (see the QUIT flush
                    // above). `zc_pins` is owned by `send_zc` until its CQE.
                    let sent = out.len() + zc_inserts.iter().map(|i| i.len).sum::<usize>();
                    let flushed_ok = rt
                        .send_zc(
                            &mut stream,
                            std::mem::take(&mut out),
                            std::mem::take(&mut zc_inserts),
                            std::mem::take(&mut zc_pins),
                        )
                        .await
                        .is_ok();
                    if flushed_ok {
                        // #527: net output for the io_uring protocol-error reply before close.
                        state_rc.borrow().counters.on_net_output(sent as u64);
                    }
                    break 'conn;
                }
            }
        }

        // CROSS-SHARD HOP OVERLAP (#8 / #514): the batch ended (decode Incomplete) with a run of remote
        // hops still pending (no trailing barrier drained them). Assemble their replies into `out` NOW,
        // in order, BEFORE the flush -- so every reply for this batch is on the wire in command order.
        if !pending.is_empty() {
            drain_deferred_hops(
                &mut pending,
                &mut out,
                &ctx,
                &mut conn,
                &env,
                &state_rc,
                &push_tx,
                &shed_flag,
                &inbox,
            )
            .await;
        }

        // #510: `batch` now holds exactly the unconsumed trailing bytes (a partial frame, or empty);
        // move them back into `read_buf` for the flush + subscriber-idle / recv paths below (which
        // own `read_buf` and append to it). `read_buf` was emptied when `batch` was formed, so this
        // restores just the carry-over -- the same buffer state the old post-batch drain left.
        read_buf.extend_from_slice(&batch);

        // OUTPUT-BUFFER hard cap (PROD-SAFETY #5), identical to the tokio path: the pre-flush check,
        // using the `obl` read once at the top of this batch (shared with the intra-batch check).
        if obl > 0 && out.len() as u64 > obl {
            break;
        }

        if !out.is_empty() {
            // OneShotFixed WRITE tier (#284): stage the reply through this shard's REGISTERED
            // fixed buffer when one is free and the reply fits, else the owned send. Byte-identical
            // output; hands the buffer back for reuse exactly like the owned `rt.send` did.
            // #515: bytes SENT = `out` plus the spliced pinned zero-copy value regions (`zc_inserts`),
            // which are NOT copied into `out` -- count both for the net-output stat. Empty `zc_inserts`
            // (no large GET this batch) makes the flush byte-identical to a plain `send_batch(out)`. A
            // large GET always writes its `$len\r\n..\r\n` framing into `out`, so `out` is non-empty
            // whenever `zc_inserts` is (this guard never skips a pending splice). The pins (`zc_pins`)
            // are OWNED by `send_zc` until its CQE.
            let sent = out.len() + zc_inserts.iter().map(|i| i.len).sum::<usize>();
            let flush_result = rt
                .send_zc(
                    &mut stream,
                    std::mem::take(&mut out),
                    std::mem::take(&mut zc_inserts),
                    std::mem::take(&mut zc_pins),
                )
                .await;
            match flush_result {
                Ok(returned) => {
                    out = returned;
                    // #527: net output for the io_uring batch flush (one relaxed atomic add).
                    state_rc.borrow().counters.on_net_output(sent as u64);
                }
                Err(_) => break,
            }
        }

        // CLIENT KILL (PROD-7): close if a peer flagged this connection (cold relaxed load). FIX2:
        // also close if the publisher SHED this connection (its bounded push channel overflowed):
        // a flooded subscriber must be torn down rather than accumulate forever. For a
        // non-subscriber `shed_flag` is never tripped (no table sender registered), so this is a
        // single relaxed atomic load on the cold post-batch path.
        if client_handle.is_killed() || shed_flag.is_tripped() {
            break;
        }

        // CLIENT PAUSE (PROD-7, FIX3, #388): the pause stall is now PER-COMMAND, done in the decode
        // loop above (`pause_stall`) right before each command is dispatched, identical to the tokio
        // serve loop -- a WRITE pause holds only writes (reads + admin like SAVE proceed), an ALL
        // pause holds every command, and the very next command after a pause begins is held. There is
        // no post-batch pause work; the default (no pause) cost is the single relaxed atomic load per
        // command at that gate.

        // IDLE WAIT. The NON-subscriber path is the plain io_uring `recv` (byte-unchanged from
        // before this fix). A connection in SUBSCRIBE mode instead takes the `select!`-based
        // `subscriber_idle_wait_uring` (FIX2), which delivers a PUSH-while-idle promptly (it selects
        // on the push channel alongside the recv), coalesces queued pushes into one write, observes
        // the SHED kill-signal, and detects a peer close -- so pub/sub messages are no longer
        // silently dropped on the io_uring path. FIFO holds because `out` was flushed above before
        // this idle wait, so a push is rendered + sent only AFTER the in-flight command batch's
        // reply went out.
        if conn.is_subscriber() || conn.tracking_on {
            if subscriber_idle_wait_generic(
                &rt,
                &mut stream,
                &mut push_rx,
                &shed_flag,
                &mut read_buf,
                &mut out,
                conn.proto,
                // #543: race a short poll of the shard shutdown flag so a parked subscriber closes
                // promptly on a graceful stop. `rt` supplies the timer seam (its `timer` is the
                // canonical tokio time driver under `tokio_uring::start`), ADR-0003-clean.
                &shutdown,
            )
            .await
            {
                break;
            }
        } else {
            // Read the next command batch. `recv_batch` uses this shard's REGISTERED fixed-buffer
            // datapath when the kernel selected it (the startup probe, #495/#496), else the owned
            // recv seam -- both APPEND into `read_buf`, preserving any partial-frame carryover, so
            // this is behavior-preserving for the pipelining model. A clean EOF (`n == 0`) or any
            // error closes the connection.
            let Ok(n) = rt.recv_batch(&mut stream, &mut read_buf).await else {
                break;
            };
            if n == 0 {
                break; // peer closed
            }
            // #527: count the command bytes read off the client socket (net input), io_uring path.
            state_rc.borrow().counters.on_net_input(n as u64);
        }

        // QUERY-BUFFER hard cap (#528), identical to the tokio path: every idle-wait arm above (the
        // subscriber drain / the plain recv_batch) APPENDS the newly-arrived bytes into `read_buf`,
        // then this loop re-decodes. If the accumulated inbound query buffer exceeds the configured
        // `query_buffer_limit`, CLOSE the connection rather than let a slow-dribble multibulk force
        // unbounded pre-auth inbound buffering. A single cold relaxed load on this post-batch / idle
        // path; `0` disables the cap. The oversized buffer is dropped and the connection closed.
        let qbl = ctx.runtime.query_buffer_limit();
        if qbl > 0 && read_buf.len() as u64 > qbl {
            break;
        }
    }

    // Connection close: same shard-local cleanup the tokio path performs (subscription table,
    // dropped push_tx, watch deregistration, connection-close counter). The conn-gate + registry
    // guards release on Drop as this function returns.
    deregister_all_subscriptions(&conn);
    drop(push_tx);
    if !conn.watch.is_empty() {
        use ironcache_storage::Watch;
        store_rc.borrow_mut().unwatch(&conn.watch);
        conn.clear_watch();
    }
    state_rc.borrow_mut().counters.on_connection_close();
}

/// Thin TOKIO-URING wrapper: does the concrete-`UringTcpStream` per-connection setup (peer/local
/// address for CLIENT INFO + SO_KEEPALIVE, both via the runtime crate's borrowed-fd helpers so the
/// `unsafe` stays out of this crate), then runs the shared [`serve_connection_generic`] loop.
#[cfg(all(target_os = "linux", feature = "io_uring"))]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_connection_uring(
    rt: ironcache_runtime::IoUringRuntime,
    stream: ironcache_runtime::UringTcpStream,
    home: ShardId,
    ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
    persist: Option<Arc<crate::persist::PersistState>>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    let (addr, laddr) = ironcache_runtime::peer_local_addrs(&stream);
    ironcache_runtime::set_keepalive_uring(&stream, ctx.runtime.tcp_keepalive_secs());
    serve_connection_generic(
        rt,
        stream,
        home,
        ctx,
        default_proto,
        inbox,
        persist,
        addr,
        laddr,
        shutdown,
    )
    .await;
}

/// Thin RAW io_uring wrapper (#682 P2): the same per-connection setup as `serve_connection_uring`
/// but on the raw backend's concrete `RawUringTcpStream`, then the SAME shared generic serve loop.
#[cfg(all(target_os = "linux", feature = "io_uring_raw"))]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_connection_raw_uring(
    rt: ironcache_runtime::RawIoUringRuntime,
    stream: ironcache_runtime::RawUringTcpStream,
    home: ShardId,
    ctx: ServerContext,
    default_proto: ProtoVersion,
    inbox: coordinator::Inbox,
    persist: Option<Arc<crate::persist::PersistState>>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    let (addr, laddr) = ironcache_runtime::peer_local_addrs_raw(&stream);
    ironcache_runtime::set_keepalive_raw(&stream, ctx.runtime.tcp_keepalive_secs());
    serve_connection_generic(
        rt,
        stream,
        home,
        ctx,
        default_proto,
        inbox,
        persist,
        addr,
        laddr,
        shutdown,
    )
    .await;
}

/// How often a subscribe-mode idle wait re-checks the shared shutdown flag (#543). A subscriber
/// parked on the `select!` of (push recv, shed, socket recv) has no arm a graceful stop wakes, so
/// it races this SHORT timer (the Runtime timer SEAM, ADR-0003 -- NOT wall-clock) to observe the
/// flag within one interval and close. Chosen well UNDER the shutdown-latency budget (the shard
/// join must return in ~2s with active subscribers) yet coarse enough that a fully idle subscriber
/// wakes only a few times a second -- a negligible cost the steady-state push hot path never pays
/// (a ready push always wins its own arm before this timer elapses).
const SUBSCRIBER_SHUTDOWN_POLL: core::time::Duration = core::time::Duration::from_millis(250);

/// The SUBSCRIBE-mode idle wait (SERVER_PUSH.md #20, PR 91a): `select!` between draining the
/// per-connection push channel, observing the SHED kill-signal, and reading the next command.
/// Returns `true` when the connection should CLOSE (the push consumer was SHED for back-pressure,
/// a peer close, or an I/O error), `false` to keep looping. Split out of [`serve_connection`] so
/// the hot non-subscriber path stays a plain `rt.recv` and this select! lives in one place.
///
/// The select! is FAIR (no `biased;`, FIX E): a `biased` push-first poll could STARVE the read
/// arm for a fast-flooded subscriber (the push arm would always be ready first), so the connection
/// could never re-arm its command read; random fairness re-arms the read promptly. All three arms
/// are still polled each iteration, so a command is never starved and a ready push is still
/// delivered. A delivered push is COALESCED with any further already-queued pushes (`try_recv`,
/// non-blocking) into ONE write. The read branch reads into a FRESH buffer and APPENDS to
/// `read_buf`: had another branch won, a `read_buf` moved into the CANCELLED recv future would be
/// lost with any partial frame it held; reading into a temporary keeps `read_buf`'s partial bytes
/// safe across cancellation. No RefCell borrow is held across the `.await`s (render is pure; the
/// subscription table is untouched).
///
/// The SHED arm (FIX D) `await`s `shed.wait()`: when the publisher overflows this connection's
/// bound it trips the signal AND drops the table sender, but the serve loop holds its OWN
/// `push_tx` clone, so `push_rx.recv()` would NOT return `None`. The signal is the disconnect
/// trigger; `wait()` parks on the signal's `Notify` (SPIN-FREE, no busy poll), so a healthy
/// subscriber pays nothing for this arm.
///
/// The SHUTDOWN arm (#543) races a SHORT periodic poll of the shared per-shard shutdown flag
/// (`timer_rt.timer(SUBSCRIBER_SHUTDOWN_POLL)`, the SAME Runtime timer seam the idle-timeout arm
/// uses -- ADR-0003, no wall-clock on the decision path). A non-subscriber closes on shutdown
/// because the acceptor drops its connection channel, but a subscriber PARKED on this `select!` of
/// (push recv, shed, socket recv) has NO arm that a graceful stop wakes, so without this it would
/// sit here until the DRAIN_GRACE backstop expires and block the shard-thread join. The poll fires
/// ONLY on a fully idle subscriber (a ready push always wins its own arm first, so the steady-state
/// hot path is unaffected); on each wake it does one relaxed atomic load -- `true` -> close, `false`
/// -> re-arm the idle wait (the same "return false, re-loop with an unchanged `read_buf`" path a
/// delivered push already takes), so a false wake costs one cheap loop turn, never a busy spin.
#[allow(clippy::too_many_arguments)]
async fn subscriber_idle_wait(
    stream: &mut ironcache_runtime::ClientStream,
    push_rx: &mut tokio::sync::mpsc::Receiver<crate::pubsub::ServerPush>,
    shed: &std::sync::Arc<crate::pubsub::ShedSignal>,
    read_buf: &mut Vec<u8>,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    timer_rt: &TokioRuntime,
) -> bool {
    // Fast pre-check: if the publisher already shed this connection between iterations, close
    // now without entering the select! (the table sender is gone; nothing more will arrive).
    if shed.is_tripped() {
        return true;
    }
    // Fast pre-check: if a graceful stop already began between iterations, close now (#543) without
    // entering the select! -- symmetrical to the shed pre-check above.
    if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
        return true;
    }
    tokio::select! {
        maybe_push = push_rx.recv() => {
            let Some(push) = maybe_push else {
                // Both the table sender(s) AND the serve loop's own push_tx are gone: the
                // channel is fully closed. Treat as a disconnect and close.
                return true;
            };
            // `out` was flushed before the idle wait, so it is empty here; rendering into it
            // preserves the flush-before-idle FIFO ordering.
            out.clear();
            encode_into(out, &push.render(proto), proto);
            while let Ok(next) = push_rx.try_recv() {
                encode_into(out, &next.render(proto), proto);
            }
            let sent = out.len();
            stream.send(std::mem::take(out)).await.map_or(true, |returned| {
                *out = returned;
                // #527: net output for a coalesced pub/sub push write (cold path -- reach the
                // serving shard's counters through the thread-local, no `state_rc` in scope here).
                shard_state().borrow().counters.on_net_output(sent as u64);
                false
            })
        }
        () = shed.wait() => {
            // The publisher SHED this slow consumer (its push channel overflowed past the
            // bound): close the connection (its subscriptions are cleaned up on the close path).
            true
        }
        () = timer_rt.timer(SUBSCRIBER_SHUTDOWN_POLL) => {
            // #543: a short poll of the shared shutdown flag. On a graceful stop, close (`true`);
            // otherwise (`false`) re-arm the idle wait -- the outer serve loop re-decodes the
            // unchanged `read_buf` (Incomplete), flushes the empty `out`, and re-enters here.
            shutdown.load(std::sync::atomic::Ordering::Relaxed)
        }
        res = stream.recv(Vec::new()) => {
            let Ok(res) = res else { return true; };
            if res.n == 0 {
                return true; // peer closed
            }
            read_buf.extend_from_slice(&res.buf);
            // #527: net input for a command read while in the subscriber idle wait.
            shard_state().borrow().counters.on_net_input(res.n as u64);
            false
        }
    }
}

/// The io_uring-path SUBSCRIBE-mode idle wait (FIX2, the io_uring analog of [`subscriber_idle_wait`]).
/// `select!`s between draining this connection's per-connection push channel, observing the SHED
/// kill-signal, and reading the next command over the io_uring owned-buffer `recv` seam. Returns
/// `true` when the connection should CLOSE (the push consumer was SHED for back-pressure, a peer
/// close, or an I/O error), `false` to keep looping.
///
/// This is a SEPARATE function from the tokio [`subscriber_idle_wait`] because the io_uring transport
/// is a distinct type ([`ironcache_runtime::UringTcpStream`] driven through the [`Runtime`] seam's
/// owned-buffer `recv`/`send`), not the tokio `ClientStream`. The fairness/coalescing/FIFO STRUCTURE
/// mirrors [`subscriber_idle_wait`], but one io_uring-path semantic differs and is a documented
/// limitation (a soak-time follow-up): tokio's `recv` is READINESS-based, so cancelling it (a push
/// or shed arm winning the `select!`) consumes no socket bytes; io_uring's `recv` is COMPLETION-based,
/// so a recv SQE that already consumed inbound client bytes into its buffer before the push/shed arm
/// wins is dropped (the buffer is kept alive by tokio-uring's Ignored lifecycle until the kernel CQE,
/// so this is memory-safe, but those consumed bytes are NOT re-read). In practice this can only lose
/// inbound bytes for a subscriber that is ACTIVELY PIPELINING commands while simultaneously receiving
/// a push (an uncommon pattern; an idle subscriber's cancelled recv consumed nothing). A true fix
/// (an AsyncCancel + drain, or not racing recv against pushes mid-command) is deferred to the Linux
/// soak where io_uring can be runtime-validated. The rest applies verbatim:
///   * NO `biased;`, so a fast-flooded subscriber never starves its command-read arm.
///   * a delivered push coalesces any further already-queued pushes (`try_recv`) into ONE write.
///   * the read arm reads into a FRESH `Vec::new()` and APPENDS to `read_buf`, so a partial frame
///     already in `read_buf` survives the recv future being cancelled when another arm wins. The
///     io_uring `recv` future keeps its OWN throwaway buffer alive until the kernel completes/cancels
///     the op (tokio-uring's cancel-on-drop contract) -- `read_buf` is never moved into it, so no
///     buffered bytes are at risk.
///
/// The SHED arm parks on the signal's `Notify` (spin-free); the fast pre-check closes promptly when
/// the publisher already shed this connection between iterations.
///
/// The SHUTDOWN arm (#543) mirrors the tokio [`subscriber_idle_wait`]: it races a SHORT poll of the
/// shared per-shard shutdown flag ([`SUBSCRIBER_SHUTDOWN_POLL`] via `rt.timer`, whose seam is the
/// canonical tokio time driver under `tokio_uring::start`), so a subscriber parked on this `select!`
/// observes a graceful stop within one interval and closes instead of blocking the shard-thread join
/// until the drain grace. It fires only on a fully idle subscriber; a false wake re-arms the wait.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
#[allow(clippy::too_many_arguments)]
async fn subscriber_idle_wait_generic<R>(
    rt: &R,
    stream: &mut R::Stream,
    push_rx: &mut tokio::sync::mpsc::Receiver<crate::pubsub::ServerPush>,
    shed: &std::sync::Arc<crate::pubsub::ShedSignal>,
    read_buf: &mut Vec<u8>,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> bool
where
    R: ironcache_runtime::BatchedRecvSend,
{
    // The socket-read arm uses `recv_batch` (NOT a fresh single-shot `rt.recv`): on the raw backend a
    // MULTISHOT recv may already be armed on this fd from the main loop, and opening a competing
    // single-shot `Read` would split the byte stream between two ops (#513 review). `recv_batch`
    // drains the SAME multishot slot; a `select!`-cancel of it is drop-safe (the multishot slot is
    // persistent, and the owned fallback appends into a fresh buffer). `rt`'s methods resolve through
    // the `R: BatchedRecvSend` bound directly.
    // Fast pre-check: if the publisher already shed this connection between iterations, close now
    // without entering the select! (the table sender is gone; nothing more will arrive). This is
    // ALSO the close-on-shed for a pure-idle flooded subscriber (FIX2 non-negotiable).
    if shed.is_tripped() {
        return true;
    }
    // Fast pre-check: if a graceful stop already began between iterations, close now (#543).
    if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
        return true;
    }
    tokio::select! {
        maybe_push = push_rx.recv() => {
            let Some(push) = maybe_push else {
                // Both the table sender(s) AND the serve loop's own push_tx are gone: the
                // channel is fully closed. Treat as a disconnect and close.
                return true;
            };
            // `out` was flushed before the idle wait, so it is empty here; rendering into it
            // preserves the flush-before-idle FIFO ordering.
            out.clear();
            encode_into(out, &push.render(proto), proto);
            while let Ok(next) = push_rx.try_recv() {
                encode_into(out, &next.render(proto), proto);
            }
            let sent = out.len();
            rt.send(stream, std::mem::take(out)).await.map_or(true, |returned| {
                *out = returned;
                // #527: net output for a coalesced pub/sub push write (io_uring cold path).
                shard_state().borrow().counters.on_net_output(sent as u64);
                false
            })
        }
        () = shed.wait() => {
            // The publisher SHED this slow consumer (its push channel overflowed past the bound):
            // close the connection (its subscriptions are cleaned up on the close path).
            true
        }
        () = rt.timer(SUBSCRIBER_SHUTDOWN_POLL) => {
            // #543: a short poll of the shared shutdown flag. On a graceful stop, close (`true`);
            // otherwise (`false`) re-arm the idle wait (the outer serve loop re-decodes the
            // unchanged `read_buf`, flushes the empty `out`, and re-enters here).
            shutdown.load(std::sync::atomic::Ordering::Relaxed)
        }
        n = rt.recv_batch(stream, read_buf) => {
            let Ok(n) = n else { return true; };
            if n == 0 {
                return true; // peer closed
            }
            // recv_batch already APPENDED the newly-arrived bytes into `read_buf` (multishot slot or
            // owned fallback); count them. #527: net input for a subscriber-idle-wait command read.
            shard_state().borrow().counters.on_net_input(n as u64);
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
pub(crate) fn subscriber_gate_blocks(
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
                // Sharded Pub/Sub (#410): a RESP2 subscriber may also run the SHARD (un)subscribe
                // control commands (Redis allows SSUBSCRIBE/SUNSUBSCRIBE in subscribe mode). SPUBLISH
                // is NOT allowed (it is a publish, like PUBLISH, which the gate blocks).
                | b"SSUBSCRIBE"
                | b"SUNSUBSCRIBE"
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
