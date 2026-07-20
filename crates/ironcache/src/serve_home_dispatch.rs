// SPDX-License-Identifier: MIT OR Apache-2.0
//! The HOME-shard command dispatch split out of `serve.rs` (#625): `handle_request` (the sync home
//! path that runs Tier-0 commands + the #511 GET-by-reference fast path), `get_home_by_ref` (the
//! by-ref GET encoder + the #515 zero-copy splice decision), the per-shard-thread zero-copy GET sink
//! (`ZcSink` / `ZC_SINK` / `zc_sink_active` / `push_zc_bulk` / `drain_zc_sink`, io_uring-only), and
//! the INFO-keyspace gate. Behavior-preserving relocation: the bodies are byte-identical.

use super::{ShardState, ShardStoreImpl, encode_into};
use ironcache_env::{Clock, SystemEnv};
use ironcache_observe::{CounterSnapshot, MemoryInfo};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{
    ConnState, CounterDeltas, Request, TimingWheel, UnixMillis, dispatch_with_cmd,
};
use ironcache_store::process_memory;
use std::cell::RefCell;
use std::rc::Rc;

/// Whether an `INFO [section]` reply will INCLUDE the `# Keyspace` section (#531), so the router
/// only pays the cross-shard keyspace gather when the client will actually see it. This mirrors
/// `ironcache_observe::build_info`'s section `want` gate EXACTLY: the keyspace section renders for a
/// bare `INFO` (no section) or a section of `default` / `all` / `everything` / `keyspace` (case-
/// insensitive). `INFO server` / `INFO stats` / etc. do NOT include it, so they skip the fan-out.
pub(crate) fn info_reply_includes_keyspace(request: &Request) -> bool {
    match request.args.get(1) {
        None => true,
        Some(section) => {
            let s = String::from_utf8_lossy(section).to_ascii_lowercase();
            s == "default" || s == "all" || s == "everything" || s == "keyspace"
        }
    }
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
pub(crate) fn handle_request(
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    cmd_upper: &[u8],
    // #531: the NODE-WIDE INFO `# Keyspace` lines (per-db counts summed across every shard), or
    // `None` to fall back to THIS shard's local `db_len`. The router gathers it (via a whole-
    // keyspace fan-out) ONLY for an INFO whose reply includes the keyspace section on a >1-shard
    // node; every other command and the single-shard node pass `None` (byte-identical). Borrowed
    // from the router's stack for the duration of the synchronous dispatch.
    node_keyspace: Option<&[ironcache_observe::KeyspaceDbLine]>,
    out: &mut Vec<u8>,
) -> bool {
    // #511 GET-BY-REFERENCE HOME FAST PATH (Dragonfly GET-gap, root cause #2). A plain 2-arg `GET`
    // served on its OWN (home) shard is answered by encoding the RESP bulk string DIRECTLY from the
    // stored value bytes into `out`, DROPPING the per-GET `Bytes::copy_from_slice` + the
    // `Value::BulkString` allocation that `cmd_get` builds (the value is written store->`out` in ONE
    // copy, ZERO heap alloc). Everything else -- a wrong-arity `GET`, every other command, and the
    // cross-shard HOP path (whose reply must be an OWNED, `Send` `Value` crossing the coordinator
    // channel, so it KEEPS `cmd_get`) -- falls through to the UNCHANGED `dispatch_with_cmd` below.
    // This is a home-path-only diversion of the reply ENCODING; the router already ran the auth /
    // subscribe-mode gates (a GET reaching here is authenticated and is not a blocked subscriber
    // command), so bypassing dispatch's redundant backstop gates is safe. The `!conn.in_multi`
    // guard is REQUIRED: `route_in_multi` also funnels through `handle_request` and relies on
    // dispatch's QUEUE GATE to stage a GET inside a transaction as `+QUEUED` (NOT execute it), so an
    // in-MULTI GET must fall through to `dispatch_with_cmd` below; only the LIVE (non-queued) home
    // GET takes the by-ref fast path. A queued GET is replayed by `EXEC` through `dispatch`
    // (`cmd_get`), so its reply bytes are unchanged.
    if cmd_upper == b"GET" && request.args.len() == 2 && !conn.in_multi {
        return get_home_by_ref(ctx, conn, env, store_rc, state_rc, &request.args[1], out);
    }
    state_rc.borrow_mut().counters.on_command();
    // INFO ROLLUP (#531): the `# Stats`/`# Clients` counters are the NODE-WIDE sum, not this
    // serving shard's ~1/N view. The metrics registry is always present now (built at boot even
    // with `/metrics` off), and every shard's `ShardCounters` mutate their registered cell, so
    // `aggregate()` folds EVERY shard into one snapshot -- invariant to which shard homed this
    // connection, and consistent with DBSIZE / `/metrics`. The serving-shard snapshot is the
    // defensive fallback for a registry-absent `ServerContext` (unit tests that build one bare); in
    // the binary the registry is always `Some`, so the node-wide arm is always taken. The closure
    // is invoked ONLY by INFO (inside dispatch); the aggregate arm borrows nothing of `state_rc`,
    // and the fallback arm's `state_rc.borrow()` runs sequentially with (never aliasing) dispatch's
    // later mutable borrow, exactly as before.
    let snapshot_fn = || {
        ctx.metrics_registry.as_ref().map_or_else(
            || state_rc.borrow().counters.snapshot(),
            ironcache_observe::MetricsRegistry::aggregate,
        )
    };
    let rollup: &dyn Fn() -> CounterSnapshot = &snapshot_fn;
    // #531: the node-wide INFO keyspace source. `Some` slice -> yield the fanned-out per-db lines;
    // `None` -> `cmd_info` falls back to this shard's local `db_len` (single-shard / non-INFO).
    let keyspace_fn = || node_keyspace.map(<[_]>::to_vec);
    let keyspace: ironcache_server::KeyspaceFn<'_> = &keyspace_fn;
    // COMMANDSTATS / ERRORSTATS render (#413): render the serving shard's per-command + per-error
    // tables into the INFO section bodies. Invoked ONLY when INFO asks for those sections (the
    // closure is not called otherwise), and it borrows `state_rc` immutably like `rollup` does
    // (sequentially, never aliasing dispatch's later mutable borrow).
    let cmdstats_fn = || {
        let (mut cs, mut es) = (String::new(), String::new());
        // COMMANDSTATS node-wide (#527): sum EVERY shard's per-command atomic table via the registry
        // (the SAME cross-shard rollup #545 uses for `# Stats`) and render node-wide `cmdstat_<cmd>`
        // lines -- invariant to which shard homed this connection. The registry is always present in
        // the binary; a bare unit-test context without one renders an empty body (it records no
        // per-command stats either), byte-identical to the pre-#527 empty-closure fallback.
        if let Some(reg) = ctx.metrics_registry.as_ref() {
            ironcache_server::render_commandstats_agg(&mut cs, &reg.aggregate_command_stats());
        }
        // ERRORSTATS stays serving-shard-scoped (#527 follow-up): render THIS shard's local error
        // table (single-shard nodes see the whole node; multi-shard error aggregation is the
        // remaining smaller follow-up).
        state_rc.borrow().command_stats.render_errorstats(&mut es);
        (cs, es)
    };
    let cmdstats: ironcache_server::CmdStatsFn<'_> = &cmdstats_fn;
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
            cmdstats,
            keyspace,
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
    let reset_stats = deltas.reset_stats;
    {
        deltas.expired += lazy_expired;
        let mut st = state_rc.borrow_mut();
        if deltas != CounterDeltas::default() {
            st.counters.apply(deltas);
        }
        // CONFIG RESETSTAT (#413): the same signal that zeroes the counter cell also clears the
        // per-command + per-error stats tables (Redis `resetServerStats` resets both).
        if reset_stats {
            st.command_stats.reset();
        }
        st.last_policy_generation = shard_generation;
    }
    // CONFIG RESETSTAT NODE-WIDE (#531): `apply` above zeroed only THIS serving shard's cell, but
    // INFO now reports the node-wide rollup (every shard's cell summed via `aggregate()`), so a
    // reset must fan across EVERY shard's cell or a sibling shard's stale totals would survive in
    // the rollup. The registry is always present in the binary; the reset is a handful of relaxed
    // atomic stores per cell (RESETSTAT is a rare admin command, never on the data hot path).
    if reset_stats {
        if let Some(registry) = ctx.metrics_registry.as_ref() {
            registry.reset_stats();
        }
    }
    encode_into(out, &reply, conn.proto);
    conn.should_close
}

/// The per-shard-thread ZERO-COPY GET sink (#515 P4c). See [`ZC_SINK`] / [`push_zc_bulk`].
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
#[derive(Default)]
pub(crate) struct ZcSink {
    /// The ordered value SPLICES (offset in `out` + pinned `(ptr, len)`) for this batch's flush.
    inserts: Vec<ironcache_runtime::ZcInsert>,
    /// The frozen-slot handles (type-erased [`ironcache_store::ZcPin`]s) backing those splices; the
    /// io_uring `send_zc` takes ownership of these until its CQE so the bytes outlive the write.
    pins: Vec<Box<dyn core::any::Any>>,
}

#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
thread_local! {
    /// ZERO-COPY GET sink for THIS shard thread (#515 P4c). The io_uring serve loop
    /// ([`serve_connection_generic`]) installs one (`Some`) on its shard thread; [`get_home_by_ref`]
    /// pushes a large String hit's frozen-value pin + splice offset here INSTEAD of copying the bytes
    /// into `out`, and the loop DRAINS it (via [`drain_zc_sink`]) into the flush's `zc_inserts`/
    /// `zc_pins` immediately after every `route_and_dispatch` returns.
    ///
    /// SOUNDNESS of the shared thread-local across the connections multiplexed on this shard thread:
    /// the ONLY pusher ([`get_home_by_ref`]) is fully SYNCHRONOUS, and `route_and_dispatch`'s
    /// home-GET path has NO `.await` between that push and the loop's drain (the post-dispatch
    /// blocking-wake + keyspace-publish are both sync, and a GET wakes/publishes nothing). So the sink
    /// is always drained back to empty before the loop's next yield -- no other connection can ever
    /// observe another's pins at an await boundary. The TOKIO serve loop never installs a sink, so
    /// `get_home_by_ref` there finds `None` and copies via `encode_bulk_ref` (byte-identical to #511).
    pub(crate) static ZC_SINK: core::cell::RefCell<Option<ZcSink>> = const { core::cell::RefCell::new(None) };
}

/// Is a zero-copy GET sink installed on THIS thread (i.e. are we on an io_uring serve loop)? A single
/// thread-local borrow + `is_some`. On a non-io_uring build there is no sink type, so this is a
/// compile-time `false` and every GET takes the copy path.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
fn zc_sink_active() -> bool {
    ZC_SINK.with(|c| c.borrow().is_some())
}
#[cfg(not(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
)))]
const fn zc_sink_active() -> bool {
    false
}

/// PIN a present large String value for a zero-copy send and frame it into `out` (#515 P4c). Called
/// ONLY after [`zc_sink_active`] returned true and the by-ref classify saw a String hit at/above the
/// live `zero-copy-get-threshold`, so the sink IS installed and the key IS live (same synchronous `borrow_mut`
/// scope, no await, no other code interleaves). Frames `$<len>\r\n` then the SPLICE POINT then `\r\n`
/// -- the value bytes are NOT copied into `out`; the send interleaves them from the pin at that
/// offset. Returns `true` on success. Returns `false` (leaving `out` UNTOUCHED) only in the
/// unreachable-in-practice case that the re-probe misses or the sink vanished, so the caller can fall
/// back to a copy and never desync the reply.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
fn push_zc_bulk(
    store: &ShardStoreImpl,
    db: u32,
    key: &[u8],
    now: UnixMillis,
    out: &mut Vec<u8>,
) -> bool {
    // Re-probe under the SAME `borrow_mut` to obtain the slot-`Arc`-backed pin. `pin_value_frozen`
    // holds a clone of the value's slot `Arc`, so any later (or concurrent) write to that key COWs
    // the live slot and this frozen clone keeps the ORIGINAL bytes valid + immutable until dropped
    // (the #576 mechanism) -- no fence, no copy. `None` is unreachable here (we just read the key as a
    // live String in this same scope), so a `None` cleanly declines to the caller's copy fallback.
    let Some(pin) = store.pin_value_frozen(db, key, now) else {
        return false;
    };
    ZC_SINK.with(|c| {
        let mut g = c.borrow_mut();
        let Some(sink) = g.as_mut() else {
            // Unreachable (the caller checked `zc_sink_active`, and nothing uninstalls the sink);
            // decline WITHOUT having written the header, so `out` is pristine for the copy fallback.
            return false;
        };
        // Header, then the splice offset (`at` = where the value logically goes: after `out[..at]`,
        // before the trailing CRLF), then the trailing CRLF. `send_zc` splices the pinned bytes at
        // `at`, reproducing exactly `encode_bulk_ref`'s `$<len>\r\n<bytes>\r\n` on the wire.
        ironcache_protocol::encode_bulk_len_prefix(out, pin.len());
        let at = out.len();
        sink.inserts.push(ironcache_runtime::ZcInsert {
            at,
            ptr: pin.as_ptr(),
            len: pin.len(),
        });
        sink.pins.push(Box::new(pin));
        out.extend_from_slice(b"\r\n");
        true
    })
}
#[cfg(not(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
)))]
fn push_zc_bulk(_: &ShardStoreImpl, _: u32, _: &[u8], _: UnixMillis, _: &mut Vec<u8>) -> bool {
    // No io_uring send_zc on this target; `zc_sink_active()` is a const `false`, so this is never
    // reached. Present only so `get_home_by_ref` compiles identically across targets.
    false
}

/// DRAIN this shard thread's zero-copy sink into the flush's insert/pin lists (#515 P4c). Called by
/// the io_uring serve loop immediately after each `route_and_dispatch` returns -- a window with NO
/// `.await`, so the sink holds exactly THIS command's splices (a home GET may have pushed one) and no
/// other multiplexed connection can have raced it (see [`ZC_SINK`]). Moves the elements out (leaving
/// the sink empty for the next command); a no-op fast path when the command pinned nothing.
#[cfg(all(
    target_os = "linux",
    any(feature = "io_uring", feature = "io_uring_raw")
))]
pub(crate) fn drain_zc_sink(
    inserts: &mut Vec<ironcache_runtime::ZcInsert>,
    pins: &mut Vec<Box<dyn core::any::Any>>,
) {
    ZC_SINK.with(|c| {
        if let Some(sink) = c.borrow_mut().as_mut() {
            if !sink.inserts.is_empty() {
                inserts.append(&mut sink.inserts);
                pins.append(&mut sink.pins);
            }
        }
    });
}

/// #511 GET-BY-REFERENCE HOME FAST PATH. Answer a plain 2-arg `GET` served on its home shard by
/// framing the RESP bulk string DIRECTLY from the stored value bytes into `out`, dropping the
/// `Bytes::copy_from_slice` + `Value::BulkString` allocation `cmd_get` pays (root cause #2 of the
/// Dragonfly GET gap). The value bytes are written store->`out` in a SINGLE copy with ZERO heap
/// allocation; the cross-shard HOP path is untouched (it still returns an owned `Value` via
/// `cmd_get`, which must be `Send` to cross the coordinator channel -- a borrow cannot hop).
///
/// BORROW SAFETY. `store.read` returns a `ValueRef` that BORROWS the shard store (the value bytes
/// are a `&[u8]` into the stored buffer, the #519 single-probe read). That borrow is CONSUMED here,
/// INSIDE the `store` borrow scope, before it is released: `encode_bulk_ref` copies the bytes into
/// the SEPARATE `out` buffer immediately, so the value ref can NEVER outlive the store borrow. A
/// `GET` does not mutate the value; the only write `read` performs is the in-object S3-FIFO freq
/// bump, which happens BEFORE the bytes are handed back (single `find_mut`), so the byte slice we
/// encode cannot alias a concurrent mutation or a freed entry.
///
/// PARITY WITH `dispatch`. The command counter, the `keyspace_hits`/`keyspace_misses` fold, the
/// per-command notify-flag snapshot (so a lazy-TTL `expired` event fired by `store.read` reads the
/// CURRENT flags), and the lazy-expiry `expired_keys` drain are all reproduced so INFO + keyspace
/// notifications stay identical. The WRONGTYPE and NULL replies reuse the EXACT `Value`s `cmd_get`
/// returns (byte-identical). The router's post-dispatch wake + keyspace-event publish still run on
/// the returned `close` flag, exactly as for the general path. The active timing-wheel drain and
/// the rare maxmemory-policy hot-swap check are deliberately NOT reproduced here: neither affects a
/// GET's reply bytes (they reap OTHER keys / swap the eviction policy), both are still driven by
/// every non-GET command and the background reap timer, and skipping them on a read matches Redis's
/// access-does-not-active-expire behavior. Kept as one self-contained fn so the change is trivially
/// reversible (delete the top-of-`handle_request` branch + this fn) and cannot drift the hot path.
fn get_home_by_ref(
    ctx: &ServerContext,
    conn: &mut ConnState,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    state_rc: &Rc<RefCell<ShardState>>,
    key: &[u8],
    out: &mut Vec<u8>,
) -> bool {
    // The `Store` waist trait, in scope so `read` (the by-ref accessor) resolves on the concrete
    // `ShardStoreImpl`. Local like the other inner-scope waist-trait uses in this module.
    use ironcache_storage::{DataType, Store};

    state_rc.borrow_mut().counters.on_command();
    let now = UnixMillis(env.borrow().now_unix_millis());
    // Snapshot the live `notify-keyspace-events` flags into this shard's per-command emit gate,
    // EXACTLY as `dispatch_with_cmd` does, so a keyspace `expired` event fired by the lazy TTL
    // backstop inside `store.read` below reads the CURRENT flags (not the previous command's). One
    // relaxed atomic load + a thread-local `Cell` write; a no-op when notifications are disabled.
    ironcache_config::notify::set_command_flags(ctx.runtime.notify_flags());

    let mut deltas = CounterDeltas::default();
    let lazy_expired;
    {
        let mut store = store_rc.borrow_mut();
        // Classify the read; for the COPY path frame the reply inline from the borrowed value bytes.
        // For the #515 ZERO-COPY path we only DECIDE here (`true`): the value is pinned AFTER `v`'s
        // borrow of `store` ends (below), because `pin_value_frozen` also borrows `store` (`&self`)
        // and must not overlap the `read` borrow.
        let defer_zc = match store.read(conn.db, key, now) {
            Some(v) if v.data_type() == DataType::String => {
                deltas.keyspace_hits += 1;
                let bytes = v.as_bytes();
                // #515 ZERO-COPY GET: a value at/above the live `zero-copy-get-threshold` on the
                // io_uring serve loop is SPLICED into the socket write straight from the store -- its
                // bytes are NEVER copied into `out`. A smaller value, a `0` threshold (zero-copy
                // disabled), or any value on the tokio loop (`zc_sink_active()` is a const `false` off
                // io_uring), takes the by-ref COPY fast path (#511): frame `$<len>\r\n<bytes>\r\n`
                // straight from the stored bytes into `out` -- no `Bytes::copy_from_slice`, no
                // `Value::BulkString`. `out` is a distinct buffer from `store`, so `v.as_bytes()` (a
                // borrow into the store) and `&mut out` do not alias; the borrow ends at the arm
                // boundary, inside the store scope. The threshold is one relaxed atomic load per home
                // GET (config-tunable, hot-reloadable via `CONFIG SET zero-copy-get-threshold`).
                let zc_threshold = ctx.runtime.zero_copy_get_threshold();
                if zc_threshold != 0 && bytes.len() as u64 >= zc_threshold && zc_sink_active() {
                    true
                } else {
                    ironcache_protocol::encode_bulk_ref(out, bytes);
                    false
                }
            }
            Some(_) => {
                // A non-string key: byte-identical to `cmd_get`'s WRONGTYPE reply. Rare, off the hot
                // path. Neither a hit nor a miss (mirrors `keyspace_counted`).
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_type()),
                    conn.proto,
                );
                false
            }
            None => {
                // Missing OR lazily-expired: the null reply (`$-1` RESP2 / `_` RESP3), a keyspace
                // MISS. Byte-identical to `cmd_get`'s `Value::Null`.
                encode_into(out, &ironcache_server::Value::Null, conn.proto);
                deltas.keyspace_misses += 1;
                false
            }
        };
        // `v` (and its borrow of `store`) is now dropped, so `store` is free to pin. Frame the large
        // value's reply via the zero-copy splice. `push_zc_bulk` re-probes under this SAME synchronous
        // `borrow_mut` (no await, nothing else runs), so it is guaranteed to find the still-live key;
        // its `false` return is the unreachable-in-practice defensive fallback (re-read + copy) so a
        // missed pin can never desync the reply.
        if defer_zc && !push_zc_bulk(&store, conn.db, key, now, out) {
            match store.read(conn.db, key, now) {
                Some(v) if v.data_type() == DataType::String => {
                    ironcache_protocol::encode_bulk_ref(out, v.as_bytes());
                }
                _ => encode_into(out, &ironcache_server::Value::Null, conn.proto),
            }
        }
        // Drain the lazy-backstop expiry count `store.read` may have produced (a GET of an expired
        // key reaps it), inside the store scope, exactly as `handle_request` does after dispatch.
        lazy_expired = store.take_lazy_expired();
    }

    // Fold this command's keyspace hit/miss + lazy-expiry count into the shard's counters for INFO.
    // A cheap no-op on the common hit path with notifications/TTLs quiescent.
    deltas.expired += lazy_expired;
    if deltas != CounterDeltas::default() {
        state_rc.borrow_mut().counters.apply(deltas);
    }
    conn.should_close
}
