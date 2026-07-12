// SPDX-License-Identifier: MIT OR Apache-2.0
//! Thread-per-core bootstrap (RUNTIME.md "topology", ADR-0002).
//!
//! [`run_shards`] spawns one OS thread per shard, each with its own current-thread
//! tokio runtime and `LocalSet` (the shard executor). A single dedicated ACCEPTOR
//! thread owns the one listening socket and round-robins each accepted connection
//! to a shard over a per-shard channel; the receiving shard adopts the connection
//! onto ITS reactor and serves it entirely on that shard, so per-connection state
//! is core-local with no shared hot-path structure.
//!
//! ## Why a userspace acceptor instead of per-shard `SO_REUSEPORT`
//!
//! Earlier each shard bound its own `SO_REUSEPORT` listener and ran its own accept
//! loop, relying on the KERNEL to load-balance accepts across the listeners. That
//! balances on Linux but NOT on macOS/BSD, where accepts concentrate on a single
//! listener, so N shards behaved like one shard for I/O (all connections funneled
//! to one core). Distributing accepts in USERSPACE (one acceptor, round-robin to
//! per-shard channels) is portable: it balances identically on every platform and
//! makes throughput scale with shard count. The acceptor only does `accept()` +
//! hand-off; the connection still lives its whole life on the shard that adopts it
//! (which shard accepts a connection does not affect correctness, only I/O spread,
//! because keyspace ops route by key through the coordinator).
//!
//! The per-connection serve logic is supplied by the caller as an async closure
//! over the shard's [`crate::Runtime`], keeping this layer free of any protocol or
//! command knowledge.
//!
//! LOGGING NOTE (OBSERVABILITY.md, #152): the binary crate's operational logs were migrated to
//! the `tracing` facade (filtered by `--log-level`). The few `eprintln!` calls that remain in
//! THIS module (acceptor / shard-thread spawn + drain-grace diagnostics) are intentionally left
//! as `eprintln!` for now: `ironcache-runtime` is the pure I/O/runtime SEAM and does not yet take
//! a `tracing` dependency edge. They are infrequent boot/shutdown-path diagnostics, not hot-path
//! logs; routing them through `tracing` is a small follow-up that adds the dependency here.

use core::time::Duration;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A shard identity: which shard (`index`) of how many (`total`). Used for
/// per-shard counters (OBSERVABILITY.md) and for the `k = HASH(KEY) % N` routing
/// rule once a store exists (ADR-0002); PR-1 has no store, so it is identity only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardId {
    /// This shard's index in `[0, total)`.
    pub index: usize,
    /// The total number of shards `N`.
    pub total: usize,
}

/// How many shards to run and where to bind. The shard count defaults to the
/// available parallelism (CONFIG.md) but is overridable.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Number of shards (one OS thread / current-thread runtime each).
    pub shards: usize,
    /// The single address the acceptor thread binds and accepts on; connections
    /// are then round-robined to the shards in userspace.
    pub bind: SocketAddr,
    /// SHARD-OWNER ENDPOINTS (#517): when `true`, bind ONE listener PER shard at
    /// `bind.port() + i` (shard `i`), each homing its accepted connections on THAT shard, instead of
    /// one listener round-robining across shards. A cluster-aware client then routes each key to the
    /// port of the shard that owns it (`CLUSTER SLOTS`), so the connection lands on the key's owner
    /// and the internal cross-shard hop is skipped. `false` (the default) keeps the single-acceptor
    /// round-robin. Uses DISTINCT PORTS (not `SO_REUSEPORT`), so it is portable and needs no kernel
    /// accept-balancing.
    pub shard_owner_ports: bool,
}

/// A handle to a running set of shards, used to signal graceful shutdown and join.
#[derive(Debug)]
pub struct ShardSet {
    shutdown: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<()>>,
    /// The RESP client listener raw fd(s) captured at bind time (#638, streamed live-cutover): the
    /// single round-robin acceptor's listener (one fd), or in shard-owner mode (#517) one fd per
    /// shard in shard order. Exposed via [`Self::client_listener_fds`] so the in-server cutover host
    /// can pass the listen socket to the spawned receiver sibling for the inherited-listener no-RST
    /// handoff ([`crate::tokio_rt::command_inherit_listener`]). This is a NON-owning record (an
    /// integer per listener): the acceptor thread(s) keep the listener(s) open for the process
    /// lifetime, so these fds stay valid; nothing here ever closes them. Empty on the io_uring boot
    /// ([`Self::from_parts`]) until that path captures its listeners too.
    #[cfg(unix)]
    client_listener_fds: Vec<std::os::fd::RawFd>,
}

/// The grace window a shard waits for its in-flight connection tasks to finish
/// after it stops accepting, before it returns regardless (SHUTDOWN.md bounded
/// drain). Kept here so the bound is one constant; the binary may make it a knob.
pub const DRAIN_GRACE: Duration = Duration::from_secs(5);

impl ShardSet {
    /// Construct a [`ShardSet`] from its shared shutdown flag and the spawned thread
    /// handles. Used by an alternate per-shard bootstrap (the io_uring boot,
    /// [`crate::io_uring_rt::run_shards_uring`]) that builds the same shutdown-flag +
    /// acceptor + per-shard-thread shape as [`run_shards`] but on a different per-thread
    /// runtime, so it can return the SAME `ShardSet` the binary joins on shutdown. The
    /// fields stay private; this is the one sanctioned constructor outside `run_shards`.
    #[must_use]
    pub fn from_parts(
        shutdown: Arc<AtomicBool>,
        handles: Vec<std::thread::JoinHandle<()>>,
    ) -> Self {
        ShardSet {
            shutdown,
            handles,
            // The io_uring boot does not (yet) capture its client-listener fd(s) here; the streamed
            // cutover host is gated to the tokio boot for now (#638), so an empty record is correct.
            #[cfg(unix)]
            client_listener_fds: Vec::new(),
        }
    }

    /// Signal all shards to stop accepting and drain, then wait for their threads.
    ///
    /// Each shard performs a BOUNDED drain (see [`DRAIN_GRACE`]): it stops
    /// accepting, then awaits its live connection tasks until they finish or the
    /// grace window elapses, and only then its accept loop returns. This call
    /// joins every shard thread and surfaces the FIRST join error (a shard thread
    /// that panicked) rather than silently discarding it.
    ///
    /// # Errors
    ///
    /// Returns the first thread-join error if any shard thread panicked.
    pub fn shutdown_and_join(self) -> std::thread::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        let mut first_err: std::thread::Result<()> = Ok(());
        for h in self.handles {
            if let Err(e) = h.join() {
                // Keep joining the rest, but remember the first panic to surface.
                if first_err.is_ok() {
                    first_err = Err(e);
                }
            }
        }
        first_err
    }

    /// The shared shutdown flag, so a signal handler can flip it without holding
    /// the [`ShardSet`]. Reads are relaxed-acceptable: shutdown is level-triggered.
    #[must_use]
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// The RESP client listener raw fd(s) captured at bind time (#638, streamed live-cutover).
    ///
    /// In the DEFAULT single-acceptor mode this is exactly one fd (the round-robin listener); in
    /// shard-owner mode (#517) it is one fd per shard, in shard order. The streamed live-cutover host
    /// passes these to the receiver-sibling spawn so the listen socket is INHERITED (dup2'd) across
    /// the flip for the no-RST handoff -- the acceptor thread(s) keep the listener(s) OPEN for the
    /// process lifetime, so the fds are valid at spawn time and the caller must NOT close them (it is
    /// a non-owning copy). Empty on the io_uring boot path until it captures its listeners too.
    #[cfg(unix)]
    #[must_use]
    pub fn client_listener_fds(&self) -> Vec<std::os::fd::RawFd> {
        self.client_listener_fds.clone()
    }
}

/// The default shard count: the host's available parallelism (CONFIG.md). Never
/// zero (a degenerate host reports at least one).
#[must_use]
pub fn available_shards() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

#[cfg(feature = "tokio")]
mod tokio_bootstrap {
    use super::{Arc, AtomicBool, DRAIN_GRACE, Duration, Ordering, ShardConfig, ShardId, ShardSet};
    use crate::TokioRuntime;
    use crate::tokio_rt::listener_for;
    use std::cell::Cell;
    use std::future::Future;
    use std::rc::Rc;

    /// A core-local count of in-flight connection tasks on one shard. Incremented
    /// when a connection task starts and decremented when it completes (even on
    /// panic, via the drop guard). Single-threaded per shard, so a plain
    /// `Rc<Cell<_>>` suffices (no atomics; shared-nothing ADR-0002).
    type LiveTasks = Rc<Cell<usize>>;

    /// RAII guard that decrements the shard's live-task count when a connection
    /// task ends, including on panic (so the drain count stays accurate).
    struct LiveGuard(LiveTasks);
    impl Drop for LiveGuard {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_sub(1));
        }
    }

    /// Run the shard set. ONE listener is bound up front and a single dedicated
    /// ACCEPTOR thread round-robins accepted connections to the shards over a
    /// per-shard channel (userspace load-balancing, portable across platforms; see
    /// the module docs for why this replaces per-shard `SO_REUSEPORT`). For each
    /// shard this spawns an OS thread that:
    ///   1. builds a current-thread tokio runtime (NOT multi-thread; ADR-0002),
    ///   2. awaits ITS connection channel for inbound `std::net::TcpStream`s handed
    ///      over by the acceptor, adopting each onto THIS shard's reactor with
    ///      `tokio::net::TcpStream::from_std` (the connection now lives on this core),
    ///   3. spawns `serve` per connection on the shard-local `LocalSet`,
    ///   4. stops taking new connections and drains in-flight tasks when the
    ///      shutdown flag is set.
    ///
    /// `serve` is cloned per shard and invoked per connection with the shard's
    /// [`TokioRuntime`], the accepted [`tokio::net::TcpStream`], the [`ShardId`],
    /// AND the shared shutdown flag (#543): a per-connection serve loop that PARKS
    /// (e.g. a pub/sub subscriber blocked on its push channel) can then race the
    /// same flag the acceptor and drain loop watch, so a graceful stop closes it
    /// PROMPTLY instead of blocking the shard-thread join until the drain grace.
    /// It returns a `'static` future (the connection task).
    ///
    /// `inboxes` hands each shard ITS OWN cross-shard inbound item by index (the
    /// coordinator's per-shard MPSC receiver, COORDINATOR.md #107): shard `index`
    /// takes `inboxes[index]` and the `drain` closure turns it into a background
    /// drain-loop future spawned on the shard's LocalSet ALONGSIDE the accept loop, so
    /// a shard processes both newly-accepted connections AND cross-shard work routed to
    /// the keys it owns. The seam is GENERIC over the item type `I` and the drain
    /// closure so this runtime layer stays free of the coordinator's concrete types
    /// (no `ShardWork`/`Receiver` naming leaks here); the binary supplies both. A
    /// length mismatch (`inboxes.len() != total`) is a wiring bug and panics.
    ///
    /// Returns a [`ShardSet`] for shutdown/join. If a shard thread fails to bind
    /// it logs to stderr and exits that thread; at least one bound shard is
    /// required for a useful server (the binary checks this separately).
    pub fn run_shards<S, Fut, I, D, DFut>(
        cfg: &ShardConfig,
        serve: S,
        inboxes: Vec<I>,
        drain: D,
    ) -> std::io::Result<ShardSet>
    where
        S: Fn(TokioRuntime, tokio::net::TcpStream, ShardId, Arc<AtomicBool>) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = ()> + 'static,
        I: Send + 'static,
        // The drain closure receives THIS shard's index (0-based), its inbox, AND the shared
        // shutdown flag, so a per-shard background task (the cross-shard drain loop, and #58
        // persistence load/save) can name its own shard (e.g. its `dump-shard-<index>.icss`
        // snapshot file) AND observe a graceful stop. The shutdown flag is the SAME `Arc<AtomicBool>`
        // [`ShardSet::shutdown_flag`] hands the signal handler, so shard 0's drain loop can drive the
        // SAVE-ON-EXIT (#139, SHUTDOWN.md) when a signal flips it, before the shard threads join. The
        // index is the same `ShardId.index` the serve closure sees, so the two agree.
        D: Fn(usize, I, Arc<AtomicBool>) -> DFut + Clone + Send + 'static,
        DFut: Future<Output = ()> + 'static,
    {
        let shutdown = Arc::new(AtomicBool::new(false));
        let total = cfg.shards.max(1);
        assert_eq!(
            inboxes.len(),
            total,
            "run_shards: one inbox per shard required (got {}, need {total})",
            inboxes.len()
        );

        // Listener binding happens below (either the single round-robin listener or, in shard-owner
        // mode, one listener per shard) -- SYNCHRONOUSLY in this call, so a bind failure (e.g. port in
        // use) surfaces as an error here rather than inside a spawned thread. `listener_for` ADOPTS a
        // systemd socket-activation inherited fd when one was passed (LISTEN_FDS, #389 -- the listen
        // queue then survives an upgrade restart with no connection-refused window), else self-binds
        // with SO_REUSEPORT. The listeners are owned by the acceptor thread(s); the shards never bind.

        // One connection channel per shard: the acceptor sends accepted
        // `std::net::TcpStream`s, the shard receives them. Unbounded so the
        // (synchronous, non-async) acceptor can hand off without blocking on a
        // shard's reactor; the hand-off is just a queued pointer, and a shard
        // that is momentarily busy buffers a few connections rather than stalling
        // the acceptor (and thus every other shard). These channels carry only the
        // raw socket, no per-key/hot-path data (shared-nothing ADR-0002 intact).
        let mut conn_senders = Vec::with_capacity(total);
        let mut conn_receivers = Vec::with_capacity(total);
        for _ in 0..total {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<std::net::TcpStream>();
            conn_senders.push(tx);
            conn_receivers.push(rx);
        }

        let mut handles = Vec::with_capacity(total + 1);

        // The RESP client listener raw fd(s), captured at bind time BEFORE the listener is moved into
        // its acceptor thread (#638): the streamed-cutover host reads these back via
        // [`ShardSet::client_listener_fds`] to hand the listen socket to a receiver sibling (no-RST
        // inherited-listener handoff). The acceptor keeps the listener OPEN, so the fd stays valid;
        // this is a non-owning integer copy, never closed here.
        #[cfg(unix)]
        let mut client_listener_fds: Vec<std::os::fd::RawFd> = Vec::with_capacity(total);

        // THE ACCEPTOR(S). A plain OS thread (no tokio runtime): a blocking `std` accept loop with a
        // shutdown-aware poll. `tokio::sync::mpsc::UnboundedSender::send` does not require a runtime,
        // so the hand-off is valid from this sync context.
        if cfg.shard_owner_ports {
            // SHARD-OWNER MODE (#517): bind ONE listener PER shard at `bind.port() + i`, each homing
            // its accepted connections on THAT shard (its own `conn_senders[i]`), so a cluster-aware
            // client that dials the owner's port lands on the key's owner shard -- no internal hop.
            // `bind_shard_owner_listeners` binds all N up front (fail fast, no half-bound state) and
            // rejects the socket-activation combo; only once every port is bound do we spawn.
            let listeners = bind_shard_owner_listeners(cfg.bind, total)?;
            for (index, (listener, sender)) in listeners.into_iter().zip(conn_senders).enumerate() {
                // Capture this shard's listener fd (#638) before the listener moves into its acceptor.
                #[cfg(unix)]
                {
                    use std::os::fd::AsRawFd as _;
                    client_listener_fds.push(listener.as_raw_fd());
                }
                let shutdown = Arc::clone(&shutdown);
                let acceptor = std::thread::Builder::new()
                    .name(format!("ironcache-acceptor-{index}"))
                    .spawn(move || single_shard_acceptor_loop(&listener, &sender, &shutdown))?;
                handles.push(acceptor);
            }
        } else {
            // DEFAULT: one listener, round-robin each accepted connection to the next shard's channel.
            let listener = listener_for(cfg.bind)?;
            // Capture the single client-listener fd (#638) before the listener moves into the acceptor.
            #[cfg(unix)]
            {
                use std::os::fd::AsRawFd as _;
                client_listener_fds.push(listener.as_raw_fd());
            }
            let shutdown = Arc::clone(&shutdown);
            let acceptor = std::thread::Builder::new()
                .name("ironcache-acceptor".to_string())
                .spawn(move || acceptor_loop(&listener, &conn_senders, &shutdown))?;
            handles.push(acceptor);
        }

        // Hand each shard its own inbox by moving items OUT of the vec by index. The
        // vec is consumed (into_iter) so each `I` is owned by exactly one shard thread.
        for ((index, inbox), conn_rx) in inboxes.into_iter().enumerate().zip(conn_receivers) {
            let shutdown = Arc::clone(&shutdown);
            // A second clone of the same flag for the drain loop: shard 0's drain loop watches it to
            // drive the SAVE-ON-EXIT (#139) on a graceful stop, alongside the serve loop's own watch.
            let drain_shutdown = Arc::clone(&shutdown);
            let serve = serve.clone();
            let drain = drain.clone();
            let shard = ShardId { index, total };
            let handle = std::thread::Builder::new()
                .name(format!("ironcache-shard-{index}"))
                .spawn(move || {
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            eprintln!("shard {index}: failed to build runtime: {e}");
                            return;
                        }
                    };
                    let local = tokio::task::LocalSet::new();
                    // Catch a panic escaping the serve loop so the thread logs it
                    // (and bumps a per-shard shard_died counter for future
                    // OBSERVABILITY wiring) before it exits, instead of unwinding
                    // silently. The panic is then resumed so `join()` still surfaces
                    // it to `shutdown_and_join` (which no longer discards it).
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        local.block_on(&rt, async move {
                            // Spawn the cross-shard DRAIN LOOP on this shard's LocalSet
                            // BEFORE the serve loop (COORDINATOR.md #107): a shard can
                            // own keys and must service remote work even if it never
                            // accepts a connection. The serve loop below then runs for
                            // the shard's lifetime; the drain loop runs concurrently on
                            // the same single-threaded LocalSet (interleaved, never
                            // parallel, so the shard-local RefCells stay single-threaded).
                            let drain_task =
                                tokio::task::spawn_local(drain(index, inbox, drain_shutdown));
                            serve_loop(conn_rx, &serve, shard, &shutdown).await;
                            // GRACEFUL DRAIN-TASK JOIN (#139, SHUTDOWN.md): the serve loop has
                            // returned, so the shard is stopping. AWAIT the drain task before this
                            // `block_on` returns (which would otherwise DROP the fire-and-forget drain
                            // task the instant the serve loop ends, cancelling a half-run save-on-exit
                            // on shard 0). The drain loop now watches the shutdown flag and RETURNS
                            // promptly on a graceful stop (shard 0 runs its save then `exit(0)`s, the
                            // others finish a brief bounded post-flag drain), so this join adds no
                            // steady-state shutdown latency. It is still BOUNDED by the SAME
                            // [`DRAIN_GRACE`] as a final backstop: a wedged drain task can never trap
                            // shutdown -- on the deadline we proceed and the drop cancels whatever is
                            // left (the prior committed snapshot stays valid).
                            let drain_grace = tokio::time::sleep(DRAIN_GRACE);
                            tokio::pin!(drain_grace);
                            tokio::select! {
                                _ = drain_task => {}
                                () = &mut drain_grace => {
                                    eprintln!(
                                        "shard {index}: drain task did not finish within the grace \
                                         window; proceeding with shutdown"
                                    );
                                }
                            }
                        });
                    }));
                    if let Err(panic) = result {
                        // shard_died counter: PR-1 has no metrics registry yet, so
                        // this is a local tally logged on the way out; the registry
                        // wiring (OBSERVABILITY.md #152) reads it later.
                        let shard_died: u64 = 1;
                        eprintln!(
                            "shard {index}: serve loop panicked (shard_died={shard_died}); \
                             shard thread exiting"
                        );
                        std::panic::resume_unwind(panic);
                    }
                })?;
            handles.push(handle);
        }

        Ok(ShardSet {
            shutdown,
            handles,
            #[cfg(unix)]
            client_listener_fds,
        })
    }

    /// The single acceptor's loop: accept on the one listener and round-robin each
    /// connection to a shard's channel. Runs on a dedicated OS thread with NO tokio
    /// runtime (plain blocking `std` accept).
    ///
    /// The listener is set non-blocking so the loop can observe the shutdown flag
    /// between polls instead of parking forever in `accept()` while no connection
    /// arrives: on `WouldBlock` it sleeps briefly (a 1ms poll, not a hot spin) and
    /// re-checks shutdown. On shutdown it stops accepting and returns, which drops
    /// every shard sender; each shard's `recv()` then observes channel-closed and
    /// proceeds to drain (SHUTDOWN.md).
    fn acceptor_loop(
        listener: &std::net::TcpListener,
        conn_senders: &[tokio::sync::mpsc::UnboundedSender<std::net::TcpStream>],
        shutdown: &Arc<AtomicBool>,
    ) {
        // Non-blocking so a quiet listener cannot keep us from seeing shutdown.
        if let Err(e) = listener.set_nonblocking(true) {
            eprintln!("acceptor: set_nonblocking failed: {e}; shutdown may be delayed");
        }
        let poll = Duration::from_millis(1);
        let mut next: usize = 0;
        let n = conn_senders.len().max(1);
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _peer)) => {
                    // Disable Nagle here so it is set regardless of which shard
                    // adopts the socket; request/reply caches want low latency.
                    let _ = stream.set_nodelay(true);
                    // Round-robin to the next shard. A plain integer counter (NOT
                    // rand): deterministic spread, no entropy needed.
                    let target = next % n;
                    next = next.wrapping_add(1);
                    // If a shard thread is gone (its receiver dropped) the send
                    // fails; skip that connection rather than crash the acceptor.
                    if let Err(e) = conn_senders[target].send(stream) {
                        eprintln!("acceptor: shard {target} channel closed: {e}");
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No pending connection: nap briefly, then re-check shutdown.
                    std::thread::sleep(poll);
                }
                Err(e) => {
                    // Transient accept errors (e.g. EMFILE) should not kill the
                    // acceptor; back off briefly and continue.
                    eprintln!("acceptor: accept error: {e}");
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        // Returning drops `conn_senders`, closing every shard channel so the shard
        // serve loops observe channel-closed and move on to drain.
    }

    /// SHARD-OWNER binding (#517): bind ONE listener per shard at `bind.port() + i`, in shard order.
    ///
    /// Binds ALL N up front so a mid-list port conflict/overflow fails boot BEFORE any acceptor
    /// thread is spawned -- no orphaned acceptors left running on the ports that did bind. Rejects
    /// systemd socket activation (`LISTEN_FDS`, #389): activation hands ONE inherited fd for ONE
    /// port, but this mode needs N DISTINCT self-bound ports; adopting the one fd N times (as
    /// `listener_for` would, since it ignores its `addr` under activation) would alias it (a
    /// multi-close-unsound double free on shutdown) AND leave ports `base+1..` unbound. The two are
    /// fundamentally incompatible, so reject the combo loudly (mirroring the io_uring guard in
    /// `Config::validate`). A malformed activation env -> `Err` -> treated as inactive, and
    /// `listener_for` then self-binds, which is correct here.
    fn bind_shard_owner_listeners(
        bind: std::net::SocketAddr,
        total: usize,
    ) -> std::io::Result<Vec<std::net::TcpListener>> {
        if crate::listen_fds::from_env()
            .map(|fds| !fds.is_empty())
            .unwrap_or(false)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "shard-owners mode is incompatible with systemd socket activation (LISTEN_FDS): it \
                 needs N distinct self-bound ports, but activation supplies one inherited socket for \
                 one port",
            ));
        }
        let mut listeners = Vec::with_capacity(total);
        for index in 0..total {
            let offset = u16::try_from(index).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "shard index exceeds u16")
            })?;
            let port = bind.port().checked_add(offset).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "shard-owner port {} + shard {index} overflows u16 (lower the base port or \
                         the shard count)",
                        bind.port()
                    ),
                )
            })?;
            listeners.push(listener_for(std::net::SocketAddr::new(bind.ip(), port))?);
        }
        Ok(listeners)
    }

    /// SHARD-OWNER acceptor (#517): owns ONE per-shard listener and hands EVERY accepted connection
    /// to a SINGLE shard's channel (no round-robin) -- so a connection dialed to shard `i`'s port
    /// homes on shard `i`. Otherwise identical to [`acceptor_loop`] (blocking `std` accept with a
    /// shutdown-aware poll, Nagle off, back-off on transient errors). When `sender` is gone (the shard
    /// thread exited) the send fails and the connection is dropped rather than crashing the acceptor.
    fn single_shard_acceptor_loop(
        listener: &std::net::TcpListener,
        sender: &tokio::sync::mpsc::UnboundedSender<std::net::TcpStream>,
        shutdown: &Arc<AtomicBool>,
    ) {
        if let Err(e) = listener.set_nonblocking(true) {
            eprintln!("shard-owner acceptor: set_nonblocking failed: {e}; shutdown may be delayed");
        }
        let poll = Duration::from_millis(1);
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _peer)) => {
                    let _ = stream.set_nodelay(true);
                    if let Err(e) = sender.send(stream) {
                        eprintln!("shard-owner acceptor: shard channel closed: {e}");
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(poll);
                }
                Err(e) => {
                    eprintln!("shard-owner acceptor: accept error: {e}");
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        // Returning drops `sender`, closing this shard's channel so its serve loop drains.
    }

    /// The shard's serve loop: instead of accepting, it AWAITS its connection
    /// channel for `std::net::TcpStream`s handed over by the acceptor, adopts each
    /// onto THIS shard's tokio reactor, and spawns `serve` per connection on the
    /// shard-local `LocalSet`. Runs concurrently with the drain loop on the same
    /// single-threaded executor.
    async fn serve_loop<S, Fut>(
        mut conn_rx: tokio::sync::mpsc::UnboundedReceiver<std::net::TcpStream>,
        serve: &S,
        shard: ShardId,
        shutdown: &Arc<AtomicBool>,
    ) where
        S: Fn(TokioRuntime, tokio::net::TcpStream, ShardId, Arc<AtomicBool>) -> Fut
            + Clone
            + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        // Core-local count of in-flight connection tasks, for the bounded drain.
        let live: LiveTasks = Rc::new(Cell::new(0));

        while !shutdown.load(Ordering::Relaxed) {
            // Race the channel recv against a short timer so a shutdown is observed
            // even when no new connection arrives (the acceptor also closes the
            // channel on shutdown, which `recv()` reports as `None`).
            tokio::select! {
                maybe = conn_rx.recv() => {
                    match maybe {
                        Some(std_stream) => {
                            // Adopt the connection onto THIS shard's reactor: the
                            // socket must be non-blocking for tokio, then `from_std`
                            // registers it with this thread's runtime so all of its
                            // I/O readiness lives on this core (ADR-0002). That
                            // registration is the whole point of the userspace
                            // hand-off: it distributes connections across cores.
                            if let Err(e) = std_stream.set_nonblocking(true) {
                                eprintln!("shard {}: set_nonblocking failed: {e}; dropping connection", shard.index);
                                continue;
                            }
                            let stream = match tokio::net::TcpStream::from_std(std_stream) {
                                Ok(s) => s,
                                Err(e) => {
                                    eprintln!("shard {}: from_std failed: {e}; dropping connection", shard.index);
                                    continue;
                                }
                            };
                            // Hand the per-connection serve closure a clone of the SAME shutdown
                            // flag the acceptor + drain loop watch (#543), so a serve loop that
                            // parks (e.g. a subscriber idle-waiting on its push channel) can race
                            // it and close promptly on a graceful stop rather than lingering until
                            // the drain grace elapses.
                            let fut = serve(TokioRuntime::new(), stream, shard, Arc::clone(shutdown));
                            // Track this connection for the drain: bump the live
                            // count, and decrement via a drop guard when the task
                            // ends (including on panic).
                            live.set(live.get() + 1);
                            let guard = LiveGuard(Rc::clone(&live));
                            // Pin to this shard's LocalSet: the connection lives
                            // its whole life on this core (ADR-0002).
                            tokio::task::spawn_local(async move {
                                let _guard = guard;
                                fut.await;
                            });
                        }
                        None => {
                            // The acceptor dropped its sender (shutdown). Stop taking
                            // new connections and fall through to the drain.
                            break;
                        }
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }

        // Shutdown observed: stop taking new connections (loop exited) and drain
        // in-flight connection tasks up to the grace deadline. We poll the live
        // count on a short tick rather than collecting JoinHandles, which keeps this
        // O(1) in bookkeeping and works with the fire-and-forget spawn_local model.
        drain_live_tasks(&live, shard).await;
    }

    /// Await the shard's in-flight connection tasks until the live count reaches
    /// zero or the [`DRAIN_GRACE`] window elapses, then return. Bounded by design
    /// (SHUTDOWN.md): a slow/stuck client cannot block shutdown forever.
    async fn drain_live_tasks(live: &LiveTasks, shard: ShardId) {
        if live.get() == 0 {
            return;
        }
        let deadline = tokio::time::Instant::now() + DRAIN_GRACE;
        let tick = Duration::from_millis(20);
        while live.get() > 0 {
            if tokio::time::Instant::now() >= deadline {
                eprintln!(
                    "shard {}: drain grace elapsed with {} connection task(s) still live; \
                     proceeding with shutdown",
                    shard.index,
                    live.get()
                );
                break;
            }
            // Yield to the LocalSet so the in-flight connection tasks make progress
            // and their drop guards decrement the live count.
            tokio::time::sleep(tick).await;
        }
    }
}

#[cfg(feature = "tokio")]
pub use tokio_bootstrap::run_shards;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn available_shards_is_at_least_one() {
        assert!(available_shards() >= 1);
    }

    #[test]
    fn shard_id_fields() {
        let s = ShardId { index: 2, total: 4 };
        assert_eq!(s.index, 2);
        assert_eq!(s.total, 4);
    }

    /// #638 PR-1: [`ShardSet::client_listener_fds`] exposes the RESP client listener fd captured at
    /// bind time, and the listener is STILL ALIVE (adoptable) -- the acceptor keeps it open, so a
    /// later cutover host can hand it to a receiver sibling for the no-RST inherited-listener handoff.
    #[cfg(all(unix, feature = "tokio"))]
    #[test]
    fn client_listener_fds_exposes_a_live_listener() {
        use crate::TokioRuntime;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        // Bind an ephemeral loopback port (0) so the test never collides with a fixed port.
        let cfg = ShardConfig {
            shards: 1,
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            shard_owner_ports: false,
        };
        let serve = |_rt: TokioRuntime,
                     _s: tokio::net::TcpStream,
                     _shard: ShardId,
                     _sd: Arc<AtomicBool>| async {};
        let drain = |_idx: usize, _inbox: (), _sd: Arc<AtomicBool>| async {};
        let set = run_shards(&cfg, serve, vec![()], drain).expect("run_shards binds one listener");

        let fds = set.client_listener_fds();
        assert_eq!(
            fds.len(),
            1,
            "the default single-acceptor mode exposes exactly one client listener fd"
        );
        let fd = fds[0];
        assert!(fd >= 0, "the exposed value is a real file descriptor");
        // The listener is STILL ALIVE / adoptable: the acceptor thread holds it open, so F_GETFD
        // succeeds on the fd we handed out (a closed fd would return EBADF, i.e. < 0).
        // SAFETY: F_GETFD only reads the descriptor's flags; it neither takes ownership of the fd
        // nor mutates the socket, so it cannot disturb the acceptor that owns it.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert!(
            flags >= 0,
            "the exposed client-listener fd is live (F_GETFD ok), so a sibling can inherit it"
        );

        set.shutdown_and_join().expect("clean shutdown");
    }
}
