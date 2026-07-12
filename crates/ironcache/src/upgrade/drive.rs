// SPDX-License-Identifier: MIT OR Apache-2.0
//! The OPT-IN WIRING for the streamed upgrade handoff (#391 Phase 2c): the layer that connects the
//! merged, data-safe transport core ([`crate::upgrade::stream`]) to a REAL AF_UNIX socket
//! rendezvous, wraps every leg in a CALLER-side deadline, and gathers the per-shard outcomes into
//! the cross-shard [`stream::CutoverBarrier`] flip decision.
//!
//! ## What this lands (correct + tested)
//!
//! - The OPT-IN GATE: [`HandoffPlan::from_config`] is `Some` ONLY when `handoff_socket` is
//!   configured. With NO `handoff_socket` it is `None`, so the DEFAULT upgrade path (the #390 tmpfs
//!   / durable `SAVE` -> reload, [`crate::persist`] + [`crate::handoff`]) is BYTE-UNCHANGED -- the
//!   streamed handoff is never consulted, no socket is bound, no code on the default boot/hot path
//!   changes. This is the paramount property: streamed handoff is used ONLY when explicitly enabled.
//! - The REAL-SOCKET RENDEZVOUS: [`bind_handoff_listener`] (the OLD/sender process binds the
//!   node-local well-known socket), [`accept_handoff`] (accept one stream per shard), and
//!   [`connect_handoff`] (the NEW/receiver sibling connects, retrying while the sender is still
//!   coming up). This is a real `tokio::net::UnixListener` / `UnixStream` pair -- the SAME transport
//!   two real processes use -- not the in-test `UnixStream::pair` the core unit tests drive.
//! - The CALLER-OWNED TIMEOUT: every leg ([`send_bulk_timed`] / [`send_cutover_timed`] /
//!   [`recv_bulk_timed`] / [`recv_cutover_timed`] / the convenience [`send_shard_timed`] /
//!   [`recv_shard_timed`], plus the rendezvous) is wrapped in `tokio::time::timeout`, so a HUNG or
//!   wedged peer ABORTS with [`stream::HandoffError::Timeout`] rather than hanging the upgrade. The
//!   transport core flagged the timeout as caller-owned; this is that caller.
//! - The CROSS-SHARD FLIP: [`barrier_from_results`] folds the per-shard results into the
//!   ALL-OR-NOTHING [`stream::CutoverState`] -- Commit iff EVERY shard handed off, Abort the instant
//!   any shard failed (sticky, fail-closed).
//!
//! ## Abort-safety (the #1 rule, inherited from the core)
//!
//! The SENDER only READS its store (the core `send_*` never mutates or drops it), so on ANY failure
//! -- a socket error, a delta overflow, a peer abort, OR a caller TIMEOUT -- the OLD process's data
//! is intact and it keeps serving; the durable `data_dir` snapshot is never touched and stays a
//! valid fallback. A `tokio::time::timeout` that fires simply DROPS the in-flight send/recv future
//! (cancellation): the sender's dropped socket surfaces to the peer as an EOF (which the peer treats
//! as an abort), and the receiver's dropped partial store is never adopted. There is no code path
//! here that can lose an acknowledged write or serve a half-loaded store.
//!
//! ## Deferred to the follow-up (why this is "Part of #391", not "Closes")
//!
//! The FINAL live-traffic serve-flip is deliberately NOT wired here, because each piece touches the
//! live datapath / process lifecycle and must carry its own data-loss-focused review, and none can
//! be made correct AND tested in-harness without two real sharded processes:
//!
//! - The RECEIVER boot substitution: the new process's per-shard drain loop
//!   ([`crate::coordinator::run_drain_loop`]) pulling over the socket INTO its thread-local
//!   `Rc<RefCell<ShardStoreImpl>>` instead of `load_shard_on_boot`, and adopting only on the fully
//!   cutover-acked path.
//! - The SENDER freeze: driving `send_bulk` from a #588 frozen Arc-COW view so the old shard keeps
//!   serving DURING the bulk (the core `send_bulk` takes a `&ShardStore` borrow for the whole scan;
//!   the non-blocking frozen-view integration is the deferred part).
//! - The dispatch `-LOADING` write-QUIESCE across shards for the final delta cut, and the
//!   cross-thread coordination that stops an old shard ONLY after the barrier commits across ALL
//!   shards (there is no per-shard loading flag today; only a node-wide `CLIENT PAUSE WRITE`).
//! - The acceptor DRAIN-AND-FINAL-ACCEPT for the non-socket-activated `SO_REUSEPORT` case (systemd
//!   socket-activation, #389, is the supported no-RST path: the listener fd is inherited, never
//!   closed across the handoff, so no accept backlog is orphaned).
//! - The ORCHESTRATOR spawning the sibling process, passing it the socket path + the receive role,
//!   and the old draining in-flight + exiting on Commit / resuming on Abort.
//!
//! ## Determinism (ADR-0003)
//!
//! Off the engine decision path: this reads no engine clock and no RNG. The `now` fed to the
//! snapshot / delta is the caller's clock (the `ironcache-env` seam), and the timeout is driven by
//! the tokio timer, exactly as the transport core and the persistence save path already require.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

use ironcache_config::{Config, HandoffRole};
use ironcache_repl::{ReplId, ReplOffset, ReplRing};
use ironcache_storage::{AccountingHook, EvictionHook, UnixMillis};
use ironcache_store::ShardStore;

use super::stream::{self, CutoverBarrier, CutoverState, HandoffError, LoadedShard};

/// The default per-leg deadline the wiring wraps each handoff phase in when the operator does not
/// override it. Sized generously for a healthy multi-GB bulk stream over a local unix socket yet far
/// under any supervisor's hard-kill grace, so a genuinely wedged peer aborts the upgrade (and falls
/// back to the durable path) rather than hanging. Driven by the tokio timer (ADR-0003: an ops-path
/// deadline, not an engine clock read).
pub const DEFAULT_HANDOFF_TIMEOUT: Duration = Duration::from_secs(30);

/// The default bulk chunk size (keys per [`stream::send_bulk`] snapshot pull): the constant-memory
/// borrow discipline pulls this many entries under the store borrow, releases it, then ships them,
/// so peak transfer memory is one chunk regardless of keyspace size.
pub const DEFAULT_HANDOFF_CHUNK_MAX: usize = 256;

/// The retry cadence [`connect_handoff`] polls on while the sender's listener is not yet bound (the
/// spawned sibling connects before the old process has finished binding the socket). Bounded overall
/// by the caller's connect timeout. Coarse: this is a one-shot boot rendezvous, never a hot path.
#[cfg(unix)]
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(20);

/// The resolved, OPT-IN plan for a streamed handoff: the rendezvous socket, the per-leg deadline, and
/// the bulk chunk size. Produced ONLY when a `handoff_socket` is configured ([`Self::from_config`]);
/// `None` is the single gate that keeps the default #390/durable upgrade path byte-unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffPlan {
    /// The node-local unix socket the old (sender) process binds and the new (receiver) sibling
    /// connects to. A well-known path both ends agree on (the operator's `handoff_socket`).
    pub socket: PathBuf,
    /// The caller-side deadline each handoff leg is wrapped in ([`DEFAULT_HANDOFF_TIMEOUT`]).
    pub timeout: Duration,
    /// The bulk snapshot chunk size ([`DEFAULT_HANDOFF_CHUNK_MAX`]).
    pub chunk_max: usize,
}

impl HandoffPlan {
    /// Resolve the streamed-handoff plan from the config, or `None` when NO `handoff_socket` is set.
    ///
    /// `None` is the OPT-IN gate: the caller that gets `None` MUST take the unchanged default path
    /// (the #390 tmpfs / durable `SAVE` -> reload). Only a `Some` plan ever binds a socket or streams
    /// state, so a node without `handoff_socket` configured behaves exactly as before this wiring.
    #[must_use]
    pub fn from_config(config: &Config) -> Option<Self> {
        let socket = config.handoff_socket.clone()?;
        Some(HandoffPlan {
            socket,
            timeout: DEFAULT_HANDOFF_TIMEOUT,
            chunk_max: DEFAULT_HANDOFF_CHUNK_MAX,
        })
    }

    /// The RECEIVER-role gate (#391 PR-2): resolve a handoff plan ONLY when this process was booted
    /// as the streamed-handoff RECEIVER, i.e. a `handoff_socket` is set AND
    /// `handoff_role == receiver`. `None` in every other case -- no socket, or the default
    /// [`HandoffRole::Sender`] role -- so the receiver boot-substitution in
    /// [`crate::coordinator::run_drain_loop`] is skipped and the shard loads from disk exactly as
    /// today. This is the single branch that decides receive-over-socket vs. load-from-disk at boot.
    #[must_use]
    pub fn receiver_from_config(config: &Config) -> Option<Self> {
        if config.handoff_role != HandoffRole::Receiver {
            return None;
        }
        Self::from_config(config)
    }
}

// ---------------------------------------------------------------------------------------------
// Real-socket rendezvous. The OLD (sender) process is the LISTENER (it owns the node-local path
// and is already running); the NEW (receiver) sibling CONNECTS once per shard.
// ---------------------------------------------------------------------------------------------

/// SENDER side: bind the node-local handoff socket, removing any stale socket file a crashed prior
/// handoff left (the path is well-known, not pid-scoped, so a leftover file would `EADDRINUSE`).
///
/// This binds ONLY the private handoff rendezvous socket; it does NOT touch the client listener, so
/// it cannot orphan a client accept backlog (the client listener stays owned by the acceptor / the
/// inherited socket-activation fd, #389). Errors map to [`HandoffError::Io`]; on any error the caller
/// aborts the streamed handoff and keeps serving on the durable path.
///
/// # Errors
/// [`HandoffError::Io`] if the socket cannot be bound.
#[cfg(unix)]
pub fn bind_handoff_listener(path: &Path) -> Result<UnixListener, HandoffError> {
    // A leftover socket FILE from a crashed prior handoff would make `bind` fail with EADDRINUSE;
    // removing it is safe because the path is a dedicated, node-local handoff rendezvous (never a
    // client-facing or data path). A genuinely-in-use path (a concurrent handoff) still fails the
    // bind below after the unlink, which the caller surfaces as an abort.
    let _ = std::fs::remove_file(path);
    UnixListener::bind(path).map_err(|e| HandoffError::Io(e.to_string()))
}

/// SENDER side: accept ONE per-shard stream from the receiver, bounded by `timeout` so a receiver
/// that never connects aborts the handoff rather than hanging the old process.
///
/// # Errors
/// [`HandoffError::Timeout`] if no connection arrives before the deadline; [`HandoffError::Io`] on an
/// accept failure.
#[cfg(unix)]
pub async fn accept_handoff(
    listener: &UnixListener,
    timeout: Duration,
) -> Result<UnixStream, HandoffError> {
    match tokio::time::timeout(timeout, listener.accept()).await {
        Ok(Ok((stream, _addr))) => Ok(stream),
        Ok(Err(e)) => Err(HandoffError::Io(e.to_string())),
        Err(_elapsed) => Err(HandoffError::Timeout { phase: "accept" }),
    }
}

/// RECEIVER side: connect to the sender's handoff socket, RETRYING while the socket is not yet bound
/// (the spawned sibling races the old process's bind), bounded overall by `timeout`.
///
/// A `NotFound` / `ConnectionRefused` is the not-yet-bound case and is retried on
/// [`CONNECT_RETRY_INTERVAL`]; any other I/O error is a hard failure. The whole retry loop is wrapped
/// in one `tokio::time::timeout`, so a sender that never comes up aborts the receiver (it exits
/// without serving) rather than spinning forever.
///
/// # Errors
/// [`HandoffError::Timeout`] if the socket never becomes connectable before the deadline;
/// [`HandoffError::Io`] on a non-retryable connect error.
#[cfg(unix)]
pub async fn connect_handoff(path: &Path, timeout: Duration) -> Result<UnixStream, HandoffError> {
    let connect = async {
        loop {
            match UnixStream::connect(path).await {
                Ok(stream) => return Ok::<UnixStream, HandoffError>(stream),
                // The listener may not be bound yet (the old process is still coming up / binding):
                // retry until it appears or the outer deadline fires.
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    tokio::time::sleep(CONNECT_RETRY_INTERVAL).await;
                }
                // A genuine, non-transient error (bad path perms, etc.): abort.
                Err(e) => return Err(HandoffError::Io(e.to_string())),
            }
        }
    };
    match tokio::time::timeout(timeout, connect).await {
        Ok(res) => res,
        Err(_elapsed) => Err(HandoffError::Timeout { phase: "connect" }),
    }
}

/// Derive the PER-SHARD handoff socket path from a base: `<base>.<shard_index>` (#391/#638).
///
/// A multi-shard streamed live-cutover rendezvouses ONE unix socket PER shard so shard `i`'s tokio
/// stream is bound + accepted ON shard `i`'s runtime and never crosses a thread (deterministic
/// i<->i pairing, no central accept). This is the SINGLE place that derivation lives, so the
/// sender's per-shard bind and the receiver's per-shard connect always agree. The existing
/// single-path callers keep passing the base path directly and are unaffected.
#[cfg(unix)]
#[must_use]
pub fn per_shard_handoff_path(base: &Path, shard_index: usize) -> PathBuf {
    // Append `.<i>` to the WHOLE base path (not just its file name), so `/run/ic/handoff.sock`
    // becomes `/run/ic/handoff.sock.0`; the base stays a well-known, node-local rendezvous.
    let mut raw = base.as_os_str().to_owned();
    raw.push(format!(".{shard_index}"));
    PathBuf::from(raw)
}

/// SENDER side: bind the per-shard handoff socket `<base>.<shard_index>` (see
/// [`per_shard_handoff_path`]). Delegates to [`bind_handoff_listener`] on the derived path, so the
/// stale-file cleanup + [`HandoffError::Io`] mapping are identical to the single-path bind.
///
/// # Errors
/// [`HandoffError::Io`] if the per-shard socket cannot be bound.
#[cfg(unix)]
pub fn bind_handoff_listener_for_shard(
    base: &Path,
    shard_index: usize,
) -> Result<UnixListener, HandoffError> {
    bind_handoff_listener(&per_shard_handoff_path(base, shard_index))
}

/// RECEIVER side: connect to the per-shard handoff socket `<base>.<shard_index>` (see
/// [`per_shard_handoff_path`]), retrying until it is bound, bounded by `timeout`. Delegates to
/// [`connect_handoff`] on the derived path so the retry + timeout behavior is identical.
///
/// # Errors
/// [`HandoffError::Timeout`] if the per-shard socket never becomes connectable before the deadline;
/// [`HandoffError::Io`] on a non-retryable connect error.
#[cfg(unix)]
pub async fn connect_handoff_for_shard(
    base: &Path,
    shard_index: usize,
    timeout: Duration,
) -> Result<UnixStream, HandoffError> {
    connect_handoff(&per_shard_handoff_path(base, shard_index), timeout).await
}

// ---------------------------------------------------------------------------------------------
// Caller-side TIMEOUT wrappers around every transport leg. A fired timeout DROPS (cancels) the
// in-flight future: the sender is read-only so the OLD store is intact and it keeps serving; the
// receiver's partial fresh store is dropped (adopt nothing). The dropped socket surfaces to the
// peer as an EOF, which the peer treats as an abort.
// ---------------------------------------------------------------------------------------------

/// [`stream::send_bulk`] under a caller deadline. On timeout the OLD store is untouched (read-only),
/// so the old process keeps serving; the receiver sees the dropped socket and aborts.
///
/// # Errors
/// Any [`HandoffError`] from the bulk phase, or [`HandoffError::Timeout`] if it exceeds `timeout`.
#[allow(clippy::too_many_arguments)]
pub async fn send_bulk_timed<E, A, S>(
    stream: &mut S,
    store: &ShardStore<E, A>,
    ring: &Rc<RefCell<ReplRing>>,
    shard: u32,
    replid: ReplId,
    now: UnixMillis,
    chunk_max: usize,
    timeout: Duration,
) -> Result<ReplOffset, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::time::timeout(
        timeout,
        stream::send_bulk(stream, store, ring, shard, replid, now, chunk_max),
    )
    .await
    {
        Ok(res) => res,
        Err(_elapsed) => Err(HandoffError::Timeout { phase: "send-bulk" }),
    }
}

/// [`stream::send_cutover`] under a caller deadline. On timeout the OLD store is untouched.
///
/// # Errors
/// Any [`HandoffError`] from the cutover phase, or [`HandoffError::Timeout`] if it exceeds `timeout`.
pub async fn send_cutover_timed<S>(
    stream: &mut S,
    ring: &Rc<RefCell<ReplRing>>,
    end_offset: ReplOffset,
    chunk_max: usize,
    timeout: Duration,
) -> Result<ReplOffset, HandoffError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::time::timeout(
        timeout,
        stream::send_cutover(stream, ring, end_offset, chunk_max),
    )
    .await
    {
        Ok(res) => res,
        Err(_elapsed) => Err(HandoffError::Timeout {
            phase: "send-cutover",
        }),
    }
}

/// [`stream::recv_bulk`] under a caller deadline. On timeout the partial fresh store is dropped
/// (adopt nothing) and the receiver exits without serving.
///
/// # Errors
/// Any [`HandoffError`] from the bulk phase, or [`HandoffError::Timeout`] if it exceeds `timeout`.
pub async fn recv_bulk_timed<E, A, S, M>(
    stream: &mut S,
    make_store: M,
    expected_databases: u32,
    now: UnixMillis,
    timeout: Duration,
) -> Result<(ShardStore<E, A>, u32, ReplOffset), HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
    M: FnMut() -> ShardStore<E, A>,
{
    match tokio::time::timeout(
        timeout,
        stream::recv_bulk(stream, make_store, expected_databases, now),
    )
    .await
    {
        Ok(res) => res,
        Err(_elapsed) => Err(HandoffError::Timeout { phase: "recv-bulk" }),
    }
}

/// [`stream::recv_cutover`] under a caller deadline. On timeout the owned store is dropped (adopt
/// nothing).
///
/// # Errors
/// Any [`HandoffError`] from the cutover phase, or [`HandoffError::Timeout`] if it exceeds `timeout`.
pub async fn recv_cutover_timed<E, A, S>(
    stream: &mut S,
    store: ShardStore<E, A>,
    shard: u32,
    end_offset: ReplOffset,
    now: UnixMillis,
    timeout: Duration,
) -> Result<LoadedShard<E, A>, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::time::timeout(
        timeout,
        stream::recv_cutover(stream, store, shard, end_offset, now),
    )
    .await
    {
        Ok(res) => res,
        Err(_elapsed) => Err(HandoffError::Timeout {
            phase: "recv-cutover",
        }),
    }
}

/// The whole SENDER side for one shard (bulk + cutover, no live quiesce between -- the simple
/// no-concurrent-writes case) under a SINGLE caller deadline covering both phases.
///
/// # Errors
/// Any [`HandoffError`] from either phase, or [`HandoffError::Timeout`] on the deadline.
#[allow(clippy::too_many_arguments)]
pub async fn send_shard_timed<E, A, S>(
    stream: &mut S,
    store: &ShardStore<E, A>,
    ring: &Rc<RefCell<ReplRing>>,
    shard: u32,
    replid: ReplId,
    now: UnixMillis,
    chunk_max: usize,
    timeout: Duration,
) -> Result<ReplOffset, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::time::timeout(
        timeout,
        stream::send_shard(stream, store, ring, shard, replid, now, chunk_max),
    )
    .await
    {
        Ok(res) => res,
        Err(_elapsed) => Err(HandoffError::Timeout {
            phase: "send-shard",
        }),
    }
}

/// The whole RECEIVER side for one shard (bulk + cutover) under a SINGLE caller deadline. On any
/// error (including the timeout) the partial fresh store is dropped: adopt nothing.
///
/// # Errors
/// Any [`HandoffError`] from either phase, or [`HandoffError::Timeout`] on the deadline.
pub async fn recv_shard_timed<E, A, S, M>(
    stream: &mut S,
    make_store: M,
    expected_databases: u32,
    now: UnixMillis,
    timeout: Duration,
) -> Result<LoadedShard<E, A>, HandoffError>
where
    E: EvictionHook,
    A: AccountingHook,
    S: AsyncRead + AsyncWrite + Unpin,
    M: FnMut() -> ShardStore<E, A>,
{
    match tokio::time::timeout(
        timeout,
        stream::recv_shard(stream, make_store, expected_databases, now),
    )
    .await
    {
        Ok(res) => res,
        Err(_elapsed) => Err(HandoffError::Timeout {
            phase: "recv-shard",
        }),
    }
}

// ---------------------------------------------------------------------------------------------
// Cross-shard flip decision.
// ---------------------------------------------------------------------------------------------

/// Fold the per-shard handoff results into the ALL-OR-NOTHING cross-shard [`CutoverState`] via the
/// pure [`CutoverBarrier`]: [`CutoverState::Commit`] iff EVERY shard succeeded, and
/// [`CutoverState::Abort`] the instant ANY shard failed (sticky, fail-closed). An empty slice is a
/// degenerate empty handoff that commits immediately (nothing to transfer).
///
/// The caller uses this ONE decision to flip: on Commit the receiver adopts every shard and serves
/// (and the old stops serving); on Abort the receiver adopts NOTHING and the old keeps serving every
/// shard -- so no key is ever served by neither or both processes.
#[must_use]
pub fn barrier_from_results<T>(results: &[Result<T, HandoffError>]) -> CutoverState {
    let mut barrier = CutoverBarrier::new(results.len());
    for result in results {
        match result {
            Ok(_) => barrier.record_commit(),
            Err(_) => barrier.record_abort(),
        }
    }
    barrier.state()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The OPT-IN gate: NO `handoff_socket` -> `None` (the default #390/durable upgrade path is
    /// untouched); a configured socket -> a `Some` plan carrying it. This is the single property that
    /// guarantees a node without the knob set behaves exactly as before this wiring.
    #[test]
    fn plan_is_none_without_socket_and_some_with_it() {
        let default_cfg = Config::default();
        assert!(
            HandoffPlan::from_config(&default_cfg).is_none(),
            "no handoff_socket -> no plan -> the default #390/durable upgrade path is byte-unchanged"
        );

        let enabled = Config {
            handoff_socket: Some(PathBuf::from("/run/ironcache/handoff.sock")),
            ..Config::default()
        };
        let plan = HandoffPlan::from_config(&enabled).expect("a configured socket yields a plan");
        assert_eq!(plan.socket, PathBuf::from("/run/ironcache/handoff.sock"));
        assert_eq!(plan.timeout, DEFAULT_HANDOFF_TIMEOUT);
        assert_eq!(plan.chunk_max, DEFAULT_HANDOFF_CHUNK_MAX);
    }

    /// The RECEIVER-role gate (#391 PR-2): `receiver_from_config` is `Some` ONLY when a socket is set
    /// AND the role is `receiver`. The default role (`sender`) and the no-socket case both yield
    /// `None`, so the default boot keeps calling `load_shard_on_boot` unchanged; only a process
    /// explicitly booted as the receiver takes the socket boot-substitution.
    #[test]
    fn receiver_plan_is_some_only_with_socket_and_receiver_role() {
        // Default config: no socket, default sender role -> no receive plan (disk-load boot).
        assert!(
            HandoffPlan::receiver_from_config(&Config::default()).is_none(),
            "default config (no socket, sender role) -> no receive plan -> disk load unchanged"
        );

        // A socket but the DEFAULT sender role -> still no receive plan (the old process streams; it
        // does not receive), so its own boot loads from disk as before.
        let socket_only = Config {
            handoff_socket: Some(PathBuf::from("/run/ironcache/handoff.sock")),
            ..Config::default()
        };
        assert!(
            HandoffPlan::receiver_from_config(&socket_only).is_none(),
            "a socket with the sender role is NOT a receiver -> disk load unchanged"
        );

        // The receiver role but NO socket -> no plan (nothing to connect to).
        let role_only = Config {
            handoff_role: HandoffRole::Receiver,
            ..Config::default()
        };
        assert!(
            HandoffPlan::receiver_from_config(&role_only).is_none(),
            "the receiver role without a socket has no rendezvous -> no receive plan"
        );

        // Socket AND receiver role -> a receive plan carrying the socket.
        let receiver = Config {
            handoff_socket: Some(PathBuf::from("/run/ironcache/handoff.sock")),
            handoff_role: HandoffRole::Receiver,
            ..Config::default()
        };
        let plan = HandoffPlan::receiver_from_config(&receiver)
            .expect("socket + receiver role yields a receive plan");
        assert_eq!(plan.socket, PathBuf::from("/run/ironcache/handoff.sock"));
    }

    /// The cross-shard flip is ALL-OR-NOTHING: every shard Ok -> Commit; a single Err -> Abort
    /// (fail-closed), even amid many successes; the empty handoff commits.
    #[test]
    fn barrier_from_results_commits_all_ok_aborts_any_err() {
        let all_ok: Vec<Result<u8, HandoffError>> = vec![Ok(1), Ok(2), Ok(3)];
        assert_eq!(barrier_from_results(&all_ok), CutoverState::Commit);

        let one_err: Vec<Result<u8, HandoffError>> = vec![Ok(1), Err(HandoffError::Aborted), Ok(3)];
        assert_eq!(
            barrier_from_results(&one_err),
            CutoverState::Abort,
            "a single shard failure aborts the whole flip (fail-closed)"
        );

        let empty: Vec<Result<u8, HandoffError>> = Vec::new();
        assert_eq!(barrier_from_results(&empty), CutoverState::Commit);
    }
}

/// End-to-end wiring tests over a REAL `tokio::net::UnixListener` / `UnixStream` bound to a real
/// socket FILE (the same transport two processes use), driving both ends on ONE `current_thread`
/// runtime with `tokio::join!` because `ShardStore` / `ReplRing` are the shared-nothing single-thread
/// (`Rc`) types (so the futures are `!Send`). Data-safety FIRST: the abort + timeout tests assert the
/// OLD store stays intact and NO partial store is adopted.
#[cfg(all(test, unix))]
mod socket_tests {
    use super::*;
    use ironcache_repl::ReplObserver;
    use ironcache_storage::{ExpireWrite, NewValue, Store};

    const NOW: UnixMillis = UnixMillis(1_000);
    const DBS: u32 = 4;
    /// A generous deadline for the success paths (a healthy local-socket handoff completes in ms;
    /// this is far above that so the timer never fires on a green run).
    const GENEROUS: Duration = Duration::from_secs(10);

    fn replid() -> ReplId {
        ReplId::from_bytes([0xCD; 20])
    }

    /// A unique node-local socket path under the temp dir (pid + tag keep parallel tests from
    /// colliding on the well-known-name property the real path has).
    fn sock_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ic-handoff-drive-{tag}-{}.sock",
            std::process::id()
        ))
    }

    /// A fresh store with an OBSERVED ring installed BEFORE the writes (so every write is tracked as
    /// a delta `StreamOp`), populated with `n` keys spread across the databases.
    fn populated(n: u32, tag: &str) -> (ShardStore, Rc<RefCell<ReplRing>>) {
        let ring = ReplRing::new(4096, ReplOffset::ZERO);
        let mut store = ShardStore::new(DBS);
        store.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        for i in 0..n {
            let key = format!("{tag}-k{i}");
            let val = format!("{tag}-v{i}");
            store.upsert(
                i % DBS,
                key.as_bytes(),
                NewValue::Bytes(val.as_bytes()),
                ExpireWrite::Clear,
                NOW,
            );
        }
        (store, ring)
    }

    /// DATA-SAFETY (the primary acceptance): a populated multi-shard dataset streams old->new over a
    /// REAL socket (bind/accept on the sender, connect on the receiver, one stream per shard), the
    /// cross-shard barrier COMMITS, and every adopted new store serves EVERY key.
    #[tokio::test(flavor = "current_thread")]
    async fn real_socket_multi_shard_handoff_serves_every_key() {
        let shard_count = 3u32;
        let per_shard = 30u32;
        let path = sock_path("e2e");
        let listener = bind_handoff_listener(&path).expect("bind the handoff socket");

        let mut results: Vec<Result<ReplOffset, HandoffError>> = Vec::new();
        let mut adopted: Vec<ShardStore> = Vec::new();

        for shard in 0..shard_count {
            let tag = format!("s{shard}");
            let (src, ring) = populated(per_shard, &tag);

            // Rendezvous: the receiver CONNECTS while the sender ACCEPTS (concurrently).
            let (client, server) = tokio::join!(
                connect_handoff(&path, GENEROUS),
                accept_handoff(&listener, GENEROUS)
            );
            let mut recv_stream = client.expect("receiver connects");
            let mut send_stream = server.expect("sender accepts");

            // Drive the shard: the ACCEPTED stream is the sender (old, read-only), the CONNECTED
            // stream is the receiver (new, fresh store).
            let send = send_shard_timed(
                &mut send_stream,
                &src,
                &ring,
                shard,
                replid(),
                NOW,
                4,
                GENEROUS,
            );
            let recv = recv_shard_timed(
                &mut recv_stream,
                || ShardStore::new(DBS),
                DBS,
                NOW,
                GENEROUS,
            );
            let (sres, rres) = tokio::join!(send, recv);

            let final_off = sres.expect("send completes");
            let loaded = rres.expect("recv completes");
            assert_eq!(loaded.shard, shard, "the shard id round-trips");
            assert_eq!(loaded.final_offset, final_off, "both ends agree on the cut");
            results.push(Ok(final_off));
            adopted.push(loaded.store);
        }

        assert_eq!(
            barrier_from_results(&results),
            CutoverState::Commit,
            "every shard handed off -> the cross-shard flip commits"
        );

        for (shard, store) in adopted.iter_mut().enumerate() {
            let tag = format!("s{shard}");
            for i in 0..per_shard {
                let key = format!("{tag}-k{i}");
                let want = format!("{tag}-v{i}");
                assert_eq!(
                    store.read(i % DBS, key.as_bytes(), NOW).unwrap().as_bytes(),
                    want.as_bytes(),
                    "adopted shard {shard} serves {key} after the real-socket handoff"
                );
            }
        }
        let _ = std::fs::remove_file(&path);
    }

    /// DELTA over a REAL socket + the TIMED phase wrappers: writes made DURING the transfer window
    /// (after the bulk cut, before cutover) are present after cutover. Drives the two phases
    /// explicitly through [`send_bulk_timed`] / [`recv_bulk_timed`] then
    /// [`send_cutover_timed`] / [`recv_cutover_timed`].
    #[tokio::test(flavor = "current_thread")]
    async fn writes_during_transfer_present_after_cutover_over_real_socket() {
        let path = sock_path("delta");
        let listener = bind_handoff_listener(&path).expect("bind the handoff socket");
        let (mut src, ring) = populated(12, "d");

        let (client, server) = tokio::join!(
            connect_handoff(&path, GENEROUS),
            accept_handoff(&listener, GENEROUS)
        );
        let mut recv_stream = client.expect("connect");
        let mut send_stream = server.expect("accept");

        // Phase 1: BULK (concurrently). The cut is captured at the start of send_bulk.
        let (end_off, mut store) = {
            let send =
                send_bulk_timed(&mut send_stream, &src, &ring, 0, replid(), NOW, 4, GENEROUS);
            let recv = recv_bulk_timed(
                &mut recv_stream,
                || ShardStore::new(DBS),
                DBS,
                NOW,
                GENEROUS,
            );
            let (s, r) = tokio::join!(send, recv);
            let end_s = s.expect("bulk send completes");
            let (store, _shard, end_r) = r.expect("bulk recv completes");
            assert_eq!(end_s, end_r, "both ends agree on the cut offset");
            (end_s, store)
        };

        // Writes AFTER the cut (offset > end_off): a CREATE, an OVERWRITE, and a DELETE, captured by
        // the observer ring as the delta. `d-k4` was written to db 4%4 = 0; `d-k1` to db 1.
        src.upsert(
            0,
            b"d-new",
            NewValue::Bytes(b"fresh"),
            ExpireWrite::Clear,
            NOW,
        );
        src.upsert(
            0,
            b"d-k4",
            NewValue::Bytes(b"overwritten"),
            ExpireWrite::Clear,
            NOW,
        );
        src.delete(1, b"d-k1", NOW);

        // Phase 2: DELTA + CUTOVER (concurrently), timed.
        let send = send_cutover_timed(&mut send_stream, &ring, end_off, 4, GENEROUS);
        let recv = recv_cutover_timed(&mut recv_stream, store, 0, end_off, NOW, GENEROUS);
        let (sres, rres) = tokio::join!(send, recv);
        let final_off = sres.expect("cutover send completes");
        let loaded = rres.expect("cutover recv completes");
        assert_eq!(loaded.final_offset, final_off);
        store = loaded.store;

        assert_eq!(
            store.read(0, b"d-new", NOW).unwrap().as_bytes(),
            b"fresh",
            "a create during transfer is present after cutover"
        );
        assert_eq!(
            store.read(0, b"d-k4", NOW).unwrap().as_bytes(),
            b"overwritten",
            "an overwrite during transfer wins after cutover"
        );
        assert!(
            store.read(1, b"d-k1", NOW).is_none(),
            "a delete during transfer is applied after cutover"
        );
        assert_eq!(
            store.read(0, b"d-k0", NOW).unwrap().as_bytes(),
            b"d-v0",
            "an untouched bulk key survives"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// ABORT-SAFETY (receiver crash mid-transfer over a real socket): the receiver handshakes then
    /// DROPS the socket; the sender's stream errors out and it aborts, leaving the OLD store fully
    /// intact and NO store adopted -> the old process keeps serving.
    #[tokio::test(flavor = "current_thread")]
    async fn receiver_crash_aborts_and_old_keeps_serving() {
        let path = sock_path("abort");
        let listener = bind_handoff_listener(&path).expect("bind");
        let (mut src, ring) = populated(50, "c");

        let (client, server) = tokio::join!(
            connect_handoff(&path, GENEROUS),
            accept_handoff(&listener, GENEROUS)
        );
        let recv_stream = client.expect("connect");
        let mut send_stream = server.expect("accept");

        // A receiver that reads the HELLO, acks, then CRASHES (drops the socket) before the bulk.
        let crasher = async move {
            let mut s = recv_stream;
            // Pull the sender's HELLO + ack it via a minimal recv_bulk that will then be dropped when
            // the store build races the socket close; simplest: just drop the stream after a beat so
            // the sender's bulk writes / cutover fail.
            let _ = tokio::io::AsyncReadExt::read_u8(&mut s).await;
            // `s` drops here -> the sender's writes/reads now fail with a broken pipe / EOF.
        };
        // chunk_max = 1 so the bulk is many frames (the broken pipe bites promptly).
        let send = send_shard_timed(&mut send_stream, &src, &ring, 0, replid(), NOW, 1, GENEROUS);
        let (sres, ()) = tokio::join!(send, crasher);

        assert!(
            sres.is_err(),
            "the sender aborts when the receiver crashes mid-transfer"
        );
        // The old store is fully intact (the sender only READ it): every key still serves.
        for i in 0..50u32 {
            let key = format!("c-k{i}");
            let want = format!("c-v{i}");
            assert_eq!(
                src.read(i % DBS, key.as_bytes(), NOW).unwrap().as_bytes(),
                want.as_bytes(),
                "old process still serves {key} after the aborted handoff"
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// TIMEOUT (hung peer): the receiver connects but then NEVER answers (never reads/acks the
    /// HELLO). The sender's [`send_shard_timed`] with a SHORT deadline ABORTS with
    /// [`HandoffError::Timeout`] rather than hanging, and the OLD store stays intact.
    #[tokio::test(flavor = "current_thread")]
    async fn hung_peer_times_out_and_old_keeps_serving() {
        let path = sock_path("timeout");
        let listener = bind_handoff_listener(&path).expect("bind");
        let (mut src, ring) = populated(20, "h");

        let (client, server) = tokio::join!(
            connect_handoff(&path, GENEROUS),
            accept_handoff(&listener, GENEROUS)
        );
        // Hold the receiver end but NEVER read from it: the sender blocks awaiting the HELLO_ACK.
        let _hung_receiver = client.expect("connect");
        let mut send_stream = server.expect("accept");

        let short = Duration::from_millis(150);
        let sres =
            send_shard_timed(&mut send_stream, &src, &ring, 0, replid(), NOW, 4, short).await;
        assert!(
            matches!(sres, Err(HandoffError::Timeout { .. })),
            "a hung peer makes the sender time out (abort), not hang: {sres:?}"
        );
        // The old store is intact after the timed-out handoff.
        for i in 0..20u32 {
            let key = format!("h-k{i}");
            assert!(
                src.read(i % DBS, key.as_bytes(), NOW).is_some(),
                "old process still serves {key} after the timed-out handoff"
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// PER-SHARD socket multiplexing (#638 PR-1): the helper derives `<base>.<i>`, distinct paths per
    /// shard, and a bind + connect round-trips over each shard's OWN suffixed path (deterministic
    /// i<->i pairing). Two shards bind + connect independently, proving the per-shard variants wrap
    /// the derivation correctly.
    #[tokio::test(flavor = "current_thread")]
    async fn per_shard_paths_derive_and_bind_connect_independently() {
        let base = sock_path("pershard");

        // Derivation: `<base>.<i>`, and distinct per shard.
        assert_eq!(
            per_shard_handoff_path(&base, 0),
            PathBuf::from(format!("{}.0", base.display())),
            "shard 0 derives <base>.0"
        );
        assert_eq!(
            per_shard_handoff_path(&base, 3),
            PathBuf::from(format!("{}.3", base.display())),
            "shard 3 derives <base>.3"
        );
        assert_ne!(
            per_shard_handoff_path(&base, 0),
            per_shard_handoff_path(&base, 1),
            "different shards derive different socket paths"
        );

        // Each shard binds + connects its OWN suffixed path (the i<->i pairing), concurrently.
        let l0 = bind_handoff_listener_for_shard(&base, 0).expect("bind shard 0's socket");
        let l1 = bind_handoff_listener_for_shard(&base, 1).expect("bind shard 1's socket");
        let (c0, a0) = tokio::join!(
            connect_handoff_for_shard(&base, 0, GENEROUS),
            accept_handoff(&l0, GENEROUS)
        );
        c0.expect("shard 0 connects to <base>.0");
        a0.expect("shard 0 accepts on <base>.0");
        let (c1, a1) = tokio::join!(
            connect_handoff_for_shard(&base, 1, GENEROUS),
            accept_handoff(&l1, GENEROUS)
        );
        c1.expect("shard 1 connects to <base>.1");
        a1.expect("shard 1 accepts on <base>.1");

        let _ = std::fs::remove_file(per_shard_handoff_path(&base, 0));
        let _ = std::fs::remove_file(per_shard_handoff_path(&base, 1));
    }

    /// The RENDEZVOUS timeout itself: [`connect_handoff`] to a socket that is NEVER bound aborts with
    /// [`HandoffError::Timeout`] (phase = connect) rather than retrying forever.
    #[tokio::test(flavor = "current_thread")]
    async fn connect_to_unbound_socket_times_out() {
        let path = sock_path("noconnect");
        let _ = std::fs::remove_file(&path); // ensure nothing is listening
        let res = connect_handoff(&path, Duration::from_millis(120)).await;
        assert!(
            matches!(res, Err(HandoffError::Timeout { phase: "connect" })),
            "connecting to an unbound socket times out: {res:?}"
        );
    }
}
