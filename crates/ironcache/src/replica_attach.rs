// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-7d LIVE per-shard replica attach: wire the HA-7b full-sync + HA-7c tail into the live
//! serve path so a node that becomes a committed REPLICA of some slots mirrors the slot
//! OWNER's keyspace and serves READONLY reads from it.
//!
//! This module is the bridge between three already-built pieces and the running shard:
//! - the committed [`ironcache_cluster::SlotMap`] (`ctx.cluster`), written by the Raft
//!   `ConfigSm` when an `AssignReplica` naming this node commits, and read here through
//!   [`SlotMap::is_replica_of_self`] / [`SlotMap::moved_target`];
//! - the replication primitives in [`ironcache_repl`] (the [`ReplObserver`] + [`ReplRing`]
//!   tail source, [`drive_full_sync`] / [`receive_full_sync`], [`drain_and_ship`] /
//!   [`ReplicaApplier`]);
//! - the shard's thread-local store handle ([`crate::serve::shard_store`]) and the passive
//!   -replica guard ([`crate::serve::set_replica_passive`]).
//!
//! ## Transport security (PROD-3): frame-bounded AND TLS + shared-secret authenticated
//!
//! The replication wire frames ([`ironcache_repl::Frame`]) are bounded against a forged
//! per-argument length (the [`ironcache_runtime::MAX_CLUSTER_FRAME_LEN`] cap enforced in
//! `ironcache_repl::frames`), closing the memory-DoS where a malicious `$<huge>\r\n` header could
//! drive the recv buffer to grow unbounded -- the HIGHEST-severity vector on this transport, fixed
//! regardless of TLS.
//!
//! The replication LINK (the keyspace-bearing data plane: the full-sync snapshot + the committed
//! -write tail + the HA-6 migration data-copy) is now ENCRYPTED + PEER-AUTHENTICATED, closing the
//! keyspace-siphon vector. It reuses, byte for byte, the SAME building blocks the Raft cluster-bus
//! (`RAFTMSG`) proved (PROD-3): the [`ironcache_clusterbus::ClusterSecurity`] handle built once at
//! boot ([`crate::raft_boot::build_cluster_security`]) and the [`ironcache_runtime::SecureStream`]
//! (`Plain | ClientTls | ServerTls`) the bus reads/writes through. Every repl stream is a
//! `SecureStream`:
//!
//! * the SOURCE LISTENER ([`serve_replica_conn`] + the accept loop): when security is configured it
//!   runs [`ClusterSecurity::accept`] (the `HANDSHAKE_TIMEOUT`-bounded TLS server handshake + the
//!   constant-time shared-secret peer check) on the accepted `TcpStream` BEFORE the first
//!   ReplConf/ImportReq; a TLS / secret failure drops the connection (logged) so an unauthenticated
//!   party can never pull the keyspace, exactly like the bus listener.
//! * the DIAL side (replica -> owner in [`attach_once`]; importer -> source in [`import_once`]):
//!   [`ClusterSecurity::dial`] (the CA-verifying TLS client handshake + the secret) wraps the dialed
//!   `TcpStream` BEFORE the ReplConf/ImportReq, so the dialer authenticates the owner AND presents
//!   the secret.
//!
//! The SAME `cluster_tls` / `cluster_secret` / cert / key / CA config drives both transports (no new
//! knobs). When cluster TLS + secret are OFF (the DEFAULT), the repl link is
//! [`SecureStream::Plain`] -- a thin passthrough to the exact `TcpStream` read/write the runtime
//! backend uses, so the plaintext path is BYTE-IDENTICAL to before this layer; the engine is
//! untouched. An operator running the default plaintext repl still SHOULD restrict the data-plane
//! port (`repl_port(client_port)`) to the trusted intra-cluster network.
//!
//! ## Gated to raft-mode ONLY (the default path is byte-unchanged)
//!
//! [`spawn_on_shard`] is the SOLE entry point, and the per-shard drain-loop setup calls it
//! ONLY when `ctx.raft.is_some()` (raft-governance mode). The DEFAULT static path and the
//! raft-control-plane-without-replicas path therefore never reach this module; nothing here
//! runs, no listener binds, no observer is installed, and the [`crate::serve::REPLICA_PASSIVE`]
//! guard stays `false`. Even in raft-mode, the replica side does NOTHING until an
//! `AssignReplica` naming this node is committed into the shared map (the control task polls
//! and stays idle until then).
//!
//! ## Single-shard correctness (shards == 1), the per-shard pattern documented
//!
//! The HA-7d acceptance gate runs `shards == 1`, so this wires ONE primary repl listener +
//! ONE replica control task per node, both correct for a single shard. With `shards == 1`
//! THIS node's single shard owns the union of every slot it owns, and a slot it replicates
//! maps to THIS one shard, so "this shard should be a replica" reduces to "is this node a
//! committed replica of ANY slot" ([`any_replica_of_self`]). The MULTI-SHARD generalization
//! is the identical pattern per shard: each shard installs its observer + listener at its own
//! repl port band and the control task filters slots to the ones THIS shard would own; the
//! repl primitives are already per-shard (`ReplRing` / `ReplObserver` / `ReplicaApplier` are
//! all shard-local `Rc<RefCell<..>>`), so no cross-shard state is introduced. We implement
//! the single-shard case and document the multi-shard fan-out rather than build it
//! speculatively.
//!
//! ## Two roles on every raft-mode shard
//!
//! Every raft-mode shard plays BOTH roles, because the committed map can make it an owner
//! (primary) of some slots and a replica of others at the same time:
//!
//! 1. PRIMARY: at shard boot it installs a [`ReplObserver`] feeding a per-shard [`ReplRing`]
//!    (so every local write is observed into the tail buffer) and runs a repl LISTENER on the
//!    node's repl port. On an accepted replica connection it [`drive_full_sync`]s its current
//!    snapshot (capturing the snapshot cut `end_offset` ATOMICALLY per CARRY-FORWARD 1) then
//!    loops [`drain_and_ship`] to stream the tail. Installing the observer flips the store's
//!    `repl_active` gate -- the HA-5a seam's intended use in raft-mode -- which does NOT
//!    affect the default static path (it never reaches this module).
//! 2. REPLICA: a control task polls the committed map on the [`Runtime::timer`] cadence (NOT
//!    `std::sleep`). When THIS shard should be a replica and is not yet attached, it dials the
//!    slot owner's repl endpoint, [`receive_full_sync`]s into a FRESH [`ShardStoreImpl`], and
//!    on success ATOMICALLY swaps it into the live thread-local store handle, marks the shard
//!    [`crate::serve::set_replica_passive`] (CARRY-FORWARD 2), then tails: recv frames and
//!    [`ReplicaApplier::apply`] them, re-full-syncing on a [`ApplyOutcome::Gap`].
//!
//! ## Borrow discipline (ADR-0002, the repl crate's collect-then-drain rule)
//!
//! No `RefCell` borrow is ever held across an `.await`. The socket lives behind an
//! `Rc<RefCell<Option<Stream>>>` that each I/O step TAKES out, awaits on the owned value, and
//! puts back (the idiom `tests/fullsync_loopback.rs` uses); the store handle is borrowed only
//! inside the synchronous chunk-pull / apply steps that the repl primitives already structure
//! to release the borrow before any send/recv.

use core::cell::RefCell;
use core::time::Duration;
use std::net::SocketAddr;
use std::rc::Rc;

use ironcache_clusterbus::ClusterSecurity;
use ironcache_env::Clock;
use ironcache_protocol::slot::key_slot;
use ironcache_repl::{
    ApplyOutcome, DiskBacklog, Frame, FrameError, InSyncReplicas, ReplId, ReplNodeStatus,
    ReplObserver, ReplOffset, ReplRing, ReplicaApplier, ShipOutcome, decode_kvobj, drain_and_ship,
    encode_kvobj, receive_full_sync,
};
use ironcache_runtime::tokio_rt::bind_exclusive;
use ironcache_runtime::{Runtime, SecureStream, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_storage::UnixMillis;
use ironcache_store::SnapshotCursor;

use crate::serve::ShardStoreImpl;

/// The bounded depth of a primary shard's replication tail ring (HA-7c [`ReplRing`] cap). A
/// generous window so a momentarily-behind replica resumes from the in-memory tail rather than
/// forcing a full re-sync. When it overflows (the replica fell behind the in-memory window) the op
/// either SPILLS to the HA-7e disk-backed backlog (when `repl_backlog_disk_bytes > 0` + a data_dir,
/// so the replica still resumes incrementally from disk) or, with no disk backlog (the DEFAULT),
/// [`drain_and_ship`] reports [`ShipOutcome::ResyncNeeded`] and the replica re-full-syncs (the MVP
/// full-resync-on-gap policy, correct though not optimal).
const TAIL_RING_CAP: usize = 65_536;

/// How many tail ops to ship per [`drain_and_ship`] pass (a bounded batch so a borrow is held
/// only for an O(batch) copy off the ring before the sends).
const TAIL_SHIP_BATCH: usize = 1_024;

/// The full-sync chunk size (entries per [`Frame::SyncKv`] batch). Bounds the primary's peak
/// transfer memory to one chunk (the collect-then-drain discipline in [`drive_full_sync`]).
const FULLSYNC_CHUNK_MAX: usize = 256;

/// How often the primary's per-connection serve loop wakes to drain new tail ops onto the wire
/// (and the replica's control task re-checks the committed map / re-dials). Through the
/// [`Runtime::timer`] seam (ADR-0003), never `std::sleep`. Small so the tail latency is low and
/// a freshly-committed AssignReplica is acted on promptly; the cost when idle is one timer wake.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Backoff between replica dial / re-sync attempts when the owner is unreachable or a sync
/// fails, so a flapping owner does not busy-spin the control task. Through the timer seam.
const RECONNECT_BACKOFF: Duration = Duration::from_millis(200);

/// The replication-port offset for the CO-LOCATED raft+repl deployment (HA-7d). A node's repl
/// listener binds at [`repl_port`] of its client port: a dedicated data-plane port (REPLICATION.md
/// #77), separate from the RESP client port AND the Raft cluster-bus port.
///
/// It is DELIBERATELY NOT [`ironcache_repl::REPL_PORT_OFFSET`] (10001): that standalone value sits
/// ONE above the Raft bus offset ([`crate::raft_boot::BUS_PORT_OFFSET`] = 10000), so for two
/// nodes whose client ports are ADJACENT (`P` and `P+1`, the common ephemeral-port case in the
/// raft loopback tests) `repl_port(P+1) == bus_port(P)` -- the repl listener would land on a
/// neighbor's bus port. A FIXED, LARGE offset of 20000 (well clear of the bus band) means an
/// aliasing collision needs client ports 10000 apart, which ephemeral allocation never produces;
/// combined with the EXCLUSIVE (non-`SO_REUSEPORT`) bind in [`run_primary_listener`], any residual
/// collision FAILS the bind cleanly rather than silently stealing the other service's traffic.
const REPL_PORT_OFFSET: u16 = 20_000;

/// The repl-listener port for a client `port`, overflow-safe (mirrors
/// [`crate::raft_boot::bus_port`]'s convention so a HIGH ephemeral test port does not panic).
/// `port + REPL_PORT_OFFSET` whenever it fits a `u16`; for a high port it falls back to
/// `port - REPL_PORT_OFFSET`. Either way it is a bijection on distinct client ports, so two
/// co-located nodes never collide on a repl port (the property the raft-mode loopback tests
/// rely on). The advertised OWNER port a replica resolves through [`SlotMap::moved_target`] is
/// the owner's CLIENT port, so the replica dials `repl_port(owner_client_port)` to reach the
/// owner's repl listener.
#[must_use]
pub(crate) fn repl_port(port: u16) -> u16 {
    if port <= u16::MAX - REPL_PORT_OFFSET {
        port + REPL_PORT_OFFSET
    } else {
        port - REPL_PORT_OFFSET
    }
}

/// Whether THIS node is a committed REPLICA of ANY of the 16384 slots (the single-shard
/// "this shard should be a replica" test, HA-7d). A cold scan of the committed map's
/// `is_replica_of_self` (two atomic loads per slot, NO node lock per slot in the common
/// all-`UNASSIGNED` case), run only on the control task's poll cadence (never the hot path).
/// Returns the FIRST such slot so the caller can resolve THAT slot's owner endpoint; `None`
/// when this node replicates nothing (the steady state until an AssignReplica commits).
#[must_use]
fn any_replica_of_self(map: &ironcache_cluster::SlotMap) -> Option<u16> {
    (0..ironcache_cluster::CLUSTER_SLOTS).find(|&slot| map.is_replica_of_self(slot))
}

/// Whether THIS node is the committed IMPORTING destination of SOME slot (HA-6 data-copy), and
/// which one. The IMPORTING tag is set ONLY on the destination node (the `ConfigSm` apply gates
/// `set_importing` on `is_self(dest)`), so a committed `MigrationState::Importing` on this node's
/// shared map means THIS node is the importer; no extra `is_self` check is needed. A cold scan of
/// the parallel `migration_state` array (one relaxed atomic load per slot), run only on the import
/// control task's poll cadence (never the hot path). Returns the FIRST such slot; `None` in the
/// steady state until a `SetSlotImporting` naming this node commits.
#[must_use]
fn any_importing_slot(map: &ironcache_cluster::SlotMap) -> Option<u16> {
    (0..ironcache_cluster::CLUSTER_SLOTS)
        .find(|&slot| map.migration_state(slot) == ironcache_cluster::MigrationState::Importing)
}

/// Build THIS shard's OPTIONAL HA-7e disk-backed replication backlog from the boot config. Returns
/// `Some(DiskBacklog)` only when `repl_backlog_disk_bytes > 0` AND a `data_dir` is configured (the
/// spill lives under `<data_dir>/repl-backlog-shard-<n>`, per shard so segments never collide);
/// `None` (the DEFAULT, or no data_dir) leaves the in-memory-only tail, byte-identical to pre-HA-7e.
/// A directory-creation failure inside `DiskBacklog::open` also yields `None` (the safe degradation:
/// replication still runs, just without the wider window).
fn build_disk_backlog(ctx: &ServerContext, shard_index: usize) -> Option<DiskBacklog> {
    let max_bytes = ctx.boot.repl_backlog_disk_bytes;
    if max_bytes == 0 {
        return None; // disabled: in-memory-only, byte-identical.
    }
    let data_dir = ctx.boot.data_dir.as_ref()?; // no data_dir -> the knob is inert.
    let shard_dir = data_dir.join(format!("repl-backlog-shard-{shard_index}"));
    DiskBacklog::open(&shard_dir, max_bytes)
}

/// Spawn the HA-7d replica-attach machinery on THIS shard's `LocalSet` (the per-shard
/// `spawn_on_shard` executor): the PRIMARY repl listener + observer and the REPLICA control
/// task. Called from the per-shard drain-loop setup ONLY in raft-mode (the caller gates on
/// `ctx.raft.is_some()`), so the default path never invokes it.
///
/// `store_rc` is THIS shard's live store handle (the same `Rc<RefCell<ShardStoreImpl>>` the
/// serve loop holds); the primary observer is installed on it and the replica side atomically
/// swaps a fresh full-synced store into it. `bind` is the node's bind address; `client_port`
/// is the node's advertised CLIENT port (the repl listener binds at [`repl_port`] of it).
///
/// Idempotent per shard via [`PRIMARY_STARTED`]: a second call (e.g. a connection arriving
/// before the drain loop's first poll) is a no-op, so neither the observer nor the listener is
/// duplicated.
pub(crate) fn spawn_on_shard(
    ctx: &ServerContext,
    store_rc: Rc<RefCell<ShardStoreImpl>>,
    bind: std::net::IpAddr,
    client_port: u16,
    shard_index: usize,
) {
    if PRIMARY_STARTED.with(std::cell::Cell::get) {
        return; // already wired on this shard (idempotent).
    }
    PRIMARY_STARTED.with(|c| c.set(true));

    let Some(cluster) = ctx.cluster.clone() else {
        // Raft-mode always installs a shared map; defensively do nothing without one.
        return;
    };

    // INTRA-CLUSTER TRANSPORT SECURITY (PROD-3) for the REPLICATION transport: build the SAME
    // optional TLS + shared-secret handle the Raft cluster-bus uses, from the SAME config
    // (`cluster_tls` / `cluster_secret` / cert / key / CA -- no new knobs). `None` (the default) is
    // the plaintext repl link, byte-unchanged (the repl streams are `SecureStream::Plain`). Built
    // ONCE per shard here (it loads the cert/key/CA PEMs); the rustls configs + secret live behind
    // `Arc`s inside the handle, so it CLONES cheaply onto the source listener (accept side) and the
    // replica / importer control tasks (dial side). A bad PEM that passed config validation but
    // rustls rejects logs and degrades to NO repl security (the data path still runs); the bus boot
    // already fails loudly on the same PEM, so this never silently mis-secures only repl.
    let security = match crate::raft_boot::build_cluster_security(&ctx.boot) {
        Ok(sec) => sec,
        Err(e) => {
            tracing::error!(error = %e, "replica-attach: failed to build repl transport security; \
                 running the replication link WITHOUT TLS/secret");
            None
        }
    };

    // HA-7e: an OPTIONAL per-shard DISK-BACKED backlog spilling the in-memory tail past `cap`, so a
    // replica behind the in-memory ring catches up INCREMENTALLY from disk instead of a full
    // snapshot re-sync. Engaged ONLY when `repl_backlog_disk_bytes > 0` AND a `data_dir` is
    // configured; otherwise `None` -> the in-memory-only tail, byte-identical to before HA-7e. The
    // spill is PER SHARD (under `<data_dir>/repl-backlog-shard-<n>`) so shards never share segments.
    let disk_backlog = build_disk_backlog(ctx, shard_index);

    // The per-shard replication tail ring + the observer feeding it. Installing the observer
    // flips the store's `repl_active` gate (the HA-5a seam's intended raft-mode use): from now
    // on every local write on this shard is enqueued onto `ring` as a tail op. One Rc clone of
    // the ring stays here for the listener's `drain_and_ship`; the other lives in the boxed
    // observer the store owns.
    let ring = ReplRing::with_disk(TAIL_RING_CAP, ReplOffset::ZERO, disk_backlog);
    store_rc
        .borrow_mut()
        .set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));

    // The replication id this primary advertises in `FullSync` AND the resume HISTORY token the
    // resume gate verifies against. It MUST identify the replication HISTORY (which restarts on every
    // primary boot, because the offset space restarts at `ReplOffset::ZERO` and the disk backlog
    // purges prior segments), NOT the STABLE cluster identity. So we use the PER-BOOT history token
    // (`ctx.repl_history_id`, freshly drawn from the boot RNG seam) when present (raft-governance
    // mode, the only mode that serves the live resume). A NEW token on every restart is the fence
    // against silent divergence: a reconnecting replica that last synced under the OLD token will
    // mismatch and full-resync (the correct, safe behavior) rather than blindly resume against a
    // primary whose history was reset.
    //
    // FALLBACK (no per-boot token, e.g. a defensive non-raft path that somehow reaches here): keep
    // the prior stable-cluster-id-derived value. That value is UNCHANGED across restarts, so a
    // replica would never see a token change -- but in that fallback the resume gate ALSO never
    // matches a remembered token unless the cluster id round-trips, and the ack<=head guard plus the
    // first-connect-full-sync rule still hold, so the conservative outcome is a full re-sync.
    let replid = ctx.repl_history_id.unwrap_or_else(|| {
        ReplId::from_hex(ctx.info.cluster_node_id.as_bytes())
            .unwrap_or_else(|| ReplId::from_bytes([0u8; 20]))
    });

    // The NODE-LEVEL replication status cell (HA-7e): the repl tasks publish role / offsets / link
    // state here for INFO / CLUSTER SHARDS + the HA-8 promotion gate. Raft-mode always installs
    // one (serve::run_server), so `Some` in practice; a defensive fresh cell keeps the wiring
    // total without an Option threading through every task. It is `Send + Sync` (atomics), so it
    // clones cheaply into both shard-local tasks.
    let status: std::sync::Arc<ReplNodeStatus> = ctx
        .repl_status
        .clone()
        .unwrap_or_else(|| std::sync::Arc::new(ReplNodeStatus::new()));

    // The SOURCE-SIDE in-sync-replica COUNT (ADR-0026, the WRITE-SIDE `min-replicas-to-write`
    // guardrail): the primary's per-replica serve tasks maintain it (lock-free per-connection
    // deltas) and the WRITE path reads it. Raft-mode always installs one (serve::run_server), so
    // `Some` in practice; a defensive fresh cell keeps the wiring total. It is `Send + Sync` (a
    // single AtomicUsize), cloned cheaply into the listener task. The LAG GATE the count uses is
    // `min_replicas_max_lag` (the same offset-lag units as replica_max_lag).
    let in_sync: std::sync::Arc<InSyncReplicas> = ctx
        .in_sync_replicas
        .clone()
        .unwrap_or_else(|| std::sync::Arc::new(InSyncReplicas::new()));
    let min_replicas_max_lag = ctx.boot.min_replicas_max_lag;

    let rt = TokioRuntime::new();

    // --- PRIMARY: bind the repl listener + serve replica connections. ---
    let listen_addr = SocketAddr::new(bind, repl_port(client_port));
    let listener_ring = Rc::clone(&ring);
    let listener_store = Rc::clone(&store_rc);
    let listener_status = std::sync::Arc::clone(&status);
    let listener_in_sync = std::sync::Arc::clone(&in_sync);
    let listener_security = security.clone();
    rt.spawn_on_shard(async move {
        run_primary_listener(
            TokioRuntime::new(),
            listen_addr,
            replid,
            listener_store,
            listener_ring,
            listener_status,
            listener_in_sync,
            min_replicas_max_lag,
            listener_security,
        )
        .await;
    });

    // --- REPLICA: the control task that attaches THIS shard to a slot owner when committed. ---
    let databases = ctx.databases;
    let policy_name = ctx.info.maxmemory_policy;
    let reserved_bits = crate::serve::scan_reserved_bits(ctx.shards);
    // HA-8 failover inputs: the Raft handle to PROPOSE a promotion, this node's 40-hex id (the
    // `new_primary` of a self-proposed promotion), the lag bound (promotion eligibility gate), and
    // the link-down timeout before proposing. The handle is cloned (Send); a `None` (defensive,
    // raft-mode always has one) simply disables self-promotion.
    let raft = ctx.raft.clone();
    let self_node_id = ctx.info.cluster_node_id.to_string();
    let failover = FailoverParams {
        replica_max_lag: ctx.boot.replica_max_lag,
        failover_timeout: Duration::from_secs(ctx.boot.failover_timeout_secs),
    };
    let replica_cluster = std::sync::Arc::clone(&cluster);
    let replica_store = Rc::clone(&store_rc);
    let replica_status = std::sync::Arc::clone(&status);
    let replica_security = security.clone();
    rt.spawn_on_shard(async move {
        run_replica_control(
            TokioRuntime::new(),
            replica_cluster,
            replica_store,
            databases,
            policy_name,
            reserved_bits,
            replica_status,
            raft,
            self_node_id,
            failover,
            replica_security,
        )
        .await;
    });

    // --- IMPORT: the HA-6 data-copy control task that pulls a migrating slot's data when this
    // node is the committed IMPORTING destination. Distinct from the replica control task above
    // (a node can replicate some slots AND import another at the same time): this one drives the
    // ADDITIVE per-slot merge into the SAME live store, never a store swap and never passive. It
    // does nothing until a committed `SetSlotImporting` naming this node lands in the shared map.
    rt.spawn_on_shard(async move {
        run_import_control(TokioRuntime::new(), cluster, store_rc, security).await;
    });
}

/// HA-8 failover-detection knobs handed to the replica control task (kept in one struct so the
/// task's already-wide signature does not grow two more scalars).
#[derive(Debug, Clone, Copy)]
struct FailoverParams {
    /// The replication-lag bound (logical writes): a replica is promotion-eligible ONLY when it
    /// was in sync within this bound at last contact (ADR-0026, so a stale replica is never
    /// promoted -> no data loss beyond the async-replication window).
    replica_max_lag: u64,
    /// How long the master link must be CONTINUOUSLY down before this replica proposes its own
    /// promotion (the failover-detection timeout).
    failover_timeout: Duration,
}

// ===========================================================================================
// PRIMARY SIDE
// ===========================================================================================

/// Bind the primary repl listener and serve each accepted replica connection on its own
/// shard-local task until the listener errors (process shutdown). Mirrors
/// [`ironcache_repl::run_primary_repl_listener`]'s accept-and-spawn shape but serves the HA-7b
/// full-sync + HA-7c tail (not just heartbeats). A bind failure logs and returns (the data
/// path still runs; replication simply never serves, the safe degradation).
///
/// The over-7-args lint is allowed: each argument is a distinct, orthogonal seam (the runtime, the
/// listen address, the repl id, the live store handle, the tail ring, the node status cell, plus the
/// write-side guardrail count cell + its lag bound); bundling them would just move the same fields
/// behind one name.
#[allow(clippy::too_many_arguments)]
async fn run_primary_listener(
    rt: TokioRuntime,
    listen_addr: SocketAddr,
    replid: ReplId,
    store_rc: Rc<RefCell<ShardStoreImpl>>,
    ring: Rc<RefCell<ReplRing>>,
    status: std::sync::Arc<ReplNodeStatus>,
    in_sync: std::sync::Arc<InSyncReplicas>,
    min_replicas_max_lag: u64,
    security: Option<ClusterSecurity>,
) {
    // EXCLUSIVE bind (NO SO_REUSEPORT): the repl listener is a single per-node socket that must
    // never SHARE a port with another service. A reuseport bind would let the kernel load-balance
    // traffic between this listener and any other reuseport socket on the same port (e.g. a
    // neighbor node's Raft bus or client listener that aliased this port), silently stealing
    // consensus / client traffic. An exclusive bind turns such a collision into a clean error.
    let listener = match bind_exclusive(listen_addr) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(%listen_addr, error = %e, "replica-attach: failed to bind repl listener");
            return;
        }
    };
    // Publish this node's current head as a master from the moment the listener is up (HA-7e), so
    // INFO / CLUSTER SHARDS report the real master offset even before any replica attaches.
    status.set_master_head(ring.borrow().head());
    loop {
        let Ok((stream, peer)) = rt.accept(&listener).await else {
            return; // listener failed; the primary is going away.
        };
        let rt2 = TokioRuntime::new();
        let store = Rc::clone(&store_rc);
        let ring = Rc::clone(&ring);
        let status = std::sync::Arc::clone(&status);
        let in_sync = std::sync::Arc::clone(&in_sync);
        let security = security.clone();
        rt.spawn_on_shard(async move {
            // SECURE THE ACCEPTED CONNECTION (PROD-3) BEFORE the first ReplConf/ImportReq byte:
            // when `security` is `Some` this runs the bus-proven TLS server handshake + the
            // constant-time shared-secret peer check ([`ClusterSecurity::accept`]); a TLS / secret
            // failure (or a plaintext dialer to a TLS source) drops the connection here -- an
            // unauthenticated party can never reach the keyspace transfer. `None` (the default)
            // wraps the raw TcpStream as `SecureStream::Plain` (byte-identical plaintext).
            let Some(secure) = secure_accept(security.as_ref(), stream).await else {
                tracing::warn!(%peer, "replica-attach: repl source rejected a connection that \
                     failed the TLS/secret handshake");
                return;
            };
            serve_replica_conn(
                rt2,
                secure,
                replid,
                store,
                ring,
                status,
                in_sync,
                min_replicas_max_lag,
            )
            .await;
        });
    }
}

/// SECURE a freshly ACCEPTED repl `TcpStream` for the SOURCE listener (PROD-3), mirroring the bus
/// `run_listener_secure` accept path: with `security` `Some`, run [`ClusterSecurity::accept`] (the
/// `HANDSHAKE_TIMEOUT`-bounded TLS server handshake + the shared-secret peer check) and return the
/// [`SecureStream`] on success; on a TLS / secret failure return `None` so the caller drops the
/// connection. With `None` (the default) wrap the raw stream as [`SecureStream::Plain`], which is a
/// thin passthrough to the same `TcpStream` read/write the runtime backend uses -- byte-identical
/// to the pre-PROD-3 plaintext repl path.
async fn secure_accept(
    security: Option<&ClusterSecurity>,
    tcp: <TokioRuntime as Runtime>::Stream,
) -> Option<SecureStream> {
    let Some(sec) = security else {
        return Some(SecureStream::plain(tcp));
    };
    match sec.accept(tcp).await {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!(error = %e, "replica-attach: repl source TLS/secret handshake failed");
            None
        }
    }
}

/// SECURE a freshly DIALED repl `TcpStream` for the replica / importer DIAL side (PROD-3), mirroring
/// the bus `connect_endpoint_secure` dial path: with `security` `Some`, run [`ClusterSecurity::dial`]
/// (the CA-verifying TLS client handshake + the shared-secret presentation) and return the
/// [`SecureStream`]; on a TLS / secret failure return `None` so the caller backs off + re-dials.
/// With `None` (the default) wrap the raw stream as [`SecureStream::Plain`] (byte-identical
/// plaintext).
async fn secure_dial(
    security: Option<&ClusterSecurity>,
    tcp: <TokioRuntime as Runtime>::Stream,
) -> Option<SecureStream> {
    let Some(sec) = security else {
        return Some(SecureStream::plain(tcp));
    };
    match sec.dial(tcp).await {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!(error = %e, "replica-attach: repl dial TLS/secret handshake failed");
            None
        }
    }
}

/// Serve ONE accepted replica connection: read the attach handshake, full-sync the current
/// snapshot (capturing the cut `end_offset` per CARRY-FORWARD 1), then loop [`drain_and_ship`]
/// to stream the tail until the link drops.
///
/// ## Two attach modes (HA-7d replica vs HA-6 slot-import), distinguished by a leading frame
///
/// A plain REPLICA sends [`Frame::ReplConf`] first and wants the WHOLE store: `slot_filter`
/// stays `None`, the full-sync + tail are unfiltered, and this is the byte-identical HA-7d path.
/// An IMPORTING DESTINATION sends [`Frame::ImportReq`] FIRST (then `REPLCONF`) to scope the
/// attach to one migrating slot: `slot_filter` becomes `Some(slot)`, and BOTH the snapshot and
/// the steady-state tail are filtered to keys hashing to that slot (`key_slot(key) == slot`).
/// The source keeps OWNING + serving the slot throughout (ownership transfers only at the
/// committed FLIP), so its live writes to the slot land in the tail and reach the importer until
/// the FLIP makes the importer the owner and it stops importing.
///
/// The stream lives behind `Rc<RefCell<Option<_>>>` so each send/recv TAKES it out, awaits on
/// the owned value, and puts it back -- no `RefCell` borrow crosses an `.await`.
///
/// The over-7-args lint is allowed: each argument is a distinct, orthogonal seam (the runtime, the
/// accepted stream, the repl id, the live store handle, the tail ring, the node status cell, plus
/// the write-side guardrail count cell + its lag bound); bundling them would just move the same
/// fields behind one name.
#[allow(clippy::too_many_arguments)]
async fn serve_replica_conn(
    rt: TokioRuntime,
    stream: SecureStream,
    replid: ReplId,
    store_rc: Rc<RefCell<ShardStoreImpl>>,
    ring: Rc<RefCell<ReplRing>>,
    status: std::sync::Arc<ReplNodeStatus>,
    in_sync: std::sync::Arc<InSyncReplicas>,
    min_replicas_max_lag: u64,
) {
    let stream = Rc::new(RefCell::new(Some(stream)));
    // THIS connection's contribution to the source-side in-sync-replica COUNT (ADR-0026, the
    // WRITE-SIDE guardrail). The task is the SINGLE WRITER of its own contribution: `counted`
    // tracks whether it currently counts toward the quorum, and every step nudges the shared
    // counter by exactly one on a verdict change (lock-free `fetch_add`/`fetch_sub`). On EVERY
    // exit path below it is cleared via `in_sync.replica_gone(counted)` so a disconnected replica
    // stops counting; `counted` is updated in lockstep so that final clear is exact.
    let mut counted = false;

    // (1) Read the attach handshake. A leading IMPORTREQ scopes the transfer to ONE slot (an
    // importing destination, HA-6 data-copy); otherwise it is a plain whole-store replica attach
    // (HA-7d, `slot_filter == None`, byte-identical). Either way the REPLCONF handshake follows
    // (the replica names itself + its resume offset). A replica that has never synced sends ack 0;
    // we always full-sync from scratch in this MVP, so the ack is read-and-acknowledged but the
    // sync starts at the snapshot cut regardless.
    let mut pending: Vec<u8> = Vec::new();
    let Some((slot_filter, handshake_ack, handshake_token, replica_node_id)) =
        read_attach_handshake(&rt, &stream, &mut pending).await
    else {
        return; // the peer closed / sent garbage before attaching.
    };
    // Publish that a replica is connected (PRIMARY side, HA-7e): connected_slaves -> 1 with the
    // replica's resume offset. INFO renders the `slaveN:` line + the master-side lag from this.
    status.set_replica_connected(handshake_ack);
    // #365 stage 2: record the replica's advertised NodeId ONLY for a PLAIN attach (a steady
    // replica), not a scoped import (`slot_filter` Some, a transient HA-6 slot data-copy that is
    // not an INFO `slaveN`). Reporting-only: it never affects the sync below.
    if slot_filter.is_none() {
        status.set_replica_id(replica_node_id);
    }

    // (2) INCREMENTAL RESUME vs FULL SYNC (HA-7e). A RECONNECTING plain replica advertises its real
    // applied offset (`handshake_ack > 0`, it kept its store) AND the replication HISTORY token it
    // last full-synced under. We RESUME -- skip the full snapshot and stream the tail from
    // `handshake_ack` -- ONLY when ALL of these hold; otherwise we full-sync from scratch (today's
    // behavior, the SAFE fallback):
    //
    //   * HISTORY-TOKEN MATCH (the silent-divergence fence): the replica's remembered token EXACTLY
    //     equals THIS primary's current history token (`replid`, freshly minted on every boot). A
    //     primary RESTART mints a NEW token while the offset space resets to `ZERO`, so a replica
    //     that resumes against a different history (the primary lost/reset its writes) is forced to
    //     full-resync rather than blindly keep its now-stale store. A first-connect replica carries
    //     NO token (`None`) and so always full-syncs, as before.
    //   * ACK NOT AHEAD OF HEAD (`handshake_ack <= head`): the replica must not claim MORE than this
    //     primary actually has. If it does, the primary lost/reset history (a restart back to a
    //     SMALLER store), so the offset-window check's "caught up, nothing to serve" verdict
    //     (`can_serve_from(ack)` returns true for `ack >= head`) would WRONGLY treat the replica as
    //     in sync and ship nothing, leaving it silently diverged. Guard it explicitly -> full-sync.
    //   * the offset is genuinely SERVEABLE from the RECOVERABLE window (the in-memory ring OR the
    //     HA-7e disk-backed spill) and no resync is latched -- the original window check.
    //
    // A SCOPED import (`slot_filter` Some) NEVER resumes -- it is an additive slot-migration
    // data-copy that always re-snapshots the slot. The resume decision is taken under one synchronous
    // borrow (flush staged spill first so the disk run is complete).
    let token_matches = handshake_token == Some(replid);
    let can_resume = slot_filter.is_none() && token_matches && {
        let mut r = ring.borrow_mut();
        if r.has_disk() {
            r.flush_spill();
        }
        let head = r.head();
        handshake_ack.0 > 0
            && handshake_ack.0 <= head.0
            && !r.needs_resync()
            && r.can_serve_from(handshake_ack)
    };

    let cut = if can_resume {
        // RESUME: do NOT send a FullSync; the replica keeps its store and continues applying from
        // `handshake_ack`. The tail loop below ships ops with offset strictly greater than it
        // (drawing from disk then memory, gap-free). The send cursor starts at the replica's ack.
        handshake_ack
    } else {
        // FULL SYNC. CARRY-FORWARD 1 (ATOMIC end_offset capture) is satisfied inside
        // `drive_full_sync_chunked` (see its doc + the cited code point): the cut is the ring head
        // captured with NO `.await` between the head read and the first snapshot chunk pull. When
        // `slot_filter` is `Some(slot)`, only that slot's keys are shipped (a scoped snapshot). The
        // returned cut is THIS connection's per-connection send-cursor START (C1).
        let Ok(cut) =
            drive_full_sync_chunked(&rt, &stream, replid, &store_rc, &ring, slot_filter).await
        else {
            status.set_replica_disconnected(); // the link dropped mid-sync; the replica re-syncs.
            in_sync.replica_gone(counted); // no-op (never counted pre-sync), kept for uniform cleanup.
            return;
        };
        cut
    };
    // The full sync completed: this replica is now caught up to the snapshot cut (send_cursor ==
    // cut), so it is IN SYNC. Count it toward the write-side quorum (ADR-0026). An IMPORT scope
    // (slot_filter Some) is a slot-migration data-copy, NOT a durability replica, so it must NOT
    // count toward the write quorum -- a write must be on real replicas, not a transient importer.
    let lag_eligible = slot_filter.is_none();
    counted = update_in_sync(
        &in_sync,
        &ring,
        cut,
        min_replicas_max_lag,
        lag_eligible,
        counted,
    );
    // C1: THIS connection's OWN send cursor, starting at the snapshot cut. The ring keeps no
    // shared send cursor, so two concurrent consumers (e.g. this connection AND another replica
    // / an importer draining the same source ring) each advance their OWN cursor and each see
    // every tail op past it -- no split.
    let mut send_cursor = cut;

    // (3) The steady-state tail: ship newly-observed ops in offset order. `drain_and_ship`
    // takes the ring borrow only for the bounded batch copy, releases it, then awaits the
    // sends (so the write funnel is never blocked behind a network await). On an overflow the
    // replica fell too far behind: drop the connection so it reconnects + re-full-syncs (MVP
    // full-resync-on-gap). When the batch is empty, wait the poll interval (timer seam) so an
    // idle tail does not busy-spin. After EACH pass, publish the current head (PRIMARY side,
    // HA-7e) so INFO / CLUSTER SHARDS report the advancing master offset.
    //
    // SCOPED TAIL (HA-6): when `slot_filter` is `Some(slot)`, a frame whose key does NOT hash to
    // the slot is DROPPED here on the source before the send (the importer wants only that slot's
    // mutations). The drop does NOT desync the offset stream: the importer's apply path is
    // ADDITIVE + offset-agnostic in import mode (see `run_import_tail`), so a missing offset is
    // not a gap. A plain replica (`None`) keeps every frame, byte-identical.
    loop {
        let send_stream = Rc::clone(&stream);
        let rt_send = rt;
        let outcome = drain_and_ship(&ring, &mut send_cursor, TAIL_SHIP_BATCH, move |frame| {
            let stream = Rc::clone(&send_stream);
            let pass = frame_in_slot(&frame, slot_filter);
            let bytes = frame.encode();
            async move {
                if pass {
                    send_bytes(&rt_send, &stream, bytes).await
                } else {
                    Ok(()) // filtered out of a scoped slot-import tail; not an error.
                }
            }
        })
        .await;
        // Publish the advancing master head (and re-affirm the slave's last-known offset) so the
        // observable master offset tracks the writes shipped. Cold node-level publish, not per key.
        status.set_master_head(ring.borrow().head());
        // WRITE-SIDE GUARDRAIL (ADR-0026): recompute this replica's in-sync verdict from the lag
        // it has SHIPPED to (`head - send_cursor`) and nudge the shared count on a change. The
        // source measures lag by how far its shipping is behind the head: a replica being kept
        // current (send_cursor near head) is in sync; one that fell behind (the ring is filling
        // faster than it drains) is not. Cold node-level update, not per stored key.
        counted = update_in_sync(
            &in_sync,
            &ring,
            send_cursor,
            min_replicas_max_lag,
            lag_eligible,
            counted,
        );
        match outcome {
            ShipOutcome::Shipped(0) => rt.timer(POLL_INTERVAL).await,
            ShipOutcome::Shipped(_) => {}
            // The ring overflowed (the replica is too far behind) or the link dropped: end the
            // connection. The replica reconnects and re-attaches, which full-syncs from a fresh
            // cut; the ring's resync latch is cleared by the next attach's rebase-at-head.
            ShipOutcome::ResyncNeeded | ShipOutcome::LinkDown => {
                status.set_replica_disconnected();
                in_sync.replica_gone(counted); // this replica is gone: drop its quorum contribution.
                return;
            }
        }
    }
}

/// Recompute THIS replica connection's in-sync verdict for the WRITE-SIDE guardrail count
/// (ADR-0026) and publish the one-step delta, returning the new `counted` state.
///
/// The SOURCE measures lag as `head - shipped`: how far the primary's current offset is ahead of
/// what this connection has SHIPPED (`shipped` is the per-connection send cursor). A replica is IN
/// SYNC when `eligible` (a real durability replica, not a transient slot importer) AND that lag is
/// `<= max_lag` (the [`InSyncReplicas`] lag gate, the same offset-lag semantics as the promotion
/// gate). `head < shipped` cannot happen (the cursor never outruns the head) but is clamped to 0 by
/// `saturating_sub` defensively. The actual counter mutation (a single `fetch_add`/`fetch_sub` on a
/// change, a no-op when unchanged) lives in [`InSyncReplicas::set_replica_in_sync`]; this is the
/// thin source-side adapter that reads `head` off the ring (one synchronous borrow, no `.await`).
fn update_in_sync(
    in_sync: &InSyncReplicas,
    ring: &Rc<RefCell<ReplRing>>,
    shipped: ReplOffset,
    max_lag: u64,
    eligible: bool,
    counted: bool,
) -> bool {
    let head = ring.borrow().head();
    let lag = head.0.saturating_sub(shipped.0);
    let now_in_sync = eligible && lag <= max_lag;
    in_sync.set_replica_in_sync(counted, now_in_sync)
}

/// Drive a full sync of THIS shard's live store (behind `store_rc`) to a replica, capturing the
/// snapshot cut `end_offset` ATOMICALLY with the snapshot scan start (CARRY-FORWARD 1) and
/// holding NO `RefCell` borrow across any `.await`.
///
/// This is the RefCell-aware twin of [`ironcache_repl::drive_full_sync`]: that primitive takes a
/// `&ShardStore` (owned-borrow, fine when the store is not shared), but here the live store is a
/// `Rc<RefCell<ShardStoreImpl>>` shared with the serve loop, so the store borrow MUST be released
/// before each network send (else the funnel stalls behind a network await AND clippy's
/// await-holding-refcell lint fires). It mirrors `drive_full_sync`'s exact discipline: take the
/// store borrow, pull ONE bounded `snapshot_chunk` (owned data), RELEASE the borrow, then await
/// the sends.
///
/// ## CARRY-FORWARD 1 (ATOMIC end_offset capture) -- the exact code point
///
/// The line `let end_offset = ring.borrow().head();` below captures the cut, and the `ring`
/// borrow is dropped on that SAME line (a temporary). The VERY NEXT statement sends the FULLSYNC
/// frame and then enters the chunk loop; there is NO write to this shard's store between reading
/// `head()` and pulling the first chunk that is NOT itself reflected in `head()`, because: (a) the
/// shard is single-threaded (ADR-0002), so no other task runs between the synchronous `head()`
/// read and the first `store_rc.borrow()`/`snapshot_chunk` below -- the FULLSYNC `send` await in
/// between cannot let a local WRITE interleave on this shard (writes run on this same thread); and
/// (b) every local write bumps the ring `head` past `end_offset` via the installed observer, so it
/// lands in the tail (offset > end_offset), shipped by `serve_replica_conn`'s `drain_and_ship`
/// loop, never lost. Thus the boundary write is in the snapshot XOR the tail, exactly once.
///
/// # Errors
/// Returns `Err(())` if any frame send fails (the replica link dropped mid-sync); the caller
/// drops the connection and the replica reconnects + re-syncs. On success returns the captured
/// snapshot cut `end_offset` -- THIS connection's per-connection send-cursor START (C1).
async fn drive_full_sync_chunked(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<SecureStream>>>,
    replid: ReplId,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    ring: &Rc<RefCell<ReplRing>>,
    slot_filter: Option<u16>,
) -> Result<ReplOffset, ()> {
    // `now` for the snapshot's lazy-expiry skip (ADR-0003 clock seam), captured ONCE for the scan.
    let now = now_from_env();

    // CARRY-FORWARD 1: capture the snapshot cut = the ring's current head, borrow dropped on this
    // line. NO `.await` runs between here and the first `snapshot_chunk` pull below that could let
    // a local write be missed (see the doc above).
    let end_offset = ring.borrow().head();

    // Announce the full sync: the replid names the stream, end_offset names the cut.
    send_bytes(rt, stream, Frame::FullSync { replid, end_offset }.encode()).await?;

    // Stream the whole snapshot in bounded chunks. Each iteration: BORROW the store, pull one
    // chunk (owned), encode each entry, RELEASE the borrow, THEN await the sends.
    let databases = store_rc.borrow().databases();
    let mut cursor = SnapshotCursor::START;
    while !cursor.is_done(databases) {
        let frames: Vec<Vec<u8>> = {
            let store = store_rc.borrow();
            let (chunk, next) = store.snapshot_chunk(cursor, FULLSYNC_CHUNK_MAX, now);
            cursor = next;
            chunk
                .into_iter()
                // SCOPED SNAPSHOT (HA-6): when `slot_filter` is `Some(slot)`, ship ONLY keys
                // hashing to it. The filter runs on the bounded chunk (at most FULLSYNC_CHUNK_MAX
                // entries already in hand), so it is MEMORY-NEUTRAL: `snapshot_chunk` already
                // bounds the per-chunk materialization, and filtering only drops entries from that
                // bounded batch (it never widens it). A plain replica passes `None` -> every key
                // ships, byte-identical to the HA-7d path.
                .filter(|(_, key, _)| key_in_slot(key, slot_filter))
                .map(|(db, key, kv)| {
                    Frame::SyncKv {
                        db,
                        key: key.into_vec(),
                        kvobj_bytes: encode_kvobj(&kv),
                    }
                    .encode()
                })
                .collect()
        }; // the store borrow ends here, before any await below.
        for bytes in frames {
            send_bytes(rt, stream, bytes).await?;
        }
    }

    // Terminate the stream with the cut offset (self-contained SYNCEND).
    send_bytes(rt, stream, Frame::SyncEnd { end_offset }.encode()).await?;
    Ok(end_offset)
}

/// Read inbound bytes until the attach `REPLCONF` arrives, returning
/// `Some((slot_filter, ack, resume_token))` once it does (the handshake), or `None` if the socket
/// closes / sends a malformed frame first.
///
/// A leading [`Frame::ImportReq`] (sent by an IMPORTING destination, HA-6 data-copy) BEFORE the
/// REPLCONF scopes the attach to one slot: its slot is captured into `slot_filter` and returned
/// alongside the `ack`. A plain replica sends no IMPORTREQ, so `slot_filter` is `None` and the
/// transfer is the unfiltered whole-store HA-7d attach (byte-identical). Any OTHER non-REPLCONF
/// frame before the handshake is ignored (a stray heartbeat). `pending` carries the partial read
/// buffer across reads. The `ack` is the master's view of the peer's resume offset (HA-7e INFO).
/// `resume_token` is the per-boot replication HISTORY token the replica last full-synced under
/// (`Some` only for a reconnecting replica with a prior store); the resume gate accepts an
/// incremental tail ONLY when it EXACTLY matches the primary's CURRENT history token (else a full
/// re-sync), so a primary restart -- which mints a new token -- can never let a replica silently
/// resume against a reset history.
async fn read_attach_handshake(
    _rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<SecureStream>>>,
    pending: &mut Vec<u8>,
) -> Option<(Option<u16>, ReplOffset, Option<ReplId>, u64)> {
    let mut slot_filter: Option<u16> = None;
    loop {
        // Drain any complete frames already buffered.
        loop {
            match Frame::decode(pending) {
                Ok(Some((frame, consumed))) => {
                    pending.drain(..consumed);
                    match frame {
                        Frame::ReplConf {
                            node,
                            ack,
                            resume_token,
                        } => {
                            // Attached; carry the scope (if any), the peer's resume offset, the
                            // resume history token it last synced under (`None` for a first-connect
                            // replica; gates whether the primary may RESUME, HA-7e safety), AND the
                            // replica's advertised `NodeId` (#365 stage 2; `0` if it did not
                            // advertise). The token gates resume; the node id is reporting-only.
                            return Some((slot_filter, ack, resume_token, node));
                        }
                        // A leading IMPORTREQ scopes this attach to one slot (HA-6 data-copy).
                        // Record it and keep reading for the REPLCONF that follows.
                        Frame::ImportReq { slot } => slot_filter = Some(slot),
                        // Any other non-REPLCONF frame before the handshake: ignore, keep reading.
                        _ => {}
                    }
                }
                Ok(None) => break,              // need more bytes.
                Err(FrameError) => return None, // malformed: drop the connection.
            }
        }
        // Read another chunk (TAKE/put-back so no borrow crosses the await). The I/O is on the
        // SecureStream (TLS-decrypted when the link is secured, plaintext when not), not the raw
        // runtime seam, so the handshake + the whole transfer ride the same secure channel.
        let taken: Vec<u8> = core::mem::take(pending);
        let mut s = stream.borrow_mut().take().expect("stream present");
        let res = s.recv(taken).await;
        *stream.borrow_mut() = Some(s);
        match res {
            Ok(r) => {
                if r.n == 0 {
                    return None; // replica closed before attaching.
                }
                *pending = r.buf;
            }
            Err(_) => return None,
        }
    }
}

// ===========================================================================================
// REPLICA SIDE
// ===========================================================================================

/// The replica CONTROL task: poll the committed map on the timer cadence; when THIS shard
/// should be a replica and is not yet attached, attach (full-sync + tail). Loops forever (the
/// shard's lifetime), so a role change that makes this node a replica LATER is picked up, and a
/// detached / dropped link is re-attached.
///
/// HA-8 FAILOVER DETECTION lives here (the cleanest place: this task already owns the
/// replica-side link lifecycle). When `attach_once` returns the link is down; this task records
/// HOW LONG it has been continuously down, and once that exceeds `failover.failover_timeout` AND
/// the replica was IN SYNC at last contact (the lag gate, so a stale replica is never promoted),
/// it PROPOSES `PromoteReplica { its replicated slots, itself }` to the Raft leader through the
/// `Send` [`RaftHandle`]. On the leader the proposal commits and every node (including the OLD
/// primary, once it rejoins) applies it; the committed log + the epoch bump is THE SPLIT-BRAIN
/// FENCE (only one owner per slot at any committed epoch). A spurious promotion (the primary was
/// actually alive) is SAFE for split-brain -- the committed entry atomically transfers ownership
/// and the old primary steps down on apply; it costs only an unnecessary failover (like Sentinel).
#[allow(clippy::too_many_arguments)]
async fn run_replica_control(
    rt: TokioRuntime,
    cluster: std::sync::Arc<ironcache_cluster::SlotMap>,
    store_rc: Rc<RefCell<ShardStoreImpl>>,
    databases: u32,
    policy_name: &'static str,
    reserved_bits: u32,
    status: std::sync::Arc<ReplNodeStatus>,
    raft: Option<ironcache_server::RaftHandle>,
    self_node_id: String,
    failover: FailoverParams,
    replica_security: Option<ClusterSecurity>,
) {
    // HA-8: how long the master link has been CONTINUOUSLY down (None == the link is up / never
    // attached). Reset to the current env time on the FIRST down observation after a healthy
    // contact, and cleared on a successful (re)attach, so the elapsed measures one unbroken outage.
    let mut down_since: Option<ironcache_env::Monotonic> = None;
    // HA-8 fix: the in-sync verdict LATCHED at the last REAL master contact (the attach that just
    // dropped), NOT recomputed each iteration. The promotion lag gate must use the in-sync state as
    // of the last contact, because once the link is marked down a subsequent re-check reports
    // not-in-sync (link down) and would wrongly refuse the promotion. Without the latch the two
    // gate conditions (failover timeout elapsed AND in-sync) are never simultaneously true on a
    // dead master: in-sync holds only on the first post-drop iteration (before the timeout elapses),
    // and is false on every later dial-fail iteration (when the timeout has elapsed). Latched here,
    // updated only on a real attach, reset when this node stops being a replica.
    let mut last_contact_in_sync = false;
    // HA-7e: the replica's CARRY-FORWARD resume state -- the highest offset it has DURABLY APPLIED
    // AND the per-boot history token it last full-synced under -- carried ACROSS reconnects so a
    // re-dial can RESUME incrementally (from the owner's in-memory ring or its disk-backed backlog)
    // instead of always full-syncing. `FRESH` (offset ZERO, token None) means "no store yet" (a fresh
    // attach full-syncs); `attach_once` returns the new state to carry forward. It is RESET to `FRESH`
    // whenever this node stops being a replica (a future replica role starts fresh). The token is the
    // silent-divergence fence: a primary restart mints a NEW token, so the remembered token mismatches
    // and the primary forces a full re-sync (instead of resuming against a reset history).
    let mut resume = ResumeState::FRESH;
    loop {
        // Should THIS shard be a replica right now? (single-shard: replica of ANY slot.)
        //
        // H1 DUAL-ROLE GUARD (fail safe, never corrupt): on the single-shard model a shard
        // serves ONE of {owner-importing, passive-replica} at a time, because the replica role
        // SWAPS the whole store + marks it passive while the import role ADDITIVELY merges into
        // the live store -- running both on one `Rc<RefCell<ShardStoreImpl>>` would clash (the
        // swap discards the importer's merged keys; additive writes into a passive mirror have
        // wrong delete semantics). So if THIS node is ALSO a committed importer of some slot, do
        // NOT attach as a passive replica: skip to the idle poll until the import completes /
        // aborts (then `any_importing_slot` clears and a later iteration attaches). The import
        // control task holds the complementary guard. The multi-shard generalization (a distinct
        // shard per role) is the real fix, tracked; here we guarantee no corruption.
        if any_importing_slot(&cluster).is_some() {
            down_since = None;
            last_contact_in_sync = false;
            resume = ResumeState::FRESH; // not attaching as a replica: a later attach is fresh.
            rt.timer(POLL_INTERVAL).await;
            continue;
        }
        if let Some(slot) = any_replica_of_self(&cluster) {
            // Resolve the slot OWNER's advertised CLIENT endpoint; the repl listener is at
            // `repl_port` of that client port. `moved_target` returns None for an unassigned
            // slot (mid-formation); back off and re-check.
            if let Some((host, owner_client_port)) = cluster.moved_target(slot) {
                // RESOLVE the owner's repl endpoint accepting a DNS hostname OR an IP literal (k8s):
                // `host` is re-fetched from the committed map each loop iteration, so this re-resolves
                // on every (re)attach -- a restarted owner pod that kept its DNS name but got a new IP
                // is dialed at its new address. The old IP-only parse silently skipped a DNS-named
                // owner (no replication ever attached). A not-yet-resolvable host backs off + retries.
                //
                // H1: `resolve` is ASYNC (getaddrinfo on tokio's blocking pool, bounded by
                // RESOLVE_TIMEOUT via the Runtime timer seam), so a wedged resolver never freezes this
                // shard's executor; awaited with the shard's `rt`.
                if let Ok(owner_addr) = ironcache_clusterbus::PeerEndpoint::new(
                    host.clone(),
                    repl_port(owner_client_port),
                )
                .resolve(&rt)
                .await
                {
                    // Attach: full-sync + atomic swap + passive + tail. Returns when the link
                    // drops or a Gap forces a re-attach; we loop and re-attach (re-checking the
                    // map first, so a role removal stops us). Publishes the replica-side status
                    // (role / link / offsets) into `status` for INFO / CLUSTER SHARDS + the HA-8
                    // gate; the master ENDPOINT reported is the owner's advertised CLIENT
                    // host:port (what an operator dials), not the internal repl port.
                    resume = attach_once(
                        &rt,
                        owner_addr,
                        &host,
                        owner_client_port,
                        &store_rc,
                        databases,
                        policy_name,
                        reserved_bits,
                        &status,
                        replica_security.as_ref(),
                        &self_node_id,
                        resume,
                    )
                    .await;
                    // The attach returned: the link is down (drop / gap / dial fail). BEFORE we
                    // publish link-down, snapshot whether the replica was in sync AT LAST CONTACT:
                    // `attach_once` left the status with link UP (set at attach) + the last
                    // observed master head + applied offset, so `is_in_sync` here reflects the lag
                    // at the moment the link broke. A dial-fail (never attached this round) leaves
                    // the prior link state; in_sync is false then, which correctly withholds a
                    // promotion until a real in-sync contact has happened.
                    // The in-sync verdict AS OBSERVED right now: on a REAL attach-then-drop the
                    // status still shows link UP here (set_master_link_down is called below), so this
                    // reflects the lag at the moment the link broke; on a DIAL-FAIL (never attached
                    // this round) the link is already down so this is false (and we keep the latch
                    // instead).
                    let observed_in_sync = status.is_in_sync(failover.replica_max_lag);
                    // Was the link actually UP this round (a real attach), vs a dial-fail that never
                    // connected? A real attach means THIS return starts a FRESH outage, so reset the
                    // outage clock; a dial-fail leaves the running outage clock intact (the master
                    // has been unreachable continuously). Snapshot the link BEFORE marking it down.
                    let was_attached = status.snapshot().master_link.is_up();
                    // Publish the link-down state (HA-7e) so INFO reports master_link_status:down
                    // and the read-staleness + promotion gates see this replica as not in sync
                    // until it re-attaches.
                    status.set_master_link_down();
                    // HA-8: the outage clock. A fresh attach-then-drop RESTARTS it at `now` (so the
                    // failover timeout measures THIS unbroken outage, not a stale earlier one); a
                    // dial-fail with the link already down KEEPS the existing start. Then check
                    // whether the outage + the lag gate warrant a self-promotion.
                    let now = now_monotonic();
                    if was_attached {
                        down_since = Some(now);
                    }
                    // LATCH the in-sync verdict: a real attach updates it from this contact; a
                    // dial-fail keeps the prior value (so the gate, which fires several iterations
                    // later once the timeout elapses, sees the last-contact state, not the now-down
                    // state). See `latch_in_sync`.
                    last_contact_in_sync =
                        latch_in_sync(was_attached, observed_in_sync, last_contact_in_sync);
                    let started = *down_since.get_or_insert(now);
                    if now.saturating_duration_since(started) >= failover.failover_timeout {
                        maybe_propose_self_promotion(
                            &cluster,
                            raft.as_ref(),
                            &self_node_id,
                            last_contact_in_sync,
                        )
                        .await;
                        // Whether or not the proposal landed (it may have been NotLeader, or the
                        // committed promotion may not have applied here yet), keep retrying on the
                        // backoff cadence; once the promotion commits, this node OWNS the slots and
                        // `any_replica_of_self` returns None, so the loop falls through to the idle
                        // poll (it is no longer a replica of anything).
                    }
                    // After a link drop, back off briefly before re-attaching.
                    rt.timer(RECONNECT_BACKOFF).await;
                    continue;
                }
            }
            // Owner not resolvable yet: treat as a down link for the outage clock too, then back
            // off and re-check.
            down_since.get_or_insert_with(now_monotonic);
            rt.timer(RECONNECT_BACKOFF).await;
        } else {
            // Not a replica (the steady state until an AssignReplica commits, OR after a committed
            // promotion made THIS node the owner). The link is healthy-or-irrelevant: clear the
            // outage clock + the latched in-sync verdict so a future replica role starts a fresh
            // failover-timeout window with no stale latch.
            down_since = None;
            last_contact_in_sync = false;
            resume = ResumeState::FRESH; // no longer a replica: a future attach is fresh.
            rt.timer(POLL_INTERVAL).await;
        }
    }
}

/// The replica-side HA-8 failover proposal: if this node was IN SYNC at last contact (the lag
/// gate) and a Raft handle is available, PROPOSE `PromoteReplica { the slots this node replicates,
/// itself }` to the leader. A no-op when not in sync (a stale replica must NOT be promoted -> no
/// data loss) or when there is no handle (defensive; raft-mode always has one). The proposal is
/// fire-and-forget from this task's perspective: a `NotLeader` outcome just means we retry on the
/// next outage tick; a committed promotion flips ownership cluster-wide via the fenced log.
///
/// HA-9 LEADER-FORWARDING is what makes this work from ANY in-sync replica, not just one that
/// happens to be the raft leader: `handle.propose()` on a follower now transparently FORWARDS the
/// `PromoteReplica` to the recognized leader, which proposes + commits it (the fenced owner flip
/// then applies on every node). Before forwarding, a follower's propose returned `NotLeader` and
/// self-promotion could only fire when the in-sync replica was itself the leader; now any in-sync
/// replica can drive its own promotion. No logic change here is needed: the forward lives in the
/// control-plane run loop behind this same `propose()` call.
async fn maybe_propose_self_promotion(
    cluster: &ironcache_cluster::SlotMap,
    raft: Option<&ironcache_server::RaftHandle>,
    self_node_id: &str,
    in_sync_at_last_contact: bool,
) {
    // Decide WHAT to promote via the pure lag gate, factored into `promotion_proposal` so the gate
    // is unit-testable WITHOUT a RaftHandle. `None` -> a stale replica (the gate refused) or this
    // node replicates nothing: propose nothing.
    let Some(slots) = promotion_proposal(cluster, in_sync_at_last_contact) else {
        return;
    };
    let Some(handle) = raft else {
        return; // no control-plane handle (defensive): cannot propose.
    };
    // Propose the promotion. Committed -> every node applies the owner flip (the fence); NotLeader
    // -> a follower cannot commit, so we simply retry on the next outage tick (the usual way a
    // forming cluster finds the leader; HA-4c surfaces no leader hint yet).
    let _ = handle
        .propose(ironcache_raft::ConfigCmd::PromoteReplica {
            slots,
            new_primary: self_node_id.to_owned(),
        })
        .await;
}

/// The latched "in sync at last master contact" verdict (HA-8 fix). A REAL attach (`was_attached`)
/// adopts the freshly `observed` verdict; a dial-fail (the master is unreachable, never attached
/// this round) KEEPS the `prev` latch. This is what makes the promotion lag gate correct on a dead
/// master: the gate fires only once the failover timeout elapses (several dial-fail iterations after
/// the link broke), by which point a re-checked `observed` would be `false` (link down) and would
/// wrongly veto the promotion. Latching at the last real contact preserves the true in-sync state.
#[must_use]
fn latch_in_sync(was_attached: bool, observed: bool, prev: bool) -> bool {
    if was_attached { observed } else { prev }
}

/// The PURE HA-8 failover DECISION (the lag gate), split out from [`maybe_propose_self_promotion`]
/// so it is unit-testable without a control-plane handle. Returns `Some(slots)` ONLY when BOTH:
/// (1) this node was IN SYNC within the bound at last contact (the lag gate -- a STALE replica
/// must NEVER be promoted, or a committed promotion would lose the writes it had not yet pulled),
/// and (2) it currently replicates at least one slot (the set it would become primary of). Returns
/// `None` otherwise. A deterministic function of the committed map + the in-sync flag the caller
/// snapshotted BEFORE the link went down; no time / rand / IO. The slot scan runs only on the cold
/// control cadence (never the hot path).
#[must_use]
fn promotion_proposal(
    cluster: &ironcache_cluster::SlotMap,
    in_sync_at_last_contact: bool,
) -> Option<Vec<u16>> {
    // THE LAG GATE: never promote a replica that was not in sync within the bound at last contact.
    if !in_sync_at_last_contact {
        return None;
    }
    let slots = slots_replicated_by_self(cluster);
    if slots.is_empty() {
        return None;
    }
    Some(slots)
}

/// Every slot THIS node is a committed REPLICA of (HA-8: the set a self-promotion would transfer
/// to it). A cold scan of `is_replica_of_self` over the 16384 slots, run only on the control
/// task's failover cadence (never the hot path), mirroring [`any_replica_of_self`] but collecting
/// the whole set rather than the first.
#[must_use]
fn slots_replicated_by_self(map: &ironcache_cluster::SlotMap) -> Vec<u16> {
    (0..ironcache_cluster::CLUSTER_SLOTS)
        .filter(|&slot| map.is_replica_of_self(slot))
        .collect()
}

/// The current monotonic instant through THIS shard's stable `SystemEnv` clock seam (ADR-0003):
/// the failover outage clock reads time ONLY here, never `std::time` directly (the determinism
/// invariant). It uses the SAME per-shard env [`now_from_env`] uses, so every `Monotonic` reading
/// shares ONE origin and `saturating_duration_since` between two readings measures real elapsed
/// time. A freshly-constructed `SystemEnv` per call would instead anchor a NEW origin each time
/// (its `now()` is `origin.elapsed()`), making two readings incomparable -- the outage clock would
/// never advance; reusing the shard env is what makes the failover timeout meaningful.
fn now_monotonic() -> ironcache_env::Monotonic {
    crate::serve::shard_env().borrow().now()
}

/// The replica's CARRY-FORWARD resume state (HA-7e): the highest offset it has durably applied AND
/// the per-boot replication HISTORY token it last full-synced under. Carried across reconnects by
/// `run_replica_control` so a re-dial can advertise both and the primary can decide RESUME vs
/// FULL-SYNC. `token == None` means "no store yet / never synced" (a fresh attach always full-syncs);
/// after a full sync the token is set to the `FullSync.replid` the primary advertised, so the next
/// reconnect re-advertises it and the primary's gate verifies the histories match before resuming.
#[derive(Debug, Clone, Copy)]
struct ResumeState {
    /// The highest offset durably applied (the resume point), `ZERO` when there is no store.
    offset: ReplOffset,
    /// The history token last synced under (`None` until the first full sync completes).
    token: Option<ReplId>,
}

impl ResumeState {
    /// The fresh state: no store, no history token -> the primary always full-syncs.
    const FRESH: ResumeState = ResumeState {
        offset: ReplOffset::ZERO,
        token: None,
    };
}

/// Attach ONCE to the owner at `owner_addr`: dial, advertise the replica's resume offset AND its
/// remembered history token, then EITHER resume the tail incrementally (HA-7e, keeping the live
/// store) OR [`receive_full_sync`] into a FRESH [`ShardStoreImpl`] + ATOMICALLY swap it in, mark the
/// shard passive (CARRY-FORWARD 2), then run the tail until the link drops / a Gap forces a re-sync.
/// Returns the [`ResumeState`] at the end of the session (the highest applied offset + the token the
/// store was synced under), which the caller carries forward to the next dial (so a re-dial resumes
/// incrementally instead of full-syncing). On a terminal condition that left the store untouched
/// (dial fail, sync fail, dial/handshake error) it returns the INCOMING `resume` UNCHANGED -- a
/// no-op for the caller's carry-forward.
///
/// ## Incremental resume vs full sync (HA-7e), decided by the PRIMARY + the first frame
///
/// The replica sends `resume.offset` as its REPLCONF ack AND `resume.token` as the REPLCONF resume
/// token. The PRIMARY decides (see `serve_replica_conn`): if the token EXACTLY matches its current
/// per-boot history token AND the offset is within its recoverable window (in-memory ring OR the
/// disk-backed backlog, and not ahead of head) it RESUMES -- it sends NO `FullSync`, just the tail
/// from the ack. The replica detects this by PEEKING the first inbound frame: a `FullSync` means a
/// full re-sync (the fresh-store path; the replica adopts the NEW token from `FullSync.replid`); a
/// stream/heartbeat frame means a RESUME (keep the live store + the existing token, continue the
/// applier from `resume.offset`). A fresh replica (`token == None`, no store) always full-syncs. On
/// a FAILED / interrupted full sync the partial temp store is DISCARDED (never swapped) and the live
/// store is UNTOUCHED.
///
/// The over-7-args lint is allowed: each argument is a distinct, orthogonal seam (the runtime,
/// the owner's repl dial address vs its advertised CLIENT endpoint for the status report, the
/// live store handle, the store-construction facts, the node status cell, and the resume state);
/// bundling them would just move the same fields behind one name.
#[allow(clippy::too_many_arguments)]
/// The `NodeId` (as a raw `u64`) a replica advertises in its `REPLCONF` handshake (#365
/// stage 1, REPL_FIDELITY.md): `node_id_from_announce` of its OWN 40-hex announce id, i.e.
/// the first 16 hex chars as a `u64`. This is the SAME mapping the leader-hint resolution
/// and the slot-map use, so a primary can later resolve it back to the replica's advertised
/// endpoint by the reverse lookup over its slot-map members. Sending it is
/// backward-compatible: `node` is advisory today, so an older primary simply ignores the
/// value, and the replica's sync behaviour is unchanged (it keys off `ack` / `resume_token`,
/// never `node`). In standalone replication the value is harmless and unused (no membership
/// to resolve against). PURE: unit-tested without a node.
fn replica_handshake_node_id(self_announce_id: &str) -> u64 {
    crate::raft_boot::node_id_from_announce(self_announce_id).0
}

#[allow(clippy::too_many_arguments)] // a repl-attach driver threading the resolved attach inputs.
async fn attach_once(
    rt: &TokioRuntime,
    owner_addr: SocketAddr,
    owner_host: &str,
    owner_client_port: u16,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    databases: u32,
    policy_name: &'static str,
    reserved_bits: u32,
    status: &std::sync::Arc<ReplNodeStatus>,
    security: Option<&ClusterSecurity>,
    self_announce_id: &str,
    resume: ResumeState,
) -> ResumeState {
    let resume_from = resume.offset;
    // Dial the owner's repl endpoint. A failed dial returns the unchanged resume state; the caller
    // backs off + retries (and carries `resume` forward to the next dial).
    let Ok(tcp) = rt.connect(owner_addr).await else {
        return resume;
    };
    // PROD-3: TLS-secure the dialed link (CA-verify the owner + present the shared secret) when
    // cluster security is on; `None` -> a plaintext `SecureStream::Plain` (byte-identical). A
    // TLS/secret failure returns so the caller backs off + re-dials.
    let Some(stream) = secure_dial(security, tcp).await else {
        return resume;
    };
    let stream = Rc::new(RefCell::new(Some(stream)));

    // Send the attach REPLCONF handshake advertising `resume.offset` AND the remembered history
    // token (HA-7e). A fresh replica sends `ZERO` + `None` (no store, no token) so the primary
    // full-syncs; a reconnecting replica sends its real applied offset + the token it last synced
    // under, so the primary can RESUME incrementally ONLY IF the token matches its current history
    // (the silent-divergence fence) AND the offset is within the recoverable (memory + disk) window.
    // `node` advertises THIS replica's identity (#365 stage 1): `node_id_from_announce` of our
    // own announce id, which a primary can resolve back to our advertised endpoint. It is advisory
    // (an older primary ignores it; our sync keys off `ack` / `resume_token`), so this is
    // backward-compatible.
    let handshake = Frame::ReplConf {
        node: replica_handshake_node_id(self_announce_id),
        ack: resume_from,
        resume_token: resume.token,
    }
    .encode();
    if !send_bytes_ok(rt, &stream, handshake).await {
        return resume;
    }

    let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
    let queue = Rc::new(RefCell::new(std::collections::VecDeque::<Frame>::new()));

    // PEEK the first inbound frame to learn whether the primary chose FULL SYNC or RESUME. A
    // `FullSync` first frame -> full-sync path; ANY other first frame (a tail StreamPut/StreamDel,
    // or a heartbeat) -> resume path. A fresh replica (resume_from ZERO) is ALWAYS full-synced by
    // the primary, so the peek there is a `FullSync`. The peeked frame is pushed back onto the recv
    // queue so the chosen path's recv loop consumes it (next_frame drains the queue first).
    let Some(first) = next_frame(rt, &stream, &pending, &queue).await else {
        return resume; // link dropped before the first frame; nothing applied.
    };
    // On a FULL SYNC the `FullSync.replid` IS the primary's per-boot history token: REMEMBER it so a
    // later reconnect re-advertises it and the primary's resume gate can verify the histories match.
    let full_sync_token = match &first {
        Frame::FullSync { replid, .. } => Some(*replid),
        _ => None,
    };
    let is_full_sync = full_sync_token.is_some();
    queue.borrow_mut().push_front(first); // put it back for the path below.

    if is_full_sync {
        // ---- FULL SYNC (today's behavior): receive into a FRESH store + atomic swap. ----
        // The fresh store is built EXACTLY like `shard_store` builds the live one (same Policy,
        // accounting, scan-band bits), so the swapped-in store is the SAME concrete type.
        let make_store =
            move || crate::serve::fresh_shard_store(databases, policy_name, reserved_bits);
        let loaded = {
            let recv_rt = *rt;
            let recv_stream = Rc::clone(&stream);
            let pending = Rc::clone(&pending);
            let queue = Rc::clone(&queue);
            receive_full_sync(make_store, move || {
                let pending = Rc::clone(&pending);
                let queue = Rc::clone(&queue);
                let stream = Rc::clone(&recv_stream);
                async move { next_frame(&recv_rt, &stream, &pending, &queue).await }
            })
            .await
        };
        let Ok(loaded) = loaded else {
            // The sync did not complete: the partial temp store was discarded inside
            // receive_full_sync. The live store is UNTOUCHED. Return the unchanged resume state
            // (no store this round); the caller re-attaches (and will full-sync again).
            return resume;
        };

        // ATOMIC STORE SWAP (HA-7d): replace the RefCell contents with the fully-loaded fresh store
        // in ONE statement; reads immediately hit the synced data, the OLD store drops. This is the
        // only place the live store is replaced, ONLY after a COMPLETE sync.
        *store_rc.borrow_mut() = loaded.store; // <-- the atomic store swap.
        // CARRY-FORWARD 2: the replica store is now passive (the reaper-stop flag + the store-level
        // lazy-expiry-passive flag), so a READONLY read never self-removes a key the primary holds.
        store_rc.borrow_mut().set_passive(true);
        crate::serve::set_replica_passive(true);

        // Publish the REPLICA-side status (HA-7e): role replica, link UP, the snapshot cut as the
        // initial applied + observed-master offset.
        status.set_replica_attached(owner_host, owner_client_port, loaded.end_offset);

        // Run the steady-state tail from the snapshot cut; returns the highest applied offset. On a
        // Gap the replica re-attaches (and the carried-forward offset lets it resume if possible).
        // The carried-forward TOKEN is the one the primary advertised in THIS `FullSync` (the new
        // history), so the next reconnect re-advertises it and the resume gate can verify a match.
        let applied = run_replica_tail(
            rt,
            &stream,
            store_rc,
            loaded.end_offset,
            status,
            &pending,
            &queue,
        )
        .await;
        ResumeState {
            offset: applied,
            token: full_sync_token,
        }
    } else {
        // ---- RESUME (HA-7e): the primary is streaming the tail from `resume_from`; KEEP the live
        // store (it already holds the keyspace from the prior session) and continue the applier from
        // `resume_from`. NO fresh store, NO swap. The store is already passive from the prior attach
        // (it was never un-marked), but set it again defensively (idempotent) so a resume after any
        // path leaves the replica removal-passive. ----
        store_rc.borrow_mut().set_passive(true);
        crate::serve::set_replica_passive(true);
        status.set_replica_attached(owner_host, owner_client_port, resume_from);
        // Continue the tail from `resume_from`: the first applied stream frame must be
        // `resume_from.next()`. A Gap (the primary could not actually serve a contiguous tail) tears
        // down + re-attaches; the carried-forward offset is whatever was applied (possibly resume_from).
        // The TOKEN is UNCHANGED on a resume (the same history the prior full-sync established): carry
        // `resume.token` forward so the next reconnect still presents it.
        let applied =
            run_replica_tail(rt, &stream, store_rc, resume_from, status, &pending, &queue).await;
        ResumeState {
            offset: applied,
            token: resume.token,
        }
    }
}

/// The replica STEADY-STATE TAIL (HA-7c apply): recv stream frames and apply them in offset
/// order via [`ReplicaApplier`], returning the HIGHEST APPLIED OFFSET when the link drops or a
/// [`ApplyOutcome::Gap`] forces a full re-sync. The returned offset is the caller's next
/// `resume_from` (HA-7e), so a re-dial resumes incrementally. No `RefCell` borrow crosses an
/// `.await`: each frame is recv'd (store borrow not held), THEN the store is borrowed for the
/// synchronous `apply`, THEN the borrow drops before the next recv.
///
/// `pending`/`queue` are the recv buffers SHARED with the attach (so the first peeked-then-pushed
/// -back frame, and any bytes already buffered, are consumed here with NO gap).
async fn run_replica_tail(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<SecureStream>>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    start: ReplOffset,
    status: &std::sync::Arc<ReplNodeStatus>,
    pending: &Rc<RefCell<Vec<u8>>>,
    queue: &Rc<RefCell<std::collections::VecDeque<Frame>>>,
) -> ReplOffset {
    let mut applier = ReplicaApplier::new(start);
    loop {
        let Some(frame) = next_frame(rt, stream, pending, queue).await else {
            return applier.applied(); // link dropped / peer closed: the caller re-attaches.
        };
        // The master's head as observed on the link (HA-7e): a stream put/del carries the offset
        // of a write the master produced, so it is a lower bound on the master head. Publishing it
        // BEFORE the apply keeps the replica's lag (`master_offset - applied`) honest even while a
        // batch of frames is still being drained. A heartbeat / non-stream frame carries no
        // offset, so it leaves the observed head unchanged.
        if let Some(off) = stream_frame_offset(&frame) {
            status.set_observed_master_head(off);
        }
        // Apply synchronously: borrow the store ONLY for the apply, release before next recv.
        let outcome = {
            let mut store = store_rc.borrow_mut();
            applier.apply(&mut store, frame, now_from_env())
        };
        match outcome {
            ApplyOutcome::Applied(_) | ApplyOutcome::Duplicate => {
                // Publish the advancing applied offset (REPLICA side, HA-7e). Monotonic; a
                // duplicate leaves it unchanged. Cold node-level publish, not per stored key.
                status.set_replica_applied(applier.applied());
            }
            // A gap (a missing offset or a corrupt post-image): the safe recovery is a full
            // re-sync. Return the applied offset; the caller re-dials. With the disk backlog the
            // carried-forward offset may still let the re-dial resume; otherwise it full-syncs.
            ApplyOutcome::Gap => return applier.applied(),
        }
    }
}

// ===========================================================================================
// IMPORT (DESTINATION) SIDE -- HA-6 online slot migration data-copy
// ===========================================================================================
//
// The IMPORTING destination of a migrating slot pulls the slot's data from the SOURCE (the
// current owner) and applies it ADDITIVELY into its EXISTING live store, so by the time the
// committed FLIP makes it the owner it already HAS every key in the slot. This is the live
// data-copy MIGRATION.md describes (snapshot the migrating slot, stream incremental mutations,
// flip in one Raft step), reusing the HA-5b snapshot + HA-7c tail transport scoped to one slot.
//
// THE KEY DIFFERENCE FROM REPLICA-ATTACH (`attach_once`): a replica SWAPS a fresh full-synced
// store into the live handle and marks the shard PASSIVE. An importer must NOT do either: it
// already OWNS other slots whose data must survive (a swap would discard them), and it actively
// serves those slots (passive would wrongly stop their reaper + lazy-expiry). So the importer
// INSERTS the scoped snapshot + the scoped tail directly into the live store via `insert_object`
// (additive) / `delete`, leaving every other slot's data untouched.
//
// OFFSET-AGNOSTIC APPLY (why not `ReplicaApplier`): the source FILTERS the stream to the slot, so
// the importer sees a SUBSET of the source's offset sequence (the other slots' offsets are
// dropped on the source). A gap in offsets is therefore EXPECTED, not corruption, so the importer
// applies each scoped frame unconditionally (additively + idempotently) rather than gating on
// `offset == applied.next()`. Last-write-wins still holds: the snapshot loads first, then the
// tail (every write after the snapshot cut) applies on top, in offset order on the wire.

/// The IMPORT CONTROL task (HA-6 data-copy): poll the committed map on the timer cadence; when
/// THIS node is the committed IMPORTING destination of some slot, run a scoped [`import_once`] to
/// that slot's SOURCE (the slot's recorded migration peer = the current owner). Loops forever (the
/// shard's lifetime), so a fresh `SetSlotImporting` is picked up, and a dropped import link is
/// re-dialed while the slot is still IMPORTING here.
///
/// LIFECYCLE: IMPORTING (committed) -> import (snapshot + scoped tail, additive) -> the committed
/// FLIP clears the IMPORTING tag -> `any_importing_slot` returns `None` (this node now OWNS the
/// slot) -> the task falls through to the idle poll. A STABLE abort clears the tag the same way.
/// It NEVER swaps the store and NEVER marks the shard passive, so it does not perturb the
/// replica-of-a-slot path (the separate replica control task owns the swap/passive lifecycle) or
/// the HA-8 promotion latch (which lives entirely in the replica control task). The default static
/// path + raft-without-migration never reach here (no IMPORTING tag is ever committed).
async fn run_import_control(
    rt: TokioRuntime,
    cluster: std::sync::Arc<ironcache_cluster::SlotMap>,
    store_rc: Rc<RefCell<ShardStoreImpl>>,
    security: Option<ClusterSecurity>,
) {
    loop {
        // H1 DUAL-ROLE GUARD (fail safe, never corrupt): the import role merges ADDITIVELY into
        // the LIVE store, but the replica role SWAPS the whole store + marks it PASSIVE. On the
        // single-shard model the two cannot share one `Rc<RefCell<ShardStoreImpl>>` (an additive
        // insert into a passive mirror has wrong delete semantics; a replica store swap would
        // discard the importer's merged keys). So if THIS shard is acting as a passive replica --
        // or is a committed replica of any slot (the replica control task is about to / has
        // attached + swapped) -- do NOT run the import: skip to the idle poll until the replica
        // role clears. The replica control task holds the complementary guard (it does not attach
        // while this node is a committed importer). One of the two roles always yields, so the
        // shard serves exactly one of {owner-importing, passive-replica} at a time -- no
        // corruption. The multi-shard generalization (a distinct shard per role) is the real fix,
        // tracked. The `is_passive` read borrows the store only for the synchronous flag check.
        let acting_passive = store_rc.borrow().is_passive();
        if acting_passive || any_replica_of_self(&cluster).is_some() {
            rt.timer(POLL_INTERVAL).await;
            continue;
        }
        // Is THIS node importing some slot right now? (the committed IMPORTING tag is set only on
        // the destination, so a hit means this node is the importer.)
        if let Some(slot) = any_importing_slot(&cluster) {
            // Resolve the slot's SOURCE endpoint: when IMPORTING, the recorded migration peer is
            // the SRC (the current owner), whose repl listener is at `repl_port` of its client
            // port. `None` (peer not yet resolvable mid-formation) -> back off + re-check.
            if let Some((host, src_client_port)) = cluster.migration_peer_endpoint(slot) {
                // RESOLVE the migration source's repl endpoint accepting a DNS hostname OR an IP
                // literal (k8s); `host` is re-read from the committed map each loop iteration so it
                // re-resolves per (re)dial. The old IP-only parse silently skipped a DNS-named source.
                //
                // H1: `resolve` is ASYNC (getaddrinfo on tokio's blocking pool, bounded by
                // RESOLVE_TIMEOUT via the Runtime timer seam), so a wedged resolver never freezes this
                // shard's executor; awaited with the shard's `rt`.
                if let Ok(src_addr) =
                    ironcache_clusterbus::PeerEndpoint::new(host, repl_port(src_client_port))
                        .resolve(&rt)
                        .await
                {
                    // The committed-map predicate the tail polls so it STOPS once the FLIP / abort
                    // clears the IMPORTING tag for THIS slot (re-reads the shared map each frame).
                    let importing_cluster = std::sync::Arc::clone(&cluster);
                    let is_still_importing = move || {
                        importing_cluster.migration_state(slot)
                            == ironcache_cluster::MigrationState::Importing
                    };
                    import_once(
                        &rt,
                        src_addr,
                        slot,
                        &store_rc,
                        &is_still_importing,
                        security.as_ref(),
                    )
                    .await;
                    // M2 ABORT PURGE: the import returned (link drop / FLIP / abort). Purge the
                    // partially-merged slot ONLY when the import truly ENDED WITHOUT WINNING the
                    // slot: the IMPORTING tag has cleared (a committed FLIP or a STABLE abort) AND
                    // this node does NOT own the slot. Then the merged keys are ORPHANS -- purge
                    // them so an aborted migration does not LEAK memory across repeated attempts,
                    // and so they cannot resurface as live if that slot is later assigned here
                    // without a fresh migration. A committed FLIP-TO-SELF leaves `owns(slot)` true
                    // -> KEEP every merged key (it is now ours). A bare LINK DROP while the slot is
                    // STILL IMPORTING here is NOT a purge (the tag is still set): the loop re-dials
                    // and the source's snapshot continues the merge, so in-progress data is never
                    // churned. The purge is slot-scoped (the caller owns `key_slot`; the store stays
                    // slot-agnostic) and observer-silent (these keys were never owned / replicated
                    // out, so no downstream HA-7 replica must be told to delete them).
                    if !is_still_importing() && !cluster.owns(slot) {
                        let purged = store_rc
                            .borrow_mut()
                            .remove_keys_where(|key| key_slot(key) == slot);
                        let _ = purged; // count is for tests / future observability.
                    }
                    // Back off briefly, then the loop re-checks the map: if the slot still IMPORTs
                    // here, re-dial; if the FLIP cleared it, `any_importing_slot` is now None and we
                    // idle.
                    rt.timer(RECONNECT_BACKOFF).await;
                    continue;
                }
            }
            // Source not resolvable yet: back off and re-check.
            rt.timer(RECONNECT_BACKOFF).await;
        } else {
            // Not importing anything (the steady state until a SetSlotImporting commits, OR after
            // the FLIP made this node the owner): idle poll.
            rt.timer(POLL_INTERVAL).await;
        }
    }
}

/// Drive ONE import attach to the SOURCE at `src_addr` for `slot`: dial, send the scoped attach
/// (`IMPORTREQ slot` then `REPLCONF`), apply the scoped full-sync ADDITIVELY into the LIVE store,
/// then tail the scoped mutation stream additively until the link drops OR the FLIP clears the
/// IMPORTING tag (the caller re-checks the map and stops). Returns when any terminal condition is
/// reached (dial fail, sync fail, link drop, or the import is no longer wanted); the caller backs
/// off + re-dials while the slot is still IMPORTING here.
///
/// `is_still_importing` is the committed-map predicate the tail polls so it STOPS promptly once
/// the FLIP (or a STABLE abort) clears the IMPORTING tag -- the importer then owns the slot (or
/// the migration aborted) and must not keep pulling.
async fn import_once<F>(
    rt: &TokioRuntime,
    src_addr: SocketAddr,
    slot: u16,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    is_still_importing: &F,
    security: Option<&ClusterSecurity>,
) where
    F: Fn() -> bool,
{
    // Dial the source's repl endpoint. A failed dial returns; the caller backs off + retries.
    let Ok(tcp) = rt.connect(src_addr).await else {
        return;
    };
    // PROD-3: TLS-secure the dialed link (CA-verify the source + present the shared secret) when
    // cluster security is on; `None` -> plaintext `SecureStream::Plain` (byte-identical). A
    // TLS/secret failure returns so the caller backs off + re-dials.
    let Some(stream) = secure_dial(security, tcp).await else {
        return;
    };
    let stream = Rc::new(RefCell::new(Some(stream)));

    // Scope the attach to THIS slot: IMPORTREQ first (so the source filters its snapshot + tail),
    // then the REPLCONF handshake the source's serve loop waits for. ack 0 -> full sync from
    // scratch (the importer holds no prior offset for the slot in this MVP).
    if !send_bytes_ok(rt, &stream, Frame::ImportReq { slot }.encode()).await {
        return;
    }
    let handshake = Frame::ReplConf {
        node: 0,
        ack: ReplOffset::ZERO,
        // A scoped import NEVER resumes (it always re-snapshots the slot), so it carries no history
        // token; the source's resume gate sees `slot_filter` Some and never resumes regardless.
        resume_token: None,
    }
    .encode();
    if !send_bytes_ok(rt, &stream, handshake).await {
        return;
    }

    // Receive the scoped full-sync ADDITIVELY into the LIVE store (no fresh store, no swap, no
    // passive). Returns the cut offset on SYNCEND, or None on a mid-sync drop (the caller
    // re-dials). The live store keeps all its OTHER slots' keys throughout.
    let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
    let queue = Rc::new(RefCell::new(std::collections::VecDeque::<Frame>::new()));
    if receive_scoped_snapshot(rt, &stream, store_rc, &pending, &queue)
        .await
        .is_none()
    {
        return; // the sync did not complete; re-dial.
    }

    // Tail the scoped mutation stream additively until the link drops or the import is no longer
    // wanted (the FLIP / abort cleared the IMPORTING tag). Reuses the same recv buffers so any
    // tail frames that raced ahead of SYNCEND are already queued.
    run_import_tail(
        rt,
        &stream,
        slot,
        store_rc,
        &pending,
        &queue,
        is_still_importing,
    )
    .await;
}

/// Receive a SCOPED full-sync ADDITIVELY into the LIVE store (HA-6 import): on `FULLSYNC` start
/// loading, apply each `SyncKv` via `insert_object` (additive, into the existing store), and on
/// `SyncEnd` return the cut offset. Returns `None` on a mid-sync drop / malformed entry / EOF
/// before SYNCEND (the caller re-dials). Unlike [`receive_full_sync`] this NEVER builds a fresh
/// store and NEVER swaps -- the importer's other slots must survive, so the snapshot is merged in.
///
/// No `RefCell` borrow crosses an `.await`: each frame is recv'd (store borrow not held), THEN the
/// store is borrowed only for the synchronous `insert_object`, THEN dropped before the next recv.
async fn receive_scoped_snapshot(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<SecureStream>>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    pending: &Rc<RefCell<Vec<u8>>>,
    queue: &Rc<RefCell<std::collections::VecDeque<Frame>>>,
) -> Option<ReplOffset> {
    let mut began = false;
    loop {
        let frame = next_frame(rt, stream, pending, queue).await?;
        match frame {
            Frame::FullSync { .. } => began = true,
            Frame::SyncKv {
                db, kvobj_bytes, ..
            } => {
                if !began {
                    return None; // a data frame before FULLSYNC: protocol violation, re-dial.
                }
                let obj = decode_kvobj(&kvobj_bytes)?; // malformed -> re-dial.
                // ADDITIVE merge into the LIVE store: insert the slot's key alongside this node's
                // existing keys (every other slot untouched).
                store_rc.borrow_mut().insert_object(db, obj);
            }
            Frame::SyncEnd { end_offset } => return Some(end_offset),
            // A stray heartbeat, or a pre-SYNCEND StreamPut/StreamDel. This arm DISCARDS the frame
            // (it was popped from the queue here, NOT deferred into the tail). That is safe ONLY
            // because of the SOURCE ORDERING invariant: the in-tree source sends the WHOLE scoped
            // snapshot before ANY tail frame -- `drive_full_sync_chunked` ships every SyncKv and
            // the SyncEnd, and RETURNS, before `serve_replica_conn` enters its `drain_and_ship`
            // tail loop. So no StreamPut/StreamDel can arrive before SYNCEND, and this arm only
            // ever drops genuine non-data frames (a stray heartbeat). It is NOT relied upon that
            // "the tail loop drains it afterwards" (it does not -- the frame is gone); the
            // correctness rests on the source never emitting a tail frame inside the snapshot.
            _ => {}
        }
    }
}

/// The IMPORT STEADY-STATE TAIL (HA-6): recv the SCOPED stream and apply each put/del ADDITIVELY
/// into the LIVE store until the link drops OR the slot is no longer IMPORTING here (the committed
/// FLIP / abort cleared it). Offset-agnostic by design (the source filtered the stream to the
/// slot, so the importer sees a subsequence with expected gaps): every scoped frame is applied
/// unconditionally + idempotently (a put overwrites in place, a delete of an absent key is a
/// no-op), which is correct because the snapshot loaded first and the tail carries every write
/// after the cut in wire order (last-write-wins).
///
/// No `RefCell` borrow crosses an `.await`: recv (no store borrow), then borrow for the
/// synchronous apply, then drop before the next recv. The IMPORTING re-check is a cold map read
/// between frames (never the hot path).
async fn run_import_tail<F>(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<SecureStream>>>,
    slot: u16,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    pending: &Rc<RefCell<Vec<u8>>>,
    queue: &Rc<RefCell<std::collections::VecDeque<Frame>>>,
    is_still_importing: &F,
) where
    F: Fn() -> bool,
{
    loop {
        // Stop promptly once the FLIP (or a STABLE abort) cleared the IMPORTING tag: this node now
        // OWNS the slot (the source serves MOVED) and must not keep pulling, or the migration was
        // aborted. Checked BEFORE the next recv so a freshly-committed FLIP ends the import on the
        // next frame boundary without waiting for the source to close the link.
        if !is_still_importing() {
            return;
        }
        let Some(frame) = next_frame(rt, stream, pending, queue).await else {
            return; // link dropped / source closed: the caller re-checks the map + re-dials.
        };
        // Apply the scoped frame additively. A frame for a key NOT in our slot should never arrive
        // (the source filters), but defend anyway: drop it rather than pollute another slot.
        let now = now_from_env();
        let mut store = store_rc.borrow_mut();
        match frame {
            Frame::StreamPut {
                db,
                key,
                kvobj_bytes,
                ..
            } => {
                if key_slot(&key) == slot {
                    if let Some(obj) = decode_kvobj(&kvobj_bytes) {
                        store.insert_object(db, obj);
                    }
                    // A malformed post-image on a scoped import is dropped (not a full-resync
                    // trigger): the next snapshot on re-dial re-establishes the key. Defensive;
                    // the source encodes with the same codec, so this never fires in practice.
                }
            }
            Frame::StreamDel { db, key, .. } => {
                if key_slot(&key) == slot {
                    use ironcache_storage::Store;
                    store.delete(db, &key, now);
                }
            }
            // A non-stream frame (a stray heartbeat): nothing to apply.
            _ => {}
        }
        // The store borrow drops here at end of scope, before the next recv.
        drop(store);
    }
}

/// The replication offset a STREAM frame (a `StreamPut` / `StreamDel`) carries, or `None` for a
/// non-stream frame (a stray heartbeat / handshake). Used by the replica tail to observe the
/// master's head (HA-7e); a stream frame's offset is a lower bound on the master head.
fn stream_frame_offset(frame: &Frame) -> Option<ReplOffset> {
    match frame {
        Frame::StreamPut { offset, .. } | Frame::StreamDel { offset, .. } => Some(*offset),
        _ => None,
    }
}

/// Whether `key` belongs in a transfer scoped by `slot_filter` (HA-6 data-copy). `None` (a plain
/// whole-store replica attach) accepts EVERY key, so the replica path is byte-identical; `Some(s)`
/// (an importing destination scoped to slot `s`) accepts only keys whose `key_slot` is `s`. Pure
/// (CRC16/XMODEM over the hash-tag), no clock / rand / IO.
#[must_use]
fn key_in_slot(key: &[u8], slot_filter: Option<u16>) -> bool {
    match slot_filter {
        None => true,
        Some(slot) => key_slot(key) == slot,
    }
}

/// Whether a STREAM frame (a `StreamPut` / `StreamDel`) belongs in a tail scoped by `slot_filter`
/// (HA-6 data-copy). A non-stream frame (a stray heartbeat) is always kept (it carries no key);
/// a stream frame is kept iff its key hashes to the scoped slot. `None` keeps everything (the
/// byte-identical replica tail).
#[must_use]
fn frame_in_slot(frame: &Frame, slot_filter: Option<u16>) -> bool {
    match frame {
        Frame::StreamPut { key, .. } | Frame::StreamDel { key, .. } => {
            key_in_slot(key, slot_filter)
        }
        _ => true,
    }
}

// ===========================================================================================
// SHARED I/O HELPERS (the take/put-back stream idiom; no borrow across await)
// ===========================================================================================

/// Send `bytes` on the take/put-back stream, returning `Result<(), ()>` (the sink shape the
/// repl primitives' `send` closure wants). The stream is taken out, the I/O awaited on the
/// owned value, and put back, so no `RefCell` borrow crosses the await.
async fn send_bytes(
    _rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<SecureStream>>>,
    bytes: Vec<u8>,
) -> Result<(), ()> {
    let mut s = stream.borrow_mut().take().expect("stream present");
    let res = s.send(bytes).await;
    *stream.borrow_mut() = Some(s);
    res.map(|_| ()).map_err(|_| ())
}

/// [`send_bytes`] returning a plain `bool` (`true` on success), for the handshake send where a
/// `Result<(), ()>` is not the consumed shape.
async fn send_bytes_ok(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<SecureStream>>>,
    bytes: Vec<u8>,
) -> bool {
    send_bytes(rt, stream, bytes).await.is_ok()
}

/// Pull the next complete [`Frame`] from the take/put-back stream, buffering partial reads in
/// `pending` and decoded-but-not-yet-returned frames in `queue` (so a single read that yields
/// several frames returns them one at a time). Returns `None` on a clean close / I/O error /
/// malformed frame. No `RefCell` borrow crosses the recv await (the stream is taken out for it).
async fn next_frame(
    _rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<SecureStream>>>,
    pending: &Rc<RefCell<Vec<u8>>>,
    queue: &Rc<RefCell<std::collections::VecDeque<Frame>>>,
) -> Option<Frame> {
    loop {
        if let Some(f) = queue.borrow_mut().pop_front() {
            return Some(f);
        }
        // Decode every complete frame already buffered into the queue.
        let mut drained_any = false;
        loop {
            let decoded = Frame::decode(&pending.borrow());
            match decoded {
                Ok(Some((frame, consumed))) => {
                    pending.borrow_mut().drain(..consumed);
                    queue.borrow_mut().push_back(frame);
                    drained_any = true;
                }
                Ok(None) => break,              // need more bytes.
                Err(FrameError) => return None, // malformed: abort the link.
            }
        }
        if drained_any {
            continue;
        }
        // Need more bytes: read a chunk (TAKE/put-back so no borrow crosses the await).
        let taken: Vec<u8> = core::mem::take(&mut *pending.borrow_mut());
        let mut s = stream.borrow_mut().take().expect("stream present");
        let res = s.recv(taken).await;
        *stream.borrow_mut() = Some(s);
        match res {
            Ok(r) => {
                if r.n == 0 {
                    return None; // peer closed.
                }
                *pending.borrow_mut() = r.buf;
            }
            Err(_) => return None,
        }
    }
}

/// Read `now` (absolute wall-clock millis) from THIS shard's `SystemEnv` clock seam (ADR-0003,
/// never `std::time` directly). Used for the snapshot scan's lazy-expiry skip and the replica
/// apply's lazy-expiry probe on a delete.
fn now_from_env() -> UnixMillis {
    UnixMillis(crate::serve::shard_env().borrow().now_unix_millis())
}

thread_local! {
    /// Whether THIS shard has already wired up its replica-attach machinery (the observer +
    /// listener + control task). Guards [`spawn_on_shard`] so a repeated call is a no-op (the
    /// observer is installed once, the listener bound once). A plain `Cell` (single-threaded
    /// per shard, shared-nothing ADR-0002). Defaults `false`; only ever set on the raft-mode
    /// path, so the default static path never touches it.
    static PRIMARY_STARTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #365 stage 1: the id a replica advertises in `REPLCONF` is `node_id_from_announce` of its
    /// own announce id (the first 16 hex chars as a `u64`), and it MUST equal what the primary
    /// re-derives for the SAME announce id (the resolution invariant). Sending the prior `0` would
    /// have left the primary unable to identify the replica.
    #[test]
    fn replica_handshake_node_id_is_the_announce_prefix_and_round_trips() {
        // First 16 hex of the 40-hex id, as a u64.
        assert_eq!(replica_handshake_node_id(&"0".repeat(40)), 0);
        assert_eq!(
            replica_handshake_node_id(&"1".repeat(40)),
            0x1111_1111_1111_1111
        );
        // "00000000000000ff...." -> first 16 = "00000000000000ff" = 0xff.
        let id = "00000000000000ff1234567890abcdef00000000";
        assert_eq!(replica_handshake_node_id(id), 0xff);
        // The resolution invariant: it is exactly what the primary derives for the same id.
        assert_eq!(
            replica_handshake_node_id(id),
            crate::raft_boot::node_id_from_announce(id).0
        );
        // And it is no longer the old placeholder for a non-trivial id.
        assert_ne!(replica_handshake_node_id(id), 0);
    }

    /// `repl_port` is the documented client-port + offset, overflow-safe (a high port falls
    /// back to `port - offset`), and a bijection on distinct client ports (so co-located nodes
    /// never collide).
    #[test]
    fn repl_port_offsets_and_is_overflow_safe() {
        assert_eq!(repl_port(6379), 6379 + REPL_PORT_OFFSET);
        // A high port that would overflow falls back to `port - offset`.
        let high = u16::MAX - 1;
        assert_eq!(repl_port(high), high - REPL_PORT_OFFSET);
        // Bijection: two distinct client ports map to two distinct repl ports.
        assert_ne!(repl_port(6379), repl_port(6380));
        assert_ne!(repl_port(high), repl_port(high - 1));
    }

    /// `any_replica_of_self` is `None` on a fresh map (this node replicates nothing) and
    /// `Some(slot)` once an AssignReplica for self is committed -- the single-shard "should
    /// this shard be a replica" predicate. Uses the cluster crate's public surface to build a
    /// committed-replica state the way the ConfigSm apply does (`set_slot_replica`).
    #[test]
    fn any_replica_of_self_tracks_committed_assignment() {
        let id0 = "0000000000000000000000000000000000000000";
        let id1 = "1111111111111111111111111111111111111111";
        // A 2-node static map; self == id1 (owns the upper half, replicates nothing yet).
        let map = ironcache_cluster::SlotMap::build(
            vec![
                (
                    ironcache_cluster::NodeEntry {
                        id: id0.into(),
                        host: "10.0.0.10".into(),
                        port: 6379,
                    },
                    vec![[0, 8191]],
                ),
                (
                    ironcache_cluster::NodeEntry {
                        id: id1.into(),
                        host: "10.0.0.11".into(),
                        port: 6379,
                    },
                    vec![[8192, 16383]],
                ),
            ],
            id1,
        )
        .expect("valid 2-node map");

        // Fresh: self replicates nothing.
        assert_eq!(any_replica_of_self(&map), None);

        // Commit "self (id1) replicates slot 0" exactly as the ConfigSm apply does.
        map.set_slot_replica(0, id1).expect("known node");
        assert_eq!(any_replica_of_self(&map), Some(0));
        // The owner of slot 0 is still id0 (replica assignment does not change ownership), so a
        // replica resolves the owner endpoint via moved_target.
        assert_eq!(map.moved_target(0), Some(("10.0.0.10".to_string(), 6379)));
    }

    /// A 2-node map with self (id1) committed as a replica of two id0-owned slots, the way a
    /// committed AssignReplica leaves it.
    fn map_self_replicates_two_slots() -> ironcache_cluster::SlotMap {
        let id0 = "0000000000000000000000000000000000000000";
        let id1 = "1111111111111111111111111111111111111111";
        let map = ironcache_cluster::SlotMap::build(
            vec![
                (
                    ironcache_cluster::NodeEntry {
                        id: id0.into(),
                        host: "10.0.0.10".into(),
                        port: 6379,
                    },
                    vec![[0, 8191]],
                ),
                (
                    ironcache_cluster::NodeEntry {
                        id: id1.into(),
                        host: "10.0.0.11".into(),
                        port: 6379,
                    },
                    vec![[8192, 16383]],
                ),
            ],
            id1,
        )
        .expect("valid 2-node map");
        // self (id1) replicates two slots id0 owns.
        map.set_slot_replica(0, id1).expect("known node");
        map.set_slot_replica(7, id1).expect("known node");
        map
    }

    /// THE LAG GATE (HA-8, the safety half of the failover decision). `promotion_proposal` proposes
    /// the replicated slots ONLY when the node was in sync at last contact; a NOT-in-sync (stale)
    /// replica is refused (`None`) so a committed promotion can never lose the writes it had not yet
    /// pulled. This drives the gate decision directly (the `if !in_sync` branch), which the DST
    /// split-brain gate proves the APPLY of but does not itself exercise.
    #[test]
    fn promotion_proposal_lag_gate_refuses_a_stale_replica() {
        let map = map_self_replicates_two_slots();

        // In sync -> propose exactly the slots this node replicates.
        assert_eq!(
            promotion_proposal(&map, true),
            Some(vec![0, 7]),
            "an in-sync replica proposes its replicated slots"
        );
        // NOT in sync (stale / past the lag bound at last contact) -> propose NOTHING.
        assert_eq!(
            promotion_proposal(&map, false),
            None,
            "a stale replica must NEVER be promoted (the lag gate)"
        );
    }

    /// `promotion_proposal` is also `None` for an in-sync node that replicates NO slot (nothing to
    /// promote) -- the second half of the decision, so an idle in-sync node never proposes a
    /// no-op (empty-slots) promotion.
    #[test]
    fn promotion_proposal_is_none_when_node_replicates_nothing() {
        let id0 = "0000000000000000000000000000000000000000";
        let id1 = "1111111111111111111111111111111111111111";
        let map = ironcache_cluster::SlotMap::build(
            vec![
                (
                    ironcache_cluster::NodeEntry {
                        id: id0.into(),
                        host: "10.0.0.10".into(),
                        port: 6379,
                    },
                    vec![[0, 8191]],
                ),
                (
                    ironcache_cluster::NodeEntry {
                        id: id1.into(),
                        host: "10.0.0.11".into(),
                        port: 6379,
                    },
                    vec![[8192, 16383]],
                ),
            ],
            id1,
        )
        .expect("valid 2-node map");
        // In sync but replicates nothing -> no proposal.
        assert_eq!(promotion_proposal(&map, true), None);
    }

    /// THE HA-8 LATCH FIX (regression): the in-sync verdict the promotion gate uses is LATCHED at
    /// the last real master contact, not recomputed each iteration. The load-bearing case is a
    /// DIAL-FAIL after an in-sync contact (`was_attached=false`, `observed=false` because the link
    /// is down): the latch MUST stay `true` so the gate -- which only fires several dial-fail
    /// iterations later, once the failover timeout elapses -- still promotes. The pre-fix code used
    /// `observed` directly, which is exactly this `false`, so it never promoted a dead master's
    /// in-sync replica. This test pins the latch rule.
    #[test]
    fn latch_in_sync_keeps_last_contact_verdict_across_dial_fails() {
        // A real attach adopts the observed verdict.
        assert!(
            latch_in_sync(true, true, false),
            "in-sync attach latches true"
        );
        assert!(
            !latch_in_sync(true, false, true),
            "a lagging attach latches false (overrides a stale true)"
        );
        // THE FIX: a dial-fail (link down -> observed false) KEEPS the prior latch, so an in-sync
        // replica of a now-dead master stays promotable until the timeout elapses.
        assert!(
            latch_in_sync(false, false, true),
            "a dial-fail must NOT clear an in-sync latch (the bug the live AWS test caught)"
        );
        // A dial-fail with no prior in-sync contact stays not-in-sync (never promote a never-synced
        // replica).
        assert!(
            !latch_in_sync(false, false, false),
            "no prior contact stays not-in-sync"
        );
    }

    /// END-TO-END over real loopback TCP: a primary shard with an installed [`ReplObserver`]
    /// drives the WIRED [`drive_full_sync_chunked`] (the CARRY-FORWARD 1 capture + chunked,
    /// borrow-releasing driver) to a replica that runs the SAME [`receive_full_sync`] the live
    /// attach path uses, and the replica's freshly-loaded store matches the primary key-for-key
    /// AND adopts the captured cut offset. This exercises the load-bearing wired logic (the
    /// chunked snapshot driver + the cut capture) the live serve path runs, end to end.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn drive_full_sync_chunked_loopback_matches_primary() {
        use ironcache_storage::{ExpireWrite, NewValue, Store};
        use std::collections::VecDeque;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            // A free loopback port for the primary listener.
            let port = {
                let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                l.local_addr().unwrap().port()
            };
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let listener = bind_exclusive(addr).unwrap();

            // The PRIMARY shard store + its observer/ring (exactly the spawn_on_shard wiring),
            // populated with a spread of keys/dbs so the transfer spans multiple chunks.
            let store_rc: Rc<RefCell<ShardStoreImpl>> = Rc::new(RefCell::new(
                crate::serve::fresh_shard_store(4, "noeviction", 0),
            ));
            let ring = ReplRing::new(TAIL_RING_CAP, ReplOffset::ZERO);
            store_rc
                .borrow_mut()
                .set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
            {
                let now = now_from_env();
                let mut s = store_rc.borrow_mut();
                for i in 0..50u32 {
                    s.upsert(
                        i % 4,
                        format!("k{i:03}").as_bytes(),
                        NewValue::Bytes(format!("v{i}").as_bytes()),
                        ExpireWrite::Clear,
                        now,
                    );
                }
            }
            // After the writes the ring head is non-zero; the cut the driver captures must be it.
            let expected_cut = ring.borrow().head();
            assert_eq!(
                expected_cut,
                ReplOffset(50),
                "50 writes advanced the offset"
            );

            let expected = fingerprint(&store_rc.borrow());

            // PRIMARY task: accept one connection, read the handshake, drive the chunked sync.
            let prim_store = Rc::clone(&store_rc);
            let prim_ring = Rc::clone(&ring);
            tokio::task::spawn_local(async move {
                let rt = TokioRuntime::new();
                let (stream, _peer) = rt.accept(&listener).await.expect("accept");
                let stream = Rc::new(RefCell::new(Some(SecureStream::plain(stream))));
                let mut pending = Vec::new();
                let attached = read_attach_handshake(&rt, &stream, &mut pending).await;
                if attached.is_none() {
                    return;
                }
                let replid = ReplId::from_bytes([0xCD; 20]);
                // Plain whole-store attach (no slot filter): the byte-identical HA-7d path.
                let _ =
                    drive_full_sync_chunked(&rt, &stream, replid, &prim_store, &prim_ring, None)
                        .await;
            });

            // REPLICA task: dial, send REPLCONF, receive_full_sync into a fresh store.
            let result = tokio::task::spawn_local(async move {
                let rt = TokioRuntime::new();
                let stream = loop {
                    match rt.connect(addr).await {
                        Ok(s) => break s,
                        Err(_) => rt.timer(Duration::from_millis(10)).await,
                    }
                };
                let stream = Rc::new(RefCell::new(Some(SecureStream::plain(stream))));
                let handshake = Frame::ReplConf {
                    node: 7,
                    ack: ReplOffset::ZERO,
                    resume_token: None,
                }
                .encode();
                send_bytes(&rt, &stream, handshake)
                    .await
                    .expect("handshake");

                let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
                let queue = Rc::new(RefCell::new(VecDeque::<Frame>::new()));
                receive_full_sync(
                    || crate::serve::fresh_shard_store(4, "noeviction", 0),
                    move || {
                        let pending = Rc::clone(&pending);
                        let queue = Rc::clone(&queue);
                        let stream = Rc::clone(&stream);
                        async move { next_frame(&rt, &stream, &pending, &queue).await }
                    },
                )
                .await
                .map(|l| (fingerprint(&l.store), l.end_offset))
            })
            .await
            .expect("replica task joined");

            let (got, cut) = result.expect("the full sync completed");
            assert_eq!(
                cut, expected_cut,
                "the replica adopts the captured cut offset"
            );
            assert_eq!(
                got, expected,
                "the replica's store matches the primary key-for-key after SYNCEND"
            );
        });
    }

    /// A consumer for `two_consumers_on_one_source_ring_each_receive_every_tail_op`: dial the
    /// source, REPLCONF (plain whole-store attach), drain the snapshot to SYNCEND, then collect the
    /// tail StreamPut keys until it has seen the post-attach key `live`, returning the tail key set.
    async fn two_consumer_collect_tail(addr: SocketAddr) -> std::collections::BTreeSet<Vec<u8>> {
        let rt = TokioRuntime::new();
        let stream = loop {
            match rt.connect(addr).await {
                Ok(s) => break s,
                Err(_) => rt.timer(Duration::from_millis(10)).await,
            }
        };
        let stream = Rc::new(RefCell::new(Some(SecureStream::plain(stream))));
        send_bytes(
            &rt,
            &stream,
            Frame::ReplConf {
                node: 0,
                ack: ReplOffset::ZERO,
                resume_token: None,
            }
            .encode(),
        )
        .await
        .expect("replconf");

        let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
        let queue = Rc::new(RefCell::new(std::collections::VecDeque::<Frame>::new()));
        let mut tail_keys: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        let mut past_syncend = false;
        loop {
            let Some(frame) = next_frame(&rt, &stream, &pending, &queue).await else {
                break;
            };
            match frame {
                Frame::SyncEnd { .. } => past_syncend = true,
                Frame::StreamPut { key, .. } if past_syncend => {
                    let done = key == b"live";
                    tail_keys.insert(key);
                    if done {
                        break; // saw the post-attach write; done.
                    }
                }
                _ => {}
            }
        }
        tail_keys
    }

    /// C1 OVER REAL LOOPBACK (the production serve path): TWO concurrent consumers attach to ONE
    /// source shard's ring via the live [`serve_replica_conn`] -- each on its OWN connection. A
    /// write made on the source AFTER both have attached must reach BOTH through the tail. With the
    /// OLD shared `send_cursor` the first connection's `drain_and_ship` advanced the one shared
    /// cursor past the new op, so the SECOND connection silently shipped NOTHING for it (the split
    /// / lost write). With the per-connection cursor each connection ships every op past its OWN
    /// cursor, so BOTH receivers observe the post-attach write. This drives the exact wired path
    /// (`serve_replica_conn` -> `drain_and_ship(&ring, &mut send_cursor, ...)`) the live listener
    /// runs, with two consumers on one ring.
    #[test]
    fn two_consumers_on_one_source_ring_each_receive_every_tail_op() {
        use ironcache_storage::{ExpireWrite, NewValue, Store};

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            let port = {
                let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                l.local_addr().unwrap().port()
            };
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let listener = bind_exclusive(addr).unwrap();

            // The SOURCE shard store + observer/ring (the spawn_on_shard wiring).
            let store_rc: Rc<RefCell<ShardStoreImpl>> = Rc::new(RefCell::new(
                crate::serve::fresh_shard_store(2, "noeviction", 0),
            ));
            let ring = ReplRing::new(TAIL_RING_CAP, ReplOffset::ZERO);
            store_rc
                .borrow_mut()
                .set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
            // One pre-existing key (part of the snapshot both consumers receive).
            store_rc.borrow_mut().upsert(
                0,
                b"pre",
                NewValue::Bytes(b"v0"),
                ExpireWrite::Clear,
                now_from_env(),
            );

            // The SOURCE serves EACH accepted connection with the production `serve_replica_conn`,
            // both draining the SAME ring (the two-consumer fan-out). Accept exactly two.
            let src_store = Rc::clone(&store_rc);
            let src_ring = Rc::clone(&ring);
            tokio::task::spawn_local(async move {
                let rt = TokioRuntime::new();
                let replid = ReplId::from_bytes([0x5a; 20]);
                for _ in 0..2 {
                    let (stream, _peer) = rt.accept(&listener).await.expect("accept");
                    let store = Rc::clone(&src_store);
                    let ring = Rc::clone(&src_ring);
                    let status = std::sync::Arc::new(ReplNodeStatus::new());
                    // This test exercises the two-consumer tail fan-out, not the write-side
                    // guardrail: pass a fresh in-sync count cell + a generous lag bound (the count
                    // is maintained but unread here).
                    let in_sync = std::sync::Arc::new(InSyncReplicas::new());
                    tokio::task::spawn_local(async move {
                        serve_replica_conn(
                            TokioRuntime::new(),
                            SecureStream::plain(stream),
                            replid,
                            store,
                            ring,
                            status,
                            in_sync,
                            u64::MAX,
                        )
                        .await;
                    });
                }
                // Keep the listener task alive for the duration (the connections were spawned).
                rt.timer(Duration::from_secs(5)).await;
            });

            // Start BOTH consumers (the helper `two_consumer_collect_tail` dials, REPLCONFs,
            // drains the snapshot, then collects tail StreamPut keys until it sees `live`).
            let c1 = tokio::task::spawn_local(two_consumer_collect_tail(addr));
            let c2 = tokio::task::spawn_local(two_consumer_collect_tail(addr));
            // After both have connected, make a POST-ATTACH write on the source. It must fan out to
            // BOTH consumers' tails. A short timer lets both handshakes + snapshots complete first.
            let writer_store = Rc::clone(&store_rc);
            tokio::task::spawn_local(async move {
                let rt = TokioRuntime::new();
                rt.timer(Duration::from_millis(150)).await;
                writer_store.borrow_mut().upsert(
                    0,
                    b"live",
                    NewValue::Bytes(b"v1"),
                    ExpireWrite::Clear,
                    now_from_env(),
                );
            });

            let r1 = c1.await.expect("consumer 1 joined");
            let r2 = c2.await.expect("consumer 2 joined");
            assert!(
                r1.contains(b"live".as_slice()),
                "consumer 1 received the post-attach tail write"
            );
            assert!(
                r2.contains(b"live".as_slice()),
                "consumer 2 ALSO received it -- the tail fanned out to BOTH, no split"
            );
        });
    }

    /// THE PER-SLOT SNAPSHOT FILTER (HA-6 source): with `slot_filter == Some(slot)`, the chunked
    /// driver ships ONLY keys hashing to that slot; with `None` (the replica path) it ships every
    /// key. Proven over real loopback: a primary populated with keys spread across many slots,
    /// scoped to ONE slot, transfers exactly that slot's keys to the receiver and nothing else.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn scoped_snapshot_ships_only_the_slots_keys() {
        use ironcache_storage::{ExpireWrite, NewValue, Store};
        use std::collections::VecDeque;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            let port = {
                let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                l.local_addr().unwrap().port()
            };
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let listener = bind_exclusive(addr).unwrap();

            // Populate a primary across MANY slots. Pick the target slot as the slot of one key,
            // then record exactly which of the populated keys hash to it (the expected scoped set).
            let store_rc: Rc<RefCell<ShardStoreImpl>> = Rc::new(RefCell::new(
                crate::serve::fresh_shard_store(2, "noeviction", 0),
            ));
            let ring = ReplRing::new(TAIL_RING_CAP, ReplOffset::ZERO);
            store_rc
                .borrow_mut()
                .set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
            let keys: Vec<String> = (0..120u32).map(|i| format!("key{i:04}")).collect();
            let target_slot = key_slot(keys[0].as_bytes());
            let mut expected: Vec<(u32, Vec<u8>)> = Vec::new();
            {
                let now = now_from_env();
                let mut s = store_rc.borrow_mut();
                for (i, k) in keys.iter().enumerate() {
                    let db = (i % 2) as u32;
                    s.upsert(
                        db,
                        k.as_bytes(),
                        NewValue::Bytes(b"v"),
                        ExpireWrite::Clear,
                        now,
                    );
                    if key_slot(k.as_bytes()) == target_slot {
                        expected.push((db, k.as_bytes().to_vec()));
                    }
                }
            }
            expected.sort();
            assert!(
                !expected.is_empty(),
                "the target slot must have at least one key"
            );

            // SOURCE: accept, read the (scoped) handshake, drive the SCOPED full-sync.
            let prim_store = Rc::clone(&store_rc);
            let prim_ring = Rc::clone(&ring);
            tokio::task::spawn_local(async move {
                let rt = TokioRuntime::new();
                let (stream, _peer) = rt.accept(&listener).await.expect("accept");
                let stream = Rc::new(RefCell::new(Some(SecureStream::plain(stream))));
                let mut pending = Vec::new();
                let Some((slot_filter, _ack, _token, _node)) =
                    read_attach_handshake(&rt, &stream, &mut pending).await
                else {
                    return;
                };
                assert_eq!(
                    slot_filter,
                    Some(target_slot),
                    "the source reads the IMPORTREQ scope from the handshake"
                );
                let replid = ReplId::from_bytes([0xAB; 20]);
                let _ = drive_full_sync_chunked(
                    &rt,
                    &stream,
                    replid,
                    &prim_store,
                    &prim_ring,
                    slot_filter,
                )
                .await;
            });

            // IMPORTER: dial, send IMPORTREQ(target) then REPLCONF, COLLECT the SYNCKV keys.
            let got = tokio::task::spawn_local(async move {
                let rt = TokioRuntime::new();
                let stream = loop {
                    match rt.connect(addr).await {
                        Ok(s) => break s,
                        Err(_) => rt.timer(Duration::from_millis(10)).await,
                    }
                };
                let stream = Rc::new(RefCell::new(Some(SecureStream::plain(stream))));
                send_bytes(
                    &rt,
                    &stream,
                    Frame::ImportReq { slot: target_slot }.encode(),
                )
                .await
                .expect("importreq");
                send_bytes(
                    &rt,
                    &stream,
                    Frame::ReplConf {
                        node: 0,
                        ack: ReplOffset::ZERO,
                        resume_token: None,
                    }
                    .encode(),
                )
                .await
                .expect("replconf");

                let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
                let queue = Rc::new(RefCell::new(VecDeque::<Frame>::new()));
                let mut out: Vec<(u32, Vec<u8>)> = Vec::new();
                loop {
                    let Some(frame) = next_frame(&rt, &stream, &pending, &queue).await else {
                        break;
                    };
                    match frame {
                        Frame::SyncKv { db, key, .. } => out.push((db, key)),
                        Frame::SyncEnd { .. } => break,
                        _ => {}
                    }
                }
                out.sort();
                out
            })
            .await
            .expect("importer joined");

            assert_eq!(
                got, expected,
                "the scoped snapshot ships EXACTLY the target slot's keys, no others"
            );
        });
    }

    /// THE ADDITIVE DEST APPLY (HA-6 import): `receive_scoped_snapshot` merges the scoped snapshot
    /// into the EXISTING live store via `insert_object`, leaving every OTHER slot's pre-existing
    /// data intact (it never swaps a fresh store in, unlike the replica attach). Proven directly on
    /// a store: pre-load an unrelated key, additively insert a slot key, assert BOTH survive.
    #[test]
    fn additive_insert_leaves_other_slots_intact() {
        use ironcache_storage::{ExpireWrite, NewValue, Store};

        let mut store = crate::serve::fresh_shard_store(2, "noeviction", 0);
        let now = now_from_env();
        // Pre-existing data on the dest in some OTHER slot (an owned slot the importer must keep).
        store.upsert(
            0,
            b"keep-me",
            NewValue::Bytes(b"original"),
            ExpireWrite::Clear,
            now,
        );
        let kept_slot = key_slot(b"keep-me");

        // The migrating slot's key, ADDITIVELY inserted (the same call `receive_scoped_snapshot`
        // uses). Pick a key that is in a DIFFERENT slot than `keep-me` so the two never collide.
        let mut migrated_key = String::new();
        for i in 0..200_000u32 {
            let k = format!("m{i}");
            if key_slot(k.as_bytes()) != kept_slot {
                migrated_key = k;
                break;
            }
        }
        assert!(!migrated_key.is_empty(), "found a distinct-slot key");
        // Build a KvObj the additive path inserts (mirror the wire round-trip: encode then decode).
        store.upsert(
            1,
            migrated_key.as_bytes(),
            NewValue::Bytes(b"imported"),
            ExpireWrite::Clear,
            now,
        );
        let obj = {
            let mut cur = SnapshotCursor::START;
            let mut found = None;
            while !cur.is_done(store.databases()) {
                let (chunk, next) = store.snapshot_chunk(cur, 64, now);
                for (db, key, kv) in chunk {
                    if db == 1 && &*key == migrated_key.as_bytes() {
                        found = Some(kv);
                    }
                }
                cur = next;
            }
            found.expect("the migrated key is in the store")
        };
        // Fresh dest store: pre-load ONLY the kept key, then ADDITIVELY insert the migrated obj.
        let mut dest = crate::serve::fresh_shard_store(2, "noeviction", 0);
        dest.upsert(
            0,
            b"keep-me",
            NewValue::Bytes(b"original"),
            ExpireWrite::Clear,
            now,
        );
        dest.insert_object(1, obj); // <-- the additive merge (no swap).

        // BOTH survive: the pre-existing other-slot key AND the additively-merged slot key.
        assert_eq!(
            dest.read(0, b"keep-me", now).unwrap().as_bytes(),
            b"original",
            "the pre-existing OTHER-slot key survives the additive import"
        );
        assert_eq!(
            dest.read(1, migrated_key.as_bytes(), now)
                .unwrap()
                .as_bytes(),
            b"imported",
            "the additively-merged migrating-slot key is present"
        );
    }

    /// M2 (the abort purge): the slot-scoped purge the import control task runs on an aborted /
    /// un-won import removes EXACTLY the migrating slot's partially-merged keys from the live store,
    /// leaving every OTHER slot's keys (the importer's owned data) intact. This is the same
    /// `remove_keys_where(|k| key_slot(k) == slot)` call the import control task issues when the
    /// import ends without owning the slot; here we drive it directly on a mixed-slot store.
    #[test]
    fn aborted_import_purge_removes_only_the_slot_keys() {
        use ironcache_storage::{ExpireWrite, NewValue, Store};

        let mut store = crate::serve::fresh_shard_store(2, "noeviction", 0);
        let now = now_from_env();

        // An OWNED key (some other slot) the importer must KEEP across the abort purge.
        store.upsert(
            0,
            b"keep-me",
            NewValue::Bytes(b"owned"),
            ExpireWrite::Clear,
            now,
        );
        let kept_slot = key_slot(b"keep-me");

        // Several partially-merged keys ALL in ONE migrating slot (the import's payload). Find a
        // hash-tag that lands in a different slot than `keep-me`, then co-locate the merged keys.
        let migrating_slot = (0..200_000u32)
            .map(|i| key_slot(format!("{{m{i}}}:x").as_bytes()))
            .find(|&s| s != kept_slot)
            .expect("a distinct slot");
        let merged: Vec<String> = (0..5)
            .map(|i| {
                // Use the SAME hash tag so every merged key hashes to `migrating_slot`.
                let tag_owner = (0..200_000u32)
                    .find(|&j| key_slot(format!("{{m{j}}}:x").as_bytes()) == migrating_slot)
                    .unwrap();
                format!("{{m{tag_owner}}}:{i}")
            })
            .collect();
        for k in &merged {
            assert_eq!(
                key_slot(k.as_bytes()),
                migrating_slot,
                "co-located in the slot"
            );
            store.upsert(
                0,
                k.as_bytes(),
                NewValue::Bytes(b"imported"),
                ExpireWrite::Clear,
                now,
            );
        }
        assert_eq!(store.len(), 1 + merged.len());

        // THE PURGE (the import-abort cleanup): remove the migrating slot's keys.
        let purged = store.remove_keys_where(|key| key_slot(key) == migrating_slot);
        assert_eq!(purged, merged.len(), "all merged slot keys purged");

        // The owned other-slot key SURVIVES; none of the slot's keys remain.
        assert_eq!(store.read(0, b"keep-me", now).unwrap().as_bytes(), b"owned");
        for k in &merged {
            assert!(
                store.read(0, k.as_bytes(), now).is_none(),
                "the aborted import's slot key {k:?} is gone (no orphan leak)"
            );
        }
        assert_eq!(store.len(), 1, "only the owned key remains");
    }

    /// The pure scope predicates: `None` (a plain replica attach) accepts every key + frame
    /// (byte-identical), `Some(slot)` accepts only that slot's keys; a non-stream frame is always
    /// kept (it carries no key).
    #[test]
    fn slot_scope_predicates_filter_correctly() {
        let slot = key_slot(b"alpha");
        // None == accept everything (the byte-identical replica path).
        assert!(key_in_slot(b"alpha", None));
        assert!(key_in_slot(b"beta", None));
        // Some(slot) accepts only that slot's keys.
        assert!(key_in_slot(b"alpha", Some(slot)));
        // A key in a different slot is rejected (find one deterministically).
        let other = (0..200_000u32)
            .map(|i| format!("x{i}"))
            .find(|k| key_slot(k.as_bytes()) != slot)
            .expect("a distinct-slot key");
        assert!(!key_in_slot(other.as_bytes(), Some(slot)));
        // frame_in_slot: a non-stream frame is always kept; a stream frame is filtered by key.
        assert!(frame_in_slot(
            &Frame::ReplPing {
                replid: ReplId::from_bytes([0u8; 20]),
                offset: ReplOffset::ZERO,
            },
            Some(slot)
        ));
        assert!(frame_in_slot(
            &Frame::StreamDel {
                offset: ReplOffset(1),
                db: 0,
                key: b"alpha".to_vec(),
            },
            Some(slot)
        ));
        assert!(!frame_in_slot(
            &Frame::StreamDel {
                offset: ReplOffset(1),
                db: 0,
                key: other.into_bytes(),
            },
            Some(slot)
        ));
    }

    /// THE PRODUCTION RESUME GATE (`serve_replica_conn`) over real loopback: a reconnecting replica
    /// is RESUMED only on an EXACT history-token match (else FULL-SYNC), AND a token-match alone does
    /// not let an ack AHEAD of head be treated as "caught up". This is the live counterpart of the
    /// repl-crate restart loopback test, exercising the actual serve gate (not a mirror):
    ///
    ///   * connection A: ack > 0 but a MISMATCHED token (a restarted-primary scenario: the replica
    ///     last synced under a DIFFERENT history) -> the primary sends a `FullSync` (the fence; pre-fix
    ///     it would have resumed and shipped nothing -> silent divergence);
    ///   * connection B: ack > 0 with the MATCHING token and a SERVEABLE offset (within the in-memory
    ///     window) -> the primary RESUMES (the first inbound frame is NOT a `FullSync`);
    ///   * connection C: the MATCHING token but ack AHEAD of head (`ack > head`) -> `FullSync` (the
    ///     ack-ahead guard: a replica claiming more than the primary has must not be called in sync).
    #[test]
    fn resume_gate_requires_matching_history_token_and_ack_not_ahead_of_head() {
        use ironcache_storage::{ExpireWrite, NewValue, Store};

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            let port = {
                let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                l.local_addr().unwrap().port()
            };
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let listener = bind_exclusive(addr).unwrap();

            // The PRIMARY shard + observer/ring, with a few writes so head > 0 and the in-memory
            // window can serve a resume from a low offset.
            let store_rc: Rc<RefCell<ShardStoreImpl>> = Rc::new(RefCell::new(
                crate::serve::fresh_shard_store(2, "noeviction", 0),
            ));
            let ring = ReplRing::new(TAIL_RING_CAP, ReplOffset::ZERO);
            store_rc
                .borrow_mut()
                .set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
            {
                let now = now_from_env();
                let mut s = store_rc.borrow_mut();
                for i in 0..5u32 {
                    s.upsert(
                        0,
                        format!("k{i}").as_bytes(),
                        NewValue::Bytes(b"v"),
                        ExpireWrite::Clear,
                        now,
                    );
                }
            }
            let head = ring.borrow().head();
            assert_eq!(head, ReplOffset(5), "5 writes advanced the head");

            // The primary's per-boot history token (what `serve_replica_conn` gates against).
            let primary_token = ReplId::from_bytes([0xA1; 20]);

            // The SOURCE: serve THREE connections with the production `serve_replica_conn`.
            let src_store = Rc::clone(&store_rc);
            let src_ring = Rc::clone(&ring);
            tokio::task::spawn_local(async move {
                let rt = TokioRuntime::new();
                for _ in 0..3 {
                    let (stream, _peer) = rt.accept(&listener).await.expect("accept");
                    let store = Rc::clone(&src_store);
                    let ring = Rc::clone(&src_ring);
                    let status = std::sync::Arc::new(ReplNodeStatus::new());
                    let in_sync = std::sync::Arc::new(InSyncReplicas::new());
                    tokio::task::spawn_local(async move {
                        serve_replica_conn(
                            TokioRuntime::new(),
                            SecureStream::plain(stream),
                            primary_token,
                            store,
                            ring,
                            status,
                            in_sync,
                            u64::MAX,
                        )
                        .await;
                    });
                }
                rt.timer(Duration::from_secs(5)).await;
            });

            // A: ack within window but a MISMATCHED token -> FULL SYNC (the silent-divergence fence).
            let wrong_token = ReplId::from_bytes([0xB2; 20]);
            assert!(
                attach_first_frame_is_full_sync(addr, ReplOffset(2), Some(wrong_token)).await,
                "a mismatched history token must force a FULL sync, never a blind resume"
            );

            // B: ack within window AND the MATCHING token -> RESUME (first frame is NOT a FullSync).
            assert!(
                !attach_first_frame_is_full_sync(addr, ReplOffset(2), Some(primary_token)).await,
                "a matching token + serveable offset resumes incrementally (no FullSync)"
            );

            // C: the MATCHING token but ack AHEAD of head -> FULL SYNC (the ack-ahead guard).
            assert!(
                attach_first_frame_is_full_sync(addr, ReplOffset(head.0 + 10), Some(primary_token))
                    .await,
                "an ack ahead of head must full-sync, never be treated as caught up"
            );
        });
    }

    /// Helper for `resume_gate_requires_matching_history_token_and_ack_not_ahead_of_head`: dial the
    /// source, send a `REPLCONF` attach advertising `(ack, token)`, and return whether the FIRST
    /// inbound frame is a `FullSync` (i.e. the primary chose a full re-sync over an incremental
    /// resume). A module-level helper (not a nested fn) so the test body stays small + lint-clean.
    async fn attach_first_frame_is_full_sync(
        addr: SocketAddr,
        ack: ReplOffset,
        token: Option<ReplId>,
    ) -> bool {
        let rt = TokioRuntime::new();
        let stream = loop {
            match rt.connect(addr).await {
                Ok(s) => break s,
                Err(_) => rt.timer(Duration::from_millis(5)).await,
            }
        };
        let stream = Rc::new(RefCell::new(Some(SecureStream::plain(stream))));
        send_bytes(
            &rt,
            &stream,
            Frame::ReplConf {
                node: 1,
                ack,
                resume_token: token,
            }
            .encode(),
        )
        .await
        .expect("replconf");
        let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
        let queue = Rc::new(RefCell::new(std::collections::VecDeque::<Frame>::new()));
        matches!(
            next_frame(&rt, &stream, &pending, &queue).await,
            Some(Frame::FullSync { .. })
        )
    }

    /// A comparable `(db, key, encode_kvobj-bytes)` fingerprint of every live key, sorted; two
    /// stores are equal iff their fingerprints match (the same faithful comparison the repl
    /// crate's loopback tests use).
    fn fingerprint(s: &ShardStoreImpl) -> Vec<(u32, Vec<u8>, Vec<u8>)> {
        let mut cursor = SnapshotCursor::START;
        let mut out = Vec::new();
        let mut guard = 0;
        let now = now_from_env();
        while !cursor.is_done(s.databases()) {
            let (chunk, next) = s.snapshot_chunk(cursor, 64, now);
            for (db, key, kv) in chunk {
                out.push((db, key.into_vec(), encode_kvobj(&kv)));
            }
            cursor = next;
            guard += 1;
            assert!(guard < 100_000, "drain terminates");
        }
        out.sort();
        out
    }
}
