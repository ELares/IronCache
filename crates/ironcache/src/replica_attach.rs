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

use ironcache_env::Clock;
use ironcache_repl::{
    ApplyOutcome, Frame, FrameError, ReplId, ReplNodeStatus, ReplObserver, ReplOffset, ReplRing,
    ReplicaApplier, ShipOutcome, drain_and_ship, encode_kvobj, receive_full_sync,
};
use ironcache_runtime::tokio_rt::bind_exclusive;
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_storage::UnixMillis;
use ironcache_store::SnapshotCursor;

use crate::serve::ShardStoreImpl;

/// The bounded depth of a primary shard's replication tail ring (HA-7c [`ReplRing`] cap). A
/// generous window so a momentarily-behind replica resumes from the tail rather than forcing a
/// full re-sync; if it overflows (the replica fell too far behind) [`drain_and_ship`] reports
/// [`ShipOutcome::ResyncNeeded`] and the replica re-full-syncs (the MVP full-resync-on-gap
/// policy, correct though not optimal; a disk-backed backlog is HA-7e, deferred).
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
) {
    if PRIMARY_STARTED.with(std::cell::Cell::get) {
        return; // already wired on this shard (idempotent).
    }
    PRIMARY_STARTED.with(|c| c.set(true));

    let Some(cluster) = ctx.cluster.clone() else {
        // Raft-mode always installs a shared map; defensively do nothing without one.
        return;
    };

    // The per-shard replication tail ring + the observer feeding it. Installing the observer
    // flips the store's `repl_active` gate (the HA-5a seam's intended raft-mode use): from now
    // on every local write on this shard is enqueued onto `ring` as a tail op. One Rc clone of
    // the ring stays here for the listener's `drain_and_ship`; the other lives in the boxed
    // observer the store owns.
    let ring = ReplRing::new(TAIL_RING_CAP, ReplOffset::ZERO);
    store_rc
        .borrow_mut()
        .set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));

    // The replication id this primary advertises: derived from this node's stable 40-hex
    // cluster node id (`from_hex` reads exactly 40 hex chars into the 20-byte ReplId), so it is
    // stable across the process lifetime and distinct per node. A replica that sees a CHANGED
    // replid would full-resync (HA-7b); here it is fixed for the node, which is the intended
    // single-stream-per-primary identity.
    let replid = ReplId::from_hex(ctx.info.cluster_node_id.as_bytes())
        .unwrap_or_else(|| ReplId::from_bytes([0u8; 20]));

    // The NODE-LEVEL replication status cell (HA-7e): the repl tasks publish role / offsets / link
    // state here for INFO / CLUSTER SHARDS + the HA-8 promotion gate. Raft-mode always installs
    // one (serve::run_server), so `Some` in practice; a defensive fresh cell keeps the wiring
    // total without an Option threading through every task. It is `Send + Sync` (atomics), so it
    // clones cheaply into both shard-local tasks.
    let status: std::sync::Arc<ReplNodeStatus> = ctx
        .repl_status
        .clone()
        .unwrap_or_else(|| std::sync::Arc::new(ReplNodeStatus::new()));

    let rt = TokioRuntime::new();

    // --- PRIMARY: bind the repl listener + serve replica connections. ---
    let listen_addr = SocketAddr::new(bind, repl_port(client_port));
    let listener_ring = Rc::clone(&ring);
    let listener_store = Rc::clone(&store_rc);
    let listener_status = std::sync::Arc::clone(&status);
    rt.spawn_on_shard(async move {
        run_primary_listener(
            TokioRuntime::new(),
            listen_addr,
            replid,
            listener_store,
            listener_ring,
            listener_status,
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
    rt.spawn_on_shard(async move {
        run_replica_control(
            TokioRuntime::new(),
            cluster,
            store_rc,
            databases,
            policy_name,
            reserved_bits,
            status,
            raft,
            self_node_id,
            failover,
        )
        .await;
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
async fn run_primary_listener(
    rt: TokioRuntime,
    listen_addr: SocketAddr,
    replid: ReplId,
    store_rc: Rc<RefCell<ShardStoreImpl>>,
    ring: Rc<RefCell<ReplRing>>,
    status: std::sync::Arc<ReplNodeStatus>,
) {
    // EXCLUSIVE bind (NO SO_REUSEPORT): the repl listener is a single per-node socket that must
    // never SHARE a port with another service. A reuseport bind would let the kernel load-balance
    // traffic between this listener and any other reuseport socket on the same port (e.g. a
    // neighbor node's Raft bus or client listener that aliased this port), silently stealing
    // consensus / client traffic. An exclusive bind turns such a collision into a clean error.
    let listener = match bind_exclusive(listen_addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("replica-attach: failed to bind repl listener {listen_addr}: {e}");
            return;
        }
    };
    // Publish this node's current head as a master from the moment the listener is up (HA-7e), so
    // INFO / CLUSTER SHARDS report the real master offset even before any replica attaches.
    status.set_master_head(ring.borrow().head());
    loop {
        let Ok((stream, _peer)) = rt.accept(&listener).await else {
            return; // listener failed; the primary is going away.
        };
        let rt2 = TokioRuntime::new();
        let store = Rc::clone(&store_rc);
        let ring = Rc::clone(&ring);
        let status = std::sync::Arc::clone(&status);
        rt.spawn_on_shard(async move {
            serve_replica_conn(rt2, stream, replid, store, ring, status).await;
        });
    }
}

/// Serve ONE accepted replica connection: read the attach handshake, full-sync the current
/// snapshot (capturing the cut `end_offset` per CARRY-FORWARD 1), then loop [`drain_and_ship`]
/// to stream the tail until the link drops.
///
/// The stream lives behind `Rc<RefCell<Option<_>>>` so each send/recv TAKES it out, awaits on
/// the owned value, and puts it back -- no `RefCell` borrow crosses an `.await`.
async fn serve_replica_conn(
    rt: TokioRuntime,
    stream: <TokioRuntime as Runtime>::Stream,
    replid: ReplId,
    store_rc: Rc<RefCell<ShardStoreImpl>>,
    ring: Rc<RefCell<ReplRing>>,
    status: std::sync::Arc<ReplNodeStatus>,
) {
    let stream = Rc::new(RefCell::new(Some(stream)));

    // (1) Read the attach REPLCONF handshake (the replica names itself + its resume offset).
    // A replica that has never synced sends ack 0; we always full-sync from scratch in this
    // MVP (a partial resume from the tail is HA-7e), so the ack is read-and-acknowledged but
    // the sync starts at the snapshot cut regardless. The handshake `ack` is the master's view
    // of the replica's resume offset (HA-7e); a steady-state per-ack stream is an HA-7e follow-up.
    let mut pending: Vec<u8> = Vec::new();
    let Some(handshake_ack) = read_attach_handshake(&rt, &stream, &mut pending).await else {
        return; // the replica closed / sent garbage before attaching.
    };
    // Publish that a replica is connected (PRIMARY side, HA-7e): connected_slaves -> 1 with the
    // replica's resume offset. INFO renders the `slaveN:` line + the master-side lag from this.
    status.set_replica_connected(handshake_ack);

    // (2) Drive the full sync. CARRY-FORWARD 1 (ATOMIC end_offset capture) is satisfied inside
    // `drive_full_sync_chunked` (see its doc + the cited code point): the cut is the ring head
    // captured with NO `.await` between the head read and the first snapshot chunk pull.
    if drive_full_sync_chunked(&rt, &stream, replid, &store_rc, &ring)
        .await
        .is_err()
    {
        status.set_replica_disconnected(); // the link dropped mid-sync; the replica re-syncs.
        return;
    }

    // (3) The steady-state tail: ship newly-observed ops in offset order. `drain_and_ship`
    // takes the ring borrow only for the bounded batch copy, releases it, then awaits the
    // sends (so the write funnel is never blocked behind a network await). On an overflow the
    // replica fell too far behind: drop the connection so it reconnects + re-full-syncs (MVP
    // full-resync-on-gap). When the batch is empty, wait the poll interval (timer seam) so an
    // idle tail does not busy-spin. After EACH pass, publish the current head (PRIMARY side,
    // HA-7e) so INFO / CLUSTER SHARDS report the advancing master offset.
    loop {
        let send_stream = Rc::clone(&stream);
        let rt_send = rt;
        let outcome = drain_and_ship(&ring, TAIL_SHIP_BATCH, move |frame| {
            let stream = Rc::clone(&send_stream);
            let bytes = frame.encode();
            async move { send_bytes(&rt_send, &stream, bytes).await }
        })
        .await;
        // Publish the advancing master head (and re-affirm the slave's last-known offset) so the
        // observable master offset tracks the writes shipped. Cold node-level publish, not per key.
        status.set_master_head(ring.borrow().head());
        match outcome {
            ShipOutcome::Shipped(0) => rt.timer(POLL_INTERVAL).await,
            ShipOutcome::Shipped(_) => {}
            // The ring overflowed (the replica is too far behind) or the link dropped: end the
            // connection. The replica reconnects and re-attaches, which full-syncs from a fresh
            // cut; the ring's resync latch is cleared by the next attach's rebase-at-head.
            ShipOutcome::ResyncNeeded | ShipOutcome::LinkDown => {
                status.set_replica_disconnected();
                return;
            }
        }
    }
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
/// drops the connection and the replica reconnects + re-syncs.
async fn drive_full_sync_chunked(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<<TokioRuntime as Runtime>::Stream>>>,
    replid: ReplId,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    ring: &Rc<RefCell<ReplRing>>,
) -> Result<(), ()> {
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
    send_bytes(rt, stream, Frame::SyncEnd { end_offset }.encode()).await
}

/// Read inbound bytes until the replica's attach `REPLCONF` arrives, returning `Some(ack)` with
/// the replica's resume offset once it does (the handshake), or `None` if the socket closes /
/// sends a malformed frame first. Any non-REPLCONF frame before the handshake is ignored (a stray
/// heartbeat). `pending` carries the partial read buffer across reads. The `ack` is the master's
/// view of the replica's resume offset, published into the node status for INFO (HA-7e).
async fn read_attach_handshake(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<<TokioRuntime as Runtime>::Stream>>>,
    pending: &mut Vec<u8>,
) -> Option<ReplOffset> {
    loop {
        // Drain any complete frames already buffered.
        loop {
            match Frame::decode(pending) {
                Ok(Some((frame, consumed))) => {
                    pending.drain(..consumed);
                    if let Frame::ReplConf { ack, .. } = frame {
                        return Some(ack); // attached; carry the replica's resume offset.
                    }
                    // A non-REPLCONF frame before the handshake: ignore, keep reading.
                }
                Ok(None) => break,              // need more bytes.
                Err(FrameError) => return None, // malformed: drop the connection.
            }
        }
        // Read another chunk (TAKE/put-back so no borrow crosses the await).
        let taken: Vec<u8> = core::mem::take(pending);
        let mut s = stream.borrow_mut().take().expect("stream present");
        let res = rt.recv(&mut s, taken).await;
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
    loop {
        // Should THIS shard be a replica right now? (single-shard: replica of ANY slot.)
        if let Some(slot) = any_replica_of_self(&cluster) {
            // Resolve the slot OWNER's advertised CLIENT endpoint; the repl listener is at
            // `repl_port` of that client port. `moved_target` returns None for an unassigned
            // slot (mid-formation); back off and re-check.
            if let Some((host, owner_client_port)) = cluster.moved_target(slot) {
                if let Ok(owner_addr) =
                    format!("{host}:{}", repl_port(owner_client_port)).parse::<SocketAddr>()
                {
                    // Attach: full-sync + atomic swap + passive + tail. Returns when the link
                    // drops or a Gap forces a re-attach; we loop and re-attach (re-checking the
                    // map first, so a role removal stops us). Publishes the replica-side status
                    // (role / link / offsets) into `status` for INFO / CLUSTER SHARDS + the HA-8
                    // gate; the master ENDPOINT reported is the owner's advertised CLIENT
                    // host:port (what an operator dials), not the internal repl port.
                    attach_once(
                        &rt,
                        owner_addr,
                        &host,
                        owner_client_port,
                        &store_rc,
                        databases,
                        policy_name,
                        reserved_bits,
                        &status,
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

/// Attach ONCE to the owner at `owner_addr`: dial, [`receive_full_sync`] into a FRESH
/// [`ShardStoreImpl`], ATOMICALLY swap it into the live `store_rc`, mark the shard passive
/// (CARRY-FORWARD 2), then run the tail until the link drops / a Gap forces a re-sync. Returns
/// on any terminal condition (dial fail, sync fail, link drop, gap) so the caller re-attaches.
///
/// On a FAILED / interrupted sync the partial temp store is DISCARDED (it is dropped on the
/// error return inside `receive_full_sync`, never swapped) and we return WITHOUT having
/// perturbed the live store: a half-loaded store is NEVER swapped in.
///
/// The over-7-args lint is allowed: each argument is a distinct, orthogonal seam (the runtime,
/// the owner's repl dial address vs its advertised CLIENT endpoint for the status report, the
/// live store handle, the store-construction facts, and the node status cell to publish into);
/// bundling them would just move the same fields behind one name.
#[allow(clippy::too_many_arguments)]
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
) {
    // Dial the owner's repl endpoint. A failed dial returns; the caller backs off + retries.
    let Ok(stream) = rt.connect(owner_addr).await else {
        return;
    };
    let stream = Rc::new(RefCell::new(Some(stream)));

    // Send the attach REPLCONF handshake (this node names itself; ack 0 -> full sync from
    // scratch in this MVP). `node`/`ack` are advisory to the primary's link bookkeeping; the
    // sync is driven by the primary regardless.
    let handshake = Frame::ReplConf {
        node: 0,
        ack: ReplOffset::ZERO,
    }
    .encode();
    if !send_bytes_ok(rt, &stream, handshake).await {
        return;
    }

    // Receive the full sync into a FRESH store built EXACTLY like `shard_store` builds the live
    // one (same Policy from the configured name, same accounting, same scan-band bits), so the
    // swapped-in store is the SAME concrete `ShardStoreImpl` type and behaves identically.
    let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
    let queue = Rc::new(RefCell::new(std::collections::VecDeque::<Frame>::new()));
    let make_store = move || crate::serve::fresh_shard_store(databases, policy_name, reserved_bits);
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
        // receive_full_sync. The live store is UNTOUCHED. Return so the caller re-attaches.
        return;
    };

    // ATOMIC STORE SWAP (HA-7d): replace the RefCell contents with the fully-loaded fresh
    // store in ONE statement. The `Rc` handle stays shared with the serve loop, so reads
    // immediately start hitting the synced data; the OLD store is dropped at end of statement.
    // This is the only place the live store is replaced, and it happens ONLY after a COMPLETE
    // sync (a half-loaded store is never reachable here). Marking the shard passive BEFORE the
    // swap is published is unnecessary on a single-threaded shard (no other task runs during
    // this synchronous block), but we set passive immediately after so the reaper -- if it
    // fires on the very next tick -- sees the replica state.
    *store_rc.borrow_mut() = loaded.store; // <-- the atomic store swap.
    // CARRY-FORWARD 2: the replica store is now passive. Two complementary guards: the
    // serve-loop flag stops the background reaper tick, and the STORE-level flag makes the
    // lazy-on-read expiry path report a due key as absent WITHOUT physically removing it
    // (so a READONLY read can never self-remove a key the primary still holds; removal comes
    // only from the primary's StreamDel). Both together make the replica removal-passive
    // end to end.
    store_rc.borrow_mut().set_passive(true);
    crate::serve::set_replica_passive(true);

    // Publish the REPLICA-side status (HA-7e): this node is now a replica of `owner_host:port`,
    // link UP, with the snapshot cut as its initial applied + observed-master offset. INFO renders
    // `role:replica`/`master_host`/`master_link_status:up`/the offsets, and the HA-8 gate reads
    // `is_in_sync` off this. The endpoint is the owner's advertised CLIENT host:port (what
    // operators see), not the internal repl port the replica dials.
    status.set_replica_attached(owner_host, owner_client_port, loaded.end_offset);

    // Run the steady-state tail from the snapshot cut. On a Gap (a missing offset / corrupt
    // frame) the replica fell behind the primary's bounded buffer; tear down + re-attach (the
    // caller's loop re-dials, which full-syncs from a fresh cut).
    run_replica_tail(rt, &stream, store_rc, loaded.end_offset, status).await;
}

/// The replica STEADY-STATE TAIL (HA-7c apply): recv stream frames and apply them in offset
/// order via [`ReplicaApplier`], returning when the link drops or a [`ApplyOutcome::Gap`]
/// forces a full re-sync. No `RefCell` borrow crosses an `.await`: each frame is recv'd (store
/// borrow not held), THEN the store is borrowed for the synchronous `apply`, THEN the borrow
/// drops before the next recv.
async fn run_replica_tail(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<<TokioRuntime as Runtime>::Stream>>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    start: ReplOffset,
    status: &std::sync::Arc<ReplNodeStatus>,
) {
    let mut applier = ReplicaApplier::new(start);
    let pending = Rc::new(RefCell::new(Vec::<u8>::new()));
    let queue = Rc::new(RefCell::new(std::collections::VecDeque::<Frame>::new()));
    loop {
        let Some(frame) = next_frame(rt, stream, &pending, &queue).await else {
            return; // link dropped / peer closed: the caller re-attaches.
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
            // re-sync. Return so the caller re-dials + full-syncs from a fresh cut.
            ApplyOutcome::Gap => return,
        }
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

// ===========================================================================================
// SHARED I/O HELPERS (the take/put-back stream idiom; no borrow across await)
// ===========================================================================================

/// Send `bytes` on the take/put-back stream, returning `Result<(), ()>` (the sink shape the
/// repl primitives' `send` closure wants). The stream is taken out, the I/O awaited on the
/// owned value, and put back, so no `RefCell` borrow crosses the await.
async fn send_bytes(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<<TokioRuntime as Runtime>::Stream>>>,
    bytes: Vec<u8>,
) -> Result<(), ()> {
    let mut s = stream.borrow_mut().take().expect("stream present");
    let res = rt.send(&mut s, bytes).await;
    *stream.borrow_mut() = Some(s);
    res.map(|_| ()).map_err(|_| ())
}

/// [`send_bytes`] returning a plain `bool` (`true` on success), for the handshake send where a
/// `Result<(), ()>` is not the consumed shape.
async fn send_bytes_ok(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<<TokioRuntime as Runtime>::Stream>>>,
    bytes: Vec<u8>,
) -> bool {
    send_bytes(rt, stream, bytes).await.is_ok()
}

/// Pull the next complete [`Frame`] from the take/put-back stream, buffering partial reads in
/// `pending` and decoded-but-not-yet-returned frames in `queue` (so a single read that yields
/// several frames returns them one at a time). Returns `None` on a clean close / I/O error /
/// malformed frame. No `RefCell` borrow crosses the recv await (the stream is taken out for it).
async fn next_frame(
    rt: &TokioRuntime,
    stream: &Rc<RefCell<Option<<TokioRuntime as Runtime>::Stream>>>,
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
        let res = rt.recv(&mut s, taken).await;
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
                let stream = Rc::new(RefCell::new(Some(stream)));
                let mut pending = Vec::new();
                let attached = read_attach_handshake(&rt, &stream, &mut pending).await;
                if attached.is_none() {
                    return;
                }
                let replid = ReplId::from_bytes([0xCD; 20]);
                let _ =
                    drive_full_sync_chunked(&rt, &stream, replid, &prim_store, &prim_ring).await;
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
                let stream = Rc::new(RefCell::new(Some(stream)));
                let handshake = Frame::ReplConf {
                    node: 7,
                    ack: ReplOffset::ZERO,
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
