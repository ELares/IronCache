// SPDX-License-Identifier: MIT OR Apache-2.0
//! Replication LAG tracking + the node-level replication STATUS cell (HA-7e).
//!
//! HA-7a..7d built the replication LINK, full-sync, steady-state tail, and live attach. They
//! already track the two numbers lag is made of: the primary's `head` (its current logical
//! offset, [`crate::observer::ReplRing::head`]) and each replica's `acked` offset (the replica's
//! applied/resume point, [`crate::observer::ReplRing::acked`] on the primary side,
//! [`crate::stream::ReplicaApplier::applied`] on the replica side). HA-7e turns those into an
//! OBSERVABLE node-level status the serve layer (INFO `# Replication`, `CLUSTER SHARDS`) reads,
//! and a clean promotion SIGNAL HA-8 consumes.
//!
//! ## What lag is
//!
//! For ONE primary<->replica link, lag is `head - acked` measured in logical writes: how many
//! observed writes the primary has produced that the replica has not yet durably applied. It is
//! [`lag`], clamped at 0 (a replica can never be "ahead" of the primary; an over-ack is treated
//! as caught up). When the link is DOWN the lag is UNKNOWN, not 0: the primary cannot know how
//! far a disconnected replica fell, so [`ReplicaLag::lag`] returns `None` for a down link, which
//! callers (and the promotion gate) MUST treat as "not in sync".
//!
//! ## The promotion signal (ADR-0026, what HA-8 consumes)
//!
//! [`replica_is_in_sync`] is the predicate HA-8's promotion gate calls: a replica is
//! promotion-eligible ONLY when its link is UP and its lag is `<= max_lag` (the
//! min-replicas-max-lag bound, ADR-0026). A down link or an unknown/too-large lag is NOT
//! eligible. HA-7e wires the SIGNAL; the actual promotion (picking the most-caught-up eligible
//! replica and assigning it the slots) is HA-8.
//!
//! ## The node-level status cell (no hot-path lock)
//!
//! [`ReplNodeStatus`] is a small `Send + Sync` cell: the node-level offsets / link / role are
//! lock-free ATOMICS, and the two COLD collections (the replica-side `master_endpoint` and the
//! primary-side per-replica [`ReplicaState`] list, #365 N-replica) sit behind a `std::sync::Mutex`
//! taken ONLY on the repl cadence (attach / ack / detach) + the rare INFO/topology read, NEVER on
//! the data hot path or per stored key. It is NODE-LEVEL cold state (one cell per node), so it does
//! not touch `bytes_per_key`. The repl tasks publish after every step (the replica-side control/tail
//! task writes its half; each primary serve task upserts ITS replica's entry, so N concurrent
//! replicas no longer clobber a single cell); the serve layer reads a [`ReplStatusSnapshot`] to
//! render INFO / CLUSTER SHARDS / topology. REPORTING-ONLY: the ADR-0026 in-sync quorum lives in the
//! separate `InSyncReplicas` counter, not this cell.

use core::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};

use crate::cursor::ReplOffset;

/// The replication ROLE a node plays for a slot range it participates in (HA-7e). A node can be
/// a MASTER of some slots and a REPLICA of others at once; this is the role for ONE such range
/// (the single-shard HA-7d wiring tracks the node-level role, the union across slots it serves).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplRole {
    /// This node OWNS the slot range (the primary): it advertises `head` and ships the tail.
    Master,
    /// This node MIRRORS the slot range from a master (the replica): it applies the tail and
    /// serves READONLY reads, tracking its applied offset + the link to its master.
    Replica,
}

impl ReplRole {
    /// The lowercase wire token Redis uses in INFO (`role:master|replica`) and CLUSTER SHARDS
    /// (`role => master|replica`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ReplRole::Master => "master",
            ReplRole::Replica => "replica",
        }
    }

    /// Decode a role from its stored `u8` tag, defaulting to `Master` for the unset/default tag
    /// (a fresh node with no committed replica role is a master of everything it owns, which is
    /// also the byte-compatible standalone default).
    #[must_use]
    fn from_tag(tag: u8) -> Self {
        match tag {
            ROLE_REPLICA => ReplRole::Replica,
            // ROLE_MASTER and the default 0 both decode to Master.
            _ => ReplRole::Master,
        }
    }
}

/// The connectivity of a replica's link to its master (HA-7e), reported as Redis's
/// `master_link_status:up|down`. UP means the link is connected AND syncing (a full-sync
/// completed and the steady-state tail is being applied); DOWN means disconnected / mid-(re)dial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkStatus {
    /// The link is connected and the tail is being applied.
    Up,
    /// The link is down (never connected, dropped, or re-dialing).
    Down,
}

impl LinkStatus {
    /// The lowercase wire token Redis uses (`master_link_status:up|down`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            LinkStatus::Up => "up",
            LinkStatus::Down => "down",
        }
    }

    /// Whether the link is up.
    #[must_use]
    pub fn is_up(self) -> bool {
        matches!(self, LinkStatus::Up)
    }

    /// Decode a link status from its stored `u8` tag, defaulting to `Down` for the unset/default
    /// tag (a fresh replica has not connected yet, so its link is down until it attaches).
    #[must_use]
    fn from_tag(tag: u8) -> Self {
        match tag {
            LINK_UP => LinkStatus::Up,
            _ => LinkStatus::Down,
        }
    }
}

// The compact `u8` tags the role/link atomics store. The DEFAULT (0) decodes to the
// byte-compatible standalone posture: role master, link down (no replica attached).
const ROLE_MASTER: u8 = 0;
const ROLE_REPLICA: u8 = 1;
const LINK_DOWN: u8 = 0;
const LINK_UP: u8 = 1;

/// The replication LAG of one replica relative to its primary: `head - acked` in logical writes,
/// or UNKNOWN when the link is down.
///
/// Construct via [`ReplicaLag::compute`] (link up, from the two offsets) or
/// [`ReplicaLag::unknown`] (link down). [`ReplicaLag::lag`] is `Some(n)` only for an up link, so
/// a caller cannot accidentally treat a disconnected replica as caught up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaLag {
    /// The lag in logical writes when the link is up; `None` when the link is down (unknown).
    lag: Option<u64>,
}

impl ReplicaLag {
    /// The lag for an UP link, computed as [`lag`]`(head, acked)` (clamped at 0).
    #[must_use]
    pub fn compute(head: ReplOffset, acked: ReplOffset) -> Self {
        ReplicaLag {
            lag: Some(lag(head, acked)),
        }
    }

    /// The UNKNOWN lag for a DOWN link: the primary cannot know how far a disconnected replica
    /// fell, so the lag is unknown (never 0).
    #[must_use]
    pub fn unknown() -> Self {
        ReplicaLag { lag: None }
    }

    /// The lag in logical writes, or `None` if the link is down (unknown).
    #[must_use]
    pub fn lag(self) -> Option<u64> {
        self.lag
    }

    /// Whether this replica is in sync within `max_lag`: the link is UP (lag known) AND the lag
    /// is `<= max_lag`. A down link (`None`) is NOT in sync. This is the per-replica core of the
    /// [`replica_is_in_sync`] promotion signal HA-8 consumes.
    #[must_use]
    pub fn in_sync(self, max_lag: u64) -> bool {
        matches!(self.lag, Some(n) if n <= max_lag)
    }
}

/// The lag between a primary's `head` and a replica's `acked` offset, in logical writes, clamped
/// at 0 (HA-7e).
///
/// `head` is the primary's current offset; `acked` is how far the replica has durably applied. A
/// healthy, caught-up replica has `acked == head` (lag 0). `acked > head` (an over-ack, or a
/// stale read across the two atomics) is clamped to 0: a replica can never be "ahead" of the
/// writes the primary produced, so the safe reading is "caught up".
#[must_use]
pub fn lag(head: ReplOffset, acked: ReplOffset) -> u64 {
    head.0.saturating_sub(acked.0)
}

/// The HA-8 PROMOTION SIGNAL (ADR-0026 min-replicas-max-lag): whether a replica is
/// promotion-eligible.
///
/// A replica may be promoted to master of its slots ONLY when its link to the current master is
/// UP and its lag is within `max_lag` logical writes. A down link (the master may be gone, but
/// the replica is too far behind to know it is current) or a lag above the bound makes the
/// replica INELIGIBLE. HA-8's failover gate calls this on each candidate replica and promotes
/// only an eligible one (preferring the least-lagging); HA-7e provides the signal, not the
/// promotion.
#[must_use]
pub fn replica_is_in_sync(link: LinkStatus, lag: ReplicaLag, max_lag: u64) -> bool {
    link.is_up() && lag.in_sync(max_lag)
}

/// The SOURCE-SIDE in-sync-replica COUNT (ADR-0026, the WRITE-SIDE guardrail `min-replicas-to
/// -write`): a single CHEAP ATOMIC the primary's per-replica serve tasks maintain, and the WRITE
/// hot path reads with ONE relaxed load when the guardrail is enabled.
///
/// ## What it counts, and how the repl tasks maintain it (single-delta-per-task, no lock)
///
/// Each attached replica connection has a per-connection serve task that is the SINGLE WRITER of
/// its OWN contribution to this count. The task tracks whether IT is currently counted (a local
/// bool), and on each step recomputes whether its replica is IN SYNC (link up AND lag = `head -
/// shipped <= max_lag`, the same lag gate [`replica_is_in_sync`] uses for promotion). When that
/// verdict CHANGES it nudges the shared counter by exactly one ([`Self::set_replica_in_sync`]):
/// `fetch_add(1)` when it becomes in sync, `fetch_sub(1)` when it falls out (lag grew, or the link
/// dropped). On task exit (the replica disconnected) it clears its contribution
/// ([`Self::replica_gone`]). So the counter is the live number of in-sync replicas, maintained with
/// only `fetch_add`/`fetch_sub` -- NO lock, NO per-key cost, off the data hot path.
///
/// ## Scope: per-node == per-slot in the single-shard-per-node topology (documented)
///
/// This count is NODE-LEVEL (one cell per node), not per-slot: it counts the node's attached
/// in-sync replicas regardless of which slot a write targets. For the realistic single-shard-per
/// -node topology the HA acceptance gate runs (`shards == 1`, one shard owning the node's slots),
/// per-node IS per-slot -- a replica of the node is a replica of every slot the node owns -- so the
/// write-path check `count >= min_replicas_to_write` is exact. The per-SLOT generalization (a write
/// gated on the in-sync replicas of THAT slot's range specifically) mirrors the existing per-shard
/// migration key-presence note: it needs per-slot replica accounting and lands with the multi-shard
/// fan-out; here we count per-node and document the scope rather than build it speculatively.
///
/// ## The default path is byte-unchanged
///
/// This cell exists only in raft-governance mode (the same gate as [`ReplNodeStatus`]); the write
/// path reads it ONLY when `min_replicas_to_write > 0` (a single `> 0` short-circuit guards the
/// load), so with the guardrail at its default-disabled 0 the counter is never read and the hot
/// path is byte-identical.
#[derive(Debug, Default)]
pub struct InSyncReplicas {
    /// The live number of attached replicas currently in sync (link up AND lag <= the configured
    /// `min_replicas_max_lag`). Maintained by per-connection deltas; read with one relaxed load.
    count: AtomicUsize,
}

impl InSyncReplicas {
    /// A fresh count of ZERO (no replicas attached): the standalone/no-replica posture. With the
    /// guardrail enabled and no in-sync replica, every write is rejected `-NOREPLICAS`, which is
    /// the intended safe default (an owner with no good replica must not silently accept writes
    /// it cannot replicate).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The current number of in-sync replicas (the WRITE hot-path read: ONE relaxed load). Relaxed
    /// is sufficient -- the count is an advisory quorum gate, not an ordering anchor; a write that
    /// races a just-attached replica's increment simply uses the value it observed, which is a
    /// momentary, self-correcting skew (the same eventual-consistency the rest of the repl status
    /// cell has).
    #[must_use]
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Transition THIS replica connection's in-sync contribution to `in_sync`, given whether it
    /// `was_counted` before, and return the new `was_counted` state for the caller to carry. The
    /// per-connection task is the SINGLE WRITER of its own contribution, so this is a lock-free
    /// `fetch_add(1)` / `fetch_sub(1)` of exactly one on a CHANGE, and a no-op when unchanged.
    /// `fetch_sub` is guarded by `was_counted` so the counter can never go negative.
    pub fn set_replica_in_sync(&self, was_counted: bool, in_sync: bool) -> bool {
        match (was_counted, in_sync) {
            (false, true) => {
                self.count.fetch_add(1, Ordering::Relaxed);
                true
            }
            (true, false) => {
                self.count.fetch_sub(1, Ordering::Relaxed);
                false
            }
            // No change (still in / still out): the counter already reflects this contribution.
            (same, _) => same,
        }
    }

    /// THIS replica connection went away (the serve task is exiting): drop its contribution if it
    /// was counted, so a disconnected replica no longer counts toward the write quorum. Idempotent
    /// via `was_counted` (a second call with `false` is a no-op), so a task that already fell out
    /// of sync before disconnecting does not double-decrement.
    pub fn replica_gone(&self, was_counted: bool) {
        if was_counted {
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// The NODE-LEVEL replication status cell (HA-7e): a small `Send + Sync` bag of ATOMICS the repl
/// tasks publish to (single-writer per role half, NO lock) and the serve layer reads.
///
/// One cell per node, shared as an `Arc`. It is COLD state: updated on the replication cadence
/// (per attach / per heartbeat / per drained batch), NEVER per stored key, so it is off the data
/// hot path and does not affect `bytes_per_key`. The fields:
///
/// - `role`: this node's replication role (master by default; replica once it attaches as one).
/// - `node_offset`: the node's own replication offset (the master's `head`, or the replica's
///   applied offset). The CLUSTER SHARDS `replication-offset` + INFO `master_repl_offset` /
///   `slave_repl_offset`.
/// - PRIMARY side: the `replicas` list (one [`ReplicaState`] per connected replica, #365
///   N-replica), each with its advertised `NodeId` + last-acked offset. INFO's `slaveN:` lines +
///   the per-replica lag (`head - acked`); `connected_slaves` is the list length.
/// - REPLICA side: `master_link` (up/down), `master_host`/`master_port` (the master endpoint),
///   and `master_offset` (the master's head as last seen on the link, for the replica's own lag).
///
/// ## Single-writer-per-half discipline (no lock, ADR-0002)
///
/// The PRIMARY serve task writes the master-side fields; the REPLICA control/tail task writes the
/// replica-side fields and the role. Each field is an independent atomic, so a reader never blocks
/// a writer and vice versa. A reader assembling a [`ReplStatusSnapshot`] may observe a field set
/// written across two cadence steps (a benign torn read for OBSERVABILITY only); the promotion
/// gate reads the same atomics but is itself driven on the control task, so it sees its own
/// writes coherently.
/// One connected replica's primary-side REPORTING state (#365 N-replica): its advertised `NodeId`
/// (the map key; `0` if it did not advertise) and its last-acked offset (the primary's view). A
/// renderer resolves the id to the replica's endpoint via the slot map and computes the lag as
/// `head - acked`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaState {
    /// The replica's advertised `NodeId` (`0` if it did not advertise an id).
    pub node_id: u64,
    /// The replica's last-acked offset, as the primary last recorded it.
    pub acked: ReplOffset,
}

#[derive(Debug)]
pub struct ReplNodeStatus {
    /// The node's replication role tag ([`ReplRole`]). Default 0 = master (standalone posture).
    role: AtomicU8,
    /// The node's own replication offset (master head or replica applied), as a raw `u64`.
    node_offset: AtomicU64,
    /// PRIMARY: the connected replicas, one [`ReplicaState`] per attached (plain, non-import) replica
    /// keyed by its advertised `NodeId` (#365 N-replica). The transport serves N replicas
    /// concurrently (one task each); each task upserts its own entry on attach + ack and removes it
    /// on disconnect, so concurrent replicas no longer clobber a single cell. Behind the SAME
    /// cold-path `std::sync::Mutex` posture as `master_endpoint` (taken on the repl cadence + the rare
    /// INFO read, never on the data hot path / per stored key). REPORTING-ONLY: the ADR-0026 in-sync
    /// quorum is the separate `InSyncReplicas` counter, NOT this.
    replicas: std::sync::Mutex<Vec<ReplicaState>>,
    /// REPLICA: the link to the master tag ([`LinkStatus`]). Default 0 = down (not attached).
    master_link: AtomicU8,
    /// REPLICA: the master's head offset as last observed on the link (raw `u64`), for the
    /// replica's own lag (`master_offset - node_offset`).
    master_offset: AtomicU64,
    /// REPLICA: the master endpoint host/port, behind a lock taken ONLY on a role/attach change
    /// (never per heartbeat, never on the data hot path). `None` until this node attaches as a
    /// replica. A `std::sync::Mutex` is acceptable here: this is the node-level COLD status cell
    /// (one per node), NOT a hot-path crate's data structure, and the lock is taken only on the
    /// rare attach/detach transition + the rare INFO read.
    master_endpoint: std::sync::Mutex<Option<(String, u16)>>,
}

impl Default for ReplNodeStatus {
    fn default() -> Self {
        ReplNodeStatus {
            role: AtomicU8::new(ROLE_MASTER),
            node_offset: AtomicU64::new(0),
            replicas: std::sync::Mutex::new(Vec::new()),
            master_link: AtomicU8::new(LINK_DOWN),
            master_offset: AtomicU64::new(0),
            master_endpoint: std::sync::Mutex::new(None),
        }
    }
}

impl ReplNodeStatus {
    /// A fresh node status in the standalone/master-of-everything default posture: role master,
    /// offset 0, no connected slaves, link down, no master endpoint. This is byte-compatible with
    /// a node that never attaches a replica (the default static path).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    // --- PRIMARY-side publishers (the per-replica serve task is the single writer) ---

    /// Publish the primary's current `head` offset (the node's own replication offset). Called by
    /// the primary serve loop as it ships the tail; a stale lower value is ignored (monotonic).
    pub fn set_master_head(&self, head: ReplOffset) {
        self.role.store(ROLE_MASTER, Ordering::Relaxed);
        Self::store_monotonic(&self.node_offset, head.0);
    }

    /// Upsert one connected replica's state (PRIMARY side, #365 N-replica): set or advance the entry
    /// for `node_id` with its last-acked offset (monotonically; a stale lower ack is ignored). Called
    /// by THAT replica's serve task at its plain attach + each steady ack. A replica that did not
    /// advertise an id uses `node_id == 0` (it still counts as connected; INFO just cannot resolve its
    /// endpoint). Cold-path lock (repl cadence + the rare INFO read).
    pub fn set_replica(&self, node_id: u64, acked: ReplOffset) {
        if let Ok(mut v) = self.replicas.lock() {
            if let Some(e) = v.iter_mut().find(|e| e.node_id == node_id) {
                if acked.0 > e.acked.0 {
                    e.acked = acked;
                }
            } else {
                v.push(ReplicaState { node_id, acked });
            }
        }
    }

    /// Remove one connected replica (PRIMARY side): its link dropped, so it stops being reported.
    /// Removing an id that is not present is a no-op (e.g. a scoped import that was never recorded).
    pub fn remove_replica(&self, node_id: u64) {
        if let Ok(mut v) = self.replicas.lock() {
            v.retain(|e| e.node_id != node_id);
        }
    }

    // --- REPLICA-side publishers (the control/tail task is the single writer) ---

    /// Publish that THIS node has attached as a REPLICA of the master at `host:port`, with the
    /// snapshot cut `start` as its initial applied offset and the link UP (REPLICA side). Sets the
    /// role to replica. Called at the atomic-store-swap point in the live attach.
    pub fn set_replica_attached(&self, host: &str, port: u16, start: ReplOffset) {
        self.role.store(ROLE_REPLICA, Ordering::Relaxed);
        self.master_link.store(LINK_UP, Ordering::Relaxed);
        Self::store_monotonic(&self.node_offset, start.0);
        Self::store_monotonic(&self.master_offset, start.0);
        if let Ok(mut ep) = self.master_endpoint.lock() {
            *ep = Some((host.to_owned(), port));
        }
    }

    /// Publish the replica's advancing applied offset (REPLICA side), as it applies tail ops.
    /// Monotonic.
    pub fn set_replica_applied(&self, applied: ReplOffset) {
        Self::store_monotonic(&self.node_offset, applied.0);
    }

    /// Publish the master's head offset as observed on the link (REPLICA side), for the replica's
    /// own lag. Monotonic.
    pub fn set_observed_master_head(&self, head: ReplOffset) {
        Self::store_monotonic(&self.master_offset, head.0);
    }

    /// Publish that the replica's link to its master went DOWN (REPLICA side): the link drops to
    /// down so INFO reports `master_link_status:down` and the promotion gate sees it as not in
    /// sync. The role + offsets are retained (the resume point survives a drop).
    pub fn set_master_link_down(&self) {
        self.master_link.store(LINK_DOWN, Ordering::Relaxed);
    }

    // --- Readers ---

    /// A consistent-enough [`ReplStatusSnapshot`] for the serve layer (INFO / CLUSTER SHARDS).
    /// Reads each atomic once; the lock is taken only to copy the master endpoint string (a rare
    /// INFO read, off the data hot path).
    #[must_use]
    pub fn snapshot(&self) -> ReplStatusSnapshot {
        let role = ReplRole::from_tag(self.role.load(Ordering::Relaxed));
        let master_endpoint = self.master_endpoint.lock().ok().and_then(|ep| ep.clone());
        let replicas = self.replicas.lock().map(|v| v.clone()).unwrap_or_default();
        ReplStatusSnapshot {
            role,
            node_offset: ReplOffset(self.node_offset.load(Ordering::Relaxed)),
            replicas,
            master_link: LinkStatus::from_tag(self.master_link.load(Ordering::Relaxed)),
            master_offset: ReplOffset(self.master_offset.load(Ordering::Relaxed)),
            master_endpoint,
        }
    }

    /// The HA-8 promotion SIGNAL for THIS node as a replica: whether it is in sync within
    /// `max_lag` (link up AND lag = `master_offset - node_offset` within the bound). Reads the
    /// replica-side atomics directly (the control task that calls this is their writer, so it sees
    /// its own writes coherently). Returns `false` for a node that is not a replica (a master is
    /// never a promotion CANDIDATE of its own slots).
    #[must_use]
    pub fn is_in_sync(&self, max_lag: u64) -> bool {
        if ReplRole::from_tag(self.role.load(Ordering::Relaxed)) != ReplRole::Replica {
            return false;
        }
        let link = LinkStatus::from_tag(self.master_link.load(Ordering::Relaxed));
        let head = ReplOffset(self.master_offset.load(Ordering::Relaxed));
        let acked = ReplOffset(self.node_offset.load(Ordering::Relaxed));
        let l = if link.is_up() {
            ReplicaLag::compute(head, acked)
        } else {
            ReplicaLag::unknown()
        };
        replica_is_in_sync(link, l, max_lag)
    }

    /// CAS-monotonic store: only advance an offset atomic, never lower it (so a stale/reordered
    /// publish never moves an offset backwards). Relaxed is sufficient: each offset is an
    /// independent observability counter, not an ordering anchor for other state.
    fn store_monotonic(cell: &AtomicU64, value: u64) {
        let mut cur = cell.load(Ordering::Relaxed);
        while value > cur {
            match cell.compare_exchange_weak(cur, value, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
}

/// A point-in-time read of a node's [`ReplNodeStatus`] for the serve layer (HA-7e). Plain data,
/// cheap to build; INFO `# Replication` and CLUSTER SHARDS render from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplStatusSnapshot {
    /// This node's replication role.
    pub role: ReplRole,
    /// The node's own replication offset (master head or replica applied).
    pub node_offset: ReplOffset,
    /// PRIMARY: the connected replicas, one per attached replica (#365 N-replica). Empty when none,
    /// and when this node is itself a replica. Render one `slaveN:` line per entry.
    pub replicas: Vec<ReplicaState>,
    /// REPLICA: the link to the master.
    pub master_link: LinkStatus,
    /// REPLICA: the master's head offset as last observed on the link (for the replica's lag).
    pub master_offset: ReplOffset,
    /// REPLICA: the master endpoint `(host, port)`, if attached as a replica.
    pub master_endpoint: Option<(String, u16)>,
}

impl ReplStatusSnapshot {
    /// PRIMARY: the number of connected replicas (the `connected_slaves` INFO field).
    #[must_use]
    pub fn connected_slaves(&self) -> u64 {
        self.replicas.len() as u64
    }

    /// PRIMARY: the master's view of one connected replica's lag (`node_offset - acked`), treating
    /// it as up while connected (the primary's serve loop only records a connected replica). The
    /// per-replica analog of the old single-replica `slave_lag`.
    #[must_use]
    pub fn slave_lag_of(&self, acked: ReplOffset) -> ReplicaLag {
        ReplicaLag::compute(self.node_offset, acked)
    }

    /// REPLICA: this node's own lag relative to its master (`master_offset - node_offset`) when
    /// the link is up, else unknown. The promotion signal for THIS node as a candidate.
    #[must_use]
    pub fn replica_lag(&self) -> ReplicaLag {
        if self.master_link.is_up() {
            ReplicaLag::compute(self.master_offset, self.node_offset)
        } else {
            ReplicaLag::unknown()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_is_head_minus_acked_clamped_at_zero() {
        // Caught up: lag 0.
        assert_eq!(lag(ReplOffset(10), ReplOffset(10)), 0);
        // Behind by 3.
        assert_eq!(lag(ReplOffset(10), ReplOffset(7)), 3);
        // An over-ack (acked > head) clamps to 0 (a replica is never ahead of the primary).
        assert_eq!(lag(ReplOffset(5), ReplOffset(9)), 0);
        // From the stream origin.
        assert_eq!(lag(ReplOffset(0), ReplOffset(0)), 0);
        assert_eq!(lag(ReplOffset(4), ReplOffset(0)), 4);
    }

    #[test]
    fn replica_lag_known_when_up_unknown_when_down() {
        let up = ReplicaLag::compute(ReplOffset(10), ReplOffset(8));
        assert_eq!(up.lag(), Some(2));
        let down = ReplicaLag::unknown();
        assert_eq!(down.lag(), None);
        // An over-ack still clamps to 0 (not a negative / wrapped value).
        assert_eq!(
            ReplicaLag::compute(ReplOffset(3), ReplOffset(7)).lag(),
            Some(0)
        );
    }

    #[test]
    fn in_sync_true_only_when_up_and_within_lag() {
        // Up + within lag -> eligible.
        let up_ok = ReplicaLag::compute(ReplOffset(10), ReplOffset(9)); // lag 1
        assert!(replica_is_in_sync(LinkStatus::Up, up_ok, 2));
        // Up + at the bound -> eligible (<=).
        let up_edge = ReplicaLag::compute(ReplOffset(10), ReplOffset(8)); // lag 2
        assert!(replica_is_in_sync(LinkStatus::Up, up_edge, 2));
        // Up + over the bound -> NOT eligible.
        let up_lagging = ReplicaLag::compute(ReplOffset(10), ReplOffset(5)); // lag 5
        assert!(!replica_is_in_sync(LinkStatus::Up, up_lagging, 2));
        // Down -> NOT eligible regardless of the (unknown) lag.
        assert!(!replica_is_in_sync(
            LinkStatus::Down,
            ReplicaLag::unknown(),
            u64::MAX
        ));
        // A DOWN link with a stale "known" lag is still rejected by the link gate.
        assert!(!replica_is_in_sync(LinkStatus::Down, up_ok, 100));
    }

    #[test]
    fn node_status_defaults_to_standalone_master_posture() {
        let s = ReplNodeStatus::new().snapshot();
        assert_eq!(s.role, ReplRole::Master);
        assert_eq!(s.node_offset, ReplOffset::ZERO);
        assert_eq!(s.connected_slaves(), 0);
        assert!(s.replicas.is_empty());
        assert_eq!(s.master_link, LinkStatus::Down);
        assert_eq!(s.master_endpoint, None);
    }

    #[test]
    fn primary_tracks_n_replicas_by_id_with_lag() {
        let status = ReplNodeStatus::new();
        status.set_master_head(ReplOffset(100));
        // #365 N-replica: two distinct replicas attach, keyed by their advertised NodeId.
        status.set_replica(0xABCD, ReplOffset(95)); // lag 5
        status.set_replica(0x1234, ReplOffset(90)); // lag 10
        let s = status.snapshot();
        assert_eq!(s.role, ReplRole::Master);
        assert_eq!(s.node_offset, ReplOffset(100));
        assert_eq!(
            s.connected_slaves(),
            2,
            "both replicas are tracked, not clobbered"
        );
        // Each replica keeps its OWN id + offset; the master's per-replica lag is head - acked.
        let a = s.replicas.iter().find(|r| r.node_id == 0xABCD).unwrap();
        assert_eq!(a.acked, ReplOffset(95));
        assert_eq!(s.slave_lag_of(a.acked).lag(), Some(5));
        let b = s.replicas.iter().find(|r| r.node_id == 0x1234).unwrap();
        assert_eq!(s.slave_lag_of(b.acked).lag(), Some(10));

        // A re-ack ADVANCES that replica's offset monotonically (a stale lower ack is ignored).
        status.set_replica(0xABCD, ReplOffset(99));
        status.set_replica(0xABCD, ReplOffset(40)); // stale, ignored
        let s = status.snapshot();
        assert_eq!(
            s.connected_slaves(),
            2,
            "a re-ack updates, does not duplicate"
        );
        assert_eq!(
            s.replicas
                .iter()
                .find(|r| r.node_id == 0xABCD)
                .unwrap()
                .acked,
            ReplOffset(99)
        );

        // One replica disconnects: only its entry is removed.
        status.remove_replica(0x1234);
        let s = status.snapshot();
        assert_eq!(s.connected_slaves(), 1);
        assert!(s.replicas.iter().all(|r| r.node_id != 0x1234));
        assert!(s.replicas.iter().any(|r| r.node_id == 0xABCD));
    }

    #[test]
    fn offsets_are_monotonic_and_ignore_stale_lower_publishes() {
        let status = ReplNodeStatus::new();
        status.set_master_head(ReplOffset(50));
        status.set_master_head(ReplOffset(40)); // stale, ignored
        assert_eq!(status.snapshot().node_offset, ReplOffset(50));
        status.set_master_head(ReplOffset(60)); // advances
        assert_eq!(status.snapshot().node_offset, ReplOffset(60));
    }

    #[test]
    fn replica_publishes_attach_link_and_in_sync_signal() {
        let status = ReplNodeStatus::new();
        // Attach as a replica of a master at 10.0.0.1:6379, cut at offset 30.
        status.set_replica_attached("10.0.0.1", 6379, ReplOffset(30));
        // Observe the master ahead at 33, and apply up to 31.
        status.set_observed_master_head(ReplOffset(33));
        status.set_replica_applied(ReplOffset(31));
        let s = status.snapshot();
        assert_eq!(s.role, ReplRole::Replica);
        assert_eq!(s.master_link, LinkStatus::Up);
        assert_eq!(s.node_offset, ReplOffset(31));
        assert_eq!(s.master_offset, ReplOffset(33));
        assert_eq!(s.master_endpoint, Some(("10.0.0.1".to_owned(), 6379)));
        // The replica's own lag is 33 - 31 = 2; in sync within 2, not within 1.
        assert_eq!(s.replica_lag().lag(), Some(2));
        assert!(status.is_in_sync(2));
        assert!(!status.is_in_sync(1));

        // The link drops: in sync becomes false (unknown lag), the role/offsets survive.
        status.set_master_link_down();
        let s = status.snapshot();
        assert_eq!(s.master_link, LinkStatus::Down);
        assert_eq!(s.replica_lag().lag(), None);
        assert!(!status.is_in_sync(u64::MAX));
        // The resume point (applied offset) is retained across the drop.
        assert_eq!(s.node_offset, ReplOffset(31));
    }

    #[test]
    fn a_master_is_never_its_own_promotion_candidate() {
        let status = ReplNodeStatus::new();
        status.set_master_head(ReplOffset(10));
        // A master node is not a replica, so it is never `is_in_sync` (not a promotion candidate
        // of its own slots).
        assert!(!status.is_in_sync(u64::MAX));
    }

    #[test]
    fn in_sync_replica_count_reflects_attach_lag_and_disconnect() {
        // The WRITE-SIDE guardrail's source-side count (ADR-0026), maintained by per-connection
        // deltas. Drive it the way the per-replica serve tasks do: each task carries a local
        // `was_counted` and nudges the shared counter by exactly one on a verdict change.
        let in_sync = InSyncReplicas::new();
        // 0 replicas -> 0.
        assert_eq!(in_sync.count(), 0);

        // Replica A attaches IN SYNC (lag within the bound) -> counted, count 1.
        let mut a_counted = false;
        a_counted = in_sync.set_replica_in_sync(a_counted, true);
        assert!(a_counted);
        assert_eq!(in_sync.count(), 1);

        // Idempotent: A stays in sync -> no double-count.
        a_counted = in_sync.set_replica_in_sync(a_counted, true);
        assert_eq!(in_sync.count(), 1);

        // Replica B attaches but is LAGGING past max_lag -> NOT counted, count stays 1.
        let mut b_counted = false;
        b_counted = in_sync.set_replica_in_sync(b_counted, false);
        assert!(!b_counted);
        assert_eq!(in_sync.count(), 1);

        // B catches up (now in sync) -> counted, count 2.
        b_counted = in_sync.set_replica_in_sync(b_counted, true);
        assert_eq!(in_sync.count(), 2);

        // A falls out of sync (lag grew / link blipped) -> decremented, count 1.
        a_counted = in_sync.set_replica_in_sync(a_counted, false);
        assert!(!a_counted);
        assert_eq!(in_sync.count(), 1);

        // B disconnects while counted -> its contribution is dropped, count 0.
        in_sync.replica_gone(b_counted);
        assert_eq!(in_sync.count(), 0);

        // A disconnects while NOT counted -> no underflow (no double-decrement).
        in_sync.replica_gone(a_counted);
        assert_eq!(in_sync.count(), 0);
    }
}
