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
//! ## The node-level status cell (single-writer, no hot-path lock)
//!
//! [`ReplNodeStatus`] is a small `Send + Sync` cell of ATOMICS (no `Mutex`, no hot-path lock):
//! the repl tasks (the primary per-replica serve task and the replica control/tail task, each a
//! SINGLE WRITER for its half) publish the current offsets + link state after every step, and
//! the serve layer reads a [`ReplStatusSnapshot`] of it to render INFO / CLUSTER SHARDS. It is
//! NODE-LEVEL cold state (one cell per node, updated on the repl cadence, never per stored key),
//! so it does not touch the data hot path or `bytes_per_key`. The atomics let a reader on any
//! shard observe a consistent-enough view without taking the writer's executor offline; an
//! occasional torn read across the two role halves is harmless for observability (each field is
//! itself atomic, and the role + offsets are written by the one owning task).

use core::sync::atomic::{AtomicU8, AtomicU64, Ordering};

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

// HA-7e follow-up: the WRITE-SIDE guardrail (min-replicas-to-write / min-replicas-max-lag write
// GATING -- rejecting a write on a master that lacks enough in-sync replicas, ADR-0026) is a
// configurable, default-off guardrail and is intentionally NOT built here; HA-7e ships only the
// lag SIGNAL above (what HA-8's promotion gate consumes). The bounded ring + full-resync-on
// -overflow guardrail (HA-7c, [`crate::observer::ReplRing`]) already exists and needs no new work.

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
/// - PRIMARY side: `connected_slaves`, and for the (single, HA-7d) attached replica its
///   `slave_offset` + whether it is connected (`slave_connected`). INFO's `slaveN:` line + the
///   per-replica lag (`head - slave_offset`).
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
#[derive(Debug)]
pub struct ReplNodeStatus {
    /// The node's replication role tag ([`ReplRole`]). Default 0 = master (standalone posture).
    role: AtomicU8,
    /// The node's own replication offset (master head or replica applied), as a raw `u64`.
    node_offset: AtomicU64,
    /// PRIMARY: the number of connected replicas (0 or 1 in the single-shard HA-7d wiring).
    connected_slaves: AtomicU64,
    /// PRIMARY: the attached replica's last-acked offset (raw `u64`); meaningful when
    /// `connected_slaves > 0`.
    slave_offset: AtomicU64,
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
            connected_slaves: AtomicU64::new(0),
            slave_offset: AtomicU64::new(0),
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

    /// Publish that a replica is connected with its last-acked offset `acked` (PRIMARY side). The
    /// single-shard HA-7d wiring tracks one replica; `connected_slaves` is set to 1 and the
    /// replica's offset advanced monotonically.
    pub fn set_replica_connected(&self, acked: ReplOffset) {
        self.connected_slaves.store(1, Ordering::Relaxed);
        Self::store_monotonic(&self.slave_offset, acked.0);
    }

    /// Publish that the (single) connected replica went away (PRIMARY side): `connected_slaves`
    /// drops to 0. The replica's last offset is left as-is (it is only read when connected).
    pub fn set_replica_disconnected(&self) {
        self.connected_slaves.store(0, Ordering::Relaxed);
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
        ReplStatusSnapshot {
            role,
            node_offset: ReplOffset(self.node_offset.load(Ordering::Relaxed)),
            connected_slaves: self.connected_slaves.load(Ordering::Relaxed),
            slave_offset: ReplOffset(self.slave_offset.load(Ordering::Relaxed)),
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
    /// PRIMARY: the number of connected replicas.
    pub connected_slaves: u64,
    /// PRIMARY: the connected replica's last-acked offset (meaningful when `connected_slaves > 0`).
    pub slave_offset: ReplOffset,
    /// REPLICA: the link to the master.
    pub master_link: LinkStatus,
    /// REPLICA: the master's head offset as last observed on the link (for the replica's lag).
    pub master_offset: ReplOffset,
    /// REPLICA: the master endpoint `(host, port)`, if attached as a replica.
    pub master_endpoint: Option<(String, u16)>,
}

impl ReplStatusSnapshot {
    /// PRIMARY: the connected replica's lag (`node_offset - slave_offset`) when one is connected,
    /// else `None`. The replica is treated as up while connected (the primary's serve loop only
    /// advertises a connected replica), so this is the master's view of how far behind its
    /// replica is.
    #[must_use]
    pub fn slave_lag(&self) -> Option<ReplicaLag> {
        if self.connected_slaves == 0 {
            None
        } else {
            Some(ReplicaLag::compute(self.node_offset, self.slave_offset))
        }
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
        assert_eq!(s.connected_slaves, 0);
        assert_eq!(s.master_link, LinkStatus::Down);
        assert_eq!(s.master_endpoint, None);
        // A master with no slave -> no slave lag; a (non-attached) replica view is unknown.
        assert_eq!(s.slave_lag(), None);
    }

    #[test]
    fn primary_publishes_head_and_connected_slave_with_lag() {
        let status = ReplNodeStatus::new();
        status.set_master_head(ReplOffset(100));
        status.set_replica_connected(ReplOffset(95));
        let s = status.snapshot();
        assert_eq!(s.role, ReplRole::Master);
        assert_eq!(s.node_offset, ReplOffset(100));
        assert_eq!(s.connected_slaves, 1);
        assert_eq!(s.slave_offset, ReplOffset(95));
        // The master's view of its replica's lag: 100 - 95 = 5.
        assert_eq!(s.slave_lag().and_then(ReplicaLag::lag), Some(5));

        // The replica disconnects: connected_slaves drops, no slave lag.
        status.set_replica_disconnected();
        let s = status.snapshot();
        assert_eq!(s.connected_slaves, 0);
        assert_eq!(s.slave_lag(), None);
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
}
