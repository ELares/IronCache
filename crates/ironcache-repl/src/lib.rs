// SPDX-License-Identifier: MIT OR Apache-2.0
//! Replication transport (HA-7a): the primary->replica link, the `(replid, offset)`
//! cursor, and the wire frames, kept ENTIRELY SEPARATE from the Raft cluster-bus so
//! replication can never block consensus (REPLICATION.md #77).
//!
//! ## Why a separate transport
//!
//! Consensus (the Raft cluster-bus, [`ironcache_raft_net`] over
//! [`ironcache_clusterbus`]) carries small, latency-critical control messages:
//! votes, AppendEntries, heartbeats. A stalled or slow node must NEVER let a bulk
//! replication stream sit in front of a Raft heartbeat, or a healthy leader could
//! be falsely failed. So this crate stands up an INDEPENDENT data plane: its own
//! dedicated port (the cluster base port + 10001), its own listener and dialer, its
//! own frames. It shares only the seams every node already uses (the
//! [`ironcache_runtime::Runtime`] for I/O and timers, the [`ironcache_env`]
//! determinism seam for the clock); it has NO dependency on the Raft crates and
//! never touches the consensus port.
//!
//! ## The three pieces (7a scope)
//!
//! 1. THE CURSOR ([`cursor`]): [`ReplId`] (the primary's replication id, a 20-byte
//!    value rendered 40-hex on the wire) and [`ReplOffset`] (a monotonic LOGICAL
//!    write-sequence offset, advanced per write in HA-7c; in 7a the primary advances
//!    it trivially so the mechanism is exercised). Together they name a resume point.
//! 2. THE FRAMES ([`frames`]): two RESP-array frames mirroring the Raft `RAFTMSG`
//!    codec shape: [`Frame::ReplConf`] (replica -> primary: attach handshake AND
//!    steady-state ack of the highest tracked offset) and [`Frame::ReplPing`]
//!    (primary -> replica: heartbeat carrying the primary's `(replid, offset)`). The
//!    full-sync frames are HA-7b; the codec is left extensible.
//! 3. THE LINK ([`link`] + [`transport`]): a PURE state machine ([`ReplicaLink`] /
//!    [`PrimaryLink`], a step function over [`LinkEvent`]) kept separable from the
//!    socket so it is DST-testable, plus the real-I/O adapters
//!    ([`run_replica_link`] / [`run_primary_repl_listener`]) that drive it over the
//!    `Runtime` seam and reconnect-resume on a drop.
//!
//! ## The data pieces (7b/7c)
//!
//! HA-7b added the FULL-SYNC ([`fullsync`] + [`kvcodec`]): the primary's whole snapshot
//! shipped to a fresh replica. HA-7c adds the STEADY-STATE TAIL: the [`observer`]
//! ([`ReplObserver`] + the bounded [`ReplRing`]) plugged into the HA-5a write-observation
//! seam advances the primary's offset PER WRITE and enqueues each as a [`StreamOp`]; the
//! [`stream`] half ships them ([`drain_and_ship`]) and the replica applies them in offset
//! order, idempotently, full-resyncing on a gap ([`ReplicaApplier`]). The convergence gate
//! (in `tests/convergence.rs`) drives a seeded workload + injected link faults and asserts
//! the replica keyspace equals the primary's over many seeds.
//!
//! ## The data-plane port offset
//!
//! The replication listener binds at `cluster_base_port + REPL_PORT_OFFSET`, a
//! fixed, documented offset off whatever base port a node's cluster bus uses, so the
//! replication plane has a deterministic address that never collides with the RESP
//! client port or the Raft bus.

#![forbid(unsafe_code)]

pub mod cursor;
pub use cursor::{ReplId, ReplOffset};

pub mod frames;
pub use frames::{
    FULLSYNC, Frame, FrameError, IMPORTREQ, REPLCONF, REPLPING, STREAMDEL, STREAMPUT, SYNCEND,
    SYNCKV,
};

pub mod kvcodec;
pub use kvcodec::{decode_kvobj, encode_entry_into, encode_kvobj};

pub mod link;
pub use link::{DEFAULT_HEARTBEAT, LinkEffects, LinkEvent, PrimaryLink, ReplState, ReplicaLink};

pub mod fullsync;
pub use fullsync::{FullSyncError, drive_full_sync, receive_full_sync};

pub mod observer;
pub use observer::{ReplObserver, ReplRing, StreamOp};

pub mod disk_backlog;
pub use disk_backlog::{DiskBacklog, SpillError};

pub mod lag;
pub use lag::{
    CandidateReplica, InSyncReplicas, LinkStatus, PromotionSafety, ReplNodeStatus, ReplRole,
    ReplStatusSnapshot, ReplicaLag, lag, replica_is_in_sync, safe_to_promote,
};

// The clustered rolling-upgrade orchestration state machine (#392 Phase 3): the PURE next-step
// decision (replicas first -> promote -> old primary last) consuming the safe_to_promote guardrail.
// The binary swap + the raft commit are the clustered driver's job; this decides what/when.
pub mod upgrade_plan;
pub use upgrade_plan::{
    BlockReason, UpgradeActions, UpgradeReport, UpgradeStep, drive_upgrade_step,
    run_rolling_upgrade, upgrade_step,
};

pub mod stream;
pub use stream::{ApplyOutcome, ReplicaApplier, ShipOutcome, drain_and_ship};

pub mod transport;
pub use transport::{ReplicaObserver, run_primary_repl_listener, run_replica_link};

/// The fixed offset, added to a node's cluster base port, where its replication
/// listener binds. Keeping replication on its OWN port (not the RESP client port or
/// the Raft bus port) is what physically separates the replication data plane from
/// the consensus control plane (REPLICATION.md #77).
pub const REPL_PORT_OFFSET: u16 = 10001;
