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
//! NO snapshot, NO data, NO apply: those are HA-7b (full-sync) and HA-7c (the
//! steady-state KV stream + apply, wired to the HA-5a write-observation seam). This
//! slice is purely additive: a new crate exercising the cursor + heartbeat + ack.
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
pub use frames::{Frame, FrameError, REPLCONF, REPLPING};

pub mod link;
pub use link::{DEFAULT_HEARTBEAT, LinkEffects, LinkEvent, PrimaryLink, ReplState, ReplicaLink};

pub mod transport;
pub use transport::{ReplicaObserver, run_primary_repl_listener, run_replica_link};

/// The fixed offset, added to a node's cluster base port, where its replication
/// listener binds. Keeping replication on its OWN port (not the RESP client port or
/// the Raft bus port) is what physically separates the replication data plane from
/// the consensus control plane (REPLICATION.md #77).
pub const REPL_PORT_OFFSET: u16 = 10001;
