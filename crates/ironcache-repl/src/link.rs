// SPDX-License-Identifier: MIT OR Apache-2.0
//! The replication link state machine: the pure, DST-friendly core of HA-7a.
//!
//! Like the Raft engine ([`ironcache_raft`] in spirit), the link logic is a PURE
//! step function: it owns no clock, no socket, and no RNG. It reacts to a
//! [`LinkEvent`] by mutating its in-memory state and recording its intent on a
//! [`LinkEffects`] set (a frame to send, a timer to arm). The [`crate::transport`]
//! adapter is the ONLY thing that touches a real socket or timer; it drives this
//! step and performs the I/O the effects describe. Keeping the state machine
//! separable is what lets the SAME logic run under the deterministic [`ironcache_sim`]
//! harness (where partition/heal/drop are injected) and over real TCP.
//!
//! Two roles, two state machines, one shape:
//!
//! - [`ReplicaLink`] (the replica side, the interesting one): connects, sends
//!   [`Frame::ReplConf`] with its last-acked offset, receives [`Frame::ReplPing`],
//!   advances its OBSERVED offset monotonically, and on a disconnect RECONNECTS and
//!   re-sends `REPLCONF` from its last-acked offset (the resume point). The
//!   last-acked offset NEVER goes backwards.
//! - [`PrimaryLink`] (the primary side, per connected replica): tracks that
//!   replica's acked offset (from its `REPLCONF`) and emits `REPLPING` heartbeats
//!   carrying the primary's current `(replid, offset)`.
//!
//! NO data and NO apply: 7a is the cursor + heartbeat + ack only. HA-7b adds the
//! full-sync transition, HA-7c the steady-state KV stream and apply.

use core::time::Duration;

use crate::cursor::{ReplId, ReplOffset};
use crate::frames::Frame;

/// How long a replica waits between heartbeats before it declares the link dead and
/// reconnects, and how often a primary emits a `REPLPING`. A plain default for 7a;
/// the real value is config-driven later. Read by the transport (which owns the
/// clock); the pure step function only emits an "arm a timer for THIS duration"
/// intent and never reads time itself.
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_millis(100);

/// The replica link's observable state.
///
/// `Disconnected` -> `Connecting` -> `Synced` is the happy path; a `Disconnected`
/// at any point is the reconnect trigger. `Synced` carries the highest OBSERVED
/// offset (what the primary's last `REPLPING` reported); the SEPARATELY-tracked
/// `acked` offset (on [`ReplicaLink`]) is the durable resume point and is what a
/// reconnect's `REPLCONF` carries. (7b extends this enum with a `FullSync` state;
/// the variant set is deliberately open.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplState {
    /// No link: either never connected or the connection dropped. The next
    /// [`LinkEvent::Connected`] moves to `Connecting`.
    Disconnected,
    /// The socket is up and the attach `REPLCONF` has been sent; awaiting the first
    /// `REPLPING`.
    Connecting,
    /// Receiving heartbeats. `offset` is the highest offset OBSERVED from the
    /// primary's `REPLPING`s (monotonic non-decreasing).
    Synced {
        /// The highest offset observed from the primary so far.
        offset: ReplOffset,
    },
}

/// An input to a link step. The transport translates real-world happenings (a
/// socket connected, a decoded frame arrived, the socket dropped, a timer fired)
/// into these; the pure step never observes the socket or clock directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkEvent {
    /// The transport established (or re-established) the connection.
    Connected,
    /// A [`Frame::ReplPing`] arrived from the primary (replica side).
    GotPing {
        /// The primary's advertised replication id.
        replid: ReplId,
        /// The primary's advertised current offset.
        offset: ReplOffset,
    },
    /// A [`Frame::ReplConf`] arrived from a replica (primary side).
    GotReplconf {
        /// The replica's node id.
        node: u64,
        /// The replica's acked (resume-point) offset.
        ack: ReplOffset,
    },
    /// The connection dropped (peer closed or an I/O error). Triggers reconnect on
    /// the replica side.
    Disconnected,
    /// The heartbeat timer fired (used by the primary to emit a `REPLPING`).
    Tick,
}

/// The intent a link step records: at most one frame to send and at most one timer
/// to arm. The transport applies these AFTER the step returns (the engine borrow
/// has ended), exactly the collect-then-drain discipline the DST harness and the
/// Raft adapter use, so a step can never observe a half-applied effect.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LinkEffects {
    /// A frame to write to the peer, if any.
    pub send: Option<Frame>,
    /// A timer to arm (the heartbeat / reconnect interval), if any.
    pub arm_timer: Option<Duration>,
}

impl LinkEffects {
    /// An empty effect set (nothing to send, no timer to arm).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

// ---------------------------------------------------------------------------
// The replica side.
// ---------------------------------------------------------------------------

/// The replica's view of its link to a primary: the pure state machine that attaches
/// with `REPLCONF`, tracks the primary's offset from `REPLPING`, and resumes from
/// its last-acked offset on reconnect.
///
/// The `acked` offset is the durable resume point: it advances monotonically as the
/// replica observes (and, in 7c, durably applies) the primary's stream, and it is
/// the value a reconnect's `REPLCONF` carries. In 7a, where no data crosses the
/// wire, the replica treats an OBSERVED `REPLPING` offset as immediately acked (the
/// cursor is exercised; durable apply gates the ack in 7c).
#[derive(Debug, Clone)]
pub struct ReplicaLink {
    /// This replica's node id, sent in every `REPLCONF`.
    node: u64,
    /// The current link state.
    state: ReplState,
    /// The highest offset the replica has ACKED (its durable resume point). Sent in
    /// `REPLCONF` on attach/reconnect. Monotonic non-decreasing.
    acked: ReplOffset,
    /// The replid the primary last advertised, if any. A CHANGED replid is the 7b
    /// full-sync trigger; in 7a it is observed and recorded only.
    primary_replid: Option<ReplId>,
}

impl ReplicaLink {
    /// A fresh replica link for `node`, starting `Disconnected` with `acked` as its
    /// resume point (`ReplOffset::ZERO` for a never-synced replica, or a recovered
    /// offset after a restart).
    #[must_use]
    pub fn new(node: u64, acked: ReplOffset) -> Self {
        ReplicaLink {
            node,
            state: ReplState::Disconnected,
            acked,
            primary_replid: None,
        }
    }

    /// The current link state.
    #[must_use]
    pub fn state(&self) -> ReplState {
        self.state
    }

    /// The replica's last-acked offset: its durable resume point and the value its
    /// next `REPLCONF` carries.
    #[must_use]
    pub fn acked(&self) -> ReplOffset {
        self.acked
    }

    /// The replid the primary last advertised, if a `REPLPING` has been seen.
    #[must_use]
    pub fn primary_replid(&self) -> Option<ReplId> {
        self.primary_replid
    }

    /// Step the replica link on `event`, returning the effects to apply.
    ///
    /// - `Connected` -> send `REPLCONF` from the last-acked offset (attach / resume),
    ///   move to `Connecting`, and arm the heartbeat-deadline timer.
    /// - `GotPing` -> advance the observed offset MONOTONICALLY, ack it (7a treats
    ///   observed-as-acked; 7c gates the ack on durable apply), record the replid,
    ///   move to `Synced`, and re-arm the deadline.
    /// - `Disconnected` -> drop to `Disconnected` (the transport will re-dial, which
    ///   produces the next `Connected`); no frame, no timer.
    /// - `Tick` (deadline) -> a missed heartbeat; treat as a dropped link and go
    ///   `Disconnected` so the transport reconnects.
    /// - `GotReplconf` -> not a replica input; ignored.
    pub fn step(&mut self, event: LinkEvent) -> LinkEffects {
        let mut fx = LinkEffects::new();
        match event {
            LinkEvent::Connected => {
                self.state = ReplState::Connecting;
                // Attach / resume: announce our id and the offset to resume FROM.
                fx.send = Some(Frame::ReplConf {
                    node: self.node,
                    ack: self.acked,
                });
                // Arm the heartbeat-deadline timer; a Tick before the next ping means
                // the link is dead.
                fx.arm_timer = Some(DEFAULT_HEARTBEAT);
            }
            LinkEvent::GotPing { replid, offset } => {
                self.primary_replid = Some(replid);
                // Advance the observed offset monotonically: a stale or reordered
                // ping with a LOWER offset never moves the cursor backwards.
                let observed = match self.state {
                    ReplState::Synced { offset: prev } => prev.max_with(offset),
                    _ => offset,
                };
                // 7a: observed is immediately acked (no durable apply yet). The ack
                // also only ever advances.
                self.acked = self.acked.max_with(observed);
                self.state = ReplState::Synced { offset: observed };
                // Re-arm the deadline: we just heard from the primary.
                fx.arm_timer = Some(DEFAULT_HEARTBEAT);
            }
            LinkEvent::Disconnected | LinkEvent::Tick => {
                // A dropped socket or a missed-heartbeat deadline: declare the link
                // dead. The transport re-dials, which yields the next `Connected`,
                // whose `REPLCONF` resumes from `self.acked` (unchanged here, so the
                // resume point is preserved).
                self.state = ReplState::Disconnected;
            }
            LinkEvent::GotReplconf { .. } => {
                // A primary-side input; a replica never receives REPLCONF. Ignore.
            }
        }
        fx
    }
}

// ---------------------------------------------------------------------------
// The primary side.
// ---------------------------------------------------------------------------

/// The primary's view of ONE connected replica: the pure state machine that records
/// that replica's acked offset (from its `REPLCONF`) and emits `REPLPING`
/// heartbeats carrying the primary's current `(replid, offset)`.
///
/// The primary's own stream offset advances elsewhere (per write in 7c, per tick in
/// 7a); this link is handed the CURRENT offset when it needs to send a heartbeat, so
/// the link itself owns no offset-advancing policy, only the per-replica ack
/// tracking.
#[derive(Debug, Clone)]
pub struct PrimaryLink {
    /// The primary's replication id, advertised in every `REPLPING`.
    replid: ReplId,
    /// The connected replica's node id, learned from its first `REPLCONF`.
    replica: Option<u64>,
    /// The highest offset this replica has acked (monotonic non-decreasing). The
    /// primary uses this (in 7b/7c) to decide where to resume the replica's stream.
    replica_acked: ReplOffset,
    /// Whether the connection is up (a `REPLCONF` has been received and not since
    /// disconnected).
    connected: bool,
}

impl PrimaryLink {
    /// A fresh primary-side link advertising `replid`, with no replica attached yet.
    #[must_use]
    pub fn new(replid: ReplId) -> Self {
        PrimaryLink {
            replid,
            replica: None,
            replica_acked: ReplOffset::ZERO,
            connected: false,
        }
    }

    /// The replica's node id, once it has attached.
    #[must_use]
    pub fn replica(&self) -> Option<u64> {
        self.replica
    }

    /// The highest offset the replica has acked (its resume point as the primary
    /// understands it).
    #[must_use]
    pub fn replica_acked(&self) -> ReplOffset {
        self.replica_acked
    }

    /// Whether a replica is currently attached.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Step the primary link on `event`, given the primary's `current_offset` (the
    /// value a heartbeat advertises), returning the effects to apply.
    ///
    /// - `Connected` -> a replica's socket arrived; arm the heartbeat timer. (The
    ///   first heartbeat goes out on the next `Tick`.)
    /// - `GotReplconf` -> record the replica's id and advance its acked offset
    ///   MONOTONICALLY (a stale ack never lowers it).
    /// - `Tick` -> emit a `REPLPING` advertising `(replid, current_offset)` and
    ///   re-arm the heartbeat timer.
    /// - `Disconnected` -> the replica dropped; stop (no heartbeat, no timer) until
    ///   it reconnects. The acked offset is RETAINED as the resume point.
    /// - `GotPing` -> not a primary input; ignored.
    pub fn step(&mut self, event: LinkEvent, current_offset: ReplOffset) -> LinkEffects {
        let mut fx = LinkEffects::new();
        match event {
            LinkEvent::Connected => {
                self.connected = true;
                fx.arm_timer = Some(DEFAULT_HEARTBEAT);
            }
            LinkEvent::GotReplconf { node, ack } => {
                self.connected = true;
                self.replica = Some(node);
                self.replica_acked = self.replica_acked.max_with(ack);
            }
            LinkEvent::Tick => {
                if self.connected {
                    fx.send = Some(Frame::ReplPing {
                        replid: self.replid,
                        offset: current_offset,
                    });
                    fx.arm_timer = Some(DEFAULT_HEARTBEAT);
                }
            }
            LinkEvent::Disconnected => {
                self.connected = false;
            }
            LinkEvent::GotPing { .. } => {
                // A replica-side input; a primary never receives REPLPING. Ignore.
            }
        }
        fx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replid(n: u8) -> ReplId {
        ReplId::from_bytes([n; 20])
    }

    #[test]
    fn replica_attaches_then_syncs_and_tracks_offset() {
        let mut link = ReplicaLink::new(9, ReplOffset::ZERO);
        assert_eq!(link.state(), ReplState::Disconnected);

        // Connect: it sends REPLCONF from offset 0 and arms the deadline.
        let fx = link.step(LinkEvent::Connected);
        assert_eq!(
            fx.send,
            Some(Frame::ReplConf {
                node: 9,
                ack: ReplOffset(0)
            })
        );
        assert_eq!(fx.arm_timer, Some(DEFAULT_HEARTBEAT));
        assert_eq!(link.state(), ReplState::Connecting);

        // First ping at offset 5: it syncs and observes 5.
        let fx = link.step(LinkEvent::GotPing {
            replid: replid(1),
            offset: ReplOffset(5),
        });
        assert_eq!(
            link.state(),
            ReplState::Synced {
                offset: ReplOffset(5)
            }
        );
        assert_eq!(link.acked(), ReplOffset(5));
        assert_eq!(link.primary_replid(), Some(replid(1)));
        assert_eq!(fx.arm_timer, Some(DEFAULT_HEARTBEAT));

        // A higher ping advances; a stale lower ping does NOT move it backwards.
        link.step(LinkEvent::GotPing {
            replid: replid(1),
            offset: ReplOffset(8),
        });
        assert_eq!(
            link.state(),
            ReplState::Synced {
                offset: ReplOffset(8)
            }
        );
        link.step(LinkEvent::GotPing {
            replid: replid(1),
            offset: ReplOffset(3),
        });
        assert_eq!(
            link.state(),
            ReplState::Synced {
                offset: ReplOffset(8)
            },
            "a stale lower ping must not lower the observed offset"
        );
        assert_eq!(link.acked(), ReplOffset(8));
    }

    #[test]
    fn replica_reconnect_resumes_from_acked_offset() {
        let mut link = ReplicaLink::new(2, ReplOffset::ZERO);
        link.step(LinkEvent::Connected);
        link.step(LinkEvent::GotPing {
            replid: replid(7),
            offset: ReplOffset(11),
        });
        assert_eq!(link.acked(), ReplOffset(11));

        // The link drops.
        link.step(LinkEvent::Disconnected);
        assert_eq!(link.state(), ReplState::Disconnected);
        // The resume point is preserved across the drop.
        assert_eq!(link.acked(), ReplOffset(11));

        // On reconnect, the REPLCONF carries the last-acked offset (11), not 0.
        let fx = link.step(LinkEvent::Connected);
        assert_eq!(
            fx.send,
            Some(Frame::ReplConf {
                node: 2,
                ack: ReplOffset(11)
            }),
            "reconnect must resume from the last-acked offset"
        );
    }

    #[test]
    fn replica_missed_heartbeat_drops_the_link() {
        let mut link = ReplicaLink::new(1, ReplOffset::ZERO);
        link.step(LinkEvent::Connected);
        link.step(LinkEvent::GotPing {
            replid: replid(1),
            offset: ReplOffset(4),
        });
        // A Tick (deadline) with no intervening ping declares the link dead.
        let fx = link.step(LinkEvent::Tick);
        assert_eq!(link.state(), ReplState::Disconnected);
        assert_eq!(fx.send, None);
        // The acked offset survives, so the eventual reconnect resumes from 4.
        assert_eq!(link.acked(), ReplOffset(4));
    }

    #[test]
    fn primary_heartbeats_and_tracks_replica_ack() {
        let mut link = PrimaryLink::new(replid(3));
        assert!(!link.is_connected());

        // The replica attaches with REPLCONF at ack 6.
        link.step(
            LinkEvent::GotReplconf {
                node: 42,
                ack: ReplOffset(6),
            },
            ReplOffset(10),
        );
        assert_eq!(link.replica(), Some(42));
        assert_eq!(link.replica_acked(), ReplOffset(6));
        assert!(link.is_connected());

        // A Tick emits a REPLPING advertising the primary's CURRENT offset.
        let fx = link.step(LinkEvent::Tick, ReplOffset(10));
        assert_eq!(
            fx.send,
            Some(Frame::ReplPing {
                replid: replid(3),
                offset: ReplOffset(10)
            })
        );
        assert_eq!(fx.arm_timer, Some(DEFAULT_HEARTBEAT));

        // A stale ack never lowers the tracked resume point.
        link.step(
            LinkEvent::GotReplconf {
                node: 42,
                ack: ReplOffset(2),
            },
            ReplOffset(12),
        );
        assert_eq!(link.replica_acked(), ReplOffset(6));

        // On disconnect, no heartbeat goes out and the ack is retained.
        link.step(LinkEvent::Disconnected, ReplOffset(12));
        assert!(!link.is_connected());
        let fx = link.step(LinkEvent::Tick, ReplOffset(12));
        assert_eq!(fx.send, None, "no heartbeat to a disconnected replica");
        assert_eq!(link.replica_acked(), ReplOffset(6));
    }
}
