// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-7a DST: the replication link state machine under the deterministic
//! [`ironcache_sim`] virtual-time network with partition/heal.
//!
//! The link logic ([`ironcache_repl::ReplicaLink`] / [`PrimaryLink`]) is a PURE
//! step function, exactly the shape [`ironcache_sim::SimNode`] models, so it runs in
//! the same Flow/VOPR-style harness the Raft engine is verified in. The two sides
//! are wrapped as one [`ReplNode`] enum (the `Network` holds nodes of a single type
//! sharing `Msg = Frame`); the harness injects a partition, heals it, and drives the
//! virtual clock, and the test asserts the three HA-7a properties:
//!
//!   (a) the replica tracks the primary's offset MONOTONICALLY (never backwards);
//!   (b) after a partition + heal, the replica RECONNECTS and re-sends `REPLCONF`
//!       from its LAST-ACKED offset (the resume point), and the acked offset never
//!       regresses;
//!   (c) two seeded runs of the scenario dispatch a BYTE-IDENTICAL trace.
//!
//! No real socket, no real clock: the connect/disconnect of a real link is modeled
//! with sim timers. The replica arms a periodic DIAL timer (a real reconnect loop)
//! and a DEADLINE timer (a missed heartbeat declares the link dead); under a
//! partition the attach `REPLCONF` is dropped so the link stays Disconnected and the
//! next dial retries. This is faithful to the transport's `run_replica_link`, which
//! reconnects on a drop and resumes from its last-acked offset.

use core::time::Duration;

use ironcache_repl::link::{LinkEvent, PrimaryLink, ReplState, ReplicaLink};
use ironcache_repl::{Frame, LinkEffects, ReplId, ReplOffset};
use ironcache_sim::{Network, NodeId, SimCtx, SimNode};

// Timer tokens used by the simulated link. Distinct so re-arming one does not touch
// another.
const TOK_DIAL: u64 = 1; // replica: periodic attempt to (re)establish the link
const TOK_DEADLINE: u64 = 2; // replica: heartbeat deadline; a fire means "link dead"
const TOK_HEARTBEAT: u64 = 3; // primary: emit a REPLPING

// The sim cadences (virtual durations chosen so heartbeats land well inside the
// deadline and the dial period recovers a dropped link promptly).
const DIAL_PERIOD: Duration = Duration::from_millis(40);
const DEADLINE: Duration = Duration::from_millis(120);
const HEARTBEAT: Duration = Duration::from_millis(30);

const PRIMARY: NodeId = NodeId(1);
const REPLICA: NodeId = NodeId(2);

fn the_replid() -> ReplId {
    ReplId::from_bytes([0xAB; 20])
}

/// One node in the simulated replication pair: either the primary or the replica.
/// Both speak [`Frame`].
enum ReplNode {
    Primary(PrimarySim),
    Replica(ReplicaSim),
}

/// The primary side under simulation: the pure [`PrimaryLink`] plus a trivially
/// advancing offset (7a advances the offset per heartbeat tick, exercising the
/// cursor with no data on the wire).
struct PrimarySim {
    link: PrimaryLink,
    offset: ReplOffset,
}

/// The replica side under simulation: the pure [`ReplicaLink`] plus a record of
/// every offset it observed (for monotonicity) and every `REPLCONF` ack it emitted
/// (for the resume-point check).
struct ReplicaSim {
    link: ReplicaLink,
    observed: Vec<ReplOffset>,
    replconf_acks: Vec<ReplOffset>,
}

impl SimNode for ReplNode {
    type Msg = Frame;

    fn on_message(&mut self, _from: NodeId, msg: Frame, ctx: &mut SimCtx<'_, Frame>) {
        match self {
            ReplNode::Primary(p) => {
                if let Frame::ReplConf { node, ack } = msg {
                    // A replica attached/resumed: record its ack and (re)arm heartbeat.
                    let fx = p.link.step(LinkEvent::GotReplconf { node, ack }, p.offset);
                    apply(ctx, REPLICA, fx, TOK_HEARTBEAT);
                    // Ensure the heartbeat is running even if the step did not re-arm
                    // (GotReplconf arms nothing on its own).
                    ctx.set_timer(TOK_HEARTBEAT, HEARTBEAT);
                }
            }
            ReplNode::Replica(r) => match msg {
                Frame::ReplPing { replid, offset } => {
                    r.link.step(LinkEvent::GotPing { replid, offset });
                    if let ReplState::Synced { offset: obs } = r.link.state() {
                        r.observed.push(obs);
                    }
                    // A ping resets the heartbeat deadline.
                    ctx.set_timer(TOK_DEADLINE, DEADLINE);
                }
                Frame::ReplConf { .. } => {
                    // The boot kick: arm the replica's first dial. (The link ignores
                    // a REPLCONF, so this only schedules the dial loop.)
                    ctx.set_timer(TOK_DIAL, DIAL_PERIOD);
                }
                // The HA-7b full-sync frames and the HA-7c steady-state tail frames are not
                // exercised by this 7a heartbeat/cursor DST scenario; ignore them here (their
                // own tests cover them).
                Frame::FullSync { .. }
                | Frame::SyncKv { .. }
                | Frame::SyncEnd { .. }
                | Frame::StreamPut { .. }
                | Frame::StreamDel { .. }
                | Frame::ImportReq { .. } => {}
            },
        }
    }

    fn on_timer(&mut self, token: u64, ctx: &mut SimCtx<'_, Frame>) {
        match self {
            ReplNode::Primary(p) => {
                if token == TOK_HEARTBEAT {
                    // Advance the logical offset once per heartbeat (7a: per-tick),
                    // then emit a REPLPING advertising it.
                    p.offset = p.offset.next();
                    let fx = p.link.step(LinkEvent::Tick, p.offset);
                    apply(ctx, REPLICA, fx, TOK_HEARTBEAT);
                }
            }
            ReplNode::Replica(r) => match token {
                TOK_DIAL => {
                    // (Re)dial: if the link is down, attach. A real dial succeeds by
                    // the REPLCONF reaching the primary; under a partition it is
                    // dropped and the link stays Disconnected, so the next dial
                    // retries.
                    if r.link.state() == ReplState::Disconnected {
                        let fx = r.link.step(LinkEvent::Connected);
                        if let Some(Frame::ReplConf { ack, .. }) = fx.send {
                            r.replconf_acks.push(ack);
                        }
                        apply(ctx, PRIMARY, fx, TOK_DEADLINE);
                    }
                    // Keep dialing periodically so a dropped link recovers.
                    ctx.set_timer(TOK_DIAL, DIAL_PERIOD);
                }
                TOK_DEADLINE => {
                    // No ping before the deadline: declare the link dead so the next
                    // dial re-attaches and resumes from the last-acked offset.
                    r.link.step(LinkEvent::Tick); // ReplicaLink: Tick => Disconnected
                }
                _ => {}
            },
        }
    }
}

/// Apply a replica/primary link step's effects in the sim: send the emitted frame to
/// `to`, and if the step armed a timer, arm the role's local cadence timer. The pure
/// step's arm DURATION is its heartbeat interval; the sim maps it to the role token's
/// chosen virtual period (the absolute value is a harness choice; what matters is the
/// arm happens, preserving the link's timing structure).
fn apply(ctx: &mut SimCtx<'_, Frame>, to: NodeId, fx: LinkEffects, timer_token: u64) {
    if let Some(frame) = fx.send {
        ctx.send(to, frame);
    }
    if fx.arm_timer.is_some() {
        let period = if timer_token == TOK_HEARTBEAT {
            HEARTBEAT
        } else {
            DEADLINE
        };
        ctx.set_timer(timer_token, period);
    }
}

/// Build the two-node replication network (clock at origin, RNG seeded).
fn build(seed: u64) -> Network<ReplNode> {
    let mut net = Network::new(seed);
    net.add_node(
        PRIMARY,
        ReplNode::Primary(PrimarySim {
            link: PrimaryLink::new(the_replid()),
            offset: ReplOffset::ZERO,
        }),
    );
    net.add_node(
        REPLICA,
        ReplNode::Replica(ReplicaSim {
            link: ReplicaLink::new(REPLICA.0, ReplOffset::ZERO),
            observed: Vec::new(),
            replconf_acks: Vec::new(),
        }),
    );
    net
}

/// The boot kick: a frame whose arrival arms the replica's first dial (see the
/// replica's `on_message`). The link itself ignores a REPLCONF; this only starts the
/// dial loop, after which the link is timer-driven.
fn boot_kick() -> Frame {
    Frame::ReplConf {
        node: 0,
        ack: ReplOffset::ZERO,
    }
}

/// Inspect the replica node after a run.
fn replica(net: &Network<ReplNode>) -> &ReplicaSim {
    match net.node(REPLICA).expect("replica present") {
        ReplNode::Replica(r) => r,
        ReplNode::Primary(_) => panic!("REPLICA id holds a primary"),
    }
}

/// Run the full partition/heal scenario on a fresh seeded network and return it for
/// inspection (and its trace via `net.trace()`), plus the RESUME POINT: the offset
/// the replica had acked at the moment its link went Disconnected under the
/// partition. That is the exact value a correct reconnect `REPLCONF` must carry.
fn run_scenario(seed: u64) -> (Network<ReplNode>, ReplOffset) {
    let mut net = build(seed);
    net.tell(REPLICA, REPLICA, boot_kick());

    // Phase 1: establish the link and let several heartbeats flow.
    net.run_steps(40);

    // Phase 2: partition the replica off. Heartbeats are dropped, the deadline fires,
    // and the link goes Disconnected. Step one event at a time so we can capture the
    // acked offset at the EXACT moment the link drops (the resume point).
    net.partition(&[PRIMARY], &[REPLICA]);
    let mut resume_point = replica(&net).link.acked();
    for _ in 0..80 {
        if !net.step() {
            break;
        }
        let r = replica(&net);
        if r.link.state() == ReplState::Disconnected {
            resume_point = r.link.acked();
            break;
        }
    }
    // Drain the rest of the partitioned interval (failed dials across the cut).
    net.run_steps(80);

    // Phase 3: heal. The next dial's REPLCONF reaches the primary; the link resumes
    // from `resume_point` and offsets advance again.
    net.heal();
    net.run_steps(160);

    (net, resume_point)
}

#[test]
fn replica_tracks_offset_resumes_after_partition_and_replays_deterministically() {
    // (c) Determinism: two seeded runs dispatch byte-identical traces.
    let (net1, _) = run_scenario(7);
    let (net2, _) = run_scenario(7);
    assert_eq!(
        net1.trace(),
        net2.trace(),
        "two seeded runs must replay byte-identically"
    );

    let (net, resume_point) = run_scenario(7);
    let r = replica(&net);

    // The link established at all (offsets observed, acks emitted).
    assert!(!r.observed.is_empty(), "the replica should observe pings");
    assert!(resume_point > ReplOffset::ZERO, "the cursor should advance");

    // (a) Monotonicity: observed offsets never regress.
    for w in r.observed.windows(2) {
        assert!(
            w[1] >= w[0],
            "observed offset regressed: {:?} then {:?}",
            w[0],
            w[1]
        );
    }
    // The resume points the replica announced (its REPLCONF acks) are likewise
    // monotonic non-decreasing: the cursor never goes backwards across reconnects.
    for w in r.replconf_acks.windows(2) {
        assert!(
            w[1] >= w[0],
            "REPLCONF ack regressed: {:?} then {:?}",
            w[0],
            w[1]
        );
    }

    // (b) Resume after heal: the link re-attached at least twice (initial + at least
    // one reconnect) and the reconnect REPLCONF carried the EXACT resume point the
    // link held when it dropped (not zero), then offsets advanced further.
    assert!(
        r.replconf_acks.len() >= 2,
        "the replica should attach at least twice (initial + reconnect); acks {:?}",
        r.replconf_acks
    );
    let resumed = r.replconf_acks.iter().skip(1).any(|&a| a == resume_point);
    assert!(
        resumed,
        "a reconnect REPLCONF must resume from the offset acked at disconnect \
         ({resume_point:?}); saw acks {:?}",
        r.replconf_acks
    );
    let acked_after = r.link.acked();
    assert!(
        acked_after >= resume_point,
        "after heal the acked offset must not regress (resume {resume_point:?}, now {acked_after:?})"
    );
    assert!(
        acked_after > resume_point,
        "after heal the stream must advance past the resume point"
    );
}
