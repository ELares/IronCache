// SPDX-License-Identifier: MIT OR Apache-2.0
//! Deterministic single-threaded multi-node simulation harness (HA-2, TESTING.md
//! "deterministic simulation (DST)" / JEPSEN_PLAN.md).
//!
//! This is the verification SUBSTRATE the Raft control plane (#73) and replication
//! (#77) are written and tested against, before they are wired to real transport.
//! It is a Flow/VOPR-style simulator [dst-fdb-tigerbeetle-single-seed]: a set of
//! step-based node state machines, a virtual-time message network, and a fault
//! injector, driven entirely off the [`ironcache_env`] determinism seam (ADR-0003)
//! so every run replays byte-identically from its seed.
//!
//! ## Why standalone (not the real server)
//!
//! The shipped server (`ironcache::serve` / `ironcache-runtime::bootstrap`) is
//! HARDCODED to the tokio backend: real threads, real time, real sockets. It is
//! NOT generic over a simulated clock, so it cannot be driven in virtual time.
//! HA-2 therefore does NOT run the server in simulation. It defines the SHAPE the
//! consensus and replication protocols will be written in - a pure step function
//! over messages and timers - and exercises that shape here. Production wires the
//! SAME [`SimNode`] logic to `ironcache-clusterbus` + the tokio runtime later; the
//! sim is the test harness, the real transport is the deployment.
//!
//! ## The model
//!
//! A protocol implements [`SimNode`]: a state machine that reacts to two inputs,
//! an inbound message ([`SimNode::on_message`]) and an expired timer
//! ([`SimNode::on_timer`]). A node never touches the clock, the RNG, or the
//! network directly; every effect is recorded on the [`SimCtx`] it is handed for
//! the step (outbound sends, timer arm/cancel, and reads of virtual time and the
//! seeded RNG). The [`Network`] drains those effects AFTER the callback returns, so
//! a node can never observe a half-applied step, and the harness owns the single
//! point where nondeterminism could leak in.
//!
//! ## Determinism (the whole point)
//!
//! Two runs with the same seed, the same node code, and the same fault script
//! dispatch an IDENTICAL sequence of events. Two properties guarantee it:
//!
//! 1. The event queue has a TOTAL order. Events sort by `(scheduled_time ASC, seq
//!    ASC)` where `seq` is a single monotonically increasing counter stamped at
//!    enqueue time. No two events share a key, so the pop order is a function of
//!    history alone, never of `BinaryHeap` internal layout or hashing.
//! 2. Every nondeterministic CHOICE (per-message latency, the drop roll, and any
//!    node-level randomness like an election-timeout jitter) is drawn from the ONE
//!    seeded [`ironcache_env::TestEnv`] RNG, in a fixed, documented order at a
//!    fixed point in [`Network::step`]. The draw order is: for each outbound send
//!    of a step, in send order, the drop roll first (when a drop probability is
//!    set), then the latency draw (when a latency range is set). Node RNG draws
//!    happen during the callback, before any of the network draws for that step.
//!
//! A compact [`TraceRecord`] of every dispatched event is recorded so a test can
//! assert two seeded runs produce byte-identical traces; that is the single-seed
//! reproducibility property the Jepsen gate depends on.

#![forbid(unsafe_code)]

use core::cmp::Reverse;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use ironcache_env::{Clock, Env, Monotonic, Rng, TestEnv};

/// A node's identity in the simulated cluster.
///
/// A thin newtype over a `u64` so node ids are `Copy`, totally ordered (the
/// [`Network`] keeps nodes in a `BTreeMap` keyed by this, for deterministic
/// iteration), and cheap to stamp into a [`TraceRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

/// A step-based node state machine, the shape every simulated protocol (Raft,
/// replication, migration) is written in.
///
/// A node is PURE with respect to the outside world: it reacts to an inbound
/// message or an expired timer and records its intended effects on the supplied
/// [`SimCtx`]. It never reads the wall clock, the OS RNG, or the network; it never
/// blocks. The associated [`SimNode::Msg`] is the protocol's wire message; it is
/// `Clone` because the network may fan a logical send out to many peers and must
/// hand each delivery its own copy.
pub trait SimNode {
    /// The protocol message type carried between nodes.
    type Msg: Clone;

    /// React to a message delivered `from` another node. Record outbound sends and
    /// timer operations on `ctx`; do not mutate the network directly.
    fn on_message(&mut self, from: NodeId, msg: Self::Msg, ctx: &mut SimCtx<'_, Self::Msg>);

    /// React to a timer previously armed with [`SimCtx::set_timer`] under `token`
    /// expiring. A cancelled timer never fires. Record effects on `ctx`.
    fn on_timer(&mut self, token: u64, ctx: &mut SimCtx<'_, Self::Msg>);
}

/// The effects a node intends during one step, plus the read-only handles
/// (virtual `now`, seeded RNG) it is allowed to consult.
///
/// Effects are COLLECTED here and applied by [`Network::step`] only after the node
/// callback returns. That ordering is deliberate: a node can never observe a
/// half-mutated network mid-callback, and the harness keeps a single, auditable
/// point where sends become deliveries and where the fault rules and latency draws
/// are applied. Timer operations are recorded in issue order; a `set_timer` for a
/// token already armed REPLACES it (the latest arm wins), matching how a protocol
/// "reset my election timeout" reads.
pub struct SimCtx<'a, M> {
    /// The virtual instant this step is running at (the event's scheduled time).
    now: Monotonic,
    /// Outbound sends queued this step, in issue order.
    sends: Vec<(NodeId, M)>,
    /// Timer operations queued this step, in issue order.
    timer_ops: Vec<TimerOp>,
    /// A borrow of the sim's single seeded RNG, so node-level randomness (election
    /// jitter, etc.) draws from the same reproducible stream as the network.
    rng: &'a mut <TestEnv as Env>::Rng,
}

/// A timer arm or cancel recorded on the [`SimCtx`] during a step.
enum TimerOp {
    /// Arm `token` to fire `after` the current virtual time.
    Set { token: u64, after: Duration },
    /// Cancel `token` if it is currently armed (a no-op otherwise).
    Cancel { token: u64 },
}

impl<M> SimCtx<'_, M> {
    /// The current virtual time (the scheduled time of the event being dispatched).
    #[inline]
    pub fn now(&self) -> Monotonic {
        self.now
    }

    /// A `u64` in `[0, bound)` from the sim's seeded RNG. Use this for any
    /// node-level randomness (e.g. a randomized election timeout) so it stays
    /// replayable. Returns `0` when `bound == 0`.
    #[inline]
    pub fn gen_below(&mut self, bound: u64) -> u64 {
        self.rng.gen_below(bound)
    }

    /// A `f64` in `[0.0, 1.0)` from the sim's seeded RNG, for node-level randomness
    /// that wants a unit fraction.
    #[inline]
    pub fn gen_unit_f64(&mut self) -> f64 {
        self.rng.gen_unit_f64()
    }

    /// Queue a message to `to`. Whether it is delivered, and after how much virtual
    /// latency, is decided by the [`Network`] under the active [`FaultConfig`] when
    /// this step's effects are drained.
    #[inline]
    pub fn send(&mut self, to: NodeId, msg: M) {
        self.sends.push((to, msg));
    }

    /// Arm a timer to fire `after` the current virtual time, identified by `token`.
    /// Arming a token that is already armed replaces it (latest arm wins).
    #[inline]
    pub fn set_timer(&mut self, token: u64, after: Duration) {
        self.timer_ops.push(TimerOp::Set { token, after });
    }

    /// Cancel an armed timer by `token`. A no-op if no such timer is armed; a
    /// cancelled timer never invokes [`SimNode::on_timer`].
    #[inline]
    pub fn cancel_timer(&mut self, token: u64) {
        self.timer_ops.push(TimerOp::Cancel { token });
    }
}

// ---------------------------------------------------------------------------
// Event queue.
// ---------------------------------------------------------------------------

/// A scheduled simulation event. Ordered FIRST by virtual time, then by the global
/// enqueue sequence number, giving every event a unique total-order key (see the
/// crate-level determinism note).
enum Event<M> {
    /// Deliver `msg` to `to` (sent `from`) at virtual time `at`.
    Deliver {
        at: Monotonic,
        seq: u64,
        from: NodeId,
        to: NodeId,
        msg: M,
    },
    /// Fire timer `token` on `node` at virtual time `at`, unless cancelled first.
    Timer {
        at: Monotonic,
        /// The global total-order tiebreak key (from the shared `next_seq` counter),
        /// unique across BOTH `Deliver` and `Timer` events so `(at, seq)` is a true
        /// total order.
        seq: u64,
        /// The arm-generation (from the separate `next_timer_gen` counter): the
        /// validity tag the dispatch check compares against the live timer table, so
        /// a cancelled or re-armed timer's stale event is recognized and skipped.
        /// Kept SEPARATE from `seq` because the generation is only unique among
        /// timers, not globally; overloading `seq` with it would break key uniqueness.
        arm_gen: u64,
        node: NodeId,
        token: u64,
    },
}

impl<M> Event<M> {
    #[inline]
    fn at(&self) -> Monotonic {
        match self {
            Event::Deliver { at, .. } | Event::Timer { at, .. } => *at,
        }
    }

    #[inline]
    fn seq(&self) -> u64 {
        match self {
            Event::Deliver { seq, .. } | Event::Timer { seq, .. } => *seq,
        }
    }

    /// The total-order key: `(at, seq)`. `seq` is globally unique so the key is too.
    #[inline]
    fn key(&self) -> (Monotonic, u64) {
        (self.at(), self.seq())
    }
}

// The queue is a `BinaryHeap` (a max-heap), so we wrap each event in a small struct
// whose `Ord` is BY KEY and store it under `Reverse` to pop the EARLIEST first. The
// payload (`Event`) is intentionally NOT part of the ordering; the `(at, seq)` key
// is a total order on its own (seq is unique), so the heap order is fully defined
// without comparing `M`, which need not be `Ord`.
struct Queued<M>(Event<M>);

impl<M> PartialEq for Queued<M> {
    fn eq(&self, other: &Self) -> bool {
        self.0.key() == other.0.key()
    }
}
impl<M> Eq for Queued<M> {}
impl<M> PartialOrd for Queued<M> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<M> Ord for Queued<M> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.0.key().cmp(&other.0.key())
    }
}

// ---------------------------------------------------------------------------
// Trace.
// ---------------------------------------------------------------------------

/// The kind of a dispatched event, recorded in the [`TraceRecord`] stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceKind {
    /// A message was delivered to a node's [`SimNode::on_message`].
    Deliver,
    /// A timer fired into a node's [`SimNode::on_timer`].
    Timer,
}

/// A compact record of one dispatched event. The sim appends one per event it
/// actually dispatches (cancelled timers and dropped messages never produce one).
///
/// Two seeded runs of the same scenario produce an EQUAL `Vec<TraceRecord>`; that
/// equality is the single-seed reproducibility assertion (a test compares the two
/// trace vectors directly). Time is recorded as whole virtual milliseconds, which
/// is the granularity the harness schedules at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceRecord {
    /// Whether this was a delivery or a timer firing.
    pub kind: TraceKind,
    /// The virtual time the event dispatched at, in milliseconds since origin.
    pub at_millis: u64,
    /// For a [`TraceKind::Deliver`], the sender; for a [`TraceKind::Timer`], the
    /// node whose timer fired (sender and node coincide for a timer).
    pub from: NodeId,
    /// The node the event was dispatched TO.
    pub to: NodeId,
    /// For a [`TraceKind::Timer`], the timer token; for a delivery, `0`.
    pub token: u64,
}

// ---------------------------------------------------------------------------
// Fault configuration.
// ---------------------------------------------------------------------------

/// The default per-message link latency when no range has been set: a small fixed
/// 1ms hop, so a freshly constructed [`Network`] still advances virtual time on
/// each delivery (a zero-latency default would let an immediate-flood storm at a
/// single instant, which is a less faithful model and harder to reason about).
const DEFAULT_LATENCY: Duration = Duration::from_millis(1);

/// The fault model applied to every outbound send: a network partition, a uniform
/// per-message latency range, and an independent per-message drop probability.
///
/// All three are evaluated by [`Network::step`] using the sim's single seeded RNG,
/// so the faults are part of the reproducible run. A message is dropped if it
/// crosses an active partition boundary OR if the per-message drop roll fires.
struct FaultConfig {
    /// The two sides of an active partition, as id sets. Empty when healed. A
    /// message is blocked iff its `from` and `to` fall on OPPOSITE sides.
    partition: Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)>,
    /// The inclusive latency range each delivered message's hop is drawn from.
    latency_min: Duration,
    latency_max: Duration,
    /// The independent per-message drop probability in `[0.0, 1.0]`.
    drop_prob: f64,
}

impl FaultConfig {
    fn new() -> Self {
        FaultConfig {
            partition: None,
            latency_min: DEFAULT_LATENCY,
            latency_max: DEFAULT_LATENCY,
            drop_prob: 0.0,
        }
    }

    /// Whether a `from -> to` send is blocked by the active partition (opposite
    /// sides). Same-side and unpartitioned-node traffic is never partition-blocked.
    fn partition_blocks(&self, from: NodeId, to: NodeId) -> bool {
        match &self.partition {
            None => false,
            Some((a, b)) => {
                (a.contains(&from) && b.contains(&to)) || (b.contains(&from) && a.contains(&to))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Network.
// ---------------------------------------------------------------------------

/// The simulated cluster: the nodes, the virtual clock and seeded RNG, the event
/// queue, the armed-timer bookkeeping, the fault model, and the dispatch trace.
///
/// Generic over the node type `N: SimNode`; all nodes in one `Network` share a
/// message type (`N::Msg`). Drive it with [`Network::tell`] to inject input and
/// [`Network::step`] / [`Network::run_until_idle`] / [`Network::run_steps`] to
/// advance virtual time. Inject faults with [`Network::partition`] /
/// [`Network::heal`] / [`Network::set_latency`] / [`Network::set_drop_prob`].
pub struct Network<N: SimNode> {
    /// Nodes keyed for DETERMINISTIC iteration (`BTreeMap`, not `HashMap`).
    nodes: BTreeMap<NodeId, N>,
    /// The single determinism seam: virtual clock + seeded RNG for the whole sim.
    env: TestEnv,
    /// The min-heap of pending events, ordered by `(at, seq)` (via `Reverse`).
    queue: BinaryHeap<Reverse<Queued<N::Msg>>>,
    /// The monotonically increasing enqueue counter that makes every event key
    /// unique, and thus the queue order total and history-determined.
    next_seq: u64,
    /// Per-node armed timers: `node -> (token -> arm-generation)`. A `Timer` event
    /// only fires if its generation still matches the latest arm for that token; an
    /// arm bumps the generation, a cancel removes the entry. This gives correct
    /// "reset/cancel" semantics without mutating the heap.
    timers: BTreeMap<NodeId, BTreeMap<u64, u64>>,
    /// The next timer arm-generation to hand out (global, monotonically increasing).
    next_timer_gen: u64,
    /// The active fault model.
    faults: FaultConfig,
    /// The dispatch trace; one record per event actually dispatched.
    trace: Vec<TraceRecord>,
}

impl<N: SimNode> Network<N> {
    /// A new, empty network whose clock starts at the origin and whose RNG is
    /// seeded with `seed`. Same `seed` + same nodes + same script replays
    /// byte-identically.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Network {
            nodes: BTreeMap::new(),
            env: TestEnv::new(seed),
            queue: BinaryHeap::new(),
            next_seq: 0,
            timers: BTreeMap::new(),
            next_timer_gen: 0,
            faults: FaultConfig::new(),
            trace: Vec::new(),
        }
    }

    /// Add `node` under `id`. Adding an id that already exists replaces the node.
    pub fn add_node(&mut self, id: NodeId, node: N) {
        self.nodes.insert(id, node);
    }

    /// Borrow the node stored under `id`, or `None` if no such node is present. A
    /// read-only accessor so a test can inspect a protocol's observable state (e.g.
    /// a Raft node's role/term) after a scenario without exposing the node map.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&N> {
        self.nodes.get(&id)
    }

    /// The dispatch trace so far (one [`TraceRecord`] per dispatched event). Compare
    /// two seeded runs' traces for the reproducibility assertion.
    #[must_use]
    pub fn trace(&self) -> &[TraceRecord] {
        &self.trace
    }

    /// The number of pending (not yet dispatched) events in the queue. Useful as a
    /// quiescence check in tests.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    /// Inject input: enqueue a delivery of `msg` to `to` as if sent `from`, subject
    /// to the SAME fault rules (partition / drop) and latency draw as any node send.
    ///
    /// This is how a test kicks a protocol (a client request, an initial broadcast).
    /// `from` is the apparent sender the node sees in [`SimNode::on_message`]; for a
    /// pure external kick, pass the target's own id (or any sentinel) as `from`. The
    /// delivery is scheduled at `now + latency`, so it is "immediate-ish" rather than
    /// at the exact current instant, keeping it on the same footing as inter-node
    /// traffic. If a partition or a drop roll blocks it, nothing is enqueued.
    pub fn tell(&mut self, to: NodeId, from: NodeId, msg: N::Msg) {
        self.route_send(from, to, msg);
    }

    /// Decide the fate of one `from -> to` send under the fault model and, if it
    /// survives, schedule its delivery at `now + latency`. The RNG draw order here
    /// is fixed and is the ONLY place network nondeterminism is drawn: the drop roll
    /// first (only when a drop probability is set), then the latency draw (only when
    /// the range is non-degenerate). Keeping the draws conditional means a scenario
    /// that sets neither consumes no RNG, so adding/removing a fault does not perturb
    /// an unrelated node's RNG stream.
    fn route_send(&mut self, from: NodeId, to: NodeId, msg: N::Msg) {
        if self.faults.partition_blocks(from, to) {
            return;
        }
        if self.faults.drop_prob > 0.0 {
            let roll = self.env.rng().gen_unit_f64();
            if roll < self.faults.drop_prob {
                return;
            }
        }
        let latency = self.draw_latency();
        let at = self.env.now().saturating_add(latency);
        let seq = self.take_seq();
        self.queue.push(Reverse(Queued(Event::Deliver {
            at,
            seq,
            from,
            to,
            msg,
        })));
    }

    /// Draw a per-message latency uniformly from `[latency_min, latency_max]`. A
    /// degenerate range (min == max, the default) draws NO random number, so the
    /// common no-latency-configured scenario does not consume the RNG stream.
    fn draw_latency(&mut self) -> Duration {
        let min = self.faults.latency_min;
        let max = self.faults.latency_max;
        if max <= min {
            return min;
        }
        // `max > min` is guaranteed by the guard above; `saturating_sub` keeps the
        // arithmetic panic-free without a deterministic-clock concern (it is a value
        // subtraction on configured durations, not a clock read).
        let span = max.saturating_sub(min).as_nanos();
        // span fits in u64 for any realistic configured latency; saturate defensively.
        let span_u64 = u64::try_from(span).unwrap_or(u64::MAX);
        // Inclusive range: gen_below(span+1) lands in [0, span]; saturate the +1.
        let pick = self.env.rng().gen_below(span_u64.saturating_add(1));
        min.saturating_add(Duration::from_nanos(pick))
    }

    #[inline]
    fn take_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    /// Pop and dispatch the single earliest event. Returns `false` when the queue is
    /// empty (nothing dispatched), `true` otherwise.
    ///
    /// The sequence per step is: pop the `(at, seq)`-minimum event; advance the
    /// virtual clock to its time; for a `Timer`, skip it if it has been cancelled or
    /// superseded; build the [`SimCtx`], run the node callback, record a
    /// [`TraceRecord`], then DRAIN the ctx (apply timer ops, then route each send
    /// through the fault model, in send order).
    pub fn step(&mut self) -> bool {
        let Some(Reverse(Queued(event))) = self.queue.pop() else {
            return false;
        };
        // Advance virtual time to the event. The clock only ever moves forward: the
        // queue is a min-heap on time, so each popped event is at or after `now`.
        let target = event.at();
        let delta = target.saturating_duration_since(self.env.now());
        if delta > Duration::ZERO {
            self.env.advance(delta);
        }

        match event {
            Event::Deliver { from, to, msg, .. } => {
                self.dispatch_message(from, to, msg);
            }
            Event::Timer {
                node,
                token,
                arm_gen,
                ..
            } => {
                self.dispatch_timer(node, token, arm_gen);
            }
        }
        true
    }

    /// Run a single delivery into a node, recording the trace and draining effects.
    fn dispatch_message(&mut self, from: NodeId, to: NodeId, msg: N::Msg) {
        // A delivery to an unknown node is silently discarded (the peer is gone).
        if !self.nodes.contains_key(&to) {
            return;
        }
        let now = self.env.now();
        self.trace.push(TraceRecord {
            kind: TraceKind::Deliver,
            at_millis: millis(now),
            from,
            to,
            token: 0,
        });
        // The ctx holds a mutable borrow of `self.env.rng()` for the callback. We
        // scope it to this block and destructure out the collected effects, so the
        // env borrow is released BEFORE `drain_ctx` takes `&mut self` again.
        let (sends, timer_ops) = {
            let mut ctx = SimCtx {
                now,
                sends: Vec::new(),
                timer_ops: Vec::new(),
                rng: self.env.rng(),
            };
            if let Some(node) = self.nodes.get_mut(&to) {
                node.on_message(from, msg, &mut ctx);
            }
            (ctx.sends, ctx.timer_ops)
        };
        self.drain_ctx(to, sends, timer_ops);
    }

    /// Fire a timer into a node IF it is still the current arm for its token.
    fn dispatch_timer(&mut self, node: NodeId, token: u64, arm_gen: u64) {
        // Re-validate against the live timer table: a timer fires only if an entry for
        // (node, token) still exists AND its arm-generation matches this event's. If
        // the live generation differs, this Timer was cancelled or superseded by a
        // later arm (which bumped the generation), so it does not fire.
        let live = self.timers.get(&node).and_then(|m| m.get(&token)).copied();
        if live != Some(arm_gen) {
            return;
        }
        // Consume the arm: a one-shot timer fires once. Remove it so a re-arm starts
        // fresh and a stale duplicate cannot fire.
        if let Some(m) = self.timers.get_mut(&node) {
            m.remove(&token);
        }
        if !self.nodes.contains_key(&node) {
            return;
        }
        let now = self.env.now();
        self.trace.push(TraceRecord {
            kind: TraceKind::Timer,
            at_millis: millis(now),
            from: node,
            to: node,
            token,
        });
        let (sends, timer_ops) = {
            let mut ctx = SimCtx {
                now,
                sends: Vec::new(),
                timer_ops: Vec::new(),
                rng: self.env.rng(),
            };
            if let Some(n) = self.nodes.get_mut(&node) {
                n.on_timer(token, &mut ctx);
            }
            (ctx.sends, ctx.timer_ops)
        };
        self.drain_ctx(node, sends, timer_ops);
    }

    /// Apply a finished step's collected effects: timer ops first (in issue order),
    /// then sends (in issue order, each through the fault model). Timer ops are
    /// applied before sends so a step that both cancels a timer and sends does so in
    /// a defined order; neither can observe the other's network effect anyway.
    fn drain_ctx(&mut self, node: NodeId, sends: Vec<(NodeId, N::Msg)>, timer_ops: Vec<TimerOp>) {
        for op in timer_ops {
            match op {
                TimerOp::Set { token, after } => self.arm_timer(node, token, after),
                TimerOp::Cancel { token } => self.cancel_timer(node, token),
            }
        }
        for (to, msg) in sends {
            self.route_send(node, to, msg);
        }
    }

    /// Arm (or re-arm) a one-shot timer on `node` under `token`, firing `after` the
    /// current virtual time. A fresh arm-generation is stamped into BOTH the live
    /// timer table and the scheduled `Timer` event's `arm_gen` field, so a superseded
    /// or cancelled arm is recognized and skipped at dispatch.
    fn arm_timer(&mut self, node: NodeId, token: u64, after: Duration) {
        let arm_gen = self.next_timer_gen;
        self.next_timer_gen += 1;
        self.timers.entry(node).or_default().insert(token, arm_gen);
        let at = self.env.now().saturating_add(after);
        // Two DISTINCT identifiers, from two counters, each doing one job:
        // - `seq` (from the shared `next_seq`) is the GLOBAL total-order tiebreak,
        //   unique across Deliver and Timer events so the `(at, seq)` heap key is a
        //   true total order (a Timer and a Deliver at the same instant never tie).
        // - `arm_gen` (from `next_timer_gen`) is the validity tag the dispatch check
        //   compares, recognizing a cancelled or re-armed timer's stale event.
        let seq = self.take_seq();
        self.queue.push(Reverse(Queued(Event::Timer {
            at,
            seq,
            arm_gen,
            node,
            token,
        })));
    }

    /// Cancel an armed timer (remove its live entry). The already-scheduled `Timer`
    /// event stays in the heap but will not fire: its generation no longer matches.
    fn cancel_timer(&mut self, node: NodeId, token: u64) {
        if let Some(m) = self.timers.get_mut(&node) {
            m.remove(&token);
        }
    }

    /// Step until the queue is empty or `max_steps` steps have run, whichever comes
    /// first. Returns the number of steps actually run. `max_steps` is a runaway
    /// guard for a protocol that schedules faster than it drains (e.g. a periodic
    /// timer that never stops); a clean scenario empties the queue well under it.
    pub fn run_until_idle(&mut self, max_steps: usize) -> usize {
        let mut ran = 0;
        while ran < max_steps {
            if !self.step() {
                break;
            }
            ran += 1;
        }
        ran
    }

    /// Run exactly `n` steps, or fewer if the queue empties first. Returns the
    /// number of steps actually run.
    pub fn run_steps(&mut self, n: usize) -> usize {
        let mut ran = 0;
        while ran < n {
            if !self.step() {
                break;
            }
            ran += 1;
        }
        ran
    }

    // -- Fault API ----------------------------------------------------------

    /// Partition the cluster into two groups: all messages BETWEEN the groups (both
    /// directions) are dropped until [`Network::heal`]. Traffic within a group, and
    /// to/from any node in neither group, is unaffected. Replaces any prior
    /// partition. The groups need not cover every node.
    pub fn partition(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        let a: BTreeSet<NodeId> = group_a.iter().copied().collect();
        let b: BTreeSet<NodeId> = group_b.iter().copied().collect();
        self.faults.partition = Some((a, b));
    }

    /// Heal any active partition. Messages already dropped while partitioned stay
    /// dropped (they were never enqueued); future sends flow normally.
    pub fn heal(&mut self) {
        self.faults.partition = None;
    }

    /// Set the per-message latency range. Each delivered message's hop is drawn
    /// uniformly from `[min, max]` (inclusive) via the seeded RNG. A degenerate
    /// range (`min == max`, the default 1ms) is a fixed latency and draws no RNG.
    /// If `max < min`, `min` is used for both (the range is clamped to a point).
    pub fn set_latency(&mut self, min: Duration, max: Duration) {
        self.faults.latency_min = min;
        self.faults.latency_max = if max < min { min } else { max };
    }

    /// Set the independent per-message drop probability (clamped to `[0.0, 1.0]`).
    /// Each send rolls `gen_unit_f64()` and is dropped if the roll is `< p`. `0.0`
    /// drops nothing (and draws no RNG); `1.0` drops every cross-node message.
    pub fn set_drop_prob(&mut self, p: f64) {
        self.faults.drop_prob = p.clamp(0.0, 1.0);
    }
}

/// Whole virtual milliseconds since the clock origin, for [`TraceRecord`]. The sim
/// schedules at millisecond-or-finer granularity but the trace records whole ms,
/// which is the comparison granularity the determinism assertions use.
#[inline]
fn millis(at: Monotonic) -> u64 {
    u64::try_from(at.since_origin().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A toy protocol: a reliable flood broadcast. On first sight of a value, a node
    /// records it and forwards it to every peer; a value already seen is ignored
    /// (this is what makes the flood terminate and what makes the partition test
    /// observable). The peer list is each node's view of the cluster.
    struct Flood {
        seen: BTreeSet<u64>,
        peers: Vec<NodeId>,
    }

    impl Flood {
        fn new(peers: Vec<NodeId>) -> Self {
            Flood {
                seen: BTreeSet::new(),
                peers,
            }
        }
    }

    impl SimNode for Flood {
        type Msg = u64;

        fn on_message(&mut self, _from: NodeId, msg: u64, ctx: &mut SimCtx<'_, u64>) {
            if self.seen.insert(msg) {
                // First time we have seen this value: forward to every peer. Peers
                // that already saw it will ignore it, so the flood converges.
                for &peer in &self.peers {
                    ctx.send(peer, msg);
                }
            }
        }

        fn on_timer(&mut self, _token: u64, _ctx: &mut SimCtx<'_, u64>) {
            // The flood protocol uses no timers.
        }
    }

    /// A toy protocol that arms one timer and records the virtual time it fires at.
    struct TimerNode {
        fired_at: Option<Monotonic>,
        fired_token: Option<u64>,
    }

    impl SimNode for TimerNode {
        type Msg = u64;

        fn on_message(&mut self, _from: NodeId, msg: u64, ctx: &mut SimCtx<'_, u64>) {
            // A message of value N arms a timer (token N) for 100ms from now.
            ctx.set_timer(msg, Duration::from_millis(100));
        }

        fn on_timer(&mut self, token: u64, ctx: &mut SimCtx<'_, u64>) {
            self.fired_at = Some(ctx.now());
            self.fired_token = Some(token);
        }
    }

    /// A toy protocol that, on a message, both arms then immediately cancels a timer
    /// (to exercise cancellation), and arms a second timer that should fire.
    struct CancelNode {
        fired: Vec<u64>,
    }

    impl SimNode for CancelNode {
        type Msg = u64;

        fn on_message(&mut self, _from: NodeId, _msg: u64, ctx: &mut SimCtx<'_, u64>) {
            ctx.set_timer(1, Duration::from_millis(50));
            ctx.cancel_timer(1); // token 1 must never fire
            ctx.set_timer(2, Duration::from_millis(50)); // token 2 must fire
        }

        fn on_timer(&mut self, token: u64, _ctx: &mut SimCtx<'_, u64>) {
            self.fired.push(token);
        }
    }

    /// On its first message, both sends to a peer (a `Deliver` at now + the 1ms
    /// default latency) AND arms a 1ms timer (a `Timer` at the SAME instant). The two
    /// events tie on `at`, so their order is decided purely by the globally-unique
    /// `seq`: this is the case the seq/arm-generation split exists to make a true,
    /// replayable total order rather than a `BinaryHeap`-internal accident.
    struct Both {
        peer: NodeId,
        got: Vec<u64>,
        fired: Vec<u64>,
        kicked: bool,
    }

    impl SimNode for Both {
        type Msg = u64;

        fn on_message(&mut self, _from: NodeId, msg: u64, ctx: &mut SimCtx<'_, u64>) {
            self.got.push(msg);
            if !self.kicked {
                self.kicked = true;
                ctx.send(self.peer, msg); // Deliver at now + 1ms default latency
                ctx.set_timer(msg, Duration::from_millis(1)); // Timer at the same now + 1ms
            }
        }

        fn on_timer(&mut self, token: u64, _ctx: &mut SimCtx<'_, u64>) {
            self.fired.push(token);
        }
    }

    const A: NodeId = NodeId(1);
    const B: NodeId = NodeId(2);
    const C: NodeId = NodeId(3);

    fn flood_network(seed: u64) -> Network<Flood> {
        let mut net = Network::new(seed);
        net.add_node(A, Flood::new(vec![B, C]));
        net.add_node(B, Flood::new(vec![A, C]));
        net.add_node(C, Flood::new(vec![A, B]));
        net
    }

    #[test]
    fn determinism_replay() {
        // Two identically seeded, identically scripted runs must dispatch the exact
        // same sequence of events. This is the single-seed reproducibility property.
        let run = |seed: u64| {
            let mut net = flood_network(seed);
            net.tell(A, A, 7); // inject value 7 at node A
            let steps = net.run_until_idle(10_000);
            (steps, net.trace().to_vec())
        };
        let (steps1, trace1) = run(42);
        let (steps2, trace2) = run(42);
        assert_eq!(steps1, steps2, "step counts must match for the same seed");
        assert_eq!(
            trace1, trace2,
            "traces must be byte-identical for the same seed"
        );
        assert!(!trace1.is_empty(), "the flood should dispatch something");
    }

    #[test]
    fn partition_blocks_then_heal_delivers() {
        let mut net = flood_network(7);
        // [A] | [B, C]: A is isolated from B and C.
        net.partition(&[A], &[B, C]);
        net.tell(B, B, 100); // flood value 100 starting at B
        net.run_until_idle(10_000);
        // C is on B's side, so it sees 100; A is partitioned off, so it does not.
        assert!(
            node_seen(&net, C, 100),
            "C must see the value (same side as B)"
        );
        assert!(
            !node_seen(&net, A, 100),
            "A must NOT see it across the partition"
        );
        assert!(
            seen_is_empty(&net, A),
            "A's seen set must be empty while isolated"
        );

        // Heal, then flood a second value: B/C forward to A, which now sees it.
        net.heal();
        net.tell(B, B, 200);
        net.run_until_idle(10_000);
        assert!(
            node_seen(&net, A, 200),
            "A must see the second value after heal"
        );
    }

    #[test]
    fn timer_fires_at_virtual_time() {
        let mut net = Network::new(1);
        net.add_node(
            A,
            TimerNode {
                fired_at: None,
                fired_token: None,
            },
        );
        // tell incurs the default 1ms link latency, so the message arrives at t=1ms;
        // the node then arms a 100ms timer, which fires at t=101ms.
        net.tell(A, A, 5);
        net.run_until_idle(100);
        let node = node_ref::<TimerNode>(&net, A);
        assert_eq!(node.fired_token, Some(5), "timer token 5 should have fired");
        assert_eq!(
            node.fired_at.map(Monotonic::since_origin),
            Some(Duration::from_millis(101)),
            "timer must fire at message-arrival (1ms) + 100ms = 101ms virtual time"
        );
        // No real time was spent: the harness never slept.
    }

    #[test]
    fn cancel_timer_does_not_fire() {
        // A re-arm/cancel must be honored: a cancelled token never fires, a live one
        // does. This pins the timer cancellation semantics documented on SimCtx.
        let mut net = Network::new(1);
        net.add_node(A, CancelNode { fired: Vec::new() });
        net.tell(A, A, 0);
        net.run_until_idle(100);
        let node = node_ref::<CancelNode>(&net, A);
        assert_eq!(
            node.fired,
            vec![2],
            "only the non-cancelled token (2) fires"
        );
    }

    #[test]
    fn drop_prob_zero_delivers_one_drops_all() {
        // drop_prob(0.0): a normal flood, A sees a value forwarded from B.
        let mut net = flood_network(3);
        net.set_latency(Duration::from_millis(1), Duration::from_millis(5));
        net.set_drop_prob(0.0);
        net.tell(B, B, 1);
        net.run_until_idle(10_000);
        assert!(
            node_seen(&net, A, 1),
            "with drop_prob 0.0 the flood reaches A"
        );

        // drop_prob(1.0): every cross-node message is dropped. The kick to B is
        // itself a routed send, so even B never sees the value.
        let mut net = flood_network(3);
        net.set_drop_prob(1.0);
        net.tell(B, B, 1);
        net.run_until_idle(10_000);
        assert!(seen_is_empty(&net, A), "A sees nothing with drop_prob 1.0");
        assert!(
            seen_is_empty(&net, B),
            "B sees nothing with drop_prob 1.0 (its kick is dropped)"
        );
    }

    #[test]
    fn same_instant_deliver_and_timer_tie_is_deterministic() {
        // A Deliver and a Timer scheduled at the SAME virtual instant must order by
        // the globally-unique seq, giving a defined, replayable result. Two seeded
        // runs must dispatch the identical trace, and the trace must actually contain
        // a tie (a Deliver and a Timer sharing an at_millis) so the case is exercised.
        let run = || {
            let mut net: Network<Both> = Network::new(99);
            net.add_node(
                A,
                Both {
                    peer: B,
                    got: Vec::new(),
                    fired: Vec::new(),
                    kicked: false,
                },
            );
            net.add_node(
                B,
                Both {
                    peer: A,
                    got: Vec::new(),
                    fired: Vec::new(),
                    kicked: false,
                },
            );
            net.tell(A, A, 9);
            net.run_until_idle(10_000);
            net.trace().to_vec()
        };
        let t1 = run();
        let t2 = run();
        assert_eq!(t1, t2, "the same-instant tie must replay identically");
        let tie = t1.iter().any(|r| {
            r.kind == TraceKind::Timer
                && t1
                    .iter()
                    .any(|d| d.kind == TraceKind::Deliver && d.at_millis == r.at_millis)
        });
        assert!(
            tie,
            "the scenario must produce a Deliver/Timer tie at one instant"
        );
    }

    // -- test helpers -------------------------------------------------------

    fn node_ref<N: SimNode + 'static>(net: &Network<N>, id: NodeId) -> &N {
        net.nodes.get(&id).expect("node exists")
    }

    fn node_seen(net: &Network<Flood>, id: NodeId, value: u64) -> bool {
        net.nodes.get(&id).is_some_and(|n| n.seen.contains(&value))
    }

    fn seen_is_empty(net: &Network<Flood>, id: NodeId) -> bool {
        net.nodes.get(&id).is_some_and(|n| n.seen.is_empty())
    }
}
