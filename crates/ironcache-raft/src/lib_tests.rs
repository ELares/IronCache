// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use core::time::Duration;
use std::collections::BTreeMap;

use ironcache_sim::NodeId as SimId;
use ironcache_sim::{Network, SimCtx};

// -- id mapping ---------------------------------------------------------
//
// The engine's `NodeId` and the sim's `NodeId` are distinct types (the engine
// is transport-agnostic). The mapping is the identity on the inner `u64`, which
// keeps test reasoning simple and is all an adapter has to commit to.

fn to_sim(id: NodeId) -> SimId {
    SimId(id.0)
}

fn to_raft(id: SimId) -> NodeId {
    NodeId(id.0)
}

// -- RaftRng wrapper over SimCtx ---------------------------------------

/// A [`RaftRng`] that draws from the sim's single seeded RNG via
/// [`SimCtx::gen_below`], so the engine's election jitter is part of the
/// reproducible run. It borrows the `SimCtx` for the duration of one engine
/// call and is dropped before the effects are drained back onto the ctx.
struct SimRng<'a, 'c> {
    ctx: &'a mut SimCtx<'c, RaftMsg>,
}

impl RaftRng for SimRng<'_, '_> {
    fn gen_below(&mut self, bound: u64) -> u64 {
        self.ctx.gen_below(bound)
    }
}

// -- the SimNode adapter -----------------------------------------------

/// Wraps a pure [`RaftNode`] as an [`ironcache_sim::SimNode`].
///
/// Each callback: reads `now` from the ctx; builds a [`SimRng`] borrowing the
/// ctx and runs the engine into a local [`Effects`]; drops the borrow; then
/// drains the effects onto the ctx (timer ops first, then sends, matching the
/// sim's drain order). The initial election timer is armed by [`RaftSimNode`]'s
/// own `start`, invoked by the [`RaftCluster`] builder right after `add_node`.
struct RaftSimNode {
    engine: RaftNode<MemStorage>,
    started: bool,
}

impl RaftSimNode {
    fn new(id: NodeId, voters: BTreeSet<NodeId>, config: RaftConfig) -> Self {
        RaftSimNode {
            engine: RaftNode::new(id, voters, MemStorage::new(), config),
            started: false,
        }
    }

    /// Run the engine's [`RaftNode::start`] exactly once (idempotent), arming
    /// the initial election timer.
    ///
    /// The sim consumes a node in `add_node` and offers only a read accessor, so
    /// the harness cannot reach in and call `start` directly. Instead, the
    /// adapter drives `start` LAZILY on a node's first callback: the
    /// [`RaftCluster`] builder injects one harmless bootstrap delivery per node
    /// (a term-0 self `AppendEntries`, dropped by the engine as same-term noise
    /// but used here purely as the "you are now live, arm your timer" trigger),
    /// and this method runs `start` before that first message is processed. The
    /// engine reads no clock on `start`, so the ctx's `now` is the correct
    /// argument. A re-arm by the bootstrap message itself is harmless (latest
    /// arm wins).
    fn ensure_started(&mut self, ctx: &mut SimCtx<'_, RaftMsg>) {
        if self.started {
            return;
        }
        self.started = true;
        let now = ctx.now();
        let mut effects = Effects::new();
        {
            let mut rng = SimRng { ctx };
            self.engine.start(now, &mut rng, &mut effects);
        }
        drain(ctx, effects);
    }
}

impl ironcache_sim::SimNode for RaftSimNode {
    type Msg = RaftMsg;

    fn on_message(&mut self, from: SimId, msg: RaftMsg, ctx: &mut SimCtx<'_, RaftMsg>) {
        self.ensure_started(ctx);
        let now = ctx.now();
        let mut effects = Effects::new();
        {
            let mut rng = SimRng { ctx };
            self.engine
                .on_message(now, &mut rng, to_raft(from), msg, &mut effects);
        }
        drain(ctx, effects);
    }

    fn on_timer(&mut self, token: u64, ctx: &mut SimCtx<'_, RaftMsg>) {
        self.ensure_started(ctx);
        let now = ctx.now();
        let mut effects = Effects::new();
        {
            let mut rng = SimRng { ctx };
            self.engine.on_timer(now, &mut rng, token, &mut effects);
        }
        drain(ctx, effects);
    }
}

/// Apply a finished step's [`Effects`] onto the sim ctx: timer ops first, then
/// sends, mapping raft ids to sim ids. This mirrors the sim's own drain order.
fn drain(ctx: &mut SimCtx<'_, RaftMsg>, effects: Effects) {
    for op in effects.timer_ops {
        match op {
            TimerOp::Set { token, after } => ctx.set_timer(token, after),
            TimerOp::Cancel { token } => ctx.cancel_timer(token),
        }
    }
    for (to, msg) in effects.sends {
        ctx.send(to_sim(to), msg);
    }
}

// -- cluster builder ----------------------------------------------------

/// A small test harness: builds a [`Network`] of `n` Raft voters (ids 1..=n),
/// arms each one's initial election timer, and exposes role/term reads via the
/// new [`Network::node`] accessor.
struct RaftCluster {
    net: Network<RaftSimNode>,
    ids: Vec<NodeId>,
}

impl RaftCluster {
    /// Build `n` voters (ids `1..=n`) with `config` on a network seeded with
    /// `seed`, then bootstrap each so its initial election timer is armed.
    fn new(n: u64, seed: u64, config: RaftConfig) -> Self {
        let ids: Vec<NodeId> = (1..=n).map(NodeId).collect();
        let voters: BTreeSet<NodeId> = ids.iter().copied().collect();
        let mut net = Network::new(seed);
        for &id in &ids {
            net.add_node(to_sim(id), RaftSimNode::new(id, voters.clone(), config));
        }
        let mut cluster = RaftCluster { net, ids };
        cluster.start_all();
        cluster
    }

    /// Bootstrap every node: inject one harmless self-addressed delivery so each
    /// node's first callback runs (which triggers its one-time
    /// [`RaftNode::start`], arming the initial election timer; see
    /// [`RaftSimNode::ensure_started`]).
    ///
    /// The bootstrap message is a term-0 `AppendEntries` from the node to
    /// itself. On a fresh term-0 follower this is `term == currentTerm`, so the
    /// engine's recognize-leader path re-arms the election timer and changes no
    /// role state; combined with the lazy `start`, the only durable effect is
    /// "the election timer is armed", which is exactly what is wanted. It is
    /// fully deterministic, so the seed sweep's replay assertion holds.
    fn start_all(&mut self) {
        for &id in &self.ids {
            self.net.tell(
                to_sim(id),
                to_sim(id),
                RaftMsg::AppendEntries {
                    term: 0,
                    leader: id,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                },
            );
        }
    }

    fn run_until_idle(&mut self, max_steps: usize) -> usize {
        self.net.run_until_idle(max_steps)
    }

    fn role(&self, id: NodeId) -> Role {
        self.net
            .node(to_sim(id))
            .expect("node exists")
            .engine
            .role()
    }

    fn term(&self, id: NodeId) -> u64 {
        self.net
            .node(to_sim(id))
            .expect("node exists")
            .engine
            .current_term()
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.ids
            .iter()
            .copied()
            .filter(|&id| self.role(id) == Role::Leader)
            .collect()
    }

    // -- 3b log/commit accessors ---------------------------------------

    /// A node's committed index (the 3b watermark; see [`RaftNode::commit_index`]).
    fn commit_index(&self, id: NodeId) -> u64 {
        self.net
            .node(to_sim(id))
            .expect("node exists")
            .engine
            .commit_index()
    }

    /// A node's last-applied watermark.
    fn last_applied(&self, id: NodeId) -> u64 {
        self.net
            .node(to_sim(id))
            .expect("node exists")
            .engine
            .last_applied()
    }

    /// A node's log entries, cloned for inspection (via the storage accessor).
    fn log(&self, id: NodeId) -> Vec<LogEntry> {
        self.net
            .node(to_sim(id))
            .expect("node exists")
            .engine
            .storage()
            .log()
            .to_vec()
    }

    /// Inject a client proposal at `leader` by self-`tell`ing a
    /// [`RaftMsg::Propose`] (delivered through the same deterministic transport
    /// as any message, so it is part of the reproducible run). On a non-leader
    /// it is a no-op (the engine rejects it), which is exactly the redirect
    /// behavior under test.
    fn propose(&mut self, leader: NodeId, payload: EntryPayload) {
        self.net
            .tell(to_sim(leader), to_sim(leader), RaftMsg::Propose { payload });
    }

    // -- HA-3d membership helpers --------------------------------------

    /// Propose a single-server membership change at `leader` THROUGH THE WIRE: a
    /// self-`tell`ed `RaftMsg::Propose` carrying the [`EntryPayload::ConfigChange`], so
    /// the proposal and its replication ride the SAME deterministic transport (and
    /// auto-drain through [`RaftSimNode`]) as any message -- exactly like
    /// [`RaftCluster::propose`]. The engine's `propose` path enforces the leader-only +
    /// one-change-in-flight guards; on a non-leader (or an in-flight change) it is an
    /// inert no-op, which is the refusal under test. (For the engine's RETURN-VALUE
    /// verdict -- accepted index vs refused -- the unit tests call
    /// [`RaftNode::propose_membership_change`] directly; the DST gate observes the
    /// EFFECT on the committed config instead.)
    fn propose_membership(&mut self, leader: NodeId, change: MembershipChange) {
        self.net.tell(
            to_sim(leader),
            to_sim(leader),
            RaftMsg::Propose {
                payload: EntryPayload::ConfigChange(change),
            },
        );
    }

    /// Add a FRESH node `id` mid-run (a joining server, HA-3d), seeded with the given
    /// `voters` argument as its constructor config. A joining node typically starts
    /// with a config that does NOT yet include itself (it learns it is a voter/learner
    /// once the AddVoter/AddLearner entry replicates into its log); this helper lets a
    /// scenario pick that seed. The node is bootstrapped (its first callback arms the
    /// election timer) via the same harmless term-0 self-AppendEntries
    /// [`RaftCluster::start_all`] uses.
    fn add_joining_node(&mut self, id: NodeId, seed_voters: BTreeSet<NodeId>, config: RaftConfig) {
        self.net
            .add_node(to_sim(id), RaftSimNode::new(id, seed_voters, config));
        self.ids.push(id);
        self.ids.sort_unstable();
        self.net.tell(
            to_sim(id),
            to_sim(id),
            RaftMsg::AppendEntries {
                term: 0,
                leader: id,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            },
        );
    }

    /// The current (log-derived) voter set of `id`'s engine (HA-3d inspection).
    fn voters_of(&self, id: NodeId) -> BTreeSet<NodeId> {
        self.net
            .node(to_sim(id))
            .expect("node exists")
            .engine
            .voters()
            .clone()
    }

    /// The current learner set of `id`'s engine (HA-3d inspection).
    fn learners_of(&self, id: NodeId) -> BTreeSet<NodeId> {
        self.net
            .node(to_sim(id))
            .expect("node exists")
            .engine
            .learners()
            .clone()
    }
}

// -- election-safety checker -------------------------------------------

/// Election Safety (Raft section 5.2, the headline invariant): at most one
/// leader can be elected in a given term. We assert it over the OBSERVABLE
/// state: group the current leaders by their `currentTerm`; no term may have
/// two. Run after every scenario (and at quiescent points within).
///
/// Note this is the strongest property a state-snapshot checker can assert
/// without a per-term history; because a node's term is monotonic and a leader
/// holds the term it won, two distinct same-term leaders co-existing at ANY
/// quiescent observation is exactly the split-brain this forbids.
fn assert_election_safety(cluster: &RaftCluster) {
    let mut by_term: BTreeMap<u64, Vec<NodeId>> = BTreeMap::new();
    for &id in &cluster.ids {
        if cluster.role(id) == Role::Leader {
            by_term.entry(cluster.term(id)).or_default().push(id);
        }
    }
    for (term, leaders) in &by_term {
        assert!(
            leaders.len() <= 1,
            "election safety violated: term {term} has leaders {leaders:?}"
        );
    }
}

// -- 3b invariant checkers: Log Matching + State Machine Safety --------

/// The Log Matching Property (Raft section 5.3): "if two logs contain an entry
/// with the same index and term, then the logs are identical in all entries up
/// through that index." We assert it pairwise over every node pair: for each
/// common index, if both logs hold an entry there with the SAME term, then every
/// entry at-or-before that index (term AND payload) must be identical between the
/// two logs. A divergence here means replication corrupted a log; it is the
/// structural invariant that underwrites the up-to-date check and commit safety.
fn assert_log_matching(cluster: &RaftCluster) {
    let logs: Vec<(NodeId, Vec<LogEntry>)> = cluster
        .ids
        .iter()
        .map(|&id| (id, cluster.log(id)))
        .collect();
    for i in 0..logs.len() {
        for j in (i + 1)..logs.len() {
            let (id_a, log_a) = (&logs[i].0, &logs[i].1);
            let (id_b, log_b) = (&logs[j].0, &logs[j].1);
            let common = log_a.len().min(log_b.len());
            for k in 0..common {
                let ea = &log_a[k];
                let eb = &log_b[k];
                // Logs are 1-based and contiguous, so position k is index k+1 in
                // both; assert the index bookkeeping holds before comparing terms.
                let idx = u64::try_from(k + 1).unwrap_or(u64::MAX);
                assert_eq!(ea.index, idx, "node {id_a:?} log index bookkeeping");
                assert_eq!(eb.index, idx, "node {id_b:?} log index bookkeeping");
                if ea.term == eb.term {
                    // Same (index, term): every entry up through k must match
                    // exactly (term and payload) on both logs.
                    for m in 0..=k {
                        assert_eq!(
                            log_a[m],
                            log_b[m],
                            "log matching violated: nodes {id_a:?} and {id_b:?} \
                                 agree at index {idx} (term {}) but differ at index {}",
                            ea.term,
                            m + 1
                        );
                    }
                }
            }
        }
    }
}

/// State Machine Safety (Raft section 5.4.3 / Figure 3): "if a server has
/// applied a log entry at a given index to its state machine, no other server
/// will ever apply a different log entry for the same index." Because 3b's apply
/// is a sink, we assert the equivalent over the COMMITTED prefix: no two nodes
/// hold a DIFFERENT entry at any index that BOTH consider committed (index <=
/// their respective `commit_index`). A committed entry is, by definition, agreed;
/// a divergence in a committed prefix is exactly the data loss 5.4.2 forbids.
///
/// This is a snapshot check; the cross-TIME guarantee (a once-committed entry is
/// never later overwritten) is enforced by [`CommitLedger`], which records every
/// committed entry ever observed and re-checks it on each step.
fn assert_state_machine_safety(cluster: &RaftCluster) {
    for i in 0..cluster.ids.len() {
        for j in (i + 1)..cluster.ids.len() {
            let id_a = cluster.ids[i];
            let id_b = cluster.ids[j];
            let log_a = cluster.log(id_a);
            let log_b = cluster.log(id_b);
            let committed = cluster.commit_index(id_a).min(cluster.commit_index(id_b));
            for idx in 1..=committed {
                let pos = usize::try_from(idx - 1).unwrap_or(usize::MAX);
                let ea = log_a.get(pos);
                let eb = log_b.get(pos);
                // Both nodes claim idx committed, so both MUST have the entry and
                // the entries must be identical (a committed entry is agreed).
                assert_eq!(
                    ea, eb,
                    "state machine safety violated: nodes {id_a:?} and {id_b:?} \
                         both committed index {idx} but hold different entries \
                         ({ea:?} vs {eb:?})"
                );
            }
        }
    }
}

/// A cross-TIME ledger of every committed entry ever observed, to prove the
/// strongest State Machine Safety statement: once an entry is committed at an
/// index, NO node ever holds a different entry at that index again (it is never
/// overwritten or lost). A snapshot check cannot see this; the ledger is sampled
/// on every step chunk and remembers (index -> the committed entry), then asserts
/// every node's current log is consistent with that history.
///
/// This is THE Figure-8 gate's witness: the section-5.4.2 commit rule exists
/// precisely so this ledger never has to overwrite an entry it already recorded.
#[derive(Default)]
struct CommitLedger {
    /// index -> the entry that was observed committed there (the durable truth).
    committed: BTreeMap<u64, LogEntry>,
}

impl CommitLedger {
    fn new() -> Self {
        Self::default()
    }

    /// Sample the cluster: for every node, record each entry at-or-below that
    /// node's `commit_index` as durable, and assert it never contradicts a
    /// previously recorded entry at the same index. Recording from EVERY node is
    /// safe because the committed prefix is, by the algorithm's correctness, the
    /// same on all nodes that have it (snapshot SMS guards the same-step case).
    fn observe_and_check(&mut self, cluster: &RaftCluster) {
        for &id in &cluster.ids {
            let ci = cluster.commit_index(id);
            let log = cluster.log(id);
            for idx in 1..=ci {
                let pos = usize::try_from(idx - 1).unwrap_or(usize::MAX);
                let Some(entry) = log.get(pos) else {
                    panic!("node {id:?} claims commit_index {ci} but lacks index {idx}");
                };
                match self.committed.get(&idx) {
                    Some(prev) => assert_eq!(
                        prev, entry,
                        "state machine safety violated ACROSS TIME: index {idx} was \
                             committed as {prev:?} but node {id:?} now holds {entry:?} \
                             (a committed entry was overwritten - Figure-8 failure)"
                    ),
                    None => {
                        self.committed.insert(idx, entry.clone());
                    }
                }
            }
        }
    }
}

/// Convenience: run all three structural invariants at a quiescent point.
fn assert_3b_invariants(cluster: &RaftCluster) {
    assert_election_safety(cluster);
    assert_log_matching(cluster);
    assert_state_machine_safety(cluster);
}

// -- scenario 1: clean start elects exactly one leader -----------------

#[test]
fn clean_start_elects_one_leader() {
    let mut cluster = RaftCluster::new(3, 1, RaftConfig::default());
    let ran = cluster.run_until_idle(100_000);
    assert!(ran > 0, "the cluster should have done work");
    assert_election_safety(&cluster);
    let leaders = cluster.leaders();
    assert_eq!(
        leaders.len(),
        1,
        "exactly one leader after a clean start, got {leaders:?}"
    );
    // Every node should agree on the term, and it is the leader's term.
    let leader = leaders[0];
    let lterm = cluster.term(leader);
    for &id in &cluster.ids {
        assert_eq!(
            cluster.term(id),
            lterm,
            "node {id:?} term disagrees with leader term {lterm}"
        );
    }
}

// -- scenario 2: no two leaders per term under a forced split vote -----

#[test]
fn no_two_leaders_per_term_under_split_vote() {
    // Across 50 seeds, the jittered election timeouts plus a message-latency
    // range produce many runs where two candidates stand close together and
    // split the vote. Election safety must hold throughout (asserted at every
    // quiescent checkpoint), and the cluster must STILL converge to exactly one
    // leader (the fresh jitter drawn on each RE-arm after a failed round
    // eventually breaks the tie). The base+jitter below give a [150ms, 300ms)
    // timeout window.
    let config = RaftConfig {
        election_timeout_base: Duration::from_millis(150),
        election_timeout_jitter: Duration::from_millis(150),
        heartbeat_interval: Duration::from_millis(50),
        // Compaction off (the default): this is a pure election-safety scenario.
        ..RaftConfig::default()
    };
    for seed in 0..50u64 {
        let mut cluster = RaftCluster::new(5, seed, config);
        cluster
            .net
            .set_latency(Duration::from_millis(1), Duration::from_millis(20));
        // Step in chunks, asserting safety at every quiescent checkpoint.
        for _ in 0..40 {
            cluster.net.run_steps(200);
            assert_election_safety(&cluster);
        }
        cluster.run_until_idle(200_000);
        assert_election_safety(&cluster);
        let leaders = cluster.leaders();
        assert_eq!(
            leaders.len(),
            1,
            "seed {seed}: must converge to one leader, got {leaders:?}"
        );
    }
}

// -- scenario 3: leader isolation, partition, then heal ----------------

/// Run until exactly one leader exists (or `max_rounds` chunks elapse).
fn run_to_single_leader(cluster: &mut RaftCluster, chunk: usize, max_rounds: usize) -> NodeId {
    for _ in 0..max_rounds {
        cluster.net.run_steps(chunk);
        assert_election_safety(cluster);
        let leaders = cluster.leaders();
        if leaders.len() == 1 {
            return leaders[0];
        }
    }
    let leaders = cluster.leaders();
    panic!("did not converge to a single leader; leaders = {leaders:?}");
}

#[test]
fn leader_isolation_partition_then_heal() {
    let config = RaftConfig::default();
    let mut cluster = RaftCluster::new(5, 7, config);
    let old_leader = run_to_single_leader(&mut cluster, 500, 200);
    let old_term = cluster.term(old_leader);
    assert_election_safety(&cluster);

    // Isolate the leader from the other four. The majority side (four nodes)
    // must elect a NEW leader at a HIGHER term; the isolated old leader cannot
    // get votes and cannot stay authoritative.
    let others: Vec<SimId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != old_leader)
        .map(to_sim)
        .collect();
    cluster.net.partition(&[to_sim(old_leader)], &others);

    // Let the majority side run an election. Assert safety throughout.
    let mut new_leader = None;
    for _ in 0..400 {
        cluster.net.run_steps(500);
        assert_election_safety(&cluster);
        let majority_leaders: Vec<NodeId> = cluster
            .leaders()
            .into_iter()
            .filter(|&id| id != old_leader)
            .collect();
        if majority_leaders.len() == 1 {
            new_leader = Some(majority_leaders[0]);
            break;
        }
    }
    let new_leader = new_leader.expect("majority side must elect a new leader");
    assert!(
        cluster.term(new_leader) > old_term,
        "new leader term {} must exceed old term {old_term}",
        cluster.term(new_leader)
    );
    assert_election_safety(&cluster);

    // Heal. The old leader, on hearing the higher term, steps down to Follower;
    // the cluster converges to exactly one leader.
    cluster.net.heal();
    for _ in 0..400 {
        cluster.net.run_steps(500);
        assert_election_safety(&cluster);
        if cluster.role(old_leader) == Role::Follower && cluster.leaders().len() == 1 {
            break;
        }
    }
    assert_election_safety(&cluster);
    assert_eq!(
        cluster.role(old_leader),
        Role::Follower,
        "the isolated old leader must step down after heal"
    );
    let leaders = cluster.leaders();
    assert_eq!(
        leaders.len(),
        1,
        "the cluster must converge to one leader after heal, got {leaders:?}"
    );
}

// -- PROD-9 scenario A: a partitioned-then-rejoining node does NOT disrupt ----

/// PROD-9 (pre-vote + stickiness): a node partitioned AWAY from a stable cluster and
/// then REJOINED must NOT DEPOSE the standing leader -- the leader must keep its
/// leadership and its TERM across the whole partition+heal, and on rejoin the victim must
/// return as a follower with no election churn. WITHOUT pre-vote the isolated node would
/// inflate its term on EVERY timeout and on rejoin force a needless election (term jump +
/// leader churn). WITH pre-vote it can never pass its own pre-vote while isolated (no
/// quorum reachable), so it stays quiet -- EXCEPT for the etcd #8525 mixed-version
/// fallback: after PRE_VOTE_FALLBACK_ROUNDS ungranted rounds it falls back to a REAL
/// election once, so a still-isolated victim DOES term-bump at a BOUNDED slow rate (this
/// is the deliberate liveness trade-off -- a genuinely stuck subset must not lock out
/// forever). The victim still cannot WIN (no quorum), so it never disrupts the leader, and
/// on heal it rejoins cleanly. We pin BOTH halves: the victim's local term may climb, but
/// the LEADER's term and leadership are untouched and the cluster re-converges to it.
/// Swept over seeds.
#[test]
fn partitioned_rejoining_node_does_not_disrupt_stable_leader() {
    for seed in 0..40u64 {
        let mut cluster = RaftCluster::new(5, seed, RaftConfig::default());
        let leader = run_to_single_leader(&mut cluster, 500, 200);
        let leader_term = cluster.term(leader);
        assert_election_safety(&cluster);

        // Isolate ONE follower (not the leader) from everyone else. The remaining four
        // are still a majority, so the leader keeps quorum-contact and stays leader.
        let victim = *cluster
            .ids
            .iter()
            .find(|&&id| id != leader)
            .expect("a non-leader follower exists");
        let rest: Vec<SimId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != victim)
            .map(to_sim)
            .collect();
        cluster.net.partition(&[to_sim(victim)], &rest);

        // Let the isolated node sit through MANY election timeouts. Its pre-vote rounds
        // always fail (no reachable quorum); the fallback may term-bump it at a bounded
        // rate, but it can NEVER win and so never disturbs the standing leader's majority.
        for _ in 0..30 {
            cluster.net.run_steps(500);
            assert_election_safety(&cluster);
            assert_ne!(
                cluster.role(victim),
                Role::Leader,
                "seed {seed}: the isolated minority node can NEVER win (no reachable quorum)"
            );
        }

        // The standing leader is UNDISTURBED: it keeps leadership at its ORIGINAL term.
        // The victim's local term may have climbed (the bounded fallback), but it could not
        // depose the leader -- the disruption-free property the fix preserves.
        assert_eq!(
            cluster.role(leader),
            Role::Leader,
            "seed {seed}: the leader must remain leader while a minority node is isolated"
        );
        assert_eq!(
            cluster.term(leader),
            leader_term,
            "seed {seed}: the leader's term is NOT inflated while it holds quorum (no disruption)"
        );

        // Heal: the rejoining node rejoins as a follower with no election churn; the
        // standing leader is still leader at its ORIGINAL term (no disruption on rejoin).
        // The victim may carry a higher local term from the fallback, but the leader's
        // current-term heartbeat (or a real re-election it wins) brings it back as a
        // follower under the SAME leader -- the cluster re-converges with one leader.
        cluster.net.heal();
        for _ in 0..60 {
            cluster.net.run_steps(500);
            assert_election_safety(&cluster);
            if cluster.role(victim) == Role::Follower && cluster.leaders() == vec![leader] {
                break;
            }
        }
        assert_eq!(
            cluster.leaders(),
            vec![leader],
            "seed {seed}: after heal the SAME leader still leads (no disruptive election)"
        );
        assert_eq!(
            cluster.role(victim),
            Role::Follower,
            "seed {seed}: the rejoined node settles back to a follower under the same leader"
        );
    }
}

// -- PROD-9 scenario B: a partitioned leader STEPS DOWN (check-quorum) ---------

/// PROD-9 (check-quorum): a leader partitioned AWAY from a majority of voters must STEP
/// DOWN within roughly an election timeout, rather than indefinitely believing it is
/// leader (and serving stale leader-only operations). We isolate the elected leader from
/// the other four (a minority of one) and assert it RELINQUISHES leadership without ever
/// hearing a higher term -- the check-quorum self-demotion, not the higher-term step-down.
#[test]
fn partitioned_leader_steps_down_on_quorum_loss() {
    for seed in 0..40u64 {
        let mut cluster = RaftCluster::new(5, seed, RaftConfig::default());
        let leader = run_to_single_leader(&mut cluster, 500, 200);
        assert_election_safety(&cluster);

        // Isolate the leader from the other four. It can no longer reach a quorum, so
        // check-quorum must depose it. We do NOT heal, and the majority side WILL elect a
        // new leader at a higher term, but the OLD leader's step-down here is driven by
        // check-quorum (no higher-term message reaches it across the partition).
        let others: Vec<SimId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != leader)
            .map(to_sim)
            .collect();
        cluster.net.partition(&[to_sim(leader)], &others);

        // Run long enough for several election timeouts to elapse on the isolated leader.
        let mut stepped_down = false;
        for _ in 0..60 {
            cluster.net.run_steps(500);
            assert_election_safety(&cluster);
            if cluster.role(leader) != Role::Leader {
                stepped_down = true;
                break;
            }
        }
        assert!(
            stepped_down,
            "seed {seed}: a leader that lost quorum-contact must step down (check-quorum)"
        );
        // The deposed leader is no longer Leader. Once down it becomes a Follower and then
        // campaigns again (pre-vote, and after the etcd #8525 fallback a real election),
        // so it may be either Follower or Candidate by now -- the point is it RELINQUISHED
        // leadership and, being partitioned from the majority, can NEVER re-win. We pin the
        // relinquish + cannot-re-win invariant; the exact post-demotion role is incidental.
        assert_ne!(
            cluster.role(leader),
            Role::Leader,
            "seed {seed}: the deposed leader has relinquished leadership"
        );

        // Drive it through MANY more timeouts: a partitioned ex-leader keeps trying (and
        // the fallback bumps its term at a bounded rate) but it must NEVER become leader
        // again -- no quorum is reachable, so election safety holds throughout.
        for _ in 0..30 {
            cluster.net.run_steps(500);
            assert_election_safety(&cluster);
            assert_ne!(
                cluster.role(leader),
                Role::Leader,
                "seed {seed}: a partitioned ex-leader can never re-win (no reachable quorum)"
            );
        }
    }
}

/// PROD-9 (check-quorum, single-message): a partitioned leader steps down on its OWN
/// heartbeat tick once the contact window lapses, with NO inbound message at all. This
/// isolates the check-quorum mechanism from any higher-term step-down: we drive a
/// leader's `on_heartbeat_timer` past the contact window and assert the self-demotion.
#[test]
fn leader_steps_down_on_heartbeat_when_quorum_contact_is_stale() {
    // A 3-voter leader at term 1. Make it leader by hand-seating the won state at t=0.
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    let mut storage = MemStorage::new();
    storage.set_current_term(1);
    let mut node = RaftNode::new(NodeId(1), voters, storage, RaftConfig::default());
    let mut rng = ZeroRng;
    node.role = Role::Candidate;
    node.votes.clear();
    node.votes.insert(NodeId(1));
    node.votes.insert(NodeId(2));
    // Win at t=0; this seeds quorum-contact for every voter at t=0.
    let mut won = Effects::new();
    node.maybe_become_leader(Monotonic::ZERO, &mut won);
    assert_eq!(node.role(), Role::Leader, "node won the election");

    // A heartbeat WITHIN the contact window: still leader (a quorum is fresh from the win).
    let within = Monotonic::from_since_origin(Duration::from_millis(10));
    let mut e_in = Effects::new();
    node.on_heartbeat_timer(within, &mut rng, &mut e_in);
    assert_eq!(
        node.role(),
        Role::Leader,
        "a heartbeat inside the contact window keeps leadership"
    );

    // A heartbeat AFTER the contact window with no peer having acked since the win: the
    // leader has not heard from a quorum in an election timeout -> it steps down.
    let after = Monotonic::from_since_origin(
        RaftConfig::default().election_timeout_base + Duration::from_millis(1),
    );
    let mut e_out = Effects::new();
    node.on_heartbeat_timer(after, &mut rng, &mut e_out);
    assert_eq!(
        node.role(),
        Role::Follower,
        "check-quorum: a leader with stale quorum-contact steps down on its heartbeat tick"
    );
    // The step-down did NOT bump the term (it is a voluntary demotion, not a term race).
    assert_eq!(
        node.current_term(),
        1,
        "check-quorum step-down does not inflate the term"
    );
}

// -- PROD-9 scenario C: liveness still holds (a real leader loss elects) -------

/// PROD-9 liveness: a genuine leader loss still elects a NEW leader promptly. Pre-vote +
/// check-quorum must not WEDGE elections: when the real leader is removed from contact,
/// the surviving majority's pre-vote rounds succeed (they cannot hear the dead leader, so
/// stickiness does not block them) and one of them wins. Swept over seeds; the existing
/// `leader_isolation_partition_then_heal` covers the heal half, this pins the elect half.
#[test]
fn liveness_new_leader_elected_after_leader_loss_under_pre_vote() {
    for seed in 0..40u64 {
        let mut cluster = RaftCluster::new(5, seed, RaftConfig::default());
        let leader = run_to_single_leader(&mut cluster, 500, 200);
        let old_term = cluster.term(leader);

        // Isolate the leader: the majority of four must elect a NEW leader (pre-vote does
        // not block them -- they have no fresh leader contact across the partition).
        let others: Vec<SimId> = cluster
            .ids
            .iter()
            .copied()
            .filter(|&id| id != leader)
            .map(to_sim)
            .collect();
        cluster.net.partition(&[to_sim(leader)], &others);

        let mut new_leader = None;
        for _ in 0..120 {
            cluster.net.run_steps(500);
            assert_election_safety(&cluster);
            let majority_leaders: Vec<NodeId> = cluster
                .leaders()
                .into_iter()
                .filter(|&id| id != leader)
                .collect();
            if majority_leaders.len() == 1 {
                new_leader = Some(majority_leaders[0]);
                break;
            }
        }
        let new_leader =
            new_leader.unwrap_or_else(|| panic!("seed {seed}: majority must elect a new leader"));
        assert!(
            cluster.term(new_leader) > old_term,
            "seed {seed}: the new leader's term {} must exceed the old {old_term}",
            cluster.term(new_leader)
        );
        assert_election_safety(&cluster);
    }
}

// -- scenario 4: single-voter cluster self-elects ----------------------

#[test]
fn single_voter_self_elects() {
    let mut cluster = RaftCluster::new(1, 3, RaftConfig::default());
    cluster.run_until_idle(10_000);
    assert_election_safety(&cluster);
    let only = cluster.ids[0];
    assert_eq!(
        cluster.role(only),
        Role::Leader,
        "a single-voter cluster must self-elect immediately"
    );
    assert_eq!(cluster.leaders(), vec![only]);
}

// -- scenario 5: determinism + safety over a seed sweep ----------------

/// Replay scenario 1 (clean start) for `seed`, returning the final trace and
/// the elected leader so two runs can be compared byte-for-byte.
fn replay_clean_start(seed: u64) -> (Vec<ironcache_sim::TraceRecord>, Vec<NodeId>) {
    let mut cluster = RaftCluster::new(3, seed, RaftConfig::default());
    cluster.run_until_idle(200_000);
    assert_election_safety(&cluster);
    (cluster.net.trace().to_vec(), cluster.leaders())
}

/// Replay scenario 3 (partition then heal) for `seed`, returning the final
/// trace. The fault script is fixed (partition the FIRST elected leader), so
/// two same-seed runs are identical.
fn replay_partition(seed: u64) -> Vec<ironcache_sim::TraceRecord> {
    let config = RaftConfig::default();
    let mut cluster = RaftCluster::new(5, seed, config);
    let old_leader = run_to_single_leader(&mut cluster, 500, 200);
    let others: Vec<SimId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != old_leader)
        .map(to_sim)
        .collect();
    cluster.net.partition(&[to_sim(old_leader)], &others);
    cluster.net.run_steps(50_000);
    assert_election_safety(&cluster);
    cluster.net.heal();
    cluster.net.run_steps(50_000);
    assert_election_safety(&cluster);
    cluster.net.trace().to_vec()
}

#[test]
fn determinism_and_safety_seed_sweep() {
    for seed in 0..200u64 {
        // Scenario 1 across the sweep: each run elects exactly one leader, is
        // election-safe, and replays byte-identically.
        let (trace_a, leaders_a) = replay_clean_start(seed);
        let (trace_b, leaders_b) = replay_clean_start(seed);
        assert_eq!(
            leaders_a.len(),
            1,
            "seed {seed}: clean start must elect exactly one leader"
        );
        assert_eq!(
            trace_a, trace_b,
            "seed {seed}: clean-start trace must replay byte-identically"
        );
        assert_eq!(leaders_a, leaders_b, "seed {seed}: same leader on replay");

        // Scenario 3 across the sweep: same-seed replay is byte-identical and
        // election-safe (asserted inside replay_partition).
        let p_a = replay_partition(seed);
        let p_b = replay_partition(seed);
        assert_eq!(
            p_a, p_b,
            "seed {seed}: partition-then-heal trace must replay byte-identically"
        );
    }
}

// -- engine-direct safety unit tests -----------------------------------
//
// These drive the pure engine (no sim) to pin the exact vote-grant rules that
// the integration scenarios only observe at the leader-count granularity.

/// A deterministic [`RaftRng`] for engine-direct tests where the election
/// jitter value is irrelevant (always 0).
struct ZeroRng;
impl RaftRng for ZeroRng {
    fn gen_below(&mut self, _bound: u64) -> u64 {
        0
    }
}

/// Whether `effects` contains a granted `RequestVoteResp` addressed to
/// `candidate`.
fn reply_granted(effects: &Effects, candidate: NodeId) -> bool {
    effects.sends.iter().any(|(to, msg)| {
        *to == candidate
            && matches!(
                msg,
                RaftMsg::RequestVoteResp {
                    vote_granted: true,
                    ..
                }
            )
    })
}

/// Whether `effects` contains a `PreVoteResp` addressed to `candidate` with the given
/// grant polarity (PROD-9). A pre-vote reply is always sent (grant or deny), so this
/// asserts the polarity rather than mere presence.
fn pre_vote_reply(effects: &Effects, candidate: NodeId, granted: bool) -> bool {
    effects.sends.iter().any(|(to, msg)| {
        *to == candidate
            && matches!(
                msg,
                RaftMsg::PreVoteResp { vote_granted, .. } if *vote_granted == granted
            )
    })
}

/// Count the `PreVote` solicitations in `effects` (PROD-9), to assert a pre-vote round
/// fans out to peers WITHOUT any term-bumping `RequestVote` having been emitted.
fn count_pre_votes(effects: &Effects) -> usize {
    effects
        .sends
        .iter()
        .filter(|(_, msg)| matches!(msg, RaftMsg::PreVote { .. }))
        .count()
}

/// Count the (real, term-bumping) `RequestVote` messages in `effects` (PROD-9).
fn count_request_votes(effects: &Effects) -> usize {
    effects
        .sends
        .iter()
        .filter(|(_, msg)| matches!(msg, RaftMsg::RequestVote { .. }))
        .count()
}

#[test]
fn pre_vote_grants_only_when_log_up_to_date_and_no_fresh_leader() {
    // PROD-9 pre-vote receiver logic (Ongaro section 9.6): a peer grants a pre-vote IFF
    // the pre-candidate's log is at least as up-to-date AND no current leader is fresh,
    // and NEVER mutates persistent term / vote state (the non-binding poll property).
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    let mut storage = MemStorage::new();
    storage.set_current_term(5);
    storage.append(LogEntry {
        term: 5,
        index: 1,
        payload: EntryPayload::Noop,
    });
    let mut node = RaftNode::new(NodeId(1), voters, storage, RaftConfig::default());

    // (1) Up-to-date pre-candidate (term 6 hypothetical, log >= ours), no fresh leader:
    // GRANT, and NO persistent state changes.
    let mut e1 = Effects::new();
    node.on_pre_vote(Monotonic::ZERO, 6, NodeId(2), 1, 5, &mut e1);
    assert!(
        pre_vote_reply(&e1, NodeId(2), true),
        "up-to-date pre-vote is granted"
    );
    assert_eq!(
        node.current_term(),
        5,
        "a pre-vote NEVER adopts the hypothetical term"
    );
    assert_eq!(
        node.storage().voted_for(),
        None,
        "a pre-vote NEVER records a vote"
    );

    // (2) A STALE-log pre-candidate (empty log) is DENIED on the up-to-date check.
    let mut e2 = Effects::new();
    node.on_pre_vote(Monotonic::ZERO, 6, NodeId(3), 0, 0, &mut e2);
    assert!(
        pre_vote_reply(&e2, NodeId(3), false),
        "a stale-log pre-vote is denied"
    );

    // (3) With a FRESH leader, even an up-to-date pre-candidate is DENIED (stickiness).
    let mut rng = ZeroRng;
    let mut warm = Effects::new();
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        5,
        NodeId(3),
        1,
        5,
        Vec::new(),
        1,
        &mut warm,
    );
    let soon = Monotonic::from_since_origin(Duration::from_millis(5));
    let mut e3 = Effects::new();
    node.on_pre_vote(soon, 6, NodeId(2), 1, 5, &mut e3);
    assert!(
        pre_vote_reply(&e3, NodeId(2), false),
        "stickiness: an up-to-date pre-vote is denied while a leader is fresh"
    );

    // (4) Once the leader goes stale, the same up-to-date pre-vote is granted again.
    let later = Monotonic::from_since_origin(
        RaftConfig::default().election_timeout_base + Duration::from_millis(1),
    );
    let mut e4 = Effects::new();
    node.on_pre_vote(later, 6, NodeId(2), 1, 5, &mut e4);
    assert!(
        pre_vote_reply(&e4, NodeId(2), true),
        "after the leader goes stale, the up-to-date pre-vote is granted"
    );
}

#[test]
fn election_timeout_runs_pre_vote_first_and_does_not_bump_term() {
    // PROD-9: with pre-vote ON, an election timeout starts a PRE-VOTE round (fans out
    // PreVote, NOT RequestVote) and does NOT increment the term. The term only advances
    // once a quorum of pre-votes converts the node to a real candidate.
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    let mut node = RaftNode::new(NodeId(1), voters, MemStorage::new(), RaftConfig::default());
    let mut rng = ZeroRng;
    let mut out = Effects::new();
    node.on_election_timeout(Monotonic::ZERO, &mut rng, &mut out);
    assert_eq!(node.current_term(), 0, "pre-vote does NOT bump the term");
    assert_eq!(
        node.role(),
        Role::Follower,
        "a pre-candidate stays a Follower"
    );
    assert_eq!(count_pre_votes(&out), 2, "PreVote is sent to both peers");
    assert_eq!(
        count_request_votes(&out),
        0,
        "no real RequestVote yet (no quorum)"
    );

    // A quorum of pre-vote grants (one peer plus self) converts to a real candidate: NOW
    // the term bumps and RequestVotes go out.
    let mut out2 = Effects::new();
    let pre_term = node.pre_vote_term;
    node.on_pre_vote_resp(Monotonic::ZERO, NodeId(2), pre_term, true, &mut out2);
    assert_eq!(
        node.current_term(),
        1,
        "the pre-vote quorum converts to a real election"
    );
    assert_eq!(node.role(), Role::Candidate, "now a real candidate");
    assert_eq!(
        count_request_votes(&out2),
        2,
        "real RequestVotes fan out on conversion"
    );
}

#[test]
fn a_voter_grants_at_most_one_candidate_per_term() {
    // The double-vote guard (Figure 2 RequestVote rule 2): votedFor in
    // {None, candidate}. A voter that granted candidate A in a term must refuse
    // a DIFFERENT candidate B in that same term, but may idempotently re-grant A.
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    let mut node = RaftNode::new(NodeId(2), voters, MemStorage::new(), RaftConfig::default());
    let mut rng = ZeroRng;

    let mut e1 = Effects::new();
    node.on_request_vote(Monotonic::ZERO, &mut rng, 5, NodeId(1), 0, 0, &mut e1);
    assert!(
        reply_granted(&e1, NodeId(1)),
        "first candidate in the term is granted"
    );
    assert_eq!(node.current_term(), 5);

    let mut e2 = Effects::new();
    node.on_request_vote(Monotonic::ZERO, &mut rng, 5, NodeId(3), 0, 0, &mut e2);
    assert!(
        !reply_granted(&e2, NodeId(3)),
        "a second distinct candidate in the same term must be refused"
    );

    let mut e3 = Effects::new();
    node.on_request_vote(Monotonic::ZERO, &mut rng, 5, NodeId(1), 0, 0, &mut e3);
    assert!(
        reply_granted(&e3, NodeId(1)),
        "the SAME candidate may be re-granted (idempotent)"
    );
}

#[test]
fn up_to_date_check_is_term_then_index() {
    // Section 5.4.1: a candidate log is at least as up-to-date iff its last term
    // is higher, or the last term is equal and its index is >= ours.
    let voters: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
    let mut storage = MemStorage::new();
    storage.append(LogEntry {
        term: 1,
        index: 1,
        payload: EntryPayload::Noop,
    });
    storage.append(LogEntry {
        term: 2,
        index: 2,
        payload: EntryPayload::Noop,
    });
    storage.append(LogEntry {
        term: 2,
        index: 3,
        payload: EntryPayload::Noop,
    });
    let node = RaftNode::new(NodeId(1), voters, storage, RaftConfig::default());
    // Our last entry is (index 3, term 2).
    assert!(
        !node.candidate_log_up_to_date(99, 1),
        "a lower last term is stale even with a longer index"
    );
    assert!(
        !node.candidate_log_up_to_date(2, 2),
        "same term, shorter index is stale"
    );
    assert!(
        node.candidate_log_up_to_date(3, 2),
        "same term, equal index is up-to-date"
    );
    assert!(
        node.candidate_log_up_to_date(4, 2),
        "same term, longer index is up-to-date"
    );
    assert!(
        node.candidate_log_up_to_date(1, 3),
        "a higher last term is up-to-date even with a shorter index"
    );
}

/// Build a single follower (id 1) in a 3-voter cluster at term 5 with one log entry,
/// using `config`. The helper isolates the RequestVote-handling unit under both the
/// legacy (pre-vote OFF) and hardened (pre-vote ON) configs.
fn disruptor_target(config: RaftConfig) -> RaftNode<MemStorage> {
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    let mut storage = MemStorage::new();
    storage.set_current_term(5);
    // A non-empty log: strictly more up-to-date than the disruptor's empty log.
    storage.append(LogEntry {
        term: 5,
        index: 1,
        payload: EntryPayload::Noop,
    });
    RaftNode::new(NodeId(1), voters, storage, config)
}

#[test]
fn disruptive_stale_node_churns_term_only_with_pre_vote_off() {
    // LEGACY behaviour (pre-vote OFF), the original regression anchor: a higher-term
    // RequestVote from a node whose log is too stale to win still forces the recipient
    // to ADOPT the higher term and step down (the churn mechanism), yet the vote is
    // REFUSED so the disruptor never actually wins. Election Safety is always preserved;
    // only liveness degrades. We pin this so the legacy revert path stays exercised.
    let config = RaftConfig {
        pre_vote: false,
        ..RaftConfig::default()
    };
    let mut node = disruptor_target(config);
    let mut rng = ZeroRng;
    let mut eff = Effects::new();
    // Disruptor (node 2) at a HIGHER term 9 with a STALE (empty) log.
    node.on_request_vote(Monotonic::ZERO, &mut rng, 9, NodeId(2), 0, 0, &mut eff);
    assert_eq!(
        node.current_term(),
        9,
        "pre-vote off: the higher term is adopted (the legacy churn mechanism)"
    );
    assert_eq!(
        node.role(),
        Role::Follower,
        "the recipient steps down to follower"
    );
    assert!(
        !reply_granted(&eff, NodeId(2)),
        "the stale-log disruptor is refused the vote, so it cannot win"
    );
}

#[test]
fn disruptive_stale_node_with_fresh_leader_does_not_churn_term_under_stickiness() {
    // HARDENED behaviour (pre-vote / stickiness ON, the default, Ongaro section 9.6):
    // a higher-term RequestVote arriving while a VALID CURRENT LEADER is fresh is
    // REFUSED WITHOUT adopting the disruptor's inflated term -- the leader-stickiness
    // disruptive-server mitigation. So a flapping node cannot depose a healthy leader.
    let mut node = disruptor_target(RaftConfig::default());
    let mut rng = ZeroRng;
    // First, hear from the current-term (5) leader (node 3) so a leader is fresh at t=0.
    let mut warm = Effects::new();
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        5,
        NodeId(3),
        1,
        5,
        Vec::new(),
        1,
        &mut warm,
    );
    assert_eq!(
        node.current_term(),
        5,
        "still term 5 after the leader heartbeat"
    );

    // Disruptor (node 2) at a HIGHER term 9 with a STALE (empty) log, arriving WITHIN the
    // minimum election timeout of the leader contact (t still ~0).
    let now = Monotonic::from_since_origin(Duration::from_millis(10));
    let mut eff = Effects::new();
    node.on_request_vote(now, &mut rng, 9, NodeId(2), 0, 0, &mut eff);
    assert_eq!(
        node.current_term(),
        5,
        "stickiness: the inflated term is NOT adopted while a leader is fresh"
    );
    assert_eq!(
        node.role(),
        Role::Follower,
        "the recipient stays a follower (no churn)"
    );
    assert!(
        !reply_granted(&eff, NodeId(2)),
        "the disruptor is refused the vote"
    );

    // Once the leader goes stale (past the minimum election timeout) the same disruptor
    // RequestVote is processed normally again (term adopted) -- stickiness is a freshness
    // window, not a permanent block, so a genuinely dead leader does not wedge elections.
    let later = Monotonic::from_since_origin(
        RaftConfig::default().election_timeout_base + Duration::from_millis(1),
    );
    let mut eff2 = Effects::new();
    node.on_request_vote(later, &mut rng, 9, NodeId(2), 0, 0, &mut eff2);
    assert_eq!(
        node.current_term(),
        9,
        "after the leader goes stale, a higher-term vote is processed normally"
    );
}

#[test]
fn engaged_ungranted_pre_vote_rounds_fall_back_to_a_real_election() {
    // PROD-9 follow-up (etcd #8525 mixed-version safety net): a node whose peers ARE
    // reachable and answering `PreVote` but never form a GRANT-quorum -- the mixed-version
    // migration deadlock, e.g. pre-vote-aware peers that REJECT while a quorum of grants
    // stays unreachable -- must NOT pre-vote forever. After PRE_VOTE_FALLBACK_ROUNDS
    // consecutive ENGAGED-but-ungranted rounds it falls back ONCE to a real, term-bumping
    // election so the otherwise-locked-out subset can still make progress.
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    let mut node = RaftNode::new(NodeId(1), voters, MemStorage::new(), RaftConfig::default());
    let mut rng = ZeroRng;

    // Each timeout starts a pre-vote round (PreVote fans out, term stays 0). A peer REPLIES
    // each round but REJECTS (vote_granted = false): the round is ENGAGED (a peer is
    // reachable) yet never reaches quorum. The first PRE_VOTE_FALLBACK_ROUNDS such rounds
    // must all stay pre-vote rounds (no term bump), accumulating the fallback counter.
    for round in 0..PRE_VOTE_FALLBACK_ROUNDS {
        let mut out = Effects::new();
        node.on_election_timeout(Monotonic::ZERO, &mut rng, &mut out);
        assert_eq!(
            node.current_term(),
            0,
            "round {round}: an ungranted pre-vote round must NOT bump the term"
        );
        assert_eq!(
            node.role(),
            Role::Follower,
            "round {round}: a pre-candidate stays a Follower"
        );
        assert_eq!(
            count_pre_votes(&out),
            2,
            "round {round}: it is still pre-voting (PreVote fans out to both peers)"
        );
        assert_eq!(
            count_request_votes(&out),
            0,
            "round {round}: no real RequestVote while still under the fallback threshold"
        );
        // A reachable peer answers this round but REJECTS the pre-vote: engagement without
        // a grant. This is what distinguishes the mixed-version deadlock from a partition.
        let pre_term = node.pre_vote_term;
        node.on_pre_vote_resp(
            Monotonic::ZERO,
            NodeId(2),
            pre_term,
            false,
            &mut Effects::new(),
        );
    }

    // The NEXT timeout is the fallback: a REAL election. The term bumps to 1, the node
    // becomes a real Candidate, and RequestVotes fan out -- progress at last, WITHOUT any
    // pre-vote quorum ever having been reached. This is the lockout the fix closes.
    let mut out = Effects::new();
    node.on_election_timeout(Monotonic::ZERO, &mut rng, &mut out);
    assert_eq!(
        node.current_term(),
        1,
        "after the fallback threshold, the node campaigns for REAL (term bumps)"
    );
    assert_eq!(
        node.role(),
        Role::Candidate,
        "the fallback converts to a real candidate"
    );
    assert_eq!(
        count_request_votes(&out),
        2,
        "the fallback fans out real RequestVotes to both peers"
    );
    assert_eq!(
        count_pre_votes(&out),
        0,
        "the fallback round is a REAL election, not another pre-vote"
    );
    assert_eq!(
        node.failed_pre_vote_rounds, 0,
        "the counter resets after the fallback fires (back to pre-vote mode)"
    );
}

#[test]
fn fully_isolated_node_never_falls_back_and_never_inflates_its_term() {
    // The disruption-free guard (Ongaro section 9.6): a FULLY isolated node receives NO
    // `PreVoteResp` at all, so its rounds are never "engaged" -- the etcd #8525 fallback
    // counter never advances and the node NEVER bumps its term. This is what keeps a
    // rejoining isolated node from deposing the standing leader (the whole point of
    // pre-vote). We drive MANY more timeouts than the fallback threshold with no replies
    // and assert the term and role never change.
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    let mut node = RaftNode::new(NodeId(1), voters, MemStorage::new(), RaftConfig::default());
    let mut rng = ZeroRng;

    for round in 0..(PRE_VOTE_FALLBACK_ROUNDS + 5) {
        let mut out = Effects::new();
        node.on_election_timeout(Monotonic::ZERO, &mut rng, &mut out);
        // No PreVoteResp is ever delivered (a true partition): always a pre-vote round,
        // never the fallback, term pinned at 0, role pinned at Follower.
        assert_eq!(
            node.current_term(),
            0,
            "round {round}: an ISOLATED node must NEVER inflate its term (no engagement)"
        );
        assert_eq!(
            node.role(),
            Role::Follower,
            "round {round}: an isolated node stays a Follower forever"
        );
        assert_eq!(
            count_pre_votes(&out),
            2,
            "round {round}: it keeps pre-voting (never the term-bumping fallback)"
        );
        assert_eq!(
            count_request_votes(&out),
            0,
            "round {round}: an isolated node never starts a real election (no disruption)"
        );
        assert_eq!(
            node.failed_pre_vote_rounds, 0,
            "round {round}: the fallback counter never advances without engagement"
        );
    }
}

#[test]
fn healthy_pre_vote_cluster_never_falls_back() {
    // The CONVERSE invariant: in a healthy all-pre-vote cluster the fallback NEVER fires.
    // A node that keeps winning its pre-vote quorum (or keeps hearing a live leader)
    // always resets the counter before it reaches the threshold, so the steady-state path
    // is byte-identical to plain pre-vote (no spurious term bump, no fallback). We run
    // MORE than PRE_VOTE_FALLBACK_ROUNDS successful rounds to prove no accumulation.
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    let mut node = RaftNode::new(NodeId(1), voters, MemStorage::new(), RaftConfig::default());
    let mut rng = ZeroRng;

    for round in 0..(PRE_VOTE_FALLBACK_ROUNDS + 3) {
        // A pre-vote round that DOES reach quorum: time out, then grant from one peer so
        // {granter, self} is a majority and the node converts to a real candidate.
        let mut out = Effects::new();
        node.on_election_timeout(Monotonic::ZERO, &mut rng, &mut out);
        assert_eq!(
            count_pre_votes(&out),
            2,
            "round {round}: the timeout starts a pre-vote round (never the fallback)"
        );
        let pre_term = node.pre_vote_term;
        let mut grant = Effects::new();
        node.on_pre_vote_resp(Monotonic::ZERO, NodeId(2), pre_term, true, &mut grant);
        assert_eq!(
            node.role(),
            Role::Candidate,
            "round {round}: a granted pre-vote quorum converts to a real candidate"
        );
        assert_eq!(
            node.failed_pre_vote_rounds, 0,
            "round {round}: winning a pre-vote resets the fallback counter (no accumulation)"
        );
        // Step the node back down to a follower (a fresh higher-term leader appears) so the
        // next loop iteration runs another clean pre-vote round. observe_term also resets
        // the counter -- the leader-progress reset path -- which we assert holds at 0.
        let next_term = node.current_term() + 1;
        let mut step = Effects::new();
        node.on_append_entries(
            Monotonic::ZERO,
            &mut rng,
            next_term,
            NodeId(3),
            0,
            0,
            Vec::new(),
            0,
            &mut step,
        );
        assert_eq!(
            node.role(),
            Role::Follower,
            "round {round}: the node follows the fresh leader"
        );
        assert_eq!(
            node.failed_pre_vote_rounds, 0,
            "round {round}: hearing a live leader keeps the fallback counter at 0"
        );
    }
}

#[test]
fn election_safety_holds_under_message_drops_and_converges_after_heal() {
    // The most valuable nemesis: dropped (and thus effectively reordered/retried)
    // RequestVote / RequestVoteResp messages are exactly where double-vote and
    // double-tally bugs hide. Election Safety must hold throughout a lossy run,
    // and once the drops stop the cluster must still converge to one leader.
    let config = RaftConfig::default();
    for seed in 0..50u64 {
        let mut cluster = RaftCluster::new(5, seed, config);
        cluster
            .net
            .set_latency(Duration::from_millis(1), Duration::from_millis(20));
        cluster.net.set_drop_prob(0.2);
        for _ in 0..60 {
            cluster.net.run_steps(300);
            assert_election_safety(&cluster);
        }
        // Heal the drops; the cluster must converge to exactly one leader.
        cluster.net.set_drop_prob(0.0);
        cluster.run_until_idle(500_000);
        assert_election_safety(&cluster);
        assert_eq!(
            cluster.leaders().len(),
            1,
            "seed {seed}: must converge to one leader once drops stop, got {:?}",
            cluster.leaders()
        );
    }
}

// =====================================================================
// 3b DST scenarios (log replication + commit; sections 5.3, 5.4.2).
// =====================================================================

/// Run `cluster` to a single leader, then return it. A thin wrapper that also
/// asserts the 3b structural invariants once quiescent.
fn elect_one_leader(cluster: &mut RaftCluster) -> NodeId {
    let leader = run_to_single_leader(cluster, 500, 200);
    // Drain any in-flight replication so the no-op the leader appended on
    // election settles before we start proposing.
    cluster.net.run_steps(5_000);
    assert_3b_invariants(cluster);
    leader
}

fn payload(tag: u8) -> EntryPayload {
    EntryPayload::Bytes(vec![tag])
}

// -- scenario 1: a replicated entry commits and all logs converge ------

#[test]
fn replicated_entry_commits_and_converges() {
    // 3 voters. Elect a leader, propose several entries, run to quiescence.
    // Assert: every node's log converges to the same sequence, commit_index
    // advances past the proposals on a majority, last_applied tracks it, and the
    // two 3b structural invariants hold.
    let mut cluster = RaftCluster::new(3, 11, RaftConfig::default());
    let leader = elect_one_leader(&mut cluster);
    let commit_before = cluster.commit_index(leader);

    // Propose 5 opaque entries at the leader.
    for tag in 0..5u8 {
        cluster.propose(leader, payload(tag));
        cluster.net.run_steps(2_000);
        assert_3b_invariants(&cluster);
    }
    cluster.run_until_idle(100_000);
    assert_3b_invariants(&cluster);

    // The leader's commit_index advanced by at least the 5 proposals (plus the
    // election no-op committed transitively once a current-term entry committed).
    let leader_commit = cluster.commit_index(leader);
    assert!(
        leader_commit >= commit_before + 5,
        "leader commit_index must advance past the proposals: {commit_before} -> {leader_commit}"
    );

    // Every node converges to the leader's exact log, and every node commits and
    // applies up to (at least) the same watermark.
    let leader_log = cluster.log(leader);
    for &id in &cluster.ids {
        assert_eq!(
            cluster.log(id),
            leader_log,
            "node {id:?} log must converge to the leader's"
        );
        assert_eq!(
            cluster.commit_index(id),
            leader_commit,
            "node {id:?} commit_index must match the leader's once idle"
        );
        assert_eq!(
            cluster.last_applied(id),
            cluster.commit_index(id),
            "node {id:?} must have applied up to its commit_index (apply sink)"
        );
    }
}

// -- scenario 2 (D): log convergence after partition heal --------------

#[test]
fn log_convergence_after_partition_heal() {
    // 5 voters. The leader commits entries with a 3-node majority while 2 nodes
    // are partitioned off; on heal, the lagging nodes catch up via the nextIndex
    // decrement/retry backup and every log agrees up to commit_index.
    let mut cluster = RaftCluster::new(5, 23, RaftConfig::default());
    let leader = elect_one_leader(&mut cluster);

    // Choose two followers to isolate; keep the leader + two others as the
    // majority side (3 of 5 = a majority, so the leader stays authoritative and
    // can still commit).
    let followers: Vec<NodeId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != leader)
        .collect();
    let lagging = [followers[0], followers[1]];
    let majority_side: Vec<SimId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != lagging[0] && id != lagging[1])
        .map(to_sim)
        .collect();
    let lagging_side: Vec<SimId> = lagging.iter().copied().map(to_sim).collect();
    cluster.net.partition(&majority_side, &lagging_side);

    // Propose while partitioned; the majority side commits these.
    for tag in 0..6u8 {
        cluster.propose(leader, payload(tag));
        cluster.net.run_steps(3_000);
        assert_3b_invariants(&cluster);
    }
    cluster.net.run_steps(10_000);
    assert_3b_invariants(&cluster);

    let committed_while_partitioned = cluster.commit_index(leader);
    assert!(
        committed_while_partitioned > 0,
        "the majority side must commit while the minority is partitioned"
    );
    // The lagging nodes did NOT receive the new entries (they were partitioned).
    for &id in &lagging {
        assert!(
            cluster.commit_index(id) < committed_while_partitioned,
            "lagging node {id:?} must trail the committed index while partitioned"
        );
    }

    // Heal; the lagging nodes must catch up via nextIndex decrement/retry.
    cluster.net.heal();
    cluster.run_until_idle(200_000);
    assert_3b_invariants(&cluster);

    let leader_log = cluster.log(leader);
    let target_commit = cluster.commit_index(leader);
    for &id in &cluster.ids {
        assert_eq!(
            cluster.log(id),
            leader_log,
            "node {id:?} log must converge after heal"
        );
        assert_eq!(
            cluster.commit_index(id),
            target_commit,
            "node {id:?} must catch up to the committed index after heal"
        );
    }
}

// -- scenario 3 (E1): the Figure-8 commit-safety gate ------------------

/// THE Figure-8 safety rule (section 5.4.2), proven on the PURE engine where the
/// exact log states the paper draws can be constructed deterministically (the
/// sim's single-partition model cannot force the precise 5-way leader hand-off
/// Figure 8 requires; driving the engine directly is the faithful reproduction).
///
/// Construction mirrors Figure 8 exactly. Cluster S1..S5. We make S1 the leader
/// in term 4 with this log: index1=(term1), index2=(term2). S2 also has
/// index2=(term2) (S1 replicated it to S2 back in term 2). S3,S4,S5 have only
/// index1. The danger the rule guards: S1 must NOT commit index2 (a term-2 entry)
/// just because index2 is now on a MAJORITY {S1,S2,S1-counts-3rd?}. We drive S1's
/// commit logic with match_index showing index2 on a majority and assert S1 does
/// NOT advance commit to index2 (it is a PRIOR-term entry). Then S1 appends a
/// term-4 entry at index3, gets it onto a majority, and NOW commit jumps to
/// index3, carrying index2 with it transitively. That committed state is then
/// durable: a subsequent leader cannot overwrite it (it is on a majority with a
/// current-or-newer term).
#[test]
fn figure_8_commit_safety() {
    let voters: BTreeSet<NodeId> = (1..=5).map(NodeId).collect();
    // Build S1's storage exactly as Figure 8 (c): index1 term1, index2 term2.
    let mut s1 = MemStorage::new();
    s1.set_current_term(4);
    s1.append(LogEntry {
        term: 1,
        index: 1,
        payload: EntryPayload::Noop,
    });
    s1.append(LogEntry {
        term: 2,
        index: 2,
        payload: payload(0xAA),
    });
    let mut leader = RaftNode::new(NodeId(1), voters.clone(), s1, RaftConfig::default());
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);

    // Force S1 to leader in term 4 the way the engine reaches it: it just won an
    // election. We replay that by hand so next_index/match_index initialize.
    // (Directly flipping role would skip the Figure-2 leader init.) Win via a
    // crafted vote round at the engine boundary instead: easier and faithful is
    // to call the internal promotion path through a candidate transition.
    // Simplest deterministic path: set role to Candidate with a full tally, then
    // run maybe_become_leader, which initializes next/match and appends the
    // term-4 no-op. To avoid the no-op perturbing the index math below, we model
    // the post-election state by initializing leader markers ourselves and then
    // exercising ONLY the commit rule. The commit rule is the unit under test.
    promote_to_leader_for_test(&mut leader);

    // After promotion the leader appended a term-4 no-op at index3. Figure 8's
    // index2 (term 2) is now the SECOND entry; the no-op is index3 (term 4).
    assert_eq!(leader.storage().last_log_index(), 3);
    assert_eq!(
        leader.storage().term_at(2),
        2,
        "index2 is the prior-term entry"
    );
    assert_eq!(
        leader.storage().term_at(3),
        4,
        "index3 is the current-term no-op"
    );
    assert_eq!(leader.commit_index(), 0, "nothing committed yet");

    // STEP 1 (the dangerous one): a MAJORITY now stores index2 (the term-2
    // entry). Model S2 acknowledging up to index2, and S1 itself has it: that is
    // 2 of 5; bring in S3 acking index2 too -> 3 of 5 = a majority storing
    // index2. The section-5.4.2 rule MUST refuse to commit index2 by this count,
    // because index2 is from a PRIOR term (term 2 != currentTerm 4).
    let mut out = Effects::new();
    leader.on_append_entries_resp(now, &mut rng, NodeId(2), 4, true, 2, &mut out);
    leader.on_append_entries_resp(now, &mut rng, NodeId(3), 4, true, 2, &mut out);
    assert_eq!(
        leader.commit_index(),
        0,
        "FIGURE 8: a prior-term entry (index2, term2) on a MAJORITY must NOT be \
             committed by replica count (section 5.4.2)"
    );
    // HA-prod-commit-ack: no commit advanced, so NO committed-through is recorded.
    assert_eq!(
        out.committed_through, None,
        "no commit advanced -> no committed-through record"
    );

    // STEP 2: the leader replicates its CURRENT-term entry (index3, term4) to a
    // majority. The moment index3 is on a majority, commit jumps to index3 - and
    // index2 commits TRANSITIVELY (Log Matching: index2 precedes the now-committed
    // index3). This is the ONLY way the prior-term entry becomes committed.
    let mut out2 = Effects::new();
    leader.on_append_entries_resp(now, &mut rng, NodeId(2), 4, true, 3, &mut out2);
    leader.on_append_entries_resp(now, &mut rng, NodeId(3), 4, true, 3, &mut out2);
    assert_eq!(
        leader.commit_index(),
        3,
        "once a CURRENT-term entry (index3, term4) is on a majority, commit \
             advances to it and carries index2 with it transitively"
    );
    // HA-prod-commit-ack: commit advanced to 3 exactly when commit_index did, so the
    // step records committed_through == 3 (the adapter resolves a parked ack <= 3).
    assert_eq!(
        out2.committed_through,
        Some(3),
        "committed-through advances exactly when commit_index does"
    );
    assert_eq!(
        leader.last_applied(),
        3,
        "apply pipeline follows commit_index"
    );

    // STEP 3 (durability): index2 is now committed. Assert the engine never
    // un-commits it and never overwrites it. Re-running the commit rule (more
    // acks, idle heartbeats) only ever advances or holds commit, never rewinds.
    let committed_log = leader.storage().log().to_vec();
    leader.on_append_entries_resp(now, &mut rng, NodeId(4), 4, true, 3, &mut out);
    leader.on_append_entries_resp(now, &mut rng, NodeId(5), 4, true, 3, &mut out);
    assert_eq!(
        leader.commit_index(),
        3,
        "commit_index is monotone: extra acks never rewind it"
    );
    assert_eq!(
        &leader.storage().log()[..2],
        &committed_log[..2],
        "the committed prefix (index1, index2) is never overwritten"
    );
}

// -- HA-prod-commit-ack: the committed-through Effects record ------------
//
// These prove the engine NOTIFICATION the production adapter uses to resolve a
// propose ack on TRUE COMMIT (not at append): an entry is NOT reported committed
// until a majority has appended it; the record advances exactly when commit_index
// does; a single voter commits on append; and an uncommitted entry overwritten by a
// new leader is detectable (so the adapter can fail its parked ack).

/// N=1: a single-voter leader is its own majority, so a proposed entry commits the
/// instant it is appended, and `propose` returns the assigned index with the same
/// step recording `committed_through == index` (the adapter resolves promptly, the
/// same observable timing as before this change).
#[test]
fn single_voter_commits_on_append() {
    let voters: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
    let storage = MemStorage::new();
    let mut node = RaftNode::new(NodeId(1), voters, storage, RaftConfig::default());
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);

    // Time out -> become candidate -> self-elect (a 1-voter majority is itself). The
    // election appends a term no-op at index 1 but does NOT run the commit rule on the
    // winning step (maybe_become_leader broadcasts + arms heartbeat only), so nothing
    // is committed yet -- exactly the pre-existing engine behaviour, unchanged here.
    let mut out = Effects::new();
    node.on_timer(now, &mut rng, ELECTION_TIMEOUT, &mut out);
    assert!(node.is_leader(), "the lone voter self-elects");
    assert_eq!(
        node.commit_index(),
        0,
        "the election step runs no commit rule"
    );
    assert_eq!(
        out.committed_through, None,
        "no commit advanced on the winning step"
    );

    // A client proposal commits ON APPEND: propose() runs maybe_advance_commit, and a
    // single voter is its own majority, so the proposal (index 2) commits within THIS
    // step -- carrying the index-1 no-op with it -- so commit jumps to 2 and the step
    // records committed_through == 2. The parked ack therefore resolves promptly, the
    // same observable timing as the old commit-on-append behaviour for N=1.
    let mut out = Effects::new();
    let index = node
        .propose(payload(7), now, &mut rng, &mut out)
        .expect("leader accepts the proposal");
    assert_eq!(index, 2, "the proposal lands at index 2");
    assert_eq!(
        node.commit_index(),
        2,
        "N=1 commits the proposal (and the no-op below it) on append"
    );
    assert_eq!(
        out.committed_through,
        Some(2),
        "N=1: propose's own step records the entry committed (resolves promptly)"
    );
}

/// N=3: a proposed entry is NOT committed (no committed-through is recorded) until a
/// MAJORITY has appended it. The leader plus ONE follower is only 2 of 3 (a
/// majority), so the FIRST follower ack commits; before that ack arrives the entry is
/// uncommitted and `committed_through` stays `None`. This is exactly what lets the
/// adapter hold a parked ack until true commit.
#[test]
fn three_voter_commits_only_on_majority() {
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    let storage = MemStorage::new();
    let mut leader = RaftNode::new(NodeId(1), voters, storage, RaftConfig::default());
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);

    // Drive the leader to power via the engine (term 1 election, then a majority of
    // granted votes), so next_index / match_index initialize the Figure-2 way. PROD-9:
    // the timeout runs a pre-vote round first, so promote through it (grant from node 2).
    time_out_to_candidate(&mut leader, NodeId(2), now);
    assert_eq!(
        leader.role(),
        Role::Candidate,
        "election timeout -> candidate"
    );
    let mut out = Effects::new();
    leader.on_message(
        now,
        &mut rng,
        NodeId(2),
        RaftMsg::RequestVoteResp {
            term: leader.current_term(),
            vote_granted: true,
        },
        &mut out,
    );
    assert!(leader.is_leader(), "a 2/3 vote majority elects the leader");
    // The election no-op is at the leader's last index, still uncommitted (no
    // follower has acked it yet), so winning the election records nothing committed.
    assert_eq!(leader.commit_index(), 0, "nothing committed before any ack");
    assert_eq!(
        out.committed_through, None,
        "the no-op is appended but not yet on a majority"
    );

    // Propose a client entry. It is APPENDED at the next index but NOT committed (it
    // is on only the leader, 1 of 3); propose's own step records NO commit.
    let mut out = Effects::new();
    let index = leader
        .propose(payload(9), now, &mut rng, &mut out)
        .expect("leader accepts the proposal");
    assert!(index >= 2, "the proposal lands above the election no-op");
    assert_eq!(
        leader.commit_index(),
        0,
        "the proposed entry is on ONLY the leader (1/3): not committed on append"
    );
    assert_eq!(
        out.committed_through, None,
        "HA-prod-commit-ack: a 3-voter proposal does NOT commit at append time"
    );

    // ONE follower acks up to the proposal's index: leader + that follower = 2/3 = a
    // majority. NOW commit advances to `index`, and the ack-step records it. (One
    // follower acking is the moment the parked ack would resolve Committed.)
    let mut out = Effects::new();
    leader.on_append_entries_resp(
        now,
        &mut rng,
        NodeId(2),
        leader.current_term(),
        true,
        index,
        &mut out,
    );
    assert_eq!(
        leader.commit_index(),
        index,
        "the first follower ack puts the entry on a majority -> committed"
    );
    assert_eq!(
        out.committed_through,
        Some(index),
        "committed-through is recorded exactly when commit_index reaches the entry"
    );
}

/// An uncommitted entry that a NEW leader overwrites is DETECTABLE (the term at its
/// index changes), which is how the adapter fails a parked ack with NotLeader. We
/// build a follower holding an uncommitted term-1 entry at index 2, then deliver a
/// higher-term AppendEntries from a new leader that truncates it and replaces it with
/// a term-2 entry. The committed-through record never names the overwritten index, and
/// `term_at(2)` now reads the NEW term, exactly the signal the adapter inspects.
#[test]
fn overwritten_uncommitted_entry_is_detectable() {
    // Follower at term 1 with an uncommitted log [ (t1,i1), (t1,i2) ]; nothing is
    // committed (commit_index 0), so index 2 is overwrite-eligible.
    let mut node = follower_with_log(2, 1, &[(1, 1), (1, 2)]);
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    assert_eq!(node.commit_index(), 0, "nothing committed at the follower");
    assert_eq!(
        node.storage().term_at(2),
        1,
        "index 2 holds the term-1 entry"
    );

    // A NEW leader in term 2 sends AppendEntries that keeps index 1 but replaces
    // index 2 with a term-2 entry (a conflict at index 2 -> truncate + append). The
    // leader has not committed index 2 either (leader_commit caps at index 1).
    let mut out = Effects::new();
    node.on_message(
        now,
        &mut rng,
        NodeId(1),
        RaftMsg::AppendEntries {
            term: 2,
            leader: NodeId(1),
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![noop(2, 2)],
            leader_commit: 1,
        },
        &mut out,
    );
    // The overwrite landed: index 2 now holds the NEW term. THIS is the signal the
    // adapter reads (a parked ack at index 2 sees term_at(2) != its proposal's term).
    assert_eq!(
        node.storage().term_at(2),
        2,
        "the new leader OVERWROTE the uncommitted index-2 entry (term 1 -> 2)"
    );
    // Commit advanced only to index 1 (what the new leader vouched as committed), so
    // the committed-through record NEVER names the overwritten index 2.
    assert_eq!(node.commit_index(), 1, "only index 1 committed");
    assert_eq!(
        out.committed_through,
        Some(1),
        "committed-through names index 1, NEVER the overwritten index 2"
    );
}

/// A second, end-to-end Figure-8 witness over the FULL sim across a seed sweep:
/// drive leader changes and partitions so old-term entries get replicated widely,
/// and assert the cross-TIME [`CommitLedger`] never sees a committed entry
/// overwritten (the safety property the 5.4.2 rule guarantees). This is the
/// "closest deterministic reproduction" the spec asks for when a fully scripted
/// 5-way Figure 8 cannot be forced through the single-partition sim model.
#[test]
fn figure_8_commit_safety_seed_sweep() {
    let config = RaftConfig::default();
    for seed in 0..40u64 {
        let mut cluster = RaftCluster::new(5, seed, config);
        cluster
            .net
            .set_latency(Duration::from_millis(1), Duration::from_millis(15));
        let mut ledger = CommitLedger::new();

        // Round after round: find the current leader, propose, then partition it
        // off so a NEW leader rises with the old leader's entries possibly only
        // partially replicated (the Figure-8 precondition: prior-term entries
        // scattered across a changing majority). Heal and repeat. The ledger is
        // sampled every chunk; it must never record an overwrite.
        for round in 0..6 {
            let leader = run_to_single_leader(&mut cluster, 500, 200);
            ledger.observe_and_check(&cluster);
            assert_3b_invariants(&cluster);

            // Propose a couple of entries tagged by round so they are distinct.
            cluster.propose(leader, payload(round));
            cluster.propose(leader, payload(round.wrapping_add(100)));
            // Let them partially replicate.
            cluster.net.run_steps(800);
            ledger.observe_and_check(&cluster);

            // Isolate the leader -> a new leader must rise on the majority side.
            let others: Vec<SimId> = cluster
                .ids
                .iter()
                .copied()
                .filter(|&id| id != leader)
                .map(to_sim)
                .collect();
            cluster.net.partition(&[to_sim(leader)], &others);
            for _ in 0..20 {
                cluster.net.run_steps(500);
                ledger.observe_and_check(&cluster);
                assert_3b_invariants(&cluster);
            }
            // Heal; let everything reconcile, sampling the ledger throughout.
            cluster.net.heal();
            for _ in 0..20 {
                cluster.net.run_steps(500);
                ledger.observe_and_check(&cluster);
                assert_3b_invariants(&cluster);
            }
        }
        cluster.run_until_idle(200_000);
        ledger.observe_and_check(&cluster);
        assert_3b_invariants(&cluster);
    }
}

// -- scenario 4: determinism replay of propose+partition+heal ----------

/// Replay one propose+partition+heal run for `seed`, returning the trace plus a
/// per-node (log, commit_index) snapshot, so two same-seed runs can be compared
/// byte-for-byte. The fault script is fixed (partition the first elected leader
/// after a fixed set of proposals), so a same-seed replay is identical.
fn replay_propose_partition(
    seed: u64,
) -> (Vec<ironcache_sim::TraceRecord>, Vec<(Vec<LogEntry>, u64)>) {
    let mut cluster = RaftCluster::new(5, seed, RaftConfig::default());
    let leader = run_to_single_leader(&mut cluster, 500, 200);
    cluster.net.run_steps(5_000);

    // A fixed proposal + partition + heal script.
    for tag in 0..4u8 {
        cluster.propose(leader, payload(tag));
        cluster.net.run_steps(1_500);
    }
    let others: Vec<SimId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != leader)
        .map(to_sim)
        .collect();
    cluster.net.partition(&[to_sim(leader)], &others);
    cluster.net.run_steps(40_000);
    cluster.net.heal();
    cluster.net.run_steps(40_000);

    assert_3b_invariants(&cluster);
    let snapshot: Vec<(Vec<LogEntry>, u64)> = cluster
        .ids
        .iter()
        .map(|&id| (cluster.log(id), cluster.commit_index(id)))
        .collect();
    (cluster.net.trace().to_vec(), snapshot)
}

#[test]
fn determinism_replay_3b() {
    // A propose+partition+heal scenario must replay byte-identically across a
    // 100-seed sweep, with the log-matching + state-machine-safety invariants
    // asserted (inside replay) each seed.
    for seed in 0..100u64 {
        let (trace_a, snap_a) = replay_propose_partition(seed);
        let (trace_b, snap_b) = replay_propose_partition(seed);
        assert_eq!(
            trace_a, trace_b,
            "seed {seed}: propose+partition+heal trace must replay byte-identically"
        );
        assert_eq!(
            snap_a, snap_b,
            "seed {seed}: per-node (log, commit_index) must replay identically"
        );
    }
}

// =====================================================================
// 3d DST MEMBERSHIP-SAFETY GATE (the merge-blocker, Raft section 6).
//
// Grow a cluster 1 -> 3 (then exercise learners + a leader self-removal) UNDER
// PARTITIONS, across many seeds, asserting across the WHOLE timeline that
// ELECTION SAFETY holds (NEVER two leaders in one term, even mid-reconfiguration
// with a partition that could split old-vs-new config -- the disjoint-majority
// hazard single-server changes must rule out), plus Log-Matching + State-Machine
// -Safety across the config changes, and determinism replay.
// =====================================================================

/// Run one membership scenario for `seed`: a 1-voter cluster GROWS to 3 by adding
/// two voters one at a time (each via a committed ConfigChange), UNDER an injected
/// partition that isolates one node mid-reconfiguration, then heals. Returns the final
/// per-node (voters, committed-log) snapshot so two same-seed runs can be compared.
/// Election safety + log-matching + state-machine-safety are asserted at every
/// quiescent checkpoint INSIDE the run (the gate is the in-run asserts; the returned
/// snapshot is the determinism witness).
fn run_membership_grow_under_partition(seed: u64) -> Vec<(BTreeSet<NodeId>, Vec<LogEntry>)> {
    let config = RaftConfig::default();
    // Start with a single voter that self-elects, plus two not-yet-members standing by
    // (seeded with an empty-of-self config so they do not campaign until added).
    let mut cluster = RaftCluster::new(1, seed, config);
    cluster
        .net
        .set_latency(Duration::from_millis(1), Duration::from_millis(15));
    cluster.run_until_idle(20_000);
    assert_election_safety(&cluster);
    let leader = cluster.leaders();
    assert_eq!(leader.len(), 1, "seed {seed}: the lone voter self-elects");
    let leader = leader[0];

    // Bring up two fresh joining nodes (NodeId 2, 3) whose seed config is just {1}
    // (they learn they are voters when the AddVoter entry replicates to them).
    let seed_cfg: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
    cluster.add_joining_node(NodeId(2), seed_cfg.clone(), config);
    cluster.add_joining_node(NodeId(3), seed_cfg, config);

    // GROW 1 -> 2: propose AddVoter(2); let it replicate + commit, asserting safety
    // throughout. We checkpoint election safety in chunks (mid-reconfiguration).
    cluster.propose_membership(leader, MembershipChange::AddVoter(NodeId(2)));
    for _ in 0..30 {
        cluster.net.run_steps(200);
        assert_election_safety(&cluster);
        assert_log_matching(&cluster);
    }
    cluster.run_until_idle(50_000);
    assert_election_safety(&cluster);

    // PARTITION the new node (2) away from {1,3} mid-reconfiguration, then GROW 1 -> 3
    // by proposing AddVoter(3) from whoever is leader. The partition is exactly the
    // disjoint-majority setup: while it holds, no term may have two leaders.
    let cur_leader = cluster.leaders().first().copied().unwrap_or(leader);
    cluster.net.partition(
        &[to_sim(NodeId(2))],
        &[to_sim(NodeId(1)), to_sim(NodeId(3))],
    );
    cluster.propose_membership(cur_leader, MembershipChange::AddVoter(NodeId(3)));
    for _ in 0..40 {
        cluster.net.run_steps(200);
        // THE GATE: election safety must hold at EVERY checkpoint, even with the
        // partition splitting the cluster mid-reconfiguration.
        assert_election_safety(&cluster);
        assert_log_matching(&cluster);
        assert_state_machine_safety(&cluster);
    }

    // HEAL and converge. Election safety still holds; the cluster settles to one leader.
    cluster.net.heal();
    for _ in 0..60 {
        cluster.net.run_steps(500);
        assert_election_safety(&cluster);
        assert_log_matching(&cluster);
        assert_state_machine_safety(&cluster);
        if cluster.leaders().len() == 1 {
            break;
        }
    }
    cluster.run_until_idle(100_000);
    assert_election_safety(&cluster);
    assert_log_matching(&cluster);
    assert_state_machine_safety(&cluster);

    // Snapshot the final per-node (voters, committed-log prefix) for the determinism
    // assertion (committed prefix is the agreed truth).
    cluster
        .ids
        .iter()
        .map(|&id| {
            let ci = cluster.commit_index(id);
            let log = cluster.log(id);
            let committed: Vec<LogEntry> = log.into_iter().filter(|e| e.index <= ci).collect();
            (cluster.voters_of(id), committed)
        })
        .collect()
}

#[test]
fn membership_grow_under_partition_keeps_election_safety() {
    // THE merge-blocker gate across a seed sweep: grow 1 -> 3 under a partition that
    // splits old-vs-new config mid-reconfiguration; election safety + log-matching +
    // state-machine-safety hold across the WHOLE timeline (asserted inside the run),
    // and the cluster converges to a single 3-voter leader.
    for seed in 0..40u64 {
        let snap = run_membership_grow_under_partition(seed);
        // After heal + convergence every node agrees the config is the 3-voter set.
        let all_three: BTreeSet<NodeId> = (1..=3).map(NodeId).collect();
        for (voters, _) in &snap {
            assert_eq!(
                voters, &all_three,
                "seed {seed}: every node converges to the 3-voter config"
            );
        }
    }
}

/// FIX3 DISJOINT-MAJORITY GATE (Raft section 6 overlap proof). Grow 4 -> 5 and, DURING
/// the reconfiguration window (after AddVoter(5) is appended on the leader, before it
/// commits cluster-wide), PARTITION the cluster exactly along the OLD-vs-NEW majority
/// boundary, then let BOTH sides try to elect under randomized timing.
///
/// THE BOUNDARY. C_old = {1,2,3,4} (majority 3); C_new = {1,2,3,4,5} (majority 3). The
/// partition is `A = {1,2,3}` against `B = {4,5}`. Side A is a majority of BOTH configs
/// (3 of 4 old, 3 of 5 new), so A can elect a leader on whichever config its members
/// hold. Side B includes the JUST-ADDED voter 5 (which, under append-time adoption,
/// believes the config is C_new) plus the highest old voter 4. Under the WRONG rule
/// (adopt-on-COMMIT) a node on B that has NOT yet learned of voter 5 would count quora
/// over the smaller C_old, and 5 (which thinks it is a full voter) could grant / solicit
/// votes -- the exact path to a SECOND leader in a term A also leads.
///
/// The single-server overlap theorem says any C_old-majority and any C_new-majority
/// SHARE a node (because the two configs differ by exactly one member), so two DISJOINT
/// electing majorities cannot exist on opposite sides of ANY partition, and B (a
/// minority of both) can never elect while A holds a majority. The gate asserts that
/// headline across the WHOLE timeline: `assert_election_safety` (NEVER two leaders in one
/// term), plus Log-Matching, State-Machine-Safety, and committed-entries-survive (the
/// `CommitLedger`) -- AND that the reconfiguration CONVERGES to the correct 5-voter
/// config (the caller's post-run assertion).
///
/// NON-VACUITY (confirmed by a scratch experiment, not committed). Flipping append-time
/// adoption to adopt-on-COMMIT (fold only ConfigChange entries at index <= commit_index
/// in `recompute_config_from_log`) makes this gate FAIL: a node on side B that has the
/// uncommitted AddVoter(5) in its log no longer counts itself / voter 5, so it falls back
/// to the smaller C_old quorum, and the cluster does NOT converge to the appended 5-voter
/// config (the committed voter set ends up {1,2,3,4}, tripping the caller's convergence
/// assertion). Because the overlap theorem holds in BOTH rules within this crash-free
/// harness, the observable break under the wrong rule is the RECONFIGURATION CORRECTNESS
/// / LIVENESS one, which is exactly what this scenario exercises; a genuine two-leaders
/// (`assert_election_safety`) failure here would be a real engine bug to FIX, not assert
/// around.
///
/// Returns the final per-node (voters, committed-log) snapshot for the determinism
/// replay assertion.
fn run_membership_grow_disjoint_majority(seed: u64) -> Vec<(BTreeSet<NodeId>, Vec<LogEntry>)> {
    // A short, well-separated election window so both partition sides genuinely attempt
    // elections within the run (the jitter varies the interleaving across seeds).
    let config = RaftConfig {
        election_timeout_base: Duration::from_millis(150),
        election_timeout_jitter: Duration::from_millis(150),
        heartbeat_interval: Duration::from_millis(50),
        ..RaftConfig::default()
    };
    let mut cluster = RaftCluster::new(4, seed, config);
    cluster
        .net
        .set_latency(Duration::from_millis(1), Duration::from_millis(20));
    let mut ledger = CommitLedger::new();

    // Converge the 4-voter cluster to a single leader.
    let leader = run_to_single_leader(&mut cluster, 200, 400);
    cluster.run_until_idle(50_000);
    assert_election_safety(&cluster);
    ledger.observe_and_check(&cluster);

    // Bring up the joining node 5 (seed config = the 4 voters; it learns it is a voter
    // when AddVoter(5) replicates into its log).
    let voters4: BTreeSet<NodeId> = (1..=4).map(NodeId).collect();
    cluster.add_joining_node(NodeId(5), voters4, config);

    // OPEN THE RECONFIGURATION WINDOW: propose AddVoter(5) on the leader. Append-time
    // adoption flips the leader to C_new immediately; the entry is not yet committed
    // cluster-wide. Step only a LITTLE so the change is mid-flight (partially
    // replicated), then partition along the old-vs-new boundary.
    cluster.propose_membership(leader, MembershipChange::AddVoter(NodeId(5)));
    for _ in 0..3 {
        cluster.net.run_steps(40);
        assert_election_safety(&cluster);
        assert_log_matching(&cluster);
        assert_state_machine_safety(&cluster);
        ledger.observe_and_check(&cluster);
    }

    // PARTITION exactly along the boundary: A = {1,2,3} (a majority of both configs) vs
    // B = {4,5} (a minority of both; carries the just-added voter 5). Both sides will
    // time out and campaign under randomized jitter.
    cluster.net.partition(
        &[to_sim(NodeId(1)), to_sim(NodeId(2)), to_sim(NodeId(3))],
        &[to_sim(NodeId(4)), to_sim(NodeId(5))],
    );
    for _ in 0..60 {
        cluster.net.run_steps(100);
        // THE GATE: at EVERY checkpoint while the partition straddles the old/new
        // boundary, there is never more than one leader per term, and committed
        // history never diverges.
        assert_election_safety(&cluster);
        assert_log_matching(&cluster);
        assert_state_machine_safety(&cluster);
        ledger.observe_and_check(&cluster);
    }

    // HEAL and converge to a single 5-voter leader; safety holds throughout.
    cluster.net.heal();
    for _ in 0..120 {
        cluster.net.run_steps(500);
        assert_election_safety(&cluster);
        assert_log_matching(&cluster);
        assert_state_machine_safety(&cluster);
        ledger.observe_and_check(&cluster);
        if cluster.leaders().len() == 1 && cluster.voters_of(cluster.leaders()[0]).len() == 5 {
            break;
        }
    }
    cluster.run_until_idle(200_000);
    assert_election_safety(&cluster);
    assert_log_matching(&cluster);
    assert_state_machine_safety(&cluster);
    ledger.observe_and_check(&cluster);

    cluster
        .ids
        .iter()
        .map(|&id| {
            let ci = cluster.commit_index(id);
            let log = cluster.log(id);
            let committed: Vec<LogEntry> = log.into_iter().filter(|e| e.index <= ci).collect();
            (cluster.voters_of(id), committed)
        })
        .collect()
}

#[test]
fn membership_grow_disjoint_majority_keeps_election_safety() {
    // FIX3: the disjoint-majority gate across a seed sweep. Grow 4 -> 5 and partition
    // along the old-vs-new majority boundary mid-reconfiguration; ELECTION SAFETY (never
    // two leaders per term) plus Log-Matching + State-Machine-Safety +
    // committed-entries-survive hold across the WHOLE timeline (asserted inside the run),
    // and the cluster converges to a single 5-voter leader.
    for seed in 0..40u64 {
        let snap = run_membership_grow_disjoint_majority(seed);
        let all_five: BTreeSet<NodeId> = (1..=5).map(NodeId).collect();
        for (voters, _) in &snap {
            assert_eq!(
                voters, &all_five,
                "seed {seed}: every node converges to the 5-voter config"
            );
        }
    }
}

#[test]
fn membership_grow_disjoint_majority_replays_deterministically() {
    // The disjoint-majority scenario is deterministic: two same-seed runs produce
    // identical final per-node (voters, committed-log) snapshots.
    for seed in 0..15u64 {
        let a = run_membership_grow_disjoint_majority(seed);
        let b = run_membership_grow_disjoint_majority(seed);
        assert_eq!(
            a, b,
            "seed {seed}: the disjoint-majority scenario must replay identically"
        );
    }
}

#[test]
fn membership_grow_under_partition_replays_deterministically() {
    // Two same-seed runs of the grow-under-partition scenario produce IDENTICAL final
    // per-node (voters, committed-log) snapshots: the membership path is deterministic.
    for seed in 0..20u64 {
        let a = run_membership_grow_under_partition(seed);
        let b = run_membership_grow_under_partition(seed);
        assert_eq!(
            a, b,
            "seed {seed}: membership grow-under-partition must replay identically"
        );
    }
}

#[test]
fn learner_catches_up_then_is_promoted_to_voter() {
    // A 3-voter cluster adds a LEARNER (NodeId 4) that catches up via AppendEntries,
    // then is promoted to a voter once caught up -- all while staying election-safe.
    let config = RaftConfig::default();
    let mut cluster = RaftCluster::new(3, 4242, config);
    let leader = run_to_single_leader(&mut cluster, 500, 200);
    cluster.net.run_steps(5_000);
    assert_election_safety(&cluster);

    // Pile some committed entries onto the cluster so a fresh learner is BEHIND.
    for tag in 0..6u8 {
        cluster.propose(leader, EntryPayload::Bytes(vec![tag]));
        cluster.net.run_steps(1_000);
    }
    cluster.run_until_idle(50_000);

    // Add a fresh learner (NodeId 4), seeded with the 3-voter config (it is not yet a
    // member of it). Propose AddLearner(4); it replicates the whole log to catch up.
    let voters3: BTreeSet<NodeId> = (1..=3).map(NodeId).collect();
    cluster.add_joining_node(NodeId(4), voters3, config);
    let leader = cluster.leaders().first().copied().expect("a leader");
    cluster.propose_membership(leader, MembershipChange::AddLearner(NodeId(4)));
    cluster.run_until_idle(100_000);
    assert_election_safety(&cluster);
    // The leader sees NodeId(4) as a learner (not a voter).
    assert!(
        cluster.learners_of(leader).contains(&NodeId(4)),
        "the new node is a learner on the leader"
    );
    assert!(
        !cluster.voters_of(leader).contains(&NodeId(4)),
        "a learner is not counted as a voter"
    );
    // The learner caught up: its committed prefix matches the leader's.
    let leader_ci = cluster.commit_index(leader);
    assert!(
        cluster.commit_index(NodeId(4)) >= leader_ci.saturating_sub(LEARNER_CATCHUP_LAG),
        "the learner caught up to within the lag gate"
    );

    // PROMOTE the learner to a voter; the cluster is now 4 voters and stays safe.
    cluster.propose_membership(leader, MembershipChange::PromoteLearner(NodeId(4)));
    cluster.run_until_idle(100_000);
    assert_election_safety(&cluster);
    let voters4: BTreeSet<NodeId> = (1..=4).map(NodeId).collect();
    for &id in &cluster.ids {
        assert_eq!(
            cluster.voters_of(id),
            voters4,
            "node {id:?} converged to the 4-voter config after promotion"
        );
    }
    assert_eq!(
        cluster.leaders().len(),
        1,
        "exactly one leader in the grown 4-voter cluster"
    );
}

#[test]
fn shrink_cluster_removes_a_voter_and_stays_safe() {
    // A 3-voter cluster SHRINKS to 2 by removing a non-leader voter, under election
    // safety; the committed config converges to {remaining two}.
    let config = RaftConfig::default();
    let mut cluster = RaftCluster::new(3, 909, config);
    let leader = run_to_single_leader(&mut cluster, 500, 200);
    cluster.net.run_steps(5_000);
    assert_election_safety(&cluster);
    // Remove a voter that is NOT the leader.
    let victim = cluster
        .ids
        .iter()
        .copied()
        .find(|&id| id != leader)
        .expect("a non-leader voter");
    cluster.propose_membership(leader, MembershipChange::RemoveVoter(victim));
    for _ in 0..40 {
        cluster.net.run_steps(300);
        assert_election_safety(&cluster);
        assert_log_matching(&cluster);
    }
    cluster.run_until_idle(100_000);
    assert_election_safety(&cluster);
    // The leader's committed config excludes the victim.
    assert!(
        !cluster.voters_of(leader).contains(&victim),
        "the removed voter is gone from the leader's config"
    );
    assert_eq!(
        cluster.voters_of(leader).len(),
        2,
        "the cluster shrank to 2 voters"
    );
    assert_eq!(cluster.leaders().len(), 1, "still exactly one leader");
}

/// Promote a candidate-free node directly to leader for the Figure-8 unit test:
/// seat a full vote tally and run the engine's own promotion path so
/// `next_index`/`match_index` initialize per Figure 2 and the election no-op is
/// appended, exactly as a real election would leave the node. Test-only.
fn promote_to_leader_for_test(node: &mut RaftNode<MemStorage>) {
    // Move to Candidate in the current term with every vote, then let the
    // engine's maybe_become_leader run via a self vote record. We reach the
    // private transition through the public step surface: simulate winning by
    // delivering granted RequestVoteResp from a majority. First become candidate
    // by timing out is wrong (it bumps the term); instead we seat the role and
    // votes through a direct election timeout would change term. To keep term 4,
    // we drive promotion by hand-seating the candidate state and invoking the
    // crate-internal maybe_become_leader (same module, so it is reachable).
    node.role = Role::Candidate;
    node.votes.clear();
    node.votes.insert(NodeId(1));
    node.votes.insert(NodeId(2));
    node.votes.insert(NodeId(3));
    let mut out = Effects::new();
    node.maybe_become_leader(Monotonic::ZERO, &mut out);
    assert!(node.is_leader(), "promotion must reach Leader");
}

// -- AppendEntries reconciliation safety (engine-direct, closing review gaps) --
//
// The truncate-only-on-conflict loop and the follower commit cap are the most
// safety-critical follower logic. These drive on_append_entries directly so a
// regression is caught deterministically, without relying on the sim to stumble
// into the precise race.

/// Build a follower at `term` whose log holds the given (term, index) entries.
fn follower_with_log(id: u64, term: u64, log: &[(u64, u64)]) -> RaftNode<MemStorage> {
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    let mut storage = MemStorage::new();
    storage.set_current_term(term);
    for &(t, i) in log {
        storage.append(LogEntry {
            term: t,
            index: i,
            payload: EntryPayload::Noop,
        });
    }
    RaftNode::new(NodeId(id), voters, storage, RaftConfig::default())
}

fn noop(term: u64, index: u64) -> LogEntry {
    LogEntry {
        term,
        index,
        payload: EntryPayload::Noop,
    }
}

fn log_terms(node: &RaftNode<MemStorage>) -> Vec<u64> {
    node.storage().log().iter().map(|e| e.term).collect()
}

/// Drive a multi-voter `node` from a Follower to a real Candidate the PROD-9 way: fire
/// the election timeout (which starts a PRE-VOTE round under the default pre-vote-on
/// config), then deliver ONE granted `PreVoteResp` from `granter` so the pre-vote quorum
/// (granter + self) converts it to a real candidate (term bumped, RequestVotes sent).
/// Used by the legacy hand-driven election unit tests so they reach candidacy without
/// each having to spell out the pre-vote handshake. (Single-voter clusters self-promote
/// straight through, so they do not need this.)
fn time_out_to_candidate(node: &mut RaftNode<MemStorage>, granter: NodeId, now: Monotonic) {
    let mut rng = ZeroRng;
    node.on_timer(now, &mut rng, ELECTION_TIMEOUT, &mut Effects::new());
    // The pre-vote round targets current_term + 1; grant it from one peer to reach quorum.
    let pre_term = node.pre_vote_term;
    node.on_message(
        now,
        &mut rng,
        granter,
        RaftMsg::PreVoteResp {
            term: pre_term,
            vote_granted: true,
        },
        &mut Effects::new(),
    );
}

#[test]
fn identical_retransmit_does_not_truncate_the_log() {
    // G2(a): a duplicate/retransmitted AppendEntries whose entries the follower
    // already holds identically must leave the log byte-identical (truncate
    // nothing) - else a delayed RPC could drop a committed suffix.
    let mut node = follower_with_log(2, 5, &[(1, 1), (5, 2), (5, 3)]);
    let mut rng = ZeroRng;
    let mut out = Effects::new();
    // Leader (term 5) retransmits the whole log from the start.
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        5,
        NodeId(1),
        0,
        0,
        vec![noop(1, 1), noop(5, 2), noop(5, 3)],
        3,
        &mut out,
    );
    assert_eq!(
        log_terms(&node),
        vec![1, 5, 5],
        "identical retransmit must not alter the log"
    );
}

#[test]
fn conflicting_entry_truncates_from_the_conflict_index() {
    // G2(b): a genuine conflict (same index, different term) truncates from that
    // index and appends the leader's entries.
    let mut node = follower_with_log(2, 5, &[(1, 1), (2, 2), (2, 3)]);
    let mut rng = ZeroRng;
    let mut out = Effects::new();
    // Leader (term 5) has [t1@1, t5@2]; prev (1, t1) matches, entry @2 is t5 != t2.
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        5,
        NodeId(1),
        1,
        1,
        vec![noop(5, 2)],
        0,
        &mut out,
    );
    assert_eq!(
        log_terms(&node),
        vec![1, 5],
        "conflict at index 2 must truncate the stale t2 tail and append t5"
    );
}

#[test]
fn stale_leader_commit_does_not_regress_commit_index() {
    // G2(c): a delayed/duplicate AppendEntries carrying a SMALLER leader_commit
    // must not lower the follower's commit_index.
    let mut node = follower_with_log(2, 5, &[(1, 1), (5, 2), (5, 3)]);
    let mut rng = ZeroRng;
    // First, a fresh leader_commit of 3 commits up to index 3.
    let mut out1 = Effects::new();
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        5,
        NodeId(1),
        3,
        5,
        Vec::new(),
        3,
        &mut out1,
    );
    assert_eq!(node.commit_index(), 3, "commit advances to leader_commit");
    // Then a stale RPC with leader_commit 1: commit must hold at 3.
    let mut out2 = Effects::new();
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        5,
        NodeId(1),
        3,
        5,
        Vec::new(),
        1,
        &mut out2,
    );
    assert_eq!(
        node.commit_index(),
        3,
        "a smaller leader_commit must not regress commit_index"
    );
}

#[test]
fn follower_caps_commit_at_the_vouched_index_not_a_stale_tail() {
    // G3: a follower with a longer STALE tail must commit only up to the last
    // entry THIS RPC vouched for (prev_log_index + entries.len()), never up to
    // its own last_log_index. Also covers G4: the apply hook actually ran
    // (applied_count tracks commit_index), not just the watermark moving.
    let mut node = follower_with_log(2, 5, &[(1, 1), (2, 2), (2, 3)]);
    let mut rng = ZeroRng;
    let mut out = Effects::new();
    // Leader vouches only for index 1 (entries=[t1@1], prev 0) but leader_commit=3.
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        5,
        NodeId(1),
        0,
        0,
        vec![noop(1, 1)],
        3,
        &mut out,
    );
    assert_eq!(
        node.commit_index(),
        1,
        "commit must be capped at the vouched index 1, not the stale tail at 3"
    );
    assert_eq!(
        node.applied_count(),
        1,
        "the apply hook ran for exactly the committed entry"
    );
    assert_eq!(
        node.last_applied(),
        node.commit_index(),
        "last_applied tracks commit_index"
    );
}

#[test]
fn uncommitted_prior_term_entry_is_safely_overwritten() {
    // G1: the OVERWRITE path the CommitLedger guards is reachable and safe. A
    // follower holds an UNCOMMITTED prior-term entry (idx2=t2, commit_index=1).
    // A higher-term leader (term 3) that lacks it sends a conflicting entry at
    // idx2; the follower overwrites idx2=t2 with t3. This is SAFE precisely
    // because t2 was never committed (commit_index stays >= 1 and never covered
    // idx2), which is the section-5.4.2 guarantee: only an entry NOT yet
    // committed can be overwritten.
    let mut node = follower_with_log(2, 2, &[(1, 1), (2, 2)]);
    // Commit only index 1 (idx2=t2 is replicated but NOT committed).
    let mut rng = ZeroRng;
    let mut warm = Effects::new();
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        2,
        NodeId(1),
        1,
        1,
        Vec::new(),
        1,
        &mut warm,
    );
    assert_eq!(
        node.commit_index(),
        1,
        "only index 1 is committed before the leader change"
    );
    // A term-3 leader without idx2's t2 entry conflicts at index 2 with t3.
    let mut out = Effects::new();
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        3,
        NodeId(5),
        1,
        1,
        vec![noop(3, 2)],
        1,
        &mut out,
    );
    assert_eq!(node.current_term(), 3, "the higher term is adopted");
    assert_eq!(
        log_terms(&node),
        vec![1, 3],
        "the uncommitted prior-term entry is overwritten by the new leader"
    );
    assert!(
        node.commit_index() >= 1,
        "commit never regresses; no COMMITTED entry was overwritten (idx1 survives)"
    );
}

// -- HA-9 leader-forwarding: the leader_id passive record ----------------

#[test]
fn leader_id_tracks_accept_and_clears_on_election_start() {
    // HA-9: after accepting a current-term AppendEntries a follower records the
    // sender as its leader; on starting its own election (becoming Candidate) the
    // record clears (the new term's leader is not yet known). leader_id is a
    // PASSIVE record - asserting it here pins the forwarding-routing source without
    // touching any decision.
    let mut node = follower_with_log(2, 5, &[]);
    // This test pins the leader_id passive record across an IMMEDIATE candidate
    // transition, so it drives the legacy (pre-vote OFF) election path -- the PROD-9
    // pre-vote-first timing is exercised by `election_timeout_runs_pre_vote_first_*`.
    node.config.pre_vote = false;
    assert_eq!(node.leader_id(), None, "a fresh follower knows no leader");

    // A valid current-term (term 5) leader (node 1) heartbeats this follower.
    let mut rng = ZeroRng;
    let mut out = Effects::new();
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        5,
        NodeId(1),
        0,
        0,
        Vec::new(),
        0,
        &mut out,
    );
    assert_eq!(
        node.leader_id(),
        Some(NodeId(1)),
        "accepting a current-term AppendEntries records the sender as leader"
    );
    assert_eq!(
        node.role(),
        Role::Follower,
        "still a follower after a heartbeat"
    );

    // The follower's election timer fires: it becomes a Candidate and no longer
    // recognizes any leader.
    let mut out2 = Effects::new();
    let now = Monotonic::from_since_origin(Duration::ZERO);
    node.on_timer(now, &mut rng, ELECTION_TIMEOUT, &mut out2);
    assert_eq!(
        node.role(),
        Role::Candidate,
        "the election timer makes us a candidate"
    );
    assert_eq!(
        node.leader_id(),
        None,
        "starting an election clears the recognized leader"
    );
}

#[test]
fn leader_id_is_self_on_winning_and_clears_on_higher_term_step_down() {
    // HA-9: a node that wins leadership records ITSELF as leader (a forwarded
    // proposal routed to it proposes locally); a later higher-term step-down clears
    // the record (the new term's leader is unknown until its first AppendEntries).
    let mut node = follower_with_log(1, 4, &[]);
    promote_to_leader_for_test(&mut node);
    assert_eq!(
        node.leader_id(),
        Some(NodeId(1)),
        "a freshly elected leader recognizes itself"
    );

    // A higher-term (term 9) AppendEntries from a new leader (node 2) steps us down;
    // observe_term clears the stale self-record before the same-term recognize path
    // re-sets it to the new leader.
    let mut rng = ZeroRng;
    let mut out = Effects::new();
    node.on_append_entries(
        Monotonic::ZERO,
        &mut rng,
        9,
        NodeId(2),
        0,
        0,
        Vec::new(),
        0,
        &mut out,
    );
    assert_eq!(node.current_term(), 9, "the higher term is adopted");
    assert_eq!(node.role(), Role::Follower, "stepped down to follower");
    assert_eq!(
        node.leader_id(),
        Some(NodeId(2)),
        "the new term's accepted leader is recorded after the step-down"
    );
}

// =====================================================================
// 3d: RAFT CLUSTER-MEMBERSHIP (single-server changes + learners, section 6).
//
// Engine-direct unit tests pinning the membership rules (append-time config
// adoption, majority math over the current voter set, learner exclusion,
// one-change-in-flight, persisted-config-survives-restart). The DST
// membership-safety GATE (no two leaders mid-reconfiguration under partition,
// across many seeds) lives further below with the other DST sweeps.
// =====================================================================

/// Drive a single-voter node to leadership (it self-elects instantly), returning it
/// ready to propose. Used by the membership unit tests that grow a cluster from 1.
fn leader_single_voter(id: NodeId) -> RaftNode<MemStorage> {
    let voters: BTreeSet<NodeId> = [id].into_iter().collect();
    let mut node = RaftNode::new(id, voters, MemStorage::new(), RaftConfig::default());
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    node.start(now, &mut rng, &mut Effects::new());
    node.on_timer(now, &mut rng, ELECTION_TIMEOUT, &mut Effects::new());
    assert!(node.is_leader(), "single voter self-elects");
    node
}

/// Propose a membership change directly on an engine (test helper), returning the
/// verdict. Drains nothing (the test inspects state, not the wire).
fn propose_member(node: &mut RaftNode<MemStorage>, change: MembershipChange) -> Option<u64> {
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    node.propose_membership_change(change, now, &mut rng, &mut Effects::new())
}

#[test]
fn add_voter_adopts_config_on_append_and_updates_majority() {
    // A 1-voter leader appends AddVoter(2): it adopts {1,2} ON APPEND (section 6),
    // BEFORE the entry commits. The majority needed jumps from 1 to 2 at once.
    let mut node = leader_single_voter(NodeId(1));
    assert_eq!(node.voters(), &[NodeId(1)].into_iter().collect());
    // Before the change, the single voter has committed its own appends (majority 1).
    let idx = propose_member(&mut node, MembershipChange::AddVoter(NodeId(2)))
        .expect("leader accepts the membership change");
    assert!(idx >= 1);
    // APPEND-TIME ADOPTION: the voter set is {1,2} immediately, not after commit.
    assert_eq!(
        node.voters(),
        &[NodeId(1), NodeId(2)].into_iter().collect(),
        "the new voter is adopted on append (section 6)"
    );
    // The AddVoter entry is NOT yet committed: NodeId(2) is unreachable in this unit
    // test, so a 2-voter majority cannot form. commit_index must NOT have advanced to
    // include the AddVoter entry (the majority math now needs 2, and only the leader
    // has it).
    assert!(
        node.commit_index() < idx,
        "the AddVoter entry is uncommitted (needs the new 2-voter majority)"
    );
}

#[test]
fn remove_voter_adopts_config_on_append() {
    // A 3-voter leader removes a peer; on append it adopts the 2-voter config and the
    // remaining-2 majority (2) governs subsequent commits.
    let voters: BTreeSet<NodeId> = (1..=3).map(NodeId).collect();
    let mut node = RaftNode::new(NodeId(1), voters, MemStorage::new(), RaftConfig::default());
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    node.start(now, &mut rng, &mut Effects::new());
    // Force leadership at term 1 with a self-vote majority is awkward here; drive an
    // election and grant from peers directly. PROD-9: the timeout starts a pre-vote
    // round, so promote through it (grant from node 2) before the real-vote grant.
    time_out_to_candidate(&mut node, NodeId(2), now);
    node.on_message(
        now,
        &mut rng,
        NodeId(2),
        RaftMsg::RequestVoteResp {
            term: node.current_term(),
            vote_granted: true,
        },
        &mut Effects::new(),
    );
    assert!(node.is_leader(), "won with 2 of 3 votes");
    propose_member(&mut node, MembershipChange::RemoveVoter(NodeId(3)))
        .expect("leader accepts the removal");
    assert_eq!(
        node.voters(),
        &[NodeId(1), NodeId(2)].into_iter().collect(),
        "the removed voter is dropped on append (section 6)"
    );
}

#[test]
fn learner_is_added_replicated_to_but_excluded_from_majority() {
    // AddLearner(2) puts 2 in the learner set, NOT the voter set: the single voter
    // remains the whole majority, so the AddLearner entry commits at once (a learner
    // never gates commit). A subsequent client entry also commits with majority 1.
    let mut node = leader_single_voter(NodeId(1));
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    let mut out = Effects::new();
    let idx = node
        .propose_membership_change(
            MembershipChange::AddLearner(NodeId(2)),
            now,
            &mut rng,
            &mut out,
        )
        .expect("leader accepts AddLearner");
    assert_eq!(node.learners(), &[NodeId(2)].into_iter().collect());
    assert_eq!(
        node.voters(),
        &[NodeId(1)].into_iter().collect(),
        "a learner is NOT a voter"
    );
    // The learner does not gate commit: the lone voter is still a majority, so the
    // AddLearner entry committed immediately.
    assert!(
        node.commit_index() >= idx,
        "AddLearner commits at once (learner excluded from majority)"
    );
    // The leader DOES replicate to the learner: a broadcast addresses NodeId(2).
    let addressed_learner = out.sends.iter().any(|(to, _)| *to == NodeId(2));
    assert!(
        addressed_learner,
        "the leader replicates to the learner (AppendEntries addressed to it)"
    );
}

#[test]
fn promote_learner_moves_it_into_the_voter_set() {
    let mut node = leader_single_voter(NodeId(1));
    propose_member(&mut node, MembershipChange::AddLearner(NodeId(2))).expect("AddLearner");
    // Pretend the learner caught up (set its match_index to the leader's last index),
    // so promotion is reasonable; the engine permits promotion regardless (safe).
    let last = node.storage().last_log_index();
    node.match_index.insert(NodeId(2), last);
    assert!(
        node.learner_caught_up(NodeId(2)),
        "the learner is within the catch-up lag gate"
    );
    propose_member(&mut node, MembershipChange::PromoteLearner(NodeId(2))).expect("PromoteLearner");
    assert_eq!(
        node.voters(),
        &[NodeId(1), NodeId(2)].into_iter().collect(),
        "the promoted learner is now a voter"
    );
    assert!(
        node.learners().is_empty(),
        "the promoted learner left the learner set"
    );
}

#[test]
fn remove_learner_drops_it_from_the_learner_set() {
    // RemoveLearner is the symmetric complement of AddLearner: it drops a non-voting learner
    // and is ALWAYS safe (a learner is in no quorum). The voter set is untouched.
    let mut node = leader_single_voter(NodeId(1));
    propose_member(&mut node, MembershipChange::AddLearner(NodeId(2))).expect("AddLearner");
    assert_eq!(node.learners(), &[NodeId(2)].into_iter().collect());
    // A removal is a single-server change subject to one-change-in-flight; the AddLearner above
    // committed at once (a learner never gates commit), so the next change is accepted.
    assert!(!node.membership_change_in_flight());
    propose_member(&mut node, MembershipChange::RemoveLearner(NodeId(2))).expect("RemoveLearner");
    assert!(
        node.learners().is_empty(),
        "RemoveLearner drops the learner from the learner set"
    );
    assert_eq!(
        node.voters(),
        &[NodeId(1)].into_iter().collect(),
        "RemoveLearner leaves the voter set untouched"
    );
}

/// F3 (engine-authoritative no-demote): `apply_membership_delta` must NEVER turn a current VOTER
/// into a learner via `AddLearner` (that would silently shrink the quorum). Tested directly on
/// the pure delta, the engine's single source of truth, so the invariant holds regardless of any
/// caller. A node that is neither a voter nor a learner is still staged as a new learner.
#[test]
fn add_learner_never_demotes_a_current_voter() {
    // AddLearner of an EXISTING VOTER is a NO-OP: the voter stays a voter, no learner is created.
    let mut voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2)].into_iter().collect();
    let mut learners: BTreeSet<NodeId> = BTreeSet::new();
    RaftNode::<MemStorage>::apply_membership_delta(
        &mut voters,
        &mut learners,
        MembershipChange::AddLearner(NodeId(2)),
    );
    assert_eq!(
        voters,
        [NodeId(1), NodeId(2)].into_iter().collect(),
        "AddLearner of a current voter must NOT remove it from the voter set"
    );
    assert!(
        learners.is_empty(),
        "AddLearner of a current voter must NOT stage it as a learner"
    );

    // AddLearner of a NON-member stages it as a new learner (the normal MEET path).
    RaftNode::<MemStorage>::apply_membership_delta(
        &mut voters,
        &mut learners,
        MembershipChange::AddLearner(NodeId(3)),
    );
    assert_eq!(learners, [NodeId(3)].into_iter().collect());
    assert_eq!(voters, [NodeId(1), NodeId(2)].into_iter().collect());

    // AddLearner of an EXISTING LEARNER is idempotent (still one learner, no change).
    RaftNode::<MemStorage>::apply_membership_delta(
        &mut voters,
        &mut learners,
        MembershipChange::AddLearner(NodeId(3)),
    );
    assert_eq!(learners, [NodeId(3)].into_iter().collect());
    assert_eq!(voters, [NodeId(1), NodeId(2)].into_iter().collect());
}

#[test]
fn has_membership_change_in_flight_accessor_mirrors_the_internal_predicate() {
    // The public read-only accessor the production driver consults to distinguish a
    // one-change-in-flight refusal from not-leader. It must equal the internal predicate.
    let mut node = leader_single_voter(NodeId(1));
    assert!(!node.has_membership_change_in_flight());
    // AddVoter(2) needs the new 2-voter majority (2 is unreachable), so it stays uncommitted.
    propose_member(&mut node, MembershipChange::AddVoter(NodeId(2))).expect("AddVoter");
    assert!(node.has_membership_change_in_flight());
    assert_eq!(
        node.has_membership_change_in_flight(),
        node.membership_change_in_flight(),
        "the public accessor mirrors the internal one-change-in-flight predicate"
    );
}

#[test]
fn only_one_membership_change_in_flight() {
    // The section-6 one-change-in-flight rule: while an AddVoter is uncommitted (the
    // new voter is unreachable here, so it cannot commit), a SECOND membership change
    // is REFUSED. A non-membership client proposal is still accepted (only membership
    // is gated).
    let mut node = leader_single_voter(NodeId(1));
    propose_member(&mut node, MembershipChange::AddVoter(NodeId(2)))
        .expect("first change accepted");
    // The AddVoter is uncommitted (needs the new 2-voter majority, which cannot form).
    assert!(node.membership_change_in_flight());
    let second = propose_member(&mut node, MembershipChange::AddVoter(NodeId(3)));
    assert_eq!(
        second, None,
        "a second membership change is refused while one is in flight"
    );
    // A plain client entry is NOT gated by the membership-in-flight rule.
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    let client = node.propose(
        EntryPayload::Bytes(vec![9]),
        now,
        &mut rng,
        &mut Effects::new(),
    );
    assert!(
        client.is_some(),
        "a non-membership proposal is still accepted"
    );
}

#[test]
fn a_learner_does_not_start_elections() {
    // A node whose own log-derived config makes it a LEARNER (here: it is in neither
    // its voters nor... actually a learner) must not campaign on an election timeout.
    // We seed a node that is not a voter in its own config and confirm a timeout does
    // not make it a candidate or inflate the term.
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2)].into_iter().collect();
    // NodeId(3) is NOT in its own seed config (a joining node before it learns it is a
    // voter): it must not start elections (it cannot win and would only churn terms).
    let mut joiner = RaftNode::new(NodeId(3), voters, MemStorage::new(), RaftConfig::default());
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    joiner.start(now, &mut rng, &mut Effects::new());
    let term_before = joiner.current_term();
    joiner.on_timer(now, &mut rng, ELECTION_TIMEOUT, &mut Effects::new());
    assert_eq!(
        joiner.role(),
        Role::Follower,
        "a non-voter does not become a candidate on timeout"
    );
    assert_eq!(
        joiner.current_term(),
        term_before,
        "a non-voter does not inflate the term"
    );
}

#[test]
fn a_learner_vote_does_not_count_toward_the_election_quorum() {
    // A candidate counts only votes FROM CURRENT-CONFIG VOTERS. We build a leader with
    // voters {1,2} and a learner {3}; a granted vote from the learner must NOT count
    // toward the 2-voter majority. Drive an election on NodeId(1) with voters {1,2}
    // and learner {3}: a vote from learner 3 alone (plus self) is NOT a majority of
    // voters (needs 2 voters, has only self=1 voter).
    let mut node = leader_single_voter(NodeId(1));
    // Grow to voters {1,2} + learner {3} via committed changes is heavy here; instead
    // verify the counting helper directly: append AddVoter(2) + AddLearner is awkward
    // without a reachable peer. So assert the rule via maybe_become_leader counting:
    // make NodeId(1) a fresh candidate in a 3-config {1,2} voters + {3} learner.
    // Simplest: a 2-voter candidate that only gets a learner's vote stays a candidate.
    propose_member(&mut node, MembershipChange::AddVoter(NodeId(2))).expect("AddVoter(2)");
    // Now config is voters {1,2} (adopted on append; AddVoter is uncommitted as 2 is
    // unreachable). Re-campaign by hand-seating candidate state, then assert the
    // counting rule: a 2-voter config needs 2 VOTER votes; a learner's vote never tips.
    node.role = Role::Candidate;
    node.votes.clear();
    node.votes.insert(NodeId(1));
    node.maybe_become_leader(Monotonic::ZERO, &mut Effects::new());
    assert_eq!(
        node.role(),
        Role::Candidate,
        "a single voter-vote is not a majority of the 2-voter config"
    );
    // A vote from a NON-voter (e.g. learner id 3) must not tip it over.
    node.votes.insert(NodeId(3));
    node.maybe_become_leader(Monotonic::ZERO, &mut Effects::new());
    assert_eq!(
        node.role(),
        Role::Candidate,
        "a learner's vote does not count toward the voter majority"
    );
    // A genuine second VOTER vote wins.
    node.votes.insert(NodeId(2));
    node.maybe_become_leader(Monotonic::ZERO, &mut Effects::new());
    assert_eq!(node.role(), Role::Leader, "two voter-votes is a majority");
}

#[test]
fn membership_config_survives_restart_via_log() {
    // A node's voter set is DERIVED FROM THE LOG, so it must recover across a restart.
    // Build a single-voter leader, commit AddLearner(2) (commits at once), crash
    // (extract storage), rebuild on it: the recovered node re-derives {learner 2} from
    // the surviving log (no snapshot here, so the whole log is replayed).
    let mut node = leader_single_voter(NodeId(1));
    propose_member(&mut node, MembershipChange::AddLearner(NodeId(2))).expect("AddLearner");
    assert_eq!(node.learners(), &[NodeId(2)].into_iter().collect());
    let RaftNode { storage, .. } = node;
    let voters: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
    let restored = RaftNode::<MemStorage>::new(NodeId(1), voters, storage, RaftConfig::default());
    assert_eq!(
        restored.learners(),
        &[NodeId(2)].into_iter().collect(),
        "the learner config is re-derived from the surviving log on restart"
    );
    assert_eq!(
        restored.voters(),
        &[NodeId(1)].into_iter().collect(),
        "the voter set is re-derived from the log on restart"
    );
}

#[test]
fn membership_config_survives_restart_via_snapshot() {
    // With compaction ON, the AddLearner entry is compacted away; the config baseline
    // persisted beside the snapshot is what a restart restores the config from. A
    // single-voter leader proposes enough entries to cross the threshold + compact,
    // then crashes and rebuilds: the recovered config must still show the learner even
    // though its ConfigChange entry was compacted out of the log.
    let config = RaftConfig {
        snapshot_threshold: 2,
        ..RaftConfig::default()
    };
    let voters: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
    let mut node = RaftNode::new(NodeId(1), voters.clone(), MemStorage::new(), config);
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    node.start(now, &mut rng, &mut Effects::new());
    node.on_timer(now, &mut rng, ELECTION_TIMEOUT, &mut Effects::new());
    // AddLearner(2) commits at once (single voter majority), adopting {learner 2}.
    propose_member(&mut node, MembershipChange::AddLearner(NodeId(2))).expect("AddLearner");
    // Pile on plain entries to cross the snapshot threshold and trigger compaction past
    // the AddLearner index.
    for tag in 0..6u8 {
        node.propose(
            EntryPayload::Bytes(vec![tag]),
            now,
            &mut rng,
            &mut Effects::new(),
        );
    }
    assert!(
        node.storage().log_start_index() > 1,
        "the log compacted past the AddLearner entry"
    );
    assert!(
        node.storage().load_config_baseline().is_some(),
        "the config baseline was persisted beside the snapshot"
    );
    // CRASH + RESTART on the same storage.
    let RaftNode { storage, .. } = node;
    let restored = RaftNode::new(NodeId(1), voters, storage, config);
    assert_eq!(
        restored.learners(),
        &[NodeId(2)].into_iter().collect(),
        "the learner config is restored from the persisted snapshot baseline"
    );
}

#[test]
fn compaction_baseline_excludes_an_uncommitted_configchange_then_reverts_on_truncation() {
    // FIX1 REGRESSION (HA-3d): compaction must capture the COMMITTED config as of the
    // snapshot point, NOT the live `self.voters` (which adopts UNCOMMITTED ConfigChange
    // entries on append). A follower holds an uncommitted AddVoter ABOVE last_applied
    // when compaction fires; that entry is later truncated (it was on a deposed
    // leader's log). The node's config MUST revert to exclude the truncated voter.
    //
    // OLD CODE: baseline = self.voters = {1,2} (folds the uncommitted AddVoter at idx 4)
    // -> after truncation, recompute = {1,2} + empty tail = {1,2}: the change is NEVER
    // un-adopted (the baseline is the recompute floor) -> WRONG majority. This test
    // FAILS on the old code (final voters {1,2}) and PASSES on the fixed code ({1}).
    let config = RaftConfig {
        snapshot_threshold: 2,
        ..RaftConfig::default()
    };
    let voters: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
    let mut node = RaftNode::new(NodeId(1), voters.clone(), MemStorage::new(), config);
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    node.start(now, &mut rng, &mut Effects::new());

    // A term-1 leader (NodeId(9)) ships four entries: plain entries at 1..=3 and an
    // AddVoter(2) ConfigChange at index 4, but advertises leader_commit = 3. So the
    // follower COMMITS only 1..=3 (AddVoter at 4 stays UNCOMMITTED) yet ADOPTS {1,2}
    // on append (append-time adoption). last_applied advances to 3, crossing the
    // snapshot threshold (3 entries above the empty snapshot > 2), so compaction fires
    // at last_included_index = 3 WHILE the uncommitted AddVoter sits at index 4.
    let entries = vec![
        LogEntry {
            term: 1,
            index: 1,
            payload: EntryPayload::Bytes(vec![1]),
        },
        LogEntry {
            term: 1,
            index: 2,
            payload: EntryPayload::Bytes(vec![2]),
        },
        LogEntry {
            term: 1,
            index: 3,
            payload: EntryPayload::Bytes(vec![3]),
        },
        LogEntry {
            term: 1,
            index: 4,
            payload: EntryPayload::ConfigChange(MembershipChange::AddVoter(NodeId(2))),
        },
    ];
    node.on_message(
        now,
        &mut rng,
        NodeId(9),
        RaftMsg::AppendEntries {
            term: 1,
            leader: NodeId(9),
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 3,
        },
        &mut Effects::new(),
    );
    // Append-time adoption took {1,2}, but only 1..=3 are committed.
    assert_eq!(
        node.voters(),
        &[NodeId(1), NodeId(2)].into_iter().collect(),
        "the follower adopts the uncommitted AddVoter on append"
    );
    assert_eq!(node.commit_index(), 3, "only entries 1..=3 are committed");
    assert!(
        node.storage().log_start_index() > 3,
        "compaction fired at the committed/applied watermark (index 3)"
    );
    // THE FIX: the persisted baseline is the COMMITTED config at index 3 ({1}), NOT the
    // live voters ({1,2}). On the old code this baseline would (wrongly) be {1,2}.
    assert_eq!(
        node.storage().load_config_baseline(),
        Some(([NodeId(1)].into_iter().collect(), BTreeSet::new())),
        "the saved baseline is the COMMITTED config at the snapshot point (excludes the \
             uncommitted AddVoter)"
    );

    // The term-1 leader is deposed. A term-2 leader (NodeId(8)) re-replicates index 4
    // with a DIFFERENT (term-2) entry that is NOT a ConfigChange: prev = (3, term 1)
    // still matches the follower's surviving entry at 3, so the consistency check
    // passes; the conflicting index-4 entry TRUNCATES the AddVoter and appends a plain
    // entry. After truncation the surviving log carries NO ConfigChange.
    node.on_message(
        now,
        &mut rng,
        NodeId(8),
        RaftMsg::AppendEntries {
            term: 2,
            leader: NodeId(8),
            prev_log_index: 3,
            prev_log_term: 1,
            entries: vec![LogEntry {
                term: 2,
                index: 4,
                payload: EntryPayload::Bytes(vec![44]),
            }],
            leader_commit: 3,
        },
        &mut Effects::new(),
    );
    // THE HEADLINE: the config REVERTS to exclude the truncated AddVoter. With the bug
    // (baseline {1,2}) this would still report {1,2} -> wrong majority forever.
    assert_eq!(
        node.voters(),
        &[NodeId(1)].into_iter().collect(),
        "the truncated AddVoter is un-adopted: config reverts to the committed baseline"
    );
    assert!(
        node.learners().is_empty(),
        "no spurious learners survive the truncation"
    );
}

#[test]
fn leader_removing_itself_steps_down_after_the_entry_commits() {
    // A 3-voter leader removes ITSELF: on append it adopts the new 2-voter config
    // {2,3}; the removing entry commits on the NEW-config majority (2 of {2,3}); once
    // committed, the leader (no longer a voter) STEPS DOWN (section 6). We drive the
    // two remaining voters' acks directly so the removal commits, then check step-down.
    let voters: BTreeSet<NodeId> = (1..=3).map(NodeId).collect();
    let mut node = RaftNode::new(NodeId(1), voters, MemStorage::new(), RaftConfig::default());
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    node.start(now, &mut rng, &mut Effects::new());
    // PROD-9: the timeout starts a pre-vote round; promote through it (grant from node 2).
    time_out_to_candidate(&mut node, NodeId(2), now);
    node.on_message(
        now,
        &mut rng,
        NodeId(2),
        RaftMsg::RequestVoteResp {
            term: node.current_term(),
            vote_granted: true,
        },
        &mut Effects::new(),
    );
    assert!(node.is_leader(), "won with 2 of 3 votes");
    // The leader proposes its own removal; on append the config is {2,3}.
    let removal_idx = node
        .propose_membership_change(
            MembershipChange::RemoveVoter(NodeId(1)),
            now,
            &mut rng,
            &mut Effects::new(),
        )
        .expect("leader proposes its own removal");
    assert_eq!(
        node.voters(),
        &[NodeId(2), NodeId(3)].into_iter().collect(),
        "on append the leader adopts the new 2-voter config (without itself)"
    );
    assert!(node.is_leader(), "still leader until the removal COMMITS");
    // The two remaining voters ack up to the removal entry: that is a NEW-config
    // majority (2 of {2,3}), so the removal commits and the leader steps down.
    for peer in [NodeId(2), NodeId(3)] {
        node.on_message(
            now,
            &mut rng,
            peer,
            RaftMsg::AppendEntriesResp {
                term: node.current_term(),
                success: true,
                match_index: removal_idx,
            },
            &mut Effects::new(),
        );
    }
    assert!(
        node.commit_index() >= removal_idx,
        "the removal entry committed on the new-config majority"
    );
    assert_eq!(
        node.role(),
        Role::Follower,
        "a leader that committed its own removal steps down (section 6)"
    );
}

// =====================================================================
// 3e: config state-machine apply -> SlotMap (CONTROL_PLANE.md #73).
//
// These exercise the REAL apply: committed ConfigCmd entries replayed onto each
// node's own SlotMap, proving the headline property - LINEARIZABLE SLOT
// OWNERSHIP: no two nodes ever claim the same slot at the same config epoch.
// The engine is unchanged on the replication/commit paths; only the state
// machine seam differs from the CountingSm scenarios above.
// =====================================================================

use ironcache_cluster::{NodeEntry, SlotMap};

/// The fixed `NodeId(u64)` -> SlotMap-string-id mapping the 3e sim adapter
/// commits to. The SlotMap's node identity is a 40-lowercase-hex string (Redis
/// node-id shape, validated by the cluster crate), distinct from the engine's
/// transport `NodeId(u64)`; this is the analog of [`to_sim`] / [`to_raft`] for
/// the cluster layer. A `u64` is at most 16 hex digits, so `{:040x}` zero-pads to
/// exactly 40 lowercase-hex characters - always a valid SlotMap id, and a
/// bijection on the `u64`, so distinct raft ids map to distinct cluster ids.
fn slot_id(id: NodeId) -> String {
    format!("{:040x}", id.0)
}

/// A deterministic advertised endpoint for a node (the SlotMap stores host/port
/// for MOVED redirects; the values are irrelevant to ownership, but must be
/// identical across nodes so the converged node tables match byte-for-byte).
fn slot_host(id: NodeId) -> String {
    format!("10.0.0.{}", id.0)
}

const SLOT_PORT: u16 = 6379;

/// The config state machine (3e): a [`StateMachine`] that replays committed
/// [`ConfigCmd`]s onto an [`ironcache_cluster::SlotMap`].
///
/// Each raft node owns its OWN `ConfigSm`, seeded with `SlotMap::empty_self` for
/// THAT node's id (so `me()` / `owns()` are node-relative), but every node
/// applies the SAME committed `ConfigCmd` sequence in the SAME order. Because
/// `apply` is deterministic and the committed log is byte-identical on every
/// node (Raft's Log Matching + State Machine Safety), the node tables and the
/// slot->owner projection converge to one identical GLOBAL view: that is the
/// linearizable-slot-ownership property under test.
///
/// EPOCH POLICY (CONTROL_PLANE.md line 39, "every committed change advances the
/// epoch"): on every committed slot-OWNERSHIP change (`SetSlotOwner` /
/// `AssignSlots`) the machine calls [`SlotMap::bump_epoch`]. That is the
/// Redis-faithful BUMPEPOCH primitive the cluster crate exposes; it is monotone
/// and deterministic, so `current_epoch()` never decreases and is identical
/// across nodes at any committed point. (BUMPEPOCH is idempotent once a node is
/// already at the max epoch - the Redis "+STILL" reply - so the epoch is
/// monotone-non-decreasing and convergent rather than strictly +1 per change;
/// that is exactly what the no-two-owners-per-epoch and epoch-monotonic checkers
/// require, since identical deterministic apply means all nodes agree on the
/// owner at any shared epoch.) `AddNode` / `RemoveNode` are table-only and do not
/// bump; `SetConfigEpoch` seeds the epoch directly (only valid while alone).
///
/// Mutation errors from the SlotMap (e.g. a `forget` of a slot-owning node) are
/// DETERMINISTIC across nodes (same map state + same command), so swallowing
/// them keeps every node's apply identical; the scenarios are constructed so the
/// committed order never produces a spurious error (AddNode precedes any
/// reference to the node; ownership is moved away before a RemoveNode).
struct ConfigSm {
    map: SlotMap,
    /// A monotonic config epoch driven by the COMMITTED LOG: incremented once per
    /// applied config entry, so it is a deterministic function of the applied
    /// prefix. NOT the SlotMap's Redis-client-facing epoch (whose
    /// bump_epoch/set_config_epoch carry admin-command STILL / guard semantics
    /// that are wrong for a log-driven counter; see apply).
    epoch: u64,
}

impl ConfigSm {
    /// Seed a fresh config state machine for the node `id`: an `empty_self`
    /// SlotMap owning ZERO slots, with this node alone in its table (peers arrive
    /// via committed `AddNode`s). Matches a fresh cluster-enabled node's boot map.
    fn seed(id: NodeId) -> Self {
        ConfigSm {
            map: SlotMap::empty_self(&slot_id(id), &slot_host(id), SLOT_PORT),
            epoch: 0,
        }
    }

    /// Borrow the converged slot map (test inspection).
    fn map(&self) -> &SlotMap {
        &self.map
    }

    /// The monotonic, log-driven config epoch (count of applied config entries).
    fn config_epoch(&self) -> u64 {
        self.epoch
    }
}

impl StateMachine for ConfigSm {
    fn apply(&mut self, entry: &LogEntry) {
        // Only Config payloads touch the slot map; Noop (election no-op) and
        // Bytes (opaque) are no-ops for the config machine, exactly as the engine
        // commits them without interpretation.
        let EntryPayload::Config(cmd) = &entry.payload else {
            return;
        };
        // Every committed config entry advances the monotonic config epoch
        // (CONTROL_PLANE.md line 39). The +1-per-applied-entry counter makes the
        // epoch a DETERMINISTIC FUNCTION OF THE APPLIED PREFIX: two nodes at the
        // same epoch have applied the identical config prefix and therefore agree
        // on every slot's owner (the linearizable-ownership property). We do NOT
        // use SlotMap::bump_epoch / set_config_epoch for this: those carry Redis
        // admin-command semantics (bump returns STILL once my_epoch == maxEpoch;
        // set is rejected once the node knows peers), which are wrong for a
        // log-driven counter and would let distinct ownership states share an
        // epoch (and trip the no-two-owners-per-epoch invariant).
        self.epoch += 1;
        match cmd {
            ConfigCmd::AddNode { id, host, port } => {
                // Idempotent: a node applying AddNode for its OWN id (already in
                // its empty_self table) is a no-op in the cluster crate.
                self.map.meet(NodeEntry {
                    id: id.as_str().into(),
                    host: host.as_str().into(),
                    port: *port,
                });
            }
            ConfigCmd::RemoveNode { id } => {
                // Deterministic across nodes (same table + same command). The
                // scenarios move ownership away first, so this never orphans.
                let _ = self.map.forget(id);
            }
            ConfigCmd::SetSlotOwner { slot, node } => {
                let _ = self.map.set_slot_node(*slot, node);
            }
            ConfigCmd::AssignSlots { node, slots } => {
                for &slot in slots {
                    let _ = self.map.set_slot_node(slot, node);
                }
            }
            ConfigCmd::UnassignSlots { slots } => {
                // The inverse of AssignSlots: clear each slot's owner (owner=UNASSIGNED +
                // mine[] cleared in lockstep) so it is owned by NOBODY. NODE-RELATIVE is
                // automatic (every node clears the same slots) + idempotent. Mirrors the
                // production ConfigSm.
                for &slot in slots {
                    self.map.clear_slot_owner(slot);
                }
            }
            ConfigCmd::AssignReplica { node, slots } => {
                // HA-7d: record `node` as the slot's replica in the parallel structure
                // (deterministic across nodes, like the owner assignment above).
                for &slot in slots {
                    let _ = self.map.set_slot_replica(slot, node);
                }
            }
            ConfigCmd::PromoteReplica { slots, new_primary } => {
                // HA-8 FAILOVER: flip each slot's OWNER to `new_primary` (set_slot_node keeps
                // mine[] in lockstep, so the OLD primary's owns() goes false on apply -- the
                // split-brain fence) and CLEAR `new_primary` from the slot's replica set (it is
                // the owner now). Deterministic + idempotent, like the assignment arms above.
                for &slot in slots {
                    let _ = self.map.set_slot_node(slot, new_primary);
                    self.map.clear_slot_replica(slot, new_primary);
                }
            }
            ConfigCmd::SetSlotMigrating { slot, dest } => {
                // HA-6: NODE-RELATIVE -- only the slot's current OWNER (the SOURCE) carries a
                // MIGRATING tag (committed cmds apply on every node, so the tag must be scoped
                // by `owns()`). Parallel arrays only; owns() unchanged. Mirrors the production
                // ConfigSm.
                if self.map.owns(*slot) {
                    let _ = self.map.set_migrating(*slot, dest);
                }
            }
            ConfigCmd::SetSlotImporting { slot, src, dest } => {
                // HA-6: NODE-RELATIVE -- ONLY the `dest` node carries an IMPORTING tag, gated on
                // `is_self(dest)` (endpoint compare). The old `!owns()` form tagged EVERY non-
                // owner, so a BYSTANDER (a third non-owner that is not the dest) was wrongly
                // tagged too; gating on the dest tags exactly the one importer. Parallel arrays
                // only. Mirrors the production ConfigSm.
                if self.map.is_self(dest) {
                    let _ = self.map.set_importing(*slot, src);
                }
            }
            ConfigCmd::SetSlotStable { slot } => {
                // HA-6: clear the slot's migration state (abort path; idempotent).
                self.map.clear_migration(*slot);
            }
            ConfigCmd::SetConfigEpoch(_epoch) => {
                // The Raft-driven config epoch is the log-driven counter above;
                // the SlotMap's own (Redis-client) epoch is not used for the
                // linearizable-ownership property in 3e.
            }
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        // 3c: the applied config state is the SlotMap committed view + the log-driven
        // epoch counter. Layout [u64 epoch][SlotMap committed-config bytes], the same
        // shape the production ConfigSm uses, so the round-trip + DST tests exercise
        // the real serialization.
        let mut out = self.epoch.to_le_bytes().to_vec();
        out.extend_from_slice(&self.map.serialize_committed());
        out
    }

    fn restore(&mut self, data: &[u8]) {
        // 3c: restore the log-driven epoch counter then rebuild the SlotMap committed
        // view in place (keeping this node's own empty_self identity). A short buffer
        // restores to the cleared baseline (never produced by `snapshot`).
        let (epoch_bytes, map_bytes) = data.split_at(data.len().min(8));
        self.epoch = epoch_bytes
            .get(..8)
            .and_then(|b| <[u8; 8]>::try_from(b).ok())
            .map_or(0, u64::from_le_bytes);
        self.map.restore_committed(map_bytes);
    }
}

// -- config-cluster sim harness (parallel to RaftCluster, ConfigSm-backed) --

/// A [`SimNode`] wrapping a config-state-machine raft node. Mirrors
/// [`RaftSimNode`] exactly (lazy `start`, effects drain), but the engine carries
/// a [`ConfigSm`] instead of the default `CountingSm`, so committed `ConfigCmd`s
/// drive a real `SlotMap`.
///
/// [`SimNode`]: ironcache_sim::SimNode
struct ConfigSimNode {
    engine: RaftNode<MemStorage, ConfigSm>,
    started: bool,
}

impl ConfigSimNode {
    fn new(id: NodeId, voters: BTreeSet<NodeId>, config: RaftConfig) -> Self {
        ConfigSimNode {
            engine: RaftNode::with_state_machine(
                id,
                voters,
                MemStorage::new(),
                config,
                ConfigSm::seed(id),
            ),
            started: false,
        }
    }

    fn ensure_started(&mut self, ctx: &mut SimCtx<'_, RaftMsg>) {
        if self.started {
            return;
        }
        self.started = true;
        let now = ctx.now();
        let mut effects = Effects::new();
        {
            let mut rng = SimRng { ctx };
            self.engine.start(now, &mut rng, &mut effects);
        }
        drain(ctx, effects);
    }
}

impl ironcache_sim::SimNode for ConfigSimNode {
    type Msg = RaftMsg;

    fn on_message(&mut self, from: SimId, msg: RaftMsg, ctx: &mut SimCtx<'_, RaftMsg>) {
        self.ensure_started(ctx);
        let now = ctx.now();
        let mut effects = Effects::new();
        {
            let mut rng = SimRng { ctx };
            self.engine
                .on_message(now, &mut rng, to_raft(from), msg, &mut effects);
        }
        drain(ctx, effects);
    }

    fn on_timer(&mut self, token: u64, ctx: &mut SimCtx<'_, RaftMsg>) {
        self.ensure_started(ctx);
        let now = ctx.now();
        let mut effects = Effects::new();
        {
            let mut rng = SimRng { ctx };
            self.engine.on_timer(now, &mut rng, token, &mut effects);
        }
        drain(ctx, effects);
    }
}

/// A config-cluster harness: `n` voters each with a [`ConfigSm`], built and
/// bootstrapped exactly as [`RaftCluster`]. Exposes the role/term/commit reads
/// the scenarios need plus SlotMap projections per node.
struct ConfigCluster {
    net: Network<ConfigSimNode>,
    ids: Vec<NodeId>,
}

impl ConfigCluster {
    fn new(n: u64, seed: u64, config: RaftConfig) -> Self {
        let ids: Vec<NodeId> = (1..=n).map(NodeId).collect();
        let voters: BTreeSet<NodeId> = ids.iter().copied().collect();
        let mut net = Network::new(seed);
        for &id in &ids {
            net.add_node(to_sim(id), ConfigSimNode::new(id, voters.clone(), config));
        }
        let mut cluster = ConfigCluster { net, ids };
        cluster.start_all();
        cluster
    }

    /// Bootstrap every node (same harmless term-0 self-AppendEntries trigger as
    /// [`RaftCluster::start_all`]).
    fn start_all(&mut self) {
        for &id in &self.ids {
            self.net.tell(
                to_sim(id),
                to_sim(id),
                RaftMsg::AppendEntries {
                    term: 0,
                    leader: id,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                },
            );
        }
    }

    fn engine(&self, id: NodeId) -> &RaftNode<MemStorage, ConfigSm> {
        &self.net.node(to_sim(id)).expect("node exists").engine
    }

    /// Mutable engine access, for a test driver that must reach INTO a node between
    /// steps (PROD-9: simulating a follower restart that drops its volatile
    /// chunked-snapshot receive buffer).
    fn engine_mut(&mut self, id: NodeId) -> &mut RaftNode<MemStorage, ConfigSm> {
        &mut self.net.node_mut(to_sim(id)).expect("node exists").engine
    }

    /// PROD-9: drop a follower's IN-PROGRESS chunked-snapshot receive buffer, modeling a
    /// node restart mid-transfer (the buffer is VOLATILE, lost on a crash). The leader
    /// then restarts the transfer from offset 0 (the follower rejects the next non-zero
    /// chunk and replies `next_offset == 0`). Persistent storage is untouched, as a real
    /// restart keeps it.
    fn drop_snapshot_rx(&mut self, id: NodeId) {
        self.engine_mut(id).snapshot_rx = None;
    }

    /// PROD-9: the byte length of `id`'s persisted snapshot (0 if none). Used to assert
    /// a multi-chunk transfer actually spanned several chunks (snapshot len > chunk size).
    fn snapshot_len(&self, id: NodeId) -> usize {
        self.engine(id)
            .storage()
            .load_snapshot()
            .map_or(0, |(_, data)| data.len())
    }

    fn role(&self, id: NodeId) -> Role {
        self.engine(id).role()
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.ids
            .iter()
            .copied()
            .filter(|&id| self.role(id) == Role::Leader)
            .collect()
    }

    fn commit_index(&self, id: NodeId) -> u64 {
        self.engine(id).commit_index()
    }

    fn log(&self, id: NodeId) -> Vec<LogEntry> {
        self.engine(id).storage().log().to_vec()
    }

    /// The node's converged SlotMap (via the state-machine accessor).
    fn map(&self, id: NodeId) -> &SlotMap {
        self.engine(id).state_machine().map()
    }

    fn current_epoch(&self, id: NodeId) -> u64 {
        // The Raft-driven, log-monotonic config epoch (a deterministic function
        // of the applied prefix), NOT the SlotMap's Redis-client epoch.
        self.engine(id).state_machine().config_epoch()
    }

    /// The 1-based index of the FIRST entry still in `id`'s log (HA-3c). `> 1` means
    /// the node has compacted a prefix (the snapshot subsumes it).
    fn log_start(&self, id: NodeId) -> u64 {
        self.engine(id).storage().log_start_index()
    }

    /// Whether `id` holds a persisted snapshot (HA-3c).
    fn has_snapshot(&self, id: NodeId) -> bool {
        self.engine(id).storage().load_snapshot().is_some()
    }

    /// The node's slot->owner-string projection: for each ASSIGNED slot, the
    /// 40-hex id of its owner. The directly comparable global ownership view (the
    /// `ranges()` shape carries node INDICES that differ per node's table order,
    /// so we resolve to owner IDs for a node-independent comparison).
    fn owner_by_slot(&self, id: NodeId) -> BTreeMap<u16, String> {
        let map = self.map(id);
        let nodes = map.nodes();
        let mut out = BTreeMap::new();
        for (start, end, node_idx) in map.ranges() {
            let owner = nodes[node_idx].id.to_string();
            for slot in start..=end {
                out.insert(slot, owner.clone());
            }
        }
        out
    }

    fn propose(&mut self, leader: NodeId, cmd: ConfigCmd) {
        self.net.tell(
            to_sim(leader),
            to_sim(leader),
            RaftMsg::Propose {
                payload: EntryPayload::Config(cmd),
            },
        );
    }

    fn run_steps(&mut self, n: usize) -> usize {
        self.net.run_steps(n)
    }

    fn run_until_idle(&mut self, max_steps: usize) -> usize {
        self.net.run_until_idle(max_steps)
    }
}

// -- 3e checkers: linearizable slot ownership + epoch monotonicity ----------

/// LINEARIZABLE SLOT OWNERSHIP (the headline #73 property): across all nodes,
/// for each slot, no two nodes report a DIFFERENT owner while at the SAME
/// `current_epoch()`.
///
/// Because committed entries are byte-identical and `ConfigSm::apply` is
/// deterministic, every node at a given committed epoch holds the same
/// owner-per-slot; this checker proves that empirically and would catch an apply
/// bug (a node mis-applying a `ConfigCmd` would expose a divergent owner at a
/// shared epoch). It groups (slot, epoch) -> set-of-owners and asserts each group
/// is a singleton.
fn assert_no_two_owners_per_epoch(cluster: &ConfigCluster) {
    // (slot, epoch) -> the owner id first seen, plus the node that reported it.
    let mut seen: BTreeMap<(u16, u64), (String, NodeId)> = BTreeMap::new();
    for &id in &cluster.ids {
        let epoch = cluster.current_epoch(id);
        for (slot, owner) in cluster.owner_by_slot(id) {
            match seen.get(&(slot, epoch)) {
                Some((prev_owner, prev_node)) => assert_eq!(
                    prev_owner, &owner,
                    "linearizable-ownership violated: slot {slot} at epoch {epoch} is owned by \
                         {prev_owner} per node {prev_node:?} but by {owner} per node {id:?}"
                ),
                None => {
                    seen.insert((slot, epoch), (owner, id));
                }
            }
        }
    }
}

/// EPOCH MONOTONICITY: no node's `current_epoch()` ever decreases across
/// observations. Sampled against a running per-node high-water map; each call
/// asserts the current epoch is `>=` the highest previously seen for that node,
/// then records the new high-water.
#[derive(Default)]
struct EpochMonotonic {
    high_water: BTreeMap<NodeId, u64>,
}

impl EpochMonotonic {
    fn new() -> Self {
        Self::default()
    }

    fn observe(&mut self, cluster: &ConfigCluster) {
        for &id in &cluster.ids {
            let epoch = cluster.current_epoch(id);
            let hw = self.high_water.entry(id).or_insert(0);
            assert!(
                epoch >= *hw,
                "epoch monotonicity violated: node {id:?} epoch went {hw} -> {epoch}"
            );
            *hw = epoch;
        }
    }
}

/// Run both 3e checkers at a quiescent point (the epoch-monotonic one against a
/// supplied tracker so it spans the whole scenario).
fn assert_3e_invariants(cluster: &ConfigCluster, epochs: &mut EpochMonotonic) {
    assert_no_two_owners_per_epoch(cluster);
    epochs.observe(cluster);
}

/// Elect a single leader on a config cluster and settle the election no-op.
fn elect_config_leader(cluster: &mut ConfigCluster) -> NodeId {
    for _ in 0..200 {
        cluster.run_steps(500);
        let leaders = cluster.leaders();
        if leaders.len() == 1 {
            cluster.run_steps(5_000);
            return leaders[0];
        }
    }
    panic!("config cluster did not converge to a single leader");
}

/// Assert every node's committed log is a consistent prefix of the leader's (no
/// committed config change is lost or reordered): for each node, every entry up
/// to its commit_index equals the leader's entry at that index.
fn assert_committed_prefix_agrees(cluster: &ConfigCluster, leader: NodeId) {
    let leader_log = cluster.log(leader);
    for &id in &cluster.ids {
        let ci = cluster.commit_index(id);
        let log = cluster.log(id);
        for idx in 1..=ci {
            let pos = usize::try_from(idx - 1).unwrap();
            assert_eq!(
                log.get(pos),
                leader_log.get(pos),
                "node {id:?} committed index {idx} disagrees with the leader's log \
                     (a committed config change was lost or reordered)"
            );
        }
    }
}

// -- scenario H: config applies and converges (partition + heal) -----------

#[test]
fn config_applies_and_converges() {
    // 5 voters. Elect a leader; propose AddNode for every peer, then assign the
    // slot space across the nodes, partition the leader off and heal, and assert
    // every node's SlotMap projection is IDENTICAL at the final committed epoch,
    // the epoch is monotone everywhere, and no-two-owners holds throughout.
    let mut cluster = ConfigCluster::new(5, 101, RaftConfig::default());
    let mut epochs = EpochMonotonic::new();
    let leader = elect_config_leader(&mut cluster);
    assert_3e_invariants(&cluster, &mut epochs);

    // AddNode every node (including the leader's own id; meet is idempotent on
    // self). Committed BEFORE any slot assignment references them.
    for id in cluster.ids.clone() {
        cluster.propose(
            leader,
            ConfigCmd::AddNode {
                id: slot_id(id),
                host: slot_host(id),
                port: SLOT_PORT,
            },
        );
        cluster.run_steps(1_500);
        assert_3e_invariants(&cluster, &mut epochs);
    }

    // Assign the 16384-slot space in contiguous bands, one band per node, via a
    // mix of AssignSlots (batches) and one SetSlotOwner (single slot), so both
    // apply paths are exercised.
    let n = cluster.ids.len() as u32;
    let band = u32::from(ironcache_cluster::CLUSTER_SLOTS) / n;
    for (k, id) in cluster.ids.clone().into_iter().enumerate() {
        let start = (k as u32) * band;
        let end = if k + 1 == cluster.ids.len() {
            u32::from(ironcache_cluster::CLUSTER_SLOTS) - 1
        } else {
            start + band - 1
        };
        let slots: Vec<u16> = (start..=end).map(|s| s as u16).collect();
        // Assign all but the last slot of the band as a batch, the last as a
        // single SetSlotOwner.
        let (head, tail) = slots.split_at(slots.len() - 1);
        cluster.propose(
            leader,
            ConfigCmd::AssignSlots {
                node: slot_id(id),
                slots: head.to_vec(),
            },
        );
        cluster.propose(
            leader,
            ConfigCmd::SetSlotOwner {
                slot: tail[0],
                node: slot_id(id),
            },
        );
        cluster.run_steps(2_000);
        assert_3e_invariants(&cluster, &mut epochs);
    }

    // A SetConfigEpoch is a no-op here (the leader knows other nodes, so the
    // SlotMap rejects it deterministically on every node); include it to prove
    // the command is handled uniformly and does not perturb convergence.
    cluster.propose(leader, ConfigCmd::SetConfigEpoch(99));
    cluster.run_steps(1_500);
    assert_3e_invariants(&cluster, &mut epochs);

    // Partition the leader off; a new leader rises and keeps committing nothing
    // new here (we stop proposing), then heal and let everything reconcile.
    let others: Vec<SimId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != leader)
        .map(to_sim)
        .collect();
    cluster.net.partition(&[to_sim(leader)], &others);
    for _ in 0..40 {
        cluster.run_steps(500);
        assert_3e_invariants(&cluster, &mut epochs);
    }
    cluster.net.heal();
    cluster.run_until_idle(200_000);
    assert_3e_invariants(&cluster, &mut epochs);

    // Final state: every node's slot->owner projection is IDENTICAL, the whole
    // space is assigned, and the epoch agrees everywhere.
    let reference = cluster.owner_by_slot(cluster.ids[0]);
    assert_eq!(
        reference.len(),
        usize::from(ironcache_cluster::CLUSTER_SLOTS),
        "the full slot space must be assigned after convergence"
    );
    let ref_epoch = cluster.current_epoch(cluster.ids[0]);
    for &id in &cluster.ids {
        assert_eq!(
            cluster.owner_by_slot(id),
            reference,
            "node {id:?} slot->owner projection must match every other node's"
        );
        assert_eq!(
            cluster.current_epoch(id),
            ref_epoch,
            "node {id:?} config epoch must match every other node's once converged"
        );
    }
    assert_no_two_owners_per_epoch(&cluster);
}

// -- scenario I: slot ownership under partition (THE headline gate) ---------

/// Replay scenario I for one `seed`: a migration-shaped SetSlotOwner sequence
/// proposed while the cluster is partitioned, then healed. Returns the final
/// per-node owner-by-slot snapshot + epoch so a seed sweep can compare, and runs
/// the no-two-owners + epoch-monotonic checkers throughout.
fn run_slot_ownership_under_partition(seed: u64) -> Vec<(BTreeMap<u16, String>, u64)> {
    let mut cluster = ConfigCluster::new(5, seed, RaftConfig::default());
    cluster
        .net
        .set_latency(Duration::from_millis(1), Duration::from_millis(15));
    let mut epochs = EpochMonotonic::new();
    let leader = elect_config_leader(&mut cluster);

    // Build the node table first (committed before any slot reference).
    for id in cluster.ids.clone() {
        cluster.propose(
            leader,
            ConfigCmd::AddNode {
                id: slot_id(id),
                host: slot_host(id),
                port: SLOT_PORT,
            },
        );
    }
    cluster.run_steps(5_000);
    assert_3e_invariants(&cluster, &mut epochs);

    // Claim a handful of slots for the leader, then run a MIGRATION-shaped
    // sequence (re-home each slot to successive nodes) WHILE PARTITIONED: the
    // majority side keeps committing the ownership flips; the minority cannot.
    let slots: [u16; 4] = [0, 4096, 8192, 12288];
    for &s in &slots {
        cluster.propose(
            leader,
            ConfigCmd::SetSlotOwner {
                slot: s,
                node: slot_id(leader),
            },
        );
    }
    cluster.run_steps(3_000);
    assert_3e_invariants(&cluster, &mut epochs);

    // Partition: leader + one follower as the minority (2 of 5, cannot commit);
    // the other three are the majority and elect their own leader.
    let minority_follower = *cluster
        .ids
        .iter()
        .find(|&&id| id != leader)
        .expect("a follower exists");
    let minority: Vec<SimId> = [leader, minority_follower]
        .iter()
        .copied()
        .map(to_sim)
        .collect();
    let majority: Vec<SimId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != leader && id != minority_follower)
        .map(to_sim)
        .collect();
    cluster.net.partition(&minority, &majority);

    // The majority elects a leader; migrate each slot to a NEW owner on that side.
    let mut maj_leader = None;
    for _ in 0..200 {
        cluster.run_steps(500);
        assert_3e_invariants(&cluster, &mut epochs);
        let ml: Vec<NodeId> = cluster
            .leaders()
            .into_iter()
            .filter(|id| majority.contains(&to_sim(*id)))
            .collect();
        if ml.len() == 1 {
            maj_leader = Some(ml[0]);
            break;
        }
    }
    let maj_leader = maj_leader.expect("the majority side must elect a leader");
    // Migrate every slot to the majority leader (the migration-shaped change
    // that must never produce two owners at one epoch).
    for &s in &slots {
        cluster.propose(
            maj_leader,
            ConfigCmd::SetSlotOwner {
                slot: s,
                node: slot_id(maj_leader),
            },
        );
    }
    for _ in 0..40 {
        cluster.run_steps(500);
        assert_3e_invariants(&cluster, &mut epochs);
    }

    // Heal; the minority side adopts the majority's committed config. Sample the
    // checkers throughout the reconciliation.
    cluster.net.heal();
    for _ in 0..80 {
        cluster.run_steps(500);
        assert_3e_invariants(&cluster, &mut epochs);
    }
    cluster.run_until_idle(200_000);
    assert_3e_invariants(&cluster, &mut epochs);

    // No committed config change is lost: every node's committed prefix agrees
    // with the (final) majority leader's log.
    let final_leader = {
        let ls = cluster.leaders();
        assert_eq!(ls.len(), 1, "exactly one leader after heal");
        ls[0]
    };
    assert_committed_prefix_agrees(&cluster, final_leader);

    cluster
        .ids
        .iter()
        .map(|&id| (cluster.owner_by_slot(id), cluster.current_epoch(id)))
        .collect()
}

#[test]
fn slot_ownership_under_partition() {
    // THE headline gate, across a seed sweep: no epoch ever shows two owners for
    // one slot (asserted inside the run via assert_no_two_owners_per_epoch every
    // chunk) and, after heal, all nodes converge to one ownership view with no
    // committed change lost.
    for seed in 0..30u64 {
        let snaps = run_slot_ownership_under_partition(seed);
        let (ref_owner, ref_epoch) = &snaps[0];
        for (owner, epoch) in &snaps {
            assert_eq!(
                owner, ref_owner,
                "seed {seed}: all nodes must converge to one slot->owner view after heal"
            );
            assert_eq!(
                epoch, ref_epoch,
                "seed {seed}: all nodes must agree on the config epoch after heal"
            );
        }
        // The migration landed: every probed slot is owned (by the same node on
        // every replica, already asserted above).
        for s in [0u16, 4096, 8192, 12288] {
            assert!(
                ref_owner.contains_key(&s),
                "seed {seed}: migrated slot {s} must have an owner after convergence"
            );
        }
    }
}

// =====================================================================
// HA-8: FAILOVER (promotion) -- THE SPLIT-BRAIN GATE.
//
// A committed PromoteReplica transfers a slot's ownership from a (dead) primary
// to an in-sync replica. The danger is SPLIT-BRAIN (two owners of a slot) and
// DATA LOSS (promoting a stale replica). These scenarios prove the APPLY-side
// fence: across an entire partition/heal failover timeline, NO two nodes ever
// `owns()` a slot at the SAME committed state, and the epoch advances on the
// promotion. The pure engine here has no replication link, so it always feeds an
// in-sync candidate; the DATA-LOSS half (a too-stale replica is NEVER proposed)
// is the LAG GATE, proven directly where it lives by the unit test
// `ironcache::replica_attach::tests::promotion_proposal_lag_gate_refuses_a_stale_replica`.
// =====================================================================

/// THE SPLIT-BRAIN ASSERTION (the merge-blocker): NO two nodes ever have
/// `owns()==true` for the same slot AT THE SAME CONFIG EPOCH. It asserts the
/// `owns()` PROPERTY but evaluates it via each node's COLD `owner_by_slot`
/// projection (the `owner[]` array, coalesced from `ranges()`), which is
/// equivalent to the hot `mine[]`/`owns()` bitmap by the separately-tested
/// owner/`mine[]` lockstep invariant -- and O(assigned) per node, so the
/// thousands-of-timelines sweep stays fast.
///
/// This is the rigorous form of THE FENCE (CONTROL_PLANE.md / the HA-8 design point
/// 2): the config epoch advances on every committed ownership change, so two nodes
/// at the SAME epoch have applied the identical committed prefix and therefore agree
/// on every slot's single owner. The qualifier "at the same committed state" is the
/// EPOCH: a client (or node) at epoch E always sees exactly one owner of a slot.
///
/// Why epoch-keyed and not unconditional: during an ACTIVE partition a stale
/// minority node sits at an EARLIER epoch (it cannot commit), still showing
/// `owns()==true` for a slot it last owned, while the majority commits a
/// PromoteReplica that gives a NEW owner the slot at a HIGHER epoch. That transient
/// is NOT split-brain -- the two believe they own at DIFFERENT epochs, and a client
/// touching the stale node gets MOVED carrying the OLD epoch (the system as a whole
/// has advanced). The DANGEROUS thing -- two owners a client could see as
/// simultaneously authoritative -- is exactly two owners AT ONE EPOCH, which this
/// forbids. (The post-heal convergence to ONE global owner is asserted separately,
/// unconditionally, once every node has caught its log up.)
///
/// Because each node's `ConfigSm` is seeded `empty_self` for THAT node's id,
/// `map(id).owns(slot)` is true iff node `id` is the slot's owner in its OWN
/// committed view; this scans all nodes and groups self-owned slots by (slot, epoch).
///
/// It iterates each node's `owner_by_slot()` projection (the ASSIGNED slots only,
/// coalesced from `ranges()`) rather than all 16384 raw slots, and counts a slot as
/// owned by `id` only when that node's OWN view resolves the owner to `id` (i.e.
/// `owns()` would be true) -- the same fact, but O(assigned) per node so the
/// thousands-of-timelines sweep stays fast. A slot owned by two nodes at one epoch
/// is exactly the split-brain a failover must never produce.
fn assert_at_most_one_owner_per_slot(cluster: &ConfigCluster) {
    // (slot, epoch) -> the (single) node observed to own it at that epoch.
    let mut owner_of: BTreeMap<(u16, u64), NodeId> = BTreeMap::new();
    for &id in &cluster.ids {
        let epoch = cluster.current_epoch(id);
        let self_id = slot_id(id);
        // owner_by_slot resolves each ASSIGNED slot to its owner's 40-hex id in THIS
        // node's view; a slot whose owner is `self_id` is one this node `owns()`.
        for (slot, owner) in cluster.owner_by_slot(id) {
            if owner != self_id {
                continue; // this node does not own the slot in its own view.
            }
            if let Some(&other) = owner_of.get(&(slot, epoch)) {
                panic!(
                    "SPLIT-BRAIN: slot {slot} is owns()==true on BOTH node {other:?} and \
                         node {id:?} at the SAME config epoch {epoch} (two owners at one \
                         committed state)"
                );
            }
            owner_of.insert((slot, epoch), id);
        }
    }
}

/// The UNCONDITIONAL post-convergence form: once the cluster has fully healed and
/// every node has caught its committed log up, NO slot may be `owns()==true` on more
/// than one node, FULL STOP (every node is at the same final epoch, so this is the
/// epoch-keyed property collapsed to one epoch). Called only at the end of a
/// scenario, after `run_until_idle`, to prove the failover converged to ONE owner.
fn assert_exactly_one_owner_after_convergence(cluster: &ConfigCluster) {
    let mut owner_of: BTreeMap<u16, NodeId> = BTreeMap::new();
    for &id in &cluster.ids {
        let self_id = slot_id(id);
        for (slot, owner) in cluster.owner_by_slot(id) {
            if owner != self_id {
                continue; // not self-owned in this node's view.
            }
            if let Some(&other) = owner_of.get(&slot) {
                panic!(
                    "POST-HEAL SPLIT-BRAIN: slot {slot} is owns()==true on BOTH node \
                         {other:?} and node {id:?} after full convergence (the failover did \
                         not converge to one owner)"
                );
            }
            owner_of.insert(slot, id);
        }
    }
}

/// Elect a single leader CONFINED to `group` (a partitioned side). Returns it, or
/// `None` if the side does not elect one within the budget. Asserts the
/// split-brain invariant at every chunk so a bad promotion mid-election trips.
fn run_to_leader_in_group(
    cluster: &mut ConfigCluster,
    group: &[SimId],
    max_rounds: usize,
) -> Option<NodeId> {
    for _ in 0..max_rounds {
        cluster.run_steps(500);
        assert_at_most_one_owner_per_slot(cluster);
        let ls: Vec<NodeId> = cluster
            .leaders()
            .into_iter()
            .filter(|id| group.contains(&to_sim(*id)))
            .collect();
        if ls.len() == 1 {
            return Some(ls[0]);
        }
    }
    None
}

/// Replay the HA-8 split-brain failover gate for one `seed` with `partition_after`
/// controlling WHEN (in chunks) the owner is partitioned away, so the seed sweep
/// randomizes partition timing. Returns the final per-node owner-by-slot snapshot +
/// epoch (for the seed-sweep convergence assertion). The split-brain checker
/// (`assert_at_most_one_owner_per_slot`) and the epoch-monotonic checker run at
/// EVERY quiescent step throughout.
///
/// The lag gate is modeled at the DECISION LEVEL: the pure engine has no
/// replication link, so the test plays the role of HA-8's failover controller and
/// promotes ONLY the replica it has marked in-sync (`in_sync_replica`); it asserts
/// a too-stale replica (`stale_replica`) is NEVER named in a promotion. This is the
/// same gate `replica_is_in_sync` enforces in production (the controller proposes
/// PromoteReplica only for an in-sync candidate); here we prove the COMMITTED
/// PROMOTION itself is split-brain-safe given that gate.
#[allow(clippy::too_many_lines)]
fn run_failover_split_brain_gate(
    seed: u64,
    partition_after: usize,
) -> Vec<(BTreeMap<u16, String>, u64)> {
    // 3 voters: this is the N=3 cluster the gate spec calls for. The OWNER is the
    // first leader; the IN-SYNC replica + the stale replica are the other two.
    let mut cluster = ConfigCluster::new(3, seed, RaftConfig::default());
    cluster
        .net
        .set_latency(Duration::from_millis(1), Duration::from_millis(15));
    let mut epochs = EpochMonotonic::new();
    let owner = elect_config_leader(&mut cluster);
    assert_3e_invariants(&cluster, &mut epochs);
    assert_at_most_one_owner_per_slot(&cluster);

    // Build the node table (committed before any slot/replica reference).
    for id in cluster.ids.clone() {
        cluster.propose(
            owner,
            ConfigCmd::AddNode {
                id: slot_id(id),
                host: slot_host(id),
                port: SLOT_PORT,
            },
        );
    }
    cluster.run_steps(3_000);
    assert_3e_invariants(&cluster, &mut epochs);

    // The slots under failover. The OWNER claims them; ONE peer is the in-sync
    // replica (promotable), the OTHER is a deliberately-stale replica (must NOT be
    // promoted -- the lag gate). The peers are the two non-owner ids.
    let slots: [u16; 3] = [0, 6000, 12000];
    let peers: Vec<NodeId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != owner)
        .collect();
    let in_sync_replica = peers[0];
    let stale_replica = peers[1];
    for &s in &slots {
        cluster.propose(
            owner,
            ConfigCmd::SetSlotOwner {
                slot: s,
                node: slot_id(owner),
            },
        );
    }
    // Record BOTH peers as replicas of the slots (AssignReplica). They are equal in
    // the committed map; the lag gate (which one is in-sync) lives in the failover
    // controller's decision, modeled below.
    cluster.propose(
        owner,
        ConfigCmd::AssignReplica {
            node: slot_id(in_sync_replica),
            slots: slots.to_vec(),
        },
    );
    cluster.propose(
        owner,
        ConfigCmd::AssignReplica {
            node: slot_id(stale_replica),
            slots: slots.to_vec(),
        },
    );
    cluster.run_steps(3_000);
    assert_3e_invariants(&cluster, &mut epochs);
    assert_at_most_one_owner_per_slot(&cluster);

    // The pre-promotion epoch (every node agrees once converged; sample the owner's).
    let pre_promotion_epoch = cluster.current_epoch(owner);

    // Let the cluster run a randomized number of chunks BEFORE the partition, so the
    // partition lands at different points relative to in-flight replication.
    for _ in 0..partition_after {
        cluster.run_steps(200);
        assert_at_most_one_owner_per_slot(&cluster);
    }

    // PARTITION the owner away from the other two (the majority). The owner (1 of 3)
    // cannot commit; the two-node majority elects a leader and runs the failover.
    let majority: Vec<SimId> = peers.iter().copied().map(to_sim).collect();
    cluster.net.partition(&[to_sim(owner)], &majority);

    // The majority elects a leader. The failover controller (this test) PROMOTES the
    // IN-SYNC replica -- NEVER the stale one (the lag gate). A spurious promotion
    // (the owner is actually alive across the partition) is SAFE for split-brain: the
    // committed entry atomically transfers ownership and the old owner steps down on
    // apply (proven by the checker, which runs through the whole timeline).
    let maj_leader = run_to_leader_in_group(&mut cluster, &majority, 200)
        .expect("the majority side must elect a leader");

    // THE PROMOTION: name the IN-SYNC replica as the new primary of the slots. The
    // lag gate is the choice of `in_sync_replica` here; assert we never name the
    // stale one.
    let new_primary = in_sync_replica;
    assert_ne!(
        new_primary, stale_replica,
        "the lag gate must never promote the too-stale replica"
    );
    cluster.propose(
        maj_leader,
        ConfigCmd::PromoteReplica {
            slots: slots.to_vec(),
            new_primary: slot_id(new_primary),
        },
    );
    for _ in 0..25 {
        cluster.run_steps(500);
        assert_at_most_one_owner_per_slot(&cluster);
        assert_3e_invariants(&cluster, &mut epochs);
    }

    // The new primary, on the majority side, now OWNS the slots (committed there).
    for &s in &slots {
        assert!(
            cluster.map(new_primary).owns(s),
            "seed {seed}: the promoted in-sync replica must own slot {s} after the commit"
        );
    }
    // The new owner's epoch advanced past the pre-promotion epoch (the fence's epoch bump).
    assert!(
        cluster.current_epoch(new_primary) > pre_promotion_epoch,
        "seed {seed}: the new owner's epoch ({}) must exceed the pre-promotion epoch ({})",
        cluster.current_epoch(new_primary),
        pre_promotion_epoch
    );

    // HEAL. The OLD primary, catching its Raft log up, applies the committed
    // PromoteReplica: its `owns()` for the slots goes FALSE (it serves MOVED). The
    // split-brain checker runs through the entire reconciliation.
    cluster.net.heal();
    for _ in 0..30 {
        cluster.run_steps(500);
        assert_at_most_one_owner_per_slot(&cluster);
        assert_3e_invariants(&cluster, &mut epochs);
    }
    cluster.run_until_idle(100_000);
    assert_at_most_one_owner_per_slot(&cluster);
    assert_3e_invariants(&cluster, &mut epochs);
    // Post-heal: every node has caught its committed log up, so there is now EXACTLY
    // one owner per slot across the whole cluster (the epoch-keyed transient is gone).
    assert_exactly_one_owner_after_convergence(&cluster);

    // The OLD primary lost ownership (the fence) and now resolves MOVED to the new
    // owner's endpoint for every promoted slot.
    for &s in &slots {
        assert!(
            !cluster.map(owner).owns(s),
            "seed {seed}: the OLD primary must lose ownership of slot {s} after heal (MOVED)"
        );
        let moved = cluster.map(owner).moved_target(s);
        assert_eq!(
            moved,
            Some((slot_host(new_primary), SLOT_PORT)),
            "seed {seed}: the old primary must MOVED slot {s} to the new owner's endpoint"
        );
    }
    // No committed change was lost: every node's committed prefix agrees with the leader's.
    let final_leader = {
        let ls = cluster.leaders();
        assert_eq!(ls.len(), 1, "seed {seed}: exactly one leader after heal");
        ls[0]
    };
    assert_committed_prefix_agrees(&cluster, final_leader);

    cluster
        .ids
        .iter()
        .map(|&id| (cluster.owner_by_slot(id), cluster.current_epoch(id)))
        .collect()
}

#[test]
fn failover_split_brain_gate() {
    // THE MERGE-BLOCKER, across thousands of (seed, partition-timing) pairs. For
    // each seed we vary `partition_after` so the owner is isolated at different
    // points in the timeline (randomized partition timing). Throughout EVERY run:
    // - the split-brain checker (`assert_at_most_one_owner_per_slot`) runs at every
    //   quiescent step -> two simultaneous owners would panic immediately;
    // - the epoch is monotone everywhere and the new owner's epoch exceeds the
    //   pre-promotion epoch;
    // - the lag gate never promotes the stale replica;
    // and after heal all nodes converge to ONE ownership view (the old primary
    // having lost the slots and MOVED to the new owner).
    //
    // 200 seeds x 5 partition-timing offsets = 1000 distinct failover timelines,
    // each scanning all 3 nodes' ASSIGNED slots (epoch-keyed) for two owners at
    // every quiescent chunk across the partition/heal timeline.
    for seed in 0..200u64 {
        for partition_after in 0..5usize {
            let snaps = run_failover_split_brain_gate(seed, partition_after);
            let (ref_owner, ref_epoch) = &snaps[0];
            for (owner, epoch) in &snaps {
                assert_eq!(
                    owner, ref_owner,
                    "seed {seed}/{partition_after}: all nodes must converge to one \
                         slot->owner view after the failover heals"
                );
                assert_eq!(
                    epoch, ref_epoch,
                    "seed {seed}/{partition_after}: all nodes must agree on the config \
                         epoch after the failover heals"
                );
            }
        }
    }
}

// THE LAG GATE (no data loss): a too-stale replica is NEVER promoted. The gate
// PREDICATE itself (`ironcache_repl::replica_is_in_sync`: link up AND lag <=
// max_lag, the only promotion-eligible state) is unit-tested in
// `ironcache-repl/src/lag.rs` (`in_sync_true_only_when_up_and_within_lag`), and is
// NOT re-tested here to keep the pure engine crate free of an `ironcache-repl`
// dependency. The split-brain DST gate above models that gate at the DECISION level
// -- the failover controller (the test) promotes ONLY the in-sync replica and
// asserts it never names the stale one -- which is exactly how the production
// controller consumes the predicate before proposing a `PromoteReplica`.

// =====================================================================
// HA-6: ONLINE SLOT MIGRATION -- THE CRASH-AT-FLIP GATE.
//
// A migration moves a slot's data SRC -> DEST, then transfers ownership in ONE committed
// SetSlotOwner (the FLIP). The danger is LOST KEYS / DOUBLE-OWNERSHIP at the FLIP boundary: a
// crash/partition right as the FLIP is proposed must leave EXACTLY ONE owner -- SRC if the FLIP
// did not commit, DEST if it did, never both, and a committed FLIP is NEVER lost. This gate
// partitions the leader away at randomized points around the FLIP across thousands of timelines
// and asserts, at every quiescent step: no two nodes owns()==true for a slot at one epoch
// (`assert_at_most_one_owner_per_slot`); the epoch is monotone; and after heal EXACTLY one owner
// (`assert_exactly_one_owner_after_convergence`), with every node's committed prefix agreeing.
//
// It reuses the SAME owns()-property checkers as the HA-8 failover gate (the migration FLIP and
// the failover promotion both transfer ownership through one committed ConfigCmd, so the
// split-brain property is identical), driven by the migration handshake (SetSlotMigrating /
// SetSlotImporting then the SetSlotOwner FLIP). The data MOVE itself is not modeled in the pure
// engine (it has no store / replication link); this gate proves the STATE MACHINE + the
// committed FLIP + the crash-at-FLIP safety, which is the part that cannot be stubbed.
// =====================================================================

/// Replay the HA-6 crash-at-FLIP gate for one `seed` with `partition_after` controlling WHEN (in
/// chunks) the leader is partitioned away relative to the FLIP proposal, so the seed sweep
/// randomizes the crash timing. Returns the final per-node owner-by-slot snapshot + epoch (for
/// the seed-sweep convergence assertion). The split-brain checker and the epoch-monotonic checker
/// run at EVERY quiescent step throughout.
#[allow(clippy::too_many_lines)]
fn run_migration_crash_at_flip_gate(
    seed: u64,
    partition_after: usize,
) -> Vec<(BTreeMap<u16, String>, u64)> {
    // 3 voters: SRC is the first leader (the slot owner); DEST is a peer (the migration target).
    let mut cluster = ConfigCluster::new(3, seed, RaftConfig::default());
    cluster
        .net
        .set_latency(Duration::from_millis(1), Duration::from_millis(15));
    let mut epochs = EpochMonotonic::new();
    let src = elect_config_leader(&mut cluster);
    assert_3e_invariants(&cluster, &mut epochs);
    assert_at_most_one_owner_per_slot(&cluster);

    // Build the node table (committed before any slot / migration reference).
    for id in cluster.ids.clone() {
        cluster.propose(
            src,
            ConfigCmd::AddNode {
                id: slot_id(id),
                host: slot_host(id),
                port: SLOT_PORT,
            },
        );
    }
    cluster.run_steps(3_000);
    assert_3e_invariants(&cluster, &mut epochs);

    // The migrating slots. SRC claims them, then begins the migration handshake to DEST.
    let slots: [u16; 3] = [0, 6000, 12000];
    let dest = *cluster
        .ids
        .iter()
        .find(|&&id| id != src)
        .expect("a peer to migrate to");
    for &s in &slots {
        cluster.propose(
            src,
            ConfigCmd::SetSlotOwner {
                slot: s,
                node: slot_id(src),
            },
        );
    }
    // SRC MIGRATING the slots to DEST; DEST IMPORTING from SRC (the per-slot handshake). These
    // tag migration STATE (the parallel arrays), NOT ownership -- SRC still owns throughout.
    for &s in &slots {
        cluster.propose(
            src,
            ConfigCmd::SetSlotMigrating {
                slot: s,
                dest: slot_id(dest),
            },
        );
        cluster.propose(
            src,
            ConfigCmd::SetSlotImporting {
                slot: s,
                src: slot_id(src),
                // Finding 2: the IMPORTING tag is set on EXACTLY the dest (DEST here), never on
                // the third bystander voter -- so only DEST records IMPORTING during the handshake.
                dest: slot_id(dest),
            },
        );
    }
    cluster.run_steps(3_000);
    assert_3e_invariants(&cluster, &mut epochs);
    assert_at_most_one_owner_per_slot(&cluster);
    // SRC still owns every migrating slot (the handshake never transfers ownership).
    for &s in &slots {
        assert!(
            cluster.map(src).owns(s),
            "seed {seed}: SRC must still own slot {s} during the migration handshake"
        );
    }

    let pre_flip_epoch = cluster.current_epoch(src);

    // Run a randomized number of chunks BEFORE the partition, so it lands at different points
    // relative to in-flight replication of the handshake.
    for _ in 0..partition_after {
        cluster.run_steps(200);
        assert_at_most_one_owner_per_slot(&cluster);
    }

    // PARTITION the leader (SRC) away from the other two. SRC (1 of 3) cannot commit; the
    // two-node majority (which includes DEST) elects a leader and drives the FLIP. This models
    // the crash AT the FLIP boundary: the FLIP is proposed under partition, so across the seed
    // sweep it sometimes commits (majority side) and sometimes does not (if SRC was the only
    // node that could have proposed and it is isolated).
    let majority: Vec<SimId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != src)
        .map(to_sim)
        .collect();
    cluster.net.partition(&[to_sim(src)], &majority);

    // The majority elects a leader, then proposes THE FLIP: SetSlotOwner(slot -> DEST). On
    // commit, DEST owns and the migration clears (set_slot_node clears the migration in
    // lockstep). A spurious FLIP (SRC alive across the partition) is SAFE for split-brain: the
    // committed entry atomically transfers ownership and SRC steps down on apply.
    let maj_leader = run_to_leader_in_group(&mut cluster, &majority, 200)
        .expect("the majority side must elect a leader");
    for &s in &slots {
        cluster.propose(
            maj_leader,
            ConfigCmd::SetSlotOwner {
                slot: s,
                node: slot_id(dest),
            },
        );
    }
    for _ in 0..25 {
        cluster.run_steps(500);
        assert_at_most_one_owner_per_slot(&cluster);
        assert_3e_invariants(&cluster, &mut epochs);
    }

    // The majority committed the FLIP: DEST now OWNS the slots (committed there) and the
    // migration state is cleared on the majority side.
    for &s in &slots {
        assert!(
            cluster.map(dest).owns(s),
            "seed {seed}: DEST must own slot {s} after the committed FLIP"
        );
    }
    assert!(
        cluster.current_epoch(dest) > pre_flip_epoch,
        "seed {seed}: the FLIP must advance the epoch ({} > {pre_flip_epoch})",
        cluster.current_epoch(dest)
    );

    // HEAL. SRC, catching its Raft log up, applies the committed FLIP: its owns() for the slots
    // goes FALSE (it serves MOVED, not ASK -- the migration is cleared). The split-brain checker
    // runs through the entire reconciliation.
    cluster.net.heal();
    for _ in 0..30 {
        cluster.run_steps(500);
        assert_at_most_one_owner_per_slot(&cluster);
        assert_3e_invariants(&cluster, &mut epochs);
    }
    cluster.run_until_idle(100_000);
    assert_at_most_one_owner_per_slot(&cluster);
    assert_3e_invariants(&cluster, &mut epochs);
    // Post-heal: every node caught up -> EXACTLY one owner per slot (the committed FLIP is never
    // lost; a node that never saw a FLIP commit would leave SRC the sole owner -- also exactly
    // one -- but here the majority committed it, so DEST is the converged owner).
    assert_exactly_one_owner_after_convergence(&cluster);

    // SRC lost ownership and resolves MOVED to DEST for every migrated slot (NOT ASK: the FLIP
    // cleared the migration tag, so the cold redirect path no longer treats the slot as
    // migrating). We assert the OWNERSHIP fence here; the ASK-vs-MOVED redirect itself is unit-
    // and loopback-tested where the redirect lives (the pure engine has no redirect path).
    for &s in &slots {
        assert!(
            !cluster.map(src).owns(s),
            "seed {seed}: SRC must lose ownership of slot {s} after heal (MOVED, not ASK)"
        );
        assert_eq!(
            cluster.map(src).migration_state(s),
            ironcache_cluster::MigrationState::None,
            "seed {seed}: the committed FLIP cleared SRC's migration tag for slot {s}"
        );
        assert_eq!(
            cluster.map(src).moved_target(s),
            Some((slot_host(dest), SLOT_PORT)),
            "seed {seed}: SRC must MOVED slot {s} to the new owner DEST"
        );
    }
    // No committed change was lost: every node's committed prefix agrees with the leader's.
    let final_leader = {
        let ls = cluster.leaders();
        assert_eq!(ls.len(), 1, "seed {seed}: exactly one leader after heal");
        ls[0]
    };
    assert_committed_prefix_agrees(&cluster, final_leader);

    cluster
        .ids
        .iter()
        .map(|&id| (cluster.owner_by_slot(id), cluster.current_epoch(id)))
        .collect()
}

/// Replay the HA-6 crash-at-FLIP gate's COMPLEMENTARY half: the ABORT timeline, where the FLIP
/// does NOT commit. The `run_migration_crash_at_flip_gate` above always lands the FLIP on the
/// majority (so it always commits and DEST always becomes the owner); it never exercises the
/// other branch its comment claims -- "SRC remains the sole clean owner if the FLIP did not
/// commit (migration safely abortable)". This gate proves that branch:
///
///   1. Drive the migration handshake (SRC owns; SRC MIGRATING -> DEST; DEST IMPORTING from SRC).
///   2. Partition SRC (the leader, a minority of 1) away from the majority {DEST, bystander}.
///   3. PROPOSE THE FLIP on the ISOLATED SRC. With only 1 of 3 it can NEVER reach quorum, so the
///      FLIP is appended to SRC's local log but is NEVER committed (and never applied anywhere).
///   4. The MAJORITY elects a leader and commits `SetSlotStable{slot}` -- the ABORT.
///   5. HEAL. Raft log reconciliation OVERWRITES SRC's uncommitted FLIP tail with the majority
///      leader's log (which carries the committed STABLE, not the FLIP), so the FLIP is discarded.
///
/// Asserts, with the SAME checkers as the commit gate: at every quiescent step NEVER two owners
/// per slot per epoch (`assert_at_most_one_owner_per_slot`); epoch monotone (`assert_3e_invariants`
/// via `EpochMonotonic`); and after heal EXACTLY ONE owner per slot (`assert_exactly_one_owner_
/// after_convergence`) which is the SOURCE (the un-committed FLIP was discarded -> ownership never
/// transferred), with SRC's migration_state back to None (the committed STABLE cleared it) and
/// every node's committed prefix agreeing. Randomized partition timing over the seed sweep.
#[allow(clippy::too_many_lines)]
fn run_migration_abort_leaves_one_owner_gate(
    seed: u64,
    partition_after: usize,
) -> Vec<(BTreeMap<u16, String>, u64)> {
    // 3 voters: SRC is the first leader (the slot owner); DEST is the migration target; the
    // third node is a bystander voter that, with DEST, forms the majority once SRC is isolated.
    let mut cluster = ConfigCluster::new(3, seed, RaftConfig::default());
    cluster
        .net
        .set_latency(Duration::from_millis(1), Duration::from_millis(15));
    let mut epochs = EpochMonotonic::new();
    let src = elect_config_leader(&mut cluster);
    assert_3e_invariants(&cluster, &mut epochs);
    assert_at_most_one_owner_per_slot(&cluster);

    // Build the node table (committed before any slot / migration reference).
    for id in cluster.ids.clone() {
        cluster.propose(
            src,
            ConfigCmd::AddNode {
                id: slot_id(id),
                host: slot_host(id),
                port: SLOT_PORT,
            },
        );
    }
    cluster.run_steps(3_000);
    assert_3e_invariants(&cluster, &mut epochs);

    // SRC claims the slots, then begins the migration handshake to DEST.
    let slots: [u16; 3] = [0, 6000, 12000];
    let dest = *cluster
        .ids
        .iter()
        .find(|&&id| id != src)
        .expect("a peer to migrate to");
    // The handshake is ORDER-SENSITIVE per slot (the owner claim must apply BEFORE the MIGRATING
    // tag, which `set_migrating` gates on `owns()`). Proposals are delivered fire-and-forget and
    // the sim REORDERS them, so we propose each step and SETTLE it (run until it commits) before
    // the next -- a deterministic, in-order handshake. `propose_and_settle` returns when the
    // leader's commit index has advanced past the proposal (bounded), so the abort timeline below
    // starts from a known LIVE migration on SRC, not a reordered partial.
    let propose_and_settle = |cluster: &mut ConfigCluster, cmd: ConfigCmd| {
        let before = cluster.commit_index(src);
        cluster.propose(src, cmd);
        for _ in 0..40 {
            cluster.run_steps(500);
            assert_at_most_one_owner_per_slot(cluster);
            if cluster.commit_index(src) > before {
                return;
            }
        }
        panic!("seed {seed}: a handshake proposal did not commit on SRC");
    };
    for &s in &slots {
        propose_and_settle(
            &mut cluster,
            ConfigCmd::SetSlotOwner {
                slot: s,
                node: slot_id(src),
            },
        );
    }
    // SRC MIGRATING the slots to DEST; DEST IMPORTING from SRC (the per-slot handshake). These
    // tag migration STATE only -- SRC still owns throughout (no ownership transfer yet).
    for &s in &slots {
        propose_and_settle(
            &mut cluster,
            ConfigCmd::SetSlotMigrating {
                slot: s,
                dest: slot_id(dest),
            },
        );
        propose_and_settle(
            &mut cluster,
            ConfigCmd::SetSlotImporting {
                slot: s,
                src: slot_id(src),
                dest: slot_id(dest),
            },
        );
    }
    assert_3e_invariants(&cluster, &mut epochs);
    assert_at_most_one_owner_per_slot(&cluster);
    // The handshake is fully applied + IN-ORDER: SRC owns every slot AND is tagged MIGRATING (a
    // LIVE migration to then abort). DEST is NOT tagged (it only applies its own IMPORTING, and
    // we drive the abort before any data move) -- the bystander is never tagged (Finding 2).
    for &s in &slots {
        assert!(
            cluster.map(src).owns(s),
            "seed {seed}: SRC must still own slot {s} during the migration handshake"
        );
        assert_eq!(
            cluster.map(src).migration_state(s),
            ironcache_cluster::MigrationState::Migrating,
            "seed {seed}: SRC is tagged MIGRATING during the handshake (a live migration to abort)"
        );
    }

    // Run a randomized number of chunks BEFORE the partition (randomized abort timing).
    for _ in 0..partition_after {
        cluster.run_steps(200);
        assert_at_most_one_owner_per_slot(&cluster);
    }

    // PARTITION SRC (the leader, 1 of 3) away from the majority {DEST, bystander}.
    let majority: Vec<SimId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != src)
        .map(to_sim)
        .collect();
    cluster.net.partition(&[to_sim(src)], &majority);

    // PROPOSE THE FLIP on the ISOLATED SRC. As a minority of 1 it cannot reach quorum, so this
    // entry is appended to SRC's LOCAL log but is NEVER committed (and never applied anywhere).
    // Give it time to land locally + churn the network; the FLIP must stay uncommitted.
    for &s in &slots {
        cluster.propose(
            src,
            ConfigCmd::SetSlotOwner {
                slot: s,
                node: slot_id(dest),
            },
        );
    }
    for _ in 0..10 {
        cluster.run_steps(200);
        assert_at_most_one_owner_per_slot(&cluster);
    }
    // The isolated SRC's FLIP did NOT commit: SRC still owns and is still tagged MIGRATING (no
    // apply happened), and DEST has NOT become the owner.
    for &s in &slots {
        assert!(
            cluster.map(src).owns(s),
            "seed {seed}: the isolated SRC's un-quorate FLIP must NOT transfer ownership"
        );
        assert!(
            !cluster.map(dest).owns(s),
            "seed {seed}: DEST must NOT own slot {s} while the FLIP is uncommitted"
        );
    }

    // The MAJORITY elects a leader and commits the ABORT: SetSlotStable on each slot. STABLE
    // clears the migration tag WITHOUT transferring ownership.
    let maj_leader = run_to_leader_in_group(&mut cluster, &majority, 200)
        .expect("the majority side must elect a leader");
    for &s in &slots {
        cluster.propose(maj_leader, ConfigCmd::SetSlotStable { slot: s });
    }
    for _ in 0..25 {
        cluster.run_steps(500);
        assert_at_most_one_owner_per_slot(&cluster);
        assert_3e_invariants(&cluster, &mut epochs);
    }

    // HEAL. SRC, catching its Raft log up, has its uncommitted FLIP tail OVERWRITTEN by the
    // majority leader's log (Figure-8 reconciliation): the FLIP is discarded and the committed
    // STABLE is applied, so SRC's migration tag clears and its ownership is UNCHANGED.
    cluster.net.heal();
    for _ in 0..30 {
        cluster.run_steps(500);
        assert_at_most_one_owner_per_slot(&cluster);
        assert_3e_invariants(&cluster, &mut epochs);
    }
    cluster.run_until_idle(100_000);
    assert_at_most_one_owner_per_slot(&cluster);
    assert_3e_invariants(&cluster, &mut epochs);

    // Post-heal: EXACTLY ONE owner per slot, and it is the SOURCE -- the un-committed FLIP was
    // discarded, so ownership never transferred; the migration aborted cleanly.
    assert_exactly_one_owner_after_convergence(&cluster);
    for &s in &slots {
        assert!(
            cluster.map(src).owns(s),
            "seed {seed}: after an ABORTED migration the SOURCE remains the sole owner of slot {s}"
        );
        assert!(
            !cluster.map(dest).owns(s),
            "seed {seed}: DEST never owns slot {s} when the FLIP did not commit"
        );
        // The STABLE abort cleared the SOURCE's migration tag back to None on EVERY node.
        for &id in &cluster.ids {
            assert_eq!(
                cluster.map(id).migration_state(s),
                ironcache_cluster::MigrationState::None,
                "seed {seed}: the committed STABLE abort cleared the migration tag on node {id:?} \
                     for slot {s}"
            );
        }
        // SRC, still the owner, resolves MOVED to ITSELF (it never gave the slot up).
        assert_eq!(
            cluster.map(src).moved_target(s),
            Some((slot_host(src), SLOT_PORT)),
            "seed {seed}: the SOURCE still owns slot {s} after the abort (MOVED to self)"
        );
    }
    // No committed change was lost or reordered (the discarded FLIP was never committed).
    let final_leader = {
        let ls = cluster.leaders();
        assert_eq!(ls.len(), 1, "seed {seed}: exactly one leader after heal");
        ls[0]
    };
    assert_committed_prefix_agrees(&cluster, final_leader);

    cluster
        .ids
        .iter()
        .map(|&id| (cluster.owner_by_slot(id), cluster.current_epoch(id)))
        .collect()
}

#[test]
fn migration_abort_leaves_one_owner_gate() {
    // THE COMPLEMENTARY HALF of the crash-at-FLIP gate: across (seed, partition-timing) pairs,
    // an ABORTED migration (the FLIP proposed on an isolated minority, never committed; a STABLE
    // committed on the majority) must leave the SOURCE the SOLE clean owner with the migration
    // cleared -- never two owners, never a lost SOURCE, never a half-applied FLIP. 200 seeds x 5
    // partition-timing offsets = 1000 distinct abort timelines, each scanning all assigned slots
    // for two owners at every quiescent chunk and asserting one-owner==SOURCE after heal.
    for seed in 0..200u64 {
        for partition_after in 0..5usize {
            let snaps = run_migration_abort_leaves_one_owner_gate(seed, partition_after);
            let (ref_owner, ref_epoch) = &snaps[0];
            for (owner, epoch) in &snaps {
                assert_eq!(
                    owner, ref_owner,
                    "seed {seed}/{partition_after}: all nodes must converge to one \
                         slot->owner view after the migration aborts"
                );
                assert_eq!(
                    epoch, ref_epoch,
                    "seed {seed}/{partition_after}: all nodes must agree on the config \
                         epoch after the migration aborts"
                );
            }
        }
    }
}

#[test]
fn migration_crash_at_flip_gate() {
    // THE HA-6 MERGE-BLOCKER, across thousands of (seed, partition-timing) pairs. For each seed
    // we vary `partition_after` so the leader is isolated at different points around the FLIP
    // (randomized crash timing). Throughout EVERY run:
    // - the split-brain checker (`assert_at_most_one_owner_per_slot`) runs at every quiescent
    //   step -> two simultaneous owners of a migrating/flipped slot would panic immediately;
    // - the epoch is monotone everywhere and the FLIP advances it past the pre-FLIP epoch;
    // and after heal all nodes converge to ONE ownership view (SRC having lost the slots, the
    // migration tag cleared, MOVED to DEST). A committed FLIP is never lost.
    //
    // 200 seeds x 5 partition-timing offsets = 1000 distinct migration timelines, each scanning
    // all 3 nodes' ASSIGNED slots (epoch-keyed) for two owners at every quiescent chunk across
    // the partition/heal timeline.
    for seed in 0..200u64 {
        for partition_after in 0..5usize {
            let snaps = run_migration_crash_at_flip_gate(seed, partition_after);
            let (ref_owner, ref_epoch) = &snaps[0];
            for (owner, epoch) in &snaps {
                assert_eq!(
                    owner, ref_owner,
                    "seed {seed}/{partition_after}: all nodes must converge to one \
                         slot->owner view after the migration heals"
                );
                assert_eq!(
                    epoch, ref_epoch,
                    "seed {seed}/{partition_after}: all nodes must agree on the config \
                         epoch after the migration heals"
                );
            }
        }
    }
}

// -- scenario determinism_replay_3e ----------------------------------------

/// A per-node replay snapshot: the SlotMap's slot ranges plus the node's
/// current config epoch. Compared for byte-identical equality across two
/// same-seed runs to prove deterministic replay of the config state machine.
type NodeConfigSnapshot = (Vec<(u16, u16, usize)>, u64);

/// One config-proposal + partition + heal run for `seed`, returning the trace
/// plus a per-node (ranges, current_epoch) snapshot for byte-identical replay
/// comparison. The fault script is fixed (partition the first leader after a
/// fixed proposal set), so a same-seed replay is identical. The 3e checkers run
/// inside the run (via the shared helper).
fn replay_config_partition(
    seed: u64,
) -> (Vec<ironcache_sim::TraceRecord>, Vec<NodeConfigSnapshot>) {
    let mut cluster = ConfigCluster::new(5, seed, RaftConfig::default());
    let mut epochs = EpochMonotonic::new();
    let leader = elect_config_leader(&mut cluster);

    for id in cluster.ids.clone() {
        cluster.propose(
            leader,
            ConfigCmd::AddNode {
                id: slot_id(id),
                host: slot_host(id),
                port: SLOT_PORT,
            },
        );
    }
    cluster.run_steps(3_000);
    // A fixed slot-assignment + migration script.
    for (k, id) in cluster.ids.clone().into_iter().enumerate() {
        cluster.propose(
            leader,
            ConfigCmd::AssignSlots {
                node: slot_id(id),
                slots: vec![(k as u16) * 1000, (k as u16) * 1000 + 1],
            },
        );
        cluster.run_steps(1_500);
        assert_3e_invariants(&cluster, &mut epochs);
    }

    let others: Vec<SimId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != leader)
        .map(to_sim)
        .collect();
    cluster.net.partition(&[to_sim(leader)], &others);
    cluster.run_steps(40_000);
    assert_3e_invariants(&cluster, &mut epochs);
    cluster.net.heal();
    cluster.run_steps(40_000);
    assert_3e_invariants(&cluster, &mut epochs);

    let snapshot: Vec<NodeConfigSnapshot> = cluster
        .ids
        .iter()
        .map(|&id| (cluster.map(id).ranges(), cluster.current_epoch(id)))
        .collect();
    (cluster.net.trace().to_vec(), snapshot)
}

#[test]
fn determinism_replay_3e() {
    // A config-proposal + partition + heal scenario must replay byte-identically
    // across >=100 seeds: same trace AND same per-node (ranges, current_epoch)
    // snapshot, with the no-two-owners + epoch-monotonic checkers asserted each
    // seed (inside replay_config_partition).
    for seed in 0..100u64 {
        let (trace_a, snap_a) = replay_config_partition(seed);
        let (trace_b, snap_b) = replay_config_partition(seed);
        assert_eq!(
            trace_a, trace_b,
            "seed {seed}: config+partition+heal trace must replay byte-identically"
        );
        assert_eq!(
            snap_a, snap_b,
            "seed {seed}: per-node (ranges, current_epoch) must replay identically"
        );
    }
}

// =====================================================================
// HA-3c: snapshot + log compaction (Raft section 7).
//
// The state machine is snapshotted, the log is compacted below the snapshot
// index, and a leader installs its snapshot on a follower whose required entries
// were already compacted. These exercise the StateMachine snapshot/restore seam,
// the RaftStorage save/load/compact_to seam, and the InstallSnapshot RPC, ending
// with a DST gate that proves a long-partitioned follower is caught up via
// InstallSnapshot to the IDENTICAL committed state, with the safety checkers held.
// =====================================================================

// -- StateMachine snapshot/restore unit ---------------------------------

#[test]
fn counting_sm_snapshot_restore_round_trips() {
    // The trivial machine's whole state is its applied counter; snapshot -> restore
    // into a fresh machine recovers the same count.
    let mut sm = CountingSm::new();
    for _ in 0..7 {
        sm.apply(&noop(1, 1));
    }
    assert_eq!(sm.applied(), 7);
    let snap = sm.snapshot();
    let mut fresh = CountingSm::new();
    fresh.restore(&snap);
    assert_eq!(fresh.applied(), 7, "restore recovers the applied watermark");
}

#[test]
fn config_sm_snapshot_restore_round_trips() {
    // Build a ConfigSm with owners / replicas / migration / epoch set, snapshot it,
    // restore into a FRESH ConfigSm of the SAME node, and assert identical SlotMap
    // state + epoch (the config-state-machine half of the Raft snapshot).
    let id = NodeId(1);
    let mut sm = ConfigSm::seed(id);
    // Apply a committed config sequence: add peers, claim slots, a replica, a migration.
    let peer2 = NodeId(2);
    let peer3 = NodeId(3);
    let seq = [
        ConfigCmd::AddNode {
            id: slot_id(peer2),
            host: slot_host(peer2),
            port: SLOT_PORT,
        },
        ConfigCmd::AddNode {
            id: slot_id(peer3),
            host: slot_host(peer3),
            port: SLOT_PORT,
        },
        ConfigCmd::AssignSlots {
            node: slot_id(id),
            slots: vec![0, 1, 2],
        },
        ConfigCmd::SetSlotOwner {
            slot: 100,
            node: slot_id(peer2),
        },
        ConfigCmd::AssignReplica {
            node: slot_id(peer3),
            slots: vec![0, 1],
        },
        // slot 2 is owned by self, so MIGRATING toward peer2 holds (gated on owns()).
        ConfigCmd::SetSlotMigrating {
            slot: 2,
            dest: slot_id(peer2),
        },
    ];
    for (i, cmd) in seq.iter().enumerate() {
        let idx = i as u64 + 1;
        sm.apply(&LogEntry {
            term: 1,
            index: idx,
            payload: EntryPayload::Config(cmd.clone()),
        });
    }
    let epoch_before = sm.config_epoch();
    let snap = sm.snapshot();

    // Restore into a fresh ConfigSm of the SAME node id.
    let mut fresh = ConfigSm::seed(id);
    fresh.restore(&snap);

    // Identical SlotMap committed view (owns(), owner ids, replica, migration) + epoch.
    assert_eq!(fresh.config_epoch(), epoch_before);
    let a = sm.map();
    let b = fresh.map();
    assert!(b.owns(0) && b.owns(1) && b.owns(2));
    assert!(!b.owns(100));
    assert_eq!(
        a.serialize_committed(),
        b.serialize_committed(),
        "the restored map's committed view must match the source's"
    );
    assert!(b.is_replica_of(0, &slot_id(peer3)) && b.is_replica_of(1, &slot_id(peer3)));
    assert_eq!(
        b.migration_state(2),
        ironcache_cluster::MigrationState::Migrating
    );
    assert_eq!(
        b.migration_peer_id(2).as_deref(),
        Some(slot_id(peer2).as_str())
    );
    // The serialized form is a fixed point (deterministic function of the view).
    assert_eq!(
        fresh.map().serialize_committed(),
        sm.map().serialize_committed()
    );
}

// -- RaftStorage save/load + compact_to (MemStorage) --------------------

#[test]
fn mem_storage_save_load_snapshot_and_compact() {
    // Build a log 1..=5, snapshot at 3, compact to 3, then assert: the prefix is
    // gone, term_at(3) is answered from the snapshot meta, the tail (4,5) survives,
    // load_snapshot returns what was saved, and log_start_index moved to 4.
    let mut s = MemStorage::new();
    for i in 1..=5u64 {
        s.append(LogEntry {
            term: if i <= 3 { 2 } else { 3 },
            index: i,
            payload: EntryPayload::Bytes(vec![i as u8]),
        });
    }
    let meta = SnapshotMeta {
        last_included_index: 3,
        last_included_term: 2,
    };
    s.save_snapshot(meta, b"sm-state-at-3");
    s.compact_to(3);

    assert_eq!(s.log_start_index(), 4);
    assert_eq!(s.entry_at(1), None, "compacted prefix is gone");
    assert_eq!(
        s.entry_at(3),
        None,
        "the last_included entry is compacted away"
    );
    assert_eq!(
        s.term_at(3),
        2,
        "term_at(last_included_index) answered from the snapshot meta after compaction"
    );
    assert_eq!(s.entry_at(4).map(|e| e.index), Some(4), "the tail survives");
    assert_eq!(s.last_log_index(), 5);
    assert_eq!(
        s.entries_from(4),
        (4..=5)
            .map(|i| LogEntry {
                term: 3,
                index: i,
                payload: EntryPayload::Bytes(vec![i as u8])
            })
            .collect::<Vec<_>>()
    );
    let (loaded_meta, loaded_data) = s.load_snapshot().expect("snapshot saved");
    assert_eq!(loaded_meta, meta);
    assert_eq!(loaded_data, b"sm-state-at-3");
}

#[test]
fn mem_storage_fully_compacted_log_reads_from_snapshot() {
    // Compacting to the last index leaves an EMPTY log; last_log_index / last_log_term
    // must then come from the snapshot meta (a fully-compacted log ends at the snapshot).
    let mut s = MemStorage::new();
    for i in 1..=3u64 {
        s.append(noop(2, i));
    }
    s.save_snapshot(
        SnapshotMeta {
            last_included_index: 3,
            last_included_term: 2,
        },
        b"x",
    );
    s.compact_to(3);
    assert_eq!(
        s.last_log_index(),
        3,
        "fully-compacted log ends at the snapshot"
    );
    assert_eq!(s.last_log_term(), 2);
    assert_eq!(s.term_at(3), 2);
    assert_eq!(s.log_start_index(), 4);
    assert!(s.entries_from(4).is_empty());
    // A new append lands at index 4 (just above the snapshot) and is readable.
    s.append(noop(3, 4));
    assert_eq!(s.entry_at(4), Some(noop(3, 4)));
    assert_eq!(s.last_log_index(), 4);
}

// -- the DST snapshot gate (the merge-blocker) -------------------------

/// Propose a config entry at `leader` and settle it (run until the leader's commit
/// index advances past it). Deterministic, bounded; returns when committed.
fn config_propose_and_settle(cluster: &mut ConfigCluster, leader: NodeId, cmd: ConfigCmd) {
    let before = cluster.commit_index(leader);
    cluster.propose(leader, cmd);
    for _ in 0..80 {
        cluster.run_steps(500);
        if cluster.commit_index(leader) > before {
            return;
        }
    }
    panic!("a config proposal did not commit at the leader");
}

/// Replay the HA-3c snapshot DST gate for one `seed`: an N=3 cluster where the leader
/// takes snapshots + compacts, and a follower PARTITIONED long enough that its needed
/// entries are compacted away is caught up via InstallSnapshot after heal, converging
/// to the IDENTICAL state machine as the leader. Throughout, the 3e safety checkers
/// (no-two-owners-per-epoch, epoch-monotonic) and the committed-prefix agreement hold.
/// Returns the final per-node (owner-by-slot, epoch) snapshot for the seed-sweep
/// convergence assertion.
fn run_snapshot_catchup_gate(seed: u64) -> Vec<(BTreeMap<u16, String>, u64)> {
    // The original gate: the ENGINE-default chunk size (so the small config snapshot ships
    // in a SINGLE `done` chunk, byte-equivalent to the pre-PROD-9 whole-snapshot install),
    // no extra chunk loss, no mid-transfer restart.
    run_snapshot_catchup_gate_with(seed, SnapshotCatchupOpts::default())
}

/// PROD-9: knobs for the chunked-InstallSnapshot DST gate, so one parameterized scenario
/// drives the single-chunk (original), MULTI-CHUNK, DROPPED-CHUNK, and RESTART-MID-TRANSFER
/// variants. All variants must converge to the SAME byte-identical state machine.
#[derive(Clone, Copy)]
struct SnapshotCatchupOpts {
    /// The InstallSnapshot chunk size. The engine default ships the small config snapshot in
    /// one chunk; a tiny value (e.g. 1) forces a MULTI-chunk transfer.
    chunk_bytes: usize,
    /// When true, assert the leader's snapshot exceeds the chunk size (so the catch-up
    /// genuinely spans several chunks). Set by the multi-chunk variants; the default
    /// single-chunk run (chunk size 256 KiB > the tiny config snapshot) leaves it false.
    expect_multi_chunk: bool,
    /// When true, the laggard's link is repeatedly RE-PARTITIONED during the catch-up so
    /// chunks in flight to it are DROPPED and the leader must resume the transfer from the
    /// last acked offset; then healed for good. Tests dropped / retried chunk handling.
    lossy_catchup: bool,
    /// When true, the laggard's volatile receive buffer is DROPPED partway through the
    /// catch-up (a restart mid-transfer); the leader must restart the transfer from offset 0.
    restart_mid_transfer: bool,
}

impl Default for SnapshotCatchupOpts {
    fn default() -> Self {
        SnapshotCatchupOpts {
            chunk_bytes: DEFAULT_SNAPSHOT_CHUNK_BYTES,
            expect_multi_chunk: false,
            lossy_catchup: false,
            restart_mid_transfer: false,
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run_snapshot_catchup_gate_with(
    seed: u64,
    opts: SnapshotCatchupOpts,
) -> Vec<(BTreeMap<u16, String>, u64)> {
    // A small snapshot threshold so the leader compacts after a handful of entries.
    // PROD-9: the chunk size comes from the opts so the multi-chunk path is exercised.
    let config = RaftConfig {
        snapshot_threshold: 4,
        snapshot_chunk_bytes: opts.chunk_bytes,
        ..RaftConfig::default()
    };
    let mut cluster = ConfigCluster::new(3, seed, config);
    cluster
        .net
        .set_latency(Duration::from_millis(1), Duration::from_millis(15));
    let mut epochs = EpochMonotonic::new();
    let leader = elect_config_leader(&mut cluster);
    assert_3e_invariants(&cluster, &mut epochs);

    // Build the node table (committed before any slot reference).
    for id in cluster.ids.clone() {
        config_propose_and_settle(
            &mut cluster,
            leader,
            ConfigCmd::AddNode {
                id: slot_id(id),
                host: slot_host(id),
                port: SLOT_PORT,
            },
        );
    }
    assert_3e_invariants(&cluster, &mut epochs);

    // PARTITION one follower off (the laggard). The leader + the other follower are a
    // 2-of-3 majority, so the leader keeps committing + compacting while the laggard is gone.
    let laggard = *cluster
        .ids
        .iter()
        .find(|&&id| id != leader)
        .expect("a follower exists");
    let majority: Vec<SimId> = cluster
        .ids
        .iter()
        .copied()
        .filter(|&id| id != laggard)
        .map(to_sim)
        .collect();
    let minority = [to_sim(laggard)];
    cluster.net.partition(&majority, &minority);

    let laggard_log_start_before = cluster.log_start(laggard);

    // Propose MANY ownership flips on the majority side: enough that the log grows well
    // past the threshold and the leader snapshots + compacts past the laggard's last index.
    let slots: [u16; 6] = [0, 3000, 6000, 9000, 12000, 16_000];
    for round in 0..4u8 {
        for &s in &slots {
            // Alternate the owner so successive entries are distinct committed flips.
            let owner = if (round + (s % 3) as u8) % 2 == 0 {
                leader
            } else {
                cluster
                    .ids
                    .iter()
                    .copied()
                    .find(|&id| id != laggard && id != leader)
                    .unwrap_or(leader)
            };
            config_propose_and_settle(
                &mut cluster,
                leader,
                ConfigCmd::SetSlotOwner {
                    slot: s,
                    node: slot_id(owner),
                },
            );
        }
        assert_3e_invariants(&cluster, &mut epochs);
    }

    // The leader compacted: it holds a snapshot and its log no longer starts at 1.
    assert!(
        cluster.has_snapshot(leader),
        "seed {seed}: the leader must have taken a snapshot after exceeding the threshold"
    );
    assert!(
        cluster.log_start(leader) > 1,
        "seed {seed}: the leader's log must be compacted (start > 1)"
    );
    // The laggard, partitioned off, has NOT advanced its compaction point: its needed
    // entries are now BELOW the leader's log start, so only an InstallSnapshot can catch it up.
    assert_eq!(
        cluster.log_start(laggard),
        laggard_log_start_before,
        "seed {seed}: the partitioned laggard cannot have compacted while isolated"
    );
    assert!(
        cluster.commit_index(laggard) < cluster.commit_index(leader),
        "seed {seed}: the laggard must trail the leader's committed index while partitioned"
    );

    // PROD-9: when a multi-chunk transfer is requested, the leader's snapshot MUST be larger
    // than the chunk size, so catching up the laggard genuinely takes several chunks (the
    // multi-chunk path is actually exercised, not just configured). The default single-chunk
    // run uses a 256 KiB chunk that dwarfs the tiny config snapshot, so it does NOT assert this.
    if opts.expect_multi_chunk {
        let snap_len = cluster.snapshot_len(leader);
        assert!(
            snap_len > opts.chunk_bytes,
            "seed {seed}: the leader's snapshot ({snap_len} B) must exceed the chunk size \
                 ({} B) so the catch-up spans several chunks",
            opts.chunk_bytes
        );
    }

    // HEAL. The leader, seeing the laggard's next_index below its log start, ships its
    // snapshot in bounded chunks (PROD-9); the laggard reassembles + installs on the final
    // chunk and then catches up the tail via AppendEntries.
    cluster.net.heal();
    // PROD-9 DROPPED-CHUNK resilience: apply a per-message loss during the catch-up window.
    // A lost chunk gets no ack, so the leader retransmits from the last acked offset on its
    // next heartbeat; a lost ack likewise re-drives the chunk. The follower NEVER installs a
    // partial (install is gated on the final `done` chunk of a contiguous run), so the only
    // observable effect of loss is a slower catch-up.
    if opts.lossy_catchup {
        // PROD-9 DROPPED-CHUNK resilience, modeled by RE-PARTITIONING the laggard mid-transfer
        // (rather than a global message drop, which would churn the 3-node election term and
        // confound the test). Each re-partition DROPS the chunks in flight to the laggard; on
        // heal the leader RESUMES the transfer from the last acked offset (heartbeat-driven),
        // re-sending the lost chunk. The leader + the other follower stay a stable quorum
        // throughout (no election churn), so this isolates the chunk-loss behaviour. The
        // follower NEVER installs a partial (install is gated on the final `done` chunk of a
        // contiguous run), which the safety checkers + final convergence pin.
        for cycle in 0..6 {
            // Let some chunks flow to the laggard, then yank the link (drop in-flight chunks).
            cluster.net.heal();
            cluster.run_steps(700);
            assert_at_most_one_owner_per_slot(&cluster);
            assert_3e_invariants(&cluster, &mut epochs);
            // SAFETY: the laggard never commits past the cluster-wide committed high-water
            // (a half-received snapshot would jump it past a real committed index).
            let max_commit = cluster
                .ids
                .iter()
                .map(|&id| cluster.commit_index(id))
                .max()
                .unwrap_or(0);
            assert!(
                cluster.commit_index(laggard) <= max_commit,
                "seed {seed} cycle {cycle}: the laggard must never commit past the cluster \
                     high-water (a partial snapshot must never be installed)"
            );
            cluster.net.partition(&majority, &minority);
            cluster.run_steps(700);
        }
        // Heal for good so the transfer can finish + the cluster converges.
        cluster.net.heal();
    }
    // PROD-9 RESTART-MID-TRANSFER: drop the laggard's volatile receive buffer partway
    // through, modeling a crash that loses the in-progress reassembly. The leader's next
    // non-zero-offset chunk is rejected (the follower replies next_offset == 0), so the
    // transfer restarts cleanly from offset 0 -- and still converges.
    if opts.restart_mid_transfer {
        // Drive a few steps so a transfer is genuinely in flight, then yank the buffer.
        cluster.run_steps(2_000);
        cluster.drop_snapshot_rx(laggard);
    }
    for _ in 0..60 {
        cluster.run_steps(500);
        assert_at_most_one_owner_per_slot(&cluster);
        assert_3e_invariants(&cluster, &mut epochs);
    }
    // A generous idle budget: a multi-chunk catch-up (tiny chunks) after a lossy / restarted
    // window takes many round-trips, and a lossy window may have churned the term, so allow
    // ample steps to settle. Convergence (not just a step count) is what the assertions below
    // require.
    cluster.run_until_idle(2_000_000);
    assert_3e_invariants(&cluster, &mut epochs);
    assert_exactly_one_owner_after_convergence(&cluster);

    // The laggard was caught up VIA THE SNAPSHOT: it now holds a snapshot of its own and
    // its committed prefix agrees with the leader's; its state machine is identical.
    assert!(
        cluster.has_snapshot(laggard),
        "seed {seed}: the laggard must have installed a snapshot to catch up"
    );
    let final_leader = {
        let ls = cluster.leaders();
        assert_eq!(ls.len(), 1, "seed {seed}: exactly one leader after heal");
        ls[0]
    };
    assert_committed_prefix_agrees(&cluster, final_leader);
    // The laggard's converged ownership view + epoch equal the leader's (identical SM).
    assert_eq!(
        cluster.owner_by_slot(laggard),
        cluster.owner_by_slot(final_leader),
        "seed {seed}: the snapshot-caught-up follower must hold the IDENTICAL committed view"
    );
    assert_eq!(
        cluster.current_epoch(laggard),
        cluster.current_epoch(final_leader),
        "seed {seed}: the snapshot-caught-up follower must agree on the config epoch"
    );

    cluster
        .ids
        .iter()
        .map(|&id| (cluster.owner_by_slot(id), cluster.current_epoch(id)))
        .collect()
}

/// Assert every node in a catch-up gate's per-node `(owner-by-slot, epoch)` snapshots
/// converged to one identical view (the headline byte-identical-install property). Shared
/// by the original gate and the PROD-9 chunked variants.
fn assert_catchup_converged(seed: u64, snaps: &[(BTreeMap<u16, String>, u64)]) {
    let (ref_owner, ref_epoch) = &snaps[0];
    for (owner, epoch) in snaps {
        assert_eq!(
            owner, ref_owner,
            "seed {seed}: all nodes converge to one slot->owner view after snapshot catch-up"
        );
        assert_eq!(
            epoch, ref_epoch,
            "seed {seed}: all nodes agree on the config epoch after snapshot catch-up"
        );
    }
}

#[test]
fn snapshot_catchup_gate() {
    // THE HA-3c MERGE-BLOCKER, across a seed sweep. For each seed: a leader snapshots +
    // compacts while a follower is partitioned past the compaction point, then on heal the
    // follower is caught up VIA InstallSnapshot and converges to the IDENTICAL state machine,
    // with State-Machine-Safety (no two owners per slot per epoch), Log-Matching (committed
    // prefix agreement), Election-Safety, and epoch monotonicity all held throughout.
    for seed in 0..40u64 {
        assert_catchup_converged(seed, &run_snapshot_catchup_gate(seed));
    }
}

#[test]
fn snapshot_catchup_gate_multi_chunk() {
    // PROD-9: the SAME catch-up, but with a TINY chunk size (1 byte) so the config snapshot
    // is shipped in MANY bounded InstallSnapshot chunks. The follower reassembles them
    // contiguously and installs on the final `done` chunk, ending BYTE-IDENTICAL to the
    // single-chunk install (the gate asserts the snapshot len exceeds the chunk size, so the
    // multi-chunk path is genuinely exercised). Convergence + every safety checker still hold.
    for seed in 0..24u64 {
        let opts = SnapshotCatchupOpts {
            chunk_bytes: 1,
            expect_multi_chunk: true,
            ..SnapshotCatchupOpts::default()
        };
        assert_catchup_converged(seed, &run_snapshot_catchup_gate_with(seed, opts));
    }
}

#[test]
fn snapshot_catchup_gate_dropped_chunks() {
    // PROD-9: a MULTI-chunk catch-up under CHUNK LOSS. The laggard's link is repeatedly
    // re-partitioned mid-transfer, so chunks in flight to it are DROPPED and the leader must
    // RESUME the transfer from the last acked offset (heartbeat-driven) -- exercising
    // retransmit + the duplicate / reordered first-chunk reset paths. The laggard never
    // installs a partial (install is gated on the final `done` chunk of a contiguous run),
    // and once the link heals for good the cluster converges to the IDENTICAL state. The
    // small chunk size keeps the transfer many-chunked so loss genuinely interrupts it.
    // Asserted over a seed sweep.
    for seed in 0..24u64 {
        let opts = SnapshotCatchupOpts {
            chunk_bytes: 4,
            expect_multi_chunk: true,
            lossy_catchup: true,
            ..SnapshotCatchupOpts::default()
        };
        assert_catchup_converged(seed, &run_snapshot_catchup_gate_with(seed, opts));
    }
}

#[test]
fn snapshot_catchup_gate_restart_mid_transfer() {
    // PROD-9: a MULTI-chunk catch-up where the laggard RESTARTS mid-transfer -- its volatile
    // receive buffer is dropped partway through. The leader's next non-zero-offset chunk is
    // rejected (the follower replies next_offset == 0 because it holds no buffer), so the
    // transfer cleanly RESTARTS from offset 0. No partial is ever installed, and the cluster
    // still converges to the IDENTICAL state. Asserted across a seed sweep.
    for seed in 0..24u64 {
        let opts = SnapshotCatchupOpts {
            chunk_bytes: 4,
            expect_multi_chunk: true,
            restart_mid_transfer: true,
            ..SnapshotCatchupOpts::default()
        };
        assert_catchup_converged(seed, &run_snapshot_catchup_gate_with(seed, opts));
    }
}

// -- crash / restore-from-snapshot --------------------------------------

#[test]
#[allow(clippy::too_many_lines)]
fn node_crash_restores_identical_applied_state_from_snapshot_and_tail() {
    // A node that snapshots + compacts, then "crashes" (we extract its storage) and is rebuilt
    // on that SAME storage, must recover the IDENTICAL applied state for the SNAPSHOTTED prefix
    // (the snapshot restores the state machine, with NO double-apply of the compacted prefix),
    // then RE-APPLY the surviving log tail once it commits again, reaching the full pre-crash
    // view. (commit_index is VOLATILE in Raft: a restart trusts only the snapshot as committed
    // and re-confirms the tail by re-replication; here a single-voter leader re-confirms it at
    // once on the next propose.) We drive a single-voter leader, which commits + applies +
    // compacts on its own.
    let voters: BTreeSet<NodeId> = [NodeId(1)].into_iter().collect();
    let config = RaftConfig {
        snapshot_threshold: 3,
        ..RaftConfig::default()
    };
    // A projection helper: a SlotMap's owner-by-slot view (node-independent).
    let owners_of = |node: &RaftNode<MemStorage, ConfigSm>| -> BTreeMap<u16, String> {
        let map = node.state_machine().map();
        let nodes = map.nodes();
        let mut out = BTreeMap::new();
        for (start, end, idx) in map.ranges() {
            let owner = nodes[idx].id.to_string();
            for slot in start..=end {
                out.insert(slot, owner.clone());
            }
        }
        out
    };

    let storage = MemStorage::new();
    let mut node = RaftNode::with_state_machine(
        NodeId(1),
        voters.clone(),
        storage,
        config,
        ConfigSm::seed(NodeId(1)),
    );
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    node.start(now, &mut rng, &mut Effects::new());
    // Become leader (single voter => instant majority).
    node.on_timer(now, &mut rng, ELECTION_TIMEOUT, &mut Effects::new());
    assert!(node.is_leader());

    // Propose enough config entries to cross the threshold and trigger compaction.
    node.propose(
        EntryPayload::Config(ConfigCmd::AssignSlots {
            node: slot_id(NodeId(1)),
            slots: vec![0, 1, 2, 3],
        }),
        now,
        &mut rng,
        &mut Effects::new(),
    );
    for s in 4..12u16 {
        node.propose(
            EntryPayload::Config(ConfigCmd::SetSlotOwner {
                slot: s,
                node: slot_id(NodeId(1)),
            }),
            now,
            &mut rng,
            &mut Effects::new(),
        );
    }
    // The single-voter leader committed + applied + compacted (the trigger ran in apply).
    let (snap_meta, _) = node
        .storage()
        .load_snapshot()
        .expect("the node must have snapshotted");
    assert!(
        node.storage().log_start_index() > 1,
        "the log must be compacted"
    );

    // Capture the FULL pre-crash applied view (owner-by-slot + epoch) for the post-recovery
    // re-confirmation, and the SNAPSHOT view (the prefix the snapshot subsumes) for the
    // immediate-post-restart assertion.
    let pre_owners = owners_of(&node);
    let pre_epoch = node.state_machine().config_epoch();
    // The state-machine view AT the snapshot boundary: rebuild a ConfigSm from the snapshot's
    // bytes alone (this is exactly what restore does), so we know what the restored node should
    // show BEFORE the tail re-applies.
    let snapshot_only_owners = {
        let mut sm = ConfigSm::seed(NodeId(1));
        sm.restore(&node.storage().load_snapshot().expect("snapshot").1);
        let map = sm.map();
        let nodes = map.nodes();
        let mut out = BTreeMap::new();
        for (start, end, idx) in map.ranges() {
            let owner = nodes[idx].id.to_string();
            for slot in start..=end {
                out.insert(slot, owner.clone());
            }
        }
        out
    };

    // CRASH + RESTART: extract the storage (the durable record) and rebuild a fresh node +
    // fresh state machine on it. with_state_machine restores the SM from the snapshot and sets
    // commit_index/last_applied to the snapshot index (NO double-apply of the compacted prefix).
    let RaftNode { storage, .. } = node;
    let mut restored = RaftNode::with_state_machine(
        NodeId(1),
        voters,
        storage,
        config,
        ConfigSm::seed(NodeId(1)),
    );

    // Immediately post-restart: the applied watermarks equal the SNAPSHOT index, and the SM
    // reflects exactly the snapshotted prefix (the tail above the snapshot is in the log but is
    // not yet re-confirmed committed -- commit_index is volatile).
    assert_eq!(
        restored.commit_index(),
        snap_meta.last_included_index,
        "restored commit_index is the snapshot index (commit is volatile, re-confirmed later)"
    );
    assert_eq!(
        restored.last_applied(),
        snap_meta.last_included_index,
        "last_applied set to the snapshot prefix (no double-apply of the compacted entries)"
    );
    assert_eq!(
        owners_of(&restored),
        snapshot_only_owners,
        "the restored SM reflects exactly the snapshotted (committed+applied) prefix"
    );

    // RE-CONFIRM the tail: drive the restored node back to leadership; the single-voter leader
    // re-commits its whole log (it appends a fresh no-op and advances commit), re-applying the
    // surviving tail above the snapshot. The applied state then equals the FULL pre-crash view.
    restored.start(now, &mut rng, &mut Effects::new());
    restored.on_timer(now, &mut rng, ELECTION_TIMEOUT, &mut Effects::new());
    assert!(
        restored.is_leader(),
        "the restored node re-elects (single voter)"
    );
    // A fresh proposal on the single-voter leader advances commit_index to the top of the log
    // (the section-5.4.2 rule commits the current-term proposal, carrying the prior-term tail
    // above the snapshot with it transitively), which drives apply over the surviving tail.
    restored.propose(EntryPayload::Noop, now, &mut rng, &mut Effects::new());
    assert_eq!(
        owners_of(&restored),
        pre_owners,
        "after re-confirming the tail, the applied SlotMap view equals the full pre-crash view"
    );
    assert_eq!(
        restored.state_machine().config_epoch(),
        pre_epoch,
        "the re-confirmed config epoch equals the full pre-crash epoch"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn install_snapshot_resp_does_not_over_advance_match_index_on_second_compaction() {
    // FIX 2 (Figure 13): the leader advances a follower's match_index from the index
    // the follower ECHOED in its InstallSnapshotResp, NOT from the leader's OWN current
    // snapshot meta. So a SECOND compaction inside the in-flight InstallSnapshot window
    // must NOT push match_index past what the follower actually installed (a
    // match_index lie is a false-commit hazard).
    //
    // Setup: a 3-voter leader (term 4) that has snapshotted at index K and compacted.
    // We send NodeId(2) an InstallSnapshot(K), then COMPACT AGAIN to K' > K (simulating
    // the leader applying + compacting more committed entries while the install is in
    // flight). When NodeId(2)'s reply arrives echoing K, the leader must set
    // match_index[2] = K, NOT the newer K'.
    let voters: BTreeSet<NodeId> = (1..=3).map(NodeId).collect();
    let mut storage = MemStorage::new();
    storage.set_current_term(4);
    // A log 1..=8 at term 4, all committed-and-applied on the leader by construction.
    for i in 1..=8u64 {
        storage.append(LogEntry {
            term: 4,
            index: i,
            payload: EntryPayload::Noop,
        });
    }
    let mut leader = RaftNode::new(NodeId(1), voters, storage, RaftConfig::default());
    let mut rng = ZeroRng;
    let now = Monotonic::from_since_origin(Duration::ZERO);
    leader.role = Role::Leader;
    leader.commit_index = 8;
    leader.last_applied = 8;
    // Initialize peer markers as a fresh leader would (Figure 2): next_index = last+1,
    // match_index = 0.
    for peer in [NodeId(2), NodeId(3)] {
        leader.next_index.insert(peer, 9);
        leader.match_index.insert(peer, 0);
    }

    // The FIRST snapshot + compaction: K = 4. The follower's needed entries are below
    // this, so a send to it would be an InstallSnapshot(K=4).
    let first_k = 4u64;
    leader.storage.save_snapshot(
        SnapshotMeta {
            last_included_index: first_k,
            last_included_term: 4,
        },
        b"snap-at-4",
    );
    leader.storage.compact_to(first_k);
    // Drive a fresh InstallSnapshot to the lagging follower (next_index 1 <= 4 forces
    // the InstallSnapshot branch), and confirm that is what goes on the wire.
    leader.next_index.insert(NodeId(2), 1);
    let mut out = Effects::new();
    leader.send_append_entries_to(NodeId(2), &mut out);
    let sent_k = out.sends.iter().find_map(|(to, m)| match m {
        RaftMsg::InstallSnapshot {
            last_included_index,
            ..
        } if *to == NodeId(2) => Some(*last_included_index),
        _ => None,
    });
    assert_eq!(
        sent_k,
        Some(first_k),
        "the leader must ship InstallSnapshot at the FIRST snapshot index K=4"
    );

    // SECOND compaction inside the in-flight window: K' = 7 > K. The leader's CURRENT
    // load_snapshot() now answers 7, the value the OLD (buggy) code would have read.
    let second_k = 7u64;
    leader.storage.save_snapshot(
        SnapshotMeta {
            last_included_index: second_k,
            last_included_term: 4,
        },
        b"snap-at-7",
    );
    leader.storage.compact_to(second_k);
    assert_eq!(
        leader
            .storage
            .load_snapshot()
            .map(|(m, _)| m.last_included_index),
        Some(second_k),
        "the leader has compacted AGAIN to K'=7 (the over-advance trap)"
    );

    // The follower's reply finally arrives, ECHOING the index it actually installed (K=4).
    let mut out = Effects::new();
    leader.on_message(
        now,
        &mut rng,
        NodeId(2),
        RaftMsg::InstallSnapshotResp {
            term: 4,
            last_included_index: first_k,
            // PROD-9: the follower INSTALLED the (single-chunk) snapshot, so the leader
            // advances its markers from the echoed index.
            installed: true,
            next_offset: 0,
        },
        &mut out,
    );

    // THE FIX: match_index[2] is the ECHOED K=4, NOT the leader's current K'=7. Reading
    // the leader's own meta (the bug) would have set it to 7, claiming the follower
    // holds entries 5..=7 it never installed.
    assert_eq!(
        leader.match_index.get(&NodeId(2)).copied(),
        Some(first_k),
        "match_index advances from the ECHOED install index (4), never the newer K'=7"
    );
    assert_eq!(
        leader.next_index.get(&NodeId(2)).copied(),
        Some(first_k + 1),
        "next_index follows the echoed index (5), so the leader re-ships entries 5.."
    );

    // A reordered OLDER reply (a smaller echoed index) must not REWIND the marker
    // (the .max guard): deliver an echo of 2 and confirm match_index stays at 4.
    let mut out = Effects::new();
    leader.on_message(
        now,
        &mut rng,
        NodeId(2),
        RaftMsg::InstallSnapshotResp {
            term: 4,
            last_included_index: 2,
            installed: true,
            next_offset: 0,
        },
        &mut out,
    );
    assert_eq!(
        leader.match_index.get(&NodeId(2)).copied(),
        Some(first_k),
        "a reordered older echo (2) must not rewind match_index below 4 (.max guard)"
    );
}

// -- PROD-9: chunked InstallSnapshot offset/done state machine ---------------

/// Build a fresh FOLLOWER (NodeId(2)) in a {1,2} cluster at `term`, ready to receive an
/// InstallSnapshot from leader NodeId(1). A bare `CountingSm`-backed node: the chunk
/// assembly + offset/done logic under test is state-machine-agnostic.
fn chunk_rx_follower(term: u64) -> (RaftNode<MemStorage>, Monotonic) {
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2)].into_iter().collect();
    let mut storage = MemStorage::new();
    storage.set_current_term(term);
    let mut node = RaftNode::new(NodeId(2), voters, storage, RaftConfig::default());
    let now = Monotonic::from_since_origin(Duration::ZERO);
    node.start(now, &mut ZeroRng, &mut Effects::new());
    (node, now)
}

/// Deliver one InstallSnapshot chunk to a follower and return the single resp it emits
/// (term, echoed last_included_index, installed, next_offset). A CountingSm snapshot is an
/// opaque blob, so any bytes serve as the snapshot payload.
#[allow(clippy::too_many_arguments)]
fn deliver_chunk(
    node: &mut RaftNode<MemStorage>,
    now: Monotonic,
    term: u64,
    last_included_index: u64,
    last_included_term: u64,
    offset: u64,
    data: Vec<u8>,
    done: bool,
) -> (u64, u64, bool, u64) {
    let mut out = Effects::new();
    node.on_message(
        now,
        &mut ZeroRng,
        NodeId(1),
        RaftMsg::InstallSnapshot {
            term,
            leader_id: NodeId(1),
            last_included_index,
            last_included_term,
            offset,
            data,
            done,
            voters: BTreeSet::new(),
            learners: BTreeSet::new(),
        },
        &mut out,
    );
    out.sends
        .iter()
        .find_map(|(_, m)| match m {
            RaftMsg::InstallSnapshotResp {
                term,
                last_included_index,
                installed,
                next_offset,
            } => Some((*term, *last_included_index, *installed, *next_offset)),
            _ => None,
        })
        .expect("the follower must reply InstallSnapshotResp to a chunk")
}

#[test]
fn chunked_install_first_chunk_resets_then_appends_then_installs_on_done() {
    // A 3-chunk snapshot transfer (bytes [A][B][C], chunk size 1): the first chunk
    // (offset 0) seeds the buffer, the middle chunk appends at offset 1, and the final
    // chunk (offset 2, done) installs. Only the LAST reply is `installed`; the earlier two
    // are progress acks reporting the next expected offset. The installed snapshot is the
    // concatenation [A][B][C], byte-identical to a single whole-snapshot install.
    let (mut node, now) = chunk_rx_follower(5);
    assert!(node.storage().load_snapshot().is_none());

    // offset 0: first chunk, buffer reset + seeded. Not done -> ack next_offset == 1.
    let (t0, idx0, inst0, next0) = deliver_chunk(&mut node, now, 5, 9, 5, 0, vec![b'A'], false);
    assert_eq!((t0, idx0, inst0, next0), (5, 9, false, 1));
    assert!(
        node.storage().load_snapshot().is_none(),
        "no install before done"
    );

    // offset 1: append. Not done -> ack next_offset == 2.
    let (_, _, inst1, next1) = deliver_chunk(&mut node, now, 5, 9, 5, 1, vec![b'B'], false);
    assert_eq!((inst1, next1), (false, 2));
    assert!(node.storage().load_snapshot().is_none(), "still no install");

    // offset 2: final chunk -> install. Reply installed == true, echoing index 9.
    let (_, idx2, inst2, _) = deliver_chunk(&mut node, now, 5, 9, 5, 2, vec![b'C'], true);
    assert_eq!((idx2, inst2), (9, true));
    let (meta, data) = node
        .storage()
        .load_snapshot()
        .expect("the snapshot must be installed on the done chunk");
    assert_eq!(meta.last_included_index, 9);
    assert_eq!(meta.last_included_term, 5);
    assert_eq!(
        data, b"ABC",
        "the reassembled bytes are the contiguous concatenation"
    );
    assert_eq!(
        node.commit_index(),
        9,
        "commit advances to the installed index"
    );
}

#[test]
fn chunked_install_single_chunk_equals_whole_snapshot() {
    // FORWARD-COMPAT: a single chunk at offset 0 with done == true installs the whole
    // snapshot in one message (byte-equivalent to the pre-PROD-9 whole-snapshot install).
    let (mut node, now) = chunk_rx_follower(7);
    let (_, idx, installed, _) =
        deliver_chunk(&mut node, now, 7, 3, 7, 0, b"whole-snapshot".to_vec(), true);
    assert_eq!((idx, installed), (3, true));
    let (meta, data) = node
        .storage()
        .load_snapshot()
        .expect("installed in one chunk");
    assert_eq!(meta.last_included_index, 3);
    assert_eq!(data, b"whole-snapshot");
}

#[test]
fn chunked_install_out_of_order_chunk_is_rejected_and_steers_offset() {
    // After offset 0 is buffered (length 1), a chunk arriving at the WRONG offset (offset 5,
    // a gap) is REJECTED: no install, the buffer is untouched, and the reply steers the
    // leader back to the offset actually expected (1). A DUPLICATE of offset 0 (re-seed) is
    // accepted (a fresh first chunk always restarts), but a stale GAP never corrupts the
    // buffer.
    let (mut node, now) = chunk_rx_follower(5);
    let (_, _, _, next0) = deliver_chunk(&mut node, now, 5, 9, 5, 0, vec![b'A'], false);
    assert_eq!(next0, 1);

    // A gap chunk at offset 5 (we only hold 1 byte): rejected, steer back to 1.
    let (_, _, inst_gap, next_gap) = deliver_chunk(&mut node, now, 5, 9, 5, 5, vec![b'Z'], false);
    assert_eq!(
        (inst_gap, next_gap),
        (false, 1),
        "an out-of-order chunk is rejected and the reply asks for the expected offset"
    );
    assert!(node.storage().load_snapshot().is_none());

    // The correct offset-1 chunk now resumes cleanly to a done install.
    let (_, _, _, next1) = deliver_chunk(&mut node, now, 5, 9, 5, 1, vec![b'B'], false);
    assert_eq!(next1, 2);
    let (_, _, inst2, _) = deliver_chunk(&mut node, now, 5, 9, 5, 2, vec![b'C'], true);
    assert!(
        inst2,
        "the transfer completes after the gap was rejected + retried"
    );
    assert_eq!(node.storage().load_snapshot().unwrap().1, b"ABC");
}

#[test]
fn chunked_install_duplicate_first_chunk_restarts_cleanly() {
    // A DUPLICATED / re-sent first chunk (offset 0) always restarts the buffer, so a
    // retransmit after a lost ack does not splice bytes. Send offset 0 twice, then finish.
    let (mut node, now) = chunk_rx_follower(5);
    deliver_chunk(&mut node, now, 5, 9, 5, 0, vec![b'A'], false);
    // A second offset-0 chunk (a retransmit) resets the buffer to just [A] again.
    let (_, _, _, next_dup) = deliver_chunk(&mut node, now, 5, 9, 5, 0, vec![b'A'], false);
    assert_eq!(
        next_dup, 1,
        "the re-sent first chunk restarts at length 1, not 2"
    );
    let (_, _, _, _) = deliver_chunk(&mut node, now, 5, 9, 5, 1, vec![b'B'], true);
    assert_eq!(
        node.storage().load_snapshot().unwrap().1,
        b"AB",
        "the duplicate did not double the first chunk's bytes"
    );
}

#[test]
fn chunked_install_stale_term_chunk_is_rejected_without_buffering() {
    // A chunk from a STALE-TERM leader (term below ours) is rejected: no install, no buffer,
    // and the reply carries OUR higher term (so the stale leader steps down) + installed
    // false. We never start a transfer for a leader we have already moved past.
    let (mut node, now) = chunk_rx_follower(9);
    let (resp_term, _, installed, next) =
        deliver_chunk(&mut node, now, 4, 100, 4, 0, vec![b'X'], true);
    assert_eq!(resp_term, 9, "the reply carries our higher term");
    assert!(!installed, "a stale-term snapshot is never installed");
    assert_eq!(next, 0);
    assert!(
        node.storage().load_snapshot().is_none(),
        "no snapshot is persisted from a stale-term leader"
    );
}

#[test]
fn leader_slices_snapshot_into_bounded_chunks() {
    // PROD-9 LEADER SLICING: a single-voter leader with a snapshot larger than the chunk
    // size, asked to catch up a lagging peer, emits the FIRST bounded chunk (offset 0,
    // data.len() == chunk size, done == false). The chunk is strictly under the bus frame
    // bound, and a follow-up resp advances to the next chunk.
    let voters: BTreeSet<NodeId> = [NodeId(1), NodeId(2)].into_iter().collect();
    let mut storage = MemStorage::new();
    storage.set_current_term(3);
    // A snapshot of 10 bytes; chunk size 4 -> chunks of [4,4,2].
    storage.save_snapshot(
        SnapshotMeta {
            last_included_index: 6,
            last_included_term: 3,
        },
        b"0123456789",
    );
    storage.compact_to(6);
    let config = RaftConfig {
        snapshot_chunk_bytes: 4,
        ..RaftConfig::default()
    };
    let mut leader = RaftNode::new(NodeId(1), voters, storage, config);
    leader.role = Role::Leader;
    leader.next_index.insert(NodeId(2), 1); // below the snapshot -> InstallSnapshot path
    leader.match_index.insert(NodeId(2), 0);

    let mut out = Effects::new();
    leader.send_append_entries_to(NodeId(2), &mut out);
    let (offset, len, done) = out
        .sends
        .iter()
        .find_map(|(to, m)| match m {
            RaftMsg::InstallSnapshot {
                offset, data, done, ..
            } if *to == NodeId(2) => Some((*offset, data.len(), *done)),
            _ => None,
        })
        .expect("the leader must ship a chunked InstallSnapshot");
    assert_eq!(offset, 0, "the first chunk starts at offset 0");
    assert_eq!(len, 4, "the chunk is bounded by snapshot_chunk_bytes (4)");
    assert!(
        !done,
        "a 10-byte snapshot at chunk size 4 is not done on the first chunk"
    );
    // The chunk size (and thus every chunk) is bounded by the config knob, which production
    // sets well under the cluster-bus frame bound; the adapter codec asserts the frame-bound
    // relationship directly (the engine stays pure and never imports the runtime constant).
    assert!(
        len <= config.snapshot_chunk_bytes,
        "each chunk is bounded by the chunk size"
    );

    // The leader tracks per-follower progress; an ack at offset 4 ships the next chunk.
    let mut out2 = Effects::new();
    leader.on_message(
        Monotonic::from_since_origin(Duration::ZERO),
        &mut ZeroRng,
        NodeId(2),
        RaftMsg::InstallSnapshotResp {
            term: 3,
            last_included_index: 6,
            installed: false,
            next_offset: 4,
        },
        &mut out2,
    );
    let next_offset = out2.sends.iter().find_map(|(to, m)| match m {
        RaftMsg::InstallSnapshot { offset, .. } if *to == NodeId(2) => Some(*offset),
        _ => None,
    });
    assert_eq!(
        next_offset,
        Some(4),
        "the next chunk continues from the acked offset"
    );
}
