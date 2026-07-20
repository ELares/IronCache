// SPDX-License-Identifier: MIT OR Apache-2.0
//! The LIVE clustered rolling-upgrade DRIVER (#392 Phase 3): the `impl UpgradeActions` that turns
//! the pure sequence [`ironcache_repl::upgrade_step`] into real cluster I/O.
//!
//! The pure decision layer already exists: [`ironcache_repl::run_rolling_upgrade`] walks the fixed
//! node-by-node sequence (upgrade the replicas first, promote an upgraded in-sync replica, upgrade
//! the old primary last) through the [`ironcache_repl::UpgradeActions`] trait, and
//! [`crate::cluster_upgrade::ClusterView`] turns a per-tick snapshot into the five `upgrade_step`
//! inputs + the promotion candidate. This module supplies the LIVE half:
//!
//! - [`LiveCluster`] is the live `impl UpgradeActions`. It caches a per-tick [`ClusterView`]
//!   ([`LiveCluster::refresh`]) and delegates the five observe getters to it; it executes the three
//!   act methods over the wire.
//! - The OBSERVE wire layer builds that view from the authenticated RESP surface: per-node `INFO`
//!   (role / self-version / `master_link_status` / the master-side per-replica `lag=`) plus
//!   `CLUSTER SHARDS` (live member set) and `CLUSTER INFO` (the raft quorum signal). Versions run
//!   through ONE shared [`normalize_version`] so the EXACT-STRING upgraded check is stable.
//! - [`NodeUpgrader`] is the out-of-band per-node binary swap. The prod [`SshUpgrader`] SSH-invokes
//!   the already-hardened single-node `ironcache upgrade` on the node host (its own verify -> SAVE
//!   -> atomic swap -> restart -> health-gate -> auto-rollback); tests use a stub.
//! - [`LiveCluster::promote_candidate`] is the FAILOVER-FREEZE fence, the load-bearing RPO=0
//!   mechanism: `CLIENT PAUSE WRITE` on the OLD PRIMARY (freeze acked writes at head H, via the
//!   already-shipped [`crate::upgrade::pause::Pauser`] seam) -> poll the master-side view of the
//!   chosen candidate's lag until it reaches EXACTLY 0 (the candidate has applied through H) -> only
//!   then `CLUSTER FAILOVER` to the candidate. On a drain timeout it FAILS CLOSED (abort, unpause,
//!   error), so a stuck drain never promotes a lagging candidate.
//!
//! Scope: ONE shard being rolled, matching the pure layer's single-primary model. The RESP client
//! ([`RespClusterClient`]) and the SSH actuator ([`SshUpgrader`]) are thin, clearly-marked
//! out-of-band actuators; their real transport is exercised by the separate acceptance / docker
//! layers (the pure driver logic here is unit-tested with mocks).

use std::collections::BTreeSet;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};

use ironcache_repl::{
    LinkStatus, PromotionSafety, ReplOffset, ReplicaLag, UpgradeActions, UpgradeReport,
    run_rolling_upgrade,
};

use crate::cluster_upgrade::{ClusterView, NodeRole, NodeView};
use crate::upgrade::pause::{PauseError, PauseTarget, Pauser};

// ---------------------------------------------------------------------------
// Version normalization (ONE shared normalizer, load-bearing for the exact-match upgraded check)
// ---------------------------------------------------------------------------

/// Normalize a self-reported version to the canonical form the driver matches on (#392): strip a
/// leading `v`/`V` tag on a numeric version (`v1.2.3` -> `1.2.3`), drop SemVer build metadata
/// (everything from the first `+`), and trim surrounding whitespace.
///
/// BOTH the observed node version and the `target_version` MUST pass through this ONE function before
/// the `ClusterView`'s exact-string `is_upgraded` check; otherwise a formatting skew leaves
/// `replicas_to_upgrade` never reaching 0 and the roll stalls (fail-closed, but no progress).
#[must_use]
pub fn normalize_version(raw: &str) -> String {
    let trimmed = raw.trim();
    // Strip a single leading `v`/`V` ONLY when it prefixes a numeric version, never a version that
    // legitimately starts with a letter.
    let unprefixed = match trimmed.strip_prefix(['v', 'V']) {
        Some(rest) if rest.starts_with(|c: char| c.is_ascii_digit()) => rest,
        _ => trimmed,
    };
    // Drop build metadata (from the first `+`) and re-trim.
    let base = unprefixed.split('+').next().unwrap_or(unprefixed);
    base.trim().to_owned()
}

// ---------------------------------------------------------------------------
// Static actuation map (the "C hybrid" inventory: node-id -> how to reach + actuate it)
// ---------------------------------------------------------------------------

/// The static, per-node actuation info the driver needs but cannot discover live: how to REACH the
/// node over authenticated RESP (`resp_addr` + optional `auth`) and how to ACTUATE its out-of-band
/// binary swap (`ssh` target + `upgrade_source`). The dynamic topology (roles / versions / lag /
/// membership) is discovered live each tick; this is the stable half of the hybrid inventory.
///
/// PRIVACY: the `ssh` target and `resp_addr` come entirely from the operator-supplied map; the driver
/// hardcodes no host / user / credential.
#[derive(Debug, Clone)]
pub struct ActuationTarget {
    /// The node's announce id (the `CLUSTER FAILOVER` / promotion target, and the inventory key).
    pub id: String,
    /// The authenticated RESP `host:port` for `INFO` / `CLUSTER` / `CLIENT PAUSE` / `CLUSTER FAILOVER`.
    pub resp_addr: String,
    /// The optional `requirepass` (sent only over the RESP socket, never logged / argv).
    pub auth: Option<String>,
    /// The SSH target for the out-of-band binary swap (opaque, e.g. `user@host`); from the map only.
    pub ssh: String,
    /// The single-node upgrade source args to pass through, e.g. `--to v1.2.3`.
    pub upgrade_source: String,
}

/// The static actuation map for the shard being rolled: `id -> ActuationTarget`, plus the seed
/// discovery order (the nodes to try `CLUSTER SHARDS` / `CLUSTER INFO` against).
#[derive(Debug, Clone)]
pub struct Inventory {
    nodes: Vec<ActuationTarget>,
}

impl Inventory {
    /// Build an inventory from the per-node actuation targets (the discovery seed order is the given
    /// order).
    #[must_use]
    pub fn new(nodes: Vec<ActuationTarget>) -> Self {
        Self { nodes }
    }

    /// The actuation target for `id`, if present.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&ActuationTarget> {
        self.nodes.iter().find(|t| t.id == id)
    }

    /// Iterate the actuation targets (inventory / seed order).
    pub fn iter(&self) -> impl Iterator<Item = &ActuationTarget> {
        self.nodes.iter()
    }

    /// The number of inventory nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the inventory is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The static inventory id-set (cross-checked against the live-discovered set each tick).
    #[must_use]
    pub fn ids(&self) -> BTreeSet<String> {
        self.nodes.iter().map(|t| t.id.clone()).collect()
    }
}

// ---------------------------------------------------------------------------
// The observe seams (mockable): RESP cluster client + the out-of-band node upgrader + a pacing seam
// ---------------------------------------------------------------------------

/// One replica's entry as seen from the MASTER's `INFO # Replication`
/// (`slaveN:ip=..,port=..,state=online,offset=..,lag=..`). The master-side `lag=` is authoritative
/// per HA-7e (the primary knows exactly `head - acked` for each replica).
#[derive(Debug, Clone)]
pub struct SlaveEntry {
    /// The replica's advertised host (matched against its inventory `resp_addr` host).
    pub host: String,
    /// The replica's advertised port.
    pub port: u16,
    /// The master-side lag for this replica (`lag=`), in logical writes.
    pub lag: u64,
}

/// One node's observed replication state, parsed from its `INFO` (role / self-version / link, plus
/// the master's per-replica `slaveN` view when this node is the master). Versions are RAW here;
/// [`LiveCluster::refresh`] normalizes them through [`normalize_version`].
#[derive(Debug, Clone)]
pub struct NodeObservation {
    /// The node's replication role for the shard.
    pub role: NodeRole,
    /// The node's self-reported `ironcache_version` (raw, pre-normalization).
    pub version: String,
    /// A REPLICA's link status to its master (`master_link_status`); `Down` / unused for a master.
    pub link: LinkStatus,
    /// A MASTER's per-replica `slaveN` view (empty for a replica).
    pub slaves: Vec<SlaveEntry>,
    /// The node's internal shard count, from INFO `io_threads_active` (#731). `1` when the field is
    /// absent (an older node, or one that does not report it) so we never SPURIOUSLY refuse a roll;
    /// the pre-flight refuses only on a node that AFFIRMATIVELY reports `> 1` (the HA replica path is
    /// single-shard, so a multi-shard node cannot deliver RPO=0 -- it would serve ~1/N after promotion).
    pub shards: usize,
}

/// The raft quorum signal parsed from `CLUSTER INFO` (#392): quorum is `cluster_state:ok` AND a
/// recognized `cluster_raft_leader` (a leader that can COMMIT the `PromoteReplica` fence).
#[derive(Debug, Clone, Copy)]
pub struct QuorumObservation {
    /// `cluster_state:ok`.
    pub state_ok: bool,
    /// A `cluster_raft_leader` that is present (not `none` / empty).
    pub raft_leader_present: bool,
}

impl QuorumObservation {
    /// Whether the config-plane raft has a majority quorum (both signals hold).
    #[must_use]
    pub fn has_quorum(self) -> bool {
        self.state_ok && self.raft_leader_present
    }
}

/// The authenticated-RESP OBSERVE + FAILOVER seam (#392), abstracted so the driver logic is
/// unit-tested with a mock. The prod impl is [`RespClusterClient`].
pub trait ClusterClient {
    /// Discover the live member id-set (topology half of the hybrid inventory) from a seed via
    /// `CLUSTER SHARDS`.
    ///
    /// # Errors
    /// A transport / protocol failure reaching the seed.
    fn discover_members(
        &mut self,
        seed: &ActuationTarget,
    ) -> Result<Vec<String>, ClusterUpgradeError>;

    /// Observe one node's replication state via `INFO` (role / version / link + the master's
    /// per-replica view).
    ///
    /// # Errors
    /// A transport / protocol failure reaching the node.
    fn observe_node(
        &mut self,
        node: &ActuationTarget,
    ) -> Result<NodeObservation, ClusterUpgradeError>;

    /// Observe the raft quorum signal from a seed via `CLUSTER INFO`.
    ///
    /// # Errors
    /// A transport / protocol failure reaching the seed.
    fn observe_quorum(
        &mut self,
        seed: &ActuationTarget,
    ) -> Result<QuorumObservation, ClusterUpgradeError>;

    /// Commit the promotion of `node` (the chosen candidate) via `CLUSTER FAILOVER` (the server
    /// re-checks its OWN in-sync gate + proposes the committed `PromoteReplica` epoch fence).
    ///
    /// # Errors
    /// A refusal (`-` reply) or a transport failure.
    fn cluster_failover(&mut self, node: &ActuationTarget) -> Result<(), ClusterUpgradeError>;
}

/// The out-of-band per-node BINARY-SWAP seam (#392 slice 1): drive one node's self-updater. The prod
/// [`SshUpgrader`] SSH-invokes the already-hardened single-node `ironcache upgrade` on the node host;
/// tests use a stub. Isolated behind this trait because it is the one non-containerizable, privileged
/// actuator.
pub trait NodeUpgrader {
    /// Upgrade the node named by `target` in place (its own verify -> SAVE -> swap -> restart ->
    /// health-gate -> auto-rollback). Returns once that node is up on the new binary; the driver
    /// observes the re-attach / in-sync on the next [`LiveCluster::refresh`].
    ///
    /// # Errors
    /// The node upgrade could not be started / completed.
    fn upgrade(&mut self, target: &ActuationTarget) -> Result<(), ClusterUpgradeError>;
}

/// The loop-pacing seam (the backoff between ticks and between drain polls), abstracted so tests run
/// with a no-op / state-advancing sleeper instead of wall-clock delays.
pub trait Sleeper {
    /// Pause for `dur` (prod: a real sleep; tests: advance simulated state / no-op).
    fn sleep(&self, dur: Duration);
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A clustered rolling-upgrade failure. The driver surfaces it and STOPS rather than pressing on
/// across a broken / drifting cluster (#392 fails closed).
#[derive(Debug, thiserror::Error)]
pub enum ClusterUpgradeError {
    /// A RESP transport / connect / auth / protocol failure.
    #[error("RESP {op}: {detail}")]
    Client {
        /// The operation that failed (connect / AUTH / resp / ...).
        op: String,
        /// The failure detail.
        detail: String,
    },
    /// An `INFO` observation could not be read / parsed for a node.
    #[error("observing node {node}: {detail}")]
    Observe {
        /// The node id.
        node: String,
        /// The failure detail.
        detail: String,
    },
    /// The live member set could not be discovered from any seed.
    #[error("discovering cluster members: {detail}")]
    Discover {
        /// The failure detail.
        detail: String,
    },
    /// A `CLUSTER FAILOVER` was refused / failed.
    #[error("promoting {node} via CLUSTER FAILOVER: {detail}")]
    Failover {
        /// The candidate node id.
        node: String,
        /// The refusal / failure detail.
        detail: String,
    },
    /// A per-node binary upgrade could not be started / completed.
    #[error("upgrading node {node}: {detail}")]
    Upgrade {
        /// The node id.
        node: String,
        /// The failure detail.
        detail: String,
    },
    /// The failover-freeze pause / unpause seam failed (could not establish / release the freeze).
    #[error("failover-freeze: {0}")]
    Pause(#[from] PauseError),
    /// The live-discovered member set does not match the static inventory (a node was added / removed
    /// mid-roll): the `old_primary_id`-present contract is broken, so the roll aborts loudly.
    #[error("cluster membership drift: inventory={inventory} discovered={discovered}")]
    MembershipDrift {
        /// The static inventory id-set.
        inventory: String,
        /// The live-discovered id-set.
        discovered: String,
    },
    /// The old primary id has not been captured yet (no master observed at the first refresh).
    #[error("the pre-roll primary id is not known (no master observed)")]
    OldPrimaryUnknown,
    /// An id referenced by the driver is missing from the inventory.
    #[error("node {id} is not in the inventory")]
    UnknownNode {
        /// The missing id.
        id: String,
    },
    /// `promote_candidate` found no promotable candidate (defensive: `upgrade_step` should only emit
    /// Promote when one exists).
    #[error("no promotable candidate at the promote step")]
    NoCandidate,
    /// `upgrade_next_replica` found no not-yet-upgraded replica (defensive).
    #[error("no replica left to upgrade at the upgrade-replica step")]
    NoReplicaToUpgrade,
    /// The failover-freeze drain did not reach lag 0 within the poll budget: FAIL CLOSED (unpause, no
    /// failover) rather than promote a candidate that may be missing acknowledged writes.
    #[error(
        "failover-freeze drain timed out for candidate {candidate} after {polls} polls (fail-closed: no failover)"
    )]
    DrainTimeout {
        /// The candidate that did not drain to 0.
        candidate: String,
        /// The number of polls attempted.
        polls: u32,
    },
    /// #731: a node reports `shards > 1`. The HA cluster-replica path is single-shard by design
    /// (a replica full-syncs the WHOLE keyspace into ONE internal shard), so a multi-shard node
    /// promoted on failover would serve only ~1/N of its keyspace -- the driver CANNOT deliver its
    /// RPO=0 contract. FAIL CLOSED at the pre-flight rather than roll into silent partial-data.
    #[error(
        "node {node} has shards={shards} (>1): the HA replica path is single-shard, so `ironcache upgrade --cluster` cannot guarantee RPO=0 on it (a promoted replica would serve ~1/{shards} of its keyspace). Set `shards = 1` on every node for cluster replication / rolling upgrades (multi-shard is fine for non-HA / throughput-only deployments) (#731)."
    )]
    MultiShardUnsupported {
        /// The node reporting more than one shard.
        node: String,
        /// The reported shard count.
        shards: usize,
    },
}

// ---------------------------------------------------------------------------
// Pure parsers (unit-tested against fixtures)
// ---------------------------------------------------------------------------

/// Parse the `INFO # Server` + `# Replication` facts into a [`NodeObservation`] (role / raw version /
/// link + the master's per-replica `slaveN` view). Lenient: unknown lines are ignored, a missing
/// field falls back to the standalone-master default (role master, empty version, link down).
#[must_use]
pub fn parse_info_replication(info: &str) -> NodeObservation {
    let mut role = NodeRole::Master;
    let mut version = String::new();
    let mut link = LinkStatus::Down;
    let mut slaves = Vec::new();
    // #731: default 1 so an absent field (older node) never spuriously refuses a roll; only an
    // affirmative `io_threads_active:N` (N>1) trips the multi-shard pre-flight refusal.
    let mut shards = 1usize;
    for raw in info.lines() {
        let field = raw.trim_end_matches('\r');
        if let Some(v) = field.strip_prefix("role:") {
            role = if v.trim() == "replica" {
                NodeRole::Replica
            } else {
                NodeRole::Master
            };
        } else if let Some(v) = field.strip_prefix("ironcache_version:") {
            v.trim().clone_into(&mut version);
        } else if let Some(v) = field.strip_prefix("master_link_status:") {
            link = if v.trim() == "up" {
                LinkStatus::Up
            } else {
                LinkStatus::Down
            };
        } else if let Some(v) = field.strip_prefix("io_threads_active:") {
            // The node's internal shard count (ironcache-observe renders `server.shards` here). A
            // malformed value keeps the safe default of 1.
            if let Ok(n) = v.trim().parse::<usize>() {
                shards = n.max(1);
            }
        } else if let Some(rest) = strip_slave_prefix(field) {
            if let Some(entry) = parse_slave_line(rest) {
                slaves.push(entry);
            }
        }
    }
    NodeObservation {
        role,
        version,
        link,
        slaves,
        shards,
    }
}

/// Match a `slaveN:` line and return the part after the `:` (the `ip=..,port=..,...` body), or
/// `None` if the line is not a numbered slave line.
fn strip_slave_prefix(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("slave")?;
    let (num, tail) = rest.split_once(':')?;
    (!num.is_empty() && num.bytes().all(|b| b.is_ascii_digit())).then_some(tail)
}

/// Parse a slave line body `ip=..,port=..,state=online,offset=..,lag=..` into a [`SlaveEntry`].
/// Requires `ip` + `port` + `lag`; a malformed key/value pair is skipped, a missing required field
/// yields `None`.
fn parse_slave_line(body: &str) -> Option<SlaveEntry> {
    let mut host = None;
    let mut port = None;
    let mut lag = None;
    for kv in body.split(',') {
        if let Some((k, v)) = kv.split_once('=') {
            match k.trim() {
                "ip" => host = Some(v.trim().to_owned()),
                "port" => port = v.trim().parse::<u16>().ok(),
                "lag" => lag = v.trim().parse::<u64>().ok(),
                _ => {}
            }
        }
    }
    Some(SlaveEntry {
        host: host?,
        port: port?,
        lag: lag?,
    })
}

/// Parse the `CLUSTER INFO` body into the raft [`QuorumObservation`] (#392): `cluster_state:ok` and a
/// present `cluster_raft_leader`. A verbatim `txt:` prefix line (RESP3) is ignored by the line scan.
#[must_use]
pub fn parse_cluster_info(info: &str) -> QuorumObservation {
    let mut state_ok = false;
    let mut raft_leader_present = false;
    for raw in info.lines() {
        let line = raw.trim_end_matches('\r');
        if let Some(v) = line.strip_prefix("cluster_state:") {
            state_ok = v.trim() == "ok";
        } else if let Some(v) = line.strip_prefix("cluster_raft_leader:") {
            let v = v.trim();
            raft_leader_present = !v.is_empty() && v != "none";
        }
    }
    QuorumObservation {
        state_ok,
        raft_leader_present,
    }
}

/// Split a `host:port` into its parts (rightmost `:` splits, so an IPv6-less `host:port` is handled).
fn split_host_port(addr: &str) -> Option<(&str, u16)> {
    let (host, port) = addr.rsplit_once(':')?;
    Some((host, port.parse::<u16>().ok()?))
}

/// The MASTER-side lag for the replica at `replica_resp_addr`, matched by its RESP endpoint against
/// the master's `slaveN` entries. `None` when the master does not (yet) list that replica.
fn slave_lag_for(slaves: &[SlaveEntry], replica_resp_addr: &str) -> Option<u64> {
    let (host, port) = split_host_port(replica_resp_addr)?;
    slaves
        .iter()
        .find(|s| s.host == host && s.port == port)
        .map(|s| s.lag)
}

/// Build a KNOWN [`ReplicaLag`] of exactly `n` logical writes (the master-side `lag=` figure). The
/// only public `ReplicaLag` constructors are `compute(head, acked)` and `unknown()`, so a known lag
/// of `n` is `compute(n, 0)` (`n - 0 = n`); the raw offsets are not otherwise needed here.
fn known_lag(n: u64) -> ReplicaLag {
    ReplicaLag::compute(ReplOffset(n), ReplOffset(0))
}

// ---------------------------------------------------------------------------
// The live driver
// ---------------------------------------------------------------------------

/// The failover-freeze fence tuning (#392): the `CLIENT PAUSE <ms> WRITE` window, the drain poll
/// budget, and the delay between drain polls.
#[derive(Debug, Clone)]
pub struct FreezeCfg {
    /// The `<ms>` for `CLIENT PAUSE <ms> WRITE` on the old primary (must comfortably cover the drain
    /// + the commit + a margin; it self-cancels once the old primary is demoted).
    pub pause_window_ms: u64,
    /// The maximum number of lag polls before the drain FAILS CLOSED (no failover).
    pub max_drain_polls: u32,
    /// The delay between drain polls.
    pub drain_poll_delay: Duration,
}

impl Default for FreezeCfg {
    fn default() -> Self {
        Self {
            pause_window_ms: 30_000,
            max_drain_polls: 600,
            drain_poll_delay: Duration::from_millis(100),
        }
    }
}

/// The per-tick loop pacing.
#[derive(Debug, Clone)]
pub struct PollCfg {
    /// The backoff between rolling-upgrade ticks (before each re-poll).
    pub tick_delay: Duration,
}

impl Default for PollCfg {
    fn default() -> Self {
        Self {
            tick_delay: Duration::from_millis(500),
        }
    }
}

/// The non-seam configuration for a [`LiveCluster`] (bundled so construction stays under the argument
/// limit).
#[derive(Debug, Clone)]
pub struct DriverConfig {
    /// The static actuation map.
    pub inventory: Inventory,
    /// The version being rolled TO (explicit; dev/lock builds pin `0.0.0`). Normalized on use.
    pub target_version: String,
    /// The in-sync lag bound the driver's gate uses (the server re-checks its own on failover).
    pub max_lag: u64,
    /// The loop pacing.
    pub poll: PollCfg,
    /// The failover-freeze tuning.
    pub freeze: FreezeCfg,
}

/// The LIVE `impl UpgradeActions` (#392): a thin shell over the pure [`ClusterView`]. It caches a
/// per-tick snapshot, delegates the five observe getters to it, and executes the three act methods
/// over the wire (`promote_candidate` being the failover-freeze fence).
pub struct LiveCluster<C: ClusterClient, U: NodeUpgrader> {
    client: C,
    upgrader: U,
    pauser: Box<dyn Pauser>,
    sleeper: Box<dyn Sleeper>,
    inventory: Inventory,
    target_version: String,
    max_lag: u64,
    /// Captured ONCE at the first refresh (load-bearing: roles flip on promotion, so the pre-roll
    /// primary id must be remembered or a demoted old primary is re-counted / re-promoted).
    old_primary_id: Option<String>,
    /// This tick's snapshot the observe getters read.
    view: ClusterView,
    poll: PollCfg,
    freeze: FreezeCfg,
}

impl<C: ClusterClient, U: NodeUpgrader> LiveCluster<C, U> {
    /// Build a live driver over the given seams + configuration. Construction does NO I/O; the first
    /// [`LiveCluster::refresh`] (done by [`run_cluster_upgrade`]) captures `old_primary_id` and the
    /// initial view.
    #[must_use]
    pub fn new(
        client: C,
        upgrader: U,
        pauser: Box<dyn Pauser>,
        sleeper: Box<dyn Sleeper>,
        config: DriverConfig,
    ) -> Self {
        let view = ClusterView {
            nodes: Vec::new(),
            target_version: normalize_version(&config.target_version),
            max_lag: config.max_lag,
            raft_quorum: false,
            old_primary_id: None,
        };
        Self {
            client,
            upgrader,
            pauser,
            sleeper,
            inventory: config.inventory,
            target_version: config.target_version,
            max_lag: config.max_lag,
            old_primary_id: None,
            view,
            poll: config.poll,
            freeze: config.freeze,
        }
    }

    /// The captured pre-roll primary id (present after the first refresh).
    #[must_use]
    pub fn old_primary_id(&self) -> Option<&str> {
        self.old_primary_id.as_deref()
    }

    /// The current cached view (for a pre-check / inspection).
    #[must_use]
    pub fn view(&self) -> &ClusterView {
        &self.view
    }

    /// OBSERVE the cluster and assemble this tick's [`ClusterView`]: discover the live member set,
    /// cross-check it against the static inventory, read each node's `INFO` (role / version / link +
    /// the master's per-replica lag), read the raft quorum, capture `old_primary_id` once, and cache
    /// + return the view.
    ///
    /// # Errors
    /// A transport / protocol failure, or membership drift (the live set != the inventory set).
    pub fn refresh(&mut self) -> Result<ClusterView, ClusterUpgradeError> {
        // 1. Discover the live member id-set (topology half of the hybrid inventory).
        let discovered: BTreeSet<String> = self.discover_members_any_seed()?.into_iter().collect();
        // 2. Membership cross-check: the live set MUST equal the static inventory set (drift aborts).
        let inventory_ids = self.inventory.ids();
        if discovered != inventory_ids {
            return Err(ClusterUpgradeError::MembershipDrift {
                inventory: join_ids(&inventory_ids),
                discovered: join_ids(&discovered),
            });
        }
        // 3. Observe each node's replication state.
        let mut observed: Vec<(ActuationTarget, NodeObservation)> =
            Vec::with_capacity(self.inventory.len());
        for target in self.inventory.iter() {
            let obs = self.client.observe_node(target)?;
            observed.push((target.clone(), obs));
        }
        // 3b. #731 PRE-FLIGHT: refuse a node with shards > 1. The HA cluster-replica path is
        // single-shard (a replica full-syncs the whole keyspace into ONE internal shard), so a
        // multi-shard node promoted on the failover-freeze fence would serve only ~1/N of its
        // keyspace -- the driver's RPO=0 contract is unachievable. FAIL CLOSED here (before any
        // upgrade/promote action) rather than silently roll into partial data after a failover.
        if let Some((target, obs)) = observed.iter().find(|(_, o)| o.shards > 1) {
            return Err(ClusterUpgradeError::MultiShardUnsupported {
                node: target.id.clone(),
                shards: obs.shards,
            });
        }
        // 4. The MASTER's per-replica view is authoritative for lag (HA-7e).
        let master_slaves: Vec<SlaveEntry> = observed
            .iter()
            .find(|(_, o)| o.role == NodeRole::Master)
            .map(|(_, o)| o.slaves.clone())
            .unwrap_or_default();
        // 5. Assemble the NodeViews (versions through the ONE shared normalizer).
        let mut nodes = Vec::with_capacity(observed.len());
        for (target, obs) in &observed {
            let version = normalize_version(&obs.version);
            let (link, lag) = match obs.role {
                NodeRole::Master => (LinkStatus::Down, None),
                NodeRole::Replica if obs.link.is_up() => {
                    // Master-side lag for THIS replica; unknown if the master does not list it.
                    let lag = slave_lag_for(&master_slaves, &target.resp_addr)
                        .map_or_else(ReplicaLag::unknown, known_lag);
                    (LinkStatus::Up, Some(lag))
                }
                NodeRole::Replica => (LinkStatus::Down, Some(ReplicaLag::unknown())),
            };
            nodes.push(NodeView {
                id: target.id.clone(),
                version,
                role: obs.role,
                link,
                lag,
            });
        }
        // 6. Capture old_primary_id ONCE (the current master), the first time a master is observed.
        if self.old_primary_id.is_none() {
            if let Some(master) = nodes.iter().find(|n| n.role == NodeRole::Master) {
                self.old_primary_id = Some(master.id.clone());
            }
        }
        // 7. Raft quorum from a reachable seed.
        let raft_quorum = self.observe_quorum_any_seed()?.has_quorum();
        // 8. Assemble + cache the per-tick view.
        let view = ClusterView {
            nodes,
            target_version: normalize_version(&self.target_version),
            max_lag: self.max_lag,
            raft_quorum,
            old_primary_id: self.old_primary_id.clone(),
        };
        self.view = view.clone();
        Ok(view)
    }

    /// Discover the live member set from the first reachable seed (try each inventory node in order).
    fn discover_members_any_seed(&mut self) -> Result<Vec<String>, ClusterUpgradeError> {
        let mut last: Option<ClusterUpgradeError> = None;
        for seed in self.inventory.iter() {
            match self.client.discover_members(seed) {
                Ok(ids) => return Ok(ids),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or(ClusterUpgradeError::Discover {
            detail: "no inventory nodes to query".to_owned(),
        }))
    }

    /// Read the raft quorum signal from the first reachable seed.
    fn observe_quorum_any_seed(&mut self) -> Result<QuorumObservation, ClusterUpgradeError> {
        let mut last: Option<ClusterUpgradeError> = None;
        for seed in self.inventory.iter() {
            match self.client.observe_quorum(seed) {
                Ok(q) => return Ok(q),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or(ClusterUpgradeError::Discover {
            detail: "no inventory nodes to read quorum from".to_owned(),
        }))
    }

    /// Drive one node's out-of-band binary swap by id.
    fn upgrade_node(&mut self, id: &str) -> Result<(), ClusterUpgradeError> {
        let target = self
            .inventory
            .get(id)
            .ok_or_else(|| ClusterUpgradeError::UnknownNode { id: id.to_owned() })?
            .clone();
        self.upgrader.upgrade(&target)
    }

    /// Poll the OLD PRIMARY's master-side view of the candidate's lag until it reaches EXACTLY 0
    /// (drained through the frozen head H), or FAIL CLOSED with [`ClusterUpgradeError::DrainTimeout`]
    /// after the poll budget. No `CLUSTER FAILOVER` is issued from here; the caller commits only on
    /// `Ok`.
    fn drain_candidate_to_zero(
        &mut self,
        old_primary: &ActuationTarget,
        candidate: &ActuationTarget,
    ) -> Result<(), ClusterUpgradeError> {
        for _ in 0..self.freeze.max_drain_polls {
            let obs = self.client.observe_node(old_primary)?;
            if slave_lag_for(&obs.slaves, &candidate.resp_addr) == Some(0) {
                return Ok(());
            }
            self.sleeper.sleep(self.freeze.drain_poll_delay);
        }
        Err(ClusterUpgradeError::DrainTimeout {
            candidate: candidate.id.clone(),
            polls: self.freeze.max_drain_polls,
        })
    }
}

impl<C: ClusterClient, U: NodeUpgrader> UpgradeActions for LiveCluster<C, U> {
    type Error = ClusterUpgradeError;

    fn replicas_to_upgrade(&self) -> usize {
        self.view.replicas_to_upgrade()
    }
    fn replica_catching_up(&self) -> bool {
        self.view.replica_catching_up()
    }
    fn promotion_safety(&self) -> PromotionSafety {
        self.view.promotion_safety()
    }
    fn primary_demoted(&self) -> bool {
        self.view.primary_demoted()
    }
    fn old_primary_upgraded(&self) -> bool {
        self.view.old_primary_upgraded()
    }
    fn master_on_target(&self) -> bool {
        self.view.master_on_target()
    }

    fn upgrade_next_replica(&mut self) -> Result<(), Self::Error> {
        // The next not-yet-upgraded replica, DETERMINISTIC by id (least id first).
        let target_version = self.view.target_version.clone();
        let next_id = self
            .view
            .nodes
            .iter()
            .filter(|n| n.role == NodeRole::Replica && n.version != target_version)
            .map(|n| n.id.clone())
            .min()
            .ok_or(ClusterUpgradeError::NoReplicaToUpgrade)?;
        self.upgrade_node(&next_id)
    }

    /// The FAILOVER-FREEZE fence (#392, the load-bearing RPO=0 mechanism): freeze acked writes on the
    /// OLD PRIMARY, drain the chosen candidate to lag EXACTLY 0, and only then commit the failover.
    /// FAILS CLOSED on a drain timeout (unpause, no failover).
    fn promote_candidate(&mut self) -> Result<(), Self::Error> {
        // Choose the least-lagging upgraded in-sync replica from THIS tick's view.
        let candidate_id = self
            .view
            .select_promote_candidate()
            .map(|n| n.id.clone())
            .ok_or(ClusterUpgradeError::NoCandidate)?;
        let old_primary_id = self
            .old_primary_id
            .clone()
            .ok_or(ClusterUpgradeError::OldPrimaryUnknown)?;
        let old_primary = self
            .inventory
            .get(&old_primary_id)
            .ok_or_else(|| ClusterUpgradeError::UnknownNode {
                id: old_primary_id.clone(),
            })?
            .clone();
        let candidate = self
            .inventory
            .get(&candidate_id)
            .ok_or_else(|| ClusterUpgradeError::UnknownNode {
                id: candidate_id.clone(),
            })?
            .clone();

        // (a) FREEZE: CLIENT PAUSE <ms> WRITE on the OLD PRIMARY. No write is acked past its head H
        //     while the freeze holds (the shipped Pauser seam, pointed at the old primary).
        let pause_target = PauseTarget {
            resp_addr: old_primary.resp_addr.clone(),
            auth: old_primary.auth.clone(),
            window_ms: self.freeze.pause_window_ms,
        };
        self.pauser.freeze(&pause_target)?;

        // (b) DRAIN: poll the OLD PRIMARY's master-side view of the candidate's lag until EXACTLY 0
        //     (the candidate has applied through H). FAIL CLOSED on a drain timeout: unpause and
        //     refuse, so a lagging candidate is NEVER promoted.
        if let Err(e) = self.drain_candidate_to_zero(&old_primary, &candidate) {
            let _ = self.pauser.unfreeze(&pause_target);
            return Err(e);
        }

        // (c) COMMIT: only now, with the candidate provably holding every acked write, issue
        //     CLUSTER FAILOVER (the server re-checks in-sync + commits the PromoteReplica epoch
        //     fence). On failure, unpause and surface (fail closed).
        if let Err(e) = self.client.cluster_failover(&candidate) {
            let _ = self.pauser.unfreeze(&pause_target);
            return Err(e);
        }

        // (d) UNPAUSE for cleanliness: the old primary is demoted (a refresh confirms), so it no
        //     longer accepts writes, but release the freeze so nothing lingers.
        let _ = self.pauser.unfreeze(&pause_target);
        Ok(())
    }

    fn upgrade_old_primary(&mut self) -> Result<(), Self::Error> {
        let id = self
            .old_primary_id
            .clone()
            .ok_or(ClusterUpgradeError::OldPrimaryUnknown)?;
        self.upgrade_node(&id)
    }

    fn wait_for_progress(&mut self) -> Result<(), Self::Error> {
        // Pace the loop, then RE-OBSERVE (refresh is the natural re-poll point between ticks).
        self.sleeper.sleep(self.poll.tick_delay);
        self.refresh()?;
        Ok(())
    }
}

/// Join a sorted id-set for an error message.
fn join_ids(ids: &BTreeSet<String>) -> String {
    ids.iter().cloned().collect::<Vec<_>>().join(",")
}

/// Drive the WHOLE clustered rolling upgrade of ONE shard (#392): first refresh (captures
/// `old_primary_id` + validates membership), skip a shard that is ALREADY fully upgraded (idempotent
/// no-op, no gratuitous failover), else drive the pure [`run_rolling_upgrade`] to completion.
///
/// # Errors
/// The first cluster action / observation error, or membership drift.
pub fn run_cluster_upgrade<C: ClusterClient, U: NodeUpgrader>(
    live: &mut LiveCluster<C, U>,
    max_ticks: usize,
) -> Result<UpgradeReport, ClusterUpgradeError> {
    live.refresh()?;
    if live.view().shard_fully_upgraded() {
        // Every node already on the target: a re-run / target == current is a no-op (no CLUSTER
        // FAILOVER, no per-node swap).
        return Ok(UpgradeReport::Completed);
    }
    run_rolling_upgrade(live, max_ticks)
}

// ---------------------------------------------------------------------------
// Prod out-of-band actuators (thin, clearly-marked; real transport is exercised by later layers)
// ---------------------------------------------------------------------------

/// The prod per-node binary-swap actuator (#392 slice 1): SSH-invoke the already-hardened single-node
/// `ironcache upgrade` on the node host. THIN + OUT-OF-BAND + PRIVILEGED: the real host/user and the
/// credential live entirely in the static actuation target (never hardcoded here); this is a minimal
/// `std::process::Command` wrapper. Not unit-tested in-process (the single-node upgrade is covered at
/// its own layer; the end-to-end swap is a docker smoke test).
#[derive(Debug, Clone)]
pub struct SshUpgrader {
    /// The remote binary name to invoke (default `ironcache`).
    remote_binary: String,
    /// Extra args appended after the upgrade source (default `--yes` for non-interactive runs).
    extra_args: Vec<String>,
}

impl Default for SshUpgrader {
    fn default() -> Self {
        Self {
            remote_binary: "ironcache".to_owned(),
            extra_args: vec!["--yes".to_owned()],
        }
    }
}

impl SshUpgrader {
    /// A prod SSH upgrader with the default remote binary + args.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl NodeUpgrader for SshUpgrader {
    fn upgrade(&mut self, target: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
        // `ssh <target.ssh> <remote_binary> upgrade <upgrade_source...> <extra_args...>`. The remote
        // single-node upgrade runs its own verify -> SAVE -> swap -> restart -> health-gate ->
        // auto-rollback and only exits 0 once healthy on the new binary.
        let mut cmd = std::process::Command::new("ssh");
        cmd.arg(&target.ssh).arg(&self.remote_binary).arg("upgrade");
        for a in target.upgrade_source.split_whitespace() {
            cmd.arg(a);
        }
        for a in &self.extra_args {
            cmd.arg(a);
        }
        let output = cmd.output().map_err(|e| ClusterUpgradeError::Upgrade {
            node: target.id.clone(),
            detail: format!("spawning the remote upgrade: {e}"),
        })?;
        if !output.status.success() {
            return Err(ClusterUpgradeError::Upgrade {
                node: target.id.clone(),
                detail: format!(
                    "the remote `{} upgrade` exited {:?}: {}",
                    self.remote_binary,
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            });
        }
        Ok(())
    }
}

/// A per-node binary-swap actuator that runs an operator-supplied LOCAL COMMAND instead of SSH: the
/// [`SshUpgrader`] alternative for deployments that actuate a node's swap through a container
/// orchestrator, `systemd`, or a config-management runner rather than an interactive SSH login (and
/// the seam the #630 docker rolling-upgrade smoke drives, so the live composition can be exercised
/// without an sshd in the distroless node image). The command TEMPLATE is a whitespace-split
/// `program arg...` in which the placeholders `{id}`, `{source}`, and `{target}` are replaced, per
/// node, by that node's inventory `id`, `upgrade_source`, and `ssh` target respectively. Like
/// [`SshUpgrader`] it is a thin `std::process::Command` wrapper that MUST exit 0 only once the node
/// is up on the new binary; the driver observes the re-attach / in-sync on the next
/// [`LiveCluster::refresh`].
///
/// Example (#630 docker smoke): `docker compose up -d --force-recreate --wait {id}`.
///
/// PRIVACY / trust: the template is OPERATOR-SUPPLIED (a CLI flag), exactly as the SSH target is
/// operator-supplied in the inventory; the driver never hardcodes an actuation command.
#[derive(Debug, Clone)]
pub struct CommandUpgrader {
    /// The program to run (the first template token).
    program: String,
    /// The argument template (the remaining tokens); `{id}` / `{source}` / `{target}` are expanded
    /// per node on each `upgrade`.
    arg_template: Vec<String>,
}

impl CommandUpgrader {
    /// Build a command actuator from a whitespace-split template (`program arg1 arg2 ...`), the first
    /// token being the program. Returns `None` when the template has no program token (empty /
    /// whitespace-only), so the caller can fail loud rather than spawn nothing.
    #[must_use]
    pub fn from_template(template: &str) -> Option<Self> {
        let mut parts = template.split_whitespace().map(str::to_owned);
        let program = parts.next()?;
        Some(Self {
            program,
            arg_template: parts.collect(),
        })
    }

    /// Substitute the per-node placeholders in one template argument.
    fn expand(arg: &str, target: &ActuationTarget) -> String {
        arg.replace("{id}", &target.id)
            .replace("{source}", &target.upgrade_source)
            .replace("{target}", &target.ssh)
    }
}

impl NodeUpgrader for CommandUpgrader {
    fn upgrade(&mut self, target: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
        // `<program> <arg...>` with `{id}` / `{source}` / `{target}` expanded per node. Same
        // contract as `SshUpgrader`: the command runs the node's own verify -> swap -> health-gate
        // out of band and only exits 0 once healthy on the new binary.
        let mut cmd = std::process::Command::new(&self.program);
        for a in &self.arg_template {
            cmd.arg(Self::expand(a, target));
        }
        let output = cmd.output().map_err(|e| ClusterUpgradeError::Upgrade {
            node: target.id.clone(),
            detail: format!("spawning the actuation command `{}`: {e}", self.program),
        })?;
        if !output.status.success() {
            return Err(ClusterUpgradeError::Upgrade {
                node: target.id.clone(),
                detail: format!(
                    "the actuation command `{}` exited {:?}: {}",
                    self.program,
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            });
        }
        Ok(())
    }
}

/// Forward `NodeUpgrader` through a `Box`, so the CLI can pick the actuator (SSH vs command) at
/// runtime and hand the generic driver ONE boxed upgrader instead of monomorphizing the whole
/// orchestrator twice.
impl NodeUpgrader for Box<dyn NodeUpgrader> {
    fn upgrade(&mut self, target: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
        (**self).upgrade(target)
    }
}

/// The prod loop-pacing sleeper: a real thread sleep. This is the short-lived CLI orchestrator path
/// (off the server determinism boundary), so a wall-clock sleep is appropriate.
#[derive(Debug, Clone, Copy, Default)]
pub struct ThreadSleeper;

impl Sleeper for ThreadSleeper {
    fn sleep(&self, dur: Duration) {
        std::thread::sleep(dur);
    }
}

/// The prod authenticated-RESP cluster client (#392 slice 2): a thin RESP2 client over the
/// network-exposed, `requirepass`-authenticated surface. It reuses the pure parsers above; the real
/// transport is exercised by the acceptance / docker layers, not in-process here.
pub struct RespClusterClient {
    rt: tokio::runtime::Runtime,
    timeout: Duration,
}

impl RespClusterClient {
    /// Build a RESP client with the per-exchange `timeout` (its own current-thread runtime, since
    /// this is a short-lived CLI orchestrator, not a shard executor).
    ///
    /// # Errors
    /// If the current-thread tokio runtime cannot be built.
    pub fn new(timeout: Duration) -> Result<Self, ClusterUpgradeError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ClusterUpgradeError::Client {
                op: "runtime".to_owned(),
                detail: e.to_string(),
            })?;
        Ok(Self { rt, timeout })
    }

    /// One bounded RESP exchange: connect, optional AUTH, send `args`, read ONE reply.
    fn call(
        &self,
        target: &ActuationTarget,
        args: &[&[u8]],
    ) -> Result<RawReply, ClusterUpgradeError> {
        self.rt.block_on(async {
            match tokio::time::timeout(
                self.timeout,
                resp_exchange(&target.resp_addr, target.auth.as_deref(), args),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(ClusterUpgradeError::Client {
                    op: "resp".to_owned(),
                    detail: format!("timed out after {:?}", self.timeout),
                }),
            }
        })
    }
}

impl ClusterClient for RespClusterClient {
    fn discover_members(
        &mut self,
        seed: &ActuationTarget,
    ) -> Result<Vec<String>, ClusterUpgradeError> {
        let reply = self.call(seed, &[b"CLUSTER", b"SHARDS"])?;
        Ok(collect_member_ids(&reply))
    }

    fn observe_node(
        &mut self,
        node: &ActuationTarget,
    ) -> Result<NodeObservation, ClusterUpgradeError> {
        let reply = self.call(node, &[b"INFO"])?;
        let text = bulk_text(&reply).ok_or_else(|| ClusterUpgradeError::Observe {
            node: node.id.clone(),
            detail: "INFO did not return a bulk string".to_owned(),
        })?;
        Ok(parse_info_replication(&text))
    }

    fn observe_quorum(
        &mut self,
        seed: &ActuationTarget,
    ) -> Result<QuorumObservation, ClusterUpgradeError> {
        let reply = self.call(seed, &[b"CLUSTER", b"INFO"])?;
        let text = bulk_text(&reply).ok_or_else(|| ClusterUpgradeError::Observe {
            node: seed.id.clone(),
            detail: "CLUSTER INFO did not return a bulk string".to_owned(),
        })?;
        Ok(parse_cluster_info(&text))
    }

    fn cluster_failover(&mut self, node: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
        match self.call(node, &[b"CLUSTER", b"FAILOVER"])? {
            RawReply::Simple(s) if s.eq_ignore_ascii_case("OK") => Ok(()),
            RawReply::Error(e) => Err(ClusterUpgradeError::Failover {
                node: node.id.clone(),
                detail: e,
            }),
            other => Err(ClusterUpgradeError::Failover {
                node: node.id.clone(),
                detail: format!("unexpected reply: {other:?}"),
            }),
        }
    }
}

// -- minimal RESP2/RESP3 reply reader (enough for INFO / CLUSTER INFO / CLUSTER SHARDS / FAILOVER) --

/// A minimal RESP reply value (only the shapes the driver's reads need).
#[derive(Debug)]
enum RawReply {
    Simple(String),
    Error(String),
    /// A RESP integer. The value is not needed by any of the driver's reads (ids come from bulk
    /// strings), so it is parsed-and-discarded to keep the reader total.
    Int,
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RawReply>>),
}

/// A buffered RESP stream (reads through a `BufReader`, writes delegate to the inner socket).
type RespStream = tokio::io::BufReader<tokio::net::TcpStream>;

/// A boxed reply future (RESP arrays need recursion, which async fns cannot express directly).
type ReplyFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<RawReply, ClusterUpgradeError>> + 'a>,
>;

/// Connect, optional AUTH, send `args`, read ONE reply.
async fn resp_exchange(
    resp_addr: &str,
    auth: Option<&str>,
    args: &[&[u8]],
) -> Result<RawReply, ClusterUpgradeError> {
    let stream = tokio::net::TcpStream::connect(resp_addr)
        .await
        .map_err(|e| ClusterUpgradeError::Client {
            op: "connect".to_owned(),
            detail: format!("{resp_addr}: {e}"),
        })?;
    let mut br = tokio::io::BufReader::new(stream);
    if let Some(pw) = auth {
        write_command(&mut br, &[b"AUTH", pw.as_bytes()]).await?;
        match read_reply(&mut br).await? {
            RawReply::Simple(s) if s.eq_ignore_ascii_case("OK") => {}
            RawReply::Error(e) => {
                return Err(ClusterUpgradeError::Client {
                    op: "AUTH".to_owned(),
                    detail: e,
                });
            }
            other => {
                return Err(ClusterUpgradeError::Client {
                    op: "AUTH".to_owned(),
                    detail: format!("unexpected reply: {other:?}"),
                });
            }
        }
    }
    write_command(&mut br, args).await?;
    read_reply(&mut br).await
}

/// Encode `args` as a RESP2 command array and write it.
async fn write_command(br: &mut RespStream, args: &[&[u8]]) -> Result<(), ClusterUpgradeError> {
    let mut buf = Vec::with_capacity(32);
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    br.write_all(&buf).await.map_err(resp_io)?;
    br.flush().await.map_err(resp_io)
}

/// Read ONE line (up to and excluding the terminating CRLF).
async fn read_line(br: &mut RespStream) -> Result<String, ClusterUpgradeError> {
    let mut buf = Vec::with_capacity(32);
    let n = br.read_until(b'\n', &mut buf).await.map_err(resp_io)?;
    if n == 0 {
        return Err(ClusterUpgradeError::Client {
            op: "resp".to_owned(),
            detail: "connection closed mid-reply".to_owned(),
        });
    }
    while matches!(buf.last(), Some(b'\n' | b'\r')) {
        buf.pop();
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Read ONE RESP reply (recursively for arrays / maps).
fn read_reply(br: &mut RespStream) -> ReplyFuture<'_> {
    Box::pin(async move {
        let mut prefix = [0u8; 1];
        br.read_exact(&mut prefix).await.map_err(resp_io)?;
        match prefix[0] {
            b'+' => Ok(RawReply::Simple(read_line(br).await?)),
            b'-' => Ok(RawReply::Error(read_line(br).await?)),
            b':' => {
                let _ = read_line(br).await?;
                Ok(RawReply::Int)
            }
            // Bulk ($) / verbatim (=) string.
            b'$' | b'=' => {
                let len: i64 = read_line(br)
                    .await?
                    .trim()
                    .parse()
                    .map_err(|_| resp_proto("bulk length"))?;
                if len < 0 {
                    return Ok(RawReply::Bulk(None));
                }
                let want = usize::try_from(len).unwrap_or(0);
                let mut body = vec![0u8; want + 2]; // + CRLF
                br.read_exact(&mut body).await.map_err(resp_io)?;
                body.truncate(want);
                Ok(RawReply::Bulk(Some(body)))
            }
            // Array (*) / set (~) / push (>).
            b'*' | b'~' | b'>' => {
                let count: i64 = read_line(br)
                    .await?
                    .trim()
                    .parse()
                    .map_err(|_| resp_proto("array count"))?;
                if count < 0 {
                    return Ok(RawReply::Array(None));
                }
                let mut items = Vec::new();
                for _ in 0..count {
                    items.push(read_reply(br).await?);
                }
                Ok(RawReply::Array(Some(items)))
            }
            // Map (%): read as a flat key/value array.
            b'%' => {
                let count: i64 = read_line(br)
                    .await?
                    .trim()
                    .parse()
                    .map_err(|_| resp_proto("map count"))?;
                let mut items = Vec::new();
                for _ in 0..count.saturating_mul(2) {
                    items.push(read_reply(br).await?);
                }
                Ok(RawReply::Array(Some(items)))
            }
            // Null (_).
            b'_' => {
                let _ = read_line(br).await?;
                Ok(RawReply::Bulk(None))
            }
            other => Err(resp_proto(&format!(
                "unexpected RESP type byte 0x{other:02x}"
            ))),
        }
    })
}

/// A RESP IO error. Takes the error by value because it is used as `map_err(resp_io)` (which hands
/// the error over by value); a by-ref signature would force a closure at every call site.
#[allow(clippy::needless_pass_by_value)]
fn resp_io(e: std::io::Error) -> ClusterUpgradeError {
    ClusterUpgradeError::Client {
        op: "resp io".to_owned(),
        detail: e.to_string(),
    }
}

/// A RESP protocol error.
fn resp_proto(detail: &str) -> ClusterUpgradeError {
    ClusterUpgradeError::Client {
        op: "resp".to_owned(),
        detail: detail.to_owned(),
    }
}

/// Extract a bulk / simple string from a reply.
fn bulk_text(reply: &RawReply) -> Option<String> {
    match reply {
        RawReply::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        RawReply::Simple(s) => Some(s.clone()),
        _ => None,
    }
}

/// A bulk / simple string value, for walking a flat RESP map.
fn as_bulk_str(reply: &RawReply) -> Option<String> {
    match reply {
        RawReply::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        RawReply::Simple(s) => Some(s.clone()),
        _ => None,
    }
}

/// Look up `key` in a flat `[k, v, k, v, ...]` RESP map, returning the value after it.
fn flat_lookup<'a>(items: &'a [RawReply], key: &str) -> Option<&'a RawReply> {
    let mut it = items.iter();
    while let Some(k) = it.next() {
        if as_bulk_str(k).as_deref() == Some(key) {
            return it.next();
        }
    }
    None
}

/// Extract the live member ids from a `CLUSTER SHARDS` reply (an array of shards, each a flat map
/// with a `nodes` array; each node a flat map with an `id`). Best-effort: an unexpected shape yields
/// the ids it could find.
fn collect_member_ids(reply: &RawReply) -> Vec<String> {
    let mut ids = Vec::new();
    let RawReply::Array(Some(shards)) = reply else {
        return ids;
    };
    for shard in shards {
        let RawReply::Array(Some(fields)) = shard else {
            continue;
        };
        let Some(RawReply::Array(Some(nodes))) = flat_lookup(fields, "nodes") else {
            continue;
        };
        for node in nodes {
            let RawReply::Array(Some(node_fields)) = node else {
                continue;
            };
            if let Some(id) = flat_lookup(node_fields, "id").and_then(as_bulk_str) {
                ids.push(id);
            }
        }
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    // ---- version normalization ----

    #[test]
    fn normalize_version_strips_v_prefix_build_metadata_and_whitespace() {
        assert_eq!(normalize_version("v1.2.3"), "1.2.3", "leading v stripped");
        assert_eq!(normalize_version("V2.0.0"), "2.0.0", "leading V stripped");
        assert_eq!(
            normalize_version("1.2.3+build.5"),
            "1.2.3",
            "build metadata dropped"
        );
        assert_eq!(
            normalize_version("  1.2.3  "),
            "1.2.3",
            "surrounding whitespace trimmed"
        );
        assert_eq!(
            normalize_version("v1.2.3+abc def"),
            "1.2.3",
            "v-prefix AND build metadata (incl. inner space) dropped"
        );
        // A version that merely starts with a letter is NOT stripped (the char after `v` is not a
        // digit, so no false 'v' strip).
        assert_eq!(normalize_version("version9"), "version9");
        // Already canonical is unchanged; both sides of an exact match normalize identically.
        assert_eq!(normalize_version("1.2.3"), normalize_version("v1.2.3 "));
    }

    // ---- INFO / CLUSTER INFO parsing ----

    #[test]
    fn parse_info_reads_master_role_version_and_slave_lag() {
        let info = "# Server\r\n\
             redis_version:7.4.0\r\n\
             ironcache_version:v1.2.3\r\n\
             # Replication\r\n\
             role:master\r\n\
             connected_slaves:2\r\n\
             slave0:ip=10.0.0.5,port=7005,state=online,offset=190,lag=10\r\n\
             slave1:ip=10.0.0.6,port=7006,state=online,offset=200,lag=0\r\n\
             master_repl_offset:200\r\n";
        let obs = parse_info_replication(info);
        assert_eq!(obs.role, NodeRole::Master);
        assert_eq!(obs.version, "v1.2.3", "raw version (normalized later)");
        assert_eq!(obs.slaves.len(), 2);
        let s0 = &obs.slaves[0];
        assert_eq!((s0.host.as_str(), s0.port, s0.lag), ("10.0.0.5", 7005, 10));
        let s1 = &obs.slaves[1];
        assert_eq!((s1.host.as_str(), s1.port, s1.lag), ("10.0.0.6", 7006, 0));
    }

    #[test]
    fn parse_info_reads_replica_role_and_link() {
        let up = "role:replica\r\nmaster_link_status:up\r\nironcache_version:1.2.3\r\n";
        let obs = parse_info_replication(up);
        assert_eq!(obs.role, NodeRole::Replica);
        assert_eq!(obs.link, LinkStatus::Up);
        assert!(obs.slaves.is_empty());
        let down = "role:replica\r\nmaster_link_status:down\r\n";
        assert_eq!(parse_info_replication(down).link, LinkStatus::Down);
    }

    #[test]
    fn parse_info_reads_shard_count_from_io_threads_active() {
        // #731: the shard count comes from INFO `io_threads_active` (ironcache-observe renders
        // `server.shards` there). A multi-shard node reports > 1 (the pre-flight refuses it).
        let multi = "role:master\r\nio_threads_active:4\r\nironcache_version:1.2.3\r\n";
        assert_eq!(parse_info_replication(multi).shards, 4);
        // Absent field -> default 1 (never spuriously refuse an older node that omits it).
        let absent = "role:master\r\nironcache_version:1.2.3\r\n";
        assert_eq!(parse_info_replication(absent).shards, 1);
        // A single-shard node reports 1 explicitly.
        let single = "role:replica\r\nio_threads_active:1\r\n";
        assert_eq!(parse_info_replication(single).shards, 1);
        // Malformed value falls back to the safe default of 1.
        let bad = "role:master\r\nio_threads_active:notanumber\r\n";
        assert_eq!(parse_info_replication(bad).shards, 1);
    }

    #[test]
    fn parse_cluster_info_reads_quorum_signal() {
        // ok + a recognized leader -> quorum.
        let ok = "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_raft_leader:node-a\r\n";
        assert!(parse_cluster_info(ok).has_quorum());
        // ok but NO leader -> not quorum (cannot commit the fence).
        let no_leader = "cluster_state:ok\r\ncluster_raft_leader:none\r\n";
        assert!(!parse_cluster_info(no_leader).has_quorum());
        // fail state -> not quorum.
        let failed = "cluster_state:fail\r\ncluster_raft_leader:node-a\r\n";
        assert!(!parse_cluster_info(failed).has_quorum());
        // no raft-leader line at all (non-raft) -> not quorum.
        let plain = "cluster_state:ok\r\n";
        assert!(!parse_cluster_info(plain).has_quorum());
    }

    // ---- shared simulated cluster for the driver-level mocks ----

    fn addr_of(id: &str) -> String {
        // A stable, distinct loopback endpoint per node (the host:port the slaveN view matches on).
        let port = match id {
            "p" => 7001,
            "r1" => 7002,
            "r2" => 7003,
            _ => 7009,
        };
        format!("127.0.0.1:{port}")
    }

    fn target(id: &str) -> ActuationTarget {
        ActuationTarget {
            id: id.to_owned(),
            resp_addr: addr_of(id),
            auth: None,
            ssh: format!("op@{id}"),
            upgrade_source: "--to 1.2.3".to_owned(),
        }
    }

    struct SimNode {
        id: String,
        version: String,
        role: NodeRole,
        /// Ticks remaining until a just-upgraded replica has caught up (link down + large lag while
        /// > 0). Models the swap+resync the StubUpgrader triggers.
        resync: u32,
        /// The node's internal shard count reported via INFO `io_threads_active` (#731). Default 1
        /// (the supported single-shard HA config); a test sets it > 1 to exercise the pre-flight.
        shards: usize,
    }

    struct Sim {
        nodes: Vec<SimNode>,
        target: String,
        quorum: bool,
        failovers: usize,
        events: Vec<String>,
    }

    impl Sim {
        fn three_node(target_version: &str, node_version: &str) -> Rc<RefCell<Self>> {
            let mk = |id: &str, role| SimNode {
                id: id.to_owned(),
                version: node_version.to_owned(),
                role,
                resync: 0,
                shards: 1,
            };
            Rc::new(RefCell::new(Sim {
                nodes: vec![
                    mk("p", NodeRole::Master),
                    mk("r1", NodeRole::Replica),
                    mk("r2", NodeRole::Replica),
                ],
                target: target_version.to_owned(),
                quorum: true,
                failovers: 0,
                events: Vec::new(),
            }))
        }

        fn node(&self, id: &str) -> &SimNode {
            self.nodes.iter().find(|n| n.id == id).expect("node")
        }
        fn node_mut(&mut self, id: &str) -> &mut SimNode {
            self.nodes.iter_mut().find(|n| n.id == id).expect("node")
        }
        fn master_id(&self) -> Option<String> {
            self.nodes
                .iter()
                .find(|n| n.role == NodeRole::Master)
                .map(|n| n.id.clone())
        }
        /// Advance one tick: a resyncing replica gets one step closer to caught up.
        fn tick(&mut self) {
            for n in &mut self.nodes {
                n.resync = n.resync.saturating_sub(1);
            }
        }
    }

    // A cluster client backed by the shared sim.
    struct SimClient {
        sim: Rc<RefCell<Sim>>,
    }
    impl ClusterClient for SimClient {
        fn discover_members(
            &mut self,
            _seed: &ActuationTarget,
        ) -> Result<Vec<String>, ClusterUpgradeError> {
            Ok(self
                .sim
                .borrow()
                .nodes
                .iter()
                .map(|n| n.id.clone())
                .collect())
        }
        fn observe_node(
            &mut self,
            node: &ActuationTarget,
        ) -> Result<NodeObservation, ClusterUpgradeError> {
            let sim = self.sim.borrow();
            let n = sim.node(&node.id);
            let slaves = if n.role == NodeRole::Master {
                // The master's per-replica view: every other node, with lag 0 when caught up else a
                // large lag while resyncing.
                sim.nodes
                    .iter()
                    .filter(|o| o.id != n.id)
                    .map(|o| {
                        let addr = addr_of(&o.id);
                        let (host, port) = split_host_port(&addr).unwrap();
                        SlaveEntry {
                            host: host.to_owned(),
                            port,
                            lag: if o.resync == 0 { 0 } else { 1000 },
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let link = if n.role == NodeRole::Replica && n.resync == 0 {
                LinkStatus::Up
            } else {
                LinkStatus::Down
            };
            Ok(NodeObservation {
                role: n.role,
                version: n.version.clone(),
                link,
                slaves,
                shards: n.shards,
            })
        }
        fn observe_quorum(
            &mut self,
            _seed: &ActuationTarget,
        ) -> Result<QuorumObservation, ClusterUpgradeError> {
            let ok = self.sim.borrow().quorum;
            Ok(QuorumObservation {
                state_ok: ok,
                raft_leader_present: ok,
            })
        }
        fn cluster_failover(&mut self, node: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
            let mut sim = self.sim.borrow_mut();
            if let Some(old_master) = sim.master_id() {
                sim.node_mut(&old_master).role = NodeRole::Replica;
            }
            sim.node_mut(&node.id).role = NodeRole::Master;
            sim.failovers += 1;
            sim.events.push(format!("FAILOVER {}", node.id));
            Ok(())
        }
    }

    // The stub binary-swap actuator: records the upgrade ORDER and flips the node to the target
    // version, then marks it resyncing (caught up after a simulated tick).
    struct StubUpgrader {
        sim: Rc<RefCell<Sim>>,
        order: Rc<RefCell<Vec<String>>>,
    }
    impl NodeUpgrader for StubUpgrader {
        fn upgrade(&mut self, target: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
            let mut sim = self.sim.borrow_mut();
            let tv = sim.target.clone();
            let n = sim.node_mut(&target.id);
            n.version = tv;
            n.resync = 1;
            drop(sim);
            self.order.borrow_mut().push(target.id.clone());
            Ok(())
        }
    }

    // The mock pauser: records PAUSE / UNPAUSE (freeze seam) into the shared event log.
    struct SimPauser {
        sim: Rc<RefCell<Sim>>,
    }
    impl Pauser for SimPauser {
        fn freeze(&self, target: &PauseTarget) -> Result<(), PauseError> {
            self.sim
                .borrow_mut()
                .events
                .push(format!("PAUSE {}", target.resp_addr));
            Ok(())
        }
        fn unfreeze(&self, target: &PauseTarget) -> Result<(), PauseError> {
            self.sim
                .borrow_mut()
                .events
                .push(format!("UNPAUSE {}", target.resp_addr));
            Ok(())
        }
    }

    // A sleeper that advances the sim by one tick (models the resync completing during the wait).
    struct SimSleeper {
        sim: Rc<RefCell<Sim>>,
    }
    impl Sleeper for SimSleeper {
        fn sleep(&self, _dur: Duration) {
            self.sim.borrow_mut().tick();
        }
    }

    fn inventory() -> Inventory {
        Inventory::new(vec![target("p"), target("r1"), target("r2")])
    }

    fn build_live(
        sim: &Rc<RefCell<Sim>>,
        order: &Rc<RefCell<Vec<String>>>,
    ) -> LiveCluster<SimClient, StubUpgrader> {
        let config = DriverConfig {
            inventory: inventory(),
            target_version: "1.2.3".to_owned(),
            max_lag: 2,
            poll: PollCfg {
                tick_delay: Duration::ZERO,
            },
            freeze: FreezeCfg {
                pause_window_ms: 30_000,
                max_drain_polls: 8,
                drain_poll_delay: Duration::ZERO,
            },
        };
        LiveCluster::new(
            SimClient { sim: sim.clone() },
            StubUpgrader {
                sim: sim.clone(),
                order: order.clone(),
            },
            Box::new(SimPauser { sim: sim.clone() }),
            Box::new(SimSleeper { sim: sim.clone() }),
            config,
        )
    }

    // ---- the full 3-node roll ----

    #[test]
    fn full_three_node_roll_upgrades_primary_last_promotes_once_and_completes() {
        let sim = Sim::three_node("1.2.3", "1.2.2");
        let order = Rc::new(RefCell::new(Vec::new()));
        let mut live = build_live(&sim, &order);

        let report = run_cluster_upgrade(&mut live, 50).expect("roll ok");

        // Terminates DONE (not stalled).
        assert_eq!(report, UpgradeReport::Completed);
        // Every replica upgraded BEFORE the primary: the order is r1, r2, then the old primary p LAST.
        assert_eq!(
            *order.borrow(),
            vec!["r1".to_owned(), "r2".to_owned(), "p".to_owned()],
            "primary upgraded last"
        );
        // The promotion happened EXACTLY once.
        assert_eq!(sim.borrow().failovers, 1, "exactly one failover");
        // old_primary_id was captured once as p and stayed fixed across the roll (no spurious second
        // promotion of the demoted old primary).
        assert_eq!(
            live.old_primary_id(),
            Some("p"),
            "old primary id stayed fixed"
        );
        // The one failover promoted an upgraded replica (r1), and it happened before the old primary
        // was upgraded (which is last in the order log).
        let events = sim.borrow().events.clone();
        assert!(
            events.iter().any(|e| e == "FAILOVER r1"),
            "promoted the candidate r1: {events:?}"
        );
    }

    #[test]
    fn already_fully_upgraded_shard_is_a_no_op_with_no_failover() {
        // Every node already on the target: the shard_fully_upgraded pre-check short-circuits.
        let sim = Sim::three_node("1.2.3", "1.2.3");
        let order = Rc::new(RefCell::new(Vec::new()));
        let mut live = build_live(&sim, &order);

        let report = run_cluster_upgrade(&mut live, 50).expect("idempotent");
        assert_eq!(report, UpgradeReport::Completed);
        assert!(order.borrow().is_empty(), "no node was upgraded");
        assert_eq!(sim.borrow().failovers, 0, "no gratuitous failover");
        assert!(sim.borrow().events.is_empty(), "no pause / failover issued");
    }

    #[test]
    fn refresh_normalizes_versions_so_a_v_prefix_skew_still_reaches_zero_replicas() {
        // Nodes report v-prefixed versions; the target is bare. The ONE shared normalizer must make
        // the exact-match upgraded check see them as equal (else the roll would stall).
        let sim = Sim::three_node("1.2.3", "v1.2.3");
        let order = Rc::new(RefCell::new(Vec::new()));
        let mut live = build_live(&sim, &order);
        live.refresh().expect("refresh");
        assert_eq!(
            live.view().replicas_to_upgrade(),
            0,
            "v-prefix skew normalized away"
        );
        assert!(live.view().shard_fully_upgraded());
    }

    #[test]
    fn membership_drift_aborts_the_refresh() {
        // The live-discovered set differs from the inventory (a node vanished): abort loudly.
        let sim = Sim::three_node("1.2.3", "1.2.2");
        sim.borrow_mut().nodes.pop(); // drop r2 from the live cluster
        let order = Rc::new(RefCell::new(Vec::new()));
        let mut live = build_live(&sim, &order);
        let err = live.refresh().expect_err("drift must abort");
        assert!(
            matches!(err, ClusterUpgradeError::MembershipDrift { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn refresh_fails_closed_on_a_multishard_node() {
        // #731: a node reporting shards > 1 cannot deliver RPO=0 (the HA replica path is
        // single-shard), so the pre-flight refuses BEFORE any upgrade/promote action.
        let sim = Sim::three_node("1.2.3", "1.2.2");
        sim.borrow_mut().node_mut("r1").shards = 4; // a replica is multi-shard
        let order = Rc::new(RefCell::new(Vec::new()));
        let mut live = build_live(&sim, &order);
        let err = live.refresh().expect_err("multi-shard must fail closed");
        match err {
            ClusterUpgradeError::MultiShardUnsupported { node, shards } => {
                assert_eq!(node, "r1");
                assert_eq!(shards, 4);
            }
            other => panic!("expected MultiShardUnsupported, got {other:?}"),
        }
        // No failover / upgrade action ran (refused at the pre-flight).
        assert_eq!(sim.borrow().failovers, 0);
    }

    #[test]
    fn refresh_allows_an_all_single_shard_cluster() {
        // The supported config (every node shards == 1) passes the #731 pre-flight cleanly.
        let sim = Sim::three_node("1.2.3", "1.2.2");
        let order = Rc::new(RefCell::new(Vec::new()));
        let mut live = build_live(&sim, &order);
        assert!(
            live.refresh().is_ok(),
            "an all-single-shard cluster must pass the pre-flight"
        );
    }

    // ---- the failover-freeze fence (the RPO=0 core) ----

    // A freeze-focused client: the candidate's master-side lag drains by one per observe of the old
    // primary; cluster_failover records the candidate's lag AT COMMIT (to prove it only fires at 0).
    struct FreezeClient {
        state: Rc<RefCell<FreezeState>>,
    }
    struct FreezeState {
        candidate_addr: String,
        candidate_lag: u64,
        drains: bool,
        events: Vec<String>,
        failover_lag_at_commit: Option<u64>,
    }
    impl ClusterClient for FreezeClient {
        fn discover_members(
            &mut self,
            _seed: &ActuationTarget,
        ) -> Result<Vec<String>, ClusterUpgradeError> {
            Ok(vec!["p".to_owned(), "r1".to_owned()])
        }
        fn observe_node(
            &mut self,
            _node: &ActuationTarget,
        ) -> Result<NodeObservation, ClusterUpgradeError> {
            let mut st = self.state.borrow_mut();
            let cur = st.candidate_lag;
            if st.drains {
                st.candidate_lag = st.candidate_lag.saturating_sub(1);
            }
            let (host, port) = split_host_port(&st.candidate_addr).unwrap();
            Ok(NodeObservation {
                role: NodeRole::Master,
                version: "1.2.2".to_owned(),
                link: LinkStatus::Down,
                slaves: vec![SlaveEntry {
                    host: host.to_owned(),
                    port,
                    lag: cur,
                }],
                shards: 1,
            })
        }
        fn observe_quorum(
            &mut self,
            _seed: &ActuationTarget,
        ) -> Result<QuorumObservation, ClusterUpgradeError> {
            Ok(QuorumObservation {
                state_ok: true,
                raft_leader_present: true,
            })
        }
        fn cluster_failover(&mut self, node: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
            let mut st = self.state.borrow_mut();
            st.failover_lag_at_commit = Some(st.candidate_lag);
            st.events.push(format!("FAILOVER {}", node.id));
            Ok(())
        }
    }
    struct FreezePauser {
        state: Rc<RefCell<FreezeState>>,
    }
    impl Pauser for FreezePauser {
        fn freeze(&self, _t: &PauseTarget) -> Result<(), PauseError> {
            self.state.borrow_mut().events.push("PAUSE".to_owned());
            Ok(())
        }
        fn unfreeze(&self, _t: &PauseTarget) -> Result<(), PauseError> {
            self.state.borrow_mut().events.push("UNPAUSE".to_owned());
            Ok(())
        }
    }
    struct NoopUpgrader;
    impl NodeUpgrader for NoopUpgrader {
        fn upgrade(&mut self, _t: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
            Ok(())
        }
    }
    struct NoopSleeper;
    impl Sleeper for NoopSleeper {
        fn sleep(&self, _dur: Duration) {}
    }

    // Build a LiveCluster whose view is hand-set to {p master, r1 upgraded in-sync replica}, ready to
    // promote r1. The freeze client controls the drain.
    fn freeze_live(
        state: &Rc<RefCell<FreezeState>>,
        max_drain_polls: u32,
    ) -> LiveCluster<FreezeClient, NoopUpgrader> {
        let config = DriverConfig {
            inventory: Inventory::new(vec![target("p"), target("r1")]),
            target_version: "1.2.3".to_owned(),
            max_lag: 2,
            poll: PollCfg {
                tick_delay: Duration::ZERO,
            },
            freeze: FreezeCfg {
                pause_window_ms: 30_000,
                max_drain_polls,
                drain_poll_delay: Duration::ZERO,
            },
        };
        let mut live = LiveCluster::new(
            FreezeClient {
                state: state.clone(),
            },
            NoopUpgrader,
            Box::new(FreezePauser {
                state: state.clone(),
            }),
            Box::new(NoopSleeper),
            config,
        );
        // Hand-build this tick's view: p is master (old), r1 is an upgraded, in-sync candidate.
        live.view = ClusterView {
            nodes: vec![
                NodeView {
                    id: "p".to_owned(),
                    version: "1.2.2".to_owned(),
                    role: NodeRole::Master,
                    link: LinkStatus::Down,
                    lag: None,
                },
                NodeView {
                    id: "r1".to_owned(),
                    version: "1.2.3".to_owned(),
                    role: NodeRole::Replica,
                    link: LinkStatus::Up,
                    lag: Some(known_lag(0)),
                },
            ],
            target_version: "1.2.3".to_owned(),
            max_lag: 2,
            raft_quorum: true,
            old_primary_id: Some("p".to_owned()),
        };
        live.old_primary_id = Some("p".to_owned());
        live
    }

    #[test]
    fn promote_freezes_then_drains_to_zero_then_fails_over() {
        // The candidate starts lagging by 2; each observe of the old primary drains it by one. The
        // failover must NOT fire until the lag reaches 0.
        let state = Rc::new(RefCell::new(FreezeState {
            candidate_addr: addr_of("r1"),
            candidate_lag: 2,
            drains: true,
            events: Vec::new(),
            failover_lag_at_commit: None,
        }));
        let mut live = freeze_live(&state, 8);
        live.promote_candidate().expect("promote ok");

        let st = state.borrow();
        // Order: PAUSE the old primary, THEN (after the drain) FAILOVER the candidate, THEN UNPAUSE.
        assert_eq!(
            st.events,
            vec![
                "PAUSE".to_owned(),
                "FAILOVER r1".to_owned(),
                "UNPAUSE".to_owned()
            ],
            "pause -> failover -> unpause"
        );
        // The failover fired ONLY once the candidate had drained to lag 0 (the RPO=0 fence).
        assert_eq!(
            st.failover_lag_at_commit,
            Some(0),
            "no failover until the candidate applied through the frozen head"
        );
    }

    #[test]
    fn promote_fails_closed_on_a_drain_timeout_without_failing_over() {
        // The candidate NEVER drains (stuck lagging): the fence must FAIL CLOSED -- unpause, error,
        // and issue NO failover.
        let state = Rc::new(RefCell::new(FreezeState {
            candidate_addr: addr_of("r1"),
            candidate_lag: 5,
            drains: false,
            events: Vec::new(),
            failover_lag_at_commit: None,
        }));
        let mut live = freeze_live(&state, 3);
        let err = live.promote_candidate().expect_err("must fail closed");

        assert!(
            matches!(err, ClusterUpgradeError::DrainTimeout { polls: 3, .. }),
            "{err:?}"
        );
        let st = state.borrow();
        // PAUSE then UNPAUSE, and crucially NO FAILOVER was ever issued.
        assert_eq!(st.events, vec!["PAUSE".to_owned(), "UNPAUSE".to_owned()]);
        assert!(
            st.failover_lag_at_commit.is_none(),
            "a stuck drain must never promote"
        );
    }

    // -----------------------------------------------------------------------
    // CommandUpgrader (#630): the non-SSH, operator-supplied command actuator.
    // -----------------------------------------------------------------------

    fn cmd_target(id: &str) -> ActuationTarget {
        ActuationTarget {
            id: id.to_owned(),
            resp_addr: "127.0.0.1:6379".to_owned(),
            auth: None,
            ssh: "deploy@host".to_owned(),
            upgrade_source: "--to v2".to_owned(),
        }
    }

    #[test]
    fn command_upgrader_from_template_splits_program_and_args() {
        let u = CommandUpgrader::from_template("docker compose up -d --force-recreate {id}")
            .expect("non-empty template");
        assert_eq!(u.program, "docker");
        assert_eq!(
            u.arg_template,
            vec!["compose", "up", "-d", "--force-recreate", "{id}"]
        );
    }

    #[test]
    fn command_upgrader_empty_template_is_none() {
        assert!(CommandUpgrader::from_template("").is_none());
        assert!(CommandUpgrader::from_template("   ").is_none());
    }

    #[test]
    fn command_upgrader_expands_every_placeholder_per_node() {
        let t = cmd_target("node-a");
        assert_eq!(CommandUpgrader::expand("{id}", &t), "node-a");
        assert_eq!(CommandUpgrader::expand("{target}", &t), "deploy@host");
        assert_eq!(CommandUpgrader::expand("{source}", &t), "--to v2");
        // Multiple placeholders in one arg all expand.
        assert_eq!(
            CommandUpgrader::expand("swap-{id}::{source}", &t),
            "swap-node-a::--to v2"
        );
        // A literal with no placeholder is untouched.
        assert_eq!(
            CommandUpgrader::expand("--force-recreate", &t),
            "--force-recreate"
        );
    }

    #[test]
    fn command_upgrader_ok_on_zero_exit() {
        // `true` ignores its args and exits 0 -> a successful actuation. (Placeholders still expand.)
        let mut u = CommandUpgrader::from_template("true {id}").expect("template");
        assert!(u.upgrade(&cmd_target("node-a")).is_ok());
    }

    #[test]
    fn command_upgrader_errors_on_nonzero_exit_naming_the_node() {
        // `false` exits 1 -> the driver must see an Upgrade error carrying the node id.
        let mut u = CommandUpgrader::from_template("false").expect("template");
        let err = u.upgrade(&cmd_target("node-a")).unwrap_err();
        assert!(
            matches!(&err, ClusterUpgradeError::Upgrade { node, .. } if node == "node-a"),
            "{err:?}"
        );
    }

    #[test]
    fn command_upgrader_errors_when_program_is_missing() {
        // A non-existent program is a spawn failure surfaced as an Upgrade error, not a panic.
        let mut u = CommandUpgrader::from_template("ironcache-no-such-actuator-binary-xyz {id}")
            .expect("template");
        let err = u.upgrade(&cmd_target("node-a")).unwrap_err();
        assert!(
            matches!(&err, ClusterUpgradeError::Upgrade { node, .. } if node == "node-a"),
            "{err:?}"
        );
    }

    #[test]
    fn boxed_node_upgrader_forwards() {
        // The CLI hands the driver a `Box<dyn NodeUpgrader>` so it can pick SSH vs command at runtime.
        let mut boxed: Box<dyn NodeUpgrader> =
            Box::new(CommandUpgrader::from_template("true").expect("template"));
        assert!(boxed.upgrade(&cmd_target("node-a")).is_ok());
    }
}
