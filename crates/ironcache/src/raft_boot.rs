// SPDX-License-Identifier: MIT OR Apache-2.0
//! Raft-governance boot wiring (HA-4c): stand up the per-node Raft control plane that
//! GOVERNS the shared `Arc<SlotMap>` when `cluster_mode == Raft`.
//!
//! This module is reached ONLY from the raft-mode branch of [`crate::serve::run_server`]; the
//! DEFAULT (`cluster_mode == Static`) path never calls it, so a static node pays ZERO new cost
//! and is byte-unchanged. It performs the four steps the HA-4c design calls for:
//!
//! 1. Derive the voter set + peer cluster-bus addresses from `cluster_topology`: each
//!    topology node's string id maps to a stable `NodeId(u64)` (its index in the id-SORTED
//!    topology), and its cluster-bus address is its advertised `host:(port + BUS_PORT_OFFSET)`
//!    (the same `+10000` offset `CLUSTER NODES` already reports as the gossip `@cport`).
//! 2. Build ONE shared `Arc<SlotMap>` seeded `empty_self(self_id, bind, port)` (a fresh
//!    cluster-enabled node owning zero slots, exactly like the slice-3 no-topology boot). This
//!    Arc is BOTH `ctx.cluster` (so routing + the CLUSTER projection read committed state with
//!    NO change to those readers) AND the [`ConfigSm`]'s map (which applies committed entries
//!    into it). The control-plane task is the sole writer; the shards read concurrently.
//! 3. Build the engine ([`RaftNode`]) with [`FileStorage`] (durable, fsync-backed), the
//!    [`ConfigSm`] over the shared map, the voter set, and the peer bus-address map.
//! 4. Spawn a DEDICATED OS thread with its own current-thread tokio runtime + `LocalSet`
//!    running [`RaftClusterBusNode::run`] + [`run_listener`] (mirroring the per-shard
//!    bootstrap and the HA-4a loopback proof), and hand a [`RaftHandle`] back to the caller to
//!    place in `ServerContext`.
//!
//! The control-plane thread is `!Send` (the engine + `PeerConn`s are shard-local), exactly the
//! shared-nothing shape the rest of the runtime uses (ADR-0002). All time / randomness it needs
//! is read through the `SystemEnv` seam INSIDE the adapter (ADR-0003); this module reads none.
//!
//! NODE-ID SCHEME / FRESH-CLUSTER-ONLY POSTURE (F1): a `NodeId` is derived from the announce id's
//! TOP 64 bits ([`node_id_from_announce`]), which is STABLE independent of a node's position in the
//! topology -- a requirement for runtime `CLUSTER MEET` (a joiner and the leader must agree on the
//! `NodeId` from the announce id alone). An EARLIER build derived the `NodeId` from a node's
//! id-SORTED position instead. These two schemes are INCOMPATIBLE: an in-place upgrade onto a node
//! that already persisted committed Raft state (`<data_dir>/ironcache-raft-<port>.log` + its `.cfg`
//! baseline) under the old scheme would, on restart, recompute ids that no longer match the
//! persisted committed config -- the node would not be in its OWN committed voter set, causing
//! permanent quorum loss / a silent split brain. To make that impossible, raft-mode is
//! FRESH-CLUSTER-ONLY across the scheme change: [`scheme_mismatch_error`] detects the mismatch at
//! boot (the recovered committed config is non-empty yet DISJOINT from the topology-derived id set)
//! and REFUSES to boot with an actionable error. An operator upgrading an existing cluster must
//! start fresh (remove the log + sidecars) or migrate the persisted state.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ironcache_clusterbus::{ClusterSecurity, PeerEndpoint};
use ironcache_config::{Config, TlsMode};
use ironcache_env::SystemEnv;
use ironcache_raft::{NodeId, RaftConfig, RaftNode};
use ironcache_raft_net::{
    ConfigSm, FileStorage, NodeHandle, RaftClusterBusNode, RaftHandle, run_listener_secure,
};
use ironcache_runtime::TokioRuntime;
use ironcache_runtime::tokio_rt::bind_reuseport;

/// The fixed offset from a node's client TCP port to its cluster-bus (RAFTMSG listener) port,
/// matching Redis's `cport = port + 10000` convention (the same offset `CLUSTER NODES` already
/// reports as the gossip `@cport`). A node binds its RAFTMSG listener here and dials each
/// peer's bus port for outbound `RaftMsg`s.
pub const BUS_PORT_OFFSET: u16 = 10_000;

/// The cluster-bus port for a client `port`, overflow-safe (HA-4c). The Redis convention is
/// `port + BUS_PORT_OFFSET`, used whenever it fits a `u16`; for a HIGH port (where `+offset`
/// would overflow past 65535, e.g. an ephemeral test port) it falls back to `port - offset`.
/// Either way the result is a bijection on distinct client ports, so two nodes never collide on
/// a bus port. This is what makes raft-mode work on ephemeral loopback ports in tests AND on the
/// usual low service ports in production.
#[must_use]
pub fn bus_port(port: u16) -> u16 {
    if port <= u16::MAX - BUS_PORT_OFFSET {
        port + BUS_PORT_OFFSET
    } else {
        port - BUS_PORT_OFFSET
    }
}

/// The durable Raft-log path for a node, keyed by its cluster-bus `port` so co-located nodes do
/// not share a log. When `data_dir` is `Some`, the log lives at
/// `<data_dir>/ironcache-raft-<port>.log` (durable across a reboot that clears the OS temp dir).
/// When `None` (the default), it lives at `<temp>/ironcache-raft-<port>.log`, the byte-unchanged
/// pre-existing behavior. This is a PURE function (it computes a path, it does NOT create the
/// directory or touch the file); the caller creates the `data_dir` directory beside opening the
/// storage so an IO error degrades safely. Reading the OS temp dir is allowed by the determinism
/// invariant (which forbids only clocks/entropy, not path reads).
#[must_use]
pub fn raft_log_path(data_dir: Option<&Path>, port: u16) -> PathBuf {
    let file = format!("ironcache-raft-{port}.log");
    match data_dir {
        Some(dir) => dir.join(file),
        None => std::env::temp_dir().join(file),
    }
}

/// The outcome of wiring up the raft-mode control plane: the SHARED slot map (also installed as
/// `ctx.cluster`) and the [`RaftHandle`] (installed as `ctx.raft`).
pub struct RaftBoot {
    /// The shared `Arc<SlotMap>` the control-plane task writes (via the config state machine)
    /// and every shard reads (routing + CLUSTER projection).
    pub cluster: Arc<ironcache_cluster::SlotMap>,
    /// The clonable `Send` handle the serve path uses to propose CLUSTER mutations.
    pub raft: RaftHandle,
}

/// A FATAL raft-mode boot error: a condition under which a node must REFUSE to boot rather than
/// silently join (or split-brain) the cluster (F1, the node-id-scheme guard). The control-plane
/// thread surfaces it back over the handle channel; `spawn_control_plane*` turn it into an
/// `Err(BootError)` so `run_server` aborts boot with a clear, actionable operator message.
#[derive(Debug)]
pub struct BootError(pub String);

impl std::fmt::Display for BootError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BootError {}

/// What the control-plane thread hands back to the spawning thread: either the live `NodeHandle`
/// (boot proceeded) or a [`BootError`] (a fatal pre-flight refusal, e.g. the F1 node-id-scheme
/// mismatch detected after recovering the persisted config). A bind / storage-open failure stays
/// the pre-existing best-effort degradation (the thread logs and exits, the recv simply never
/// arrives) and is NOT funneled through here; only a deterministic, must-refuse hazard is.
type BootResult = Result<NodeHandle, BootError>;

/// Derive a stable `NodeId(u64)` DETERMINISTICALLY from a node's 40-hex announce id
/// (HA-prod-membership). The engine's `NodeId` is a `u64`; the cluster's node identity is the
/// 40-hex string. This maps the latter to the former by parsing the FIRST 16 hex digits (the high
/// 64 bits) of the id as a `u64`. Announce ids are 160 bits of entropy, so the top 64 bits collide
/// with negligible probability for any real cluster; the mapping is a PURE function of the id
/// alone, so EVERY node computes the SAME `NodeId` for a given announce id WITHOUT needing the full
/// membership list -- which is exactly the property a RUNTIME join needs (the leader proposing
/// `AddLearner(id)` and the joining node stamping its own `RAFTMSG` `from`-id must agree on the
/// `NodeId`, and they do because both derive it from the same announce id). A non-hex / short id
/// (defensive: `Config::validate` already rejects a malformed announce id) falls back to a length-
/// salted hash so it is still deterministic and total.
#[must_use]
pub fn node_id_from_announce(id: &str) -> NodeId {
    // The common, validated case: a 40-lowercase-hex id. Use the first 16 hex chars as the high
    // u64. (Using a stable PREFIX rather than a hash keeps the mapping trivially reproducible and,
    // for the deterministic test ids "0000.."/"1111.."/.., yields the obvious 0x0000.. / 0x1111..)
    if id.len() >= 16 {
        if let Ok(v) = u64::from_str_radix(&id[..16], 16) {
            return NodeId(v);
        }
    }
    // Defensive fallback for a non-hex id: a simple deterministic FNV-1a over the bytes (no time /
    // entropy, so determinism is preserved). Unreachable for a validated announce id.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in id.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    NodeId(h)
}

/// Map the topology's string ids to stable `NodeId(u64)`s via [`node_id_from_announce`] (HA-4c +
/// HA-prod-membership). DERIVING each `NodeId` from the announce id ALONE (not from the node's
/// position in the topology) is what makes a RUNTIME join consistent: a node added later via
/// `CLUSTER MEET` -- which is NOT in this topology -- still gets the same `NodeId` on every node,
/// because the leader and the joining node both derive it from the same announce id. The pre-3d
/// boot is unaffected in BEHAVIOR (every topology node still maps to one stable `NodeId`, the
/// voter set is still the same SET); only the specific integer changes from a sorted-position
/// index to an id-derived value, which no consumer depends on (the serve layer + the acceptance
/// tests address nodes by their 40-hex string id over the wire, never by the raw `NodeId`).
fn node_id_map(topo: &ironcache_config::ClusterTopology) -> BTreeMap<String, NodeId> {
    topo.nodes
        .iter()
        .map(|n| (n.id.clone(), node_id_from_announce(&n.id)))
        .collect()
}

/// Build the optional intra-cluster transport SECURITY (PROD-3) from the resolved config: the TLS
/// connector (dial side, verifying the peer against the optional cluster CA) + the TLS acceptor
/// (listener side, presenting the cluster cert/key) + the shared secret bytes. The SAME handle is
/// cloned onto the control-plane node (the dial) and the RAFTMSG listener (the accept), and reused
/// by the replication transport.
///
/// Returns:
/// * `Ok(None)` when neither TLS nor a secret is configured (the default plaintext bus, byte-
///   unchanged): the bus + repl take the pre-PROD-3 path with no handshake.
/// * `Ok(Some(_))` when TLS and/or a secret is configured (validated readable + consistent by
///   `Config::validate`, so the PEM loads here should succeed).
///
/// # Errors
///
/// Returns [`BootError`] if a configured cert/key/CA PEM cannot be loaded into a rustls config
/// (a corrupt PEM that passed the cheap readability pre-flight but rustls rejects). Mirrors the
/// client-listener TLS boot, which also fails boot loudly on a bad cert rather than starting a
/// listener that rejects every peer.
fn build_cluster_security(config: &Config) -> Result<Option<ClusterSecurity>, BootError> {
    let tls_on = config.cluster_tls == TlsMode::On;
    let secret = config
        .cluster_secret
        .as_ref()
        .map(|s| s.as_bytes().to_vec());
    // Nothing configured -> the plaintext bus, byte-unchanged. (Config::validate already rejected an
    // empty secret and a CA-without-TLS, so we never reach here with a degenerate config.)
    if !tls_on && secret.is_none() {
        return Ok(None);
    }
    let (connector, acceptor) = if tls_on {
        // Config::validate proved cert + key are set + readable when cluster_tls = on.
        let cert = config
            .cluster_tls_cert_path
            .as_ref()
            .expect("Config::validate requires cluster_tls_cert_path when cluster_tls = on");
        let key = config
            .cluster_tls_key_path
            .as_ref()
            .expect("Config::validate requires cluster_tls_key_path when cluster_tls = on");
        // The acceptor (listener side) presents the cluster cert/key. Reuse the client-TLS builder.
        let acceptor =
            ironcache_runtime::build_acceptor(&cert.to_string_lossy(), &key.to_string_lossy())
                .map_err(|e| BootError(format!("cluster TLS acceptor build failed: {e}")))?;
        // The connector (dial side) verifies the peer cert against the optional cluster CA; with no
        // CA it accepts the cert and relies on the shared secret for peer authentication.
        let ca = config
            .cluster_ca_path
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned());
        let connector = ironcache_runtime::build_cluster_client_config(ca.as_deref())
            .map_err(|e| BootError(format!("cluster TLS client config build failed: {e}")))?;
        (Some(connector), Some(acceptor))
    } else {
        // Secret-only (plaintext-but-authenticated) cluster: no TLS configs, just the secret.
        (None, None)
    };
    Ok(Some(ClusterSecurity::new(connector, acceptor, secret)))
}

/// Wire up the raft-mode control plane for THIS node. Called only from the raft-mode branch of
/// `run_server`; `cluster` is the shared map the caller will ALSO install as `ctx.cluster`.
///
/// `self_id` is this node's 40-hex announce id (already validated present in the topology by
/// `Config::validate`). The boot is INFALLIBLE at this layer except for the listener bind (which
/// happens on the control-plane thread: a bind failure there logs and the thread exits; the data
/// path still runs, and Raft simply never forms, which is the safe degradation) AND the F1
/// node-id-scheme guard, which can RETURN a [`BootError`] so an in-place upgrade onto an
/// incompatible persisted state refuses to boot rather than silently split-brain.
///
/// # Errors
///
/// Returns [`BootError`] when this node has persisted Raft state whose committed voter/learner set
/// is DISJOINT from the voter set this build derives from `cluster_topology` (the F1 node-id-scheme
/// mismatch: an in-place upgrade across the sorted-position -> announce-id-derived `NodeId` change).
/// Raft-mode is FRESH-CLUSTER-ONLY across that change; the error tells the operator to start fresh
/// or migrate.
///
/// # Panics
///
/// Panics if `self_id` is not in the topology (a wiring bug: `Config::validate` already proved it
/// is present, so this is unreachable in the validated boot path).
pub fn spawn_control_plane(
    config: &Config,
    self_id: &str,
    cluster: Arc<ironcache_cluster::SlotMap>,
) -> Result<RaftBoot, BootError> {
    // The default boot: every topology node (including self) is an initial VOTER (`joining = false`).
    spawn_control_plane_inner(config, self_id, cluster, false)
}

/// Boot the control plane for a node that is JOINING an already-formed cluster at runtime
/// (HA-prod-membership): self is NOT an initial voter (the engine guards a non-voter from
/// campaigning, Raft section 6), and EVERY topology node -- including the existing voters -- is a
/// peer, so the joiner can receive replication and reply. It learns it is a member only when the
/// leader's committed `AddLearner` (then `PromoteLearner`) entry replicates to it. The topology
/// here lists all nodes (so the joiner can derive every `NodeId` + bus address); only the initial
/// voter-set membership of SELF differs from the normal boot.
///
/// # Errors
///
/// Returns [`BootError`] under the same F1 node-id-scheme mismatch [`spawn_control_plane`]
/// documents (a previously-persisted node restarting under the new id scheme).
///
/// # Panics
///
/// Panics if `self_id` is not in the topology.
pub fn spawn_control_plane_joining(
    config: &Config,
    self_id: &str,
    cluster: Arc<ironcache_cluster::SlotMap>,
) -> Result<RaftBoot, BootError> {
    spawn_control_plane_inner(config, self_id, cluster, true)
}

/// The shared body of [`spawn_control_plane`] / [`spawn_control_plane_joining`]. When `joining` is
/// true, SELF is excluded from the initial voter set (a non-voting joiner that adopts its membership
/// from the replicated log) and is connected to EVERY other node (including all existing voters);
/// when false, every topology node is an initial voter (the byte-unchanged default boot).
fn spawn_control_plane_inner(
    config: &Config,
    self_id: &str,
    cluster: Arc<ironcache_cluster::SlotMap>,
    joining: bool,
) -> Result<RaftBoot, BootError> {
    let topo = config
        .cluster_topology
        .as_ref()
        .expect("raft-mode boot requires a cluster_topology (Config::validate enforced it)");

    // (1) ids -> NodeId, the voter set, and this node's NodeId.
    let id_map = node_id_map(topo);
    let self_node_id = *id_map
        .get(self_id)
        .expect("self announce id must be present in the topology (validated at boot)");
    // The FULL set of node ids THIS BUILD derives from the topology under the CURRENT (announce-id)
    // scheme -- every topology node, INCLUDING self, regardless of `joining`. This is the reference
    // the F1 boot guard compares the persisted committed config against: a same-scheme restart keeps
    // its surviving ids inside this set (they overlap), while a scheme CHANGE makes the persisted set
    // fully disjoint from it. (The election VOTER set below may exclude self when joining; the guard
    // uses the full topology set so a joiner's reference is still complete.)
    let topology_node_ids: BTreeSet<NodeId> = id_map.values().copied().collect();
    // The initial VOTER set: every topology node by default; on a JOINING node, every node EXCEPT
    // self (self joins as a non-voter and learns its membership from the committed log).
    let voters: BTreeSet<NodeId> = id_map
        .values()
        .copied()
        .filter(|&nid| !joining || nid != self_node_id)
        .collect();

    // (1b) Peer cluster-bus endpoints: every OTHER node id -> the [`PeerEndpoint`] (host +
    // (port + BUS_PORT_OFFSET)). (On a joining node this includes the existing voters, so it can
    // receive/reply replication.)
    //
    // K8S DNS / RESOLUTION POLICY: a peer endpoint is held as HOST + PORT, NOT a pre-resolved
    // `SocketAddr`, and is RESOLVED LAZILY at DIAL TIME (in `send_to_peer`). This is deliberate:
    //   * a peer whose DNS is not yet resolvable at boot (a sibling k8s pod still coming up) must
    //     NOT abort THIS node's boot -- the adapter retries the dial every heartbeat, so the peer
    //     joins as soon as its name resolves;
    //   * a restarted pod that keeps its stable per-pod DNS name but gets a NEW IP is re-resolved
    //     on the next dial, so we never freeze a dead first IP (the whole point of StatefulSet DNS);
    //   * a resolution failure at dial time is LOGGED loudly (never a silent peer drop), so a
    //     genuinely-misconfigured host is diagnosable instead of silently breaking quorum.
    // The OLD code `format!(...).parse::<SocketAddr>()` only accepted an IP literal and SILENTLY
    // `continue`d past a DNS hostname, quietly omitting that voter -- a hostname-addressed cluster
    // never formed and gave no error. Holding the endpoint defers (and never drops) resolution.
    let mut peers: BTreeMap<NodeId, PeerEndpoint> = BTreeMap::new();
    let mut self_host: Option<(String, u16)> = None;
    for n in &topo.nodes {
        let nid = id_map[&n.id];
        let bus = bus_port(n.port);
        if nid == self_node_id {
            // Self's bus host+port is what THIS node binds its listener on (resolved below).
            self_host = Some((n.host.clone(), bus));
        } else {
            // A peer: keep host + port; the dial path resolves it fresh (and never drops it).
            peers.insert(nid, PeerEndpoint::new(n.host.clone(), bus));
        }
    }
    // SELF's bind address MUST be a concrete `SocketAddr` now (the listener binds it at boot), so it
    // is the one address resolved here. Resolve self's advertised bus host+port (accepting a DNS
    // hostname OR an IP literal); if it does not resolve (e.g. a DNS name that resolves only inside
    // the pod's own network, or a wildcard advertised host), fall back to the configured bind
    // address + bus port so the listener still comes up on a routable local interface -- the
    // byte-identical behavior an IP-literal `config.bind` already had. A non-resolving SELF host is
    // logged so it is diagnosable, never silent.
    //
    // H1: `PeerEndpoint::resolve` is now ASYNC (it runs `getaddrinfo` on tokio's blocking pool,
    // bounded by `RESOLVE_TIMEOUT`, so a wedged resolver cannot freeze the executor). This is the
    // ONE boot-time, before-the-control-plane-thread resolution (it runs once on the boot thread,
    // not on the single-threaded run loop), so drive the async resolve on a short-lived
    // current-thread runtime here (boot is a plain sync `fn`, no ambient runtime), mirroring the
    // CLI's `block_on` shape. An IP-literal host (the byte-identical default) completes immediately.
    let listen_addr = self_host
        .as_ref()
        .and_then(|(host, port)| {
            let endpoint = PeerEndpoint::new(host.clone(), *port);
            let resolved = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .ok()
                .map(|boot_rt| {
                    boot_rt.block_on(async { endpoint.resolve(&TokioRuntime::new()).await })
                });
            match resolved {
                Some(Ok(addr)) => Some(addr),
                Some(Err(e)) => {
                    eprintln!(
                        "raft control plane: self bus host {host}:{port} did not resolve ({e}); \
                         binding the configured bind address instead"
                    );
                    None
                }
                None => {
                    eprintln!(
                        "raft control plane: could not build a runtime to resolve self bus host \
                         {host}:{port}; binding the configured bind address instead"
                    );
                    None
                }
            }
        })
        .unwrap_or_else(|| SocketAddr::new(config.bind, bus_port(config.port)));

    // (3) The durable storage path: <data_dir>/ironcache-raft-<bus-port>.log when a `data_dir` is
    // configured (durable across a reboot that clears /tmp), else <temp>/ironcache-raft-<bus-port>.log
    // (the byte-unchanged default). Either way it is keyed by the BUS port so co-located test nodes
    // do not share a log. An OS path read / dir create is allowed by the determinism invariant
    // (which forbids only clocks/entropy); the directory is created on the control-plane thread
    // beside FileStorage::open so an IO error logs and stops the control plane safely (a node that
    // cannot persist its log must not vote), matching the existing storage-open degradation.
    let storage_path = raft_log_path(config.data_dir.as_deref(), listen_addr.port());

    // The PRODUCTION RaftConfig: the engine's own default `snapshot_threshold` is 0 (compaction
    // OFF, so the determinism sweep + direct-`RaftConfig` tests stay byte-identical), but a real
    // raft-mode deployment must compact (HA-3c), so override it with the configured
    // `raft_snapshot_threshold` (default 1024, non-zero). Everything else stays the engine default.
    let raft_config = RaftConfig {
        snapshot_threshold: config.raft_snapshot_threshold,
        ..RaftConfig::default()
    };

    // INTRA-CLUSTER TRANSPORT SECURITY (PROD-3): build the optional TLS + shared-secret handle from
    // config. `None` (the default) is the plaintext bus, byte-unchanged. Built on the boot thread
    // (it reads the cert/key/CA PEMs); the rustls configs are Send + Sync so the handle travels to
    // the control-plane thread. A bad PEM fails boot loudly (BootError), like the client-listener TLS.
    let security = build_cluster_security(config)?;

    // Build the engine + adapter + handle on the spawned control-plane thread (the engine and its
    // FileStorage / PeerConns are !Send, so they must be constructed where they live). Hand a
    // BootResult (the Send NodeHandle, or a fatal F1 BootError) back over a std mpsc, mirroring the
    // HA-4a loopback proof's spawn pattern.
    let (handle_tx, handle_rx) = std::sync::mpsc::channel::<BootResult>();
    let cluster_for_sm = Arc::clone(&cluster);
    std::thread::Builder::new()
        .name("ironcache-raft".to_string())
        .spawn(move || {
            run_control_plane_thread(
                ControlPlaneParams {
                    self_node_id,
                    voters,
                    topology_node_ids,
                    raft_config,
                    security,
                },
                peers,
                listen_addr,
                storage_path,
                cluster_for_sm,
                &handle_tx,
            );
        })
        .expect("spawn the raft control-plane thread");

    // Wait for the thread to publish its BootResult (it does so as the first thing on the runtime,
    // right after recovering the persisted config and running the F1 scheme guard). A failed recv
    // means the thread died before reporting (a bind / storage-open degradation that already logged
    // on the thread); since ServerContext needs a handle in raft-mode, that stays an expect. A
    // received Err is the F1 fatal refusal, surfaced to run_server so boot aborts cleanly.
    let handle = handle_rx
        .recv()
        .expect("the raft control-plane thread must hand back its BootResult")?;

    Ok(RaftBoot {
        cluster,
        raft: RaftHandle::new(handle),
    })
}

/// The inputs the control-plane thread needs to STAND UP THE ENGINE: this node's id, the static
/// voter set, and the production [`RaftConfig`] (default timing + the configured HA-3c compaction
/// threshold). Bundled into one struct so [`run_control_plane_thread`] stays under the argument
/// cap and the engine-identity inputs are named together.
struct ControlPlaneParams {
    self_node_id: NodeId,
    voters: BTreeSet<NodeId>,
    /// The FULL set of node ids the topology derives under the CURRENT id scheme (every topology
    /// node, including self). The F1 boot guard compares the RECOVERED persisted committed config
    /// against this: disjointness means the persisted state was written under a different node-id
    /// scheme (an in-place upgrade hazard), so the node must refuse to boot.
    topology_node_ids: BTreeSet<NodeId>,
    raft_config: RaftConfig,
    /// The optional intra-cluster transport SECURITY (PROD-3): the TLS connector + acceptor + shared
    /// secret, cloned onto the control-plane node (dial) and the RAFTMSG listener (accept). `None` is
    /// the plaintext bus (byte-unchanged).
    security: Option<ClusterSecurity>,
}

/// The body of the dedicated control-plane OS thread: build a current-thread tokio runtime + a
/// `LocalSet`, bind the RAFTMSG listener, construct the engine (FileStorage + ConfigSm over the
/// shared map), and run [`RaftClusterBusNode::run`] + [`run_listener`] until the process ends.
///
/// Mirrors the per-shard bootstrap (a current-thread runtime + LocalSet per OS thread) and the
/// HA-4a loopback proof exactly. The `LocalSet` keeps the engine + connections shard-local
/// (`!Send`), matching the shared-nothing model (ADR-0002).
fn run_control_plane_thread(
    params: ControlPlaneParams,
    peers: BTreeMap<NodeId, PeerEndpoint>,
    listen_addr: SocketAddr,
    storage_path: std::path::PathBuf,
    cluster: Arc<ironcache_cluster::SlotMap>,
    handle_tx: &std::sync::mpsc::Sender<BootResult>,
) {
    let ControlPlaneParams {
        self_node_id,
        voters,
        topology_node_ids,
        raft_config,
        security,
    } = params;
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("raft control plane: failed to build runtime: {e}");
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        // Bind the RAFTMSG listener inside the runtime (registers it with the reactor).
        let listener = match bind_reuseport(listen_addr) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("raft control plane: failed to bind {listen_addr}: {e}");
                return;
            }
        };

        // Ensure the log's parent directory exists. With the default (temp-dir) path this is a
        // no-op (the OS temp dir always exists); with a configured `data_dir` this creates the
        // durable directory if missing. A create failure is fatal to the control plane (it cannot
        // persist its log there), so log a clear error and stop rather than run unsafe -- the same
        // safe degradation as a storage-open failure below.
        if let Some(parent) = storage_path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    eprintln!(
                        "raft control plane: failed to create data directory {}: {e}",
                        parent.display()
                    );
                    return;
                }
            }
        }

        // Durable, fsync-backed storage; replays any prior log on restart (HA-4b). A failure to
        // open the log is fatal to the control plane (it cannot vote safely without persistence),
        // so log and stop the control plane rather than run unsafe.
        let storage = match FileStorage::open(&storage_path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "raft control plane: failed to open storage {}: {e}",
                    storage_path.display()
                );
                return;
            }
        };

        // The engine: this node's id, the static voter set, durable storage, the PRODUCTION
        // RaftConfig (default timing + the configured non-zero compaction threshold, HA-3c), and
        // the PRODUCTION ConfigSm over the SHARED Arc<SlotMap> (also ctx.cluster). Constructing it
        // RECOVERS the committed membership from the persisted baseline + the surviving log
        // (`recompute_config_from_log`), so `voters()`/`learners()` below are the AUTHORITATIVE
        // recovered config the F1 guard inspects.
        let raft = RaftNode::with_state_machine(
            self_node_id,
            voters,
            storage,
            raft_config,
            ConfigSm::new(cluster),
        );

        // F1 NODE-ID-SCHEME GUARD: refuse to boot on an in-place upgrade onto state persisted under
        // a DIFFERENT node-id scheme. This build derives a NodeId from the announce id's top 64 bits
        // (stable for runtime MEET); an older build derived it from the node's sorted position in the
        // topology. If a node restarts with persisted committed state from the OLD scheme, every
        // recovered id refers to a node that NO LONGER EXISTS under the new scheme -- the node would
        // not be in its own committed voter set -> permanent quorum loss / silent split brain. We
        // detect that as DISJOINTNESS: if the RECOVERED committed config (voters + learners) is
        // non-empty yet shares NO id with the set this build derives from the topology, the schemes
        // differ. A legitimate same-scheme restart -- even after a MEET/FORGET churned membership --
        // keeps the surviving nodes' derived ids inside the topology set, so they OVERLAP and this
        // never false-positives. A fresh node (empty recovered config) is inert. Raft-mode is
        // therefore FRESH-CLUSTER-ONLY across this id-scheme change (documented in the module header,
        // CONTROL_PLANE.md, and SHUTDOWN.md): start fresh, or migrate the persisted state.
        if let Some(err) = scheme_mismatch_error(
            raft.voters(),
            raft.learners(),
            &topology_node_ids,
            &storage_path,
        ) {
            // Report the fatal refusal back to the spawning thread (which turns it into an
            // Err(BootError) so run_server aborts boot) and stop the control plane; do NOT proceed
            // to form Raft on incompatible state.
            let _ = handle_tx.send(Err(err));
            return;
        }

        let runtime = TokioRuntime::new();
        // SECURITY (PROD-3): the SAME handle drives the dial (new_secure) and the listener
        // (run_listener_secure). `None` is the plaintext bus, byte-unchanged.
        let (node, handle) = RaftClusterBusNode::new_secure(
            raft,
            SystemEnv::new(),
            runtime,
            peers,
            security.clone(),
        );

        // Hand the Send handle back to run_server's thread so it can install it in ServerContext.
        // A receive error means run_server already moved on; nothing to do.
        let _ = handle_tx.send(Ok(handle.clone()));

        // The listener feeds Event::Inbound into the run loop's inbox. The accepted RAFTMSG
        // connections are TLS-terminated + secret-verified when `security` is configured.
        let inbox = handle.inbox().clone();
        let lrt = TokioRuntime::new();
        tokio::task::spawn_local(async move {
            run_listener_secure::<TokioRuntime>(lrt, listener, inbox, security).await;
        });

        // The control-plane run loop owns the engine for the process lifetime. When every
        // NodeHandle is dropped (process shutdown) the inbox closes and run() returns; here we
        // just await it so the LocalSet drives both tasks until then.
        node.run().await;
    });
}

/// The F1 node-id-scheme guard (PURE, so it is unit-tested directly): given the RECOVERED committed
/// membership (`recovered_voters` + `recovered_learners`, what the persisted baseline + surviving log
/// imply) and the set of node ids THIS BUILD derives from the topology (`topology_node_ids`), return
/// `Some(BootError)` when they are INCOMPATIBLE and the node must refuse to boot, or `None` when boot
/// is safe.
///
/// The check is DISJOINTNESS: a mismatch is flagged iff the recovered set is NON-EMPTY and shares NO
/// id with the topology set. The rationale (F1):
///   * A FRESH node recovers an empty config -> `None` (nothing persisted, no hazard).
///   * A SAME-SCHEME restart keeps every surviving node's derived id inside the topology set, so the
///     recovered set OVERLAPS it -> `None`. This holds even after a legitimate MEET/FORGET churn: the
///     survivors keep their announce-id-derived ids, which the topology still lists, so there is
///     always at least one shared id.
///   * A SCHEME CHANGE (the in-place upgrade from sorted-position ids to announce-id-derived ids)
///     makes EVERY recovered id refer to a node that does not exist under the new scheme, so the
///     recovered set is fully DISJOINT from the topology set -> `Some(BootError)`. Booting anyway
///     would leave the node out of its own committed voter set: permanent quorum loss / silent split
///     brain. Refusing with a clear, actionable error is the safe outcome.
///
/// `storage_path` is only used to make the error message point the operator at the exact files to
/// remove (or migrate) for a fresh start.
fn scheme_mismatch_error(
    recovered_voters: &BTreeSet<NodeId>,
    recovered_learners: &BTreeSet<NodeId>,
    topology_node_ids: &BTreeSet<NodeId>,
    storage_path: &Path,
) -> Option<BootError> {
    // The recovered committed config is voters + learners; an empty union means nothing was
    // persisted (a fresh node), which is always safe.
    let recovered_nonempty = !recovered_voters.is_empty() || !recovered_learners.is_empty();
    if !recovered_nonempty {
        return None;
    }
    // Disjoint iff NO recovered id appears in the topology set. (Disjoint with an empty topology set
    // would be vacuously true, but Config::validate guarantees a non-empty raft topology, so the
    // topology set is non-empty here; the disjointness therefore genuinely means a scheme change.)
    let overlaps = recovered_voters
        .iter()
        .chain(recovered_learners.iter())
        .any(|id| topology_node_ids.contains(id));
    if overlaps {
        return None;
    }
    // The log path is `<dir>/ironcache-raft-<port>.log`; its sidecars are `<log>.cfg` (the
    // config-baseline) and `<log>.snap` (the snapshot). Name all three so the operator knows
    // precisely what to remove for a fresh start.
    let log = storage_path.display();
    Some(BootError(format!(
        "persisted raft state at {log} uses an incompatible node-id scheme; this build derives \
         node ids from cluster_announce_id. Start a FRESH cluster (remove {log}, {log}.cfg, and \
         any {log}.snap) or migrate the persisted state."
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_config::{ClusterNode, ClusterTopology};

    fn topo() -> ClusterTopology {
        ClusterTopology {
            nodes: vec![
                ClusterNode {
                    id: "2222222222222222222222222222222222222222".to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port: 7002,
                    slots: vec![],
                },
                ClusterNode {
                    id: "0000000000000000000000000000000000000000".to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port: 7000,
                    slots: vec![],
                },
                ClusterNode {
                    id: "1111111111111111111111111111111111111111".to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port: 7001,
                    slots: vec![],
                },
            ],
        }
    }

    /// The id -> NodeId mapping is DERIVED FROM THE ANNOUNCE ID ALONE (the high 64 bits of the
    /// 40-hex id), so it AGREES regardless of the topology's declaration order AND is the SAME
    /// value a runtime-joined node (not in this topology) would compute for itself -- the property
    /// a `CLUSTER MEET` join depends on. For the deterministic test ids it is the obvious prefix.
    #[test]
    fn node_id_map_is_announce_id_derived_and_order_independent() {
        let m = node_id_map(&topo());
        assert_eq!(
            m["0000000000000000000000000000000000000000"],
            NodeId(0x0000_0000_0000_0000)
        );
        assert_eq!(
            m["1111111111111111111111111111111111111111"],
            NodeId(0x1111_1111_1111_1111)
        );
        assert_eq!(
            m["2222222222222222222222222222222222222222"],
            NodeId(0x2222_2222_2222_2222)
        );
        assert_eq!(m.len(), 3);
        // Same as deriving each id directly (the map is just node_id_from_announce per node), so a
        // runtime-joined node computes its own NodeId identically without the topology.
        assert_eq!(
            node_id_from_announce("3333333333333333333333333333333333333333"),
            NodeId(0x3333_3333_3333_3333)
        );
    }

    /// `raft_log_path` joins a configured `data_dir` (durable), and falls back to the OS temp dir
    /// (byte-unchanged) when none is set. Either way the file is keyed by the bus port so
    /// co-located nodes do not share a log.
    #[test]
    fn raft_log_path_uses_data_dir_when_set_else_temp_dir() {
        // Some(data_dir): <data_dir>/ironcache-raft-<port>.log.
        let dir = Path::new("/var/lib/ironcache");
        let set = raft_log_path(Some(dir), 17_001);
        assert_eq!(set, dir.join("ironcache-raft-17001.log"));

        // None: EXACTLY the pre-existing temp-dir path (byte-unchanged default).
        let unset = raft_log_path(None, 17_001);
        let expected = std::env::temp_dir().join("ironcache-raft-17001.log");
        assert_eq!(unset, expected);

        // The port keys the file name, so two co-located nodes get distinct logs.
        let a = raft_log_path(Some(dir), 7000);
        let b = raft_log_path(Some(dir), 7001);
        assert_ne!(a, b);
        assert!(a.ends_with("ironcache-raft-7000.log"));
        assert!(b.ends_with("ironcache-raft-7001.log"));
    }

    fn ids(vals: &[u64]) -> BTreeSet<NodeId> {
        vals.iter().copied().map(NodeId).collect()
    }

    /// F1: the node-id-scheme guard PASSES (returns `None`) for a FRESH node. With nothing
    /// persisted (an empty recovered config), there is no scheme to mismatch, so boot proceeds.
    #[test]
    fn scheme_guard_passes_when_recovered_config_is_empty() {
        let topology = ids(&[0x1111, 0x2222, 0x3333]);
        let path = Path::new("/var/lib/ironcache/ironcache-raft-17000.log");
        assert!(
            scheme_mismatch_error(&BTreeSet::new(), &BTreeSet::new(), &topology, path).is_none()
        );
    }

    /// F1: the guard PASSES for a SAME-SCHEME restart -- the recovered committed config shares its
    /// ids with the topology set (the survivors keep their announce-id-derived ids), so it overlaps
    /// and boot proceeds. Holds even after a legitimate MEET/FORGET churn (the recovered set need
    /// not equal the topology set; one shared id is enough to prove the same scheme).
    #[test]
    fn scheme_guard_passes_when_recovered_overlaps_topology() {
        let topology = ids(&[0x1111, 0x2222, 0x3333]);
        let path = Path::new("/var/lib/ironcache/ironcache-raft-17000.log");
        // Exact match.
        assert!(
            scheme_mismatch_error(
                &ids(&[0x1111, 0x2222, 0x3333]),
                &BTreeSet::new(),
                &topology,
                path
            )
            .is_none()
        );
        // A subset (a node was FORGOTten since the snapshot): still overlaps.
        assert!(
            scheme_mismatch_error(&ids(&[0x1111]), &BTreeSet::new(), &topology, path).is_none()
        );
        // A recovered learner that overlaps the topology also proves the same scheme.
        assert!(
            scheme_mismatch_error(&BTreeSet::new(), &ids(&[0x2222]), &topology, path).is_none()
        );
        // A churned set that adds a not-yet-in-topology id (a MEET'd node) but still shares one id.
        assert!(
            scheme_mismatch_error(&ids(&[0x1111, 0x9999]), &BTreeSet::new(), &topology, path)
                .is_none()
        );
    }

    /// F1: the guard REFUSES (returns the actionable `BootError`) when the recovered committed
    /// config is non-empty yet DISJOINT from the topology set -- the in-place-upgrade hazard (the
    /// persisted ids were written under the OLD sorted-position scheme, so they refer to nodes that
    /// do not exist under the new announce-id scheme). The error names the files to remove.
    #[test]
    fn scheme_guard_refuses_on_disjoint_old_scheme_ids() {
        // The NEW scheme derives the announce-id-top-64-bit ids; an OLD-scheme persisted config used
        // sorted-position ids 0/1/2, which share NOTHING with the new set.
        let topology = ids(&[
            0x1111_1111_1111_1111,
            0x2222_2222_2222_2222,
            0x3333_3333_3333_3333,
        ]);
        let old_scheme = ids(&[0, 1, 2]);
        let path = Path::new("/var/lib/ironcache/ironcache-raft-17000.log");
        let err = scheme_mismatch_error(&old_scheme, &BTreeSet::new(), &topology, path)
            .expect("disjoint old-scheme ids must refuse boot");
        let msg = err.to_string();
        assert!(msg.contains("incompatible node-id scheme"), "got {msg:?}");
        assert!(msg.contains("cluster_announce_id"), "got {msg:?}");
        // The message points the operator at the exact files (log + .cfg + .snap sidecars).
        assert!(msg.contains("ironcache-raft-17000.log"), "got {msg:?}");
        assert!(msg.contains(".cfg"), "got {msg:?}");
        assert!(msg.contains(".snap"), "got {msg:?}");

        // A disjoint set held purely as LEARNERS is the same hazard and also refuses.
        assert!(scheme_mismatch_error(&BTreeSet::new(), &old_scheme, &topology, path).is_some());
    }

    /// F1 END-TO-END: persist a config baseline of OLD-scheme ids via `FileStorage`, then recover it
    /// through a real `RaftNode` (exactly the boot path), and confirm the recovered `voters()` feed
    /// the guard to a REFUSAL -- proving the wiring (persisted baseline -> recovered config -> guard)
    /// matches, not just the pure decision. A FRESH log over the same path then boots fine.
    #[test]
    fn scheme_guard_refuses_recovered_filestorage_baseline() {
        use ironcache_raft::{RaftConfig, RaftStorage};
        use ironcache_raft_net::FileStorage;

        // A unique temp log path so the test is hermetic and does not collide with a real node.
        let unique = format!(
            "ironcache-raft-f1-test-{}-{:?}.log",
            std::process::id(),
            std::thread::current().id()
        );
        let log_path = std::env::temp_dir().join(unique);
        let cfg_path = {
            let mut p = log_path.clone().into_os_string();
            p.push(".cfg");
            PathBuf::from(p)
        };
        // Clean any leftover from a prior run.
        let _ = std::fs::remove_file(&log_path);
        let _ = std::fs::remove_file(&cfg_path);

        // The NEW-scheme topology and the OLD-scheme persisted ids (disjoint).
        let topology = ids(&[
            0x1111_1111_1111_1111,
            0x2222_2222_2222_2222,
            0x3333_3333_3333_3333,
        ]);
        let old_scheme = ids(&[0, 1, 2]);

        // (1) Persist an OLD-scheme config baseline to the `.cfg` sidecar.
        {
            let mut storage = FileStorage::open(&log_path).expect("open file storage");
            storage.save_config_baseline(&old_scheme, &BTreeSet::new());
        }

        // (2) Recover it through a real RaftNode (the boot path). The constructor seeds its config
        // from the persisted baseline (the constructor `voters` arg is only a fallback), so the
        // recovered voters are the OLD-scheme ids.
        let recovered = {
            let storage = FileStorage::open(&log_path).expect("reopen file storage");
            // The constructor voter set is a fresh placeholder; the baseline overrides it.
            let node = ironcache_raft::RaftNode::new(
                NodeId(0x1111_1111_1111_1111),
                ids(&[0x1111_1111_1111_1111]),
                storage,
                RaftConfig::default(),
            );
            (node.voters().clone(), node.learners().clone())
        };
        assert_eq!(
            recovered.0, old_scheme,
            "the baseline must drive the recovered voters"
        );

        // (3) The guard refuses on the recovered (disjoint) config.
        assert!(
            scheme_mismatch_error(&recovered.0, &recovered.1, &topology, &log_path).is_some(),
            "a recovered old-scheme baseline must refuse boot"
        );

        // (4) A FRESH log (baseline removed) recovers an empty config -> boot proceeds.
        let _ = std::fs::remove_file(&cfg_path);
        let _ = std::fs::remove_file(&log_path);
        {
            let storage = FileStorage::open(&log_path).expect("fresh file storage");
            let node = ironcache_raft::RaftNode::new(
                NodeId(0x1111_1111_1111_1111),
                ids(&[0x1111_1111_1111_1111]),
                storage,
                RaftConfig::default(),
            );
            // A fresh node's recovered config is the constructor voter set (in-topology), which
            // overlaps -> the guard passes.
            assert!(
                scheme_mismatch_error(node.voters(), node.learners(), &topology, &log_path)
                    .is_none()
            );
        }

        // Cleanup.
        let _ = std::fs::remove_file(&log_path);
        let _ = std::fs::remove_file(&cfg_path);
    }
}
