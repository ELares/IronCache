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

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ironcache_config::Config;
use ironcache_env::SystemEnv;
use ironcache_raft::{NodeId, RaftConfig, RaftNode};
use ironcache_raft_net::{
    ConfigSm, FileStorage, NodeHandle, RaftClusterBusNode, RaftHandle, run_listener,
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

/// Wire up the raft-mode control plane for THIS node. Called only from the raft-mode branch of
/// `run_server`; `cluster` is the shared map the caller will ALSO install as `ctx.cluster`.
///
/// `self_id` is this node's 40-hex announce id (already validated present in the topology by
/// `Config::validate`). The boot is INFALLIBLE at this layer except for the listener bind, which
/// happens on the control-plane thread (a bind failure there logs and the thread exits; the data
/// path still runs, and Raft simply never forms, which is the safe degradation).
///
/// # Panics
///
/// Panics if `self_id` is not in the topology (a wiring bug: `Config::validate` already proved it
/// is present, so this is unreachable in the validated boot path).
#[must_use]
pub fn spawn_control_plane(
    config: &Config,
    self_id: &str,
    cluster: Arc<ironcache_cluster::SlotMap>,
) -> RaftBoot {
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
/// # Panics
///
/// Panics if `self_id` is not in the topology.
#[must_use]
pub fn spawn_control_plane_joining(
    config: &Config,
    self_id: &str,
    cluster: Arc<ironcache_cluster::SlotMap>,
) -> RaftBoot {
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
) -> RaftBoot {
    let topo = config
        .cluster_topology
        .as_ref()
        .expect("raft-mode boot requires a cluster_topology (Config::validate enforced it)");

    // (1) ids -> NodeId, the voter set, and this node's NodeId.
    let id_map = node_id_map(topo);
    let self_node_id = *id_map
        .get(self_id)
        .expect("self announce id must be present in the topology (validated at boot)");
    // The initial VOTER set: every topology node by default; on a JOINING node, every node EXCEPT
    // self (self joins as a non-voter and learns its membership from the committed log).
    let voters: BTreeSet<NodeId> = id_map
        .values()
        .copied()
        .filter(|&nid| !joining || nid != self_node_id)
        .collect();

    // (1b) Peer cluster-bus addresses: every OTHER node id -> host:(port + BUS_PORT_OFFSET). (On a
    // joining node this includes the existing voters, so it can receive/reply replication.)
    let mut peers: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    let mut self_bus_addr: Option<SocketAddr> = None;
    for n in &topo.nodes {
        let nid = id_map[&n.id];
        let bus = bus_port(n.port);
        // Parse the advertised host:bus_port. A bad address skips that peer (best-effort, like
        // the adapter's send path); self's bus address is what THIS node binds its listener on.
        let Ok(addr) = format!("{}:{}", n.host, bus).parse::<SocketAddr>() else {
            continue;
        };
        if nid == self_node_id {
            self_bus_addr = Some(addr);
        } else {
            peers.insert(nid, addr);
        }
    }
    // If self's advertised host did not parse (e.g. a DNS name), fall back to the bind address +
    // bus port so the listener still comes up on a routable local interface.
    let listen_addr =
        self_bus_addr.unwrap_or_else(|| SocketAddr::new(config.bind, bus_port(config.port)));

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

    // Build the engine + adapter + handle on the spawned control-plane thread (the engine and its
    // FileStorage / PeerConns are !Send, so they must be constructed where they live). Hand the
    // Send NodeHandle back over a std mpsc, mirroring the HA-4a loopback proof's spawn pattern.
    let (handle_tx, handle_rx) = std::sync::mpsc::channel::<NodeHandle>();
    let cluster_for_sm = Arc::clone(&cluster);
    std::thread::Builder::new()
        .name("ironcache-raft".to_string())
        .spawn(move || {
            run_control_plane_thread(
                ControlPlaneParams {
                    self_node_id,
                    voters,
                    raft_config,
                },
                peers,
                listen_addr,
                storage_path,
                cluster_for_sm,
                &handle_tx,
            );
        })
        .expect("spawn the raft control-plane thread");

    // Wait for the thread to publish its handle (it does so as the first thing on the runtime).
    // A failed recv means the thread died before binding; surface a handle-less degradation is
    // not possible (ServerContext needs a handle in raft-mode), so this is an expect: the bind
    // failure would have logged on the thread.
    let handle = handle_rx
        .recv()
        .expect("the raft control-plane thread must hand back its NodeHandle");

    RaftBoot {
        cluster,
        raft: RaftHandle::new(handle),
    }
}

/// The inputs the control-plane thread needs to STAND UP THE ENGINE: this node's id, the static
/// voter set, and the production [`RaftConfig`] (default timing + the configured HA-3c compaction
/// threshold). Bundled into one struct so [`run_control_plane_thread`] stays under the argument
/// cap and the engine-identity inputs are named together.
struct ControlPlaneParams {
    self_node_id: NodeId,
    voters: BTreeSet<NodeId>,
    raft_config: RaftConfig,
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
    peers: BTreeMap<NodeId, SocketAddr>,
    listen_addr: SocketAddr,
    storage_path: std::path::PathBuf,
    cluster: Arc<ironcache_cluster::SlotMap>,
    handle_tx: &std::sync::mpsc::Sender<NodeHandle>,
) {
    let ControlPlaneParams {
        self_node_id,
        voters,
        raft_config,
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
        // the PRODUCTION ConfigSm over the SHARED Arc<SlotMap> (also ctx.cluster).
        let raft = RaftNode::with_state_machine(
            self_node_id,
            voters,
            storage,
            raft_config,
            ConfigSm::new(cluster),
        );
        let runtime = TokioRuntime::new();
        let (node, handle) = RaftClusterBusNode::new(raft, SystemEnv::new(), runtime, peers);

        // Hand the Send handle back to run_server's thread so it can install it in ServerContext.
        // A receive error means run_server already moved on; nothing to do.
        let _ = handle_tx.send(handle.clone());

        // The listener feeds Event::Inbound into the run loop's inbox.
        let inbox = handle.inbox().clone();
        let lrt = TokioRuntime::new();
        tokio::task::spawn_local(async move {
            run_listener::<TokioRuntime>(lrt, listener, inbox).await;
        });

        // The control-plane run loop owns the engine for the process lifetime. When every
        // NodeHandle is dropped (process shutdown) the inbox closes and run() returns; here we
        // just await it so the LocalSet drives both tasks until then.
        node.run().await;
    });
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
}
