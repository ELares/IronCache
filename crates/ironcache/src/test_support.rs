// SPDX-License-Identifier: MIT OR Apache-2.0
//! Test-only boot helpers, so integration tests can stand up the REAL multi-shard
//! `run_server` (the SO_REUSEPORT thread-per-core topology + cross-shard coordinator)
//! on an ephemeral port without reaching into private internals or duplicating config
//! plumbing. Not part of the binary's runtime path.

use crate::serve::run_server;
use ironcache_config::{ClusterMode, ClusterTopology, Config};
use ironcache_runtime::bootstrap::ShardSet;

/// Boot a real server on `127.0.0.1:port` across `shards` shards and return the running
/// [`ShardSet`]. The caller keeps the handle alive for the server's lifetime and calls
/// [`ShardSet::shutdown_and_join`] at the end.
///
/// # Panics
///
/// Panics if the server fails to bind (e.g. the port is already in use), which a test
/// wants to surface immediately.
#[must_use]
pub fn run_server_for_test(port: u16, shards: usize) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        ..Config::default()
    };
    run_server(&config).expect("test server failed to bind")
}

/// Boot a real CLUSTER-mode server on `127.0.0.1:port` (single shard) with a static slot
/// `topology` and this node's `announce_id`, returning the running [`ShardSet`]
/// (CLUSTER_CONTRACT.md #70, slice 2). The topology is shared by every node in a cluster
/// integration test; each node passes its OWN matching announce id, so the same map yields a
/// different `self` per node and thus the right MOVED targets.
///
/// # Panics
///
/// Panics if the server fails to bind, OR if the topology fails to validate (a bad map is a
/// test bug the harness should surface immediately, not swallow).
#[must_use]
pub fn run_cluster_node_for_test(
    port: u16,
    topology: ClusterTopology,
    announce_id: &str,
) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards: 1,
        databases: 16,
        cluster_enabled: true,
        cluster_topology: Some(topology),
        cluster_announce_id: Some(announce_id.to_owned()),
        ..Config::default()
    };
    config
        .validate()
        .expect("test cluster topology must validate");
    run_server(&config).expect("test cluster node failed to bind")
}

/// Boot a real RAFT-GOVERNANCE node (HA-4c) on `127.0.0.1:port` (single shard): cluster mode
/// enabled, `cluster_mode = Raft`, with the shared `topology` (which supplies the voter set + the
/// peer cluster-bus addresses) and this node's `announce_id`. The node spawns its Raft
/// control-plane thread (a RAFTMSG listener on `port + 10000`, dialing each peer's bus port) and
/// installs the shared `Arc<SlotMap>` the control plane governs as `ctx.cluster`. Slot ownership
/// is NOT taken from the topology's static ranges here: it is established at runtime through
/// committed `CLUSTER ADDSLOTS` proposals (the topology ranges are ignored in raft-mode, so the
/// caller may leave them empty).
///
/// # Panics
///
/// Panics if the server fails to bind, OR if the topology fails to validate.
#[must_use]
pub fn run_raft_node_for_test(port: u16, topology: ClusterTopology, announce_id: &str) -> ShardSet {
    run_raft_node_for_test_with(port, topology, announce_id, None)
}

/// Like [`run_raft_node_for_test`] but with an OPTIONAL short HA-8 `failover_timeout_secs`
/// override (`Some(secs)`), so a failover test can drive a promotion in seconds instead of
/// waiting the production default. `None` leaves the default. `replica_max_lag` keeps its default
/// (generous, so an attached-and-caught-up replica is in-sync and thus promotable).
///
/// # Panics
///
/// Panics if the server fails to bind, OR if the topology fails to validate.
#[must_use]
pub fn run_raft_node_for_test_with(
    port: u16,
    topology: ClusterTopology,
    announce_id: &str,
    failover_timeout_secs: Option<u64>,
) -> ShardSet {
    let mut config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards: 1,
        databases: 16,
        cluster_enabled: true,
        cluster_mode: ClusterMode::Raft,
        cluster_topology: Some(topology),
        cluster_announce_id: Some(announce_id.to_owned()),
        ..Config::default()
    };
    if let Some(secs) = failover_timeout_secs {
        config.failover_timeout_secs = secs;
    }
    config
        .validate()
        .expect("test raft cluster topology must validate");
    run_server(&config).expect("test raft cluster node failed to bind")
}
