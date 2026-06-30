// SPDX-License-Identifier: MIT OR Apache-2.0
//! Test-only boot helpers, so integration tests can stand up the REAL multi-shard
//! `run_server` (the SO_REUSEPORT thread-per-core topology + cross-shard coordinator)
//! on an ephemeral port without reaching into private internals or duplicating config
//! plumbing. Not part of the binary's runtime path.

use crate::serve::run_server;
use ironcache_config::{ClusterMode, ClusterTopology, Config, TlsMode};
use ironcache_runtime::bootstrap::ShardSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// Boot a real server on `127.0.0.1:port` across `shards` shards WITH the out-of-band metrics
/// endpoint (OBSERVABILITY.md, #152) bound on `127.0.0.1:metrics_port`, returning the running
/// [`ShardSet`]. Mirrors the binary's `cmd_server` metrics wiring (registry per shard, live/ready
/// flags, `spawn_metrics_server`) so an integration test can scrape the live `/metrics`, `/livez`,
/// `/readyz` over real sockets.
///
/// # Panics
///
/// Panics if the server or the metrics listener fails to bind.
#[must_use]
pub fn run_server_with_metrics_for_test(port: u16, shards: usize, metrics_port: u16) -> ShardSet {
    use crate::metrics_http::{self, MetricsState, ReadyState};
    use ironcache_observe::MetricsRegistry;

    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        ..Config::default()
    };
    let registry = MetricsRegistry::new(shards);
    let live = Arc::new(AtomicBool::new(false));
    // Readiness sized to the shard count: each shard signals its load-on-boot completion through the
    // SAME state threaded into the server boot, so `/readyz` flips to 200 only after every shard has
    // loaded (mirrors cmd_server, #152).
    let ready = Arc::new(ReadyState::with_shards(shards));
    let handles = crate::serve::run_server_observed(
        &config,
        Some(registry.clone()),
        Some(Arc::clone(&ready)),
    )
    .expect("test metrics server failed to bind");
    let runtime = Arc::clone(&handles.runtime);
    let state = MetricsState::new(
        registry,
        Arc::clone(&live),
        Arc::clone(&ready),
        shards,
        Arc::new(move || runtime.maxmemory()),
        handles.raft.clone(),
        handles.persist.clone(),
        handles.topology.clone(),
    );
    let addr = format!("127.0.0.1:{metrics_port}");
    metrics_http::spawn_metrics_server(&addr, state).expect("metrics endpoint failed to bind");
    // Boot complete: mark live (mirrors cmd_server). Readiness is NOT flipped here -- each shard
    // signals it once its load-on-boot finishes.
    live.store(true, std::sync::atomic::Ordering::SeqCst);
    handles.set
}

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

/// Boot a real server on `127.0.0.1:port` across `shards` shards with the CONNECTION-SAFETY limits
/// set (PROD-SAFETY #3/#4/#5): `maxclients` (the simultaneous-connection ceiling; `0` disables),
/// `timeout_secs` (the idle timeout; `0` disables), and `output_buffer_limit` (the per-connection
/// output-buffer hard cap in bytes; `0` disables). Lets an integration test prove each limit is
/// enforced over real sockets (the Nth+1 connection rejected, an idle connection closed, an
/// oversized output dropped) without reaching into private internals.
///
/// # Panics
///
/// Panics if the config fails to validate or the server fails to bind.
#[must_use]
pub fn run_server_with_limits_for_test(
    port: u16,
    shards: usize,
    maxclients: u64,
    timeout_secs: u64,
    output_buffer_limit: u64,
) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        maxclients,
        timeout_secs,
        output_buffer_limit,
        ..Config::default()
    };
    config.validate().expect("test limits config must validate");
    run_server(&config).expect("test limits server failed to bind")
}

/// Boot a real server with a `requirepass` (#65) on `127.0.0.1:port` across `shards` shards (NO
/// persistence), so a test can prove the HOISTED router NOAUTH chokepoint gates EVERY path: an
/// UNAUTHENTICATED client must get `-NOAUTH` for a FOREIGN-shard keyed command (the cross-shard
/// hop), the whole-keyspace fan-outs (KEYS/SCAN/FLUSHALL), and the in-MULTI queue path; after
/// `AUTH <password>` the same commands work. `password` is the PLAINTEXT a client AUTHs with; it is
/// stored hashed at rest (SHA-256 hex), matching `Config::finalize_requirepass` (this builds
/// `Config` directly, bypassing `resolve`, so it must hash here).
///
/// # Panics
///
/// Panics if the config fails to validate or the server fails to bind.
#[must_use]
pub fn run_server_with_auth_for_test(port: u16, shards: usize, password: &str) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        // The runtime auth overlay reads the requirepass as the SHA-256 hex digest AT REST (#65).
        requirepass: Some(ironcache_config::sha256_hex(password.as_bytes())),
        ..Config::default()
    };
    config.validate().expect("test auth config must validate");
    run_server(&config).expect("test auth server failed to bind")
}

/// Boot a real server with an ACL FILE configured (#106) on `127.0.0.1:port` across `shards`
/// shards, so an integration test can prove the aclfile boot-LOAD + `ACL SAVE`/`ACL LOAD` round
/// trip over real sockets. `aclfile` is the path the server loads `user <name> <rules>...` lines
/// from at boot and writes back on `ACL SAVE`. With no `requirepass` and the aclfile holding only
/// an all-permissive `default`, the no-narrowed-user path stays byte-identical until a non-default
/// user is added.
///
/// # Panics
///
/// Panics if the config fails to validate or the server fails to bind.
#[must_use]
pub fn run_server_with_aclfile_for_test(port: u16, shards: usize, aclfile: PathBuf) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        aclfile: Some(aclfile),
        ..Config::default()
    };
    config
        .validate()
        .expect("test aclfile config must validate");
    run_server(&config).expect("test aclfile server failed to bind")
}

/// Boot a real CLUSTER-mode server (static `topology`, this node's `announce_id`) WITH a
/// `requirepass` across `shards` shards, so a test can prove the hoisted NOAUTH chokepoint gates the
/// CLUSTER topology MUTATORS (MEET/FORGET/ADDSLOTS/SETSLOT/DELSLOTS/REPLICATE/SET-CONFIG-EPOCH)
/// before they can take over or WIPE the cluster: an UNAUTHENTICATED client must get `-NOAUTH`, not
/// the mutator's reply. `password` is the PLAINTEXT a client AUTHs with (stored SHA-256 hex at rest).
///
/// # Panics
///
/// Panics if the config fails to validate (a bad topology / password) or the server fails to bind.
#[must_use]
pub fn run_cluster_node_with_auth_for_test(
    port: u16,
    shards: usize,
    topology: ClusterTopology,
    announce_id: &str,
    password: &str,
) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        cluster_enabled: true,
        cluster_topology: Some(topology),
        cluster_announce_id: Some(announce_id.to_owned()),
        requirepass: Some(ironcache_config::sha256_hex(password.as_bytes())),
        ..Config::default()
    };
    config
        .validate()
        .expect("test cluster+auth config must validate");
    run_server(&config).expect("test cluster+auth node failed to bind")
}

/// Boot a real server with PERSISTENCE ENABLED (#58) on `127.0.0.1:port` across `shards` shards,
/// using `data_dir` as the on-disk snapshot location. The server LOADS any committed snapshot in
/// `data_dir` at boot, and `SAVE` / `BGSAVE` write `<data_dir>/dump-shard-<n>.icss` +
/// `<data_dir>/dump.manifest`. `save_interval_secs` / `save_min_changes` set the optional periodic
/// save policy (pass `0`/`0` to disable it -> only explicit SAVE/BGSAVE persist).
///
/// # Panics
///
/// Panics if the config fails to validate or the server fails to bind.
#[must_use]
pub fn run_persist_server_for_test(
    port: u16,
    shards: usize,
    data_dir: PathBuf,
    save_interval_secs: u64,
    save_min_changes: u64,
) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        data_dir: Some(data_dir),
        save_interval_secs,
        save_min_changes,
        ..Config::default()
    };
    config
        .validate()
        .expect("test persist config must validate");
    run_server(&config).expect("test persist server failed to bind")
}

/// Boot a real server with PERSISTENCE ENABLED and a `requirepass` (#58 + #65), so a test can prove
/// the persistence command interception is AUTH-GATED (H2): an UNAUTHENTICATED client must get
/// `-NOAUTH` for SAVE / BGSAVE / LASTSAVE and write no snapshot. `password` is the PLAINTEXT a client
/// AUTHs with; it is stored hashed at rest (SHA-256 hex), matching `Config::finalize_requirepass`
/// (this builds `Config` directly, bypassing `resolve`, so it must hash here).
///
/// # Panics
///
/// Panics if the config fails to validate or the server fails to bind.
#[must_use]
pub fn run_persist_server_with_auth_for_test(
    port: u16,
    shards: usize,
    data_dir: PathBuf,
    password: &str,
) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        data_dir: Some(data_dir),
        // The runtime auth overlay reads the requirepass as the SHA-256 hex digest AT REST (#65).
        requirepass: Some(ironcache_config::sha256_hex(password.as_bytes())),
        ..Config::default()
    };
    config
        .validate()
        .expect("test persist+auth config must validate");
    run_server(&config).expect("test persist+auth server failed to bind")
}

/// Boot a real server with embedded TLS ENABLED (`tls = on`, #105) on `127.0.0.1:port` across
/// `shards` shards, presenting the cert/key at `cert_path` / `key_path`, and return the running
/// [`ShardSet`]. The client listener is TLS-only: a plaintext client to this port fails the
/// handshake.
///
/// # Panics
///
/// Panics if the config fails to validate (e.g. an unreadable cert) or the server fails to bind.
#[must_use]
pub fn run_tls_server_for_test(
    port: u16,
    shards: usize,
    cert_path: PathBuf,
    key_path: PathBuf,
) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        tls: TlsMode::On,
        tls_cert_path: Some(cert_path),
        tls_key_path: Some(key_path),
        ..Config::default()
    };
    config.validate().expect("test TLS config must validate");
    run_server(&config).expect("test TLS server failed to bind")
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

/// Like [`run_cluster_node_for_test`] but across `shards` shards (HA-6 multi-shard online slot
/// migration, COORDINATOR.md #107): the migration ASK decision must resolve a key's presence on the
/// shard that OWNS it (the FNV `owner_shard`), which on a multi-shard node may be a SIBLING of the
/// connection's accept shard. A static cluster node lets a test drive the migration state machine
/// directly (`CLUSTER SETSLOT <slot> MIGRATING <dest>`) without standing up a full raft quorum.
///
/// # Panics
///
/// Panics if the server fails to bind, OR if the topology fails to validate.
#[must_use]
pub fn run_cluster_node_for_test_shards(
    port: u16,
    shards: usize,
    topology: ClusterTopology,
    announce_id: &str,
) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
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

/// Like [`run_raft_node_for_test`] but with the WRITE-SIDE replication guardrail enabled
/// (`min-replicas-to-write`, ADR-0026) at `min_replicas_to_write`, so a guardrail loopback test can
/// drive an owner that REJECTS a write (`-NOREPLICAS`) until enough replicas are in sync. The lag
/// bound (`min_replicas_max_lag`) is left generous so an attached + caught-up replica counts toward
/// the quorum promptly; the failover timeout keeps its default.
///
/// # Panics
///
/// Panics if the server fails to bind, OR if the topology fails to validate.
/// Like [`run_raft_node_for_test`] but ALSO returns the raft-mode [`RaftHandle`](ironcache_server::RaftHandle)
/// (HA-prod-membership), so a test can observe the live Raft CONFIGURATION (voter / learner sets)
/// directly via `RaftHandle::config` without a new wire surface.
///
/// # Panics
///
/// Panics if the server fails to bind, OR if the topology fails to validate.
#[must_use]
pub fn run_raft_node_with_handle(
    port: u16,
    topology: ClusterTopology,
    announce_id: &str,
) -> (ShardSet, Option<ironcache_server::RaftHandle>) {
    let config = Config {
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
    config
        .validate()
        .expect("test raft cluster topology must validate");
    crate::serve::run_server_inner(&config).expect("test raft node failed to bind")
}

/// Like [`run_raft_joining_node_for_test`] but ALSO returns the raft-mode
/// [`RaftHandle`](ironcache_server::RaftHandle), so a test can observe the joiner's adopted
/// membership directly (HA-prod-membership).
///
/// # Panics
///
/// Panics if the server fails to bind, OR if the topology fails to validate.
#[must_use]
pub fn run_raft_joining_node_with_handle(
    port: u16,
    topology: ClusterTopology,
    announce_id: &str,
) -> (ShardSet, Option<ironcache_server::RaftHandle>) {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards: 1,
        databases: 16,
        cluster_enabled: true,
        cluster_mode: ClusterMode::Raft,
        cluster_topology: Some(topology),
        cluster_announce_id: Some(announce_id.to_owned()),
        cluster_raft_joining: true,
        ..Config::default()
    };
    config
        .validate()
        .expect("test raft joining topology must validate");
    crate::serve::run_server_inner(&config).expect("test raft joining node failed to bind")
}

/// Boot a real RAFT node that is JOINING an already-formed cluster at runtime (HA-prod-membership):
/// `cluster_raft_joining = true`, so it boots as a NON-VOTER (it does not campaign and is not in the
/// initial voter set) and learns it is a member only when the leader's committed `AddLearner` (then
/// auto-promote `PromoteLearner`) entry replicates to it after an operator `CLUSTER MEET`. The
/// `topology` lists ALL nodes (so the joiner derives every `NodeId` + bus address); `announce_id` is
/// this joiner's id. Used by the membership loopback to bring up a 4th node that MEET stages in.
///
/// # Panics
///
/// Panics if the server fails to bind, OR if the topology fails to validate.
#[must_use]
pub fn run_raft_joining_node_for_test(
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
        cluster_mode: ClusterMode::Raft,
        cluster_topology: Some(topology),
        cluster_announce_id: Some(announce_id.to_owned()),
        cluster_raft_joining: true,
        ..Config::default()
    };
    config
        .validate()
        .expect("test raft joining topology must validate");
    run_server(&config).expect("test raft joining node failed to bind")
}

#[must_use]
pub fn run_raft_node_for_test_min_replicas(
    port: u16,
    topology: ClusterTopology,
    announce_id: &str,
    min_replicas_to_write: u32,
) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards: 1,
        databases: 16,
        cluster_enabled: true,
        cluster_mode: ClusterMode::Raft,
        cluster_topology: Some(topology),
        cluster_announce_id: Some(announce_id.to_owned()),
        min_replicas_to_write,
        // A generous lag bound so a caught-up replica is counted in-sync quickly.
        min_replicas_max_lag: 1_000_000,
        ..Config::default()
    };
    config
        .validate()
        .expect("test raft cluster topology must validate");
    run_server(&config).expect("test raft cluster node failed to bind")
}
