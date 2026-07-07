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
        // The coordinator inbox, so `/metrics` samples the per-shard inbox-depth gauge (#556).
        Some(handles.inbox.clone()),
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

/// Boot a real server on `127.0.0.1:port` across `shards` shards with the per-connection
/// QUERY-BUFFER cap set (#528): `query_buffer_limit` is the inbound-buffer hard cap in bytes (`0`
/// disables). Lets an integration test prove the cap CLOSES a connection that announces a large
/// multibulk and then dribbles bytes (a slow-loris memory-amplification DoS) over a real socket,
/// without reaching into private internals. The output cap is left off (high default) so the two
/// connection-memory caps can be exercised independently.
///
/// # Panics
///
/// Panics if the config fails to validate or the server fails to bind.
#[must_use]
pub fn run_server_with_query_buffer_limit_for_test(
    port: u16,
    shards: usize,
    query_buffer_limit: u64,
) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        query_buffer_limit,
        ..Config::default()
    };
    config
        .validate()
        .expect("test query-buffer-limit config must validate");
    run_server(&config).expect("test query-buffer-limit server failed to bind")
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

/// Boot a SHARD-OWNERS node (#517) on `127.0.0.1` with `shards` internal shards: cluster enabled,
/// `cluster_mode = ShardOwners`, NO static topology (the node auto-owns all 16384 slots, so it serves
/// every key immediately). Returns the running `ShardSet` AND the base port it bound; the node's per-
/// shard listeners are `base .. base + shards - 1`.
///
/// The node binds `shards` CONTIGUOUS ports, but only the base is picked from the OS ephemeral pool,
/// so under parallel test load an adjacent port can be grabbed between probe and bind. This RETRIES
/// the whole boot with a fresh contiguous block until every listener binds -- so the shard-owner
/// tests are robust under the parallel `cargo test --workspace` run (a plain single-port `free_port`
/// reserved only the base and flaked with bind panics).
#[must_use]
pub fn run_shard_owners_node_for_test(shards: usize) -> (ShardSet, u16) {
    for _ in 0..64 {
        let base = free_contiguous_ports(shards);
        let config = Config {
            bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            port: base,
            shards,
            databases: 16,
            cluster_enabled: true,
            cluster_mode: ClusterMode::ShardOwners,
            ..Config::default()
        };
        config
            .validate()
            .expect("shard-owners config must validate");
        // A bind race (an adjacent port taken between probe and bind) returns Err -> retry a fresh
        // block; any OTHER boot error would also retry, but only a transient bind race is plausible.
        if let Ok(set) = run_server(&config) {
            return (set, base);
        }
    }
    panic!("could not bind {shards} contiguous shard-owner ports after 64 attempts");
}

/// Like [`run_shard_owners_node_for_test`] but ALSO stands up the out-of-band `/metrics` endpoint on
/// `127.0.0.1:metrics_port` (#556), so a test can scrape the coordinator hop counters + inbox-depth
/// gauge on a shard-owners node and ASSERT the #517 zero-hop property: a cluster-aware client dialing
/// each key's OWNER port is served locally with NO hop, so `hops_sent` stays ~0. Returns the running
/// `ShardSet` and the base RESP port (`base .. base + shards - 1`; the metrics endpoint is on the
/// separate `metrics_port`).
///
/// # Panics
///
/// Panics if the config fails to validate, or the server / metrics listener fails to bind after the
/// contiguous-port retries.
#[must_use]
pub fn run_shard_owners_node_with_metrics_for_test(
    shards: usize,
    metrics_port: u16,
) -> (ShardSet, u16) {
    use crate::metrics_http::{self, MetricsState, ReadyState};
    use ironcache_observe::MetricsRegistry;
    for _ in 0..64 {
        let base = free_contiguous_ports(shards);
        let config = Config {
            bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            port: base,
            shards,
            databases: 16,
            cluster_enabled: true,
            cluster_mode: ClusterMode::ShardOwners,
            ..Config::default()
        };
        config
            .validate()
            .expect("shard-owners config must validate");
        let registry = MetricsRegistry::new(shards);
        let live = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(ReadyState::with_shards(shards));
        // A bind race on the contiguous RESP block returns Err -> retry a fresh block (mirrors
        // `run_shard_owners_node_for_test`). Only spawn the metrics endpoint once the RESP block bound.
        let Ok(handles) = crate::serve::run_server_observed(
            &config,
            Some(registry.clone()),
            Some(Arc::clone(&ready)),
        ) else {
            continue;
        };
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
            Some(handles.inbox.clone()),
        );
        let addr = format!("127.0.0.1:{metrics_port}");
        metrics_http::spawn_metrics_server(&addr, state).expect("metrics endpoint failed to bind");
        live.store(true, std::sync::atomic::Ordering::SeqCst);
        return (handles.set, base);
    }
    panic!("could not bind {shards} contiguous shard-owner ports after 64 attempts");
}

/// Find a base port `b` such that `b .. b + n - 1` are ALL currently bindable on 127.0.0.1, by
/// holding a listener on every one during the probe (so the block is momentarily reserved as a
/// unit), then releasing them for the caller to bind. Retries until it finds a clean contiguous run.
fn free_contiguous_ports(n: usize) -> u16 {
    let span = u16::try_from(n.max(1)).expect("shard count fits u16");
    for _ in 0..256 {
        let Ok(l0) = std::net::TcpListener::bind(("127.0.0.1", 0)) else {
            continue;
        };
        let base = l0.local_addr().expect("listener has a local addr").port();
        if base.checked_add(span - 1).is_none() {
            continue; // base too high for the block to fit u16; try another ephemeral port
        }
        let mut held = vec![l0];
        let mut all_free = true;
        for i in 1..span {
            if let Ok(l) = std::net::TcpListener::bind(("127.0.0.1", base + i)) {
                held.push(l);
            } else {
                all_free = false;
                break;
            }
        }
        drop(held); // release the block so the caller can bind it
        if all_free {
            return base;
        }
    }
    panic!("could not find {n} contiguous free ports after 256 attempts");
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
