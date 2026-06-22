// SPDX-License-Identifier: MIT OR Apache-2.0
//! Acquire a single node's state into a snapshot, and the single-node topology
//! model (issue #366).
//!
//! [`acquire_node`] connects to one node, `AUTH`s (if configured), `PING`s, and
//! `INFO`s, folding the result into a [`NodeSnapshot`]. A down / refused / hung
//! node is NOT an error here: it yields a snapshot with `reachable = false` and an
//! `error` string, never a panic and never a hang (every node operation is bounded
//! by the timeouts the caller passes through to [`crate::node::NodeClient`]).
//!
//! [`Topology`] is the cluster-wide view the poller publishes. The MVP models the
//! single-node case: a standalone node when `cluster_enabled` is false, with room
//! for a clustered mode later ([`TopologyMode`]).
//!
//! ## Determinism (ADR-0003)
//!
//! The only nondeterminism is the `fetched_unixtime` stamp, taken through the
//! `ironcache-env` [`Clock`] seam, never `SystemTime::now` directly.

use std::time::Duration;

use ironcache_env::Clock;

use crate::info::{NodeInfo, parse_info};
use crate::node::{NodeAuth, NodeClient, NodeError, NodeTls};

/// A point-in-time view of ONE node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSnapshot {
    /// The node address (`host:port`) acquired.
    pub addr: String,
    /// Whether the node answered (connect + AUTH + PING + INFO all succeeded).
    pub reachable: bool,
    /// A human-readable error when `reachable` is false (the failure reason);
    /// `None` when reachable. Never contains a secret.
    pub error: Option<String>,
    /// The parsed `INFO` when reachable; `None` otherwise.
    pub info: Option<NodeInfo>,
    /// Unix time (seconds) the snapshot was taken, via the env clock seam.
    pub fetched_unixtime: u64,
}

/// The deployment shape the console believes it is monitoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyMode {
    /// A single standalone node (`cluster_enabled = false`). The MVP shape.
    Standalone,
    /// A clustered deployment (`cluster_enabled = true`). Modeled minimally here
    /// (the seed reports cluster mode); full multi-node discovery lands later.
    Clustered,
}

/// The cluster-wide view the poll loop publishes for the REST/UI layers to read.
/// The single-node MVP holds exactly one node; the shape leaves room to grow to a
/// list of nodes once cluster discovery lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topology {
    /// The deployment mode inferred from the acquired node(s).
    pub mode: TopologyMode,
    /// The acquired node snapshots. The MVP has exactly one (the first seed).
    pub nodes: Vec<NodeSnapshot>,
    /// Unix time (seconds) this topology was assembled.
    pub fetched_unixtime: u64,
}

impl Topology {
    /// Whether ANY node in the topology answered. The poller treats a topology
    /// with no reachable node as a FAILED poll (so `/readyz` stays not-ready and
    /// the failure counter advances) even though it produced a (degraded) view.
    #[must_use]
    pub fn any_reachable(&self) -> bool {
        self.nodes.iter().any(|n| n.reachable)
    }
}

/// Connect to `addr`, AUTH (if `auth`), PING, and INFO, folding the result into a
/// [`NodeSnapshot`]. NEVER returns an error: a connect/timeout/auth/protocol
/// failure becomes `reachable = false` with the reason in `error`. The
/// `fetched_unixtime` is stamped from `clock` (the env seam).
pub async fn acquire_node<C: Clock>(
    clock: &C,
    addr: &str,
    tls: Option<&NodeTls>,
    auth: Option<&NodeAuth>,
    connect_timeout: Duration,
    op_timeout: Duration,
) -> NodeSnapshot {
    let fetched_unixtime = clock.now_unix_millis() / 1000;
    match acquire_inner(addr, tls, auth, connect_timeout, op_timeout).await {
        Ok(info) => NodeSnapshot {
            addr: addr.to_owned(),
            reachable: true,
            error: None,
            info: Some(info),
            fetched_unixtime,
        },
        Err(e) => NodeSnapshot {
            addr: addr.to_owned(),
            reachable: false,
            error: Some(e.to_string()),
            info: None,
            fetched_unixtime,
        },
    }
}

/// The fallible body of [`acquire_node`]: connect + AUTH + PING + INFO -> parsed
/// [`NodeInfo`]. Any [`NodeError`] propagates here and is captured by the caller.
async fn acquire_inner(
    addr: &str,
    tls: Option<&NodeTls>,
    auth: Option<&NodeAuth>,
    connect_timeout: Duration,
    op_timeout: Duration,
) -> Result<NodeInfo, NodeError> {
    let mut client = NodeClient::connect(addr, tls, auth, connect_timeout, op_timeout).await?;
    client.ping().await?;
    let info_body = client.info().await?;
    Ok(parse_info(&info_body))
}

/// Assemble a single-node [`Topology`] from one acquired snapshot. The mode is
/// [`TopologyMode::Clustered`] when the node reports `cluster_enabled`, else
/// [`TopologyMode::Standalone`] (the MVP default, including for an unreachable
/// node where the mode is unknown).
#[must_use]
pub fn single_node_topology<C: Clock>(clock: &C, snapshot: NodeSnapshot) -> Topology {
    let fetched_unixtime = clock.now_unix_millis() / 1000;
    let mode = match &snapshot.info {
        Some(info) if info.cluster_enabled => TopologyMode::Clustered,
        _ => TopologyMode::Standalone,
    };
    Topology {
        mode,
        nodes: vec![snapshot],
        fetched_unixtime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_env::SystemEnv;
    use tokio::io::AsyncWriteExt as _;

    fn standalone_snapshot(addr: &str) -> NodeSnapshot {
        let info = NodeInfo {
            cluster_enabled: false,
            ..Default::default()
        };
        NodeSnapshot {
            addr: addr.to_owned(),
            reachable: true,
            error: None,
            info: Some(info),
            fetched_unixtime: 1,
        }
    }

    #[test]
    fn standalone_topology_when_cluster_disabled() {
        let env = SystemEnv::new();
        let topo = single_node_topology(&env, standalone_snapshot("127.0.0.1:6379"));
        assert_eq!(topo.mode, TopologyMode::Standalone);
        assert_eq!(topo.nodes.len(), 1);
        assert!(topo.any_reachable());
    }

    #[test]
    fn clustered_topology_when_cluster_enabled() {
        let env = SystemEnv::new();
        let mut snap = standalone_snapshot("127.0.0.1:6379");
        snap.info.as_mut().unwrap().cluster_enabled = true;
        let topo = single_node_topology(&env, snap);
        assert_eq!(topo.mode, TopologyMode::Clustered);
    }

    #[test]
    fn unreachable_node_is_standalone_and_not_reachable() {
        let env = SystemEnv::new();
        let snap = NodeSnapshot {
            addr: "127.0.0.1:1".to_owned(),
            reachable: false,
            error: Some("connect refused".to_owned()),
            info: None,
            fetched_unixtime: 0,
        };
        let topo = single_node_topology(&env, snap);
        assert_eq!(topo.mode, TopologyMode::Standalone);
        assert!(!topo.any_reachable());
    }

    /// Acquiring a DOWN node (nothing listening) yields a not-reachable snapshot
    /// with an error string, never a panic or a hang.
    #[tokio::test]
    async fn acquire_down_node_is_unreachable_not_panic() {
        let env = SystemEnv::new();
        // A port we bind then drop, so the dial is refused.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let snap = acquire_node(
            &env,
            &addr,
            None,
            None,
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .await;
        assert!(!snap.reachable);
        assert!(snap.error.is_some());
        assert!(snap.info.is_none());
        assert_eq!(snap.addr, addr);
    }

    /// Acquiring a hung node (accepts but never replies) yields a not-reachable
    /// snapshot promptly via the op timeout, never a hang.
    #[tokio::test]
    async fn acquire_hung_node_times_out_to_unreachable() {
        let env = SystemEnv::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (sock, _peer) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(sock);
        });
        // A TIGHT outer guard (runtime timer, allowed by the determinism lint)
        // proves promptness without reading a real clock: the 200ms op timeout
        // returns inside the 2s guard, so `acquire_node` resolves (not hangs).
        let snap = tokio::time::timeout(
            Duration::from_secs(2),
            acquire_node(
                &env,
                &addr,
                None,
                None,
                Duration::from_secs(2),
                Duration::from_millis(200),
            ),
        )
        .await
        .expect("acquire must resolve via the op timeout, not hang past the guard");
        assert!(!snap.reachable);
        assert!(
            snap.error
                .as_deref()
                .unwrap_or_default()
                .contains("timed out")
        );
        server.abort();
    }

    /// Acquiring a healthy stub RESP node yields a reachable snapshot with parsed
    /// INFO and a standalone topology.
    #[tokio::test]
    async fn acquire_healthy_node_is_reachable_with_info() {
        let env = SystemEnv::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut chunk = [0u8; 1024];
            // PING -> +PONG
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(b"+PONG\r\n").await.unwrap();
            // INFO -> bulk body
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            let body = "redis_version:7.0.0\r\ncluster_enabled:0\r\ndb0:keys=3,expires=0\r\n";
            let bulk = format!("${}\r\n{body}\r\n", body.len());
            sock.write_all(bulk.as_bytes()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let snap = acquire_node(
            &env,
            &addr,
            None,
            None,
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .await;
        assert!(snap.reachable, "error: {:?}", snap.error);
        let info = snap.info.as_ref().unwrap();
        assert_eq!(info.redis_version.as_deref(), Some("7.0.0"));
        assert_eq!(info.total_keys, Some(3));
        let topo = single_node_topology(&env, snap);
        assert_eq!(topo.mode, TopologyMode::Standalone);
        assert!(topo.any_reachable());
        server.abort();
    }
}
