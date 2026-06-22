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
use crate::node::{ClientInfo, NodeAuth, NodeClient, NodeTls, SlowlogEntry};

/// A point-in-time view of ONE node.
///
/// Acquisition is RESILIENT per section: connect + AUTH + PING + INFO together
/// decide `reachable`, but the richer `SLOWLOG` / `CLIENT LIST` sections are
/// fetched best-effort once the node is reachable. A per-section failure (a
/// timeout, a parse fault, or an ACL denial because the console's monitor user
/// lacks the admin command) records that section's `*_error` and leaves its data
/// empty, NEVER flipping `reachable` to false or failing the whole acquire.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
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
    /// The `SLOWLOG GET` entries when the section was fetched; empty otherwise.
    pub slowlog: Vec<SlowlogEntry>,
    /// The error for the slowlog section if it failed (timeout / parse / ACL
    /// denial); `None` when the section succeeded (or the node was unreachable).
    pub slowlog_error: Option<String>,
    /// The `CLIENT LIST` clients when the section was fetched; empty otherwise.
    pub clients: Vec<ClientInfo>,
    /// The error for the client-list section if it failed; `None` on success (or
    /// an unreachable node).
    pub clients_error: Option<String>,
    /// Unix time (seconds) the snapshot was taken, via the env clock seam.
    pub fetched_unixtime: u64,
}

/// The deployment shape the console believes it is monitoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
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

/// How many slowlog entries to request per poll. A generous cap that still bounds
/// the reply; the node truncates to its own configured `slowlog-max-len`.
const SLOWLOG_COUNT: u64 = 128;

/// Connect to `addr`, AUTH (if `auth`), PING, and INFO, folding the result into a
/// [`NodeSnapshot`]. NEVER returns an error: a connect/timeout/auth/protocol
/// failure becomes `reachable = false` with the reason in `error`. Once the node
/// is reachable, the slowlog and client-list sections are fetched best-effort:
/// each is bounded by the same `op_timeout`, and a per-section failure (or an ACL
/// denial of the admin command) records that section's `*_error` and yields an
/// empty list, never failing the whole acquire or flipping `reachable`. The
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
    // Phase 1: connect + PING + INFO decide reachability. A failure here is the
    // whole node being down/unreachable, so no rich sections are attempted.
    let mut client = match NodeClient::connect(addr, tls, auth, connect_timeout, op_timeout).await {
        Ok(c) => c,
        Err(e) => {
            return unreachable_snapshot(addr, &e.to_string(), fetched_unixtime);
        }
    };
    if let Err(e) = client.ping().await {
        return unreachable_snapshot(addr, &e.to_string(), fetched_unixtime);
    }
    let info = match client.info().await {
        Ok(body) => parse_info(&body),
        Err(e) => return unreachable_snapshot(addr, &e.to_string(), fetched_unixtime),
    };

    // Phase 2: RESILIENT rich sections on the SAME reachable connection. Each is
    // independently fault-isolated: its error is recorded, its data left empty,
    // and the node stays `reachable`.
    let (slowlog, slowlog_error) = match client.slowlog(SLOWLOG_COUNT).await {
        Ok(entries) => (entries, None),
        Err(e) => (Vec::new(), Some(e.to_string())),
    };
    let (clients, clients_error) = match client.client_list().await {
        Ok(list) => (list, None),
        Err(e) => (Vec::new(), Some(e.to_string())),
    };

    NodeSnapshot {
        addr: addr.to_owned(),
        reachable: true,
        error: None,
        info: Some(info),
        slowlog,
        slowlog_error,
        clients,
        clients_error,
        fetched_unixtime,
    }
}

/// Build an unreachable [`NodeSnapshot`] with the failure reason. The rich
/// sections are empty and their `*_error` is `None` (the node never answered, so
/// "the slowlog section failed" would be misleading; the single `error` carries
/// the reason the node is unreachable).
fn unreachable_snapshot(addr: &str, error: &str, fetched_unixtime: u64) -> NodeSnapshot {
    NodeSnapshot {
        addr: addr.to_owned(),
        reachable: false,
        error: Some(error.to_owned()),
        info: None,
        slowlog: Vec::new(),
        slowlog_error: None,
        clients: Vec::new(),
        clients_error: None,
        fetched_unixtime,
    }
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
            slowlog: Vec::new(),
            slowlog_error: None,
            clients: Vec::new(),
            clients_error: None,
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
            slowlog: Vec::new(),
            slowlog_error: None,
            clients: Vec::new(),
            clients_error: None,
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
    /// INFO, the slowlog + client-list sections, and a standalone topology. The
    /// stub answers all four poll commands (PING, INFO, SLOWLOG GET, CLIENT LIST).
    #[tokio::test]
    async fn acquire_healthy_node_is_reachable_with_info_and_sections() {
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
            // SLOWLOG GET 128 -> one 6-field entry.
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(
                b"*1\r\n*6\r\n:1\r\n:1700000000\r\n:12000\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n$13\r\n10.0.0.7:5000\r\n$2\r\nw1\r\n",
            )
            .await
            .unwrap();
            // CLIENT LIST -> a one-line bulk body.
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            let clist = "id=7 addr=127.0.0.1:6379 name=console age=1 idle=0 db=0 cmd=client|list\n";
            let cbulk = format!("${}\r\n{clist}\r\n", clist.len());
            sock.write_all(cbulk.as_bytes()).await.unwrap();
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
        // The resilient sections were fetched with no error.
        assert!(snap.slowlog_error.is_none(), "{:?}", snap.slowlog_error);
        assert_eq!(snap.slowlog.len(), 1);
        assert_eq!(snap.slowlog[0].argv, vec!["GET", "foo"]);
        assert!(snap.clients_error.is_none(), "{:?}", snap.clients_error);
        assert_eq!(snap.clients.len(), 1);
        assert_eq!(snap.clients[0].id, Some(7));
        let topo = single_node_topology(&env, snap);
        assert_eq!(topo.mode, TopologyMode::Standalone);
        assert!(topo.any_reachable());
        server.abort();
    }

    /// Section resilience: a node reachable for PING + INFO but that returns a
    /// RESP error to SLOWLOG (an ACL denial of the admin command) stays
    /// `reachable` with its INFO intact; the slowlog section records the error and
    /// its data is empty, and the client-list section is independent.
    #[tokio::test]
    async fn acquire_records_section_error_without_failing_acquire() {
        let env = SystemEnv::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut chunk = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(b"+PONG\r\n").await.unwrap();
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            let body = "redis_version:7.0.0\r\ncluster_enabled:0\r\n";
            let bulk = format!("${}\r\n{body}\r\n", body.len());
            sock.write_all(bulk.as_bytes()).await.unwrap();
            // SLOWLOG GET -> ACL denial (a RESP error).
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(
                b"-NOPERM this user has no permissions to run the 'slowlog' command\r\n",
            )
            .await
            .unwrap();
            // CLIENT LIST -> succeeds (independent of the slowlog failure).
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(b"$0\r\n\r\n").await.unwrap();
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
        // The node is still reachable with INFO; the acquire did not fail.
        assert!(snap.reachable, "error: {:?}", snap.error);
        assert!(snap.info.is_some());
        // The slowlog section recorded its error and yielded no data.
        assert!(snap.slowlog.is_empty());
        let serr = snap.slowlog_error.as_deref().unwrap_or_default();
        assert!(serr.contains("NOPERM"), "{serr}");
        // The client-list section is independent and succeeded (empty body).
        assert!(snap.clients_error.is_none(), "{:?}", snap.clients_error);
        assert!(snap.clients.is_empty());
        server.abort();
    }
}
