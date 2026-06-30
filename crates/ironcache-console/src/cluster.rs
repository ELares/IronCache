// SPDX-License-Identifier: MIT OR Apache-2.0
//! Cluster topology discovery (#354): the console fetches the engine's structured `/topology`
//! endpoint (#365) over HTTP and parses it into a typed model, so it tracks membership /
//! slot-to-owner / committed epoch / raft state WITHOUT parsing human-readable `CLUSTER NODES`/
//! `SHARDS` text (the review for #354 calls for reading the structured API over the text).
//!
//! `/topology` is COHERENT in STANDALONE mode (the engine returns a single-node answer with the node
//! owning all slots at epoch 0, NOT an error), so this works for the default single-node prod
//! deployment too: the console never blocks on a leader/epoch/slot-map that does not exist.
//!
//! A fetch/parse failure is BEST-EFFORT: it leaves the cluster view absent (the node stays reachable
//! via the RESP poll and its INFO sections), never failing the whole poll. The slot ranges bind to
//! the engine's COMMITTED epoch (`committed_epoch`), the IronCache fence against two owners per slot.
//!
//! Unknown JSON fields are TOLERATED (the document carries a `schema_version`), so a newer engine
//! that adds fields, e.g. the per-replica endpoint/lag fidelity that is #365's follow-up, does not
//! break an older console; the console upgrades its parser when it wants the new fields.

use std::time::Duration;

/// The parsed `/topology` document (#365 schema v1): node identity, the cluster view (mode +
/// membership + slot-to-owner + committed epoch), the optional raft state, and the node role.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClusterTopology {
    /// The document schema version (`1`); future-additive fields bump nothing breaking.
    pub schema_version: u32,
    /// The polled node's identity.
    pub node: TopoNode,
    /// The cluster view (membership + slots + epoch), coherent single-node in standalone mode.
    pub cluster: TopoClusterView,
    /// The raft consensus state, `None` outside raft-governance mode.
    pub raft: Option<TopoRaft>,
    /// The node's replication role (first cut: role only; per-replica fidelity is #365's follow-up).
    pub replication: TopoReplication,
}

/// The polled node's identity + engine facts.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopoNode {
    /// The stable 40-hex node id.
    pub id: String,
    /// The engine semantic version string.
    pub engine_version: String,
    /// The advertised RESP port.
    pub tcp_port: u16,
    /// The node's shard (thread-per-core) count.
    pub shards: u64,
}

/// The cluster membership + slot ownership view.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopoClusterView {
    /// `none` (standalone), `static`, or `raft`.
    pub mode: String,
    /// Whether the node booted in cluster mode.
    pub enabled: bool,
    /// The committed config epoch (the fence: never two owners per slot per epoch).
    pub committed_epoch: u64,
    /// The known members (the node itself in standalone).
    pub members: Vec<TopoMember>,
    /// The slot-to-owner ranges; standalone is one `[0, 16383]` owned by the node.
    pub slots: Vec<TopoSlotRange>,
}

/// One cluster member's advertised endpoint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopoMember {
    /// The member's 40-hex node id.
    pub id: String,
    /// The advertised host clients dial.
    pub host: String,
    /// The advertised port clients dial.
    pub port: u16,
}

/// One contiguous slot range and its owning node id (`None` for an unassigned range).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopoSlotRange {
    /// The first slot in the range (inclusive).
    pub start: u16,
    /// The last slot in the range (inclusive).
    pub end: u16,
    /// The owning node id, or `None` when the range is unassigned.
    pub owner_id: Option<String>,
}

/// The raft consensus snapshot (present only in raft-governance mode).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopoRaft {
    /// Whether this node believes it is the leader.
    pub is_leader: bool,
    /// The recognized leader's node id, or `None` while forming / mid-election.
    pub leader_id: Option<u64>,
    /// The current raft term.
    pub term: u64,
    /// The highest committed log index.
    pub commit_index: u64,
    /// The voter-set size.
    pub voters: u64,
}

/// The node's replication role.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopoReplication {
    /// `master` or `replica`.
    pub role: String,
}

/// Parse a `/topology` JSON document into the typed model. A malformed body is a typed `Err`
/// (string), never a panic.
///
/// # Errors
/// Returns the parse error message when the body is not a valid `/topology` document.
pub fn parse_cluster_topology(json: &str) -> Result<ClusterTopology, String> {
    serde_json::from_str(json).map_err(|e| format!("parse /topology: {e}"))
}

/// Fetch + parse the engine's `/topology` from `base_url` (the node's HTTP admin base, e.g.
/// `http://10.0.0.1:9100`). Bounded by the connect/read timeouts; a non-2xx status or an unparseable
/// body is a typed `Err`. The outbound fetch goes through the SSRF-screened [`crate::httpclient`].
///
/// # Errors
/// Returns an error string on a connect/timeout/HTTP-status/parse failure (the caller treats it as a
/// best-effort miss, not a node-down condition).
pub async fn fetch_cluster_topology(
    base_url: &str,
    connect_timeout: Duration,
    read_timeout: Duration,
) -> Result<ClusterTopology, String> {
    let url = format!("{}/topology", base_url.trim_end_matches('/'));
    let resp = crate::httpclient::get(&url, connect_timeout, read_timeout)
        .await
        .map_err(|e| e.to_string())?;
    if !(200..300).contains(&resp.status) {
        return Err(format!("/topology returned HTTP {}", resp.status));
    }
    parse_cluster_topology(&resp.body_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const STANDALONE: &str = r#"{"schema_version":1,
        "node":{"id":"abc123","engine_version":"2026.6.29","tcp_port":6379,"shards":4},
        "cluster":{"mode":"none","enabled":false,"committed_epoch":0,
            "members":[{"id":"abc123","host":"","port":6379}],
            "slots":[{"start":0,"end":16383,"owner_id":"abc123"}]},
        "raft":null,"replication":{"role":"master"}}"#;

    #[test]
    fn parses_the_standalone_single_node_answer() {
        let t = parse_cluster_topology(STANDALONE).expect("valid standalone doc");
        assert_eq!(t.schema_version, 1);
        assert_eq!(t.node.id, "abc123");
        assert_eq!(t.node.tcp_port, 6379);
        assert_eq!(t.cluster.mode, "none");
        assert!(!t.cluster.enabled);
        assert_eq!(t.cluster.committed_epoch, 0);
        assert_eq!(t.cluster.members.len(), 1);
        // The single node owns the whole slot space.
        assert_eq!(t.cluster.slots.len(), 1);
        assert_eq!(t.cluster.slots[0].start, 0);
        assert_eq!(t.cluster.slots[0].end, 16383);
        assert_eq!(t.cluster.slots[0].owner_id.as_deref(), Some("abc123"));
        assert!(t.raft.is_none());
        assert_eq!(t.replication.role, "master");
    }

    #[test]
    fn parses_a_raft_clustered_doc_with_multiple_members_and_ranges() {
        let json = r#"{"schema_version":1,
            "node":{"id":"n1","engine_version":"2026.6.29","tcp_port":7000,"shards":1},
            "cluster":{"mode":"raft","enabled":true,"committed_epoch":7,
                "members":[{"id":"n1","host":"10.0.0.1","port":7000},
                           {"id":"n2","host":"10.0.0.2","port":7000}],
                "slots":[{"start":0,"end":8191,"owner_id":"n1"},
                         {"start":8192,"end":16383,"owner_id":"n2"}]},
            "raft":{"is_leader":true,"leader_id":1,"term":3,"commit_index":42,"voters":3},
            "replication":{"role":"master"}}"#;
        let t = parse_cluster_topology(json).expect("valid raft doc");
        assert_eq!(t.cluster.mode, "raft");
        assert_eq!(t.cluster.committed_epoch, 7);
        assert_eq!(t.cluster.members.len(), 2);
        assert_eq!(t.cluster.slots[1].owner_id.as_deref(), Some("n2"));
        let raft = t.raft.expect("raft present");
        assert!(raft.is_leader);
        assert_eq!(raft.leader_id, Some(1));
        assert_eq!(raft.voters, 3);
    }

    #[test]
    fn tolerates_unknown_future_fields() {
        // A newer engine adds a `replication.replicas` array + a top-level field; the older console
        // parser must ignore them (forward-compat via schema_version), not error.
        let json = r#"{"schema_version":1,"future_top":42,
            "node":{"id":"x","engine_version":"v","tcp_port":1,"shards":1,"extra":true},
            "cluster":{"mode":"none","enabled":false,"committed_epoch":0,"members":[],"slots":[]},
            "raft":null,
            "replication":{"role":"master","replicas":[{"id":"r1"}]}}"#;
        let t = parse_cluster_topology(json).expect("unknown fields tolerated");
        assert_eq!(t.node.id, "x");
        assert_eq!(t.replication.role, "master");
    }

    #[test]
    fn an_unassigned_slot_range_has_a_null_owner() {
        let json = r#"{"schema_version":1,
            "node":{"id":"x","engine_version":"v","tcp_port":1,"shards":1},
            "cluster":{"mode":"static","enabled":true,"committed_epoch":0,"members":[],
                "slots":[{"start":0,"end":99,"owner_id":null}]},
            "raft":null,"replication":{"role":"master"}}"#;
        let t = parse_cluster_topology(json).expect("valid");
        assert!(t.cluster.slots[0].owner_id.is_none());
    }

    #[test]
    fn malformed_json_is_a_typed_error_not_a_panic() {
        let err = parse_cluster_topology("{not json").expect_err("malformed errors");
        assert!(err.contains("parse /topology"), "{err}");
    }

    /// Spawn a one-shot stub HTTP server on loopback that replies `status` + `body`, returning its
    /// base URL. Mirrors the httpclient end-to-end test pattern.
    async fn stub_topology_server(
        status: u16,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::AsyncWriteExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut sink = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut sink)
                .await
                .unwrap();
            let reason = if status == 200 { "OK" } else { "Error" };
            let resp = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });
        (format!("http://{addr}"), server)
    }

    #[tokio::test]
    async fn fetch_parses_topology_from_a_stub_server_end_to_end() {
        let (base, server) = stub_topology_server(200, STANDALONE).await;
        let t = fetch_cluster_topology(&base, Duration::from_secs(2), Duration::from_secs(2))
            .await
            .expect("fetch + parse the standalone topology");
        assert_eq!(t.node.id, "abc123");
        assert_eq!(t.cluster.mode, "none");
        assert_eq!(t.cluster.slots[0].end, 16383);
        server.abort();
    }

    #[tokio::test]
    async fn fetch_maps_a_non_2xx_status_to_a_typed_error() {
        let (base, server) = stub_topology_server(503, "service unavailable").await;
        let err = fetch_cluster_topology(&base, Duration::from_secs(2), Duration::from_secs(2))
            .await
            .expect_err("a 503 is an error, not a parse attempt");
        assert!(err.contains("HTTP 503"), "{err}");
        server.abort();
    }
}
