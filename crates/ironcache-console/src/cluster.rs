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
//! that adds fields (e.g. a future cross-node replica rollup in `CLUSTER SHARDS`) does not break an
//! older console; the console upgrades its parser when it wants the new fields.

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
    /// The node's replication role plus the per-replica/master fidelity (#365): role, the master's
    /// connected replicas with endpoint + offset + lag, or a replica's master endpoint + link.
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
    /// The slots actively migrating in/out of the polled node (#354). Empty when nothing is
    /// migrating, in standalone, or against an older engine that did not report the array yet.
    #[serde(default)]
    pub migrations: Vec<TopoMigration>,
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

/// The node's replication role plus this node's view of replication (#365): a REPLICA carries its
/// master endpoint + link, a MASTER carries one entry per connected replica with each replica's
/// resolved endpoint + offset + lag. Every field past `role` is OPTIONAL: an older engine (or the
/// standalone `{"role":"master"}` default) simply omits them, and `Option`/`#[serde(default)]` make
/// that a clean parse, not an error (forward-compat via `schema_version`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopoReplication {
    /// `master` or `replica`.
    pub role: String,
    /// REPLICA only: the master's advertised host, when known.
    #[serde(default)]
    pub master_host: Option<String>,
    /// REPLICA only: the master's advertised port, when known.
    #[serde(default)]
    pub master_port: Option<u16>,
    /// REPLICA only: the replication link state (`up` / `down` / `connecting` ...), when reported.
    #[serde(default)]
    pub master_link: Option<String>,
    /// MASTER only: one entry per connected replica (N-replica, #365). Empty for a replica, a
    /// master with no replicas attached, or an older engine that did not report them.
    #[serde(default)]
    pub replicas: Vec<TopoReplica>,
}

/// One connected replica's resolved endpoint + replication progress, as a master reports it (#365).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopoReplica {
    /// The replica's resolved advertised host (empty when the master could not resolve its id yet).
    pub host: String,
    /// The replica's resolved advertised port (`0` when unresolved).
    pub port: u16,
    /// The replica's last acknowledged replication offset.
    pub offset: u64,
    /// The replica's lag behind the master's stream, in offset units.
    pub lag: u64,
}

/// One slot actively migrating in/out of the polled node (#354): the console reads this to detect a
/// resharding in progress and refresh topology faster. `state` is `migrating` (this node is the
/// source, slots leaving) or `importing` (this node is the destination, slots arriving); the `peer_*`
/// fields name the other side of the move and are present only when the engine resolved them.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopoMigration {
    /// The migrating slot (`0..=16383`).
    pub slot: u16,
    /// `migrating` (leaving this node) or `importing` (arriving at this node).
    pub state: String,
    /// The migration peer's 40-hex node id, when resolved.
    #[serde(default)]
    pub peer_id: Option<String>,
    /// The migration peer's advertised host, when resolved.
    #[serde(default)]
    pub peer_host: Option<String>,
    /// The migration peer's advertised port, when resolved.
    #[serde(default)]
    pub peer_port: Option<u16>,
}

impl ClusterTopology {
    /// Whether the polled node reports a slot migration in progress (#354): any `migrating` or
    /// `importing` slot. The poll loop reads this to refresh topology FASTER while slots are moving
    /// (a resharding is the window where a stale slot map lies about ownership), reverting to the
    /// steady cadence once the array drains. O(1): the engine only lists actively-migrating slots, so
    /// the array is empty in steady state and tiny during a resharding.
    #[must_use]
    pub fn migration_in_progress(&self) -> bool {
        !self.cluster.migrations.is_empty()
    }
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
    let topology = parse_cluster_topology(&resp.body_string())?;
    // The central hazard (#368): a split-ownership slot map (one slot under two owners). The
    // engine's committed-epoch fence is supposed to make this impossible, so if it ever appears
    // surface it LOUDLY rather than silently rendering a lie. The view is still returned (do not
    // hide engine state from the operator), but the warning flags the incoherence.
    if !slot_ranges_are_disjoint(&topology.cluster.slots) {
        tracing::warn!(
            url = %url,
            epoch = topology.cluster.committed_epoch,
            "/topology slot ranges OVERLAP (a split-ownership view the epoch fence should prevent); \
             the slot map is not coherent"
        );
    }
    Ok(topology)
}

/// Whether the slot ranges are pairwise DISJOINT, i.e. no slot falls in two ranges (so no slot can
/// have two owners). This is the console-side guard for the epic's central hazard (#368): the engine
/// coalesces same-owner ranges and the committed-epoch fence forbids two owners per slot, so a
/// well-formed `/topology` is always disjoint; a violation means the slot map is incoherent.
/// O(n log n): sort the ranges by start, then check each starts strictly after the previous one ends.
#[must_use]
pub fn slot_ranges_are_disjoint(slots: &[TopoSlotRange]) -> bool {
    let mut ranges: Vec<(u16, u16)> = slots.iter().map(|s| (s.start, s.end)).collect();
    ranges.sort_unstable();
    ranges
        .windows(2)
        // Overlap when the next range starts at or before the previous range ends.
        .all(|w| w[1].0 > w[0].1)
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
        // A still-newer engine adds fields this console does not know yet (a top-level key, a per-node
        // key, a per-replica key, a per-migration key); the parser must ignore them, not error, while
        // still reading the fields it DOES know. Forward-compat via `schema_version`.
        let json = r#"{"schema_version":1,"future_top":42,
            "node":{"id":"x","engine_version":"v","tcp_port":1,"shards":1,"future_node":true},
            "cluster":{"mode":"raft","enabled":true,"committed_epoch":3,"members":[],"slots":[],
                "migrations":[{"slot":7,"state":"importing","future_mig":1}]},
            "raft":null,
            "replication":{"role":"master","future_repl":9,
                "replicas":[{"host":"10.0.0.9","port":7002,"offset":11,"lag":0,"future_rep":true}]}}"#;
        let t = parse_cluster_topology(json).expect("unknown fields tolerated");
        assert_eq!(t.node.id, "x");
        assert_eq!(t.replication.role, "master");
        // The known fields alongside the unknown ones still parse.
        assert_eq!(t.replication.replicas.len(), 1);
        assert_eq!(t.replication.replicas[0].port, 7002);
        assert_eq!(t.cluster.migrations.len(), 1);
        assert_eq!(t.cluster.migrations[0].slot, 7);
    }

    #[test]
    fn parses_per_replica_fidelity_and_active_migrations() {
        // A master mid-resharding: two connected replicas with resolved endpoints + offset/lag, and
        // slot 42 migrating out toward a peer. The console reads both (#354/#365).
        let json = r#"{"schema_version":1,
            "node":{"id":"n1","engine_version":"v","tcp_port":7000,"shards":1},
            "cluster":{"mode":"raft","enabled":true,"committed_epoch":9,
                "members":[{"id":"n1","host":"10.0.0.1","port":7000},
                           {"id":"n2","host":"10.0.0.2","port":7000}],
                "slots":[{"start":0,"end":16383,"owner_id":"n1"}],
                "migrations":[{"slot":42,"state":"migrating","peer_id":"n2",
                    "peer_host":"10.0.0.2","peer_port":7000}]},
            "raft":{"is_leader":true,"leader_id":1,"term":3,"commit_index":42,"voters":3},
            "replication":{"role":"master","replicas":[
                {"host":"10.0.0.2","port":7000,"offset":1000,"lag":0},
                {"host":"10.0.0.3","port":7000,"offset":950,"lag":50}]}}"#;
        let t = parse_cluster_topology(json).expect("valid fidelity doc");
        assert_eq!(t.replication.role, "master");
        assert_eq!(t.replication.replicas.len(), 2);
        assert_eq!(t.replication.replicas[0].host, "10.0.0.2");
        assert_eq!(t.replication.replicas[0].offset, 1000);
        assert_eq!(t.replication.replicas[1].lag, 50);
        assert_eq!(t.cluster.migrations.len(), 1);
        assert_eq!(t.cluster.migrations[0].slot, 42);
        assert_eq!(t.cluster.migrations[0].state, "migrating");
        assert_eq!(t.cluster.migrations[0].peer_id.as_deref(), Some("n2"));
        assert_eq!(t.cluster.migrations[0].peer_port, Some(7000));
        assert!(t.migration_in_progress(), "an active migration is detected");
    }

    #[test]
    fn parses_a_replica_role_with_its_master_endpoint() {
        // A replica reports its master endpoint + link, and lists no replicas of its own.
        let json = r#"{"schema_version":1,
            "node":{"id":"r1","engine_version":"v","tcp_port":7001,"shards":1},
            "cluster":{"mode":"raft","enabled":true,"committed_epoch":9,
                "members":[{"id":"n1","host":"10.0.0.1","port":7000}],
                "slots":[{"start":0,"end":16383,"owner_id":"n1"}]},
            "raft":{"is_leader":false,"leader_id":1,"term":3,"commit_index":42,"voters":3},
            "replication":{"role":"replica","master_host":"10.0.0.1","master_port":7000,
                "master_link":"up"}}"#;
        let t = parse_cluster_topology(json).expect("valid replica doc");
        assert_eq!(t.replication.role, "replica");
        assert_eq!(t.replication.master_host.as_deref(), Some("10.0.0.1"));
        assert_eq!(t.replication.master_port, Some(7000));
        assert_eq!(t.replication.master_link.as_deref(), Some("up"));
        assert!(t.replication.replicas.is_empty());
        // No migrations array present -> defaults empty -> no migration in progress.
        assert!(!t.migration_in_progress());
    }

    #[test]
    fn standalone_has_no_replicas_and_no_migration() {
        // The single-node default: role master, no replicas, no migrations array. The new fields all
        // default cleanly (no parse error), and no migration is reported.
        let t = parse_cluster_topology(STANDALONE).expect("valid standalone doc");
        assert!(t.replication.replicas.is_empty());
        assert!(t.cluster.migrations.is_empty());
        assert!(!t.migration_in_progress());
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

    // ===================== #368: topology-correctness-under-churn =====================
    // A deterministic harness that drives the REAL fetch+parse path against controllable stub
    // `/topology` servers through a churn sequence, asserting the console never adopts an
    // incoherent (split-ownership) slot map and never regresses the committed epoch (the fence).

    fn slot(start: u16, end: u16, owner: &str) -> TopoSlotRange {
        TopoSlotRange {
            start,
            end,
            owner_id: Some(owner.to_owned()),
        }
    }

    #[test]
    fn slot_ranges_disjoint_accepts_coherent_maps_and_rejects_overlap() {
        // Empty + single are trivially disjoint.
        assert!(slot_ranges_are_disjoint(&[]));
        assert!(slot_ranges_are_disjoint(&[slot(0, 16383, "a")]));
        // A coherent two-owner split, in order AND shuffled (the fn sorts first).
        assert!(slot_ranges_are_disjoint(&[
            slot(0, 8191, "a"),
            slot(8192, 16383, "b")
        ]));
        assert!(slot_ranges_are_disjoint(&[
            slot(8192, 16383, "b"),
            slot(0, 8191, "a")
        ]));
        // Slot 8192 falls in BOTH ranges -> a split-ownership view.
        assert!(!slot_ranges_are_disjoint(&[
            slot(0, 8192, "a"),
            slot(8192, 16383, "b")
        ]));
        // A range fully inside another also overlaps.
        assert!(!slot_ranges_are_disjoint(&[
            slot(0, 16383, "a"),
            slot(100, 200, "b")
        ]));
        // Two ranges starting on the same slot overlap.
        assert!(!slot_ranges_are_disjoint(&[
            slot(0, 10, "a"),
            slot(0, 5, "b")
        ]));
    }

    // Churn frames: snapshots the engine might serve mid-migration / mid-failover. All are
    // COHERENT (disjoint slots), with a non-decreasing committed epoch and an evolving leader.
    const FRAME_TWO_NODE_EPOCH7: &str = r#"{"schema_version":1,
        "node":{"id":"aaa","engine_version":"2026.6.29","tcp_port":7000,"shards":4},
        "cluster":{"mode":"raft","enabled":true,"committed_epoch":7,
            "members":[{"id":"aaa","host":"10.0.0.1","port":7000},{"id":"bbb","host":"10.0.0.2","port":7001}],
            "slots":[{"start":0,"end":8191,"owner_id":"aaa"},{"start":8192,"end":16383,"owner_id":"bbb"}]},
        "raft":{"is_leader":true,"leader_id":1,"term":3,"commit_index":42,"voters":3},
        "replication":{"role":"master"}}"#;
    // Epoch bumped 7 -> 8, slots remapped (aaa sheds the 4096..8191 band to bbb), leader 1 -> 2.
    const FRAME_TWO_NODE_EPOCH8_REMAP: &str = r#"{"schema_version":1,
        "node":{"id":"aaa","engine_version":"2026.6.29","tcp_port":7000,"shards":4},
        "cluster":{"mode":"raft","enabled":true,"committed_epoch":8,
            "members":[{"id":"aaa","host":"10.0.0.1","port":7000},{"id":"bbb","host":"10.0.0.2","port":7001}],
            "slots":[{"start":0,"end":4095,"owner_id":"aaa"},{"start":4096,"end":16383,"owner_id":"bbb"}]},
        "raft":{"is_leader":false,"leader_id":2,"term":4,"commit_index":58,"voters":3},
        "replication":{"role":"master"}}"#;

    #[tokio::test]
    async fn churn_sequence_never_yields_a_split_view_or_regressed_epoch() {
        // standalone (epoch 0) -> two-node split (epoch 7, leader 1) -> remap (epoch 8, leader 2).
        let frames: [(&str, &str); 3] = [
            ("standalone", STANDALONE),
            ("split-epoch7", FRAME_TWO_NODE_EPOCH7),
            ("remap-epoch8", FRAME_TWO_NODE_EPOCH8_REMAP),
        ];
        let mut last_epoch = 0u64;
        let mut last_leader: Option<u64> = None;
        for (name, body) in frames {
            let (base, server) = stub_topology_server(200, body).await;
            let t = fetch_cluster_topology(&base, Duration::from_secs(2), Duration::from_secs(2))
                .await
                .unwrap_or_else(|e| panic!("frame {name} should fetch: {e}"));
            server.abort();
            // INVARIANT 1: never a split-ownership view.
            assert!(
                slot_ranges_are_disjoint(&t.cluster.slots),
                "frame {name}: the console adopted a split-ownership slot map"
            );
            // INVARIANT 2: the committed epoch (the fence) never goes backward across the churn.
            assert!(
                t.cluster.committed_epoch >= last_epoch,
                "frame {name}: committed epoch regressed {last_epoch} -> {}",
                t.cluster.committed_epoch
            );
            last_epoch = t.cluster.committed_epoch;
            last_leader = t.raft.as_ref().and_then(|r| r.leader_id).or(last_leader);
        }
        // The sequence really did advance the epoch and move the leader (the churn happened).
        assert_eq!(last_epoch, 8, "the epoch advanced to 8");
        assert_eq!(last_leader, Some(2), "the leader moved to node 2");
    }

    #[tokio::test]
    async fn a_split_ownership_topology_is_returned_but_flagged_incoherent() {
        // A malformed engine answer where slot 8192 has TWO owners. The fetch still returns it
        // (we do not hide engine state), but the coherence guard reports the overlap so the
        // discovery layer can warn. This is the defensive path for the central hazard.
        const SPLIT: &str = r#"{"schema_version":1,
            "node":{"id":"aaa","engine_version":"2026.6.29","tcp_port":7000,"shards":4},
            "cluster":{"mode":"raft","enabled":true,"committed_epoch":9,
                "members":[{"id":"aaa","host":"10.0.0.1","port":7000},{"id":"bbb","host":"10.0.0.2","port":7001}],
                "slots":[{"start":0,"end":8192,"owner_id":"aaa"},{"start":8192,"end":16383,"owner_id":"bbb"}]},
            "raft":null,"replication":{"role":"master"}}"#;
        let (base, server) = stub_topology_server(200, SPLIT).await;
        let t = fetch_cluster_topology(&base, Duration::from_secs(2), Duration::from_secs(2))
            .await
            .expect("a parseable body is returned even when incoherent");
        server.abort();
        assert!(
            !slot_ranges_are_disjoint(&t.cluster.slots),
            "the overlap (slot 8192 owned twice) must be detectable"
        );
    }

    // Slot 4096 is MIGRATING from aaa toward bbb at epoch 7: the COMMITTED slot map is unchanged
    // (aaa still owns the whole [0, 8191] band until the move commits), and the `migrations` array
    // signals the in-flight move. So the view is coherent (disjoint) AND a migration is detectable.
    const FRAME_MIGRATING_EPOCH7: &str = r#"{"schema_version":1,
        "node":{"id":"aaa","engine_version":"2026.6.29","tcp_port":7000,"shards":4},
        "cluster":{"mode":"raft","enabled":true,"committed_epoch":7,
            "members":[{"id":"aaa","host":"10.0.0.1","port":7000},{"id":"bbb","host":"10.0.0.2","port":7001}],
            "slots":[{"start":0,"end":8191,"owner_id":"aaa"},{"start":8192,"end":16383,"owner_id":"bbb"}],
            "migrations":[{"slot":4096,"state":"migrating","peer_id":"bbb","peer_host":"10.0.0.2","peer_port":7001}]},
        "raft":{"is_leader":true,"leader_id":1,"term":3,"commit_index":50,"voters":3},
        "replication":{"role":"master"}}"#;

    #[tokio::test]
    async fn a_slot_migration_is_detected_and_stays_coherent_through_to_commit() {
        // The #368 slot-migration scenario (now expressible via the engine `migrations` array #462 +
        // the console parser #354): stable -> mid-migration -> committed remap. Across the whole
        // sequence the slot map stays coherent and the epoch never regresses; the migration flag
        // tracks false -> true -> false as the move starts and commits.
        let frames: [(&str, &str, bool); 3] = [
            ("stable-epoch7", FRAME_TWO_NODE_EPOCH7, false),
            ("migrating-epoch7", FRAME_MIGRATING_EPOCH7, true),
            ("committed-epoch8", FRAME_TWO_NODE_EPOCH8_REMAP, false),
        ];
        let mut last_epoch = 0u64;
        let mut saw_migration = false;
        for (name, body, want_migrating) in frames {
            let (base, server) = stub_topology_server(200, body).await;
            let t = fetch_cluster_topology(&base, Duration::from_secs(2), Duration::from_secs(2))
                .await
                .unwrap_or_else(|e| panic!("frame {name} should fetch: {e}"));
            server.abort();
            // INVARIANT: the view is coherent even MID-migration (the source still owns the slot
            // until the move commits, so there is never a split-ownership window).
            assert!(
                slot_ranges_are_disjoint(&t.cluster.slots),
                "frame {name}: a migration must not produce a split-ownership slot map"
            );
            // INVARIANT: the committed epoch never regresses across the resharding.
            assert!(
                t.cluster.committed_epoch >= last_epoch,
                "frame {name}: committed epoch regressed {last_epoch} -> {}",
                t.cluster.committed_epoch
            );
            last_epoch = t.cluster.committed_epoch;
            // The console detects the in-flight migration exactly when one is reported.
            assert_eq!(
                t.migration_in_progress(),
                want_migrating,
                "frame {name}: migration_in_progress should be {want_migrating}"
            );
            if t.migration_in_progress() {
                saw_migration = true;
                // The migrating slot carries its resolved peer so the console can target the move.
                let m = &t.cluster.migrations[0];
                assert_eq!(m.slot, 4096);
                assert_eq!(m.state, "migrating");
                assert_eq!(m.peer_id.as_deref(), Some("bbb"));
                assert_eq!(m.peer_port, Some(7001));
            }
        }
        // The sequence really did pass THROUGH an in-flight migration and out the other side.
        assert!(
            saw_migration,
            "the sequence exercised an in-flight migration"
        );
        assert_eq!(last_epoch, 8, "the resharding committed at epoch 8");
    }

    #[tokio::test]
    async fn a_down_node_degrades_gracefully_not_fatally() {
        // Bind then DROP the listener so the port has no acceptor (connection refused / times
        // out): discovery must surface a typed Err (a best-effort miss), never panic.
        let addr = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap()
        };
        let base = format!("http://{addr}");
        let result = fetch_cluster_topology(
            &base,
            Duration::from_millis(500),
            Duration::from_millis(500),
        )
        .await;
        assert!(
            result.is_err(),
            "a down node is a best-effort Err, not a panic or a fabricated topology"
        );
    }
}
