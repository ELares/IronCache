// SPDX-License-Identifier: MIT OR Apache-2.0
//! The structured topology read endpoint (#365): a versioned JSON document the console reads
//! authoritative topology from (node identity, membership, slot-to-owner map, committed epoch, raft
//! leader/term/commit/voters), so it never has to parse human-readable `CLUSTER NODES`/`SHARDS`
//! text. Served read-only at `GET /topology` on the admin HTTP listener, alongside `/metrics`
//! `/livez` `/readyz`.
//!
//! It returns a COHERENT SINGLE-NODE answer in standalone mode (cluster support disabled), NOT an
//! error: the node owns all 16384 slots at epoch 0 with itself as the only member. This is the
//! `cluster_enabled=false` production deployment, where every `CLUSTER` RESP subcommand returns
//! `-ERR cluster support disabled`; the console still gets a real topology here.
//!
//! JSON is hand-rolled (no `serde` dependency, matching this crate's minimal-third-party-dep
//! posture); the shape is small, fixed, and read-only by construction (it only reads the live
//! `SlotMap` / `RaftHandle` snapshots, mutating nothing).
//!
//! SCOPE: node identity + `cluster_mode`/`enabled` + membership + slot-to-owner + committed epoch +
//! raft state, in BOTH static and raft modes, plus the `migrations` array (the slots actively
//! migrating in/out of this node, #354). The `replication` object reports the node role plus this
//! node's view of replication (#365, REPL_FIDELITY.md): a REPLICA carries its master endpoint + link;
//! a MASTER carries one entry per connected replica (N-replica) with each replica's RESOLVED endpoint
//! + offset + lag. The cross-node replica state in CLUSTER SHARDS is the remaining #365 follow-up.

use std::sync::Arc;

use ironcache_server::RaftHandle;

/// The highest cluster slot index (slots are `0..=16383`).
const CLUSTER_SLOTS_MAX: u16 = 16383;

/// The state the `/topology` renderer reads, bundled so it threads through `BootHandles` /
/// `MetricsState` as ONE field. Every member is cheap to clone (`&'static str` + `Arc`).
#[derive(Clone)]
pub struct TopologyHandle {
    /// The stable 40-hex node id (`CLUSTER MYID`), identical across shards.
    pub node_id: &'static str,
    /// Whether the node booted in cluster mode (`cluster-enabled`).
    pub cluster_enabled: bool,
    /// Whether raft governance is active (vs a static cluster map / standalone).
    pub raft_mode: bool,
    /// The advertised RESP port clients dial.
    pub tcp_port: u16,
    /// The shard (thread-per-core) count.
    pub shards: usize,
    /// The slot-ownership map, `Some` only when a cluster topology is configured; `None` is
    /// standalone, where a coherent single-node answer is synthesized.
    pub cluster: Option<Arc<ironcache_cluster::SlotMap>>,
    /// The live node-level replication status cell (`Some` in raft-governance mode). Snapshotted at
    /// render time for the real `replication` object (role + per-replica/master endpoint, #365);
    /// `None` renders the standalone `role:master` default.
    pub repl_status: Option<Arc<ironcache_server::ReplNodeStatus>>,
}

impl TopologyHandle {
    /// A standalone handle (no cluster map): the non-cluster boot and the tests use this. The node
    /// owns the whole keyspace; `/topology` synthesizes the single-node projection.
    #[must_use]
    pub fn standalone(node_id: &'static str, tcp_port: u16, shards: usize) -> Self {
        TopologyHandle {
            node_id,
            cluster_enabled: false,
            raft_mode: false,
            tcp_port,
            shards,
            cluster: None,
            repl_status: None,
        }
    }
}

/// Append `s` to `out` as a JSON string literal (double-quoted, with the mandatory escapes). Node
/// ids are 40-hex and hosts are hostnames/IPs, but escaping is applied unconditionally so a hostile
/// or odd advertised host can never break the document.
fn json_str(out: &mut String, s: &str) {
    use core::fmt::Write as _;
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Render the `/topology` JSON document from the live handles. `raft` is the same handle the
/// `/metrics` gauges read (`Some` only in raft-governance mode). Pure: it reads snapshots and builds
/// a string, mutating nothing.
#[must_use]
pub fn render_topology_json(handle: &TopologyHandle, raft: Option<&RaftHandle>) -> String {
    use core::fmt::Write as _;
    let mut o = String::with_capacity(1024);
    o.push('{');
    let _ = write!(o, "\"schema_version\":1,");

    // node identity.
    o.push_str("\"node\":{\"id\":");
    json_str(&mut o, handle.node_id);
    o.push_str(",\"engine_version\":");
    json_str(&mut o, env!("CARGO_PKG_VERSION"));
    let _ = write!(
        o,
        ",\"tcp_port\":{},\"shards\":{}}},",
        handle.tcp_port, handle.shards
    );

    // cluster: membership + slot-to-owner + committed epoch (or the standalone single-node answer).
    let mode = if handle.raft_mode {
        "raft"
    } else if handle.cluster_enabled {
        "static"
    } else {
        "none"
    };
    o.push_str("\"cluster\":{\"mode\":");
    json_str(&mut o, mode);
    let _ = write!(o, ",\"enabled\":{},", handle.cluster_enabled);
    if let Some(map) = &handle.cluster {
        let _ = write!(o, "\"committed_epoch\":{},", map.current_epoch());
        let nodes = map.nodes();
        o.push_str("\"members\":[");
        for (i, n) in nodes.iter().enumerate() {
            if i > 0 {
                o.push(',');
            }
            o.push_str("{\"id\":");
            json_str(&mut o, &n.id);
            o.push_str(",\"host\":");
            json_str(&mut o, &n.host);
            let _ = write!(o, ",\"port\":{}}}", n.port);
        }
        o.push_str("],\"slots\":[");
        for (j, (start, end, owner_idx)) in map.ranges().into_iter().enumerate() {
            if j > 0 {
                o.push(',');
            }
            let _ = write!(o, "{{\"start\":{start},\"end\":{end},\"owner_id\":");
            match nodes.get(owner_idx) {
                Some(n) => json_str(&mut o, &n.id),
                None => o.push_str("null"),
            }
            o.push('}');
        }
        o.push(']');
    } else {
        // Standalone: the node owns all slots at epoch 0 and is the only member.
        o.push_str("\"committed_epoch\":0,\"members\":[{\"id\":");
        json_str(&mut o, handle.node_id);
        let _ = write!(
            o,
            ",\"host\":\"\",\"port\":{}}}],\"slots\":[",
            handle.tcp_port
        );
        let _ = write!(o, "{{\"start\":0,\"end\":{CLUSTER_SLOTS_MAX},\"owner_id\":");
        json_str(&mut o, handle.node_id);
        o.push_str("}]");
    }
    // migrations (#354): the slots actively migrating IN/OUT of THIS node, so the console can
    // detect a migration in progress and re-poll faster. Empty in standalone / when idle.
    render_migrations(&mut o, handle);
    o.push_str("},");

    // raft: the consensus state (null outside raft-governance mode).
    o.push_str("\"raft\":");
    if let Some(h) = raft {
        let s = h.status();
        let _ = write!(o, "{{\"is_leader\":{},\"leader_id\":", s.is_leader());
        match s.leader_id {
            Some(id) => {
                let _ = write!(o, "{}", id.0);
            }
            None => o.push_str("null"),
        }
        let _ = write!(
            o,
            ",\"term\":{},\"commit_index\":{},\"voters\":{}}}",
            s.current_term,
            s.commit_index,
            h.config().voters.len()
        );
    } else {
        o.push_str("null");
    }

    // replication (#365): the real role, plus the master endpoint/link for a replica or the
    // connected replica's resolved endpoint/offset/lag for a master. Standalone (no status cell)
    // keeps the byte-compatible `{"role":"master"}`.
    o.push_str(",\"replication\":");
    render_replication(&mut o, handle);
    o.push('}'); // close the top-level topology object.
    o
}

/// Render the `,"migrations":[...]` array into the cluster object (#354): one entry per slot that is
/// currently MIGRATING out of or IMPORTING into this node, with its peer id + endpoint, so the
/// console can detect a migration and refresh faster. Empty in standalone / when nothing is
/// migrating. O(slots) on the rare `/topology` read: the per-slot `migration_state` pre-check is a
/// relaxed atomic load (the node lock is taken ONLY for the few actively-migrating slots), so the hot
/// `owns()` path and the default static answer are untouched.
fn render_migrations(o: &mut String, h: &TopologyHandle) {
    use core::fmt::Write as _;
    o.push_str(",\"migrations\":[");
    let Some(map) = h.cluster.as_ref() else {
        o.push(']');
        return;
    };
    let mut first = true;
    for slot in 0..=CLUSTER_SLOTS_MAX {
        let state = match map.migration_state(slot) {
            ironcache_cluster::MigrationState::Migrating => "migrating",
            ironcache_cluster::MigrationState::Importing => "importing",
            ironcache_cluster::MigrationState::None => continue,
        };
        if !first {
            o.push(',');
        }
        first = false;
        let _ = write!(o, "{{\"slot\":{slot},\"state\":\"{state}\"");
        if let Some(id) = map.migration_peer_id(slot) {
            o.push_str(",\"peer_id\":");
            json_str(o, &id);
        }
        if let Some((host, port)) = map.migration_peer_endpoint(slot) {
            o.push_str(",\"peer_host\":");
            json_str(o, &host);
            let _ = write!(o, ",\"peer_port\":{port}");
        }
        o.push('}');
    }
    o.push(']');
}

/// Render the `replication` object (#365). With no status cell (standalone) it is the
/// byte-compatible `{"role":"master"}`. A REPLICA reports its master endpoint + link; a MASTER
/// reports one entry per connected replica (N-replica), each with its resolved endpoint + offset +
/// lag. Pure: reads the status snapshot + the slot map, mutates nothing.
fn render_replication(o: &mut String, h: &TopologyHandle) {
    use core::fmt::Write as _;
    let Some(status) = h.repl_status.as_ref() else {
        o.push_str("{\"role\":\"master\"}");
        return;
    };
    let snap = status.snapshot();
    match snap.role {
        ironcache_repl::ReplRole::Replica => {
            o.push_str("{\"role\":\"replica\"");
            if let Some((host, port)) = &snap.master_endpoint {
                o.push_str(",\"master_host\":");
                json_str(o, host);
                let _ = write!(o, ",\"master_port\":{port}");
            }
            let _ = write!(o, ",\"master_link\":\"{}\"}}", snap.master_link.as_str());
        }
        ironcache_repl::ReplRole::Master => {
            o.push_str("{\"role\":\"master\",\"replicas\":[");
            // One entry per connected replica (#365 N-replica), each with its resolved endpoint +
            // offset + lag.
            for (i, r) in snap.replicas.iter().enumerate() {
                if i > 0 {
                    o.push(',');
                }
                let lag = snap.slave_lag_of(r.acked).lag().unwrap_or(0);
                let (host, port) =
                    resolve_replica_endpoint(h, r.node_id).unwrap_or((String::new(), 0));
                o.push_str("{\"host\":");
                json_str(o, &host);
                let _ = write!(
                    o,
                    ",\"port\":{},\"offset\":{},\"lag\":{}}}",
                    port, r.acked.0, lag
                );
            }
            o.push_str("]}");
        }
    }
}

/// Resolve a connected replica's endpoint from its captured `NodeId` via the slot map (#365): find
/// the member whose announce id derives to the `NodeId` (`node_id_from_announce`, the same reverse
/// lookup `dispatch.rs` uses for INFO). `None` when the id is unset (`0`), there is no cluster, or no
/// member matches. O(members) on the rare `/topology` read.
fn resolve_replica_endpoint(h: &TopologyHandle, slave_id: u64) -> Option<(String, u16)> {
    if slave_id == 0 {
        return None;
    }
    let map = h.cluster.as_ref()?;
    map.nodes().into_iter().find_map(|n| {
        (crate::raft_boot::node_id_from_announce(&n.id).0 == slave_id)
            .then(|| (n.host.to_string(), n.port))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_is_a_coherent_single_node_answer_not_an_error() {
        let h = TopologyHandle::standalone("abc123", 6379, 4);
        let json = render_topology_json(&h, None);
        // Node identity.
        assert!(json.contains("\"id\":\"abc123\""), "{json}");
        assert!(json.contains("\"tcp_port\":6379"), "{json}");
        assert!(json.contains("\"shards\":4"), "{json}");
        // Standalone cluster projection: mode none, disabled, epoch 0, self owns ALL slots.
        assert!(json.contains("\"mode\":\"none\""), "{json}");
        assert!(json.contains("\"enabled\":false"), "{json}");
        assert!(json.contains("\"committed_epoch\":0"), "{json}");
        assert!(
            json.contains("\"start\":0,\"end\":16383,\"owner_id\":\"abc123\""),
            "self owns all 16384 slots: {json}"
        );
        // Self is the only member.
        assert!(json.contains("\"members\":[{\"id\":\"abc123\""), "{json}");
        // No cluster support => raft is null, role master.
        assert!(json.contains("\"raft\":null"), "{json}");
        assert!(
            json.contains("\"replication\":{\"role\":\"master\"}"),
            "{json}"
        );
        // It is valid-ish JSON: balanced braces (a cheap structural sanity check).
        assert_eq!(
            json.matches('{').count(),
            json.matches('}').count(),
            "balanced braces: {json}"
        );
    }

    /// #365: a MASTER with a connected replica reports the replica's REAL resolved endpoint +
    /// offset + lag in the `replication.replicas` array (not a placeholder).
    #[test]
    fn topology_replication_resolves_a_connected_replica() {
        let replica_id = "aaaaaaaaaaaaaaaa000000000000000000000000";
        let node_id = crate::raft_boot::node_id_from_announce(replica_id).0;
        let self_id = "1111111111111111111111111111111111111111";
        let map = ironcache_cluster::SlotMap::build(
            vec![
                (
                    ironcache_cluster::NodeEntry {
                        id: self_id.into(),
                        host: "10.0.0.1".into(),
                        port: 7001,
                    },
                    vec![[0, 16383]],
                ),
                (
                    ironcache_cluster::NodeEntry {
                        id: replica_id.into(),
                        host: "10.0.0.5".into(),
                        port: 7005,
                    },
                    vec![],
                ),
            ],
            self_id,
        )
        .unwrap();
        let status = Arc::new(ironcache_server::ReplNodeStatus::new());
        status.set_master_head(ironcache_repl::ReplOffset(200));
        status.set_replica(node_id, ironcache_repl::ReplOffset(190)); // lag 10
        let h = TopologyHandle {
            node_id: self_id,
            cluster_enabled: true,
            raft_mode: false,
            tcp_port: 7001,
            shards: 1,
            cluster: Some(Arc::new(map)),
            repl_status: Some(status),
        };
        let json = render_topology_json(&h, None);
        assert!(
            json.contains(
                "\"replication\":{\"role\":\"master\",\"replicas\":[{\"host\":\"10.0.0.5\",\"port\":7005,\"offset\":190,\"lag\":10}]}"
            ),
            "{json}"
        );
        assert_eq!(
            json.matches('{').count(),
            json.matches('}').count(),
            "{json}"
        );
    }

    /// #365: a REPLICA reports its master endpoint + link in the `replication` object.
    #[test]
    fn topology_replication_reports_the_master_for_a_replica() {
        let status = Arc::new(ironcache_server::ReplNodeStatus::new());
        status.set_replica_attached("10.0.0.9", 6400, ironcache_repl::ReplOffset(50));
        let h = TopologyHandle {
            node_id: "nodeB",
            cluster_enabled: false,
            raft_mode: false,
            tcp_port: 6379,
            shards: 1,
            cluster: None,
            repl_status: Some(status),
        };
        let json = render_topology_json(&h, None);
        assert!(
            json.contains(
                "\"replication\":{\"role\":\"replica\",\"master_host\":\"10.0.0.9\",\"master_port\":6400,\"master_link\":\"up\"}"
            ),
            "{json}"
        );
    }

    /// #365: a MASTER with NO connected replica reports an empty `replicas` array.
    #[test]
    fn topology_replication_master_with_no_replica_is_empty_array() {
        let status = Arc::new(ironcache_server::ReplNodeStatus::new());
        status.set_master_head(ironcache_repl::ReplOffset(5));
        let h = TopologyHandle {
            node_id: "nodeC",
            cluster_enabled: false,
            raft_mode: false,
            tcp_port: 6379,
            shards: 1,
            cluster: None,
            repl_status: Some(status),
        };
        let json = render_topology_json(&h, None);
        assert!(
            json.contains("\"replication\":{\"role\":\"master\",\"replicas\":[]}"),
            "{json}"
        );
    }

    /// #354: a slot actively migrating out of this node appears in the `migrations` array with its
    /// state + peer, so the console can detect the migration. A non-migrating cluster reports `[]`.
    #[test]
    fn topology_reports_actively_migrating_slots() {
        let self_id = "1111111111111111111111111111111111111111";
        let dest_id = "2222222222222222222222222222222222222222";
        let map = ironcache_cluster::SlotMap::build(
            vec![
                (
                    ironcache_cluster::NodeEntry {
                        id: self_id.into(),
                        host: "10.0.0.1".into(),
                        port: 7001,
                    },
                    vec![[0, 16383]],
                ),
                (
                    ironcache_cluster::NodeEntry {
                        id: dest_id.into(),
                        host: "10.0.0.2".into(),
                        port: 7002,
                    },
                    vec![],
                ),
            ],
            self_id,
        )
        .unwrap();
        // Idle: the migrations array is empty.
        let h0 = TopologyHandle {
            node_id: self_id,
            cluster_enabled: true,
            raft_mode: false,
            tcp_port: 7001,
            shards: 1,
            cluster: Some(Arc::new(
                ironcache_cluster::SlotMap::build(
                    vec![(
                        ironcache_cluster::NodeEntry {
                            id: self_id.into(),
                            host: "10.0.0.1".into(),
                            port: 7001,
                        },
                        vec![[0, 16383]],
                    )],
                    self_id,
                )
                .unwrap(),
            )),
            repl_status: None,
        };
        assert!(
            render_topology_json(&h0, None).contains("\"migrations\":[]"),
            "an idle cluster reports no migrations"
        );

        // Mark slot 42 MIGRATING toward dest, then it shows up.
        map.set_migrating(42, dest_id).expect("set_migrating");
        let h = TopologyHandle {
            node_id: self_id,
            cluster_enabled: true,
            raft_mode: false,
            tcp_port: 7001,
            shards: 1,
            cluster: Some(Arc::new(map)),
            repl_status: None,
        };
        let json = render_topology_json(&h, None);
        assert!(
            json.contains("\"migrations\":[{\"slot\":42,\"state\":\"migrating\""),
            "{json}"
        );
        assert!(
            json.contains("\"peer_id\":\"2222222222222222222222222222222222222222\""),
            "{json}"
        );
        assert_eq!(
            json.matches('{').count(),
            json.matches('}').count(),
            "{json}"
        );
    }

    #[test]
    fn json_string_escaping_is_applied() {
        let mut s = String::new();
        json_str(&mut s, "a\"b\\c\nd");
        assert_eq!(s, "\"a\\\"b\\\\c\\nd\"");
    }

    #[test]
    fn schema_version_is_present_and_first() {
        let h = TopologyHandle::standalone("n", 1234, 1);
        let json = render_topology_json(&h, None);
        assert!(json.starts_with("{\"schema_version\":1,"), "{json}");
    }

    #[test]
    fn cluster_mode_renders_members_and_slot_ranges() {
        // A real SlotMap (self owning a claimed range) exercises the cluster-populated path: the
        // member's advertised endpoint and the slot-to-owner range must both render.
        let map = Arc::new(ironcache_cluster::SlotMap::empty_self(
            "nodeA", "10.0.0.1", 7000,
        ));
        map.add_slots(&[0, 1, 2, 3])
            .expect("claim slots 0..=3 for self");
        let h = TopologyHandle {
            node_id: "nodeA",
            cluster_enabled: true,
            raft_mode: false,
            tcp_port: 7000,
            shards: 1,
            cluster: Some(map),
            repl_status: None,
        };
        let json = render_topology_json(&h, None);
        assert!(json.contains("\"mode\":\"static\""), "{json}");
        assert!(json.contains("\"enabled\":true"), "{json}");
        // The member renders its advertised endpoint from the SlotMap, not a placeholder.
        assert!(
            json.contains("\"id\":\"nodeA\",\"host\":\"10.0.0.1\",\"port\":7000"),
            "member endpoint: {json}"
        );
        // The claimed contiguous range 0..=3 is owned by nodeA.
        assert!(
            json.contains("\"start\":0,\"end\":3,\"owner_id\":\"nodeA\""),
            "slot range: {json}"
        );
        assert_eq!(
            json.matches('{').count(),
            json.matches('}').count(),
            "balanced braces: {json}"
        );
    }
}
