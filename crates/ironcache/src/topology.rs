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
//! SCOPE (first cut, the primary #365 acceptance): node identity + `cluster_mode`/`enabled` +
//! membership + slot-to-owner + committed epoch + raft state, in BOTH static and raft modes. The
//! per-replica endpoint/offset/lag fidelity (#365 parts 3-4) needs the replication handshake +
//! lag-model changes and is the documented follow-up; the `replication` object here reports the node
//! role only.

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

    // replication: first cut reports the node role only; per-replica endpoint/offset/lag is the
    // documented #365 follow-up (parts 3-4 need the replication handshake + lag-model changes).
    o.push_str(",\"replication\":{\"role\":\"master\"}}");
    o
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
}
