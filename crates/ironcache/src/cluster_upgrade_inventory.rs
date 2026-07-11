// SPDX-License-Identifier: MIT OR Apache-2.0
//! The `ironcache upgrade --cluster` TOML INVENTORY + the `--dry-run` PLAN preview (#392 slice 4).
//!
//! The live rolling-upgrade DRIVER ([`crate::cluster_upgrade_driver`]) discovers the dynamic
//! topology (roles / versions / lag / membership) live each tick, but it needs a STATIC actuation
//! map it cannot discover: how to REACH each node over authenticated RESP (`resp_addr` + optional
//! `auth`) and how to ACTUATE its out-of-band binary swap (`ssh_target` + `upgrade_source`), plus
//! which node(s) to CLUSTER-observe from (the discovery `seeds`). This is the "hybrid inventory": the
//! operator supplies that static half in a small TOML file, this module parses + validates it into
//! the driver's [`Inventory`] / [`Vec<ActuationTarget>`], and the driver reads the rest off the wire.
//!
//! ## Inventory format (all values are PLACEHOLDERS)
//!
//! ```toml
//! # Which node(s) to CLUSTER-observe from (the seed discovery order). Each must name a `[[node]]`
//! # id; at least one is required.
//! seeds = ["node-a"]
//!
//! [[node]]
//! id             = "node-a"                 # the announce id (CLUSTER FAILOVER / promotion target)
//! resp_addr      = "10.0.0.1:6379"          # authenticated RESP host:port (INFO / CLUSTER / PAUSE)
//! auth           = "REQUIREPASS"            # optional requirepass (sent only over the RESP socket)
//! ssh_target     = "deploy@node-a.example"  # opaque ssh target (user@host or an ssh alias)
//! upgrade_source = "--to v1.2.3"            # the per-node `ironcache upgrade` source args
//!
//! [[node]]
//! id             = "node-b"
//! resp_addr      = "10.0.0.2:6379"
//! ssh_target     = "deploy@node-b.example"
//! upgrade_source = "--to v1.2.3"
//! ```
//!
//! ## The `--dry-run` seam
//!
//! [`derive_plan`] turns a SINGLE observed [`ClusterView`] into an ordered [`UpgradePlan`] (which
//! replicas roll first, the promotion candidate, the old primary last) WITHOUT taking any action. It
//! is a pure read-only function over the view -- the `--dry-run` preview and its unit test both use
//! it, so the no-action branch is testable without a live cluster. It does NOT touch the driver's act
//! methods, so it changes no driver logic.

use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;

use serde::Deserialize;

use crate::cluster_upgrade::{ClusterView, NodeRole};
use crate::cluster_upgrade_driver::{ActuationTarget, Inventory};

// ---------------------------------------------------------------------------
// The raw TOML shape (serde), then validated into the driver's Inventory
// ---------------------------------------------------------------------------

/// The raw TOML document: the observe `seeds` + the per-node actuation entries. `deny_unknown_fields`
/// so a mistyped key is a clear parse error rather than a silently-ignored setting.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInventory {
    /// The node id(s) to CLUSTER-observe from (seed discovery order). Defaulted so an absent / empty
    /// list produces the clear [`InventoryError::NoSeed`] rather than a serde "missing field" error.
    #[serde(default)]
    seeds: Vec<String>,
    /// The per-node actuation entries (`[[node]]`).
    #[serde(default, rename = "node")]
    nodes: Vec<RawNode>,
}

/// One raw `[[node]]` entry. `deny_unknown_fields` for the same clear-error reason as the document.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNode {
    /// The node's announce id (unique; the inventory key + the CLUSTER FAILOVER / promotion target).
    id: String,
    /// The authenticated RESP `host:port`.
    resp_addr: String,
    /// The optional `requirepass` (sent only over the RESP socket, never argv / logs).
    #[serde(default)]
    auth: Option<String>,
    /// The opaque SSH target for the out-of-band binary swap (`user@host` or an ssh alias).
    ssh_target: String,
    /// The per-node `ironcache upgrade` source args to pass through (e.g. `--to v1.2.3`).
    upgrade_source: String,
}

/// A clustered-upgrade inventory failure (#392): a malformed / invalid actuation map. Surfaced with a
/// clear message so an operator can fix the file before any node is touched (fail closed).
#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    /// The inventory file could not be read.
    #[error("reading the inventory file {path}: {detail}")]
    Read {
        /// The inventory path.
        path: String,
        /// The IO failure detail.
        detail: String,
    },
    /// The TOML did not parse (a syntax error, a missing required field, an unknown key, ...).
    #[error("parsing the inventory TOML: {0}")]
    Malformed(String),
    /// The inventory has no `[[node]]` entries.
    #[error("the inventory has no nodes (add at least one [[node]] entry)")]
    NoNodes,
    /// Two `[[node]]` entries share an id (the id is the actuation key + the membership cross-check).
    #[error("duplicate node id {0:?} (each [[node]] id must be unique)")]
    DuplicateId(String),
    /// A required per-node string field is empty.
    #[error("node {id:?} has an empty {field} (it is required)")]
    EmptyField {
        /// The offending node id (or the entry position when the id itself is empty).
        id: String,
        /// The empty field name.
        field: &'static str,
    },
    /// A node's `resp_addr` is not a well-formed `host:port`.
    #[error(
        "node {id:?} has a malformed resp_addr {addr:?} (expected host:port with a 1-65535 port)"
    )]
    BadAddr {
        /// The offending node id.
        id: String,
        /// The malformed address.
        addr: String,
    },
    /// The `seeds` list is empty (at least one observe seed is required).
    #[error("the inventory has no seeds (add at least one node id to `seeds`)")]
    NoSeed,
    /// A `seeds` entry does not name any declared `[[node]]` id.
    #[error("seed {0:?} is not a declared node id")]
    UnknownSeed(String),
}

/// Parse + validate an inventory TOML document into the driver's [`Inventory`]. The returned inventory
/// is ordered SEEDS-FIRST (declared seed order, then the remaining nodes in declared order), so the
/// driver's "try each node as a discovery seed in order" tries the operator's chosen seeds first.
///
/// Validates (fail closed, clear error): the TOML parses, there is at least one node, ids are unique
/// and non-empty, each `ssh_target` / `upgrade_source` is non-empty, each `resp_addr` is a well-formed
/// `host:port`, there is at least one seed, and every seed names a declared node.
///
/// # Errors
/// An [`InventoryError`] for any of the above.
pub fn parse_inventory(content: &str) -> Result<Inventory, InventoryError> {
    let raw: RawInventory =
        toml::from_str(content).map_err(|e| InventoryError::Malformed(e.to_string()))?;

    if raw.nodes.is_empty() {
        return Err(InventoryError::NoNodes);
    }

    // Per-node field + address validation, and unique-id detection, in declared order.
    let mut seen_ids: BTreeSet<&str> = BTreeSet::new();
    for node in &raw.nodes {
        let id = node.id.trim();
        if id.is_empty() {
            return Err(InventoryError::EmptyField {
                id: node.id.clone(),
                field: "id",
            });
        }
        if !seen_ids.insert(id) {
            return Err(InventoryError::DuplicateId(id.to_owned()));
        }
        if node.ssh_target.trim().is_empty() {
            return Err(InventoryError::EmptyField {
                id: id.to_owned(),
                field: "ssh_target",
            });
        }
        if node.upgrade_source.trim().is_empty() {
            return Err(InventoryError::EmptyField {
                id: id.to_owned(),
                field: "upgrade_source",
            });
        }
        if !is_valid_host_port(node.resp_addr.trim()) {
            return Err(InventoryError::BadAddr {
                id: id.to_owned(),
                addr: node.resp_addr.clone(),
            });
        }
    }

    // Seed validation: at least one, and each names a declared node.
    if raw.seeds.is_empty() {
        return Err(InventoryError::NoSeed);
    }
    for seed in &raw.seeds {
        if !seen_ids.contains(seed.trim()) {
            return Err(InventoryError::UnknownSeed(seed.clone()));
        }
    }

    // Assemble the actuation targets, ordered SEEDS-FIRST (dedup seeds, keep declared order among the
    // rest). The seed order is the driver's discovery-seed try order.
    let mut ordered: Vec<ActuationTarget> = Vec::with_capacity(raw.nodes.len());
    let mut placed: BTreeSet<&str> = BTreeSet::new();
    for seed in &raw.seeds {
        let seed = seed.trim();
        if !placed.insert(seed) {
            continue; // a duplicate seed id: place the node once.
        }
        if let Some(node) = raw.nodes.iter().find(|n| n.id.trim() == seed) {
            ordered.push(to_target(node));
        }
    }
    for node in &raw.nodes {
        if placed.insert(node.id.trim()) {
            ordered.push(to_target(node));
        }
    }

    Ok(Inventory::new(ordered))
}

/// Load + validate an inventory from a TOML file path.
///
/// # Errors
/// [`InventoryError::Read`] if the file cannot be read, else any [`parse_inventory`] error.
pub fn load_inventory(path: &Path) -> Result<Inventory, InventoryError> {
    let content = std::fs::read_to_string(path).map_err(|e| InventoryError::Read {
        path: path.display().to_string(),
        detail: e.to_string(),
    })?;
    parse_inventory(&content)
}

/// Map a validated raw node to the driver's [`ActuationTarget`] (the TOML `ssh_target` becomes the
/// driver's `ssh` field). Trims the addressable fields so a stray whitespace does not defeat the
/// master-side `slaveN` host:port match.
fn to_target(node: &RawNode) -> ActuationTarget {
    ActuationTarget {
        id: node.id.trim().to_owned(),
        resp_addr: node.resp_addr.trim().to_owned(),
        auth: node.auth.clone().filter(|a| !a.is_empty()),
        ssh: node.ssh_target.trim().to_owned(),
        upgrade_source: node.upgrade_source.trim().to_owned(),
    }
}

/// Whether `addr` is a well-formed `host:port` (a non-empty host and a 1-65535 port). The rightmost
/// `:` splits, so a bare `host:port` is handled; an IPv6 literal would need brackets (out of scope
/// for the placeholder inventory, matching the driver's own `host:port` matching).
fn is_valid_host_port(addr: &str) -> bool {
    let Some((host, port)) = addr.rsplit_once(':') else {
        return false;
    };
    if host.is_empty() {
        return false;
    }
    matches!(port.parse::<u16>(), Ok(p) if p != 0)
}

// ---------------------------------------------------------------------------
// The --dry-run plan preview (the pure, unit-testable no-action seam)
// ---------------------------------------------------------------------------

/// One node named in an [`UpgradePlan`] (its id + its currently-observed version).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanNode {
    /// The node's announce id.
    pub id: String,
    /// The node's currently-observed (normalized) version.
    pub version: String,
}

/// A previewed rolling-upgrade plan for ONE shard, derived PURELY from a single observed
/// [`ClusterView`] (the `--dry-run` seam). Nothing here is acted on; it is what the driver WOULD do,
/// in order: upgrade the old-version replicas first (deterministic by id, matching the driver's
/// `upgrade_next_replica` least-id pick), promote an upgraded in-sync candidate, then upgrade the old
/// primary LAST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradePlan {
    /// The version every node is rolled TO.
    pub target_version: String,
    /// The shard is ALREADY fully on the target: a re-run / no-op, so there are no steps.
    pub already_upgraded: bool,
    /// The observed shard master (the node that is PROMOTED AWAY FROM and upgraded LAST), if one was
    /// observed in this snapshot.
    pub master: Option<PlanNode>,
    /// The replicas to upgrade FIRST, in the driver's deterministic least-id-first order.
    pub replica_upgrades: Vec<PlanNode>,
    /// The current least-lagging upgraded in-sync promotion candidate, if one exists at observe time.
    /// `None` while no replica is yet an upgraded in-sync candidate (the driver re-selects the actual
    /// candidate at promote time, once the replicas are rolled); the preview only reports what is
    /// promotable NOW.
    pub promote_candidate: Option<String>,
}

/// Derive the ordered [`UpgradePlan`] from a single observed [`ClusterView`] (pure, no action). This
/// is the `--dry-run` seam: the CLI prints this instead of running the roll, and the unit test asserts
/// the no-action plan without a live cluster. It only READS the view, so it changes no driver logic.
#[must_use]
pub fn derive_plan(view: &ClusterView) -> UpgradePlan {
    let target_version = view.target_version.clone();
    let already_upgraded = view.shard_fully_upgraded();

    let master = view
        .nodes
        .iter()
        .find(|n| n.role == NodeRole::Master)
        .map(|n| PlanNode {
            id: n.id.clone(),
            version: n.version.clone(),
        });

    // The old-version replicas, deterministic least-id-first (mirrors `upgrade_next_replica`).
    let mut replica_upgrades: Vec<PlanNode> = view
        .nodes
        .iter()
        .filter(|n| n.role == NodeRole::Replica && n.version != target_version)
        .map(|n| PlanNode {
            id: n.id.clone(),
            version: n.version.clone(),
        })
        .collect();
    replica_upgrades.sort_by(|a, b| a.id.cmp(&b.id));

    let promote_candidate = view.select_promote_candidate().map(|n| n.id.clone());

    UpgradePlan {
        target_version,
        already_upgraded,
        master,
        replica_upgrades,
        promote_candidate,
    }
}

impl fmt::Display for UpgradePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "rolling-upgrade plan (dry-run, NO action taken) -> target version {}",
            self.target_version
        )?;
        if self.already_upgraded {
            writeln!(
                f,
                "  every node is already on {}: nothing to do (no-op)",
                self.target_version
            )?;
            return Ok(());
        }
        match &self.master {
            Some(m) => writeln!(f, "  current primary: {} (on {})", m.id, m.version)?,
            None => writeln!(f, "  current primary: (none observed in this snapshot)")?,
        }
        let mut step = 1usize;
        for r in &self.replica_upgrades {
            writeln!(
                f,
                "  {step}. upgrade replica {} (on {} -> {})",
                r.id, r.version, self.target_version
            )?;
            step += 1;
        }
        match &self.promote_candidate {
            Some(c) => writeln!(
                f,
                "  {step}. promote an upgraded in-sync replica (freeze-drain-failover; currently least-lagging candidate: {c})"
            )?,
            None => writeln!(
                f,
                "  {step}. promote an upgraded in-sync replica (freeze-drain-failover; candidate chosen at promote time)"
            )?,
        }
        step += 1;
        match &self.master {
            Some(m) => writeln!(f, "  {step}. upgrade the old primary {} LAST", m.id)?,
            None => writeln!(f, "  {step}. upgrade the old primary LAST")?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_upgrade::NodeView;
    use ironcache_repl::{LinkStatus, ReplOffset, ReplicaLag};

    // ---- inventory parsing (placeholders only) ----

    const VALID: &str = r#"
        seeds = ["node-a"]

        [[node]]
        id             = "node-a"
        resp_addr      = "10.0.0.1:6379"
        auth           = "requirepass-a"
        ssh_target     = "deploy@node-a.example"
        upgrade_source = "--to v1.2.3"

        [[node]]
        id             = "node-b"
        resp_addr      = "10.0.0.2:6379"
        ssh_target     = "deploy@node-b.example"
        upgrade_source = "--to v1.2.3"

        [[node]]
        id             = "node-c"
        resp_addr      = "10.0.0.3:6379"
        ssh_target     = "deploy@node-c.example"
        upgrade_source = "--to v1.2.3"
    "#;

    #[test]
    fn valid_inventory_round_trips_to_the_expected_actuation_targets() {
        let inv = parse_inventory(VALID).expect("valid inventory parses");
        assert_eq!(inv.len(), 3);
        // Seed-first order: node-a is the sole seed, then the rest in declared order.
        let ids: Vec<&str> = inv.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids, ["node-a", "node-b", "node-c"]);

        let a = inv.get("node-a").expect("node-a present");
        assert_eq!(a.resp_addr, "10.0.0.1:6379");
        assert_eq!(a.auth.as_deref(), Some("requirepass-a"));
        assert_eq!(a.ssh, "deploy@node-a.example", "ssh_target maps to ssh");
        assert_eq!(a.upgrade_source, "--to v1.2.3");

        // An omitted `auth` is None (an unauthenticated / open-loopback node).
        let b = inv.get("node-b").expect("node-b present");
        assert!(b.auth.is_none(), "absent auth is None");
    }

    #[test]
    fn seeds_are_placed_first_regardless_of_declaration_order() {
        // node-b is the seed but declared second: it must lead the discovery order.
        let toml = r#"
            seeds = ["node-b"]
            [[node]]
            id = "node-a"
            resp_addr = "10.0.0.1:6379"
            ssh_target = "deploy@node-a.example"
            upgrade_source = "--to v1.2.3"
            [[node]]
            id = "node-b"
            resp_addr = "10.0.0.2:6379"
            ssh_target = "deploy@node-b.example"
            upgrade_source = "--to v1.2.3"
        "#;
        let inv = parse_inventory(toml).expect("parses");
        let ids: Vec<&str> = inv.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids, ["node-b", "node-a"], "the seed leads");
    }

    #[test]
    fn malformed_toml_is_a_clear_error() {
        let err = parse_inventory("this is = not valid = toml").expect_err("malformed");
        assert!(matches!(err, InventoryError::Malformed(_)), "{err}");
    }

    #[test]
    fn an_unknown_key_is_a_malformed_error() {
        // `deny_unknown_fields`: a typo'd key is caught, not silently ignored.
        let toml = r#"
            seeds = ["node-a"]
            [[node]]
            id = "node-a"
            resp_addr = "10.0.0.1:6379"
            ssh_target = "deploy@node-a.example"
            upgrade_source = "--to v1.2.3"
            typo_field = "oops"
        "#;
        assert!(matches!(
            parse_inventory(toml).expect_err("unknown key"),
            InventoryError::Malformed(_)
        ));
    }

    #[test]
    fn empty_node_list_is_rejected() {
        let err = parse_inventory("seeds = [\"node-a\"]\n").expect_err("no nodes");
        assert!(matches!(err, InventoryError::NoNodes), "{err}");
    }

    #[test]
    fn duplicate_ids_are_rejected() {
        let toml = r#"
            seeds = ["node-a"]
            [[node]]
            id = "node-a"
            resp_addr = "10.0.0.1:6379"
            ssh_target = "deploy@node-a.example"
            upgrade_source = "--to v1.2.3"
            [[node]]
            id = "node-a"
            resp_addr = "10.0.0.2:6379"
            ssh_target = "deploy@node-a.example"
            upgrade_source = "--to v1.2.3"
        "#;
        let err = parse_inventory(toml).expect_err("dup id");
        assert!(
            matches!(err, InventoryError::DuplicateId(ref id) if id == "node-a"),
            "{err}"
        );
    }

    #[test]
    fn missing_seed_is_rejected() {
        let toml = r#"
            seeds = []
            [[node]]
            id = "node-a"
            resp_addr = "10.0.0.1:6379"
            ssh_target = "deploy@node-a.example"
            upgrade_source = "--to v1.2.3"
        "#;
        assert!(matches!(
            parse_inventory(toml).expect_err("no seed"),
            InventoryError::NoSeed
        ));
    }

    #[test]
    fn an_unknown_seed_is_rejected() {
        let toml = r#"
            seeds = ["node-z"]
            [[node]]
            id = "node-a"
            resp_addr = "10.0.0.1:6379"
            ssh_target = "deploy@node-a.example"
            upgrade_source = "--to v1.2.3"
        "#;
        let err = parse_inventory(toml).expect_err("unknown seed");
        assert!(
            matches!(err, InventoryError::UnknownSeed(ref s) if s == "node-z"),
            "{err}"
        );
    }

    #[test]
    fn a_bad_addr_is_rejected() {
        for bad in [
            "10.0.0.1",
            "10.0.0.1:",
            "10.0.0.1:0",
            "10.0.0.1:99999",
            ":6379",
        ] {
            let toml = format!(
                r#"
                seeds = ["node-a"]
                [[node]]
                id = "node-a"
                resp_addr = "{bad}"
                ssh_target = "deploy@node-a.example"
                upgrade_source = "--to v1.2.3"
                "#
            );
            let err = parse_inventory(&toml).expect_err("bad addr");
            assert!(
                matches!(err, InventoryError::BadAddr { .. }),
                "addr {bad:?} should be rejected, got {err}"
            );
        }
    }

    #[test]
    fn an_empty_required_field_is_rejected() {
        let toml = r#"
            seeds = ["node-a"]
            [[node]]
            id = "node-a"
            resp_addr = "10.0.0.1:6379"
            ssh_target = ""
            upgrade_source = "--to v1.2.3"
        "#;
        let err = parse_inventory(toml).expect_err("empty ssh_target");
        assert!(
            matches!(
                err,
                InventoryError::EmptyField {
                    field: "ssh_target",
                    ..
                }
            ),
            "{err}"
        );
    }

    // ---- the --dry-run plan preview ----

    const TARGET: &str = "1.2.3";
    const OLD: &str = "1.2.2";

    fn master(id: &str, version: &str) -> NodeView {
        NodeView {
            id: id.to_owned(),
            version: version.to_owned(),
            role: NodeRole::Master,
            link: LinkStatus::Down,
            lag: None,
        }
    }
    fn in_sync_replica(id: &str, version: &str) -> NodeView {
        NodeView {
            id: id.to_owned(),
            version: version.to_owned(),
            role: NodeRole::Replica,
            link: LinkStatus::Up,
            lag: Some(ReplicaLag::compute(ReplOffset(10), ReplOffset(10))), // lag 0
        }
    }
    fn view(nodes: Vec<NodeView>) -> ClusterView {
        ClusterView {
            nodes,
            target_version: TARGET.to_owned(),
            max_lag: 2,
            raft_quorum: true,
            old_primary_id: None,
        }
    }

    #[test]
    fn derive_plan_orders_replicas_first_then_promote_then_old_primary_last() {
        // Pre-roll: p is the old-version master, r2/r1 are old-version replicas (declared out of id
        // order to prove the plan sorts them least-id-first).
        let v = view(vec![
            master("p", OLD),
            in_sync_replica("r2", OLD),
            in_sync_replica("r1", OLD),
        ]);
        let plan = derive_plan(&v);
        assert!(!plan.already_upgraded);
        assert_eq!(plan.master.as_ref().map(|m| m.id.as_str()), Some("p"));
        // Replicas roll first, deterministic by id.
        let roll: Vec<&str> = plan
            .replica_upgrades
            .iter()
            .map(|n| n.id.as_str())
            .collect();
        assert_eq!(roll, ["r1", "r2"], "least-id-first replica roll order");
        // No upgraded in-sync candidate yet (both replicas are still old), so the preview defers.
        assert!(plan.promote_candidate.is_none());
    }

    #[test]
    fn derive_plan_names_the_current_candidate_when_replicas_are_already_upgraded() {
        // Phase-2 posture: replicas upgraded + in sync, master still old -> a candidate is promotable
        // now, and no replica upgrades remain in the plan.
        let v = view(vec![
            master("p", OLD),
            in_sync_replica("r1", TARGET),
            in_sync_replica("r2", TARGET),
        ]);
        let plan = derive_plan(&v);
        assert!(
            plan.replica_upgrades.is_empty(),
            "replicas already upgraded"
        );
        assert!(
            plan.promote_candidate.is_some(),
            "an upgraded in-sync candidate is named"
        );
        assert_eq!(plan.master.as_ref().map(|m| m.id.as_str()), Some("p"));
    }

    #[test]
    fn derive_plan_reports_a_fully_upgraded_shard_as_a_noop() {
        let v = view(vec![master("p", TARGET), in_sync_replica("r1", TARGET)]);
        let plan = derive_plan(&v);
        assert!(plan.already_upgraded, "nothing to do");
        assert!(plan.replica_upgrades.is_empty());
        // The rendered preview says so (and takes NO action -- the whole point of the seam).
        assert!(
            plan.to_string().contains("nothing to do"),
            "no-op preview: {plan}"
        );
    }
}
