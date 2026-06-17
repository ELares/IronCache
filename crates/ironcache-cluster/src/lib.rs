// SPDX-License-Identifier: MIT OR Apache-2.0
//! The static, config-driven 16384-slot ownership map (CLUSTER_CONTRACT.md #70, slice 2).
//!
//! Slice 1 gave IronCache the client-visible `CLUSTER` introspection surface and the pure
//! CRC16/XMODEM slot projection, but every node still behaved as a single-node cluster that
//! auto-owned all 16384 slots. Slice 2 introduces a REAL multi-node topology: a STATIC map,
//! resolved once at boot from config, that assigns each of the 16384 wire slots to exactly
//! one node. This map is what drives MOVED redirection, CROSSSLOT enforcement, and the
//! multi-node `CLUSTER SLOTS / SHARDS / NODES / INFO` projection.
//!
//! ## Why a dedicated leaf crate
//!
//! The slot map has THREE consumers: the router (in `ironcache`), the projection (in
//! `ironcache-server`), and the validation (in `ironcache-config`). Pulling the map into
//! any one of them creates a dependency cycle, because `ironcache-config` must NOT depend
//! on `ironcache-server` (config is a lower layer). A leaf crate depending only on
//! `ironcache-protocol` (for the `CLUSTER_SLOTS` constant) breaks the cycle: config and
//! server both depend on it, and it depends on neither.
//!
//! ## Purity (ADR-0003 / ADR-0002)
//!
//! The map is PURE: [`SlotMap::build`] is a deterministic function of its `(nodes, self_id)`
//! input, and the query methods are deterministic functions of the map. There is no `rand`,
//! no `std::time`, and no lock anywhere in this crate, so the determinism invariant
//! (ADR-0003) and the shared-nothing invariant (ADR-0002) hold trivially. The hot-path
//! ownership query ([`SlotMap::owns`]) is a single O(1) dense-array index with no branch.

/// The number of hash slots in the Redis-Cluster wire space (16384), re-exported from the
/// protocol crate (the single source of the wire constant). A key's slot is its
/// CRC16/XMODEM reduced into this range; this map assigns each of those 16384 slots an owner.
pub const CLUSTER_SLOTS: u16 = ironcache_protocol::CLUSTER_SLOTS;

/// The sentinel `owner[slot]` value for an UNASSIGNED slot (no node owns it). It can only
/// appear when [`SlotMap::fully_assigned`] is `false`; slice-2 validation rejects a gappy
/// map at boot ([`SlotMapError::Gap`]), so a fully-validated map never carries this value.
/// It exists for forward-compatibility (a partial map is structurally representable) and so
/// [`SlotMap::owner`] can return `None` for an unassigned slot rather than index a bogus node.
const UNASSIGNED: u16 = u16::MAX;

/// One node's identity and advertised endpoint, resolved from the cluster topology config.
///
/// The `host`/`port` are what CLIENTS dial (the advertised endpoint a MOVED redirect points
/// at), NOT the node's bind address (which may be `0.0.0.0`). The `id` is the stable
/// 40-lowercase-hex node id, the same value `CLUSTER MYID` reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEntry {
    /// The stable 40-lowercase-hex node id (validated by [`SlotMap::build`]).
    pub id: Box<str>,
    /// The advertised host clients dial (NOT the bind address).
    pub host: Box<str>,
    /// The advertised TCP port clients dial.
    pub port: u16,
}

/// The immutable, boot-resolved slot ownership map: which node owns each of the 16384 slots,
/// and which node is THIS one.
///
/// Two representations are kept, each serving a different consumer:
/// - the dense `owner` array (slot -> node index) is the HOT-PATH structure: [`owns`](Self::owns)
///   and [`owner`](Self::owner) are O(1) array indexes, allocation-free per request;
/// - the `nodes` vector drives the projection ([`ranges`](Self::ranges) coalesces contiguous
///   equal-owner runs into the `(start, end, node)` shape `CLUSTER SLOTS / SHARDS / NODES` need).
#[derive(Debug, Clone)]
pub struct SlotMap {
    /// The nodes, deduplicated by id, in declaration (config) order. Index into this by the
    /// values in `owner`. Order is deterministic (ADR-0003) so the projection is stable.
    nodes: Vec<NodeEntry>,
    /// Dense slot -> node-index map (32 KiB, boxed off-stack). `owner[slot]` is the index
    /// into `nodes`, or [`UNASSIGNED`] for a gap (only possible when `fully_assigned` is false).
    owner: Box<[u16; CLUSTER_SLOTS as usize]>,
    /// The index into `nodes` of THIS node (the one whose announce id was passed to `build`).
    self_idx: usize,
    /// Whether every one of the 16384 slots has an owner. Slice-2 validation requires this.
    fully_assigned: bool,
}

/// Why a topology failed to build into a valid [`SlotMap`]. Mapped onto
/// `ironcache_config::ConfigError::Invalid` by the config crate so a bad topology hard-fails
/// boot with a precise, operator-readable reason rather than a silent fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotMapError {
    /// The topology had no nodes at all.
    Empty,
    /// A node id is not exactly 40 lowercase hex characters. Carries the offending id.
    BadId(String),
    /// Two nodes share the same id. Carries the duplicated id.
    DuplicateId(String),
    /// A slot range was malformed: `start > end`, or `end >= 16384`. Carries `(start, end)`.
    BadRange(u16, u16),
    /// Two nodes claim the same slot. Carries `(slot, first_owner_id, second_owner_id)`.
    Overlap(u16, String, String),
    /// A slot has no owner (the map is not fully assigned). Carries the first unassigned slot.
    /// Slice 2 requires a COMPLETE static map (partial maps need ASK/migration, deferred).
    Gap(u16),
    /// THIS node's announce id is not present in the topology. Carries the announce id.
    SelfNotPresent(String),
}

impl core::fmt::Display for SlotMapError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SlotMapError::Empty => write!(f, "cluster topology has no nodes"),
            SlotMapError::BadId(id) => {
                write!(f, "node id '{id}' is not 40 lowercase hex characters")
            }
            SlotMapError::DuplicateId(id) => write!(f, "duplicate node id '{id}'"),
            SlotMapError::BadRange(start, end) => write!(
                f,
                "invalid slot range [{start}, {end}] (start must be <= end and end < {CLUSTER_SLOTS})"
            ),
            SlotMapError::Overlap(slot, a, b) => {
                write!(f, "slot {slot} is owned by both '{a}' and '{b}'")
            }
            SlotMapError::Gap(slot) => write!(
                f,
                "slot {slot} is unassigned; slice 2 requires a complete static map (every slot owned)"
            ),
            SlotMapError::SelfNotPresent(id) => write!(
                f,
                "this node's announce id '{id}' is not present in cluster_topology"
            ),
        }
    }
}

/// Validate that a node id is exactly 40 lowercase hex characters (the Redis node-id shape).
#[must_use]
fn is_valid_node_id(id: &str) -> bool {
    id.len() == 40
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl SlotMap {
    /// Build and validate a slot map from the resolved topology and THIS node's announce id.
    ///
    /// `nodes` carries each node's id, advertised endpoint, and the inclusive `[start, end]`
    /// slot ranges it owns (as `slot_ranges`, parallel to `nodes`). `self_id` is this node's
    /// 40-hex announce id; it must match exactly one node's id.
    ///
    /// Returns the single source of truth, or a precise [`SlotMapError`] on the first
    /// problem detected (empty / bad id / duplicate id / bad range / overlap / gap /
    /// self-not-present). This is the ONE place the topology is validated; the config crate
    /// calls it (with a throwaway result) for `Config::validate`, and the server calls it
    /// for real at boot.
    ///
    /// # Errors
    ///
    /// See [`SlotMapError`] for the rejection cases.
    pub fn build(
        nodes: Vec<(NodeEntry, Vec<[u16; 2]>)>,
        self_id: &str,
    ) -> Result<SlotMap, SlotMapError> {
        if nodes.is_empty() {
            return Err(SlotMapError::Empty);
        }

        // Dense slot -> node-index map, sentinel-filled. Built on the HEAP via a Vec then
        // converted to a boxed fixed-size array (avoids materializing a 32 KiB array on the
        // stack, which `Box::new([..; N])` would do before the move; clippy::large_stack_arrays).
        let mut owner: Box<[u16; CLUSTER_SLOTS as usize]> =
            vec![UNASSIGNED; CLUSTER_SLOTS as usize]
                .into_boxed_slice()
                .try_into()
                .expect("the vec is exactly CLUSTER_SLOTS long");
        let mut entries: Vec<NodeEntry> = Vec::with_capacity(nodes.len());

        for (idx, (entry, ranges)) in nodes.into_iter().enumerate() {
            // (1) id shape: exactly 40 lowercase hex.
            if !is_valid_node_id(&entry.id) {
                return Err(SlotMapError::BadId(entry.id.into_string()));
            }
            // (2) no duplicate ids.
            if entries.iter().any(|e| e.id == entry.id) {
                return Err(SlotMapError::DuplicateId(entry.id.into_string()));
            }
            // (3) each range well-formed, (4) no overlap (a second writer to a slot).
            for [start, end] in ranges {
                if start > end || end >= CLUSTER_SLOTS {
                    return Err(SlotMapError::BadRange(start, end));
                }
                for slot in start..=end {
                    let cur = owner[slot as usize];
                    if cur != UNASSIGNED {
                        // Already owned by an earlier node: report both ids.
                        let first = entries[cur as usize].id.to_string();
                        return Err(SlotMapError::Overlap(slot, first, entry.id.into_string()));
                    }
                    // `idx` fits u16: MAX nodes is bounded by the 16384 slots, far below u16::MAX.
                    owner[slot as usize] = idx as u16;
                }
            }
            entries.push(entry);
        }

        // (5) full coverage: no gap. Slice 2 requires a complete map (partial -> ASK, deferred),
        // so a gap is a hard error and a successfully-built map is ALWAYS fully assigned. The
        // `fully_assigned` field stays in the struct for the forward-compat partial-map shape
        // and so the projection counters do not assume completeness.
        for (slot, &o) in owner.iter().enumerate() {
            if o == UNASSIGNED {
                return Err(SlotMapError::Gap(slot as u16));
            }
        }

        // (6) self present: the announce id must name one of the nodes.
        let self_idx = entries
            .iter()
            .position(|e| e.id.as_ref() == self_id)
            .ok_or_else(|| SlotMapError::SelfNotPresent(self_id.to_owned()))?;

        Ok(SlotMap {
            nodes: entries,
            owner,
            self_idx,
            fully_assigned: true,
        })
    }

    /// Whether THIS node owns `slot` (the hot path). O(1) dense-array index, no branch.
    ///
    /// # Panics
    ///
    /// Panics if `slot >= 16384` (an out-of-range slot is a caller bug; `key_slot` always
    /// returns a slot in range). Indexing the fixed-size array bounds-checks this.
    #[must_use]
    pub fn owns(&self, slot: u16) -> bool {
        self.owner[slot as usize] == self.self_idx as u16
    }

    /// The node that owns `slot`, or `None` if the slot is unassigned (only possible on a
    /// partial map, which slice-2 validation rejects at boot). O(1).
    #[must_use]
    pub fn owner(&self, slot: u16) -> Option<&NodeEntry> {
        let idx = self.owner[slot as usize];
        if idx == UNASSIGNED {
            None
        } else {
            self.nodes.get(idx as usize)
        }
    }

    /// THIS node's entry (id + advertised endpoint).
    #[must_use]
    pub fn me(&self) -> &NodeEntry {
        &self.nodes[self.self_idx]
    }

    /// All nodes, in declaration (config) order, for the projection.
    #[must_use]
    pub fn nodes(&self) -> &[NodeEntry] {
        &self.nodes
    }

    /// Whether every one of the 16384 slots has an owner (always `true` for a slice-2
    /// validated map, since [`build`](Self::build) rejects a gap).
    #[must_use]
    pub fn is_fully_assigned(&self) -> bool {
        self.fully_assigned
    }

    /// The total number of assigned slots (for `CLUSTER INFO cluster_slots_assigned`).
    #[must_use]
    pub fn slots_assigned(&self) -> u32 {
        self.owner.iter().filter(|&&o| o != UNASSIGNED).count() as u32
    }

    /// The number of known nodes (for `CLUSTER INFO cluster_known_nodes`).
    #[must_use]
    pub fn known_nodes(&self) -> usize {
        self.nodes.len()
    }

    /// The number of nodes serving at least one slot (Redis's `cluster_size`). Equals
    /// [`known_nodes`](Self::known_nodes) for a complete map (every node owns >= 1 slot in a
    /// valid slice-2 topology).
    #[must_use]
    pub fn cluster_size(&self) -> usize {
        // A node serves >= 1 slot iff its index appears in `owner`. Count distinct owners.
        let mut seen = vec![false; self.nodes.len()];
        for &o in self.owner.iter() {
            if o != UNASSIGNED {
                seen[o as usize] = true;
            }
        }
        seen.iter().filter(|&&s| s).count()
    }

    /// Coalesce contiguous runs of equal-owner slots into `(start, end, node_index)` ranges,
    /// ascending by slot. This is the shape `CLUSTER SLOTS / SHARDS / NODES` all need: a node
    /// owning `0-100` and `101-200` (two config ranges) coalesces to one `0-200` range, and
    /// a node owning two NON-contiguous spans yields two ranges. Unassigned slots (partial
    /// map) are skipped, so a gap simply splits the surrounding ranges.
    #[must_use]
    pub fn ranges(&self) -> Vec<(u16, u16, usize)> {
        let mut out = Vec::new();
        let mut run_start: Option<(u16, u16)> = None; // (start_slot, owner_idx)
        for slot in 0..CLUSTER_SLOTS {
            let o = self.owner[slot as usize];
            match run_start {
                Some((_, owner_idx)) if o == owner_idx => {
                    // The run continues; nothing to emit yet.
                }
                Some((start, owner_idx)) => {
                    // The run ended at slot-1; emit it, then start a fresh run (or none).
                    out.push((start, slot - 1, owner_idx as usize));
                    run_start = if o == UNASSIGNED {
                        None
                    } else {
                        Some((slot, o))
                    };
                }
                None => {
                    run_start = if o == UNASSIGNED {
                        None
                    } else {
                        Some((slot, o))
                    };
                }
            }
        }
        // Flush the final open run (the last slot, 16383, never triggers the "ended" arm).
        if let Some((start, owner_idx)) = run_start {
            out.push((start, CLUSTER_SLOTS - 1, owner_idx as usize));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID0: &str = "0000000000000000000000000000000000000000";
    const ID1: &str = "1111111111111111111111111111111111111111";
    const ID2: &str = "2222222222222222222222222222222222222222";

    fn node(id: &str, host: &str, port: u16) -> NodeEntry {
        NodeEntry {
            id: id.into(),
            host: host.into(),
            port,
        }
    }

    /// A canonical 3-node full-coverage topology splitting the slot space in thirds, with
    /// `self_id` = ID1 (the middle node).
    fn three_node() -> SlotMap {
        SlotMap::build(
            vec![
                (node(ID0, "10.0.0.10", 6379), vec![[0, 5460]]),
                (node(ID1, "10.0.0.11", 6379), vec![[5461, 10922]]),
                (node(ID2, "10.0.0.12", 6379), vec![[10923, 16383]]),
            ],
            ID1,
        )
        .expect("a full 3-way split is valid")
    }

    #[test]
    fn owns_and_owner_are_consistent_across_all_slots() {
        let map = three_node();
        let me = map.me().id.clone();
        for slot in 0..CLUSTER_SLOTS {
            let owner = map.owner(slot).expect("full map has no gap");
            // owns(slot) iff the owner is me.
            assert_eq!(
                map.owns(slot),
                owner.id == me,
                "owns/owner disagree at slot {slot}"
            );
        }
        // The middle third belongs to self (ID1, 5461..=10922); the outer thirds do not.
        assert!(!map.owns(0));
        assert!(map.owns(5461));
        assert!(map.owns(10922));
        assert!(!map.owns(10923));
        assert_eq!(map.me().id.as_ref(), ID1);
    }

    #[test]
    fn ranges_coalesce_contiguous_and_are_sorted() {
        // Two config ranges for one node that ABUT (0-100, 101-200) coalesce to one 0-200.
        let map = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 100], [101, 200]]),
                (node(ID1, "h1", 2), vec![[201, 16383]]),
            ],
            ID0,
        )
        .unwrap();
        let ranges = map.ranges();
        assert_eq!(
            ranges,
            vec![(0, 200, 0), (201, 16383, 1)],
            "abutting same-owner ranges coalesce; ascending"
        );
    }

    #[test]
    fn ranges_split_on_owner_change_and_noncontiguous() {
        // ID0 owns 0-10 AND 21-30 (non-contiguous); ID1 owns 11-20 and 31-16383.
        let map = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 10], [21, 30]]),
                (node(ID1, "h1", 2), vec![[11, 20], [31, 16383]]),
            ],
            ID0,
        )
        .unwrap();
        let ranges = map.ranges();
        // Ascending, split at every owner boundary; ID0's two spans stay separate.
        assert_eq!(
            ranges,
            vec![(0, 10, 0), (11, 20, 1), (21, 30, 0), (31, 16383, 1),]
        );
    }

    #[test]
    fn slots_assigned_known_nodes_and_cluster_size() {
        let map = three_node();
        assert_eq!(map.slots_assigned(), u32::from(CLUSTER_SLOTS)); // 16384
        assert_eq!(map.known_nodes(), 3);
        assert_eq!(map.cluster_size(), 3); // every node serves a third
        assert!(map.is_fully_assigned());
    }

    #[test]
    fn reject_empty_topology() {
        assert_eq!(
            SlotMap::build(vec![], ID0).unwrap_err(),
            SlotMapError::Empty
        );
    }

    #[test]
    fn reject_bad_node_id() {
        // Too short.
        assert!(matches!(
            SlotMap::build(vec![(node("abc", "h", 1), vec![[0, 16383]])], "abc"),
            Err(SlotMapError::BadId(_))
        ));
        // Uppercase hex is rejected (Redis ids are lowercase).
        let upper = "A000000000000000000000000000000000000000";
        assert!(matches!(
            SlotMap::build(vec![(node(upper, "h", 1), vec![[0, 16383]])], upper),
            Err(SlotMapError::BadId(_))
        ));
        // Non-hex char.
        let nonhex = "g000000000000000000000000000000000000000";
        assert!(matches!(
            SlotMap::build(vec![(node(nonhex, "h", 1), vec![[0, 16383]])], nonhex),
            Err(SlotMapError::BadId(_))
        ));
    }

    #[test]
    fn reject_duplicate_id() {
        let err = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 8191]]),
                (node(ID0, "h1", 2), vec![[8192, 16383]]),
            ],
            ID0,
        )
        .unwrap_err();
        assert_eq!(err, SlotMapError::DuplicateId(ID0.to_owned()));
    }

    #[test]
    fn reject_bad_range() {
        // start > end.
        assert_eq!(
            SlotMap::build(vec![(node(ID0, "h", 1), vec![[100, 50]])], ID0).unwrap_err(),
            SlotMapError::BadRange(100, 50)
        );
        // end >= 16384.
        assert_eq!(
            SlotMap::build(vec![(node(ID0, "h", 1), vec![[0, 16384]])], ID0).unwrap_err(),
            SlotMapError::BadRange(0, 16384)
        );
    }

    #[test]
    fn reject_overlap() {
        // ID0 owns 0-8191, ID1 owns 8000-16383: slot 8000 is the first overlap.
        let err = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 8191]]),
                (node(ID1, "h1", 2), vec![[8000, 16383]]),
            ],
            ID0,
        )
        .unwrap_err();
        assert_eq!(
            err,
            SlotMapError::Overlap(8000, ID0.to_owned(), ID1.to_owned())
        );
    }

    #[test]
    fn reject_gap() {
        // ID0 owns 0-8190, ID1 owns 8192-16383: slot 8191 is unassigned.
        let err = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 8190]]),
                (node(ID1, "h1", 2), vec![[8192, 16383]]),
            ],
            ID0,
        )
        .unwrap_err();
        assert_eq!(err, SlotMapError::Gap(8191));
    }

    #[test]
    fn reject_self_not_present() {
        let missing = "9999999999999999999999999999999999999999";
        let err = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 8191]]),
                (node(ID1, "h1", 2), vec![[8192, 16383]]),
            ],
            missing,
        )
        .unwrap_err();
        assert_eq!(err, SlotMapError::SelfNotPresent(missing.to_owned()));
    }

    #[test]
    fn single_node_owns_all_slots() {
        // A one-node topology owning the whole space is valid (degenerate but legal).
        let map = SlotMap::build(vec![(node(ID0, "h", 1), vec![[0, 16383]])], ID0).unwrap();
        assert_eq!(map.ranges(), vec![(0, 16383, 0)]);
        assert_eq!(map.known_nodes(), 1);
        assert_eq!(map.cluster_size(), 1);
        for slot in 0..CLUSTER_SLOTS {
            assert!(map.owns(slot));
        }
    }
}
