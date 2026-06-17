// SPDX-License-Identifier: MIT OR Apache-2.0
//! The 16384-slot ownership map (CLUSTER_CONTRACT.md #70). Slice 2 made it a STATIC,
//! config-driven map resolved once at boot; slice 3 makes it RUNTIME-MUTABLE so a node can
//! form a cluster from itself (`CLUSTER MEET / ADDSLOTS / SETSLOT / FORGET / ...`).
//!
//! Slice 1 gave IronCache the client-visible `CLUSTER` introspection surface and the pure
//! CRC16/XMODEM slot projection, but every node still behaved as a single-node cluster that
//! auto-owned all 16384 slots. Slice 2 introduced a REAL multi-node topology: a static map,
//! resolved once at boot from config, assigning each of the 16384 wire slots to exactly one
//! node. This map is what drives MOVED redirection, CROSSSLOT enforcement, and the multi-node
//! `CLUSTER SLOTS / SHARDS / NODES / INFO` projection.
//!
//! ## Slice 3: runtime self-formation (Option A)
//!
//! A cluster-enabled node now boots with an EMPTY single-node map ([`SlotMap::empty_self`])
//! that owns ZERO slots (matching a fresh real-Redis cluster-enabled node), and the operator
//! drives it to a full cluster over the wire: `CLUSTER MEET` grows the node set, `CLUSTER
//! ADDSLOTS / ADDSLOTSRANGE` claims slots, `CLUSTER SETSLOT <slot> NODE <id>` flips ownership,
//! `CLUSTER FORGET` drops a node, and the epoch verbs (`SET-CONFIG-EPOCH` / `BUMPEPOCH`) set
//! the config epoch. Inter-node SYNC of these mutations (gossip / outbound TCP) is DEFERRED to
//! slice 3b; each node mutates only its OWN local view here.
//!
//! ## Why a dedicated leaf crate
//!
//! The slot map has THREE consumers: the router (in `ironcache`), the projection (in
//! `ironcache-server`), and the validation (in `ironcache-config`). Pulling the map into any
//! one of them creates a dependency cycle, because `ironcache-config` must NOT depend on
//! `ironcache-server` (config is a lower layer). A leaf crate depending only on
//! `ironcache-protocol` (for the `CLUSTER_SLOTS` constant) breaks the cycle: config and server
//! both depend on it, and it depends on neither.
//!
//! ## Concurrency model (mirrors `ironcache-config::RuntimeConfig`)
//!
//! Slice 2 was PURE (no rand, no time, no lock). Slice 3 must allow runtime mutation that is
//! visible across every shard's `Arc<SlotMap>` clone, so it adopts the SAME shape
//! `RuntimeConfig` uses: interior atomics for the HOT-PATH read (`owns`) plus a `Mutex` for the
//! rarely-written, cold-read node table:
//!
//! * `mine` is a dense `[AtomicBool; 16384]` SELF-ownership bitmap: the hot ownership query
//!   [`owns`](SlotMap::owns) is a SINGLE lock-free `Acquire` load (`mine[slot]`), allocation-free.
//!   It reads ONLY this one atomic, so it can NEVER observe a torn `(owner[slot], self_idx)` pair
//!   while FORGET renumbers the owner array under the lock. The mutators keep `mine[slot]` in
//!   lockstep with `owner[slot]` at EVERY mutation site (`mine[slot] = (owner[slot] == self_idx)`).
//! * `owner` is a dense `[AtomicU16; 16384]` (slot -> node index). It is the COLD projection / MOVED
//!   structure ONLY: a slot's node index, read while holding the table lock so the index stays
//!   consistent with the `nodes` snapshot during a FORGET renumber. Slot mutators publish with a
//!   `Release` store. `owns()` does NOT read it.
//! * `nodes` (and the parallel `config_epochs`) live behind a `std::sync::Mutex`. They are
//!   mutated only by MEET / FORGET / SETSLOT-bookkeeping and read only on the COLD projection /
//!   MOVED path (`CLUSTER SLOTS / SHARDS / NODES`, and `moved_target`, which now reads `owner[]`
//!   INSIDE the lock). `owns()` NEVER takes the lock. This crate is NOT in the hot-path-crate set
//!   the shared-nothing invariant guards (`scripts/ci/check-rust-invariants.sh`:
//!   storage/store/eviction/expiry/server/observe), so a `Mutex` here is permitted, and it follows
//!   the `RuntimeConfig` precedent (Arc + interior mutability, written rarely, read off the hot
//!   path).
//! * `self_idx`, `my_epoch`, and `current_epoch` are atomics (FORGET can shift indices;
//!   the epochs are scalar counters). ADR-0003 determinism holds: no rand, no `std::time`, no
//!   clock; atomics and `std::sync::Mutex` carry no nondeterminism.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};

/// The number of hash slots in the Redis-Cluster wire space (16384), re-exported from the
/// protocol crate (the single source of the wire constant). A key's slot is its
/// CRC16/XMODEM reduced into this range; this map assigns each of those 16384 slots an owner.
pub const CLUSTER_SLOTS: u16 = ironcache_protocol::CLUSTER_SLOTS;

/// The sentinel `owner[slot]` value for an UNASSIGNED slot (no node owns it). A fresh
/// [`SlotMap::empty_self`] node carries this in every slot until `CLUSTER ADDSLOTS`, and a
/// partial map (mid-formation) carries it in the not-yet-claimed slots. [`SlotMap::owner`]
/// returns `None` for it rather than indexing a bogus node.
const UNASSIGNED: u16 = u16::MAX;

/// One node's identity and advertised endpoint.
///
/// The `host`/`port` are what CLIENTS dial (the advertised endpoint a MOVED redirect points
/// at), NOT the node's bind address (which may be `0.0.0.0`). The `id` is the stable
/// 40-lowercase-hex node id, the same value `CLUSTER MYID` reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEntry {
    /// The stable 40-lowercase-hex node id (validated by [`SlotMap::build`] / [`SlotMap::meet`]).
    pub id: Box<str>,
    /// The advertised host clients dial (NOT the bind address).
    pub host: Box<str>,
    /// The advertised TCP port clients dial.
    pub port: u16,
}

/// The node table and its parallel per-node config epochs, behind one lock so a node's
/// addition / removal (MEET / FORGET) and its epoch are mutated together atomically.
#[derive(Debug, Clone, Default)]
struct NodeTable {
    /// The nodes, deduplicated by id. Index into this by the values in `owner`. The order is
    /// append-on-MEET (config order at boot), so the projection is stable between mutations.
    nodes: Vec<NodeEntry>,
    /// Each node's config epoch, parallel to `nodes` (`config_epochs[i]` is `nodes[i]`'s). Slice
    /// 3 only ever sets THIS node's epoch (SET-CONFIG-EPOCH / BUMPEPOCH); peers stay 0 until
    /// gossip lands them (slice 3b).
    config_epochs: Vec<u64>,
}

/// The runtime-mutable slot ownership map: which node owns each of the 16384 slots, which node
/// is THIS one, and the cluster epochs.
///
/// THREE representations are kept, each serving a different consumer:
/// - the dense `mine` self-ownership bitmap is the HOT-PATH structure: [`owns`](Self::owns) is a
///   SINGLE O(1) lock-free atomic load, allocation-free per request, and reads NOTHING else (so it
///   can never observe a torn `(owner, self_idx)` pair while FORGET renumbers `owner` under lock);
/// - the dense `owner` array (slot -> node index) is the COLD MOVED / projection structure,
///   read only while holding the table lock so the index is consistent with the `nodes` snapshot;
/// - the `table` (nodes + epochs, behind a `Mutex`) drives the COLD projection
///   ([`ranges`](Self::ranges) coalesces contiguous equal-owner runs into the `(start, end,
///   node)` shape `CLUSTER SLOTS / SHARDS / NODES` need) and MOVED ([`moved_target`](Self::moved_target)).
#[derive(Debug)]
pub struct SlotMap {
    /// The node table + epochs (cold path: projection / MOVED / MEET / FORGET). NEVER read by
    /// [`owns`](Self::owns).
    table: Mutex<NodeTable>,
    /// Dense per-slot SELF-ownership bitmap (16 KiB of `AtomicBool`, boxed off-stack). `mine[slot]`
    /// is `true` iff THIS node owns `slot`. The ONLY thing [`owns`](Self::owns) reads, lock-free
    /// (`Acquire`); kept in lockstep with `owner[slot]` at every mutation site (set to
    /// `owner[slot] == self_idx` on each owner store). Because `owns()` reads this one atomic and
    /// nothing else, a concurrent FORGET renumber of `owner`/`self_idx` (under the lock) can NEVER
    /// produce a torn read that mis-homes a slot.
    mine: Box<[AtomicBool; CLUSTER_SLOTS as usize]>,
    /// Dense slot -> node-index map (32 KiB of `AtomicU16`, boxed off-stack). `owner[slot]` is
    /// the index into `table.nodes`, or [`UNASSIGNED`] for a slot no node owns. The COLD MOVED /
    /// projection structure: read while holding the table lock (so the index stays consistent with
    /// the `nodes` snapshot during a FORGET renumber), published by the slot mutators (`Release`).
    /// NOT read by [`owns`](Self::owns).
    owner: Box<[AtomicU16; CLUSTER_SLOTS as usize]>,
    /// The index into `table.nodes` of THIS node. An atomic because FORGET can shift indices.
    /// Used ONLY on the cold projection / MOVED / mutation paths; [`owns`](Self::owns) does not
    /// read it (it reads `mine` instead).
    self_idx: AtomicU16,
    /// THIS node's config epoch (CLUSTER INFO `cluster_my_epoch`). Set by SET-CONFIG-EPOCH /
    /// BUMPEPOCH.
    my_epoch: AtomicU64,
    /// The highest config epoch this node has observed (CLUSTER INFO `cluster_current_epoch`).
    /// Raised by SET-CONFIG-EPOCH / BUMPEPOCH; in slice 3 (no gossip) it tracks `my_epoch`.
    current_epoch: AtomicU64,
}

/// Why a topology failed to build into a valid [`SlotMap`] at boot. Mapped onto
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

/// Why a runtime CLUSTER mutation (ADDSLOTS / DELSLOTS / SETSLOT / FORGET / SET-CONFIG-EPOCH)
/// was rejected. Its [`Display`](core::fmt::Display) is BYTE-EXACT to the message Redis emits
/// from `clusterCommand` (src/cluster_legacy.c), so the server wraps it directly into the
/// `-ERR <message>` reply. The strings were verified against
/// `https://raw.githubusercontent.com/redis/redis/7.4.0/src/cluster_legacy.c`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotMutError {
    /// ADDSLOTS / ADDSLOTSRANGE on a slot already owned by someone.
    /// Redis: `addReplyErrorFormat(c,"Slot %d is already busy", slot)`.
    SlotBusy(u16),
    /// DELSLOTS / DELSLOTSRANGE on a slot that has no owner.
    /// Redis: `addReplyErrorFormat(c,"Slot %d is already unassigned", slot)`.
    SlotUnassigned(u16),
    /// A slot named more than once in a single ADD/DEL batch.
    /// Redis: `addReplyErrorFormat(c,"Slot %d specified multiple times",(int)slot)`.
    SlotDuplicate(u16),
    /// SETSLOT / FORGET naming a node id not in this node's table.
    /// Redis: `addReplyErrorFormat(c,"Unknown node %s", ...)`.
    UnknownNode(String),
    /// FORGET on this node itself.
    /// Redis: `addReplyError(c,"I tried hard but I can't forget myself...")`.
    ForgetSelf,
    /// FORGET on a node that still owns slots. Redis 7.4's FORGET has NO such guard (it
    /// blacklists + deletes any non-self, non-master node); this is an IronCache-specific
    /// safety guard so a slot is never left orphaned (no gossip to re-home it in slice 3).
    /// Documented deviation; carries the node id.
    NodeOwnsSlots(String),
    /// SET-CONFIG-EPOCH when this node already knows other nodes.
    /// Redis: `addReplyError(c,"The user can assign a config epoch only when the node does not know any other node.")`.
    EpochKnowsOthers,
    /// SET-CONFIG-EPOCH when this node's config epoch is already non-zero.
    /// Redis: `addReplyError(c,"Node config epoch is already non-zero")`.
    EpochAlreadySet,
}

impl core::fmt::Display for SlotMutError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SlotMutError::SlotBusy(n) => write!(f, "Slot {n} is already busy"),
            SlotMutError::SlotUnassigned(n) => write!(f, "Slot {n} is already unassigned"),
            SlotMutError::SlotDuplicate(n) => write!(f, "Slot {n} specified multiple times"),
            SlotMutError::UnknownNode(id) => write!(f, "Unknown node {id}"),
            SlotMutError::ForgetSelf => {
                write!(f, "I tried hard but I can't forget myself...")
            }
            // Deviation from Redis 7.4 (which has no such guard); see the variant doc.
            SlotMutError::NodeOwnsSlots(id) => {
                write!(f, "Can't forget node {id} while it still serves slots")
            }
            SlotMutError::EpochKnowsOthers => write!(
                f,
                "The user can assign a config epoch only when the node does not know any other node."
            ),
            SlotMutError::EpochAlreadySet => write!(f, "Node config epoch is already non-zero"),
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

/// Allocate a dense `[AtomicU16; CLUSTER_SLOTS]` on the HEAP filled with `init`, avoiding a
/// 32 KiB stack array (clippy::large_stack_arrays). Built via a `Vec` then converted to a
/// boxed fixed-size array.
fn new_owner_array(init: u16) -> Box<[AtomicU16; CLUSTER_SLOTS as usize]> {
    let mut v: Vec<AtomicU16> = Vec::with_capacity(CLUSTER_SLOTS as usize);
    for _ in 0..CLUSTER_SLOTS {
        v.push(AtomicU16::new(init));
    }
    v.into_boxed_slice()
        .try_into()
        .expect("the vec is exactly CLUSTER_SLOTS long")
}

/// Allocate a dense `[AtomicBool; CLUSTER_SLOTS]` on the HEAP filled with `init` (the per-slot
/// self-ownership bitmap), avoiding a 16 KiB stack array (clippy::large_stack_arrays). Built via a
/// `Vec` then converted to a boxed fixed-size array, mirroring [`new_owner_array`].
fn new_mine_array(init: bool) -> Box<[AtomicBool; CLUSTER_SLOTS as usize]> {
    let mut v: Vec<AtomicBool> = Vec::with_capacity(CLUSTER_SLOTS as usize);
    for _ in 0..CLUSTER_SLOTS {
        v.push(AtomicBool::new(init));
    }
    v.into_boxed_slice()
        .try_into()
        .expect("the vec is exactly CLUSTER_SLOTS long")
}

impl SlotMap {
    /// Build and validate a STATIC slot map from the resolved topology and THIS node's announce
    /// id (the slice-2 boot path, byte-for-byte unchanged in behaviour).
    ///
    /// `nodes` carries each node's id, advertised endpoint, and the inclusive `[start, end]`
    /// slot ranges it owns (as `slot_ranges`, parallel to `nodes`). `self_id` is this node's
    /// 40-hex announce id; it must match exactly one node's id.
    ///
    /// Returns the single source of truth, or a precise [`SlotMapError`] on the first problem
    /// detected (empty / bad id / duplicate id / bad range / overlap / gap / self-not-present).
    /// This is the ONE place a static topology is validated; the config crate calls it (with a
    /// throwaway result) for `Config::validate`, and the server calls it for real at boot.
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

        // Dense slot -> node-index map, sentinel-filled (heap-allocated, see `new_owner_array`).
        let owner = new_owner_array(UNASSIGNED);
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
                    let cur = owner[slot as usize].load(Ordering::Relaxed);
                    if cur != UNASSIGNED {
                        // Already owned by an earlier node: report both ids.
                        let first = entries[cur as usize].id.to_string();
                        return Err(SlotMapError::Overlap(slot, first, entry.id.into_string()));
                    }
                    // `idx` fits u16: MAX nodes is bounded by the 16384 slots, far below u16::MAX.
                    owner[slot as usize].store(idx as u16, Ordering::Relaxed);
                }
            }
            entries.push(entry);
        }

        // (5) full coverage: no gap. Slice 2 requires a complete map (partial -> ASK, deferred),
        // so a gap is a hard error and a successfully-built static map is ALWAYS fully assigned.
        for slot in 0..CLUSTER_SLOTS {
            if owner[slot as usize].load(Ordering::Relaxed) == UNASSIGNED {
                return Err(SlotMapError::Gap(slot));
            }
        }

        // (6) self present: the announce id must name one of the nodes.
        let self_idx = entries
            .iter()
            .position(|e| e.id.as_ref() == self_id)
            .ok_or_else(|| SlotMapError::SelfNotPresent(self_id.to_owned()))?;

        // Initialize the self-ownership bitmap in lockstep with `owner`: `mine[slot]` is true iff
        // `owner[slot]` is THIS node. (At build time there are no concurrent readers, so Relaxed.)
        let self_idx_u16 = self_idx as u16;
        let mine = new_mine_array(false);
        for slot in 0..CLUSTER_SLOTS {
            if owner[slot as usize].load(Ordering::Relaxed) == self_idx_u16 {
                mine[slot as usize].store(true, Ordering::Relaxed);
            }
        }

        let config_epochs = vec![0u64; entries.len()];
        Ok(SlotMap {
            table: Mutex::new(NodeTable {
                nodes: entries,
                config_epochs,
            }),
            mine,
            owner,
            self_idx: AtomicU16::new(self_idx_u16),
            my_epoch: AtomicU64::new(0),
            current_epoch: AtomicU64::new(0),
        })
    }

    /// A fresh single-node map: THIS node alone, owning ZERO slots, epochs 0. The boot state of
    /// a cluster-enabled node with no static topology (slice 3). The operator drives it to a
    /// full cluster with `CLUSTER MEET` / `ADDSLOTS` / ... (matching a fresh real-Redis node,
    /// which owns no slots until `CLUSTER ADDSLOTS`).
    ///
    /// `id` is this node's 40-hex node id; `host`/`port` its advertised endpoint. The id is NOT
    /// re-validated here (the boot path supplies the same id `ServerInfo` reports); a malformed
    /// id simply yields a node whose `CLUSTER MYID` is that id.
    #[must_use]
    pub fn empty_self(id: &str, host: &str, port: u16) -> SlotMap {
        SlotMap {
            table: Mutex::new(NodeTable {
                nodes: vec![NodeEntry {
                    id: id.into(),
                    host: host.into(),
                    port,
                }],
                config_epochs: vec![0u64],
            }),
            // A fresh node owns ZERO slots, so the self-ownership bitmap is all-false.
            mine: new_mine_array(false),
            owner: new_owner_array(UNASSIGNED),
            self_idx: AtomicU16::new(0),
            my_epoch: AtomicU64::new(0),
            current_epoch: AtomicU64::new(0),
        }
    }

    // ----- HOT PATH (lock-free) -----

    /// Whether THIS node owns `slot` (the hot path). A SINGLE lock-free `Acquire` load of the
    /// self-ownership bitmap, allocation-free, NEVER takes the node lock.
    ///
    /// Reading ONLY `mine[slot]` (and not the `(owner[slot], self_idx)` pair) is what makes this
    /// race-free: FORGET renumbers `owner`/`self_idx` under the table lock, but a fresh `self_idx`
    /// can never be paired with a stale `owner[slot]` here, so a node never mistakes a foreign slot
    /// for its own. The mutators keep `mine[slot]` in lockstep with `owner[slot]`.
    ///
    /// # Panics
    ///
    /// Panics if `slot >= 16384` (an out-of-range slot is a caller bug; `key_slot` always
    /// returns a slot in range). Indexing the fixed-size array bounds-checks this.
    #[must_use]
    pub fn owns(&self, slot: u16) -> bool {
        self.mine[slot as usize].load(Ordering::Acquire)
    }

    // ----- COLD PATH (node-lock-taking accessors; owned clones) -----

    /// The advertised `(host, port)` of the node that owns `slot`, or `None` if the slot is
    /// unassigned. Computed under the node lock (cold MOVED path). Returns an OWNED `(String,
    /// u16)` so the caller holds no borrow into the locked table.
    ///
    /// The `owner[slot]` load happens INSIDE the lock: FORGET renumbers `owner[]` and removes from
    /// `nodes` under the SAME lock, so reading the index here while holding the lock guarantees the
    /// index is consistent with the `nodes` snapshot (no torn index-into-a-shifted-vec).
    #[must_use]
    pub fn moved_target(&self, slot: u16) -> Option<(String, u16)> {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = self.owner[slot as usize].load(Ordering::Acquire);
        if idx == UNASSIGNED {
            return None;
        }
        table
            .nodes
            .get(idx as usize)
            .map(|n| (n.host.to_string(), n.port))
    }

    /// THIS node's entry (id + advertised endpoint), cloned out from under the node lock.
    #[must_use]
    pub fn me(&self) -> NodeEntry {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = self.self_idx.load(Ordering::Acquire) as usize;
        table.nodes[idx].clone()
    }

    /// All nodes, in table order, cloned out from under the node lock (the projection).
    #[must_use]
    pub fn nodes(&self) -> Vec<NodeEntry> {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        table.nodes.clone()
    }

    /// The number of known nodes (for `CLUSTER INFO cluster_known_nodes`).
    #[must_use]
    pub fn known_nodes(&self) -> usize {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        table.nodes.len()
    }

    /// Whether every one of the 16384 slots has an owner (`CLUSTER INFO cluster_state:ok`).
    #[must_use]
    pub fn is_fully_assigned(&self) -> bool {
        self.owner
            .iter()
            .all(|o| o.load(Ordering::Acquire) != UNASSIGNED)
    }

    /// The total number of assigned slots (for `CLUSTER INFO cluster_slots_assigned`).
    #[must_use]
    pub fn slots_assigned(&self) -> u32 {
        self.owner
            .iter()
            .filter(|o| o.load(Ordering::Acquire) != UNASSIGNED)
            .count() as u32
    }

    /// The number of nodes serving at least one slot (Redis's `cluster_size`). Computed under
    /// the node lock so it is consistent with the node table.
    #[must_use]
    pub fn cluster_size(&self) -> usize {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let mut seen = vec![false; table.nodes.len()];
        for o in self.owner.iter() {
            let idx = o.load(Ordering::Acquire);
            if idx != UNASSIGNED && (idx as usize) < seen.len() {
                seen[idx as usize] = true;
            }
        }
        seen.iter().filter(|&&s| s).count()
    }

    /// THIS node's config epoch (`CLUSTER INFO cluster_my_epoch`).
    #[must_use]
    pub fn my_epoch(&self) -> u64 {
        self.my_epoch.load(Ordering::Acquire)
    }

    /// The highest config epoch this node has observed (`CLUSTER INFO cluster_current_epoch`).
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Acquire)
    }

    /// Coalesce contiguous runs of equal-owner slots into `(start, end, node_index)` ranges,
    /// ascending by slot. This is the shape `CLUSTER SLOTS / SHARDS / NODES` all need: a node
    /// owning `0-100` and `101-200` coalesces to one `0-200`, and a node owning two
    /// NON-contiguous spans yields two ranges. Unassigned slots are skipped, so a gap simply
    /// splits the surrounding ranges. Reads the owner array atomically; the `node_index` indexes
    /// the [`nodes`](Self::nodes) snapshot taken by the caller (the projection takes both in one
    /// breath, and slice 3 mutates only from the single command path).
    #[must_use]
    pub fn ranges(&self) -> Vec<(u16, u16, usize)> {
        let mut out = Vec::new();
        let mut run_start: Option<(u16, u16)> = None; // (start_slot, owner_idx)
        for slot in 0..CLUSTER_SLOTS {
            let o = self.owner[slot as usize].load(Ordering::Acquire);
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

    // ----- MUTATORS (slice 3; validate-the-whole-batch-first, then mutate; all-or-nothing) -----

    /// `CLUSTER ADDSLOTS / ADDSLOTSRANGE`: claim each slot for THIS node. ALL of `slots` must be
    /// UNASSIGNED and named at most once, else nothing is mutated and the first offending slot is
    /// reported ([`SlotMutError::SlotBusy`] / [`SlotMutError::SlotDuplicate`]).
    ///
    /// # Errors
    ///
    /// [`SlotMutError::SlotBusy`] if a slot is already owned; [`SlotMutError::SlotDuplicate`] if
    /// a slot is named twice in `slots`.
    pub fn add_slots(&self, slots: &[u16]) -> Result<(), SlotMutError> {
        // Validate the WHOLE batch first (all-or-nothing): each slot UNASSIGNED, no duplicate in
        // the request. We hold the node lock for the duration so a concurrent mutator cannot
        // interleave (slice 3 mutates from one command path, but this keeps the batch atomic).
        let _guard = self.table.lock().expect("slot-map node lock poisoned");
        let mut seen = vec![false; CLUSTER_SLOTS as usize];
        for &slot in slots {
            if seen[slot as usize] {
                return Err(SlotMutError::SlotDuplicate(slot));
            }
            seen[slot as usize] = true;
            if self.owner[slot as usize].load(Ordering::Acquire) != UNASSIGNED {
                return Err(SlotMutError::SlotBusy(slot));
            }
        }
        let me = self.self_idx.load(Ordering::Acquire);
        for &slot in slots {
            self.owner[slot as usize].store(me, Ordering::Release);
            // These slots are now THIS node's: set the self-ownership bitmap in lockstep.
            self.mine[slot as usize].store(true, Ordering::Release);
        }
        Ok(())
    }

    /// `CLUSTER DELSLOTS / DELSLOTSRANGE`: release each slot (set it UNASSIGNED). ALL of `slots`
    /// must currently be assigned and named at most once, else nothing is mutated and the first
    /// offending slot is reported.
    ///
    /// # Errors
    ///
    /// [`SlotMutError::SlotUnassigned`] if a slot has no owner; [`SlotMutError::SlotDuplicate`]
    /// if a slot is named twice in `slots`.
    pub fn del_slots(&self, slots: &[u16]) -> Result<(), SlotMutError> {
        let _guard = self.table.lock().expect("slot-map node lock poisoned");
        let mut seen = vec![false; CLUSTER_SLOTS as usize];
        for &slot in slots {
            if seen[slot as usize] {
                return Err(SlotMutError::SlotDuplicate(slot));
            }
            seen[slot as usize] = true;
            if self.owner[slot as usize].load(Ordering::Acquire) == UNASSIGNED {
                return Err(SlotMutError::SlotUnassigned(slot));
            }
        }
        for &slot in slots {
            self.owner[slot as usize].store(UNASSIGNED, Ordering::Release);
            // These slots are now unassigned, hence no longer ours: clear the bitmap in lockstep.
            self.mine[slot as usize].store(false, Ordering::Release);
        }
        Ok(())
    }

    /// `CLUSTER SETSLOT <slot> NODE <node-id>`: flip `slot`'s owner to the named node (the
    /// ownership-transfer form; MIGRATING / IMPORTING / STABLE are slice 4). The node id must be
    /// known.
    ///
    /// # Errors
    ///
    /// [`SlotMutError::UnknownNode`] if `node_id` is not in the node table.
    ///
    /// DOCUMENTED SLICE-3 BOUNDARY: this is a single owner store with NO migration state, so a slot
    /// flipped AWAY from self can briefly still be served locally by an in-flight request that read
    /// `owns()` just before this store (and, symmetrically, a slot flipped TO self may be served a
    /// moment late). This is the inherent SETSLOT-without-migration window; it is NOT a torn read
    /// (each atomic is internally consistent). The durable fix is the slice-4 MIGRATING / IMPORTING
    /// / ASK state machine, which gates serving during the handoff.
    pub fn set_slot_node(&self, slot: u16, node_id: &str) -> Result<(), SlotMutError> {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = table
            .nodes
            .iter()
            .position(|n| n.id.as_ref() == node_id)
            .ok_or_else(|| SlotMutError::UnknownNode(node_id.to_owned()))?;
        let idx_u16 = idx as u16;
        self.owner[slot as usize].store(idx_u16, Ordering::Release);
        // Keep the self-ownership bitmap in lockstep: this slot is ours iff it now points at self.
        let me = self.self_idx.load(Ordering::Acquire);
        self.mine[slot as usize].store(idx_u16 == me, Ordering::Release);
        Ok(())
    }

    /// `CLUSTER FLUSHSLOTS`: release every slot THIS node owns (set them UNASSIGNED). Slots owned
    /// by OTHER nodes are untouched. Always succeeds.
    ///
    /// DOCUMENTED DIVERGENCE from Redis 7.4: Redis errors `DB must be empty to perform CLUSTER
    /// FLUSHSLOTS.` when the DB is non-empty. IronCache has no per-slot / per-DB key-count index
    /// yet (the same gap COUNTKEYSINSLOT documents), so it cannot test DB-emptiness and always
    /// succeeds. The emptiness gate lands with the cross-shard slot index in a later slice.
    pub fn flush_slots(&self) {
        let _guard = self.table.lock().expect("slot-map node lock poisoned");
        let me = self.self_idx.load(Ordering::Acquire);
        for (slot, o) in self.owner.iter().enumerate() {
            if o.load(Ordering::Acquire) == me {
                o.store(UNASSIGNED, Ordering::Release);
                // Released slot is no longer ours: clear the self-ownership bitmap in lockstep.
                self.mine[slot].store(false, Ordering::Release);
            }
        }
    }

    /// `CLUSTER MEET`: add `entry` to the node table. Appends if its id is new; a duplicate id is
    /// idempotent (Ok, no-op), matching Redis (MEET to a known node is harmless). Slice 3 only
    /// records the node locally; the actual handshake / gossip is slice 3b.
    pub fn meet(&self, entry: NodeEntry) {
        let mut table = self.table.lock().expect("slot-map node lock poisoned");
        if table.nodes.iter().any(|n| n.id == entry.id) {
            return; // idempotent: already known.
        }
        table.nodes.push(entry);
        table.config_epochs.push(0);
    }

    /// `CLUSTER FORGET <node-id>`: remove the named node from the table and REINDEX the owner
    /// array (every owner index `> removed` is decremented, and `self_idx` likewise if it was
    /// `> removed`).
    ///
    /// # Errors
    ///
    /// [`SlotMutError::ForgetSelf`] if `node_id` is this node; [`SlotMutError::UnknownNode`] if it
    /// is not in the table; [`SlotMutError::NodeOwnsSlots`] if it still owns at least one slot
    /// (an IronCache safety guard, see the variant doc).
    pub fn forget(&self, node_id: &str) -> Result<(), SlotMutError> {
        let mut table = self.table.lock().expect("slot-map node lock poisoned");
        let removed = table
            .nodes
            .iter()
            .position(|n| n.id.as_ref() == node_id)
            .ok_or_else(|| SlotMutError::UnknownNode(node_id.to_owned()))?;
        let me = self.self_idx.load(Ordering::Acquire) as usize;
        if removed == me {
            return Err(SlotMutError::ForgetSelf);
        }
        // Guard: refuse to orphan slots (no gossip to re-home them in slice 3).
        let removed_u16 = removed as u16;
        if self
            .owner
            .iter()
            .any(|o| o.load(Ordering::Acquire) == removed_u16)
        {
            return Err(SlotMutError::NodeOwnsSlots(node_id.to_owned()));
        }
        // Remove from the table, then reindex the owner array and self_idx: every index strictly
        // above `removed` shifts down by one. UNASSIGNED (u16::MAX) is far above any real index,
        // so it is never `> removed` in the meaningful sense; guard it explicitly anyway.
        //
        // NOTE: `mine[]` is deliberately UNCHANGED here. The guard above proved the removed node
        // owns ZERO slots, so no slot's ownership flips; and `mine[slot]` records "is this slot
        // self's", which is invariant under the owner/self_idx renumber (self's slots stay self's,
        // whatever index self lands on). This is also WHY `owns()` (which reads only `mine`) does
        // not race this renumber: the renumber it would otherwise race no longer feeds `owns()`.
        table.nodes.remove(removed);
        table.config_epochs.remove(removed);
        for o in self.owner.iter() {
            let idx = o.load(Ordering::Acquire);
            if idx != UNASSIGNED && idx > removed_u16 {
                o.store(idx - 1, Ordering::Release);
            }
        }
        if me > removed {
            self.self_idx.store((me - 1) as u16, Ordering::Release);
        }
        Ok(())
    }

    /// `CLUSTER SET-CONFIG-EPOCH <epoch>`: set THIS node's config epoch, allowed ONLY when the
    /// node is totally fresh (it knows no other node AND its config epoch is still 0). Raises
    /// `current_epoch` to `epoch` if higher. Mirrors Redis's cluster-creation rule.
    ///
    /// # Errors
    ///
    /// [`SlotMutError::EpochKnowsOthers`] if the node knows other nodes;
    /// [`SlotMutError::EpochAlreadySet`] if its config epoch is already non-zero.
    pub fn set_config_epoch(&self, epoch: u64) -> Result<(), SlotMutError> {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        if table.nodes.len() > 1 {
            return Err(SlotMutError::EpochKnowsOthers);
        }
        if self.my_epoch.load(Ordering::Acquire) != 0 {
            return Err(SlotMutError::EpochAlreadySet);
        }
        self.my_epoch.store(epoch, Ordering::Release);
        if self.current_epoch.load(Ordering::Acquire) < epoch {
            self.current_epoch.store(epoch, Ordering::Release);
        }
        // Reflect into this node's parallel config-epoch slot.
        let me = self.self_idx.load(Ordering::Acquire) as usize;
        let mut table = table;
        if let Some(e) = table.config_epochs.get_mut(me) {
            *e = epoch;
        }
        Ok(())
    }

    /// `CLUSTER BUMPEPOCH`: conditionally advance the config epoch, mirroring Redis's
    /// `clusterBumpConfigEpochWithoutConsensus`. Computes `maxEpoch = max(current_epoch, max node
    /// config epoch)` and bumps ONLY when `my_epoch == 0 || my_epoch != maxEpoch`: then
    /// `current_epoch = maxEpoch + 1`, `my_epoch = current_epoch`, and it returns
    /// [`BumpEpoch::Bumped`] with the new epoch. Otherwise nothing changes and it returns
    /// [`BumpEpoch::Still`] with the unchanged `my_epoch`. Always succeeds.
    pub fn bump_epoch(&self) -> BumpEpoch {
        let mut table = self.table.lock().expect("slot-map node lock poisoned");
        let my = self.my_epoch.load(Ordering::Acquire);
        // maxEpoch over current_epoch and every known node's config epoch (Redis scans the node
        // dict; we scan the parallel config_epochs vec).
        let max_epoch = table
            .config_epochs
            .iter()
            .copied()
            .fold(self.current_epoch.load(Ordering::Acquire), u64::max);
        // Redis bumps only when this node's epoch is zero or not already the max.
        if my == 0 || my != max_epoch {
            let next = max_epoch + 1;
            self.current_epoch.store(next, Ordering::Release);
            self.my_epoch.store(next, Ordering::Release);
            let me = self.self_idx.load(Ordering::Acquire) as usize;
            if let Some(e) = table.config_epochs.get_mut(me) {
                *e = next;
            }
            BumpEpoch::Bumped(next)
        } else {
            // Already at the max: no change, report STILL with the unchanged epoch.
            BumpEpoch::Still(my)
        }
    }
}

/// The outcome of [`SlotMap::bump_epoch`], mirroring Redis's BUMPEPOCH reply
/// (`+BUMPED <epoch>` on a real bump, `+STILL <epoch>` when the epoch was already the max).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BumpEpoch {
    /// The epoch was advanced; carries the new (and current) `my_epoch`. Reply `+BUMPED <epoch>`.
    Bumped(u64),
    /// No change (the epoch was already the max); carries the unchanged `my_epoch`. Reply
    /// `+STILL <epoch>`.
    Still(u64),
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

    // ----- slice-2 behaviour (preserved byte-for-byte) -----

    #[test]
    fn owns_and_owner_are_consistent_across_all_slots() {
        let map = three_node();
        let me = map.me().id.clone();
        for slot in 0..CLUSTER_SLOTS {
            let owner = map.owner_id(slot).expect("full map has no gap");
            assert_eq!(
                map.owns(slot),
                owner == me,
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
        let map = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 100], [101, 200]]),
                (node(ID1, "h1", 2), vec![[201, 16383]]),
            ],
            ID0,
        )
        .unwrap();
        assert_eq!(
            map.ranges(),
            vec![(0, 200, 0), (201, 16383, 1)],
            "abutting same-owner ranges coalesce; ascending"
        );
    }

    #[test]
    fn ranges_split_on_owner_change_and_noncontiguous() {
        let map = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 10], [21, 30]]),
                (node(ID1, "h1", 2), vec![[11, 20], [31, 16383]]),
            ],
            ID0,
        )
        .unwrap();
        assert_eq!(
            map.ranges(),
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
        assert!(matches!(
            SlotMap::build(vec![(node("abc", "h", 1), vec![[0, 16383]])], "abc"),
            Err(SlotMapError::BadId(_))
        ));
        let upper = "A000000000000000000000000000000000000000";
        assert!(matches!(
            SlotMap::build(vec![(node(upper, "h", 1), vec![[0, 16383]])], upper),
            Err(SlotMapError::BadId(_))
        ));
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
        assert_eq!(
            SlotMap::build(vec![(node(ID0, "h", 1), vec![[100, 50]])], ID0).unwrap_err(),
            SlotMapError::BadRange(100, 50)
        );
        assert_eq!(
            SlotMap::build(vec![(node(ID0, "h", 1), vec![[0, 16384]])], ID0).unwrap_err(),
            SlotMapError::BadRange(0, 16384)
        );
    }

    #[test]
    fn reject_overlap() {
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
        let map = SlotMap::build(vec![(node(ID0, "h", 1), vec![[0, 16383]])], ID0).unwrap();
        assert_eq!(map.ranges(), vec![(0, 16383, 0)]);
        assert_eq!(map.known_nodes(), 1);
        assert_eq!(map.cluster_size(), 1);
        for slot in 0..CLUSTER_SLOTS {
            assert!(map.owns(slot));
        }
    }

    // ----- slice-3: empty_self + mutators -----

    #[test]
    fn empty_self_owns_zero_slots() {
        let map = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        assert_eq!(map.slots_assigned(), 0);
        assert_eq!(map.known_nodes(), 1);
        assert_eq!(map.cluster_size(), 0); // owns no slot -> serves no slot
        assert!(!map.is_fully_assigned());
        assert_eq!(map.me().id.as_ref(), ID0);
        assert_eq!(map.my_epoch(), 0);
        assert_eq!(map.current_epoch(), 0);
        for slot in 0..CLUSTER_SLOTS {
            assert!(!map.owns(slot));
        }
        assert!(map.ranges().is_empty());
    }

    #[test]
    fn add_slots_claims_for_self_then_busy_with_no_partial_mutation() {
        let map = SlotMap::empty_self(ID0, "h", 1);
        map.add_slots(&[0, 1, 2, 3]).unwrap();
        for s in 0..=3 {
            assert!(map.owns(s));
        }
        assert_eq!(map.slots_assigned(), 4);
        // A batch that includes an already-owned slot is rejected with the exact busy error,
        // and NOTHING in the batch is applied (all-or-nothing).
        let err = map.add_slots(&[5, 6, 2]).unwrap_err();
        assert_eq!(err, SlotMutError::SlotBusy(2));
        assert_eq!(err.to_string(), "Slot 2 is already busy");
        assert!(!map.owns(5), "no partial mutation on a rejected batch");
        assert!(!map.owns(6), "no partial mutation on a rejected batch");
        assert_eq!(map.slots_assigned(), 4);
    }

    #[test]
    fn add_slots_rejects_duplicate_in_batch() {
        let map = SlotMap::empty_self(ID0, "h", 1);
        let err = map.add_slots(&[7, 8, 7]).unwrap_err();
        assert_eq!(err, SlotMutError::SlotDuplicate(7));
        assert_eq!(err.to_string(), "Slot 7 specified multiple times");
        assert_eq!(map.slots_assigned(), 0, "no partial mutation");
    }

    #[test]
    fn del_slots_unassigns_self_and_rejects_unassigned_or_duplicate() {
        let map = SlotMap::empty_self(ID0, "h", 1);
        map.add_slots(&[10, 11, 12]).unwrap();
        map.del_slots(&[10, 11]).unwrap();
        assert!(!map.owns(10));
        assert!(!map.owns(11));
        assert!(map.owns(12));
        // Deleting an unassigned slot is the exact error, all-or-nothing.
        let err = map.del_slots(&[12, 99]).unwrap_err();
        assert_eq!(err, SlotMutError::SlotUnassigned(99));
        assert_eq!(err.to_string(), "Slot 99 is already unassigned");
        assert!(map.owns(12), "no partial mutation: 12 stays owned");
        // A duplicate in a DEL batch is rejected too.
        let dup = map.del_slots(&[12, 12]).unwrap_err();
        assert_eq!(dup, SlotMutError::SlotDuplicate(12));
    }

    #[test]
    fn set_slot_node_flips_to_known_node_and_rejects_unknown() {
        let map = SlotMap::empty_self(ID0, "h", 1);
        map.add_slots(&[0]).unwrap();
        map.meet(node(ID1, "h1", 2));
        // Flip slot 0 to ID1: self no longer owns it, the owner_id is ID1.
        map.set_slot_node(0, ID1).unwrap();
        assert!(!map.owns(0));
        assert_eq!(map.owner_id(0).unwrap().as_ref(), ID1);
        // Unknown node id -> the exact error.
        let err = map.set_slot_node(0, ID2).unwrap_err();
        assert_eq!(err, SlotMutError::UnknownNode(ID2.to_owned()));
        assert_eq!(err.to_string(), format!("Unknown node {ID2}"));
    }

    #[test]
    fn flush_slots_releases_only_self_owned() {
        let map = SlotMap::empty_self(ID0, "h", 1);
        map.meet(node(ID1, "h1", 2));
        map.add_slots(&[0, 1, 2]).unwrap();
        map.set_slot_node(2, ID1).unwrap(); // slot 2 now ID1's
        map.flush_slots();
        assert!(!map.owns(0));
        assert!(!map.owns(1));
        // ID1's slot 2 is untouched by self's FLUSHSLOTS.
        assert_eq!(map.owner_id(2).unwrap().as_ref(), ID1);
    }

    #[test]
    fn meet_appends_and_is_idempotent_on_duplicate_id() {
        let map = SlotMap::empty_self(ID0, "h", 1);
        assert_eq!(map.known_nodes(), 1);
        map.meet(node(ID1, "h1", 2));
        assert_eq!(map.known_nodes(), 2);
        // A duplicate id is idempotent: no growth, no error.
        map.meet(node(ID1, "different-host", 999));
        assert_eq!(map.known_nodes(), 2, "duplicate MEET is a no-op");
        let nodes = map.nodes();
        // The original entry is preserved (the dup did not overwrite host/port).
        let id1 = nodes.iter().find(|n| n.id.as_ref() == ID1).unwrap();
        assert_eq!(id1.host.as_ref(), "h1");
        assert_eq!(id1.port, 2);
    }

    #[test]
    fn forget_removes_slotless_node_and_reindexes_owner() {
        // 3-node empty-self map: self=ID0 (idx0), peers ID1 (idx1), ID2 (idx2).
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.meet(node(ID2, "h2", 3));
        // Give ID2 a slot via SETSLOT (so after reindexing we can prove it still resolves).
        map.add_slots(&[5]).unwrap(); // self owns slot 5
        map.set_slot_node(5, ID2).unwrap(); // slot 5 -> ID2 (idx2)
        assert_eq!(map.owner_id(5).unwrap().as_ref(), ID2);
        // FORGET ID1 (idx1, owns no slots): ID2 shifts from idx2 to idx1, and slot 5 must STILL
        // resolve to ID2 after the owner-array reindex.
        map.forget(ID1).unwrap();
        assert_eq!(map.known_nodes(), 2);
        let nodes = map.nodes();
        assert!(nodes.iter().all(|n| n.id.as_ref() != ID1));
        assert_eq!(
            map.owner_id(5).unwrap().as_ref(),
            ID2,
            "ID2's slot survives the reindex"
        );
    }

    #[test]
    fn forget_self_is_rejected() {
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        let err = map.forget(ID0).unwrap_err();
        assert_eq!(err, SlotMutError::ForgetSelf);
        assert_eq!(err.to_string(), "I tried hard but I can't forget myself...");
    }

    #[test]
    fn forget_unknown_node_is_rejected() {
        let map = SlotMap::empty_self(ID0, "h0", 1);
        let err = map.forget(ID1).unwrap_err();
        assert_eq!(err, SlotMutError::UnknownNode(ID1.to_owned()));
        assert_eq!(err.to_string(), format!("Unknown node {ID1}"));
    }

    #[test]
    fn forget_node_owning_slots_is_rejected() {
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.add_slots(&[0]).unwrap();
        map.set_slot_node(0, ID1).unwrap(); // ID1 now owns slot 0
        let err = map.forget(ID1).unwrap_err();
        assert_eq!(err, SlotMutError::NodeOwnsSlots(ID1.to_owned()));
    }

    #[test]
    fn set_config_epoch_only_when_alone_and_unset() {
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.set_config_epoch(7).unwrap();
        assert_eq!(map.my_epoch(), 7);
        assert_eq!(map.current_epoch(), 7);
        // Already non-zero -> rejected.
        let err = map.set_config_epoch(8).unwrap_err();
        assert_eq!(err, SlotMutError::EpochAlreadySet);
        assert_eq!(err.to_string(), "Node config epoch is already non-zero");
        assert_eq!(map.my_epoch(), 7, "unchanged after rejection");

        // Fresh map that knows another node -> rejected with the other error.
        let map2 = SlotMap::empty_self(ID0, "h0", 1);
        map2.meet(node(ID1, "h1", 2));
        let err2 = map2.set_config_epoch(1).unwrap_err();
        assert_eq!(err2, SlotMutError::EpochKnowsOthers);
        assert_eq!(
            err2.to_string(),
            "The user can assign a config epoch only when the node does not know any other node."
        );
        assert_eq!(map2.my_epoch(), 0, "unchanged after rejection");
    }

    #[test]
    fn bump_epoch_bumps_then_holds_still_at_the_max() {
        // Redis's clusterBumpConfigEpochWithoutConsensus: the FIRST bump on a my_epoch==0 node
        // advances to 1; a SECOND immediate bump is a no-op (my_epoch already == maxEpoch == 1),
        // so it replies STILL with the unchanged epoch.
        let map = SlotMap::empty_self(ID0, "h0", 1);
        assert_eq!(map.bump_epoch(), BumpEpoch::Bumped(1));
        assert_eq!(map.current_epoch(), 1);
        assert_eq!(map.my_epoch(), 1);
        // Second consecutive bump: no change, STILL 1 (epoch stays 1).
        assert_eq!(map.bump_epoch(), BumpEpoch::Still(1));
        assert_eq!(
            map.current_epoch(),
            1,
            "STILL leaves current_epoch unchanged"
        );
        assert_eq!(map.my_epoch(), 1, "STILL leaves my_epoch unchanged");
    }

    #[test]
    fn bump_epoch_after_set_config_epoch_is_still() {
        // SET-CONFIG-EPOCH 5 makes my_epoch == maxEpoch == 5, so an immediate BUMPEPOCH is STILL 5.
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.set_config_epoch(5).unwrap();
        assert_eq!(map.bump_epoch(), BumpEpoch::Still(5));
        assert_eq!(map.my_epoch(), 5);
        assert_eq!(map.current_epoch(), 5);
    }

    #[test]
    fn owns_and_ranges_consistent_after_a_mutation_sequence() {
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.add_slots(&[0, 1, 2, 3, 4]).unwrap();
        map.del_slots(&[2]).unwrap();
        // Now self owns 0,1,3,4 (slot 2 is a gap). ranges() must split around the gap.
        assert_eq!(map.ranges(), vec![(0, 1, 0), (3, 4, 0)]);
        for s in [0, 1, 3, 4] {
            assert!(map.owns(s));
        }
        assert!(!map.owns(2));
        assert_eq!(map.slots_assigned(), 4);
    }

    #[test]
    fn determinism_same_sequence_yields_identical_ranges() {
        let drive = || {
            let m = SlotMap::empty_self(ID0, "h0", 1);
            m.meet(node(ID1, "h1", 2));
            m.add_slots(&[0, 1, 2, 3, 4, 5]).unwrap();
            m.set_slot_node(3, ID1).unwrap();
            m.del_slots(&[5]).unwrap();
            m.ranges()
        };
        assert_eq!(
            drive(),
            drive(),
            "the same mutation sequence is deterministic"
        );
    }

    /// THE self-ownership invariant (FINDING 1): after EVERY mutator, `owns(slot)` (which reads only
    /// the `mine` bitmap) must agree with the cold `owner[slot] == self_idx` truth for ALL 16384
    /// slots. If `mine` and `owner`/`self_idx` ever disagreed, a node could mis-home a write; this
    /// drives a full add/del/set/flush/forget sequence and asserts they never diverge.
    #[test]
    fn mine_bitmap_never_disagrees_with_owner_and_self_idx() {
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.meet(node(ID2, "h2", 3));

        // A helper that asserts the invariant across every slot using the cold owner/self_idx truth.
        let check = |m: &SlotMap, ctx: &str| {
            let self_idx = m.self_idx.load(Ordering::Acquire);
            for slot in 0..CLUSTER_SLOTS {
                let owner = m.owner[slot as usize].load(Ordering::Acquire);
                let cold = owner == self_idx; // the pre-fix definition of self-ownership
                assert_eq!(
                    m.owns(slot),
                    cold,
                    "{ctx}: mine/owner disagree at slot {slot} (owner={owner}, self_idx={self_idx})"
                );
            }
        };

        check(&map, "fresh empty_self");
        map.add_slots(&[0, 1, 2, 3, 4, 5]).unwrap();
        check(&map, "after add_slots");
        map.del_slots(&[2, 4]).unwrap();
        check(&map, "after del_slots");
        map.set_slot_node(1, ID1).unwrap(); // flip a self slot AWAY
        check(&map, "after set_slot_node away");
        map.set_slot_node(1, ID0).unwrap(); // flip it BACK to self
        check(&map, "after set_slot_node back");
        map.add_slots(&[100, 101]).unwrap();
        map.set_slot_node(100, ID2).unwrap();
        check(&map, "after mixed adds + set to peer");
        map.flush_slots();
        check(&map, "after flush_slots");
        // FORGET a slotless node (ID1 owns nothing now) and re-check the invariant holds.
        map.forget(ID1).unwrap();
        check(&map, "after forget");
    }

    /// FORGET removes a node that owns ZERO slots (its guard enforces this), so it must NOT alter
    /// any `mine[]` entry: self's owned slots stay self's across the owner/self_idx renumber. This
    /// pins the FINDING-1 reasoning that `owns()` (reading only `mine`) cannot race the renumber.
    #[test]
    fn forget_does_not_alter_the_mine_bitmap() {
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2)); // idx1, slotless
        map.meet(node(ID2, "h2", 3)); // idx2
        map.add_slots(&[0, 1, 2]).unwrap(); // self (idx0) owns 0,1,2
        map.set_slot_node(2, ID2).unwrap(); // slot 2 -> ID2 (idx2); self keeps 0,1

        // Snapshot the mine bitmap before FORGET.
        let before: Vec<bool> = (0..CLUSTER_SLOTS)
            .map(|s| map.mine[s as usize].load(Ordering::Acquire))
            .collect();
        // FORGET ID1 (slotless): ID2 renumbers from idx2 to idx1, self_idx stays 0.
        map.forget(ID1).unwrap();
        let after: Vec<bool> = (0..CLUSTER_SLOTS)
            .map(|s| map.mine[s as usize].load(Ordering::Acquire))
            .collect();
        assert_eq!(before, after, "FORGET must not touch any mine[] entry");
        // And self still owns exactly 0 and 1 (slot 2 is ID2's).
        assert!(map.owns(0) && map.owns(1) && !map.owns(2));
        // ID2's slot still resolves after the renumber (owner-index path stays correct).
        assert_eq!(map.owner_id(2).unwrap().as_ref(), ID2);
    }

    // ----- test-only helper: an owned owner id for a slot (mirrors the removed `owner()` ref) -----

    impl SlotMap {
        /// The id of the node owning `slot`, or `None` if unassigned. Test-only convenience that
        /// reads the owner index then the (locked) node table.
        fn owner_id(&self, slot: u16) -> Option<Box<str>> {
            let idx = self.owner[slot as usize].load(Ordering::Acquire);
            if idx == UNASSIGNED {
                return None;
            }
            let table = self.table.lock().expect("slot-map node lock poisoned");
            table.nodes.get(idx as usize).map(|n| n.id.clone())
        }
    }
}
