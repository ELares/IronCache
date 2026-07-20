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
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, AtomicU64, Ordering};

/// The number of hash slots in the Redis-Cluster wire space (16384), re-exported from the
/// protocol crate (the single source of the wire constant). A key's slot is its
/// CRC16/XMODEM reduced into this range; this map assigns each of those 16384 slots an owner.
pub const CLUSTER_SLOTS: u16 = ironcache_protocol::CLUSTER_SLOTS;

/// The sentinel `owner[slot]` value for an UNASSIGNED slot (no node owns it). A fresh
/// [`SlotMap::empty_self`] node carries this in every slot until `CLUSTER ADDSLOTS`, and a
/// partial map (mid-formation) carries it in the not-yet-claimed slots. [`SlotMap::owner`]
/// returns `None` for it rather than indexing a bogus node.
const UNASSIGNED: u16 = u16::MAX;

/// The per-slot migration phase tag (HA-6 online slot migration). A COLD, purely-additive
/// scalar stored in `migration_state[slot]` (an `AtomicU8`) PARALLEL to `owner`/`replicas`,
/// NEVER read by the hot [`owns`](SlotMap::owns) path. The default static path (and raft mode
/// before any `SETSLOT MIGRATING/IMPORTING` commits) leaves every slot at [`MigrationState::None`],
/// so it is entirely inert and the routing is byte-unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationState {
    /// Not migrating. The default for every slot; the static path never leaves this.
    None,
    /// `CLUSTER SETSLOT <slot> MIGRATING <dest>`: THIS node still OWNS the slot, but is shipping
    /// its keys to `dest` (the index in `migration_peer[slot]`). A command on this slot whose
    /// key is ALREADY GONE (migrated / never existed) is answered `-ASK <slot> <dest>` (a one-time
    /// hint, NOT MOVED: ownership has not changed). Keys still present are served locally.
    Migrating,
    /// `CLUSTER SETSLOT <slot> IMPORTING <src>`: THIS node is RECEIVING the slot from `src` (the
    /// index in `migration_peer[slot]`) but does NOT yet own it. A normal command is MOVED to the
    /// real owner UNLESS the connection set the one-shot `ASKING` flag, in which case it is served
    /// locally (the migrated key has arrived here).
    Importing,
}

/// The migration-tag byte stored in the `AtomicU8` parallel array. Kept distinct from the public
/// [`MigrationState`] enum so the in-memory representation is fixed (a `repr`-stable byte) and the
/// enum can carry the peer index separately.
const MIG_NONE: u8 = 0;
const MIG_MIGRATING: u8 = 1;
const MIG_IMPORTING: u8 = 2;

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
///   node)` shape `CLUSTER SLOTS / SHARDS / NODES` need) and MOVED ([`moved_target`](Self::moved_target));
/// - the dense `replicas` array (slot -> replica node index, HA-7d) is a SEPARATE cold structure
///   for the replica-read routing path ([`is_replica_of`](Self::is_replica_of) /
///   [`replicas_of`](Self::replicas_of)), written only by the Raft `AssignReplica` apply
///   ([`set_slot_replica`](Self::set_slot_replica)) and NEVER read by `owns()`.
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
    /// Dense slot -> REPLICA node-index map (32 KiB of `AtomicU16`, boxed off-stack), PARALLEL
    /// to `owner` (HA-7d). `replicas[slot]` is the index into `table.nodes` of the node that
    /// REPLICATES `slot` (the MVP single-replica-per-slot), or [`UNASSIGNED`] for a slot with no
    /// replica. This is a NEW, purely-additive cold structure: it is read ONLY on the COLD replica
    /// -read routing path ([`is_replica_of`](Self::is_replica_of) / [`replicas_of`](Self::replicas_of),
    /// both lock-taking like [`moved_target`](Self::moved_target)), written ONLY by the Raft apply
    /// path ([`set_slot_replica`](Self::set_slot_replica)), and NEVER touched by the hot `owns()`
    /// path or the `mine[]` bitmap. So `owns()` stays a single `mine[slot]` atomic load and the
    /// default static path is byte-unchanged (this array is all-[`UNASSIGNED`] unless an
    /// AssignReplica is committed). It is renumbered alongside `owner` on FORGET.
    replicas: Box<[AtomicU16; CLUSTER_SLOTS as usize]>,
    /// Dense per-slot MIGRATION-PHASE tag (16 KiB of `AtomicU8`, boxed off-stack), PARALLEL to
    /// `owner` (HA-6). `migration_state[slot]` is one of [`MIG_NONE`] / [`MIG_MIGRATING`] /
    /// [`MIG_IMPORTING`]. This is a NEW, purely-additive COLD structure, mirroring how `replicas`
    /// was added (HA-7d): it is read ONLY on the cold redirect path
    /// ([`migration_state`](Self::migration_state)), written ONLY by the Raft apply path
    /// ([`set_migrating`](Self::set_migrating) / [`set_importing`](Self::set_importing) /
    /// [`clear_migration`](Self::clear_migration)), and NEVER touched by the hot `owns()` path or
    /// the `mine[]` bitmap. So `owns()` stays a single `mine[slot]` atomic load and the default
    /// static path is byte-unchanged (this array is all-[`MIG_NONE`] until a `SETSLOT MIGRATING /
    /// IMPORTING` is committed). It is reset to [`MIG_NONE`] for a slot whenever its OWNER flips
    /// (the FLIP clears any in-flight migration) and renumber-safe on FORGET (the tag is a phase,
    /// not a node index, so a FORGET renumber does not touch it; the PEER index below IS renumbered).
    migration_state: Box<[AtomicU8; CLUSTER_SLOTS as usize]>,
    /// Dense per-slot MIGRATION-PEER node-index map (32 KiB of `AtomicU16`, boxed off-stack),
    /// PARALLEL to `owner` (HA-6). For a slot tagged [`MIG_MIGRATING`] this is the DEST node index
    /// (where keys are being shipped, the `-ASK` target); for [`MIG_IMPORTING`] it is the SRC node
    /// index (the current owner the importer will adopt from). [`UNASSIGNED`] when the slot is not
    /// migrating. Read only alongside `migration_state` on the cold redirect path; renumbered on
    /// FORGET exactly like `replicas` (an index above the removed shifts down; an index equal to
    /// the removed clears the migration). NEVER read by `owns()`.
    migration_peer: Box<[AtomicU16; CLUSTER_SLOTS as usize]>,
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

// ---------------------------------------------------------------------------
// Compact, deterministic (de)serialization for the committed-config SNAPSHOT
// (HA-3c). A tiny hand-rolled little-endian codec (no serde), mirroring the
// raft-net wire / fsync-log style: a `u64` length-prefixed string, fixed-width
// scalars, every read bounds-checked so a malformed buffer yields None.
// ---------------------------------------------------------------------------

fn ser_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn ser_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// A length-prefixed UTF-8 string: a `u64` byte length then the UTF-8 bytes.
fn ser_str(out: &mut Vec<u8>, s: &str) {
    ser_u64(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

/// A forward-only, bounds-checked cursor over a committed-config snapshot buffer.
struct De<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> De<'a> {
    fn new(buf: &'a [u8]) -> Self {
        De { buf, pos: 0 }
    }

    fn u8(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn u16(&mut self) -> Option<u16> {
        let end = self.pos.checked_add(2)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(u16::from_le_bytes([slice[0], slice[1]]))
    }

    fn u64(&mut self) -> Option<u64> {
        let end = self.pos.checked_add(8)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(slice);
        Some(u64::from_le_bytes(bytes))
    }

    fn string(&mut self) -> Option<String> {
        let len = usize::try_from(self.u64()?).ok()?;
        let end = self.pos.checked_add(len)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        String::from_utf8(slice.to_vec()).ok()
    }
}

/// The fully-decoded committed-config snapshot (the output of [`decode_committed_snapshot`],
/// the input to the atomic publish phase of [`SlotMap::restore_committed`]). Holding the whole
/// decode as owned buffers is what lets the restore reject a malformed snapshot BEFORE touching
/// the live map (so a bad snapshot never half-wipes a good committed view) and then publish the
/// good one in a single locked pass.
struct CommittedSnapshot {
    /// The committed cluster epoch (the log-driven counter).
    epoch: u64,
    /// The node table the snapshot carries (sorted by id by `serialize_committed`).
    snap_nodes: Vec<NodeEntry>,
    /// `(slot, owner_id)` for each assigned slot.
    owners: Vec<(u16, String)>,
    /// `(slot, replica_id)` for each slot with a replica.
    reps: Vec<(u16, String)>,
    /// `(slot, state_byte, peer_id)` for each migrating slot.
    migs: Vec<(u16, u8, String)>,
}

/// Decode a committed-config snapshot (the inverse of [`SlotMap::serialize_committed`]) into
/// owned buffers, or `None` on ANY short / malformed read. PURE: it touches no map state, so a
/// `None` lets the caller leave the live map untouched (atomic-on-garbage). Mirrors the
/// bounds-checked, no-serde decode style the rest of the crate uses.
fn decode_committed_snapshot(data: &[u8]) -> Option<CommittedSnapshot> {
    let mut cur = De::new(data);
    let epoch = cur.u64()?;
    let node_count = usize::try_from(cur.u64()?).ok()?;
    let mut snap_nodes: Vec<NodeEntry> = Vec::with_capacity(node_count.min(CLUSTER_SLOTS as usize));
    for _ in 0..node_count {
        snap_nodes.push(NodeEntry {
            id: cur.string()?.as_str().into(),
            host: cur.string()?.as_str().into(),
            port: cur.u16()?,
        });
    }
    let owner_count = cur.u64()?;
    let mut owners: Vec<(u16, String)> = Vec::new();
    for _ in 0..owner_count {
        owners.push((cur.u16()?, cur.string()?));
    }
    let rep_count = cur.u64()?;
    let mut reps: Vec<(u16, String)> = Vec::new();
    for _ in 0..rep_count {
        reps.push((cur.u16()?, cur.string()?));
    }
    let mig_count = cur.u64()?;
    let mut migs: Vec<(u16, u8, String)> = Vec::new();
    for _ in 0..mig_count {
        migs.push((cur.u16()?, cur.u8()?, cur.string()?));
    }
    Some(CommittedSnapshot {
        epoch,
        snap_nodes,
        owners,
        reps,
        migs,
    })
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

/// Allocate a dense `[AtomicU8; CLUSTER_SLOTS]` on the HEAP filled with `init` (the per-slot
/// migration-phase tag, HA-6), avoiding an 8 KiB stack array (clippy::large_stack_arrays). Built
/// via a `Vec` then converted to a boxed fixed-size array, mirroring [`new_mine_array`].
fn new_mig_array(init: u8) -> Box<[AtomicU8; CLUSTER_SLOTS as usize]> {
    let mut v: Vec<AtomicU8> = Vec::with_capacity(CLUSTER_SLOTS as usize);
    for _ in 0..CLUSTER_SLOTS {
        v.push(AtomicU8::new(init));
    }
    v.into_boxed_slice()
        .try_into()
        .expect("the vec is exactly CLUSTER_SLOTS long")
}

/// One node's slot-balance position in a [`SlotMap::rebalance_plan`] (#371): how many slots it owns
/// now versus the balanced target. `target_slots - current_slots` is the signed move count (positive
/// = the node should RECEIVE that many slots, negative = it should give that many up). The plan is a
/// DRY-RUN summary; applying it (driving committed `SETSLOT` migrations) is a separate step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceTarget {
    /// The node id (40-hex announce id).
    pub node_id: String,
    /// How many slots the node owns in the committed map now.
    pub current_slots: u32,
    /// The balanced target slot count (the assigned slots spread as evenly as possible across the
    /// known nodes; the first `total % nodes` nodes get one extra so the targets sum to `total`).
    pub target_slots: u32,
}

/// One concrete slot relocation in a rebalance APPLY plan (#371, REBALANCE_APPLY.md): move `slot`
/// from `src_node_id` to `dst_node_id`. [`SlotMap::rebalance_moves`] derives the ordered list from
/// [`SlotMap::rebalance_plan`]'s per-node targets; the APPLY driver drives each move (committed
/// `SETSLOT MIGRATING`/`IMPORTING` -> key drain -> `SETSLOT NODE`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotMove {
    /// The slot to relocate (`0..=16383`).
    pub slot: u16,
    /// The current owner (the migration SOURCE).
    pub src_node_id: String,
    /// The balanced-target owner (the migration DESTINATION).
    pub dst_node_id: String,
}

/// The NEXT committed step the rebalance-APPLY controller should take for ONE [`SlotMove`] (#371,
/// REBALANCE_APPLY.md). The controller loops [`apply_step`] per move against the AUTHORITATIVE
/// committed state + the live copy progress, so it is RESUMABLE: a restart re-derives the step from
/// the committed map + the counts, holding no private controller checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyStep {
    /// The slot is not yet migrating: propose the committed `SETSLOT <slot> MIGRATING <dst>` (source)
    /// + `SETSLOT <slot> IMPORTING <src>` (destination). HA-6 then auto-copies the slot's data + tail.
    StartMigration,
    /// Migration is committed and HA-6 is still copying (the destination is not yet caught up): do
    /// nothing, poll again. The scoped tail keeps the destination converging as the source serves
    /// writes.
    AwaitCopy,
    /// The destination has SAFELY caught up (the driver's in-sync verdict): propose the committed
    /// `SETSLOT <slot> NODE <dst>`, the epoch-bumping ownership flip.
    Commit,
    /// Ownership has flipped to the destination: this move is complete.
    Done,
}

/// Decide the next rebalance-APPLY step for a move (#371, REBALANCE_APPLY.md), PURELY from the
/// authoritative committed state + a caught-up verdict the driver supplies:
/// - `owner_is_destination`: has the committed owner already flipped to the destination?
/// - `migration_in_flight`: has `MIGRATING`/`IMPORTING` been committed (and not yet flipped)?
/// - `destination_caught_up`: has HA-6 finished copying the slot to the destination such that it is
///   SAFE to flip ownership?
///
/// IMPORTANT: this function does NOT decide "caught up" itself, because the SAFE flip condition is a
/// data-safety judgement (a last-moment source write must not race the flip), not a coarse count
/// compare. The DRIVER computes `destination_caught_up` with the strongest signal available -- the
/// import being IN-SYNC on the source's offset (the ADR-0026 in-sync posture, not just
/// `COUNTKEYSINSLOT` parity, which is only a progress hint) and/or a brief source-write quiesce at the
/// flip -- and passes the verdict in. Keeping that judgement in the driver (where the live repl/import
/// state lives) and the STATE TRANSITION here (pure) is the clean split.
///
/// Because every input is read fresh from the committed map + the live import state, the controller
/// needs no durable checkpoint: after a crash it re-reads and resumes at the same step.
#[must_use]
pub fn apply_step(
    owner_is_destination: bool,
    migration_in_flight: bool,
    destination_caught_up: bool,
) -> ApplyStep {
    if owner_is_destination {
        ApplyStep::Done
    } else if !migration_in_flight {
        ApplyStep::StartMigration
    } else if destination_caught_up {
        ApplyStep::Commit
    } else {
        ApplyStep::AwaitCopy
    }
}

/// A single committed `CLUSTER SETSLOT` action the rebalance-APPLY driver proposes through the raft
/// path (#371, REBALANCE_APPLY.md). The driver translates each into the corresponding
/// `ConfigCmd::SetSlot*` proposal; keeping the MAPPING pure (here) and the raft I/O in the driver
/// keeps the decision logic unit-testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetSlotAction {
    /// `SETSLOT <slot> MIGRATING <dest>` (tag the SOURCE, which still owns the slot).
    Migrating { slot: u16, dest: String },
    /// `SETSLOT <slot> IMPORTING <src>` (tag the DESTINATION, which HA-6 then pulls into).
    Importing { slot: u16, src: String },
    /// `SETSLOT <slot> NODE <owner>` (the committed, epoch-bumping ownership flip to the destination).
    Node { slot: u16, owner: String },
}

/// The committed `SETSLOT` action(s) the driver must propose for `step` of one `mv` (#371): the
/// concrete realization of an [`ApplyStep`]. `StartMigration` proposes BOTH the source-side
/// `MIGRATING` and the destination-side `IMPORTING` (which arms HA-6's auto-copy); `Commit` proposes
/// the `NODE` flip; `AwaitCopy` / `Done` propose nothing (the driver just polls / advances). Pure, so
/// the driver's proposal set is unit-tested without a raft quorum.
#[must_use]
pub fn apply_actions(step: ApplyStep, mv: &SlotMove) -> Vec<SetSlotAction> {
    match step {
        ApplyStep::StartMigration => vec![
            SetSlotAction::Migrating {
                slot: mv.slot,
                dest: mv.dst_node_id.clone(),
            },
            SetSlotAction::Importing {
                slot: mv.slot,
                src: mv.src_node_id.clone(),
            },
        ],
        ApplyStep::Commit => vec![SetSlotAction::Node {
            slot: mv.slot,
            owner: mv.dst_node_id.clone(),
        }],
        ApplyStep::AwaitCopy | ApplyStep::Done => Vec::new(),
    }
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
            // No replicas at boot (the static path never assigns them; HA-7d's AssignReplica is
            // the only writer). All-UNASSIGNED keeps the cold replica-read path inert.
            replicas: new_owner_array(UNASSIGNED),
            // No migration at boot (HA-6): all-NONE keeps the cold redirect path inert, so the
            // static routing is byte-unchanged until a SETSLOT MIGRATING/IMPORTING commits.
            migration_state: new_mig_array(MIG_NONE),
            migration_peer: new_owner_array(UNASSIGNED),
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
            // No replicas at boot (HA-7d AssignReplica is the only writer; see the field doc).
            replicas: new_owner_array(UNASSIGNED),
            // No migration at boot (HA-6); the cold redirect path stays inert (see the field docs).
            migration_state: new_mig_array(MIG_NONE),
            migration_peer: new_owner_array(UNASSIGNED),
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

    /// The id of the node currently OWNING `slot`, or `None` if unassigned. Parallel to
    /// [`moved_target`](Self::moved_target) but returns the owner's NODE ID (owned) instead of its
    /// endpoint; the owner index is read INSIDE the node lock so it stays consistent with the `nodes`
    /// snapshot during a concurrent FORGET renumber. Used by the [`ConfigCmd::PromoteReplica`] apply
    /// (#728) to capture the demoted OLD owner before the ownership flip so it can be re-homed as the
    /// slot's replica of the new primary.
    #[must_use]
    pub fn owner_id(&self, slot: u16) -> Option<Box<str>> {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = self.owner[slot as usize].load(Ordering::Acquire);
        if idx == UNASSIGNED {
            return None;
        }
        table.nodes.get(idx as usize).map(|n| n.id.clone())
    }

    /// The node INDICES that replicate `slot` (HA-7d), as an owned `Vec` so the caller holds no
    /// borrow into the locked table. COLD projection, parallel to [`moved_target`](Self::moved_target):
    /// it reads `replicas[slot]` (the MVP single replica) and returns it (or empty if none). The hot
    /// `owns()` path never reads `replicas`, so this is purely additive.
    ///
    /// The returned indices are positions in the [`nodes`](Self::nodes) snapshot; callers that need
    /// the node identity should take it alongside (or use [`is_replica_of`](Self::is_replica_of),
    /// which resolves an id directly). MVP: at most one replica per slot, so the `Vec` has length 0
    /// or 1; the `Vec` return shape leaves room for a multi-replica set without an API change.
    #[must_use]
    pub fn replicas_of(&self, slot: u16) -> Vec<u16> {
        let idx = self.replicas[slot as usize].load(Ordering::Acquire);
        if idx == UNASSIGNED {
            Vec::new()
        } else {
            vec![idx]
        }
    }

    /// Whether the node with id `node_id` REPLICATES `slot` (HA-7d). COLD: it loads `replicas[slot]`
    /// and, under the node lock, compares the named node's table position (so the index stays
    /// consistent with the `nodes` snapshot during a FORGET renumber, exactly like
    /// [`moved_target`](Self::moved_target)). Returns `false` for an unassigned replica slot or an
    /// unknown id. The hot `owns()` path is untouched; a node that is a slot's replica but NOT its
    /// owner has `owns(slot) == false` and `is_replica_of(slot, self_id) == true`, which is what the
    /// replica-read router keys on.
    #[must_use]
    pub fn is_replica_of(&self, slot: u16, node_id: &str) -> bool {
        let idx = self.replicas[slot as usize].load(Ordering::Acquire);
        if idx == UNASSIGNED {
            return false;
        }
        let table = self.table.lock().expect("slot-map node lock poisoned");
        table
            .nodes
            .get(idx as usize)
            .is_some_and(|n| n.id.as_ref() == node_id)
    }

    /// Whether THIS node is a REPLICA of `slot` (HA-7d). COLD: it loads `replicas[slot]` then, IF
    /// set, compares the replica node entry's advertised `(host, port)` to THIS node's advertised
    /// endpoint under the node lock. Matching by ENDPOINT (not the table index) is deliberate: the
    /// same physical node can appear in the table under MORE THAN ONE id (its own `empty_self`
    /// announce id AND a host:port-synthesized id a peer's `CLUSTER MEET` added), so a committed
    /// `AssignReplica` that named the synth id would point `replicas[slot]` at the synth-id entry
    /// while `self_idx` points at the announce-id entry; both share this node's endpoint, so the
    /// endpoint compare correctly recognizes self either way. It does NOT read the hot `mine[]`
    /// bitmap, so `owns()` is untouched; a node can have `owns(slot) == false` while
    /// `is_replica_of_self(slot) == true` (the exact replica-read condition).
    #[must_use]
    pub fn is_replica_of_self(&self, slot: u16) -> bool {
        let idx = self.replicas[slot as usize].load(Ordering::Acquire);
        if idx == UNASSIGNED {
            return false;
        }
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let self_idx = self.self_idx.load(Ordering::Acquire) as usize;
        let (Some(rep), Some(me)) = (table.nodes.get(idx as usize), table.nodes.get(self_idx))
        else {
            return false;
        };
        rep.host == me.host && rep.port == me.port
    }

    // ----- HA-6: per-slot migration state (cold, additive; NEVER read by owns()) -----

    /// `slot`'s current migration phase (HA-6): [`MigrationState::None`] (not migrating),
    /// [`MigrationState::Migrating`] (THIS node owns it but is shipping it OUT), or
    /// [`MigrationState::Importing`] (THIS node is receiving it IN). A SINGLE relaxed-ordering
    /// `AtomicU8` load of the parallel `migration_state[slot]`, allocation-free. COLD: the redirect
    /// path consults it ONLY for an already-foreign-or-owned slot decision; the hot `owns()` path
    /// never reads it, so the default static path (all-NONE) is byte-unchanged.
    #[must_use]
    pub fn migration_state(&self, slot: u16) -> MigrationState {
        match self.migration_state[slot as usize].load(Ordering::Acquire) {
            MIG_MIGRATING => MigrationState::Migrating,
            MIG_IMPORTING => MigrationState::Importing,
            // Any other byte (only ever MIG_NONE is written for "not migrating") is treated as None.
            _ => MigrationState::None,
        }
    }

    /// The advertised `(host, port)` of `slot`'s MIGRATION PEER (HA-6): the DEST when the slot is
    /// MIGRATING (the `-ASK` redirect target) or the SRC when IMPORTING, or `None` when the slot is
    /// not migrating / the peer index is unset / unknown. Computed under the node lock (cold path),
    /// returning an OWNED `(String, u16)` so the caller holds no borrow into the locked table,
    /// exactly like [`moved_target`](Self::moved_target).
    #[must_use]
    pub fn migration_peer_endpoint(&self, slot: u16) -> Option<(String, u16)> {
        // Cheap pre-check OUTSIDE the lock: a non-migrating slot has no peer (the common case).
        if self.migration_state[slot as usize].load(Ordering::Acquire) == MIG_NONE {
            return None;
        }
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = self.migration_peer[slot as usize].load(Ordering::Acquire);
        if idx == UNASSIGNED {
            return None;
        }
        table
            .nodes
            .get(idx as usize)
            .map(|n| (n.host.to_string(), n.port))
    }

    /// The NODE ID of `slot`'s MIGRATION PEER (HA-6): the DEST id when the slot is MIGRATING, the
    /// SRC id when IMPORTING, or `None` when the slot is not migrating / the peer index is unset /
    /// unknown. The id-typed companion to [`migration_peer_endpoint`](Self::migration_peer_endpoint),
    /// used by the raft-mode `SETSLOT IMPORTING` proposal builder to fill the `dest` field: the
    /// command names only `src`, but the slot is already MIGRATING toward a known DEST (the MIGRATING
    /// step of the handshake committed first), so the leader reads the recorded DEST here and tags
    /// IMPORTING on EXACTLY that node (via `is_self(dest)`), never on the leader or a bystander.
    /// Cold path (a node-lock read, only on a SETSLOT proposal).
    #[must_use]
    pub fn migration_peer_id(&self, slot: u16) -> Option<String> {
        if self.migration_state[slot as usize].load(Ordering::Acquire) == MIG_NONE {
            return None;
        }
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = self.migration_peer[slot as usize].load(Ordering::Acquire);
        if idx == UNASSIGNED {
            return None;
        }
        table.nodes.get(idx as usize).map(|n| n.id.to_string())
    }

    /// `CLUSTER SETSLOT <slot> MIGRATING <dest>` apply ([`ConfigCmd::SetSlotMigrating`]): tag `slot`
    /// MIGRATING with `dest` as the peer (the `-ASK` target). `dest` must be a known node. Writes
    /// ONLY the parallel `migration_state` / `migration_peer` arrays; it does NOT touch `owner`,
    /// `mine`, `replicas`, or `owns()`, so the slot stays OWNED by whoever owned it (this node, in
    /// the source-side handshake). Inert on the hot path.
    ///
    /// # Errors
    ///
    /// [`SlotMutError::UnknownNode`] if `dest` is not in the node table.
    pub fn set_migrating(&self, slot: u16, dest: &str) -> Result<(), SlotMutError> {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = table
            .nodes
            .iter()
            .position(|n| n.id.as_ref() == dest)
            .ok_or_else(|| SlotMutError::UnknownNode(dest.to_owned()))?;
        // Publish the peer index FIRST, then the phase tag, both Release; a cold reader that sees
        // MIG_MIGRATING is guaranteed to also see the matching peer index.
        self.migration_peer[slot as usize].store(idx as u16, Ordering::Release);
        self.migration_state[slot as usize].store(MIG_MIGRATING, Ordering::Release);
        Ok(())
    }

    /// `CLUSTER SETSLOT <slot> IMPORTING <src>` apply ([`ConfigCmd::SetSlotImporting`]): tag `slot`
    /// IMPORTING with `src` as the peer (the current owner the importer adopts from). `src` must be
    /// a known node. Writes ONLY the parallel arrays; it does NOT touch `owner`, `mine`,
    /// `replicas`, or `owns()`, so the slot is NOT yet owned by this node until the committed FLIP.
    ///
    /// # Errors
    ///
    /// [`SlotMutError::UnknownNode`] if `src` is not in the node table.
    pub fn set_importing(&self, slot: u16, src: &str) -> Result<(), SlotMutError> {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = table
            .nodes
            .iter()
            .position(|n| n.id.as_ref() == src)
            .ok_or_else(|| SlotMutError::UnknownNode(src.to_owned()))?;
        self.migration_peer[slot as usize].store(idx as u16, Ordering::Release);
        self.migration_state[slot as usize].store(MIG_IMPORTING, Ordering::Release);
        Ok(())
    }

    /// `CLUSTER SETSLOT <slot> STABLE` apply ([`ConfigCmd::SetSlotStable`]) AND the FLIP-side
    /// migration clear: reset `slot` to [`MigrationState::None`] (clears both the phase tag and the
    /// peer index). Idempotent (clearing an already-clear slot is a no-op). Writes ONLY the parallel
    /// arrays; never touches `owner`, `mine`, `replicas`, or `owns()`. Called by `SetSlotStable`
    /// (abort a migration) and by [`set_slot_node`](Self::set_slot_node)'s caller after a committed
    /// FLIP (the migration is complete once ownership transfers).
    pub fn clear_migration(&self, slot: u16) {
        // Clear the phase FIRST (so a cold reader stops treating the slot as migrating), then the
        // peer index. Both Release.
        self.migration_state[slot as usize].store(MIG_NONE, Ordering::Release);
        self.migration_peer[slot as usize].store(UNASSIGNED, Ordering::Release);
    }

    /// THIS node's entry (id + advertised endpoint), cloned out from under the node lock.
    #[must_use]
    pub fn me(&self) -> NodeEntry {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = self.self_idx.load(Ordering::Acquire) as usize;
        table.nodes[idx].clone()
    }

    /// Whether `node_id` names THIS node (HA-6). Self is recognized FIRST by an exact id match, then
    /// by the advertised `(host, port)` ENDPOINT against `me()` -- the SAME dual-identity recognition
    /// `set_slot_node` / `is_replica_of_self` use: the same physical node can appear in the table
    /// under MORE THAN ONE id (its own `empty_self` announce id AND a host:port-synthesized id a
    /// peer's `CLUSTER MEET` added), so a committed `SetSlotImporting { dest }` that named the synth
    /// id must still be recognized as self by endpoint. Returns false when `node_id` is unknown or
    /// either entry is missing (a non-self / defensive case). It reads ONLY the node table, NEVER the
    /// hot `mine[]` bitmap, so `owns()` is untouched.
    #[must_use]
    pub fn is_self(&self, node_id: &str) -> bool {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let me_idx = self.self_idx.load(Ordering::Acquire) as usize;
        let Some(my_entry) = table.nodes.get(me_idx) else {
            return false;
        };
        if my_entry.id.as_ref() == node_id {
            return true;
        }
        // Fall back to the endpoint compare for the dual announce-id / synth-id identity.
        table
            .nodes
            .iter()
            .find(|n| n.id.as_ref() == node_id)
            .is_some_and(|n| n.host == my_entry.host && n.port == my_entry.port)
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

    /// UNCHECKED setter for `current_epoch`, used ONLY by the Raft apply path (HA-4c).
    ///
    /// The Raft control plane drives a MONOTONIC, log-driven epoch (incremented once per applied
    /// config entry, a deterministic function of the committed prefix; CONTROL_PLANE.md). To
    /// surface that epoch in `CLUSTER INFO cluster_current_epoch` it must be written directly,
    /// bypassing the Redis admin-command guards [`set_config_epoch`](Self::set_config_epoch)
    /// (rejected once peers are known) and [`bump_epoch`](Self::bump_epoch) (STILL once at the
    /// max) carry: those guard SEMANTICS are wrong for a log-driven counter. This setter just
    /// stores the value with no guard, with a `Release` store so a concurrent reader on a shard
    /// thread sees a consistent value (it follows the same ordering as the other epoch stores).
    ///
    /// The static-governance (slice 2/3) path NEVER calls this; only `ConfigSm::apply` does, on
    /// the single Raft control-plane task. So it does not perturb any pre-HA-4c behavior.
    pub fn set_committed_epoch(&self, epoch: u64) {
        self.current_epoch.store(epoch, Ordering::Release);
    }

    /// Serialize THIS map's COMMITTED CONFIG STATE to a compact, deterministic,
    /// NODE-INDEPENDENT byte form (HA-3c Raft snapshot, CONTROL_PLANE.md). The bytes
    /// capture the committed slot-ownership view -- the node table, owner-per-slot,
    /// replica-per-slot, migration-per-slot, and the cluster epoch -- so a fresh node
    /// fed the SAME committed `ConfigCmd` prefix produces the SAME bytes and a
    /// [`restore_committed`](Self::restore_committed) of those bytes reaches an
    /// IDENTICAL committed view. This is the `SlotMap` half of the config state
    /// machine's `StateMachine::snapshot`.
    ///
    /// NODE-INDEPENDENCE is the whole point: `owner[]` / `replicas[]` /
    /// `migration_peer[]` hold table INDICES, which differ per node's table order (each
    /// node lists ITSELF first via `empty_self`), so the on-the-wire form is keyed by
    /// node-ID STRING instead, and the node list is sorted by id, so two nodes at the
    /// same committed prefix emit byte-identical snapshots. THIS node's own identity
    /// (`self_idx`) is NOT serialized: a restore preserves the restoring node's own
    /// `empty_self` identity (it stays node-relative), exactly as replaying the
    /// committed log entry-by-entry would.
    ///
    /// The local advertised endpoint of self is part of the node list (so a restore on
    /// a peer rebuilds the same table), but the committed epoch carried is the
    /// log-driven `current_epoch`. Reads the whole state under the node lock so the
    /// node table, owner array, and the parallel arrays are a consistent snapshot.
    #[must_use]
    pub fn serialize_committed(&self) -> Vec<u8> {
        let table = self.table.lock().expect("slot-map node lock poisoned");

        // The node-id for a table index, or None for UNASSIGNED / out of range.
        let id_of = |idx: u16| -> Option<&str> {
            if idx == UNASSIGNED {
                return None;
            }
            table.nodes.get(idx as usize).map(|n| n.id.as_ref())
        };

        // Nodes, SORTED BY ID for a node-independent byte order.
        let mut nodes: Vec<&NodeEntry> = table.nodes.iter().collect();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));

        let mut out = Vec::with_capacity(256);
        // The committed cluster epoch (the log-driven counter the ConfigSm publishes).
        ser_u64(&mut out, self.current_epoch.load(Ordering::Acquire));
        // Node table (sorted by id): count, then (id, host, port) each.
        ser_u64(&mut out, nodes.len() as u64);
        for n in &nodes {
            ser_str(&mut out, &n.id);
            ser_str(&mut out, &n.host);
            ser_u16(&mut out, n.port);
        }
        // Owner-per-slot, as (slot, owner_id) for each ASSIGNED slot (ascending slot).
        let mut owners: Vec<(u16, &str)> = Vec::new();
        for slot in 0..CLUSTER_SLOTS {
            if let Some(id) = id_of(self.owner[slot as usize].load(Ordering::Acquire)) {
                owners.push((slot, id));
            }
        }
        ser_u64(&mut out, owners.len() as u64);
        for (slot, id) in &owners {
            ser_u16(&mut out, *slot);
            ser_str(&mut out, id);
        }
        // Replica-per-slot, as (slot, replica_id) for each slot with a replica.
        let mut reps: Vec<(u16, &str)> = Vec::new();
        for slot in 0..CLUSTER_SLOTS {
            if let Some(id) = id_of(self.replicas[slot as usize].load(Ordering::Acquire)) {
                reps.push((slot, id));
            }
        }
        ser_u64(&mut out, reps.len() as u64);
        for (slot, id) in &reps {
            ser_u16(&mut out, *slot);
            ser_str(&mut out, id);
        }
        // Migration-per-slot, as (slot, state_byte, peer_id) for each migrating slot.
        let mut migs: Vec<(u16, u8, &str)> = Vec::new();
        for slot in 0..CLUSTER_SLOTS {
            let st = self.migration_state[slot as usize].load(Ordering::Acquire);
            if st == MIG_NONE {
                continue;
            }
            if let Some(id) = id_of(self.migration_peer[slot as usize].load(Ordering::Acquire)) {
                migs.push((slot, st, id));
            }
        }
        ser_u64(&mut out, migs.len() as u64);
        for (slot, st, id) in &migs {
            ser_u16(&mut out, *slot);
            out.push(*st);
            ser_str(&mut out, id);
        }
        out
    }

    /// REPLACE this map's committed config state with the one serialized in `data`
    /// (HA-3c Raft snapshot restore). The inverse of
    /// [`serialize_committed`](Self::serialize_committed): after a restore the map's
    /// committed view (node table, owner / replica / migration per slot, epoch) is
    /// IDENTICAL to a map that had applied the same committed prefix, EXCEPT it keeps
    /// THIS node's own `empty_self` self-identity (so `owns()` / `me()` stay
    /// node-relative). Used when the config state machine installs a leader's snapshot
    /// or restores from a persisted snapshot on restart.
    ///
    /// It MUTATES THE EXISTING (shared) map in place rather than returning a new one,
    /// because the production `ConfigSm` holds an `Arc<SlotMap>` ALSO read by the serve
    /// path; an in-place restore keeps that shared `Arc` valid.
    ///
    /// ## ATOMIC vs concurrent `owns()` READERS (FIX 3)
    ///
    /// This is the SINGLE writer (the Raft control-plane task), but it is NOT the only
    /// accessor: the serve / routing threads call [`owns`](Self::owns) (a lock-free
    /// `Acquire` load of `mine[slot]`) on the hot path WITHOUT taking the table lock. The
    /// earlier "clear every `mine[]` to false, release the lock, then re-set the owned
    /// slots one at a time" shape therefore had a WINDOW: a concurrent `owns(slot)` for a
    /// slot this node actually owns could observe the transient `false` and emit a SPURIOUS
    /// `MOVED` for an owned key, for the whole 16384-slot rebuild. Holding the table lock
    /// alone does NOT fix this (`owns()` never takes that lock).
    ///
    /// So the restore is now COMPUTE-THEN-PUBLISH: it DECODES the whole snapshot into local
    /// buffers FIRST, then under the table lock writes each per-slot atomic exactly ONCE to
    /// its FINAL value. A slot owned by self both before and after is written `true` (its
    /// observable value never dips to `false`); a slot that changes hands is written once to
    /// its new value. A concurrent `owns(slot)` thus sees only the BEFORE or the AFTER value
    /// for that slot, never a torn all-cleared intermediate. The node table (`nodes` /
    /// `config_epochs` / `self_idx`) is swapped under the SAME held lock so the cold MOVED /
    /// projection readers (which DO take the lock) see a consistent table+index snapshot.
    ///
    /// ## Atomicity on malformed input
    ///
    /// A malformed / truncated `data` (never produced by `serialize_committed`) is detected
    /// DURING the decode phase, BEFORE any mutation, so the map is left UNCHANGED (a bad
    /// snapshot never half-wipes a good committed view). Restore stays total (no panic).
    ///
    /// Self-identity is preserved: the restored table keeps THIS node's own `empty_self`
    /// entry as `self_idx`, with the snapshot's other nodes appended (a snapshot node whose
    /// id equals self's is folded onto self, exactly as the prior `meet`-is-idempotent-on-
    /// self-by-id path did), so `owns()` / `me()` stay node-relative.
    ///
    /// FORWARD-ONLY SAFETY: the engine restores only from a snapshot of an APPLIED =
    /// COMMITTED prefix, so this never installs an uncommitted or conflicting view.
    pub fn restore_committed(&self, data: &[u8]) {
        // -- PHASE 1: DECODE into local buffers. A short / malformed read yields None and the
        // map is left UNCHANGED (no half-wipe of a good committed view).
        let Some(CommittedSnapshot {
            epoch,
            snap_nodes,
            owners,
            reps,
            migs,
        }) = decode_committed_snapshot(data)
        else {
            return;
        };

        // -- PHASE 2: PUBLISH atomically under the table lock. Build the new node table
        // (self preserved + snapshot peers appended), resolve every id to its index in that
        // table, then write each slot's atomics ONCE to its final value (no clear-pass), so a
        // concurrent lock-free owns() sees only before-or-after per slot.
        let mut table = self.table.lock().expect("slot-map node lock poisoned");

        // Keep THIS node's own entry as self (node-relative identity). Append each snapshot
        // node whose id is NOT already present (self's id is present), mirroring the prior
        // forget-all-then-meet-each path (meet is idempotent on self by id).
        let me_idx = self.self_idx.load(Ordering::Acquire) as usize;
        let self_entry = table.nodes[me_idx].clone();
        let mut new_nodes: Vec<NodeEntry> = Vec::with_capacity(snap_nodes.len() + 1);
        let mut new_epochs: Vec<u64> = Vec::with_capacity(snap_nodes.len() + 1);
        new_nodes.push(self_entry.clone());
        new_epochs.push(0);
        for n in snap_nodes {
            if new_nodes.iter().any(|e| e.id == n.id) {
                continue; // idempotent on self / any duplicate id, exactly like meet().
            }
            new_nodes.push(n);
            new_epochs.push(0);
        }
        // An id -> new-table-index lookup over the rebuilt node list.
        let index_of = |id: &str| -> Option<u16> {
            new_nodes
                .iter()
                .position(|e| e.id.as_ref() == id)
                .map(|p| p as u16)
        };

        // Per-slot FINAL values, defaulting to the empty baseline; the snapshot lists fill
        // in the assigned slots. Computing into dense fixed arrays keeps the publish a single
        // pass of one-store-per-atomic with no transient clear.
        let n_slots = CLUSTER_SLOTS as usize;
        let mut owner_of = vec![UNASSIGNED; n_slots];
        let mut mine_of = vec![false; n_slots];
        let mut replica_of = vec![UNASSIGNED; n_slots];
        let mut mig_state_of = vec![MIG_NONE; n_slots];
        let mut mig_peer_of = vec![UNASSIGNED; n_slots];

        for (slot, id) in &owners {
            if let (true, Some(idx)) = ((*slot as usize) < n_slots, index_of(id)) {
                owner_of[*slot as usize] = idx;
                // mine[slot] is true iff the new owner IS self: by table index OR by
                // advertised endpoint (the dual announce-id / synth-id identity), the same
                // rule set_slot_node uses. self is at index 0 of new_nodes.
                let is_self = idx == 0
                    || new_nodes.get(idx as usize).is_some_and(|owner| {
                        owner.host == self_entry.host && owner.port == self_entry.port
                    });
                mine_of[*slot as usize] = is_self;
            }
        }
        for (slot, id) in &reps {
            if let (true, Some(idx)) = ((*slot as usize) < n_slots, index_of(id)) {
                replica_of[*slot as usize] = idx;
            }
        }
        for (slot, st, id) in &migs {
            if (*slot as usize) < n_slots && (*st == MIG_MIGRATING || *st == MIG_IMPORTING) {
                if let Some(idx) = index_of(id) {
                    mig_state_of[*slot as usize] = *st;
                    mig_peer_of[*slot as usize] = idx;
                }
            }
        }

        // Swap in the new node table + reset self to index 0 (where we placed it), THEN write
        // each slot's atomics ONCE to its final value, all while holding the lock.
        table.nodes = new_nodes;
        table.config_epochs = new_epochs;
        self.self_idx.store(0, Ordering::Release);
        for slot in 0..n_slots {
            self.owner[slot].store(owner_of[slot], Ordering::Release);
            self.mine[slot].store(mine_of[slot], Ordering::Release);
            self.replicas[slot].store(replica_of[slot], Ordering::Release);
            self.migration_state[slot].store(mig_state_of[slot], Ordering::Release);
            self.migration_peer[slot].store(mig_peer_of[slot], Ordering::Release);
        }
        // Publish the committed epoch (the log-driven counter) under the SAME lock, so a
        // reader that already holds a consistent table also reads the matching epoch.
        self.current_epoch.store(epoch, Ordering::Release);
        drop(table);
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

    /// Compute a slot-balance plan (#371, the rebalance DRY-RUN): for every known node, how many
    /// slots it owns now versus a balanced target (the assigned slots spread as evenly as possible
    /// across the known nodes). PURE + read-only (it mutates nothing); the caller renders it for the
    /// operator to inspect BEFORE any apply. Applying it (driving committed `SETSLOT` migrations) is
    /// a separate, gated step.
    ///
    /// O(slots + nodes): one pass over the coalesced ranges to count, then one pass over the nodes
    /// to assign targets. The targets sum to the total assigned slots (the first `total % nodes`
    /// nodes get one extra), so the plan is conservation-preserving: balancing moves slots, never
    /// creates or drops them.
    #[must_use]
    pub fn rebalance_plan(&self) -> Vec<RebalanceTarget> {
        let nodes = self.nodes();
        if nodes.is_empty() {
            return Vec::new();
        }
        // Count the slots each node owns now (by its index in `nodes`).
        let mut current = vec![0u32; nodes.len()];
        for (start, end, owner_idx) in self.ranges() {
            if let Some(slot) = current.get_mut(owner_idx) {
                *slot += u32::from(end.saturating_sub(start)) + 1;
            }
        }
        let total: u32 = current.iter().sum();
        let n = u32::try_from(nodes.len()).unwrap_or(u32::MAX);
        let base = total / n;
        let extra = total % n;
        nodes
            .iter()
            .enumerate()
            .map(|(i, node)| RebalanceTarget {
                node_id: node.id.to_string(),
                current_slots: current[i],
                // The first `extra` nodes get one extra slot so the targets sum to `total`.
                target_slots: base + u32::from(u32::try_from(i).unwrap_or(u32::MAX) < extra),
            })
            .collect()
    }

    /// Derive the CONCRETE slot moves that realize [`rebalance_plan`](Self::rebalance_plan)'s
    /// per-node targets (#371, REBALANCE_APPLY.md slice 1): an ordered `{slot, src, dst}` list the
    /// APPLY driver drives one at a time. PURE + read-only over ONE consistent snapshot; the caller
    /// (a dry-run render or the driver) decides what to do with it.
    ///
    /// Each DONOR (a node over its balanced target) sheds its lowest-numbered surplus slots to the
    /// RECEIVERS (nodes under target) in node order, until every node reaches its target. Conservation
    /// holds: the targets sum to the assigned-slot total (the first `total % nodes` nodes get one
    /// extra), so `sum(surplus over donors) == sum(deficit over receivers)` and every surplus slot is
    /// placed. DETERMINISTIC: donors + receivers walk node index order, a donor's slots ascend, so the
    /// same map always yields the same moves. Returns empty for a single node (nothing to balance) and
    /// for a map already at the plan's exact targets. It realizes [`rebalance_plan`]'s CANONICAL
    /// (index-based) target, so a map balanced only WITHIN the rounding (its `base + 1` slots sit on
    /// different nodes than the canonical `first extra` choice) yields a few cosmetic moves, matching
    /// the plan's within-one deltas; the APPLY driver may skip a trivially-small plan. O(slots + nodes).
    #[must_use]
    pub fn rebalance_moves(&self) -> Vec<SlotMove> {
        let nodes = self.nodes();
        if nodes.len() < 2 {
            return Vec::new();
        }
        // One snapshot: the slots each node owns, ascending. `ranges()` yields the owner index per
        // coalesced range; an UNASSIGNED range's index is out of bounds and is skipped (unassigned
        // slots are not owned, so a rebalance does not move them, matching `rebalance_plan`).
        let mut owned: Vec<Vec<u16>> = vec![Vec::new(); nodes.len()];
        for (start, end, owner_idx) in self.ranges() {
            if let Some(v) = owned.get_mut(owner_idx) {
                v.extend(start..=end);
            }
        }
        for slots in &mut owned {
            slots.sort_unstable(); // ascending, independent of range yield order (determinism).
        }
        // The balanced targets, computed the SAME way as `rebalance_plan`: base each, the first
        // `extra` nodes get one more, so the targets sum to the assigned total.
        let total: u32 = owned
            .iter()
            .map(|v| u32::try_from(v.len()).unwrap_or(u32::MAX))
            .sum();
        let n = u32::try_from(nodes.len()).unwrap_or(u32::MAX);
        let base = total / n;
        let extra = total % n;
        // surplus[i] = current - target: a DONOR is > 0, a RECEIVER is < 0.
        let mut surplus: Vec<i64> = (0..nodes.len())
            .map(|i| {
                let target = base + u32::from(u32::try_from(i).unwrap_or(u32::MAX) < extra);
                i64::from(u32::try_from(owned[i].len()).unwrap_or(u32::MAX)) - i64::from(target)
            })
            .collect();
        let mut moves = Vec::new();
        let mut recv = 0usize; // monotonic receiver cursor (a filled receiver is never revisited).
        for donor in 0..nodes.len() {
            let mut slot_iter = owned[donor].iter();
            while surplus[donor] > 0 {
                // The next node that still NEEDS slots (surplus < 0); skip donors + filled receivers.
                while recv < nodes.len() && surplus[recv] >= 0 {
                    recv += 1;
                }
                if recv >= nodes.len() {
                    break; // conserved maps never hit this; defensive.
                }
                let Some(&slot) = slot_iter.next() else {
                    break;
                };
                moves.push(SlotMove {
                    slot,
                    src_node_id: nodes[donor].id.to_string(),
                    dst_node_id: nodes[recv].id.to_string(),
                });
                surplus[donor] -= 1;
                surplus[recv] += 1;
            }
        }
        moves
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
    /// DOCUMENTED SLICE-3 BOUNDARY: this is a single owner store, so a slot flipped AWAY from self
    /// can briefly still be served locally by an in-flight request that read `owns()` just before
    /// this store (and, symmetrically, a slot flipped TO self may be served a moment late). This is
    /// the inherent SETSLOT window; it is NOT a torn read (each atomic is internally consistent).
    /// The HA-6 MIGRATING / IMPORTING / ASK state machine gates serving during the handoff so the
    /// per-key cutover is clean; this single FLIP is the atomic ownership-transfer step at its end.
    ///
    /// HA-6: a committed FLIP also CLEARS any in-flight migration state on the slot (the migration
    /// is complete once ownership transfers), so the source stops sending ASK and serves MOVED, and
    /// the destination stops requiring ASKING. Clearing here keeps the FLIP atomic with the
    /// migration teardown on EVERY node's apply (idempotent: clearing an already-clear slot is a
    /// no-op), which is what makes a committed FLIP leave EXACTLY one owner and no stale migration
    /// tag, on both endpoints, after they catch their logs up.
    pub fn set_slot_node(&self, slot: u16, node_id: &str) -> Result<(), SlotMutError> {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = table
            .nodes
            .iter()
            .position(|n| n.id.as_ref() == node_id)
            .ok_or_else(|| SlotMutError::UnknownNode(node_id.to_owned()))?;
        let idx_u16 = idx as u16;
        self.owner[slot as usize].store(idx_u16, Ordering::Release);
        // Keep the self-ownership bitmap in lockstep: this slot is ours iff the new owner IS self.
        // Self is recognized by table INDEX (the common case) OR by advertised ENDPOINT (the
        // dual-identity case, mirroring `is_replica_of_self`): the SAME physical node can appear in
        // the table under MORE THAN ONE id -- its own `empty_self` announce id AND a host:port-
        // synthesized id a peer's `CLUSTER MEET` added. A committed FLIP that names the synth id
        // would point `owner[slot]` at the synth-id entry while `self_idx` points at the announce-id
        // entry; both share this node's endpoint, so the endpoint compare correctly recognizes self
        // either way (without it, a node would fail to claim a slot flipped to its own synth id and
        // would MOVED to itself). owns() still reads ONLY this `mine[slot]` bit, unchanged.
        let me = self.self_idx.load(Ordering::Acquire) as usize;
        let is_self = idx == me
            || match (table.nodes.get(idx), table.nodes.get(me)) {
                (Some(new_owner), Some(my_entry)) => {
                    new_owner.host == my_entry.host && new_owner.port == my_entry.port
                }
                // Defensive: a missing entry cannot be self.
                _ => false,
            };
        self.mine[slot as usize].store(is_self, Ordering::Release);
        // HA-6: the FLIP completes (or aborts) any migration on this slot. Clear the parallel
        // migration arrays in lockstep with the owner store (cold path, never read by owns()).
        self.migration_state[slot as usize].store(MIG_NONE, Ordering::Release);
        self.migration_peer[slot as usize].store(UNASSIGNED, Ordering::Release);
        Ok(())
    }

    /// UN-assign `slot`: clear its owner so it is owned by NOBODY (the [`ConfigCmd::UnassignSlots`]
    /// apply; the committed-log analog of `CLUSTER DELSLOTS / DELSLOTSRANGE / FLUSHSLOTS`). Sets
    /// `owner[slot]` to [`UNASSIGNED`] and clears `mine[slot]` in LOCKSTEP (the same invariant
    /// [`SlotMap::del_slots`] / [`SlotMap::flush_slots`] keep), so a node that owned the slot loses
    /// it (`owns()` goes false) and a node that did not is unaffected. Unlike [`SlotMap::del_slots`]
    /// (the Redis-client mutator, which ERRORS if a slot is already unassigned), this is
    /// UNCONDITIONAL and IDEMPOTENT: clearing an already-unassigned slot is a no-op, so re-applying a
    /// committed entry yields the identical map. Always succeeds (no node-id lookup, no precondition).
    pub fn clear_slot_owner(&self, slot: u16) {
        let _guard = self.table.lock().expect("slot-map node lock poisoned");
        self.owner[slot as usize].store(UNASSIGNED, Ordering::Release);
        // The slot is now unassigned, hence no longer ours: clear the self-ownership bitmap in
        // lockstep so owns() (a single mine[slot] load) is correct on every node.
        self.mine[slot as usize].store(false, Ordering::Release);
    }

    /// Assign `node_id` as the REPLICA of `slot` (HA-7d; the [`ConfigCmd::AssignReplica`] apply).
    /// The node id must be known (a prior committed `AddNode`). Writes the new parallel
    /// `replicas[slot]` index; it does NOT touch `owner[slot]`, `mine[slot]`, or `owns()`, so the
    /// hot path is unaffected and a slot can have a distinct owner and replica simultaneously.
    ///
    /// # Errors
    ///
    /// [`SlotMutError::UnknownNode`] if `node_id` is not in the node table.
    pub fn set_slot_replica(&self, slot: u16, node_id: &str) -> Result<(), SlotMutError> {
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let idx = table
            .nodes
            .iter()
            .position(|n| n.id.as_ref() == node_id)
            .ok_or_else(|| SlotMutError::UnknownNode(node_id.to_owned()))?;
        // `idx` fits u16 (node count is bounded far below u16::MAX by the 16384 slots). Publish with
        // a Release store so a concurrent cold reader on a shard thread sees a consistent value.
        self.replicas[slot as usize].store(idx as u16, Ordering::Release);
        Ok(())
    }

    /// Clear `slot`'s REPLICA entry IFF it currently names `node_id` (HA-8; the
    /// [`ConfigCmd::PromoteReplica`] apply calls this AFTER flipping the owner to `node_id`, so the
    /// just-promoted node is no longer recorded as the slot's replica -- it is the OWNER now). A
    /// no-op when `slot`'s replica is unassigned, names a DIFFERENT node, or `node_id` is unknown:
    /// promotion must never silently clear some OTHER node's replica role, and re-applying the same
    /// committed promotion (idempotent) is harmless because the entry is already cleared. Writes
    /// `replicas[slot]` only (NEVER `owner[slot]`, `mine[slot]`, or `owns()`); the hot path is
    /// untouched and the default static path stays inert (this array is all-`UNASSIGNED` until an
    /// AssignReplica/PromoteReplica is committed).
    pub fn clear_slot_replica(&self, slot: u16, node_id: &str) {
        // Load the current replica index OUTSIDE the lock first (the common case is UNASSIGNED:
        // nothing to do, no lock needed). Only when a replica IS recorded do we take the node lock
        // to resolve its id consistently with the `nodes` snapshot (like `is_replica_of`).
        let idx = self.replicas[slot as usize].load(Ordering::Acquire);
        if idx == UNASSIGNED {
            return;
        }
        let table = self.table.lock().expect("slot-map node lock poisoned");
        let names_node = table
            .nodes
            .get(idx as usize)
            .is_some_and(|n| n.id.as_ref() == node_id);
        if names_node {
            // The slot's recorded replica is exactly `node_id`: clear it (it is now the owner).
            // Release store so a concurrent cold reader on a shard thread sees the cleared value.
            self.replicas[slot as usize].store(UNASSIGNED, Ordering::Release);
        }
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
        // Renumber `replicas[]` identically (HA-7d): a replica index strictly above `removed`
        // shifts down by one, and any slot whose REPLICA was the removed node is cleared to
        // UNASSIGNED (that replica is gone). This keeps `replicas[]` consistent with the shrunk
        // node table, exactly as the owner renumber does for `owner[]`. `mine[]` is still
        // untouched (the owns()-race reasoning is unchanged; replicas[] is never read by owns()).
        for r in self.replicas.iter() {
            let idx = r.load(Ordering::Acquire);
            if idx == removed_u16 {
                r.store(UNASSIGNED, Ordering::Release);
            } else if idx != UNASSIGNED && idx > removed_u16 {
                r.store(idx - 1, Ordering::Release);
            }
        }
        // Renumber `migration_peer[]` identically (HA-6): a peer index strictly above `removed`
        // shifts down by one, and any slot whose migration PEER (dest/src) was the removed node has
        // its migration CLEARED (the peer is gone, so the in-flight migration can no longer
        // complete). `migration_state[]` is the phase TAG (not a node index), so a slot whose peer
        // is cleared also drops to MIG_NONE here to keep the two arrays consistent. `mine[]` is
        // still untouched (the owns()-race reasoning is unchanged; neither array feeds owns()).
        for slot in 0..CLUSTER_SLOTS as usize {
            let idx = self.migration_peer[slot].load(Ordering::Acquire);
            if idx == removed_u16 {
                self.migration_peer[slot].store(UNASSIGNED, Ordering::Release);
                self.migration_state[slot].store(MIG_NONE, Ordering::Release);
            } else if idx != UNASSIGNED && idx > removed_u16 {
                self.migration_peer[slot].store(idx - 1, Ordering::Release);
            }
        }
        if me > removed {
            self.self_idx.store((me - 1) as u16, Ordering::Release);
        }
        Ok(())
    }

    /// Collapse duplicate node-table entries that advertise the SAME `(host, port)` ENDPOINT into a
    /// single entry, reindexing `owner` / `replicas` / `migration_peer` / `self_idx` so routing is
    /// preserved. Returns the number of entries removed (0 = the table was already endpoint-unique).
    ///
    /// WHY (item-7 id-reconciliation, DEFENSIVE): the raft-mode MEET now LEARNS a peer's real
    /// announce id (`serve::build_meet`), so a fresh cluster never creates a synth/announce
    /// duplicate. But a table built BEFORE that fix (or one that fell back to the synth id once for
    /// an unreachable peer, then later learned the real id on a re-MEET) can hold TWO entries for one
    /// physical node: its real announce id AND a `host:port`-synthesized id. Both share the node's
    /// endpoint, so routing/ownership were already correct (they match by endpoint), but
    /// `cluster_known_nodes` / `CLUSTER NODES` were INFLATED. This reconciles those duplicates.
    ///
    /// THE RULE: for each endpoint group, KEEP one canonical entry and remove the rest. The kept
    /// entry is THIS node's entry when self is in the group (so `self_idx` keeps naming self),
    /// otherwise the LOWEST-INDEX entry in the group (stable / order-independent). Every owner /
    /// replica / migration-peer index that pointed at a removed duplicate is REMAPPED onto the kept
    /// index FIRST, then the duplicates are removed and the surviving indices are renumbered (every
    /// index above a removed slot shifts down), exactly like [`forget`](Self::forget) does for one
    /// node. `mine[]` is left untouched: it records "is this slot self's", which is invariant under a
    /// renumber that only collapses ALIASES of an endpoint (self's slots stay self's), so the hot
    /// `owns()` path is unaffected and never races this cold, lock-held reconciliation.
    ///
    /// This is purely additive: it touches only the cold node table + the cold index arrays under
    /// the table lock; the default static path never calls it.
    pub fn dedup_nodes_by_endpoint(&self) -> usize {
        let mut table = self.table.lock().expect("slot-map node lock poisoned");
        let me = self.self_idx.load(Ordering::Acquire) as usize;

        // Map each old index -> the index of its endpoint group's CANONICAL (kept) entry. Walk in
        // table order; the first entry seen for an endpoint is canonical UNLESS self shares that
        // endpoint, in which case self becomes canonical (so self_idx keeps naming self).
        let n = table.nodes.len();
        let mut canonical: Vec<usize> = (0..n).collect();
        for i in 0..n {
            for j in 0..i {
                if canonical[j] == j // j is a group leader (not itself an alias)
                    && table.nodes[j].host == table.nodes[i].host
                    && table.nodes[j].port == table.nodes[i].port
                {
                    // i is a duplicate of group-leader j. Prefer self as the leader if i IS self.
                    if i == me {
                        // Re-point the whole existing group (j and any alias already mapped to j)
                        // at self (i), then i leads.
                        for c in &mut canonical {
                            if *c == j {
                                *c = i;
                            }
                        }
                        canonical[i] = i;
                    } else {
                        canonical[i] = canonical[j];
                    }
                    break;
                }
            }
        }

        // Which OLD indices are aliases (removed)? An index is removed iff it is not its own
        // canonical. Nothing to do when the table is already endpoint-unique.
        let removed_any = (0..n).any(|i| canonical[i] != i);
        if !removed_any {
            return 0;
        }

        // First REMAP every index array off the removed aliases onto their canonical (still using
        // OLD indices), so no array points at an entry about to be deleted.
        let remap = |idx: u16| -> u16 {
            if idx == UNASSIGNED {
                idx
            } else {
                canonical[idx as usize] as u16
            }
        };
        for o in self.owner.iter() {
            o.store(remap(o.load(Ordering::Acquire)), Ordering::Release);
        }
        for r in self.replicas.iter() {
            r.store(remap(r.load(Ordering::Acquire)), Ordering::Release);
        }
        for p in self.migration_peer.iter() {
            p.store(remap(p.load(Ordering::Acquire)), Ordering::Release);
        }
        // self_idx onto its canonical (self is its own canonical by the rule above, but the leader
        // it pointed at may differ if self was an alias of an earlier entry; remap is correct).
        let new_self_old = canonical[me];

        // Now COMPACT: build the kept set (old indices that are their own canonical), in order, and
        // an old->new index translation for the survivors.
        let mut old_to_new: Vec<u16> = vec![UNASSIGNED; n];
        let mut new_nodes: Vec<NodeEntry> = Vec::with_capacity(n);
        let mut new_epochs: Vec<u64> = Vec::with_capacity(n);
        let mut next_new = 0u16;
        for i in 0..n {
            if canonical[i] == i {
                old_to_new[i] = next_new;
                new_nodes.push(table.nodes[i].clone());
                new_epochs.push(table.config_epochs[i]);
                next_new += 1;
            }
        }
        let removed = n - new_nodes.len();

        // Translate every (already-remapped) index array from old survivor indices to new ones.
        let translate = |idx: u16| -> u16 {
            if idx == UNASSIGNED {
                idx
            } else {
                old_to_new[idx as usize]
            }
        };
        for o in self.owner.iter() {
            o.store(translate(o.load(Ordering::Acquire)), Ordering::Release);
        }
        for r in self.replicas.iter() {
            r.store(translate(r.load(Ordering::Acquire)), Ordering::Release);
        }
        for p in self.migration_peer.iter() {
            p.store(translate(p.load(Ordering::Acquire)), Ordering::Release);
        }
        self.self_idx
            .store(old_to_new[new_self_old], Ordering::Release);

        table.nodes = new_nodes;
        table.config_epochs = new_epochs;
        removed
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
    fn rebalance_moves_realize_the_plan_targets_and_conserve_slots() {
        // ID0 owns the whole space; ID1 + ID2 are empty. The moves must level ownership to the plan.
        let map = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 16383]]),
                (node(ID1, "h1", 2), vec![]),
                (node(ID2, "h2", 3), vec![]),
            ],
            ID0,
        )
        .unwrap();
        let plan = map.rebalance_plan();
        let moves = map.rebalance_moves();
        let idx = |id: &str| plan.iter().position(|t| t.node_id == id).unwrap();

        // Applying the moves to the current counts yields EXACTLY the balanced targets.
        let mut count: Vec<i64> = plan.iter().map(|t| i64::from(t.current_slots)).collect();
        for m in &moves {
            assert_eq!(m.src_node_id, ID0, "ID0 is the only donor");
            assert_ne!(m.src_node_id, m.dst_node_id, "a move changes owner");
            count[idx(&m.src_node_id)] -= 1;
            count[idx(&m.dst_node_id)] += 1;
        }
        for (i, t) in plan.iter().enumerate() {
            assert_eq!(
                count[i],
                i64::from(t.target_slots),
                "node {} reaches its target",
                t.node_id
            );
        }

        // Every moved slot is distinct (no slot moves twice) and was one of ID0's slots.
        let mut slots: Vec<u16> = moves.iter().map(|m| m.slot).collect();
        let move_count = slots.len();
        slots.sort_unstable();
        slots.dedup();
        assert_eq!(slots.len(), move_count, "no slot is moved twice");
        // The move count equals ID0's surplus (current - target).
        let d0 = idx(ID0);
        assert_eq!(
            u32::try_from(move_count).unwrap(),
            plan[d0].current_slots - plan[d0].target_slots,
            "moves == the donor's surplus"
        );
    }

    #[test]
    fn rebalance_moves_of_an_exactly_balanced_or_single_node_map_is_empty() {
        // A map already at the CANONICAL target (base+1 on the first node, base on the rest, since
        // 16384 % 3 == 1) needs no moves: node0 owns 5462, node1 + node2 own 5461 each.
        let canonical = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 5461]]),
                (node(ID1, "h1", 2), vec![[5462, 10922]]),
                (node(ID2, "h2", 3), vec![[10923, 16383]]),
            ],
            ID0,
        )
        .unwrap();
        assert!(
            canonical.rebalance_moves().is_empty(),
            "an exactly-balanced map moves nothing"
        );
        // A single node has nothing to balance against.
        let solo = SlotMap::build(vec![(node(ID0, "h0", 1), vec![[0, 16383]])], ID0).unwrap();
        assert!(solo.rebalance_moves().is_empty());
    }

    #[test]
    fn rebalance_moves_is_deterministic() {
        let map = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 16383]]),
                (node(ID1, "h1", 2), vec![]),
                (node(ID2, "h2", 3), vec![]),
            ],
            ID0,
        )
        .unwrap();
        assert_eq!(
            map.rebalance_moves(),
            map.rebalance_moves(),
            "the same map yields the same moves"
        );
    }

    #[test]
    fn rebalance_moves_handle_multiple_donors_and_receivers() {
        // Two overloaded owners (8192 each) + two empty; 16384/4 = 4096 exactly (no remainder), so
        // each donor sheds 4096 and each receiver gains 4096. Exercises the receiver cursor spanning
        // more than one donor and more than one receiver.
        const ID3: &str = "3333333333333333333333333333333333333333";
        let map = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 8191]]),
                (node(ID1, "h1", 2), vec![[8192, 16383]]),
                (node(ID2, "h2", 3), vec![]),
                (node(ID3, "h3", 4), vec![]),
            ],
            ID0,
        )
        .unwrap();
        let plan = map.rebalance_plan();
        let moves = map.rebalance_moves();
        let idx = |id: &str| plan.iter().position(|t| t.node_id == id).unwrap();

        let mut count: Vec<i64> = plan.iter().map(|t| i64::from(t.current_slots)).collect();
        for m in &moves {
            // Only the two loaded owners donate; only the two empties receive.
            assert!(
                m.src_node_id == ID0 || m.src_node_id == ID1,
                "donor is a loaded owner"
            );
            assert!(
                m.dst_node_id == ID2 || m.dst_node_id == ID3,
                "receiver is an empty owner"
            );
            count[idx(&m.src_node_id)] -= 1;
            count[idx(&m.dst_node_id)] += 1;
        }
        for (i, t) in plan.iter().enumerate() {
            assert_eq!(
                count[i],
                i64::from(t.target_slots),
                "node {} reaches 4096",
                t.node_id
            );
            assert_eq!(t.target_slots, 4096, "an even 4-way split");
        }
        // No slot moved twice; the total moved is the summed surplus (4096 + 4096).
        let mut slots: Vec<u16> = moves.iter().map(|m| m.slot).collect();
        let n = slots.len();
        slots.sort_unstable();
        slots.dedup();
        assert_eq!(slots.len(), n, "no slot moved twice");
        assert_eq!(n, 8192, "both donors' surplus is relocated");
    }

    #[test]
    fn apply_step_walks_a_move_start_to_done() {
        // Not migrating yet -> start (the caught-up verdict is irrelevant here).
        assert_eq!(apply_step(false, false, false), ApplyStep::StartMigration);
        assert_eq!(apply_step(false, false, true), ApplyStep::StartMigration);

        // Migrating but not yet caught up -> keep waiting for HA-6 to copy.
        assert_eq!(apply_step(false, true, false), ApplyStep::AwaitCopy);

        // Migrating and safely caught up (the driver's verdict) -> commit the flip.
        assert_eq!(apply_step(false, true, true), ApplyStep::Commit);

        // Ownership flipped -> done, regardless of the (now stale) migrating / caught-up flags.
        assert_eq!(apply_step(true, false, false), ApplyStep::Done);
        assert_eq!(apply_step(true, true, false), ApplyStep::Done);
        assert_eq!(apply_step(true, true, true), ApplyStep::Done);
    }

    #[test]
    fn apply_actions_maps_each_step_to_the_right_setslot_proposals() {
        let mv = SlotMove {
            slot: 42,
            src_node_id: "src".to_owned(),
            dst_node_id: "dst".to_owned(),
        };
        // StartMigration -> BOTH the source MIGRATING and the destination IMPORTING (arms HA-6).
        assert_eq!(
            apply_actions(ApplyStep::StartMigration, &mv),
            vec![
                SetSlotAction::Migrating {
                    slot: 42,
                    dest: "dst".to_owned()
                },
                SetSlotAction::Importing {
                    slot: 42,
                    src: "src".to_owned()
                },
            ]
        );
        // Commit -> the NODE flip to the destination.
        assert_eq!(
            apply_actions(ApplyStep::Commit, &mv),
            vec![SetSlotAction::Node {
                slot: 42,
                owner: "dst".to_owned()
            }]
        );
        // AwaitCopy / Done -> nothing to propose (poll / advance).
        assert!(apply_actions(ApplyStep::AwaitCopy, &mv).is_empty());
        assert!(apply_actions(ApplyStep::Done, &mv).is_empty());
    }

    #[test]
    fn apply_step_is_a_pure_function_of_committed_state_so_it_resumes() {
        // The controller holds no checkpoint: the SAME inputs always yield the SAME step, so a
        // restart re-deriving from the committed map + the live import state picks up at the same
        // place. Exhaustive over the 8-point input space.
        for &owner in &[false, true] {
            for &mig in &[false, true] {
                for &caught in &[false, true] {
                    assert_eq!(
                        apply_step(owner, mig, caught),
                        apply_step(owner, mig, caught)
                    );
                }
            }
        }
    }

    #[test]
    fn rebalance_plan_of_a_balanced_map_proposes_no_moves() {
        let plan = three_node().rebalance_plan();
        assert_eq!(plan.len(), 3);
        // A 16384/3 split is 5461 + 5462 + 5461 (CLUSTER_SLOTS is not divisible by 3), so the
        // targets are within one slot of the current counts: a balanced map asks for no real moves.
        for t in &plan {
            let delta = i64::from(t.target_slots) - i64::from(t.current_slots);
            assert!(
                delta.abs() <= 1,
                "balanced node {} moves {delta}",
                t.node_id
            );
        }
        let total: u32 = plan.iter().map(|t| t.target_slots).sum();
        assert_eq!(
            total,
            u32::from(CLUSTER_SLOTS),
            "targets conserve every slot"
        );
    }

    #[test]
    fn rebalance_plan_of_a_skewed_map_levels_the_owners() {
        // ID0 owns the whole space; ID1 and ID2 are known but empty.
        let map = SlotMap::build(
            vec![
                (node(ID0, "h0", 1), vec![[0, 16383]]),
                (node(ID1, "h1", 2), vec![]),
                (node(ID2, "h2", 3), vec![]),
            ],
            ID0,
        )
        .unwrap();
        let plan = map.rebalance_plan();
        assert_eq!(plan[0].current_slots, u32::from(CLUSTER_SLOTS));
        assert_eq!(plan[1].current_slots, 0);
        assert_eq!(plan[2].current_slots, 0);
        // Targets even out (5462 + 5461 + 5461) and still sum to the whole space, so the overloaded
        // owner sheds ~2/3 of its slots while conservation holds.
        let total: u32 = plan.iter().map(|t| t.target_slots).sum();
        assert_eq!(total, u32::from(CLUSTER_SLOTS));
        assert!(
            plan[0].target_slots < plan[0].current_slots,
            "ID0 sheds slots"
        );
        assert!(
            plan[1].target_slots > 0 && plan[2].target_slots > 0,
            "empty nodes gain slots"
        );
        assert!(
            plan.iter().map(|t| t.target_slots).max().unwrap()
                - plan.iter().map(|t| t.target_slots).min().unwrap()
                <= 1,
            "targets differ by at most one slot"
        );
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

    /// HA-6 Finding 2: `is_self` recognizes THIS node by id, by advertised endpoint (the dual
    /// announce-id / synth-id identity), and rejects a peer / an unknown id.
    #[test]
    fn is_self_recognizes_own_id_and_endpoint() {
        let map = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        // A second table entry with a DIFFERENT id but THIS node's endpoint (the synth-id case a
        // peer's MEET adds): is_self must recognize it by host:port even though the id differs.
        map.meet(node(ID2, "127.0.0.1", 7000));
        // A genuine peer at a different endpoint.
        map.meet(node(ID1, "127.0.0.1", 7001));

        assert!(map.is_self(ID0), "own announce id is self");
        assert!(
            map.is_self(ID2),
            "a synth id sharing this node's endpoint is self (endpoint compare)"
        );
        assert!(!map.is_self(ID1), "a peer at another endpoint is NOT self");
        assert!(
            !map.is_self("ffffffffffffffffffffffffffffffffffffffff"),
            "an unknown id is NOT self"
        );
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
    fn set_committed_epoch_stores_unchecked_for_the_raft_apply_path() {
        // The Raft apply path drives a log-driven epoch directly, bypassing the Redis admin
        // guards. set_config_epoch is rejected once peers are known, but set_committed_epoch
        // is unguarded (HA-4c): it just stores the value.
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2)); // now knows a peer -> set_config_epoch would reject
        assert!(matches!(
            map.set_config_epoch(7),
            Err(SlotMutError::EpochKnowsOthers)
        ));
        // set_committed_epoch stores regardless, and current_epoch reflects it (monotone is the
        // caller's responsibility; here we just prove the unchecked store + read-back).
        map.set_committed_epoch(7);
        assert_eq!(map.current_epoch(), 7);
        map.set_committed_epoch(42);
        assert_eq!(map.current_epoch(), 42);
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

    /// item-7: MEET with a peer's REAL announce id COINCIDES with the peer's SELF-added announce
    /// entry (a duplicate id), so it is a no-op -- the table holds ONE entry per node. This is the
    /// invariant the serve-layer "learn the real id on MEET" relies on: once the leader learns and
    /// commits the peer's real id, re-meeting it (or the peer self-adding) does not inflate the
    /// table. Contrast the OLD behaviour, where a host:port-SYNTHESIZED id differs from the announce
    /// id and so APPENDED a duplicate-by-endpoint entry (proven in the dedup test below).
    #[test]
    fn meet_with_real_id_is_idempotent_vs_the_self_added_announce_entry() {
        // The peer (ID1 @ h1:2) self-added its announce entry.
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        assert_eq!(map.known_nodes(), 2);
        // A leader that LEARNED ID1's real announce id commits AddNode{ID1, h1, 2}: idempotent
        // (same id), so NO duplicate -- known_nodes stays 2.
        map.meet(node(ID1, "h1", 2));
        assert_eq!(
            map.known_nodes(),
            2,
            "MEET with the peer's real announce id must not inflate the table"
        );
    }

    /// item-7 (defensive reconciliation): `dedup_nodes_by_endpoint` collapses a SYNTH-id entry and
    /// an ANNOUNCE-id entry that share one physical endpoint into a SINGLE entry, while PRESERVING
    /// routing (owner/replica/migration indices follow the kept entry) and self-recognition.
    #[test]
    fn dedup_nodes_by_endpoint_collapses_synth_and_announce_pair() {
        // A synth id (distinct from any announce id) that a stale MEET would have appended for ID1.
        const SYNTH_FOR_ID1: &str = "5555555555555555555555555555555555555555";

        // self=ID0 @ h0:1. ID1 @ h1:2 is present under BOTH its announce id AND a synth id (the old
        // inflation). ID2 @ h2:3 is a normal single-entry peer.
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2)); // ID1's announce entry (idx 1)
        map.meet(node(SYNTH_FOR_ID1, "h1", 2)); // the synth duplicate of ID1's endpoint (idx 2)
        map.meet(node(ID2, "h2", 3)); // a clean peer (idx 3)
        assert_eq!(
            map.known_nodes(),
            4,
            "the synth duplicate inflated the table"
        );

        // Own a couple of slots and assign cross-node state that REFERENCES the synth entry, so the
        // dedup's index remap is exercised:
        map.add_slots(&[0, 1]).unwrap(); // self owns 0,1
        map.set_slot_node(0, SYNTH_FOR_ID1).unwrap(); // slot 0 owned by the synth entry
        map.set_slot_node(1, ID2).unwrap(); // slot 1 owned by ID2
        map.set_slot_replica(1, SYNTH_FOR_ID1).unwrap(); // slot 1 replicated by the synth entry

        // Reconcile: the synth entry collapses into ID1's announce entry (same endpoint).
        let removed = map.dedup_nodes_by_endpoint();
        assert_eq!(removed, 1, "exactly the one synth duplicate is removed");
        assert_eq!(map.known_nodes(), 3, "one entry per physical node now");

        // The surviving id for h1:2 is the ANNOUNCE id (the kept canonical), and the synth id is
        // gone.
        let nodes = map.nodes();
        assert!(nodes.iter().any(|n| n.id.as_ref() == ID1));
        assert!(
            !nodes.iter().any(|n| n.id.as_ref() == SYNTH_FOR_ID1),
            "the synth id must be gone after dedup"
        );

        // ROUTING PRESERVED: slot 0's owner (was the synth entry) now resolves to ID1's announce id
        // by the remap; slot 1 still owned by ID2 and replicated by ID1's (remapped) entry.
        assert_eq!(map.owner_id(0).unwrap().as_ref(), ID1);
        assert_eq!(map.owner_id(1).unwrap().as_ref(), ID2);
        assert!(
            map.is_replica_of(1, ID1),
            "replica index followed the remap"
        );
        // self_idx still names self (ID0 still owns no remapped change to its own slots semantics;
        // self owned none of the deduped slots, but is_self must still hold).
        assert!(map.is_self(ID0), "self recognition survives the reindex");

        // Idempotent: a second dedup finds nothing to do.
        assert_eq!(map.dedup_nodes_by_endpoint(), 0, "already endpoint-unique");
    }

    /// `dedup_nodes_by_endpoint` keeps SELF as the canonical when self shares an endpoint with a
    /// later synth-id alias (so `self_idx` keeps naming self) and is a no-op on an already-unique
    /// table.
    #[test]
    fn dedup_prefers_self_and_is_noop_when_unique() {
        const SYNTH_FOR_SELF: &str = "6666666666666666666666666666666666666666";
        // self=ID0 @ 127.0.0.1:7000; a synth alias of SELF's own endpoint was appended (e.g. a peer
        // MEET'd this node under a synth id before learning its real id).
        let map = SlotMap::empty_self(ID0, "127.0.0.1", 7000);
        map.meet(node(SYNTH_FOR_SELF, "127.0.0.1", 7000));
        map.meet(node(ID1, "127.0.0.1", 7001));
        assert_eq!(map.known_nodes(), 3);

        let removed = map.dedup_nodes_by_endpoint();
        assert_eq!(removed, 1);
        assert_eq!(map.known_nodes(), 2);
        // self is STILL recognized by its announce id, and the kept entry for 127.0.0.1:7000 is the
        // announce id (self), not the synth alias.
        assert!(map.is_self(ID0));
        let nodes = map.nodes();
        assert!(nodes.iter().any(|n| n.id.as_ref() == ID0));
        assert!(!nodes.iter().any(|n| n.id.as_ref() == SYNTH_FOR_SELF));
        // A clean table is left untouched.
        assert_eq!(map.dedup_nodes_by_endpoint(), 0);
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

    // ----- HA-7d: replica assignment (the new parallel replicas[] structure) -----

    #[test]
    fn set_slot_replica_records_in_parallel_structure_without_touching_ownership() {
        // self=ID0 owns slot 5; ID1 is a known peer assigned as the slot's REPLICA. The owner,
        // the mine[] bitmap, and owns() must be UNAFFECTED by the replica assignment.
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.add_slots(&[5]).unwrap(); // self owns slot 5
        assert!(map.owns(5));
        assert!(map.replicas_of(5).is_empty(), "no replica yet");
        assert!(!map.is_replica_of(5, ID1));

        map.set_slot_replica(5, ID1).unwrap();
        // Ownership is unchanged: self still owns slot 5 (owns() reads only mine[]).
        assert!(map.owns(5), "replica assignment must not change owns()");
        assert_eq!(map.owner_id(5).unwrap().as_ref(), ID0);
        // The replica is recorded in the parallel structure.
        assert!(map.is_replica_of(5, ID1));
        assert!(
            !map.is_replica_of(5, ID0),
            "the owner is not its own replica here"
        );
        let reps = map.replicas_of(5);
        assert_eq!(reps.len(), 1, "MVP single replica per slot");

        // An unknown node id is rejected with the exact error (no mutation).
        let err = map.set_slot_replica(5, ID2).unwrap_err();
        assert_eq!(err, SlotMutError::UnknownNode(ID2.to_owned()));
    }

    #[test]
    fn replicas_empty_by_default_and_owns_unchanged() {
        // The default (no AssignReplica) map has an empty replica set for every slot, and the hot
        // owns() path is byte-identical to the pre-HA-7d behavior (the static-path guarantee).
        let map = three_node();
        for slot in [0u16, 5461, 10922, 16383] {
            assert!(map.replicas_of(slot).is_empty());
            assert!(!map.is_replica_of(slot, ID0));
            assert!(!map.is_replica_of(slot, ID1));
        }
        // owns() still reflects ONLY ownership (ID1 = the middle third).
        assert!(map.owns(5461) && map.owns(10922));
        assert!(!map.owns(0) && !map.owns(16383));
    }

    // ----- HA-8: promotion-side replica clearing (clear_slot_replica) -----

    #[test]
    fn clear_slot_replica_clears_only_the_named_replica_and_is_idempotent() {
        // self=ID0 owns slot 5; ID1 replicates it. PROMOTION flips the owner to ID1 then clears
        // ID1's replica entry (it is the owner now). clear_slot_replica must clear ONLY when the
        // recorded replica is exactly the named node, and be a no-op otherwise + on re-apply.
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.meet(node(ID2, "h2", 3));
        map.add_slots(&[5]).unwrap();
        map.set_slot_replica(5, ID1).unwrap();
        assert!(map.is_replica_of(5, ID1));

        // Clearing a DIFFERENT node's name is a no-op (ID1 is still the replica).
        map.clear_slot_replica(5, ID2);
        assert!(
            map.is_replica_of(5, ID1),
            "clearing a non-matching id is a no-op"
        );

        // The promotion sequence: flip owner to ID1, then clear ID1 from the replica set.
        map.set_slot_node(5, ID1).unwrap();
        map.clear_slot_replica(5, ID1);
        assert_eq!(
            map.owner_id(5).unwrap().as_ref(),
            ID1,
            "ID1 now owns slot 5"
        );
        assert!(
            !map.is_replica_of(5, ID1),
            "the promoted node is no longer recorded as a replica"
        );
        assert!(
            map.replicas_of(5).is_empty(),
            "the replica entry is cleared"
        );

        // Idempotent re-apply: clearing an already-clear entry is harmless.
        map.clear_slot_replica(5, ID1);
        assert!(map.replicas_of(5).is_empty());

        // A clear on an UNASSIGNED-replica slot (slot 6 never had a replica) is a no-op, no lock.
        map.clear_slot_replica(6, ID1);
        assert!(map.replicas_of(6).is_empty());
    }

    #[test]
    fn clear_slot_replica_does_not_touch_ownership_or_owns() {
        // clear_slot_replica must NEVER alter owner[]/mine[]/owns(): it writes only replicas[].
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.add_slots(&[7]).unwrap(); // self (ID0) owns slot 7
        map.set_slot_replica(7, ID1).unwrap();
        assert!(map.owns(7));
        // Clear ID1's replica role on a slot ID0 still owns: ownership is untouched.
        map.clear_slot_replica(7, ID1);
        assert!(map.owns(7), "clear_slot_replica must not change owns()");
        assert_eq!(map.owner_id(7).unwrap().as_ref(), ID0);
        assert!(!map.is_replica_of(7, ID1));
    }

    #[test]
    fn forget_renumbers_and_clears_replicas() {
        // self=ID0 (idx0), ID1 (idx1), ID2 (idx2). ID2 replicates slot 7; ID1 replicates slot 8.
        // FORGET ID1 (slotless owner): ID2 shifts idx2->idx1, slot 7 must STILL resolve to ID2 as
        // its replica; slot 8's replica (the removed ID1) must be CLEARED.
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.meet(node(ID2, "h2", 3));
        map.set_slot_replica(7, ID2).unwrap();
        map.set_slot_replica(8, ID1).unwrap();
        assert!(map.is_replica_of(7, ID2));
        assert!(map.is_replica_of(8, ID1));

        map.forget(ID1).unwrap();
        assert!(
            map.is_replica_of(7, ID2),
            "ID2's replica survives the reindex"
        );
        assert!(
            map.replicas_of(8).is_empty(),
            "the forgotten node's replica entry is cleared"
        );
    }

    // ----- HA-6: online slot migration state (the new parallel migration_state/peer arrays) -----

    #[test]
    fn migration_state_set_and_clear_with_accessors_and_default_none() {
        // self=ID0 owns slot 5; ID1 is a known peer. Default is None for every slot.
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.add_slots(&[5]).unwrap();
        assert_eq!(map.migration_state(5), MigrationState::None);
        assert!(map.migration_peer_endpoint(5).is_none());
        assert!(map.migration_peer_id(5).is_none());

        // SETSLOT 5 MIGRATING ID1: the slot is tagged MIGRATING with ID1 as the dest (ASK target);
        // ownership is UNCHANGED (owns() still true) -- migration state never feeds owns().
        map.set_migrating(5, ID1).unwrap();
        assert_eq!(map.migration_state(5), MigrationState::Migrating);
        assert_eq!(map.migration_peer_endpoint(5), Some(("h1".to_owned(), 2)));
        // The id-typed peer accessor (HA-6 Finding 2: the IMPORTING proposal builder reads this for
        // the dest) names the recorded migration peer.
        assert_eq!(map.migration_peer_id(5).as_deref(), Some(ID1));
        assert!(map.owns(5), "MIGRATING must not change owns()");
        assert_eq!(map.owner_id(5).unwrap().as_ref(), ID0);

        // STABLE clears it back to None (idempotent).
        map.clear_migration(5);
        assert_eq!(map.migration_state(5), MigrationState::None);
        assert!(map.migration_peer_endpoint(5).is_none());
        assert!(map.migration_peer_id(5).is_none());
        map.clear_migration(5); // idempotent no-op
        assert_eq!(map.migration_state(5), MigrationState::None);

        // IMPORTING on a slot this node does NOT own (slot 6, owned by nobody here): tagged
        // IMPORTING with ID1 as the src; owns() stays false.
        map.set_importing(6, ID1).unwrap();
        assert_eq!(map.migration_state(6), MigrationState::Importing);
        assert_eq!(map.migration_peer_endpoint(6), Some(("h1".to_owned(), 2)));
        assert!(!map.owns(6), "IMPORTING must not grant ownership");

        // An unknown dest/src is rejected with the exact error (no mutation).
        let err = map.set_migrating(5, ID2).unwrap_err();
        assert_eq!(err, SlotMutError::UnknownNode(ID2.to_owned()));
        let err = map.set_importing(7, ID2).unwrap_err();
        assert_eq!(err, SlotMutError::UnknownNode(ID2.to_owned()));
    }

    /// THE owns()-INDEPENDENCE invariant (HA-6 FINDING): migration state must NEVER affect owns().
    /// Drive a full migrating/importing/clear sequence and assert owns() reflects ONLY ownership at
    /// every step, and that the default (no migration) is byte-identical to the pre-HA-6 behavior.
    #[test]
    fn migration_state_never_affects_owns() {
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.add_slots(&[0, 1, 2]).unwrap(); // self owns 0,1,2

        // Snapshot owns() across all slots before any migration tag.
        let before: Vec<bool> = (0..CLUSTER_SLOTS).map(|s| map.owns(s)).collect();
        // Tag a self-owned slot MIGRATING, a foreign slot IMPORTING, then clear both.
        map.set_migrating(0, ID1).unwrap();
        map.set_importing(5000, ID1).unwrap();
        let after_tag: Vec<bool> = (0..CLUSTER_SLOTS).map(|s| map.owns(s)).collect();
        assert_eq!(
            before, after_tag,
            "tagging MIGRATING/IMPORTING must not change any owns()"
        );
        map.clear_migration(0);
        map.clear_migration(5000);
        let after_clear: Vec<bool> = (0..CLUSTER_SLOTS).map(|s| map.owns(s)).collect();
        assert_eq!(
            before, after_clear,
            "clearing migration must not change owns()"
        );
    }

    #[test]
    fn set_slot_node_flip_clears_migration_state() {
        // self=ID0 owns slot 5, MIGRATING to ID1. The committed FLIP (set_slot_node -> ID1) must
        // transfer ownership AND clear the migration in one atomic apply (no stale ASK afterward).
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.add_slots(&[5]).unwrap();
        map.set_migrating(5, ID1).unwrap();
        assert_eq!(map.migration_state(5), MigrationState::Migrating);
        assert!(map.owns(5));

        // THE FLIP: ownership moves to ID1; migration state clears in lockstep.
        map.set_slot_node(5, ID1).unwrap();
        assert!(!map.owns(5), "the FLIP transfers ownership away");
        assert_eq!(map.owner_id(5).unwrap().as_ref(), ID1);
        assert_eq!(
            map.migration_state(5),
            MigrationState::None,
            "the FLIP clears the migration state (no stale ASK)"
        );
        assert!(map.migration_peer_endpoint(5).is_none());
    }

    #[test]
    fn forget_renumbers_and_clears_migration_peer() {
        // self=ID0 (idx0), ID1 (idx1), ID2 (idx2). Slot 7 MIGRATING to ID2; slot 8 IMPORTING from
        // ID1. FORGET ID1 (slotless): ID2 shifts idx2->idx1, slot 7's peer must STILL resolve to
        // ID2; slot 8's peer (the removed ID1) must be CLEARED (migration abandoned).
        let map = SlotMap::empty_self(ID0, "h0", 1);
        map.meet(node(ID1, "h1", 2));
        map.meet(node(ID2, "h2", 3));
        map.add_slots(&[7]).unwrap(); // self owns slot 7 (so it can MIGRATE out)
        map.set_migrating(7, ID2).unwrap();
        map.set_importing(8, ID1).unwrap();
        assert_eq!(map.migration_peer_endpoint(7), Some(("h2".to_owned(), 3)));
        assert_eq!(map.migration_peer_endpoint(8), Some(("h1".to_owned(), 2)));

        map.forget(ID1).unwrap();
        assert_eq!(
            map.migration_peer_endpoint(7),
            Some(("h2".to_owned(), 3)),
            "ID2's migration peer survives the reindex"
        );
        assert_eq!(
            map.migration_state(8),
            MigrationState::None,
            "the forgotten peer's migration is cleared"
        );
        assert!(map.migration_peer_endpoint(8).is_none());
    }

    // ----- HA-3c: committed-config serialize / restore (the Raft snapshot SlotMap half) -----

    /// `serialize_committed` -> `restore_committed` round-trips the WHOLE committed view: a map
    /// with peers, owners (incl. a foreign owner), a replica, and a live migration restores into a
    /// fresh `empty_self` of the SAME node to an IDENTICAL committed view (node-relative owns(),
    /// owner ids per slot, replica, migration state + peer, epoch). This is the property the config
    /// state machine's snapshot/restore rests on.
    #[test]
    fn serialize_restore_round_trips_the_committed_view() {
        // Build a rich committed view on node ID0's map.
        let src = SlotMap::empty_self(ID0, "10.0.0.0", 7000);
        src.meet(node(ID1, "10.0.0.1", 7001));
        src.meet(node(ID2, "10.0.0.2", 7002));
        // ID0 owns [0,1,2]; ID1 owns [100]; ID2 owns [200].
        src.set_slot_node(0, ID0).unwrap();
        src.set_slot_node(1, ID0).unwrap();
        src.set_slot_node(2, ID0).unwrap();
        src.set_slot_node(100, ID1).unwrap();
        src.set_slot_node(200, ID2).unwrap();
        // ID1 replicates slot 0; slot 2 is MIGRATING from ID0 -> ID1 (ID0 owns it, so the tag holds).
        src.set_slot_replica(0, ID1).unwrap();
        src.set_migrating(2, ID1).unwrap();
        src.set_committed_epoch(7);

        let bytes = src.serialize_committed();

        // Restore into a FRESH empty_self of the SAME node (keeps ID0's self-identity).
        let dst = SlotMap::empty_self(ID0, "10.0.0.0", 7000);
        dst.restore_committed(&bytes);

        // The committed view is identical: node-relative ownership, owner ids per slot, the
        // replica, the migration, and the epoch.
        assert_eq!(dst.known_nodes(), 3);
        assert!(dst.owns(0) && dst.owns(1) && dst.owns(2));
        assert!(!dst.owns(100) && !dst.owns(200));
        assert_eq!(dst.owner_id(100).as_deref(), Some(ID1));
        assert_eq!(dst.owner_id(200).as_deref(), Some(ID2));
        assert!(dst.is_replica_of(0, ID1));
        assert_eq!(dst.migration_state(2), MigrationState::Migrating);
        assert_eq!(dst.migration_peer_id(2).as_deref(), Some(ID1));
        assert_eq!(dst.current_epoch(), 7);
        // Re-serializing the restored map yields the SAME bytes (a fixed point: the form is a
        // deterministic function of the committed view, so a round-trip is idempotent).
        assert_eq!(dst.serialize_committed(), bytes);
    }

    /// restore_committed REPLACES whatever was in the map: a map already carrying stale committed
    /// state (different owners / replicas / migration) is fully overwritten by the snapshot, so the
    /// restored view matches the snapshot and not the prior state (the install-on-a-lagging-node
    /// case). Self-identity is preserved.
    #[test]
    fn restore_overwrites_prior_committed_state() {
        // Source view: ID1's map where ID0 owns [0], ID1 owns [1].
        let src = SlotMap::empty_self(ID1, "10.0.0.1", 7001);
        src.meet(node(ID0, "10.0.0.0", 7000));
        src.set_slot_node(0, ID0).unwrap();
        src.set_slot_node(1, ID1).unwrap();
        src.set_committed_epoch(3);
        let bytes = src.serialize_committed();

        // Destination starts with DIFFERENT stale committed state (ID1 owns [0,1], a stale peer).
        let dst = SlotMap::empty_self(ID1, "10.0.0.1", 7001);
        dst.meet(node(ID2, "10.0.0.2", 7002));
        dst.set_slot_node(0, ID1).unwrap();
        dst.set_slot_node(1, ID1).unwrap();
        dst.set_slot_node(50, ID2).unwrap();
        dst.set_committed_epoch(99);
        assert!(dst.owns(0));

        dst.restore_committed(&bytes);

        // The stale state is gone; the snapshot view is in place. ID1's self-identity is kept, so
        // owns() is node-relative: ID1 owns [1], ID0 owns [0], slot 50 is unassigned, epoch is 3.
        assert!(
            !dst.owns(0),
            "slot 0 is ID0's in the snapshot, not self (ID1)"
        );
        assert!(dst.owns(1), "slot 1 is self's (ID1) in the snapshot");
        assert_eq!(dst.owner_id(0).as_deref(), Some(ID0));
        assert_eq!(dst.owner_id(50), None, "the stale slot 50 is gone");
        assert_eq!(dst.current_epoch(), 3);
        assert_eq!(dst.serialize_committed(), bytes);
    }

    /// A malformed / truncated buffer (never produced by `serialize_committed`) leaves the map
    /// UNCHANGED rather than panicking (restore is total). FIX 3 made restore decode-first-then-
    /// publish, so a bad snapshot is rejected DURING the decode phase before ANY mutation: a
    /// malformed snapshot can no longer half-wipe a good committed view (strictly safer than the
    /// prior clear-to-baseline behaviour, and an extension of the atomic-restore property).
    #[test]
    fn restore_of_garbage_is_total_and_leaves_the_map_unchanged() {
        let map = SlotMap::empty_self(ID0, "10.0.0.0", 7000);
        map.meet(node(ID1, "10.0.0.1", 7001));
        map.set_slot_node(0, ID0).unwrap();
        map.set_committed_epoch(5);
        // Truncated buffer (a half-written u64 epoch) is rejected before any mutation.
        map.restore_committed(&[1, 2, 3]);
        // The prior good committed view is intact (the bad snapshot did not touch it).
        assert!(
            map.owns(0),
            "the owned slot survives a rejected garbage restore"
        );
        assert_eq!(map.slots_assigned(), 1);
        assert_eq!(map.known_nodes(), 2);
        assert_eq!(map.current_epoch(), 5);
    }

    /// FIX 3: a restore writes each `mine[slot]` ONCE to its FINAL value (compute-then-publish),
    /// never through an all-cleared intermediate, so a concurrent lock-free `owns(slot)` for a
    /// slot that is self's both BEFORE and AFTER the restore can never observe a transient
    /// `false` (the spurious-MOVED window the old clear-then-reset shape had). We cannot easily
    /// catch a sub-microsecond torn read deterministically, but we CAN prove the post-restore
    /// `owns()` view exactly matches the serialized state, and (the structural guarantee) that a
    /// slot owned in both the prior and the restored view stays owned with no API that clears it
    /// first. This ordering test hammers owns() on a stable-owned slot across many restores from
    /// a separate thread and asserts it is NEVER observed false.
    #[test]
    fn restore_never_transiently_clears_a_stably_owned_slot() {
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicBool, Ordering as O};

        // ID0 owns slot 0 in BOTH the source and a second source, so slot 0 is self's across
        // every restore: a correct compute-then-publish never dips owns(0) to false.
        let make_bytes = |peer_owns_extra: u16| -> Vec<u8> {
            let src = SlotMap::empty_self(ID0, "10.0.0.0", 7000);
            src.meet(node(ID1, "10.0.0.1", 7001));
            src.set_slot_node(0, ID0).unwrap(); // slot 0 is ALWAYS self's.
            src.set_slot_node(peer_owns_extra, ID1).unwrap(); // a churning foreign slot.
            src.set_committed_epoch(1);
            src.serialize_committed()
        };
        let bytes_a = make_bytes(100);
        let bytes_b = make_bytes(200);

        let map = StdArc::new(SlotMap::empty_self(ID0, "10.0.0.0", 7000));
        map.restore_committed(&bytes_a); // establish the initial owned view.
        assert!(map.owns(0));

        let stop = StdArc::new(AtomicBool::new(false));
        let saw_false = StdArc::new(AtomicBool::new(false));
        let reader = {
            let map = StdArc::clone(&map);
            let stop = StdArc::clone(&stop);
            let saw_false = StdArc::clone(&saw_false);
            std::thread::spawn(move || {
                while !stop.load(O::Acquire) {
                    if !map.owns(0) {
                        saw_false.store(true, O::Release);
                    }
                }
            })
        };
        // Hammer restores from the writer side; slot 0 must stay owned throughout.
        for i in 0..2000u32 {
            map.restore_committed(if i % 2 == 0 { &bytes_a } else { &bytes_b });
        }
        stop.store(true, O::Release);
        reader.join().expect("reader thread joins");
        assert!(
            !saw_false.load(O::Acquire),
            "owns(0) for a stably-owned slot must NEVER be observed false during a restore"
        );
        // And the final view is exactly the serialized state.
        assert!(map.owns(0));
        assert!(!map.owns(100) && !map.owns(200));
    }
}
