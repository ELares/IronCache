// SPDX-License-Identifier: MIT OR Apache-2.0
//! The concrete per-shard store (HASHTABLE.md #35, OBJECT_LAYOUT.md #111,
//! ADR-0005), implementing the [`ironcache_storage::Store`] narrow waist.
//!
//! [`ShardStore`] holds one unsynchronized `hashbrown::HashMap` per logical
//! database (KEYSPACE.md per-DB keyspace), mapping key bytes to a [`kvobj::KvObj`].
//! The map is owned by exactly one core and touched with no lock, no atomic, and no
//! CAS on the hot path (ADR-0002/0005); `hashbrown`'s power-of-two all-at-once
//! resize is the growth policy (HASHTABLE.md "Growth and rehash"). Per-shard state
//! is held via `&mut self`, so the binary wires it as `Rc<RefCell<ShardStore>>`
//! (the same pattern as the per-shard `Env`).
//!
//! ## Slot partitioning (#570, the bounded-resize tail-latency lever)
//!
//! Each logical DB's keyspace is PARTITIONED into [`DEFAULT_SLOTS_PER_DB`] per-slot
//! tables (`dbs[db]` is a `Vec<HashTable<Entry>>`, one small table per slot), routing
//! every op by [`slot_index`] (a FIXED-SEED key hash, ADR-0003). This is the intended
//! per-slot design (HASHTABLE.md "Growth and rehash"): a `hashbrown` all-at-once resize
//! now rehashes only ONE slot's ~N/S entries, not the DB's whole N, so the worst-case
//! single-insert resize on the serving core is bounded to a small slot (at 1M keys and
//! 256 slots, ~4000 entries instead of 1M, a ~250x smaller p99.9 stall). This is an
//! INTERNAL representation change behind the frozen `Store` waist: no command-layer or
//! waist signature moves. The store-internal slot is DISTINCT from the cluster wire slot
//! (ironcache-cluster's CRC16 16384-space, #70): this is a memory/latency partition of
//! one shard's own tables, not a routing concern.
//!
//! The slot count is a MEMORY/latency tradeoff, so it is a config knob with a safe
//! default (the tunability tenet): more slots shrink the resize unit but add a fixed
//! per-DB `Vec` cost. To keep that cost off unused DBs, a DB's slot tables are allocated
//! LAZILY on its first write (an untouched DB carries an empty `Vec`, no slot tables), so
//! a typical single-DB workload pays for one DB's slots, not all `databases` of them.
//!
//! ## Determinism and time (ADR-0003)
//!
//! The store reads no clock: `now: UnixMillis` is passed in by the caller. The
//! lazy expiry-on-read backstop (EXPIRATION.md) lives in every read path here: an
//! entry whose deadline has strictly passed (`now > expire_at`, the Valkey
//! boundary; alive at `now == expire_at`) is removed and reported as absent.

// `#![forbid(unsafe_code)]` is LIFTED for the tagged-pointer `Entry` representation
// (the per-shard table slot, kvobj.rs): `Entry` is a single 8-byte `NonNull<u8>`
// tagged pointer (low bit 0 = a manually-allocated Str thin blob, low bit 1 = a
// `Box<CollEntry>`), which halves the table slot from 16 to 8 bytes. `unsafe` is
// AUTHORIZED and is CONFINED to the one heavily-documented `Entry` impl in kvobj.rs;
// every `unsafe` block there carries a `// SAFETY:` justification. Everything else in
// this crate stays safe. `deny(unsafe_op_in_unsafe_fn)` keeps each unsafe operation
// inside an explicit `unsafe {}` block (no implicit-unsafe-body) so the SAFETY
// comments sit on the actual operations.
#![deny(unsafe_op_in_unsafe_fn)]

pub mod encoding;
pub mod kvobj;

/// Re-export the per-shard table value type at the crate root so the store-public
/// [`WriteObserver`] trait (HA-5a) can name its `on_put` post-image argument with a
/// root-accessible path and external implementors (serve/repl) can reference the same
/// type. Purely additive: `kvobj::Entry` is unchanged and still reachable by its module
/// path.
pub use kvobj::Entry;
/// Re-export [`kvobj::KvObj`] at the crate root: the public, owned builder/transfer type
/// the forkless SNAPSHOT iterator (HA-5b #60) yields per entry and a replica replays via
/// [`ShardStore::insert_object`]. Purely additive.
pub use kvobj::KvObj;

use bytes::Bytes;
use hashbrown::hash_map::Entry as WatchMapEntry;
use hashbrown::{DefaultHashBuilder, HashMap, HashSet};

// THE INDEX BACKEND SEAM (#285 Stage 3). The per-slot INDEX table is the ONLY hashbrown
// role these cfg'd imports swap: the `HashData::HashTable` VALUE encoding (with its
// user-visible OBJECT ENCODING name) and the WATCH map above stay hashbrown under either
// backend, which is why the hashbrown dependency itself is unconditional. Every index call
// site below is written against the shared API surface (find / find_mut / entry /
// find_entry / iter / len / is_empty / reserve / Clone / Debug -- see
// ironcache-dashtable's index module doc, whose parity the oracle suite proves), so the
// backend choice is THESE IMPORTS plus nothing else.
//
// Default (no `dashtable` feature): hashbrown's SIMD-probed Swiss table, the shipping
// backend. With `--features dashtable`: the Dash extendible-hashing index (segment-at-a-
// time growth, no power-of-two doubling trough), the #285 memory bet under evaluation --
// NOT production-ready until the DASHTABLE.md stage-4 head-to-heads prove the memory win
// with no speed regression.
#[cfg(not(feature = "dashtable"))]
use hashbrown::{HashTable, hash_table::Entry as TableEntry};
#[cfg(feature = "dashtable")]
use ironcache_dashtable::index::{DashIndex as HashTable, Entry as TableEntry};
use ironcache_eviction::{EvictionPolicy, Policy, VictimStrategy, map_policy_name};
use ironcache_storage::{
    AccountingHook, CountingAccounting, DataType, EncodingThresholds, EvictionHook, ExpireWrite,
    Keyspace, MoveMode, MoveOutcome, NewValue, NullEviction, OccupiedEntry, OccupiedEntryMut,
    RmwAction, RmwEntry, RmwStep, ScanCursor, Store, UnixMillis, ValueRef, VictimFreq, WatchEntry,
};
use std::hash::BuildHasher;
use std::sync::Arc;

/// The FIXED-SEED stable key hash that the SCAN cursor iterates in ascending order
/// (KEYSPACE.md "the full hash recomputed from the embedded key"). It is a small
/// dedicated wyhash-style mix over the raw key bytes (ADR-0003 determinism):
///
/// - It is RECOMPUTABLE from the key bytes alone (the cursor encodes a position in
///   this hash's order, and a resume re-derives every key's hash), so the iteration
///   order is stable across calls and processes.
/// - It is NOT `hashbrown`'s per-table hasher (a `RandomState` / per-table tag, which
///   differs run-to-run and would break a multi-call SCAN), and NOT `std` `rand`.
/// - Because the value depends ONLY on the key bytes, it is INVARIANT across a
///   `hashbrown` all-at-once resize: a resize moves entries to new buckets but does
///   not change any key's `scan_hash`, so a SCAN that spans a resize still visits
///   every key present throughout in the same total order (the rehash-tolerance
///   guarantee KEYSPACE.md mandates; reverse-binary bucket iteration is rejected
///   there precisely because it is NOT resize-invariant for an all-at-once table).
///
/// The mix is the wyhash final-mix (a fixed 64-bit secret, deterministic, public
/// domain), folded byte-by-byte so it is collision-resistant enough to spread keys
/// across the 64-bit order while staying allocation-free and branch-light.
#[must_use]
pub fn scan_hash(key: &[u8]) -> u64 {
    // wyhash-style: a fixed seed, then a per-byte fold through a 64-bit multiply-xor
    // mix. Fully determined by the key bytes (no table state, no OS entropy).
    const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
    const SECRET: u64 = 0xA076_1D64_78BD_642F;
    let mut h: u64 = SEED ^ SECRET;
    for &b in key {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3); // FNV-1a prime spread
        h ^= h >> 33;
    }
    // Final avalanche (splitmix64 finalizer) so close keys land far apart in the order.
    h = h.wrapping_add(0x9E37_79B9_7F4A_7C15);
    h = (h ^ (h >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^ (h >> 31)
}

/// The default number of per-slot tables each database is partitioned into (#570). A
/// MODEST power-of-two count: the per-insert resize unit is bounded to ~N/S entries, so
/// at 1M keys a resize rehashes ~4000 entries (~80us) instead of ~1M (~6ms), a ~250x
/// smaller p99.9 stall, at a small fixed per-DB `Vec` cost (S empty [`HashTable`]s, each
/// a few words with NO bucket allocation until first insert). 256 balances the tail-
/// latency win against that fixed cost so the perf-gate bytes-per-key does not regress
/// (BENCHMARK.md #8). Overridable per the tunability tenet via
/// [`ShardStore::with_slots_per_db`] (a boot/config knob); the value is rounded UP to a
/// power of two there so routing is a mask.
pub const DEFAULT_SLOTS_PER_DB: usize = 256;

/// The per-DB partition slot for `key` given `slots` (a power of two): the FIXED-SEED,
/// key-derived [`scan_hash`] masked to the slot count. It is DELIBERATELY the fixed-seed
/// hash, NOT `hashbrown`'s per-run table hasher, so slot routing is DETERMINISTIC run-to-
/// run (ADR-0003: two shards with identical keyspaces partition identically, and the
/// physical resize timing is reproducible). Because [`scan_hash`] is well-avalanched,
/// masking its low bits spreads keys evenly across the slots, keeping every slot near
/// the ~N/S average so no single slot's resize approaches the DB's whole N. This is the
/// STORE-INTERNAL slot, separate from the cluster wire slot (ironcache-cluster CRC16).
#[inline]
#[must_use]
fn slot_index(key: &[u8], slots: usize) -> usize {
    // `slots` is a power of two (enforced at construction), so `& (slots - 1)` is the
    // low-bit mask. A single slot (`slots == 1`) masks to 0 (one table, the pre-#570
    // behavior).
    (scan_hash(key) as usize) & (slots - 1)
}

/// The result of the pure SCAN cursor-stepping core ([`scan_plan`]): which sorted
/// keys to EXAMINE this batch (the borrow lives as long as the input `order`) and the
/// next [`ScanCursor`] to return.
struct ScanPlan<'a> {
    /// The keys to examine this batch, in ascending (hash, bytes) order.
    examined: Vec<&'a [u8]>,
    /// The next cursor: `ScanCursor(0)` means the iteration is complete; otherwise the
    /// `scan_hash` of the FIRST not-yet-examined key (the resume threshold).
    next: ScanCursor,
}

/// The PURE SCAN cursor-stepping core over a pre-sorted `(scan_hash, key_bytes)`
/// slice (KEYSPACE.md cursor-stability contract). Separated from the store so it can
/// be unit-tested with HAND-CRAFTED hashes, including a FORCED equal-hash collision
/// (two distinct keys sharing a 64-bit hash), without inverting [`scan_hash`].
///
/// ## The cursor invariant (and why a non-terminal cursor is never 0)
///
/// The cursor is the `scan_hash` of the NEXT not-yet-examined key (the resume
/// THRESHOLD), with `>=` resume semantics; `ScanCursor(0)` is reserved for "complete".
/// An equal-hash GROUP (all keys sharing one hash) is NEVER split across two calls:
/// once a batch reaches its `count` budget, it keeps examining until the hash CHANGES,
/// then stops at the group boundary. So the next cursor is always the hash at the
/// START of a fresh, fully-un-examined group, and resuming at `hash >= cursor` returns
/// every key of that group (the equal-hash discriminator is re-derived from the group
/// boundary, needing no extra cursor field).
///
/// A non-terminal cursor is NEVER `0`: a key whose `scan_hash` is 0 sorts FIRST, so it
/// is examined on the start batch (cursor 0 == start) and the next un-examined key
/// then has a strictly greater hash. Thus a returned `ScanCursor(0)` unambiguously
/// means complete, never "resume from the 0-hash key".
///
/// `count` bounds the keys EXAMINED (a hint, like Redis); `count == 0` is treated as
/// 1 so progress is always made.
///
/// ## `band_bits`: BAND-ALIGNING the next cursor for the cross-shard composite cursor
///
/// `band_bits` is the number of LOW hash bits the cross-shard composite SCAN cursor
/// CANNOT carry (it reserves them for the high shard-index field; see
/// [`ScanCursor::SHARD_BITS`]). It is `0` on a single-shard server (the inner cursor
/// passes through verbatim) and [`ScanCursor::SHARD_BITS`] when more than one shard is
/// configured. When `band_bits > 0` the next cursor is rounded DOWN to its `2^band_bits`
/// BAND FLOOR and a band is NEVER split across calls, so the composite cursor's truncating
/// `inner >> band_bits` encode is LOSSLESS (the cleared low bits are already 0) and the
/// cross-shard wire cursor strictly advances by at least one band -> termination. The
/// inclusive `scan_hash >= cursor` resume re-includes the whole band start, so a band
/// floor never skips an un-examined key.
///
/// With `band_bits == 0` (single shard) this is BYTE-IDENTICAL to the prior group-only
/// logic: the stop rule degenerates to "hash changed" and the next cursor is the exact
/// first un-examined hash, so single-shard SCAN tokens are unchanged.
fn scan_plan<'a>(
    order: &[(u64, &'a [u8])],
    cursor: ScanCursor,
    count: usize,
    band_bits: u32,
) -> ScanPlan<'a> {
    let total = order.len();
    // The resume position: the first key whose hash is >= the cursor. For the start
    // cursor (0) that is index 0. Because a group/band is never split, `start` always
    // lands on a group/band boundary, so `>=` returns the whole resumed group/band.
    let start = if cursor.is_start() {
        0
    } else {
        order.partition_point(|&(h, _)| h < cursor.0)
    };
    if start >= total {
        return ScanPlan {
            examined: Vec::new(),
            next: ScanCursor::START,
        };
    }

    // The BAND of a hash: with band_bits==0 a band IS the exact hash (today's group
    // boundary); with band_bits>0 it is the hash with its low band_bits bits cleared, so
    // all hashes in one 2^band_bits window share a band. `>> band_bits` is the band id
    // (a u32 shift of 0 is the identity, so band_bits==0 yields the raw hash).
    let band = |h: u64| h >> band_bits;

    let count = count.max(1);
    let mut examined: Vec<&'a [u8]> = Vec::new();
    let mut i = start;
    let mut n = 0usize;
    while i < total {
        let (h, key) = order[i];
        // Stop once the per-call budget is spent AND we are at a BAND boundary (the band
        // differs from the last examined key), so a band (and, with band_bits==0, an
        // equal-hash group) is never split across two calls.
        if n >= count && i > start && band(h) != band(order[i - 1].0) {
            break;
        }
        examined.push(key);
        n += 1;
        i += 1;
    }

    // The next cursor: 0 (complete) if we consumed the whole order; otherwise the BAND
    // FLOOR of the first un-examined key. The floor is `(hash >> band_bits) << band_bits`
    // (with band_bits==0 it is the exact hash, today's behavior). The floor is always a
    // strictly-greater band start than the prior batch's last band, never inside it. A
    // non-terminal band floor for a non-start position is never 0 because a 0-band key
    // sorts FIRST and is examined on the start batch (see the cursor invariant doc).
    let next = if i >= total {
        ScanCursor::START
    } else {
        ScanCursor(band(order[i].0) << band_bits)
    };
    ScanPlan { examined, next }
}

/// The per-shard store: one `hashbrown::HashMap` per logical database, plus the
/// eviction and accounting hooks fired from inside the primitives.
///
/// Generic over the hook types so PR-3 can swap in the real S3-FIFO eviction and
/// the jemalloc accounting without touching the waist; PR-2a defaults to
/// [`NullEviction`] and [`CountingAccounting`].
#[derive(Debug)]
pub struct ShardStore<E: EvictionHook = NullEviction, A: AccountingHook = CountingAccounting> {
    /// Per-database, per-SLOT SwissTables (#570), each storing a single-allocation
    /// [`Entry`] per key (memory Round 3). `dbs[db]` is the keyspace for `SELECT db`,
    /// PARTITIONED into [`Self::slots`] per-slot tables so a `hashbrown` all-at-once
    /// resize rehashes only one slot's ~N/S entries, not the DB's whole N (the bounded-
    /// resize tail-latency lever). `dbs[db][slot_index(key, slots)]` is the table for a
    /// key. Unlike a `HashMap<Box<[u8]>, _>`, the low-level [`HashTable`] stores ONLY the
    /// entry and derives the key from inside it ([`Entry::key`]), so there is no separate
    /// map key allocation and no key duplication. Lookups hash the probe key with
    /// [`Self::hasher`] and pass the hash + an eq closure to `find`/`find_entry`/`entry`.
    ///
    /// LAZY per-DB: `dbs[db]` is an EMPTY `Vec` until the DB's first write, when it is
    /// filled with `slots` empty tables ([`Self::slot_table_mut`]). An untouched DB
    /// therefore carries no slot tables, so a single-DB workload pays for one DB's slot
    /// `Vec`, not all `databases` of them. Empty tables allocate no bucket array, so the
    /// per-DB fixed cost is `slots` `HashTable` structs plus the `Vec` (a few KB), not
    /// per-key state.
    ///
    /// ## Per-slot `Arc` copy-on-write snapshot isolation (#576)
    ///
    /// Each slot table is held behind an [`Arc`] so a SAVE can FREEZE the whole shard in
    /// O(slots) atomic-refcount bumps ([`Self::begin_save`]) and hand the frozen slot
    /// tables to a dedicated persist thread that reads them OFF the serving core, with NO
    /// O(N) serving-side copy. The datapath stays uncontended:
    /// - GET reads through the `Arc` as a SHARED deref ([`Self::slot_table`]) -- no
    ///   `Arc::get_mut`, no atomic on the read path (the freq bump goes through the
    ///   interior-mutable [`Entry::bump_freq_shared`], gated OFF while a save holds a
    ///   frozen clone, see [`Self::saving`], so no datapath thread ever mutates a shared
    ///   frozen pointee -- the soundness crux).
    /// - A write takes `Arc::make_mut` ([`Self::slot_table_mut`]): the COMMON (not-saving)
    ///   case is `strong_count == 1`, an uncontended in-place mutate; while a save holds a
    ///   frozen clone of this slot (`strong_count > 1`) the FIRST write DEEP-CLONES the
    ///   slot's entries into a fresh table (`HashTable<Entry>: Clone` clones each `Entry`,
    ///   and `Entry`'s `Clone` is DEEP -- a new pointee allocation, see its impl), swaps
    ///   the live `Arc` to the fresh copy, and leaves the frozen `Arc` owning the ORIGINAL
    ///   entries. So a write during a save is NEVER visible in the concurrent dump and
    ///   NEVER mutates/frees a pointee the persist thread is reading (no data race, no
    ///   use-after-free). Later writes to the same slot see `strong_count == 1` again
    ///   (one-time COW per written slot).
    dbs: Vec<Vec<Arc<HashTable<Entry>>>>,
    /// The per-DB slot count (a power of two, [`DEFAULT_SLOTS_PER_DB`] by default, set at
    /// construction via [`Self::with_slots_per_db`]). Fixed for the store's life: slot
    /// routing must be stable, so this is chosen once at boot (a restart-required config
    /// knob) and never changes while keys are resident. A power of two so [`slot_index`]
    /// routes with a mask.
    slots: usize,
    /// The fixed per-store hasher used for EVERY key hash fed to the [`HashTable`]
    /// explicit-hash API (the table stores no hasher of its own). One `RandomState`
    /// instance shared across all dbs so a key hashes identically regardless of which db
    /// it lands in; constructed once at boot. This is `hashbrown`'s default
    /// (`foldhash`), the same hasher the prior `HashMap` used internally. It is NOT the
    /// SCAN order hash ([`scan_hash`], a fixed-seed stable hash); this one only needs to
    /// be a good table hash and may vary run-to-run.
    hasher: DefaultHashBuilder,
    /// The eviction policy hook (no-op in PR-2a).
    eviction: E,
    /// The accounting hook (logical-byte counter in PR-2a). It is fed the same
    /// add/sub deltas as [`Self::used`] so a PR-3 hook (jemalloc) sees every
    /// insert/remove; the frozen [`AccountingHook`] trait is add/sub-only, so the
    /// running total `used_memory()` returns is mirrored in [`Self::used`] rather
    /// than read back out of the hook.
    accounting: A,
    /// The running logical-byte total (what `used_memory()` returns in PR-2a). Kept
    /// in lockstep with the accounting hook's add/sub deltas so the read is O(1).
    /// PR-2b swaps `used_memory()` to the jemalloc `stats.allocated` mallctl.
    used: u64,
    /// The count of keys reaped by the LAZY expiry-on-read backstop since the last
    /// drain (PR-3b INFO `expired_keys`, the lazy-path signal). The serve loop drains
    /// it with [`Self::take_lazy_expired`] after each command and folds it into the
    /// shard's `expired_keys` counter, so the lazy path contributes to `expired_keys`
    /// alongside the active timing-wheel drain. Not a waist concept (it is an
    /// introspection accumulator on the concrete store), so it adds no primitive
    /// signature.
    lazy_expired: u64,
    /// The WATCH per-key version slots (TRANSACTIONS.md per-key dirty-CAS, PR-10b).
    /// Keyed by `(db, key)`; a slot exists ONLY while at least one connection watches
    /// the key (created on WATCH, dropped on the last UNWATCH). The write funnel bumps
    /// the watched key's `version` so a WATCH snapshot taken earlier reads as dirty.
    /// Plain field, single-thread per shard (no std::sync, no atomics, ADR-0002/0005).
    watch_versions: HashMap<(u32, Box<[u8]>), WatchSlot>,
    /// The monotonically-increasing per-shard version clock (a u64 COUNTER, NOT a clock
    /// or RNG: deterministic, ADR-0003). Each notify of a watched key bumps it and
    /// stamps the key's slot, so distinct writes get strictly-increasing versions and a
    /// stale snapshot's version never accidentally re-matches.
    version_clock: u64,
    /// The FAST-PATH gate for the write-funnel notify: the number of `(db, key)` slots
    /// currently watched (== `watch_versions.len()`, tracked alongside so the funnel can
    /// branch on a plain integer with NO hash probe). When `0` (the overwhelming common
    /// case, no WATCH active) the funnel notify does a single integer check and returns;
    /// the non-watching hot path pays ~one branch.
    watched_count: usize,
    /// The number of LOW `scan_hash` bits the cross-shard composite SCAN cursor reserves
    /// for the shard index, so [`Self::scan_step`] returns BAND-ALIGNED next cursors that
    /// the coordinator's `compose`/`decompose` round-trips LOSSLESSLY (COORDINATOR.md
    /// #107). It is `0` on a single-shard server (`scan_step` is then byte-identical to
    /// the pre-coordinator behavior: the exact next-key hash, no band rounding) and
    /// [`ScanCursor::SHARD_BITS`] when more than one shard is configured. Set ONCE at
    /// construction from the boot shard count; the store reads no shard topology otherwise.
    scan_band_bits: u32,
    /// The cache-mode AMORTIZED-EVICTION POOL: a small BOUNDED, TRANSIENT staging buffer
    /// of victim candidates harvested by ONE table scan ([`Self::refill_evict_pool`]) and
    /// consumed across MANY eviction episodes ([`Self::evict_to_fit_pooled`]), so the O(N)
    /// scan is amortized over up to [`EVICT_POOL_CAP`] evictions (EVICTION.md, Redis
    /// evict.c eviction pool). It is BOUNDED by `EVICT_POOL_CAP` and so is NOT per-key
    /// state: it never grows with the keyspace, preserving the zero-per-key-state memory
    /// property of the cache-mode policy. It is stored HOTTEST-LAST (sorted so the COLDEST
    /// victim is at the BACK), so an eviction episode takes the coldest with an O(1)
    /// `pop` (never `remove(0)`). Empty at rest and between refills; only the cache-mode
    /// (`TableScanLowestFreq`) path ever touches it (the roster path is unchanged).
    evict_pool: Vec<PoolCandidate>,
    /// The OPTIONAL data-plane WRITE OBSERVER (HA-5a): a background sink that observes
    /// EVERY applied write to this shard (create/overwrite/in-place edit/TTL change ->
    /// [`WriteObserver::on_put`]; delete/expiry/flush/eviction -> [`WriteObserver::on_remove`]).
    /// It is the FOUNDATION reused by replication (HA-7) and migration (HA-6): the
    /// observer turns the store's in-process write funnel into a replayable write stream.
    ///
    /// It is a DYNAMIC, shard-local, single-threaded field (the shard is owned by one core
    /// and touched as `&mut self`, so the observer is called `&mut`, with NO lock and NO
    /// atomic, ADR-0002/0005). It is `None` by default; serve/repl installs one later via
    /// [`Self::set_write_observer`]. Adding it does NOT reopen the frozen `Store` waist:
    /// it is store-internal, fired from the same write-funnel attach points the funnel doc
    /// reserves for the OnWrite hook (STORAGE_API.md).
    repl_observer: Option<Box<dyn WriteObserver>>,
    /// The FAST-PATH gate for the write-observer fire (the exact sibling of
    /// [`Self::watched_count`]). `false` by default (no observer installed: the static
    /// datastore path and the raft control plane never observe), in which case EACH write
    /// funnel site does a SINGLE bool test and proceeds with NO further work, NO box
    /// dereference, and NO observer call: the same cost profile as the shipped
    /// `watched_count == 0` WATCH gate. Set `true` only while an observer is installed
    /// (kept in lockstep with `repl_observer.is_some()`), so the funnel can branch on a
    /// plain bool with no `Option` probe on the non-observing hot path.
    repl_active: bool,
    /// PASSIVE-REPLICA mode (HA-7d, CARRY-FORWARD 2): when `true`, this shard is a replica
    /// that mirrors a primary, so it removes keys ONLY from the replication stream (the
    /// primary's `StreamDel`), never on its own. A due key is reported as logically expired
    /// (absent) on read but is NOT physically removed, matching real-Redis replica semantics:
    /// physical removal waits for the master's DEL, so the replica stays in lockstep with the
    /// primary and does not pre-empt the primary's expiry or double-count `expired_keys`.
    /// Default `false`; the only mutator is [`Self::set_passive`].
    passive: bool,
    /// The LIVE collection-encoding thresholds snapshot (#40, the runtime `*-max-listpack-*` /
    /// `set-max-intset-entries`). The store reads these at the encoding-transition decision (via the
    /// typed `OccupiedEntryMut` it hands the closure, and when materializing a `RmwAction::Insert`
    /// create-on-missing collection), so a `CONFIG SET` of a threshold reaches FUTURE inserts. A
    /// plain `Copy` field (the shard is single-threaded, ADR-0002/0005: no lock/atomic per edit);
    /// the dispatch refreshes it via [`Self::set_encoding_thresholds`] only when the runtime config
    /// generation moves (the same per-command generation check the eviction-policy hot-swap rides),
    /// so the per-edit cost at the default is a single `Copy` read, byte-identical to the prior
    /// compiled-constant behavior. Default [`EncodingThresholds::defaults`] so a store built without
    /// an explicit snapshot (every existing test fixture) behaves exactly as the compiled defaults.
    encoding_thresholds: EncodingThresholds,
    /// SAVE-IN-PROGRESS flag (#576 per-slot Arc-COW): `true` for exactly the window a
    /// [`Self::begin_save`] freeze is outstanding (a persist thread holds `Arc` clones of
    /// this shard's slot tables), cleared by [`Self::end_save`] once that thread has
    /// finished reading + dropped its frozen clones. It gates ONE thing on the datapath:
    /// the interior-mutable S3-FIFO freq bump ([`Entry::bump_freq_shared`]) on the GET /
    /// rmw READ path, which is the ONLY datapath write that reaches a stored pointee
    /// through a SHARED `&Entry` (every other write funnels through `Arc::make_mut`, which
    /// COW-copies a frozen slot first). While a save holds a frozen clone, a freq bump on
    /// a not-yet-COW'd slot would mutate a byte the persist thread is concurrently reading
    /// (a data race), so it is SKIPPED for the save window -- a tiny, bounded S3-FIFO
    /// fidelity loss (the freq stays put; eviction is unaffected outside the short save)
    /// traded for snapshot-read soundness. A plain `bool` (NOT an atomic): the shard is
    /// single-threaded (ADR-0002/0005), set/cleared through `&mut self` on the owning core
    /// and read through the same core's read path; the persist thread NEVER touches it.
    saving: bool,
}

/// The data-plane WRITE OBSERVER (HA-5a): a background sink the per-shard store calls on
/// EVERY applied write, so a replication (HA-7) or migration (HA-6) task can mirror the
/// shard's mutations as a replayable stream. It is the hot-path-safe observation seam: it
/// is installed via [`ShardStore::set_write_observer`] and is gated OFF by default (the
/// non-observing path pays at most one bool branch, see [`ShardStore::repl_active`]).
///
/// The store calls the observer AFTER the table mutation has been applied, so each call
/// carries the POST-IMAGE (the committed new state), which is what a replication stream
/// replays to reconstruct the write:
///
/// - [`Self::on_put`] fires for a create / overwrite / in-place collection edit / TTL-only
///   change, with `new` borrowing the COMMITTED post-write [`Entry`] (its value bytes,
///   data type, encoding and deadline are all readable through the entry's accessors).
/// - [`Self::on_remove`] fires for a delete / expiry (lazy backstop + active reaper) /
///   FLUSHDB / FLUSHALL / eviction / a RENAME's source removal / an in-place edit that
///   drains a collection to empty, with the `(db, key)` that left the keyspace.
///
/// SINGLE-THREADED: the store is shard-local and owned by one core, so the observer is
/// called `&mut self` with no synchronization (ADR-0002/0005). DETERMINISM (ADR-0003): the
/// store passes the observer no clock and no RNG; the observer sees only the `(db, key)`
/// and the post-image, and any timestamp it needs comes from the caller's `now`, never the
/// store. An implementor MUST NOT block or panic on the hot path (it runs inline on the
/// owning core); the intended shape is an enqueue onto a shard-local ring the background
/// repl/migration task drains.
///
/// Requires [`std::fmt::Debug`] so the boxed observer fits the `#[derive(Debug)]` on
/// [`ShardStore`] (the same reason the store's hook generics are `Debug`); a trivial
/// `#[derive(Debug)]` on the implementor satisfies it.
pub trait WriteObserver: std::fmt::Debug {
    /// A key was created or overwritten (or edited in place, or had its TTL changed):
    /// `new` is the POST-IMAGE, the committed new [`Entry`] as it now sits in the table.
    /// Read the value bytes via [`Entry::str_value_bytes`], the type via
    /// [`Entry::data_type`], the encoding via [`Entry::encoding`], and the deadline via
    /// [`Entry::expire_at`] (the post-image carries everything needed to reconstruct the
    /// write). Fired AFTER the table mutation, so `new` is the value a replica would store.
    fn on_put(&mut self, db: u32, key: &[u8], new: &Entry);
    /// A key was removed from `db` (an explicit delete, a lazy/active expiry, a FLUSHDB /
    /// FLUSHALL, an eviction, a RENAME's source removal, or an in-place edit that emptied a
    /// collection). Fired AFTER the entry has left the table.
    fn on_remove(&mut self, db: u32, key: &[u8]);
}

/// The store-side [`VictimFreq`] the eviction policy reads through during
/// `select_victim` (freq-in-object). It borrows the store's per-DB tables + the table
/// hasher so it can look up the live entry for a candidate `(db, key)` and read or
/// decrement its 2-bit S3-FIFO promote frequency (which now lives ON the entry).
///
/// `evict_to_fit` SPLITS the `ShardStore` borrow (`&mut self.eviction` plus this over
/// `&mut self.dbs`) so the policy can read the freq out of the tables WITHOUT a
/// double-borrow of `self`. It mirrors `db_index`'s clamp (the command layer validates
/// the db range upstream; the clamp is the same defensive backstop).
struct TableVictimFreq<'a> {
    dbs: &'a mut [Vec<Arc<HashTable<Entry>>>],
    hasher: &'a DefaultHashBuilder,
    /// The per-DB slot count (a copy of [`ShardStore::slots`]) so this can route a probe
    /// key to its slot table exactly as the store does.
    slots: usize,
}

impl TableVictimFreq<'_> {
    /// The clamped map index for `db` (same backstop as [`ShardStore::db_index`]).
    fn db_index(&self, db: u32) -> usize {
        (db as usize).min(self.dbs.len().saturating_sub(1))
    }
}

impl VictimFreq for TableVictimFreq<'_> {
    fn get(&self, db: u32, key: &[u8]) -> Option<u8> {
        let db_idx = self.db_index(db);
        let h = self.hasher.hash_one(key);
        // Route to the key's slot table; an untouched DB (empty slot `Vec`) yields None.
        self.dbs
            .get(db_idx)?
            .get(slot_index(key, self.slots))?
            .find(h, |e| e.key() == key)
            .map(Entry::freq)
    }

    fn dec(&mut self, db: u32, key: &[u8]) {
        let db_idx = self.db_index(db);
        let h = self.hasher.hash_one(key);
        let slot = slot_index(key, self.slots);
        // A second-chance freq decrement is a MUTATION, so it takes `Arc::make_mut` (COW):
        // during a save this deep-clones a still-frozen slot before writing (so the persist
        // thread's frozen view is untouched), then decrements the fresh copy; outside a save
        // it is an uncontended in-place `&mut` (strong_count == 1).
        if let Some(obj) = self
            .dbs
            .get_mut(db_idx)
            .and_then(|d| d.get_mut(slot))
            .map(Arc::make_mut)
            .and_then(|t| t.find_mut(h, |e| e.key() == key))
        {
            obj.dec_freq();
        }
    }
}

/// The cache-mode eviction-pool size: the small BOUNDED set of victim candidates one
/// table scan harvests, then consumes across many eviction episodes (EVICTION.md
/// amortized cache-mode eviction). It is the SCAN AMORTIZATION FACTOR: one O(N) refill
/// scan is amortized over up to `EVICT_POOL_CAP` evictions, so a cache pinned at
/// capacity pays the O(N) scan once per `EVICT_POOL_CAP` evictions instead of once per
/// eviction episode. This is Redis's eviction-pool idea (evict.c `EVPOOL_SIZE`); 64 is
/// large enough to amortize the scan well while staying tiny relative to any real
/// keyspace (so the pool is never per-key state, see [`PoolCandidate`]).
const EVICT_POOL_CAP: usize = 64;

/// One harvested cache-mode eviction candidate, held in the BOUNDED, TRANSIENT
/// [`ShardStore::evict_pool`]. This is NOT per-key state: the pool holds at most
/// [`EVICT_POOL_CAP`] entries no matter how large the keyspace grows, so it does not
/// scale with N and preserves the zero-per-key-state memory property of the cache-mode
/// policy (the eviction policy itself still holds nothing per key; the 2-bit access
/// `freq` lives ON the stored object). The pool is a short-lived staging buffer for
/// the coldest victims a refill scan found, drained as eviction episodes consume it and
/// re-validated against the live table on every pop (a pooled key may have been
/// deleted / overwritten / expired / persisted since the refill).
///
/// The fields are exactly the cache-mode total-order keys (EVICTION.md, ADR-0003):
/// `freq` (the LFU primary key), the `scan_hash` of `key`, the raw `key` bytes, and the
/// `db` final tie-break, so the pool can be ordered by the SAME deterministic total
/// order the full-scan path used.
#[derive(Debug)]
struct PoolCandidate {
    /// The candidate's logical db (the final, total-order tie-break).
    db: u32,
    /// The candidate key bytes (owned: the table entry it was read from may be gone or
    /// moved by a resize by the time the pool is consumed).
    key: Box<[u8]>,
    /// The in-object 2-bit access frequency at refill time (the LFU sort primary key).
    freq: u8,
}

/// One WATCH per-key version slot (TRANSACTIONS.md per-key dirty-CAS, PR-10b). Held in
/// [`ShardStore::watch_versions`] only while the key is watched.
#[derive(Debug, Clone, Copy)]
struct WatchSlot {
    /// The key's current version. Bumped to the shard `version_clock` on every write to
    /// the key while it is watched (the notify on the funnel).
    version: u64,
    /// How many connections currently watch this `(db, key)`. The slot is dropped when
    /// this reaches zero (the last UNWATCH / EXEC / DISCARD / RESET / connection close).
    watchers: u32,
}

impl ShardStore<NullEviction, CountingAccounting> {
    /// A store with `databases` logical DBs and the PR-2a default hooks (no-op
    /// eviction, logical-byte accounting).
    #[must_use]
    pub fn new(databases: u32) -> Self {
        ShardStore::with_hooks(databases, NullEviction, CountingAccounting::new())
    }
}

impl<E: EvictionHook, A: AccountingHook> ShardStore<E, A> {
    /// A store with explicit hooks (PR-3 supplies the real S3-FIFO/jemalloc hooks).
    pub fn with_hooks(databases: u32, eviction: E, accounting: A) -> Self {
        let n = databases.max(1) as usize;
        // One EMPTY slot `Vec` per DB: the `slots` per-slot tables (#570) are filled
        // LAZILY on the DB's first write ([`Self::slot_table_mut`]), so an unused DB
        // carries no slot overhead. `Vec::new()` allocates nothing.
        let dbs = (0..n).map(|_| Vec::new()).collect();
        ShardStore {
            dbs,
            slots: DEFAULT_SLOTS_PER_DB,
            hasher: DefaultHashBuilder::default(),
            eviction,
            accounting,
            used: 0,
            lazy_expired: 0,
            watch_versions: HashMap::new(),
            version_clock: 0,
            watched_count: 0,
            // Default 0: a single-shard server (and every test fixture) gets the
            // pre-coordinator byte-identical SCAN behavior. The boot path sets the real
            // reserved-band width via [`Self::with_scan_band_bits`] when shards > 1.
            scan_band_bits: 0,
            // Empty at boot: the cache-mode eviction pool is filled lazily by the first
            // over-budget refill scan and drained as eviction episodes consume it. It is
            // BOUNDED by EVICT_POOL_CAP, never per-key state.
            evict_pool: Vec::new(),
            // No write observer at boot (HA-5a): the static datastore path and the raft
            // control plane never observe. The fast-path gate `repl_active` is `false`, so
            // every write funnel site does a single bool test and proceeds (the same cost
            // profile as the `watched_count == 0` WATCH gate). serve/repl installs an
            // observer later via `set_write_observer`.
            repl_observer: None,
            repl_active: false,
            passive: false,
            // The collection-encoding thresholds default to the compiled Redis defaults (#40), so a
            // store built without an explicit snapshot is byte-identical to the pre-runtime-threshold
            // behavior. The boot/serve path installs the live snapshot via `set_encoding_thresholds`.
            encoding_thresholds: EncodingThresholds::defaults(),
            // No save in flight at boot (#576): the datapath bumps the S3-FIFO freq
            // normally; only a live `begin_save` freeze flips this on for its window.
            saving: false,
        }
    }

    /// Set the cross-shard SCAN reserved-band width (a CONSUMING builder, COORDINATOR.md
    /// #107). The boot path calls this with [`ScanCursor::SHARD_BITS`] when the server
    /// runs more than one shard, so [`Self::scan_step`] returns BAND-ALIGNED next cursors
    /// the coordinator's composite cursor round-trips losslessly; it stays `0` for a
    /// single-shard server (SCAN is then byte-identical to the pre-coordinator behavior).
    ///
    /// `bits` MUST be `< 64` (it is a hash right-shift amount); the only callers pass `0`
    /// or `ScanCursor::SHARD_BITS` (8). Builder form so the common constructors keep their
    /// signatures and every existing test fixture is unaffected (defaults to `0`).
    #[must_use]
    pub fn with_scan_band_bits(mut self, bits: u32) -> Self {
        debug_assert!(
            bits < 64,
            "scan_band_bits is a hash shift amount, must be < 64"
        );
        self.scan_band_bits = bits;
        self
    }

    /// Set the per-DB slot partition count (#570, a CONSUMING builder, the tunability-tenet
    /// config knob). The boot path calls this with the configured `store-slots-per-db` value
    /// (default [`DEFAULT_SLOTS_PER_DB`]); it stays at the default for every test fixture and
    /// the memory harness. `slots` is rounded UP to a power of two (so [`slot_index`] routes
    /// with a mask) and clamped to at least 1 (`1` == one table per DB, the pre-#570 layout).
    ///
    /// The slot count is a MEMORY vs tail-latency tradeoff: more slots bound the per-insert
    /// resize to fewer entries but add a fixed per-touched-DB `Vec` cost. It is fixed for the
    /// store's life (slot routing must be stable), so it is a boot/restart-required knob and
    /// MUST be set before any insert.
    ///
    /// # Panics
    ///
    /// Panics (in ALL build profiles) if the store already holds keys. Changing the slot count
    /// after a write would re-route existing keys to different slots, making them silently
    /// unreachable; a hard fail at this cold construction-time seam is strictly safer than that
    /// silent data loss (a `debug_assert` here would be a release-mode footgun). The builder is
    /// called immediately after construction, so this never fires on the nominal path.
    #[must_use]
    pub fn with_slots_per_db(mut self, slots: usize) -> Self {
        assert!(
            self.is_empty() && self.dbs.iter().all(Vec::is_empty),
            "slot count must be set before any DB is touched (routing must stay stable)"
        );
        self.slots = slots.max(1).next_power_of_two();
        self
    }

    /// Pre-size database `db`'s keyspace to hold at least `additional` more keys
    /// without an intermediate rehash. A bulk-load and measurement seam: the
    /// memory-model harness (BENCHMARK.md #8) reserves to the final key count so a
    /// fill triggers no table resize, which lets it separate the per-entry data
    /// cost (resize-free, key-count-independent) from the hash table's slot slack
    /// (whose size is a function of capacity, not of the stored object). An
    /// out-of-range `db` is a no-op. Purely additive: it touches no primitive
    /// signature and changes no observable command behavior, only the table's
    /// pre-allocated capacity.
    pub fn reserve(&mut self, db: u32, additional: usize) {
        let slots = self.slots;
        let hasher = self.hasher.clone();
        let Some(dbv) = self.dbs.get_mut(db as usize) else {
            return;
        };
        // Materialize this DB's slot tables (lazy per-DB) so the reservation lands on real
        // tables, then SPREAD it across the slots: a bulk fill distributes ~additional/S
        // keys to each slot, so pre-sizing each slot to that share makes the fill
        // ALLOCATION-FREE. The memory model (BENCHMARK.md #8) relies on this to separate
        // the per-entry data cost from the table slack. Under the default hashbrown
        // backend the guarantee is exact (no resize can occur below the reserved
        // capacity); under the `dashtable` backend it pre-builds the directory + pre-
        // allocates every segment's slot storage, so a WELL-MIXED fill allocates nothing,
        // while a hash-skewed segment can still split locally (dash's bounded incremental
        // growth; see DashIndex::reserve's contract note). Because S and the reservation
        // share the power-of-two rounding, the aggregate bucket count matches the
        // pre-#570 single table (the per-slot slack sums back to one table's), so
        // bytes-per-key does not regress.
        if dbv.is_empty() {
            // `arc_with_non_send_sync` is EXPECTED and intentional here: `HashTable<Entry>` is
            // `!Send`/`!Sync` (its `Entry` is a raw tagged pointer), but the #576 design uses `Arc`
            // (not `Rc`) DELIBERATELY -- its ATOMIC refcount is what lets a save FREEZE a slot into a
            // cross-thread `FrozenSlot` and COW it via `Arc::make_mut`. See the `dbs` field + the
            // `FrozenSlot` soundness doc for why sending the frozen `Arc` is sound.
            #[allow(clippy::arc_with_non_send_sync)]
            dbv.resize_with(slots, || Arc::new(HashTable::new()));
        }
        let per_slot = additional.div_ceil(slots);
        for table in dbv.iter_mut() {
            // The explicit-hash table's `reserve` needs a hasher closure to re-place
            // entries on a grow: hash each entry's embedded key. `Arc::make_mut` yields the
            // unique `&mut HashTable` (a bulk-load seam runs before any save, so this is the
            // uncontended strong_count == 1 path; if a save ever raced it would COW first).
            Arc::make_mut(table).reserve(per_slot, |e| hasher.hash_one(e.key()));
        }
    }

    /// Hash a probe key with the store's fixed table hasher (the value fed to the
    /// [`HashTable`] explicit-hash API). NOT the SCAN order hash.
    #[inline]
    fn key_hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// The IMMUTABLE per-slot table holding `key` in `db_idx` (#570 read routing), or
    /// `None` if `db_idx`'s slot tables are not yet allocated (an untouched DB -> the key
    /// is absent, no allocation). `db_idx` is the already-validated/clamped Vec index.
    #[inline]
    fn slot_table(&self, db_idx: usize, key: &[u8]) -> Option<&HashTable<Entry>> {
        // An untouched DB has an empty slot `Vec`, so `get(slot)` returns None (the key is
        // absent). A touched DB has exactly `slots` tables, so the slot always resolves.
        // SHARED `Arc` deref (#576): the read path derefs through the `Arc` (one pointer
        // indirection, NO `Arc::get_mut`, NO atomic refcount touch), so a GET stays a shared
        // borrow even while a save holds a frozen clone of this slot.
        self.dbs
            .get(db_idx)?
            .get(slot_index(key, self.slots))
            .map(|arc| &**arc)
    }

    /// The MUTABLE per-slot table for `key` in `db_idx` (#570 write routing), ALLOCATING
    /// this DB's `slots` slot tables on first touch (lazy per-DB: an unused DB carries no
    /// slot `Vec`). Every write funnel routes through here, so an insert into a fresh DB
    /// materializes its slot tables exactly once. `db_idx` is the validated/clamped index
    /// (in range, so the direct `self.dbs[db_idx]` cannot panic).
    ///
    /// COPY-ON-WRITE (#576): the `&mut HashTable` comes from `Arc::make_mut`. In the COMMON
    /// (no-save) case the slot `Arc` is uniquely owned (`strong_count == 1`), so `make_mut`
    /// hands back the live table in place -- an uncontended `&mut`, no copy. While a save
    /// holds a FROZEN clone of this slot (`strong_count > 1`), the FIRST write DEEP-CLONES
    /// the slot's entries into a fresh table and re-points the live `Arc` at it, leaving the
    /// frozen `Arc` sole-owning the ORIGINAL entries for the persist thread; the write then
    /// lands on the fresh copy. `HashTable<Entry>: Clone` clones each `Entry`, and `Entry`'s
    /// `Clone` is a DEEP clone (a new pointee allocation -- see its impl), so the frozen and
    /// live tables share NO pointee: a datapath write can never mutate/free what the dump
    /// reads. Subsequent writes to the same slot in the same save see `strong_count == 1`
    /// again (one-time COW per written slot, the ~0.7ms-per-slot cost).
    #[inline]
    fn slot_table_mut(&mut self, db_idx: usize, key: &[u8]) -> &mut HashTable<Entry> {
        let slots = self.slots;
        let slot = slot_index(key, slots);
        let dbv = &mut self.dbs[db_idx];
        if dbv.is_empty() {
            // First write to this DB: fill its slot `Vec` with `slots` empty tables (no
            // bucket array is allocated until each table's own first insert). `Arc` (not `Rc`) is
            // deliberate for the #576 cross-thread freeze/COW; the `!Send`/`!Sync` inner is expected.
            #[allow(clippy::arc_with_non_send_sync)]
            dbv.resize_with(slots, || Arc::new(HashTable::new()));
        }
        Arc::make_mut(&mut dbv[slot])
    }

    /// The WATCH write-funnel NOTIFY (TRANSACTIONS.md per-key dirty-CAS, PR-10b). Called
    /// from the store-internal write funnel ([`Self::put_object`], [`Self::remove_object`],
    /// [`Self::remove_object_crediting`]) so EVERY create/overwrite/delete/expiry of a
    /// watched key bumps its version. This is the EXACT attach point the funnel doc
    /// comment reserves for the OnWrite hook; it is store-internal, so adding it does NOT
    /// reopen the frozen `Store` waist (STORAGE_API.md).
    ///
    /// FAST PATH: gated behind `watched_count > 0`. When no connection is watching
    /// anything (the common case) this is a single integer compare and an immediate
    /// return: the non-watching hot path pays ~one branch and does NO hash probe. Only
    /// when a watch is active does it hash-probe `watch_versions` for `(db, key)` and, if
    /// the key is watched, bump the shard `version_clock` and stamp the slot.
    ///
    /// Determinism (ADR-0003): the bump reads the u64 `version_clock` COUNTER, never a
    /// clock or RNG.
    fn touch_watch(&mut self, db: u32, key: &[u8]) {
        // FAST PATH: no watches anywhere -> one branch, no hash probe.
        if self.watched_count == 0 {
            return;
        }
        // A watch is active: probe for THIS key. The tuple key `(u32, Box<[u8]>)` does
        // not borrow as `(u32, &[u8])`, so we build an owned probe key. This allocation
        // is OFF the non-watching hot path (gated by `watched_count > 0` above): it is
        // paid only on a write while SOME key is watched, which is rare relative to the
        // command stream, so it does not perturb the common path.
        let probe = (db, key.to_vec().into_boxed_slice());
        if let Some(slot) = self.watch_versions.get_mut(&probe) {
            self.version_clock += 1;
            slot.version = self.version_clock;
        }
    }

    /// Dirty EVERY watched key in `db` (TRANSACTIONS.md, PR-10b): bump the version of
    /// each watch slot whose db matches, INCLUDING watched-but-ABSENT keys. This is the
    /// FLUSHDB/SWAPDB signal -- Redis's `touchAllWatchedKeysOnDb` (src/multi.c) dirties
    /// every key watched in the flushed/swapped db, not just the resident ones, so a
    /// watched key that was absent at WATCH and would have stayed absent is now dirtied
    /// by the bulk operation (a flushed db is a structural change every watcher must see).
    ///
    /// Gated behind the `watched_count` fast path: when nothing is watched this is a
    /// single integer check. Otherwise it iterates the watch slots ONCE (O(watched keys),
    /// not O(db)). Determinism: bumps the u64 `version_clock` counter, no clock/RNG.
    fn touch_all_watches_in_db(&mut self, db: u32) {
        if self.watched_count == 0 {
            return;
        }
        for (slot_db, slot) in &mut self.watch_versions {
            if slot_db.0 == db {
                self.version_clock += 1;
                slot.version = self.version_clock;
            }
        }
    }

    /// Fire the write observer's `on_put` POST-IMAGE for `(db, key)` (HA-5a write-funnel
    /// fire). Called from the write funnel AFTER the table mutation, so the entry it borrows
    /// is the COMMITTED new value: a replication stream replays the post-image.
    ///
    /// FAST PATH: gated behind `repl_active` (the exact sibling of the `watched_count` WATCH
    /// gate). When no observer is installed (the default, the static datastore + raft control
    /// plane) this is a single bool compare and an immediate return: no box dereference, no
    /// table probe, no observer call. Only when an observer is installed does it re-find the
    /// post-write entry in `db_idx`'s table and hand the observer a borrow of it.
    ///
    /// The entry is re-found (rather than threaded through every funnel site) so the fire is
    /// a self-contained gated helper, mirroring `touch_watch`; the re-find is OFF the
    /// non-observing hot path (gated by `repl_active` above) and the table lookup is the same
    /// O(1) probe the funnel already pays. The split borrow (`repl_observer` mutable,
    /// `dbs` immutable) is taken by destructuring the distinct fields of `self`, so there is
    /// no double-borrow of `self`. A key that is somehow absent post-write (it never is on
    /// the put paths) is a no-op.
    fn observe_put(&mut self, db: u32, db_idx: usize, key: &[u8]) {
        // FAST PATH: no observer installed -> one branch, no probe, no box deref.
        if !self.repl_active {
            return;
        }
        // Split the borrow across the two distinct fields (no `&mut self` method call that
        // would borrow the whole store). The observer is `&mut`; the table is read-only.
        let Some(observer) = self.repl_observer.as_mut() else {
            return;
        };
        let h = self.hasher.hash_one(key);
        if let Some(obj) = self
            .dbs
            .get(db_idx)
            .and_then(|d| d.get(slot_index(key, self.slots)))
            .and_then(|t| t.find(h, |e| e.key() == key))
        {
            observer.on_put(db, key, obj);
        }
    }

    /// Fire the write observer's `on_remove` for `(db, key)` (HA-5a write-funnel fire).
    /// Called from the remove funnel AFTER the entry has left the table. Gated behind the
    /// `repl_active` fast path exactly like [`Self::observe_put`]: one bool branch and an
    /// immediate return when no observer is installed (the default), so the non-observing
    /// hot path pays the same ~one-branch cost as the `watched_count == 0` WATCH gate.
    fn observe_remove(&mut self, db: u32, key: &[u8]) {
        // FAST PATH: no observer installed -> one branch, no box deref.
        if !self.repl_active {
            return;
        }
        if let Some(observer) = self.repl_observer.as_mut() {
            observer.on_remove(db, key);
        }
    }

    /// Charge `bytes` to both the accounting hook and the running total.
    fn account_add(&mut self, bytes: usize) {
        self.accounting.add(bytes);
        self.used = self.used.saturating_add(bytes as u64);
    }

    /// Credit `bytes` from both the accounting hook and the running total.
    fn account_sub(&mut self, bytes: usize) {
        self.accounting.sub(bytes);
        self.used = self.used.saturating_sub(bytes as u64);
    }

    /// The number of logical databases.
    #[must_use]
    pub fn databases(&self) -> usize {
        self.dbs.len()
    }

    /// Total live entry count across all DBs (test/introspection helper; not a
    /// waist method).
    #[must_use]
    pub fn len(&self) -> usize {
        // Sum every slot table across every DB (#570).
        self.dbs
            .iter()
            .flat_map(|db| db.iter())
            .map(|t| t.len())
            .sum()
    }

    /// Whether the store holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.dbs.iter().all(|db| db.iter().all(|t| t.is_empty()))
    }

    /// The map index for the validated logical `db`. The command layer validates the
    /// DB range at SELECT time (KEYSPACE.md), so a well-behaved caller always passes
    /// an in-range `db`. A `debug_assert` fires loudly in tests and DST if a future
    /// un-validated caller (SWAPDB/MOVE/COPY, a cluster coordinator) routes an
    /// out-of-range db; the RELEASE build clamps to the last DB as a defensive
    /// backstop so an out-of-range db never panics the shard in production.
    fn db_index(&self, db: u32) -> usize {
        debug_assert!(
            (db as usize) < self.dbs.len(),
            "db index {db} out of range (have {} dbs); the command layer must \
             validate the DB range before calling the store",
            self.dbs.len()
        );
        (db as usize).min(self.dbs.len().saturating_sub(1))
    }

    /// The lazy expiry-on-read backstop (EXPIRATION.md). If `key` in `db` is
    /// present but its deadline has passed at `now`, remove it (firing the
    /// eviction/accounting remove hooks) and report it gone. Returns whether a
    /// LIVE entry remains for the key afterwards.
    ///
    /// `db` is the validated logical DB id passed to the hooks; `db_idx` is the
    /// (possibly clamped) Vec index for the map (see [`Self::db_index`]).
    ///
    /// FOLLOW-UP (#8/PR-2b efficiency): this does a `get` (the expiry probe) plus a
    /// `contains_key`, and the read/type_of callers then do ANOTHER `get` for the
    /// live entry, so a hot read hashes the key up to three times. Collapse to a
    /// single hash probe with the Entry API (or a get-once handle threaded to the
    /// caller) once the read path is restructured around it. No change now.
    fn expire_if_due(&mut self, db: u32, db_idx: usize, key: &[u8], now: UnixMillis) -> bool {
        let h = self.key_hash(key);
        let due = self
            .slot_table(db_idx, key)
            .and_then(|t| t.find(h, |e| e.key() == key))
            .is_some_and(|o| o.is_expired(now));
        if due {
            // PASSIVE REPLICA (HA-7d, CARRY-FORWARD 2): a replica reports a due key as
            // logically expired (absent) but does NOT physically remove it. Removal comes
            // ONLY from the replication stream (the primary's StreamDel), so the replica
            // never pre-empts the primary's own expiry, never double-counts `expired_keys`,
            // and never fires on_remove/account_sub for an expiry the primary owns. A
            // re-SET on the primary before its reaper fires arrives as a StreamPut and
            // overwrites the still-resident entry; the eventual StreamDel removes it.
            if self.passive {
                return false;
            }
            // Route the removal through the WRITE FUNNEL (`remove_object`): it fires
            // on_remove + account_sub AND `touch_watch`, so a watched key that lazily
            // expires between WATCH and EXEC is dirtied (the lazy-expiry dirty signal).
            // Inlining the remove here would skip that notify and rest the dirty signal
            // ONLY on the present/absent fallback. The lazy-backstop counter is bumped
            // AFTER, gated on the funnel actually having removed a resident entry.
            if self.remove_object(db, db_idx, key) {
                // Count the lazy-backstop reclamation for INFO `expired_keys` (PR-3b).
                // The serve loop drains this with `take_lazy_expired` after each
                // command. This is the lazy-path signal that complements the active
                // timing-wheel drain's count.
                self.lazy_expired = self.lazy_expired.saturating_add(1);
                // KEYSPACE NOTIFICATION (PROD-8): a LAZY TTL reap fires the `expired` event (class
                // `x`), exactly like the active drain. `record` short-circuits on the disabled
                // default BEFORE touching the key, so the lazy-expiry hot path is byte-identical
                // when notifications are off. The key is the one just reaped.
                ironcache_config::notify::record(
                    ironcache_config::EventClass::Expired,
                    "expired",
                    key,
                    db,
                );
            }
            return false;
        }
        // Present-and-live iff it exists (it did not expire above).
        self.slot_table(db_idx, key)
            .is_some_and(|t| t.find(h, |e| e.key() == key).is_some())
    }

    /// Insert or replace `key`'s object, adjusting the accounting/eviction hooks for
    /// the byte delta. Returns whether a live entry existed before (overwrite vs
    /// create). Caller guarantees any due expiry already ran.
    ///
    /// This (with [`Self::remove_object`] and the [`Store::rmw`] body) is the
    /// store-internal WRITE FUNNEL. The Wave-3 forkless-snapshot OnWrite pre-image
    /// hook (#60) attaches HERE, capturing the old object before it is overwritten;
    /// because this funnel is store-internal and not part of the frozen `Store`
    /// trait, adding it does NOT reopen the storage waist (STORAGE_API.md).
    ///
    /// `db` is the validated logical DB id passed to the hooks; `db_idx` is the
    /// (possibly clamped) Vec index for the map (see [`Self::db_index`]).
    fn put_object(&mut self, db: u32, db_idx: usize, key: &[u8], obj: Entry) -> bool {
        // WATCH notify (PR-10b): a create or overwrite of a watched key bumps its
        // version (gated behind the watched_count fast path inside touch_watch). This
        // fires for a create on a watched-ABSENT key too (a watched-absent key now
        // present is a modification), and for a no-op overwrite that stores the same
        // bytes (any write touches the version, matching Redis).
        self.touch_watch(db, key);
        let new_bytes = obj.accounted_bytes();
        let h = self.key_hash(key);
        let hasher = self.hasher.clone();
        // Replace inside the entry scope, capturing any old weight, then update the
        // hooks AFTER the table borrow ends (the hooks borrow `self` mutably). The
        // explicit-hash `entry` takes the probe hash, an eq closure (compare embedded
        // keys), and a hasher closure (re-place entries on a grow).
        let old_bytes = match self.slot_table_mut(db_idx, key).entry(
            h,
            |e| e.key() == key,
            |e| hasher.hash_one(e.key()),
        ) {
            TableEntry::Occupied(mut e) => {
                let old = e.get().accounted_bytes();
                // freq-in-object: a reused key KEEPS its S3-FIFO promote frequency
                // (the policy semantic "a replaced key carries its frequency"). The
                // freq now lives ON the entry, so an upsert that swaps the value
                // would reset it to 0 unless we carry the old entry's freq onto the
                // new entry here. (The policy's on_remove/on_insert below only moves
                // the KEY between queues; the freq is the store's to preserve.)
                let old_freq = e.get().freq();
                let mut obj = obj;
                obj.set_freq(old_freq);
                *e.get_mut() = obj;
                Some(old)
            }
            TableEntry::Vacant(e) => {
                e.insert(obj);
                None
            }
        };
        // A value REPLACE does NOT change the key's eviction-policy membership: the key
        // stays tracked, at its current FIFO position, with its carried freq (set above).
        // S3-FIFO is insertion-ordered (a write is an access that bumps freq, never a
        // reposition), so the policy needs NO notification - only the accounting byte
        // delta. Firing on_remove + on_insert here was both a fidelity wart (it moved the
        // key to the back of its queue) AND, with the freq-in-object policy's O(N)
        // on_remove queue splice, an O(N) cost on EVERY replace (catastrophic on a large
        // keyspace: it halved the head-to-head throughput). Only a TRUE insert (a new key,
        // the Vacant arm) enters the policy; a true delete/eviction fires on_remove via
        // `remove_object`.
        if let Some(old) = old_bytes {
            // Replace: accounting only, no policy churn (the key stays tracked in place).
            self.account_sub(old);
            self.account_add(new_bytes);
        } else {
            // Fresh insert: the new key enters the eviction policy's queues.
            self.account_add(new_bytes);
            self.eviction.on_insert(db, key, new_bytes);
        }
        // Write observer (HA-5a): a create or overwrite of `key` is an applied write. Fire
        // the post-image AFTER the table mutation + accounting (so the entry the observer
        // borrows is the committed new value), gated behind the `repl_active` fast path
        // inside `observe_put` (one bool branch when no observer is installed). This covers
        // every put-funnel caller: blind upsert, the rmw/rmw_mut Insert/Replace arms, a
        // RENAME/COPY destination write, and the test/collection `insert_object` seam.
        self.observe_put(db, db_idx, key);
        old_bytes.is_some()
    }

    /// Remove `key`'s object, crediting the hooks (the store-internal REMOVE FUNNEL).
    /// Returns whether it existed. Used both for an explicit delete (the `rmw` Delete
    /// arm, where the caller guarantees any due expiry already ran, so an existing entry
    /// is live) AND for an expiry removal: BOTH the lazy backstop ([`Self::expire_if_due`])
    /// and the active reaper ([`Self::reap_if_expired`]) route the actual removal through
    /// here, so on_remove + account_sub + the WATCH notify fire on every removal path.
    ///
    /// `db` is the validated logical DB id passed to the hooks; `db_idx` is the
    /// (possibly clamped) Vec index for the map (see [`Self::db_index`]).
    fn remove_object(&mut self, db: u32, db_idx: usize, key: &[u8]) -> bool {
        // WATCH notify (PR-10b): a delete or expiry of a watched key bumps its version.
        // Because the lazy/active expiry paths reach here (expire_if_due / reap_if_expired
        // both call remove_object), a watched key that expires is dirtied too, and
        // FLUSHDB/FLUSHALL (which loop remove_object) dirty every watched key they remove.
        // A watched-but-ABSENT key flush is handled in flush_db (it iterates the watch
        // slots), since remove_object only fires for a key that was actually resident.
        self.touch_watch(db, key);
        let h = self.key_hash(key);
        let removed = match self
            .slot_table_mut(db_idx, key)
            .find_entry(h, |e| e.key() == key)
        {
            Ok(occ) => {
                let (obj, _) = occ.remove();
                Some(obj.accounted_bytes())
            }
            Err(_absent) => None,
        };
        if let Some(bytes) = removed {
            self.account_sub(bytes);
            self.eviction.on_remove(db, key, bytes);
            // Write observer (HA-5a): a removal is an applied write. Fire AFTER the entry has
            // left the table, gated behind `repl_active`. Because EVERY removal path funnels
            // through here (explicit delete, the rmw Delete arm, the lazy expiry backstop and
            // the active reaper, FLUSHDB/FLUSHALL, eviction, and a RENAME's source removal),
            // this one fire covers them all.
            self.observe_remove(db, key);
            true
        } else {
            false
        }
    }

    /// Remove `key`'s object, crediting an EXPLICIT `bytes` weight (PR-5 in-place
    /// path). Unlike [`Self::remove_object`], which reads the object's CURRENT weight,
    /// this credits the caller-supplied figure: after an in-place collection edit the
    /// in-memory object is already shorter than at observe time, so the
    /// `rmw_mut` Delete / empty path must credit the PRE-EDIT weight (the bytes the
    /// accounting hook was charged at insert time) to avoid leaking the popped bytes.
    /// Returns whether the key existed.
    fn remove_object_crediting(
        &mut self,
        db: u32,
        db_idx: usize,
        key: &[u8],
        bytes: usize,
    ) -> bool {
        // WATCH notify (PR-10b): the in-place Delete / empty-collection path also
        // touches a watched key's version (a collection drained to empty by an edit is a
        // modification, like any delete).
        self.touch_watch(db, key);
        let h = self.key_hash(key);
        let existed = match self
            .slot_table_mut(db_idx, key)
            .find_entry(h, |e| e.key() == key)
        {
            Ok(occ) => {
                occ.remove();
                true
            }
            Err(_absent) => false,
        };
        if existed {
            self.account_sub(bytes);
            self.eviction.on_remove(db, key, bytes);
            // Write observer (HA-5a): the in-place Delete / emptied-collection path also
            // removes the key, so it is an applied removal. Fire AFTER the entry has left the
            // table, gated behind `repl_active`.
            self.observe_remove(db, key);
            true
        } else {
            false
        }
    }

    /// Set the TTL deadline of the entry for `key` in table `db_idx` (the TTL-only
    /// write path shared by the `rmw`/`rmw_mut` Keep/Mutated arms). For a Str entry an
    /// add/remove of the deadline rebuilds the blob (the 8-byte field appears/
    /// disappears); a deadline-only change patches in place. A no-op if the key is gone.
    fn set_entry_expire(&mut self, db_idx: usize, key: &[u8], deadline: Option<UnixMillis>) {
        let h = self.key_hash(key);
        let slot = slot_index(key, self.slots);
        // A TTL write is a MUTATION, so it takes `Arc::make_mut` (COW): during a save this
        // deep-clones a still-frozen slot before patching the deadline (so the dump reflects
        // the pre-freeze TTL); outside a save it is an uncontended in-place `&mut`.
        if let Some(obj) = self
            .dbs
            .get_mut(db_idx)
            .and_then(|d| d.get_mut(slot))
            .map(Arc::make_mut)
            .and_then(|t| t.find_mut(h, |e| e.key() == key))
        {
            obj.set_expire_at(deadline);
        }
    }

    /// Build the read-borrow view for an entry. Memory Round 3: an int-encoded value
    /// stores its CANONICAL DECIMAL bytes INLINE in the blob (encoding reported as
    /// `int` from the header), so the view borrows them directly with NO per-read
    /// allocation (the prior `int_decimal_bytes` allocation the FOLLOW-UP note flagged
    /// is now eliminated). A string borrows its stored bytes; a collection borrows an
    /// empty slice (only its data_type / encoding are read for GET / OBJECT ENCODING).
    fn view_of(obj: &Entry) -> ValueRef<'_> {
        // `str_value_bytes()` is the int decimal digits / embstr / raw bytes for a Str
        // entry and an empty slice for a Coll entry (which is not byte-readable as a
        // string: GET checks the String data_type; OBJECT ENCODING reads the encoding).
        ValueRef::borrowed(
            obj.data_type(),
            obj.encoding(),
            obj.expire_at(),
            obj.str_value_bytes(),
        )
    }

    /// Build the rmw observation handle for an entry (same borrow rule as
    /// [`Self::view_of`]: int decimal bytes are inline, so no per-read allocation).
    /// A collection observed through the READ-ONLY rmw arm exposes empty bytes; the
    /// closure sees the collection data_type and returns WRONGTYPE. In-place collection
    /// edits use the MUTABLE arm (`rmw_mut` -> OccupiedEntryMut), not this handle.
    fn occupied_of(obj: &Entry) -> OccupiedEntry<'_> {
        OccupiedEntry::borrowed(
            obj.data_type(),
            obj.encoding(),
            obj.expire_at(),
            obj.str_value_bytes(),
        )
    }
}

impl<E: EvictionHook, A: AccountingHook> Store for ShardStore<E, A> {
    fn read(&mut self, db: u32, key: &[u8], now: UnixMillis) -> Option<ValueRef<'_>> {
        let db_idx = self.db_index(db);
        if !self.expire_if_due(db, db_idx, key, now) {
            return None;
        }
        // freq-in-object: the S3-FIFO 2-bit promote frequency lives ON the stored entry,
        // and the store bumps the just-accessed entry INLINE here (it already holds the
        // entry, so this is O(1) with NO policy lookup). The eviction policy no longer
        // owns a per-key index, and the per-access `on_access` policy call is GONE from
        // the hot path (the no-op policies do nothing with it, and the FIFO-class engine
        // reads the freq off the object at `select_victim` time via `VictimFreq`).
        let h = self.key_hash(key);
        // SHARED READ (#576 prerequisite): the GET hot path looks the entry up with the
        // SHARED `find` (not `find_mut`) and bumps the S3-FIFO freq through the entry's
        // INTERIOR-MUTABLE `bump_freq_shared` accessor, so a hit needs NO `&mut` on the
        // table or the entry -- the whole read path is a shared borrow. This decouples the
        // freq bump from `&mut` so a later per-slot Arc-COW value never triggers
        // `Arc::get_mut`'s refcount check on a plain GET (the measured driver of the Arc
        // regression). The bump is BYTE-IDENTICAL to the old `find_mut(..).bump_freq()`, so
        // eviction picks the same victims. Still a SINGLE probe (#511): one `find` both
        // bumps (via the shared accessor) and yields the view. `expire_if_due` above
        // returned true, so the key's slot table exists (the DB is touched, #570), hence
        // `slot_table(..)` is `Some`.
        //
        // #576 SOUNDNESS GATE: `bump_freq_shared` is the ONLY datapath write reaching a
        // stored pointee through a SHARED `&Entry`. While a save holds a frozen `Arc` clone
        // of this slot the SAME pointee may be READ by the persist thread, so the bump is
        // SKIPPED for the save window (a bounded, correctness-neutral S3-FIFO fidelity loss)
        // to avoid a read/write data race on the freq byte. The flag is a `Copy` bool read
        // before the shared table borrow.
        let bump = !self.saving;
        self.slot_table(db_idx, key)
            .and_then(|t| t.find(h, |e| e.key() == key))
            .map(|obj| {
                if bump {
                    obj.bump_freq_shared();
                }
                Self::view_of(obj)
            })
    }

    // The Store-trait view of the passive-replica flag (HA-7d), so generic command handlers
    // can gate their lazy field reaping on it exactly as the key-level path gates `expire_if_due`.
    fn is_passive(&self) -> bool {
        self.passive
    }

    fn upsert(
        &mut self,
        db: u32,
        key: &[u8],
        value: NewValue<'_>,
        expire: ExpireWrite,
        now: UnixMillis,
    ) -> bool {
        let db_idx = self.db_index(db);
        // Whether a live key existed before this blind set (the return value), and
        // its old deadline (for ExpireWrite::Keep).
        let existed = self.expire_if_due(db, db_idx, key, now);
        let old_deadline = if existed {
            let h = self.key_hash(key);
            self.slot_table(db_idx, key)
                .and_then(|t| t.find(h, |e| e.key() == key))
                .and_then(Entry::expire_at)
        } else {
            None
        };
        let new_deadline = resolve_expire(expire, old_deadline);
        let obj = match value {
            NewValue::Bytes(b) => Entry::str_from_bytes(key, b, new_deadline),
            NewValue::Int(n) => Entry::str_from_int(key, n, new_deadline),
        };
        self.put_object(db, db_idx, key, obj);
        existed
    }

    fn delete(&mut self, db: u32, key: &[u8], now: UnixMillis) -> bool {
        let db_idx = self.db_index(db);
        // A lazily-expired key counts as not-existing: run the backstop first.
        if !self.expire_if_due(db, db_idx, key, now) {
            return false;
        }
        self.remove_object(db, db_idx, key)
    }

    fn rmw<R>(
        &mut self,
        db: u32,
        key: &[u8],
        now: UnixMillis,
        f: impl FnOnce(RmwEntry<'_>) -> RmwStep<R>,
    ) -> R {
        let db_idx = self.db_index(db);
        let live = self.expire_if_due(db, db_idx, key, now);

        // Observe (atomically with the write that follows, on the owning core).
        let step = if live {
            // freq-in-object: bump the just-accessed entry's S3-FIFO freq INLINE through the
            // SHARED interior-mutable accessor (O(1), no policy lookup, no `&mut` on the
            // table). One `find` both bumps the freq AND yields the read-only view, instead
            // of a `find_mut` (bump) followed by a second `find` (view). The per-access
            // `on_access` policy call is gone from the hot path; the policy reads the freq
            // off the object at `select_victim` time via `VictimFreq`. `live` is true, so the
            // key's slot table exists (the DB is touched, #570), hence `slot_table` is `Some`.
            let h = self.key_hash(key);
            // #576 SOUNDNESS GATE (see `read`): skip the shared-`&Entry` freq bump while a
            // save holds a frozen clone, so the persist thread's concurrent read of this
            // pointee never races the bump. A `Copy` bool read before the table borrow.
            let bump = !self.saving;
            let obj = self
                .slot_table(db_idx, key)
                .and_then(|t| t.find(h, |e| e.key() == key))
                .expect("live entry present");
            if bump {
                obj.bump_freq_shared();
            }
            let entry = RmwEntry::Occupied(Self::occupied_of(obj));
            f(entry)
        } else {
            f(RmwEntry::Vacant)
        };

        // The current (pre-write) deadline, for ExpireWrite::Keep/Unchanged.
        let old_deadline = if live {
            let h = self.key_hash(key);
            self.slot_table(db_idx, key)
                .and_then(|t| t.find(h, |e| e.key() == key))
                .and_then(Entry::expire_at)
        } else {
            None
        };

        match step.action {
            RmwAction::Keep => {
                // Value untouched; the TTL may still change (e.g. a future GETEX).
                if live {
                    let new_deadline = match step.expire {
                        ExpireWrite::Unchanged => old_deadline,
                        other => resolve_expire(other, old_deadline),
                    };
                    if new_deadline != old_deadline {
                        // WATCH notify (PR-10b): a TTL change on a watched key IS a write
                        // (EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT/PERSIST/GETEX-with-TTL all
                        // signal in Redis -> keyModified -> touchWatchedKey). Scoped to the
                        // real-change branch: a no-op TTL write (bare GETEX, an EXPIRE that
                        // does not move the deadline) keeps the key CLEAN, matching Redis.
                        self.touch_watch(db, key);
                        self.set_entry_expire(db_idx, key, new_deadline);
                        // Write observer (HA-5a): a real TTL change (EXPIRE/PEXPIRE/PERSIST/
                        // GETEX-with-TTL) is an applied write to the key. `set_entry_expire`
                        // patches the deadline in place (it does not go through put_object),
                        // so fire the post-image here with the new deadline; gated behind the
                        // `repl_active` fast path. Scoped to the real-change branch (mirrors
                        // the WATCH notify above), so a no-op TTL write fires nothing.
                        self.observe_put(db, db_idx, key);
                    }
                }
            }
            RmwAction::Insert(v) | RmwAction::Replace(v) => {
                let new_deadline = match step.expire {
                    ExpireWrite::Unchanged => old_deadline,
                    other => resolve_expire(other, old_deadline),
                };
                // Materialize the new value against the LIVE encoding thresholds (#40): a
                // create-on-missing collection (e.g. RPUSH on a fresh key) respects a runtime
                // `CONFIG SET *-max-listpack-*`. A `Copy` snapshot read; non-collection values ignore
                // it. NOT a reconstruction (that path goes through KvObj::from_new_owned with the
                // unlimited thresholds + a forced encoding), so the live thresholds are correct here.
                let obj = Entry::from_new_owned(key, v, new_deadline, &self.encoding_thresholds);
                // put_object fires the write observer (on_put post-image).
                self.put_object(db, db_idx, key, obj);
            }
            RmwAction::Delete => {
                if live {
                    // remove_object fires the write observer (on_remove).
                    self.remove_object(db, db_idx, key);
                }
            }
            // The READ-ONLY rmw arm hands out no mutable handle, so there is nothing
            // to measure: Mutated is treated as Keep (TTL effect still honored). A
            // value-internal in-place edit must go through `rmw_mut` (OccupiedMut).
            RmwAction::Mutated => {
                if live {
                    let new_deadline = match step.expire {
                        ExpireWrite::Unchanged => old_deadline,
                        other => resolve_expire(other, old_deadline),
                    };
                    if new_deadline != old_deadline {
                        self.set_entry_expire(db_idx, key, new_deadline);
                    }
                }
            }
        }
        step.reply
    }

    // The in-place-mutation RMW funnel: observe -> typed mutable handle -> measure delta /
    // recompute encoding / empty-deletes-key, with the PR-10b WATCH notify on the Mutated
    // arm. The post-action match over Keep/Insert/Replace/Delete/Mutated is the intended
    // shape, so the line-count lint is allowed here (the same allowance the read-only `rmw`
    // would carry; the additive WATCH notify nudged this over the 100-line bar).
    #[allow(clippy::too_many_lines)]
    fn rmw_mut<R>(
        &mut self,
        db: u32,
        key: &[u8],
        now: UnixMillis,
        f: impl FnOnce(RmwEntry<'_>) -> RmwStep<R>,
    ) -> R {
        let db_idx = self.db_index(db);
        let live = self.expire_if_due(db, db_idx, key, now);

        // For the OccupiedMut path the store MEASURES the accounting delta itself (it
        // does not trust the handler): record the pre-edit weight, hand out a typed
        // mutable handle, run the closure, then measure the post-edit weight.
        let key_h = self.key_hash(key);
        let old_bytes = if live {
            // freq-in-object: bump the accessed entry's S3-FIFO freq INLINE (O(1), no
            // policy lookup) and read its pre-edit weight in the SAME mutable borrow.
            // The per-access `on_access` policy call is gone from the hot path. `live` is
            // true, so the key's slot table exists (#570).
            self.slot_table_mut(db_idx, key)
                .find_mut(key_h, |e| e.key() == key)
                .map_or(0, |obj| {
                    obj.bump_freq();
                    obj.accounted_bytes()
                })
        } else {
            0
        };

        // Snapshot the LIVE encoding thresholds (#40) BEFORE the mutable entry borrow (a `Copy` of a
        // disjoint field), so the typed collection view carries them into the conversion-deciding
        // `*Value` methods the closure calls. Read once per rmw (cold relative to decode/dispatch);
        // at the default this is the same compiled-default values the constants held.
        let thresholds = self.encoding_thresholds;
        let step = if live {
            let obj = self
                .slot_table_mut(db_idx, key)
                .find_mut(key_h, |e| e.key() == key)
                .expect("live entry present");
            // Read the REAL pre-edit metadata BEFORE taking the typed mutable borrow
            // (these are Copy scalars; `as_*_mut` then borrows the value mutably). The
            // mutable handle carries the same type/encoding/TTL the read-only
            // `occupied_of()` path exposes. The store still recomputes the POST-edit
            // encoding after a `Mutated` return; this is the PRE-edit snapshot.
            let data_type = obj.data_type();
            let encoding = obj.encoding();
            let expire_at = obj.expire_at();
            // Build the typed mutable view from the entry's collection arm: a list yields
            // the list arm, etc.; a Str entry yields the non-collection arm (the handler's
            // `as_*_mut` then returns None -> WRONGTYPE). The empty-collection check after
            // a Mutated return uses `Entry::is_empty_collection`, defined over the SAME
            // `collection_len` mapping (kvobj.rs), so the two sites cannot drift.
            //
            // Each collection view also carries the live encoding thresholds (`with_thresholds`)
            // so a mutating handler's conversion decision honors a `CONFIG SET *-max-listpack-*`.
            //
            // The repr is matched ONCE (not via sequential `as_*_mut` borrows, which would
            // each take and drop a fresh `&mut` and obscure the dispatch) so each
            // collection type maps to exactly one arm.
            let entry = match obj.as_coll_val_mut() {
                Some(kvobj::CollVal::List(l)) => RmwEntry::OccupiedMut(
                    OccupiedEntryMut::list(encoding, expire_at, l).with_thresholds(thresholds),
                ),
                Some(kvobj::CollVal::Hash(h)) => RmwEntry::OccupiedMut(
                    OccupiedEntryMut::hash(encoding, expire_at, h).with_thresholds(thresholds),
                ),
                Some(kvobj::CollVal::Set(s)) => RmwEntry::OccupiedMut(
                    OccupiedEntryMut::set(encoding, expire_at, s).with_thresholds(thresholds),
                ),
                Some(kvobj::CollVal::ZSet(z)) => RmwEntry::OccupiedMut(
                    OccupiedEntryMut::zset(encoding, expire_at, z).with_thresholds(thresholds),
                ),
                // A Str entry yields the non-collection arm (the handler's `as_*_mut`
                // then returns None -> WRONGTYPE). No conversion applies, so no thresholds.
                None => RmwEntry::OccupiedMut(OccupiedEntryMut::non_collection(
                    data_type, encoding, expire_at,
                )),
            };
            f(entry)
        } else {
            f(RmwEntry::Vacant)
        };

        let old_deadline = if live {
            self.slot_table(db_idx, key)
                .and_then(|t| t.find(key_h, |e| e.key() == key))
                .and_then(Entry::expire_at)
        } else {
            None
        };

        match step.action {
            RmwAction::Keep => {
                if live {
                    let new_deadline = match step.expire {
                        ExpireWrite::Unchanged => old_deadline,
                        other => resolve_expire(other, old_deadline),
                    };
                    if new_deadline != old_deadline {
                        // WATCH notify (PR-10b): same as the read-only `rmw` Keep arm -- a
                        // TTL change on a watched key is a write, scoped to the real-change
                        // branch so a no-op TTL write stays clean (matches Redis).
                        self.touch_watch(db, key);
                        self.set_entry_expire(db_idx, key, new_deadline);
                        // Write observer (HA-5a): a real TTL change is an applied write;
                        // `set_entry_expire` patches in place (not through put_object), so
                        // fire the post-image here. Same real-change scoping as the WATCH
                        // notify, gated behind `repl_active`.
                        self.observe_put(db, db_idx, key);
                    }
                }
            }
            RmwAction::Insert(v) | RmwAction::Replace(v) => {
                let new_deadline = match step.expire {
                    ExpireWrite::Unchanged => old_deadline,
                    other => resolve_expire(other, old_deadline),
                };
                // Materialize the new value against the LIVE encoding thresholds (#40): a
                // create-on-missing collection (e.g. RPUSH on a fresh key) respects a runtime
                // `CONFIG SET *-max-listpack-*`. A `Copy` snapshot read; non-collection values ignore
                // it. NOT a reconstruction (that path goes through KvObj::from_new_owned with the
                // unlimited thresholds + a forced encoding), so the live thresholds are correct here.
                let obj = Entry::from_new_owned(key, v, new_deadline, &self.encoding_thresholds);
                // put_object fires the write observer (on_put post-image).
                self.put_object(db, db_idx, key, obj);
            }
            RmwAction::Delete => {
                if live {
                    // The handler may have edited the value in place on the borrowed
                    // handle BEFORE returning Delete (e.g. LPOP that drains the last
                    // element pops it, then returns Delete). The in-memory object is
                    // therefore SHORTER than at observe time, so crediting its current
                    // weight would leak the popped bytes. Credit the PRE-EDIT weight
                    // (`old_bytes`) and remove the entry directly.
                    self.remove_object_crediting(db, db_idx, key, old_bytes);
                }
            }
            // The in-place collection edit already happened on the borrowed handle.
            // The store now: (1) if the edit EMPTIED the collection, removes the key
            // (empty-collection-deletes-key backstop); else (2) measures the byte
            // delta, charges account_add/sub and re-fires on_remove(old)/on_insert(new)
            // (the same re-account pattern put_object uses), recomputes the encoding
            // from the post-edit repr, and applies any TTL effect.
            RmwAction::Mutated => {
                if live {
                    let emptied = self
                        .slot_table(db_idx, key)
                        .and_then(|t| t.find(key_h, |e| e.key() == key))
                        .is_some_and(Entry::is_empty_collection);
                    if emptied {
                        // Same pre-edit-weight credit as the Delete arm: the edit
                        // already shrank the in-memory object, so credit `old_bytes`.
                        // The WATCH notify fires inside remove_object_crediting (an
                        // emptied collection is a delete), so it is NOT fired again here
                        // -- each logical write bumps the version exactly once.
                        self.remove_object_crediting(db, db_idx, key, old_bytes);
                    } else {
                        // WATCH notify (PR-10b): a non-emptying in-place collection edit IS
                        // a write to the key, so it must bump a watched key's version EVEN
                        // when the edit is a no-op on the value (SADD of an existing member,
                        // HSET of the same value) -- Redis treats any write command touching
                        // the key as a modification. The funnel functions
                        // (put_object/remove_object) are NOT called on this non-emptying
                        // same-size in-place path, so the notify must fire here. (The emptied
                        // branch above already notifies via remove_object_crediting.)
                        self.touch_watch(db, key);
                        let new_bytes = self
                            .slot_table(db_idx, key)
                            .and_then(|t| t.find(key_h, |e| e.key() == key))
                            .map_or(0, Entry::accounted_bytes);
                        // Re-account the signed delta and re-fire the eviction sizing
                        // so the policy's per-key byte estimate tracks the edit.
                        if new_bytes != old_bytes {
                            self.eviction.on_remove(db, key, old_bytes);
                            self.account_sub(old_bytes);
                            self.account_add(new_bytes);
                            self.eviction.on_insert(db, key, new_bytes);
                        }
                        // Recompute the encoding (listpack <-> quicklist) and apply the
                        // TTL effect.
                        let new_deadline = match step.expire {
                            ExpireWrite::Unchanged => old_deadline,
                            other => resolve_expire(other, old_deadline),
                        };
                        if let Some(obj) = self
                            .slot_table_mut(db_idx, key)
                            .find_mut(key_h, |e| e.key() == key)
                        {
                            obj.recompute_encoding();
                            if new_deadline != old_deadline {
                                obj.set_expire_at(new_deadline);
                            }
                        }
                        // Write observer (HA-5a): a non-emptying in-place collection edit
                        // (LPUSH/SADD/HSET/ZADD...) is an applied write that NEVER funnels
                        // through put_object/remove_object on this path, so fire it here --
                        // AFTER the encoding recompute + TTL apply, so the post-image the
                        // observer borrows is the fully-committed edited entry. Fired
                        // unconditionally in the non-emptying branch (like the WATCH notify
                        // above), so even a no-op same-size edit (SADD of an existing member)
                        // is observed. Gated behind `repl_active`. (The emptied branch above
                        // already fired on_remove via remove_object_crediting.)
                        self.observe_put(db, db_idx, key);
                    }
                }
            }
        }
        step.reply
    }

    fn contains(&mut self, db: u32, key: &[u8], now: UnixMillis) -> bool {
        let db_idx = self.db_index(db);
        self.expire_if_due(db, db_idx, key, now)
    }

    fn type_of(&mut self, db: u32, key: &[u8], now: UnixMillis) -> Option<DataType> {
        let db_idx = self.db_index(db);
        if !self.expire_if_due(db, db_idx, key, now) {
            return None;
        }
        let h = self.key_hash(key);
        self.slot_table(db_idx, key)
            .and_then(|t| t.find(h, |e| e.key() == key))
            .map(Entry::data_type)
    }

    fn used_memory(&self) -> u64 {
        // PR-2a: the O(1) running logical-byte total, kept in lockstep with the
        // accounting hook. PR-2b swaps this for the jemalloc stats.allocated mallctl
        // behind this same method.
        self.used
    }
}

impl<E: EvictionPolicy, A: AccountingHook> ironcache_storage::Admit for ShardStore<E, A> {
    /// Whether the configured policy evicts at the ceiling (cache mode) vs rejects
    /// the write (strict datastore mode / `noeviction`). Dispatch reads this to
    /// choose evict-to-fit vs an immediate `-OOM` (ADMISSION.md).
    fn policy_evicts(&self) -> bool {
        self.eviction.evicts()
    }

    /// Whether the configured policy restricts victims to TTL-bearing keys (the
    /// `volatile-*` family). Exposed for INFO/introspection; [`Self::evict_to_fit`]
    /// already enforces it internally.
    fn policy_volatile_only(&self) -> bool {
        self.eviction.volatile_only()
    }

    /// The CONFIGURED `maxmemory-policy` name the policy echoes VERBATIM (for INFO
    /// `maxmemory_policy` and CONFIG GET); the exact configured spelling, not an
    /// engine-family substitution (ADR-0009).
    fn policy_name(&self) -> String {
        self.eviction.policy_name()
    }

    /// Evict entries until `used_memory()` is at or below `budget_bytes` (`used <=
    /// budget`), or until the policy can free no more (or a per-call iteration cap is
    /// hit). Returns the number of entries evicted (ADMISSION.md evict-to-fit;
    /// ADR-0007 cache mode).
    ///
    /// The over-budget test is strict `>` to match Redis's getMaxmemoryState (evict.c):
    /// memory is "under limit" at `used <= maxmemory`, so eviction frees down to
    /// `used <= budget` (NOT strictly below it). When already at or under budget this is
    /// a NO-OP that frees nothing (preserved across both victim strategies).
    ///
    /// ## Two victim strategies (the zero-per-key-state cache-mode refactor)
    ///
    /// The store branches on [`EvictionPolicy::victim_strategy`]:
    ///
    /// - [`VictimStrategy::None`] (`noeviction`): free nothing, return 0. The caller then
    ///   replies `-OOM`.
    /// - [`VictimStrategy::TableScanLowestFreq`] (the cache-mode default): the policy
    ///   holds NO per-key state, so the store scans its own table for the lowest-frequency
    ///   live entries and evicts those first (exact LFU over the in-object 2-bit freq).
    ///   The scan is AMORTIZED through a bounded eviction pool: one scan harvests the
    ///   coldest [`EVICT_POOL_CAP`] candidates, then many eviction episodes drain that
    ///   pool before the next scan. See [`Self::evict_to_fit_pooled`].
    /// - [`VictimStrategy::Roster`] (`Random` / `WTinyLfu`): the policy keeps its own
    ///   candidate roster, driven round-by-round through [`EvictionHook::select_victim`].
    ///   See [`Self::evict_to_fit_roster`].
    fn evict_to_fit(&mut self, budget_bytes: u64, now: UnixMillis) -> u64 {
        // NO-OP when already under budget, for EVERY strategy (preserved): the strict `>`
        // test means a single under-budget call frees nothing and does not even scan.
        if self.used_memory() <= budget_bytes {
            return 0;
        }
        match self.eviction.victim_strategy() {
            VictimStrategy::None => 0,
            VictimStrategy::TableScanLowestFreq => self.evict_to_fit_pooled(budget_bytes, now),
            VictimStrategy::Roster => self.evict_to_fit_roster(budget_bytes, now),
        }
    }

    /// The access-frequency estimate for OBJECT FREQ, delegated to the configured
    /// policy (only the W-TinyLFU LFU engine returns `Some`; non-LFU policies return
    /// `None`, which dispatch maps to the OBJECT FREQ LFU-gating error). Read-only.
    fn access_freq(&self, db: u32, key: &[u8]) -> Option<u8> {
        self.eviction.access_freq(db, key)
    }
}

impl<E: EvictionPolicy, A: AccountingHook> ShardStore<E, A> {
    /// Evict the lowest-frequency live entries through an AMORTIZED EVICTION POOL (the
    /// [`VictimStrategy::TableScanLowestFreq`] cache-mode path). The eviction policy holds
    /// NO per-key state: the 2-bit access frequency lives ON each stored object (bumped by
    /// the store inline on read). Returns the count evicted.
    ///
    /// ## The amortized-pool design (why this is not an O(N) scan per eviction)
    ///
    /// The previous design did one O(N) table scan per eviction EPISODE; under SUSTAINED
    /// eviction (a cache pinned at capacity where most SETs insert a non-resident key) an
    /// episode is essentially every insert, so the scan degraded to O(N) PER INSERT. This
    /// design replaces the per-episode scan with a small BOUNDED, TRANSIENT pool of victim
    /// candidates ([`ShardStore::evict_pool`], at most [`EVICT_POOL_CAP`]): ONE scan
    /// ([`Self::refill_evict_pool`]) harvests the coldest `EVICT_POOL_CAP` candidates, and
    /// then up to `EVICT_POOL_CAP` eviction episodes drain that pool with NO scan, so the
    /// O(N) scan is amortized over `EVICT_POOL_CAP` evictions (Redis's evict.c eviction
    /// pool). The pool is BOUNDED, so it is NOT per-key state: it never grows with the
    /// keyspace, preserving the cache-mode policy's zero-per-key-state memory property.
    ///
    /// ## Determinism (ADR-0003)
    ///
    /// Victims are taken in ascending `(freq, scan_hash, key, db)` order, the SAME total
    /// order the prior full scan used: frequency is the primary key (the LFU signal),
    /// `scan_hash` then the raw key bytes break a 64-bit hash collision, and `db` is the
    /// FINAL tie-break so the order is TOTAL (the same key bytes can be resident in two dbs
    /// at the same freq, differing only in db). The pool is filled by the same order, so
    /// two shards with identical keyspaces and access history evict identical keys. No RNG,
    /// no wall-clock (only the passed `now`).
    ///
    /// ## Volatile-only (preserved)
    ///
    /// Under a `volatile_only` policy ONLY TTL-bearing live entries are collected into the
    /// pool (a live non-TTL entry is never collected, so it is spared), exactly as before.
    /// A pooled key that LOST its TTL (a PERSIST between refill and pop) is re-validated and
    /// SKIPPED at pop time. If no TTL-bearing key exists nothing is collected and the
    /// episode frees nothing (the -OOM path), matching Redis volatile-* with no expirable key.
    ///
    /// ## Progress / termination
    ///
    /// The loop terminates when `used <= budget` OR a refill yields an EMPTY pool (nothing
    /// evictable -> return what was freed; the caller replies `-OOM`). A pool entry that is
    /// stale at pop time (the key was deleted / overwritten away / persisted / already
    /// expired since the refill) is skipped, which SHRINKS the pool; an empty pool triggers
    /// exactly ONE refill, and a refill that finds no eligible live candidate returns an
    /// empty pool -> break. So a run of stale skips cannot loop forever.
    ///
    /// ## Evict EXACTLY to fit (Redis maxmemory semantics)
    ///
    /// Victims are evicted coldest-first until `used <= budget`, then the loop STOPS: it
    /// never sheds live data that already fits (Redis's evict loop). Any candidates left in
    /// the pool are simply carried to the NEXT call (still valid victims to re-validate);
    /// they are bounded, so carrying them keeps no per-key state. This carry-across-calls is
    /// the whole point: a cache pinned at capacity calls `evict_to_fit` once per SET, each
    /// needing ~1 eviction, and ONE refill scan serves up to `EVICT_POOL_CAP` of those.
    ///
    /// ## Coldest-first across a carried (possibly stale) pool
    ///
    /// A carried pool was ordered coldest-first AT ITS REFILL; later SETs insert FRESH cold
    /// (freq-0) keys that are NOT in the carried pool. So a pooled candidate that is WARM
    /// (live freq > 0) might be popped while a colder fresh key exists outside the pool.
    /// Before evicting a warm candidate the loop therefore does ONE fresh refill (picking up
    /// the fresh cold keys) and retries; the `refreshed_since_progress` latch caps this at
    /// one extra scan between evictions, so if the freshly-refilled pool's coldest is STILL
    /// warm (no colder key exists anywhere) the warm key is then evicted -- progress is
    /// guaranteed and the hottest keys are always the last to go.
    fn evict_to_fit_pooled(&mut self, budget_bytes: u64, now: UnixMillis) -> u64 {
        let volatile_only = self.eviction.volatile_only();
        let mut evicted: u64 = 0;
        // Whether the pool has been (re)scanned at all THIS call. Gates the one-shot
        // "refill-before-evicting-a-warm-candidate" quality retry to AT MOST ONCE per call,
        // NOT once per eviction. The retry only exists to refresh a STALE pool CARRIED from a
        // prior call (whose coldest leftovers may be warmer than keys inserted since); once
        // we have rescanned this call the pool reflects the current keyspace (no inserts
        // happen DURING an evict_to_fit call), so every later refill is already fresh and no
        // further retry is needed. Capping it per-call (rather than resetting after each
        // eviction) is what keeps eviction AMORTIZED at O(N/CAP): in the common case (a large
        // freq-0 cold tail >> CAP) the carried pool's first pop is cold, the retry never
        // fires, and the pool drains CAP victims per scan. (An all-warm keyspace - no freq-0
        // key - is inherently O(N)/episode for an EXACT-LFU scan with no random sampling;
        // that is the floor here, not a regression.)
        let mut refilled_this_call = false;
        loop {
            if self.used_memory() <= budget_bytes {
                break;
            }
            if self.evict_pool.is_empty() {
                // ONE bounded scan: reap expired entries (free budget) and harvest the
                // coldest live eligible candidates into the pool.
                self.refill_evict_pool(now);
                refilled_this_call = true;
                // The expired-reap inside refill may already have freed enough.
                if self.used_memory() <= budget_bytes {
                    break;
                }
                // Nothing evictable (e.g. volatile-only with no live TTL key): the caller
                // then replies -OOM. This is the termination guard for a no-progress refill.
                if self.evict_pool.is_empty() {
                    break;
                }
            }
            // Take the COLDEST candidate. The pool is stored HOTTEST-LAST (the coldest at
            // the back), so `pop` is the coldest victim in O(1) (never `remove(0)`).
            let Some(cand) = self.evict_pool.pop() else {
                // Defensive: the is_empty()/refill guard above means the pool is non-empty
                // here, but a pop guard keeps the loop total without an unwrap.
                break;
            };
            let db_idx = self.db_index(cand.db);
            let h = self.key_hash(&cand.key);
            // Re-validate against the live table: the pool can be STALE (the key may have
            // been deleted, overwritten away, persisted, expired, or WARMED UP since the
            // refill). Read only the fields we need under the immutable borrow, then drop it
            // before any mutating funnel call.
            let Some((lost_ttl, is_expired, live_freq)) = self
                .slot_table(db_idx, &cand.key)
                .and_then(|t| t.find(h, |e| e.key() == &cand.key[..]))
                .map(|obj| (obj.expire_at().is_none(), obj.is_expired(now), obj.freq()))
            else {
                // The key is gone since the refill: skip it (no progress; the pool shrank
                // by this pop, so an emptied pool triggers a fresh refill on the next turn).
                continue;
            };
            if is_expired {
                // Already expired since the refill: reap it (the lazy backstop). This frees
                // budget but is NOT counted as an eviction, exactly as the full-scan path
                // treated an expired candidate. It is forward progress.
                self.expire_if_due(cand.db, db_idx, &cand.key, now);
                continue;
            }
            if volatile_only && lost_ttl {
                // The pooled key LOST its TTL (a PERSIST between refill and pop): it is no
                // longer eligible under volatile-only, so skip it (it was spared). The pool
                // shrinks toward the next refill, which re-harvests only eligible keys.
                continue;
            }
            // The candidate's cold-ness is the MAX of the freq it was POOLED at
            // (`cand.freq`, the pool's ordering key) and its LIVE freq (`live_freq`): a key
            // never gets colder on its own, so `live_freq >= cand.freq`, but reading both
            // keeps the warm-check honest about a candidate that was already warm at harvest
            // AND one that warmed up since (a read between refill and pop).
            let cand_freq = cand.freq.max(live_freq);
            // COLDEST-FIRST across a CARRIED (stale) pool: a WARM candidate (freq > 0) from a
            // pool carried over from a PRIOR call might be popped while a colder key inserted
            // since that pool's scan exists (not yet pooled). If we have NOT rescanned this
            // call yet, do ONE fresh refill to surface the current coldest, then retry. This
            // fires AT MOST ONCE per call (gated by `refilled_this_call`, never reset): after
            // it, the pool reflects the current keyspace (no inserts happen DURING this call),
            // so every later pop - warm or cold - is genuinely the coldest available and is
            // evicted directly. This preserves O(N/CAP) amortization (the retry never fires on
            // the common freq-0-cold-tail pool) while sparing a hot key over a fresh cold one
            // in the carried-pool case. A genuinely warm-only keyspace still evicts (the
            // refill's coldest is also warm), and the loop terminates.
            if cand_freq > 0 && !refilled_this_call {
                self.evict_pool.clear();
                self.refill_evict_pool(now);
                refilled_this_call = true;
                continue;
            }
            if self.remove_object(cand.db, db_idx, &cand.key) {
                evicted += 1;
                // KEYSPACE NOTIFICATION (PROD-8): a maxmemory EVICTION fires the `evicted` event
                // (class `e`) on the victim key. `record` short-circuits on the disabled default,
                // so the eviction hot path is byte-identical when notifications are off.
                ironcache_config::notify::record(
                    ironcache_config::EventClass::Evicted,
                    "evicted",
                    &cand.key,
                    cand.db,
                );
            }
        }
        evicted
    }

    /// Refill the cache-mode eviction pool with one BOUNDED scan (the amortized-scan core
    /// of [`Self::evict_to_fit_pooled`]). It (a) reaps already-expired entries inline (the free
    /// lazy backstop, NOT an eviction) and (b) FILLS [`ShardStore::evict_pool`] with the
    /// coldest [`EVICT_POOL_CAP`] live eligible candidates, in the deterministic
    /// `(freq, scan_hash, key, db)` total order (ADR-0003).
    ///
    /// The coldest-CAP selection is a BOUNDED MAX-HEAP, not a clone-all + full sort: it is
    /// O(N log CAP) time and O(CAP) memory, and a candidate warmer than the warmest kept is
    /// skipped without allocating its key. So the per-refill cost no longer scales its
    /// allocations with the resident count N (the #285 follow-up), which is what makes eviction
    /// feasible at large resident sets; the selected victim set is byte-identical to the prior
    /// full sort.
    ///
    /// The pool is left sorted HOTTEST-LAST (coldest at the back) so the consumer takes the
    /// coldest with an O(1) `pop`. It holds at most `EVICT_POOL_CAP` entries, so it is
    /// BOUNDED and never per-key state.
    fn refill_evict_pool(&mut self, now: UnixMillis) {
        use std::collections::BinaryHeap;
        // A live eviction candidate, ordered COLDEST-FIRST by the deterministic total order
        // (freq, scan_hash, key, db) (ADR-0003). Declared before any statement to satisfy
        // clippy::items_after_statements. The DERIVED `Ord` compares the fields top-to-bottom,
        // which IS that exact order, so a max-heap of these keeps the WARMEST of the kept set on
        // top -- the candidate to drop when a colder one arrives. `db` is the FINAL tie-break so
        // the order is TOTAL: the same key bytes can be resident in two dbs at the same freq (each
        // db is its own table; `scan_hash` is key-only), and without `db` those two would compare
        // Equal and two shards with identical state could evict different keys.
        #[derive(PartialEq, Eq, PartialOrd, Ord)]
        struct ColdEntry {
            freq: u8,
            scan_h: u64,
            key: Box<[u8]>,
            db: u32,
        }
        let volatile_only = self.eviction.volatile_only();
        // ONE pass over every db's table. Rather than clone EVERY resident key into a Vec and
        // sort all N (the prior O(N) allocations + O(N log N) sort), keep ONLY the coldest
        // EVICT_POOL_CAP candidates in a bounded max-heap: O(N log CAP) comparisons, O(CAP)
        // memory, and -- the real win at large resident counts -- a candidate strictly WARMER than
        // the warmest kept is skipped WITHOUT cloning its key (only the coldest CAP, plus the
        // heap's transient pushes, ever allocate a key). The selected victim SET is byte-identical
        // to the prior full sort (same total order, exact key tie-break). Already-expired entries
        // are stashed (db, key) to reap AFTER the scan, since reaping mutates the table this loop
        // borrows immutably. The 2-bit freq is read straight off the object (no policy lookup).
        let mut heap: BinaryHeap<ColdEntry> = BinaryHeap::with_capacity(EVICT_POOL_CAP + 1);
        let mut expired: Vec<(u32, Box<[u8]>)> = Vec::new();
        for (db_idx, slots) in self.dbs.iter().enumerate() {
            let db = db_idx as u32;
            // Every slot table of this DB (#570): an untouched DB has an empty slot `Vec`.
            for obj in slots.iter().flat_map(|t| t.iter()) {
                if obj.is_expired(now) {
                    // Collected only to reap it for free below (NOT an eviction candidate).
                    expired.push((db, obj.key().to_vec().into_boxed_slice()));
                    continue;
                }
                if volatile_only && obj.expire_at().is_none() {
                    // Volatile-only spares a live non-TTL entry: it is NOT a candidate.
                    continue;
                }
                let freq = obj.freq();
                let scan_h = scan_hash(obj.key());
                // Bounded selection: once the heap holds CAP, a candidate that is NOT strictly
                // colder than the warmest kept (the heap top) cannot be among the coldest CAP, so
                // skip it with NO key clone. (Two distinct keys never compare Equal, so the only
                // keep case is strictly colder.)
                if heap.len() >= EVICT_POOL_CAP {
                    let top = heap
                        .peek()
                        .expect("len >= CAP (> 0) so the heap is non-empty");
                    let colder = freq
                        .cmp(&top.freq)
                        .then_with(|| scan_h.cmp(&top.scan_h))
                        .then_with(|| obj.key().cmp(top.key.as_ref()))
                        .then_with(|| db.cmp(&top.db))
                        == core::cmp::Ordering::Less;
                    if !colder {
                        continue;
                    }
                }
                heap.push(ColdEntry {
                    freq,
                    scan_h,
                    key: obj.key().to_vec().into_boxed_slice(),
                    db,
                });
                if heap.len() > EVICT_POOL_CAP {
                    heap.pop(); // drop the warmest, keeping the coldest CAP
                }
            }
        }
        // Reap already-expired candidates: dead weight the lazy/active backstops would reap
        // anyway, so freeing them is free budget and counts as forward progress (NOT an eviction).
        // May by itself bring `used` under budget (the caller re-checks after this returns).
        for (db, key) in &expired {
            let db_idx = self.db_index(*db);
            self.expire_if_due(*db, db_idx, key, now);
        }
        // The heap holds the coldest EVICT_POOL_CAP. `into_sorted_vec` yields them COLDEST-FIRST;
        // the pool is stored HOTTEST-FIRST / COLDEST-LAST so the consumer `pop`s the coldest in
        // O(1), so reverse once (O(CAP)).
        let mut coldest = heap.into_sorted_vec();
        coldest.reverse();
        self.evict_pool = coldest
            .into_iter()
            .map(|c| PoolCandidate {
                db: c.db,
                key: c.key,
                freq: c.freq,
            })
            .collect();
    }

    /// Evict by driving the policy's OWN candidate roster round-by-round through
    /// [`EvictionHook::select_victim`] (the [`VictimStrategy::Roster`] path for `Random`
    /// and `WTinyLfu`). Each round asks for a `(db, key)` and deletes it (firing
    /// `on_remove` + freeing its bytes through the accounting hook), stopping as soon as
    /// the budget is met. If `select_victim` returns `None` the policy can free no more, so
    /// we return what we freed so far; the caller then decides whether to reply `-OOM`.
    ///
    /// ## Volatile-only enforcement (the #46 re-eligibility fix)
    ///
    /// For a `volatile_only` policy (the `volatile-*` family) only TTL-bearing keys
    /// are eligible. The frozen hooks do not pass TTL to the policy, so the FILTER
    /// lives here, where the store can read `expire_at`: a victim with NO TTL is
    /// RE-REGISTERED into the policy (NON-DESTRUCTIVELY, via
    /// [`EvictionPolicy::re_register`]) rather than dropped, and the loop asks for the
    /// next victim. Re-registering (instead of an `on_remove` drop) is the #46 fix: a
    /// non-TTL key the store declines to evict STAYS an eviction candidate, so once a later
    /// EXPIRE attaches a TTL it becomes eligible. The scan is bounded by tracking the
    /// distinct keys examined-and-skipped this call: once that set covers the whole live
    /// keyspace with no eligible TTL-bearing victim found, the loop returns what it freed
    /// so far (zero, here), leaving the over-budget write to be rejected `-OOM` (matching
    /// Redis volatile-* with no expirable keys).
    ///
    /// `now` is consulted only to skip an ALREADY-expired victim (it will be reaped
    /// lazily anyway). The iteration cap is a defensive secondary bound.
    fn evict_to_fit_roster(&mut self, budget_bytes: u64, now: UnixMillis) -> u64 {
        let volatile_only = self.eviction.volatile_only();
        let mut evicted: u64 = 0;
        // The bounded-scan guard for the #46 re-eligibility fix, as a DISTINCT-KEY set.
        // Under a volatile-* policy a non-TTL victim is RE-REGISTERED (kept as a
        // candidate) rather than dropped, so the policy can keep offering the same
        // non-TTL keys forever. We bound the scan by recording each DISTINCT (db, key)
        // we have examined-and-skipped this call: the loop dispatches no-progress (lets
        // the caller reply -OOM) ONLY once that set covers the WHOLE live keyspace
        // (`skipped.len() >= self.len()`), i.e. every live key has been offered and none
        // was an eligible TTL-bearing victim (matching Redis volatile-*
        // OOM-when-no-evictable-volatile-key).
        //
        // Why a DISTINCT set, not the old CONSECUTIVE-skip counter: `re_register` feeds
        // a skipped key back so `select_victim` can re-offer it, and the policy may
        // re-offer the SAME non-TTL key several times before it reaches an eligible
        // TTL victim parked deeper in its queues. A consecutive-skip counter trips on
        // those repeats and falsely reports OOM while an evictable volatile key still
        // exists; counting DISTINCT keys does not trip until genuinely every live key
        // has been offered, so a reachable TTL victim is always found first.
        //
        // Any actual eviction (or an expired-reap) shrinks the live keyspace and frees
        // budget, so we CLEAR the set on that forward progress: the bound is then
        // re-measured against the new (smaller) keyspace.
        let mut skipped: HashSet<(u32, Box<[u8]>)> = HashSet::new();
        // A defensive secondary cap: even if a policy mis-behaves (e.g. re-offers a key
        // the set already holds without ever offering the rest), the loop ends. With the
        // distinct-set bound above this should never be the binding limit.
        let max_rounds = self.len().saturating_mul(4).saturating_add(64);
        let mut rounds = 0usize;
        // Strict `>`: free down to `used <= budget`, matching Redis getMaxmemoryState
        // (under-limit at `used <= maxmemory`). At used==budget the loop does not run.
        while self.used_memory() > budget_bytes {
            if rounds >= max_rounds {
                break;
            }
            rounds += 1;
            // freq-in-object: the policy reads each candidate's S3-FIFO freq through a
            // `VictimFreq` backed by the store's own tables. `self.eviction` and
            // `self.dbs` are SEPARATE fields, so we split the borrow: bind each as its
            // own `&mut` rather than going through `&mut self` (which would double-borrow
            // when the closure over `dbs` is handed to `eviction.select_victim`).
            let evict = &mut self.eviction;
            let mut freq = TableVictimFreq {
                dbs: &mut self.dbs,
                hasher: &self.hasher,
                slots: self.slots,
            };
            let Some((db, key)) = evict.select_victim(&mut freq) else {
                break;
            };
            let db_idx = self.db_index(db);
            // Inspect the candidate (immutable borrow), extract the state, then drop
            // the borrow before any mutating call (the hooks borrow self mut).
            let kh = self.key_hash(&key);
            let (present, is_expired, lacks_ttl) = match self
                .slot_table(db_idx, &key)
                .and_then(|t| t.find(kh, |e| e.key() == &key[..]))
            {
                Some(obj) => (true, obj.is_expired(now), obj.expire_at().is_none()),
                None => (false, false, true),
            };
            // A STALE victim (the policy offered a key the store no longer holds, e.g.
            // a Random roster entry the store did not actually delete on a prior skip):
            // prune it from the policy so it is not re-offered, then ask for the next.
            if !present {
                self.eviction.on_remove(db, &key, 0);
                continue;
            }
            // An already-expired victim is reaped by the lazy backstop rather than
            // counted as an eviction (it would have read as absent anyway); this also
            // drops it from the policy queue via expire_if_due's on_remove. This is
            // forward progress, so clear the distinct-skip set.
            if is_expired {
                self.expire_if_due(db, db_idx, &key, now);
                skipped.clear();
                continue;
            }
            if volatile_only && lacks_ttl {
                // Only TTL-bearing keys are eligible. A non-TTL victim is NOT deleted
                // and NOT dropped from the policy: it is RE-REGISTERED so it remains a
                // candidate (the #46 re-eligibility fix). Record it as a DISTINCT skip
                // and re-register it. Stop ONLY once the distinct-skip set covers the
                // whole live keyspace (every live key offered, none an eligible TTL
                // victim), so an eligible main-resident TTL victim is always reached
                // before the bound trips. The membership check is BEFORE the insert so a
                // re-offered key does not grow the set.
                self.eviction.re_register(db, &key);
                skipped.insert((db, key));
                if skipped.len() >= self.len() {
                    break;
                }
                continue;
            }
            if self.remove_object(db, db_idx, &key) {
                evicted += 1;
                // KEYSPACE NOTIFICATION (PROD-8): the roster eviction path (Random / volatile-*)
                // fires the same `evicted` event (class `e`) on the victim as the pooled path.
                // Zero-cost when notifications are disabled.
                ironcache_config::notify::record(
                    ironcache_config::EventClass::Evicted,
                    "evicted",
                    &key,
                    db,
                );
                // Forward progress: the keyspace shrank and budget was freed, so clear
                // the skip set; a subsequent stretch of non-TTL skips is measured afresh
                // against the (now smaller) keyspace.
                skipped.clear();
            }
            // If the victim was already gone (a stale queue entry), the loop simply
            // asks for the next victim; it does not count as an eviction.
        }
        evicted
    }
}

impl<A: AccountingHook> ironcache_storage::PolicySwap for ShardStore<Policy, A> {
    /// Rebuild this shard's eviction policy from `name` (CONFIG.md `maxmemory-policy`
    /// hot-swap), seeded from `rng_seed` (the caller drew it through the Env RNG seam,
    /// ADR-0003: no std rand in the library). Implemented ONLY for the concrete
    /// [`Policy`] hook (the swap installs a fresh `Policy`), not for the generic `E`.
    ///
    /// The previous policy's RANKING HISTORY (S3-FIFO queue positions / W-TinyLFU sketch
    /// counts / LRU recency) is DISCARDED: the new policy starts with empty eviction
    /// ordering. CONFIG.md and Redis both document this ("the policy switch takes time to
    /// adjust"). The KEYSPACE and the byte accounting are UNTOUCHED, so no resident data
    /// is lost.
    ///
    /// IC-1 fix: the new policy is RE-SEEDED from the live keyspace BEFORE returning, so
    /// it has eviction candidates immediately. Without this, the fresh policy has an
    /// EMPTY roster while the keyspace is populated, so [`EvictionHook::select_victim`]
    /// returns `None` and a populated, over-budget shard would reply a spurious `-OOM`
    /// (eviction cannot find a victim) until every key happens to be re-observed by a
    /// later access/insert. We iterate every live entry in every db and call
    /// [`EvictionHook::on_insert`] with the SAME logical-byte accounting the normal
    /// insert path uses ([`KvObj::accounted_bytes`]), skipping any entry already past its
    /// deadline at `now` (a lazily-expired key must not be re-seeded as a candidate; the
    /// lazy/active backstops will reap it). This is O(live keys), once per (rare) swap,
    /// off the hot path. Returns `false` for an unrecognized `name` (leaving the existing
    /// policy in place); the dispatch layer validates the name first, so that is the
    /// defensive path.
    fn set_policy_by_name(&mut self, name: &str, rng_seed: u64, now: UnixMillis) -> bool {
        let Some(policy) = map_policy_name(name, rng_seed) else {
            return false;
        };
        self.eviction = policy;
        // Re-seed the fresh policy's candidate roster from the live keyspace so
        // select_victim works immediately (IC-1). Collect (db, key, bytes) first so the
        // immutable db borrow ends before the mutable eviction-hook calls; skip entries
        // whose deadline has strictly passed at `now` (lazily-expired, must not seed).
        let mut seed_set: Vec<(u32, Box<[u8]>, usize)> = self
            .dbs
            .iter()
            .enumerate()
            .flat_map(|(db_idx, slots)| {
                let db = db_idx as u32;
                // Every slot table of this DB (#570).
                slots.iter().flat_map(|t| t.iter()).filter_map(move |obj| {
                    if obj.is_expired(now) {
                        None
                    } else {
                        Some((
                            db,
                            obj.key().to_vec().into_boxed_slice(),
                            obj.accounted_bytes(),
                        ))
                    }
                })
            })
            .collect();
        // Re-seed in a DETERMINISTIC order (ADR-0003): the `hashbrown` map iteration
        // order varies per instance (a per-table RandomState), so feeding `on_insert` in
        // raw iteration order would make a *-random policy's re-seeded roster (and thus
        // its seeded victim choice) differ run-to-run. Sort by (db, scan_hash, key bytes)
        // -- the same stable, resize-invariant order SCAN/RANDOMKEY use -- so two shards
        // with identical keyspaces and the same RNG seed re-seed identically.
        seed_set.sort_unstable_by(|(da, ka, _), (db, kb, _)| {
            da.cmp(db)
                .then_with(|| scan_hash(ka).cmp(&scan_hash(kb)))
                .then_with(|| ka.cmp(kb))
        });
        for (db, key, bytes) in seed_set {
            self.eviction.on_insert(db, &key, bytes);
        }
        true
    }

    fn apply_encoding_thresholds(&mut self, thresholds: EncodingThresholds) {
        // Refresh the cached snapshot the encoding-transition decision reads (#40). A plain field
        // write; existing keys keep their encoding (this only changes WHEN a future insert converts).
        self.set_encoding_thresholds(thresholds);
    }
}

impl<E: EvictionHook, A: AccountingHook> ironcache_storage::ActiveExpiry for ShardStore<E, A> {
    /// Reap `key` ONLY if it is present and its stored deadline has STRICTLY passed at
    /// `now` (the active-drain re-check, EXPIRATION.md). The timing wheel may offer a
    /// STALE entry (a re-TTL'd / PERSISTed / overwritten key), so this re-checks the
    /// real `expire_at` and reaps only a genuinely-expired key (firing the
    /// eviction/accounting remove hooks). A live key is left untouched and reported
    /// `false`. The lazy-expired accumulator is NOT bumped here (the serve loop counts
    /// active-drain reclamations separately into `expired_keys`), avoiding a double
    /// count.
    fn reap_if_expired(&mut self, db: u32, key: &[u8], now: UnixMillis) -> bool {
        let db_idx = self.db_index(db);
        let h = self.key_hash(key);
        let expired = self
            .slot_table(db_idx, key)
            .and_then(|t| t.find(h, |e| e.key() == key))
            .is_some_and(|o| o.is_expired(now));
        if !expired {
            return false;
        }
        // remove_object fires on_remove + frees the bytes through the accounting hook.
        self.remove_object(db, db_idx, key)
    }
}

impl<E: EvictionHook, A: AccountingHook> Keyspace for ShardStore<E, A> {
    /// One bounded SCAN batch in ascending [`scan_hash`] order (KEYSPACE.md). See the
    /// trait method docs for the contract; the resume predicate matches KEYSPACE.md
    /// exactly: a key is emitted iff `scan_hash(k) > cursor_hash` OR
    /// (`scan_hash(k) == cursor_hash` AND `k_bytes > last_emitted_bytes_at_that_hash`),
    /// so two distinct keys colliding on the same 64-bit hash are BOTH returned.
    ///
    /// The cursor's integer IS the last-emitted `scan_hash` (PR-4a single-slot: the
    /// reserved slot high-bits are zero). The equal-hash discriminator is NOT carried
    /// in the integer; instead, on resume, same-hash keys whose bytes sort at or before
    /// the largest already-emitted-at-that-hash are skipped, which re-derives the
    /// discriminator from the key bytes without widening the wire token.
    ///
    /// Implementation: build the sorted `(scan_hash, key)` view of the live db ONCE per
    /// call (O(n log n), the "sort each batch on the fly" mechanism KEYSPACE.md names),
    /// binary-search to the resume point, then examine up to `count` keys applying the
    /// `keep` filter, skipping lazily-expired ones. Because `scan_hash` is
    /// resize-invariant, the sorted order is identical before and after a `hashbrown`
    /// resize, so iteration is total across a resize.
    fn scan_step(
        &mut self,
        db: u32,
        cursor: ScanCursor,
        count: usize,
        now: UnixMillis,
        mut keep: impl FnMut(&[u8], DataType) -> bool,
    ) -> (ScanCursor, Vec<Box<[u8]>>) {
        let db_idx = self.db_index(db);
        let Some(slots) = self.dbs.get(db_idx) else {
            return (ScanCursor::START, Vec::new());
        };

        // The sorted (scan_hash, key_bytes) view over EVERY slot table of the DB (#570).
        // `scan_hash` is recomputed from the key bytes (read out of each entry), NOT from
        // the table's internal hasher, so the order is stable across calls and across a
        // resize (KEYSPACE.md). It is also SLOT-INDEPENDENT: partitioning the DB into slots
        // does not change any key's `scan_hash`, so the MERGED order over all slots is
        // byte-identical to the pre-#570 single table's, and the cursor stays the exact same
        // `scan_hash` threshold. That is why the per-slot split needs NO cursor-format change:
        // SCAN still returns every key exactly once across pages, and the wire token and the
        // cross-shard composite cursor are unchanged. Sorting by (hash, bytes) gives a total
        // order even for equal-hash keys. Each `&[u8]` borrows the key INSIDE its entry.
        let mut order: Vec<(u64, &[u8])> = slots
            .iter()
            .flat_map(|t| t.iter())
            .map(|e| (scan_hash(e.key()), e.key()))
            .collect();
        if order.is_empty() {
            // Empty (or untouched) db -> complete immediately (cursor 0).
            return (ScanCursor::START, Vec::new());
        }
        order.sort_unstable();

        // Walk the sorted order, choosing which keys to EXAMINE this batch and what the
        // next cursor is (the pure cursor-stepping core, unit-tested in isolation). The
        // shard's `scan_band_bits` makes the next cursor BAND-ALIGNED for the cross-shard
        // composite cursor (0 on a single-shard server -> byte-identical to before).
        let plan = scan_plan(&order, cursor, count, self.scan_band_bits);

        // Realize the plan: for each examined key, skip a lazily-expired one (the lazy
        // backstop / active drain reclaim it; SCAN never returns it) and apply the
        // MATCH/TYPE `keep` filter BEFORE cloning the key into the result.
        let mut kept: Vec<Box<[u8]>> = Vec::with_capacity(plan.examined.len());
        for &key in &plan.examined {
            // Re-find the entry in ITS slot table (#570); the `order`/`plan.examined` slices
            // borrow the keys inside the entries, and `self.slot_table` / `self.hasher` are
            // further shared borrows, so all the immutable borrows coexist.
            if let Some(obj) = self
                .slot_table(db_idx, key)
                .and_then(|t| t.find(self.hasher.hash_one(key), |e| e.key() == key))
            {
                if obj.is_expired(now) {
                    continue;
                }
                if keep(key, obj.data_type()) {
                    kept.push(key.to_vec().into_boxed_slice());
                }
            }
        }
        (plan.next, kept)
    }

    fn db_len(&self, db: u32) -> usize {
        let db_idx = self.db_index(db);
        // RAW table length (Redis does not active-expire on DBSIZE): the dict size,
        // including not-yet-reaped expired keys. No lazy backstop here. Sum across every
        // slot table of the DB (#570).
        self.dbs
            .get(db_idx)
            .map_or(0, |slots| slots.iter().map(|t| t.len()).sum())
    }

    fn random_key(&mut self, db: u32, pick: u64, now: UnixMillis) -> Option<Box<[u8]>> {
        let db_idx = self.db_index(db);
        let slots = self.dbs.get(db_idx)?;
        // Total count across every slot table of the DB (#570).
        let n: usize = slots.iter().map(|t| t.len()).sum();
        if n == 0 {
            return None;
        }
        // The caller drew `pick` from the Env RNG (ADR-0003: the store reads no RNG).
        // Map it to a starting index, then probe forward DETERMINISTICALLY in the
        // sorted scan order, skipping expired keys, so an expired key at the picked
        // position does not yield `None` while live keys remain. The order carries the
        // key + its live/expired flag so no re-lookup is needed. Merged over all slots so
        // the sorted order (and thus the deterministic pick) is slot-independent.
        let mut order: Vec<(&[u8], bool)> = slots
            .iter()
            .flat_map(|t| t.iter())
            .map(|e| (e.key(), e.is_expired(now)))
            .collect();
        order.sort_unstable_by(|a, b| scan_hash(a.0).cmp(&scan_hash(b.0)).then(a.0.cmp(b.0)));
        let start = (pick % n as u64) as usize;
        for off in 0..n {
            let idx = (start + off) % n;
            let (key, expired) = order[idx];
            if !expired {
                return Some(key.to_vec().into_boxed_slice());
            }
        }
        None
    }

    fn flush_db(&mut self, db: u32) -> u64 {
        let db_idx = self.db_index(db);
        // Collect the keys across every slot table of the DB (#570), releasing the table
        // borrow before the removal funnel mutates.
        let keys: Vec<Box<[u8]>> = match self.dbs.get(db_idx) {
            Some(slots) => slots
                .iter()
                .flat_map(|t| t.iter())
                .map(|e| e.key().to_vec().into_boxed_slice())
                .collect(),
            None => return 0,
        };
        let mut removed = 0u64;
        for key in &keys {
            // remove_object fires the eviction/accounting remove hooks and frees bytes
            // (and notifies the WATCH version of each resident watched key, PR-10b).
            if self.remove_object(db, db_idx, key) {
                removed += 1;
            }
        }
        // WATCH (PR-10b): also dirty every key WATCHED in this db that was NOT resident
        // (a watched-absent key), matching Redis touchAllWatchedKeysOnDb -- a FLUSHDB
        // signals all of the db's watched keys, not only the ones that held a value.
        self.touch_all_watches_in_db(db);
        removed
    }

    fn flush_all(&mut self) -> u64 {
        let mut removed = 0u64;
        for db in 0..self.dbs.len() as u32 {
            removed += self.flush_db(db);
        }
        removed
    }

    fn move_object(
        &mut self,
        src_db: u32,
        src: &[u8],
        dst_db: u32,
        dst: &[u8],
        mode: MoveMode,
        replace: bool,
        now: UnixMillis,
    ) -> MoveOutcome {
        let src_idx = self.db_index(src_db);
        let dst_idx = self.db_index(dst_db);

        // A lazily-expired source reads as absent (run the backstop first).
        if !self.expire_if_due(src_db, src_idx, src, now) {
            return MoveOutcome::NoSource;
        }
        // A RENAME/COPY/MOVE onto its own identical (db,key) is a special case: RENAME
        // of a key to itself is a no-op success in Redis; treat src==dst as a move that
        // leaves the value where it is.
        let same_slot = src_idx == dst_idx && src == dst;
        if same_slot {
            return match mode {
                MoveMode::Rename => MoveOutcome::Moved,
                MoveMode::Copy => MoveOutcome::Copied,
            };
        }

        // Destination occupancy gate (RENAMENX-0 / COPY-without-REPLACE / MOVE-occupied).
        // A lazily-expired destination counts as absent.
        let dst_live = self.expire_if_due(dst_db, dst_idx, dst, now);
        if dst_live && !replace {
            return MoveOutcome::DestExists;
        }

        // Take the source object INTACT (preserving encoding + remaining TTL). Re-key it
        // to the destination key bytes; the value representation and `expire_at` are
        // carried unchanged (KEYSPACE.md "moves the value object INTACT"). For a Str
        // entry `rekey` rebuilds the blob with the new embedded key; for a Coll it is a
        // field write.
        let src_h = self.key_hash(src);
        let Some(mut obj) = self
            .slot_table(src_idx, src)
            .and_then(|t| t.find(src_h, |e| e.key() == src))
            .cloned()
        else {
            return MoveOutcome::NoSource;
        };
        obj.rekey(dst);

        // Write the destination through the funnel (fires insert hooks, accounts bytes;
        // a replaced live destination is credited inside put_object).
        self.put_object(dst_db, dst_idx, dst, obj);

        match mode {
            MoveMode::Rename => {
                // Remove the source (fires remove hooks, credits its bytes).
                self.remove_object(src_db, src_idx, src);
                MoveOutcome::Moved
            }
            MoveMode::Copy => MoveOutcome::Copied,
        }
    }

    fn swap_db(&mut self, a: u32, b: u32) {
        let ai = self.db_index(a);
        let bi = self.db_index(b);
        if ai != bi {
            // O(1) Vec element swap: the two DBs' whole slot `Vec`s trade places (#570); no
            // entry is created or destroyed, so no hook fires and the accounting total is
            // unchanged (the same entries are still resident, just under different db ids).
            self.dbs.swap(ai, bi);
            // WATCH (PR-10b): the contents under db `a` and db `b` both changed wholesale,
            // so every key watched in EITHER db is dirtied. Redis treats SWAPDB like a
            // flush of both dbs for watch purposes (the watched (db,key) now maps to a
            // different value or to absence). Bumps all watch slots in both dbs.
            self.touch_all_watches_in_db(a);
            self.touch_all_watches_in_db(b);
        }
    }
}

impl<E: EvictionHook, A: AccountingHook> ironcache_storage::Watch for ShardStore<E, A> {
    /// Register `(db, key)` as watched and snapshot it (TRANSACTIONS.md per-key
    /// dirty-CAS, PR-10b). Ensures a [`WatchSlot`] exists (created at the CURRENT
    /// `version_clock` if the key was never watched), increments its watcher count and
    /// the `watched_count` fast-path flag, and returns the [`WatchEntry`] carrying the
    /// slot version + whether the key is present-and-live at `now`.
    ///
    /// The present/absent probe runs the LAZY expiry backstop ([`Self::expire_if_due`]):
    /// a key already past its deadline at WATCH time is reaped now and recorded ABSENT
    /// (`present_at_watch = false`), so an already-expired key watched and left absent is
    /// clean at EXEC (the Redis 6.0.9+ `wk->expired` rule). That reap goes through
    /// `remove_object`, which notifies the watch -- but the slot is (re)stamped to the
    /// CURRENT clock AFTER the probe below, so the snapshot version matches the
    /// post-probe slot and the just-reaped key does not read as spuriously dirty.
    fn watch_snapshot(&mut self, db: u32, key: &[u8], now: UnixMillis) -> WatchEntry {
        let db_idx = self.db_index(db);
        // Probe present-and-live FIRST (this may lazily reap an already-expired key,
        // bumping its slot if one already existed from a prior watcher). The snapshot
        // version is read AFTER, so it reflects the post-reap state.
        let present = self.expire_if_due(db, db_idx, key, now);
        let probe = (db, key.to_vec().into_boxed_slice());
        let version = match self.watch_versions.entry(probe) {
            WatchMapEntry::Occupied(mut e) => {
                let slot = e.get_mut();
                slot.watchers += 1;
                self.watched_count += 1;
                slot.version
            }
            WatchMapEntry::Vacant(e) => {
                let version = self.version_clock;
                e.insert(WatchSlot {
                    version,
                    watchers: 1,
                });
                self.watched_count += 1;
                version
            }
        };
        WatchEntry {
            db,
            key: key.to_vec().into_boxed_slice(),
            version,
            present_at_watch: present,
        }
    }

    /// Whether `entry`'s key has been modified since the snapshot (the EXEC dirty-CAS
    /// check, PR-10b). Dirty iff the slot's CURRENT version differs from
    /// `entry.version`, OR the current present/absent status differs from
    /// `entry.present_at_watch`.
    ///
    /// The present/absent check runs the lazy backstop, so a watched key whose deadline
    /// passed between WATCH and EXEC reads as absent here -> dirty if it was present at
    /// watch (and that reap also bumped the version, so the version check would catch it
    /// too; both signals agree). A watched-absent key that a write created reads present
    /// -> dirty. If the slot is gone (e.g. all other watchers unwatched and the key was
    /// never written), only the present/absent comparison remains, which is correct: an
    /// untouched key has the same present/absent status it had at watch.
    fn watch_is_dirty(&mut self, entry: &WatchEntry, now: UnixMillis) -> bool {
        let db_idx = self.db_index(entry.db);
        let present_now = self.expire_if_due(entry.db, db_idx, &entry.key, now);
        if present_now != entry.present_at_watch {
            return true;
        }
        let probe = (entry.db, entry.key.clone());
        match self.watch_versions.get(&probe) {
            // A live slot: dirty iff its version moved past the snapshot.
            Some(slot) => slot.version != entry.version,
            // No slot (the key was never written while watched and other watchers left):
            // the version cannot have moved, so cleanliness rests on the present/absent
            // check above (already equal here), so it is clean.
            None => false,
        }
    }

    /// Deregister `entries` (PR-10b): per entry decrement the slot's watcher count and
    /// the `watched_count` flag, removing the slot when the last watcher leaves so the
    /// store carries no watch state for an unwatched key (and the fast path returns to a
    /// single integer check once every connection has unwatched).
    fn unwatch(&mut self, entries: &[WatchEntry]) {
        for entry in entries {
            let probe = (entry.db, entry.key.clone());
            if let WatchMapEntry::Occupied(mut e) = self.watch_versions.entry(probe) {
                let slot = e.get_mut();
                slot.watchers = slot.watchers.saturating_sub(1);
                // Each entry corresponds to exactly one watcher increment from
                // `watch_snapshot`, so decrement the fast-path flag in lockstep.
                self.watched_count = self.watched_count.saturating_sub(1);
                if slot.watchers == 0 {
                    e.remove();
                }
            }
        }
    }
}

impl<E: EvictionHook, A: AccountingHook> ShardStore<E, A> {
    /// Whether `key` is PRESENT and LIVE (not expired as of `now`) in `db` (HA-6 online slot
    /// migration: the source-side ASK decision needs to know if a key has already migrated away /
    /// never existed vs is still here). A pure READ: it never reaps the key or fires any hook (a
    /// lazily-expired key reports `false` but is left for the normal reap path), so it is safe to
    /// call from the cold redirect path. Independent of the WATCH funnel and the hot put/get path;
    /// the default (non-migration) routing never calls it.
    #[must_use]
    pub fn contains_live(&self, db: u32, key: &[u8], now: UnixMillis) -> bool {
        let db_idx = self.db_index(db);
        let h = self.key_hash(key);
        self.slot_table(db_idx, key)
            .and_then(|t| t.find(h, |e| e.key() == key))
            .is_some_and(|o| !o.is_expired(now))
    }

    /// Borrow the accounting hook (test/introspection helper).
    #[must_use]
    pub fn accounting(&self) -> &A {
        &self.accounting
    }

    /// Take (and reset) the count of keys reaped by the LAZY expiry-on-read backstop
    /// since the last call (PR-3b INFO `expired_keys`, the lazy-path signal). The
    /// serve loop calls this after each command and folds the result into the shard's
    /// `expired_keys` counter, so both the lazy backstop and the active timing-wheel
    /// drain contribute to `expired_keys`. Not a waist method; an introspection
    /// accumulator on the concrete store.
    pub fn take_lazy_expired(&mut self) -> u64 {
        std::mem::take(&mut self.lazy_expired)
    }

    /// The number of `(db, key)` WATCH slots the store currently tracks (== the
    /// fast-path `watched_count` flag, PR-10b). Zero when no connection is watching
    /// anything, in which case the write-funnel notify ([`Self::touch_watch`]) does a
    /// single integer check and returns with NO hash probe. Test/introspection helper:
    /// the HOT-PATH test asserts this stays `0` for a connection that never WATCHes, so
    /// the funnel notify provably does no work on the non-watching path. Not a waist
    /// method.
    #[must_use]
    pub fn watched_count(&self) -> usize {
        self.watched_count
    }

    /// The current per-shard WATCH version clock (the deterministic u64 COUNTER, PR-10b).
    /// It is bumped ONLY when a watched key is touched by the write funnel; a write while
    /// NOTHING is watched leaves it unchanged (the funnel fast path returns before the
    /// bump). Test/introspection helper: the HOT-PATH test asserts it does not advance
    /// across writes when `watched_count == 0`, proving the notify reads no clock/RNG and
    /// does no per-key work on the non-watching path. Not a waist method.
    #[must_use]
    pub fn version_clock(&self) -> u64 {
        self.version_clock
    }

    /// Install the data-plane WRITE OBSERVER (HA-5a). After this, EVERY applied write to
    /// the shard is reported to `observer` (create/overwrite/in-place edit/TTL change ->
    /// [`WriteObserver::on_put`] with the committed post-image; delete/expiry/flush/
    /// eviction/RENAME-source/emptied-collection -> [`WriteObserver::on_remove`]). Flips the
    /// `repl_active` fast-path gate ON, so the funnel sites begin firing.
    ///
    /// Store-public so serve/repl installs it on the owning core when replication (HA-7) or
    /// migration (HA-6) starts for this shard. Replaces any previously-installed observer
    /// (the prior box is dropped). The shard is single-threaded, so this is a plain `&mut`
    /// field write with no synchronization (ADR-0002/0005).
    pub fn set_write_observer(&mut self, observer: Box<dyn WriteObserver>) {
        self.repl_observer = Some(observer);
        self.repl_active = true;
    }

    /// Clear the data-plane write observer (HA-5a): drop the installed box and flip the
    /// `repl_active` fast-path gate back OFF, so the funnel sites return to the default
    /// single-bool-branch cost profile (identical to a store that never had an observer).
    /// Store-public so serve/repl tears it down when replication/migration stops. A no-op
    /// (beyond clearing the already-`false` flag) if no observer was installed.
    pub fn clear_write_observer(&mut self) {
        self.repl_observer = None;
        self.repl_active = false;
    }

    /// Whether a write observer is currently installed (the `repl_active` fast-path gate,
    /// HA-5a). `false` by default and after [`Self::clear_write_observer`]; `true` between a
    /// [`Self::set_write_observer`] and the next clear. Test/introspection helper: the
    /// HOT-PATH test asserts this stays `false` on the default path, proving the funnel
    /// fires no observer work when none is installed. Not a waist method.
    #[must_use]
    pub fn write_observer_active(&self) -> bool {
        self.repl_active
    }

    /// Mark this shard a PASSIVE REPLICA (HA-7d, CARRY-FORWARD 2) or clear it. When passive,
    /// the shard removes keys ONLY from the replication stream: a due key is reported absent
    /// on read but not physically removed (see [`Self::expire_if_due`]). The caller (the
    /// replica-attach path) sets this `true` right after swapping in the full-synced store,
    /// and the active-expiry/eviction reapers are never invoked on it. Default `false`.
    pub fn set_passive(&mut self, passive: bool) {
        self.passive = passive;
    }

    /// Whether this shard is a passive replica (removal only via the replication stream).
    #[must_use]
    pub fn is_passive(&self) -> bool {
        self.passive
    }

    /// Install the LIVE collection-encoding thresholds snapshot (#40). The serve/dispatch layer
    /// calls this with the runtime overlay's current [`EncodingThresholds`] when the runtime config
    /// generation moves (the SAME per-command generation check the eviction-policy hot-swap rides),
    /// so a `CONFIG SET *-max-listpack-*` reaches this shard's encoding-transition decision for
    /// FUTURE inserts. A plain `&mut` field write on the single-threaded shard (ADR-0002/0005), off
    /// the per-command hot path (only on a generation change). Existing keys are NOT re-encoded.
    pub fn set_encoding_thresholds(&mut self, thresholds: EncodingThresholds) {
        self.encoding_thresholds = thresholds;
    }

    /// The store's CURRENT collection-encoding thresholds snapshot (#40). Test/introspection helper;
    /// the encoding-transition decision reads this `Copy` field directly.
    #[must_use]
    pub fn encoding_thresholds(&self) -> EncodingThresholds {
        self.encoding_thresholds
    }

    /// Insert a fully-formed [`KvObj`] under `db`, bypassing the string-only
    /// command path. This is the only way in PR-2a to plant a NON-string value
    /// (PR-2a commands produce only Strings), so the WRONGTYPE path of GET/GETSET/
    /// STRLEN can be exercised before collections land. The accounting/eviction
    /// hooks fire as for any insert. Reserved for tests and the future collection
    /// commands; documented as a seam, not a fifth primitive.
    pub fn insert_object(&mut self, db: u32, obj: KvObj) {
        let db_idx = self.db_index(db);
        let key = obj.key.clone();
        // The public builder/transfer type is `KvObj` (tests construct it and set its
        // fields directly); convert it to the single-allocation table `Entry` at the
        // funnel boundary.
        let entry = Entry::from_kvobj(obj);
        self.put_object(db, db_idx, &key, entry);
    }

    /// Remove EVERY live key (across all databases) for which `pred(key)` is true, returning the
    /// count removed. A LOCAL-CLEANUP primitive (HA-6 M2): when an aborted / un-won slot import
    /// ends, the importer purges the partially-merged slot's keys (`pred = |k| key_slot(k) ==
    /// slot`, the slot predicate supplied by the caller, which owns the protocol `key_slot` -- the
    /// store stays slot-agnostic). Without this an aborted migration LEAKS the merged keys (memory,
    /// and a future un-migrated re-assignment of that slot to this node would resurface stale data).
    ///
    /// Unlike [`Store::delete`] this does NOT fire the write observer: the purged keys belong to a
    /// slot this node never OWNED and never replicated out, so a downstream HA-7 replica must NOT be
    /// told to delete them (it correctly never had them). Accounting + the eviction hook ARE
    /// credited (the bytes really leave the store) and a watched key's version IS bumped (a removal
    /// is a modification), keeping every other invariant intact.
    ///
    /// Two phases per database so no table borrow is held across the mutation: COLLECT the matching
    /// keys (a bounded owned `Vec`), then remove each via the crediting removal path.
    pub fn remove_keys_where<P: Fn(&[u8]) -> bool>(&mut self, pred: P) -> usize {
        let mut removed = 0usize;
        for db_idx in 0..self.dbs.len() {
            // Phase 1: collect the matching keys across every slot table of the DB (#570),
            // releasing the table borrow before mutating.
            let victims: Vec<Box<[u8]>> = self.dbs[db_idx]
                .iter()
                .flat_map(|t| t.iter())
                .filter(|e| pred(e.key()))
                .map(|e| e.key().to_vec().into_boxed_slice())
                .collect();
            // Phase 2: remove each, crediting accounting + the eviction hook + the watch version,
            // WITHOUT firing the write observer (this is local cleanup, not a replicated write).
            for key in victims {
                let db = db_idx as u32;
                self.touch_watch(db, &key);
                let h = self.key_hash(&key);
                if let Ok(occ) = self
                    .slot_table_mut(db_idx, &key)
                    .find_entry(h, |e| e.key() == &*key)
                {
                    let (obj, _) = occ.remove();
                    let bytes = obj.accounted_bytes();
                    self.account_sub(bytes);
                    self.eviction.on_remove(db, &key, bytes);
                    removed += 1;
                }
            }
        }
        removed
    }
}

/// One snapshot entry (HA-5b #60): the `(db, key, value)` triple the SNAPSHOT iterator
/// yields, where `value` is an OWNED, borrow-free [`KvObj`] reconstruction a replica
/// replays via [`ShardStore::insert_object`]. A type alias so the chunk return type stays
/// readable (and clippy's `type_complexity` lint is satisfied) at every call site.
pub type SnapshotEntry = (u32, Box<[u8]>, KvObj);

/// A RESUMABLE snapshot cursor (HA-5b #60): the position of a chunked snapshot iteration
/// across the shard's keyspace. It encodes WHICH database is being scanned (`db_index`,
/// iterated 0, 1, 2, ... in turn) and WHERE in that database's `scan_hash` order the next
/// chunk resumes (`scan`, an inner [`ScanCursor`]).
///
/// The caller starts at [`SnapshotCursor::START`], passes the returned cursor to the next
/// [`ShardStore::snapshot_chunk`] call, and stops when [`SnapshotCursor::is_done`] is true.
/// Between chunks the cursor is a plain `Copy` value the caller can hold while it releases
/// the store borrow and awaits (e.g. shipping the chunk to a replica), then resumes -- this
/// is what makes the iteration CONSTANT-MEMORY (a bounded chunk per call, never a
/// full-keyspace materialization) and FORKLESS (no copy-on-write of the table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotCursor {
    /// The database currently being scanned (0-based). When it reaches the database count
    /// the iteration is COMPLETE (no more databases to visit); see [`Self::is_done`]. The
    /// store advances it to the next database once a database's inner scan completes.
    db_index: u32,
    /// The inner per-database SCAN resume position (the same resize-stable `scan_hash`
    /// cursor SCAN uses). [`ScanCursor::START`] means "start this database from the
    /// beginning"; a non-start value is the `scan_hash` threshold to resume at.
    scan: ScanCursor,
}

impl SnapshotCursor {
    /// The start-of-iteration cursor: database 0, inner scan at the start.
    pub const START: SnapshotCursor = SnapshotCursor {
        db_index: 0,
        scan: ScanCursor::START,
    };

    /// Whether the iteration is COMPLETE for a store with `databases` databases: every
    /// database has been fully scanned (`db_index` has advanced past the last one). The
    /// caller loops `while !cursor.is_done(n)`.
    #[must_use]
    pub fn is_done(self, databases: usize) -> bool {
        self.db_index as usize >= databases
    }

    /// The database this cursor is positioned at (introspection / tests).
    #[must_use]
    pub fn db_index(self) -> u32 {
        self.db_index
    }
}

impl<E: EvictionHook, A: AccountingHook> ShardStore<E, A> {
    /// Pull ONE bounded chunk of a SNAPSHOT of this shard (HA-5b #60), resuming at
    /// `cursor`. Returns the chunk (each entry as `(db, key, KvObj)`, an owned borrow-free
    /// reconstruction the replica replays via [`Self::insert_object`]) and the NEXT cursor
    /// to pass back. The iteration is COMPLETE once the returned cursor
    /// [`SnapshotCursor::is_done`] is true.
    ///
    /// ## What the snapshot IS: the bulk base of a snapshot+stream protocol
    ///
    /// The snapshot is a resumable, constant-memory SCAN DUMP that emits `(db, key,
    /// current-value)` for EVERY live key, AT-LEAST-ONCE. It is NOT a strict point-in-time
    /// copy and carries NO per-key version. Correctness comes from LAST-WRITE-WINS via the
    /// HA-5a mutation stream: a replica/importer loads the snapshot, THEN applies the stream
    /// from the offset captured at snapshot-begin. The stream is authoritative + idempotent,
    /// so:
    /// - a key NOT modified after begin -> its snapshot value IS its current value (correct);
    /// - a key modified/created/deleted after begin -> the stream carries that write (offset
    ///   > begin) and the replica applies it AFTER the snapshot (correct, last-write-wins);
    /// - a key the SCAN emits redundantly (also in the stream) -> idempotent.
    ///
    /// So emitting ALL current keys, UNFILTERED, is correct; the point-in-time-ness is
    /// provided by the stream-from-begin-offset (HA-7), NOT by this snapshot. There is no
    /// `cut` parameter and no version filter -- adding either would only narrow the dump,
    /// which the stream already reconciles. (The begin-OFFSET coupling is the replication
    /// offset counter, which does not exist yet; it is HA-7's job.)
    ///
    /// ## Constant memory + resumability (the chunked pull)
    ///
    /// The chunk EXAMINES at most `max` keys (`max == 0` is treated as 1 so progress is
    /// always made), so the returned `Vec` and the per-entry clones are BOUNDED by `max`,
    /// NEVER a full-keyspace materialization. The caller takes the store borrow per chunk,
    /// collects the bounded batch, releases the borrow, awaits (ships the batch), then
    /// resumes with the returned cursor.
    ///
    /// ## Resize tolerance + at-least-once
    ///
    /// The per-database walk REUSES the SCAN cursor core ([`scan_plan`]) over the
    /// resize-stable `scan_hash` order, with `band_bits == 0` (this is a shard-local
    /// iteration, not the cross-shard composite SCAN, so it wants the exact next-key cursor,
    /// not a band floor). Because `scan_hash` is invariant across a `hashbrown` resize,
    /// iteration is TOTAL across concurrent table growth between chunks: a key present
    /// throughout the snapshot is visited at least once. SCAN's AT-LEAST-ONCE guarantee is
    /// fine here -- the replica applies each `(db, key, value)` idempotently, so a key
    /// emitted twice is harmless.
    ///
    /// ## Per-database progression
    ///
    /// Database 0 is scanned to completion, then database 1, etc. (the cursor encodes
    /// `(db_index, scan)`). A chunk may SPAN a database boundary: when a database's inner
    /// scan completes with budget left, the walk advances to the next database and keeps
    /// filling the same chunk, so empty databases never waste a round-trip and an empty
    /// shard terminates in a single call.
    ///
    /// `now` is the caller's clock (ADR-0003: the store reads no clock): a lazily-expired
    /// entry is SKIPPED (the snapshot never ships a logically-dead key, no
    /// tombstones-as-live), matching SCAN.
    #[must_use]
    pub fn snapshot_chunk(
        &self,
        cursor: SnapshotCursor,
        max: usize,
        now: UnixMillis,
    ) -> (Vec<SnapshotEntry>, SnapshotCursor) {
        let databases = self.dbs.len();
        let budget = max.max(1);
        let mut out: Vec<SnapshotEntry> = Vec::new();
        let mut db_index = cursor.db_index;
        let mut scan = cursor.scan;

        // Walk databases in order, filling the chunk up to `budget` EXAMINED keys. A chunk
        // may span a database boundary so empty dbs do not waste round-trips.
        while (db_index as usize) < databases {
            let db_idx = db_index as usize;
            // Remaining examine-budget for this chunk (bounds the total keys touched, so the
            // chunk and its clones stay constant-memory regardless of keyspace size).
            let remaining = budget - out.len().min(budget);
            if remaining == 0 {
                break;
            }
            let (next_scan, examined) = self.snapshot_scan_db(db_idx, scan, remaining);
            // Emit each examined key's CURRENT value, reconstructed as an owned KvObj
            // transfer value. No version filter (correctness comes from the stream); only a
            // lazy-expiry skip so a logically-dead key is never shipped as live.
            for key in examined {
                let h = self.hasher.hash_one(&*key);
                if let Some(obj) = self
                    .slot_table(db_idx, &key)
                    .and_then(|t| t.find(h, |e| e.key() == &*key))
                {
                    if obj.is_expired(now) {
                        continue;
                    }
                    let kv = obj.to_kvobj();
                    out.push((db_index, key, kv));
                }
            }
            if next_scan.is_start() {
                // This database is fully scanned: advance to the next, resetting the inner
                // scan to the start. The outer loop continues filling the same chunk.
                db_index += 1;
                scan = ScanCursor::START;
            } else {
                // More of this database remains: stop here with the resume position. (The
                // budget is spent or this database is not yet exhausted.)
                scan = next_scan;
                break;
            }
        }

        (out, SnapshotCursor { db_index, scan })
    }

    /// One bounded SCAN step over database `db_idx`'s table for the snapshot walk: returns
    /// the next inner [`ScanCursor`] and the EXAMINED keys (owned, so the table borrow is
    /// released before the caller re-finds + reconstructs each). It is the snapshot's reuse
    /// of the SCAN cursor core ([`scan_plan`]) with `band_bits == 0` (shard-local exact
    /// cursor). Separated so the borrow of `self.dbs[db_idx]` is scoped to building the
    /// sorted order + the plan, then dropped before the caller re-borrows `self` to
    /// reconstruct values.
    fn snapshot_scan_db(
        &self,
        db_idx: usize,
        cursor: ScanCursor,
        count: usize,
    ) -> (ScanCursor, Vec<Box<[u8]>>) {
        let Some(slots) = self.dbs.get(db_idx) else {
            return (ScanCursor::START, Vec::new());
        };
        // The sorted (scan_hash, key) view over EVERY slot table of the DB (#570), identical
        // in spirit to `scan_step`: `scan_hash` is recomputed from the key bytes, so the
        // order is stable across calls AND across a hashbrown resize between chunks (writes
        // continue during the snapshot), and it is slot-independent (the merged order equals
        // the pre-#570 single table's), so the inner cursor is unchanged.
        let mut order: Vec<(u64, &[u8])> = slots
            .iter()
            .flat_map(|t| t.iter())
            .map(|e| (scan_hash(e.key()), e.key()))
            .collect();
        if order.is_empty() {
            return (ScanCursor::START, Vec::new());
        }
        order.sort_unstable();
        // band_bits == 0: the snapshot is shard-local, so it wants the EXACT next-key
        // cursor (no cross-shard band rounding). The plan EXAMINES up to `count` keys.
        let plan = scan_plan(&order, cursor, count, 0);
        let examined: Vec<Box<[u8]>> = plan
            .examined
            .iter()
            .map(|&k| k.to_vec().into_boxed_slice())
            .collect();
        (plan.next, examined)
    }
}

/// A FROZEN per-slot table handed to the dedicated persist thread for an off-thread,
/// copy-free SAVE (#576). It wraps an [`Arc`] clone of ONE of the store's slot tables,
/// captured at [`ShardStore::begin_save`], plus the logical `db` the slot belongs to (so
/// the persist thread can write db-tagged records). The persist thread iterates
/// [`Self::entries`] and encodes each entry directly -- NO O(N) serving-side copy, the
/// whole point of the fix.
///
/// ## Why the `unsafe impl Send` is SOUND
///
/// `Arc<HashTable<Entry>>` is `!Send` (an [`Entry`] is a raw `NonNull` tagged pointer with
/// interior-mutable freq, so it is `!Send`/`!Sync`, which is CORRECT for the otherwise
/// shard-local, single-core store, ADR-0002/0005). NOTE `HashTable` here is the cfg'd
/// INDEX-BACKEND alias (#285): the proof below depends only on two properties BOTH
/// backends provide -- `Clone` is a DEEP clone (so `Arc::make_mut` re-homes every entry
/// the live side mutates), and the table's read paths never mutate through `&self` --
/// hashbrown by its documented semantics, `DashIndex` by construction (100% safe code, no
/// interior mutability; its oracle suite pins the deep-clone divergence). Sending a
/// `FrozenSlot` to the persist thread is nonetheless sound because, for the WHOLE lifetime
/// of the frozen clone, the slot's table and every entry pointee it holds are DE-FACTO
/// IMMUTABLE from the datapath's side:
///
/// 1. Any datapath WRITE to this slot goes through `Arc::make_mut`
///    ([`ShardStore::slot_table_mut`]): while the frozen clone is outstanding the slot's
///    `strong_count > 1`, so `make_mut` DEEP-CLONES the entries into a FRESH table and
///    re-points the LIVE `Arc` at it, leaving THIS frozen `Arc` sole-owning the ORIGINAL
///    entries. So a write never forms `&mut` to, nor frees, a pointee this `FrozenSlot`
///    reads.
/// 2. The one datapath write that reaches a pointee through a SHARED `&Entry` -- the
///    S3-FIFO freq bump ([`Entry::bump_freq_shared`]) -- is GATED OFF for the save window
///    ([`ShardStore::saving`]), so no freq byte is mutated under the persist thread's read.
///
/// Thus the persist thread has exclusive, read-only access to immutable data: no `&mut` and
/// no free of those pointees occurs until it DROPS the `FrozenSlot` (after all its reads and
/// the file write). The `Arc` refcount itself is atomic, so the cross-thread clone/drop is
/// data-race-free; and dropping a `FrozenSlot` only frees pointees when its `strong_count`
/// reached 1 (the slot was COW'd away, so no datapath reader remains) -- never a
/// still-shared pointee. This mirrors exactly why the store is otherwise `!Send` (mutable
/// aliasing on the owning core), which does NOT apply to a frozen-immutable slot.
pub struct FrozenSlot {
    /// The logical database this slot belongs to (captured at freeze), so the persist thread
    /// writes each record under its correct `db` tag.
    db: u32,
    /// An `Arc` clone of one of the store's slot tables, frozen at [`ShardStore::begin_save`].
    /// Solely read (never `&mut`) by the persist thread until it is dropped.
    table: Arc<HashTable<Entry>>,
}

// SAFETY: see the `FrozenSlot` type doc. A `FrozenSlot` is created ONLY for a slot that is
// FROZEN for the duration of a save; the datapath never mutates a frozen slot's table or its
// entries' pointees (every write COWs via `Arc::make_mut` first, and the shared freq bump is
// gated off while `saving`), so handing the `Arc` to the persist thread grants exclusive
// de-facto-immutable READ access. No `&mut` and no free of those pointees races the persist
// thread's reads. The `Arc` refcount is atomic, so the cross-thread move + drop is sound.
unsafe impl Send for FrozenSlot {}

impl FrozenSlot {
    /// The logical database this frozen slot belongs to (the record `db` tag).
    #[must_use]
    pub fn db(&self) -> u32 {
        self.db
    }

    /// Iterate the frozen slot's entries by SHARED reference (the persist thread's read-only
    /// dump source). The `impl Iterator` return keeps the underlying `hashbrown` iterator an
    /// implementation detail. The caller (the persist crate) applies the lazy-expiry skip via
    /// [`Entry::is_expired`] and encodes each live entry with [`Entry::to_kvobj`].
    pub fn entries(&self) -> impl Iterator<Item = &Entry> + '_ {
        self.table.iter()
    }
}

impl<E: EvictionHook, A: AccountingHook> ShardStore<E, A> {
    /// BEGIN an off-thread SAVE (#576 per-slot Arc-COW): FREEZE the whole shard by taking an
    /// `Arc` clone of every NON-EMPTY slot table (O(non-empty slots) atomic refcount bumps,
    /// NOT an O(N-keys) copy) and return them as [`FrozenSlot`]s for a dedicated persist
    /// thread to encode + fsync OFF the serving core. Also sets the [`Self::saving`] flag so
    /// the datapath (a) COWs a frozen slot on its first write ([`Self::slot_table_mut`] via
    /// `Arc::make_mut`) instead of mutating the frozen entries, and (b) skips the shared
    /// S3-FIFO freq bump for the save window (the soundness gate).
    ///
    /// EMPTY slots are skipped: they hold nothing to dump, and a key INSERTED into an empty
    /// slot during the save is a brand-new key that the snapshot may legitimately omit
    /// (SCAN/snapshot semantics), so leaving it un-frozen (an in-place insert, no COW) is
    /// correct and saves the clone.
    ///
    /// CONSISTENCY: the returned freeze is a per-shard POINT-IN-TIME view AS OF this call --
    /// stronger than the pre-#576 chunked walk. A key written mid-save COWs, so the dump keeps
    /// its PRE-freeze value (or omits it if newly created); the live store keeps the new value.
    /// Cross-shard it stays FUZZY (each shard freezes at its own instant), the accepted cache
    /// warm-start tradeoff (see `ironcache_persist`).
    ///
    /// MEMORY: the freeze itself is O(slots) pointers (no key copy). The transient extra memory a
    /// save costs is bounded by the fraction of slots WRITTEN during it: each written slot is
    /// deep-cloned ONCE (the frozen original is retained by the persist thread until the save
    /// completes), so a save with no concurrent writes costs ~0 extra and a save racing a full
    /// rewrite costs up to ~1x the keyspace transiently -- the same class of COW headroom a fork
    /// snapshot needs, and freed as the persist thread finishes.
    ///
    /// The caller MUST pair this with [`Self::end_save`] once the persist thread has finished
    /// reading and dropped its [`FrozenSlot`]s, so the flag does not stay set (which would
    /// keep the datapath COWing + skipping freq bumps).
    #[must_use]
    pub fn begin_save(&mut self) -> Vec<FrozenSlot> {
        self.saving = true;
        let mut frozen = Vec::new();
        for (db_idx, dbv) in self.dbs.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let db = db_idx as u32;
            for slot in dbv {
                // Skip empty slots: nothing to dump, and a mid-save insert there is a new key
                // the snapshot may omit, so it need not be frozen (an in-place insert is fine).
                if slot.is_empty() {
                    continue;
                }
                // O(1) freeze: an atomic refcount bump. `make_mut` on a later write to this
                // slot sees strong_count > 1 and COWs, protecting these frozen entries.
                frozen.push(FrozenSlot {
                    db,
                    table: Arc::clone(slot),
                });
            }
        }
        frozen
    }

    /// END an off-thread SAVE (#576): clear the [`Self::saving`] flag. MUST be called only
    /// AFTER the persist thread has finished reading its [`FrozenSlot`]s (i.e. once the save's
    /// result has been received), so a datapath freq bump resuming here can never race the
    /// persist thread's reads. Idempotent (a plain `false` store).
    pub fn end_save(&mut self) {
        self.saving = false;
    }

    /// Whether an off-thread save is currently frozen over this shard (#576). Introspection /
    /// test helper; the datapath reads the private [`Self::saving`] field directly.
    #[must_use]
    pub fn is_saving(&self) -> bool {
        self.saving
    }
}

/// A RESUMABLE cursor over a FROZEN slot view (the #588 [`ShardStore::begin_save`] output),
/// the frozen-scan analogue of [`SnapshotCursor`]. It is a plain slot index into the
/// `&[FrozenSlot]` vector: [`frozen_snapshot_chunk`] emits WHOLE slots per chunk and returns
/// the next slot to resume at.
///
/// The frozen scan is TEAR-FREE by construction, which is the whole reason the streamed
/// handoff (#391) drives it instead of the live [`ShardStore::snapshot_chunk`]: each frozen
/// slot table is an `Arc` clone captured at the freeze and IMMUTABLE for the save's lifetime
/// (a concurrent write to that slot COW-copies a fresh table via `Arc::make_mut`, leaving THIS
/// frozen `Arc` untouched, see [`FrozenSlot`]). So iterating it visits EXACTLY the keys present
/// at the freeze, in a stable order, and a concurrent rehash of the LIVE store can never skip a
/// pre-existing key from this walk -- unlike a live chunked scan whose resize-stable cursor can
/// (a torn scan) drop a pre-existing key that rehashes into an already-visited position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrozenCursor {
    /// The next frozen-slot index to emit (0-based into the `&[FrozenSlot]` vector).
    slot: usize,
}

impl FrozenCursor {
    /// The start-of-iteration cursor: the first frozen slot.
    pub const START: FrozenCursor = FrozenCursor { slot: 0 };

    /// Whether the walk over a `total`-slot frozen view is COMPLETE (every slot emitted). The
    /// caller loops `while !cursor.is_done(frozen.len())`.
    #[must_use]
    pub fn is_done(self, total: usize) -> bool {
        self.slot >= total
    }

    /// The frozen-slot index this cursor is positioned at (introspection / tests).
    #[must_use]
    pub fn slot(self) -> usize {
        self.slot
    }
}

/// Pull ONE bounded chunk of a FROZEN slot view (`frozen`, from [`ShardStore::begin_save`]),
/// resuming at `cursor`. Returns the chunk (each live entry as an owned `(db, key, KvObj)`
/// [`SnapshotEntry`], the same borrow-free transfer triple [`ShardStore::snapshot_chunk`]
/// yields, replayed by [`ShardStore::insert_object`]) and the NEXT [`FrozenCursor`]. The walk
/// is COMPLETE once the returned cursor [`FrozenCursor::is_done`] is true.
///
/// This is the frozen-view counterpart of [`ShardStore::snapshot_chunk`] and is DATA-SAFE for
/// the streamed handoff (#391): because each [`FrozenSlot`] is an immutable point-in-time `Arc`
/// clone, this walk NEVER tears under a concurrent write (which COWs a fresh live copy and
/// leaves the frozen `Arc` untouched), so a pre-existing key can never be skipped mid-scan. A
/// live chunked scan CAN drop such a key when a concurrent rehash moves it behind the cursor;
/// that torn-scan window is exactly what driving the handoff off the frozen view closes.
///
/// It ADDS a read-only accessor over the existing [`FrozenSlot`] surface (same shape the
/// persist crate's `dump_frozen_slots` consumes); it does NOT touch the frozen-save primitives
/// ([`ShardStore::begin_save`] / [`ShardStore::end_save`] / the `Arc::make_mut` COW write path),
/// so their soundness argument is unchanged.
///
/// CONSTANT MEMORY: the chunk is built from WHOLE slots up to a `max`-entry budget (`max == 0`
/// is treated as 1 so progress is always made) and stops at a slot boundary, so peak is bounded
/// by `max` plus at most one frozen slot (~keyspace/slots by default). The returned `Vec` is
/// owned, so the caller holds no borrow of `frozen` across an `.await`.
///
/// `now` is the caller's clock (ADR-0003: the store reads no clock): a lazily-expired entry is
/// SKIPPED (never shipped as a live key, matching [`ShardStore::snapshot_chunk`]); the entry's
/// ABSOLUTE deadline is preserved verbatim by [`Entry::to_kvobj`] (never rebased to `now`).
#[must_use]
pub fn frozen_snapshot_chunk(
    frozen: &[FrozenSlot],
    cursor: FrozenCursor,
    max: usize,
    now: UnixMillis,
) -> (Vec<SnapshotEntry>, FrozenCursor) {
    let budget = max.max(1);
    let mut out: Vec<SnapshotEntry> = Vec::new();
    let mut slot = cursor.slot;
    // Walk WHOLE frozen slots, filling the chunk until the budget is reached at a slot boundary.
    // Never splitting a slot keeps the cursor a plain index (no O(within) re-skip of a hashbrown
    // iterator) while bounding peak memory by budget + one slot.
    while slot < frozen.len() {
        let fs = &frozen[slot];
        let db = fs.db();
        for entry in fs.entries() {
            // Skip a logically-dead key (lazy expiry), exactly as the live snapshot does; the
            // absolute deadline of a live key is carried verbatim in `to_kvobj`.
            if entry.is_expired(now) {
                continue;
            }
            out.push((
                db,
                entry.key().to_vec().into_boxed_slice(),
                entry.to_kvobj(),
            ));
        }
        slot += 1;
        if out.len() >= budget {
            break;
        }
    }
    (out, FrozenCursor { slot })
}

/// Resolve an [`ExpireWrite`] against the entry's current deadline into the new
/// absolute deadline. `Keep`/`Unchanged` preserve the old deadline; `Set` sets it;
/// `Clear` removes it.
fn resolve_expire(expire: ExpireWrite, old: Option<UnixMillis>) -> Option<UnixMillis> {
    match expire {
        ExpireWrite::Unchanged | ExpireWrite::Keep => old,
        ExpireWrite::Set(at) => Some(at),
        ExpireWrite::Clear => None,
    }
}

/// Decimal bytes of an i64 (re-export of the kvobj helper for the command layer if
/// it ever needs to format an int reply without a read).
#[must_use]
pub fn format_int(n: i64) -> Bytes {
    kvobj::int_decimal_bytes(n)
}

// ---------------------------------------------------------------------------
// Process-global allocator accounting (ADR-0006, OBSERVABILITY.md). This is the
// HONEST process-wide figure INFO's `used_memory` reports, SEPARATE from the
// per-shard logical-byte counter [`Store::used_memory`] (which stays the fast
// per-shard number PR-3's eviction budget checks; it is NOT replaced by these).
//
// jemalloc caches its statistics and only refreshes them when the `epoch` is
// advanced, so each read advances the epoch first, then reads `stats.allocated`
// (the live allocated total, the analog of Redis `used_memory`) or
// `stats.resident` (RSS). The tikv-jemalloc-ctl `stats` API is SAFE, so this crate
// keeps `#![forbid(unsafe_code)]`.
//
// PR-3 FOLLOW-UP: per-shard-arena attribution (ADR-0006 "Per-shard arenas keep
// accounting and fragmentation shard-local") so eviction can budget per shard
// precisely. PR-2b reports the honest PROCESS-GLOBAL total for INFO; the read is
// done ONCE on the shard serving INFO (the caller must not sum it across shards,
// which would N-times over-count a process-global figure).
// ---------------------------------------------------------------------------

/// The process-wide jemalloc `stats.allocated` total in bytes (the live allocated
/// total, the analog of Redis `used_memory`), advancing the epoch first so the
/// figure is fresh. Returns 0 if the stat cannot be read.
///
/// This is the PROCESS-GLOBAL figure for INFO `used_memory`; it is NOT the
/// per-shard logical-byte counter ([`Store::used_memory`]). Read it ONCE per INFO
/// (on the serving shard); do NOT sum it across shards.
#[cfg(not(target_env = "msvc"))]
#[must_use]
pub fn process_allocated_bytes() -> u64 {
    // Advance the epoch so the cached stats refresh, then read allocated. Any
    // mallctl error (e.g. jemalloc not the active allocator) degrades to 0 rather
    // than panicking the INFO path.
    let _ = tikv_jemalloc_ctl::epoch::advance();
    tikv_jemalloc_ctl::stats::allocated::read()
        .map(|b| b as u64)
        .unwrap_or(0)
}

/// The process-wide jemalloc `stats.resident` total in bytes (RSS), advancing the
/// epoch first. Returns 0 if the stat cannot be read. Process-global; read once for
/// INFO `used_memory_rss`.
#[cfg(not(target_env = "msvc"))]
#[must_use]
pub fn process_resident_bytes() -> u64 {
    let _ = tikv_jemalloc_ctl::epoch::advance();
    tikv_jemalloc_ctl::stats::resident::read()
        .map(|b| b as u64)
        .unwrap_or(0)
}

/// The process-wide jemalloc `(allocated, resident)` pair in bytes, read from a
/// SINGLE epoch snapshot: the epoch is advanced ONCE and both `stats.allocated`
/// (the `used_memory` analog) and `stats.resident` (RSS) are then read from that
/// same refreshed snapshot. INFO uses this so its two memory figures are mutually
/// consistent (no skew from two independent epoch advances). Either stat degrades
/// to 0 if it cannot be read. Process-global; call ONCE per INFO on the serving
/// shard, NOT summed across shards.
#[cfg(not(target_env = "msvc"))]
#[must_use]
pub fn process_memory() -> (u64, u64) {
    // One epoch advance refreshes the cached stats; both reads then come from the
    // same snapshot.
    let _ = tikv_jemalloc_ctl::epoch::advance();
    let allocated = tikv_jemalloc_ctl::stats::allocated::read()
        .map(|b| b as u64)
        .unwrap_or(0);
    let resident = tikv_jemalloc_ctl::stats::resident::read()
        .map(|b| b as u64)
        .unwrap_or(0);
    (allocated, resident)
}

/// MSVC fallback: the system allocator is selected there (no jemalloc to query),
/// so the process-global allocator figure is unavailable and reported as 0. INFO
/// still emits the field with a parse-clean value.
#[cfg(target_env = "msvc")]
#[must_use]
pub fn process_allocated_bytes() -> u64 {
    0
}

/// MSVC fallback for RSS (see [`process_allocated_bytes`]).
#[cfg(target_env = "msvc")]
#[must_use]
pub fn process_resident_bytes() -> u64 {
    0
}

/// MSVC fallback for the single-snapshot pair (see [`process_allocated_bytes`]):
/// no jemalloc to query, so both figures are 0.
#[cfg(target_env = "msvc")]
#[must_use]
pub fn process_memory() -> (u64, u64) {
    (0, 0)
}

#[cfg(test)]
mod scan_core_tests {
    //! White-box unit tests for the SCAN cursor primitives ([`scan_hash`],
    //! [`scan_plan`]) that need HAND-CRAFTED hashes (a forced equal-hash collision),
    //! which the black-box `tests/keyspace.rs` integration tests cannot construct
    //! without inverting `scan_hash`.

    use super::{ScanCursor, scan_hash, scan_plan};

    /// Build a sorted `(hash, key)` order from explicit `(hash, key)` pairs (the input
    /// shape `scan_plan` consumes). Sorts by (hash, bytes) like the store does.
    fn order(pairs: &[(u64, &'static [u8])]) -> Vec<(u64, &'static [u8])> {
        let mut v = pairs.to_vec();
        v.sort_unstable();
        v
    }

    /// Drive `scan_plan` to completion and collect every examined key, asserting the
    /// cursor terminates at 0. Returns the examined keys in emission order.
    fn drive(order: &[(u64, &'static [u8])], count: usize) -> Vec<&'static [u8]> {
        drive_bands(order, count, 0)
    }

    /// Drive `scan_plan` to completion with an explicit `band_bits` reserved-band width
    /// (the cross-shard composite-cursor case). Asserts the cursor terminates at 0; the
    /// loop bound is GENEROUS (each step advances at least one band, but a band may take
    /// several COUNT-bounded calls when keys share a band, so allow extra iterations).
    fn drive_bands(
        order: &[(u64, &'static [u8])],
        count: usize,
        band_bits: u32,
    ) -> Vec<&'static [u8]> {
        let mut out = Vec::new();
        let mut cursor = ScanCursor::START;
        // A generous loop bound so a cursor bug fails the test rather than hangs.
        for _ in 0..(order.len() * 2 + 4) {
            let plan = scan_plan(order, cursor, count, band_bits);
            out.extend(plan.examined.iter().copied());
            if plan.next.is_start() {
                return out;
            }
            // Band-alignment invariant: a non-terminal next cursor has its low band_bits
            // bits cleared, so the composite cursor's `>> band_bits` encode is lossless.
            if band_bits > 0 {
                let low_mask = (1u64 << band_bits) - 1;
                assert_eq!(
                    plan.next.0 & low_mask,
                    0,
                    "next cursor must be band-aligned (low band_bits clear)"
                );
            }
            // Strict forward progress: the cursor must advance.
            assert!(plan.next.0 > cursor.0, "cursor must strictly advance");
            cursor = plan.next;
        }
        panic!("scan_plan did not terminate (cursor never returned 0)");
    }

    #[test]
    fn scan_hash_is_deterministic_and_pure() {
        // Recomputable from the key bytes alone: the same bytes always hash the same,
        // across calls (and, by construction, processes). Different bytes differ.
        assert_eq!(scan_hash(b"alpha"), scan_hash(b"alpha"));
        assert_ne!(scan_hash(b"alpha"), scan_hash(b"beta"));
        assert_ne!(scan_hash(b""), scan_hash(b"\0"));
    }

    #[test]
    fn empty_order_completes_immediately() {
        let plan = scan_plan(&[], ScanCursor::START, 10, 0);
        assert!(plan.examined.is_empty());
        assert!(plan.next.is_start(), "empty -> cursor 0 (complete)");
    }

    #[test]
    fn full_iteration_visits_every_key_once_small_count() {
        // Distinct hashes; COUNT 1 still completes and visits each key exactly once.
        let o = order(&[(30, b"c"), (10, b"a"), (20, b"b"), (40, b"d")]);
        let visited = drive(&o, 1);
        assert_eq!(visited, vec![&b"a"[..], b"b", b"c", b"d"]);
    }

    #[test]
    fn forced_equal_hash_collision_returns_both_keys() {
        // THE forced-collision test: two DISTINCT keys sharing the same 64-bit hash
        // (impossible to find by inverting scan_hash, so constructed here). The
        // equal-hash group must NEVER be split and BOTH keys must be returned, even
        // with COUNT 1 (the group is emitted whole once reached).
        let o = order(&[(7, b"k1"), (7, b"k2"), (9, b"z")]);
        let visited = drive(&o, 1);
        assert!(visited.contains(&&b"k1"[..]), "k1 (collision) returned");
        assert!(visited.contains(&&b"k2"[..]), "k2 (collision) returned");
        assert!(visited.contains(&&b"z"[..]));
        assert_eq!(visited.len(), 3, "every key returned exactly once");
    }

    #[test]
    fn equal_hash_group_is_never_split_across_calls() {
        // A group of 3 keys at hash 5, then one at hash 8. With COUNT 1 the first call
        // must emit the WHOLE hash-5 group (never split), then a second call the hash-8
        // key. The returned cursor after the first call is the hash-8 group start (8),
        // never a value inside the hash-5 group.
        let o = order(&[(5, b"a"), (5, b"b"), (5, b"c"), (8, b"d")]);
        let plan1 = scan_plan(&o, ScanCursor::START, 1, 0);
        assert_eq!(
            plan1.examined.len(),
            3,
            "whole equal-hash group in one batch"
        );
        assert_eq!(
            plan1.next,
            ScanCursor(8),
            "cursor resumes at the next group"
        );
        let plan2 = scan_plan(&o, plan1.next, 1, 0);
        assert_eq!(plan2.examined, vec![&b"d"[..]]);
        assert!(plan2.next.is_start(), "complete after the last group");
    }

    #[test]
    fn non_terminal_cursor_is_never_zero_even_with_a_zero_hash_key() {
        // A key whose scan_hash is 0 sorts FIRST and is examined on the start batch, so
        // the next cursor is strictly greater than 0. A returned 0 thus unambiguously
        // means complete, never "resume from the 0-hash key".
        let o = order(&[(0, b"zero"), (1, b"one"), (2, b"two")]);
        let plan = scan_plan(&o, ScanCursor::START, 1, 0);
        assert!(
            plan.examined.contains(&&b"zero"[..]),
            "0-hash key examined first"
        );
        assert!(
            !plan.next.is_start(),
            "next cursor is non-zero (more remain)"
        );
        assert_ne!(plan.next, ScanCursor(0));
        // Driving to completion still visits all three exactly once.
        let visited = drive(&o, 1);
        assert_eq!(visited.len(), 3);
    }

    #[test]
    fn count_is_a_hint_examined_count_bounds_the_batch() {
        // With distinct hashes and COUNT 2, the first batch examines exactly 2 keys.
        let o = order(&[(1, b"a"), (2, b"b"), (3, b"c"), (4, b"d"), (5, b"e")]);
        let plan = scan_plan(&o, ScanCursor::START, 2, 0);
        assert_eq!(plan.examined.len(), 2);
        assert_eq!(plan.next, ScanCursor(3), "resume at the 3rd key's hash");
    }

    #[test]
    fn band_aligned_next_cursor_clears_low_bits() {
        // FIX 1: with band_bits=8 the next cursor is the BAND FLOOR of the first
        // un-examined key (low 8 bits cleared), so the composite cursor's `>> 8` encode
        // is LOSSLESS. Two keys share the band [0x100, 0x1FF]; the next un-examined key
        // 0x205 floors to 0x200.
        let o = order(&[(0x105, b"a"), (0x1A0, b"b"), (0x205, b"c")]);
        let plan = scan_plan(&o, ScanCursor::START, 1, 8);
        // The whole [0x100, 0x1FF] band must be emitted in the first batch (a band is
        // never split), and the next cursor is the floor of 0x205 -> 0x200.
        assert_eq!(plan.examined.len(), 2, "whole band emitted, never split");
        assert_eq!(
            plan.next,
            ScanCursor(0x200),
            "next cursor is the band floor"
        );
        assert_eq!(plan.next.0 & 0xFF, 0, "low 8 bits cleared (band-aligned)");
    }

    #[test]
    fn dense_band_terminates_and_visits_every_key_with_band_bits() {
        // FIX 1 (the regression guard at the cursor-core level): a DENSE 256-band -- many
        // keys whose hashes share the top 56 bits (all in band 0x300 = [0x300, 0x3FF]) --
        // plus keys outside it. With band_bits=8 and COUNT 1 the loop MUST terminate (the
        // band-aligned cursor strictly advances by >= one band, never re-floors into the
        // same band forever) and visit every key. Before the band-alignment fix, the
        // non-aligned next cursor inside a dense band would not advance the composite
        // cursor and the SCAN loop would never terminate.
        let dense: Vec<(u64, &'static [u8])> = vec![
            (0x300, b"d0"),
            (0x305, b"d1"),
            (0x310, b"d2"),
            (0x3AB, b"d3"),
            (0x3FF, b"d4"),
            (0x100, b"before"),
            (0x900, b"after0"),
            (0x9FF, b"after1"),
        ];
        let o = order(&dense);
        // COUNT 1 (the worst case) and COUNT 3, both must terminate + cover.
        for count in [1usize, 3] {
            let visited = drive_bands(&o, count, 8);
            let mut got: Vec<&[u8]> = visited.clone();
            got.sort_unstable();
            got.dedup();
            let mut expect: Vec<&[u8]> = dense.iter().map(|&(_, k)| k).collect();
            expect.sort_unstable();
            assert_eq!(got, expect, "every key visited (count={count})");
        }
    }
}

#[cfg(test)]
mod policy_swap_tests {
    //! The additive [`PolicySwap`](ironcache_storage::PolicySwap) hot-swap on the
    //! concrete [`ShardStore`] (CONFIG.md `maxmemory-policy` hot-swap, PR-4b). Proves
    //! the swap installs a fresh policy, leaves the keyspace intact, resets the
    //! eviction history, and is deterministic from a fixed seed.

    use super::*;
    use ironcache_eviction::EvictionPolicy;
    use ironcache_storage::{Admit, CountingAccounting, NewValue, PolicySwap, Store, VictimFreq};

    type TestStore = ShardStore<Policy, CountingAccounting>;

    /// A no-op [`VictimFreq`] for the policy-direct tests: the Random policy ignores
    /// freq entirely (it has no recency/frequency notion), so `select_victim` is fed a
    /// `VictimFreq` that reports no key present. (The S3-FIFO freq path is exercised by
    /// the integration tests in `tests/eviction.rs`, which drive a real store.)
    struct NoFreq;
    impl VictimFreq for NoFreq {
        fn get(&self, _db: u32, _key: &[u8]) -> Option<u8> {
            None
        }
        fn dec(&mut self, _db: u32, _key: &[u8]) {}
    }

    fn store_with(name: &str) -> TestStore {
        let policy = map_policy_name(name, 1).expect("known policy name");
        ShardStore::with_hooks(4, policy, CountingAccounting::new())
    }

    #[test]
    fn refill_pool_selects_the_globally_coldest_cap_in_deterministic_order() {
        // N >> CAP exercises the bounded-heap selection (the #285 follow-up): inserting 200
        // identical-freq keys and refilling must harvest EXACTLY the coldest EVICT_POOL_CAP under
        // the deterministic (freq, scan_hash, key, db) order, byte-identical to a reference full
        // sort. "allkeys-lru" is the S3-FIFO engine, which uses the TableScanLowestFreq pooled
        // path (refill_evict_pool).
        let mut store = store_with("allkeys-lru");
        let n = 200usize;
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for i in 0..n {
            let key = format!("key{i:04}").into_bytes();
            store.upsert(
                0,
                &key,
                NewValue::Bytes(b"v"),
                ExpireWrite::Clear,
                UnixMillis(0),
            );
            keys.push(key);
        }
        assert_eq!(store.len(), n);

        store.refill_evict_pool(UnixMillis(0));
        assert_eq!(
            store.evict_pool.len(),
            EVICT_POOL_CAP,
            "the pool holds exactly the coldest CAP when N > CAP"
        );

        // Reference: all keys share one freq (identical inserts, no reads), so the deterministic
        // order reduces to (scan_hash, key). Sort ascending = coldest-first; the pool stores the
        // coldest CAP COLDEST-LAST (the consumer pops the coldest), so it equals the coldest CAP
        // reversed.
        let mut by_order: Vec<&Vec<u8>> = keys.iter().collect();
        by_order.sort_by(|a, b| scan_hash(a).cmp(&scan_hash(b)).then_with(|| a.cmp(b)));
        let mut expected: Vec<Vec<u8>> =
            by_order.into_iter().take(EVICT_POOL_CAP).cloned().collect();
        expected.reverse();
        let pool_keys: Vec<Vec<u8>> = store.evict_pool.iter().map(|c| c.key.to_vec()).collect();
        assert_eq!(
            pool_keys, expected,
            "bounded-heap selection must match the full-sort coldest CAP, coldest-last"
        );
    }

    #[test]
    fn swap_changes_policy_name_and_keeps_keyspace() {
        let mut store = store_with("allkeys-lru");
        // Plant some live data.
        store.upsert(
            0,
            b"k1",
            NewValue::Bytes(b"v1"),
            ExpireWrite::Clear,
            UnixMillis(0),
        );
        store.upsert(
            0,
            b"k2",
            NewValue::Bytes(b"v2"),
            ExpireWrite::Clear,
            UnixMillis(0),
        );
        assert_eq!(store.len(), 2);
        assert_eq!(store.eviction.policy_name(), "allkeys-lru");
        // OBJECT-FREQ-style accessor returns None under a non-LFU policy.
        assert!(store.access_freq(0, b"k1").is_none());

        // Swap to allkeys-lfu (the real W-TinyLFU engine).
        assert!(store.set_policy_by_name("allkeys-lfu", 7, UnixMillis(0)));
        assert_eq!(store.eviction.policy_name(), "allkeys-lfu");
        // The keyspace is INTACT across the swap (only eviction metadata reset).
        assert_eq!(store.len(), 2);
        // A read under the new LFU policy now tracks frequency (Some), where it was None.
        let _ = store.read(0, b"k1", UnixMillis(0));
        assert!(
            store.access_freq(0, b"k1").is_some(),
            "LFU policy now tracks access frequency after the swap"
        );
    }

    #[test]
    fn swap_rejects_unknown_name_and_keeps_policy() {
        let mut store = store_with("allkeys-lru");
        assert!(!store.set_policy_by_name("allkeys-ttl", 1, UnixMillis(0)));
        // The existing policy is unchanged on a rejected swap.
        assert_eq!(store.eviction.policy_name(), "allkeys-lru");
    }

    #[test]
    fn swap_reseeds_policy_so_eviction_works_immediately_no_spurious_oom() {
        // IC-1: a populated, over-budget shard must still EVICT right after a policy
        // swap. Before the fix the fresh policy had an EMPTY roster, so select_victim
        // returned None and evict_to_fit freed nothing (the caller would then reply a
        // spurious -OOM). After the fix the swap re-seeds the new policy from the live
        // keyspace, so eviction finds a victim on the very next call.
        let mut store = store_with("allkeys-lru");
        // Plant several live keys with NO read/insert touching the NEW policy yet.
        for i in 0..8u8 {
            let key = [b'k', i];
            store.upsert(
                0,
                &key,
                NewValue::Bytes(b"value-bytes"),
                ExpireWrite::Clear,
                UnixMillis(0),
            );
        }
        let before = store.len();
        assert_eq!(before, 8);
        let used = store.used_memory();
        assert!(used > 0);

        // Swap to allkeys-lfu (a DIFFERENT engine: a fresh, empty W-TinyLFU policy).
        assert!(store.set_policy_by_name("allkeys-lfu", 7, UnixMillis(0)));
        assert_eq!(store.eviction.policy_name(), "allkeys-lfu");

        // A denyoom write over a TINY budget must EVICT (no spurious -OOM): evict_to_fit
        // frees down to the budget. With the re-seed it can select victims immediately;
        // without it (the bug) it would free ZERO and the caller would -OOM.
        let tiny_budget = used / 4;
        let evicted = store.evict_to_fit(tiny_budget, UnixMillis(0));
        assert!(
            evicted > 0,
            "post-swap eviction freed nothing (spurious -OOM): the new policy was not \
             re-seeded from the live keyspace"
        );
        assert!(
            store.used_memory() <= tiny_budget,
            "eviction did not bring usage under the budget"
        );
        assert!(store.len() < before, "no keys were actually evicted");

        // OBJECT FREQ works under the new LFU policy for a surviving key (access_freq is
        // Some), proving the swap installed a functioning LFU engine.
        let survivor = (0..8u8)
            .map(|i| [b'k', i])
            .find(|k| store.contains(0, k, UnixMillis(0)))
            .expect("at least one key survives the partial eviction");
        assert!(
            store.access_freq(0, &survivor).is_some(),
            "OBJECT FREQ must work after the swap to an LFU policy"
        );
    }

    #[test]
    fn swap_does_not_reseed_lazily_expired_entries() {
        // A key already past its deadline at the swap `now` must NOT be re-seeded as an
        // eviction candidate (it is lazily-expired; the backstop reaps it on the next
        // observe). With one expired and one live key, the swap re-seeds ONLY the live
        // key into the new policy, so evict_to_fit over a zero budget evicts the live key
        // (a real eviction) and never offers the expired key as a victim.
        let mut store = store_with("allkeys-lru");
        // A live key (no TTL) and a key whose deadline is in the past at now=100.
        store.upsert(
            0,
            b"live",
            NewValue::Bytes(b"v"),
            ExpireWrite::Clear,
            UnixMillis(0),
        );
        store.upsert(
            0,
            b"dead",
            NewValue::Bytes(b"v"),
            ExpireWrite::Set(UnixMillis(10)),
            UnixMillis(0),
        );
        assert_eq!(store.len(), 2);

        // Swap at now=100, after `dead`'s deadline (10) but the keyspace still holds it
        // (not yet reaped). The re-seed must skip `dead`.
        assert!(store.set_policy_by_name("allkeys-lfu", 1, UnixMillis(100)));

        // Evicting to zero budget at now=100: `live` is the only re-seeded candidate and
        // is evicted (a real eviction). `dead` was never re-seeded, so the policy never
        // offers it; it stays resident-but-stale until a read/active-drain reaps it.
        let evicted = store.evict_to_fit(0, UnixMillis(100));
        assert_eq!(evicted, 1, "exactly the live key is evicted post-swap");
        // `live` is gone.
        assert!(!store.contains(0, b"live", UnixMillis(100)));
        // `dead` was not re-seeded as a candidate; observing it now lazily reaps it
        // (the backstop), confirming it was treated as expired, not as an eviction
        // candidate.
        assert!(
            !store.contains(0, b"dead", UnixMillis(100)),
            "the expired key reads as absent (lazy backstop), never an eviction victim"
        );
        assert_eq!(store.len(), 0, "live evicted, dead reaped on observe");
    }

    #[test]
    fn swap_seed_is_deterministic() {
        // Two stores swapped to a *-random policy with the SAME seed select the same
        // victim sequence (ADR-0003: the swap seeds the RNG from the determinism seam).
        let mut a = store_with("allkeys-lru");
        let mut b = store_with("allkeys-lru");
        for s in [&mut a, &mut b] {
            for i in 0..8u8 {
                let key = [b'k', i];
                s.upsert(
                    0,
                    &key,
                    NewValue::Bytes(b"v"),
                    ExpireWrite::Clear,
                    UnixMillis(0),
                );
            }
        }
        // The swap RE-SEEDS the Random roster from the live keyspace (IC-1), so both
        // stores have an identical, populated roster immediately after the swap.
        assert!(a.set_policy_by_name("allkeys-random", 12345, UnixMillis(0)));
        assert!(b.set_policy_by_name("allkeys-random", 12345, UnixMillis(0)));
        // The same seed over the same re-seeded roster yields the same victim choice.
        assert_eq!(
            a.eviction.select_victim(&mut NoFreq),
            b.eviction.select_victim(&mut NoFreq)
        );
    }

    /// M2 (the abort-purge primitive): `remove_keys_where` removes EVERY matching key across all
    /// databases, leaves non-matching keys intact, credits accounting (used_memory drops), and
    /// returns the count removed. (The HA-6 importer passes a slot predicate; here we use a key
    /// prefix to test the primitive directly.)
    #[test]
    fn remove_keys_where_purges_matching_keys_across_dbs_and_credits_accounting() {
        let mut store = store_with("noeviction");
        let now = UnixMillis(0);
        // Two "doomed" keys (prefix d) in different dbs, and two "kept" keys (prefix k).
        store.upsert(0, b"d-one", NewValue::Bytes(b"x"), ExpireWrite::Clear, now);
        store.upsert(1, b"d-two", NewValue::Bytes(b"y"), ExpireWrite::Clear, now);
        store.upsert(0, b"k-one", NewValue::Bytes(b"a"), ExpireWrite::Clear, now);
        store.upsert(2, b"k-two", NewValue::Bytes(b"b"), ExpireWrite::Clear, now);
        assert_eq!(store.len(), 4);
        let used_before = store.used_memory();

        let removed = store.remove_keys_where(|key| key.starts_with(b"d-"));
        assert_eq!(removed, 2, "both doomed keys removed");
        assert_eq!(store.len(), 2, "only the kept keys remain");
        // The doomed keys are gone; the kept keys survive (across the dbs they were in).
        assert!(store.read(0, b"d-one", now).is_none());
        assert!(store.read(1, b"d-two", now).is_none());
        assert_eq!(store.read(0, b"k-one", now).unwrap().as_bytes(), b"a");
        assert_eq!(store.read(2, b"k-two", now).unwrap().as_bytes(), b"b");
        // Accounting was credited (the purged bytes left the per-shard counter).
        assert!(
            store.used_memory() < used_before,
            "purged bytes credited from used_memory"
        );

        // A predicate matching nothing is a no-op returning 0.
        assert_eq!(store.remove_keys_where(|_| false), 0);
        assert_eq!(store.len(), 2);
    }

    /// M2 invariant: the purge is OBSERVER-SILENT (a local cleanup of never-owned import data is
    /// NOT a replicated write). With an observer installed, removing keys via `remove_keys_where`
    /// enqueues NOTHING onto the replication ring, whereas an ordinary `delete` does.
    #[test]
    fn remove_keys_where_does_not_fire_the_write_observer() {
        use core::cell::RefCell as StdRefCell;
        use std::rc::Rc;

        // A tiny capturing observer: counts on_remove calls.
        #[derive(Debug, Default)]
        struct Counter {
            removes: Rc<StdRefCell<usize>>,
        }
        impl WriteObserver for Counter {
            fn on_put(&mut self, _db: u32, _key: &[u8], _new: &Entry) {}
            fn on_remove(&mut self, _db: u32, _key: &[u8]) {
                *self.removes.borrow_mut() += 1;
            }
        }

        let mut store = store_with("noeviction");
        let now = UnixMillis(0);
        let removes = Rc::new(StdRefCell::new(0usize));
        store.set_write_observer(Box::new(Counter {
            removes: Rc::clone(&removes),
        }));

        store.upsert(0, b"d-one", NewValue::Bytes(b"x"), ExpireWrite::Clear, now);
        store.upsert(0, b"k-one", NewValue::Bytes(b"a"), ExpireWrite::Clear, now);

        // The purge fires NO observer removal (silent local cleanup).
        let purged = store.remove_keys_where(|key| key.starts_with(b"d-"));
        assert_eq!(purged, 1);
        assert_eq!(
            *removes.borrow(),
            0,
            "remove_keys_where is observer-silent (no StreamDel shipped)"
        );

        // A normal delete, by contrast, DOES fire the observer (one removal observed).
        assert!(store.delete(0, b"k-one", now));
        assert_eq!(
            *removes.borrow(),
            1,
            "an ordinary delete still fires the observer"
        );
    }
}

#[cfg(test)]
mod slot_tests {
    //! The per-slot table partition (#570): multi-slot point-op round-trips, the
    //! exactly-once full SCAN across pages over a multi-slot DB, the bounded-resize
    //! property (the max slot holds ~N/S, not N), lazy per-DB slot allocation, and the
    //! deterministic fixed-seed slot routing + the tunable slot count.

    use super::*;
    use ironcache_storage::{ExpireWrite, Keyspace, NewValue, ScanCursor, Store};
    use std::collections::HashSet;

    const NOW: UnixMillis = UnixMillis(0);

    /// Drive a full SCAN to completion with a small `count` (so it spans many pages) and
    /// return every key emitted, in order. Asserts the cursor terminates.
    fn drain_scan<E: EvictionHook, A: AccountingHook>(
        store: &mut ShardStore<E, A>,
        db: u32,
        count: usize,
    ) -> Vec<Vec<u8>> {
        let mut seen = Vec::new();
        let mut cursor = ScanCursor::START;
        // A generous page bound so a cursor bug fails rather than hangs.
        for _ in 0..(store.db_len(db) + 100) {
            let (next, batch) = store.scan_step(db, cursor, count, NOW, |_k, _t| true);
            seen.extend(batch.into_iter().map(|k| k.to_vec()));
            if next.is_start() {
                return seen;
            }
            cursor = next;
        }
        panic!("SCAN did not terminate");
    }

    #[test]
    fn multi_slot_get_delete_exists_round_trip() {
        // 2000 keys >> the 256-slot default, so the keyspace spans (essentially) every slot.
        let mut store = ShardStore::new(1);
        let n = 2000usize;
        for i in 0..n {
            let key = format!("key:{i}").into_bytes();
            store.upsert(0, &key, NewValue::Int(i as i64), ExpireWrite::Clear, NOW);
        }
        assert_eq!(store.len(), n);
        // The keys really span multiple slots (the whole point of the partition).
        let occupied = store.dbs[0].iter().filter(|t| !t.is_empty()).count();
        assert!(
            occupied > 1,
            "keys must span multiple slots, got {occupied}"
        );

        // Every key reads back with its value and reports present.
        for i in 0..n {
            let key = format!("key:{i}").into_bytes();
            let v = store.read(0, &key, NOW).expect("present");
            assert_eq!(v.as_bytes(), i.to_string().as_bytes());
            assert!(store.contains(0, &key, NOW));
        }
        // Delete the even keys; the odd keys remain.
        for i in (0..n).step_by(2) {
            let key = format!("key:{i}").into_bytes();
            assert!(store.delete(0, &key, NOW), "delete {i}");
        }
        for i in 0..n {
            let key = format!("key:{i}").into_bytes();
            assert_eq!(store.contains(0, &key, NOW), i % 2 == 1, "key {i}");
        }
        assert_eq!(store.len(), n / 2);
        // Re-deleting an already-gone key (and a never-present key) is a clean false.
        assert!(!store.delete(0, b"key:0", NOW));
        assert!(!store.delete(0, b"never", NOW));
    }

    #[test]
    fn full_scan_returns_every_multi_slot_key_exactly_once() {
        // The exactly-once-across-pages guarantee over a multi-slot DB: SCAN merges every
        // slot in the global scan_hash order, so a small COUNT (many pages) still returns
        // every key exactly once and never misses one present for the whole scan.
        let mut store = ShardStore::new(1);
        let n = 1500usize;
        let mut expected: HashSet<Vec<u8>> = HashSet::new();
        for i in 0..n {
            let key = format!("k{i}").into_bytes();
            store.upsert(0, &key, NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
            expected.insert(key);
        }
        let seen = drain_scan(&mut store, 0, 7);
        let seen_set: HashSet<Vec<u8>> = seen.iter().cloned().collect();
        assert_eq!(
            seen.len(),
            seen_set.len(),
            "no key returned twice across pages"
        );
        assert_eq!(seen_set, expected, "every key returned exactly once");
    }

    #[test]
    fn per_slot_partition_bounds_the_resize_unit() {
        // The bounded-resize property (#570): the LARGEST slot table holds only ~N/S
        // entries, so the worst-case single-insert all-at-once rehash touches ~one slot's
        // entries, NOT the DB's whole N. We measure and report the MAX slot occupancy (the
        // p100 resize unit). 100k keys extrapolates linearly: at 1M keys / 256 slots the max
        // slot is ~4000 entries (~80us resize) vs ~1M (~6ms) for a single table.
        let mut store = ShardStore::new(1);
        let n = 100_000usize;
        for i in 0..n {
            let key = format!("key:{i}").into_bytes();
            store.upsert(0, &key, NewValue::Int(i as i64), ExpireWrite::Clear, NOW);
        }
        assert_eq!(store.len(), n);
        let slots = store.slots;
        let lens: Vec<usize> = store.dbs[0].iter().map(|t| t.len()).collect();
        let max = lens.iter().copied().max().expect("slots present");
        let mean = n / slots;
        // A well-avalanched fixed-seed slot hash keeps the max near the mean N/S (measured
        // MAX ~439 at n=100k, ~1.13x the 390 mean). A generous 2x-mean bound absorbs hash
        // imbalance while still proving the resize unit is ~N/S, not N.
        assert!(
            max <= mean * 2,
            "MAX slot {max} exceeds 2x mean {mean} (slots={slots}, n={n})"
        );
        // The resize unit (max slot) is DRASTICALLY smaller than N (the pre-#570 unit).
        assert!(
            (max as f64) < (n as f64) / 50.0,
            "resize unit (max slot {max}) must be << N ({n})"
        );
    }

    #[test]
    fn untouched_db_allocates_no_slot_tables() {
        // Lazy per-DB: only a written DB materializes its slot tables; an untouched DB
        // carries an empty slot Vec, and reads/scan/dbsize on it allocate nothing.
        let mut store = ShardStore::new(4);
        store.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
        assert_eq!(store.dbs[0].len(), store.slots, "written db has its slots");
        for db in 1..4 {
            assert!(
                store.dbs[db].is_empty(),
                "untouched db {db} carries no slots"
            );
        }
        // Read/exists/dbsize/scan on untouched DBs are correct AND non-allocating.
        assert!(store.read(1, b"absent", NOW).is_none());
        assert!(!store.contains(2, b"absent", NOW));
        assert_eq!(store.db_len(3), 0);
        let (cur, batch) = store.scan_step(1, ScanCursor::START, 10, NOW, |_, _| true);
        assert!(cur.is_start() && batch.is_empty());
        for db in 1..4 {
            assert!(
                store.dbs[db].is_empty(),
                "read/scan must not allocate untouched db {db}"
            );
        }
    }

    #[test]
    fn slot_index_is_deterministic_and_masked() {
        // Fixed-seed routing: a key maps to the same slot every call, always in [0, slots).
        for slots in [1usize, 2, 16, 256, 1024] {
            for k in [&b"alpha"[..], b"beta", b"", b"a-long-key-name-01234567890"] {
                let s = slot_index(k, slots);
                assert_eq!(s, slot_index(k, slots), "deterministic");
                assert!(s < slots, "masked into [0, {slots})");
            }
        }
        // slots == 1 collapses to slot 0 (the single-table pre-#570 layout).
        assert_eq!(slot_index(b"anything", 1), 0);
    }

    #[test]
    fn with_slots_per_db_rounds_up_to_power_of_two() {
        assert_eq!(ShardStore::new(1).with_slots_per_db(100).slots, 128);
        assert_eq!(ShardStore::new(1).with_slots_per_db(0).slots, 1);
        assert_eq!(ShardStore::new(1).with_slots_per_db(256).slots, 256);
        assert_eq!(
            ShardStore::new(1)
                .with_slots_per_db(DEFAULT_SLOTS_PER_DB)
                .slots,
            DEFAULT_SLOTS_PER_DB
        );
    }

    #[test]
    fn custom_slot_count_routes_and_scans() {
        // A config-overridden slot count still round-trips point ops and full SCAN.
        let mut store = ShardStore::new(1).with_slots_per_db(64);
        assert_eq!(store.slots, 64);
        let n = 500usize;
        for i in 0..n {
            let key = format!("z{i}").into_bytes();
            store.upsert(0, &key, NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
        }
        assert_eq!(store.dbs[0].len(), 64, "the DB has exactly 64 slot tables");
        assert_eq!(store.len(), n);
        let seen: HashSet<Vec<u8>> = drain_scan(&mut store, 0, 5).into_iter().collect();
        assert_eq!(
            seen.len(),
            n,
            "full scan returns every key under a custom slot count"
        );
    }

    #[test]
    fn swap_db_exchanges_multi_slot_keyspaces() {
        // SWAPDB trades the two DBs' whole slot Vecs (#570): counts and membership follow.
        let mut store = ShardStore::new(2);
        for i in 0..300 {
            let k = format!("a{i}").into_bytes();
            store.upsert(0, &k, NewValue::Int(i as i64), ExpireWrite::Clear, NOW);
        }
        for i in 0..100 {
            let k = format!("b{i}").into_bytes();
            store.upsert(1, &k, NewValue::Int(i as i64), ExpireWrite::Clear, NOW);
        }
        store.swap_db(0, 1);
        assert_eq!(store.db_len(0), 100);
        assert_eq!(store.db_len(1), 300);
        assert!(store.contains(0, b"b50", NOW));
        assert!(store.contains(1, b"a250", NOW));
    }
}

#[cfg(test)]
mod arc_cow_tests {
    //! The per-slot `Arc` copy-on-write snapshot isolation (#576): a [`ShardStore::begin_save`]
    //! freeze is a per-shard POINT-IN-TIME view the (simulated) persist thread reads, and every
    //! LIVE write during the save COW-copies its slot with a DEEP clone so the frozen view is
    //! never mutated / freed under the reader. These are single-threaded, deterministic, and
    //! Miri-friendly (the crux test simulates the persist thread's read handle by simply HOLDING
    //! the `FrozenSlot`s across the live mutations + drop).

    use super::*;
    use ironcache_storage::{ExpireWrite, NewValue, Store};

    const NOW: UnixMillis = UnixMillis(0);

    /// The LIVE S3-FIFO freq of `(db, key)`, read straight off the stored entry via the
    /// crate-internal accessors (so this works for a `NullEviction` store, which does not get the
    /// `Admit` trait's `access_freq`).
    fn live_freq<E: EvictionHook, A: AccountingHook>(
        store: &ShardStore<E, A>,
        db: u32,
        key: &[u8],
    ) -> Option<u8> {
        let db_idx = store.db_index(db);
        let h = store.key_hash(key);
        store
            .slot_table(db_idx, key)
            .and_then(|t| t.find(h, |e| e.key() == key))
            .map(Entry::freq)
    }

    /// Read the STRING value a `FrozenSlot` set holds for `(db, key)`, reconstructed from the
    /// frozen entry (the same `to_kvobj` the persist thread encodes through). `None` if absent.
    fn frozen_value(frozen: &[FrozenSlot], db: u32, key: &[u8]) -> Option<Vec<u8>> {
        for slot in frozen.iter().filter(|s| s.db() == db) {
            for e in slot.entries() {
                if e.key() == key {
                    return Some(match e.to_kvobj().value {
                        crate::kvobj::ValueRepr::Int(n) => n.to_string().into_bytes(),
                        crate::kvobj::ValueRepr::Inline(b) | crate::kvobj::ValueRepr::Raw(b) => {
                            b.into_vec()
                        }
                        _ => Vec::new(),
                    });
                }
            }
        }
        None
    }

    /// THE COW CORRECTNESS CRUX (+ the Miri target): while a freeze is held (as the persist
    /// thread would), OVERWRITE + DELETE the frozen keys in the LIVE store. A shallow clone that
    /// shared the entry pointees would corrupt / free what the frozen reader still reads (a
    /// use-after-free / double-free Miri would catch). The deep-clone-on-COW must leave the
    /// frozen view byte-for-byte at its pre-freeze state while the live store advances.
    #[test]
    fn cow_deep_clone_isolates_frozen_slots_from_overwrite_and_delete() {
        let mut store = ShardStore::new(1);
        // Enough keys to span many slots, so the freeze covers many slot `Arc`s and the live
        // writes COW many distinct slots.
        let n = 500usize;
        for i in 0..n {
            let k = format!("k{i}").into_bytes();
            let v = format!("orig-{i}");
            store.upsert(
                0,
                &k,
                NewValue::Bytes(v.as_bytes()),
                ExpireWrite::Clear,
                NOW,
            );
        }

        // FREEZE: hold the frozen slots exactly as the persist thread would (a read handle over
        // the pre-freeze entries). `saving` is now set.
        let frozen = store.begin_save();
        assert!(store.is_saving(), "begin_save sets the saving flag");
        let frozen_keys: usize = frozen.iter().map(|s| s.entries().count()).sum();
        assert_eq!(
            frozen_keys, n,
            "the freeze captures the whole pre-freeze keyspace"
        );

        // MUTATE the LIVE store while the frozen slots are held: OVERWRITE even keys (frees the
        // old pointee + allocates a new one) and DELETE odd keys (frees the pointee). With a
        // shallow clone BOTH would corrupt/free the frozen reader's pointee.
        for i in 0..n {
            let k = format!("k{i}").into_bytes();
            if i % 2 == 0 {
                let v = format!("NEW-{i}");
                store.upsert(
                    0,
                    &k,
                    NewValue::Bytes(v.as_bytes()),
                    ExpireWrite::Clear,
                    NOW,
                );
            } else {
                assert!(store.delete(0, &k, NOW), "live delete of k{i}");
            }
        }

        // The FROZEN slots STILL show the PRE-freeze value for EVERY key (overwritten AND
        // deleted): deep-clone-on-COW isolated them. Reading here would UAF under a shallow clone.
        for i in 0..n {
            let k = format!("k{i}");
            assert_eq!(
                frozen_value(&frozen, 0, k.as_bytes()).as_deref(),
                Some(format!("orig-{i}").as_bytes()),
                "frozen slot must retain the pre-freeze value for {k} (COW isolation)"
            );
        }
        // The frozen entry COUNT is unchanged: a live delete did not remove from the frozen view.
        let frozen_keys_after: usize = frozen.iter().map(|s| s.entries().count()).sum();
        assert_eq!(
            frozen_keys_after, n,
            "the frozen point-in-time is unperturbed"
        );

        // The LIVE store reflects the POST-write state.
        for i in 0..n {
            let k = format!("k{i}").into_bytes();
            if i % 2 == 0 {
                assert_eq!(
                    store
                        .read(0, &k, NOW)
                        .expect("overwritten key live")
                        .as_bytes(),
                    format!("NEW-{i}").as_bytes()
                );
            } else {
                assert!(
                    store.read(0, &k, NOW).is_none(),
                    "deleted key gone from live store"
                );
            }
        }

        // Drop the frozen handle (the persist thread finishing), THEN end the save. Dropping the
        // frozen `Arc`s frees ONLY the COW'd-away originals (no live reader remains) -- no UAF /
        // double-free (Miri validates the drop ordering).
        drop(frozen);
        store.end_save();
        assert!(!store.is_saving(), "end_save clears the saving flag");

        // Post-save writes take the uncontended fast path and are correct.
        store.upsert(0, b"k0", NewValue::Bytes(b"post"), ExpireWrite::Clear, NOW);
        assert_eq!(
            store.read(0, b"k0", NOW).expect("k0 live").as_bytes(),
            b"post"
        );
        // A key deleted during the save stays gone post-save (the fast path sees it absent).
        assert!(
            !store.delete(0, b"k1", NOW),
            "k1 was deleted during the save"
        );
    }

    /// The SOUNDNESS GATE: while a save holds a frozen clone, a GET must NOT bump the S3-FIFO
    /// freq (a shared interior-mutable write to a frozen pointee the persist thread reads would
    /// be a data race). The bump resumes after `end_save`.
    #[test]
    fn freq_bump_skipped_while_saving_then_resumes() {
        let mut store = ShardStore::new(1);
        store.upsert(0, b"hot", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
        let f0 = live_freq(&store, 0, b"hot").expect("present");
        store.read(0, b"hot", NOW);
        let f1 = live_freq(&store, 0, b"hot").expect("present");
        assert!(f1 > f0, "a read bumps the S3-FIFO freq when not saving");

        // FREEZE: reads during the save leave the freq UNCHANGED (the gate).
        let frozen = store.begin_save();
        let before = live_freq(&store, 0, b"hot").expect("present");
        for _ in 0..5 {
            store.read(0, b"hot", NOW);
        }
        let during = live_freq(&store, 0, b"hot").expect("present");
        assert_eq!(
            during, before,
            "the freq bump is SKIPPED while a save holds a frozen clone (soundness gate)"
        );
        drop(frozen);
        store.end_save();

        // After the save the bump resumes.
        let resume0 = live_freq(&store, 0, b"hot").expect("present");
        store.read(0, b"hot", NOW);
        let resume1 = live_freq(&store, 0, b"hot").expect("present");
        assert!(resume1 > resume0, "the freq bump resumes after end_save");
    }

    /// At SCALE (200k keys): a freeze is a stable per-shard point-in-time even as a large
    /// fraction of the keyspace is overwritten + deleted on the LIVE store. Every COW is
    /// one-time-per-slot; the frozen view keeps its full pre-freeze contents.
    #[test]
    fn point_in_time_holds_under_mass_writes_at_scale() {
        let mut store = ShardStore::new(1);
        let n = 200_000usize;
        for i in 0..n {
            let k = format!("key:{i}").into_bytes();
            store.upsert(0, &k, NewValue::Int(i as i64), ExpireWrite::Clear, NOW);
        }
        let frozen = store.begin_save();
        let frozen_count: usize = frozen.iter().map(|s| s.entries().count()).sum();
        assert_eq!(
            frozen_count, n,
            "the freeze captures the whole pre-freeze keyspace"
        );

        // Mass mutation on the LIVE store: overwrite a third, delete a third, leave a third.
        for i in 0..n {
            let k = format!("key:{i}").into_bytes();
            match i % 3 {
                0 => {
                    store.upsert(
                        0,
                        &k,
                        NewValue::Int(i as i64 + 1_000_000),
                        ExpireWrite::Clear,
                        NOW,
                    );
                }
                1 => {
                    store.delete(0, &k, NOW);
                }
                _ => {}
            }
        }
        // The frozen view is UNCHANGED -- a true per-shard point-in-time isolated from every
        // live write by COW.
        let frozen_count_after: usize = frozen.iter().map(|s| s.entries().count()).sum();
        assert_eq!(
            frozen_count_after, n,
            "the frozen point-in-time is unperturbed by live writes"
        );
        for i in (0..n).step_by(9973) {
            assert_eq!(
                frozen_value(&frozen, 0, format!("key:{i}").as_bytes()),
                Some(i.to_string().into_bytes()),
                "frozen value for key:{i} is the pre-freeze int"
            );
        }
        drop(frozen);
        store.end_save();

        // The live store reflects the post-write state.
        let deleted = (0..n).filter(|i| i % 3 == 1).count();
        assert_eq!(
            store.len(),
            n - deleted,
            "a third of the keys were deleted from the live store"
        );
    }
}
