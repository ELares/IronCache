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
//! ## Slot partitioning (deferred)
//!
//! HASHTABLE.md describes a per-SLOT table within each shard (the 16384-slot space,
//! ADR-0011). The slot dimension is a cluster-routing concern (#35/#129/#75); PR-2a
//! is single-node and uses one table per DB. The slot split is an internal
//! representation change behind the same `Store` waist (a `HashMap` per (db, slot)
//! instead of per db) and changes no command-layer or waist signature, so it is
//! deferred without freezing anything out.
//!
//! ## Determinism and time (ADR-0003)
//!
//! The store reads no clock: `now: UnixMillis` is passed in by the caller. The
//! lazy expiry-on-read backstop (EXPIRATION.md) lives in every read path here: an
//! entry whose deadline has strictly passed (`now > expire_at`, the Valkey
//! boundary; alive at `now == expire_at`) is removed and reported as absent.

#![forbid(unsafe_code)]

pub mod encoding;
pub mod kvobj;

use bytes::Bytes;
use hashbrown::HashMap;
use hashbrown::HashSet;
use hashbrown::hash_map::Entry;
use ironcache_eviction::EvictionPolicy;
use ironcache_storage::{
    AccountingHook, CountingAccounting, DataType, EvictionHook, ExpireWrite, Keyspace, MoveMode,
    MoveOutcome, NewValue, NullEviction, OccupiedEntry, RmwAction, RmwEntry, RmwStep, ScanCursor,
    Store, UnixMillis, ValueRef,
};
use kvobj::{KvObj, int_decimal_bytes};

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
fn scan_plan<'a>(order: &[(u64, &'a [u8])], cursor: ScanCursor, count: usize) -> ScanPlan<'a> {
    let total = order.len();
    // The resume position: the first key whose hash is >= the cursor. For the start
    // cursor (0) that is index 0. Because a group is never split, `start` always lands
    // on a group boundary, so `>=` returns the whole resumed group.
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

    let count = count.max(1);
    let mut examined: Vec<&'a [u8]> = Vec::new();
    let mut i = start;
    let mut n = 0usize;
    while i < total {
        let (h, key) = order[i];
        // Stop once the per-call budget is spent AND we are at a group boundary (the
        // hash differs from the last examined key), so a group is never split.
        if n >= count && i > start && h != order[i - 1].0 {
            break;
        }
        examined.push(key);
        n += 1;
        i += 1;
    }

    // The next cursor: 0 (complete) if we consumed the whole order; otherwise the hash
    // of the first un-examined key (always a strictly-greater group start, never 0).
    let next = if i >= total {
        ScanCursor::START
    } else {
        ScanCursor(order[i].0)
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
    /// One key->kvobj map per database. `dbs[db]` is the keyspace for `SELECT db`.
    dbs: Vec<HashMap<Box<[u8]>, KvObj>>,
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
        let mut dbs = Vec::with_capacity(n);
        for _ in 0..n {
            dbs.push(HashMap::new());
        }
        ShardStore {
            dbs,
            eviction,
            accounting,
            used: 0,
            lazy_expired: 0,
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
        self.dbs.iter().map(HashMap::len).sum()
    }

    /// Whether the store holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.dbs.iter().all(HashMap::is_empty)
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
        let due = self
            .dbs
            .get(db_idx)
            .and_then(|m| m.get(key))
            .is_some_and(|o| o.is_expired(now));
        if due {
            if let Some(obj) = self.dbs[db_idx].remove(key) {
                let bytes = obj.accounted_bytes();
                self.account_sub(bytes);
                self.eviction.on_remove(db, key, bytes);
                // Count the lazy-backstop reclamation for INFO `expired_keys` (PR-3b).
                // The serve loop drains this with `take_lazy_expired` after each
                // command. This is the lazy-path signal that complements the active
                // timing-wheel drain's count.
                self.lazy_expired = self.lazy_expired.saturating_add(1);
            }
            return false;
        }
        // Present-and-live iff it exists (it did not expire above).
        self.dbs.get(db_idx).is_some_and(|m| m.contains_key(key))
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
    fn put_object(&mut self, db: u32, db_idx: usize, key: &[u8], obj: KvObj) -> bool {
        let new_bytes = obj.accounted_bytes();
        let boxed: Box<[u8]> = key.to_vec().into_boxed_slice();
        // Replace inside the entry scope, capturing any old weight, then update the
        // hooks AFTER the table borrow ends (the hooks borrow `self` mutably).
        let old_bytes = match self.dbs[db_idx].entry(boxed) {
            Entry::Occupied(mut e) => {
                let old = e.get().accounted_bytes();
                *e.get_mut() = obj;
                Some(old)
            }
            Entry::Vacant(e) => {
                e.insert(obj);
                None
            }
        };
        if let Some(old) = old_bytes {
            self.account_sub(old);
            self.eviction.on_remove(db, key, old);
        }
        self.account_add(new_bytes);
        self.eviction.on_insert(db, key, new_bytes);
        old_bytes.is_some()
    }

    /// Remove `key`'s object, crediting the hooks. Returns whether it existed
    /// (caller guarantees any due expiry already ran, so an existing entry is live).
    ///
    /// `db` is the validated logical DB id passed to the hooks; `db_idx` is the
    /// (possibly clamped) Vec index for the map (see [`Self::db_index`]).
    fn remove_object(&mut self, db: u32, db_idx: usize, key: &[u8]) -> bool {
        if let Some(obj) = self.dbs[db_idx].remove(key) {
            let bytes = obj.accounted_bytes();
            self.account_sub(bytes);
            self.eviction.on_remove(db, key, bytes);
            true
        } else {
            false
        }
    }

    /// Build the read-borrow view for an object. An int materializes its decimal
    /// bytes (owned); a string borrows the stored buffer.
    ///
    /// FOLLOW-UP (#8/Efficient): the int branch allocates a fresh `Bytes` per read
    /// via `int_decimal_bytes`. When the FAM object-layout work lands, format the
    /// decimal digits into an inline/borrowable buffer carried by the view (or by
    /// the object) so an int read does no per-read heap allocation. No change now.
    fn view_of(obj: &KvObj) -> ValueRef<'_> {
        match &obj.value {
            kvobj::ValueRepr::Int(n) => {
                ValueRef::from_int_bytes(obj.header.data_type, obj.expire_at, int_decimal_bytes(*n))
            }
            kvobj::ValueRepr::Inline(b) => ValueRef::borrowed(
                obj.header.data_type,
                obj.header.encoding,
                obj.expire_at,
                b.as_bytes(),
            ),
            kvobj::ValueRepr::Raw(b) => {
                ValueRef::borrowed(obj.header.data_type, obj.header.encoding, obj.expire_at, b)
            }
        }
    }

    /// Build the rmw observation handle for an object (same int-materialization as
    /// [`Self::view_of`]). Returns the handle plus the int decimal `Bytes` keeper so
    /// the borrow stays valid for the closure.
    fn occupied_of(obj: &KvObj) -> OccupiedEntry<'_> {
        match &obj.value {
            kvobj::ValueRepr::Int(n) => OccupiedEntry::from_int_bytes(
                obj.header.data_type,
                obj.expire_at,
                int_decimal_bytes(*n),
            ),
            kvobj::ValueRepr::Inline(b) => OccupiedEntry::borrowed(
                obj.header.data_type,
                obj.header.encoding,
                obj.expire_at,
                b.as_bytes(),
            ),
            kvobj::ValueRepr::Raw(b) => {
                OccupiedEntry::borrowed(obj.header.data_type, obj.header.encoding, obj.expire_at, b)
            }
        }
    }
}

impl<E: EvictionHook, A: AccountingHook> Store for ShardStore<E, A> {
    fn read(&mut self, db: u32, key: &[u8], now: UnixMillis) -> Option<ValueRef<'_>> {
        let db_idx = self.db_index(db);
        if !self.expire_if_due(db, db_idx, key, now) {
            return None;
        }
        // The S3-FIFO 2-bit frequency is owned by the POLICY (per-key counter bumped
        // in `on_access`); the kvobj `eviction_rank` header field is RESERVED for the
        // eventual single-source migration (see the eviction crate docs) and is not
        // written on the access path, since nothing reads it today.
        self.eviction.on_access(db, key);
        self.dbs[db_idx].get(key).map(Self::view_of)
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
            self.dbs[db_idx].get(key).and_then(|o| o.expire_at)
        } else {
            None
        };
        let new_deadline = resolve_expire(expire, old_deadline);
        let obj = match value {
            NewValue::Bytes(b) => KvObj::from_bytes(key, b, new_deadline),
            NewValue::Int(n) => KvObj::from_int(key, n, new_deadline),
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
            // The S3-FIFO 2-bit frequency is owned by the POLICY (bumped in
            // `on_access`); the kvobj `eviction_rank` header field is RESERVED, not
            // written here (nothing reads it). See the eviction crate docs.
            self.eviction.on_access(db, key);
            let obj = self.dbs[db_idx].get(key).expect("live entry present");
            let entry = RmwEntry::Occupied(Self::occupied_of(obj));
            f(entry)
        } else {
            f(RmwEntry::Vacant)
        };

        // The current (pre-write) deadline, for ExpireWrite::Keep/Unchanged.
        let old_deadline = if live {
            self.dbs[db_idx].get(key).and_then(|o| o.expire_at)
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
                        if let Some(obj) = self.dbs[db_idx].get_mut(key) {
                            obj.expire_at = new_deadline;
                            obj.header.ttl_present = new_deadline.is_some();
                        }
                    }
                }
            }
            RmwAction::Insert(v) | RmwAction::Replace(v) => {
                let new_deadline = match step.expire {
                    ExpireWrite::Unchanged => old_deadline,
                    other => resolve_expire(other, old_deadline),
                };
                let obj = KvObj::from_new_owned(key, v, new_deadline);
                self.put_object(db, db_idx, key, obj);
            }
            RmwAction::Delete => {
                if live {
                    self.remove_object(db, db_idx, key);
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
        self.dbs[db_idx].get(key).map(|o| o.header.data_type)
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
    /// The loop condition is strict `>` to match Redis's getMaxmemoryState (evict.c):
    /// memory is "under limit" at `used <= maxmemory`, so eviction frees down to
    /// `used <= budget` (NOT strictly below it) and stops the instant the budget is
    /// met exactly.
    ///
    /// The store drives the policy through the [`EvictionHook`] surface: each round
    /// asks [`EvictionHook::select_victim`] for a `(db, key)` and deletes it (which
    /// fires `on_remove` and frees its bytes through the accounting hook), stopping as
    /// soon as the budget is met. If `select_victim` returns `None` the policy cannot
    /// free anything (an empty keyspace, or the `noeviction` policy), so we stop and
    /// return what we evicted so far; the caller then decides whether to reply `-OOM`.
    ///
    /// ## Volatile-only enforcement (the #46 re-eligibility fix, completed in 3b)
    ///
    /// For a `volatile_only` policy (the `volatile-*` family) only TTL-bearing keys
    /// are eligible. The frozen hooks do not pass TTL to the policy, so the FILTER
    /// lives here, where the store can read `expire_at`: a victim with NO TTL is
    /// RE-REGISTERED into the policy (NON-DESTRUCTIVELY, via
    /// [`EvictionPolicy::re_register`]) rather than dropped, and the loop asks for the
    /// next victim. Re-registering (instead of the PR-3a `on_remove` drop) is the #46
    /// fix: a non-TTL key the store declines to evict STAYS an eviction candidate, so
    /// once a later EXPIRE attaches a TTL it becomes eligible. The scan is bounded by
    /// tracking the distinct keys examined-and-skipped this call: once that set covers
    /// the whole live keyspace with no eligible TTL-bearing victim found, the loop
    /// returns what it freed so far (zero, here), leaving the over-budget write to be
    /// rejected `-OOM` (matching Redis volatile-* with no expirable keys).
    ///
    /// `now` is consulted only to skip an ALREADY-expired victim (it will be reaped
    /// lazily anyway). The iteration cap is a defensive secondary bound.
    fn evict_to_fit(&mut self, budget_bytes: u64, now: UnixMillis) -> u64 {
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
            let Some((db, key)) = self.eviction.select_victim() else {
                break;
            };
            let db_idx = self.db_index(db);
            // Inspect the candidate (immutable borrow), extract the state, then drop
            // the borrow before any mutating call (the hooks borrow self mut).
            let (present, is_expired, lacks_ttl) = match self.dbs[db_idx].get(&*key) {
                Some(obj) => (true, obj.is_expired(now), obj.expire_at.is_none()),
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

    /// The access-frequency estimate for OBJECT FREQ, delegated to the configured
    /// policy (only the W-TinyLFU LFU engine returns `Some`; non-LFU policies return
    /// `None`, which dispatch maps to the OBJECT FREQ LFU-gating error). Read-only.
    fn access_freq(&self, db: u32, key: &[u8]) -> Option<u8> {
        self.eviction.access_freq(db, key)
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
        let expired = self
            .dbs
            .get(db_idx)
            .and_then(|m| m.get(key))
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
        let Some(map) = self.dbs.get(db_idx) else {
            return (ScanCursor::START, Vec::new());
        };
        if map.is_empty() {
            // Empty db -> complete immediately (cursor 0).
            return (ScanCursor::START, Vec::new());
        }

        // The sorted (scan_hash, key_bytes) view. `scan_hash` is recomputed from the
        // key bytes here, NOT read from the table's internal hasher, so the order is
        // stable across calls and across a resize (KEYSPACE.md). Sorting by (hash,
        // bytes) gives a total order even for equal-hash keys.
        let mut order: Vec<(u64, &[u8])> = map.keys().map(|k| (scan_hash(k), k.as_ref())).collect();
        order.sort_unstable();

        // Walk the sorted order, choosing which keys to EXAMINE this batch and what the
        // next cursor is (the pure cursor-stepping core, unit-tested in isolation).
        let plan = scan_plan(&order, cursor, count);

        // Realize the plan: for each examined key, skip a lazily-expired one (the lazy
        // backstop / active drain reclaim it; SCAN never returns it) and apply the
        // MATCH/TYPE `keep` filter BEFORE cloning the key into the result.
        let mut kept: Vec<Box<[u8]>> = Vec::with_capacity(plan.examined.len());
        for &key in &plan.examined {
            if let Some(obj) = map.get(key) {
                if obj.is_expired(now) {
                    continue;
                }
                if keep(key, obj.header.data_type) {
                    kept.push(key.to_vec().into_boxed_slice());
                }
            }
        }
        (plan.next, kept)
    }

    fn db_len(&self, db: u32) -> usize {
        let db_idx = self.db_index(db);
        // RAW table length (Redis does not active-expire on DBSIZE): the dict size,
        // including not-yet-reaped expired keys. No lazy backstop here.
        self.dbs.get(db_idx).map_or(0, HashMap::len)
    }

    fn random_key(&mut self, db: u32, pick: u64, now: UnixMillis) -> Option<Box<[u8]>> {
        let db_idx = self.db_index(db);
        let map = self.dbs.get(db_idx)?;
        let n = map.len();
        if n == 0 {
            return None;
        }
        // The caller drew `pick` from the Env RNG (ADR-0003: the store reads no RNG).
        // Map it to a starting index, then probe forward DETERMINISTICALLY in the
        // sorted scan order, skipping expired keys, so an expired key at the picked
        // position does not yield `None` while live keys remain.
        let mut order: Vec<&[u8]> = map.keys().map(std::convert::AsRef::as_ref).collect();
        order.sort_unstable_by(|a, b| scan_hash(a).cmp(&scan_hash(b)).then(a.cmp(b)));
        let start = (pick % n as u64) as usize;
        for off in 0..n {
            let idx = (start + off) % n;
            let key = order[idx];
            if let Some(obj) = map.get(key) {
                if !obj.is_expired(now) {
                    return Some(key.to_vec().into_boxed_slice());
                }
            }
        }
        None
    }

    fn flush_db(&mut self, db: u32) -> u64 {
        let db_idx = self.db_index(db);
        let keys: Vec<Box<[u8]>> = match self.dbs.get(db_idx) {
            Some(map) => map.keys().cloned().collect(),
            None => return 0,
        };
        let mut removed = 0u64;
        for key in &keys {
            // remove_object fires the eviction/accounting remove hooks and frees bytes.
            if self.remove_object(db, db_idx, key) {
                removed += 1;
            }
        }
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
        // carried unchanged (KEYSPACE.md "moves the value object INTACT").
        let Some(mut obj) = self.dbs[src_idx].get(src).cloned() else {
            return MoveOutcome::NoSource;
        };
        obj.key = dst.to_vec().into_boxed_slice();

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
            // O(1) Vec element swap: the per-DB maps trade places; no entry is created
            // or destroyed, so no hook fires and the accounting total is unchanged
            // (the same entries are still resident, just under different db ids).
            self.dbs.swap(ai, bi);
        }
    }
}

impl<E: EvictionHook, A: AccountingHook> ShardStore<E, A> {
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

    /// Insert a fully-formed [`KvObj`] under `db`, bypassing the string-only
    /// command path. This is the only way in PR-2a to plant a NON-string value
    /// (PR-2a commands produce only Strings), so the WRONGTYPE path of GET/GETSET/
    /// STRLEN can be exercised before collections land. The accounting/eviction
    /// hooks fire as for any insert. Reserved for tests and the future collection
    /// commands; documented as a seam, not a fifth primitive.
    pub fn insert_object(&mut self, db: u32, obj: KvObj) {
        let db_idx = self.db_index(db);
        let key = obj.key.clone();
        self.put_object(db, db_idx, &key, obj);
    }
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
    int_decimal_bytes(n)
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
        let mut out = Vec::new();
        let mut cursor = ScanCursor::START;
        // A generous loop bound so a cursor bug fails the test rather than hangs.
        for _ in 0..(order.len() + 4) {
            let plan = scan_plan(order, cursor, count);
            out.extend(plan.examined.iter().copied());
            if plan.next.is_start() {
                return out;
            }
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
        let plan = scan_plan(&[], ScanCursor::START, 10);
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
        let plan1 = scan_plan(&o, ScanCursor::START, 1);
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
        let plan2 = scan_plan(&o, plan1.next, 1);
        assert_eq!(plan2.examined, vec![&b"d"[..]]);
        assert!(plan2.next.is_start(), "complete after the last group");
    }

    #[test]
    fn non_terminal_cursor_is_never_zero_even_with_a_zero_hash_key() {
        // A key whose scan_hash is 0 sorts FIRST and is examined on the start batch, so
        // the next cursor is strictly greater than 0. A returned 0 thus unambiguously
        // means complete, never "resume from the 0-hash key".
        let o = order(&[(0, b"zero"), (1, b"one"), (2, b"two")]);
        let plan = scan_plan(&o, ScanCursor::START, 1);
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
        let plan = scan_plan(&o, ScanCursor::START, 2);
        assert_eq!(plan.examined.len(), 2);
        assert_eq!(plan.next, ScanCursor(3), "resume at the 3rd key's hash");
    }
}
