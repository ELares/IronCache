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
use hashbrown::hash_map::Entry;
use ironcache_storage::{
    AccountingHook, CountingAccounting, DataType, EvictionHook, ExpireWrite, NewValue,
    NullEviction, OccupiedEntry, RmwAction, RmwEntry, RmwStep, Store, UnixMillis, ValueRef,
};
use kvobj::{KvObj, int_decimal_bytes};

use std::hash::BuildHasher;

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
    /// The hasher used to derive the full 64-bit key hash handed to the eviction
    /// hook (the same hash basis SCAN orders on, #129). It is `hashbrown`'s default
    /// `DefaultHashBuilder`; seeded-hash tuning is a #8 follow-up.
    hash_builder: hashbrown::DefaultHashBuilder,
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
            hash_builder: hashbrown::DefaultHashBuilder::default(),
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

    /// The full 64-bit key hash for the eviction hook (and the SCAN-order basis).
    fn key_hash(&self, key: &[u8]) -> u64 {
        self.hash_builder.hash_one(key)
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
            let key_hash = self.key_hash(key);
            if let Some(obj) = self.dbs[db_idx].remove(key) {
                let bytes = obj.accounted_bytes();
                self.account_sub(bytes);
                self.eviction.on_remove(db, key_hash, bytes);
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
        let key_hash = self.key_hash(key);
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
            self.eviction.on_remove(db, key_hash, old);
        }
        self.account_add(new_bytes);
        self.eviction.on_insert(db, key_hash, new_bytes);
        old_bytes.is_some()
    }

    /// Remove `key`'s object, crediting the hooks. Returns whether it existed
    /// (caller guarantees any due expiry already ran, so an existing entry is live).
    ///
    /// `db` is the validated logical DB id passed to the hooks; `db_idx` is the
    /// (possibly clamped) Vec index for the map (see [`Self::db_index`]).
    fn remove_object(&mut self, db: u32, db_idx: usize, key: &[u8]) -> bool {
        let key_hash = self.key_hash(key);
        if let Some(obj) = self.dbs[db_idx].remove(key) {
            let bytes = obj.accounted_bytes();
            self.account_sub(bytes);
            self.eviction.on_remove(db, key_hash, bytes);
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
        let key_hash = self.key_hash(key);
        self.eviction.on_access(db, key_hash);
        // The entry is present and live (expire_if_due returned true).
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
        let key_hash = self.key_hash(key);

        // Observe (atomically with the write that follows, on the owning core).
        let step = if live {
            self.eviction.on_access(db, key_hash);
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

impl<E: EvictionHook, A: AccountingHook> ShardStore<E, A> {
    /// Borrow the accounting hook (test/introspection helper).
    #[must_use]
    pub fn accounting(&self) -> &A {
        &self.accounting
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
