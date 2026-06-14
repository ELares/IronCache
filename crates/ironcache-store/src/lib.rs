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
use ironcache_eviction::EvictionPolicy;
use ironcache_storage::{
    AccountingHook, CountingAccounting, DataType, EvictionHook, ExpireWrite, NewValue,
    NullEviction, OccupiedEntry, RmwAction, RmwEntry, RmwStep, Store, UnixMillis, ValueRef,
};
use kvobj::{KvObj, int_decimal_bytes};

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
        // The bounded-scan guard for the #46 re-eligibility fix. Under a volatile-*
        // policy a non-TTL victim is RE-REGISTERED (kept as a candidate) rather than
        // dropped, so the policy can keep offering the same non-TTL keys forever. We
        // bound the scan by counting CONSECUTIVE skips since the last forward progress
        // (an eviction or an expired-reap): once that count exceeds the live entry
        // count, the policy has cycled through its whole offerable set with no eligible
        // TTL-bearing victim, so we stop and let the caller reply -OOM (matching Redis
        // volatile-* OOM-when-no-evictable-volatile-key). Any forward progress resets
        // the counter, so a run that keeps finding eligible TTL keys never trips it.
        let mut consecutive_skips: usize = 0;
        // A defensive secondary cap: even if a policy mis-behaves, the loop ends. With
        // the consecutive-skip bound above this should never be the binding limit.
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
            // forward progress, so reset the consecutive-skip guard.
            if is_expired {
                self.expire_if_due(db, db_idx, &key, now);
                consecutive_skips = 0;
                continue;
            }
            if volatile_only && lacks_ttl {
                // Only TTL-bearing keys are eligible. A non-TTL victim is NOT deleted
                // and NOT dropped from the policy: it is RE-REGISTERED so it remains a
                // candidate (the #46 re-eligibility fix). Bound the scan: if we have
                // skipped more keys in a row than the store holds, the policy has
                // cycled its whole offerable set with no eligible TTL-bearing victim,
                // so we STOP and let the caller reply -OOM.
                consecutive_skips += 1;
                self.eviction.re_register(db, &key);
                if consecutive_skips > self.len() {
                    break;
                }
                continue;
            }
            if self.remove_object(db, db_idx, &key) {
                evicted += 1;
                // Forward progress: reset the skip guard so a subsequent stretch of
                // non-TTL skips is measured afresh against the (now smaller) keyspace.
                consecutive_skips = 0;
            }
            // If the victim was already gone (a stale queue entry), the loop simply
            // asks for the next victim; it does not count as an eviction.
        }
        evicted
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
