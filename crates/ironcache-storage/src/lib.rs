// SPDX-License-Identifier: MIT OR Apache-2.0
//! The narrow-waist storage API for IronCache (STORAGE_API.md, #34).
//!
//! This is THE freeze point between the RESP/command layer (#15/#128/#129) above
//! and the per-shard store (#35, the `ironcache-store` crate) below. The command
//! layer depends ONLY on the four primitives plus the hook types named here; it
//! never names a concrete map or kvobj type, so the index/object layout can change
//! without reopening the command layer (the layering contract). This crate carries
//! NO hashbrown and NO concrete storage types by design.
//!
//! ## The four primitives (STORAGE_API.md "the four primitives")
//!
//! - [`Store::read`] - borrow the value (or absence) for a read-only command.
//! - [`Store::upsert`] - blind set, replacing any existing value.
//! - [`Store::delete`] - remove a key, returning whether it existed-and-was-live.
//! - [`Store::rmw`] - the atomic read-modify-write: the single atomic write funnel
//!   behind conditional SET (NX/XX/GET), SETNX, GETSET, INCR, and expiry-on-write.
//!   The closure observes the entry and returns the write decision; it runs on the
//!   owning core with exclusive access, so observe-and-write is atomic by
//!   construction with no lock (ADR-0002/0005).
//!
//! ## What `rmw` supports today, and the additive collection extension
//!
//! In PR-2a the [`RmwAction`] surface is `Insert`/`Replace`/`Keep`/`Delete`: the
//! closure decides on a WHOLE owned value (`Replace(NewValueOwned)`), so an in
//! -place value mutation is expressed as rebuild-and-`Replace`. This is the right
//! and complete surface for strings. It is NOT yet a value-internal in-place edit:
//! the closure cannot reach into a stored collection and push one list element
//! without handing back a whole rebuilt value.
//!
//! VALUE-INTERNAL in-place mutation (LPUSH appending to a list, HSET setting one
//! field, and the APPEND/SETRANGE efficiency path in PR-2b) is an ADDITIVE
//! extension to [`OccupiedEntry`]/[`RmwAction`] (a new mutable accessor / a new
//! action variant) to be co-designed WITH the Tier-2 value types. That extension
//! adds capability without churning existing string callers or PR-3's eviction/
//! accounting callers, so it does not reopen the waist. It is deliberately NOT
//! pre-designed here (no half-baked `Mutate` variant); this note records the plan.
//!
//! [`Store::contains`] and [`Store::type_of`] are cheap convenience entry points
//! for EXISTS/TYPE (each is `read().is_some()` / the read's data type), provided so
//! those commands do not pay to materialize a [`ValueRef`]; they are NOT a fifth
//! primitive.
//!
//! ## Freeze scope
//!
//! The FROZEN surface is: the key-level primitives (read/upsert/delete/rmw), the
//! TTL effect ([`ExpireWrite`] and the per-entry `expire_at` deadline), the
//! accounting hook, and the eviction victim-KEY selection ([`EvictionHook`]). The
//! value-internal in-place mutation described above, and the store-internal
//! snapshot pre-image hook (see [`Store`]), are ADDITIVE extensions that land with
//! their features without reopening this waist.
//!
//! ## Determinism and shared-nothing (ADR-0002/0003/0005)
//!
//! Time enters the store ONLY as a [`UnixMillis`] `now` argument passed by the
//! caller (computed from the Env clock at the binary edge); this crate imports
//! neither `std::time` nor `rand`. The trait is a synchronous, single-shard,
//! single-threaded contract: there is no `async`, no lock, and no atomic, because
//! the owning core has exclusive access.
//!
//! ## TTL (EXPIRATION.md, PR-2a slice)
//!
//! TTL is NOT a separate hook: it is an `Option<UnixMillis>` deadline carried on
//! the entry. The lazy expiry-on-read backstop lives inside `read`/`rmw`/
//! `contains`/`type_of`: an entry whose deadline has strictly passed (`now >
//! expire_at`, the Valkey boundary) reads as absent and is removed; a key is alive
//! at `now == expire_at`. The active per-shard timing wheel and the EXPIRE/TTL/PERSIST commands
//! are PR-3 and attach as a side structure keyed off this same field, with NO
//! signature change here.
//!
//! ## maxmemory admission / OOM (architecture decision)
//!
//! The write primitives are WRITE-ALWAYS-SUCCEEDS: they do NOT enforce a memory
//! ceiling. maxmemory admission and the out-of-memory reply are enforced ABOVE the
//! waist at the command-dispatch layer, matching Redis (`processCommand` checks the
//! command's `denyoom` flag and runs `freeMemoryIfNeeded`/`performEvictions` BEFORE
//! the command body, not inside the storage layer). This keeps the primitives
//! frozen and lets the dispatch layer own the policy: the store exposes
//! [`Store::used_memory`] (a read) today, and PR-3 adds an evict-to-fit path the
//! dispatch layer drives by calling [`EvictionHook::select_victim`] (which returns
//! the victim `(db, key)`) and then deleting it in a loop until under budget, or
//! replying `-OOM` for a `denyoom` write when nothing more can be freed. This is a
//! recorded decision, not a change to the FROZEN four primitives.

#![forbid(unsafe_code)]

use bytes::Bytes;

// ---------------------------------------------------------------------------
// Time basis (ADR-0003): the absolute wall-clock deadline, passed in by the
// caller. The store never reads a clock itself.
// ---------------------------------------------------------------------------

/// Absolute wall-clock milliseconds since the Unix epoch, the deadline basis for
/// TTL. Matches [`ironcache_env::Clock::now_unix_millis`] (the binary wraps that
/// value in this newtype before calling the store). Frozen so the absolute-TTL
/// commands (EXAT/PXAT/EXPIREAT, PR-3) all share one basis.
///
/// It is `Ord` so the deadline comparison (`now > deadline`, the Valkey
/// strictly-greater expiry boundary) is a plain comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnixMillis(pub u64);

// ---------------------------------------------------------------------------
// Type and encoding tags (OBJECT_LAYOUT.md #111, ENCODINGS.md #112).
// ---------------------------------------------------------------------------

/// The logical Redis data type of a value. Only [`DataType::String`] is produced
/// in PR-2a; the collection variants are reserved so the WRONGTYPE check and the
/// TYPE command do not churn the freeze-point enum when collections land.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    /// A string value (the only type produced in PR-2a).
    String,
    /// A list (reserved, not produced in PR-2a).
    List,
    /// A set (reserved).
    Set,
    /// A hash (reserved).
    Hash,
    /// A sorted set (reserved).
    ZSet,
    /// A stream (reserved).
    Stream,
}

impl DataType {
    /// The Redis-compatible type name reported by `TYPE` (`string`/`list`/...).
    #[must_use]
    pub const fn type_name(self) -> &'static str {
        match self {
            DataType::String => "string",
            DataType::List => "list",
            DataType::Set => "set",
            DataType::Hash => "hash",
            DataType::ZSet => "zset",
            DataType::Stream => "stream",
        }
    }
}

/// The internal encoding of a value, surfaced by `OBJECT ENCODING` (ADR-0009).
/// PR-2a produces the three string encodings; the collection encodings are
/// reserved so the freeze-point enum does not churn (ENCODINGS.md, ADR-0018).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// A canonical i64, stored with no value allocation (`OBJECT ENCODING` -> int).
    Int,
    /// A short string stored inline in the object (`OBJECT ENCODING` -> embstr).
    EmbStr,
    /// A string stored out-of-line (`OBJECT ENCODING` -> raw).
    Raw,
    /// Reserved collection encodings (not produced in PR-2a).
    ListPack,
    /// Reserved (intset).
    IntSet,
    /// Reserved (hashtable for large hashes/sets).
    HashTable,
    /// Reserved (skiplist for large zsets).
    SkipList,
}

impl Encoding {
    /// The Redis-compatible encoding name reported by `OBJECT ENCODING`.
    #[must_use]
    pub const fn encoding_name(self) -> &'static str {
        match self {
            Encoding::Int => "int",
            Encoding::EmbStr => "embstr",
            Encoding::Raw => "raw",
            Encoding::ListPack => "listpack",
            Encoding::IntSet => "intset",
            Encoding::HashTable => "hashtable",
            Encoding::SkipList => "skiplist",
        }
    }
}

// ---------------------------------------------------------------------------
// The read borrow (STORAGE_API.md "Read can hand out a borrow ... zero-copy to
// the serializer"). Valid for the duration of one command on the owning core.
// ---------------------------------------------------------------------------

/// A read-only view of a live value, borrowed for the duration of one command.
///
/// The command layer always sees BYTES and never the int encoding: an int-encoded
/// value materializes its decimal bytes into the view (carried as an owned
/// [`Bytes`] so the borrow can outlive the formatting scratch) while still
/// reporting [`Encoding::Int`] from [`ValueRef::encoding`]. A string-encoded value
/// borrows the stored bytes directly.
///
/// Because the shard is single-owner (ADR-0005) and the borrow is tied to the
/// store borrow that produced it, Rust's borrow checker statically prevents a
/// concurrent mutation from invalidating the view (OBJECT_LAYOUT.md borrow
/// -lifetime contract).
#[derive(Debug, Clone)]
pub struct ValueRef<'a> {
    data_type: DataType,
    encoding: Encoding,
    expire_at: Option<UnixMillis>,
    bytes: ValueBytes<'a>,
}

/// The byte source behind a [`ValueRef`]: either a direct borrow into the stored
/// buffer (string encodings) or owned decimal bytes materialized from an int.
#[derive(Debug, Clone)]
enum ValueBytes<'a> {
    /// A borrow into the stored value buffer (embstr/raw).
    Borrowed(&'a [u8]),
    /// Decimal bytes materialized from an int-encoded value.
    Owned(Bytes),
}

impl<'a> ValueRef<'a> {
    /// Construct a view that borrows the stored bytes directly (embstr/raw).
    #[must_use]
    pub fn borrowed(
        data_type: DataType,
        encoding: Encoding,
        expire_at: Option<UnixMillis>,
        bytes: &'a [u8],
    ) -> Self {
        ValueRef {
            data_type,
            encoding,
            expire_at,
            bytes: ValueBytes::Borrowed(bytes),
        }
    }

    /// Construct a view over decimal bytes materialized from an int-encoded value.
    /// The reported encoding is still [`Encoding::Int`]; only the bytes are owned.
    #[must_use]
    pub fn from_int_bytes(
        data_type: DataType,
        expire_at: Option<UnixMillis>,
        bytes: Bytes,
    ) -> Self {
        ValueRef {
            data_type,
            encoding: Encoding::Int,
            expire_at,
            bytes: ValueBytes::Owned(bytes),
        }
    }

    /// The logical data type (for WRONGTYPE checks).
    #[must_use]
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    /// The internal encoding (for `OBJECT ENCODING`).
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        self.encoding
    }

    /// The TTL deadline, if any.
    #[must_use]
    pub fn expire_at(&self) -> Option<UnixMillis> {
        self.expire_at
    }

    /// The value bytes. For an int-encoded value these are the canonical decimal
    /// digits; the command layer never sees the int representation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match &self.bytes {
            ValueBytes::Borrowed(b) => b,
            ValueBytes::Owned(b) => b,
        }
    }

    /// The byte length of the value (STRLEN). For an int this is the decimal digit
    /// count (including a leading `-`).
    #[must_use]
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    /// Whether the value is the empty string.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }
}

// ---------------------------------------------------------------------------
// Write values (STORAGE_API.md). A plain SET of a numeric string can store the
// int encoding; a borrowed-bytes write avoids an allocation when the store can
// classify in place (NewValue), while RMW-built values are owned (NewValueOwned).
// ---------------------------------------------------------------------------

/// A value to write, borrowed from the request for the duration of the call. Used
/// by [`Store::upsert`], the blind-set path: `Bytes` lets the store classify the
/// encoding from the request bytes with no intermediate allocation; `Int` lets a
/// caller that already parsed a number store the int encoding directly.
#[derive(Debug, Clone, Copy)]
pub enum NewValue<'a> {
    /// Raw bytes; the store classifies int/embstr/raw (ENCODINGS.md).
    Bytes(&'a [u8]),
    /// An already-parsed integer; stored as the int encoding with no value alloc.
    Int(i64),
}

/// An owned value to write, produced inside an [`Store::rmw`] closure (which must
/// not hold a borrow of the entry while returning the new value). Same variants as
/// [`NewValue`] but owning the bytes.
#[derive(Debug, Clone)]
pub enum NewValueOwned {
    /// Owned bytes; the store classifies int/embstr/raw (ENCODINGS.md).
    Bytes(Bytes),
    /// An already-parsed integer; stored as the int encoding with no value alloc.
    Int(i64),
}

impl NewValueOwned {
    /// Convenience constructor for owned bytes from anything byte-like.
    pub fn bytes(b: impl Into<Bytes>) -> Self {
        NewValueOwned::Bytes(b.into())
    }
}

/// How a write affects the entry's TTL deadline (SET options, EXPIRATION.md).
///
/// - `KEEPTTL` maps to [`ExpireWrite::Keep`].
/// - `EX`/`PX`/`EXAT`/`PXAT` map to [`ExpireWrite::Set`] with the absolute
///   deadline (the command layer converts relative EX/PX against `now`).
/// - a default SET (no TTL option) maps to [`ExpireWrite::Clear`].
/// - an in-place edit that must not touch the TTL (e.g. a future APPEND) maps to
///   [`ExpireWrite::Unchanged`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireWrite {
    /// Leave the existing deadline exactly as it is (in-place edits).
    Unchanged,
    /// Preserve the existing deadline across a value replacement (KEEPTTL).
    Keep,
    /// Set the deadline to this absolute time.
    Set(UnixMillis),
    /// Remove any deadline (default SET semantics).
    Clear,
}

// ---------------------------------------------------------------------------
// RMW: the atomic write funnel (STORAGE_API.md). In PR-2a the closure decides on a
// whole owned value (Insert/Replace/Keep/Delete); value-internal in-place mutation
// for collections is the additive extension noted in the module docs.
// ---------------------------------------------------------------------------

/// The entry handed to an [`Store::rmw`] closure: either the key is absent
/// ([`RmwEntry::Vacant`]) or present and live ([`RmwEntry::Occupied`]). A lazily
/// -expired key is presented as `Vacant` (the backstop ran before the closure).
#[derive(Debug)]
pub enum RmwEntry<'a> {
    /// No live value for the key.
    Vacant,
    /// A live value; observe it through [`OccupiedEntry`] before deciding the write.
    Occupied(OccupiedEntry<'a>),
}

/// A read-only observation of the occupied entry inside an [`Store::rmw`] closure.
///
/// The closure observes the current value (type/bytes/encoding/TTL) and then
/// returns an [`RmwStep`] describing the write; observation and write are atomic on
/// the owning core (no lock). The same int-materialization rule as [`ValueRef`]
/// applies to [`OccupiedEntry::as_bytes`]: the closure sees decimal bytes for an
/// int-encoded value.
///
/// PR-2a observation is READ-ONLY (the write is expressed as the whole owned value
/// in [`RmwAction`]). A mutable, value-internal accessor for collection in-place
/// edits (LPUSH/HSET) and the PR-2b APPEND/SETRANGE efficiency path is the additive
/// extension noted in the module docs; it is not present yet.
#[derive(Debug)]
pub struct OccupiedEntry<'a> {
    data_type: DataType,
    encoding: Encoding,
    expire_at: Option<UnixMillis>,
    bytes: ValueBytes<'a>,
}

impl<'a> OccupiedEntry<'a> {
    /// Construct from a direct borrow into the stored bytes (embstr/raw).
    #[must_use]
    pub fn borrowed(
        data_type: DataType,
        encoding: Encoding,
        expire_at: Option<UnixMillis>,
        bytes: &'a [u8],
    ) -> Self {
        OccupiedEntry {
            data_type,
            encoding,
            expire_at,
            bytes: ValueBytes::Borrowed(bytes),
        }
    }

    /// Construct over decimal bytes materialized from an int-encoded value.
    #[must_use]
    pub fn from_int_bytes(
        data_type: DataType,
        expire_at: Option<UnixMillis>,
        bytes: Bytes,
    ) -> Self {
        OccupiedEntry {
            data_type,
            encoding: Encoding::Int,
            expire_at,
            bytes: ValueBytes::Owned(bytes),
        }
    }

    /// The logical data type (for WRONGTYPE checks inside the closure).
    #[must_use]
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    /// The internal encoding.
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        self.encoding
    }

    /// The TTL deadline, if any.
    #[must_use]
    pub fn expire_at(&self) -> Option<UnixMillis> {
        self.expire_at
    }

    /// The current value bytes (decimal digits for an int-encoded value).
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match &self.bytes {
            ValueBytes::Borrowed(b) => b,
            ValueBytes::Owned(b) => b,
        }
    }
}

/// The write decision an [`Store::rmw`] closure returns: an [`RmwAction`] on the
/// value, an [`ExpireWrite`] on the TTL, and the `reply` to hand back to the
/// command layer. The store applies the action and TTL atomically and returns
/// `reply`; the closure is monomorphized (no `dyn`, no lock).
#[derive(Debug)]
pub struct RmwStep<R> {
    /// What to do to the value.
    pub action: RmwAction,
    /// What to do to the TTL deadline.
    pub expire: ExpireWrite,
    /// The reply value handed back to the caller of `rmw`.
    pub reply: R,
}

/// The value mutation an [`RmwStep`] requests. PR-2a operates on the WHOLE owned
/// value: an in-place value edit is expressed as rebuild-and-[`RmwAction::Replace`].
/// A value-internal in-place variant for collections (and the APPEND/SETRANGE
/// efficiency path) is the additive extension noted in the module docs; it is not
/// present yet, to avoid freezing a half-designed mutation surface.
#[derive(Debug)]
pub enum RmwAction {
    /// Leave the value untouched (the TTL may still change via `expire`).
    Keep,
    /// Insert a new value; meaningful when the entry was [`RmwEntry::Vacant`].
    /// (If the entry was occupied, `Insert` behaves like `Replace`.)
    Insert(NewValueOwned),
    /// Replace the existing value; meaningful when the entry was occupied.
    /// (If the entry was vacant, `Replace` behaves like `Insert`.)
    Replace(NewValueOwned),
    /// Delete the entry.
    Delete,
}

// ---------------------------------------------------------------------------
// Hooks (STORAGE_API.md "callback hooks"). Carried NOW so PR-3 (eviction, honest
// accounting) needs no primitive-signature change.
// ---------------------------------------------------------------------------

/// The eviction policy hook (#48, ADR-0008 S3-FIFO). The store fires these from
/// INSIDE the primitives: `on_access` on a read/rmw of a live key, `on_insert` on
/// an insert, `on_remove` on a delete/replace/expiry. PR-2a ships
/// [`NullEviction`] (all no-ops); the real S3-FIFO policy and `select_victim`
/// driving land in PR-3 (`ironcache-eviction`).
///
/// `db` is the validated logical DB id (the same id SELECT validates, KEYSPACE.md);
/// `key` is the entry's key BYTES.
///
/// ## Why this RESERVED hook is byte-keyed (a PR-3 refinement)
///
/// This trait was reserved in PR-2a with a `key_hash: u64` argument as a placeholder.
/// PR-3 refines it to take `key: &[u8]` directly, because the only thing
/// [`Self::select_victim`] can return that the store can act on is the OWNED key
/// bytes (`Box<[u8]>`, the store's table key): a u64 hash cannot drive a delete from
/// a byte-keyed map (EVICTION.md `evict_victim() -> key`). Keeping the policy's
/// queues keyed by the same bytes it must return makes the policy coherent in ONE
/// place rather than carrying a parallel hash->key side map. The store already has
/// `key: &[u8]` at every call site, so this refinement is mechanical there. This is
/// a reserved-hook refinement, NOT a change to the FROZEN four primitives.
pub trait EvictionHook {
    /// A live key was read or observed in an rmw.
    fn on_access(&mut self, db: u32, key: &[u8]);
    /// A new value was inserted, costing `bytes` logical bytes.
    fn on_insert(&mut self, db: u32, key: &[u8], bytes: usize);
    /// A value was removed (delete, replace, or expiry), freeing `bytes`.
    fn on_remove(&mut self, db: u32, key: &[u8], bytes: usize);
    /// Pick a victim to evict when over budget, or `None` if none. Returns the
    /// `(db, key)` pair so the caller (`ShardStore::evict_to_fit`) can delete it
    /// from the correct per-DB map: the KEY bytes (not a hash) are returned because
    /// the store's table is keyed by the owned key bytes (`Box<[u8]>`, ADR-0008), so
    /// a hash cannot drive the delete (EVICTION.md `evict_victim() -> key`). PR-3's
    /// S3-FIFO evicts through this surface.
    fn select_victim(&mut self) -> Option<(u32, Box<[u8]>)>;
}

/// A no-op eviction hook (PR-2a default). Eviction-on-by-default (ADR-0007) and
/// the S3-FIFO policy land in PR-3; until then nothing is ever selected.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullEviction;

impl EvictionHook for NullEviction {
    #[inline]
    fn on_access(&mut self, _db: u32, _key: &[u8]) {}
    #[inline]
    fn on_insert(&mut self, _db: u32, _key: &[u8], _bytes: usize) {}
    #[inline]
    fn on_remove(&mut self, _db: u32, _key: &[u8], _bytes: usize) {}
    #[inline]
    fn select_victim(&mut self) -> Option<(u32, Box<[u8]>)> {
        None
    }
}

/// The accounting hook (#41/ADR-0006): every insert/remove updates the shard's
/// logical byte count so `maxmemory` is honest (invariant 3). PR-2a uses the
/// logical-byte [`CountingAccounting`]; PR-2b swaps the source of
/// [`Store::used_memory`] to the jemalloc `stats.allocated` mallctl without
/// changing this hook.
pub trait AccountingHook {
    /// Add `bytes` to the accounted total.
    fn add(&mut self, bytes: usize);
    /// Subtract `bytes` from the accounted total (saturating at zero).
    fn sub(&mut self, bytes: usize);
}

/// A logical-byte counter (PR-2a accounting). `used()` is what
/// [`Store::used_memory`] returns in PR-2a.
#[derive(Debug, Default, Clone, Copy)]
pub struct CountingAccounting {
    used: u64,
}

impl CountingAccounting {
    /// A fresh zeroed counter.
    #[must_use]
    pub fn new() -> Self {
        CountingAccounting { used: 0 }
    }

    /// The current accounted logical-byte total.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.used
    }
}

impl AccountingHook for CountingAccounting {
    #[inline]
    fn add(&mut self, bytes: usize) {
        self.used = self.used.saturating_add(bytes as u64);
    }

    #[inline]
    fn sub(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes as u64);
    }
}

// ---------------------------------------------------------------------------
// The Store trait: the four primitives + the cheap EXISTS/TYPE backstops +
// used_memory. The trait is concrete (the impl is generic over the hooks).
// ---------------------------------------------------------------------------

/// The storage waist (STORAGE_API.md). The command layer is generic over `S: Store`
/// and names only this trait and the types above; the concrete per-shard
/// implementation lives in `ironcache-store`.
///
/// Every method takes `&mut self` (writes mutate; reads may lazily expire and so
/// remove) and the absolute `now` deadline basis (ADR-0003: the store never reads
/// a clock). `db` selects the logical database (KEYSPACE.md per-DB keyspace).
pub trait Store {
    /// Borrow the live value for `key`, or `None` if absent OR lazily expired. An
    /// entry whose deadline has strictly passed (`now > expire_at`, the Valkey
    /// boundary; alive at `now == expire_at`) is removed and reported as `None`
    /// (the lazy backstop, EXPIRATION.md).
    fn read(&mut self, db: u32, key: &[u8], now: UnixMillis) -> Option<ValueRef<'_>>;

    /// Blind set: store `value` for `key` with the `expire` TTL effect, replacing
    /// any existing value. Returns whether a LIVE key existed before the write (so
    /// a caller can distinguish create from overwrite; a lazily-expired prior
    /// value counts as not-existing).
    fn upsert(
        &mut self,
        db: u32,
        key: &[u8],
        value: NewValue<'_>,
        expire: ExpireWrite,
        now: UnixMillis,
    ) -> bool;

    /// Remove `key`. Returns whether it existed AND was live (a lazily-expired key
    /// is removed but reported as not-existing, matching Redis DEL semantics).
    fn delete(&mut self, db: u32, key: &[u8], now: UnixMillis) -> bool;

    /// The atomic read-modify-write primitive. The closure observes the entry
    /// ([`RmwEntry::Vacant`] or [`RmwEntry::Occupied`]) and returns an [`RmwStep`]
    /// carrying the value action, the TTL effect, and the reply `R`. The store
    /// applies the step atomically on the owning core (no lock) and returns `R`.
    /// A lazily-expired key is presented as `Vacant` and removed before the closure
    /// runs.
    fn rmw<R>(
        &mut self,
        db: u32,
        key: &[u8],
        now: UnixMillis,
        f: impl FnOnce(RmwEntry<'_>) -> RmwStep<R>,
    ) -> R;

    /// Whether `key` is present and live. Equivalent to `read(..).is_some()` but
    /// avoids materializing a [`ValueRef`]; provided for cheap EXISTS, not a fifth
    /// primitive. Lazily expires a stale key as a side effect.
    fn contains(&mut self, db: u32, key: &[u8], now: UnixMillis) -> bool;

    /// The [`DataType`] of `key`, or `None` if absent/expired. For TYPE; never
    /// returns WRONGTYPE. Lazily expires a stale key as a side effect.
    fn type_of(&mut self, db: u32, key: &[u8], now: UnixMillis) -> Option<DataType>;

    /// The accounted logical-byte total (PR-2a: the [`CountingAccounting`] counter;
    /// PR-2b: the jemalloc `stats.allocated` mallctl). Not yet wired into INFO in
    /// PR-2a (the INFO `used_memory` stub stays as-is per the PR-2a scope line).
    fn used_memory(&self) -> u64;
}

// ---------------------------------------------------------------------------
// Admission surface (ADMISSION.md #128, ADR-0007). A SEPARATE trait from the frozen
// four primitives: it lets the command-dispatch layer enforce the maxmemory ceiling
// (evict-to-fit in cache mode, reply -OOM in datastore/noeviction) WITHOUT naming the
// concrete store or the concrete policy. The store implements it over its configured
// policy; dispatch bounds on `S: Store + Admit`.
// ---------------------------------------------------------------------------

/// The maxmemory admission surface the dispatch layer drives (ADMISSION.md). This is
/// NOT one of the frozen four primitives; it is an additive waist trait (the eviction
/// victim-KEY selection it builds on, [`EvictionHook`], was always reserved). The
/// concrete per-shard store implements it over its configured eviction policy, so
/// dispatch enforces the ceiling generically.
pub trait Admit {
    /// Whether the configured policy evicts at the ceiling (cache mode) rather than
    /// rejecting the write (strict datastore / `noeviction`). Dispatch reads this to
    /// choose evict-to-fit vs an immediate `-OOM`.
    fn policy_evicts(&self) -> bool;

    /// Whether the configured policy restricts victims to TTL-bearing keys (the
    /// `volatile-*` family). For INFO / introspection.
    fn policy_volatile_only(&self) -> bool;

    /// The CONFIGURED `maxmemory-policy` name the policy echoes VERBATIM (INFO /
    /// CONFIG GET). Redis round-trips the configured enum string unchanged (e.g.
    /// `allkeys-lfu`, `volatile-ttl`), NOT a substituted engine-family name, so this
    /// returns the exact configured spelling and is safe for INFO and CONFIG GET.
    fn policy_name(&self) -> String;

    /// Evict entries until `used_memory()` is below `budget_bytes`, or until the
    /// policy can free no more. Returns the number of entries evicted (the caller
    /// bumps the `evicted_keys` counter and, if still over budget, replies `-OOM`).
    fn evict_to_fit(&mut self, budget_bytes: u64, now: UnixMillis) -> u64;

    /// The access-frequency estimate for `(db, key)` for OBJECT FREQ, or `None` if the
    /// configured policy keeps no frequency estimate (every non-LFU policy). The
    /// dispatch layer maps `None` to the canonical OBJECT FREQ LFU-gating error and a
    /// `Some(v)` to the integer reply. Additive (read-only introspection over the
    /// configured policy), NOT one of the frozen four primitives.
    fn access_freq(&self, db: u32, key: &[u8]) -> Option<u8>;
}

// ---------------------------------------------------------------------------
// Active-expiry surface (EXPIRATION.md #51). A SEPARATE trait from the frozen four
// primitives (like Admit): it lets the dispatch layer drive the per-shard timing
// wheel's active drain WITHOUT naming the concrete store. The lazy expiry-on-read
// backstop inside the four primitives remains the correctness guarantee; this surface
// is the BOUNDED active reclamation that keeps resident memory for expired keys low.
// ---------------------------------------------------------------------------

/// The active-expiry reaping surface the dispatch layer drives from the timing wheel
/// (EXPIRATION.md). NOT one of the frozen four primitives; an additive waist trait
/// (the per-entry `expire_at` deadline it acts on was always part of the waist). The
/// concrete per-shard store implements it; dispatch bounds on `S: Store + ActiveExpiry`
/// so the bounded active drain runs generically over the same `now` basis.
pub trait ActiveExpiry {
    /// Reap `key` ONLY if it is present and its stored deadline has STRICTLY passed at
    /// `now` (`now > expire_at`, the Valkey boundary). Returns whether a key was
    /// actually reaped (firing the remove hooks). The timing wheel may offer a STALE
    /// entry (a re-TTL'd / PERSISTed / overwritten key), so the implementation
    /// RE-CHECKS the real `expire_at` and reaps only a genuinely-expired key; a live
    /// key is left untouched and reported `false`. This is why a wheel registration
    /// need not be kept consistent with the store (the drain self-corrects).
    fn reap_if_expired(&mut self, db: u32, key: &[u8], now: UnixMillis) -> bool;
}

// ---------------------------------------------------------------------------
// Runtime policy-swap surface (CONFIG.md maxmemory-policy hot-swap, #50/#85, PR-4b).
// A SEPARATE trait from the frozen four primitives (like Admit/ActiveExpiry): it lets
// the dispatch layer rebuild a shard's eviction policy on a `CONFIG SET
// maxmemory-policy` WITHOUT naming the concrete policy enum (which lives in
// ironcache-eviction, a crate the waist does not depend on, to keep the waist
// policy-agnostic). The waist names only a policy NAME string and an RNG SEED; the
// concrete store maps the name to a policy through ironcache-eviction. This is
// ADDITIVE: it adds NO method to the four `Store` primitives and does not change their
// signatures.
// ---------------------------------------------------------------------------

/// The runtime eviction-policy swap surface (CONFIG.md `maxmemory-policy` hot-swap).
/// NOT one of the frozen four primitives; an additive waist trait. A `CONFIG SET
/// maxmemory-policy` on the dispatch path rebuilds the serving shard's policy through
/// this, seeded from the Env RNG (ADR-0003 determinism: the seed comes through the
/// determinism seam, never std rand). The concrete per-shard store implements it.
///
/// The previous policy's eviction RANKING HISTORY (S3-FIFO queue positions / W-TinyLFU
/// sketch counts / LRU recency) is reset on swap, which is acceptable (Redis itself
/// warns the policy switch "takes time to adjust"). But the new policy IS RE-SEEDED from
/// the live keyspace (its roster is repopulated from every resident key), so eviction
/// works IMMEDIATELY after a swap: a populated over-budget shard can still select a
/// victim on the very next write rather than spuriously replying `-OOM` until keys are
/// re-observed. Only the ranking metadata resets; the candidate set does not.
pub trait PolicySwap {
    /// Rebuild this shard's eviction policy from the Redis `maxmemory-policy` `name`,
    /// seeding any RNG-bearing variant (`*-random`) from `rng_seed` (drawn by the
    /// caller through the Env RNG seam, ADR-0003). Returns `false` if `name` is not a
    /// recognized policy name (the dispatch layer validated it already, so this is the
    /// defensive path).
    ///
    /// On a successful swap the new policy's RANKING HISTORY starts empty (the
    /// eviction-ordering reset CONFIG.md/Redis both document), but its CANDIDATE ROSTER
    /// is RE-SEEDED from the live keyspace: every resident, not-yet-lazily-expired entry
    /// (its deadline has not strictly passed at `now`) is re-observed into the new policy
    /// so `select_victim` has candidates immediately and eviction does not falsely OOM.
    /// `now` is the lazy-expiry boundary used to skip entries past their deadline.
    fn set_policy_by_name(&mut self, name: &str, rng_seed: u64, now: UnixMillis) -> bool;
}

// ---------------------------------------------------------------------------
// Keyspace iteration surface (KEYSPACE.md #129). A SEPARATE trait from the frozen
// four primitives (like Admit and ActiveExpiry): it lets the command-dispatch layer
// run the generic keyspace commands (SCAN/KEYS/DBSIZE/RANDOMKEY/RENAME/COPY/MOVE/
// SWAPDB/FLUSHDB/FLUSHALL) WITHOUT naming the concrete map or kvobj type. The new
// iteration capability the SCAN cursor needs is additive, so it does NOT reopen the
// frozen waist; dispatch bounds on `S: Store + Admit + ActiveExpiry + Keyspace`.
// ---------------------------------------------------------------------------

/// A SCAN cursor (KEYSPACE.md "SCAN cursor-stability contract"). The wire form is a
/// decimal string ([`Self::to_token`]/[`Self::from_token`]); `0` is the start and a
/// returned `0` means the iteration is complete.
///
/// ## What the cursor encodes (the freeze-sensitive headline)
///
/// The value is the resume point in ASCENDING FULL 64-bit key-hash order, where the
/// hash is a FIXED-SEED stable hash recomputable from the key bytes (NOT hashbrown's
/// per-table RandomState tag, NOT std `rand`): the last full key-hash already emitted.
/// Resumption (KEYSPACE.md) returns keys whose hash is STRICTLY GREATER than the
/// cursor hash, plus any not-yet-emitted keys whose hash EQUALS the cursor hash
/// (discriminated by raw key bytes), so two distinct keys that collide on the same
/// 64-bit hash are never skipped. Because a key's full hash is invariant across a
/// `hashbrown` all-at-once resize, iteration is TOTAL across a resize (the
/// rehash-tolerance guarantee KEYSPACE.md mandates); reverse-binary iteration over the
/// SwissTable bucket index is explicitly rejected there (it is tied to Redis's
/// incremental two-table rehash, which does not transfer).
///
/// ## Reserved high bits (do NOT populate in PR-4a)
///
/// KEYSPACE.md reserves the cursor's HIGH bits for a future SLOT id so a cluster
/// coordinator can fan SCAN out across slots/nodes and report a migrated slot with a
/// MOVED-style redirection. PR-4a is single-node single-slot, so the slot field is
/// always zero and the whole 64-bit value carries the intra-slot hash position. The
/// equal-hash discriminator KEYSPACE.md also mentions is NOT carried in the cursor
/// integer here: the store derives it from the raw key bytes at resume time (it
/// re-emits same-hash keys whose bytes sort after the largest already-emitted at that
/// hash), which needs no extra cursor field and keeps the wire token a plain decimal
/// hash. This is the documented narrowing of KEYSPACE.md's open "bit-split" question
/// for the single-slot case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScanCursor(pub u64);

impl ScanCursor {
    /// The start-of-iteration cursor (`0`).
    pub const START: ScanCursor = ScanCursor(0);

    /// Whether this is the start/complete sentinel (`0`).
    #[must_use]
    pub fn is_start(self) -> bool {
        self.0 == 0
    }

    /// The wire token: the decimal-string form a client sends and receives (Redis
    /// SCAN cursors are decimal bulk strings).
    #[must_use]
    pub fn to_token(self) -> String {
        self.0.to_string()
    }

    /// Parse a wire token (a decimal string) back into a cursor. Returns `None` on a
    /// non-decimal / out-of-u64-range token (the caller maps that to the canonical
    /// `invalid cursor` error).
    #[must_use]
    pub fn from_token(token: &[u8]) -> Option<ScanCursor> {
        if token.is_empty() {
            return None;
        }
        let mut acc: u64 = 0;
        for &b in token {
            if !b.is_ascii_digit() {
                return None;
            }
            acc = acc.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
        }
        Some(ScanCursor(acc))
    }
}

/// How [`Keyspace::move_object`] relocates the source value (RENAME/RENAMENX/MOVE
/// consume the source; COPY leaves it in place).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveMode {
    /// Move the value object to the destination, REMOVING the source (RENAME /
    /// RENAMENX / MOVE).
    Rename,
    /// Copy the value object to the destination, LEAVING the source intact (COPY).
    Copy,
}

/// The outcome of a [`Keyspace::move_object`] call (RENAME/RENAMENX/COPY/MOVE).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveOutcome {
    /// The source was moved to the destination (RENAME / MOVE success).
    Moved,
    /// The source was copied to the destination (COPY success).
    Copied,
    /// The source key did not exist (or was lazily expired): nothing was done.
    NoSource,
    /// The destination already held a live key and `replace` was false: nothing was
    /// done (RENAMENX-returns-0 / COPY-without-REPLACE / MOVE-dest-occupied).
    DestExists,
}

/// The keyspace iteration + bulk-management surface the dispatch layer drives for the
/// generic keyspace commands (KEYSPACE.md). NOT one of the frozen four primitives; an
/// additive waist trait alongside [`Admit`]/[`ActiveExpiry`]. The concrete per-shard
/// store implements it; dispatch bounds on `S: Store + Admit + ActiveExpiry + Keyspace`.
///
/// ## Cross-shard scope (single-shard-per-connection for now)
///
/// Every method operates on ONE shard's DB(s). Since no cross-shard key routing exists
/// yet (the store IS the connection's whole keyspace, ADR-0011 single-node-first), SCAN
/// / KEYS / DBSIZE / RANDOMKEY / FLUSHDB cover the connection's entire keyspace. A true
/// cross-shard fan-out (SCAN every node and merge, with the cursor's reserved slot bits
/// driving MOVED-style redirection) is DEFERRED to the coordinator/clustering work
/// (#29/#75); the iteration seam is SHAPED for it ([`ScanCursor`]'s reserved high bits)
/// but PR-4a builds no fan-out. `move_object` is same-shard only for the same reason
/// (a cross-shard RENAME/COPY routes through the coordinator later, KEYSPACE.md).
pub trait Keyspace {
    /// Run ONE bounded SCAN batch over `db` in ascending full-key-hash order, starting
    /// after `cursor` (KEYSPACE.md cursor-stability contract). `count` bounds the keys
    /// EXAMINED this call (a hint, like Redis: an empty batch with a non-zero returned
    /// cursor is legal). `keep(key, type)` is the MATCH/TYPE filter applied BEFORE a
    /// key is cloned into the result, so a filtered-out key costs no allocation. Lazily
    /// -expired keys (deadline strictly past `now`) are skipped (NOT returned, NOT
    /// reaped here; the lazy backstop / active drain reclaim them). Returns the next
    /// cursor (`ScanCursor(0)` = iteration complete) and the kept keys for this batch.
    fn scan_step(
        &mut self,
        db: u32,
        cursor: ScanCursor,
        count: usize,
        now: UnixMillis,
        keep: impl FnMut(&[u8], DataType) -> bool,
    ) -> (ScanCursor, Vec<Box<[u8]>>);

    /// The number of keys in `db` (DBSIZE). A RAW live-ish count: Redis does NOT
    /// actively expire on DBSIZE (it returns the dict size, including not-yet-reaped
    /// expired keys), so this returns the table length WITHOUT running the lazy
    /// backstop, matching the oracle.
    fn db_len(&self, db: u32) -> usize;

    /// A pseudo-random live key from `db` (RANDOMKEY), or `None` if `db` is empty (of
    /// live keys). `pick` is a random index the CALLER drew from the Env RNG (ADR-0003:
    /// the store reads no RNG; randomness enters through the determinism seam). An
    /// expired key at the picked position is skipped (the implementation probes onward
    /// deterministically from `pick`).
    fn random_key(&mut self, db: u32, pick: u64, now: UnixMillis) -> Option<Box<[u8]>>;

    /// Remove every key in `db` (FLUSHDB), firing the remove hooks / accounting for
    /// each. Returns the number of entries removed.
    fn flush_db(&mut self, db: u32) -> u64;

    /// Remove every key in EVERY db (FLUSHALL), firing the remove hooks / accounting.
    /// Returns the total number of entries removed.
    fn flush_all(&mut self) -> u64;

    /// Move or copy the value object at `(src_db, src)` to `(dst_db, dst)`,
    /// PRESERVING the value object intact (encoding + remaining TTL), for
    /// RENAME/RENAMENX/COPY/MOVE (KEYSPACE.md "moves the value object INTACT"). `mode`
    /// selects move-vs-copy; `replace` permits overwriting a live destination. Returns
    /// the [`MoveOutcome`]. A lazily-expired source reads as [`MoveOutcome::NoSource`].
    /// SAME-SHARD only (no cross-shard routing exists; a cross-shard form goes through
    /// the coordinator later, KEYSPACE.md).
    #[allow(clippy::too_many_arguments)]
    fn move_object(
        &mut self,
        src_db: u32,
        src: &[u8],
        dst_db: u32,
        dst: &[u8],
        mode: MoveMode,
        replace: bool,
        now: UnixMillis,
    ) -> MoveOutcome;

    /// Swap the entire contents of two logical databases (SWAPDB), an O(1) operation
    /// (the per-DB maps are Vec elements; the swap exchanges them without touching any
    /// entry). No hooks fire (no entry is created or destroyed; the keys simply belong
    /// to a different db id afterward).
    fn swap_db(&mut self, a: u32, b: u32);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_and_encoding_names_match_oracle() {
        assert_eq!(DataType::String.type_name(), "string");
        assert_eq!(DataType::List.type_name(), "list");
        assert_eq!(DataType::Set.type_name(), "set");
        assert_eq!(DataType::Hash.type_name(), "hash");
        assert_eq!(DataType::ZSet.type_name(), "zset");
        assert_eq!(DataType::Stream.type_name(), "stream");
        assert_eq!(Encoding::Int.encoding_name(), "int");
        assert_eq!(Encoding::EmbStr.encoding_name(), "embstr");
        assert_eq!(Encoding::Raw.encoding_name(), "raw");
    }

    #[test]
    fn value_ref_borrowed_exposes_bytes_and_meta() {
        let v = ValueRef::borrowed(DataType::String, Encoding::Raw, None, b"hello");
        assert_eq!(v.as_bytes(), b"hello");
        assert_eq!(v.len(), 5);
        assert!(!v.is_empty());
        assert_eq!(v.data_type(), DataType::String);
        assert_eq!(v.encoding(), Encoding::Raw);
        assert_eq!(v.expire_at(), None);
    }

    #[test]
    fn value_ref_int_materializes_decimal_bytes_but_reports_int() {
        // The command layer must see decimal bytes while encoding stays int.
        let v = ValueRef::from_int_bytes(
            DataType::String,
            Some(UnixMillis(42)),
            Bytes::from_static(b"-12345"),
        );
        assert_eq!(v.as_bytes(), b"-12345");
        assert_eq!(v.len(), 6); // decimal length includes the sign
        assert_eq!(v.encoding(), Encoding::Int);
        assert_eq!(v.expire_at(), Some(UnixMillis(42)));
    }

    #[test]
    fn counting_accounting_tracks_and_saturates() {
        let mut a = CountingAccounting::new();
        assert_eq!(a.used(), 0);
        a.add(100);
        a.add(50);
        assert_eq!(a.used(), 150);
        a.sub(40);
        assert_eq!(a.used(), 110);
        // Saturating: subtracting past zero stays at zero.
        a.sub(1_000);
        assert_eq!(a.used(), 0);
    }

    #[test]
    fn null_eviction_selects_nothing_and_is_inert() {
        let mut e = NullEviction;
        e.on_access(0, b"k");
        e.on_insert(0, b"k", 10);
        e.on_remove(0, b"k", 10);
        assert_eq!(e.select_victim(), None);
    }

    #[test]
    fn unix_millis_is_ordered() {
        assert!(UnixMillis(10) < UnixMillis(20));
        assert!(UnixMillis(20) <= UnixMillis(20));
    }

    #[test]
    fn scan_cursor_token_round_trips() {
        // The wire form is a decimal string; 0 is the start/complete sentinel.
        assert_eq!(ScanCursor::START.to_token(), "0");
        assert!(ScanCursor::START.is_start());
        for raw in [0u64, 1, 42, 12345, u64::MAX] {
            let c = ScanCursor(raw);
            let token = c.to_token();
            assert_eq!(ScanCursor::from_token(token.as_bytes()), Some(c), "{raw}");
        }
        // A returned cursor of 0 means complete; a non-zero cursor does not.
        assert!(!ScanCursor(7).is_start());
    }

    #[test]
    fn scan_cursor_rejects_malformed_tokens() {
        assert_eq!(ScanCursor::from_token(b""), None);
        assert_eq!(ScanCursor::from_token(b"-1"), None);
        assert_eq!(ScanCursor::from_token(b"abc"), None);
        assert_eq!(ScanCursor::from_token(b"12x"), None);
        // Overflow past u64.
        assert_eq!(ScanCursor::from_token(b"18446744073709551616"), None);
    }
}
