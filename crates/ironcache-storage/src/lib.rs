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
//! dispatch layer drives by calling [`EvictionHook::select_victim`] (fix: the
//! victim KEY) and then [`Store::delete`] in a loop until under budget, or replying
//! `-OOM` for a `denyoom` write when nothing more can be freed. This is a recorded
//! decision, not a primitive-signature change.

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
/// an insert, `on_remove` on a delete/replace. PR-2a ships [`NullEviction`] (all
/// no-ops); the real S3-FIFO policy and `select_victim` driving are PR-3.
///
/// `db` is the validated logical DB id (the same id SELECT validates, KEYSPACE.md),
/// and `key_hash` is the full 64-bit key hash (the same value SCAN orders on,
/// #129). The on_access/on_insert/on_remove callbacks take the hash because that is
/// all the policy's queues need to rank an entry.
pub trait EvictionHook {
    /// A live key was read or observed in an rmw.
    fn on_access(&mut self, db: u32, key_hash: u64);
    /// A new value was inserted, costing `bytes` logical bytes.
    fn on_insert(&mut self, db: u32, key_hash: u64, bytes: usize);
    /// A value was removed (delete, replace, or expiry), freeing `bytes`.
    fn on_remove(&mut self, db: u32, key_hash: u64, bytes: usize);
    /// Pick a victim KEY to evict when over budget, or `None` if none. The KEY
    /// bytes (not the hash) are returned because the store's table is keyed by the
    /// owned key bytes (`Box<[u8]>`, ADR-0008): a u64 hash cannot drive a delete
    /// from a byte-keyed map, so the policy must surface the key it chose
    /// (EVICTION.md `evict_victim() -> key`). PR-3's S3-FIFO evicts through this
    /// frozen surface.
    fn select_victim(&mut self) -> Option<Box<[u8]>>;
}

/// A no-op eviction hook (PR-2a default). Eviction-on-by-default (ADR-0007) and
/// the S3-FIFO policy land in PR-3; until then nothing is ever selected.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullEviction;

impl EvictionHook for NullEviction {
    #[inline]
    fn on_access(&mut self, _db: u32, _key_hash: u64) {}
    #[inline]
    fn on_insert(&mut self, _db: u32, _key_hash: u64, _bytes: usize) {}
    #[inline]
    fn on_remove(&mut self, _db: u32, _key_hash: u64, _bytes: usize) {}
    #[inline]
    fn select_victim(&mut self) -> Option<Box<[u8]>> {
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
        e.on_access(0, 1);
        e.on_insert(0, 1, 10);
        e.on_remove(0, 1, 10);
        assert_eq!(e.select_victim(), None);
    }

    #[test]
    fn unix_millis_is_ordered() {
        assert!(UnixMillis(10) < UnixMillis(20));
        assert!(UnixMillis(20) <= UnixMillis(20));
    }
}
