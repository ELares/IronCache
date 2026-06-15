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
//! field, and the APPEND/SETRANGE efficiency path) is an ADDITIVE extension to the
//! RMW surface, LANDED in PR-5 for collections: a new action variant
//! [`RmwAction::Mutated`] (carries no value: the edit already happened on the
//! borrowed handle), a new entry arm [`RmwEntry::OccupiedMut`] carrying a typed
//! mutable view [`OccupiedEntryMut`], a per-type abstract op vocabulary ([`ListValue`]
//! for PR-5; the hash/set/zset traits are added in PR-6/7/8), and a convenience
//! primitive [`Store::rmw_mut`] that hands out the mutable arm. The store measures
//! the byte delta around the closure (it does not trust the handler), recomputes the
//! encoding, and deletes the key if the edit empties the collection. The extension
//! adds capability without churning existing string callers or PR-3's eviction/
//! accounting callers (they bind only `Vacant`/`Occupied` and return the existing
//! value actions), so it does not reopen the FROZEN four primitives.
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
    /// A small collection stored in one contiguous listpack-equivalent pack
    /// (`OBJECT ENCODING` -> listpack). PR-5 produces this for a small LIST.
    ListPack,
    /// A large LIST stored as a quicklist-equivalent chunked deque (`OBJECT
    /// ENCODING` -> quicklist). PR-5 produces this once a list exceeds the
    /// `list-max-listpack-size` byte budget or the per-node element cap (#40,
    /// LIST_LARGE.md). The reported NAME is a pure function of the active repr.
    QuickList,
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
            Encoding::QuickList => "quicklist",
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
    /// A new LIST value (PR-5 create-on-missing path: LPUSH/RPUSH/LMOVE-push on a
    /// vacant key). The elements are in head-to-tail order; the store builds the
    /// concrete list value from them. The collection is built from a whole owned
    /// value here (the create case); SUBSEQUENT edits go through the in-place
    /// [`RmwAction::Mutated`] path, not a rebuild.
    List(Vec<Vec<u8>>),
    /// A new HASH value (PR-6 create-on-missing path: HSET/HSETNX/HINCRBY/... on a
    /// vacant key). The `(field, value)` pairs are in insertion order; the store builds
    /// the concrete hash value from them. As with [`NewValueOwned::List`], this is the
    /// create case only; SUBSEQUENT edits go through the in-place [`RmwAction::Mutated`]
    /// path, not a rebuild.
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    /// A new SET value (PR-7 create-on-missing path: SADD/SMOVE-into-dst/*STORE on a
    /// vacant key). The members are in insertion order (deduplicated by the store as it
    /// builds the concrete set value, applying the intset/listpack/hashtable ladder). As
    /// with [`NewValueOwned::List`]/[`NewValueOwned::Hash`], this is the create case only;
    /// SUBSEQUENT edits go through the in-place [`RmwAction::Mutated`] path, not a rebuild.
    Set(Vec<Vec<u8>>),
    /// A new ZSET value (PR-8 create-on-missing path: ZADD/ZINCRBY/*STORE/ZRANGESTORE on a
    /// vacant key). The `(member, score)` pairs are deduplicated by the store as it builds
    /// the concrete zset value (the LAST score for a repeated member wins, since the caller
    /// already resolved aggregation), applying the listpack/skiplist ladder and the
    /// (score, member) ordering. As with the other create variants, this is the create case
    /// only; SUBSEQUENT edits go through the in-place [`RmwAction::Mutated`] path.
    ZSet(Vec<(Vec<u8>, f64)>),
}

impl NewValueOwned {
    /// Convenience constructor for owned bytes from anything byte-like.
    pub fn bytes(b: impl Into<Bytes>) -> Self {
        NewValueOwned::Bytes(b.into())
    }

    /// Convenience constructor for a new LIST value from head-to-tail elements.
    #[must_use]
    pub fn list(elems: Vec<Vec<u8>>) -> Self {
        NewValueOwned::List(elems)
    }

    /// Convenience constructor for a new HASH value from `(field, value)` pairs in
    /// insertion order.
    #[must_use]
    pub fn hash(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        NewValueOwned::Hash(pairs)
    }

    /// Convenience constructor for a new SET value from members in insertion order
    /// (the store deduplicates and applies the encoding ladder as it builds the value).
    #[must_use]
    pub fn set(members: Vec<Vec<u8>>) -> Self {
        NewValueOwned::Set(members)
    }

    /// Convenience constructor for a new ZSET value from `(member, score)` pairs (the
    /// store deduplicates -- last score wins -- and applies the encoding ladder + ordering
    /// as it builds the value).
    #[must_use]
    pub fn zset(pairs: Vec<(Vec<u8>, f64)>) -> Self {
        NewValueOwned::ZSet(pairs)
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
/// ([`RmwEntry::Vacant`]), present and observed READ-ONLY ([`RmwEntry::Occupied`]),
/// or present and held with a TYPED MUTABLE view ([`RmwEntry::OccupiedMut`], the
/// PR-5 collection in-place-mutation arm). A lazily-expired key is presented as
/// `Vacant` (the backstop ran before the closure).
///
/// ## The OccupiedMut arm (PR-5, the in-place-mutation extension)
///
/// `OccupiedMut` is ADDITIVE: the existing string command handlers bind only
/// `Vacant`/`Occupied` and are unaffected. A handler that wants to edit a stored
/// COLLECTION in place (LPUSH appending one element, LSET rewriting one) asks the
/// store for the mutable arm by requesting it via [`Store::rmw_mut`] (a thin
/// convenience over `rmw` that hands out `OccupiedMut` instead of `Occupied`), then
/// edits through a typed view ([`OccupiedEntryMut::as_list_mut`]) and returns
/// [`RmwAction::Mutated`] to signal "the edit already happened on the borrowed
/// handle". The STORE measures the accounting delta around the closure (it does NOT
/// trust the handler), re-fires the eviction sizing hooks, recomputes the encoding
/// from the post-edit representation, and removes the key if the edit emptied the
/// collection. This mechanism is the FROZEN surface all four collection types build
/// on (lists in PR-5; hashes/sets/zsets in PR-6/7/8 add `as_hash_mut`/`as_set_mut`/
/// `as_zset_mut` to [`OccupiedEntryMut`] additively).
pub enum RmwEntry<'a> {
    /// No live value for the key.
    Vacant,
    /// A live value; observe it READ-ONLY through [`OccupiedEntry`] before deciding
    /// the write (the string-command path: the write is a whole owned value).
    Occupied(OccupiedEntry<'a>),
    /// A live value held with a TYPED MUTABLE view ([`OccupiedEntryMut`]) for a
    /// value-internal in-place edit (the collection path, PR-5). The handler edits
    /// through the typed accessor and returns [`RmwAction::Mutated`]; the store
    /// measures the byte delta. Produced only by [`Store::rmw_mut`].
    OccupiedMut(OccupiedEntryMut<'a>),
}

impl core::fmt::Debug for RmwEntry<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RmwEntry::Vacant => f.write_str("Vacant"),
            RmwEntry::Occupied(o) => f.debug_tuple("Occupied").field(o).finish(),
            // The mutable view carries a `&mut dyn` collection handle (not Debug), so
            // print only its read-side metadata, not the value contents.
            RmwEntry::OccupiedMut(o) => f
                .debug_struct("OccupiedMut")
                .field("data_type", &o.data_type())
                .field("encoding", &o.encoding())
                .finish(),
        }
    }
}

// ---------------------------------------------------------------------------
// The value-internal in-place-mutation surface (PR-5 collection extension,
// COLLECTIONS.md / STORAGE_API.md "the RMW in-place-mutation contract").
//
// `OccupiedEntryMut` is the MUTABLE analog of `OccupiedEntry`: it exposes the same
// read accessors PLUS a typed mutable view of the stored collection. The `*Value`
// traits (`ListValue` here; `HashValue`/`SetValue`/`ZSetValue` in PR-6/7/8) are the
// ABSTRACT collection-op vocabulary the command layer calls -- the value-layer
// analog of the `Keyspace` side-trait. The per-command `dyn` indirection is off the
// string hot path and is an accepted cost; a #8 perf follow-up may monomorphize it.
// ---------------------------------------------------------------------------

/// The abstract LIST mutation + read vocabulary the command layer calls through the
/// in-place-mutation arm (COLLECTIONS.md, COMMANDS.md list semantics). The concrete
/// list value (`ironcache_store::kvobj::ListVal`) implements it; the command layer
/// names ONLY this trait, never the concrete list representation, so the listpack ->
/// quicklist transition can change without reopening the command layer.
///
/// The method set is designed to cover ALL the list commands: LPUSH/RPUSH
/// ([`push_front`](ListValue::push_front)/[`push_back`](ListValue::push_back)),
/// LPOP/RPOP ([`pop_front`](ListValue::pop_front)/[`pop_back`](ListValue::pop_back)),
/// LLEN ([`len`](ListValue::len)), LINDEX ([`get`](ListValue::get)), LSET
/// ([`set`](ListValue::set)), LINSERT
/// ([`insert_before`](ListValue::insert_before)/[`insert_after`](ListValue::insert_after)),
/// LREM ([`remove_matching`](ListValue::remove_matching)), LTRIM
/// ([`trim`](ListValue::trim)), LRANGE ([`range`](ListValue::range)), LPOS
/// ([`pos`](ListValue::pos)). LMOVE/RPOPLPUSH compose pop + push.
///
/// Indices follow Redis: a non-negative index counts from the head (0-based); a
/// negative index counts from the tail (`-1` is the last element). Out-of-range
/// indices read as absent (the command layer maps that to nil / the index-out-of
/// -range error as the command dictates).
pub trait ListValue {
    /// Prepend one element (LPUSH). After this the new element is at index 0.
    fn push_front(&mut self, elem: &[u8]);

    /// Append one element (RPUSH). After this the new element is the last.
    fn push_back(&mut self, elem: &[u8]);

    /// Remove and return the head element (LPOP), or `None` if the list is empty.
    fn pop_front(&mut self) -> Option<Vec<u8>>;

    /// Remove and return the tail element (RPOP), or `None` if the list is empty.
    fn pop_back(&mut self) -> Option<Vec<u8>>;

    /// The element count (LLEN).
    fn len(&self) -> usize;

    /// Whether the list holds no elements. (A list value should never be stored
    /// empty -- the store removes the key when an edit empties it -- but the
    /// predicate is part of the vocabulary so the store can detect the empty case.)
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The element at signed Redis `index` (LINDEX), or `None` if out of range. A
    /// negative index counts from the tail (`-1` is the last element).
    fn get(&self, index: i64) -> Option<Vec<u8>>;

    /// Overwrite the element at signed Redis `index` with `elem` (LSET). Returns
    /// `true` on success, `false` if the index is out of range (the command layer
    /// maps `false` to the index-out-of-range error).
    fn set(&mut self, index: i64, elem: &[u8]) -> bool;

    /// Insert `elem` immediately BEFORE the first element equal to `pivot` (LINSERT
    /// BEFORE). Returns the new length, or `None` if `pivot` is not present (the
    /// command layer maps `None` to the `-1` reply).
    fn insert_before(&mut self, pivot: &[u8], elem: &[u8]) -> Option<usize>;

    /// Insert `elem` immediately AFTER the first element equal to `pivot` (LINSERT
    /// AFTER). Returns the new length, or `None` if `pivot` is not present.
    fn insert_after(&mut self, pivot: &[u8], elem: &[u8]) -> Option<usize>;

    /// Remove up to `count` elements equal to `elem` (LREM). `count > 0` removes
    /// from head to tail, `count < 0` from tail to head, `count == 0` removes all
    /// matches. Returns the number removed.
    fn remove_matching(&mut self, count: i64, elem: &[u8]) -> usize;

    /// Trim the list to the inclusive signed range `[start, stop]` (LTRIM). Indices
    /// are normalized Redis-identically (negative from the tail, clamped to bounds);
    /// an empty resulting range removes every element (the store then deletes the
    /// key).
    fn trim(&mut self, start: i64, stop: i64);

    /// The elements in the inclusive signed range `[start, stop]` (LRANGE), in head
    /// -to-tail order. Out-of-range / inverted ranges yield an empty vector.
    fn range(&self, start: i64, stop: i64) -> Vec<Vec<u8>>;

    /// Find positions of `elem` (LPOS). `rank` selects which match to start from
    /// (1-based; a negative rank scans from the tail); `count` bounds how many
    /// matches to return (`Some(0)` means all matches, `None` means just the first);
    /// `maxlen` bounds how many elements to compare (`0` means no limit). Returns the
    /// matched 0-based indices in scan order.
    fn pos(&self, elem: &[u8], rank: i64, count: Option<usize>, maxlen: usize) -> Vec<usize>;
}

/// The abstract HASH mutation + read vocabulary the command layer calls through the
/// in-place-mutation arm (PR-6, COLLECTIONS.md, COMMANDS.md hash semantics). The
/// concrete hash value (`ironcache_store::kvobj::HashVal`) implements it; the command
/// layer names ONLY this trait, never the concrete hash representation, so the listpack
/// -> hashtable transition can change without reopening the command layer. This is the
/// HASH analog of [`ListValue`], added ADDITIVELY in PR-6 (same shape, same `dyn`
/// indirection off the string hot path).
///
/// The method set is designed to cover ALL the hash commands: HSET
/// ([`set`](HashValue::set)), HSETNX ([`set_nx`](HashValue::set_nx)), HGET
/// ([`get`](HashValue::get)), HMGET (repeated [`get`](HashValue::get)), HDEL
/// ([`del`](HashValue::del)), HGETALL/HKEYS/HVALS ([`iter`](HashValue::iter) /
/// [`fields`](HashValue::fields) / [`values`](HashValue::values)), HLEN
/// ([`len`](HashValue::len)), HEXISTS ([`contains`](HashValue::contains)), HSTRLEN
/// ([`strlen`](HashValue::strlen)), HINCRBY/HINCRBYFLOAT ([`get`](HashValue::get) +
/// [`set`](HashValue::set) compose the read-modify-write), HRANDFIELD + HSCAN (over the
/// field order [`iter`](HashValue::iter) / [`fields`](HashValue::fields) exposes).
///
/// A hash value is NEVER stored empty: when the last field is removed the store deletes
/// the key (the empty-collection-deletes-key backstop), so an empty hash is never
/// observable, matching Redis.
pub trait HashValue {
    /// Set `field` to `value` (HSET). Returns `true` if the field was NEW (the hash
    /// grew), `false` if an existing field's value was overwritten in place.
    fn set(&mut self, field: &[u8], value: &[u8]) -> bool;

    /// Set `field` to `value` ONLY if the field does not already exist (HSETNX).
    /// Returns `true` if the field was set (was absent), `false` if it already existed
    /// (no change).
    fn set_nx(&mut self, field: &[u8], value: &[u8]) -> bool;

    /// The value of `field` (HGET / HMGET), or `None` if the field is absent.
    fn get(&self, field: &[u8]) -> Option<&[u8]>;

    /// Remove `field` (HDEL). Returns `true` if it existed and was removed.
    fn del(&mut self, field: &[u8]) -> bool;

    /// Whether `field` is present (HEXISTS).
    fn contains(&self, field: &[u8]) -> bool;

    /// The field count (HLEN).
    fn len(&self) -> usize;

    /// Whether the hash holds no fields. (A hash value should never be stored empty --
    /// the store removes the key when an edit empties it -- but the predicate is part of
    /// the vocabulary so the store can detect the empty case.)
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The byte length of `field`'s value (HSTRLEN), or `0` if the field is absent
    /// (Redis HSTRLEN of a missing field is 0).
    fn strlen(&self, field: &[u8]) -> usize {
        self.get(field).map_or(0, <[u8]>::len)
    }

    /// All fields (HKEYS), in the hash's iteration order. The order matches
    /// [`pairs`](HashValue::pairs) and is what HSCAN/HRANDFIELD index into.
    fn fields(&self) -> Vec<Vec<u8>>;

    /// All values (HVALS), in the hash's iteration order (paired 1:1 with
    /// [`fields`](HashValue::fields)).
    fn values(&self) -> Vec<Vec<u8>>;

    /// All `(field, value)` pairs (HGETALL / HSCAN / HRANDFIELD), in the hash's
    /// iteration order. The order is STABLE for a given representation (the listpack
    /// small form preserves insertion order; the hashtable form is sorted by the
    /// fixed-seed stable field hash so the order is deterministic across a resize, the
    /// same resize-invariant order SCAN uses, ADR-0003). The store and command layer
    /// rely on this stability for deterministic HSCAN/HRANDFIELD. (Named `pairs` rather
    /// than `iter` because it returns an OWNED snapshot Vec, not an `Iterator`.)
    fn pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Whether the hash is in the small `listpack` encoding (vs the large `hashtable`
    /// encoding). HSCAN uses this to match Redis's small-collection behavior: a
    /// listpack-encoded hash is returned in ONE reply with cursor 0 (COUNT ignored),
    /// while a hashtable-encoded hash paginates by COUNT (KEYSPACE.md + Redis
    /// small-collection SCAN). This is the encoding name [`encoding`] would report,
    /// surfaced as a cheap accessor so the command layer need not parse the encoding
    /// string. Default `true` is the conservative small-collection answer (return all at
    /// once) for any implementor that has not specialized it.
    ///
    /// [`encoding`]: crate::Encoding
    fn is_listpack(&self) -> bool {
        true
    }
}

/// The abstract SET mutation + read vocabulary the command layer calls through the
/// in-place-mutation arm (PR-7, COLLECTIONS.md, COMMANDS.md set semantics). The
/// concrete set value (`ironcache_store::kvobj::SetVal`) implements it; the command
/// layer names ONLY this trait, never the concrete set representation, so the
/// intset -> listpack -> hashtable ladder can change without reopening the command
/// layer. This is the SET analog of [`ListValue`]/[`HashValue`], added ADDITIVELY in
/// PR-7 (same shape, same `dyn` indirection off the string hot path).
///
/// The method set is designed to cover ALL the set commands: SADD
/// ([`add`](SetValue::add)), SREM ([`remove`](SetValue::remove)), SISMEMBER/SMISMEMBER
/// ([`contains`](SetValue::contains)), SCARD ([`len`](SetValue::len)), SMEMBERS /
/// SINTER / SUNION / SDIFF / SSCAN ([`members`](SetValue::members) snapshot), SPOP /
/// SRANDMEMBER (the caller indexes [`members`](SetValue::members) with Env-drawn
/// indices, then SPOP calls [`remove`](SetValue::remove) on the chosen members), and
/// SSCAN small-collection behavior ([`is_listpack`](SetValue::is_listpack)).
///
/// A set value is NEVER stored empty: when the last member is removed the store deletes
/// the key (the empty-collection-deletes-key backstop), so an empty set is never
/// observable, matching Redis.
pub trait SetValue {
    /// Add `member` (SADD). Returns `true` if the member was NEW (the set grew),
    /// `false` if it was already present (no change).
    fn add(&mut self, member: &[u8]) -> bool;

    /// Remove `member` (SREM). Returns `true` if it existed and was removed.
    fn remove(&mut self, member: &[u8]) -> bool;

    /// Whether `member` is present (SISMEMBER / SMISMEMBER).
    fn contains(&self, member: &[u8]) -> bool;

    /// The member count (SCARD).
    fn len(&self) -> usize;

    /// Whether the set holds no members. (A set value should never be stored empty --
    /// the store removes the key when an edit empties it -- but the predicate is part
    /// of the vocabulary so the store can detect the empty case.)
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// All members (SMEMBERS / SSCAN / SINTER / SUNION / SDIFF / SPOP / SRANDMEMBER), in
    /// the set's iteration order. The order is STABLE for a given representation (the
    /// intset form is ascending-integer order; the listpack form is insertion order; the
    /// hashtable form is sorted by the fixed-seed stable member hash so the order is
    /// deterministic across a resize, the same resize-invariant order SCAN uses,
    /// ADR-0003). The store and command layer rely on this stability for deterministic
    /// SPOP/SRANDMEMBER/SSCAN. (Named `members` rather than `iter` because it returns an
    /// OWNED snapshot Vec, not an `Iterator`.)
    fn members(&self) -> Vec<Vec<u8>>;

    /// Whether the set is in a SMALL encoding (`intset` or `listpack`) vs the large
    /// `hashtable` encoding. SSCAN uses this to match Redis's small-collection behavior:
    /// a small (intset/listpack) set is returned in ONE reply with cursor 0 (COUNT
    /// ignored), while a hashtable-encoded set paginates by COUNT (KEYSPACE.md + Redis
    /// small-collection SCAN). Default `true` is the conservative small-collection answer
    /// (return all at once) for any implementor that has not specialized it.
    fn is_listpack(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// ZSet (sorted-set) range/decision vocabulary (PR-8, ZSET_LARGE.md, COMMANDS.md zset
// semantics). The waist OWNS these small value types so the command layer can express
// a score range, a lex range, and the ZADD NX/XX/GT/LT decision WITHOUT naming the
// concrete zset representation (the layering contract). They are pure data (no store
// types), added ADDITIVELY alongside the [`ZSetValue`] trait.
// ---------------------------------------------------------------------------

/// One end of a SCORE range (ZRANGEBYSCORE / ZCOUNT / ZRANGE BYSCORE). The score is an
/// `f64`; `inclusive` distinguishes the Redis `[`/inclusive vs `(`/exclusive bound. The
/// command layer parses `-inf`/`+inf`/`(1.5`/`1.5` into this; `+inf`/`-inf` are just
/// `f64::INFINITY`/`f64::NEG_INFINITY` (always inclusive in Redis since nothing equals
/// infinity except infinity itself, which the inclusive flag handles correctly).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreBound {
    /// The score value (may be `+inf`/`-inf`).
    pub score: f64,
    /// Whether the bound is inclusive (`[`/bare) vs exclusive (`(`).
    pub inclusive: bool,
}

impl ScoreBound {
    /// An inclusive bound at `score`.
    #[must_use]
    pub fn inclusive(score: f64) -> Self {
        ScoreBound {
            score,
            inclusive: true,
        }
    }

    /// An exclusive bound at `score`.
    #[must_use]
    pub fn exclusive(score: f64) -> Self {
        ScoreBound {
            score,
            inclusive: false,
        }
    }

    /// Whether `s` satisfies this bound as a MINIMUM (lower) bound (`s > score` for an
    /// exclusive bound, `s >= score` for an inclusive one).
    #[must_use]
    pub fn allows_min(&self, s: f64) -> bool {
        if self.inclusive {
            s >= self.score
        } else {
            s > self.score
        }
    }

    /// Whether `s` satisfies this bound as a MAXIMUM (upper) bound (`s < score` for an
    /// exclusive bound, `s <= score` for an inclusive one).
    #[must_use]
    pub fn allows_max(&self, s: f64) -> bool {
        if self.inclusive {
            s <= self.score
        } else {
            s < self.score
        }
    }
}

/// One end of a LEX range (ZRANGEBYLEX / ZLEXCOUNT / ZRANGE BYLEX). Redis lex ranges
/// assume all members share a score and compare member BYTES: `[m` inclusive, `(m`
/// exclusive, `-` the minimum (before all members), `+` the maximum (after all members).
#[derive(Debug, Clone, PartialEq)]
pub enum LexBound {
    /// `-` for a min bound / `+`-equivalent unreachable-low: smaller than every member.
    NegInf,
    /// `+` for a max bound: larger than every member.
    PosInf,
    /// `[m` inclusive at the member bytes.
    Inclusive(Vec<u8>),
    /// `(m` exclusive at the member bytes.
    Exclusive(Vec<u8>),
}

impl LexBound {
    /// Whether `m` satisfies this bound as a MINIMUM (lower) bound.
    #[must_use]
    pub fn allows_min(&self, m: &[u8]) -> bool {
        match self {
            LexBound::NegInf => true,
            LexBound::PosInf => false,
            LexBound::Inclusive(b) => m >= b.as_slice(),
            LexBound::Exclusive(b) => m > b.as_slice(),
        }
    }

    /// Whether `m` satisfies this bound as a MAXIMUM (upper) bound.
    #[must_use]
    pub fn allows_max(&self, m: &[u8]) -> bool {
        match self {
            LexBound::NegInf => false,
            LexBound::PosInf => true,
            LexBound::Inclusive(b) => m <= b.as_slice(),
            LexBound::Exclusive(b) => m < b.as_slice(),
        }
    }
}

/// The ZADD per-member flag decision the command layer passes to [`ZSetValue::add`]
/// (the NX/XX/GT/LT matrix). Pure data; the concrete zset applies it atomically.
/// NX/XX/GT/LT are validated for compatibility at the command layer BEFORE this is
/// built (NX+GT/NX+LT/GT+LT/NX+XX are syntax errors), so a `ZAddFlags` is always a
/// legal combination here.
// The four flags mirror Redis's ZADD NX/XX/GT/LT option bits one-for-one; they are
// independent boolean toggles (not a state enum), and the command layer validates the
// illegal combinations before constructing this. A bitflags type would obscure the
// direct correspondence to the Redis option tokens, so the four-bool struct is the
// clearest faithful shape here.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ZAddFlags {
    /// NX: only add a NEW member; never update an existing one.
    pub nx: bool,
    /// XX: only update an EXISTING member; never add a new one.
    pub xx: bool,
    /// GT: only update if the new score is strictly GREATER than the current.
    pub gt: bool,
    /// LT: only update if the new score is strictly LESS than the current.
    pub lt: bool,
}

/// The outcome of a single [`ZSetValue::add`] (one ZADD score/member pair under the
/// flag matrix): whether the member was newly ADDED, whether its score CHANGED
/// (added counts as changed), and the member's score AFTER the operation (`None` if
/// the op was suppressed by NX/XX/GT/LT and the member is absent, so INCR returns nil).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZAddOutcome {
    /// Whether a NEW member was added (the zset grew).
    pub added: bool,
    /// Whether the member's score was added-or-updated (ZADD CH counts this).
    pub changed: bool,
    /// The member's score AFTER the op, or `None` if the op did not apply and the
    /// member is absent (NX-on-existing / XX-on-missing / GT/LT suppressed). For INCR
    /// a `None` means the reply is nil.
    pub new_score: Option<f64>,
}

/// The outcome of a single ZINCRBY / ZADD INCR (one increment against one member).
/// Distinguishes the three cases the command layer must reply to differently: the
/// increment APPLIED (the new score), the increment was SUPPRESSED by a NX/XX/GT/LT flag
/// (the reply is nil), or the resulting score is NaN (an existing `+inf` incremented by
/// `-inf`, or vice versa) which Redis reports as an error WITHOUT mutating the member.
///
/// The store NEVER stores a NaN: on [`Self::Nan`] the member is left UNCHANGED so the
/// command layer can return `-ERR resulting score is not a number (NaN)` over a value the
/// caller can still observe at its prior score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IncrOutcome {
    /// The increment applied; the value is the member's new (finite or infinite) score.
    Updated(f64),
    /// A NX/XX/GT/LT flag blocked the increment; the member is unchanged. INCR replies nil.
    Suppressed,
    /// The resulting score would be NaN (`+inf + -inf`); the member is left UNCHANGED and
    /// the command layer returns the resulting-score-is-NaN error (no mutation).
    Nan,
}

/// The abstract SORTED-SET (zset) mutation + read vocabulary the command layer calls
/// through the in-place-mutation arm (PR-8, ZSET_LARGE.md, COMMANDS.md zset semantics).
/// The concrete zset value (`ironcache_store::kvobj::ZSetVal`) implements it; the command
/// layer names ONLY this trait, never the concrete zset representation, so the listpack
/// -> skiplist transition can change without reopening the command layer. This is the
/// ZSET analog of [`ListValue`]/[`HashValue`]/[`SetValue`], added ADDITIVELY in PR-8.
///
/// A zset member is UNIQUE; each carries an `f64` score; the zset is ordered by
/// `(score ASC, member-bytes ASC)` for equal scores (the Redis skiplist order,
/// [redis-zset-skiplist-plus-ht]). A NaN INPUT score is rejected at parse time before
/// reaching [`Self::add`]/[`Self::incr`]; a NaN ARITHMETIC RESULT inside [`Self::incr`]
/// (an existing `+inf` incremented by `-inf`) is signalled as [`IncrOutcome::Nan`] WITHOUT
/// mutating, so a NaN never enters the order. `+inf`/`-inf` are allowed and ordered as the
/// extreme scores.
///
/// A zset value is NEVER stored empty: when the last member is removed the store deletes
/// the key (the empty-collection-deletes-key backstop), so an empty zset is never
/// observable, matching Redis.
pub trait ZSetValue {
    /// ZADD one `(member, score)` pair under the NX/XX/GT/LT `flags`, returning the
    /// [`ZAddOutcome`] (added? / changed? / new score). The implementation enforces the
    /// flag matrix atomically: NX suppresses an update of an existing member; XX
    /// suppresses adding a new member; GT/LT suppress an update unless the new score is
    /// strictly greater/less than the current. The member ordering is maintained.
    fn add(&mut self, member: &[u8], score: f64, flags: ZAddFlags) -> ZAddOutcome;

    /// ZINCRBY / ZADD INCR: add `delta` to `member`'s score (creating it at `delta` if
    /// absent, UNLESS suppressed by `flags`), returning an [`IncrOutcome`]:
    /// [`IncrOutcome::Updated`] with the new score on success, [`IncrOutcome::Suppressed`]
    /// when a NX/XX/GT/LT flag blocks it (INCR replies nil), or [`IncrOutcome::Nan`] when
    /// the resulting score is NaN (`+inf + -inf`). The store NEVER stores a NaN: on the Nan
    /// outcome the member is left UNCHANGED so the command layer can return the
    /// resulting-score-is-NaN error over an unmutated value. The member ordering is
    /// maintained.
    fn incr(&mut self, member: &[u8], delta: f64, flags: ZAddFlags) -> IncrOutcome;

    /// The score of `member` (ZSCORE / ZMSCORE), or `None` if absent.
    fn score(&self, member: &[u8]) -> Option<f64>;

    /// Remove `member` (ZREM). Returns `true` if it existed and was removed.
    fn remove(&mut self, member: &[u8]) -> bool;

    /// The member count (ZCARD).
    fn len(&self) -> usize;

    /// Whether the zset holds no members. (A zset value should never be stored empty --
    /// the store removes the key when an edit empties it -- but the predicate is part of
    /// the vocabulary so the store can detect the empty case.)
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The 0-based RANK of `member` in `(score, member)` order (ZRANK), or in REVERSE
    /// order when `rev` (ZREVRANK). `None` if the member is absent.
    fn rank(&self, member: &[u8], rev: bool) -> Option<usize>;

    /// The `(member, score)` pairs in the inclusive signed RANK range `[start, stop]`
    /// (ZRANGE by index / ZREVRANGE), in `(score, member)` order, or reversed when
    /// `rev`. Negative indices count from the tail; out-of-range / inverted ranges
    /// yield an empty vector. Redis normalization (the same as LRANGE).
    fn range_by_rank(&self, start: i64, stop: i64, rev: bool) -> Vec<(Vec<u8>, f64)>;

    /// The `(member, score)` pairs whose score is within `[min, max]` (ZRANGEBYSCORE /
    /// ZRANGE BYSCORE), in ascending `(score, member)` order, or DESCENDING when `rev`
    /// (ZREVRANGEBYSCORE: in that case `min`/`max` are still the SMALLER/LARGER bound,
    /// the command layer swaps the argument order before calling). `limit` is an
    /// optional `(offset, count)` applied AFTER ordering (count < 0 means "to the end").
    fn range_by_score(
        &self,
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Vec<(Vec<u8>, f64)>;

    /// The members whose bytes are within the lex range `[min, max]` (ZRANGEBYLEX /
    /// ZRANGE BYLEX), in ascending member order or DESCENDING when `rev`. Defined for an
    /// equal-score zset (Redis semantics); the implementation compares member bytes.
    /// `limit` is an optional `(offset, count)` applied after ordering.
    fn range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Vec<Vec<u8>>;

    /// The count of members whose score is within `[min, max]` (ZCOUNT).
    fn count_by_score(&self, min: ScoreBound, max: ScoreBound) -> usize;

    /// The count of members whose bytes are within the lex range `[min, max]`
    /// (ZLEXCOUNT).
    fn count_by_lex(&self, min: &LexBound, max: &LexBound) -> usize;

    /// Remove and return up to `count` members with the LOWEST scores (ZPOPMIN), in
    /// ascending `(score, member)` order. Returns the removed `(member, score)` pairs.
    fn pop_min(&mut self, count: usize) -> Vec<(Vec<u8>, f64)>;

    /// Remove and return up to `count` members with the HIGHEST scores (ZPOPMAX), in
    /// DESCENDING `(score, member)` order (the highest first). Returns the removed
    /// `(member, score)` pairs.
    fn pop_max(&mut self, count: usize) -> Vec<(Vec<u8>, f64)>;

    /// Remove every member whose rank is within the inclusive signed range `[start,
    /// stop]` (ZREMRANGEBYRANK). Returns the number removed.
    fn remove_range_by_rank(&mut self, start: i64, stop: i64) -> usize;

    /// Remove every member whose score is within `[min, max]` (ZREMRANGEBYSCORE).
    /// Returns the number removed.
    fn remove_range_by_score(&mut self, min: ScoreBound, max: ScoreBound) -> usize;

    /// Remove every member whose bytes are within the lex range `[min, max]`
    /// (ZREMRANGEBYLEX). Returns the number removed.
    fn remove_range_by_lex(&mut self, min: &LexBound, max: &LexBound) -> usize;

    /// All `(member, score)` pairs in `(score, member)` order (ZSCAN / aggregation
    /// source read / ZRANDMEMBER). A deterministic, stable order (the score order, with
    /// member-byte tiebreak), so ZSCAN / ZRANDMEMBER are deterministic (ADR-0003).
    fn members_with_scores(&self) -> Vec<(Vec<u8>, f64)>;

    /// Whether the zset is in the small `listpack` encoding (vs the large `skiplist`
    /// encoding). ZSCAN uses this to match Redis's small-collection behavior: a
    /// listpack-encoded zset is returned in ONE reply with cursor 0 (COUNT ignored),
    /// while a skiplist-encoded zset paginates by COUNT. Default `true` is the
    /// conservative small-collection answer.
    fn is_listpack(&self) -> bool {
        true
    }
}

/// A MUTABLE observation of an occupied entry inside a [`Store::rmw_mut`] closure
/// (the collection in-place-mutation arm, PR-5). It exposes the same read accessors
/// as [`OccupiedEntry`] PLUS typed mutable views of the stored collection.
///
/// A handler edits through the typed accessor (e.g. [`Self::as_list_mut`]) and
/// returns [`RmwAction::Mutated`]; the store measures the byte delta around the
/// closure, recomputes the encoding, and deletes the key if the edit emptied the
/// collection (the empty-collection-deletes-key backstop). A type mismatch
/// ([`Self::as_list_mut`] on a non-list) returns `None`, and the handler returns
/// WRONGTYPE + [`RmwAction::Keep`] with no edit.
///
/// The `as_zset_mut` accessor is RESERVED for PR-8: it is added additively (alongside
/// the `ZSetValue` trait, which is NOT defined yet) without changing this struct's
/// existing surface. PR-6 added the `as_hash_mut` accessor (with the [`ValueMut::Hash`]
/// arm and the [`HashValue`] trait) that way; PR-7 adds [`Self::as_set_mut`] (with the
/// [`ValueMut::Set`] arm and the [`SetValue`] trait) the same way.
pub struct OccupiedEntryMut<'a> {
    data_type: DataType,
    encoding: Encoding,
    expire_at: Option<UnixMillis>,
    /// The typed mutable view of the stored value. PR-5 carries only the list arm;
    /// the other collection arms are added in their PRs.
    value: ValueMut<'a>,
}

/// The typed mutable value view behind an [`OccupiedEntryMut`]. PR-5 has the list
/// arm and a `NonCollection` arm (a string/int/embstr/raw value, for which the
/// typed collection accessors all return `None` -> WRONGTYPE). PR-6 adds the [`Hash`]
/// arm; PR-7 adds the [`Set`] arm; the zset arm is added additively in PR-8.
///
/// [`Hash`]: ValueMut::Hash
/// [`Set`]: ValueMut::Set
pub enum ValueMut<'a> {
    /// A non-collection value (string family). No typed collection view applies; the
    /// `as_*_mut` accessors all return `None` so the handler returns WRONGTYPE.
    NonCollection,
    /// A list value, borrowed mutably for the closure (LPUSH/LPOP/LSET/... edits).
    List(&'a mut dyn ListValue),
    /// A hash value, borrowed mutably for the closure (HSET/HDEL/HINCRBY/... edits,
    /// PR-6). The HASH analog of the [`ValueMut::List`] arm.
    Hash(&'a mut dyn HashValue),
    /// A set value, borrowed mutably for the closure (SADD/SREM/SPOP/... edits, PR-7).
    /// The SET analog of the [`ValueMut::List`]/[`ValueMut::Hash`] arms.
    Set(&'a mut dyn SetValue),
    /// A sorted-set (zset) value, borrowed mutably for the closure (ZADD/ZREM/ZPOPMIN/...
    /// edits, PR-8). The ZSET analog of the other collection arms.
    ZSet(&'a mut dyn ZSetValue),
}

impl<'a> OccupiedEntryMut<'a> {
    /// Construct a mutable view over a LIST value (the store hands this out when the
    /// stored value is a list).
    #[must_use]
    pub fn list(
        encoding: Encoding,
        expire_at: Option<UnixMillis>,
        list: &'a mut dyn ListValue,
    ) -> Self {
        OccupiedEntryMut {
            data_type: DataType::List,
            encoding,
            expire_at,
            value: ValueMut::List(list),
        }
    }

    /// Construct a mutable view over a HASH value (PR-6: the store hands this out when
    /// the stored value is a hash). The HASH analog of [`Self::list`].
    #[must_use]
    pub fn hash(
        encoding: Encoding,
        expire_at: Option<UnixMillis>,
        hash: &'a mut dyn HashValue,
    ) -> Self {
        OccupiedEntryMut {
            data_type: DataType::Hash,
            encoding,
            expire_at,
            value: ValueMut::Hash(hash),
        }
    }

    /// Construct a mutable view over a SET value (PR-7: the store hands this out when
    /// the stored value is a set). The SET analog of [`Self::list`]/[`Self::hash`].
    #[must_use]
    pub fn set(
        encoding: Encoding,
        expire_at: Option<UnixMillis>,
        set: &'a mut dyn SetValue,
    ) -> Self {
        OccupiedEntryMut {
            data_type: DataType::Set,
            encoding,
            expire_at,
            value: ValueMut::Set(set),
        }
    }

    /// Construct a mutable view over a ZSET value (PR-8: the store hands this out when
    /// the stored value is a sorted set). The ZSET analog of [`Self::list`]/[`Self::hash`]/
    /// [`Self::set`].
    #[must_use]
    pub fn zset(
        encoding: Encoding,
        expire_at: Option<UnixMillis>,
        zset: &'a mut dyn ZSetValue,
    ) -> Self {
        OccupiedEntryMut {
            data_type: DataType::ZSet,
            encoding,
            expire_at,
            value: ValueMut::ZSet(zset),
        }
    }

    /// Construct a mutable view over a NON-collection value (string family): the
    /// typed collection accessors return `None`, so a collection handler returns
    /// WRONGTYPE without editing.
    #[must_use]
    pub fn non_collection(
        data_type: DataType,
        encoding: Encoding,
        expire_at: Option<UnixMillis>,
    ) -> Self {
        OccupiedEntryMut {
            data_type,
            encoding,
            expire_at,
            value: ValueMut::NonCollection,
        }
    }

    /// The logical data type (for WRONGTYPE checks inside the closure).
    #[must_use]
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    /// The internal encoding (read off the PRE-edit representation; the store
    /// recomputes the post-edit encoding after the closure returns `Mutated`).
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        self.encoding
    }

    /// The TTL deadline, if any.
    #[must_use]
    pub fn expire_at(&self) -> Option<UnixMillis> {
        self.expire_at
    }

    /// The typed mutable LIST view, or `None` if the stored value is not a list (the
    /// handler returns WRONGTYPE + [`RmwAction::Keep`] on `None`). PR-5's list
    /// commands edit through this.
    pub fn as_list_mut(&mut self) -> Option<&mut dyn ListValue> {
        match &mut self.value {
            ValueMut::List(l) => Some(&mut **l),
            ValueMut::NonCollection | ValueMut::Hash(_) | ValueMut::Set(_) | ValueMut::ZSet(_) => {
                None
            }
        }
    }

    /// The typed mutable HASH view, or `None` if the stored value is not a hash (the
    /// handler returns WRONGTYPE + [`RmwAction::Keep`] on `None`). PR-6's hash commands
    /// edit through this. The HASH analog of [`Self::as_list_mut`].
    pub fn as_hash_mut(&mut self) -> Option<&mut dyn HashValue> {
        match &mut self.value {
            ValueMut::Hash(h) => Some(&mut **h),
            ValueMut::NonCollection | ValueMut::List(_) | ValueMut::Set(_) | ValueMut::ZSet(_) => {
                None
            }
        }
    }

    /// The typed mutable SET view, or `None` if the stored value is not a set (the
    /// handler returns WRONGTYPE + [`RmwAction::Keep`] on `None`). PR-7's set commands
    /// edit through this. The SET analog of [`Self::as_list_mut`]/[`Self::as_hash_mut`].
    pub fn as_set_mut(&mut self) -> Option<&mut dyn SetValue> {
        match &mut self.value {
            ValueMut::Set(s) => Some(&mut **s),
            ValueMut::NonCollection | ValueMut::List(_) | ValueMut::Hash(_) | ValueMut::ZSet(_) => {
                None
            }
        }
    }

    /// The typed mutable ZSET view, or `None` if the stored value is not a sorted set
    /// (the handler returns WRONGTYPE + [`RmwAction::Keep`] on `None`). PR-8's zset
    /// commands edit through this. The ZSET analog of [`Self::as_list_mut`]/
    /// [`Self::as_hash_mut`]/[`Self::as_set_mut`]. This was the RESERVED slot the PR-5
    /// docs named; PR-8 fills it additively alongside the [`ZSetValue`] trait and the
    /// [`ValueMut::ZSet`] arm.
    pub fn as_zset_mut(&mut self) -> Option<&mut dyn ZSetValue> {
        match &mut self.value {
            ValueMut::ZSet(z) => Some(&mut **z),
            ValueMut::NonCollection | ValueMut::List(_) | ValueMut::Hash(_) | ValueMut::Set(_) => {
                None
            }
        }
    }
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
    /// The value was edited IN PLACE on the [`OccupiedEntryMut`] handle (PR-5
    /// collection in-place mutation): the edit already happened, so this variant
    /// carries NO value (it adds no generic parameter and does not touch the
    /// existing variants). The store measures the byte delta around the closure,
    /// re-fires the eviction sizing hooks, recomputes the encoding from the
    /// post-edit representation, and -- if the edit EMPTIED the collection --
    /// removes the key (the empty-collection-deletes-key backstop). Meaningful only
    /// on the [`RmwEntry::OccupiedMut`] arm; on any other arm it is treated as
    /// [`RmwAction::Keep`] (no value was borrowed to edit).
    Mutated,
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

    /// The atomic read-modify-write primitive WITH the collection in-place-mutation
    /// arm (PR-5, COLLECTIONS.md). Identical to [`Store::rmw`] except a live entry is
    /// presented as [`RmwEntry::OccupiedMut`] (a typed MUTABLE view) instead of
    /// [`RmwEntry::Occupied`] (a read-only view), so the closure can edit a stored
    /// collection in place and return [`RmwAction::Mutated`].
    ///
    /// This is NOT one of the frozen four primitives: it is the ADDITIVE in-place
    /// -mutation extension the storage waist module docs reserved. It is a `Store`
    /// trait method (not a separate side-trait) because it shares the exact same
    /// write-funnel + TTL-resolution + hook-firing body as `rmw`; the only difference
    /// is which observation handle the closure sees and the [`RmwAction::Mutated`]
    /// post-processing (measure delta / recompute encoding / empty-deletes-key).
    ///
    /// THE STORE MEASURES THE ACCOUNTING DELTA (it does not trust the handler): it
    /// records `accounted_bytes()` BEFORE handing out the mutable handle and AFTER the
    /// closure returns `Mutated`, then charges the signed difference and re-fires
    /// `on_remove(old)`/`on_insert(new)`. It recomputes the stored `encoding` from the
    /// post-edit representation, and if the edit emptied the collection it removes the
    /// key. A handler that already knows the post-edit count is zero may return
    /// [`RmwAction::Delete`] directly instead; both are supported. A lazily-expired
    /// key is presented as `Vacant` and removed before the closure runs.
    fn rmw_mut<R>(
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
        // PR-5 collection encodings: the two list names.
        assert_eq!(Encoding::ListPack.encoding_name(), "listpack");
        assert_eq!(Encoding::QuickList.encoding_name(), "quicklist");
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
