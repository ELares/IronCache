// SPDX-License-Identifier: MIT OR Apache-2.0
//! The per-key object (`kvobj`): the entry the per-shard table stores for one key
//! (OBJECT_LAYOUT.md #111, ENCODINGS.md #112).
//!
//! ## One-allocation target vs the safe representation used now
//!
//! OBJECT_LAYOUT.md specifies the eventual `kvobj` as a SINGLE heap allocation
//! holding, in order, a packed header, the key bytes inline, and (for small values)
//! the value inline, with large values pointing out-of-line. That flexible-array
//! -member (FAM) layout requires manual allocation and pointer arithmetic, i.e.
//! `unsafe`. The house style forbids unsafe (`#![forbid(unsafe_code)]`), so PR-2a
//! uses a SAFE multi-field representation here:
//!
//! ```text
//! KvObj { header: Header, key: Box<[u8]>, value: ValueRepr, expire_at: Option<UnixMillis> }
//! ```
//!
//! This is behaviorally identical for everything PR-2a observes (type/encoding/
//! TTL/eviction-rank/snapshot-version are all readable from the one object with no
//! side map, satisfying the OBJECT_LAYOUT.md "single kvobj, no side lookup"
//! acceptance test) but it is two-to-three small allocations rather than one. The
//! true single-allocation FAM packing is the #8/Efficient memory follow-up; it
//! slots in behind this same `KvObj` API without changing the storage waist or the
//! command layer. The folded-header metadata (below) is laid out as the FAM
//! version will pack it, so that follow-up is a representation change only.

use crate::encoding::{Classified, classify};
use crate::scan_hash;
use bytes::Bytes;
use hashbrown::HashMap;
use hashbrown::HashSet;
// The compiled Redis default thresholds are now sourced through `EncodingThresholds` (the live
// snapshot the store reads); the bare config constants are referenced ONLY by the unit tests below
// (to pin the default boundaries), so the import is test-gated to keep the non-test build clean.
#[cfg(test)]
use ironcache_config::{
    DEFAULT_HASH_MAX_LISTPACK_ENTRIES, DEFAULT_HASH_MAX_LISTPACK_VALUE,
    DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES, DEFAULT_SET_MAX_INTSET_ENTRIES,
    DEFAULT_SET_MAX_LISTPACK_ENTRIES, DEFAULT_SET_MAX_LISTPACK_VALUE,
    DEFAULT_ZSET_MAX_LISTPACK_ENTRIES, DEFAULT_ZSET_MAX_LISTPACK_VALUE,
};
use ironcache_storage::{
    DataType, Encoding, EncodingThresholds, HashValue, IncrOutcome, LexBound, ListValue,
    NewValueOwned, ScoreBound, SetValue, UnixMillis, ZAddFlags, ZAddOutcome, ZSetValue,
};
use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::collections::{BTreeSet, VecDeque};
use std::ptr::NonNull;

/// The packed per-key header (OBJECT_LAYOUT.md "packed header and metadata bits").
///
/// In the eventual FAM layout these fields are bit-packed into a few bytes (type +
/// encoding in 4 bits each, a 2-bit eviction rank, a TTL-present flag, a reserved
/// snapshot-version u32). Here they are plain fields with the SAME semantics; the
/// packing is the #8 follow-up. None of the reserved fields are load-bearing in
/// PR-2a (eviction is the no-op hook; the snapshot cut is PR-later), but they are
/// carried so the object is read-complete per the OBJECT_LAYOUT.md acceptance test.
#[derive(Debug, Clone, Copy)]
pub struct Header {
    /// The logical data type (4-bit field in the packed layout).
    pub data_type: DataType,
    /// The value encoding (4-bit field in the packed layout).
    pub encoding: Encoding,
    /// The 2-bit access frequency counter, capped at 3. This is the SINGLE SOURCE OF
    /// TRUTH for eviction frequency (the freq-in-object work that superseded ADR-0008's
    /// policy-owned S3-FIFO queues): the access path WRITES it (the store calls
    /// `bump_freq` inline on every read) and the eviction path READS it (the cache-mode
    /// `evict_to_fit` table-scan evicts the lowest-frequency entries first). It lives in
    /// the live tagged-pointer `Entry` (the Str blob flags byte for thin string objects,
    /// `CollEntry.eviction_rank` for boxed collections), NOT in this owned `Header` view,
    /// which is the read-complete decode helper. Stored as a `u8`, masked to two bits.
    pub eviction_rank: u8,
    /// Whether a TTL deadline is present (the TTL-present flag bit). Mirrors
    /// `expire_at.is_some()`; kept explicit to match the packed-header shape.
    pub ttl_present: bool,
    /// The forkless-snapshot version stamp (#60). Reserved u32 in PR-2a; the
    /// snapshot cut that bumps it is a later PR.
    pub snapshot_version: u32,
}

impl Header {
    /// The maximum S3-FIFO rank (2-bit counter capped at 3, ADR-0008).
    pub const MAX_EVICTION_RANK: u8 = 3;

    /// A header for a freshly written STRING value of the given encoding/ttl.
    #[must_use]
    pub fn new(encoding: Encoding, ttl_present: bool) -> Self {
        Header::with_type(DataType::String, encoding, ttl_present)
    }

    /// A header for a freshly written value of an explicit data type (PR-5: the LIST
    /// path passes [`DataType::List`]; the string path uses [`Header::new`]).
    #[must_use]
    pub fn with_type(data_type: DataType, encoding: Encoding, ttl_present: bool) -> Self {
        Header {
            data_type,
            encoding,
            eviction_rank: 0,
            ttl_present,
            snapshot_version: 0,
        }
    }
}

/// The value representation inside a [`KvObj`] (ENCODINGS.md #112).
#[derive(Debug, Clone)]
pub enum ValueRepr {
    /// An int-encoded value: the raw i64, NO value allocation (the decimal bytes
    /// are materialized on read). `OBJECT ENCODING` -> int.
    Int(i64),
    /// A short string (embstr). `OBJECT ENCODING` -> embstr.
    ///
    /// BOXED (memory Round 2): the bytes live behind a `Box<[u8]>` rather than a
    /// fixed inline buffer, so this variant is one pointer wide and the largest
    /// `ValueRepr` variant shrinks to a `Box<[u8]>`, cutting every per-key `KvObj`
    /// and the hashbrown table slot. The embstr-vs-raw distinction is the SAME (it is
    /// recorded in [`Header::encoding`], NOT by the variant): a value classified as
    /// embstr by [`crate::encoding::EMBSTR_THRESHOLD`] is `Inline`, a longer one is
    /// [`ValueRepr::Raw`],
    /// and `OBJECT ENCODING` reports `embstr` / `raw` exactly as before. Redis/Valkey
    /// also heap-allocate the object body, so this is allocation-parity with redis
    /// plus a smaller slot.
    Inline(Box<[u8]>),
    /// A long string stored out-of-line. `OBJECT ENCODING` -> raw.
    Raw(Box<[u8]>),
    /// A LIST value (PR-5). `OBJECT ENCODING` -> `listpack` while small, `quicklist`
    /// once over the threshold (a pure function of the active repr, #40).
    ///
    /// BOXED (memory Round 1): the four collection structs are the large `ValueRepr`
    /// variants (`ListVal` 40 / `HashVal` 40 / `SetVal` 48 / `ZSetVal` 64). Holding them
    /// behind a `Box` drops `ValueRepr` to the string-variant bound, which shrinks every
    /// per-key `KvObj` and the hashbrown table slot. The string/int hot path
    /// (`Int`/`Inline`/`Raw`) is UNBOXED so the embstr SSO is untouched; the collections
    /// already heap-allocate their contents, so the `Box` is a negligible extra
    /// indirection only on collection ops.
    List(Box<ListVal>),
    /// A HASH value (PR-6). `OBJECT ENCODING` -> `listpack` while small, `hashtable`
    /// once over the entry-count OR per-element-byte threshold (a pure function of the
    /// active repr, #40). BOXED (memory Round 1); see [`ValueRepr::List`].
    Hash(Box<HashVal>),
    /// A SET value (PR-7). `OBJECT ENCODING` -> `intset` while all-integer and small,
    /// `listpack` once a non-integer member is added (and still small), `hashtable` once
    /// over the entry-count OR per-member-byte threshold (a pure function of the active
    /// repr, #40). The conversion is ONE-WAY (never demotes). BOXED (memory Round 1);
    /// see [`ValueRepr::List`].
    Set(Box<SetVal>),
    /// A ZSET (sorted set) value (PR-8). `OBJECT ENCODING` -> `listpack` while small,
    /// `skiplist` once over the entry-count OR per-member-byte threshold (a pure function
    /// of the active repr, #40). The conversion is ONE-WAY (never demotes). BOXED
    /// (memory Round 1); see [`ValueRepr::List`].
    ZSet(Box<ZSetVal>),
}

impl ValueRepr {
    /// The encoding this representation reports. For a LIST the name is a pure
    /// function of the active repr ([`ListVal::encoding`]); see #40.
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        match self {
            ValueRepr::Int(_) => Encoding::Int,
            ValueRepr::Inline(_) => Encoding::EmbStr,
            ValueRepr::Raw(_) => Encoding::Raw,
            ValueRepr::List(l) => l.encoding(),
            ValueRepr::Hash(h) => h.encoding(),
            ValueRepr::Set(s) => s.encoding(),
            ValueRepr::ZSet(z) => z.encoding(),
        }
    }

    /// The logical byte length of the value as the command layer sees it: the
    /// decimal-digit count for an int, the byte length for a string, the SUM of
    /// element byte lengths for a list. This is what STRLEN reports for a string and
    /// what the accounting hook charges for the value bytes.
    #[must_use]
    pub fn logical_len(&self) -> usize {
        match self {
            ValueRepr::Int(n) => int_decimal_len(*n),
            // Embstr and raw both hold the value bytes behind a `Box<[u8]>`; the
            // embstr-vs-raw distinction lives in `Header.encoding`, not the variant.
            ValueRepr::Inline(b) | ValueRepr::Raw(b) => b.len(),
            ValueRepr::List(l) => l.element_bytes(),
            ValueRepr::Hash(h) => h.element_bytes(),
            ValueRepr::Set(s) => s.element_bytes(),
            ValueRepr::ZSet(z) => z.element_bytes(),
        }
    }
}

// ---------------------------------------------------------------------------
// ListVal: the PR-5 list value (COLLECTIONS.md, LIST_LARGE.md, OBJECT_ENCODING_
// MAPPING.md #40). A pragmatic v1 backed by a `VecDeque<Box<[u8]>>` of elements in
// head-to-tail order, tracking the running element-byte total so accounting and the
// encoding transition are O(1) reads.
//
// ## Representation and the cascade-free contract
//
// The exact Redis listpack BYTE format is DEFERRED: OBJECT ENCODING reports the NAME
// by the ACTIVE repr (#40, OBJECT_ENCODING_MAPPING.md), not the byte layout, so a
// contiguous element deque satisfies the wire contract. The cascade-free property
// COLLECTIONS.md mandates ("an insert performs at most one tail memmove and no
// predecessor rewrite") is satisfied because a `VecDeque` insert shifts the shorter
// side ONCE with no per-element rewrite. The chunked-listpack quicklist (a
// `VecDeque` of contiguous packed chunks) is the documented #8/#135/#136 follow-up;
// it slots in behind this same `ListValue` API without touching the waist or the
// command layer.
//
// ## The listpack -> quicklist transition (#40)
//
// The reported encoding is a PURE FUNCTION of the active state: a list is `listpack`
// while its total element bytes are at or below the `list-max-listpack-size` byte
// budget, and reports `quicklist` once it exceeds that budget. There is NO
// element-count cap for lists (Redis's default `-2` negative fill sizes by BYTES with
// the count left unlimited, so e.g. a 129-element list of small values stays
// `listpack`). Reconfiguring the budget changes WHEN a value converts, never the name
// reported for a value that has not converted.
// ---------------------------------------------------------------------------

/// A LIST value (PR-5). Elements are stored in head-to-tail order in a `VecDeque`,
/// with a running element-byte total for O(1) accounting and an O(1) encoding
/// transition check. The transition threshold is the config default byte budget
/// (`list-max-listpack-size`, default 8 KB); there is NO element-count cap for lists.
/// A runtime-settable budget is a follow-up.
#[derive(Debug, Clone, Default)]
pub struct ListVal {
    /// The elements in head (front) to tail (back) order.
    elems: VecDeque<Box<[u8]>>,
    /// The running sum of element byte lengths (kept in lockstep with `elems` so the
    /// accounting weight and the encoding transition are O(1)).
    total_bytes: usize,
    /// The one-way `quicklist` ENCODING RATCHET (#40, runtime `list-max-listpack-size`): latched
    /// `true` once an edit crosses the LIVE `list-max-listpack-size` budget (byte tier and/or the
    /// positive element-count cap), and NEVER reset (Redis's quicklist does not demote, and this
    /// matches the one-way ratchet of the hash/set/zset types). Before this change the encoding was
    /// recomputed byte-purely against the COMPILED 8 KB default on every read; the ratchet is what
    /// makes a runtime `CONFIG SET list-max-listpack-size` affect FUTURE inserts only -- an existing
    /// list keeps the encoding it had (its flag does not move until the next edit re-evaluates the
    /// live budget). `force_quicklist` sets it for faithful reconstruction.
    quicklisted: bool,
}

impl ListVal {
    /// An empty list (used as the create-on-missing seed before the first push).
    #[must_use]
    pub fn new() -> Self {
        ListVal {
            elems: VecDeque::new(),
            total_bytes: 0,
            quicklisted: false,
        }
    }

    /// The sum of element byte lengths (the value-bytes side of accounting and the
    /// `logical_len` for a list). Does NOT include the key bytes (the kvobj adds
    /// those) or per-element bookkeeping (the FAM/chunk packing is a #8 follow-up).
    #[must_use]
    pub fn element_bytes(&self) -> usize {
        self.total_bytes
    }

    /// The encoding this list reports (#40): `quicklist` once the one-way [`Self::quicklisted`]
    /// ratchet has latched, else `listpack`. The ratchet latches in a mutation when the edit crosses
    /// the LIVE `list-max-listpack-size` budget, so the reported encoding is stable for an existing
    /// list across a `CONFIG SET` (it only moves on the NEXT edit), matching Redis (existing keys
    /// keep their encoding) and the hash/set/zset one-way ratchet.
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        if self.quicklisted {
            Encoding::QuickList
        } else {
            Encoding::ListPack
        }
    }

    /// Re-evaluate the one-way `quicklist` ratchet against the LIVE `list-max-listpack-size` after a
    /// mutation: latch [`Self::quicklisted`] if the current `(total_bytes, len)` exceeds EITHER the
    /// resolved byte budget OR (for the positive element-count form) the entry cap. Once latched it
    /// never resets. At the default `-2` (8 KB byte budget, no count cap) this is the same single
    /// byte compare it was before, now reading the live resolved budget.
    fn maybe_ratchet_quicklist(&mut self, thresholds: &EncodingThresholds) {
        if self.quicklisted {
            return;
        }
        let (byte_budget, entry_cap) = thresholds.list_budget();
        if self.total_bytes > byte_budget || self.elems.len() > entry_cap {
            self.quicklisted = true;
        }
    }

    /// Force the one-way `quicklist` form regardless of the current size (the faithful-reconstruction
    /// seam, HA-7b): a wire/persist codec that rebuilds a list under the UNLIMITED thresholds (so no
    /// transition fires during the rebuild) calls this when the recorded encoding was `quicklist`, so
    /// the rebuilt object reproduces the source's encoding. `OBJECT ENCODING` is a pure function of
    /// this flag. No effect on contents.
    pub fn force_quicklist(&mut self) {
        self.quicklisted = true;
    }

    /// Normalize a signed Redis index against the current length into a `usize`
    /// in-bounds offset, or `None` if it falls outside `[0, len)`. A negative index
    /// counts from the tail (`-1` is the last element).
    fn resolve_index(&self, index: i64) -> Option<usize> {
        let len = self.elems.len() as i64;
        let i = if index < 0 { index + len } else { index };
        if i < 0 || i >= len {
            None
        } else {
            Some(i as usize)
        }
    }

    /// Normalize a signed inclusive Redis range `[start, stop]` against the current
    /// length into a half-open `usize` range `start_idx..end_idx` (end exclusive),
    /// clamped to bounds. Returns an EMPTY range (`a..a`) when the range is empty or
    /// inverted, matching Redis LRANGE/LTRIM normalization.
    fn resolve_range(&self, start: i64, stop: i64) -> std::ops::Range<usize> {
        let len = self.elems.len() as i64;
        if len == 0 {
            return 0..0;
        }
        let mut s = if start < 0 { start + len } else { start };
        let mut e = if stop < 0 { stop + len } else { stop };
        if s < 0 {
            s = 0;
        }
        if e >= len {
            e = len - 1;
        }
        if s > e || s >= len {
            return 0..0;
        }
        // s..=e inclusive -> s..(e+1) half-open. Both are now in [0, len).
        (s as usize)..((e + 1) as usize)
    }
}

impl ListValue for ListVal {
    fn push_front(&mut self, elem: &[u8], thresholds: &EncodingThresholds) {
        self.total_bytes += elem.len();
        self.elems.push_front(elem.to_vec().into_boxed_slice());
        self.maybe_ratchet_quicklist(thresholds);
    }

    fn push_back(&mut self, elem: &[u8], thresholds: &EncodingThresholds) {
        self.total_bytes += elem.len();
        self.elems.push_back(elem.to_vec().into_boxed_slice());
        self.maybe_ratchet_quicklist(thresholds);
    }

    fn pop_front(&mut self) -> Option<Vec<u8>> {
        let e = self.elems.pop_front()?;
        self.total_bytes -= e.len();
        Some(e.into_vec())
    }

    fn pop_back(&mut self) -> Option<Vec<u8>> {
        let e = self.elems.pop_back()?;
        self.total_bytes -= e.len();
        Some(e.into_vec())
    }

    fn len(&self) -> usize {
        self.elems.len()
    }

    fn get(&self, index: i64) -> Option<Vec<u8>> {
        let i = self.resolve_index(index)?;
        self.elems.get(i).map(|e| e.to_vec())
    }

    fn set(&mut self, index: i64, elem: &[u8], thresholds: &EncodingThresholds) -> bool {
        let Some(i) = self.resolve_index(index) else {
            return false;
        };
        // One in-place entry rewrite (LSET): swap the element, adjust the byte total.
        let slot = &mut self.elems[i];
        self.total_bytes -= slot.len();
        self.total_bytes += elem.len();
        *slot = elem.to_vec().into_boxed_slice();
        // An overwrite that grew the bytes can cross the budget.
        self.maybe_ratchet_quicklist(thresholds);
        true
    }

    fn insert_before(
        &mut self,
        pivot: &[u8],
        elem: &[u8],
        thresholds: &EncodingThresholds,
    ) -> Option<usize> {
        let at = self.elems.iter().position(|e| e.as_ref() == pivot)?;
        // VecDeque::insert shifts the shorter side once (one tail memmove, no
        // predecessor rewrite), satisfying the cascade-free contract.
        self.elems.insert(at, elem.to_vec().into_boxed_slice());
        self.total_bytes += elem.len();
        self.maybe_ratchet_quicklist(thresholds);
        Some(self.elems.len())
    }

    fn insert_after(
        &mut self,
        pivot: &[u8],
        elem: &[u8],
        thresholds: &EncodingThresholds,
    ) -> Option<usize> {
        let at = self.elems.iter().position(|e| e.as_ref() == pivot)?;
        self.elems.insert(at + 1, elem.to_vec().into_boxed_slice());
        self.total_bytes += elem.len();
        self.maybe_ratchet_quicklist(thresholds);
        Some(self.elems.len())
    }

    fn remove_matching(&mut self, count: i64, elem: &[u8]) -> usize {
        let mut removed = 0usize;
        if count >= 0 {
            // count == 0 -> remove all; count > 0 -> at most `count`, head to tail.
            let cap = if count == 0 {
                usize::MAX
            } else {
                count as usize
            };
            let mut i = 0;
            while i < self.elems.len() && removed < cap {
                if self.elems[i].as_ref() == elem {
                    let e = self.elems.remove(i).expect("index in range");
                    self.total_bytes -= e.len();
                    removed += 1;
                    // Do not advance `i`: the next element shifted into this slot.
                } else {
                    i += 1;
                }
            }
        } else {
            // count < 0 -> at most |count|, tail to head.
            let cap = count.unsigned_abs() as usize;
            let mut i = self.elems.len();
            while i > 0 && removed < cap {
                i -= 1;
                if self.elems[i].as_ref() == elem {
                    let e = self.elems.remove(i).expect("index in range");
                    self.total_bytes -= e.len();
                    removed += 1;
                }
            }
        }
        removed
    }

    fn trim(&mut self, start: i64, stop: i64) {
        let keep = self.resolve_range(start, stop);
        if keep.is_empty() {
            self.elems.clear();
            self.total_bytes = 0;
            return;
        }
        // Drop the tail past `keep.end`, then the head before `keep.start`. Crediting
        // the byte total as we go keeps it exact.
        while self.elems.len() > keep.end {
            if let Some(e) = self.elems.pop_back() {
                self.total_bytes -= e.len();
            }
        }
        for _ in 0..keep.start {
            if let Some(e) = self.elems.pop_front() {
                self.total_bytes -= e.len();
            }
        }
    }

    fn range(&self, start: i64, stop: i64) -> Vec<Vec<u8>> {
        let r = self.resolve_range(start, stop);
        self.elems
            .iter()
            .skip(r.start)
            .take(r.len())
            .map(|e| e.to_vec())
            .collect()
    }

    fn pos(&self, elem: &[u8], rank: i64, count: Option<usize>, maxlen: usize) -> Vec<usize> {
        let mut out = Vec::new();
        let len = self.elems.len();
        if len == 0 || rank == 0 {
            return out;
        }
        // `count == Some(0)` means "all matches"; `None` means "just the first".
        let want = match count {
            None => 1,
            Some(0) => usize::MAX,
            Some(n) => n,
        };
        // `maxlen == 0` means no comparison limit.
        let cmp_limit = if maxlen == 0 { usize::MAX } else { maxlen };

        // The MAXLEN-bounded scan: `take(cmp_limit)` caps the elements COMPARED. For a
        // positive rank we scan head->tail (the first `cmp_limit`); for a negative rank
        // tail->head (the last `cmp_limit`). `to_skip` skips the first `|rank|-1`
        // matches; then we collect up to `want` matches.
        let mut to_skip = (rank.unsigned_abs() as usize) - 1;
        let mut collect = |i: usize, e: &[u8]| -> bool {
            // Returns true to STOP scanning (enough matches found).
            if e == elem {
                if to_skip > 0 {
                    to_skip -= 1;
                    return false;
                }
                out.push(i);
                return out.len() >= want;
            }
            false
        };
        if rank > 0 {
            for (i, e) in self.elems.iter().enumerate().take(cmp_limit) {
                if collect(i, e) {
                    break;
                }
            }
        } else {
            for (i, e) in self.elems.iter().enumerate().rev().take(cmp_limit) {
                if collect(i, e) {
                    break;
                }
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// HashVal: the PR-6 hash value (COLLECTIONS.md, ENCODINGS.md, OBJECT_ENCODING_
// MAPPING.md #40, HASHTABLE.md addendum). The HASH analog of `ListVal`: a small
// listpack-like form (a `Vec<(field, value)>` with linear scan, reporting `listpack`)
// that PROMOTES to a `hashbrown::HashMap` (reporting `hashtable`) once it grows past
// the HASH entry-count cap (512, NOT the 128 ZSET/SET cap) OR any field-or-value byte
// length exceeds the per-element byte cap (64).
//
// ## The listpack -> hashtable transition (#40), and its one-way ratchet
//
// The TRANSITION (small -> large) is checked on every mutating edit: a `HashVal` is a
// listpack while `entries <= hash-max-listpack-entries` AND every field-and-value byte
// length `<= hash-max-listpack-value`; once either bound is crossed it converts to the
// hashtable form. Like Redis, the conversion is ONE-WAY (a hash that grew to hashtable
// stays hashtable even if later shrunk): Redis never converts a hashtable hash back to
// listpack, so OBJECT ENCODING is a pure function of the ACTIVE repr (which form is
// resident), and the active form only ratchets up. This differs from the LIST
// listpack<->quicklist transition (which is reversible because the list reports the name
// purely from its current byte total); the hash matches Redis's one-way encoding ratchet
// for hashes (`hashTypeTryConversion` only ever promotes).
//
// ## Iteration order (HSCAN/HRANDFIELD/HGETALL determinism, ADR-0003)
//
// The listpack form preserves INSERTION order (a `Vec`); the hashtable form's
// `hashbrown::HashMap` has a per-table RandomState iteration order that varies
// run-to-run, which would break deterministic HSCAN/HRANDFIELD/HGETALL. So `iter()` /
// `fields()` SORT the hashtable form by the fixed-seed stable field hash (`scan_hash`,
// the same resize-invariant order the keyspace SCAN uses), giving a deterministic,
// resize-invariant order. The listpack form is already deterministic (insertion order),
// so it is returned as-is.
// ---------------------------------------------------------------------------

/// One stored hash entry: an owned `(field, value)` byte pair. A type alias so the
/// listpack form and its helpers do not repeat the (clippy-flagged) nested boxed-slice
/// tuple.
type HashEntry = (Box<[u8]>, Box<[u8]>);

/// A HASH value (PR-6). Stored as a small listpack-like `Vec<(field, value)>` while it
/// fits the listpack thresholds, promoting to a `hashbrown::HashMap` once it exceeds the
/// HASH entry-count cap (512; the 128 default is the ZSET/SET cap, not the hash one) OR
/// any field-or-value byte length exceeds the per-element byte cap (64). A running
/// field+value byte total is kept for O(1) accounting. The reported
/// encoding is a pure function of the active form (`listpack` vs `hashtable`).
/// The field/value storage form of a hash, independent of any per-field TTL state. The
/// listpack/hashtable ratchet (#40) lives here; the per-field TTL side-map lives on the
/// wrapping [`HashVal`] so a hash without field TTLs pays nothing for them.
#[derive(Debug, Clone)]
enum HashData {
    /// The small listpack-equivalent form: `(field, value)` pairs in insertion order,
    /// linear-scanned. Reports [`Encoding::ListPack`].
    ListPack(Vec<HashEntry>),
    /// The large hashtable form: a `hashbrown::HashMap`. Reports [`Encoding::HashTable`].
    /// One-way: a hash never converts back to the listpack form (Redis parity).
    HashTable(HashMap<Box<[u8]>, Box<[u8]>>),
}

/// Per-field expiry deadlines (Redis 7.4 HEXPIRE family). A field present in this map
/// expires at its absolute deadline; a field absent from it never expires. Kept separate
/// from the field/value [`HashData`] so the no-TTL hash (the overwhelming common case) holds
/// no TTL allocation at all.
type FieldTtls = HashMap<Box<[u8]>, UnixMillis>;

/// A HASH value (PR-6). The field/value pairs live in [`HashData`] (a small listpack-like
/// form that promotes one-way to a `hashbrown::HashMap`); an OPTIONAL per-field TTL side-map
/// (#408, Redis 7.4 HEXPIRE) is `None` until the first field TTL is set, so a hash without
/// field TTLs is byte-identical in memory and on the wire to before this feature. The
/// reported encoding is a pure function of the active form and whether any field TTL exists
/// (`listpack` vs `listpackex` for the small form, `hashtable` for the large form).
#[derive(Debug, Clone)]
pub struct HashVal {
    data: HashData,
    /// `None` while no field carries a TTL (zero cost); `Some` once a field TTL is set.
    ttls: Option<Box<FieldTtls>>,
}

impl Default for HashVal {
    fn default() -> Self {
        HashVal::new()
    }
}

impl HashVal {
    /// An empty hash (the create-on-missing seed before the first field is set), in the
    /// small listpack form.
    #[must_use]
    pub fn new() -> Self {
        HashVal {
            data: HashData::ListPack(Vec::new()),
            ttls: None,
        }
    }

    /// The sum of field+value byte lengths (the value-bytes side of accounting and the
    /// `logical_len` for a hash). Does NOT include the key bytes (the kvobj adds those)
    /// or per-entry bookkeeping (the FAM/packing is a #8 follow-up).
    #[must_use]
    pub fn element_bytes(&self) -> usize {
        match &self.data {
            HashData::ListPack(v) => v.iter().map(|(f, val)| f.len() + val.len()).sum(),
            HashData::HashTable(m) => m.iter().map(|(f, val)| f.len() + val.len()).sum(),
        }
    }

    /// The encoding this hash reports, a PURE FUNCTION of the active form (#40) and whether
    /// any field carries a TTL: `listpack` for the small form with no field TTL, `listpackex`
    /// for the small form once a field TTL is set (#408, Redis 7.4), and `hashtable` for the
    /// large form (Redis has no `hashtableex` name).
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        match &self.data {
            HashData::ListPack(_) => {
                if self.ttls.is_some() {
                    Encoding::ListPackEx
                } else {
                    Encoding::ListPack
                }
            }
            HashData::HashTable(_) => Encoding::HashTable,
        }
    }

    /// Remove `field`'s per-field expiry deadline if present, freeing the side-map once it is
    /// empty so a hash that loses its last field TTL returns to the zero-overhead form.
    /// Returns whether a deadline was removed.
    fn clear_field_ttl(&mut self, field: &[u8]) -> bool {
        let Some(ttls) = self.ttls.as_mut() else {
            return false;
        };
        let removed = ttls.remove(field).is_some();
        if ttls.is_empty() {
            self.ttls = None;
        }
        removed
    }

    /// Whether the small listpack form should convert to the hashtable form after an
    /// edit that left `entries` entries with a new `field`/`value` (the #40 transition):
    /// convert once `entries > hash-max-listpack-entries` (the HASH cap, default 512, NOT the
    /// 128 ZSET/SET cap) OR either the new field or value byte length exceeds
    /// `hash-max-listpack-value` (default 64). Reads the LIVE caps from `t` so a
    /// `CONFIG SET hash-max-listpack-*` changes WHEN a future insert converts (existing keys
    /// keep their encoding). At the default this is the same two compares it was before, just
    /// reading `t` fields (seeded to the compiled defaults) instead of constants.
    fn should_convert(
        entries: usize,
        field_len: usize,
        value_len: usize,
        t: &EncodingThresholds,
    ) -> bool {
        entries > t.hash_max_listpack_entries
            || field_len > t.hash_max_listpack_value
            || value_len > t.hash_max_listpack_value
    }

    /// Promote the small listpack form to the large hashtable form (one-way). A no-op if
    /// already a hashtable.
    fn convert_to_hashtable(&mut self) {
        if let HashData::ListPack(v) = &mut self.data {
            let mut m: HashMap<Box<[u8]>, Box<[u8]>> = HashMap::with_capacity(v.len());
            for (f, val) in v.drain(..) {
                m.insert(f, val);
            }
            self.data = HashData::HashTable(m);
        }
    }

    /// Find the index of `field` in the small listpack form (linear scan), or `None`.
    fn listpack_pos(v: &[HashEntry], field: &[u8]) -> Option<usize> {
        v.iter().position(|(f, _)| f.as_ref() == field)
    }

    /// Force the large `hashtable` form regardless of the current entry count (a no-op if
    /// already a hashtable). The faithful-reconstruction seam (HA-7b): the one-way encoding
    /// ratchet means a hash that grew to `hashtable` then shrank below the listpack
    /// thresholds STAYS `hashtable` ([`HashValue::del`] never demotes), so a wire codec that
    /// rebuilds a hash from its logical pairs (which would otherwise pick the SMALL listpack
    /// form for a now-small entry set) must be able to reproduce the captured active repr.
    /// `OBJECT ENCODING` is a pure function of the active form, so this restores the exact
    /// encoding the source object reported. No effect on contents, only on the resident form.
    pub fn force_large_encoding(&mut self) {
        self.convert_to_hashtable();
    }
}

impl HashValue for HashVal {
    fn set(&mut self, field: &[u8], value: &[u8], thresholds: &EncodingThresholds) -> bool {
        // Overwrite-in-place if present (no growth); else insert (growth). After an
        // insert into the small form, re-check the listpack -> hashtable transition.
        match &mut self.data {
            HashData::ListPack(v) => {
                if let Some(i) = HashVal::listpack_pos(v, field) {
                    v[i].1 = value.to_vec().into_boxed_slice();
                    // An overwrite can still cross the per-element value-byte cap.
                    if HashVal::should_convert(v.len(), field.len(), value.len(), thresholds) {
                        self.convert_to_hashtable();
                    }
                    false
                } else {
                    v.push((
                        field.to_vec().into_boxed_slice(),
                        value.to_vec().into_boxed_slice(),
                    ));
                    if HashVal::should_convert(v.len(), field.len(), value.len(), thresholds) {
                        self.convert_to_hashtable();
                    }
                    true
                }
            }
            HashData::HashTable(m) => m
                .insert(
                    field.to_vec().into_boxed_slice(),
                    value.to_vec().into_boxed_slice(),
                )
                .is_none(),
        }
    }

    fn set_nx(&mut self, field: &[u8], value: &[u8], thresholds: &EncodingThresholds) -> bool {
        // DEFERRED #8 follow-up (efficiency papercut, correctness-neutral): this does a
        // double linear scan on the listpack form (contains then set both scan); a single
        // entry-API pass would avoid the second scan.
        if self.contains(field) {
            return false;
        }
        self.set(field, value, thresholds);
        true
    }

    fn get(&self, field: &[u8]) -> Option<&[u8]> {
        match &self.data {
            HashData::ListPack(v) => HashVal::listpack_pos(v, field).map(|i| v[i].1.as_ref()),
            HashData::HashTable(m) => m.get(field).map(std::convert::AsRef::as_ref),
        }
    }

    fn del(&mut self, field: &[u8]) -> bool {
        // Drop any per-field TTL for the removed field too (and free the side-map if it
        // becomes empty), so a deleted field leaves no orphan deadline behind.
        let removed = match &mut self.data {
            HashData::ListPack(v) => {
                if let Some(i) = HashVal::listpack_pos(v, field) {
                    v.remove(i);
                    true
                } else {
                    false
                }
            }
            // One-way ratchet: a hashtable hash stays a hashtable even as it shrinks
            // (Redis parity), so we do NOT demote back to listpack on removal.
            HashData::HashTable(m) => m.remove(field).is_some(),
        };
        if removed {
            self.clear_field_ttl(field);
        }
        removed
    }

    fn contains(&self, field: &[u8]) -> bool {
        match &self.data {
            HashData::ListPack(v) => HashVal::listpack_pos(v, field).is_some(),
            HashData::HashTable(m) => m.contains_key(field),
        }
    }

    fn len(&self) -> usize {
        match &self.data {
            HashData::ListPack(v) => v.len(),
            HashData::HashTable(m) => m.len(),
        }
    }

    // DEFERRED #8 follow-up (efficiency papercut, correctness-neutral): fields()/values()
    // each materialize the FULL pairs() snapshot (cloning both halves) only to drop one;
    // a half-clone path would avoid the wasted allocation.
    fn fields(&self) -> Vec<Vec<u8>> {
        self.pairs().into_iter().map(|(f, _)| f).collect()
    }

    fn values(&self) -> Vec<Vec<u8>> {
        self.pairs().into_iter().map(|(_, v)| v).collect()
    }

    fn is_listpack(&self) -> bool {
        matches!(self.data, HashData::ListPack(_))
    }

    fn pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        match &self.data {
            // The listpack form is already in deterministic INSERTION order.
            HashData::ListPack(v) => v
                .iter()
                .map(|(f, val)| (f.to_vec(), val.to_vec()))
                .collect(),
            // The hashtable form's `hashbrown` iteration order varies run-to-run, so
            // SORT by the fixed-seed stable field hash (then raw bytes) for a
            // deterministic, resize-invariant order (the same order SCAN uses, ADR-0003).
            HashData::HashTable(m) => {
                let mut out: Vec<(Vec<u8>, Vec<u8>)> = m
                    .iter()
                    .map(|(f, val)| (f.to_vec(), val.to_vec()))
                    .collect();
                out.sort_unstable_by(|(fa, _), (fb, _)| {
                    scan_hash(fa).cmp(&scan_hash(fb)).then_with(|| fa.cmp(fb))
                });
                out
            }
        }
    }

    // --- Per-field TTL (#408, Redis 7.4 HEXPIRE family). The side-map is allocated only
    // when a field TTL is set and freed when the last one is cleared/reaped. ---

    fn field_ttl(&self, field: &[u8]) -> Option<UnixMillis> {
        self.ttls.as_ref().and_then(|t| t.get(field).copied())
    }

    fn set_field_ttl(&mut self, field: &[u8], deadline: UnixMillis) {
        self.ttls
            .get_or_insert_with(|| Box::new(FieldTtls::default()))
            .insert(field.to_vec().into_boxed_slice(), deadline);
    }

    fn persist_field(&mut self, field: &[u8]) -> bool {
        self.clear_field_ttl(field)
    }

    fn min_field_ttl(&self) -> Option<UnixMillis> {
        self.ttls.as_ref().and_then(|t| t.values().copied().min())
    }

    fn has_field_ttls(&self) -> bool {
        self.ttls.is_some()
    }

    fn field_ttl_pairs(&self) -> Vec<(Vec<u8>, UnixMillis)> {
        self.ttls.as_ref().map_or_else(Vec::new, |t| {
            let mut v: Vec<(Vec<u8>, UnixMillis)> =
                t.iter().map(|(f, d)| (f.to_vec(), *d)).collect();
            // Sort by field so the wire/snapshot codec is CANONICAL (the side-map is a HashMap
            // with run-to-run iteration order; a stable order keeps encode -> decode -> encode
            // byte-identical, the property the codec round-trip test asserts).
            v.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            v
        })
    }

    fn reap_expired_fields(&mut self, now: UnixMillis) -> Vec<Vec<u8>> {
        let expired: Vec<Vec<u8>> = match self.ttls.as_ref() {
            Some(ttls) => ttls
                .iter()
                .filter(|&(_, &d)| d <= now)
                .map(|(f, _)| f.to_vec())
                .collect(),
            None => return Vec::new(),
        };
        for f in &expired {
            // del() removes the field/value AND its deadline, freeing the side-map when empty.
            self.del(f);
        }
        expired
    }
}

// ---------------------------------------------------------------------------
// SetVal: the PR-7 set value (COLLECTIONS.md, ENCODINGS.md, OBJECT_ENCODING_
// MAPPING.md #40, the intset analog). The SET analog of `ListVal`/`HashVal`, with the
// three-rung Redis encoding ladder:
//
//   1. `intset`    -- an ALL-INTEGER set, stored as a SORTED `Vec<i64>` with
//                     binary-search membership (mirroring Redis's intset
//                     [redis-intset-layout]). Reports `Encoding::IntSet`.
//   2. `listpack`  -- a small MIXED-member set (or an all-integer set that took a
//                     non-integer member), stored as a `Vec<Box<[u8]>>` of raw member
//                     bytes with linear scan. Reports `Encoding::ListPack`.
//   3. `hashtable` -- a large set, stored as a `hashbrown::HashSet<Box<[u8]>>`. Reports
//                     `Encoding::HashTable`.
//
// ## The conversion ladder (#40, [redis-set-encodings-thresholds]) and its one-way ratchet
//
// Verified against Redis 7.4 src/t_set.c setTypeMaybeConvert / intsetUpgradeAndAdd and the
// pinned claims [redis-set-encoding-defaults] / [redis-set-encodings-thresholds]:
//
//   - An all-integer set with count <= set-max-intset-entries (512) is `intset`.
//   - Adding a NON-integer member, OR exceeding set-max-intset-entries, triggers a
//     conversion: the set becomes `listpack` IFF the resulting member count is <=
//     set-max-listpack-entries (128) AND every member byte length is <=
//     set-max-listpack-value (64); otherwise it becomes `hashtable`. (Because 512 > 128,
//     an integer set that exceeds 512 entries goes STRAIGHT to `hashtable`: it cannot fit
//     the 128-member listpack.)
//   - A `listpack` set that then grows past 128 members OR takes a member longer than 64
//     bytes converts to `hashtable`.
//
// Conversions are ONE-WAY: a set that grew to `listpack` or `hashtable` STAYS there even
// if later shrunk (Redis never demotes), so `OBJECT ENCODING` is a pure function of the
// ACTIVE repr and the active form only ratchets up. A set that started `intset` and only
// ever held integers stays `intset` (the count check fires on growth, never demotes on
// removal).
//
// ## Iteration order (SMEMBERS/SPOP/SRANDMEMBER/SSCAN determinism, ADR-0003)
//
// The intset form is in ascending-integer order (a sorted Vec); the listpack form is in
// insertion order (a Vec). The hashtable form's `hashbrown::HashSet` iteration order
// varies run-to-run, which would break deterministic SPOP/SRANDMEMBER/SSCAN. So
// `members()` SORTS the hashtable form by the fixed-seed stable member hash (`scan_hash`,
// the same resize-invariant order the keyspace SCAN uses), giving a deterministic,
// resize-invariant order. The intset/listpack forms are already deterministic.
// ---------------------------------------------------------------------------

/// A SET value (PR-7). The three Redis set encodings with a one-way intset -> listpack
/// -> hashtable ladder (see the module comment). A running member-byte total is NOT kept
/// (unlike `ListVal`): `element_bytes` is recomputed on demand, which is fine because the
/// store measures the accounting delta around each in-place edit and the set sizes are
/// bounded to the listpack thresholds before promotion. The reported encoding is a pure
/// function of the active form.
#[derive(Debug, Clone)]
pub enum SetVal {
    /// The all-integer `intset` form: a SORTED `Vec<i64>`, binary-search membership.
    /// Reports [`Encoding::IntSet`].
    IntSet(Vec<i64>),
    /// The small mixed-member `listpack` form: raw member bytes in insertion order,
    /// linear-scanned. Reports [`Encoding::ListPack`].
    ListPack(Vec<Box<[u8]>>),
    /// The large `hashtable` form: a `hashbrown::HashSet`. Reports [`Encoding::HashTable`].
    /// One-way: a set never converts back to a smaller form (Redis parity).
    HashTable(HashSet<Box<[u8]>>),
}

impl Default for SetVal {
    fn default() -> Self {
        SetVal::new()
    }
}

impl SetVal {
    /// An empty set (the create-on-missing seed before the first member), in the small
    /// `intset` form (an empty set is all-integer vacuously, matching Redis's
    /// `intsetNew`).
    #[must_use]
    pub fn new() -> Self {
        SetVal::IntSet(Vec::new())
    }

    /// The sum of member byte lengths (the value-bytes side of accounting and the
    /// `logical_len` for a set). For the intset form this is the sum of each integer's
    /// DECIMAL length (the byte length it would serialize to on the wire, matching how a
    /// member is returned to the client and how the listpack/hashtable forms store it),
    /// so the accounting weight is continuous across the intset -> listpack conversion.
    /// Does NOT include the key bytes (the kvobj adds those).
    #[must_use]
    pub fn element_bytes(&self) -> usize {
        match self {
            SetVal::IntSet(v) => v.iter().map(|n| int_decimal_len(*n)).sum(),
            SetVal::ListPack(v) => v.iter().map(|m| m.len()).sum(),
            SetVal::HashTable(m) => m.iter().map(|m| m.len()).sum(),
        }
    }

    /// The encoding this set reports, a PURE FUNCTION of the active form (#40):
    /// `intset` / `listpack` / `hashtable`.
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        match self {
            SetVal::IntSet(_) => Encoding::IntSet,
            SetVal::ListPack(_) => Encoding::ListPack,
            SetVal::HashTable(_) => Encoding::HashTable,
        }
    }

    /// Whether the set is in a SMALL form (`intset` or `listpack`) vs the large
    /// `hashtable` form (drives SSCAN's small-collection one-shot behavior).
    #[must_use]
    pub fn is_small(&self) -> bool {
        !matches!(self, SetVal::HashTable(_))
    }

    /// Parse a member as a CANONICAL i64 the way Redis decides intset membership
    /// (`string2ll`): a member is intset-eligible iff it round-trips as a canonical
    /// integer string (no leading zeros except "0", no "+", no whitespace), so the
    /// decimal form of the parsed integer equals the original bytes. Returns `None` for
    /// any non-canonical-integer member, which forces the listpack/hashtable form.
    fn parse_canonical_int(member: &[u8]) -> Option<i64> {
        let n = parse_set_i64(member)?;
        // Round-trip check: a member like "007" parses as 7 but is NOT canonical, so it
        // is NOT an intset member (Redis keeps it as a listpack member). The decimal
        // form of the parsed integer must equal the original bytes.
        if int_decimal_bytes(n).as_ref() == member {
            Some(n)
        } else {
            None
        }
    }

    /// Build a set from members in insertion order (the create-on-missing path), applying
    /// the encoding ladder against the LIVE `t` caps. Deduplicates. All-integer + small ->
    /// intset; else the listpack/hashtable choice per the thresholds.
    fn from_members(members: &[Vec<u8>], t: &EncodingThresholds) -> Self {
        let mut set = SetVal::new();
        for m in members {
            set.add(m, t);
        }
        set
    }

    /// Convert the current form to `hashtable` (one-way). A no-op if already a hashtable.
    fn convert_to_hashtable(&mut self) {
        match self {
            SetVal::HashTable(_) => {}
            SetVal::IntSet(v) => {
                let mut m: HashSet<Box<[u8]>> = HashSet::with_capacity(v.len());
                for n in v.drain(..) {
                    m.insert(int_decimal_bytes(n).as_ref().to_vec().into_boxed_slice());
                }
                *self = SetVal::HashTable(m);
            }
            SetVal::ListPack(v) => {
                let mut m: HashSet<Box<[u8]>> = HashSet::with_capacity(v.len());
                for member in v.drain(..) {
                    m.insert(member);
                }
                *self = SetVal::HashTable(m);
            }
        }
    }

    /// Convert the current form to `listpack` (one-way relative to intset; never called
    /// on a hashtable). Used when an all-integer intset takes a non-integer member while
    /// still small enough to be a listpack.
    fn convert_intset_to_listpack(&mut self) {
        if let SetVal::IntSet(v) = self {
            let listpack: Vec<Box<[u8]>> = v
                .drain(..)
                .map(|n| int_decimal_bytes(n).as_ref().to_vec().into_boxed_slice())
                .collect();
            *self = SetVal::ListPack(listpack);
        }
    }

    /// Whether a listpack/intset-derived set of `entries` members where the LARGEST
    /// member byte length is `max_member_len` must convert to `hashtable` (the #40
    /// listpack thresholds): convert once `entries > set-max-listpack-entries` (default 128)
    /// OR any member byte length exceeds `set-max-listpack-value` (default 64). Reads the LIVE
    /// caps from `t` so a `CONFIG SET set-max-listpack-*` changes WHEN a future add converts.
    fn listpack_overflows(entries: usize, max_member_len: usize, t: &EncodingThresholds) -> bool {
        entries > t.set_max_listpack_entries || max_member_len > t.set_max_listpack_value
    }

    /// Add a member to the intset form, applying the intset thresholds. Returns whether
    /// the member was new. The caller guarantees `self` is the intset form and `n` is the
    /// canonical integer for `member`. Reads the LIVE caps from `t`.
    fn add_int(&mut self, n: i64, member: &[u8], t: &EncodingThresholds) -> bool {
        let SetVal::IntSet(v) = self else {
            unreachable!("add_int called on a non-intset form");
        };
        match v.binary_search(&n) {
            Ok(_) => false, // already present
            Err(pos) => {
                v.insert(pos, n);
                // Growth past set-max-intset-entries converts away from intset. We re-use
                // the listpack-overflow check after a tentative listpack conversion: if
                // it would overflow the listpack, go hashtable instead. With the DEFAULT caps
                // (intset 512 > listpack 128) an integer set past 512 always goes straight to
                // hashtable; with a runtime-raised listpack cap it may fit a listpack instead.
                if v.len() > t.set_max_intset_entries {
                    let entries = v.len();
                    if SetVal::listpack_overflows(entries, member.len(), t) {
                        self.convert_to_hashtable();
                    } else {
                        self.convert_intset_to_listpack();
                    }
                }
                true
            }
        }
    }

    /// Force the large `hashtable` form regardless of the current member count (a no-op if
    /// already a hashtable). The faithful-reconstruction seam (HA-7b): the one-way encoding
    /// ratchet means a set that grew to `hashtable` then shrank STAYS `hashtable`
    /// ([`SetValue::remove`] never demotes), so a wire codec that rebuilds a set from its
    /// logical members (which would otherwise pick a smaller form for a now-small set) must
    /// be able to reproduce the captured active repr. See [`HashVal::force_large_encoding`].
    pub fn force_large_encoding(&mut self) {
        self.convert_to_hashtable();
    }

    /// Force the `listpack` form from an `intset` (a no-op for an already-`listpack` or
    /// `hashtable` set). The faithful-reconstruction seam (HA-7b): an all-integer set that
    /// reached `listpack` (because it once held a non-integer member that was later removed,
    /// or it crossed the intset-entries cap) does NOT demote back to `intset`
    /// ([`SetValue::remove`] never demotes), so a wire codec that rebuilds an all-integer set
    /// from its members (which would otherwise pick `intset`) must be able to reproduce the
    /// captured `listpack` repr. See [`Self::force_large_encoding`].
    pub fn force_listpack(&mut self) {
        self.convert_intset_to_listpack();
    }
}

impl SetValue for SetVal {
    fn add(&mut self, member: &[u8], thresholds: &EncodingThresholds) -> bool {
        match self {
            SetVal::IntSet(_) => {
                if let Some(n) = SetVal::parse_canonical_int(member) {
                    self.add_int(n, member, thresholds)
                } else {
                    // A non-integer member leaves intset: convert to listpack (or
                    // hashtable if the resulting set would overflow the listpack), then
                    // add the member.
                    self.convert_intset_to_listpack();
                    self.add(member, thresholds)
                }
            }
            SetVal::ListPack(v) => {
                if v.iter().any(|m| m.as_ref() == member) {
                    return false;
                }
                v.push(member.to_vec().into_boxed_slice());
                // Re-check the listpack thresholds after the insert.
                if SetVal::listpack_overflows(v.len(), member.len(), thresholds) {
                    self.convert_to_hashtable();
                }
                true
            }
            SetVal::HashTable(m) => m.insert(member.to_vec().into_boxed_slice()),
        }
    }

    fn remove(&mut self, member: &[u8]) -> bool {
        match self {
            SetVal::IntSet(v) => {
                let Some(n) = SetVal::parse_canonical_int(member) else {
                    return false;
                };
                match v.binary_search(&n) {
                    Ok(pos) => {
                        v.remove(pos);
                        true
                    }
                    Err(_) => false,
                }
            }
            // One-way ratchet: a listpack/hashtable set stays in its form as it shrinks
            // (Redis parity), so we do NOT demote on removal.
            SetVal::ListPack(v) => {
                if let Some(pos) = v.iter().position(|m| m.as_ref() == member) {
                    v.remove(pos);
                    true
                } else {
                    false
                }
            }
            SetVal::HashTable(m) => m.remove(member),
        }
    }

    fn contains(&self, member: &[u8]) -> bool {
        match self {
            SetVal::IntSet(v) => {
                SetVal::parse_canonical_int(member).is_some_and(|n| v.binary_search(&n).is_ok())
            }
            SetVal::ListPack(v) => v.iter().any(|m| m.as_ref() == member),
            SetVal::HashTable(m) => m.contains(member),
        }
    }

    fn len(&self) -> usize {
        match self {
            SetVal::IntSet(v) => v.len(),
            SetVal::ListPack(v) => v.len(),
            SetVal::HashTable(m) => m.len(),
        }
    }

    fn is_listpack(&self) -> bool {
        self.is_small()
    }

    fn members(&self) -> Vec<Vec<u8>> {
        match self {
            // The intset form is already in deterministic ascending-integer order; emit
            // each integer's decimal bytes.
            SetVal::IntSet(v) => v
                .iter()
                .map(|n| int_decimal_bytes(*n).as_ref().to_vec())
                .collect(),
            // The listpack form is in deterministic insertion order.
            SetVal::ListPack(v) => v.iter().map(|m| m.to_vec()).collect(),
            // The hashtable form's `hashbrown` iteration order varies run-to-run, so SORT
            // by the fixed-seed stable member hash (then raw bytes) for a deterministic,
            // resize-invariant order (the same order SCAN uses, ADR-0003).
            SetVal::HashTable(m) => {
                let mut out: Vec<Vec<u8>> = m.iter().map(|m| m.to_vec()).collect();
                out.sort_unstable_by(|a, b| scan_hash(a).cmp(&scan_hash(b)).then_with(|| a.cmp(b)));
                out
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ZSetVal: the PR-8 sorted-set (zset) value (COLLECTIONS.md, ZSET_LARGE.md,
// OBJECT_ENCODING_MAPPING.md #40, ADR-0018). The ZSET analog of `ListVal`/`HashVal`/
// `SetVal`, with the two-rung Redis zset encoding ladder:
//
//   1. `listpack`  -- a small zset, stored as a `Vec<(member, score)>` kept SORTED by
//                     (score ASC, member-bytes ASC), linear-scan membership. Reports
//                     `Encoding::ListPack`.
//   2. `skiplist`  -- a large zset, stored as a DUAL structure mirroring Redis
//                     [redis-zset-skiplist-plus-ht]: an ordered index keyed by
//                     (OrderedScore, member) for range/rank, plus a parallel
//                     `HashMap<member, score>` for O(1) ZSCORE and ZADD score-update.
//                     Reports `Encoding::SkipList`.
//
// ## The provisional large form (ZSET_LARGE.md #134/#136)
//
// The ordered index is a `BTreeSet<(OrderedScore, Box<[u8]>)>` rather than a true
// skiplist. ZSET_LARGE.md calls the skiplist "provisional" and commits this code to the
// TRAIT (ZSetValue) the index sits behind, not the concrete structure; the #136 bake-off
// (skiplist vs cache-conscious B-tree vs ART) picks the perf winner later. A BTreeMap
// gives the same ordered-range / rank semantics with correct ordering, so it satisfies
// the v1 correctness + Compatible bar; the real skiplist (or the bake-off winner) is the
// documented #134/#136 follow-up [skiplist-vs-btree-cache]. The member bytes ARE
// duplicated between the index key and the map key here (a v1 simplification; the
// shared-SDS single-allocation packing is the same #8 follow-up the other collections
// note), which the store's measured-delta accounting handles correctly regardless.
//
// ## The listpack -> skiplist transition (#40) and its one-way ratchet
//
// A `ZSetVal` is a listpack while `entries <= zset-max-listpack-entries` (128) AND every
// member byte length is `<= zset-max-listpack-value` (64); once either bound is crossed it
// converts to the skiplist form. Like Redis (and like HashVal/SetVal), the conversion is
// ONE-WAY: a zset that grew to skiplist STAYS skiplist even if later shrunk, so OBJECT
// ENCODING is a pure function of the ACTIVE repr and only ratchets up.
//
// ## Ordering (the (score, member) total order)
//
// Members order by (score ASC, then member-bytes ASC for equal scores), the Redis
// skiplist order [redis-zset-skiplist-plus-ht]. Scores compare with `f64::total_cmp` via
// the `OrderedScore` newtype, which gives a total order over all finite + infinite f64
// (+inf is the maximum, -inf the minimum). A NaN INPUT score is rejected at parse time
// before it reaches `add`/`incr`; a NaN ARITHMETIC RESULT (an existing +inf incremented by
// -inf via ZINCRBY/ZADD INCR) is caught inside `incr`, which returns `IncrOutcome::Nan`
// WITHOUT mutating, so a NaN never enters the order. `total_cmp` orders -0.0
// before +0.0, but the command layer never produces a distinct -0.0 score (parse_f64
// yields +0.0 for "0"/"-0"), so the -0.0/+0.0 distinction is not observable.
//
// ## Determinism (ZSCAN/ZRANDMEMBER, ADR-0003)
//
// Both forms expose members in the deterministic (score, member) order, so ZSCAN and the
// ZRANDMEMBER index draws (the caller draws the seed through the Env RNG seam) are
// deterministic and resize-invariant: the order does not depend on `hashbrown`'s
// per-table RandomState (the map is used ONLY for O(1) score lookup, never iterated for an
// ordered result).
// ---------------------------------------------------------------------------

/// A total-order newtype over an `f64` zset SCORE (ZSET_LARGE.md ordering). Orders by
/// `f64::total_cmp`, which is a total order over all finite and infinite f64 (NaN is
/// rejected by the command layer before a score reaches the zset, so it never appears
/// here). `+inf` is the maximum and `-inf` the minimum, matching Redis's score order.
#[derive(Debug, Clone, Copy)]
struct OrderedScore(f64);

impl PartialEq for OrderedScore {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == std::cmp::Ordering::Equal
    }
}
impl Eq for OrderedScore {}
impl PartialOrd for OrderedScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// One stored zset entry in the listpack form: an owned `(member, score)` pair. A type
/// alias so the listpack form and its helpers do not repeat the boxed-slice tuple.
type ZSetEntry = (Box<[u8]>, f64);

/// The private representation behind a [`ZSetVal`]: the two Redis zset encodings. Kept
/// private so the public [`ZSetVal`] exposes no internal type (the `OrderedScore`
/// newtype, the index/map shapes) -- the encoding is the only thing the public API
/// surfaces, via [`ZSetVal::encoding`].
#[derive(Debug, Clone)]
enum ZSetRepr {
    /// The small `listpack` form: `(member, score)` pairs kept SORTED by (score, member),
    /// linear-scanned for membership. Reports [`Encoding::ListPack`].
    ListPack(Vec<ZSetEntry>),
    /// The large `skiplist` form: a dual structure -- an ordered index
    /// `BTreeSet<(OrderedScore, member)>` for range/rank, plus a parallel
    /// `HashMap<member, score>` for O(1) score lookup [redis-zset-skiplist-plus-ht].
    /// Reports [`Encoding::SkipList`]. One-way: never converts back to listpack.
    SkipList {
        /// The ordered index keyed by (score, member) for range and rank queries.
        index: BTreeSet<(OrderedScore, Box<[u8]>)>,
        /// The member -> score map for O(1) ZSCORE and ZADD score-update.
        scores: HashMap<Box<[u8]>, f64>,
    },
}

/// A ZSET (sorted set) value (PR-8). The two Redis zset encodings with a one-way
/// listpack -> skiplist ladder (see the module comment), held behind a private
/// [`ZSetRepr`] so the public type exposes no internals. The reported encoding is a pure
/// function of the active form.
#[derive(Debug, Clone)]
pub struct ZSetVal(ZSetRepr);

impl Default for ZSetVal {
    fn default() -> Self {
        ZSetVal::new()
    }
}

impl ZSetVal {
    /// An empty zset (the create-on-missing seed before the first member), in the small
    /// listpack form.
    #[must_use]
    pub fn new() -> Self {
        ZSetVal(ZSetRepr::ListPack(Vec::new()))
    }

    /// The sum of member byte lengths PLUS a fixed 8-byte score charge per member (the
    /// value-bytes side of accounting and the `logical_len` for a zset). The 8-byte score
    /// charge is the f64 score weight (matching the task's "member bytes + a fixed 8-byte
    /// score charge" accounting basis). Does NOT include the key bytes (the kvobj adds
    /// those).
    #[must_use]
    pub fn element_bytes(&self) -> usize {
        match &self.0 {
            ZSetRepr::ListPack(v) => v.iter().map(|(m, _)| m.len() + 8).sum(),
            ZSetRepr::SkipList { scores, .. } => scores.keys().map(|m| m.len() + 8).sum(),
        }
    }

    /// The encoding this zset reports, a PURE FUNCTION of the active form (#40):
    /// `listpack` for the small form, `skiplist` for the large form.
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        match &self.0 {
            ZSetRepr::ListPack(_) => Encoding::ListPack,
            ZSetRepr::SkipList { .. } => Encoding::SkipList,
        }
    }

    /// Whether the zset is in the small `listpack` form (drives ZSCAN's small-collection
    /// one-shot behavior).
    #[must_use]
    pub fn is_small(&self) -> bool {
        matches!(self.0, ZSetRepr::ListPack(_))
    }

    /// Whether a listpack zset of `entries` members where a new/updated member byte length
    /// is `member_len` must convert to `skiplist` (the #40 listpack thresholds): convert
    /// once `entries > zset-max-listpack-entries` (default 128) OR any member byte length
    /// exceeds `zset-max-listpack-value` (default 64). Reads the LIVE caps from `t`.
    fn listpack_overflows(entries: usize, member_len: usize, t: &EncodingThresholds) -> bool {
        entries > t.zset_max_listpack_entries || member_len > t.zset_max_listpack_value
    }

    /// Promote the small listpack form to the large skiplist form (one-way). A no-op if
    /// already a skiplist.
    fn convert_to_skiplist(&mut self) {
        if let ZSetRepr::ListPack(v) = &mut self.0 {
            let mut index: BTreeSet<(OrderedScore, Box<[u8]>)> = BTreeSet::new();
            let mut scores: HashMap<Box<[u8]>, f64> = HashMap::with_capacity(v.len());
            for (m, s) in v.drain(..) {
                index.insert((OrderedScore(s), m.clone()));
                scores.insert(m, s);
            }
            self.0 = ZSetRepr::SkipList { index, scores };
        }
    }

    /// Force the large `skiplist` form regardless of the current member count (a no-op if
    /// already a skiplist). The faithful-reconstruction seam (HA-7b): the one-way encoding
    /// ratchet means a zset that grew to `skiplist` then shrank STAYS `skiplist`
    /// ([`ZSetValue::remove`] never demotes), so a wire codec that rebuilds a zset from its
    /// (member, score) pairs (which would otherwise pick the SMALL listpack form for a
    /// now-small zset) must be able to reproduce the captured active repr.
    /// See [`HashVal::force_large_encoding`].
    pub fn force_large_encoding(&mut self) {
        self.convert_to_skiplist();
    }

    /// Find the index of `member` in the sorted listpack form (linear scan), or `None`.
    fn listpack_pos(v: &[ZSetEntry], member: &[u8]) -> Option<usize> {
        v.iter().position(|(m, _)| m.as_ref() == member)
    }

    /// Insert `member` at `score` into the sorted listpack vec, keeping the (score,
    /// member) order. The caller guarantees `member` is NOT already present.
    fn listpack_insert_sorted(v: &mut Vec<ZSetEntry>, member: &[u8], score: f64) {
        let pos = v
            .binary_search_by(|(m, s)| s.total_cmp(&score).then_with(|| m.as_ref().cmp(member)))
            .unwrap_or_else(|e| e);
        v.insert(pos, (member.to_vec().into_boxed_slice(), score));
    }

    /// Set `member` to `score` UNCONDITIONALLY (used by `add`/`incr` after the flag
    /// decision is made). Adds if absent, rewrites the score if present (re-sorting in the
    /// listpack form, remove-then-reinsert in the skiplist index), and re-checks the
    /// listpack->skiplist transition against the LIVE `t` caps. Returns whether the member was NEW.
    fn put(&mut self, member: &[u8], score: f64, t: &EncodingThresholds) -> bool {
        match &mut self.0 {
            ZSetRepr::ListPack(v) => {
                let was_new = if let Some(i) = ZSetVal::listpack_pos(v, member) {
                    // Score rewrite: remove the old entry then re-insert in order.
                    v.remove(i);
                    ZSetVal::listpack_insert_sorted(v, member, score);
                    false
                } else {
                    ZSetVal::listpack_insert_sorted(v, member, score);
                    true
                };
                if ZSetVal::listpack_overflows(v.len(), member.len(), t) {
                    self.convert_to_skiplist();
                }
                was_new
            }
            ZSetRepr::SkipList { index, scores } => {
                if let Some(old) = scores.get(member).copied() {
                    // Score update: remove-then-reinsert in the ordered index plus an
                    // in-place score rewrite in the map (the ZSET_LARGE.md sync invariant).
                    index.remove(&(OrderedScore(old), member.to_vec().into_boxed_slice()));
                    index.insert((OrderedScore(score), member.to_vec().into_boxed_slice()));
                    scores.insert(member.to_vec().into_boxed_slice(), score);
                    false
                } else {
                    index.insert((OrderedScore(score), member.to_vec().into_boxed_slice()));
                    scores.insert(member.to_vec().into_boxed_slice(), score);
                    true
                }
            }
        }
    }

    /// All `(member, score)` pairs in (score, member) order. The single ordered-snapshot
    /// helper the range/rank/scan methods build on.
    fn ordered(&self) -> Vec<(Vec<u8>, f64)> {
        match &self.0 {
            ZSetRepr::ListPack(v) => v.iter().map(|(m, s)| (m.to_vec(), *s)).collect(),
            ZSetRepr::SkipList { index, .. } => {
                index.iter().map(|(s, m)| (m.to_vec(), s.0)).collect()
            }
        }
    }

    /// Construct a zset from `(member, score)` pairs in arbitrary order (the
    /// create-on-missing / *STORE path), deduplicating by member (the LAST score wins, as
    /// the caller already resolved aggregation) and applying the encoding ladder.
    fn from_pairs(pairs: &[(Vec<u8>, f64)], t: &EncodingThresholds) -> Self {
        let mut z = ZSetVal::new();
        for (m, s) in pairs {
            z.put(m, *s, t);
        }
        z
    }

    /// Normalize a signed inclusive Redis RANK range `[start, stop]` against `len` into a
    /// half-open `usize` range, clamped to bounds; an empty/inverted range yields `a..a`.
    /// The same normalization LRANGE/ZRANGE-by-index use.
    fn resolve_rank_range(len: usize, start: i64, stop: i64) -> std::ops::Range<usize> {
        let len_i = len as i64;
        if len_i == 0 {
            return 0..0;
        }
        let mut s = if start < 0 { start + len_i } else { start };
        let mut e = if stop < 0 { stop + len_i } else { stop };
        if s < 0 {
            s = 0;
        }
        if e >= len_i {
            e = len_i - 1;
        }
        if s > e || s >= len_i {
            return 0..0;
        }
        (s as usize)..((e + 1) as usize)
    }

    /// Apply an optional `(offset, count)` LIMIT to an already-ordered list: skip
    /// `offset` (a negative offset yields nothing, matching Redis), then take `count`
    /// (a negative count means "to the end").
    fn apply_limit<T>(mut items: Vec<T>, limit: Option<(i64, i64)>) -> Vec<T> {
        let Some((offset, count)) = limit else {
            return items;
        };
        if offset < 0 {
            return Vec::new();
        }
        let offset = offset as usize;
        if offset >= items.len() {
            return Vec::new();
        }
        items.drain(..offset);
        if count >= 0 {
            items.truncate(count as usize);
        }
        items
    }
}

/// Whether a score UPDATE from `cur` to `new` passes the GT/LT gate (used by both `add`
/// and `incr`). GT permits the update only when `new` is strictly greater than `cur`; LT
/// only when strictly less; with neither set the update always passes. Written with
/// positive comparisons (no negated partial-ord operator) so a non-finite score compares
/// the IEEE way Redis relies on (GT vs `+inf` never updates, etc.).
fn gate_passes(cur: f64, new: f64, flags: ZAddFlags) -> bool {
    if flags.gt {
        new > cur
    } else if flags.lt {
        new < cur
    } else {
        true
    }
}

impl ZSetValue for ZSetVal {
    #[allow(clippy::float_cmp)] // `score != cur` is the exact Redis CH "changed" check.
    fn add(
        &mut self,
        member: &[u8],
        score: f64,
        flags: ZAddFlags,
        thresholds: &EncodingThresholds,
    ) -> ZAddOutcome {
        let Some(cur) = self.score(member) else {
            // The member is absent: XX suppresses adding it; GT/LT alone DO add new members
            // (per Redis they only gate UPDATES), so they fall through to the add.
            if flags.xx {
                return ZAddOutcome {
                    added: false,
                    changed: false,
                    new_score: None,
                };
            }
            self.put(member, score, thresholds);
            return ZAddOutcome {
                added: true,
                changed: true,
                new_score: Some(score),
            };
        };
        // The member exists: NX suppresses any update; GT/LT gate on the new score.
        if flags.nx || !gate_passes(cur, score, flags) {
            return ZAddOutcome {
                added: false,
                changed: false,
                new_score: Some(cur),
            };
        }
        // Apply the score (an equal score still counts as unchanged, matching Redis: CH
        // counts a member as changed only if the score actually differs).
        let changed = score != cur;
        if changed {
            self.put(member, score, thresholds);
        }
        ZAddOutcome {
            added: false,
            changed,
            new_score: Some(score),
        }
    }

    fn incr(
        &mut self,
        member: &[u8],
        delta: f64,
        flags: ZAddFlags,
        thresholds: &EncodingThresholds,
    ) -> IncrOutcome {
        let Some(cur) = self.score(member) else {
            if flags.xx {
                return IncrOutcome::Suppressed; // XX on a missing member: INCR -> nil.
            }
            // Create at `delta` (ZINCRBY/ZADD INCR on a missing member starts from 0). A
            // NaN delta itself was rejected at parse time, so the create score is never NaN;
            // guard defensively anyway so the store never stores a NaN.
            if delta.is_nan() {
                return IncrOutcome::Nan;
            }
            self.put(member, delta, thresholds);
            return IncrOutcome::Updated(delta);
        };
        if flags.nx {
            return IncrOutcome::Suppressed; // NX on an existing member: INCR -> nil.
        }
        let new = cur + delta;
        // A NaN RESULT (e.g. an existing +inf incremented by -inf) is rejected WITHOUT
        // mutating: the store never holds a NaN score, and the command layer returns
        // `-ERR resulting score is not a number (NaN)`. Checked BEFORE the gate so the
        // NaN error takes precedence over a GT/LT suppression.
        if new.is_nan() {
            return IncrOutcome::Nan;
        }
        if !gate_passes(cur, new, flags) {
            return IncrOutcome::Suppressed; // GT/LT gate failed: INCR -> nil.
        }
        self.put(member, new, thresholds);
        IncrOutcome::Updated(new)
    }

    fn score(&self, member: &[u8]) -> Option<f64> {
        match &self.0 {
            ZSetRepr::ListPack(v) => ZSetVal::listpack_pos(v, member).map(|i| v[i].1),
            ZSetRepr::SkipList { scores, .. } => scores.get(member).copied(),
        }
    }

    fn remove(&mut self, member: &[u8]) -> bool {
        match &mut self.0 {
            ZSetRepr::ListPack(v) => {
                if let Some(i) = ZSetVal::listpack_pos(v, member) {
                    v.remove(i);
                    true
                } else {
                    false
                }
            }
            // One-way ratchet: a skiplist zset stays a skiplist as it shrinks (Redis
            // parity), so we do NOT demote back to listpack on removal.
            ZSetRepr::SkipList { index, scores } => {
                if let Some(old) = scores.remove(member) {
                    index.remove(&(OrderedScore(old), member.to_vec().into_boxed_slice()));
                    true
                } else {
                    false
                }
            }
        }
    }

    fn len(&self) -> usize {
        match &self.0 {
            ZSetRepr::ListPack(v) => v.len(),
            ZSetRepr::SkipList { scores, .. } => scores.len(),
        }
    }

    fn rank(&self, member: &[u8], rev: bool) -> Option<usize> {
        let ordered = self.ordered();
        let pos = ordered.iter().position(|(m, _)| m.as_slice() == member)?;
        Some(if rev { ordered.len() - 1 - pos } else { pos })
    }

    fn range_by_rank(&self, start: i64, stop: i64, rev: bool) -> Vec<(Vec<u8>, f64)> {
        let mut ordered = self.ordered();
        if rev {
            ordered.reverse();
        }
        let r = ZSetVal::resolve_rank_range(ordered.len(), start, stop);
        ordered[r].to_vec()
    }

    fn range_by_score(
        &self,
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Vec<(Vec<u8>, f64)> {
        // Filter in ascending order, then reverse for rev (so the LIMIT offset/count apply
        // to the result order, matching Redis ZREVRANGEBYSCORE).
        let mut out: Vec<(Vec<u8>, f64)> = self
            .ordered()
            .into_iter()
            .filter(|(_, s)| min.allows_min(*s) && max.allows_max(*s))
            .collect();
        if rev {
            out.reverse();
        }
        ZSetVal::apply_limit(out, limit)
    }

    fn range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = self
            .ordered()
            .into_iter()
            .filter(|(m, _)| min.allows_min(m) && max.allows_max(m))
            .map(|(m, _)| m)
            .collect();
        if rev {
            out.reverse();
        }
        ZSetVal::apply_limit(out, limit)
    }

    fn count_by_score(&self, min: ScoreBound, max: ScoreBound) -> usize {
        self.ordered()
            .iter()
            .filter(|(_, s)| min.allows_min(*s) && max.allows_max(*s))
            .count()
    }

    fn count_by_lex(&self, min: &LexBound, max: &LexBound) -> usize {
        self.ordered()
            .iter()
            .filter(|(m, _)| min.allows_min(m) && max.allows_max(m))
            .count()
    }

    fn pop_min(&mut self, count: usize) -> Vec<(Vec<u8>, f64)> {
        let mut ordered = self.ordered();
        ordered.truncate(count);
        for (m, _) in &ordered {
            self.remove(m);
        }
        ordered
    }

    fn pop_max(&mut self, count: usize) -> Vec<(Vec<u8>, f64)> {
        let mut ordered = self.ordered();
        ordered.reverse();
        ordered.truncate(count);
        for (m, _) in &ordered {
            self.remove(m);
        }
        ordered
    }

    fn remove_range_by_rank(&mut self, start: i64, stop: i64) -> usize {
        let ordered = self.ordered();
        let r = ZSetVal::resolve_rank_range(ordered.len(), start, stop);
        let victims: Vec<Vec<u8>> = ordered[r].iter().map(|(m, _)| m.clone()).collect();
        for m in &victims {
            self.remove(m);
        }
        victims.len()
    }

    fn remove_range_by_score(&mut self, min: ScoreBound, max: ScoreBound) -> usize {
        let victims: Vec<Vec<u8>> = self
            .ordered()
            .into_iter()
            .filter(|(_, s)| min.allows_min(*s) && max.allows_max(*s))
            .map(|(m, _)| m)
            .collect();
        for m in &victims {
            self.remove(m);
        }
        victims.len()
    }

    fn remove_range_by_lex(&mut self, min: &LexBound, max: &LexBound) -> usize {
        let victims: Vec<Vec<u8>> = self
            .ordered()
            .into_iter()
            .filter(|(m, _)| min.allows_min(m) && max.allows_max(m))
            .map(|(m, _)| m)
            .collect();
        for m in &victims {
            self.remove(m);
        }
        victims.len()
    }

    fn members_with_scores(&self) -> Vec<(Vec<u8>, f64)> {
        self.ordered()
    }

    fn is_listpack(&self) -> bool {
        self.is_small()
    }
}

/// Parse a member as an i64 the way Redis `string2ll` (src/util.c) does for intset
/// membership: an optional single leading `-` then ASCII digits, full i64 range,
/// rejecting leading `+`, whitespace, and overflow. Leading-zero canonicality is checked
/// by the round-trip in [`SetVal::parse_canonical_int`], not here. Kept local to the
/// store (the command layer has its own `parse_i64`; this is the store-side intset
/// classifier).
fn parse_set_i64(arg: &[u8]) -> Option<i64> {
    if arg.is_empty() {
        return None;
    }
    let (neg, digits) = if arg[0] == b'-' {
        (true, &arg[1..])
    } else {
        (false, arg)
    };
    if digits.is_empty() {
        return None;
    }
    let mut acc: u64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
    }
    if neg {
        const MIN_MAGNITUDE: u64 = (i64::MAX as u64) + 1;
        if acc > MIN_MAGNITUDE {
            return None;
        }
        if acc == MIN_MAGNITUDE {
            return Some(i64::MIN);
        }
        Some(-(acc as i64))
    } else {
        if acc > i64::MAX as u64 {
            return None;
        }
        Some(acc as i64)
    }
}

/// Decimal byte length of an i64 (digit count plus a sign for negatives), without
/// allocating. Used by STRLEN and accounting for int-encoded values.
#[must_use]
pub fn int_decimal_len(n: i64) -> usize {
    // Fast, allocation-free decimal length. `itoa`-style buffer would also work;
    // a small fixed buffer keeps it dependency-free and deterministic.
    let mut buf = [0u8; 20];
    format_i64(n, &mut buf).len()
}

/// Format an i64 into `buf` (big enough for any i64: 20 bytes covers
/// "-9223372036854775808") and return the written slice. No allocation, no
/// `std::fmt` machinery on the value path.
fn format_i64(n: i64, buf: &mut [u8; 20]) -> &[u8] {
    // Work in i128 so i64::MIN negates cleanly.
    let mut v = i128::from(n);
    let neg = v < 0;
    if neg {
        v = -v;
    }
    let mut i = buf.len();
    if v == 0 {
        i -= 1;
        buf[i] = b'0';
    } else {
        while v > 0 {
            i -= 1;
            buf[i] = b'0' + (v % 10) as u8;
            v /= 10;
        }
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    &buf[i..]
}

/// The decimal bytes of an i64 as an owned [`Bytes`] (the int-materialization the
/// read path hands to a [`ValueRef`]).
#[must_use]
pub fn int_decimal_bytes(n: i64) -> Bytes {
    let mut buf = [0u8; 20];
    Bytes::copy_from_slice(format_i64(n, &mut buf))
}

/// One key/value entry: the safe-representation `kvobj` (OBJECT_LAYOUT.md #111).
///
/// Holds the packed [`Header`], the key bytes, the [`ValueRepr`], and the optional
/// absolute TTL deadline. Type/encoding/TTL/eviction-rank/snapshot-version are ALL
/// readable from this one object with no side lookup, which is the OBJECT_LAYOUT.md
/// read-completeness contract. See the module docs for why this is a safe multi
/// -field rep now and a single FAM allocation later.
#[derive(Debug, Clone)]
pub struct KvObj {
    /// The packed metadata header.
    pub header: Header,
    /// The key bytes (inline in the FAM layout; a `Box<[u8]>` in the safe rep).
    pub key: Box<[u8]>,
    /// The value representation.
    pub value: ValueRepr,
    /// The absolute TTL deadline, if any (EXPIRATION.md). `None` means no TTL.
    pub expire_at: Option<UnixMillis>,
}

impl KvObj {
    /// Build a `KvObj` from a key and already-classified value bytes/encoding.
    #[must_use]
    pub fn from_classified(
        key: &[u8],
        classified: Classified,
        bytes: &[u8],
        expire_at: Option<UnixMillis>,
    ) -> Self {
        let value = match classified {
            Classified::Int(n) => ValueRepr::Int(n),
            Classified::EmbStr => ValueRepr::Inline(bytes.to_vec().into_boxed_slice()),
            Classified::Raw => ValueRepr::Raw(bytes.to_vec().into_boxed_slice()),
        };
        let header = Header::new(value.encoding(), expire_at.is_some());
        KvObj {
            header,
            key: key.to_vec().into_boxed_slice(),
            value,
            expire_at,
        }
    }

    /// Build a `KvObj` from a key and raw value bytes, classifying the encoding
    /// (the common SET path).
    #[must_use]
    pub fn from_bytes(key: &[u8], bytes: &[u8], expire_at: Option<UnixMillis>) -> Self {
        KvObj::from_classified(key, classify(bytes), bytes, expire_at)
    }

    /// Build a `KvObj` from a key and an already-parsed integer (the `NewValue::Int`
    /// / `NewValueOwned::Int` fast path): stored int-encoded with no value alloc.
    #[must_use]
    pub fn from_int(key: &[u8], n: i64, expire_at: Option<UnixMillis>) -> Self {
        KvObj {
            header: Header::new(Encoding::Int, expire_at.is_some()),
            key: key.to_vec().into_boxed_slice(),
            value: ValueRepr::Int(n),
            expire_at,
        }
    }

    /// Build a `KvObj` from a key and an owned write value (the rmw write path).
    /// An `Int` variant is stored int-encoded directly; a `Bytes` variant is
    /// classified (so a numeric string written via rmw still becomes int). `thresholds`
    /// carries the LIVE collection-encoding caps (#40) the freshly-built collection's
    /// transition is evaluated against: the store passes its live snapshot (so a new
    /// collection respects a `CONFIG SET *-max-listpack-*`); a RECONSTRUCTION caller
    /// (kvcodec/persist) passes [`EncodingThresholds::unlimited`] so NO transition fires
    /// during the rebuild and the recorded encoding is then FORCED, reproducing the source
    /// encoding regardless of the local thresholds.
    #[must_use]
    pub fn from_new_owned(
        key: &[u8],
        value: NewValueOwned,
        expire_at: Option<UnixMillis>,
        thresholds: &EncodingThresholds,
    ) -> Self {
        match value {
            NewValueOwned::Int(n) => KvObj::from_int(key, n, expire_at),
            NewValueOwned::Bytes(b) => KvObj::from_bytes(key, &b, expire_at),
            // The PR-5 create-on-missing LIST path: build the list value from the
            // head-to-tail elements. Subsequent edits go through the in-place
            // RmwAction::Mutated path, not this rebuild.
            NewValueOwned::List(elems) => {
                let mut list = ListVal::new();
                for e in &elems {
                    list.push_back(e, thresholds);
                }
                KvObj::from_list(key, list, expire_at)
            }
            // The PR-6 create-on-missing HASH path: build the hash value from the
            // insertion-ordered (field, value) pairs. Subsequent edits go through the
            // in-place RmwAction::Mutated path, not this rebuild.
            NewValueOwned::Hash(pairs) => {
                let mut hash = HashVal::new();
                for (f, v) in &pairs {
                    hash.set(f, v, thresholds);
                }
                KvObj::from_hash(key, hash, expire_at)
            }
            // The HSETEX create-on-missing path (#408): build the hash, then attach each
            // field's TTL so a brand-new key is created already carrying field expirations.
            NewValueOwned::HashEx(pairs, ttls) => {
                let mut hash = HashVal::new();
                for (f, v) in &pairs {
                    hash.set(f, v, thresholds);
                }
                for (f, deadline) in &ttls {
                    if hash.contains(f) {
                        hash.set_field_ttl(f, *deadline);
                    }
                }
                KvObj::from_hash(key, hash, expire_at)
            }
            // The PR-7 create-on-missing SET path: build the set value from the members
            // (deduped + ladder-applied by `SetVal::from_members`). Subsequent edits go
            // through the in-place RmwAction::Mutated path, not this rebuild.
            NewValueOwned::Set(members) => {
                let set = SetVal::from_members(&members, thresholds);
                KvObj::from_set(key, set, expire_at)
            }
            // The PR-8 create-on-missing ZSET path: build the zset value from the
            // (member, score) pairs (deduped -- last score wins -- + ladder/ordering
            // applied by `ZSetVal::from_pairs`). Subsequent edits go through the in-place
            // RmwAction::Mutated path, not this rebuild.
            NewValueOwned::ZSet(pairs) => {
                let zset = ZSetVal::from_pairs(&pairs, thresholds);
                KvObj::from_zset(key, zset, expire_at)
            }
        }
    }

    /// Build a `KvObj` holding a LIST value (PR-5). The data type is
    /// [`DataType::List`] and the encoding is read off the list's active repr
    /// ([`ListVal::encoding`]); the store recomputes it after each in-place edit.
    #[must_use]
    pub fn from_list(key: &[u8], list: ListVal, expire_at: Option<UnixMillis>) -> Self {
        let encoding = list.encoding();
        KvObj {
            header: Header::with_type(DataType::List, encoding, expire_at.is_some()),
            key: key.to_vec().into_boxed_slice(),
            value: ValueRepr::List(Box::new(list)),
            expire_at,
        }
    }

    /// Build a `KvObj` holding a HASH value (PR-6). The data type is
    /// [`DataType::Hash`] and the encoding is read off the hash's active form
    /// ([`HashVal::encoding`]); the store recomputes it after each in-place edit.
    #[must_use]
    pub fn from_hash(key: &[u8], hash: HashVal, expire_at: Option<UnixMillis>) -> Self {
        let encoding = hash.encoding();
        KvObj {
            header: Header::with_type(DataType::Hash, encoding, expire_at.is_some()),
            key: key.to_vec().into_boxed_slice(),
            value: ValueRepr::Hash(Box::new(hash)),
            expire_at,
        }
    }

    /// Build a `KvObj` holding a SET value (PR-7). The data type is [`DataType::Set`]
    /// and the encoding is read off the set's active form ([`SetVal::encoding`]); the
    /// store recomputes it after each in-place edit.
    #[must_use]
    pub fn from_set(key: &[u8], set: SetVal, expire_at: Option<UnixMillis>) -> Self {
        let encoding = set.encoding();
        KvObj {
            header: Header::with_type(DataType::Set, encoding, expire_at.is_some()),
            key: key.to_vec().into_boxed_slice(),
            value: ValueRepr::Set(Box::new(set)),
            expire_at,
        }
    }

    /// Build a `KvObj` holding a ZSET value (PR-8). The data type is [`DataType::ZSet`]
    /// and the encoding is read off the zset's active form ([`ZSetVal::encoding`]); the
    /// store recomputes it after each in-place edit.
    #[must_use]
    pub fn from_zset(key: &[u8], zset: ZSetVal, expire_at: Option<UnixMillis>) -> Self {
        let encoding = zset.encoding();
        KvObj {
            header: Header::with_type(DataType::ZSet, encoding, expire_at.is_some()),
            key: key.to_vec().into_boxed_slice(),
            value: ValueRepr::ZSet(Box::new(zset)),
            expire_at,
        }
    }

    /// Recompute and store `header.encoding` from the CURRENT value representation
    /// (PR-5: called by the store after an in-place collection edit, so a list that
    /// crossed the listpack->quicklist threshold, or a hash that crossed listpack->
    /// hashtable, reports the new name). A no-op for a string value whose encoding is
    /// already in lockstep with its repr.
    pub fn recompute_encoding(&mut self) {
        self.header.encoding = self.value.encoding();
    }

    /// A mutable borrow of the stored LIST value, or `None` if this entry is not a
    /// list (PR-5: the store hands this to the in-place-mutation arm; a non-list
    /// yields `None` -> WRONGTYPE).
    pub fn as_list_mut(&mut self) -> Option<&mut ListVal> {
        match &mut self.value {
            // Deref through the `Box` (memory Round 1) to the `&mut ListVal` the
            // collection trait + in-place RMW path expect.
            ValueRepr::List(l) => Some(&mut **l),
            _ => None,
        }
    }

    /// A mutable borrow of the stored HASH value, or `None` if this entry is not a hash
    /// (PR-6: the store hands this to the in-place-mutation arm; a non-hash yields
    /// `None` -> WRONGTYPE). The HASH analog of [`Self::as_list_mut`].
    pub fn as_hash_mut(&mut self) -> Option<&mut HashVal> {
        match &mut self.value {
            ValueRepr::Hash(h) => Some(&mut **h),
            _ => None,
        }
    }

    /// A mutable borrow of the stored SET value, or `None` if this entry is not a set
    /// (PR-7: the store hands this to the in-place-mutation arm; a non-set yields `None`
    /// -> WRONGTYPE). The SET analog of [`Self::as_list_mut`]/[`Self::as_hash_mut`].
    pub fn as_set_mut(&mut self) -> Option<&mut SetVal> {
        match &mut self.value {
            ValueRepr::Set(s) => Some(&mut **s),
            _ => None,
        }
    }

    /// A mutable borrow of the stored ZSET value, or `None` if this entry is not a sorted
    /// set (PR-8: the store hands this to the in-place-mutation arm; a non-zset yields
    /// `None` -> WRONGTYPE). The ZSET analog of [`Self::as_list_mut`]/[`Self::as_hash_mut`]/
    /// [`Self::as_set_mut`].
    pub fn as_zset_mut(&mut self) -> Option<&mut ZSetVal> {
        match &mut self.value {
            ValueRepr::ZSet(z) => Some(&mut **z),
            _ => None,
        }
    }

    /// Whether this entry holds a LIST value (PR-5).
    #[must_use]
    pub fn is_list(&self) -> bool {
        matches!(self.value, ValueRepr::List(_))
    }

    /// Whether this entry holds a HASH value (PR-6).
    #[must_use]
    pub fn is_hash(&self) -> bool {
        matches!(self.value, ValueRepr::Hash(_))
    }

    /// Whether this entry holds a SET value (PR-7).
    #[must_use]
    pub fn is_set(&self) -> bool {
        matches!(self.value, ValueRepr::Set(_))
    }

    /// Whether this entry holds a ZSET value (PR-8).
    #[must_use]
    pub fn is_zset(&self) -> bool {
        matches!(self.value, ValueRepr::ZSet(_))
    }

    /// Whether this entry is a COLLECTION at all (list/hash/set/...; PR-5 list, PR-6 hash,
    /// PR-7 set). The SINGLE source of "what reprs are collections", so the store's
    /// `rmw_mut` type-dispatch and the empty-collection check stay in sync (the PR-5
    /// review's consolidation ask). A non-collection (string family) is `false`.
    ///
    /// PR-8 NOTE: when the zset repr lands, add its arm to the THREE collection sites that
    /// share this enum -- [`Self::collection_len`] (which backs
    /// [`Self::is_empty_collection`]), the `rmw_mut` type-dispatch in `lib.rs`
    /// ([`crate::ShardStore`] selecting `as_*_mut`), and (if a read view is needed) the
    /// store's `view_of`/`occupied_of` collection arms -- IN LOCKSTEP.
    #[must_use]
    pub fn is_collection(&self) -> bool {
        matches!(
            self.value,
            ValueRepr::List(_) | ValueRepr::Hash(_) | ValueRepr::Set(_) | ValueRepr::ZSet(_)
        )
    }

    /// The element COUNT of this entry IF it is a collection, else `None` (a
    /// non-collection has no element count). This is the SINGLE place that maps each
    /// collection repr to its element count, so [`Self::is_empty_collection`] and any
    /// future len-based check cannot drift from the `rmw_mut` type-dispatch. Add new
    /// collection arms HERE when the set/zset reprs land.
    #[must_use]
    pub fn collection_len(&self) -> Option<usize> {
        match &self.value {
            ValueRepr::List(l) => Some(l.len()),
            ValueRepr::Hash(h) => Some(h.len()),
            ValueRepr::Set(s) => Some(s.len()),
            ValueRepr::ZSet(z) => Some(z.len()),
            _ => None,
        }
    }

    /// Whether this entry is a COLLECTION that currently holds zero ELEMENTS (PR-5/6:
    /// the empty-collection-deletes-key check, by element COUNT, not byte count -- a
    /// hash of empty-string values has zero value bytes but is NOT empty). Returns
    /// `false` for a non-collection value (a string is never "empty" in this sense).
    /// Defined in terms of [`Self::collection_len`] so it cannot drift from the
    /// type-dispatch.
    #[must_use]
    pub fn is_empty_collection(&self) -> bool {
        self.collection_len() == Some(0)
    }

    /// Replace this object's VALUE in place (and reclassify its encoding) while
    /// keeping the key. The TTL is set separately by the store. Returns the new
    /// value's logical length (for accounting deltas).
    pub fn set_value_bytes(&mut self, bytes: &[u8]) {
        self.value = match classify(bytes) {
            Classified::Int(n) => ValueRepr::Int(n),
            Classified::EmbStr => ValueRepr::Inline(bytes.to_vec().into_boxed_slice()),
            Classified::Raw => ValueRepr::Raw(bytes.to_vec().into_boxed_slice()),
        };
        self.header.encoding = self.value.encoding();
    }

    /// The logical byte length of the value (STRLEN / accounting).
    #[must_use]
    pub fn logical_len(&self) -> usize {
        self.value.logical_len()
    }

    /// The accounting weight of this entry: key bytes + value logical bytes. This
    /// is what the accounting hook charges on insert and credits on remove. (The
    /// fixed per-object header overhead is added when the FAM layout lands; PR-2a
    /// counts the variable bytes, which is the honest logical-byte basis the
    /// `used_memory` counter exposes.)
    #[must_use]
    pub fn accounted_bytes(&self) -> usize {
        self.key.len() + self.logical_len()
    }

    /// Whether this entry's TTL deadline has passed at `now` (the lazy-backstop
    /// predicate). An entry with no deadline never expires.
    ///
    /// The comparison is STRICTLY greater (`now > deadline`), matching Valkey's
    /// `timestampIsExpired`/`keyIsExpired` (`return now > when;`, src/db.c): a key
    /// is ALIVE at `now == deadline` and expired only once `now` strictly exceeds
    /// the deadline.
    #[must_use]
    pub fn is_expired(&self, now: UnixMillis) -> bool {
        match self.expire_at {
            Some(deadline) => now > deadline,
            None => false,
        }
    }
}

// ===========================================================================
// The SINGLE-ALLOCATION table entry (memory Round 3, OBJECT_LAYOUT.md #111).
//
// The per-shard table now stores ONE `Entry` per key, NOT a `HashMap` key/value
// pair. The previous representation was `hashbrown::HashMap<Box<[u8]>, KvObj>`:
// THREE allocations per string key (the map's owned key `Box`, the `KvObj`'s own
// duplicate key `Box`, and the value `Box`), carried in an 80-byte map slot.
//
// `Entry` collapses a STRING key to ONE allocation (the Str arm): a single thin blob
// that holds, contiguously, a `u32` total-length prefix, then a small packed header,
// the optional TTL deadline, the key length, the key bytes, and the value bytes (the
// value is INLINE in the same blob, tighter than redis's two-allocation kvobj). The
// table itself stores ONLY the `Entry` and derives the key from inside it (the
// `Entry::key` helper), so there is no separate map key allocation and no key
// duplication.
//
// A COLLECTION key (the Coll arm) is a `Box<CollEntry>`: a small header + the key +
// the existing boxed collection value. Collections already heap-allocate their
// contents, so one extra small box is negligible.
//
// ## The 8-byte tagged-pointer slot (perf/tagged-slot)
//
// `Entry` is an 8-byte TAGGED POINTER (`NonNull<u8>`), not a 16-byte enum. The Str arm
// was a `Box<[u8]>` FAT pointer (ptr + length, 16 bytes); moving the length into the
// allocation as the `u32` prefix leaves a one-word THIN pointer, and the Str/Coll arms
// are distinguished by the pointer's low bit (0 = Str blob, 1 = `Box<CollEntry>`)
// rather than an enum discriminant. This HALVES the `hashbrown::HashTable<Entry>` slot
// (16 -> 8 bytes), roughly halving `table_bytes_per_key`. Distinguishing by the low
// bit is sound because both allocations are >= 2-aligned (the Str blob is allocated
// align 8; `CollEntry` has align >= 2), so the low bit is always free.
//
// This is the ONLY place `ironcache-store` uses `unsafe`: the manual Str allocation,
// the tag set/clear, the access reconstructions, and the `Drop`/`dealloc`. Every
// `unsafe` block in the `Entry` impl carries a `// SAFETY:` justification (alignment,
// validity, provenance, no-aliasing, no-double-free). The blob CONTENT is still parsed
// with SAFE slicing only (`get(..)` / `try_into` / `u64::from_le_bytes`) over the
// `&[u8]` the safe `str_blob()` accessor hands back. See [`Entry`].
// ===========================================================================

/// The byte offset of the key-length field is computed from the header + optional
/// TTL; these constants name the fixed-size pieces of the [`Entry::Str`] blob.
mod blob {
    /// Header: `[data_type:u8][encoding:u8][flags:u8]`.
    pub const HEADER_LEN: usize = 3;
    /// The TTL deadline (u64 little-endian milliseconds), present iff the
    /// [`FLAG_HAS_TTL`] bit is set in the flags byte.
    pub const TTL_LEN: usize = 8;
    /// The key-length prefix (u32 little-endian), always present.
    pub const KEYLEN_LEN: usize = 4;
    /// The flags byte bit: a TTL deadline u64 follows the header.
    pub const FLAG_HAS_TTL: u8 = 0b0000_0001;
    /// The S3-FIFO 2-bit promote frequency, packed into bits 1-2 of the flags byte
    /// (freq-in-object). Bit 0 is [`FLAG_HAS_TTL`]; bits 1-2 hold the 0..=3 frequency
    /// the eviction policy reads through the store's `VictimFreq` accessor. Packing it
    /// into the EXISTING flags byte adds NO bytes to the blob and needs NO new `unsafe`
    /// (the byte is read/written through the safe `str_blob()`/`str_blob_mut()` views).
    pub const FREQ_SHIFT: u8 = 1;
    /// The 2-bit mask for the freq field once shifted down (a value in 0..=3).
    pub const FREQ_MASK: u8 = 0b0000_0011;
}

/// Encode a [`DataType`] as the blob header's first byte.
fn data_type_to_u8(t: DataType) -> u8 {
    match t {
        DataType::String => 0,
        DataType::List => 1,
        DataType::Set => 2,
        DataType::Hash => 3,
        DataType::ZSet => 4,
        DataType::Stream => 5,
    }
}

/// Decode the blob header's first byte back into a [`DataType`].
fn data_type_from_u8(b: u8) -> DataType {
    match b {
        1 => DataType::List,
        2 => DataType::Set,
        3 => DataType::Hash,
        4 => DataType::ZSet,
        5 => DataType::Stream,
        // 0 (and any unexpected byte, defensively) is String.
        _ => DataType::String,
    }
}

/// Encode an [`Encoding`] as the blob header's second byte.
fn encoding_to_u8(e: Encoding) -> u8 {
    match e {
        Encoding::Int => 0,
        Encoding::EmbStr => 1,
        Encoding::Raw => 2,
        Encoding::ListPack => 3,
        Encoding::QuickList => 4,
        Encoding::IntSet => 5,
        Encoding::HashTable => 6,
        Encoding::SkipList => 7,
        Encoding::ListPackEx => 8,
    }
}

/// Decode the blob header's second byte back into an [`Encoding`].
fn encoding_from_u8(b: u8) -> Encoding {
    match b {
        0 => Encoding::Int,
        2 => Encoding::Raw,
        3 => Encoding::ListPack,
        4 => Encoding::QuickList,
        5 => Encoding::IntSet,
        6 => Encoding::HashTable,
        7 => Encoding::SkipList,
        8 => Encoding::ListPackEx,
        // 1 (and any unexpected byte, defensively) is EmbStr.
        _ => Encoding::EmbStr,
    }
}

/// The collection value held inside a [`CollEntry`]. The four collection structs
/// (`ListVal`/`HashVal`/`SetVal`/`ZSetVal`) are owned here UNCHANGED; the `CollEntry`
/// `Box` provides the single indirection (memory Round 1 boxed the values inside the
/// old `ValueRepr`; Round 3 moves that box up to the whole collection entry).
#[derive(Debug, Clone)]
pub enum CollVal {
    /// A LIST value (PR-5).
    List(ListVal),
    /// A HASH value (PR-6).
    Hash(HashVal),
    /// A SET value (PR-7).
    Set(SetVal),
    /// A ZSET value (PR-8).
    ZSet(ZSetVal),
}

impl CollVal {
    /// The data type this collection reports.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        match self {
            CollVal::List(_) => DataType::List,
            CollVal::Hash(_) => DataType::Hash,
            CollVal::Set(_) => DataType::Set,
            CollVal::ZSet(_) => DataType::ZSet,
        }
    }

    /// The encoding this collection reports (a pure function of its active repr).
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        match self {
            CollVal::List(l) => l.encoding(),
            CollVal::Hash(h) => h.encoding(),
            CollVal::Set(s) => s.encoding(),
            CollVal::ZSet(z) => z.encoding(),
        }
    }

    /// The sum of element byte lengths (the value side of accounting).
    #[must_use]
    pub fn element_bytes(&self) -> usize {
        match self {
            CollVal::List(l) => l.element_bytes(),
            CollVal::Hash(h) => h.element_bytes(),
            CollVal::Set(s) => s.element_bytes(),
            CollVal::ZSet(z) => z.element_bytes(),
        }
    }

    /// The element count.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            CollVal::List(l) => l.len(),
            CollVal::Hash(h) => h.len(),
            CollVal::Set(s) => s.len(),
            CollVal::ZSet(z) => z.len(),
        }
    }

    /// Whether the collection holds zero elements.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A COLLECTION table entry (memory Round 3): a small header, the key bytes, the
/// optional TTL deadline, and the boxed-up collection value. One `Box<CollEntry>`
/// behind the [`Entry::Coll`] arm. The key is stored INSIDE so the table's eq/hash
/// closures read it (no separate map key allocation).
#[derive(Debug, Clone)]
pub struct CollEntry {
    /// The eviction rank (2-bit S3-FIFO counter; RESERVED, mirrors the old
    /// `Header::eviction_rank`). Not load-bearing today (the policy owns the rank).
    pub eviction_rank: u8,
    /// The forkless-snapshot version stamp (#60; RESERVED, mirrors the old
    /// `Header::snapshot_version`).
    pub snapshot_version: u32,
    /// The absolute TTL deadline, if any.
    pub expire_at: Option<UnixMillis>,
    /// The key bytes (stored here so the table eq/hash closures can read them).
    pub key: Box<[u8]>,
    /// The collection value.
    pub value: CollVal,
}

/// One key/value table entry as a SINGLE allocation behind an 8-byte TAGGED-POINTER
/// slot (the `perf/tagged-slot` shrink of memory Round 3, OBJECT_LAYOUT.md #111). The
/// per-shard `hashbrown::HashTable` stores `Entry` directly and derives the key from
/// inside it.
///
/// ## Representation: an 8-byte tagged `NonNull<u8>` (was a 16-byte enum)
///
/// `Entry` is a SINGLE machine word: a `NonNull<u8>` whose LOW BIT is a tag.
///
/// - **Str** (low bit `0`): a MANUALLY allocated THIN blob. The old `Entry::Str` was a
///   `Box<[u8]>` FAT pointer (ptr + length = 16 bytes), so the enum was 16 bytes. Here
///   the length is moved INTO the allocation as a fixed `u32` prefix, leaving a THIN
///   one-word pointer. The blob is allocated with alignment 8 (>= 2), so the pointer is
///   always even and the low bit is free to tag. Layout:
///   `[u32 total_len][the Round-3 blob: type|enc|flags|ttl?|key_len|key|value]`.
///   `total_len` is the WHOLE allocation size (prefix included) so `Drop` can recover
///   the exact `Layout` to `dealloc`. All the existing blob parsing is preserved,
///   relocated behind this thin pointer at the fixed [`STR_BLOB_OFFSET`] (just past the
///   length prefix).
/// - **Coll** (low bit `1`): `Box::into_raw(Box<CollEntry>)`. A `CollEntry` has
///   alignment >= 2 (it contains `u32`/pointer fields), so its box pointer is even and
///   the low bit is free. We set the low bit to tag; on access we mask it off and
///   reconstruct `&CollEntry` / `&mut CollEntry`; on `Drop` we mask and `Box::from_raw`.
///
/// ## Slot size
///
/// `size_of::<Entry>()` is 8 bytes on a 64-bit target (one `NonNull<u8>`, no length
/// word, no discriminant — the tag lives in the pointer's low bit). The hashbrown
/// `HashTable<Entry>` slot is therefore 8 bytes plus the 1-byte control tag, HALF the
/// prior 16-byte enum, which roughly halves `table_bytes_per_key`. This keeps the
/// Round-3 win (one allocation per string key, no key duplication, value inline) and
/// adds the slot shrink.
///
/// ## `unsafe` discipline (this is the ONLY unsafe in the crate)
///
/// All `unsafe` in `ironcache-store` is confined to this `Entry` impl. Every `unsafe`
/// block carries a `// SAFETY:` comment. `Entry` owns a raw pointer (a Str blob it must
/// `dealloc`, or a `Box<CollEntry>` it must drop), with the same single-owner semantics
/// the old `enum { Box<[u8]>, Box<CollEntry> }` had. Holding a raw pointer makes `Entry`
/// auto-`!Send`/`!Sync`; that is CORRECT and intended (the per-shard store is owned by
/// one core, single-threaded, ADR-0002/0005 — it is never sent across threads), and no
/// `unsafe impl Send/Sync` is added.
pub struct Entry(NonNull<u8>);

/// The byte offset of the Round-3 blob inside a Str allocation: just past the `u32`
/// total-length prefix. The blob's own header/ttl/key/value parsing offsets are then
/// RELATIVE to a slice that starts here, so the existing `blob::*` constants and the
/// `str_*` parsers are reused verbatim against `str_blob()`.
const STR_BLOB_OFFSET: usize = 4;

/// The alignment of a Str blob allocation. MUST be `>= 2` so the low pointer bit is
/// always 0 and free for the tag; 8 also satisfies the `u32` length prefix's natural
/// alignment so the prefix read/write is aligned.
const STR_ALIGN: usize = 8;

/// The tag bit carried in [`Entry`]'s pointer low bit: `0` = Str thin blob, `1` = Coll
/// `Box<CollEntry>`.
const TAG_COLL: usize = 1;

// Compile-time guards for the tagged-pointer invariant: BOTH the Str blob and the
// `Box<CollEntry>` must be at least 2-aligned so the low bit is always free for the
// tag. These are `const` assertions (not `debug_assert!`, which is stripped in release
// where production runs), so a future change that broke either alignment (e.g. making
// `CollEntry` a 1-aligned type) fails the BUILD rather than silently corrupting the
// tag scheme at runtime.
const _: () = assert!(
    STR_ALIGN >= 2,
    "STR_ALIGN must be >= 2 so the Str blob pointer's low tag bit is always free"
);
const _: () = assert!(
    std::mem::align_of::<CollEntry>() >= 2,
    "CollEntry must be >= 2-aligned so the Coll pointer's low tag bit is always free"
);

impl Entry {
    /// Allocate a Str thin blob: a `u32` total-length prefix followed by the Round-3
    /// blob bytes, with alignment [`STR_ALIGN`] (so the pointer is even and tag-free).
    /// Returns an even (low-bit-0) `NonNull<u8>` to the start of the allocation.
    ///
    /// The blob bytes are produced by [`Self::build_str_blob_bytes`] (the SAFE layout
    /// builder); this function only does the manual allocation + copy + length prefix.
    fn alloc_str_blob(blob_bytes: &[u8]) -> NonNull<u8> {
        let total = STR_BLOB_OFFSET + blob_bytes.len();
        // The u32 length prefix MUST equal the real allocation size, because `Drop`
        // recovers the dealloc `Layout` from it: if the prefix were SATURATED (clamped to
        // u32::MAX) while the allocation is `total > u32::MAX` bytes, `dealloc` would run
        // with a size-mismatched Layout = undefined behavior. So this is a HARD invariant,
        // not a saturating cast. It is unreachable in practice: a single value is bounded
        // by proto-max-bulk-len (512 MB) and the only unbounded-growth command (APPEND)
        // rejects a result past that ceiling (see `cmd_append`), so `total` never
        // approaches 4 GiB. The `expect` is the soundness backstop for the manual-alloc
        // boundary: a panic on a >4 GiB blob is strictly better than heap corruption.
        let total_u32 = u32::try_from(total).expect(
            "Str blob total length must fit u32 (4 GiB); values are bounded by \
             proto-max-bulk-len (512 MB) and APPEND rejects larger, so this is unreachable",
        );
        // A Layout with size == total (>= STR_BLOB_OFFSET == 4, never zero) and align 8.
        // align 8 is a valid power-of-two; size is rounded-up implicitly by alloc.
        let layout = Layout::from_size_align(total, STR_ALIGN)
            .expect("Str blob layout: align 8 is valid and size is bounded");
        // SAFETY: `layout` has a non-zero size (total >= STR_BLOB_OFFSET == 4 > 0) and a
        // valid power-of-two alignment (8), the two requirements of `alloc`. The returned
        // pointer is either null (handled below via `handle_alloc_error`) or points to
        // `total` uninitialized, properly-aligned bytes that we exclusively own and fully
        // initialize before any read.
        let raw = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(raw) else {
            // Allocation failure: never return a dangling/null pointer.
            handle_alloc_error(layout);
        };
        // SAFETY: `ptr` points to `total` writable bytes we just allocated and uniquely
        // own. The two writes stay within `[0, total)`: the 4-byte length prefix occupies
        // `[0, 4)`, and the `blob_bytes` copy occupies `[4, 4 + blob_bytes.len()) ==
        // [STR_BLOB_OFFSET, total)`. `write_unaligned` is used for the prefix because the
        // `u32` is written through a `*mut u8` (byte pointer); align 8 actually makes it
        // aligned, but `write_unaligned` is correct regardless and avoids any aliasing
        // assumption. Source and destination of the copy do not overlap (a fresh alloc).
        unsafe {
            let len_bytes = total_u32.to_le_bytes();
            ptr.as_ptr().cast::<[u8; 4]>().write_unaligned(len_bytes);
            ptr.as_ptr()
                .add(STR_BLOB_OFFSET)
                .copy_from_nonoverlapping(blob_bytes.as_ptr(), blob_bytes.len());
        }
        // The alloc returned an 8-aligned pointer, so the low bit is 0 (the Str tag).
        debug_assert_eq!(
            ptr.as_ptr().addr() & TAG_COLL,
            0,
            "Str blob pointer must be even (align >= 2) so the tag bit is free"
        );
        ptr
    }

    /// Construct a Str [`Entry`] from already-built Round-3 blob bytes (the single place
    /// a Str pointer is tagged). The pointer is even out of [`Self::alloc_str_blob`], so
    /// the Str tag (low bit 0) needs no bit-set.
    fn str_from_blob_bytes(blob_bytes: &[u8]) -> Self {
        let ptr = Entry::alloc_str_blob(blob_bytes);
        // Low bit already 0 == Str; store the provenance-carrying pointer verbatim.
        Entry(ptr)
    }

    /// Construct a Coll [`Entry`] from a `Box<CollEntry>` (the single place a Coll
    /// pointer is tagged): take the box's raw pointer (even, since `CollEntry`'s align is
    /// at least 2) and SET the low bit via strict-provenance `map_addr` so the tagged
    /// pointer keeps the box's provenance.
    fn coll_from_box(b: Box<CollEntry>) -> Self {
        let raw: *mut CollEntry = Box::into_raw(b);
        debug_assert_eq!(
            raw.addr() & TAG_COLL,
            0,
            "CollEntry box pointer must be even (align >= 2) so the tag bit is free"
        );
        // Set the tag bit while PRESERVING provenance (`map_addr` keeps the original
        // allocation's provenance; a bare `as usize | 1` cast back to a pointer would
        // strip it). Cast to `*mut u8` first so the addr is the byte address.
        let tagged: *mut u8 = raw.cast::<u8>().map_addr(|a| a | TAG_COLL);
        // SAFETY: `raw` came from `Box::into_raw` so it is non-null; OR-ing a bit into a
        // non-null even address keeps it non-null (the high bits are unchanged), so the
        // `NonNull` invariant holds.
        Entry(unsafe { NonNull::new_unchecked(tagged) })
    }

    /// Whether this entry is the Coll arm (low tag bit set). A Str entry returns `false`.
    #[inline]
    fn is_coll(&self) -> bool {
        (self.0.as_ptr().addr() & TAG_COLL) == TAG_COLL
    }

    /// The Str blob (the Round-3 bytes WITHOUT the length prefix) as a shared slice, or
    /// `None` if this is a Coll entry. The returned slice borrows `self`.
    ///
    /// The slice is `[STR_BLOB_OFFSET, total_len)` of the allocation. This is the single
    /// place the manual-alloc layout is read back into a safe `&[u8]`; every Str parser
    /// (`str_flags`/`str_key_len`/`key`/`str_value_bytes`/...) goes through here.
    fn str_blob(&self) -> Option<&[u8]> {
        if self.is_coll() {
            return None;
        }
        let ptr = self.0.as_ptr();
        // SAFETY: this is a Str entry (low bit 0), so `self.0` points to the start of a
        // Str allocation we own: a `u32` LE total-length prefix at offset 0 followed by
        // the blob bytes. The prefix was written in `alloc_str_blob` and is always
        // initialized. We read it (unaligned-safe through a byte pointer) to recover the
        // allocation length, then form a slice over the blob region `[STR_BLOB_OFFSET,
        // total)`, which lies entirely within the allocation (`total` is the full size).
        // The borrow is tied to `&self`, so the allocation outlives the slice and is not
        // mutated through another path while the slice lives.
        let total = unsafe {
            let len_bytes = ptr.cast::<[u8; 4]>().read_unaligned();
            u32::from_le_bytes(len_bytes) as usize
        };
        let blob_len = total - STR_BLOB_OFFSET;
        // SAFETY: `ptr.add(STR_BLOB_OFFSET)` is in-bounds (STR_BLOB_OFFSET == 4 <= total),
        // `blob_len == total - STR_BLOB_OFFSET` bytes from there stay within the
        // allocation, the bytes were initialized by `alloc_str_blob`'s copy, and the
        // resulting `&[u8]` borrows `self` (so the allocation stays alive and un-aliased).
        let slice = unsafe { std::slice::from_raw_parts(ptr.add(STR_BLOB_OFFSET), blob_len) };
        Some(slice)
    }

    /// The Str blob as a MUTABLE slice (for the in-place TTL patch), or `None` for a Coll
    /// entry. Borrows `self` mutably so no shared `str_blob()` borrow can co-exist.
    fn str_blob_mut(&mut self) -> Option<&mut [u8]> {
        if self.is_coll() {
            return None;
        }
        let ptr = self.0.as_ptr();
        // SAFETY: Str entry; same length-prefix recovery as `str_blob`. See its SAFETY.
        let total = unsafe {
            let len_bytes = ptr.cast::<[u8; 4]>().read_unaligned();
            u32::from_le_bytes(len_bytes) as usize
        };
        let blob_len = total - STR_BLOB_OFFSET;
        // SAFETY: same in-bounds + initialized reasoning as `str_blob`, but `&mut self`
        // guarantees this is the UNIQUE borrow of the allocation for the slice's lifetime,
        // so a `&mut [u8]` is sound (no aliasing). The slice covers `[STR_BLOB_OFFSET,
        // total)` of the allocation we own.
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr.add(STR_BLOB_OFFSET), blob_len) };
        Some(slice)
    }

    /// The untagged `*mut CollEntry` behind a Coll entry: clear the low tag bit
    /// (provenance-preserving) to recover the exact pointer `Box::into_raw` returned. The
    /// caller MUST have checked [`Self::is_coll`]; on a Str entry this would mis-cast.
    ///
    /// `cast_ptr_alignment` is allowed here with justification: the byte pointer's
    /// ADDRESS is `Box<CollEntry>`'s original address (the box was correctly `CollEntry`-
    /// aligned, align >= 2, and we only toggled bit 0 then cleared it), so the recovered
    /// `*mut CollEntry` is properly aligned. The lint fires on the static `u8 -> CollEntry`
    /// cast and cannot see the runtime alignment invariant.
    #[allow(clippy::cast_ptr_alignment)]
    fn coll_ptr(&self) -> *mut CollEntry {
        // Clear the tag bit while PRESERVING provenance (`map_addr`), then re-type.
        self.0
            .as_ptr()
            .map_addr(|a| a & !TAG_COLL)
            .cast::<CollEntry>()
    }

    /// The `&CollEntry` behind a Coll entry, or `None` for a Str entry. Borrows `self`.
    fn coll_ref(&self) -> Option<&CollEntry> {
        if !self.is_coll() {
            return None;
        }
        let ce = self.coll_ptr();
        // SAFETY: this is a Coll entry, so `self.0` is a tagged `Box<CollEntry>` pointer
        // produced by `coll_from_box`. `coll_ptr` masks off the low tag bit and recovers
        // the exact, properly-aligned, non-null pointer `Box::into_raw` returned, which
        // points to a live, initialized `CollEntry` we own. The reference borrows `&self`,
        // so the box outlives it and is not mutably aliased for its lifetime.
        Some(unsafe { &*ce })
    }

    /// The `&mut CollEntry` behind a Coll entry, or `None` for a Str entry. Borrows
    /// `self` mutably (unique access).
    fn coll_mut(&mut self) -> Option<&mut CollEntry> {
        if !self.is_coll() {
            return None;
        }
        let ce = self.coll_ptr();
        // SAFETY: as in `coll_ref()`, `coll_ptr` masks the tag and recovers the live
        // `Box<CollEntry>` pointer. `&mut self` makes this the unique borrow, so handing
        // out `&mut CollEntry` is sound (no aliasing) for the borrow's lifetime.
        Some(unsafe { &mut *ce })
    }

    /// Assemble the Round-3 Str blob BYTES (header, optional TTL, key-len, key, value).
    /// SAFE: pure `Vec<u8>` construction, identical layout to the prior `build_str_blob`.
    /// The thin-pointer allocation (length prefix + this) happens in
    /// [`Self::alloc_str_blob`]; this is just the byte layout.
    fn build_str_blob_bytes(
        data_type: DataType,
        encoding: Encoding,
        expire_at: Option<UnixMillis>,
        key: &[u8],
        value_bytes: &[u8],
    ) -> Vec<u8> {
        let has_ttl = expire_at.is_some();
        let ttl_len = if has_ttl { blob::TTL_LEN } else { 0 };
        let total = blob::HEADER_LEN + ttl_len + blob::KEYLEN_LEN + key.len() + value_bytes.len();
        let mut buf = Vec::with_capacity(total);
        // Header: type, encoding, flags.
        buf.push(data_type_to_u8(data_type));
        buf.push(encoding_to_u8(encoding));
        buf.push(if has_ttl { blob::FLAG_HAS_TTL } else { 0 });
        // Optional TTL deadline (u64 LE).
        if let Some(UnixMillis(deadline)) = expire_at {
            buf.extend_from_slice(&deadline.to_le_bytes());
        }
        // Key length (u32 LE) + key bytes. A key longer than u32::MAX is not
        // representable; Redis keys are bounded far below this, and the command
        // layer rejects oversize keys, so the cast is safe in practice. Saturate
        // defensively rather than wrap.
        let key_len = u32::try_from(key.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(key);
        // Value bytes (the rest of the blob).
        buf.extend_from_slice(value_bytes);
        buf
    }

    /// Assemble a Str [`Entry`] from its parts (the single write site for the Str blob
    /// layout). `value_bytes` are the bytes stored after the key: for int the canonical
    /// decimal digits, for embstr/raw the string bytes.
    fn build_str_blob(
        data_type: DataType,
        encoding: Encoding,
        expire_at: Option<UnixMillis>,
        key: &[u8],
        value_bytes: &[u8],
    ) -> Self {
        let bytes = Entry::build_str_blob_bytes(data_type, encoding, expire_at, key, value_bytes);
        Entry::str_from_blob_bytes(&bytes)
    }

    /// Build a STRING entry from a key and an already-[`classify`]d value.
    #[must_use]
    pub fn str_from_classified(
        key: &[u8],
        classified: Classified,
        bytes: &[u8],
        expire_at: Option<UnixMillis>,
    ) -> Self {
        match classified {
            Classified::Int(n) => Entry::str_from_int(key, n, expire_at),
            Classified::EmbStr => {
                Entry::build_str_blob(DataType::String, Encoding::EmbStr, expire_at, key, bytes)
            }
            Classified::Raw => {
                Entry::build_str_blob(DataType::String, Encoding::Raw, expire_at, key, bytes)
            }
        }
    }

    /// Build a STRING entry from a key and raw value bytes, classifying the encoding.
    #[must_use]
    pub fn str_from_bytes(key: &[u8], bytes: &[u8], expire_at: Option<UnixMillis>) -> Self {
        Entry::str_from_classified(key, classify(bytes), bytes, expire_at)
    }

    /// Build an int-encoded STRING entry: the value bytes are the canonical decimal
    /// digits (so the read path borrows them; `OBJECT ENCODING` reports `int`).
    #[must_use]
    pub fn str_from_int(key: &[u8], n: i64, expire_at: Option<UnixMillis>) -> Self {
        let mut buf = [0u8; 20];
        let digits = format_i64(n, &mut buf);
        Entry::build_str_blob(DataType::String, Encoding::Int, expire_at, key, digits)
    }

    /// Build a COLLECTION entry from a key and a [`CollVal`].
    #[must_use]
    pub fn coll(key: &[u8], value: CollVal, expire_at: Option<UnixMillis>) -> Self {
        Entry::coll_from_box(Box::new(CollEntry {
            eviction_rank: 0,
            snapshot_version: 0,
            expire_at,
            key: key.to_vec().into_boxed_slice(),
            value,
        }))
    }

    /// Build an entry from a key and an owned RMW write value ([`NewValueOwned`]). `thresholds`
    /// carries the LIVE collection-encoding caps (#40) a freshly-built collection's transition is
    /// evaluated against (the store passes its live snapshot; a reconstruction path passes
    /// [`EncodingThresholds::unlimited`] and forces the recorded encoding). See
    /// [`KvObj::from_new_owned`].
    #[must_use]
    pub fn from_new_owned(
        key: &[u8],
        value: NewValueOwned,
        expire_at: Option<UnixMillis>,
        thresholds: &EncodingThresholds,
    ) -> Self {
        match value {
            NewValueOwned::Int(n) => Entry::str_from_int(key, n, expire_at),
            NewValueOwned::Bytes(b) => Entry::str_from_bytes(key, &b, expire_at),
            NewValueOwned::List(elems) => {
                let mut list = ListVal::new();
                for e in &elems {
                    list.push_back(e, thresholds);
                }
                Entry::coll(key, CollVal::List(list), expire_at)
            }
            NewValueOwned::Hash(pairs) => {
                let mut hash = HashVal::new();
                for (f, v) in &pairs {
                    hash.set(f, v, thresholds);
                }
                Entry::coll(key, CollVal::Hash(hash), expire_at)
            }
            // The HSETEX create-on-missing path (#408): build the hash, then attach each field's
            // TTL so a brand-new key is created already carrying field expirations.
            NewValueOwned::HashEx(pairs, ttls) => {
                let mut hash = HashVal::new();
                for (f, v) in &pairs {
                    hash.set(f, v, thresholds);
                }
                for (f, deadline) in &ttls {
                    if hash.contains(f) {
                        hash.set_field_ttl(f, *deadline);
                    }
                }
                Entry::coll(key, CollVal::Hash(hash), expire_at)
            }
            NewValueOwned::Set(members) => Entry::coll(
                key,
                CollVal::Set(SetVal::from_members(&members, thresholds)),
                expire_at,
            ),
            NewValueOwned::ZSet(pairs) => Entry::coll(
                key,
                CollVal::ZSet(ZSetVal::from_pairs(&pairs, thresholds)),
                expire_at,
            ),
        }
    }

    /// Build an `Entry` from a fully-formed [`KvObj`] (the `insert_object` / move
    /// paths; the `KvObj` is the public builder/transfer type the tests construct).
    #[must_use]
    pub fn from_kvobj(obj: KvObj) -> Self {
        let KvObj {
            header,
            key,
            value,
            expire_at,
        } = obj;
        match value {
            ValueRepr::Int(n) => Entry::str_from_int(&key, n, expire_at),
            // The embstr-vs-raw distinction lives in the HEADER encoding, not the
            // variant, so honor `header.encoding` when laying down the blob.
            ValueRepr::Inline(b) | ValueRepr::Raw(b) => {
                Entry::build_str_blob(header.data_type, header.encoding, expire_at, &key, &b)
            }
            ValueRepr::List(l) => Entry::coll(&key, CollVal::List(*l), expire_at),
            ValueRepr::Hash(h) => Entry::coll(&key, CollVal::Hash(*h), expire_at),
            ValueRepr::Set(s) => Entry::coll(&key, CollVal::Set(*s), expire_at),
            ValueRepr::ZSet(z) => Entry::coll(&key, CollVal::ZSet(*z), expire_at),
        }
    }

    /// Reconstruct the public [`KvObj`] transfer type from this stored entry (HA-5b #60:
    /// the REVERSE of [`Self::from_kvobj`]). This is the OWNED, borrow-free per-entry
    /// representation the forkless SNAPSHOT iterator emits: it carries everything a replica
    /// needs to replay the write (the data type + encoding in the header, the key bytes, the
    /// value -- string bytes or a deep-cloned collection -- and the TTL deadline). A replica
    /// re-applies it through [`ShardStore::insert_object`](crate::ShardStore::insert_object),
    /// which round-trips it back to an `Entry`.
    ///
    /// READ-ONLY over the existing blob/`CollEntry` fields: it adds NO per-key bytes and
    /// does NOT touch the hot write path. The value is reconstructed FAITHFULLY: an
    /// int-encoded string yields [`ValueRepr::Int`] (parsed from its canonical decimal
    /// digits), an embstr/raw string yields [`ValueRepr::Inline`]/[`ValueRepr::Raw`] per the
    /// stored encoding, and a collection yields the matching boxed [`ValueRepr`] variant by
    /// DEEP-CLONING the stored `CollVal` (the only allocation on the snapshot path, bounded
    /// per chunk). The reserved [`Header::snapshot_version`] is left at its default -- it is
    /// not load-bearing for the snapshot (correctness comes from the HA-5a stream, not a
    /// per-key version; see [`ShardStore::snapshot_chunk`](crate::ShardStore::snapshot_chunk)).
    #[must_use]
    pub fn to_kvobj(&self) -> KvObj {
        let data_type = self.data_type();
        let encoding = self.encoding();
        let expire_at = self.expire_at();
        let key = self.key().to_vec().into_boxed_slice();
        let value = if let Some(c) = self.coll_ref() {
            // Deep-clone the collection value into the matching boxed ValueRepr variant.
            match &c.value {
                CollVal::List(l) => ValueRepr::List(Box::new(l.clone())),
                CollVal::Hash(h) => ValueRepr::Hash(Box::new(h.clone())),
                CollVal::Set(s) => ValueRepr::Set(Box::new(s.clone())),
                CollVal::ZSet(z) => ValueRepr::ZSet(Box::new(z.clone())),
            }
        } else {
            let bytes = self.str_value_bytes();
            match encoding {
                // An int-encoded string stores its canonical decimal digits; parse them
                // back to the i64 so the replica stores it int-encoded too. The digits are
                // always valid (they were produced by `format_i64`), so the parse cannot
                // realistically fail; fall back to a Raw byte copy if it ever did, which is
                // still a faithful value (the replica re-classifies it).
                Encoding::Int => std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .map_or_else(
                        || ValueRepr::Raw(bytes.to_vec().into_boxed_slice()),
                        ValueRepr::Int,
                    ),
                Encoding::Raw => ValueRepr::Raw(bytes.to_vec().into_boxed_slice()),
                // EmbStr (and any other string encoding, defensively) is inline.
                _ => ValueRepr::Inline(bytes.to_vec().into_boxed_slice()),
            }
        };
        let header = Header::with_type(data_type, encoding, expire_at.is_some());
        KvObj {
            header,
            key,
            value,
            expire_at,
        }
    }

    // -- STRING blob parsing (SAFE slicing only) --

    /// The flags byte of a Str blob.
    fn str_flags(blob: &[u8]) -> u8 {
        blob.get(2).copied().unwrap_or(0)
    }

    /// The byte offset where the key-length field begins (after the header and the
    /// optional TTL).
    fn str_keylen_offset(blob: &[u8]) -> usize {
        if Entry::str_flags(blob) & blob::FLAG_HAS_TTL != 0 {
            blob::HEADER_LEN + blob::TTL_LEN
        } else {
            blob::HEADER_LEN
        }
    }

    /// The key length stored in a Str blob.
    fn str_key_len(blob: &[u8]) -> usize {
        let off = Entry::str_keylen_offset(blob);
        blob.get(off..off + blob::KEYLEN_LEN)
            .and_then(|s| s.try_into().ok())
            .map_or(0, |a: [u8; 4]| u32::from_le_bytes(a) as usize)
    }

    /// The byte offset where the key bytes begin in a Str blob.
    fn str_key_offset(blob: &[u8]) -> usize {
        Entry::str_keylen_offset(blob) + blob::KEYLEN_LEN
    }

    /// The KEY bytes of this entry (used by the table's eq/hash closures and the
    /// eviction/SCAN hooks). For a `Coll` this is the stored `CollEntry.key`.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        if let Some(blob) = self.str_blob() {
            let off = Entry::str_key_offset(blob);
            let klen = Entry::str_key_len(blob);
            blob.get(off..off + klen).unwrap_or(&[])
        } else {
            // Coll: the key is stored inside the CollEntry.
            self.coll_ref().map_or(&[], |c| &c.key)
        }
    }

    /// The VALUE bytes of a STRING entry (decimal digits for int, the string bytes
    /// for embstr/raw). Returns an empty slice for a collection entry (which has no
    /// byte-readable value, matching the old `view_of` for a collection).
    #[must_use]
    pub fn str_value_bytes(&self) -> &[u8] {
        if let Some(blob) = self.str_blob() {
            let val_off = Entry::str_key_offset(blob) + Entry::str_key_len(blob);
            blob.get(val_off..).unwrap_or(&[])
        } else {
            &[]
        }
    }

    /// The logical data type (for TYPE / WRONGTYPE / the SCAN type filter).
    #[must_use]
    pub fn data_type(&self) -> DataType {
        if let Some(blob) = self.str_blob() {
            data_type_from_u8(blob.first().copied().unwrap_or(0))
        } else {
            self.coll_ref()
                .map_or(DataType::String, |c| c.value.data_type())
        }
    }

    /// The internal encoding (for `OBJECT ENCODING`). For a Str entry it is read
    /// straight from the header byte; for a Coll it is the live pure-function repr.
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        if let Some(blob) = self.str_blob() {
            encoding_from_u8(blob.get(1).copied().unwrap_or(1))
        } else {
            self.coll_ref()
                .map_or(Encoding::EmbStr, |c| c.value.encoding())
        }
    }

    /// The absolute TTL deadline, if any.
    #[must_use]
    pub fn expire_at(&self) -> Option<UnixMillis> {
        if let Some(blob) = self.str_blob() {
            if Entry::str_flags(blob) & blob::FLAG_HAS_TTL != 0 {
                blob.get(blob::HEADER_LEN..blob::HEADER_LEN + blob::TTL_LEN)
                    .and_then(|s| s.try_into().ok())
                    .map(|a: [u8; 8]| UnixMillis(u64::from_le_bytes(a)))
            } else {
                None
            }
        } else {
            self.coll_ref().and_then(|c| c.expire_at)
        }
    }

    /// Overwrite this entry's TTL deadline in place (EXPIRE/PERSIST/KEEPTTL on an
    /// otherwise-untouched value). For a Str entry a TTL add/remove changes the blob
    /// LENGTH (the 8-byte deadline field appears/disappears), so the blob is rebuilt;
    /// a deadline-only change to an already-TTL'd blob is patched in place. For a
    /// Coll entry it is a plain field write.
    pub fn set_expire_at(&mut self, expire_at: Option<UnixMillis>) {
        if let Some(c) = self.coll_mut() {
            c.expire_at = expire_at;
            return;
        }
        // Str entry. Read the current TTL-present flag through the shared blob view.
        let had_ttl = self
            .str_blob()
            .is_some_and(|b| Entry::str_flags(b) & blob::FLAG_HAS_TTL != 0);
        match (had_ttl, expire_at) {
            // Patch the existing TTL field in place (same blob length) via the mutable
            // blob view (no realloc).
            (true, Some(UnixMillis(deadline))) => {
                if let Some(blob) = self.str_blob_mut() {
                    let bytes = deadline.to_le_bytes();
                    for (i, b) in bytes.iter().enumerate() {
                        blob[blob::HEADER_LEN + i] = *b;
                    }
                }
            }
            // No change (no TTL before, none after).
            (false, None) => {}
            // Add or remove the TTL field: the blob length changes, so rebuild (the old
            // allocation is freed when the old `self` is dropped by the assignment).
            _ => {
                let data_type = self.data_type();
                let encoding = self.encoding();
                // Carry the S3-FIFO freq across the rebuild: this is a TTL-only change to
                // the SAME value, not a fresh write, so the promote frequency must survive.
                let freq = self.freq();
                // Re-extract key + value from the OLD blob before rebuilding.
                let key = self.key().to_vec();
                let value = self.str_value_bytes().to_vec();
                *self = Entry::build_str_blob(data_type, encoding, expire_at, &key, &value);
                self.set_freq(freq);
            }
        }
    }

    /// Whether this entry's TTL deadline has strictly passed at `now` (the lazy
    /// backstop predicate; `now > deadline`, alive at `now == deadline`).
    #[must_use]
    pub fn is_expired(&self, now: UnixMillis) -> bool {
        match self.expire_at() {
            Some(deadline) => now > deadline,
            None => false,
        }
    }

    /// The maximum S3-FIFO 2-bit promote frequency (capped at 3, ADR-0008).
    const MAX_FREQ: u8 = 3;

    /// The S3-FIFO 2-bit promote frequency of this entry (0..=3), the freq-in-object
    /// counter the eviction policy reads through the store's `VictimFreq` accessor.
    ///
    /// For a Str entry it is bits 1-2 of the flags byte (read via the safe `str_blob()`
    /// view; bit 0 is the TTL-present flag). For a Coll entry it is the
    /// `CollEntry.eviction_rank` field, masked to 2 bits.
    #[must_use]
    pub fn freq(&self) -> u8 {
        if let Some(blob) = self.str_blob() {
            (Entry::str_flags(blob) >> blob::FREQ_SHIFT) & blob::FREQ_MASK
        } else {
            self.coll_ref()
                .map_or(0, |c| c.eviction_rank & blob::FREQ_MASK)
        }
    }

    /// Set the S3-FIFO 2-bit promote frequency (capped at [`Self::MAX_FREQ`]). For a Str
    /// entry it patches bits 1-2 of the flags byte in place through the safe
    /// `str_blob_mut()` view (no realloc, no layout change, no new `unsafe`); for a Coll
    /// entry it writes `CollEntry.eviction_rank`.
    pub fn set_freq(&mut self, f: u8) {
        let f = f.min(Entry::MAX_FREQ);
        if let Some(c) = self.coll_mut() {
            c.eviction_rank = f;
            return;
        }
        if let Some(blob) = self.str_blob_mut() {
            // Clear the existing freq bits (1-2), then OR in the new value, leaving the
            // TTL-present bit (0) and any high bits untouched. The flags byte is at the
            // fixed blob offset 2 (after data_type and encoding).
            let flags = blob[2];
            blob[2] = (flags & !(blob::FREQ_MASK << blob::FREQ_SHIFT)) | (f << blob::FREQ_SHIFT);
        }
    }

    /// Bump the S3-FIFO 2-bit promote frequency by one, saturating at
    /// [`Self::MAX_FREQ`] (the read-path hot bump: the store calls this on the
    /// just-accessed entry, so the freq lives with the object and `on_access` needs no
    /// policy lookup).
    pub fn bump_freq(&mut self) {
        let next = (self.freq() + 1).min(Entry::MAX_FREQ);
        self.set_freq(next);
    }

    /// Decrement the S3-FIFO 2-bit promote frequency by one, saturating at 0 (the
    /// main-queue second-chance step, driven by the policy through `VictimFreq::dec`).
    pub fn dec_freq(&mut self) {
        let cur = self.freq();
        self.set_freq(cur.saturating_sub(1));
    }

    /// The logical byte length of the value (STRLEN basis).
    #[must_use]
    pub fn logical_len(&self) -> usize {
        if let Some(c) = self.coll_ref() {
            c.value.element_bytes()
        } else {
            self.str_value_bytes().len()
        }
    }

    /// The accounting weight: key bytes + value logical bytes. Identical model to the
    /// old `KvObj::accounted_bytes` (the `used_memory` counter and the accounting
    /// tests rely on this exact figure).
    #[must_use]
    pub fn accounted_bytes(&self) -> usize {
        self.key().len() + self.logical_len()
    }

    /// Whether this entry is a COLLECTION (list/hash/set/zset).
    #[must_use]
    pub fn is_collection(&self) -> bool {
        self.is_coll()
    }

    /// The element count IF this is a collection, else `None`.
    #[must_use]
    pub fn collection_len(&self) -> Option<usize> {
        self.coll_ref().map(|c| c.value.len())
    }

    /// Whether this is a COLLECTION holding zero elements (the empty-collection
    /// -deletes-key backstop check, by element count).
    #[must_use]
    pub fn is_empty_collection(&self) -> bool {
        self.collection_len() == Some(0)
    }

    /// Recompute and store the encoding for a COLLECTION entry from its current repr
    /// (after an in-place edit a list may cross listpack->quicklist, etc.). A no-op
    /// for a Str entry (its encoding is fixed at write time and patched by
    /// `set_value_bytes`). Mirrors the old `KvObj::recompute_encoding`.
    pub fn recompute_encoding(&mut self) {
        // The Coll encoding is a PURE FUNCTION of the live value (read via
        // `c.value.encoding()`), so there is nothing cached to update: `encoding()`
        // already reflects the post-edit repr; a Str entry's encoding is fixed at write
        // time. An explicit no-op, kept so the store's call site reads the same as the
        // old KvObj path.
    }

    /// A mutable borrow of the stored collection value as a `&mut ListVal`, or `None`
    /// if this entry is not a list.
    pub fn as_list_mut(&mut self) -> Option<&mut ListVal> {
        match self.coll_mut()?.value {
            CollVal::List(ref mut l) => Some(l),
            _ => None,
        }
    }

    /// A mutable borrow of the stored HASH value, or `None`.
    pub fn as_hash_mut(&mut self) -> Option<&mut HashVal> {
        match self.coll_mut()?.value {
            CollVal::Hash(ref mut h) => Some(h),
            _ => None,
        }
    }

    /// A mutable borrow of the stored SET value, or `None`.
    pub fn as_set_mut(&mut self) -> Option<&mut SetVal> {
        match self.coll_mut()?.value {
            CollVal::Set(ref mut s) => Some(s),
            _ => None,
        }
    }

    /// A mutable borrow of the stored ZSET value, or `None`.
    pub fn as_zset_mut(&mut self) -> Option<&mut ZSetVal> {
        match self.coll_mut()?.value {
            CollVal::ZSet(ref mut z) => Some(z),
            _ => None,
        }
    }

    /// A mutable borrow of the whole stored [`CollVal`] (the collection value union),
    /// or `None` for a Str entry. The store's `rmw_mut` type-dispatch matches on this in
    /// ONE place to build the typed mutable view, replacing the prior `match obj {
    /// Entry::Coll(c) => &mut c.value, .. }` now that `Entry` is an opaque tagged pointer
    /// (the variant is no longer a public field). Borrows `self` mutably (unique access).
    pub fn as_coll_val_mut(&mut self) -> Option<&mut CollVal> {
        self.coll_mut().map(|c| &mut c.value)
    }

    /// Re-key this entry to `new_key` (the RENAME/MOVE/COPY relocation; the value
    /// object is preserved INTACT with its encoding + remaining TTL). For a Str entry
    /// the key is INSIDE the blob, so the blob is rebuilt with the new key; for a Coll
    /// entry it is a field write.
    pub fn rekey(&mut self, new_key: &[u8]) {
        if let Some(c) = self.coll_mut() {
            c.key = new_key.to_vec().into_boxed_slice();
            return;
        }
        let data_type = self.data_type();
        let encoding = self.encoding();
        let expire_at = self.expire_at();
        // The value object is preserved INTACT (encoding + remaining TTL), so the
        // S3-FIFO freq travels with it across the rebuild rather than resetting to 0.
        let freq = self.freq();
        let value = self.str_value_bytes().to_vec();
        *self = Entry::build_str_blob(data_type, encoding, expire_at, new_key, &value);
        self.set_freq(freq);
    }
}

impl Drop for Entry {
    /// Free the owned allocation: a Coll reconstructs and drops the `Box<CollEntry>`; a
    /// Str recovers the `Layout` from the total-length prefix and `dealloc`s. Exactly one
    /// of the two paths runs per `Entry`, so there is NO double-free and NO leak.
    fn drop(&mut self) {
        if self.is_coll() {
            // Coll: mask the tag and reconstruct the original `Box<CollEntry>` so its
            // own Drop frees the box (and recursively the collection contents).
            let ce = self.coll_ptr();
            // SAFETY: this Entry owns a Coll pointer produced by `coll_from_box`
            // (`Box::into_raw` + low-bit tag). `coll_ptr` masks the tag and recovers the
            // EXACT pointer `into_raw` returned, so `Box::from_raw` reconstitutes the
            // original box with matching allocator/layout. `drop` runs at most once per
            // value (Drop contract), so the box is freed exactly once: no double-free, no
            // leak.
            unsafe {
                drop(Box::from_raw(ce));
            }
        } else {
            // Str: read the total-length prefix to rebuild the exact allocation Layout,
            // then dealloc the whole block.
            let ptr = self.0.as_ptr();
            // SAFETY: Str entry (low bit 0); `self.0` points at the start of a Str
            // allocation whose first 4 bytes are the LE `u32` total length written by
            // `alloc_str_blob`. Reading them (unaligned-safe) recovers the original
            // allocation size.
            let total = unsafe {
                let len_bytes = ptr.cast::<[u8; 4]>().read_unaligned();
                u32::from_le_bytes(len_bytes) as usize
            };
            // The Layout MUST match the one `alloc_str_blob` used: size == total, align
            // == STR_ALIGN. `expect` cannot fire (the same args succeeded at alloc time).
            let layout = Layout::from_size_align(total, STR_ALIGN)
                .expect("Str blob layout matches the allocation-time layout");
            // SAFETY: `ptr` was returned by `alloc(layout)` in `alloc_str_blob` with this
            // exact `layout` (same size recovered from the prefix, same STR_ALIGN), and is
            // still live and owned by this Entry. `dealloc` runs once per value (Drop
            // contract), so the block is freed exactly once.
            unsafe {
                dealloc(ptr, layout);
            }
        }
    }
}

impl Clone for Entry {
    /// A DEEP clone: a Coll clones its `CollEntry` (re-boxed + re-tagged); a Str copies
    /// its blob bytes into a fresh thin allocation. Each clone owns a DISTINCT allocation,
    /// so the two `Entry`s drop independently (no shared ownership, no double-free). The
    /// only `.clone()` caller is the MOVE/COPY path (`find(..).cloned()`), which needs an
    /// owned copy; the hot read/write paths never clone an `Entry`.
    fn clone(&self) -> Self {
        if let Some(c) = self.coll_ref() {
            // Deep-clone the CollEntry (its derived Clone clones the key + collection
            // value), re-box, and re-tag.
            Entry::coll_from_box(Box::new(c.clone()))
        } else {
            // Copy the blob bytes into a fresh thin allocation (the length prefix is
            // re-derived from the byte length, so the clone is layout-identical).
            let blob = self.str_blob().unwrap_or(&[]);
            Entry::str_from_blob_bytes(blob)
        }
    }
}

impl std::fmt::Debug for Entry {
    /// A structural debug view that reads through the tagged pointer (the raw pointer
    /// itself is not informative). Mirrors what the old `#[derive(Debug)]` enum showed:
    /// the arm plus the key/type/encoding/ttl, so `ShardStore`'s derived `Debug` (which
    /// formats the `HashTable<Entry>`) stays readable.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(c) = self.coll_ref() {
            f.debug_struct("Entry::Coll")
                .field("key", &c.key)
                .field("value", &c.value)
                .field("expire_at", &c.expire_at)
                .finish()
        } else {
            f.debug_struct("Entry::Str")
                .field("key", &self.key())
                .field("data_type", &self.data_type())
                .field("encoding", &self.encoding())
                .field("expire_at", &self.expire_at())
                .finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The DEFAULT collection-encoding thresholds (the compiled Redis defaults) the unit tests
    /// exercise the conversion ladder against. The runtime-settable behavior (a LOWERED threshold
    /// converting a NEW collection sooner) is covered by the dedicated `encoding_threshold_*` tests
    /// at the end of this module + the registry/runtime tests in `ironcache-config`.
    const TH: &EncodingThresholds = &EncodingThresholds::defaults();

    #[test]
    fn hash_field_ttl_set_read_persist_and_reap() {
        let mut h = HashVal::new();
        h.set(b"a", b"1", TH);
        h.set(b"b", b"2", TH);
        h.set(b"c", b"3", TH);
        // No field TTLs yet: zero-overhead form, encoding unchanged.
        assert!(!h.has_field_ttls());
        assert_eq!(h.field_ttl(b"a"), None);
        assert_eq!(h.min_field_ttl(), None);
        assert_eq!(h.encoding(), Encoding::ListPack);

        // Set deadlines on two fields.
        h.set_field_ttl(b"a", UnixMillis(100));
        h.set_field_ttl(b"b", UnixMillis(50));
        assert!(h.has_field_ttls());
        assert_eq!(h.field_ttl(b"a"), Some(UnixMillis(100)));
        assert_eq!(h.field_ttl(b"b"), Some(UnixMillis(50)));
        assert_eq!(h.field_ttl(b"c"), None);
        // The nearest deadline drives the wheel registration.
        assert_eq!(h.min_field_ttl(), Some(UnixMillis(50)));

        // PERSIST removes one deadline; the field stays.
        assert!(h.persist_field(b"a"));
        assert_eq!(h.field_ttl(b"a"), None);
        assert!(h.contains(b"a"));
        assert_eq!(h.min_field_ttl(), Some(UnixMillis(50)));

        // Reaping at now=49 removes nothing (b's deadline 50 is still future under `<= now`).
        assert!(h.reap_expired_fields(UnixMillis(49)).is_empty());
        // At now=50, b is reaped (deadline <= now): the field AND its deadline are gone.
        let reaped = h.reap_expired_fields(UnixMillis(50));
        assert_eq!(reaped, vec![b"b".to_vec()]);
        assert!(!h.contains(b"b"));
        assert_eq!(h.field_ttl(b"b"), None);
        // The last TTL is gone, so the side-map is freed back to the zero-overhead form.
        assert!(!h.has_field_ttls());
        assert_eq!(h.min_field_ttl(), None);

        // del() also drops a field's deadline (no orphan deadline survives).
        h.set_field_ttl(b"a", UnixMillis(200));
        assert!(h.has_field_ttls());
        assert!(h.del(b"a"));
        assert!(!h.has_field_ttls());
    }

    #[test]
    fn int_value_has_no_alloc_and_reports_int() {
        let o = KvObj::from_bytes(b"k", b"12345", None);
        assert!(matches!(o.value, ValueRepr::Int(12345)));
        assert_eq!(o.header.encoding, Encoding::Int);
        // STRLEN of an int is the decimal length.
        assert_eq!(o.logical_len(), 5);
    }

    #[test]
    fn ten_byte_string_is_inline_embstr() {
        let o = KvObj::from_bytes(b"k", b"abcdefghij", None);
        assert!(matches!(o.value, ValueRepr::Inline(_)));
        assert_eq!(o.header.encoding, Encoding::EmbStr);
        assert_eq!(o.logical_len(), 10);
    }

    #[test]
    fn hundred_byte_string_is_raw() {
        let big = vec![b'z'; 100];
        let o = KvObj::from_bytes(b"k", &big, None);
        assert!(matches!(o.value, ValueRepr::Raw(_)));
        assert_eq!(o.header.encoding, Encoding::Raw);
        assert_eq!(o.logical_len(), 100);
    }

    #[test]
    fn int_decimal_materialization_matches() {
        assert_eq!(&int_decimal_bytes(0)[..], b"0");
        assert_eq!(&int_decimal_bytes(12345)[..], b"12345");
        assert_eq!(&int_decimal_bytes(-12345)[..], b"-12345");
        assert_eq!(&int_decimal_bytes(i64::MAX)[..], b"9223372036854775807");
        assert_eq!(&int_decimal_bytes(i64::MIN)[..], b"-9223372036854775808");
        assert_eq!(int_decimal_len(-12345), 6);
        assert_eq!(int_decimal_len(i64::MIN), 20);
    }

    #[test]
    fn set_value_bytes_reclassifies_encoding() {
        let mut o = KvObj::from_bytes(b"k", b"short", None);
        assert_eq!(o.header.encoding, Encoding::EmbStr);
        o.set_value_bytes(b"42");
        assert_eq!(o.header.encoding, Encoding::Int);
        assert!(matches!(o.value, ValueRepr::Int(42)));
        let big = vec![b'q'; 100];
        o.set_value_bytes(&big);
        assert_eq!(o.header.encoding, Encoding::Raw);
    }

    #[test]
    fn ttl_present_flag_tracks_expire_at() {
        let with = KvObj::from_bytes(b"k", b"v", Some(UnixMillis(10)));
        assert!(with.header.ttl_present);
        // Canonical Valkey boundary (`now > when`): ALIVE at now == deadline.
        assert!(!with.is_expired(UnixMillis(10)));
        assert!(with.is_expired(UnixMillis(11)));
        assert!(!with.is_expired(UnixMillis(9)));
        let without = KvObj::from_bytes(b"k", b"v", None);
        assert!(!without.header.ttl_present);
        assert!(!without.is_expired(UnixMillis(u64::MAX)));
    }

    #[test]
    fn is_expired_boundary_is_strictly_greater_than_deadline() {
        // The Valkey contract (src/db.c `return now > when;`): a key with deadline
        // D is LIVE at now == D and DEAD only at now == D + 1.
        let o = KvObj::from_bytes(b"k", b"v", Some(UnixMillis(100)));
        assert!(!o.is_expired(UnixMillis(99)), "before deadline: live");
        assert!(!o.is_expired(UnixMillis(100)), "at deadline: live");
        assert!(o.is_expired(UnixMillis(101)), "one past deadline: dead");
    }

    #[test]
    fn accounted_bytes_is_key_plus_value() {
        let o = KvObj::from_bytes(b"key", b"value", None);
        assert_eq!(o.accounted_bytes(), 3 + 5);
        let i = KvObj::from_int(b"k", 12345, None);
        assert_eq!(i.accounted_bytes(), 1 + 5); // "12345" is 5 decimal digits
    }

    // -- freq-in-object (S3-FIFO 2-bit promote frequency on the stored Entry): the
    // Str path packs it into spare FLAGS bits (no blob layout change, no new unsafe);
    // the Coll path uses CollEntry.eviction_rank. These run under `cargo miri test`. --

    #[test]
    fn str_entry_freq_packs_into_spare_flags_bits_and_caps_at_3() {
        let mut e = Entry::str_from_bytes(b"k", b"value", None);
        assert_eq!(e.freq(), 0, "a fresh entry starts at freq 0");
        e.bump_freq();
        assert_eq!(e.freq(), 1);
        e.bump_freq();
        e.bump_freq();
        assert_eq!(e.freq(), 3);
        e.bump_freq();
        assert_eq!(e.freq(), 3, "freq caps at MAX_FREQ (3)");
        e.set_freq(2);
        assert_eq!(e.freq(), 2);
        e.set_freq(7);
        assert_eq!(e.freq(), 3, "set_freq caps at 3");
        e.dec_freq();
        assert_eq!(e.freq(), 2);
        // The freq bits must NOT disturb the rest of the entry: key/value/type/encoding
        // and the TTL-present flag are all read through the same blob bytes.
        assert_eq!(e.key(), b"k");
        assert_eq!(e.str_value_bytes(), b"value");
        assert_eq!(e.data_type(), DataType::String);
        assert_eq!(
            e.expire_at(),
            None,
            "no TTL: bit 0 untouched by the freq bits"
        );
    }

    #[test]
    fn str_entry_freq_and_ttl_flag_are_independent() {
        // The freq bits (1-2) and the TTL-present bit (0) share the flags byte; setting
        // one must not corrupt the other.
        let mut e = Entry::str_from_bytes(b"k", b"v", Some(UnixMillis(12345)));
        assert_eq!(e.expire_at(), Some(UnixMillis(12345)));
        assert_eq!(e.freq(), 0);
        e.set_freq(3);
        assert_eq!(e.freq(), 3);
        // The TTL deadline still reads back exactly (TTL-present bit + the deadline
        // bytes are untouched by the freq write).
        assert_eq!(e.expire_at(), Some(UnixMillis(12345)));
        assert_eq!(e.key(), b"k");
        assert_eq!(e.str_value_bytes(), b"v");
    }

    #[test]
    fn coll_entry_freq_uses_eviction_rank() {
        let mut list = ListVal::new();
        list.push_back(b"a", TH);
        let mut e = Entry::coll(b"mylist", CollVal::List(list), None);
        assert!(e.is_collection());
        assert_eq!(e.freq(), 0);
        e.bump_freq();
        e.bump_freq();
        assert_eq!(e.freq(), 2);
        e.set_freq(9);
        assert_eq!(e.freq(), 3, "Coll freq caps at 3 (masked to 2 bits)");
        e.dec_freq();
        assert_eq!(e.freq(), 2);
        assert_eq!(e.key(), b"mylist");
    }

    #[test]
    fn freq_survives_a_ttl_rebuild_and_a_rekey() {
        // A TTL add/remove rebuilds the Str blob; a rekey rebuilds it with a new key.
        // Both preserve the SAME value, so the S3-FIFO freq must travel across.
        let mut e = Entry::str_from_bytes(b"k", b"v", None);
        e.set_freq(3);
        e.set_expire_at(Some(UnixMillis(999))); // adds the TTL field -> rebuild
        assert_eq!(e.freq(), 3, "freq survives a TTL-add rebuild");
        assert_eq!(e.expire_at(), Some(UnixMillis(999)));
        e.set_expire_at(None); // removes the TTL field -> rebuild
        assert_eq!(e.freq(), 3, "freq survives a TTL-remove rebuild");
        e.rekey(b"newkey");
        assert_eq!(e.freq(), 3, "freq survives a rekey");
        assert_eq!(e.key(), b"newkey");
        assert_eq!(e.str_value_bytes(), b"v");
    }

    // -- ListVal unit tests (PR-5): the abstract list-op vocabulary + the
    // listpack->quicklist transition + accounting weight. --

    #[test]
    fn listval_push_pop_order() {
        let mut l = ListVal::new();
        l.push_back(b"a", TH);
        l.push_back(b"b", TH);
        l.push_front(b"z", TH);
        // [z, a, b]
        assert_eq!(l.len(), 3);
        assert_eq!(l.get(0), Some(b"z".to_vec()));
        assert_eq!(l.get(-1), Some(b"b".to_vec()));
        assert_eq!(l.pop_front(), Some(b"z".to_vec()));
        assert_eq!(l.pop_back(), Some(b"b".to_vec()));
        assert_eq!(l.len(), 1);
        assert_eq!(l.pop_front(), Some(b"a".to_vec()));
        assert!(l.is_empty());
        assert_eq!(l.pop_front(), None);
    }

    #[test]
    fn listval_set_and_index_out_of_range() {
        let mut l = ListVal::new();
        l.push_back(b"a", TH);
        l.push_back(b"b", TH);
        assert!(l.set(1, b"B", TH));
        assert!(l.set(-2, b"A", TH));
        assert_eq!(l.range(0, -1), vec![b"A".to_vec(), b"B".to_vec()]);
        // Out of range -> false, no change.
        assert!(!l.set(5, b"x", TH));
        assert!(!l.set(-5, b"x", TH));
        assert_eq!(l.get(9), None);
    }

    #[test]
    fn listval_insert_remove_trim_range() {
        let mut l = ListVal::new();
        for e in [b"a", b"b", b"c", b"a"] {
            l.push_back(e, TH);
        }
        // insert_before/after by pivot.
        assert_eq!(l.insert_before(b"b", b"X", TH), Some(5)); // [a, X, b, c, a]
        assert_eq!(l.insert_after(b"c", b"Y", TH), Some(6)); // [a, X, b, c, Y, a]
        assert_eq!(l.insert_before(b"zzz", b"q", TH), None); // pivot absent
        assert_eq!(
            l.range(0, -1),
            vec![
                b"a".to_vec(),
                b"X".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"Y".to_vec(),
                b"a".to_vec()
            ]
        );
        // remove_matching count<0 (tail->head): remove the LAST 'a' -> index 5 gone.
        assert_eq!(l.remove_matching(-1, b"a"), 1);
        assert_eq!(l.get(0), Some(b"a".to_vec())); // head 'a' survives
        // trim to [1, 2].
        l.trim(1, 2);
        assert_eq!(l.range(0, -1), vec![b"X".to_vec(), b"b".to_vec()]);
        // trim to empty.
        l.trim(5, 10);
        assert!(l.is_empty());
    }

    #[test]
    fn listval_pos_rank_count_maxlen() {
        let mut l = ListVal::new();
        for e in [b"a", b"b", b"a", b"c", b"a"] {
            l.push_back(e, TH);
        }
        // First match.
        assert_eq!(l.pos(b"a", 1, None, 0), vec![0]);
        // RANK 2 -> second match.
        assert_eq!(l.pos(b"a", 2, None, 0), vec![2]);
        // RANK -1 -> last match.
        assert_eq!(l.pos(b"a", -1, None, 0), vec![4]);
        // COUNT 0 -> all matches.
        assert_eq!(l.pos(b"a", 1, Some(0), 0), vec![0, 2, 4]);
        // MAXLEN 3 with COUNT 0 -> only positions in the first 3 elements.
        assert_eq!(l.pos(b"a", 1, Some(0), 3), vec![0, 2]);
        // No match.
        assert!(l.pos(b"zzz", 1, None, 0).is_empty());
    }

    #[test]
    fn listval_encoding_transition_is_byte_driven_only() {
        let mut l = ListVal::new();
        l.push_back(b"a", TH);
        assert_eq!(l.encoding(), Encoding::ListPack);
        // Over the byte budget -> quicklist.
        let big = vec![b'q'; DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES + 1];
        l.push_back(&big, TH);
        assert_eq!(l.encoding(), Encoding::QuickList);
        // The transition is ONE-WAY (#40 runtime-threshold ratchet, matching Redis's quicklist
        // and the hash/set/zset types): popping the big element back off does NOT demote -- the
        // list STAYS `quicklist` (the ratchet latched on the push that crossed the budget).
        assert_eq!(l.pop_back(), Some(big));
        assert_eq!(l.encoding(), Encoding::QuickList);

        // There is NO element-count cap for lists (Redis -2 negative fill: count
        // unlimited). MANY small elements that stay UNDER the byte budget remain
        // `listpack`, even well past any collection entry cap (the 512 hash / 128
        // zset-set caps). Use 200 single-byte elements (200 bytes, far under the 8 KB
        // budget).
        let mut l2 = ListVal::new();
        for _ in 0..200 {
            l2.push_back(b"z", TH);
        }
        assert_eq!(l2.elems.len(), 200);
        assert_eq!(
            l2.encoding(),
            Encoding::ListPack,
            "many small elements stay listpack: byte-driven transition only, no entry cap"
        );

        // Crossing the 8 KB byte budget with many small elements flips to quicklist.
        // Each push of 100 bytes; after enough pushes the total exceeds 8 KB.
        let chunk = vec![b'y'; 100];
        let pushes = (DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES / chunk.len()) + 2;
        let mut l3 = ListVal::new();
        for _ in 0..pushes {
            l3.push_back(&chunk, TH);
        }
        assert!(l3.element_bytes() > DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES);
        assert_eq!(
            l3.encoding(),
            Encoding::QuickList,
            "crossing the 8 KB byte budget flips to quicklist"
        );
    }

    #[test]
    fn listval_element_bytes_tracks_edits() {
        let mut l = ListVal::new();
        l.push_back(b"abc", TH); // 3
        l.push_front(b"de", TH); // 2
        assert_eq!(l.element_bytes(), 5);
        l.set(0, b"DEFG", TH); // de(2) -> DEFG(4): +2
        assert_eq!(l.element_bytes(), 7);
        l.pop_back(); // -3
        assert_eq!(l.element_bytes(), 4);
        l.remove_matching(0, b"DEFG"); // -4
        assert_eq!(l.element_bytes(), 0);
        assert!(l.is_empty());
    }

    // -- HashVal::should_convert boundary (PR-6 review): the Redis-correct 512/513 HASH
    // entry boundary + the 64-byte per-element cap, tested DIRECTLY (no need to insert 513
    // real elements; this pins the threshold constant + the comparison). --

    #[test]
    fn hashval_should_convert_pins_the_512_entry_and_64_byte_boundaries() {
        // The HASH entry cap is 512 (NOT the 128 zset/set cap). Verified vs Redis 7.4
        // config.c / t_hash.c and the pinned claim redis-hash-max-listpack-entries-512.
        assert_eq!(DEFAULT_HASH_MAX_LISTPACK_ENTRIES, 512);
        assert_eq!(DEFAULT_HASH_MAX_LISTPACK_VALUE, 64);

        // Entry-count boundary: 512 small entries stay listpack; 513 flips to hashtable.
        // `should_convert(entries, .., TH)` is called AFTER the edit, with the post-edit count.
        assert!(
            !HashVal::should_convert(512, 1, 1, TH),
            "exactly 512 entries (small field/value) stays listpack"
        );
        assert!(
            HashVal::should_convert(513, 1, 1, TH),
            "513 entries flips to hashtable (over the 512 HASH cap)"
        );

        // Per-element byte boundary (independent of entry count): a field or value of 64
        // bytes stays listpack; 65 bytes flips. Few entries, so only the byte cap fires.
        assert!(
            !HashVal::should_convert(2, 64, 64, TH),
            "a 64-byte field AND a 64-byte value (at the cap) stays listpack"
        );
        assert!(
            HashVal::should_convert(2, 65, 1, TH),
            "a 65-byte field (over the 64 cap) flips to hashtable"
        );
        assert!(
            HashVal::should_convert(2, 1, 65, TH),
            "a 65-byte value (over the 64 cap) flips to hashtable"
        );
    }

    /// Area A (#40): a LOWERED runtime `hash-max-listpack-entries` makes a NEW hash promote to
    /// `hashtable` past the new (smaller) cap, while an EXISTING hash built under the default cap is
    /// untouched. Proves the conversion reads the LIVE thresholds, and that a change is FUTURE-only.
    #[test]
    fn hash_encoding_honors_a_lowered_runtime_threshold_and_does_not_reencode_existing() {
        // A LOWERED cap of 4 entries.
        let low = EncodingThresholds {
            hash_max_listpack_entries: 4,
            ..EncodingThresholds::defaults()
        };
        // A NEW hash with 5 fields under the lowered cap -> hashtable (5 > 4).
        let mut h = HashVal::new();
        for i in 0..5 {
            h.set(format!("f{i}").as_bytes(), b"v", &low);
        }
        assert_eq!(
            h.encoding(),
            Encoding::HashTable,
            "a NEW hash past the lowered cap is hashtable"
        );
        // An EXISTING small hash built under the DEFAULT (512) cap stays listpack: lowering the cap
        // afterward does NOT re-encode it (no further edits re-evaluate it).
        let mut existing = HashVal::new();
        for i in 0..5 {
            existing.set(format!("f{i}").as_bytes(), b"v", TH);
        }
        assert_eq!(
            existing.encoding(),
            Encoding::ListPack,
            "an EXISTING hash built under the default cap is untouched by a later lower cap"
        );
    }

    /// Area A (#40): a RAISED runtime `set-max-listpack-entries` keeps a NEW set in the listpack
    /// form past the default 128 cap (proving the live threshold widens, not just narrows).
    #[test]
    fn set_encoding_honors_a_raised_runtime_listpack_threshold() {
        let high = EncodingThresholds {
            set_max_listpack_entries: 1000,
            ..EncodingThresholds::defaults()
        };
        let mut s = SetVal::new();
        // 200 non-integer members: over the DEFAULT 128 listpack cap but under the raised 1000.
        for i in 0..200 {
            s.add(format!("m{i}").as_bytes(), &high);
        }
        assert_eq!(
            s.encoding(),
            Encoding::ListPack,
            "a raised set-max-listpack-entries keeps a 200-member set in listpack"
        );
        // Under the DEFAULT cap the same 200 members would be a hashtable.
        let mut s_default = SetVal::new();
        for i in 0..200 {
            s_default.add(format!("m{i}").as_bytes(), TH);
        }
        assert_eq!(s_default.encoding(), Encoding::HashTable);
    }

    // -- SetVal unit tests (PR-7): the abstract set-op vocabulary + the
    // intset->listpack->hashtable conversion ladder + accounting weight. --

    #[test]
    fn setval_intset_stays_intset_for_all_integer_members() {
        let mut s = SetVal::new();
        assert!(s.add(b"3", TH));
        assert!(s.add(b"1", TH));
        assert!(s.add(b"2", TH));
        // Re-adding is a no-op.
        assert!(!s.add(b"2", TH));
        assert_eq!(s.encoding(), Encoding::IntSet);
        assert_eq!(s.len(), 3);
        // Members are in ASCENDING integer order (the intset sort).
        assert_eq!(
            s.members(),
            vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]
        );
        // Binary-search membership.
        assert!(s.contains(b"2"));
        assert!(!s.contains(b"9"));
        // element_bytes is the sum of each integer's DECIMAL length.
        assert_eq!(s.element_bytes(), 3); // "1","2","3" = 1+1+1
    }

    #[test]
    fn setval_non_canonical_int_is_not_an_intset_member() {
        // "007" parses as 7 but is NOT canonical (leading zeros), so it leaves intset for
        // listpack (Redis keeps it as a listpack member).
        let mut s = SetVal::new();
        assert!(s.add(b"7", TH));
        assert_eq!(s.encoding(), Encoding::IntSet);
        assert!(s.add(b"007", TH));
        assert_eq!(
            s.encoding(),
            Encoding::ListPack,
            "a non-canonical integer member converts intset -> listpack"
        );
        assert!(s.contains(b"7"));
        assert!(s.contains(b"007"));
        assert!(!s.contains(b"8"));
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn setval_non_integer_member_converts_intset_to_listpack() {
        let mut s = SetVal::new();
        s.add(b"1", TH);
        s.add(b"2", TH);
        assert_eq!(s.encoding(), Encoding::IntSet);
        // A non-integer member converts to listpack (still small).
        assert!(s.add(b"hello", TH));
        assert_eq!(s.encoding(), Encoding::ListPack);
        assert_eq!(s.len(), 3);
        // The previously-integer members are preserved as their decimal bytes.
        let mut m = s.members();
        m.sort();
        assert_eq!(m, vec![b"1".to_vec(), b"2".to_vec(), b"hello".to_vec()]);
    }

    #[test]
    fn setval_intset_over_512_goes_straight_to_hashtable() {
        // An all-integer set exceeding set-max-intset-entries (512) goes STRAIGHT to
        // hashtable (512 > the 128 listpack cap, so it cannot fit a listpack).
        let mut s = SetVal::new();
        for i in 0..=DEFAULT_SET_MAX_INTSET_ENTRIES {
            s.add(i.to_string().as_bytes(), TH);
        }
        // 513 members.
        assert_eq!(s.len(), DEFAULT_SET_MAX_INTSET_ENTRIES + 1);
        assert_eq!(
            s.encoding(),
            Encoding::HashTable,
            "an integer set past 512 entries goes straight to hashtable (512 > 128)"
        );
        // Exactly 512 stays intset.
        let mut s512 = SetVal::new();
        for i in 0..DEFAULT_SET_MAX_INTSET_ENTRIES {
            s512.add(i.to_string().as_bytes(), TH);
        }
        assert_eq!(s512.len(), DEFAULT_SET_MAX_INTSET_ENTRIES);
        assert_eq!(
            s512.encoding(),
            Encoding::IntSet,
            "exactly 512 integer entries stays intset"
        );
    }

    #[test]
    fn setval_listpack_over_128_entries_converts_to_hashtable() {
        // A listpack set (a non-integer member forced it) that grows past
        // set-max-listpack-entries (128) converts to hashtable.
        let mut s = SetVal::new();
        s.add(b"x", TH); // non-integer -> listpack
        assert_eq!(s.encoding(), Encoding::ListPack);
        for i in 0..DEFAULT_SET_MAX_LISTPACK_ENTRIES {
            s.add(format!("m{i}").as_bytes(), TH);
        }
        assert!(s.len() > DEFAULT_SET_MAX_LISTPACK_ENTRIES);
        assert_eq!(s.encoding(), Encoding::HashTable);
    }

    #[test]
    fn setval_listpack_over_64_byte_member_converts_to_hashtable() {
        // A listpack set with a member exceeding set-max-listpack-value (64 bytes)
        // converts to hashtable, even with few members.
        let mut s = SetVal::new();
        s.add(b"x", TH); // non-integer -> listpack
        assert_eq!(s.encoding(), Encoding::ListPack);
        let big = vec![b'q'; DEFAULT_SET_MAX_LISTPACK_VALUE + 1];
        s.add(&big, TH);
        assert_eq!(s.encoding(), Encoding::HashTable);
        // A 64-byte member (at the cap) stays listpack.
        let mut s2 = SetVal::new();
        s2.add(b"y", TH);
        let at_cap = vec![b'q'; DEFAULT_SET_MAX_LISTPACK_VALUE];
        s2.add(&at_cap, TH);
        assert_eq!(s2.encoding(), Encoding::ListPack);
    }

    #[test]
    fn setval_conversion_is_one_way_no_demote() {
        // Once a set is hashtable, removing members back down to a tiny size keeps it
        // hashtable (Redis one-way ratchet).
        let mut s = SetVal::new();
        s.add(b"x", TH); // listpack
        let big = vec![b'q'; DEFAULT_SET_MAX_LISTPACK_VALUE + 1];
        s.add(&big, TH); // -> hashtable
        assert_eq!(s.encoding(), Encoding::HashTable);
        assert!(s.remove(&big));
        assert_eq!(s.len(), 1);
        assert_eq!(
            s.encoding(),
            Encoding::HashTable,
            "a hashtable set never demotes back to listpack/intset"
        );
        // Likewise an intset that converted to listpack stays listpack on shrink.
        let mut s2 = SetVal::new();
        s2.add(b"1", TH);
        s2.add(b"nonint", TH); // -> listpack
        assert_eq!(s2.encoding(), Encoding::ListPack);
        assert!(s2.remove(b"nonint"));
        assert_eq!(
            s2.encoding(),
            Encoding::ListPack,
            "a listpack set never demotes back to intset"
        );
    }

    #[test]
    fn setval_element_bytes_tracks_edits_across_forms() {
        let mut s = SetVal::new();
        s.add(b"10", TH); // intset, decimal len 2
        s.add(b"200", TH); // intset, decimal len 3
        assert_eq!(s.element_bytes(), 5);
        // Convert to listpack with a non-integer member; the integer members keep their
        // decimal byte weight (continuous across the conversion).
        s.add(b"ab", TH); // +2 -> listpack
        assert_eq!(s.element_bytes(), 7);
        s.remove(b"200"); // -3
        assert_eq!(s.element_bytes(), 4);
    }

    #[test]
    fn kvobj_from_set_reports_set_type_and_encoding() {
        let mut set = SetVal::new();
        set.add(b"1", TH);
        let o = KvObj::from_set(b"k", set, None);
        assert_eq!(o.header.data_type, DataType::Set);
        assert_eq!(o.header.encoding, Encoding::IntSet);
        assert!(o.is_set());
        assert!(o.is_collection());
        assert!(!o.is_empty_collection());
        // accounted_bytes = key + member decimal bytes.
        assert_eq!(o.accounted_bytes(), 1 + 1);
    }

    #[test]
    fn kvobj_from_set_via_new_owned_dedupes_and_applies_ladder() {
        // Create-on-missing through NewValueOwned::Set: dedupe + the ladder.
        let o = KvObj::from_new_owned(
            b"k",
            NewValueOwned::set(vec![b"1".to_vec(), b"2".to_vec(), b"1".to_vec()]),
            None,
            TH,
        );
        assert_eq!(o.collection_len(), Some(2), "duplicate member deduped");
        assert_eq!(o.header.encoding, Encoding::IntSet);
        // A mixed create -> listpack.
        let o2 = KvObj::from_new_owned(
            b"k",
            NewValueOwned::set(vec![b"1".to_vec(), b"x".to_vec()]),
            None,
            TH,
        );
        assert_eq!(o2.header.encoding, Encoding::ListPack);
        assert_eq!(o2.collection_len(), Some(2));
    }

    #[test]
    fn kvobj_from_list_reports_list_type_and_encoding() {
        let mut l = ListVal::new();
        l.push_back(b"x", TH);
        let o = KvObj::from_list(b"k", l, None);
        assert_eq!(o.header.data_type, DataType::List);
        assert_eq!(o.header.encoding, Encoding::ListPack);
        assert!(o.is_list());
        assert!(!o.is_empty_collection());
        // accounted_bytes = key + element bytes.
        assert_eq!(o.accounted_bytes(), 1 + 1);
    }

    // -- ZSetVal unit tests (PR-8): the (score, member) total order + the
    // listpack->skiplist transition + score-update + NaN/inf handling + accounting. --

    fn no_flags() -> ZAddFlags {
        ZAddFlags::default()
    }

    #[test]
    fn zsetval_orders_by_score_then_member_lex() {
        let mut z = ZSetVal::new();
        // Insert out of order; the (score, member) order must come out sorted.
        z.add(b"b", 2.0, no_flags(), TH);
        z.add(b"a", 1.0, no_flags(), TH);
        z.add(b"c", 2.0, no_flags(), TH); // equal score to b -> member lex tiebreak (b before c)
        z.add(b"d", 1.0, no_flags(), TH); // equal score to a -> a before d
        let order: Vec<Vec<u8>> = z
            .members_with_scores()
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        assert_eq!(
            order,
            vec![b"a".to_vec(), b"d".to_vec(), b"b".to_vec(), b"c".to_vec()],
            "score ASC, then member-bytes ASC for equal scores"
        );
    }

    #[test]
    fn zsetval_inf_scores_order_at_the_extremes() {
        let mut z = ZSetVal::new();
        z.add(b"mid", 0.0, no_flags(), TH);
        z.add(b"hi", f64::INFINITY, no_flags(), TH);
        z.add(b"lo", f64::NEG_INFINITY, no_flags(), TH);
        let order: Vec<Vec<u8>> = z
            .members_with_scores()
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        assert_eq!(order, vec![b"lo".to_vec(), b"mid".to_vec(), b"hi".to_vec()]);
        assert_eq!(z.score(b"hi"), Some(f64::INFINITY));
        assert_eq!(z.score(b"lo"), Some(f64::NEG_INFINITY));
    }

    #[test]
    fn zsetval_score_update_reorders_and_does_not_grow() {
        let mut z = ZSetVal::new();
        assert!(z.add(b"a", 1.0, no_flags(), TH).added);
        assert!(z.add(b"b", 2.0, no_flags(), TH).added);
        // Update a's score above b: it must reorder and NOT be a new member.
        let out = z.add(b"a", 9.0, no_flags(), TH);
        assert!(!out.added);
        assert!(out.changed);
        assert_eq!(out.new_score, Some(9.0));
        assert_eq!(z.len(), 2);
        let order: Vec<Vec<u8>> = z
            .members_with_scores()
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        assert_eq!(order, vec![b"b".to_vec(), b"a".to_vec()]);
    }

    #[test]
    fn zsetval_transition_at_entry_and_byte_thresholds_is_one_way() {
        // Entry-count threshold: 128 entries stays listpack; 129 flips to skiplist.
        let mut z = ZSetVal::new();
        for i in 0..DEFAULT_ZSET_MAX_LISTPACK_ENTRIES {
            z.add(format!("m{i:04}").as_bytes(), i as f64, no_flags(), TH);
        }
        assert_eq!(z.len(), DEFAULT_ZSET_MAX_LISTPACK_ENTRIES);
        assert_eq!(
            z.encoding(),
            Encoding::ListPack,
            "exactly 128 stays listpack"
        );
        z.add(b"overflow", 999.0, no_flags(), TH);
        assert_eq!(
            z.encoding(),
            Encoding::SkipList,
            "129 entries flips to skiplist"
        );
        // One-way ratchet: removing back below 128 keeps it skiplist.
        for i in 0..DEFAULT_ZSET_MAX_LISTPACK_ENTRIES {
            z.remove(format!("m{i:04}").as_bytes());
        }
        assert!(z.len() < DEFAULT_ZSET_MAX_LISTPACK_ENTRIES);
        assert_eq!(
            z.encoding(),
            Encoding::SkipList,
            "a skiplist zset never demotes back to listpack"
        );

        // Per-member byte threshold: a 64-byte member stays listpack; 65 flips.
        let mut z2 = ZSetVal::new();
        z2.add(b"x", 1.0, no_flags(), TH);
        let at_cap = vec![b'q'; DEFAULT_ZSET_MAX_LISTPACK_VALUE];
        z2.add(&at_cap, 2.0, no_flags(), TH);
        assert_eq!(
            z2.encoding(),
            Encoding::ListPack,
            "64-byte member at the cap"
        );
        let over_cap = vec![b'q'; DEFAULT_ZSET_MAX_LISTPACK_VALUE + 1];
        z2.add(&over_cap, 3.0, no_flags(), TH);
        assert_eq!(
            z2.encoding(),
            Encoding::SkipList,
            "a 65-byte member flips to skiplist"
        );
    }

    #[test]
    fn zsetval_add_matrix_nx_xx_gt_lt() {
        let mut z = ZSetVal::new();
        z.add(b"a", 5.0, no_flags(), TH);
        // NX on an existing member: suppressed (no change), new_score reflects current.
        let nx = z.add(
            b"a",
            1.0,
            ZAddFlags {
                nx: true,
                ..no_flags()
            },
            TH,
        );
        assert!(!nx.added && !nx.changed && nx.new_score == Some(5.0));
        // XX on a missing member: suppressed, new_score None.
        let xx = z.add(
            b"new",
            1.0,
            ZAddFlags {
                xx: true,
                ..no_flags()
            },
            TH,
        );
        assert!(!xx.added && !xx.changed && xx.new_score.is_none());
        // GT updates only if greater: 3 < 5 -> suppressed.
        let gt_lo = z.add(
            b"a",
            3.0,
            ZAddFlags {
                gt: true,
                ..no_flags()
            },
            TH,
        );
        assert!(!gt_lo.changed && z.score(b"a") == Some(5.0));
        // GT updates if greater: 9 > 5 -> applied.
        let gt_hi = z.add(
            b"a",
            9.0,
            ZAddFlags {
                gt: true,
                ..no_flags()
            },
            TH,
        );
        assert!(gt_hi.changed && z.score(b"a") == Some(9.0));
        // GT/LT alone still ADD a new member (Redis: GT/LT do not prevent adds).
        let added = z.add(
            b"fresh",
            7.0,
            ZAddFlags {
                gt: true,
                ..no_flags()
            },
            TH,
        );
        assert!(added.added && z.score(b"fresh") == Some(7.0));
    }

    #[test]
    fn zsetval_incr_and_suppression() {
        let mut z = ZSetVal::new();
        // INCR on a missing member starts from delta.
        assert_eq!(z.incr(b"a", 2.5, no_flags(), TH), IncrOutcome::Updated(2.5));
        assert_eq!(z.incr(b"a", 2.5, no_flags(), TH), IncrOutcome::Updated(5.0));
        // NX INCR on an existing member: suppressed (nil).
        assert_eq!(
            z.incr(
                b"a",
                1.0,
                ZAddFlags {
                    nx: true,
                    ..no_flags()
                },
                TH
            ),
            IncrOutcome::Suppressed
        );
        // XX INCR on a missing member: suppressed (nil).
        assert_eq!(
            z.incr(
                b"missing",
                1.0,
                ZAddFlags {
                    xx: true,
                    ..no_flags()
                },
                TH
            ),
            IncrOutcome::Suppressed
        );
    }

    #[test]
    fn zsetval_incr_nan_result_is_signalled_and_does_not_mutate() {
        // An existing +inf incremented by -inf yields NaN: the store must NOT store it.
        let mut z = ZSetVal::new();
        assert_eq!(
            z.incr(b"m", f64::INFINITY, no_flags(), TH),
            IncrOutcome::Updated(f64::INFINITY)
        );
        assert_eq!(
            z.incr(b"m", f64::NEG_INFINITY, no_flags(), TH),
            IncrOutcome::Nan
        );
        // The member is UNCHANGED at +inf (no NaN stored, the order is untouched).
        assert_eq!(z.score(b"m"), Some(f64::INFINITY));

        // The symmetric case: an existing -inf incremented by +inf.
        let mut z2 = ZSetVal::new();
        assert_eq!(
            z2.incr(b"m", f64::NEG_INFINITY, no_flags(), TH),
            IncrOutcome::Updated(f64::NEG_INFINITY)
        );
        assert_eq!(
            z2.incr(b"m", f64::INFINITY, no_flags(), TH),
            IncrOutcome::Nan
        );
        assert_eq!(z2.score(b"m"), Some(f64::NEG_INFINITY));
    }

    #[test]
    fn zsetval_range_by_score_rank_lex_and_pops() {
        let mut z = ZSetVal::new();
        for (m, s) in [(b"a", 1.0), (b"b", 2.0), (b"c", 3.0), (b"d", 4.0)] {
            z.add(m, s, no_flags(), TH);
        }
        // range_by_score inclusive [2,3].
        let by_score = z.range_by_score(
            ScoreBound::inclusive(2.0),
            ScoreBound::inclusive(3.0),
            false,
            None,
        );
        assert_eq!(by_score, vec![(b"b".to_vec(), 2.0), (b"c".to_vec(), 3.0)]);
        // exclusive lower (2 -> excludes b.
        let excl = z.range_by_score(
            ScoreBound::exclusive(2.0),
            ScoreBound::inclusive(4.0),
            false,
            None,
        );
        assert_eq!(excl.len(), 2);
        assert_eq!(excl[0].0, b"c".to_vec());
        // rank.
        assert_eq!(z.rank(b"a", false), Some(0));
        assert_eq!(z.rank(b"a", true), Some(3));
        assert_eq!(z.rank(b"zzz", false), None);
        // range_by_rank [0, 1].
        let r = z.range_by_rank(0, 1, false);
        assert_eq!(r, vec![(b"a".to_vec(), 1.0), (b"b".to_vec(), 2.0)]);
        // count_by_score.
        assert_eq!(
            z.count_by_score(ScoreBound::inclusive(2.0), ScoreBound::inclusive(3.0)),
            2
        );
        // pop_min/pop_max.
        assert_eq!(z.pop_min(1), vec![(b"a".to_vec(), 1.0)]);
        assert_eq!(z.pop_max(1), vec![(b"d".to_vec(), 4.0)]);
        assert_eq!(z.len(), 2);
    }

    #[test]
    fn zsetval_lex_range_equal_scores() {
        let mut z = ZSetVal::new();
        for m in [b"a".as_slice(), b"b", b"c", b"d"] {
            z.add(m, 0.0, no_flags(), TH);
        }
        // [b, (d -> b, c.
        let lex = z.range_by_lex(
            &LexBound::Inclusive(b"b".to_vec()),
            &LexBound::Exclusive(b"d".to_vec()),
            false,
            None,
        );
        assert_eq!(lex, vec![b"b".to_vec(), b"c".to_vec()]);
        // - to + -> all.
        let all = z.range_by_lex(&LexBound::NegInf, &LexBound::PosInf, false, None);
        assert_eq!(all.len(), 4);
        assert_eq!(z.count_by_lex(&LexBound::NegInf, &LexBound::PosInf), 4);
    }

    #[test]
    fn kvobj_from_zset_reports_zset_type_and_encoding_and_accounting() {
        let mut z = ZSetVal::new();
        z.add(b"m", 1.0, no_flags(), TH);
        let o = KvObj::from_zset(b"k", z, None);
        assert_eq!(o.header.data_type, DataType::ZSet);
        assert_eq!(o.header.encoding, Encoding::ListPack);
        assert!(o.is_zset());
        assert!(o.is_collection());
        assert!(!o.is_empty_collection());
        // accounted_bytes = key(1) + member bytes(1) + 8-byte score charge.
        assert_eq!(o.accounted_bytes(), 1 + 1 + 8);
    }

    #[test]
    fn kvobj_from_zset_via_new_owned_dedupes_last_score_wins() {
        let o = KvObj::from_new_owned(
            b"k",
            NewValueOwned::zset(vec![
                (b"a".to_vec(), 1.0),
                (b"b".to_vec(), 2.0),
                (b"a".to_vec(), 9.0),
            ]),
            None,
            TH,
        );
        assert_eq!(o.collection_len(), Some(2), "duplicate member deduped");
        if let ValueRepr::ZSet(z) = &o.value {
            assert_eq!(z.score(b"a"), Some(9.0), "last score wins");
        } else {
            panic!("expected a zset value");
        }
    }

    // ----------------------------------------------------------------------
    // Entry tagged-pointer (perf/tagged-slot) unit tests. These exercise the
    // manual alloc / dealloc / Drop / Clone / tagged-access UNSAFE paths in
    // isolation, so `cargo +nightly miri test -p ironcache-store` flags any UB
    // (provenance, aliasing, double-free, leak) on the Entry representation.
    // ----------------------------------------------------------------------

    #[test]
    fn entry_slot_is_one_word() {
        // The whole point of the change: the slot is 8 bytes (was 16), and `NonNull`'s
        // niche keeps `Option<Entry>` at 8 too.
        assert_eq!(std::mem::size_of::<Entry>(), 8);
        assert_eq!(std::mem::size_of::<Option<Entry>>(), 8);
    }

    #[test]
    fn entry_str_roundtrips_key_value_type_encoding_and_drops() {
        // Build/read each string encoding; the Entry drops at end of scope, exercising
        // the Str dealloc path under miri.
        let raw = vec![b'z'; 100];
        let cases: &[(&[u8], &[u8], Encoding)] = &[
            (b"k1", b"12345", Encoding::Int), // int: value bytes are the decimal digits
            (b"k2", b"hello", Encoding::EmbStr), // short string
            (b"k3", &raw, Encoding::Raw),     // long string
        ];
        for (key, val, enc) in cases {
            let e = Entry::str_from_bytes(key, val, None);
            assert_eq!(e.key(), *key, "key parsed back from the blob");
            assert_eq!(e.str_value_bytes(), *val, "value parsed back from the blob");
            assert_eq!(e.data_type(), DataType::String);
            assert_eq!(e.encoding(), *enc);
            assert_eq!(e.expire_at(), None);
            assert_eq!(e.accounted_bytes(), key.len() + val.len());
            assert!(!e.is_collection());
            assert_eq!(e.collection_len(), None);
        }
    }

    #[test]
    fn entry_str_ttl_present_and_patch_and_add_remove() {
        // With a TTL: the deadline parses back, the blob carries the 8-byte field.
        let e = Entry::str_from_bytes(b"key", b"val", Some(UnixMillis(1234)));
        assert_eq!(e.expire_at(), Some(UnixMillis(1234)));
        assert_eq!(e.key(), b"key");
        assert_eq!(e.str_value_bytes(), b"val");

        // In-place patch (same blob length): a deadline-only change must NOT realloc and
        // must round-trip.
        let mut e = e;
        e.set_expire_at(Some(UnixMillis(9999)));
        assert_eq!(e.expire_at(), Some(UnixMillis(9999)));
        assert_eq!(e.key(), b"key", "patch leaves key intact");
        assert_eq!(e.str_value_bytes(), b"val", "patch leaves value intact");

        // Remove the TTL: the blob shrinks (rebuild), the old alloc is freed.
        e.set_expire_at(None);
        assert_eq!(e.expire_at(), None);
        assert_eq!(e.key(), b"key");
        assert_eq!(e.str_value_bytes(), b"val");

        // Add a TTL back: the blob grows (rebuild).
        e.set_expire_at(Some(UnixMillis(42)));
        assert_eq!(e.expire_at(), Some(UnixMillis(42)));
        assert_eq!(e.str_value_bytes(), b"val");
    }

    #[test]
    fn entry_clone_is_a_deep_independent_copy_str() {
        // Cloning a Str entry copies the blob into a fresh allocation; both drop
        // independently (no double-free, no shared ownership), which miri verifies.
        let original = Entry::str_from_bytes(b"shared", b"payload", Some(UnixMillis(7)));
        let cloned = original.clone();
        assert_eq!(cloned.key(), b"shared");
        assert_eq!(cloned.str_value_bytes(), b"payload");
        assert_eq!(cloned.expire_at(), Some(UnixMillis(7)));
        // Mutating the clone's TTL must not touch the original (distinct allocations).
        let mut cloned = cloned;
        cloned.set_expire_at(Some(UnixMillis(8)));
        assert_eq!(cloned.expire_at(), Some(UnixMillis(8)));
        assert_eq!(original.expire_at(), Some(UnixMillis(7)));
        drop(cloned);
        // The original is still valid after the clone dropped.
        assert_eq!(original.str_value_bytes(), b"payload");
    }

    #[test]
    fn entry_coll_tagged_pointer_access_mutate_clone_and_drop() {
        // A Coll entry: the low tag bit marks it; access masks it off. Build a list,
        // read it, mutate in place through `as_list_mut`, clone, and drop.
        let mut list = ListVal::new();
        list.push_back(b"a", TH);
        list.push_back(b"b", TH);
        let mut e = Entry::coll(b"mykey", CollVal::List(list), Some(UnixMillis(5)));
        assert!(e.is_collection());
        assert_eq!(e.key(), b"mykey");
        assert_eq!(e.data_type(), DataType::List);
        assert_eq!(e.encoding(), Encoding::ListPack);
        assert_eq!(e.expire_at(), Some(UnixMillis(5)));
        assert_eq!(e.collection_len(), Some(2));
        assert!(!e.is_empty_collection());

        // Mutate through the tagged pointer (`coll_mut` masks the tag).
        {
            let l = e.as_list_mut().expect("list arm");
            l.push_back(b"c", TH);
        }
        assert_eq!(e.collection_len(), Some(3));

        // A non-matching `as_*_mut` returns None (WRONGTYPE), not the wrong arm.
        assert!(e.as_hash_mut().is_none());
        assert!(e.as_set_mut().is_none());
        assert!(e.as_zset_mut().is_none());

        // Deep clone: an independent CollEntry box (re-boxed + re-tagged).
        let cloned = e.clone();
        assert_eq!(cloned.collection_len(), Some(3));
        assert_eq!(cloned.key(), b"mykey");

        // Re-key the original (a Coll field write); the clone is unaffected.
        let mut e = e;
        e.rekey(b"renamed");
        assert_eq!(e.key(), b"renamed");
        assert_eq!(cloned.key(), b"mykey", "clone unaffected by original rekey");
        // Both drop here: two distinct boxes freed exactly once each.
    }

    #[test]
    fn entry_coll_set_expire_at_is_a_field_write() {
        let mut set = SetVal::new();
        set.add(b"1", TH);
        let mut e = Entry::coll(b"s", CollVal::Set(set), None);
        assert_eq!(e.expire_at(), None);
        e.set_expire_at(Some(UnixMillis(100)));
        assert_eq!(e.expire_at(), Some(UnixMillis(100)));
        e.set_expire_at(None);
        assert_eq!(e.expire_at(), None);
        assert_eq!(e.data_type(), DataType::Set);
    }

    #[test]
    fn entry_str_rekey_rebuilds_blob_with_new_key() {
        let mut e = Entry::str_from_bytes(b"old", b"value", Some(UnixMillis(3)));
        e.rekey(b"new-and-longer-key");
        assert_eq!(e.key(), b"new-and-longer-key");
        assert_eq!(
            e.str_value_bytes(),
            b"value",
            "value preserved across rekey"
        );
        assert_eq!(
            e.expire_at(),
            Some(UnixMillis(3)),
            "ttl preserved across rekey"
        );
        assert_eq!(e.data_type(), DataType::String);
    }

    #[test]
    fn entry_empty_key_and_empty_value_are_well_formed() {
        // Boundary: an empty key and/or empty value still produce a valid thin blob
        // (the total length is >= STR_BLOB_OFFSET, never a zero-size alloc).
        let e = Entry::str_from_bytes(b"", b"", None);
        assert_eq!(e.key(), b"");
        assert_eq!(e.str_value_bytes(), b"");
        assert_eq!(e.accounted_bytes(), 0);
        let e2 = Entry::str_from_bytes(b"k", b"", None);
        assert_eq!(e2.key(), b"k");
        assert_eq!(e2.str_value_bytes(), b"");
    }
}
