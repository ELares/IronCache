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

use crate::encoding::{Classified, EMBSTR_THRESHOLD, classify};
use crate::scan_hash;
use bytes::Bytes;
use hashbrown::HashMap;
use hashbrown::HashSet;
use ironcache_config::{
    DEFAULT_HASH_MAX_LISTPACK_ENTRIES, DEFAULT_HASH_MAX_LISTPACK_VALUE,
    DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES, DEFAULT_SET_MAX_INTSET_ENTRIES,
    DEFAULT_SET_MAX_LISTPACK_ENTRIES, DEFAULT_SET_MAX_LISTPACK_VALUE,
};
use ironcache_storage::{
    DataType, Encoding, HashValue, ListValue, NewValueOwned, SetValue, UnixMillis,
};
use std::collections::VecDeque;

/// The inline-value buffer capacity (embstr). Matches [`EMBSTR_THRESHOLD`]; a
/// value classified as embstr fits here without a separate allocation in the
/// eventual FAM layout. In the safe rep it is a fixed-size inline array plus a
/// length, so an embstr value adds no heap allocation beyond the `KvObj` itself.
pub const INLINE_CAP: usize = EMBSTR_THRESHOLD;

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
    /// The S3-FIFO eviction rank: a 2-bit frequency counter capped at 3 (ADR-0008).
    /// RESERVED: in PR-3a the S3-FIFO 2-bit frequency is owned by the POLICY (a
    /// per-key counter on the queued entry), because `select_victim` is policy-only
    /// and cannot borrow this header. This field is carried for the eventual
    /// single-source-of-truth migration (when the decision path can read the rank
    /// across the storage boundary, a later PR), but the access path does NOT write it
    /// today, since nothing reads it. Stored as a `u8` here, masked to two bits.
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

/// A small inline string buffer ([`INLINE_CAP`] bytes plus a length), the safe-rep
/// stand-in for the FAM inline-value region. An embstr value lives here with no
/// extra heap allocation.
#[derive(Debug, Clone)]
pub struct InlineBuf {
    buf: [u8; INLINE_CAP],
    len: u8,
}

impl InlineBuf {
    /// Build from bytes that are known to fit ([`INLINE_CAP`]). Panics if too long;
    /// callers (the classifier path) only construct this for embstr-sized values.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() <= INLINE_CAP,
            "InlineBuf overflow: {} > {INLINE_CAP}",
            bytes.len()
        );
        let mut buf = [0u8; INLINE_CAP];
        buf[..bytes.len()].copy_from_slice(bytes);
        InlineBuf {
            buf,
            len: bytes.len() as u8,
        }
    }

    /// The inline bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }
}

/// The value representation inside a [`KvObj`] (ENCODINGS.md #112).
#[derive(Debug, Clone)]
pub enum ValueRepr {
    /// An int-encoded value: the raw i64, NO value allocation (the decimal bytes
    /// are materialized on read). `OBJECT ENCODING` -> int.
    Int(i64),
    /// A short string stored inline. `OBJECT ENCODING` -> embstr.
    Inline(InlineBuf),
    /// A long string stored out-of-line. `OBJECT ENCODING` -> raw.
    Raw(Box<[u8]>),
    /// A LIST value (PR-5). `OBJECT ENCODING` -> `listpack` while small, `quicklist`
    /// once over the threshold (a pure function of the active repr, #40).
    List(ListVal),
    /// A HASH value (PR-6). `OBJECT ENCODING` -> `listpack` while small, `hashtable`
    /// once over the entry-count OR per-element-byte threshold (a pure function of the
    /// active repr, #40).
    Hash(HashVal),
    /// A SET value (PR-7). `OBJECT ENCODING` -> `intset` while all-integer and small,
    /// `listpack` once a non-integer member is added (and still small), `hashtable` once
    /// over the entry-count OR per-member-byte threshold (a pure function of the active
    /// repr, #40). The conversion is ONE-WAY (never demotes).
    Set(SetVal),
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
            ValueRepr::Inline(b) => b.as_bytes().len(),
            ValueRepr::Raw(b) => b.len(),
            ValueRepr::List(l) => l.element_bytes(),
            ValueRepr::Hash(h) => h.element_bytes(),
            ValueRepr::Set(s) => s.element_bytes(),
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
}

impl ListVal {
    /// An empty list (used as the create-on-missing seed before the first push).
    #[must_use]
    pub fn new() -> Self {
        ListVal {
            elems: VecDeque::new(),
            total_bytes: 0,
        }
    }

    /// The sum of element byte lengths (the value-bytes side of accounting and the
    /// `logical_len` for a list). Does NOT include the key bytes (the kvobj adds
    /// those) or per-element bookkeeping (the FAM/chunk packing is a #8 follow-up).
    #[must_use]
    pub fn element_bytes(&self) -> usize {
        self.total_bytes
    }

    /// The encoding this list reports, a PURE FUNCTION of the active repr (#40):
    /// `listpack` while the total element bytes stay at or below the byte budget,
    /// `quicklist` once over it. There is NO element-count cap for lists: Redis's
    /// default `list-max-listpack-size -2` is a negative fill, which sizes the listpack
    /// node by BYTES (8 KB) with the element count left unlimited (quicklist.c
    /// `quicklistNodeLimit` sets `count = UINT_MAX` for a negative fill). The 128-entry
    /// cap is the HASH/ZSET default, NOT the list default. See the type docs.
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        if self.total_bytes > DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES {
            Encoding::QuickList
        } else {
            Encoding::ListPack
        }
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
    fn push_front(&mut self, elem: &[u8]) {
        self.total_bytes += elem.len();
        self.elems.push_front(elem.to_vec().into_boxed_slice());
    }

    fn push_back(&mut self, elem: &[u8]) {
        self.total_bytes += elem.len();
        self.elems.push_back(elem.to_vec().into_boxed_slice());
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

    fn set(&mut self, index: i64, elem: &[u8]) -> bool {
        let Some(i) = self.resolve_index(index) else {
            return false;
        };
        // One in-place entry rewrite (LSET): swap the element, adjust the byte total.
        let slot = &mut self.elems[i];
        self.total_bytes -= slot.len();
        self.total_bytes += elem.len();
        *slot = elem.to_vec().into_boxed_slice();
        true
    }

    fn insert_before(&mut self, pivot: &[u8], elem: &[u8]) -> Option<usize> {
        let at = self.elems.iter().position(|e| e.as_ref() == pivot)?;
        // VecDeque::insert shifts the shorter side once (one tail memmove, no
        // predecessor rewrite), satisfying the cascade-free contract.
        self.elems.insert(at, elem.to_vec().into_boxed_slice());
        self.total_bytes += elem.len();
        Some(self.elems.len())
    }

    fn insert_after(&mut self, pivot: &[u8], elem: &[u8]) -> Option<usize> {
        let at = self.elems.iter().position(|e| e.as_ref() == pivot)?;
        self.elems.insert(at + 1, elem.to_vec().into_boxed_slice());
        self.total_bytes += elem.len();
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
#[derive(Debug, Clone)]
pub enum HashVal {
    /// The small listpack-equivalent form: `(field, value)` pairs in insertion order,
    /// linear-scanned. Reports [`Encoding::ListPack`].
    ListPack(Vec<HashEntry>),
    /// The large hashtable form: a `hashbrown::HashMap`. Reports [`Encoding::HashTable`].
    /// One-way: a hash never converts back to the listpack form (Redis parity).
    HashTable(HashMap<Box<[u8]>, Box<[u8]>>),
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
        HashVal::ListPack(Vec::new())
    }

    /// The sum of field+value byte lengths (the value-bytes side of accounting and the
    /// `logical_len` for a hash). Does NOT include the key bytes (the kvobj adds those)
    /// or per-entry bookkeeping (the FAM/packing is a #8 follow-up).
    #[must_use]
    pub fn element_bytes(&self) -> usize {
        match self {
            HashVal::ListPack(v) => v.iter().map(|(f, val)| f.len() + val.len()).sum(),
            HashVal::HashTable(m) => m.iter().map(|(f, val)| f.len() + val.len()).sum(),
        }
    }

    /// The encoding this hash reports, a PURE FUNCTION of the active form (#40):
    /// `listpack` for the small form, `hashtable` for the large form.
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        match self {
            HashVal::ListPack(_) => Encoding::ListPack,
            HashVal::HashTable(_) => Encoding::HashTable,
        }
    }

    /// Whether the small listpack form should convert to the hashtable form after an
    /// edit that left `entries` entries with a new `field`/`value` (the #40 transition):
    /// convert once `entries > hash-max-listpack-entries` (the HASH cap, 512, NOT the 128
    /// ZSET/SET cap) OR either the new field or value byte length exceeds
    /// `hash-max-listpack-value` (64). Reads the HASH entry constant
    /// ([`DEFAULT_HASH_MAX_LISTPACK_ENTRIES`]).
    fn should_convert(entries: usize, field_len: usize, value_len: usize) -> bool {
        entries > DEFAULT_HASH_MAX_LISTPACK_ENTRIES
            || field_len > DEFAULT_HASH_MAX_LISTPACK_VALUE
            || value_len > DEFAULT_HASH_MAX_LISTPACK_VALUE
    }

    /// Promote the small listpack form to the large hashtable form (one-way). A no-op if
    /// already a hashtable.
    fn convert_to_hashtable(&mut self) {
        if let HashVal::ListPack(v) = self {
            let mut m: HashMap<Box<[u8]>, Box<[u8]>> = HashMap::with_capacity(v.len());
            for (f, val) in v.drain(..) {
                m.insert(f, val);
            }
            *self = HashVal::HashTable(m);
        }
    }

    /// Find the index of `field` in the small listpack form (linear scan), or `None`.
    fn listpack_pos(v: &[HashEntry], field: &[u8]) -> Option<usize> {
        v.iter().position(|(f, _)| f.as_ref() == field)
    }
}

impl HashValue for HashVal {
    fn set(&mut self, field: &[u8], value: &[u8]) -> bool {
        // Overwrite-in-place if present (no growth); else insert (growth). After an
        // insert into the small form, re-check the listpack -> hashtable transition.
        match self {
            HashVal::ListPack(v) => {
                if let Some(i) = HashVal::listpack_pos(v, field) {
                    v[i].1 = value.to_vec().into_boxed_slice();
                    // An overwrite can still cross the per-element value-byte cap.
                    if HashVal::should_convert(v.len(), field.len(), value.len()) {
                        self.convert_to_hashtable();
                    }
                    false
                } else {
                    v.push((
                        field.to_vec().into_boxed_slice(),
                        value.to_vec().into_boxed_slice(),
                    ));
                    if HashVal::should_convert(v.len(), field.len(), value.len()) {
                        self.convert_to_hashtable();
                    }
                    true
                }
            }
            HashVal::HashTable(m) => m
                .insert(
                    field.to_vec().into_boxed_slice(),
                    value.to_vec().into_boxed_slice(),
                )
                .is_none(),
        }
    }

    fn set_nx(&mut self, field: &[u8], value: &[u8]) -> bool {
        // DEFERRED #8 follow-up (efficiency papercut, correctness-neutral): this does a
        // double linear scan on the listpack form (contains then set both scan); a single
        // entry-API pass would avoid the second scan.
        if self.contains(field) {
            return false;
        }
        self.set(field, value);
        true
    }

    fn get(&self, field: &[u8]) -> Option<&[u8]> {
        match self {
            HashVal::ListPack(v) => HashVal::listpack_pos(v, field).map(|i| v[i].1.as_ref()),
            HashVal::HashTable(m) => m.get(field).map(std::convert::AsRef::as_ref),
        }
    }

    fn del(&mut self, field: &[u8]) -> bool {
        match self {
            HashVal::ListPack(v) => {
                if let Some(i) = HashVal::listpack_pos(v, field) {
                    v.remove(i);
                    true
                } else {
                    false
                }
            }
            // One-way ratchet: a hashtable hash stays a hashtable even as it shrinks
            // (Redis parity), so we do NOT demote back to listpack on removal.
            HashVal::HashTable(m) => m.remove(field).is_some(),
        }
    }

    fn contains(&self, field: &[u8]) -> bool {
        match self {
            HashVal::ListPack(v) => HashVal::listpack_pos(v, field).is_some(),
            HashVal::HashTable(m) => m.contains_key(field),
        }
    }

    fn len(&self) -> usize {
        match self {
            HashVal::ListPack(v) => v.len(),
            HashVal::HashTable(m) => m.len(),
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
        matches!(self, HashVal::ListPack(_))
    }

    fn pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            // The listpack form is already in deterministic INSERTION order.
            HashVal::ListPack(v) => v
                .iter()
                .map(|(f, val)| (f.to_vec(), val.to_vec()))
                .collect(),
            // The hashtable form's `hashbrown` iteration order varies run-to-run, so
            // SORT by the fixed-seed stable field hash (then raw bytes) for a
            // deterministic, resize-invariant order (the same order SCAN uses, ADR-0003).
            HashVal::HashTable(m) => {
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
    /// the encoding ladder. Deduplicates. All-integer + small -> intset; else the
    /// listpack/hashtable choice per the thresholds.
    fn from_members(members: &[Vec<u8>]) -> Self {
        let mut set = SetVal::new();
        for m in members {
            set.add(m);
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
    /// listpack thresholds): convert once `entries > set-max-listpack-entries` (128) OR
    /// any member byte length exceeds `set-max-listpack-value` (64).
    fn listpack_overflows(entries: usize, max_member_len: usize) -> bool {
        entries > DEFAULT_SET_MAX_LISTPACK_ENTRIES
            || max_member_len > DEFAULT_SET_MAX_LISTPACK_VALUE
    }

    /// Add a member to the intset form, applying the intset thresholds. Returns whether
    /// the member was new. The caller guarantees `self` is the intset form and `n` is the
    /// canonical integer for `member`.
    fn add_int(&mut self, n: i64, member: &[u8]) -> bool {
        let SetVal::IntSet(v) = self else {
            unreachable!("add_int called on a non-intset form");
        };
        match v.binary_search(&n) {
            Ok(_) => false, // already present
            Err(pos) => {
                v.insert(pos, n);
                // Growth past set-max-intset-entries converts away from intset. Because
                // 512 > 128 (the listpack entry cap), an integer set that exceeds 512
                // members cannot fit a listpack and goes straight to hashtable. We re-use
                // the listpack-overflow check after a tentative listpack conversion: if
                // it would overflow the listpack, go hashtable instead.
                if v.len() > DEFAULT_SET_MAX_INTSET_ENTRIES {
                    let entries = v.len();
                    // The largest integer member's decimal length (<= 20 < 64), so the
                    // per-member byte cap never fires for integers; only the entry count
                    // matters. 513 > 128 always, so this always promotes to hashtable.
                    if SetVal::listpack_overflows(entries, member.len()) {
                        self.convert_to_hashtable();
                    } else {
                        self.convert_intset_to_listpack();
                    }
                }
                true
            }
        }
    }
}

impl SetValue for SetVal {
    fn add(&mut self, member: &[u8]) -> bool {
        match self {
            SetVal::IntSet(_) => {
                if let Some(n) = SetVal::parse_canonical_int(member) {
                    self.add_int(n, member)
                } else {
                    // A non-integer member leaves intset: convert to listpack (or
                    // hashtable if the resulting set would overflow the listpack), then
                    // add the member.
                    self.convert_intset_to_listpack();
                    self.add(member)
                }
            }
            SetVal::ListPack(v) => {
                if v.iter().any(|m| m.as_ref() == member) {
                    return false;
                }
                v.push(member.to_vec().into_boxed_slice());
                // Re-check the listpack thresholds after the insert.
                if SetVal::listpack_overflows(v.len(), member.len()) {
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
            Classified::EmbStr => ValueRepr::Inline(InlineBuf::from_bytes(bytes)),
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
    /// classified (so a numeric string written via rmw still becomes int).
    #[must_use]
    pub fn from_new_owned(key: &[u8], value: NewValueOwned, expire_at: Option<UnixMillis>) -> Self {
        match value {
            NewValueOwned::Int(n) => KvObj::from_int(key, n, expire_at),
            NewValueOwned::Bytes(b) => KvObj::from_bytes(key, &b, expire_at),
            // The PR-5 create-on-missing LIST path: build the list value from the
            // head-to-tail elements. Subsequent edits go through the in-place
            // RmwAction::Mutated path, not this rebuild.
            NewValueOwned::List(elems) => {
                let mut list = ListVal::new();
                for e in &elems {
                    list.push_back(e);
                }
                KvObj::from_list(key, list, expire_at)
            }
            // The PR-6 create-on-missing HASH path: build the hash value from the
            // insertion-ordered (field, value) pairs. Subsequent edits go through the
            // in-place RmwAction::Mutated path, not this rebuild.
            NewValueOwned::Hash(pairs) => {
                let mut hash = HashVal::new();
                for (f, v) in &pairs {
                    hash.set(f, v);
                }
                KvObj::from_hash(key, hash, expire_at)
            }
            // The PR-7 create-on-missing SET path: build the set value from the members
            // (deduped + ladder-applied by `SetVal::from_members`). Subsequent edits go
            // through the in-place RmwAction::Mutated path, not this rebuild.
            NewValueOwned::Set(members) => {
                let set = SetVal::from_members(&members);
                KvObj::from_set(key, set, expire_at)
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
            value: ValueRepr::List(list),
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
            value: ValueRepr::Hash(hash),
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
            value: ValueRepr::Set(set),
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
            ValueRepr::List(l) => Some(l),
            _ => None,
        }
    }

    /// A mutable borrow of the stored HASH value, or `None` if this entry is not a hash
    /// (PR-6: the store hands this to the in-place-mutation arm; a non-hash yields
    /// `None` -> WRONGTYPE). The HASH analog of [`Self::as_list_mut`].
    pub fn as_hash_mut(&mut self) -> Option<&mut HashVal> {
        match &mut self.value {
            ValueRepr::Hash(h) => Some(h),
            _ => None,
        }
    }

    /// A mutable borrow of the stored SET value, or `None` if this entry is not a set
    /// (PR-7: the store hands this to the in-place-mutation arm; a non-set yields `None`
    /// -> WRONGTYPE). The SET analog of [`Self::as_list_mut`]/[`Self::as_hash_mut`].
    pub fn as_set_mut(&mut self) -> Option<&mut SetVal> {
        match &mut self.value {
            ValueRepr::Set(s) => Some(s),
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
            ValueRepr::List(_) | ValueRepr::Hash(_) | ValueRepr::Set(_)
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
            Classified::EmbStr => ValueRepr::Inline(InlineBuf::from_bytes(bytes)),
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

#[cfg(test)]
mod tests {
    use super::*;

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

    // -- ListVal unit tests (PR-5): the abstract list-op vocabulary + the
    // listpack->quicklist transition + accounting weight. --

    #[test]
    fn listval_push_pop_order() {
        let mut l = ListVal::new();
        l.push_back(b"a");
        l.push_back(b"b");
        l.push_front(b"z");
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
        l.push_back(b"a");
        l.push_back(b"b");
        assert!(l.set(1, b"B"));
        assert!(l.set(-2, b"A"));
        assert_eq!(l.range(0, -1), vec![b"A".to_vec(), b"B".to_vec()]);
        // Out of range -> false, no change.
        assert!(!l.set(5, b"x"));
        assert!(!l.set(-5, b"x"));
        assert_eq!(l.get(9), None);
    }

    #[test]
    fn listval_insert_remove_trim_range() {
        let mut l = ListVal::new();
        for e in [b"a", b"b", b"c", b"a"] {
            l.push_back(e);
        }
        // insert_before/after by pivot.
        assert_eq!(l.insert_before(b"b", b"X"), Some(5)); // [a, X, b, c, a]
        assert_eq!(l.insert_after(b"c", b"Y"), Some(6)); // [a, X, b, c, Y, a]
        assert_eq!(l.insert_before(b"zzz", b"q"), None); // pivot absent
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
            l.push_back(e);
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
        l.push_back(b"a");
        assert_eq!(l.encoding(), Encoding::ListPack);
        // Over the byte budget -> quicklist.
        let big = vec![b'q'; super::DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES + 1];
        l.push_back(&big);
        assert_eq!(l.encoding(), Encoding::QuickList);
        // Pop it back off -> listpack again (a pure function of the active repr).
        assert_eq!(l.pop_back(), Some(big));
        assert_eq!(l.encoding(), Encoding::ListPack);

        // There is NO element-count cap for lists (Redis -2 negative fill: count
        // unlimited). MANY small elements that stay UNDER the byte budget remain
        // `listpack`, even well past any collection entry cap (the 512 hash / 128
        // zset-set caps). Use 200 single-byte elements (200 bytes, far under the 8 KB
        // budget).
        let mut l2 = ListVal::new();
        for _ in 0..200 {
            l2.push_back(b"z");
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
        let pushes = (super::DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES / chunk.len()) + 2;
        let mut l3 = ListVal::new();
        for _ in 0..pushes {
            l3.push_back(&chunk);
        }
        assert!(l3.element_bytes() > super::DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES);
        assert_eq!(
            l3.encoding(),
            Encoding::QuickList,
            "crossing the 8 KB byte budget flips to quicklist"
        );
    }

    #[test]
    fn listval_element_bytes_tracks_edits() {
        let mut l = ListVal::new();
        l.push_back(b"abc"); // 3
        l.push_front(b"de"); // 2
        assert_eq!(l.element_bytes(), 5);
        l.set(0, b"DEFG"); // de(2) -> DEFG(4): +2
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
        // `should_convert(entries, ..)` is called AFTER the edit, with the post-edit count.
        assert!(
            !HashVal::should_convert(512, 1, 1),
            "exactly 512 entries (small field/value) stays listpack"
        );
        assert!(
            HashVal::should_convert(513, 1, 1),
            "513 entries flips to hashtable (over the 512 HASH cap)"
        );

        // Per-element byte boundary (independent of entry count): a field or value of 64
        // bytes stays listpack; 65 bytes flips. Few entries, so only the byte cap fires.
        assert!(
            !HashVal::should_convert(2, 64, 64),
            "a 64-byte field AND a 64-byte value (at the cap) stays listpack"
        );
        assert!(
            HashVal::should_convert(2, 65, 1),
            "a 65-byte field (over the 64 cap) flips to hashtable"
        );
        assert!(
            HashVal::should_convert(2, 1, 65),
            "a 65-byte value (over the 64 cap) flips to hashtable"
        );
    }

    // -- SetVal unit tests (PR-7): the abstract set-op vocabulary + the
    // intset->listpack->hashtable conversion ladder + accounting weight. --

    #[test]
    fn setval_intset_stays_intset_for_all_integer_members() {
        let mut s = SetVal::new();
        assert!(s.add(b"3"));
        assert!(s.add(b"1"));
        assert!(s.add(b"2"));
        // Re-adding is a no-op.
        assert!(!s.add(b"2"));
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
        assert!(s.add(b"7"));
        assert_eq!(s.encoding(), Encoding::IntSet);
        assert!(s.add(b"007"));
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
        s.add(b"1");
        s.add(b"2");
        assert_eq!(s.encoding(), Encoding::IntSet);
        // A non-integer member converts to listpack (still small).
        assert!(s.add(b"hello"));
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
            s.add(i.to_string().as_bytes());
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
            s512.add(i.to_string().as_bytes());
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
        s.add(b"x"); // non-integer -> listpack
        assert_eq!(s.encoding(), Encoding::ListPack);
        for i in 0..DEFAULT_SET_MAX_LISTPACK_ENTRIES {
            s.add(format!("m{i}").as_bytes());
        }
        assert!(s.len() > DEFAULT_SET_MAX_LISTPACK_ENTRIES);
        assert_eq!(s.encoding(), Encoding::HashTable);
    }

    #[test]
    fn setval_listpack_over_64_byte_member_converts_to_hashtable() {
        // A listpack set with a member exceeding set-max-listpack-value (64 bytes)
        // converts to hashtable, even with few members.
        let mut s = SetVal::new();
        s.add(b"x"); // non-integer -> listpack
        assert_eq!(s.encoding(), Encoding::ListPack);
        let big = vec![b'q'; DEFAULT_SET_MAX_LISTPACK_VALUE + 1];
        s.add(&big);
        assert_eq!(s.encoding(), Encoding::HashTable);
        // A 64-byte member (at the cap) stays listpack.
        let mut s2 = SetVal::new();
        s2.add(b"y");
        let at_cap = vec![b'q'; DEFAULT_SET_MAX_LISTPACK_VALUE];
        s2.add(&at_cap);
        assert_eq!(s2.encoding(), Encoding::ListPack);
    }

    #[test]
    fn setval_conversion_is_one_way_no_demote() {
        // Once a set is hashtable, removing members back down to a tiny size keeps it
        // hashtable (Redis one-way ratchet).
        let mut s = SetVal::new();
        s.add(b"x"); // listpack
        let big = vec![b'q'; DEFAULT_SET_MAX_LISTPACK_VALUE + 1];
        s.add(&big); // -> hashtable
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
        s2.add(b"1");
        s2.add(b"nonint"); // -> listpack
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
        s.add(b"10"); // intset, decimal len 2
        s.add(b"200"); // intset, decimal len 3
        assert_eq!(s.element_bytes(), 5);
        // Convert to listpack with a non-integer member; the integer members keep their
        // decimal byte weight (continuous across the conversion).
        s.add(b"ab"); // +2 -> listpack
        assert_eq!(s.element_bytes(), 7);
        s.remove(b"200"); // -3
        assert_eq!(s.element_bytes(), 4);
    }

    #[test]
    fn kvobj_from_set_reports_set_type_and_encoding() {
        let mut set = SetVal::new();
        set.add(b"1");
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
        );
        assert_eq!(o.collection_len(), Some(2), "duplicate member deduped");
        assert_eq!(o.header.encoding, Encoding::IntSet);
        // A mixed create -> listpack.
        let o2 = KvObj::from_new_owned(
            b"k",
            NewValueOwned::set(vec![b"1".to_vec(), b"x".to_vec()]),
            None,
        );
        assert_eq!(o2.header.encoding, Encoding::ListPack);
        assert_eq!(o2.collection_len(), Some(2));
    }

    #[test]
    fn kvobj_from_list_reports_list_type_and_encoding() {
        let mut l = ListVal::new();
        l.push_back(b"x");
        let o = KvObj::from_list(b"k", l, None);
        assert_eq!(o.header.data_type, DataType::List);
        assert_eq!(o.header.encoding, Encoding::ListPack);
        assert!(o.is_list());
        assert!(!o.is_empty_collection());
        // accounted_bytes = key + element bytes.
        assert_eq!(o.accounted_bytes(), 1 + 1);
    }
}
