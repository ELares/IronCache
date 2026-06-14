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
use bytes::Bytes;
use ironcache_storage::{DataType, Encoding, NewValueOwned, UnixMillis};

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
    /// The S3-FIFO eviction rank: a 2-bit frequency counter capped at 3
    /// (ADR-0008). Reserved in PR-2a (the eviction hook is a no-op); PR-3 drives
    /// it. Stored as a `u8` here, masked to two bits.
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

    /// A header for a freshly written value of the given encoding/ttl.
    #[must_use]
    pub fn new(encoding: Encoding, ttl_present: bool) -> Self {
        Header {
            data_type: DataType::String,
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
}

impl ValueRepr {
    /// The encoding this representation reports.
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        match self {
            ValueRepr::Int(_) => Encoding::Int,
            ValueRepr::Inline(_) => Encoding::EmbStr,
            ValueRepr::Raw(_) => Encoding::Raw,
        }
    }

    /// The logical byte length of the value as the command layer sees it: the
    /// decimal-digit count for an int, the byte length for a string. This is what
    /// STRLEN reports and what the accounting hook charges for the value.
    #[must_use]
    pub fn logical_len(&self) -> usize {
        match self {
            ValueRepr::Int(n) => int_decimal_len(*n),
            ValueRepr::Inline(b) => b.as_bytes().len(),
            ValueRepr::Raw(b) => b.len(),
        }
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
        }
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
}
