// SPDX-License-Identifier: MIT OR Apache-2.0
//! The KvObj wire codec (HA-7b full-sync): a hand-rolled, length-delimited encoding of
//! the public [`ironcache_store::KvObj`] transfer type for the snapshot stream.
//!
//! The full-sync (HA-7b) ships the primary's HA-5b snapshot to a fresh replica: the
//! primary drives [`ironcache_store::ShardStore::snapshot_chunk`], which yields each live
//! key as an owned [`KvObj`], and the replica replays each via
//! [`ironcache_store::ShardStore::insert_object`]. Between the two sits THIS codec: it
//! turns one `KvObj` into a self-describing byte string ([`encode_kvobj`]) that rides in a
//! [`crate::frames::Frame::SyncKv`] bulk arg, and back ([`decode_kvobj`]).
//!
//! ## What it carries (everything `insert_object` needs to reconstruct the entry)
//!
//! - the DATA TYPE ([`DataType`]) and ENCODING ([`Encoding`]) tags (so the replica's
//!   `OBJECT TYPE`/`OBJECT ENCODING` match the primary's exactly, including the one-way
//!   collection ratchet -- see below),
//! - the KEY bytes,
//! - the absolute TTL deadline ([`UnixMillis`]) if present,
//! - the VALUE: for a string, the canonical value bytes (the decimal digits for an int,
//!   the raw bytes for embstr/raw); for a collection, its logical contents IN REPR ORDER
//!   (list elements head-to-tail, hash pairs, set members, zset member+score pairs).
//!
//! ## Faithful encoding round-trip (the one-way collection ratchet)
//!
//! A collection's `OBJECT ENCODING` is a pure function of its ACTIVE in-memory repr, and
//! the small->large conversion is ONE-WAY (a hash/set/zset that grew to its large form
//! stays there even after it shrinks; a set that left `intset` never returns). Rebuilding a
//! collection from its logical contents alone would pick the SMALLEST form that fits the
//! contents, which can UNDER-report the encoding for a value that was promoted then shrank.
//! So the codec carries the encoding tag and, after rebuilding via the store's create-path
//! builder, FORCES the recorded form (`force_large_encoding` / `force_listpack`) when the
//! natural rebuild landed on a smaller one. This reproduces the exact repr the snapshot's
//! [`KvObj`] captured (the source `to_kvobj` deep-clones the live repr), so the encoding
//! round-trips byte-for-byte through the wire.
//!
//! ## Why hand-rolled (no serde)
//!
//! The same rationale as [`crate::frames`] and the Raft codec: the workspace keeps serde
//! off the transport adapters, and a fixed length-delimited layout is smaller and easier to
//! audit than a generic format. Integers are little-endian fixed width; the decoder is
//! TOTAL -- any truncated, over-long, or unknown-tag input yields `None`, never a panic.

use ironcache_storage::{
    DataType, Encoding, HashValue, ListValue, NewValueOwned, SetValue, UnixMillis, ZSetValue,
};
use ironcache_store::KvObj;
use ironcache_store::kvobj::ValueRepr;

/// The data-type wire tags (one byte). A stable on-wire enum, decoupled from the in-memory
/// [`DataType`] discriminant so a reorder of that enum never silently changes the wire.
mod type_tag {
    pub const STRING: u8 = 0;
    pub const LIST: u8 = 1;
    pub const SET: u8 = 2;
    pub const HASH: u8 = 3;
    pub const ZSET: u8 = 4;
}

/// The encoding wire tags (one byte). A stable on-wire enum, decoupled from the in-memory
/// [`Encoding`] discriminant.
mod enc_tag {
    pub const INT: u8 = 0;
    pub const EMBSTR: u8 = 1;
    pub const RAW: u8 = 2;
    pub const LISTPACK: u8 = 3;
    pub const QUICKLIST: u8 = 4;
    pub const INTSET: u8 = 5;
    pub const HASHTABLE: u8 = 6;
    pub const SKIPLIST: u8 = 7;
}

/// Map an in-memory [`DataType`] to its wire tag.
fn type_to_tag(t: DataType) -> u8 {
    match t {
        // The stream type has no value repr the store materializes yet; map it to STRING so
        // the codec is total. (No snapshot entry is a stream today.)
        DataType::String | DataType::Stream => type_tag::STRING,
        DataType::List => type_tag::LIST,
        DataType::Set => type_tag::SET,
        DataType::Hash => type_tag::HASH,
        DataType::ZSet => type_tag::ZSET,
    }
}

/// Map an in-memory [`Encoding`] to its wire tag.
fn enc_to_tag(e: Encoding) -> u8 {
    match e {
        Encoding::Int => enc_tag::INT,
        Encoding::EmbStr => enc_tag::EMBSTR,
        Encoding::Raw => enc_tag::RAW,
        Encoding::ListPack => enc_tag::LISTPACK,
        Encoding::QuickList => enc_tag::QUICKLIST,
        Encoding::IntSet => enc_tag::INTSET,
        Encoding::HashTable => enc_tag::HASHTABLE,
        Encoding::SkipList => enc_tag::SKIPLIST,
    }
}

/// Map a wire encoding tag back to an [`Encoding`], or `None` for an unknown tag.
fn tag_to_enc(tag: u8) -> Option<Encoding> {
    Some(match tag {
        enc_tag::INT => Encoding::Int,
        enc_tag::EMBSTR => Encoding::EmbStr,
        enc_tag::RAW => Encoding::Raw,
        enc_tag::LISTPACK => Encoding::ListPack,
        enc_tag::QUICKLIST => Encoding::QuickList,
        enc_tag::INTSET => Encoding::IntSet,
        enc_tag::HASHTABLE => Encoding::HashTable,
        enc_tag::SKIPLIST => Encoding::SkipList,
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Encode.
// ---------------------------------------------------------------------------

/// Append a u32 length-prefixed byte string (`[u32 len LE][bytes]`).
fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    #[allow(clippy::cast_possible_truncation)]
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

/// Append a u32 count (`[u32 LE]`).
fn put_count(out: &mut Vec<u8>, n: usize) {
    #[allow(clippy::cast_possible_truncation)]
    out.extend_from_slice(&(n as u32).to_le_bytes());
}

/// Encode a [`KvObj`] into a self-describing byte string (the inverse of
/// [`decode_kvobj`]); the pair round-trips every data type, encoding, and TTL state.
///
/// Layout: `[type_tag u8][enc_tag u8][ttl_present u8][expire_at u64 LE (iff ttl)]
/// [key: u32-len + bytes][value payload]`. The value payload depends on the type: a string
/// is one length-prefixed byte string (the canonical value bytes); a collection is a u32
/// count followed by its elements (list element / set member: one byte string each; hash:
/// field + value byte strings; zset: member byte string + an 8-byte f64 score).
#[must_use]
pub fn encode_kvobj(obj: &KvObj) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + obj.key.len());
    out.push(type_to_tag(obj.header.data_type));
    out.push(enc_to_tag(obj.header.encoding));
    match obj.expire_at {
        Some(UnixMillis(ms)) => {
            out.push(1);
            out.extend_from_slice(&ms.to_le_bytes());
        }
        None => out.push(0),
    }
    put_bytes(&mut out, &obj.key);

    match &obj.value {
        // A string-family value: carry the canonical value bytes. The int encoding stores
        // its decimal digits, so the decoder re-derives the int from them via the builder.
        ValueRepr::Int(n) => put_bytes(&mut out, n.to_string().as_bytes()),
        ValueRepr::Inline(b) | ValueRepr::Raw(b) => put_bytes(&mut out, b),
        // A LIST: its elements head-to-tail (the order `range(0, -1)` yields).
        ValueRepr::List(l) => {
            let elems = l.range(0, -1);
            put_count(&mut out, elems.len());
            for e in &elems {
                put_bytes(&mut out, e);
            }
        }
        // A HASH: its (field, value) pairs in the store's deterministic order.
        ValueRepr::Hash(h) => {
            let pairs = h.pairs();
            put_count(&mut out, pairs.len());
            for (f, v) in &pairs {
                put_bytes(&mut out, f);
                put_bytes(&mut out, v);
            }
        }
        // A SET: its members in the store's deterministic order.
        ValueRepr::Set(s) => {
            let members = s.members();
            put_count(&mut out, members.len());
            for m in &members {
                put_bytes(&mut out, m);
            }
        }
        // A ZSET: its (member, score) pairs in (score, member) order; score as f64 bits.
        ValueRepr::ZSet(z) => {
            let pairs = z.members_with_scores();
            put_count(&mut out, pairs.len());
            for (m, score) in &pairs {
                put_bytes(&mut out, m);
                out.extend_from_slice(&score.to_le_bytes());
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Decode (total: never panics, returns None on any malformed input).
// ---------------------------------------------------------------------------

/// A forward-only cursor over the encoded body, reading fixed-width LE integers and
/// length-prefixed byte strings. Every read is bounds-checked; a short read returns `None`.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    /// Read one byte, or `None` if exhausted.
    fn u8(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    /// Read a little-endian u32, or `None` if fewer than 4 bytes remain.
    fn u32(&mut self) -> Option<u32> {
        let end = self.pos.checked_add(4)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(u32::from_le_bytes(slice.try_into().ok()?))
    }

    /// Read a little-endian u64, or `None` if fewer than 8 bytes remain.
    fn u64(&mut self) -> Option<u64> {
        let end = self.pos.checked_add(8)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(u64::from_le_bytes(slice.try_into().ok()?))
    }

    /// Read a little-endian f64 (from its raw bits), or `None` if short.
    fn f64(&mut self) -> Option<f64> {
        Some(f64::from_bits(self.u64()?))
    }

    /// Read a u32-length-prefixed byte string, or `None` if the length runs past the end.
    fn bytes(&mut self) -> Option<Vec<u8>> {
        let len = self.u32()? as usize;
        let end = self.pos.checked_add(len)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice.to_vec())
    }

    /// Whether every byte has been consumed (a well-formed body has no trailing slop).
    fn is_done(&self) -> bool {
        self.pos == self.buf.len()
    }
}

/// Decode a [`KvObj`] from the bytes [`encode_kvobj`] produced, or `None` if the input is
/// truncated, over-long (trailing bytes), or carries an unknown tag. TOTAL: never panics.
///
/// The value is reconstructed FAITHFULLY: a string is rebuilt from its bytes (an int from
/// its decimal digits via the store's create-path classifier); a collection is rebuilt from
/// its logical contents via the store's create-path builder, then the recorded ENCODING is
/// FORCED so a one-way-ratcheted (promoted-then-shrunk) collection reproduces its exact
/// active repr. The result is ready for
/// [`ironcache_store::ShardStore::insert_object`](ironcache_store::ShardStore::insert_object).
#[must_use]
pub fn decode_kvobj(buf: &[u8]) -> Option<KvObj> {
    let mut r = Reader::new(buf);
    let type_tag = r.u8()?;
    let enc_tag = r.u8()?;
    let encoding = tag_to_enc(enc_tag)?;
    let expire_at = match r.u8()? {
        0 => None,
        1 => Some(UnixMillis(r.u64()?)),
        _ => return None, // a ttl-present flag other than 0/1 is malformed
    };
    let key = r.bytes()?;

    let obj = match type_tag {
        type_tag::STRING => {
            let bytes = r.bytes()?;
            if !r.is_done() {
                return None;
            }
            // Rebuild from the value bytes. The store's classifier picks int/embstr/raw; for
            // an int-encoded value the decimal digits re-classify back to int, and for a
            // value the source recorded as raw (a numeric string forced raw is not produced
            // by the store, but be faithful) we honor the recorded string encoding by
            // building bytes and stamping the header below.
            let mut obj = KvObj::from_bytes(&key, &bytes, expire_at);
            // Honor the recorded STRING encoding exactly. `from_bytes` classifies, which is
            // correct for int/embstr/raw values the store actually produces; stamping keeps
            // the embstr/raw distinction the source reported (it lives in the header, which
            // `insert_object` reads for the string blob).
            obj.header.encoding = encoding;
            obj
        }
        type_tag::LIST => {
            let elems = read_byte_vec(&mut r)?;
            if !r.is_done() {
                return None;
            }
            // The list encoding is a pure function of the element-byte total (reversible),
            // so rebuilding reproduces it; no force needed.
            KvObj::from_new_owned(&key, NewValueOwned::list(elems), expire_at)
        }
        type_tag::HASH => {
            let count = r.u32()? as usize;
            let mut pairs = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let f = r.bytes()?;
                let v = r.bytes()?;
                pairs.push((f, v));
            }
            if !r.is_done() {
                return None;
            }
            let mut obj = KvObj::from_new_owned(&key, NewValueOwned::hash(pairs), expire_at);
            // Force the one-way large form if the source was a hashtable but the rebuilt
            // contents fit a listpack (a promoted-then-shrunk hash).
            if encoding == Encoding::HashTable {
                if let Some(h) = obj.as_hash_mut() {
                    h.force_large_encoding();
                }
            }
            obj.recompute_encoding();
            obj
        }
        type_tag::SET => {
            let members = read_byte_vec(&mut r)?;
            if !r.is_done() {
                return None;
            }
            let mut obj = KvObj::from_new_owned(&key, NewValueOwned::set(members), expire_at);
            // Force the recorded form for the one-way ratchet: a hashtable set whose members
            // now fit a smaller form, or a listpack set of all-integer members that would
            // otherwise rebuild as an intset.
            if let Some(s) = obj.as_set_mut() {
                match encoding {
                    Encoding::HashTable => s.force_large_encoding(),
                    Encoding::ListPack => s.force_listpack(),
                    _ => {}
                }
            }
            obj.recompute_encoding();
            obj
        }
        type_tag::ZSET => {
            let count = r.u32()? as usize;
            let mut pairs = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let m = r.bytes()?;
                let score = r.f64()?;
                pairs.push((m, score));
            }
            if !r.is_done() {
                return None;
            }
            let mut obj = KvObj::from_new_owned(&key, NewValueOwned::zset(pairs), expire_at);
            if encoding == Encoding::SkipList {
                if let Some(z) = obj.as_zset_mut() {
                    z.force_large_encoding();
                }
            }
            obj.recompute_encoding();
            obj
        }
        _ => return None, // unknown data-type tag
    };
    Some(obj)
}

/// Read a `[u32 count]` then `count` length-prefixed byte strings (the list/set payload).
fn read_byte_vec(r: &mut Reader<'_>) -> Option<Vec<Vec<u8>>> {
    let count = r.u32()? as usize;
    let mut out = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        out.push(r.bytes()?);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::{DataType, Encoding, UnixMillis};

    /// Assert a `KvObj` survives encode -> decode with its key, type, encoding, TTL, and
    /// value all preserved. Compares the OBSERVABLE fields (the wire-faithful set), then
    /// re-encodes the decoded object and asserts the bytes match (the codec is canonical).
    fn assert_round_trips(obj: &KvObj) {
        let bytes = encode_kvobj(obj);
        let decoded = decode_kvobj(&bytes).expect("a well-formed KvObj decodes");
        assert_eq!(decoded.key, obj.key, "key round-trips");
        assert_eq!(
            decoded.header.data_type, obj.header.data_type,
            "data type round-trips for {:?}",
            obj.header.data_type
        );
        assert_eq!(
            decoded.header.encoding, obj.header.encoding,
            "encoding round-trips for {:?}/{:?}",
            obj.header.data_type, obj.header.encoding
        );
        assert_eq!(decoded.expire_at, obj.expire_at, "TTL round-trips");
        // Re-encoding the decoded object must reproduce the exact bytes (canonical codec):
        // this compares the full value payload (every element/pair) without naming the
        // private Val internals.
        let reencoded = encode_kvobj(&decoded);
        assert_eq!(reencoded, bytes, "the codec is canonical (value preserved)");
    }

    #[test]
    fn kvobj_codec_round_trips_every_type() {
        // -- STRINGS: int, embstr, raw; with and without a TTL. --
        assert_round_trips(&KvObj::from_bytes(b"k-int", b"12345", None));
        assert_round_trips(&KvObj::from_bytes(b"k-int-neg", b"-9", None));
        assert_round_trips(&KvObj::from_bytes(b"k-emb", b"short string", None));
        // A long (> embstr threshold) value is raw-encoded.
        let long = vec![b'x'; 1024];
        assert_round_trips(&KvObj::from_bytes(b"k-raw", &long, None));
        // The empty string.
        assert_round_trips(&KvObj::from_bytes(b"k-empty", b"", None));
        // With a TTL deadline.
        assert_round_trips(&KvObj::from_bytes(
            b"k-ttl",
            b"withttl",
            Some(UnixMillis(5_000)),
        ));
        assert_round_trips(&KvObj::from_int(b"k-int-ttl", 42, Some(UnixMillis(1))));
        // Verify the recorded encodings are what we expect (not all-EmbStr by accident).
        assert_eq!(
            KvObj::from_bytes(b"k", b"100", None).header.encoding,
            Encoding::Int
        );
        assert_eq!(
            KvObj::from_bytes(b"k", &long, None).header.encoding,
            Encoding::Raw
        );

        // -- LIST: small (listpack) and large (quicklist). --
        let small_list = KvObj::from_new_owned(
            b"l-small",
            NewValueOwned::list(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]),
            None,
        );
        assert_eq!(small_list.header.data_type, DataType::List);
        assert_eq!(small_list.header.encoding, Encoding::ListPack);
        assert_round_trips(&small_list);
        // A list whose element bytes exceed the listpack byte budget reports quicklist.
        let big_elem = vec![b'z'; 16 * 1024];
        let big_list = KvObj::from_new_owned(
            b"l-big",
            NewValueOwned::list(vec![big_elem.clone(), big_elem]),
            Some(UnixMillis(9)),
        );
        assert_eq!(big_list.header.encoding, Encoding::QuickList);
        assert_round_trips(&big_list);

        // -- SET: intset, listpack, hashtable. --
        let intset = KvObj::from_new_owned(
            b"s-int",
            NewValueOwned::set(vec![b"3".to_vec(), b"1".to_vec(), b"2".to_vec()]),
            None,
        );
        assert_eq!(intset.header.encoding, Encoding::IntSet);
        assert_round_trips(&intset);
        // A non-integer member forces the listpack form.
        let lp_set = KvObj::from_new_owned(
            b"s-lp",
            NewValueOwned::set(vec![b"alpha".to_vec(), b"beta".to_vec()]),
            None,
        );
        assert_eq!(lp_set.header.encoding, Encoding::ListPack);
        assert_round_trips(&lp_set);
        // Over 128 members -> hashtable.
        let big_members: Vec<Vec<u8>> = (0..200u32)
            .map(|i| format!("member-{i:04}").into_bytes())
            .collect();
        let ht_set = KvObj::from_new_owned(b"s-ht", NewValueOwned::set(big_members), None);
        assert_eq!(ht_set.header.encoding, Encoding::HashTable);
        assert_round_trips(&ht_set);

        // -- HASH: listpack and hashtable. --
        let lp_hash = KvObj::from_new_owned(
            b"h-lp",
            NewValueOwned::hash(vec![
                (b"f1".to_vec(), b"v1".to_vec()),
                (b"f2".to_vec(), b"v2".to_vec()),
            ]),
            Some(UnixMillis(7)),
        );
        assert_eq!(lp_hash.header.encoding, Encoding::ListPack);
        assert_round_trips(&lp_hash);
        // Over 512 entries -> hashtable.
        let big_pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..600u32)
            .map(|i| {
                (
                    format!("f{i:04}").into_bytes(),
                    format!("v{i}").into_bytes(),
                )
            })
            .collect();
        let ht_hash = KvObj::from_new_owned(b"h-ht", NewValueOwned::hash(big_pairs), None);
        assert_eq!(ht_hash.header.encoding, Encoding::HashTable);
        assert_round_trips(&ht_hash);

        // -- ZSET: listpack and skiplist. --
        let lp_zset = KvObj::from_new_owned(
            b"z-lp",
            NewValueOwned::zset(vec![
                (b"a".to_vec(), 1.5),
                (b"b".to_vec(), -2.0),
                (b"c".to_vec(), 3.0),
            ]),
            None,
        );
        assert_eq!(lp_zset.header.encoding, Encoding::ListPack);
        assert_round_trips(&lp_zset);
        // Over 128 members -> skiplist.
        let big_z: Vec<(Vec<u8>, f64)> = (0..200u32)
            .map(|i| (format!("m{i:04}").into_bytes(), f64::from(i)))
            .collect();
        let sl_zset =
            KvObj::from_new_owned(b"z-sl", NewValueOwned::zset(big_z), Some(UnixMillis(1)));
        assert_eq!(sl_zset.header.encoding, Encoding::SkipList);
        assert_round_trips(&sl_zset);
    }

    #[test]
    fn decode_rejects_malformed() {
        // Empty / truncated bodies. (KvObj has no PartialEq, so assert via is_none.)
        assert!(decode_kvobj(b"").is_none());
        assert!(decode_kvobj(&[type_tag::STRING]).is_none()); // no enc tag
        // A valid prefix then a truncated key length.
        assert!(decode_kvobj(&[type_tag::STRING, enc_tag::RAW, 0, 0, 0]).is_none());
        // An unknown data-type tag (with a valid header otherwise).
        let mut bad = vec![250u8, enc_tag::RAW, 0];
        bad.extend_from_slice(&0u32.to_le_bytes()); // empty key
        bad.extend_from_slice(&0u32.to_le_bytes()); // empty value
        assert!(decode_kvobj(&bad).is_none());
        // An unknown encoding tag.
        let mut bad_enc = vec![type_tag::STRING, 99u8, 0];
        bad_enc.extend_from_slice(&0u32.to_le_bytes());
        bad_enc.extend_from_slice(&0u32.to_le_bytes());
        assert!(decode_kvobj(&bad_enc).is_none());
        // A bad ttl-present flag (2).
        let mut bad_ttl = vec![type_tag::STRING, enc_tag::RAW, 2];
        bad_ttl.extend_from_slice(&0u32.to_le_bytes());
        assert!(decode_kvobj(&bad_ttl).is_none());
        // Trailing slop after a complete string body is rejected (canonical).
        let mut slop = encode_kvobj(&KvObj::from_bytes(b"k", b"v", None));
        slop.push(0xFF);
        assert!(decode_kvobj(&slop).is_none());
    }

    /// A promoted-then-shrunk hash (grown to hashtable, then most fields removed) keeps its
    /// hashtable encoding across the wire: the codec carries the tag and forces the form on
    /// decode, so the one-way ratchet is reproduced rather than silently demoted.
    #[test]
    fn shrunk_collection_keeps_its_promoted_encoding() {
        // Build a large hashtable hash, then drop it to two fields (the one-way ratchet
        // keeps it a hashtable in memory). `as_hash_mut` + the HashValue trait drive the
        // edit without naming the private Val type.
        let big_pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..600u32)
            .map(|i| (format!("f{i:04}").into_bytes(), b"v".to_vec()))
            .collect();
        let mut obj = KvObj::from_new_owned(b"h", NewValueOwned::hash(big_pairs), None);
        {
            let h = obj.as_hash_mut().expect("a hash");
            for i in 2..600u32 {
                h.del(format!("f{i:04}").as_bytes());
            }
        }
        obj.recompute_encoding();
        assert_eq!(
            obj.header.encoding,
            Encoding::HashTable,
            "the shrunk hash stays a hashtable (one-way ratchet)"
        );
        assert!(obj.value.logical_len() > 0);

        let bytes = encode_kvobj(&obj);
        let decoded = decode_kvobj(&bytes).expect("decodes");
        assert_eq!(
            decoded.header.encoding,
            Encoding::HashTable,
            "the codec reproduces the promoted-then-shrunk hashtable encoding"
        );
        // Same two fields survived.
        assert_eq!(decoded.collection_len(), Some(2));
    }
}
