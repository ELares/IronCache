// SPDX-License-Identifier: MIT OR Apache-2.0
//! The INCREMENTAL DELTA snapshot file format (#676 Phase 1b): a small per-shard file that records
//! only the keys MUTATED since a base snapshot, so a save re-reads and re-encodes a fraction of the
//! keyspace instead of all of it. Reducing the persist thread's READ footprint is the only lever
//! that moves the during-snapshot p99.9 (a shared memory-bandwidth floor, #676), and the measured
//! dirty fraction (PR #696) is small in the frequent-snapshot regime, so a delta is a real tail win
//! there.
//!
//! ## Relationship to the base format
//!
//! The BASE file ([`crate::format`]) is UNCHANGED: `dump-shard-<n>.icss` with type-less
//! `[db][len][kvobj]` records. A delta is a SEPARATE file with its OWN magic (`ICSD`) and TYPED
//! records, so a base and a delta can never be confused and the base format needs no version bump.
//! A snapshot at rest is one base plus an ordered CHAIN of deltas; the manifest (a later slice) is
//! the single commit point that names the chain and its per-file CRCs.
//!
//! ## Record types (the delta semantics)
//!
//! Each delta record is one of:
//!   - PUT `[tag=0][db][keylen][key][vallen][encoded-value]` -- the key was created/overwritten/edited
//!     or had its TTL changed; `encoded-value` is the SAME [`ironcache_repl::encode_entry_into`] blob
//!     the base uses, so the loader decodes it with the base decoder.
//!   - TOMBSTONE `[tag=1][db][keylen][key]` -- the key was deleted/expired/evicted; warm-start must
//!     REMOVE it so it does not resurrect from the base.
//!
//! The key is carried EXPLICITLY (not only inside the encoded blob) so the loader can dedup by
//! `(db, key)` across a chain WITHOUT decoding every value: applying the chain in order, a later
//! record for a key WINS (a re-write after a delete, or vice versa), which is [`fold_deltas`].
//!
//! ## Determinism (ADR-0003)
//!
//! This module reads NO clock and NO RNG. The epochs are passed in by the caller (the save id from
//! the env Clock seam). [`fold_deltas`] returns a [`std::collections::BTreeMap`] so the folded
//! effect iterates in a fixed key order regardless of the dirty set's (hashbrown) order, and the
//! byte layout + CRC are pure functions of the input.
//!
//! ## Contracts the WIRING slices must honor (this pure codec cannot enforce them alone)
//!
//! Two adversarial reviews confirmed the format is forward-safe, and surfaced three invariants that
//! live in the not-yet-written manifest/save/loader slices, NOT here. They are recorded so they are
//! not rediscovered the hard way:
//!
//! 1. **Re-shard folded effects like base records.** The base loader re-shards by the key DECODED
//!    from the value blob; [`fold_deltas`] keys on the EXPLICIT record key. Both come from the same
//!    `entry.key()` at write time, so they are byte-identical -- and the explicit key is exactly what
//!    lets the loader re-shard a `Put`/`Tombstone` with the SAME `route(key, shard_count)` filter it
//!    runs on base records WITHOUT decoding the blob. The loader slice MUST apply that filter to
//!    folded effects, or an N->M shard-count change would misroute delta keys while base keys land.
//! 2. **Manifest list-order is AUTHORITATIVE; `delta_epoch` is a cross-check.** The chain is replayed
//!    in the order the manifest lists it (that is the order `fold_deltas` is fed). `delta_epoch` is a
//!    self-describing validation signal the loader MAY assert (strictly increasing across the chain;
//!    `base_epoch` equals the loaded base's epoch), NOT a second ordering the fold consults. The codec
//!    does not force monotonicity, so the save/manifest slice must keep the two consistent.
//! 3. **Truncate to a CONTIGUOUS good PREFIX.** [`fold_deltas`] is a left-fold: last-writer-wins is
//!    correct ONLY over a contiguous prefix. A torn/missing TAIL delta is safely ignorable (the
//!    reader stops), but folding a chain across a HOLE (a dropped MIDDLE delta) would apply a newer
//!    write over an older one that logically superseded the gap -- corruption. The manifest +
//!    crash-truncation slice MUST drop everything after the first bad link (never fold a suffix across
//!    a hole); the `delta_epoch` cross-check in (2) is the cheap way to DETECT a hole.

use ironcache_store::Entry;

/// The magic at the head of every delta file: ASCII `ICSD` (IronCache Snapshot Delta), DISTINCT from
/// the base [`crate::format::MAGIC`] (`ICSS`) so a delta and a base are never mistaken for each other.
pub const DELTA_MAGIC: [u8; 4] = *b"ICSD";

/// The delta file format version. Independent of the base [`crate::format::FORMAT_VERSION`]; bumped
/// only on a breaking delta-layout change, and [`split_delta_header`] rejects an unknown one.
pub const DELTA_FORMAT_VERSION: u32 = 1;

/// The fixed delta file header: `MAGIC(4) + version(4) + shard(4) + base_epoch(8) + delta_epoch(8)`.
/// `base_epoch` names the base snapshot this delta applies ONTO (so a delta whose base is not the
/// loaded base is rejected); `delta_epoch` orders the chain. The body CRC lives in the manifest
/// entry (like a base file), so a torn delta is caught before decode.
pub const DELTA_HEADER_LEN: usize = 4 + 4 + 4 + 8 + 8;

/// Record tag: a PUT (create/overwrite/edit/TTL-change), carrying the encoded value.
const TAG_PUT: u8 = 0;
/// Record tag: a TOMBSTONE (delete/expiry/eviction), carrying only the key.
const TAG_TOMBSTONE: u8 = 1;

/// Write the fixed delta file header into `out`.
pub fn put_delta_header(out: &mut Vec<u8>, shard: u32, base_epoch: u64, delta_epoch: u64) {
    out.extend_from_slice(&DELTA_MAGIC);
    out.extend_from_slice(&DELTA_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&shard.to_le_bytes());
    out.extend_from_slice(&base_epoch.to_le_bytes());
    out.extend_from_slice(&delta_epoch.to_le_bytes());
}

/// Validate + strip a delta file header, returning `(base_epoch, delta_epoch, record-body)`. `None`
/// if the header is short / wrong magic / unknown version / wrong shard (so load treats the file as
/// no-delta rather than mis-decoding it), mirroring [`crate::format::split_shard_header`].
#[must_use]
pub fn split_delta_header(buf: &[u8], shard: u32) -> Option<(u64, u64, &[u8])> {
    if buf.len() < DELTA_HEADER_LEN {
        return None;
    }
    if buf[0..4] != DELTA_MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().ok()?);
    if version != DELTA_FORMAT_VERSION {
        return None;
    }
    let file_shard = u32::from_le_bytes(buf[8..12].try_into().ok()?);
    if file_shard != shard {
        return None;
    }
    let base_epoch = u64::from_le_bytes(buf[12..20].try_into().ok()?);
    let delta_epoch = u64::from_le_bytes(buf[20..28].try_into().ok()?);
    Some((base_epoch, delta_epoch, &buf[DELTA_HEADER_LEN..]))
}

/// Append a `[u32 len LE][bytes]` field. The length is stored as u32 (matching the base
/// `put_record`); a >=4 GiB key/value would truncate silently, so this fails LOUD in debug (a
/// round-trip-equality assert on the cast) rather than encoding a corrupt record if a future
/// save-wiring caller is ever handed an oversized one. Cache keys/values are far below this.
fn put_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    #[allow(clippy::cast_possible_truncation)]
    let len = bytes.len() as u32;
    debug_assert_eq!(
        len as usize,
        bytes.len(),
        "delta record field exceeds u32 length"
    );
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Append a PUT record: `[tag=0][db][keylen][key][vallen][encoded-value]`.
pub fn put_put_record(out: &mut Vec<u8>, db: u32, key: &[u8], encoded_value: &[u8]) {
    out.push(TAG_PUT);
    out.extend_from_slice(&db.to_le_bytes());
    put_len_prefixed(out, key);
    put_len_prefixed(out, encoded_value);
}

/// Append a TOMBSTONE record: `[tag=1][db][keylen][key]`.
pub fn put_tombstone_record(out: &mut Vec<u8>, db: u32, key: &[u8]) {
    out.push(TAG_TOMBSTONE);
    out.extend_from_slice(&db.to_le_bytes());
    put_len_prefixed(out, key);
}

/// One decoded delta record: a PUT (with the encoded value blob) or a TOMBSTONE (key only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaRecord<'a> {
    /// The key was written; `value` is the [`ironcache_repl::encode_entry_into`] blob to decode.
    Put {
        /// The logical database.
        db: u32,
        /// The key bytes.
        key: &'a [u8],
        /// The encoded value record (base-format blob).
        value: &'a [u8],
    },
    /// The key was removed; warm-start must delete it so it does not resurrect from the base.
    Tombstone {
        /// The logical database.
        db: u32,
        /// The key bytes.
        key: &'a [u8],
    },
}

/// Iterate the records in a delta file body (the slice [`split_delta_header`] returned). TOTAL: a
/// truncated / malformed tail returns `None` from `next_record` and ends the iteration, never a
/// panic (the manifest body CRC already rejected a torn file; this is the belt-and-suspenders decode
/// that also bounds the chain-fold on a Phase-2 crash-truncated delta).
pub struct DeltaRecordReader<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> DeltaRecordReader<'a> {
    /// Start reading records from a delta file body.
    #[must_use]
    pub fn new(body: &'a [u8]) -> Self {
        DeltaRecordReader { body, pos: 0 }
    }

    /// The next record, or `None` at the end (or on a truncated / unknown-tag record).
    pub fn next_record(&mut self) -> Option<DeltaRecord<'a>> {
        if self.pos == self.body.len() {
            return None;
        }
        let tag = *self.body.get(self.pos)?;
        let mut p = self.pos.checked_add(1)?;
        let db = u32::from_le_bytes(self.body.get(p..p.checked_add(4)?)?.try_into().ok()?);
        p = p.checked_add(4)?;
        let keylen =
            u32::from_le_bytes(self.body.get(p..p.checked_add(4)?)?.try_into().ok()?) as usize;
        p = p.checked_add(4)?;
        let key = self.body.get(p..p.checked_add(keylen)?)?;
        p = p.checked_add(keylen)?;
        match tag {
            TAG_TOMBSTONE => {
                self.pos = p;
                Some(DeltaRecord::Tombstone { db, key })
            }
            TAG_PUT => {
                let vallen =
                    u32::from_le_bytes(self.body.get(p..p.checked_add(4)?)?.try_into().ok()?)
                        as usize;
                p = p.checked_add(4)?;
                let value = self.body.get(p..p.checked_add(vallen)?)?;
                self.pos = p.checked_add(vallen)?;
                Some(DeltaRecord::Put { db, key, value })
            }
            _ => None, // an unknown tag: stop (a torn / foreign tail), never mis-parse.
        }
    }
}

/// The sealed bytes of a delta file plus the counts and CRC the manifest chain records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaDump {
    /// The complete delta file bytes (header + record body).
    pub bytes: Vec<u8>,
    /// The number of PUT records.
    pub puts: u64,
    /// The number of TOMBSTONE records.
    pub tombstones: u64,
    /// The CRC-32 of the record BODY (after the header), recorded in the manifest to detect a torn
    /// delta on load, exactly as a base file's body CRC does.
    pub crc: u32,
}

/// INCREMENTAL builder for a delta file, the [`crate::ShardDumpBuilder`] analog for the mutated
/// keys: [`push_put`](Self::push_put) a written key (encoded with the same reused-scratch
/// [`ironcache_repl::encode_entry_into`] path the base uses) or [`push_tombstone`](Self::push_tombstone)
/// a removed key, then [`finish`](Self::finish) to seal the file bytes + the body CRC.
#[derive(Debug, Default)]
pub struct DeltaBuilder {
    /// The typed record body accumulated so far (header prepended at `finish`).
    body: Vec<u8>,
    /// PUT records so far.
    puts: u64,
    /// TOMBSTONE records so far.
    tombstones: u64,
    /// A reused per-entry encode buffer (#676 Phase 0 pattern): no fresh per-entry allocation.
    scratch: Vec<u8>,
}

impl DeltaBuilder {
    /// A fresh, empty delta builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a PUT for a written key, read directly from its stored [`Entry`]. The value is encoded
    /// with [`ironcache_repl::encode_entry_into`] into the reused scratch, byte-identical to the base
    /// record for the same key, so the loader decodes a delta PUT with the base decoder. The key is
    /// also written explicitly for chain dedup.
    pub fn push_put(&mut self, db: u32, entry: &Entry) {
        self.scratch.clear();
        ironcache_repl::encode_entry_into(&mut self.scratch, entry);
        put_put_record(&mut self.body, db, entry.key(), &self.scratch);
        self.puts += 1;
    }

    /// Append a TOMBSTONE for a removed key.
    pub fn push_tombstone(&mut self, db: u32, key: &[u8]) {
        put_tombstone_record(&mut self.body, db, key);
        self.tombstones += 1;
    }

    /// Whether this delta has recorded no records (an empty window: nothing was dirtied). The caller
    /// can skip writing an empty delta file.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.puts == 0 && self.tombstones == 0
    }

    /// SEAL the accumulated body into a [`DeltaDump`]: prepend the delta header (`shard`,
    /// `base_epoch`, `delta_epoch`) and compute the body CRC recorded in the manifest chain.
    #[must_use]
    pub fn finish(self, shard: u32, base_epoch: u64, delta_epoch: u64) -> DeltaDump {
        let crc = crate::format::crc32(&self.body);
        let mut bytes = Vec::with_capacity(DELTA_HEADER_LEN + self.body.len());
        put_delta_header(&mut bytes, shard, base_epoch, delta_epoch);
        bytes.extend_from_slice(&self.body);
        DeltaDump {
            bytes,
            puts: self.puts,
            tombstones: self.tombstones,
            crc,
        }
    }
}

/// The NET effect of a delta chain on one `(db, key)`, after applying every delta in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaEffect {
    /// The key's final state is this encoded value (the last PUT wins); the loader decodes + inserts.
    Put(Vec<u8>),
    /// The key's final state is REMOVED (the last record was a TOMBSTONE); the loader deletes it.
    Tombstone,
}

/// FOLD a chain of delta record bodies (in manifest-chain order, oldest first) into the NET per-key
/// effect: the LAST record for each `(db, key)` wins, so a re-write after a delete is a `Put` and a
/// delete after a write is a `Tombstone`. This is the loader's replay collapsed to a single pass:
/// load the base, then apply this map (`Put` -> decode + insert/overwrite, `Tombstone` -> remove).
///
/// The result is a [`BTreeMap`] so iteration order is a fixed function of the keys (ADR-0003), and
/// each body is decoded totally (a truncated tail simply stops that body's contribution).
///
/// [`BTreeMap`]: std::collections::BTreeMap
#[must_use]
pub fn fold_deltas<'a>(
    delta_bodies: impl IntoIterator<Item = &'a [u8]>,
) -> std::collections::BTreeMap<(u32, Vec<u8>), DeltaEffect> {
    let mut net: std::collections::BTreeMap<(u32, Vec<u8>), DeltaEffect> =
        std::collections::BTreeMap::new();
    for body in delta_bodies {
        let mut reader = DeltaRecordReader::new(body);
        while let Some(rec) = reader.next_record() {
            match rec {
                DeltaRecord::Put { db, key, value } => {
                    net.insert((db, key.to_vec()), DeltaEffect::Put(value.to_vec()));
                }
                DeltaRecord::Tombstone { db, key } => {
                    net.insert((db, key.to_vec()), DeltaEffect::Tombstone);
                }
            }
        }
    }
    net
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A PUT then a TOMBSTONE for one shard, read back by the reader, prove the record round-trip and
    /// the header strip.
    #[test]
    fn header_and_records_round_trip() {
        let mut b = DeltaBuilder::new();
        // push raw records via the free fns (an Entry fixture is heavier; the codec is what we test).
        put_put_record(&mut b.body, 0, b"k1", b"encoded-v1");
        b.puts += 1;
        put_tombstone_record(&mut b.body, 2, b"gone");
        b.tombstones += 1;
        let dump = b.finish(5, 100, 101);
        assert_eq!(dump.puts, 1);
        assert_eq!(dump.tombstones, 1);

        let (base_epoch, delta_epoch, body) =
            split_delta_header(&dump.bytes, 5).expect("valid delta header for shard 5");
        assert_eq!((base_epoch, delta_epoch), (100, 101));
        assert_eq!(crate::format::crc32(body), dump.crc);

        let mut r = DeltaRecordReader::new(body);
        assert_eq!(
            r.next_record(),
            Some(DeltaRecord::Put {
                db: 0,
                key: b"k1",
                value: b"encoded-v1"
            })
        );
        assert_eq!(
            r.next_record(),
            Some(DeltaRecord::Tombstone {
                db: 2,
                key: b"gone"
            })
        );
        assert_eq!(r.next_record(), None);
    }

    #[test]
    fn header_rejects_wrong_magic_version_and_shard() {
        let mut b = DeltaBuilder::new();
        put_tombstone_record(&mut b.body, 0, b"x");
        b.tombstones += 1;
        let dump = b.finish(3, 1, 2);

        // Wrong shard: the manifest pairs a delta file with its shard.
        assert!(split_delta_header(&dump.bytes, 4).is_none());
        // Short / foreign buffers.
        assert!(split_delta_header(b"ICSD", 3).is_none());
        assert!(split_delta_header(b"XXXXxxxxyyyyzzzzzzzzaaaaaaaa", 3).is_none());
        // A base file (ICSS magic) is not a delta.
        let mut base = Vec::new();
        crate::format::put_shard_header(&mut base, 3);
        assert!(split_delta_header(&base, 3).is_none());
    }

    #[test]
    fn record_reader_tolerates_truncated_and_unknown_tag() {
        // A complete tombstone then a truncated PUT (tag + db + keylen promising 50, but no key).
        let mut body = Vec::new();
        put_tombstone_record(&mut body, 0, b"ok");
        body.push(TAG_PUT);
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&50u32.to_le_bytes());
        let mut r = DeltaRecordReader::new(&body);
        assert_eq!(
            r.next_record(),
            Some(DeltaRecord::Tombstone { db: 0, key: b"ok" })
        );
        assert_eq!(r.next_record(), None); // truncated tail -> stop, no panic.

        // An unknown tag stops the iteration too.
        let mut bad = Vec::new();
        put_tombstone_record(&mut bad, 0, b"first");
        bad.push(9); // unknown tag
        let mut r2 = DeltaRecordReader::new(&bad);
        assert!(matches!(
            r2.next_record(),
            Some(DeltaRecord::Tombstone { .. })
        ));
        assert_eq!(r2.next_record(), None);
    }

    #[test]
    fn fold_applies_later_wins_and_tombstone_removes() {
        // Chain of three delta bodies for one shard, applied oldest-first.
        let mut d1 = Vec::new();
        put_put_record(&mut d1, 0, b"a", b"a-v1");
        put_put_record(&mut d1, 0, b"b", b"b-v1");
        put_tombstone_record(&mut d1, 0, b"c"); // c deleted

        let mut d2 = Vec::new();
        put_tombstone_record(&mut d2, 0, b"a"); // a now deleted (later wins over d1's PUT)
        put_put_record(&mut d2, 0, b"c", b"c-reborn"); // c re-created (later wins over d1's tombstone)

        let mut d3 = Vec::new();
        put_put_record(&mut d3, 0, b"b", b"b-v2"); // b overwritten
        put_put_record(&mut d3, 1, b"a", b"db1-a"); // a different (db,key)

        let net = fold_deltas([d1.as_slice(), d2.as_slice(), d3.as_slice()]);

        assert_eq!(net.get(&(0, b"a".to_vec())), Some(&DeltaEffect::Tombstone));
        assert_eq!(
            net.get(&(0, b"b".to_vec())),
            Some(&DeltaEffect::Put(b"b-v2".to_vec()))
        );
        assert_eq!(
            net.get(&(0, b"c".to_vec())),
            Some(&DeltaEffect::Put(b"c-reborn".to_vec()))
        );
        assert_eq!(
            net.get(&(1, b"a".to_vec())),
            Some(&DeltaEffect::Put(b"db1-a".to_vec()))
        );
        assert_eq!(net.len(), 4);
    }

    #[test]
    fn push_put_encodes_a_loader_decodable_value_from_a_real_entry() {
        use ironcache_storage::{ExpireWrite, NewValue, Store, UnixMillis};
        use ironcache_store::ShardStore;
        let now = UnixMillis(0);
        let mut store: ShardStore = ShardStore::new(1);
        store.upsert(
            0,
            b"hello",
            NewValue::Bytes(b"world"),
            ExpireWrite::Clear,
            now,
        );

        // Freeze and push every resident entry as a delta PUT (the save-path shape).
        let frozen = store.begin_save();
        let mut b = DeltaBuilder::new();
        for slot in &frozen {
            let db = slot.db();
            for entry in slot.entries() {
                b.push_put(db, entry);
            }
        }
        assert_eq!(b.puts, 1);
        assert_eq!(b.tombstones, 0);
        let dump = b.finish(0, 10, 11);

        let (_, _, body) = split_delta_header(&dump.bytes, 0).expect("valid delta header");
        let mut r = DeltaRecordReader::new(body);
        match r.next_record() {
            Some(DeltaRecord::Put { db, key, value }) => {
                assert_eq!(db, 0);
                assert_eq!(key, b"hello", "the PUT carries the explicit key");
                // The value blob is the SAME base-format encoding, so the base decoder reads it.
                assert!(
                    ironcache_repl::decode_kvobj(value).is_some(),
                    "a delta PUT value decodes with the base decoder"
                );
            }
            other => panic!("expected a PUT record, got {other:?}"),
        }
        assert_eq!(r.next_record(), None);
        drop(frozen);
        store.end_save();
    }

    #[test]
    fn fold_stops_a_body_at_a_truncated_tail() {
        // fold_deltas drives the total reader, so a truncated tail contributes its good prefix and
        // stops -- never a panic, and the good records before the tear still land.
        let mut torn = Vec::new();
        put_put_record(&mut torn, 0, b"good", b"v");
        torn.push(TAG_PUT); // a truncated PUT: tag + db + keylen promising 9, no key.
        torn.extend_from_slice(&0u32.to_le_bytes());
        torn.extend_from_slice(&9u32.to_le_bytes());
        let net = fold_deltas([torn.as_slice()]);
        assert_eq!(
            net.get(&(0, b"good".to_vec())),
            Some(&DeltaEffect::Put(b"v".to_vec()))
        );
        assert_eq!(
            net.len(),
            1,
            "the truncated tail contributed nothing, no panic"
        );
    }

    #[test]
    fn fold_is_deterministic_order() {
        // The folded map iterates in a fixed key order regardless of insertion order (ADR-0003).
        let mut d = Vec::new();
        for k in [b"z".as_slice(), b"a", b"m", b"b"] {
            put_put_record(&mut d, 0, k, b"v");
        }
        let net = fold_deltas([d.as_slice()]);
        let keys: Vec<_> = net.keys().map(|(_, k)| k.clone()).collect();
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"b".to_vec(), b"m".to_vec(), b"z".to_vec()]
        );
    }
}
