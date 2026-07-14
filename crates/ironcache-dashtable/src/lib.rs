// SPDX-License-Identifier: MIT OR Apache-2.0
//! A standalone Dash-style extendible-hashing table (#285, STAGES 1-2: the algorithm core + the
//! cache-mode segment-local eviction).
//!
//! This crate validates the NOVEL part of the Dash design (DASHTABLE.md) in isolation, with zero
//! blast radius on the store and zero `unsafe` (so `miri` is trivial): the extendible DIRECTORY, the
//! per-segment LOCAL depth, the SEGMENT SPLIT on overflow, the DIRECTORY DOUBLING when a split would
//! exceed the global depth, and the 1-byte FINGERPRINT that gates a lookup so it skips non-matching
//! slots. It is a correctness reference, NOT yet the production index.
//!
//! ## Cache mode (STAGE 2, DASHTABLE.md "Segment-local O(1) eviction")
//!
//! [`Dashtable::insert_cache`] is the CACHE-mode insert: instead of SPLITTING a full segment (which
//! grows the table, the datastore behavior), it EVICTS the coldest slot IN THAT SEGMENT and places
//! the new key, so memory stays bounded and eviction touches O(`SEGMENT_CAP`) = O(1) slots with no
//! table-wide scan and no per-key side state. The victim is the slot minimizing the total order
//! `(freq, scan_hash, key)`, the SAME SHAPE the store's `refill_evict_pool` `ColdEntry` uses
//! (`freq, scan_h, key[, db]`; EVICTION.md, ADR-0003). Both the 2-bit frequency and the `scan_hash`
//! are supplied by the CALLER (`freq_of` reads the freq out of the value = freq-in-object; `scan_hash`
//! is the caller's scan-order hash over the key), so when the store wires this table it passes its
//! OWN `freq` + `scan_hash` and the victim is then byte-identical to `refill_evict_pool`'s choice by
//! construction; two shards with identical state evict identically. Passing the scan hash in (rather
//! than hashing the key with this crate's internal directory hash, which is a DIFFERENT function)
//! is what makes that parity real rather than merely same-shaped. [`Dashtable::with_directory_bits`]
//! pre-sizes the segment array for a known working set (a cache is sized up front; growth is by
//! eviction, not splits).
//!
//! ## What is deliberately deferred (later stages of DASHTABLE.md)
//!
//! - The dense, cache-line-packed `unsafe` layout (`Box<[NonNull<Segment>]>` directory, raw segment
//!   slots) that delivers the actual throughput/memory win. Here a [`Segment`] is a safe `Vec` of
//!   slots and the directory is a `Vec<usize>` of segment indices (multiple directory entries may
//!   hold the same index = the aliasing of extendible hashing), so the algorithm is provably correct
//!   before any pointer work.
//! - The bucketized probing (target + neighbor bucket) and stash buckets that raise load factor and
//!   cache efficiency. Here a segment is a single flat slot pool; a lookup is an O(`SEGMENT_CAP`)
//!   fingerprint-gated scan (`SEGMENT_CAP` is a small constant), which is correct but not yet
//!   cache-optimal.
//! - Wiring into the store behind a feature flag (stage 3), and the pinned-Linux + DragonflyDB
//!   head-to-heads that PROVE the memory/throughput win (stage 4). The eviction VICTIM-QUALITY is
//!   validated here on macOS by the model test; only the perf claim needs the Linux/bench harness.
//!
//! Directory mechanics use the TOP `global_depth` bits of the key hash as the directory index and a
//! disjoint hash byte as the fingerprint, exactly as DASHTABLE.md specifies.

pub mod index;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// The maximum live records a segment holds before an insert forces a SPLIT. A small constant here so
/// the stage-1 tests exercise many splits + directory doublings with modest key counts; the dense
/// store-wired layout (later stage) tunes the real geometry (DASHTABLE.md: Dragonfly uses 840).
const SEGMENT_CAP: usize = 16;

/// The split-retry bound: a backstop on loop iterations per insert. The REAL pathological-collision
/// guards are [`MAX_GLOBAL_DEPTH`] and the futility fast-path in [`Dashtable::insert`]: a bound on
/// SPLIT COUNT alone does NOT bound memory, because each futile split of a segment already at the
/// global depth DOUBLES the directory (records sharing the top-k hash bits drive 2^k directory
/// entries long before 64 splits elapse).
const MAX_SPLITS: u32 = 64;

/// The hard cap on the directory depth, shared by [`Dashtable`] and [`index::DashIndex`]: splitting
/// a segment already at this global depth is refused and the record FORCE-PLACED past the segment
/// cap instead (the segment grows linearly -- correct, just slower to probe). At depth 20 the
/// directory is at most 2^20 entries and holds tens of millions of records at segment-cap load,
/// unreachable with a well-mixed 64-bit hash at this table's scale, so the cap never trips on
/// legitimate load; it exists so hash-crafted (or astronomically unlucky) shared top-bit prefixes
/// cannot drive unbounded directory doubling.
pub(crate) const MAX_GLOBAL_DEPTH: u8 = 20;

/// One stored record: the key, the value, and the 1-byte fingerprint of the key hash (the fast
/// reject that lets a lookup skip non-matching slots without comparing keys).
struct Slot<K, V> {
    fingerprint: u8,
    key: K,
    value: V,
}

/// One segment: a flat pool of up to [`SEGMENT_CAP`] slots plus its extendible-hashing LOCAL depth
/// (how many top hash bits distinguish the keys routed here). Owned once in [`Dashtable::segments`];
/// the directory only stores its index, so aliased directory entries never double-own a segment.
struct Segment<K, V> {
    local_depth: u8,
    slots: Vec<Slot<K, V>>,
}

/// A Dash-style extendible-hashing table mapping `K` to `V`.
///
/// The DIRECTORY is `2^global_depth` entries, each an index into [`Self::segments`]; the top
/// `global_depth` bits of a key's hash pick the directory entry. A segment whose `local_depth` is
/// below `global_depth` is shared by several directory entries (aliasing). Growth is incremental: a
/// full segment SPLITS (one new segment, records repartitioned by one more hash bit), and only when a
/// split would exceed the global depth does the DIRECTORY DOUBLE (a pointer-array copy, never a
/// rehash of records). There is no power-of-two doubling trough.
pub struct Dashtable<K, V> {
    global_depth: u8,
    directory: Vec<usize>,
    segments: Vec<Segment<K, V>>,
    len: usize,
}

/// Hash a key through a fixed-seed [`DefaultHasher`] (deterministic across runs and processes, unlike
/// `RandomState`), so the directory index + fingerprint are reproducible (the determinism the store
/// integration will need; ADR-0003).
fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    h.finish()
}

/// The 1-byte fingerprint of a hash: the low byte, disjoint from the TOP bits the directory indexes,
/// so it carries independent entropy. Shared with [`index::DashIndex`] (`pub(crate)`) so both tables
/// route + fingerprint identically from the same 64-bit hash.
#[allow(clippy::cast_possible_truncation)] // masked to one byte, so the cast is exact.
pub(crate) fn fingerprint(h: u64) -> u8 {
    (h & 0xFF) as u8
}

/// Whether the `n`-th bit from the TOP of `h` (1-indexed) is set. `n` is in `1..=64`. Shared with
/// [`index::DashIndex`] (`pub(crate)`), same rationale as [`fingerprint`].
pub(crate) fn bit_from_top(h: u64, n: u8) -> bool {
    (h >> (64 - u32::from(n))) & 1 == 1
}

impl<K, V> Default for Dashtable<K, V>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Dashtable<K, V>
where
    K: Hash + Eq,
{
    /// A fresh table: one segment at global/local depth 0 (the whole keyspace maps to it until the
    /// first overflow).
    #[must_use]
    pub fn new() -> Self {
        Dashtable {
            global_depth: 0,
            directory: vec![0],
            segments: vec![Segment {
                local_depth: 0,
                slots: Vec::new(),
            }],
            len: 0,
        }
    }

    /// A table PRE-SIZED to `2^dir_bits` DISTINCT segments (directory and every local depth =
    /// `dir_bits`, each directory entry owning its own segment, no aliasing). This is the CACHE-mode
    /// shape: the working-set size is known up front and growth is by segment-local EVICTION
    /// ([`Self::insert_cache`]), not by splitting, so the segment array is fixed. `dir_bits` must be
    /// `<= 63` (the top-bits directory index).
    #[must_use]
    pub fn with_directory_bits(dir_bits: u8) -> Self {
        assert!(dir_bits <= 63, "dir_bits must be <= 63");
        let n = 1usize << dir_bits;
        Dashtable {
            global_depth: dir_bits,
            directory: (0..n).collect(),
            segments: (0..n)
                .map(|_| Segment {
                    local_depth: dir_bits,
                    slots: Vec::new(),
                })
                .collect(),
            len: 0,
        }
    }

    /// The number of live records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the table holds no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The current global depth (the directory holds `2^global_depth` entries). Introspection for
    /// tests + future tuning.
    #[must_use]
    pub fn global_depth(&self) -> u8 {
        self.global_depth
    }

    /// The number of live segments (grows by one per split). Introspection for tests.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// The directory index for a hash: the top `global_depth` bits (0 when the directory is a single
    /// entry, where a 64-bit shift would be undefined).
    #[allow(clippy::cast_possible_truncation)] // < 2^global_depth <= directory.len(), always in range.
    fn dir_index(&self, h: u64) -> usize {
        if self.global_depth == 0 {
            0
        } else {
            (h >> (64 - u32::from(self.global_depth))) as usize
        }
    }

    /// Look up `key`, returning its value if present. A fingerprint-gated scan of the routed
    /// segment: only slots whose fingerprint matches the key's hash byte compare keys.
    #[must_use]
    pub fn get(&self, key: &K) -> Option<&V> {
        let h = hash_key(key);
        let fp = fingerprint(h);
        let seg = &self.segments[self.directory[self.dir_index(h)]];
        seg.slots
            .iter()
            .find(|s| s.fingerprint == fp && &s.key == key)
            .map(|s| &s.value)
    }

    /// Insert `key` -> `value`, returning the previous value if the key was present (an overwrite).
    /// A new key that overflows its segment triggers a SPLIT (and a directory DOUBLE if needed)
    /// before placement, so the table grows incrementally with no rehash spike.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let h = hash_key(&key);
        let fp = fingerprint(h);
        // Overwrite in place if the key already exists in its routed segment.
        let si = self.directory[self.dir_index(h)];
        if let Some(slot) = self.segments[si]
            .slots
            .iter_mut()
            .find(|s| s.fingerprint == fp && s.key == key)
        {
            return Some(std::mem::replace(&mut slot.value, value));
        }
        // A new key: make room (splitting as needed), then place it ONCE. Guards mirror
        // [`index::DashIndex::entry`] (see there): FUTILITY force-places when the full
        // segment's records all hash identically to the incoming key (no depth separates
        // them; splitting would only double the directory forever), the depth cap bounds
        // the directory against long-shared top-bit prefixes, and MAX_SPLITS backstops the
        // iteration count. A force-placed segment grows past SEGMENT_CAP linearly, which
        // stays correct (probes scan it), just slower.
        let mut splits = 0;
        loop {
            let si = self.directory[self.dir_index(h)];
            let seg = &self.segments[si];
            if seg.slots.len() < SEGMENT_CAP || splits >= MAX_SPLITS {
                break;
            }
            if seg.slots.iter().all(|s| s.fingerprint == fp)
                && seg.slots.iter().all(|s| hash_key(&s.key) == h)
            {
                break; // fully collided with the incoming key
            }
            if seg.local_depth == self.global_depth && self.global_depth >= MAX_GLOBAL_DEPTH {
                break; // splitting would double the directory past the hard cap
            }
            self.split(self.dir_index(h));
            splits += 1;
        }
        let si = self.directory[self.dir_index(h)];
        self.segments[si].slots.push(Slot {
            fingerprint: fp,
            key,
            value,
        });
        self.len += 1;
        None
    }

    /// Remove `key`, returning its value if it was present.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let h = hash_key(key);
        let fp = fingerprint(h);
        let si = self.directory[self.dir_index(h)];
        let slots = &mut self.segments[si].slots;
        let pos = slots
            .iter()
            .position(|s| s.fingerprint == fp && &s.key == key)?;
        self.len -= 1;
        Some(slots.swap_remove(pos).value)
    }

    /// Iterate every live `(key, value)`. Walks [`Self::segments`] directly (each owned once), so an
    /// aliased segment is visited exactly once. Order is unspecified.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.segments
            .iter()
            .flat_map(|seg| seg.slots.iter().map(|s| (&s.key, &s.value)))
    }

    /// Double the directory: every entry is duplicated (`new[i] = old[i >> 1]`), so each old prefix
    /// `k` becomes the two prefixes `2k` and `2k+1`, both still pointing at the same segment until a
    /// split separates them. Moves no records; bumps the global depth.
    fn double_directory(&mut self) {
        let old_len = self.directory.len();
        let mut next = Vec::with_capacity(old_len * 2);
        for i in 0..old_len * 2 {
            next.push(self.directory[i >> 1]);
        }
        self.directory = next;
        self.global_depth += 1;
    }

    /// Split the segment routed to by directory entry `dir_idx`: allocate a buddy segment at
    /// `local_depth + 1`, repartition the records between the two by the `(local_depth + 1)`-th top
    /// hash bit, and re-point the directory entries whose `(local_depth + 1)`-th prefix bit is 1 at
    /// the buddy. Doubles the directory first if the segment is already at the global depth.
    fn split(&mut self, dir_idx: usize) {
        let seg_idx = self.directory[dir_idx];
        let local = self.segments[seg_idx].local_depth;
        if local == self.global_depth {
            self.double_directory();
        }
        let new_local = local + 1;
        let buddy_idx = self.segments.len();
        self.segments.push(Segment {
            local_depth: new_local,
            slots: Vec::new(),
        });
        self.segments[seg_idx].local_depth = new_local;
        // Repartition the old segment's records: those whose new-local-th top hash bit is 1 move to
        // the buddy, the rest stay.
        let old_slots = std::mem::take(&mut self.segments[seg_idx].slots);
        for slot in old_slots {
            let h = hash_key(&slot.key);
            if bit_from_top(h, new_local) {
                self.segments[buddy_idx].slots.push(slot);
            } else {
                self.segments[seg_idx].slots.push(slot);
            }
        }
        // Re-point the directory: an entry that pointed at the old segment and whose prefix has the
        // new-local-th top bit set now points at the buddy. For directory index `i` (a top
        // `global_depth`-bit prefix), that bit is bit `global_depth - new_local` of `i`.
        let shift = u32::from(self.global_depth - new_local);
        for (i, entry) in self.directory.iter_mut().enumerate() {
            if *entry == seg_idx && (i >> shift) & 1 == 1 {
                *entry = buddy_idx;
            }
        }
    }
}

/// The outcome of a CACHE-mode [`Dashtable::insert_cache`].
#[derive(Debug, PartialEq, Eq)]
pub enum CacheInsert<K, V> {
    /// The key was new and there was room in its segment; nothing was evicted. `len` grew by one.
    Inserted,
    /// The key already existed; its value was overwritten in place. Carries the PREVIOUS value.
    /// `len` is unchanged.
    Overwrote(V),
    /// The key was new but its segment was FULL, so the coldest slot was EVICTED to make room and the
    /// new key placed. Carries the evicted `(key, value)` (so the caller can release its accounting).
    /// `len` is unchanged (one out, one in).
    Evicted {
        /// The evicted key.
        key: K,
        /// The evicted value.
        value: V,
    },
}

impl<K, V> Dashtable<K, V>
where
    K: Hash + Eq + Ord,
{
    /// CACHE-mode insert: `key` -> `value`, EVICTING the coldest slot in the routed segment instead of
    /// splitting when that segment is full (the (b) lever of #285, DASHTABLE.md). An existing key is
    /// overwritten in place. `freq_of` reads the 2-bit frequency out of the value (freq-in-object,
    /// EVICTION.md); `scan_hash_of` is the caller's scan-order hash over the key (the store passes its
    /// own `scan_hash`, so the tie-break matches `refill_evict_pool` exactly). The evicted victim is
    /// the slot minimizing the deterministic total order `(freq, scan_hash, key)`, so the selection is
    /// O(`SEGMENT_CAP`) = O(1) and reproducible across shards (ADR-0003).
    pub fn insert_cache<F, H>(
        &mut self,
        key: K,
        value: V,
        freq_of: F,
        scan_hash_of: H,
    ) -> CacheInsert<K, V>
    where
        F: Fn(&V) -> u8,
        H: Fn(&K) -> u64,
    {
        let h = hash_key(&key);
        let fp = fingerprint(h);
        let si = self.directory[self.dir_index(h)];

        // Overwrite in place if present (no eviction, len unchanged).
        if let Some(slot) = self.segments[si]
            .slots
            .iter_mut()
            .find(|s| s.fingerprint == fp && s.key == key)
        {
            return CacheInsert::Overwrote(std::mem::replace(&mut slot.value, value));
        }

        // A full segment EVICTS its coldest slot (cache mode) rather than splitting.
        if self.segments[si].slots.len() >= SEGMENT_CAP {
            let victim = Self::evict_victim(&self.segments[si].slots, &freq_of, &scan_hash_of);
            // swap_remove is O(1) and order-independent (a segment is an unordered pool); then place
            // the new slot. Net len unchanged.
            let evicted = self.segments[si].slots.swap_remove(victim);
            self.segments[si].slots.push(Slot {
                fingerprint: fp,
                key,
                value,
            });
            return CacheInsert::Evicted {
                key: evicted.key,
                value: evicted.value,
            };
        }

        // Room to spare: place and grow.
        self.segments[si].slots.push(Slot {
            fingerprint: fp,
            key,
            value,
        });
        self.len += 1;
        CacheInsert::Inserted
    }

    /// The index of the segment's COLDEST slot by the total order `(freq, scan_hash, key)` (the
    /// eviction victim). `scan_hash_of` is the caller's scan-order hash (the store's `scan_hash` when
    /// wired), NOT this crate's internal directory hash [`hash_key`] -- passing it in is what makes the
    /// victim identical to the store's, since the two hash functions differ. The final `key` tie-break
    /// makes the order TOTAL so the choice is unique + reproducible. Panics only if called on an empty
    /// segment (never, in `insert_cache`, which checks fullness first).
    fn evict_victim<F, H>(slots: &[Slot<K, V>], freq_of: &F, scan_hash_of: &H) -> usize
    where
        F: Fn(&V) -> u8,
        H: Fn(&K) -> u64,
    {
        slots
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                freq_of(&a.value)
                    .cmp(&freq_of(&b.value))
                    .then_with(|| scan_hash_of(&a.key).cmp(&scan_hash_of(&b.key)))
                    .then_with(|| a.key.cmp(&b.key))
            })
            .map(|(i, _)| i)
            .expect("evict_victim on a non-empty segment")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn basic_crud_and_overwrite() {
        let mut dt: Dashtable<u64, String> = Dashtable::new();
        assert!(dt.is_empty());
        assert_eq!(dt.get(&1), None);
        assert_eq!(dt.insert(1, "a".into()), None);
        assert_eq!(dt.insert(2, "b".into()), None);
        assert_eq!(dt.len(), 2);
        assert_eq!(dt.get(&1), Some(&"a".to_string()));
        // Overwrite returns the old value and does not grow len.
        assert_eq!(dt.insert(1, "a2".into()), Some("a".to_string()));
        assert_eq!(dt.len(), 2);
        assert_eq!(dt.get(&1), Some(&"a2".to_string()));
        // Remove.
        assert_eq!(dt.remove(&2), Some("b".to_string()));
        assert_eq!(dt.get(&2), None);
        assert_eq!(dt.remove(&2), None);
        assert_eq!(dt.len(), 1);
    }

    #[test]
    fn many_inserts_force_splits_and_doublings_and_keep_every_key() {
        let mut dt: Dashtable<u64, u64> = Dashtable::new();
        let n = 5000u64;
        for i in 0..n {
            assert_eq!(dt.insert(i, i.wrapping_mul(7)), None);
        }
        assert_eq!(dt.len(), n as usize);
        // The table must have grown structurally, not sat in one oversized segment.
        assert!(dt.global_depth() > 0, "directory should have doubled");
        assert!(
            dt.segment_count() > 1,
            "segments should have split, got {}",
            dt.segment_count()
        );
        // Every key survived the splits with its value intact.
        for i in 0..n {
            assert_eq!(dt.get(&i), Some(&i.wrapping_mul(7)), "key {i} lost");
        }
        // iter() yields exactly the live set once each.
        assert_eq!(dt.iter().count(), n as usize);
    }

    #[test]
    fn matches_hashmap_oracle_over_a_deterministic_op_stream() {
        // A deterministic LCG drives a long insert/get/remove stream over an overlapping key space,
        // and the Dashtable must agree with a HashMap oracle on every operation (no rand: the LCG is
        // pure arithmetic, satisfying the determinism invariant).
        let mut dt: Dashtable<u64, u32> = Dashtable::new();
        let mut oracle: HashMap<u64, u32> = HashMap::new();
        let mut state = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state
        };
        for _ in 0..30_000 {
            let r = next();
            let key = (r >> 32) % 800; // overlapping key space forces overwrites + re-inserts
            #[allow(clippy::cast_possible_truncation)]
            let val = r as u32;
            match r % 3 {
                0 => assert_eq!(dt.insert(key, val), oracle.insert(key, val), "insert {key}"),
                1 => assert_eq!(dt.get(&key), oracle.get(&key), "get {key}"),
                _ => assert_eq!(dt.remove(&key), oracle.remove(&key), "remove {key}"),
            }
            assert_eq!(dt.len(), oracle.len());
        }
        // Final whole-table agreement, both directions.
        for (k, v) in &oracle {
            assert_eq!(dt.get(k), Some(v));
        }
        let mut from_dt: Vec<(u64, u32)> = dt.iter().map(|(k, v)| (*k, *v)).collect();
        let mut from_oracle: Vec<(u64, u32)> = oracle.iter().map(|(k, v)| (*k, *v)).collect();
        from_dt.sort_unstable();
        from_oracle.sort_unstable();
        assert_eq!(from_dt, from_oracle);
    }

    // ---- STAGE 2: cache-mode segment-local eviction. ----

    /// A value that carries its own 2-bit frequency (freq-in-object).
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Freqd {
        freq: u8,
        tag: u32,
    }
    fn freq_of(v: &Freqd) -> u8 {
        v.freq
    }

    /// A BYTE-EXACT port of `ironcache_store::scan_hash` (store/src/lib.rs), the store's real
    /// SCAN-order hash. The eviction tie-break must use THIS (not the crate's internal directory hash)
    /// for the victim to match `refill_evict_pool`; the standalone crate does not depend on the store,
    /// so the test replicates the function and feeds it in as the `scan_hash_of` accessor, exactly as
    /// the store would at stage-3 wiring. Kept in lockstep with the store definition.
    fn store_scan_hash(key: &[u8]) -> u64 {
        const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
        const SECRET: u64 = 0xA076_1D64_78BD_642F;
        let mut h: u64 = SEED ^ SECRET;
        for &b in key {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01B3);
            h ^= h >> 33;
        }
        h = h.wrapping_add(0x9E37_79B9_7F4A_7C15);
        h = (h ^ (h >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h = (h ^ (h >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        h ^ (h >> 31)
    }
    /// The `scan_hash_of` accessor for a `u64` key: the store's scan_hash over the key's big-endian
    /// bytes (the byte encoding the store would key on). Takes `&u64` (not `u64`) because it is passed
    /// as the `Fn(&K) -> u64` accessor, whose signature requires a reference.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn scan_hash_of(k: &u64) -> u64 {
        store_scan_hash(&k.to_be_bytes())
    }

    /// The store's `refill_evict_pool` victim order: COLDEST by `(freq, scan_hash, key)`, using the
    /// STORE's real `scan_hash`. This is the independent ORACLE the dashtable's eviction must agree
    /// with (DASHTABLE.md stage 2: "the same victim as the current freq-in-object selection"), and it
    /// now validates parity with the ACTUAL store hash, not the crate's internal one.
    fn oracle_victim(entries: &[(u64, u8)]) -> u64 {
        entries
            .iter()
            .copied()
            .min_by(|&(ka, fa), &(kb, fb)| {
                fa.cmp(&fb)
                    .then(scan_hash_of(&ka).cmp(&scan_hash_of(&kb)))
                    .then(ka.cmp(&kb))
            })
            .map(|(k, _)| k)
            .unwrap()
    }

    #[test]
    fn cache_evicts_the_freq_in_object_victim() {
        // Fill ONE segment to capacity, each value carrying a deterministic 2-bit freq. The next
        // insert must evict exactly the slot the store's freq-in-object order would pick.
        let mut dt: Dashtable<u64, Freqd> = Dashtable::with_directory_bits(0);
        let entries: Vec<(u64, u8)> = (0..SEGMENT_CAP as u64)
            .map(|i| (i, (i % 4) as u8))
            .collect();
        for &(k, freq) in &entries {
            assert_eq!(
                dt.insert_cache(
                    k,
                    Freqd {
                        freq,
                        tag: k as u32
                    },
                    freq_of,
                    scan_hash_of
                ),
                CacheInsert::Inserted
            );
        }
        assert_eq!(dt.len(), SEGMENT_CAP);

        let expected = oracle_victim(&entries);
        let out = dt.insert_cache(9999, Freqd { freq: 3, tag: 9999 }, freq_of, scan_hash_of);
        match out {
            CacheInsert::Evicted { key, .. } => {
                assert_eq!(key, expected, "evicted the wrong victim");
            }
            other => panic!("expected an eviction, got {other:?}"),
        }
        assert_eq!(dt.get(&expected), None, "the victim must be gone");
        assert_eq!(
            dt.get(&9999),
            Some(&Freqd { freq: 3, tag: 9999 }),
            "the new key must be present"
        );
        assert_eq!(
            dt.len(),
            SEGMENT_CAP,
            "cache mode holds len steady across an evict-insert"
        );
    }

    #[test]
    fn cache_higher_freq_survives_eviction() {
        // One HOT slot (freq 3) among cold ones (freq 0) must never be the victim across many evicts.
        let mut dt: Dashtable<u64, Freqd> = Dashtable::with_directory_bits(0);
        dt.insert_cache(u64::MAX, Freqd { freq: 3, tag: 1 }, freq_of, scan_hash_of); // the hot key
        for i in 0..(SEGMENT_CAP as u64 - 1) {
            dt.insert_cache(i, Freqd { freq: 0, tag: 0 }, freq_of, scan_hash_of);
        }
        // Drive many evictions with fresh cold keys; the hot key must persist throughout.
        for i in 1000..1100u64 {
            dt.insert_cache(i, Freqd { freq: 0, tag: 0 }, freq_of, scan_hash_of);
            assert_eq!(
                dt.get(&u64::MAX),
                Some(&Freqd { freq: 3, tag: 1 }),
                "hot key was evicted"
            );
        }
    }

    #[test]
    fn cache_eviction_is_segment_local() {
        // With 16 distinct segments, filling+evicting the segment one key routes to must leave EVERY
        // other segment's contents untouched (the O(1) locality claim).
        let mut dt: Dashtable<u64, Freqd> = Dashtable::with_directory_bits(4);
        // Seed one key into every segment via its top-4-bit prefix (key = prefix << 60).
        for p in 0..16u64 {
            let key = p << 60;
            dt.insert_cache(
                key,
                Freqd {
                    freq: 2,
                    tag: p as u32,
                },
                freq_of,
                scan_hash_of,
            );
        }
        assert_eq!(dt.len(), 16);
        // Hammer segment 0 (prefix 0) to full + beyond, forcing local evictions there.
        for i in 1..(SEGMENT_CAP as u64 * 3) {
            dt.insert_cache(i, Freqd { freq: 0, tag: 0 }, freq_of, scan_hash_of); // top bits 0 -> segment 0
        }
        // Every OTHER segment's seeded key is intact (only segment 0 evicted).
        for p in 1..16u64 {
            let key = p << 60;
            assert_eq!(
                dt.get(&key),
                Some(&Freqd {
                    freq: 2,
                    tag: p as u32
                }),
                "segment {p} was disturbed by eviction in segment 0"
            );
        }
    }

    #[test]
    fn cache_insert_overwrites_in_place_without_eviction() {
        let mut dt: Dashtable<u64, Freqd> = Dashtable::with_directory_bits(0);
        assert_eq!(
            dt.insert_cache(7, Freqd { freq: 1, tag: 10 }, freq_of, scan_hash_of),
            CacheInsert::Inserted
        );
        assert_eq!(
            dt.insert_cache(7, Freqd { freq: 2, tag: 20 }, freq_of, scan_hash_of),
            CacheInsert::Overwrote(Freqd { freq: 1, tag: 10 })
        );
        assert_eq!(dt.get(&7), Some(&Freqd { freq: 2, tag: 20 }));
        assert_eq!(dt.len(), 1, "an overwrite does not change len");
    }
}
