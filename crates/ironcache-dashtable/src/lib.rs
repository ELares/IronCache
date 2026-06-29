// SPDX-License-Identifier: MIT OR Apache-2.0
//! A standalone Dash-style extendible-hashing table (#285, STAGE 1: the algorithm core).
//!
//! This crate validates the NOVEL part of the Dash design (DASHTABLE.md) in isolation, with zero
//! blast radius on the store and zero `unsafe` (so `miri` is trivial): the extendible DIRECTORY, the
//! per-segment LOCAL depth, the SEGMENT SPLIT on overflow, the DIRECTORY DOUBLING when a split would
//! exceed the global depth, and the 1-byte FINGERPRINT that gates a lookup so it skips non-matching
//! slots. It is a correctness reference, NOT yet the production index.
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
//! - Wiring into the store behind a feature flag, and the freq-in-object segment-local eviction.
//!
//! Directory mechanics use the TOP `global_depth` bits of the key hash as the directory index and a
//! disjoint hash byte as the fingerprint, exactly as DASHTABLE.md specifies.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// The maximum live records a segment holds before an insert forces a SPLIT. A small constant here so
/// the stage-1 tests exercise many splits + directory doublings with modest key counts; the dense
/// store-wired layout (later stage) tunes the real geometry (DASHTABLE.md: Dragonfly uses 840).
const SEGMENT_CAP: usize = 16;

/// The split-retry bound: after this many splits without making room, force the record in anyway. A
/// segment only fails to split usefully when every record shares the SAME 64-bit hash (distinct keys
/// colliding fully), which a 64-bit hash makes astronomically rare; the bound just guarantees
/// termination instead of looping forever in that pathological case.
const MAX_SPLITS: u32 = 64;

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
/// so it carries independent entropy.
#[allow(clippy::cast_possible_truncation)] // masked to one byte, so the cast is exact.
fn fingerprint(h: u64) -> u8 {
    (h & 0xFF) as u8
}

/// Whether the `n`-th bit from the TOP of `h` (1-indexed) is set. `n` is in `1..=64`.
fn bit_from_top(h: u64, n: u8) -> bool {
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
        // A new key: make room (splitting as needed), then place it ONCE.
        let mut splits = 0;
        loop {
            let si = self.directory[self.dir_index(h)];
            if self.segments[si].slots.len() < SEGMENT_CAP || splits >= MAX_SPLITS {
                break;
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
}
