// SPDX-License-Identifier: MIT OR Apache-2.0
//! [`DashIndex`]: the KEY-IN-OBJECT, EXPLICIT-HASH Dash index (#285 Stage 3, PR-1).
//!
//! The store's per-slot index is `hashbrown::HashTable<Entry>`: the table stores ONLY the
//! entry object (the key lives INSIDE it) and every operation takes the 64-bit hash plus an
//! `eq` closure from the caller (the store hashes with its own hasher and compares against
//! the embedded key). This module is the Dash-shaped equivalent with the SAME API SHAPE, so
//! the store can swap index types behind a `dashtable` feature flag with cfg'd IMPORTS (the
//! table type alias plus one for the [`Entry`] enum, whose two store match arms name the
//! hashbrown path today) and otherwise byte-identical call sites (`find`/`find_mut`/
//! `entry`/`find_entry`/`iter`/`len`/`reserve`/`Clone`/`Debug`), per the Stage 3 plan
//! (DASHTABLE.md "Implementation plan"). The swap was rehearsed against the real store
//! during review: with those imports, the whole store crate compiles and its full suite
//! passes on this index.
//!
//! ## Shape (extendible hashing, same mechanics as [`crate::Dashtable`])
//!
//! A DIRECTORY of `2^global_depth` entries maps the TOP `global_depth` bits of the caller's
//! hash to a SEGMENT; a full segment SPLITS (one new segment, records repartitioned by one
//! more top hash bit) and the directory DOUBLES (a pointer-array copy, never a record
//! rehash) only when a split would exceed the global depth. Growth is therefore incremental
//! with no power-of-two doubling trough, which is the #285 memory thesis. The 1-byte
//! FINGERPRINT (the hash's low byte, disjoint from the top routing bits) gates every probe
//! so non-matching slots are skipped without running the caller's `eq`.
//!
//! Because the table does NOT store hashes (1 fingerprint byte per record, matching
//! hashbrown's 1 control byte per bucket), every operation that can MOVE records (a split
//! during `entry`, a pre-size in `reserve`) takes a `hasher` closure to recompute a
//! record's hash from the object, exactly as `hashbrown::HashTable`'s `entry`/`reserve` do.
//!
//! ## Layout (this PR: safe parallel arrays; the dense layout is the follow-up)
//!
//! A segment is a pair of PARALLEL Vecs: `fps: Vec<u8>` (packed fingerprints) and
//! `items: Vec<T>` (the objects). Parallel arrays -- not a `Vec<(u8, T)>` -- because the
//! store's `Entry` is 8-byte-aligned, so an interleaved slot would pad 1+8 to a 16-byte
//! stride and single-handedly erase the memory win; packed parallel arrays keep ~9 bytes
//! per record even in this safe representation, and are already the dense layout's shape
//! (the follow-up swaps the two Vecs for one co-allocated block, it does not reshape).
//!
//! ## The zero-allocation empty table (a store constraint)
//!
//! The store keeps `slots_per_db` (default 256) index tables PER TOUCHED DB and relies on
//! an EMPTY table allocating nothing ("256 empty tables cost a few KB"). [`DashIndex::new`]
//! therefore allocates NOTHING (empty directory, no segments); the first insert or
//! `reserve` lazily creates the depth-0 single-segment shape. `hashbrown::HashTable::new`
//! makes the same promise (no bucket array until first insert).
//!
//! ## Determinism (ADR-0003)
//!
//! The table stores no hasher and draws no randomness: routing, splits, and iteration
//! order are pure functions of the op history and the CALLER-supplied hashes. (The store
//! feeds its per-run table hash here, exactly as it does hashbrown's explicit-hash API, and
//! defends every ordering it exposes by sorting on its own fixed-seed `scan_hash` -- so
//! per-run hash variance is invisible, on either backend.)

use crate::{MAX_GLOBAL_DEPTH, bit_from_top, fingerprint};

/// The maximum live records a segment holds before an insert forces a SPLIT. Larger than the
/// stage-1 reference's test-oriented 16: a fingerprint pre-scan of 64 packed bytes is ~one
/// cache line of work, while fewer/larger segments amortize the per-segment fixed costs
/// (headers + directory entries) across more records. The dense-layout follow-up tunes the
/// real geometry against the microbench (DASHTABLE.md "Parameter tuning").
const SEGMENT_CAP: usize = 64;

/// The split-retry bound per insert: a backstop on loop iterations. The REAL guards against
/// pathological collisions are [`MAX_GLOBAL_DEPTH`] and the futility fast-path in
/// [`DashIndex::entry`] (see there); this bound only caps the loop count.
const MAX_SPLITS: u32 = 64;

/// One segment: up to [`SEGMENT_CAP`] records as PARALLEL arrays (see the module doc for why
/// parallel), plus the extendible-hashing LOCAL depth (how many top hash bits distinguish
/// the records routed here). Owned once in [`DashIndex::segments`]; the directory stores
/// only indices, so aliased directory entries never double-own a segment.
#[derive(Clone, Debug)]
struct Segment<T> {
    local_depth: u8,
    /// `fps[i]` is the fingerprint of `items[i]`; the two Vecs always have equal length.
    fps: Vec<u8>,
    items: Vec<T>,
}

impl<T> Segment<T> {
    /// A fresh segment with ORGANIC slot growth (the split path: slack stays proportional
    /// to what the repartition actually moves).
    fn new(local_depth: u8) -> Self {
        Segment {
            local_depth,
            fps: Vec::new(),
            items: Vec::new(),
        }
    }

    /// A fresh segment with its slot storage PRE-ALLOCATED to [`SEGMENT_CAP`] (the
    /// `reserve` bulk-load path: the fill that follows must not allocate per record --
    /// see [`DashIndex::reserve`]).
    fn with_capacity(local_depth: u8) -> Self {
        Segment {
            local_depth,
            fps: Vec::with_capacity(SEGMENT_CAP),
            items: Vec::with_capacity(SEGMENT_CAP),
        }
    }

    /// The probe: the position of the record whose fingerprint matches AND whose object
    /// satisfies `eq`. The fingerprint gate means `eq` runs only on the ~1/256 of records
    /// whose hash low byte collides.
    fn position(&self, fp: u8, mut eq: impl FnMut(&T) -> bool) -> Option<usize> {
        self.fps
            .iter()
            .enumerate()
            .filter(|&(_, &f)| f == fp)
            .map(|(i, _)| i)
            .find(|&i| eq(&self.items[i]))
    }

    fn push(&mut self, fp: u8, item: T) {
        self.fps.push(fp);
        self.items.push(item);
    }

    /// Remove position `i` keeping the parallel arrays in sync (swap-remove on both).
    fn swap_remove(&mut self, i: usize) -> T {
        self.fps.swap_remove(i);
        self.items.swap_remove(i)
    }
}

/// A Dash-style extendible-hashing index over key-in-object records `T`, with the same
/// explicit-hash API shape as `hashbrown::HashTable<T>`. See the module doc.
///
/// `Debug` because the store derives `Debug` on the shard store that embeds its tables
/// (hashbrown's table is `Debug` too, so the derive must hold under either backend).
#[derive(Debug)]
pub struct DashIndex<T> {
    global_depth: u8,
    /// `2^global_depth` entries (or EMPTY before the first insert -- the zero-allocation
    /// empty state), each an index into [`Self::segments`]. `u32`: the directory is the one
    /// structure aliasing multiplies, so halving its entry width vs `usize` matters at high
    /// depths, and 4 billion segments is far beyond any per-slot table's reach.
    directory: Vec<u32>,
    segments: Vec<Segment<T>>,
    len: usize,
}

impl<T> Clone for DashIndex<T>
where
    T: Clone,
{
    /// A DEEP clone (directory + every segment + every record). The store's #576 per-slot
    /// Arc copy-on-write depends on exactly this: `Arc::make_mut` on a frozen slot deep-
    /// clones the table so the save keeps the frozen records while writes proceed.
    fn clone(&self) -> Self {
        DashIndex {
            global_depth: self.global_depth,
            directory: self.directory.clone(),
            segments: self.segments.clone(),
            len: self.len,
        }
    }
}

impl<T> Default for DashIndex<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DashIndex<T> {
    /// An empty index that allocates NOTHING (see the module doc: the store keeps hundreds
    /// of empty per-slot tables, so the empty state must be free, like
    /// `hashbrown::HashTable::new`). The depth-0 single-segment shape is created lazily by
    /// the first insert or `reserve`.
    #[must_use]
    pub fn new() -> Self {
        DashIndex {
            global_depth: 0,
            directory: Vec::new(),
            segments: Vec::new(),
            len: 0,
        }
    }

    /// The number of live records. O(1) (DBSIZE sums this per slot table).
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the index holds no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The current global depth (the directory holds `2^global_depth` entries once
    /// initialized). Introspection for tests + tuning.
    #[must_use]
    pub fn global_depth(&self) -> u8 {
        self.global_depth
    }

    /// The number of live segments (0 before the first insert; grows by one per split).
    /// Introspection for tests + tuning.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// The directory index for a hash: the top `global_depth` bits (0 at depth 0, where a
    /// 64-bit shift would be undefined). The caller must ensure the directory is non-empty.
    #[allow(clippy::cast_possible_truncation)] // < 2^global_depth <= directory.len(), always in range.
    fn dir_index(&self, h: u64) -> usize {
        if self.global_depth == 0 {
            0
        } else {
            (h >> (64 - u32::from(self.global_depth))) as usize
        }
    }

    /// The segment index routed to by `h`, or `None` while the table is still the
    /// zero-allocation empty shape.
    fn route(&self, h: u64) -> Option<usize> {
        if self.directory.is_empty() {
            None
        } else {
            Some(self.directory[self.dir_index(h)] as usize)
        }
    }

    /// Create the depth-0 single-segment shape on the first mutation (the lazy counterpart
    /// of [`crate::Dashtable::new`]'s eager one-segment start).
    fn ensure_init(&mut self) {
        if self.directory.is_empty() {
            self.directory.push(0);
            self.segments.push(Segment::new(0));
        }
    }

    /// Look up the record for `hash` whose object satisfies `eq` (shared).
    ///
    /// Mirrors `hashbrown::HashTable::find`: the caller supplies the full 64-bit hash (the
    /// store's table hash of the key) and an `eq` that compares the EMBEDDED key.
    #[must_use]
    pub fn find(&self, hash: u64, eq: impl FnMut(&T) -> bool) -> Option<&T> {
        let seg = &self.segments[self.route(hash)?];
        let pos = seg.position(fingerprint(hash), eq)?;
        Some(&seg.items[pos])
    }

    /// Look up the record for `hash` whose object satisfies `eq` (mutable).
    ///
    /// Mirrors `hashbrown::HashTable::find_mut`. The caller must not mutate the embedded
    /// key in a way that changes its hash (the same contract hashbrown documents).
    #[must_use]
    pub fn find_mut(&mut self, hash: u64, eq: impl FnMut(&T) -> bool) -> Option<&mut T> {
        let si = self.route(hash)?;
        let seg = &mut self.segments[si];
        let pos = seg.position(fingerprint(hash), eq)?;
        Some(&mut seg.items[pos])
    }

    /// The upsert funnel, mirroring `hashbrown::HashTable::entry`: returns
    /// [`Entry::Occupied`] when a record matches (`get`/`get_mut` it), else
    /// [`Entry::Vacant`] (call `insert`). `hasher` recomputes an EXISTING record's hash
    /// from its object; it is needed because making room for a new record may SPLIT the
    /// routed segment (repartitioning records by one more hash bit), exactly as
    /// hashbrown's `entry` may need it to grow the table.
    ///
    /// Room for the vacant insert is made HERE (the split loop), not in `Vacant::insert`,
    /// so the returned placement is stable: the `&mut` borrow the entry holds prevents any
    /// interleaved mutation between `entry` and `insert`.
    pub fn entry(
        &mut self,
        hash: u64,
        eq: impl FnMut(&T) -> bool,
        hasher: impl Fn(&T) -> u64,
    ) -> Entry<'_, T> {
        self.ensure_init();
        let fp = fingerprint(hash);
        let si = self.directory[self.dir_index(hash)] as usize;
        if let Some(pos) = self.segments[si].position(fp, eq) {
            return Entry::Occupied(OccupiedEntry {
                table: self,
                seg: si,
                pos,
            });
        }
        // Absent: make room in the routed segment BEFORE handing out the vacant placement
        // (a split re-routes; the loop re-reads the directory until the routed segment has
        // room, or a guard trips and the record is FORCE-PLACED past SEGMENT_CAP -- the
        // segment then grows linearly, which stays correct, just slower to probe). Guards,
        // in order:
        //   1. FUTILITY: every record in the full segment hashes IDENTICALLY to the
        //      incoming record -- no split at any depth can separate them, so splitting
        //      would only double the directory forever. Gated by the cheap all-fps-equal
        //      prefilter so the hasher sweep runs only when full collision is plausible.
        //   2. MAX_GLOBAL_DEPTH: records sharing a long-but-not-total top-bit prefix DO
        //      separate eventually, but each intervening split doubles the directory; the
        //      depth cap bounds that at 2^MAX_GLOBAL_DEPTH entries (see the const).
        //   3. MAX_SPLITS: a plain iteration backstop.
        let mut splits = 0;
        loop {
            let si = self.directory[self.dir_index(hash)] as usize;
            let seg = &self.segments[si];
            if seg.items.len() < SEGMENT_CAP || splits >= MAX_SPLITS {
                break;
            }
            if seg.fps.iter().all(|&f| f == fp) && seg.items.iter().all(|it| hasher(it) == hash) {
                break; // fully collided with the incoming record: no depth separates them
            }
            if seg.local_depth == self.global_depth && self.global_depth >= MAX_GLOBAL_DEPTH {
                break; // splitting would double the directory past the hard cap
            }
            self.split(self.dir_index(hash), &hasher);
            splits += 1;
        }
        let si = self.directory[self.dir_index(hash)] as usize;
        Entry::Vacant(VacantEntry {
            table: self,
            seg: si,
            fp,
        })
    }

    /// The remove funnel, mirroring `hashbrown::HashTable::find_entry`: `Ok` holds an
    /// occupied entry whose [`OccupiedEntry::remove`] extracts the record; `Err(Absent)`
    /// means no record matched. (hashbrown's `AbsentEntry` carries the table borrow for
    /// re-insertion chaining; the store never uses that, so `Absent` is a unit.)
    ///
    /// # Errors
    ///
    /// `Err(Absent)` when no record with a matching fingerprint satisfies `eq`.
    pub fn find_entry(
        &mut self,
        hash: u64,
        eq: impl FnMut(&T) -> bool,
    ) -> Result<OccupiedEntry<'_, T>, Absent> {
        let Some(si) = self.route(hash) else {
            return Err(Absent);
        };
        match self.segments[si].position(fingerprint(hash), eq) {
            Some(pos) => Ok(OccupiedEntry {
                table: self,
                seg: si,
                pos,
            }),
            None => Err(Absent),
        }
    }

    /// Pre-size for `additional` more records, mirroring `hashbrown::HashTable::reserve`'s
    /// role as the bulk-load seam (the store's `reserve` spreads a keyspace across its slot
    /// tables before a fill; consumer: the memmodel bench).
    ///
    /// On an EMPTY table this builds a directory of enough DISTINCT segments that a
    /// uniform fill of `additional` records lands ~5/8 full per segment, EACH SEGMENT
    /// PRE-ALLOCATED to [`SEGMENT_CAP`]: a well-mixed fill then triggers no directory
    /// doubling, few-to-no splits, and NO per-record heap allocation -- the property the
    /// memmodel bench's table-vs-object bytes decomposition depends on (it attributes
    /// reserve-time allocations to the table and fill-time allocations to the objects, so
    /// slot storage allocated during the fill would silently misattribute the very
    /// bytes-per-key figures #285 exists to improve). On a non-empty table it only
    /// pre-DOUBLES the directory to the target depth (pointer copies; no records move) --
    /// segments still split organically on overflow, which is dash's incremental growth
    /// working as designed. NOTE the contract is deliberately WEAKER than hashbrown's
    /// "no resize during the fill": an extendible-hashing fill can always locally split a
    /// hash-SKEWED segment; what reserve removes is the directory-wide work and (on the
    /// empty path) the per-record allocation. The target depth is clamped to
    /// [`MAX_GLOBAL_DEPTH`], the same hard directory bound the split path enforces.
    /// `hasher` is accepted for shape-parity with the hashbrown API (a future eager
    /// pre-split would repartition with it) but the current pre-sizing moves no records.
    pub fn reserve(&mut self, additional: usize, hasher: impl Fn(&T) -> u64) {
        let _ = &hasher; // shape-parity; see the doc comment.
        let needed = self.len + additional;
        if needed == 0 {
            return;
        }
        // Target ~5/8 of SEGMENT_CAP per segment so a uniform fill has slack before a
        // split; round the segment count up to the next power of two (the directory is
        // always a power of two), then CLAMP to the hard directory bound -- reserve must
        // not build what the split path refuses to grow to (and the clamp is what makes
        // the u32 directory-entry cast below exact by construction: 2^20 < u32::MAX).
        #[allow(clippy::cast_possible_truncation)] // 64-bit usize: log2 < 64 always fits u8.
        let target_depth = (needed
            .div_ceil(SEGMENT_CAP * 5 / 8)
            .next_power_of_two()
            .max(1)
            .trailing_zeros() as u8)
            .min(MAX_GLOBAL_DEPTH);
        let target_segments = 1usize << target_depth;
        if self.directory.is_empty() {
            // The empty fast path: build the pre-sized shape directly, every directory
            // entry owning its own segment at local depth == global depth (no aliasing),
            // slot storage pre-allocated so the fill itself allocates nothing.
            self.global_depth = target_depth;
            #[allow(clippy::cast_possible_truncation)]
            // target_segments = 2^target_depth <= 2^MAX_GLOBAL_DEPTH < u32::MAX.
            {
                self.directory = (0..target_segments as u32).collect();
            }
            self.segments = (0..target_segments)
                .map(|_| Segment::with_capacity(target_depth))
                .collect();
            return;
        }
        while self.global_depth < target_depth {
            self.double_directory();
        }
    }

    /// Iterate every live record. Walks [`Self::segments`] directly (each owned once), so
    /// an aliased segment is visited exactly once. Order is UNSPECIFIED, matching
    /// hashbrown: the store sorts everything it exposes by its own fixed-seed scan hash.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.segments.iter().flat_map(|seg| seg.items.iter())
    }

    /// Double the directory: every entry is duplicated (`new[i] = old[i >> 1]`), so each
    /// old prefix `k` becomes the two prefixes `2k`/`2k+1`, both still pointing at the same
    /// segment until a split separates them. Moves no records; bumps the global depth.
    fn double_directory(&mut self) {
        let old_len = self.directory.len();
        let mut next = Vec::with_capacity(old_len * 2);
        for i in 0..old_len * 2 {
            next.push(self.directory[i >> 1]);
        }
        self.directory = next;
        self.global_depth += 1;
    }

    /// Split the segment routed to by directory entry `dir_idx`: allocate a buddy at
    /// `local_depth + 1`, repartition the records between the two by the
    /// `(local_depth + 1)`-th top hash bit (hashes recomputed via `hasher` -- the table
    /// stores only fingerprints), and re-point the directory entries whose prefix has that
    /// bit set at the buddy. Doubles the directory first if the segment is already at the
    /// global depth. Mechanics identical to [`crate::Dashtable::split`]; this variant
    /// carries the caller's hasher and the parallel-array layout.
    fn split(&mut self, dir_idx: usize, hasher: &impl Fn(&T) -> u64) {
        let seg_idx = self.directory[dir_idx] as usize;
        let local = self.segments[seg_idx].local_depth;
        if local == self.global_depth {
            self.double_directory();
        }
        let new_local = local + 1;
        let buddy_idx = self.segments.len();
        self.segments.push(Segment::new(new_local));
        self.segments[seg_idx].local_depth = new_local;
        // Repartition: records whose new-local-th top hash bit is 1 move to the buddy.
        let old_fps = std::mem::take(&mut self.segments[seg_idx].fps);
        let old_items = std::mem::take(&mut self.segments[seg_idx].items);
        for (fp, item) in old_fps.into_iter().zip(old_items) {
            let h = hasher(&item);
            let target = if bit_from_top(h, new_local) {
                buddy_idx
            } else {
                seg_idx
            };
            self.segments[target].push(fp, item);
        }
        // Re-point the directory: an entry that pointed at the old segment and whose
        // prefix has the new-local-th top bit set now points at the buddy. For directory
        // index `i` (a top `global_depth`-bit prefix), that bit is bit
        // `global_depth - new_local` of `i`.
        #[allow(clippy::cast_possible_truncation)] // buddy_idx < u32::MAX (directory entry width).
        let buddy_u32 = buddy_idx as u32;
        #[allow(clippy::cast_possible_truncation)] // seg_idx came out of the directory.
        let seg_u32 = seg_idx as u32;
        let shift = u32::from(self.global_depth - new_local);
        for (i, entry) in self.directory.iter_mut().enumerate() {
            if *entry == seg_u32 && (i >> shift) & 1 == 1 {
                *entry = buddy_u32;
            }
        }
    }
}

/// The result of [`DashIndex::entry`], mirroring `hashbrown::hash_table::Entry` so the
/// store's upsert `match` is byte-identical under either backend.
pub enum Entry<'a, T> {
    /// A record matched: read or replace it in place.
    Occupied(OccupiedEntry<'a, T>),
    /// No record matched: room is already made; `insert` places the new record.
    Vacant(VacantEntry<'a, T>),
}

/// A matched record, mirroring `hashbrown::hash_table::OccupiedEntry`.
pub struct OccupiedEntry<'a, T> {
    table: &'a mut DashIndex<T>,
    seg: usize,
    pos: usize,
}

impl<'a, T> OccupiedEntry<'a, T> {
    /// The matched record (shared).
    #[must_use]
    pub fn get(&self) -> &T {
        &self.table.segments[self.seg].items[self.pos]
    }

    /// The matched record (mutable, borrow bound to `&mut self`). The caller must not
    /// change the embedded key's hash (the hashbrown contract).
    #[must_use]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.table.segments[self.seg].items[self.pos]
    }

    /// The matched record (mutable, consuming the entry: the borrow lives as long as the
    /// original `&'a mut` table borrow). Mirrors hashbrown's `into_mut`.
    #[must_use]
    pub fn into_mut(self) -> &'a mut T {
        &mut self.table.segments[self.seg].items[self.pos]
    }

    /// Remove the matched record, returning it. The second tuple element mirrors the
    /// SHAPE of hashbrown's `(T, VacantEntry)` return so store call sites destructure
    /// identically (`let (obj, _) = occ.remove();`). It is the [`RemovedVacancy`] ZST --
    /// not a re-insertable vacancy, because a hash-less dash slot cannot be re-targeted
    /// without the original full hash (the store never re-inserts through it) -- and not a
    /// bare `()`, because pedantic clippy's `ignored_unit_patterns` would reject the
    /// store's `_` binding against a unit while the same binding must also compile against
    /// hashbrown's `VacantEntry` arm. NOT `#[must_use]`: hashbrown's `remove` is not, and
    /// the store discards the whole return on one path.
    pub fn remove(self) -> (T, RemovedVacancy) {
        let item = self.table.segments[self.seg].swap_remove(self.pos);
        self.table.len -= 1;
        (item, RemovedVacancy)
    }
}

/// The placeholder second element of [`OccupiedEntry::remove`]'s return (see there): shape
/// parity with hashbrown's `(T, VacantEntry)` without claiming to be a usable vacancy.
#[derive(Debug)]
pub struct RemovedVacancy;

/// A vacancy for a new record, mirroring `hashbrown::hash_table::VacantEntry`. Room in the
/// target segment was already made by [`DashIndex::entry`] (see there), so `insert` is a
/// plain placement.
pub struct VacantEntry<'a, T> {
    table: &'a mut DashIndex<T>,
    seg: usize,
    fp: u8,
}

impl<'a, T> VacantEntry<'a, T> {
    /// Place `value`, returning the occupied entry for it (mirrors hashbrown).
    pub fn insert(self, value: T) -> OccupiedEntry<'a, T> {
        let seg = &mut self.table.segments[self.seg];
        seg.push(self.fp, value);
        self.table.len += 1;
        let pos = seg.items.len() - 1;
        OccupiedEntry {
            table: self.table,
            seg: self.seg,
            pos,
        }
    }
}

/// The `Err` of [`DashIndex::find_entry`]: no record matched. A unit (see `find_entry`).
#[derive(Debug)]
pub struct Absent;

#[cfg(test)]
mod tests {
    use super::{DashIndex, Entry, SEGMENT_CAP};

    /// The test record: key-in-object, like the store's `Entry` (key + payload in one
    /// object; the table stores only the object).
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Rec {
        key: u64,
        val: u64,
    }

    /// The test hash: SplitMix64's finalizer over the embedded key -- a fixed, well-mixed
    /// stand-in for the store's table hash (deterministic here so tests can reason about
    /// routing; the REAL table hash may vary per run, which the table never observes
    /// beyond routing).
    fn hash_of(key: u64) -> u64 {
        let mut z = key.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn insert(t: &mut DashIndex<Rec>, key: u64, val: u64) -> Option<u64> {
        match t.entry(hash_of(key), |r| r.key == key, |r| hash_of(r.key)) {
            Entry::Occupied(mut e) => Some(std::mem::replace(&mut e.get_mut().val, val)),
            Entry::Vacant(e) => {
                e.insert(Rec { key, val });
                None
            }
        }
    }

    fn get(t: &DashIndex<Rec>, key: u64) -> Option<u64> {
        t.find(hash_of(key), |r| r.key == key).map(|r| r.val)
    }

    fn remove(t: &mut DashIndex<Rec>, key: u64) -> Option<u64> {
        match t.find_entry(hash_of(key), |r| r.key == key) {
            Ok(occ) => Some(occ.remove().0.val),
            Err(_) => None,
        }
    }

    #[test]
    fn empty_table_allocates_nothing_and_answers_lookups() {
        let t: DashIndex<Rec> = DashIndex::new();
        // The zero-allocation empty state the store's 256-empty-tables constraint needs:
        // no directory, no segments (capacity 0 means no heap allocation was made).
        assert_eq!(t.segment_count(), 0);
        assert_eq!(t.len(), 0);
        assert!(t.is_empty());
        assert_eq!(get(&t, 42), None);
        assert_eq!(t.iter().count(), 0);
    }

    #[test]
    fn insert_find_overwrite_remove_roundtrip() {
        let mut t = DashIndex::new();
        assert_eq!(insert(&mut t, 1, 10), None);
        assert_eq!(insert(&mut t, 2, 20), None);
        assert_eq!(t.len(), 2);
        assert_eq!(get(&t, 1), Some(10));
        assert_eq!(get(&t, 2), Some(20));
        assert_eq!(get(&t, 3), None);
        // Overwrite through the Occupied arm returns the previous value, len unchanged.
        assert_eq!(insert(&mut t, 1, 11), Some(10));
        assert_eq!(t.len(), 2);
        assert_eq!(get(&t, 1), Some(11));
        // find_mut edits in place.
        t.find_mut(hash_of(2), |r| r.key == 2).unwrap().val = 21;
        assert_eq!(get(&t, 2), Some(21));
        // Remove through find_entry; a second remove misses.
        assert_eq!(remove(&mut t, 1), Some(11));
        assert_eq!(remove(&mut t, 1), None);
        assert_eq!(t.len(), 1);
        assert_eq!(get(&t, 1), None);
        assert_eq!(get(&t, 2), Some(21));
    }

    #[test]
    fn growth_splits_segments_and_every_record_stays_findable() {
        let mut t = DashIndex::new();
        let n = (SEGMENT_CAP * 40) as u64; // forces many splits + directory doublings
        for k in 0..n {
            assert_eq!(insert(&mut t, k, k * 2), None);
        }
        assert_eq!(t.len(), n as usize);
        assert!(t.segment_count() > 1, "growth must have split segments");
        assert!(
            t.global_depth() > 0,
            "growth must have doubled the directory"
        );
        for k in 0..n {
            assert_eq!(get(&t, k), Some(k * 2), "key {k} lost after growth");
        }
        // Iteration visits every record exactly once (aliased segments not double-walked).
        let mut seen: Vec<u64> = t.iter().map(|r| r.key).collect();
        seen.sort_unstable();
        assert_eq!(seen, (0..n).collect::<Vec<_>>());
    }

    #[test]
    fn remove_across_segments_keeps_the_rest_intact() {
        let mut t = DashIndex::new();
        let n = (SEGMENT_CAP * 8) as u64;
        for k in 0..n {
            insert(&mut t, k, k);
        }
        for k in (0..n).step_by(2) {
            assert_eq!(remove(&mut t, k), Some(k));
        }
        assert_eq!(t.len(), (n / 2) as usize);
        for k in 0..n {
            let expect = if k % 2 == 0 { None } else { Some(k) };
            assert_eq!(
                get(&t, k),
                expect,
                "key {k} wrong after interleaved removes"
            );
        }
    }

    #[test]
    fn pathological_full_hash_collision_terminates_and_stays_correct() {
        // Every key hashes IDENTICALLY: splits cannot separate them, so the MAX_SPLITS
        // bound must trip and force-place past SEGMENT_CAP rather than loop forever.
        let mut t: DashIndex<Rec> = DashIndex::new();
        let n = (SEGMENT_CAP * 2) as u64;
        for k in 0..n {
            match t.entry(0xDEAD_BEEF, |r| r.key == k, |_| 0xDEAD_BEEF) {
                Entry::Occupied(_) => panic!("distinct keys must not match"),
                Entry::Vacant(e) => {
                    e.insert(Rec { key: k, val: k });
                }
            }
        }
        assert_eq!(t.len(), n as usize);
        for k in 0..n {
            let got = t.find(0xDEAD_BEEF, |r| r.key == k).map(|r| r.val);
            assert_eq!(got, Some(k), "collided key {k} lost");
        }
        // And removal still works through the same collided hash. (`_` binding, the exact
        // shape the store uses -- it must bind RemovedVacancy here and hashbrown's
        // VacantEntry there.)
        let (rec, _) = t
            .find_entry(0xDEAD_BEEF, |r| r.key == 3)
            .expect("key 3 present")
            .remove();
        assert_eq!(rec.key, 3);
        assert_eq!(t.len(), n as usize - 1);
    }

    #[test]
    fn deep_shared_prefix_hits_the_depth_cap_and_force_places() {
        use super::MAX_GLOBAL_DEPTH;
        // Two hash values differing ONLY in the LAST bit: splits cannot separate them
        // until depth 64, so the MAX_GLOBAL_DEPTH cap must trip (bounding the directory)
        // and the records force-place past SEGMENT_CAP, staying fully correct.
        let mut t: DashIndex<Rec> = DashIndex::new();
        let n = (SEGMENT_CAP + 8) as u64;
        for k in 0..n {
            let h = k & 1; // hash 0 or 1: top 63 bits shared
            match t.entry(h, |r| r.key == k, |r| r.key & 1) {
                Entry::Occupied(_) => panic!("distinct keys must not match"),
                Entry::Vacant(e) => {
                    e.insert(Rec { key: k, val: k });
                }
            }
        }
        assert_eq!(t.len(), n as usize);
        assert!(
            t.global_depth() <= MAX_GLOBAL_DEPTH,
            "directory depth must be capped, got {}",
            t.global_depth()
        );
        for k in 0..n {
            let got = t.find(k & 1, |r| r.key == k).map(|r| r.val);
            assert_eq!(got, Some(k), "deep-prefix key {k} lost");
        }
    }

    #[test]
    fn clone_is_deep_and_diverges() {
        let mut a = DashIndex::new();
        for k in 0..(SEGMENT_CAP as u64 * 4) {
            insert(&mut a, k, k);
        }
        let mut b = a.clone();
        // Mutations on each side are invisible to the other (the #576 COW requirement).
        insert(&mut a, 9999, 1);
        remove(&mut b, 0);
        assert_eq!(get(&a, 9999), Some(1));
        assert_eq!(get(&b, 9999), None);
        assert_eq!(get(&a, 0), Some(0));
        assert_eq!(get(&b, 0), None);
    }

    #[test]
    fn reserve_on_empty_presizes_so_a_uniform_fill_never_doubles() {
        let mut t: DashIndex<Rec> = DashIndex::new();
        let n = 4096usize;
        t.reserve(n, |r| hash_of(r.key));
        let depth_after_reserve = t.global_depth();
        let segs_after_reserve = t.segment_count();
        assert!(segs_after_reserve.is_power_of_two());
        // A PERFECTLY uniform fill: keys whose hashes are evenly spread over the top bits
        // (crafted, not hashed), so segment occupancy is exactly balanced and NO split or
        // doubling can occur -- proving the pre-size actually pre-sized.
        let step = u64::MAX / n as u64;
        for i in 0..n {
            let h = i as u64 * step;
            match t.entry(h, |_| false, |_| unreachable!("balanced fill never splits")) {
                Entry::Occupied(_) => unreachable!("eq is false"),
                Entry::Vacant(e) => {
                    e.insert(Rec {
                        key: i as u64,
                        val: 0,
                    });
                }
            }
        }
        assert_eq!(t.len(), n);
        assert_eq!(
            t.global_depth(),
            depth_after_reserve,
            "no directory doubling"
        );
        assert_eq!(t.segment_count(), segs_after_reserve, "no splits");
    }

    #[test]
    fn reserve_clamps_the_directory_depth_to_the_hard_cap() {
        use super::MAX_GLOBAL_DEPTH;
        // A reservation large enough to want depth > MAX_GLOBAL_DEPTH must be CLAMPED:
        // reserve shares the split path's hard directory bound (an unclamped depth would
        // also break the u32 directory-entry cast at astronomical sizes). Exercised via
        // the NON-empty path (directory doubling only, ~4 MiB) -- the empty fast path
        // shares the same clamped target_depth, and pre-allocating 2^20 segments in a
        // unit test would be needlessly heavy.
        let mut t = DashIndex::new();
        insert(&mut t, 1, 1);
        let want_more_than_cap = SEGMENT_CAP * (1 << (MAX_GLOBAL_DEPTH + 2));
        t.reserve(want_more_than_cap, |r| hash_of(r.key));
        assert_eq!(
            t.global_depth(),
            MAX_GLOBAL_DEPTH,
            "depth must clamp at the cap"
        );
        // The table stays fully functional at the cap.
        insert(&mut t, 2, 20);
        assert_eq!(get(&t, 1), Some(1));
        assert_eq!(get(&t, 2), Some(20));
    }

    #[test]
    fn into_mut_extends_the_borrow() {
        let mut t = DashIndex::new();
        insert(&mut t, 7, 70);
        let r: &mut Rec = match t.entry(hash_of(7), |r| r.key == 7, |r| hash_of(r.key)) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(_) => panic!("key 7 present"),
        };
        r.val = 71;
        assert_eq!(get(&t, 7), Some(71));
    }
}
