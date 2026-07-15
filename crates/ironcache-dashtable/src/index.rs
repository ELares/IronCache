// SPDX-License-Identifier: MIT OR Apache-2.0
//! [`DashIndex`]: the KEY-IN-OBJECT, EXPLICIT-HASH Dash index (#285 Stage 3).
//!
//! The store's per-slot index is `hashbrown::HashTable<Entry>`: the table stores ONLY the
//! entry object (the key lives INSIDE it) and every operation takes the 64-bit hash plus an
//! `eq` closure from the caller (the store hashes with its own hasher and compares against
//! the embedded key). This module is the Dash-shaped equivalent with the SAME API SHAPE, so
//! the store can swap index types behind a `dashtable` feature flag with cfg'd IMPORTS (the
//! table type alias plus one for the [`Entry`] enum) and otherwise byte-identical call
//! sites (`find`/`find_mut`/`entry`/`find_entry`/`iter`/`len`/`reserve`/`Clone`/`Debug`),
//! per the Stage 3 plan (DASHTABLE.md "Implementation plan"). PR-2 (#653) wired exactly
//! that seam; the full store suite runs on this index under `--features dashtable`.
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
//! ## The DENSE segment layout (PR-3)
//!
//! A segment is ONE inline block: a packed fingerprint array `[u8; SEGMENT_CAP]`, then the
//! record slots `[MaybeUninit<T>; SEGMENT_CAP]`, with `len` marking the initialized prefix
//! of both. Records are PACKED (`0..len`; removal back-swaps), so no occupancy bitmap is
//! needed. Segments live by value in one `Vec<Segment<T>>`, so the whole table is the
//! directory + ONE segment array allocation: a probe is directory load -> segment load ->
//! a fingerprint scan over contiguous bytes -> the matching slot, with no per-segment heap
//! indirection (the safe PR-1 layout paid two `Vec` indirections per probe, which the
//! microbench showed as a ~10x hit-probe gap vs hashbrown's SIMD group scan). The
//! fingerprint prescan is a plain byte loop over `[u8; 64]`, which LLVM vectorizes.
//!
//! The pathological FORCE-PLACE path (see the guards in [`DashIndex::entry`]) spills past
//! the inline capacity into `overflow: Option<Box<Vec<(u8, T)>>>` -- one pointer-width in
//! the common segment (`None`), a rarely-taken branch on probes, and linear growth for the
//! inseparable-records regime, mirroring the PR-1 behavior.
//!
//! ### The `unsafe` inventory and its invariant
//!
//! This module's ONLY `unsafe` is `MaybeUninit` slot management inside [`Segment`], all of
//! it downstream of ONE invariant: `items[0..len]` are initialized and everything at
//! `len..` is logically uninitialized (moved-out or never written). Every `unsafe` block
//! states which side of that invariant it relies on. Structural moves of whole segments
//! (the `Vec<Segment>` growing on a split) are plain bitwise moves: a segment holds no
//! self-referential pointers. Drop/Clone are manual: `Drop` drops exactly the initialized
//! prefix; `Clone` clones it behind a panic guard (a mid-clone panic drops the cloned
//! prefix rather than leaking). The gate for all of this is `miri` over the crate's test
//! suite (the oracle parity tests hammer these paths with drop-observing payloads) plus
//! the store's full suite under `--features dashtable`.
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

use core::mem::MaybeUninit;

use crate::{MAX_GLOBAL_DEPTH, bit_from_top, fingerprint};

/// The segment geometry (PR-5b, the bucketed Dragonfly shape): 4 BUCKETS of 14 slots plus
/// a 4-slot STASH, 60 records per segment. A record's HOME bucket comes from spare hash
/// bits (disjoint from the directory's top bits and the fingerprint's low byte); placement
/// is TWO-CHOICE (home, then the next bucket) with the stash as the shared third chance,
/// which is what lifts the achievable load factor well above the flat-array layout's
/// split-at-first-overflow behavior -- the memory lever this shape exists for. All
/// presence + match bits pack into one `u64` (enforced below).
const BUCKETS: usize = 4;
const BUCKET_CAP: usize = 14;
const REGULAR_CAP: usize = BUCKETS * BUCKET_CAP; // 56
const STASH_CAP: usize = 4;
const SEGMENT_CAP: usize = REGULAR_CAP + STASH_CAP; // 60
/// The stash slots' presence-bit window (bits 56..60 of the segment mask).
const STASH_WINDOW: u64 = ((1u64 << STASH_CAP) - 1) << REGULAR_CAP;
const _: () = assert!(
    SEGMENT_CAP <= 64,
    "presence and fingerprint-match bits pack into a u64"
);
const _: () = assert!(
    BUCKETS.is_power_of_two(),
    "bucket_of masks with BUCKETS - 1"
);

/// A record's HOME bucket: two spare hash bits ABOVE the fingerprint byte (bits 8..10),
/// disjoint from both the fingerprint (bits 0..8) and the directory index (top bits), so
/// the three routing decisions carry independent entropy.
#[allow(clippy::cast_possible_truncation)] // masked to BUCKETS - 1 (< 4), exact.
fn bucket_of(h: u64) -> usize {
    ((h >> 8) & (BUCKETS as u64 - 1)) as usize
}

/// The presence/match-bit window of bucket `b` (14 bits at `b * 14`).
fn bucket_window(b: usize) -> u64 {
    ((1u64 << BUCKET_CAP) - 1) << (b * BUCKET_CAP)
}

/// The split-retry bound per insert: a backstop on loop iterations. The REAL guards against
/// pathological collisions are [`MAX_GLOBAL_DEPTH`] and the futility fast-path in
/// [`DashIndex::entry`] (see there); this bound only caps the loop count.
const MAX_SPLITS: u32 = 64;

/// Where a probe found a record: in the segment's inline slots or its overflow spill.
#[derive(Clone, Copy, Debug)]
enum Loc {
    /// Inline slot index (`0..REGULAR_CAP` a bucket slot, `REGULAR_CAP..SEGMENT_CAP` a
    /// stash slot). Valid iff the segment mask has that bit set.
    Inline(usize),
    /// Index into the overflow `Vec` (present only after a force-place).
    Overflow(usize),
}

/// One BUCKETED segment (see the module doc "The DENSE segment layout" and the geometry
/// consts above).
///
/// LAYOUT (`repr(C, align(64))`): line 0 is EXACTLY `mask` (8 bytes of presence bits) plus
/// `fps` (56 packed fingerprints) -- the one line every probe must read. Slots are NOT
/// packed: presence is the bitmask (removal just clears a bit; `Loc`s are stable), and the
/// `unsafe` invariant is `items[i]` initialized IFF mask bit `i` is set. The stash
/// fingerprints + metadata head line 1, followed by the items.
#[repr(C, align(64))]
struct Segment<T> {
    /// Presence bits: `0..REGULAR_CAP` the bucket slots, then the stash. Bit i set IFF
    /// `items[i]` is initialized (THE segment invariant every `unsafe` block cites).
    mask: u64,
    /// `fps[i]` is the fingerprint of bucket slot `i` WHEN mask bit `i` is set; stale
    /// bytes otherwise (the branchless match scan reads them all, then ANDs with `mask`).
    fps: [u8; REGULAR_CAP],
    /// Fingerprints of the stash slots (`items[REGULAR_CAP + j]`), mask-gated like `fps`.
    stash_fps: [u8; STASH_CAP],
    local_depth: u8,
    /// The FORCE-PLACE spill (see [`DashIndex::entry`]'s guards): `None` in every
    /// non-pathological segment (one pointer-width), a boxed `(fingerprint, record)` list
    /// otherwise. Probes and iteration consult it whenever present. The `Box` around the
    /// `Vec` is DELIBERATE (clippy's advice to drop it is wrong here): a bare
    /// `Option<Vec<..>>` is 24 inline bytes in EVERY segment of every table, paid for a
    /// spill that pathological collisions alone create; boxing keeps the common segment at
    /// one pointer-width and pushes the 24-byte Vec header behind it only when the spill
    /// actually exists.
    #[allow(clippy::box_collection)]
    overflow: Option<Box<Vec<(u8, T)>>>,
    items: [MaybeUninit<T>; SEGMENT_CAP],
}

impl<T> Segment<T> {
    fn new(local_depth: u8) -> Self {
        Segment {
            mask: 0,
            fps: [0; REGULAR_CAP],
            stash_fps: [0; STASH_CAP],
            local_depth,
            overflow: None,
            items: [const { MaybeUninit::uninit() }; SEGMENT_CAP],
        }
    }

    /// The total live records (inline + overflow).
    fn count(&self) -> usize {
        self.mask.count_ones() as usize + self.overflow.as_ref().map_or(0, |ov| ov.len())
    }

    /// Whether a record homed at bucket `b` can be placed INLINE: a free slot in its home
    /// bucket, the two-choice neighbor, or the stash. This -- not total occupancy -- is
    /// the bucketed split trigger: other buckets' free slots are unreachable to this
    /// record, so "the segment has room" is per-record now.
    fn has_room_for(&self, b: usize) -> bool {
        let free = !self.mask;
        let nb = (b + 1) & (BUCKETS - 1);
        free & (bucket_window(b) | bucket_window(nb) | STASH_WINDOW) != 0
    }

    /// The branchless fingerprint pre-scan over the bucket slots: one match bit per slot,
    /// scanning the whole fixed array with no early exit and no calls (the shape LLVM
    /// vectorizes); callers AND the result with `mask` and the relevant bucket windows.
    fn match_bits(&self, fp: u8) -> u64 {
        let mut bits = 0u64;
        for (i, &f) in self.fps.iter().enumerate() {
            bits |= u64::from(f == fp) << i;
        }
        bits
    }

    /// The probe: the location of the record whose fingerprint matches AND whose object
    /// satisfies `eq`, checking the hash's home bucket, its two-choice neighbor, the
    /// stash, then the spill. `eq` runs only on the ~1/256 of scanned records whose hash
    /// low byte collides.
    fn locate(&self, hash: u64, mut eq: impl FnMut(&T) -> bool) -> Option<Loc> {
        let fp = fingerprint(hash);
        let b = bucket_of(hash);
        let nb = (b + 1) & (BUCKETS - 1);
        let mut cand = self.match_bits(fp) & self.mask & (bucket_window(b) | bucket_window(nb));
        while cand != 0 {
            #[allow(clippy::cast_possible_truncation)] // trailing_zeros of a u64 is <= 64.
            let i = cand.trailing_zeros() as usize;
            cand &= cand - 1;
            // SAFETY: bit `i` of `mask` is set (cand was ANDed with it), so `items[i]` is
            // initialized (the segment invariant).
            if eq(unsafe { self.items[i].assume_init_ref() }) {
                return Some(Loc::Inline(i));
            }
        }
        let mut stash = self.mask & STASH_WINDOW;
        while stash != 0 {
            #[allow(clippy::cast_possible_truncation)]
            let i = stash.trailing_zeros() as usize;
            stash &= stash - 1;
            if self.stash_fps[i - REGULAR_CAP] == fp {
                // SAFETY: bit `i` of `mask` is set, so `items[i]` is initialized.
                if eq(unsafe { self.items[i].assume_init_ref() }) {
                    return Some(Loc::Inline(i));
                }
            }
        }
        if let Some(ov) = &self.overflow {
            for (i, (f, item)) in ov.iter().enumerate() {
                if *f == fp && eq(item) {
                    return Some(Loc::Overflow(i));
                }
            }
        }
        None
    }

    /// A shared borrow of the record at `loc`. `loc` must have come from THIS segment's
    /// [`Self::locate`]/[`Self::push`] with no intervening mutation (the callers hold the
    /// table's borrow, so the type system enforces that).
    fn get(&self, loc: Loc) -> &T {
        match loc {
            Loc::Inline(i) => {
                debug_assert!(self.mask & (1 << i) != 0, "loc bit must be present");
                // SAFETY: a `Loc::Inline(i)` is only constructed with mask bit `i` set
                // (locate / push), and no mutation intervened, so the slot is initialized.
                unsafe { self.items[i].assume_init_ref() }
            }
            Loc::Overflow(i) => &self.overflow.as_ref().expect("overflow loc implies spill")[i].1,
        }
    }

    /// A mutable borrow of the record at `loc` (same contract as [`Self::get`]).
    fn get_mut(&mut self, loc: Loc) -> &mut T {
        match loc {
            Loc::Inline(i) => {
                debug_assert!(self.mask & (1 << i) != 0, "loc bit must be present");
                // SAFETY: as in `get`: the loc came from this segment with the bit set.
                unsafe { self.items[i].assume_init_mut() }
            }
            Loc::Overflow(i) => {
                &mut self.overflow.as_mut().expect("overflow loc implies spill")[i].1
            }
        }
    }

    /// Place a record homed at `bucket`: its home bucket first, then the two-choice
    /// neighbor, then the stash, else the overflow spill (the force-place path -- the
    /// caller's split loop already decided growth cannot or should not help).
    fn push(&mut self, bucket: usize, fp: u8, item: T) -> Loc {
        let free = !self.mask;
        let nb = (bucket + 1) & (BUCKETS - 1);
        for window in [bucket_window(bucket), bucket_window(nb)] {
            let cand = free & window;
            if cand != 0 {
                #[allow(clippy::cast_possible_truncation)]
                let i = cand.trailing_zeros() as usize;
                self.fps[i] = fp;
                self.items[i].write(item);
                self.mask |= 1 << i;
                return Loc::Inline(i);
            }
        }
        let cand = free & STASH_WINDOW;
        if cand != 0 {
            #[allow(clippy::cast_possible_truncation)]
            let i = cand.trailing_zeros() as usize;
            self.stash_fps[i - REGULAR_CAP] = fp;
            self.items[i].write(item);
            self.mask |= 1 << i;
            return Loc::Inline(i);
        }
        let ov = self.overflow.get_or_insert_with(Box::default);
        ov.push((fp, item));
        Loc::Overflow(ov.len() - 1)
    }

    /// Remove and return the record at `loc` (same loc contract as [`Self::get`]).
    /// Inline removal just CLEARS the presence bit -- no record moves (the bitmask model's
    /// payoff over the packed-prefix layout).
    fn remove_at(&mut self, loc: Loc) -> T {
        match loc {
            Loc::Inline(i) => {
                debug_assert!(self.mask & (1 << i) != 0, "loc bit must be present");
                self.mask &= !(1u64 << i);
                // SAFETY: the bit WAS set (loc contract), so `items[i]` was initialized;
                // clearing the bit first means `Drop`/iteration never touch the moved-out
                // slot even if the caller panics after this returns.
                unsafe { self.items[i].assume_init_read() }
            }
            Loc::Overflow(i) => {
                let ov = self.overflow.as_mut().expect("overflow loc implies spill");
                let (_, item) = ov.swap_remove(i);
                if ov.is_empty() {
                    // Drop the spill so the common no-overflow probe fast path returns.
                    self.overflow = None;
                }
                item
            }
        }
    }

    /// The fingerprint of the record in inline slot `i` (bucket or stash region).
    fn fp_of(&self, i: usize) -> u8 {
        if i < REGULAR_CAP {
            self.fps[i]
        } else {
            self.stash_fps[i - REGULAR_CAP]
        }
    }

    /// Move EVERY record out (inline + overflow), leaving the segment empty. Used by the
    /// split to repartition. The output `Vec` is pre-sized, so the per-record pushes below
    /// cannot allocate; the mask is zeroed BEFORE the reads so `Drop` can never
    /// double-drop a moved-out slot.
    fn drain_all(&mut self) -> Vec<(u8, T)> {
        let mut out = Vec::with_capacity(self.count());
        let mut m = self.mask;
        self.mask = 0;
        while m != 0 {
            #[allow(clippy::cast_possible_truncation)]
            let i = m.trailing_zeros() as usize;
            m &= m - 1;
            // SAFETY: bit `i` was set in the pre-zeroed mask, so `items[i]` was
            // initialized; each slot is read out exactly once and never touched again.
            let item = unsafe { self.items[i].assume_init_read() };
            out.push((self.fp_of(i), item));
        }
        if let Some(ov) = self.overflow.take() {
            out.extend(*ov);
        }
        out
    }

    /// Iterate every live record (inline mask bits, then the overflow spill).
    fn iter(&self) -> impl Iterator<Item = &T> {
        let mut m = self.mask;
        core::iter::from_fn(move || {
            if m == 0 {
                return None;
            }
            #[allow(clippy::cast_possible_truncation)]
            let i = m.trailing_zeros() as usize;
            m &= m - 1;
            // SAFETY: bit `i` of the mask is set, so `items[i]` is initialized; the
            // shared borrow of `self` pins the mask for the iterator's lifetime.
            Some(unsafe { self.items[i].assume_init_ref() })
        })
        .chain(
            self.overflow
                .iter()
                .flat_map(|ov| ov.iter().map(|(_, item)| item)),
        )
    }

    /// Whether EVERY live record's fingerprint equals `fp` (the cheap futility prefilter;
    /// see [`DashIndex::entry`]).
    fn all_fps_equal(&self, fp: u8) -> bool {
        let mut m = self.mask;
        while m != 0 {
            #[allow(clippy::cast_possible_truncation)]
            let i = m.trailing_zeros() as usize;
            m &= m - 1;
            if self.fp_of(i) != fp {
                return false;
            }
        }
        self.overflow
            .as_ref()
            .is_none_or(|ov| ov.iter().all(|(f, _)| *f == fp))
    }
}

impl<T> Drop for Segment<T> {
    fn drop(&mut self) {
        let mut m = self.mask;
        while m != 0 {
            #[allow(clippy::cast_possible_truncation)]
            let i = m.trailing_zeros() as usize;
            m &= m - 1;
            // SAFETY: bit `i` of the mask is set, so `items[i]` is initialized; each is
            // dropped exactly once here, and the segment is never used again. The
            // overflow `Vec` drops itself after this body.
            unsafe { self.items[i].assume_init_drop() };
        }
    }
}

/// The panic guard for [`Segment`]'s manual `Clone`: `T::clone` is user code and may panic
/// mid-clone; the guard drops the records cloned so far (tracked as a bitmask) so nothing
/// leaks. On success the clone loop forgets the guard and ownership passes to the new
/// segment.
struct MaskGuard<'a, T> {
    items: &'a mut [MaybeUninit<T>; SEGMENT_CAP],
    done: u64,
}

impl<T> Drop for MaskGuard<'_, T> {
    fn drop(&mut self) {
        let mut m = self.done;
        while m != 0 {
            #[allow(clippy::cast_possible_truncation)]
            let i = m.trailing_zeros() as usize;
            m &= m - 1;
            // SAFETY: bit `i` of `done` is set only after the clone loop initialized
            // `items[i]` (see `Segment::clone`).
            unsafe { self.items[i].assume_init_drop() };
        }
    }
}

impl<T: Clone> Clone for Segment<T> {
    fn clone(&self) -> Self {
        let mut items: [MaybeUninit<T>; SEGMENT_CAP] =
            [const { MaybeUninit::uninit() }; SEGMENT_CAP];
        let mut guard = MaskGuard {
            items: &mut items,
            done: 0,
        };
        let mut m = self.mask;
        while m != 0 {
            #[allow(clippy::cast_possible_truncation)]
            let i = m.trailing_zeros() as usize;
            m &= m - 1;
            // SAFETY: source bit `i` is set, so the source slot is initialized.
            let src = unsafe { self.items[i].assume_init_ref() };
            guard.items[i].write(src.clone());
            guard.done |= 1 << i;
        }
        // Clone the overflow BEFORE forgetting the guard: `T::clone` in the spill is user
        // code too, and with the guard already forgotten a panic there would leak every
        // freshly cloned inline record (the bare `MaybeUninit` array drops nothing) --
        // proven by an adversarial probe under miri. With this ordering the unwind runs
        // through the still-armed guard, which drops the cloned records.
        let overflow = self.overflow.clone();
        core::mem::forget(guard);
        Segment {
            mask: self.mask,
            fps: self.fps,
            stash_fps: self.stash_fps,
            local_depth: self.local_depth,
            overflow,
            items,
        }
    }
}

impl<T> core::fmt::Debug for Segment<T> {
    /// Structural debug only (depth + occupancy): the record slots are `MaybeUninit`, so a
    /// derived impl is impossible, and the store's `Debug` (a derive over the shard store)
    /// only needs the shape to render.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Segment")
            .field("local_depth", &self.local_depth)
            .field("live", &self.mask.count_ones())
            .field("overflow", &self.overflow.as_ref().map_or(0, |ov| ov.len()))
            .finish_non_exhaustive()
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
        let loc = seg.locate(hash, eq)?;
        Some(seg.get(loc))
    }

    /// Look up the record for `hash` whose object satisfies `eq` (mutable).
    ///
    /// Mirrors `hashbrown::HashTable::find_mut`. The caller must not mutate the embedded
    /// key in a way that changes its hash (the same contract hashbrown documents).
    #[must_use]
    pub fn find_mut(&mut self, hash: u64, eq: impl FnMut(&T) -> bool) -> Option<&mut T> {
        let si = self.route(hash)?;
        let seg = &mut self.segments[si];
        let loc = seg.locate(hash, eq)?;
        Some(seg.get_mut(loc))
    }

    /// The upsert funnel, mirroring `hashbrown::HashTable::entry`: returns
    /// [`Entry::Occupied`] when a record matches (`get`/`get_mut` it), else
    /// [`Entry::Vacant`] (call `insert`). `hasher` recomputes an EXISTING record's hash
    /// from its object; it is needed because making room for a new record may SPLIT the
    /// routed segment (repartitioning records by one more hash bit), exactly as
    /// hashbrown's `entry` may need it to grow the table. A `hasher` panic mid-split can
    /// lose that segment's records (they were mid-move) but never breaks memory safety --
    /// the same class of contract hashbrown documents for a panicking hasher.
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
        let bucket = bucket_of(hash);
        let si = self.directory[self.dir_index(hash)] as usize;
        if let Some(loc) = self.segments[si].locate(hash, eq) {
            return Entry::Occupied(OccupiedEntry {
                table: self,
                seg: si,
                loc,
            });
        }
        // Absent: make room in the routed segment BEFORE handing out the vacant placement
        // (a split re-routes; the loop re-reads the directory until the routed segment has
        // room, or a guard trips and the record is FORCE-PLACED into the overflow spill --
        // the segment then grows linearly, which stays correct, just slower to probe).
        // Guards, in order:
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
            // BUCKETED trigger: split when THIS record's placement options (home bucket,
            // two-choice neighbor, stash) are all full -- not when the whole segment is.
            // Other buckets' free slots are unreachable to this record, so total-count
            // room would let the spill grow unbounded under bucket skew.
            if seg.has_room_for(bucket) || splits >= MAX_SPLITS {
                break;
            }
            if seg.all_fps_equal(fp) && seg.iter().all(|it| hasher(it) == hash) {
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
            bucket,
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
        match self.segments[si].locate(hash, eq) {
            Some(loc) => Ok(OccupiedEntry {
                table: self,
                seg: si,
                loc,
            }),
            None => Err(Absent),
        }
    }

    /// Pre-size for `additional` more records, mirroring `hashbrown::HashTable::reserve`'s
    /// role as the bulk-load seam (the store's `reserve` spreads a keyspace across its slot
    /// tables before a fill; consumer: the memmodel bench).
    ///
    /// On an EMPTY table this builds a directory of enough DISTINCT segments that a
    /// uniform fill of `additional` records lands at ~7/8 mean segment load (the
    /// power-of-two round-up spreads actual loads over (7/16, 7/8]). Segment slot storage
    /// is INLINE (the dense layout), so a well-mixed fill after a reserve performs no
    /// PER-RECORD allocation -- but note the contract is STATISTICAL, not absolute: at
    /// mean loads near the target boundary, binomial spread overflows a fraction of BUCKETS
    /// (measured ~12% at exactly-boundary sizes), each a local split, and the FIRST such
    /// split doubles the directory (reserve leaves every local depth == global depth).
    /// The memmodel bench's table-vs-object decomposition should treat those allocations
    /// as noise at its measurement sizes (~0.2 B/key) rather than assume literal zero.
    /// On a non-empty table it only
    /// pre-DOUBLES the directory to the target depth (pointer copies; no records move) --
    /// segments still split organically on overflow, which is dash's incremental growth
    /// working as designed. NOTE the contract is deliberately WEAKER than hashbrown's
    /// "no resize during the fill": an extendible-hashing fill can always locally split a
    /// hash-SKEWED segment; what reserve removes is the directory-wide work. The target
    /// depth is clamped to [`MAX_GLOBAL_DEPTH`], the same hard directory bound the split
    /// path enforces. `hasher` is accepted for shape-parity with the hashbrown API (a
    /// future eager pre-split would repartition with it) but the current pre-sizing moves
    /// no records.
    pub fn reserve(&mut self, additional: usize, hasher: impl Fn(&T) -> u64) {
        let _ = &hasher; // shape-parity; see the doc comment.
        let needed = self.len + additional;
        if needed == 0 {
            return;
        }
        // Target ~7/8 of SEGMENT_CAP per segment: the memory-decisive constant. The
        // interplay with the power-of-two round-up DOMINATES this choice: a smaller
        // divisor that nudges the segment count just past a 2^k boundary DOUBLES the
        // reserved footprint (measured: a 4/5 retune crossed a boundary at the
        // memmodel size and produced ~2x hashbrown's table bytes; 5/8 did the same in
        // the flat layout's history). At 7/8, per-bucket mean load sits near the cap,
        // so a RESERVED fill pays some bucket-boundary splits (a bench-only artifact:
        // production tables grow organically, and the memmodel caveat is documented
        // above); a mixed-local-depth reserve (alias pairs for non-power-of-two
        // segment counts, splitting locally without any doubling) is the noted
        // follow-up that removes the round-up cliff entirely. The segment
        // count rounds up to a power of two (the directory is one), then CLAMPS to the
        // hard directory bound -- reserve must not build what the split path refuses to
        // grow to (and the clamp is what makes the u32 directory-entry cast below exact by
        // construction: 2^20 < u32::MAX).
        #[allow(clippy::cast_possible_truncation)] // 64-bit usize: log2 < 64 always fits u8.
        let target_depth = (needed
            .div_ceil(SEGMENT_CAP * 7 / 8)
            .next_power_of_two()
            .max(1)
            .trailing_zeros() as u8)
            .min(MAX_GLOBAL_DEPTH);
        let target_segments = 1usize << target_depth;
        if self.directory.is_empty() {
            // The empty fast path: build the pre-sized shape directly, every directory
            // entry owning its own segment at local depth == global depth (no aliasing).
            self.global_depth = target_depth;
            #[allow(clippy::cast_possible_truncation)]
            // target_segments = 2^target_depth <= 2^MAX_GLOBAL_DEPTH < u32::MAX.
            {
                self.directory = (0..target_segments as u32).collect();
            }
            self.segments = (0..target_segments)
                .map(|_| Segment::new(target_depth))
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
        self.segments.iter().flat_map(Segment::iter)
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
    /// global depth. Mechanics identical to [`crate::Dashtable::split`]; the records move
    /// through [`Segment::drain_all`] + [`Segment::push`] (which re-fills inline slots
    /// first, so a split of a force-placed segment shrinks its overflow back inline).
    fn split(&mut self, dir_idx: usize, hasher: &impl Fn(&T) -> u64) {
        let seg_idx = self.directory[dir_idx] as usize;
        let local = self.segments[seg_idx].local_depth;
        if local == self.global_depth {
            self.double_directory();
        }
        let new_local = local + 1;
        let buddy_idx = self.segments.len();
        // BOUNDED-SLACK growth for the segments Vec: segments are BY-VALUE (the dense
        // layout's probe locality), so Vec's default doubling would strand ~600 dead
        // bytes per unused capacity slot -- a segment-array-level trough, the exact
        // pathology this table exists to remove. But exact growth (reserve_exact(1) per
        // split) is QUADRATIC in segment count over an organic fill (measured 28x slower
        // per-insert than hashbrown at 4M keys/table). The compromise: grow by ~1/8 of
        // the current length -- amortized O(1) memcpy per split, transient slack capped
        // at 12.5% of segment bytes (~1.6 B/key worst case, far under the 2x doubling
        // trough being avoided).
        if self.segments.len() == self.segments.capacity() {
            let grow = (self.segments.len() / 8).max(1);
            self.segments.reserve_exact(grow);
        }
        self.segments.push(Segment::new(new_local));
        self.segments[seg_idx].local_depth = new_local;
        // Repartition: records whose new-local-th top hash bit is 1 move to the buddy.
        // SELF-ACCOUNTING len across the move: debit the drained records up front and
        // credit each one back as it is re-placed, so a `hasher` panic mid-loop (which
        // drops the not-yet-placed records; memory-safe, records lost per the entry()
        // contract) leaves `len()` equal to the TRUE live count instead of overcounting
        // forever (DBSIZE reads it).
        let drained = self.segments[seg_idx].drain_all();
        self.len -= drained.len();
        for (fp, item) in drained {
            let h = hasher(&item);
            let target = if bit_from_top(h, new_local) {
                buddy_idx
            } else {
                seg_idx
            };
            self.segments[target].push(bucket_of(h), fp, item);
            self.len += 1;
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
    loc: Loc,
}

impl<'a, T> OccupiedEntry<'a, T> {
    /// The matched record (shared).
    #[must_use]
    pub fn get(&self) -> &T {
        self.table.segments[self.seg].get(self.loc)
    }

    /// The matched record (mutable, borrow bound to `&mut self`). The caller must not
    /// change the embedded key's hash (the hashbrown contract).
    #[must_use]
    pub fn get_mut(&mut self) -> &mut T {
        self.table.segments[self.seg].get_mut(self.loc)
    }

    /// The matched record (mutable, consuming the entry: the borrow lives as long as the
    /// original `&'a mut` table borrow). Mirrors hashbrown's `into_mut`.
    #[must_use]
    pub fn into_mut(self) -> &'a mut T {
        self.table.segments[self.seg].get_mut(self.loc)
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
        let item = self.table.segments[self.seg].remove_at(self.loc);
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
/// plain placement (into the inline block, or the overflow spill on the force-place path).
pub struct VacantEntry<'a, T> {
    table: &'a mut DashIndex<T>,
    seg: usize,
    fp: u8,
    /// The record's home bucket (from the query hash), for the two-choice placement.
    bucket: usize,
}

impl<'a, T> VacantEntry<'a, T> {
    /// Place `value`, returning the occupied entry for it (mirrors hashbrown).
    pub fn insert(self, value: T) -> OccupiedEntry<'a, T> {
        let loc = self.table.segments[self.seg].push(self.bucket, self.fp, value);
        self.table.len += 1;
        OccupiedEntry {
            table: self.table,
            seg: self.seg,
            loc,
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
        // Miri runs ~two orders slower; 4 segments' worth still forces splits + a doubling.
        let scale: usize = if cfg!(miri) { 4 } else { 40 };
        let n = (SEGMENT_CAP * scale) as u64; // forces many splits + directory doublings
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
        let n = (SEGMENT_CAP * if cfg!(miri) { 2 } else { 8 }) as u64;
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
        // Every key hashes IDENTICALLY: splits cannot separate them, so the futility guard
        // must trip and FORCE-PLACE into the overflow spill rather than loop forever.
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
    fn overflow_spill_roundtrip_with_drop_heavy_payload() {
        // The force-place path with a HEAP-owning payload (String), so miri verifies no
        // leak and no double-drop across: spill creation, inline+overflow interleaved
        // removal, spill drain-back on removal-to-empty, clone divergence, and table drop.
        let mut t: DashIndex<(u64, String)> = DashIndex::new();
        let n = (SEGMENT_CAP + 16) as u64;
        for k in 0..n {
            match t.entry(7, |r| r.0 == k, |_| 7) {
                Entry::Occupied(_) => panic!("distinct keys"),
                Entry::Vacant(e) => {
                    e.insert((k, format!("value-{k}")));
                }
            }
        }
        assert_eq!(t.len(), n as usize);
        assert_eq!(t.iter().count(), n as usize);
        // Clone while the spill is live; diverge both sides.
        let mut c = t.clone();
        // Remove EVERY record from the clone (drains the spill back to None and empties
        // the inline block); the original must be untouched.
        for k in 0..n {
            let got = c
                .find_entry(7, |r| r.0 == k)
                .ok()
                .map(|occ| occ.remove().0.1);
            assert_eq!(got.as_deref(), Some(format!("value-{k}").as_str()));
        }
        assert!(c.is_empty());
        assert_eq!(t.len(), n as usize, "original untouched by clone drain");
        // Interleaved removal from the ORIGINAL: alternate keys, spanning inline+overflow.
        for k in (0..n).step_by(2) {
            assert!(t.find_entry(7, |r| r.0 == k).is_ok(), "key {k} present");
            let (rec, _) = t.find_entry(7, |r| r.0 == k).unwrap().remove();
            assert_eq!(rec.0, k);
        }
        for k in 0..n {
            let expect = k % 2 != 0;
            assert_eq!(
                t.find(7, |r| r.0 == k).is_some(),
                expect,
                "key {k} wrong after interleaved spill removes"
            );
        }
        // The survivors drop with the table here; miri asserts nothing leaks.
    }

    #[test]
    fn split_of_a_force_placed_segment_reflows_overflow_inline() {
        // Records collide on the TOP bits up to the depth cap boundary is expensive to
        // stage; instead: fill one segment past CAP with two SEPARABLE hash groups that
        // share the top bit pattern only at depth 0 (hash 0 vs 1 << 62), then insert a
        // record that forces a split. The split's drain+push must reflow overflow records
        // back into inline slots of the two halves.
        let mut t: DashIndex<Rec> = DashIndex::new();
        let h_a = 0u64; // top bits 00...
        let h_b = 1u64 << 62; // top bits 01... (separates at depth 2)
        let n = SEGMENT_CAP as u64 + 8; // forces force-place at depth 0 before the split
        // Alternate the two groups so both sides hold records; the segment overflows
        // because at depth 0 everything routes to the one segment, and the split loop
        // (futility fails: hashes differ) splits until they separate -- but we insert with
        // entry(), so splits happen on the way. To FORCE the spill first, use a constant
        // hash for the first CAP+8 inserts (futility trips), then switch to separable
        // hashes for the next inserts, which split the segment and must reflow.
        for k in 0..n {
            match t.entry(h_a, |r| r.key == k, |r| if r.key < n { h_a } else { h_b }) {
                Entry::Occupied(_) => panic!("distinct keys"),
                Entry::Vacant(e) => {
                    e.insert(Rec { key: k, val: k });
                }
            }
        }
        assert_eq!(t.len(), n as usize, "spill populated");
        // Now insert records whose hash differs (h_b): the split loop can separate the
        // groups (futility no longer holds), reflowing the h_a spill inline.
        for k in n..(n + 8) {
            match t.entry(h_b, |r| r.key == k, |r| if r.key < n { h_a } else { h_b }) {
                Entry::Occupied(_) => panic!("distinct keys"),
                Entry::Vacant(e) => {
                    e.insert(Rec { key: k, val: k });
                }
            }
        }
        // Everything stays findable through its own hash.
        for k in 0..n {
            assert_eq!(
                t.find(h_a, |r| r.key == k).map(|r| r.val),
                Some(k),
                "group-a key {k} lost across the reflow split"
            );
        }
        for k in n..(n + 8) {
            assert_eq!(
                t.find(h_b, |r| r.key == k).map(|r| r.val),
                Some(k),
                "group-b key {k} lost"
            );
        }
    }

    #[test]
    // Not under miri: reaching the depth cap builds a 2^20-entry directory (plus every
    // intermediate doubling), which miri's memory tracking amplifies into a resource
    // blowup. The UNSAFE paths this test drives (force-place, overflow probes/removal)
    // are miri-covered at low depth by the overflow + pathological-collision tests; the
    // depth-cap arithmetic itself has no unsafe and is exercised natively.
    #[cfg_attr(miri, ignore)]
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
    fn clone_panic_in_overflow_drops_the_cloned_inline_prefix() {
        // REGRESSION (adversarial review): `Segment::clone` used to forget its panic
        // guard BEFORE cloning the overflow spill, so a panicking `T::clone` in the
        // spill leaked every freshly cloned inline record (proven with a miri probe).
        // This test re-creates that exact shape -- a full inline block + a spill whose
        // first record's clone panics -- and asserts, via a drop-counting payload, that
        // every clone made before the panic is dropped during the unwind. Runs under
        // miri (the leak checker is the authoritative judge).
        use std::sync::atomic::{AtomicI64, Ordering};
        static LIVE: AtomicI64 = AtomicI64::new(0);

        #[derive(Debug)]
        struct Bomb {
            key: u64,
            armed: bool,
        }
        impl Clone for Bomb {
            fn clone(&self) -> Self {
                assert!(!self.armed, "spill clone bomb");
                LIVE.fetch_add(1, Ordering::SeqCst);
                Bomb {
                    key: self.key,
                    armed: false,
                }
            }
        }
        impl Drop for Bomb {
            fn drop(&mut self) {
                LIVE.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let mut t: DashIndex<Bomb> = DashIndex::new();
        let n = (SEGMENT_CAP + 4) as u64; // fills inline, spills 4 (constant hash)
        for k in 0..n {
            LIVE.fetch_add(1, Ordering::SeqCst); // the original insert
            let armed = k == u64::try_from(SEGMENT_CAP).unwrap(); // the FIRST spill record
            match t.entry(9, |r| r.key == k, |_| 9) {
                Entry::Occupied(_) => panic!("distinct keys"),
                Entry::Vacant(e) => {
                    e.insert(Bomb { key: k, armed });
                }
            }
        }
        let before = LIVE.load(Ordering::SeqCst);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| t.clone()));
        assert!(panicked.is_err(), "the spill bomb must have gone off");
        assert_eq!(
            LIVE.load(Ordering::SeqCst),
            before,
            "every record cloned before the panic must have been dropped (no leak)"
        );
        drop(t);
        assert_eq!(LIVE.load(Ordering::SeqCst), 0, "originals all dropped");
    }

    #[test]
    fn clone_is_deep_and_diverges() {
        let mut a = DashIndex::new();
        for k in 0..(SEGMENT_CAP as u64 * if cfg!(miri) { 2 } else { 4 }) {
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
        let n = if cfg!(miri) { 512 } else { 4096 };
        t.reserve(n, |r| hash_of(r.key));
        let depth_after_reserve = t.global_depth();
        let segs_after_reserve = t.segment_count();
        assert!(segs_after_reserve.is_power_of_two());
        // A PERFECTLY uniform fill: hashes crafted so BOTH routing decisions are balanced
        // -- the top bits spread evenly over the directory (i * step) AND the bucket bits
        // (8..10) round-robin over the four in-segment buckets (i & 3, OR'd in without
        // perturbing the top bits). Segment AND bucket occupancy are then exactly
        // balanced, so NO split or doubling can occur -- proving the pre-size actually
        // pre-sized. (Top-bit uniformity alone is NOT enough under the bucketed layout:
        // the i * step stride's bucket bits cluster, overflowing one bucket pair well
        // below segment capacity -- which is a legitimate SPLIT, per the reserve doc's
        // statistical caveat, not a pre-size failure.)
        let step = u64::MAX / n as u64;
        for i in 0..n {
            let h = (i as u64 * step) & !0x3FF | ((i as u64 & 3) << 8) | (i as u64 & 0xFF);
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
    // Not under miri: the clamped reserve still doubles the directory to 2^20 entries
    // (see the deep-prefix test's note); no unsafe is exercised beyond what the other
    // tests cover.
    #[cfg_attr(miri, ignore)]
    fn reserve_clamps_the_directory_depth_to_the_hard_cap() {
        use super::MAX_GLOBAL_DEPTH;
        // A reservation large enough to want depth > MAX_GLOBAL_DEPTH must be CLAMPED:
        // reserve shares the split path's hard directory bound (an unclamped depth would
        // also break the u32 directory-entry cast at astronomical sizes). Exercised via
        // the NON-empty path (directory doubling only, ~4 MiB) -- the empty fast path
        // shares the same clamped target_depth, and building 2^20 dense segments in a
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
