// SPDX-License-Identifier: MIT OR Apache-2.0
//! W-TinyLFU-fronted eviction (#49, WTINYLFU.md, ADR-0008 selectable variant).
//!
//! This is the selectable W-TinyLFU variant that fronts eviction with a frequency
//! estimator (a 4-bit count-min sketch), NOT the S3-FIFO default. It plugs into the
//! SAME [`EvictionHook`] + [`crate::EvictionPolicy`] traits as S3-FIFO and Random, so
//! it drops into the per-shard store with no store/waist change (it is a new
//! [`crate::Policy`] variant, not a new primitive).
//!
//! Per WTINYLFU.md the variant keeps ONLY the frequency comparison: there is NO SLRU
//! window and NO per-hit list relink (both deferred / dropped, see the spec's
//! "Reconciliation with EVICTION.md"), and NO doorkeeper (OFF by default, also
//! deferred here).
//!
//! ## Decision-path-only sketch (no per-read mutation, #57, WTINYLFU.md:135-136)
//!
//! WTINYLFU.md's "Decision-path contract" specifies a sketch that is consulted and
//! min-incremented ONLY on the admission/eviction decision path, never on the GET hot
//! path, plus an acceptance lint asserting "no per-read sketch mutation". This module
//! implements exactly that: [`WTinyLfu::on_access`] is a NO-OP (the GET read path does
//! NOT touch the sketch, so the read path stays the FIFO-class core's in-place
//! metadata write with no list relink). The sketch is min-incremented at exactly two
//! points, both on the decision path:
//!
//! - the CANDIDATE, in [`EvictionHook::on_insert`] ("seen at the door"): one
//!   min-increment for the just-inserted key, which is also recorded as the pending
//!   admission candidate;
//! - the VICTIM, inside the admission door ([`WTinyLfu::admit_or_reject`]): one
//!   min-increment of the chosen would-be victim when it is evaluated.
//!
//! This is the headline #57 contract: frequency tracks the stream of admission
//! candidates and victims, not every read.
//!
//! ## What lives here
//!
//! - [`CmSketch`]: a 4-bit count-min frequency estimator with MIN-INCREMENT and
//!   PERIODIC HALVING aging (`wtinylfu-cmsketch-4bit`). Deterministic seeded hashing
//!   (ADR-0003: no `RandomState`, no std rand). UNCHANGED except for WHERE
//!   [`CmSketch::increment`] is now called (the decision path, not per read).
//! - [`WTinyLfu`]: the policy. A recency victim FIFO of resident keys (like
//!   S3-FIFO's main) so [`EvictionHook::select_victim`] can return a `(db, key)`;
//!   `on_access` is a NO-OP (decision-path contract); `on_insert` tracks the key,
//!   bumps the sketch, and records the pending admission candidate; `on_remove`
//!   untracks (and clears a stale pending candidate); a lowest-priority re-offer FIFO
//!   carries the #46 volatile-only re-eligibility contract (same distinct-set bound
//!   as S3-FIFO).
//!
//! ## The candidate-admission DOOR (wired through the FROZEN EvictionHook)
//!
//! Full TinyLFU admits an incoming candidate over the chosen victim only if
//! `sketch.estimate(candidate) > sketch.estimate(victim)` (a STRICT win; incumbent
//! wins ties, WTINYLFU.md "Tie-break"). That decision needs the CANDIDATE key at the
//! eviction boundary, but the store's `evict_to_fit` drives eviction purely through
//! [`EvictionHook::select_victim`] (a `&mut self -> Option<(db, key)>` with NO
//! candidate argument). Rather than change that FROZEN trait, this policy captures the
//! candidate out-of-band: `on_insert` records the most-recently-inserted `(db, key)`
//! as `self.candidate`, and [`WTinyLfu::select_victim`] consumes it ONE-SHOT on the
//! first call of an `evict_to_fit` run via [`WTinyLfu::admit_or_reject`].
//!
//! The door compares the candidate against the COLDEST resident (the would-be
//! victim):
//!
//! - candidate IS the coldest resident -> trivially self-evict (one key);
//! - candidate estimate STRICTLY greater than the victim estimate -> ADMIT the
//!   candidate by evicting the VICTIM;
//! - tie or candidate colder -> REJECT: the just-inserted candidate is itself the
//!   victim returned (stored-then-evicted), the incumbent wins the tie.
//!
//! After the candidate is consumed, `select_victim` falls through to the existing
//! plain frequency-ordered choice (coldest resident, then the re-offer FIFO).
//!
//! ## Redis scan-resistance semantics (this is the SELECTABLE policy, NOT the default)
//!
//! "A rejected cold candidate is stored-then-evicted": the write SUCCEEDS into the
//! keyspace and is immediately reclaimed by the door when it loses to a hotter
//! incumbent, so a scan flood of cold one-hit candidates churns through their own
//! slots while the established hot residents survive. This is the scan-resistance
//! W-TinyLFU exists for. Note this is the SELECTABLE W-TinyLFU-fronted variant
//! (`allkeys-lfu` / `volatile-lfu`); the DEFAULT eviction core is S3-FIFO (ADR-0008)
//! and is untouched by this module.
//!
//! ## Determinism and shared-nothing (ADR-0002/0003/0005)
//!
//! The sketch hashes with a FIXED-SEED deterministic mixer (a SplitMix64-style
//! finalizer per row, seeded by a compile-time constant XOR'd with the row index), so
//! a seeded replay makes identical estimates and identical victim choices; it never
//! touches `RandomState` or OS entropy. The policy is per-shard and unsynchronized
//! (`&mut self`, owned `Vec`/`VecDeque`; no `std::sync`, no atomics).

use std::collections::VecDeque;

use ironcache_storage::EvictionHook;

use crate::EvictionPolicy;

/// The maximum value a 4-bit counter can hold (saturates at 15,
/// `wtinylfu-cmsketch-4bit`).
const COUNTER_MAX: u8 = 15;

/// The count-min sketch DEPTH (number of independent hash rows). Provisional 4 rows
/// (WTINYLFU.md "provisional depth 4 rows"); a #47/#8 harness knob. More rows lower
/// the overestimate probability at a linear cost on the (bounded) decision path.
const SKETCH_DEPTH: usize = 4;

/// The count-min sketch WIDTH (counters per row), a power of two so the column index
/// is a cheap mask. Provisional default (WTINYLFU.md "width sized to about 8 bytes per
/// entry"); a #47/#8 harness knob. 1024 columns x 4 rows x 4 bits = 2 KiB per shard,
/// independent of the keyspace size (the sketch is out of the object, so cold keys
/// cost near zero, unlike a per-object Morris counter).
const SKETCH_WIDTH: usize = 1024;

/// The aging SAMPLE SIZE: once the running increment count reaches this, every counter
/// is halved (right-shift by one) to bound staleness and track phase changes
/// (WTINYLFU.md "Aging: periodic halving", provisional `10x cache-max`). Here it is a
/// fixed multiple of the sketch width (a proxy for the cache maximum, since the true
/// shard cache-max is not known to the policy); a #47/#8 harness knob. Kept modest so
/// aging actually fires under test-sized workloads.
const AGING_SAMPLE_SIZE: u64 = (SKETCH_WIDTH as u64) * 10;

/// A 4-bit count-min sketch frequency estimator (`wtinylfu-cmsketch-4bit`).
///
/// Counters are packed two-per-byte (4 bits each) in a flat `Vec<u8>` of
/// `depth * width / 2` bytes. [`Self::increment`] does a MIN-INCREMENT (bump only the
/// smallest counter across the depth rows) to bound overestimation; [`Self::estimate`]
/// returns the MINIMUM across rows (the count-min lower bound). Aging halves all
/// counters once the running increment count reaches [`AGING_SAMPLE_SIZE`].
#[derive(Debug, Clone)]
pub struct CmSketch {
    /// Packed 4-bit counters: `cells[row * width + col]` is the logical counter,
    /// stored as a nibble in `packed[idx / 2]` (low nibble for even idx, high for odd).
    packed: Vec<u8>,
    /// Per-row hash seeds (fixed, deterministic; NOT OS entropy).
    seeds: [u64; SKETCH_DEPTH],
    /// The running count of increments since the last halving (drives aging).
    increments: u64,
    /// Total number of counter HALVINGS performed (test/introspection).
    agings: u64,
}

impl Default for CmSketch {
    fn default() -> Self {
        Self::new()
    }
}

impl CmSketch {
    /// A fresh, all-zero sketch with the provisional [`SKETCH_DEPTH`] x [`SKETCH_WIDTH`]
    /// geometry and FIXED per-row seeds (ADR-0003 determinism).
    #[must_use]
    pub fn new() -> Self {
        // Fixed base seed (a SplitMix64 golden-ratio-ish constant), XOR-rotated per row
        // so the rows are independent yet fully deterministic across runs. No OS entropy.
        const BASE_SEED: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut seeds = [0u64; SKETCH_DEPTH];
        for (i, s) in seeds.iter_mut().enumerate() {
            // A distinct, deterministic seed per row: mix the base with the row index.
            *s = splitmix(BASE_SEED ^ (0xD1B5_4A32_D192_ED03u64.wrapping_mul(i as u64 + 1)));
        }
        CmSketch {
            packed: vec![0u8; SKETCH_DEPTH * SKETCH_WIDTH / 2],
            seeds,
            increments: 0,
            agings: 0,
        }
    }

    /// The flat cell index for `(row, col)`.
    fn cell_index(row: usize, col: usize) -> usize {
        row * SKETCH_WIDTH + col
    }

    /// Read the 4-bit counter at flat cell index `idx`.
    fn get_cell(&self, idx: usize) -> u8 {
        let byte = self.packed[idx / 2];
        if idx % 2 == 0 { byte & 0x0F } else { byte >> 4 }
    }

    /// Write the 4-bit counter at flat cell index `idx` (value masked to 4 bits).
    fn set_cell(&mut self, idx: usize, val: u8) {
        let v = val & 0x0F;
        let byte = &mut self.packed[idx / 2];
        if idx % 2 == 0 {
            *byte = (*byte & 0xF0) | v;
        } else {
            *byte = (*byte & 0x0F) | (v << 4);
        }
    }

    /// The column index for `key` in `row` (deterministic, seeded; masked to the
    /// power-of-two width).
    fn column(&self, row: usize, key: &[u8], db: u32) -> usize {
        let h = hash_key(self.seeds[row], db, key);
        (h as usize) & (SKETCH_WIDTH - 1)
    }

    /// Estimate the frequency of `(db, key)`: the MINIMUM counter across the depth rows
    /// (the count-min lower-bound estimate). O(depth) reads, the bounded decision-path
    /// cost (WTINYLFU.md "a small bounded (depth-many) set of reads").
    #[must_use]
    pub fn estimate(&self, db: u32, key: &[u8]) -> u8 {
        let mut min = COUNTER_MAX;
        for row in 0..SKETCH_DEPTH {
            let col = self.column(row, key, db);
            let c = self.get_cell(Self::cell_index(row, col));
            if c < min {
                min = c;
            }
        }
        min
    }

    /// Record one access of `(db, key)` with a MIN-INCREMENT: bump ONLY the counters
    /// equal to the current minimum across the depth rows (Caffeine's conservative
    /// update; bounds overestimation). Saturates at [`COUNTER_MAX`]. Advances the aging
    /// clock and halves all counters when it reaches [`AGING_SAMPLE_SIZE`].
    pub fn increment(&mut self, db: u32, key: &[u8]) {
        // Find the per-row columns and the current minimum across rows.
        let mut cols = [0usize; SKETCH_DEPTH];
        let mut min = COUNTER_MAX;
        for (row, slot) in cols.iter_mut().enumerate() {
            let col = self.column(row, key, db);
            *slot = col;
            let c = self.get_cell(Self::cell_index(row, col));
            if c < min {
                min = c;
            }
        }
        // Min-increment (Caffeine's conservative update): bump ONLY the cells AT the
        // current minimum. Cells already above the minimum exceed the true count, so
        // bumping them would only worsen the overestimate. A counter at the cap stays
        // saturated. This is what bounds the count-min overestimate.
        if min < COUNTER_MAX {
            for (row, &col) in cols.iter().enumerate() {
                let idx = Self::cell_index(row, col);
                let c = self.get_cell(idx);
                if c == min {
                    self.set_cell(idx, c + 1);
                }
            }
        }
        // Aging: count this increment and halve all counters once the sample size is hit.
        self.increments += 1;
        if self.increments >= AGING_SAMPLE_SIZE {
            self.halve();
            self.increments = 0;
        }
    }

    /// Halve every counter (right-shift by one), the periodic aging pass
    /// (WTINYLFU.md). A single linear sweep over the packed words; both nibbles of each
    /// byte are shifted independently. This decays old frequencies so the estimate
    /// tracks phase changes and CAPS the counters back down (so a long-lived hot key
    /// cannot pin a stale victim forever).
    pub fn halve(&mut self) {
        for byte in &mut self.packed {
            let lo = (*byte & 0x0F) >> 1;
            let hi = ((*byte >> 4) & 0x0F) >> 1;
            *byte = (hi << 4) | lo;
        }
        self.agings += 1;
    }

    /// The number of halvings performed (test/introspection).
    #[must_use]
    pub fn agings(&self) -> u64 {
        self.agings
    }
}

/// A deterministic key hash for a sketch row, seeded by `seed` (FNV-1a over the seed,
/// db, and key bytes, then a SplitMix64 finalizer). NOT cryptographic and NOT
/// `RandomState`: it is byte-identical on every run (ADR-0003).
fn hash_key(seed: u64, db: u32, key: &[u8]) -> u64 {
    // FNV-1a 64-bit over seed || db || key, then a splitmix finalizer to spread the bits
    // across the whole word (FNV alone leaves the low bits weakly mixed for the mask).
    let mut h: u64 = 0xCBF2_9CE4_8422_2325 ^ seed;
    for b in seed.to_le_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    for b in db.to_le_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    for &b in key {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    splitmix(h)
}

/// A SplitMix64 finalizer (public-domain Steele/Lea/Flood mix). Spreads bits so the
/// low-order mask used for the column index is well-distributed.
fn splitmix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A tracked resident key: a `(db, key)` in the recency victim FIFO.
#[derive(Debug, Clone)]
struct Entry {
    db: u32,
    key: Box<[u8]>,
}

impl Entry {
    fn matches(&self, db: u32, key: &[u8]) -> bool {
        self.db == db && self.key.as_ref() == key
    }
}

/// The W-TinyLFU-fronted eviction policy (per shard, unsynchronized; ADR-0005).
///
/// Holds the frequency [`CmSketch`] plus a recency FIFO of resident keys so
/// `select_victim` can return a `(db, key)`. See the module docs for the
/// candidate-admission door (the strict-win / incumbent-wins-ties rule) and the
/// decision-path-only sketch contract (no per-read mutation, #57).
#[derive(Debug, Clone)]
pub struct WTinyLfu {
    /// The 4-bit count-min frequency estimator.
    sketch: CmSketch,
    /// The recency FIFO of resident keys (insertion order; the victim search scans this
    /// for the lowest-frequency resident, FIFO order breaking frequency ties).
    resident: VecDeque<Entry>,
    /// The PENDING admission candidate: the most-recently-inserted `(db, key)`, recorded
    /// by `on_insert` ("seen at the door"). `select_victim` consumes it ONE-SHOT (a
    /// `take`) on the first call of an `evict_to_fit` run and runs it through the
    /// candidate-admission door ([`WTinyLfu::admit_or_reject`]). Cleared by `on_remove`
    /// if the candidate is deleted before the door runs (no stale candidate).
    candidate: Option<Entry>,
    /// The LOWEST-priority re-offer FIFO for the #46 volatile-only re-eligibility fix:
    /// a non-TTL victim the store declines to delete is re-registered HERE rather than
    /// back into `resident`. `select_victim` consults it ONLY after `resident` is
    /// exhausted, so every fresh resident candidate (an eligible TTL victim included)
    /// is offered BEFORE a re-registered key cycles again. Mirrors S3-FIFO's reoffer
    /// queue so the store's distinct-set bound terminates the scan with no false -OOM.
    reoffer: VecDeque<Entry>,
    /// Whether victims are restricted to TTL-bearing keys (the volatile-* family),
    /// enforced by the store in `evict_to_fit`.
    volatile_only: bool,
    /// The CONFIGURED `maxmemory-policy` name echoed VERBATIM by `policy_name()`
    /// (CONFIG GET / INFO). `map_policy_name` plants the configured spelling
    /// (`allkeys-lfu` / `volatile-lfu`); `new` defaults to the family name.
    name: String,
}

impl WTinyLfu {
    /// A fresh W-TinyLFU policy. `volatile_only` selects the `volatile-*` restriction;
    /// the configured name defaults to the family name (`allkeys-lfu`/`volatile-lfu`).
    #[must_use]
    pub fn new(volatile_only: bool) -> Self {
        let name = if volatile_only {
            "volatile-lfu"
        } else {
            "allkeys-lfu"
        };
        WTinyLfu::with_name(volatile_only, name)
    }

    /// A fresh W-TinyLFU policy carrying the exact CONFIGURED policy name, returned
    /// verbatim by [`EvictionPolicy::policy_name`] (CONFIG GET / INFO round-trip the
    /// configured enum string).
    #[must_use]
    pub fn with_name(volatile_only: bool, name: &str) -> Self {
        WTinyLfu {
            sketch: CmSketch::new(),
            resident: VecDeque::new(),
            reoffer: VecDeque::new(),
            candidate: None,
            volatile_only,
            name: name.to_owned(),
        }
    }

    /// The internal eviction ENGINE family label. SEPARATE from the configured Redis
    /// name [`Self::policy_name`] echoes verbatim: the engine that serves the name is
    /// W-TinyLFU here (the real LFU-family engine, no longer the 3a S3-FIFO stand-in).
    #[must_use]
    pub fn engine_family(&self) -> &'static str {
        if self.volatile_only {
            "volatile-lfu"
        } else {
            "allkeys-lfu"
        }
    }

    /// The frequency estimate for `(db, key)` (test/introspection; the decision path
    /// reads this for the victim search).
    #[must_use]
    pub fn estimate(&self, db: u32, key: &[u8]) -> u8 {
        self.sketch.estimate(db, key)
    }

    /// Whether `(db, key)` is tracked in either the resident or re-offer FIFO.
    fn tracks(&self, db: u32, key: &[u8]) -> bool {
        self.resident.iter().any(|e| e.matches(db, key))
            || self.reoffer.iter().any(|e| e.matches(db, key))
    }

    /// Remove `(db, key)` from whichever FIFO holds it. Returns whether it was found.
    fn remove_entry(&mut self, db: u32, key: &[u8]) -> bool {
        if let Some(i) = self.resident.iter().position(|e| e.matches(db, key)) {
            self.resident.remove(i);
            return true;
        }
        if let Some(i) = self.reoffer.iter().position(|e| e.matches(db, key)) {
            self.reoffer.remove(i);
            return true;
        }
        false
    }

    /// The index of the resident key with the LOWEST estimated frequency (FIFO order
    /// breaks ties: the EARLIEST-inserted lowest-frequency key wins, the recency
    /// fallback). `None` if `resident` is empty. This is the coldest-resident search
    /// shared by [`Self::pop_lowest_frequency_resident`] (which removes it) and the
    /// door [`Self::admit_or_reject`] (which compares the candidate against it).
    ///
    /// O(resident) linear scan, matching S3-FIFO's PR-3a scan; the eventual
    /// intrusive-frequency layout removes it (a #8 follow-up).
    fn lowest_frequency_resident_index(&self) -> Option<usize> {
        if self.resident.is_empty() {
            return None;
        }
        let mut best_idx = 0usize;
        let mut best_est = COUNTER_MAX;
        for (i, e) in self.resident.iter().enumerate() {
            let est = self.sketch.estimate(e.db, &e.key);
            if est < best_est {
                best_est = est;
                best_idx = i;
                if best_est == 0 {
                    break; // cannot go lower than zero; take the first coldest.
                }
            }
        }
        Some(best_idx)
    }

    /// Pop the resident key with the LOWEST estimated frequency (FIFO order breaks
    /// ties: the EARLIEST-inserted lowest-frequency key wins, which is the recency
    /// fallback). The post-door frequency-ordered fallback choice. Returns `None` if
    /// `resident` is empty.
    fn pop_lowest_frequency_resident(&mut self) -> Option<Entry> {
        let idx = self.lowest_frequency_resident_index()?;
        self.resident.remove(idx)
    }

    /// The candidate-admission DOOR (WTINYLFU.md "Decision-path contract" + "Tie-break").
    ///
    /// Compares the just-inserted `cand` against the COLDEST resident (the would-be
    /// victim) and returns the key to evict. The strict-win / incumbent-wins-ties rule:
    ///
    /// - candidate IS the coldest resident (same index) -> self-evict it (one key,
    ///   trivially the coldest; no comparison needed).
    /// - otherwise: min-increment the VICTIM (the decision-path bump for the resident
    ///   being evaluated), then admit the candidate ONLY on a STRICT win
    ///   (`estimate(cand) > estimate(victim)`), in which case evict the VICTIM. On a tie
    ///   or a colder candidate, REJECT the candidate: it is itself returned as the
    ///   victim (stored-then-evicted), so the incumbent keeps its slot.
    ///
    /// Edge cases preserve `evict_to_fit`'s guaranteed progress (every path returns a
    /// LIVE key, or `None` only when nothing at all is evictable):
    ///
    /// - the candidate was removed between insert and here (not tracked) -> evict the
    ///   would-be victim instead;
    /// - no resident victim at all -> evict the candidate if it is still resident, else
    ///   pop the lowest-priority re-offer FIFO.
    fn admit_or_reject(&mut self, cand: &Entry) -> Option<(u32, Box<[u8]>)> {
        let cand_idx = self
            .resident
            .iter()
            .position(|e| e.matches(cand.db, &cand.key));
        match self.lowest_frequency_resident_index() {
            None => {
                // No resident victim. Evict the candidate itself if still tracked,
                // otherwise fall back to the re-offer FIFO so the loop still progresses.
                if let Some(i) = cand_idx {
                    let e = self.resident.remove(i)?;
                    return Some((e.db, e.key));
                }
                self.reoffer.pop_front().map(|e| (e.db, e.key))
            }
            Some(victim_idx) => {
                // The candidate is the coldest resident: trivially self-evict it.
                if cand_idx == Some(victim_idx) {
                    let e = self.resident.remove(victim_idx)?;
                    return Some((e.db, e.key));
                }
                // Decision-path bump of the VICTIM (the resident being evaluated), then
                // read both estimates for the strict-win comparison.
                let victim = &self.resident[victim_idx];
                let (vdb, vkey) = (victim.db, victim.key.clone());
                self.sketch.increment(vdb, &vkey);
                let victim_est = self.sketch.estimate(vdb, &vkey);
                let cand_est = self.sketch.estimate(cand.db, &cand.key);
                if cand_idx.is_none() {
                    // The candidate vanished (deleted) before the door ran: there is no
                    // candidate to admit or self-evict, so evict the would-be victim.
                    let e = self.resident.remove(victim_idx)?;
                    return Some((e.db, e.key));
                }
                if cand_est > victim_est {
                    // STRICT win: admit the candidate by evicting the victim.
                    let e = self.resident.remove(victim_idx)?;
                    Some((e.db, e.key))
                } else {
                    // Tie or candidate colder: REJECT. The just-inserted candidate is
                    // itself the victim (stored-then-evicted); the incumbent wins.
                    let i = cand_idx?;
                    let e = self.resident.remove(i)?;
                    Some((e.db, e.key))
                }
            }
        }
    }
}

impl EvictionHook for WTinyLfu {
    fn on_access(&mut self, _db: u32, _key: &[u8]) {
        // DECISION-PATH CONTRACT (WTINYLFU.md:135-136, #57): the GET read path is a
        // NO-OP. The sketch is min-incremented ONLY on the decision path (the candidate
        // in `on_insert`, the victim in `admit_or_reject`), NEVER per read. This is the
        // headline "no per-read sketch mutation" lint: the read path stays the
        // FIFO-class core's in-place metadata write with no list relink. Deliberately
        // empty, not a TODO.
    }

    fn on_insert(&mut self, db: u32, key: &[u8], _bytes: usize) {
        // Decision-path bump: the candidate is "seen at the door". One min-increment of
        // the just-inserted key, and record it as the pending admission candidate that
        // `select_victim` runs through the door ONE-SHOT.
        self.sketch.increment(db, key);
        self.candidate = Some(Entry {
            db,
            key: key.to_vec().into_boxed_slice(),
        });
        // Track residency. A replace of an already-tracked key stays resident (no
        // duplicate); a fresh key is pushed to the recency FIFO.
        if !self.tracks(db, key) {
            self.resident.push_back(Entry {
                db,
                key: key.to_vec().into_boxed_slice(),
            });
        }
    }

    fn on_remove(&mut self, db: u32, key: &[u8], _bytes: usize) {
        // An external delete / replace / expiry: drop it so a stale entry is never
        // offered as a victim. (The sketch frequency is intentionally NOT cleared: a
        // returning key keeps its earned frequency until aging decays it, the
        // ghost-like memory that gives scan resistance across a delete+reinsert.)
        self.remove_entry(db, key);
        // If the pending candidate was just removed, clear it so the door never runs on
        // a stale candidate.
        if let Some(c) = &self.candidate {
            if c.matches(db, key) {
                self.candidate = None;
            }
        }
    }

    fn select_victim(&mut self) -> Option<(u32, Box<[u8]>)> {
        // ONE-SHOT candidate-admission door: on the FIRST select_victim of an
        // evict_to_fit run, consume the pending candidate (the just-inserted key) and
        // run it through the door (admit on a strict win by evicting the victim, else
        // reject by self-evicting the candidate; see `admit_or_reject`). The candidate
        // is `take`n, so subsequent select_victim calls in the same loop fall through to
        // the plain frequency-ordered choice below.
        if let Some(cand) = self.candidate.take() {
            return self.admit_or_reject(&cand);
        }
        // Fall-through frequency-ordered victim choice: evict the lowest-estimated-
        // frequency resident key. A returned victim is popped OUT of the resident FIFO;
        // the store may SKIP deleting it (a volatile-* policy skips a non-TTL victim)
        // and then call `re_register` to put it back as a candidate (#46). When
        // `resident` is exhausted, drain the lowest-priority re-offer FIFO so a fresh
        // resident candidate is always offered before a re-registered key cycles again.
        if let Some(e) = self.pop_lowest_frequency_resident() {
            return Some((e.db, e.key));
        }
        if let Some(e) = self.reoffer.pop_front() {
            return Some((e.db, e.key));
        }
        None
    }
}

impl EvictionPolicy for WTinyLfu {
    fn policy_name(&self) -> String {
        // The CONFIGURED name, returned VERBATIM (e.g. allkeys-lfu, volatile-lfu).
        self.name.clone()
    }

    fn evicts(&self) -> bool {
        true
    }

    fn volatile_only(&self) -> bool {
        self.volatile_only
    }

    fn access_freq(&self, db: u32, key: &[u8]) -> Option<u8> {
        // The LFU engine: OBJECT FREQ reports the 4-bit count-min sketch estimate
        // (0..=15), the logarithmic access-frequency counter Redis's LFU exposes. This
        // is the only policy that returns Some, so OBJECT FREQ succeeds exactly under an
        // *-lfu maxmemory policy and errors otherwise (the LFU gate).
        Some(self.sketch.estimate(db, key))
    }

    fn re_register(&mut self, db: u32, key: &[u8]) {
        // The volatile-* re-eligibility fix (#46), SAME contract as S3-FIFO: a victim
        // the store declined to delete (a non-TTL key under volatile-*) is put BACK so
        // it stays an eviction candidate; a later EXPIRE that attaches a TTL makes it
        // eligible. Re-queued to the dedicated LOWEST-PRIORITY re-offer FIFO (NOT
        // resident) so a fresh resident candidate is always reached first and the
        // store's distinct-set bound terminates the scan with no false -OOM. Idempotent:
        // do not duplicate an already-tracked key.
        if self.tracks(db, key) {
            return;
        }
        self.reoffer.push_back(Entry {
            db,
            key: key.to_vec().into_boxed_slice(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ins(p: &mut WTinyLfu, key: &[u8]) {
        p.on_insert(0, key, key.len());
    }
    fn acc(p: &mut WTinyLfu, key: &[u8]) {
        p.on_access(0, key);
    }
    fn victim_key(p: &mut WTinyLfu) -> Option<Vec<u8>> {
        p.select_victim().map(|(_, k)| k.into_vec())
    }
    /// Raise a key's sketch estimate by `n` DECISION-PATH increments, the genuine
    /// frequency dimension under #57 (on_access is a no-op, so a GET loop cannot do
    /// this). Drives the sketch directly the way the candidate/victim decision path
    /// does, leaving the residency FIFO untouched.
    fn bump(p: &mut WTinyLfu, key: &[u8], n: usize) {
        for _ in 0..n {
            p.sketch.increment(0, key);
        }
    }

    #[test]
    fn sketch_estimate_rises_with_accesses() {
        let mut s = CmSketch::new();
        assert_eq!(s.estimate(0, b"k"), 0, "unseen key estimates zero");
        for i in 1..=5u8 {
            s.increment(0, b"k");
            assert_eq!(s.estimate(0, b"k"), i, "estimate tracks the access count");
        }
    }

    #[test]
    fn sketch_saturates_at_counter_max() {
        let mut s = CmSketch::new();
        for _ in 0..100 {
            s.increment(0, b"hot");
        }
        assert_eq!(
            s.estimate(0, b"hot"),
            COUNTER_MAX,
            "a 4-bit counter saturates at 15"
        );
    }

    #[test]
    fn sketch_min_increment_only_bumps_the_minimum_cells() {
        // Min-increment: a key sharing one of its cells with a much hotter key should
        // still track its OWN access count (the shared cell is already high, so the
        // min-increment bumps the key's other, lower cells). We assert the estimate of a
        // freshly-incremented key equals its own increment count, not the hot neighbor's.
        let mut s = CmSketch::new();
        for _ in 0..15 {
            s.increment(0, b"hot");
        }
        // A different key incremented 3 times reads ~3 (its own count), NOT 15, because
        // estimate is the MIN across rows and min-increment did not inflate its cells.
        for _ in 0..3 {
            s.increment(0, b"cool");
        }
        assert_eq!(
            s.estimate(0, b"cool"),
            3,
            "min-increment keeps a cool key's estimate at its own count"
        );
    }

    #[test]
    fn sketch_halving_decays_old_frequencies_and_caps_counters() {
        let mut s = CmSketch::new();
        for _ in 0..15 {
            s.increment(0, b"k");
        }
        assert_eq!(s.estimate(0, b"k"), 15);
        s.halve();
        assert_eq!(s.estimate(0, b"k"), 7, "halving right-shifts the counter");
        s.halve();
        assert_eq!(s.estimate(0, b"k"), 3);
    }

    #[test]
    fn sketch_periodic_aging_fires_at_the_sample_size() {
        let mut s = CmSketch::new();
        assert_eq!(s.agings(), 0);
        // Drive enough increments to cross AGING_SAMPLE_SIZE at least once.
        for i in 0..AGING_SAMPLE_SIZE {
            s.increment(0, format!("k{i}").as_bytes());
        }
        assert!(
            s.agings() >= 1,
            "periodic halving fires once the sample size is reached"
        );
    }

    #[test]
    fn sketch_hashing_is_deterministic_across_instances() {
        // ADR-0003: two fresh sketches make identical estimates for the same access
        // sequence (fixed seeds, no RandomState).
        let mut a = CmSketch::new();
        let mut b = CmSketch::new();
        for _ in 0..7 {
            a.increment(3, b"key");
            b.increment(3, b"key");
        }
        assert_eq!(a.estimate(3, b"key"), b.estimate(3, b"key"));
        assert_eq!(a.estimate(0, b"other"), b.estimate(0, b"other"));
    }

    #[test]
    fn on_access_does_not_mutate_the_sketch() {
        // THE headline #57 acceptance lint (WTINYLFU.md:135-136): the GET read path is a
        // NO-OP. Call on_access many times on a tracked key and assert its sketch
        // estimate is UNCHANGED (zero, since on_insert is not called here). Frequency is
        // built ONLY on the decision path, never per read.
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"k");
        // on_insert bumped the candidate once; read it, then prove on_access never moves
        // the estimate.
        let before = p.estimate(0, b"k");
        for _ in 0..1000 {
            acc(&mut p, b"k");
        }
        assert_eq!(
            p.estimate(0, b"k"),
            before,
            "on_access is a no-op: the read path must not mutate the sketch (#57)"
        );

        // And on a never-inserted key, on_access leaves it at the unseen-zero estimate.
        let mut q = WTinyLfu::new(false);
        for _ in 0..1000 {
            q.on_access(0, b"never");
        }
        assert_eq!(
            q.estimate(0, b"never"),
            0,
            "on_access must not create frequency for a read-only key"
        );
    }

    #[test]
    fn candidate_admission_door_strict_win_admits_candidate_evicts_victim() {
        // The door admits the candidate on a STRICT frequency win: it evicts the COLDER
        // incumbent victim, not the candidate. Make "hot" genuinely hotter than the
        // about-to-be-inserted candidate via decision-path bumps.
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"hot");
        bump(&mut p, b"hot", 5); // hot estimate ~6 (1 from on_insert + 5)
        // Insert the candidate "cand"; on_insert bumps it to ~1 and records it as the
        // pending candidate, then raise it ABOVE the incumbent so it strictly wins.
        ins(&mut p, b"cand");
        bump(&mut p, b"cand", 10); // cand now ~11, strictly hotter than hot
        // First select_victim consumes the candidate through the door: cand (~11) wins
        // strictly over the coldest resident "hot" (~6), so the VICTIM (hot) is evicted.
        assert_eq!(
            victim_key(&mut p),
            Some(b"hot".to_vec()),
            "a strict-win candidate is admitted: the colder incumbent is evicted"
        );
        assert!(
            p.tracks(0, b"cand"),
            "the admitted candidate stays resident"
        );
        assert!(!p.tracks(0, b"hot"), "the colder incumbent was evicted");
    }

    #[test]
    fn candidate_admission_door_rejects_a_colder_candidate_self_evicting_it() {
        // The door REJECTS a candidate colder than the coldest incumbent: the
        // just-inserted candidate is itself evicted (stored-then-evicted), the incumbent
        // keeps its slot. This is the scan-resistance semantics.
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"warm");
        bump(&mut p, b"warm", 8); // warm estimate ~9
        // Insert the cold candidate (estimate ~1 from on_insert only).
        ins(&mut p, b"cold");
        // The door: cold (~1) vs coldest resident "warm" (~9). cold does NOT strictly
        // win, so it is REJECTED and self-evicted.
        assert_eq!(
            victim_key(&mut p),
            Some(b"cold".to_vec()),
            "a colder candidate is rejected: it self-evicts"
        );
        assert!(p.tracks(0, b"warm"), "the warmer incumbent survives");
        assert!(!p.tracks(0, b"cold"), "the rejected candidate was evicted");
    }

    #[test]
    fn candidate_admission_door_tie_break_incumbent_wins() {
        // On a frequency TIE the incumbent wins (admit only on a STRICT win,
        // WTINYLFU.md "Tie-break"): the candidate is rejected and self-evicts.
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"inc");
        bump(&mut p, b"inc", 4); // inc estimate ~5
        // Insert a candidate and bump it to EXACTLY the incumbent's estimate (a tie). The
        // door min-increments the victim once when it evaluates it, so match THAT: after
        // the victim bump "inc" reads ~6, so bring the candidate to ~6 too for the tie.
        ins(&mut p, b"tie"); // ~1 from on_insert
        bump(&mut p, b"tie", 5); // tie now ~6, equal to inc after its decision-path bump
        assert_eq!(
            victim_key(&mut p),
            Some(b"tie".to_vec()),
            "a tie rejects the candidate (incumbent wins ties)"
        );
        assert!(p.tracks(0, b"inc"), "the incumbent wins the tie");
        assert!(!p.tracks(0, b"tie"), "the tied candidate self-evicts");
    }

    #[test]
    fn scan_flood_does_not_evict_the_hot_key_via_the_door() {
        // A flood of distinct COLD one-hit candidates must not displace an established
        // hot resident: each cold candidate loses the admission door (estimate ~1 vs the
        // hot incumbent's high estimate) and self-evicts, so the hot key survives. This
        // is the cache-pollution resistance W-TinyLFU exists for, via the door.
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"hot");
        bump(&mut p, b"hot", 14); // hot saturates near the cap
        // Each cold candidate is inserted (becoming the pending candidate), then the door
        // runs: cold (~1) loses to the hot incumbent, so the COLD candidate self-evicts.
        for i in 0..50u32 {
            ins(&mut p, format!("scan{i}").as_bytes());
            let v = victim_key(&mut p).expect("a cold candidate self-evicts at the door");
            assert_eq!(
                v,
                format!("scan{i}").into_bytes(),
                "the cold candidate loses the door and self-evicts, sparing the hot key"
            );
            assert_ne!(v, b"hot".to_vec(), "the hot key is never chosen");
        }
        assert!(
            p.tracks(0, b"hot"),
            "the hot key survives the whole cold-candidate flood"
        );
    }

    #[test]
    fn frequently_evaluated_key_survives_one_hit_key_is_evicted() {
        // The HEADLINE scan-resistance property restated for the decision-path model: a
        // key that has accrued frequency on the decision path survives while a one-hit
        // key is evicted. Insert both, raise "hot" via decision-path bumps, then evicting
        // (after the candidate is consumed) targets the cold one-hit key.
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"hot");
        ins(&mut p, b"scan"); // scan is the LAST insert => the pending candidate
        bump(&mut p, b"hot", 10); // hot ~11
        // First select_victim consumes the candidate "scan" (~1) vs coldest resident.
        // The coldest resident IS "scan" itself, so it self-evicts (trivial coldest).
        assert_eq!(victim_key(&mut p), Some(b"scan".to_vec()));
        assert!(p.tracks(0, b"hot"), "the high-frequency key survives");
        assert!(!p.tracks(0, b"scan"), "the one-hit key is evicted");
    }

    #[test]
    fn candidate_is_consumed_after_the_first_decision() {
        // The candidate is ONE-SHOT: it drives only the FIRST select_victim of an
        // evict_to_fit run; later calls fall through to the plain frequency-ordered
        // choice. Insert several keys, then two select_victim calls: the first runs the
        // door on the last-inserted candidate, the second is a plain coldest-resident
        // choice (no candidate left).
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"a");
        ins(&mut p, b"b");
        ins(&mut p, b"c"); // c is the pending candidate (last insert)
        bump(&mut p, b"a", 5); // a is warm; b and c are cold (~1)
        // First decision: candidate "c" vs coldest resident. b and c are both ~1; the
        // coldest-resident index is "b" (earlier FIFO). c does not strictly beat b, so c
        // self-evicts (rejected).
        let first = victim_key(&mut p).expect("first victim via the door");
        assert_eq!(first, b"c".to_vec(), "the candidate is consumed first");
        // Candidate now cleared: the second call is a plain coldest-resident choice.
        let second = victim_key(&mut p).expect("second victim, no candidate");
        assert_eq!(
            second,
            b"b".to_vec(),
            "plain coldest resident after the door"
        );
        assert!(p.tracks(0, b"a"), "the warm key survives both decisions");
    }

    #[test]
    fn candidate_that_is_the_coldest_resident_self_evicts() {
        // When the just-inserted candidate IS the coldest resident, the door trivially
        // self-evicts it (one key, no comparison): a brand-new cold key inserted into a
        // set of warmer residents loses immediately.
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"warm1");
        ins(&mut p, b"warm2");
        bump(&mut p, b"warm1", 6);
        bump(&mut p, b"warm2", 6);
        ins(&mut p, b"newcold"); // the candidate, estimate ~1, the coldest resident
        assert_eq!(
            victim_key(&mut p),
            Some(b"newcold".to_vec()),
            "the candidate is the coldest resident, so it self-evicts"
        );
        assert!(p.tracks(0, b"warm1"));
        assert!(p.tracks(0, b"warm2"));
    }

    #[test]
    fn candidate_alone_self_evicts_when_no_other_resident() {
        // Edge case: the candidate is the ONLY resident. The door self-evicts it (there
        // is no other victim), preserving evict_to_fit's progress guarantee.
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"solo");
        assert_eq!(
            victim_key(&mut p),
            Some(b"solo".to_vec()),
            "the sole resident candidate self-evicts"
        );
        assert!(!p.tracks(0, b"solo"));
        assert_eq!(p.select_victim(), None, "nothing left to evict");
    }

    #[test]
    fn candidate_removed_before_the_door_evicts_the_would_be_victim() {
        // Edge case: the candidate is deleted (on_remove) between insert and the door.
        // on_remove clears the stale candidate, so select_victim falls through to the
        // plain coldest-resident choice (no door run).
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"hot");
        bump(&mut p, b"hot", 8);
        ins(&mut p, b"cand"); // pending candidate
        p.on_remove(0, b"cand", 1); // candidate deleted before the door
        // The candidate was cleared, so select_victim is a plain coldest-resident choice;
        // "hot" is the only resident left.
        assert_eq!(
            victim_key(&mut p),
            Some(b"hot".to_vec()),
            "a removed candidate clears; the door does not run on a stale key"
        );
    }

    #[test]
    fn empty_policy_yields_no_victim() {
        let mut p = WTinyLfu::new(false);
        assert_eq!(p.select_victim(), None);
    }

    #[test]
    fn on_remove_drops_a_tracked_key_so_it_is_not_returned() {
        let mut p = WTinyLfu::new(false);
        ins(&mut p, b"x");
        ins(&mut p, b"y");
        p.on_remove(0, b"x", 1);
        // x removed; the only victim now is y.
        assert_eq!(victim_key(&mut p), Some(b"y".to_vec()));
        assert_eq!(p.select_victim(), None);
    }

    #[test]
    fn volatile_flag_drives_name_and_posture() {
        let all = WTinyLfu::new(false);
        assert_eq!(all.policy_name(), "allkeys-lfu");
        assert_eq!(all.engine_family(), "allkeys-lfu");
        assert!(!all.volatile_only());
        let vol = WTinyLfu::new(true);
        assert_eq!(vol.policy_name(), "volatile-lfu");
        assert_eq!(vol.engine_family(), "volatile-lfu");
        assert!(vol.volatile_only());
        assert!(vol.evicts());
    }

    #[test]
    fn configured_name_round_trips_verbatim() {
        let lfu = WTinyLfu::with_name(false, "allkeys-lfu");
        assert_eq!(lfu.policy_name(), "allkeys-lfu");
        let vol = WTinyLfu::with_name(true, "volatile-lfu");
        assert_eq!(vol.policy_name(), "volatile-lfu");
    }

    #[test]
    fn re_register_keeps_a_skipped_victim_trackable() {
        // The #46 re-eligibility fix, SAME contract as S3-FIFO: a victim the store
        // declines to delete is RE-REGISTERED, so the policy keeps offering it (instead
        // of dropping it, which would make it un-evictable forever).
        let mut p = WTinyLfu::new(true);
        ins(&mut p, b"x");
        let v = p.select_victim().expect("x is offered as a victim");
        assert_eq!(v.1.as_ref(), b"x");
        assert!(!p.tracks(0, b"x"), "select_victim pops the candidate out");
        p.re_register(0, b"x");
        assert!(
            p.tracks(0, b"x"),
            "re_register keeps the key trackable (#46)"
        );
        assert_eq!(
            p.select_victim().map(|(_, k)| k.into_vec()),
            Some(b"x".to_vec())
        );
        // Idempotent: re-registering a still-tracked key does not duplicate.
        p.re_register(0, b"x"); // x was just popped; re-adds once
        p.re_register(0, b"x"); // already present; no-op
        let count = p
            .resident
            .iter()
            .chain(p.reoffer.iter())
            .filter(|e| e.matches(0, b"x"))
            .count();
        assert_eq!(count, 1, "re_register must not duplicate a tracked key");
    }

    #[test]
    fn re_offer_is_drained_only_after_resident() {
        // A re-registered key sits in the lowest-priority re-offer FIFO: a fresh resident
        // candidate is always offered BEFORE it cycles again (the #46 starvation fix).
        let mut p = WTinyLfu::new(true);
        ins(&mut p, b"a");
        // Pull "a" and re-register it (store declined: non-TTL under volatile-*).
        let v = p.select_victim().unwrap();
        assert_eq!(v.1.as_ref(), b"a");
        p.re_register(0, b"a");
        // Now a fresh resident "b" arrives: it must be offered before "a" cycles again.
        ins(&mut p, b"b");
        assert_eq!(victim_key(&mut p), Some(b"b".to_vec()), "resident first");
        assert_eq!(victim_key(&mut p), Some(b"a".to_vec()), "re-offer last");
    }

    #[test]
    fn determinism_identical_victim_choices_on_replay() {
        // ADR-0003: the same insert/decision sequence yields the IDENTICAL victim
        // sequence on a fresh replay (fixed-seed sketch hashing, deterministic FIFO
        // tie-break, deterministic door). Review fix (A): the old version used an
        // on_access/acc() loop to "make a subset hot", which is DEAD CODE under #57
        // (on_access is a no-op). This builds a GENUINE frequency dimension via bump()
        // (decision-path increments) so a deterministic subset really gets a higher
        // estimate, and asserts the hot subset is evicted LATER than the cold one.
        let run = || -> Vec<Vec<u8>> {
            let mut p = WTinyLfu::new(false);
            for i in 0..40u32 {
                ins(&mut p, format!("k{i}").as_bytes());
            }
            // Genuine frequency: every 3rd key is made hot with a deterministic, increasing
            // number of decision-path increments. This is a REAL estimate difference, not
            // an inert read loop.
            for i in (0..40u32).step_by(3) {
                bump(&mut p, format!("k{i}").as_bytes(), 5 + (i as usize % 7));
            }
            // Drain every key. The first select_victim consumes the pending candidate (the
            // last inserted "k39", a cold key), then the rest are plain coldest-first
            // choices, so the cold keys go before the hot subset.
            let mut out = Vec::new();
            while let Some((db, k)) = p.select_victim() {
                p.on_remove(db, &k, 0);
                out.push(k.into_vec());
            }
            out
        };
        let a = run();
        let b = run();
        assert_eq!(a, b, "identical sequence => identical victim order");
        assert_eq!(a.len(), 40, "every key is eventually evicted");

        // The frequency dimension is REAL and ordered: the hot subset (every 3rd key)
        // must be evicted LATER than the cold majority. Compute the average eviction
        // position of hot vs cold and assert hot evicts strictly later. This is what makes
        // the test exercise frequency-differentiated ordering, not just FIFO.
        let pos = |key: &str| a.iter().position(|k| k == key.as_bytes()).unwrap();
        let mut hot_positions = Vec::new();
        let mut cold_positions = Vec::new();
        for i in 0..40u32 {
            let p = pos(&format!("k{i}"));
            if i % 3 == 0 {
                hot_positions.push(p);
            } else {
                cold_positions.push(p);
            }
        }
        let avg = |v: &[usize]| v.iter().sum::<usize>() as f64 / v.len() as f64;
        assert!(
            avg(&hot_positions) > avg(&cold_positions),
            "the higher-frequency subset is evicted later (frequency-ordered, not FIFO): \
             hot avg {} vs cold avg {}",
            avg(&hot_positions),
            avg(&cold_positions),
        );
    }
}
