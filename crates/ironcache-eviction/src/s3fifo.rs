// SPDX-License-Identifier: MIT OR Apache-2.0
//! S3-FIFO: the default cache-mode eviction engine (ADR-0008, EVICTION.md #48).
//!
//! S3-FIFO partitions a shard's cache into a SMALL probationary FIFO (about 10%) and
//! a large MAIN FIFO (about 90%), with a GHOST queue of recently-evicted key
//! FINGERPRINTS (not values) [s3fifo-small-main-split]. Most objects are one-hit
//! wonders [s3fifo-onehit-wonder-72pct], so a fresh key enters the small queue and is
//! evicted cheaply unless it is reused; a key seen again is PROMOTED to main. A key
//! whose fingerprint is still in the ghost when it is re-inserted is admitted
//! straight to main (it was popular enough to come back). A 2-bit frequency counter
//! capped at 3 [s3fifo-freq-counter-2bit-cap3] drives the promote/second-chance
//! decision.
//!
//! ## 2-bit frequency: policy-side counter (PR-3a)
//!
//! The frequency lives ON each tracked key (the slab [`Slot::freq`]), bumped in
//! [`EvictionHook::on_access`], and this is the SINGLE source of truth. The crate docs
//! explain why it is the policy-side counter rather than the kvobj `eviction_rank`:
//! `select_victim` is policy-only and cannot borrow the kvobj header. The kvobj
//! `eviction_rank` field is RESERVED for a later single-source migration and is NOT
//! written on the store's access path today. The counter is bounded (one per tracked
//! key, dropped when the key leaves all queues) and capped at 3, so it is the 2-bit
//! field S3-FIFO needs.
//!
//! ## O(1) layout: generational slab + key->slot index + FIFO queues of handles
//!
//! The hot path ([`EvictionHook::on_access`] -> [`S3Fifo::bump_freq`]) is a SINGLE
//! point lookup, NOT a scan. The state is three pieces (the #8 follow-up the PR-3a
//! comment promised; PR-3a held the keys directly in the three `VecDeque`s and SCANNED
//! them on every access, an O(N) cost the load profile flagged as the #1 compute hot
//! frame):
//!
//! - A SLAB ([`Slot`]) owns each live key's data ONCE: a `Vec<Slot>` plus a freelist of
//!   free indices. A freed slot bumps its `gen` (the generation counter) so any stale
//!   handle that still names it is detected by a `gen` mismatch. Allocating reuses a
//!   freelist slot or pushes a new one.
//! - A [`Handle`] is `{ idx, generation }` (8 bytes, `Copy`). The three FIFO queues are
//!   `VecDeque<Handle>` in FIFO order, preserving the exact small/main/reoffer ordering
//!   semantics PR-3a had.
//! - An INDEX (`hashbrown::HashTable<u32>` of slot indices) gives O(1) key->slot
//!   lookup. It uses the LOW-LEVEL explicit-hash API (`find`/`insert_unique`/
//!   `find_entry`): it stores ONLY the u32 slot index (the key bytes live in the slab,
//!   never duplicated), and is fed a FIXED, deterministic hash of `(db, key)` -- the
//!   same [`S3Fifo::fingerprint`] FNV-1a constant used for the ghost ring, so the index
//!   is reproducible run-to-run (ADR-0003: no `RandomState`, no OS entropy). The index
//!   is a POINT-LOOKUP structure ONLY; it is NEVER iterated for any client-visible
//!   order, so a fixed seed is sound (ADR-0003 forbids a randomly-seeded hasher only
//!   where iteration order would leak across calls -- this index has no such order).
//!
//! Each queue carries a LIVE count (`*_live`), and the policy a total `live`, so
//! `entry_count`/`small_cap`/the `draw_small` heuristic compute on LIVE keys EXACTLY as
//! PR-3a did -- a stale handle still sitting in a `VecDeque` (left there when its key was
//! removed via `on_remove`) does NOT count. A stale handle is discarded lazily when
//! `select_victim` pops it (gen mismatch), so removal stays O(1) (no scan to splice it
//! out).
//!
//! ## Byte-budget-driven, not count-driven
//!
//! The eviction TRIGGER is the store's byte budget (`evict_to_fit`), not a queue
//! capacity: the store calls [`EvictionHook::select_victim`] repeatedly until it is
//! under budget. The `small_cap`/`ghost_cap` bounds here size the small-queue
//! admission split and the ghost ring (a fraction of the running entry count), not a
//! hard eviction threshold. `select_victim` GUARANTEES PROGRESS: even an all-hot
//! keyspace yields a victim within a bounded number of promotion rounds, so the
//! store's evict-to-fit loop cannot spin.

use std::collections::VecDeque;

use hashbrown::HashTable;
use ironcache_storage::EvictionHook;

use crate::EvictionPolicy;

/// The S3-FIFO 2-bit frequency cap (ADR-0008 `s3fifo-freq-counter-2bit-cap3`).
const MAX_FREQ: u8 = 3;

/// The small (probationary) queue's share of the running entry count. S3-FIFO uses
/// about 10% small / 90% main [s3fifo-small-main-split].
const SMALL_FRACTION_NUM: usize = 1;
const SMALL_FRACTION_DEN: usize = 10;

/// The ghost ring's share of the running entry count (a fraction of main capacity;
/// EVICTION.md "sized as a fraction of the main capacity"). Kept modest and always at
/// least a small floor so a tiny keyspace still re-admits a returning key.
const GHOST_FRACTION_NUM: usize = 9;
const GHOST_FRACTION_DEN: usize = 10;
const GHOST_FLOOR: usize = 8;

/// Which FIFO queue a live slot's handle currently sits in. Mirrors the queue the
/// handle was last pushed to, so a removal can decrement the right per-queue live count
/// without scanning, and `select_victim` can keep `Slot::queue` in lockstep on a
/// promotion / second-chance requeue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueTag {
    Small,
    Main,
    Reoffer,
}

/// A slab slot owning one tracked key's data ONCE (no key duplication anywhere else;
/// the index holds only this slot's `u32` position). A free slot is identified by a
/// `gen` bump relative to the handles that still name it.
#[derive(Debug, Clone)]
struct Slot {
    /// The logical database id of the tracked key.
    db: u32,
    /// The tracked key bytes (the SINGLE owned copy).
    key: Box<[u8]>,
    /// The 2-bit access frequency (capped at [`MAX_FREQ`]).
    freq: u8,
    /// Which FIFO queue this slot's handle currently sits in (kept in lockstep with the
    /// queue moves so a removal decrements the correct per-queue live count).
    queue: QueueTag,
    /// The generation counter: bumped each time the slot is freed, so a stale [`Handle`]
    /// left in a `VecDeque` is detected by a generation mismatch (a tombstone to discard).
    /// (Named `generation`, not `gen`, since `gen` is a reserved keyword in edition 2024.)
    generation: u32,
    /// Whether this slot is currently LIVE (occupied). A freed slot stays in the `Vec`
    /// (its index recycles via the freelist) but is marked dead so a lookup that races a
    /// recycled `gen` cannot resolve a tombstone as live.
    live: bool,
}

impl Slot {
    /// Whether this slot holds the live key `(db, key)`.
    fn matches(&self, db: u32, key: &[u8]) -> bool {
        self.live && self.db == db && self.key.as_ref() == key
    }
}

/// A FIFO-queue handle naming a slab slot (`Copy`, 8 bytes). It is STABLE: removing the
/// key frees the slot and bumps its `gen`, so the handle becomes a tombstone detected on
/// pop rather than being spliced out (O(1) removal, no scan).
#[derive(Debug, Clone, Copy)]
struct Handle {
    idx: u32,
    generation: u32,
}

/// The S3-FIFO policy state (per shard, unsynchronized; ADR-0005).
#[derive(Debug, Clone)]
pub struct S3Fifo {
    /// The slab owning each live key's data once (indexed by `Handle::idx`).
    slots: Vec<Slot>,
    /// The freelist of recyclable slot indices (a freed slot's index lands here and is
    /// reused on the next allocation, after a `gen` bump).
    free: Vec<u32>,
    /// The O(1) key->slot index: `hashbrown::HashTable<u32>` of LIVE slot indices, keyed
    /// by the deterministic [`Self::fingerprint`] of `(db, key)` (fixed seed, ADR-0003).
    /// Stores ONLY the index (the key lives in the slab). Point lookups only, never
    /// iterated for any client-visible order.
    index: HashTable<u32>,
    /// The small probationary FIFO (fresh keys land here unless ghost-readmitted). Holds
    /// handles in FIFO order; a stale handle (its slot freed) is discarded on pop.
    small: VecDeque<Handle>,
    /// The large main FIFO (promoted / ghost-readmitted keys).
    main: VecDeque<Handle>,
    /// The LOWEST-priority re-offer FIFO for the volatile-* re-eligibility fix (#46):
    /// a non-TTL victim the store declines to delete is re-registered HERE rather than
    /// back into small/main. `select_victim` drains this queue ONLY after small and main
    /// are exhausted, so every fresh small/main candidate (including an eligible TTL
    /// victim parked anywhere) is offered BEFORE a re-registered key cycles again. This
    /// is what stops the #46 false `-OOM`: feeding skipped keys back into small (the old
    /// behavior) kept small over its ~10% target and STARVED main, so a main-resident
    /// TTL victim was never reached; the dedicated lowest-priority queue removes that
    /// starvation in either direction. The store's distinct-key skip set then terminates
    /// the scan once every live key (now all sitting here) has been offered with no
    /// eligible TTL victim.
    reoffer: VecDeque<Handle>,
    /// The count of LIVE handles in the small queue (excludes stale tombstones still
    /// sitting in `small`). `small_cap`/`draw_small` read this, NOT `small.len()`.
    small_live: usize,
    /// The count of LIVE handles in the main queue.
    main_live: usize,
    /// The count of LIVE handles in the re-offer queue.
    reoffer_live: usize,
    /// The total count of live slots, so `entry_count` is O(1) and EXACT (==
    /// `small_live + main_live + reoffer_live` == the number of live slab slots).
    live: usize,
    /// The ghost ring of recently-evicted key fingerprints (FIFO, bounded).
    ghost: VecDeque<u64>,
    /// Whether victims are restricted to TTL-bearing keys (the volatile-* family),
    /// enforced by the store in `evict_to_fit`.
    volatile_only: bool,
    /// The CONFIGURED `maxmemory-policy` name this policy echoes VERBATIM from
    /// `policy_name()` (CONFIG GET / INFO). `map_policy_name` plants the exact
    /// configured spelling here (e.g. `allkeys-lfu`, `volatile-ttl`); `new` defaults
    /// it to the family name. The ENGINE is always S3-FIFO ([`Self::engine_family`]);
    /// the NAME round-trips unchanged (ADR-0009).
    name: String,
}

impl S3Fifo {
    /// A fresh S3-FIFO policy. `volatile_only` selects the `volatile-*` restriction;
    /// the configured policy name defaults to the family name
    /// (`allkeys-lru`/`volatile-lru`). Use [`Self::with_name`] (via `map_policy_name`)
    /// to carry a specific configured spelling (e.g. `allkeys-lfu`, `volatile-ttl`).
    #[must_use]
    pub fn new(volatile_only: bool) -> Self {
        let name = if volatile_only {
            "volatile-lru"
        } else {
            "allkeys-lru"
        };
        S3Fifo::with_name(volatile_only, name)
    }

    /// A fresh S3-FIFO policy carrying the exact CONFIGURED policy name, returned
    /// verbatim by [`EvictionPolicy::policy_name`]. `map_policy_name` uses this so
    /// CONFIG GET / INFO round-trip the configured enum string (ADR-0009).
    #[must_use]
    pub fn with_name(volatile_only: bool, name: &str) -> Self {
        S3Fifo {
            slots: Vec::new(),
            free: Vec::new(),
            index: HashTable::new(),
            small: VecDeque::new(),
            main: VecDeque::new(),
            reoffer: VecDeque::new(),
            small_live: 0,
            main_live: 0,
            reoffer_live: 0,
            live: 0,
            ghost: VecDeque::new(),
            volatile_only,
            name: name.to_owned(),
        }
    }

    /// The internal eviction ENGINE family label (always S3-FIFO here). This is the
    /// engine identity, SEPARATE from the configured Redis name [`Self::policy_name`]
    /// returns verbatim: the engine serves the configured name with a documented
    /// victim-ordering divergence for the `*-lfu`/`volatile-ttl` spellings (ADR-0009).
    #[must_use]
    pub fn engine_family(&self) -> &'static str {
        if self.volatile_only {
            "volatile-lru"
        } else {
            "allkeys-lru"
        }
    }

    /// A stable, deterministic fingerprint of a `(db, key)` for the ghost ring AND the
    /// key->slot index hash. This is NOT cryptographic and NOT the store's hash; it is a
    /// fixed-constant FNV-1a over `db` then the key bytes, so it is identical on every
    /// run (ADR-0003: no OS entropy, no `RandomState`). For the ghost ring a collision
    /// only costs a spurious main-admission, which is harmless; for the index it only
    /// costs an extra `eq` probe of a colliding slot's `(db, key)` (the `eq` closure
    /// disambiguates exactly), so correctness does not depend on collision-freedom.
    fn fingerprint(db: u32, key: &[u8]) -> u64 {
        // FNV-1a 64-bit, fixed offset basis and prime (public-domain constants).
        let mut h: u64 = 0xCBF2_9CE4_8422_2325;
        for b in db.to_le_bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01B3);
        }
        for &b in key {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01B3);
        }
        h
    }

    /// The running total of tracked keys (small + main + the re-offer queue), counting
    /// LIVE keys only (stale handles excluded). All three queues hold LIVE keys the
    /// store still tracks, so they all count toward the cap sizing and the
    /// guaranteed-progress round bound. O(1) (a maintained counter, not a sum of
    /// `VecDeque::len()`, which would include stale tombstones).
    fn entry_count(&self) -> usize {
        self.live
    }

    /// O(1) key->slot lookup: the LIVE slot index for `(db, key)`, or `None`. Hashes
    /// `(db, key)` with the fixed [`Self::fingerprint`] and disambiguates collisions via
    /// the slot's [`Slot::matches`].
    fn lookup(&self, db: u32, key: &[u8]) -> Option<u32> {
        let fp = Self::fingerprint(db, key);
        self.index
            .find(fp, |&i| self.slots[i as usize].matches(db, key))
            .copied()
    }

    /// Whether `(db, key)` is tracked (in any of the three queues). O(1).
    fn tracks(&self, db: u32, key: &[u8]) -> bool {
        self.lookup(db, key).is_some()
    }

    /// The current small-queue target capacity (~10% of the running entry count, at
    /// least 1 so a fresh key always has a home). Computed on LIVE counts.
    fn small_cap(&self) -> usize {
        (self.entry_count() * SMALL_FRACTION_NUM / SMALL_FRACTION_DEN).max(1)
    }

    /// The current ghost-ring target capacity (a fraction of the running entry count,
    /// with a small floor).
    fn ghost_cap(&self) -> usize {
        (self.entry_count() * GHOST_FRACTION_NUM / GHOST_FRACTION_DEN).max(GHOST_FLOOR)
    }

    /// Whether `fp` is currently in the ghost ring.
    fn ghost_contains(&self, fp: u64) -> bool {
        self.ghost.contains(&fp)
    }

    /// Remove `fp` from the ghost ring (on a ghost re-admit).
    fn ghost_remove(&mut self, fp: u64) {
        if let Some(i) = self.ghost.iter().position(|g| *g == fp) {
            self.ghost.remove(i);
        }
    }

    /// Record `fp` as recently-evicted, trimming the ring to its capacity (FIFO).
    fn ghost_record(&mut self, fp: u64) {
        if self.ghost_contains(fp) {
            return;
        }
        self.ghost.push_back(fp);
        let cap = self.ghost_cap();
        while self.ghost.len() > cap {
            self.ghost.pop_front();
        }
    }

    /// Allocate a slab slot for a fresh `(db, key)` with `freq = 0` and the given queue
    /// tag, insert it into the index, bump the total live count, and return its handle.
    /// Reuses a freelist slot (bumping its `gen`) or pushes a new one. The CALLER pushes
    /// the returned handle to the matching queue and bumps that queue's live count.
    fn alloc_slot(&mut self, db: u32, key: &[u8], queue: QueueTag) -> Handle {
        let idx = if let Some(idx) = self.free.pop() {
            let slot = &mut self.slots[idx as usize];
            slot.db = db;
            slot.key = key.to_vec().into_boxed_slice();
            slot.freq = 0;
            slot.queue = queue;
            slot.generation = slot.generation.wrapping_add(1);
            slot.live = true;
            idx
        } else {
            let idx = u32::try_from(self.slots.len()).expect("slab index fits in u32");
            self.slots.push(Slot {
                db,
                key: key.to_vec().into_boxed_slice(),
                freq: 0,
                queue,
                generation: 0,
                live: true,
            });
            idx
        };
        let g = self.slots[idx as usize].generation;
        // Insert the index entry: the hash closure reads the (db, key) from the slot in
        // the slab (no key duplication in the index), so a future resize re-places by
        // re-hashing the slot's key.
        let fp = Self::fingerprint(db, key);
        self.index
            .insert_unique(fp, idx, |&i| Self::slot_hash(&self.slots, i));
        self.live += 1;
        Handle { idx, generation: g }
    }

    /// The index hash of a slot's `(db, key)` (the hasher closure passed to the
    /// explicit-hash table). Reads the key from the slab so the index stores no key.
    fn slot_hash(slots: &[Slot], idx: u32) -> u64 {
        let s = &slots[idx as usize];
        Self::fingerprint(s.db, &s.key)
    }

    /// Free the live slot at `idx`: remove it from the index, mark it dead, bump its
    /// `gen`, push its index to the freelist, and decrement the total live count. Does
    /// NOT touch the queues -- the handle that named this slot is left in its `VecDeque`
    /// as a STALE tombstone (discarded on pop), so freeing is O(1) with no queue scan.
    /// The CALLER decrements the relevant per-queue live count.
    fn free_slot(&mut self, idx: u32) {
        let (db, fp) = {
            let s = &self.slots[idx as usize];
            (s.db, Self::fingerprint(s.db, &s.key))
        };
        // Remove the index entry for this exact slot (match on the index value being
        // this slot idx, not just the key, so a recycled fingerprint cannot remove the
        // wrong entry).
        if let Ok(occ) = self
            .index
            .find_entry(fp, |&i| i == idx && self.slots[i as usize].db == db)
        {
            occ.remove();
        }
        let slot = &mut self.slots[idx as usize];
        slot.live = false;
        slot.generation = slot.generation.wrapping_add(1);
        // Drop the key bytes eagerly so a freed slot does not pin memory while parked on
        // the freelist (re-set on the next alloc).
        slot.key = Box::default();
        self.free.push(idx);
        self.live -= 1;
    }

    /// Resolve a popped handle to a LIVE slot index, or `None` if it is a STALE tombstone
    /// (the slot was freed by a prior `on_remove`, detected by the `gen` mismatch / a
    /// not-live slot). A stale handle represents no live key, so the caller discards it.
    fn resolve(&self, h: Handle) -> Option<u32> {
        let slot = self.slots.get(h.idx as usize)?;
        if slot.live && slot.generation == h.generation {
            Some(h.idx)
        } else {
            None
        }
    }

    /// Decrement the per-queue live count for `queue` (the handle just left that queue).
    fn dec_queue_live(&mut self, queue: QueueTag) {
        match queue {
            QueueTag::Small => self.small_live -= 1,
            QueueTag::Main => self.main_live -= 1,
            QueueTag::Reoffer => self.reoffer_live -= 1,
        }
    }

    /// Bump the 2-bit frequency of the slot for `(db, key)`, if tracked. O(1).
    fn bump_freq(&mut self, db: u32, key: &[u8]) {
        if let Some(idx) = self.lookup(db, key) {
            let slot = &mut self.slots[idx as usize];
            slot.freq = (slot.freq + 1).min(MAX_FREQ);
        }
    }

    /// Remove a `(db, key)` (on an external delete / replace / expiry). Frees its slot
    /// (leaving a stale handle in whatever queue held it) and decrements that queue's
    /// live count. Returns whether it was found. O(1) (no queue scan).
    fn remove_entry(&mut self, db: u32, key: &[u8]) -> bool {
        let Some(idx) = self.lookup(db, key) else {
            return false;
        };
        let queue = self.slots[idx as usize].queue;
        self.free_slot(idx);
        self.dec_queue_live(queue);
        true
    }
}

impl EvictionHook for S3Fifo {
    fn on_access(&mut self, db: u32, key: &[u8]) {
        // A single in-place metadata write (the 2-bit bump) behind ONE O(1) index
        // lookup, no relink (EVICTION.md hot-path contract). The PR-3a linear scan of
        // the three queues is gone: the key->slot index resolves the slot directly.
        self.bump_freq(db, key);
    }

    fn on_insert(&mut self, db: u32, key: &[u8], _bytes: usize) {
        // A replace of an already-tracked key: it is already tracked, so just treat it
        // as a fresh access (bump) rather than duplicating it.
        if let Some(idx) = self.lookup(db, key) {
            let slot = &mut self.slots[idx as usize];
            slot.freq = (slot.freq + 1).min(MAX_FREQ);
            return;
        }
        let fp = Self::fingerprint(db, key);
        if self.ghost_contains(fp) {
            // Seen recently: admit straight to main (it earned a second life).
            self.ghost_remove(fp);
            let h = self.alloc_slot(db, key, QueueTag::Main);
            self.main.push_back(h);
            self.main_live += 1;
        } else {
            // Fresh: probationary small queue.
            let h = self.alloc_slot(db, key, QueueTag::Small);
            self.small.push_back(h);
            self.small_live += 1;
        }
    }

    fn on_remove(&mut self, db: u32, key: &[u8], _bytes: usize) {
        // An external delete/replace/expiry: drop it from its queue so a stale entry
        // is never returned as a victim. (A replace re-inserts afterwards via the
        // store's put funnel, which fires on_remove then on_insert.) O(1): the slot is
        // freed and its handle becomes a stale tombstone discarded on a later pop.
        self.remove_entry(db, key);
    }

    fn select_victim(&mut self) -> Option<(u32, Box<[u8]>)> {
        // A returned victim has been pop_front'd OUT of its queue and freed: the policy
        // no longer tracks it. The store may then SKIP deleting it (a volatile-* policy
        // skips a non-TTL victim, see `ShardStore::evict_to_fit`); in that case the store
        // calls [`EvictionPolicy::re_register`] to put the key BACK as a candidate (the
        // #46 re-eligibility fix), so a later EXPIRE that attaches a TTL makes it eligible
        // without a rewrite. (PR-3a instead DROPPED such a key, which under-evicted a
        // volatile-* policy; #46 is now fixed.)
        //
        // Guaranteed progress: cap the total promotion/second-chance rounds so an
        // all-hot keyspace still yields a victim instead of spinning. The bound is
        // the current LIVE entry count plus a margin: after that many promotions every
        // live entry's freq has been examined, and the eventual victim is returned. The
        // +1 guarantees at least one attempt even with a single entry. A STALE handle
        // (a tombstone from a prior `on_remove`) does NOT consume a round and is NOT a
        // candidate: it is discarded and the loop continues, so the round budget is spent
        // only on LIVE keys. Stale handles are finite and each is removed from its queue
        // when popped, so the loop always makes progress and terminates.
        let mut rounds = self.entry_count().saturating_add(1);

        loop {
            if rounds == 0 {
                // Promotion cap hit (all-hot keyspace). Force-evict the small front,
                // then main, then the re-offer queue, whichever has a LIVE handle, so the
                // store always makes progress (it must be able to free SOMETHING to honor
                // the budget). Stale handles encountered here are discarded.
                if let Some((idx, _)) = self.pop_live(QueueTag::Small) {
                    let (db, key) = self.take_key(idx);
                    // Free (decrement `live`) BEFORE `ghost_record` so `ghost_cap`'s
                    // `entry_count()` EXCLUDES this victim - matching PR-3a, which had
                    // pop_front'd the victim out of its queue before recording the ghost.
                    // (`take_key` already cloned the key out, so freeing here is safe.)
                    self.free_slot(idx);
                    self.ghost_record(Self::fingerprint(db, &key));
                    return Some((db, key));
                }
                if let Some((idx, _)) = self.pop_live(QueueTag::Main) {
                    let (db, key) = self.take_key(idx);
                    self.free_slot(idx);
                    return Some((db, key));
                }
                if let Some((idx, _)) = self.pop_live(QueueTag::Reoffer) {
                    let (db, key) = self.take_key(idx);
                    self.free_slot(idx);
                    return Some((db, key));
                }
                return None;
            }
            rounds -= 1;

            // The 10/90 split (s3fifo-small-main-split): draw from small while it is
            // OVER its ~10% target (its overflow is the probationary churn S3-FIFO
            // evicts first); otherwise drain main with a second chance. When small is
            // within target but main is empty, fall back to small so a tiny keyspace
            // (all in small) still yields a victim. The re-offer queue (skipped
            // non-TTL volatile candidates, #46) is drained LAST, only once small and
            // main are exhausted, so every fresh small/main candidate is offered first.
            // Counts are LIVE counts (stale handles excluded).
            let draw_small =
                (self.small_live > self.small_cap() || self.main_live == 0) && self.small_live > 0;

            if draw_small {
                let Some((idx, _)) = self.pop_live(QueueTag::Small) else {
                    // Only stale handles remained in small; nothing live consumed. Retry
                    // (the stale handles were drained by pop_live without using a round's
                    // worth of decision, but we already spent a round; loop again).
                    continue;
                };
                if self.slots[idx as usize].freq > 1 {
                    // Reused while probationary: promote to main (keep its frequency).
                    let slot = &mut self.slots[idx as usize];
                    slot.freq = MAX_FREQ.min(slot.freq);
                    slot.queue = QueueTag::Main;
                    let g = slot.generation;
                    self.main.push_back(Handle { idx, generation: g });
                    self.main_live += 1;
                    continue;
                }
                // Cold one-hit-wonder: evict and remember its fingerprint. Free
                // (decrement `live`) BEFORE `ghost_record` so `ghost_cap`'s
                // `entry_count()` EXCLUDES this victim, matching PR-3a (which pop_front'd
                // the victim before recording the ghost). `take_key` already cloned out.
                let (db, key) = self.take_key(idx);
                self.free_slot(idx);
                self.ghost_record(Self::fingerprint(db, &key));
                return Some((db, key));
            }

            // Second-chance scan of main.
            if let Some((idx, _)) = self.pop_live(QueueTag::Main) {
                if self.slots[idx as usize].freq > 0 {
                    // Second chance: decrement and re-queue at the back.
                    let slot = &mut self.slots[idx as usize];
                    slot.freq -= 1;
                    slot.queue = QueueTag::Main;
                    let g = slot.generation;
                    self.main.push_back(Handle { idx, generation: g });
                    self.main_live += 1;
                    continue;
                }
                // Cold in main: evict (no ghost record for main evictions, matching
                // S3-FIFO: the ghost tracks SMALL-queue evictions of one-hit wonders).
                let (db, key) = self.take_key(idx);
                self.free_slot(idx);
                return Some((db, key));
            }

            // Small and main exhausted (of live handles): drain the lowest-priority
            // re-offer queue (#46). These are keys the store skipped (non-TTL under
            // volatile-*) and asked to keep as candidates; offering them only now
            // guarantees a fresh small/main candidate (an eligible TTL victim included)
            // is always reached first. The store re-checks TTL and either evicts (if a
            // TTL has since been attached) or re-registers again; its distinct-key skip
            // set bounds the cycle.
            if let Some((idx, _)) = self.pop_live(QueueTag::Reoffer) {
                let (db, key) = self.take_key(idx);
                self.free_slot(idx);
                return Some((db, key));
            }

            // All three queues empty of live handles: nothing to evict.
            return None;
        }
    }
}

impl S3Fifo {
    /// Pop the front LIVE handle of `queue`, discarding any leading STALE tombstones
    /// (their slots were freed by a prior `on_remove`), and decrement that queue's live
    /// count for the live handle returned. Returns `(slot_idx, handle)` or `None` if the
    /// queue holds no live handle. The slot is NOT freed here -- the caller decides to
    /// evict (free) or requeue (push a fresh handle).
    fn pop_live(&mut self, queue: QueueTag) -> Option<(u32, Handle)> {
        loop {
            let h = match queue {
                QueueTag::Small => self.small.pop_front(),
                QueueTag::Main => self.main.pop_front(),
                QueueTag::Reoffer => self.reoffer.pop_front(),
            }?;
            if let Some(idx) = self.resolve(h) {
                self.dec_queue_live(queue);
                return Some((idx, h));
            }
            // Stale tombstone: skip it (its key was already removed; no live count to
            // touch, it was decremented at removal time).
        }
    }

    /// Clone out the `(db, key)` of the live slot at `idx` BEFORE freeing it (the victim
    /// payload returned to the store).
    fn take_key(&self, idx: u32) -> (u32, Box<[u8]>) {
        let s = &self.slots[idx as usize];
        (s.db, s.key.clone())
    }
}

impl EvictionPolicy for S3Fifo {
    fn policy_name(&self) -> String {
        // The CONFIGURED name, returned VERBATIM (e.g. allkeys-lfu, volatile-ttl).
        // Redis round-trips the configured enum string unchanged for CONFIG GET/INFO;
        // the engine that serves it is FIFO-class ([`Self::engine_family`]), a
        // documented victim-ordering divergence (ADR-0009), but the NAME is honored.
        self.name.clone()
    }

    fn evicts(&self) -> bool {
        true
    }

    fn volatile_only(&self) -> bool {
        self.volatile_only
    }

    fn access_freq(&self, _db: u32, _key: &[u8]) -> Option<u8> {
        // S3-FIFO is NOT an LFU engine: it keeps a 2-bit promote-frequency for its OWN
        // queue decisions, not a Redis OBJECT FREQ estimate. OBJECT FREQ requires an
        // LFU maxmemory policy, so the FIFO-class engine reports None and the dispatch
        // layer emits the LFU-gating error (matching Redis, which errors OBJECT FREQ
        // unless maxmemory-policy is *-lfu).
        None
    }

    fn re_register(&mut self, db: u32, key: &[u8]) {
        // The volatile-* re-eligibility fix (#46): `select_victim` pop_front'd this
        // key, and the store declined to delete it (a non-TTL key under a volatile-*
        // policy). Put it BACK so it stays an eviction candidate; a later EXPIRE that
        // attaches a TTL then makes it eligible.
        //
        // We re-queue to the dedicated LOWEST-PRIORITY re-offer queue, NOT small or main.
        // Feeding skipped keys back into small kept it permanently OVER its ~10% target
        // and STARVED main, so a main-resident eligible TTL victim was never offered,
        // producing a false `-OOM` while an evictable volatile key existed (the #46 bug);
        // feeding them into main would symmetrically risk starving small. The separate
        // re-offer queue (drained by `select_victim` only after small and main) removes
        // the starvation in BOTH directions: every fresh small/main candidate (an
        // eligible TTL victim included) is offered before any re-registered key cycles
        // again. The store's distinct-key skip set (see `ShardStore::evict_to_fit`) then
        // terminates the scan once every live key has been offered with no eligible TTL
        // victim. Idempotent: do not duplicate an already-tracked key.
        if self.tracks(db, key) {
            return;
        }
        let h = self.alloc_slot(db, key, QueueTag::Reoffer);
        self.reoffer.push_back(h);
        self.reoffer_live += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ins(p: &mut S3Fifo, key: &[u8]) {
        p.on_insert(0, key, key.len());
    }
    fn acc(p: &mut S3Fifo, key: &[u8]) {
        p.on_access(0, key);
    }
    fn victim_key(p: &mut S3Fifo) -> Option<Vec<u8>> {
        p.select_victim().map(|(_, k)| k.into_vec())
    }

    #[test]
    fn ghost_cap_on_a_cold_eviction_excludes_the_victim_just_like_pr3a() {
        // REGRESSION (review finding): `ghost_record`'s `ghost_cap()` reads
        // `entry_count()`. PR-3a pop_front'd the victim BEFORE recording the ghost, so the
        // count EXCLUDED the victim. The O(1) rewrite must free the slot (decrement
        // `live`) BEFORE `ghost_record` so the cap is computed on the same N-1, or the
        // ghost ring keeps one extra fingerprint at the boundary, flipping a later
        // re-insert's admission (main vs small) and the victim order. This only shows up
        // at >= 10 live keys (below that `ghost_cap` floors at 8 and the off-by-one hides).
        let mut p = S3Fifo::new(false);
        // (1) Evict 8 distinct cold keys so the ghost ring fills to its floor of 8.
        let cold: [&[u8]; 8] = [b"a0", b"a1", b"a2", b"a3", b"a4", b"a5", b"a6", b"a7"];
        for k in cold {
            ins(&mut p, k);
            assert_eq!(victim_key(&mut p), Some(k.to_vec()));
        }
        for k in cold {
            assert!(p.ghost_contains(S3Fifo::fingerprint(0, k)));
        }
        // (2) Insert 10 fresh keys: now entry_count == 10 (> the 9-key ghost-cap knee).
        let fresh: [&[u8]; 10] = [
            b"f0", b"f1", b"f2", b"f3", b"f4", b"f5", b"f6", b"f7", b"f8", b"f9",
        ];
        for k in fresh {
            ins(&mut p, k);
        }
        // (3) The first cold eviction records fresh f0's fingerprint. With the victim
        // EXCLUDED (live 10 -> 9 before the cap), ghost_cap == 8, so pushing f0 trims the
        // OLDEST fingerprint (a0). The buggy ordering (cap computed on 10) would keep a0.
        assert_eq!(victim_key(&mut p), Some(b"f0".to_vec()));
        assert!(
            !p.ghost_contains(S3Fifo::fingerprint(0, b"a0")),
            "a0 must be trimmed: ghost_cap on a cold eviction must exclude the victim (PR-3a parity)"
        );
        // The just-evicted f0 and a still-recent a7 remain in the ring (it is not empty).
        assert!(p.ghost_contains(S3Fifo::fingerprint(0, b"f0")));
        assert!(p.ghost_contains(S3Fifo::fingerprint(0, b"a7")));
        // (4) Re-inserting a0 now MISSES the ghost -> admitted to small (not main), the
        // PR-3a admission this fix preserves.
        ins(&mut p, b"a0");
        assert!(
            p.in_small(b"a0"),
            "a0 missed the ghost, so it lands in small"
        );
    }

    // Test-only helpers over the new representation (the PR-3a tests poked the
    // `VecDeque<Entry>` directly; these check the same facts against the slab + handle
    // queues). A key is "in small" iff a LIVE handle in the small queue resolves to a
    // slot matching it, etc.
    impl S3Fifo {
        /// Whether a LIVE handle in `queue` resolves to a slot holding `(0, key)`.
        fn contains_in(&self, queue: &VecDeque<Handle>, key: &[u8]) -> bool {
            queue.iter().any(|&h| {
                self.slots
                    .get(h.idx as usize)
                    .is_some_and(|s| s.live && s.generation == h.generation && s.matches(0, key))
            })
        }
        fn in_small(&self, key: &[u8]) -> bool {
            self.contains_in(&self.small, key)
        }
        fn in_main(&self, key: &[u8]) -> bool {
            self.contains_in(&self.main, key)
        }
        fn small_or_main_has(&self, key: &[u8]) -> bool {
            self.in_small(key) || self.in_main(key)
        }
        /// The total LIVE tracked count (== `entry_count`), for the "drained empty" checks.
        fn live_count(&self) -> usize {
            self.entry_count()
        }
    }

    #[test]
    fn cold_key_is_evicted_before_a_hot_one() {
        let mut p = S3Fifo::new(false);
        ins(&mut p, b"cold");
        ins(&mut p, b"hot");
        // "hot" is accessed several times (freq > 1), "cold" never.
        acc(&mut p, b"hot");
        acc(&mut p, b"hot");
        // The first victim must be the cold key: the hot key is skipped (its freq
        // keeps it; it is either promoted past or left queued behind cold).
        assert_eq!(victim_key(&mut p), Some(b"cold".to_vec()));
        // The hot key survived the first eviction (it is still tracked somewhere).
        assert!(
            p.small_or_main_has(b"hot"),
            "the hot key must survive the cold eviction"
        );
        // The cold key is gone from both queues.
        assert!(
            !p.small_or_main_has(b"cold"),
            "the cold key must be evicted"
        );
        // A subsequent eviction (with no further accesses) eventually frees the hot
        // key too, demonstrating the second-chance drain terminates.
        let next = victim_key(&mut p);
        assert_eq!(next, Some(b"hot".to_vec()));
        assert!(p.live_count() == 0);
    }

    #[test]
    fn ghost_readmits_a_returning_key_to_main() {
        let mut p = S3Fifo::new(false);
        ins(&mut p, b"k");
        // Evict it (cold) -> fingerprint lands in ghost.
        assert_eq!(victim_key(&mut p), Some(b"k".to_vec()));
        assert!(p.ghost_contains(S3Fifo::fingerprint(0, b"k")));
        // Re-insert: it was in ghost, so it is admitted straight to main.
        ins(&mut p, b"k");
        assert!(p.in_main(b"k"));
        assert!(!p.in_small(b"k"));
        // And the ghost entry was consumed.
        assert!(!p.ghost_contains(S3Fifo::fingerprint(0, b"k")));
    }

    #[test]
    fn all_keys_hot_still_yields_a_victim_within_the_promotion_cap() {
        // Every key is hot (high freq). The promotion cap must still force a victim
        // out so the store's evict-to-fit loop terminates (guaranteed progress).
        let mut p = S3Fifo::new(false);
        for k in [b"a".as_slice(), b"b", b"c", b"d"] {
            ins(&mut p, k);
            // Hammer each key so freq saturates at MAX in both small and (later) main.
            for _ in 0..10 {
                acc(&mut p, k);
            }
        }
        // Even though nothing is cold, select_victim must return Some within bounded
        // rounds (not loop forever).
        let v = victim_key(&mut p);
        assert!(v.is_some(), "an all-hot keyspace must still yield a victim");
        // Draining keeps yielding victims until empty, never spinning.
        let mut count = 1;
        while victim_key(&mut p).is_some() {
            count += 1;
            assert!(
                count <= 100,
                "select_victim must not spin on a hot keyspace"
            );
        }
        assert!(p.live_count() == 0);
    }

    #[test]
    fn empty_policy_yields_no_victim() {
        let mut p = S3Fifo::new(false);
        assert_eq!(p.select_victim(), None);
    }

    #[test]
    fn on_remove_drops_a_queued_key_so_it_is_not_returned() {
        let mut p = S3Fifo::new(false);
        ins(&mut p, b"x");
        ins(&mut p, b"y");
        p.on_remove(0, b"x", 1);
        // x was removed externally; the only victim now is y.
        assert_eq!(victim_key(&mut p), Some(b"y".to_vec()));
        assert_eq!(p.select_victim(), None);
    }

    #[test]
    fn volatile_flag_drives_name_and_posture() {
        // `new` defaults the configured name to the family name.
        let all = S3Fifo::new(false);
        assert_eq!(all.policy_name(), "allkeys-lru");
        assert_eq!(all.engine_family(), "allkeys-lru");
        assert!(!all.volatile_only());
        let vol = S3Fifo::new(true);
        assert_eq!(vol.policy_name(), "volatile-lru");
        assert_eq!(vol.engine_family(), "volatile-lru");
        assert!(vol.volatile_only());
        assert!(vol.evicts());
    }

    #[test]
    fn configured_name_round_trips_verbatim_over_the_engine_family() {
        // The configured spelling is returned VERBATIM even when the engine family
        // diverges (ADR-0009): the NAME is honored, the ENGINE is S3-FIFO.
        let lfu = S3Fifo::with_name(false, "allkeys-lfu");
        assert_eq!(lfu.policy_name(), "allkeys-lfu");
        assert_eq!(lfu.engine_family(), "allkeys-lru");
        let ttl = S3Fifo::with_name(true, "volatile-ttl");
        assert_eq!(ttl.policy_name(), "volatile-ttl");
        assert_eq!(ttl.engine_family(), "volatile-lru");
    }

    #[test]
    fn re_register_keeps_a_skipped_victim_trackable() {
        // The #46 re-eligibility fix: a victim the store declines to delete (a non-TTL
        // key under volatile-*) is RE-REGISTERED, so the policy keeps offering it. The
        // PR-3a path dropped it (on_remove), so it could never be a victim again.
        let mut p = S3Fifo::new(true);
        ins(&mut p, b"x");
        // The store pulls x as a victim, then (non-TTL under volatile-*) re-registers
        // it instead of deleting.
        let v = p.select_victim().expect("x is offered as a victim");
        assert_eq!(v.1.as_ref(), b"x");
        // After select_victim, x is no longer queued (it was pop_front'd).
        assert!(!p.tracks(0, b"x"), "select_victim pops the candidate out");
        // Re-register puts it back (into the lowest-priority re-offer queue) so it stays
        // a candidate.
        p.re_register(0, b"x");
        assert!(
            p.tracks(0, b"x"),
            "re_register keeps the key trackable (#46)"
        );
        // It is offered again on the next pass (now eligible if it has since gained a
        // TTL at the store; the policy does not know about TTL, the store filters).
        assert_eq!(
            p.select_victim().map(|(_, k)| k.into_vec()),
            Some(b"x".to_vec())
        );
        // re_register is idempotent: re-registering a still-tracked key does not dup.
        p.re_register(0, b"x"); // x was just popped, so this re-adds once
        p.re_register(0, b"x"); // already present now -> no-op
        // Exactly one live tracked instance of x (no duplicate).
        assert!(p.tracks(0, b"x"));
        assert_eq!(
            p.live_count(),
            1,
            "re_register must not duplicate a tracked key"
        );
    }

    #[test]
    fn fingerprint_is_stable_and_db_sensitive() {
        // Deterministic across calls (no OS entropy) and distinguishes db.
        assert_eq!(S3Fifo::fingerprint(0, b"k"), S3Fifo::fingerprint(0, b"k"));
        assert_ne!(S3Fifo::fingerprint(0, b"k"), S3Fifo::fingerprint(1, b"k"));
        assert_ne!(S3Fifo::fingerprint(0, b"k"), S3Fifo::fingerprint(0, b"j"));
    }

    #[test]
    fn slot_recycling_does_not_resurrect_a_removed_key() {
        // A removed key's handle becomes a stale tombstone; its slot recycles for a new
        // key. The stale handle must NOT resolve to the new key, and the index must not
        // report the removed key as tracked.
        let mut p = S3Fifo::new(false);
        ins(&mut p, b"a");
        ins(&mut p, b"b");
        p.on_remove(0, b"a", 1); // frees a's slot (likely idx 0), leaves a stale handle
        assert!(!p.tracks(0, b"a"));
        ins(&mut p, b"c"); // recycles the freed slot for c (bumps gen)
        assert!(p.tracks(0, b"c"));
        assert!(!p.tracks(0, b"a"), "removed key must not resurrect");
        // The live tracked set is exactly {b, c}.
        assert_eq!(p.live_count(), 2);
        assert!(p.tracks(0, b"b"));
    }

    #[test]
    fn replace_of_a_tracked_key_bumps_not_duplicates() {
        // on_insert of an already-tracked key bumps its freq (a replace) rather than
        // allocating a second slot (the O(1) tracks-then-bump path).
        let mut p = S3Fifo::new(false);
        ins(&mut p, b"k");
        ins(&mut p, b"k");
        ins(&mut p, b"k");
        assert_eq!(p.live_count(), 1, "a replace must not duplicate the key");
        let idx = p.lookup(0, b"k").expect("k tracked");
        assert!(p.slots[idx as usize].freq >= 1, "the replace bumped freq");
    }
}
