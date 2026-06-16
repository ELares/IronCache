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
//! ## 2-bit frequency: in the STORED OBJECT, not the policy (freq-in-object)
//!
//! The frequency lives ON the stored object (the kvobj's `CollEntry.eviction_rank` /
//! the Str blob's spare FLAGS bits), NOT in the policy. PR-3a kept a policy-side
//! per-key index (a slab + handle queues + a `key->slot` hash map) purely so the hot
//! `on_access` 2-bit bump could be O(1); but that index was net-new per-key memory
//! (~28 B/key on the whole-process `used_memory`), which lost the memory head-to-head.
//!
//! The fix (this rewrite): the STORE bumps the just-accessed entry's freq INLINE on the
//! read path (it already holds the entry, so the bump is O(1) with no policy call), so
//! `on_access` no longer needs the policy AT ALL, and the policy no longer needs its
//! per-key index/slab/handles. The policy is back to ~its pre-rewrite memory footprint:
//! three FIFO queues holding the KEY directly + the ghost ring + a few counters.
//! `select_victim` still makes the promote/second-chance/ghost decision, but it reads
//! and decrements each candidate's freq through a [`VictimFreq`] the store passes
//! (backed by the store's own tables). The hot path stays O(1); the policy memory is
//! reclaimed.
//!
//! ## The queue layout (KEY-in-queue, O(N) splice on remove)
//!
//! The state is three `VecDeque<(u32 db, Box<[u8]> key)>` FIFOs (small / main / reoffer)
//! plus the ghost ring. A key sits in exactly one queue. `on_insert` pushes the key;
//! `on_remove` splices it out (an O(N) scan of the three queues, ACCEPTABLE because
//! removal fires only on writes/deletes (~10% of ops), NOT the read hot path that the
//! profile flagged). `select_victim` pops the front of the chosen queue and consults
//! `VictimFreq` for the freq. A popped key whose `freq.get` returns `None` is a stale
//! tombstone (removed since) and is skipped. This is the SAME exact ordering algorithm
//! PR-3a had (10/90 draw_small, freq>1 small->main promote, freq>0 main second-chance,
//! reoffer drained last, rounds-bounded progress, ghost on small/cold evictions only).
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

use ironcache_storage::{EvictionHook, VictimFreq};

use crate::EvictionPolicy;

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

/// One tracked key in a FIFO queue: its logical db id and the owned key bytes (the
/// SINGLE owned copy; the freq lives on the stored object, not here).
type QKey = (u32, Box<[u8]>);

/// The S3-FIFO policy state (per shard, unsynchronized; ADR-0005).
///
/// freq-in-object: the per-key 2-bit frequency is NOT here (it lives on the stored
/// object, read via [`VictimFreq`] in `select_victim`). This struct is back to ~the
/// pre-rewrite footprint: three key FIFOs + the ghost ring + the posture flags.
#[derive(Debug, Clone)]
pub struct S3Fifo {
    /// The small probationary FIFO (fresh keys land here unless ghost-readmitted),
    /// holding `(db, key)` in FIFO order.
    small: VecDeque<QKey>,
    /// The large main FIFO (promoted / ghost-readmitted keys).
    main: VecDeque<QKey>,
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
    reoffer: VecDeque<QKey>,
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
            small: VecDeque::new(),
            main: VecDeque::new(),
            reoffer: VecDeque::new(),
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

    /// A stable, deterministic fingerprint of a `(db, key)` for the ghost ring. This is
    /// NOT cryptographic and NOT the store's hash; it is a fixed-constant FNV-1a over
    /// `db` then the key bytes, so it is identical on every run (ADR-0003: no OS
    /// entropy, no `RandomState`). For the ghost ring a collision only costs a spurious
    /// main-admission, which is harmless.
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

    /// The running total of tracked keys (small + main + the re-offer queue). All three
    /// queues hold LIVE keys the store still tracks, so they all count toward the cap
    /// sizing and the guaranteed-progress round bound. (A key that was removed via
    /// `on_remove` is spliced OUT of its queue, so no stale entry is counted here.)
    fn entry_count(&self) -> usize {
        self.small.len() + self.main.len() + self.reoffer.len()
    }

    /// Whether `(db, key)` is tracked in any of the three queues. O(N) (a scan), used
    /// only on the WRITE path (`re_register` idempotence), never on the read hot path.
    fn tracks(&self, db: u32, key: &[u8]) -> bool {
        let hit = |q: &VecDeque<QKey>| q.iter().any(|(d, k)| *d == db && k.as_ref() == key);
        hit(&self.small) || hit(&self.main) || hit(&self.reoffer)
    }

    /// The current small-queue target capacity (~10% of the running entry count, at
    /// least 1 so a fresh key always has a home).
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

    /// Splice `(db, key)` out of whichever of the three queues holds it (the O(N)
    /// removal). Returns whether it was found. Fires only on the WRITE path
    /// (`on_remove`), NOT the read hot path, so the scan is not the profile's
    /// bottleneck.
    fn remove_key(&mut self, db: u32, key: &[u8]) -> bool {
        let splice = |q: &mut VecDeque<QKey>| -> bool {
            if let Some(i) = q.iter().position(|(d, k)| *d == db && k.as_ref() == key) {
                q.remove(i);
                true
            } else {
                false
            }
        };
        // A key sits in exactly one queue, so stop at the first hit.
        splice(&mut self.small) || splice(&mut self.main) || splice(&mut self.reoffer)
    }
}

impl EvictionHook for S3Fifo {
    fn on_access(&mut self, _db: u32, _key: &[u8]) {
        // freq-in-object: the 2-bit promote frequency lives ON the stored object and the
        // STORE bumps it inline on the read path (it holds the entry). The policy no
        // longer keeps a per-key freq, so there is nothing to do here. The store no
        // longer calls this on the hot path; kept as a no-op for the trait contract.
    }

    fn on_insert(&mut self, db: u32, key: &[u8], _bytes: usize) {
        // A fresh insert: admit to main if the fingerprint is still in the ghost (it was
        // popular enough to come back), else to the probationary small queue.
        //
        // A REPLACE of an already-tracked key is delivered by the store as on_remove
        // THEN on_insert (the put funnel fires both), so the old queue entry was already
        // spliced out by on_remove and there is no duplicate to guard against here. The
        // store separately carries the REUSED KEY'S FREQUENCY across the replace (it
        // copies the old entry's freq onto the new entry), preserving the S3-FIFO "a
        // reused key keeps its frequency" semantic without the policy tracking it.
        let fp = Self::fingerprint(db, key);
        if self.ghost_contains(fp) {
            self.ghost_remove(fp);
            self.main.push_back((db, key.to_vec().into_boxed_slice()));
        } else {
            self.small.push_back((db, key.to_vec().into_boxed_slice()));
        }
    }

    fn on_remove(&mut self, db: u32, key: &[u8], _bytes: usize) {
        // An external delete/replace/expiry: splice it out of its queue so a stale entry
        // is never offered as a victim. (A replace re-inserts afterwards via the store's
        // put funnel, which fires on_remove then on_insert.) O(N) splice, on the write
        // path only.
        self.remove_key(db, key);
    }

    fn select_victim(&mut self, freq: &mut dyn VictimFreq) -> Option<(u32, Box<[u8]>)> {
        // A returned victim has been pop_front'd OUT of its queue: the policy no longer
        // tracks it. The store may then SKIP deleting it (a volatile-* policy skips a
        // non-TTL victim, see `ShardStore::evict_to_fit`); in that case the store calls
        // [`EvictionPolicy::re_register`] to put the key BACK as a candidate (the #46
        // re-eligibility fix).
        //
        // freq-in-object: the promote/second-chance decision needs each candidate's
        // freq, which lives on the STORED OBJECT now. We read it via `freq.get` and
        // decrement it via `freq.dec`. A popped key whose `freq.get` returns `None` is a
        // STALE tombstone (removed since, e.g. concurrently expired) and is skipped
        // WITHOUT consuming a promotion round.
        //
        // Guaranteed progress: cap the total promotion/second-chance rounds so an
        // all-hot keyspace still yields a victim instead of spinning. The bound is the
        // current entry count plus a margin: after that many promotions every live
        // entry's freq has been examined, and the eventual victim is returned. The +1
        // guarantees at least one attempt even with a single entry.
        let mut rounds = self.entry_count().saturating_add(1);

        loop {
            if rounds == 0 {
                // Promotion cap hit (all-hot keyspace). Force-evict the small front, then
                // main, then the re-offer queue, whichever has a key, so the store always
                // makes progress (it must be able to free SOMETHING to honor the budget).
                // Stale tombstones (freq None) are discarded as encountered.
                if let Some((db, key)) = self.pop_present(QueueTag::Small, freq) {
                    // Splice-count parity with PR-3a: the victim is OUT of its queue
                    // (pop_front), so `entry_count()` already EXCLUDES it before
                    // `ghost_record` computes `ghost_cap`.
                    self.ghost_record(Self::fingerprint(db, &key));
                    return Some((db, key));
                }
                if let Some((db, key)) = self.pop_present(QueueTag::Main, freq) {
                    return Some((db, key));
                }
                if let Some((db, key)) = self.pop_present(QueueTag::Reoffer, freq) {
                    return Some((db, key));
                }
                return None;
            }
            rounds -= 1;

            // The 10/90 split (s3fifo-small-main-split): draw from small while it is
            // OVER its ~10% target (its overflow is the probationary churn S3-FIFO
            // evicts first); otherwise drain main with a second chance. When small is
            // within target but main is empty, fall back to small so a tiny keyspace
            // (all in small) still yields a victim. The re-offer queue (skipped non-TTL
            // volatile candidates, #46) is drained LAST, only once small and main are
            // exhausted, so every fresh small/main candidate is offered first.
            let draw_small = (self.small.len() > self.small_cap() || self.main.is_empty())
                && !self.small.is_empty();

            if draw_small {
                let Some((db, key, f)) = self.pop_with_freq(QueueTag::Small, freq) else {
                    // Only stale tombstones remained in small; nothing live consumed.
                    // Loop again (we already spent a round).
                    continue;
                };
                if f > 1 {
                    // Reused while probationary: promote to main (its freq stays on the
                    // object; nothing to write here).
                    self.main.push_back((db, key));
                    continue;
                }
                // Cold one-hit-wonder: evict and remember its fingerprint. The key is
                // already pop_front'd OUT, so `entry_count()` excludes it before
                // `ghost_cap` (PR-3a parity).
                self.ghost_record(Self::fingerprint(db, &key));
                return Some((db, key));
            }

            // Second-chance scan of main.
            if let Some((db, key, f)) = self.pop_with_freq(QueueTag::Main, freq) {
                if f > 0 {
                    // Second chance: decrement the object's freq and re-queue at the back.
                    freq.dec(db, &key);
                    self.main.push_back((db, key));
                    continue;
                }
                // Cold in main: evict (no ghost record for main evictions, matching
                // S3-FIFO: the ghost tracks SMALL-queue evictions of one-hit wonders).
                return Some((db, key));
            }

            // Small and main exhausted (of present keys): drain the lowest-priority
            // re-offer queue (#46). These are keys the store skipped (non-TTL under
            // volatile-*) and asked to keep as candidates; offering them only now
            // guarantees a fresh small/main candidate (an eligible TTL victim included)
            // is always reached first. The store re-checks TTL and either evicts (if a
            // TTL has since been attached) or re-registers again; its distinct-key skip
            // set bounds the cycle.
            if let Some((db, key)) = self.pop_present(QueueTag::Reoffer, freq) {
                return Some((db, key));
            }

            // All three queues empty of present keys: nothing to evict.
            return None;
        }
    }
}

/// Which FIFO queue a `pop_*` helper draws from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueTag {
    Small,
    Main,
    Reoffer,
}

impl S3Fifo {
    /// Pop the front of `queue`, returning the first key that is STILL PRESENT in the
    /// store (its `freq.get` is `Some`). Leading stale tombstones (removed since;
    /// `freq.get` is `None`) are discarded as they are popped. Returns `(db, key)` or
    /// `None` if the queue holds no present key.
    fn pop_present(&mut self, queue: QueueTag, freq: &dyn VictimFreq) -> Option<(u32, Box<[u8]>)> {
        loop {
            let (db, key) = self.pop_front(queue)?;
            if freq.get(db, &key).is_some() {
                return Some((db, key));
            }
            // Stale tombstone: the store removed this key since it was queued. Drop it
            // and keep popping.
        }
    }

    /// Pop the front PRESENT key of `queue` together with its current freq (read off the
    /// stored object via `freq`). Leading stale tombstones are discarded. Returns
    /// `(db, key, freq)` or `None`.
    fn pop_with_freq(
        &mut self,
        queue: QueueTag,
        freq: &dyn VictimFreq,
    ) -> Option<(u32, Box<[u8]>, u8)> {
        loop {
            let (db, key) = self.pop_front(queue)?;
            if let Some(f) = freq.get(db, &key) {
                return Some((db, key, f));
            }
            // Stale tombstone: discard and keep popping.
        }
    }

    /// Pop the raw front entry of `queue` (no presence check).
    fn pop_front(&mut self, queue: QueueTag) -> Option<QKey> {
        match queue {
            QueueTag::Small => self.small.pop_front(),
            QueueTag::Main => self.main.pop_front(),
            QueueTag::Reoffer => self.reoffer.pop_front(),
        }
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
        // queue decisions (now on the stored object), not a Redis OBJECT FREQ estimate.
        // OBJECT FREQ requires an LFU maxmemory policy, so the FIFO-class engine reports
        // None and the dispatch layer emits the LFU-gating error (matching Redis, which
        // errors OBJECT FREQ unless maxmemory-policy is *-lfu).
        None
    }

    fn re_register(&mut self, db: u32, key: &[u8]) {
        // The volatile-* re-eligibility fix (#46): `select_victim` pop_front'd this key,
        // and the store declined to delete it (a non-TTL key under a volatile-* policy).
        // Put it BACK so it stays an eviction candidate; a later EXPIRE that attaches a
        // TTL then makes it eligible.
        //
        // We re-queue to the dedicated LOWEST-PRIORITY re-offer queue, NOT small or main.
        // Feeding skipped keys back into small kept it permanently OVER its ~10% target
        // and STARVED main, so a main-resident eligible TTL victim was never offered,
        // producing a false `-OOM` while an evictable volatile key existed (the #46 bug);
        // feeding them into main would symmetrically risk starving small. The separate
        // re-offer queue (drained by `select_victim` only after small and main) removes
        // the starvation in BOTH directions. Idempotent: do not duplicate an
        // already-tracked key.
        if self.tracks(db, key) {
            return;
        }
        self.reoffer
            .push_back((db, key.to_vec().into_boxed_slice()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A test [`VictimFreq`] standing in for the store's tables: a `(db, key) -> freq`
    /// map. A key absent from the map is treated as "no longer present" (a stale
    /// tombstone), exactly as the real store reports a key it no longer holds. The tests
    /// drive `ins`/`acc`/remove against BOTH this map and the policy so they stay in
    /// lockstep, mirroring how the store bumps freq inline while the policy queues keys.
    #[derive(Default)]
    struct FreqMap {
        freqs: HashMap<(u32, Vec<u8>), u8>,
    }

    impl FreqMap {
        fn insert(&mut self, db: u32, key: &[u8]) {
            // A fresh key starts at freq 0 (the store's fresh-entry default). A replace
            // keeps the existing freq (the store carries it across), so do not reset.
            self.freqs.entry((db, key.to_vec())).or_insert(0);
        }
        fn bump(&mut self, db: u32, key: &[u8]) {
            let e = self.freqs.entry((db, key.to_vec())).or_insert(0);
            *e = (*e + 1).min(3);
        }
        fn remove(&mut self, db: u32, key: &[u8]) {
            self.freqs.remove(&(db, key.to_vec()));
        }
    }

    impl VictimFreq for FreqMap {
        fn get(&self, db: u32, key: &[u8]) -> Option<u8> {
            self.freqs.get(&(db, key.to_vec())).copied()
        }
        fn dec(&mut self, db: u32, key: &[u8]) {
            if let Some(f) = self.freqs.get_mut(&(db, key.to_vec())) {
                *f = f.saturating_sub(1);
            }
        }
    }

    /// A policy + its companion freq map, driven together so the freq the store would
    /// hold on the object is what `select_victim` reads through `VictimFreq`.
    struct Harness {
        p: S3Fifo,
        f: FreqMap,
    }

    impl Harness {
        fn new(volatile_only: bool) -> Self {
            Harness {
                p: S3Fifo::new(volatile_only),
                f: FreqMap::default(),
            }
        }
        /// Insert a key: the store would `on_insert` the key into the policy AND seed the
        /// object's freq. A replace (key already present) keeps the freq (store carries
        /// it), so the map is not reset.
        fn ins(&mut self, key: &[u8]) {
            self.f.insert(0, key);
            self.p.on_insert(0, key, key.len());
        }
        /// Access a key: the store bumps the OBJECT's freq inline (the policy's on_access
        /// is now a no-op).
        fn acc(&mut self, key: &[u8]) {
            self.f.bump(0, key);
            self.p.on_access(0, key);
        }
        /// Remove a key (delete/expiry): the store drops the object (so the freq map
        /// loses it) and fires on_remove.
        fn rem(&mut self, key: &[u8]) {
            self.f.remove(0, key);
            self.p.on_remove(0, key, key.len());
        }
        /// Select a victim and, like the store, delete it from the freq map (the store
        /// removes the object on a real eviction).
        fn victim_key(&mut self) -> Option<Vec<u8>> {
            let v = self.p.select_victim(&mut self.f);
            if let Some((db, key)) = &v {
                self.f.remove(*db, key);
            }
            v.map(|(_, k)| k.into_vec())
        }
        /// Select a victim WITHOUT deleting it from the freq map (the volatile-* skip
        /// path: the store offers the key but declines to delete, then re-registers it).
        fn offer_victim(&mut self) -> Option<Vec<u8>> {
            self.p.select_victim(&mut self.f).map(|(_, k)| k.into_vec())
        }
        fn tracks(&self, key: &[u8]) -> bool {
            self.p.tracks(0, key)
        }
        fn in_small(&self, key: &[u8]) -> bool {
            self.p
                .small
                .iter()
                .any(|(d, k)| *d == 0 && k.as_ref() == key)
        }
        fn in_main(&self, key: &[u8]) -> bool {
            self.p
                .main
                .iter()
                .any(|(d, k)| *d == 0 && k.as_ref() == key)
        }
        fn small_or_main_has(&self, key: &[u8]) -> bool {
            self.in_small(key) || self.in_main(key)
        }
        fn live_count(&self) -> usize {
            self.p.entry_count()
        }
        fn ghost_contains(&self, fp: u64) -> bool {
            self.p.ghost_contains(fp)
        }
    }

    #[test]
    fn ghost_cap_on_a_cold_eviction_excludes_the_victim_just_like_pr3a() {
        // REGRESSION (review finding): `ghost_record`'s `ghost_cap()` reads
        // `entry_count()`. The victim is pop_front'd OUT of its queue BEFORE recording
        // the ghost, so the count EXCLUDES the victim. If it did not, the ghost ring
        // would keep one extra fingerprint at the boundary, flipping a later re-insert's
        // admission (main vs small) and the victim order. This only shows up at >= 10
        // live keys (below that `ghost_cap` floors at 8 and the off-by-one hides).
        let mut h = Harness::new(false);
        // (1) Evict 8 distinct cold keys so the ghost ring fills to its floor of 8.
        let cold: [&[u8]; 8] = [b"a0", b"a1", b"a2", b"a3", b"a4", b"a5", b"a6", b"a7"];
        for k in cold {
            h.ins(k);
            assert_eq!(h.victim_key(), Some(k.to_vec()));
        }
        for k in cold {
            assert!(h.ghost_contains(S3Fifo::fingerprint(0, k)));
        }
        // (2) Insert 10 fresh keys: now entry_count == 10 (> the 9-key ghost-cap knee).
        let fresh: [&[u8]; 10] = [
            b"f0", b"f1", b"f2", b"f3", b"f4", b"f5", b"f6", b"f7", b"f8", b"f9",
        ];
        for k in fresh {
            h.ins(k);
        }
        // (3) The first cold eviction records fresh f0's fingerprint. With the victim
        // EXCLUDED (live 10 -> 9 before the cap), ghost_cap == 8, so pushing f0 trims the
        // OLDEST fingerprint (a0). The buggy ordering (cap computed on 10) would keep a0.
        assert_eq!(h.victim_key(), Some(b"f0".to_vec()));
        assert!(
            !h.ghost_contains(S3Fifo::fingerprint(0, b"a0")),
            "a0 must be trimmed: ghost_cap on a cold eviction must exclude the victim (PR-3a parity)"
        );
        // The just-evicted f0 and a still-recent a7 remain in the ring (it is not empty).
        assert!(h.ghost_contains(S3Fifo::fingerprint(0, b"f0")));
        assert!(h.ghost_contains(S3Fifo::fingerprint(0, b"a7")));
        // (4) Re-inserting a0 now MISSES the ghost -> admitted to small (not main), the
        // PR-3a admission this fix preserves.
        h.ins(b"a0");
        assert!(
            h.in_small(b"a0"),
            "a0 missed the ghost, so it lands in small"
        );
    }

    #[test]
    fn cold_key_is_evicted_before_a_hot_one() {
        let mut h = Harness::new(false);
        h.ins(b"cold");
        h.ins(b"hot");
        // "hot" is accessed several times (freq > 1), "cold" never.
        h.acc(b"hot");
        h.acc(b"hot");
        // The first victim must be the cold key: the hot key is skipped (its freq
        // keeps it; it is either promoted past or left queued behind cold).
        assert_eq!(h.victim_key(), Some(b"cold".to_vec()));
        // The hot key survived the first eviction (it is still tracked somewhere).
        assert!(
            h.small_or_main_has(b"hot"),
            "the hot key must survive the cold eviction"
        );
        // The cold key is gone from both queues.
        assert!(
            !h.small_or_main_has(b"cold"),
            "the cold key must be evicted"
        );
        // A subsequent eviction (with no further accesses) eventually frees the hot
        // key too, demonstrating the second-chance drain terminates.
        let next = h.victim_key();
        assert_eq!(next, Some(b"hot".to_vec()));
        assert!(h.live_count() == 0);
    }

    #[test]
    fn ghost_readmits_a_returning_key_to_main() {
        let mut h = Harness::new(false);
        h.ins(b"k");
        // Evict it (cold) -> fingerprint lands in ghost.
        assert_eq!(h.victim_key(), Some(b"k".to_vec()));
        assert!(h.ghost_contains(S3Fifo::fingerprint(0, b"k")));
        // Re-insert: it was in ghost, so it is admitted straight to main.
        h.ins(b"k");
        assert!(h.in_main(b"k"));
        assert!(!h.in_small(b"k"));
        // And the ghost entry was consumed.
        assert!(!h.ghost_contains(S3Fifo::fingerprint(0, b"k")));
    }

    #[test]
    fn all_keys_hot_still_yields_a_victim_within_the_promotion_cap() {
        // Every key is hot (high freq). The promotion cap must still force a victim
        // out so the store's evict-to-fit loop terminates (guaranteed progress).
        let mut h = Harness::new(false);
        for k in [b"a".as_slice(), b"b", b"c", b"d"] {
            h.ins(k);
            // Hammer each key so freq saturates at MAX.
            for _ in 0..10 {
                h.acc(k);
            }
        }
        // Even though nothing is cold, select_victim must return Some within bounded
        // rounds (not loop forever).
        let v = h.victim_key();
        assert!(v.is_some(), "an all-hot keyspace must still yield a victim");
        // Draining keeps yielding victims until empty, never spinning.
        let mut count = 1;
        while h.victim_key().is_some() {
            count += 1;
            assert!(
                count <= 100,
                "select_victim must not spin on a hot keyspace"
            );
        }
        assert!(h.live_count() == 0);
    }

    #[test]
    fn empty_policy_yields_no_victim() {
        let mut h = Harness::new(false);
        assert_eq!(h.offer_victim(), None);
    }

    #[test]
    fn on_remove_drops_a_queued_key_so_it_is_not_returned() {
        let mut h = Harness::new(false);
        h.ins(b"x");
        h.ins(b"y");
        h.rem(b"x");
        // x was removed externally; the only victim now is y.
        assert_eq!(h.victim_key(), Some(b"y".to_vec()));
        assert_eq!(h.offer_victim(), None);
    }

    #[test]
    fn a_stale_tombstone_in_the_queue_is_skipped_not_returned() {
        // freq-in-object edge: if the store removed a key from the OBJECT TABLE but the
        // policy's queue still holds it (e.g. a path that did not fire on_remove), the
        // freq map reports None and select_victim skips it WITHOUT consuming a round or
        // returning a dead key. Force this by removing only from the freq map.
        let mut h = Harness::new(false);
        h.ins(b"gone");
        h.ins(b"live");
        // Simulate the store dropping `gone` from its tables without an on_remove: only
        // the freq map loses it, the policy queue still holds the stale key.
        h.f.remove(0, b"gone");
        // select_victim pops `gone` (freq None -> skip), then returns `live`.
        assert_eq!(h.victim_key(), Some(b"live".to_vec()));
        assert_eq!(
            h.offer_victim(),
            None,
            "the stale tombstone was skipped, not returned"
        );
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
        // key under volatile-*) is RE-REGISTERED, so the policy keeps offering it.
        let mut h = Harness::new(true);
        h.ins(b"x");
        // The store pulls x as a victim, then (non-TTL under volatile-*) re-registers
        // it instead of deleting (so it stays in the freq map: use offer_victim).
        let v = h.offer_victim().expect("x is offered as a victim");
        assert_eq!(v, b"x".to_vec());
        // After select_victim, x is no longer queued (it was pop_front'd).
        assert!(!h.tracks(b"x"), "select_victim pops the candidate out");
        // Re-register puts it back (into the lowest-priority re-offer queue).
        h.p.re_register(0, b"x");
        assert!(h.tracks(b"x"), "re_register keeps the key trackable (#46)");
        // It is offered again on the next pass.
        assert_eq!(h.offer_victim(), Some(b"x".to_vec()));
        // re_register is idempotent: re-registering a still-tracked key does not dup.
        h.p.re_register(0, b"x"); // x was just popped, so this re-adds once
        h.p.re_register(0, b"x"); // already present now -> no-op
        assert!(h.tracks(b"x"));
        assert_eq!(
            h.live_count(),
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
    fn replace_does_not_duplicate_the_key_in_a_queue() {
        // The store delivers a replace as on_remove THEN on_insert, so the old queue
        // entry is spliced out first and there is exactly one queue entry for the key.
        let mut h = Harness::new(false);
        h.ins(b"k");
        // Simulate the store's replace funnel: on_remove then on_insert.
        h.p.on_remove(0, b"k", 1);
        h.p.on_insert(0, b"k", 1);
        assert_eq!(h.live_count(), 1, "a replace must not duplicate the key");
        assert!(h.tracks(b"k"));
    }
}
