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
//! The frequency lives ON each queued entry ([`Entry::freq`]), bumped in
//! [`EvictionHook::on_access`], and this is the SINGLE source of truth. The crate docs
//! explain why it is the policy-side counter rather than the kvobj `eviction_rank`:
//! `select_victim` is policy-only and cannot borrow the kvobj header. The kvobj
//! `eviction_rank` field is RESERVED for a later single-source migration and is NOT
//! written on the store's access path today. The counter is bounded (one per queued
//! key, dropped when the key leaves both queues) and capped at 3, so it is the 2-bit
//! field S3-FIFO needs.
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

/// A queued entry: a `(db, key)` plus its 2-bit access frequency.
#[derive(Debug, Clone)]
struct Entry {
    db: u32,
    key: Box<[u8]>,
    freq: u8,
}

impl Entry {
    fn matches(&self, db: u32, key: &[u8]) -> bool {
        self.db == db && self.key.as_ref() == key
    }
}

/// The S3-FIFO policy state (per shard, unsynchronized; ADR-0005).
#[derive(Debug, Clone)]
pub struct S3Fifo {
    /// The small probationary FIFO (fresh keys land here unless ghost-readmitted).
    small: VecDeque<Entry>,
    /// The large main FIFO (promoted / ghost-readmitted keys).
    main: VecDeque<Entry>,
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
    reoffer: VecDeque<Entry>,
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

    /// A stable, deterministic fingerprint of a `(db, key)` for the ghost ring. This
    /// is NOT cryptographic and NOT the store's hash; it is a fixed-constant FNV-1a
    /// over `db` then the key bytes, so it is identical on every run (ADR-0003: no OS
    /// entropy, no RandomState). Collisions only cost a spurious main-admission, which
    /// is harmless.
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

    /// The running total of queued entries (small + main + the re-offer queue). All
    /// three hold LIVE keys the store still tracks, so they all count toward the cap
    /// sizing and the guaranteed-progress round bound.
    fn entry_count(&self) -> usize {
        self.small.len() + self.main.len() + self.reoffer.len()
    }

    /// Whether `(db, key)` is tracked in ANY of the three queues (small/main/reoffer).
    fn tracks(&self, db: u32, key: &[u8]) -> bool {
        self.small.iter().any(|e| e.matches(db, key))
            || self.main.iter().any(|e| e.matches(db, key))
            || self.reoffer.iter().any(|e| e.matches(db, key))
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

    /// Bump the 2-bit frequency of a queued entry matching `(db, key)`, if present in
    /// any of the three queues.
    fn bump_freq(&mut self, db: u32, key: &[u8]) {
        if let Some(e) = self.small.iter_mut().find(|e| e.matches(db, key)) {
            e.freq = (e.freq + 1).min(MAX_FREQ);
            return;
        }
        if let Some(e) = self.main.iter_mut().find(|e| e.matches(db, key)) {
            e.freq = (e.freq + 1).min(MAX_FREQ);
            return;
        }
        if let Some(e) = self.reoffer.iter_mut().find(|e| e.matches(db, key)) {
            e.freq = (e.freq + 1).min(MAX_FREQ);
        }
    }

    /// Remove a `(db, key)` from whichever queue holds it (on an external delete /
    /// replace / expiry). Returns whether it was found.
    fn remove_entry(&mut self, db: u32, key: &[u8]) -> bool {
        if let Some(i) = self.small.iter().position(|e| e.matches(db, key)) {
            self.small.remove(i);
            return true;
        }
        if let Some(i) = self.main.iter().position(|e| e.matches(db, key)) {
            self.main.remove(i);
            return true;
        }
        if let Some(i) = self.reoffer.iter().position(|e| e.matches(db, key)) {
            self.reoffer.remove(i);
            return true;
        }
        false
    }
}

impl EvictionHook for S3Fifo {
    fn on_access(&mut self, db: u32, key: &[u8]) {
        // A single in-place metadata write (the 2-bit bump), no relink (EVICTION.md
        // hot-path contract). The find is a linear scan for PR-3a; the eventual
        // intrusive-link layout removes it (a #8 follow-up).
        self.bump_freq(db, key);
    }

    fn on_insert(&mut self, db: u32, key: &[u8], _bytes: usize) {
        // A replace of an already-queued key: it is already tracked, so just treat it
        // as a fresh access (bump) rather than duplicating it.
        if self.tracks(db, key) {
            self.bump_freq(db, key);
            return;
        }
        let fp = Self::fingerprint(db, key);
        let entry = Entry {
            db,
            key: key.to_vec().into_boxed_slice(),
            freq: 0,
        };
        if self.ghost_contains(fp) {
            // Seen recently: admit straight to main (it earned a second life).
            self.ghost_remove(fp);
            self.main.push_back(entry);
        } else {
            // Fresh: probationary small queue.
            self.small.push_back(entry);
        }
    }

    fn on_remove(&mut self, db: u32, key: &[u8], _bytes: usize) {
        // An external delete/replace/expiry: drop it from its queue so a stale entry
        // is never returned as a victim. (A replace re-inserts afterwards via the
        // store's put funnel, which fires on_remove then on_insert.)
        self.remove_entry(db, key);
    }

    fn select_victim(&mut self) -> Option<(u32, Box<[u8]>)> {
        // A returned victim has been pop_front'd OUT of its queue: the policy no longer
        // tracks it. The store may then SKIP deleting it (a volatile-* policy skips a
        // non-TTL victim, see `ShardStore::evict_to_fit`); in that case the store calls
        // [`EvictionPolicy::re_register`] to put the key BACK as a candidate (the #46
        // re-eligibility fix), so a later EXPIRE that attaches a TTL makes it eligible
        // without a rewrite. (PR-3a instead DROPPED such a key, which under-evicted a
        // volatile-* policy; #46 is now fixed.)
        //
        // Guaranteed progress: cap the total promotion/second-chance rounds so an
        // all-hot keyspace still yields a victim instead of spinning. The bound is
        // the current entry count plus a margin: after that many promotions every
        // entry's freq has been examined, and the eventual victim is returned. The +1
        // guarantees at least one attempt even with a single entry.
        let mut rounds = self.entry_count().saturating_add(1);

        loop {
            if rounds == 0 {
                // Promotion cap hit (all-hot keyspace). Force-evict the small front,
                // then main, then the re-offer queue, whichever exists, so the store
                // always makes progress (it must be able to free SOMETHING to honor the
                // budget).
                if let Some(e) = self.small.pop_front() {
                    let fp = Self::fingerprint(e.db, &e.key);
                    self.ghost_record(fp);
                    return Some((e.db, e.key));
                }
                if let Some(e) = self.main.pop_front() {
                    return Some((e.db, e.key));
                }
                if let Some(e) = self.reoffer.pop_front() {
                    return Some((e.db, e.key));
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
            let draw_small = (self.small.len() > self.small_cap() || self.main.is_empty())
                && !self.small.is_empty();

            if draw_small {
                let mut e = self
                    .small
                    .pop_front()
                    .expect("draw_small implies non-empty");
                if e.freq > 1 {
                    // Reused while probationary: promote to main (keep its frequency).
                    e.freq = MAX_FREQ.min(e.freq);
                    self.main.push_back(e);
                    continue;
                }
                // Cold one-hit-wonder: evict and remember its fingerprint.
                let fp = Self::fingerprint(e.db, &e.key);
                self.ghost_record(fp);
                return Some((e.db, e.key));
            }

            // Second-chance scan of main.
            if let Some(mut e) = self.main.pop_front() {
                if e.freq > 0 {
                    // Second chance: decrement and re-queue at the back.
                    e.freq -= 1;
                    self.main.push_back(e);
                    continue;
                }
                // Cold in main: evict (no ghost record for main evictions, matching
                // S3-FIFO: the ghost tracks SMALL-queue evictions of one-hit wonders).
                return Some((e.db, e.key));
            }

            // Small and main exhausted: drain the lowest-priority re-offer queue (#46).
            // These are keys the store skipped (non-TTL under volatile-*) and asked to
            // keep as candidates; offering them only now guarantees a fresh small/main
            // candidate (an eligible TTL victim included) is always reached first. The
            // store re-checks TTL and either evicts (if a TTL has since been attached)
            // or re-registers again; its distinct-key skip set bounds the cycle.
            if let Some(e) = self.reoffer.pop_front() {
                return Some((e.db, e.key));
            }

            // All three queues empty: nothing to evict.
            return None;
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
        self.reoffer.push_back(Entry {
            db,
            key: key.to_vec().into_boxed_slice(),
            freq: 0,
        });
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
        let hot_present = p
            .small
            .iter()
            .chain(p.main.iter())
            .any(|e| e.key.as_ref() == b"hot");
        assert!(hot_present, "the hot key must survive the cold eviction");
        // The cold key is gone from both queues.
        assert!(
            !p.small
                .iter()
                .chain(p.main.iter())
                .any(|e| e.key.as_ref() == b"cold"),
            "the cold key must be evicted"
        );
        // A subsequent eviction (with no further accesses) eventually frees the hot
        // key too, demonstrating the second-chance drain terminates.
        let next = victim_key(&mut p);
        assert_eq!(next, Some(b"hot".to_vec()));
        assert!(p.small.is_empty() && p.main.is_empty());
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
        assert!(p.main.iter().any(|e| e.key.as_ref() == b"k"));
        assert!(p.small.is_empty());
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
        assert!(p.small.is_empty() && p.main.is_empty());
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
        let count = p
            .small
            .iter()
            .chain(p.main.iter())
            .chain(p.reoffer.iter())
            .filter(|e| e.matches(0, b"x"))
            .count();
        assert_eq!(count, 1, "re_register must not duplicate a tracked key");
    }

    #[test]
    fn fingerprint_is_stable_and_db_sensitive() {
        // Deterministic across calls (no OS entropy) and distinguishes db.
        assert_eq!(S3Fifo::fingerprint(0, b"k"), S3Fifo::fingerprint(0, b"k"));
        assert_ne!(S3Fifo::fingerprint(0, b"k"), S3Fifo::fingerprint(1, b"k"));
        assert_ne!(S3Fifo::fingerprint(0, b"k"), S3Fifo::fingerprint(0, b"j"));
    }
}
