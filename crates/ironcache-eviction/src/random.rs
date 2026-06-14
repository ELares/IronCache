// SPDX-License-Identifier: MIT OR Apache-2.0
//! The random-victim eviction policy (`allkeys-random` / `volatile-random`, #50).
//!
//! Redis's `*-random` policies evict a uniformly-random key. Because
//! [`select_victim`](crate::EvictionPolicy::select_victim) is policy-only, the policy
//! keeps its own roster of live `(db, key)` pairs (populated by `on_insert`, pruned
//! by `on_remove`) and draws one at random through the determinism seam's RNG. The
//! `volatile_only` restriction (TTL-bearing keys only) is enforced by the store in
//! `evict_to_fit`, which skips a drawn victim that has no TTL; the policy itself does
//! not see TTL through the frozen hooks.

use ironcache_env::{Rng, SplitMix64};
use ironcache_storage::EvictionHook;

use crate::EvictionPolicy;

/// A uniformly-random victim policy. Holds a roster of live keys and a seeded RNG
/// (the determinism seam's [`SplitMix64`], ADR-0003: no std rand).
#[derive(Debug, Clone)]
pub struct Random {
    /// The live `(db, key)` roster. A drawn victim is picked uniformly from here.
    /// Swap-remove on `on_remove` keeps draws O(1) while the order stays a set, not
    /// a queue (random has no recency notion).
    keys: Vec<(u32, Box<[u8]>)>,
    /// The determinism-seam RNG (seeded by the binary from its Env).
    rng: SplitMix64,
    /// Whether victims are restricted to TTL-bearing keys (enforced by the store).
    volatile_only: bool,
}

impl Random {
    /// A random policy seeded with `seed` (the binary derives it from its Env clock /
    /// RNG, ADR-0003). `volatile_only` selects the `volatile-random` variant.
    #[must_use]
    pub fn new(seed: u64, volatile_only: bool) -> Self {
        Random {
            keys: Vec::new(),
            rng: SplitMix64::new(seed),
            volatile_only,
        }
    }

    /// The index of `(db, key)` in the roster, if present.
    fn position(&self, db: u32, key: &[u8]) -> Option<usize> {
        self.keys
            .iter()
            .position(|(d, k)| *d == db && k.as_ref() == key)
    }
}

impl EvictionHook for Random {
    fn on_access(&mut self, _db: u32, _key: &[u8]) {
        // Random eviction has no recency/frequency notion: an access changes nothing.
    }

    fn on_insert(&mut self, db: u32, key: &[u8], _bytes: usize) {
        // Idempotent: a replace (insert of an existing key) must not duplicate the
        // roster entry, or the same key could be drawn twice and skew the roster.
        if self.position(db, key).is_none() {
            self.keys.push((db, key.to_vec().into_boxed_slice()));
        }
    }

    fn on_remove(&mut self, db: u32, key: &[u8], _bytes: usize) {
        if let Some(i) = self.position(db, key) {
            // Swap-remove: order is irrelevant for random selection.
            self.keys.swap_remove(i);
        }
    }

    fn select_victim(&mut self) -> Option<(u32, Box<[u8]>)> {
        if self.keys.is_empty() {
            return None;
        }
        let n = self.keys.len() as u64;
        let i = self.rng.gen_below(n) as usize;
        // Clone the chosen key out; it stays in the roster until the store's delete
        // fires `on_remove`, which prunes it. Returning a clone keeps the roster
        // consistent if the store decides not to delete (e.g. volatile_only skip).
        let (db, key) = &self.keys[i];
        Some((*db, key.clone()))
    }
}

impl EvictionPolicy for Random {
    fn policy_name(&self) -> &'static str {
        if self.volatile_only {
            "volatile-random"
        } else {
            "allkeys-random"
        }
    }

    fn evicts(&self) -> bool {
        true
    }

    fn volatile_only(&self) -> bool {
        self.volatile_only
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert(p: &mut Random, key: &[u8]) {
        p.on_insert(0, key, key.len());
    }

    #[test]
    fn empty_roster_yields_no_victim() {
        let mut p = Random::new(1, false);
        assert_eq!(p.select_victim(), None);
    }

    #[test]
    fn deterministic_victim_sequence_under_a_fixed_seed() {
        // Same seed + same insert order => identical victim sequence on replay. This
        // is the determinism contract (ADR-0003): the policy never touches std rand.
        // A larger roster makes the draws sensitive to more of the RNG stream than the
        // top-bit-only reduction would expose over tiny bounds.
        let keys: Vec<Vec<u8>> = (0u8..32).map(|i| vec![b'k', i]).collect();
        let draw_sequence = |seed: u64| -> Vec<Box<[u8]>> {
            let mut p = Random::new(seed, false);
            for k in &keys {
                insert(&mut p, k);
            }
            let mut out = Vec::new();
            // Draw, then prune that key (simulating the store's delete) so the roster
            // shrinks like a real eviction run.
            for _ in 0..keys.len() {
                let (db, key) = p.select_victim().expect("victim while roster non-empty");
                p.on_remove(db, &key, key.len());
                out.push(key);
            }
            out
        };
        let a = draw_sequence(0xDEAD_BEEF);
        let b = draw_sequence(0xDEAD_BEEF);
        assert_eq!(a, b, "same seed must replay the same victim sequence");
        // A different seed changes the sequence over a roster this size (the top-bit
        // reduction collapses tiny bounds, but 32 keys exposes enough of the stream).
        let c = draw_sequence(0x1234_5678);
        assert_ne!(a, c, "a different seed should change the sequence");
        // Every inserted key is evicted exactly once (the run drains the roster).
        let mut sorted = a.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), keys.len(), "each key evicted exactly once");
    }

    #[test]
    fn remove_prunes_and_insert_is_idempotent() {
        let mut p = Random::new(1, false);
        insert(&mut p, b"x");
        insert(&mut p, b"x"); // replace: must not duplicate
        assert_eq!(p.keys.len(), 1);
        p.on_remove(0, b"x", 1);
        assert_eq!(p.keys.len(), 0);
        assert_eq!(p.select_victim(), None);
    }

    #[test]
    fn volatile_flag_drives_name_and_posture() {
        let all = Random::new(1, false);
        assert_eq!(all.policy_name(), "allkeys-random");
        assert!(!all.volatile_only());
        let vol = Random::new(1, true);
        assert_eq!(vol.policy_name(), "volatile-random");
        assert!(vol.volatile_only());
        assert!(vol.evicts());
    }
}
