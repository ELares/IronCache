// SPDX-License-Identifier: MIT OR Apache-2.0
//! Pluggable eviction policies for IronCache (EVICTION.md #48/#50, ADR-0007/0008).
//!
//! This crate extends the reserved [`ironcache_storage::EvictionHook`] with the
//! [`EvictionPolicy`] trait (policy identity + posture) and bundles the three PR-3a
//! policies behind one enum-dispatched [`Policy`]:
//!
//! - [`Policy::NoEviction`] - strict datastore mode: never selects a victim, so a
//!   write at the ceiling is rejected `-OOM` (ADR-0007 opt-in).
//! - [`Policy::S3Fifo`] - the cache-mode default (ADR-0008): a small (~10%)
//!   probationary FIFO + a large (~90%) main FIFO + a ghost ring of recently-evicted
//!   key fingerprints, with a 2-bit frequency counter (`s3fifo-freq-counter-2bit-cap3`).
//! - [`Policy::Random`] - a uniformly-random victim drawn through the determinism
//!   seam's RNG (ADR-0003), the `allkeys-random`/`volatile-random` mapping.
//!
//! ## Where the 2-bit frequency lives (a documented PR-3a choice)
//!
//! EVICTION.md folds the S3-FIFO 2-bit frequency into the kvobj header
//! (`eviction_rank`), and the store DOES bump that field on every read/rmw access.
//! The eviction DECISION, however, runs entirely inside [`S3Fifo::select_victim`],
//! which is a policy-only method (the store cannot reach into the policy's queues to
//! make the promote-or-evict call). So for PR-3a the S3-FIFO policy keeps its OWN
//! bounded 2-bit frequency counter, keyed by the queued key, bumped in
//! [`EvictionHook::on_access`]. This is the "policy-side 2-bit counter map" the PR-3
//! brief explicitly permits, chosen because `select_victim` has no borrow of the
//! kvobj header. It is bounded: an entry exists only while the key is queued (small
//! or main), and it is dropped when the key leaves both queues. The store-side
//! `eviction_rank` bump is kept (it is the eventual single-source-of-truth once the
//! decision path can read it across the boundary, a #8/3c follow-up) and is harmless
//! today. The two never disagree on direction (both count accesses up to 3).
//!
//! ## Volatile-only victim restriction
//!
//! The `volatile-*` Redis policies restrict the victim set to keys that carry a TTL.
//! The frozen hook signatures do not pass TTL-presence to the policy, so the policy
//! cannot filter on its own. Instead [`EvictionPolicy::volatile_only`] is a posture
//! FLAG the store reads in `evict_to_fit`: a `volatile_only` policy's victims are
//! filtered there against `expire_at` (the store has the kvobj), and a victim with
//! no TTL is skipped rather than deleted. This keeps TTL knowledge where it lives
//! (the store) without threading TTL through the frozen hooks. `volatile-ttl`
//! nearest-expiry-first ordering lands with the timing wheel in 3b; for 3a it maps
//! to S3-FIFO over the volatile set (a documented divergence, ADR-0009).
//!
//! ## Determinism and shared-nothing (ADR-0002/0003/0005)
//!
//! No `std::time`/`Instant`/`SystemTime`/`rand` here: the [`Random`] policy draws
//! through [`ironcache_env::SplitMix64`] (the determinism seam's RNG type), seeded by
//! the binary from its `Env`. No `std::sync` locks/atomics and no interior mutability
//! beyond the policy's own `&mut self`: the policy is per-shard and unsynchronized.

#![forbid(unsafe_code)]

mod random;
mod s3fifo;

pub use random::Random;
pub use s3fifo::S3Fifo;

use ironcache_storage::EvictionHook;

/// The pluggable eviction policy contract (EVICTION.md #48). It EXTENDS the reserved
/// storage hook [`EvictionHook`] (the per-access/insert/remove callbacks and victim
/// selection) with the policy's IDENTITY and POSTURE:
///
/// - [`Self::policy_name`] - the Redis-recognized `maxmemory-policy` name this policy
///   echoes from `CONFIG GET`/INFO (one of the eight Redis names).
/// - [`Self::evicts`] - whether this policy ever frees memory (false only for
///   `NoEviction`); the dispatch layer uses it to choose evict-to-fit vs reply `-OOM`.
/// - [`Self::volatile_only`] - whether victims are restricted to TTL-bearing keys
///   (the `volatile-*` family); the store enforces it in `evict_to_fit`.
pub trait EvictionPolicy: EvictionHook {
    /// The Redis-recognized `maxmemory-policy` name (echoed by CONFIG GET/INFO).
    fn policy_name(&self) -> &'static str;
    /// Whether this policy frees memory at the ceiling (false only for NoEviction).
    fn evicts(&self) -> bool;
    /// Whether victims are restricted to keys carrying a TTL (the volatile-* family).
    fn volatile_only(&self) -> bool;
}

/// The bundled eviction policy, enum-dispatched (EVICTION.md "enum dispatch" option).
///
/// Enum dispatch (not a `dyn` object) keeps the policy monomorphized into the store
/// with no vtable on the access path, while still letting the binary pick the policy
/// at boot from config. The variants carry their own state (S3-FIFO's queues, the
/// Random RNG); `NoEviction` is stateless.
#[derive(Debug, Clone)]
pub enum Policy {
    /// Strict datastore mode: never evicts (ADR-0007 opt-in). Maps to `noeviction`.
    NoEviction,
    /// The cache-mode default S3-FIFO engine (ADR-0008). `volatile_only` restricts
    /// it to TTL-bearing keys (the `volatile-*` family).
    S3Fifo(S3Fifo),
    /// A uniformly-random victim through the determinism seam (ADR-0003). Maps to
    /// `allkeys-random`/`volatile-random`.
    Random(Random),
    // 3c: WTinyLfu - the W-TinyLFU-fronted admission filter (#49) is DEFERRED to a
    // later PR; it adds a per-shard 4-bit Count-Min sketch, out of scope for 3a.
}

impl Policy {
    /// The default cache-mode policy (ADR-0007/0008): S3-FIFO over all keys, echoing
    /// the Redis name `allkeys-lru` (the typical cache default; the FIFO-class engine
    /// serves the named family with a documented victim-ordering divergence,
    /// ADR-0009). This is the policy a zero-config boot uses.
    #[must_use]
    pub fn cache_default() -> Self {
        Policy::S3Fifo(S3Fifo::new(false))
    }
}

impl EvictionHook for Policy {
    #[inline]
    fn on_access(&mut self, db: u32, key: &[u8]) {
        match self {
            Policy::NoEviction => {}
            Policy::S3Fifo(p) => p.on_access(db, key),
            Policy::Random(p) => p.on_access(db, key),
        }
    }

    #[inline]
    fn on_insert(&mut self, db: u32, key: &[u8], bytes: usize) {
        match self {
            Policy::NoEviction => {}
            Policy::S3Fifo(p) => p.on_insert(db, key, bytes),
            Policy::Random(p) => p.on_insert(db, key, bytes),
        }
    }

    #[inline]
    fn on_remove(&mut self, db: u32, key: &[u8], bytes: usize) {
        match self {
            Policy::NoEviction => {}
            Policy::S3Fifo(p) => p.on_remove(db, key, bytes),
            Policy::Random(p) => p.on_remove(db, key, bytes),
        }
    }

    #[inline]
    fn select_victim(&mut self) -> Option<(u32, Box<[u8]>)> {
        match self {
            Policy::NoEviction => None,
            Policy::S3Fifo(p) => p.select_victim(),
            Policy::Random(p) => p.select_victim(),
        }
    }
}

impl EvictionPolicy for Policy {
    fn policy_name(&self) -> &'static str {
        match self {
            Policy::NoEviction => "noeviction",
            Policy::S3Fifo(p) => p.policy_name(),
            Policy::Random(p) => p.policy_name(),
        }
    }

    fn evicts(&self) -> bool {
        !matches!(self, Policy::NoEviction)
    }

    fn volatile_only(&self) -> bool {
        match self {
            Policy::NoEviction => false,
            Policy::S3Fifo(p) => p.volatile_only(),
            Policy::Random(p) => p.volatile_only(),
        }
    }
}

// ---------------------------------------------------------------------------
// Redis maxmemory-policy name mapping (#50, EVICTION.md "Redis policy-name mapping").
// ---------------------------------------------------------------------------

/// The eight Redis `maxmemory-policy` names IronCache accepts at config validation
/// (CONFIG.md / EVICTION.md). Unknown names are rejected; `maxmemory-samples` (a
/// no-op under the FIFO-class engine) is handled at the CONFIG layer, not here.
pub const REDIS_POLICY_NAMES: [&str; 8] = [
    "noeviction",
    "allkeys-lru",
    "allkeys-lfu",
    "allkeys-random",
    "volatile-lru",
    "volatile-lfu",
    "volatile-random",
    "volatile-ttl",
];

/// Whether `name` is one of the eight Redis `maxmemory-policy` names (used by config
/// validation, CONFIG.md). Case-insensitive (Redis accepts any case).
#[must_use]
pub fn is_valid_policy_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    REDIS_POLICY_NAMES.contains(&lower.as_str())
}

/// Map a Redis `maxmemory-policy` name to a constructed [`Policy`] (#50), with the
/// `Random` policy seeded from `rng_seed` (the binary derives this from its `Env`,
/// ADR-0003). Returns `None` for an unrecognized name (config validation rejects it).
///
/// Mapping (EVICTION.md / ADR-0009):
/// - `noeviction` -> [`Policy::NoEviction`] (strict datastore mode).
/// - `allkeys-lru` / `allkeys-lfu` -> S3-FIFO over all keys. The `*-lfu` name is
///   SERVED by the FIFO-class engine: the name is accepted and echoed, but the
///   victim ordering is S3-FIFO's, not Redis's sampled LFU (a documented
///   default-behavior divergence, ADR-0009).
/// - `allkeys-random` -> [`Policy::Random`] over all keys.
/// - `volatile-lru` / `volatile-lfu` / `volatile-ttl` -> S3-FIFO restricted to
///   TTL-bearing keys. `volatile-ttl` nearest-expiry-first ordering lands with the
///   timing wheel in 3b; for 3a it maps to S3-FIFO `volatile_only` with that
///   documented note (ADR-0009).
/// - `volatile-random` -> [`Policy::Random`] restricted to TTL-bearing keys.
#[must_use]
pub fn map_policy_name(name: &str, rng_seed: u64) -> Option<Policy> {
    let lower = name.to_ascii_lowercase();
    let policy = match lower.as_str() {
        "noeviction" => Policy::NoEviction,
        "allkeys-lru" | "allkeys-lfu" => Policy::S3Fifo(S3Fifo::new(false)),
        "allkeys-random" => Policy::Random(Random::new(rng_seed, false)),
        "volatile-lru" | "volatile-lfu" | "volatile-ttl" => Policy::S3Fifo(S3Fifo::new(true)),
        "volatile-random" => Policy::Random(Random::new(rng_seed, true)),
        _ => return None,
    };
    Some(policy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_validation_accepts_the_eight_and_rejects_others() {
        for n in REDIS_POLICY_NAMES {
            assert!(is_valid_policy_name(n), "{n} should be valid");
            // Case-insensitive.
            assert!(is_valid_policy_name(&n.to_uppercase()));
        }
        for bad in [
            "",
            "lru",
            "allkeys",
            "allkeys-ttl",
            "maxmemory-samples",
            "bogus",
        ] {
            assert!(!is_valid_policy_name(bad), "{bad} should be invalid");
        }
    }

    #[test]
    fn mapping_covers_all_eight_with_correct_posture() {
        let cases: &[(&str, &str, bool, bool)] = &[
            // name, policy_name echoed, evicts, volatile_only
            ("noeviction", "noeviction", false, false),
            ("allkeys-lru", "allkeys-lru", true, false),
            ("allkeys-lfu", "allkeys-lru", true, false),
            ("allkeys-random", "allkeys-random", true, false),
            ("volatile-lru", "volatile-lru", true, true),
            ("volatile-lfu", "volatile-lru", true, true),
            ("volatile-random", "volatile-random", true, true),
            ("volatile-ttl", "volatile-lru", true, true),
        ];
        for (name, _echo, evicts, vol) in cases {
            let p = map_policy_name(name, 1).unwrap_or_else(|| panic!("{name} should map"));
            assert_eq!(p.evicts(), *evicts, "{name} evicts()");
            assert_eq!(p.volatile_only(), *vol, "{name} volatile_only()");
        }
        // An unknown name maps to None.
        assert!(map_policy_name("bogus", 1).is_none());
    }

    #[test]
    fn policy_name_echo_is_redis_recognized() {
        // The echoed name must always be one of the eight Redis names (so CONFIG GET
        // round-trips a recognized value), even where the engine diverges (lfu/ttl).
        for name in REDIS_POLICY_NAMES {
            let p = map_policy_name(name, 7).unwrap();
            assert!(
                is_valid_policy_name(p.policy_name()),
                "{name} echoed non-Redis name {}",
                p.policy_name()
            );
        }
    }

    #[test]
    fn cache_default_is_eviction_on_with_a_redis_name() {
        // ADR-0007: the cache-mode default must be eviction-ON with a Redis name,
        // NOT noeviction.
        let p = Policy::cache_default();
        assert!(p.evicts());
        assert!(!p.volatile_only());
        assert!(is_valid_policy_name(p.policy_name()));
        assert_ne!(p.policy_name(), "noeviction");
    }
}
