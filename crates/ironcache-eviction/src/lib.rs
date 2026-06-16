// SPDX-License-Identifier: MIT OR Apache-2.0
//! Pluggable eviction policies for IronCache (EVICTION.md #48/#50, ADR-0007/0008).
//!
//! This crate extends the reserved [`ironcache_storage::EvictionHook`] with the
//! [`EvictionPolicy`] trait (policy identity + posture) and bundles the eviction
//! policies behind one enum-dispatched [`Policy`]:
//!
//! - [`Policy::NoEviction`] - strict datastore mode: never selects a victim, so a
//!   write at the ceiling is rejected `-OOM` (ADR-0007 opt-in).
//! - [`Policy::S3Fifo`] - the cache-mode default. ZERO per-key state: the 2-bit access
//!   frequency lives ON each stored object (freq-in-object), so when over budget the
//!   STORE scans its table and evicts the lowest-frequency entries (exact batch-LFU,
//!   deterministic `scan_hash` tie-break). This SUPERSEDES the old S3-FIFO per-key
//!   queues (the type name is kept for surface continuity; see [`S3Fifo`] and
//!   [`VictimStrategy::TableScanLowestFreq`]). The per-key queues cost ~49 B/key on the
//!   whole-process `used_memory`, which lost the memory head-to-head; removing them is
//!   the win.
//! - [`Policy::Random`] - a uniformly-random victim drawn through the determinism
//!   seam's RNG (ADR-0003), the `allkeys-random`/`volatile-random` mapping.
//! - [`Policy::WTinyLfu`] - the selectable W-TinyLFU-fronted variant (#49,
//!   WTINYLFU.md): a 4-bit count-min frequency sketch (min-increment + periodic
//!   halving aging) over a recency victim FIFO, serving the `allkeys-lfu`/`volatile-lfu`
//!   names. PR-3c makes these names REAL W-TinyLFU (they mapped to S3-FIFO under a
//!   documented 3a divergence). The admission decision is wired as a frequency-ordered
//!   victim choice (evict the lowest-estimated-frequency resident), a documented
//!   divergence from full TinyLFU candidate-vs-victim admission (see [`WTinyLfu`]).
//!
//! ## Where the 2-bit frequency lives (freq-in-object) and who picks the victim
//!
//! The 2-bit access frequency lives ON each stored object (the kvobj's
//! `Entry::freq()`/`bump_freq()`), bumped by the STORE inline on the read path. The
//! cache-mode policy ([`S3Fifo`]) therefore holds ZERO per-key state: it does NOT keep a
//! roster, a counter, or any queues. When over budget the STORE scans its own table for
//! the lowest-frequency entries and evicts those first (exact batch-LFU). This is the
//! [`VictimStrategy::TableScanLowestFreq`] path; the policy's `select_victim` is never
//! consulted. The roster policies ([`Random`], [`WTinyLfu`]) still keep their own
//! candidate set and are driven through [`EvictionHook::select_victim`]
//! ([`VictimStrategy::Roster`]).
//!
//! Removing the cache-mode per-key queues is the memory win: they cost ~49 B/key on the
//! whole-process `used_memory` (a key copy + a `Box` alloc + queue slack on top of the
//! store's blob), which lost the memory head-to-head even though the store alone won.
//!
//! ## Volatile-only victim restriction
//!
//! The `volatile-*` Redis policies restrict the victim set to keys that carry a TTL.
//! [`EvictionPolicy::volatile_only`] is a posture FLAG the store reads in `evict_to_fit`,
//! where it can see `expire_at` (it has the kvobj). For the table-scan cache-mode path a
//! live non-TTL entry is simply NOT collected as a candidate during the pass (so it is
//! spared and stays eligible automatically once a later EXPIRE attaches a TTL: the next
//! over-budget scan re-discovers it). For the roster path a skipped non-TTL victim is
//! RE-REGISTERED into the policy ([`EvictionPolicy::re_register`], the #46 fix) so it
//! stays a candidate. Either way TTL knowledge stays where it lives (the store) without
//! threading TTL through the frozen hooks. The `volatile-ttl` nearest-expiry-first
//! ordering remains a documented divergence (it maps to the cache-mode engine over the
//! volatile set, ADR-0009).
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
mod wtinylfu;

pub use random::Random;
pub use s3fifo::S3Fifo;
pub use wtinylfu::{CmSketch, WTinyLfu};

use ironcache_storage::{EvictionHook, VictimFreq};

/// The pluggable eviction policy contract (EVICTION.md #48). It EXTENDS the reserved
/// storage hook [`EvictionHook`] (the per-access/insert/remove callbacks and victim
/// selection) with the policy's IDENTITY and POSTURE:
///
/// - [`Self::policy_name`] - the CONFIGURED `maxmemory-policy` name this policy echoes
///   VERBATIM from `CONFIG GET`/INFO (the exact configured enum string, ADR-0009).
/// - [`Self::evicts`] - whether this policy ever frees memory (false only for
///   `NoEviction`); the dispatch layer uses it to choose evict-to-fit vs reply `-OOM`.
/// - [`Self::volatile_only`] - whether victims are restricted to TTL-bearing keys
///   (the `volatile-*` family); the store enforces it in `evict_to_fit`.
pub trait EvictionPolicy: EvictionHook {
    /// The CONFIGURED `maxmemory-policy` name, returned VERBATIM (echoed by CONFIG
    /// GET / INFO). Redis round-trips the configured enum string unchanged (e.g. a
    /// server configured `allkeys-lfu` or `volatile-ttl` echoes exactly that, NOT a
    /// substituted engine-family name), so this returns the exact name the policy was
    /// built from. The engine that SERVES the name may diverge from Redis's (the
    /// FIFO-class engine serves `*-lfu`/`volatile-ttl`, ADR-0009); the NAME is still
    /// honored verbatim, which keeps INFO `maxmemory_policy` and CONFIG GET safe.
    fn policy_name(&self) -> String;
    /// Whether this policy frees memory at the ceiling (false only for NoEviction).
    fn evicts(&self) -> bool;
    /// Whether victims are restricted to keys carrying a TTL (the volatile-* family).
    fn volatile_only(&self) -> bool;

    /// How the store's `evict_to_fit` should SOURCE victims for this policy (the
    /// zero-per-key-state cache-mode refactor). The store branches on this:
    ///
    /// - [`VictimStrategy::None`] - the policy never evicts ([`Policy::NoEviction`]);
    ///   `evict_to_fit` frees nothing.
    /// - [`VictimStrategy::Roster`] - the policy keeps its OWN candidate roster and the
    ///   store drives it round-by-round through [`EvictionHook::select_victim`]
    ///   (`Random` draws uniformly; `WTinyLfu` runs the count-min victim search).
    /// - [`VictimStrategy::TableScanLowestFreq`] - the policy holds NO per-key state
    ///   ([`S3Fifo`], the cache-mode default). The 2-bit access frequency lives ON each
    ///   stored object, so the store scans its table for the lowest-freq entries and
    ///   evicts those first (exact LFU; deterministic `scan_hash` tie-break, ADR-0003).
    ///   `select_victim` is never consulted for this strategy.
    ///
    /// Default: `Roster` (the pre-refactor behavior for any policy that still keeps a
    /// roster), so a new roster policy needs no override.
    fn victim_strategy(&self) -> VictimStrategy {
        VictimStrategy::Roster
    }

    /// The access-frequency estimate for `(db, key)` for OBJECT FREQ, or `None` if
    /// this policy keeps no frequency estimate (every NON-LFU policy). Only the
    /// W-TinyLFU LFU-family engine ([`Policy::WTinyLfu`]) returns `Some` (the 4-bit
    /// count-min sketch estimate, 0..=15); `noeviction`/`*-lru`/`*-random`/`*-ttl`
    /// return `None`. The dispatch layer maps `None` to the canonical OBJECT FREQ
    /// LFU-gating error (FREQ requires an LFU maxmemory policy). Additive: it is a
    /// read-only introspection accessor, NOT part of the frozen four `Store`
    /// primitives and NOT a hook the store fires on the hot path.
    fn access_freq(&self, db: u32, key: &[u8]) -> Option<u8>;

    /// Re-register a `(db, key)` the store could NOT evict back into the policy's
    /// tracking, NON-DESTRUCTIVELY (the volatile-* re-eligibility fix, #46).
    ///
    /// `select_victim` pop_front's a candidate OUT of its queue. When `evict_to_fit`
    /// cannot use that candidate because it carries no TTL under a `volatile_only`
    /// policy, it must NOT drop it (the PR-3a bug: a dropped non-TTL key could never
    /// be evicted again even after a later EXPIRE gave it a TTL). Instead the store
    /// calls this to put the key BACK into the policy so it remains an eviction
    /// candidate; a later EXPIRE that attaches a TTL then makes it eligible. Distinct
    /// from `on_insert` (no byte accounting, no ghost/recency churn): it is a pure
    /// re-track of an already-tracked key the store declined to delete.
    fn re_register(&mut self, db: u32, key: &[u8]);
}

/// How the store sources eviction victims for a policy (the zero-per-key-state
/// cache-mode refactor). Returned by [`EvictionPolicy::victim_strategy`]; the store's
/// `evict_to_fit` branches on it.
///
/// The cache-mode default ([`Policy::S3Fifo`]) is [`Self::TableScanLowestFreq`]: it
/// holds NO per-key tracking state, so the store scans its own table for the
/// lowest-frequency entries (the 2-bit freq lives ON each stored object). The roster
/// policies ([`Policy::Random`], [`Policy::WTinyLfu`]) keep their own candidate set and
/// are driven through [`EvictionHook::select_victim`] ([`Self::Roster`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VictimStrategy {
    /// The policy never evicts (`noeviction`); the store frees nothing.
    None,
    /// The policy keeps its own candidate roster; the store drives it through
    /// [`EvictionHook::select_victim`], one victim per round.
    Roster,
    /// The policy holds NO per-key state; the store scans its table and evicts the
    /// lowest-frequency live entries first (exact LFU over the in-object 2-bit freq,
    /// deterministic `scan_hash` tie-break, ADR-0003). `select_victim` is not consulted.
    TableScanLowestFreq,
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
    /// The cache-mode default: the zero-per-key-state batch-LFU engine. Holds NO
    /// per-key tracking; the store scans its table for the lowest-frequency victims
    /// (the 2-bit freq is on each object). `volatile_only` restricts it to TTL-bearing
    /// keys (the `volatile-*` family). The type is named [`S3Fifo`] for surface
    /// continuity; the engine is no longer S3-FIFO (see [`S3Fifo`]).
    S3Fifo(S3Fifo),
    /// A uniformly-random victim through the determinism seam (ADR-0003). Maps to
    /// `allkeys-random`/`volatile-random`.
    Random(Random),
    /// The selectable W-TinyLFU-fronted variant (#49, WTINYLFU.md): a 4-bit count-min
    /// frequency sketch over a recency victim FIFO. `volatile_only` restricts it to
    /// TTL-bearing keys (the `volatile-*` family). Serves `allkeys-lfu`/`volatile-lfu`.
    WTinyLfu(WTinyLfu),
}

impl Policy {
    /// The default cache-mode policy (ADR-0007): the zero-per-key-state batch-LFU engine
    /// over all keys, echoing the Redis name `allkeys-lru` (the typical cache default;
    /// the engine serves the named family with a documented victim-ordering divergence,
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
            Policy::WTinyLfu(p) => p.on_access(db, key),
        }
    }

    #[inline]
    fn on_insert(&mut self, db: u32, key: &[u8], bytes: usize) {
        match self {
            Policy::NoEviction => {}
            Policy::S3Fifo(p) => p.on_insert(db, key, bytes),
            Policy::Random(p) => p.on_insert(db, key, bytes),
            Policy::WTinyLfu(p) => p.on_insert(db, key, bytes),
        }
    }

    #[inline]
    fn on_remove(&mut self, db: u32, key: &[u8], bytes: usize) {
        match self {
            Policy::NoEviction => {}
            Policy::S3Fifo(p) => p.on_remove(db, key, bytes),
            Policy::Random(p) => p.on_remove(db, key, bytes),
            Policy::WTinyLfu(p) => p.on_remove(db, key, bytes),
        }
    }

    #[inline]
    fn select_victim(&mut self, freq: &mut dyn VictimFreq) -> Option<(u32, Box<[u8]>)> {
        match self {
            Policy::NoEviction => None,
            // Only S3-FIFO reads `freq` (the store-side object freq); Random/W-TinyLFU
            // keep their own (or no) frequency and ignore it.
            Policy::S3Fifo(p) => p.select_victim(freq),
            Policy::Random(p) => p.select_victim(freq),
            Policy::WTinyLfu(p) => p.select_victim(freq),
        }
    }
}

impl EvictionPolicy for Policy {
    fn policy_name(&self) -> String {
        match self {
            // `noeviction` has exactly one configured spelling, so the unit variant
            // carries no string; the other two echo their configured name verbatim.
            Policy::NoEviction => "noeviction".to_owned(),
            Policy::S3Fifo(p) => p.policy_name(),
            Policy::Random(p) => p.policy_name(),
            Policy::WTinyLfu(p) => p.policy_name(),
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
            Policy::WTinyLfu(p) => p.volatile_only(),
        }
    }

    fn access_freq(&self, db: u32, key: &[u8]) -> Option<u8> {
        match self {
            // Only the W-TinyLFU LFU engine keeps a frequency estimate (OBJECT FREQ);
            // every non-LFU policy returns None (the dispatch layer then emits the
            // OBJECT FREQ LFU-gating error).
            Policy::NoEviction => None,
            Policy::S3Fifo(p) => p.access_freq(db, key),
            Policy::Random(p) => p.access_freq(db, key),
            Policy::WTinyLfu(p) => p.access_freq(db, key),
        }
    }

    fn re_register(&mut self, db: u32, key: &[u8]) {
        match self {
            // NoEviction never offers a victim, so it is never asked to re-register.
            Policy::NoEviction => {}
            Policy::S3Fifo(p) => p.re_register(db, key),
            Policy::Random(p) => p.re_register(db, key),
            Policy::WTinyLfu(p) => p.re_register(db, key),
        }
    }

    fn victim_strategy(&self) -> VictimStrategy {
        match self {
            Policy::NoEviction => VictimStrategy::None,
            // The cache-mode default holds NO per-key state: the store scans its table
            // for the lowest-frequency victims (the 2-bit freq is on each object).
            Policy::S3Fifo(p) => p.victim_strategy(),
            // The roster policies keep their own candidate set; the store drives them
            // through select_victim.
            Policy::Random(p) => p.victim_strategy(),
            Policy::WTinyLfu(p) => p.victim_strategy(),
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
/// The constructed policy carries the configured name VERBATIM (the lowercased
/// spelling): [`EvictionPolicy::policy_name`] returns it unchanged for CONFIG GET /
/// INFO, so `allkeys-lfu` and `volatile-ttl` round-trip exactly even though the
/// engine that SERVES them diverges (a documented victim-ordering divergence,
/// ADR-0009; see [`S3Fifo::engine_family`] for the engine label).
///
/// Mapping (EVICTION.md / ADR-0009; the `*-lfu` rows are REAL W-TinyLFU, PR-3c):
/// - `noeviction` -> [`Policy::NoEviction`] (strict datastore mode).
/// - `allkeys-lru` -> the zero-per-key-state batch-LFU engine over all keys (the store
///   scans its table for the lowest-freq victims). The `*-lru` name is a documented
///   victim-ordering divergence (ADR-0009).
/// - `allkeys-lfu` -> [`Policy::WTinyLfu`] over all keys: the real W-TinyLFU-fronted
///   variant (#49). PR-3a mapped `*-lfu` to S3-FIFO as a documented stand-in; PR-3c
///   makes it the actual frequency-admission engine. The name still echoes verbatim.
/// - `allkeys-random` -> [`Policy::Random`] over all keys.
/// - `volatile-lru` / `volatile-ttl` -> the batch-LFU engine restricted to TTL-bearing
///   keys, each echoing its own configured name verbatim. `volatile-ttl`
///   nearest-expiry-first ordering is a documented divergence (it maps to the cache-mode
///   engine with `volatile_only`, ADR-0009).
/// - `volatile-lfu` -> [`Policy::WTinyLfu`] restricted to TTL-bearing keys: the real
///   W-TinyLFU variant over the volatile set (`volatile_only=true`).
/// - `volatile-random` -> [`Policy::Random`] restricted to TTL-bearing keys.
#[must_use]
pub fn map_policy_name(name: &str, rng_seed: u64) -> Option<Policy> {
    let lower = name.to_ascii_lowercase();
    let policy = match lower.as_str() {
        "noeviction" => Policy::NoEviction,
        "allkeys-lru" => Policy::S3Fifo(S3Fifo::with_name(false, &lower)),
        // `*-lfu` is now REAL W-TinyLFU (PR-3c): the 4-bit count-min frequency engine,
        // no longer the S3-FIFO 3a stand-in. The configured name still round-trips.
        "allkeys-lfu" => Policy::WTinyLfu(WTinyLfu::with_name(false, &lower)),
        "allkeys-random" => Policy::Random(Random::with_name(rng_seed, false, &lower)),
        "volatile-lru" | "volatile-ttl" => Policy::S3Fifo(S3Fifo::with_name(true, &lower)),
        "volatile-lfu" => Policy::WTinyLfu(WTinyLfu::with_name(true, &lower)),
        "volatile-random" => Policy::Random(Random::with_name(rng_seed, true, &lower)),
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
            // name, policy_name echoed VERBATIM, evicts, volatile_only
            ("noeviction", "noeviction", false, false),
            ("allkeys-lru", "allkeys-lru", true, false),
            ("allkeys-lfu", "allkeys-lfu", true, false),
            ("allkeys-random", "allkeys-random", true, false),
            ("volatile-lru", "volatile-lru", true, true),
            ("volatile-lfu", "volatile-lfu", true, true),
            ("volatile-random", "volatile-random", true, true),
            ("volatile-ttl", "volatile-ttl", true, true),
        ];
        for (name, echo, evicts, vol) in cases {
            let p = map_policy_name(name, 1).unwrap_or_else(|| panic!("{name} should map"));
            // The configured name round-trips VERBATIM (no engine-family substitution).
            assert_eq!(
                p.policy_name(),
                *echo,
                "{name} policy_name() must be verbatim"
            );
            assert_eq!(p.evicts(), *evicts, "{name} evicts()");
            assert_eq!(p.volatile_only(), *vol, "{name} volatile_only()");
        }
        // A configured name is echoed in its lowercased canonical spelling regardless
        // of input case (Redis accepts any case; CONFIG GET reports the enum string).
        assert_eq!(
            map_policy_name("AllKeys-LFU", 1).unwrap().policy_name(),
            "allkeys-lfu"
        );
        // An unknown name maps to None.
        assert!(map_policy_name("bogus", 1).is_none());
    }

    #[test]
    fn policy_name_echo_is_redis_recognized() {
        // The echoed name must always be one of the eight Redis names (so CONFIG GET
        // round-trips a recognized value), even where the engine diverges (lfu/ttl).
        for name in REDIS_POLICY_NAMES {
            let p = map_policy_name(name, 7).unwrap();
            let echoed = p.policy_name();
            assert!(
                is_valid_policy_name(&echoed),
                "{name} echoed non-Redis name {echoed}"
            );
            // And it is the configured name verbatim (round-trip).
            assert_eq!(echoed, name, "{name} must echo verbatim");
        }
    }

    #[test]
    fn lfu_names_map_to_real_wtinylfu_with_verbatim_name() {
        // PR-3c: `*-lfu` now maps to the REAL W-TinyLFU variant (not the 3a S3-FIFO
        // stand-in), echoing the configured name verbatim and carrying the right posture.
        let all = map_policy_name("allkeys-lfu", 1).expect("allkeys-lfu maps");
        assert!(
            matches!(all, Policy::WTinyLfu(_)),
            "allkeys-lfu must map to the W-TinyLFU variant"
        );
        assert_eq!(all.policy_name(), "allkeys-lfu");
        assert!(all.evicts());
        assert!(!all.volatile_only());

        let vol = map_policy_name("volatile-lfu", 1).expect("volatile-lfu maps");
        assert!(
            matches!(vol, Policy::WTinyLfu(_)),
            "volatile-lfu must map to the W-TinyLFU variant"
        );
        assert_eq!(vol.policy_name(), "volatile-lfu");
        assert!(vol.volatile_only());

        // Case-insensitive input still echoes the lowercased configured spelling.
        let ci = map_policy_name("AllKeys-LFU", 1).unwrap();
        assert!(matches!(ci, Policy::WTinyLfu(_)));
        assert_eq!(ci.policy_name(), "allkeys-lfu");

        // The non-lfu names keep their existing engines (regression guard).
        assert!(matches!(
            map_policy_name("allkeys-lru", 1).unwrap(),
            Policy::S3Fifo(_)
        ));
        assert!(matches!(
            map_policy_name("volatile-ttl", 1).unwrap(),
            Policy::S3Fifo(_)
        ));
        assert!(matches!(
            map_policy_name("allkeys-random", 1).unwrap(),
            Policy::Random(_)
        ));
    }

    #[test]
    fn cache_default_is_eviction_on_with_a_redis_name() {
        // ADR-0007: the cache-mode default must be eviction-ON with a Redis name,
        // NOT noeviction.
        let p = Policy::cache_default();
        assert!(p.evicts());
        assert!(!p.volatile_only());
        assert!(is_valid_policy_name(&p.policy_name()));
        assert_ne!(p.policy_name(), "noeviction");
    }
}
