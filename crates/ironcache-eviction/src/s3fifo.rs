// SPDX-License-Identifier: MIT OR Apache-2.0
//! The cache-mode eviction policy: zero-per-key-state batch-LFU over the store table.
//!
//! This serves the cache-mode `maxmemory-policy` names (`allkeys-lru`, `volatile-lru`,
//! and the verbatim-name spellings `allkeys-lfu` / `volatile-lfu` / `volatile-ttl` that
//! `map_policy_name` routes here). The type is named [`S3Fifo`] for surface continuity
//! (the [`crate::Policy::S3Fifo`] variant and every call site keep working), but its
//! INTERNALS are no longer S3-FIFO queues: it holds NO per-key tracking state at all.
//!
//! ## Why the queues are gone (the memory win)
//!
//! The previous S3-FIFO engine stored every live key in three `VecDeque<(u32, Box<[u8]>)>`
//! FIFOs (small / main / re-offer) plus a ghost ring. A measured head-to-head showed that
//! per-key queue state cost ~49 B/key on the whole-process `used_memory` (a key copy + a
//! `Box` allocation + queue slack on top of the store's own blob), which LOST the memory
//! race to DragonflyDB even though IronCache's STORE alone already beat it. The 2-bit
//! access frequency that drove the S3-FIFO promote/second-chance decision already lives ON
//! each stored object (the freq-in-object work: `Entry::freq()` / `bump_freq()`, bumped by
//! the store inline on every read). So the policy needs NO separate per-key structure: when
//! the shard is over budget the STORE scans its own table for the lowest-frequency entries
//! and evicts those first (exact LFU). This policy therefore carries only its posture (the
//! configured name, the volatile-only flag). Its `on_access`/`on_insert`/
//! `on_remove` are no-ops and its `select_victim` returns `None`; the store drives eviction
//! through [`crate::VictimStrategy::TableScanLowestFreq`], NOT through `select_victim`.
//!
//! This SUPERSEDES the S3-FIFO scan-resistance of ADR-0008 with exact-LFU-over-2-bit-freq,
//! which is the redis / Dragonfly-class low-memory approach: frequency-aware (a key read
//! more often has a higher freq and survives a cold one-hit-wonder) with zero per-key
//! tracking overhead. The 10/90 small-main split, the ghost ring, and the FIFO recency
//! ordering are gone; the surviving signal is the in-object frequency.
//!
//! ## Determinism (ADR-0003)
//!
//! The eviction ORDER is the store's deterministic `scan_hash` order (the tie-break among
//! equal-frequency candidates), with no RNG and no wall-clock. Two shards with identical
//! keyspaces and the same access history evict the same keys.
//!
//! ## Volatile-only + name round-trip (preserved)
//!
//! `volatile_only` still restricts victims to TTL-bearing keys; the STORE enforces it in
//! `evict_to_fit` (it has the kvobj and reads `expire_at`), exactly as before. The
//! CONFIGURED `maxmemory-policy` name still round-trips VERBATIM through [`policy_name`],
//! so `allkeys-lfu` / `volatile-ttl` echo unchanged for CONFIG GET / INFO even though the
//! engine that serves them is this batch-LFU engine (a documented victim-ordering
//! divergence, ADR-0009).

use ironcache_storage::{EvictionHook, VictimFreq};

use crate::{EvictionPolicy, VictimStrategy};

/// The cache-mode eviction policy (per shard, unsynchronized; ADR-0005).
///
/// Holds NO per-key state: only the posture (configured name, volatile-only flag). The
/// 2-bit access frequency lives ON each stored object, and the store scans its table for
/// the lowest-frequency victims, evicting exactly to fit
/// ([`crate::VictimStrategy::TableScanLowestFreq`]). The name [`S3Fifo`] is kept for
/// surface compatibility; the engine is batch-LFU, not S3-FIFO (see the module docs).
#[derive(Debug, Clone)]
pub struct S3Fifo {
    /// Whether victims are restricted to TTL-bearing keys (the volatile-* family),
    /// enforced by the store in `evict_to_fit` (it reads `expire_at`).
    volatile_only: bool,
    /// The CONFIGURED `maxmemory-policy` name this policy echoes VERBATIM from
    /// `policy_name()` (CONFIG GET / INFO). `map_policy_name` plants the exact configured
    /// spelling here (e.g. `allkeys-lfu`, `volatile-ttl`); `new` defaults it to the family
    /// name. The ENGINE is always batch-LFU ([`Self::engine_family`]); the NAME
    /// round-trips unchanged (ADR-0009).
    name: String,
}

impl S3Fifo {
    /// A fresh cache-mode policy. `volatile_only` selects the `volatile-*` restriction;
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

    /// A fresh cache-mode policy carrying the exact CONFIGURED policy name, returned
    /// verbatim by [`EvictionPolicy::policy_name`]. `map_policy_name` uses this so
    /// CONFIG GET / INFO round-trip the configured enum string (ADR-0009).
    #[must_use]
    pub fn with_name(volatile_only: bool, name: &str) -> Self {
        S3Fifo {
            volatile_only,
            name: name.to_owned(),
        }
    }

    /// The internal eviction ENGINE family label (the batch-LFU engine here). This is the
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
}

impl EvictionHook for S3Fifo {
    fn on_access(&mut self, _db: u32, _key: &[u8]) {
        // Zero per-key state: the 2-bit access frequency lives ON the stored object and the
        // STORE bumps it inline on the read path (it holds the entry). Nothing to do here.
    }

    fn on_insert(&mut self, _db: u32, _key: &[u8], _bytes: usize) {
        // Zero per-key state: a fresh entry's freq is seeded on the object by the store; the
        // policy keeps no roster, so an insert tracks nothing.
    }

    fn on_remove(&mut self, _db: u32, _key: &[u8], _bytes: usize) {
        // Zero per-key state: a delete/replace/expiry drops the in-object freq with the
        // entry; the policy keeps no roster, so a remove tracks nothing.
    }

    fn select_victim(&mut self, _freq: &mut dyn VictimFreq) -> Option<(u32, Box<[u8]>)> {
        // The cache-mode policy holds NO candidate roster, so it offers no victim through
        // this path. The store scans its table for the lowest-frequency entries directly
        // ([`crate::VictimStrategy::TableScanLowestFreq`]); `select_victim` is never
        // consulted for this policy. Kept as a `None`-returning no-op for the trait.
        None
    }
}

impl EvictionPolicy for S3Fifo {
    fn policy_name(&self) -> String {
        // The CONFIGURED name, returned VERBATIM (e.g. allkeys-lfu, volatile-ttl).
        // Redis round-trips the configured enum string unchanged for CONFIG GET/INFO;
        // the engine that serves it is batch-LFU ([`Self::engine_family`]), a documented
        // victim-ordering divergence (ADR-0009), but the NAME is honored.
        self.name.clone()
    }

    fn evicts(&self) -> bool {
        true
    }

    fn volatile_only(&self) -> bool {
        self.volatile_only
    }

    fn access_freq(&self, _db: u32, _key: &[u8]) -> Option<u8> {
        // This is NOT an LFU OBJECT FREQ engine: the 2-bit in-object freq drives the
        // batch-LFU victim choice, not a Redis OBJECT FREQ estimate. OBJECT FREQ requires
        // an LFU maxmemory policy, so this reports None and the dispatch layer emits the
        // LFU-gating error (matching Redis, which errors OBJECT FREQ unless the policy is
        // *-lfu; the *-lfu names map to the W-TinyLFU engine, which DOES return Some).
        None
    }

    fn re_register(&mut self, _db: u32, _key: &[u8]) {
        // The volatile-* re-eligibility fix (#46) is a NO-OP here: this policy keeps no
        // roster, so there is nothing to put a skipped key back into. The store's
        // table-scan path re-discovers every live key on the NEXT scan (a key that gains a
        // TTL via a later EXPIRE is found by the next over-budget pass), so a non-TTL key
        // the store declined to evict stays an eviction candidate automatically. Kept so
        // the store can call `re_register` uniformly across policies.
    }

    fn victim_strategy(&self) -> VictimStrategy {
        // Zero per-key state: the store scans its table for the lowest-frequency victims.
        VictimStrategy::TableScanLowestFreq
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A no-op [`VictimFreq`]: the cache-mode policy never consults it (it offers no
    /// victim through `select_victim`), so the tests feed it a stub.
    struct NoFreq;
    impl VictimFreq for NoFreq {
        fn get(&self, _db: u32, _key: &[u8]) -> Option<u8> {
            None
        }
        fn dec(&mut self, _db: u32, _key: &[u8]) {}
    }

    #[test]
    fn the_policy_holds_no_per_key_state() {
        // The whole point of the refactor: hooks track nothing, so they cannot grow any
        // per-key memory. Confirm the struct is the fixed posture-only size and the hooks
        // are inert.
        let mut p = S3Fifo::new(false);
        p.on_insert(0, b"a", 100);
        p.on_insert(0, b"b", 100);
        p.on_access(0, b"a");
        p.on_remove(0, b"b", 100);
        // No roster, so select_victim offers nothing (the store scans the table instead).
        assert_eq!(p.select_victim(&mut NoFreq), None);
        // re_register is a no-op (no roster to re-add into).
        p.re_register(0, b"a");
        assert_eq!(p.select_victim(&mut NoFreq), None);
    }

    #[test]
    fn victim_strategy_is_table_scan_lowest_freq() {
        assert_eq!(
            S3Fifo::new(false).victim_strategy(),
            VictimStrategy::TableScanLowestFreq
        );
        assert_eq!(
            S3Fifo::new(true).victim_strategy(),
            VictimStrategy::TableScanLowestFreq
        );
    }

    #[test]
    fn posture_and_name_round_trip() {
        let all = S3Fifo::new(false);
        assert_eq!(all.policy_name(), "allkeys-lru");
        assert_eq!(all.engine_family(), "allkeys-lru");
        assert!(all.evicts());
        assert!(!all.volatile_only());
        assert!(all.access_freq(0, b"k").is_none());

        let vol = S3Fifo::new(true);
        assert_eq!(vol.policy_name(), "volatile-lru");
        assert_eq!(vol.engine_family(), "volatile-lru");
        assert!(vol.volatile_only());

        // The CONFIGURED name is carried VERBATIM (the *-lfu / *-ttl spellings route here
        // and must echo unchanged for CONFIG GET / INFO).
        let lfu = S3Fifo::with_name(false, "allkeys-lfu");
        assert_eq!(lfu.policy_name(), "allkeys-lfu");
        let ttl = S3Fifo::with_name(true, "volatile-ttl");
        assert_eq!(ttl.policy_name(), "volatile-ttl");
        assert!(ttl.volatile_only());
    }
}
