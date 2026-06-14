// SPDX-License-Identifier: MIT OR Apache-2.0
//! Integration: the per-shard store driving a real `EvictionPolicy` through
//! evict-to-fit + the memory ceiling (PR-3a; EVICTION.md, ADMISSION.md, ADR-0007/8).
//!
//! These exercise the store's `evict_to_fit` against the bundled policies: cache mode
//! (S3-FIFO) frees memory under the budget; `noeviction` frees nothing; `volatile-*`
//! only frees TTL-bearing keys. The store is constructed with an explicit policy via
//! `with_hooks` (the binary's wiring path).

use ironcache_eviction::Policy;
use ironcache_storage::{
    Admit, ExpireWrite, NewValue, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};
use ironcache_store::ShardStore;

type PolicyStore = ShardStore<Policy, ironcache_storage::CountingAccounting>;

fn store_with(policy: Policy) -> PolicyStore {
    ShardStore::with_hooks(16, policy, ironcache_storage::CountingAccounting::new())
}

/// Blind-set a key with a 100-byte value (so the byte budget is easy to reason about)
/// and no TTL.
fn set(st: &mut PolicyStore, key: &[u8]) {
    let val = vec![b'v'; 100];
    st.upsert(
        0,
        key,
        NewValue::Bytes(&val),
        ExpireWrite::Clear,
        UnixMillis(0),
    );
}

/// Blind-set a key with a 100-byte value and a TTL deadline.
fn set_ttl(st: &mut PolicyStore, key: &[u8], deadline: u64) {
    let val = vec![b'v'; 100];
    st.upsert(
        0,
        key,
        NewValue::Bytes(&val),
        ExpireWrite::Set(UnixMillis(deadline)),
        UnixMillis(0),
    );
}

#[test]
fn cache_mode_evicts_to_fit_under_budget() {
    let mut st = store_with(Policy::cache_default());
    // Write well past a small budget. Each entry is ~101+ bytes (key + 100 value).
    for i in 0u32..50 {
        set(&mut st, format!("key{i}").as_bytes());
    }
    let before = st.used_memory();
    assert!(before > 1_000, "expected the writes to exceed the budget");

    // Evict to a 1000-byte budget: used_memory must drop to at or below it (strict
    // `>` over-limit semantics: eviction frees down to `used <= budget`), and at least
    // one entry must have been evicted.
    let budget = 1_000u64;
    let evicted = st.evict_to_fit(budget, UnixMillis(0));
    assert!(evicted > 0, "cache mode must evict to fit the budget");
    assert!(
        st.used_memory() <= budget,
        "used_memory {} not within budget {budget}",
        st.used_memory()
    );
}

#[test]
fn noeviction_frees_nothing_over_budget() {
    let mut st = store_with(Policy::NoEviction);
    for i in 0u32..10 {
        set(&mut st, format!("k{i}").as_bytes());
    }
    let before = st.used_memory();
    assert!(before > 500);
    // noeviction never selects a victim: evict_to_fit frees nothing and used_memory
    // is unchanged (the dispatch layer will reply -OOM instead).
    let evicted = st.evict_to_fit(100, UnixMillis(0));
    assert_eq!(evicted, 0, "noeviction must evict nothing");
    assert_eq!(st.used_memory(), before, "used_memory must be unchanged");
    assert!(
        !st.policy_evicts(),
        "noeviction policy_evicts() must be false"
    );
}

#[test]
fn cache_mode_under_budget_is_a_noop() {
    let mut st = store_with(Policy::cache_default());
    set(&mut st, b"a");
    let before = st.used_memory();
    // Budget far above usage: no eviction.
    let evicted = st.evict_to_fit(1_000_000, UnixMillis(0));
    assert_eq!(evicted, 0);
    assert_eq!(st.used_memory(), before);
}

#[test]
fn volatile_only_evicts_ttl_keys_and_spares_non_ttl_keys() {
    // volatile-* restricts victims to TTL-bearing keys. Mix TTL and non-TTL keys,
    // force eviction, and confirm only the TTL keys can be freed.
    let mut st = store_with(Policy::S3Fifo(
        // volatile-only S3-FIFO (the volatile-lru/lfu/ttl mapping).
        ironcache_eviction::S3Fifo::new(true),
    ));
    assert!(st.policy_volatile_only());
    // Two non-TTL keys and two TTL keys (deadline far in the future so they are live).
    set(&mut st, b"persist1");
    set(&mut st, b"persist2");
    set_ttl(&mut st, b"vol1", 1_000_000);
    set_ttl(&mut st, b"vol2", 1_000_000);

    // Budget that only the two non-TTL keys' worth of space would satisfy: the policy
    // can only free the two TTL keys, so used_memory cannot drop below the two
    // non-TTL keys' footprint. Drive eviction hard (budget 0 would try to free
    // everything, but only TTL keys are eligible).
    let _ = st.evict_to_fit(0, UnixMillis(0));

    // The TTL keys are gone; the non-TTL keys remain (volatile-* cannot evict them).
    assert!(
        st.read(0, b"vol1", UnixMillis(0)).is_none(),
        "vol1 (TTL) should be evicted"
    );
    assert!(
        st.read(0, b"vol2", UnixMillis(0)).is_none(),
        "vol2 (TTL) should be evicted"
    );
    assert!(
        st.read(0, b"persist1", UnixMillis(0)).is_some(),
        "persist1 (no TTL) must survive volatile-only eviction"
    );
    assert!(
        st.read(0, b"persist2", UnixMillis(0)).is_some(),
        "persist2 (no TTL) must survive volatile-only eviction"
    );
}

#[test]
fn volatile_only_with_no_ttl_keys_frees_nothing() {
    let mut st = store_with(Policy::S3Fifo(ironcache_eviction::S3Fifo::new(true)));
    set(&mut st, b"a");
    set(&mut st, b"b");
    let before = st.used_memory();
    // No TTL-bearing keys exist, so a volatile-only policy frees nothing (the
    // dispatch layer then replies -OOM, matching Redis volatile-* with no expirable
    // keys).
    let evicted = st.evict_to_fit(0, UnixMillis(0));
    assert_eq!(evicted, 0);
    assert_eq!(st.used_memory(), before);
}

/// Attach a TTL to an EXISTING key the way the 3b EXPIRE command does: an `rmw`
/// with `RmwAction::Keep` (value untouched) + `ExpireWrite::Set` (the new deadline).
/// This is NOT a rewrite (`upsert` would re-insert and so re-track the key through
/// the eviction `on_insert` funnel, masking the re-eligibility path); it sets only
/// the deadline, which is exactly the case #46 must handle.
fn expire_existing(st: &mut PolicyStore, key: &[u8], deadline: u64) {
    st.rmw(0, key, UnixMillis(0), |entry| {
        assert!(
            matches!(entry, RmwEntry::Occupied(_)),
            "expire_existing requires a live key"
        );
        RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Set(UnixMillis(deadline)),
            reply: (),
        }
    });
}

#[test]
fn volatile_re_eligibility_after_expire_attaches_a_ttl() {
    // The #46 fix: a non-TTL key the volatile-* policy could not evict (because it
    // lacks a TTL) must REMAIN an eviction candidate, so a later EXPIRE that attaches
    // a TTL makes it eligible. The PR-3a bug dropped such a key from the policy on the
    // first skip, so it could never be evicted again even after gaining a TTL.
    let mut st = store_with(Policy::S3Fifo(ironcache_eviction::S3Fifo::new(true)));
    assert!(st.policy_volatile_only());

    // All keys start WITHOUT a TTL. (No `read` between the phases: a read fires
    // `on_access`, which bumps the S3-FIFO frequency and would confound the proof with
    // frequency-promotion; the entry count is asserted instead to confirm survival.)
    set(&mut st, b"a");
    set(&mut st, b"b");
    set(&mut st, b"c");
    let before = st.used_memory();
    assert_eq!(st.len(), 3);
    assert!(before > 0);

    // Phase 1: with no TTL-bearing keys, a volatile-* policy frees NOTHING (it tries
    // each key, finds no TTL, RE-REGISTERS it, and the bounded scan stops). This both
    // proves the OOM-when-no-evictable-volatile-key behavior AND exercises the skip +
    // re-register path that #46 must make non-destructive. The PR-3a bug dropped each
    // skipped key from the policy here, so phase 2 could never evict it.
    let freed = st.evict_to_fit(0, UnixMillis(0));
    assert_eq!(freed, 0, "no TTL keys: volatile-only frees nothing");
    assert_eq!(st.used_memory(), before, "nothing was evicted");
    assert_eq!(st.len(), 3, "all keys survive (re-registered, NOT dropped)");

    // Phase 2: EXPIRE attaches a TTL to ONE previously-skipped key (the 3b EXPIRE
    // path: set the deadline without rewriting the value, so the eviction `on_insert`
    // funnel does NOT run). The #46 fix means this key is still tracked by the policy
    // from phase 1, so it is now an eligible victim. (b starts at frequency 0, so this
    // single access leaves it at 1, which is still evict-eligible.)
    expire_existing(&mut st, b"b", 1_000_000);

    // Now evict_to_fit CAN free that key (and only that key: a and c still lack a TTL).
    let freed2 = st.evict_to_fit(0, UnixMillis(0));
    assert!(
        freed2 >= 1,
        "after EXPIRE attaches a TTL, the key becomes evictable (the #46 fix)"
    );
    assert!(
        st.read(0, b"b", UnixMillis(0)).is_none(),
        "the EXPIRE'd key b must now be evictable"
    );
    // The still-non-TTL keys are spared.
    assert!(
        st.read(0, b"a", UnixMillis(0)).is_some(),
        "a (no TTL) must survive"
    );
    assert!(
        st.read(0, b"c", UnixMillis(0)).is_some(),
        "c (no TTL) must survive"
    );
}

#[test]
fn volatile_main_resident_ttl_victim_is_evicted_not_false_oom() {
    // The #2 fix (false -OOM under volatile-only): a TTL key promoted into MAIN, with
    // many non-TTL keys flooding the SMALL queue, must still be found and evicted rather
    // than the scan tripping a premature -OOM while an evictable volatile key exists.
    // The old consecutive-skip bound + re-register-into-small kept small over its ~10%
    // target and STARVED main, so the main-resident TTL victim was never reached; the
    // distinct-key bound + the lowest-priority re-offer queue reach it.
    let mut st = store_with(Policy::S3Fifo(ironcache_eviction::S3Fifo::new(true)));
    assert!(st.policy_volatile_only());

    // One TTL key, read several times so a later eviction PROMOTES it into MAIN (the
    // S3-FIFO small->main promotion fires when a high-frequency small entry is drawn).
    set_ttl(&mut st, b"vol", 1_000_000);
    for _ in 0..5 {
        assert!(st.read(0, b"vol", UnixMillis(0)).is_some());
    }
    // Many non-TTL keys flooding the small queue (well past small's ~10% target).
    for i in 0u32..30 {
        set(&mut st, format!("plain{i}").as_bytes());
    }
    let live_before = st.len();
    assert_eq!(live_before, 31, "1 TTL + 30 non-TTL keys");

    // A budget that requires freeing roughly one key: only the single TTL key is
    // eligible (volatile-only spares the 30 non-TTL keys). The scan must reach and evict
    // it without spinning or replying -OOM.
    let budget = st.used_memory() - 50; // forces at least one eviction
    let evicted = st.evict_to_fit(budget, UnixMillis(0));
    assert!(
        evicted >= 1,
        "the main-resident TTL victim must be evicted (no false -OOM)"
    );
    assert!(
        st.read(0, b"vol", UnixMillis(0)).is_none(),
        "the TTL key was the eligible victim and is gone"
    );
    // The non-TTL keys all survive (volatile-only cannot evict them); termination is
    // bounded (the test returning at all proves no infinite loop).
    for i in 0u32..30 {
        assert!(
            st.read(0, format!("plain{i}").as_bytes(), UnixMillis(0))
                .is_some(),
            "non-TTL key plain{i} must survive volatile-only eviction"
        );
    }
}

#[test]
fn random_policy_evicts_to_fit() {
    // The Random policy (allkeys-random) also frees memory to fit the budget.
    let mut st = store_with(Policy::Random(ironcache_eviction::Random::new(42, false)));
    for i in 0u32..30 {
        set(&mut st, format!("r{i}").as_bytes());
    }
    let budget = 500u64;
    let evicted = st.evict_to_fit(budget, UnixMillis(0));
    assert!(evicted > 0);
    assert!(st.used_memory() <= budget);
}

#[test]
fn evict_to_fit_is_deterministic_on_replay() {
    // A seeded Random policy + identical writes produce identical victims on replay
    // (the determinism contract, ADR-0003). We capture which keys survive.
    let run = || -> Vec<bool> {
        let mut st = store_with(Policy::Random(ironcache_eviction::Random::new(
            0xABCD, false,
        )));
        for i in 0u32..20 {
            set(&mut st, format!("d{i}").as_bytes());
        }
        st.evict_to_fit(700, UnixMillis(0));
        (0u32..20)
            .map(|i| {
                st.read(0, format!("d{i}").as_bytes(), UnixMillis(0))
                    .is_some()
            })
            .collect()
    };
    let a = run();
    let b = run();
    assert_eq!(a, b, "seeded eviction must replay identically");
    // Sanity: some keys survived and some were evicted (not all-or-nothing).
    assert!(a.iter().any(|&x| x));
    assert!(a.iter().any(|&x| !x));
}

#[test]
fn policy_name_and_accessors_reflect_the_configured_policy() {
    use ironcache_eviction::map_policy_name;

    assert_eq!(store_with(Policy::NoEviction).policy_name(), "noeviction");

    // The CONFIGURED name round-trips VERBATIM through the store's Admit::policy_name,
    // NOT the engine-family name: a server configured `allkeys-lfu` reports exactly
    // `allkeys-lfu` (safe for INFO / CONFIG GET), even though the engine that serves
    // it is FIFO-class (ADR-0009).
    assert_eq!(
        store_with(map_policy_name("allkeys-lfu", 1).unwrap()).policy_name(),
        "allkeys-lfu"
    );
    assert_eq!(
        store_with(map_policy_name("volatile-ttl", 1).unwrap()).policy_name(),
        "volatile-ttl"
    );
    // The family default still round-trips its own spelling.
    assert_eq!(
        store_with(Policy::cache_default()).policy_name(),
        "allkeys-lru"
    );

    let rnd = store_with(map_policy_name("allkeys-random", 1).unwrap());
    assert_eq!(rnd.policy_name(), "allkeys-random");
    assert!(rnd.policy_evicts());
    assert!(!rnd.policy_volatile_only());

    // A volatile policy reports its configured name and the volatile posture.
    let vol = store_with(map_policy_name("volatile-ttl", 1).unwrap());
    assert!(vol.policy_volatile_only());
}
