// SPDX-License-Identifier: MIT OR Apache-2.0
//! #527 (M3 config-rollback escape hatch): the `IRONCACHE_IGNORE_UNKNOWN_CONFIG_KEYS` env var must
//! parse into `ConfigOverlay::from_env` as the bootstrap leniency flag, so a rollback can flip the
//! hatch from the environment (the FILE path then boots past a forward-incompatible key with a WARN
//! instead of hard-failing). This lives in its OWN integration binary with a SINGLE test so the
//! `IRONCACHE_*` env mutation cannot race any other test reading the environment in parallel (the same
//! isolation the socket-activation `LISTEN_*` and unknown-env tests use).

use ironcache_config::ConfigOverlay;

/// Restores the environment on drop so a panic mid-test cannot leak the var into any later reuse.
struct EnvGuard;
impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: single-threaded test binary; no other thread reads the env concurrently.
        unsafe {
            std::env::remove_var("IRONCACHE_IGNORE_UNKNOWN_CONFIG_KEYS");
        }
    }
}

#[test]
fn ignore_unknown_config_keys_parses_from_env() {
    let _restore = EnvGuard;

    // SAFETY: single-threaded test binary; set before any env read below.
    unsafe {
        std::env::set_var("IRONCACHE_IGNORE_UNKNOWN_CONFIG_KEYS", "true");
    }
    let on = ConfigOverlay::from_env().expect("the escape-hatch env var must parse");
    assert_eq!(
        on.ignore_unknown_config_keys,
        Some(true),
        "IRONCACHE_IGNORE_UNKNOWN_CONFIG_KEYS=true must set the bootstrap flag"
    );

    // A garbage value hard-fails boot rather than silently leaving the hatch off.
    // SAFETY: single-threaded test binary.
    unsafe {
        std::env::set_var("IRONCACHE_IGNORE_UNKNOWN_CONFIG_KEYS", "banana");
    }
    assert!(
        ConfigOverlay::from_env().is_err(),
        "a non-boolean escape-hatch value must fail boot, not be silently ignored"
    );

    // Cleared, the default env path resolves cleanly with the flag unset.
    // SAFETY: single-threaded test binary.
    unsafe {
        std::env::remove_var("IRONCACHE_IGNORE_UNKNOWN_CONFIG_KEYS");
    }
    let unset = ConfigOverlay::from_env().expect("clean env path must resolve");
    assert_eq!(unset.ignore_unknown_config_keys, None);
}
