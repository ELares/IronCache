// SPDX-License-Identifier: MIT OR Apache-2.0
//! #557 (M2 config safety): `ConfigOverlay::from_env` must FAIL on an unknown `IRONCACHE_*`
//! environment variable, naming the offending key, matching the strict `deny_unknown_fields` TOML
//! posture (a typo'd knob is never silently ignored).
//!
//! This lives in its OWN integration binary with a SINGLE test so the `IRONCACHE_*` env mutation
//! cannot race any other test reading the environment in parallel (the same isolation the
//! socket-activation `LISTEN_*` test uses). The whole round-trip runs sequentially inside the one
//! test: set the typo, assert boot fails, clear it, assert the default path is clean again.

use ironcache_config::ConfigOverlay;

/// Restores the environment on drop so a panic mid-test cannot leak the typo'd var into any later
/// process reuse.
struct EnvGuard;
impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: single-threaded test binary; no other thread reads the env concurrently.
        unsafe {
            std::env::remove_var("IRONCACHE_MAXCLIENT");
        }
    }
}

#[test]
fn from_env_rejects_unknown_ironcache_var_naming_the_key() {
    let _restore = EnvGuard;

    // A typo of `IRONCACHE_MAXCLIENTS` (dropped trailing S) that the fixed reader would otherwise
    // silently ignore -- keeping the default ceiling despite the operator's intent.
    // SAFETY: single-threaded test binary; set before any env read below.
    unsafe {
        std::env::set_var("IRONCACHE_MAXCLIENT", "512");
    }

    let err = ConfigOverlay::from_env()
        .expect_err("a typo'd IRONCACHE_* env var must fail config resolution, not be ignored");
    let msg = err.to_string();
    assert!(
        msg.contains("IRONCACHE_MAXCLIENT"),
        "the boot error must name the unknown key, got: {msg}"
    );
    assert!(
        msg.contains("did you mean IRONCACHE_MAXCLIENTS"),
        "the error should suggest the nearest valid key, got: {msg}"
    );

    // Clearing the typo restores the clean default path: from_env succeeds with no IRONCACHE_* set.
    // SAFETY: single-threaded test binary.
    unsafe {
        std::env::remove_var("IRONCACHE_MAXCLIENT");
    }
    ConfigOverlay::from_env()
        .expect("with no unknown IRONCACHE_* var set, the default env path must resolve cleanly");
}
