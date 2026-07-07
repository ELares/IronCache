// SPDX-License-Identifier: MIT OR Apache-2.0
//! #557 (M2 config safety): `ConfigOverlay::from_env` must NOT fail boot on an unknown `IRONCACHE_*`
//! environment variable. The environment is a namespace shared with the OS + orchestrator (this
//! repo's own driver-matrix harness exports `IRONCACHE_BIN`), so an unknown key is WARNED loudly
//! (with a nearest-key suggestion, surfaced via `tracing::warn!`) but boot proceeds. Only the config
//! FILE stays strict (`deny_unknown_fields`). This test asserts the boot-does-not-abort behavior.
//!
//! It lives in its OWN integration binary with a SINGLE test so the `IRONCACHE_*` env mutation
//! cannot race any other test reading the environment in parallel (the same isolation the
//! socket-activation `LISTEN_*` test uses). The round-trip runs sequentially inside the one test:
//! set an unknown var, assert boot still succeeds, clear it, assert the default path is clean.

use ironcache_config::ConfigOverlay;

/// Restores the environment on drop so a panic mid-test cannot leak the var into any later process
/// reuse.
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
fn from_env_warns_but_does_not_fail_on_unknown_ironcache_var() {
    let _restore = EnvGuard;

    // An unknown server-namespaced var: either a typo of `IRONCACHE_MAXCLIENTS` (dropped trailing S)
    // or, like the harness's `IRONCACHE_BIN`, an orchestrator variable in the shared namespace.
    // Either way it must not ABORT boot -- it is warned (nearest-key suggestion via tracing) and the
    // otherwise-valid config still resolves. Hard-failing here would break any deployment whose
    // environment carries an `IRONCACHE_*` var the server does not own.
    // SAFETY: single-threaded test binary; set before any env read below.
    unsafe {
        std::env::set_var("IRONCACHE_MAXCLIENT", "512");
    }

    ConfigOverlay::from_env().expect(
        "an unknown IRONCACHE_* env var must be warned, not abort boot (the env is a shared \
         namespace); only the config FILE stays strict",
    );

    // Clearing it leaves the clean default path: from_env still resolves with no IRONCACHE_* set.
    // SAFETY: single-threaded test binary.
    unsafe {
        std::env::remove_var("IRONCACHE_MAXCLIENT");
    }
    ConfigOverlay::from_env()
        .expect("with no unknown IRONCACHE_* var set, the default env path must resolve cleanly");
}
