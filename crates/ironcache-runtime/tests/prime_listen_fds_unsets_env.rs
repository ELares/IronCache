// SPDX-License-Identifier: MIT OR Apache-2.0
//! #389: `prime_from_env_and_unset` captures the socket-activation environment ONCE and then UNSETS
//! `LISTEN_*` (the `sd_listen_fds(3)` `unset_environment` convention), while the captured snapshot
//! keeps feeding every later `from_env` consumer so clearing the env changes no adoption decision.
//!
//! This lives in its OWN integration binary with a SINGLE test: it both mutates the `LISTEN_*` env
//! AND primes the crate's process-wide activation snapshot (a `OnceLock`), so it must not run in the
//! same process as any other test that reads the environment or `from_env`.

use ironcache_runtime::listen_fds;

/// Clears any `LISTEN_*` the test set, so a panic before the in-test unset cannot leak activation
/// state to another binary's environment inheritance.
struct EnvGuard;
impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: single-threaded test binary; no other thread reads the env concurrently.
        unsafe {
            std::env::remove_var("LISTEN_PID");
            std::env::remove_var("LISTEN_FDS");
            std::env::remove_var("LISTEN_FDNAMES");
        }
    }
}

#[test]
fn prime_captures_the_snapshot_then_unsets_the_listen_env() {
    let _restore = EnvGuard;
    // Present a valid, NAMED single-socket activation: LISTEN_PID must equal THIS process, one fd,
    // named `resp` (the packaged `FileDescriptorName=resp`).
    // SAFETY: single-threaded test binary; set before any env read.
    unsafe {
        std::env::set_var("LISTEN_PID", std::process::id().to_string());
        std::env::set_var("LISTEN_FDS", "1");
        std::env::set_var("LISTEN_FDNAMES", "resp");
    }

    // Capture once, then clear the environment (the sd convention).
    listen_fds::prime_from_env_and_unset();

    // The LISTEN_* vars are gone: a later-exec'd child cannot inherit them and re-adopt our fds.
    assert!(
        std::env::var("LISTEN_PID").is_err(),
        "LISTEN_PID must be unset after prime"
    );
    assert!(
        std::env::var("LISTEN_FDS").is_err(),
        "LISTEN_FDS must be unset after prime"
    );
    assert!(
        std::env::var("LISTEN_FDNAMES").is_err(),
        "LISTEN_FDNAMES must be unset after prime"
    );

    // The activation decision SURVIVES the unset: `from_env` returns the primed snapshot, not the
    // (now-empty) live environment. Without the snapshot this would be `Ok(empty)` and the RESP
    // listener would wrongly self-bind after the env was cleared.
    let fds = listen_fds::from_env().expect("primed snapshot parses");
    assert_eq!(fds.len(), 1, "one inherited fd, from the snapshot: {fds:?}");
    assert_eq!(fds[0].fd, listen_fds::SD_LISTEN_FDS_START, "fd 3");
    assert_eq!(fds[0].name.as_deref(), Some("resp"), "named resp");

    // And the RESP-listener selection resolves to that fd (fd 3, named resp).
    assert_eq!(
        listen_fds::resp_listener_fd(&fds).map(|f| f.fd),
        Some(listen_fds::SD_LISTEN_FDS_START),
        "resp_listener_fd picks the resp fd from the snapshot"
    );
}
