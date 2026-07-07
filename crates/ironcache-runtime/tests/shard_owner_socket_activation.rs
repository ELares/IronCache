// SPDX-License-Identifier: MIT OR Apache-2.0
//! #517 PR3 review regression: shard-owner mode must REJECT systemd socket activation at boot.
//!
//! Socket activation (`LISTEN_FDS`, #389) hands the process ONE inherited listener fd for ONE port,
//! but shard-owner mode needs N DISTINCT self-bound ports. Without the guard, `run_shards` would
//! adopt the single inherited fd N times -- aliasing it (a multi-close-unsound double free on
//! shutdown) and leaving ports `base+1..` unbound. `run_shards` must fail boot loudly instead.
//!
//! This lives in its OWN integration binary with a SINGLE test so the `LISTEN_*` env mutation cannot
//! race any other test reading the environment in parallel. The guard fires BEFORE any bind/thread,
//! so the test never opens a socket.

use ironcache_runtime::TokioRuntime;
use ironcache_runtime::bootstrap::{ShardConfig, ShardId, run_shards};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// Restores (or clears) the `LISTEN_*` env on drop so a panic cannot leak activation state.
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
fn shard_owners_rejects_systemd_socket_activation() {
    let _restore = EnvGuard;
    // Present a valid activation env: LISTEN_PID must equal THIS process, LISTEN_FDS a positive count
    // (the fd itself is never touched -- the guard only checks the list is non-empty).
    // SAFETY: single-threaded test binary; set before any env read.
    unsafe {
        std::env::set_var("LISTEN_PID", std::process::id().to_string());
        std::env::set_var("LISTEN_FDS", "1");
    }

    let cfg = ShardConfig {
        shards: 2,
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6390),
        shard_owner_ports: true,
    };
    let serve = |_rt: TokioRuntime,
                 _s: tokio::net::TcpStream,
                 _shard: ShardId,
                 _sd: Arc<AtomicBool>| async {};
    let drain = |_idx: usize, _inbox: (), _shutdown: Arc<AtomicBool>| async {};

    let result = run_shards(&cfg, serve, vec![(), ()], drain);
    assert!(
        result.is_err(),
        "shard-owners + socket activation must fail boot, not adopt one fd N times"
    );
    let msg = result.err().unwrap().to_string();
    assert!(
        msg.contains("socket activation"),
        "the boot error should name the incompatibility, got: {msg}"
    );
}
