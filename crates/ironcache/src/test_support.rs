// SPDX-License-Identifier: MIT OR Apache-2.0
//! Test-only boot helpers, so integration tests can stand up the REAL multi-shard
//! `run_server` (the SO_REUSEPORT thread-per-core topology + cross-shard coordinator)
//! on an ephemeral port without reaching into private internals or duplicating config
//! plumbing. Not part of the binary's runtime path.

use crate::serve::run_server;
use ironcache_config::Config;
use ironcache_runtime::bootstrap::ShardSet;

/// Boot a real server on `127.0.0.1:port` across `shards` shards and return the running
/// [`ShardSet`]. The caller keeps the handle alive for the server's lifetime and calls
/// [`ShardSet::shutdown_and_join`] at the end.
///
/// # Panics
///
/// Panics if the server fails to bind (e.g. the port is already in use), which a test
/// wants to surface immediately.
#[must_use]
pub fn run_server_for_test(port: u16, shards: usize) -> ShardSet {
    let config = Config {
        bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port,
        shards,
        databases: 16,
        ..Config::default()
    };
    run_server(&config).expect("test server failed to bind")
}
