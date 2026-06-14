// SPDX-License-Identifier: MIT OR Apache-2.0
//! Minimal observability scaffold for IronCache (OBSERVABILITY.md, #86/#152).
//!
//! PR-1 ships the SHAPE of `INFO`, not the full field catalog: the standard
//! sections (`server`, `clients`, `memory`, `stats`) with Redis-recognized field
//! names so `redis_exporter` and existing parsers do not choke, populated with
//! real values where trivial (version, uptime via the Env clock, tcp_port,
//! connected_clients) and zero/placeholder elsewhere. The native `# IronCache`
//! section and the Prometheus `/metrics` endpoint are later PRs.
//!
//! Counters are per-shard (shared-nothing, ADR-0002) and rolled up for INFO by
//! summing snapshots; there is no shared atomic on the hot path.

use ironcache_env::Clock;

/// The IronCache server version reported in `INFO` and `HELLO`. Sourced from the
/// crate version at build time.
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Per-shard counters. Each shard owns one of these and mutates it with no
/// synchronization (it is core-local). For INFO, the server collects a
/// [`CounterSnapshot`] from each shard and sums them with [`CounterSnapshot::add`].
#[derive(Debug, Default)]
pub struct ShardCounters {
    connections_received: u64,
    commands_processed: u64,
    connected_clients: u64,
}

impl ShardCounters {
    /// A fresh zeroed counter set.
    #[must_use]
    pub fn new() -> Self {
        ShardCounters::default()
    }

    /// Record a newly accepted connection.
    pub fn on_connection_open(&mut self) {
        self.connections_received += 1;
        self.connected_clients += 1;
    }

    /// Record a closed connection.
    pub fn on_connection_close(&mut self) {
        self.connected_clients = self.connected_clients.saturating_sub(1);
    }

    /// Record a processed command.
    pub fn on_command(&mut self) {
        self.commands_processed += 1;
    }

    /// Take an immutable snapshot for rollup.
    #[must_use]
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            connections_received: self.connections_received,
            commands_processed: self.commands_processed,
            connected_clients: self.connected_clients,
        }
    }
}

/// An immutable, summable snapshot of one shard's counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CounterSnapshot {
    /// Total connections accepted by this shard since start.
    pub connections_received: u64,
    /// Total commands processed by this shard since start.
    pub commands_processed: u64,
    /// Currently-open connections on this shard.
    pub connected_clients: u64,
}

impl CounterSnapshot {
    /// Fold another snapshot into this one (the rollup operation).
    #[must_use]
    pub fn merge(self, other: CounterSnapshot) -> CounterSnapshot {
        CounterSnapshot {
            connections_received: self.connections_received + other.connections_received,
            commands_processed: self.commands_processed + other.commands_processed,
            connected_clients: self.connected_clients + other.connected_clients,
        }
    }
}

/// Immutable server facts needed to render INFO that do not change after boot.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    /// The bound TCP port.
    pub tcp_port: u16,
    /// The configured shard count (reported as IronCache's analog of io-threads).
    pub shards: usize,
    /// The process id.
    pub pid: u32,
    /// The monotonic instant captured at boot, for uptime.
    pub started_at: ironcache_env::Monotonic,
}

/// Build the `INFO` reply body (OBSERVABILITY.md). `section` is the optional
/// lowercased section filter (e.g. `server`); `None` or `"default"`/`"all"`
/// renders all sections.
///
/// The returned `String` is the raw INFO body; the caller wraps it as a bulk
/// string. Lines use `\r\n` and `field:value` exactly as Redis does so existing
/// parsers work.
#[must_use]
pub fn build_info<C: Clock>(
    clock: &C,
    server: &ServerInfo,
    rolled: CounterSnapshot,
    section: Option<&str>,
) -> String {
    // `write!` into a String never fails; the `let _ =` discards the Result.
    use core::fmt::Write as _;

    let want = |name: &str| match section {
        None => true,
        Some(s) => {
            let s = s.to_ascii_lowercase();
            s == "default" || s == "all" || s == "everything" || s == name
        }
    };

    let uptime_secs = clock
        .now()
        .saturating_duration_since(server.started_at)
        .as_secs();

    let mut out = String::new();
    if want("server") {
        out.push_str("# Server\r\n");
        out.push_str("redis_version:7.4.0\r\n"); // compatibility version tag for clients/exporters
        let _ = write!(out, "ironcache_version:{SERVER_VERSION}\r\n");
        out.push_str("redis_mode:standalone\r\n");
        let _ = write!(out, "os:{}\r\n", std::env::consts::OS);
        let _ = write!(out, "arch_bits:{}\r\n", usize::BITS);
        let _ = write!(out, "process_id:{}\r\n", server.pid);
        let _ = write!(out, "run_id:{}\r\n", run_id_placeholder());
        let _ = write!(out, "tcp_port:{}\r\n", server.tcp_port);
        let _ = write!(out, "uptime_in_seconds:{uptime_secs}\r\n");
        let _ = write!(out, "uptime_in_days:{}\r\n", uptime_secs / 86_400);
        let _ = write!(out, "io_threads_active:{}\r\n", server.shards);
        out.push_str("\r\n");
    }
    if want("clients") {
        out.push_str("# Clients\r\n");
        let _ = write!(out, "connected_clients:{}\r\n", rolled.connected_clients);
        out.push_str("cluster_connections:0\r\n");
        out.push_str("blocked_clients:0\r\n");
        out.push_str("\r\n");
    }
    if want("memory") {
        out.push_str("# Memory\r\n");
        // PR-1 has no allocator accounting yet (ADR-0006 lands with the store);
        // report zeros with the correct field names so exporters parse cleanly.
        out.push_str("used_memory:0\r\n");
        out.push_str("used_memory_human:0B\r\n");
        out.push_str("used_memory_rss:0\r\n");
        out.push_str("maxmemory:0\r\n");
        out.push_str("maxmemory_policy:noeviction\r\n");
        out.push_str("mem_fragmentation_ratio:0.00\r\n");
        out.push_str("mem_allocator:jemalloc\r\n");
        out.push_str("\r\n");
    }
    if want("stats") {
        out.push_str("# Stats\r\n");
        let _ = write!(
            out,
            "total_connections_received:{}\r\n",
            rolled.connections_received
        );
        let _ = write!(
            out,
            "total_commands_processed:{}\r\n",
            rolled.commands_processed
        );
        out.push_str("expired_keys:0\r\n");
        out.push_str("evicted_keys:0\r\n");
        out.push_str("keyspace_hits:0\r\n");
        out.push_str("keyspace_misses:0\r\n");
        out.push_str("\r\n");
    }
    out
}

/// A stable-per-process placeholder run id. The real 40-hex run id ships with the
/// observability registry (#152); a fixed placeholder is fine for PR-1 and keeps
/// the seam off the Env (no rand here).
fn run_id_placeholder() -> &'static str {
    "0000000000000000000000000000000000000000"
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_env::{Monotonic, TestEnv};
    use std::time::Duration;

    fn server() -> ServerInfo {
        ServerInfo {
            tcp_port: 6379,
            shards: 4,
            pid: 1234,
            started_at: Monotonic::ZERO,
        }
    }

    #[test]
    fn info_has_standard_sections_and_fields() {
        let env = TestEnv::new(1);
        let body = build_info(&env, &server(), CounterSnapshot::default(), None);
        assert!(body.contains("# Server\r\n"));
        assert!(body.contains("# Clients\r\n"));
        assert!(body.contains("# Memory\r\n"));
        assert!(body.contains("# Stats\r\n"));
        assert!(body.contains("tcp_port:6379\r\n"));
        assert!(body.contains("connected_clients:0\r\n"));
        assert!(body.contains("mem_allocator:jemalloc\r\n"));
        assert!(body.contains(&format!("ironcache_version:{SERVER_VERSION}\r\n")));
    }

    #[test]
    fn info_section_filter() {
        let env = TestEnv::new(1);
        let only_server = build_info(&env, &server(), CounterSnapshot::default(), Some("server"));
        assert!(only_server.contains("# Server\r\n"));
        assert!(!only_server.contains("# Memory\r\n"));
    }

    #[test]
    fn info_uptime_uses_clock() {
        let mut env = TestEnv::new(1);
        env.advance(Duration::from_secs(90));
        let body = build_info(&env, &server(), CounterSnapshot::default(), Some("server"));
        assert!(body.contains("uptime_in_seconds:90\r\n"), "{body}");
    }

    #[test]
    fn counters_rollup() {
        let mut a = ShardCounters::new();
        a.on_connection_open();
        a.on_command();
        a.on_command();
        let mut b = ShardCounters::new();
        b.on_connection_open();
        b.on_connection_open();
        b.on_connection_close();
        let rolled = a.snapshot().merge(b.snapshot());
        assert_eq!(rolled.connections_received, 3);
        assert_eq!(rolled.commands_processed, 2);
        assert_eq!(rolled.connected_clients, 2); // a:1 + b:(2-1)
    }
}
