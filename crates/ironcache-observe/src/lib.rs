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
    evicted_keys: u64,
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

    /// Record `n` keys evicted to honor the memory ceiling (PR-3a; INFO
    /// `evicted_keys`). Called by the dispatch admission path after `evict_to_fit`.
    pub fn on_evicted(&mut self, n: u64) {
        self.evicted_keys += n;
    }

    /// Take an immutable snapshot for rollup.
    #[must_use]
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            connections_received: self.connections_received,
            commands_processed: self.commands_processed,
            connected_clients: self.connected_clients,
            evicted_keys: self.evicted_keys,
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
    /// Total keys evicted by this shard to honor the memory ceiling (INFO
    /// `evicted_keys`, PR-3a).
    pub evicted_keys: u64,
}

impl CounterSnapshot {
    /// Fold another snapshot into this one (the rollup operation).
    #[must_use]
    pub fn merge(self, other: CounterSnapshot) -> CounterSnapshot {
        CounterSnapshot {
            connections_received: self.connections_received + other.connections_received,
            commands_processed: self.commands_processed + other.commands_processed,
            connected_clients: self.connected_clients + other.connected_clients,
            evicted_keys: self.evicted_keys + other.evicted_keys,
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
    /// The resolved memory ceiling in bytes, reported in the INFO `memory`
    /// section's `maxmemory` field. `0` means unlimited.
    pub maxmemory: u64,
    /// The configured eviction policy name (one of the eight Redis
    /// `maxmemory-policy` names), reported in the INFO `memory` section's
    /// `maxmemory_policy` field. Static after boot in PR-3a (the CONFIG SET runtime
    /// switch is deferred to 3c).
    pub maxmemory_policy: &'static str,
    /// The name of the global allocator actually selected at build time
    /// (`jemalloc` or `system`), reported as INFO `mem_allocator`. Derived from
    /// the same cfg that picks the `#[global_allocator]`, so INFO never claims
    /// jemalloc on a build that linked the system allocator.
    pub mem_allocator: &'static str,
}

/// A memory snapshot for the INFO `memory` section (ADR-0006, OBSERVABILITY.md).
///
/// These are the PROCESS-GLOBAL allocator figures (jemalloc `stats.allocated` /
/// `stats.resident`), read ONCE by the caller on the shard serving INFO. They are
/// distinct from the per-shard logical-byte counter (`Store::used_memory`, the fast
/// number PR-3's eviction budget checks): a process-global figure must NOT be
/// summed across shards or it would N-times over-count, so the caller passes one
/// already-read value here rather than a per-shard sum.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MemoryInfo {
    /// `used_memory`: the allocator-attributed live allocated total in bytes
    /// (the analog of Redis `used_memory`, ADR-0006).
    pub used_memory: u64,
    /// `used_memory_rss`: the resident set size in bytes (jemalloc
    /// `stats.resident`). May exceed `used_memory` under fragmentation.
    pub used_memory_rss: u64,
}

/// Build the `INFO` reply body (OBSERVABILITY.md). `section` is the optional
/// lowercased section filter (e.g. `server`); `None` or `"default"`/`"all"`
/// renders all sections.
///
/// `memory` carries the process-global allocator figures (ADR-0006), read once by
/// the caller; the `memory` section reports them for `used_memory`/`used_memory_rss`
/// and derives `used_memory_human` and `mem_fragmentation_ratio` (RSS/used) from
/// them.
///
/// The returned `String` is the raw INFO body; the caller wraps it as a bulk
/// string. Lines use `\r\n` and `field:value` exactly as Redis does so existing
/// parsers work.
#[must_use]
pub fn build_info<C: Clock>(
    clock: &C,
    server: &ServerInfo,
    rolled: CounterSnapshot,
    memory: MemoryInfo,
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
        // PR-2b: used_memory* are the PROCESS-GLOBAL jemalloc figures (ADR-0006),
        // read once by the caller and passed in. maxmemory and mem_allocator are
        // threaded from config. The per-shard logical-byte counter is a separate,
        // shard-local number (PR-3 eviction budget) and is NOT what used_memory
        // reports.
        let _ = write!(out, "used_memory:{}\r\n", memory.used_memory);
        let _ = write!(
            out,
            "used_memory_human:{}\r\n",
            human_bytes(memory.used_memory)
        );
        let _ = write!(out, "used_memory_rss:{}\r\n", memory.used_memory_rss);
        let _ = write!(out, "maxmemory:{}\r\n", server.maxmemory);
        // PR-3a: the CONFIGURED eviction policy name (ADR-0007 cache mode default is
        // allkeys-lru, NOT noeviction). Static after boot in 3a.
        let _ = write!(out, "maxmemory_policy:{}\r\n", server.maxmemory_policy);
        // mem_fragmentation_ratio = RSS / used (OBSERVABILITY.md); 0.00 when used is
        // 0 (avoid a divide-by-zero), matching the no-data startup case.
        let frag = if memory.used_memory > 0 {
            memory.used_memory_rss as f64 / memory.used_memory as f64
        } else {
            0.0
        };
        let _ = write!(out, "mem_fragmentation_ratio:{frag:.2}\r\n");
        let _ = write!(out, "mem_allocator:{}\r\n", server.mem_allocator);
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
        // PR-3a: the rolled-up evicted-keys total (bumped by the dispatch admission
        // path after evict_to_fit). expired_keys / keyspace_hits / misses are 3b.
        let _ = write!(out, "evicted_keys:{}\r\n", rolled.evicted_keys);
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

/// Render a byte count the way Redis's `bytesToHuman` does for `used_memory_human`:
/// `B`/`K`/`M`/`G` with two decimals above the byte scale (e.g. `1.00K`, `1.50M`),
/// and a plain integer with a `B` suffix below 1024 (e.g. `512B`). 1K = 1024 bytes
/// (binary), matching Redis. Deterministic and allocation-light (no float for the
/// byte case).
fn human_bytes(n: u64) -> String {
    const K: f64 = 1024.0;
    const M: f64 = 1024.0 * 1024.0;
    const G: f64 = 1024.0 * 1024.0 * 1024.0;
    let f = n as f64;
    if f < K {
        format!("{n}B")
    } else if f < M {
        format!("{:.2}K", f / K)
    } else if f < G {
        format!("{:.2}M", f / M)
    } else {
        format!("{:.2}G", f / G)
    }
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
            maxmemory: 0,
            maxmemory_policy: "allkeys-lru",
            mem_allocator: "jemalloc",
        }
    }

    #[test]
    fn info_has_standard_sections_and_fields() {
        let env = TestEnv::new(1);
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            None,
        );
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
    fn info_memory_threads_maxmemory_and_allocator() {
        let env = TestEnv::new(1);
        let mut s = server();
        s.maxmemory = 256 * 1024 * 1024;
        s.mem_allocator = "system";
        let body = build_info(
            &env,
            &s,
            CounterSnapshot::default(),
            MemoryInfo::default(),
            Some("memory"),
        );
        assert!(
            body.contains(&format!("maxmemory:{}\r\n", 256 * 1024 * 1024)),
            "{body}"
        );
        assert!(body.contains("mem_allocator:system\r\n"), "{body}");
    }

    #[test]
    fn info_reports_configured_policy_and_evicted_keys() {
        // PR-3a: maxmemory_policy is the CONFIGURED name (not the old hardcoded
        // noeviction), and evicted_keys is the rolled-up counter.
        let env = TestEnv::new(1);
        let mut s = server();
        s.maxmemory_policy = "volatile-ttl";
        let rolled = CounterSnapshot {
            evicted_keys: 7,
            ..Default::default()
        };
        let body = build_info(&env, &s, rolled, MemoryInfo::default(), None);
        assert!(body.contains("maxmemory_policy:volatile-ttl\r\n"), "{body}");
        assert!(!body.contains("maxmemory_policy:noeviction\r\n"), "{body}");
        assert!(body.contains("evicted_keys:7\r\n"), "{body}");
    }

    #[test]
    fn info_memory_reports_used_memory_and_frag_ratio() {
        // The process-global figures are reported verbatim, human-rendered, and the
        // fragmentation ratio is RSS/used.
        let env = TestEnv::new(1);
        let mem = MemoryInfo {
            used_memory: 2 * 1024 * 1024,     // 2 MiB
            used_memory_rss: 3 * 1024 * 1024, // 3 MiB
        };
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            mem,
            Some("memory"),
        );
        assert!(
            body.contains(&format!("used_memory:{}\r\n", 2 * 1024 * 1024)),
            "{body}"
        );
        assert!(body.contains("used_memory_human:2.00M\r\n"), "{body}");
        assert!(
            body.contains(&format!("used_memory_rss:{}\r\n", 3 * 1024 * 1024)),
            "{body}"
        );
        // 3 MiB / 2 MiB = 1.50.
        assert!(body.contains("mem_fragmentation_ratio:1.50\r\n"), "{body}");
    }

    #[test]
    fn info_memory_zero_used_has_no_divide_by_zero() {
        let env = TestEnv::new(1);
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            Some("memory"),
        );
        assert!(body.contains("used_memory:0\r\n"), "{body}");
        assert!(body.contains("used_memory_human:0B\r\n"), "{body}");
        assert!(body.contains("mem_fragmentation_ratio:0.00\r\n"), "{body}");
    }

    #[test]
    fn human_bytes_renders_like_redis() {
        assert_eq!(human_bytes(0), "0B");
        assert_eq!(human_bytes(512), "512B");
        assert_eq!(human_bytes(1024), "1.00K");
        assert_eq!(human_bytes(1536), "1.50K");
        assert_eq!(human_bytes(1024 * 1024), "1.00M");
        assert_eq!(human_bytes(1024 * 1024 * 1024), "1.00G");
    }

    #[test]
    fn info_section_filter() {
        let env = TestEnv::new(1);
        let only_server = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            Some("server"),
        );
        assert!(only_server.contains("# Server\r\n"));
        assert!(!only_server.contains("# Memory\r\n"));
    }

    #[test]
    fn info_uptime_uses_clock() {
        let mut env = TestEnv::new(1);
        env.advance(Duration::from_secs(90));
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            Some("server"),
        );
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
