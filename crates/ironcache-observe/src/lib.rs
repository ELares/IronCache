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
    /// Keys reclaimed because their TTL passed (INFO `expired_keys`, PR-3b). Bumped by
    /// the active timing-wheel drain AND the lazy expiry-on-read backstop.
    expired_keys: u64,
    /// Read commands that found a live key (INFO `keyspace_hits`, PR-3b).
    keyspace_hits: u64,
    /// Read commands that found no live key, including a lazily-expired one (INFO
    /// `keyspace_misses`, PR-3b).
    keyspace_misses: u64,
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

    /// Record `n` keys reclaimed due to TTL expiry (PR-3b; INFO `expired_keys`).
    /// Called by the serve loop after the active timing-wheel drain.
    pub fn on_expired(&mut self, n: u64) {
        self.expired_keys += n;
    }

    /// Record `n` keyspace hits (a read found a live key, INFO `keyspace_hits`).
    pub fn on_keyspace_hits(&mut self, n: u64) {
        self.keyspace_hits += n;
    }

    /// Record `n` keyspace misses (a read found no live key, INFO `keyspace_misses`).
    pub fn on_keyspace_misses(&mut self, n: u64) {
        self.keyspace_misses += n;
    }

    /// Fold a batch of per-command counter deltas (PR-3b: the eviction / expiry /
    /// keyspace-hit-miss outputs dispatch accumulates for one command) into this
    /// shard's counters. Called once per command after dispatch returns, so the
    /// dynamic counters do not alias the INFO rollup's borrow during dispatch.
    ///
    /// `d.reset_stats` (PR-4b `CONFIG RESETSTAT`) zeroes the resettable STAT counters
    /// FIRST (the additive deltas are then applied on top, though a RESETSTAT command
    /// produces no other deltas). It zeroes the same stats Redis `resetServerStats`
    /// does: the eviction / expiry / keyspace hit-miss totals and the command /
    /// connection counters. It does NOT touch `connected_clients` (a live gauge, not a
    /// since-reset stat), matching Redis (RESETSTAT leaves connected_clients alone).
    pub fn apply(&mut self, d: CounterDeltas) {
        if d.reset_stats {
            self.evicted_keys = 0;
            self.expired_keys = 0;
            self.keyspace_hits = 0;
            self.keyspace_misses = 0;
            self.commands_processed = 0;
            self.connections_received = 0;
        }
        self.evicted_keys += d.evicted;
        self.expired_keys += d.expired;
        self.keyspace_hits += d.keyspace_hits;
        self.keyspace_misses += d.keyspace_misses;
    }

    /// Take an immutable snapshot for rollup.
    #[must_use]
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            connections_received: self.connections_received,
            commands_processed: self.commands_processed,
            connected_clients: self.connected_clients,
            evicted_keys: self.evicted_keys,
            expired_keys: self.expired_keys,
            keyspace_hits: self.keyspace_hits,
            keyspace_misses: self.keyspace_misses,
        }
    }
}

/// The per-command counter deltas dispatch (and the active drain) accumulate for ONE
/// command, applied to the shard's [`ShardCounters`] after dispatch returns. Passed
/// as a single `&mut` out-parameter so the dynamic counters do not alias the INFO
/// rollup closure's borrow of the same shard counters during dispatch (the serve loop
/// applies the deltas once dispatch has returned).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CounterDeltas {
    /// Keys evicted by the admission gate (`evict_to_fit`) this command.
    pub evicted: u64,
    /// Keys reclaimed by the active TTL drain (and the lazy backstop) this command.
    pub expired: u64,
    /// Keyspace hits from read commands this command.
    pub keyspace_hits: u64,
    /// Keyspace misses from read commands this command.
    pub keyspace_misses: u64,
    /// `CONFIG RESETSTAT` (PR-4b): when true, [`ShardCounters::apply`] zeroes the
    /// resettable STAT counters on the serving shard FIRST (serving-shard-scoped, like
    /// the single-shard KEYS/SCAN scope; the cross-shard reset is a coordinator
    /// follow-up). The dispatch layer sets this for a `CONFIG RESETSTAT` and the serve
    /// loop honors it in `apply`.
    pub reset_stats: bool,
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
    /// Total keys reclaimed by this shard due to TTL expiry (INFO `expired_keys`,
    /// PR-3b: the active wheel drain plus the lazy backstop).
    pub expired_keys: u64,
    /// Total read hits on a live key (INFO `keyspace_hits`, PR-3b).
    pub keyspace_hits: u64,
    /// Total read misses (absent/expired key) (INFO `keyspace_misses`, PR-3b).
    pub keyspace_misses: u64,
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
            expired_keys: self.expired_keys + other.expired_keys,
            keyspace_hits: self.keyspace_hits + other.keyspace_hits,
            keyspace_misses: self.keyspace_misses + other.keyspace_misses,
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
    /// The stable 40-lowercase-hex cluster node id, generated ONCE at boot through the
    /// determinism seam (ADR-0003: drawn from the binary's `SystemEnv` RNG in
    /// `serve::run_server`, then leaked to `'static`), identical across shards
    /// (CLUSTER_CONTRACT.md #70). Reported by `CLUSTER MYID` / `CLUSTER NODES`. A real
    /// Redis assigns a 40-hex node id whether or not cluster mode is on, and so does
    /// IronCache.
    pub cluster_node_id: &'static str,
    /// Whether the server booted in cluster mode (Redis `cluster-enabled`,
    /// CLUSTER_CONTRACT.md #70). Reported by the INFO `# Cluster` section
    /// (`cluster_enabled:0/1`) and `CLUSTER INFO`. Slice 1 is cluster-disabled, so this is
    /// `false` in practice; the field is sourced from config so a later slice can flip it.
    pub cluster_enabled: bool,
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

/// One connected replica's line in a master's INFO `# Replication` section (HA-7e): the
/// `slaveN:ip=..,port=..,state=online,offset=..,lag=..` entry Redis emits per connected slave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaLine {
    /// The replica's advertised IP/host.
    pub ip: String,
    /// The replica's advertised client port.
    pub port: u16,
    /// The replica's last-acked replication offset.
    pub offset: u64,
    /// The replica's lag in logical writes (the master's `head - replica_acked`).
    pub lag: u64,
}

/// The replication facts INFO's `# Replication` section renders (HA-7e), translated by the serve
/// layer from the node-level replication status (`ironcache_repl::ReplStatusSnapshot`).
///
/// This is a PLAIN POD with NO dependency on the replication crate, so `ironcache-observe` stays
/// a leaf: the server crate (which DOES know the repl status) fills it in. The DEFAULT
/// ([`ReplicationInfo::standalone`]) is a master with no slaves at offset 0, byte-compatible with
/// a standalone Redis's `# Replication` section, which is what the default static path reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationInfo {
    /// `true` if this node is a master, `false` if it is a replica.
    pub is_master: bool,
    /// The node's own replication offset (`master_repl_offset` on a master; the replica's
    /// applied offset is reported separately as `slave_repl_offset`).
    pub master_repl_offset: u64,
    /// MASTER side: the connected replicas (each rendered as a `slaveN:` line). Empty on a master
    /// with no slaves and on a replica.
    pub slaves: Vec<ReplicaLine>,
    /// REPLICA side: `Some((host, port))` the master endpoint when this node is a replica.
    pub master_endpoint: Option<(String, u16)>,
    /// REPLICA side: whether the link to the master is up (`master_link_status:up|down`).
    pub master_link_up: bool,
    /// REPLICA side: this replica's own applied offset (`slave_repl_offset`).
    pub slave_repl_offset: u64,
}

impl ReplicationInfo {
    /// The standalone/default `# Replication` posture: a master with no slaves at offset 0. This
    /// is byte-compatible with a standalone Redis and is what the DEFAULT static path reports
    /// (no replication status cell present).
    #[must_use]
    pub fn standalone() -> Self {
        ReplicationInfo {
            is_master: true,
            master_repl_offset: 0,
            slaves: Vec::new(),
            master_endpoint: None,
            master_link_up: false,
            slave_repl_offset: 0,
        }
    }
}

impl Default for ReplicationInfo {
    fn default() -> Self {
        Self::standalone()
    }
}

/// The CURRENT effective `maxmemory`/`maxmemory_policy` INFO reports (CONFIG.md, the
/// `CONFIG SET` hot-swap, PR-4b). The boot values live in [`ServerInfo`] as static
/// facts, but a runtime `CONFIG SET` changes the effective ceiling/policy, so the
/// caller reads the CURRENT values from the runtime-config cell and passes them here.
/// INFO then reflects a `CONFIG SET maxmemory`/`maxmemory-policy` immediately.
#[derive(Debug, Clone, Copy)]
pub struct EffectiveMemoryConfig<'a> {
    /// The current effective `maxmemory` ceiling in bytes (0 = unlimited).
    pub maxmemory: u64,
    /// The current effective `maxmemory-policy` name (verbatim).
    pub maxmemory_policy: &'a str,
}

/// Build the `INFO` reply body (OBSERVABILITY.md). `section` is the optional
/// lowercased section filter (e.g. `server`); `None` or `"default"`/`"all"`
/// renders all sections.
///
/// `memory` carries the process-global allocator figures (ADR-0006), read once by
/// the caller; the `memory` section reports them for `used_memory`/`used_memory_rss`
/// and derives `used_memory_human` and `mem_fragmentation_ratio` (RSS/used) from
/// them. `effective` carries the CURRENT `maxmemory`/`maxmemory_policy` (PR-4b): the
/// caller reads them from the runtime-config cell so a `CONFIG SET` is reflected in
/// INFO, rather than the static boot values held in [`ServerInfo`].
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
    effective: EffectiveMemoryConfig<'_>,
    replication: &ReplicationInfo,
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
        // PR-4b: report the CURRENT effective maxmemory/maxmemory_policy (read from the
        // runtime-config cell), so a `CONFIG SET maxmemory`/`maxmemory-policy` is
        // reflected here immediately. The boot values in `server` are the static facts;
        // `effective` is the live overlay.
        let _ = write!(out, "maxmemory:{}\r\n", effective.maxmemory);
        let _ = write!(out, "maxmemory_policy:{}\r\n", effective.maxmemory_policy);
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
        // PR-3b: expired_keys is the rolled-up TTL-reclamation total (active wheel
        // drain + lazy backstop). PR-3a: evicted_keys is the maxmemory-eviction total.
        let _ = write!(out, "expired_keys:{}\r\n", rolled.expired_keys);
        let _ = write!(out, "evicted_keys:{}\r\n", rolled.evicted_keys);
        // PR-3b: keyspace hit/miss totals from read commands.
        let _ = write!(out, "keyspace_hits:{}\r\n", rolled.keyspace_hits);
        let _ = write!(out, "keyspace_misses:{}\r\n", rolled.keyspace_misses);
        out.push_str("\r\n");
    }
    if want("replication") {
        push_replication_section(&mut out, replication);
    }
    if want("cluster") {
        // The `# Cluster` section (CLUSTER_CONTRACT.md #70). Redis emits this section
        // (after Stats) whether or not cluster mode is on; the single `cluster_enabled`
        // field is `0` when disabled and `1` when enabled, sourced from config and kept
        // consistent with `CLUSTER INFO`'s `cluster_enabled:` line. Slice 1 is
        // cluster-disabled, so this reports `0`.
        out.push_str("# Cluster\r\n");
        let _ = write!(
            out,
            "cluster_enabled:{}\r\n",
            u8::from(server.cluster_enabled)
        );
        out.push_str("\r\n");
    }
    out
}

/// Append the INFO `# Replication` section (HA-7e) to `out`, matching Redis's field names + shape
/// so existing parsers / `redis_exporter` read it. A MASTER reports `role:master`,
/// `connected_slaves`, and one `slaveN:` line per connected replica; a REPLICA additionally
/// reports its `master_host`/`master_port`/`master_link_status`/`slave_repl_offset`/
/// `slave_read_only`. In the DEFAULT static (standalone) posture this is `role:master` with 0
/// slaves at offset 0, byte-compatible with a standalone Redis.
fn push_replication_section(out: &mut String, replication: &ReplicationInfo) {
    use core::fmt::Write as _;
    out.push_str("# Replication\r\n");
    if replication.is_master {
        out.push_str("role:master\r\n");
        let _ = write!(out, "connected_slaves:{}\r\n", replication.slaves.len());
        for (i, s) in replication.slaves.iter().enumerate() {
            // slaveN:ip=<ip>,port=<port>,state=online,offset=<offset>,lag=<lag>
            let _ = write!(
                out,
                "slave{i}:ip={},port={},state=online,offset={},lag={}\r\n",
                s.ip, s.port, s.offset, s.lag
            );
        }
    } else {
        out.push_str("role:replica\r\n");
        // The master endpoint: host/port the replica is attached to (empty strings / 0 if not yet
        // resolved, matching Redis's pre-attach placeholders).
        let (mhost, mport) = replication
            .master_endpoint
            .clone()
            .unwrap_or_else(|| (String::new(), 0));
        let _ = write!(out, "master_host:{mhost}\r\n");
        let _ = write!(out, "master_port:{mport}\r\n");
        let _ = write!(
            out,
            "master_link_status:{}\r\n",
            if replication.master_link_up {
                "up"
            } else {
                "down"
            }
        );
        // A replica is read-only by default (HA-7d passive replica): slave_read_only:1.
        out.push_str("slave_read_only:1\r\n");
        let _ = write!(
            out,
            "slave_repl_offset:{}\r\n",
            replication.slave_repl_offset
        );
    }
    // master_repl_offset is reported in BOTH roles (Redis does too): the master's head, or the
    // master offset a replica last observed.
    let _ = write!(
        out,
        "master_repl_offset:{}\r\n",
        replication.master_repl_offset
    );
    out.push_str("\r\n");
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
            cluster_node_id: "0000000000000000000000000000000000000000",
            cluster_enabled: false,
        }
    }

    /// The default effective memory config for the tests (mirrors the boot values).
    fn eff() -> EffectiveMemoryConfig<'static> {
        EffectiveMemoryConfig {
            maxmemory: 0,
            maxmemory_policy: "allkeys-lru",
        }
    }

    /// The default (standalone) replication info for the tests.
    fn repl() -> ReplicationInfo {
        ReplicationInfo::standalone()
    }

    #[test]
    fn info_has_standard_sections_and_fields() {
        let env = TestEnv::new(1);
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            None,
        );
        assert!(body.contains("# Server\r\n"));
        assert!(body.contains("# Clients\r\n"));
        assert!(body.contains("# Memory\r\n"));
        assert!(body.contains("# Stats\r\n"));
        // The `# Cluster` section reports cluster_enabled:0 in the cluster-disabled
        // default (CLUSTER_CONTRACT.md #70).
        assert!(body.contains("# Cluster\r\n"));
        assert!(body.contains("cluster_enabled:0\r\n"));
        assert!(body.contains("tcp_port:6379\r\n"));
        assert!(body.contains("connected_clients:0\r\n"));
        assert!(body.contains("mem_allocator:jemalloc\r\n"));
        assert!(body.contains(&format!("ironcache_version:{SERVER_VERSION}\r\n")));
    }

    #[test]
    fn info_memory_threads_maxmemory_and_allocator() {
        let env = TestEnv::new(1);
        let mut s = server();
        s.mem_allocator = "system";
        // PR-4b: maxmemory is read from the EFFECTIVE config (the runtime overlay), not
        // the static ServerInfo, so INFO reflects a CONFIG SET.
        let effective = EffectiveMemoryConfig {
            maxmemory: 256 * 1024 * 1024,
            maxmemory_policy: "allkeys-lru",
        };
        let body = build_info(
            &env,
            &s,
            CounterSnapshot::default(),
            MemoryInfo::default(),
            effective,
            &repl(),
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
        // PR-4b: maxmemory_policy is the CURRENT effective name (read from the runtime
        // overlay), and evicted_keys is the rolled-up counter.
        let env = TestEnv::new(1);
        let effective = EffectiveMemoryConfig {
            maxmemory: 0,
            maxmemory_policy: "volatile-ttl",
        };
        let rolled = CounterSnapshot {
            evicted_keys: 7,
            ..Default::default()
        };
        let body = build_info(
            &env,
            &server(),
            rolled,
            MemoryInfo::default(),
            effective,
            &repl(),
            None,
        );
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
            eff(),
            &repl(),
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
            eff(),
            &repl(),
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
            eff(),
            &repl(),
            Some("server"),
        );
        assert!(only_server.contains("# Server\r\n"));
        assert!(!only_server.contains("# Memory\r\n"));
        // The `# Cluster` section is gated by the filter too: a server-only INFO omits it.
        assert!(!only_server.contains("# Cluster\r\n"));
        // Asking for the cluster section yields it with the disabled flag.
        let only_cluster = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            Some("cluster"),
        );
        assert!(only_cluster.contains("# Cluster\r\n"));
        assert!(only_cluster.contains("cluster_enabled:0\r\n"));
        assert!(!only_cluster.contains("# Server\r\n"));
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
            eff(),
            &repl(),
            Some("server"),
        );
        assert!(body.contains("uptime_in_seconds:90\r\n"), "{body}");
    }

    /// A master with NO slaves renders the byte-compatible standalone `# Replication` posture:
    /// role:master, connected_slaves:0, master_repl_offset:0, and NO slaveN lines.
    #[test]
    fn info_replication_master_no_slaves_matches_standalone() {
        let env = TestEnv::new(1);
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &ReplicationInfo::standalone(),
            Some("replication"),
        );
        assert!(body.contains("# Replication\r\n"), "{body}");
        assert!(body.contains("role:master\r\n"), "{body}");
        assert!(body.contains("connected_slaves:0\r\n"), "{body}");
        assert!(body.contains("master_repl_offset:0\r\n"), "{body}");
        assert!(!body.contains("slave0:"), "{body}");
        // A standalone reports neither master_host nor slave_read_only (those are replica-only).
        assert!(!body.contains("master_host:"), "{body}");
        assert!(!body.contains("slave_read_only:"), "{body}");
    }

    /// A master WITH a connected slave renders `connected_slaves:1` and a `slave0:` line carrying
    /// the slave's offset + lag, plus its own master_repl_offset.
    #[test]
    fn info_replication_master_with_slave_reports_offset_and_lag() {
        let env = TestEnv::new(1);
        let repl = ReplicationInfo {
            is_master: true,
            master_repl_offset: 100,
            slaves: vec![ReplicaLine {
                ip: "10.0.0.2".to_owned(),
                port: 6380,
                offset: 95,
                lag: 5,
            }],
            master_endpoint: None,
            master_link_up: false,
            slave_repl_offset: 0,
        };
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl,
            Some("replication"),
        );
        assert!(body.contains("role:master\r\n"), "{body}");
        assert!(body.contains("connected_slaves:1\r\n"), "{body}");
        assert!(
            body.contains("slave0:ip=10.0.0.2,port=6380,state=online,offset=95,lag=5\r\n"),
            "{body}"
        );
        assert!(body.contains("master_repl_offset:100\r\n"), "{body}");
    }

    /// A replica renders `role:replica`, its master endpoint + link status, `slave_read_only:1`,
    /// its own `slave_repl_offset`, and the master's `master_repl_offset`.
    #[test]
    fn info_replication_replica_reports_master_link_and_offsets() {
        let env = TestEnv::new(1);
        let repl = ReplicationInfo {
            is_master: false,
            master_repl_offset: 100, // the master's head as observed
            slaves: Vec::new(),
            master_endpoint: Some(("10.0.0.1".to_owned(), 6379)),
            master_link_up: true,
            slave_repl_offset: 98, // this replica's applied offset
        };
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl,
            Some("replication"),
        );
        assert!(body.contains("role:replica\r\n"), "{body}");
        assert!(body.contains("master_host:10.0.0.1\r\n"), "{body}");
        assert!(body.contains("master_port:6379\r\n"), "{body}");
        assert!(body.contains("master_link_status:up\r\n"), "{body}");
        assert!(body.contains("slave_read_only:1\r\n"), "{body}");
        assert!(body.contains("slave_repl_offset:98\r\n"), "{body}");
        assert!(body.contains("master_repl_offset:100\r\n"), "{body}");
        // A replica reports no connected_slaves line / no slaveN entries.
        assert!(!body.contains("connected_slaves:"), "{body}");
        // A down link reports master_link_status:down.
        let down = ReplicationInfo {
            master_link_up: false,
            ..repl
        };
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &down,
            Some("replication"),
        );
        assert!(body.contains("master_link_status:down\r\n"), "{body}");
    }

    /// The `# Replication` section is gated by the section filter (a server-only INFO omits it; a
    /// replication-only INFO yields it and not the others).
    #[test]
    fn info_replication_section_is_filtered() {
        let env = TestEnv::new(1);
        let only_server = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            Some("server"),
        );
        assert!(!only_server.contains("# Replication\r\n"), "{only_server}");
        let only_repl = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            Some("replication"),
        );
        assert!(only_repl.contains("# Replication\r\n"), "{only_repl}");
        assert!(!only_repl.contains("# Server\r\n"), "{only_repl}");
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
