// SPDX-License-Identifier: MIT OR Apache-2.0
//! Parse the Redis/IronCache `INFO` reply into a dashboard view (issue #366).
//!
//! `INFO` returns a single bulk string of `# Section` headers and `key:value`
//! lines (CRLF- or LF-separated). The console extracts the handful of fields the
//! dashboard shows ([`NodeInfo`]) and ALSO keeps the full `key:value` map in
//! [`NodeInfo::raw`] so a field we did not model (version skew across server
//! releases) is still inspectable. Parsing is DEFENSIVE: a missing field becomes
//! `None`, an unparseable number becomes `None`, an unknown line is kept only in
//! the raw map, and a blank line / comment is skipped. It never errors and never
//! panics: a malformed INFO yields a sparse [`NodeInfo`], not a failure.
//!
//! ## Determinism (ADR-0003)
//!
//! Pure string parsing: no clock, no RNG, no I/O.

use std::collections::HashMap;

/// The dashboard-relevant slice of a node's `INFO`, plus the full raw map.
///
/// Every typed field is optional: a node that omits it (an older/newer server, a
/// disabled subsystem) leaves it `None` rather than forcing a default that would
/// read as a real value on the dashboard.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct NodeInfo {
    /// `redis_version` (the server-reported version string).
    pub redis_version: Option<String>,
    /// `uptime_in_seconds`.
    pub uptime_in_seconds: Option<u64>,
    /// `connected_clients`.
    pub connected_clients: Option<u64>,
    /// `used_memory` (bytes, the allocator's view).
    pub used_memory: Option<u64>,
    /// `used_memory_rss` (bytes, the OS RSS view).
    pub used_memory_rss: Option<u64>,
    /// `maxmemory` (bytes; `0` means no limit, kept verbatim).
    pub maxmemory: Option<u64>,
    /// `keyspace_hits`.
    pub keyspace_hits: Option<u64>,
    /// `keyspace_misses`.
    pub keyspace_misses: Option<u64>,
    /// `total_commands_processed`.
    pub total_commands_processed: Option<u64>,
    /// `evicted_keys`.
    pub evicted_keys: Option<u64>,
    /// `expired_keys`.
    pub expired_keys: Option<u64>,
    /// `rdb_last_save_time` (unix seconds of the last successful save).
    pub rdb_last_save_time: Option<u64>,
    /// `rdb_changes_since_last_save`.
    pub rdb_changes_since_last_save: Option<u64>,
    /// `cluster_enabled` (`1` -> `true`, `0`/absent -> `false`).
    pub cluster_enabled: bool,
    /// The total key count summed across every `dbN:keys=...` line in the
    /// `# Keyspace` section. `None` if no keyspace line was present.
    pub total_keys: Option<u64>,
    /// The full `key:value` map (every parsed line), for fields not modeled above.
    pub raw: HashMap<String, String>,
}

impl NodeInfo {
    /// The cache hit ratio in `[0.0, 1.0]`, or `None` if neither hits nor misses
    /// are known or both are zero (an undefined ratio). A small convenience the
    /// dashboard/REST layers reuse rather than recomputing.
    #[must_use]
    pub fn hit_ratio(&self) -> Option<f64> {
        let hits = self.keyspace_hits?;
        let misses = self.keyspace_misses?;
        let total = hits.checked_add(misses)?;
        if total == 0 {
            return None;
        }
        Some(hits as f64 / total as f64)
    }
}

/// Parse an `INFO` bulk-string body into a [`NodeInfo`]. Tolerant by design: it
/// accepts CRLF or LF line endings, skips `# Section` headers and blank lines,
/// splits each remaining line on the FIRST `:` (a value may itself contain `:`,
/// e.g. a keyspace line), and records every pair in [`NodeInfo::raw`]. The typed
/// fields are then read out of that map.
#[must_use]
pub fn parse_info(body: &str) -> NodeInfo {
    let mut raw: HashMap<String, String> = HashMap::new();
    let mut total_keys: Option<u64> = None;

    for line in body.lines() {
        let line = line.trim_end_matches('\r');
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() {
            continue;
        }
        // A keyspace line is `dbN:keys=<n>,expires=<m>,...`; sum the keys.
        if key.starts_with("db") && value.contains("keys=") {
            if let Some(n) = parse_keyspace_keys(value) {
                total_keys = Some(total_keys.unwrap_or(0).saturating_add(n));
            }
        }
        raw.insert(key.to_owned(), value.to_owned());
    }

    let u = |k: &str| raw.get(k).and_then(|v| v.trim().parse::<u64>().ok());
    let s = |k: &str| raw.get(k).map(ToOwned::to_owned);

    NodeInfo {
        redis_version: s("redis_version"),
        uptime_in_seconds: u("uptime_in_seconds"),
        connected_clients: u("connected_clients"),
        used_memory: u("used_memory"),
        used_memory_rss: u("used_memory_rss"),
        maxmemory: u("maxmemory"),
        keyspace_hits: u("keyspace_hits"),
        keyspace_misses: u("keyspace_misses"),
        total_commands_processed: u("total_commands_processed"),
        evicted_keys: u("evicted_keys"),
        expired_keys: u("expired_keys"),
        rdb_last_save_time: u("rdb_last_save_time"),
        rdb_changes_since_last_save: u("rdb_changes_since_last_save"),
        cluster_enabled: raw.get("cluster_enabled").map(String::as_str) == Some("1"),
        total_keys,
        raw,
    }
}

/// Extract the `keys=<n>` count from a keyspace value (`keys=12,expires=3,...`).
fn parse_keyspace_keys(value: &str) -> Option<u64> {
    value
        .split(',')
        .find_map(|part| part.trim().strip_prefix("keys="))
        .and_then(|n| n.trim().parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    // A realistic (trimmed) INFO sample with CRLF line endings, multiple sections,
    // a keyspace section with two databases, and a couple of fields we do NOT
    // model (to prove they survive in `raw`).
    const SAMPLE: &str = "# Server\r\n\
        redis_version:7.2.4\r\n\
        uptime_in_seconds:123456\r\n\
        \r\n\
        # Clients\r\n\
        connected_clients:42\r\n\
        \r\n\
        # Memory\r\n\
        used_memory:1048576\r\n\
        used_memory_rss:2097152\r\n\
        maxmemory:0\r\n\
        mem_fragmentation_ratio:1.50\r\n\
        \r\n\
        # Persistence\r\n\
        rdb_changes_since_last_save:7\r\n\
        rdb_last_save_time:1700000000\r\n\
        \r\n\
        # Stats\r\n\
        total_commands_processed:9999\r\n\
        expired_keys:13\r\n\
        evicted_keys:5\r\n\
        keyspace_hits:800\r\n\
        keyspace_misses:200\r\n\
        \r\n\
        # Cluster\r\n\
        cluster_enabled:0\r\n\
        \r\n\
        # Keyspace\r\n\
        db0:keys=10,expires=2,avg_ttl=0\r\n\
        db1:keys=5,expires=0,avg_ttl=0\r\n";

    #[test]
    fn parses_the_modeled_fields() {
        let info = parse_info(SAMPLE);
        assert_eq!(info.redis_version.as_deref(), Some("7.2.4"));
        assert_eq!(info.uptime_in_seconds, Some(123_456));
        assert_eq!(info.connected_clients, Some(42));
        assert_eq!(info.used_memory, Some(1_048_576));
        assert_eq!(info.used_memory_rss, Some(2_097_152));
        assert_eq!(info.maxmemory, Some(0));
        assert_eq!(info.keyspace_hits, Some(800));
        assert_eq!(info.keyspace_misses, Some(200));
        assert_eq!(info.total_commands_processed, Some(9999));
        assert_eq!(info.evicted_keys, Some(5));
        assert_eq!(info.expired_keys, Some(13));
        assert_eq!(info.rdb_last_save_time, Some(1_700_000_000));
        assert_eq!(info.rdb_changes_since_last_save, Some(7));
        assert!(!info.cluster_enabled);
    }

    #[test]
    fn sums_keys_across_databases() {
        let info = parse_info(SAMPLE);
        assert_eq!(info.total_keys, Some(15));
    }

    #[test]
    fn keeps_unmodeled_fields_in_raw() {
        let info = parse_info(SAMPLE);
        assert_eq!(
            info.raw.get("mem_fragmentation_ratio").map(String::as_str),
            Some("1.50")
        );
        // Section headers are NOT keys.
        assert!(!info.raw.contains_key("# Server"));
    }

    #[test]
    fn hit_ratio_is_computed() {
        let info = parse_info(SAMPLE);
        // 800 / (800 + 200) = 0.8
        assert!((info.hit_ratio().unwrap() - 0.8).abs() < 1e-9);
    }

    #[test]
    fn cluster_enabled_true() {
        let info = parse_info("# Cluster\ncluster_enabled:1\n");
        assert!(info.cluster_enabled);
    }

    #[test]
    fn tolerates_lf_only_and_missing_fields() {
        let info = parse_info("redis_version:8.0.0\nconnected_clients:1\n");
        assert_eq!(info.redis_version.as_deref(), Some("8.0.0"));
        assert_eq!(info.connected_clients, Some(1));
        // Unset fields are None, not defaulted.
        assert_eq!(info.used_memory, None);
        assert_eq!(info.total_keys, None);
        assert!(!info.cluster_enabled);
    }

    #[test]
    fn tolerates_garbage_and_blank_lines() {
        let info =
            parse_info("\n# only a header\n\nnotakeyvalue\n:emptykey\nredis_version:1.2.3\n");
        // Garbage lines are skipped; the one good line still parses.
        assert_eq!(info.redis_version.as_deref(), Some("1.2.3"));
    }

    #[test]
    fn unparseable_number_is_none_not_error() {
        let info = parse_info("uptime_in_seconds:not-a-number\n");
        assert_eq!(info.uptime_in_seconds, None);
        // But the raw value is preserved verbatim.
        assert_eq!(
            info.raw.get("uptime_in_seconds").map(String::as_str),
            Some("not-a-number")
        );
    }

    #[test]
    fn empty_info_is_empty_node_info() {
        let info = parse_info("");
        assert_eq!(info, NodeInfo::default());
    }
}
